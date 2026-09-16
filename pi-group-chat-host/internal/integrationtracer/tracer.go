// Package integrationtracer hosts the PG-40/PG-41 Host-side integration
// scenarios. It is deliberately NOT a composition root: each scenario builds
// its own in-memory authorities and reports factual outcomes as JSON for the
// Python orchestrators under
// specs/benchmark-diagnosis-replay-trajectory-export/conformance/integration.
// PG-50B remains the sole writer of cmd/bench-runner and the runtime
// composition.
package integrationtracer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	hostcut "river2.dev/pi-group-chat-host/internal/consolidationcut"
	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/evaluationbatch"
	"river2.dev/pi-group-chat-host/internal/evidence"
	"river2.dev/pi-group-chat-host/internal/ports"
	"river2.dev/pi-group-chat-host/internal/room"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

func shaHex(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func digestOf(input string) string {
	return "sha256:" + shaHex(input)
}

// fixtureFreezeStore is the tracer's deterministic FreezeStore: one sealed
// room with exact 1:1 receipts, and one room with an open segment that the
// coordinator must defer fail-closed. Each sealed segment carries both of its
// projection batches (room_shared and agent_private) so downstream scopes can
// be split per space instead of sharing one batch set. PG-50B replaces this
// with the durable store adapter.
type fixtureFreezeStore struct {
	tenantID domain.TenantID
	epoch    int64
	sequence int64
	sealed   map[domain.RoomID][]domain.SegmentID
	open     map[domain.RoomID][]domain.SegmentID
}

func (s *fixtureFreezeStore) RoomFreezePoint(_ context.Context, tenantID domain.TenantID, roomID domain.RoomID) (hostcut.RoomFreezePoint, error) {
	return hostcut.RoomFreezePoint{TenantID: tenantID, RoomID: roomID, Epoch: s.epoch, Sequence: s.sequence}, nil
}

func (s *fixtureFreezeStore) SealedSegments(_ context.Context, roomID domain.RoomID, _ int64) ([]domain.SegmentID, error) {
	return s.sealed[roomID], nil
}

func (s *fixtureFreezeStore) OpenSegments(_ context.Context, roomID domain.RoomID, _ int64) ([]domain.SegmentID, error) {
	return s.open[roomID], nil
}

func (s *fixtureFreezeStore) WaitForCommitReceipts(_ context.Context, roomID domain.RoomID, sealed []domain.SegmentID) ([]hostcut.CommitReceipt, error) {
	receipts := make([]hostcut.CommitReceipt, 0, len(sealed))
	for index, segment := range sealed {
		receipts = append(receipts, hostcut.CommitReceipt{
			ReceiptID: domain.SegmentID("receipt-" + string(segment)),
			SegmentID: segment,
			BatchIDs: []string{
				sharedBatchID(roomID, index+1),
				privateBatchID(roomID, index+1),
			},
			Digest: digestOf(string(s.tenantID) + "|" + string(segment)),
		})
	}
	return receipts, nil
}

// The fixture batch naming encodes the projection kind; the split below is
// fixture-authority string matching, replaced by typed projection rows in
// PG-50B.
func sharedBatchID(roomID domain.RoomID, index int) string {
	return fmt.Sprintf("batch-%s-shared-%03d", roomID, index)
}

func privateBatchID(roomID domain.RoomID, index int) string {
	return fmt.Sprintf("batch-%s-private-%03d", roomID, index)
}

func isSharedBatch(batchID string) bool {
	return strings.Contains(batchID, "-shared-")
}

// sameStringSet reports set equality ignoring order and duplicates.
func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]int, len(left))
	for _, value := range left {
		seen[value]++
	}
	for _, value := range right {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}

// hostSpaceJSON is one frozen space scope in the contract-shaped freeze
// output: its own evidence batch set plus its projection head/query
// watermark, so the two spaces never share one undifferentiated batch list.
type hostSpaceJSON struct {
	SpaceID               string   `json:"space_id"`
	Scope                 string   `json:"scope"`
	EvidenceBatchIDs      []string `json:"evidence_batch_ids"`
	ProjectionHeadVersion int64    `json:"projection_head_version"`
	QueryWatermark        int64    `json:"query_watermark"`
}

// hostReceiptJSON is one evidence commit receipt in the contract-shaped
// output.
type hostReceiptJSON struct {
	ReceiptID   string   `json:"receipt_id"`
	SegmentID   string   `json:"segment_id"`
	BatchIDs    []string `json:"batch_ids"`
	BatchDigest string   `json:"batch_digest"`
}

// hostFreezeJSON is the contract-shaped freeze output consumed by the GMS
// production-cut driver (mirror of the GMS integrationtracer hostFreeze).
type hostFreezeJSON struct {
	TenantID            string            `json:"tenant_id"`
	RoomID              string            `json:"room_id"`
	RoomEpoch           int64             `json:"room_epoch"`
	RoomSequence        int64             `json:"room_sequence"`
	SealedSegmentIDs    []string          `json:"sealed_segment_ids"`
	DeferredSegmentIDs  []string          `json:"deferred_segment_ids"`
	SubmissionPermitted bool              `json:"submission_permitted"`
	Receipts            []hostReceiptJSON `json:"receipts"`
	Spaces              []hostSpaceJSON   `json:"spaces"`
}

// splitProjectionBatches partitions receipt batches into the room_shared and
// agent_private projection sets.
func splitProjectionBatches(receipts []hostcut.CommitReceipt) (shared, private []string) {
	for _, receipt := range receipts {
		for _, batch := range receipt.BatchIDs {
			if isSharedBatch(batch) {
				shared = append(shared, batch)
			} else {
				private = append(private, batch)
			}
		}
	}
	return shared, private
}

// Freeze runs the PG-40 Host half (SC-4.4): the coordinator pins epoch,
// sequence, and the sealed segment set; a room with an open segment is
// deferred fail-closed with submission forbidden; the sealed room's receipts
// cover exactly; and the same request replays the identical snapshot while a
// same-key/different-body retry conflicts. The sealed room's freeze is
// written to outputPath as the GMS driver's input, with the shared and
// private space scopes carrying their own distinct evidence batch sets and
// projection watermarks (P1-8).
func Freeze(outputPath string) (map[string]any, error) {
	ctx := context.Background()
	tenantID := domain.TenantID("tenant-pg40")
	sealedRoom := domain.RoomID("room-pg40-main")
	openRoom := domain.RoomID("room-pg40-open")
	store := &fixtureFreezeStore{
		tenantID: tenantID, epoch: 7, sequence: 42,
		sealed: map[domain.RoomID][]domain.SegmentID{
			sealedRoom: {"segment-main-001", "segment-main-002"},
			openRoom:   {"segment-open-001"},
		},
		open: map[domain.RoomID][]domain.SegmentID{
			openRoom: {"segment-open-002"},
		},
	}
	coordinator := hostcut.NewCoordinator(store)

	snapshot, err := coordinator.Freeze(ctx, hostcut.FreezeRequest{
		TenantID: tenantID, RoomID: sealedRoom, Mode: hostcut.ModeForce, IdempotencyKey: "freeze-pg40-main",
	})
	if err != nil {
		return nil, fmt.Errorf("freeze sealed room: %w", err)
	}
	replay, err := coordinator.Freeze(ctx, hostcut.FreezeRequest{
		TenantID: tenantID, RoomID: sealedRoom, Mode: hostcut.ModeForce, IdempotencyKey: "freeze-pg40-main",
	})
	if err != nil {
		return nil, fmt.Errorf("freeze replay: %w", err)
	}
	_, errConflict := coordinator.Freeze(ctx, hostcut.FreezeRequest{
		TenantID: tenantID, RoomID: sealedRoom, Mode: hostcut.ModeThreshold, IdempotencyKey: "freeze-pg40-main",
	})
	bodyConflict := errors.Is(errConflict, hostcut.ErrFreezeRequestBodyConflict)

	deferred, err := coordinator.Freeze(ctx, hostcut.FreezeRequest{
		TenantID: tenantID, RoomID: openRoom, Mode: hostcut.ModeForce, IdempotencyKey: "freeze-pg40-open",
	})
	if err != nil {
		return nil, fmt.Errorf("freeze open room: %w", err)
	}

	idempotent := replay.RoomEpoch == snapshot.RoomEpoch &&
		replay.RoomSequence == snapshot.RoomSequence &&
		len(replay.SealedSegmentIDs) == len(snapshot.SealedSegmentIDs) &&
		replay.SubmissionPermitted == snapshot.SubmissionPermitted

	// The sealed room's evidence settles through the real evidence service
	// (R4): every sealed segment creates its room_shared and agent_private
	// outbox rows transactionally, and the space scopes below are derived
	// from rows read back out of the store rather than re-derived from
	// fixture strings.
	evidenceStore := memorystore.NewStore()
	settler := evidence.NewService(evidenceStore)
	settledAt := time.Now()
	receiptBatchesBySegment := map[domain.SegmentID][]string{}
	for _, receipt := range snapshot.EvidenceReceipts {
		receiptBatchesBySegment[receipt.SegmentID] = receipt.BatchIDs
	}
	for index, segment := range snapshot.SealedSegmentIDs {
		var sharedBatch, privateBatch string
		for _, batch := range receiptBatchesBySegment[segment] {
			if isSharedBatch(batch) {
				sharedBatch = batch
			} else {
				privateBatch = batch
			}
		}
		entries := []domain.EvidenceOutboxEntry{
			{
				ID: fmt.Sprintf("outbox-pg40-%03d-shared", index+1), SourceSegmentID: segment,
				Projection: domain.ProjectionRoomShared, SpaceID: domain.SpaceID("space-pg40-shared"),
				BatchID: sharedBatch, State: "pending", NextAttemptAt: settledAt,
			},
			{
				ID: fmt.Sprintf("outbox-pg40-%03d-private", index+1), SourceSegmentID: segment,
				Projection: domain.ProjectionAgentPrivate, SpaceID: domain.SpaceID("space-pg40-private"),
				BatchID: privateBatch, State: "pending", NextAttemptAt: settledAt,
			},
		}
		if err := settler.SettleAndCreateOutbox(ctx, domain.DeliveryID(fmt.Sprintf("delivery-pg40-%03d", index+1)), segment, settledAt, entries); err != nil {
			return nil, fmt.Errorf("settle segment %s: %w", segment, err)
		}
	}
	sharedFromStore := []string{}
	privateFromStore := []string{}
	outboxRowsRead := 0
	for _, segment := range snapshot.SealedSegmentIDs {
		rows, err := evidenceStore.OutboxEntriesBySegment(ctx, segment)
		if err != nil {
			return nil, fmt.Errorf("read outbox rows for %s: %w", segment, err)
		}
		outboxRowsRead += len(rows)
		for _, row := range rows {
			switch row.Projection {
			case domain.ProjectionRoomShared:
				sharedFromStore = append(sharedFromStore, row.BatchID)
			case domain.ProjectionAgentPrivate:
				privateFromStore = append(privateFromStore, row.BatchID)
			}
		}
	}

	// Emit the sealed room's freeze as the GMS driver input: each space scope
	// carries only its own projection's store-derived batches plus distinct
	// projection head/query watermarks. The head/watermark numbers are
	// fixture-authority values (PG-50B replaces them with typed projection
	// rows); the batch sets come from the durable outbox rows.
	receiptShared, receiptPrivate := splitProjectionBatches(snapshot.EvidenceReceipts)
	batchesMatchReceipts := sameStringSet(sharedFromStore, receiptShared) && sameStringSet(privateFromStore, receiptPrivate)
	sharedBatches, privateBatches := sharedFromStore, privateFromStore
	out := hostFreezeJSON{
		TenantID:            string(snapshot.TenantID),
		RoomID:              string(snapshot.RoomID),
		RoomEpoch:           snapshot.RoomEpoch,
		RoomSequence:        snapshot.RoomSequence,
		SubmissionPermitted: snapshot.SubmissionPermitted,
	}
	for _, segment := range snapshot.SealedSegmentIDs {
		out.SealedSegmentIDs = append(out.SealedSegmentIDs, string(segment))
	}
	for _, segment := range snapshot.DeferredSegmentIDs {
		out.DeferredSegmentIDs = append(out.DeferredSegmentIDs, string(segment))
	}
	for _, receipt := range snapshot.EvidenceReceipts {
		out.Receipts = append(out.Receipts, hostReceiptJSON{
			ReceiptID: string(receipt.ReceiptID), SegmentID: string(receipt.SegmentID),
			BatchIDs: receipt.BatchIDs, BatchDigest: receipt.Digest,
		})
	}
	out.Spaces = append(out.Spaces,
		hostSpaceJSON{
			SpaceID: "space-pg40-shared", Scope: "shared", EvidenceBatchIDs: sharedBatches,
			// fixture_authority: head/watermark are deterministic tracer
			// values until PG-50B wires typed projection rows.
			ProjectionHeadVersion: 8, QueryWatermark: 72,
		},
		hostSpaceJSON{
			SpaceID: "space-pg40-private", Scope: "private", EvidenceBatchIDs: privateBatches,
			ProjectionHeadVersion: 3, QueryWatermark: 41,
		},
	)
	privateSet := map[string]bool{}
	for _, batch := range privateBatches {
		privateSet[batch] = true
	}
	overlap := false
	for _, batch := range sharedBatches {
		if privateSet[batch] {
			overlap = true
		}
	}
	distinctBatches := len(sharedBatches) > 0 && len(privateBatches) > 0 && !overlap

	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode host freeze: %w", err)
	}
	if err := os.WriteFile(outputPath, append(encoded, '\n'), 0o644); err != nil {
		return nil, fmt.Errorf("write host freeze: %w", err)
	}

	return map[string]any{
		"freeze_path": outputPath,
		"sealed_room": map[string]any{
			"room_id":                     string(snapshot.RoomID),
			"epoch":                       snapshot.RoomEpoch,
			"sequence":                    snapshot.RoomSequence,
			"sealed_segments":             len(snapshot.SealedSegmentIDs),
			"receipts":                    len(snapshot.EvidenceReceipts),
			"receipts_complete":           snapshot.ReceiptsComplete,
			"submission":                  snapshot.SubmissionPermitted,
			"idempotent_replay":           idempotent,
			"body_conflict":               bodyConflict,
			"spaces":                      len(out.Spaces),
			"distinct_evidence_batches":   distinctBatches,
			"shared_batches":              len(sharedBatches),
			"private_batches":             len(privateBatches),
			"projection_heads":            []int64{out.Spaces[0].ProjectionHeadVersion, out.Spaces[1].ProjectionHeadVersion},
			"query_watermarks":            []int64{out.Spaces[0].QueryWatermark, out.Spaces[1].QueryWatermark},
			"outbox_rows_persisted":       outboxRowsRead,
			"batches_match_receipts":      batchesMatchReceipts,
			"projection_watermark_source": "fixture_authority",
		},
		"open_room": map[string]any{
			"room_id":           string(deferred.RoomID),
			"sealed_segments":   len(deferred.SealedSegmentIDs),
			"deferred_segments": len(deferred.DeferredSegmentIDs),
			"receipts_awaited":  len(deferred.EvidenceReceipts),
			"submission":        deferred.SubmissionPermitted,
		},
	}, nil
}

// agentRunResult is one simulated agent run (a registered child of exactly
// one logical run).
type agentRunResult struct {
	AgentRunID string `json:"agent_run_id"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

// logicalRunLedgerEntry is the immutable-identity ledger record for one
// logical run of the known set (P1-9): attempts, registered children,
// terminal state, and failure/abort reason. The ledger is keyed by logical
// run identity — never by RoomID — so two logical runs sharing a room can
// never overwrite each other, and Room close readiness is derived from this
// same ledger.
type logicalRunLedgerEntry struct {
	LogicalRunID  string           `json:"logical_run_id"`
	TaskID        string           `json:"task_id"`
	EpisodeID     string           `json:"episode_id"`
	RoomID        string           `json:"room_id"`
	Attempts      int              `json:"attempts"`
	Children      []agentRunResult `json:"children"`
	TerminalState string           `json:"terminal_state"`
	FailureReason string           `json:"failure_reason,omitempty"`
}

// barrierFactsJSON is the contract-shaped benchmark-barrier output consumed
// by the GMS batch-diagnosis driver (mirror of the GMS integrationtracer
// hostBarrierFacts).
type barrierFactsJSON struct {
	TenantID string `json:"tenant_id"`
	BatchID  string `json:"batch_id"`
	ArmID    string `json:"arm_id"`
	Seed     int64  `json:"seed"`
	Barrier  struct {
		BlockedWhilePending bool `json:"blocked_while_pending"`
		BlockedWhilePartial bool `json:"blocked_while_partial"`
		ReleasedWhenDurable bool `json:"released_when_durable"`
	} `json:"barrier"`
	Admission struct {
		NextEpisodeAdmitted bool     `json:"next_episode_admitted"`
		BlockingOutboxIDs   []string `json:"blocking_outbox_ids"`
		Reason              string   `json:"reason"`
	} `json:"admission"`
	Restart struct {
		RecoveredRows    int      `json:"recovered_rows"`
		StagedRowBlocked bool     `json:"staged_row_blocked_barrier"`
		RecoveredIDs     []string `json:"recovered_ids"`
	} `json:"restart"`
	LogicalRuns []logicalRunFactsJSON `json:"logical_runs"`
}

type logicalRunFactsJSON struct {
	LogicalRunID     string            `json:"logical_run_id"`
	TaskID           string            `json:"task_id"`
	EpisodeID        string            `json:"episode_id"`
	RoomID           string            `json:"room_id"`
	Attempts         int               `json:"attempts"`
	TerminalState    string            `json:"terminal_state"`
	FailureReason    string            `json:"failure_reason,omitempty"`
	RoomEpoch        int64             `json:"room_epoch"`
	RoomClosed       bool              `json:"room_closed"`
	SealedSegmentIDs []string          `json:"sealed_segment_ids"`
	Receipts         []hostReceiptJSON `json:"receipts"`
	Spaces           []hostSpaceJSON   `json:"spaces"`
	Children         []agentRunResult  `json:"children"`
}

// BenchmarkBarrier runs the PG-41 Host half: strict manifest parse over the
// frozen known set, the logical-run identity ledger, all-registered-runs-
// terminal room closing with the receipts guard, the fail-closed evidence
// barrier with a real next-episode admission seam and restart recovery, and
// the barrier facts file the GMS batch-diagnosis driver consumes.
func BenchmarkBarrier(manifestPath, factsOutputPath string) (map[string]any, error) {
	ctx := context.Background()
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	manifest, err := evaluationbatch.ParseBatchManifest(strings.NewReader(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("parse batch manifest: %w", err)
	}
	// Strict parse probe: a duplicate logical run identity is rejected.
	duplicate := strings.Replace(string(raw), `"logical_run_id": "run-3"`, `"logical_run_id": "run-1"`, 1)
	_, errDuplicate := evaluationbatch.ParseBatchManifest(strings.NewReader(duplicate))
	strictParse := errDuplicate != nil

	// agents-run: simulate every expected agent run to a terminal state and
	// record it in the ledger keyed by logical run identity; a failed run is
	// preserved with its error, never dropped.
	ledger := make([]*logicalRunLedgerEntry, 0, len(manifest.LogicalRuns))
	ledgerByID := map[string]*logicalRunLedgerEntry{}
	for _, run := range manifest.LogicalRuns {
		entry := &logicalRunLedgerEntry{
			LogicalRunID: run.LogicalRunID, TaskID: run.TaskID, EpisodeID: run.EpisodeID,
			RoomID: run.RoomID, Attempts: 1, TerminalState: "completed",
		}
		for agentIndex, agentRunID := range run.ExpectedAgentRuns {
			result := agentRunResult{AgentRunID: agentRunID, Status: "succeeded"}
			if run.RoomID == "room-bench-2" && agentIndex == 0 {
				result = agentRunResult{AgentRunID: agentRunID, Status: "failed", Error: "AGENT_TIMEOUT"}
				entry.TerminalState = "failed"
				entry.FailureReason = "AGENT_TIMEOUT"
			}
			entry.Children = append(entry.Children, result)
		}
		if entry.TerminalState == "failed" && entry.FailureReason == "" {
			entry.FailureReason = "UNKNOWN"
		}
		ledger = append(ledger, entry)
		ledgerByID[entry.LogicalRunID] = entry
	}
	failedPreserved := 0
	allTerminal := true
	for _, entry := range ledger {
		if entry.TerminalState == "failed" {
			failedPreserved++
			if entry.FailureReason == "" {
				allTerminal = false
			}
		} else if entry.TerminalState != "completed" {
			allTerminal = false
		}
		for _, child := range entry.Children {
			if child.Status != "succeeded" && child.Status != "failed" {
				allTerminal = false
			}
		}
	}

	// room-close: readiness is derived from the ledger — a room whose
	// registered runs are not all terminal cannot enter closing; once they
	// are, the room closes only after the receipts guard is satisfied
	// (SC-4.5).
	lifecycle := room.NewLifecycle(nil)
	tenantID := domain.TenantID("tenant-pg41")
	roomOrder := []string{}
	runsByRoom := map[string][]*logicalRunLedgerEntry{}
	for _, entry := range ledger {
		if _, seen := runsByRoom[entry.RoomID]; !seen {
			roomOrder = append(roomOrder, entry.RoomID)
		}
		runsByRoom[entry.RoomID] = append(runsByRoom[entry.RoomID], entry)
	}
	roomClose := map[string]struct {
		epoch  int64
		closed bool
	}{}
	nonTerminalRejected := 0
	guardRejected := 0
	closedRooms := 0
	for _, roomID := range roomOrder {
		entries := runsByRoom[roomID]
		registered := []room.RegisteredAgentRun{}
		roomReady := true
		for _, entry := range entries {
			if entry.TerminalState != "completed" && entry.TerminalState != "failed" {
				roomReady = false
			}
			for _, child := range entry.Children {
				registered = append(registered, room.RegisteredAgentRun{
					AgentRunID: child.AgentRunID, Terminal: child.Status != "running",
				})
			}
		}
		if !roomReady {
			continue
		}
		// Probe: with a registered run forced non-terminal, closing is
		// rejected before any state change.
		pending := append([]room.RegisteredAgentRun(nil), registered...)
		pending[0].Terminal = false
		if _, err := lifecycle.EnterClosing(ctx, room.EnterClosingRequest{
			TenantID: tenantID, RoomID: domain.RoomID(roomID), RoomKind: room.BenchmarkRoom, ExpectedEpoch: 5,
			RegisteredAgentRuns: pending, Reason: room.ReasonBarrierReached,
		}); errors.Is(err, room.ErrRegisteredRunsNotTerminal) {
			nonTerminalRejected++
		}
		closing, err := lifecycle.EnterClosing(ctx, room.EnterClosingRequest{
			TenantID: tenantID, RoomID: domain.RoomID(roomID), RoomKind: room.BenchmarkRoom, ExpectedEpoch: 5,
			RegisteredAgentRuns: registered, Reason: room.ReasonBarrierReached,
		})
		if err != nil {
			return nil, fmt.Errorf("enter closing %s: %w", roomID, err)
		}
		if _, err := lifecycle.CloseRoom(ctx, room.CloseRoomRequest{
			TenantID: tenantID, RoomID: domain.RoomID(roomID), ExpectedEpoch: closing.RoomEpoch,
			RegisteredAgentRuns: registered, ReceiptsComplete: false,
			OpenDeliveries: 0, OpenSegments: 0, Reason: room.ReasonBarrierReached,
		}); errors.Is(err, room.ErrCloseGuardNotSatisfied) {
			guardRejected++
		}
		closed, err := lifecycle.CloseRoom(ctx, room.CloseRoomRequest{
			TenantID: tenantID, RoomID: domain.RoomID(roomID), ExpectedEpoch: closing.RoomEpoch,
			RegisteredAgentRuns: registered, ReceiptsComplete: true,
			OpenDeliveries: 0, OpenSegments: 0, Reason: room.ReasonBarrierReached,
		})
		if err != nil {
			return nil, fmt.Errorf("close room %s: %w", roomID, err)
		}
		if closed.State == room.LifecycleClosed {
			closedRooms++
			roomClose[roomID] = struct {
				epoch  int64
				closed bool
			}{epoch: closed.RoomEpoch, closed: true}
		}
	}

	// batch-barrier evidence gate: freeze each room through the coordinator
	// (the same seam as PG-40) and settle one delivery/segment per logical
	// run through the evidence service, creating the room_shared and
	// agent_private outbox rows transactionally. While any row is
	// non-committed the barrier is not reached and next-episode admission
	// stays refused with the blocking outbox IDs.
	freezeStore := &fixtureFreezeStore{
		tenantID: tenantID, epoch: 7, sequence: 42,
		sealed: map[domain.RoomID][]domain.SegmentID{},
		open:   map[domain.RoomID][]domain.SegmentID{},
	}
	for index, entry := range ledger {
		segment := domain.SegmentID(fmt.Sprintf("segment-bench-%d", index+1))
		freezeStore.sealed[domain.RoomID(entry.RoomID)] = append(freezeStore.sealed[domain.RoomID(entry.RoomID)], segment)
	}
	coordinator := hostcut.NewCoordinator(freezeStore)
	store := memorystore.NewStore()
	settler := evidence.NewService(store)
	now := time.Now()
	type runEvidence struct {
		snapshot hostcut.HostFreezeSnapshot
		entries  []domain.EvidenceOutboxEntry
	}
	evidenceByRun := map[string]*runEvidence{}
	outboxOrder := []string{}
	for index, entry := range ledger {
		snapshot, err := coordinator.Freeze(ctx, hostcut.FreezeRequest{
			TenantID: tenantID, RoomID: domain.RoomID(entry.RoomID), Mode: hostcut.ModeForce,
			IdempotencyKey: "freeze-bench-" + entry.LogicalRunID,
		})
		if err != nil {
			return nil, fmt.Errorf("freeze room for logical run %s: %w", entry.LogicalRunID, err)
		}
		sharedBatches, privateBatches := splitProjectionBatches(snapshot.EvidenceReceipts)
		segment := snapshot.SealedSegmentIDs[0]
		entries := []domain.EvidenceOutboxEntry{
			{
				ID: fmt.Sprintf("outbox-bench-%d-shared", index+1), SourceSegmentID: segment,
				Projection: domain.ProjectionRoomShared,
				SpaceID:    domain.SpaceID(fmt.Sprintf("space-bench-%d-shared", index+1)),
				BatchID:    sharedBatches[0], State: "pending", NextAttemptAt: now,
			},
			{
				ID: fmt.Sprintf("outbox-bench-%d-private", index+1), SourceSegmentID: segment,
				Projection: domain.ProjectionAgentPrivate,
				SpaceID:    domain.SpaceID(fmt.Sprintf("space-bench-%d-private", index+1)),
				BatchID:    privateBatches[0], State: "pending", NextAttemptAt: now,
			},
		}
		deliveryID := domain.DeliveryID(fmt.Sprintf("delivery-bench-%d", index+1))
		if err := settler.SettleAndCreateOutbox(ctx, deliveryID, segment, now, entries); err != nil {
			return nil, fmt.Errorf("settle segment for logical run %s: %w", entry.LogicalRunID, err)
		}
		evidenceByRun[entry.LogicalRunID] = &runEvidence{snapshot: snapshot, entries: entries}
		for _, outboxEntry := range entries {
			outboxOrder = append(outboxOrder, outboxEntry.ID)
		}
	}

	barrierReady := func() bool {
		// Non-destructive read (R5): a barrier check must never clear
		// leases, or it would kill a concurrent drain worker's in-flight
		// claim. Lease clearing belongs to the explicit restart recovery
		// sequence alone.
		pending, err := store.Outbox().ListPending(ctx, time.Now())
		if err != nil {
			return false
		}
		return len(pending) == 0 && closedRooms == len(ledger) && allTerminal
	}
	// admissionDecision is the store-backed batch-gate admission authority
	// (R5): AdmitNextEpisode checks outbox durability and records the
	// admission under the same lock settlement writes rows through, so an
	// outbox row created between a check and an admission serializes ahead
	// of the admission and blocks it. The known-set terminality facts live
	// with the ledger and gate the same verdict.
	admissionDecision := func() (bool, []string, string) {
		admitted, blocking, err := store.AdmitNextEpisode(ctx, manifest.EvaluationBatchID)
		if err != nil {
			return false, nil, "ADMISSION_AUTHORITY_ERROR"
		}
		if len(blocking) > 0 {
			return false, blocking, "EVIDENCE_PROJECTIONS_NOT_DURABLE"
		}
		if !admitted {
			return false, nil, "ADMISSION_AUTHORITY_ERROR"
		}
		if closedRooms != len(ledger) || !allTerminal {
			return false, nil, "KNOWN_SET_NOT_TERMINAL"
		}
		return true, nil, "BARRIER_RELEASED"
	}

	blockedWhilePending := !barrierReady()
	admittedWhilePending, blockingWhilePending, reasonWhilePending := admissionDecision()

	// R5 worker-ownership probes on a throwaway store: an unleased worker
	// cannot stage or commit, a stale pre-restart token dies at recovery, a
	// staged row is never re-staged, and a fresh settlement landing between
	// the check and the admission blocks it — and one batch admits once.
	probeStore := memorystore.NewStore()
	probeOutbox := probeStore.Outbox()
	probeEntry := domain.EvidenceOutboxEntry{
		ID: "outbox-probe-1", SourceSegmentID: "segment-probe-1",
		Projection: domain.ProjectionRoomShared, SpaceID: "space-probe", BatchID: "batch-probe-1", State: "pending",
	}
	if err := evidence.NewService(probeStore).SettleAndCreateOutbox(ctx, "delivery-probe-1", probeEntry.SourceSegmentID, time.Now(), []domain.EvidenceOutboxEntry{probeEntry}); err != nil {
		return nil, fmt.Errorf("settle probe row: %w", err)
	}
	errUnleasedStage := probeOutbox.MarkStaged(ctx, ports.OutboxClaim{EntryID: probeEntry.ID, Token: "never-minted"})
	unleasedStageRejected := errors.Is(errUnleasedStage, memorystore.ErrOutboxNotLeased)
	staleClaim, staleEntry, err := probeOutbox.ClaimPending(ctx, time.Now())
	if err != nil {
		return nil, fmt.Errorf("claim probe row: %w", err)
	}
	if _, err := probeOutbox.RecoverPending(ctx, time.Now()); err != nil {
		return nil, fmt.Errorf("recover probe rows: %w", err)
	}
	errStaleStage := probeOutbox.MarkStaged(ctx, staleClaim)
	staleWorkerRejected := errors.Is(errStaleStage, memorystore.ErrOutboxNotLeased) && staleEntry.ID == probeEntry.ID
	freshClaim, _, err := probeOutbox.ClaimPending(ctx, time.Now())
	if err != nil {
		return nil, fmt.Errorf("re-claim probe row after restart: %w", err)
	}
	if err := probeOutbox.MarkStaged(ctx, freshClaim); err != nil {
		return nil, fmt.Errorf("stage probe row: %w", err)
	}
	// The stage lease is retained through the commit (R5 round 4): the same
	// claim that staged the row is the only one that can advance it — a
	// re-claim for the staged row no longer exists, and even the staging
	// worker re-presenting its live token cannot re-stage the durable
	// receipt.
	errRestage := probeOutbox.MarkStaged(ctx, freshClaim)
	restageRefused := errors.Is(errRestage, memorystore.ErrOutboxAlreadyStaged)
	if err := probeOutbox.MarkCommitted(ctx, freshClaim); err != nil {
		return nil, fmt.Errorf("commit staged probe row: %w", err)
	}
	admittedProbe, _, err := probeStore.AdmitNextEpisode(ctx, "probe-batch")
	if err != nil || !admittedProbe {
		return nil, fmt.Errorf("probe admission after drain: admitted=%v err=%v", admittedProbe, err)
	}
	_, _, errDoubleAdmission := probeStore.AdmitNextEpisode(ctx, "probe-batch")
	doubleAdmissionRejected := errors.Is(errDoubleAdmission, memorystore.ErrOutboxDoubleAdmission)
	// check-vs-new-outbox: a settlement landing after a clean check but
	// before the admission serializes under the same lock and blocks it.
	lateEntry := domain.EvidenceOutboxEntry{
		ID: "outbox-probe-2", SourceSegmentID: "segment-probe-2",
		Projection: domain.ProjectionAgentPrivate, SpaceID: "space-probe", BatchID: "batch-probe-2", State: "pending",
	}
	if err := evidence.NewService(probeStore).SettleAndCreateOutbox(ctx, "delivery-probe-2", lateEntry.SourceSegmentID, time.Now(), []domain.EvidenceOutboxEntry{lateEntry}); err != nil {
		return nil, fmt.Errorf("settle late probe row: %w", err)
	}
	admittedLate, blockingLate, _ := probeStore.AdmitNextEpisode(ctx, "probe-batch-late")
	checkVsNewOutboxBlocked := !admittedLate && len(blockingLate) == 1 && blockingLate[0] == lateEntry.ID
	// Two workers claiming concurrently hold distinct single-flight leases:
	// the second claim never hands out the first worker's row.
	thirdEntry := domain.EvidenceOutboxEntry{
		ID: "outbox-probe-3", SourceSegmentID: "segment-probe-3",
		Projection: domain.ProjectionRoomShared, SpaceID: "space-probe", BatchID: "batch-probe-3", State: "pending",
	}
	if err := evidence.NewService(probeStore).SettleAndCreateOutbox(ctx, "delivery-probe-3", thirdEntry.SourceSegmentID, time.Now(), []domain.EvidenceOutboxEntry{thirdEntry}); err != nil {
		return nil, fmt.Errorf("settle third probe row: %w", err)
	}
	firstWorker, firstRow, err := probeOutbox.ClaimPending(ctx, time.Now())
	if err != nil {
		return nil, fmt.Errorf("first worker claim: %w", err)
	}
	secondWorker, secondRow, err := probeOutbox.ClaimPending(ctx, time.Now())
	if err != nil {
		return nil, fmt.Errorf("second worker claim: %w", err)
	}
	doubleWorkerDistinct := firstWorker.Token != secondWorker.Token && firstRow.ID != secondRow.ID
	// A worker's token only ever advances its own row: presenting worker 2's
	// token against worker 1's row is a foreign stage.
	errForeignStage := probeOutbox.MarkStaged(ctx, ports.OutboxClaim{EntryID: firstRow.ID, Token: secondWorker.Token})
	foreignStageRejected := errors.Is(errForeignStage, memorystore.ErrOutboxNotLeased)

	// Crash/restart recovery: one projection worker claims and stages a row,
	// then the process "restarts" before committing. Recovery lists every
	// non-committed row (the staged one included) and kills the crashed
	// worker's lease; a staged-but-uncommitted projection never releases the
	// barrier, and the crashed worker's token can no longer advance the row.
	crashClaim, crashEntry, err := store.Outbox().ClaimPending(ctx, time.Now())
	if err != nil {
		return nil, fmt.Errorf("claim pending for crash simulation: %w", err)
	}
	if err := store.Outbox().MarkStaged(ctx, crashClaim); err != nil {
		return nil, fmt.Errorf("stage before crash: %w", err)
	}
	recoveredAfterRestart, err := store.Outbox().RecoverPending(ctx, time.Now())
	if err != nil {
		return nil, fmt.Errorf("restart recovery: %w", err)
	}
	errCrashedCommit := store.Outbox().MarkCommitted(ctx, crashClaim)
	crashedWorkerRejected := errors.Is(errCrashedCommit, memorystore.ErrOutboxNotLeased)
	recoveredIDs := make([]string, 0, len(recoveredAfterRestart))
	stagedRecovered := false
	for _, entry := range recoveredAfterRestart {
		recoveredIDs = append(recoveredIDs, entry.ID)
		if entry.ID == crashEntry.ID && entry.State == "staged" {
			stagedRecovered = true
		}
	}
	blockedAfterRestart := !barrierReady() && stagedRecovered

	// Drain half of the queue deterministically (the crash row first: its
	// commit is the idempotent retry recovery prescribes); the barrier must
	// stay closed while the other half is still non-committed.
	half := len(outboxOrder) / 2
	committed := 0
	crashRetryClaim, crashRetryEntry, err := store.Outbox().ClaimPending(ctx, time.Now())
	if err != nil {
		return nil, fmt.Errorf("claim crash row for commit-only retry: %w", err)
	}
	if crashRetryEntry.ID != crashEntry.ID || crashRetryEntry.State != "staged" {
		return nil, fmt.Errorf("recovery did not hand back staged crash row %s (got %s/%s)", crashEntry.ID, crashRetryEntry.ID, crashRetryEntry.State)
	}
	if err := store.Outbox().MarkCommitted(ctx, crashRetryClaim); err != nil {
		return nil, fmt.Errorf("commit crash row: %w", err)
	}
	committed++
	for committed < half {
		claim, entry, err := store.Outbox().ClaimPending(ctx, time.Now())
		if errors.Is(err, ports.ErrNoOutboxClaim) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("claim for partial drain: %w", err)
		}
		if entry.State == "staged" {
			if err := store.Outbox().MarkCommitted(ctx, claim); err != nil {
				return nil, fmt.Errorf("commit staged row: %w", err)
			}
			committed++
			continue
		}
		if err := store.Outbox().MarkStaged(ctx, claim); err != nil {
			return nil, fmt.Errorf("mark staged: %w", err)
		}
		// Same-claim commit (R5 round 4): the retained stage lease makes
		// the staging worker the row's only committer.
		if err := store.Outbox().MarkCommitted(ctx, claim); err != nil {
			return nil, fmt.Errorf("mark committed: %w", err)
		}
		committed++
	}
	blockedWhilePartial := !barrierReady()
	admittedWhilePartial, blockingWhilePartial, _ := admissionDecision()

	// Drain the rest through the real claim/stage/commit worker loop.
	for {
		claim, entry, err := store.Outbox().ClaimPending(ctx, time.Now())
		if errors.Is(err, ports.ErrNoOutboxClaim) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("claim pending: %w", err)
		}
		if entry.State == "staged" {
			if err := store.Outbox().MarkCommitted(ctx, claim); err != nil {
				return nil, fmt.Errorf("mark committed: %w", err)
			}
			continue
		}
		if err := store.Outbox().MarkStaged(ctx, claim); err != nil {
			return nil, fmt.Errorf("mark staged: %w", err)
		}
		// Same-claim commit (R5 round 4): the retained stage lease makes
		// the staging worker the row's only committer.
		if err := store.Outbox().MarkCommitted(ctx, claim); err != nil {
			return nil, fmt.Errorf("mark committed: %w", err)
		}
	}
	released := barrierReady()
	admittedReleased, blockingReleased, reasonReleased := admissionDecision()

	// Emit the barrier facts file: the GMS batch-diagnosis driver consumes
	// this plus the frozen manifest and refuses to diagnose when the barrier
	// did not release (P0-2).
	facts := barrierFactsJSON{
		TenantID: string(tenantID), BatchID: manifest.EvaluationBatchID,
		ArmID: manifest.ArmID, Seed: manifest.Seed,
	}
	facts.Barrier.BlockedWhilePending = blockedWhilePending
	facts.Barrier.BlockedWhilePartial = blockedWhilePartial
	facts.Barrier.ReleasedWhenDurable = released
	facts.Admission.NextEpisodeAdmitted = admittedReleased
	facts.Admission.BlockingOutboxIDs = blockingReleased
	facts.Admission.Reason = reasonReleased
	facts.Restart.RecoveredRows = len(recoveredAfterRestart)
	facts.Restart.StagedRowBlocked = blockedAfterRestart
	facts.Restart.RecoveredIDs = recoveredIDs
	for index, entry := range ledger {
		ev := evidenceByRun[entry.LogicalRunID]
		sharedBatches, privateBatches := splitProjectionBatches(ev.snapshot.EvidenceReceipts)
		runFacts := logicalRunFactsJSON{
			LogicalRunID: entry.LogicalRunID, TaskID: entry.TaskID, EpisodeID: entry.EpisodeID,
			RoomID: entry.RoomID, Attempts: entry.Attempts,
			TerminalState: entry.TerminalState, FailureReason: entry.FailureReason,
			RoomEpoch: roomClose[entry.RoomID].epoch, RoomClosed: roomClose[entry.RoomID].closed,
			Children: entry.Children,
			Spaces: []hostSpaceJSON{
				{
					SpaceID: string(ev.entries[0].SpaceID), Scope: "shared", EvidenceBatchIDs: sharedBatches,
					ProjectionHeadVersion: int64(8 + index), QueryWatermark: int64(72 + index),
				},
				{
					SpaceID: string(ev.entries[1].SpaceID), Scope: "private", EvidenceBatchIDs: privateBatches,
					ProjectionHeadVersion: int64(3 + index), QueryWatermark: int64(41 + index),
				},
			},
		}
		for _, segment := range ev.snapshot.SealedSegmentIDs {
			runFacts.SealedSegmentIDs = append(runFacts.SealedSegmentIDs, string(segment))
		}
		for _, receipt := range ev.snapshot.EvidenceReceipts {
			runFacts.Receipts = append(runFacts.Receipts, hostReceiptJSON{
				ReceiptID: string(receipt.ReceiptID), SegmentID: string(receipt.SegmentID),
				BatchIDs: receipt.BatchIDs, BatchDigest: receipt.Digest,
			})
		}
		facts.LogicalRuns = append(facts.LogicalRuns, runFacts)
	}
	encoded, err := json.MarshalIndent(facts, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode barrier facts: %w", err)
	}
	if err := os.WriteFile(factsOutputPath, append(encoded, '\n'), 0o644); err != nil {
		return nil, fmt.Errorf("write barrier facts: %w", err)
	}

	return map[string]any{
		"facts_path": factsOutputPath,
		"manifest": map[string]any{
			"evaluation_batch_id": manifest.EvaluationBatchID,
			"arm_id":              manifest.ArmID,
			"seed":                manifest.Seed,
			"logical_runs":        len(manifest.LogicalRuns),
			"strict_parse":        strictParse,
		},
		"ledger": map[string]any{
			"keyed_by":       "logical_run_id",
			"entries":        len(ledger),
			"all_terminal":   allTerminal,
			"failed":         failedPreserved,
			"failed_dropped": len(ledger) != len(ledgerByID),
		},
		"agents": map[string]any{
			"rooms":                 len(roomOrder),
			"failed_preserved":      failedPreserved > 0,
			"non_terminal_rejected": nonTerminalRejected == len(roomOrder),
		},
		"rooms": map[string]any{
			"closed":         closedRooms,
			"expected":       len(ledger),
			"guard_rejected": guardRejected == len(roomOrder),
		},
		"barrier": map[string]any{
			"blocked_while_pending": blockedWhilePending,
			"blocked_while_partial": blockedWhilePartial,
			"released_when_durable": released,
		},
		"admission": map[string]any{
			"refused_while_pending":       !admittedWhilePending && len(blockingWhilePending) == len(outboxOrder) && reasonWhilePending == "EVIDENCE_PROJECTIONS_NOT_DURABLE",
			"blocking_while_pending":      blockingWhilePending,
			"refused_while_partial":       !admittedWhilePartial && len(blockingWhilePartial) > 0,
			"admitted_when_released":      admittedReleased && len(blockingReleased) == 0 && reasonReleased == "BARRIER_RELEASED",
			"unleased_stage_rejected":     unleasedStageRejected,
			"stale_worker_rejected":       staleWorkerRejected,
			"restage_refused":             restageRefused,
			"double_admission_rejected":   doubleAdmissionRejected,
			"check_vs_new_outbox_blocked": checkVsNewOutboxBlocked,
			"double_worker_distinct":      doubleWorkerDistinct,
			"foreign_stage_rejected":      foreignStageRejected,
			"admission_authority":         "store.AdmitNextEpisode",
		},
		"restart": map[string]any{
			"recovered_rows":          len(recoveredAfterRestart),
			"expected_rows":           len(outboxOrder),
			"staged_blocked":          blockedAfterRestart,
			"crash_row_staged":        crashEntry.ID,
			"crashed_worker_rejected": crashedWorkerRejected,
		},
	}, nil
}
