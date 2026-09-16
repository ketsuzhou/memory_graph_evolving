package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

// releaseFailingOutbox wraps a working outbox queue and fails every
// Reschedule, simulating a backend that cannot record a failed row's retry
// or release its lease.
type releaseFailingOutbox struct {
	ports.OutboxStore
}

func (o *releaseFailingOutbox) Reschedule(context.Context, ports.OutboxClaim, time.Time, string) error {
	return errors.New("outbox backend unavailable: retry not recorded")
}

// adoptFailingOutbox wraps the standalone durable queue and fails every
// Reconcile handoff, simulating a worker queue that never receives the
// settled segment's evidence rows.
type adoptFailingOutbox struct {
	*memorystore.Outbox
}

func (o *adoptFailingOutbox) Reconcile(_ context.Context, entries []domain.EvidenceOutboxEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return errors.New("queue handoff unavailable")
}

// newWorkingMemoryStub serves a Graph Memory stub that accepts every stage
// and commit, for restart-reconciliation flows that must reach a durable
// committed state.
func newWorkingMemoryStub(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":stage") {
			var body struct{ BatchID, IdempotencyKey, SpaceID string }
			_ = json.NewDecoder(r.Body).Decode(&body)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"batch_id": body.BatchID, "idempotency_key": body.IdempotencyKey, "space_id": body.SpaceID, "state": "staged", "duplicate": false})
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":commit") {
			var body struct {
				CommitID string `json:"commit_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(map[string]any{"batch_id": "batch-recovered", "commit_id": body.CommitID, "state": "committed", "memory_version": 1, "committed_at": "2026-09-14T12:01:00Z", "duplicate": false})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	return server
}

// R5 round 5: a drain whose row fails at the remote stage must release the
// claim's lease for a retry. When the release itself fails, the drain error
// must carry BOTH root causes — the remote failure and the un-released lease
// — and the row stays single-flight-leased until a restart boundary: no
// in-process caller can re-claim it, so the failure is never papered over
// as a green partial drain.
func TestDrainFailsClosedWhenLeaseReleaseFails(t *testing.T) {
	t.Parallel()

	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"code":"INTERNAL"}}`, http.StatusInternalServerError)
	}))
	defer memory.Close()

	store := memorystore.NewStore()
	dependencies := Dependencies{
		Clock:         fixedClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)},
		IDs:           &deterministicIDs{},
		RoomStore:     store,
		DeliveryStore: store,
		DAGStore:      store,
		OutboxStore:   &releaseFailingOutbox{OutboxStore: store.Outbox()},
	}
	scenario := RecoveryScenario{
		Authority:     ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", SharedSpaceID: "space-shared", PrivateSpaceID: "space-agent-a"},
		Messages:      []VisibleMessage{{ID: "human-message-001", Sequence: 1, AuthorID: "human-a", Content: "resume this", IdempotencyKey: "human-op-001"}},
		Segments:      []SegmentRecord{{ID: "segment-previous", DeliveryID: "delivery-previous", State: "settled"}},
		Outbox:        []OutboxRecord{{ID: "outbox-shared-previous", SourceSegmentID: "segment-previous", Projection: "room_shared", SpaceID: "space-shared", BatchID: "batch-shared-previous", State: "pending"}},
		MemoryBaseURL: memory.URL, MemoryAuthToken: "test-memory-token",
		PromptRequestID: "prompt-none",
	}

	_, err := NewOrchestrator(dependencies).RecoverAfterRestart(t.Context(), scenario)
	if err == nil {
		t.Fatal("drain reported success while the row's lease could not be released for retry")
	}
	for _, fragment := range []string{"stage batch", "releasing the row's lease for retry also failed", "outbox backend unavailable"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("drain error hides the %s root cause: %v", fragment, err)
		}
	}
	// The unreleased lease pins the row in-flight: a same-process retry
	// cannot claim it, which is exactly why the release failure must
	// surface to the caller instead of being swallowed.
	pending, err := store.Outbox().ListPending(t.Context(), time.Now())
	if err != nil {
		t.Fatalf("list pending after failed release: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "outbox-shared-previous" {
		t.Fatalf("pending rows after failed release = %#v, want the leased row", pending)
	}
	if _, _, err := store.Outbox().ClaimPending(t.Context(), time.Now()); !errors.Is(err, ports.ErrNoOutboxClaim) {
		t.Fatalf("claim after failed release = %v, want ErrNoOutboxClaim: the live lease must keep the row single-flight until a restart boundary", err)
	}
}

// R5 round 5: with a standalone durable outbox wiring, segment settlement
// in the store is only half the handoff — the worker queue must receive the
// rows. A failed Adopt must block the recovery: otherwise the store says
// settled, the queue never sees the evidence, and every later drain
// green-lights on silently missing receipts.
func TestRecoveryFailsClosedWhenOutboxHandoffFails(t *testing.T) {
	t.Parallel()

	store := memorystore.NewStore()
	queue := &adoptFailingOutbox{Outbox: memorystore.NewOutbox()}
	dependencies := Dependencies{
		Clock:         fixedClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)},
		IDs:           &deterministicIDs{},
		RoomStore:     store,
		DeliveryStore: store,
		DAGStore:      store,
		OutboxStore:   queue,
	}
	scenario := RecoveryScenario{
		Authority:     ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", SharedSpaceID: "space-shared", PrivateSpaceID: "space-agent-a"},
		Messages:      []VisibleMessage{{ID: "human-message-001", Sequence: 1, AuthorID: "human-a", Content: "resume this", IdempotencyKey: "human-op-001"}},
		Segments:      []SegmentRecord{{ID: "segment-previous", DeliveryID: "delivery-previous", State: "settled"}},
		Outbox:        []OutboxRecord{{ID: "outbox-shared-previous", SourceSegmentID: "segment-previous", Projection: "room_shared", SpaceID: "space-shared", BatchID: "batch-shared-previous", State: "pending"}},
		MemoryBaseURL: "http://127.0.0.1:1", MemoryAuthToken: "test-memory-token",
		PromptRequestID: "prompt-none",
	}

	_, err := NewOrchestrator(dependencies).RecoverAfterRestart(t.Context(), scenario)
	if err == nil {
		t.Fatal("recovery completed while the evidence handoff to the worker queue failed")
	}
	for _, fragment := range []string{"recover segment segment-previous", "hand evidence outbox entries to the worker queue", "queue handoff unavailable"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("recovery error hides the %s root cause: %v", fragment, err)
		}
	}
	if rows := queue.Entries(); len(rows) != 0 {
		t.Fatalf("worker queue received %d rows from a failed handoff, want 0", len(rows))
	}
}

// R5 round 6: a settlement whose handoff failed is a durable half-handoff —
// the store recorded the rows as settled, the queue never received them.
// A later run must refuse to report success while the handoff is still
// broken: the restart boundary re-hands the settled ledger to the queue,
// and a queue that still refuses it fails closed instead of letting a
// drain read an empty queue and green-light over silently missing
// evidence.
func TestLaterRunsRefuseWhileOutboxHandoffIsBroken(t *testing.T) {
	t.Parallel()

	store := memorystore.NewStore()
	brokenQueue := &adoptFailingOutbox{Outbox: memorystore.NewOutbox()}
	dependencies := Dependencies{
		Clock:         fixedClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)},
		IDs:           &deterministicIDs{},
		RoomStore:     store,
		DeliveryStore: store,
		DAGStore:      store,
		OutboxStore:   brokenQueue,
	}
	scenario := RecoveryScenario{
		Authority:     ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", SharedSpaceID: "space-shared", PrivateSpaceID: "space-agent-a"},
		Messages:      []VisibleMessage{{ID: "human-message-001", Sequence: 1, AuthorID: "human-a", Content: "resume this", IdempotencyKey: "human-op-001"}},
		Segments:      []SegmentRecord{{ID: "segment-previous", DeliveryID: "delivery-previous", State: "settled"}},
		Outbox:        []OutboxRecord{{ID: "outbox-shared-previous", SourceSegmentID: "segment-previous", Projection: "room_shared", SpaceID: "space-shared", BatchID: "batch-shared-previous", State: "pending"}},
		MemoryBaseURL: "http://127.0.0.1:1", MemoryAuthToken: "test-memory-token",
		PromptRequestID: "prompt-none",
	}
	if _, err := NewOrchestrator(dependencies).RecoverAfterRestart(t.Context(), scenario); err == nil {
		t.Fatal("first recovery with a broken queue handoff must fail")
	}

	// The half-handoff state: the store's ledger holds the settled rows, the
	// queue holds none of them. A later run must not report success.
	_, err := NewOrchestrator(dependencies).RecoverAfterRestart(t.Context(), scenario)
	if err == nil {
		t.Fatal("later run reported success over settled evidence the worker queue never received")
	}
	if !strings.Contains(err.Error(), "reconcile settled evidence into the worker queue") {
		t.Fatalf("later run hides the handoff root cause: %v", err)
	}
	if rows := brokenQueue.Entries(); len(rows) != 0 {
		t.Fatalf("broken queue received %d rows, want 0", len(rows))
	}
	ledger, err := store.OutboxEntries(t.Context())
	if err != nil {
		t.Fatalf("read settled evidence ledger: %v", err)
	}
	if len(ledger) != 1 || ledger[0].ID != "outbox-shared-previous" {
		t.Fatalf("settled evidence ledger = %#v, want the half-handed row intact (evidence must not be lost)", ledger)
	}
}

// R5 round 6: the store's settled rows are the durable evidence ledger, so a
// half-handoff is recoverable: after a settlement whose queue handoff
// failed, a restart with a healthy worker queue re-hands the ledger at the
// startup boundary and the drain completes — the evidence is committed, not
// lost, and no settled row is silently dropped.
func TestRestartReconcilesPendingHandoff(t *testing.T) {
	t.Parallel()

	memory := newWorkingMemoryStub(t)
	store := memorystore.NewStore()
	brokenQueue := &adoptFailingOutbox{Outbox: memorystore.NewOutbox()}
	dependencies := Dependencies{
		Clock:         fixedClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)},
		IDs:           &deterministicIDs{},
		RoomStore:     store,
		DeliveryStore: store,
		DAGStore:      store,
		OutboxStore:   brokenQueue,
	}
	pi := writeFakePi(t, map[string][]string{
		"prompt-recovered": ordinaryPiFrames("prompt-recovered", "reply-recovered", "human-message-001", "Recovered exactly once."),
	})
	scenario := RecoveryScenario{
		Authority:     ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", SharedSpaceID: "space-shared", PrivateSpaceID: "space-agent-a"},
		Messages:      []VisibleMessage{{ID: "human-message-001", Sequence: 1, AuthorID: "human-a", Content: "resume this", IdempotencyKey: "human-op-001"}},
		Deliveries:    []DeliveryRecord{{ID: "delivery-001", AgentID: "agent-a", MessageID: "human-message-001", State: "pending", Attempts: 1}},
		MemoryBaseURL: memory.URL, MemoryAuthToken: "test-memory-token",
		PiBinary: pi, PromptRequestID: "prompt-recovered",
	}
	if _, err := NewOrchestrator(dependencies).RecoverAfterRestart(t.Context(), scenario); err == nil {
		t.Fatal("first recovery with a broken queue handoff must fail")
	}
	ledger, err := store.OutboxEntries(t.Context())
	if err != nil {
		t.Fatalf("read settled evidence ledger: %v", err)
	}
	if len(ledger) != 2 {
		t.Fatalf("settled evidence ledger = %d rows, want the settled turn's room-shared and agent-private pair", len(ledger))
	}

	// Restart with a healthy worker queue over the same durable store: the
	// startup sweep re-hands the whole ledger, and the drain commits the
	// previously half-handed evidence.
	healthyQueue := memorystore.NewOutbox()
	restarted := NewOrchestrator(Dependencies{
		Clock:     fixedClock{now: time.Date(2026, 9, 14, 12, 1, 0, 0, time.UTC)},
		IDs:       &deterministicIDs{},
		RoomStore: store, DeliveryStore: store, DAGStore: store,
		OutboxStore: healthyQueue,
	})
	result, err := restarted.RecoverAfterRestart(t.Context(), RecoveryScenario{
		Authority:     ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", SharedSpaceID: "space-shared", PrivateSpaceID: "space-agent-a"},
		MemoryBaseURL: memory.URL, MemoryAuthToken: "test-memory-token",
		PromptRequestID: "prompt-none",
	})
	if err != nil {
		t.Fatalf("restart with a healthy queue failed to reconcile the pending handoff: %v", err)
	}
	if len(result.MemoryVisibleBatchIDs) != 2 {
		t.Fatalf("restart committed batches = %v, want both settled batches recovered from the pending handoff", result.MemoryVisibleBatchIDs)
	}
	for _, row := range healthyQueue.Entries() {
		if row.Entry.State != "committed" {
			t.Fatalf("reconciled row %s state = %s, want committed", row.Entry.ID, row.Entry.State)
		}
	}
}

// R5 round 5: the turn settlement path hands the same settled evidence to
// the worker queue, so a failed handoff must fail the turn — the recovered
// delivery reports an error instead of the settlement being treated as
// complete with its evidence silently missing from the queue.
func TestTurnSettlementFailsClosedWhenOutboxHandoffFails(t *testing.T) {
	t.Parallel()

	pi := writeFakePi(t, map[string][]string{
		"prompt-recovered": ordinaryPiFrames("prompt-recovered", "reply-recovered", "human-message-001", "Recovered exactly once."),
	})
	store := memorystore.NewStore()
	queue := &adoptFailingOutbox{Outbox: memorystore.NewOutbox()}
	dependencies := Dependencies{
		Clock:         fixedClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)},
		IDs:           &deterministicIDs{},
		RoomStore:     store,
		DeliveryStore: store,
		DAGStore:      store,
		OutboxStore:   queue,
	}
	scenario := RecoveryScenario{
		Authority:     ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", SharedSpaceID: "space-shared", PrivateSpaceID: "space-agent-a"},
		Messages:      []VisibleMessage{{ID: "human-message-001", Sequence: 1, AuthorID: "human-a", Content: "resume this", IdempotencyKey: "human-op-001"}},
		Deliveries:    []DeliveryRecord{{ID: "delivery-001", AgentID: "agent-a", MessageID: "human-message-001", State: "pending", Attempts: 1}},
		MemoryBaseURL: "http://127.0.0.1:1", MemoryAuthToken: "test-memory-token",
		PiBinary: pi, PromptRequestID: "prompt-recovered",
	}

	_, err := NewOrchestrator(dependencies).RecoverAfterRestart(t.Context(), scenario)
	if err == nil {
		t.Fatal("recovered turn completed while the evidence handoff to the worker queue failed")
	}
	for _, fragment := range []string{"recover delivery delivery-001", "hand evidence outbox entries to the worker queue", "queue handoff unavailable"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("recovery error hides the %s root cause: %v", fragment, err)
		}
	}
	if rows := queue.Entries(); len(rows) != 0 {
		t.Fatalf("worker queue received %d rows from a failed handoff, want 0", len(rows))
	}
}
