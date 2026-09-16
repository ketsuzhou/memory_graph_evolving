package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/evidence"
	"river2.dev/pi-group-chat-host/internal/ports"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

func TestCommitRetryPreservesStagedReceipt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	outbox := store.Outbox()
	retryAt := time.Date(2026, time.September, 14, 12, 1, 0, 0, time.UTC)
	entry := domain.EvidenceOutboxEntry{
		ID:              "outbox-1",
		SourceSegmentID: "segment-1",
		Projection:      domain.ProjectionRoomShared,
		SpaceID:         "space-room-shared",
		BatchID:         "batch-1",
		State:           "pending",
	}
	service := evidence.NewService(store)
	if err := service.SettleAndCreateOutbox(ctx, "delivery-1", entry.SourceSegmentID, retryAt.Add(-time.Minute), []domain.EvidenceOutboxEntry{entry}); err != nil {
		t.Fatalf("settle segment and create outbox: %v", err)
	}
	claim, _, err := outbox.ClaimPending(ctx, retryAt)
	if err != nil {
		t.Fatalf("claim outbox row: %v", err)
	}
	if err := outbox.MarkStaged(ctx, claim); err != nil {
		t.Fatalf("record stage receipt: %v", err)
	}

	// A commit failure schedules another commit attempt. It must not erase the
	// durable stage receipt, otherwise recovery re-stages an already staged
	// batch instead of retrying only the idempotent commit. The stage lease
	// is retained through the commit (R5 round 4), so the same claim
	// reschedules its own staged row.
	if err := outbox.Reschedule(ctx, claim, retryAt, "commit unavailable"); err != nil {
		t.Fatalf("reschedule commit: %v", err)
	}
	recovered, err := outbox.RecoverPending(ctx, retryAt)
	if err != nil {
		t.Fatalf("recover outbox: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("recovered %d entries, want 1", len(recovered))
	}
	if recovered[0].State != "staged" {
		t.Fatalf("recovered state = %q, want staged receipt preserved", recovered[0].State)
	}
}

func TestStoreOutboxMarkStagedRefusesCommittedDowngrade(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	outbox := store.Outbox()
	entry := domain.EvidenceOutboxEntry{
		ID: "outbox-committed", SourceSegmentID: "segment-committed",
		Projection: domain.ProjectionRoomShared, SpaceID: "space-room-shared", BatchID: "batch-committed", State: "pending",
	}
	if err := evidence.NewService(store).SettleAndCreateOutbox(ctx, "delivery-committed", entry.SourceSegmentID, time.Now().UTC(), []domain.EvidenceOutboxEntry{entry}); err != nil {
		t.Fatalf("settle segment and create outbox: %v", err)
	}
	stageClaim, _, err := outbox.ClaimPending(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim for staging: %v", err)
	}
	if err := outbox.MarkStaged(ctx, stageClaim); err != nil {
		t.Fatalf("mark staged: %v", err)
	}
	// The stage lease is retained through the commit (R5 round 4): the same
	// claim commits the row it staged.
	if err := outbox.MarkCommitted(ctx, stageClaim); err != nil {
		t.Fatalf("mark committed: %v", err)
	}
	// The stale stage claim must neither downgrade the committed row nor
	// pass the lease check.
	if err := outbox.MarkStaged(ctx, stageClaim); err == nil {
		t.Fatal("MarkStaged downgraded a committed durable outbox entry")
	}
	pending, err := outbox.RecoverPending(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("recover after refused downgrade: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("committed entry became recoverable after refused downgrade: %#v", pending)
	}
}

// R5: every stage/commit/reschedule must present the lease token ClaimPending
// minted. A worker that never claimed, a stale pre-restart worker, and a
// foreign worker's token all fail closed, and a staged row is only ever
// committed — never re-staged.
func TestOutboxLeaseOwnershipFailsClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	outbox := store.Outbox()
	entry := domain.EvidenceOutboxEntry{
		ID: "outbox-lease", SourceSegmentID: "segment-lease",
		Projection: domain.ProjectionAgentPrivate, SpaceID: "space-private", BatchID: "batch-lease", State: "pending",
	}
	if err := evidence.NewService(store).SettleAndCreateOutbox(ctx, "delivery-lease", entry.SourceSegmentID, time.Now().UTC(), []domain.EvidenceOutboxEntry{entry}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// An unleased worker cannot stage or commit a pending row.
	if err := outbox.MarkStaged(ctx, ports.OutboxClaim{EntryID: entry.ID, Token: "never-minted"}); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
		t.Fatalf("unleased stage = %v, want ErrOutboxNotLeased", err)
	}
	claim, _, err := outbox.ClaimPending(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// A foreign token (another worker's row) does not advance this row.
	if err := outbox.MarkStaged(ctx, ports.OutboxClaim{EntryID: entry.ID, Token: claim.Token + "-forged"}); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
		t.Fatalf("forged-token stage = %v, want ErrOutboxNotLeased", err)
	}
	// Commit requires the durable stage receipt first.
	if err := outbox.MarkCommitted(ctx, claim); !errors.Is(err, memorystore.ErrOutboxStageReceiptRequired) {
		t.Fatalf("commit without stage receipt = %v, want ErrOutboxStageReceiptRequired", err)
	}
	if err := outbox.MarkStaged(ctx, claim); err != nil {
		t.Fatalf("stage: %v", err)
	}
	// Recovery is the restart boundary: it lists the staged row and kills the
	// crashed worker's lease.
	if _, err := outbox.RecoverPending(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if err := outbox.MarkCommitted(ctx, claim); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
		t.Fatalf("stale pre-restart commit = %v, want ErrOutboxNotLeased", err)
	}
	// A fresh claim hands back the staged row; re-staging is refused and the
	// commit-only retry lands.
	retryClaim, retryEntry, err := outbox.ClaimPending(ctx, time.Now().UTC())
	if err != nil || retryEntry.ID != entry.ID || retryEntry.State != "staged" {
		t.Fatalf("re-claim after restart = (%#v, %v), want staged row %s", retryEntry, err, entry.ID)
	}
	if err := outbox.MarkStaged(ctx, retryClaim); !errors.Is(err, memorystore.ErrOutboxAlreadyStaged) {
		t.Fatalf("re-stage = %v, want ErrOutboxAlreadyStaged", err)
	}
	if err := outbox.MarkCommitted(ctx, retryClaim); err != nil {
		t.Fatalf("commit-only retry: %v", err)
	}
	pending, err := outbox.RecoverPending(ctx, time.Now().UTC())
	if err != nil || len(pending) != 0 {
		t.Fatalf("recover after commit = (%d, %v), want drained", len(pending), err)
	}
}

// R5: two workers hold distinct single-flight leases, and the batch-gate
// admission authority admits one batch exactly once — a settlement landing
// after a clean check but before the admission blocks it under the same lock.
func TestOutboxSingleFlightAndAdmissionAuthority(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	outbox := store.Outbox()
	settle := func(id, segment, batch string) {
		t.Helper()
		entry := domain.EvidenceOutboxEntry{
			ID: id, SourceSegmentID: domain.SegmentID(segment), Projection: domain.ProjectionRoomShared,
			SpaceID: "space-shared", BatchID: batch, State: "pending",
		}
		if err := evidence.NewService(store).SettleAndCreateOutbox(ctx, domain.DeliveryID("delivery-"+id), entry.SourceSegmentID, time.Now().UTC(), []domain.EvidenceOutboxEntry{entry}); err != nil {
			t.Fatalf("settle %s: %v", id, err)
		}
	}
	settle("outbox-a", "segment-a", "batch-a")
	settle("outbox-b", "segment-b", "batch-b")
	first, firstRow, err := outbox.ClaimPending(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	second, secondRow, err := outbox.ClaimPending(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if firstRow.ID == secondRow.ID || first.Token == second.Token {
		t.Fatalf("single-flight handed one row to two workers: %#v / %#v", firstRow, secondRow)
	}
	// The admission authority blocks while rows are non-committed.
	admitted, blocking, err := store.AdmitNextEpisode(ctx, "batch-gate")
	if err != nil || admitted || len(blocking) != 2 {
		t.Fatalf("admission while pending = (%v, %v, %v), want refused with 2 blocking", admitted, blocking, err)
	}
	drain := func(claim ports.OutboxClaim, entry domain.EvidenceOutboxEntry) {
		t.Helper()
		if entry.State == "pending" {
			if err := outbox.MarkStaged(ctx, claim); err != nil {
				t.Fatalf("stage %s: %v", entry.ID, err)
			}
		}
		// Same-claim commit (R5 round 4): the retained stage lease makes the
		// staging worker the row's only committer.
		if err := outbox.MarkCommitted(ctx, claim); err != nil {
			t.Fatalf("commit %s: %v", entry.ID, err)
		}
	}
	drain(first, firstRow)
	drain(second, secondRow)
	// A settlement landing between the (implicit) clean check and the
	// admission serializes under the same lock and blocks the gate.
	settle("outbox-late", "segment-late", "batch-late")
	admitted, blocking, err = store.AdmitNextEpisode(ctx, "batch-gate")
	if err != nil || admitted || len(blocking) != 1 || blocking[0] != "outbox-late" {
		t.Fatalf("admission with late row = (%v, %v, %v), want refused on outbox-late", admitted, blocking, err)
	}
	lateClaim, lateRow, err := outbox.ClaimPending(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim late row: %v", err)
	}
	drain(lateClaim, lateRow)
	admitted, blocking, err = store.AdmitNextEpisode(ctx, "batch-gate")
	if err != nil || !admitted || len(blocking) != 0 {
		t.Fatalf("admission after drain = (%v, %v, %v), want admitted", admitted, blocking, err)
	}
	// One batch admits exactly once.
	if _, _, err := store.AdmitNextEpisode(ctx, "batch-gate"); !errors.Is(err, memorystore.ErrOutboxDoubleAdmission) {
		t.Fatalf("double admission = %v, want ErrOutboxDoubleAdmission", err)
	}
	if admissions := store.Admissions(); len(admissions) != 1 || admissions[0] != "batch-gate" {
		t.Fatalf("admission log = %v, want [batch-gate]", admissions)
	}
}
