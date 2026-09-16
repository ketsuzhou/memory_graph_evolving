package memory_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/evidence"
	"river2.dev/pi-group-chat-host/internal/ports"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

func settleRestartRow(t *testing.T, store *memorystore.Store, ctx context.Context, suffix string) domain.EvidenceOutboxEntry {
	t.Helper()
	entry := domain.EvidenceOutboxEntry{
		ID:              "outbox-" + suffix,
		SourceSegmentID: domain.SegmentID("segment-" + suffix),
		Projection:      domain.ProjectionRoomShared,
		SpaceID:         domain.SpaceID("space-" + suffix),
		BatchID:         "batch-" + suffix,
		State:           "pending",
	}
	if err := evidence.NewService(store).SettleAndCreateOutbox(ctx, domain.DeliveryID("delivery-"+suffix), entry.SourceSegmentID, time.Now(), []domain.EvidenceOutboxEntry{entry}); err != nil {
		t.Fatalf("settle row %s: %v", suffix, err)
	}
	return entry
}

// R5 round 3: lease tokens are fenced by a persisted outbox epoch that a
// restore strictly bumps. A token minted in the previous process lifetime can
// never collide with (and validate as) a fresh lease on the same row —
// without the epoch, the restarted claim counter would re-mint the identical
// token and a stale worker could advance a row it no longer owns.
func TestOutboxRestartEpochFencesStaleTokens(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	settleRestartRow(t, store, ctx, "restart-1")
	preRestart, preRow, err := store.Outbox().ClaimPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("pre-restart claim: %v", err)
	}
	image, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := memorystore.NewStore()
	if err := restored.Restore(image); err != nil {
		t.Fatalf("restore: %v", err)
	}
	postRestart, postRow, err := restored.Outbox().ClaimPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("post-restart claim: %v", err)
	}
	if postRestart.Token == preRestart.Token {
		t.Fatalf("restart re-minted the pre-restart token %q for row %s: a stale worker from the previous process lifetime could pass it off as its own lease", postRestart.Token, postRow.ID)
	}
	if postRow.ID != preRow.ID {
		t.Fatalf("post-restart claim handed row %s, want the restored pending row %s", postRow.ID, preRow.ID)
	}
	if err := restored.Outbox().MarkStaged(ctx, preRestart); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
		t.Fatalf("pre-restart token after restore = %v, want ErrOutboxNotLeased", err)
	}
	if err := restored.Outbox().MarkStaged(ctx, postRestart); err != nil {
		t.Fatalf("post-restart owner could not stage its own row: %v", err)
	}
}

// R5 round 3: batch-gate admission decisions are durable. A batch that
// admitted a next episode before a restart is still admitted afterwards —
// re-admitting it must stay a double admission, not a fresh grant.
func TestOutboxAdmissionSurvivesRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	settleRestartRow(t, store, ctx, "admission-1")
	claim, _, err := store.Outbox().ClaimPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Outbox().MarkStaged(ctx, claim); err != nil {
		t.Fatalf("stage: %v", err)
	}
	// Same-claim commit (R5 round 4): the retained stage lease makes the
	// staging worker the row's only committer.
	if err := store.Outbox().MarkCommitted(ctx, claim); err != nil {
		t.Fatalf("commit: %v", err)
	}
	admitted, blocking, err := store.AdmitNextEpisode(ctx, "batch-admission-1")
	if err != nil || !admitted || len(blocking) > 0 {
		t.Fatalf("pre-restart admission = (%v, %v, %v), want admitted", admitted, blocking, err)
	}
	image, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := memorystore.NewStore()
	if err := restored.Restore(image); err != nil {
		t.Fatalf("restore: %v", err)
	}
	_, _, err = restored.AdmitNextEpisode(ctx, "batch-admission-1")
	if !errors.Is(err, memorystore.ErrOutboxDoubleAdmission) {
		t.Fatalf("post-restart re-admission = %v, want ErrOutboxDoubleAdmission", err)
	}
	if log := restored.Admissions(); len(log) != 1 || log[0] != "batch-admission-1" {
		t.Fatalf("admission log did not survive the restart: %v", log)
	}
}

// R5 round 3: ListPending is the non-destructive pending read a concurrent
// actor (barrier check, drain recount) uses: it must not clear a live
// worker's lease. RecoverPending keeps clearing leases — but only as the
// explicit startup-recovery boundary.
func TestOutboxListPendingDoesNotClearLiveLeases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	settleRestartRow(t, store, ctx, "lease-1")
	settleRestartRow(t, store, ctx, "lease-2")
	worker, _, err := store.Outbox().ClaimPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("worker claim: %v", err)
	}
	pending, err := store.Outbox().ListPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("list pending = %d rows, want both non-committed rows", len(pending))
	}
	if err := store.Outbox().MarkStaged(ctx, worker); err != nil {
		t.Fatalf("worker's lease was killed by a concurrent ListPending read: %v", err)
	}

	// Contrast: the startup-recovery boundary clears every lease by design —
	// a worker from the previous process lifetime fails closed afterwards.
	restart := memorystore.NewStore()
	settleRestartRow(t, restart, ctx, "lease-restart")
	stale, _, err := restart.Outbox().ClaimPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("pre-restart claim: %v", err)
	}
	if _, err := restart.Outbox().RecoverPending(ctx, time.Now()); err != nil {
		t.Fatalf("startup recovery: %v", err)
	}
	if err := restart.Outbox().MarkStaged(ctx, stale); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
		t.Fatalf("stale token after startup recovery = %v, want ErrOutboxNotLeased", err)
	}
}

// R5 round 3: two drains running concurrently — each doing the runtime's
// claim→stage→re-claim→commit loop with a non-destructive ListPending recount
// at start and end — must interleave without killing each other's leases:
// every row commits exactly once and neither drain observes a lease failure.
func TestConcurrentDrainsDoNotKillEachOthersLeases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	const rowCount = 6
	rows := make([]string, 0, rowCount)
	for index := 0; index < rowCount; index++ {
		entry := settleRestartRow(t, store, ctx, "drain-"+string(rune('a'+index)))
		rows = append(rows, entry.ID)
	}

	var mu sync.Mutex
	committedByID := map[string]int{}
	var failures []string
	recordCommit := func(id string) {
		mu.Lock()
		defer mu.Unlock()
		committedByID[id]++
	}
	recordFailure := func(drain string, err error) {
		mu.Lock()
		defer mu.Unlock()
		failures = append(failures, drain+": "+err.Error())
	}
	drain := func(name string) {
		if _, err := store.Outbox().ListPending(ctx, time.Now()); err != nil {
			recordFailure(name, err)
			return
		}
		for {
			claim, entry, err := store.Outbox().ClaimPending(ctx, time.Now())
			if errors.Is(err, ports.ErrNoOutboxClaim) {
				break
			}
			if err != nil {
				recordFailure(name, err)
				return
			}
			if entry.State == "staged" {
				if err := store.Outbox().MarkCommitted(ctx, claim); err != nil {
					recordFailure(name, err)
					return
				}
				recordCommit(entry.ID)
				continue
			}
			if err := store.Outbox().MarkStaged(ctx, claim); err != nil {
				recordFailure(name, err)
				return
			}
			// Same-claim commit (R5 round 4): the retained stage lease makes
			// the staging worker the row's only committer, so a concurrent
			// drain can never commit the wrong row.
			if err := store.Outbox().MarkCommitted(ctx, claim); err != nil {
				recordFailure(name, err)
				return
			}
			recordCommit(entry.ID)
		}
		// End-of-drain recount (what drainOutbox reports as its fail-closed
		// remaining count): must be non-destructive or it kills the other
		// drain's in-flight claims.
		if _, err := store.Outbox().ListPending(ctx, time.Now()); err != nil {
			recordFailure(name, err)
		}
	}

	var wg sync.WaitGroup
	for _, name := range []string{"drain-a", "drain-b"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			drain(name)
		}(name)
	}
	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("concurrent drains observed lease failures: %v", failures)
	}
	if len(committedByID) != rowCount {
		t.Fatalf("committed %d distinct rows, want all %d", len(committedByID), rowCount)
	}
	for _, id := range rows {
		if committedByID[id] != 1 {
			t.Fatalf("row %s committed %d times, want exactly once", id, committedByID[id])
		}
	}
	pending, err := store.Outbox().ListPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("final list pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("final pending rows = %d, want 0", len(pending))
	}
}

// R5 round 4: deterministic barrier around the retained stage lease. The
// worker that staged a row holds the only lease that can commit it: a
// concurrent worker claiming another row can never advance the staged row
// with its own token, and the terminal commit releases the lease so the row
// commits exactly once. The only in-process lease release is an explicit
// Reschedule, whose retry path is commit-only. Runs against both
// ports.OutboxStore wirings — the store-backed queue the production runtime
// uses and the standalone durable queue the restart recovery test wires —
// because both must fence identically.
func TestOutboxStageLeaseBarrierDeterministic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	runBarrier := func(t *testing.T, queue ports.OutboxStore, rowStates func() (map[string]string, error)) {
		t.Helper()
		now := time.Now()

		// Worker A claims the first row and records its stage receipt; the
		// stage lease is retained through the commit.
		aClaim, aEntry, err := queue.ClaimPending(ctx, now)
		if err != nil {
			t.Fatalf("worker A claim: %v", err)
		}
		if err := queue.MarkStaged(ctx, aClaim); err != nil {
			t.Fatalf("worker A stage: %v", err)
		}

		// Worker B claims concurrently: the retained stage lease keeps A's
		// row single-flight, so B can only be handed a DIFFERENT row.
		bClaim, bEntry, err := queue.ClaimPending(ctx, now)
		if err != nil {
			t.Fatalf("worker B claim: %v", err)
		}
		if bEntry.ID == aEntry.ID {
			t.Fatalf("worker B re-claimed the staged row %s: the retained stage lease must keep it single-flight", aEntry.ID)
		}
		if bEntry.State != "pending" {
			t.Fatalf("worker B was handed row %s in state %s, want pending", bEntry.ID, bEntry.State)
		}

		// Worker B must not advance A's staged row with its own token.
		if err := queue.MarkStaged(ctx, ports.OutboxClaim{EntryID: aEntry.ID, Token: bClaim.Token}); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
			t.Fatalf("worker B staged A's row %s = %v, want ErrOutboxNotLeased", aEntry.ID, err)
		}
		if err := queue.MarkCommitted(ctx, ports.OutboxClaim{EntryID: aEntry.ID, Token: bClaim.Token}); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
			t.Fatalf("worker B committed A's row %s = %v, want ErrOutboxNotLeased", aEntry.ID, err)
		}

		// Worker A commits its own staged row under the retained lease —
		// exactly once: the terminal commit releases the lease, so the same
		// token can never land a second commit.
		if err := queue.MarkCommitted(ctx, aClaim); err != nil {
			t.Fatalf("worker A commit: %v", err)
		}
		if err := queue.MarkCommitted(ctx, aClaim); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
			t.Fatalf("repeat commit with the released lease = %v, want ErrOutboxNotLeased", err)
		}

		// Reschedule release path: worker B stages its own row, then a
		// failed commit reschedules it — the only in-process release path
		// for a live lease. B's token dies with the reschedule, and the
		// next claimant retries only the idempotent commit.
		if err := queue.MarkStaged(ctx, bClaim); err != nil {
			t.Fatalf("worker B stage: %v", err)
		}
		if err := queue.Reschedule(ctx, bClaim, now.Add(-time.Minute), "commit unavailable"); err != nil {
			t.Fatalf("reschedule staged row: %v", err)
		}
		if err := queue.MarkCommitted(ctx, bClaim); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
			t.Fatalf("released lease still commits %s = %v, want ErrOutboxNotLeased", bEntry.ID, err)
		}
		retryClaim, retryEntry, err := queue.ClaimPending(ctx, now)
		if err != nil {
			t.Fatalf("retry claim: %v", err)
		}
		if retryEntry.ID != bEntry.ID || retryEntry.State != "staged" {
			t.Fatalf("retry claim handed (%s, %s), want the staged row %s", retryEntry.ID, retryEntry.State, bEntry.ID)
		}
		if err := queue.MarkStaged(ctx, retryClaim); !errors.Is(err, memorystore.ErrOutboxAlreadyStaged) {
			t.Fatalf("re-stage after reschedule = %v, want ErrOutboxAlreadyStaged", err)
		}
		if err := queue.MarkCommitted(ctx, retryClaim); err != nil {
			t.Fatalf("commit-only retry: %v", err)
		}

		// Exactly once, no leftover live lease: nothing pending, every row
		// terminal, and the queue hands out no further claims.
		pending, err := queue.ListPending(ctx, now)
		if err != nil {
			t.Fatalf("final list pending: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("final pending rows = %d, want 0", len(pending))
		}
		if _, _, err := queue.ClaimPending(ctx, now); !errors.Is(err, ports.ErrNoOutboxClaim) {
			t.Fatalf("post-drain claim = %v, want ErrNoOutboxClaim (no leftover live lease)", err)
		}
		states, err := rowStates()
		if err != nil {
			t.Fatalf("read row states: %v", err)
		}
		for _, id := range []string{aEntry.ID, bEntry.ID} {
			if states[id] != "committed" {
				t.Fatalf("row %s state = %q, want committed", id, states[id])
			}
		}
	}

	t.Run("store-backed queue", func(t *testing.T) {
		store := memorystore.NewStore()
		first := settleRestartRow(t, store, ctx, "barrier-store-1")
		second := settleRestartRow(t, store, ctx, "barrier-store-2")
		rowStates := func() (map[string]string, error) {
			states := map[string]string{}
			for _, entry := range []domain.EvidenceOutboxEntry{first, second} {
				rows, err := store.OutboxEntriesBySegment(ctx, entry.SourceSegmentID)
				if err != nil {
					return nil, err
				}
				for _, row := range rows {
					states[row.ID] = row.State
				}
			}
			return states, nil
		}
		runBarrier(t, store.Outbox(), rowStates)
	})

	t.Run("standalone durable queue", func(t *testing.T) {
		outbox := memorystore.NewOutbox()
		seed := func(id string) domain.EvidenceOutboxEntry {
			return domain.EvidenceOutboxEntry{
				ID: id, SourceSegmentID: domain.SegmentID("segment-" + id), Projection: domain.ProjectionRoomShared,
				SpaceID: domain.SpaceID("space-" + id), BatchID: "batch-" + id, State: "pending",
			}
		}
		if err := outbox.Adopt([]domain.EvidenceOutboxEntry{seed("outbox-barrier-durable-1"), seed("outbox-barrier-durable-2")}); err != nil {
			t.Fatalf("adopt rows: %v", err)
		}
		rowStates := func() (map[string]string, error) {
			states := map[string]string{}
			for _, row := range outbox.Entries() {
				states[row.Entry.ID] = row.Entry.State
				if row.CommitAttempts != 1 {
					return nil, fmt.Errorf("row %s committed %d times, want exactly once", row.Entry.ID, row.CommitAttempts)
				}
			}
			return states, nil
		}
		runBarrier(t, outbox, rowStates)
	})
}

// R5 round 4: the standalone durable Outbox persists its lease epoch, so a
// restore fences stale tokens exactly like the Store does: a token minted in
// the previous process lifetime can never validate against a row re-claimed
// after the restore bumped the epoch. Live leases are not part of the image —
// the crashed worker's claim dies with the process that minted it.
func TestStandaloneOutboxRestoreEpochFencesStaleTokens(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	outbox := memorystore.NewOutbox()
	entry := domain.EvidenceOutboxEntry{
		ID:              "outbox-standalone-restart",
		SourceSegmentID: domain.SegmentID("segment-standalone-restart"),
		Projection:      domain.ProjectionRoomShared,
		SpaceID:         domain.SpaceID("space-standalone-restart"),
		BatchID:         "batch-standalone-restart",
		State:           "pending",
	}
	if err := outbox.Adopt([]domain.EvidenceOutboxEntry{entry}); err != nil {
		t.Fatalf("adopt into standalone outbox: %v", err)
	}
	preRestart, preRow, err := outbox.ClaimPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("pre-restart claim: %v", err)
	}
	image, err := outbox.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := memorystore.NewOutbox()
	if err := restored.Restore(image); err != nil {
		t.Fatalf("restore: %v", err)
	}
	postRestart, postRow, err := restored.ClaimPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("post-restart claim: %v", err)
	}
	if postRestart.Token == preRestart.Token {
		t.Fatalf("restore re-minted the pre-restart token %q for row %s: a stale worker from the previous process lifetime could pass it off as its own lease", postRestart.Token, postRow.ID)
	}
	if postRow.ID != preRow.ID {
		t.Fatalf("post-restart claim handed row %s, want the restored pending row %s: live leases must not survive the restart", postRow.ID, preRow.ID)
	}
	if err := restored.MarkStaged(ctx, preRestart); !errors.Is(err, memorystore.ErrOutboxNotLeased) {
		t.Fatalf("pre-restart token after restore = %v, want ErrOutboxNotLeased", err)
	}
	if err := restored.MarkStaged(ctx, postRestart); err != nil {
		t.Fatalf("post-restart owner could not stage its own row: %v", err)
	}
}

// R5 round 6: Reconcile is the mandatory evidence-handoff contract on both
// ports.OutboxStore wirings. Rows the queue is missing are inserted; rows
// already present keep their workflow state — a committed row is never
// downgraded by the ledger's stale recorded state; and the store-backed
// queue verifies rows the settlement transaction wrote, failing closed on a
// row the store never held.
func TestOutboxReconcileHandoffContract(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now()

	t.Run("standalone durable queue", func(t *testing.T) {
		outbox := memorystore.NewOutbox()
		seed := func(id string) domain.EvidenceOutboxEntry {
			return domain.EvidenceOutboxEntry{
				ID: id, SourceSegmentID: domain.SegmentID("segment-" + id), Projection: domain.ProjectionRoomShared,
				SpaceID: domain.SpaceID("space-" + id), BatchID: "batch-" + id, State: "pending",
			}
		}
		first := seed("outbox-reconcile-1")
		second := seed("outbox-reconcile-2")
		if err := outbox.Adopt([]domain.EvidenceOutboxEntry{first}); err != nil {
			t.Fatalf("seed queue: %v", err)
		}
		claim, _, err := outbox.ClaimPending(ctx, now)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := outbox.MarkStaged(ctx, claim); err != nil {
			t.Fatalf("stage: %v", err)
		}
		if err := outbox.MarkCommitted(ctx, claim); err != nil {
			t.Fatalf("commit: %v", err)
		}

		// The durable ledger hands both rows: the committed one with its
		// stale recorded state (the dual-store ledger never saw the queue's
		// workflow) and the missing one.
		if err := outbox.Reconcile(ctx, []domain.EvidenceOutboxEntry{first, second}); err != nil {
			t.Fatalf("handoff reconcile: %v", err)
		}
		states := map[string]memorystore.OutboxRowSnapshot{}
		for _, row := range outbox.Entries() {
			states[row.Entry.ID] = row
		}
		if len(states) != 2 {
			t.Fatalf("queue rows after reconcile = %d, want both settled rows", len(states))
		}
		if committed := states[first.ID]; committed.Entry.State != "committed" || committed.CommitAttempts != 1 {
			t.Fatalf("reconcile downgraded the committed row: state=%s commitAttempts=%d, want committed/1 — existing rows keep their workflow state", committed.Entry.State, committed.CommitAttempts)
		}
		if added := states[second.ID]; added.Entry.State != "pending" {
			t.Fatalf("missing row was inserted in state %s, want pending", added.Entry.State)
		}
	})

	t.Run("store-backed queue", func(t *testing.T) {
		store := memorystore.NewStore()
		settleRestartRow(t, store, ctx, "reconcile-store-1")
		entries, err := store.OutboxEntriesBySegment(ctx, domain.SegmentID("segment-reconcile-store-1"))
		if err != nil {
			t.Fatalf("read settled rows: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("settled rows = %d, want 1", len(entries))
		}
		if err := store.Outbox().Reconcile(ctx, entries); err != nil {
			t.Fatalf("store-backed handoff verification failed on rows the settlement transaction wrote: %v", err)
		}
		// A row the store never held is a contract breach: the queue is
		// claiming evidence the settlement never recorded.
		foreign := domain.EvidenceOutboxEntry{
			ID: "outbox-never-settled", SourceSegmentID: domain.SegmentID("segment-foreign"), Projection: domain.ProjectionRoomShared,
			SpaceID: domain.SpaceID("space-foreign"), BatchID: "batch-foreign", State: "pending",
		}
		if err := store.Outbox().Reconcile(ctx, []domain.EvidenceOutboxEntry{foreign}); err == nil {
			t.Fatal("store-backed handoff verified a row the settlement never wrote")
		}
	})
}

// R5 round 6: an exhausted epoch fails closed at restore, before any state
// is installed — a rejected image must leave the receiver untouched, and a
// later valid image must still install cleanly.
func TestStandaloneOutboxRestoreRejectsExhaustedEpoch(t *testing.T) {
	t.Parallel()

	outbox := memorystore.NewOutbox()
	entry := domain.EvidenceOutboxEntry{
		ID: "outbox-epoch-exhausted", SourceSegmentID: domain.SegmentID("segment-epoch-exhausted"),
		Projection: domain.ProjectionRoomShared, SpaceID: domain.SpaceID("space-epoch"), BatchID: "batch-epoch", State: "pending",
	}
	if err := outbox.Adopt([]domain.EvidenceOutboxEntry{entry}); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	image, err := outbox.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var raw struct {
		Epoch int64           `json:"epoch"`
		Rows  json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(image, &raw); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	tampered, err := json.Marshal(struct {
		Epoch int64           `json:"epoch"`
		Rows  json.RawMessage `json:"rows"`
	}{Epoch: math.MaxInt64, Rows: raw.Rows})
	if err != nil {
		t.Fatalf("re-encode exhausted snapshot: %v", err)
	}

	// The untampered image restores fine (sanity: the guard does not reject
	// ordinary restarts).
	healthy := memorystore.NewOutbox()
	if err := healthy.Restore(image); err != nil {
		t.Fatalf("untampered restore: %v", err)
	}
	// The exhausted image is rejected before any state is installed.
	rejected := memorystore.NewOutbox()
	if err := rejected.Restore(tampered); err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("exhausted-epoch restore = %v, want fail-closed exhaustion error", err)
	}
	if rows := rejected.Entries(); len(rows) != 0 {
		t.Fatalf("rejected restore left %d installed rows, want receiver untouched", len(rows))
	}
	// A later valid image still installs cleanly over the untouched
	// receiver.
	if err := rejected.Restore(image); err != nil {
		t.Fatalf("restore after rejection: %v", err)
	}
	if rows := rejected.Entries(); len(rows) != 1 || rows[0].Entry.ID != entry.ID {
		t.Fatalf("post-rejection restore rows = %#v, want the settled row", rows)
	}
}
