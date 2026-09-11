package ledger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
)

// ---------------------------------------------------------------------------
// Test fixtures and helpers
// ---------------------------------------------------------------------------

// loadRegistry loads the digest-verified system reason registry (Contract
// §13.7.1 R2/R5) that every port error must be checked against.
func loadRegistry(t *testing.T) *ContractReasonRegistry {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load system reason policy: %v", err)
	}
	return &ContractReasonRegistry{Policy: policy}
}

func newStoreAndManager(t *testing.T) (*MemoryStore, *Manager) {
	t.Helper()
	registry := loadRegistry(t)
	store, err := NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	mgr, err := NewManager(store, registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return store, mgr
}

// fixtureEvent reads one $FIX/events/<case>/source.json golden payload.
func fixtureEvent(t *testing.T, caseName string) []byte {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "events", caseName, "source.json"))
	if err != nil {
		t.Fatalf("read event fixture %s: %v", caseName, err)
	}
	return data
}

const (
	testOutboxKey = "sha256:eea6884dce88e9d76553b2e7b4c3f8332c33cccf983ba642777664064d08548d"
	testDigestA   = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testDigestB   = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

// proposalEventPayload builds a valid gms.proposal-event.v1 payload whose
// from->to transition is an edge of $FIX schema/state/proposal-lifecycle.
func proposalEventPayload(eventID string, sequence int, from, to string) []byte {
	return []byte(fmt.Sprintf(`{
		"schema_version": "gms.proposal-event.v1",
		"proposal_ref": {"id": "prop-0001", "version": 1, "digest": "sha256:8ebd2236f7b625e092df06845dbf6cb8dd9ca0d467be4e2964737580f243e46a"},
		"event_sequence": %d,
		"event_id": %q,
		"event_digest": "sha256:f5b9847f7d7591481f2bf83c4494058e8b41c0b14def783f9405ae5b31322bb0",
		"from_state": %q,
		"to_state": %q,
		"reason_codes": [],
		"authority_ref": {"id": "gms.admission.v1", "version": 1, "digest": "sha256:29529b790f105302a8627ce789c6050e00c4fe59657bc3b3bfddb8786d1ff476"},
		"record_refs": [],
		"payload_digest": "sha256:fe5d3431c7f34c459490aa01dd4c442962bb10c5724657ac57cc87eb46bbf06e5",
		"expected_previous_event_sequence": %d
	}`, sequence, eventID, from, to, sequence-1))
}

// activationPayloadFor rewrites the golden activation fixture to a given
// sequence/event id: the ledger assigns the sequence at staging time while
// the fixture pins sequence 1.
func activationPayloadFor(payload []byte, sequence int, eventID string) []byte {
	rewritten := strings.Replace(string(payload), `"activation_sequence": 1`, fmt.Sprintf(`"activation_sequence": %d`, sequence), 1)
	rewritten = strings.Replace(rewritten, `"event_id": "act-0001"`, fmt.Sprintf(`"event_id": %q`, eventID), 1)
	return []byte(rewritten)
}

func wantReason(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with reason %s, got nil", code)
	}
	if got := ReasonOf(err); got != code {
		t.Fatalf("expected reason %s, got %q (err: %v)", code, got, err)
	}
}

func stageActivationRelease(tx *Tx, lineage, eventID string, payload []byte) (Entry, error) {
	entry, err := tx.AppendEvent(LedgerActivation, "", eventID, payload)
	if err != nil {
		return Entry{}, err
	}
	if err := tx.CompareAndSwapHead(HeadSkillActive, lineage, entry.Sequence-1, "", entry.Sequence, entry.PayloadDigest); err != nil {
		return entry, err
	}
	err = tx.EnqueueOutbox(OutboxRecord{
		OutboxKey:          testOutboxKey,
		Ledger:             LedgerActivation,
		ActivationSequence: entry.Sequence,
		EventDigest:        entry.PayloadDigest,
		ProjectionTarget:   "runtime",
		DeliveryState:      DeliveryPending,
	})
	return entry, err
}

// ---------------------------------------------------------------------------
// GMS-102 Red test: a transaction that returns an error must leave no ledger
// entry, no head CAS, no outbox record, no content blob and no idempotency
// record behind (Contract §13.1/§13.5, GMS §2.9: no partial version, head,
// event or proposal terminal state may ever be exposed).
// ---------------------------------------------------------------------------

func TestTransactionRollsBackAllLedgerHeadAndOutboxWrites(t *testing.T) {
	store, mgr := newStoreAndManager(t)
	activationPayload := fixtureEvent(t, "event-001-activation")

	var staged Entry
	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		entry, err := stageActivationRelease(tx, "sg-commit-checklist", "act-0001", activationPayload)
		staged = entry
		if err != nil {
			return err
		}
		if _, _, err := tx.RecordIdempotency("release-1", testDigestA, []byte(`"released"`)); err != nil {
			return err
		}
		return errors.New("protected gate failed mid-transaction")
	})
	if err == nil {
		t.Fatal("WithinTx must surface the fn error")
	}
	if !strings.Contains(err.Error(), "protected gate failed mid-transaction") {
		t.Fatalf("WithinTx must return the caller error verbatim, got %v", err)
	}

	// The activation ledger stays empty.
	entries, err := store.Snapshot(LedgerActivation, "")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("rolled-back transaction left %d activation ledger entries, want 0", len(entries))
	}
	// The active head was never written.
	seq, digest, ok, err := store.GetHead(HeadSkillActive, "sg-commit-checklist")
	if err != nil {
		t.Fatalf("GetHead: %v", err)
	}
	if ok || seq != 0 || digest != "" {
		t.Fatalf("rolled-back transaction left an active head (ok=%v seq=%d digest=%s), want absent", ok, seq, digest)
	}
	// The outbox has no pending record.
	pending, err := store.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("rolled-back transaction left %d outbox records, want 0", len(pending))
	}
	// The payload blob never became addressable content.
	if staged.PayloadDigest != "" {
		if _, found, err := store.Get(staged.PayloadDigest); err != nil || found {
			t.Fatalf("rolled-back transaction left its payload in the content store (found=%v err=%v)", found, err)
		}
	}
	// The idempotency key was not consumed.
	if _, found, err := store.Lookup("release-1"); err != nil || found {
		t.Fatalf("rolled-back transaction recorded idempotency key (found=%v err=%v)", found, err)
	}
}

// The happy-path twin: a nil return commits every staged write.
func TestCommitAppliesLedgerHeadOutboxIdempotencyAtomically(t *testing.T) {
	store, mgr := newStoreAndManager(t)
	activationPayload := fixtureEvent(t, "event-001-activation")

	var committed Entry
	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		entry, err := stageActivationRelease(tx, "sg-commit-checklist", "act-0001", activationPayload)
		committed = entry
		if err != nil {
			return err
		}
		replayed, _, err := tx.RecordIdempotency("release-1", testDigestA, []byte(`"released"`))
		if err != nil || replayed {
			return fmt.Errorf("idempotency record: replayed=%v err=%v", replayed, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithinTx: %v", err)
	}

	entries, err := store.Snapshot(LedgerActivation, "")
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected 1 committed activation entry, got %d (err=%v)", len(entries), err)
	}
	if entries[0] != committed {
		t.Fatalf("committed entry %+v does not match staged %+v", entries[0], committed)
	}
	seq, digest, ok, err := store.GetHead(HeadSkillActive, "sg-commit-checklist")
	if err != nil || !ok || seq != 1 || digest != committed.PayloadDigest {
		t.Fatalf("active head not committed as expected (ok=%v seq=%d err=%v)", ok, seq, err)
	}
	pending, err := store.Pending()
	if err != nil || len(pending) != 1 || pending[0].ActivationSequence != 1 {
		t.Fatalf("outbox record not committed (n=%d err=%v)", len(pending), err)
	}
	if blob, found, err := store.Get(committed.PayloadDigest); err != nil || !found || len(blob) == 0 {
		t.Fatalf("payload content not committed (found=%v err=%v)", found, err)
	}
	if rec, found, err := store.Lookup("release-1"); err != nil || !found || string(rec.Outcome) != `"released"` {
		t.Fatalf("idempotency record not committed (found=%v err=%v)", found, err)
	}
}

// Append-only: no mutation API exists; replaying an occupied sequence slot is
// rejected and history cannot be overwritten (Contract §5.1.3, §9.1).
func TestAppendOnlyRejectsHistoryMutationAndDuplicateSequence(t *testing.T) {
	store, mgr := newStoreAndManager(t)

	var first Entry
	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		entry, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-1", proposalEventPayload("pe-1", 1, "none", "proposed"))
		first = entry
		if err != nil {
			return err
		}
		entry2, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-2", proposalEventPayload("pe-2", 2, "proposed", "admitted"))
		if err != nil {
			return err
		}
		if entry2.Sequence != 2 {
			t.Errorf("second append sequence = %d, want 2", entry2.Sequence)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithinTx: %v", err)
	}

	// Replaying the exact same entry is an idempotent no-op (§13.2).
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		return tx.AppendEntry(first, proposalEventPayload("pe-1", 1, "none", "proposed"))
	})
	if err != nil {
		t.Fatalf("identical re-append must be an idempotent no-op, got %v", err)
	}

	// Same slot, different payload: rejected with the ledger's canonical
	// sequence-conflict code.
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		mutated := proposalEventPayload("pe-1-mutated", 1, "none", "proposed")
		digest, err := tx.PutContent(mutated)
		if err != nil {
			return err
		}
		return tx.AppendEntry(Entry{
			Ledger:        LedgerProposal,
			Stream:        "prop-0001",
			Sequence:      1,
			EventID:       "pe-1-mutated",
			PayloadDigest: digest,
		}, mutated)
	})
	wantReason(t, err, "PROPOSAL_STATE_CONFLICT")

	// Overwriting history through the primitive port is equally rejected.
	err = store.Append(Entry{Ledger: LedgerProposal, Stream: "prop-0001", Sequence: 1, EventID: "pe-x", PayloadDigest: testDigestB})
	wantReason(t, err, "PROPOSAL_STATE_CONFLICT")
	// A gap is rejected too (monotone, no reuse: GMS §2.6).
	err = store.Append(Entry{Ledger: LedgerProposal, Stream: "prop-0001", Sequence: 9, EventID: "pe-x", PayloadDigest: testDigestB})
	wantReason(t, err, "PROPOSAL_STATE_CONFLICT")

	entries, err := store.Snapshot(LedgerProposal, "prop-0001")
	if err != nil || len(entries) != 2 || entries[0].EventID != "pe-1" || entries[1].EventID != "pe-2" {
		t.Fatalf("history mutated or lost: %d entries (err=%v)", len(entries), err)
	}
}

// An exact-replay append inside a FAILED transaction must not undo the
// pre-existing entry it replayed: rollback only removes writes made by the
// in-flight commit (Contract §13.5: history is never deleted or rewritten).
func TestReplayedAppendInFailedTxKeepsOriginalEntry(t *testing.T) {
	store, mgr := newStoreAndManager(t)

	var first Entry
	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		entry, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-1", proposalEventPayload("pe-1", 1, "none", "proposed"))
		first = entry
		return err
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		// Exact replay: a committed no-op that must survive rollback.
		if err := tx.AppendEntry(first, proposalEventPayload("pe-1", 1, "none", "proposed")); err != nil {
			return err
		}
		// Second append conflicts at sequence 1 and fails the commit.
		mutated := proposalEventPayload("pe-1-mutated", 1, "none", "proposed")
		digest, err := tx.PutContent(mutated)
		if err != nil {
			return err
		}
		return tx.AppendEntry(Entry{
			Ledger:        LedgerProposal,
			Stream:        "prop-0001",
			Sequence:      1,
			EventID:       "pe-1-mutated",
			PayloadDigest: digest,
		}, mutated)
	})
	wantReason(t, err, "PROPOSAL_STATE_CONFLICT")

	entries, err := store.Snapshot(LedgerProposal, "prop-0001")
	if err != nil || len(entries) != 1 || entries[0].EventID != "pe-1" {
		t.Fatalf("failed transaction removed replayed history: %d entries (err=%v)", len(entries), err)
	}
}

// Idempotency both ways (Contract §13.2, GMS §2.1): same key + same digest
// replays the recorded outcome without new records; same key + different
// digest is IDEMPOTENCY_CONFLICT; a retry that stages new writes fails
// closed instead of producing a second version.
func TestIdempotencySameDigestReplaysAndDifferentDigestConflicts(t *testing.T) {
	store, mgr := newStoreAndManager(t)

	// The mutation body races the merge-group winner head (a sequence-2
	// activation payload would not be reusable across retries).
	run := func(key, digest string, stageWrites bool) (replayed bool, recorded []byte, err error) {
		err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
			// A retry re-executes the whole mutation body first; the store
			// must refuse to double-apply it (Contract §13.2).
			if stageWrites {
				if err := tx.CompareAndSwapHead(HeadMergeGroupWinner, "group-1", 0, "", 1, testDigestA); err != nil {
					return err
				}
			}
			replayed, recorded, err = tx.RecordIdempotency(key, digest, []byte(`"released"`))
			return err
		})
		return
	}

	if replayed, _, err := run("release-1", testDigestA, true); err != nil || replayed {
		t.Fatalf("first execution: replayed=%v err=%v", replayed, err)
	}
	if seq, digest, ok, _ := store.GetHead(HeadMergeGroupWinner, "group-1"); !ok || seq != 1 || digest != testDigestA {
		t.Fatalf("first execution did not commit the group winner head (ok=%v seq=%d)", ok, seq)
	}

	// Same key + same digest: replay of the recorded outcome, no new records.
	replayed, recorded, err := run("release-1", testDigestA, false)
	if err != nil {
		t.Fatalf("replay execution: %v", err)
	}
	if !replayed || string(recorded) != `"released"` {
		t.Fatalf("replay must return the recorded outcome, got replayed=%v recorded=%s", replayed, recorded)
	}
	if seq, _, ok, _ := store.GetHead(HeadMergeGroupWinner, "group-1"); !ok || seq != 1 {
		t.Fatalf("replay moved the group winner head: ok=%v seq=%d", ok, seq)
	}

	// Same key + different digest: IDEMPOTENCY_CONFLICT, nothing applied.
	_, _, err = run("release-1", testDigestB, true)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")
	if seq, digest, _, _ := store.GetHead(HeadMergeGroupWinner, "group-1"); seq != 1 || digest != testDigestA {
		t.Fatalf("conflicting execution changed the head: seq=%d digest=%s", seq, digest)
	}

	// A retry that re-stages writes while replaying fails closed (Retry MUST
	// NOT produce a new version or duplicate activation, GMS §11.3).
	_, _, err = run("release-1", testDigestA, true)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")
	if seq, digest, _, _ := store.GetHead(HeadMergeGroupWinner, "group-1"); seq != 1 || digest != testDigestA {
		t.Fatalf("replayed execution with staged writes changed the head: seq=%d digest=%s", seq, digest)
	}
}

// Stale multi-head CAS (Contract §9.2.2, §13.7, GMS §2.1): wrong expected
// sequence or digest is rejected with the head kind's canonical conflict
// code and leaves no side effects.
func TestStaleCASRejectedWithoutSideEffects(t *testing.T) {
	store, mgr := newStoreAndManager(t)
	activationPayload := fixtureEvent(t, "event-001-activation")

	// Absent head: expected (0, "") matches.
	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := stageActivationRelease(tx, "sg-commit-checklist", "act-0001", activationPayload)
		return err
	})
	if err != nil {
		t.Fatalf("initial activation: %v", err)
	}

	// Stale expectation on the sequence.
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		if _, err := tx.AppendEvent(LedgerActivation, "", "act-0002", activationPayloadFor(activationPayload, 2, "act-0002")); err != nil {
			return err
		}
		return tx.CompareAndSwapHead(HeadSkillActive, "sg-commit-checklist", 0, "", 2, testDigestB)
	})
	wantReason(t, err, "ACTIVE_HEAD_CONFLICT")
	// The staged append was rolled back together with the failed CAS.
	entries, _ := store.Snapshot(LedgerActivation, "")
	if len(entries) != 1 {
		t.Fatalf("stale CAS left %d activation entries, want 1", len(entries))
	}

	// Stale expectation on the digest.
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		return tx.CompareAndSwapHead(HeadSkillActive, "sg-commit-checklist", 1, testDigestA, 2, testDigestB)
	})
	wantReason(t, err, "ACTIVE_HEAD_CONFLICT")

	// Other head kinds map to their canonical conflict codes.
	for _, tc := range []struct {
		kind HeadKind
		code string
	}{
		{HeadMergeSource, "MERGE_SOURCE_HEAD_STALE"},
		{HeadMergeGroupWinner, "MERGE_GROUP_IN_FLIGHT"},
		{HeadLineageVersion, "LINEAGE_VERSION_CONFLICT"},
		{HeadProjection, "PROJECTION_HEAD_CONFLICT"},
		{HeadActivationSequence, "ACTIVATION_SEQUENCE_CONFLICT"},
	} {
		err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
			return tx.CompareAndSwapHead(tc.kind, "k", 7, testDigestA, 8, testDigestB)
		})
		wantReason(t, err, tc.code)
	}

	seq, digest, _, _ := store.GetHead(HeadSkillActive, "sg-commit-checklist")
	if seq != 1 || digest == "" || digest == testDigestB {
		t.Fatalf("active head disturbed by stale CAS attempts: seq=%d digest=%s", seq, digest)
	}
}

// Partial outbox (GMS §2.6/§2.9): an outbox record whose activation
// sequence/event digest does not resolve to a committed or same-transaction
// ledger entry rejects the whole commit.
func TestPartialOutboxRejectedRollsBackCommit(t *testing.T) {
	store, mgr := newStoreAndManager(t)
	activationPayload := fixtureEvent(t, "event-001-activation")

	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		entry, err := tx.AppendEvent(LedgerActivation, "", "act-0001", activationPayload)
		if err != nil {
			return err
		}
		return tx.EnqueueOutbox(OutboxRecord{
			OutboxKey:          testOutboxKey,
			Ledger:             LedgerActivation,
			ActivationSequence: entry.Sequence + 5, // never appended anywhere
			EventDigest:        entry.PayloadDigest,
			ProjectionTarget:   "runtime",
			DeliveryState:      DeliveryPending,
		})
	})
	wantReason(t, err, "ACTIVATION_EVENT_INVALID")

	entries, _ := store.Snapshot(LedgerActivation, "")
	if len(entries) != 0 {
		t.Fatalf("partial-outbox commit left %d activation entries, want 0", len(entries))
	}
	pending, _ := store.Pending()
	if len(pending) != 0 {
		t.Fatalf("partial-outbox commit left %d outbox records, want 0", len(pending))
	}
}

// Concurrency: two transactions race the same head; exactly one wins
// (exact-CAS, GMS §2.1). Run under -race.
func TestConcurrentTransactionsOnSameHeadExactlyOneWins(t *testing.T) {
	store, mgr := newStoreAndManager(t)
	activationPayload := fixtureEvent(t, "event-001-activation")

	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := stageActivationRelease(tx, "sg-commit-checklist", "act-0001", activationPayload)
		return err
	})
	if err != nil {
		t.Fatalf("seed activation: %v", err)
	}

	var wg sync.WaitGroup
	var successes, conflicts int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eventID := fmt.Sprintf("act-race-%d", i)
			err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
				entry, err := tx.AppendEvent(LedgerActivation, "", eventID, activationPayloadFor(activationPayload, 2, eventID))
				if err != nil {
					return err
				}
				return tx.CompareAndSwapHead(HeadSkillActive, "sg-commit-checklist", 1, mustHeadDigest(t, store), entry.Sequence, entry.PayloadDigest)
			})
			switch {
			case err == nil:
				atomic.AddInt32(&successes, 1)
			case ReasonOf(err) == "ACTIVE_HEAD_CONFLICT":
				atomic.AddInt32(&conflicts, 1)
			case ReasonOf(err) == "REF_MISMATCH":
				// When the winner's entry commits first, the loser's append
				// is rejected by the chained payload-sequence check before
				// it ever reaches the CAS — an equally fail-closed way to
				// lose the race.
				atomic.AddInt32(&conflicts, 1)
			default:
				t.Errorf("unexpected race error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if successes != 1 || conflicts != 1 {
		t.Fatalf("race outcome: successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
	entries, _ := store.Snapshot(LedgerActivation, "")
	if len(entries) != 2 {
		t.Fatalf("ledger has %d activation entries, want exactly 2 (seed + single winner)", len(entries))
	}
	if seq, _, ok, _ := store.GetHead(HeadSkillActive, "sg-commit-checklist"); !ok || seq != 2 {
		t.Fatalf("active head seq=%d ok=%v, want 2", seq, ok)
	}
}

func mustHeadDigest(t *testing.T, store *MemoryStore) string {
	t.Helper()
	_, digest, ok, err := store.GetHead(HeadSkillActive, "sg-commit-checklist")
	if err != nil || !ok || digest == "" {
		t.Fatalf("seed head missing: ok=%v err=%v", ok, err)
	}
	return digest
}

// Unknown ledger kinds and payload schema versions are rejected fail-closed
// (Contract §16.6: the schema files are the machine-readable authority).
func TestUnknownLedgerKindAndPayloadSchemaRejected(t *testing.T) {
	store, mgr := newStoreAndManager(t)

	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := tx.AppendEvent(LedgerKind("bogus"), "", "x-1", proposalEventPayload("x-1", 1, "none", "proposed"))
		return err
	})
	wantReason(t, err, "SCHEMA_ENUM_INVALID")

	err = store.Append(Entry{Ledger: LedgerKind("bogus"), Sequence: 1, EventID: "x-1", PayloadDigest: testDigestA})
	wantReason(t, err, "SCHEMA_ENUM_INVALID")

	// A proposal payload does not belong to the activation ledger.
	activationPayload := fixtureEvent(t, "event-001-activation")
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := tx.AppendEvent(LedgerProposal, "prop-0001", "act-0001", activationPayload)
		return err
	})
	wantReason(t, err, "SCHEMA_VERSION_UNSUPPORTED")

	// A payload with a schema version unknown to every ledger.
	unknownSchema := []byte(`{"schema_version":"gms.unknown.v9","event_id":"x"}`)
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := tx.AppendEvent(LedgerProposal, "prop-0001", "x-1", unknownSchema)
		return err
	})
	wantReason(t, err, "SCHEMA_VERSION_UNSUPPORTED")

	// The activation ledger is a single global stream (GMS §2.6).
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := tx.AppendEvent(LedgerActivation, "stream-x", "act-0001", activationPayload)
		return err
	})
	wantReason(t, err, "SCHEMA_ENUM_INVALID")
}

// Event payloads with illegal state transitions are rejected at staging time
// (proposal-event schema description: every transition MUST be an edge of
// schema/state/proposal-lifecycle.schema.json, ILLEGAL_STATE_TRANSITION
// otherwise).
func TestIllegalStateTransitionRejected(t *testing.T) {
	store, mgr := newStoreAndManager(t)

	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-1", proposalEventPayload("pe-1", 1, "proposed", "released"))
		return err
	})
	wantReason(t, err, "ILLEGAL_STATE_TRANSITION")

	// Terminal states never reopen (Contract §9.1).
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-1", proposalEventPayload("pe-1", 1, "released", "proposed"))
		return err
	})
	wantReason(t, err, "ILLEGAL_STATE_TRANSITION")

	entries, _ := store.Snapshot(LedgerProposal, "prop-0001")
	if len(entries) != 0 {
		t.Fatalf("illegal transition left %d entries, want 0", len(entries))
	}

	// The merge event twin uses the merge lifecycle table.
	mergePayload := fixtureEvent(t, "event-005-merge-proposal-event")
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		_, err := tx.AppendEvent(LedgerMergeEvent, "mp-0001", "mpe-0001", mergePayload)
		return err
	})
	if err != nil {
		t.Fatalf("legal merge genesis event rejected: %v", err)
	}
}

// A commit whose durable apply phase fails midway leaves no partial records:
// the manager compensates every already-applied write (the "snapshot persist
// failure reported as success" illusion is structurally impossible).
func TestCommitFailureLeavesNoPartialRecords(t *testing.T) {
	registry := loadRegistry(t)
	base, err := NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	flaky := &failingAppendStore{MemoryStore: base, allowedAppends: 1}
	mgr, err := NewManager(flaky, registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	payloadA := proposalEventPayload("pe-1", 1, "none", "proposed")
	payloadB := proposalEventPayload("pe-2", 2, "proposed", "admitted")
	var digests []string
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		e1, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-1", payloadA)
		if err != nil {
			return err
		}
		digests = append(digests, e1.PayloadDigest)
		e2, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-2", payloadB)
		if err != nil {
			return err
		}
		digests = append(digests, e2.PayloadDigest)
		return nil
	})
	if err == nil {
		t.Fatal("commit must surface the adapter failure")
	}

	entries, _ := flaky.Snapshot(LedgerProposal, "prop-0001")
	if len(entries) != 0 {
		t.Fatalf("failed commit left %d ledger entries, want 0", len(entries))
	}
	for _, d := range digests {
		if _, found, _ := flaky.Get(d); found {
			t.Fatalf("failed commit left content blob %s behind", d)
		}
	}
}

// failingAppendStore lets the first N appends through, then fails.
type failingAppendStore struct {
	*MemoryStore
	allowedAppends int32
}

func (f *failingAppendStore) Append(entry Entry) error {
	if atomic.AddInt32(&f.allowedAppends, -1) < 0 {
		return errors.New("simulated durable-append I/O failure")
	}
	return f.MemoryStore.Append(entry)
}

// Outbox delivery (GMS §2.6): delivery may retry but never mints a new
// ActivationEvent; the same outbox key with a different digest conflicts.
func TestOutboxDeliveryIdempotentAndDigestBound(t *testing.T) {
	store, mgr := newStoreAndManager(t)
	activationPayload := fixtureEvent(t, "event-001-activation")

	var entry Entry
	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		e, err := stageActivationRelease(tx, "sg-commit-checklist", "act-0001", activationPayload)
		entry = e
		return err
	})
	if err != nil {
		t.Fatalf("WithinTx: %v", err)
	}

	pending, err := store.Pending()
	if err != nil || len(pending) != 1 {
		t.Fatalf("Pending: n=%d err=%v", len(pending), err)
	}

	// Wrong digest on delivery is rejected.
	err = store.MarkDelivered(testOutboxKey, entry.Sequence, testDigestB)
	wantReason(t, err, "DIGEST_MISMATCH")

	// Correct delivery, then idempotent re-delivery.
	if err := store.MarkDelivered(testOutboxKey, entry.Sequence, entry.PayloadDigest); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if err := store.MarkDelivered(testOutboxKey, entry.Sequence, entry.PayloadDigest); err != nil {
		t.Fatalf("re-delivery must be idempotent, got %v", err)
	}
	pending, err = store.Pending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("delivered record still pending: n=%d err=%v", len(pending), err)
	}
	// The outbox never duplicated the ledger event.
	entries, _ := store.Snapshot(LedgerActivation, "")
	if len(entries) != 1 {
		t.Fatalf("outbox delivery minted extra activation events: %d", len(entries))
	}

	// Re-enqueueing the same key with a different digest conflicts.
	err = mgr.WithinTx(context.Background(), func(tx *Tx) error {
		return tx.EnqueueOutbox(OutboxRecord{
			OutboxKey:          testOutboxKey,
			Ledger:             LedgerActivation,
			ActivationSequence: entry.Sequence,
			EventDigest:        testDigestB,
			ProjectionTarget:   "runtime",
			DeliveryState:      DeliveryPending,
		})
	})
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")
}

// Reads hand out deep copies: callers cannot mutate authoritative state
// through a returned slice or byte buffer.
func TestReadsReturnDeepCopies(t *testing.T) {
	store, mgr := newStoreAndManager(t)

	var first Entry
	err := mgr.WithinTx(context.Background(), func(tx *Tx) error {
		entry, err := tx.AppendEvent(LedgerProposal, "prop-0001", "pe-1", proposalEventPayload("pe-1", 1, "none", "proposed"))
		first = entry
		return err
	})
	if err != nil {
		t.Fatalf("WithinTx: %v", err)
	}

	entries, _ := store.Snapshot(LedgerProposal, "prop-0001")
	entries[0].EventID = "tampered"
	entries[0].Sequence = 99
	again, _ := store.Snapshot(LedgerProposal, "prop-0001")
	if again[0].EventID != "pe-1" || again[0].Sequence != 1 {
		t.Fatalf("Snapshot is not a deep copy: %+v", again[0])
	}

	blob, _, _ := store.Get(first.PayloadDigest)
	blob[0] = 'x'
	blob2, _, _ := store.Get(first.PayloadDigest)
	if blob2[0] == 'x' {
		t.Fatal("ContentStore.Get is not a deep copy")
	}
}

// Single authority: the Go transition tables must equal the machines frozen
// in $FIX/schema/state (Contract §16.6 F1/F4), including the genesis edge
// none -> initial state that the event DTOs use for creation events.
func TestStateTablesMatchConformanceSchemas(t *testing.T) {
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	cases := []struct {
		file    string
		machine string
		table   map[string]map[string]bool
	}{
		{"proposal-lifecycle.schema.json", "skill-proposal", ProposalEventTransitions},
		{"merge-lifecycle.schema.json", "merge-proposal", MergeEventTransitions},
	}
	for _, tc := range cases {
		raw, err := os.ReadFile(filepath.Join(dir, "schema", "state", tc.file))
		if err != nil {
			t.Fatalf("read %s: %v", tc.file, err)
		}
		doc, err := contract.ParseJSONStrict(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", tc.file, err)
		}
		obj, _ := contract.AsObject(doc)
		machines, _ := contract.AsArray(obj["machines"])
		for _, m := range machines {
			machine, _ := contract.AsObject(m)
			id, _ := contract.AsString(machine["machine_id"])
			if id != tc.machine {
				continue
			}
			want := map[string]map[string]bool{}
			initials, _ := contract.AsArray(machine["initial_states"])
			for _, rawInitial := range initials {
				initial, _ := contract.AsString(rawInitial)
				wantEdge(want, "none", initial)
			}
			transitions, _ := contract.AsArray(machine["transitions"])
			for _, rawTr := range transitions {
				tr, _ := contract.AsObject(rawTr)
				from, _ := contract.AsString(tr["from"])
				to, _ := contract.AsString(tr["to"])
				wantEdge(want, from, to)
			}
			if !sameEdges(want, tc.table) {
				t.Errorf("%s/%s: Go table diverges from the frozen schema:\nwant %v\ngot  %v",
					tc.file, tc.machine, flatten(want), flatten(tc.table))
			}
		}
	}
}

func wantEdge(table map[string]map[string]bool, from, to string) {
	if table[from] == nil {
		table[from] = map[string]bool{}
	}
	table[from][to] = true
}

func sameEdges(a, b map[string]map[string]bool) bool {
	fa, fb := flatten(a), flatten(b)
	if len(fa) != len(fb) {
		return false
	}
	set := map[string]bool{}
	for _, e := range fa {
		set[e] = true
	}
	for _, e := range fb {
		if !set[e] {
			return false
		}
	}
	return true
}

func flatten(table map[string]map[string]bool) []string {
	var out []string
	for from, targets := range table {
		for to := range targets {
			out = append(out, from+" -> "+to)
		}
	}
	return out
}

// Every reason code the ports can emit exists in the digest-verified system
// registry (Contract §13.7.1 R5: unknown codes fail closed, never guessed).
func TestMappedReasonCodesExistInRegistry(t *testing.T) {
	registry := loadRegistry(t)
	for _, kind := range AllLedgerKinds() {
		spec, ok := LedgerSpec(kind)
		if !ok {
			t.Fatalf("LedgerSpec(%s) missing", kind)
		}
		if err := registry.Verify(spec.SequenceConflictCode); err != nil {
			t.Errorf("ledger %s sequence code: %v", kind, err)
		}
	}
	for _, kind := range AllHeadKinds() {
		spec, ok := HeadSpec(kind)
		if !ok {
			t.Fatalf("HeadSpec(%s) missing", kind)
		}
		if err := registry.Verify(spec.ConflictCode); err != nil {
			t.Errorf("head %s conflict code: %v", kind, err)
		}
	}
	for _, code := range []string{
		ReasonIdempotencyConflict, ReasonActivationEventInvalid,
		ReasonDigestMismatch, ReasonRefMismatch, ReasonIllegalStateTransition,
		ReasonSchemaEnumInvalid, ReasonSchemaVersionUnsupported,
		ReasonRequiredFieldMissing, ReasonInvalidJSON,
	} {
		if err := registry.Verify(code); err != nil {
			t.Errorf("code %s: %v", code, err)
		}
	}
}
