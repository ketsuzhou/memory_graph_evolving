package evidence_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/store/memory"
)

type fakeClock struct {
	now time.Time
}

func (c fakeClock) Now() time.Time {
	return c.now
}

type evidenceFixture struct {
	ctx       context.Context
	clock     fakeClock
	store     *memory.Store
	service   *evidence.Service
	tenantID  domain.TenantID
	principal domain.PrincipalID
	shared    domain.SpaceID
	private   domain.SpaceID
}

type protocolError interface {
	error
	StatusCode() int
	Code() string
}

func requireConflict(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded, want 409 conflict")
	}
	var typed protocolError
	if !errors.As(err, &typed) {
		t.Fatalf("error %T (%v) does not expose protocol status and code", err, err)
	}
	if typed.StatusCode() != 409 || typed.Code() != wantCode {
		t.Fatalf("conflict = (%d, %q), want (409, %q)", typed.StatusCode(), typed.Code(), wantCode)
	}
}

func newEvidenceFixture(t *testing.T) evidenceFixture {
	t.Helper()

	ctx := context.Background()
	clock := fakeClock{now: time.Date(2026, time.September, 8, 11, 0, 0, 123456789, time.UTC)}
	store := memory.New()
	tenantID := domain.TenantID("tenant-tracer")
	principalID := domain.PrincipalID("host-service")
	ownerID := domain.PrincipalID("agent-owner")
	sharedID := domain.SpaceID("room-shared")
	privateID := domain.SpaceID("agent-private")

	created, err := store.InitializeTenant(ctx, domain.Tenant{
		ID:                   tenantID,
		DisplayName:          "Tracer tenant",
		BootstrapPrincipalID: principalID,
	}, domain.Principal{
		ID:          principalID,
		TenantID:    tenantID,
		Kind:        domain.PrincipalService,
		DisplayName: "Host service",
	})
	if err != nil {
		t.Fatalf("initialize tenant: %v", err)
	}
	if !created {
		t.Fatal("first tenant initialization was not reported as created")
	}

	created, err = store.PutPrincipal(ctx, tenantID, domain.Principal{
		ID:          ownerID,
		TenantID:    tenantID,
		Kind:        domain.PrincipalAgent,
		DisplayName: "Owning agent",
	})
	if err != nil {
		t.Fatalf("put private-space owner: %v", err)
	}
	if !created {
		t.Fatal("first principal registration was not reported as created")
	}

	owner := ownerID
	spaces := []domain.Space{
		{
			ID:          sharedID,
			TenantID:    tenantID,
			Scope:       domain.SpaceShared,
			DisplayName: "Room shared",
		},
		{
			ID:               privateID,
			TenantID:         tenantID,
			Scope:            domain.SpacePrivate,
			OwnerPrincipalID: &owner,
			DisplayName:      "Agent private",
		},
	}
	for _, space := range spaces {
		created, err = store.PutSpace(ctx, tenantID, space)
		if err != nil {
			t.Fatalf("put space %q: %v", space.ID, err)
		}
		if !created {
			t.Fatalf("first registration of space %q was not reported as created", space.ID)
		}
	}

	created, err = store.PutGrant(ctx, tenantID, domain.Grant{
		ID:          "host-lifecycle",
		TenantID:    tenantID,
		PrincipalID: principalID,
		SpaceIDs:    []domain.SpaceID{sharedID, privateID},
		Purpose:     domain.GrantPurposeLifecycle,
		Operations: []domain.GrantOperation{
			domain.GrantOperationEvidenceStage,
			domain.GrantOperationEvidenceCommit,
			domain.GrantOperationRecall,
		},
		ExpiresAt: clock.now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("put lifecycle grant: %v", err)
	}
	if !created {
		t.Fatal("first grant registration was not reported as created")
	}

	return evidenceFixture{
		ctx:       ctx,
		clock:     clock,
		store:     store,
		service:   evidence.New(store, store, clock),
		tenantID:  tenantID,
		principal: principalID,
		shared:    sharedID,
		private:   privateID,
	}
}

func (f evidenceFixture) batch(id, key string, spaceID domain.SpaceID, sourceKind, content string) domain.EvidenceBatch {
	return domain.EvidenceBatch{
		ID:              domain.BatchID(id),
		TenantID:        f.tenantID,
		IdempotencyKey:  key,
		SpaceID:         spaceID,
		StreamID:        "stream-" + string(spaceID),
		SourceSegmentID: "opaque-segment-1",
		Provenance: domain.Provenance{
			HostType:       "pi-group-chat-host",
			HostInstanceID: "host-instance-1",
			SourceKind:     sourceKind,
			CapturedAt:     f.clock.now.Add(-2 * time.Minute),
			ContentSHA256:  strings.Repeat("a", 64),
		},
		Events: []domain.EvidenceEvent{
			{
				ID:         "event-1",
				Sequence:   1,
				Kind:       "room_message",
				Content:    content,
				OccurredAt: f.clock.now.Add(-time.Minute),
			},
		},
		Links:           []domain.EvidenceLink{},
		TerminalOutcome: "settled",
	}
}

func cloneBatch(batch domain.EvidenceBatch) domain.EvidenceBatch {
	cloned := batch
	cloned.Events = append([]domain.EvidenceEvent(nil), batch.Events...)
	cloned.Links = append([]domain.EvidenceLink(nil), batch.Links...)
	return cloned
}

func requireVersion(t *testing.T, batch domain.EvidenceBatch, want int64) {
	t.Helper()
	if batch.MemoryVersion == nil {
		t.Fatalf("batch %q has nil memory version, want %d", batch.ID, want)
	}
	if got := *batch.MemoryVersion; got != want {
		t.Fatalf("batch %q memory version = %d, want %d", batch.ID, got, want)
	}
}

func TestStageAndCommitAreSemanticallyIdempotent(t *testing.T) {
	f := newEvidenceFixture(t)
	candidate := f.batch("batch-shared", "outbox-shared", f.shared, "room_shared", "The launch is Friday.")

	staged, duplicate, err := f.service.Stage(f.ctx, f.tenantID, f.principal, candidate)
	if err != nil {
		t.Fatalf("first stage: %v", err)
	}
	if duplicate {
		t.Fatal("first stage was reported as a duplicate")
	}
	if staged.State != domain.EvidenceBatchStaged {
		t.Fatalf("first stage state = %q, want %q", staged.State, domain.EvidenceBatchStaged)
	}

	// A separately allocated but semantically identical decoded request must replay.
	retryInput := cloneBatch(candidate)
	retriedStage, duplicate, err := f.service.Stage(f.ctx, f.tenantID, f.principal, retryInput)
	if err != nil {
		t.Fatalf("identical stage retry: %v", err)
	}
	if !duplicate {
		t.Fatal("identical stage retry was not reported as a duplicate")
	}
	if !reflect.DeepEqual(retriedStage, staged) {
		t.Fatalf("stage retry changed the stored batch:\nfirst: %#v\nretry: %#v", staged, retriedStage)
	}

	committed, duplicate, err := f.service.Commit(f.ctx, f.tenantID, f.principal, candidate.ID, "commit-1")
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if duplicate {
		t.Fatal("first commit was reported as a duplicate")
	}
	if committed.State != domain.EvidenceBatchCommitted {
		t.Fatalf("first commit state = %q, want %q", committed.State, domain.EvidenceBatchCommitted)
	}
	requireVersion(t, committed, 1)
	if committed.CommittedAt == nil || !committed.CommittedAt.Equal(f.clock.now) {
		t.Fatalf("committed_at = %v, want deterministic clock time %v", committed.CommittedAt, f.clock.now)
	}

	for _, commitID := range []string{"commit-1", "commit-2"} {
		replayed, replayDuplicate, replayErr := f.service.Commit(f.ctx, f.tenantID, f.principal, candidate.ID, commitID)
		if replayErr != nil {
			t.Fatalf("commit replay %q: %v", commitID, replayErr)
		}
		if !replayDuplicate {
			t.Errorf("commit replay %q was not reported as a duplicate", commitID)
		}
		if !reflect.DeepEqual(replayed, committed) {
			t.Errorf("commit replay %q changed immutable result:\nfirst: %#v\nreplay: %#v", commitID, committed, replayed)
		}
	}

	replayedStage, duplicate, err := f.service.Stage(f.ctx, f.tenantID, f.principal, cloneBatch(candidate))
	if err != nil {
		t.Fatalf("stage replay after commit: %v", err)
	}
	if !duplicate {
		t.Fatal("stage replay after commit was not reported as a duplicate")
	}
	if !reflect.DeepEqual(replayedStage, committed) {
		t.Fatalf("stage replay after commit did not return immutable committed resource:\ncommit: %#v\nreplay: %#v", committed, replayedStage)
	}
}

func TestIdempotencyConflictFailsClosed(t *testing.T) {
	f := newEvidenceFixture(t)
	original := f.batch("batch-original", "same-outbox-key", f.shared, "room_shared", "Original first-turn fact.")

	staged, duplicate, err := f.service.Stage(f.ctx, f.tenantID, f.principal, original)
	if err != nil {
		t.Fatalf("stage original: %v", err)
	}
	if duplicate {
		t.Fatal("first stage was reported as a duplicate")
	}

	conflict := cloneBatch(original)
	conflict.Events[0].Content = "Conflicting replacement fact."
	_, _, conflictErr := f.service.Stage(f.ctx, f.tenantID, f.principal, conflict)
	requireConflict(t, conflictErr, "IDEMPOTENCY_CONFLICT")

	stored, err := f.service.Batch(f.ctx, f.tenantID, f.principal, original.ID)
	if err != nil {
		t.Fatalf("read original after conflict: %v", err)
	}
	if !reflect.DeepEqual(stored, staged) {
		t.Fatalf("conflict replaced or changed original batch:\noriginal: %#v\nstored: %#v", staged, stored)
	}

	items, err := f.store.Recall(f.ctx, f.tenantID, []domain.PinnedSpace{{SpaceID: f.shared, MemoryVersion: 0}}, "fact", 10)
	if err != nil {
		t.Fatalf("recall after rejected conflict: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("rejected conflict made %d recall items visible, want none", len(items))
	}

	replayed, duplicate, err := f.service.Stage(f.ctx, f.tenantID, f.principal, cloneBatch(original))
	if err != nil {
		t.Fatalf("identical retry after conflict: %v", err)
	}
	if !duplicate || !reflect.DeepEqual(replayed, staged) {
		t.Fatalf("conflict poisoned original idempotency record: duplicate=%v replay=%#v original=%#v", duplicate, replayed, staged)
	}
}

type failOnceEvidenceStore struct {
	*memory.Store
	commitErr error
}

func (s *failOnceEvidenceStore) Commit(ctx context.Context, tenantID domain.TenantID, batchID domain.BatchID, commitID string, committedAt time.Time) (domain.EvidenceBatch, bool, error) {
	if s.commitErr != nil {
		err := s.commitErr
		s.commitErr = nil
		return domain.EvidenceBatch{}, false, err
	}
	return s.Store.Commit(ctx, tenantID, batchID, commitID, committedAt)
}

func TestStageIsInvisibleAndCommitPublishesAtomically(t *testing.T) {
	f := newEvidenceFixture(t)
	failure := errors.New("injected failure before commit swap")
	failingStore := &failOnceEvidenceStore{Store: f.store, commitErr: failure}
	service := evidence.New(f.store, failingStore, f.clock)
	batch := f.batch("batch-atomic", "outbox-atomic", f.shared, "room_shared", "Atomic event one.")
	batch.Events = append(batch.Events, domain.EvidenceEvent{
		ID:         "event-2",
		Sequence:   2,
		Kind:       "room_message",
		Content:    "Atomic event two.",
		OccurredAt: f.clock.now.Add(-30 * time.Second),
	})

	if _, _, err := service.Stage(f.ctx, f.tenantID, f.principal, batch); err != nil {
		t.Fatalf("stage batch: %v", err)
	}
	assertNoVisibleEvidence(t, f, "after stage")

	if _, _, err := service.Commit(f.ctx, f.tenantID, f.principal, batch.ID, "commit-atomic"); !errors.Is(err, failure) {
		t.Fatalf("failed commit error = %v, want injected %v", err, failure)
	}
	assertNoVisibleEvidence(t, f, "after failed commit")

	stillStaged, err := f.store.Batch(f.ctx, f.tenantID, batch.ID)
	if err != nil {
		t.Fatalf("read batch after failed commit: %v", err)
	}
	if stillStaged.State != domain.EvidenceBatchStaged || stillStaged.MemoryVersion != nil || stillStaged.CommittedAt != nil {
		t.Fatalf("failed commit partially changed batch: %#v", stillStaged)
	}
	space, err := f.store.Space(f.ctx, f.tenantID, f.shared)
	if err != nil {
		t.Fatalf("read space after failed commit: %v", err)
	}
	if space.Version != 0 {
		t.Fatalf("failed commit advanced space version to %d, want 0", space.Version)
	}

	committed, duplicate, err := service.Commit(f.ctx, f.tenantID, f.principal, batch.ID, "commit-atomic")
	if err != nil {
		t.Fatalf("commit after injected failure: %v", err)
	}
	if duplicate {
		t.Fatal("first successful commit was reported as duplicate")
	}
	requireVersion(t, committed, 1)

	space, err = f.store.Space(f.ctx, f.tenantID, f.shared)
	if err != nil {
		t.Fatalf("read space after commit: %v", err)
	}
	if space.Version != 1 {
		t.Fatalf("successful commit advanced space version to %d, want exactly 1", space.Version)
	}

	items, err := f.store.Recall(f.ctx, f.tenantID, []domain.PinnedSpace{{SpaceID: f.shared, MemoryVersion: 1}}, "Atomic", 10)
	if err != nil {
		t.Fatalf("recall after successful commit: %v", err)
	}
	if len(items) != len(batch.Events) {
		t.Fatalf("atomic commit exposed %d items, want all %d events", len(items), len(batch.Events))
	}
	seenEvents := make(map[string]bool)
	for _, item := range items {
		if item.SourceSpaceID != f.shared {
			t.Errorf("item source space = %q, want %q", item.SourceSpaceID, f.shared)
		}
		if item.MemoryVersion != 1 {
			t.Errorf("item memory version = %d, want 1", item.MemoryVersion)
		}
		if item.Citation.EvidenceBatchID != batch.ID {
			t.Errorf("item cites batch %q, want %q", item.Citation.EvidenceBatchID, batch.ID)
		}
		for _, eventID := range item.Citation.EventIDs {
			seenEvents[eventID] = true
		}
	}
	for _, event := range batch.Events {
		if !seenEvents[event.ID] {
			t.Errorf("committed event %q is missing from recall citations", event.ID)
		}
	}
}

func assertNoVisibleEvidence(t *testing.T, f evidenceFixture, when string) {
	t.Helper()
	items, err := f.store.Recall(f.ctx, f.tenantID, []domain.PinnedSpace{{SpaceID: f.shared, MemoryVersion: 0}}, "Atomic", 10)
	if err != nil {
		t.Fatalf("recall %s: %v", when, err)
	}
	if len(items) != 0 {
		t.Fatalf("%s, recall returned %d items, want none", when, len(items))
	}
}

func TestBatchIDIsGloballyUniqueWithinTenant(t *testing.T) {
	f := newEvidenceFixture(t)
	shared := f.batch("globally-unique-batch", "shared-outbox", f.shared, "room_shared", "Shared fact.")
	stagedShared, _, err := f.service.Stage(f.ctx, f.tenantID, f.principal, shared)
	if err != nil {
		t.Fatalf("stage shared batch: %v", err)
	}

	privateReuse := f.batch("globally-unique-batch", "private-outbox", f.private, "agent_private", "Private fact.")
	_, _, conflictErr := f.service.Stage(f.ctx, f.tenantID, f.principal, privateReuse)
	requireConflict(t, conflictErr, "IDEMPOTENCY_CONFLICT")

	stored, err := f.service.Batch(f.ctx, f.tenantID, f.principal, shared.ID)
	if err != nil {
		t.Fatalf("read original after batch_id conflict: %v", err)
	}
	if !reflect.DeepEqual(stored, stagedShared) {
		t.Fatalf("batch_id conflict replaced original:\noriginal: %#v\nstored: %#v", stagedShared, stored)
	}

	privateReuse.ID = "private-unique-batch"
	stagedPrivate, duplicate, err := f.service.Stage(f.ctx, f.tenantID, f.principal, privateReuse)
	if err != nil {
		t.Fatalf("stage private batch with unique ID after conflict: %v", err)
	}
	if duplicate || stagedPrivate.SpaceID != f.private {
		t.Fatalf("unique private stage = (%#v, duplicate=%v), want new private batch", stagedPrivate, duplicate)
	}

	items, err := f.store.Recall(f.ctx, f.tenantID, []domain.PinnedSpace{
		{SpaceID: f.shared, MemoryVersion: 0},
		{SpaceID: f.private, MemoryVersion: 0},
	}, "fact", 10)
	if err != nil {
		t.Fatalf("recall staged batches: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("staged batches exposed %d recall items, want none", len(items))
	}
}
