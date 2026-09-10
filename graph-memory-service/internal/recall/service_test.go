package recall_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/ports"
	"river2.dev/graph-memory-service/internal/recall"
	"river2.dev/graph-memory-service/internal/store/memory"
)

const (
	testTenantID    domain.TenantID    = "tenant-1"
	testPrincipalID domain.PrincipalID = "host-service"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.now = now
}

type recordingRecallStore struct {
	calls  int
	pinned [][]domain.PinnedSpace
	items  []domain.RecallItem
	err    error
}

func (s *recordingRecallStore) Retrieve(_ context.Context, _ domain.TenantID, pinned []domain.PinnedSpace, _ string, _ int) (ports.RecallOutcome, error) {
	s.calls++
	s.pinned = append(s.pinned, append([]domain.PinnedSpace(nil), pinned...))
	return ports.RecallOutcome{
		Items: append([]domain.RecallItem(nil), s.items...), Applied: []ports.RetrievalChannel{ports.RetrievalBM25},
	}, s.err
}

type fixture struct {
	store    *memory.Store
	clock    *fakeClock
	evidence *evidence.Service
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	clock := &fakeClock{now: time.Date(2026, time.September, 8, 11, 17, 0, 0, time.UTC)}
	store := memory.New()
	created, err := store.InitializeTenant(context.Background(), domain.Tenant{
		ID:                   testTenantID,
		DisplayName:          "Test tenant",
		BootstrapPrincipalID: testPrincipalID,
	}, domain.Principal{
		ID:          testPrincipalID,
		TenantID:    testTenantID,
		Kind:        domain.PrincipalService,
		DisplayName: "Host service",
	})
	if err != nil {
		t.Fatalf("initialize tenant: %v", err)
	}
	if !created {
		t.Fatalf("initialize tenant: created = false, want true")
	}

	return &fixture{
		store:    store,
		clock:    clock,
		evidence: evidence.New(store, store, clock),
	}
}

func (f *fixture) putSpace(t *testing.T, id domain.SpaceID, scope domain.SpaceScope, version int64) {
	t.Helper()

	var owner *domain.PrincipalID
	if scope == domain.SpacePrivate {
		principalID := testPrincipalID
		owner = &principalID
	}
	created, err := f.store.PutSpace(context.Background(), testTenantID, domain.Space{
		ID:               id,
		TenantID:         testTenantID,
		Scope:            scope,
		OwnerPrincipalID: owner,
		DisplayName:      string(id),
		Version:          version,
	})
	if err != nil {
		t.Fatalf("put space %q: %v", id, err)
	}
	if !created {
		t.Fatalf("put space %q: created = false, want true", id)
	}
}

func (f *fixture) putGrant(t *testing.T, id domain.GrantID, spaces []domain.SpaceID, operations []domain.GrantOperation, expiresAt time.Time) {
	t.Helper()

	created, err := f.store.PutGrant(context.Background(), testTenantID, domain.Grant{
		ID:          id,
		TenantID:    testTenantID,
		PrincipalID: testPrincipalID,
		SpaceIDs:    append([]domain.SpaceID(nil), spaces...),
		Purpose:     domain.GrantPurposeLifecycle,
		Operations:  append([]domain.GrantOperation(nil), operations...),
		ExpiresAt:   expiresAt,
	})
	if err != nil {
		t.Fatalf("put grant %q: %v", id, err)
	}
	if !created {
		t.Fatalf("put grant %q: created = false, want true", id)
	}
}

func (f *fixture) commitEvidence(t *testing.T, spaceID domain.SpaceID) domain.RecallItem {
	t.Helper()

	batch := domain.EvidenceBatch{
		ID:              "batch-1",
		TenantID:        testTenantID,
		IdempotencyKey:  "outbox-1",
		SpaceID:         spaceID,
		StreamID:        "stream-1",
		SourceSegmentID: "opaque-segment-1",
		Provenance: domain.Provenance{
			HostType:       "pi-group-chat-host",
			HostInstanceID: "host-1",
			SourceKind:     "room_shared",
			CapturedAt:     f.clock.Now().Add(-time.Minute),
			ContentSHA256:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		Events: []domain.EvidenceEvent{{
			ID:         "event-1",
			Sequence:   1,
			Kind:       "room_message",
			Content:    "The durable answer is 42.",
			OccurredAt: f.clock.Now().Add(-time.Minute),
		}},
		Links:           []domain.EvidenceLink{},
		TerminalOutcome: "settled",
	}

	staged, duplicate, err := f.evidence.Stage(context.Background(), testTenantID, testPrincipalID, batch)
	if err != nil {
		t.Fatalf("stage evidence: %v", err)
	}
	if duplicate {
		t.Fatalf("stage evidence: duplicate = true, want false")
	}
	committed, duplicate, err := f.evidence.Commit(context.Background(), testTenantID, testPrincipalID, staged.ID, "commit-1")
	if err != nil {
		t.Fatalf("commit evidence: %v", err)
	}
	if duplicate {
		t.Fatalf("commit evidence: duplicate = true, want false")
	}
	if committed.MemoryVersion == nil {
		t.Fatalf("commit evidence: memory version is nil")
	}

	return domain.RecallItem{
		Content:       batch.Events[0].Content,
		SourceSpaceID: spaceID,
		MemoryVersion: *committed.MemoryVersion,
		Citation: domain.Citation{
			ID:              "citation-1",
			EvidenceBatchID: committed.ID,
			EventIDs:        []string{batch.Events[0].ID},
		},
		Score: 1,
	}
}

// protocolError documents the stable error metadata the use-case layer must
// expose so the HTTP adapter can map failures without parsing error strings.
type protocolError interface {
	error
	Code() string
	StatusCode() int
}

func assertProtocolError(t *testing.T, err error, wantStatus int, wantCodes ...string) {
	t.Helper()

	if err == nil {
		t.Fatalf("Recall error = nil, want status %d and one of codes %v", wantStatus, wantCodes)
	}
	var got protocolError
	if !errors.As(err, &got) {
		t.Fatalf("Recall error %T does not expose Code() string and StatusCode() int", err)
	}
	if got.StatusCode() != wantStatus {
		t.Errorf("Recall status = %d, want %d", got.StatusCode(), wantStatus)
	}
	for _, code := range wantCodes {
		if got.Code() == code {
			return
		}
	}
	t.Errorf("Recall code = %q, want one of %v", got.Code(), wantCodes)
}

func TestRecallRejectsMissingForbiddenAndExpiredSpacesAtomically(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*testing.T, *fixture, domain.SpaceID, domain.SpaceID)
		wantStatus int
		wantCodes  []string
	}{
		{
			name: "missing space is 404",
			configure: func(t *testing.T, f *fixture, sharedID, _ domain.SpaceID) {
				f.putSpace(t, sharedID, domain.SpaceShared, 7)
				f.putGrant(t, "grant-shared", []domain.SpaceID{sharedID}, []domain.GrantOperation{domain.GrantOperationRecall}, f.clock.Now().Add(time.Hour))
			},
			wantStatus: 404,
			wantCodes:  []string{"SPACE_NOT_FOUND"},
		},
		{
			name: "existing ungranted space is 403",
			configure: func(t *testing.T, f *fixture, sharedID, privateID domain.SpaceID) {
				f.putSpace(t, sharedID, domain.SpaceShared, 7)
				f.putSpace(t, privateID, domain.SpacePrivate, 9)
				f.putGrant(t, "grant-shared", []domain.SpaceID{sharedID}, []domain.GrantOperation{domain.GrantOperationRecall}, f.clock.Now().Add(time.Hour))
			},
			wantStatus: 403,
			wantCodes:  []string{"SPACE_FORBIDDEN", "GRANT_MISSING"},
		},
		{
			name: "grant expiring exactly now is 403",
			configure: func(t *testing.T, f *fixture, sharedID, privateID domain.SpaceID) {
				f.putSpace(t, sharedID, domain.SpaceShared, 7)
				f.putSpace(t, privateID, domain.SpacePrivate, 9)
				expiresAt := f.clock.Now().Add(time.Minute)
				f.putGrant(t, "grant-pair", []domain.SpaceID{sharedID, privateID}, []domain.GrantOperation{domain.GrantOperationRecall}, expiresAt)
				f.clock.Set(expiresAt)
			},
			wantStatus: 403,
			wantCodes:  []string{"GRANT_EXPIRED"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			sharedID := domain.SpaceID("room-shared")
			privateID := domain.SpaceID("owner-private")
			tt.configure(t, f, sharedID, privateID)

			wouldLeakIfRead := domain.RecallItem{
				Content:       "authorized-space evidence must not become a partial result",
				SourceSpaceID: sharedID,
				MemoryVersion: 7,
				Citation: domain.Citation{
					ID:              "citation-would-leak",
					EvidenceBatchID: "batch-would-leak",
					EventIDs:        []string{"event-would-leak"},
				},
				Score: 1,
			}
			retrieval := &recordingRecallStore{items: []domain.RecallItem{wouldLeakIfRead}}
			service := recall.New(f.store, retrieval, f.clock)

			result, err := service.Recall(context.Background(), testTenantID, testPrincipalID, domain.RecallRequest{
				RequestID:  "recall-atomic",
				Query:      "answer",
				SpaceIDs:   []domain.SpaceID{sharedID, privateID},
				MaxResults: 10,
				DeadlineMS: 1000,
			})

			assertProtocolError(t, err, tt.wantStatus, tt.wantCodes...)
			if retrieval.calls != 0 {
				t.Errorf("RecallStore.Retrieve calls = %d, want 0: the complete space list must be authorized before reads", retrieval.calls)
			}
			if len(result.Items) != 0 {
				t.Errorf("Recall returned %d partial items after scope rejection, want 0", len(result.Items))
			}
		})
	}
}

func TestRecallFailureReturnsScopedDegradation(t *testing.T) {
	f := newFixture(t)
	sharedID := domain.SpaceID("room-shared")
	privateID := domain.SpaceID("owner-private")
	f.putSpace(t, sharedID, domain.SpaceShared, 7)
	f.putSpace(t, privateID, domain.SpacePrivate, 9)
	f.putGrant(t, "grant-evidence", []domain.SpaceID{sharedID}, []domain.GrantOperation{
		domain.GrantOperationEvidenceStage,
		domain.GrantOperationEvidenceCommit,
	}, f.clock.Now().Add(time.Hour))
	f.putGrant(t, "grant-recall", []domain.SpaceID{sharedID, privateID}, []domain.GrantOperation{
		domain.GrantOperationRecall,
	}, f.clock.Now().Add(time.Hour))

	partialItem := f.commitEvidence(t, sharedID)
	retrieval := &recordingRecallStore{
		items: []domain.RecallItem{partialItem},
		err:   errors.New("injected retrieval outage after authorization"),
	}
	service := recall.New(f.store, retrieval, f.clock)
	requestedSpaces := []domain.SpaceID{sharedID, privateID}

	result, err := service.Recall(context.Background(), testTenantID, testPrincipalID, domain.RecallRequest{
		RequestID:  "recall-degraded",
		Query:      "durable answer",
		SpaceIDs:   requestedSpaces,
		MaxResults: 10,
		DeadlineMS: 1000,
	})

	if err != nil {
		t.Fatalf("Recall returned transport-level error %v; post-authorization retrieval failure must be a typed successful result", err)
	}
	if retrieval.calls != 1 {
		t.Fatalf("RecallStore.Retrieve calls = %d, want 1", retrieval.calls)
	}
	if len(retrieval.pinned) != 1 {
		t.Fatalf("recorded pinned scope count = %d, want 1", len(retrieval.pinned))
	}
	assertExactPinnedScope(t, retrieval.pinned[0], []domain.PinnedSpace{
		{SpaceID: sharedID, MemoryVersion: 7},
		{SpaceID: privateID, MemoryVersion: 9},
	})
	if result.RequestID != "recall-degraded" {
		t.Errorf("result request ID = %q, want %q", result.RequestID, "recall-degraded")
	}
	if result.Degradation.State != "partial" {
		t.Errorf("degradation state = %q, want %q", result.Degradation.State, "partial")
	}
	if len(result.Degradation.Reasons) == 0 {
		t.Errorf("degradation reasons are empty, want a typed retrieval-failure reason")
	}
	if len(result.Items) == 0 {
		t.Fatalf("degraded result items are empty, want the authorized partial item returned by the store")
	}
	for _, item := range result.Items {
		if !containsSpace(requestedSpaces, item.SourceSpaceID) {
			t.Errorf("result widened scope to source space %q; requested spaces were %v", item.SourceSpaceID, requestedSpaces)
		}
	}
}

func assertExactPinnedScope(t *testing.T, got, want []domain.PinnedSpace) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("pinned spaces = %v, want exact scope %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pinned space[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func containsSpace(spaces []domain.SpaceID, target domain.SpaceID) bool {
	for _, spaceID := range spaces {
		if spaceID == target {
			return true
		}
	}
	return false
}
