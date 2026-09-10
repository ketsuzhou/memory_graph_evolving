package exploration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/store/memory"
)

const (
	tenantID       domain.TenantID    = "tenant-test"
	memoryAgentID  domain.PrincipalID = "principal-memory-agent"
	privateOwnerID domain.PrincipalID = "principal-private-owner"
	sharedSpaceID  domain.SpaceID     = "space-room-shared"
	privateSpaceID domain.SpaceID     = "space-agent-private"
	sharedBatchID  domain.BatchID     = "batch-room-shared"
)

var fixedNow = time.Date(2026, time.September, 8, 11, 17, 33, 0, time.UTC)

type fakeClock struct {
	now time.Time
}

func (c fakeClock) Now() time.Time { return c.now }

type fixture struct {
	service *exploration.Service
	store   *memory.Store
}

func newFixture(t *testing.T) fixture {
	t.Helper()

	ctx := context.Background()
	clock := fakeClock{now: fixedNow}
	store := memory.New()
	bootstrapID := domain.PrincipalID("principal-bootstrap-service")

	created, err := store.InitializeTenant(ctx, domain.Tenant{
		ID:                   tenantID,
		DisplayName:          "Test tenant",
		BootstrapPrincipalID: bootstrapID,
	}, domain.Principal{
		ID:          bootstrapID,
		TenantID:    tenantID,
		Kind:        domain.PrincipalService,
		DisplayName: "Bootstrap service",
	})
	mustCreate(t, "initialize tenant", created, err)
	created, err = store.PutPrincipal(ctx, tenantID, domain.Principal{
		ID:          memoryAgentID,
		TenantID:    tenantID,
		Kind:        domain.PrincipalAgent,
		DisplayName: "Memory agent",
	})
	mustCreate(t, "register memory agent", created, err)
	created, err = store.PutPrincipal(ctx, tenantID, domain.Principal{
		ID:          privateOwnerID,
		TenantID:    tenantID,
		Kind:        domain.PrincipalAgent,
		DisplayName: "Private-space owner",
	})
	mustCreate(t, "register private-space owner", created, err)
	created, err = store.PutSpace(ctx, tenantID, domain.Space{
		ID:          sharedSpaceID,
		TenantID:    tenantID,
		Scope:       domain.SpaceShared,
		DisplayName: "Room shared",
	})
	mustCreate(t, "register shared space", created, err)
	owner := privateOwnerID
	created, err = store.PutSpace(ctx, tenantID, domain.Space{
		ID:               privateSpaceID,
		TenantID:         tenantID,
		Scope:            domain.SpacePrivate,
		OwnerPrincipalID: &owner,
		DisplayName:      "Agent private",
	})
	mustCreate(t, "register private space", created, err)

	operations := []domain.GrantOperation{
		domain.GrantOperationExplorationStart,
		domain.GrantOperationExplorationExplore,
		domain.GrantOperationExplorationRedirect,
		domain.GrantOperationExplorationSubmit,
	}
	created, err = store.PutGrant(ctx, tenantID, domain.Grant{
		ID:          "grant-shared-exploration",
		TenantID:    tenantID,
		PrincipalID: memoryAgentID,
		SpaceIDs:    []domain.SpaceID{sharedSpaceID},
		Purpose:     domain.GrantPurposeToolPlane,
		Operations:  operations,
		ExpiresAt:   fixedNow.Add(time.Hour),
	})
	mustCreate(t, "grant shared exploration", created, err)
	created, err = store.PutGrant(ctx, tenantID, domain.Grant{
		ID:          "grant-private-exploration",
		TenantID:    tenantID,
		PrincipalID: memoryAgentID,
		SpaceIDs:    []domain.SpaceID{privateSpaceID},
		Purpose:     domain.GrantPurposeToolPlane,
		Operations:  operations,
		ExpiresAt:   fixedNow.Add(time.Hour),
	})
	mustCreate(t, "grant private exploration", created, err)

	seedCommittedBatch(t, store, domain.EvidenceBatch{
		ID:              sharedBatchID,
		TenantID:        tenantID,
		IdempotencyKey:  "outbox-room-shared",
		SpaceID:         sharedSpaceID,
		StreamID:        "stream-room-shared",
		SourceSegmentID: "opaque-segment-shared",
		Provenance: domain.Provenance{
			HostType:       "pi-group-chat-host",
			HostInstanceID: "host-test",
			SourceKind:     "room_shared",
			CapturedAt:     fixedNow.Add(-2 * time.Minute),
			ContentSHA256:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Events: []domain.EvidenceEvent{{
			ID:         "event-shared-answer",
			Sequence:   1,
			Kind:       "room_message",
			Content:    "The launch code is maple.",
			OccurredAt: fixedNow.Add(-2 * time.Minute),
		}},
		Links:           []domain.EvidenceLink{},
		TerminalOutcome: "settled",
	})
	seedCommittedBatch(t, store, domain.EvidenceBatch{
		ID:              "batch-agent-private",
		TenantID:        tenantID,
		IdempotencyKey:  "outbox-agent-private",
		SpaceID:         privateSpaceID,
		StreamID:        "stream-agent-private",
		SourceSegmentID: "opaque-segment-private",
		Provenance: domain.Provenance{
			HostType:       "pi-group-chat-host",
			HostInstanceID: "host-test",
			SourceKind:     "agent_private",
			CapturedAt:     fixedNow.Add(-time.Minute),
			ContentSHA256:  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		Events: []domain.EvidenceEvent{{
			ID:         "event-private-answer",
			Sequence:   1,
			Kind:       "pi_internal",
			Content:    "Private material must never enter an exploration.",
			OccurredAt: fixedNow.Add(-time.Minute),
		}},
		Links:           []domain.EvidenceLink{},
		TerminalOutcome: "settled",
	})

	return fixture{
		service: exploration.New(store, store, store, clock),
		store:   store,
	}
}

func TestExplorationStartExploreRedirectSubmitPinsVersionAndEndsTerminal(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	started, err := fx.service.Start(ctx, tenantID, memoryAgentID, exploration.StartRequest{
		RequestID:      "request-lifecycle",
		IdempotencyKey: "start-lifecycle",
		SpaceIDs:       []domain.SpaceID{sharedSpaceID},
		Query:          "What is the launch code?",
		MaxSteps:       2,
		MaxResults:     10,
	})
	if err != nil {
		t.Fatalf("start exploration: %v", err)
	}
	if started.Duplicate {
		t.Fatal("first start reported duplicate")
	}
	if started.Session.ID == "" {
		t.Fatal("start returned an empty session ID")
	}
	if started.Session.State != "active" {
		t.Fatalf("start state = %q, want active", started.Session.State)
	}
	if started.Session.TenantID != tenantID || started.Session.PrincipalID != memoryAgentID {
		t.Fatalf("session authority = (%q, %q), want (%q, %q)", started.Session.TenantID, started.Session.PrincipalID, tenantID, memoryAgentID)
	}
	if got, want := started.Session.SpaceIDs, []domain.SpaceID{sharedSpaceID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("session spaces = %v, want %v", got, want)
	}
	if got, want := started.Session.PinnedSpaces, []domain.PinnedSpace{{SpaceID: sharedSpaceID, MemoryVersion: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pinned spaces = %#v, want %#v", got, want)
	}
	if started.RemainingSteps != 2 {
		t.Fatalf("remaining steps after start = %d, want 2", started.RemainingSteps)
	}
	if len(started.Items) == 0 {
		t.Fatal("start returned no recall items")
	}
	served := started.Items[0]
	if served.SourceSpaceID != sharedSpaceID || served.MemoryVersion != 1 || served.Citation.EvidenceBatchID != sharedBatchID {
		t.Fatalf("start item lost source identity: %#v", served)
	}

	explored, err := fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.ExploreRequest{
		OperationID:      "operation-explore",
		AnchorCitationID: served.Citation.ID,
		Relation:         "related",
		Limit:            10,
	})
	if err != nil {
		t.Fatalf("explore: %v", err)
	}
	assertActiveStep(t, explored, started.Session.ID, "operation-explore", 1)

	redirected, err := fx.service.Redirect(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.RedirectRequest{
		OperationID:       "operation-redirect",
		Query:             "Find the launch credential.",
		AnchorCitationIDs: []string{served.Citation.ID},
		Reason:            "The first wording may be too narrow.",
	})
	if err != nil {
		t.Fatalf("redirect: %v", err)
	}
	assertActiveStep(t, redirected, started.Session.ID, "operation-redirect", 0)

	submitted, err := fx.service.Submit(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.SubmitRequest{
		OperationID: "operation-submit",
		Found:       true,
		Summary:     "The launch code is maple.",
		CitationIDs: []string{served.Citation.ID},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if submitted.State != "submitted" || !submitted.Found || submitted.Summary != "The launch code is maple." {
		t.Fatalf("unexpected submit result: %#v", submitted)
	}
	if submitted.SessionID != started.Session.ID || submitted.OperationID != "operation-submit" || submitted.Duplicate {
		t.Fatalf("submit identity/idempotency fields are wrong: %#v", submitted)
	}
	if got, want := submitted.Citations, []exploration.SubmittedCitation{{
		CitationID:      served.Citation.ID,
		SourceSpaceID:   sharedSpaceID,
		MemoryVersion:   1,
		EvidenceBatchID: sharedBatchID,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("submitted citations = %#v, want %#v", got, want)
	}

	_, err = fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.ExploreRequest{
		OperationID:      "operation-after-submit",
		AnchorCitationID: served.Citation.ID,
		Relation:         "related",
		Limit:            1,
	})
	requireTypedStatus(t, err, 409)
}

func TestExplorationStepBudgetExhaustionIsTypedAnd429Worthy(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	started := startShared(t, fx.service, "request-budget", "start-budget", 1)
	citationID := firstCitationID(t, started)
	first, err := fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.ExploreRequest{
		OperationID:      "operation-budget-1",
		AnchorCitationID: citationID,
		Relation:         "related",
		Limit:            1,
	})
	if err != nil {
		t.Fatalf("first budgeted step: %v", err)
	}
	assertActiveStep(t, first, started.Session.ID, "operation-budget-1", 0)

	_, err = fx.service.Redirect(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.RedirectRequest{
		OperationID:       "operation-over-budget",
		Query:             "Try another route.",
		AnchorCitationIDs: []string{citationID},
		Reason:            "The single allowed step was already consumed.",
	})
	requireTypedProtocolError(t, err, 429, "EXPLORATION_BUDGET_EXHAUSTED")
}

func TestExplorationSubmitAcceptsOnlyCitationsServedByThisSession(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	started := startShared(t, fx.service, "request-citation-fence", "start-citation-fence", 1)
	servedCitationID := firstCitationID(t, started)

	_, err := fx.service.Submit(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.SubmitRequest{
		OperationID: "operation-submit-foreign",
		Found:       true,
		Summary:     "This citation was never served by this session.",
		CitationIDs: []string{"citation-from-another-session"},
	})
	requireTypedStatus(t, err, 422)

	submitted, err := fx.service.Submit(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.SubmitRequest{
		OperationID: "operation-submit-served",
		Found:       true,
		Summary:     "The launch code is maple.",
		CitationIDs: []string{servedCitationID},
	})
	if err != nil {
		t.Fatalf("submit session-served citation after rejected foreign citation: %v", err)
	}
	if submitted.State != "submitted" || len(submitted.Citations) != 1 || submitted.Citations[0].CitationID != servedCitationID {
		t.Fatalf("unexpected valid submission: %#v", submitted)
	}
}

func TestExplorationOperationReplayIsSemanticAndConflictingReuseFailsClosed(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	started := startShared(t, fx.service, "request-operation-idempotency", "start-operation-idempotency", 3)
	request := exploration.ExploreRequest{
		OperationID:      "operation-semantic-key",
		AnchorCitationID: firstCitationID(t, started),
		Relation:         "related",
		Limit:            7,
	}

	first, err := fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, request)
	if err != nil {
		t.Fatalf("first explore operation: %v", err)
	}
	replayed, err := fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, request)
	if err != nil {
		t.Fatalf("identical operation replay: %v", err)
	}
	if !replayed.Duplicate {
		t.Fatal("identical operation replay was not marked duplicate")
	}
	replayed.Duplicate = false
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("identical replay changed stored response:\nfirst:  %#v\nreplay: %#v", first, replayed)
	}

	changed := request
	changed.Relation = "mentions"
	_, err = fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, changed)
	requireTypedProtocolError(t, err, 409, "IDEMPOTENCY_CONFLICT")

	fresh, err := fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.ExploreRequest{
		OperationID:      "operation-after-conflict",
		AnchorCitationID: request.AnchorCitationID,
		Relation:         "related",
		Limit:            1,
	})
	if err != nil {
		t.Fatalf("fresh operation after conflict: %v", err)
	}
	if fresh.RemainingSteps != first.RemainingSteps-1 {
		t.Fatalf("conflicting retry consumed a step: remaining = %d, want %d", fresh.RemainingSteps, first.RemainingSteps-1)
	}
}

func TestExplorationRejectsPrivateSpaceWith403EvenWhenGranted(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	_, err := fx.service.Start(ctx, tenantID, memoryAgentID, exploration.StartRequest{
		RequestID:      "request-private",
		IdempotencyKey: "start-private-then-shared",
		SpaceIDs:       []domain.SpaceID{privateSpaceID},
		Query:          "Reveal private material.",
		MaxSteps:       1,
		MaxResults:     10,
	})
	requireTypedProtocolError(t, err, 403, "SPACE_FORBIDDEN")

	started, err := fx.service.Start(ctx, tenantID, memoryAgentID, exploration.StartRequest{
		RequestID:      "request-shared-after-private",
		IdempotencyKey: "start-private-then-shared",
		SpaceIDs:       []domain.SpaceID{sharedSpaceID},
		Query:          "What is the launch code?",
		MaxSteps:       1,
		MaxResults:     10,
	})
	if err != nil {
		t.Fatalf("rejected private start reserved idempotency key: %v", err)
	}
	if got, want := started.Session.PinnedSpaces, []domain.PinnedSpace{{SpaceID: sharedSpaceID, MemoryVersion: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shared start pinned spaces = %#v, want %#v", got, want)
	}
}

func seedCommittedBatch(t *testing.T, store *memory.Store, batch domain.EvidenceBatch) {
	t.Helper()
	ctx := context.Background()
	staged, duplicate, err := store.Stage(ctx, batch)
	if err != nil {
		t.Fatalf("stage %s: %v", batch.ID, err)
	}
	if duplicate {
		t.Fatalf("first stage of %s reported duplicate", batch.ID)
	}
	if staged.ID != batch.ID {
		t.Fatalf("staged batch ID = %q, want %q", staged.ID, batch.ID)
	}
	committed, duplicate, err := store.Commit(ctx, tenantID, batch.ID, "commit-"+string(batch.ID), fixedNow)
	if err != nil {
		t.Fatalf("commit %s: %v", batch.ID, err)
	}
	if duplicate {
		t.Fatalf("first commit of %s reported duplicate", batch.ID)
	}
	if committed.State != domain.EvidenceBatchCommitted || committed.MemoryVersion == nil || *committed.MemoryVersion != 1 {
		t.Fatalf("committed batch %s has unexpected state/version: %#v", batch.ID, committed)
	}
}

func mustCreate(t *testing.T, action string, created bool, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", action, err)
	}
	if !created {
		t.Fatalf("%s: first write was not created", action)
	}
}

func startShared(t *testing.T, service *exploration.Service, requestID, idempotencyKey string, maxSteps int) exploration.StartResult {
	t.Helper()
	started, err := service.Start(context.Background(), tenantID, memoryAgentID, exploration.StartRequest{
		RequestID:      requestID,
		IdempotencyKey: idempotencyKey,
		SpaceIDs:       []domain.SpaceID{sharedSpaceID},
		Query:          "What is the launch code?",
		MaxSteps:       maxSteps,
		MaxResults:     10,
	})
	if err != nil {
		t.Fatalf("start shared exploration: %v", err)
	}
	if got, want := started.Session.PinnedSpaces, []domain.PinnedSpace{{SpaceID: sharedSpaceID, MemoryVersion: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pinned spaces = %#v, want %#v", got, want)
	}
	return started
}

func firstCitationID(t *testing.T, result exploration.StartResult) string {
	t.Helper()
	if len(result.Items) == 0 {
		t.Fatal("start returned no items")
	}
	if result.Items[0].Citation.ID == "" {
		t.Fatal("start item has empty citation ID")
	}
	return result.Items[0].Citation.ID
}

func assertActiveStep(t *testing.T, result exploration.StepResult, sessionID domain.ExplorationSessionID, operationID string, remaining int) {
	t.Helper()
	if result.SessionID != sessionID || result.OperationID != operationID {
		t.Fatalf("step identity = (%q, %q), want (%q, %q)", result.SessionID, result.OperationID, sessionID, operationID)
	}
	if result.State != "active" {
		t.Fatalf("step state = %q, want active", result.State)
	}
	if result.RemainingSteps != remaining {
		t.Fatalf("remaining steps = %d, want %d", result.RemainingSteps, remaining)
	}
	if result.Duplicate {
		t.Fatal("first step application reported duplicate")
	}
}

type statusCoder interface {
	StatusCode() int
}

type httpStatuser interface {
	HTTPStatus() int
}

type protocolCoder interface {
	Code() string
}

type errorProtocolCoder interface {
	ErrorCode() string
}

func requireTypedProtocolError(t *testing.T, err error, wantStatus int, wantCode string) {
	t.Helper()
	requireTypedStatus(t, err, wantStatus)

	var code string
	var coded protocolCoder
	if errors.As(err, &coded) {
		code = coded.Code()
	} else {
		var errorCoded errorProtocolCoder
		if !errors.As(err, &errorCoded) {
			t.Fatalf("error %T (%v) has no typed Code or ErrorCode method", err, err)
		}
		code = errorCoded.ErrorCode()
	}
	if code != wantCode {
		t.Fatalf("error code = %q, want %q", code, wantCode)
	}
}

func requireTypedStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected typed status %d error, got nil", want)
	}

	var got int
	var statusCoded statusCoder
	if errors.As(err, &statusCoded) {
		got = statusCoded.StatusCode()
	} else {
		var httpStatused httpStatuser
		if !errors.As(err, &httpStatused) {
			t.Fatalf("error %T (%v) is not status-bearing and cannot map to HTTP %d", err, err, want)
		}
		got = httpStatused.HTTPStatus()
	}
	if got != want {
		t.Fatalf("error status = %d, want %d (%v)", got, want, err)
	}
}
