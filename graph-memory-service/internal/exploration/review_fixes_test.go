package exploration_test

import (
	"context"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/exploration"
)

// TestReviewPerStepAuthorizationFailsClosed proves explore/redirect/submit
// re-authorize their own operation: a principal holding only exploration.start
// can open a session but cannot take a single step or submit it.
func TestReviewPerStepAuthorizationFailsClosed(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	startOnly := domain.PrincipalID("principal-start-only")
	if _, err := fx.store.PutPrincipal(ctx, tenantID, domain.Principal{
		ID: startOnly, TenantID: tenantID, Kind: domain.PrincipalAgent, DisplayName: "Start-only agent",
	}); err != nil {
		t.Fatalf("put principal: %v", err)
	}
	if _, err := fx.store.PutGrant(ctx, tenantID, domain.Grant{
		ID: "grant-start-only", TenantID: tenantID, PrincipalID: startOnly,
		SpaceIDs:   []domain.SpaceID{sharedSpaceID},
		Purpose:    domain.GrantPurposeToolPlane,
		Operations: []domain.GrantOperation{domain.GrantOperationExplorationStart},
		ExpiresAt:  fixedNow.Add(time.Hour),
	}); err != nil {
		t.Fatalf("put grant: %v", err)
	}

	started, err := fx.service.Start(ctx, tenantID, startOnly, exploration.StartRequest{
		RequestID: "request-start-only", IdempotencyKey: "start-only",
		SpaceIDs: []domain.SpaceID{sharedSpaceID}, Query: "What is the launch code?",
		MaxSteps: 5, MaxResults: 10,
	})
	if err != nil {
		t.Fatalf("start with start-only grant: %v", err)
	}
	citation := firstCitationID(t, started)

	_, err = fx.service.Explore(ctx, tenantID, startOnly, started.Session.ID, exploration.ExploreRequest{
		OperationID: "op-explore-denied", AnchorCitationID: citation, Relation: "related", Limit: 1,
	})
	requireTypedProtocolError(t, err, 403, "GRANT_MISSING")
	_, err = fx.service.Redirect(ctx, tenantID, startOnly, started.Session.ID, exploration.RedirectRequest{
		OperationID: "op-redirect-denied", Query: "Try again.", AnchorCitationIDs: []string{citation}, Reason: "steering",
	})
	requireTypedProtocolError(t, err, 403, "GRANT_MISSING")
	_, err = fx.service.Submit(ctx, tenantID, startOnly, started.Session.ID, exploration.SubmitRequest{
		OperationID: "op-submit-denied", Found: false,
	})
	requireTypedProtocolError(t, err, 403, "GRANT_MISSING")
}

// TestReviewGrantExpiryMidSessionFailsClosed proves a session cannot outlive
// its grant: submitting after the covering grant expired is 403 GRANT_EXPIRED.
func TestReviewGrantExpiryMidSessionFailsClosed(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	agent := domain.PrincipalID("principal-short-lived")
	if _, err := fx.store.PutPrincipal(ctx, tenantID, domain.Principal{
		ID: agent, TenantID: tenantID, Kind: domain.PrincipalAgent, DisplayName: "Short-lived agent",
	}); err != nil {
		t.Fatalf("put principal: %v", err)
	}
	if _, err := fx.store.PutGrant(ctx, tenantID, domain.Grant{
		ID: "grant-short-lived", TenantID: tenantID, PrincipalID: agent,
		SpaceIDs:   []domain.SpaceID{sharedSpaceID},
		Purpose:    domain.GrantPurposeToolPlane,
		Operations: []domain.GrantOperation{domain.GrantOperationExplorationStart, domain.GrantOperationExplorationSubmit},
		ExpiresAt:  fixedNow.Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("put grant: %v", err)
	}

	started, err := fx.service.Start(ctx, tenantID, agent, exploration.StartRequest{
		RequestID: "request-expiry", IdempotencyKey: "start-expiry",
		SpaceIDs: []domain.SpaceID{sharedSpaceID}, Query: "What is the launch code?",
		MaxSteps: 5, MaxResults: 10,
	})
	if err != nil {
		t.Fatalf("start before expiry: %v", err)
	}

	afterExpiry := exploration.New(fx.store, fx.store, fx.store, fakeClock{now: fixedNow.Add(time.Minute)})
	_, err = afterExpiry.Submit(ctx, tenantID, agent, started.Session.ID, exploration.SubmitRequest{
		OperationID: "op-submit-expired", Found: false,
	})
	requireTypedProtocolError(t, err, 403, "GRANT_EXPIRED")
}

// TestReviewStartIdempotencyComparesSemantics proves the start idempotency key
// replays only semantically identical requests; changed content under the same
// key fails closed with 409 IDEMPOTENCY_CONFLICT.
func TestReviewStartIdempotencyComparesSemantics(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	base := exploration.StartRequest{
		RequestID: "request-m1", IdempotencyKey: "start-m1",
		SpaceIDs: []domain.SpaceID{sharedSpaceID}, Query: "What is the launch code?",
		MaxSteps: 3, MaxResults: 10,
	}
	first, err := fx.service.Start(ctx, tenantID, memoryAgentID, base)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	if first.Duplicate {
		t.Fatal("first start reported duplicate")
	}

	replay, err := fx.service.Start(ctx, tenantID, memoryAgentID, base)
	if err != nil {
		t.Fatalf("identical replay: %v", err)
	}
	if !replay.Duplicate || replay.Session.ID != first.Session.ID {
		t.Fatalf("identical replay = (duplicate=%v, session=%q), want (true, %q)", replay.Duplicate, replay.Session.ID, first.Session.ID)
	}

	changed := base
	changed.Query = "A different question entirely"
	_, err = fx.service.Start(ctx, tenantID, memoryAgentID, changed)
	requireTypedProtocolError(t, err, 409, "IDEMPOTENCY_CONFLICT")

	changedBudget := base
	changedBudget.MaxResults = 5
	_, err = fx.service.Start(ctx, tenantID, memoryAgentID, changedBudget)
	requireTypedProtocolError(t, err, 409, "IDEMPOTENCY_CONFLICT")

	changedRequest := base
	changedRequest.RequestID = "request-m1-other"
	_, err = fx.service.Start(ctx, tenantID, memoryAgentID, changedRequest)
	requireTypedProtocolError(t, err, 409, "IDEMPOTENCY_CONFLICT")
}

// TestReviewResultBudgetCapsServedItems proves max_results is a server-side
// session-wide budget: once the start query consumed it, explore is 429 even
// with step budget remaining.
func TestReviewResultBudgetCapsServedItems(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	started, err := fx.service.Start(ctx, tenantID, memoryAgentID, exploration.StartRequest{
		RequestID: "request-budget-results", IdempotencyKey: "start-budget-results",
		SpaceIDs: []domain.SpaceID{sharedSpaceID}, Query: "What is the launch code?",
		MaxSteps: 5, MaxResults: 1,
	})
	if err != nil {
		t.Fatalf("start with max_results=1: %v", err)
	}
	if len(started.Items) != 1 {
		t.Fatalf("start items = %d, want exactly the budgeted one", len(started.Items))
	}

	_, err = fx.service.Explore(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.ExploreRequest{
		OperationID: "op-explore-over-results", AnchorCitationID: firstCitationID(t, started), Relation: "related", Limit: 5,
	})
	requireTypedProtocolError(t, err, 429, "EXPLORATION_BUDGET_EXHAUSTED")
	_, err = fx.service.Redirect(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.RedirectRequest{
		OperationID: "op-redirect-over-results", Query: "Try another route.", AnchorCitationIDs: []string{firstCitationID(t, started)}, Reason: "steering",
	})
	requireTypedProtocolError(t, err, 429, "EXPLORATION_BUDGET_EXHAUSTED")

	// The session itself is untouched: a valid submit still terminates it.
	submitted, err := fx.service.Submit(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.SubmitRequest{
		OperationID: "op-submit-after-budget", Found: true,
		Summary: "The launch code is maple.", CitationIDs: []string{firstCitationID(t, started)},
	})
	if err != nil || submitted.State != "submitted" {
		t.Fatalf("submit after exhausted result budget = (%#v, %v), want submitted", submitted, err)
	}
}

// TestReviewFoundSubmissionRequiresSummary proves found=true with an empty
// summary is rejected 422 without terminating the session.
func TestReviewFoundSubmissionRequiresSummary(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()

	started := startShared(t, fx.service, "request-summary", "start-summary", 3)
	citation := firstCitationID(t, started)

	_, err := fx.service.Submit(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.SubmitRequest{
		OperationID: "op-submit-no-summary", Found: true, Summary: "", CitationIDs: []string{citation},
	})
	requireTypedProtocolError(t, err, 422, "SUMMARY_REQUIRED")

	submitted, err := fx.service.Submit(ctx, tenantID, memoryAgentID, started.Session.ID, exploration.SubmitRequest{
		OperationID: "op-submit-with-summary", Found: true,
		Summary: "The launch code is maple.", CitationIDs: []string{citation},
	})
	if err != nil || submitted.State != "submitted" {
		t.Fatalf("valid submit after summary rejection = (%#v, %v)", submitted, err)
	}
}
