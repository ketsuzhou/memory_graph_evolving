package authz_test

import (
	"context"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/store/memory"
)

type purposeClock struct {
	now time.Time
}

func (c purposeClock) Now() time.Time { return c.now }

// TestAuthorizeExactEnforcesPurposeFence proves a Grant only covers requests
// on its own plane: lifecycle grants never authorize tool-plane operations and
// tool-plane grants never authorize lifecycle operations, even when the
// operation list and Space set would otherwise match.
func TestAuthorizeExactEnforcesPurposeFence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	tenant := domain.TenantID("tenant-purpose")
	agent := domain.PrincipalID("agent-purpose")
	space := domain.SpaceID("space-purpose")

	created, err := store.InitializeTenant(ctx, domain.Tenant{
		ID: tenant, DisplayName: "Purpose tenant", BootstrapPrincipalID: "bootstrap",
	}, domain.Principal{ID: "bootstrap", TenantID: tenant, Kind: domain.PrincipalService, DisplayName: "Bootstrap"})
	if err != nil || !created {
		t.Fatalf("initialize tenant: created=%v err=%v", created, err)
	}
	if _, err := store.PutPrincipal(ctx, tenant, domain.Principal{ID: agent, TenantID: tenant, Kind: domain.PrincipalAgent, DisplayName: "Agent"}); err != nil {
		t.Fatalf("put principal: %v", err)
	}
	if _, err := store.PutSpace(ctx, tenant, domain.Space{ID: space, TenantID: tenant, Scope: domain.SpaceShared, DisplayName: "Shared"}); err != nil {
		t.Fatalf("put space: %v", err)
	}
	allOperations := []domain.GrantOperation{
		domain.GrantOperationEvidenceStage,
		domain.GrantOperationEvidenceCommit,
		domain.GrantOperationRecall,
		domain.GrantOperationExplorationStart,
		domain.GrantOperationExplorationExplore,
		domain.GrantOperationExplorationRedirect,
		domain.GrantOperationExplorationSubmit,
	}
	if _, err := store.PutGrant(ctx, tenant, domain.Grant{
		ID: "grant-lifecycle", TenantID: tenant, PrincipalID: agent,
		SpaceIDs: []domain.SpaceID{space}, Purpose: domain.GrantPurposeLifecycle,
		Operations: allOperations, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("put lifecycle grant: %v", err)
	}

	authorizer := authz.NewAuthorizer(store, purposeClock{now: now})
	identity := authz.Identity{TenantID: tenant, PrincipalID: agent}

	// Same principal, same Space, same operation names: the lifecycle grant
	// must not cover tool-plane exploration requests.
	if _, err := authorizer.AuthorizeExact(ctx, identity, []domain.SpaceID{space}, domain.GrantPurposeToolPlane, domain.GrantOperationExplorationStart); !isCode(err, "GRANT_MISSING") {
		t.Fatalf("lifecycle grant covering tool_plane start: err=%v, want GRANT_MISSING", err)
	}
	// And the mirror direction: a principal holding only a tool-plane grant
	// (with every operation listed) cannot make a lifecycle recall request.
	toolOnly := domain.PrincipalID("agent-tool-plane-only")
	if _, err := store.PutPrincipal(ctx, tenant, domain.Principal{ID: toolOnly, TenantID: tenant, Kind: domain.PrincipalAgent, DisplayName: "Tool-plane only"}); err != nil {
		t.Fatalf("put tool-plane principal: %v", err)
	}
	if _, err := store.PutGrant(ctx, tenant, domain.Grant{
		ID: "grant-tool-plane", TenantID: tenant, PrincipalID: toolOnly,
		SpaceIDs: []domain.SpaceID{space}, Purpose: domain.GrantPurposeToolPlane,
		Operations: allOperations, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("put tool-plane grant: %v", err)
	}
	toolIdentity := authz.Identity{TenantID: tenant, PrincipalID: toolOnly}
	if _, err := authorizer.AuthorizeExact(ctx, toolIdentity, []domain.SpaceID{space}, domain.GrantPurposeLifecycle, domain.GrantOperationRecall); !isCode(err, "GRANT_MISSING") {
		t.Fatalf("tool_plane grant covering lifecycle recall: err=%v, want GRANT_MISSING", err)
	}
	// Matching purpose on the same grant still succeeds and pins the version.
	pinned, err := authorizer.AuthorizeExact(ctx, toolIdentity, []domain.SpaceID{space}, domain.GrantPurposeToolPlane, domain.GrantOperationExplorationStart)
	if err != nil {
		t.Fatalf("matching-purpose authorization failed: %v", err)
	}
	if len(pinned) != 1 || pinned[0].SpaceID != space {
		t.Fatalf("pinned = %#v, want one entry for %q", pinned, space)
	}
	pinned, err = authorizer.AuthorizeExact(ctx, identity, []domain.SpaceID{space}, domain.GrantPurposeLifecycle, domain.GrantOperationRecall)
	if err != nil || len(pinned) != 1 {
		t.Fatalf("matching-purpose lifecycle authorization = (%#v, %v)", pinned, err)
	}
}

func isCode(err error, code string) bool {
	if err == nil {
		return false
	}
	var typed *domain.ProtocolError
	if !asProtocolError(err, &typed) {
		return false
	}
	return typed.Code() == code
}

func asProtocolError(err error, target **domain.ProtocolError) bool {
	typed, ok := err.(*domain.ProtocolError)
	if ok {
		*target = typed
	}
	return ok
}
