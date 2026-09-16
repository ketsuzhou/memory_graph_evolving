package httpapi

import (
	"context"
	"errors"
	"testing"

	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/store/memory"
)

// The manifest provider is part of the durable authority's dependency
// surface: a provider READ failure is an availability fault and must surface
// transient (the service wraps it ErrGuardAuthorityUnavailable → HTTP 500),
// while a deterministic absence (ErrCutNotFound) stays a fact rebuttal
// (ErrGuardNotSatisfied → HTTP 422 RECEIPT_MISSING).
func TestDurableGuardAuthorityClassifiesManifestProviderFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	bindings := map[string][]domain.SpaceID{"room-1": {"space-1"}}

	transient := NewDurableGuardAuthority(memory.New(), func(context.Context, domain.TenantID, consolidationcut.CutID) (consolidationcut.Manifest, error) {
		return consolidationcut.Manifest{}, errors.New("manifest store unavailable")
	}, bindings)
	_, _, err := transient.ReceiptsComplete(ctx, "tenant-1", "cut-1", "sha256:aa")
	if err == nil || errors.Is(err, consolidationcut.ErrGuardNotSatisfied) {
		t.Fatalf("manifest provider read failure = %v, want transient (not ErrGuardNotSatisfied)", err)
	}

	missing := NewDurableGuardAuthority(memory.New(), func(context.Context, domain.TenantID, consolidationcut.CutID) (consolidationcut.Manifest, error) {
		return consolidationcut.Manifest{}, consolidationcut.ErrCutNotFound
	}, bindings)
	_, _, err = missing.ReceiptsComplete(ctx, "tenant-1", "cut-1", "sha256:aa")
	if !errors.Is(err, consolidationcut.ErrGuardNotSatisfied) {
		t.Fatalf("missing manifest = %v, want deterministic ErrGuardNotSatisfied", err)
	}
}
