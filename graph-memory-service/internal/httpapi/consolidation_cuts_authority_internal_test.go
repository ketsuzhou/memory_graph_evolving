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

// The published_version DTO field is only produced by a consolidating→completed
// transition's space results — a stage the composition cannot reach through
// the HTTP seam without the not-yet-wired workers — so the wire shape is
// pinned here at the DTO boundary.
func TestCutSpaceResultsDTOPublishesVersions(t *testing.T) {
	t.Parallel()
	head := domain.ProjectionVersion(7)
	results := cutSpaceResultsDTO([]consolidationcut.SpaceResult{
		{SpaceID: "space-published", Result: consolidationcut.SpaceResultPublished, PublishedVersion: &head},
		{SpaceID: "space-pending", Result: consolidationcut.SpaceResultPending},
	})
	if len(results) != 2 {
		t.Fatalf("results = %v, want 2 entries", results)
	}
	published, _ := results[0].(map[string]any)
	if published["published_version"] != int64(7) {
		t.Fatalf("published space = %v, want published_version 7", published)
	}
	pending, _ := results[1].(map[string]any)
	if _, ok := pending["published_version"]; ok {
		t.Fatalf("pending space must not carry published_version: %v", pending)
	}
}
