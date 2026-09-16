package modellineage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"river2.dev/graph-memory-service/internal/artifactsecurity"
	"river2.dev/graph-memory-service/internal/domain"
)

// seedLineage builds source → {mid-a, mid-b} → merge → leaf so transitive
// closure, diamond dedup, and cascade coverage are all observable.
func seedLineage(t *testing.T, svc *Service) (context.Context, CheckpointRef, []CheckpointRef) {
	t.Helper()
	ctx := context.Background()
	tenant := domain.TenantID("tenant-a")
	source := CheckpointRef{ID: "checkpoint-source", TenantID: tenant}
	midA := CheckpointRef{ID: "checkpoint-mid-a", TenantID: tenant}
	midB := CheckpointRef{ID: "checkpoint-mid-b", TenantID: tenant}
	merge := CheckpointRef{ID: "checkpoint-merge", TenantID: tenant}
	leaf := CheckpointRef{ID: "checkpoint-leaf", TenantID: tenant}
	for _, node := range []Node{
		{Ref: source},
		{Ref: midA, Parents: []CheckpointRef{source}},
		{Ref: midB, Parents: []CheckpointRef{source}},
		{Ref: merge, Parents: []CheckpointRef{midA, midB}},
		{Ref: leaf, Parents: []CheckpointRef{merge}},
	} {
		if err := svc.Put(ctx, node); err != nil {
			t.Fatalf("seed %s: %v", node.Ref.ID, err)
		}
	}
	return ctx, source, []CheckpointRef{midA, midB, merge, leaf}
}

func TestAffectedDescendantsAreCompleteTransitiveAndDeduplicated(t *testing.T) {
	svc := NewService()
	ctx, source, want := seedLineage(t, svc)
	descendants, err := svc.AffectedDescendants(ctx, source)
	if errors.Is(err, ErrModelLineageNotImplemented) {
		t.Fatal("RED: complete, transitive, deduplicated model/checkpoint descendant lineage is not implemented")
	}
	if err != nil {
		t.Fatalf("affected descendants: %v", err)
	}
	if !reflect.DeepEqual(descendants, want) {
		t.Fatalf("descendants = %v, want the complete transitive diamond %v (merge deduplicated, source excluded)", descendants, want)
	}
}

func TestTombstonePropagatesAcrossAllAffectedModelDescendants(t *testing.T) {
	svc := NewService()
	ctx, source, want := seedLineage(t, svc)
	tombstones, err := svc.TombstoneCascade(ctx, source, artifactsecurity.EraseUserDeletion)
	if errors.Is(err, ErrModelLineageNotImplemented) {
		t.Fatal("RED: tombstone propagation across model/checkpoint descendants is not implemented")
	}
	if err != nil {
		t.Fatalf("tombstone cascade: %v", err)
	}
	if len(tombstones) != len(want)+1 {
		t.Fatalf("cascade produced %d tombstones, want the source root plus one per affected descendant (%d)", len(tombstones), len(want)+1)
	}
	if tombstones[0].Ref != source {
		t.Fatalf("cascade did not record the erased source root first: %#v", tombstones[0])
	}
	for _, tombstone := range tombstones {
		if tombstone.SourceRef != source || tombstone.Reason != string(artifactsecurity.EraseUserDeletion) {
			t.Fatalf("tombstone lost source/reason provenance: %#v", tombstone)
		}
	}
	// Cascades are recorded and idempotent: a repeated identical cascade
	// adds no duplicate records, and the ledger stays queryable afterwards.
	if _, err := svc.TombstoneCascade(ctx, source, artifactsecurity.EraseUserDeletion); err != nil {
		t.Fatalf("repeated cascade: %v", err)
	}
	if recorded := svc.Tombstones(); len(recorded) != len(want)+1 {
		t.Fatalf("recorded tombstones = %d, want the source root plus one per affected descendant (%d)", len(recorded), len(want)+1)
	}
}

func TestLineageRejectsRewritesCrossTenantParentsAndKeepsEdgesClean(t *testing.T) {
	svc := NewService()
	ctx, source, _ := seedLineage(t, svc)
	tenantA := domain.TenantID("tenant-a")

	// Rewriting an existing ref would strand stale adjacency and fabricate
	// lineage; it is rejected outright.
	if err := svc.Put(ctx, Node{Ref: source, Parents: []CheckpointRef{{ID: "checkpoint-other", TenantID: tenantA}}}); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("duplicate Put of a registered ref was accepted: %v", err)
	}
	// The rejected rewrite must not have mutated the graph.
	descendants, err := svc.AffectedDescendants(ctx, source)
	if err != nil {
		t.Fatalf("descendants after rejected rewrite: %v", err)
	}
	if len(descendants) != 4 {
		t.Fatalf("rejected rewrite mutated the lineage graph: %v", descendants)
	}

	// Training lineage never crosses tenants.
	crossTenant := Node{Ref: CheckpointRef{ID: "checkpoint-foreign", TenantID: domain.TenantID("tenant-b")}}
	if err := svc.Put(ctx, crossTenant); err != nil {
		t.Fatalf("seed foreign root: %v", err)
	}
	if err := svc.Put(ctx, Node{Ref: CheckpointRef{ID: "checkpoint-hybrid", TenantID: tenantA}, Parents: []CheckpointRef{crossTenant.Ref}}); !errors.Is(err, ErrCrossTenantParent) {
		t.Fatalf("cross-tenant parent edge was accepted: %v", err)
	}
	// The foreign tenant's graph is fully isolated from tenant-a tracing.
	foreignDescendants, err := svc.AffectedDescendants(ctx, crossTenant.Ref)
	if err != nil || len(foreignDescendants) != 0 {
		t.Fatalf("foreign tenant graph leaked into lineage: %v err=%v", foreignDescendants, err)
	}
}

func TestPutUnderTombstonedAncestorIsRejected(t *testing.T) {
	svc := NewService()
	ctx, source, _ := seedLineage(t, svc)
	if _, err := svc.TombstoneCascade(ctx, source, artifactsecurity.EraseUserDeletion); err != nil {
		t.Fatalf("cascade: %v", err)
	}
	// Erased lineage cannot silently regrow: neither the source nor any of
	// its tombstoned descendants accepts a new child.
	regrown := Node{Ref: CheckpointRef{ID: "checkpoint-regrown", TenantID: domain.TenantID("tenant-a")}, Parents: []CheckpointRef{{ID: "checkpoint-merge", TenantID: domain.TenantID("tenant-a")}}}
	if err := svc.Put(ctx, regrown); !errors.Is(err, ErrTombstonedAncestor) {
		t.Fatalf("Put under a tombstoned ancestor was accepted: %v", err)
	}
	// P1-10 root-parent bypass: a new live child naming the ERASED SOURCE
	// ROOT directly as its parent is rejected too — the cascade records the
	// root itself, not only its descendants.
	rootChild := Node{Ref: CheckpointRef{ID: "checkpoint-root-child", TenantID: domain.TenantID("tenant-a")}, Parents: []CheckpointRef{source}}
	if err := svc.Put(ctx, rootChild); !errors.Is(err, ErrTombstonedAncestor) {
		t.Fatalf("Put with the erased source root as direct parent was accepted: %v", err)
	}
	if _, err := svc.TombstoneCascade(ctx, CheckpointRef{ID: "checkpoint-never", TenantID: domain.TenantID("tenant-a")}, artifactsecurity.EraseUserDeletion); !errors.Is(err, ErrNodeUnknown) {
		t.Fatalf("cascade of an unknown ref was accepted: %v", err)
	}
	// Unrelated subtrees keep growing normally.
	other := Node{Ref: CheckpointRef{ID: "checkpoint-unrelated", TenantID: domain.TenantID("tenant-a")}}
	if err := svc.Put(ctx, other); err != nil {
		t.Fatalf("unrelated Put rejected: %v", err)
	}
}

// P1-10: a Put racing a TombstoneCascade either lands before the cascade
// (and is then itself tombstoned as a descendant) or is rejected as a node
// under a tombstoned ancestor — it can never survive live under an erased
// subtree. Run under -race this also pins the single-lock-span invariant.
func TestConcurrentCascadeAndPutNeverLeavesLiveChildUnderErasedRoot(t *testing.T) {
	for attempt := 0; attempt < 32; attempt++ {
		svc := NewService()
		ctx := context.Background()
		tenantA := domain.TenantID("tenant-a")
		root := CheckpointRef{ID: "checkpoint-root", TenantID: tenantA}
		if err := svc.Put(ctx, Node{Ref: root}); err != nil {
			t.Fatalf("seed root: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		var putErr error
		go func() {
			defer wg.Done()
			_, _ = svc.TombstoneCascade(ctx, root, artifactsecurity.EraseUserDeletion)
		}()
		go func() {
			defer wg.Done()
			putErr = svc.Put(ctx, Node{Ref: CheckpointRef{ID: "checkpoint-racer", TenantID: tenantA}, Parents: []CheckpointRef{root}})
		}()
		wg.Wait()
		if putErr != nil && !errors.Is(putErr, ErrTombstonedAncestor) {
			t.Fatalf("racing Put failed with an unexpected error: %v", putErr)
		}
		if putErr == nil {
			// The Put landed first: the racer is a descendant of the root,
			// so the cascade (single lock span over snapshot+writes) must
			// have tombstoned it.
			tombstoned := false
			for _, tombstone := range svc.Tombstones() {
				if tombstone.Ref.ID == "checkpoint-racer" {
					tombstoned = true
				}
			}
			if !tombstoned {
				t.Fatal("Put that landed before the cascade left a live child under the erased root")
			}
		}
	}
}

// R6: the cascade reason must come from the closed vocabulary artifact erase
// shares. Free text, PII-looking values, and unknown codes are rejected
// before any tombstone is written, so nothing sensitive reaches lineage
// records through this seam.
func TestTombstoneCascadeRejectsFreeTextAndUnknownReasons(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	source := CheckpointRef{ID: "checkpoint-reason", TenantID: domain.TenantID("tenant-a")}
	if err := svc.Put(ctx, Node{Ref: source}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	child := CheckpointRef{ID: "checkpoint-reason-child", TenantID: domain.TenantID("tenant-a")}
	if err := svc.Put(ctx, Node{Ref: child, Parents: []CheckpointRef{source}}); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	for _, reason := range []artifactsecurity.EraseReason{
		"",
		"revoked",
		artifactsecurity.EraseReason("user zhou@example.com asked to delete payload " + strings.Repeat("A", 64)),
		artifactsecurity.EraseReason("PAYLOAD_ERASED"),
	} {
		if _, err := svc.TombstoneCascade(ctx, source, reason); !errors.Is(err, ErrCascadeReasonInvalid) {
			t.Fatalf("cascade reason %q = %v, want ErrCascadeReasonInvalid", reason, err)
		}
	}
	if recorded := svc.Tombstones(); len(recorded) != 0 {
		t.Fatalf("rejected reasons still wrote tombstones: %#v", recorded)
	}
	// The closed vocabulary itself still cascades.
	tombstones, err := svc.TombstoneCascade(ctx, source, artifactsecurity.EraseSecurityIncident)
	if err != nil {
		t.Fatalf("closed-vocabulary cascade: %v", err)
	}
	if len(tombstones) != 2 || tombstones[0].Reason != string(artifactsecurity.EraseSecurityIncident) {
		t.Fatalf("closed-vocabulary cascade = %#v", tombstones)
	}
}
