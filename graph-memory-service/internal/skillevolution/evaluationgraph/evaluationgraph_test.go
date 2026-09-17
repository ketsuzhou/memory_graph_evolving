package evaluationgraph

import (
	"errors"
	"reflect"
	"testing"
)

func TestBuildSameCanonicalInputProducesDeterministicDigestNodesEdgesAndOrder(t *testing.T) {
	fixture := consolidatedFixture(t)

	first, err := Build(fixture)
	if err != nil { t.Fatal(err) }
	second, err := Build(fixture)
	if err != nil { t.Fatal(err) }

	if first.ProjectionDigest != second.ProjectionDigest {
		t.Fatalf("digest differs: %q != %q", first.ProjectionDigest, second.ProjectionDigest)
	}
	if !reflect.DeepEqual(first.Nodes, second.Nodes) || !reflect.DeepEqual(first.Edges, second.Edges) {
		t.Fatalf("same canonical input produced a different ordered graph\nfirst: %#v %#v\nsecond: %#v %#v", first.Nodes, first.Edges, second.Nodes, second.Edges)
	}
	wantNodes := []string{
		"skill://evaluation/alpha@1",
		"skill://evaluation/beta@2",
		"skill://evaluation/gamma@1",
	}
	for i, want := range wantNodes {
		if got := first.Nodes[i].Ref.String(); got != want { t.Fatalf("node[%d] = %q, want %q", i, got, want) }
	}
	got, err := first.Explore(ScopeEvaluation, RevisionRef{LineageID: "alpha", Revision: 1}, 2)
	if err != nil { t.Fatal(err) }
	if len(got) != 3 || got[0].Ref.LineageID != "alpha" || got[1].Ref.LineageID != "beta" || got[2].Ref.LineageID != "gamma" {
		t.Fatalf("deterministic multi-hop exploration = %#v", got)
	}
}

func TestNonActiveProposalIsReadableOnlyInEvaluationScope(t *testing.T) {
	view, err := Build(consolidatedFixture(t))
	if err != nil { t.Fatal(err) }
	ref := RevisionRef{LineageID: "alpha", Revision: 1}

	node, err := view.Get(ScopeEvaluation, ref)
	if err != nil { t.Fatal(err) }
	if node.Body != "advisory alpha" { t.Fatalf("evaluation get body = %q", node.Body) }
	if _, err := view.Explore(ScopeEvaluation, ref, 1); err != nil { t.Fatalf("evaluation explore: %v", err) }
	if _, err := view.Get(ScopeRuntime, ref); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("runtime get error = %v, want scope denial", err)
	}
	if _, err := view.Explore(ScopeRuntime, ref, 1); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("runtime explore error = %v, want scope denial", err)
	}
}

func TestBuildFailsClosedForProjectionPinWatermarkAndDigestMismatch(t *testing.T) {
	for name, mutate := range map[string]func(*CanonicalFixture){
		"ledger digest": func(f *CanonicalFixture) { f.Pin.LedgerDigest = "sha256:wrong" },
		"watermark": func(f *CanonicalFixture) { f.Pin.Watermark = "evaluation@0:wrong" },
		"watermark digest": func(f *CanonicalFixture) { f.Pin.WatermarkDigest = "sha256:wrong" },
		"record digest": func(f *CanonicalFixture) { f.Records[0].Digest = "sha256:wrong" },
	} {
		t.Run(name, func(t *testing.T) {
			fixture := cloneFixture(t, consolidatedFixture(t))
			mutate(&fixture)
			if _, err := Build(fixture); !errors.Is(err, ErrPinMismatch) {
				t.Fatalf("Build() error = %v, want pin mismatch", err)
			}
		})
	}
}

func TestBuildRejectsHeldOutFeedbackAsProjectionInput(t *testing.T) {
	alpha := RevisionRef{LineageID: "alpha", Revision: 1}
	fixture, err := NewFixture([]CanonicalRecord{
		{Sequence: 1, ID: "proposal-alpha", Kind: RecordProposal, Proposal: &Proposal{Ref: alpha, Body: "advisory alpha", Advisory: true}},
		{Sequence: 2, ID: "consolidate-alpha", Kind: RecordConsolidation, Consolidation: &Consolidation{Target: alpha, Disposition: "retain"}},
		{Sequence: 3, ID: "held-out-1", Kind: RecordHeldOutFeedback},
	})
	if err != nil { t.Fatal(err) }
	if _, err := Build(fixture); !errors.Is(err, ErrHeldOutFeedbackInput) {
		t.Fatalf("Build() error = %v, want held-out feedback rejection", err)
	}
}

func TestProjectionDeletionRebuildsByteEquivalentViewFromCanonicalFixture(t *testing.T) {
	fixture := consolidatedFixture(t)
	original, err := Build(fixture)
	if err != nil { t.Fatal(err) }
	originalBytes, err := original.Bytes()
	if err != nil { t.Fatal(err) }

	cache := &Cache{}
	cache.Replace(original)
	cache.Delete()
	if _, ok := cache.Snapshot(); ok { t.Fatal("deleted projection remained in cache") }

	rebuilt, err := Build(fixture)
	if err != nil { t.Fatal(err) }
	rebuiltBytes, err := rebuilt.Bytes()
	if err != nil { t.Fatal(err) }
	if string(originalBytes) != string(rebuiltBytes) {
		t.Fatalf("rebuilt view differs\noriginal: %s\nrebuilt: %s", originalBytes, rebuiltBytes)
	}
}

func consolidatedFixture(t *testing.T) CanonicalFixture {
	t.Helper()
	alpha := RevisionRef{LineageID: "alpha", Revision: 1}
	beta := RevisionRef{LineageID: "beta", Revision: 2}
	gamma := RevisionRef{LineageID: "gamma", Revision: 1}
	fixture, err := NewFixture([]CanonicalRecord{
		{Sequence: 1, ID: "proposal-alpha", Kind: RecordProposal, Proposal: &Proposal{Ref: alpha, Body: "advisory alpha", Advisory: true}},
		{Sequence: 2, ID: "proposal-gamma", Kind: RecordProposal, Proposal: &Proposal{Ref: gamma, Body: "advisory gamma", Advisory: true}},
		{Sequence: 3, ID: "proposal-beta", Kind: RecordProposal, Proposal: &Proposal{Ref: beta, Body: "advisory beta", Advisory: true}},
		{Sequence: 4, ID: "consolidate-beta", Kind: RecordConsolidation, Consolidation: &Consolidation{Target: beta, Disposition: "specialize"}},
		{Sequence: 5, ID: "consolidate-alpha", Kind: RecordConsolidation, Consolidation: &Consolidation{Target: alpha, Disposition: "retain"}},
		{Sequence: 6, ID: "consolidate-gamma", Kind: RecordConsolidation, Consolidation: &Consolidation{Target: gamma, Disposition: "merge"}},
		{Sequence: 7, ID: "alpha-beta", Kind: RecordRelation, Relation: &Relation{From: alpha, To: beta, Kind: "specializes"}},
		{Sequence: 8, ID: "beta-gamma", Kind: RecordRelation, Relation: &Relation{From: beta, To: gamma, Kind: "related_to"}},
	})
	if err != nil { t.Fatal(err) }
	return fixture
}

func cloneFixture(t *testing.T, fixture CanonicalFixture) CanonicalFixture {
	t.Helper()
	return CanonicalFixture{Pin: fixture.Pin, Records: cloneRecords(fixture.Records)}
}
