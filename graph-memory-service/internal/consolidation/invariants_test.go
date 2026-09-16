package consolidation_test

import (
	"context"
	"testing"

	"river2.dev/graph-memory-service/internal/consolidation"
)

func TestConsolidationRejectsPostSubmitOperations(t *testing.T) {
	t.Parallel()

	store, input := roundFixture("round-post-submit")
	input.Operations = []consolidation.Operation{
		{Kind: consolidation.OperationSubmit},
		{
			Kind: consolidation.OperationAddNode,
			Node: &consolidation.ProjectionNode{ID: "node-after-submit", Content: "must be rejected"},
		},
	}
	replayCalled := false
	runner := replayFunc(func(_ context.Context, _, candidate consolidation.DerivedProjection, _ consolidation.RetrievalReplayPlan, _ consolidation.BacktestConfig) (consolidation.CandidateStats, error) {
		replayCalled = true
		return consolidation.CandidateStats{Version: candidate.Version, Digest: candidate.Digest, Passed: true}, nil
	})

	result, _, err := consolidation.New(store, runner, fixedClock{now: frozenNow}, consolidation.DefaultConfig()).Run(context.Background(), input)
	if err != nil {
		t.Fatalf("post-submit operation should produce a durable rejection: %v", err)
	}
	if result.Outcome != consolidation.RoundRejected {
		t.Fatalf("outcome = %q, want rejected", result.Outcome)
	}
	if replayCalled || store.publishCalls != 0 {
		t.Fatalf("post-submit round escaped to replay/publish: replay=%v publish=%d", replayCalled, store.publishCalls)
	}
}

func TestConsolidationRejectsHierarchyCycles(t *testing.T) {
	t.Parallel()

	store, input := roundFixture("round-hierarchy-cycle")
	base := store.projections[1]
	base.Edges = append(base.Edges, consolidation.ProjectionEdge{
		ID: "hierarchy-a-b", Kind: consolidation.EdgeHierarchy,
		Relation: "contains", From: "node-a", To: "node-b",
	})
	store.projections[1] = base
	input.Operations = []consolidation.Operation{
		{
			Kind: consolidation.OperationAddHierarchyEdge,
			Edge: &consolidation.ProjectionEdge{
				ID: "hierarchy-b-a", Kind: consolidation.EdgeHierarchy,
				Relation: "contains", From: "node-b", To: "node-a",
			},
		},
		{Kind: consolidation.OperationSubmit},
	}
	replayCalled := false
	runner := replayFunc(func(_ context.Context, _, candidate consolidation.DerivedProjection, _ consolidation.RetrievalReplayPlan, _ consolidation.BacktestConfig) (consolidation.CandidateStats, error) {
		replayCalled = true
		return consolidation.CandidateStats{Version: candidate.Version, Digest: candidate.Digest, Passed: true}, nil
	})

	result, _, err := consolidation.New(store, runner, fixedClock{now: frozenNow}, consolidation.DefaultConfig()).Run(context.Background(), input)
	if err != nil {
		t.Fatalf("hierarchy cycle should produce a durable rejection: %v", err)
	}
	if result.Outcome != consolidation.RoundRejected {
		t.Fatalf("outcome = %q, want rejected", result.Outcome)
	}
	if replayCalled || store.publishCalls != 0 {
		t.Fatalf("cyclic hierarchy escaped to replay/publish: replay=%v publish=%d", replayCalled, store.publishCalls)
	}
}
