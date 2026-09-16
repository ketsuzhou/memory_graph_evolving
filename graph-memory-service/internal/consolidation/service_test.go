package consolidation_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/consolidation"
	"river2.dev/graph-memory-service/internal/domain"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type replayFunc func(context.Context, consolidation.DerivedProjection, consolidation.DerivedProjection, consolidation.RetrievalReplayPlan, consolidation.BacktestConfig) (consolidation.CandidateStats, error)

func (f replayFunc) Evaluate(ctx context.Context, base, candidate consolidation.DerivedProjection, plan consolidation.RetrievalReplayPlan, cfg consolidation.BacktestConfig) (consolidation.CandidateStats, error) {
	return f(ctx, base, candidate, plan, cfg)
}

type recordingStore struct {
	activity     domain.ConsolidationActivity
	head         domain.ProjectionHead
	projections  map[domain.ProjectionVersion]domain.DerivedProjection
	rounds       map[domain.ConsolidationRoundID]domain.RoundResult
	publishCalls int
}

func (s *recordingStore) Activity(context.Context, domain.TenantID, domain.SpaceID) (domain.ConsolidationActivity, error) {
	return s.activity, nil
}
func (s *recordingStore) ProjectionHead(context.Context, domain.TenantID, domain.SpaceID) (domain.ProjectionHead, error) {
	return s.head, nil
}
func (s *recordingStore) Projection(_ context.Context, _ domain.TenantID, _ domain.SpaceID, version domain.ProjectionVersion) (domain.DerivedProjection, error) {
	projection, ok := s.projections[version]
	if !ok {
		return domain.DerivedProjection{}, errors.New("projection not found")
	}
	return cloneProjection(projection), nil
}
func (s *recordingStore) Round(_ context.Context, _ domain.TenantID, _ domain.SpaceID, roundID domain.ConsolidationRoundID) (domain.RoundResult, bool, error) {
	result, ok := s.rounds[roundID]
	return result, ok, nil
}
func (s *recordingStore) RecordRound(_ context.Context, _ domain.TenantID, _ domain.SpaceID, round domain.RoundResult) error {
	if existing, ok := s.rounds[round.RoundID]; ok {
		if existing.Outcome == round.Outcome && existing.OperationDigest == round.OperationDigest {
			return nil
		}
		return errors.New("round conflict")
	}
	s.rounds[round.RoundID] = round
	return nil
}
func (s *recordingStore) PublishRound(_ context.Context, input domain.ConsolidationPublishInput) (domain.RoundResult, bool, error) {
	s.publishCalls++
	if input.ExpectedHead != s.head {
		return domain.RoundResult{}, false, errors.New("stale projection head")
	}
	if existing, ok := s.rounds[input.Round.RoundID]; ok {
		return existing, true, nil
	}
	s.rounds[input.Round.RoundID] = input.Round
	s.projections[input.Projection.Version] = cloneProjection(input.Projection)
	s.head = domain.ProjectionHead{
		TenantID: input.TenantID, SpaceID: input.SpaceID, Version: input.Projection.Version,
		Digest: input.Projection.Digest, EvidenceWatermark: input.Projection.EvidenceWatermark,
	}
	return input.Round, false, nil
}

func TestConsolidationThresholdsAndDefaultsAreFrozen(t *testing.T) {
	cfg := consolidation.DefaultConfig()
	wantWeights := consolidation.CostWeights{Round: 1, Tail: 0.5, Embed: 0.2, Node: 0.1, Graph: 0.05}
	if cfg.TriggerCommittedBatches != 50 || cfg.TriggerQueries != 200 || cfg.RecallTolerance != 0.02 || cfg.RoundsTolerance != 1 || cfg.CostWeights != wantWeights {
		t.Fatalf("default config = %#v, want frozen thresholds, tolerances, and weights", cfg)
	}

	tests := []struct {
		name    string
		batches int64
		queries int64
		want    bool
	}{
		{name: "below both thresholds", batches: 49, queries: 199},
		{name: "batch threshold is inclusive", batches: 50, want: true},
		{name: "query threshold is inclusive", queries: 200, want: true},
		{name: "either threshold is sufficient", batches: 1, queries: 200, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := consolidation.ShouldConsolidate(test.batches, test.queries, cfg); got != test.want {
				t.Fatalf("ShouldConsolidate(%d, %d) = %v, want %v", test.batches, test.queries, got, test.want)
			}
		})
	}
}

func TestConsolidationRoundSupportsExactlyTheEightFrozenOperations(t *testing.T) {
	refA := domain.EvidenceRef{BatchID: "batch-a", EventIDs: []string{"event-a"}}
	refB := domain.EvidenceRef{BatchID: "batch-b", EventIDs: []string{"event-b"}}
	tests := []struct {
		name  string
		op    consolidation.Operation
		check func(*testing.T, consolidation.DerivedProjection)
	}{
		{
			name: "add node", op: consolidation.Operation{Kind: consolidation.OperationAddNode, Node: &consolidation.ProjectionNode{ID: "node-c", Content: "C", EvidenceRefs: []domain.EvidenceRef{refA}}},
			check: func(t *testing.T, got consolidation.DerivedProjection) { requireNode(t, got, "node-c") },
		},
		{
			name: "update node", op: consolidation.Operation{Kind: consolidation.OperationUpdateNode, NodeID: "node-a", Node: &consolidation.ProjectionNode{ID: "node-a", Content: "A updated", EvidenceRefs: []domain.EvidenceRef{refB}}},
			check: func(t *testing.T, got consolidation.DerivedProjection) {
				node := requireNode(t, got, "node-a")
				if node.Content != "A updated" || !containsEvidence(node.EvidenceRefs, refA) || !containsEvidence(node.EvidenceRefs, refB) {
					t.Fatalf("updated node lost content or monotonic provenance: %#v", node)
				}
			},
		},
		{
			name: "delete node", op: consolidation.Operation{Kind: consolidation.OperationDeleteNode, NodeID: "node-b"},
			check: func(t *testing.T, got consolidation.DerivedProjection) {
				if findNode(got, "node-b") != nil {
					t.Fatal("deleted node remains in candidate projection")
				}
			},
		},
		{
			name: "merge nodes", op: consolidation.Operation{Kind: consolidation.OperationMergeNodes, InputNodeIDs: []consolidation.ProjectionNodeID{"node-a", "node-b"}, Node: &consolidation.ProjectionNode{ID: "node-c", Content: "A and B"}},
			check: func(t *testing.T, got consolidation.DerivedProjection) {
				merged := requireNode(t, got, "node-c")
				if !containsEvidence(merged.EvidenceRefs, refA) || !containsEvidence(merged.EvidenceRefs, refB) {
					t.Fatalf("merged node provenance = %#v, want union", merged.EvidenceRefs)
				}
				requireNode(t, got, "node-a")
				requireNode(t, got, "node-b")
			},
		},
		{
			name: "add hierarchy edge", op: consolidation.Operation{Kind: consolidation.OperationAddHierarchyEdge, Edge: &consolidation.ProjectionEdge{ID: "edge-h", Kind: consolidation.EdgeHierarchy, From: "node-a", To: "node-b", EvidenceRefs: []domain.EvidenceRef{refA}}},
			check: func(t *testing.T, got consolidation.DerivedProjection) {
				requireEdge(t, got, "edge-h", consolidation.EdgeHierarchy)
			},
		},
		{
			name: "add relation edge", op: consolidation.Operation{Kind: consolidation.OperationAddRelationEdge, Edge: &consolidation.ProjectionEdge{ID: "edge-r", Kind: consolidation.EdgeRelation, Relation: "supports", From: "node-b", To: "node-a", EvidenceRefs: []domain.EvidenceRef{refB}}},
			check: func(t *testing.T, got consolidation.DerivedProjection) {
				requireEdge(t, got, "edge-r", consolidation.EdgeRelation)
			},
		},
		{
			name: "prune edge", op: consolidation.Operation{Kind: consolidation.OperationPruneEdge, EdgeID: "edge-existing"},
			check: func(t *testing.T, got consolidation.DerivedProjection) {
				for _, edge := range got.Edges {
					if edge.ID == "edge-existing" {
						t.Fatal("pruned edge remains in candidate projection")
					}
				}
			},
		},
		{
			name: "submit", op: consolidation.Operation{Kind: consolidation.OperationSubmit},
			check: func(t *testing.T, got consolidation.DerivedProjection) {
				requireNode(t, got, "node-a")
				requireNode(t, got, "node-b")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, input := roundFixture(consolidation.ConsolidationRoundID("round-" + test.name))
			input.Operations = []consolidation.Operation{test.op}
			before := cloneProjection(store.projections[1])
			service := consolidation.New(store, passingReplay(), fixedClock{now: frozenNow}, consolidation.DefaultConfig())
			result, duplicate, err := service.Run(context.Background(), input)
			if err != nil {
				t.Fatalf("run round: %v", err)
			}
			if duplicate || result.Outcome != consolidation.RoundPublished || result.PublishedVersion == nil {
				t.Fatalf("round result = %#v, duplicate=%v", result, duplicate)
			}
			published := store.projections[*result.PublishedVersion]
			test.check(t, published)
			if !reflect.DeepEqual(store.projections[1], before) {
				t.Fatal("operation mutated the published base projection or its committed evidence references")
			}
		})
	}
}

func TestConsolidationReplayBoundaryAndFailedShadowAreNonPublishing(t *testing.T) {
	tests := []struct {
		name        string
		recall      float64
		baseline    float64
		replayError error
		wantOutcome consolidation.RoundOutcome
		wantPublish bool
	}{
		{name: "recall regression exactly at tolerance publishes", recall: 0.78, baseline: 0.80, wantOutcome: consolidation.RoundPublished, wantPublish: true},
		{name: "recall regression beyond tolerance rejects", recall: 0.779999, baseline: 0.80, wantOutcome: consolidation.RoundRejected},
		{name: "infrastructure invalid replay fails closed", replayError: errors.New("replay infrastructure invalid"), wantOutcome: consolidation.RoundFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, input := roundFixture(consolidation.ConsolidationRoundID("round-" + test.name))
			originalHead := store.head
			runner := replayFunc(func(_ context.Context, _, candidate consolidation.DerivedProjection, _ consolidation.RetrievalReplayPlan, cfg consolidation.BacktestConfig) (consolidation.CandidateStats, error) {
				if cfg.RecallTolerance != 0.02 {
					t.Fatalf("replay tolerance = %v, want 0.02", cfg.RecallTolerance)
				}
				if test.replayError != nil {
					return consolidation.CandidateStats{}, test.replayError
				}
				passed := test.recall >= test.baseline-cfg.RecallTolerance
				stats := consolidation.CandidateStats{Version: candidate.Version, Digest: candidate.Digest, Passed: passed, Recall: test.recall, BaselineRecall: test.baseline}
				if !passed {
					stats.GateFailures = []string{"recall_regression"}
				}
				return stats, nil
			})
			result, _, err := consolidation.New(store, runner, fixedClock{now: frozenNow}, consolidation.DefaultConfig()).Run(context.Background(), input)
			if test.replayError == nil && err != nil {
				t.Fatalf("run round: %v", err)
			}
			if result.RoundID != input.RoundID || result.Outcome != test.wantOutcome {
				t.Fatalf("round result = %#v, want outcome %q", result, test.wantOutcome)
			}
			if test.wantPublish {
				if store.head == originalHead || store.publishCalls != 1 {
					t.Fatalf("passing replay did not publish exactly once: head=%#v calls=%d", store.head, store.publishCalls)
				}
			} else if store.head != originalHead || len(store.projections) != 1 {
				t.Fatalf("failed shadow escaped: head=%#v projections=%d", store.head, len(store.projections))
			}
		})
	}
}

func TestConsolidationRoundReplayIsIdempotentAndStaleCASNeverRebases(t *testing.T) {
	t.Run("same round and operations return the original version", func(t *testing.T) {
		store, input := roundFixture("round-idempotent")
		service := consolidation.New(store, passingReplay(), fixedClock{now: frozenNow}, consolidation.DefaultConfig())
		first, duplicate, err := service.Run(context.Background(), input)
		if err != nil || duplicate || first.PublishedVersion == nil {
			t.Fatalf("first run = (%#v, duplicate=%v, %v)", first, duplicate, err)
		}
		second, duplicate, err := service.Run(context.Background(), input)
		if err != nil || !duplicate || second.PublishedVersion == nil || *second.PublishedVersion != *first.PublishedVersion {
			t.Fatalf("replay = (%#v, duplicate=%v, %v), want original version", second, duplicate, err)
		}
		if store.publishCalls != 1 || len(store.projections) != 2 {
			t.Fatalf("round replay published another version: calls=%d versions=%d", store.publishCalls, len(store.projections))
		}
	})

	t.Run("head change during replay is rejected without rebase", func(t *testing.T) {
		store, input := roundFixture("round-stale")
		runner := replayFunc(func(_ context.Context, _, candidate consolidation.DerivedProjection, _ consolidation.RetrievalReplayPlan, _ consolidation.BacktestConfig) (consolidation.CandidateStats, error) {
			store.head = domain.ProjectionHead{TenantID: "tenant-1", SpaceID: "space-1", Version: 2, Digest: "other-writer", EvidenceWatermark: 10}
			store.projections[2] = domain.DerivedProjection{TenantID: "tenant-1", SpaceID: "space-1", Version: 2, ParentVersion: 1, Digest: "other-writer"}
			return consolidation.CandidateStats{Version: candidate.Version, Digest: candidate.Digest, Passed: true, Recall: 1, BaselineRecall: 1}, nil
		})
		result, _, err := consolidation.New(store, runner, fixedClock{now: frozenNow}, consolidation.DefaultConfig()).Run(context.Background(), input)
		if err == nil {
			t.Fatal("stale CAS unexpectedly published")
		}
		if result.RoundID != input.RoundID || store.head.Version != 2 || store.head.Digest != "other-writer" || len(store.projections) != 2 {
			t.Fatalf("stale round rebased or changed winner: result=%#v head=%#v versions=%d", result, store.head, len(store.projections))
		}
	})
}

var frozenNow = time.Date(2026, time.September, 8, 13, 0, 0, 0, time.UTC)

func roundFixture(roundID consolidation.ConsolidationRoundID) (*recordingStore, consolidation.RoundInput) {
	base := domain.DerivedProjection{
		TenantID: "tenant-1", SpaceID: "space-1", Version: 1, EvidenceWatermark: 10, QueryWatermark: 20,
		Digest: "base-digest", RoundID: "round-base", CreatedAt: frozenNow.Add(-time.Hour),
		Nodes: []domain.ProjectionNode{
			{ID: "node-a", Content: "A", EvidenceRefs: []domain.EvidenceRef{{BatchID: "batch-a", EventIDs: []string{"event-a"}}}},
			{ID: "node-b", Content: "B", EvidenceRefs: []domain.EvidenceRef{{BatchID: "batch-b", EventIDs: []string{"event-b"}}}},
		},
		Edges: []domain.ProjectionEdge{{ID: "edge-existing", Kind: domain.EdgeRelation, Relation: "related", From: "node-a", To: "node-b"}},
	}
	store := &recordingStore{
		activity:    domain.ConsolidationActivity{SpaceID: "space-1", CommittedBatchOrdinal: 10, QueryOrdinal: 20},
		head:        domain.ProjectionHead{TenantID: "tenant-1", SpaceID: "space-1", Version: 1, Digest: "base-digest", EvidenceWatermark: 10},
		projections: map[domain.ProjectionVersion]domain.DerivedProjection{1: cloneProjection(base)},
		rounds:      make(map[domain.ConsolidationRoundID]domain.RoundResult),
	}
	input := consolidation.RoundInput{
		TenantID: "tenant-1", SpaceID: "space-1", RoundID: roundID, BaseVersion: 1, BaseDigest: "base-digest",
		EvidenceWatermark: 10, QueryWatermark: 20, Operations: []consolidation.Operation{{Kind: consolidation.OperationSubmit}},
		ReplayPlan: consolidation.RetrievalReplayPlan{ID: "plan-" + string(roundID), BaseVersion: 1, BaseDigest: "base-digest", Cases: []consolidation.RetrievalReplayCase{}, RetrievalPolicyVersion: "retrieval-v1", EmbeddingPolicyVersion: "embedding-v1", EvaluatorVersion: "evaluator-v1", MaxResults: 10, MaxRounds: 3},
	}
	return store, input
}

func passingReplay() consolidation.ReplayRunner {
	return replayFunc(func(_ context.Context, _, candidate consolidation.DerivedProjection, _ consolidation.RetrievalReplayPlan, _ consolidation.BacktestConfig) (consolidation.CandidateStats, error) {
		return consolidation.CandidateStats{Version: candidate.Version, Digest: candidate.Digest, Passed: true, Recall: 1, BaselineRecall: 1, Queries: []consolidation.QueryBacktestStat{}}, nil
	})
}

func cloneProjection(projection domain.DerivedProjection) domain.DerivedProjection {
	cloned := projection
	cloned.Nodes = append([]domain.ProjectionNode(nil), projection.Nodes...)
	for index := range cloned.Nodes {
		cloned.Nodes[index].EvidenceRefs = append([]domain.EvidenceRef(nil), projection.Nodes[index].EvidenceRefs...)
	}
	cloned.Edges = append([]domain.ProjectionEdge(nil), projection.Edges...)
	return cloned
}

func findNode(projection consolidation.DerivedProjection, id consolidation.ProjectionNodeID) *consolidation.ProjectionNode {
	for index := range projection.Nodes {
		if projection.Nodes[index].ID == id {
			return &projection.Nodes[index]
		}
	}
	return nil
}

func requireNode(t *testing.T, projection consolidation.DerivedProjection, id consolidation.ProjectionNodeID) *consolidation.ProjectionNode {
	t.Helper()
	node := findNode(projection, id)
	if node == nil {
		t.Fatalf("projection does not contain node %q: %#v", id, projection.Nodes)
	}
	return node
}

func requireEdge(t *testing.T, projection consolidation.DerivedProjection, id consolidation.ProjectionEdgeID, kind consolidation.EdgeKind) {
	t.Helper()
	for _, edge := range projection.Edges {
		if edge.ID == id && edge.Kind == kind {
			return
		}
	}
	t.Fatalf("projection does not contain %q edge %q: %#v", kind, id, projection.Edges)
}

func containsEvidence(refs []domain.EvidenceRef, want domain.EvidenceRef) bool {
	for _, ref := range refs {
		if reflect.DeepEqual(ref, want) {
			return true
		}
	}
	return false
}

func TestConsolidationOperationFailureDiscardsShadowAndPreservesPublishedVersion(t *testing.T) {
	store, input := roundFixture("round-invalid-shadow")
	input.Operations = []consolidation.Operation{{
		Kind: consolidation.OperationUpdateNode, NodeID: "missing-node",
		Node: &consolidation.ProjectionNode{ID: "missing-node", Content: "must not escape"},
	}}
	beforeHead := store.head
	beforeProjection := cloneProjection(store.projections[1])
	replayCalled := false
	runner := replayFunc(func(context.Context, consolidation.DerivedProjection, consolidation.DerivedProjection, consolidation.RetrievalReplayPlan, consolidation.BacktestConfig) (consolidation.CandidateStats, error) {
		replayCalled = true
		return consolidation.CandidateStats{Passed: true}, nil
	})

	result, _, err := consolidation.New(store, runner, fixedClock{now: frozenNow}, consolidation.DefaultConfig()).Run(context.Background(), input)
	if err != nil {
		t.Fatalf("invalid shadow operation should produce a durable rejection: %v", err)
	}
	if result.RoundID != input.RoundID || result.Outcome != consolidation.RoundRejected || len(result.Rejections) != 1 || result.Rejections[0].Kind != consolidation.OperationUpdateNode {
		t.Fatalf("operation rejection = %#v", result)
	}
	if replayCalled || store.publishCalls != 0 || store.head != beforeHead || !reflect.DeepEqual(store.projections[1], beforeProjection) {
		t.Fatalf("failed shadow escaped: replay=%v publish=%d head=%#v projection=%#v", replayCalled, store.publishCalls, store.head, store.projections[1])
	}
}

func TestConsolidationRecordedRoundBindsFullOperationList(t *testing.T) {
	store, input := roundFixture("round-operation-digest")
	input.Operations = []consolidation.Operation{
		{Kind: consolidation.OperationSubmit},
		{
			Kind: consolidation.OperationAddNode,
			Node: &consolidation.ProjectionNode{
				ID: "post-submit-node", Content: "must be part of the request digest",
			},
		},
	}
	service := consolidation.New(store, passingReplay(), fixedClock{now: frozenNow}, consolidation.DefaultConfig())

	first, duplicate, err := service.Run(context.Background(), input)
	if err != nil || duplicate || first.Outcome != consolidation.RoundRejected || first.OperationDigest == "" {
		t.Fatalf("first rejected round = (%#v, duplicate=%v, err=%v)", first, duplicate, err)
	}
	stored, found, err := store.Round(context.Background(), input.TenantID, input.SpaceID, input.RoundID)
	if err != nil || !found || !reflect.DeepEqual(stored, first) {
		t.Fatalf("stored rejected round = (%#v, found=%v, err=%v), want first result", stored, found, err)
	}

	second, duplicate, err := service.Run(context.Background(), input)
	if err != nil || !duplicate || !reflect.DeepEqual(second, first) {
		t.Fatalf("identical round replay = (%#v, duplicate=%v, err=%v), want stored result", second, duplicate, err)
	}
	if store.publishCalls != 0 {
		t.Fatalf("rejected round was published %d times", store.publishCalls)
	}

	mutated := input
	mutated.Operations = append([]consolidation.Operation(nil), input.Operations...)
	mutated.Operations[1].Node = &consolidation.ProjectionNode{
		ID: "post-submit-node", Content: "different operation list",
	}
	conflict, duplicate, err := service.Run(context.Background(), mutated)
	if err == nil || !duplicate || conflict.Outcome != consolidation.RoundFailed || conflict.OperationDigest == first.OperationDigest {
		t.Fatalf("mutated round replay = (%#v, duplicate=%v, err=%v), want digest conflict", conflict, duplicate, err)
	}
	stored, found, err = store.Round(context.Background(), input.TenantID, input.SpaceID, input.RoundID)
	if err != nil || !found || !reflect.DeepEqual(stored, first) {
		t.Fatalf("conflict overwrote stored round = (%#v, found=%v, err=%v)", stored, found, err)
	}
}
