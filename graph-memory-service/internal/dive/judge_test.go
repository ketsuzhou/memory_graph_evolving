package dive_test

import (
	"math"
	"reflect"
	"testing"

	"river2.dev/graph-memory-service/internal/dive"
	"river2.dev/graph-memory-service/internal/domain"
)

func TestClassifyTrajectoryUsesOnlyFrozenTerminalClasses(t *testing.T) {
	tests := []struct {
		name       string
		trajectory domain.DiveTrajectory
		want       domain.DiveClassification
	}{
		{name: "submitted found", trajectory: domain.DiveTrajectory{State: "submitted", Found: true}, want: domain.DiveFound},
		{name: "submitted miss", trajectory: domain.DiveTrajectory{State: "submitted", Found: false}, want: domain.DiveMiss},
		{name: "error", trajectory: domain.DiveTrajectory{State: "error", TerminalReason: domain.DiveError}, want: domain.DiveError},
		{name: "budget exhausted", trajectory: domain.DiveTrajectory{State: "budget_exhausted", TerminalReason: domain.DiveBudgetExhausted}, want: domain.DiveBudgetExhausted},
		{name: "timeout", trajectory: domain.DiveTrajectory{State: "timeout", TerminalReason: domain.DiveTimeout}, want: domain.DiveTimeout},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := dive.ClassifyTrajectory(test.trajectory)
			if err != nil {
				t.Fatalf("classify trajectory: %v", err)
			}
			if got != test.want {
				t.Fatalf("classification = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBuildInformationItemsDeduplicatesCitationByFirstServedOrder(t *testing.T) {
	firstA := domain.RecallItem{
		Content: "Alpha fact", SourceSpaceID: "space-1", MemoryVersion: 3,
		Citation: domain.Citation{ID: "citation-a", EvidenceBatchID: "batch-a", EventIDs: []string{"event-a"}}, Score: 0.9,
	}
	itemB := domain.RecallItem{
		Content: "Beta fact", SourceSpaceID: "space-1", MemoryVersion: 4,
		Citation: domain.Citation{ID: "citation-b", EvidenceBatchID: "batch-b", EventIDs: []string{"event-b"}}, Score: 0.8,
	}
	laterA := firstA
	laterA.Content = "Overwritten alpha"
	laterA.Score = 0.1

	got, err := dive.BuildInformationItems(domain.DiveTrajectory{
		SessionID: "session-items", State: "submitted", Found: true,
		ServedItems:          []domain.RecallItem{firstA, itemB, laterA},
		SubmittedCitationIDs: []string{"citation-a", "citation-a"},
	})
	if err != nil {
		t.Fatalf("build information items: %v", err)
	}
	want := []domain.DiveInformationItem{
		{
			CitationID: "citation-a", Statement: "Alpha fact", SourceSpaceID: "space-1", MemoryVersion: 3,
			EvidenceBatchID: "batch-a", EventIDs: []string{"event-a"}, RetrievalScore: 0.9,
			Submitted: true, Authoritative: true,
		},
		{
			CitationID: "citation-b", Statement: "Beta fact", SourceSpaceID: "space-1", MemoryVersion: 4,
			EvidenceBatchID: "batch-b", EventIDs: []string{"event-b"}, RetrievalScore: 0.8,
			Submitted: false, Authoritative: true,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("information items:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestDiveRewardsAreDeterministicAndNonCompensating(t *testing.T) {
	t.Run("overall score is the weakest dimension", func(t *testing.T) {
		got, err := dive.OverallScore(domain.DiveDimensions{Relevance: 0.8, Groundedness: 0.4, Completeness: 0.7})
		if err != nil {
			t.Fatalf("overall score: %v", err)
		}
		if got != 0.4 {
			t.Fatalf("overall = %v, want 0.4", got)
		}
	})

	t.Run("violations receive a strict deterministic penalty", func(t *testing.T) {
		if got := dive.DeterministicViolationReward(0.1, 3); !closeEnough(got, -1.3) {
			t.Fatalf("violation reward = %v, want -1.3", got)
		}
		if got := dive.DeterministicViolationReward(0.1, 0); !closeEnough(got, -1.1) {
			t.Fatalf("zero-round violation reward = %v, want -1.1", got)
		}
	})

	t.Run("consolidation reward preserves quality and absolute cost components", func(t *testing.T) {
		input := domain.ConsolidationRewardInput{
			Passed: true, Recall: 0.9, BaselineRecall: 0.8, Coverage: 0.8, BaselineCoverage: 0.7,
			Regressions: 1, Queries: 4, CandidateRounds: 3, BaselineRounds: 4,
			EmbedBytes: 2048, ChangedNodes: 2, EdgeChurn: 1,
		}
		got, components := dive.ConsolidationReward(input, domain.ConsolidationRewardWeights{Quality: 1, Efficiency: 0.01})
		if !closeEnough(got, -0.09) {
			t.Fatalf("consolidation reward = %v, want -0.09", got)
		}
		if !closeEnough(components.QualityDelta, -0.05) || !closeEnough(components.EfficiencyDelta, -4) {
			t.Fatalf("reward components = %#v", components)
		}
	})

	t.Run("hard gate failure cannot be compensated", func(t *testing.T) {
		got, components := dive.ConsolidationReward(domain.ConsolidationRewardInput{
			Passed: false, GateFailures: []string{"recall_regression"}, Recall: 1, Coverage: 1, BaselineRounds: 100,
		}, domain.ConsolidationRewardWeights{Quality: 10, Efficiency: 10})
		if got != -1 || components.Source != "hard_gate_failure" {
			t.Fatalf("hard-gate reward = %v, components=%#v", got, components)
		}
	})
}

func closeEnough(got, want float64) bool {
	return math.Abs(got-want) < 1e-12
}
