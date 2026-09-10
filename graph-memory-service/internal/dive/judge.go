package dive

import (
	"context"
	"errors"
	"fmt"
	"math"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

// Scorer is an internal deterministic/evaluator seam. It receives only the
// server-owned trajectory and must return finite dimensions in [0,1].
type Scorer interface {
	Score(context.Context, domain.DiveTrajectory) (domain.DiveDimensions, error)
}

type Judge struct {
	store  ports.DiveStore
	scorer Scorer
	clock  ports.Clock
}

func New(store ports.DiveStore, scorer Scorer, clock ports.Clock) *Judge {
	return &Judge{store: store, scorer: scorer, clock: clock}
}

// JudgeSubmitted scores one terminal exploration trajectory from server-owned
// state and stores the result idempotently. Found and miss trajectories are
// graded by the scorer; violation trajectories bypass it with zero dimensions.
func (j *Judge) JudgeSubmitted(ctx context.Context, tenantID domain.TenantID, sessionID domain.ExplorationSessionID) (domain.DiveResult, bool, error) {
	trajectory, err := j.store.DiveTrajectory(ctx, tenantID, sessionID)
	if err != nil {
		return domain.DiveResult{}, false, err
	}
	classification, err := ClassifyTrajectory(trajectory)
	if err != nil {
		return domain.DiveResult{}, false, err
	}
	items, err := BuildInformationItems(trajectory)
	if err != nil {
		return domain.DiveResult{}, false, err
	}
	score := domain.DiveTrajectoryScore{
		SessionID: sessionID, Classification: classification,
		Rounds: trajectory.Rounds,
	}
	switch classification {
	case domain.DiveFound, domain.DiveMiss:
		dimensions, err := j.scorer.Score(ctx, trajectory)
		if err != nil {
			return domain.DiveResult{}, false, err
		}
		overall, err := OverallScore(dimensions)
		if err != nil {
			return domain.DiveResult{}, false, err
		}
		score.Relevance, score.Groundedness, score.Completeness, score.Overall = dimensions.Relevance, dimensions.Groundedness, dimensions.Completeness, overall
	}
	return j.store.PutDiveResult(ctx, domain.DiveResult{
		TenantID: tenantID, SessionID: sessionID, Score: score, Items: items, EvaluatedAt: j.clock.Now(),
	})
}

// ClassifyTrajectory maps a trajectory onto exactly one of the five frozen
// terminal classes. Anything else is a protocol violation, not a guess.
func ClassifyTrajectory(trajectory domain.DiveTrajectory) (domain.DiveClassification, error) {
	switch trajectory.State {
	case "submitted":
		if trajectory.Found {
			return domain.DiveFound, nil
		}
		return domain.DiveMiss, nil
	case "error", "budget_exhausted", "timeout":
		if domain.DiveClassification(trajectory.State) != trajectory.TerminalReason {
			return "", fmt.Errorf("dive classify: terminal state %q does not match terminal reason %q", trajectory.State, trajectory.TerminalReason)
		}
		return trajectory.TerminalReason, nil
	default:
		return "", fmt.Errorf("dive classify: session %s is not terminal (state %q)", trajectory.SessionID, trajectory.State)
	}
}

// BuildInformationItems returns the first-served occurrence of every citation
// in served order. Later re-serves of a citation never overwrite the original
// statement, score, or version.
func BuildInformationItems(trajectory domain.DiveTrajectory) ([]domain.DiveInformationItem, error) {
	submitted := map[string]bool{}
	for _, citationID := range trajectory.SubmittedCitationIDs {
		submitted[citationID] = true
	}
	items := make([]domain.DiveInformationItem, 0, len(trajectory.ServedItems))
	seen := map[string]bool{}
	for _, served := range trajectory.ServedItems {
		citationID := served.Citation.ID
		if seen[citationID] {
			continue
		}
		seen[citationID] = true
		items = append(items, domain.DiveInformationItem{
			CitationID: citationID, Statement: served.Content, SourceSpaceID: served.SourceSpaceID,
			MemoryVersion: served.MemoryVersion, EvidenceBatchID: served.Citation.EvidenceBatchID,
			EventIDs: served.Citation.EventIDs, RetrievalScore: served.Score,
			Submitted: submitted[citationID], Authoritative: true,
		})
	}
	return items, nil
}

// OverallScore is the weakest dimension: no dimension compensates another.
func OverallScore(dimensions domain.DiveDimensions) (float64, error) {
	for _, value := range []float64{dimensions.Relevance, dimensions.Groundedness, dimensions.Completeness} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, errors.New("dive score: dimensions must be finite")
		}
		if value < 0 || value > 1 {
			return 0, errors.New("dive score: dimensions must stay in [0,1]")
		}
	}
	overall := dimensions.Relevance
	if dimensions.Groundedness < overall {
		overall = dimensions.Groundedness
	}
	if dimensions.Completeness < overall {
		overall = dimensions.Completeness
	}
	return overall, nil
}

// DeterministicViolationReward is zero overall quality minus the cost of the
// rounds burned (at least one) minus a fixed violation penalty — strictly
// below any graded trajectory of the same rounds and a pure function of its
// inputs.
func DeterministicViolationReward(wRound float64, rounds int) float64 {
	if rounds < 1 {
		rounds = 1
	}
	return -wRound*float64(rounds) - 1
}

// DefaultConsolidationRewardWeights is the conservative bootstrap: quality
// deltas dominate; absolute efficiency costs apply a small brake.
func DefaultConsolidationRewardWeights() domain.ConsolidationRewardWeights {
	return domain.ConsolidationRewardWeights{Quality: 1.0, Efficiency: 0.01}
}

// HardGateFailureReward is the deterministic negative reward for a candidate
// that failed the consolidation hard gates: never a formula value computed
// from rejected stats.
const HardGateFailureReward = -1.0

// ConsolidationReward computes one candidate's reward from its own baseline
// deltas and absolute costs:
//
//	quality_delta    = recall_delta + coverage_delta − regression_penalty
//	efficiency_delta = baseline_rounds − candidate_rounds − embedKiB − node_churn − edge_churn
//	reward           = w_quality × quality_delta + w_efficiency × efficiency_delta
//
// Gate failure short-circuits to HardGateFailureReward and cannot be
// compensated by any component.
func ConsolidationReward(input domain.ConsolidationRewardInput, weights domain.ConsolidationRewardWeights) (float64, domain.RewardComponents) {
	if !input.Passed {
		return HardGateFailureReward, domain.RewardComponents{Source: "hard_gate_failure"}
	}
	components := domain.RewardComponents{
		Source:        "consolidation",
		RecallDelta:   input.Recall - input.BaselineRecall,
		CoverageDelta: input.Coverage - input.BaselineCoverage,
	}
	if input.Queries > 0 {
		components.RegressionPenalty = float64(input.Regressions) / float64(input.Queries)
	}
	embedKiB := float64(input.EmbedBytes) / 1024
	components.QualityDelta = components.RecallDelta + components.CoverageDelta - components.RegressionPenalty
	components.EfficiencyDelta = input.BaselineRounds - input.CandidateRounds - embedKiB -
		float64(input.ChangedNodes) - float64(input.EdgeChurn)
	return weights.Quality*components.QualityDelta + weights.Efficiency*components.EfficiencyDelta, components
}
