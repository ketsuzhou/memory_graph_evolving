package main

import (
	"context"
	"fmt"
)

// graphBatchStrategyID is intentionally distinct from the legacy batch arm.
// Authority is the GMS canonical ledger plus a sealed EvaluationFreezeManifest.
// runner-local []skillProposal and Markdown/hash-prefix ledgers are not writers.
const graphBatchStrategyID = "warm-skill-graph-batch"

const (
	defaultMaxGraphStepsPerEpisode        = 12
	defaultMaxOffersPerAgentPerCheckpoint = 3
	defaultMaxMemoryTurnsPerEpisode       = 6
	graphBatchEmptyGraphTerminal          = "no_candidate_from_empty_graph"
	graphBatchMentionInterruptGrace       = "50ms"
	graphBatchMentionKillGrace            = "50ms"
)

// graphBatchFrozenConfig is the generation-0 configuration recorded verbatim
// in every skeleton artifact. Budgets and interrupt graces are the contract
// defaults composed by the canonical pipeline.
type graphBatchFrozenConfig struct {
	Strategy                       string `json:"strategy"`
	InitialSkillSnapshot           string `json:"initial_skill_snapshot"`
	SkillFeedbackPersistence       string `json:"skill_feedback_persistence"`
	MaxGraphStepsPerEpisode        int    `json:"max_graph_steps_per_episode"`
	MaxOffersPerAgentPerCheckpoint int    `json:"max_offers_per_agent_per_checkpoint"`
	MaxMemoryTurnsPerEpisode       int    `json:"max_memory_turns_per_episode"`
	MentionInterruptGrace          string `json:"mention_interrupt_grace"`
	MentionKillGrace               string `json:"mention_kill_grace"`
	SkillAdaptation                string `json:"skill_adaptation"`
	TaskContextMonitoring          string `json:"task_context_monitoring"`
	TestFeedbackSink               string `json:"test_feedback_sink"`
}

func defaultGraphBatchFrozenConfig() graphBatchFrozenConfig {
	return graphBatchFrozenConfig{
		Strategy:                       graphBatchStrategyID,
		InitialSkillSnapshot:           "empty",
		SkillFeedbackPersistence:       "train_only",
		MaxGraphStepsPerEpisode:        defaultMaxGraphStepsPerEpisode,
		MaxOffersPerAgentPerCheckpoint: defaultMaxOffersPerAgentPerCheckpoint,
		MaxMemoryTurnsPerEpisode:       defaultMaxMemoryTurnsPerEpisode,
		MentionInterruptGrace:          graphBatchMentionInterruptGrace,
		MentionKillGrace:               graphBatchMentionKillGrace,
		SkillAdaptation:                "optional",
		TaskContextMonitoring:          "opening_only",
		TestFeedbackSink:               "trace_only",
	}
}

// graphBatchEvaluationGraph is deliberately limited to the empty generation-0
// snapshot. A non-empty graph is not silently treated as empty: it must wait
// for the authoritative GMS composition in TB-14.
type graphBatchEvaluationGraph struct {
	SkillIDs []string `json:"skill_ids"`
	EdgeIDs  []string `json:"edge_ids"`
}

func emptyGraphBatchEvaluationGraph() graphBatchEvaluationGraph {
	return graphBatchEvaluationGraph{SkillIDs: []string{}, EdgeIDs: []string{}}
}

// graphBatchFailureAccounting distinguishes normal no-skill completion from
// mechanism failures, so an empty graph cannot affect timeout/error metrics.
type graphBatchFailureAccounting struct {
	NormalNoSkillEpisodes int `json:"normal_no_skill_episodes"`
	MemoryTimeouts        int `json:"memory_timeouts"`
	MemoryErrors          int `json:"memory_errors"`
	ProtocolErrors        int `json:"protocol_errors"`
}

// graphBatchAttemptArtifact is the standalone empty-graph attempt artifact.
// The canonical pipeline records a separate graphBatchPipelineResult; this
// type stays isolated from the legacy attemptRecord schema.
type graphBatchAttemptArtifact struct {
	Strategy               string                      `json:"strategy"`
	InitialSkillSnapshot   graphBatchEvaluationGraph   `json:"initial_skill_snapshot"`
	FrozenConfig           graphBatchFrozenConfig      `json:"frozen_config"`
	MemoryTerminal         string                      `json:"memory_terminal"`
	TaskStatus             string                      `json:"task_status"`
	FailureAccounting      graphBatchFailureAccounting `json:"failure_accounting"`
}

// graphBatchTaskSession is the narrow session seam for this skeleton. The
// production Host/Pi session is intentionally not wired here.
type graphBatchTaskSession interface {
	CompleteNoSkillEpisode(context.Context) error
}

type graphBatchEpisodeRequest struct {
	Strategy             string
	InitialSkillGraph    graphBatchEvaluationGraph
	FrozenConfig         graphBatchFrozenConfig
	TaskSession          graphBatchTaskSession
}

// runGraphBatchEmptyGraphEpisode completes one generation-0 task after the
// memory path deterministically finds no seed candidate. Invalid input fails
// before task execution; normal empty-graph completion is never reclassified
// as timeout, memory error, or protocol error.
func runGraphBatchEmptyGraphEpisode(ctx context.Context, request graphBatchEpisodeRequest) (graphBatchAttemptArtifact, error) {
	if request.Strategy != graphBatchStrategyID {
		return graphBatchAttemptArtifact{}, fmt.Errorf("graph batch strategy %q is not supported", request.Strategy)
	}
	if err := validateGraphBatchFrozenConfig(request.FrozenConfig); err != nil {
		return graphBatchAttemptArtifact{}, err
	}
	if len(request.InitialSkillGraph.SkillIDs) != 0 || len(request.InitialSkillGraph.EdgeIDs) != 0 {
		return graphBatchAttemptArtifact{}, fmt.Errorf("invalid generation-0 evaluation graph: initial graph must be empty")
	}
	if request.TaskSession == nil {
		return graphBatchAttemptArtifact{}, fmt.Errorf("graph batch task session is required")
	}
	if err := request.TaskSession.CompleteNoSkillEpisode(ctx); err != nil {
		return graphBatchAttemptArtifact{}, fmt.Errorf("complete no-skill task episode: %w", err)
	}
	return graphBatchAttemptArtifact{
		Strategy:             graphBatchStrategyID,
		InitialSkillSnapshot: emptyGraphBatchEvaluationGraph(),
		FrozenConfig:         request.FrozenConfig,
		MemoryTerminal:       graphBatchEmptyGraphTerminal,
		TaskStatus:           "completed",
		FailureAccounting:    graphBatchFailureAccounting{NormalNoSkillEpisodes: 1},
	}, nil
}

func validateGraphBatchFrozenConfig(config graphBatchFrozenConfig) error {
	if config != defaultGraphBatchFrozenConfig() {
		return fmt.Errorf("graph batch configuration is not the frozen contract default")
	}
	return nil
}
