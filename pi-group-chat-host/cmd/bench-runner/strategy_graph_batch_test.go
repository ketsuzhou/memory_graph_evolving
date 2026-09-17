package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type graphBatchFixtureSession struct {
	completed int
}

func (session *graphBatchFixtureSession) CompleteNoSkillEpisode(context.Context) error {
	session.completed++
	return nil
}

func TestGraphBatchStrategyDispatchTable(t *testing.T) {
	legacy := map[string]skillStrategy{
		"warm-skill":           skillStrategyBatch,
		"warm-skill-batch":     skillStrategyBatch,
		"warm-skill-replay":    skillStrategyReplay,
		"warm-skill-online":    skillStrategyOnline,
		"warm-skill-continual": skillStrategyContinual,
	}
	for arm, want := range legacy {
		got, ok := skillStrategyFor(arm)
		if !ok || got != want {
			t.Errorf("legacy strategy for %q = %q, %t; want %q, true", arm, got, ok, want)
		}
	}

	got, ok := skillStrategyFor(graphBatchStrategyID)
	if !ok || got != skillStrategyGraphBatch || !isSkillArm(graphBatchStrategyID) {
		t.Fatalf("graph batch strategy = %q, %t; want %q, true", got, ok, skillStrategyGraphBatch)
	}
	if _, ok := skillStrategyFor("warm-skill-graph-batch-typo"); ok || isSkillArm("warm-skill-graph-batch-typo") {
		t.Fatal("unknown graph strategy must fail closed")
	}
}

func TestGraphBatchRunArmFailsClosedBeforeLegacyComposition(t *testing.T) {
	called := false
	err := runArm(context.Background(), armConfig{arm: graphBatchStrategyID}, func(attemptRecord) {
		called = true
	})
	if err == nil || !strings.Contains(err.Error(), "execution is not composed") {
		t.Fatalf("runArm error = %v; want an uncomposed graph-batch error", err)
	}
	if called {
		t.Fatal("uncomposed graph batch must not emit a legacy attempt record")
	}
}

func TestGraphBatchEmptyGraphEpisodeWritesNormalNoSkillArtifact(t *testing.T) {
	session := new(graphBatchFixtureSession)
	artifact, err := runGraphBatchEmptyGraphEpisode(context.Background(), graphBatchEpisodeRequest{
		Strategy:          graphBatchStrategyID,
		InitialSkillGraph: emptyGraphBatchEvaluationGraph(),
		FrozenConfig:      defaultGraphBatchFrozenConfig(),
		TaskSession:       session,
	})
	if err != nil {
		t.Fatalf("run empty graph episode: %v", err)
	}
	if session.completed != 1 {
		t.Fatalf("task completed %d times; want 1", session.completed)
	}
	if artifact.Strategy != graphBatchStrategyID || artifact.MemoryTerminal != graphBatchEmptyGraphTerminal || artifact.TaskStatus != "completed" {
		t.Fatalf("unexpected terminal artifact: %#v", artifact)
	}
	if len(artifact.InitialSkillSnapshot.SkillIDs) != 0 || len(artifact.InitialSkillSnapshot.EdgeIDs) != 0 {
		t.Fatalf("initial snapshot is not empty: %#v", artifact.InitialSkillSnapshot)
	}
	if artifact.FrozenConfig != defaultGraphBatchFrozenConfig() {
		t.Fatalf("frozen config = %#v; want contract defaults %#v", artifact.FrozenConfig, defaultGraphBatchFrozenConfig())
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	for _, field := range []string{"\"strategy\"", "\"initial_skill_snapshot\"", "\"frozen_config\"", "\"memory_terminal\""} {
		if !containsJSONField(string(encoded), field) {
			t.Errorf("artifact omits required field %s: %s", field, encoded)
		}
	}
}

func TestGraphBatchEmptyGraphIsNormalNoSkillNotMechanismFailure(t *testing.T) {
	artifact, err := runGraphBatchEmptyGraphEpisode(context.Background(), graphBatchEpisodeRequest{
		Strategy:          graphBatchStrategyID,
		InitialSkillGraph: emptyGraphBatchEvaluationGraph(),
		FrozenConfig:      defaultGraphBatchFrozenConfig(),
		TaskSession:       new(graphBatchFixtureSession),
	})
	if err != nil {
		t.Fatalf("run empty graph episode: %v", err)
	}
	if artifact.FailureAccounting.NormalNoSkillEpisodes != 1 {
		t.Errorf("normal no-skill episodes = %d; want 1", artifact.FailureAccounting.NormalNoSkillEpisodes)
	}
	if artifact.FailureAccounting.MemoryTimeouts != 0 || artifact.FailureAccounting.MemoryErrors != 0 || artifact.FailureAccounting.ProtocolErrors != 0 {
		t.Errorf("empty graph was counted as a mechanism failure: %#v", artifact.FailureAccounting)
	}
}

func TestGraphBatchEpisodeFailsClosedBeforeTaskForInvalidInputs(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*graphBatchEpisodeRequest)
	}{
		{
			name: "unknown strategy",
			mutate: func(request *graphBatchEpisodeRequest) {
				request.Strategy = "unknown"
			},
		},
		{
			name: "nonempty initial graph",
			mutate: func(request *graphBatchEpisodeRequest) {
				request.InitialSkillGraph.SkillIDs = []string{"skill://test/one@1"}
			},
		},
		{
			name: "invalid graph step budget",
			mutate: func(request *graphBatchEpisodeRequest) {
				request.FrozenConfig.MaxGraphStepsPerEpisode = 0
			},
		},
		{
			name: "invalid offer budget",
			mutate: func(request *graphBatchEpisodeRequest) {
				request.FrozenConfig.MaxOffersPerAgentPerCheckpoint = 0
			},
		},
		{
			name: "invalid memory turn budget",
			mutate: func(request *graphBatchEpisodeRequest) {
				request.FrozenConfig.MaxMemoryTurnsPerEpisode = 6 + 1
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			session := new(graphBatchFixtureSession)
			request := graphBatchEpisodeRequest{
				Strategy:          graphBatchStrategyID,
				InitialSkillGraph: emptyGraphBatchEvaluationGraph(),
				FrozenConfig:      defaultGraphBatchFrozenConfig(),
				TaskSession:       session,
			}
			testCase.mutate(&request)
			if _, err := runGraphBatchEmptyGraphEpisode(context.Background(), request); err == nil {
				t.Fatal("invalid request succeeded; want fail closed")
			}
			if session.completed != 0 {
				t.Fatalf("invalid request ran task %d times; want 0", session.completed)
			}
		})
	}
}

func containsJSONField(value, field string) bool {
	return strings.Contains(value, field)
}
