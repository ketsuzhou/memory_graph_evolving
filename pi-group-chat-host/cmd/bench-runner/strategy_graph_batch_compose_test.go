package main

import (
	"context"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/testdata/rawadmit"
	"river2.dev/pi-group-chat-host/internal/diagnosisfanout"
)

func TestComposeGraphBatchRuntimeRequiresGMS(t *testing.T) {
	_, err := composeGraphBatchRuntime(armConfig{
		arm: graphBatchStrategyID,
		episodes: []manifestEpisode{{
			EpisodeID: "train-1", Split: "train", TaskID: "task-1",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "requires a GMS instance") {
		t.Fatalf("compose error = %v; want GMS required", err)
	}
}

func TestPartitionGraphBatchEpisodesSplitsTrainAndHeldOut(t *testing.T) {
	trains, heldOut := partitionGraphBatchEpisodes([]manifestEpisode{
		{EpisodeID: "tr-1", Split: "train", TaskID: "t1", Turns: []struct {
			Prompt string `json:"prompt"`
		}{{Prompt: "solve train"}}},
		{EpisodeID: "te-1", Split: "test", TaskID: "t2"},
	})
	if len(trains) != 1 || trains[0].Trajectory.ID != "tr-1" {
		t.Fatalf("trains = %#v", trains)
	}
	if len(heldOut) != 1 || heldOut[0].AttemptID != "te-1" {
		t.Fatalf("held-out = %#v", heldOut)
	}
}

func TestImportLegacySkillLedgerRequiresLegacyUnverified(t *testing.T) {
	if _, err := importLegacySkillLedger([]graphBatchLegacyImport{{
		ID: "skills.md#abc", ProvenanceClass: "",
	}}); err == nil || !strings.Contains(err.Error(), "legacy_unverified") {
		t.Fatalf("unmarked import error = %v", err)
	}
	got, err := importLegacySkillLedger([]graphBatchLegacyImport{{
		ID: "skills.md#abc", ProvenanceClass: graphBatchLegacyUnverified,
	}})
	if err != nil || len(got) != 1 || got[0] != "skills.md#abc" {
		t.Fatalf("verified import = %v %v", got, err)
	}
}

func TestSkillDeliveryClassMarksLegacyBrokenDelivery(t *testing.T) {
	if got := skillDeliveryClass("warm-skill-batch"); got != legacyBrokenDelivery {
		t.Fatalf("legacy class = %q", got)
	}
	if got := skillDeliveryClass(graphBatchStrategyID); got != graphBatchAuthorityGMS {
		t.Fatalf("graph-batch class = %q", got)
	}
}

func TestCLIDiagnosisWorkerProposalIsAdmittedByGMSContentGate(t *testing.T) {
	trains, _ := partitionGraphBatchEpisodes([]manifestEpisode{{
		EpisodeID: "tr-1", Split: "train", TaskID: "t1",
	}})
	if len(trains) != 1 {
		t.Fatalf("trains = %#v", trains)
	}
	traj := trains[0].Trajectory
	traj.Outcome = diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusFail, Cause: diagnosisfanout.CauseUnknown}
	input := diagnosisfanout.BuildDiagnosisInput(traj)
	draft, err := graphBatchCLIDiagnosisWorker{}.Diagnose(context.Background(), input)
	if err != nil {
		t.Fatalf("diagnose: %v", err)
	}
	fact := firstObservableFact(input)
	insight, _ := draft.ProposalBody["non_obvious_insight"].(string)
	if fact == "" || !strings.Contains(strings.ToLower(insight), strings.ToLower(fact)) {
		t.Fatalf("insight %q is not entailed by fact %q", insight, fact)
	}
	step, _ := draft.ProposalBody["decision_policy_or_steps"].([]any)
	if len(step) == 0 {
		t.Fatal("missing decision steps")
	}
	stepText, _ := step[0].(string)
	if !strings.Contains(strings.ToLower(stepText), strings.ToLower(fact)) {
		t.Fatalf("step %q is not entailed by fact %q", stepText, fact)
	}
	adapter, err := rawadmit.New()
	if err != nil {
		t.Fatalf("rawadmit: %v", err)
	}
	evidence := traj.Evidence["evidence-tr-1"]
	adapter.Put(rawadmit.FrozenSpec{
		TrajectoryID:   traj.ID,
		SnapshotID:     traj.SnapshotID,
		DiagnosisRunID: traj.DiagnosisRunID,
		Steps:          traj.CompleteTrajectory,
		PublicOutcome:  traj.Outcome.CanonicalString(),
		CheckpointID:   "checkpoint-tr-1",
		EvidenceID:     evidence.ID,
		Fact:           evidence.ObservableFacts[0],
	})
	if _, _, _, err := adapter.Admit(context.Background(), draft.IdempotencyKey, traj.DiagnosisRunID, traj.ID, draft.ProposalBody); err != nil {
		t.Fatalf("GMS content gate rejected CLI proposal: %v", err)
	}
}

func TestEvidenceIDsFromTrainsUseExactEvidenceNotProposalIDs(t *testing.T) {
	trains, _ := partitionGraphBatchEpisodes([]manifestEpisode{{
		EpisodeID: "ep-9", Split: "train", TaskID: "task-9",
	}})
	got := evidenceIDsFromTrains(trains)
	if len(got) != 1 || got[0] != "evidence-ep-9" {
		t.Fatalf("evidence IDs = %#v", got)
	}
}
