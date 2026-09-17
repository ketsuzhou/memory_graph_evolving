package main

import (
	"strings"
	"testing"
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
