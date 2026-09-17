package compositeproposal_test

import (
	"context"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/compositeproposal"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestCompositeProposalServiceDraftsGovernedCoUsageComposite(t *testing.T) {
	t.Parallel()

	sourceA := skillRef("lineage-explore", "artifact-explore")
	sourceB := skillRef("lineage-verify", "artifact-verify")
	policy := compositeproposal.CoUsagePolicy{
		Ref:                       versionedRef("co-usage-policy"),
		MinIndependentProfiles:    2,
		MinimumContributionMicros: 500_000,
		MinimumConfidenceMicros:   500_000,
		EligibleKinds:             []string{"step_guidance"},
	}
	request := compositeproposal.DraftRequest{
		Policy: policy,
		Plan: compositeproposal.CompositeDraftPlan{
			Guard: "when repository exploration must be followed by verification",
			ControlFlow: []compositeproposal.ControlFlowStep{
				{StepID: "explore", SkillRef: sourceA},
				{StepID: "verify", SkillRef: sourceB, DependsOn: []string{"explore"}},
			},
			DataFlow:        []compositeproposal.DataFlow{{FromStep: "explore", ToStep: "verify", Mapping: map[string]string{"files": "targets"}}},
			PermissionUnion: []string{"memory_expand", "skill_get"},
			FailureHandling: []compositeproposal.FailureHandling{{StepID: "verify", OnFailure: "verification_failed", Action: "return_to_exploration"}},
		},
	}

	t.Run("drafts at threshold without authority", func(t *testing.T) {
		usage := newUsageService(t)
		for _, profile := range []usageprojection.ContextProfile{profile("go-ci"), profile("go-local")} {
			recordAdoptedAndDiagnosed(t, usage, sourceA, "source-lineage-a", profile, "a", nil)
			recordAdoptedAndDiagnosed(t, usage, sourceB, "source-lineage-b", profile, "b", nil)
		}

		drafts, err := compositeproposal.NewService(usage).Draft(context.Background(), request)
		if err != nil {
			t.Fatalf("draft composite proposal: %v", err)
		}
		if len(drafts) != 1 {
			t.Fatalf("draft count = %d, want 1", len(drafts))
		}
		draft := drafts[0]
		if got, want := draft.Status, compositeproposal.DraftStatusDrafted; got != want {
			t.Fatalf("status = %q, want %q", got, want)
		}
		if !draft.NonAuthoritative {
			t.Fatal("co-usage draft must not carry activation authority")
		}
		if len(draft.SourceRefs) != 2 || len(draft.CompatibleProfileDigests) != 2 || len(draft.ThresholdEvidence) == 0 {
			t.Fatalf("draft provenance incomplete: %#v", draft)
		}
		if len(draft.Counterevidence) != 0 {
			t.Fatalf("positive draft counterevidence = %#v, want none", draft.Counterevidence)
		}
		if got, want := draft.Plan.Guard, request.Plan.Guard; got != want {
			t.Fatalf("guard = %q, want %q", got, want)
		}
		if len(draft.Plan.ControlFlow) != 2 || len(draft.Plan.DataFlow) != 1 || len(draft.Plan.PermissionUnion) != 2 {
			t.Fatalf("draft plan incomplete: %#v", draft.Plan)
		}
	})

	t.Run("blocks an otherwise qualifying draft with counterevidence", func(t *testing.T) {
		usage := newUsageService(t)
		profiles := []usageprojection.ContextProfile{profile("go-ci"), profile("go-local")}
		for index, profile := range profiles {
			counter := []contract.EvidenceRef(nil)
			if index == 1 {
				counter = []contract.EvidenceRef{evidenceRef("counterevidence")}
			}
			recordAdoptedAndDiagnosed(t, usage, sourceA, "source-lineage-a", profile, "a", counter)
			recordAdoptedAndDiagnosed(t, usage, sourceB, "source-lineage-b", profile, "b", nil)
		}

		drafts, err := compositeproposal.NewService(usage).Draft(context.Background(), request)
		if err != nil {
			t.Fatalf("draft blocked composite proposal: %v", err)
		}
		if len(drafts) != 1 {
			t.Fatalf("draft count = %d, want 1", len(drafts))
		}
		if got, want := drafts[0].Status, compositeproposal.DraftStatusBlockedCounterevidence; got != want {
			t.Fatalf("status = %q, want %q", got, want)
		}
		if len(drafts[0].Counterevidence) != 1 {
			t.Fatalf("counterevidence = %#v, want one preserved ref", drafts[0].Counterevidence)
		}
	})
}

func newUsageService(t *testing.T) *usageprojection.UsageProjectionService {
	t.Helper()
	service, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatalf("new usage service: %v", err)
	}
	return service
}

func recordAdoptedAndDiagnosed(t *testing.T, usage *usageprojection.UsageProjectionService, source *contract.SkillArtifactRef, lineage string, profile usageprojection.ContextProfile, suffix string, counterevidence []contract.EvidenceRef) {
	t.Helper()
	subject := usageprojection.UsageSubjectRef{SkillRef: source}
	if err := usage.RecordInteraction(context.Background(), usageprojection.Interaction{
		Subject: subject, ContextProfile: profile, SourceLineageID: lineage,
		Stage: usageprojection.InteractionAdopted, EvidenceRefs: []contract.EvidenceRef{evidenceRef("adoption-" + suffix + "-" + profile.TaskFamily)},
	}); err != nil {
		t.Fatalf("record adopted interaction: %v", err)
	}
	if err := usage.RecordDiagnosis(context.Background(), usageprojection.DiagnosisUtilityAssessment{
		AssessmentID: "assessment-" + suffix + "-" + profile.TaskFamily,
		Subject:      subject, ContextProfile: profile, SourceLineageID: lineage,
		ReturnedPathID: "path-" + suffix, AddressedAgentID: "agent-primary",
		AdoptionEvidenceRefs:    []contract.EvidenceRef{evidenceRef("adoption-proof-" + suffix + "-" + profile.TaskFamily)},
		ContributionScoreMicros: 800_000, ConfidenceMicros: 800_000,
		CounterevidenceRefs: counterevidence,
		Rationale:           "The adopted skill improved the observed task path.",
		EvidenceRefs:        []contract.EvidenceRef{evidenceRef("assessment-" + suffix + "-" + profile.TaskFamily)},
		RubricRef:           versionedRef("diagnosis-rubric"),
	}); err != nil {
		t.Fatalf("record diagnosis assessment: %v", err)
	}
}

func profile(taskFamily string) usageprojection.ContextProfile {
	return usageprojection.ContextProfile{
		SchemaVersion:        "context-profile/1.0",
		TaskFamily:           taskFamily,
		RuntimeClass:         "go-1.26-linux",
		WorkspaceFeatureTags: []string{"multi-package"},
		ObservableGuardFacts: []string{"verification-required"},
		ToolPolicyRef:        versionedRef("tool-policy"),
		EnvironmentClass:     "sandboxed-linux",
	}
}

func skillRef(lineageID, artifactID string) *contract.SkillArtifactRef {
	return &contract.SkillArtifactRef{
		SchemaVersion:  contract.SchemaSkillArtifactRef,
		LineageID:      lineageID,
		Version:        "1",
		Kind:           "step_guidance",
		ArtifactDigest: digest(artifactID),
	}
}

func evidenceRef(id string) contract.EvidenceRef {
	return contract.EvidenceRef{SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: id, Version: "1", EvidenceDigest: digest(id), CommitState: "committed", EvidenceKind: "observation"}
}

func versionedRef(id string) contract.VersionedRef {
	return contract.VersionedRef{ID: id, Version: "1", Digest: digest(id)}
}

func digest(seed string) string {
	return "sha256:" + strings.Repeat(string(seed[0]), 64)
}
