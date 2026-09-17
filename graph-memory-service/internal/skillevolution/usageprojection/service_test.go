package usageprojection_test

import (
	"context"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestUsageProjectionRanksDiagnosisUtilityWithoutTreatingExposureAsAdoption(t *testing.T) {
	t.Parallel()

	profile := usageprojection.ContextProfile{
		SchemaVersion:        "context-profile/1.0",
		TaskFamily:           "code_implementation",
		RuntimeClass:         "go-1.26-linux",
		WorkspaceFeatureTags: []string{"multi-package", "generated-code"},
		ObservableGuardFacts: []string{"dependency-resolution-error"},
		ToolPolicyRef:        versionedRef("tool-policy"),
		EnvironmentClass:     "sandboxed-linux",
	}
	profileWithReorderedTags := profile
	profileWithReorderedTags.WorkspaceFeatureTags = []string{"generated-code", "multi-package"}
	if profile.Digest() != profileWithReorderedTags.Digest() {
		t.Fatal("derived context digest must not depend on feature-tag ordering")
	}

	service, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatalf("new usage projection service: %v", err)
	}

	highUtilitySkill := usageprojection.UsageSubjectRef{SkillRef: skillRef("lineage-high", "skill-high")}
	adoptedCandidate := usageprojection.UsageSubjectRef{CandidateRef: candidateRef("candidate-adopted")}
	for _, stage := range []usageprojection.InteractionStage{
		usageprojection.InteractionMatched,
		usageprojection.InteractionExposed,
		usageprojection.InteractionSelected,
	} {
		if err := service.RecordInteraction(context.Background(), usageprojection.Interaction{
			Subject: highUtilitySkill, ContextProfile: profile, SourceLineageID: "source-lineage-high",
			Stage: stage, EvidenceRefs: []contract.EvidenceRef{evidenceRef("evidence-high")},
		}); err != nil {
			t.Fatalf("record high-utility interaction %q: %v", stage, err)
		}
	}
	for _, stage := range []usageprojection.InteractionStage{
		usageprojection.InteractionAdopted,
		usageprojection.InteractionVerified,
		usageprojection.InteractionOutcomeCorrelated,
	} {
		if err := service.RecordInteraction(context.Background(), usageprojection.Interaction{
			Subject: adoptedCandidate, ContextProfile: profile, SourceLineageID: "source-lineage-adopted",
			Stage: stage, EvidenceRefs: []contract.EvidenceRef{evidenceRef("evidence-adopted")},
		}); err != nil {
			t.Fatalf("record adopted-candidate interaction %q: %v", stage, err)
		}
	}

	for _, assessment := range []usageprojection.DiagnosisUtilityAssessment{
		{
			AssessmentID: "assessment-high", Subject: highUtilitySkill, ContextProfile: profile,
			ReturnedPathID: "path-high", AddressedAgentID: "agent-primary",
			ContributionScoreMicros: 900_000, ConfidenceMicros: 800_000,
			Rationale:    "The returned guidance narrowed the failure source.",
			EvidenceRefs: []contract.EvidenceRef{evidenceRef("assessment-evidence-high")},
			RubricRef:    versionedRef("diagnosis-rubric"),
		},
		{
			AssessmentID: "assessment-adopted", Subject: adoptedCandidate, ContextProfile: profile,
			ReturnedPathID: "path-adopted", AddressedAgentID: "agent-primary",
			ContributionScoreMicros: 700_000, ConfidenceMicros: 900_000,
			AdoptionEvidenceRefs: []contract.EvidenceRef{evidenceRef("adoption-evidence")},
			Rationale:            "The candidate was adopted and its result was observed.",
			EvidenceRefs:         []contract.EvidenceRef{evidenceRef("assessment-evidence-adopted")},
			RubricRef:            versionedRef("diagnosis-rubric"),
		},
	} {
		if err := service.RecordDiagnosis(context.Background(), assessment); err != nil {
			t.Fatalf("record diagnosis %q: %v", assessment.AssessmentID, err)
		}
	}

	summary, err := service.UsageSummary(context.Background(), usageprojection.UsageSummaryRequest{ContextProfile: &profile})
	if err != nil {
		t.Fatalf("usage summary: %v", err)
	}
	if got, want := summary.PolicyVersion, "usage-ranking-v1"; got != want {
		t.Fatalf("policy version = %q, want %q", got, want)
	}
	if len(summary.Entries) != 2 {
		t.Fatalf("summary entries = %d, want 2", len(summary.Entries))
	}
	if got, want := summary.Entries[0].Subject.CanonicalKey(), highUtilitySkill.CanonicalKey(); got != want {
		t.Fatalf("first ranked subject = %q, want %q", got, want)
	}
	if got := summary.Entries[0].AdoptedCount; got != 0 {
		t.Fatalf("exposed/selected skill adoption count = %d, want 0", got)
	}
	if got, want := summary.Entries[1].AdoptedCount, 1; got != want {
		t.Fatalf("adopted candidate count = %d, want %d", got, want)
	}
	if got, want := summary.Entries[1].VerifiedCount, 1; got != want {
		t.Fatalf("verified count = %d, want %d", got, want)
	}
	if got, want := summary.Entries[1].OutcomeCorrelatedCount, 1; got != want {
		t.Fatalf("outcome-correlated count = %d, want %d", got, want)
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

func candidateRef(candidateID string) *contract.CandidateArtifactRef {
	return &contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   candidateID,
		Kind:          "step_guidance",
		BodyDigest:    digest(candidateID),
		OriginType:    "skill_proposal",
		OriginRef:     versionedRef("proposal-" + candidateID),
	}
}

func evidenceRef(id string) contract.EvidenceRef {
	return contract.EvidenceRef{
		SchemaVersion:  contract.SchemaEvidenceRef,
		EvidenceID:     id,
		Version:        "1",
		EvidenceDigest: digest(id),
		CommitState:    "committed",
		EvidenceKind:   "observation",
	}
}

func versionedRef(id string) contract.VersionedRef {
	return contract.VersionedRef{ID: id, Version: "1", Digest: digest(id)}
}

func digest(seed string) string {
	return "sha256:" + strings.Repeat(string(seed[0]), 64)
}
