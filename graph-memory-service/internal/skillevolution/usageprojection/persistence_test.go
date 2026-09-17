package usageprojection_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestUsageProjectionPersistsAcrossStoreAndServiceInstances(t *testing.T) {
	storeA := usageprojection.NewMemoryStore()
	serviceA, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, storeA)
	if err != nil {
		t.Fatalf("new service A: %v", err)
	}
	profile := persistentProfile()
	subject := usageprojection.UsageSubjectRef{SkillRef: persistentSkillRef()}
	if err := serviceA.RecordInteraction(context.Background(), usageprojection.Interaction{
		Subject: subject, ContextProfile: profile, SourceLineageID: "source-lineage-1",
		Stage: usageprojection.InteractionAdopted, EvidenceRefs: []contract.EvidenceRef{persistentEvidence("adoption")},
	}); err != nil {
		t.Fatalf("record interaction: %v", err)
	}
	if err := serviceA.RecordDiagnosis(context.Background(), usageprojection.DiagnosisUtilityAssessment{
		AssessmentID: "assessment-1", Subject: subject, ContextProfile: profile, SourceLineageID: "source-lineage-1",
		ReturnedPathID: "path-1", AddressedAgentID: "agent-primary",
		AdoptionEvidenceRefs:    []contract.EvidenceRef{persistentEvidence("adoption-proof")},
		ContributionScoreMicros: 700_000, ConfidenceMicros: 800_000,
		Rationale:    "The adopted skill narrowed the error source.",
		EvidenceRefs: []contract.EvidenceRef{persistentEvidence("assessment")},
		RubricRef:    persistentVersionedRef("diagnosis-rubric"),
	}); err != nil {
		t.Fatalf("record diagnosis: %v", err)
	}
	before, err := serviceA.UsageSummary(context.Background(), usageprojection.UsageSummaryRequest{})
	if err != nil {
		t.Fatalf("summary before snapshot: %v", err)
	}

	image, err := storeA.Snapshot()
	if err != nil {
		t.Fatalf("snapshot usage store: %v", err)
	}
	storeB := usageprojection.NewMemoryStore()
	if err := storeB.Restore(image); err != nil {
		t.Fatalf("restore usage store: %v", err)
	}
	serviceB, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, storeB)
	if err != nil {
		t.Fatalf("new service B: %v", err)
	}
	after, err := serviceB.UsageSummary(context.Background(), usageprojection.UsageSummaryRequest{})
	if err != nil {
		t.Fatalf("summary after restore: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("summary after restore = %#v, want %#v", after, before)
	}
}

func persistentProfile() usageprojection.ContextProfile {
	return usageprojection.ContextProfile{
		SchemaVersion:        "context-profile/1.0",
		TaskFamily:           "code_implementation",
		RuntimeClass:         "go-1.26-linux",
		WorkspaceFeatureTags: []string{"multi-package"},
		ObservableGuardFacts: []string{"dependency-resolution-error"},
		ToolPolicyRef:        persistentVersionedRef("tool-policy"),
		EnvironmentClass:     "sandboxed-linux",
	}
}

func persistentSkillRef() *contract.SkillArtifactRef {
	return &contract.SkillArtifactRef{
		SchemaVersion: contract.SchemaSkillArtifactRef,
		LineageID:     "lineage-1", Version: "1", Kind: "step_guidance", ArtifactDigest: persistentDigest("skill"),
	}
}

func persistentEvidence(id string) contract.EvidenceRef {
	return contract.EvidenceRef{
		SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: id, Version: "1", EvidenceDigest: persistentDigest(id),
		CommitState: "committed", EvidenceKind: "observation",
	}
}

func persistentVersionedRef(id string) contract.VersionedRef {
	return contract.VersionedRef{ID: id, Version: "1", Digest: persistentDigest(id)}
}

func persistentDigest(_ string) string { return "sha256:" + strings.Repeat("a", 64) }
