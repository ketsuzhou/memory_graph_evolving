package retrieval_test

import (
	"context"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestExploreUsesNonDiagnosticUsageOnlyWithinActiveTierAndExplainsOrder(t *testing.T) {
	world := newWorld(t)
	world.registerEvidence("ev-prop-low")
	world.registerEvidence("ev-prop-high")
	low := world.activateEnvelope(t, "lineage-a-low", "prop-low", procedureEnvelope("same retrieval text extra"))
	high := world.activateEnvelope(t, "lineage-z-high", "prop-high", procedureEnvelope("same retrieval text"))
	world.project(t)

	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	profile := retrievalUsageProfile()
	highSubject := usageprojection.UsageSubjectRef{SkillRef: &high}
	lowSubject := usageprojection.UsageSubjectRef{SkillRef: &low}
	if err := usage.RecordDiagnosis(context.Background(), retrievalUsageDiagnosis(highSubject, profile, 900_000, false)); err != nil {
		t.Fatal(err)
	}
	if err := usage.RecordDiagnosis(context.Background(), retrievalUsageDiagnosis(lowSubject, profile, 999_999, true)); err != nil {
		t.Fatal(err)
	}

	response, err := world.serviceWithUsage(t, false, usage).Explore(context.Background(), toolRequest("memory_explore", "usage-order", exploreArguments("usage-order", "same retrieval text extra", 10, 5, 5, 10000, nil), 0))
	if err != nil {
		t.Fatalf("Explore: %v", err)
	}
	skills := resultArray(t, response.Result, "skill_results")
	if len(skills) != 2 {
		t.Fatalf("skill results = %d, want 2", len(skills))
	}
	first := skills[0]["skill_ref"].(map[string]any)
	if got := first["lineage_id"]; got != high.LineageID {
		t.Fatalf("first lineage = %v, want usage-ranked %s; extensions=%#v", got, high.LineageID, response.Result["extensions"])
	}
	extensions, ok := response.Result["extensions"].(map[string]any)
	if !ok {
		t.Fatalf("usage ranking explanation missing extensions: %#v", response.Result)
	}
	ranking, ok := extensions["gms.usage-ranking.v1"].(map[string]any)
	if !ok {
		t.Fatalf("usage ranking explanation missing: %#v", extensions)
	}
	if ranking["policy_version"] != "usage-ranking-v1" || ranking["authority_preserving"] != true {
		t.Fatalf("ranking explanation = %#v", ranking)
	}
	explanations, ok := ranking["skill_explanations"].([]any)
	if !ok || len(explanations) != 2 {
		t.Fatalf("skill explanations = %#v", ranking["skill_explanations"])
	}
	firstExplanation := explanations[0].(map[string]any)
	if firstExplanation["authority_tier"] != "active" || firstExplanation["usage_applied"] != true || firstExplanation["average_contribution_score_micros"] != int64(900_000) {
		t.Fatalf("first explanation = %#v", firstExplanation)
	}
}

func retrievalUsageProfile() usageprojection.ContextProfile {
	return usageprojection.ContextProfile{SchemaVersion: "context-profile/1.0", TaskFamily: "retrieval", RuntimeClass: "go-test", WorkspaceFeatureTags: []string{"usage-ranking"}, ObservableGuardFacts: []string{"active-only"}, ToolPolicyRef: contract.VersionedRef{ID: "tool-policy", Version: "1", Digest: contract.DigestBytes([]byte("tool-policy"))}, EnvironmentClass: "sandboxed-linux"}
}

func retrievalUsageDiagnosis(subject usageprojection.UsageSubjectRef, profile usageprojection.ContextProfile, score int64, diagnostic bool) usageprojection.DiagnosisUtilityAssessment {
	return usageprojection.DiagnosisUtilityAssessment{AssessmentID: "assessment-" + subject.CanonicalKey(), Subject: subject, ContextProfile: profile, SourceLineageID: "usage-source", ReturnedPathID: "path", AddressedAgentID: "agent", ContributionScoreMicros: score, ConfidenceMicros: score, Rationale: "usage ranking test", EvidenceRefs: []contract.EvidenceRef{}, RubricRef: contract.VersionedRef{ID: "rubric", Version: "1", Digest: contract.DigestBytes([]byte("rubric"))}, DiagnosticOnly: diagnostic}
}
