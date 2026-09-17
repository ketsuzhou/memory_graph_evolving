package usageprojection_test

import (
	"context"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestPrioritizePreservesAuthorityTiersAndExplainsUsageTieBreakers(t *testing.T) {
	profile := priorityProfile()
	service, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	activeHigh := prioritySkill("active-high")
	activeLow := prioritySkill("active-low")
	probationHigh := prioritySkill("probation-high")
	advisoryHigh := prioritySkill("advisory-high")
	for _, subject := range []usageprojection.UsageSubjectRef{probationHigh, advisoryHigh} {
		if err := service.RecordDiagnosis(context.Background(), priorityDiagnosis(subject, profile, 900_000, false)); err != nil {
			t.Fatalf("record %s: %v", subject.CanonicalKey(), err)
		}
	}
	if err := service.RecordDiagnosis(context.Background(), priorityDiagnosis(activeHigh, profile, 800_000, false)); err != nil {
		t.Fatal(err)
	}
	if err := service.RecordDiagnosis(context.Background(), priorityDiagnosis(activeLow, profile, 100_000, true)); err != nil {
		t.Fatal(err)
	}

	summary, err := service.UsageSummary(context.Background(), usageprojection.UsageSummaryRequest{IncludeDiagnostic: false})
	if err != nil {
		t.Fatal(err)
	}
	ordered := usageprojection.Prioritize(summary, []usageprojection.PriorityInput{
		{Key: "active-low", AuthorityTier: usageprojection.AuthorityTierActive, Subject: activeLow},
		{Key: "advisory-high", AuthorityTier: usageprojection.AuthorityTierAdvisory, Subject: advisoryHigh},
		{Key: "active-high", AuthorityTier: usageprojection.AuthorityTierActive, Subject: activeHigh},
		{Key: "probation-high", AuthorityTier: usageprojection.AuthorityTierProbation, Subject: probationHigh},
	})
	if got, want := priorityKeys(ordered), []string{"active-high", "active-low", "probation-high", "advisory-high"}; !sameStrings(got, want) {
		t.Fatalf("priority order = %v, want %v", got, want)
	}
	if ordered[0].Explanation.PolicyVersion != "usage-ranking-v1" || !ordered[0].Explanation.UsageApplied {
		t.Fatalf("active-high explanation = %+v", ordered[0].Explanation)
	}
	if ordered[1].Explanation.UsageApplied {
		t.Fatalf("diagnostic-only observation must not rank active-low: %+v", ordered[1].Explanation)
	}
	if ordered[0].Explanation.AverageContributionScoreMicros != 800_000 || ordered[0].Explanation.AuthorityTier != usageprojection.AuthorityTierActive {
		t.Fatalf("explanation misses ranking inputs: %+v", ordered[0].Explanation)
	}
}

func priorityKeys(items []usageprojection.PriorityDecision) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	return keys
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func priorityProfile() usageprojection.ContextProfile {
	return usageprojection.ContextProfile{
		SchemaVersion: "context-profile/1.0", TaskFamily: "priority-test", RuntimeClass: "go-test",
		WorkspaceFeatureTags: []string{"priority"}, ObservableGuardFacts: []string{"observable"},
		ToolPolicyRef: contract.VersionedRef{ID: "tool-policy", Version: "1", Digest: priorityDigest("tool-policy")}, EnvironmentClass: "sandboxed-linux",
	}
}

func prioritySkill(id string) usageprojection.UsageSubjectRef {
	return usageprojection.UsageSubjectRef{SkillRef: &contract.SkillArtifactRef{
		SchemaVersion: contract.SchemaSkillArtifactRef, LineageID: id, Version: "1", Kind: "step_guidance", ArtifactDigest: priorityDigest(id),
	}}
}

func priorityDiagnosis(subject usageprojection.UsageSubjectRef, profile usageprojection.ContextProfile, score int64, diagnostic bool) usageprojection.DiagnosisUtilityAssessment {
	return usageprojection.DiagnosisUtilityAssessment{
		AssessmentID: "assessment-" + subject.CanonicalKey(), Subject: subject, ContextProfile: profile, SourceLineageID: "source-" + subject.CanonicalKey(),
		ReturnedPathID: "path-" + subject.CanonicalKey(), AddressedAgentID: "agent", ContributionScoreMicros: score, ConfidenceMicros: score,
		Rationale: "priority fixture", EvidenceRefs: []contract.EvidenceRef{}, RubricRef: contract.VersionedRef{ID: "rubric", Version: "1", Digest: priorityDigest("rubric")}, DiagnosticOnly: diagnostic,
	}
}

func priorityDigest(seed string) string {
	return contract.DigestBytes([]byte(seed))
}
