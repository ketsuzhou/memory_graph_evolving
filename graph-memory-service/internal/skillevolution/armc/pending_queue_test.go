package armc_test

import (
	"context"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestPendingQueueDequeuesUsagePriorityWithoutCrossingAuthorityTiers(t *testing.T) {
	profile := queueProfile()
	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	activeLow := queueSubject("active-low")
	activeHigh := queueSubject("active-high")
	advisoryHigh := queueSubject("advisory-high")
	for _, observation := range []struct {
		subject    usageprojection.UsageSubjectRef
		score      int64
		diagnostic bool
	}{
		{activeHigh, 800_000, false}, {activeLow, 999_999, true}, {advisoryHigh, 900_000, false},
	} {
		if err := usage.RecordDiagnosis(context.Background(), queueDiagnosis(observation.subject, profile, observation.score, observation.diagnostic)); err != nil {
			t.Fatal(err)
		}
	}
	queue := armc.NewPendingQueue(usage)
	for _, item := range []armc.PendingItem{
		{CandidateID: "active-low", Subject: activeLow, AuthorityTier: usageprojection.AuthorityTierActive},
		{CandidateID: "advisory-high", Subject: advisoryHigh, AuthorityTier: usageprojection.AuthorityTierAdvisory},
		{CandidateID: "active-high", Subject: activeHigh, AuthorityTier: usageprojection.AuthorityTierActive},
	} {
		if err := queue.Enqueue(context.Background(), item); err != nil {
			t.Fatal(err)
		}
	}
	first, ok, err := queue.Dequeue(context.Background())
	if err != nil || !ok {
		t.Fatalf("first dequeue = %#v ok=%t err=%v", first, ok, err)
	}
	if first.Item.CandidateID != "active-high" || first.Explanation.AuthorityTier != usageprojection.AuthorityTierActive || !first.Explanation.UsageApplied {
		t.Fatalf("first dequeue = %#v", first)
	}
	second, ok, err := queue.Dequeue(context.Background())
	if err != nil || !ok || second.Item.CandidateID != "active-low" || second.Explanation.UsageApplied {
		t.Fatalf("second dequeue = %#v ok=%t err=%v", second, ok, err)
	}
	third, ok, err := queue.Dequeue(context.Background())
	if err != nil || !ok || third.Item.CandidateID != "advisory-high" {
		t.Fatalf("third dequeue = %#v ok=%t err=%v", third, ok, err)
	}
}

func queueProfile() usageprojection.ContextProfile {
	return usageprojection.ContextProfile{SchemaVersion: "context-profile/1.0", TaskFamily: "armc", RuntimeClass: "go-test", WorkspaceFeatureTags: []string{"pending"}, ObservableGuardFacts: []string{"queue"}, ToolPolicyRef: contract.VersionedRef{ID: "policy", Version: "1", Digest: contract.DigestBytes([]byte("policy"))}, EnvironmentClass: "sandboxed-linux"}
}
func queueSubject(id string) usageprojection.UsageSubjectRef {
	return usageprojection.UsageSubjectRef{CandidateRef: &contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: id, Kind: "tool", BodyDigest: contract.DigestBytes([]byte(id)), OriginType: "skill_proposal", OriginRef: contract.VersionedRef{ID: "proposal-" + id, Version: "1", Digest: contract.DigestBytes([]byte("proposal-" + id))}}}
}
func queueDiagnosis(subject usageprojection.UsageSubjectRef, profile usageprojection.ContextProfile, score int64, diagnostic bool) usageprojection.DiagnosisUtilityAssessment {
	return usageprojection.DiagnosisUtilityAssessment{AssessmentID: "assessment-" + subject.CanonicalKey(), Subject: subject, ContextProfile: profile, SourceLineageID: "source", ReturnedPathID: "path", AddressedAgentID: "agent", ContributionScoreMicros: score, ConfidenceMicros: score, Rationale: "queue", EvidenceRefs: []contract.EvidenceRef{}, RubricRef: contract.VersionedRef{ID: "rubric", Version: "1", Digest: contract.DigestBytes([]byte("rubric"))}, DiagnosticOnly: diagnostic}
}
