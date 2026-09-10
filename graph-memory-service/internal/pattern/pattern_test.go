package pattern_test

import (
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/pattern"
)

var patternNow = time.Date(2026, time.September, 8, 13, 30, 0, 0, time.UTC)

func TestPatternLifecycleOnlyAdvancesTentativeSupportedProposalEligible(t *testing.T) {
	policy := pattern.PatternPolicy{PolicyVersion: "pattern-v1", MinIndependentLineages: 2}
	repeats := []pattern.PatternEvidence{
		{EvidenceRef: domain.EvidenceRef{BatchID: "batch-a", EventIDs: []string{"event-a"}}, SourceSpaceID: "space-a", LineageID: "lineage-a", Strength: pattern.EvidenceRepeat},
		{EvidenceRef: domain.EvidenceRef{BatchID: "batch-b", EventIDs: []string{"event-b"}}, SourceSpaceID: "space-b", LineageID: "lineage-b", Strength: pattern.EvidenceRepeat},
	}

	t.Run("draft begins tentative", func(t *testing.T) {
		got, err := pattern.DraftPattern(pattern.DraftPatternInput{
			TenantID: "tenant-1", SpaceID: "space-1", PatternID: "pattern-1", Problem: "Repeated miss",
			Applicability: "Go repositories", RecommendedAction: "Run the focused check", Evidence: repeats,
			Actor: "miner", CreatedAt: patternNow,
		})
		if err != nil {
			t.Fatalf("draft pattern: %v", err)
		}
		if got.Status != pattern.PatternTentative || got.Revision != 1 || got.PatternID != "pattern-1" {
			t.Fatalf("draft = %#v, want tentative revision one", got)
		}
	})

	t.Run("independent repeats promote tentative to supported", func(t *testing.T) {
		assessment, err := pattern.AssessPattern(repeats, policy, nil)
		if err != nil {
			t.Fatalf("assess repeats: %v", err)
		}
		if assessment.RepeatLineages != 2 || assessment.EligibleStatus != pattern.PatternSupported {
			t.Fatalf("assessment = %#v, want supported", assessment)
		}
		got, err := pattern.AdvancePattern(pattern.PatternRevision{
			TenantID: "tenant-1", SpaceID: "space-1", PatternID: "pattern-1", Revision: 1,
			Status: pattern.PatternTentative, Applicability: "Go repositories", Evidence: repeats,
		}, assessment, "curator", patternNow)
		if err != nil || got.Status != pattern.PatternSupported || got.Revision != 2 {
			t.Fatalf("advance = (%#v, %v), want supported revision two", got, err)
		}
	})

	t.Run("contrastive support promotes supported to proposal eligible", func(t *testing.T) {
		evidence := append(append([]pattern.PatternEvidence(nil), repeats...), pattern.PatternEvidence{
			EvidenceRef: domain.EvidenceRef{BatchID: "batch-c", EventIDs: []string{"event-c"}}, SourceSpaceID: "space-c", LineageID: "lineage-c", Strength: pattern.EvidenceContrastive,
		})
		assessment, err := pattern.AssessPattern(evidence, policy, nil)
		if err != nil || !assessment.HasContrastive || assessment.EligibleStatus != pattern.PatternProposalEligible {
			t.Fatalf("assessment = (%#v, %v), want proposal eligible", assessment, err)
		}
		got, err := pattern.AdvancePattern(pattern.PatternRevision{
			TenantID: "tenant-1", SpaceID: "space-1", PatternID: "pattern-1", Revision: 2,
			Status: pattern.PatternSupported, Applicability: "Go repositories", Evidence: evidence,
		}, assessment, "curator", patternNow)
		if err != nil || got.Status != pattern.PatternProposalEligible || got.Revision != 3 {
			t.Fatalf("advance = (%#v, %v), want proposal eligible revision three", got, err)
		}
	})

	t.Run("tentative cannot skip directly to proposal eligible", func(t *testing.T) {
		_, err := pattern.AdvancePattern(pattern.PatternRevision{
			PatternID: "pattern-1", Revision: 1, Status: pattern.PatternTentative, Applicability: "Go repositories",
		}, pattern.PatternAssessment{RepeatLineages: 2, HasContrastive: true, EligibleStatus: pattern.PatternProposalEligible}, "curator", patternNow)
		if err == nil {
			t.Fatal("illegal lifecycle skip was accepted")
		}
	})

	t.Run("rejected is terminal", func(t *testing.T) {
		rejected, err := pattern.RejectPattern(pattern.PatternRevision{PatternID: "pattern-1", Revision: 2, Status: pattern.PatternSupported}, "not general", "curator", patternNow)
		if err != nil || rejected.Status != pattern.PatternRejected || rejected.RejectionReason != "not general" {
			t.Fatalf("reject = (%#v, %v)", rejected, err)
		}
		if _, err := pattern.AdvancePattern(rejected, pattern.PatternAssessment{EligibleStatus: pattern.PatternProposalEligible}, "curator", patternNow.Add(time.Minute)); err == nil {
			t.Fatal("rejected pattern resurrected")
		}
	})
}

func TestCausalPatternStrengthRequiresExactVerifiedM1Estimate(t *testing.T) {
	policy := pattern.PatternPolicy{PolicyVersion: "pattern-v1", MinIndependentLineages: 2}
	ref := pattern.CausalEstimateRef{EstimateID: "estimate-1", Revision: 3}
	baseEvidence := []pattern.PatternEvidence{
		{EvidenceRef: domain.EvidenceRef{BatchID: "batch-a", EventIDs: []string{"event-a"}}, SourceSpaceID: "space-a", LineageID: "lineage-a", Strength: pattern.EvidenceRepeat},
		{EvidenceRef: domain.EvidenceRef{BatchID: "batch-b", EventIDs: []string{"event-b"}}, SourceSpaceID: "space-b", LineageID: "lineage-b", Strength: pattern.EvidenceRepeat},
		{EvidenceRef: domain.EvidenceRef{BatchID: "batch-c", EventIDs: []string{"event-c"}}, SourceSpaceID: "space-c", LineageID: "lineage-c", Strength: pattern.EvidenceCausal, CausalEstimate: &ref},
	}

	tests := []struct {
		name      string
		estimates map[pattern.CausalEstimateRef]domain.CausalEstimateRevision
		verified  bool
		eligible  pattern.PatternStatus
	}{
		{
			name:      "agent causal label without ledger revision is ignored",
			estimates: map[pattern.CausalEstimateRef]domain.CausalEstimateRevision{},
			eligible:  pattern.PatternSupported,
		},
		{
			name: "inconclusive ledger revision cannot establish causal strength",
			estimates: map[pattern.CausalEstimateRef]domain.CausalEstimateRevision{ref: {
				EstimateID: "estimate-1", Revision: 3, Conclusion: domain.CausalConclusionInconclusive,
				CurrentApplicability: true, SourceTrialIDs: []domain.CausalTrialEventID{"with-1", "without-1"},
			}},
			eligible: pattern.PatternSupported,
		},
		{
			name: "exact conclusive current paired estimate is verified",
			estimates: map[pattern.CausalEstimateRef]domain.CausalEstimateRevision{ref: {
				EstimateID: "estimate-1", Revision: 3, Conclusion: domain.CausalConclusionConclusive,
				CurrentApplicability: true, WitnessStatus: domain.WitnessStatusActive,
				SourceTrialIDs: []domain.CausalTrialEventID{"with-1", "without-1"}, WithTrials: 1, WithoutTrials: 1,
			}},
			verified: true, eligible: pattern.PatternProposalEligible,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := pattern.AssessPattern(baseEvidence, policy, test.estimates)
			if err != nil {
				t.Fatalf("assess causal evidence: %v", err)
			}
			if got.HasVerifiedCausal != test.verified || got.EligibleStatus != test.eligible {
				t.Fatalf("assessment = %#v, want verified=%v eligible=%q", got, test.verified, test.eligible)
			}
		})
	}
}
