package similarity

import (
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
)

// Band values of the Contract §7.21 band enum (the shared DTO is the single
// source; these constants only alias it for type-safe comparisons).
const (
	BandBelowSuggestion   = "below_suggestion"
	BandSuggestionOnly    = "suggestion_only"
	BandMergeReview       = "merge_review"
	BandAutoMergeEligible = "auto_merge_eligible"
)

// Policy is the versioned integer band-threshold policy
// (Contract §5.5.7: "Similarity thresholds MUST 使用 exact versioned policy
// ref 与整数定标，禁止浮点数"). score_micros is a micro-scaled integer
// (1_000_000 == 1.0); every threshold below is an exclusive upper bound of
// its band:
//
//	score <  SuggestionOnlyBelow  → below_suggestion
//	score <  MergeReviewBelow     → suggestion_only
//	score <  AutoMergeBelow       → merge_review
//	score >= AutoMergeBelow       → auto_merge_eligible
//
// The admission-effect mapping of each band is GMS §7.1 (below_suggestion
// may never auto-admit; merge_review waits for a protected gate;
// auto_merge_eligible may auto-admit only with green compatibility and no
// blocking conflict).
type Policy struct {
	Ref contract.VersionedRef
	// SuggestionOnlyBelow: scores below never create a merge proposal.
	SuggestionOnlyBelow int64
	// MergeReviewBelow: scores below may only raise suggestions or gather
	// evidence.
	MergeReviewBelow int64
	// AutoMergeBelow: scores below need the protected review gate.
	AutoMergeBelow int64
}

// DefaultPolicyV1 is the frozen v1 threshold set. Threshold values follow
// the conformance corpus: a 920000 score assesses merge_review
// ($FIX/merge/merge-001) while 120000 is below_suggestion
// ($FIX/merge/merge-neg-001), so the auto-merge bound sits above 920000.
func DefaultPolicyV1() Policy {
	p := Policy{
		Ref: contract.VersionedRef{
			ID:      "policy.similarity-bands.v1",
			Version: "1",
		},
		SuggestionOnlyBelow: 400_000,
		MergeReviewBelow:    700_000,
		AutoMergeBelow:      950_000,
	}
	p.Ref.Digest = p.digestOfSelf()
	return p
}

// digestOfSelf freezes the threshold document as the policy ref digest: the
// same thresholds always address the same exact policy ref.
func (p Policy) digestOfSelf() string {
	digest, err := contract.DigestOf(map[string]any{
		"policy_id":             p.Ref.ID,
		"suggestion_only_below": json.Number(fmt.Sprintf("%d", p.SuggestionOnlyBelow)),
		"merge_review_below":    json.Number(fmt.Sprintf("%d", p.MergeReviewBelow)),
		"auto_merge_below":      json.Number(fmt.Sprintf("%d", p.AutoMergeBelow)),
		"scale":                 "micros",
	})
	if err != nil {
		// Integer-only decoder values always canonicalize.
		panic(fmt.Sprintf("similarity: policy digest: %v", err))
	}
	return digest
}

// Band derives the §7.21 band of one integer score under this policy.
func (p Policy) Band(scoreMicros int64) string {
	switch {
	case scoreMicros < p.SuggestionOnlyBelow:
		return BandBelowSuggestion
	case scoreMicros < p.MergeReviewBelow:
		return BandSuggestionOnly
	case scoreMicros < p.AutoMergeBelow:
		return BandMergeReview
	default:
		return BandAutoMergeEligible
	}
}

// MayPropose reports whether the band reaches the merge-proposal threshold
// (GMS §7.1: merge_review and auto_merge_eligible may create proposals;
// anything below may only raise suggestions or accumulate evidence).
func (p Policy) MayPropose(band string) bool {
	return band == BandMergeReview || band == BandAutoMergeEligible
}

// MayAutoAdmit reports whether the band authorizes the automatic admission
// path (GMS §7.1; still conditional on green compatibility gates and no
// blocking conflict).
func (p Policy) MayAutoAdmit(band string) bool {
	return band == BandAutoMergeEligible
}
