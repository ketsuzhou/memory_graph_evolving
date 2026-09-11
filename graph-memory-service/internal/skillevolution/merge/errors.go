package merge

import (
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Closed reason codes of the merge gates (frozen system registry,
// gms-merge-similarity group; every code below exists in
// $FIX/policy/system-reason-codes.v1.json — NewService verifies).
const (
	// ReasonMergeStateConflict: a lifecycle event conflicts with the frozen
	// §9.4 machine (aliased from the ledger layer).
	ReasonMergeStateConflict = ledger.ReasonMergeStateConflict
	// ReasonMergeGroupInFlight: another proposal already won this
	// proposal_group_key (single in-flight winner, M6).
	ReasonMergeGroupInFlight = ledger.ReasonMergeGroupInFlight
	// ReasonMergeSourceHeadStale: a merge source active head moved under a
	// frozen expectation (M5).
	ReasonMergeSourceHeadStale = ledger.ReasonMergeSourceHeadStale
	// ReasonActiveHeadConflict: a source is not the current active head of
	// its lineage (active-only sources, M2).
	ReasonActiveHeadConflict = ledger.ReasonActiveHeadConflict
	// ReasonSimilarityBelowThreshold: the pair's band is below the merge
	// threshold (below-threshold pairs never admit, M1).
	ReasonSimilarityBelowThreshold = contract.ReasonSimilarityBelowThreshold
	// ReasonSimilarityAssessmentInvalid: the referenced §7.21 assessment
	// does not resolve or does not cover the declared pair.
	ReasonSimilarityAssessmentInvalid = "SIMILARITY_ASSESSMENT_INVALID"
	// ReasonMergeSourceInvalid: a source ref is not an exact released ref.
	ReasonMergeSourceInvalid = "MERGE_SOURCE_INVALID"
	// ReasonMergeKindUnsupported: cross-kind pairs (v1 merges step_guidance
	// only, Contract §5.5.1).
	ReasonMergeKindUnsupported = "MERGE_KIND_UNSUPPORTED"
	// ReasonMergeSourceNotActive: a source is not the current active head
	// of its lineage (active-only sources, M2).
	ReasonMergeSourceNotActive = "MERGE_SOURCE_NOT_ACTIVE"
	// ReasonMergeProposalDuplicate: this proposal lost the group-winner
	// CAS and points at the canonical winner (M6).
	ReasonMergeProposalDuplicate = "MERGE_PROPOSAL_DUPLICATE"
	// ReasonMergeBranchRegression: a source branch is not preserved by the
	// synthesized body (required_branch_preservation).
	ReasonMergeBranchRegression = "MERGE_BRANCH_REGRESSION"
	// ReasonMergeEvidenceIncomplete: provenance coverage is incomplete.
	ReasonMergeEvidenceIncomplete = "MERGE_EVIDENCE_INCOMPLETE"
	// ReasonMergeBlockingConflict: a blocking conflict is unresolved
	// (including the M2 applicability-none and permission/port
	// incompatibilities, which are blocking by construction).
	ReasonMergeBlockingConflict = "MERGE_BLOCKING_CONFLICT"
	// ReasonEvidenceNotCommitted: evidence refs do not resolve committed.
	ReasonEvidenceNotCommitted = contract.ReasonEvidenceNotCommitted
	// ReasonSkillKindInvalid: reused artifact-gate kind code.
	ReasonSkillKindInvalid = "SKILL_KIND_INVALID"
	// ReasonSourceHeadStale: the replay/evaluator-side staleness code.
	ReasonSourceHeadStale = "SOURCE_HEAD_STALE"
	// ReasonCriticalRegression: a critical packet regressed on the candidate
	// side (bilateral no-regression, U1).
	ReasonCriticalRegression = "CRITICAL_REGRESSION"
	// ReasonMissingFamily: a required fixture family is absent.
	ReasonMissingFamily = "MISSING_FAMILY"
	// ReasonCandidateNotBound: no committed candidate at synthesis validation.
	ReasonCandidateNotBound = "CANDIDATE_NOT_FOUND"
	// ReasonCandidateReleasedBodyMismatch: candidate body digest mismatch.
	ReasonCandidateReleasedBodyMismatch = "CANDIDATE_RELEASED_BODY_MISMATCH"
	// ReasonReleaseNotAccepted: only an accepted decision authorizes M@1.
	ReasonReleaseNotAccepted = "RELEASE_NOT_ACCEPTED"
	// ReasonReleaseDecisionInvalid: the §7.12 decision document is invalid.
	ReasonReleaseDecisionInvalid = "RELEASE_DECISION_INVALID"
	// ReasonMergeWithdrawalForbidden: v1 merge proposals never withdraw
	// (the machine edge exists; this engine refuses to drive it).
	ReasonMergeWithdrawalForbidden = "MERGE_WITHDRAWAL_FORBIDDEN"
	// ReasonRefMismatch / ReasonDigestMismatch / ReasonIdempotencyConflict
	// alias the single frozen sources.
	ReasonRefMismatch         = contract.ReasonRefMismatch
	ReasonDigestMismatch      = contract.ReasonDigestMismatch
	ReasonIdempotencyConflict = ledger.ReasonIdempotencyConflict
)

// Error is the registry-code gate failure; Detail is diagnostic only.
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "merge: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// transitionError reports a frozen §9.4 machine violation.
func transitionError(format string, args ...any) *Error {
	return newError(ReasonMergeStateConflict, format, args...)
}

// CodeOf returns the closed reason code of err ("" when nil); ledger,
// validation and contract layer codes surface unchanged (single registry).
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if me, ok := err.(*Error); ok {
		return me.ReasonCode
	}
	if code := ledger.ReasonOf(err); code != "" {
		return code
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	var sce *contract.SchemaError
	if errors.As(err, &sce) {
		return sce.ReasonCode
	}
	return ""
}
