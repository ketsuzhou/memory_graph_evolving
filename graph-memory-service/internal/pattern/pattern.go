package pattern

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

// The pattern lifecycle is one-way: tentative → supported → proposal_eligible,
// with rejected as the only terminal exit. Causal strength is never
// self-assigned — it is verified against the exact M1 ledger revision the
// evidence references (ADR 0023/0038).

type PatternStatus = domain.PatternStatus
type PatternEvidenceStrength = domain.PatternEvidenceStrength
type PatternRef = domain.PatternRef
type CausalEstimateRef = domain.CausalEstimateRef
type PatternEvidence = domain.PatternEvidence
type PatternRevision = domain.PatternRevision
type PatternPolicy = domain.PatternPolicy
type PatternAssessment = domain.PatternAssessment
type DraftPatternInput = domain.DraftPatternInput

const (
	PatternTentative        = domain.PatternTentative
	PatternSupported        = domain.PatternSupported
	PatternProposalEligible = domain.PatternProposalEligible
	PatternRejected         = domain.PatternRejected

	EvidenceRepeat      = domain.EvidenceRepeat
	EvidenceContrastive = domain.EvidenceContrastive
	EvidenceCausal      = domain.EvidenceCausal
)

// DraftPattern opens a pattern at revision one as tentative. The content hash
// freezes the drafted meaning for later fingerprinting.
func DraftPattern(input DraftPatternInput) (PatternRevision, error) {
	if input.TenantID == "" || input.SpaceID == "" || input.PatternID == "" || input.Problem == "" {
		return PatternRevision{}, fmt.Errorf("pattern draft: tenant, space, id, and problem are required")
	}
	revision := PatternRevision{
		TenantID: input.TenantID, SpaceID: input.SpaceID, PatternID: input.PatternID,
		Revision: 1, Status: PatternTentative,
		Problem: input.Problem, Applicability: input.Applicability, RecommendedAction: input.RecommendedAction,
		Evidence:  append([]PatternEvidence(nil), input.Evidence...),
		CreatedBy: input.Actor, CreatedAt: input.CreatedAt,
	}
	revision.ContentHash = ContentHash(revision)
	return revision, nil
}

// AssessPattern scores one evidence list under the policy. A causal label
// only counts when the exact referenced estimate revision exists in the
// ledger, is conclusive, currently applicable, and carries both arms.
func AssessPattern(evidence []PatternEvidence, policy PatternPolicy, estimates map[CausalEstimateRef]domain.CausalEstimateRevision) (PatternAssessment, error) {
	lineages := map[string]bool{}
	hasContrastive := false
	hasVerifiedCausal := false
	for _, item := range evidence {
		switch item.Strength {
		case EvidenceRepeat:
			if item.LineageID != "" {
				lineages[item.LineageID] = true
			}
		case EvidenceContrastive:
			hasContrastive = true
		case EvidenceCausal:
			if item.CausalEstimate == nil {
				continue
			}
			estimate, found := estimates[*item.CausalEstimate]
			if !found || estimate.EstimateID != item.CausalEstimate.EstimateID || estimate.Revision != item.CausalEstimate.Revision {
				continue
			}
			if estimate.Conclusion != domain.CausalConclusionConclusive || !estimate.CurrentApplicability {
				continue
			}
			if estimate.WithTrials < 1 || estimate.WithoutTrials < 1 {
				continue
			}
			hasVerifiedCausal = true
		default:
			return PatternAssessment{}, fmt.Errorf("pattern assess: unknown evidence strength %q", item.Strength)
		}
	}
	assessment := PatternAssessment{RepeatLineages: len(lineages), HasContrastive: hasContrastive, HasVerifiedCausal: hasVerifiedCausal}
	eligible := PatternTentative
	if assessment.RepeatLineages >= policy.MinIndependentLineages {
		eligible = PatternSupported
	}
	if eligible == PatternSupported && (hasContrastive || hasVerifiedCausal) {
		eligible = PatternProposalEligible
	}
	assessment.EligibleStatus = eligible
	assessment.Rationale = fmt.Sprintf("%d independent repeat lineages (min %d), contrastive=%v, verified causal=%v",
		assessment.RepeatLineages, policy.MinIndependentLineages, hasContrastive, hasVerifiedCausal)
	return assessment, nil
}

// AdvancePattern moves exactly one lifecycle step forward along the frozen
// path; skips, backwards moves, and advances out of rejected are errors.
func AdvancePattern(current PatternRevision, assessment PatternAssessment, actor domain.PrincipalID, at time.Time) (PatternRevision, error) {
	var next PatternStatus
	switch current.Status {
	case PatternTentative:
		next = PatternSupported
	case PatternSupported:
		next = PatternProposalEligible
	case PatternProposalEligible, PatternRejected:
		return PatternRevision{}, fmt.Errorf("pattern advance: status %q cannot advance", current.Status)
	default:
		return PatternRevision{}, fmt.Errorf("pattern advance: unknown status %q", current.Status)
	}
	if assessment.EligibleStatus != next {
		return PatternRevision{}, fmt.Errorf("pattern advance: assessment %q cannot move status %q (next step is %q)", assessment.EligibleStatus, current.Status, next)
	}
	advanced := current
	advanced.Revision = current.Revision + 1
	advanced.Status = next
	advanced.CreatedBy = actor
	advanced.CreatedAt = at
	advanced.ContentHash = ContentHash(advanced)
	return advanced, nil
}

// RejectPattern terminates a pattern with a recorded reason. Rejection is
// terminal: no later advance can resurrect it.
func RejectPattern(current PatternRevision, reason string, actor domain.PrincipalID, at time.Time) (PatternRevision, error) {
	if current.Status == PatternRejected {
		return PatternRevision{}, fmt.Errorf("pattern reject: revision %d is already rejected", current.Revision)
	}
	if reason == "" {
		return PatternRevision{}, fmt.Errorf("pattern reject: reason is required")
	}
	rejected := current
	rejected.Revision = current.Revision + 1
	rejected.Status = PatternRejected
	rejected.RejectionReason = reason
	rejected.CreatedBy = actor
	rejected.CreatedAt = at
	return rejected, nil
}

// ContentHash freezes the pattern meaning: identity, guidance, and evidence
// references. Prose-only edits to unrelated fields never collide with it.
func ContentHash(revision PatternRevision) string {
	payload := struct {
		PatternID         string            `json:"pattern_id"`
		Status            string            `json:"status"`
		Problem           string            `json:"problem"`
		Applicability     string            `json:"applicability"`
		RecommendedAction string            `json:"recommended_action"`
		Evidence          []PatternEvidence `json:"evidence"`
	}{
		PatternID: revision.PatternID, Status: string(revision.Status),
		Problem: revision.Problem, Applicability: revision.Applicability, RecommendedAction: revision.RecommendedAction,
		Evidence: revision.Evidence,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
