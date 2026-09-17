package skillproposal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

type ProposalFingerprint = domain.ProposalFingerprint
type ProposalFingerprintInput = domain.ProposalFingerprintInput
type SkillProposal = domain.SkillProposal
type ProposalRoundOutcome = domain.ProposalRoundOutcome
type RejectionMemory = domain.RejectionMemory
type ReviewedDiff = domain.ReviewedDiff
type CandidateStatus = domain.CandidateStatus
type SkillCandidate = domain.SkillCandidate
type ReplayArm = domain.ReplayArm
type PairedReplayPlan = domain.PairedReplayPlan
type ReplayTrialRequest = domain.ReplayTrialRequest
type ReplayObservation = domain.ReplayObservation
type PairedReplayTrial = domain.PairedReplayTrial
type ReplayArmStats = domain.ReplayArmStats
type PairedReplayResult = domain.PairedReplayResult
type ActivationPolicyDecisionRef = domain.ActivationPolicyDecisionRef
type ActivationPolicyDecision = domain.ActivationPolicyDecision
type ArmCEvaluation = domain.ArmCEvaluation
type CoverageProof = domain.CoverageProof
type SkillActivation = domain.SkillActivation

const (
	CandidateNeedsReview = domain.CandidateNeedsReview
	CandidateEvaluating  = domain.CandidateEvaluating
	CandidateAccepted    = domain.CandidateAccepted
	CandidateRejected    = domain.CandidateRejected
	CandidateStale       = domain.CandidateStale

	WithSkill    = domain.WithSkill
	WithoutSkill = domain.WithoutSkill
)

const replayPolicyVersion = "paired-replay-v1"

// FingerprintProposal canonicalizes the material identity of a proposal:
// artifact identity, reviewed diff, scope, and pattern refs sorted by
// (pattern_id, revision). Prose, actor, and timestamps never contribute.
func FingerprintProposal(input ProposalFingerprintInput) (ProposalFingerprint, error) {
	refs := append([]domain.PatternRef(nil), input.PatternRefs...)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].PatternID != refs[j].PatternID {
			return refs[i].PatternID < refs[j].PatternID
		}
		return refs[i].Revision < refs[j].Revision
	})
	payload := struct {
		TargetSkillID         string                   `json:"target_skill_id"`
		NewSkillName          string                   `json:"new_skill_name"`
		BaseArtifactVersion   int64                    `json:"base_artifact_version"`
		BaseArtifactHash      string                   `json:"base_artifact_hash"`
		CandidateArtifactHash string                   `json:"candidate_artifact_hash"`
		BaseArtifactRef       *domain.SkillArtifactRef `json:"base_artifact_ref,omitempty"`
		CandidateArtifactRef  *domain.SkillArtifactRef `json:"candidate_artifact_ref,omitempty"`
		ReviewedDiffHash      string                   `json:"reviewed_diff_hash"`
		RequestedScope        string                   `json:"requested_scope"`
		PatternRefs           []domain.PatternRef      `json:"pattern_refs"`
	}{
		TargetSkillID: input.TargetSkillID, NewSkillName: input.NewSkillName,
		BaseArtifactVersion: input.BaseArtifactVersion, BaseArtifactHash: input.BaseArtifactHash,
		CandidateArtifactHash: input.CandidateArtifactHash, BaseArtifactRef: input.BaseArtifactRef,
		CandidateArtifactRef: input.CandidateArtifactRef, ReviewedDiffHash: input.ReviewedDiffHash,
		RequestedScope: input.RequestedScope, PatternRefs: refs,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("proposal fingerprint: encode canonical payload: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return ProposalFingerprint(hex.EncodeToString(digest[:])), nil
}

// ValidateRoundOutcome enforces the atomic round contract: exactly one
// complete proposal or one explicit no-action, never both, never neither.
func ValidateRoundOutcome(outcome ProposalRoundOutcome) error {
	hasProposal := outcome.Proposal != nil
	hasNoAction := outcome.NoActionReason != ""
	if hasProposal == hasNoAction {
		if hasProposal {
			return fmt.Errorf("proposal round %s: proposal and no-action are mutually exclusive", outcome.RoundID)
		}
		return fmt.Errorf("proposal round %s: must contain one proposal or one no-action reason", outcome.RoundID)
	}
	if hasProposal {
		proposal := outcome.Proposal
		if proposal.RoundID != outcome.RoundID || proposal.ProposalID == "" || proposal.Fingerprint == "" ||
			proposal.CandidateID == "" || len(proposal.PatternRefs) == 0 {
			return fmt.Errorf("proposal round %s: proposal is not complete", outcome.RoundID)
		}
	}
	return nil
}

type PairedReplayRunner interface {
	RunTrial(context.Context, ReplayTrialRequest) (ReplayObservation, error)
}

// PairedEvaluator runs the frozen two-arm replay: every ordinal executes both
// arms under the identical pinned plan, and only complete, infrastructure
// valid pairs count. An invalid or partial pair makes the replay
// inconclusive — it can never pass.
type PairedEvaluator struct {
	runner PairedReplayRunner
	clock  ports.Clock
}

func NewPairedEvaluator(runner PairedReplayRunner, clock ports.Clock) *PairedEvaluator {
	return &PairedEvaluator{runner: runner, clock: clock}
}

func (e *PairedEvaluator) Evaluate(ctx context.Context, plan PairedReplayPlan) (PairedReplayResult, error) {
	result := PairedReplayResult{ReplayID: plan.ReplayID, PolicyVersion: replayPolicyVersion}
	var withSuccesses, withoutSuccesses int
	completePairs := 0
	for ordinal := 1; ordinal <= plan.MaxPairs; ordinal++ {
		withObservation, withErr := e.runner.RunTrial(ctx, ReplayTrialRequest{Plan: plan, Arm: domain.WithSkill, Ordinal: ordinal})
		if withErr != nil {
			result.Conclusion = "inconclusive"
			return result, nil
		}
		withoutObservation, withoutErr := e.runner.RunTrial(ctx, ReplayTrialRequest{Plan: plan, Arm: domain.WithoutSkill, Ordinal: ordinal})
		if withoutErr != nil {
			result.Conclusion = "inconclusive"
			return result, nil
		}
		if !withObservation.InfrastructureValid || !withoutObservation.InfrastructureValid {
			result.Conclusion = "inconclusive"
			return result, nil
		}
		result.With.Trials++
		result.Without.Trials++
		if withObservation.Success {
			withSuccesses++
		}
		if withoutObservation.Success {
			withoutSuccesses++
		}
		completePairs++
		if completePairs >= plan.MinPairs {
			break
		}
	}
	if completePairs < plan.MinPairs {
		result.Conclusion = "inconclusive"
		return result, nil
	}
	result.With.Successes = withSuccesses
	result.Without.Successes = withoutSuccesses
	result.With.Mean, result.With.Variance = armStats(withSuccesses, completePairs)
	result.Without.Mean, result.Without.Variance = armStats(withoutSuccesses, completePairs)
	result.Passed = withSuccesses > withoutSuccesses
	if result.Passed {
		result.Conclusion = "conclusive"
	} else {
		result.Conclusion = "negative"
	}
	return result, nil
}

func armStats(successes, trials int) (mean, variance float64) {
	if trials == 0 {
		return 0, 0
	}
	mean = float64(successes) / float64(trials)
	variance = mean * (1 - mean)
	return mean, variance
}

// Service owns the governed proposal and candidate surface. Activation is
// policy-driven in every environment: the server resolves and independently
// revalidates an immutable Arm C ActivationPolicyDecision before publishing.
type Service struct {
	patterns   ports.PatternStore
	proposals  ports.ProposalStore
	candidates ports.CandidateStore
	policies   ports.ActivationPolicyStore
	clock      ports.Clock
}

func New(_ ports.RegistryStore, patterns ports.PatternStore, proposals ports.ProposalStore, candidates ports.CandidateStore, clock ports.Clock, policies ...ports.ActivationPolicyStore) *Service {
	var policyStore ports.ActivationPolicyStore
	if len(policies) > 0 {
		policyStore = policies[0]
	}
	return &Service{patterns: patterns, proposals: proposals, candidates: candidates, policies: policyStore, clock: clock}
}

// CompleteRound stores the round's single atomic outcome. Fingerprints are
// content-frozen: a fingerprint that was already proposed, or whose proposal
// was rejected, can never be proposed again by any round.
func (s *Service) CompleteRound(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, outcome ProposalRoundOutcome) (ProposalRoundOutcome, bool, error) {
	if err := ValidateRoundOutcome(outcome); err != nil {
		return ProposalRoundOutcome{}, false, err
	}
	if outcome.Proposal != nil {
		fingerprint := outcome.Proposal.Fingerprint
		if rejection, err := s.proposals.RejectionByFingerprint(ctx, tenantID, spaceID, fingerprint); err == nil && rejection.Fingerprint != "" {
			return ProposalRoundOutcome{}, false, domain.NewProtocolError(409, "IDEMPOTENCY_CONFLICT",
				fmt.Sprintf("fingerprint %s was rejected and is permanently blocked", fingerprint))
		} else if !isNotFound(err) {
			return ProposalRoundOutcome{}, false, err
		}
		if existing, err := s.proposals.ProposalByFingerprint(ctx, tenantID, spaceID, fingerprint); err == nil && existing.Fingerprint != "" {
			return ProposalRoundOutcome{}, false, domain.NewProtocolError(409, "IDEMPOTENCY_CONFLICT",
				fmt.Sprintf("fingerprint %s was already proposed as %s", fingerprint, existing.ProposalID))
		} else if !isNotFound(err) {
			return ProposalRoundOutcome{}, false, err
		}
	}
	return s.proposals.CompleteProposalRound(ctx, tenantID, spaceID, outcome)
}

// Activate resolves an exact immutable Arm C policy decision and independently
// revalidates its candidate, evaluation, coverage, policy binding, and expected
// active version. It accepts no caller-supplied principal, human decision,
// candidate digest, evaluation digest, or activation CAS value.
func (s *Service) Activate(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, ref ActivationPolicyDecisionRef) (SkillActivation, bool, error) {
	if s.policies == nil {
		return SkillActivation{}, false, domain.NewProtocolError(409, "ACTIVATION_POLICY_UNAVAILABLE", "policy-driven activation requires an activation policy store")
	}
	decision, err := s.policies.ActivationPolicyDecision(ctx, tenantID, spaceID, ref)
	if err != nil {
		return SkillActivation{}, false, err
	}
	if decision.Ref() != ref || domain.ActivationPolicyDecisionDigest(decision) != ref.Digest {
		return SkillActivation{}, false, domain.NewProtocolError(409, "ACTIVATION_POLICY_INVALID", "activation policy decision does not match its exact immutable reference")
	}
	if decision.Outcome != domain.ActivationOutcomeActivate {
		return SkillActivation{}, false, domain.NewProtocolError(409, "ACTIVATION_POLICY_REJECTED", "activation policy decision does not authorize activation")
	}
	candidate, err := s.candidates.Candidate(ctx, tenantID, spaceID, decision.CandidateID)
	if err != nil {
		return SkillActivation{}, false, err
	}
	if candidate.Diff.CandidateArtifactHash != decision.CandidateDigest || candidate.BaseArtifactVersion != decision.ExpectedActiveVersion {
		return SkillActivation{}, false, domain.NewProtocolError(409, "ACTIVATION_POLICY_MISMATCH", "policy decision candidate binding or expected active version differs from the authoritative candidate")
	}
	evaluation, err := s.policies.ArmCEvaluation(ctx, tenantID, spaceID, decision.EvaluationRef)
	if err != nil {
		return SkillActivation{}, false, err
	}
	if evaluation.Ref() != decision.EvaluationRef || !evaluation.Passed || evaluation.CandidateID != candidate.CandidateID || evaluation.CandidateDigest != decision.CandidateDigest {
		return SkillActivation{}, false, domain.NewProtocolError(409, "ACTIVATION_POLICY_MISMATCH", "Arm C evaluation does not exactly authorize the policy decision")
	}
	coverage, err := s.policies.CoverageProof(ctx, tenantID, spaceID, decision.CoverageRef)
	if err != nil {
		return SkillActivation{}, false, err
	}
	if coverage.Ref() != decision.CoverageRef || coverage.ThresholdPolicyRef != decision.PolicyRef || coverage.IndependentLineageCount < 1 || coverage.IndependentContextProfileCount < 1 {
		return SkillActivation{}, false, domain.NewProtocolError(409, "ACTIVATION_POLICY_MISMATCH", "coverage proof does not exactly authorize the policy decision")
	}
	activation := SkillActivation{
		ActivationID:        decision.DecisionID,
		CandidateID:         candidate.CandidateID,
		DecisionID:          decision.DecisionID,
		ReplayResultID:      decision.EvaluationRef.EvaluationID,
		CandidateDigest:     decision.CandidateDigest,
		ExpectedBaseVersion: decision.ExpectedActiveVersion,
		NewArtifactVersion:  decision.ExpectedActiveVersion + 1,
		ActivatedAt:         s.clock.Now(),
		DecisionRef:         decision.Ref(),
		EvaluationRef:       decision.EvaluationRef,
		CoverageRef:         decision.CoverageRef,
		PolicyRef:           decision.PolicyRef,
	}
	created, err := s.policies.ActivatePolicyDecision(ctx, activation)
	if err != nil {
		return SkillActivation{}, false, err
	}
	return activation, created, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var protocol *domain.ProtocolError
	if ok := asProtocolError(err, &protocol); ok {
		return protocol.Code() == "PROPOSAL_NOT_FOUND" || protocol.Code() == "REJECTION_NOT_FOUND" || protocol.StatusCode() == 404
	}
	return false
}

func asProtocolError(err error, target **domain.ProtocolError) bool {
	protocol, ok := err.(*domain.ProtocolError)
	if ok {
		*target = protocol
	}
	return ok
}
