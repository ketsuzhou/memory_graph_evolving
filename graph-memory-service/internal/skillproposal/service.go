package skillproposal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"river2.dev/graph-memory-service/internal/authz"
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
type CandidateDecision = domain.CandidateDecision
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

// Service owns the governed proposal and candidate surface. Acceptance is
// only a decision; activation is a separate human-curator action behind the
// curation purpose fence, and there is no automatic activation path.
type Service struct {
	authorizer *authz.Authorizer
	registry   ports.RegistryStore
	patterns   ports.PatternStore
	proposals  ports.ProposalStore
	candidates ports.CandidateStore
	clock      ports.Clock
}

func New(registry ports.RegistryStore, patterns ports.PatternStore, proposals ports.ProposalStore, candidates ports.CandidateStore, clock ports.Clock) *Service {
	return &Service{authorizer: authz.NewAuthorizer(registry, clock), registry: registry, patterns: patterns, proposals: proposals, candidates: candidates, clock: clock}
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

// Decide records the curator's accept/reject decision. An accepted decision
// never activates the skill — activation is a separate gated action.
func (s *Service) Decide(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, spaceID domain.SpaceID, decision CandidateDecision) (CandidateDecision, bool, error) {
	if err := s.authorize(ctx, tenantID, principalID, spaceID, domain.GrantOperationCandidateDecide); err != nil {
		return CandidateDecision{}, false, err
	}
	if decision.Decision != "accepted" && decision.Decision != "rejected" {
		return CandidateDecision{}, false, domain.NewProtocolError(422, "INVALID_REQUEST", "decision must be accepted or rejected")
	}
	decision.DecidedBy = principalID
	decision.DecidedAt = s.clock.Now()
	created, err := s.candidates.DecideCandidate(ctx, decision)
	if err != nil {
		return CandidateDecision{}, false, err
	}
	return decision, created, nil
}

// Activate promotes an accepted candidate into the active skill version. The
// gate chain is server-side: a human curator principal, a fresh exact-Space
// curation grant, and an accepted candidate — agents cannot activate, and no
// other purpose can.
func (s *Service) Activate(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, spaceID domain.SpaceID, activation SkillActivation) (SkillActivation, bool, error) {
	if s.registry != nil {
		principal, err := s.registry.Principal(ctx, tenantID, principalID)
		if err != nil {
			return SkillActivation{}, false, err
		}
		if principal.Kind != domain.PrincipalHuman {
			return SkillActivation{}, false, domain.NewProtocolError(403, "GRANT_MISSING", "only a human curator can activate a skill")
		}
	}
	if err := s.authorize(ctx, tenantID, principalID, spaceID, domain.GrantOperationCandidateActivate); err != nil {
		return SkillActivation{}, false, err
	}
	candidate, err := s.candidates.Candidate(ctx, tenantID, spaceID, activation.CandidateID)
	if err != nil {
		return SkillActivation{}, false, err
	}
	if candidate.Status != domain.CandidateAccepted {
		return SkillActivation{}, false, domain.NewProtocolError(409, "CANDIDATE_NOT_ACCEPTED", "activation requires an accepted candidate")
	}
	activation.ActivatedBy = principalID
	activation.ActivatedAt = s.clock.Now()
	activation.NewArtifactVersion = activation.ExpectedBaseVersion + 1
	created, err := s.candidates.ActivateCandidate(ctx, activation)
	if err != nil {
		return SkillActivation{}, false, err
	}
	return activation, created, nil
}

func (s *Service) authorize(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, spaceID domain.SpaceID, operation domain.GrantOperation) error {
	_, err := s.authorizer.AuthorizeExact(ctx, authz.Identity{TenantID: tenantID, PrincipalID: principalID}, []domain.SpaceID{spaceID}, domain.GrantPurposeCuration, operation)
	return err
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
