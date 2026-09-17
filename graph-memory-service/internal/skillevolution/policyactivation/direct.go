package policyactivation

import (
	"context"
	"fmt"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

// ArmCCandidateStore resolves the immutable direct registration produced from
// a GMS-202 CandidateView. It intentionally has no legacy SkillCandidate API.
type ArmCCandidateStore interface {
	ArmCCandidate(context.Context, domain.TenantID, domain.SpaceID, string) (domain.ArmCCandidateRegistration, error)
}

type DirectEvaluationRequest struct {
	TenantID  domain.TenantID
	SpaceID   domain.SpaceID
	Candidate domain.ArmCCandidateRegistration
}
type DirectActivationStore interface {
	ports.ActivationPolicyStore
	RegisterArmCCandidate(context.Context, domain.ArmCCandidateRegistration) (bool, error)
	ArmCCandidate(context.Context, domain.TenantID, domain.SpaceID, string) (domain.ArmCCandidateRegistration, error)
	ActivateArmCCandidatePolicyDecision(context.Context, domain.ArmCCandidateRegistration, domain.SkillActivation) (bool, error)
}

type DirectEvaluator interface {
	Evaluate(context.Context, DirectEvaluationRequest) (EvaluationOutput, error)
}
type DirectActivator interface {
	Activate(context.Context, domain.ArmCCandidateRegistration, domain.ActivationPolicyDecisionRef) (domain.SkillActivation, bool, error)
}

// DirectWorker is the Arm C-only replacement for Worker. It consumes an exact
// CandidateArtifactRef/body digest registration, never domain.SkillCandidate.
type DirectWorker struct {
	candidates ArmCCandidateStore
	policies   ports.ActivationPolicyStore
	evaluator  DirectEvaluator
	activator  DirectActivator
}

func NewDirectWorker(candidates ArmCCandidateStore, policies ports.ActivationPolicyStore, evaluator DirectEvaluator, activator DirectActivator) (*DirectWorker, error) {
	if candidates == nil || policies == nil || evaluator == nil || activator == nil {
		return nil, fmt.Errorf("policy activation: direct candidates, policies, evaluator, and activator are required")
	}
	return &DirectWorker{candidates: candidates, policies: policies, evaluator: evaluator, activator: activator}, nil
}
func (w *DirectWorker) Process(ctx context.Context, job Job) (Result, error) {
	candidate, err := w.candidates.ArmCCandidate(ctx, job.TenantID, job.SpaceID, job.CandidateID)
	if err != nil {
		return Result{}, err
	}
	output, err := w.evaluator.Evaluate(ctx, DirectEvaluationRequest{TenantID: job.TenantID, SpaceID: job.SpaceID, Candidate: candidate})
	if err != nil {
		return Result{}, err
	}
	if output.DecisionID == "" || output.DecisionVersion < 1 {
		return Result{}, fmt.Errorf("policy activation: server-owned evaluator omitted immutable decision identity")
	}
	if output.Evaluation.EvaluationID != "" {
		if output.Evaluation.Digest != domain.ArmCEvaluationDigest(output.Evaluation) {
			return Result{}, fmt.Errorf("policy activation: Arm C evaluation digest mismatch")
		}
		if _, err := w.policies.PutArmCEvaluation(ctx, output.Evaluation); err != nil {
			return Result{}, err
		}
	}
	if output.Coverage.ProofID != "" {
		if output.Coverage.Digest != domain.CoverageProofDigest(output.Coverage) {
			return Result{}, fmt.Errorf("policy activation: coverage proof digest mismatch")
		}
		if _, err := w.policies.PutCoverageProof(ctx, output.Coverage); err != nil {
			return Result{}, err
		}
	}
	reason := directValidationFailure(candidate, output)
	outcome := domain.ActivationOutcomeActivate
	if reason != "" {
		outcome = domain.ActivationOutcomeReject
	}
	decision := domain.ActivationPolicyDecision{DecisionID: output.DecisionID, Version: output.DecisionVersion, CandidateID: candidate.CandidateRef.CandidateID, CandidateDigest: candidate.CandidateRef.BodyDigest, EvaluationRef: output.Evaluation.Ref(), CoverageRef: output.Coverage.Ref(), PolicyRef: output.PolicyRef, ExpectedActiveVersion: candidate.ExpectedActiveVersion, Outcome: outcome, Reason: reason}
	decision.Digest = domain.ActivationPolicyDecisionDigest(decision)
	if _, err := w.policies.PutActivationPolicyDecision(ctx, decision); err != nil {
		return Result{}, err
	}
	result := Result{DecisionRef: decision.Ref()}
	if outcome == domain.ActivationOutcomeReject {
		result.Rejected, result.RejectionReason = true, reason
		return result, nil
	}
	activation, created, err := w.activator.Activate(ctx, candidate, decision.Ref())
	if err != nil {
		return Result{}, err
	}
	result.Activated = created
	if created {
		result.Activation = &activation
	}
	return result, nil
}
func directValidationFailure(candidate domain.ArmCCandidateRegistration, output EvaluationOutput) string {
	if output.PolicyRef.PolicyID == "" || output.PolicyRef.Version < 1 || output.PolicyRef.Digest == "" {
		return "policy_ref_missing"
	}
	if output.ExpectedActiveVersion != candidate.ExpectedActiveVersion {
		return "expected_active_version_mismatch"
	}
	if output.Evaluation.EvaluationID == "" || output.Evaluation.Version < 1 || output.Evaluation.Digest == "" {
		return "arm_c_evaluation_missing"
	}
	if !output.Evaluation.Passed {
		if output.Evaluation.Reason != "" {
			return output.Evaluation.Reason
		}
		return "arm_c_evaluation_not_passed"
	}
	if output.Evaluation.CandidateID != candidate.CandidateRef.CandidateID || output.Evaluation.CandidateDigest != candidate.CandidateRef.BodyDigest {
		return "arm_c_evaluation_candidate_mismatch"
	}
	if output.Coverage.ProofID == "" || output.Coverage.Version < 1 || output.Coverage.Digest == "" {
		return "coverage_proof_missing"
	}
	if output.Coverage.ThresholdPolicyRef != output.PolicyRef {
		return "coverage_policy_mismatch"
	}
	if output.Coverage.IndependentLineageCount < 1 || output.Coverage.IndependentContextProfileCount < 1 {
		return "coverage_insufficient"
	}
	return ""
}

type Clock interface{ Now() time.Time }
type directActivator struct {
	policies DirectActivationStore
	clock    Clock
}

func NewDirectActivator(policies DirectActivationStore, clock Clock) DirectActivator {
	return &directActivator{policies: policies, clock: clock}
}
func (a *directActivator) Activate(ctx context.Context, candidate domain.ArmCCandidateRegistration, ref domain.ActivationPolicyDecisionRef) (domain.SkillActivation, bool, error) {
	decision, err := a.policies.ActivationPolicyDecision(ctx, candidate.TenantID, candidate.SpaceID, ref)
	if err != nil {
		return domain.SkillActivation{}, false, err
	}
	if decision.Ref() != ref || decision.Outcome != domain.ActivationOutcomeActivate || decision.CandidateID != candidate.CandidateRef.CandidateID || decision.CandidateDigest != candidate.CandidateRef.BodyDigest || decision.ExpectedActiveVersion != candidate.ExpectedActiveVersion {
		return domain.SkillActivation{}, false, fmt.Errorf("policy activation: direct candidate binding mismatch")
	}
	evaluation, err := a.policies.ArmCEvaluation(ctx, candidate.TenantID, candidate.SpaceID, decision.EvaluationRef)
	if err != nil {
		return domain.SkillActivation{}, false, err
	}
	coverage, err := a.policies.CoverageProof(ctx, candidate.TenantID, candidate.SpaceID, decision.CoverageRef)
	if err != nil {
		return domain.SkillActivation{}, false, err
	}
	if !evaluation.Passed || evaluation.CandidateID != candidate.CandidateRef.CandidateID || evaluation.CandidateDigest != candidate.CandidateRef.BodyDigest || coverage.ThresholdPolicyRef != decision.PolicyRef {
		return domain.SkillActivation{}, false, fmt.Errorf("policy activation: direct Arm C evidence mismatch")
	}
	activation := domain.SkillActivation{ActivationID: decision.DecisionID, CandidateID: candidate.CandidateRef.CandidateID, DecisionID: decision.DecisionID, ReplayResultID: decision.EvaluationRef.EvaluationID, CandidateDigest: candidate.CandidateRef.BodyDigest, ExpectedBaseVersion: candidate.ExpectedActiveVersion, NewArtifactVersion: candidate.ExpectedActiveVersion + 1, ActivatedAt: a.clock.Now(), DecisionRef: decision.Ref(), EvaluationRef: decision.EvaluationRef, CoverageRef: decision.CoverageRef, PolicyRef: decision.PolicyRef}
	created, err := a.policies.ActivateArmCCandidatePolicyDecision(ctx, candidate, activation)
	return activation, created, err
}
