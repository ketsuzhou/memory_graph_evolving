package policyactivation_test

import (
	"context"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/policyactivation"
	"river2.dev/graph-memory-service/internal/store/memory"
)

func TestDirectWorkerPreservesArmCSemanticsWithoutLegacySkillCandidate(t *testing.T) {
	registration := directRegistration()
	store := memory.CandidateStore{Store: memory.New()}
	if _, err := store.RegisterArmCCandidate(context.Background(), registration); err != nil {
		t.Fatalf("register direct candidate: %v", err)
	}
	worker := newDirectWorker(t, store, validDirectOutput(registration))
	result, err := worker.Process(context.Background(), policyactivation.Job{TenantID: registration.TenantID, SpaceID: registration.SpaceID, CandidateID: registration.CandidateRef.CandidateID})
	if err != nil || !result.Activated || result.Activation == nil {
		t.Fatalf("direct activation = (%#v, %v)", result, err)
	}
	if result.Activation.CandidateDigest != registration.CandidateRef.BodyDigest || result.Activation.ExpectedBaseVersion != registration.ExpectedActiveVersion {
		t.Fatalf("activation binding = %#v", result.Activation)
	}
}

func TestDirectWorkerRejectsRegistrationEvaluationMismatchLikeLegacyBridge(t *testing.T) {
	registration := directRegistration()
	store := memory.CandidateStore{Store: memory.New()}
	if _, err := store.RegisterArmCCandidate(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	output := validDirectOutput(registration)
	output.Evaluation.CandidateDigest = contract.DigestBytes([]byte("mismatched-body"))
	output.Evaluation.Digest = domain.ArmCEvaluationDigest(output.Evaluation)
	worker := newDirectWorker(t, store, output)
	result, err := worker.Process(context.Background(), policyactivation.Job{TenantID: registration.TenantID, SpaceID: registration.SpaceID, CandidateID: registration.CandidateRef.CandidateID})
	if err != nil {
		t.Fatal(err)
	}
	if result.Activated || !result.Rejected || result.RejectionReason != "arm_c_evaluation_candidate_mismatch" {
		t.Fatalf("mismatch result = %#v", result)
	}
}

func newDirectWorker(t *testing.T, store memory.CandidateStore, output policyactivation.EvaluationOutput) *policyactivation.DirectWorker {
	t.Helper()
	worker, err := policyactivation.NewDirectWorker(store, store, directEvaluator{output: output}, policyactivation.NewDirectActivator(store, fixedDirectClock{time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)}))
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

type directEvaluator struct {
	output policyactivation.EvaluationOutput
}

func (d directEvaluator) Evaluate(context.Context, policyactivation.DirectEvaluationRequest) (policyactivation.EvaluationOutput, error) {
	return d.output, nil
}

type fixedDirectClock struct{ time.Time }

func (c fixedDirectClock) Now() time.Time { return c.Time }

func directRegistration() domain.ArmCCandidateRegistration {
	return domain.ArmCCandidateRegistration{TenantID: "tenant-direct", SpaceID: "space-direct", TargetSkillID: "skill-direct", ExpectedActiveVersion: 0, CandidateRef: contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: "candidate-direct", Kind: "composite", BodyDigest: contract.DigestBytes([]byte("candidate-body")), OriginType: "merge_proposal", OriginRef: contract.VersionedRef{ID: "proposal-direct", Version: "1", Digest: contract.DigestBytes([]byte("proposal"))}}}
}
func validDirectOutput(registration domain.ArmCCandidateRegistration) policyactivation.EvaluationOutput {
	policy := domain.PolicyArtifactRef{PolicyID: "arm-c-policy", Version: 1, Digest: contract.DigestBytes([]byte("policy"))}
	output := policyactivation.EvaluationOutput{DecisionID: "decision-direct", DecisionVersion: 1, Evaluation: domain.ArmCEvaluation{EvaluationID: "evaluation-direct", Version: 1, CandidateID: registration.CandidateRef.CandidateID, CandidateDigest: registration.CandidateRef.BodyDigest, Passed: true}, Coverage: domain.CoverageProof{ProofID: "coverage-direct", Version: 1, IndependentLineageCount: 2, IndependentContextProfileCount: 2, ThresholdPolicyRef: policy}, PolicyRef: policy, ExpectedActiveVersion: registration.ExpectedActiveVersion}
	output.Evaluation.Digest = domain.ArmCEvaluationDigest(output.Evaluation)
	output.Coverage.Digest = domain.CoverageProofDigest(output.Coverage)
	return output
}

func TestDirectRegistrationCannotSeedCandidateClaimedExpectedHead(t *testing.T) {
	registration := directRegistration()
	registration.ExpectedActiveVersion = 7
	store := memory.CandidateStore{Store: memory.New()}
	if _, err := store.RegisterArmCCandidate(context.Background(), registration); err == nil {
		t.Fatal("unknown target must reject a candidate-claimed nonzero expected head")
	}
}
