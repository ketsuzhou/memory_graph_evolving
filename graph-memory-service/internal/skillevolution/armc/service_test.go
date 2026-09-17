package armc_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/policyactivation"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
	"river2.dev/graph-memory-service/internal/store/memory"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestArmCServerOwnedEvaluationResolvesContractAndActivatesOrRejects(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects manifest contract digest mismatch without activation", func(t *testing.T) {
		fixture := validFixture(false)
		contractArtifact := validContract()
		manifest := validManifest(contractArtifact.Ref())
		manifest.ContractRef.Digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		manifest.Digest = armc.TaskFamilyManifestDigest(manifest)
		result, policies := process(t, contractArtifact, manifest, fixture)
		if result.Activated || !result.Rejected || result.RejectionReason != "validation_contract_ref_mismatch" {
			t.Fatalf("result = %#v, want contract-ref rejection", result)
		}
		decision, err := policies.ActivationPolicyDecision(ctx, candidate().TenantID, candidate().SpaceID, result.DecisionRef)
		if err != nil {
			t.Fatalf("read reject decision: %v", err)
		}
		if decision.Outcome != domain.ActivationOutcomeReject || decision.Reason != "validation_contract_ref_mismatch" {
			t.Fatalf("decision = %#v", decision)
		}
	})

	t.Run("paired candidate pass over baseline activates", func(t *testing.T) {
		contractArtifact := validContract()
		result, policies := process(t, contractArtifact, validManifest(contractArtifact.Ref()), validFixture(false))
		if !result.Activated || result.Rejected || result.Activation == nil {
			t.Fatalf("result = %#v, want activation", result)
		}
		decision, err := policies.ActivationPolicyDecision(ctx, candidate().TenantID, candidate().SpaceID, result.DecisionRef)
		if err != nil {
			t.Fatalf("read activation decision: %v", err)
		}
		if decision.Outcome != domain.ActivationOutcomeActivate {
			t.Fatalf("decision = %#v, want activate", decision)
		}
	})

	t.Run("paired non-improvement writes reject decision", func(t *testing.T) {
		contractArtifact := validContract()
		result, policies := process(t, contractArtifact, validManifest(contractArtifact.Ref()), validFixture(true))
		if result.Activated || !result.Rejected || result.RejectionReason != "paired_candidate_not_better" {
			t.Fatalf("result = %#v, want paired rejection", result)
		}
		decision, err := policies.ActivationPolicyDecision(ctx, candidate().TenantID, candidate().SpaceID, result.DecisionRef)
		if err != nil {
			t.Fatalf("read paired reject decision: %v", err)
		}
		if decision.Outcome != domain.ActivationOutcomeReject || decision.Reason != "paired_candidate_not_better" {
			t.Fatalf("decision = %#v", decision)
		}
	})
}

func process(t *testing.T, contractArtifact armc.ValidationContract, manifest armc.TaskFamilyManifest, fixture armc.PairedFixture) (policyactivation.Result, memory.CandidateStore) {
	t.Helper()
	ctx := context.Background()
	policies := memory.CandidateStore{Store: memory.New()}
	if _, err := policies.RegisterArmCCandidate(ctx, candidate()); err != nil {
		t.Fatalf("register direct candidate: %v", err)
	}
	store := armc.NewMemoryStore()
	if _, err := store.PutValidationContract(ctx, contractArtifact); err != nil {
		t.Fatalf("put contract: %v", err)
	}
	if _, err := store.PutTaskFamilyManifest(ctx, manifest); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if _, err := store.PutFixture(ctx, fixture); err != nil {
		t.Fatalf("put fixture: %v", err)
	}
	coverage := domain.CoverageProof{ProofID: "coverage-1", Version: 1, IndependentLineageCount: 2, IndependentContextProfileCount: 2, ThresholdPolicyRef: policyRef()}
	coverage.Digest = domain.CoverageProofDigest(coverage)
	if _, err := store.PutPlan(ctx, armc.EvaluationPlan{
		CandidateID: candidate().CandidateRef.CandidateID, DecisionID: "armc-decision-1", DecisionVersion: 1,
		ManifestRef: manifest.Ref(), FixtureIDs: []string{fixture.FixtureID},
		PolicyRef: policyRef(), ExpectedActiveVersion: candidate().ExpectedActiveVersion,
		Coverage: coverage,
	}); err != nil {
		t.Fatalf("put plan: %v", err)
	}
	evaluator, err := armc.NewService(store, loadGates(t))
	if err != nil {
		t.Fatalf("new Arm C service: %v", err)
	}
	worker, err := policyactivation.NewDirectWorker(policies, policies, evaluator.DirectEvaluator(), policyactivation.NewDirectActivator(policies, fixedClock{now: time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)}))
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	result, err := worker.Process(ctx, policyactivation.Job{TenantID: candidate().TenantID, SpaceID: candidate().SpaceID, CandidateID: candidate().CandidateRef.CandidateID})
	if err != nil {
		t.Fatalf("process Arm C: %v", err)
	}
	return result, policies
}

func validContract() armc.ValidationContract {
	contractArtifact := armc.ValidationContract{
		ContractID: "validation-contract-1", Version: 1, TaskFamily: "code_implementation",
		AllowedCommands: []string{"verify"}, RequiredArtifactInvariants: []string{"artifact.valid"},
		Limits:         armc.ResourceLimits{MaxCommands: 1, MaxCPUUnits: 10, MaxMemoryBytes: 1024, MaxDurationMillis: 1000},
		SuccessSignals: []string{"contract-pass"}, FailureSignals: []string{"contract-fail"},
	}
	contractArtifact.Digest = armc.ValidationContractDigest(contractArtifact)
	return contractArtifact
}

func validManifest(ref armc.ValidationContractRef) armc.TaskFamilyManifest {
	manifest := armc.TaskFamilyManifest{ManifestID: "family-code", Version: 1, TaskFamily: "code_implementation", ContractRef: ref}
	manifest.Digest = armc.TaskFamilyManifestDigest(manifest)
	return manifest
}

func validFixture(baselinePasses bool) armc.PairedFixture {
	baselineSignals := []string(nil)
	if baselinePasses {
		baselineSignals = []string{"contract-pass"}
	}
	return armc.PairedFixture{
		FixtureID: "fixture-1", TaskFamily: "code_implementation",
		Candidate: armc.FixtureRun{Commands: []armc.CommandResult{{Name: "verify", ExitCode: 0}}, ArtifactInvariants: map[string]bool{"artifact.valid": true}, SuccessSignals: []string{"contract-pass"}, CPUUnits: 1, MemoryBytes: 128, DurationMillis: 10},
		Baseline:  armc.FixtureRun{Commands: []armc.CommandResult{{Name: "verify", ExitCode: 0}}, ArtifactInvariants: map[string]bool{"artifact.valid": true}, SuccessSignals: baselineSignals, CPUUnits: 1, MemoryBytes: 128, DurationMillis: 10},
	}
}

func candidate() domain.ArmCCandidateRegistration {
	body := contract.DigestBytes([]byte("candidate-digest"))
	return domain.ArmCCandidateRegistration{TenantID: "tenant-1", SpaceID: "space-1", TargetSkillID: "skill-1", ExpectedActiveVersion: 0, CandidateRef: contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: "candidate-1", Kind: "step_guidance", BodyDigest: body, OriginType: "skill_proposal", OriginRef: contract.VersionedRef{ID: "proposal-1", Version: "1", Digest: contract.DigestBytes([]byte("proposal-1"))}}}
}

func policyRef() domain.PolicyArtifactRef {
	return domain.PolicyArtifactRef{PolicyID: "arm-c-policy", Version: 1, Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}
}

func loadGates(t *testing.T) *validation.Gates {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("conformance dir: %v", err)
	}
	schemas, err := validation.LoadSchemaSet(filepath.Join(dir, "schema", "shared"))
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		t.Fatalf("new gates: %v", err)
	}
	return gates
}

func TestMemoryStoreSnapshotPreservesPendingPlanInputs(t *testing.T) {
	store := armc.NewMemoryStore()
	contractArtifact := validContract()
	manifest := validManifest(contractArtifact.Ref())
	fixture := validFixture(false)
	coverage := domain.CoverageProof{ProofID: "coverage-pending", Version: 1, IndependentLineageCount: 2, IndependentContextProfileCount: 2, ThresholdPolicyRef: policyRef()}
	coverage.Digest = domain.CoverageProofDigest(coverage)
	plan := armc.EvaluationPlan{CandidateID: "pending", DecisionID: "pending-decision", DecisionVersion: 1, ManifestRef: manifest.Ref(), FixtureIDs: []string{fixture.FixtureID}, PolicyRef: policyRef(), ExpectedActiveVersion: 0, Coverage: coverage}
	if _, err := store.PutValidationContract(context.Background(), contractArtifact); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutTaskFamilyManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutFixture(context.Background(), fixture); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutPlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	image, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := armc.NewMemoryStore()
	if err := restored.Restore(image); err != nil {
		t.Fatal(err)
	}
	if got, err := restored.Plan(context.Background(), "pending"); err != nil || !reflect.DeepEqual(got, plan) {
		t.Fatalf("restored plan = %#v err=%v", got, err)
	}
	if _, err := restored.ValidationContract(context.Background(), contractArtifact.Ref()); err != nil {
		t.Fatalf("contract missing after restore: %v", err)
	}
	if _, err := restored.TaskFamilyManifest(context.Background(), manifest.Ref()); err != nil {
		t.Fatalf("manifest missing after restore: %v", err)
	}
	if _, err := restored.Fixture(context.Background(), fixture.FixtureID); err != nil {
		t.Fatalf("fixture missing after restore: %v", err)
	}
}
