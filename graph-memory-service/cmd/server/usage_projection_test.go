package main

import (
	"context"
	"io"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/policyactivation"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
	"river2.dev/graph-memory-service/internal/store/memory"
)

func TestProjectorWorkerInjectsDurableUsageProjectionStore(t *testing.T) {
	store := memory.New()
	candidates := memory.CandidateStore{Store: store}
	worker, err := newProjectorWorker(httpapi.NewJSONLogger(io.Discard), 0, "test-token", "", candidates)
	if err != nil {
		t.Fatalf("new projector worker: %v", err)
	}
	if worker.usageProjection == nil || worker.usageStore == nil || worker.probation == nil || worker.armcStore == nil || worker.armcService == nil || worker.policyWorker == nil {
		t.Fatal("projector worker must wire durable usage and server-owned Arm C services")
	}
	profile := usageprojection.ContextProfile{
		SchemaVersion: "context-profile/1.0", TaskFamily: "code_implementation", RuntimeClass: "go-1.26-linux",
		ToolPolicyRef:    contract.VersionedRef{ID: "policy", Version: "1", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		EnvironmentClass: "sandboxed-linux",
	}
	if err := worker.usageProjection.RecordInteraction(context.Background(), usageprojection.Interaction{
		Subject: usageprojection.UsageSubjectRef{SkillRef: &contract.SkillArtifactRef{
			SchemaVersion: contract.SchemaSkillArtifactRef, LineageID: "lineage", Version: "1", Kind: "step_guidance",
			ArtifactDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}},
		ContextProfile: profile, SourceLineageID: "source-lineage", Stage: usageprojection.InteractionMatched,
	}); err != nil {
		t.Fatalf("record injected usage interaction: %v", err)
	}
	service, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, worker.usageStore)
	if err != nil {
		t.Fatalf("new second service over injected store: %v", err)
	}
	summary, err := service.UsageSummary(context.Background(), usageprojection.UsageSummaryRequest{})
	if err != nil {
		t.Fatalf("read injected usage summary: %v", err)
	}
	if len(summary.Entries) != 1 {
		t.Fatalf("injected store summary entries = %d, want 1", len(summary.Entries))
	}
}

func TestProjectorWorkerCompleteArmCUsesServerOwnedEvaluator(t *testing.T) {
	store := memory.New()
	candidates := memory.CandidateStore{Store: store}
	candidate := domain.ArmCCandidateRegistration{TenantID: "tenant-1", SpaceID: "space-1", TargetSkillID: "skill-1", ExpectedActiveVersion: 0, CandidateRef: contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: "candidate-1", Kind: "step_guidance", BodyDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", OriginType: "skill_proposal", OriginRef: contract.VersionedRef{ID: "proposal-1", Version: "1", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}
	if _, err := candidates.RegisterArmCCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("register direct candidate: %v", err)
	}
	worker, err := newProjectorWorker(httpapi.NewJSONLogger(io.Discard), time.Second, "test-token", "", candidates)
	if err != nil {
		t.Fatalf("new projector worker: %v", err)
	}
	policy := domain.PolicyArtifactRef{PolicyID: "arm-c-policy", Version: 1, Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}
	validationContract := armc.ValidationContract{ContractID: "contract-1", Version: 1, TaskFamily: "code_implementation", AllowedCommands: []string{"verify"}, RequiredArtifactInvariants: []string{"artifact.valid"}, Limits: armc.ResourceLimits{MaxCommands: 1, MaxCPUUnits: 10, MaxMemoryBytes: 1024, MaxDurationMillis: 1000}, SuccessSignals: []string{"pass"}}
	validationContract.Digest = armc.ValidationContractDigest(validationContract)
	if _, err := worker.armcStore.PutValidationContract(context.Background(), validationContract); err != nil {
		t.Fatalf("put validation contract: %v", err)
	}
	manifest := armc.TaskFamilyManifest{ManifestID: "family-1", Version: 1, TaskFamily: "code_implementation", ContractRef: validationContract.Ref()}
	manifest.Digest = armc.TaskFamilyManifestDigest(manifest)
	if _, err := worker.armcStore.PutTaskFamilyManifest(context.Background(), manifest); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	fixture := armc.PairedFixture{FixtureID: "fixture-1", TaskFamily: "code_implementation", Candidate: armc.FixtureRun{Commands: []armc.CommandResult{{Name: "verify", ExitCode: 0}}, ArtifactInvariants: map[string]bool{"artifact.valid": true}, SuccessSignals: []string{"pass"}, CPUUnits: 1, MemoryBytes: 1, DurationMillis: 1}, Baseline: armc.FixtureRun{Commands: []armc.CommandResult{{Name: "verify", ExitCode: 0}}, ArtifactInvariants: map[string]bool{"artifact.valid": true}, CPUUnits: 1, MemoryBytes: 1, DurationMillis: 1}}
	if _, err := worker.armcStore.PutFixture(context.Background(), fixture); err != nil {
		t.Fatalf("put fixture: %v", err)
	}
	coverage := domain.CoverageProof{ProofID: "coverage-1", Version: 1, IndependentLineageCount: 2, IndependentContextProfileCount: 2, ThresholdPolicyRef: policy}
	coverage.Digest = domain.CoverageProofDigest(coverage)
	if _, err := worker.armcStore.PutPlan(context.Background(), armc.EvaluationPlan{CandidateID: candidate.CandidateRef.CandidateID, DecisionID: "decision-1", DecisionVersion: 1, ManifestRef: manifest.Ref(), FixtureIDs: []string{fixture.FixtureID}, PolicyRef: policy, ExpectedActiveVersion: candidate.ExpectedActiveVersion, Coverage: coverage}); err != nil {
		t.Fatalf("put Arm C plan: %v", err)
	}
	result, err := worker.CompleteArmC(context.Background(), policyactivation.Job{TenantID: candidate.TenantID, SpaceID: candidate.SpaceID, CandidateID: candidate.CandidateRef.CandidateID})
	if err != nil || !result.Activated || result.Activation == nil {
		t.Fatalf("server-owned Arm C completion = (%#v, %v)", result, err)
	}
}
