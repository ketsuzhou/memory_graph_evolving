package probation_test

import (
	"context"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/probation"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestProbationPolicyRequiresIndependentContinuationEvidenceAndToolContractValidation(t *testing.T) {
	store := usageprojection.NewMemoryStore()
	service, err := probation.NewService(probation.Policy{Version: "probation-v1", MinimumIndependentContexts: 2, RefutationSuspensionThreshold: 1, RecoveryAdditionalContexts: 1}, store)
	if err != nil {
		t.Fatal(err)
	}
	subject := subject("candidate-tool")
	one := profile("one")
	if err := store.AppendInteraction(context.Background(), continuation(subject, one)); err != nil {
		t.Fatal(err)
	}
	result := service.Admit(context.Background(), probation.AdmissionRequest{CandidateID: "candidate-tool", Subject: subject, Kind: "tool", ToolContractValidationID: "unregistered"})
	if result.Record.State != probation.StateAdvisory || result.Reason != probation.ReasonInsufficientIndependentContexts {
		t.Fatalf("one context = %#v", result)
	}
	if err := store.AppendInteraction(context.Background(), continuation(subject, profile("two"))); err != nil {
		t.Fatal(err)
	}
	result = service.Admit(context.Background(), probation.AdmissionRequest{CandidateID: "candidate-tool", Subject: subject, Kind: "tool", ToolContractValidationID: "missing-validation"})
	if result.Record.State != probation.StateAdvisory || result.Reason != probation.ReasonToolContractValidationRequired {
		t.Fatalf("missing tool validation = %#v", result)
	}
	validation := toolValidation(subject, true)
	if !service.RegisterToolContractValidation(validation) {
		t.Fatal("register exact tool validation")
	}
	result = service.Admit(context.Background(), probation.AdmissionRequest{CandidateID: "candidate-tool", Subject: subject, Kind: "tool", ToolContractValidationID: validation.ValidationID})
	if result.Record.State != probation.StateProbation || len(result.Record.Envelope.ContextProfileDigests) != 2 || result.Record.ActivationAuthority != probation.ActivationAuthorityArmCOnly {
		t.Fatalf("admission = %#v", result)
	}
	if result.Record.State == "active" {
		t.Fatal("probation must never become active without Arm C")
	}
}

func TestProbationEnvelopeRejectsOutsideContextAndSuspendsThenRequiresExplicitRecovery(t *testing.T) {
	store := usageprojection.NewMemoryStore()
	service, err := probation.NewService(probation.Policy{Version: "probation-v1", MinimumIndependentContexts: 2, RefutationSuspensionThreshold: 1, RecoveryAdditionalContexts: 1}, store)
	if err != nil {
		t.Fatal(err)
	}
	subject := subject("candidate-guidance")
	insideA, insideB, outside := profile("a"), profile("b"), profile("outside")
	for _, p := range []usageprojection.ContextProfile{insideA, insideB, profile("pre-suspension-third")} {
		if err := store.AppendInteraction(context.Background(), continuation(subject, p)); err != nil {
			t.Fatal(err)
		}
	}
	admitted := service.Admit(context.Background(), probation.AdmissionRequest{CandidateID: "candidate-guidance", Subject: subject, Kind: "step_guidance"})
	if !service.AdvisoryVisible("candidate-guidance", insideA) || service.AdvisoryVisible("candidate-guidance", outside) {
		t.Fatalf("envelope visibility incorrect: %+v", admitted.Record)
	}
	if err := store.AppendDiagnosis(context.Background(), refutation(subject, insideA)); err != nil {
		t.Fatal(err)
	}
	suspended := service.Reconcile(context.Background(), "candidate-guidance")
	if suspended.Record.State != probation.StateSuspended || suspended.Reason != probation.ReasonRefutationThresholdReached || service.AdvisoryVisible("candidate-guidance", insideA) {
		t.Fatalf("suspension = %#v", suspended)
	}
	recovery := service.Recover(context.Background(), probation.RecoveryRequest{CandidateID: "candidate-guidance", Explicit: true})
	if recovery.Record.State != probation.StateSuspended || recovery.Reason != probation.ReasonRecoveryEvidenceInsufficient {
		t.Fatalf("premature recovery = %#v", recovery)
	}
	if err := store.AppendInteraction(context.Background(), continuation(subject, profile("c"))); err != nil {
		t.Fatal(err)
	}
	recovery = service.Recover(context.Background(), probation.RecoveryRequest{CandidateID: "candidate-guidance", Explicit: true})
	if recovery.Record.State != probation.StateProbation || !service.AdvisoryVisible("candidate-guidance", profile("c")) {
		t.Fatalf("explicit recovery = %#v", recovery)
	}
}

func TestProbationIgnoresDiagnosticAndUncommittedUsageEvidence(t *testing.T) {
	store := usageprojection.NewMemoryStore()
	service, err := probation.NewService(probation.Policy{Version: "probation-v1", MinimumIndependentContexts: 2, RefutationSuspensionThreshold: 1, RecoveryAdditionalContexts: 1}, store)
	if err != nil {
		t.Fatal(err)
	}
	subject := subject("candidate-safe")
	one := continuation(subject, profile("one"))
	one.DiagnosticOnly = true
	two := continuation(subject, profile("two"))
	two.EvidenceRefs[0].CommitState = "staged"
	if err := store.AppendInteraction(context.Background(), one); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendInteraction(context.Background(), two); err != nil {
		t.Fatal(err)
	}
	result := service.Admit(context.Background(), probation.AdmissionRequest{CandidateID: "candidate-safe", Subject: subject, Kind: "step_guidance"})
	if result.Record.State != probation.StateAdvisory || result.Reason != probation.ReasonInsufficientIndependentContexts {
		t.Fatalf("inadmissible evidence = %#v", result)
	}
}

func profile(id string) usageprojection.ContextProfile {
	return usageprojection.ContextProfile{SchemaVersion: "context-profile/1.0", TaskFamily: "task-" + id, RuntimeClass: "go-test", WorkspaceFeatureTags: []string{id}, ObservableGuardFacts: []string{"continuation"}, ToolPolicyRef: vref("policy-" + id), EnvironmentClass: "sandboxed"}
}
func subject(id string) usageprojection.UsageSubjectRef {
	return usageprojection.UsageSubjectRef{CandidateRef: &contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: id, Kind: "tool", BodyDigest: digest(id), OriginType: "skill_proposal", OriginRef: vref("proposal-" + id)}}
}
func continuation(s usageprojection.UsageSubjectRef, p usageprojection.ContextProfile) usageprojection.Interaction {
	return usageprojection.Interaction{Subject: s, ContextProfile: p, SourceLineageID: "lineage-" + p.TaskFamily, Stage: usageprojection.InteractionVerified, EvidenceRefs: []contract.EvidenceRef{{SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "evidence-" + p.TaskFamily, Version: "1", EvidenceDigest: digest("evidence-" + p.TaskFamily), CommitState: "committed", EvidenceKind: "observation"}}}
}
func refutation(s usageprojection.UsageSubjectRef, p usageprojection.ContextProfile) usageprojection.DiagnosisUtilityAssessment {
	return usageprojection.DiagnosisUtilityAssessment{AssessmentID: "refute-" + p.TaskFamily, Subject: s, ContextProfile: p, SourceLineageID: "lineage-refute", ReturnedPathID: "path", AddressedAgentID: "agent", Rationale: "refuted", EvidenceRefs: []contract.EvidenceRef{{SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "evidence-refute", Version: "1", EvidenceDigest: digest("evidence-refute"), CommitState: "committed", EvidenceKind: "failure_path"}}, CounterevidenceRefs: []contract.EvidenceRef{{SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "counter", Version: "1", EvidenceDigest: digest("counter"), CommitState: "sealed", EvidenceKind: "failure_path"}}, RubricRef: vref("rubric")}
}
func vref(id string) contract.VersionedRef {
	return contract.VersionedRef{ID: id, Version: "1", Digest: digest(id)}
}
func digest(seed string) string { return "sha256:" + strings.Repeat(string(seed[0]), 64) }

func TestProbationSuspendsAfterToolContractFailureThreshold(t *testing.T) {
	store := usageprojection.NewMemoryStore()
	service, err := probation.NewService(probation.Policy{Version: "probation-v1", MinimumIndependentContexts: 2, RefutationSuspensionThreshold: 2, ContractFailureSuspensionThreshold: 2, RecoveryAdditionalContexts: 1}, store)
	if err != nil {
		t.Fatal(err)
	}
	subject := subject("candidate-contract")
	for _, p := range []usageprojection.ContextProfile{profile("a"), profile("b")} {
		if err := store.AppendInteraction(context.Background(), continuation(subject, p)); err != nil {
			t.Fatal(err)
		}
	}
	validation := toolValidation(subject, true)
	if !service.RegisterToolContractValidation(validation) {
		t.Fatal("register validation")
	}
	admitted := service.Admit(context.Background(), probation.AdmissionRequest{CandidateID: "candidate-contract", Subject: subject, Kind: "tool", ToolContractValidationID: validation.ValidationID})
	if admitted.Record.State != probation.StateProbation {
		t.Fatalf("admission = %#v", admitted)
	}
	if decision := service.ReportToolContractValidation("candidate-contract", toolValidation(subject, false)); decision.Record.State != probation.StateProbation {
		t.Fatalf("first failure = %#v", decision)
	}
	suspended := service.ReportToolContractValidation("candidate-contract", toolValidation(subject, false))
	if suspended.Record.State != probation.StateSuspended || suspended.Reason != probation.ReasonToolContractFailureThresholdReached {
		t.Fatalf("contract suspension = %#v", suspended)
	}
}

func toolValidation(subject usageprojection.UsageSubjectRef, passed bool) probation.ToolContractValidation {
	suffix := "failed"
	if passed {
		suffix = "passed"
	}
	return probation.ToolContractValidation{ResultRef: vref("validation-result-" + suffix + "-" + subject.CandidateRef.CandidateID), ValidationID: "validation-" + suffix + "-" + subject.CandidateRef.CandidateID, CandidateID: subject.CandidateRef.CandidateID, CandidateBodyDigest: subject.CandidateRef.BodyDigest, ContractRef: vref("tool-contract"), Passed: passed}
}
