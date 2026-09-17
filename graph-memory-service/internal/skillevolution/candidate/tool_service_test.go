package candidate_test

import (
	"context"
	"testing"

	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
)

// TestToolCandidateServiceKeepsSingleLLMSynthesisDraftAndNonExecutable proves
// the first ToolCandidateService safety boundary through its public interface:
// one source opportunity may be retained as a draft, but cannot enter Arm C or
// supply a runtime execution input.
func TestToolCandidateServiceKeepsSingleLLMSynthesisDraftAndNonExecutable(t *testing.T) {
	t.Parallel()

	service := candidate.NewToolCandidateService()
	view, err := service.SubmitLLMSynthesized(context.Background(), candidate.LLMSynthesizedToolCandidateRequest{
		CandidateID: "tool-candidate-1",
		Opportunities: []candidate.ToolOpportunityEvidence{{
			OpportunityID:   "opportunity-1",
			SourceLineageID: "lineage-1",
		}},
	})
	if err != nil {
		t.Fatalf("submit LLM-synthesized tool candidate: %v", err)
	}
	if got, want := view.Status(), candidate.ToolCandidateDraft; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if view.ArmCEligible() {
		t.Fatal("single-source LLM synthesis must not be Arm-C-eligible")
	}
	if got, want := candidate.CodeOf(view.RuntimeInput()), candidate.ReasonCandidateNotExecutable; got != want {
		t.Fatalf("runtime input code = %q, want %q", got, want)
	}
}

// TestToolCandidateServiceQualifiesIndependentDirectExtractionsForArmC proves
// that direct extraction requires independent source lineage and Context
// Profile evidence before a candidate may enter Arm C. Eligibility is not
// activation: the candidate still cannot become a runtime execution input.
func TestToolCandidateServiceQualifiesIndependentDirectExtractionsForArmC(t *testing.T) {
	t.Parallel()

	service := candidate.NewToolCandidateService()
	view, err := service.SubmitDirectExtraction(context.Background(), candidate.DirectExtractionToolCandidateRequest{
		CandidateID: "tool-candidate-2",
		Executions: []candidate.DirectExtractionExecutionEvidence{
			{
				ExecutionID:      "execution-1",
				SourceLineageID:  "lineage-1",
				ContextProfileID: "context-profile-1",
			},
			{
				ExecutionID:      "execution-2",
				SourceLineageID:  "lineage-2",
				ContextProfileID: "context-profile-2",
			},
		},
	})
	if err != nil {
		t.Fatalf("submit directly extracted tool candidate: %v", err)
	}
	if got, want := view.Status(), candidate.ToolCandidateArmCEligible; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if !view.ArmCEligible() {
		t.Fatal("two independent direct extractions must be Arm-C-eligible")
	}
	if got, want := candidate.CodeOf(view.RuntimeInput()), candidate.ReasonCandidateNotExecutable; got != want {
		t.Fatalf("runtime input code = %q, want %q", got, want)
	}
}

// TestToolCandidateServiceQualifiesIndependentValidatedLLMSynthesisForArmC
// proves that LLM synthesis needs two opportunities that are independent in
// both source lineage and Context Profile and each carry passing Tool Contract
// Validation evidence. Arm C eligibility remains non-executable.
func TestToolCandidateServiceQualifiesIndependentValidatedLLMSynthesisForArmC(t *testing.T) {
	t.Parallel()

	service := candidate.NewToolCandidateService()
	validatedOpportunities := []candidate.ToolOpportunityEvidence{
		{
			OpportunityID:    "opportunity-1",
			SourceLineageID:  "lineage-1",
			ContextProfileID: "context-profile-1",
			ContractValidation: candidate.ToolContractValidationEvidence{
				ValidationID: "validation-1",
				Passed:       true,
			},
		},
		{
			OpportunityID:    "opportunity-2",
			SourceLineageID:  "lineage-2",
			ContextProfileID: "context-profile-2",
			ContractValidation: candidate.ToolContractValidationEvidence{
				ValidationID: "validation-2",
				Passed:       true,
			},
		},
	}

	t.Run("qualifies two independent validated opportunities", func(t *testing.T) {
		view, err := service.SubmitLLMSynthesized(context.Background(), candidate.LLMSynthesizedToolCandidateRequest{
			CandidateID:   "tool-candidate-3",
			Opportunities: validatedOpportunities,
		})
		if err != nil {
			t.Fatalf("submit LLM-synthesized tool candidate: %v", err)
		}
		if got, want := view.Status(), candidate.ToolCandidateArmCEligible; got != want {
			t.Fatalf("status = %q, want %q", got, want)
		}
		if !view.ArmCEligible() {
			t.Fatal("two independent validated opportunities must be Arm-C-eligible")
		}
		if got, want := candidate.CodeOf(view.RuntimeInput()), candidate.ReasonCandidateNotExecutable; got != want {
			t.Fatalf("runtime input code = %q, want %q", got, want)
		}
	})

	t.Run("keeps draft when either opportunity lacks passing validation", func(t *testing.T) {
		missingValidation := append([]candidate.ToolOpportunityEvidence(nil), validatedOpportunities...)
		missingValidation[1].ContractValidation = candidate.ToolContractValidationEvidence{}
		view, err := service.SubmitLLMSynthesized(context.Background(), candidate.LLMSynthesizedToolCandidateRequest{
			CandidateID:   "tool-candidate-4",
			Opportunities: missingValidation,
		})
		if err != nil {
			t.Fatalf("submit LLM-synthesized tool candidate: %v", err)
		}
		if got, want := view.Status(), candidate.ToolCandidateDraft; got != want {
			t.Fatalf("status = %q, want %q", got, want)
		}
		if view.ArmCEligible() {
			t.Fatal("missing validation must keep LLM-synthesized candidate out of Arm C")
		}
		if got, want := candidate.CodeOf(view.RuntimeInput()), candidate.ReasonCandidateNotExecutable; got != want {
			t.Fatalf("runtime input code = %q, want %q", got, want)
		}
	})
}
