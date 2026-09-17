package candidate

import "context"

// ToolCandidateStatus is the externally visible lifecycle state of a Tool
// Candidate. Later slices add candidate eligibility and evaluation states;
// the initial state is always draft.
type ToolCandidateStatus string

const (
	ToolCandidateDraft        ToolCandidateStatus = "draft"
	ToolCandidateArmCEligible ToolCandidateStatus = "arm_c_eligible"
)

// ToolContractValidationEvidence is the public evidence that one tool
// opportunity's fixed-fixture Tool Contract Validation passed.
type ToolContractValidationEvidence struct {
	ValidationID string
	Passed       bool
}

// ToolOpportunityEvidence identifies one independently provenanced operation
// that may later support Tool Candidate extraction or synthesis. LLM-synthesis
// eligibility requires each opportunity to carry passing Tool Contract
// Validation evidence and to be independent by lineage and Context Profile.
type ToolOpportunityEvidence struct {
	OpportunityID      string
	SourceLineageID    string
	ContextProfileID   string
	ContractValidation ToolContractValidationEvidence
}

// DirectExtractionExecutionEvidence identifies one successful execution from
// which a Tool Candidate can be directly extracted. Arm C eligibility requires
// evidence from at least two independent source lineages and Context Profiles.
type DirectExtractionExecutionEvidence struct {
	ExecutionID      string
	SourceLineageID  string
	ContextProfileID string
}

// LLMSynthesizedToolCandidateRequest submits a candidate synthesized from
// observed tool opportunities. The candidate becomes Arm-C-eligible only when
// at least two independent opportunities each carry passing Tool Contract
// Validation evidence.
type LLMSynthesizedToolCandidateRequest struct {
	CandidateID   string
	Opportunities []ToolOpportunityEvidence
}

// DirectExtractionToolCandidateRequest submits a candidate extracted from
// successful executions of the same reusable operation.
type DirectExtractionToolCandidateRequest struct {
	CandidateID string
	Executions  []DirectExtractionExecutionEvidence
}

// ToolCandidateService is the public seam for governed Tool Candidate
// creation. It hides source-evidence accounting and Arm C eligibility rules
// behind a small lifecycle interface.
type ToolCandidateService struct{}

func NewToolCandidateService() *ToolCandidateService { return &ToolCandidateService{} }

// SubmitLLMSynthesized retains an LLM-synthesized candidate as a draft until
// two independent, contract-validated source opportunities qualify it for Arm
// C. Eligibility never grants runtime execution authority.
func (*ToolCandidateService) SubmitLLMSynthesized(_ context.Context, request LLMSynthesizedToolCandidateRequest) (*ToolCandidateView, error) {
	eligible := hasIndependentValidatedToolOpportunities(request.Opportunities)
	status := ToolCandidateDraft
	if eligible {
		status = ToolCandidateArmCEligible
	}
	return &ToolCandidateView{
		candidateID:  request.CandidateID,
		status:       status,
		armCEligible: eligible,
	}, nil
}

func hasIndependentValidatedToolOpportunities(opportunities []ToolOpportunityEvidence) bool {
	lineages := make(map[string]struct{})
	contexts := make(map[string]struct{})
	for _, opportunity := range opportunities {
		if opportunity.ContractValidation.ValidationID == "" || !opportunity.ContractValidation.Passed {
			return false
		}
		if opportunity.SourceLineageID != "" {
			lineages[opportunity.SourceLineageID] = struct{}{}
		}
		if opportunity.ContextProfileID != "" {
			contexts[opportunity.ContextProfileID] = struct{}{}
		}
	}
	return len(lineages) >= 2 && len(contexts) >= 2
}

// SubmitDirectExtraction qualifies a directly extracted candidate for Arm C
// only when successful executions come from at least two independent source
// lineages and Context Profiles. Eligibility never grants execution authority.
func (*ToolCandidateService) SubmitDirectExtraction(_ context.Context, request DirectExtractionToolCandidateRequest) (*ToolCandidateView, error) {
	eligible := hasIndependentDirectExtractionEvidence(request.Executions)
	status := ToolCandidateDraft
	if eligible {
		status = ToolCandidateArmCEligible
	}
	return &ToolCandidateView{
		candidateID:  request.CandidateID,
		status:       status,
		armCEligible: eligible,
	}, nil
}

func hasIndependentDirectExtractionEvidence(executions []DirectExtractionExecutionEvidence) bool {
	lineages := make(map[string]struct{})
	contexts := make(map[string]struct{})
	for _, execution := range executions {
		if execution.SourceLineageID != "" {
			lineages[execution.SourceLineageID] = struct{}{}
		}
		if execution.ContextProfileID != "" {
			contexts[execution.ContextProfileID] = struct{}{}
		}
	}
	return len(lineages) >= 2 && len(contexts) >= 2
}

// ToolCandidateView is the immutable public result of Tool Candidate
// submission. It intentionally exposes state and eligibility but no runtime
// execution input.
type ToolCandidateView struct {
	candidateID  string
	status       ToolCandidateStatus
	armCEligible bool
}

func (v *ToolCandidateView) CandidateID() string { return v.candidateID }

func (v *ToolCandidateView) Status() ToolCandidateStatus { return v.status }

// ArmCEligible reports whether a candidate may enter the Candidate Validation
// Gate. Eligibility is not activation and does not create a runtime input.
func (v *ToolCandidateView) ArmCEligible() bool { return v.armCEligible }

// RuntimeInput fails closed because only activated Tool Skill revisions may
// execute. It matches the existing immutable CandidateView contract.
func (v *ToolCandidateView) RuntimeInput() error {
	return newError(ReasonCandidateNotExecutable,
		"tool candidate %s is not a runtime execution input; only an activated Tool Skill revision may execute", v.candidateID)
}
