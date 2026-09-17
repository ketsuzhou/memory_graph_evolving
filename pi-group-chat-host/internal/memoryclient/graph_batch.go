package memoryclient

import (
	"context"
	"net/http"
)

const (
	SkillProposePath             = "/v1/skill-evolution/tools/skill_propose"
	SkillConsolidatePath         = "/v1/skill-evolution/tools/skill_consolidate"
	EvaluationFreezePath         = "/v1/skill-evolution/evaluation-freeze"
	GraphBatchTrajectoryRegister = "/v1/skill-evolution/trajectories:register"
)

type SkillProposeRequest struct {
	RequestID      string         `json:"request_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	DiagnosisRunID string         `json:"diagnosis_run_id"`
	TrajectoryID   string         `json:"trajectory_id"`
	Body           map[string]any `json:"body"`
}

type SkillProposeResponse struct {
	ProposalID    string `json:"proposal_id"`
	ContentDigest string `json:"content_digest"`
	URI           string `json:"uri"`
}

type SkillConsolidateRequest struct {
	RequestID              string   `json:"request_id"`
	IdempotencyKey         string   `json:"idempotency_key"`
	AgentRunID             string   `json:"agent_run_id"`
	ExpectedLedgerRevision uint64   `json:"expected_ledger_revision"`
	FamilyProposalIDs      []string `json:"family_proposal_ids"`
}

type SkillConsolidateDecision struct {
	DecisionID        string   `json:"decision_id"`
	Operation         string   `json:"operation"`
	SourceProposalIDs []string `json:"source_proposal_ids"`
}

type SkillConsolidateResponse struct {
	LedgerRevision uint64                      `json:"ledger_revision"`
	LedgerDigest   string                      `json:"ledger_digest"`
	Decisions      []SkillConsolidateDecision `json:"decisions"`
}

type EvaluationFreezeEvidenceBatch struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

type EvaluationFreezeEvidenceCut struct {
	Batches         []EvaluationFreezeEvidenceBatch `json:"batches"`
	Watermark       string                          `json:"watermark"`
	WatermarkDigest string                          `json:"watermark_digest"`
}

type EvaluationFreezeScopeItem struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type EvaluationFreezePolicy struct {
	PromptDigest        string `json:"prompt_digest"`
	SchemaDigest        string `json:"schema_digest"`
	ModelDigest         string `json:"model_digest"`
	ToolDigest          string `json:"tool_digest"`
	ConfigDigest        string `json:"config_digest"`
	GradingPolicyDigest string `json:"grading_policy_digest"`
}

type EvaluationFreezeRequest struct {
	RequestID              string                      `json:"request_id"`
	ExpectedLedgerRevision uint64                      `json:"expected_ledger_revision"`
	ExpectedLedgerDigest   string                      `json:"expected_ledger_digest"`
	ProposalIDs            []string                    `json:"proposal_ids"`
	EvidenceCut            EvaluationFreezeEvidenceCut `json:"evidence_cut"`
	Policy                 EvaluationFreezePolicy      `json:"policy"`
	Scope                  []EvaluationFreezeScopeItem `json:"scope"`
}

type EvaluationFreezeResponse struct {
	ManifestID     string `json:"manifest_id"`
	Digest         string `json:"digest"`
	LedgerRevision uint64 `json:"ledger_revision"`
	LedgerDigest   string `json:"ledger_digest"`
}

type GraphBatchFrozenTrajectory struct {
	ID                        string                       `json:"id"`
	SnapshotID                string                       `json:"snapshot_id"`
	CompleteTrajectory        []string                     `json:"complete_trajectory"`
	PublicOutcome             string                       `json:"public_outcome"`
	Checkpoints               map[string]GraphBatchCheckpoint `json:"checkpoints"`
	Evidence                  map[string]GraphBatchEvidence   `json:"evidence"`
	AuthorizedDiagnosisRunIDs []string                     `json:"authorized_diagnosis_run_ids"`
}

type GraphBatchCheckpoint struct {
	ID         string `json:"id"`
	SnapshotID string `json:"snapshot_id"`
}

type GraphBatchEvidence struct {
	ID              string   `json:"id"`
	TrajectoryID    string   `json:"trajectory_id"`
	SnapshotID      string   `json:"snapshot_id"`
	ObservableFacts []string `json:"observable_facts"`
}

type GraphBatchTrajectoryRequest struct {
	DiagnosisRunID string                     `json:"diagnosis_run_id"`
	Trajectory     GraphBatchFrozenTrajectory `json:"trajectory"`
}

func (c *Client) SkillPropose(ctx context.Context, request SkillProposeRequest) (SkillProposeResponse, error) {
	var response SkillProposeResponse
	err := c.do(ctx, http.MethodPost, SkillProposePath, request, &response)
	return response, err
}

func (c *Client) SkillConsolidate(ctx context.Context, request SkillConsolidateRequest) (SkillConsolidateResponse, error) {
	var response SkillConsolidateResponse
	err := c.do(ctx, http.MethodPost, SkillConsolidatePath, request, &response)
	return response, err
}

func (c *Client) EvaluationFreeze(ctx context.Context, request EvaluationFreezeRequest) (EvaluationFreezeResponse, error) {
	var response EvaluationFreezeResponse
	err := c.do(ctx, http.MethodPost, EvaluationFreezePath, request, &response)
	return response, err
}

func (c *Client) RegisterGraphBatchTrajectory(ctx context.Context, request GraphBatchTrajectoryRequest) error {
	return c.do(ctx, http.MethodPost, GraphBatchTrajectoryRegister, request, nil)
}
