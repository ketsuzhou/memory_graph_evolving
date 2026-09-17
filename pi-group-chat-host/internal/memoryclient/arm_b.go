package memoryclient

import (
	"context"
	"net/http"
)

const (
	SkillEvolutionInteractionRecordPath     = "/v1/skill-evolution/interactions:record"
	SkillEvolutionDiagnosisRecordPath       = "/v1/skill-evolution/diagnoses:record"
	SkillEvolutionAdvisoryReadPath          = "/v1/skill-evolution/advisory-candidates:read"
	SkillEvolutionCandidateOutcomesReadPath = "/v1/skill-evolution/candidate-outcomes:read"
)

// SkillEvolutionVersionedRef carries the exact immutable policy/rubric/origin
// reference required by the Arm B producer surface.
type SkillEvolutionVersionedRef struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
	Digest  string `json:"digest"`
}

type SkillEvolutionCandidateRef struct {
	SchemaVersion string                     `json:"schema_version"`
	CandidateID   string                     `json:"candidate_id"`
	Kind          string                     `json:"kind"`
	BodyDigest    string                     `json:"body_digest"`
	OriginType    string                     `json:"origin_type"`
	OriginRef     SkillEvolutionVersionedRef `json:"origin_ref"`
}

type SkillEvolutionSubject struct {
	CandidateRef *SkillEvolutionCandidateRef `json:"candidate_ref,omitempty"`
}

type SkillEvolutionEvidenceRef struct {
	SchemaVersion  string                      `json:"schema_version"`
	EvidenceID     string                      `json:"evidence_id"`
	Version        int64                       `json:"version"`
	EvidenceDigest string                      `json:"evidence_digest"`
	CommitState    string                      `json:"commit_state"`
	EvidenceKind   string                      `json:"evidence_kind"`
	SourceSegment  *SkillEvolutionVersionedRef `json:"source_segment_ref,omitempty"`
}

type SkillEvolutionContextProfile struct {
	SchemaVersion        string                     `json:"schema_version"`
	TaskFamily           string                     `json:"task_family"`
	RuntimeClass         string                     `json:"runtime_class"`
	WorkspaceFeatureTags []string                   `json:"workspace_feature_tags"`
	ObservableGuardFacts []string                   `json:"observable_guard_facts"`
	ToolPolicyRef        SkillEvolutionVersionedRef `json:"tool_policy_ref"`
	EnvironmentClass     string                     `json:"environment_class"`
}

type SkillEvolutionInteractionRecordRequest struct {
	RequestID         string                       `json:"request_id"`
	Subject           SkillEvolutionSubject        `json:"subject"`
	ContextProfile    SkillEvolutionContextProfile `json:"context_profile"`
	Stage             string                       `json:"stage"`
	EvidenceRefs      []SkillEvolutionEvidenceRef  `json:"evidence_refs,omitempty"`
	EvaluationBatchID string                       `json:"evaluation_batch_id,omitempty"`
	DiagnosticOnly    bool                         `json:"diagnostic_only,omitempty"`
}

type SkillEvolutionDiagnosisRecordRequest struct {
	RequestID               string                       `json:"request_id"`
	AssessmentID            string                       `json:"assessment_id"`
	Subject                 SkillEvolutionSubject        `json:"subject"`
	ContextProfile          SkillEvolutionContextProfile `json:"context_profile"`
	ReturnedPathID          string                       `json:"returned_path_id"`
	AddressedAgentID        string                       `json:"addressed_agent_id"`
	AdoptionEvidenceRefs    []SkillEvolutionEvidenceRef  `json:"adoption_evidence_refs,omitempty"`
	ContributionScoreMicros int64                        `json:"contribution_score_micros"`
	ConfidenceMicros        int64                        `json:"confidence_micros"`
	CounterevidenceRefs     []SkillEvolutionEvidenceRef  `json:"counterevidence_refs,omitempty"`
	Rationale               string                       `json:"rationale"`
	EvidenceRefs            []SkillEvolutionEvidenceRef  `json:"evidence_refs"`
	RubricRef               SkillEvolutionVersionedRef   `json:"rubric_ref"`
	EvaluationBatchID       string                       `json:"evaluation_batch_id"`
	DiagnosticOnly          bool                         `json:"diagnostic_only,omitempty"`
}

type SkillEvolutionWriteResponse struct {
	SchemaVersion    string `json:"schema_version"`
	RequestID        string `json:"request_id"`
	Recorded         bool   `json:"recorded"`
	NonAuthoritative bool   `json:"non_authoritative"`
	WriterIdentity   string `json:"writer_identity"`
}

type SkillEvolutionAdvisoryReadRequest struct {
	RequestID      string                       `json:"request_id"`
	ContextProfile SkillEvolutionContextProfile `json:"context_profile"`
}

type SkillEvolutionAdvisoryCandidate struct {
	CandidateID      string                      `json:"candidate_id"`
	CandidateRef     *SkillEvolutionCandidateRef `json:"candidate_ref,omitempty"`
	Kind             string                      `json:"kind"`
	Guidance         string                      `json:"guidance"`
	NonAuthoritative bool                        `json:"non_authoritative"`
}

type SkillEvolutionAdvisoryReadResponse struct {
	SchemaVersion    string                            `json:"schema_version"`
	RequestID        string                            `json:"request_id"`
	NonAuthoritative bool                              `json:"non_authoritative"`
	Candidates       []SkillEvolutionAdvisoryCandidate `json:"candidates"`
}

// RecordSkillEvolutionInteraction persists an observational Arm B stage. The
// server owns writer identity; this client deliberately exposes no such field.
func (c *Client) RecordSkillEvolutionInteraction(ctx context.Context, request SkillEvolutionInteractionRecordRequest) (SkillEvolutionWriteResponse, error) {
	var response SkillEvolutionWriteResponse
	err := c.do(ctx, http.MethodPost, SkillEvolutionInteractionRecordPath, request, &response)
	return response, err
}

// RecordSkillEvolutionDiagnosis persists an observational assessment. It has
// no authority to activate, promote, or modify an advisory candidate.
func (c *Client) RecordSkillEvolutionDiagnosis(ctx context.Context, request SkillEvolutionDiagnosisRecordRequest) (SkillEvolutionWriteResponse, error) {
	var response SkillEvolutionWriteResponse
	err := c.do(ctx, http.MethodPost, SkillEvolutionDiagnosisRecordPath, request, &response)
	return response, err
}

// ReadSkillEvolutionAdvisoryCandidates reads text-only, non-authoritative
// candidates. It intentionally carries no runtime/tool execution surface.
func (c *Client) ReadSkillEvolutionAdvisoryCandidates(ctx context.Context, request SkillEvolutionAdvisoryReadRequest) (SkillEvolutionAdvisoryReadResponse, error) {
	var response SkillEvolutionAdvisoryReadResponse
	err := c.do(ctx, http.MethodPost, SkillEvolutionAdvisoryReadPath, request, &response)
	return response, err
}

type SkillEvolutionCandidateOutcomesReadRequest struct {
	RequestID     string                       `json:"request_id"`
	CandidateRefs []SkillEvolutionCandidateRef `json:"candidate_refs"`
}
type SkillEvolutionCandidateOutcome struct {
	CandidateRef SkillEvolutionCandidateRef  `json:"candidate_ref"`
	Status       string                      `json:"status"`
	DecisionRef  *SkillEvolutionVersionedRef `json:"decision_ref,omitempty"`
	Reason       string                      `json:"reason,omitempty"`
}
type SkillEvolutionCandidateOutcomesReadResponse struct {
	SchemaVersion string                           `json:"schema_version"`
	RequestID     string                           `json:"request_id"`
	Outcomes      []SkillEvolutionCandidateOutcome `json:"outcomes"`
}

// ReadSkillEvolutionCandidateOutcomes returns authoritative Arm C lifecycle
// status for exact advisory candidate revisions. It never infers status from
// usage counts.
func (c *Client) ReadSkillEvolutionCandidateOutcomes(ctx context.Context, request SkillEvolutionCandidateOutcomesReadRequest) (SkillEvolutionCandidateOutcomesReadResponse, error) {
	var response SkillEvolutionCandidateOutcomesReadResponse
	err := c.do(ctx, http.MethodPost, SkillEvolutionCandidateOutcomesReadPath, request, &response)
	return response, err
}
