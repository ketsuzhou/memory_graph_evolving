package ports

import "context"

type GraphMemoryClient interface {
	InitializeTenant(context.Context, InitializeTenantRequest) (InitializeTenantResponse, error)
	RegisterPrincipal(context.Context, RegisterPrincipalRequest) (RegisterPrincipalResponse, error)
	RegisterSpace(context.Context, RegisterSpaceRequest) (RegisterSpaceResponse, error)
	RegisterGrant(context.Context, RegisterGrantRequest) (RegisterGrantResponse, error)
	StageEvidenceBatch(context.Context, StageEvidenceBatchRequest) (StageEvidenceBatchResponse, error)
	CommitEvidenceBatch(context.Context, string, CommitEvidenceBatchRequest) (CommitEvidenceBatchResponse, error)
	EvidenceBatch(context.Context, string) (EvidenceBatchStatusResponse, error)
	Recall(context.Context, RecallRequest) (RecallResponse, error)
	StartExploration(context.Context, StartExplorationRequest) (StartExplorationResponse, error)
	Explore(context.Context, string, ExploreRequest) (ExploreResponse, error)
	Redirect(context.Context, string, RedirectRequest) (RedirectResponse, error)
	Submit(context.Context, string, SubmitRequest) (SubmitResponse, error)
}

type InitializeTenantRequest struct {
	TenantID             string `json:"tenant_id"`
	DisplayName          string `json:"display_name"`
	BootstrapPrincipalID string `json:"bootstrap_principal_id"`
}

type InitializeTenantResponse struct {
	TenantID             string `json:"tenant_id"`
	BootstrapPrincipalID string `json:"bootstrap_principal_id"`
	Duplicate            bool   `json:"duplicate"`
}

type RegisterPrincipalRequest struct {
	PrincipalID string `json:"principal_id"`
	Kind        string `json:"kind"`
	DisplayName string `json:"display_name"`
}

type RegisterPrincipalResponse struct {
	PrincipalID string `json:"principal_id"`
	Kind        string `json:"kind"`
	Duplicate   bool   `json:"duplicate"`
}

type RegisterSpaceRequest struct {
	SpaceID          string  `json:"space_id"`
	Scope            string  `json:"scope"`
	OwnerPrincipalID *string `json:"owner_principal_id"`
	DisplayName      string  `json:"display_name"`
}

type RegisterSpaceResponse struct {
	SpaceID          string  `json:"space_id"`
	Scope            string  `json:"scope"`
	OwnerPrincipalID *string `json:"owner_principal_id"`
	Duplicate        bool    `json:"duplicate"`
}

type RegisterGrantRequest struct {
	GrantID     string   `json:"grant_id"`
	PrincipalID string   `json:"principal_id"`
	SpaceIDs    []string `json:"space_ids"`
	Purpose     string   `json:"purpose"`
	Operations  []string `json:"operations"`
	ExpiresAt   string   `json:"expires_at"`
}

type RegisterGrantResponse struct {
	GrantID     string   `json:"grant_id"`
	PrincipalID string   `json:"principal_id"`
	SpaceIDs    []string `json:"space_ids"`
	Purpose     string   `json:"purpose"`
	Operations  []string `json:"operations"`
	ExpiresAt   string   `json:"expires_at"`
	Duplicate   bool     `json:"duplicate"`
}

type EvidenceProvenance struct {
	HostType       string `json:"host_type"`
	HostInstanceID string `json:"host_instance_id"`
	SourceKind     string `json:"source_kind"`
	CapturedAt     string `json:"captured_at"`
	ContentSHA256  string `json:"content_sha256"`
}

type EvidenceEvent struct {
	EventID    string `json:"event_id"`
	Sequence   int64  `json:"sequence"`
	Kind       string `json:"kind"`
	Content    string `json:"content"`
	OccurredAt string `json:"occurred_at"`
}

type EvidenceLink struct {
	LinkID      string `json:"link_id"`
	FromEventID string `json:"from_event_id"`
	ToEventID   string `json:"to_event_id"`
	Relation    string `json:"relation"`
}

type StageEvidenceBatchRequest struct {
	BatchID         string             `json:"batch_id"`
	IdempotencyKey  string             `json:"idempotency_key"`
	SpaceID         string             `json:"space_id"`
	StreamID        string             `json:"stream_id"`
	SourceSegmentID string             `json:"source_segment_id"`
	Provenance      EvidenceProvenance `json:"provenance"`
	Events          []EvidenceEvent    `json:"events"`
	Links           []EvidenceLink     `json:"links"`
	TerminalOutcome string             `json:"terminal_outcome"`
}

type StageEvidenceBatchResponse struct {
	BatchID        string `json:"batch_id"`
	IdempotencyKey string `json:"idempotency_key"`
	SpaceID        string `json:"space_id"`
	State          string `json:"state"`
	Duplicate      bool   `json:"duplicate"`
}

type CommitEvidenceBatchRequest struct {
	CommitID string `json:"commit_id"`
}

type CommitEvidenceBatchResponse struct {
	BatchID       string `json:"batch_id"`
	CommitID      string `json:"commit_id"`
	State         string `json:"state"`
	MemoryVersion int64  `json:"memory_version"`
	CommittedAt   string `json:"committed_at"`
	Duplicate     bool   `json:"duplicate"`
}

type EvidenceBatchStatusResponse struct {
	BatchID         string  `json:"batch_id"`
	IdempotencyKey  string  `json:"idempotency_key"`
	SpaceID         string  `json:"space_id"`
	SourceSegmentID string  `json:"source_segment_id"`
	State           string  `json:"state"`
	MemoryVersion   *int64  `json:"memory_version"`
	CommittedAt     *string `json:"committed_at"`
}

type RecallRequest struct {
	RequestID string   `json:"request_id"`
	Query     string   `json:"query"`
	SpaceIDs  []string `json:"space_ids"`
	// SpaceVersions optionally pins individual spaces to an earlier version
	// (a consolidation-cut manifest's frozen projection head) instead of the
	// live head. The service filters each pinned space's retrieval to exactly
	// the evidence at or below the pinned version; a pin ahead of the head is
	// a 422, never a silent widening.
	SpaceVersions map[string]int64 `json:"space_versions,omitempty"`
	MaxResults    int              `json:"max_results"`
	DeadlineMS    int              `json:"deadline_ms"`
}

type Citation struct {
	CitationID      string   `json:"citation_id"`
	EvidenceBatchID string   `json:"evidence_batch_id"`
	EventIDs        []string `json:"event_ids"`
}

type RecallItem struct {
	Content       string   `json:"content"`
	SourceSpaceID string   `json:"source_space_id"`
	MemoryVersion int64    `json:"memory_version"`
	Citation      Citation `json:"citation"`
	Score         float64  `json:"score"`
}

type RecallDegradation struct {
	State   string   `json:"state"`
	Reasons []string `json:"reasons"`
}

type RecallResponse struct {
	RequestID   string            `json:"request_id"`
	Items       []RecallItem      `json:"items"`
	Degradation RecallDegradation `json:"degradation"`
}

type StartExplorationRequest struct {
	RequestID      string   `json:"request_id"`
	IdempotencyKey string   `json:"idempotency_key"`
	SpaceIDs       []string `json:"space_ids"`
	Query          string   `json:"query"`
	MaxSteps       int      `json:"max_steps"`
	MaxResults     int      `json:"max_results"`
}

type PinnedSpace struct {
	SpaceID       string `json:"space_id"`
	MemoryVersion int64  `json:"memory_version"`
}

type StartExplorationResponse struct {
	SessionID      string        `json:"session_id"`
	RequestID      string        `json:"request_id"`
	State          string        `json:"state"`
	PinnedSpaces   []PinnedSpace `json:"pinned_spaces"`
	Items          []RecallItem  `json:"items"`
	RemainingSteps int           `json:"remaining_steps"`
	Duplicate      bool          `json:"duplicate"`
}

type ExploreRequest struct {
	OperationID      string `json:"operation_id"`
	AnchorCitationID string `json:"anchor_citation_id"`
	Relation         string `json:"relation"`
	Limit            int    `json:"limit"`
}

type ExploreResponse struct {
	SessionID      string       `json:"session_id"`
	OperationID    string       `json:"operation_id"`
	State          string       `json:"state"`
	Items          []RecallItem `json:"items"`
	RemainingSteps int          `json:"remaining_steps"`
	Duplicate      bool         `json:"duplicate"`
}

type RedirectRequest struct {
	OperationID       string   `json:"operation_id"`
	Query             string   `json:"query"`
	AnchorCitationIDs []string `json:"anchor_citation_ids"`
	Reason            string   `json:"reason"`
}

type RedirectResponse = ExploreResponse

type SubmitRequest struct {
	OperationID string   `json:"operation_id"`
	Found       bool     `json:"found"`
	Summary     string   `json:"summary"`
	CitationIDs []string `json:"citation_ids"`
}

type SubmittedCitation struct {
	CitationID      string `json:"citation_id"`
	SourceSpaceID   string `json:"source_space_id"`
	MemoryVersion   int64  `json:"memory_version"`
	EvidenceBatchID string `json:"evidence_batch_id"`
}

type SubmitResponse struct {
	SessionID   string              `json:"session_id"`
	OperationID string              `json:"operation_id"`
	State       string              `json:"state"`
	Found       bool                `json:"found"`
	Summary     string              `json:"summary"`
	Citations   []SubmittedCitation `json:"citations"`
	Duplicate   bool                `json:"duplicate"`
}
