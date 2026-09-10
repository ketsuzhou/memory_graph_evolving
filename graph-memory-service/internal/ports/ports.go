package ports

import (
	"context"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

type Clock interface {
	Now() time.Time
}

type RegistryStore interface {
	InitializeTenant(context.Context, domain.Tenant, domain.Principal) (created bool, err error)
	PutPrincipal(context.Context, domain.TenantID, domain.Principal) (created bool, err error)
	PutSpace(context.Context, domain.TenantID, domain.Space) (created bool, err error)
	PutGrant(context.Context, domain.TenantID, domain.Grant) (created bool, err error)
	Space(context.Context, domain.TenantID, domain.SpaceID) (domain.Space, error)
	Principal(context.Context, domain.TenantID, domain.PrincipalID) (domain.Principal, error)
	Grants(context.Context, domain.TenantID, domain.PrincipalID) ([]domain.Grant, error)
	ActiveGrants(context.Context, domain.TenantID, domain.PrincipalID, time.Time) ([]domain.Grant, error)
}

type EvidenceStore interface {
	Stage(context.Context, domain.EvidenceBatch) (batch domain.EvidenceBatch, duplicate bool, err error)
	Commit(context.Context, domain.TenantID, domain.BatchID, string, time.Time) (batch domain.EvidenceBatch, duplicate bool, err error)
	Batch(context.Context, domain.TenantID, domain.BatchID) (domain.EvidenceBatch, error)
}

type RetrievalChannel string

const (
	RetrievalBM25      RetrievalChannel = "bm25"
	RetrievalEmbedding RetrievalChannel = "embedding"
)

// RecallOutcome separates successful bounded items from a non-fatal channel
// degradation. Fatal scope/store/provider failures remain errors. Applied is
// the exact closed set of channels that contributed to the returned scores.
type RecallOutcome struct {
	Items       []domain.RecallItem
	Applied     []RetrievalChannel
	Degradation *domain.RecallDegradation
}

type RecallStore interface {
	Retrieve(context.Context, domain.TenantID, []domain.PinnedSpace, string, int) (RecallOutcome, error)
}

// QueryActivityStore records only completed Recall queries against the exact
// immutable space pins that were authorized and searched.
type QueryActivityStore interface {
	RecordSuccessfulRecall(context.Context, domain.TenantID, []domain.PinnedSpace) error
}

// EvidenceDocument is an immutable copy of one committed evidence event. Store
// adapters return these copies in publication order so retrieval never holds an
// authoritative-store lock while tokenizing or calling an external provider.
type EvidenceDocument struct {
	SpaceID       domain.SpaceID
	MemoryVersion int64
	BatchID       domain.BatchID
	EventID       string
	Content       string
}

type EvidenceDocumentStore interface {
	CommittedEvidenceDocuments(context.Context, domain.TenantID, []domain.PinnedSpace) ([]EvidenceDocument, error)
}

// EmbeddingProvider is the provider seam used by retrieval. CacheKey identifies
// the endpoint/model policy but must never contain credentials.
type EmbeddingProvider interface {
	Embed(context.Context, []string) ([][]float64, error)
	CacheKey() string
}

type EmbeddingNeighbor struct {
	NodeID domain.ProjectionNodeID
	Cosine float64
}

// EmbeddingNeighborFinder ranks nodes inside one exact immutable projection.
// Excluded node IDs are never returned.
type EmbeddingNeighborFinder interface {
	EmbeddingNeighbors(context.Context, domain.TenantID, domain.PinnedSpace, domain.DerivedProjection, []domain.ProjectionNodeID, map[domain.ProjectionNodeID]struct{}, int) ([]EmbeddingNeighbor, error)
}

type ExplorationStore interface {
	Start(context.Context, domain.ExplorationSession, string) (session domain.ExplorationSession, duplicate bool, err error)
	Session(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.ExplorationSession, error)
	ServedItem(context.Context, domain.ExplorationSessionID, string) (domain.RecallItem, error)
	Replay(context.Context, domain.ExplorationSessionID, string, domain.ExplorationOperation) (storedResponse []byte, found bool, err error)
	Apply(context.Context, domain.ExplorationSessionID, string, domain.ExplorationOperation) (storedResponse []byte, duplicate bool, err error)
	RecordServed(context.Context, domain.ExplorationSessionID, []domain.RecallItem) error
}

// IntentKind is the closed set of navigation decisions a selector may make.
// The selector can only refer to action and citation IDs present in its input;
// services remain authoritative for authorization, traversal and submission.
type IntentKind string

const (
	IntentExplore  IntentKind = "explore"
	IntentRedirect IntentKind = "redirect"
	IntentSubmit   IntentKind = "submit"
)

// Intent contains no free-form result text. External model output is a
// non-authoritative choice among server-generated actions and served citations.
type Intent struct {
	Kind        IntentKind `json:"intent"`
	ActionID    string     `json:"action_id"`
	CitationIDs []string   `json:"citation_ids"`
}

type NavigationTraversalCandidate struct {
	RouteKind string `json:"route_kind"`
	Relation  string `json:"relation"`
	Direction string `json:"direction"`
}

type NavigationCandidate struct {
	CitationID string                        `json:"citation_id"`
	Content    string                        `json:"content"`
	Traversal  *NavigationTraversalCandidate `json:"traversal,omitempty"`
}

type NavigationAction struct {
	ActionID   string     `json:"action_id"`
	Intent     IntentKind `json:"intent"`
	CitationID string     `json:"citation_id,omitempty"`
	Relation   string     `json:"relation,omitempty"`
	Limit      int        `json:"limit,omitempty"`
	Found      bool       `json:"found,omitempty"`
}

// NavigationObservation is the complete and deliberately bounded model view.
// It excludes identity, grants, Spaces, versions, provider secrets and raw DB
// or projection node identifiers.
type NavigationObservation struct {
	Query            string                `json:"query"`
	RemainingSteps   int                   `json:"remaining_steps"`
	RemainingResults int                   `json:"remaining_results"`
	Candidates       []NavigationCandidate `json:"candidates"`
	Actions          []NavigationAction    `json:"actions"`
}

type ModelUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type ModelPolicy struct {
	Provider     string `json:"provider"`
	Model        string `json:"model"`
	PromptPolicy string `json:"prompt_policy"`
}

type PathSelection struct {
	Intent Intent      `json:"intent"`
	Usage  ModelUsage  `json:"usage"`
	Policy ModelPolicy `json:"policy"`
}

type PathSelector interface {
	Select(context.Context, NavigationObservation) (PathSelection, error)
}

// NavigationStepJournal persists a selector decision before its deterministic
// exploration suboperation runs. ResponseJSON is filled after that operation,
// allowing a crash retry to replay the same semantic request and operation ID.
type NavigationStepJournal struct {
	Index          int              `json:"index"`
	Intent         Intent           `json:"intent"`
	Action         NavigationAction `json:"action"`
	OperationID    string           `json:"operation_id"`
	ModelCall      bool             `json:"model_call"`
	Usage          ModelUsage       `json:"usage"`
	ModelPolicy    ModelPolicy      `json:"model_policy"`
	AppliedPolicy  string           `json:"applied_policy"`
	Degradation    string           `json:"degradation,omitempty"`
	RedirectQuery  string           `json:"redirect_query,omitempty"`
	RedirectReason string           `json:"redirect_reason,omitempty"`
	SubmitSummary  string           `json:"submit_summary,omitempty"`
	ResponseJSON   []byte           `json:"response_json,omitempty"`
}

type NavigationRunJournal struct {
	SessionID     domain.ExplorationSessionID `json:"session_id"`
	RunID         string                      `json:"run_id"`
	MaxModelCalls int                         `json:"max_model_calls"`
	StartedAt     time.Time                   `json:"started_at"`
	Deadline      time.Time                   `json:"deadline"`
	State         string                      `json:"state"`
	Steps         []NavigationStepJournal     `json:"steps"`
	ResultJSON    []byte                      `json:"result_json,omitempty"`
}

// NavigationStore is the minimal durable port needed to resume automatic
// navigation without re-sampling a persisted logical step.
type NavigationStore interface {
	StartNavigationRun(context.Context, domain.ExplorationSessionID, string, int, time.Time, time.Time) (NavigationRunJournal, bool, error)
	NavigationRun(context.Context, domain.ExplorationSessionID, string) (NavigationRunJournal, bool, error)
	RecordNavigationIntent(context.Context, domain.ExplorationSessionID, string, NavigationStepJournal) (NavigationStepJournal, bool, error)
	CompleteNavigationStep(context.Context, domain.ExplorationSessionID, string, int, string, []byte) error
	CompleteNavigationRun(context.Context, domain.ExplorationSessionID, string, []byte) error
	ServedNavigationItems(context.Context, domain.ExplorationSessionID, int) ([]domain.RecallItem, error)
}
