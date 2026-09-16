package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"
)

type TenantID string
type SpaceID string
type PrincipalID string
type GrantID string
type BatchID string
type ExplorationSessionID string

type Tenant struct {
	ID                   TenantID
	DisplayName          string
	BootstrapPrincipalID PrincipalID
}

type SpaceScope string

const (
	SpaceShared  SpaceScope = "shared"
	SpacePrivate SpaceScope = "private"
)

type Space struct {
	ID               SpaceID
	TenantID         TenantID
	Scope            SpaceScope
	OwnerPrincipalID *PrincipalID
	DisplayName      string
	Version          int64
}

type PrincipalKind string

const (
	PrincipalHuman   PrincipalKind = "human"
	PrincipalAgent   PrincipalKind = "agent"
	PrincipalService PrincipalKind = "service"
)

type Principal struct {
	ID          PrincipalID
	TenantID    TenantID
	Kind        PrincipalKind
	DisplayName string
}

type GrantPurpose string
type GrantOperation string

const (
	GrantPurposeLifecycle GrantPurpose = "lifecycle"
	GrantPurposeToolPlane GrantPurpose = "tool_plane"

	GrantOperationEvidenceStage       GrantOperation = "evidence.stage"
	GrantOperationEvidenceCommit      GrantOperation = "evidence.commit"
	GrantOperationRecall              GrantOperation = "recall"
	GrantOperationExplorationStart    GrantOperation = "exploration.start"
	GrantOperationExplorationExplore  GrantOperation = "exploration.explore"
	GrantOperationExplorationRedirect GrantOperation = "exploration.redirect"
	GrantOperationExplorationSubmit   GrantOperation = "exploration.submit"
)

type Grant struct {
	ID          GrantID
	TenantID    TenantID
	PrincipalID PrincipalID
	SpaceIDs    []SpaceID
	Purpose     GrantPurpose
	Operations  []GrantOperation
	ExpiresAt   time.Time
}

// ActiveAt reports whether the grant is still valid at now. Expiry is exact:
// the grant is active only strictly before ExpiresAt, with no grace interval.
func (g Grant) ActiveAt(now time.Time) bool {
	return now.Before(g.ExpiresAt)
}

// ProtocolError is the stable failure metadata every use case returns so the
// HTTP adapter can map failures without parsing error strings.
type ProtocolError struct {
	Status  int
	ErrCode string
	Message string
}

func (e *ProtocolError) Error() string   { return e.Message }
func (e *ProtocolError) StatusCode() int { return e.Status }
func (e *ProtocolError) Code() string    { return e.ErrCode }

func NewProtocolError(status int, code, message string) *ProtocolError {
	return &ProtocolError{Status: status, ErrCode: code, Message: message}
}

type Provenance struct {
	HostType       string
	HostInstanceID string
	SourceKind     string
	CapturedAt     time.Time
	ContentSHA256  string
}

type EvidenceEvent struct {
	ID         string
	Sequence   int64
	Kind       string
	Content    string
	OccurredAt time.Time
}

type EvidenceLink struct {
	ID          string
	FromEventID string
	ToEventID   string
	Relation    string
}

type EvidenceBatchState string

const (
	EvidenceBatchStaged    EvidenceBatchState = "staged"
	EvidenceBatchCommitted EvidenceBatchState = "committed"
)

type EvidenceBatch struct {
	ID              BatchID
	TenantID        TenantID
	IdempotencyKey  string
	SpaceID         SpaceID
	StreamID        string
	SourceSegmentID string
	Provenance      Provenance
	Events          []EvidenceEvent
	Links           []EvidenceLink
	TerminalOutcome string
	State           EvidenceBatchState
	MemoryVersion   *int64
	CommittedAt     *time.Time
}

type Citation struct {
	ID              string
	EvidenceBatchID BatchID
	EventIDs        []string
}

// StableCitationID hashes an unambiguous, length-prefixed identity tuple.
// The complete SHA-256 digest keeps IDs fixed-size and safely below the
// protocol's OpaqueID limit even when source IDs are at their maximum length.
func StableCitationID(prefix string, identityParts ...string) string {
	hasher := sha256.New()
	var length [8]byte
	for _, part := range identityParts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(part))
	}
	return prefix + hex.EncodeToString(hasher.Sum(nil))
}

type RecallRequest struct {
	RequestID  string
	Query      string
	SpaceIDs   []SpaceID
	// SpaceVersions optionally pins individual spaces to an earlier version
	// instead of their current head — the frozen-head versions a
	// consolidation-cut manifest publishes per space. A pinned space's read
	// resolves to exactly the evidence committed at or below that version
	// (the snapshot's evidence watermark), so a later commit can never leak
	// into a pinned read. Keys must be a subset of SpaceIDs, values must sit
	// in [0, current head]; a pin ahead of the head is a 422, never a silent
	// clamp to the live head.
	SpaceVersions map[SpaceID]int64
	MaxResults    int
	DeadlineMS    int
}

type TraversalMetadata struct {
	ProjectionNodeID  ProjectionNodeID
	ProjectionVersion ProjectionVersion
	ProjectionDigest  string
	RouteKind         string
	EdgeID            ProjectionEdgeID
	EdgeKind          EdgeKind
	Relation          string
	Direction         string
}

type RecallItem struct {
	Content       string
	SourceSpaceID SpaceID
	MemoryVersion int64
	Citation      Citation
	Score         float64
	Traversal     *TraversalMetadata
}

type RecallDegradation struct {
	State   string
	Reasons []string
}

type RecallResult struct {
	RequestID   string
	Items       []RecallItem
	Degradation RecallDegradation
}

type ExplorationBudget struct {
	MaxSteps   int
	MaxResults int
}

type PinnedSpace struct {
	SpaceID                     SpaceID
	MemoryVersion               int64
	ProjectionVersion           ProjectionVersion
	ProjectionDigest            string
	ProjectionEvidenceWatermark int64
}

type ExplorationSession struct {
	ID           ExplorationSessionID
	TenantID     TenantID
	PrincipalID  PrincipalID
	SpaceIDs     []SpaceID
	PinnedSpaces []PinnedSpace
	RequestID    string
	Query        string
	Budget       ExplorationBudget
	StepsUsed    int
	// ResultsServed counts items served against the session result budget:
	// the start query counts once (distinct citations), every step's returned
	// items count as occurrences. ADR 0040 freezes this conservative rule.
	ResultsServed int
	State         string
	ExpiresAt     time.Time
}

// ExplorationOperation is the atomic step contract exploration requests
// implement for the exploration store: a stable semantic encoding for
// idempotency comparison and a mutation executed under the store's critical
// section against a session clone.
type ExplorationOperation interface {
	SemanticJSON() ([]byte, error)
	Mutate(session ExplorationSession, servedItem func(string) (RecallItem, bool)) (ExplorationSession, any, error)
}

// ServedCitations is implemented by exploration step responses so the store
// can record exactly which recall items the session served.
type ServedCitations interface {
	ServedCitations() []RecallItem
}
