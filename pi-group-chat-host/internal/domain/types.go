package domain

import "time"

type TenantID string
type RoomID string
type AgentID string
type MessageID string
type DeliveryID string
type SegmentID string
type SpaceID string

type Room struct {
	ID            RoomID
	TenantID      TenantID
	SharedSpaceID SpaceID
	NextSequence  int64
	MemoryAgentID AgentID
}

type RoomMessage struct {
	ID               MessageID
	RoomID           RoomID
	Sequence         int64
	AuthorID         string
	Content          string
	ReplyToMessageID *MessageID
	CreatedAt        time.Time
	IdempotencyKey   string
}

type DeliveryState string

const (
	DeliveryPending  DeliveryState = "pending"
	DeliveryAccepted DeliveryState = "accepted"
	DeliverySettled  DeliveryState = "settled"
	DeliveryFailed   DeliveryState = "failed"
	DeliveryAborted  DeliveryState = "aborted"
)

type Delivery struct {
	ID          DeliveryID
	RoomID      RoomID
	AgentID     AgentID
	MessageID   MessageID
	State       DeliveryState
	Attempts    int
	AcceptedAt  *time.Time
	SettledAt   *time.Time
	TerminalAt  *time.Time
	FailureCode string
}

type SegmentState string

const (
	SegmentOpen    SegmentState = "open"
	SegmentSettled SegmentState = "settled"
	SegmentFailed  SegmentState = "failed"
	SegmentAborted SegmentState = "aborted"
)

type InteractionSegment struct {
	ID          SegmentID
	TenantID    TenantID
	RoomID      RoomID
	AgentID     AgentID
	DeliveryID  DeliveryID
	State       SegmentState
	OpenedAt    time.Time
	ClosedAt    *time.Time
	CloseReason string
}

type SegmentLinkKind string

const (
	LinkMentions    SegmentLinkKind = "mentions"
	LinkRespondsTo  SegmentLinkKind = "responds_to"
	LinkContinues   SegmentLinkKind = "continues"
	LinkDelegatesTo SegmentLinkKind = "delegates_to"
)

type SegmentLink struct {
	ID            string
	RoomID        RoomID
	FromSegmentID SegmentID
	ToSegmentID   SegmentID
	Kind          SegmentLinkKind
	CreatedAt     time.Time
}

type EvidenceProjection string

const (
	ProjectionRoomShared   EvidenceProjection = "room_shared"
	ProjectionAgentPrivate EvidenceProjection = "agent_private"
)

type EvidenceOutboxEntry struct {
	ID              string
	SourceSegmentID SegmentID
	Projection      EvidenceProjection
	SpaceID         SpaceID
	BatchID         string
	State           string
	Attempts        int
	NextAttemptAt   time.Time
	LastError       string
}

type AgentProfileKind string

const (
	ProfileOrdinary AgentProfileKind = "ordinary"
	ProfileMemory   AgentProfileKind = "memory"
)

type AgentProfile struct {
	Kind                 AgentProfileKind
	BuiltinToolsEnabled  bool
	AllowedToolNames     []string
	WorkingDirectory     string
	EnvironmentAllowlist []string
}
