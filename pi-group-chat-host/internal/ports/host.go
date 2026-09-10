package ports

import (
	"context"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
)

type Clock interface {
	Now() time.Time
}

type IDSource interface {
	NewID(kind string) string
}

type RoomStore interface {
	CreateRoom(context.Context, domain.Room, domain.AgentID, domain.AgentID) (created bool, err error)
	Room(context.Context, domain.RoomID) (domain.Room, error)
	AppendMessageAndDeliveries(context.Context, domain.RoomID, string, domain.RoomMessage, []domain.Delivery) (domain.RoomMessage, []domain.Delivery, bool, error)
	PublishToolMessage(context.Context, domain.RoomID, domain.AgentID, string, domain.RoomMessage) (domain.RoomMessage, bool, error)
}

type DeliveryStore interface {
	ClaimNext(context.Context, domain.RoomID, domain.AgentID, time.Time) (domain.Delivery, error)
	AcceptAndOpenSegment(context.Context, domain.DeliveryID, domain.InteractionSegment, time.Time) error
	SettleAndCloseSegment(context.Context, domain.DeliveryID, domain.SegmentID, time.Time, []domain.EvidenceOutboxEntry) error
	FailAndCloseSegment(context.Context, domain.DeliveryID, domain.SegmentID, string, time.Time) error
	AbortAndCloseSegment(context.Context, domain.DeliveryID, domain.SegmentID, string, time.Time) error
	RecoverPending(context.Context, time.Time) ([]domain.Delivery, error)
}

type SegmentEvent struct {
	ID         string
	SegmentID  domain.SegmentID
	Sequence   int64
	Kind       string
	Content    string
	OccurredAt time.Time
}

type DAGStore interface {
	AppendEvent(context.Context, domain.SegmentID, SegmentEvent) error
	PutLink(context.Context, domain.SegmentLink) (created bool, err error)
	Segment(context.Context, domain.SegmentID) (domain.InteractionSegment, error)
}

type OutboxStore interface {
	ClaimPending(context.Context, time.Time) (domain.EvidenceOutboxEntry, error)
	MarkStaged(context.Context, string) error
	MarkCommitted(context.Context, string) error
	Reschedule(context.Context, string, time.Time, string) error
	RecoverPending(context.Context, time.Time) ([]domain.EvidenceOutboxEntry, error)
}

type PiEvent struct {
	Type       string
	ToolCallID string
	ToolName   string
	Content    string
	WillRetry  *bool
	// Result and IsError are carried by tool_execution_end frames so the
	// Host can gate every side effect on a successful completion.
	Result  string
	IsError *bool
}

type PromptAcceptance struct {
	ID         string
	Success    bool
	AcceptedAt time.Time
}

type PiProcess interface {
	Prompt(context.Context, string, func(PiEvent) error) (PromptAcceptance, error)
	Close() error
}

type PiLauncher interface {
	VerifyExactVersion(context.Context, string) error
	Start(context.Context, domain.AgentProfile) (PiProcess, error)
}
