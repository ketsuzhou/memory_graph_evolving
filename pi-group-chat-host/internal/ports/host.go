package ports

import (
	"context"
	"errors"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
)

// ErrNoOutboxClaim reports that no outbox row is currently claimable: the
// queue is drained or every remaining row is committed or scheduled in the
// future.
var ErrNoOutboxClaim = errors.New("ports: no claimable outbox entry")

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

// OutboxClaim is a worker's lease over one claimed outbox row (R5): the
// token is minted by ClaimPending and every stage/commit/reschedule call
// must present it, so only the claim owner can advance the row — a stale
// worker holding a pre-restart token, or a worker that never claimed, fails
// closed.
type OutboxClaim struct {
	EntryID string
	Token   string
}

type OutboxStore interface {
	ClaimPending(context.Context, time.Time) (OutboxClaim, domain.EvidenceOutboxEntry, error)
	MarkStaged(context.Context, OutboxClaim) error
	MarkCommitted(context.Context, OutboxClaim) error
	Reschedule(context.Context, OutboxClaim, time.Time, string) error
	// ListPending is the non-destructive pending read: it lists every
	// non-committed row without clearing leases, so recount and barrier
	// checks never invalidate a live worker's claim. RecoverPending is the
	// startup-recovery boundary and the only lease-clearing read (R5).
	ListPending(context.Context, time.Time) ([]domain.EvidenceOutboxEntry, error)
	RecoverPending(context.Context, time.Time) ([]domain.EvidenceOutboxEntry, error)
	// Reconcile is the mandatory, durable evidence-handoff contract (R5
	// round 6): the caller hands over the settled evidence rows the
	// durable store has just recorded, and the queue must guarantee, before
	// the call returns, that every one of them is durably enqueued for a
	// worker to drain. Rows the queue is missing are inserted; rows already
	// present keep their current workflow state — a staged or committed
	// row is never downgraded. A queue that shares the durable store with
	// settlement verifies the rows are present (the settlement
	// transaction wrote them under the same lock); a separate durable
	// queue inserts the missing ones. An error means at least one settled
	// row is not enqueued and can be silently lost — callers must fail
	// closed on it, never drain an empty queue and report success. This is
	// a required capability, not an optional adapter hook: a worker queue
	// that cannot take the handoff must refuse it here.
	Reconcile(context.Context, []domain.EvidenceOutboxEntry) error
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
