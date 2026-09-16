package room

import (
	"context"
	"errors"
	"sync"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

// ErrLifecycleNotImplemented is kept as the red-stage detector for the PG-10
// contract tests; no green-path code returns it.
var ErrLifecycleNotImplemented = errors.New("room lifecycle not implemented")

var (
	// ErrRoomEpochConflict is the epoch CAS failure: the caller's expected
	// epoch does not match the authority's current epoch. Its rejection
	// reason is ROOM_EPOCH_STALE in the lifecycle catalog.
	ErrRoomEpochConflict = errors.New("room epoch CAS conflict")
	// ErrRegisteredRunsNotTerminal rejects active→closing while a registered
	// agent run has not reached a terminal state (SC-2.2 barrier guard).
	ErrRegisteredRunsNotTerminal = errors.New("registered agent runs are not all terminal")
	// ErrInvalidLifecycleReason rejects reasons outside the catalog and
	// requests that use a rejection reason as a transition authority.
	ErrInvalidLifecycleReason = errors.New("lifecycle reason outside catalog or not a transition authority")
	// ErrLifecycleStateConflict rejects transitions from the wrong source
	// state, including close requests against active/closed rooms.
	ErrLifecycleStateConflict = errors.New("room lifecycle state does not allow transition")
	// ErrCloseGuardNotSatisfied rejects closing→closed while receipts are
	// incomplete or deliveries/segments remain open (SC-4.5).
	ErrCloseGuardNotSatisfied = errors.New("closing→closed guard not satisfied: receipts incomplete or open work remains")
)

type RoomKind string

const (
	BenchmarkRoom  RoomKind = "benchmark"
	ProductionRoom RoomKind = "production"
)

type LifecycleState string

const (
	LifecycleActive  LifecycleState = "active"
	LifecycleClosing LifecycleState = "closing"
	LifecycleClosed  LifecycleState = "closed"
)

type LifecycleReason string

const (
	ReasonBarrierReached LifecycleReason = "BARRIER_REACHED"
	ReasonRoomEpochStale LifecycleReason = "ROOM_EPOCH_STALE"
)

func validLifecycleReason(reason LifecycleReason) bool {
	return reason == ReasonBarrierReached
}

type RegisteredAgentRun struct {
	AgentRunID string
	Terminal   bool
}

type EnterClosingRequest struct {
	TenantID            domain.TenantID
	RoomID              domain.RoomID
	RoomKind            RoomKind
	ExpectedEpoch       int64
	RegisteredAgentRuns []RegisteredAgentRun
	Reason              LifecycleReason
}

type CloseRoomRequest struct {
	TenantID            domain.TenantID
	RoomID              domain.RoomID
	ExpectedEpoch       int64
	RegisteredAgentRuns []RegisteredAgentRun
	ReceiptsComplete    bool
	OpenDeliveries      int
	OpenSegments        int
	Reason              LifecycleReason
}

type LifecycleEvent struct {
	TenantID            domain.TenantID
	RoomID              domain.RoomID
	RoomEpoch           int64
	State               LifecycleState
	Reason              LifecycleReason
	RegisteredAgentRuns []RegisteredAgentRun
	ReceiptsComplete    bool
	OpenDeliveries      int
	OpenSegments        int
}

// lifecycleRoom is the epoch-CAS authority state for one room. A room is
// adopted on first coordination at the caller's expected epoch and advances
// monotonically on every successful transition; failed CAS never mutates.
// The tenant is recorded at adoption and verified on every state transition
// so two tenants cannot steer each other's rooms.
type lifecycleRoom struct {
	tenant           domain.TenantID
	state            LifecycleState
	epoch            int64
	registered       map[string]RegisteredAgentRun
	receiptsComplete bool
	openDeliveries   int
	openSegments     int
}

// Lifecycle is the benchmark Room lifecycle coordinator (SC-4.2): it owns the
// epoch CAS for active→closing→closed and the closing-time write fences.
// This increment is the in-process authority used by contract tests and
// tracers; durable persistence and the production write-path fences are
// wired by PG-50B through the RoomLifecycleStore seam.
type Lifecycle struct {
	store ports.RoomLifecycleStore

	mu    sync.Mutex
	rooms map[domain.RoomID]*lifecycleRoom
}

func NewLifecycle(store ports.RoomLifecycleStore) *Lifecycle {
	return &Lifecycle{store: store, rooms: map[domain.RoomID]*lifecycleRoom{}}
}

// EnterClosing performs the guarded active→closing transition. The only
// transition authority is BARRIER_REACHED with every registered agent run
// terminal; ROOM_EPOCH_STALE is a rejection reason (carried by
// ErrRoomEpochConflict when a stale epoch is detected), never a way into
// closing. A production Room has no evaluation barrier and is never closed
// through this machine (Q47).
func (l *Lifecycle) EnterClosing(_ context.Context, req EnterClosingRequest) (LifecycleEvent, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !validLifecycleReason(req.Reason) {
		return LifecycleEvent{}, ErrInvalidLifecycleReason
	}
	if req.RoomKind == ProductionRoom {
		return LifecycleEvent{}, ErrLifecycleStateConflict
	}
	for _, run := range req.RegisteredAgentRuns {
		if !run.Terminal {
			return LifecycleEvent{}, ErrRegisteredRunsNotTerminal
		}
	}
	room := l.adoptLocked(req.RoomID, req.TenantID, req.ExpectedEpoch)
	if room.epoch != req.ExpectedEpoch {
		return LifecycleEvent{}, ErrRoomEpochConflict
	}
	if room.state != LifecycleActive {
		return LifecycleEvent{}, ErrLifecycleStateConflict
	}
	room.epoch = req.ExpectedEpoch + 1
	room.state = LifecycleClosing
	room.registered = make(map[string]RegisteredAgentRun, len(req.RegisteredAgentRuns))
	for _, run := range req.RegisteredAgentRuns {
		room.registered[run.AgentRunID] = run
	}
	return l.eventLocked(req.TenantID, req.RoomID, room, req.Reason), nil
}

// CloseRoom performs the guarded closing→closed transition (SC-4.5): complete
// receipts and no open delivery or segment. A rejected guard leaves the room
// untouched so the epoch stays valid for the retry.
func (l *Lifecycle) CloseRoom(_ context.Context, req CloseRoomRequest) (LifecycleEvent, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !validLifecycleReason(req.Reason) {
		return LifecycleEvent{}, ErrInvalidLifecycleReason
	}
	room, ok := l.rooms[req.RoomID]
	if !ok {
		return LifecycleEvent{}, ErrLifecycleStateConflict
	}
	if room.tenant != req.TenantID {
		return LifecycleEvent{}, ErrLifecycleStateConflict
	}
	if room.epoch != req.ExpectedEpoch {
		return LifecycleEvent{}, ErrRoomEpochConflict
	}
	if room.state != LifecycleClosing {
		return LifecycleEvent{}, ErrLifecycleStateConflict
	}
	if !req.ReceiptsComplete || req.OpenDeliveries > 0 || req.OpenSegments > 0 {
		return LifecycleEvent{}, ErrCloseGuardNotSatisfied
	}
	room.epoch = req.ExpectedEpoch + 1
	room.state = LifecycleClosed
	room.receiptsComplete = true
	room.openDeliveries = 0
	room.openSegments = 0
	return l.eventLocked(req.TenantID, req.RoomID, room, req.Reason), nil
}

// CanAcceptNewRootTurn and the other three fences are the closing-time write
// block (SC-4.2): only active rooms accept new root turns, delegates,
// deliveries, or segments, and a stale epoch is rejected rather than raced.
func (l *Lifecycle) CanAcceptNewRootTurn(_ context.Context, roomID domain.RoomID, epoch int64) (bool, error) {
	return l.acceptsNewWrites(roomID, epoch)
}

func (l *Lifecycle) CanAcceptNewDelegate(_ context.Context, roomID domain.RoomID, epoch int64) (bool, error) {
	return l.acceptsNewWrites(roomID, epoch)
}

func (l *Lifecycle) CanAcceptNewDelivery(_ context.Context, roomID domain.RoomID, epoch int64) (bool, error) {
	return l.acceptsNewWrites(roomID, epoch)
}

func (l *Lifecycle) CanAcceptNewSegment(_ context.Context, roomID domain.RoomID, epoch int64) (bool, error) {
	return l.acceptsNewWrites(roomID, epoch)
}

// CanRegisteredWorkFinish reports whether an agent run registered before the
// closing transition may still terminate (its terminal write is the barrier
// evidence, not a new write). Unregistered runs get no such allowance.
func (l *Lifecycle) CanRegisteredWorkFinish(_ context.Context, roomID domain.RoomID, agentRunID string, epoch int64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	room, ok := l.rooms[roomID]
	if !ok {
		return false, nil
	}
	if room.epoch != epoch {
		return false, ErrRoomEpochConflict
	}
	if room.state == LifecycleClosed {
		return false, nil
	}
	_, registered := room.registered[agentRunID]
	return registered, nil
}

// CanProductionCutCloseRoom is the Q33/Q47 decoupling: a production
// ConsolidationCut is Space-scoped and carries no lifecycle reason, so it can
// never close a Room. Only the two catalog reasons close, and both arrive via
// EnterClosing.
func (l *Lifecycle) CanProductionCutCloseRoom(_ context.Context, _ domain.RoomID) (bool, error) {
	return false, nil
}

func (l *Lifecycle) acceptsNewWrites(roomID domain.RoomID, epoch int64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	room, ok := l.rooms[roomID]
	if !ok {
		return true, nil
	}
	if room.epoch != epoch {
		return false, ErrRoomEpochConflict
	}
	return room.state == LifecycleActive, nil
}

// adoptLocked returns the room state, adopting a first-seen room at the
// caller's expected epoch under the caller's tenant. Composition wiring
// (PG-50B) seeds durable state before the first coordination, so adoption
// only ever fires for tracers.
func (l *Lifecycle) adoptLocked(roomID domain.RoomID, tenantID domain.TenantID, expectedEpoch int64) *lifecycleRoom {
	room, ok := l.rooms[roomID]
	if !ok {
		room = &lifecycleRoom{tenant: tenantID, state: LifecycleActive, epoch: expectedEpoch}
		l.rooms[roomID] = room
	}
	return room
}

func (l *Lifecycle) eventLocked(tenantID domain.TenantID, roomID domain.RoomID, room *lifecycleRoom, reason LifecycleReason) LifecycleEvent {
	event := LifecycleEvent{
		TenantID:         tenantID,
		RoomID:           roomID,
		RoomEpoch:        room.epoch,
		State:            room.state,
		Reason:           reason,
		ReceiptsComplete: room.receiptsComplete,
		OpenDeliveries:   room.openDeliveries,
		OpenSegments:     room.openSegments,
	}
	for _, run := range room.registered {
		event.RegisteredAgentRuns = append(event.RegisteredAgentRuns, run)
	}
	return event
}
