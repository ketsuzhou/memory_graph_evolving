package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

var (
	ErrNotFound = errors.New("memory store: not found")
	// ErrOutboxNotLeased fails a stage/commit/reschedule whose claim token
	// does not match the row's live lease: an unclaimed worker, a stale
	// pre-restart worker, or another worker's token (R5).
	ErrOutboxNotLeased = errors.New("memory store: outbox entry is not leased to this worker")
	// ErrOutboxAlreadyStaged refuses a re-stage of a row that already holds
	// a durable stage receipt: recovery retries only the idempotent commit.
	ErrOutboxAlreadyStaged = errors.New("memory store: outbox entry already staged; recovery must retry only the commit")
	// ErrOutboxStageReceiptRequired refuses a commit from a row that never
	// staged: commit is only ever the retry of a durable stage.
	ErrOutboxStageReceiptRequired = errors.New("memory store: outbox commit requires a durable stage receipt")
	// ErrOutboxDoubleAdmission refuses a second admission of the same batch:
	// one barrier release admits exactly one next episode.
	ErrOutboxDoubleAdmission = errors.New("memory store: batch already admitted its next episode")
)

// replyKey scopes a tool operation id to the publishing agent: the same
// client_operation_id chosen by two agents names two independent operations.
type replyKey struct {
	Agent domain.AgentID
	Op    string
}

type roomRecord struct {
	room         domain.Room
	messages     []domain.RoomMessage
	deliveries   []domain.Delivery
	mentionKeys  map[string]domain.RoomMessage
	replyKeys    map[replyKey]domain.RoomMessage
	deliveryUniq map[string]domain.DeliveryID
}

// ErrIdempotencyConflict reports a tool operation key reused with different
// content; the original publication stays authoritative.
var ErrIdempotencyConflict = errors.New("memory store: tool operation key reused with different content")

type Store struct {
	mu            sync.Mutex
	rooms         map[domain.RoomID]*roomRecord
	roomOrder     []domain.RoomID
	segments      map[domain.SegmentID]domain.InteractionSegment
	segmentOrder  []domain.SegmentID
	events        map[domain.SegmentID][]ports.SegmentEvent
	links         map[string]domain.SegmentLink
	outbox        map[domain.SegmentID][]domain.EvidenceOutboxEntry
	outboxMeta    map[string]*outboxAttemptState
	outboxEpoch   int64
	outboxClaims  int64
	admittedBatch map[string]bool
	admissionLog  []string
	claimed       map[domain.DeliveryID]bool
	agentInFlight map[domain.RoomID]map[domain.AgentID]domain.DeliveryID
}

// outboxAttemptState tracks per-entry worker progress so one Store can serve
// as both the settlement transaction and the worker-visible outbox queue.
// leaseToken is the live claim owner (empty = unleased); every token is
// prefixed with the store's outbox epoch, which a restore strictly bumps, so
// a token minted in an earlier process lifetime can never validate against a
// row re-claimed after a restart (R5).
type outboxAttemptState struct {
	stageAttempts  int
	commitAttempts int
	inFlight       bool
	leaseToken     string
}

func NewStore() *Store {
	return &Store{
		rooms:         map[domain.RoomID]*roomRecord{},
		segments:      map[domain.SegmentID]domain.InteractionSegment{},
		events:        map[domain.SegmentID][]ports.SegmentEvent{},
		links:         map[string]domain.SegmentLink{},
		outbox:        map[domain.SegmentID][]domain.EvidenceOutboxEntry{},
		outboxMeta:    map[string]*outboxAttemptState{},
		admittedBatch: map[string]bool{},
		claimed:       map[domain.DeliveryID]bool{},
		agentInFlight: map[domain.RoomID]map[domain.AgentID]domain.DeliveryID{},
	}
}

func (s *Store) roomRecord(roomID domain.RoomID) *roomRecord {
	record, ok := s.rooms[roomID]
	if !ok {
		record = &roomRecord{
			room:         domain.Room{ID: roomID},
			mentionKeys:  map[string]domain.RoomMessage{},
			replyKeys:    map[replyKey]domain.RoomMessage{},
			deliveryUniq: map[string]domain.DeliveryID{},
		}
		s.rooms[roomID] = record
		s.roomOrder = append(s.roomOrder, roomID)
	}
	return record
}

func (s *Store) CreateRoom(_ context.Context, room domain.Room, ordinaryAgent, memoryAgent domain.AgentID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rooms[room.ID]; exists {
		return false, nil
	}
	if memoryAgent != "" {
		room.MemoryAgentID = memoryAgent
	}
	record := &roomRecord{
		room:         room,
		mentionKeys:  map[string]domain.RoomMessage{},
		replyKeys:    map[replyKey]domain.RoomMessage{},
		deliveryUniq: map[string]domain.DeliveryID{},
	}
	s.rooms[room.ID] = record
	s.roomOrder = append(s.roomOrder, room.ID)
	_ = ordinaryAgent
	return true, nil
}

func (s *Store) Room(_ context.Context, roomID domain.RoomID) (domain.Room, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.rooms[roomID]
	if !ok {
		return domain.Room{}, ErrNotFound
	}
	return record.room, nil
}

// AppendMessageAndDeliveries publishes one canonical message keyed by the
// human idempotency key and creates at most one delivery per unique
// (agent_id, message_id). Replays return the original message and original
// deliveries with duplicate=true.
func (s *Store) AppendMessageAndDeliveries(
	_ context.Context,
	roomID domain.RoomID,
	idempotencyKey string,
	message domain.RoomMessage,
	deliveries []domain.Delivery,
) (domain.RoomMessage, []domain.Delivery, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.roomRecord(roomID)
	if original, ok := record.mentionKeys[idempotencyKey]; ok {
		return original, record.deliveriesFor(original.ID), true, nil
	}
	message.RoomID = roomID
	message.Sequence = record.room.NextSequence + 1
	if message.ID == "" {
		message.ID = domain.MessageID(defaultMessageID(record, message.Sequence))
	}
	record.room.NextSequence = message.Sequence
	if message.CreatedAt.IsZero() {
		message.CreatedAt = time.Now().UTC()
	}
	record.messages = append(record.messages, message)
	record.mentionKeys[idempotencyKey] = message
	stored := make([]domain.Delivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		key := string(delivery.AgentID) + "\x00" + string(message.ID)
		if _, exists := record.deliveryUniq[key]; exists {
			continue
		}
		if delivery.ID == "" {
			delivery.ID = domain.DeliveryID(defaultDeliveryID(record))
		}
		delivery.RoomID = roomID
		delivery.MessageID = message.ID
		if delivery.State == "" {
			delivery.State = domain.DeliveryPending
		}
		record.deliveryUniq[key] = delivery.ID
		record.deliveries = append(record.deliveries, delivery)
		stored = append(stored, delivery)
	}
	return message, stored, false, nil
}

// PublishToolMessage publishes agent speech keyed by the agent-chosen client
// operation id. Tool messages never create deliveries. The idempotency scope
// is (room, agent, client_operation_id): another agent's identical key never
// collides, and the same key with different content fails closed instead of
// returning the original message.
func (s *Store) PublishToolMessage(
	_ context.Context,
	roomID domain.RoomID,
	agentID domain.AgentID,
	clientOperationID string,
	message domain.RoomMessage,
) (domain.RoomMessage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.roomRecord(roomID)
	key := replyKey{Agent: agentID, Op: clientOperationID}
	if original, ok := record.replyKeys[key]; ok {
		if original.Content != message.Content || !sameReplyTo(original.ReplyToMessageID, message.ReplyToMessageID) {
			return domain.RoomMessage{}, false, fmt.Errorf("%w: room=%s agent=%s key=%s", ErrIdempotencyConflict, roomID, agentID, clientOperationID)
		}
		return original, true, nil
	}
	message.RoomID = roomID
	message.AuthorID = string(agentID)
	message.Sequence = record.room.NextSequence + 1
	if message.ID == "" {
		message.ID = domain.MessageID(defaultMessageID(record, message.Sequence))
	}
	message.IdempotencyKey = clientOperationID
	if message.CreatedAt.IsZero() {
		message.CreatedAt = time.Now().UTC()
	}
	record.room.NextSequence = message.Sequence
	record.messages = append(record.messages, message)
	record.replyKeys[key] = message
	return message, false, nil
}

func sameReplyTo(a, b *domain.MessageID) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// ClaimNext hands out the oldest pending delivery for (room, agent) with
// single-flight semantics: while one delivery for that (room, agent) is in
// flight, the same delivery is returned rather than letting the agent run a
// second concurrent turn.
func (s *Store) ClaimNext(_ context.Context, roomID domain.RoomID, agentID domain.AgentID, now time.Time) (domain.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.rooms[roomID]
	if !ok {
		return domain.Delivery{}, ErrNotFound
	}
	if inFlight, ok := s.agentInFlight[roomID][agentID]; ok {
		if s.claimed[inFlight] {
			for _, delivery := range record.deliveries {
				if delivery.ID == inFlight {
					return delivery, nil
				}
			}
		}
	}
	for _, delivery := range record.deliveries {
		if delivery.AgentID != agentID || delivery.State != domain.DeliveryPending {
			continue
		}
		if s.claimed[delivery.ID] {
			continue
		}
		delivery.Attempts++
		record.updateDelivery(delivery)
		s.claimed[delivery.ID] = true
		if s.agentInFlight[roomID] == nil {
			s.agentInFlight[roomID] = map[domain.AgentID]domain.DeliveryID{}
		}
		s.agentInFlight[roomID][agentID] = delivery.ID
		return delivery, nil
	}
	return domain.Delivery{}, ErrNotFound
}

func (s *Store) AcceptAndOpenSegment(_ context.Context, deliveryID domain.DeliveryID, segment domain.InteractionSegment, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.segments[segment.ID]; exists {
		return nil
	}
	s.acceptDeliveryLocked(deliveryID, now)
	segment.State = domain.SegmentOpen
	if segment.OpenedAt.IsZero() {
		segment.OpenedAt = now
	}
	s.segments[segment.ID] = segment
	s.segmentOrder = append(s.segmentOrder, segment.ID)
	delete(s.claimed, deliveryID)
	s.releaseAgentLeaseLocked(deliveryID)
	return nil
}

func (s *Store) SettleAndCloseSegment(
	_ context.Context,
	deliveryID domain.DeliveryID,
	segmentID domain.SegmentID,
	now time.Time,
	entries []domain.EvidenceOutboxEntry,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	segment, ok := s.segments[segmentID]
	if !ok {
		segment = domain.InteractionSegment{ID: segmentID, DeliveryID: deliveryID}
		s.segments[segmentID] = segment
		s.segmentOrder = append(s.segmentOrder, segmentID)
	}
	segment.State = domain.SegmentSettled
	segment.ClosedAt = &now
	s.segments[segmentID] = segment
	s.settleDeliveryLocked(deliveryID, domain.DeliverySettled, now)
	stored := make([]domain.EvidenceOutboxEntry, 0, len(entries))
	for _, entry := range entries {
		entry.SourceSegmentID = segmentID
		if entry.State == "" {
			entry.State = "pending"
		}
		if entry.NextAttemptAt.IsZero() {
			entry.NextAttemptAt = now
		}
		stored = append(stored, entry)
	}
	s.outbox[segmentID] = stored
	delete(s.claimed, deliveryID)
	s.releaseAgentLeaseLocked(deliveryID)
	return nil
}

func (s *Store) FailAndCloseSegment(_ context.Context, deliveryID domain.DeliveryID, segmentID domain.SegmentID, reason string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	segment, ok := s.segments[segmentID]
	if !ok {
		// A failure before the prompt acknowledgement never opened a segment;
		// the delivery still fails, but no phantom segment is invented.
		s.settleDeliveryLocked(deliveryID, domain.DeliveryFailed, now)
		delete(s.claimed, deliveryID)
		s.releaseAgentLeaseLocked(deliveryID)
		return nil
	}
	segment.State = domain.SegmentFailed
	segment.ClosedAt = &now
	segment.CloseReason = reason
	s.segments[segmentID] = segment
	s.settleDeliveryLocked(deliveryID, domain.DeliveryFailed, now)
	delete(s.claimed, deliveryID)
	s.releaseAgentLeaseLocked(deliveryID)
	return nil
}

func (s *Store) AbortAndCloseSegment(_ context.Context, deliveryID domain.DeliveryID, segmentID domain.SegmentID, reason string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	segment, ok := s.segments[segmentID]
	if !ok {
		// Cancellation before acknowledgement aborts the delivery without
		// inventing a segment that was never opened.
		s.settleDeliveryLocked(deliveryID, domain.DeliveryAborted, now)
		delete(s.claimed, deliveryID)
		s.releaseAgentLeaseLocked(deliveryID)
		return nil
	}
	segment.State = domain.SegmentAborted
	segment.ClosedAt = &now
	segment.CloseReason = reason
	s.segments[segmentID] = segment
	s.settleDeliveryLocked(deliveryID, domain.DeliveryAborted, now)
	delete(s.claimed, deliveryID)
	s.releaseAgentLeaseLocked(deliveryID)
	return nil
}

func (s *Store) RecoverPending(_ context.Context, now time.Time) ([]domain.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending []domain.Delivery
	for _, roomID := range s.roomOrder {
		for _, delivery := range s.rooms[roomID].deliveries {
			if delivery.State == domain.DeliveryPending || delivery.State == domain.DeliveryAccepted {
				pending = append(pending, delivery)
			}
		}
	}
	return pending, nil
}

func (s *Store) AppendEvent(_ context.Context, segmentID domain.SegmentID, event ports.SegmentEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	event.SegmentID = segmentID
	s.events[segmentID] = append(s.events[segmentID], event)
	return nil
}

func (s *Store) PutLink(_ context.Context, link domain.SegmentLink) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.links[link.ID]; exists {
		return false, nil
	}
	s.links[link.ID] = link
	return true, nil
}

func (s *Store) Segment(_ context.Context, segmentID domain.SegmentID) (domain.InteractionSegment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	segment, ok := s.segments[segmentID]
	if !ok {
		return domain.InteractionSegment{}, ErrNotFound
	}
	return segment, nil
}

func (s *Store) OutboxEntriesBySegment(_ context.Context, segmentID domain.SegmentID) ([]domain.EvidenceOutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.EvidenceOutboxEntry(nil), s.outbox[segmentID]...), nil
}

// OutboxEntries enumerates every evidence outbox row the store holds, in
// segment order — the durable settled-evidence ledger the runtime
// reconciles the worker queue against at the drain and restart boundaries
// (R5 round 6): a settled row missing from the queue is a pending handoff
// that must be healed or reported, never silently lost.
func (s *Store) OutboxEntries(_ context.Context) ([]domain.EvidenceOutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var entries []domain.EvidenceOutboxEntry
	for _, segmentID := range s.segmentOrder {
		entries = append(entries, s.outbox[segmentID]...)
	}
	return entries, nil
}

// ReconcileOutbox is the store-backed half of the mandatory handoff contract
// (R5 round 6): settlement and the queue share this store, so every handed
// row must already be durably present — the settlement transaction wrote it
// under the same lock. A missing row is a contract breach and fails closed
// instead of letting a drain green-light over lost evidence.
func (s *Store) ReconcileOutbox(_ context.Context, entries []domain.EvidenceOutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		if _, _, _, ok := s.outboxRowByID(entry.ID); !ok {
			return fmt.Errorf("outbox handoff: settled evidence row %s is not durably enqueued in the store", entry.ID)
		}
	}
	return nil
}

// Messages returns the room's canonical messages in sequence order.
func (s *Store) Messages(_ context.Context, roomID domain.RoomID) ([]domain.RoomMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.rooms[roomID]
	if !ok {
		return nil, nil
	}
	return append([]domain.RoomMessage(nil), record.messages...), nil
}

// RoomDeliveries returns the room's deliveries in creation order.
func (s *Store) RoomDeliveries(_ context.Context, roomID domain.RoomID) ([]domain.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.rooms[roomID]
	if !ok {
		return nil, nil
	}
	return append([]domain.Delivery(nil), record.deliveries...), nil
}

// Segments returns every stored segment in creation order.
func (s *Store) Segments(_ context.Context) ([]domain.InteractionSegment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	segments := make([]domain.InteractionSegment, 0, len(s.segmentOrder))
	for _, id := range s.segmentOrder {
		segments = append(segments, s.segments[id])
	}
	return segments, nil
}

func (record *roomRecord) deliveriesFor(messageID domain.MessageID) []domain.Delivery {
	var deliveries []domain.Delivery
	for _, delivery := range record.deliveries {
		if delivery.MessageID == messageID {
			deliveries = append(deliveries, delivery)
		}
	}
	return deliveries
}

func (record *roomRecord) updateDelivery(updated domain.Delivery) {
	for index, delivery := range record.deliveries {
		if delivery.ID == updated.ID {
			record.deliveries[index] = updated
			return
		}
	}
}

func (s *Store) acceptDeliveryLocked(deliveryID domain.DeliveryID, now time.Time) {
	for _, roomID := range s.roomOrder {
		record := s.rooms[roomID]
		for index, delivery := range record.deliveries {
			if delivery.ID != deliveryID {
				continue
			}
			if delivery.State == domain.DeliveryPending {
				delivery.State = domain.DeliveryAccepted
				delivery.AcceptedAt = &now
				record.deliveries[index] = delivery
			}
			return
		}
	}
}

func (s *Store) settleDeliveryLocked(deliveryID domain.DeliveryID, target domain.DeliveryState, now time.Time) {
	for _, roomID := range s.roomOrder {
		record := s.rooms[roomID]
		for index, delivery := range record.deliveries {
			if delivery.ID != deliveryID {
				continue
			}
			switch target {
			case domain.DeliverySettled:
				delivery.State = domain.DeliverySettled
				delivery.SettledAt = &now
			case domain.DeliveryFailed, domain.DeliveryAborted:
				delivery.State = target
				delivery.TerminalAt = &now
			}
			record.deliveries[index] = delivery
			return
		}
	}
}

func defaultMessageID(record *roomRecord, sequence int64) string {
	return fmt.Sprintf("message-%s-%d", record.room.ID, sequence)
}

func defaultDeliveryID(record *roomRecord) string {
	return fmt.Sprintf("delivery-%s-%d", record.room.ID, len(record.deliveries)+1)
}

// releaseAgentLeaseLocked drops the (room, agent) single-flight lease once a
// delivery leaves the claimed state.
func (s *Store) releaseAgentLeaseLocked(deliveryID domain.DeliveryID) {
	for roomID, byAgent := range s.agentInFlight {
		for agentID, id := range byAgent {
			if id == deliveryID {
				delete(byAgent, agentID)
			}
		}
		if len(byAgent) == 0 {
			delete(s.agentInFlight, roomID)
		}
	}
}

// ---------------------------------------------------------------------------
// Worker-visible outbox surface
//
// The same Store implements ports.OutboxStore over the rows its settlement
// transaction writes, so a settled segment and its two evidence projections
// become durable together — there is no window where a segment is settled but
// the worker queue has not seen its rows.
// ---------------------------------------------------------------------------

func (s *Store) outboxRowByID(entryID string) (domain.SegmentID, int, *domain.EvidenceOutboxEntry, bool) {
	for segmentID, entries := range s.outbox {
		for index := range entries {
			if entries[index].ID == entryID {
				return segmentID, index, &entries[index], true
			}
		}
	}
	return "", -1, nil, false
}

func (s *Store) outboxMetaFor(entryID string) *outboxAttemptState {
	meta, ok := s.outboxMeta[entryID]
	if !ok {
		meta = &outboxAttemptState{}
		s.outboxMeta[entryID] = meta
	}
	return meta
}

// ClaimPending hands one non-committed entry to the worker, respecting the
// scheduled retry time and single-flight per entry, and returns the lease
// claim every later stage/commit/reschedule call must present (R5).
func (s *Store) ClaimPending(_ context.Context, now time.Time) (ports.OutboxClaim, domain.EvidenceOutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, segmentID := range s.segmentOrder {
		entries := s.outbox[segmentID]
		for index := range entries {
			entry := &entries[index]
			if entry.State == "committed" || entry.NextAttemptAt.After(now) {
				continue
			}
			meta := s.outboxMetaFor(entry.ID)
			if meta.inFlight {
				continue
			}
			meta.inFlight = true
			entry.Attempts++
			s.outboxClaims++
			// The epoch prefix fences restarts (R5): the claim counter resets
			// on restore, but the bumped epoch keeps every token distinct
			// across process lifetimes, so a stale pre-restart worker can
			// never collide with a fresh lease.
			meta.leaseToken = fmt.Sprintf("%d:%s#%d", s.outboxEpoch, entry.ID, s.outboxClaims)
			return ports.OutboxClaim{EntryID: entry.ID, Token: meta.leaseToken}, *entry, nil
		}
	}
	return ports.OutboxClaim{}, domain.EvidenceOutboxEntry{}, ports.ErrNoOutboxClaim
}

// leaseOwnerLocked verifies the claim's token against the row's live lease.
func (s *Store) leaseOwnerLocked(entryID string, claim ports.OutboxClaim) error {
	meta, ok := s.outboxMeta[entryID]
	if !ok || meta.leaseToken == "" || meta.leaseToken != claim.Token {
		return fmt.Errorf("%w: entry %s (claim token stale, foreign, or expired)", ErrOutboxNotLeased, entryID)
	}
	return nil
}

func (s *Store) MarkStaged(_ context.Context, claim ports.OutboxClaim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, entry, ok := s.outboxRowByID(claim.EntryID)
	if !ok {
		return ErrNotFound
	}
	if err := s.leaseOwnerLocked(claim.EntryID, claim); err != nil {
		return err
	}
	// committed is terminal for an entry (SC-4.8): a stale worker's stage
	// result must never downgrade durable state.
	if entry.State == "committed" {
		return fmt.Errorf("outbox entry %s is already committed; refusing staged downgrade", claim.EntryID)
	}
	// A durable stage receipt is never re-staged: recovery retries only the
	// idempotent commit (R5).
	if entry.State == "staged" {
		return fmt.Errorf("%w: entry %s", ErrOutboxAlreadyStaged, claim.EntryID)
	}
	meta := s.outboxMetaFor(claim.EntryID)
	meta.stageAttempts++
	entry.State = "staged"
	// The stage lease is RETAINED through the commit (R5 round 4): the
	// worker that staged the row stays its only owner, so a concurrent
	// drain can never commit the wrong row. The lease dies with an explicit
	// release path only: Reschedule (retry), RecoverPendingOutbox
	// (restart), or MarkCommitted (terminal).
	return nil
}

func (s *Store) MarkCommitted(_ context.Context, claim ports.OutboxClaim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, entry, ok := s.outboxRowByID(claim.EntryID)
	if !ok {
		return ErrNotFound
	}
	if err := s.leaseOwnerLocked(claim.EntryID, claim); err != nil {
		return err
	}
	// Commit requires a durable stage receipt (R5): the staging worker
	// commits under its retained lease, and only a post-restart or
	// post-reschedule worker retries the idempotent commit under a fresh
	// claim on the staged row.
	if entry.State != "staged" {
		return fmt.Errorf("%w: entry %s is %s", ErrOutboxStageReceiptRequired, claim.EntryID, entry.State)
	}
	meta := s.outboxMetaFor(claim.EntryID)
	meta.commitAttempts++
	entry.State = "committed"
	meta.inFlight = false
	meta.leaseToken = ""
	return nil
}

// Reschedule records a failed stage/commit attempt. A staged entry keeps its
// stage receipt: recovery must retry only the idempotent commit, not re-stage
// the batch. A committed entry stays terminal.
func (s *Store) Reschedule(_ context.Context, claim ports.OutboxClaim, nextAttempt time.Time, lastError string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, entry, ok := s.outboxRowByID(claim.EntryID)
	if !ok {
		return ErrNotFound
	}
	if err := s.leaseOwnerLocked(claim.EntryID, claim); err != nil {
		return err
	}
	switch entry.State {
	case "staged", "committed":
	default:
		entry.State = "pending"
	}
	entry.NextAttemptAt = nextAttempt
	entry.LastError = lastError
	meta := s.outboxMetaFor(claim.EntryID)
	meta.inFlight = false
	meta.leaseToken = ""
	return nil
}

// ListPending is the non-destructive pending read: every non-committed outbox
// row in segment order, without touching any lease. Recount and barrier
// checks use it so a concurrent drain's live claims survive the read (R5);
// only RecoverPendingOutbox — the startup-recovery boundary — clears leases.
func (s *Store) ListPending(_ context.Context, now time.Time) ([]domain.EvidenceOutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending []domain.EvidenceOutboxEntry
	for _, segmentID := range s.segmentOrder {
		for _, entry := range s.outbox[segmentID] {
			if entry.State != "committed" {
				pending = append(pending, entry)
			}
		}
	}
	return pending, nil
}

// RecoverPendingOutbox is the startup-recovery boundary: it lists every
// non-committed outbox row and clears every live lease, so rows become
// claimable again while every pre-crash claim token dies — a stale worker
// can no longer advance a row it no longer owns (R5). It must only run at
// process startup: normal draining and recounts use ListPending, which never
// clears leases, so two concurrent drains cannot kill each other's claims.
// The separate name exists because DeliveryStore and OutboxStore share the
// RecoverPending method name with different signatures; the Outbox() wrapper
// maps the port onto this method.
func (s *Store) RecoverPendingOutbox(_ context.Context, now time.Time) ([]domain.EvidenceOutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending []domain.EvidenceOutboxEntry
	for _, segmentID := range s.segmentOrder {
		for _, entry := range s.outbox[segmentID] {
			if entry.State != "committed" {
				pending = append(pending, entry)
			}
		}
	}
	for _, meta := range s.outboxMeta {
		meta.inFlight = false
		meta.leaseToken = ""
	}
	return pending, nil
}

// AdmitNextEpisode is the batch-gate admission authority (R5): under the
// same lock the settlement transaction writes outbox rows through, the next
// episode of a batch is admitted only when every evidence projection row is
// committed — a settlement landing between a check and an admission
// serializes ahead of the admission and blocks it. One batch admits at most
// once: the admission is recorded atomically with the check, and a second
// call is a double admission.
func (s *Store) AdmitNextEpisode(_ context.Context, batchID string) (bool, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.admittedBatch[batchID] {
		return false, nil, ErrOutboxDoubleAdmission
	}
	blocking := []string{}
	for _, segmentID := range s.segmentOrder {
		for _, entry := range s.outbox[segmentID] {
			if entry.State != "committed" {
				blocking = append(blocking, entry.ID)
			}
		}
	}
	if len(blocking) > 0 {
		return false, blocking, nil
	}
	s.admittedBatch[batchID] = true
	s.admissionLog = append(s.admissionLog, batchID)
	return true, nil, nil
}

// Admissions lists the batches that admitted a next episode, in admission
// order.
func (s *Store) Admissions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.admissionLog...)
}

// Adopt merges settled outbox entries into the durable queue by ID. When the
// OutboxStore port is wired to this same Store, settlement already wrote the
// rows transactionally and Adopt is an idempotent no-op merge kept for
// adapters wired with a separate queue object.
func (s *Store) Adopt(entries []domain.EvidenceOutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		if _, _, _, ok := s.outboxRowByID(entry.ID); ok {
			continue
		}
		stored := entry
		if stored.State == "" {
			stored.State = "pending"
		}
		s.outbox[stored.SourceSegmentID] = append(s.outbox[stored.SourceSegmentID], stored)
	}
	return nil
}

// Entries returns every outbox row in segment order with its attempt counters.
func (s *Store) Entries() []OutboxRowSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshots := make([]OutboxRowSnapshot, 0)
	for _, segmentID := range s.segmentOrder {
		for _, entry := range s.outbox[segmentID] {
			var stageAttempts, commitAttempts int
			if meta := s.outboxMeta[entry.ID]; meta != nil {
				stageAttempts, commitAttempts = meta.stageAttempts, meta.commitAttempts
			}
			snapshots = append(snapshots, OutboxRowSnapshot{Entry: entry, StageAttempts: stageAttempts, CommitAttempts: commitAttempts})
		}
	}
	return snapshots
}

// StoreOutbox is the ports.OutboxStore view over the same durable Store, so
// settlement writes and worker reads share one mutex and one set of rows.
type StoreOutbox struct {
	store *Store
}

// Outbox returns the worker-visible outbox queue backed by this Store.
func (s *Store) Outbox() *StoreOutbox { return &StoreOutbox{store: s} }

func (o *StoreOutbox) ClaimPending(ctx context.Context, now time.Time) (ports.OutboxClaim, domain.EvidenceOutboxEntry, error) {
	return o.store.ClaimPending(ctx, now)
}

func (o *StoreOutbox) MarkStaged(ctx context.Context, claim ports.OutboxClaim) error {
	return o.store.MarkStaged(ctx, claim)
}

func (o *StoreOutbox) MarkCommitted(ctx context.Context, claim ports.OutboxClaim) error {
	return o.store.MarkCommitted(ctx, claim)
}

func (o *StoreOutbox) Reschedule(ctx context.Context, claim ports.OutboxClaim, nextAttempt time.Time, lastError string) error {
	return o.store.Reschedule(ctx, claim, nextAttempt, lastError)
}

func (o *StoreOutbox) ListPending(ctx context.Context, now time.Time) ([]domain.EvidenceOutboxEntry, error) {
	return o.store.ListPending(ctx, now)
}

func (o *StoreOutbox) RecoverPending(ctx context.Context, now time.Time) ([]domain.EvidenceOutboxEntry, error) {
	return o.store.RecoverPendingOutbox(ctx, now)
}

func (o *StoreOutbox) Reconcile(ctx context.Context, entries []domain.EvidenceOutboxEntry) error {
	return o.store.ReconcileOutbox(ctx, entries)
}

var (
	_ ports.RoomStore     = (*Store)(nil)
	_ ports.DeliveryStore = (*Store)(nil)
	_ ports.DAGStore      = (*Store)(nil)
	_ ports.OutboxStore   = (*StoreOutbox)(nil)
)
