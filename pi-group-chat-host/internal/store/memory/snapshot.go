package memory

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

// snapshot is the full durable image of the Host store: rooms with their
// canonical transcripts and delivery rows, interaction segments, DAG events
// and links, and the evidence outbox with worker attempt counters, the outbox
// lease epoch, and the batch-gate admission decisions. Claims and
// single-flight leases are deliberately excluded — a restart releases them so
// pending deliveries become claimable again, which is the at-least-once
// contract — but the persisted epoch is bumped on restore, so a lease token
// minted in any earlier process lifetime can never validate against a row
// re-claimed after the restart (R5). Admission decisions are durable: a batch
// that admitted a next episode before the restart is still admitted. Domain
// types marshal by field name (symmetric round-trip); the JSON is only ever
// consumed by the same binary.
type snapshot struct {
	RoomOrder     []domain.RoomID                         `json:"room_order"`
	Rooms         []roomSnapshot                          `json:"rooms"`
	SegmentOrder  []domain.SegmentID                      `json:"segment_order"`
	Segments      map[string]domain.InteractionSegment    `json:"segments"`
	Events        map[string][]ports.SegmentEvent         `json:"events"`
	Links         map[string]domain.SegmentLink           `json:"links"`
	Outbox        map[string][]domain.EvidenceOutboxEntry `json:"outbox"`
	OutboxMeta    map[string]outboxAttemptSnapshot        `json:"outbox_meta"`
	OutboxEpoch   int64                                   `json:"outbox_epoch"`
	AdmittedBatch []string                                `json:"admitted_batch"`
	AdmissionLog  []string                                `json:"admission_log"`
}

type roomSnapshot struct {
	Room         domain.Room                   `json:"room"`
	Messages     []domain.RoomMessage          `json:"messages"`
	Deliveries   []domain.Delivery             `json:"deliveries"`
	MentionKeys  map[string]domain.RoomMessage `json:"mention_keys"`
	ReplyKeys    []replyKeySnapshot            `json:"reply_keys"`
	DeliveryUniq map[string]domain.DeliveryID  `json:"delivery_uniq"`
}

type replyKeySnapshot struct {
	Agent   domain.AgentID     `json:"agent"`
	Op      string             `json:"op"`
	Message domain.RoomMessage `json:"message"`
}

type outboxAttemptSnapshot struct {
	StageAttempts  int `json:"stage_attempts"`
	CommitAttempts int `json:"commit_attempts"`
}

// Snapshot returns the JSON image of the whole store under one lock.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := &snapshot{
		SegmentOrder: append([]domain.SegmentID(nil), s.segmentOrder...),
		Segments:     map[string]domain.InteractionSegment{},
		Events:       map[string][]ports.SegmentEvent{},
		Links:        map[string]domain.SegmentLink{},
		Outbox:       map[string][]domain.EvidenceOutboxEntry{},
		OutboxMeta:   map[string]outboxAttemptSnapshot{},
	}
	for id, segment := range s.segments {
		data.Segments[string(id)] = segment
	}
	for id, events := range s.events {
		data.Events[string(id)] = append([]ports.SegmentEvent(nil), events...)
	}
	for id, link := range s.links {
		data.Links[id] = link
	}
	for segmentID, entries := range s.outbox {
		data.Outbox[string(segmentID)] = append([]domain.EvidenceOutboxEntry(nil), entries...)
	}
	for id, meta := range s.outboxMeta {
		if meta != nil {
			data.OutboxMeta[id] = outboxAttemptSnapshot{StageAttempts: meta.stageAttempts, CommitAttempts: meta.commitAttempts}
		}
	}
	data.OutboxEpoch = s.outboxEpoch
	admitted := make([]string, 0, len(s.admittedBatch))
	for batchID := range s.admittedBatch {
		admitted = append(admitted, batchID)
	}
	sort.Strings(admitted)
	data.AdmittedBatch = admitted
	data.AdmissionLog = append([]string(nil), s.admissionLog...)
	data.RoomOrder = append([]domain.RoomID(nil), s.roomOrder...)
	for _, roomID := range s.roomOrder {
		record := s.rooms[roomID]
		replies := make([]replyKeySnapshot, 0, len(record.replyKeys))
		for key, message := range record.replyKeys {
			replies = append(replies, replyKeySnapshot{Agent: key.Agent, Op: key.Op, Message: message})
		}
		data.Rooms = append(data.Rooms, roomSnapshot{
			Room:         record.room,
			Messages:     append([]domain.RoomMessage(nil), record.messages...),
			Deliveries:   append([]domain.Delivery(nil), record.deliveries...),
			MentionKeys:  record.mentionKeys,
			ReplyKeys:    replies,
			DeliveryUniq: record.deliveryUniq,
		})
	}
	return json.Marshal(data)
}

// Restore replaces the store contents with a snapshot image and re-armies the
// single-flight tables as empty: after a restart nothing is claimed.
func (s *Store) Restore(image []byte) error {
	var data snapshot
	if err := json.Unmarshal(image, &data); err != nil {
		return fmt.Errorf("store snapshot is not valid JSON: %w", err)
	}
	// Validate the whole image before installing any of it (R5 round 5
	// note): an exhausted epoch fails closed here, before a single field of
	// the receiver has been replaced, so a rejected restore can never leave
	// a half-updated store behind. The restored process would otherwise
	// mint lease tokens under a wrapped epoch and re-mint past epochs'
	// tokens — the exact collision the epoch fence exists to prevent.
	if data.OutboxEpoch == math.MaxInt64 {
		return fmt.Errorf("store snapshot outbox epoch is exhausted: lease tokens can no longer be fenced across restarts")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.roomOrder = data.RoomOrder
	if s.roomOrder == nil {
		s.roomOrder = []domain.RoomID{}
	}
	s.rooms = map[domain.RoomID]*roomRecord{}
	for _, room := range data.Rooms {
		record := &roomRecord{
			room:         room.Room,
			messages:     room.Messages,
			deliveries:   room.Deliveries,
			mentionKeys:  room.MentionKeys,
			replyKeys:    make(map[replyKey]domain.RoomMessage, len(room.ReplyKeys)),
			deliveryUniq: room.DeliveryUniq,
		}
		for _, reply := range room.ReplyKeys {
			record.replyKeys[replyKey{Agent: reply.Agent, Op: reply.Op}] = reply.Message
		}
		if record.mentionKeys == nil {
			record.mentionKeys = map[string]domain.RoomMessage{}
		}
		if record.deliveryUniq == nil {
			record.deliveryUniq = map[string]domain.DeliveryID{}
		}
		s.rooms[record.room.ID] = record
	}
	s.segmentOrder = data.SegmentOrder
	if s.segmentOrder == nil {
		s.segmentOrder = []domain.SegmentID{}
	}
	s.segments = map[domain.SegmentID]domain.InteractionSegment{}
	for id, segment := range data.Segments {
		s.segments[domain.SegmentID(id)] = segment
	}
	s.events = map[domain.SegmentID][]ports.SegmentEvent{}
	for id, events := range data.Events {
		s.events[domain.SegmentID(id)] = events
	}
	s.links = map[string]domain.SegmentLink{}
	for id, link := range data.Links {
		s.links[id] = link
	}
	s.outbox = map[domain.SegmentID][]domain.EvidenceOutboxEntry{}
	for id, entries := range data.Outbox {
		s.outbox[domain.SegmentID(id)] = entries
	}
	s.outboxMeta = map[string]*outboxAttemptState{}
	for id, meta := range data.OutboxMeta {
		s.outboxMeta[id] = &outboxAttemptState{stageAttempts: meta.StageAttempts, commitAttempts: meta.CommitAttempts}
	}
	// Restart fencing (R5): the restored process mints lease tokens under a
	// strictly higher epoch, so a token minted in any earlier process
	// lifetime can never validate as the owner of a re-claimed row. The
	// claim counter restarts at zero, but the epoch prefix keeps every token
	// distinct across restarts. Admission decisions are durable: a batch
	// that admitted a next episode before the restart is still admitted, so
	// re-admitting it stays a double admission.
	s.outboxEpoch = data.OutboxEpoch + 1
	s.outboxClaims = 0
	s.admittedBatch = map[string]bool{}
	for _, batchID := range data.AdmittedBatch {
		s.admittedBatch[batchID] = true
	}
	s.admissionLog = append([]string(nil), data.AdmissionLog...)
	s.claimed = map[domain.DeliveryID]bool{}
	s.agentInFlight = map[domain.RoomID]map[domain.AgentID]domain.DeliveryID{}
	return nil
}

// PersistToFile writes the snapshot atomically: temp file in the same
// directory, fsync, rename. A crash mid-write can never leave a torn image.
func (s *Store) PersistToFile(path string) error {
	image, err := s.Snapshot()
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	if _, err := temp.Write(image); err != nil {
		temp.Close()
		os.Remove(tempName)
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		os.Remove(tempName)
		return err
	}
	if err := temp.Close(); err != nil {
		os.Remove(tempName)
		return err
	}
	return os.Rename(tempName, path)
}

// LoadFromFile restores from a snapshot file. A missing file is a clean first
// boot; anything unreadable or corrupt fails closed.
func (s *Store) LoadFromFile(path string) error {
	image, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.Restore(image)
}
