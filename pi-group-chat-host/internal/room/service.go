package room

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNotImplemented is kept as the red sentinel for this package's contract
// tests; the implementation no longer returns it.
var ErrNotImplemented = errors.New("room orchestration not implemented")

// Service is the in-process Room authority used by the tracer-bullet host: it
// owns canonical message order and the (agent_id, message_id) delivery
// uniqueness for one room set. Visible agent speech only enters a room through
// PublishReply; human mentions enter through AppendMention.
type Service struct {
	mu    sync.Mutex
	rooms map[string]*roomState
}

type roomState struct {
	messages     []Message
	deliveries   []Delivery
	nextSequence int64
	nextID       int
	mentionKeys  map[string]Message
	replyKeys    map[string]Message
	deliveryKeys map[string]bool
}

type Message struct {
	ID               string
	RoomID           string
	Sequence         int64
	AuthorID         string
	Content          string
	ReplyToMessageID string
	IdempotencyKey   string
}

type Delivery struct {
	ID        string
	AgentID   string
	MessageID string
	State     string
}

type AppendMentionRequest struct {
	RoomID          string
	AuthorID        string
	Content         string
	IdempotencyKey  string
	MentionedAgents []string
}

type AppendMentionResult struct {
	Message               Message
	Deliveries            []Delivery
	Duplicate             bool
	CanonicalMessageCount int
	DeliveryCount         int
}

type ReplyRequest struct {
	RoomID             string
	AgentID            string
	ClientOperationID  string
	InReplyToMessageID string
	Content            string
}

type ReplyResult struct {
	Message               Message
	Duplicate             bool
	CanonicalMessageCount int
	DeliveryCount         int
}

func NewService() *Service {
	return &Service{rooms: map[string]*roomState{}}
}

func (s *Service) room(roomID string) *roomState {
	state, ok := s.rooms[roomID]
	if !ok {
		state = &roomState{
			mentionKeys:  map[string]Message{},
			replyKeys:    map[string]Message{},
			deliveryKeys: map[string]bool{},
		}
		s.rooms[roomID] = state
	}
	return state
}

// AppendMention publishes one canonical human message and creates at most one
// pending delivery per mentioned (agent_id, message_id). Replaying the same
// idempotency key returns the original message and deliveries without
// duplicating anything.
func (s *Service) AppendMention(_ context.Context, request AppendMentionRequest) (AppendMentionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.room(request.RoomID)
	if original, ok := state.mentionKeys[request.IdempotencyKey]; ok {
		return AppendMentionResult{
			Message:               original,
			Deliveries:            deliveriesFor(state, original.ID),
			Duplicate:             true,
			CanonicalMessageCount: len(state.messages),
			DeliveryCount:         len(state.deliveries),
		}, nil
	}
	state.nextID++
	message := Message{
		ID:             fmt.Sprintf("message-%d", state.nextID),
		RoomID:         request.RoomID,
		Sequence:       state.nextSequence + 1,
		AuthorID:       request.AuthorID,
		Content:        request.Content,
		IdempotencyKey: request.IdempotencyKey,
	}
	state.nextSequence = message.Sequence
	state.messages = append(state.messages, message)
	state.mentionKeys[request.IdempotencyKey] = message
	var deliveries []Delivery
	for _, agentID := range request.MentionedAgents {
		key := agentID + "\x00" + message.ID
		if state.deliveryKeys[key] {
			continue
		}
		state.deliveryKeys[key] = true
		state.nextID++
		delivery := Delivery{
			ID:        fmt.Sprintf("delivery-%d", state.nextID),
			AgentID:   agentID,
			MessageID: message.ID,
			State:     "pending",
		}
		state.deliveries = append(state.deliveries, delivery)
		deliveries = append(deliveries, delivery)
	}
	return AppendMentionResult{
		Message:               message,
		Deliveries:            deliveries,
		Duplicate:             false,
		CanonicalMessageCount: len(state.messages),
		DeliveryCount:         len(state.deliveries),
	}, nil
}

// PublishReply publishes one canonical agent reply keyed by the agent-chosen
// client operation id. Retries and replays return the original message;
// replies never create new deliveries.
func (s *Service) PublishReply(_ context.Context, request ReplyRequest) (ReplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.room(request.RoomID)
	if original, ok := state.replyKeys[request.ClientOperationID]; ok {
		return ReplyResult{
			Message:               original,
			Duplicate:             true,
			CanonicalMessageCount: len(state.messages),
			DeliveryCount:         len(state.deliveries),
		}, nil
	}
	state.nextID++
	message := Message{
		ID:               fmt.Sprintf("message-%d", state.nextID),
		RoomID:           request.RoomID,
		Sequence:         state.nextSequence + 1,
		AuthorID:         request.AgentID,
		Content:          request.Content,
		ReplyToMessageID: request.InReplyToMessageID,
		IdempotencyKey:   request.ClientOperationID,
	}
	state.nextSequence = message.Sequence
	state.messages = append(state.messages, message)
	state.replyKeys[request.ClientOperationID] = message
	return ReplyResult{
		Message:               message,
		Duplicate:             false,
		CanonicalMessageCount: len(state.messages),
		DeliveryCount:         len(state.deliveries),
	}, nil
}

func deliveriesFor(state *roomState, messageID string) []Delivery {
	var deliveries []Delivery
	for _, delivery := range state.deliveries {
		if delivery.MessageID == messageID {
			deliveries = append(deliveries, delivery)
		}
	}
	return deliveries
}
