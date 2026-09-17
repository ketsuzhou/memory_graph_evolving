package directedoffer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
	"river2.dev/pi-group-chat-host/internal/room"
)

const (
	// ReasonDirectedMention is the Host interrupt reason recorded on a
	// continuation Segment (system contract §4.3).
	ReasonDirectedMention = "DIRECTED_MENTION"
)

var (
	ErrRoomIDRequired         = errors.New("directed offer: room id is required")
	ErrAuthorIDRequired       = errors.New("directed offer: author id is required")
	ErrIdempotencyKeyRequired = errors.New("directed offer: idempotency key is required")
	ErrOfferBodyConflict      = errors.New("directed offer: idempotency key reused with a different body")
	ErrExactSessionRequired   = errors.New("directed offer: exact session file and id are required")
	ErrAgentMismatch          = errors.New("directed offer: session agent does not match the binding")
	ErrSessionMismatch        = errors.New("directed offer: session file/id does not match the binding")
	ErrAlreadyAttached        = errors.New("directed offer: agent already has an attached exact session")
)

// Request is one Skill Offer or display-only Room publication.
// RecipientAgentID is the structured routing target. Content may contain
// @agent for display; the coordinator never parses it for routing.
type Request struct {
	RoomID           string
	AuthorID         string
	Content          string
	IdempotencyKey   string
	RecipientAgentID string
	SkillReference   string
}

// Result is the durable publication plus, when this offer joined an
// interrupt cycle, the continuation Segment created on resume.
type Result struct {
	Message    room.Message
	Deliveries []room.Delivery
	Duplicate  bool
	Segment    Segment
}

// Binding pins one Agent to one exact session and the currently open
// Segment. The key fields match system contract §4.2.
type Binding struct {
	EvaluationID string
	LogicalRunID string
	AttemptID    string
	RoomID       string
	AgentID      string
	Session      sessionctrl.Session
	Generation   uint64
	SegmentID    string
}

// QueuedMessage is one pending directed mention waiting for a single
// ordered resume injection.
type QueuedMessage struct {
	MessageID  string
	DeliveryID string
	Sequence   int64
	Content    string
}

// Segment is a Host-authored interaction Segment. A resume Segment records
// continuation_of and the directed-mention reason.
type Segment struct {
	ID             string
	RoomID         string
	AgentID        string
	ContinuationOf string `json:"continuation_of"`
	Reason         string
}

type offerRecord struct {
	body   offerBody
	result Result
}

type offerBody struct {
	AuthorID         string
	Content          string
	RecipientAgentID string
	SkillReference   string
}

type interruptCycle struct {
	generation uint64
	done       chan struct{}
	segment    Segment
	err        error
}

type agentRuntime struct {
	binding             Binding
	session             ExactSession
	interruptGeneration uint64
	abortStarted        bool
	pending             []QueuedMessage
	inflight            int
	currentSegment      Segment
	segments            []Segment
	segmentSeq          int
	cycle               *interruptCycle
}

// Coordinator is the Host directed-offer authority: atomic Room
// message + unique Delivery, single abort per interrupt generation, and
// one ordered exact-session resume.
type Coordinator struct {
	room *room.Service

	mu           sync.Mutex
	inflightZero *sync.Cond
	offers       map[string]offerRecord
	agents       map[string]*agentRuntime
}

// NewCoordinator builds an empty coordinator over an in-process Room.
func NewCoordinator() *Coordinator {
	c := &Coordinator{
		room:   room.NewService(),
		offers: map[string]offerRecord{},
		agents: map[string]*agentRuntime{},
	}
	c.inflightZero = sync.NewCond(&c.mu)
	return c
}

// Attach binds one exact task session. The stored Session file/id is the
// only resume selector; interactive --resume / --continue are never used.
func (c *Coordinator) Attach(binding Binding, session ExactSession) error {
	if err := binding.Session.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrExactSessionRequired, err)
	}
	if session == nil {
		return ErrExactSessionRequired
	}
	if session.AgentID() == "" || session.AgentID() != binding.AgentID {
		return ErrAgentMismatch
	}
	got := session.Session()
	if got.File != binding.Session.File || got.ID != binding.Session.ID {
		return ErrSessionMismatch
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.agents[binding.AgentID]; exists {
		return ErrAlreadyAttached
	}
	segmentID := binding.SegmentID
	if segmentID == "" {
		segmentID = "segment-" + binding.AgentID
	}
	initial := Segment{ID: segmentID, RoomID: binding.RoomID, AgentID: binding.AgentID}
	c.agents[binding.AgentID] = &agentRuntime{
		binding:        binding,
		session:        session,
		currentSegment: initial,
		segments:       []Segment{initial},
	}
	return nil
}

// queued returns the directed mentions waiting for the current resume.
func (c *Coordinator) queued(agentID string) []QueuedMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	agent, ok := c.agents[agentID]
	if !ok {
		return nil
	}
	return cloneQueued(agent.pending)
}

// Segments returns the Host-authored Segments for agentID, including the
// opening Segment and every directed-mention continuation.
func (c *Coordinator) Segments(agentID string) []Segment {
	c.mu.Lock()
	defer c.mu.Unlock()
	agent, ok := c.agents[agentID]
	if !ok {
		return nil
	}
	out := make([]Segment, len(agent.segments))
	copy(out, agent.segments)
	return out
}

// Offer atomically publishes one Room message. A structured
// RecipientAgentID creates exactly one unique Delivery and, when the
// target session is running, joins a single abort/resume cycle. Free-text
// @agent without a recipient is stored as display and does not route.
func (c *Coordinator) Offer(ctx context.Context, req Request) (Result, error) {
	if err := req.validate(); err != nil {
		return Result{}, err
	}
	key := offerKey(req.RoomID, req.IdempotencyKey)
	body := req.body()
	recipient := req.RecipientAgentID

	c.mu.Lock()
	if existing, ok := c.offers[key]; ok {
		c.mu.Unlock()
		if existing.body != body {
			return Result{}, ErrOfferBodyConflict
		}
		replay := existing.result
		replay.Duplicate = true
		return replay, nil
	}

	mentioned := []string(nil)
	if recipient != "" {
		mentioned = []string{recipient}
	}
	published, err := c.room.AppendMention(ctx, room.AppendMentionRequest{
		RoomID:          req.RoomID,
		AuthorID:        req.AuthorID,
		Content:         req.Content,
		IdempotencyKey:  req.IdempotencyKey,
		MentionedAgents: mentioned,
	})
	if err != nil {
		c.mu.Unlock()
		return Result{}, err
	}
	result := Result{
		Message:    published.Message,
		Deliveries: append([]room.Delivery(nil), published.Deliveries...),
		Duplicate:  published.Duplicate,
	}
	c.offers[key] = offerRecord{body: body, result: result}

	agent, ok := c.agents[recipient]
	if recipient == "" || !ok || (!agent.session.Running() && !agent.abortStarted) {
		c.mu.Unlock()
		return result, nil
	}

	queued := QueuedMessage{
		MessageID: published.Message.ID,
		Sequence:  published.Message.Sequence,
		Content:   published.Message.Content,
	}
	if len(published.Deliveries) > 0 {
		queued.DeliveryID = published.Deliveries[0].ID
	}
	agent.pending = append(agent.pending, queued)
	agent.inflight++
	start := !agent.abortStarted
	if start {
		agent.abortStarted = true
		agent.interruptGeneration++
		agent.cycle = &interruptCycle{generation: agent.interruptGeneration, done: make(chan struct{})}
	}
	cycle := agent.cycle
	c.mu.Unlock()

	if start {
		go c.interruptAndResume(agent, cycle)
	}

	c.mu.Lock()
	agent.inflight--
	if agent.inflight == 0 {
		c.inflightZero.Broadcast()
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case <-cycle.done:
		if cycle.err != nil {
			return result, cycle.err
		}
		result.Segment = cycle.segment
		return result, nil
	}
}

func (c *Coordinator) interruptAndResume(agent *agentRuntime, cycle *interruptCycle) {
	defer close(cycle.done)
	ctx := context.Background()

	if err := agent.session.Abort(ctx); err != nil {
		cycle.err = err
		c.finishCycle(agent)
		return
	}
	if err := agent.session.WaitSettled(ctx); err != nil {
		cycle.err = err
		c.finishCycle(agent)
		return
	}

	c.mu.Lock()
	for agent.inflight > 0 {
		c.inflightZero.Wait()
	}
	pending := append([]QueuedMessage(nil), agent.pending...)
	agent.pending = nil
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].Sequence < pending[j].Sequence
	})
	agent.segmentSeq++
	next := Segment{
		ID:             fmt.Sprintf("%s-cont-%d", agent.currentSegment.ID, agent.segmentSeq),
		RoomID:         agent.binding.RoomID,
		AgentID:        agent.binding.AgentID,
		ContinuationOf: agent.currentSegment.ID,
		Reason:         ReasonDirectedMention,
	}
	agent.currentSegment = next
	agent.segments = append(agent.segments, next)
	cycle.segment = next
	session := agent.binding.Session
	c.mu.Unlock()

	if err := agent.session.Resume(ctx, ResumeRequest{Session: session, Messages: pending}); err != nil {
		cycle.err = err
	}
	c.finishCycle(agent)
}

func (c *Coordinator) finishCycle(agent *agentRuntime) {
	c.mu.Lock()
	defer c.mu.Unlock()
	agent.abortStarted = false
	if agent.cycle != nil && agent.inflight == 0 && len(agent.pending) == 0 {
		agent.cycle = nil
	}
}

func (r Request) validate() error {
	if r.RoomID == "" {
		return ErrRoomIDRequired
	}
	if r.AuthorID == "" {
		return ErrAuthorIDRequired
	}
	if r.IdempotencyKey == "" {
		return ErrIdempotencyKeyRequired
	}
	return nil
}

func (r Request) body() offerBody {
	return offerBody{
		AuthorID:         r.AuthorID,
		Content:          r.Content,
		RecipientAgentID: r.RecipientAgentID,
		SkillReference:   r.SkillReference,
	}
}

func offerKey(roomID, key string) string {
	return roomID + "\x00" + key
}

func cloneQueued(src []QueuedMessage) []QueuedMessage {
	if len(src) == 0 {
		return nil
	}
	out := make([]QueuedMessage, len(src))
	copy(out, src)
	return out
}
