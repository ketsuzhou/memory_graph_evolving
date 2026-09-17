package skilldisposition

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/room"
	"river2.dev/pi-group-chat-host/internal/skillfence"
)

const (
	ToolSkillFeedback = skillfence.ToolSkillFeedback
	ToolSkillAdoption = "skill_adoption"

	DispositionAccepted      = "accepted"
	DispositionRejected      = "rejected"
	DispositionProtocolError = "protocol_error"

	StageAccepted            = "accepted"
	StageRejected            = "rejected"
	StageAdopted             = "adopted"
	StageVerified            = "verified"
	StageOutcomeCorrelated   = "outcome-correlated"
	StageCausallySupported   = "causally supported"

	// ForbiddenActiveSkillLabel is not a Skill Interaction Signal. This arm
	// must never record it: glossary Avoid Active Skill, contract §1.2.
	ForbiddenActiveSkillLabel   = "active"
	StatusMarginalGainSupported = "marginal-gain-supported"

	MarkerAccepted = "SKILL_ACCEPTED"
	MarkerRejected = "SKILL_REJECTED"

	DefaultMemoryAgentID = "memory-agent"

	FenceFeedback = skillfence.FenceFeedback
	FenceOpen     = ""
)

var (
	ErrPublisherRequired        = errors.New("skill disposition: publisher is required")
	ErrOfferRequired            = errors.New("skill disposition: offer binding is required")
	ErrOfferNotBound            = errors.New("skill disposition: no offer is bound")
	ErrServedRequired           = errors.New("skill disposition: offer has not been served")
	ErrInvalidFeedback          = errors.New("skill disposition: invalid skill_feedback")
	ErrProtocolError            = errors.New("skill disposition: protocol_error; fence lifted")
	ErrDispositionConflict      = errors.New("skill disposition: conflicting terminal disposition")
	ErrIncompletePublication    = errors.New("skill disposition: signal, room message, and memory delivery must commit together")
	ErrOrdinaryToolFenced       = errors.New("skill disposition: ordinary tools are fenced until protocol disposition")
	ErrAdoptionRequiresAccepted = errors.New("skill disposition: adoption requires an accepted disposition")
	ErrAdoptionIncomplete       = errors.New("skill disposition: adoption must bind skill, offer, checkpoint, behavior change and affected action")
	ErrAgentMismatch            = errors.New("skill disposition: tool call agent does not match the offer recipient")
)

// Offer is the Host-owned directed offer the feedback fence is bound to.
type Offer struct {
	OfferID        string
	AgentID        string
	RoomID         string
	SkillReference string
	CheckpointID   string
	MemoryAgentID  string
}

// ToolCall is one Pi-issued tool request observed after serve.
type ToolCall struct {
	CallID         string
	Name           string
	AgentID        string
	OfferID        string
	RoomID         string
	Disposition    string
	SkillReference string
	CheckpointID   string
	BehaviorChange string
	AffectedAction string
}

// Feedback is the structured skill_feedback body.
type Feedback struct {
	CallID         string
	OfferID        string
	AgentID        string
	RoomID         string
	Disposition    string
	SkillReference string
	CheckpointID   string
}

// AdoptionRequest is the optional skill_adoption body. It records an
// actual behavior change and is never inferred from accepted.
type AdoptionRequest struct {
	CallID         string
	OfferID        string
	AgentID        string
	SkillReference string
	CheckpointID   string
	BehaviorChange string
	AffectedAction string
}

// Disposition is the unique terminal fact for one (offer, agent).
type Disposition struct {
	OfferID        string
	AgentID        string
	Value          string
	SkillReference string
	BodyDigest     string
	CallID         string
	CheckpointID   string
}

// InteractionSignal is the Host-owned accepted/rejected interaction fact.
// It never implies adopted, verified, active, or marginal-gain-supported.
type InteractionSignal struct {
	OfferID        string
	AgentID        string
	Stage          string
	BodyDigest     string
	SkillReference string
}

// AdoptionRecord is an explicit agent-declared behavior change.
type AdoptionRecord struct {
	OfferID        string
	AgentID        string
	SkillReference string
	CheckpointID   string
	BehaviorChange string
	AffectedAction string
	CallID         string
}

// Publication is the all-or-none accepted/rejected triple.
type Publication struct {
	Signal   InteractionSignal
	Message  room.Message
	Delivery room.Delivery
}

// Result is the fence decision for one tool call or feedback.
type Result struct {
	Allowed       bool
	Duplicate     bool
	Repair        bool
	ProtocolError bool
	Fence         string
	Disposition   *Disposition
	Signal        *InteractionSignal
	Message       *room.Message
	Delivery      *room.Delivery
	Adoption      *AdoptionRecord
}

type stored struct {
	result Result
	err    error
}

type committed struct {
	disposition Disposition
	publication *Publication
}

// Coordinator owns accepted/rejected dispositions, the post-serve tool
// fence, optional adoptions, and the all-or-none publication triple.
type Coordinator struct {
	publisher Publisher

	mu           sync.Mutex
	bound        map[string]Offer
	served       map[string]skillfence.ServedFact
	terminal     map[string]committed
	repairs      map[string]int
	byCall       map[string]stored
	adoptions    []AdoptionRecord
	signals      []InteractionSignal
	publications []Publication
	later        []string
}

func NewCoordinator(publisher Publisher) (*Coordinator, error) {
	if publisher == nil {
		return nil, ErrPublisherRequired
	}
	return &Coordinator{
		publisher: publisher,
		bound:     map[string]Offer{},
		served:    map[string]skillfence.ServedFact{},
		terminal:  map[string]committed{},
		repairs:   map[string]int{},
		byCall:    map[string]stored{},
	}, nil
}

// BindOffer starts the skill_feedback fence for one served-or-pending offer.
func (c *Coordinator) BindOffer(offer Offer) error {
	if strings.TrimSpace(offer.OfferID) == "" || strings.TrimSpace(offer.AgentID) == "" {
		return ErrOfferRequired
	}
	if strings.TrimSpace(offer.RoomID) == "" {
		return ErrOfferRequired
	}
	if strings.TrimSpace(offer.MemoryAgentID) == "" {
		offer.MemoryAgentID = DefaultMemoryAgentID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bound[offer.AgentID] = offer
	return nil
}

// NoteServed records a Host-authored served fact. accepted is refused
// until this fact exists for the same offer/agent.
func (c *Coordinator) NoteServed(fact skillfence.ServedFact) error {
	if strings.TrimSpace(fact.OfferID) == "" || strings.TrimSpace(fact.AgentID) == "" {
		return ErrOfferRequired
	}
	if fact.Stage != "" && fact.Stage != skillfence.StageServed {
		return ErrServedRequired
	}
	fact.Stage = skillfence.StageServed
	c.mu.Lock()
	defer c.mu.Unlock()
	key := pairKey(fact.OfferID, fact.AgentID)
	if _, exists := c.served[key]; exists {
		return nil
	}
	c.served[key] = fact
	return nil
}

func (c *Coordinator) Fence(agentID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fenceLocked(agentID)
}

func (c *Coordinator) Dispositions() []Disposition {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Disposition, 0, len(c.terminal))
	for _, item := range c.terminal {
		out = append(out, item.disposition)
	}
	return out
}

func (c *Coordinator) Signals() []InteractionSignal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]InteractionSignal(nil), c.signals...)
}

func (c *Coordinator) Publications() []Publication {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Publication, len(c.publications))
	copy(out, c.publications)
	return out
}

func (c *Coordinator) Adoptions() []AdoptionRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]AdoptionRecord(nil), c.adoptions...)
}

// LaterStages reports later Skill Interaction Signals this coordinator
// has recorded (adopted, verified, outcome-correlated, causally supported).
// accepted never writes them, and the forbidden "active" label is never a stage.
func (c *Coordinator) LaterStages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.later...)
}

// HandleToolCall is the exclusive post-serve tool gate. Ordinary tools
// stay refused until a terminal disposition (or protocol_error) lifts
// the fence. skill_adoption is optional and independent of accepted.
func (c *Coordinator) HandleToolCall(ctx context.Context, call ToolCall) (Result, error) {
	c.mu.Lock()
	if call.CallID != "" {
		if existing, ok := c.byCall[call.CallID]; ok {
			c.mu.Unlock()
			replay := existing.result
			replay.Duplicate = true
			return replay, existing.err
		}
	}
	offer, err := c.resolveOfferLocked(call)
	if err != nil {
		c.mu.Unlock()
		return Result{}, err
	}
	if call.AgentID != "" && call.AgentID != offer.AgentID {
		c.mu.Unlock()
		return Result{Fence: c.fenceLocked(offer.AgentID)}, ErrAgentMismatch
	}

	switch call.Name {
	case ToolSkillFeedback:
		c.mu.Unlock()
		return c.feedback(ctx, Feedback{
			CallID:         call.CallID,
			OfferID:        offer.OfferID,
			AgentID:        offer.AgentID,
			RoomID:         firstNonEmpty(call.RoomID, offer.RoomID),
			Disposition:    call.Disposition,
			SkillReference: firstNonEmpty(call.SkillReference, offer.SkillReference),
			CheckpointID:   firstNonEmpty(call.CheckpointID, offer.CheckpointID),
		})
	case ToolSkillAdoption:
		c.mu.Unlock()
		return c.adopt(AdoptionRequest{
			CallID:         call.CallID,
			OfferID:        offer.OfferID,
			AgentID:        offer.AgentID,
			SkillReference: firstNonEmpty(call.SkillReference, offer.SkillReference),
			CheckpointID:   firstNonEmpty(call.CheckpointID, offer.CheckpointID),
			BehaviorChange: call.BehaviorChange,
			AffectedAction: call.AffectedAction,
		})
	default:
		if c.hasTerminalLocked(offer.OfferID, offer.AgentID) {
			result := Result{Allowed: true, Fence: FenceOpen}
			c.storeCallLocked(call.CallID, result, nil)
			c.mu.Unlock()
			return result, nil
		}
		c.mu.Unlock()
		return Result{Fence: FenceFeedback}, ErrOrdinaryToolFenced
	}
}

func (c *Coordinator) feedback(ctx context.Context, fb Feedback) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fb.CallID != "" {
		if existing, ok := c.byCall[fb.CallID]; ok {
			replay := existing.result
			replay.Duplicate = true
			return replay, existing.err
		}
	}

	key := pairKey(fb.OfferID, fb.AgentID)
	if !validDisposition(fb.Disposition) {
		return c.invalidLocked(fb, key)
	}
	if strings.TrimSpace(fb.OfferID) == "" || strings.TrimSpace(fb.AgentID) == "" {
		return c.invalidLocked(fb, key)
	}

	served, ok := c.served[key]
	if !ok {
		result := Result{Fence: FenceFeedback}
		c.storeCallLocked(fb.CallID, result, ErrServedRequired)
		return result, ErrServedRequired
	}

	skillRef := firstNonEmpty(fb.SkillReference, served.SkillReference)
	if existing, ok := c.terminal[key]; ok {
		if existing.disposition.Value == fb.Disposition && existing.disposition.SkillReference == skillRef {
			result := replayResult(existing, FenceOpen)
			c.storeCallLocked(fb.CallID, result, nil)
			return result, nil
		}
		result := Result{
			Fence:       FenceOpen,
			Disposition: cloneDisposition(&existing.disposition),
		}
		if existing.publication != nil {
			result.Signal = cloneSignal(&existing.publication.Signal)
			result.Message = cloneMessage(&existing.publication.Message)
			result.Delivery = cloneDelivery(&existing.publication.Delivery)
		}
		c.storeCallLocked(fb.CallID, result, ErrDispositionConflict)
		return result, ErrDispositionConflict
	}

	signal := InteractionSignal{
		OfferID:        served.OfferID,
		AgentID:        served.AgentID,
		Stage:          fb.Disposition,
		BodyDigest:     served.BodyDigest,
		SkillReference: skillRef,
	}
	marker := MarkerAccepted
	if fb.Disposition == DispositionRejected {
		marker = MarkerRejected
	}
	memoryID := c.memoryAgentLocked(fb.AgentID)
	roomID := firstNonEmpty(fb.RoomID, c.boundRoomLocked(fb.AgentID))
	req := directedoffer.Request{
		RoomID:           roomID,
		AuthorID:         fb.AgentID,
		Content:          formatVisibleMessage(memoryID, marker, served.OfferID, skillRef),
		IdempotencyKey:   publicationKey(served.OfferID, served.AgentID),
		RecipientAgentID: memoryID,
		SkillReference:   skillRef,
	}

	published, err := c.publisher.Publish(ctx, req)
	if err != nil {
		result := Result{Fence: FenceFeedback}
		c.storeCallLocked(fb.CallID, result, err)
		return result, err
	}
	if published.Message.ID == "" || len(published.Deliveries) != 1 {
		result := Result{Fence: FenceFeedback}
		c.storeCallLocked(fb.CallID, result, ErrIncompletePublication)
		return result, ErrIncompletePublication
	}
	delivery := published.Deliveries[0]
	if delivery.AgentID != memoryID || delivery.MessageID != published.Message.ID {
		result := Result{Fence: FenceFeedback}
		c.storeCallLocked(fb.CallID, result, ErrIncompletePublication)
		return result, ErrIncompletePublication
	}

	disp := Disposition{
		OfferID:        served.OfferID,
		AgentID:        served.AgentID,
		Value:          fb.Disposition,
		SkillReference: skillRef,
		BodyDigest:     served.BodyDigest,
		CallID:         fb.CallID,
		CheckpointID:   fb.CheckpointID,
	}
	pub := Publication{
		Signal:   signal,
		Message:  published.Message,
		Delivery: delivery,
	}
	c.terminal[key] = committed{disposition: disp, publication: &pub}
	c.signals = append(c.signals, signal)
	c.publications = append(c.publications, pub)

	result := Result{
		Allowed:     true,
		Fence:       FenceOpen,
		Disposition: cloneDisposition(&disp),
		Signal:      cloneSignal(&signal),
		Message:     cloneMessage(&published.Message),
		Delivery:    cloneDelivery(&delivery),
	}
	c.storeCallLocked(fb.CallID, result, nil)
	return result, nil
}

func (c *Coordinator) invalidLocked(fb Feedback, key string) (Result, error) {
	if existing, ok := c.terminal[key]; ok {
		result := Result{
			Fence:       c.fenceLocked(fb.AgentID),
			Disposition: cloneDisposition(&existing.disposition),
		}
		c.storeCallLocked(fb.CallID, result, ErrDispositionConflict)
		return result, ErrDispositionConflict
	}
	c.repairs[key]++
	if c.repairs[key] == 1 {
		result := Result{Repair: true, Fence: FenceFeedback}
		c.storeCallLocked(fb.CallID, result, ErrInvalidFeedback)
		return result, ErrInvalidFeedback
	}
	disp := Disposition{
		OfferID: fb.OfferID,
		AgentID: fb.AgentID,
		Value:   DispositionProtocolError,
		CallID:  fb.CallID,
	}
	c.terminal[key] = committed{disposition: disp}
	result := Result{
		Allowed:       true,
		ProtocolError: true,
		Fence:         FenceOpen,
		Disposition:   cloneDisposition(&disp),
	}
	c.storeCallLocked(fb.CallID, result, ErrProtocolError)
	return result, ErrProtocolError
}

func (c *Coordinator) adopt(req AdoptionRequest) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.CallID != "" {
		if existing, ok := c.byCall[req.CallID]; ok {
			replay := existing.result
			replay.Duplicate = true
			return replay, existing.err
		}
	}
	if strings.TrimSpace(req.OfferID) == "" || strings.TrimSpace(req.AgentID) == "" ||
		strings.TrimSpace(req.SkillReference) == "" || strings.TrimSpace(req.CheckpointID) == "" ||
		strings.TrimSpace(req.BehaviorChange) == "" || strings.TrimSpace(req.AffectedAction) == "" {
		result := Result{Fence: c.fenceLocked(req.AgentID)}
		c.storeCallLocked(req.CallID, result, ErrAdoptionIncomplete)
		return result, ErrAdoptionIncomplete
	}
	existing, ok := c.terminal[pairKey(req.OfferID, req.AgentID)]
	if !ok || existing.disposition.Value != DispositionAccepted {
		result := Result{Fence: c.fenceLocked(req.AgentID)}
		c.storeCallLocked(req.CallID, result, ErrAdoptionRequiresAccepted)
		return result, ErrAdoptionRequiresAccepted
	}
	for _, item := range c.adoptions {
		if item.OfferID == req.OfferID && item.AgentID == req.AgentID {
			result := Result{
				Allowed:   true,
				Duplicate: true,
				Fence:     FenceOpen,
				Adoption:  cloneAdoption(&item),
			}
			c.storeCallLocked(req.CallID, result, nil)
			return result, nil
		}
	}
	record := AdoptionRecord{
		OfferID:        req.OfferID,
		AgentID:        req.AgentID,
		SkillReference: req.SkillReference,
		CheckpointID:   req.CheckpointID,
		BehaviorChange: req.BehaviorChange,
		AffectedAction: req.AffectedAction,
		CallID:         req.CallID,
	}
	c.adoptions = append(c.adoptions, record)
	c.later = append(c.later, StageAdopted)
	result := Result{
		Allowed:  true,
		Fence:    FenceOpen,
		Adoption: cloneAdoption(&record),
	}
	c.storeCallLocked(req.CallID, result, nil)
	return result, nil
}

func (c *Coordinator) resolveOfferLocked(call ToolCall) (Offer, error) {
	if bound, ok := c.bound[call.AgentID]; ok {
		if call.OfferID != "" && call.OfferID != bound.OfferID {
			return Offer{
				OfferID: call.OfferID,
				AgentID: call.AgentID,
				RoomID:  firstNonEmpty(call.RoomID, bound.RoomID),
			}, nil
		}
		return bound, nil
	}
	if strings.TrimSpace(call.OfferID) == "" || strings.TrimSpace(call.AgentID) == "" {
		return Offer{}, ErrOfferNotBound
	}
	return Offer{
		OfferID:        call.OfferID,
		AgentID:        call.AgentID,
		RoomID:         call.RoomID,
		SkillReference: call.SkillReference,
		CheckpointID:   call.CheckpointID,
	}, nil
}

func (c *Coordinator) fenceLocked(agentID string) string {
	offer, ok := c.bound[agentID]
	if !ok {
		return FenceFeedback
	}
	if c.hasTerminalLocked(offer.OfferID, offer.AgentID) {
		return FenceOpen
	}
	return FenceFeedback
}

func (c *Coordinator) hasTerminalLocked(offerID, agentID string) bool {
	_, ok := c.terminal[pairKey(offerID, agentID)]
	return ok
}

func (c *Coordinator) memoryAgentLocked(agentID string) string {
	if offer, ok := c.bound[agentID]; ok && offer.MemoryAgentID != "" {
		return offer.MemoryAgentID
	}
	return DefaultMemoryAgentID
}

func (c *Coordinator) boundRoomLocked(agentID string) string {
	if offer, ok := c.bound[agentID]; ok {
		return offer.RoomID
	}
	return ""
}

func (c *Coordinator) storeCallLocked(callID string, result Result, err error) {
	if callID == "" {
		return
	}
	storedResult := result
	storedResult.Duplicate = false
	c.byCall[callID] = stored{result: storedResult, err: err}
}

func replayResult(existing committed, fence string) Result {
	result := Result{
		Allowed:     true,
		Duplicate:   true,
		Fence:       fence,
		Disposition: cloneDisposition(&existing.disposition),
	}
	if existing.publication != nil {
		result.Signal = cloneSignal(&existing.publication.Signal)
		result.Message = cloneMessage(&existing.publication.Message)
		result.Delivery = cloneDelivery(&existing.publication.Delivery)
	}
	return result
}

func validDisposition(value string) bool {
	switch strings.TrimSpace(value) {
	case DispositionAccepted, DispositionRejected:
		return true
	default:
		return false
	}
}

func formatVisibleMessage(memoryID, marker, offerID, skillRef string) string {
	return fmt.Sprintf("@%s %s offer=%s skill=%s", memoryID, marker, offerID, skillRef)
}

func publicationKey(offerID, agentID string) string {
	return "skill-disposition:" + offerID + ":" + agentID
}

func pairKey(offerID, agentID string) string {
	return offerID + "\x00" + agentID
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func cloneDisposition(in *Disposition) *Disposition {
	if in == nil {
		return nil
	}
	copy := *in
	return &copy
}

func cloneSignal(in *InteractionSignal) *InteractionSignal {
	if in == nil {
		return nil
	}
	copy := *in
	return &copy
}

func cloneMessage(in *room.Message) *room.Message {
	if in == nil {
		return nil
	}
	copy := *in
	return &copy
}

func cloneDelivery(in *room.Delivery) *room.Delivery {
	if in == nil {
		return nil
	}
	copy := *in
	return &copy
}

func cloneAdoption(in *AdoptionRecord) *AdoptionRecord {
	if in == nil {
		return nil
	}
	copy := *in
	return &copy
}
