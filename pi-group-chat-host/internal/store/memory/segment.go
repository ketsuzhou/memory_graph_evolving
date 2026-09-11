package memory

// SegmentSeals is the internal/segment Store adapter: an in-memory,
// append-only segment/seal store. One mutex guards every operation so a
// CommitClose lands as a single atomic CAS write (terminal state + frozen
// input identity + seal record together, or nothing), and terminal segments
// reject every further append with APPEND_AFTER_SETTLED (Host 3.2/3.7).

import (
	"context"
	"sync"

	"river2.dev/pi-group-chat-host/internal/segment"
)

type segmentRecord struct {
	open        segment.OpenSegment
	state       string
	head        int64
	lastTick    int64
	members     []segment.Member
	links       []segment.CausalLink
	deliveries  []segment.Delivery
	executions  []segment.ToolExecution
	closeKey    string
	inputDigest string
	result      *segment.CloseResult
	canonical   []byte
}

// SegmentSeals stores open segments, their frozen close input and the
// committed seal records. The zero value is not usable; call NewSegmentSeals.
type SegmentSeals struct {
	mu       sync.Mutex
	segments map[string]*segmentRecord
	order    []string
}

// NewSegmentSeals returns an empty segment/seal store.
func NewSegmentSeals() *SegmentSeals {
	return &SegmentSeals{segments: map[string]*segmentRecord{}}
}

func notFound(segmentID string) error {
	return &segment.CloseError{ReasonCode: "NOT_FOUND", Detail: "segment " + segmentID}
}

// sameOpenSegment compares the full open configuration; every field is
// comparable, so identical configurations replay identically and any drift
// on the same segment identity fails closed.
func sameOpenSegment(a, b segment.OpenSegment) bool {
	return a == b
}

// OpenSegment creates the open segment (Host 3.4). Re-opening with identical
// identity/configuration is idempotent; drift on the same identity fails
// closed instead of mutating the frozen configuration.
func (s *SegmentSeals) OpenSegment(_ context.Context, seg segment.OpenSegment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.segments[seg.SegmentID]; ok {
		if !sameOpenSegment(existing.open, seg) {
			return &segment.CloseError{ReasonCode: "IDEMPOTENCY_CONFLICT",
				Detail: "segment " + seg.SegmentID + " already opened with different identity/configuration"}
		}
		return nil
	}
	s.segments[seg.SegmentID] = &segmentRecord{
		open:  seg,
		state: segment.StateOpen,
		head:  seg.FrontierStart,
	}
	s.order = append(s.order, seg.SegmentID)
	return nil
}

func (s *SegmentSeals) requireOpen(segmentID string) (*segmentRecord, error) {
	rec, ok := s.segments[segmentID]
	if !ok {
		return nil, notFound(segmentID)
	}
	if rec.state != segment.StateOpen {
		return nil, &segment.CloseError{ReasonCode: "APPEND_AFTER_SETTLED",
			Detail: "segment " + segmentID + " is terminal (" + rec.state + "); appends are forbidden"}
	}
	return rec, nil
}

// AppendMember appends one member event to an open segment. Room sequences
// and logical ticks must strictly increase; event ids stay unique.
func (s *SegmentSeals) AppendMember(_ context.Context, segmentID string, m segment.Member) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.requireOpen(segmentID)
	if err != nil {
		return err
	}
	for _, existing := range rec.members {
		if existing.EventID == m.EventID {
			return &segment.CloseError{ReasonCode: "DUPLICATE_EVENT_ID", Detail: m.EventID}
		}
	}
	if m.RoomSequence <= rec.head {
		return &segment.CloseError{ReasonCode: "MEMBER_ORDER_INVALID",
			Detail: "room sequence must strictly increase beyond the frontier head"}
	}
	if len(rec.members) > 0 && m.LogicalTick <= rec.lastTick {
		return &segment.CloseError{ReasonCode: "LOGICAL_CLOCK_INVALID",
			Detail: "logical ticks must strictly increase"}
	}
	rec.members = append(rec.members, m)
	rec.head = m.RoomSequence
	rec.lastTick = m.LogicalTick
	return nil
}

// PutLink appends one causal link to an open segment. Both endpoints must
// already be frozen members (Host 3.2 invariant 2).
func (s *SegmentSeals) PutLink(_ context.Context, segmentID string, l segment.CausalLink) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.requireOpen(segmentID)
	if err != nil {
		return err
	}
	for _, existing := range rec.links {
		if existing.LinkID == l.LinkID {
			return &segment.CloseError{ReasonCode: "DUPLICATE_LINK_ID", Detail: l.LinkID}
		}
	}
	known := map[string]bool{}
	for _, m := range rec.members {
		known[m.EventID] = true
	}
	if !known[l.FromEvent] || !known[l.ToEvent] {
		return &segment.CloseError{ReasonCode: "MISSING_CAUSAL_LINK",
			Detail: "link " + l.LinkID + " endpoint is not a frozen member"}
	}
	rec.links = append(rec.links, l)
	return nil
}

// RecordDelivery records one delivery terminal state on an open segment.
func (s *SegmentSeals) RecordDelivery(_ context.Context, segmentID string, d segment.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.requireOpen(segmentID)
	if err != nil {
		return err
	}
	for _, existing := range rec.deliveries {
		if existing.DeliveryID == d.DeliveryID {
			return &segment.CloseError{ReasonCode: "DUPLICATE_DELIVERY_ID", Detail: d.DeliveryID}
		}
	}
	rec.deliveries = append(rec.deliveries, d)
	return nil
}

// RecordToolExecution records one tool correlation on an open segment.
func (s *SegmentSeals) RecordToolExecution(_ context.Context, segmentID string, e segment.ToolExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.requireOpen(segmentID)
	if err != nil {
		return err
	}
	for _, existing := range rec.executions {
		if existing.ToolCallEvent == e.ToolCallEvent {
			return &segment.CloseError{ReasonCode: "TOOL_EXECUTION_INVALID",
				Detail: "tool call " + e.ToolCallEvent + " already has an execution record"}
		}
	}
	rec.executions = append(rec.executions, e)
	return nil
}

// Snapshot returns the frozen close-input view of a segment.
func (s *SegmentSeals) Snapshot(_ context.Context, segmentID string) (segment.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.segments[segmentID]
	if !ok {
		return segment.Snapshot{}, notFound(segmentID)
	}
	return segment.Snapshot{
		Segment:      rec.open,
		State:        rec.state,
		HeadSequence: rec.head,
		Members:      append([]segment.Member(nil), rec.members...),
		Links:        append([]segment.CausalLink(nil), rec.links...),
		Deliveries:   append([]segment.Delivery(nil), rec.deliveries...),
		Executions:   append([]segment.ToolExecution(nil), rec.executions...),
	}, nil
}

// CommitClose is the single atomic terminal write.
//
// On an open segment it CASes on the expected frontier head and freezes the
// terminal state, close idempotency identity and seal record together. On a
// terminal segment the same close idempotency key with the same frozen-input
// digest replays the original result (Idempotent=true); the same key with a
// different input digest is an IDEMPOTENCY_CONFLICT; a different key cannot
// re-close a terminal segment (ILLEGAL_STATE_TRANSITION).
func (s *SegmentSeals) CommitClose(_ context.Context, c segment.Commit) (segment.CloseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.segments[c.SegmentID]
	if !ok {
		return segment.CloseResult{}, notFound(c.SegmentID)
	}
	if rec.state != segment.StateOpen {
		if rec.closeKey == c.CloseIDempotencyKey {
			if rec.inputDigest == c.InputDigest {
				replay := *rec.result
				replay.Idempotent = true
				return replay, nil
			}
			return segment.CloseResult{}, &segment.CloseError{ReasonCode: "IDEMPOTENCY_CONFLICT",
				Detail: "close key " + c.CloseIDempotencyKey + " reused with a different frozen input"}
		}
		return segment.CloseResult{}, &segment.CloseError{ReasonCode: "ILLEGAL_STATE_TRANSITION",
			Detail: "segment " + c.SegmentID + " is terminal (" + rec.state + ") and cannot re-close"}
	}
	if c.ExpectedState != segment.StateOpen {
		return segment.CloseResult{}, &segment.CloseError{ReasonCode: "ILLEGAL_STATE_TRANSITION",
			Detail: "close CAS expected state " + c.ExpectedState + " on an open segment"}
	}
	if rec.head != c.ExpectedHead {
		return segment.CloseResult{}, &segment.CloseError{ReasonCode: "FRONTIER_MISMATCH",
			Detail: "close CAS frontier head moved: expected segment head differs from frozen head"}
	}
	// Atomic commit: terminal state, idempotency identity and the seal
	// record become durable together; there is no partial-settle window.
	rec.state = c.Result.TerminalState
	rec.closeKey = c.CloseIDempotencyKey
	rec.inputDigest = c.InputDigest
	stored := c.Result
	rec.result = &stored
	rec.canonical = append([]byte(nil), c.Result.CanonicalBytes...)
	return stored, nil
}

// SealRecord rereads the committed seal record of a closed segment; the
// canonical bytes and digests are immutable across rereads.
func (s *SegmentSeals) SealRecord(_ context.Context, segmentID string) (segment.SealRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.segments[segmentID]
	if !ok {
		return segment.SealRecord{}, notFound(segmentID)
	}
	if rec.result == nil {
		return segment.SealRecord{}, notFound(segmentID)
	}
	return segment.SealRecord{
		TerminalState:       rec.state,
		SegmentDigest:       rec.result.SegmentDigest,
		CanonicalBytes:      append([]byte(nil), rec.canonical...),
		CloseIDempotencyKey: rec.closeKey,
		Result:              *rec.result,
	}, nil
}

var _ segment.Store = (*SegmentSeals)(nil)
