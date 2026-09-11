// Package segment implements the Host-authoritative Segment close/seal
// transaction (HST-202, Host spec 3-4, slice S3; Contract 7.5-7.7 DTO
// projections).
//
// One Close call is one transaction: under the service close mutex the frozen
// open-segment input is snapshotted once, validated fail-closed (DAG
// acyclicity and causal completeness, terminal deliveries and tool
// executions, payload and ToolProxyResult digests, frontier freeze, path
// family completeness), sealed (Evidence Seal + Conversation Path Seal +
// SegmentRef + terminal CheckpointRef over integer-only RFC 8785 JCS bytes
// from internal/contract), and committed through a single store CAS
// (expected open state + expected frontier head). Settled segments are
// append-only: any further event/link/delivery/execution append fails with
// APPEND_AFTER_SETTLED, and a close idempotency key replay returns the
// original refs or fails with IDEMPOTENCY_CONFLICT. Deterministic integrity
// errors freeze a failed terminal audit and publish zero refs; a cancelled
// context never partially settles.
package segment

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// Segment terminal states (Host 3.3). All three are terminal; none may
// transition into another.
const (
	StateOpen    = "open"
	StateSettled = "settled"
	StateFailed  = "failed"
	StateAborted = "aborted"
)

// CloseIntent is the protected close intent (Host 3.6-3.8).
type CloseIntent string

// Settle closes into the sealed terminal; Fail records a deterministic
// integrity failure; Abort records a legitimate cancellation.
const (
	IntentSettle CloseIntent = "settle"
	IntentFail   CloseIntent = "fail"
	IntentAbort  CloseIntent = "abort"
)

// Frozen schema markers of the sealed-segment data protocol (FND-002).
const (
	schemaSealedSegment   = "host.sealed-segment.v1"
	schemaSegmentSeal     = "host.segment-seal.v1"
	schemaSegmentRef      = "host.segment-ref.v1"
	schemaCheckpointRef   = "host.checkpoint-ref.v1"
	schemaEvidenceRef     = "gms.evidence-ref.v1"
	schemaEvidenceSeal    = "host.evidence-seal.v1"
	schemaPathSeal        = "host.path-seal.v1"
	schemaToolProxyResult = "host.tool-proxy-result.v1"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Terminal delivery states (Host 2.5); a close may not freeze a delivery in
// a nonterminal state.
var terminalDeliveryStates = map[string]bool{
	"delivered": true,
	"failed":    true,
	"cancelled": true,
}

// ---------------------------------------------------------------------------
// Frozen close-input value types
// ---------------------------------------------------------------------------

// Ref is the exact {id, version, digest} reference triple (Contract 6.2).
type Ref struct {
	ID      string
	Version int64
	Digest  string
}

// Member is one frozen Interaction DAG member event (Host 3.1).
type Member struct {
	EventID       string
	EventKind     string
	RoomSequence  int64
	LogicalTick   int64
	ActorID       string
	PayloadDigest string
	Payload       *contract.Object
}

// CausalLink is one frozen DAG causal link. Direction convention (frozen
// corpus): every causal edge runs from cause to effect, forward with respect
// to the monotone room-sequence chain.
type CausalLink struct {
	LinkID    string
	LinkKind  string
	FromEvent string
	ToEvent   string
}

// Delivery is one frozen delivery identity with its terminal state.
type Delivery struct {
	DeliveryID    string
	SubjectEvent  string
	TerminalState string
}

// ToolExecution is the frozen correlation of one tool call with its unique
// tool result, delivery and exact ToolProxyResult (Contract 7.18). The
// ToolProxyResult object is carried verbatim into the canonical seal.
type ToolExecution struct {
	ToolCallEvent         string
	ToolResultEvent       string
	DeliveryID            string
	ToolProxyResult       *contract.Object
	UpstreamAttemptRecord *contract.Object // present on failed/inconclusive results
	RetryCorrelation      *contract.Object // present on bounded-infra retries
}

// OpenSegment is the identity and close-policy configuration fixed when the
// Host opens a segment (Host 3.4).
type OpenSegment struct {
	RoomID                  string
	SegmentID               string
	SegmentVersion          int64
	FrontierStart           int64
	ClosePolicyRef          Ref
	ScopeProfileRef         Ref
	PathExtractionPolicyRef Ref
}

// TerminalAudit is the only record a failed/aborted close produces (Host 3.8).
type TerminalAudit struct {
	ReasonCode string
	Phase      string
	Detail     string
}

// ---------------------------------------------------------------------------
// Store port: the transactional segment/seal storage adapter
// ---------------------------------------------------------------------------

// Snapshot is the one-shot frozen close-input read inside a close
// transaction.
type Snapshot struct {
	Segment      OpenSegment
	State        string // open|settled|failed|aborted
	HeadSequence int64  // last appended member room sequence (FrontierStart when empty)
	Members      []Member
	Links        []CausalLink
	Deliveries   []Delivery
	Executions   []ToolExecution
}

// Commit is the single-CAS terminal write. ExpectedState/ExpectedHead are
// the caller's CAS view; CloseIDempotencyKey+InputDigest are the idempotency
// identity (same key + same frozen input replays the original result, same
// key + different input is a conflict).
type Commit struct {
	SegmentID           string
	ExpectedState       string
	ExpectedHead        int64
	CloseIDempotencyKey string
	InputDigest         string
	Result              CloseResult
}

// SealRecord is the append-only reread view of a committed close.
type SealRecord struct {
	TerminalState       string
	SegmentDigest       string
	CanonicalBytes      []byte
	CloseIDempotencyKey string
	Result              CloseResult
}

// Store is the port the close transaction runs on. Implementations MUST make
// CommitClose atomic (all terminal writes land together or not at all) and
// append-only (no mutation of a terminal segment or its seal record).
type Store interface {
	OpenSegment(ctx context.Context, seg OpenSegment) error
	AppendMember(ctx context.Context, segmentID string, m Member) error
	PutLink(ctx context.Context, segmentID string, l CausalLink) error
	RecordDelivery(ctx context.Context, segmentID string, d Delivery) error
	RecordToolExecution(ctx context.Context, segmentID string, e ToolExecution) error
	Snapshot(ctx context.Context, segmentID string) (Snapshot, error)
	CommitClose(ctx context.Context, c Commit) (CloseResult, error)
	SealRecord(ctx context.Context, segmentID string) (SealRecord, error)
}

// ---------------------------------------------------------------------------
// Close results and DTO projections
// ---------------------------------------------------------------------------

// SegmentRef is the Contract 7.5 SegmentRef carrying both seal refs.
type SegmentRef struct {
	SchemaVersion   string
	RoomID          string
	SegmentID       string
	SegmentVersion  int64
	SegmentDigest   string
	EvidenceSealRef Ref
	PathSealRef     Ref
}

// CheckpointRef is the Contract 7.6 terminal Decision Checkpoint ref.
type CheckpointRef struct {
	SchemaVersion      string
	RoomID             string
	SegmentRef         SegmentRef
	CheckpointID       string
	CheckpointSequence int64
	CheckpointDigest   string
}

// EvidenceRef is the Contract 7.7 EvidenceRef projection of the seal.
type EvidenceRef struct {
	SchemaVersion    string
	EvidenceID       string
	Version          int64
	EvidenceDigest   string
	CommitState      string
	EvidenceKind     string
	SourceSegmentRef SegmentRef
}

// EvidenceSeal is the Host 4.2 Evidence Seal exact record.
type EvidenceSeal struct {
	SealID      string
	SealVersion int64
	SealDigest  string
	Body        *contract.Object
	EvidenceRef EvidenceRef
}

// PathSeal is the Host 4.3 Conversation Path Seal exact record.
type PathSeal struct {
	SealID      string
	SealVersion int64
	SealDigest  string
	Body        *contract.Object
	PathIDs     []string
}

// CloseResult is the single-transaction close outcome. Settled closes carry
// every ref; failed/aborted closes carry only the terminal audit.
type CloseResult struct {
	TerminalState       string
	SegmentDigest       string
	CanonicalBytes      []byte
	CanonicalByteLength int64
	SegmentRef          *SegmentRef
	CheckpointRef       *CheckpointRef
	EvidenceSeal        *EvidenceSeal
	PathSeal            *PathSeal
	Paths               []Path
	TerminalAudit       *TerminalAudit
	Idempotent          bool
}

// ---------------------------------------------------------------------------
// Fail-closed error
// ---------------------------------------------------------------------------

// CloseError carries the closed reason-code registry code of a rejection.
type CloseError struct {
	ReasonCode string
	Detail     string
}

func (e *CloseError) Error() string {
	if e.Detail == "" {
		return e.ReasonCode
	}
	return e.ReasonCode + ": " + e.Detail
}

// ReasonOf extracts the reason code carried by err, or "" for plain errors.
func ReasonOf(err error) string {
	if err == nil {
		return ""
	}
	if ce, ok := err.(*CloseError); ok {
		return ce.ReasonCode
	}
	return ""
}

func rejected(reason, detail string) error {
	return &CloseError{ReasonCode: reason, Detail: detail}
}

// ---------------------------------------------------------------------------
// Service: the protected close transaction
// ---------------------------------------------------------------------------

// Service executes protected segment close transactions (Host 3.6-3.8).
// Only Host protected lifecycle code may construct and call it.
type Service struct {
	store Store
	mu    sync.Mutex // serializes close transactions into single CAS writes
}

// New returns a close service over store.
func New(store Store) *Service {
	return &Service{store: store}
}

// Open delegates segment opening to the store (idempotent on identical
// identity/configuration, conflicting on drift).
func (s *Service) Open(ctx context.Context, seg OpenSegment) error {
	return s.store.OpenSegment(ctx, seg)
}

// AppendMember appends one member event to an open segment.
func (s *Service) AppendMember(ctx context.Context, segmentID string, m Member) error {
	return s.store.AppendMember(ctx, segmentID, m)
}

// PutLink appends one causal link to an open segment.
func (s *Service) PutLink(ctx context.Context, segmentID string, l CausalLink) error {
	return s.store.PutLink(ctx, segmentID, l)
}

// RecordDelivery records one delivery terminal state on an open segment.
func (s *Service) RecordDelivery(ctx context.Context, segmentID string, d Delivery) error {
	return s.store.RecordDelivery(ctx, segmentID, d)
}

// RecordToolExecution records one tool correlation on an open segment.
func (s *Service) RecordToolExecution(ctx context.Context, segmentID string, e ToolExecution) error {
	return s.store.RecordToolExecution(ctx, segmentID, e)
}

// CloseRequest parameterizes one close transaction.
type CloseRequest struct {
	Intent CloseIntent
	// CloseIDempotencyKey is the close idempotency identity (Host 3.7).
	CloseIDempotencyKey string
	// ExpectedFrontierEnd is the caller's CAS view of the frontier head; the
	// zero value means "the current head". A stale view keeps the segment
	// open and fails with FRONTIER_MISMATCH.
	ExpectedFrontierEnd int64
	// ExpectedSegmentDigest optionally verifies a previously recorded seal
	// claim for the same frozen input before refs are published; drift fails
	// closed with SEAL_DIGEST_MISMATCH and publishes nothing.
	ExpectedSegmentDigest string
	// ReasonCode/Phase/Detail feed the terminal audit of explicit fail/abort
	// closes (e.g. ROOM_CANCELLED / operator_cancel).
	ReasonCode string
	Phase      string
	Detail     string
}

// Close executes the atomic protected close (Host 3.7). It returns the
// terminal result; settled closes carry SegmentRef/CheckpointRef/Evidence
// Seal/Path Seal, failed and aborted closes carry only the terminal audit.
// Operational errors (context cancelled, CAS/frontier conflict, idempotency
// conflict, unknown segment) leave the stored state untouched.
func (s *Service) Close(ctx context.Context, segmentID string, req CloseRequest) (CloseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return CloseResult{}, fmt.Errorf("close %s: %w", segmentID, err)
	}
	if req.CloseIDempotencyKey == "" {
		return CloseResult{}, rejected("SCHEMA_REQUIRED_FIELD_MISSING", "close idempotency key")
	}
	switch req.Intent {
	case IntentSettle, IntentFail, IntentAbort:
	default:
		return CloseResult{}, rejected("SCHEMA_ENUM_INVALID", "close intent "+string(req.Intent))
	}

	snap, err := s.store.Snapshot(ctx, segmentID)
	if err != nil {
		return CloseResult{}, err
	}

	// A terminal segment never re-closes: same key + same frozen input
	// replays the original refs; anything else conflicts.
	if snap.State != StateOpen {
		inputDigest, derr := inputDigestOf(snap, req.Intent)
		if derr != nil {
			return CloseResult{}, derr
		}
		return s.store.CommitClose(ctx, Commit{
			SegmentID:           segmentID,
			ExpectedState:       snap.State,
			ExpectedHead:        snap.HeadSequence,
			CloseIDempotencyKey: req.CloseIDempotencyKey,
			InputDigest:         inputDigest,
			Result:              CloseResult{},
		})
	}

	head := snap.HeadSequence
	if req.ExpectedFrontierEnd != 0 && req.ExpectedFrontierEnd != head {
		return CloseResult{}, rejected("FRONTIER_MISMATCH",
			"expected frontier head "+strconv.FormatInt(req.ExpectedFrontierEnd, 10)+
				" but frozen head is "+strconv.FormatInt(head, 10))
	}

	var result CloseResult
	switch req.Intent {
	case IntentAbort:
		if req.ReasonCode == "" {
			req.ReasonCode = "ROOM_CANCELLED"
		}
		if req.Phase == "" {
			req.Phase = "operator_cancel"
		}
		result, err = terminalClose(snap, req, StateAborted)
	case IntentFail:
		if req.ReasonCode == "" {
			req.ReasonCode = "SEGMENT_CLOSE_FAILED"
		}
		if req.Phase == "" {
			req.Phase = "host_close"
		}
		result, err = terminalClose(snap, req, StateFailed)
	default:
		result, err = settleClose(snap, req)
	}
	if err != nil {
		return CloseResult{}, err
	}

	// Cancellation check immediately before the single commit: a cancelled
	// close must leave the segment open with no partial seal (fail-closed).
	if err := ctx.Err(); err != nil {
		return CloseResult{}, fmt.Errorf("close %s: %w", segmentID, err)
	}
	inputDigest, derr := inputDigestOf(snap, req.Intent)
	if derr != nil {
		return CloseResult{}, derr
	}
	return s.store.CommitClose(ctx, Commit{
		SegmentID:           segmentID,
		ExpectedState:       StateOpen,
		ExpectedHead:        head,
		CloseIDempotencyKey: req.CloseIDempotencyKey,
		InputDigest:         inputDigest,
		Result:              result,
	})
}

// settleClose validates the frozen input, seals it and derives every ref.
// Deterministic integrity violations freeze a failed terminal with the
// violation's reason code and zero refs (Host 3.5/3.8); only operational
// errors surface as Go errors.
func settleClose(snap Snapshot, req CloseRequest) (CloseResult, error) {
	if reason := validateFrozenInput(snap); reason != nil {
		return terminalAuditClose(snap, req, reason)
	}
	paths := ExtractPaths(snap)
	if len(paths) == 0 {
		return terminalAuditClose(snap, req, rejected("MISSING_FAMILY", "path extraction discovered no decision-relevant path"))
	}

	frontierEnd := snap.HeadSequence
	checkpoint, cerr := checkpointIdentityOf(snap)
	if cerr != nil {
		return terminalAuditClose(snap, req, cerr)
	}
	evidenceBody, evErr := buildEvidenceSealBody(snap, paths, frontierEnd)
	if evErr != nil {
		return terminalAuditClose(snap, req, evErr)
	}
	pathBody, pErr := buildPathSealBody(snap, paths)
	if pErr != nil {
		return terminalAuditClose(snap, req, pErr)
	}

	evidenceDigest, dErr := contract.DigestOf(evidenceBody)
	if dErr != nil {
		return terminalAuditClose(snap, req, canonicalFailure(dErr))
	}
	pathDigest, dErr := contract.DigestOf(pathBody)
	if dErr != nil {
		return terminalAuditClose(snap, req, canonicalFailure(dErr))
	}

	record, rErr := buildSettledRecord(snap, req.CloseIDempotencyKey, checkpoint, evidenceBody, pathBody, frontierEnd)
	if rErr != nil {
		return terminalAuditClose(snap, req, rErr)
	}
	canonical, jErr := contract.JCS(record)
	if jErr != nil {
		return terminalAuditClose(snap, req, canonicalFailure(jErr))
	}
	segmentDigest := contract.DigestBytes(canonical)

	// Declared seal claims for the same frozen input must cover exactly these
	// canonical bytes before any ref is published.
	if req.ExpectedSegmentDigest != "" && req.ExpectedSegmentDigest != segmentDigest {
		return terminalAuditClose(snap, req, rejected("SEAL_DIGEST_MISMATCH",
			"declared segment digest "+req.ExpectedSegmentDigest+" does not cover the frozen canonical bytes "+segmentDigest))
	}

	segRef := deriveSegmentRef(snap.Segment, segmentDigest, evidenceDigest, pathDigest)
	result := CloseResult{
		TerminalState:       StateSettled,
		SegmentDigest:       segmentDigest,
		CanonicalBytes:      canonical,
		CanonicalByteLength: int64(len(canonical)),
		SegmentRef:          &segRef,
		EvidenceSeal: &EvidenceSeal{
			SealID:      "evseal-" + snap.Segment.SegmentID,
			SealVersion: 1,
			SealDigest:  evidenceDigest,
			Body:        evidenceBody,
			EvidenceRef: deriveEvidenceRef(segRef, evidenceDigest, primaryEvidenceKind(paths)),
		},
		PathSeal: &PathSeal{
			SealID:      "pathseal-" + snap.Segment.SegmentID,
			SealVersion: 1,
			SealDigest:  pathDigest,
			Body:        pathBody,
		},
		Paths: paths,
	}
	ckptRef := deriveCheckpointRef(snap.Segment.RoomID, segRef, checkpoint, segmentDigest, string(req.Intent))
	result.CheckpointRef = &ckptRef
	for _, p := range paths {
		result.PathSeal.PathIDs = append(result.PathSeal.PathIDs, p.PathID)
	}
	return result, nil
}

// terminalClose builds an explicit fail/abort terminal (no validation, no
// seals, no refs).
func terminalClose(snap Snapshot, req CloseRequest, state string) (CloseResult, error) {
	meta := closeMeta{Policy: snap.Segment.ClosePolicyRef, Key: req.CloseIDempotencyKey, Intent: string(req.Intent)}
	record, err := buildTerminalRecord(snap, meta, TerminalAudit{ReasonCode: req.ReasonCode, Phase: req.Phase, Detail: req.Detail}, state)
	if err != nil {
		return CloseResult{}, err
	}
	canonical, err := contract.JCS(record)
	if err != nil {
		if ce, ok := err.(*contract.CanonicalizationError); ok {
			return CloseResult{}, rejected(ce.ReasonCode, "terminal record not canonicalizable")
		}
		return CloseResult{}, err
	}
	return CloseResult{
		TerminalState:       state,
		SegmentDigest:       contract.DigestBytes(canonical),
		CanonicalBytes:      canonical,
		CanonicalByteLength: int64(len(canonical)),
		TerminalAudit:       &TerminalAudit{ReasonCode: req.ReasonCode, Phase: req.Phase, Detail: req.Detail},
	}, nil
}

// terminalAuditClose freezes a failed terminal for a deterministic integrity
// rejection: audit only, zero refs (Host 3.5/3.8).
func terminalAuditClose(snap Snapshot, req CloseRequest, cause error) (CloseResult, error) {
	code := ReasonOf(cause)
	if code == "" {
		code = "CANONICALIZATION_FAILED"
	}
	meta := closeMeta{Policy: snap.Segment.ClosePolicyRef, Key: req.CloseIDempotencyKey, Intent: string(IntentFail)}
	audit := TerminalAudit{ReasonCode: code, Phase: "close_integrity_check", Detail: cause.Error()}
	record, err := buildTerminalRecord(snap, meta, audit, StateFailed)
	if err != nil {
		return CloseResult{}, err
	}
	canonical, err := contract.JCS(record)
	if err != nil {
		if ce, ok := err.(*contract.CanonicalizationError); ok {
			return CloseResult{}, rejected(ce.ReasonCode, "terminal record not canonicalizable")
		}
		return CloseResult{}, err
	}
	return CloseResult{
		TerminalState:       StateFailed,
		SegmentDigest:       contract.DigestBytes(canonical),
		CanonicalBytes:      canonical,
		CanonicalByteLength: int64(len(canonical)),
		TerminalAudit:       &audit,
	}, nil
}

func canonicalFailure(err error) error {
	if ce, ok := err.(*contract.CanonicalizationError); ok {
		return rejected(ce.ReasonCode, "seal body not canonicalizable: "+ce.ReasonCode)
	}
	return rejected("CANONICALIZATION_FAILED", err.Error())
}

// ---------------------------------------------------------------------------
// Frozen-input validation (fail-closed, Host 3.5 preconditions)
// ---------------------------------------------------------------------------

var eventKinds = map[string]bool{
	"agent_message": true, "model_response": true, "tool_call": true,
	"tool_result": true, "delivery_transition": true, "decision_checkpoint": true,
	"replay_dispatch": true, "replay_completion": true,
}

var linkKinds = map[string]bool{
	"room_sequence": true, "response_to": true, "caused_by": true,
	"tool_call_to_result": true, "delivery_of": true, "segment_membership": true,
	"replay_of": true,
}

// validateFrozenInput checks the settle preconditions in the frozen corpus
// order: member integrity, tool execution completeness, delivery
// terminality, DAG acyclicity/causal completeness, ToolProxyResult digests,
// frontier/membership freeze.
func validateFrozenInput(snap Snapshot) error {
	members := snap.Members
	byID := make(map[string]*Member, len(members))
	for i := range members {
		m := &members[i]
		if _, dup := byID[m.EventID]; dup {
			return rejected("DUPLICATE_EVENT_ID", m.EventID)
		}
		byID[m.EventID] = m
		if !eventKinds[m.EventKind] {
			return rejected("UNKNOWN_EVENT_KIND", m.EventKind)
		}
		if digest, err := contract.DigestOf(m.Payload); err != nil || digest != m.PayloadDigest {
			return rejected("PAYLOAD_DIGEST_MISMATCH", m.EventID)
		}
	}

	// Every tool call must have exactly one execution record with a unique
	// tool result; a tool call reaching close without a terminal tool result
	// is the frozen MISSING_TOOL_RESULT integrity failure.
	calls := map[string]int{}
	for _, e := range snap.Executions {
		calls[e.ToolCallEvent]++
		call, ok := byID[e.ToolCallEvent]
		if !ok || call.EventKind != "tool_call" {
			return rejected("TOOL_EXECUTION_INVALID", "tool_call_event "+e.ToolCallEvent)
		}
		result, ok := byID[e.ToolResultEvent]
		if !ok || result.EventKind != "tool_result" {
			return rejected("TOOL_EXECUTION_INVALID", "tool_result_event "+e.ToolResultEvent)
		}
		if _, known := deliveryByID(snap.Deliveries, e.DeliveryID); !known {
			return rejected("TOOL_EXECUTION_INVALID", "unknown delivery "+e.DeliveryID)
		}
	}
	for _, m := range members {
		if m.EventKind == "tool_call" && calls[m.EventID] != 1 {
			return rejected("MISSING_TOOL_RESULT",
				"tool call "+m.EventID+" reached close without exactly one terminal tool result")
		}
	}

	// All deliveries must be terminal before the frontier freezes.
	seenDeliveries := map[string]bool{}
	for _, d := range snap.Deliveries {
		if seenDeliveries[d.DeliveryID] {
			return rejected("DUPLICATE_DELIVERY_ID", d.DeliveryID)
		}
		seenDeliveries[d.DeliveryID] = true
		if !terminalDeliveryStates[d.TerminalState] {
			return rejected("NONTERMINAL_TOOL", d.DeliveryID+" state "+d.TerminalState)
		}
	}

	if err := validateDAG(snap, byID); err != nil {
		return err
	}

	for i := range snap.Executions {
		if err := verifyToolProxyResult(snap.Executions[i]); err != nil {
			return err
		}
	}

	// Frontier/membership freeze: sequences strictly increase inside the
	// frozen interval and end exactly at the frozen head.
	var lastSeq, lastTick int64
	for i, m := range members {
		if i > 0 {
			if m.RoomSequence <= lastSeq {
				return rejected("MEMBER_ORDER_INVALID", m.EventID)
			}
			if m.LogicalTick <= lastTick {
				return rejected("LOGICAL_CLOCK_INVALID", m.EventID)
			}
		}
		if m.RoomSequence <= snap.Segment.FrontierStart || m.RoomSequence > snap.HeadSequence {
			return rejected("APPEND_AFTER_SETTLED",
				"member "+m.EventID+" room sequence outside the frozen frontier")
		}
		lastSeq, lastTick = m.RoomSequence, m.LogicalTick
	}
	if len(members) > 0 && members[len(members)-1].RoomSequence != snap.HeadSequence {
		return rejected("FRONTIER_MISMATCH", "frontier end must equal the last member sequence")
	}
	return nil
}

func deliveryByID(deliveries []Delivery, id string) (Delivery, bool) {
	for _, d := range deliveries {
		if d.DeliveryID == id {
			return d, true
		}
	}
	return Delivery{}, false
}

// validateDAG ports the frozen DAG rules: link endpoints resolve inside the
// frozen membership, the room-sequence chain connects consecutive members,
// per-kind causal requirements hold and the causal graph is acyclic.
func validateDAG(snap Snapshot, byID map[string]*Member) error {
	members := snap.Members
	links := snap.Links

	type edge struct{ from, to, kind string }
	linkIDs := map[string]bool{}
	var edges []edge
	for _, l := range links {
		if linkIDs[l.LinkID] {
			return rejected("DUPLICATE_LINK_ID", l.LinkID)
		}
		linkIDs[l.LinkID] = true
		if _, ok := byID[l.FromEvent]; !ok {
			return rejected("MISSING_CAUSAL_LINK", l.LinkID+" from_event not in frozen membership")
		}
		if _, ok := byID[l.ToEvent]; !ok {
			return rejected("MISSING_CAUSAL_LINK", l.LinkID+" to_event not in frozen membership")
		}
		if !linkKinds[l.LinkKind] {
			return rejected("UNKNOWN_LINK_KIND", l.LinkKind)
		}
		edges = append(edges, edge{l.FromEvent, l.ToEvent, l.LinkKind})
	}

	ordered := append([]Member(nil), members...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].RoomSequence < ordered[j].RoomSequence })
	chain := map[string]bool{}
	for _, e := range edges {
		if e.kind == "room_sequence" {
			chain[e.from+"\x00"+e.to] = true
		}
	}
	for i := 1; i < len(ordered); i++ {
		if !chain[ordered[i-1].EventID+"\x00"+ordered[i].EventID] {
			return rejected("MISSING_CAUSAL_LINK",
				"room_sequence gap "+ordered[i-1].EventID+"->"+ordered[i].EventID)
		}
	}

	outgoing := map[string]map[string]int{}
	incoming := map[string]map[string]int{}
	adjacency := map[string][]string{}
	for _, e := range edges {
		if outgoing[e.from] == nil {
			outgoing[e.from] = map[string]int{}
		}
		outgoing[e.from][e.kind]++
		if incoming[e.to] == nil {
			incoming[e.to] = map[string]int{}
		}
		incoming[e.to][e.kind]++
		adjacency[e.from] = append(adjacency[e.from], e.to)
	}
	for node := range adjacency {
		sort.Strings(adjacency[node])
	}
	for _, m := range ordered {
		inK, outK := incoming[m.EventID], outgoing[m.EventID]
		switch m.EventKind {
		case "model_response", "decision_checkpoint":
			if inK["response_to"] < 1 {
				return rejected("MISSING_CAUSAL_LINK", m.EventID+" lacks response_to")
			}
		case "tool_call":
			if outK["tool_call_to_result"] != 1 {
				return rejected("MISSING_CAUSAL_LINK", m.EventID+" must have exactly one tool result")
			}
		case "tool_result":
			if inK["tool_call_to_result"] != 1 {
				return rejected("MISSING_CAUSAL_LINK", m.EventID+" lacks tool_call_to_result")
			}
		case "delivery_transition":
			if inK["delivery_of"] != 1 {
				return rejected("MISSING_CAUSAL_LINK", m.EventID+" lacks delivery_of")
			}
		case "replay_dispatch":
			if outK["replay_of"] < 1 {
				return rejected("MISSING_CAUSAL_LINK", m.EventID+" lacks replay_of")
			}
		case "replay_completion":
			if inK["replay_of"] != 1 {
				return rejected("MISSING_CAUSAL_LINK", m.EventID+" lacks replay_of")
			}
		}
	}

	// Cycle detection: iterative three-color DFS in deterministic id order.
	const white, grey, black = 0, 1, 2
	color := map[string]int{}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, start := range ids {
		if color[start] != white {
			continue
		}
		stack := []string{start}
		iterIdx := map[string]int{}
		color[start] = grey
		for len(stack) > 0 {
			node := stack[len(stack)-1]
			neighbours := adjacency[node]
			advanced := false
			for iterIdx[node] < len(neighbours) {
				next := neighbours[iterIdx[node]]
				iterIdx[node]++
				if color[next] == grey {
					return rejected("DAG_CYCLE", "cycle through "+next)
				}
				if color[next] == white {
					color[next] = grey
					stack = append(stack, next)
					advanced = true
					break
				}
			}
			if !advanced {
				color[node] = black
				stack = stack[:len(stack)-1]
			}
		}
	}
	return nil
}

// verifyToolProxyResult verifies the exact ToolProxyResult digest protocol:
// proxy_result_digest covers the JCS of the result minus itself; succeeded
// results carry an upstream payload covered by upstream_result_digest;
// failed/inconclusive results carry an upstream attempt record referenced
// from error.record_refs.
func verifyToolProxyResult(e ToolExecution) error {
	tpr := e.ToolProxyResult
	if tpr == nil {
		return rejected("TOOL_PROXY_DIGEST_MISMATCH", "missing ToolProxyResult")
	}
	schema, _ := contract.StringOf(tpr, "schema_version")
	if schema != schemaToolProxyResult {
		return rejected("TOOL_PROXY_DIGEST_MISMATCH", "unexpected ToolProxyResult schema_version")
	}
	declared, _ := contract.StringOf(tpr, "proxy_result_digest")
	if !digestPattern.MatchString(declared) {
		return rejected("TOOL_PROXY_DIGEST_MISMATCH", "proxy_result_digest malformed")
	}
	preimage := contract.NewObject()
	for _, key := range tpr.Keys() {
		if key == "proxy_result_digest" {
			continue
		}
		v, _ := tpr.Get(key)
		preimage.Set(key, v)
	}
	digest, err := contract.DigestOf(preimage)
	if err != nil || digest != declared {
		return rejected("TOOL_PROXY_DIGEST_MISMATCH", "proxy_result_digest preimage mismatch")
	}
	status, _ := contract.StringOf(tpr, "status")
	upstream, _ := contract.StringOf(tpr, "upstream_result_digest")
	switch status {
	case "succeeded":
		resultVal, ok := tpr.Get("result")
		if !ok {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "missing result")
		}
		resultDigest, err := contract.DigestOf(resultVal)
		if err != nil || resultDigest != upstream {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "upstream_result_digest must cover the exact upstream payload")
		}
	case "failed", "inconclusive":
		errVal, ok := tpr.Get("error")
		if !ok {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "missing error")
		}
		errObj, ok := errVal.(*contract.Object)
		if !ok {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "error must be an object")
		}
		for _, field := range []string{"reason_code", "message", "retryable"} {
			if _, ok := errObj.Get(field); !ok {
				return rejected("TOOL_PROXY_DIGEST_MISMATCH", "error."+field)
			}
		}
		attempt := e.UpstreamAttemptRecord
		if attempt == nil {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "failed result needs an upstream attempt record")
		}
		attemptDigest, err := contract.DigestOf(attempt)
		if err != nil || attemptDigest != upstream {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "upstream_result_digest must cover the upstream attempt record")
		}
		refsVal, ok := errObj.Get("record_refs")
		if !ok {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "error.record_refs")
		}
		refs, ok := refsVal.(contract.Array)
		if !ok || len(refs) != 1 {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "error.record_refs must reference the attempt record")
		}
		ref, ok := refs[0].(*contract.Object)
		if !ok {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "error.record_refs[0]")
		}
		refID, _ := contract.StringOf(ref, "id")
		attemptID, _ := contract.StringOf(attempt, "attempt_id")
		refDigest, _ := contract.StringOf(ref, "digest")
		if refID != attemptID || refDigest != attemptDigest {
			return rejected("TOOL_PROXY_DIGEST_MISMATCH", "error.record_refs must reference the attempt record")
		}
	default:
		return rejected("TOOL_PROXY_STATUS_INVALID", status)
	}
	return nil
}

// checkpointIdentityOf derives the terminal Decision Checkpoint identity
// from the frozen decision_checkpoint member payload (digest-free identity;
// Host 3.6).
func checkpointIdentityOf(snap Snapshot) (checkpointIdentity, error) {
	var last *Member
	for i := range snap.Members {
		if snap.Members[i].EventKind == "decision_checkpoint" {
			last = &snap.Members[i]
		}
	}
	if last == nil {
		return checkpointIdentity{}, rejected("MISSING_CAUSAL_LINK", "no decision_checkpoint member in frozen membership")
	}
	id, _ := contract.StringOf(last.Payload, "checkpoint_id")
	seqVal, ok := last.Payload.Get("checkpoint_sequence")
	if !ok || id == "" {
		return checkpointIdentity{}, rejected("SCHEMA_REQUIRED_FIELD_MISSING", "decision_checkpoint payload identity")
	}
	seqNum, ok := seqVal.(contract.Number)
	if !ok {
		return checkpointIdentity{}, rejected("SCHEMA_REQUIRED_FIELD_MISSING", "checkpoint_sequence must be an integer")
	}
	bi, ok := seqNum.Int()
	if !ok {
		return checkpointIdentity{}, rejected("NON_INTEGER_NUMBER", "checkpoint_sequence")
	}
	return checkpointIdentity{ID: id, Sequence: bi.Int64()}, nil
}

// inputDigestOf digests the frozen close input identity (intent + frozen
// membership/links/deliveries/executions + frontier head). The same close
// idempotency key with a different input digest is an idempotency conflict.
func inputDigestOf(snap Snapshot, intent CloseIntent) (string, error) {
	obj := contract.NewObject()
	obj.Set("segment_id", contract.String(snap.Segment.SegmentID))
	obj.Set("close_intent", contract.String(string(intent)))
	obj.Set("frontier_end", number(snap.HeadSequence))
	members := contract.Array{}
	for _, m := range snap.Members {
		entry := contract.NewObject()
		entry.Set("event_id", contract.String(m.EventID))
		entry.Set("payload_digest", contract.String(m.PayloadDigest))
		members = append(members, entry)
	}
	obj.Set("members", members)
	linkIDs := contract.Array{}
	for _, l := range snap.Links {
		linkIDs = append(linkIDs, contract.String(l.LinkID))
	}
	obj.Set("causal_links", linkIDs)
	deliveries := contract.Array{}
	for _, d := range snap.Deliveries {
		entry := contract.NewObject()
		entry.Set("delivery_id", contract.String(d.DeliveryID))
		entry.Set("terminal_state", contract.String(d.TerminalState))
		deliveries = append(deliveries, entry)
	}
	obj.Set("deliveries", deliveries)
	executions := contract.Array{}
	for _, e := range snap.Executions {
		entry := contract.NewObject()
		entry.Set("tool_call_event", contract.String(e.ToolCallEvent))
		entry.Set("tool_result_event", contract.String(e.ToolResultEvent))
		if digest, err := contract.DigestOf(e.ToolProxyResult); err == nil {
			entry.Set("tool_proxy_result_digest", contract.String(digest))
		}
		executions = append(executions, entry)
	}
	obj.Set("tool_executions", executions)
	return contract.DigestOf(obj)
}
