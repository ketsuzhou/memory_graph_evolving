// service.go is the GMS-205 Runtime projector service (GMS §8–§9, §12.5/
// §12.9/§12.13–§12.14; Contract §5.3–§5.4, §9.3, §11, §13.6).
//
// The projector is the ONLY writer of the Runtime Skill Graph. It consumes
// the authoritative sources by continuous cursors — the single activation
// ledger (which doubles as the artifact-release source) plus the two
// relation sources (similarity assessments, evidence/claim assessments) —
// and commits each batch as ONE projection-storage transaction: Graph
// mutations, cursor vector, ledger-prefix commitments, head CAS and the
// Contract §7.14 watermark, atomically (GMS §8.5). The Graph is derived
// and rebuildable; the activation ledger and the active-head table remain
// the sole runtime authority (Contract §5.3.1).
package projector

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Reason codes (every code names an entry of the frozen system registry,
// Contract §13.7.1; aliases reuse the single frozen source)
// ---------------------------------------------------------------------------

const (
	// ReasonProjectionSequenceGap: the projector consumes contiguous
	// activation sequences; a gap blocks the watermark (Contract §9.3).
	ReasonProjectionSequenceGap = contract.ReasonProjectionSequenceGap
	// ReasonProjectionEventConflict: same activation_sequence with a
	// different event_digest (Contract §9.3).
	ReasonProjectionEventConflict = contract.ReasonProjectionEventConflict
	// ReasonProjectionSourceGap: a relation source skipped a record.
	ReasonProjectionSourceGap = "PROJECTION_SOURCE_GAP"
	// ReasonProjectionSourceConflict: a relation source re-delivered one
	// sequence with a different digest.
	ReasonProjectionSourceConflict = "PROJECTION_SOURCE_CONFLICT"
	// ReasonProjectionSchemaUnsupported: an unknown/unsupported projection
	// or payload schema (fail closed, Contract §13.6).
	ReasonProjectionSchemaUnsupported = "PROJECTION_SCHEMA_UNSUPPORTED"
	// ReasonProjectionRelationInvalid: a relation rule violation — unknown
	// relation, missing/floating endpoint, invented provenance (GMS §9.2).
	ReasonProjectionRelationInvalid = "PROJECTION_RELATION_INVALID"
	// ReasonProjectionBlocked: the projection stream is blocked (Contract
	// §9.3; diagnostic for the blocked state).
	ReasonProjectionBlocked = "PROJECTION_BLOCKED"
	// ReasonProjectionRebuildRequired: a rebuild is required (repair path).
	ReasonProjectionRebuildRequired = "PROJECTION_REBUILD_REQUIRED"
	// ReasonProjectionBehindRequiredSequence: the watermark would run ahead
	// of the authority (fail-closed list "watermark 超前").
	ReasonProjectionBehindRequiredSequence = "PROJECTION_BEHIND_REQUIRED_SEQUENCE"
	// ReasonEvidenceNotCommitted: an assessment endpoint does not resolve
	// to committed evidence (GMS §9.2).
	ReasonEvidenceNotCommitted = contract.ReasonEvidenceNotCommitted
	// ReasonCompositeCycle: composes closure cycles (GMS §9.3).
	ReasonCompositeCycle = contract.ReasonCompositeCycle
	// ReasonDependencyCycle: depends_on closure cycles (GMS §9.3).
	ReasonDependencyCycle = "DEPENDENCY_CYCLE"
	// ReasonProbationUnsupported: probation stays reserved and unreachable
	// (Contract §9.2.6).
	ReasonProbationUnsupported = "PROBATION_UNSUPPORTED"
)

// Aliases of codes frozen in the ledger/contract layers (single source).
const (
	ReasonProjectionHeadConflict     = ledger.ReasonProjectionHeadConflict
	ReasonDigestMismatch             = ledger.ReasonDigestMismatch
	ReasonRefMismatch                = ledger.ReasonRefMismatch
	ReasonSchemaEnumInvalid          = ledger.ReasonSchemaEnumInvalid
	ReasonSchemaFieldUnknown         = ledger.ReasonSchemaFieldUnknown
	ReasonSchemaRequiredFieldMissing = ledger.ReasonRequiredFieldMissing
)

// Error is the registry-code projector failure; Message is diagnostic only
// and never drives behavior.
type Error struct {
	ReasonCode string
	Message    string
}

func (e *Error) Error() string { return "projector: " + e.ReasonCode + ": " + e.Message }

// newError builds a projector error after verifying the code against the
// closed registry; an unregistered code fails as a wiring error instead of
// leaking an invented behavior code.
func newError(registry ledger.ReasonRegistry, code, format string, args ...any) error {
	if registry != nil {
		if err := registry.Verify(code); err != nil {
			return fmt.Errorf("projector: reason code %q is outside the closed registry: %w", code, err)
		}
	}
	return &Error{ReasonCode: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err ("" when nil); ledger,
// validation and contract-schema codes surface unchanged.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	var pe *Error
	if errors.As(err, &pe) {
		return pe.ReasonCode
	}
	if code := ledger.ReasonOf(err); code != "" {
		return code
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	return ""
}

var errNilDependency = errors.New("projector: nil dependency in projector config")

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

// SourceRecord is one committed record of a relation source: its sequence
// within the source stream, its record id, its canonical-record digest and
// the canonical bytes themselves.
type SourceRecord struct {
	Sequence  uint64
	RecordID  string
	Digest    string
	Canonical []byte
}

// RecordSource enumerates the committed records of one relation source
// after a cursor. Records MUST come back ordered by sequence and
// contiguous from after+1 (a gap or a re-delivered sequence with a
// different digest blocks the projection fail closed). The similarity
// ledger stores per-assessment streams that cannot be enumerated by the
// memory adapter, so v1 feeds this port from the producer side; a durable
// adapter MAY back it with a single enumerable projection stream.
type RecordSource interface {
	SourceName() string
	Records(after uint64) ([]SourceRecord, error)
}

// EvidenceResolver resolves one committed evidence ref by id (the same
// port the merge and candidate services consume).
type EvidenceResolver interface {
	GetEvidence(id string) (contract.EvidenceRef, bool, error)
}

// DeliveredEvent is one activation event handed to the projector by the
// delivery worker: its global activation sequence, event id, payload
// digest and canonical payload bytes.
type DeliveredEvent struct {
	Sequence      uint64
	EventID       string
	PayloadDigest string
	Payload       []byte
}

// BlockedInfo is the frozen blocking reason of a blocked projection head
// (Contract §9.3: blocked stays diagnosable; the reason code is the
// registry code, the message diagnostic only).
type BlockedInfo struct {
	Code     string
	Message  string
	Source   string
	Sequence uint64
}

// ProjectionResult reports the committed projection head after one call.
type ProjectionResult struct {
	State           string
	Cursors         CursorVector
	Projected       int // activation events newly projected by this call
	Watermark       map[string]any
	WatermarkDigest string
	Blocked         *BlockedInfo
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

// Config wires the Runtime projector.
type Config struct {
	Store    ledger.Store
	Tx       ledger.TxManager
	Registry ledger.ReasonRegistry
	Gates    *validation.Gates
	// Graph overrides the projection storage handle (tests share one).
	Graph *Graph
	// Storage overrides the projection-storage transaction adapter.
	Storage Storage
	// Similarity feeds the similar_to relation source (nil = none).
	Similarity RecordSource
	// Assessments feeds the evidence/claim assessment source (nil = none).
	Assessments RecordSource
	// Evidence resolves committed evidence for claim assessments
	// (nil blocks every assessment fail closed).
	Evidence EvidenceResolver
}

// Service is the Runtime projection service. Every projection call is
// serialized; concurrency safety of the derived state itself is the
// Graph/storage transaction's contract (head CAS, GMS §8.5).
type Service struct {
	store       ledger.Store
	mgr         ledger.TxManager
	registry    ledger.ReasonRegistry
	gates       *validation.Gates
	graph       *Graph
	storage     Storage
	similarity  RecordSource
	assessments RecordSource
	evidence    EvidenceResolver
	projectMu   sync.Mutex
}

// NewService fails closed at construction time unless every dependency is
// wired and every reason code the projector can emit exists in the
// digest-verified registry (Contract §13.7.1 R5).
func NewService(cfg Config) (*Service, error) {
	if cfg.Store == nil || cfg.Tx == nil || cfg.Registry == nil || cfg.Gates == nil {
		return nil, errNilDependency
	}
	for _, code := range []string{
		ReasonProjectionSequenceGap, ReasonProjectionEventConflict,
		ReasonProjectionSourceGap, ReasonProjectionSourceConflict,
		ReasonProjectionSchemaUnsupported, ReasonProjectionRelationInvalid,
		ReasonProjectionBlocked, ReasonProjectionRebuildRequired,
		ReasonProjectionBehindRequiredSequence, ReasonEvidenceNotCommitted,
		ReasonCompositeCycle, ReasonDependencyCycle, ReasonProbationUnsupported,
		ReasonProjectionHeadConflict, ReasonDigestMismatch, ReasonRefMismatch,
		ReasonSchemaEnumInvalid, ReasonSchemaFieldUnknown,
		ReasonSchemaRequiredFieldMissing, ledger.ReasonInvalidJSON,
		ledger.ReasonActivationEventInvalid,
	} {
		if err := cfg.Registry.Verify(code); err != nil {
			return nil, fmt.Errorf("projector: %w", err)
		}
	}
	graph := cfg.Graph
	if graph == nil {
		graph = NewGraph()
	}
	storage := cfg.Storage
	if storage == nil {
		graphStorage, err := NewGraphStorage(graph, cfg.Store)
		if err != nil {
			return nil, err
		}
		storage = graphStorage
	}
	return &Service{
		store:       cfg.Store,
		mgr:         cfg.Tx,
		registry:    cfg.Registry,
		gates:       cfg.Gates,
		graph:       graph,
		storage:     storage,
		similarity:  cfg.Similarity,
		assessments: cfg.Assessments,
		evidence:    cfg.Evidence,
	}, nil
}

// GraphHandle exposes the derived Runtime Graph (read side).
func (s *Service) GraphHandle() *Graph { return s.graph }

// Snapshot returns a deep copy of the current derived Runtime Graph.
func (s *Service) Snapshot() GraphSnapshot { return s.graph.Snapshot() }

// GraphDigest renders the canonical digest of the derived content; a
// from-zero rebuild MUST reproduce it (completion criterion 从零重建相同).
func (s *Service) GraphDigest() string { return s.graph.GraphDigest() }

// Watermark returns the current Contract §7.14 watermark document, its
// digest and the projector state.
func (s *Service) Watermark() (map[string]any, string, string) {
	state := s.graph.stateOf()
	return deepCopyDoc(state.watermark), state.watermarkDigest, state.state
}

// PriorWatermark returns the read-only old-head watermark after a rebuild
// swap (nil before the first rebuild; GMS §8.6).
func (s *Service) PriorWatermark() map[string]any {
	return deepCopyDoc(s.graph.stateOf().priorWatermark)
}

// Blocked returns the frozen blocking reason when the head is blocked.
func (s *Service) Blocked() *BlockedInfo {
	state := s.graph.stateOf()
	if state.blocked == nil {
		return nil
	}
	blocked := *state.blocked
	return &blocked
}

// pendingActivation is one activation event staged for projection.
type pendingActivation struct {
	sequence uint64
	eventID  string
	digest   string
	payload  []byte
}

// ProjectRuntime drains the authoritative sources by cursor and commits the
// next projection batch atomically (GMS §8.5): the activation ledger from
// activation cursor+1 through its head, plus every relation record after
// the relation cursors. The batch is all-or-nothing; a gap, a same-slot
// digest conflict or any rule violation commits a state-only blocked
// watermark (no Graph mutation) and returns the blocking error.
func (s *Service) ProjectRuntime(ctx context.Context) (*ProjectionResult, error) {
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	current := s.graph.stateOf()

	entries, err := s.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		return nil, err
	}
	// Below-cursor consistency: every consumed slot must still digest to
	// what was projected (a rewritten authority history blocks); the rest
	// of the ledger is the next contiguous batch.
	var batch []pendingActivation
	for _, entry := range entries {
		if entry.Sequence <= current.cursors.Activation {
			if consumed, ok := current.consumed[SourceActivation][entry.Sequence]; ok && consumed != entry.PayloadDigest {
				return s.commitBlocked(current, SourceActivation, entry.Sequence, ReasonProjectionEventConflict,
					"activation sequence %d now digests to %s but was projected from %s; the authority history diverged from the watermark (Contract §9.3)",
					entry.Sequence, entry.PayloadDigest, consumed)
			}
			continue
		}
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil {
			return nil, err
		}
		if !ok {
			return s.commitBlocked(current, SourceActivation, entry.Sequence, ReasonDigestMismatch,
				"activation event %d payload %s resolves to no committed content", entry.Sequence, entry.PayloadDigest)
		}
		batch = append(batch, pendingActivation{sequence: entry.Sequence, eventID: entry.EventID, digest: entry.PayloadDigest, payload: payload})
	}
	return s.commitBatch(current, batch)
}

// ProjectDelivered projects one explicitly delivered activation batch (the
// transactional-outbox delivery path, GMS §2.6). Fail-closed guard order:
//
//  1. same activation_sequence with a different event_digest inside the
//     batch is PROJECTION_EVENT_CONFLICT (event-neg-002; duplicates of the
//     identical event dedupe — event-003);
//  2. a re-delivered consumed slot with a different digest is
//     PROJECTION_EVENT_CONFLICT;
//  3. every delivered event MUST resolve to the committed activation
//     ledger entry at its exact sequence (id, digest and payload bytes) —
//     the watermark can never run ahead of the authority
//     (ACTIVATION_EVENT_INVALID);
//  4. the batch MUST be contiguous from activation cursor+1
//     (PROJECTION_SEQUENCE_GAP, event-neg-001);
//  5. each event re-parses strictly and folds through the relation rules.
func (s *Service) ProjectDelivered(ctx context.Context, events []DeliveredEvent) (*ProjectionResult, error) {
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	current := s.graph.stateOf()

	seen := map[uint64]string{}
	deduped := make([]DeliveredEvent, 0, len(events))
	for _, event := range events {
		if previous, ok := seen[event.Sequence]; ok {
			if previous != event.PayloadDigest {
				return s.commitBlocked(current, SourceActivation, event.Sequence, ReasonProjectionEventConflict,
					"activation sequence %d delivered twice with different event digests (%s, %s); Contract §9.3 same-sequence conflict",
					event.Sequence, previous, event.PayloadDigest)
			}
			continue // identical duplicate: idempotent (Contract §16.4 #10)
		}
		seen[event.Sequence] = event.PayloadDigest
		deduped = append(deduped, event)
	}
	sortDelivered(deduped)

	var batch []pendingActivation
	for _, event := range deduped {
		if event.Sequence <= current.cursors.Activation {
			if consumed, ok := current.consumed[SourceActivation][event.Sequence]; ok && consumed == event.PayloadDigest {
				continue // already projected: duplicate delivery is a no-op
			}
			return s.commitBlocked(current, SourceActivation, event.Sequence, ReasonProjectionEventConflict,
				"activation sequence %d re-delivered with digest %s over the projected slot (cursor %d)",
				event.Sequence, event.PayloadDigest, current.cursors.Activation)
		}
		entry, ok, err := s.store.EntryAt(ledger.LedgerActivation, "", event.Sequence)
		if err != nil {
			return nil, err
		}
		if !ok || entry.EventID != event.EventID || entry.PayloadDigest != event.PayloadDigest {
			return s.commitBlocked(current, SourceActivation, event.Sequence, ledger.ReasonActivationEventInvalid,
				"delivered event %d (%s, %s) does not resolve to the committed activation ledger entry; the watermark may never run ahead of the authority (GMS §2.6)",
				event.Sequence, event.EventID, event.PayloadDigest)
		}
		if contract.DigestBytes(event.Payload) != event.PayloadDigest {
			return s.commitBlocked(current, SourceActivation, event.Sequence, ReasonDigestMismatch,
				"delivered event %d payload digests to %s but declares %s", event.Sequence, contract.DigestBytes(event.Payload), event.PayloadDigest)
		}
		batch = append(batch, pendingActivation{sequence: event.Sequence, eventID: event.EventID, digest: event.PayloadDigest, payload: event.Payload})
	}
	return s.commitBatch(current, batch)
}

// commitBatch folds one contiguous activation batch plus every pending
// relation record through the rules and commits the whole unit atomically
// (GMS §8.5). Any failure blocks the head state-only and returns the
// blocking error; nothing is mutated.
func (s *Service) commitBatch(current *graphState, batch []pendingActivation) (*ProjectionResult, error) {
	// Contiguity: the projector consumes continuous source cursors.
	expected := current.cursors.Activation + 1
	for _, item := range batch {
		if item.sequence != expected {
			return s.commitBlocked(current, SourceActivation, item.sequence, ReasonProjectionSequenceGap,
				"activation sequences must be contiguous: the cursor is at %d but the next delivered sequence is %d (gap at %d, Contract §9.3)",
				current.cursors.Activation, item.sequence, expected)
		}
		expected++
	}

	// Nothing new: an idempotent no-op that commits nothing (duplicate
	// delivery, empty ledger) — the current head stays exactly as it is.
	if len(batch) == 0 && !s.relationPending(current.cursors) {
		return &ProjectionResult{
			State:           current.state,
			Cursors:         current.cursors,
			Watermark:       deepCopyDoc(current.watermark),
			WatermarkDigest: current.watermarkDigest,
			Blocked:         cloneBlocked(current.blocked),
		}, nil
	}

	staged := current.clone()
	var mutations []Mutation
	prefixes := current.prefixes
	cursors := current.cursors

	// Sequential fold: every event sees the effects of its predecessors.
	for _, item := range batch {
		view, err := s.parseActivationEvent(item.sequence, item.eventID, item.payload, item.digest)
		if err != nil {
			return s.commitBlockedErr(current, SourceActivation, item.sequence, err)
		}
		eventMutations, err := s.activationMutations(staged, view)
		if err != nil {
			return s.commitBlockedErr(current, SourceActivation, item.sequence, err)
		}
		for _, mutation := range eventMutations {
			staged.apply(mutation)
			mutations = append(mutations, mutation)
		}
		if prefixes.Activation, err = foldPrefix(prefixes.Activation, item.sequence, item.eventID, item.digest); err != nil {
			return nil, newError(s.registry, ReasonDigestMismatch, "activation prefix fold: %v", err)
		}
		cursors.Activation = item.sequence
	}

	// Relation sources fold after the activation batch of the same unit.
	simCursor, simPrefix, err := s.foldRelationSource(staged, s.similarity, SourceSimilarity, cursors.Similarity, prefixes.Similarity, &mutations)
	if err != nil {
		return s.commitBlockedErr(current, "", 0, err)
	}
	assessCursor, assessPrefix, err := s.foldRelationSource(staged, s.assessments, SourceEvidenceAssessment, cursors.EvidenceAssessment, prefixes.EvidenceAssessment, &mutations)
	if err != nil {
		return s.commitBlockedErr(current, "", 0, err)
	}
	cursors.Similarity, prefixes.Similarity = simCursor, simPrefix
	cursors.EvidenceAssessment, prefixes.EvidenceAssessment = assessCursor, assessPrefix

	state := StateCurrent
	if head, ok, err := s.store.Head(ledger.LedgerActivation, ""); err != nil {
		return nil, err
	} else if ok && head.Sequence > cursors.Activation {
		state = StateCatchingUp
	}
	return s.commitUnit(&CommitUnit{
		ExpectedHeadSeq:    current.headSeq,
		ExpectedHeadDigest: current.headDigest,
		Mutations:          mutations,
		Cursors:            cursors,
		Prefixes:           prefixes,
		State:              state,
	}, len(batch))
}

// foldRelationSource folds one relation source's pending records into the
// staged state: contiguity from cursor+1 (PROJECTION_SOURCE_GAP), no
// re-delivered slot (PROJECTION_SOURCE_CONFLICT), then the source's rule
// derivation. Returns the new cursor and prefix accumulator.
func (s *Service) foldRelationSource(staged *graphState, source RecordSource, name string, cursor uint64, prefix string, mutations *[]Mutation) (uint64, string, error) {
	if source == nil {
		return cursor, prefix, nil
	}
	records, err := source.Records(cursor)
	if err != nil {
		return 0, "", err
	}
	sortRecords(records)
	for _, record := range records {
		if record.Sequence <= cursor {
			return 0, "", newError(s.registry, ReasonProjectionSourceConflict,
				"source %s re-delivered sequence %d over the consumed cursor %d", name, record.Sequence, cursor)
		}
		if record.Sequence != cursor+1 {
			return 0, "", newError(s.registry, ReasonProjectionSourceGap,
				"source %s records must be contiguous: cursor %d but next record sequence is %d", name, cursor, record.Sequence)
		}
		var derived []Mutation
		if name == SourceSimilarity {
			derived, err = s.similarityMutations(staged, record)
		} else {
			derived, err = s.assessmentMutations(staged, record)
		}
		if err != nil {
			return 0, "", err
		}
		for _, mutation := range derived {
			staged.apply(mutation)
			*mutations = append(*mutations, mutation)
		}
		if prefix, err = foldPrefix(prefix, record.Sequence, record.RecordID, record.Digest); err != nil {
			return 0, "", newError(s.registry, ReasonDigestMismatch, "source %s prefix fold: %v", name, err)
		}
		cursor = record.Sequence
	}
	return cursor, prefix, nil
}

// relationPending reports whether any relation source holds records after
// its cursor (the no-op shortcut probe).
func (s *Service) relationPending(cursors CursorVector) bool {
	for _, probe := range []struct {
		source RecordSource
		cursor uint64
	}{
		{s.similarity, cursors.Similarity},
		{s.assessments, cursors.EvidenceAssessment},
	} {
		if probe.source == nil {
			continue
		}
		if records, err := probe.source.Records(probe.cursor); err == nil && len(records) > 0 {
			return true
		}
	}
	return false
}

// commitUnit builds the watermark of the new head, commits the unit through
// the projection-storage transaction and drains the outbox.
func (s *Service) commitUnit(unit *CommitUnit, projected int) (*ProjectionResult, error) {
	watermark, digest, err := buildWatermark(s.registry, unit.State, unit.Cursors, unit.Prefixes)
	if err != nil {
		return nil, err
	}
	// The committed watermark is always a valid authority instance (the
	// x-digest recomputation included).
	if err := s.gates.ValidateInstance(watermark, projectionWatermarkSchemaFile); err != nil {
		return nil, newError(s.registry, ReasonProjectionSchemaUnsupported,
			"constructed watermark failed the §7.14 authority schema: %v", err)
	}
	unit.Watermark = watermark
	unit.WatermarkDigest = digest
	if err := s.storage.CommitProjection(unit); err != nil {
		return nil, err
	}
	if err := s.drainOutbox(unit.Cursors.Activation); err != nil {
		return nil, err
	}
	return &ProjectionResult{
		State:           unit.State,
		Cursors:         unit.Cursors,
		Projected:       projected,
		Watermark:       deepCopyDoc(watermark),
		WatermarkDigest: digest,
	}, nil
}

// commitBlocked commits the state-only blocked head: no mutation, no cursor
// advance — only the watermark flips to state=blocked with the frozen
// blocking reason (Contract §9.3, §13.6). Returns the committed result AND
// the blocking error.
func (s *Service) commitBlocked(current *graphState, source string, sequence uint64, code, format string, args ...any) (*ProjectionResult, error) {
	blocking := newError(s.registry, code, format, args...)
	unit := &CommitUnit{
		ExpectedHeadSeq:    current.headSeq,
		ExpectedHeadDigest: current.headDigest,
		State:              StateBlocked,
		Cursors:            current.cursors,
		Prefixes:           current.prefixes,
		Blocked: &BlockedInfo{
			Code:     code,
			Message:  blocking.Error(),
			Source:   source,
			Sequence: sequence,
		},
	}
	result, err := s.commitUnit(unit, 0)
	if err != nil {
		return nil, err
	}
	return result, blocking
}

// commitBlockedErr blocks on an already-built rule/parse error, keeping its
// registry code.
func (s *Service) commitBlockedErr(current *graphState, source string, sequence uint64, blocking error) (*ProjectionResult, error) {
	code := CodeOf(blocking)
	if code == "" {
		code = ReasonProjectionBlocked
	}
	unit := &CommitUnit{
		ExpectedHeadSeq:    current.headSeq,
		ExpectedHeadDigest: current.headDigest,
		State:              StateBlocked,
		Cursors:            current.cursors,
		Prefixes:           current.prefixes,
		Blocked: &BlockedInfo{
			Code:     code,
			Message:  blocking.Error(),
			Source:   source,
			Sequence: sequence,
		},
	}
	result, err := s.commitUnit(unit, 0)
	if err != nil {
		return nil, err
	}
	return result, blocking
}

// drainOutbox completes the delivery of every pending runtime-targeted
// outbox record the projection now covers (GMS §2.6: delivery completion
// marks the record delivered; re-delivery never mints a new event).
func (s *Service) drainOutbox(through uint64) error {
	pending, err := s.store.Pending()
	if err != nil {
		return err
	}
	for _, record := range pending {
		if record.ActivationSequence > through || record.ProjectionTarget != StreamRuntime {
			continue
		}
		if err := s.store.MarkDelivered(record.OutboxKey, record.ActivationSequence, record.EventDigest); err != nil {
			return err
		}
	}
	return nil
}

func cloneBlocked(blocked *BlockedInfo) *BlockedInfo {
	if blocked == nil {
		return nil
	}
	out := *blocked
	return &out
}

func sortDelivered(events []DeliveredEvent) {
	for i := 1; i < len(events); i++ {
		for j := i; j > 0 && events[j].Sequence < events[j-1].Sequence; j-- {
			events[j], events[j-1] = events[j-1], events[j]
		}
	}
}

func sortRecords(records []SourceRecord) {
	for i := 1; i < len(records); i++ {
		for j := i; j > 0 && records[j].Sequence < records[j-1].Sequence; j-- {
			records[j], records[j-1] = records[j-1], records[j]
		}
	}
}
