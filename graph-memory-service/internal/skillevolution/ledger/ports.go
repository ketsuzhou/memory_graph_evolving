// Package ledger defines the immutable-ledger and atomic-transaction ports
// of the GMS skill-evolution write path (Contract §5.1, §9, §13.1–§13.5 and
// GMS Spec §2, §11.1).
//
// The ports replace the legacy illusion that "HTTP 200 followed by a whole-
// database snapshot" is a transaction. Every authoritative write goes through
// append-only ledgers, a content-addressed store, an idempotency store,
// exact compare-and-swap heads and an outbox, and every multi-write unit of
// work goes through TxManager.WithinTx, which is atomic: all staged writes
// commit together or none do (Contract §13.1, GMS §2.9).
//
// Layering (the GMS-102 refactor seam):
//   - ports.go     pure domain vocabulary: closed enums, record shapes and
//     storage port interfaces; imports no storage technology;
//   - transaction.go the domain transaction (Manager/Tx) that stages a write
//     set and applies it against any Store implementation;
//   - memory.go    the in-memory reference Store adapter. NOT production
//     durability; a durable adapter is a release gate.
//
// Reason codes: every port failure carries a reason code from the frozen
// system reason-code registry (Contract §13.7.1, digest-verified via
// internal/contract). No code outside the registry is ever emitted —
// newError verifies each code against ReasonRegistry before returning it,
// and TestMappedReasonCodesExistInRegistry pins the whole mapping.
package ledger

import (
	"context"
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
)

// ---------------------------------------------------------------------------
// Reason model (closed registry, Contract §13.7.1 / GMS §11.3–§11.5)
// ---------------------------------------------------------------------------

// The constants below NAME entries of the frozen system reason registry;
// they do not invent new codes. Where internal/contract already exports the
// name it is reused verbatim. Codes needed by these ports that the contract
// package does not export yet are named here and cite their GMS §11.4 group.
const (
	// ReasonIdempotencyConflict: same idempotency key with a different
	// canonical request digest (GMS §11.3 new-attempt set, §13.2).
	ReasonIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	// ReasonActiveHeadConflict: stale exact CAS on a lineage active head
	// (GMS §11.3 new-attempt set; Contract §9.2.2).
	ReasonActiveHeadConflict = "ACTIVE_HEAD_CONFLICT"
	// ReasonMergeSourceHeadStale: merge source active head changed under a
	// frozen expectation (GMS §11.4 merge group, M5).
	ReasonMergeSourceHeadStale = "MERGE_SOURCE_HEAD_STALE"
	// ReasonMergeGroupInFlight: proposal-group winner CAS already holds
	// another in-flight winner (GMS §2.7, Contract §5.5.8). Registry status
	// failure/none — the group must be drained by a new attempt.
	ReasonMergeGroupInFlight = "MERGE_GROUP_IN_FLIGHT"
	// ReasonLineageVersionConflict: lineage version assignment conflict
	// (GMS §2.3/§11.4).
	ReasonLineageVersionConflict = "LINEAGE_VERSION_CONFLICT"
	// ReasonProjectionHeadConflict: projection head/watermark CAS conflict
	// (GMS §11.3 new-attempt set).
	ReasonProjectionHeadConflict = "PROJECTION_HEAD_CONFLICT"
	// ReasonActivationSequenceConflict: activation sequence reuse/gap in the
	// single global stream (GMS §2.6, §11.4 activation group).
	ReasonActivationSequenceConflict = "ACTIVATION_SEQUENCE_CONFLICT"
	// ReasonProposalStateConflict: ordinary proposal state event conflict
	// (GMS §11.4 artifact/proposal/candidate group).
	ReasonProposalStateConflict = "PROPOSAL_STATE_CONFLICT"
	// ReasonMergeStateConflict: merge proposal state event conflict
	// (GMS §11.4 merge/similarity group).
	ReasonMergeStateConflict = "MERGE_STATE_CONFLICT"
	// ReasonActivationEventInvalid: outbox record does not resolve to the
	// activation event committed in the same transaction (GMS §2.6/§2.9;
	// closest canonical code for a partial outbox).
	ReasonActivationEventInvalid = "ACTIVATION_EVENT_INVALID"
)

// Aliases of codes already frozen in internal/contract (single source).
const (
	ReasonDigestMismatch           = contract.ReasonDigestMismatch
	ReasonRefMismatch              = contract.ReasonRefMismatch
	ReasonIllegalStateTransition   = contract.ReasonIllegalStateTransition
	ReasonSchemaEnumInvalid        = contract.ReasonSchemaEnumInvalid
	ReasonSchemaVersionUnsupported = contract.ReasonSchemaVersionUnsupported
	ReasonSchemaFieldUnknown       = contract.ReasonSchemaFieldUnknown
	ReasonRequiredFieldMissing     = contract.ReasonSchemaRequiredFieldMissin
	ReasonInvalidJSON              = contract.ReasonInvalidJSON
	ReasonNonIntegerNumber         = contract.ReasonNonIntegerNumber
)

// ReasonRegistry is the closed-registry gate every port error passes
// through (Contract §13.7.1 R5, GMS §11.5): Verify fails for any code
// outside the digest-verified registry, so behavior-driving codes can never
// be invented or guessed from message text.
type ReasonRegistry interface {
	Verify(code string) error
}

// Error is the fail-closed error type of every ledger port. ReasonCode is a
// closed-registry system reason code; Message is diagnostic only and never
// drives behavior.
type Error struct {
	ReasonCode string
	Message    string
}

func (e *Error) Error() string {
	return "ledger: " + e.ReasonCode + ": " + e.Message
}

// ReasonOf returns the closed-registry reason code of err, or "" when err is
// not a port error (e.g. an infrastructure error carrying no code).
func ReasonOf(err error) string {
	var portErr *Error
	if errors.As(err, &portErr) {
		return portErr.ReasonCode
	}
	return ""
}

// newError builds a port error after verifying the code against the
// registry. An unregistered code fails closed as a wiring error instead of
// leaking an invented behavior code.
func newError(registry ReasonRegistry, code, format string, args ...any) error {
	if registry != nil {
		if err := registry.Verify(code); err != nil {
			return fmt.Errorf("ledger: reason code %q is outside the closed registry: %w", code, err)
		}
	}
	return &Error{ReasonCode: code, Message: fmt.Sprintf(format, args...)}
}

// ---------------------------------------------------------------------------
// Ledger kinds (Contract §9/§13.1; GMS §2.2–§2.8)
// ---------------------------------------------------------------------------

// LedgerKind is the closed set of append-only ledger streams. Every kind
// names the GMS Spec §2 ledger it stands for; the mapping is the single
// place where ledger classification lives.
type LedgerKind string

const (
	// LedgerEvidence holds committed EvidenceRef records (GMS §2.2).
	LedgerEvidence LedgerKind = "evidence"
	// LedgerProposal holds Contract §7.9 SkillProposal records and §9.1
	// proposal state events (GMS §2.4).
	LedgerProposal LedgerKind = "proposal"
	// LedgerCandidate holds Contract §7.4 candidate bindings (GMS §2.4).
	LedgerCandidate LedgerKind = "candidate"
	// LedgerValidation holds validation records (GMS §2.4). No payload
	// schema is frozen yet, so the closed whitelist is empty: appends fail
	// closed until a frozen schema is registered (Contract §16.6).
	LedgerValidation LedgerKind = "validation"
	// LedgerReplay holds Contract §7.10/§7.11 replay requests and results
	// (GMS §2.5).
	LedgerReplay LedgerKind = "replay"
	// LedgerEvaluation holds Contract §7.12 release decisions (GMS §2.5).
	LedgerEvaluation LedgerKind = "evaluation"
	// LedgerRelease holds release version-assignment and Candidate→Released
	// mapping records (GMS §2.5/§2.9). Empty payload whitelist as above.
	LedgerRelease LedgerKind = "release"
	// LedgerActivation holds Contract §7.13 ActivationEvents, both
	// event_type=activate and event_type=deactivate (Contract §9.2.6,
	// GMS §2.6). Single global stream: the activation sequence is global,
	// positive, monotone and never reused.
	LedgerActivation LedgerKind = "activation"
	// LedgerDeactivation holds the protected deactivation authorization
	// records that deactivate events must reference (Contract §9.2.3,
	// §13.5). Empty payload whitelist as above.
	LedgerDeactivation LedgerKind = "deactivation"
	// LedgerMergeProposal holds Contract §7.22 MergeProposal records
	// (GMS §2.7).
	LedgerMergeProposal LedgerKind = "merge-proposal"
	// LedgerMergeEvent holds Contract §7.23 MergeProposalEvent state events
	// (GMS §2.7).
	LedgerMergeEvent LedgerKind = "merge-event"
	// LedgerSimilarity holds Contract §7.21 similarity assessments
	// (GMS §2.7).
	LedgerSimilarity LedgerKind = "similarity"
	// LedgerProjection holds projection checkpoint records (GMS §2.8).
	// Empty payload whitelist as above.
	LedgerProjection LedgerKind = "projection"
)

// LedgerKindSpec is the public view of one ledger kind's closed contract.
type LedgerKindSpec struct {
	// PayloadSchemas is the closed set of payload schema_version values the
	// kind accepts (Contract §16.6 schema authority). Empty means no frozen
	// schema exists yet and appends fail closed.
	PayloadSchemas []string
	// SingleStream reports whether the kind has exactly one global stream
	// (the activation ledger; GMS §2.6).
	SingleStream bool
	// SequenceConflictCode is the canonical registry reason code reported
	// when a sequence slot is reused, overwritten or gapped.
	SequenceConflictCode string
}

type ledgerKindSpec struct {
	payloadSchemas       []string
	singleStream         bool
	sequenceConflictCode string
}

// ledgerKinds freezes the kind classification. Payload schema versions come
// from $FIX/schema/shared/*.schema.json (Contract §16.6 F1/F4). For kinds
// whose records have no frozen shared schema the whitelist is empty: the
// port rejects every payload with SCHEMA_VERSION_UNSUPPORTED rather than
// accepting an invented schema. Sequence-conflict codes are the closest
// canonical registry codes per kind (GMS §11.4 groups; §13.2 for the
// generic same-slot-different-digest kinds).
var ledgerKinds = map[LedgerKind]ledgerKindSpec{
	LedgerEvidence:     {payloadSchemas: []string{contract.SchemaEvidenceRef}, sequenceConflictCode: ReasonIdempotencyConflict},
	LedgerProposal:     {payloadSchemas: []string{"gms.skill-proposal.v1", SchemaProposalEvent}, sequenceConflictCode: ReasonProposalStateConflict},
	LedgerCandidate:    {payloadSchemas: []string{contract.SchemaCandidateArtifactRef}, sequenceConflictCode: ReasonIdempotencyConflict},
	LedgerValidation:   {sequenceConflictCode: ReasonIdempotencyConflict},
	LedgerReplay:       {payloadSchemas: []string{"gms.replay-request.v1", "gms.replay-result.v1"}, sequenceConflictCode: ReasonIdempotencyConflict},
	LedgerEvaluation:   {payloadSchemas: []string{"gms.release-decision.v1"}, sequenceConflictCode: ReasonIdempotencyConflict},
	LedgerRelease:      {sequenceConflictCode: ReasonLineageVersionConflict},
	LedgerActivation:   {payloadSchemas: []string{SchemaActivationEvent}, singleStream: true, sequenceConflictCode: ReasonActivationSequenceConflict},
	LedgerDeactivation: {sequenceConflictCode: ReasonActivationSequenceConflict},
	LedgerMergeProposal: {
		payloadSchemas:       []string{"gms.merge-proposal.v1"},
		sequenceConflictCode: ReasonMergeStateConflict,
	},
	LedgerMergeEvent: {
		payloadSchemas:       []string{contract.SchemaMergeProposalEvent},
		sequenceConflictCode: ReasonMergeStateConflict,
	},
	LedgerSimilarity: {payloadSchemas: []string{contract.SchemaSimilarityAssessment}, sequenceConflictCode: ReasonIdempotencyConflict},
	LedgerProjection: {sequenceConflictCode: ReasonProjectionEventConflictCode},
}

// SchemaProposalEvent and SchemaActivationEvent are the payload schema
// versions of the ordinary proposal state event and the activation event as
// frozen in $FIX/schema/shared (CTR-005 gap-fill DTOs; Contract §16.6).
const (
	SchemaProposalEvent   = "gms.proposal-event.v1"
	SchemaActivationEvent = "gms.activation-event.v1"
	// ReasonProjectionEventConflictCode aliases the registry code for a
	// same-sequence conflict inside the projection stream (Contract §9.3).
	ReasonProjectionEventConflictCode = contract.ReasonProjectionEventConflict
)

// AllLedgerKinds returns the closed ledger kind enum in stable order.
func AllLedgerKinds() []LedgerKind {
	return []LedgerKind{
		LedgerEvidence, LedgerProposal, LedgerCandidate, LedgerValidation,
		LedgerReplay, LedgerEvaluation, LedgerRelease, LedgerActivation,
		LedgerDeactivation, LedgerMergeProposal, LedgerMergeEvent,
		LedgerSimilarity, LedgerProjection,
	}
}

// LedgerSpec returns the closed contract of one ledger kind.
func LedgerSpec(kind LedgerKind) (LedgerKindSpec, bool) {
	spec, ok := ledgerKinds[kind]
	if !ok {
		return LedgerKindSpec{}, false
	}
	schemas := make([]string, len(spec.payloadSchemas))
	copy(schemas, spec.payloadSchemas)
	return LedgerKindSpec{
		PayloadSchemas:       schemas,
		SingleStream:         spec.singleStream,
		SequenceConflictCode: spec.sequenceConflictCode,
	}, true
}

// ---------------------------------------------------------------------------
// Head kinds (multi-head exact CAS; GMS §2.6–§2.8, Contract §13.1)
// ---------------------------------------------------------------------------

// HeadKind is the closed set of compare-and-swap heads. Every head carries
// a monotone sequence and the exact digest of the record it points at.
type HeadKind string

const (
	// HeadSkillActive is the per-lineage authoritative active-head table
	// (GMS §2.6, Contract §5.3.1).
	HeadSkillActive HeadKind = "skill-active"
	// HeadMergeSource is the frozen expectation over a merge source active
	// head (Contract §5.5.5/M5).
	HeadMergeSource HeadKind = "merge-source"
	// HeadMergeGroupWinner is the proposal_group_key winner CAS allowing a
	// single admitted/in-flight winner (GMS §2.7, Contract §5.5.8).
	HeadMergeGroupWinner HeadKind = "merge-group-winner"
	// HeadLineageVersion is the per-lineage version-assignment head
	// (GMS §2.3/§2.9).
	HeadLineageVersion HeadKind = "lineage-version"
	// HeadProjection is the projection head/watermark of one projection
	// stream (GMS §2.8, Contract §5.3.4).
	HeadProjection HeadKind = "projection"
	// HeadActivationSequence guards the global activation sequence
	// allocator (GMS §2.6).
	HeadActivationSequence HeadKind = "activation-sequence"
)

// HeadKindSpec is the public view of one head kind's closed contract.
type HeadKindSpec struct {
	// ConflictCode is the canonical registry reason code reported when the
	// exact expected (sequence, digest) does not match (stale CAS).
	ConflictCode string
}

var headKinds = map[HeadKind]string{
	HeadSkillActive:        ReasonActiveHeadConflict,
	HeadMergeSource:        ReasonMergeSourceHeadStale,
	HeadMergeGroupWinner:   ReasonMergeGroupInFlight,
	HeadLineageVersion:     ReasonLineageVersionConflict,
	HeadProjection:         ReasonProjectionHeadConflict,
	HeadActivationSequence: ReasonActivationSequenceConflict,
}

// AllHeadKinds returns the closed head kind enum in stable order.
func AllHeadKinds() []HeadKind {
	return []HeadKind{
		HeadSkillActive, HeadMergeSource, HeadMergeGroupWinner,
		HeadLineageVersion, HeadProjection, HeadActivationSequence,
	}
}

// HeadSpec returns the closed contract of one head kind.
func HeadSpec(kind HeadKind) (HeadKindSpec, bool) {
	code, ok := headKinds[kind]
	if !ok {
		return HeadKindSpec{}, false
	}
	return HeadKindSpec{ConflictCode: code}, true
}

// ---------------------------------------------------------------------------
// Record shapes
// ---------------------------------------------------------------------------

// Entry is one append-only ledger record: the ledger kind, the stream
// identity inside the ledger ("" for single-stream ledgers), the sequence
// within that stream (positive, monotone, never reused), the event id and
// the digest of the canonical payload, whose bytes live in the
// ContentStore. Entries are values; ledgers hand out copies only.
type Entry struct {
	Ledger        LedgerKind
	Stream        string
	Sequence      uint64
	EventID       string
	PayloadDigest string
}

// DeliveryState of an outbox record (GMS §2.6): records are enqueued
// pending inside the activation transaction and marked delivered by the
// post-commit delivery worker. Delivery may retry; it MUST NOT mint a new
// ActivationEvent.
type DeliveryState string

const (
	DeliveryPending   DeliveryState = "pending"
	DeliveryDelivered DeliveryState = "delivered"
)

// OutboxRecord is the transactional outbox record of one activation or
// deactivation (GMS §2.6, Contract §16.4 #8): it fixes the outbox key, the
// activation ledger sequence and event digest it projects, the projection
// target and the delivery state.
type OutboxRecord struct {
	OutboxKey          string
	Ledger             LedgerKind
	ActivationSequence uint64
	EventDigest        string
	ProjectionTarget   string
	DeliveryState      DeliveryState
}

// IdempotencyRecord is the recorded outcome of one external mutation
// request (Contract §13.2, GMS §2.1): same key + same canonical request
// digest replays the recorded outcome; same key + different digest is
// IDEMPOTENCY_CONFLICT.
type IdempotencyRecord struct {
	Key           string
	RequestDigest string
	Outcome       []byte
}

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

// Ledger is the append-only authority stream port (Contract §5.1.3: all
// authoritative records are append-only; corrections are new records or
// compensating events — there is deliberately NO update or delete method).
type Ledger interface {
	// Append appends entry to its (kind, stream) ledger. The sequence MUST
	// be exactly current-head+1 (monotone, contiguous, no reuse). Re-
	// appending the exact current entry at its slot is an idempotent no-op;
	// anything else that touches an occupied or gapped slot is rejected
	// with the kind's canonical sequence-conflict code. Unknown kinds are
	// rejected fail-closed.
	Append(entry Entry) error
	// Snapshot returns a deep copy of one stream's entries.
	Snapshot(kind LedgerKind, stream string) ([]Entry, error)
	// Head returns the last entry of one stream.
	Head(kind LedgerKind, stream string) (Entry, bool, error)
	// EntryAt returns the entry at one sequence position, if committed.
	EntryAt(kind LedgerKind, stream string, sequence uint64) (Entry, bool, error)
}

// ContentStore is the content-addressed payload store: put/get canonical
// bytes keyed by their JCS SHA-256 digest (Contract §6.1, GMS §2.3). Puts
// are idempotent — identical bytes address to the same digest.
type ContentStore interface {
	Put(canonical []byte) (string, error)
	Get(digest string) ([]byte, bool, error)
}

// IdempotencyStore records external mutation outcomes (Contract §13.2).
type IdempotencyStore interface {
	// Record stores rec. Same key + same digest is an idempotent no-op;
	// same key + different digest is rejected with IDEMPOTENCY_CONFLICT.
	Record(rec IdempotencyRecord) error
	// Lookup returns a copy of the recorded outcome, if any.
	Lookup(key string) (IdempotencyRecord, bool, error)
}

// HeadStore is the multi-head exact compare-and-swap port (GMS §2.1: all
// CAS compares exact expected head/ref/sequence — never a name or a
// timestamp). A head is (kind, key) → (sequence, digest); sequence 0 with
// empty digest denotes the absent head.
type HeadStore interface {
	CompareAndSwap(kind HeadKind, key string, expectedSeq uint64, expectedDigest string, newSeq uint64, newDigest string) error
	GetHead(kind HeadKind, key string) (seq uint64, digest string, ok bool, err error)
}

// Outbox is the transactional outbox port (GMS §2.6, Contract §13.1:
// outbox records are written in the same transaction as the activation
// event and delivered afterwards).
type Outbox interface {
	// Enqueue stores a pending record. Same outbox key + same
	// (ledger, sequence, digest) is an idempotent no-op; the same key with
	// a different digest is rejected with IDEMPOTENCY_CONFLICT.
	Enqueue(rec OutboxRecord) error
	// GetOutbox returns a copy of one record by key.
	GetOutbox(outboxKey string) (OutboxRecord, bool, error)
	// Pending returns the pending records in deterministic
	// (ActivationSequence, OutboxKey) order.
	Pending() ([]OutboxRecord, error)
	// MarkDelivered records a successful delivery. The sequence and event
	// digest MUST match the recorded event (DIGEST_MISMATCH otherwise);
	// re-delivering the same record is an idempotent no-op and never mints
	// a new ActivationEvent.
	MarkDelivered(outboxKey string, activationSequence uint64, eventDigest string) error
}

// TxManager is the atomic-transaction port (Contract §13.1/§13.5, GMS
// §2.9). WithinTx runs fn as one unit of work over the ledger ports: fn
// returning an error rolls the whole transaction back; fn returning nil
// commits every staged write atomically — the commit itself is all-or-
// nothing, so a failure can never expose a partial version, head, event or
// proposal terminal state. The domain implementation is Manager
// (transaction.go); a durable adapter MAY provide its own TxManager over
// the same Store vocabulary (e.g. one ACID database transaction).
type TxManager interface {
	WithinTx(ctx context.Context, fn func(tx *Tx) error) error
}

// Store is the durable-boundary seam: the complete authoritative-write
// surface a storage adapter must provide (Contract §13.1, GMS §2.9). The
// domain transaction (Manager/Tx, transaction.go) drives exactly these
// primitives; a production adapter MUST implement them with crash-atomic,
// durable semantics. The Store deliberately has no mutation or deletion
// methods — authoritative history is append-only.
type Store interface {
	Ledger
	ContentStore
	IdempotencyStore
	HeadStore
	Outbox
}

// ---------------------------------------------------------------------------
// State-machine tables (Contract §9.1/§9.4 via $FIX/schema/state)
// ---------------------------------------------------------------------------

// The Go transition tables below are machine-derived from the frozen
// conformance schemas $FIX/schema/state/proposal-lifecycle.schema.json
// (machine "skill-proposal") and merge-lifecycle.schema.json (machine
// "merge-proposal"), plus the genesis edge none→initial-state that the
// event DTOs use for creation events (from_state "none": see
// $FIX/events/event-005-merge-proposal-event). The schema files stay the
// single authority: TestStateTablesMatchConformanceSchemas cross-checks the
// tables against $FIX on every run (Contract §16.6 F2).

// ProposalEventTransitions is the Contract §9.1 ordinary proposal state
// machine as carried by gms.proposal-event.v1 payloads.
var ProposalEventTransitions = buildTransitionTable([][2]string{
	{"none", "proposed"},
	{"proposed", "admitted"}, {"proposed", "rejected"}, {"proposed", "stale"}, {"proposed", "withdrawn"},
	{"admitted", "candidate_bound"}, {"admitted", "rejected"}, {"admitted", "stale"}, {"admitted", "withdrawn"},
	{"candidate_bound", "validating"}, {"candidate_bound", "stale"},
	{"validating", "replay_pending"}, {"validating", "rejected"}, {"validating", "stale"},
	{"replay_pending", "replaying"}, {"replay_pending", "stale"},
	{"replaying", "decision_pending"}, {"replaying", "rejected"}, {"replaying", "inconclusive"}, {"replaying", "stale"},
	{"decision_pending", "activation_pending"}, {"decision_pending", "rejected"}, {"decision_pending", "inconclusive"},
	{"activation_pending", "released"}, {"activation_pending", "rejected"}, {"activation_pending", "stale"},
})

// MergeEventTransitions is the Contract §9.4 merge proposal state machine
// as carried by gms.merge-proposal-event.v1 payloads.
var MergeEventTransitions = buildTransitionTable([][2]string{
	{"none", "proposed"},
	{"proposed", "admitted"}, {"proposed", "duplicate"}, {"proposed", "rejected"}, {"proposed", "stale"}, {"proposed", "withdrawn"},
	{"admitted", "synthesizing"}, {"admitted", "rejected"}, {"admitted", "stale"}, {"admitted", "withdrawn"},
	{"synthesizing", "candidate_bound"}, {"synthesizing", "inconclusive"}, {"synthesizing", "rejected"}, {"synthesizing", "stale"},
	{"candidate_bound", "validating"}, {"candidate_bound", "stale"},
	{"validating", "replaying"}, {"validating", "rejected"}, {"validating", "stale"},
	{"replaying", "decision_pending"}, {"replaying", "rejected"}, {"replaying", "inconclusive"}, {"replaying", "stale"},
	{"decision_pending", "activation_pending"}, {"decision_pending", "rejected"}, {"decision_pending", "inconclusive"},
	{"activation_pending", "released"}, {"activation_pending", "stale"},
})

// payloadStateTables binds payload schema versions to the state machine
// their from_state/to_state pair MUST satisfy (ILLEGAL_STATE_TRANSITION
// otherwise — the proposal-event schema description).
var payloadStateTables = map[string]map[string]map[string]bool{
	SchemaProposalEvent:               ProposalEventTransitions,
	contract.SchemaMergeProposalEvent: MergeEventTransitions,
}

func buildTransitionTable(edges [][2]string) map[string]map[string]bool {
	table := make(map[string]map[string]bool, len(edges))
	for _, edge := range edges {
		if table[edge[0]] == nil {
			table[edge[0]] = map[string]bool{}
		}
		table[edge[0]][edge[1]] = true
	}
	return table
}

// legalTransition reports whether from→to is an edge of table.
func legalTransition(table map[string]map[string]bool, from, to string) bool {
	targets, ok := table[from]
	if !ok {
		return false
	}
	return targets[to]
}

// verifyMappedReasonCodes fails closed unless every reason code this package
// can emit exists in the digest-verified registry (Contract §13.7.1 R5).
func verifyMappedReasonCodes(registry ReasonRegistry) error {
	if registry == nil {
		return errors.New("ledger: nil reason registry")
	}
	for _, kind := range AllLedgerKinds() {
		spec, ok := ledgerKinds[kind]
		if !ok {
			return fmt.Errorf("ledger: ledger kind %q has no spec", kind)
		}
		if err := registry.Verify(spec.sequenceConflictCode); err != nil {
			return fmt.Errorf("ledger kind %s: %w", kind, err)
		}
	}
	for _, kind := range AllHeadKinds() {
		code, ok := headKinds[kind]
		if !ok {
			return fmt.Errorf("ledger: head kind %q has no spec", kind)
		}
		if err := registry.Verify(code); err != nil {
			return fmt.Errorf("ledger head kind %s: %w", kind, err)
		}
	}
	for _, code := range []string{
		ReasonIdempotencyConflict, ReasonActivationEventInvalid,
		ReasonDigestMismatch, ReasonRefMismatch, ReasonIllegalStateTransition,
		ReasonSchemaEnumInvalid, ReasonSchemaVersionUnsupported,
		ReasonSchemaFieldUnknown, ReasonRequiredFieldMissing,
		ReasonInvalidJSON, ReasonNonIntegerNumber,
		ReasonProjectionEventConflictCode,
	} {
		if err := registry.Verify(code); err != nil {
			return err
		}
	}
	return nil
}
