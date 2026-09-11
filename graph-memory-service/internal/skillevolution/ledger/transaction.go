package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"river2.dev/graph-memory-service/internal/contract"
)

// digestShape is the Contract §6.1.3 digest form: "sha256:" + 64 lowercase
// hex digits.
var digestShape = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func isValidDigest(digest string) bool { return digestShape.MatchString(digest) }

// ---------------------------------------------------------------------------
// Manager: the domain transaction
// ---------------------------------------------------------------------------

// Manager is the domain transaction manager over a Store (Contract §13.1,
// GMS §2.9). It is deliberately storage-agnostic: it stages a write set in
// memory and applies it against the port primitives in one serialized
// validate+apply phase, compensating every applied write if any step fails.
// A durable adapter either plugs in behind these ports (and MAY additionally
// implement commitSerializer/compensatingStore below) or provides its own
// TxManager over the same port vocabulary.
type Manager struct {
	store    Store
	registry ReasonRegistry
}

// Manager implements the TxManager port (ports.go).
var _ TxManager = (*Manager)(nil)

// NewManager wires a transaction manager to a Store. It fails closed at
// construction time unless every reason code the ports can emit exists in
// the digest-verified registry (Contract §13.7.1 R5).
func NewManager(store Store, registry ReasonRegistry) (*Manager, error) {
	if store == nil {
		return nil, errors.New("ledger: nil store")
	}
	if err := verifyMappedReasonCodes(registry); err != nil {
		return nil, err
	}
	return &Manager{store: store, registry: registry}, nil
}

// WithinTx runs fn as one atomic unit of work over the ledger ports:
//
//   - fn returns an error  → the transaction rolls back completely; nothing
//     staged inside fn was ever applied, so there is nothing to undo;
//   - fn returns nil       → the staged write set is applied atomically
//     (all writes or none; a mid-apply failure compensates every already
//     applied write);
//   - fn panics            → nothing was applied (writes only land at
//     commit); the panic propagates to the caller.
//
// Read-snapshot consistency: with the memory adapter each port operation is
// serialized under a single mutex, so individual reads are atomic but a
// cross-port consistent read requires WithinTx itself or the adapter's
// commit lock. A durable adapter MUST provide at least equivalent
// isolation for the apply phase (e.g. one ACID database transaction).
//
// Context cancellation between fn and commit aborts the transaction without
// applying anything; it surfaces as a plain infrastructure error carrying
// no reason code.
func (m *Manager) WithinTx(ctx context.Context, fn func(tx *Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := &Tx{mgr: m, idemSeen: make(map[string]string)}
	if err := fn(tx); err != nil {
		return err // full rollback: staged writes are discarded
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.commit(tx)
}

// commitSerializer is the adapter seam that lets the Manager run its whole
// validate+apply phase as one critical section when the adapter's primitive
// writes are not transactional on their own (the memory adapter). Adapters
// with native multi-write transactions do not implement it.
type commitSerializer interface {
	Lock()
	Unlock()
}

// compensatingStore is the adapter-internal seam for reverting a failed
// apply phase. It is deliberately unexported and outside the Store surface:
// it can only remove writes made by the in-flight commit, never arbitrary
// history — the authoritative write surface stays append-only.
type compensatingStore interface {
	undoContentPut(digest string)
	undoAppend(entry Entry)
	undoHeadCAS(kind HeadKind, key string, prevSeq uint64, prevDigest string, prevExisted bool)
	undoOutbox(outboxKey string)
	undoIdempotency(key string)
}

// commit applies the staged write set in the pinned order
// content → head CAS → ledger appends → outbox → idempotency.
//
// Head CAS runs before the appends so that the contended resource fails
// fast with the head kind's canonical conflict code (the §13.1 release
// boundary lists CAS and the ledger event as one atomic unit; the internal
// order is observable only through which conflict code a race produces).
// Outbox records are validated against the ledger entries appended by this
// same transaction (or already committed) — a partial outbox rejects the
// whole commit. Idempotency records land last so a failed commit never
// consumes a key.
func (m *Manager) commit(tx *Tx) error {
	if tx.replay {
		if tx.hasWrites() {
			return newError(m.registry, ReasonIdempotencyConflict,
				"idempotent replay of key %q staged new writes; a retry MUST NOT produce a new version, activation or proposal winner (Contract §13.2)", tx.replayKey)
		}
		return nil
	}

	if serializer, ok := m.store.(commitSerializer); ok {
		serializer.Lock()
		defer serializer.Unlock()
	}
	compensate, canCompensate := m.store.(compensatingStore)

	var undos []func()
	fail := func(err error) error {
		for i := len(undos) - 1; i >= 0; i-- {
			undos[i]()
		}
		return err
	}

	// 1. Content-addressed payload blobs.
	for _, put := range tx.contentPuts {
		_, existed, err := m.store.Get(put.digest)
		if err != nil {
			return fail(err)
		}
		digest, err := m.store.Put(put.canonical)
		if err != nil {
			return fail(err)
		}
		if digest != put.digest {
			return fail(newError(m.registry, ReasonDigestMismatch,
				"content store returned digest %s for staged payload %s", digest, put.digest))
		}
		if canCompensate && !existed {
			undoDigest := put.digest
			undos = append(undos, func() { compensate.undoContentPut(undoDigest) })
		}
	}

	// 2. Exact multi-head CAS (the contended resource fails first).
	for _, op := range tx.casOps {
		prevSeq, prevDigest, prevExisted, err := m.store.GetHead(op.kind, op.key)
		if err != nil {
			return fail(err)
		}
		if err := m.store.CompareAndSwap(op.kind, op.key, op.expectedSeq, op.expectedDigest, op.newSeq, op.newDigest); err != nil {
			return fail(err)
		}
		if canCompensate {
			kind, key := op.kind, op.key
			undos = append(undos, func() { compensate.undoHeadCAS(kind, key, prevSeq, prevDigest, prevExisted) })
		}
	}

	// 3. Ledger appends (with a same-transaction overlay for outbox binding).
	overlay := make(map[string][]Entry)
	for _, entry := range tx.appends {
		// The head BEFORE the append decides whether this call extends the
		// ledger (needs undo) or is an exact-replay no-op (must NOT be
		// undone — it predates the transaction). Contiguity makes the head
		// sequence equal to the entry count.
		head, _, err := m.store.Head(entry.Ledger, entry.Stream)
		if err != nil {
			return fail(err)
		}
		if err := m.store.Append(entry); err != nil {
			return fail(err)
		}
		overlayKey := string(entry.Ledger) + "\x1f" + entry.Stream
		overlay[overlayKey] = append(overlay[overlayKey], entry)
		if canCompensate && entry.Sequence == head.Sequence+1 {
			undoEntry := entry
			undos = append(undos, func() { compensate.undoAppend(undoEntry) })
		}
	}

	// 4. Outbox records, each bound to a resolvable activation ledger entry.
	// Enqueue itself re-checks same-key bindings, so a concurrent writer
	// that committed a different binding in between surfaces here as
	// IDEMPOTENCY_CONFLICT instead of being silently dropped.
	for _, rec := range tx.outbox {
		if !m.outboxResolves(rec, overlay) {
			return fail(newError(m.registry, ReasonActivationEventInvalid,
				"outbox key %s references %s sequence %d digest %s, which is neither committed nor appended in this transaction (partial outbox, GMS §2.6/§2.9)",
				rec.OutboxKey, rec.Ledger, rec.ActivationSequence, rec.EventDigest))
		}
		_, existed, err := m.store.GetOutbox(rec.OutboxKey)
		if err != nil {
			return fail(err)
		}
		if err := m.store.Enqueue(rec); err != nil {
			return fail(err)
		}
		if canCompensate && !existed {
			key := rec.OutboxKey
			undos = append(undos, func() { compensate.undoOutbox(key) })
		}
	}

	// 5. Idempotency records land last. Record re-checks the key, so a
	// concurrent conflicting recording surfaces here instead of being
	// skipped.
	for _, rec := range tx.idem {
		_, existed, err := m.store.Lookup(rec.Key)
		if err != nil {
			return fail(err)
		}
		if err := m.store.Record(rec); err != nil {
			return fail(err)
		}
		if canCompensate && !existed {
			key := rec.Key
			undos = append(undos, func() { compensate.undoIdempotency(key) })
		}
	}
	return nil
}

// outboxResolves reports whether rec binds to a committed activation ledger
// entry or to one appended by the in-flight commit.
func (m *Manager) outboxResolves(rec OutboxRecord, overlay map[string][]Entry) bool {
	for _, entry := range overlay[string(rec.Ledger)+"\x1f"] {
		if entry.Sequence == rec.ActivationSequence {
			return entry.PayloadDigest == rec.EventDigest
		}
	}
	entry, ok, err := m.store.EntryAt(rec.Ledger, "", rec.ActivationSequence)
	if err != nil || !ok {
		return false
	}
	return entry.PayloadDigest == rec.EventDigest
}

// ---------------------------------------------------------------------------
// Tx: the staged write set
// ---------------------------------------------------------------------------

// Tx aggregates the one-shot write set of a transaction: staged content
// puts, ledger appends, head CAS operations, outbox entries and idempotency
// records. Nothing touches the Store until commit; every method validates
// fail-closed at staging time (payload shape, schema whitelist, state
// transition, digest identity) so garbage never reaches the apply phase.
type Tx struct {
	mgr *Manager

	contentPuts []stagedContent
	appends     []Entry
	casOps      []stagedCAS
	outbox      []OutboxRecord
	idem        []IdempotencyRecord
	idemSeen    map[string]string // key → request digest staged in this tx

	replay     bool
	replayKey  string
	outboxSeen map[string]OutboxRecord
}

type stagedContent struct {
	digest    string
	canonical []byte
}

type stagedCAS struct {
	kind           HeadKind
	key            string
	expectedSeq    uint64
	expectedDigest string
	newSeq         uint64
	newDigest      string
}

func (tx *Tx) hasWrites() bool {
	return len(tx.contentPuts)+len(tx.appends)+len(tx.casOps)+len(tx.outbox)+len(tx.idem) > 0
}

// NextSequence returns the sequence the next append to (kind, stream) would
// take: committed head plus the appends already staged in this transaction,
// plus one.
func (tx *Tx) NextSequence(kind LedgerKind, stream string) (uint64, error) {
	spec, ok := ledgerKinds[kind]
	if !ok {
		return 0, newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum (Contract §16.6)", kind)
	}
	if err := tx.checkStream(spec, kind, stream); err != nil {
		return 0, err
	}
	head, _, err := tx.mgr.store.Head(kind, stream)
	if err != nil {
		return 0, err
	}
	next := head.Sequence + 1
	for _, entry := range tx.appends {
		if entry.Ledger == kind && entry.Stream == stream {
			next++
		}
	}
	return next, nil
}

// PutContent stages one payload for the content-addressed store and returns
// its digest (integer-only JCS SHA-256 via internal/contract; non-integer
// numbers fail closed with NON_INTEGER_NUMBER).
func (tx *Tx) PutContent(payload []byte) (string, error) {
	canonical, digest, _, err := canonicalize(tx.mgr.registry, payload)
	if err != nil {
		return "", err
	}
	for _, put := range tx.contentPuts {
		if put.digest == digest {
			return digest, nil // content addressing makes re-puts no-ops
		}
	}
	tx.contentPuts = append(tx.contentPuts, stagedContent{digest: digest, canonical: canonical})
	return digest, nil
}

// AppendEvent validates the event payload against the ledger kind's closed
// schema whitelist and state machine, stages its canonical bytes in the
// content store, and stages the ledger entry with the auto-assigned next
// sequence. The payload's own event_id and sequence fields (when present)
// MUST match the entry (REF_MISMATCH otherwise).
func (tx *Tx) AppendEvent(kind LedgerKind, stream, eventID string, payload []byte) (Entry, error) {
	sequence, err := tx.NextSequence(kind, stream)
	if err != nil {
		return Entry{}, err
	}
	canonical, digest, err := tx.validateEventPayload(kind, eventID, sequence, payload)
	if err != nil {
		return Entry{}, err
	}
	tx.stageContent(digest, canonical)
	entry := Entry{Ledger: kind, Stream: stream, Sequence: sequence, EventID: eventID, PayloadDigest: digest}
	tx.appends = append(tx.appends, entry)
	return entry, nil
}

// AppendEntry stages an explicit entry (exact-sequence append). The payload
// MUST canonicalize to entry.PayloadDigest (DIGEST_MISMATCH otherwise) and
// satisfy the same validation as AppendEvent; sequence contiguity is
// enforced at commit by the ledger primitive.
func (tx *Tx) AppendEntry(entry Entry, payload []byte) error {
	spec, ok := ledgerKinds[entry.Ledger]
	if !ok {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum (Contract §16.6)", entry.Ledger)
	}
	if err := tx.checkStream(spec, entry.Ledger, entry.Stream); err != nil {
		return err
	}
	if entry.EventID == "" {
		return newError(tx.mgr.registry, ReasonRequiredFieldMissing, "entry event id must be non-empty")
	}
	if !isValidDigest(entry.PayloadDigest) {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "entry payload digest %q must match sha256:<64 lowercase hex>", entry.PayloadDigest)
	}
	if entry.Sequence == 0 {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "entry sequence must be a positive integer (GMS §2.6)")
	}
	canonical, digest, err := tx.validateEventPayload(entry.Ledger, entry.EventID, entry.Sequence, payload)
	if err != nil {
		return err
	}
	if digest != entry.PayloadDigest {
		return newError(tx.mgr.registry, ReasonDigestMismatch,
			"entry declares payload digest %s but payload canonicalizes to %s", entry.PayloadDigest, digest)
	}
	tx.stageContent(digest, canonical)
	tx.appends = append(tx.appends, entry)
	return nil
}

// CompareAndSwapHead stages one exact head CAS. The expected (sequence,
// digest) pair is verified at commit; a stale expectation fails with the
// head kind's canonical conflict code and rolls the whole transaction back.
func (tx *Tx) CompareAndSwapHead(kind HeadKind, key string, expectedSeq uint64, expectedDigest string, newSeq uint64, newDigest string) error {
	if _, ok := headKinds[kind]; !ok {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "unknown head kind %q outside the closed enum", kind)
	}
	if key == "" {
		return newError(tx.mgr.registry, ReasonRequiredFieldMissing, "head key must be non-empty")
	}
	if expectedDigest != "" && !isValidDigest(expectedDigest) {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "expected digest %q must be empty or match sha256:<64 lowercase hex>", expectedDigest)
	}
	if !isValidDigest(newDigest) {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "new digest %q must match sha256:<64 lowercase hex>", newDigest)
	}
	tx.casOps = append(tx.casOps, stagedCAS{
		kind: kind, key: key,
		expectedSeq: expectedSeq, expectedDigest: expectedDigest,
		newSeq: newSeq, newDigest: newDigest,
	})
	return nil
}

// EnqueueOutbox stages one pending outbox record (GMS §2.6). The record's
// activation sequence and event digest MUST resolve to an activation ledger
// entry committed in the same transaction — a partial outbox rejects the
// whole commit. The same outbox key with a different digest conflicts.
func (tx *Tx) EnqueueOutbox(rec OutboxRecord) error {
	if !isValidDigest(rec.OutboxKey) {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "outbox key %q must match sha256:<64 lowercase hex> (Contract §7.13)", rec.OutboxKey)
	}
	if !isValidDigest(rec.EventDigest) {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "outbox event digest %q must match sha256:<64 lowercase hex>", rec.EventDigest)
	}
	if rec.ProjectionTarget == "" {
		return newError(tx.mgr.registry, ReasonRequiredFieldMissing, "outbox projection target must be non-empty")
	}
	if rec.DeliveryState != DeliveryPending {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "outbox records are enqueued %q only; delivery state transitions belong to MarkDelivered", rec.DeliveryState)
	}
	if _, ok := ledgerKinds[rec.Ledger]; !ok {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum", rec.Ledger)
	}
	// v1 outbox records bind to the activation ledger only (GMS §2.6).
	if rec.Ledger != LedgerActivation {
		return newError(tx.mgr.registry, ReasonActivationEventInvalid,
			"v1 outbox records bind to the activation ledger only (GMS §2.6), got %q", rec.Ledger)
	}
	if rec.ActivationSequence == 0 {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "outbox activation sequence must be a positive integer")
	}
	// Same key + same binding staged twice is a no-op; a different binding
	// conflicts (§13.2 semantics, GMS §2.6).
	if tx.outboxSeen == nil {
		tx.outboxSeen = make(map[string]OutboxRecord)
	}
	if prior, seen := tx.outboxSeen[rec.OutboxKey]; seen {
		if prior.Ledger == rec.Ledger && prior.ActivationSequence == rec.ActivationSequence && prior.EventDigest == rec.EventDigest {
			return nil
		}
		return newError(tx.mgr.registry, ReasonIdempotencyConflict,
			"outbox key %s already staged with a different binding (GMS §2.6: same outbox key, different digest MUST conflict)", rec.OutboxKey)
	}
	// Fail fast on an already committed different binding.
	if prior, found, err := tx.mgr.store.GetOutbox(rec.OutboxKey); err != nil {
		return err
	} else if found {
		if prior.Ledger == rec.Ledger && prior.ActivationSequence == rec.ActivationSequence && prior.EventDigest == rec.EventDigest {
			return nil // idempotent re-enqueue
		}
		return newError(tx.mgr.registry, ReasonIdempotencyConflict,
			"outbox key %s already recorded with a different binding (GMS §2.6)", rec.OutboxKey)
	}
	tx.outboxSeen[rec.OutboxKey] = rec
	tx.outbox = append(tx.outbox, rec)
	return nil
}

// RecordIdempotency stages the idempotency record for (key, requestDigest)
// and reports whether this exact pair was already recorded.
//
//   - already recorded with the same digest: replayed=true and the recorded
//     outcome is returned; the transaction MUST NOT stage new writes (a
//     replay that stages writes fails closed at commit with
//     IDEMPOTENCY_CONFLICT — a retry must not mint a second version);
//   - already recorded with a different digest: IDEMPOTENCY_CONFLICT;
//   - not recorded: the record is staged and applied on commit.
func (tx *Tx) RecordIdempotency(key, requestDigest string, outcome []byte) (replayed bool, recorded []byte, err error) {
	if key == "" {
		return false, nil, newError(tx.mgr.registry, ReasonRequiredFieldMissing, "idempotency key must be non-empty (Contract §13.2)")
	}
	if !isValidDigest(requestDigest) {
		return false, nil, newError(tx.mgr.registry, ReasonSchemaEnumInvalid,
			"request digest %q must match sha256:<64 lowercase hex> (canonical request digest, GMS §2.1)", requestDigest)
	}
	if prior, found, err := tx.mgr.store.Lookup(key); err != nil {
		return false, nil, err
	} else if found {
		if prior.RequestDigest != requestDigest {
			return false, nil, newError(tx.mgr.registry, ReasonIdempotencyConflict,
				"idempotency key %s recorded with request digest %s, got %s (Contract §13.2)", key, prior.RequestDigest, requestDigest)
		}
		tx.replay = true
		tx.replayKey = key
		return true, append([]byte(nil), prior.Outcome...), nil
	}
	if priorDigest, staged := tx.idemSeen[key]; staged {
		if priorDigest != requestDigest {
			return false, nil, newError(tx.mgr.registry, ReasonIdempotencyConflict,
				"idempotency key %s staged twice with different digests in one transaction", key)
		}
		return false, nil, nil
	}
	tx.idemSeen[key] = requestDigest
	tx.idem = append(tx.idem, IdempotencyRecord{
		Key:           key,
		RequestDigest: requestDigest,
		Outcome:       append([]byte(nil), outcome...),
	})
	return false, nil, nil
}

// ---------------------------------------------------------------------------
// Staging helpers
// ---------------------------------------------------------------------------

func (tx *Tx) checkStream(spec ledgerKindSpec, kind LedgerKind, stream string) error {
	if spec.singleStream && stream != "" {
		return newError(tx.mgr.registry, ReasonSchemaEnumInvalid,
			"ledger %s is a single global stream (GMS §2.6), stream %q must be empty", kind, stream)
	}
	return nil
}

func (tx *Tx) stageContent(digest string, canonical []byte) {
	for _, put := range tx.contentPuts {
		if put.digest == digest {
			return
		}
	}
	tx.contentPuts = append(tx.contentPuts, stagedContent{digest: digest, canonical: canonical})
}

// validateEventPayload runs the fail-closed staging checks shared by
// AppendEvent and AppendEntry and returns the canonical payload bytes and
// their digest:
//
//  1. strict UTF-8/JSON parse (INVALID_JSON);
//  2. payload is a JSON object (SCHEMA_FIELD_UNKNOWN);
//  3. schema_version is inside the ledger kind's frozen whitelist
//     (SCHEMA_VERSION_UNSUPPORTED — Contract §16.6 schema authority);
//  4. integer-only JCS canonicalization (NON_INTEGER_NUMBER);
//  5. the payload's own event_id and sequence fields, when present, match
//     the entry identity (REF_MISMATCH);
//  6. for state-event payloads, from_state→to_state is an edge of the
//     frozen state machine (ILLEGAL_STATE_TRANSITION).
//
// Full DTO field validation (closed field sets, x-digest preimages) belongs
// to the strict DTO parsers of internal/contract and the GMS-201+ services;
// this layer guarantees that only schema-known, transition-legal, exactly
// addressed payloads can enter a ledger.
func (tx *Tx) validateEventPayload(kind LedgerKind, eventID string, sequence uint64, payload []byte) ([]byte, string, error) {
	spec, ok := ledgerKinds[kind]
	if !ok {
		return nil, "", newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum (Contract §16.6)", kind)
	}
	canonical, digest, obj, err := canonicalize(tx.mgr.registry, payload)
	if err != nil {
		return nil, "", err
	}
	if obj == nil {
		return nil, "", newError(tx.mgr.registry, ReasonSchemaFieldUnknown, "%s payload must be a JSON object", kind)
	}
	schemaVersion, _ := contract.AsString(obj["schema_version"])
	allowed := false
	for _, candidate := range spec.payloadSchemas {
		if candidate == schemaVersion {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, "", newError(tx.mgr.registry, ReasonSchemaVersionUnsupported,
			"%s ledger accepts payload schemas %v, got %q (Contract §16.6)", kind, spec.payloadSchemas, schemaVersion)
	}
	if id, present := contract.AsString(obj["event_id"]); present && id != eventID {
		return nil, "", newError(tx.mgr.registry, ReasonRefMismatch,
			"payload event_id %q does not match entry event id %q", id, eventID)
	}
	for _, field := range []string{"event_sequence", "activation_sequence"} {
		raw, present := obj[field]
		if !present {
			continue
		}
		number, isNumber := raw.(json.Number)
		if !isNumber {
			return nil, "", newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "payload field %q must be an integer", field)
		}
		var value uint64
		if _, err := fmt.Sscanf(number.String(), "%d", &value); err != nil {
			return nil, "", newError(tx.mgr.registry, ReasonSchemaEnumInvalid, "payload field %q must be a non-negative integer", field)
		}
		if value != sequence {
			return nil, "", newError(tx.mgr.registry, ReasonRefMismatch,
				"payload %s %d does not match entry sequence %d", field, value, sequence)
		}
	}
	if table, isStateEvent := payloadStateTables[schemaVersion]; isStateEvent {
		from, _ := contract.AsString(obj["from_state"])
		to, _ := contract.AsString(obj["to_state"])
		if !legalTransition(table, from, to) {
			return nil, "", newError(tx.mgr.registry, ReasonIllegalStateTransition,
				"%s: %s → %s is not an edge of the frozen state machine (Contract §9)", schemaVersion, from, to)
		}
	}
	return canonical, digest, nil
}

// canonicalize parses payload strictly, applies the DTO-level hashing
// normalization, canonicalizes to integer-only JCS bytes and digests them
// (all via internal/contract — the single canonicalization authority).
func canonicalize(registry ReasonRegistry, payload []byte) (canonical []byte, digest string, obj map[string]any, err error) {
	value, err := contract.ParseJSONStrict(payload)
	if err != nil {
		return nil, "", nil, newError(registry, ReasonInvalidJSON, "payload is not valid UTF-8 JSON: %v", err)
	}
	canonical, err = contract.JCS(contract.NormalizeForHashing(value))
	if err != nil {
		var canonicalErr *contract.CanonicalizationError
		if errors.As(err, &canonicalErr) {
			return nil, "", nil, newError(registry, canonicalErr.ReasonCode, "payload cannot enter the hashed core: %s", canonicalErr.ReasonCode)
		}
		return nil, "", nil, err
	}
	obj, _ = contract.AsObject(value)
	return canonical, contract.DigestBytes(canonical), obj, nil
}
