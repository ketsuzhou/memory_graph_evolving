package ledger

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
)

// MemoryStore is the in-memory reference adapter for the ledger ports.
//
// DURABILITY: NOT PRODUCTION. All state lives in process memory and is lost
// on crash or restart; there is no write-ahead log, no snapshot and no
// fsync. The adapter exists to make the Contract §13.1 / GMS §2.9
// transaction semantics — atomic commit/rollback, append-only history,
// idempotency, exact multi-head CAS, outbox binding — executable and
// testable behind the real port seams. Shipping the skill-evolution write
// path on this adapter is forbidden: a durable Store adapter (crash-atomic
// commit, e.g. one ACID database transaction per WithinTx) is a release
// gate.
//
// Concurrency: one mutex guards every map, so each port operation is
// atomic; the Manager additionally serializes its whole validate+apply
// phase through Lock/Unlock (the commitSerializer seam). Reads hand out
// deep copies — callers can never mutate authoritative state through a
// returned value.
type MemoryStore struct {
	registry ReasonRegistry

	mu      sync.Mutex // guards the maps below
	ledgers map[string][]Entry
	content map[string][]byte
	idem    map[string]IdempotencyRecord
	heads   map[string]headState
	outbox  map[string]OutboxRecord

	commitMu sync.Mutex // commitSerializer seam for the Manager
}

type headState struct {
	seq    uint64
	digest string
}

// Compile-time interface checks. MemoryStore deliberately does NOT
// implement TxManager: transactions are driven by Manager (transaction.go),
// which is the domain/storage seam a durable adapter plugs into.
var (
	_ Store             = (*MemoryStore)(nil)
	_ commitSerializer  = (*MemoryStore)(nil)
	_ compensatingStore = (*MemoryStore)(nil)
)

// NewMemoryStore builds the reference adapter. It fails closed unless every
// reason code the ports can emit exists in the digest-verified registry.
func NewMemoryStore(registry ReasonRegistry) (*MemoryStore, error) {
	if err := verifyMappedReasonCodes(registry); err != nil {
		return nil, err
	}
	return &MemoryStore{
		registry: registry,
		ledgers:  make(map[string][]Entry),
		content:  make(map[string][]byte),
		idem:     make(map[string]IdempotencyRecord),
		heads:    make(map[string]headState),
		outbox:   make(map[string]OutboxRecord),
	}, nil
}

func (s *MemoryStore) fail(code, format string, args ...any) error {
	return newError(s.registry, code, format, args...)
}

func ledgerKey(kind LedgerKind, stream string) string {
	return string(kind) + "\x1f" + stream
}

func headKey(kind HeadKind, key string) string {
	return string(kind) + "\x1f" + key
}

// ---------------------------------------------------------------------------
// Ledger (append-only)
// ---------------------------------------------------------------------------

// Append enforces append-only semantics: the sequence must be exactly
// head+1. Re-appending the exact entry that currently occupies the slot is
// an idempotent no-op (Contract §13.2); anything else that touches an
// occupied or gapped slot is rejected with the kind's canonical
// sequence-conflict code. There is no API that can rewrite history.
func (s *MemoryStore) Append(entry Entry) error {
	spec, ok := ledgerKinds[entry.Ledger]
	if !ok {
		return s.fail(ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum (Contract §16.6)", entry.Ledger)
	}
	if spec.singleStream && entry.Stream != "" {
		return s.fail(ReasonSchemaEnumInvalid, "ledger %s is a single global stream (GMS §2.6), stream %q must be empty", entry.Ledger, entry.Stream)
	}
	if entry.EventID == "" {
		return s.fail(ReasonRequiredFieldMissing, "entry event id must be non-empty")
	}
	if !isValidDigest(entry.PayloadDigest) {
		return s.fail(ReasonSchemaEnumInvalid, "entry payload digest %q must match sha256:<64 lowercase hex>", entry.PayloadDigest)
	}
	if entry.Sequence == 0 {
		return s.fail(ReasonSchemaEnumInvalid, "entry sequence must be a positive integer (GMS §2.6)")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := ledgerKey(entry.Ledger, entry.Stream)
	entries := s.ledgers[key]
	if entry.Sequence == uint64(len(entries))+1 {
		s.ledgers[key] = append(entries, entry)
		return nil
	}
	if entry.Sequence <= uint64(len(entries)) && entries[entry.Sequence-1] == entry {
		return nil // exact replay of an already committed entry
	}
	return s.fail(spec.sequenceConflictCode,
		"%s/%s: append at sequence %d refused (head=%d); sequence slots are monotone and never reused, and history cannot be overwritten (Contract §5.1.3)",
		entry.Ledger, entry.Stream, entry.Sequence, len(entries))
}

// Snapshot returns a deep copy of one stream's entries.
func (s *MemoryStore) Snapshot(kind LedgerKind, stream string) ([]Entry, error) {
	if _, ok := ledgerKinds[kind]; !ok {
		return nil, s.fail(ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum", kind)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.ledgers[ledgerKey(kind, stream)]
	out := make([]Entry, len(entries))
	copy(out, entries)
	return out, nil
}

// Head returns the last entry of one stream.
func (s *MemoryStore) Head(kind LedgerKind, stream string) (Entry, bool, error) {
	if _, ok := ledgerKinds[kind]; !ok {
		return Entry{}, false, s.fail(ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum", kind)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.ledgers[ledgerKey(kind, stream)]
	if len(entries) == 0 {
		return Entry{}, false, nil
	}
	return entries[len(entries)-1], true, nil
}

// EntryAt returns the entry at one sequence position, if committed.
func (s *MemoryStore) EntryAt(kind LedgerKind, stream string, sequence uint64) (Entry, bool, error) {
	if _, ok := ledgerKinds[kind]; !ok {
		return Entry{}, false, s.fail(ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum", kind)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.ledgers[ledgerKey(kind, stream)]
	if sequence == 0 || sequence > uint64(len(entries)) {
		return Entry{}, false, nil
	}
	return entries[sequence-1], true, nil
}

// ---------------------------------------------------------------------------
// ContentStore (content-addressed)
// ---------------------------------------------------------------------------

// Put stores canonical bytes under their SHA-256 digest. Identical bytes
// address to the same key, so re-puts are no-ops (the write surface stays
// additive; content is never mutated in place).
func (s *MemoryStore) Put(canonical []byte) (string, error) {
	digest := contract.DigestBytes(canonical)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, present := s.content[digest]; !present {
		s.content[digest] = append([]byte(nil), canonical...)
	}
	return digest, nil
}

// Get returns a copy of the canonical bytes stored under digest.
func (s *MemoryStore) Get(digest string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blob, present := s.content[digest]
	if !present {
		return nil, false, nil
	}
	return append([]byte(nil), blob...), true, nil
}

// ---------------------------------------------------------------------------
// IdempotencyStore
// ---------------------------------------------------------------------------

// Record stores rec. Same key + same digest is an idempotent no-op; the
// same key with a different digest is IDEMPOTENCY_CONFLICT (Contract
// §13.2).
func (s *MemoryStore) Record(rec IdempotencyRecord) error {
	if rec.Key == "" {
		return s.fail(ReasonRequiredFieldMissing, "idempotency key must be non-empty")
	}
	if !isValidDigest(rec.RequestDigest) {
		return s.fail(ReasonSchemaEnumInvalid, "request digest %q must match sha256:<64 lowercase hex>", rec.RequestDigest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, present := s.idem[rec.Key]; present {
		if prior.RequestDigest == rec.RequestDigest {
			return nil
		}
		return s.fail(ReasonIdempotencyConflict,
			"idempotency key %s recorded with request digest %s, got %s (Contract §13.2)", rec.Key, prior.RequestDigest, rec.RequestDigest)
	}
	s.idem[rec.Key] = IdempotencyRecord{
		Key:           rec.Key,
		RequestDigest: rec.RequestDigest,
		Outcome:       append([]byte(nil), rec.Outcome...),
	}
	return nil
}

// Lookup returns a copy of the recorded outcome, if any.
func (s *MemoryStore) Lookup(key string) (IdempotencyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, present := s.idem[key]
	if !present {
		return IdempotencyRecord{}, false, nil
	}
	rec.Outcome = append([]byte(nil), rec.Outcome...)
	return rec, true, nil
}

// ---------------------------------------------------------------------------
// HeadStore (exact CAS)
// ---------------------------------------------------------------------------

// CompareAndSwap swaps the head when and only when the stored (sequence,
// digest) exactly equals the expected pair (GMS §2.1: exact expected
// head/ref/sequence, never a name or timestamp). The new sequence must
// advance by exactly one (monotone, no reuse). Sequence 0 with an empty
// digest denotes the absent head.
func (s *MemoryStore) CompareAndSwap(kind HeadKind, key string, expectedSeq uint64, expectedDigest string, newSeq uint64, newDigest string) error {
	conflictCode, ok := headKinds[kind]
	if !ok {
		return s.fail(ReasonSchemaEnumInvalid, "unknown head kind %q outside the closed enum", kind)
	}
	if key == "" {
		return s.fail(ReasonRequiredFieldMissing, "head key must be non-empty")
	}
	if expectedDigest != "" && !isValidDigest(expectedDigest) {
		return s.fail(ReasonSchemaEnumInvalid, "expected digest %q must be empty or match sha256:<64 lowercase hex>", expectedDigest)
	}
	if !isValidDigest(newDigest) {
		return s.fail(ReasonSchemaEnumInvalid, "new digest %q must match sha256:<64 lowercase hex>", newDigest)
	}
	if newSeq != expectedSeq+1 {
		return s.fail(conflictCode, "head %s/%s: new sequence %d must be exactly expected %d + 1 (monotone, no reuse)", kind, key, newSeq, expectedSeq)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	hkey := headKey(kind, key)
	current, present := s.heads[hkey]
	if !present {
		if expectedSeq == 0 && expectedDigest == "" {
			s.heads[hkey] = headState{seq: newSeq, digest: newDigest}
			return nil
		}
		return s.fail(conflictCode,
			"head %s/%s is absent but expected (%d, %s); frozen expectation is stale (Contract §13.4)", kind, key, expectedSeq, expectedDigest)
	}
	if current.seq != expectedSeq || current.digest != expectedDigest {
		return s.fail(conflictCode,
			"head %s/%s is (%d, %s) but expected (%d, %s); frozen expectation is stale (Contract §13.4)",
			kind, key, current.seq, current.digest, expectedSeq, expectedDigest)
	}
	s.heads[hkey] = headState{seq: newSeq, digest: newDigest}
	return nil
}

// GetHead returns the current (sequence, digest) of one head.
func (s *MemoryStore) GetHead(kind HeadKind, key string) (uint64, string, bool, error) {
	if _, ok := headKinds[kind]; !ok {
		return 0, "", false, s.fail(ReasonSchemaEnumInvalid, "unknown head kind %q outside the closed enum", kind)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, present := s.heads[headKey(kind, key)]
	if !present {
		return 0, "", false, nil
	}
	return current.seq, current.digest, true, nil
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

// Enqueue stores a pending record. The same outbox key with the same
// binding is an idempotent no-op; a different binding conflicts (GMS §2.6).
// Binding to a resolvable activation ledger entry is enforced by the
// Manager's commit (cross-port check).
func (s *MemoryStore) Enqueue(rec OutboxRecord) error {
	if !isValidDigest(rec.OutboxKey) {
		return s.fail(ReasonSchemaEnumInvalid, "outbox key %q must match sha256:<64 lowercase hex> (Contract §7.13)", rec.OutboxKey)
	}
	if !isValidDigest(rec.EventDigest) {
		return s.fail(ReasonSchemaEnumInvalid, "outbox event digest %q must match sha256:<64 lowercase hex>", rec.EventDigest)
	}
	if rec.ProjectionTarget == "" {
		return s.fail(ReasonRequiredFieldMissing, "outbox projection target must be non-empty")
	}
	if rec.DeliveryState != DeliveryPending {
		return s.fail(ReasonSchemaEnumInvalid, "outbox records are enqueued %q only", rec.DeliveryState)
	}
	if _, ok := ledgerKinds[rec.Ledger]; !ok {
		return s.fail(ReasonSchemaEnumInvalid, "unknown ledger kind %q outside the closed enum", rec.Ledger)
	}
	if rec.ActivationSequence == 0 {
		return s.fail(ReasonSchemaEnumInvalid, "outbox activation sequence must be a positive integer")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, present := s.outbox[rec.OutboxKey]; present {
		if prior.Ledger == rec.Ledger && prior.ActivationSequence == rec.ActivationSequence && prior.EventDigest == rec.EventDigest {
			return nil
		}
		return s.fail(ReasonIdempotencyConflict,
			"outbox key %s already recorded with a different binding (GMS §2.6: same outbox key, different digest MUST conflict)", rec.OutboxKey)
	}
	s.outbox[rec.OutboxKey] = rec
	return nil
}

// GetOutbox returns a copy of one record by key.
func (s *MemoryStore) GetOutbox(outboxKey string) (OutboxRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, present := s.outbox[outboxKey]
	return rec, present, nil
}

// Pending returns the pending records in deterministic
// (ActivationSequence, OutboxKey) order.
func (s *MemoryStore) Pending() ([]OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending []OutboxRecord
	for _, rec := range s.outbox {
		if rec.DeliveryState == DeliveryPending {
			pending = append(pending, rec)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].ActivationSequence != pending[j].ActivationSequence {
			return pending[i].ActivationSequence < pending[j].ActivationSequence
		}
		return pending[i].OutboxKey < pending[j].OutboxKey
	})
	return pending, nil
}

// MarkDelivered records a successful delivery. The activation sequence and
// event digest MUST match the recorded event (DIGEST_MISMATCH otherwise);
// re-delivery is an idempotent no-op and never mints a new ActivationEvent
// (GMS §2.6).
func (s *MemoryStore) MarkDelivered(outboxKey string, activationSequence uint64, eventDigest string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, present := s.outbox[outboxKey]
	if !present {
		return s.fail(ReasonRefMismatch, "no outbox record for key %s", outboxKey)
	}
	if rec.EventDigest != eventDigest || rec.ActivationSequence != activationSequence {
		return s.fail(ReasonDigestMismatch,
			"delivery of outbox key %s claims (sequence %d, digest %s) but the record fixes (sequence %d, digest %s)",
			outboxKey, activationSequence, eventDigest, rec.ActivationSequence, rec.EventDigest)
	}
	if rec.DeliveryState == DeliveryDelivered {
		return nil
	}
	rec.DeliveryState = DeliveryDelivered
	s.outbox[outboxKey] = rec
	return nil
}

// ---------------------------------------------------------------------------
// commitSerializer + compensatingStore seams (unexported, Manager-only)
// ---------------------------------------------------------------------------

// Lock/Unlock serialize the Manager's validate+apply phase (see
// commitSerializer in transaction.go).
func (s *MemoryStore) Lock()   { s.commitMu.Lock() }
func (s *MemoryStore) Unlock() { s.commitMu.Unlock() }

// undoContentPut removes a blob inserted by the in-flight commit.
func (s *MemoryStore) undoContentPut(digest string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.content, digest)
}

// undoAppend removes an entry appended by the in-flight commit; replayed
// (already-present) appends are left untouched.
func (s *MemoryStore) undoAppend(entry Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ledgerKey(entry.Ledger, entry.Stream)
	entries := s.ledgers[key]
	if n := len(entries); n > 0 && entries[n-1] == entry {
		s.ledgers[key] = entries[:n-1]
	}
}

// undoHeadCAS restores a head to its pre-commit state (absent when
// prevExisted is false).
func (s *MemoryStore) undoHeadCAS(kind HeadKind, key string, prevSeq uint64, prevDigest string, prevExisted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hkey := headKey(kind, key)
	if !prevExisted {
		delete(s.heads, hkey)
		return
	}
	s.heads[hkey] = headState{seq: prevSeq, digest: prevDigest}
}

// undoOutbox removes an outbox record enqueued by the in-flight commit.
func (s *MemoryStore) undoOutbox(outboxKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.outbox, outboxKey)
}

// undoIdempotency removes an idempotency record staged by the in-flight
// commit; pre-existing records are left untouched.
func (s *MemoryStore) undoIdempotency(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.idem, key)
}

// ---------------------------------------------------------------------------
// Contract reason registry adapter
// ---------------------------------------------------------------------------

// ContractReasonRegistry adapts the digest-verified reason policy loaded by
// internal/contract (Contract §13.7.1) to the ReasonRegistry port gate.
// Construction: load the policy with contract.LoadSystemReasonPolicy and
// wrap it; the frozen registry digest is verified on load and fails closed
// on mismatch.
type ContractReasonRegistry struct {
	Policy *contract.ReasonPolicy
}

// Verify reports whether code exists in the closed registry.
func (r *ContractReasonRegistry) Verify(code string) error {
	if r == nil || r.Policy == nil {
		return errors.New("ledger: nil reason policy")
	}
	if _, err := r.Policy.Lookup(code); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	return nil
}
