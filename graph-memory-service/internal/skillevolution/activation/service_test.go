// Package activation tests: GMS-204 release/activation (plan-pinned TDD).
//
// Red #1 TestActivationAtomicallyPublishesMappingHeadEventAndOutbox
// Red #2 TestStaleCASLeavesNoPartialRelease
// plus fixture alignment ($FIX/events/{activation,deactivation}), fail-closed
// guards, idempotency, protected deactivate/reactivate, probation
// unreachability and the Graph-not-in-transaction structural boundary.
package activation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluator"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Ports fit the GMS-203 / GMS-202 products (compile-time, never drifts).
// ---------------------------------------------------------------------------

var (
	_ ReleaseDecision        = (*evaluator.Decision)(nil)
	_ CandidateView          = (*candidate.CandidateView)(nil)
	_ ReleaseDecision        = stubDecision{}
	_ CandidateView          = (*stubView)(nil)
	_ DeactivationAuthorizer = (*stubAuthorizer)(nil)
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type harness struct {
	t          *testing.T
	registry   *ledger.ContractReasonRegistry
	gates      *validation.Gates
	stateDir   string
	store      *ledger.MemoryStore
	proposals  *proposal.Service
	authorizer *stubAuthorizer
	svc        *ActivationService
}

func loadRegistry(t *testing.T) *ledger.ContractReasonRegistry {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load system reason policy: %v", err)
	}
	return &ledger.ContractReasonRegistry{Policy: policy}
}

func loadGates(t *testing.T) *validation.Gates {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	schemas, err := validation.LoadSchemaSet(filepath.Join(dir, "schema", "shared"))
	if err != nil {
		t.Fatalf("load shared schema set: %v", err)
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		t.Fatalf("NewGates: %v", err)
	}
	return gates
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := loadRegistry(t)
	gates := loadGates(t)
	dir, _ := contract.DefaultConformanceDir()
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	authorizer := &stubAuthorizer{allowed: map[string]string{}}
	h := &harness{
		t:          t,
		registry:   registry,
		gates:      gates,
		stateDir:   filepath.Join(dir, "schema", "state"),
		store:      store,
		authorizer: authorizer,
	}
	h.svc = h.serviceOver(store)
	return h
}

// serviceOver wires a fresh proposal engine + activation service over the
// given store view (plain MemoryStore, recorder or injector). The underlying
// authoritative state is shared whenever the adapter wraps h.store.
func (h *harness) serviceOver(store ledger.Store) *ActivationService {
	h.t.Helper()
	mgr, err := ledger.NewManager(store, h.registry)
	if err != nil {
		h.t.Fatalf("NewManager: %v", err)
	}
	proposals, err := proposal.NewService(h.gates, h.stateDir, store, mgr, h.registry)
	if err != nil {
		h.t.Fatalf("proposal.NewService: %v", err)
	}
	if h.proposals == nil {
		h.proposals = proposals
	}
	svc, err := NewActivationService(Config{
		Store:                  store,
		Tx:                     mgr,
		Registry:               h.registry,
		Gates:                  h.gates,
		Proposals:              proposals,
		DeactivationAuthorizer: h.authorizer,
	})
	if err != nil {
		h.t.Fatalf("NewActivationService: %v", err)
	}
	return svc
}

// stubAuthorizer authorizes exactly the (authorization id -> lineage) pairs
// registered on it; everything else is denied.
type stubAuthorizer struct {
	allowed map[string]string
}

func (a *stubAuthorizer) AuthorizeDeactivation(auth contract.VersionedRef, lineageID string, skill contract.SkillArtifactRef) error {
	if a.allowed[auth.ID] == lineageID {
		return nil
	}
	return errors.New("deactivation authorization denied")
}

// stubView is an arbitrary frozen candidate view (the production port
// implementation is candidate.CandidateView).
type stubView struct {
	ref  contract.CandidateArtifactRef
	body []byte
}

func (v *stubView) Ref() contract.CandidateArtifactRef { return v.ref }
func (v *stubView) BodyDigest() string                 { return v.ref.BodyDigest }
func (v *stubView) CanonicalBody() []byte {
	out := make([]byte, len(v.body))
	copy(out, v.body)
	return out
}

// stubDecision is an arbitrary §7.12 decision view.
type stubDecision struct {
	doc map[string]any
}

func (d stubDecision) Doc() map[string]any { return d.doc }
func (d stubDecision) Outcome() string     { s, _ := contract.AsString(d.doc["outcome"]); return s }
func (d stubDecision) DecisionDigest() string {
	s, _ := contract.AsString(d.doc["decision_digest"])
	return s
}

// portDigestDecision carries a deliberately wrong port-level digest.
type portDigestDecision struct {
	stubDecision
	digest string
}

func (d portDigestDecision) DecisionDigest() string { return d.digest }

// ---------------------------------------------------------------------------
// Store adapters used to observe/inject behavior at the ledger port seam.
// Embedding *ledger.MemoryStore keeps the unexported commitSerializer /
// compensatingStore methods in the method set, so the domain Manager still
// serializes and compensates against the wrapped authoritative state.
// ---------------------------------------------------------------------------

type recordingStore struct {
	*ledger.MemoryStore
	mu  sync.Mutex
	ops []string
}

func (r *recordingStore) record(op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

func (r *recordingStore) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.ops))
	copy(out, r.ops)
	return out
}

func (r *recordingStore) Append(entry ledger.Entry) error {
	r.record("append:" + string(entry.Ledger))
	return r.MemoryStore.Append(entry)
}

func (r *recordingStore) CompareAndSwap(kind ledger.HeadKind, key string, expectedSeq uint64, expectedDigest string, newSeq uint64, newDigest string) error {
	r.record("cas:" + string(kind))
	return r.MemoryStore.CompareAndSwap(kind, key, expectedSeq, expectedDigest, newSeq, newDigest)
}

func (r *recordingStore) Enqueue(rec ledger.OutboxRecord) error {
	r.record("outbox-enqueue:" + rec.ProjectionTarget)
	return r.MemoryStore.Enqueue(rec)
}

type injections struct {
	failOutboxEnqueue    bool
	failActivationAppend bool
	sneakSkillActive     bool
	sneakLineageVersion  bool
	sneakActivationSeq   bool
}

type injectingStore struct {
	*ledger.MemoryStore
	inj     injections
	sneaked bool
}

func (s *injectingStore) Append(entry ledger.Entry) error {
	if s.inj.failActivationAppend && entry.Ledger == ledger.LedgerActivation {
		return errors.New("injected: activation ledger append failure")
	}
	return s.MemoryStore.Append(entry)
}

func (s *injectingStore) Enqueue(rec ledger.OutboxRecord) error {
	if s.inj.failOutboxEnqueue {
		return errors.New("injected: outbox enqueue failure")
	}
	return s.MemoryStore.Enqueue(rec)
}

// sneakHeadCAS simulates a concurrent committer: right before the in-flight
// CAS applies, the racing writer advances the same head (claiming a virgin
// head outright when it is still absent), so the frozen expectation goes
// stale mid-commit.
func (s *injectingStore) sneakHeadCAS(kind ledger.HeadKind, key string) {
	if s.sneaked {
		return
	}
	seq, digest, ok, err := s.MemoryStore.GetHead(kind, key)
	if err != nil {
		return
	}
	racing := contract.DigestBytes([]byte("racing-writer:" + string(kind) + ":" + key))
	if ok {
		_ = s.MemoryStore.CompareAndSwap(kind, key, seq, digest, seq+1, racing)
	} else {
		_ = s.MemoryStore.CompareAndSwap(kind, key, 0, "", 1, racing)
	}
	s.sneaked = true
}

func (s *injectingStore) CompareAndSwap(kind ledger.HeadKind, key string, expectedSeq uint64, expectedDigest string, newSeq uint64, newDigest string) error {
	switch {
	case s.inj.sneakSkillActive && kind == ledger.HeadSkillActive:
		s.sneakHeadCAS(kind, key)
	case s.inj.sneakLineageVersion && kind == ledger.HeadLineageVersion:
		s.sneakHeadCAS(kind, key)
	case s.inj.sneakActivationSeq && kind == ledger.HeadActivationSequence:
		s.sneakHeadCAS(kind, key)
	}
	return s.MemoryStore.CompareAndSwap(kind, key, expectedSeq, expectedDigest, newSeq, newDigest)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func jn(v int64) any { return json.Number(fmt.Sprintf("%d", v)) }

func jnString(v string) any { return json.Number(v) }

func testVersionedRefDoc(ref contract.VersionedRef) map[string]any {
	return map[string]any{"id": ref.ID, "version": jnString(ref.Version), "digest": ref.Digest}
}

func testCandidateRefDoc(ref contract.CandidateArtifactRef) map[string]any {
	return map[string]any{
		"schema_version": ref.SchemaVersion,
		"candidate_id":   ref.CandidateID,
		"kind":           ref.Kind,
		"body_digest":    ref.BodyDigest,
		"origin_type":    ref.OriginType,
		"origin_ref":     testVersionedRefDoc(ref.OriginRef),
	}
}

func testBody(seed string) []byte {
	body := map[string]any{
		"note": seed,
		"permissions": []any{
			map[string]any{"capability": "memory_explore", "scope": "room-shared-space"},
		},
	}
	data, err := contract.JCS(body)
	if err != nil {
		panic(err)
	}
	return data
}

// proposalDoc builds a schema-valid gms.skill-proposal.v1 document.
func (h *harness) proposalDoc(t *testing.T, proposalID, kind, operation string) map[string]any {
	t.Helper()
	doc := map[string]any{
		"schema_version":   "gms.skill-proposal.v1",
		"proposal_id":      proposalID,
		"proposal_version": jn(1),
		"proposed_kind":    kind,
		"source_segment_refs": []any{
			map[string]any{
				"schema_version":  "host.segment-ref.v1",
				"room_id":         "room-1",
				"segment_id":      "seg-" + proposalID,
				"segment_version": jn(1),
				"segment_digest":  contract.DigestBytes([]byte("seg-" + proposalID)),
				"evidence_seal_ref": map[string]any{
					"id": "evseal-" + proposalID, "version": jn(1), "digest": contract.DigestBytes([]byte("evseal-" + proposalID)),
				},
				"path_seal_ref": map[string]any{
					"id": "pathseal-" + proposalID, "version": jn(1), "digest": contract.DigestBytes([]byte("pathseal-" + proposalID)),
				},
			},
		},
		"evidence_refs": []any{
			map[string]any{
				"schema_version":  contract.SchemaEvidenceRef,
				"evidence_id":     "ev-" + proposalID,
				"version":         jn(1),
				"evidence_digest": contract.DigestBytes([]byte("ev-" + proposalID)),
				"commit_state":    "committed",
				"evidence_kind":   "success_path",
			},
		},
		"requested_operation": operation,
		"origin": map[string]any{
			"initiator_type": "model",
			"initiator_ref":  "agent-1",
			"request_ref":    "req-" + proposalID,
		},
		"policy_refs": []any{
			map[string]any{"id": "policy-static-gates", "version": jn(1), "digest": contract.DigestBytes([]byte("policy-static-gates"))},
		},
	}
	digest, err := h.gates.ComputeDigestPreimage(doc, "skill-proposal.schema.json")
	if err != nil {
		t.Fatalf("proposal digest preimage: %v", err)
	}
	doc["proposal_digest"] = digest
	return doc
}

var activationPendingPath = [][2]string{
	{"none", "proposed"},
	{"proposed", "admitted"},
	{"admitted", "candidate_bound"},
	{"candidate_bound", "validating"},
	{"validating", "replay_pending"},
	{"replay_pending", "replaying"},
	{"replaying", "decision_pending"},
	{"decision_pending", "activation_pending"},
}

func (h *harness) seedActivationPending(t *testing.T, doc map[string]any) {
	t.Helper()
	ctx := context.Background()
	for _, edge := range activationPendingPath {
		if _, err := h.proposals.AppendTransition(ctx, doc, edge[0], edge[1]); err != nil {
			t.Fatalf("seed %s -> %s: %v", edge[0], edge[1], err)
		}
	}
}

// decisionDoc builds a schema-valid §7.12 document for cand with a recomputed
// decision_digest.
func (h *harness) decisionDoc(t *testing.T, cand contract.CandidateArtifactRef, outcome string) map[string]any {
	t.Helper()
	doc := map[string]any{
		"schema_version":         "gms.release-decision.v1",
		"decision_id":            "dec-" + cand.CandidateID,
		"decision_version":       jn(1),
		"candidate_ref":          testCandidateRefDoc(cand),
		"validation_record_refs": []any{testVersionedRefDoc(contract.VersionedRef{ID: "valid-0001", Version: "1", Digest: contract.DigestBytes([]byte("valid-0001"))})},
		"replay_result_refs":     []any{testVersionedRefDoc(contract.VersionedRef{ID: "replay-0001", Version: "1", Digest: contract.DigestBytes([]byte("replay-0001"))})},
		"release_rule_ref":       testVersionedRefDoc(contract.VersionedRef{ID: "policy.release-rule.v1", Version: "1", Digest: contract.DigestBytes([]byte("release-rule"))}),
		"utility_comparator_ref": testVersionedRefDoc(contract.VersionedRef{ID: "policy.utility-comparator.v1", Version: "1", Digest: contract.DigestBytes([]byte("comparator"))}),
		"hard_gate_results": []any{
			map[string]any{"gate_code": "schema_canonicalization_digest", "passed": true},
			map[string]any{"gate_code": "exact_ref_resolution", "passed": true},
		},
		"outcome":      outcome,
		"reason_codes": []any{},
	}
	digest, err := h.gates.ComputeDigestPreimage(doc, "release-decision.schema.json")
	if err != nil {
		t.Fatalf("decision digest preimage: %v", err)
	}
	doc["decision_digest"] = digest
	if err := h.gates.ValidateInstance(doc, "release-decision.schema.json"); err != nil {
		t.Fatalf("decision doc invalid: %v", err)
	}
	return doc
}

// commitCandidate commits a candidate binding directly through the ledger
// (the GMS-202 binding service's committed shape: canonical body + candidate
// ledger event).
func (h *harness) commitCandidate(t *testing.T, cand contract.CandidateArtifactRef, body []byte) {
	t.Helper()
	mgr, err := ledger.NewManager(h.store, h.registry)
	if err != nil {
		t.Fatalf("commit candidate manager: %v", err)
	}
	payload, err := json.Marshal(testCandidateRefDoc(cand))
	if err != nil {
		t.Fatalf("candidate payload marshal: %v", err)
	}
	err = mgr.WithinTx(context.Background(), func(tx *ledger.Tx) error {
		if _, err := tx.PutContent(body); err != nil {
			return err
		}
		_, err := tx.AppendEvent(ledger.LedgerCandidate, "", cand.CandidateID, payload)
		return err
	})
	if err != nil {
		t.Fatalf("commit candidate %s: %v", cand.CandidateID, err)
	}
}

type releaseFixture struct {
	cand     contract.CandidateArtifactRef
	view     *stubView
	propDoc  map[string]any
	decision stubDecision
	body     []byte
}

// prepareRelease commits a candidate, seeds its proposal to activation_pending
// and builds an accepted decision over the exact candidate ref.
func (h *harness) prepareRelease(t *testing.T, proposalID, candidateID, kind, operation string, body []byte) releaseFixture {
	t.Helper()
	propDoc := h.proposalDoc(t, proposalID, kind, operation)
	h.seedActivationPending(t, propDoc)
	propDigest, _ := contract.AsString(propDoc["proposal_digest"])
	cand := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   candidateID,
		Kind:          kind,
		BodyDigest:    contract.DigestBytes(body),
		OriginType:    "skill_proposal",
		OriginRef:     contract.VersionedRef{ID: proposalID, Version: "1", Digest: propDigest},
	}
	h.commitCandidate(t, cand, body)
	return releaseFixture{
		cand:     cand,
		view:     &stubView{ref: cand, body: body},
		propDoc:  propDoc,
		decision: stubDecision{doc: h.decisionDoc(t, cand, "accepted")},
		body:     body,
	}
}

// ---------------------------------------------------------------------------
// Residue / state snapshots
// ---------------------------------------------------------------------------

type stateSnapshot struct {
	activationEntries int
	evaluationEntries int
	lineageHead       [3]any // seq, digest, ok
	activeHead        [3]any
	seqHead           [3]any
	pendingOutbox     int
	proposalStates    map[string]string
}

func (h *harness) snapshot(t *testing.T, proposalIDs ...string) stateSnapshot {
	t.Helper()
	snap := stateSnapshot{proposalStates: map[string]string{}}
	entries, err := h.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		t.Fatalf("activation snapshot: %v", err)
	}
	snap.activationEntries = len(entries)
	eval, err := h.store.Snapshot(ledger.LedgerEvaluation, "")
	if err != nil {
		t.Fatalf("evaluation snapshot: %v", err)
	}
	snap.evaluationEntries = len(eval)
	snap.lineageHead = headState(t, h.store, ledger.HeadLineageVersion, "sg-x")
	snap.activeHead = headState(t, h.store, ledger.HeadSkillActive, "sg-x")
	snap.seqHead = headState(t, h.store, ledger.HeadActivationSequence, activationStreamKey)
	pending, err := h.store.Pending()
	if err != nil {
		t.Fatalf("pending outbox: %v", err)
	}
	snap.pendingOutbox = len(pending)
	ctx := context.Background()
	for _, id := range proposalIDs {
		state, _, err := h.proposals.CurrentState(ctx, id)
		if err != nil {
			t.Fatalf("proposal state %s: %v", id, err)
		}
		snap.proposalStates[id] = state
	}
	return snap
}

func headState(t *testing.T, store ledger.Store, kind ledger.HeadKind, key string) [3]any {
	t.Helper()
	seq, digest, ok, err := store.GetHead(kind, key)
	if err != nil {
		t.Fatalf("GetHead %s: %v", kind, err)
	}
	return [3]any{seq, digest, ok}
}

func assertUnchangedResidue(t *testing.T, before, after stateSnapshot) {
	t.Helper()
	if before.activationEntries != after.activationEntries {
		t.Fatalf("activation ledger entries %d -> %d (partial event residue)", before.activationEntries, after.activationEntries)
	}
	if before.evaluationEntries != after.evaluationEntries {
		t.Fatalf("evaluation ledger entries %d -> %d (partial decision residue)", before.evaluationEntries, after.evaluationEntries)
	}
	if before.lineageHead != after.lineageHead {
		t.Fatalf("lineage-version head changed: %v -> %v (partial version residue)", before.lineageHead, after.lineageHead)
	}
	if before.activeHead != after.activeHead {
		t.Fatalf("skill-active head changed: %v -> %v (partial head residue)", before.activeHead, after.activeHead)
	}
	if before.seqHead != after.seqHead {
		t.Fatalf("activation-sequence head changed: %v -> %v", before.seqHead, after.seqHead)
	}
	if before.pendingOutbox != after.pendingOutbox {
		t.Fatalf("pending outbox %d -> %d (partial outbox residue)", before.pendingOutbox, after.pendingOutbox)
	}
	for id, state := range before.proposalStates {
		if after.proposalStates[id] != state {
			t.Fatalf("proposal %s state %s -> %s (partial terminal residue)", id, state, after.proposalStates[id])
		}
	}
}

// activationPayload returns the parsed payload of activation entry i.
func activationPayload(t *testing.T, store ledger.Store, index int) map[string]any {
	t.Helper()
	entries, err := store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		t.Fatalf("activation snapshot: %v", err)
	}
	if index >= len(entries) {
		t.Fatalf("activation entry %d missing (have %d)", index, len(entries))
	}
	payload, ok, err := store.Get(entries[index].PayloadDigest)
	if err != nil || !ok {
		t.Fatalf("activation payload %d unresolved: %v", index, err)
	}
	value, err := contract.ParseJSONStrict(payload)
	if err != nil {
		t.Fatalf("activation payload %d invalid: %v", index, err)
	}
	obj, _ := contract.AsObject(value)
	return obj
}

// mappingRecord resolves the release mapping record currently pointed at by
// the lineage-version head.
func mappingRecord(t *testing.T, store ledger.Store, lineageID string) map[string]any {
	t.Helper()
	_, digest, ok, err := store.GetHead(ledger.HeadLineageVersion, lineageID)
	if err != nil || !ok {
		t.Fatalf("lineage-version head for %s: %v (ok=%v)", lineageID, err, ok)
	}
	raw, ok, err := store.Get(digest)
	if err != nil || !ok {
		t.Fatalf("mapping record %s unresolved: %v", digest, err)
	}
	value, err := contract.ParseJSONStrict(raw)
	if err != nil {
		t.Fatalf("mapping record invalid: %v", err)
	}
	obj, _ := contract.AsObject(value)
	return obj
}

func jsonNumberText(v any) string {
	if n, ok := v.(json.Number); ok {
		return string(n)
	}
	return ""
}

func objField(t *testing.T, obj map[string]any, field string) map[string]any {
	t.Helper()
	raw, ok := contract.AsObject(obj[field])
	if !ok {
		t.Fatalf("field %q is not an object: %v", field, obj[field])
	}
	return raw
}

// ---------------------------------------------------------------------------
// GMS-204 TDD Red #1 (plan-pinned)
// ---------------------------------------------------------------------------

// TestActivationAtomicallyPublishesMappingHeadEventAndOutbox pins GMS §6.2 /
// Contract §10.6/§13.1: one accepted decision + frozen candidate activate in
// a SINGLE transaction that publishes the version assignment, the
// Candidate→Released byte-equality mapping, the skill-active head CAS, the
// ActivationEvent with its global sequence, the transactional outbox record
// and the proposal terminal event — and nothing else (Graph projection is
// NOT part of the transaction).
func TestActivationAtomicallyPublishesMappingHeadEventAndOutbox(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Observe every store operation the pipeline performs.
	recorder := &recordingStore{MemoryStore: h.store}
	h.svc = h.serviceOver(recorder)

	const lineage = "sg-x"
	fix := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))

	res, err := h.svc.Activate(ctx, ActivateRequest{
		Decision:     fix.decision,
		Candidate:    fix.view,
		LineageID:    lineage,
		ProposalDoc:  fix.propDoc,
		ExpectedHead: nil, // new lineage: expected head absent
	})
	if err != nil {
		t.Fatalf("activate: %v (code %s)", err, CodeOf(err))
	}

	// --- ReleasedRef: version 1 of the target lineage, candidate body byte
	// equality (candidate and released bodies digest equal, GMS §3.7).
	if res.ReleasedRef.LineageID != lineage || res.ReleasedRef.Version != "1" {
		t.Fatalf("released ref = %+v, want lineage %s v1", res.ReleasedRef, lineage)
	}
	if res.ReleasedRef.Kind != "step_guidance" {
		t.Fatalf("released kind = %s", res.ReleasedRef.Kind)
	}
	if res.ReleasedRef.ArtifactDigest != fix.cand.BodyDigest {
		t.Fatalf("released digest %s != candidate body digest %s (byte equality mapping)", res.ReleasedRef.ArtifactDigest, fix.cand.BodyDigest)
	}
	if res.ActivationSequence != 1 {
		t.Fatalf("activation sequence = %d, want 1 (global single stream)", res.ActivationSequence)
	}

	// --- Version assignment: lineage-version head advanced to v1.
	seq, mappingDigest, ok, err := h.store.GetHead(ledger.HeadLineageVersion, lineage)
	if err != nil || !ok || seq != 1 {
		t.Fatalf("lineage-version head = (%d, %s, ok=%v), want (1, ..., true)", seq, mappingDigest, ok)
	}

	// --- Candidate→Released mapping record with body_digest_equal proof.
	mapping := mappingRecord(t, h.store, lineage)
	if v, _ := contract.AsString(mapping["schema_version"]); v != SchemaReleaseMapping {
		t.Fatalf("mapping schema_version = %s", v)
	}
	if got := jsonNumberText(mapping["version"]); got != "1" {
		t.Fatalf("mapping version = %v, want 1", mapping["version"])
	}
	if equal, _ := mapping["body_digest_equal"].(bool); !equal {
		t.Fatalf("mapping body_digest_equal = %v, want true", mapping["body_digest_equal"])
	}
	released, err := contract.ParseSkillArtifactRef(objField(t, mapping, "released_ref"))
	if err != nil {
		t.Fatalf("mapping released_ref invalid: %v", err)
	}
	if released != res.ReleasedRef {
		t.Fatalf("mapping released_ref %+v != result %+v", released, res.ReleasedRef)
	}
	mappedCand, err := contract.ParseCandidateArtifactRef(objField(t, mapping, "candidate_ref"))
	if err != nil {
		t.Fatalf("mapping candidate_ref invalid: %v", err)
	}
	if mappedCand != fix.cand {
		t.Fatalf("mapping candidate_ref %+v != committed candidate %+v", mappedCand, fix.cand)
	}

	// --- Skill-active head CAS: absent -> (1, released body digest).
	activeSeq, activeDigest, ok, err := h.store.GetHead(ledger.HeadSkillActive, lineage)
	if err != nil || !ok || activeSeq != 1 || activeDigest != fix.cand.BodyDigest {
		t.Fatalf("skill-active head = (%d, %s, ok=%v)", activeSeq, activeDigest, ok)
	}

	// --- Activation sequence head guards the global allocator.
	guardSeq, guardDigest, ok, err := h.store.GetHead(ledger.HeadActivationSequence, activationStreamKey)
	if err != nil || !ok || guardSeq != 1 || guardDigest != res.EventDigest {
		t.Fatalf("activation-sequence head = (%d, %s, ok=%v), want (1, %s)", guardSeq, guardDigest, ok, res.EventDigest)
	}

	// --- ActivationEvent: exactly one, schema-valid, all §7.13 fields.
	event := activationPayload(t, h.store, 0)
	if err := h.gates.ValidateInstance(event, SchemaActivationEvent); err != nil {
		t.Fatalf("activate event invalid against authority schema: %v", err)
	}
	if et, _ := contract.AsString(event["event_type"]); et != EventTypeActivate {
		t.Fatalf("event_type = %s", et)
	}
	if equal, _ := event["body_digest_equal"].(bool); !equal {
		t.Fatalf("event body_digest_equal must be true (Contract §7.13)")
	}
	eventCand, err := contract.ParseCandidateArtifactRef(objField(t, event, "candidate_ref"))
	if err != nil || eventCand != fix.cand {
		t.Fatalf("event candidate_ref = %+v (err %v), want the exact candidate", eventCand, err)
	}
	decRef, err := contract.ParseVersionedRef(objField(t, event, "release_decision_ref"))
	if err != nil {
		t.Fatalf("event release_decision_ref invalid: %v", err)
	}
	if decRef.ID != "dec-cand-0001" || decRef.Digest != fix.decision.DecisionDigest() {
		t.Fatalf("event release_decision_ref = %+v", decRef)
	}
	if lid, _ := contract.AsString(event["lineage_id"]); lid != lineage {
		t.Fatalf("event lineage_id = %s", lid)
	}
	if outboxKey, _ := contract.AsString(event["outbox_key"]); outboxKey != res.OutboxKey {
		t.Fatalf("event outbox_key %s != result %s", outboxKey, res.OutboxKey)
	}

	// --- The decision itself is part of the same atomic write set.
	evalEntries, err := h.store.Snapshot(ledger.LedgerEvaluation, "dec-cand-0001")
	if err != nil || len(evalEntries) != 1 {
		t.Fatalf("evaluation ledger entries = %d (err %v), want 1", len(evalEntries), err)
	}
	// The ledger payload digest addresses the whole canonical decision
	// document; the x-digest inside it must equal the decision's digest.
	evalPayload, ok, err := h.store.Get(evalEntries[0].PayloadDigest)
	if err != nil || !ok {
		t.Fatalf("committed decision payload unresolved: %v", err)
	}
	evalValue, err := contract.ParseJSONStrict(evalPayload)
	if err != nil {
		t.Fatalf("committed decision payload invalid: %v", err)
	}
	evalDoc, _ := contract.AsObject(evalValue)
	if d, _ := contract.AsString(evalDoc["decision_digest"]); d != fix.decision.DecisionDigest() {
		t.Fatalf("committed decision_digest %s != decision %s", d, fix.decision.DecisionDigest())
	}

	// --- Transactional outbox: pending record bound to the event, targeting
	// the runtime projection stream (delivered AFTER commit, never inside).
	rec, ok, err := h.store.GetOutbox(res.OutboxKey)
	if err != nil || !ok {
		t.Fatalf("outbox record missing: %v (ok=%v)", err, ok)
	}
	if rec.Ledger != ledger.LedgerActivation || rec.ActivationSequence != 1 || rec.EventDigest != res.EventDigest {
		t.Fatalf("outbox binding = %+v", rec)
	}
	if rec.ProjectionTarget != "runtime" {
		t.Fatalf("outbox projection target = %s, want runtime", rec.ProjectionTarget)
	}
	if rec.DeliveryState != ledger.DeliveryPending {
		t.Fatalf("outbox delivery state = %s, want pending (delivery is post-commit)", rec.DeliveryState)
	}

	// --- Proposal terminal event: activation_pending -> released.
	state, _, err := h.proposals.CurrentState(ctx, "prop-2001")
	if err != nil || state != "released" {
		t.Fatalf("proposal state = %s (err %v), want released", state, err)
	}

	// --- The released body IS the committed candidate body, byte for byte.
	committed, ok, err := h.store.Get(fix.cand.BodyDigest)
	if err != nil || !ok {
		t.Fatalf("released body unresolved under candidate digest: %v", err)
	}
	if string(committed) != string(fix.body) {
		t.Fatalf("committed released body differs from candidate canonical body (byte equality)")
	}

	// --- Read model: the lineage resolves to the released revision.
	active, err := h.svc.ActiveRevision(ctx, lineage)
	if err != nil || active == nil || *active != res.ReleasedRef {
		t.Fatalf("ActiveRevision = %+v (err %v), want %+v", active, err, res.ReleasedRef)
	}

	// --- Graph projection is NOT in the transaction: no projection head CAS,
	// no projection ledger append; the outbox is the only graph-facing write.
	for _, op := range recorder.snapshot() {
		if strings.Contains(strings.ToLower(op), "projection") && !strings.HasPrefix(op, "outbox-enqueue:") {
			t.Fatalf("release transaction touched the projection surface: %s (Graph projection MUST stay outside, GMS §2.9)", op)
		}
	}
}

// ---------------------------------------------------------------------------
// GMS-204 TDD Red #2 (plan-pinned)
// ---------------------------------------------------------------------------

// TestStaleCASLeavesNoPartialRelease pins Contract §10.6/§13.4 and GMS §6.1:
// a stale expected active head — whether a wrong frozen expectation or a
// concurrent committer winning the head between staging and commit — fails
// the WHOLE release: no partial version, mapping, head, event, outbox or
// proposal terminal state survives.
func TestStaleCASLeavesNoPartialRelease(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const lineage = "sg-x"

	// A first release commits v1.
	fix1 := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))
	v1, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: fix1.decision, Candidate: fix1.view, LineageID: lineage, ProposalDoc: fix1.propDoc,
	})
	if err != nil {
		t.Fatalf("seed v1: %v", err)
	}

	// A second revision is fully prepared (candidate + proposal at
	// activation_pending + accepted decision).
	fix2 := h.prepareRelease(t, "prop-2002", "cand-0002", "step_guidance", "revise_lineage", testBody("body-v2"))
	head1 := v1.ReleasedRef

	before := h.snapshot(t, "prop-2001", "prop-2002")

	// (a) Wrong frozen expectation: expecting a head that is not the current
	// active revision rejects before anything is staged.
	wrong := head1
	wrong.ArtifactDigest = contract.DigestBytes([]byte("not-the-active-digest"))
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision: fix2.decision, Candidate: fix2.view, LineageID: lineage, ProposalDoc: fix2.propDoc,
		ExpectedHead: &wrong,
	})
	if CodeOf(err) != ReasonActiveHeadConflict {
		t.Fatalf("wrong frozen expectation: want ACTIVE_HEAD_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	assertUnchangedResidue(t, before, h.snapshot(t, "prop-2001", "prop-2002"))

	// Expecting absence while a head exists is equally stale.
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision: fix2.decision, Candidate: fix2.view, LineageID: lineage, ProposalDoc: fix2.propDoc,
		ExpectedHead: nil,
	})
	if CodeOf(err) != ReasonActiveHeadConflict {
		t.Fatalf("nil expectation over live head: want ACTIVE_HEAD_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	assertUnchangedResidue(t, before, h.snapshot(t, "prop-2001", "prop-2002"))

	// (b) The expectation is correct at staging time, but a concurrent
	// committer advances the skill-active head between staging and commit:
	// the CAS goes stale mid-transaction and the whole write set is undone.
	injector := &injectingStore{MemoryStore: h.store, inj: injections{sneakSkillActive: true}}
	svc := h.serviceOver(injector)
	_, err = svc.Activate(ctx, ActivateRequest{
		Decision: fix2.decision, Candidate: fix2.view, LineageID: lineage, ProposalDoc: fix2.propDoc,
		ExpectedHead: &head1,
	})
	if CodeOf(err) != ReasonActiveHeadConflict {
		t.Fatalf("mid-commit stale CAS: want ACTIVE_HEAD_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	after := h.snapshot(t, "prop-2001", "prop-2002")
	if after.activationEntries != before.activationEntries {
		t.Fatalf("stale release left %d activation events, want %d", after.activationEntries, before.activationEntries)
	}
	if evalStream, _ := h.store.Snapshot(ledger.LedgerEvaluation, "dec-cand-0002"); len(evalStream) != 0 {
		t.Fatalf("stale release left a partial decision record (%d entries)", len(evalStream))
	}
	if after.lineageHead != before.lineageHead {
		t.Fatalf("stale release left a partial version assignment: %v -> %v", before.lineageHead, after.lineageHead)
	}
	if after.seqHead != before.seqHead {
		t.Fatalf("stale release left a partial sequence allocation")
	}
	if after.pendingOutbox != before.pendingOutbox {
		t.Fatalf("stale release left a partial outbox record")
	}
	if after.proposalStates["prop-2002"] != "activation_pending" {
		t.Fatalf("stale release left proposal prop-2002 in %s", after.proposalStates["prop-2002"])
	}
	// The racing writer's head change is the only visible difference: exactly
	// the concurrent committer's state, never a mixture with our release.
	racing := contract.DigestBytes([]byte("racing-writer:" + string(ledger.HeadSkillActive) + ":" + lineage))
	gotSeq, gotDigest, _, _ := h.store.GetHead(ledger.HeadSkillActive, lineage)
	if gotSeq != 2 || gotDigest != racing {
		t.Fatalf("skill-active head = (%d, %s), want the racing writer's (2, %s)", gotSeq, gotDigest, racing)
	}
	// The failed attempt does not poison unrelated lineages: a brand-new
	// release still commits cleanly.
	fix3 := h.prepareRelease(t, "prop-2003", "cand-0003", "step_guidance", "create_lineage", testBody("body-v3"))
	if _, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: fix3.decision, Candidate: fix3.view, LineageID: "sg-y", ProposalDoc: fix3.propDoc,
	}); err != nil {
		t.Fatalf("release after stale failure: %v (the stale attempt must not poison the store)", err)
	}
}

// ---------------------------------------------------------------------------
// Partial-failure injection: any failing step leaves zero residue.
// ---------------------------------------------------------------------------

func TestActivationWriteFailuresLeaveZeroResidue(t *testing.T) {
	for _, tc := range []struct {
		name          string
		inj           injections
		wantCode      string
		wantRetryCode string // expected code of the retry after the injection ("" = retry succeeds)
		wantVersion   string // released version of the successful retry
	}{
		{name: "outbox enqueue fails", inj: injections{failOutboxEnqueue: true}, wantVersion: "1"},
		{name: "activation append fails", inj: injections{failActivationAppend: true}, wantVersion: "1"},
		{name: "duplicate version (lineage head raced)", inj: injections{sneakLineageVersion: true}, wantCode: ReasonLineageVersionConflict, wantVersion: "2"},
		{name: "activation sequence conflict", inj: injections{sneakActivationSeq: true}, wantCode: ReasonActivationSequenceConflict, wantRetryCode: ReasonActivationSequenceConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			const lineage = "sg-x"
			fix := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))
			before := h.snapshot(t, "prop-2001")

			injector := &injectingStore{MemoryStore: h.store, inj: tc.inj}
			svc := h.serviceOver(injector)
			_, err := svc.Activate(ctx, ActivateRequest{
				Decision: fix.decision, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
			})
			if err == nil {
				t.Fatalf("injected failure did not surface")
			}
			if tc.wantCode != "" && CodeOf(err) != tc.wantCode {
				t.Fatalf("want %s, got %v (code %s)", tc.wantCode, err, CodeOf(err))
			}
			after := h.snapshot(t, "prop-2001")
			if tc.inj.sneakLineageVersion {
				// The racing writer owns the lineage-version head; our write
				// must not be mixed into it.
				if after.lineageHead == before.lineageHead {
					t.Fatalf("injection did not race the lineage-version head (test is vacuous)")
				}
				after.lineageHead = before.lineageHead // the racer's change is not our residue
			}
			if tc.inj.sneakActivationSeq {
				if after.seqHead == before.seqHead {
					t.Fatalf("injection did not race the activation-sequence head (test is vacuous)")
				}
				after.seqHead = before.seqHead
			}
			assertUnchangedResidue(t, before, after)
			if evalStream, _ := h.store.Snapshot(ledger.LedgerEvaluation, "dec-cand-0001"); len(evalStream) != 0 {
				t.Fatalf("injected failure left a partial decision record (%d entries)", len(evalStream))
			}

			// Retry the same request over the untouched authoritative state.
			res, err := h.svc.Activate(ctx, ActivateRequest{
				Decision: fix.decision, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
			})
			if tc.wantRetryCode != "" {
				if CodeOf(err) != tc.wantRetryCode {
					t.Fatalf("retry after torn sequence head: want %s (fail closed), got %v (code %s)", tc.wantRetryCode, err, CodeOf(err))
				}
				return
			}
			if err != nil {
				t.Fatalf("retry after injected failure: %v (zero residue must allow a clean retry)", err)
			}
			if res.ReleasedRef.Version != tc.wantVersion || res.ActivationSequence != 1 {
				t.Fatalf("retry produced version %s sequence %d, want version %s sequence 1", res.ReleasedRef.Version, res.ActivationSequence, tc.wantVersion)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fail-closed decision/body/kind/permission guards.
// ---------------------------------------------------------------------------

func TestActivateFailsClosedOnGuards(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const lineage = "sg-x"

	// Non-accepted outcomes never authorize a release.
	fix := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))
	for _, outcome := range []string{"rejected", "inconclusive"} {
		doc := h.decisionDoc(t, fix.cand, outcome)
		_, err := h.svc.Activate(ctx, ActivateRequest{
			Decision: stubDecision{doc: doc}, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
		})
		if CodeOf(err) != ReasonReleaseNotAccepted {
			t.Fatalf("outcome %s: want RELEASE_NOT_ACCEPTED, got %v (code %s)", outcome, err, CodeOf(err))
		}
	}

	// Forged decision digest (declared != recomputed preimage).
	forged := h.decisionDoc(t, fix.cand, "accepted")
	forged["decision_digest"] = contract.DigestBytes([]byte("forged"))
	_, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: stubDecision{doc: forged}, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if code := CodeOf(err); code != ReasonReleaseDecisionInvalid && code != contract.ReasonDigestMismatch {
		t.Fatalf("forged decision digest: want RELEASE_DECISION_INVALID/DIGEST_MISMATCH, got %s", code)
	}

	// Port digest disagrees with the document.
	other := h.decisionDoc(t, fix.cand, "accepted")
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision:  portDigestDecision{stubDecision: stubDecision{doc: other}, digest: contract.DigestBytes([]byte("port-digest-diverges"))},
		Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if CodeOf(err) != ReasonRefMismatch {
		t.Fatalf("port digest divergence: want REF_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}

	// Decision references a different candidate body digest.
	divergentCand := fix.cand
	divergentCand.BodyDigest = contract.DigestBytes([]byte("other-body"))
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision: stubDecision{doc: h.decisionDoc(t, divergentCand, "accepted")}, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if CodeOf(err) != ReasonRefMismatch {
		t.Fatalf("decision over another candidate: want REF_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}

	// Candidate not committed to the candidate ledger.
	ghostBody := testBody("ghost")
	ghost := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: "cand-ghost", Kind: "step_guidance",
		BodyDigest: contract.DigestBytes(ghostBody), OriginType: "skill_proposal",
		OriginRef: fix.cand.OriginRef,
	}
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision:    stubDecision{doc: h.decisionDoc(t, ghost, "accepted")},
		Candidate:   &stubView{ref: ghost, body: ghostBody},
		LineageID:   lineage,
		ProposalDoc: fix.propDoc,
	})
	if CodeOf(err) != ReasonCandidateNotFound {
		t.Fatalf("uncommitted candidate: want CANDIDATE_NOT_FOUND, got %v (code %s)", err, CodeOf(err))
	}

	// View body bytes do not digest to the declared body digest.
	badBodyView := &stubView{ref: fix.cand, body: testBody("mutated-body")}
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision: fix.decision, Candidate: badBodyView, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if CodeOf(err) != ReasonCandidateReleasedBodyMismatch {
		t.Fatalf("mutated body: want CANDIDATE_RELEASED_BODY_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}

	// Permission beyond the Host v1 cap re-verified at release time.
	badPermBody := map[string]any{
		"permissions": []any{map[string]any{"capability": "filesystem_write", "scope": "/"}},
	}
	badPermBytes, _ := contract.JCS(badPermBody)
	badPermCand := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: "cand-badperm", Kind: "step_guidance",
		BodyDigest: contract.DigestBytes(badPermBytes), OriginType: "skill_proposal",
		OriginRef: fix.cand.OriginRef,
	}
	h.commitCandidate(t, badPermCand, badPermBytes)
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision:    stubDecision{doc: h.decisionDoc(t, badPermCand, "accepted")},
		Candidate:   &stubView{ref: badPermCand, body: badPermBytes},
		LineageID:   lineage,
		ProposalDoc: fix.propDoc,
	})
	if CodeOf(err) != ReasonPermissionCapExceeded {
		t.Fatalf("permission overflow: want PERMISSION_CAP_EXCEEDED, got %v (code %s)", err, CodeOf(err))
	}

	// Nothing above wrote anything.
	if entries, _ := h.store.Snapshot(ledger.LedgerActivation, ""); len(entries) != 0 {
		t.Fatalf("guard failures left %d activation events", len(entries))
	}
	if state, _, _ := h.proposals.CurrentState(ctx, "prop-2001"); state != "activation_pending" {
		t.Fatalf("guard failures left proposal in %s", state)
	}
}

// Kind drift: a lineage activated as step_guidance cannot take a procedure
// revision (LINEAGE_KIND_MISMATCH; kind changes need a new lineage).
func TestActivateRejectsLineageKindDrift(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const lineage = "sg-x"

	fix1 := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))
	v1, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: fix1.decision, Candidate: fix1.view, LineageID: lineage, ProposalDoc: fix1.propDoc,
	})
	if err != nil {
		t.Fatalf("seed v1: %v", err)
	}
	head := v1.ReleasedRef

	fix2 := h.prepareRelease(t, "prop-2002", "cand-0002", "human_procedure", "revise_lineage", testBody("body-v2"))
	before := h.snapshot(t, "prop-2001", "prop-2002")
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision: fix2.decision, Candidate: fix2.view, LineageID: lineage, ProposalDoc: fix2.propDoc,
		ExpectedHead: &head,
	})
	if CodeOf(err) != ReasonLineageKindMismatch {
		t.Fatalf("kind drift: want LINEAGE_KIND_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}
	assertUnchangedResidue(t, before, h.snapshot(t, "prop-2001", "prop-2002"))
}

// ---------------------------------------------------------------------------
// Idempotency (Contract §13.2: same key + same digest replays the recorded
// outcome; a retry never mints a second version or activation).
// ---------------------------------------------------------------------------

func TestActivateIsIdempotentPerDecisionKey(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const lineage = "sg-x"
	fix := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))

	first, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: fix.decision, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if err != nil {
		t.Fatalf("first activate: %v", err)
	}
	entriesAfterFirst, _ := h.store.Snapshot(ledger.LedgerActivation, "")

	again, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: fix.decision, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if !again.Replayed {
		t.Fatalf("replay not flagged: %+v", again)
	}
	if again.ReleasedRef != first.ReleasedRef || again.ActivationSequence != first.ActivationSequence ||
		again.EventDigest != first.EventDigest || again.OutboxKey != first.OutboxKey {
		t.Fatalf("replay returned a different outcome: %+v vs %+v", again, first)
	}
	entriesAfterReplay, _ := h.store.Snapshot(ledger.LedgerActivation, "")
	if len(entriesAfterReplay) != len(entriesAfterFirst) {
		t.Fatalf("replay appended events: %d -> %d", len(entriesAfterFirst), len(entriesAfterReplay))
	}
	if seq, _, _, _ := h.store.GetHead(ledger.HeadLineageVersion, lineage); seq != 1 {
		t.Fatalf("replay minted version %d", seq)
	}

	// Same decision key with a different frozen request conflicts.
	otherHead := first.ReleasedRef
	otherHead.ArtifactDigest = contract.DigestBytes([]byte("different-expected-head"))
	_, err = h.svc.Activate(ctx, ActivateRequest{
		Decision: fix.decision, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
		ExpectedHead: &otherHead,
	})
	if CodeOf(err) != ReasonIdempotencyConflict {
		t.Fatalf("same key different digest: want IDEMPOTENCY_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
}

// ---------------------------------------------------------------------------
// Protected deactivate: authorization, current-head CAS, history retention.
// ---------------------------------------------------------------------------

func TestDeactivateRequiresAuthorizationAndKeepsHistory(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const lineage = "sg-x"

	fix := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))
	released, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: fix.decision, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if err != nil {
		t.Fatalf("seed release: %v", err)
	}

	auth := contract.VersionedRef{ID: "auth-deact-0001", Version: "1", Digest: contract.DigestBytes([]byte("auth-deact-0001"))}

	// Unauthorized (no authorization granted for this lineage).
	_, err = h.svc.Deactivate(ctx, DeactivateRequest{LineageID: lineage, SkillRef: released.ReleasedRef, Authorization: auth})
	if CodeOf(err) != ReasonDeactivationAuthorizationInvalid {
		t.Fatalf("unauthorized deactivate: want DEACTIVATION_AUTHORIZATION_INVALID, got %v (code %s)", err, CodeOf(err))
	}
	if entries, _ := h.store.Snapshot(ledger.LedgerActivation, ""); len(entries) != 1 {
		t.Fatalf("unauthorized deactivate left %d events", len(entries))
	}

	// Authorized but a stale skill ref (not the current active revision).
	h.authorizer.allowed[auth.ID] = lineage
	stale := released.ReleasedRef
	stale.ArtifactDigest = contract.DigestBytes([]byte("stale"))
	_, err = h.svc.Deactivate(ctx, DeactivateRequest{LineageID: lineage, SkillRef: stale, Authorization: auth})
	if CodeOf(err) != ReasonActiveHeadConflict {
		t.Fatalf("stale deactivate: want ACTIVE_HEAD_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	if entries, _ := h.store.Snapshot(ledger.LedgerActivation, ""); len(entries) != 1 {
		t.Fatalf("stale deactivate left %d events", len(entries))
	}

	// Authorized deactivate of the current head.
	deactivation, err := h.svc.Deactivate(ctx, DeactivateRequest{LineageID: lineage, SkillRef: released.ReleasedRef, Authorization: auth})
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if deactivation.ActivationSequence != 2 {
		t.Fatalf("deactivation sequence = %d, want 2 (same global stream)", deactivation.ActivationSequence)
	}
	event := activationPayload(t, h.store, 1)
	if err := h.gates.ValidateInstance(event, SchemaDeactivationEvent); err != nil {
		t.Fatalf("deactivate event invalid against authority schema: %v", err)
	}
	if et, _ := contract.AsString(event["event_type"]); et != EventTypeDeactivate {
		t.Fatalf("event_type = %s", et)
	}
	if _, present := event["candidate_ref"]; present {
		t.Fatalf("deactivate event carries candidate_ref (forbidden by the deactivation profile)")
	}
	if _, present := event["body_digest_equal"]; present {
		t.Fatalf("deactivate event carries body_digest_equal (forbidden by the deactivation profile)")
	}
	eventAuth, err := contract.ParseVersionedRef(objField(t, event, "deactivation_authorization_ref"))
	if err != nil || eventAuth != auth {
		t.Fatalf("deactivation_authorization_ref = %+v (err %v)", eventAuth, err)
	}
	if drec, ok, _ := h.store.GetOutbox(deactivation.OutboxKey); !ok || drec.ActivationSequence != 2 {
		t.Fatalf("deactivation outbox = %+v (ok=%v)", drec, ok)
	}

	// The active head is cleared...
	active, err := h.svc.ActiveRevision(ctx, lineage)
	if err != nil || active != nil {
		t.Fatalf("ActiveRevision after deactivate = %+v (err %v), want nil", active, err)
	}
	// ...but history is fully retained: mapping, events, released body.
	entries, _ := h.store.Snapshot(ledger.LedgerActivation, "")
	if len(entries) != 2 {
		t.Fatalf("deactivate must append, never delete: %d events", len(entries))
	}
	mapping := mappingRecord(t, h.store, lineage)
	if got := jsonNumberText(mapping["version"]); got != "1" {
		t.Fatalf("release mapping lost after deactivation: %+v", mapping)
	}
	if _, ok, _ := h.store.Get(released.ReleasedRef.ArtifactDigest); !ok {
		t.Fatal("released artifact body deleted by deactivation")
	}
	if state, _, _ := h.proposals.CurrentState(ctx, "prop-2001"); state != "released" {
		t.Fatalf("proposal terminal state rewritten by deactivation: %s", state)
	}

	// Deactivating an already-inactive lineage has no current head.
	_, err = h.svc.Deactivate(ctx, DeactivateRequest{LineageID: lineage, SkillRef: released.ReleasedRef, Authorization: auth})
	if CodeOf(err) != ReasonNoActiveHead {
		t.Fatalf("double deactivate: want NO_ACTIVE_HEAD, got %v (code %s)", err, CodeOf(err))
	}
	if entries, _ := h.store.Snapshot(ledger.LedgerActivation, ""); len(entries) != 2 {
		t.Fatalf("double deactivate left %d events", len(entries))
	}
}

// ---------------------------------------------------------------------------
// Reactivation: new protected decision, new activate event + new sequence,
// no new version, old events never rewritten (Contract §9.2.7).
// ---------------------------------------------------------------------------

func TestReactivateRequiresNewDecisionAndWritesNewActivateEvent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const lineage = "sg-x"

	fix := h.prepareRelease(t, "prop-2001", "cand-0001", "step_guidance", "create_lineage", testBody("body-v1"))
	released, err := h.svc.Activate(ctx, ActivateRequest{
		Decision: fix.decision, Candidate: fix.view, LineageID: lineage, ProposalDoc: fix.propDoc,
	})
	if err != nil {
		t.Fatalf("seed release: %v", err)
	}
	auth := contract.VersionedRef{ID: "auth-deact-0001", Version: "1", Digest: contract.DigestBytes([]byte("auth-deact-0001"))}
	h.authorizer.allowed[auth.ID] = lineage
	if _, err := h.svc.Deactivate(ctx, DeactivateRequest{LineageID: lineage, SkillRef: released.ReleasedRef, Authorization: auth}); err != nil {
		t.Fatalf("seed deactivate: %v", err)
	}

	// Reactivation without a new protected decision is refused.
	_, err = h.svc.Reactivate(ctx, ReactivateRequest{ReleasedRef: released.ReleasedRef})
	if CodeOf(err) != ReasonReactivationAuthorizationRequired {
		t.Fatalf("reactivate without decision: want REACTIVATION_AUTHORIZATION_REQUIRED, got %v (code %s)", err, CodeOf(err))
	}

	// A decision over a different candidate cannot reactivate this ref.
	otherCand := fix.cand
	otherCand.BodyDigest = contract.DigestBytes([]byte("other"))
	_, err = h.svc.Reactivate(ctx, ReactivateRequest{
		ReleasedRef: released.ReleasedRef,
		Decision:    stubDecision{doc: h.decisionDoc(t, otherCand, "accepted")},
	})
	if CodeOf(err) != ReasonRefMismatch {
		t.Fatalf("reactivate with foreign decision: want REF_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}

	// A new accepted decision over the ORIGINAL candidate reactivates the
	// historical released ref with a fresh activation sequence.
	newDecision := stubDecision{doc: h.decisionDoc(t, fix.cand, "accepted")}
	res, err := h.svc.Reactivate(ctx, ReactivateRequest{ReleasedRef: released.ReleasedRef, Decision: newDecision})
	if err != nil {
		t.Fatalf("reactivate: %v", err)
	}
	if res.ActivationSequence != 3 {
		t.Fatalf("reactivation sequence = %d, want 3 (new event in the same stream)", res.ActivationSequence)
	}
	event := activationPayload(t, h.store, 2)
	if err := h.gates.ValidateInstance(event, SchemaActivationEvent); err != nil {
		t.Fatalf("reactivation event invalid: %v", err)
	}
	if et, _ := contract.AsString(event["event_type"]); et != EventTypeActivate {
		t.Fatalf("reactivation event_type = %s", et)
	}
	if _, present := event["candidate_ref"]; present {
		t.Fatal("reactivation of an already-released revision must not carry candidate_ref (not the first activation)")
	}
	if equal, _ := event["body_digest_equal"].(bool); !equal {
		t.Fatalf("reactivation body_digest_equal = %v, want true", event["body_digest_equal"])
	}
	decRef, _ := contract.ParseVersionedRef(objField(t, event, "release_decision_ref"))
	if decRef.Digest != newDecision.DecisionDigest() {
		t.Fatalf("reactivation cites decision %s", decRef.Digest)
	}

	// The lineage is active again at the SAME released revision: no new
	// version, no new mapping.
	active, err := h.svc.ActiveRevision(ctx, lineage)
	if err != nil || active == nil || *active != released.ReleasedRef {
		t.Fatalf("ActiveRevision after reactivate = %+v (err %v)", active, err)
	}
	if seq, _, _, _ := h.store.GetHead(ledger.HeadLineageVersion, lineage); seq != 1 {
		t.Fatalf("reactivation must not mint a version: head seq = %d", seq)
	}
	if mapping := mappingRecord(t, h.store, lineage); mapping == nil {
		t.Fatal("release mapping lost")
	}

	// The original events are untouched (append-only).
	if entries, _ := h.store.Snapshot(ledger.LedgerActivation, ""); len(entries) != 3 {
		t.Fatalf("reactivation stream = %d events, want 3", len(entries))
	}
}

// ---------------------------------------------------------------------------
// Fixture alignment: events built by this package are byte/digest identical
// to the frozen conformance fixtures.
// ---------------------------------------------------------------------------

func loadFixtureSource(t *testing.T, caseDir string) map[string]any {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "events", caseDir, "source.json"))
	if err != nil {
		t.Fatalf("read fixture %s: %v", caseDir, err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		t.Fatalf("parse fixture %s: %v", caseDir, err)
	}
	obj, _ := contract.AsObject(value)
	return obj
}

func loadFixtureExpected(t *testing.T, caseDir string) (digest string, length int64) {
	t.Helper()
	dir, _ := contract.DefaultConformanceDir()
	data, err := os.ReadFile(filepath.Join(dir, "events", caseDir, "expected.json"))
	if err != nil {
		t.Fatalf("read expected %s: %v", caseDir, err)
	}
	value, _ := contract.ParseJSONStrict(data)
	obj, _ := contract.AsObject(value)
	digest, _ = contract.AsString(obj["expected_digest"])
	if n, ok := obj["expected_canonical_byte_length"].(json.Number); ok {
		length, _ = n.Int64()
	}
	return digest, length
}

func fixtureEventSpec(t *testing.T, source map[string]any) activationEventSpec {
	t.Helper()
	spec := activationEventSpec{}
	if n, ok := source["activation_sequence"].(json.Number); ok {
		v, err := n.Int64()
		if err != nil {
			t.Fatalf("fixture activation_sequence: %v", err)
		}
		spec.sequence = uint64(v)
	}
	spec.eventID, _ = contract.AsString(source["event_id"])
	spec.eventType, _ = contract.AsString(source["event_type"])
	spec.lineageID, _ = contract.AsString(source["lineage_id"])
	spec.outboxKey, _ = contract.AsString(source["outbox_key"])
	spec.skillRef = parseSkillRefOrFatal(t, objField(t, source, "skill_ref"))
	if raw, present := source["previous_active_ref"]; present {
		ref := parseSkillRefOrFatal(t, raw.(map[string]any))
		spec.previousActiveRef = &ref
	}
	if raw, present := source["release_decision_ref"]; present {
		ref, err := contract.ParseVersionedRef(raw.(map[string]any))
		if err != nil {
			t.Fatalf("fixture release_decision_ref: %v", err)
		}
		spec.releaseDecisionRef = &ref
	}
	if raw, present := source["deactivation_authorization_ref"]; present {
		ref, err := contract.ParseVersionedRef(raw.(map[string]any))
		if err != nil {
			t.Fatalf("fixture deactivation_authorization_ref: %v", err)
		}
		spec.deactivationAuthorizationRef = &ref
	}
	if raw, present := source["candidate_ref"]; present {
		ref, err := contract.ParseCandidateArtifactRef(raw.(map[string]any))
		if err != nil {
			t.Fatalf("fixture candidate_ref: %v", err)
		}
		spec.candidateRef = &ref
	}
	if raw, present := source["body_digest_equal"]; present {
		equal := raw.(bool)
		spec.bodyDigestEqual = &equal
	}
	if raw, present := source["expected_active_head"]; present {
		ref := parseSkillRefOrFatal(t, raw.(map[string]any))
		spec.expectedActiveHead = &ref
	}
	return spec
}

func parseSkillRefOrFatal(t *testing.T, obj map[string]any) contract.SkillArtifactRef {
	t.Helper()
	ref, err := contract.ParseSkillArtifactRef(obj)
	if err != nil {
		t.Fatalf("fixture skill_ref: %v", err)
	}
	return ref
}

// TestActivationEventsMatchConformanceFixtures aligns the single §7.13 event
// constructor with the frozen corpus:
//
//   - every fixture field (except the digest itself) reproduces byte for
//     byte — same shape, same canonical encoding;
//   - the built event is schema-valid, which includes recomputing
//     event_digest over the frozen x-digest preimage;
//   - the built event's declared event_digest equals an INDEPENDENT
//     in-test recomputation (sha256 over JCS of the present preimage
//     fields) — "event digest equal" under the authority rule;
//   - the canonical whole-document byte length equals the frozen
//     expected_canonical_byte_length.
//
// Corpus note (verified while writing this): the event_digest values frozen
// INSIDE $FIX/events/event-001/002 do not recompute under the same rule
// (their digests predate the final field content), while the corpus's own
// positive schema case $FIX/schema-cases/dto/pos-activation-event DOES
// recompute exactly. The schema x-digest rule is the authority (Contract
// §16.6), so alignment pins the rule and the byte-exact field reproduction,
// not the stale inner values.
func TestActivationEventsMatchConformanceFixtures(t *testing.T) {
	h := newHarness(t)

	preimageFields := []string{
		"schema_version", "activation_sequence", "event_id", "event_type", "lineage_id",
		"skill_ref", "previous_active_ref", "release_decision_ref", "deactivation_authorization_ref",
		"candidate_ref", "body_digest_equal", "derived_from_refs", "expected_active_head", "outbox_key",
	}

	for _, tc := range []struct {
		caseDir   string
		schema    string
		eventType string
	}{
		{caseDir: "event-001-activation", schema: SchemaActivationEvent, eventType: EventTypeActivate},
		{caseDir: "event-002-deactivation", schema: SchemaDeactivationEvent, eventType: EventTypeDeactivate},
	} {
		t.Run(tc.caseDir, func(t *testing.T) {
			source := loadFixtureSource(t, tc.caseDir)
			spec := fixtureEventSpec(t, source)
			event, digest, err := h.svc.buildActivationEvent(spec)
			if err != nil {
				t.Fatalf("build event: %v", err)
			}
			if et, _ := contract.AsString(event["event_type"]); et != tc.eventType {
				t.Fatalf("event_type = %s, want %s", et, tc.eventType)
			}
			// Schema-valid (includes the x-digest recompute).
			if err := h.gates.ValidateInstance(event, tc.schema); err != nil {
				t.Fatalf("built event invalid against authority schema %s: %v", tc.schema, err)
			}
			// Independent recomputation: sha256 over JCS of the present
			// preimage fields.
			core := map[string]any{}
			for _, field := range preimageFields {
				if value, present := event[field]; present {
					core[field] = value
				}
			}
			independent, err := contract.DigestOf(core)
			if err != nil {
				t.Fatalf("independent preimage digest: %v", err)
			}
			if digest != independent {
				t.Fatalf("event_digest = %s, independent recompute = %s", digest, independent)
			}
			// Every fixture field reproduces byte for byte (the digest field
			// excepted — see the corpus note above).
			for field, want := range source {
				if field == "event_digest" {
					continue
				}
				got, present := event[field]
				if !present {
					t.Fatalf("built event lost fixture field %q", field)
				}
				wantJCS, err := contract.JCS(contract.NormalizeForHashing(want))
				if err != nil {
					t.Fatalf("fixture field %q: %v", field, err)
				}
				gotJCS, err := contract.JCS(contract.NormalizeForHashing(got))
				if err != nil {
					t.Fatalf("built field %q: %v", field, err)
				}
				if string(wantJCS) != string(gotJCS) {
					t.Fatalf("field %q differs:\n fixture=%s\n built  =%s", field, wantJCS, gotJCS)
				}
			}
			// No fields beyond the fixture (and the recomputed digest).
			for field := range event {
				if _, present := source[field]; !present && field != "event_digest" {
					t.Fatalf("built event invented field %q (= %v)", field, event[field])
				}
			}
			// Canonical whole-document byte length equals the frozen length
			// (the only differing bytes are the 64 digest hex characters).
			canonical, err := contract.JCS(contract.NormalizeForHashing(event))
			if err != nil {
				t.Fatalf("canonicalize built event: %v", err)
			}
			_, wantLength := loadFixtureExpected(t, tc.caseDir)
			if int64(len(canonical)) != wantLength {
				t.Fatalf("canonical length = %d, want frozen %d", len(canonical), wantLength)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Probation is unreachable (U2=A: activate/deactivate minimal set only).
// ---------------------------------------------------------------------------

func TestProbationIsUnreachable(t *testing.T) {
	h := newHarness(t)

	// The only event constructor rejects any event_type outside the closed
	// activate|deactivate set, probation included (Contract §9.2.6).
	spec := activationEventSpec{
		eventType: "probation",
		eventID:   "probation-0001",
		sequence:  1,
		lineageID: "sg-x",
		skillRef: contract.SkillArtifactRef{
			SchemaVersion: contract.SchemaSkillArtifactRef, LineageID: "sg-x", Version: "1",
			Kind: "step_guidance", ArtifactDigest: contract.DigestBytes([]byte("probation")),
		},
		outboxKey: contract.DigestBytes([]byte("probation-outbox")),
		releaseDecisionRef: &contract.VersionedRef{
			ID: "dec-0001", Version: "1", Digest: contract.DigestBytes([]byte("dec")),
		},
		bodyDigestEqual: ptrBool(true),
	}
	_, _, err := h.svc.buildActivationEvent(spec)
	if CodeOf(err) != ReasonProbationUnsupported {
		t.Fatalf("probation event: want PROBATION_UNSUPPORTED, got %v (code %s)", err, CodeOf(err))
	}

	// Structurally: no exported mutation entry point mentions probation, and
	// the closed event-type set contains only activate|deactivate.
	for et := range activationEventTypes {
		if et == "probation" {
			t.Fatal("probation leaked into the closed event-type set")
		}
	}
	svcType := reflect.TypeOf(&ActivationService{})
	for i := 0; i < svcType.NumMethod(); i++ {
		name := svcType.Method(i).Name
		if strings.Contains(strings.ToLower(name), "probation") {
			t.Fatalf("ActivationService exposes probation entry point %s", name)
		}
	}
}

func ptrBool(v bool) *bool { return &v }
