// Package projector tests: the GMS-205 Runtime projector (plan-pinned TDD).
//
// Red #1 TestProjectorCommitsGraphAndWatermarkAtomically
// Red #2 TestProjectionGapBlocksWithoutCandidateLeakage
// plus the conformance-fixture alignment ($FIX/events/event-003/004 and the
// neg-001/neg-002 projection batches), relation-source projection
// (similar_to, supported_by/refuted_by), deactivation history retention,
// candidate exclusion, rebuild identity and the disabled Curation stream.
package projector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Ports fit the sibling products (compile-time, never drifts).
// ---------------------------------------------------------------------------

var (
	_ EvidenceResolver                  = (*stubEvidence)(nil)
	_ similarity.EvidenceResolver       = (*stubEvidence)(nil)
	_ RecordSource                      = (*memRecordSource)(nil)
	_ activation.CandidateView          = (*stubCandidateView)(nil)
	_ activation.ReleaseDecision        = stubDecision{}
	_ activation.DeactivationAuthorizer = allowAuthorizer{}
)

// stubEvidence is the committed-evidence resolver (same shape as the merge
// harness).
type stubEvidence struct {
	mu      sync.Mutex
	records map[string]contract.EvidenceRef
}

func newStubEvidence() *stubEvidence {
	return &stubEvidence{records: map[string]contract.EvidenceRef{}}
}

func (e *stubEvidence) commit(ref contract.EvidenceRef) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records[ref.EvidenceID] = ref
}

func (e *stubEvidence) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ref, ok := e.records[id]
	return ref, ok, nil
}

// allowAuthorizer permits the ordinary deactivate -> v2 flow.
type allowAuthorizer struct{}

func (allowAuthorizer) AuthorizeDeactivation(contract.VersionedRef, string, contract.SkillArtifactRef) error {
	return nil
}

type stubCandidateView struct {
	ref  contract.CandidateArtifactRef
	body []byte
}

func (v *stubCandidateView) Ref() contract.CandidateArtifactRef { return v.ref }
func (v *stubCandidateView) BodyDigest() string                 { return v.ref.BodyDigest }
func (v *stubCandidateView) CanonicalBody() []byte {
	out := make([]byte, len(v.body))
	copy(out, v.body)
	return out
}

type stubDecision struct{ doc map[string]any }

func (d stubDecision) Doc() map[string]any { return d.doc }
func (d stubDecision) Outcome() string     { s, _ := contract.AsString(d.doc["outcome"]); return s }
func (d stubDecision) DecisionDigest() string {
	s, _ := contract.AsString(d.doc["decision_digest"])
	return s
}

// memRecordSource is an in-memory relation-source feed: the producer side
// (the similarity service, the claim assessor) hands committed records to
// the projector; the projector pulls them by cursor.
type memRecordSource struct {
	name    string
	mu      sync.Mutex
	records []SourceRecord
}

func newMemRecordSource(name string) *memRecordSource { return &memRecordSource{name: name} }

func (m *memRecordSource) SourceName() string { return m.name }

func (m *memRecordSource) add(record SourceRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, record)
}

func (m *memRecordSource) Records(after uint64) ([]SourceRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []SourceRecord
	for _, record := range m.records {
		if record.Sequence > after {
			out = append(out, record)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type harness struct {
	t             *testing.T
	registry      *ledger.ContractReasonRegistry
	gates         *validation.Gates
	confDir       string
	store         *ledger.MemoryStore
	mgr           ledger.TxManager
	proposals     *proposal.Service
	artifacts     *artifact.Service
	activations   *activation.ActivationService
	similaritySvc *similarity.Service
	evidence      *stubEvidence
	graph         *Graph
	storage       *GraphStorage
	svc           *Service
	simSource     *memRecordSource
	assessSource  *memRecordSource
	candidates    map[string]contract.CandidateArtifactRef
	releaseSeq    int
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load system reason policy: %v", err)
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	schemas, err := validation.LoadSchemaSet(filepath.Join(dir, "schema", "shared"))
	if err != nil {
		t.Fatalf("load shared schema set: %v", err)
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		t.Fatalf("new gates: %v", err)
	}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	mgr, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	proposals, err := proposal.NewService(gates, filepath.Join(dir, "schema", "state"), store, mgr, registry)
	if err != nil {
		t.Fatalf("proposal.NewService: %v", err)
	}
	artifacts, err := artifact.NewService(gates)
	if err != nil {
		t.Fatalf("artifact.NewService: %v", err)
	}
	evidence := newStubEvidence()
	activations, err := activation.NewActivationService(activation.Config{
		Store: store, Tx: mgr, Registry: registry, Gates: gates,
		Proposals: proposals, DeactivationAuthorizer: allowAuthorizer{},
	})
	if err != nil {
		t.Fatalf("activation.NewActivationService: %v", err)
	}
	similaritySvc, err := similarity.NewService(similarity.Config{
		Gates: gates, Store: store, Tx: mgr, Registry: registry,
		Evidence: evidence, Heads: activations,
	})
	if err != nil {
		t.Fatalf("similarity.NewService: %v", err)
	}
	graph := NewGraph()
	storage, err := NewGraphStorage(graph, store)
	if err != nil {
		t.Fatalf("NewGraphStorage: %v", err)
	}
	simSource := newMemRecordSource(SourceSimilarity)
	assessSource := newMemRecordSource(SourceEvidenceAssessment)
	svc, err := NewService(Config{
		Store: store, Tx: mgr, Registry: registry, Gates: gates,
		Graph: graph, Storage: storage,
		Similarity: simSource, Assessments: assessSource, Evidence: evidence,
	})
	if err != nil {
		t.Fatalf("projector.NewService: %v", err)
	}
	return &harness{
		t:             t,
		registry:      registry,
		gates:         gates,
		confDir:       dir,
		store:         store,
		mgr:           mgr,
		proposals:     proposals,
		artifacts:     artifacts,
		activations:   activations,
		similaritySvc: similaritySvc,
		evidence:      evidence,
		graph:         graph,
		storage:       storage,
		svc:           svc,
		simSource:     simSource,
		assessSource:  assessSource,
		candidates:    map[string]contract.CandidateArtifactRef{},
	}
}

// candidateOf returns the candidate binding the latest release of one
// lineage was minted from (reactivation input).
func (h *harness) candidateOf(t *testing.T, ref contract.SkillArtifactRef) contract.CandidateArtifactRef {
	t.Helper()
	cand, ok := h.candidates[ref.LineageID]
	if !ok {
		t.Fatalf("no recorded candidate for %s", ref.LineageID)
	}
	return cand
}

// activeRef returns the authoritative current active revision of a lineage.
func (h *harness) activeRef(t *testing.T, lineage string) contract.SkillArtifactRef {
	t.Helper()
	active, err := h.activations.ActiveRevision(context.Background(), lineage)
	if err != nil || active == nil {
		t.Fatalf("active revision of %s: %v", lineage, err)
	}
	return *active
}

func mustString(raw any) string {
	value, _ := contract.AsString(raw)
	return value
}

// numberString renders a decoder-model JSON number for assertions.
func numberString(t *testing.T, raw any) string {
	t.Helper()
	number, ok := raw.(json.Number)
	if !ok {
		t.Fatalf("expected a JSON number, got %T", raw)
	}
	return number.String()
}

// ---------------------------------------------------------------------------
// Ordinary release pipeline (the GMS-204 flow, as in the merge harness)
// ---------------------------------------------------------------------------

func jn(v int64) any { return json.Number(strconv.FormatInt(v, 10)) }

func vrefDoc(id, digest string) map[string]any {
	return map[string]any{"id": id, "version": jn(1), "digest": digest}
}

func evidenceDoc(ref contract.EvidenceRef) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaEvidenceRef,
		"evidence_id":     ref.EvidenceID,
		"version":         jn(1),
		"evidence_digest": ref.EvidenceDigest,
		"commit_state":    "committed",
		"evidence_kind":   ref.EvidenceKind,
	}
}

func (h *harness) committedEvidence(id, kind string) contract.EvidenceRef {
	h.t.Helper()
	ref := contract.EvidenceRef{
		SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: id, Version: "1",
		EvidenceDigest: contract.DigestBytes([]byte("evidence:" + id)),
		CommitState:    "committed", EvidenceKind: kind,
	}
	h.evidence.commit(ref)
	return ref
}

// guidanceEnvelope builds one schema-valid step_guidance envelope with the
// given branches (each branch carries one committed evidence ref).
func (h *harness) guidanceEnvelope(lineage string, branchIDs []string, predicateValue string) (map[string]any, []byte) {
	h.t.Helper()
	branches := make([]any, 0, len(branchIDs))
	for _, id := range branchIDs {
		ev := h.committedEvidence("ev-"+lineage+"-"+id, "success_path")
		branches = append(branches, map[string]any{
			"branch_id": id,
			"when":      map[string]any{"task": predicateValue, "branch": id},
			"action":    map[string]any{"guidance": "Follow the " + lineage + " " + id + " procedure", "failure_action": "fallback", "evidence_refs": []any{evidenceDoc(ev)}},
			"future":    map[string]any{"expected_outcome": "context recorded", "critical_steps": []any{}, "final_task_impact": "task completes with recorded context"},
		})
	}
	envelope := map[string]any{
		"schema_version": "gms.skill-artifact.v1",
		"kind":           "step_guidance",
		"title":          lineage + " guidance",
		"description":    "test guidance of " + lineage,
		"applicability": map[string]any{
			"predicates": []any{map[string]any{"task": predicateValue}},
			"exclusions": []any{},
		},
		"permissions": []any{map[string]any{"capability": "memory_explore", "scope": lineage + "-scope"}},
		"body": map[string]any{
			"causal_context": map[string]any{
				"summary":    lineage + " decision guidance",
				"claim_refs": []any{vrefDoc("claim-"+lineage+"-0001", contract.DigestBytes([]byte("claim-"+lineage+"-0001")))},
			},
			"branches": branches,
		},
	}
	canonical, err := h.artifacts.Canonicalize(envelope)
	if err != nil {
		h.t.Fatalf("canonicalize %s envelope: %v (code %s)", lineage, err, artifact.CodeOf(err))
	}
	return envelope, canonical.Canonical()
}

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

// decisionDoc builds a schema-valid §7.12 document for cand.
func (h *harness) decisionDoc(t *testing.T, cand contract.CandidateArtifactRef, outcome string) map[string]any {
	t.Helper()
	doc := map[string]any{
		"schema_version":   "gms.release-decision.v1",
		"decision_id":      "dec-" + cand.CandidateID,
		"decision_version": jn(1),
		"candidate_ref": map[string]any{
			"schema_version": cand.SchemaVersion,
			"candidate_id":   cand.CandidateID,
			"kind":           cand.Kind,
			"body_digest":    cand.BodyDigest,
			"origin_type":    cand.OriginType,
			"origin_ref":     vrefDoc(cand.OriginRef.ID, cand.OriginRef.Digest),
		},
		"validation_record_refs": []any{vrefDoc("valid-0001", contract.DigestBytes([]byte("valid-0001")))},
		"replay_result_refs":     []any{vrefDoc("replay-0001", contract.DigestBytes([]byte("replay-0001")))},
		"release_rule_ref":       vrefDoc("policy.release-rule.v1", contract.DigestBytes([]byte("release-rule"))),
		"utility_comparator_ref": vrefDoc("policy.utility-comparator.v1", contract.DigestBytes([]byte("comparator"))),
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

// releaseSkill drives one ordinary GMS-204 release of a step_guidance
// envelope into its lineage and returns the now-active §7.3 ref.
func (h *harness) releaseSkill(t *testing.T, lineage string, body []byte) contract.SkillArtifactRef {
	t.Helper()
	ctx := context.Background()
	h.releaseSeq++
	proposalID := fmt.Sprintf("prop-src-%s-%03d", lineage, h.releaseSeq)
	propDoc := h.proposalDoc(t, proposalID, "step_guidance", "create_lineage")
	h.seedActivationPending(t, propDoc)
	propDigest, _ := contract.AsString(propDoc["proposal_digest"])
	cand := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   fmt.Sprintf("cand-src-%s-%03d", lineage, h.releaseSeq),
		Kind:          "step_guidance",
		BodyDigest:    contract.DigestBytes(body),
		OriginType:    "skill_proposal",
		OriginRef:     contract.VersionedRef{ID: proposalID, Version: "1", Digest: propDigest},
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version": cand.SchemaVersion,
		"candidate_id":   cand.CandidateID,
		"kind":           cand.Kind,
		"body_digest":    cand.BodyDigest,
		"origin_type":    cand.OriginType,
		"origin_ref":     vrefDoc(cand.OriginRef.ID, cand.OriginRef.Digest),
	})
	if err != nil {
		t.Fatalf("candidate payload: %v", err)
	}
	err = h.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		if _, err := tx.PutContent(body); err != nil {
			return err
		}
		_, err := tx.AppendEvent(ledger.LedgerCandidate, "", cand.CandidateID, payload)
		return err
	})
	if err != nil {
		t.Fatalf("commit candidate %s: %v", cand.CandidateID, err)
	}
	h.candidates[lineage] = cand
	decision := stubDecision{doc: h.decisionDoc(t, cand, "accepted")}
	res, err := h.activations.Activate(ctx, activation.ActivateRequest{
		Decision: decision, Candidate: &stubCandidateView{ref: cand, body: body},
		LineageID: lineage, ProposalDoc: propDoc,
	})
	if err != nil {
		t.Fatalf("activate source %s: %v (code %s)", lineage, err, activation.CodeOf(err))
	}
	return res.ReleasedRef
}

// deliveredEvents reads the committed activation ledger as one delivered
// batch (the delivery-worker view).
func (h *harness) deliveredEvents(t *testing.T, from, to uint64) []DeliveredEvent {
	t.Helper()
	entries, err := h.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		t.Fatalf("activation snapshot: %v", err)
	}
	var out []DeliveredEvent
	for _, entry := range entries {
		if entry.Sequence < from || entry.Sequence > to {
			continue
		}
		payload, ok, err := h.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			t.Fatalf("event %d payload unresolved", entry.Sequence)
		}
		out = append(out, DeliveredEvent{
			Sequence: entry.Sequence, EventID: entry.EventID,
			PayloadDigest: entry.PayloadDigest, Payload: payload,
		})
	}
	return out
}

// feedSimilarityAssessment pulls one committed similarity assessment from
// its per-assessment ledger stream and feeds it to the relation source.
func (h *harness) feedSimilarityAssessment(t *testing.T, assessmentID string) {
	t.Helper()
	entries, err := h.store.Snapshot(ledger.LedgerSimilarity, assessmentID)
	if err != nil || len(entries) == 0 {
		t.Fatalf("similarity stream %s: %d entries (err %v)", assessmentID, len(entries), err)
	}
	entry := entries[len(entries)-1]
	payload, ok, err := h.store.Get(entry.PayloadDigest)
	if err != nil || !ok {
		t.Fatalf("similarity payload %s unresolved", entry.PayloadDigest)
	}
	h.simSource.add(SourceRecord{
		Sequence: entry.Sequence, RecordID: entry.EventID,
		Digest: entry.PayloadDigest, Canonical: payload,
	})
}

// claimAssessment builds one GMS-private claim-assessment record (canonical
// bytes + digest) targeting a revision-scoped branch.
func claimAssessment(id string, revision contract.SkillArtifactRef, branchID, kind string, evidence contract.EvidenceRef) (SourceRecord, []byte) {
	doc := map[string]any{
		"schema_version":     schemaClaimAssessment,
		"assessment_id":      id,
		"assessment_version": jn(1),
		"branch_ref": map[string]any{
			"source_skill_ref": map[string]any{
				"schema_version":  revision.SchemaVersion,
				"lineage_id":      revision.LineageID,
				"version":         jn(mustInt(revision.Version)),
				"kind":            revision.Kind,
				"artifact_digest": revision.ArtifactDigest,
			},
			"branch_id": branchID,
		},
		"assessment_kind": kind,
		"evidence_ref":    evidenceDoc(evidence),
		"assessor_ref":    vrefDoc("gms.claim-assessor.v1", contract.DigestBytes([]byte("claim-assessor"))),
	}
	digest, err := contract.DigestOf(doc)
	if err != nil {
		panic(err)
	}
	doc["assessment_digest"] = digest
	canonical, err := contract.JCS(contract.NormalizeForHashing(doc))
	if err != nil {
		panic(err)
	}
	return SourceRecord{RecordID: id, Digest: digest, Canonical: canonical}, canonical
}

func mustInt(v string) int64 {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		panic(err)
	}
	return n
}

// fixtureValue loads one conformance fixture file.
func fixtureValue(t *testing.T, parts ...string) map[string]any {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(append([]string{dir}, parts...)...))
	if err != nil {
		t.Fatalf("read fixture %v: %v", parts, err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		t.Fatalf("parse fixture %v: %v", parts, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		t.Fatalf("fixture %v is not an object", parts)
	}
	return obj
}

// fixtureString reads one string field out of a fixture object.
func fixtureString(t *testing.T, obj map[string]any, field string) string {
	t.Helper()
	value, _ := contract.AsString(obj[field])
	return value
}

// appendMergeOriginActivation commits one hand-built merge-origin activate
// event (TWO derived_from refs, no supersedes) to the activation ledger —
// the event shape the GMS-208 merge transaction writes for M@1 — so the
// projector's derived_from rules run against a ledger-committed record.
func (h *harness) appendMergeOriginActivation(t *testing.T, merged contract.SkillArtifactRef, sources [2]contract.SkillArtifactRef, body []byte) DeliveredEvent {
	t.Helper()
	ctx := context.Background()
	var event DeliveredEvent
	err := h.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		if _, err := tx.PutContent(body); err != nil {
			return err
		}
		sequence, err := tx.NextSequence(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}
		eventID := fmt.Sprintf("evt-merge-%03d", sequence)
		outboxKey := contract.DigestBytes([]byte("outbox:" + eventID))
		doc := map[string]any{
			"schema_version":      ledger.SchemaActivationEvent,
			"activation_sequence": jn(int64(sequence)),
			"event_id":            eventID,
			"event_type":          "activate",
			"lineage_id":          merged.LineageID,
			"skill_ref": map[string]any{
				"schema_version":  contract.SchemaSkillArtifactRef,
				"lineage_id":      merged.LineageID,
				"version":         jn(mustInt(merged.Version)),
				"kind":            merged.Kind,
				"artifact_digest": merged.ArtifactDigest,
			},
			"release_decision_ref": vrefDoc("dec-merge-0001", contract.DigestBytes([]byte("dec-merge-0001"))),
			"candidate_ref": map[string]any{
				"schema_version": contract.SchemaCandidateArtifactRef,
				"candidate_id":   "cand-merge-0001",
				"kind":           merged.Kind,
				"body_digest":    merged.ArtifactDigest,
				"origin_type":    "merge_proposal",
				"origin_ref":     vrefDoc("mp-0001", contract.DigestBytes([]byte("mp-0001"))),
			},
			"body_digest_equal": true,
			"derived_from_refs": []any{
				map[string]any{
					"schema_version":  contract.SchemaSkillArtifactRef,
					"lineage_id":      sources[0].LineageID,
					"version":         jn(mustInt(sources[0].Version)),
					"kind":            sources[0].Kind,
					"artifact_digest": sources[0].ArtifactDigest,
				},
				map[string]any{
					"schema_version":  contract.SchemaSkillArtifactRef,
					"lineage_id":      sources[1].LineageID,
					"version":         jn(mustInt(sources[1].Version)),
					"kind":            sources[1].Kind,
					"artifact_digest": sources[1].ArtifactDigest,
				},
			},
			"outbox_key": outboxKey,
		}
		digest, err := contract.DigestOf(doc)
		if err != nil {
			return err
		}
		doc["event_digest"] = digest
		entry, err := tx.AppendEvent(ledger.LedgerActivation, "", eventID, mustJCS(t, doc))
		if err != nil {
			return err
		}
		event = DeliveredEvent{
			Sequence: entry.Sequence, EventID: entry.EventID,
			PayloadDigest: entry.PayloadDigest, Payload: mustJCS(t, doc),
		}
		return nil
	})
	if err != nil {
		t.Fatalf("append merge-origin activation: %v", err)
	}
	return event
}

func mustJCS(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	canonical, err := contract.JCS(contract.NormalizeForHashing(doc))
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	return canonical
}

// edgeCount counts the edges of one relation in a snapshot.
func edgeCount(snap GraphSnapshot, relation string) int {
	count := 0
	for _, edge := range snap.Edges {
		if edge.Relation == relation {
			count++
		}
	}
	return count
}
