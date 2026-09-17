package candidate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Shared test wiring
// ---------------------------------------------------------------------------

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

func newBindingHarness(t *testing.T) (*BindingService, *ledger.MemoryStore, *stubEvidence) {
	t.Helper()
	registry := loadRegistry(t)
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	mgr, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	gates := loadGates(t)
	dir, _ := contract.DefaultConformanceDir()
	artifacts, err := artifact.NewService(gates)
	if err != nil {
		t.Fatalf("artifact.NewService: %v", err)
	}
	proposals, err := proposal.NewService(gates, filepath.Join(dir, "schema", "state"), store, mgr, registry)
	if err != nil {
		t.Fatalf("proposal.NewService: %v", err)
	}
	evidence := &stubEvidence{committed: map[string]contract.EvidenceRef{}}
	svc, err := NewBindingService(Config{
		Store:     store,
		Tx:        mgr,
		Registry:  registry,
		Artifacts: artifacts,
		Proposals: proposals,
		Evidence:  evidence,
	})
	if err != nil {
		t.Fatalf("NewBindingService: %v", err)
	}
	return svc, store, evidence
}

// stubEvidence is the closed-world evidence resolver used by the tests: an
// evidence id resolves only when it was committed into the stub.
type stubEvidence struct {
	committed map[string]contract.EvidenceRef
}

func (s *stubEvidence) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	ref, ok := s.committed[id]
	return ref, ok, nil
}

func (s *stubEvidence) add(id string) {
	s.committed[id] = contract.EvidenceRef{
		SchemaVersion:  contract.SchemaEvidenceRef,
		EvidenceID:     id,
		Version:        "1",
		EvidenceDigest: "sha256:" + repeatHex(id),
		CommitState:    "committed",
		EvidenceKind:   "success_path",
	}
}

func repeatHex(seed string) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = "0123456789abcdef"[(int(seed[i%len(seed)])+i)%16]
	}
	return string(out)
}

// fixtureJSON loads one $FIX case source.json as a decoder-model value.
func fixtureJSON(t *testing.T, category, caseName string) any {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, category, caseName, "source.json"))
	if err != nil {
		t.Fatalf("read fixture %s/%s: %v", category, caseName, err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		t.Fatalf("parse fixture %s/%s: %v", category, caseName, err)
	}
	return value
}

// validStepGuidanceEnvelope is a minimal, gate-clean step_guidance artifact
// built on the artifact-001 fixture shape.
func validStepGuidanceEnvelope() map[string]any {
	ev := func(id string) map[string]any {
		return map[string]any{
			"schema_version":  contract.SchemaEvidenceRef,
			"evidence_id":     id,
			"version":         json.Number("1"),
			"evidence_digest": "sha256:" + repeatHex(id),
			"commit_state":    "committed",
			"evidence_kind":   "success_path",
		}
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "step_guidance",
		"title":          "Commit only after hooks pass",
		"description":    "Guidance fixture",
		"applicability": map[string]any{
			"predicates": []any{},
			"exclusions": []any{},
		},
		"permissions": []any{
			map[string]any{"capability": "memory_expand", "scope": "room-shared-space"},
		},
		"body": map[string]any{
			"causal_context": map[string]any{
				"summary":    "Tests settled before commit",
				"claim_refs": []any{},
			},
			"branches": []any{
				map[string]any{
					"branch_id": "b-success",
					"when":      map[string]any{"field": "repo.tests", "op": "equals", "value": "passing"},
					"action":    map[string]any{"guidance": "Stage and commit", "failure_action": "stop", "evidence_refs": []any{ev("ev-0451")}},
					"future":    map[string]any{"expected_outcome": "clean tree", "critical_steps": []any{}, "final_task_impact": "Commit lands only on green"},
				},
			},
		},
	}
}

// validProposalDoc builds a gms.skill-proposal.v1 document referencing the
// given evidence id with a recomputed proposal_digest.
func validProposalDoc(t *testing.T, gates *validation.Gates, evidenceID, kind string) map[string]any {
	t.Helper()
	doc := map[string]any{
		"schema_version":   "gms.skill-proposal.v1",
		"proposal_id":      "prop-2001",
		"proposal_version": json.Number("1"),
		"proposed_kind":    kind,
		"source_segment_refs": []any{
			map[string]any{
				"schema_version":    "host.segment-ref.v1",
				"room_id":           "room-1",
				"segment_id":        "seg-1",
				"segment_version":   json.Number("1"),
				"segment_digest":    "sha256:" + repeatHex("seg-1"),
				"evidence_seal_ref": versionedRef("evseal-seg-1"),
				"path_seal_ref":     versionedRef("pathseal-seg-1"),
			},
		},
		"evidence_refs": []any{
			map[string]any{
				"schema_version":  contract.SchemaEvidenceRef,
				"evidence_id":     evidenceID,
				"version":         json.Number("1"),
				"evidence_digest": "sha256:" + repeatHex(evidenceID),
				"commit_state":    "committed",
				"evidence_kind":   "success_path",
			},
		},
		"requested_operation": "create_lineage",
		"origin": map[string]any{
			"initiator_type": "model",
			"initiator_ref":  "agent-1",
			"request_ref":    "req-1",
		},
		"policy_refs": []any{versionedRef("policy-static-gates")},
	}
	digest, err := gates.ComputeDigestPreimage(doc, "skill-proposal.schema.json")
	if err != nil {
		t.Fatalf("proposal digest preimage: %v", err)
	}
	doc["proposal_digest"] = digest
	return doc
}

func versionedRef(id string) map[string]any {
	return map[string]any{
		"id":      id,
		"version": json.Number("1"),
		"digest":  "sha256:" + repeatHex(id),
	}
}

// ---------------------------------------------------------------------------
// GMS-202 TDD Red #1
// ---------------------------------------------------------------------------

// TestCandidateBindingRequiresCommittedEvidenceAndAllStaticGates pins the
// one-shot candidate binding transaction (GMS §4.3, Contract §9.1
// candidate_bound): a CandidateArtifactRef is minted ONLY when every evidence
// ref resolves committed and every static gate is green; the write set
// (canonical bytes, CandidateRef, proposal candidate_bound event, idempotency
// record) commits atomically or not at all. Model suggestions cannot bind
// directly, candidates are immutable and never executable.
func TestCandidateBindingRequiresCommittedEvidenceAndAllStaticGates(t *testing.T) {
	svc, store, evidence := newBindingHarness(t)
	gates := loadGates(t)
	ctx := context.Background()

	// Genesis + admitted proposal events first (state machine: none ->
	// proposed -> admitted).
	proposalDoc := validProposalDoc(t, gates, "ev-0451", "step_guidance")
	if _, err := svc.Proposals().AppendTransition(ctx, proposalDoc, "none", "proposed"); err != nil {
		t.Fatalf("seed proposed state: %v", err)
	}
	if _, err := svc.Proposals().AppendTransition(ctx, proposalDoc, "proposed", "admitted"); err != nil {
		t.Fatalf("seed admitted state: %v", err)
	}

	envelope := validStepGuidanceEnvelope()

	// 1. Uncommitted evidence -> no candidate, no ledger writes.
	_, err := svc.Bind(ctx, BindRequest{ProposalDoc: proposalDoc, ArtifactDoc: envelope})
	if CodeOf(err) != "EVIDENCE_NOT_COMMITTED" {
		t.Fatalf("uncommitted evidence: want EVIDENCE_NOT_COMMITTED, got %v", err)
	}
	assertNoCandidateLedgerWrites(t, store)

	// 2. Evidence committed but a static gate fails (permission beyond the
	// Host cap) -> still no writes.
	evidence.add("ev-0451")
	badPerms := deepCopyMap(envelope)
	badPerms["permissions"] = []any{map[string]any{"capability": "filesystem_write", "scope": "/"}}
	_, err = svc.Bind(ctx, BindRequest{ProposalDoc: proposalDoc, ArtifactDoc: badPerms})
	if CodeOf(err) != "PERMISSION_CAP_EXCEEDED" {
		t.Fatalf("permission overflow: want PERMISSION_CAP_EXCEEDED, got %v", err)
	}
	assertNoCandidateLedgerWrites(t, store)

	// 3. Kind drift (proposal kind != artifact kind) -> rejected. The
	// artifact is a gate-clean step_guidance envelope; the proposal asks
	// for a procedure: binding-level drift, not an artifact-shape error.
	driftProposal := validProposalDoc(t, gates, "ev-0451", "human_procedure")
	_, err = svc.Bind(ctx, BindRequest{ProposalDoc: driftProposal, ArtifactDoc: envelope})
	if CodeOf(err) != "SKILL_KIND_INVALID" {
		t.Fatalf("kind drift: want SKILL_KIND_INVALID, got %v", err)
	}
	assertNoCandidateLedgerWrites(t, store)

	// 4. A raw model suggestion (not a SkillProposal) cannot bind directly.
	suggestion := map[string]any{
		"schema_version": "gms.model-suggestion.v1",
		"proposal_id":    "prop-2001",
		"model_output":   envelope,
	}
	_, err = svc.Bind(ctx, BindRequest{ProposalDoc: suggestion, ArtifactDoc: envelope})
	if CodeOf(err) != "PROPOSAL_INVALID" {
		t.Fatalf("model suggestion direct bind: want PROPOSAL_INVALID, got %v", err)
	}
	assertNoCandidateLedgerWrites(t, store)

	// 5. All green -> candidate + event + idempotency committed atomically.
	view, err := svc.Bind(ctx, BindRequest{ProposalDoc: proposalDoc, ArtifactDoc: envelope})
	if err != nil {
		t.Fatalf("bind all-green: %v", err)
	}
	ref := view.Ref()
	if ref.SchemaVersion != contract.SchemaCandidateArtifactRef {
		t.Fatalf("candidate ref schema: %s", ref.SchemaVersion)
	}
	if ref.OriginType != "skill_proposal" || ref.OriginRef.ID != "prop-2001" {
		t.Fatalf("candidate origin: %+v", ref.OriginRef)
	}
	if ref.BodyDigest == "" || ref.CandidateID == "" {
		t.Fatalf("candidate identity incomplete: %+v", ref)
	}
	body := view.CanonicalBody()
	if len(body) == 0 {
		t.Fatal("canonical body missing")
	}
	if got := contract.DigestBytes(body); got != ref.BodyDigest {
		t.Fatalf("body digest %s != ref %s", got, ref.BodyDigest)
	}
	assertCandidateLedgerHas(t, store, ref)
	assertProposalState(t, svc, "prop-2001", "candidate_bound")

	// 6. Replay of the same request returns the identical immutable view.
	again, err := svc.Bind(ctx, BindRequest{ProposalDoc: proposalDoc, ArtifactDoc: envelope})
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if again.Ref().CandidateID != ref.CandidateID || again.Ref().BodyDigest != ref.BodyDigest {
		t.Fatalf("replay minted a different candidate: %+v vs %+v", again.Ref(), ref)
	}

	// 7. Same candidate identity with a mutated body -> CANDIDATE_IMMUTABLE.
	mutated := deepCopyMap(envelope)
	mutated["title"] = "Mutated after binding"
	_, err = svc.Bind(ctx, BindRequest{ProposalDoc: proposalDoc, ArtifactDoc: mutated, CandidateID: ref.CandidateID})
	if CodeOf(err) != "CANDIDATE_IMMUTABLE" {
		t.Fatalf("candidate mutation: want CANDIDATE_IMMUTABLE, got %v", err)
	}

	// 8. Candidates are never Runtime-executable input.
	if err := view.RuntimeInput(); CodeOf(err) != "CANDIDATE_NOT_EXECUTABLE" {
		t.Fatalf("runtime input: want CANDIDATE_NOT_EXECUTABLE, got %v", err)
	}

	// 9. Body view copies cannot leak into the frozen canonical bytes.
	leaked := view.CanonicalBody()
	leaked[0] = '#'
	if view.CanonicalBody()[0] == '#' {
		t.Fatal("CanonicalBody leaked a mutable slice")
	}
}

func assertNoCandidateLedgerWrites(t *testing.T, store *ledger.MemoryStore) {
	t.Helper()
	entries, err := store.Snapshot(ledger.LedgerCandidate, "")
	if err != nil {
		t.Fatalf("candidate snapshot: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected bind left %d candidate ledger entries", len(entries))
	}
}

func assertCandidateLedgerHas(t *testing.T, store *ledger.MemoryStore, want contract.CandidateArtifactRef) {
	t.Helper()
	entries, err := store.Snapshot(ledger.LedgerCandidate, "")
	if err != nil {
		t.Fatalf("candidate snapshot: %v", err)
	}
	for _, entry := range entries {
		if entry.EventID == want.CandidateID {
			payload, ok, err := store.Get(entry.PayloadDigest)
			if err != nil || !ok {
				t.Fatalf("candidate payload unresolved: %v", err)
			}
			value, _ := contract.ParseJSONStrict(payload)
			obj, _ := contract.AsObject(value)
			parsed, err := contract.ParseCandidateArtifactRef(obj)
			if err != nil {
				t.Fatalf("candidate payload invalid: %v", err)
			}
			if parsed.BodyDigest != want.BodyDigest {
				t.Fatalf("candidate body digest %s != %s", parsed.BodyDigest, want.BodyDigest)
			}
			return
		}
	}
	t.Fatalf("candidate %s not found in candidate ledger", want.CandidateID)
}

func assertProposalState(t *testing.T, svc *BindingService, proposalID, want string) {
	t.Helper()
	state, _, err := svc.Proposals().CurrentState(context.Background(), proposalID)
	if err != nil {
		t.Fatalf("current state: %v", err)
	}
	if state != want {
		t.Fatalf("proposal %s state %s, want %s", proposalID, state, want)
	}
}

func deepCopyMap(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopyMap(t)
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}

func TestMergeProvenanceAndConflictClosureGates(t *testing.T) {
	svc, _, _ := newBindingHarness(t)

	// Fixture-driven closure: merge-neg-001 (below band) and merge-neg-002
	// (blocking unresolved) fail closed; merge-003 passes.
	below := fixtureJSON(t, "merge", "merge-neg-001-below-threshold")
	if err := svc.CheckSimilarityAssessment(below); CodeOf(err) != "SIMILARITY_BELOW_THRESHOLD" {
		t.Fatalf("below threshold: want SIMILARITY_BELOW_THRESHOLD, got %v", err)
	}
	blocking := fixtureJSON(t, "merge", "merge-neg-002-blocking-conflict")
	if err := svc.CheckMergeClosure(blocking); CodeOf(err) != "MERGE_BLOCKING_CONFLICT" {
		t.Fatalf("blocking conflict: want MERGE_BLOCKING_CONFLICT, got %v", err)
	}
	accepted := fixtureJSON(t, "merge", "merge-003-merge-proposal")
	if err := svc.CheckMergeClosure(accepted); err != nil {
		t.Fatalf("resolved merge proposal rejected: %v", err)
	}
	if err := svc.CheckSimilarityAssessment(fixtureJSON(t, "merge", "merge-001-similarity-ab")); err != nil {
		t.Fatalf("similarity-ab rejected: %v", err)
	}
	if err := svc.CheckSimilarityAssessment(fixtureJSON(t, "merge", "merge-002-similarity-ba")); err != nil {
		t.Fatalf("similarity-ba rejected: %v", err)
	}

	// Provenance coverage: dual source refs + assessment ref required.
	err := svc.CheckMergeProvenance(MergeProvenance{})
	if CodeOf(err) != "MERGE_EVIDENCE_INCOMPLETE" {
		t.Fatalf("empty provenance: want MERGE_EVIDENCE_INCOMPLETE, got %v", err)
	}
}
