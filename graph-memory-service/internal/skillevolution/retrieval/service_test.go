// Package retrieval_test: GMS-206 Red/Green tests.
//
// TestExploreDeterministicallyEnforcesScopeBudgetFenceAndFreshness and
// TestSkillGetUsesFrozenToolSpecificBinding are the two plan-named Red tests:
// they drive the retrieval service end-to-end over the REAL GMS-102/202/204/205
// authorities (ledger, artifact gate, activation service, Runtime projector)
// and pin the deterministic lexical+graph integer ranking, the budget/fence/
// omission/citation/watermark self-consistency, and the frozen tool-specific
// skill_get binding (closed GuidanceView, readiness gate).
package retrieval_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/retrieval"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Real-authority world: GMS-102 ledger + GMS-202 gate + GMS-204 activation +
// GMS-205 Runtime projector, with a claim-assessment relation source and a
// committed-evidence resolver (the projector's ports).
// ---------------------------------------------------------------------------

type world struct {
	t           *testing.T
	registry    *ledger.ContractReasonRegistry
	gates       *validation.Gates
	confDir     string
	store       *ledger.MemoryStore
	proposals   *proposal.Service
	activate    *activation.ActivationService
	artifacts   *artifact.Service
	projector   *projector.Service
	evidence    *mapEvidence
	assessments *sliceRecords
}

type mapEvidence struct {
	refs map[string]contract.EvidenceRef
}

func (m *mapEvidence) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	ref, ok := m.refs[id]
	return ref, ok, nil
}

// sliceRecords is a contiguous in-order RecordSource (claim assessments).
type sliceRecords struct {
	name    string
	records []projector.SourceRecord
}

func (s *sliceRecords) SourceName() string { return s.name }

func (s *sliceRecords) Records(after uint64) ([]projector.SourceRecord, error) {
	out := make([]projector.SourceRecord, 0, len(s.records))
	for _, record := range s.records {
		if record.Sequence > after {
			out = append(out, record)
		}
	}
	return out, nil
}

func newWorld(t *testing.T) *world {
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
		t.Fatalf("NewGates: %v", err)
	}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	mgr, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	proposals, err := proposal.NewService(gates, filepath.Join(dir, "schema", "state"), store, mgr, registry)
	if err != nil {
		t.Fatalf("proposal.NewService: %v", err)
	}
	actSvc, err := activation.NewActivationService(activation.Config{
		Store: store, Tx: mgr, Registry: registry, Gates: gates, Proposals: proposals,
	})
	if err != nil {
		t.Fatalf("NewActivationService: %v", err)
	}
	artSvc, err := artifact.NewService(gates)
	if err != nil {
		t.Fatalf("artifact.NewService: %v", err)
	}
	evidence := &mapEvidence{refs: map[string]contract.EvidenceRef{}}
	assessments := &sliceRecords{name: projector.SourceEvidenceAssessment}
	proj, err := projector.NewService(projector.Config{
		Store: store, Tx: mgr, Registry: registry, Gates: gates,
		Assessments: assessments, Evidence: evidence,
	})
	if err != nil {
		t.Fatalf("projector.NewService: %v", err)
	}
	return &world{
		t: t, registry: registry, gates: gates, confDir: dir,
		store: store, proposals: proposals, activate: actSvc, artifacts: artSvc,
		projector: proj, evidence: evidence, assessments: assessments,
	}
}

// service builds the retrieval service; skillGet toggles the readiness gate.
func (w *world) service(t *testing.T, skillGet bool) *retrieval.Service {
	t.Helper()
	policyPath := filepath.Join(w.confDir, "policy", "tool-success-validation.v1.json")
	toolPolicy, err := retrieval.LoadToolPolicy(policyPath)
	if err != nil {
		t.Fatalf("retrieval.LoadToolPolicy: %v", err)
	}
	svc, err := retrieval.NewService(retrieval.Config{
		Projection:      w.projector,
		Heads:           w.activate,
		Artifacts:       w.store,
		Gate:            w.artifacts,
		Evidence:        w.evidence,
		Gates:           w.gates,
		Registry:        w.registry,
		Policy:          toolPolicy,
		SkillGetEnabled: skillGet,
	})
	if err != nil {
		t.Fatalf("retrieval.NewService: %v", err)
	}
	return svc
}

// ---------------------------------------------------------------------------
// Envelope builders (integer-only JSON numbers, Contract §6.3)
// ---------------------------------------------------------------------------

func jn(v int64) any { return json.Number(strconv.FormatInt(v, 10)) }

func evidenceDoc(id string) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaEvidenceRef,
		"evidence_id":     id,
		"version":         jn(1),
		"evidence_digest": contract.DigestBytes([]byte("evidence:" + id)),
		"commit_state":    "committed",
		"evidence_kind":   "observation",
	}
}

func (w *world) registerEvidence(id string) {
	w.t.Helper()
	ref, err := contract.ParseEvidenceRef(evidenceDoc(id))
	if err != nil {
		w.t.Fatalf("register evidence %s: %v", id, err)
	}
	w.evidence.refs[id] = ref
}

func portRef(id string) map[string]any {
	return map[string]any{
		"schema_id": "https://example.com/schemas/" + id,
		"version":   jn(1),
		"digest":    contract.DigestBytes([]byte("port:" + id)),
	}
}

func procedureEnvelope(title string) map[string]any {
	return map[string]any{
		"schema_version": "gms.skill-artifact.v1",
		"kind":           "human_procedure",
		"title":          title,
		"description":    "procedure body under test",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{map[string]any{"capability": "memory_explore", "scope": "room-shared-space"}},
		"body": map[string]any{
			"steps": []any{map[string]any{
				"step_id":        "s1",
				"instruction":    "summarize the diff",
				"preconditions":  []any{},
				"postconditions": []any{},
				"failure_action": "stop",
			}},
			"input_port_schema":  portRef(title + "-in"),
			"output_port_schema": portRef(title + "-out"),
		},
	}
}

func guidanceEnvelope(title string) map[string]any {
	branch := func(id, guidance, outcome, evidence string) map[string]any {
		return map[string]any{"branch_id": id, "when": map[string]any{}, "action": map[string]any{"guidance": guidance, "failure_action": "stop", "evidence_refs": []any{evidenceDoc(evidence)}}, "future": map[string]any{"expected_outcome": outcome, "critical_steps": []any{}, "final_task_impact": "commit verified"}}
	}
	return map[string]any{"schema_version": "gms.skill-artifact.v1", "kind": "step_guidance", "title": title, "description": "step guidance body under test", "applicability": map[string]any{"predicates": []any{}, "exclusions": []any{}}, "permissions": []any{}, "body": map[string]any{"causal_context": map[string]any{"summary": "prior context", "claim_refs": []any{}}, "branches": []any{branch("b1", "run the checklist", "checked", "ev-"+title), branch("b2", "escalate on failure", "escalated", "ev-"+title+"-b2")}}}
}

func skillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	version := ref.Version
	if _, err := strconv.ParseInt(version, 10, 64); err == nil {
		return map[string]any{
			"schema_version":  ref.SchemaVersion,
			"lineage_id":      ref.LineageID,
			"version":         json.Number(version),
			"kind":            ref.Kind,
			"artifact_digest": ref.ArtifactDigest,
		}
	}
	return map[string]any{
		"schema_version": ref.SchemaVersion, "lineage_id": ref.LineageID,
		"version": version, "kind": ref.Kind, "artifact_digest": ref.ArtifactDigest,
	}
}

// ---------------------------------------------------------------------------
// Activation plumbing (proposal -> candidate -> accepted decision -> Activate)
// ---------------------------------------------------------------------------

var activationPath = [][2]string{
	{"none", "proposed"},
	{"proposed", "admitted"},
	{"admitted", "candidate_bound"},
	{"candidate_bound", "validating"},
	{"validating", "replay_pending"},
	{"replay_pending", "replaying"},
	{"replaying", "decision_pending"},
	{"decision_pending", "activation_pending"},
}

type candidateView struct {
	ref  contract.CandidateArtifactRef
	body []byte
}

func (v *candidateView) Ref() contract.CandidateArtifactRef { return v.ref }
func (v *candidateView) BodyDigest() string                 { return v.ref.BodyDigest }
func (v *candidateView) CanonicalBody() []byte {
	out := make([]byte, len(v.body))
	copy(out, v.body)
	return out
}

type decision struct{ doc map[string]any }

func (d decision) Doc() map[string]any { return d.doc }
func (d decision) Outcome() string     { s, _ := contract.AsString(d.doc["outcome"]); return s }
func (d decision) DecisionDigest() string {
	s, _ := contract.AsString(d.doc["decision_digest"])
	return s
}

func (w *world) activateEnvelope(t *testing.T, lineageID, proposalID string, envelope map[string]any) contract.SkillArtifactRef {
	t.Helper()
	ctx := context.Background()
	canonical, err := w.artifacts.Canonicalize(envelope)
	if err != nil {
		t.Fatalf("canonicalize %s envelope: %v", envelope["kind"], err)
	}
	body, digest := canonical.Canonical(), canonical.BodyDigest()

	doc := map[string]any{
		"schema_version":   "gms.skill-proposal.v1",
		"proposal_id":      proposalID,
		"proposal_version": jn(1),
		"proposed_kind":    envelope["kind"],
		"source_segment_refs": []any{map[string]any{
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
		}},
		"evidence_refs":       []any{evidenceDoc("ev-" + proposalID)},
		"requested_operation": "create_lineage",
		"origin": map[string]any{
			"initiator_type": "model", "initiator_ref": "agent-1", "request_ref": "req-" + proposalID,
		},
		"policy_refs": []any{map[string]any{
			"id": "policy-static-gates", "version": jn(1), "digest": contract.DigestBytes([]byte("policy-static-gates")),
		}},
	}
	proposalDigest, err := w.gates.ComputeDigestPreimage(doc, "skill-proposal.schema.json")
	if err != nil {
		t.Fatalf("proposal digest preimage: %v", err)
	}
	doc["proposal_digest"] = proposalDigest
	for _, edge := range activationPath {
		if _, err := w.proposals.AppendTransition(ctx, doc, edge[0], edge[1]); err != nil {
			t.Fatalf("seed %s -> %s: %v", edge[0], edge[1], err)
		}
	}
	cand := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   "cand-" + proposalID,
		Kind:          envelope["kind"].(string),
		BodyDigest:    digest,
		OriginType:    "skill_proposal",
		OriginRef:     contract.VersionedRef{ID: proposalID, Version: "1", Digest: proposalDigest},
	}
	w.commitCandidate(t, cand, body)
	active, err := w.activate.ActiveRevision(ctx, lineageID)
	if err != nil {
		t.Fatalf("active head before activation: %v", err)
	}
	result, err := w.activate.Activate(ctx, activation.ActivateRequest{
		Decision:     w.decisionDoc(t, cand),
		Candidate:    &candidateView{ref: cand, body: body},
		LineageID:    lineageID,
		ExpectedHead: active,
		ProposalDoc:  doc,
	})
	if err != nil {
		t.Fatalf("activate %s: %v", lineageID, err)
	}
	return result.ReleasedRef
}

func (w *world) decisionDoc(t *testing.T, cand contract.CandidateArtifactRef) decision {
	t.Helper()
	vref := func(id string) map[string]any {
		return map[string]any{"id": id, "version": jn(1), "digest": contract.DigestBytes([]byte(id))}
	}
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
			"origin_ref":     map[string]any{"id": cand.OriginRef.ID, "version": jn(1), "digest": cand.OriginRef.Digest},
		},
		"validation_record_refs": []any{vref("valid-0001")},
		"replay_result_refs":     []any{vref("replay-0001")},
		"release_rule_ref":       vref("policy.release-rule.v1"),
		"utility_comparator_ref": vref("policy.utility-comparator.v1"),
		"hard_gate_results": []any{
			map[string]any{"gate_code": "schema_canonicalization_digest", "passed": true},
			map[string]any{"gate_code": "exact_ref_resolution", "passed": true},
		},
		"outcome":      "accepted",
		"reason_codes": []any{},
	}
	digest, err := w.gates.ComputeDigestPreimage(doc, "release-decision.schema.json")
	if err != nil {
		t.Fatalf("decision digest preimage: %v", err)
	}
	doc["decision_digest"] = digest
	if err := w.gates.ValidateInstance(doc, "release-decision.schema.json"); err != nil {
		t.Fatalf("decision doc invalid: %v", err)
	}
	return decision{doc: doc}
}

func (w *world) commitCandidate(t *testing.T, cand contract.CandidateArtifactRef, body []byte) {
	t.Helper()
	mgr, err := ledger.NewManager(w.store, w.registry)
	if err != nil {
		t.Fatalf("commit candidate manager: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version": cand.SchemaVersion,
		"candidate_id":   cand.CandidateID,
		"kind":           cand.Kind,
		"body_digest":    cand.BodyDigest,
		"origin_type":    cand.OriginType,
		"origin_ref":     map[string]any{"id": cand.OriginRef.ID, "version": jn(1), "digest": cand.OriginRef.Digest},
	})
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

// addAssessment feeds one claim-assessment relation record (supported_by /
// refuted_by) linking a projected branch to committed evidence.
func (w *world) addAssessment(id, kind string, revision contract.SkillArtifactRef, branchID, evidenceID string) {
	w.t.Helper()
	record := map[string]any{
		"schema_version":    "gms.claim-assessment.v1",
		"assessment_id":     id,
		"assessment_digest": contract.DigestBytes([]byte("assessment:" + id)),
		"branch_ref": map[string]any{
			"schema_version":   "gms.exact-branch-ref.v1",
			"source_skill_ref": skillRefDoc(revision),
			"branch_id":        branchID,
			"branch_digest":    contract.DigestBytes([]byte("branch:" + branchID)),
		},
		"assessment_kind": kind,
		"evidence_ref":    evidenceDoc(evidenceID),
		"assessor_ref": map[string]any{
			"id": "gms.assessor.claims.v1", "version": jn(1),
			"digest": contract.DigestBytes([]byte("gms.assessor.claims.v1")),
		},
	}
	canonical, err := contract.JCS(record)
	if err != nil {
		w.t.Fatalf("assessment %s canonicalize: %v", id, err)
	}
	w.assessments.records = append(w.assessments.records, projector.SourceRecord{
		Sequence:  uint64(len(w.assessments.records) + 1),
		RecordID:  id,
		Digest:    contract.DigestBytes(canonical),
		Canonical: canonical,
	})
}

func (w *world) project(t *testing.T) {
	t.Helper()
	if _, err := w.projector.ProjectRuntime(context.Background()); err != nil {
		t.Fatalf("ProjectRuntime: %v", err)
	}
}

func (w *world) watermarkSequence() uint64 {
	doc, _, _ := w.projector.Watermark()
	if doc == nil {
		return 0
	}
	number, ok := doc["projected_through_activation_sequence"].(json.Number)
	if !ok {
		w.t.Fatalf("watermark carries no integer sequence: %#v", doc)
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		w.t.Fatalf("watermark sequence %v: %v", number, err)
	}
	return value
}

// ---------------------------------------------------------------------------
// Request builders
// ---------------------------------------------------------------------------

var (
	scopeProfile = contract.VersionedRef{
		ID: "scope-memory-room-retrieval", Version: "1",
		Digest: contract.DigestBytes([]byte("scope-memory-room-retrieval")),
	}
	rankerPolicyRef = map[string]any{
		"id": "gms.ranker.lexical-graph.v1", "version": jn(1),
		"digest": "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045c7c9e6ece643805a4c79bb",
	}
	renderProfileRef = map[string]any{
		"id": "rsih.render-profile.default.v1", "version": jn(2),
		"digest": "sha256:9b3794ee3e2f935abc7ab8291ab89310e04898315e992154c7e320bdd124de72",
	}
	guidancePolicyRef = map[string]any{
		"id": "gms.guidance-policy.default.v1", "version": jn(1),
		"digest": "sha256:e8c0891959144f05f533813f43f8b5760726f4a635bdfc3d46230294be08403f",
	}
)

func exploreArguments(session, query string, total, evidenceCap, skillCap, tokens int64, extra map[string]any) map[string]any {
	args := map[string]any{
		"schema_version":       "gms.memory-explore-arguments.v1",
		"explore_session_id":   session,
		"query_text":           query,
		"runtime_context_hash": contract.DigestBytes([]byte("runtime-context-retrieval")),
		"ranker_policy_ref":    rankerPolicyRef,
		"budgets": map[string]any{
			"total_cap":             jn(total),
			"evidence_subcap":       jn(evidenceCap),
			"skill_subcap":          jn(skillCap),
			"guidance_token_budget": jn(tokens),
		},
		"filters": map[string]any{},
	}
	for key, value := range extra {
		args[key] = value
	}
	return args
}

func toolRequest(tool, session string, arguments map[string]any, minSeq uint64) retrieval.ToolRequest {
	request := retrieval.ToolRequest{
		ProxyRequestID: "pr-" + session + "-" + tool,
		RoomID:         "room-retrieval-1",
		AgentID:        "agent-retrieval-1",
		DeliveryID:     "delivery-" + session,
		ToolName:       tool,
		Arguments:      arguments,
		ScopeProfile:   scopeProfile,
		IdempotencyKey: contract.DigestBytes([]byte(session + tool)),
		TimeoutMillis:  5000,
	}
	if minSeq > 0 {
		request.RequestedMinActivationSequence = minSeq
		request.HasMinActivationSequence = true
	}
	return request
}

func requireCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want closed failure %s, got success", want)
	}
	if got := retrieval.CodeOf(err); got != want {
		t.Fatalf("want closed failure %s, got %v (%v)", want, got, err)
	}
}

func resultArray(t *testing.T, result map[string]any, field string) []map[string]any {
	t.Helper()
	raw, ok := result[field].([]any)
	if !ok {
		t.Fatalf("result.%s is not an array: %#v", field, result[field])
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("result.%s entry is not an object", field)
		}
		out = append(out, entry)
	}
	return out
}

// ---------------------------------------------------------------------------
// Red test #1: Explore determinism + scope/budget/fence/freshness/omission
// ---------------------------------------------------------------------------

func TestExploreDeterministicallyEnforcesScopeBudgetFenceAndFreshness(t *testing.T) {
	world := newWorld(t)

	// Three active lineages: a lexical hit ("Diff summarizer"), a guidance
	// skill with branches, and a second lexical hit sharing terms.
	world.registerEvidence("ev-Diff checklist")
	world.registerEvidence("ev-assess-1")
	procRef := world.activateEnvelope(t, "lin-proc", "prop-proc", procedureEnvelope("Diff summarizer"))
	sgRef := world.activateEnvelope(t, "lin-sg", "prop-sg", guidanceEnvelope("Diff checklist"))
	world.project(t)
	world.addAssessment("assess-1", "supports", sgRef, "b1", "ev-assess-1")
	world.project(t) // fold the claim assessment (evidence vertex + supported_by)

	svc := world.service(t, false)
	ctx := context.Background()
	watermarkSeq := world.watermarkSequence()
	if watermarkSeq < 2 {
		t.Fatalf("watermark sequence = %d, want >= 2 activations projected", watermarkSeq)
	}

	t.Run("deterministic order for same input head policy and fences", func(t *testing.T) {
		args := exploreArguments("exp-det", "diff summarizer", 10, 5, 5, 10000, nil)
		first, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-det", args, 0))
		if err != nil {
			t.Fatalf("explore: %v", err)
		}
		second, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-det", args, 0))
		if err != nil {
			t.Fatalf("explore retry: %v", err)
		}
		// Same input, same head, same policy, same fences: identical results
		// (rank order, scores, digests, fences) and the retry did not consume
		// the fences again.
		if first.UpstreamResultDigest != second.UpstreamResultDigest {
			t.Errorf("retry produced a different exact result: %s vs %s", first.UpstreamResultDigest, second.UpstreamResultDigest)
		}
		if fmt.Sprint(first.Result["served_fences"]) != fmt.Sprint(second.Result["served_fences"]) {
			t.Errorf("retry re-consumed the served fences: %#v vs %#v", first.Result["served_fences"], second.Result["served_fences"])
		}

		// A second session over the same head sees the identical order.
		other, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-det-2", exploreArguments("exp-det-2", "diff summarizer", 10, 5, 5, 10000, nil), 0))
		if err != nil {
			t.Fatalf("explore second session: %v", err)
		}
		if fmt.Sprint(other.Result["skill_results"]) != fmt.Sprint(first.Result["skill_results"]) {
			t.Errorf("same input/head/policy produced a different skill order:\n%v\n%v", other.Result["skill_results"], first.Result["skill_results"])
		}

		skills := resultArray(t, first.Result, "skill_results")
		if len(skills) != 2 {
			t.Fatalf("skill_results = %d, want both active lineages", len(skills))
		}
		// The exact lexical match outranks the weaker one; final tie-break is
		// the exact-ref canonical order, never map iteration order.
		topLineage := skills[0]["skill_ref"].(map[string]any)["lineage_id"].(string)
		if topLineage != "lin-proc" {
			t.Errorf("top ranked lineage = %q, want the exact lexical hit lin-proc", topLineage)
		}
		for i, skills := range skills {
			if skills["result_type"] != "skill" {
				t.Errorf("skill_results[%d].result_type = %#v", i, skills["result_type"])
			}
			view, ok := skills["guidance_view"].(map[string]any)
			if !ok {
				t.Fatalf("skill_results[%d] carries no guidance_view", i)
			}
			if _, private := view["watermark"]; private {
				t.Errorf("guidance view carries a private watermark field")
			}
			viewBytes, err := json.Marshal(view)
			if err != nil {
				t.Fatalf("marshal view: %v", err)
			}
			if strings.Contains(string(viewBytes), "budgets") {
				t.Errorf("guidance view carries budget fields")
			}
		}

		// Explore-family self-consistency: watermark, budgets, fences,
		// citations, omissions carrier (empty when nothing was omitted).
		if _, ok := first.Result["omissions"].([]any); !ok {
			t.Errorf("omissions carrier missing: %#v", first.Result["omissions"])
		}
		if len(first.Result["truncation_reason_codes"].([]any)) != 0 {
			t.Errorf("truncation_reason_codes = %#v, want empty", first.Result["truncation_reason_codes"])
		}
		budgets := first.Result["budgets"].(map[string]any)
		used, _ := strconv.ParseInt(budgets["total_used"].(json.Number).String(), 10, 64)
		if used != int64(len(resultArray(t, first.Result, "evidence_results"))+len(skills)) {
			t.Errorf("total_used %d != served count", used)
		}
		watermark, ok := first.Result["watermark"].(map[string]any)
		if !ok || watermark["schema_version"] != "gms.projection-watermark.v1" {
			t.Fatalf("watermark missing: %#v", first.Result["watermark"])
		}
		evidence := resultArray(t, first.Result, "evidence_results")
		if len(evidence) != 1 {
			t.Fatalf("evidence_results = %d, want the projected evidence vertex", len(evidence))
		}
		citation, ok := evidence[0]["citation"].(map[string]any)
		if !ok || citation["evidence_ref"] == nil || citation["claim"] == nil {
			t.Fatalf("evidence citation not typed: %#v", evidence[0]["citation"])
		}
	})

	t.Run("cap truncation emits typed ordered omissions only", func(t *testing.T) {
		args := exploreArguments("exp-trunc", "diff", 1, 1, 1, 10000, nil)
		response, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-trunc", args, 0))
		if err != nil {
			t.Fatalf("explore with caps: %v", err)
		}
		result := response.Result
		omissions, ok := result["omissions"].([]any)
		if !ok || len(omissions) == 0 {
			t.Fatalf("omissions = %#v, want the cap-induced typed refs", result["omissions"])
		}
		budgets := result["budgets"].(map[string]any)
		if budgets["total_used"] != jn(1) {
			t.Errorf("total_used = %#v, want 1 (omitted items never consume budget)", budgets["total_used"])
		}
		if budgets["total_cap"] != jn(1) {
			t.Errorf("total_cap = %#v", budgets["total_cap"])
		}
		codes, ok := result["truncation_reason_codes"].([]any)
		if !ok || len(codes) == 0 {
			t.Fatalf("truncation_reason_codes = %#v, want non-empty", result["truncation_reason_codes"])
		}
		seen := map[string]bool{}
		for _, code := range codes {
			seen[code.(string)] = true
		}
		if !seen["TOTAL_CAP_REACHED"] {
			t.Errorf("truncation_reason_codes = %#v, want TOTAL_CAP_REACHED (truthful: total_used == total_cap)", codes)
		}
		// Omission ordering: kind ascending, then JCS(ref) ascending.
		previous := ""
		servedEvidence := map[string]bool{}
		for _, entry := range resultArray(t, result, "evidence_results") {
			servedEvidence[entry["evidence_ref"].(map[string]any)["evidence_id"].(string)] = true
		}
		for i, entry := range omissions {
			omission := entry.(map[string]any)
			key := omission["kind"].(string) + "\x1f" + contract.CanonicalKey(omission["ref"])
			if previous != "" && key < previous {
				t.Errorf("omissions[%d] out of canonical order", i)
			}
			previous = key
			if omission["kind"] == "evidence" {
				if id := omission["ref"].(map[string]any)["evidence_id"].(string); servedEvidence[id] {
					t.Errorf("omissions list a served evidence ref %s", id)
				}
			}
			switch omission["reason_code"] {
			case "TOTAL_CAP_REACHED":
			case "EVIDENCE_SUBCAP_REACHED":
			case "SKILL_SUBCAP_REACHED":
			case "GUIDANCE_TOKEN_BUDGET_REACHED":
			default:
				t.Errorf("omissions[%d].reason_code = %#v outside the closed set", i, omission["reason_code"])
			}
		}
	})

	t.Run("cross room session reuse fails closed", func(t *testing.T) {
		args := exploreArguments("exp-scope", "diff", 10, 5, 5, 10000, nil)
		if _, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-scope", args, 0)); err != nil {
			t.Fatalf("seed session: %v", err)
		}
		request := toolRequest("memory_explore", "exp-scope", exploreArguments("exp-scope", "checklist", 10, 5, 5, 10000, nil), 0)
		request.RoomID = "room-retrieval-other"
		_, err := svc.Explore(ctx, request)
		requireCode(t, err, "EXPLORE_SCOPE_VIOLATION")
	})

	t.Run("budgets violating the caps fail closed", func(t *testing.T) {
		// A subcap overrunning the shared total can never be filled
		// truthfully (GMS §10.2/§10.8).
		args := exploreArguments("exp-budget", "diff", 1, 2, 1, 10000, nil)
		_, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-budget", args, 0))
		requireCode(t, err, "BUDGET_INVALID")
	})

	t.Run("freshness behind the requested sequence fails closed", func(t *testing.T) {
		args := exploreArguments("exp-fresh", "diff", 10, 5, 5, 10000, nil)
		_, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-fresh", args, watermarkSeq+5))
		requireCode(t, err, "PROJECTION_BEHIND_REQUIRED_SEQUENCE")
	})

	t.Run("fences must continue the session exactly", func(t *testing.T) {
		first, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-fence", exploreArguments("exp-fence", "diff", 10, 5, 5, 10000, nil), 0))
		if err != nil {
			t.Fatalf("first page: %v", err)
		}
		// Advance the projection so the next page can legally advance the
		// watermark (Contract §12.7.2 C6).
		world.activateEnvelope(t, "lin-proc", "prop-proc-2", procedureEnvelope("Diff summarizer v2"))
		world.project(t)

		// A continuation carrying the wrong fences is a fence conflict.
		wrong := exploreArguments("exp-fence", "summarizer", 10, 5, 5, 10000, map[string]any{
			"served_fences": map[string]any{
				"evidence_fence_digest": contract.DigestBytes([]byte("wrong-evidence-fence")),
				"skill_fence_digest":    contract.DigestBytes([]byte("wrong-skill-fence")),
			},
		})
		_, err = svc.Explore(ctx, toolRequest("memory_explore", "exp-fence", wrong, 0))
		requireCode(t, err, "EXPLORE_FENCE_CONFLICT")

		// The exact fences continue the session and the page advances the
		// watermark strictly (the earlier result is served by the retry cache).
		continuation := exploreArguments("exp-fence", "summarizer", 10, 5, 5, 10000, map[string]any{
			"served_fences": first.Result["served_fences"],
		})
		page, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-fence", continuation, 0))
		if err != nil {
			t.Fatalf("continuation: %v", err)
		}
		nextWatermark := page.Result["watermark"].(map[string]any)["projected_through_activation_sequence"].(json.Number)
		previousWatermark := first.Result["watermark"].(map[string]any)["projected_through_activation_sequence"].(json.Number)
		if next, _ := strconv.ParseUint(nextWatermark.String(), 10, 64); next == 0 {
			t.Fatalf("continuation watermark missing")
		} else if previous, _ := strconv.ParseUint(previousWatermark.String(), 10, 64); next <= previous {
			t.Errorf("continuation watermark %d did not advance past %d", next, previous)
		}
		// Served refs never repeat: the superseded v1 revision is not re-served
		// (active_only), and the fence reflects the cumulative served set.
		for _, skill := range resultArray(t, page.Result, "skill_results") {
			if skill["skill_ref"].(map[string]any)["version"] == procRef.Version &&
				skill["skill_ref"].(map[string]any)["lineage_id"] == procRef.LineageID {
				t.Errorf("superseded revision re-served in the continuation page")
			}
		}
	})

	t.Run("expand requires served target exact fences and depth one", func(t *testing.T) {
		first, err := svc.Explore(ctx, toolRequest("memory_explore", "exp-expand", exploreArguments("exp-expand", "diff", 10, 5, 5, 10000, nil), 0))
		if err != nil {
			t.Fatalf("seed explore: %v", err)
		}
		var view map[string]any
		for _, skill := range resultArray(t, first.Result, "skill_results") {
			if skill["skill_ref"].(map[string]any)["lineage_id"] == sgRef.LineageID {
				view = skill["guidance_view"].(map[string]any)
			}
		}
		if view == nil {
			t.Fatalf("guidance skill not served; result=%#v", first.Result["skill_results"])
		}
		expandables, _ := view["expandable_refs"].([]any)
		if len(expandables) == 0 {
			t.Fatalf("guidance view exposes no expandable refs: %#v", view)
		}
		target := expandables[0].(map[string]any)

		// Wrong fences on expand fail closed.
		wrongFences := map[string]any{
			"evidence_fence_digest": contract.DigestBytes([]byte("wrong")),
			"skill_fence_digest":    contract.DigestBytes([]byte("wrong")),
		}
		_, err = svc.Expand(ctx, toolRequest("memory_expand", "exp-expand", expandArguments("exp-expand", "digest-not-served", target, view, wrongFences, 1), 0))
		requireCode(t, err, "EXPLORE_FENCE_CONFLICT")

		// Unknown source query digest fails closed.
		_, err = svc.Expand(ctx, toolRequest("memory_expand", "exp-expand", expandArguments("exp-expand", contract.DigestBytes([]byte("unserved")), target, view, first.Result["served_fences"], 1), 0))
		requireCode(t, err, "EXPLORE_CONTINUATION_INVALID")

		// Depth beyond v1 fails closed.
		deep := expandArguments("exp-expand", "digest-not-served", target, view, first.Result["served_fences"], 2)
		_, err = svc.Expand(ctx, toolRequest("memory_expand", "exp-expand", deep, 0))
		requireCode(t, err, "EXPAND_DEPTH_UNSUPPORTED")
	})
}

func expandArguments(session, sourceDigest string, target map[string]any, view map[string]any, fences any, depth int64) map[string]any {
	fencesMap, ok := fences.(map[string]any)
	if !ok {
		fencesMap = map[string]any{} // unreachable in these tests
	}
	args := map[string]any{
		"schema_version":       "gms.memory-expand-arguments.v1",
		"explore_session_id":   session,
		"source_query_digest":  sourceDigest,
		"target":               target,
		"source_view_hash":     view["view_hash"],
		"runtime_context_hash": contract.DigestBytes([]byte("runtime-context-retrieval")),
		"ranker_policy_ref":    rankerPolicyRef,
		"budgets": map[string]any{
			"total_cap":             jn(10),
			"evidence_subcap":       jn(5),
			"skill_subcap":          jn(5),
			"guidance_token_budget": jn(10000),
		},
		"served_fences":   fencesMap,
		"max_graph_depth": jn(depth),
	}
	return args
}

// ---------------------------------------------------------------------------
// Red test #2: skill_get frozen tool-specific binding
// ---------------------------------------------------------------------------

func TestSkillGetUsesFrozenToolSpecificBinding(t *testing.T) {
	world := newWorld(t)
	world.registerEvidence("ev-Diff checklist")
	sgRef := world.activateEnvelope(t, "lin-sg", "prop-sg", guidanceEnvelope("Diff checklist"))
	world.project(t)
	world.addAssessment("assess-get", "supports", sgRef, "b1", "ev-Diff checklist")
	world.project(t)

	gated := world.service(t, false) // readiness gate closed
	open := world.service(t, true)   // gate green
	ctx := context.Background()

	skillGetArguments := func(visibility string, extra map[string]any) map[string]any {
		args := map[string]any{
			"schema_version":        "gms.skill-get-arguments.v1",
			"skill_ref":             skillRefDoc(sgRef),
			"runtime_context_hash":  contract.DigestBytes([]byte("runtime-context-retrieval")),
			"render_profile_ref":    renderProfileRef,
			"policy_ref":            guidancePolicyRef,
			"guidance_token_budget": jn(10000),
			"visibility":            visibility,
		}
		for key, value := range extra {
			args[key] = value
		}
		return args
	}

	t.Run("disabled until the readiness gate is green", func(t *testing.T) {
		_, err := gated.SkillGet(ctx, toolRequest("skill_get", "get-gated", skillGetArguments("current_active", nil), 0))
		requireCode(t, err, "TOOL_UNSUPPORTED")
	})

	t.Run("closed guidance view only after the gate", func(t *testing.T) {
		response, err := open.SkillGet(ctx, toolRequest("skill_get", "get-open", skillGetArguments("current_active", nil), 0))
		if err != nil {
			t.Fatalf("skill_get: %v", err)
		}
		view := response.Result
		if view["schema_version"] != "gms.guidance-view.v1" {
			t.Fatalf("result schema_version = %#v", view["schema_version"])
		}
		// Closed §7.15 field set: exactly the frozen fields — freshness and
		// authorization ride on the request + read audit, never on the DTO.
		want := map[string]bool{
			"schema_version": false, "source_skill_ref": false, "render_profile_ref": false,
			"policy_ref": false, "runtime_context_hash": false, "included_branch_refs": false,
			"omitted_branch_refs": false, "expandable_refs": false, "content": false,
			"content_token_count": false, "truncated": false, "view_hash": false,
		}
		for key := range view {
			if _, known := want[key]; !known {
				t.Errorf("GuidanceView carries unknown field %q (closed schema violation)", key)
			}
			want[key] = true
		}
		for key, seen := range want {
			if !seen {
				t.Errorf("GuidanceView misses required field %q", key)
			}
		}
		// view_hash == SHA-256(JCS(view minus view_hash)) per the frozen
		// x-digest preimage (all fields except view_hash).
		preimage := make(map[string]any, len(view))
		for key, value := range view {
			if key != "view_hash" {
				preimage[key] = value
			}
		}
		digest, err := contract.DigestOf(preimage)
		if err != nil {
			t.Fatalf("view preimage digest: %v", err)
		}
		if digest != view["view_hash"] {
			t.Errorf("view_hash = %v, recomputed %s", view["view_hash"], digest)
		}
		if view["truncated"] != false {
			t.Errorf("truncated = %#v, want false within budget", view["truncated"])
		}
		// Read audit carries freshness/authorization instead of the DTO.
		audit := response.ReadAudit
		if audit["room_id"] != "room-retrieval-1" || audit["agent_id"] != "agent-retrieval-1" {
			t.Errorf("read audit scope = %#v", audit)
		}
		if _, ok := audit["active_head_activation_sequence"]; !ok {
			t.Errorf("read audit misses active_head_activation_sequence: %#v", audit)
		}
	})

	t.Run("stale render profile below the frozen floor fails", func(t *testing.T) {
		stale := map[string]any{"id": "rsih.render-profile.default.v1", "version": jn(1), "digest": renderProfileRef["digest"]}
		_, err := open.SkillGet(ctx, toolRequest("skill_get", "get-stale", skillGetArguments("current_active", map[string]any{"render_profile_ref": stale}), 0))
		requireCode(t, err, "SCHEMA_VERSION_UNSUPPORTED")
	})

	t.Run("current active must be the authoritative head", func(t *testing.T) {
		ghost := sgRef
		ghost.Version = "9"
		args := skillGetArguments("current_active", map[string]any{"skill_ref": skillRefDoc(ghost)})
		_, err := open.SkillGet(ctx, toolRequest("skill_get", "get-ghost", args, 0))
		requireCode(t, err, "SKILL_NOT_CURRENT_ACTIVE")
	})

	t.Run("historical exact requires exact authorization", func(t *testing.T) {
		_, err := open.SkillGet(ctx, toolRequest("skill_get", "get-hist", skillGetArguments("historical_exact", nil), 0))
		requireCode(t, err, "HISTORICAL_READ_NOT_AUTHORIZED")
	})

	t.Run("non exact and candidate refs never render", func(t *testing.T) {
		latest := map[string]any{
			"schema_version": contract.SchemaSkillArtifactRef, "lineage_id": sgRef.LineageID,
			"version": "latest", "kind": sgRef.Kind, "artifact_digest": sgRef.ArtifactDigest,
		}
		_, err := open.SkillGet(ctx, toolRequest("skill_get", "get-latest", skillGetArguments("current_active", map[string]any{"skill_ref": latest}), 0))
		requireCode(t, err, "NON_EXACT_REF")

		candidate := map[string]any{
			"schema_version": contract.SchemaCandidateArtifactRef,
			"candidate_id":   "cand-prop-sg", "kind": sgRef.Kind,
			"body_digest": sgRef.ArtifactDigest, "origin_type": "skill_proposal",
			"origin_ref": map[string]any{"id": "prop-sg", "version": jn(1), "digest": contract.DigestBytes([]byte("prop-sg"))},
		}
		_, err = open.SkillGet(ctx, toolRequest("skill_get", "get-cand", skillGetArguments("current_active", map[string]any{"skill_ref": candidate}), 0))
		requireCode(t, err, "CANDIDATE_NOT_EXECUTABLE")
	})

	t.Run("freshness rides the request and read audit", func(t *testing.T) {
		head, err := world.activate.ActiveRevision(ctx, "lin-sg")
		if err != nil || head == nil {
			t.Fatalf("active head: %v", err)
		}
		_ = head
		entry, ok, err := world.store.Head(ledger.LedgerActivation, "")
		if err != nil || !ok {
			t.Fatalf("activation head: %v", err)
		}
		_, err = open.SkillGet(ctx, toolRequest("skill_get", "get-behind", skillGetArguments("current_active", nil), entry.Sequence+3))
		requireCode(t, err, "PROJECTION_BEHIND_REQUIRED_SEQUENCE")
	})

	t.Run("readiness gate verifies the frozen tools corpus", func(t *testing.T) {
		green, err := retrieval.VerifySkillGetReadiness(world.confDir)
		if err != nil {
			t.Fatalf("VerifySkillGetReadiness: %v", err)
		}
		if !green {
			t.Errorf("readiness gate not green over the frozen $FIX/tools corpus")
		}
	})
}

// Silence the unused-package linters for helpers kept for parity.
var _ = errors.New
