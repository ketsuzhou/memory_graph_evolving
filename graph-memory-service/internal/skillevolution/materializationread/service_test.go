// Package materializationread tests: GMS-207 exact materialization closure
// API (plan-pinned TDD).
//
// Red TestClosureReadUsesAuthoritativeHeadsAndRejectsTornSequence — the read
// walks ONLY authoritative active heads + the content-addressed artifact
// store, orders the closure canonically, and double-checks the activation
// sequence so a head that moved between the pre-read and the post-read check
// fails closed with ACTIVATION_SEQUENCE_CONFLICT and no guessed closure.
//
// The fail-closed matrix (Contract §13.7, GMS §12.7 failure modes) runs on
// deterministic fake authorities; the main test runs on the real GMS-204
// activation service over the GMS-102 ledger.
package materializationread

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Real-authority harness (GMS-102 ledger + GMS-202 gate + GMS-204 activation)
// ---------------------------------------------------------------------------

type harness struct {
	t          *testing.T
	registry   *ledger.ContractReasonRegistry
	gates      *validation.Gates
	stateDir   string
	store      *ledger.MemoryStore
	proposals  *proposal.Service
	authorizer *stubAuthorizer
	activate   *activation.ActivationService
	artifacts  *artifact.Service
	svc        *Service
	watermark  *stubWatermark
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
	mgr, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	proposals, err := proposal.NewService(gates, filepath.Join(dir, "schema", "state"), store, mgr, registry)
	if err != nil {
		t.Fatalf("proposal.NewService: %v", err)
	}
	actSvc, err := activation.NewActivationService(activation.Config{
		Store:                  store,
		Tx:                     mgr,
		Registry:               registry,
		Gates:                  gates,
		Proposals:              proposals,
		DeactivationAuthorizer: authorizer,
	})
	if err != nil {
		t.Fatalf("NewActivationService: %v", err)
	}
	artSvc, err := artifact.NewService(gates)
	if err != nil {
		t.Fatalf("artifact.NewService: %v", err)
	}
	wm := &stubWatermark{doc: map[string]any{
		"schema_version":                        "gms.projection-watermark.v1",
		"projection_stream":                     "runtime",
		"projection_schema_version":             "gms.runtime-projection.v1",
		"projection_head":                       "head-1",
		"projected_through_activation_sequence": json.Number("1"),
		"source_ledger_digest":                  contract.DigestBytes([]byte("sources")),
		"state":                                 "catching_up",
		"watermark_digest":                      contract.DigestBytes([]byte("watermark")),
	}}
	svc, err := NewService(Config{
		Heads:       actSvc,
		Artifacts:   store,
		Gate:        artSvc,
		Activations: store,
		Registry:    registry,
		Watermark:   wm,
	})
	if err != nil {
		t.Fatalf("materializationread.NewService: %v", err)
	}
	return &harness{
		t:          t,
		registry:   registry,
		gates:      gates,
		stateDir:   filepath.Join(dir, "schema", "state"),
		store:      store,
		proposals:  proposals,
		authorizer: authorizer,
		activate:   actSvc,
		artifacts:  artSvc,
		svc:        svc,
		watermark:  wm,
	}
}

// stubAuthorizer authorizes exactly the registered (authorization id ->
// lineage) pairs; everything else is denied.
type stubAuthorizer struct{ allowed map[string]string }

func (a *stubAuthorizer) AuthorizeDeactivation(auth contract.VersionedRef, lineageID string, skill contract.SkillArtifactRef) error {
	if a.allowed[auth.ID] == lineageID {
		return nil
	}
	return errors.New("deactivation authorization denied")
}

// stubWatermark is an informational §7.14 supplier (never gates the read).
type stubWatermark struct{ doc map[string]any }

func (w *stubWatermark) ProjectionWatermark(context.Context) (map[string]any, bool) {
	out := make(map[string]any, len(w.doc))
	for k, v := range w.doc {
		out[k] = v
	}
	return out, true
}

func jn(v int64) any { return json.Number(strconv.FormatInt(v, 10)) }

func jnString(v string) any { return json.Number(v) }

func portRef(id string) map[string]any {
	return map[string]any{
		"schema_id": "https://example.com/schemas/" + id,
		"version":   jn(1),
		"digest":    contract.DigestBytes([]byte("port:" + id)),
	}
}

func evidenceRef(id string) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaEvidenceRef,
		"evidence_id":     id,
		"version":         jn(1),
		"evidence_digest": contract.DigestBytes([]byte(id)),
		"commit_state":    "committed",
		"evidence_kind":   "success_path",
	}
}

func procedureEnvelope(title string, withPorts bool) map[string]any {
	body := map[string]any{
		"steps": []any{map[string]any{
			"step_id":        "s1",
			"instruction":    "summarize the diff",
			"preconditions":  []any{},
			"postconditions": []any{},
			"failure_action": "stop",
		}},
	}
	if withPorts {
		body["input_port_schema"] = portRef(title + "-in")
		body["output_port_schema"] = portRef(title + "-out")
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v1",
		"kind":           "human_procedure",
		"title":          title,
		"description":    "procedure body under test",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{map[string]any{"capability": "memory_explore", "scope": "room-shared-space"}},
		"body":           body,
	}
}

func guidanceEnvelope(title string, withPorts bool) map[string]any {
	body := map[string]any{
		"causal_context": map[string]any{
			"summary":    "prior context",
			"claim_refs": []any{},
		},
		"branches": []any{map[string]any{
			"branch_id": "b1",
			"when":      map[string]any{},
			"action": map[string]any{"guidance": "run the checklist", "failure_action": "stop", "evidence_refs": []any{evidenceRef("ev-" + title)}},
			"future": map[string]any{"expected_outcome": "checked", "critical_steps": []any{}, "final_task_impact": "commit verified"},
		}},
	}
	if withPorts {
		body["input_port_schema"] = portRef(title + "-in")
		body["output_port_schema"] = portRef(title + "-out")
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v1",
		"kind":           "step_guidance",
		"title":          title,
		"description":    "step guidance body under test",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{},
		"body":           body,
	}
}

func compositeEnvelope(title string, children ...contract.SkillArtifactRef) map[string]any {
	childDocs := make([]any, 0, len(children))
	for i, ref := range children {
		childDocs = append(childDocs, map[string]any{
			"child_id":    fmt.Sprintf("c%d", i),
			"skill_ref":   skillRefDoc(ref),
			"input_port":  "in",
			"output_port": "out",
		})
	}
	body := map[string]any{
		"children":                  childDocs,
		"edges":                     []any{},
		"retry_policies":            []any{},
		"failure_handlers":          []any{},
		"orchestration_permissions": []any{},
		"input_port_schema":         portRef(title + "-in"),
		"output_port_schema":        portRef(title + "-out"),
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v1",
		"kind":           "composite",
		"title":          title,
		"description":    "composite body under test",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{},
		"body":           body,
	}
}

func skillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         jnString(ref.Version),
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

// canonicalEnvelope canonicalizes one envelope through the real GMS-202 gate
// and returns the exact canonical bytes + digest.
func (h *harness) canonicalEnvelope(t *testing.T, envelope map[string]any) ([]byte, string) {
	t.Helper()
	canonical, err := h.artifacts.Canonicalize(envelope)
	if err != nil {
		t.Fatalf("canonicalize %s envelope: %v", envelope["kind"], err)
	}
	return canonical.Canonical(), canonical.BodyDigest()
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

func (h *harness) proposalDoc(t *testing.T, proposalID, kind string) map[string]any {
	t.Helper()
	doc := map[string]any{
		"schema_version":   "gms.skill-proposal.v1",
		"proposal_id":      proposalID,
		"proposal_version": jn(1),
		"proposed_kind":    kind,
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
		"evidence_refs": []any{map[string]any{
			"schema_version":  contract.SchemaEvidenceRef,
			"evidence_id":     "ev-" + proposalID,
			"version":         jn(1),
			"evidence_digest": contract.DigestBytes([]byte("ev-" + proposalID)),
			"commit_state":    "committed",
			"evidence_kind":   "success_path",
		}},
		"requested_operation": "create_lineage",
		"origin": map[string]any{
			"initiator_type": "model",
			"initiator_ref":  "agent-1",
			"request_ref":    "req-" + proposalID,
		},
		"policy_refs": []any{map[string]any{
			"id": "policy-static-gates", "version": jn(1), "digest": contract.DigestBytes([]byte("policy-static-gates")),
		}},
	}
	digest, err := h.gates.ComputeDigestPreimage(doc, "skill-proposal.schema.json")
	if err != nil {
		t.Fatalf("proposal digest preimage: %v", err)
	}
	doc["proposal_digest"] = digest
	return doc
}

func (h *harness) decisionDoc(t *testing.T, cand contract.CandidateArtifactRef) map[string]any {
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
			"origin_ref":     map[string]any{"id": cand.OriginRef.ID, "version": jnString(cand.OriginRef.Version), "digest": cand.OriginRef.Digest},
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

// stubView is a frozen candidate view over canonical bytes.
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

type stubDecision struct{ doc map[string]any }

func (d stubDecision) Doc() map[string]any { return d.doc }
func (d stubDecision) Outcome() string     { s, _ := contract.AsString(d.doc["outcome"]); return s }
func (d stubDecision) DecisionDigest() string {
	s, _ := contract.AsString(d.doc["decision_digest"])
	return s
}

// commitCandidate commits the candidate binding (content + candidate ledger
// event) exactly like the GMS-202 binding service's committed shape.
func (h *harness) commitCandidate(t *testing.T, cand contract.CandidateArtifactRef, body []byte) {
	t.Helper()
	mgr, err := ledger.NewManager(h.store, h.registry)
	if err != nil {
		t.Fatalf("commit candidate manager: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version": cand.SchemaVersion,
		"candidate_id":   cand.CandidateID,
		"kind":           cand.Kind,
		"body_digest":    cand.BodyDigest,
		"origin_type":    cand.OriginType,
		"origin_ref":     map[string]any{"id": cand.OriginRef.ID, "version": jnString(cand.OriginRef.Version), "digest": cand.OriginRef.Digest},
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

// activateEnvelope runs the full protected release for one envelope on the
// given lineage and returns the released exact ref (the new active head).
func (h *harness) activateEnvelope(t *testing.T, lineageID, proposalID string, envelope map[string]any) contract.SkillArtifactRef {
	t.Helper()
	ctx := context.Background()
	body, digest := h.canonicalEnvelope(t, envelope)
	propDoc := h.proposalDoc(t, proposalID, envelope["kind"].(string))
	for _, edge := range activationPendingPath {
		if _, err := h.proposals.AppendTransition(ctx, propDoc, edge[0], edge[1]); err != nil {
			t.Fatalf("seed %s -> %s: %v", edge[0], edge[1], err)
		}
	}
	propDigest, _ := contract.AsString(propDoc["proposal_digest"])
	cand := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   "cand-" + proposalID,
		Kind:          envelope["kind"].(string),
		BodyDigest:    digest,
		OriginType:    "skill_proposal",
		OriginRef:     contract.VersionedRef{ID: proposalID, Version: "1", Digest: propDigest},
	}
	h.commitCandidate(t, cand, body)
	active, err := h.activate.ActiveRevision(ctx, lineageID)
	if err != nil {
		t.Fatalf("active head before activation: %v", err)
	}
	result, err := h.activate.Activate(ctx, activation.ActivateRequest{
		Decision:     stubDecision{doc: h.decisionDoc(t, cand)},
		Candidate:    &stubView{ref: cand, body: body},
		LineageID:    lineageID,
		ExpectedHead: active,
		ProposalDoc:  propDoc,
	})
	if err != nil {
		t.Fatalf("activate %s: %v", lineageID, err)
	}
	return result.ReleasedRef
}

func refOf(lineage string, version int64, digest string) contract.SkillArtifactRef {
	return contract.SkillArtifactRef{
		SchemaVersion:  contract.SchemaSkillArtifactRef,
		LineageID:      lineage,
		Version:        strconv.FormatInt(version, 10),
		Kind:           "human_procedure",
		ArtifactDigest: digest,
	}
}

// requireCode asserts err carries exactly the wanted closed reason code.
func requireCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want failure %s, got success", want)
	}
	if got := CodeOf(err); got != want {
		t.Fatalf("reason code = %q (%v), want %q", got, err, want)
	}
}

func requireTokenShape(t *testing.T, token string) {
	t.Helper()
	if !strings.HasPrefix(token, "sha256:") || len(token) != len("sha256:")+64 {
		t.Fatalf("torn_read_token %q is not a sha256 digest", token)
	}
}

// ---------------------------------------------------------------------------
// Red: the plan-pinned test
// ---------------------------------------------------------------------------

func TestClosureReadUsesAuthoritativeHeadsAndRejectsTornSequence(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Authority world: two activated leaves + one composite over both.
	procRef := h.activateEnvelope(t, "proc-diff-summarizer", "prop-proc-1", procedureEnvelope("Diff summarizer", true))
	sgRef := h.activateEnvelope(t, "sg-commit-checklist", "prop-sg-1", guidanceEnvelope("Commit checklist", true))
	compRef := h.activateEnvelope(t, "comp-review-orchestrator", "prop-comp-1", compositeEnvelope("Review orchestrator", procRef, sgRef))
	if compRef.Kind != "composite" {
		t.Fatalf("composite ref kind = %q", compRef.Kind)
	}

	t.Run("exact closure from authoritative heads, canonical order", func(t *testing.T) {
		closure, err := h.svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{compRef}})
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if closure.SchemaVersion != SchemaClosureRead {
			t.Fatalf("schema_version = %q, want %q", closure.SchemaVersion, SchemaClosureRead)
		}
		if len(closure.Roots) != 1 || closure.Roots[0] != compRef {
			t.Fatalf("roots = %+v, want exactly the requested root", closure.Roots)
		}
		if len(closure.Nodes) != 3 {
			t.Fatalf("closure has %d nodes, want 3 (composite root + 2 children)", len(closure.Nodes))
		}
		// DFS preorder from the canonically ordered root: the root first,
		// then children in canonical (lineage, version, digest) order.
		wantOrder := []string{"comp-review-orchestrator", "proc-diff-summarizer", "sg-commit-checklist"}
		for i, want := range wantOrder {
			if closure.Nodes[i].Ref.LineageID != want {
				t.Fatalf("node[%d] = %s, want %s (canonical order)", i, closure.Nodes[i].Ref.LineageID, want)
			}
		}
		// The freeze sequence is the global activation stream head (3 events).
		if closure.ActivationSequence != 3 {
			t.Fatalf("activation_sequence = %d, want 3", closure.ActivationSequence)
		}
		requireTokenShape(t, closure.TornReadToken)
		if closure.ActivationHeadDigest == "" {
			t.Fatalf("activation_head_digest missing")
		}
		// Every node carries the exact canonical bytes (digests to its ref),
		// permissions and — for composite children — both port schemas.
		for _, node := range closure.Nodes {
			if digest := contract.DigestBytes(node.CanonicalBytes); digest != node.Ref.ArtifactDigest {
				t.Fatalf("node %s canonical bytes digest to %s, ref says %s", node.Ref.LineageID, digest, node.Ref.ArtifactDigest)
			}
			if node.Kind != node.Ref.Kind {
				t.Fatalf("node %s kind %q disagrees with ref kind %q", node.Ref.LineageID, node.Kind, node.Ref.Kind)
			}
			if node.InputPort == nil || node.OutputPort == nil {
				t.Fatalf("node %s (composite child) missing port schemas", node.Ref.LineageID)
			}
			if len(node.Permissions) == 0 && node.Ref.LineageID == "proc-diff-summarizer" {
				t.Fatalf("procedure node lost its envelope permissions")
			}
		}
		root := closure.Nodes[0]
		if len(root.Dependencies) != 2 {
			t.Fatalf("root dependencies = %d, want the 2 composite children", len(root.Dependencies))
		}
		if root.Dependencies[0] != procRef || root.Dependencies[1] != sgRef {
			t.Fatalf("root dependencies out of canonical order: %+v", root.Dependencies)
		}
		if len(closure.Nodes[1].Dependencies) != 0 || len(closure.Nodes[2].Dependencies) != 0 {
			t.Fatalf("leaf nodes must carry no dependencies")
		}
		// The watermark rides along as information only — catching_up never
		// blocks an exact read (GMS §8.1/§11.1).
		if closure.Watermark == nil || closure.Watermark["state"] != "catching_up" {
			t.Fatalf("informational projection watermark missing: %+v", closure.Watermark)
		}
	})

	t.Run("same sequence same response", func(t *testing.T) {
		first, err := h.svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{compRef}})
		if err != nil {
			t.Fatalf("first Read: %v", err)
		}
		second, err := h.svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{compRef}})
		if err != nil {
			t.Fatalf("second Read: %v", err)
		}
		if first.ActivationSequence != second.ActivationSequence ||
			first.TornReadToken != second.TornReadToken ||
			first.ActivationHeadDigest != second.ActivationHeadDigest {
			t.Fatalf("same-sequence reads disagree:\n%+v\n%+v", first, second)
		}
		if !reflect.DeepEqual(first.Nodes, second.Nodes) {
			t.Fatalf("same-sequence reads returned different closures")
		}
	})

	t.Run("multi-root request order is canonical, not arrival order", func(t *testing.T) {
		closure, err := h.svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{sgRef, procRef, compRef}})
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got := make([]string, 0, len(closure.Roots))
		for _, ref := range closure.Roots {
			got = append(got, ref.LineageID)
		}
		want := []string{"comp-review-orchestrator", "proc-diff-summarizer", "sg-commit-checklist"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("roots ordered by arrival %v, want canonical %v", got, want)
		}
		// Shared subtree nodes appear exactly once.
		if len(closure.Nodes) != 3 {
			t.Fatalf("closure nodes = %d, want the deduplicated 3", len(closure.Nodes))
		}
	})

	t.Run("root must be the current active head", func(t *testing.T) {
		// Supersede the procedure: v2 becomes the active head.
		v2 := h.activateEnvelope(t, "proc-diff-summarizer", "prop-proc-2", procedureEnvelope("Diff summarizer v2", true))
		if v2.Version == procRef.Version {
			t.Fatalf("supersede did not assign a new version")
		}
		_, err := h.svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{refOf("proc-diff-summarizer", 1, procRef.ArtifactDigest)}})
		requireCode(t, err, ReasonSkillNotCurrentActive)

		// A composite pinned to the superseded child can no longer close.
		_, err = h.svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{compRef}})
		requireCode(t, err, ReasonCompositeChildNotActive)
	})

	t.Run("deactivated root has no active head", func(t *testing.T) {
		h.authorizer.allowed["auth-deact-sg"] = "sg-commit-checklist"
		if _, err := h.activate.Deactivate(ctx, activation.DeactivateRequest{
			LineageID:     "sg-commit-checklist",
			SkillRef:      sgRef,
			Authorization: contract.VersionedRef{ID: "auth-deact-sg", Version: "1", Digest: contract.DigestBytes([]byte("auth-deact-sg"))},
		}); err != nil {
			t.Fatalf("deactivate: %v", err)
		}
		_, err := h.svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{sgRef}})
		requireCode(t, err, ReasonNoActiveHead)
	})

	t.Run("torn activation sequence fails closed without guessing", func(t *testing.T) {
		// A fresh single-root world (procedure leaf only) whose artifact
		// fetch tears the read: right after the walker's first content Get,
		// a concurrent activation commits and moves the global head.
		hh := newHarness(t)
		leaf := hh.activateEnvelope(t, "proc-tear-free", "prop-tear-1", procedureEnvelope("Tear-free leaf", true))
		torn := &tornArtifactBytes{
			ContentStore: hh.store,
			onFirstGet: func() {
				hh.activateEnvelope(hh.t, "proc-late", "prop-tear-2", procedureEnvelope("Late activation", true))
			},
		}
		svc, err := NewService(Config{
			Heads:       hh.activate,
			Artifacts:   torn,
			Gate:        hh.artifacts,
			Activations: hh.store,
			Registry:    hh.registry,
		})
		if err != nil {
			t.Fatalf("NewService(torn): %v", err)
		}
		if err != nil {
			t.Fatalf("NewService(torn): %v", err)
		}
		closure, err := svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{leaf}})
		requireCode(t, err, ReasonActivationSequenceConflict)
		if closure != nil {
			t.Fatalf("torn read returned a closure; no guess is permitted")
		}
	})
}

// tornArtifactBytes wraps the content store and commits a racing activation
// between the read's head baseline and its post-walk double-check.
type tornArtifactBytes struct {
	ledger.ContentStore // embedded: the real content store
	onFirstGet          func()
	once                sync.Once
}

func (t *tornArtifactBytes) Get(digest string) ([]byte, bool, error) {
	t.once.Do(t.onFirstGet)
	return t.ContentStore.Get(digest)
}

// ---------------------------------------------------------------------------
// Fail-closed matrix over deterministic fake authorities
// ---------------------------------------------------------------------------

// fakeHeads resolves lineage -> active head (nil = no active revision).
type fakeHeads struct {
	heads map[string]*contract.SkillArtifactRef
	err   error
}

func (f fakeHeads) ActiveRevision(context.Context, string) (*contract.SkillArtifactRef, error) {
	return nil, f.err // per-lineage variant below
}

// perLineageHeads is a fakeHeads keyed by lineage.
type perLineageHeads struct {
	heads map[string]*contract.SkillArtifactRef
}

func (p perLineageHeads) ActiveRevision(_ context.Context, lineageID string) (*contract.SkillArtifactRef, error) {
	return p.heads[lineageID], nil
}

// fakeBytes serves canonical bytes by digest.
type fakeBytes struct {
	bytes map[string][]byte
}

func (f fakeBytes) Get(digest string) ([]byte, bool, error) {
	data, ok := f.bytes[digest]
	return data, ok, nil
}

// fakeStream serves a fixed activation head. The hook runs before the SECOND
// Head read only (used to tear the read between the baseline and the
// double-check without tearing the baseline itself).
type fakeStream struct {
	seq    uint64
	digest string
	hook   func(*fakeStream)
	calls  int
}

func (f *fakeStream) Head(kind ledger.LedgerKind, stream string) (ledger.Entry, bool, error) {
	f.calls++
	if f.hook != nil && f.calls == 2 {
		f.hook(f)
	}
	if f.seq == 0 {
		return ledger.Entry{}, false, nil
	}
	return ledger.Entry{
		Ledger:        kind,
		Stream:        stream,
		Sequence:      f.seq,
		EventID:       "ev-head",
		PayloadDigest: f.digest,
	}, true, nil
}

// fakeWorld builds a service over fake authorities and the real artifact gate.
func fakeWorld(t *testing.T, heads map[string]*contract.SkillArtifactRef, bytes map[string][]byte, stream *fakeStream) *Service {
	t.Helper()
	h := newHarness(t) // gates + artifact service only
	svc, err := NewService(Config{
		Heads:       perLineageHeads{heads: heads},
		Artifacts:   fakeBytes{bytes: bytes},
		Gate:        h.artifacts,
		Activations: stream,
		Registry:    h.registry,
	})
	if err != nil {
		t.Fatalf("NewService(fake): %v", err)
	}
	return svc
}

// seedFake seeds one envelope as both stored canonical bytes and active head.
// The bytes are the raw JCS canonicalization (NOT the artifact gate): the
// deliberately-invalid fail-closed envelopes (unknown extension, permission
// overflow) must still exist in the store so the READ fails on them — the
// gate runs at read time, exactly as it does against torn historical content.
func seedFake(t *testing.T, artifacts *artifact.Service, heads map[string]*contract.SkillArtifactRef, bytes map[string][]byte, lineage string, version int64, envelope map[string]any) contract.SkillArtifactRef {
	t.Helper()
	_ = artifacts // gate unused here; kept for signature symmetry with valid worlds
	canonical, err := contract.JCS(contract.NormalizeForHashing(envelope))
	if err != nil {
		t.Fatalf("canonicalize %s: %v", lineage, err)
	}
	digest := contract.DigestBytes(canonical)
	ref := contract.SkillArtifactRef{
		SchemaVersion:  contract.SchemaSkillArtifactRef,
		LineageID:      lineage,
		Version:        strconv.FormatInt(version, 10),
		Kind:           envelope["kind"].(string),
		ArtifactDigest: digest,
	}
	heads[lineage] = &ref
	bytes[digest] = canonical
	return ref
}

func TestClosureRootRefParsingIsExactOnly(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]any
		want string
	}{
		{"candidate root", map[string]any{
			"schema_version": contract.SchemaCandidateArtifactRef, "candidate_id": "cand-1", "kind": "human_procedure",
			"body_digest": "sha256:" + strings.Repeat("a", 64), "origin_type": "skill_proposal",
			"origin_ref": map[string]any{"id": "p", "version": jn(1), "digest": "sha256:" + strings.Repeat("b", 64)},
		}, ReasonCandidateNotExecutable},
		{"latest version", map[string]any{
			"schema_version": contract.SchemaSkillArtifactRef, "lineage_id": "l", "version": "latest",
			"kind": "human_procedure", "artifact_digest": "sha256:" + strings.Repeat("a", 64),
		}, ReasonNonExactRef},
		{"graph node form", map[string]any{
			"schema_version": contract.SchemaSkillArtifactRef, "lineage_id": "l", "version": jn(1),
			"kind": "human_procedure", "artifact_digest": "sha256:" + strings.Repeat("a", 64), "graph_node_id": "n1",
		}, ReasonNonExactRef},
		{"node id form", map[string]any{"node_id": "n1"}, ReasonNonExactRef},
		{"naked lineage name", map[string]any{"lineage_id": "l"}, ReasonNonExactRef},
		{"naked id", map[string]any{"id": "l", "version": jn(1), "digest": "sha256:" + strings.Repeat("a", 64)}, ReasonNonExactRef},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseRootRef(tc.obj)
			requireCode(t, err, tc.want)
		})
	}
	// The exact form parses.
	ref, err := ParseRootRef(map[string]any{
		"schema_version": contract.SchemaSkillArtifactRef, "lineage_id": "l", "version": jn(3),
		"kind": "composite", "artifact_digest": "sha256:" + strings.Repeat("c", 64),
	})
	if err != nil || ref.LineageID != "l" || ref.Version != "3" {
		t.Fatalf("exact root parse = (%+v, %v)", ref, err)
	}
}

func TestClosureFailClosedMatrix(t *testing.T) {
	ctx := context.Background()
	base := newHarness(t) // for the real gate

	stream := &fakeStream{seq: 7, digest: "sha256:" + strings.Repeat("7", 64)}

	t.Run("missing artifact content", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		ref := seedFake(t, base.artifacts, heads, bytes, "proc-ghost", 1, procedureEnvelope("Ghost", true))
		delete(bytes, ref.ArtifactDigest) // head points at uncommitted content
		_, err := fakeWorld(t, heads, bytes, stream).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{ref}})
		requireCode(t, err, ReasonDigestMismatch)
	})

	t.Run("stored bytes digest mismatch", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		ref := seedFake(t, base.artifacts, heads, bytes, "proc-tamper", 1, procedureEnvelope("Tampered", true))
		bytes[ref.ArtifactDigest] = []byte(`{"schema_version":"gms.skill-artifact.v1"}`) // same address, different bytes
		_, err := fakeWorld(t, heads, bytes, stream).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{ref}})
		requireCode(t, err, ReasonDigestMismatch)
	})

	t.Run("unknown required extension", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		envelope := procedureEnvelope("Extended", true)
		envelope["extensions"] = map[string]any{
			"ai.example.required": map[string]any{"required": true, "value": map[string]any{}},
		}
		ref := seedFake(t, base.artifacts, heads, bytes, "proc-ext", 1, envelope)
		_, err := fakeWorld(t, heads, bytes, stream).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{ref}})
		requireCode(t, err, ReasonUnknownRequiredExtension)
	})

	t.Run("permission over host cap", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		envelope := procedureEnvelope("Overprivileged", true)
		envelope["permissions"] = []any{map[string]any{"capability": "rm-rf", "scope": "host"}}
		seedFake(t, base.artifacts, heads, bytes, "proc-cap", 1, envelope)
		ref := *heads["proc-cap"]
		_, err := fakeWorld(t, heads, bytes, stream).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{ref}})
		requireCode(t, err, ReasonPermissionCapExceeded)
	})

	t.Run("ref kind drift against body", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		ref := seedFake(t, base.artifacts, heads, bytes, "proc-drift", 1, procedureEnvelope("Drift", true))
		drifted := ref
		drifted.Kind = "step_guidance"
		heads["proc-drift"] = &drifted
		_, err := fakeWorld(t, heads, bytes, stream).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{drifted}})
		requireCode(t, err, ReasonSkillKindInvalid)
	})

	t.Run("composite child missing port schemas", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		leaf := seedFake(t, base.artifacts, heads, bytes, "proc-noport", 1, procedureEnvelope("No ports", false))
		root := seedFake(t, base.artifacts, heads, bytes, "comp-noport", 1, compositeEnvelope("Needs child ports", leaf))
		_, err := fakeWorld(t, heads, bytes, stream).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{root}})
		requireCode(t, err, ReasonPortSchemaMissing)
	})

	t.Run("closure cycle", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		// Under content addressing a digest-verified cycle is impossible (a
		// cycle needs a digest fixed point), so the DEPENDENCY_CYCLE guard is
		// a defense-in-depth invariant of the walk itself: pin it directly by
		// pre-seeding the walker's visiting stack with the root, which is
		// exactly the state a buggy or tampered body would produce.
		leaf := seedFake(t, base.artifacts, heads, bytes, "proc-cycle", 1, procedureEnvelope("Cycled", true))
		svc := fakeWorld(t, heads, bytes, stream)
		w := &walker{
			service:  svc,
			ctx:      ctx,
			maxNodes: 10,
			visiting: map[string]bool{refKey(leaf): true},
			visited:  map[string]*ClosureNode{},
		}
		_, err := w.walk(leaf, false)
		requireCode(t, err, ReasonDependencyCycle)
	})

	t.Run("node budget exceeded", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		leaf := seedFake(t, base.artifacts, heads, bytes, "proc-budget", 1, procedureEnvelope("Budgeted", true))
		root := seedFake(t, base.artifacts, heads, bytes, "comp-budget", 1, compositeEnvelope("Budgeted root", leaf))
		svc := fakeWorld(t, heads, bytes, stream)
		_, err := svc.Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{root}, MaxNodes: 1})
		requireCode(t, err, ReasonBudgetExceeded)
	})

	t.Run("fake torn sequence", func(t *testing.T) {
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		ref := seedFake(t, base.artifacts, heads, bytes, "proc-torn", 1, procedureEnvelope("Torn fake", true))
		torn := &fakeStream{seq: 7, digest: "sha256:" + strings.Repeat("7", 64)}
		torn.hook = func(f *fakeStream) {
			f.seq = 8 // the head advances between baseline and double-check
			f.digest = "sha256:" + strings.Repeat("8", 64)
		}
		closure, err := fakeWorld(t, heads, bytes, torn).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{ref}})
		requireCode(t, err, ReasonActivationSequenceConflict)
		if closure != nil {
			t.Fatalf("torn read must return no closure")
		}
	})

	t.Run("empty and duplicate roots", func(t *testing.T) {
		stream := &fakeStream{seq: 7, digest: "sha256:" + strings.Repeat("7", 64)}
		svc := fakeWorld(t, map[string]*contract.SkillArtifactRef{}, map[string][]byte{}, stream)
		if _, err := svc.Read(ctx, ReadRequest{}); err == nil {
			t.Fatalf("empty root set must fail closed")
		} else {
			requireCode(t, err, ReasonRefMismatch)
		}
		heads := map[string]*contract.SkillArtifactRef{}
		bytes := map[string][]byte{}
		ref := seedFake(t, base.artifacts, heads, bytes, "proc-dup", 1, procedureEnvelope("Dup", true))
		_, err := fakeWorld(t, heads, bytes, stream).Read(ctx, ReadRequest{Roots: []contract.SkillArtifactRef{ref, ref}})
		requireCode(t, err, ReasonRefMismatch)
	})
}

// The canonical comparator is pinned independently of any walk.
func TestCanonicalRefOrdering(t *testing.T) {
	d := func(seed byte) string { return "sha256:" + strings.Repeat(string(seed+'0'), 64) }
	refs := []contract.SkillArtifactRef{
		{LineageID: "b-line", Version: "10", ArtifactDigest: d(1)},
		{LineageID: "b-line", Version: "2", ArtifactDigest: d(1)},
		{LineageID: "b-line", Version: "2", ArtifactDigest: d(0)},
		{LineageID: "a-line", Version: "99", ArtifactDigest: d(9)},
	}
	sort.Slice(refs, func(i, j int) bool { return CompareRefs(refs[i], refs[j]) < 0 })
	want := []string{"a-line:99:" + d(9), "b-line:2:" + d(0), "b-line:2:" + d(1), "b-line:10:" + d(1)}
	got := make([]string, 0, len(refs))
	for _, ref := range refs {
		got = append(got, fmt.Sprintf("%s:%s:%s", ref.LineageID, ref.Version, ref.ArtifactDigest))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical order = %v, want %v (version compares numerically)", got, want)
	}
}
