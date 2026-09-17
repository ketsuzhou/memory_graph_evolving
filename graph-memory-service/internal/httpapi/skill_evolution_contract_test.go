// Package httpapi_test: GMS-207 closure read transport contract test.
//
// TestSkillEvolutionClosureContract pins the private skill evolution surface
// POST /v1/skill-evolution/closure:read against the REAL GMS-102/202/204
// authorities (ledger, artifact gate, activation service) through
// materializationread.Service + NewSkillEvolutionHandler: the happy-path
// response shape, same-sequence idempotency, and the fail-closed transport
// mapping (401/400/404/409/422) with the closed reason codes, plus
// OpenAPI-vs-implementation consistency.
package httpapi_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/materializationread"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/retrieval"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Real-authority world (GMS-102 ledger + GMS-202 gate + GMS-204 activation)
// ---------------------------------------------------------------------------

type sevWorld struct {
	t          *testing.T
	registry   *ledger.ContractReasonRegistry
	gates      *validation.Gates
	stateDir   string
	store      *ledger.MemoryStore
	proposals  *proposal.Service
	authorizer *sevAuthorizer
	activate   *activation.ActivationService
	artifacts  *artifact.Service
	watermark  *sevWatermark
}

type sevAuthorizer struct{ allowed map[string]string }

func (a *sevAuthorizer) AuthorizeDeactivation(auth contract.VersionedRef, lineageID string, skill contract.SkillArtifactRef) error {
	if a.allowed[auth.ID] == lineageID {
		return nil
	}
	return errors.New("deactivation authorization denied")
}

// sevWatermark is an informational Contract §7.14 supplier (never gates).
type sevWatermark struct{ doc map[string]any }

func (w *sevWatermark) ProjectionWatermark(context.Context) (map[string]any, bool) {
	out := make(map[string]any, len(w.doc))
	for k, v := range w.doc {
		out[k] = v
	}
	return out, true
}

func newSkillEvolutionWorld(t *testing.T) *sevWorld {
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
	authorizer := &sevAuthorizer{allowed: map[string]string{}}
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
	return &sevWorld{
		t:          t,
		registry:   registry,
		gates:      gates,
		stateDir:   filepath.Join(dir, "schema", "state"),
		store:      store,
		proposals:  proposals,
		authorizer: authorizer,
		activate:   actSvc,
		artifacts:  artSvc,
		watermark: &sevWatermark{doc: map[string]any{
			"schema_version":                        "gms.projection-watermark.v1",
			"projection_stream":                     "runtime",
			"projection_schema_version":             "gms.runtime-projection.v1",
			"projection_head":                       "head-1",
			"projected_through_activation_sequence": json.Number("1"),
			"source_ledger_digest":                  contract.DigestBytes([]byte("sources")),
			"state":                                 "catching_up",
			"watermark_digest":                      contract.DigestBytes([]byte("watermark")),
		}},
	}
}

// service builds the closure reader over this world; artifacts may be a
// content-store wrapper (the torn-read case).
func (w *sevWorld) service(t *testing.T, artifacts materializationread.ArtifactBytes) *materializationread.Service {
	t.Helper()
	reader, err := materializationread.NewService(materializationread.Config{
		Heads:       w.activate,
		Artifacts:   artifacts,
		Gate:        w.artifacts,
		Activations: w.store,
		Registry:    w.registry,
		Watermark:   w.watermark,
	})
	if err != nil {
		t.Fatalf("materializationread.NewService: %v", err)
	}
	return reader
}

func newClosureServer(t *testing.T, reader *materializationread.Service) *httptest.Server {
	t.Helper()
	handler := httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{
		Token:  contractToken,
		Reader: reader,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

// ---------------------------------------------------------------------------
// Envelope builders (integer-only JSON numbers, Contract §6.3)
// ---------------------------------------------------------------------------

func sevJN(v int64) any { return json.Number(strconv.FormatInt(v, 10)) }

func sevPortRef(id string) map[string]any {
	return map[string]any{
		"schema_id": "https://example.com/schemas/" + id,
		"version":   sevJN(1),
		"digest":    contract.DigestBytes([]byte("port:" + id)),
	}
}

func sevEvidenceRef(id string) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaEvidenceRef,
		"evidence_id":     id,
		"version":         sevJN(1),
		"evidence_digest": contract.DigestBytes([]byte(id)),
		"commit_state":    "committed",
		"evidence_kind":   "success_path",
	}
}

func sevProcedureEnvelope(title string, withPorts bool) map[string]any {
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
		body["input_port_schema"] = sevPortRef(title + "-in")
		body["output_port_schema"] = sevPortRef(title + "-out")
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "human_procedure",
		"title":          title,
		"description":    "procedure body under test",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{map[string]any{"capability": "memory_explore", "scope": "room-shared-space"}},
		"body":           body,
	}
}

func sevGuidanceEnvelope(title string, withPorts bool) map[string]any {
	body := map[string]any{
		"causal_context": map[string]any{"summary": "prior context", "claim_refs": []any{}},
		"branches": []any{map[string]any{
			"branch_id": "b1",
			"when":      map[string]any{},
			"action":    map[string]any{"guidance": "run the checklist", "failure_action": "stop", "evidence_refs": []any{sevEvidenceRef("ev-" + title)}},
			"future":    map[string]any{"expected_outcome": "checked", "critical_steps": []any{}, "final_task_impact": "commit verified"},
		}},
	}
	if withPorts {
		body["input_port_schema"] = sevPortRef(title + "-in")
		body["output_port_schema"] = sevPortRef(title + "-out")
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "step_guidance",
		"title":          title,
		"description":    "step guidance body under test",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{},
		"body":           body,
	}
}

func sevCompositeEnvelope(title string, children ...contract.SkillArtifactRef) map[string]any {
	childDocs := make([]any, 0, len(children))
	for i, ref := range children {
		childDocs = append(childDocs, map[string]any{
			"child_id":    fmt.Sprintf("c%d", i),
			"skill_ref":   sevSkillRefDoc(ref),
			"input_port":  "in",
			"output_port": "out",
		})
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "composite",
		"title":          title,
		"description":    "composite body under test",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{},
		"body": map[string]any{
			"children":                  childDocs,
			"edges":                     []any{},
			"retry_policies":            []any{},
			"failure_handlers":          []any{},
			"orchestration_permissions": []any{},
			"input_port_schema":         sevPortRef(title + "-in"),
			"output_port_schema":        sevPortRef(title + "-out"),
		},
	}
}

func sevSkillRefDoc(ref contract.SkillArtifactRef) map[string]any {
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
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         version,
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

// sevRootDoc builds a raw root object for the wire request.
func sevRootDoc(lineage string, version any, kind, digest string) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaSkillArtifactRef,
		"lineage_id":      lineage,
		"version":         version,
		"kind":            kind,
		"artifact_digest": digest,
	}
}

// ---------------------------------------------------------------------------
// Activation plumbing (full protected release: proposal -> candidate ->
// accepted decision -> Activate with ExpectedHead)
// ---------------------------------------------------------------------------

var sevActivationPath = [][2]string{
	{"none", "proposed"},
	{"proposed", "admitted"},
	{"admitted", "candidate_bound"},
	{"candidate_bound", "validating"},
	{"validating", "replay_pending"},
	{"replay_pending", "replaying"},
	{"replaying", "decision_pending"},
	{"decision_pending", "activation_pending"},
}

func (w *sevWorld) proposalDoc(t *testing.T, proposalID, kind string) map[string]any {
	t.Helper()
	doc := map[string]any{
		"schema_version":   "gms.skill-proposal.v1",
		"proposal_id":      proposalID,
		"proposal_version": sevJN(1),
		"proposed_kind":    kind,
		"source_segment_refs": []any{map[string]any{
			"schema_version":  "host.segment-ref.v1",
			"room_id":         "room-1",
			"segment_id":      "seg-" + proposalID,
			"segment_version": sevJN(1),
			"segment_digest":  contract.DigestBytes([]byte("seg-" + proposalID)),
			"evidence_seal_ref": map[string]any{
				"id": "evseal-" + proposalID, "version": sevJN(1), "digest": contract.DigestBytes([]byte("evseal-" + proposalID)),
			},
			"path_seal_ref": map[string]any{
				"id": "pathseal-" + proposalID, "version": sevJN(1), "digest": contract.DigestBytes([]byte("pathseal-" + proposalID)),
			},
		}},
		"evidence_refs": []any{map[string]any{
			"schema_version":  contract.SchemaEvidenceRef,
			"evidence_id":     "ev-" + proposalID,
			"version":         sevJN(1),
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
			"id": "policy-static-gates", "version": sevJN(1), "digest": contract.DigestBytes([]byte("policy-static-gates")),
		}},
	}
	digest, err := w.gates.ComputeDigestPreimage(doc, "skill-proposal.schema.json")
	if err != nil {
		t.Fatalf("proposal digest preimage: %v", err)
	}
	doc["proposal_digest"] = digest
	return doc
}

type sevCandidateView struct {
	ref  contract.CandidateArtifactRef
	body []byte
}

func (v *sevCandidateView) Ref() contract.CandidateArtifactRef { return v.ref }
func (v *sevCandidateView) BodyDigest() string                 { return v.ref.BodyDigest }
func (v *sevCandidateView) CanonicalBody() []byte {
	out := make([]byte, len(v.body))
	copy(out, v.body)
	return out
}

type sevDecision struct{ doc map[string]any }

func (d sevDecision) Doc() map[string]any { return d.doc }
func (d sevDecision) Outcome() string     { s, _ := contract.AsString(d.doc["outcome"]); return s }
func (d sevDecision) DecisionDigest() string {
	s, _ := contract.AsString(d.doc["decision_digest"])
	return s
}

func (w *sevWorld) decisionDoc(t *testing.T, cand contract.CandidateArtifactRef) map[string]any {
	t.Helper()
	vref := func(id string) map[string]any {
		return map[string]any{"id": id, "version": sevJN(1), "digest": contract.DigestBytes([]byte(id))}
	}
	doc := map[string]any{
		"schema_version":   "gms.release-decision.v1",
		"decision_id":      "dec-" + cand.CandidateID,
		"decision_version": sevJN(1),
		"candidate_ref": map[string]any{
			"schema_version": cand.SchemaVersion,
			"candidate_id":   cand.CandidateID,
			"kind":           cand.Kind,
			"body_digest":    cand.BodyDigest,
			"origin_type":    cand.OriginType,
			"origin_ref":     map[string]any{"id": cand.OriginRef.ID, "version": sevJN(1), "digest": cand.OriginRef.Digest},
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
	return doc
}

// activateEnvelope runs the full protected release and returns the new head.
func (w *sevWorld) activateEnvelope(t *testing.T, lineageID, proposalID string, envelope map[string]any) contract.SkillArtifactRef {
	t.Helper()
	ctx := context.Background()
	canonical, err := w.artifacts.Canonicalize(envelope)
	if err != nil {
		t.Fatalf("canonicalize %s envelope: %v", envelope["kind"], err)
	}
	body, digest := canonical.Canonical(), canonical.BodyDigest()
	propDoc := w.proposalDoc(t, proposalID, envelope["kind"].(string))
	for _, edge := range sevActivationPath {
		if _, err := w.proposals.AppendTransition(ctx, propDoc, edge[0], edge[1]); err != nil {
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
	w.commitCandidate(t, cand, body)
	active, err := w.activate.ActiveRevision(ctx, lineageID)
	if err != nil {
		t.Fatalf("active head before activation: %v", err)
	}
	result, err := w.activate.Activate(ctx, activation.ActivateRequest{
		Decision:     sevDecision{doc: w.decisionDoc(t, cand)},
		Candidate:    &sevCandidateView{ref: cand, body: body},
		LineageID:    lineageID,
		ExpectedHead: active,
		ProposalDoc:  propDoc,
	})
	if err != nil {
		t.Fatalf("activate %s: %v", lineageID, err)
	}
	return result.ReleasedRef
}

func (w *sevWorld) commitCandidate(t *testing.T, cand contract.CandidateArtifactRef, body []byte) {
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
		"origin_ref":     map[string]any{"id": cand.OriginRef.ID, "version": sevJN(1), "digest": cand.OriginRef.Digest},
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

// sevTornContent wraps the content store and commits a racing activation
// right after the walker's first content fetch (the deterministic tear).
type sevTornContent struct {
	ledger.ContentStore
	onFirstGet func()
	once       sync.Once
}

func (c *sevTornContent) Get(digest string) ([]byte, bool, error) {
	c.once.Do(c.onFirstGet)
	return c.ContentStore.Get(digest)
}

// ---------------------------------------------------------------------------
// The contract test
// ---------------------------------------------------------------------------

func TestSkillEvolutionClosureContract(t *testing.T) {
	world := newSkillEvolutionWorld(t)
	server := newClosureServer(t, world.service(t, world.store))

	procRef := world.activateEnvelope(t, "sev-proc", "sev-prop-proc", sevProcedureEnvelope("Diff summarizer", true))
	sgRef := world.activateEnvelope(t, "sev-sg", "sev-prop-sg", sevGuidanceEnvelope("Commit checklist", true))
	compRef := world.activateEnvelope(t, "sev-comp", "sev-prop-comp", sevCompositeEnvelope("Review orchestrator", procRef, sgRef))

	closureRequest := func(requestID string, root map[string]any) (int, http.Header, []byte) {
		t.Helper()
		return contractJSONRequest(t, server, http.MethodPost, "/v1/skill-evolution/closure:read", map[string]any{
			"request_id":      requestID,
			"root_skill_refs": []any{root},
		})
	}

	t.Run("200 exact closure with deterministic shape", func(t *testing.T) {
		status, header, payload := closureRequest("req-closure-1", sevSkillRefDoc(compRef))
		contractRequireStatus(t, status, http.StatusOK, payload)
		contractRequireJSON(t, header)
		response := contractDecodeObject(t, payload)
		contractRequireExactKeys(t, response,
			"schema_version", "request_id", "root_skill_refs", "closure_nodes",
			"activation_sequence", "activation_head_digest", "torn_read_token",
			"projection_watermark")
		if response["schema_version"] != "gms.materialization-closure-read.v1" {
			t.Errorf("schema_version = %#v", response["schema_version"])
		}
		if response["request_id"] != "req-closure-1" {
			t.Errorf("request_id = %#v, want echo", response["request_id"])
		}
		if seq, ok := response["activation_sequence"].(json.Number); !ok || seq.String() != "3" {
			t.Errorf("activation_sequence = %#v, want 3 (three activations)", response["activation_sequence"])
		}
		token, _ := response["torn_read_token"].(string)
		if !strings.HasPrefix(token, "sha256:") || len(token) != len("sha256:")+64 {
			t.Errorf("torn_read_token = %q, want sha256 digest", token)
		}
		headDigest, _ := response["activation_head_digest"].(string)
		if !strings.HasPrefix(headDigest, "sha256:") {
			t.Errorf("activation_head_digest = %q", headDigest)
		}
		// Informational watermark rides along; it never gates the read.
		watermark, ok := response["projection_watermark"].(map[string]any)
		if !ok || watermark["state"] != "catching_up" {
			t.Errorf("projection_watermark = %#v, want catching_up information", response["projection_watermark"])
		}
		nodes, ok := response["closure_nodes"].([]any)
		if !ok || len(nodes) != 3 {
			t.Fatalf("closure_nodes = %#v, want 3 nodes", response["closure_nodes"])
		}
		wantOrder := []string{"sev-comp", "sev-proc", "sev-sg"}
		for i, want := range wantOrder {
			node, ok := nodes[i].(map[string]any)
			if !ok {
				t.Fatalf("closure_nodes[%d] is not an object", i)
			}
			contractRequireExactKeys(t, node,
				"skill_ref", "kind", "artifact_digest", "canonical_bytes_base64",
				"input_port_schema", "output_port_schema", "permissions", "dependency_refs")
			ref, ok := node["skill_ref"].(map[string]any)
			if !ok {
				t.Fatalf("closure_nodes[%d].skill_ref is not an object", i)
			}
			contractRequireExactKeys(t, ref, "schema_version", "lineage_id", "version", "kind", "artifact_digest")
			if ref["lineage_id"] != want {
				t.Errorf("closure_nodes[%d].lineage_id = %#v, want %q (canonical order)", i, ref["lineage_id"], want)
			}
			// The canonical bytes must digest exactly to the node's artifact_digest.
			encoded, _ := node["canonical_bytes_base64"].(string)
			raw, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				t.Fatalf("closure_nodes[%d] canonical bytes are not base64: %v", i, err)
			}
			if digest := contract.DigestBytes(raw); digest != node["artifact_digest"] {
				t.Errorf("closure_nodes[%d] bytes digest to %s, artifact_digest says %s", i, digest, node["artifact_digest"])
			}
			if _, ok := node["permissions"].([]any); !ok {
				t.Errorf("closure_nodes[%d].permissions = %#v, want array", i, node["permissions"])
			}
			if _, ok := node["dependency_refs"].([]any); !ok {
				t.Errorf("closure_nodes[%d].dependency_refs = %#v, want array", i, node["dependency_refs"])
			}
		}
		root := nodes[0].(map[string]any)
		deps := root["dependency_refs"].([]any)
		if len(deps) != 2 {
			t.Fatalf("root dependency_refs = %d, want the two composite children", len(deps))
		}
		if deps[0].(map[string]any)["lineage_id"] != "sev-proc" || deps[1].(map[string]any)["lineage_id"] != "sev-sg" {
			t.Errorf("root dependency_refs out of canonical order: %#v", deps)
		}
		procNode := nodes[1].(map[string]any)
		if len(procNode["permissions"].([]any)) != 1 {
			t.Errorf("procedure node permissions = %#v, want the envelope permission", procNode["permissions"])
		}
	})

	t.Run("same sequence same response", func(t *testing.T) {
		_, _, first := closureRequest("req-closure-2", sevSkillRefDoc(compRef))
		_, _, second := closureRequest("req-closure-3", sevSkillRefDoc(compRef))
		a, b := contractDecodeObject(t, first), contractDecodeObject(t, second)
		if a["torn_read_token"] != b["torn_read_token"] {
			t.Errorf("torn_read_token drifted within one activation sequence: %v vs %v", a["torn_read_token"], b["torn_read_token"])
		}
		if a["activation_sequence"] != b["activation_sequence"] || a["activation_head_digest"] != b["activation_head_digest"] {
			t.Errorf("freeze marks drifted within one activation sequence")
		}
		if fmt.Sprint(a["closure_nodes"]) != fmt.Sprint(b["closure_nodes"]) {
			t.Errorf("closure_nodes differ between same-sequence reads")
		}
	})

	t.Run("401 unauthorized", func(t *testing.T) {
		status, header, payload := contractRequest(t, server, http.MethodPost, "/v1/skill-evolution/closure:read", "", `{"request_id":"r"}`)
		contractRequireError(t, status, header, payload, http.StatusUnauthorized, "UNAUTHORIZED")
	})

	t.Run("400 invalid json", func(t *testing.T) {
		status, header, payload := contractRequest(t, server, http.MethodPost, "/v1/skill-evolution/closure:read", "Bearer "+contractToken, `{"request_id":`)
		contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_JSON")
	})

	t.Run("400 invalid request shape", func(t *testing.T) {
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/skill-evolution/closure:read", map[string]any{
			"request_id": "req-bad", "root_skill_refs": []any{}, "surprise": true,
		})
		contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
	})

	t.Run("422 non exact root ref", func(t *testing.T) {
		latest := sevRootDoc("sev-proc", "latest", "human_procedure", procRef.ArtifactDigest)
		status, header, payload := closureRequest("req-latest", latest)
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "NON_EXACT_REF")
	})

	t.Run("422 candidate root", func(t *testing.T) {
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/skill-evolution/closure:read", map[string]any{
			"request_id": "req-cand",
			"root_skill_refs": []any{map[string]any{
				"schema_version": contract.SchemaCandidateArtifactRef,
				"candidate_id":   "cand-sev-prop-proc", "kind": "human_procedure",
				"body_digest": procRef.ArtifactDigest, "origin_type": "skill_proposal",
				"origin_ref": map[string]any{"id": "sev-prop-proc", "version": sevJN(1), "digest": contract.DigestBytes([]byte("x"))},
			}},
		})
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "CANDIDATE_NOT_EXECUTABLE")
	})

	t.Run("404 no active head", func(t *testing.T) {
		ghost := sevRootDoc("sev-ghost", sevJN(1), "human_procedure", "sha256:"+strings.Repeat("ab", 32))
		status, header, payload := closureRequest("req-ghost", ghost)
		contractRequireError(t, status, header, payload, http.StatusNotFound, "NO_ACTIVE_HEAD")
	})

	t.Run("422 stale root is not the current active head", func(t *testing.T) {
		// Supersede sev-proc; the old exact ref is no longer the active head.
		world.activateEnvelope(t, "sev-proc", "sev-prop-proc-2", sevProcedureEnvelope("Diff summarizer v2", true))
		status, header, payload := closureRequest("req-stale", sevSkillRefDoc(procRef))
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "SKILL_NOT_CURRENT_ACTIVE")
	})

	t.Run("409 torn activation sequence", func(t *testing.T) {
		tornWorld := newSkillEvolutionWorld(t)
		leaf := tornWorld.activateEnvelope(t, "sev-torn", "sev-prop-torn-1", sevProcedureEnvelope("Torn leaf", true))
		tornContent := &sevTornContent{ContentStore: tornWorld.store}
		tornContent.onFirstGet = func() {
			tornWorld.activateEnvelope(tornWorld.t, "sev-late", "sev-prop-torn-2", sevProcedureEnvelope("Late activation", true))
		}
		tornServer := newClosureServer(t, tornWorld.service(t, tornContent))
		body, err := json.Marshal(map[string]any{
			"request_id":      "req-torn",
			"root_skill_refs": []any{sevSkillRefDoc(leaf)},
		})
		if err != nil {
			t.Fatalf("marshal torn request: %v", err)
		}
		request, err := http.NewRequest(http.MethodPost, tornServer.URL+"/v1/skill-evolution/closure:read", strings.NewReader(string(body)))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+contractToken)
		response, err := tornServer.Client().Do(request)
		if err != nil {
			t.Fatalf("perform torn request: %v", err)
		}
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read torn response: %v", err)
		}
		contractRequireError(t, response.StatusCode, response.Header, payload, http.StatusConflict, "ACTIVATION_SEQUENCE_CONFLICT")
		// No closure body is ever returned on a torn read.
		envelope := contractDecodeObject(t, payload)
		if _, exists := envelope["closure_nodes"]; exists {
			t.Errorf("torn read returned closure content; no guess is permitted")
		}
	})

	t.Run("404 unknown route", func(t *testing.T) {
		status, header, payload := contractJSONRequest(t, server, http.MethodGet, "/v1/skill-evolution/closure:read", map[string]any{})
		contractRequireError(t, status, header, payload, http.StatusNotFound, "NOT_FOUND")
	})

	t.Run("openapi matches the implementation", func(t *testing.T) {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("locate contract test source")
		}
		spec, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "openapi", "skill-evolution.yaml"))
		if err != nil {
			t.Fatalf("read openapi/skill-evolution.yaml: %v", err)
		}
		text := string(spec)
		routes := httpapi.SkillEvolutionRoutes()
		if len(routes) != 9 || routes[0].Method != http.MethodPost || routes[0].Path != "/v1/skill-evolution/closure:read" {
			t.Fatalf("SkillEvolutionRoutes() = %#v", routes)
		}
		for _, needed := range []string{
			"/v1/skill-evolution/closure:read:",
			"/v1/skill-evolution/interactions:record:",
			"/v1/skill-evolution/diagnoses:record:",
			"/v1/skill-evolution/advisory-candidates:read:",
			"/v1/skill-evolution/usage-summary:read:",
			"/v1/skill-evolution/candidate-outcomes:read:",
			"UsageSummaryReadRequest", "UsageSummaryReadResponse",
			"gms.usage-summary-read.v1", "interaction_only",
			"authority_tier_aggregates", "CandidateArtifactRef", "ContextProfile",
			"readMaterializationClosure",
			"gms.materialization-closure-read.v1",
			"torn_read_token", "activation_sequence", "activation_head_digest",
			"canonical_bytes_base64", "closure_nodes", "root_skill_refs",
			"dependency_refs", "projection_watermark", "max_nodes",
			"bearerAuth",
			"UNAUTHORIZED", "INVALID_REQUEST", "INVALID_JSON", "NOT_FOUND",
			"ACTIVATION_SEQUENCE_CONFLICT", "NON_EXACT_REF",
			"CANDIDATE_NOT_EXECUTABLE", "NO_ACTIVE_HEAD",
			"SKILL_NOT_CURRENT_ACTIVE", "DIGEST_MISMATCH", "REF_MISMATCH",
			"COMPOSITE_CHILD_NOT_ACTIVE", "DEPENDENCY_CYCLE",
			"PORT_SCHEMA_MISSING", "PERMISSION_CAP_EXCEEDED",
			"UNKNOWN_REQUIRED_EXTENSION", "BUDGET_EXCEEDED",
		} {
			if !strings.Contains(text, needed) {
				t.Errorf("openapi/skill-evolution.yaml is missing %q (implementation emits it)", needed)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// GMS-206 Memory tool transport contract test
// ---------------------------------------------------------------------------

// sevToolsRecords is a contiguous claim-assessment RecordSource.
type sevToolsRecords struct {
	records []projector.SourceRecord
}

func (s *sevToolsRecords) SourceName() string { return projector.SourceEvidenceAssessment }
func (s *sevToolsRecords) Records(after uint64) ([]projector.SourceRecord, error) {
	out := make([]projector.SourceRecord, 0, len(s.records))
	for _, record := range s.records {
		if record.Sequence > after {
			out = append(out, record)
		}
	}
	return out, nil
}

type sevToolsEvidence struct {
	refs map[string]contract.EvidenceRef
}

func (m *sevToolsEvidence) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	ref, ok := m.refs[id]
	return ref, ok, nil
}

// sevToolsWorld extends the closure world with the GMS-205 Runtime
// projector and the GMS-206 retrieval service over the same authorities.
type sevToolsWorld struct {
	*sevWorld
	projector   *projector.Service
	evidence    *sevToolsEvidence
	assessments *sevToolsRecords
	retrieval   *retrieval.Service
}

func newSkillEvolutionToolsWorld(t *testing.T, skillGetEnabled bool) *sevToolsWorld {
	t.Helper()
	base := newSkillEvolutionWorld(t)
	evidence := &sevToolsEvidence{refs: map[string]contract.EvidenceRef{}}
	assessments := &sevToolsRecords{}
	mgr, err := ledger.NewManager(base.store, base.registry)
	if err != nil {
		t.Fatalf("projector manager: %v", err)
	}
	projectorSvc, err := projector.NewService(projector.Config{
		Store: base.store, Tx: mgr, Registry: base.registry, Gates: base.gates,
		Assessments: assessments, Evidence: evidence,
	})
	if err != nil {
		t.Fatalf("projector.NewService: %v", err)
	}
	// base.stateDir is <conformance>/schema/state; the tool matrix policy
	// lives under <conformance>/policy.
	confDir := filepath.Dir(filepath.Dir(base.stateDir))
	toolPolicy, err := retrieval.LoadToolPolicy(filepath.Join(confDir, "policy", "tool-success-validation.v1.json"))
	if err != nil {
		t.Fatalf("retrieval.LoadToolPolicy: %v", err)
	}
	retrievalSvc, err := retrieval.NewService(retrieval.Config{
		Projection:      projectorSvc,
		Heads:           base.activate,
		Artifacts:       base.store,
		Gate:            base.artifacts,
		Evidence:        evidence,
		Gates:           base.gates,
		Registry:        base.registry,
		Policy:          toolPolicy,
		SkillGetEnabled: skillGetEnabled,
	})
	if err != nil {
		t.Fatalf("retrieval.NewService: %v", err)
	}
	return &sevToolsWorld{
		sevWorld: base, projector: projectorSvc, evidence: evidence,
		assessments: assessments, retrieval: retrievalSvc,
	}
}

func (w *sevToolsWorld) registerEvidence(id string) {
	w.t.Helper()
	ref, err := contract.ParseEvidenceRef(map[string]any{
		"schema_version":  contract.SchemaEvidenceRef,
		"evidence_id":     id,
		"version":         sevJN(1),
		"evidence_digest": contract.DigestBytes([]byte("evidence:" + id)),
		"commit_state":    "committed",
		"evidence_kind":   "observation",
	})
	if err != nil {
		w.t.Fatalf("register evidence %s: %v", id, err)
	}
	w.evidence.refs[id] = ref
}

func (w *sevToolsWorld) project() {
	w.t.Helper()
	if _, err := w.projector.ProjectRuntime(context.Background()); err != nil {
		w.t.Fatalf("ProjectRuntime: %v", err)
	}
}

func newToolsServer(t *testing.T, world *sevToolsWorld) *httptest.Server {
	t.Helper()
	// The closure reader over the same store satisfies the handler's GMS-207
	// dependency; the tool routes are the surface under test here.
	reader, err := materializationread.NewService(materializationread.Config{
		Heads: world.activate, Artifacts: world.store, Gate: world.artifacts,
		Activations: world.store, Registry: world.registry, Watermark: world.watermark,
	})
	if err != nil {
		t.Fatalf("materializationread.NewService: %v", err)
	}
	handler := httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{
		Token: contractToken, Reader: reader, Retrieval: world.retrieval,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

var (
	sevToolScope = map[string]any{
		"id":      "scope-memory-room-tools",
		"version": sevJN(1),
		"digest":  contract.DigestBytes([]byte("scope-memory-room-tools")),
	}
	sevToolRanker = map[string]any{
		"id":      "gms.ranker.lexical-graph.v1",
		"version": sevJN(1),
		"digest":  "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045c7c9e6ece643805a4c79bb",
	}
	sevToolRenderProfile = map[string]any{
		"id":      "rsih.render-profile.default.v1",
		"version": sevJN(2),
		"digest":  "sha256:9b3794ee3e2f935abc7ab8291ab89310e04898315e992154c7e320bdd124de72",
	}
	sevToolGuidancePolicy = map[string]any{
		"id":      "gms.guidance-policy.default.v1",
		"version": sevJN(1),
		"digest":  "sha256:e8c0891959144f05f533813f43f8b5760726f4a635bdfc3d46230294be08403f",
	}
)

// toolProxyRequest builds one §7.17 wire envelope.
func toolProxyRequest(tool, session string, arguments map[string]any, extra map[string]any) map[string]any {
	request := map[string]any{
		"schema_version":    httpapi.ToolProxyRequestSchemaVersion,
		"proxy_request_id":  "pr-tools-" + session + "-" + tool,
		"room_id":           "room-tools-1",
		"agent_id":          "agent-tools-1",
		"delivery_id":       "delivery-tools-" + session,
		"tool_name":         tool,
		"arguments":         arguments,
		"scope_profile_ref": sevToolScope,
		"idempotency_key":   contract.DigestBytes([]byte(session + tool)),
		"timeout_millis":    sevJN(5000),
	}
	for key, value := range extra {
		request[key] = value
	}
	return request
}

func toolExploreArguments(session, query string, total, evidenceCap, skillCap int64) map[string]any {
	return map[string]any{
		"schema_version":       "gms.memory-explore-arguments.v1",
		"explore_session_id":   session,
		"query_text":           query,
		"runtime_context_hash": contract.DigestBytes([]byte("runtime-context-tools")),
		"ranker_policy_ref":    sevToolRanker,
		"budgets": map[string]any{
			"total_cap":             sevJN(total),
			"evidence_subcap":       sevJN(evidenceCap),
			"skill_subcap":          sevJN(skillCap),
			"guidance_token_budget": sevJN(10000),
		},
		"filters": map[string]any{},
	}
}

func toolSkillGetArguments(ref map[string]any, extra map[string]any) map[string]any {
	args := map[string]any{
		"schema_version":        "gms.skill-get-arguments.v1",
		"skill_ref":             ref,
		"runtime_context_hash":  contract.DigestBytes([]byte("runtime-context-tools")),
		"render_profile_ref":    sevToolRenderProfile,
		"policy_ref":            sevToolGuidancePolicy,
		"guidance_token_budget": sevJN(10000),
		"visibility":            "current_active",
	}
	for key, value := range extra {
		args[key] = value
	}
	return args
}

// contractRequireIntegerNumbers walks a decoded response value and fails on
// any non-integer JSON number (Contract §6.3 integer-only hashed core).
func contractRequireIntegerNumbers(t *testing.T, where string, value any) {
	t.Helper()
	switch v := value.(type) {
	case json.Number:
		if !contract.IsIntegerNumber(v) {
			t.Errorf("%s carries the non-integer number %s", where, v.String())
		}
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			contractRequireIntegerNumbers(t, where+"."+key, v[key])
		}
	case []any:
		for i, item := range v {
			contractRequireIntegerNumbers(t, fmt.Sprintf("%s[%d]", where, i), item)
		}
	}
}

func TestSkillEvolutionRetrievalContract(t *testing.T) {
	world := newSkillEvolutionToolsWorld(t, true)
	server := newToolsServer(t, world)

	world.registerEvidence("ev-tool-guidance")
	procRef := world.activateEnvelope(t, "tool-proc", "tool-prop-proc", sevProcedureEnvelope("Diff summarizer", true))
	sgRef := world.activateEnvelope(t, "tool-sg", "tool-prop-sg", sevGuidanceEnvelope("Diff checklist", true))
	world.project()

	const explorePath = "/v1/skill-evolution/tools/memory_explore"

	t.Run("200 explore exact upstream payload with read audit", func(t *testing.T) {
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, explorePath,
			toolProxyRequest("memory_explore", "sess-1", toolExploreArguments("sess-1", "diff summarizer", 10, 5, 5), nil))
		contractRequireStatus(t, status, http.StatusOK, payload)
		contractRequireJSON(t, header)
		response := contractDecodeObject(t, payload)
		contractRequireExactKeys(t, response,
			"schema_version", "proxy_request_id", "tool_name", "status",
			"result", "read_audit", "upstream_result_digest")
		if response["schema_version"] != httpapi.ToolUpstreamResponseSchemaVersion {
			t.Errorf("schema_version = %#v", response["schema_version"])
		}
		if response["tool_name"] != "memory_explore" || response["status"] != "success" {
			t.Errorf("tool/status = %v/%v", response["tool_name"], response["status"])
		}
		contractRequireIntegerNumbers(t, "response", response)
		result, ok := response["result"].(map[string]any)
		if !ok || result["schema_version"] != "gms.explore-result.v1" {
			t.Fatalf("result = %#v, want gms.explore-result.v1", response["result"])
		}
		// The upstream digest is the canonical digest of the exact result.
		digest, err := contract.DigestOf(result)
		if err != nil {
			t.Fatalf("result digest: %v", err)
		}
		if response["upstream_result_digest"] != digest {
			t.Errorf("upstream_result_digest = %v, recomputed %s", response["upstream_result_digest"], digest)
		}
		audit, ok := response["read_audit"].(map[string]any)
		if !ok || audit["schema_version"] != "host.read-audit.v1" {
			t.Fatalf("read_audit = %#v", response["read_audit"])
		}
		if audit["room_id"] != "room-tools-1" || audit["agent_id"] != "agent-tools-1" {
			t.Errorf("read audit scope = %#v", audit)
		}
		if _, ok := audit["watermark_projection_sequence"]; !ok {
			t.Errorf("read audit misses watermark_projection_sequence: %#v", audit)
		}
		skills, ok := result["skill_results"].([]any)
		if !ok || len(skills) != 2 {
			t.Fatalf("skill_results = %#v, want both active lineages", result["skill_results"])
		}
		// Deterministic order over the wire: the exact lexical hit ranks top.
		top := skills[0].(map[string]any)["skill_ref"].(map[string]any)
		if top["lineage_id"] != procRef.LineageID {
			t.Errorf("top ranked lineage = %#v, want the lexical hit %q", top["lineage_id"], procRef.LineageID)
		}
		if _, ok := result["omissions"].([]any); !ok {
			t.Errorf("omissions carrier missing: %#v", result["omissions"])
		}
	})

	t.Run("200 skill_get closed guidance view", func(t *testing.T) {
		status, header, payload := contractJSONRequest(t, server, http.MethodPost,
			"/v1/skill-evolution/tools/skill_get",
			toolProxyRequest("skill_get", "sess-get", toolSkillGetArguments(sevSkillRefDoc(sgRef), nil), nil))
		contractRequireStatus(t, status, http.StatusOK, payload)
		contractRequireJSON(t, header)
		response := contractDecodeObject(t, payload)
		contractRequireIntegerNumbers(t, "response", response)
		view, ok := response["result"].(map[string]any)
		if !ok || view["schema_version"] != "gms.guidance-view.v1" {
			t.Fatalf("result = %#v, want gms.guidance-view.v1", response["result"])
		}
		contractRequireExactKeys(t, view,
			"schema_version", "source_skill_ref", "render_profile_ref", "policy_ref",
			"runtime_context_hash", "included_branch_refs", "omitted_branch_refs",
			"expandable_refs", "content", "content_token_count", "truncated", "view_hash")
		audit := response["read_audit"].(map[string]any)
		if _, ok := audit["active_head_activation_sequence"]; !ok {
			t.Errorf("read audit misses active_head_activation_sequence: %#v", audit)
		}
		digest, err := contract.DigestOf(view)
		if err != nil {
			t.Fatalf("view digest: %v", err)
		}
		if response["upstream_result_digest"] != digest {
			t.Errorf("upstream_result_digest = %v, recomputed %s", response["upstream_result_digest"], digest)
		}
	})

	t.Run("401 unauthorized", func(t *testing.T) {
		status, header, payload := contractRequest(t, server, http.MethodPost, explorePath, "", `{"proxy_request_id":"r"}`)
		contractRequireError(t, status, header, payload, http.StatusUnauthorized, "UNAUTHORIZED")
	})

	t.Run("400 invalid json", func(t *testing.T) {
		status, header, payload := contractRequest(t, server, http.MethodPost, explorePath,
			"Bearer "+contractToken, `{"proxy_request_id":`)
		contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_JSON")
	})

	t.Run("400 invalid request envelope", func(t *testing.T) {
		request := toolProxyRequest("memory_explore", "sess-bad", toolExploreArguments("sess-bad", "diff", 10, 5, 5), nil)
		request["surprise"] = true
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, explorePath, request)
		contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
	})

	t.Run("400 wrong envelope schema version", func(t *testing.T) {
		request := toolProxyRequest("memory_explore", "sess-sv", toolExploreArguments("sess-sv", "diff", 10, 5, 5), nil)
		request["schema_version"] = "host.tool-proxy-request.v0"
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, explorePath, request)
		contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
	})

	t.Run("422 unknown tool name", func(t *testing.T) {
		status, header, payload := contractJSONRequest(t, server, http.MethodPost,
			"/v1/skill-evolution/tools/memory_explore",
			toolProxyRequest("memory_delete", "sess-tool", toolExploreArguments("sess-tool", "diff", 10, 5, 5), nil))
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "TOOL_UNSUPPORTED")
	})

	t.Run("422 cross room session reuse", func(t *testing.T) {
		status, _, _ := contractJSONRequest(t, server, http.MethodPost, explorePath,
			toolProxyRequest("memory_explore", "sess-scope", toolExploreArguments("sess-scope", "diff", 10, 5, 5), nil))
		if status != http.StatusOK {
			t.Fatalf("seed session status = %d, want 200", status)
		}
		request := toolProxyRequest("memory_explore", "sess-scope", toolExploreArguments("sess-scope", "checklist", 10, 5, 5), nil)
		request["room_id"] = "room-tools-other"
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, explorePath, request)
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "EXPLORE_SCOPE_VIOLATION")
	})

	t.Run("422 budgets violate the caps", func(t *testing.T) {
		request := toolProxyRequest("memory_explore", "sess-budget", toolExploreArguments("sess-budget", "diff", 1, 2, 1), nil)
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, explorePath, request)
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "BUDGET_INVALID")
	})

	t.Run("503 freshness behind the requested sequence", func(t *testing.T) {
		request := toolProxyRequest("memory_explore", "sess-fresh", toolExploreArguments("sess-fresh", "diff", 10, 5, 5),
			map[string]any{"requested_min_activation_sequence": sevJN(99999)})
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, explorePath, request)
		contractRequireError(t, status, header, payload, http.StatusServiceUnavailable, "PROJECTION_BEHIND_REQUIRED_SEQUENCE")
	})

	t.Run("422 skill_get historical exact unauthorized and stale profile", func(t *testing.T) {
		request := toolProxyRequest("skill_get", "sess-hist",
			toolSkillGetArguments(sevSkillRefDoc(sgRef), map[string]any{"visibility": "historical_exact"}), nil)
		status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/skill-evolution/tools/skill_get", request)
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "HISTORICAL_READ_NOT_AUTHORIZED")

		stale := toolProxyRequest("skill_get", "sess-stale",
			toolSkillGetArguments(sevSkillRefDoc(sgRef), map[string]any{
				"render_profile_ref": map[string]any{
					"id": "rsih.render-profile.default.v1", "version": sevJN(1),
					"digest": sevToolRenderProfile["digest"],
				},
			}), nil)
		status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/skill-evolution/tools/skill_get", stale)
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "SCHEMA_VERSION_UNSUPPORTED")
	})

	t.Run("404 unknown route", func(t *testing.T) {
		status, header, payload := contractJSONRequest(t, server, http.MethodPost,
			"/v1/skill-evolution/tools/memory_forget", map[string]any{})
		contractRequireError(t, status, header, payload, http.StatusNotFound, "NOT_FOUND")
	})

	t.Run("422 skill_get readiness gate closed", func(t *testing.T) {
		gated := newSkillEvolutionToolsWorld(t, false)
		gatedServer := newToolsServer(t, gated)
		status, header, payload := contractJSONRequest(t, gatedServer, http.MethodPost,
			"/v1/skill-evolution/tools/skill_get",
			toolProxyRequest("skill_get", "sess-gated", toolSkillGetArguments(sevSkillRefDoc(sgRef), nil), nil))
		contractRequireError(t, status, header, payload, http.StatusUnprocessableEntity, "TOOL_UNSUPPORTED")
	})

	t.Run("openapi matches the tool implementation", func(t *testing.T) {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("locate contract test source")
		}
		spec, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "openapi", "skill-evolution.yaml"))
		if err != nil {
			t.Fatalf("read openapi/skill-evolution.yaml: %v", err)
		}
		text := string(spec)
		for _, needed := range []string{
			"/v1/skill-evolution/tools/memory_explore:",
			"/v1/skill-evolution/tools/memory_expand:",
			"/v1/skill-evolution/tools/skill_get:",
			"memoryExploreTool", "memoryExpandTool", "skillGetTool",
			"host.tool-proxy-request.v1",
			"gms.tool-upstream-response.v1",
			"gms.explore-result.v1",
			"gms.guidance-view.v1",
			"upstream_result_digest", "read_audit",
			"explore_session_id", "query_digest", "ranker_policy_ref",
			"evidence_results", "skill_results", "guidance_view",
			"artifact_identity_citation", "evidence_citations", "rank_score_micros",
			"served_fences", "evidence_fence_digest", "skill_fence_digest",
			"truncation_reason_codes", "omissions", "total_used",
			"watermark_projection_sequence", "active_head_activation_sequence",
			"source_skill_ref", "render_profile_ref", "included_branch_refs",
			"omitted_branch_refs", "expandable_refs", "content_token_count", "view_hash",
			"TOOL_UNSUPPORTED", "BUDGET_INVALID", "EXPLORE_SCOPE_VIOLATION",
			"EXPLORE_FENCE_CONFLICT", "EXPLORE_CONTINUATION_INVALID",
			"EXPLORE_SESSION_INVALID", "EXPAND_DEPTH_UNSUPPORTED",
			"SKILL_NOT_CURRENT_ACTIVE", "HISTORICAL_READ_NOT_AUTHORIZED",
			"PROJECTION_BEHIND_REQUIRED_SEQUENCE", "NON_EXACT_REF",
			"CANDIDATE_NOT_EXECUTABLE", "NO_ACTIVE_HEAD",
		} {
			if !strings.Contains(text, needed) {
				t.Errorf("openapi/skill-evolution.yaml is missing %q (implementation emits it)", needed)
			}
		}
	})
}

type sevAdvisoryReader struct{ candidates []candidate.AdvisoryCandidate }

func (r sevAdvisoryReader) ListAdvisory(context.Context) ([]candidate.AdvisoryCandidate, error) {
	return r.candidates, nil
}

func TestSkillEvolutionArmBTransport(t *testing.T) {
	world := newSkillEvolutionWorld(t)
	usageStore := usageprojection.NewMemoryStore()
	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageStore)
	if err != nil {
		t.Fatalf("usage service: %v", err)
	}
	advisory := sevAdvisoryReader{candidates: []candidate.AdvisoryCandidate{
		{CandidateID: "candidate-procedure", Kind: "human_procedure", Guidance: "Inspect the smallest failing input."},
		{CandidateID: "candidate-tool", Kind: "tool", Guidance: "must not leak"},
	}}
	handler := httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{Token: contractToken, Reader: world.service(t, world.store), Usage: usage, Advisory: advisory, WriterIdentity: "server-diagnostician"})
	server := httptest.NewServer(handler)
	defer server.Close()

	unauth, err := http.NewRequest(http.MethodPost, server.URL+httpapi.SkillEvolutionInteractionRecordPath, strings.NewReader(`{"request_id":"r"}`))
	if err != nil {
		t.Fatalf("new unauth request: %v", err)
	}
	unauth.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(unauth)
	if err != nil {
		t.Fatalf("do unauth request: %v", err)
	}
	payload, _ := io.ReadAll(response.Body)
	response.Body.Close()
	contractRequireError(t, response.StatusCode, response.Header, payload, http.StatusUnauthorized, "UNAUTHORIZED")

	profile := map[string]any{"schema_version": "context-profile/1.0", "task_family": "code_implementation", "runtime_class": "go-1.26-linux", "workspace_feature_tags": []any{}, "observable_guard_facts": []any{}, "tool_policy_ref": sevVersionedRef("tool-policy"), "environment_class": "sandboxed-linux"}
	subject := map[string]any{"skill_ref": sevSkillRefDoc(world.activateEnvelope(t, "armb-skill", "armb-prop", sevProcedureEnvelope("Advice", true)))}
	status, _, payload := contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionInteractionRecordPath, map[string]any{"request_id": "interaction-1", "subject": subject, "context_profile": profile, "stage": "adopted", "evidence_refs": []any{sevEvidenceRef("armb-evidence")}})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	interactionResponse := contractDecodeObject(t, payload)
	if interactionResponse["non_authoritative"] != true || interactionResponse["writer_identity"] != "server-diagnostician" {
		t.Fatalf("interaction response=%#v", interactionResponse)
	}

	status, _, payload = contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionDiagnosisRecordPath, map[string]any{"request_id": "diagnosis-1", "assessment_id": "assessment-1", "subject": subject, "context_profile": profile, "returned_path_id": "path-1", "addressed_agent_id": "agent-primary", "contribution_score_micros": json.Number("900000"), "confidence_micros": json.Number("800000"), "rationale": "observational only", "evidence_refs": []any{sevEvidenceRef("armb-diagnosis")}, "rubric_ref": sevVersionedRef("rubric"), "evaluation_batch_id": "held-out-1"})
	contractRequireStatus(t, status, http.StatusCreated, payload)

	status, _, payload = contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionAdvisoryReadPath, map[string]any{"request_id": "advisory-1", "context_profile": profile})
	contractRequireStatus(t, status, http.StatusOK, payload)
	read := contractDecodeObject(t, payload)
	if read["non_authoritative"] != true {
		t.Fatalf("advisory response=%#v", read)
	}
	candidates, _ := read["candidates"].([]any)
	if len(candidates) != 1 || candidates[0].(map[string]any)["candidate_id"] != "candidate-procedure" {
		t.Fatalf("advisory candidates=%#v", candidates)
	}
	summary := read["usage_summary"].(map[string]any)
	entries, _ := summary["entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("diagnostic record leaked into default summary: %#v", summary)
	}
}

func sevVersionedRef(id string) map[string]any {
	return map[string]any{"id": id, "version": sevJN(1), "digest": contract.DigestBytes([]byte(id))}
}
