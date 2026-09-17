package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/skillevolution/batchconsolidation"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationfreeze"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
)

const graphBatchToken = "graph-batch-test-token"

type graphBatchWorld struct {
	t           *testing.T
	handler     http.Handler
	proposals   *rawproposal.Service
	consolidate *batchconsolidation.Service
	freeze      *evaluationfreeze.Service
	source      *rawproposal.MemoryTrajectorySource
}

func newGraphBatchWorld(t *testing.T) *graphBatchWorld {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("conformance dir: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("reason policy: %v", err)
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("memory ledger: %v", err)
	}
	manager, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("transaction manager: %v", err)
	}
	source := rawproposal.NewMemoryTrajectorySource()
	proposals, err := rawproposal.NewService(source, manager)
	if err != nil {
		t.Fatalf("raw proposal service: %v", err)
	}
	consolidate, err := batchconsolidation.NewService(proposals, store, manager)
	if err != nil {
		t.Fatalf("consolidation service: %v", err)
	}
	freeze, err := evaluationfreeze.NewService(proposals, store)
	if err != nil {
		t.Fatalf("freeze service: %v", err)
	}
	handler := httpapi.NewGraphBatchHandler(httpapi.GraphBatchDependencies{
		Token: graphBatchToken, Proposals: proposals, Consolidate: consolidate, Freeze: freeze, Source: source,
	})
	return &graphBatchWorld{
		t: t, handler: handler, proposals: proposals, consolidate: consolidate, freeze: freeze, source: source,
	}
}

func TestGraphBatchRoutesAreIndependentOfClosedV1ToolSet(t *testing.T) {
	routes := httpapi.GraphBatchRoutes()
	if len(routes) != 4 {
		t.Fatalf("GraphBatchRoutes() = %#v, want 4 POST routes", routes)
	}
	want := map[string]bool{
		http.MethodPost + " " + httpapi.GraphBatchSkillProposePath:         true,
		http.MethodPost + " " + httpapi.GraphBatchSkillConsolidatePath:     true,
		http.MethodPost + " " + httpapi.GraphBatchEvaluationFreezePath:     true,
		http.MethodPost + " " + httpapi.GraphBatchTrajectoriesRegisterPath: true,
	}
	for _, route := range routes {
		if route.Method != http.MethodPost {
			t.Fatalf("GraphBatchRoutes includes non-POST %s %s", route.Method, route.Path)
		}
		key := route.Method + " " + route.Path
		if !want[key] {
			t.Fatalf("unexpected GraphBatch route %s", key)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("GraphBatchRoutes missing %v", want)
	}
	for _, route := range httpapi.SkillEvolutionRoutes() {
		if route.Path == httpapi.GraphBatchSkillProposePath || route.Path == httpapi.GraphBatchSkillConsolidatePath {
			t.Fatalf("SkillEvolutionRoutes() must not include graph batch path %s", route.Path)
		}
	}
	if got := len(httpapi.SkillEvolutionRoutes()); got != 9 {
		t.Fatalf("SkillEvolutionRoutes() length = %d, want 9 (closed v1 tool set unchanged)", got)
	}
}

func TestGraphBatchProposeThenGetReadsProvenance(t *testing.T) {
	w := newGraphBatchWorld(t)
	w.registerFixtureTrajectory()
	body := validProposalBody(t, "raw-proposal-http-aaaaaaaaaa01")
	status, payload := w.post(httpapi.GraphBatchSkillProposePath, map[string]any{
		"request_id": "req-propose-1", "idempotency_key": "idem-propose-1",
		"diagnosis_run_id": "diagnosis-run-01", "trajectory_id": "trajectory-01", "body": body,
	})
	if status != http.StatusOK {
		t.Fatalf("propose status = %d payload=%s", status, payload)
	}
	var got struct {
		ProposalID    string `json:"proposal_id"`
		ContentDigest string `json:"content_digest"`
		URI           string `json:"uri"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("decode propose: %v", err)
	}
	if got.ProposalID != "raw-proposal-http-aaaaaaaaaa01" || got.ContentDigest == "" || got.URI == "" {
		t.Fatalf("propose response = %#v", got)
	}
	proposal, ok := w.proposals.Get(got.ProposalID)
	if !ok || proposal.ContentDigest != got.ContentDigest {
		t.Fatalf("Get(%q) = %#v ok=%v", got.ProposalID, proposal, ok)
	}
	provenance, ok := w.proposals.ProvenanceForProposal(got.ProposalID)
	if !ok {
		t.Fatal("provenance missing after propose")
	}
	if provenance.SourceTrajectoryID != "trajectory-01" || provenance.SourceSnapshotID != "snapshot-01" {
		t.Fatalf("provenance trajectory/snapshot = %#v", provenance)
	}
	if provenance.SourceCheckpoint.ID != "checkpoint-01" || len(provenance.SourceEvidence) != 1 || provenance.SourceEvidence[0].ID != "evidence-01" {
		t.Fatalf("provenance checkpoint/evidence = %#v", provenance)
	}
	if provenance.ProposalRef.ProposalID != got.ProposalID || provenance.ProposalRef.ContentDigest != got.ContentDigest {
		t.Fatalf("provenance ref = %#v", provenance.ProposalRef)
	}
}

func TestGraphBatchConsolidateCAS(t *testing.T) {
	w := newGraphBatchWorld(t)
	w.registerFixtureTrajectory()
	id := w.propose("raw-proposal-http-cas-aaaaaaa01", "idem-cas-propose")
	firstStatus, firstPayload := w.post(httpapi.GraphBatchSkillConsolidatePath, map[string]any{
		"request_id": "req-cas-1", "idempotency_key": "idem-cas-1",
		"agent_run_id": "consolidator-01", "expected_ledger_revision": 0,
		"family_proposal_ids": []string{id},
	})
	if firstStatus != http.StatusOK {
		t.Fatalf("first consolidate status = %d payload=%s", firstStatus, firstPayload)
	}
	var first struct {
		LedgerRevision json.Number `json:"ledger_revision"`
		LedgerDigest   string      `json:"ledger_digest"`
		Decisions      []struct {
			DecisionID        string   `json:"decision_id"`
			Operation         string   `json:"operation"`
			SourceProposalIDs []string `json:"source_proposal_ids"`
		} `json:"decisions"`
	}
	if err := json.Unmarshal(firstPayload, &first); err != nil {
		t.Fatalf("decode consolidate: %v", err)
	}
	if first.LedgerRevision.String() != "1" || first.LedgerDigest == "" || len(first.Decisions) == 0 {
		t.Fatalf("first consolidate = %#v", first)
	}
	if first.Decisions[0].DecisionID == "" || first.Decisions[0].Operation == "" || len(first.Decisions[0].SourceProposalIDs) != 1 {
		t.Fatalf("decision = %#v", first.Decisions[0])
	}
	staleStatus, stalePayload := w.post(httpapi.GraphBatchSkillConsolidatePath, map[string]any{
		"request_id": "req-cas-stale", "idempotency_key": "idem-cas-stale",
		"agent_run_id": "consolidator-01", "expected_ledger_revision": 0,
		"family_proposal_ids": []string{id},
	})
	if staleStatus != http.StatusConflict {
		t.Fatalf("stale CAS status = %d payload=%s, want 409", staleStatus, stalePayload)
	}
	rev, digest := w.consolidate.CurrentRevision()
	if rev != 1 || digest != first.LedgerDigest {
		t.Fatalf("ledger after stale CAS = (%d,%s), want (1,%s)", rev, digest, first.LedgerDigest)
	}
}

func TestGraphBatchFreezeBindsEvidenceLedgerAndPolicy(t *testing.T) {
	w := newGraphBatchWorld(t)
	w.registerFixtureTrajectory()
	id := w.propose("raw-proposal-http-freeze-aaa01", "idem-freeze-propose")
	status, payload := w.post(httpapi.GraphBatchSkillConsolidatePath, map[string]any{
		"request_id": "req-freeze-cas", "idempotency_key": "idem-freeze-cas",
		"agent_run_id": "consolidator-01", "expected_ledger_revision": 0,
		"family_proposal_ids": []string{id},
	})
	if status != http.StatusOK {
		t.Fatalf("consolidate before freeze: %d %s", status, payload)
	}
	var cons struct {
		LedgerRevision json.Number `json:"ledger_revision"`
		LedgerDigest   string      `json:"ledger_digest"`
	}
	if err := json.Unmarshal(payload, &cons); err != nil {
		t.Fatalf("decode consolidate: %v", err)
	}
	revision, err := strconv.ParseUint(cons.LedgerRevision.String(), 10, 64)
	if err != nil {
		t.Fatalf("ledger revision: %v", err)
	}
	policy := fixturePolicyDigests()
	graph := mustGraphFixture(t)
	freezeStatus, freezePayload := w.post(httpapi.GraphBatchEvaluationFreezePath, map[string]any{
		"request_id":               "req-freeze-1",
		"expected_ledger_revision": revision,
		"expected_ledger_digest":   cons.LedgerDigest,
		"proposal_ids":             []string{id},
		"evidence_cut": map[string]any{
			"batches": []any{map[string]any{"id": "evidence-01", "kind": "train"}},
		},
		"policy":        policy,
		"scope":         []any{map[string]any{"kind": "train_trajectory", "id": "trajectory-01"}},
		"graph_fixture": graph,
	})
	if freezeStatus != http.StatusOK {
		t.Fatalf("freeze status = %d payload=%s", freezeStatus, freezePayload)
	}
	var got struct {
		ManifestID     string      `json:"manifest_id"`
		Digest         string      `json:"digest"`
		LedgerRevision json.Number `json:"ledger_revision"`
		LedgerDigest   string      `json:"ledger_digest"`
	}
	if err := json.Unmarshal(freezePayload, &got); err != nil {
		t.Fatalf("decode freeze: %v", err)
	}
	if got.ManifestID == "" || got.Digest == "" {
		t.Fatalf("freeze missing identity: %#v", got)
	}
	if got.LedgerRevision.String() != cons.LedgerRevision.String() || got.LedgerDigest != cons.LedgerDigest {
		t.Fatalf("freeze ledger pin = (%s,%s), want (%s,%s)", got.LedgerRevision, got.LedgerDigest, cons.LedgerRevision, cons.LedgerDigest)
	}
	manifest, ok := w.freeze.SealedManifest()
	if !ok {
		t.Fatal("freeze did not seal a manifest")
	}
	if manifest.Digest != got.Digest || manifest.ID != got.ManifestID {
		t.Fatalf("sealed %#v vs response %#v", manifest, got)
	}
	if len(manifest.EvidenceCut.Batches) != 1 || manifest.EvidenceCut.Batches[0].ID != "evidence-01" {
		t.Fatalf("sealed evidence cut = %#v", manifest.EvidenceCut)
	}
	if manifest.EvidenceCut.Watermark == "" || manifest.EvidenceCut.WatermarkDigest == "" {
		t.Fatalf("sealed evidence watermark missing: %#v", manifest.EvidenceCut)
	}
	if manifest.LedgerRevision != revision || manifest.LedgerDigest != cons.LedgerDigest {
		t.Fatalf("sealed ledger = (%d,%s)", manifest.LedgerRevision, manifest.LedgerDigest)
	}
	if manifest.Policy != (evaluationfreeze.PolicyRevisions{
		PromptDigest: policy["prompt_digest"].(string), SchemaDigest: policy["schema_digest"].(string),
		ModelDigest: policy["model_digest"].(string), ToolDigest: policy["tool_digest"].(string),
		ConfigDigest: policy["config_digest"].(string), GradingPolicyDigest: policy["grading_policy_digest"].(string),
	}) {
		t.Fatalf("sealed policy = %#v", manifest.Policy)
	}
	if len(manifest.Proposals) != 1 || manifest.Proposals[0].ProposalID != id {
		t.Fatalf("sealed proposals = %#v", manifest.Proposals)
	}
}

func TestGraphBatchUnauthorizedIs401(t *testing.T) {
	w := newGraphBatchWorld(t)
	for _, path := range []string{
		httpapi.GraphBatchSkillProposePath,
		httpapi.GraphBatchSkillConsolidatePath,
		httpapi.GraphBatchEvaluationFreezePath,
		httpapi.GraphBatchTrajectoriesRegisterPath,
	} {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(`{"request_id":"req-unauth"}`)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		w.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s without token status = %d, want 401", path, rec.Code)
		}
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil || envelope.Error.Code != "UNAUTHORIZED" {
			t.Fatalf("%s unauthorized body = %s", path, rec.Body.String())
		}
	}
}

func TestGraphBatchProposeWithoutRegisteredTrajectoryFailsClosed(t *testing.T) {
	w := newGraphBatchWorld(t)
	body := validProposalBody(t, "raw-proposal-http-failclose-01")
	status, payload := w.post(httpapi.GraphBatchSkillProposePath, map[string]any{
		"request_id": "req-fail-closed", "idempotency_key": "idem-fail-closed",
		"diagnosis_run_id": "diagnosis-run-01", "trajectory_id": "trajectory-01", "body": body,
	})
	if status == http.StatusOK {
		t.Fatalf("propose without registered trajectory succeeded: %s", payload)
	}
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("fail-closed propose status = %d payload=%s, want 422", status, payload)
	}
	if got := w.proposals.Count(); got != 0 {
		t.Fatalf("fail-closed propose appended %d records", got)
	}
	if _, ok := w.proposals.Get("raw-proposal-http-failclose-01"); ok {
		t.Fatal("fail-closed propose left a readable proposal")
	}
}

func (w *graphBatchWorld) registerFixtureTrajectory() {
	w.t.Helper()
	status, payload := w.post(httpapi.GraphBatchTrajectoriesRegisterPath, map[string]any{
		"diagnosis_run_id": "diagnosis-run-01",
		"trajectory": map[string]any{
			"id": "trajectory-01", "snapshot_id": "snapshot-01",
			"complete_trajectory": []string{"opening", "checkpoint", "outcome"},
			"public_outcome":      "failed: file-not-found",
			"checkpoints": map[string]any{
				"checkpoint-01": map[string]any{"id": "checkpoint-01", "snapshot_id": "snapshot-01"},
			},
			"evidence": map[string]any{
				"evidence-01": map[string]any{
					"id": "evidence-01", "trajectory_id": "trajectory-01", "snapshot_id": "snapshot-01",
					"observable_facts": []string{"the relative path resolved outside the workspace"},
				},
			},
			"authorized_diagnosis_run_ids": []string{"diagnosis-run-01"},
		},
	})
	if status != http.StatusOK {
		w.t.Fatalf("register trajectory: %d %s", status, payload)
	}
}

func (w *graphBatchWorld) propose(proposalID, idempotencyKey string) string {
	w.t.Helper()
	status, payload := w.post(httpapi.GraphBatchSkillProposePath, map[string]any{
		"request_id": "req-" + proposalID, "idempotency_key": idempotencyKey,
		"diagnosis_run_id": "diagnosis-run-01", "trajectory_id": "trajectory-01",
		"body": validProposalBody(w.t, proposalID),
	})
	if status != http.StatusOK {
		w.t.Fatalf("propose %s: %d %s", proposalID, status, payload)
	}
	var got struct {
		ProposalID string `json:"proposal_id"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		w.t.Fatalf("decode propose: %v", err)
	}
	return got.ProposalID
}

func (w *graphBatchWorld) post(path string, body map[string]any) (int, []byte) {
	w.t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		w.t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded))
	req.Header.Set("Authorization", "Bearer "+graphBatchToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	w.handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

func validProposalBody(t *testing.T, proposalID string) map[string]any {
	t.Helper()
	body := map[string]any{
		"proposal_id": proposalID, "schema_version": rawproposal.SchemaVersion, "source_checkpoint_id": "checkpoint-01",
		"source_evidence_refs":     []any{"evidence-01"},
		"context_trigger":          "When the relative path resolved outside the workspace at checkpoint-01",
		"failure_or_opportunity":   "The relative path resolved outside the workspace and caused file-not-found.",
		"baseline_behavior":        "Invoke the tool with the relative path directly.",
		"non_obvious_insight":      "The relative path resolved outside the workspace, so tool invocation needs an explicit workspace-root path.",
		"decision_policy_or_steps": []any{"When the relative path resolved outside the workspace, resolve it against the workspace root before invoking the tool."},
		"expected_behavior_change": "Resolve the path against the workspace root before invoking the tool.",
		"contraindications":        []any{"Do not rewrite an already absolute path."},
		"pitfalls":                 []any{"Do not rewrite an already absolute path."},
		"outcome_observed":         "failed: file-not-found",
		"novelty_status":           rawproposal.NoveltyHypothesized,
		"created_by_agent_run_id":  "diagnosis-run-01",
		"created_at":               "2026-09-17T09:53:15Z",
	}
	digest, err := contract.DigestOf(map[string]any{
		"proposal_id": body["proposal_id"], "schema_version": body["schema_version"],
		"source_checkpoint_id": body["source_checkpoint_id"], "source_evidence_refs": body["source_evidence_refs"],
		"context_trigger": body["context_trigger"], "failure_or_opportunity": body["failure_or_opportunity"],
		"baseline_behavior": body["baseline_behavior"], "non_obvious_insight": body["non_obvious_insight"],
		"decision_policy_or_steps": body["decision_policy_or_steps"], "expected_behavior_change": body["expected_behavior_change"],
		"contraindications": body["contraindications"], "pitfalls": body["pitfalls"],
		"outcome_observed": body["outcome_observed"], "novelty_status": body["novelty_status"],
		"created_by_agent_run_id": body["created_by_agent_run_id"], "created_at": body["created_at"],
	})
	if err != nil {
		t.Fatalf("proposal digest: %v", err)
	}
	body["content_digest"] = digest
	return body
}

func fixturePolicyDigests() map[string]any {
	pin := func(name string) string {
		digest, err := contract.DigestOf(name)
		if err != nil {
			panic(err)
		}
		return digest
	}
	return map[string]any{
		"prompt_digest": pin("warm-skill-graph-batch.prompt.v1"), "schema_digest": pin("warm-skill-graph-batch.schema.v1"),
		"model_digest": pin("warm-skill-graph-batch.model.v1"), "tool_digest": pin("warm-skill-graph-batch.tool.v1"),
		"config_digest": pin("warm-skill-graph-batch.config.v1"), "grading_policy_digest": pin("warm-skill-graph-batch.grading.v1"),
	}
}

func mustGraphFixture(t *testing.T) evaluationgraph.CanonicalFixture {
	t.Helper()
	alpha := evaluationgraph.RevisionRef{LineageID: "alpha", Revision: 1}
	beta := evaluationgraph.RevisionRef{LineageID: "beta", Revision: 1}
	fixture, err := evaluationgraph.NewFixture([]evaluationgraph.CanonicalRecord{
		{Sequence: 1, ID: "proposal-alpha", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: alpha, Body: "advisory alpha", Advisory: true}},
		{Sequence: 2, ID: "proposal-beta", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: beta, Body: "advisory beta", Advisory: true}},
		{Sequence: 3, ID: "consolidate-alpha", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: alpha, Disposition: "retain"}},
		{Sequence: 4, ID: "consolidate-beta", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: beta, Disposition: "merge"}},
		{Sequence: 5, ID: "alpha-beta", Kind: evaluationgraph.RecordRelation, Relation: &evaluationgraph.Relation{From: alpha, To: beta, Kind: "related_to"}},
	})
	if err != nil {
		t.Fatalf("graph fixture: %v", err)
	}
	return fixture
}

func TestGraphBatchWorldUsesIndependentAuthorities(t *testing.T) {
	// Compile-time reminder that the HTTP tests exercise the same stack the
	// server composition root constructs (registry/store/manager/rawproposal).
	w := newGraphBatchWorld(t)
	if w.handler == nil || w.proposals == nil || w.consolidate == nil || w.freeze == nil || w.source == nil {
		t.Fatal("graph batch world is not fully wired")
	}
	if _, err := w.source.ReadFrozenTrajectory(context.Background(), "diagnosis-run-01", "trajectory-01"); err == nil {
		t.Fatal("unregistered trajectory must fail closed before Host injects it")
	}
}
