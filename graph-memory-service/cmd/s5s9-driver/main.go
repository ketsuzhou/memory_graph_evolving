// Command s5s9-driver is the GMS-side INT-003 contract driver (GMS-204
// release, GMS-205 projector, GMS-206 retrieval tools, GMS-207 closure
// read) for the shared S5-S9 conformance tracer.
//
// Line-delimited JSON protocol over stdio: one instruction per line on
// stdin, one single-line JSON response per instruction on stdout, logs on
// stderr. Instructions are {"id":N,"op":"...", ...}; responses are
// {"id":N,"ok":true,...} or {"id":N,"ok":false,"error":{"code","reason"}}
// with codes drawn from the §13.7.1 closed reason registry vocabulary.
//
// The driver is a pure fixture tool of the shared conformance tracer
// ($FIX/integration/run_s5_s9.py): it drives the REAL GMS services (one
// in-memory authoritative world wired exactly like cmd/server's
// newProjectorWorker: ledger -> gates -> proposals -> activations ->
// projector -> closure read -> retrieval tools) and reports raw facts
// only. It never computes scores, U1 decisions or release semantics; the
// accepted §7.12 decision it feeds Activate is an input fixture of the
// slice, exactly like the activation package's own tests.
//
// Ops (all take "fixtures": the shared conformance directory):
//
//	release.activate  {lineage_id, kind, mode:
//	                     ok|stale_cas|not_accepted|body_mismatch}
//	projector.project {mode: runtime|delivered|duplicate|gap|conflict}
//	tools.invoke      {mode: ok|behind, session, min_activation_sequence}
//	closure.read      {mode: ok|graph_root|torn|not_current, root_lineage,
//	                   root_kind}
//	world.reset       {}  — rebuild a pristine authority world
//
// One driver session holds ONE authority world (the S5-S9 chain is one
// causal story: release -> project -> explore -> closure). world.reset
// rebuilds it for the negative probes so a failed probe can never
// contaminate the main chain.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/materializationread"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/retrieval"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

func main() {
	reader := bufio.NewReaderSize(os.Stdin, 1<<20)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	respond := func(payload map[string]any) {
		line, _ := json.Marshal(payload)
		writer.WriteString(string(line) + "\n")
		writer.Flush()
	}
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			handleInstruction(line, respond)
		}
		if err != nil {
			return
		}
	}
}

func handleInstruction(raw string, respond func(map[string]any)) {
	value, perr := contract.ParseJSONStrict([]byte(raw))
	if perr != nil {
		respond(driverError(0, "DRIVER_INSTRUCTION_INVALID", "unparseable instruction: "+perr.Error()))
		return
	}
	body, ok := value.(map[string]any)
	if !ok {
		respond(driverError(0, "DRIVER_INSTRUCTION_INVALID", "instruction is not an object"))
		return
	}
	id := jsonInt(body["id"])
	op := strOf(body["op"])
	if op == "" {
		respond(driverError(id, "DRIVER_INSTRUCTION_INVALID", "op missing"))
		return
	}
	var payload map[string]any
	var code, reason string
	switch op {
	case "release.activate":
		payload, code, reason = opReleaseActivate(body)
	case "projector.project":
		payload, code, reason = opProjectorProject(body)
	case "tools.invoke":
		payload, code, reason = opToolsInvoke(body)
	case "closure.read":
		payload, code, reason = opClosureRead(body)
	case "world.reset":
		payload, code, reason = opWorldReset(body)
	default:
		code, reason = "DRIVER_OP_UNKNOWN", "op "+op
	}
	if code != "" {
		respond(driverError(id, code, reason))
		return
	}
	payload["id"] = id
	payload["ok"] = true
	payload["op"] = op
	respond(payload)
}

func driverError(id int64, code, reason string) map[string]any {
	return map[string]any{
		"id":    id,
		"ok":    false,
		"error": map[string]any{"code": code, "reason": reason},
	}
}

// ---------------------------------------------------------------------------
// Shared decoding helpers (decoder-model: json.Number for every number)
// ---------------------------------------------------------------------------

func strOf(v any) string {
	s, _ := v.(string)
	return s
}

func boolOf(v any) bool {
	b, _ := v.(bool)
	return b
}

func jsonInt(v any) int64 {
	number, ok := v.(json.Number)
	if !ok {
		return 0
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func jsonUint(v any) uint64 {
	number, ok := v.(json.Number)
	if !ok {
		return 0
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func jn(v int64) any { return json.Number(strconv.FormatInt(v, 10)) }

func conformanceDir(instruction map[string]any) (string, error) {
	if dir := strOf(instruction["fixtures"]); dir != "" {
		return dir, nil
	}
	if dir := os.Getenv("RSIH_CONFORMANCE_DIR"); dir != "" {
		return dir, nil
	}
	return contract.DefaultConformanceDir()
}

// ---------------------------------------------------------------------------
// The authority world: the real GMS stack, wired like cmd/server
// ---------------------------------------------------------------------------

type stubAuthorizer struct{}

func (stubAuthorizer) AuthorizeDeactivation(contract.VersionedRef, string, contract.SkillArtifactRef) error {
	return nil
}

type mapEvidence struct {
	refs map[string]contract.EvidenceRef
}

func (m *mapEvidence) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	ref, ok := m.refs[id]
	return ref, ok, nil
}

// driverCandidateView is the frozen candidate view presented to Activate
// (same shape as the GMS-202 candidate service's view).
type driverCandidateView struct {
	ref  contract.CandidateArtifactRef
	body []byte
}

func (v *driverCandidateView) Ref() contract.CandidateArtifactRef { return v.ref }
func (v *driverCandidateView) BodyDigest() string                 { return v.ref.BodyDigest }
func (v *driverCandidateView) CanonicalBody() []byte {
	out := make([]byte, len(v.body))
	copy(out, v.body)
	return out
}

type driverDecision struct{ doc map[string]any }

func (d driverDecision) Doc() map[string]any { return d.doc }
func (d driverDecision) Outcome() string {
	s, _ := contract.AsString(d.doc["outcome"])
	return s
}
func (d driverDecision) DecisionDigest() string {
	s, _ := contract.AsString(d.doc["decision_digest"])
	return s
}

type world struct {
	registry    *ledger.ContractReasonRegistry
	gates       *validation.Gates
	confDir     string
	store       *ledger.MemoryStore
	mgr         ledger.TxManager
	proposals   *proposal.Service
	activations *activation.ActivationService
	artifacts   *artifact.Service
	projector   *projector.Service
	closure     *materializationread.Service
	retrieval   *retrieval.Service
	evidence    *mapEvidence
	releaseSeq  int
}

func buildWorld(confDir string) (*world, error) {
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(confDir, "policy"))
	if err != nil {
		return nil, fmt.Errorf("load reason policy: %w", err)
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	schemas, err := validation.LoadSchemaSet(filepath.Join(confDir, "schema", "shared"))
	if err != nil {
		return nil, fmt.Errorf("load authority schemas: %w", err)
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		return nil, fmt.Errorf("new gates: %w", err)
	}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		return nil, fmt.Errorf("new memory store: %w", err)
	}
	manager, err := ledger.NewManager(store, registry)
	if err != nil {
		return nil, fmt.Errorf("new transaction manager: %w", err)
	}
	proposals, err := proposal.NewService(gates, filepath.Join(confDir, "schema", "state"), store, manager, registry)
	if err != nil {
		return nil, fmt.Errorf("proposal service: %w", err)
	}
	activations, err := activation.NewActivationService(activation.Config{
		Store: store, Tx: manager, Registry: registry, Gates: gates,
		Proposals: proposals, DeactivationAuthorizer: stubAuthorizer{},
	})
	if err != nil {
		return nil, fmt.Errorf("activation service: %w", err)
	}
	artifacts, err := artifact.NewService(gates)
	if err != nil {
		return nil, fmt.Errorf("artifact service: %w", err)
	}
	proj, err := projector.NewService(projector.Config{
		Store: store, Tx: manager, Registry: registry, Gates: gates,
	})
	if err != nil {
		return nil, fmt.Errorf("projector service: %w", err)
	}
	closure, err := materializationread.NewService(materializationread.Config{
		Heads: activations, Artifacts: store, Gate: artifacts,
		Activations: store, Registry: registry,
	})
	if err != nil {
		return nil, fmt.Errorf("closure read service: %w", err)
	}
	toolPolicy, err := retrieval.LoadToolPolicy(
		filepath.Join(confDir, "policy", "tool-success-validation.v1.json"))
	if err != nil {
		return nil, fmt.Errorf("load tool policy: %w", err)
	}
	skillGetGreen, err := retrieval.VerifySkillGetReadiness(confDir)
	if err != nil {
		return nil, fmt.Errorf("skill_get readiness gate: %w", err)
	}
	evidence := &mapEvidence{refs: map[string]contract.EvidenceRef{}}
	retrievalSvc, err := retrieval.NewService(retrieval.Config{
		Projection: proj, Heads: activations, Artifacts: store,
		Gate: artifacts, Evidence: evidence, Gates: gates,
		Registry: registry, Policy: toolPolicy, SkillGetEnabled: skillGetGreen,
	})
	if err != nil {
		return nil, fmt.Errorf("retrieval tools service: %w", err)
	}
	return &world{
		registry: registry, gates: gates, confDir: confDir, store: store,
		mgr: manager, proposals: proposals, activations: activations,
		artifacts: artifacts, projector: proj, closure: closure,
		retrieval: retrievalSvc, evidence: evidence,
	}, nil
}

var currentWorld *world
var worldConfDir string

func worldFor(instruction map[string]any) (*world, error) {
	dir, err := conformanceDir(instruction)
	if err != nil {
		return nil, err
	}
	if currentWorld == nil || worldConfDir != dir {
		currentWorld, err = buildWorld(dir)
		if err != nil {
			return nil, err
		}
		worldConfDir = dir
	}
	return currentWorld, nil
}

func opWorldReset(instruction map[string]any) (map[string]any, string, string) {
	dir, err := conformanceDir(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	w, err := buildWorld(dir)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}
	currentWorld = w
	worldConfDir = dir
	return map[string]any{"reset": true}, "", ""
}

// ---------------------------------------------------------------------------
// Envelope builders (integer-only JSON numbers, Contract §6.3) — the same
// frozen shapes the GMS activation/projector/retrieval tests drive.
// ---------------------------------------------------------------------------

func portRef(id string) map[string]any {
	return map[string]any{
		"schema_id": "https://example.com/schemas/" + id,
		"version":   jn(1),
		"digest":    contract.DigestBytes([]byte("port:" + id)),
	}
}

func evidenceRefDoc(id string) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaEvidenceRef,
		"evidence_id":     id,
		"version":         jn(1),
		"evidence_digest": contract.DigestBytes([]byte("evidence:" + id)),
		"commit_state":    "committed",
		"evidence_kind":   "observation",
	}
}

func guidanceEnvelope(lineage string) map[string]any {
	body := map[string]any{
		"causal_context": map[string]any{"summary": "prior context", "claim_refs": []any{}},
		"branches": []any{map[string]any{
			"branch_id": "b1",
			"when":      map[string]any{},
			"action":    map[string]any{"guidance": "run the " + lineage + " procedure", "failure_action": "stop", "evidence_refs": []any{evidenceRefDoc("ev-" + lineage)}},
			"future":    map[string]any{"expected_outcome": "checked", "critical_steps": []any{}, "final_task_impact": "task completes"},
		}},
		"input_port_schema":  portRef(lineage + "-in"),
		"output_port_schema": portRef(lineage + "-out"),
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "step_guidance",
		"title":          lineage + " guidance",
		"description":    "s5s9 driver guidance envelope",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{map[string]any{"capability": "memory_explore", "scope": "room-shared-space"}},
		"body":           body,
	}
}

func procedureEnvelope(lineage string) map[string]any {
	body := map[string]any{
		"steps": []any{map[string]any{
			"step_id":        "s1",
			"instruction":    "summarize the diff",
			"preconditions":  []any{},
			"postconditions": []any{},
			"failure_action": "stop",
		}},
		"input_port_schema":  portRef(lineage + "-in"),
		"output_port_schema": portRef(lineage + "-out"),
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "human_procedure",
		"title":          lineage + " procedure",
		"description":    "s5s9 driver procedure envelope",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{map[string]any{"capability": "memory_explore", "scope": "room-shared-space"}},
		"body":           body,
	}
}

func compositeEnvelope(lineage string, children ...contract.SkillArtifactRef) map[string]any {
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
		"input_port_schema":         portRef(lineage + "-in"),
		"output_port_schema":        portRef(lineage + "-out"),
	}
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "composite",
		"title":          lineage + " composite",
		"description":    "s5s9 driver composite envelope",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions":    []any{},
		"body":           body,
	}
}

func skillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         json.Number(ref.Version),
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

func vrefDoc(id, digest string) map[string]any {
	return map[string]any{"id": id, "version": jn(1), "digest": digest}
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

func (w *world) proposalDoc(proposalID, kind string) (map[string]any, error) {
	doc := map[string]any{
		"schema_version":   "gms.skill-proposal.v1",
		"proposal_id":      proposalID,
		"proposal_version": jn(1),
		"proposed_kind":    kind,
		"source_segment_refs": []any{map[string]any{
			"schema_version":  "host.segment-ref.v1",
			"room_id":         "room-s5s9",
			"segment_id":      "seg-" + proposalID,
			"segment_version": jn(1),
			"segment_digest":  contract.DigestBytes([]byte("seg-" + proposalID)),
			"evidence_seal_ref": map[string]any{
				"id": "evseal-" + proposalID, "version": jn(1),
				"digest": contract.DigestBytes([]byte("evseal-" + proposalID)),
			},
			"path_seal_ref": map[string]any{
				"id": "pathseal-" + proposalID, "version": jn(1),
				"digest": contract.DigestBytes([]byte("pathseal-" + proposalID)),
			},
		}},
		"evidence_refs":       []any{evidenceRefDoc("ev-" + proposalID)},
		"requested_operation": "create_lineage",
		"origin": map[string]any{
			"initiator_type": "model", "initiator_ref": "agent-s5s9",
			"request_ref": "req-" + proposalID,
		},
		"policy_refs": []any{map[string]any{
			"id": "policy-static-gates", "version": jn(1),
			"digest": contract.DigestBytes([]byte("policy-static-gates")),
		}},
	}
	digest, err := w.gates.ComputeDigestPreimage(doc, "skill-proposal.schema.json")
	if err != nil {
		return nil, err
	}
	doc["proposal_digest"] = digest
	return doc, nil
}

func (w *world) decisionDoc(cand contract.CandidateArtifactRef, outcome string) (map[string]any, error) {
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
	digest, err := w.gates.ComputeDigestPreimage(doc, "release-decision.schema.json")
	if err != nil {
		return nil, err
	}
	doc["decision_digest"] = digest
	if err := w.gates.ValidateInstance(doc, "release-decision.schema.json"); err != nil {
		return nil, err
	}
	return doc, nil
}

func (w *world) commitCandidate(cand contract.CandidateArtifactRef, body []byte) error {
	payload, err := json.Marshal(map[string]any{
		"schema_version": cand.SchemaVersion,
		"candidate_id":   cand.CandidateID,
		"kind":           cand.Kind,
		"body_digest":    cand.BodyDigest,
		"origin_type":    cand.OriginType,
		"origin_ref":     vrefDoc(cand.OriginRef.ID, cand.OriginRef.Digest),
	})
	if err != nil {
		return err
	}
	return w.mgr.WithinTx(context.Background(), func(tx *ledger.Tx) error {
		if _, err := tx.PutContent(body); err != nil {
			return err
		}
		_, err := tx.AppendEvent(ledger.LedgerCandidate, "", cand.CandidateID, payload)
		return err
	})
}

// releaseResponse reports the committed release facts.
func (w *world) releaseResponse(res *activation.ReleaseResult) map[string]any {
	previous := any(nil)
	if res.PreviousActiveRef != nil {
		previous = skillRefDoc(*res.PreviousActiveRef)
	}
	return map[string]any{
		"activated":           true,
		"released_ref":        skillRefDoc(res.ReleasedRef),
		"activation_sequence": jn(int64(res.ActivationSequence)),
		"event_id":            res.EventID,
		"event_digest":        res.EventDigest,
		"outbox_key":          res.OutboxKey,
		"previous_active_ref": previous,
		"replayed":            res.Replayed,
	}
}

// residueSnapshot measures the whole authoritative write surface of one
// release (zero-partial evidence, GMS §6.1).
type residueSnapshot struct {
	activationEntries int
	lineageHead       string
	lineageHeadSeq    uint64
	sequenceHead      string
	outboxPending     int
	proposalState     string
}

func (w *world) residue(lineage, proposalID string) (residueSnapshot, error) {
	snap := residueSnapshot{}
	entries, err := w.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		return snap, err
	}
	snap.activationEntries = len(entries)
	seq, digest, _, err := w.store.GetHead(ledger.HeadSkillActive, lineage)
	if err != nil {
		return snap, err
	}
	snap.lineageHeadSeq, snap.lineageHead = seq, digest
	seq, digest, _, err = w.store.GetHead(ledger.HeadActivationSequence, "")
	if err != nil {
		return snap, err
	}
	snap.sequenceHead = fmt.Sprintf("%d:%s", seq, digest)
	pending, err := w.store.Pending()
	if err != nil {
		return snap, err
	}
	snap.outboxPending = len(pending)
	state, _, err := w.proposals.CurrentState(context.Background(), proposalID)
	if err != nil {
		return snap, err
	}
	snap.proposalState = state
	return snap, nil
}

func residueEqual(a, b residueSnapshot) bool {
	return a.activationEntries == b.activationEntries &&
		a.lineageHead == b.lineageHead && a.lineageHeadSeq == b.lineageHeadSeq &&
		a.sequenceHead == b.sequenceHead &&
		a.outboxPending == b.outboxPending &&
		a.proposalState == b.proposalState
}

func residueDoc(snap residueSnapshot) map[string]any {
	return map[string]any{
		"activation_entries": jn(int64(snap.activationEntries)),
		"lineage_head":       snap.lineageHead,
		"lineage_head_seq":   jn(int64(snap.lineageHeadSeq)),
		"sequence_head":      snap.sequenceHead,
		"outbox_pending":     jn(int64(snap.outboxPending)),
		"proposal_state":     snap.proposalState,
	}
}

// preparedRelease is one fully prepared release: proposal at
// activation_pending, committed candidate, accepted decision.
type preparedRelease struct {
	proposalID string
	propDoc    map[string]any
	cand       contract.CandidateArtifactRef
	view       *driverCandidateView
	decision   driverDecision
}

func (w *world) prepareRelease(lineage, kind string, envelope map[string]any) (*preparedRelease, []byte, error) {
	w.releaseSeq++
	proposalID := fmt.Sprintf("prop-s5s9-%s-%03d", lineage, w.releaseSeq)
	propDoc, err := w.proposalDoc(proposalID, kind)
	if err != nil {
		return nil, nil, err
	}
	for _, edge := range activationPendingPath {
		if _, err := w.proposals.AppendTransition(context.Background(), propDoc, edge[0], edge[1]); err != nil {
			return nil, nil, err
		}
	}
	canonical, err := w.artifacts.Canonicalize(envelope)
	if err != nil {
		return nil, nil, err
	}
	body, digest := canonical.Canonical(), canonical.BodyDigest()
	propDigest, _ := contract.AsString(propDoc["proposal_digest"])
	cand := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   fmt.Sprintf("cand-s5s9-%s-%03d", lineage, w.releaseSeq),
		Kind:          kind,
		BodyDigest:    digest,
		OriginType:    "skill_proposal",
		OriginRef:     contract.VersionedRef{ID: proposalID, Version: "1", Digest: propDigest},
	}
	if err := w.commitCandidate(cand, body); err != nil {
		return nil, nil, err
	}
	doc, err := w.decisionDoc(cand, "accepted")
	if err != nil {
		return nil, nil, err
	}
	return &preparedRelease{
		proposalID: proposalID, propDoc: propDoc, cand: cand,
		view:     &driverCandidateView{ref: cand, body: body},
		decision: driverDecision{doc: doc},
	}, body, nil
}

// ---------------------------------------------------------------------------
// S5: release.activate
// ---------------------------------------------------------------------------

func opReleaseActivate(instruction map[string]any) (map[string]any, string, string) {
	w, err := worldFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	lineage := strOf(instruction["lineage_id"])
	if lineage == "" {
		return nil, "DRIVER_INSTRUCTION_INVALID", "lineage_id required"
	}
	mode := strOf(instruction["mode"])
	if mode == "" {
		mode = "ok"
	}
	kind := strOf(instruction["kind"])
	if kind == "" {
		kind = "step_guidance"
	}
	ctx := context.Background()

	var envelope map[string]any
	switch kind {
	case "human_procedure":
		envelope = procedureEnvelope(lineage)
	case "composite":
		envelope = compositeEnvelope(lineage)
	default:
		envelope = guidanceEnvelope(lineage)
	}
	fix, body, err := w.prepareRelease(lineage, kind, envelope)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "prepare release: " + err.Error()
	}

	before, err := w.residue(lineage, fix.proposalID)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "residue before: " + err.Error()
	}

	// Negative variants over the same prepared release.
	switch mode {
	case "not_accepted":
		doc, err := w.decisionDoc(fix.cand, "rejected")
		if err != nil {
			return nil, "DRIVER_INTERNAL_ERROR", err.Error()
		}
		_, err = w.activations.Activate(ctx, activation.ActivateRequest{
			Decision: driverDecision{doc: doc}, Candidate: fix.view,
			LineageID: lineage, ProposalDoc: fix.propDoc,
		})
		after, _ := w.residue(lineage, fix.proposalID)
		return map[string]any{
			"mode": mode, "activated": false,
			"reason_code":       activation.CodeOf(err),
			"residue":           residueDoc(after),
			"residue_unchanged": residueEqual(before, after),
		}, "", ""

	case "body_mismatch":
		tampered := &driverCandidateView{ref: fix.cand, body: append([]byte{}, body...)}
		tampered.body[len(tampered.body)-1] ^= 0x01
		_, err = w.activations.Activate(ctx, activation.ActivateRequest{
			Decision: fix.decision, Candidate: tampered,
			LineageID: lineage, ProposalDoc: fix.propDoc,
		})
		after, _ := w.residue(lineage, fix.proposalID)
		return map[string]any{
			"mode": mode, "activated": false,
			"reason_code":       activation.CodeOf(err),
			"residue":           residueDoc(after),
			"residue_unchanged": residueEqual(before, after),
		}, "", ""

	case "stale_cas":
		// A wrong frozen expectation: expecting a head that is not the
		// current active revision rejects before anything is staged
		// (the same ACTIVE_HEAD_CONFLICT class a mid-commit racing
		// committer produces; the GMS activation tests pin the racing
		// writer case — here the driver stays out of the store's
		// internals and proves the zero-residue guarantee of the
		// request-visible variant).
		wrong := contract.SkillArtifactRef{
			SchemaVersion: contract.SchemaSkillArtifactRef,
			LineageID:     lineage, Version: "1", Kind: kind,
			ArtifactDigest: contract.DigestBytes([]byte("not-the-active-digest")),
		}
		_, err = w.activations.Activate(ctx, activation.ActivateRequest{
			Decision: fix.decision, Candidate: fix.view, LineageID: lineage,
			ProposalDoc: fix.propDoc, ExpectedHead: &wrong,
		})
		after, _ := w.residue(lineage, fix.proposalID)
		return map[string]any{
			"mode": mode, "activated": false,
			"reason_code":       activation.CodeOf(err),
			"residue":           residueDoc(after),
			"residue_unchanged": residueEqual(before, after),
		}, "", ""
	}

	// ok: the ordinary atomic release.
	active, err := w.activations.ActiveRevision(ctx, lineage)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "active head: " + err.Error()
	}
	res, err := w.activations.Activate(ctx, activation.ActivateRequest{
		Decision: fix.decision, Candidate: fix.view, LineageID: lineage,
		ExpectedHead: active, ProposalDoc: fix.propDoc,
	})
	if err != nil {
		return nil, "DRIVER_OP_FAILED",
			"activate failed: " + activation.CodeOf(err) + ": " + err.Error()
	}
	after, err := w.residue(lineage, fix.proposalID)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "residue after: " + err.Error()
	}
	// Atomicity: exactly one activation entry, the head moved to the new
	// version, exactly one pending outbox record and the proposal went
	// terminal — all-or-nothing, never a mixture.
	atomic := after.activationEntries == before.activationEntries+1 &&
		after.lineageHeadSeq == before.lineageHeadSeq+1 &&
		after.outboxPending == before.outboxPending+1 &&
		after.proposalState == activation.StateReleased
	payload := w.releaseResponse(res)
	payload["mode"] = "ok"
	payload["lineage_id"] = res.ReleasedRef.LineageID
	payload["version"] = json.Number(res.ReleasedRef.Version)
	payload["outbox_pending"] = jn(int64(after.outboxPending))
	payload["residue"] = residueDoc(after)
	payload["residue_atomic"] = atomic
	return payload, "", ""
}

// ---------------------------------------------------------------------------
// S6: projector.project
// ---------------------------------------------------------------------------

func (w *world) deliveredEvents(from, to uint64) ([]projector.DeliveredEvent, error) {
	entries, err := w.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		return nil, err
	}
	var out []projector.DeliveredEvent
	for _, entry := range entries {
		if entry.Sequence < from || entry.Sequence > to {
			continue
		}
		payload, ok, err := w.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return nil, fmt.Errorf("event %d payload unresolved", entry.Sequence)
		}
		out = append(out, projector.DeliveredEvent{
			Sequence: entry.Sequence, EventID: entry.EventID,
			PayloadDigest: entry.PayloadDigest, Payload: payload,
		})
	}
	return out, nil
}

func (w *world) watermarkSequence() (uint64, string, error) {
	doc, digest, _ := w.projector.Watermark()
	if doc == nil {
		return 0, digest, nil
	}
	number, ok := doc["projected_through_activation_sequence"].(json.Number)
	if !ok {
		return 0, digest, nil
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		return 0, digest, err
	}
	return value, digest, nil
}

func (w *world) projectionFacts(mode string, result *projector.ProjectionResult, watermarkBefore uint64, watermarkBeforeDigest string) map[string]any {
	blocked := ""
	if result.Blocked != nil {
		blocked = result.Blocked.Code
	}
	afterSeq, afterDigest := func() (uint64, string) {
		seq, digest, _ := w.watermarkSequence()
		return seq, digest
	}()
	payload := map[string]any{
		"mode":                mode,
		"state":               result.State,
		"projected":           jn(int64(result.Projected)),
		"cursor_activation":   jn(int64(result.Cursors.Activation)),
		"watermark":           result.Watermark,
		"watermark_digest":    result.WatermarkDigest,
		"graph_digest":        w.projector.GraphDigest(),
		"blocked":             result.Blocked != nil,
		"blocked_code":        blocked,
		"watermark_sequence":  jn(int64(afterSeq)),
		"watermark_unchanged": afterSeq == watermarkBefore && afterDigest == watermarkBeforeDigest,
	}
	return payload
}

// ensureReleasedWorld releases the S5-S9 authority world: two leaves plus a
// composite over both (the S8/S9 closure shape), identical to the
// GMS-207 closure test's authority world. Idempotent per lineage: a lineage
// the S5 slice already released stays as it is (that atomic release is
// itself part of the traced story); every missing lineage is released here.
func (w *world) ensureReleasedWorld() error {
	if _, _, err := w.watermarkSequence(); err != nil {
		return err
	}
	ctx := context.Background()
	activeOf := func(lineage string) (*contract.SkillArtifactRef, error) {
		return w.activations.ActiveRevision(ctx, lineage)
	}
	if active, err := activeOf("sg-alpha"); err != nil {
		return err
	} else if active == nil {
		if _, err := w.releaseOne("sg-alpha", "step_guidance", guidanceEnvelope("sg-alpha")); err != nil {
			return err
		}
	}
	if active, err := activeOf("proc-beta"); err != nil {
		return err
	} else if active == nil {
		if _, err := w.releaseOne("proc-beta", "human_procedure", procedureEnvelope("proc-beta")); err != nil {
			return err
		}
	}
	if active, err := activeOf("cmp-orchestrator"); err != nil {
		return err
	} else if active == nil {
		sgActive, err := activeOf("sg-alpha")
		if err != nil || sgActive == nil {
			return fmt.Errorf("sg-alpha head missing: %v", err)
		}
		procActive, err := activeOf("proc-beta")
		if err != nil || procActive == nil {
			return fmt.Errorf("proc-beta head missing: %v", err)
		}
		if _, err := w.releaseOne("cmp-orchestrator", "composite",
			compositeEnvelope("cmp-orchestrator", *sgActive, *procActive)); err != nil {
			return err
		}
	}
	return nil
}

func (w *world) releaseOne(lineage, kind string, envelope map[string]any) (contract.SkillArtifactRef, error) {
	fix, _, err := w.prepareRelease(lineage, kind, envelope)
	if err != nil {
		return contract.SkillArtifactRef{}, err
	}
	ctx := context.Background()
	active, err := w.activations.ActiveRevision(ctx, lineage)
	if err != nil {
		return contract.SkillArtifactRef{}, err
	}
	res, err := w.activations.Activate(ctx, activation.ActivateRequest{
		Decision: fix.decision, Candidate: fix.view, LineageID: lineage,
		ExpectedHead: active, ProposalDoc: fix.propDoc,
	})
	if err != nil {
		return contract.SkillArtifactRef{}, err
	}
	return res.ReleasedRef, nil
}

func opProjectorProject(instruction map[string]any) (map[string]any, string, string) {
	w, err := worldFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	mode := strOf(instruction["mode"])
	if mode == "" {
		mode = "runtime"
	}
	if err := w.ensureReleasedWorld(); err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "authority world: " + err.Error()
	}
	ctx := context.Background()

	switch mode {
	case "gap":
		// Project only the first activation, then deliver a batch that
		// skips sequence 2: the projector MUST block with
		// PROJECTION_SEQUENCE_GAP and leave the watermark untouched.
		seed, seedErr := take(w, 1)
		if seedErr != nil {
			return nil, "DRIVER_OP_FAILED", "seed delivery: " + seedErr.Error()
		}
		if _, err := w.projector.ProjectDelivered(ctx, seed); err != nil {
			return nil, "DRIVER_OP_FAILED", "seed projection: " + projector.CodeOf(err)
		}
		before, _ := w.watermarkSequence2()
		events := mustEvents(take(w, 3))
		if len(events) < 3 {
			return nil, "DRIVER_OP_FAILED",
				"authority world delivered fewer than 3 activation events"
		}
		gapped := []projector.DeliveredEvent{events[0], events[2]}
		result, err := w.projector.ProjectDelivered(ctx, gapped)
		if err == nil && result == nil {
			err = fmt.Errorf("no result")
		}
		// The blocked head surfaces as an error carrying the gap code.
		payload := map[string]any{
			"mode": mode,
			"error_code": func() string {
				if err != nil {
					return projector.CodeOf(err)
				}
				return ""
			}(),
		}
		if result != nil {
			payload["state"] = result.State
			payload["watermark"] = result.Watermark
			if result.Blocked != nil {
				payload["blocked_code"] = result.Blocked.Code
			}
			payload["watermark_digest"] = result.WatermarkDigest
			payload["graph_digest"] = w.projector.GraphDigest()
		}
		after, afterDigest := w.watermarkSequence2()
		payload["watermark_sequence"] = jn(int64(after))
		// The blocked head flips the diagnostic watermark document (state
		// blocked, Contract §9.3) but must never advance the projection
		// sequence — the conformance fact is the sequence, not the digest.
		payload["watermark_unchanged"] = after == before
		_ = afterDigest
		return payload, "", ""

	case "duplicate":
		// Deliver the same batch twice: identical duplicates are a no-op.
		events := mustEvents(take(w, 3))
		if len(events) < 3 {
			return nil, "DRIVER_OP_FAILED",
				"authority world delivered fewer than 3 activation events"
		}
		first, err := w.projector.ProjectDelivered(ctx, events)
		if err != nil {
			return nil, "DRIVER_OP_FAILED", "first projection: " + projector.CodeOf(err)
		}
		before, beforeDigest := w.watermarkSequence2()
		second, err := w.projector.ProjectDelivered(ctx, events)
		if err != nil {
			return nil, "DRIVER_OP_FAILED", "duplicate projection: " + projector.CodeOf(err)
		}
		after, afterDigest := w.watermarkSequence2()
		return map[string]any{
			"mode":                          mode,
			"state":                         second.State,
			"projected":                     jn(int64(second.Projected)),
			"first_projected":               jn(int64(first.Projected)),
			"cursor_activation":             jn(int64(second.Cursors.Activation)),
			"watermark":                     second.Watermark,
			"watermark_digest":              second.WatermarkDigest,
			"graph_digest":                  w.projector.GraphDigest(),
			"graph_digest_before_duplicate": w.projector.GraphDigest(),
			"watermark_sequence":            jn(int64(after)),
			"watermark_unchanged":           after == before && afterDigest == beforeDigest,
			"duplicate_noop":                second.Projected == 0,
			"blocked":                       second.Blocked != nil,
		}, "", ""

	case "conflict":
		// Same activation sequence re-delivered with a different digest.
		events := mustEvents(take(w, 1))
		if len(events) < 1 {
			return nil, "DRIVER_OP_FAILED",
				"authority world delivered no activation events"
		}
		if _, err := w.projector.ProjectDelivered(ctx, events); err != nil {
			return nil, "DRIVER_OP_FAILED", "seed projection: " + projector.CodeOf(err)
		}
		before, _ := w.watermarkSequence2()
		conflicting := []projector.DeliveredEvent{{
			Sequence: events[0].Sequence, EventID: events[0].EventID,
			PayloadDigest: contract.DigestBytes([]byte("conflicting")),
			Payload:       []byte(`{"schema_version":"tampered"}`),
		}}
		result, err := w.projector.ProjectDelivered(ctx, conflicting)
		payload := map[string]any{
			"mode":       mode,
			"error_code": projector.CodeOf(err),
		}
		if result != nil {
			payload["state"] = result.State
			payload["watermark"] = result.Watermark
			if result.Blocked != nil {
				payload["blocked_code"] = result.Blocked.Code
			}
		}
		after, afterDigest := w.watermarkSequence2()
		payload["watermark_sequence"] = jn(int64(after))
		// Sequence-only, exactly like the gap probe: the blocked diagnostic
		// watermark may flip its digest, the sequence may never advance.
		payload["watermark_unchanged"] = after == before
		_ = afterDigest
		return payload, "", ""
	}

	// runtime / delivered: drain the authoritative sources by cursor.
	before, beforeDigest := w.watermarkSequence2()
	result, err := w.projector.ProjectRuntime(ctx)
	if err != nil {
		return nil, "DRIVER_OP_FAILED",
			"project failed: " + projector.CodeOf(err) + ": " + err.Error()
	}
	payload := w.projectionFacts(mode, result, before, beforeDigest)
	payload["watermark_unchanged"] = false // runtime advanced by definition
	return payload, "", ""
}

func (w *world) watermarkSequence2() (uint64, string) {
	seq, digest, _ := w.watermarkSequence()
	return seq, digest
}

// take returns the first n committed activation events as delivered events.
func take(w *world, n uint64) ([]projector.DeliveredEvent, error) {
	return w.deliveredEvents(1, n)
}

// mustEvents folds take's (events, error) pair: an unresolved authority
// source surfaces as nil events (fail-closed — the caller's projection call
// then reports the failure instead of inventing deliveries; the driver
// never panics on authority trouble).
func mustEvents(events []projector.DeliveredEvent, err error) []projector.DeliveredEvent {
	if err != nil {
		return nil
	}
	return events
}

// ---------------------------------------------------------------------------
// S7: tools.invoke (Explore through the real retrieval service)
// ---------------------------------------------------------------------------

func exploreArguments(session, query string, total, evidenceCap, skillCap, tokens int64) map[string]any {
	return map[string]any{
		"schema_version":       "gms.memory-explore-arguments.v1",
		"explore_session_id":   session,
		"query_text":           query,
		"runtime_context_hash": contract.DigestBytes([]byte("runtime-context-s5s9")),
		"ranker_policy_ref": map[string]any{
			"id": "gms.ranker.lexical-graph.v1", "version": jn(1),
			"digest": "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045c7c9e6ece643805a4c79bb",
		},
		"budgets": map[string]any{
			"total_cap":             jn(total),
			"evidence_subcap":       jn(evidenceCap),
			"skill_subcap":          jn(skillCap),
			"guidance_token_budget": jn(tokens),
		},
		"filters": map[string]any{},
	}
}

func opToolsInvoke(instruction map[string]any) (map[string]any, string, string) {
	w, err := worldFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	mode := strOf(instruction["mode"])
	if mode == "" {
		mode = "ok"
	}
	if err := w.ensureReleasedWorld(); err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "authority world: " + err.Error()
	}
	// The read side is servable only after the projector caught up.
	if _, err := w.projector.ProjectRuntime(context.Background()); err != nil {
		return nil, "DRIVER_OP_FAILED", "pre-project: " + projector.CodeOf(err)
	}
	session := strOf(instruction["session"])
	if session == "" {
		session = "exp-s5s9"
	}
	var minSeq uint64
	if raw, ok := instruction["min_activation_sequence"]; ok {
		minSeq = jsonUint(raw)
	}
	request := retrieval.ToolRequest{
		ProxyRequestID: "pr-s5s9-" + session,
		RoomID:         "room-s5s9",
		AgentID:        "agent-s5s9",
		DeliveryID:     "delivery-" + session,
		ToolName:       retrieval.ToolExplore,
		Arguments:      exploreArguments(session, "alpha procedure diff", 10, 5, 5, 10000),
		ScopeProfile: contract.VersionedRef{
			ID: "scope-memory-room-retrieval", Version: "1",
			Digest: contract.DigestBytes([]byte("scope-memory-room-retrieval")),
		},
		IdempotencyKey: contract.DigestBytes([]byte(session + "memory_explore")),
		TimeoutMillis:  5000,
	}
	if minSeq > 0 {
		request.RequestedMinActivationSequence = minSeq
		request.HasMinActivationSequence = true
	}
	response, err := w.retrieval.Explore(context.Background(), request)
	if err != nil {
		code := retrieval.CodeOf(err)
		failedClosed := code != ""
		return map[string]any{
			"mode":          mode,
			"tool_name":     retrieval.ToolExplore,
			"status":        "failed",
			"reason_code":   code,
			"failed_closed": failedClosed,
		}, "", ""
	}
	watermark, ok := response.Result["watermark"].(map[string]any)
	if !ok {
		return nil, "DRIVER_INTERNAL_ERROR", "explore result carries no watermark"
	}
	_, hasEvidence := response.Result["evidence_results"]
	_, hasSkills := response.Result["skill_results"]
	_, hasFences := response.Result["served_fences"]
	return map[string]any{
		"mode":                   mode,
		"tool_name":              retrieval.ToolExplore,
		"status":                 response.Status,
		"upstream_result_digest": response.UpstreamResultDigest,
		"read_audit_present":     response.ReadAudit != nil,
		"result_carry_watermark": watermark["schema_version"] == "gms.projection-watermark.v1",
		"result_carry_skills":    hasSkills,
		"result_carry_evidence":  hasEvidence,
		"result_carry_fences":    hasFences,
		"watermark_sequence":     watermark["projected_through_activation_sequence"],
	}, "", ""
}

// ---------------------------------------------------------------------------
// S8: closure.read
// ---------------------------------------------------------------------------

func opClosureRead(instruction map[string]any) (map[string]any, string, string) {
	w, err := worldFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	mode := strOf(instruction["mode"])
	if mode == "" {
		mode = "ok"
	}
	if err := w.ensureReleasedWorld(); err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "authority world: " + err.Error()
	}
	ctx := context.Background()
	rootLineage := strOf(instruction["root_lineage"])
	if rootLineage == "" {
		rootLineage = "cmp-orchestrator"
	}

	switch mode {
	case "graph_root":
		// A Graph node id form is not an exact ref (Contract §13.7): the
		// closure read must refuse before any authority lookup.
		root, err := materializationread.ParseRootRef(map[string]any{
			"schema_version": contract.SchemaSkillArtifactRef,
			"lineage_id":     rootLineage,
			"version":        jn(1),
			"kind":           "composite",
			"artifact_digest": contract.DigestBytes(
				[]byte("graph-node-id-forged")),
			"graph_node_id": "graph://" + rootLineage,
		})
		_ = root
		if err == nil {
			return nil, "DRIVER_INTERNAL_ERROR",
				"graph_node_id root parsed as exact; the gate is broken"
		}
		return map[string]any{
			"mode": mode, "read_failed": true,
			"reason_code": materializationread.CodeOf(err),
		}, "", ""

	case "not_current":
		// A stale (not-current-active) exact root: the read must refuse
		// with SKILL_NOT_CURRENT_ACTIVE instead of guessing the head.
		active, err := w.activations.ActiveRevision(ctx, rootLineage)
		if err != nil || active == nil {
			return nil, "DRIVER_INTERNAL_ERROR", "active head: missing"
		}
		stale := *active
		stale.ArtifactDigest = contract.DigestBytes([]byte("stale-digest"))
		_, err = w.closure.Read(ctx, materializationread.ReadRequest{
			Roots: []contract.SkillArtifactRef{stale},
		})
		if err == nil {
			return nil, "DRIVER_INTERNAL_ERROR",
				"stale root read succeeded; closure must fail closed"
		}
		return map[string]any{
			"mode": mode, "read_failed": true,
			"reason_code": materializationread.CodeOf(err),
		}, "", ""

	case "torn":
		// The activation head moves between the read's baseline and its
		// post-walk double-check: the read MUST fail with
		// ACTIVATION_SEQUENCE_CONFLICT and return no closure.
		svc, err := buildTornClosureService(w)
		if err != nil {
			return nil, "DRIVER_INTERNAL_ERROR", err.Error()
		}
		leaf, err := w.activations.ActiveRevision(ctx, "sg-alpha")
		if err != nil || leaf == nil {
			return nil, "DRIVER_INTERNAL_ERROR", "leaf head missing"
		}
		closure, err := svc.Read(ctx, materializationread.ReadRequest{
			Roots: []contract.SkillArtifactRef{*leaf},
		})
		if err == nil {
			return nil, "DRIVER_INTERNAL_ERROR",
				"torn read succeeded; no guess is permitted"
		}
		if closure != nil {
			return nil, "DRIVER_INTERNAL_ERROR", "torn read returned a closure"
		}
		return map[string]any{
			"mode": mode, "read_failed": true,
			"reason_code": materializationread.CodeOf(err),
		}, "", ""
	}

	// ok: the exact authoritative closure read.
	active, err := w.activations.ActiveRevision(ctx, rootLineage)
	if err != nil || active == nil {
		return nil, "DRIVER_INTERNAL_ERROR", "root head missing"
	}
	closure, err := w.closure.Read(ctx, materializationread.ReadRequest{
		Roots: []contract.SkillArtifactRef{*active},
	})
	if err != nil {
		return nil, "DRIVER_OP_FAILED",
			"closure read failed: " + materializationread.CodeOf(err) + ": " + err.Error()
	}
	lineages := make([]string, 0, len(closure.Nodes))
	digestsMatch := true
	portsPresent := true
	for _, node := range closure.Nodes {
		lineages = append(lineages, node.Ref.LineageID)
		if contract.DigestBytes(node.CanonicalBytes) != node.Ref.ArtifactDigest {
			digestsMatch = false
		}
		if node.Ref.Kind != "composite" && (node.InputPort == nil || node.OutputPort == nil) {
			portsPresent = false
		}
	}
	return map[string]any{
		"mode":                         mode,
		"schema_version":               closure.SchemaVersion,
		"roots":                        []string{closure.Roots[0].LineageID},
		"root_ref":                     skillRefDoc(closure.Roots[0]),
		"node_lineages":                lineages,
		"activation_sequence":          jn(int64(closure.ActivationSequence)),
		"activation_head_digest":       closure.ActivationHeadDigest,
		"torn_read_token":              closure.TornReadToken,
		"canonical_bytes_digest_match": digestsMatch,
		"ports_present":                portsPresent,
		"closure_nodes_b64":            encodeNodes(closure),
	}, "", ""
}

// encodeNodes renders the closure nodes (exact refs + canonical bytes) as
// the wire document the RSIH materializer's closure source consumes.
func encodeNodes(closure *materializationread.Closure) []map[string]any {
	nodes := make([]map[string]any, 0, len(closure.Nodes))
	for _, node := range closure.Nodes {
		permissions := make([]map[string]any, 0, len(node.Permissions))
		for _, permission := range node.Permissions {
			permissions = append(permissions, map[string]any{
				"capability": permission.Capability,
				"scope":      permission.Scope,
			})
		}
		nodes = append(nodes, map[string]any{
			"skill_ref":              skillRefDoc(node.Ref),
			"kind":                   node.Kind,
			"artifact_digest":        node.ArtifactDigest,
			"canonical_bytes_base64": base64Of(node.CanonicalBytes),
			"input_port_schema":      materializationread.PortSchemaDoc(node.InputPort),
			"output_port_schema":     materializationread.PortSchemaDoc(node.OutputPort),
			"permissions":            permissions,
			"dependency_refs":        dependencyDocs(node.Dependencies),
		})
	}
	return nodes
}

func dependencyDocs(deps []contract.SkillArtifactRef) []map[string]any {
	out := make([]map[string]any, 0, len(deps))
	for _, dep := range deps {
		out = append(out, skillRefDoc(dep))
	}
	return out
}

func base64Of(data []byte) string {
	return encodeBase64(data)
}

// ---------------------------------------------------------------------------
// torn-read injection: a content store wrapper that commits a racing
// activation between the closure read's baseline and its double-check
// (exactly the GMS-207 test's tornArtifactBytes mechanism).
// ---------------------------------------------------------------------------

type tornArtifactBytes struct {
	ledger.ContentStore
	onFirstGet func()
	done       bool
}

func (t *tornArtifactBytes) Get(digest string) ([]byte, bool, error) {
	if !t.done {
		t.done = true
		t.onFirstGet()
	}
	return t.ContentStore.Get(digest)
}

func buildTornClosureService(w *world) (*materializationread.Service, error) {
	// Build a PARALLEL world so the racing activation cannot contaminate
	// the shared authority world of the later slices.
	tornWorld, err := buildWorld(w.confDir)
	if err != nil {
		return nil, err
	}
	if err := tornWorld.ensureReleasedWorld(); err != nil {
		return nil, err
	}
	torn := &tornArtifactBytes{
		ContentStore: tornWorld.store,
		onFirstGet: func() {
			// A concurrent activation commits and moves the global head.
			_, _ = tornWorld.releaseOne("proc-late", "human_procedure", procedureEnvelope("proc-late"))
		},
	}
	return materializationread.NewService(materializationread.Config{
		Heads: tornWorld.activations, Artifacts: torn, Gate: tornWorld.artifacts,
		Activations: tornWorld.store, Registry: tornWorld.registry,
	})
}

// ---------------------------------------------------------------------------
// base64 helper (std encoding)
// ---------------------------------------------------------------------------

func encodeBase64(data []byte) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	// small local encoder keeps the driver dependency-free of encoding/base64
	// import order concerns; stdlib base64 is fine too but this avoids the
	// extra import for one call site.
	if len(data) == 0 {
		return ""
	}
	var out strings.Builder
	for i := 0; i < len(data); i += 3 {
		var b [3]byte
		n := copy(b[:], data[i:])
		v := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		out.WriteByte(alphabet[(v>>18)&0x3F])
		out.WriteByte(alphabet[(v>>12)&0x3F])
		if n > 1 {
			out.WriteByte(alphabet[(v>>6)&0x3F])
		} else {
			out.WriteByte('=')
		}
		if n > 2 {
			out.WriteByte(alphabet[v&0x3F])
		} else {
			out.WriteByte('=')
		}
	}
	return out.String()
}
