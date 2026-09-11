// Package merge tests: GMS-208 binary Step Guidance merge subsystem
// (plan-pinned TDD).
//
// Red #1 TestBinaryMergeCanonicalDedupReplayAndDerivedActivation
// Red #2 TestMergeSourceHeadChangeIsStaleWithoutPartialActivation
// plus the conformance-fixture alignment ($FIX/merge/*), the fail-closed
// gates (below threshold, graded initiation, blocking conflict, cross-kind,
// n-way, uncommitted evidence, critical regression) and the recorded q29b
// merge-family bilateral replay.
package merge

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluator"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Ports fit the sibling products (compile-time, never drifts).
// ---------------------------------------------------------------------------

var (
	_ similarity.EvidenceResolver       = (*stubEvidence)(nil)
	_ candidate.EvidenceResolver        = (*stubEvidence)(nil)
	_ activation.CandidateView          = (*stubCandidateView)(nil)
	_ activation.ReleaseDecision        = stubDecision{}
	_ activation.DeactivationAuthorizer = allowAuthorizer{}
	_ evaluatorLikeDecision             = (*evaluator.Decision)(nil)
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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

// allowAuthorizer permits the ORDINARY revision flow (deactivate -> v2)
// used to move a source head in the stale tests. The merge service itself
// never calls it: Test 1 pins that the whole M@1 write set contains no
// deactivation events at all.
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

type harness struct {
	t             *testing.T
	registry      *ledger.ContractReasonRegistry
	gates         *validation.Gates
	stateDir      string
	confDir       string
	store         *ledger.MemoryStore
	mgr           ledger.TxManager
	proposals     *proposal.Service
	activations   *activation.ActivationService
	candidates    *candidate.BindingService
	artifacts     *artifact.Service
	replaySvc     *replay.Service
	evaluatorSvc  *evaluator.Evaluator
	similaritySvc *similarity.Service
	evidence      *stubEvidence
	svc           *Service
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
	candidates, err := candidate.NewBindingService(candidate.Config{
		Store: store, Tx: mgr, Registry: registry, Artifacts: artifacts, Proposals: proposals, Evidence: evidence,
	})
	if err != nil {
		t.Fatalf("candidate.NewBindingService: %v", err)
	}
	replaySvc, err := replay.NewService(gates, policy)
	if err != nil {
		t.Fatalf("replay.NewService: %v", err)
	}
	evaluatorSvc, err := evaluator.New(gates)
	if err != nil {
		t.Fatalf("evaluator.New: %v", err)
	}
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
	svc, err := NewService(Config{
		Gates: gates, Store: store, Tx: mgr, Registry: registry,
		StateDir:   filepath.Join(dir, "schema", "state"),
		Similarity: similaritySvc, Evidence: evidence, Artifacts: artifacts,
		Candidates: candidates, Replay: replaySvc, Evaluator: evaluatorSvc,
		Heads: activations,
	})
	if err != nil {
		t.Fatalf("merge.NewService: %v", err)
	}
	return &harness{
		t:             t,
		registry:      registry,
		gates:         gates,
		stateDir:      filepath.Join(dir, "schema", "state"),
		confDir:       dir,
		store:         store,
		mgr:           mgr,
		proposals:     proposals,
		activations:   activations,
		candidates:    candidates,
		artifacts:     artifacts,
		replaySvc:     replaySvc,
		evaluatorSvc:  evaluatorSvc,
		similaritySvc: similaritySvc,
		evidence:      evidence,
		svc:           svc,
	}
}

// serviceOver re-wires the merge service over a store adapter (sneak
// injection at the ledger port seam); the authoritative state stays shared
// through the wrapped MemoryStore.
func (h *harness) serviceOver(store ledger.Store) *Service {
	h.t.Helper()
	mgr, err := ledger.NewManager(store, h.registry)
	if err != nil {
		h.t.Fatalf("manager over adapter: %v", err)
	}
	similarityOver, err := similarity.NewService(similarity.Config{
		Gates: h.gates, Store: store, Tx: mgr, Registry: h.registry,
		Evidence: h.evidence, Heads: h.activations,
	})
	if err != nil {
		h.t.Fatalf("similarity over adapter: %v", err)
	}
	svc, err := NewService(Config{
		Gates: h.gates, Store: store, Tx: mgr, Registry: h.registry,
		StateDir: h.stateDir, Similarity: similarityOver, Evidence: h.evidence,
		Artifacts: h.artifacts, Candidates: h.candidates, Replay: h.replaySvc,
		Evaluator: h.evaluatorSvc, Heads: h.activations,
	})
	if err != nil {
		h.t.Fatalf("merge over adapter: %v", err)
	}
	return svc
}

// ---------------------------------------------------------------------------
// Source-skill release (the ordinary GMS-204 pipeline).
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

// committedEvidence mints a committed observation evidence ref.
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
			"action": map[string]any{
				"guidance":       "Follow the " + lineage + " " + id + " procedure",
				"failure_action": "fallback",
				"evidence_refs":  []any{evidenceDoc(ev)},
			},
			"future": map[string]any{
				"expected_outcome":  "context recorded",
				"critical_steps":    []any{},
				"final_task_impact": "task completes with recorded context",
			},
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

// releaseSkill drives one ordinary GMS-204 release of a step_guidance
// envelope into its lineage and returns the now-active §7.3 ref. Each call
// mints a fresh proposal/candidate (a revision: the head CAS advances).
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
	// Commit the candidate binding through the ledger (committed shape).
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

// reviseSkill moves a source head through the ORDINARY revision flow
// (authorized deactivation of the current active revision, then a fresh
// release), producing the next version of the lineage.
func (h *harness) reviseSkill(t *testing.T, lineage string, body []byte) contract.SkillArtifactRef {
	t.Helper()
	ctx := context.Background()
	current, err := h.activations.ActiveRevision(ctx, lineage)
	if err != nil || current == nil {
		t.Fatalf("revise %s: no current active head (err %v)", lineage, err)
	}
	if _, err := h.activations.Deactivate(ctx, activation.DeactivateRequest{
		LineageID: lineage, SkillRef: *current,
		Authorization: contract.VersionedRef{ID: "auth-revise-" + lineage, Version: "1", Digest: contract.DigestBytes([]byte("auth-revise-" + lineage))},
	}); err != nil {
		t.Fatalf("deactivate %s v%s: %v (code %s)", lineage, current.Version, err, activation.CodeOf(err))
	}
	return h.releaseSkill(t, lineage, body)
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

// ---------------------------------------------------------------------------
// Merge-chain helpers
// ---------------------------------------------------------------------------

type sourcePair struct {
	a, b       contract.SkillArtifactRef
	envelopeA  map[string]any
	envelopeB  map[string]any
	assessment *similarity.Assessment
}

// releasePair releases sg-alpha and sg-beta step_guidance sources (alpha
// has no recovery branch; beta's second branch is the recovery branch) and
// assesses them in the auto-merge band.
func (h *harness) releasePair(t *testing.T, scoreMicros int64, initiator string) (*sourcePair, *Proposal) {
	t.Helper()
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"direct-answer", "recover-from-gap"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	b := h.releaseSkill(t, "sg-beta", bodyB)

	supportA := h.committedEvidence("ev-assess-a", "observation")
	supportB := h.committedEvidence("ev-assess-b", "observation")
	assessment, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID:           "sa-0001",
		Sources:                []contract.SkillArtifactRef{b, a}, // deliberately reversed: canonicalized
		ScoreMicros:            scoreMicros,
		ApplicabilityOverlap:   "partial",
		PermissionCompatible:   true,
		FeatureRecordRefs:      []contract.VersionedRef{{ID: "feature-lex-0001", Version: "1", Digest: contract.DigestBytes([]byte("feature-lex-0001"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{supportA, supportB},
		AssessorRef:            contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	})
	if err != nil {
		t.Fatalf("assess pair: %v (code %s)", err, similarity.CodeOf(err))
	}
	pair := &sourcePair{a: a, b: b, assessment: assessment}
	prop := h.proposeOverPair(t, pair, "mp-0001", initiator)
	return pair, prop
}

// proposeOverPair builds and commits one §7.22 proposal over the pair.
func (h *harness) proposeOverPair(t *testing.T, pair *sourcePair, proposalID, initiator string) *Proposal {
	t.Helper()
	prop, err := h.svc.Propose(context.Background(), h.proposeInput(pair, proposalID, initiator))
	if err != nil {
		t.Fatalf("propose %s: %v (code %s)", proposalID, err, CodeOf(err))
	}
	return prop
}

func (h *harness) proposeInput(pair *sourcePair, proposalID, initiator string) ProposeInput {
	evA := pair.assessment.Sources()[0]
	evB := pair.assessment.Sources()[1]
	supportA := h.committedEvidence("ev-set-a", "observation")
	supportB := h.committedEvidence("ev-set-b", "observation")
	claimA := contract.VersionedRef{ID: "claim-alpha-0007", Version: "1", Digest: contract.DigestBytes([]byte("claim-alpha-0007"))}
	claimB := contract.VersionedRef{ID: "claim-beta-0004", Version: "1", Digest: contract.DigestBytes([]byte("claim-beta-0004"))}
	policy := h.similaritySvc.Policy()
	return ProposeInput{
		ProposalID: proposalID,
		Assessment: pair.assessment.Ref(),
		EvidenceSets: []SourceEvidenceSet{
			{
				Source: evA, SupportingEvidenceRefs: []contract.EvidenceRef{supportA},
				SuccessPathRefs: []contract.VersionedRef{{ID: "path-a-success", Version: "1", Digest: contract.DigestBytes([]byte("path-a-success"))}},
				BranchClaimRefs: []contract.VersionedRef{claimA},
			},
			{
				Source: evB, SupportingEvidenceRefs: []contract.EvidenceRef{supportB},
				SuccessPathRefs: []contract.VersionedRef{{ID: "path-b-success", Version: "1", Digest: contract.DigestBytes([]byte("path-b-success"))}},
				BranchClaimRefs: []contract.VersionedRef{claimB},
			},
		},
		Conflicts: []map[string]any{{
			"conflict_id":           "cf-0001",
			"category":              "contradictory_action",
			"semantic_location":     "/body/branches/0/action/guidance",
			"source_a_claim_ref":    vrefDoc(claimA.ID, claimA.Digest),
			"source_b_claim_ref":    vrefDoc(claimB.ID, claimB.Digest),
			"applicability_overlap": "partial",
			"resolution": map[string]any{
				"action":        "preserve_as_branches",
				"evidence_refs": []any{evidenceDoc(h.committedEvidence("ev-cf-0001", "observation"))},
				"rationale":     "both branches retained with disjoint predicates",
			},
			"blocking": false,
		}},
		Policies: PolicyRefs{
			SimilarityPolicyRef:  policy.Ref,
			MergePolicyRef:       contract.VersionedRef{ID: "policy.merge.v1", Version: "1", Digest: contract.DigestBytes([]byte("policy.merge.v1"))},
			ValidationProfileRef: contract.VersionedRef{ID: "profile.validation.v1", Version: "1", Digest: contract.DigestBytes([]byte("profile.validation.v1"))},
			ReplayProfileRef:     contract.VersionedRef{ID: "rsih.replay-profile", Version: "1", Digest: contract.DigestBytes([]byte("rsih.replay-profile"))},
			ReleaseRuleRef:       contract.VersionedRef{ID: "policy.release-rule.v1", Version: "1", Digest: contract.DigestBytes([]byte("release-rule"))},
			UtilityComparatorRef: contract.VersionedRef{ID: "policy.utility-comparator.v1", Version: "1", Digest: contract.DigestBytes([]byte("comparator"))},
		},
		Origin: Origin{InitiatorType: initiator, InitiatorRef: "model://primary", RequestRef: "req-0042"},
	}
}

// driveToActivationPending walks the happy lifecycle proposed -> ... ->
// activation_pending and returns the bound candidate.
func (h *harness) driveToActivationPending(t *testing.T, proposalID string) contract.CandidateArtifactRef {
	t.Helper()
	ctx := context.Background()
	if _, state, err := h.svc.Admit(ctx, proposalID); err != nil || state != StateAdmitted {
		t.Fatalf("admit %s: state %s err %v (code %s)", proposalID, state, err, CodeOf(err))
	}
	synth, err := h.svc.Synthesize(ctx, proposalID)
	if err != nil {
		t.Fatalf("synthesize %s: %v (code %s)", proposalID, err, CodeOf(err))
	}
	if err := h.svc.Validate(ctx, proposalID); err != nil {
		t.Fatalf("validate %s: %v (code %s)", proposalID, err, CodeOf(err))
	}
	eval, err := h.svc.Evaluate(ctx, proposalID, h.evaluateInput(t, proposalID, synth.Candidate))
	if err != nil {
		t.Fatalf("evaluate %s: %v (code %s)", proposalID, err, CodeOf(err))
	}
	if eval.State != StateActivationPending {
		t.Fatalf("evaluate left %s in %s (outcome %s)", proposalID, eval.State, eval.Decision.Outcome())
	}
	return synth.Candidate
}

// evaluateInput assembles the bilateral §7.10 request + plan + runs whose
// U1 outcome is a strict candidate improvement: alpha covers [success],
// beta adds recovery; M (branch union) covers [success, failure, recovery]
// while the alpha-baselined overlap packet only fails.
func (h *harness) evaluateInput(t *testing.T, proposalID string, cand contract.CandidateArtifactRef) EvaluateInput {
	t.Helper()
	prop, err := h.svc.LoadProposal(proposalID, "")
	if err != nil {
		t.Fatalf("load proposal %s for evaluate: %v", proposalID, err)
	}
	pair := prop.Sources()
	candDoc := map[string]any{
		"schema_version": cand.SchemaVersion,
		"candidate_id":   cand.CandidateID,
		"kind":           cand.Kind,
		"body_digest":    cand.BodyDigest,
		"origin_type":    cand.OriginType,
		"origin_ref":     vrefDoc(cand.OriginRef.ID, cand.OriginRef.Digest),
	}
	requestDoc := map[string]any{
		"schema_version":      "gms.replay-request.v1",
		"replay_request_id":   "replay-" + proposalID,
		"candidate_ref":       candDoc,
		"baseline_skill_refs": []any{similarity.SkillRefDoc(pair[0]), similarity.SkillRefDoc(pair[1])},
		"fixture_set_refs":    []any{vrefDoc("fixture-merge-0001", contract.DigestBytes([]byte("fixture-merge-0001")))},
		"segment_refs": []any{map[string]any{
			"schema_version": "host.segment-ref.v1", "room_id": "room-1",
			"segment_id": "seg-merge-0001", "segment_version": jn(1),
			"segment_digest":    contract.DigestBytes([]byte("seg-merge-0001")),
			"evidence_seal_ref": vrefDoc("evseal-merge", contract.DigestBytes([]byte("evseal-merge"))),
			"path_seal_ref":     vrefDoc("pathseal-merge", contract.DigestBytes([]byte("pathseal-merge"))),
		}},
		"replay_profile_ref":  vrefDoc("rsih.replay-profile", contract.DigestBytes([]byte("rsih.replay-profile"))),
		"runtime_adapter_ref": vrefDoc("rsih.fake-runtime-adapter", contract.DigestBytes([]byte("rsih.fake-runtime-adapter"))),
		"mode":                "causal_evaluation",
		"idempotency_key":     contract.DigestBytes([]byte("merge-replay-idem-" + proposalID)),
		"required_source_heads": []any{
			similarity.SkillRefDoc(pair[0]), similarity.SkillRefDoc(pair[1]),
		},
	}
	packet := func(id string, side string, status string, reason string, artifact map[string]any, domains []string, critical bool) (replay.Packet, replay.RunOutput) {
		digest := contract.DigestBytes([]byte("packet:" + id + ":" + side))
		return replay.Packet{
				ID: id, Side: side, PathDomains: domains, Critical: critical,
				ExpectedRunStatus: status, ExpectedReasonCode: reason,
				ExpectedOutputDigest: digest, ExpectedArtifactRef: artifact,
			}, replay.RunOutput{
				PacketID: id, Side: side, RunStatus: status, ReasonCode: reason,
				OutputDigest: digest, CostUnits: 2, ArtifactRef: artifact,
			}
	}
	var packets []replay.Packet
	var runs []replay.RunOutput
	add := func(p replay.Packet, r replay.RunOutput) { packets = append(packets, p); runs = append(runs, r) }
	alphaDoc, betaDoc := similarity.SkillRefDoc(pair[0]), similarity.SkillRefDoc(pair[1])
	// a-only family (source_a, baseline A): both sides pass the success path.
	p, r := packet("pk-a-only", replay.SideBaseline, "succeeded", "", alphaDoc, []string{"success"}, true)
	add(p, r)
	p, r = packet("pk-a-only", replay.SideCandidate, "succeeded", "", candDoc, []string{"success"}, true)
	add(p, r)
	// b-only family (source_b, baseline B): both sides pass.
	p, r = packet("pk-b-only", replay.SideBaseline, "succeeded", "", betaDoc, []string{"success"}, true)
	add(p, r)
	p, r = packet("pk-b-only", replay.SideCandidate, "succeeded", "", candDoc, []string{"success"}, true)
	add(p, r)
	// overlap family (overlap, baseline A): the baseline alpha has no
	// recovery branch and fails semantically; the merged candidate covers
	// failure->recovery and passes with a recovery domain.
	p, r = packet("pk-overlap", replay.SideBaseline, "failed", "SKILL_BRANCH_UNCOVERED", alphaDoc, []string{"failure", "recovery"}, false)
	add(p, r)
	p, r = packet("pk-overlap", replay.SideCandidate, "succeeded", "", candDoc, []string{"failure", "recovery"}, false)
	add(p, r)
	plan := &replay.Plan{Families: []replay.Family{
		{Name: "a-only", Domain: replay.DomainSourceA, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: alphaDoc},
		{Name: "b-only", Domain: replay.DomainSourceB, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: betaDoc},
		{Name: "overlap", Domain: replay.DomainOverlap, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: alphaDoc},
	}}
	for i := range plan.Families {
		for _, pkt := range packets {
			if famOf(pkt.ID) == plan.Families[i].Name {
				plan.Families[i].Packets = append(plan.Families[i].Packets, pkt)
			}
		}
	}
	return EvaluateInput{
		RequestDoc: requestDoc,
		Plan:       plan,
		Runs:       runs,
		Comparator: evaluator.Comparator{
			Ref:                    contract.VersionedRef{ID: "policy.utility-comparator.v1", Version: "1", Digest: contract.DigestBytes([]byte("comparator"))},
			CostBudgetUnits:        1000,
			CriticalDomains:        []string{replay.DomainSourceA, replay.DomainSourceB},
			ComparisonMode:         evaluator.ModeRate,
			MinImprovementRatioNum: 1,
			MinImprovementRatioDen: 1,
		},
	}
}

func famOf(packetID string) string {
	switch packetID {
	case "pk-a-only":
		return "a-only"
	case "pk-b-only":
		return "b-only"
	default:
		return "overlap"
	}
}

// stateOf reads the derived lifecycle state.
func (h *harness) stateOf(t *testing.T, proposalID string) string {
	t.Helper()
	state, _, err := h.svc.CurrentState(proposalID)
	if err != nil {
		t.Fatalf("current state of %s: %v", proposalID, err)
	}
	return state
}

// ---------------------------------------------------------------------------
// GMS-208 TDD Red #1 (plan-pinned)
// ---------------------------------------------------------------------------

// TestBinaryMergeCanonicalDedupReplayAndDerivedActivation pins the full
// binary merge chain (Contract §5.5, §9.4, §10.3-§10.4; GMS §12.10-§12.13):
//
//   - canonical pair: A+B and B+A address the same assessment, pair key and
//     proposal group (§6.3);
//   - dedup: a second proposal over the same group loses the winner CAS and
//     becomes an exact duplicate pointing at the canonical winner (§5.5.8);
//   - replay: the bilateral A-only/B-only/overlap families canonicalize
//     through the GMS-203 pipeline and the U1 evaluator accepts the strict
//     envelope improvement (source-head expectations intact);
//   - derived activation: M@1 commits in ONE transaction with TWO
//     derived_from refs and no supersedes — A and B stay active heads of
//     their own lineages (retain), both merge-source heads advance onto the
//     activation, and the proposal ends in released.
func TestBinaryMergeCanonicalDedupReplayAndDerivedActivation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// --- Canonical pair: reversed assessment input normalizes.
	pair, prop := h.releasePair(t, 970_000, "model")
	if got := pair.assessment.Sources(); got[0].LineageID != "sg-alpha" || got[1].LineageID != "sg-beta" {
		t.Fatalf("canonical order = [%s, %s], want [sg-alpha, sg-beta] (§6.3 byte order)", got[0].LineageID, got[1].LineageID)
	}
	if prop.PairKey() != pair.assessment.PairKey() {
		t.Fatalf("proposal pair key %s != assessment pair key %s", prop.PairKey(), pair.assessment.PairKey())
	}
	if prop.Sources() != pair.assessment.Sources() {
		t.Fatalf("proposal pair differs from the assessment pair")
	}

	// --- Admission: winner pins the group and both source heads.
	if _, state, err := h.svc.Admit(ctx, "mp-0001"); err != nil || state != StateAdmitted {
		t.Fatalf("admit mp-0001: state %s err %v (code %s)", state, err, CodeOf(err))
	}
	wseq, wdigest, wok, err := h.store.GetHead(ledger.HeadMergeGroupWinner, prop.GroupKey())
	if err != nil || !wok || wseq != 1 || wdigest != prop.Digest() {
		t.Fatalf("group-winner head = (%d, %s, ok=%v), want (1, %s)", wseq, wdigest, wok, prop.Digest())
	}
	for _, source := range prop.Sources() {
		pseq, pdigest, pok, err := h.store.GetHead(ledger.HeadMergeSource, mergeSourceHeadKey(source.LineageID, prop.GroupKey()))
		if err != nil || !pok || pseq != 1 {
			t.Fatalf("merge-source head of %s = (%d, %s, ok=%v)", source.LineageID, pseq, pdigest, pok)
		}
		payload, ok, _ := h.store.Get(pdigest)
		if !ok {
			t.Fatalf("merge-source expectation of %s unresolved", source.LineageID)
		}
		value, _ := contract.ParseJSONStrict(payload)
		pinObj, _ := contract.AsObject(value)
		if schema, _ := contract.AsString(pinObj["schema_version"]); schema != SchemaMergeSourceExpectation {
			t.Fatalf("merge-source pin schema = %s", schema)
		}
		head, err := contract.ParseSkillArtifactRef(asObject(pinObj["expected_head"]))
		if err != nil || head != source {
			t.Fatalf("merge-source pin of %s froze %+v", source.LineageID, head)
		}
	}

	// --- Dedup: the second proposal over the same group is an exact
	// duplicate pointing at the canonical winner (single in-flight, M6).
	second := h.proposeOverPair(t, pair, "mp-0002", "model")
	if second.GroupKey() != prop.GroupKey() {
		t.Fatalf("second proposal group %s != winner group %s", second.GroupKey(), prop.GroupKey())
	}
	_, _, err = h.svc.Admit(ctx, "mp-0002")
	if CodeOf(err) != ReasonMergeGroupInFlight {
		t.Fatalf("second admit: want MERGE_GROUP_IN_FLIGHT, got %v (code %s)", err, CodeOf(err))
	}
	if state := h.stateOf(t, "mp-0002"); state != StateDuplicate {
		t.Fatalf("mp-0002 state = %s, want duplicate", state)
	}
	entries, err := h.store.Snapshot(ledger.LedgerMergeEvent, "mp-0002")
	if err != nil || len(entries) != 2 {
		t.Fatalf("duplicate stream has %d events (err %v), want exactly none->proposed + proposed->duplicate", len(entries), err)
	}
	dupPayload, _, _ := h.store.Get(entries[1].PayloadDigest)
	dupValue, _ := contract.ParseJSONStrict(dupPayload)
	dupObj, _ := contract.AsObject(dupValue)
	refs, _ := contract.AsArray(dupObj["record_refs"])
	if len(refs) != 1 {
		t.Fatalf("duplicate event record_refs = %v, want the canonical winner ref", refs)
	}
	winnerRef, _ := contract.AsObject(refs[0])
	if d, _ := contract.AsString(winnerRef["digest"]); d != prop.Digest() {
		t.Fatalf("duplicate event points at %s, want the winner %s", d, prop.Digest())
	}
	// The loser never touched the winner's heads.
	if _, wd, _, _ := h.store.GetHead(ledger.HeadMergeGroupWinner, prop.GroupKey()); wd != prop.Digest() {
		t.Fatalf("duplicate loser disturbed the group-winner head: %s", wd)
	}

	// --- Synthesis + validation + bilateral replay + U1 accept.
	cand := h.driveToActivationPending(t, "mp-0001")

	// The synthesized body preserves every source branch under its prefix.
	committed, ok, err := h.store.Get(cand.BodyDigest)
	if err != nil || !ok {
		t.Fatalf("merged body unresolved: %v", err)
	}
	mergedValue, _ := contract.ParseJSONStrict(committed)
	merged, _ := contract.AsObject(mergedValue)
	mergedBody, _ := contract.AsObject(merged["body"])
	mergedBranches, _ := contract.AsArray(mergedBody["branches"])
	ids := map[string]bool{}
	for _, item := range mergedBranches {
		branch, _ := contract.AsObject(item)
		id, _ := contract.AsString(branch["branch_id"])
		ids[id] = true
	}
	for _, want := range []string{"a-collect-context", "b-direct-answer", "b-recover-from-gap"} {
		if !ids[want] {
			t.Fatalf("merged body lost prefixed branch %q (has %v)", want, ids)
		}
	}
	// Conflict claims stay marked inside causal_context.
	causal, _ := contract.AsObject(mergedBody["causal_context"])
	claims, _ := contract.AsArray(causal["claim_refs"])
	claimIDs := map[string]bool{}
	for _, item := range claims {
		ref, _ := contract.AsObject(item)
		id, _ := contract.AsString(ref["id"])
		claimIDs[id] = true
	}
	if !claimIDs["claim-alpha-0007"] || !claimIDs["claim-beta-0004"] {
		t.Fatalf("conflict claim refs not folded into causal_context: %v", claimIDs)
	}

	// --- M@1 derived activation.
	res, err := h.svc.Activate(ctx, "mp-0001", ActivateInput{
		Decision:  evaluatorDecision(t, h, "mp-0001"),
		LineageID: "sg-merged",
	})
	if err != nil {
		t.Fatalf("activate M@1: %v (code %s)", err, CodeOf(err))
	}
	if res.ReleasedRef.LineageID != "sg-merged" || res.ReleasedRef.Version != "1" {
		t.Fatalf("released ref = %+v, want sg-merged v1 (M@1)", res.ReleasedRef)
	}
	if res.ReleasedRef.ArtifactDigest != cand.BodyDigest || res.ReleasedRef.Kind != "step_guidance" {
		t.Fatalf("released ref digest/kind = %s/%s (byte-equality with the candidate body)", res.ReleasedRef.ArtifactDigest, res.ReleasedRef.Kind)
	}
	if state := h.stateOf(t, "mp-0001"); state != StateReleased {
		t.Fatalf("mp-0001 state = %s, want released", state)
	}

	// The §7.13 event: exactly TWO derived_from refs, no supersedes, and
	// M is the active head of the new lineage.
	actEntries, _ := h.store.Snapshot(ledger.LedgerActivation, "")
	var mergeEvent map[string]any
	for _, entry := range actEntries {
		payload, _, _ := h.store.Get(entry.PayloadDigest)
		value, _ := contract.ParseJSONStrict(payload)
		obj, _ := contract.AsObject(value)
		if lid, _ := contract.AsString(obj["lineage_id"]); lid == "sg-merged" {
			mergeEvent = obj
		}
	}
	if mergeEvent == nil {
		t.Fatalf("no activation event for the derived lineage sg-merged")
	}
	if err := h.gates.ValidateInstance(mergeEvent, SchemaActivationEvent); err != nil {
		t.Fatalf("M@1 activation event invalid against the authority schema: %v", err)
	}
	derived, _ := contract.AsArray(mergeEvent["derived_from_refs"])
	if len(derived) != 2 {
		t.Fatalf("derived_from_refs = %v, want exactly the two sources", derived)
	}
	for i, source := range prop.Sources() {
		got, err := contract.ParseSkillArtifactRef(asObject(derived[i]))
		if err != nil || got != source {
			t.Fatalf("derived_from_refs[%d] = %+v (err %v), want %+v", i, got, err, source)
		}
	}
	if _, present := mergeEvent["supersedes"]; present {
		t.Fatalf("M@1 event carries a supersedes field (retain: sources are never superseded)")
	}
	if strings.Contains(strings.ToLower(fmt.Sprint(mergeEvent)), "deactivate") {
		t.Fatalf("M@1 write set mentions deactivation: %v", mergeEvent)
	}

	// A and B stay retained active heads of their own lineages.
	for _, source := range prop.Sources() {
		active, err := h.activations.ActiveRevision(ctx, source.LineageID)
		if err != nil || active == nil || *active != source {
			t.Fatalf("source %s no longer active: %+v (err %v) — retain is mandatory", source.LineageID, active, err)
		}
	}
	mergedActive, err := h.activations.ActiveRevision(ctx, "sg-merged")
	if err != nil || mergedActive == nil || *mergedActive != res.ReleasedRef {
		t.Fatalf("sg-merged active head = %+v (err %v)", mergedActive, err)
	}

	// Both merge-source heads advanced onto the activation event digest.
	for _, source := range prop.Sources() {
		pseq, pdigest, pok, err := h.store.GetHead(ledger.HeadMergeSource, mergeSourceHeadKey(source.LineageID, prop.GroupKey()))
		if err != nil || !pok || pseq != 2 || pdigest != res.EventDigest {
			t.Fatalf("merge-source head of %s = (%d, %s), want (2, %s)", source.LineageID, pseq, pdigest, res.EventDigest)
		}
	}
	// The group-winner head released onto the released event (a new attempt
	// over the same pair may win again).
	gseq, gdigest, _, _ := h.store.GetHead(ledger.HeadMergeGroupWinner, prop.GroupKey())
	if gseq != 2 || gdigest == prop.Digest() {
		t.Fatalf("group-winner head = (%d, %s), want released off the in-flight proposal", gseq, gdigest)
	}

	// No deactivation events exist anywhere (retain).
	for _, entry := range actEntries {
		payload, _, _ := h.store.Get(entry.PayloadDigest)
		value, _ := contract.ParseJSONStrict(payload)
		obj, _ := contract.AsObject(value)
		if et, _ := contract.AsString(obj["event_type"]); et != "activate" {
			t.Fatalf("activation stream holds event_type %q (retain forbids deactivation)", et)
		}
	}

	// Candidate Runtime leakage: the bound candidate ref has the closed
	// §7.4 field set only (a runtime field would fail the strict parser).
	if _, err := contract.ParseCandidateArtifactRef(asObject(map[string]any{
		"schema_version": cand.SchemaVersion, "candidate_id": cand.CandidateID,
		"kind": cand.Kind, "body_digest": cand.BodyDigest,
		"origin_type": cand.OriginType, "origin_ref": vrefDoc(cand.OriginRef.ID, cand.OriginRef.Digest),
		"runtime_input": map[string]any{"leak": true},
	})); err == nil {
		t.Fatalf("candidate ref with a runtime field must fail the strict §7.4 parser")
	}
}

// evaluatorDecision resolves the committed decision of one proposal from
// the evaluation ledger (written by Evaluate) as a ReleaseDecision view.
func evaluatorDecision(t *testing.T, h *harness, proposalID string) stubDecision {
	t.Helper()
	entries, err := h.store.Snapshot(ledger.LedgerEvaluation, "decision-"+proposalID)
	if err != nil || len(entries) != 1 {
		t.Fatalf("decision stream of %s: %d entries (err %v)", proposalID, len(entries), err)
	}
	payload, ok, err := h.store.Get(entries[0].PayloadDigest)
	if err != nil || !ok {
		t.Fatalf("decision payload unresolved: %v", err)
	}
	value, err := contract.ParseJSONStrict(payload)
	if err != nil {
		t.Fatalf("decision payload invalid: %v", err)
	}
	doc, _ := contract.AsObject(value)
	return stubDecision{doc: doc}
}

// ---------------------------------------------------------------------------
// GMS-208 TDD Red #2 (plan-pinned)
// ---------------------------------------------------------------------------

// TestMergeSourceHeadChangeIsStaleWithoutPartialActivation pins M5 /
// Contract §9.4.9-§9.4.13: when either source head moves before the M@1
// commit — whether before the transaction (a new ordinary release of A) or
// mid-commit (a racing writer sneaking the merge-source head CAS) — the
// whole activation fails, the proposal stales terminally, and ZERO partial
// release survives: no derived lineage/version, no active head, no
// activation event, no outbox record, no decision residue, and A/B stay
// exactly as the racing world left them.
func TestMergeSourceHeadChangeIsStaleWithoutPartialActivation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	_, prop := h.releasePair(t, 970_000, "model")
	h.driveToActivationPending(t, "mp-0001")

	residue := func() (int, int, int) {
		actEntries, _ := h.store.Snapshot(ledger.LedgerActivation, "")
		pending, _ := h.store.Pending()
		_, _, mOK, _ := h.store.GetHead(ledger.HeadLineageVersion, "sg-merged")
		_, _, aOK, _ := h.store.GetHead(ledger.HeadSkillActive, "sg-merged")
		heads := 0
		if mOK {
			heads++
		}
		if aOK {
			heads++
		}
		return len(actEntries), len(pending), heads
	}
	beforeActs, beforePending, beforeHeads := residue()

	// (a) The source head moves BEFORE the transaction: A receives a new
	// ordinary revision (v2), so the frozen expectation is stale.
	_, bodyA2 := h.guidanceEnvelope("sg-alpha", []string{"collect-context-v2"}, "research")
	a2 := h.reviseSkill(t, "sg-alpha", bodyA2)
	if a2.Version != "2" {
		t.Fatalf("seed A v2: got version %s", a2.Version)
	}
	// Re-baseline AFTER the revision: only the failed M@1 may add nothing.
	beforeActs, beforePending, beforeHeads = residue()
	_, err := h.svc.Activate(ctx, "mp-0001", ActivateInput{
		Decision: evaluatorDecision(t, h, "mp-0001"), LineageID: "sg-merged",
	})
	if code := CodeOf(err); code != ReasonMergeSourceHeadStale {
		t.Fatalf("moved source head: want MERGE_SOURCE_HEAD_STALE, got %v (code %s)", err, code)
	}
	if state := h.stateOf(t, "mp-0001"); state != StateStale {
		t.Fatalf("proposal state after stale activation = %s, want stale (terminal)", state)
	}
	acts, pending, heads := residue()
	if acts != beforeActs || pending != beforePending || heads != beforeHeads {
		t.Fatalf("stale attempt left residue: acts %d->%d pending %d->%d merged-heads %d->%d",
			beforeActs, acts, beforePending, pending, beforeHeads, heads)
	}
	// The stale terminal released both source pins and the group winner.
	for _, source := range prop.Sources() {
		pseq, _, pok, _ := h.store.GetHead(ledger.HeadMergeSource, mergeSourceHeadKey(source.LineageID, prop.GroupKey()))
		if !pok || pseq != 2 {
			t.Fatalf("source pin of %s not released by the stale terminal (seq %d ok %v)", source.LineageID, pseq, pok)
		}
	}
	if gseq, gdigest, _, _ := h.store.GetHead(ledger.HeadMergeGroupWinner, prop.GroupKey()); gseq != 2 || gdigest == prop.Digest() {
		t.Fatalf("group winner not released by the stale terminal")
	}
	// Terminal states never reopen.
	if _, _, err := h.svc.Admit(ctx, "mp-0001"); CodeOf(err) != ReasonMergeStateConflict {
		t.Fatalf("reopen of a stale proposal: want MERGE_STATE_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}

	// (b) Fresh chain over the CURRENT heads (alpha moved to v2 in (a)):
	// the stale terminal of mp-0001 freed the group, so a new assessment +
	// proposal over (sg-alpha v2, sg-beta v1) may win it again. The head
	// then moves MID-COMMIT: a racing writer sneaks one merge-source head
	// CAS between staging and commit.
	alphaNow, err := h.activations.ActiveRevision(ctx, "sg-alpha")
	if err != nil || alphaNow == nil || alphaNow.Version != "2" {
		t.Fatalf("current alpha head before (b): %+v (err %v)", alphaNow, err)
	}
	betaNow, err := h.activations.ActiveRevision(ctx, "sg-beta")
	if err != nil || betaNow == nil {
		t.Fatalf("current beta head before (b): %+v (err %v)", betaNow, err)
	}
	assess2, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID:         "sa-0002",
		Sources:              []contract.SkillArtifactRef{*betaNow, *alphaNow}, // reversed again
		ScoreMicros:          970_000,
		ApplicabilityOverlap: "partial",
		PermissionCompatible: true,
		FeatureRecordRefs:    []contract.VersionedRef{{ID: "feature-lex-0002", Version: "1", Digest: contract.DigestBytes([]byte("feature-lex-0002"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{
			h.committedEvidence("ev-assess-2a", "observation"),
			h.committedEvidence("ev-assess-2b", "observation"),
		},
		AssessorRef: contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	})
	if err != nil {
		t.Fatalf("assess pair (b): %v (code %s)", err, similarity.CodeOf(err))
	}
	pair2 := &sourcePair{a: *alphaNow, b: *betaNow, assessment: assess2}
	third := h.proposeOverPair(t, pair2, "mp-0003", "model")
	if third.GroupKey() != prop.GroupKey() {
		t.Fatalf("mp-0003 group differs; the stale terminal must free the group")
	}
	if _, state, err := h.svc.Admit(ctx, "mp-0003"); err != nil || state != StateAdmitted {
		t.Fatalf("admit mp-0003: state %s err %v (code %s)", state, err, CodeOf(err))
	}
	synth, err := h.svc.Synthesize(ctx, "mp-0003")
	if err != nil {
		t.Fatalf("synthesize mp-0003: %v (code %s)", err, CodeOf(err))
	}
	if err := h.svc.Validate(ctx, "mp-0003"); err != nil {
		t.Fatalf("validate mp-0003: %v (code %s)", err, CodeOf(err))
	}
	eval, err := h.svc.Evaluate(ctx, "mp-0003", h.evaluateInput(t, "mp-0003", synth.Candidate))
	if err != nil {
		t.Fatalf("evaluate mp-0003: %v (code %s)", err, CodeOf(err))
	}
	if eval.State != StateActivationPending {
		t.Fatalf("mp-0003 state = %s (outcome %s)", eval.State, eval.Decision.Outcome())
	}

	// Re-baseline again right before the raced commit.
	beforeActs, beforePending, beforeHeads = residue()
	injector := &sneakingStore{MemoryStore: h.store, sneakMergeSource: true}
	svcOver := h.serviceOver(injector)
	_, err = svcOver.Activate(ctx, "mp-0003", ActivateInput{
		Decision: evaluatorDecision(t, h, "mp-0003"), LineageID: "sg-merged",
	})
	if code := CodeOf(err); code != ReasonMergeSourceHeadStale && code != ReasonMergeStateConflict {
		t.Fatalf("mid-commit source-head race: want MERGE_SOURCE_HEAD_STALE, got %v (code %s)", err, code)
	}
	if state := h.stateOf(t, "mp-0003"); state != StateStale {
		t.Fatalf("mp-0003 state after mid-commit race = %s, want stale", state)
	}
	acts, pending, heads = residue()
	if acts != beforeActs || pending != beforePending || heads != beforeHeads {
		t.Fatalf("mid-commit race left residue: acts %d->%d pending %d->%d merged-heads %d->%d",
			beforeActs, acts, beforePending, pending, beforeHeads, heads)
	}
	// The derived lineage never became active and no event mentions it.
	if active, _ := h.activations.ActiveRevision(ctx, "sg-merged"); active != nil {
		t.Fatalf("derived lineage leaked an active head: %+v", active)
	}
	for _, entry := range actEntries(t, h) {
		payload, _, _ := h.store.Get(entry.PayloadDigest)
		if strings.Contains(string(payload), "sg-merged") {
			t.Fatalf("activation stream leaked the derived lineage into entry %s", entry.EventID)
		}
	}
}

func actEntries(t *testing.T, h *harness) []ledger.Entry {
	t.Helper()
	entries, err := h.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		t.Fatalf("activation snapshot: %v", err)
	}
	return entries
}

// sneakingStore races one merge-source head CAS right before it applies.
type sneakingStore struct {
	*ledger.MemoryStore
	sneakMergeSource bool
	once             bool
}

func (s *sneakingStore) CompareAndSwap(kind ledger.HeadKind, key string, expectedSeq uint64, expectedDigest string, newSeq uint64, newDigest string) error {
	if s.sneakMergeSource && !s.once && kind == ledger.HeadMergeSource {
		s.once = true
		seq, digest, ok, _ := s.MemoryStore.GetHead(kind, key)
		if ok {
			racing := contract.DigestBytes([]byte("racing-writer:" + string(kind) + ":" + key))
			_ = s.MemoryStore.CompareAndSwap(kind, key, seq, digest, seq+1, racing)
		}
	}
	return s.MemoryStore.CompareAndSwap(kind, key, expectedSeq, expectedDigest, newSeq, newDigest)
}
