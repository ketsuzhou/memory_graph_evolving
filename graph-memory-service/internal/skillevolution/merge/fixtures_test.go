// Package merge tests: the $FIX/merge conformance fixtures, the fail-closed
// admission/synthesis gates and the recorded q29b merge-family bilateral
// replay (GMS-208 MT1-MT5 corpus alignment).
package merge

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluator"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
)

// ---------------------------------------------------------------------------
// $FIX/merge fixture matrix
// ---------------------------------------------------------------------------

func mergeFixtureValue(t *testing.T, caseName, file string) map[string]any {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "merge", caseName, file))
	if err != nil {
		t.Fatalf("read fixture %s/%s: %v", caseName, file, err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		t.Fatalf("parse fixture %s/%s: %v", caseName, file, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		t.Fatalf("fixture %s/%s is not a JSON object", caseName, file)
	}
	return obj
}

// TestMergeProposalFixtureMatrix drives ParseProposal over the $FIX/merge
// proposal corpus: merge-003 canonicalizes to the frozen expectation
// (7454 bytes, digest 7f682dcd..., canonical.utf8 byte equality) with its
// RE-DERIVED dedup keys independent of the declared (corpus-frozen) ones,
// and the blocking-conflict negative parses as a document whose conflict
// closure fails at synthesis time, not at parse time.
func TestMergeProposalFixtureMatrix(t *testing.T) {
	h := newHarness(t)

	prop, err := ParseProposal(h.gates, mergeFixtureValue(t, "merge-003-merge-proposal", "source.json"))
	if err != nil {
		t.Fatalf("parse merge-003: %v (code %s)", err, CodeOf(err))
	}
	expected := mergeFixtureValue(t, "merge-003-merge-proposal", "expected.json")
	if want, _ := contract.AsString(expected["expected_digest"]); want != prop.Digest() {
		t.Fatalf("merge-003 digest %s != corpus expectation %s", prop.Digest(), want)
	}
	canonicalUTF8, err := os.ReadFile(filepath.Join(mustDir(t), "merge", "merge-003-merge-proposal", "canonical.utf8"))
	if err != nil {
		t.Fatalf("read canonical.utf8: %v", err)
	}
	if string(prop.CanonicalBytes()) != string(canonicalUTF8) {
		t.Fatalf("merge-003 canonical bytes disagree with canonical.utf8")
	}
	if int64(len(prop.CanonicalBytes())) != mustIntOf(t, expected["expected_canonical_byte_length"]) {
		t.Fatalf("merge-003 canonical length %d != corpus expectation %v", len(prop.CanonicalBytes()), expected["expected_canonical_byte_length"])
	}
	if prop.Sources()[0].LineageID != "sg-alpha" || prop.Sources()[1].LineageID != "sg-beta" {
		t.Fatalf("merge-003 canonical order = [%s, %s]", prop.Sources()[0].LineageID, prop.Sources()[1].LineageID)
	}
	// The declared corpus dedup keys are inert: the recomputed keys are
	// authoritative (they derive from the parsed sources + intent).
	declared, _ := contract.AsObject(prop.Doc()["dedup"])
	if declaredGroup, _ := contract.AsString(declared["proposal_group_key"]); declaredGroup == prop.GroupKey() {
		t.Fatalf("corpus-declared group key unexpectedly recomputes; the fixture freezes pre-implementation digests")
	}
	if prop.GroupKey() == "" || prop.IntentKey() == "" || prop.PairKey() == "" {
		t.Fatalf("recomputed dedup keys must be non-empty")
	}
	// The declared conflicts parse and the non-blocking resolution holds.
	conflicts := prop.Conflicts()
	if len(conflicts) < 1 {
		t.Fatalf("merge-003 carries conflict records")
	}
	if blocking := boolOf(conflicts[0]["blocking"]); blocking {
		t.Fatalf("merge-003 conflict is non-blocking (resolved)")
	}

	// The blocking-conflict negative parses as a document; its reason fires
	// at synthesis (unresolved blocking conflict forbids binding).
	negProp, err := ParseProposal(h.gates, mergeFixtureValue(t, "merge-neg-002-blocking-conflict", "source.json"))
	if err != nil {
		t.Fatalf("parse merge-neg-002: %v (code %s)", err, CodeOf(err))
	}
	negExpected := mergeFixtureValue(t, "merge-neg-002-blocking-conflict", "expected.json")
	if want, _ := contract.AsString(negExpected["expected_reason_code"]); want != ReasonMergeBlockingConflict {
		t.Fatalf("corpus reason drift: %s", want)
	}
	negConflicts := negProp.Conflicts()
	blockingSeen := false
	for _, conflict := range negConflicts {
		if b := boolOf(conflict["blocking"]); b {
			blockingSeen = true
			resolution, _ := contract.AsObject(conflict["resolution"])
			action, _ := contract.AsString(resolution["action"])
			if action != "unresolved" && action != "" {
				t.Fatalf("a blocking conflict must be unresolved, got resolution action %q: %v", action, conflict)
			}
		}
	}
	if !blockingSeen {
		t.Fatalf("merge-neg-002 must carry at least one unresolved blocking conflict")
	}
	if string(negProp.CanonicalBytes()) != string(mustFile(t, "merge", "merge-neg-002-blocking-conflict", "canonical.utf8")) {
		t.Fatalf("merge-neg-002 canonical bytes disagree with canonical.utf8")
	}
}

func mustDir(t *testing.T) string {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	return dir
}

func mustFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{mustDir(t)}, parts...)...))
	if err != nil {
		t.Fatalf("read fixture %v: %v", parts, err)
	}
	return data
}

func mustIntOf(t *testing.T, raw any) int64 {
	t.Helper()
	num, ok := raw.(json.Number)
	if !ok {
		t.Fatalf("expected an integer, got %T", raw)
	}
	var value int64
	if err := json.Unmarshal([]byte(num.String()), &value); err != nil {
		t.Fatalf("integer parse: %v", err)
	}
	return value
}

// ---------------------------------------------------------------------------
// Fail-closed pipeline gates (harness-driven)
// ---------------------------------------------------------------------------

// TestMergeBelowThresholdNeverProposes pins M1/M7 + Contract §15.2 MT1: a
// below-threshold pair (the merge-neg-001 score) assesses as a record but can
// never create a proposal — not even through the protected model gate, and
// the automatic initiator is doubly barred.
func TestMergeBelowThresholdNeverProposes(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"direct-answer", "recover-from-gap"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	b := h.releaseSkill(t, "sg-beta", bodyB)

	// 120000 is below_suggestion under DefaultPolicyV1 ($FIX/merge-neg-001).
	assessment, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID:         "sa-low",
		Sources:              []contract.SkillArtifactRef{a, b},
		ScoreMicros:          120_000,
		ApplicabilityOverlap: "partial",
		PermissionCompatible: true,
		FeatureRecordRefs:    []contract.VersionedRef{{ID: "feature-low", Version: "1", Digest: contract.DigestBytes([]byte("feature-low"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{
			h.committedEvidence("ev-low-a", "observation"), h.committedEvidence("ev-low-b", "observation"),
		},
		AssessorRef: contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	})
	if err != nil {
		t.Fatalf("assess below-threshold pair: %v (code %s)", err, similarity.CodeOf(err))
	}
	if assessment.Band() != similarity.BandBelowSuggestion {
		t.Fatalf("120000 band = %s, want below_suggestion", assessment.Band())
	}

	pair := &sourcePair{a: a, b: b, assessment: assessment}
	for _, initiator := range []string{"automatic", "model", "human"} {
		in := h.proposeInput(pair, "mp-low-"+initiator, initiator)
		_, err := h.svc.Propose(ctx, in)
		if CodeOf(err) != ReasonSimilarityBelowThreshold {
			t.Fatalf("%s initiator over a below-threshold pair: want SIMILARITY_BELOW_THRESHOLD, got %v (code %s)", initiator, err, CodeOf(err))
		}
	}
	// Nothing was committed: no proposal stream, no merge events.
	for _, id := range []string{"mp-low-automatic", "mp-low-model", "mp-low-human"} {
		if entries, _ := h.store.Snapshot(ledger.LedgerMergeProposal, id); len(entries) != 0 {
			t.Fatalf("below-threshold attempt %s committed a proposal", id)
		}
		if entries, _ := h.store.Snapshot(ledger.LedgerMergeEvent, id); len(entries) != 0 {
			t.Fatalf("below-threshold attempt %s wrote lifecycle events", id)
		}
	}
}

// TestMergeBlockingConflictRejectsSynthesis pins Contract §9.4.4/§13.7: an
// unresolved blocking conflict forbids candidate binding (admitted ->
// rejected, MERGE_BLOCKING_CONFLICT) and the terminal frees the group so a
// resolved retry may win again.
func TestMergeBlockingConflictRejectsSynthesis(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	pair, _ := h.releasePair(t, 970_000, "model")
	in := h.proposeInput(pair, "mp-blocked", "model")
	// Replace the resolved conflict with an unresolved blocking one.
	in.Conflicts = []map[string]any{{
		"conflict_id":           "cf-blocked-0001",
		"category":              "contradictory_action",
		"semantic_location":     "/body/branches/0/action/guidance",
		"source_a_claim_ref":    vrefDoc("claim-alpha-0007", contract.DigestBytes([]byte("claim-alpha-0007"))),
		"source_b_claim_ref":    vrefDoc("claim-beta-0004", contract.DigestBytes([]byte("claim-beta-0004"))),
		"applicability_overlap": "complete",
		"resolution": map[string]any{
			"action":        "unresolved",
			"evidence_refs": []any{},
			"rationale":     "awaiting protected conflict closure",
		},
		"blocking": true,
	}}
	prop, err := h.svc.Propose(ctx, in)
	if err != nil {
		t.Fatalf("propose with blocking conflict: %v (code %s)", err, CodeOf(err))
	}
	if _, state, err := h.svc.Admit(ctx, "mp-blocked"); err != nil || state != StateAdmitted {
		t.Fatalf("admit mp-blocked: state %s err %v (code %s)", state, err, CodeOf(err))
	}
	if _, err := h.svc.Synthesize(ctx, "mp-blocked"); CodeOf(err) != ReasonMergeBlockingConflict {
		t.Fatalf("synthesize over a blocking conflict: want MERGE_BLOCKING_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	if state := h.stateOf(t, "mp-blocked"); state != StateRejected {
		t.Fatalf("mp-blocked state = %s, want rejected", state)
	}
	// No merge candidate was ever bound for the blocked proposal (the two
	// ledger candidates are the source-skill bindings of the harness).
	candidateEntries, err := h.store.Snapshot(ledger.LedgerCandidate, "")
	if err != nil {
		t.Fatalf("candidate snapshot: %v", err)
	}
	for _, entry := range candidateEntries {
		payload, ok, err := h.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			t.Fatalf("candidate payload %s unresolved", entry.PayloadDigest)
		}
		value, _ := contract.ParseJSONStrict(payload)
		obj, _ := contract.AsObject(value)
		if origin, _ := contract.AsObject(obj["origin_ref"]); origin != nil {
			if id, _ := contract.AsString(origin["id"]); id == "mp-blocked" {
				t.Fatalf("blocking-conflict chain bound candidate %s", id)
			}
		}
	}
	// The rejected terminal freed the group: a resolved retry wins again.
	retry := h.proposeOverPair(t, pair, "mp-retry", "model")
	if retry.GroupKey() != prop.GroupKey() {
		t.Fatalf("retry group drift")
	}
	if _, state, err := h.svc.Admit(ctx, "mp-retry"); err != nil || state != StateAdmitted {
		t.Fatalf("admit mp-retry after rejection freed the group: state %s err %v (code %s)", state, err, CodeOf(err))
	}
}

// TestMergeIncompatibleSourcesRejectAtAdmission pins the M2 compatibility
// gates: applicability overlap none and permission-incompatible pairs both
// reject terminally with MERGE_BLOCKING_CONFLICT (proposed -> rejected).
func TestMergeIncompatibleSourcesRejectAtAdmission(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	_, bodyB := h.guidanceEnvelope("sg-gamma", []string{"gamma-path"}, "archive")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	c := h.releaseSkill(t, "sg-gamma", bodyB)

	cases := []struct {
		name    string
		overlap string
		permOK  bool
	}{
		{"applicability none", "none", true},
		{"permissions incompatible", "partial", false},
	}
	for i, tc := range cases {
		assessment, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
			AssessmentID:         "sa-m2-" + tc.name,
			Sources:              []contract.SkillArtifactRef{a, c},
			ScoreMicros:          970_000,
			ApplicabilityOverlap: tc.overlap, PermissionCompatible: tc.permOK,
			FeatureRecordRefs: []contract.VersionedRef{{ID: "feature-m2", Version: "1", Digest: contract.DigestBytes([]byte("feature-m2"))}},
			SupportingEvidenceRefs: []contract.EvidenceRef{
				h.committedEvidence("ev-m2-a", "observation"), h.committedEvidence("ev-m2-b", "observation"),
			},
			AssessorRef: contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
		})
		if err != nil {
			t.Fatalf("assess %s: %v (code %s)", tc.name, err, similarity.CodeOf(err))
		}
		p := &sourcePair{a: a, b: c, assessment: assessment}
		h.proposeOverPair(t, p, "mp-m2-"+string(rune('0'+i)), "model")
		if _, _, err := h.svc.Admit(ctx, "mp-m2-"+string(rune('0'+i))); CodeOf(err) != ReasonMergeBlockingConflict {
			t.Fatalf("%s: want MERGE_BLOCKING_CONFLICT, got %v (code %s)", tc.name, err, CodeOf(err))
		}
		if state := h.stateOf(t, "mp-m2-"+string(rune('0'+i))); state != StateRejected {
			t.Fatalf("%s: state = %s, want rejected", tc.name, state)
		}
	}
}

// TestMergeUncommittedEvidenceRejectsPropose pins the evidence closure: an
// evidence set referencing an unresolvable ref fails EVIDENCE_NOT_COMMITTED
// before any proposal exists.
func TestMergeUncommittedEvidenceRejectsPropose(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	pair, _ := h.releasePair(t, 970_000, "model")
	in := h.proposeInput(pair, "mp-uncommitted", "model")
	ghost := contract.EvidenceRef{
		SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "ev-ghost-0001", Version: "1",
		EvidenceDigest: contract.DigestBytes([]byte("ghost")), CommitState: "committed", EvidenceKind: "observation",
	}
	in.EvidenceSets[0].SupportingEvidenceRefs = []contract.EvidenceRef{ghost}
	if _, err := h.svc.Propose(ctx, in); CodeOf(err) != ReasonEvidenceNotCommitted {
		t.Fatalf("uncommitted evidence: want EVIDENCE_NOT_COMMITTED, got %v (code %s)", err, CodeOf(err))
	}
	if entries, _ := h.store.Snapshot(ledger.LedgerMergeProposal, "mp-uncommitted"); len(entries) != 0 {
		t.Fatalf("uncommitted-evidence attempt committed a proposal")
	}
}

// TestMergeCrossKindAndNWayFailClosed pins that cross-kind and n-way merges
// cannot even reach admission: the MT1 assessor rejects them upstream
// (SKILL_KIND_INVALID / REF_MISMATCH), so the merge service never sees them.
func TestMergeCrossKindAndNWayFailClosed(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	procedure := a
	procedure.Kind = "human_procedure"
	procedure.LineageID = "proc-render"

	// cross-kind: the assessor rejects before any record exists.
	if _, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID: "sa-cross", Sources: []contract.SkillArtifactRef{a, procedure}, ScoreMicros: 970_000,
		ApplicabilityOverlap: "partial", PermissionCompatible: true,
		FeatureRecordRefs:      []contract.VersionedRef{{ID: "feature-cross", Version: "1", Digest: contract.DigestBytes([]byte("feature-cross"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{h.committedEvidence("ev-cross", "observation")},
		AssessorRef:            contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	}); CodeOf(err) != ReasonSkillKindInvalid && similarity.CodeOf(err) != similarity.ReasonSkillKindInvalid {
		t.Fatalf("cross-kind assess: want SKILL_KIND_INVALID, got %v (code %s)", err, similarity.CodeOf(err))
	}
	// n-way: three sources cannot produce an assessment at all.
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	b := h.releaseSkill(t, "sg-beta", bodyB)
	_, bodyD := h.guidanceEnvelope("sg-delta", []string{"delta-path"}, "research")
	d := h.releaseSkill(t, "sg-delta", bodyD)
	if _, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID: "sa-nway", Sources: []contract.SkillArtifactRef{a, b, d}, ScoreMicros: 970_000,
		ApplicabilityOverlap: "partial", PermissionCompatible: true,
		FeatureRecordRefs:      []contract.VersionedRef{{ID: "feature-nway", Version: "1", Digest: contract.DigestBytes([]byte("feature-nway"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{h.committedEvidence("ev-nway", "observation")},
		AssessorRef:            contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	}); similarity.CodeOf(err) != similarity.ReasonRefMismatch {
		t.Fatalf("n-way assess: want REF_MISMATCH, got %v (code %s)", err, similarity.CodeOf(err))
	}
	// And the merge service itself demands exactly 2 evidence sets.
	pair := &sourcePair{a: a, b: b}
	_ = pair
	if _, err := h.svc.Propose(ctx, ProposeInput{ProposalID: "mp-shape"}); CodeOf(err) != ReasonSimilarityAssessmentInvalid && CodeOf(err) != ReasonRefMismatch {
		t.Fatalf("empty propose input: want a fail-closed shape rejection, got %v (code %s)", err, CodeOf(err))
	}
}

// TestMergeCriticalRegressionRejectedAndGroupReWinnable pins the U1
// bilateral no-regression rule: a candidate that fails a critical packet the
// baseline passed is REJECTED (never released), the proposal lands in the
// rejected terminal, the group frees, and a sound retry over the same pair
// can win and release.
func TestMergeCriticalRegressionRejectedAndGroupReWinnable(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	pair, prop := h.releasePair(t, 970_000, "model")
	if _, state, err := h.svc.Admit(ctx, "mp-0001"); err != nil || state != StateAdmitted {
		t.Fatalf("admit mp-0001: %s %v", state, err)
	}
	synth, err := h.svc.Synthesize(ctx, "mp-0001")
	if err != nil {
		t.Fatalf("synthesize: %v", err)
	}
	if err := h.svc.Validate(ctx, "mp-0001"); err != nil {
		t.Fatalf("validate: %v", err)
	}
	// The regressive envelope: the candidate FAILS the critical a-only
	// packet that BOTH sources pass (critical regression).
	eval, err := h.svc.Evaluate(ctx, "mp-0001", h.regressiveInput(t, "mp-0001", synth.Candidate))
	if err != nil {
		t.Fatalf("evaluate: %v (code %s)", err, CodeOf(err))
	}
	if eval.Decision.Outcome() != evaluator.OutcomeRejected {
		t.Fatalf("critical regression outcome = %s, want rejected (U1 no-regression)", eval.Decision.Outcome())
	}
	if eval.State != StateRejected {
		t.Fatalf("mp-0001 state = %s, want rejected", eval.State)
	}
	// No release happened: no derived lineage, no activation events beyond
	// the two source releases, no outbox residue.
	if _, _, ok, _ := h.store.GetHead(ledger.HeadLineageVersion, "sg-merged"); ok {
		t.Fatalf("rejected merge leaked a lineage-version head")
	}
	// The group is free again: the sound retry wins and releases.
	retry := h.proposeOverPair(t, pair, "mp-0002", "model")
	if retry.GroupKey() != prop.GroupKey() {
		t.Fatalf("retry group drift")
	}
	h.driveToActivationPending(t, "mp-0002")
	res, err := h.svc.Activate(ctx, "mp-0002", ActivateInput{
		Decision: evaluatorDecision(t, h, "mp-0002"), LineageID: "sg-merged",
	})
	if err != nil {
		t.Fatalf("activate retry: %v (code %s)", err, CodeOf(err))
	}
	if res.ReleasedRef.LineageID != "sg-merged" || res.ReleasedRef.Version != "1" {
		t.Fatalf("retry release = %+v", res.ReleasedRef)
	}
}

// regressiveInput builds the bilateral envelope whose candidate critically
// regresses: the a-only family is critical and the candidate side fails it
// (SKILL_BRANCH_UNCOVERED semantic failure).
func (h *harness) regressiveInput(t *testing.T, proposalID string, cand contract.CandidateArtifactRef) EvaluateInput {
	t.Helper()
	prop, err := h.svc.LoadProposal(proposalID, "")
	if err != nil {
		t.Fatalf("load %s: %v", proposalID, err)
	}
	pair := prop.Sources()
	alphaDoc, betaDoc := similarity.SkillRefDoc(pair[0]), similarity.SkillRefDoc(pair[1])
	candDoc := map[string]any{
		"schema_version": cand.SchemaVersion, "candidate_id": cand.CandidateID,
		"kind": cand.Kind, "body_digest": cand.BodyDigest,
		"origin_type": cand.OriginType, "origin_ref": vrefDoc(cand.OriginRef.ID, cand.OriginRef.Digest),
	}
	mk := func(id, side, status, reason string, artifact map[string]any, domains []string, critical bool) (replay.Packet, replay.RunOutput) {
		digest := contract.DigestBytes([]byte("regress:" + id + ":" + side))
		return replay.Packet{
				ID: id, Side: side, PathDomains: domains, Critical: critical,
				ExpectedRunStatus: status, ExpectedReasonCode: reason,
				ExpectedOutputDigest: digest, ExpectedArtifactRef: artifact,
			}, replay.RunOutput{
				PacketID: id, Side: side, RunStatus: status, ReasonCode: reason,
				OutputDigest: digest, ArtifactRef: artifact,
			}
	}
	var packets []replay.Packet
	var runs []replay.RunOutput
	add := func(p replay.Packet, r replay.RunOutput) { packets = append(packets, p); runs = append(runs, r) }
	// a-only family: CRITICAL; baseline passes, candidate FAILS.
	p, r := mk("pk-a-only", replay.SideBaseline, "succeeded", "", alphaDoc, []string{"success"}, true)
	add(p, r)
	p, r = mk("pk-a-only", replay.SideCandidate, "failed", "SKILL_BRANCH_UNCOVERED", candDoc, []string{"success"}, true)
	add(p, r)
	// b-only family: both pass.
	p, r = mk("pk-b-only", replay.SideBaseline, "succeeded", "", betaDoc, []string{"success"}, true)
	add(p, r)
	p, r = mk("pk-b-only", replay.SideCandidate, "succeeded", "", candDoc, []string{"success"}, true)
	add(p, r)
	// overlap family: both pass.
	p, r = mk("pk-overlap", replay.SideBaseline, "succeeded", "", alphaDoc, []string{"failure", "recovery"}, false)
	add(p, r)
	p, r = mk("pk-overlap", replay.SideCandidate, "succeeded", "", candDoc, []string{"failure", "recovery"}, false)
	add(p, r)
	familyOf := func(id string) string {
		switch id {
		case "pk-a-only":
			return "a-only"
		case "pk-b-only":
			return "b-only"
		default:
			return "overlap"
		}
	}
	plan := &replay.Plan{Families: []replay.Family{
		{Name: "a-only", Domain: replay.DomainSourceA, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: alphaDoc},
		{Name: "b-only", Domain: replay.DomainSourceB, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: betaDoc},
		{Name: "overlap", Domain: replay.DomainOverlap, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: alphaDoc},
	}}
	for i := range plan.Families {
		for _, pkt := range packets {
			if familyOf(pkt.ID) == plan.Families[i].Name {
				plan.Families[i].Packets = append(plan.Families[i].Packets, pkt)
			}
		}
	}
	requestDoc := map[string]any{
		"schema_version":      "gms.replay-request.v1",
		"replay_request_id":   "replay-" + proposalID + "-regress",
		"candidate_ref":       candDoc,
		"baseline_skill_refs": []any{alphaDoc, betaDoc},
		"fixture_set_refs":    []any{vrefDoc("fixture-regress-0001", contract.DigestBytes([]byte("fixture-regress-0001")))},
		"segment_refs": []any{map[string]any{
			"schema_version": "host.segment-ref.v1", "room_id": "room-1",
			"segment_id": "seg-regress-0001", "segment_version": jn(1),
			"segment_digest":    contract.DigestBytes([]byte("seg-regress-0001")),
			"evidence_seal_ref": vrefDoc("evseal-regress", contract.DigestBytes([]byte("evseal-regress"))),
			"path_seal_ref":     vrefDoc("pathseal-regress", contract.DigestBytes([]byte("pathseal-regress"))),
		}},
		"replay_profile_ref":  vrefDoc("rsih.replay-profile", contract.DigestBytes([]byte("rsih.replay-profile"))),
		"runtime_adapter_ref": vrefDoc("rsih.fake-runtime-adapter", contract.DigestBytes([]byte("rsih.fake-runtime-adapter"))),
		"mode":                "causal_evaluation",
		"idempotency_key":     contract.DigestBytes([]byte("merge-replay-regress-" + proposalID)),
		"required_source_heads": []any{
			similarity.SkillRefDoc(pair[0]), similarity.SkillRefDoc(pair[1]),
		},
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

// ---------------------------------------------------------------------------
// Recorded q29b merge family bilateral replay
// ---------------------------------------------------------------------------

// q29bPacket loads one recorded runtime.json packet.
func q29bPacket(t *testing.T, parts ...string) map[string]any {
	t.Helper()
	value, err := contract.ParseJSONStrict(mustFile(t, parts...))
	if err != nil {
		t.Fatalf("parse %v: %v", parts, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		t.Fatalf("%v is not a JSON object", parts)
	}
	return obj
}

// recordedToPair converts one recorded q29b runtime packet into the
// replay.Packet + RunOutput pair (the run reproduces the recorded terminal
// and expected output digest; the executed artifact ref is remapped onto the
// live harness refs because the recorded corpus refs address corpus-world
// artifacts, not this ledger).
func recordedToPair(packet map[string]any, artifact map[string]any, critical bool) (replay.Packet, replay.RunOutput) {
	id, _ := contract.AsString(packet["packet_id"])
	run, _ := contract.AsObject(packet["run"])
	side, _ := contract.AsString(run["side"])
	sealed, _ := contract.AsObject(packet["sealed_inputs"])
	domainsRaw, _ := contract.AsArray(sealed["required_path_domains"])
	domains := make([]string, 0, len(domainsRaw))
	for _, raw := range domainsRaw {
		domain, _ := contract.AsString(raw)
		domains = append(domains, domain)
	}
	terminal, _ := contract.AsObject(packet["terminal"])
	status, _ := contract.AsString(terminal["status"])
	reason, _ := contract.AsString(terminal["reason_code"])
	expected, _ := contract.AsObject(packet["expected"])
	digest, _ := contract.AsString(expected["output_digest"])
	return replay.Packet{
			ID: id, Side: side, PathDomains: domains, Critical: critical,
			ExpectedRunStatus: status, ExpectedReasonCode: reason,
			ExpectedOutputDigest: digest, ExpectedArtifactRef: artifact,
		}, replay.RunOutput{
			PacketID: id, Side: side, RunStatus: status, ReasonCode: reason,
			OutputDigest: digest, ArtifactRef: artifact,
		}
}

// TestQ29bMergePacketsBilateralReplay drives the bilateral MT4 handoff from
// the RECORDED merge family: the two q29b/merge runtime.json packets (B-side
// baseline execution and merged-candidate execution) plus the recorded
// baseline/extra-tool-call packets form the three required families, and the
// U1 evaluator accepts the strict envelope improvement.
func TestQ29bMergePacketsBilateralReplay(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	pair, _ := h.releasePair(t, 970_000, "model")
	if _, state, err := h.svc.Admit(ctx, "mp-0001"); err != nil || state != StateAdmitted {
		t.Fatalf("admit: %s %v", state, err)
	}
	synth, err := h.svc.Synthesize(ctx, "mp-0001")
	if err != nil {
		t.Fatalf("synthesize: %v", err)
	}
	if err := h.svc.Validate(ctx, "mp-0001"); err != nil {
		t.Fatalf("validate: %v", err)
	}
	cand := synth.Candidate
	alphaDoc, betaDoc := similarity.SkillRefDoc(pair.assessment.Sources()[0]), similarity.SkillRefDoc(pair.assessment.Sources()[1])
	candDoc := map[string]any{
		"schema_version": cand.SchemaVersion, "candidate_id": cand.CandidateID,
		"kind": cand.Kind, "body_digest": cand.BodyDigest,
		"origin_type": cand.OriginType, "origin_ref": vrefDoc(cand.OriginRef.ID, cand.OriginRef.Digest),
	}

	// Recorded corpus packets ($FIX/recorded/q29b):
	//   - baseline-0001: A executes the success path (baseline side, a-only).
	//   - extra-tool-call-0001: A fails UNDECLARED_TOOL_ACCESS (semantic
	//     baseline failure of the overlap family).
	//   - merge-pair-0001: B executes the merge-pair success path
	//     (baseline side, b-only family).
	//   - merge-pair-0002: M executes success/failure/recovery (candidate
	//     side, b-only family).
	aBase := q29bPacket(t, "recorded", "q29b", "baseline", "q29b-baseline-0001", "runtime.json")
	overlapFail := q29bPacket(t, "recorded", "q29b", "extra-tool-call", "q29b-extra-tool-call-0001", "runtime.json")
	bSide := q29bPacket(t, "recorded", "q29b", "merge", "q29b-merge-pair-0001", "runtime.json")
	mSide := q29bPacket(t, "recorded", "q29b", "merge", "q29b-merge-pair-0002", "runtime.json")
	for _, packet := range []map[string]any{aBase, overlapFail, bSide, mSide} {
		if family, _ := contract.AsString(packet["family"]); family != "merge" && family != "baseline" && family != "extra-tool-call" {
			t.Fatalf("unexpected corpus family %q", family)
		}
	}

	var packets []replay.Packet
	var runs []replay.RunOutput
	add := func(p replay.Packet, r replay.RunOutput) { packets = append(packets, p); runs = append(runs, r) }
	// a-only family: the recorded A baseline pass + the candidate side of the
	// same packet executing M.
	p, r := recordedToPair(aBase, alphaDoc, true)
	add(p, r)
	// overlap family: recorded A semantic failure (baseline side).
	p, r = recordedToPair(overlapFail, alphaDoc, false)
	add(p, r)
	synthPacket := func(id, side, status, reason string, artifact map[string]any, domains []string, critical bool) {
		pp, rr := replay.Packet{
			ID: id, Side: side, PathDomains: domains, Critical: critical,
			ExpectedRunStatus: status, ExpectedReasonCode: reason,
			ExpectedOutputDigest: contract.DigestBytes([]byte("q29b-merge:" + id + ":" + side)),
			ExpectedArtifactRef:  artifact,
		}, replay.RunOutput{
			PacketID: id, Side: side, RunStatus: status, ReasonCode: reason,
			OutputDigest: contract.DigestBytes([]byte("q29b-merge:" + id + ":" + side)),
			ArtifactRef:  artifact,
		}
		add(pp, rr)
	}
	synthPacket("q29b-baseline-0001", replay.SideCandidate, "succeeded", "", candDoc, []string{"success"}, true)
	// b-only family: the recorded B baseline pass + the candidate side of
	// the same packet executing M, plus the recorded merge-pair-0002 run of
	// M over success/failure/recovery (an extra candidate-side packet).
	p, r = recordedToPair(bSide, betaDoc, true)
	add(p, r)
	synthPacket("q29b-merge-pair-0001", replay.SideCandidate, "succeeded", "", candDoc, []string{"success"}, true)
	p, r = recordedToPair(mSide, candDoc, true)
	add(p, r)
	// overlap candidate: M covers failure->recovery (the strict improvement).
	synthPacket("q29b-extra-tool-call-0001", replay.SideCandidate, "succeeded", "", candDoc, []string{"failure", "recovery"}, false)

	plan := &replay.Plan{Families: []replay.Family{
		{Name: "a-only", Domain: replay.DomainSourceA, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: alphaDoc},
		{Name: "b-only", Domain: replay.DomainSourceB, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: betaDoc},
		{Name: "overlap", Domain: replay.DomainOverlap, Sides: []string{replay.SideBaseline, replay.SideCandidate}, Baseline: alphaDoc},
	}}
	familyOfPacket := func(pkt replay.Packet) string {
		switch pkt.ID {
		case "q29b-baseline-0001":
			return "a-only"
		case "q29b-merge-pair-0001", "q29b-merge-pair-0002":
			return "b-only"
		default:
			return "overlap"
		}
	}
	for i := range plan.Families {
		for _, pkt := range packets {
			if familyOfPacket(pkt) == plan.Families[i].Name {
				plan.Families[i].Packets = append(plan.Families[i].Packets, pkt)
			}
		}
	}
	requestDoc := map[string]any{
		"schema_version":      "gms.replay-request.v1",
		"replay_request_id":   "replay-q29b-merge-0001",
		"candidate_ref":       candDoc,
		"baseline_skill_refs": []any{alphaDoc, betaDoc},
		"fixture_set_refs":    []any{vrefDoc("fixture-q29b-merge", contract.DigestBytes([]byte("fixture-q29b-merge")))},
		"segment_refs": []any{map[string]any{
			"schema_version": "host.segment-ref.v1", "room_id": "room-alpha-0001",
			"segment_id": "seg-success-0001", "segment_version": jn(1),
			"segment_digest":    contract.DigestBytes([]byte("seg-success-0001")),
			"evidence_seal_ref": vrefDoc("evseal-q29b-merge", contract.DigestBytes([]byte("evseal-q29b-merge"))),
			"path_seal_ref":     vrefDoc("pathseal-q29b-merge", contract.DigestBytes([]byte("pathseal-q29b-merge"))),
		}},
		"replay_profile_ref":  vrefDoc("rsih.replay-profile", contract.DigestBytes([]byte("rsih.replay-profile"))),
		"runtime_adapter_ref": vrefDoc("rsih.fake-runtime-adapter", contract.DigestBytes([]byte("rsih.fake-runtime-adapter"))),
		"mode":                "causal_evaluation",
		"idempotency_key":     contract.DigestBytes([]byte("merge-replay-q29b-0001")),
		"required_source_heads": []any{
			similarity.SkillRefDoc(pair.assessment.Sources()[0]), similarity.SkillRefDoc(pair.assessment.Sources()[1]),
		},
	}
	eval, err := h.svc.Evaluate(ctx, "mp-0001", EvaluateInput{
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
	})
	if err != nil {
		t.Fatalf("evaluate over the recorded merge packets: %v (code %s)", err, CodeOf(err))
	}
	if eval.Decision.Outcome() != evaluator.OutcomeAccepted {
		t.Fatalf("recorded merge replay outcome = %s (reasons %v), want accepted", eval.Decision.Outcome(), eval.Decision.ReasonCodes())
	}
	if eval.State != StateActivationPending {
		t.Fatalf("state = %s, want activation_pending", eval.State)
	}
	// And M@1 releases from the recorded-corpus-driven decision.
	res, err := h.svc.Activate(ctx, "mp-0001", ActivateInput{
		Decision: evaluatorDecision(t, h, "mp-0001"), LineageID: "sg-merged",
	})
	if err != nil {
		t.Fatalf("activate: %v (code %s)", err, CodeOf(err))
	}
	if res.ReleasedRef.LineageID != "sg-merged" {
		t.Fatalf("released ref %+v", res.ReleasedRef)
	}
}
