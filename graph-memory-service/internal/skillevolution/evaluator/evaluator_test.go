package evaluator

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

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
		t.Fatalf("new gates: %v", err)
	}
	return gates
}

func newEvaluator(t *testing.T) *Evaluator {
	t.Helper()
	ev, err := New(loadGates(t))
	if err != nil {
		t.Fatalf("new evaluator: %v", err)
	}
	return ev
}

func jnum(v int64) json.Number { return json.Number(strconv.FormatInt(v, 10)) }

func skillRef(lineage string, version int64, digest string) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaSkillArtifactRef,
		"lineage_id":      lineage,
		"version":         num(version),
		"kind":            "step_guidance",
		"artifact_digest": digest,
	}
}

const (
	digestAlphaV3 = "sha256:dedd43cd5311628440bf8771574d788c0075ec864397f27df48f12280a3eb156"
	digestBetaV2  = "sha256:cee39cecbcdf53f736d8b32026858bf60f065cbbf47a000d9ff7f5947ec3fddc"
)

var (
	refAlphaV3 = skillRef("skill-alpha", 3, digestAlphaV3)
	refBetaV2  = skillRef("skill-beta", 2, digestBetaV2)

	candidateRef = contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   "cand-0001",
		Kind:          "step_guidance",
		BodyDigest:    "sha256:5ca2d3d975102507ad670945a8b77becae6eec70ce5d9463b0716867365bad34",
		OriginType:    "skill_proposal",
		OriginRef:     contract.VersionedRef{ID: "prop-0001", Version: "1", Digest: "sha256:4dbfa5e4285010517492150d190b8d574bab2034e41481fff19f475fc0fedbc2"},
	}

	adapterRef = map[string]any{
		"id": "rsih.fake-runtime-adapter", "version": jnum(1),
		"digest": "sha256:b61822af49ed662c6e882237e1e85b27b3ead0f18869a079d9ba5a7697217803",
	}
	profileRef = map[string]any{
		"id": "rsih.replay-profile", "version": jnum(1),
		"digest": "sha256:536aa4b6b9489674b74ba2daa169650346f08911a0410a99b0d0e59a4716fb09",
	}
	releaseRuleRef  = contract.VersionedRef{ID: "policy.release-rule.v1", Version: "2", Digest: "sha256:97c2588ff0cd13940d0af548dabc8b19a517eb23b24cda5463910fc6ddef69f3"}
	comparatorRefID = contract.VersionedRef{ID: "policy.utility-comparator.v1", Version: "1", Digest: "sha256:b1cc45a49b4e9d094457f30b980ab0b454290bb97e9afd5735934957d7fbffb8"}
	validationRef   = contract.VersionedRef{ID: "valid-0001", Version: "1", Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"}
)

// ---------------------------------------------------------------------------
// Synthetic paired-replay corpora built through the real replay
// canonicalizer (the evaluator never trusts counts it cannot recompute).
// ---------------------------------------------------------------------------

type sideSpec struct {
	runStatus string // "succeeded" | "failed"
	reason    string // registry reason code, "" when succeeded
	digest    string // expected output digest of this side
	cost      int64
}

func digestOf(t *testing.T, seed string) string {
	t.Helper()
	return contract.DigestBytes([]byte(seed))
}

// buildResult canonicalizes a synthetic paired corpus.
//
// cases: one entry per packet: baseline-side and candidate-side expected
// behavior. Every family is critical unless critical=false.
func buildResult(t *testing.T, domain string, cases []caseSpec, idsuffix string) *replay.Result {
	t.Helper()
	schemas := loadGates(t)
	policyDir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(policyDir, "policy"))
	if err != nil {
		t.Fatalf("load system reason policy: %v", err)
	}
	svc, err := replay.NewService(schemas, policy)
	if err != nil {
		t.Fatalf("new replay service: %v", err)
	}

	var plan replay.Plan
	var runs []replay.RunOutput
	for _, c := range cases {
		var domains []string
		if c.recovery {
			domains = []string{"recovery"}
		}
		baselinePacket := replay.Packet{
			ID:                   c.id,
			Side:                 replay.SideBaseline,
			Critical:             c.critical,
			PathDomains:          domains,
			ExpectedRunStatus:    c.baseline.runStatus,
			ExpectedReasonCode:   c.baseline.reason,
			ExpectedOutputDigest: c.baseline.digest,
			ExpectedArtifactRef:  refAlphaV3,
		}
		candidatePacket := baselinePacket
		candidatePacket.Side = replay.SideCandidate
		candidatePacket.ExpectedRunStatus = c.candidate.runStatus
		candidatePacket.ExpectedReasonCode = c.candidate.reason
		candidatePacket.ExpectedOutputDigest = c.candidate.digest
		candidatePacket.ExpectedArtifactRef = candidateRefJSON()

		plan.Families = append(plan.Families, replay.Family{
			Name:     c.name,
			Domain:   domain,
			Sides:    []string{replay.SideBaseline, replay.SideCandidate},
			Baseline: refAlphaV3,
			Packets:  []replay.Packet{baselinePacket, candidatePacket},
		})
		runs = append(runs,
			replay.RunOutput{
				PacketID: c.id, Side: replay.SideBaseline,
				RunStatus: c.baseline.runStatus, ReasonCode: c.baseline.reason,
				OutputDigest: c.baseline.digest, CostUnits: c.baseline.cost,
				ArtifactRef: refAlphaV3, AdapterRef: adapterRef, ProfileRef: profileRef,
			},
			replay.RunOutput{
				PacketID: c.id, Side: replay.SideCandidate,
				RunStatus: c.candidate.runStatus, ReasonCode: c.candidate.reason,
				OutputDigest: c.candidate.digest, CostUnits: c.candidate.cost,
				ArtifactRef: candidateRefJSON(), AdapterRef: adapterRef, ProfileRef: profileRef,
			},
		)
	}

	req, err := svc.ParseRequest(syntheticRequestDoc(t, idsuffix))
	if err != nil {
		t.Fatalf("parse synthetic replay request: %v", err)
	}
	result, err := svc.Canonicalize(req, &plan, runs)
	if err != nil {
		t.Fatalf("canonicalize synthetic corpus: %v (code %s)", err, replay.CodeOf(err))
	}
	return result
}

func candidateRefJSON() map[string]any {
	return map[string]any{
		"schema_version": contract.SchemaCandidateArtifactRef,
		"candidate_id":   candidateRef.CandidateID,
		"kind":           candidateRef.Kind,
		"body_digest":    candidateRef.BodyDigest,
		"origin_type":    candidateRef.OriginType,
		"origin_ref": map[string]any{
			"id": candidateRef.OriginRef.ID, "version": jnum(1), "digest": candidateRef.OriginRef.Digest,
		},
	}
}

type caseSpec struct {
	id        string
	name      string
	baseline  sideSpec
	candidate sideSpec
	critical  bool
	recovery  bool
}

// improvementCorpus: baseline passes 2/4 (one semantic failure, one infra
// inconclusive), candidate passes 3/4 with the recovery case recovered.
func improvementCorpus(t *testing.T, idsuffix string) *replay.Result {
	t.Helper()
	return buildResult(t, "source_a", []caseSpec{
		{"pk-both-pass", "f-both", sideSpec{"succeeded", "", digestOf(t, "b1"), 3}, sideSpec{"succeeded", "", digestOf(t, "c1"), 3}, true, false},
		{"pk-both-pass-2", "f-both2", sideSpec{"succeeded", "", digestOf(t, "b2"), 3}, sideSpec{"succeeded", "", digestOf(t, "c2"), 3}, true, false},
		{"pk-recovered", "f-recovered", sideSpec{"failed", "FAKE_CLOCK_EXHAUSTED", digestOf(t, "b3"), 3}, sideSpec{"succeeded", "", digestOf(t, "c3"), 3}, true, true},
		{"pk-infra-stays", "f-infra", sideSpec{"failed", "FAKE_TOOL_NO_RESPONSE", digestOf(t, "b4"), 3}, sideSpec{"failed", "FAKE_TOOL_NO_RESPONSE", digestOf(t, "c4"), 3}, true, false},
	}, idsuffix)
}

func baseInput(result *replay.Result) Input {
	return Input{
		DecisionID:           "decision-0001",
		Candidate:            candidateRef,
		ResultDoc:            result.Doc(),
		Records:              result.Records(),
		Envelope:             result.Envelope(),
		EnvelopedCases:       result.EnvelopeTotalCases(),
		CandidateCases:       result.CandidateTotalCases(),
		RequestBaselines:     []contract.SkillArtifactRef{skillRefTyped("skill-alpha", 3, digestAlphaV3)},
		ValidationRecordRefs: []contract.VersionedRef{validationRef},
		StaticGates:          []StaticGate{{Code: "schema_canonicalization_digest", Passed: true}, {Code: "exact_ref_resolution", Passed: true}},
		ReleaseRule:          ReleaseRule{Ref: releaseRuleRef, RequiredDomains: []string{"source_a"}},
	}
}

func skillRefTyped(lineage string, version int64, digest string) contract.SkillArtifactRef {
	return contract.SkillArtifactRef{
		SchemaVersion:  contract.SchemaSkillArtifactRef,
		LineageID:      lineage,
		Version:        strconv.FormatInt(version, 10),
		Kind:           "step_guidance",
		ArtifactDigest: digest,
	}
}

func defaultComparator() Comparator {
	return Comparator{
		Ref:                    comparatorRefID,
		CostBudgetUnits:        100,
		CriticalDomains:        []string{"source_a", "source_b", "overlap"},
		ComparisonMode:         ModeRate,
		MinImprovementRatioNum: 1,
		MinImprovementRatioDen: 1,
	}
}

// ---------------------------------------------------------------------------
// GMS-203 Red test #1 (plan-pinned).
// ---------------------------------------------------------------------------

func TestEvaluatorAppliesHardGatesAndIntegerParetoEnvelope(t *testing.T) {
	ev := newEvaluator(t)

	// --- accepted: strict improvement, hard gates green, heads fresh -----
	result := improvementCorpus(t, "acc")
	in := baseInput(result)
	decision, err := ev.Decide(defaultComparator(), in)
	if err != nil {
		t.Fatalf("decide: %v (code %s)", err, CodeOf(err))
	}
	if decision.Outcome() != OutcomeAccepted {
		t.Fatalf("outcome = %q (%v), want accepted", decision.Outcome(), decision.ReasonCodes())
	}
	if len(decision.ReasonCodes()) != 0 {
		t.Fatalf("accepted decision reason_codes = %v, want empty", decision.ReasonCodes())
	}
	for _, gate := range decision.HardGateResults() {
		if !gate.Passed {
			t.Fatalf("accepted decision has failed gate %s", gate.GateCode)
		}
	}
	if err := loadGates(t).ValidateInstance(decision.Doc(), SchemaReleaseDecision); err != nil {
		t.Fatalf("decision document invalid against authority schema: %v", err)
	}

	// Decision digest is stable for identical frozen inputs and changes
	// with the outcome.
	again, err := ev.Decide(defaultComparator(), in)
	if err != nil {
		t.Fatalf("second decide: %v", err)
	}
	if decision.DecisionDigest() != again.DecisionDigest() {
		t.Fatalf("decision digest not stable: %s vs %s", decision.DecisionDigest(), again.DecisionDigest())
	}

	// --- critical regression rejects ------------------------------------
	// Baseline passes the critical slice, candidate semantically fails it.
	regressed := buildResult(t, "source_a", []caseSpec{
		{"pk-flip", "f-flip", sideSpec{"succeeded", "", digestOf(t, "rb1"), 3}, sideSpec{"failed", "FAKE_CLOCK_EXHAUSTED", digestOf(t, "rc1"), 3}, true, false},
	}, "rej-crit")
	d := decideOrFatal(t, ev, defaultComparator(), baseInput(regressed))
	if d.Outcome() != OutcomeRejected {
		t.Fatalf("critical regression outcome = %q, want rejected", d.Outcome())
	}
	if !contains(d.ReasonCodes(), ReasonCriticalRegression) {
		t.Fatalf("critical regression reason codes = %v, want %s", d.ReasonCodes(), ReasonCriticalRegression)
	}

	// --- cost budget exceeded rejects -----------------------------------
	costly := buildResult(t, "source_a", []caseSpec{
		{"pk-costly", "f-costly", sideSpec{"succeeded", "", digestOf(t, "xb1"), 3}, sideSpec{"succeeded", "", digestOf(t, "xc1"), 3}, true, false},
		{"pk-costly-2", "f-costly2", sideSpec{"failed", "FAKE_CLOCK_EXHAUSTED", digestOf(t, "xb2"), 3}, sideSpec{"succeeded", "", digestOf(t, "xc2"), 5000}, true, true},
	}, "rej-cost")
	cmp := defaultComparator()
	cmp.CostBudgetUnits = 100
	d = decideOrFatal(t, ev, cmp, baseInput(costly))
	if d.Outcome() != OutcomeRejected || !contains(d.ReasonCodes(), ReasonUtilityCostBudgetExceeded) {
		t.Fatalf("cost over budget outcome/reasons = %q/%v, want rejected/%s", d.Outcome(), d.ReasonCodes(), ReasonUtilityCostBudgetExceeded)
	}

	// --- cost-only improvement rejects ----------------------------------
	costOnly := buildResult(t, "source_a", []caseSpec{
		{"pk-eq", "f-eq", sideSpec{"succeeded", "", digestOf(t, "yb1"), 9}, sideSpec{"succeeded", "", digestOf(t, "yc1"), 1}, true, false},
	}, "rej-costonly")
	d = decideOrFatal(t, ev, defaultComparator(), baseInput(costOnly))
	if d.Outcome() != OutcomeRejected || !contains(d.ReasonCodes(), ReasonUtilityNotParetoImproved) {
		t.Fatalf("cost-only outcome/reasons = %q/%v, want rejected/%s", d.Outcome(), d.ReasonCodes(), ReasonUtilityNotParetoImproved)
	}

	// --- infra success impersonating a semantic pass rejects -------------
	impersonated := improvementCorpus(t, "rej-infra")
	doc := impersonated.Doc()
	// Forge: count the candidate-side infra case (FAKE_TOOL_NO_RESPONSE)
	// as a candidate pass, and hide it from the inconclusive count.
	outcomes, _ := contract.AsArray(doc["fixture_outcomes"])
	for _, raw := range outcomes {
		outcome, _ := contract.AsObject(raw)
		fixtureRef, _ := contract.AsObject(outcome["fixture_ref"])
		if id, _ := contract.AsString(fixtureRef["id"]); id == "f-infra" {
			outcome["candidate_passed"] = jnum(1)
			outcome["critical_regression_count"] = jnum(0)
		}
	}
	uv, _ := contract.AsObject(doc["utility_vector"])
	uv["inconclusive_case_count"] = jnum(0)
	uv["task_success_count"] = jnum(4)
	redigest(t, doc)
	in = baseInput(impersonated)
	in.ResultDoc = doc
	d = decideOrFatal(t, ev, defaultComparator(), in)
	if d.Outcome() != OutcomeRejected || !contains(d.ReasonCodes(), ReasonReplayResultInvalid) {
		t.Fatalf("infra impersonation outcome/reasons = %q/%v, want rejected/%s", d.Outcome(), d.ReasonCodes(), ReasonReplayResultInvalid)
	}

	// --- model scores cannot enter the decision input --------------------
	scored := deepCopyDoc(improvementCorpus(t, "rej-model").Doc())
	scored["model_utility_score"] = jnum(99) // unknown core field
	in = baseInput(improvementCorpus(t, "rej-model"))
	in.ResultDoc = scored
	d = decideOrFatal(t, ev, defaultComparator(), in)
	if d.Outcome() != OutcomeRejected || !contains(d.ReasonCodes(), ReasonReplayResultInvalid) {
		t.Fatalf("model-scored result outcome/reasons = %q/%v, want rejected/%s", d.Outcome(), d.ReasonCodes(), ReasonReplayResultInvalid)
	}

	// --- stale source head is inconclusive -------------------------------
	in = baseInput(improvementCorpus(t, "inc-stale"))
	in.ExpectedSourceHeads = []contract.SkillArtifactRef{skillRefTyped("skill-alpha", 3, digestAlphaV3)}
	in.ObservedSourceHeads = []contract.SkillArtifactRef{skillRefTyped("skill-alpha", 4, digestOf(t, "moved-head"))}
	d = decideOrFatal(t, ev, defaultComparator(), in)
	if d.Outcome() != OutcomeInconclusive || !contains(d.ReasonCodes(), ReasonSourceHeadStale) {
		t.Fatalf("stale head outcome/reasons = %q/%v, want inconclusive/%s", d.Outcome(), d.ReasonCodes(), ReasonSourceHeadStale)
	}

	// --- arithmetic overflow of the frozen improvement ratio is
	// inconclusive -------------------------------------------------------
	result = improvementCorpus(t, "inc-overflow")
	in = baseInput(result)
	cmp = defaultComparator()
	cmp.MinImprovementRatioNum = int64(1) << 62
	cmp.MinImprovementRatioDen = 1
	d = decideOrFatal(t, ev, cmp, in)
	if d.Outcome() != OutcomeInconclusive || !contains(d.ReasonCodes(), ReasonUtilityArithmeticOverflow) {
		t.Fatalf("overflow outcome/reasons = %q/%v, want inconclusive/%s", d.Outcome(), d.ReasonCodes(), ReasonUtilityArithmeticOverflow)
	}

	// --- hard gate failure cannot be overridden by utility ---------------
	result = improvementCorpus(t, "rej-static")
	in = baseInput(result)
	in.StaticGates = []StaticGate{{Code: "exact_ref_resolution", Passed: false}}
	d = decideOrFatal(t, ev, defaultComparator(), in)
	if d.Outcome() != OutcomeRejected || !contains(d.ReasonCodes(), ReasonHardGateFailed) {
		t.Fatalf("static gate failure outcome/reasons = %q/%v, want rejected/%s", d.Outcome(), d.ReasonCodes(), ReasonHardGateFailed)
	}

	// --- inconclusive replay result yields an inconclusive decision ------
	ndResult := nondeterministicResult(t)
	in = baseInput(ndResult)
	in.ResultDoc = ndResult.Doc()
	d = decideOrFatal(t, ev, defaultComparator(), in)
	if d.Outcome() != OutcomeInconclusive || !contains(d.ReasonCodes(), replay.ReasonReplayNondeterministic) {
		t.Fatalf("nondeterministic replay outcome/reasons = %q/%v, want inconclusive/%s", d.Outcome(), d.ReasonCodes(), replay.ReasonReplayNondeterministic)
	}
}

// nondeterministicResult canonicalizes a corpus whose candidate packet was
// rerun with a diverging output digest.
func nondeterministicResult(t *testing.T) *replay.Result {
	t.Helper()
	// Rebuild with a second, diverging candidate run: go through the
	// service directly.
	schemas := loadGates(t)
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load reason policy: %v", err)
	}
	svc, err := replay.NewService(schemas, policy)
	if err != nil {
		t.Fatalf("new replay service: %v", err)
	}
	requestDoc := syntheticRequestDoc(t, "nd2")
	plan := syntheticPlan(t, "nd2")
	runs := syntheticRuns(t, "nd2")
	runs = append(runs, replay.RunOutput{
		PacketID: "pk-both-pass", Side: "candidate",
		RunStatus: "succeeded", OutputDigest: "sha256:" + repeat('b', 64),
		CostUnits: 3, ArtifactRef: candidateRefJSON(), AdapterRef: adapterRef, ProfileRef: profileRef,
	})
	req, err := svc.ParseRequest(requestDoc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := svc.Canonicalize(req, plan, runs)
	if err != nil {
		t.Fatalf("canonicalize nondeterministic corpus: %v", err)
	}
	return res
}

// Synthetic corpus plumbing shared with nondeterministicResult.
func syntheticCorpus(t *testing.T, idsuffix string) (map[string]any, *replay.Plan, []replay.RunOutput) {
	t.Helper()
	plan := &replay.Plan{Families: []replay.Family{{
		Name: "f-both", Domain: "source_a", Sides: []string{"baseline", "candidate"},
		Baseline: refAlphaV3,
		Packets: []replay.Packet{
			{ID: "pk-both-pass", Side: "baseline", Critical: true,
				ExpectedRunStatus: "succeeded", ExpectedOutputDigest: digestOf(t, "b1"), ExpectedArtifactRef: refAlphaV3},
			{ID: "pk-both-pass", Side: "candidate", Critical: true,
				ExpectedRunStatus: "succeeded", ExpectedOutputDigest: digestOf(t, "c1"), ExpectedArtifactRef: candidateRefJSON()},
		},
	}}}
	runs := []replay.RunOutput{
		{PacketID: "pk-both-pass", Side: "baseline", RunStatus: "succeeded",
			OutputDigest: digestOf(t, "b1"), CostUnits: 3, ArtifactRef: refAlphaV3, AdapterRef: adapterRef, ProfileRef: profileRef},
		{PacketID: "pk-both-pass", Side: "candidate", RunStatus: "succeeded",
			OutputDigest: digestOf(t, "c1"), CostUnits: 3, ArtifactRef: candidateRefJSON(), AdapterRef: adapterRef, ProfileRef: profileRef},
	}
	return syntheticRequestDoc(t, idsuffix), plan, runs
}

func syntheticRequestDoc(t *testing.T, idsuffix string) map[string]any {
	t.Helper()
	return map[string]any{
		"schema_version":      "gms.replay-request.v1",
		"replay_request_id":   "replay-" + idsuffix,
		"candidate_ref":       candidateRefJSON(),
		"baseline_skill_refs": []any{refAlphaV3},
		"fixture_set_refs":    []any{map[string]any{"id": "fixture-" + idsuffix, "version": jnum(1), "digest": digestOf(t, "fixture-"+idsuffix)}},
		"segment_refs": []any{map[string]any{
			"schema_version": "host.segment-ref.v1", "room_id": "room-alpha-0001",
			"segment_id": "seg-success-0001", "segment_version": jnum(1),
			"segment_digest":    digestOf(t, "segment-"+idsuffix),
			"evidence_seal_ref": map[string]any{"id": "evseal-0001", "version": jnum(1), "digest": digestOf(t, "evseal-"+idsuffix)},
			"path_seal_ref":     map[string]any{"id": "pathseal-0001", "version": jnum(1), "digest": digestOf(t, "pathseal-"+idsuffix)},
		}},
		"replay_profile_ref":  profileRef,
		"runtime_adapter_ref": adapterRef,
		"mode":                "causal_evaluation",
		"idempotency_key":     digestOf(t, "idem-"+idsuffix),
	}
}

func syntheticPlan(t *testing.T, idsuffix string) *replay.Plan {
	t.Helper()
	_, plan, _ := syntheticCorpus(t, idsuffix)
	return plan
}

func syntheticRuns(t *testing.T, idsuffix string) []replay.RunOutput {
	t.Helper()
	_, _, runs := syntheticCorpus(t, idsuffix)
	return runs
}

// ---------------------------------------------------------------------------
// U1 integer comparator matrix.
// ---------------------------------------------------------------------------

func TestU1IntegerComparatorMatrix(t *testing.T) {
	// Strict improvement passes (rate comparison via cross multiplication:
	// 3/4 vs 2/4).
	verdict := compareVectors(t, ModeRate, 1, 1,
		vec(3, 3, 1, 1, 10), 4, vec(2, 2, 0, 1, 10), 4)
	if verdict != VerdictAccept {
		t.Fatalf("strict improvement verdict = %q, want accept", verdict)
	}

	// All-equal rejects (no strict improvement).
	verdict = compareVectors(t, ModeRate, 1, 1,
		vec(2, 2, 0, 1, 10), 4, vec(2, 2, 0, 1, 10), 4)
	if verdict != VerdictNotImproved {
		t.Fatalf("all-equal verdict = %q, want not-improved", verdict)
	}

	// Cost-only improvement (all primary equal, cost strictly lower)
	// rejects.
	verdict = compareVectors(t, ModeRate, 1, 1,
		vec(2, 2, 0, 1, 2), 4, vec(2, 2, 0, 1, 10), 4)
	if verdict != VerdictNotImproved {
		t.Fatalf("cost-only verdict = %q, want not-improved", verdict)
	}

	// Single primary-dimension regression rejects even with another
	// dimension improved (conclusive_case_count minimized: 2/4 > 1/4).
	verdict = compareVectors(t, ModeRate, 1, 1,
		vec(3, 3, 1, 2, 10), 4, vec(2, 2, 0, 1, 10), 4)
	if verdict != VerdictNotImproved {
		t.Fatalf("single-dim regression verdict = %q, want not-improved", verdict)
	}

	// Rate cross multiplication with equal counts but different totals:
	// candidate 2/4 is worse than baseline 2/3 (2*3=6 < 2*4=8) even though
	// the raw counts are equal — raw-count comparison would call this
	// equal.
	verdict = compareVectors(t, ModeRate, 1, 1,
		vec(2, 2, 0, 0, 10), 4, vec(2, 2, 0, 0, 10), 3)
	if verdict != VerdictNotImproved {
		t.Fatalf("equal-counts-worse-rate verdict = %q, want not-improved", verdict)
	}

	// Raw mode compares integer counts directly.
	verdict = compareVectors(t, ModeRaw, 1, 1,
		vec(2, 2, 0, 0, 10), 1, vec(2, 2, 0, 0, 10), 1)
	if verdict != VerdictNotImproved {
		t.Fatalf("raw all-equal verdict = %q, want not-improved", verdict)
	}

	// Overflow of the cross products fails closed as overflow: the strict
	// improvement 4 vs 3 must meet ratio 2^62/1, and 3*2^62 overflows
	// int64 — the comparison is unprovable, so it can never accept.
	_, err := CompareUtility(replay.UtilityVector{
		TaskSuccessCount: 4, CriticalBranchPassCount: 4, RecoverySuccessCount: 4,
		InconclusiveCaseCount: 0, ExecutionCostUnits: 10,
	}, 1, replay.UtilityVector{
		TaskSuccessCount: 3, CriticalBranchPassCount: 4,
		RecoverySuccessCount: 4, InconclusiveCaseCount: 0, ExecutionCostUnits: 10,
	}, 1, ModeRaw, int64(1)<<62, 1)
	if err == nil || CodeOf(err) != ReasonUtilityArithmeticOverflow {
		t.Fatalf("overflow comparison must fail closed with %s, got %v (%s)", ReasonUtilityArithmeticOverflow, err, CodeOf(err))
	}

	// Invalid denominator fails closed (never divides).
	if _, err := CompareUtility(vec(1, 1, 0, 0, 1), 1, vec(1, 1, 0, 0, 1), 0, ModeRaw, 1, 1); err == nil || CodeOf(err) != ReasonUtilityArithmeticOverflow {
		t.Fatalf("zero denominator must fail closed, got %v (%s)", err, CodeOf(err))
	}
}

func vec(task, critical, recovery, inconclusive, cost int64) replay.UtilityVector {
	return replay.UtilityVector{
		TaskSuccessCount: task, CriticalBranchPassCount: critical,
		RecoverySuccessCount: recovery, InconclusiveCaseCount: inconclusive,
		ExecutionCostUnits: cost,
	}
}

func compareVectors(t *testing.T, mode string, ratioNum, ratioDen int64, candidate replay.UtilityVector, candTotal int64, envelope replay.UtilityVector, envTotal int64) Verdict {
	t.Helper()
	verdict, err := CompareUtility(candidate, candTotal, envelope, envTotal, mode, ratioNum, ratioDen)
	if err != nil {
		t.Fatalf("compare utility: %v", err)
	}
	return verdict
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func decideOrFatal(t *testing.T, ev *Evaluator, cmp Comparator, in Input) *Decision {
	t.Helper()
	decision, err := ev.Decide(cmp, in)
	if err != nil {
		t.Fatalf("decide: %v (code %s)", err, CodeOf(err))
	}
	return decision
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func repeat(c byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

func deepCopyDoc(doc map[string]any) map[string]any {
	data, err := contract.JCS(doc)
	if err != nil {
		panic(err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		panic(err)
	}
	out, _ := contract.AsObject(value)
	return out
}

// redigest recomputes the §7.11 result_digest of a (forged) document so the
// schema gate passes and the semantic recompute gates are what rejects it.
func redigest(t *testing.T, doc map[string]any) {
	t.Helper()
	delete(doc, "result_digest")
	core := map[string]any{}
	for _, field := range []string{"schema_version", "replay_result_id", "replay_request_ref", "candidate_ref", "status", "fixture_outcomes", "utility_vector"} {
		core[field] = doc[field]
	}
	digest, err := contract.DigestOf(core)
	if err != nil {
		t.Fatalf("redigest: %v", err)
	}
	doc["result_digest"] = digest
}
