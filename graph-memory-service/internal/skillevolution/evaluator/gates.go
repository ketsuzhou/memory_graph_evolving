package evaluator

import (
	"encoding/json"
	"strconv"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
)

// Hard gate codes recorded in §7.12 hard_gate_results (the §10.2 list:
// static gates re-recorded from GMS-202 records, replay-owned gates
// computed here).
const (
	GateStaticValidation  = "static_validation_gates"
	GateSchemaDigest      = "replay_result_schema_digest"
	GateExactRefs         = "exact_ref_resolution"
	GateFixtureCompletion = "fixture_completeness"
	GateCriticalSlices    = "critical_slices_zero_regression"
	GateInfraSeparation   = "infra_semantic_separation"
	GateCostBudget        = "cost_budget"
	GateSourceHeads       = "source_head_freshness"
	GateReferenceEnvelope = "reference_envelope"
	GateUtilityPareto     = "utility_pareto_u1"
)

// gateOrder is the deterministic recording order of §7.12 hard_gate_results.
var gateOrder = []string{
	GateStaticValidation, GateSchemaDigest, GateExactRefs,
	GateFixtureCompletion, GateCriticalSlices, GateInfraSeparation,
	GateCostBudget, GateSourceHeads, GateReferenceEnvelope, GateUtilityPareto,
}

// gateFindings carries every gate outcome plus the failure reason codes.
type gateFindings struct {
	results      map[string]GateResult
	reasons      []string // rejection reasons (deterministic policy violations)
	inconclusive []string // inconclusive reasons (input cannot support a decision)
}

func newFindings() *gateFindings {
	results := make(map[string]GateResult, len(gateOrder))
	for _, code := range gateOrder {
		results[code] = GateResult{GateCode: code, Passed: false}
	}
	return &gateFindings{results: results}
}

func (f *gateFindings) pass(code string, refs ...contract.VersionedRef) {
	f.results[code] = GateResult{GateCode: code, Passed: true, RecordRefs: refs}
}

func (f *gateFindings) fail(code string, reason string) {
	f.results[code] = GateResult{GateCode: code, Passed: false}
	if reason != "" {
		f.reasons = appendUnique(f.reasons, reason)
	}
}

// failInconclusive records a gate that could not be evaluated because the
// decision INPUT is unusable (stale heads, empty comparison basis): the
// outcome is inconclusive, never a candidate verdict.
func (f *gateFindings) failInconclusive(code string, reason string) {
	f.results[code] = GateResult{GateCode: code, Passed: false}
	if reason != "" {
		f.inconclusive = appendUnique(f.inconclusive, reason)
	}
}

func (f *gateFindings) ordered() []GateResult {
	out := make([]GateResult, 0, len(gateOrder))
	for _, code := range gateOrder {
		out = append(out, f.results[code])
	}
	return out
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// evaluateStaticGates re-records the upstream GMS-202 static gate results
// (§10.2: schema/canonicalization/digest, exact refs, required extensions,
// authority/provenance, evidence committed, permission/Host cap,
// kind/lineage, ports/DAG/retry/failure, merge conflict closure). Any red
// static gate fails the decision — utility can never override a hard gate.
func (f *gateFindings) evaluateStaticGates(in *Input) {
	if len(in.StaticGates) == 0 {
		f.fail(GateStaticValidation, ReasonHardGateFailed)
		return
	}
	for _, gate := range in.StaticGates {
		if !gate.Passed {
			f.fail(GateStaticValidation, ReasonHardGateFailed)
			f.results[GateStaticValidation] = GateResult{
				GateCode: GateStaticValidation, Passed: false, RecordRefs: gate.RecordRefs,
			}
			return
		}
	}
	refs := in.ValidationRecordRefs
	f.results[GateStaticValidation] = GateResult{
		GateCode: GateStaticValidation, Passed: true, RecordRefs: refs,
	}
}

// evaluateSchemaDigest validates the §7.11 document against the shared
// authority schema with its x-digest recomputation: closed field set (no
// model-score fields can exist), closed enums, integer-only core and the
// result_digest preimage. Forged counts without a matching digest fail
// here; forged counts WITH a matching digest fail the recompute gates.
func (e *Evaluator) evaluateSchemaDigest(f *gateFindings, doc map[string]any) {
	if err := e.gates.ValidateInstance(deepCopyValue(doc), SchemaReplayResult); err != nil {
		f.fail(GateSchemaDigest, ReasonReplayResultInvalid)
		return
	}
	f.pass(GateSchemaDigest)
}

// evaluateExactRefs checks that the result's candidate ref is exactly the
// decision candidate and every fixture outcome's baseline ref resolves to
// one of the frozen request baselines (§6.2 exact triples).
func (f *gateFindings) evaluateExactRefs(in *Input, doc map[string]any) {
	candidateRaw, _ := contract.AsObject(doc["candidate_ref"])
	if candidateRaw == nil || contract.CanonicalKey(candidateRaw) != contract.CanonicalKey(candidateRefDoc(in.Candidate)) {
		f.fail(GateExactRefs, ReasonRefMismatch)
		return
	}
	outcomes, _ := contract.AsArray(doc["fixture_outcomes"])
	for _, raw := range outcomes {
		outcome, _ := contract.AsObject(raw)
		baselineRaw, _ := contract.AsObject(outcome["baseline_ref"])
		if baselineRaw == nil {
			f.fail(GateExactRefs, ReasonRefMismatch)
			return
		}
		ref, err := contract.ParseSkillArtifactRef(baselineRaw)
		if err != nil {
			f.fail(GateExactRefs, ReasonRefMismatch)
			return
		}
		if !refInList(ref, in.RequestBaselines) {
			f.fail(GateExactRefs, ReasonRefMismatch)
			return
		}
	}
	f.pass(GateExactRefs)
}

// evaluateFixtureCompleteness: the frozen fixture set must cover every
// domain the release rule requires (A/B/overlap for merges, the single
// source domain for ordinary revisions).
func (f *gateFindings) evaluateFixtureCompleteness(doc map[string]any, rule ReleaseRule) {
	covered := map[string]bool{}
	outcomes, _ := contract.AsArray(doc["fixture_outcomes"])
	for _, raw := range outcomes {
		outcome, _ := contract.AsObject(raw)
		domain, _ := contract.AsString(outcome["domain"])
		covered[domain] = true
	}
	for _, domain := range rule.RequiredDomains {
		if !covered[domain] {
			f.fail(GateFixtureCompletion, ReasonMissingFamily)
			return
		}
	}
	f.pass(GateFixtureCompletion)
}

// evaluateInfraSeparation re-derives every declared count from the
// canonicalizer's run classifications: infra-faulted and nondeterministic
// runs must never appear in baseline_passed/candidate_passed or the
// success dimensions of the utility vector (GMS §5.3: infra success must
// not impersonate a semantic pass), and the reference envelope must
// recompute from the baseline-side records.
func (f *gateFindings) evaluateInfraSeparation(in *Input, doc map[string]any) {
	// Recompute per-family counts (semantic passes only).
	familyCandidate := map[string]int64{}
	familyBaseline := map[string]int64{}
	for _, record := range in.Records {
		if !record.Passed() {
			continue
		}
		if record.Side == replay.SideCandidate {
			familyCandidate[record.Family]++
		} else {
			familyBaseline[record.Family]++
		}
	}
	outcomes, _ := contract.AsArray(doc["fixture_outcomes"])
	declaredFamilies := map[string]bool{}
	for _, raw := range outcomes {
		outcome, _ := contract.AsObject(raw)
		fixtureRef, _ := contract.AsObject(outcome["fixture_ref"])
		family, _ := contract.AsString(fixtureRef["id"])
		declaredFamilies[family] = true
		wantBaseline, bo := intOf(outcome["baseline_passed"])
		wantCandidate, co := intOf(outcome["candidate_passed"])
		if !bo || !co || familyBaseline[family] != wantBaseline || familyCandidate[family] != wantCandidate {
			f.fail(GateInfraSeparation, ReasonReplayResultInvalid)
			return
		}
	}
	// The recorded families must be exactly the classified families.
	recordFamilies := map[string]bool{}
	for _, record := range in.Records {
		recordFamilies[record.Family] = true
	}
	if len(recordFamilies) != len(declaredFamilies) {
		f.fail(GateInfraSeparation, ReasonReplayResultInvalid)
		return
	}

	// Recompute the utility vector (cost is execution evidence, not
	// classifiable; it is checked by the budget gate).
	var utility replay.UtilityVector
	var envelope replay.UtilityVector
	var candCases, envCases int64
	for _, record := range in.Records {
		if record.Side == replay.SideCandidate {
			candCases++
			switch {
			case record.Passed():
				utility.TaskSuccessCount++
				if record.Critical {
					utility.CriticalBranchPassCount++
				}
				if record.Recovery {
					utility.RecoverySuccessCount++
				}
			case record.Category == replay.CategoryInfra, record.Category == replay.CategoryNondeterministic:
				utility.InconclusiveCaseCount++
			}
		} else {
			envCases++
			switch {
			case record.Passed():
				envelope.TaskSuccessCount++
				if record.Critical {
					envelope.CriticalBranchPassCount++
				}
				if record.Recovery {
					envelope.RecoverySuccessCount++
				}
			case record.Category == replay.CategoryInfra, record.Category == replay.CategoryNondeterministic:
				envelope.InconclusiveCaseCount++
			}
		}
	}
	// Execution cost is execution evidence (replayed cost units), not a
	// classifiable outcome: the four primary dimensions must recompute
	// exactly; the cost dimension is enforced by the budget gate.
	utilityRaw, _ := contract.AsObject(doc["utility_vector"])
	want := utilityVectorOf(utilityRaw)
	if !want.ok || !primaryDimsEqual(want.vector, utility) {
		f.fail(GateInfraSeparation, ReasonReplayResultInvalid)
		return
	}
	if in.CandidateCases != candCases || in.EnvelopedCases != envCases {
		f.fail(GateInfraSeparation, ReasonReplayResultInvalid)
		return
	}
	if !primaryDimsEqual(in.Envelope, envelope) {
		// The reference envelope must recompute exactly (§10.4: frozen
		// baseline mapping; evaluation time cannot alter it): a
		// disagreeing envelope is a forged input, rejected.
		f.fail(GateInfraSeparation, ReasonReplayResultInvalid)
		return
	}
	f.pass(GateInfraSeparation)
}

// evaluateCriticalSlices: zero critical regressions on every critical
// slice (§10.3.2/3: A and B slices must not regress; MT4 requires both).
func (f *gateFindings) evaluateCriticalSlices(cmp Comparator, doc map[string]any, records []replay.RunRecord) {
	critical := map[string]bool{}
	for _, domain := range cmp.CriticalDomains {
		critical[domain] = true
	}
	regressed := int64(0)
	outcomes, _ := contract.AsArray(doc["fixture_outcomes"])
	for _, raw := range outcomes {
		outcome, _ := contract.AsObject(raw)
		domain, _ := contract.AsString(outcome["domain"])
		if !critical[domain] {
			continue
		}
		count, ok := intOf(outcome["critical_regression_count"])
		if !ok || count < 0 {
			f.fail(GateCriticalSlices, ReasonReplayResultInvalid)
			return
		}
		regressed += count
	}
	// Independent recompute over the critical records: any critical
	// baseline-pass packet whose candidate side did not pass.
	byPacket := map[string]map[string]replay.RunRecord{}
	for _, record := range records {
		if !record.Critical {
			continue
		}
		if byPacket[record.PacketID] == nil {
			byPacket[record.PacketID] = map[string]replay.RunRecord{}
		}
		byPacket[record.PacketID][record.Side] = record
	}
	recomputed := int64(0)
	for _, sides := range byPacket {
		baseline, hasBaseline := sides[replay.SideBaseline]
		candidate, hasCandidate := sides[replay.SideCandidate]
		if hasBaseline && baseline.Passed() && (!hasCandidate || !candidate.Passed()) {
			recomputed++
		}
	}
	if regressed != recomputed || regressed > 0 {
		f.fail(GateCriticalSlices, ReasonCriticalRegression)
		return
	}
	f.pass(GateCriticalSlices)
}

// evaluateCostBudget: cost within the versioned integer budget (§10.3.6).
func (f *gateFindings) evaluateCostBudget(cmp Comparator, doc map[string]any) {
	utilityRaw, _ := contract.AsObject(doc["utility_vector"])
	parsed := utilityVectorOf(utilityRaw)
	if !parsed.ok {
		f.fail(GateCostBudget, ReasonReplayResultInvalid)
		return
	}
	if !withinCostBudget(parsed.vector.ExecutionCostUnits, cmp.CostBudgetUnits) {
		f.fail(GateCostBudget, ReasonUtilityCostBudgetExceeded)
		return
	}
	f.pass(GateCostBudget)
}

// evaluateSourceHeads: the frozen source-head expectations must still be
// exactly the currently active heads (GMS §6.5; M5). Any movement is not a
// candidate verdict but an unusable decision input → inconclusive.
func (f *gateFindings) evaluateSourceHeads(in *Input) {
	if len(in.ExpectedSourceHeads) == 0 && len(in.ObservedSourceHeads) == 0 {
		// Ordinary single-source revisions freeze the target head in the
		// activation transaction (GMS-204); nothing to check here.
		f.pass(GateSourceHeads)
		return
	}
	if headSetKey(in.ExpectedSourceHeads) != headSetKey(in.ObservedSourceHeads) {
		f.failInconclusive(GateSourceHeads, ReasonSourceHeadStale)
		return
	}
	f.pass(GateSourceHeads)
}

// evaluateEnvelope: the reference envelope is usable (families and case
// totals line up with the classified runs — the numeric recompute lives in
// the infra-separation gate; here the pairing basis itself).
func (f *gateFindings) evaluateEnvelope(in *Input, doc map[string]any) {
	if in.EnvelopedCases < 1 || in.CandidateCases < 1 {
		f.failInconclusive(GateReferenceEnvelope, ReasonReferenceEnvelopeInvalid)
		return
	}
	f.pass(GateReferenceEnvelope)
}

// --- helpers -----------------------------------------------------------------

func refInList(ref contract.SkillArtifactRef, list []contract.SkillArtifactRef) bool {
	for _, candidate := range list {
		if candidate.LineageID == ref.LineageID && candidate.Version == ref.Version &&
			candidate.ArtifactDigest == ref.ArtifactDigest && candidate.Kind == ref.Kind {
			return true
		}
	}
	return false
}

// parseInt64 reads an exact integer JSON number as int64, failing on any
// non-integer form and on values outside int64 (never truncated).
func parseInt64(raw any) (int64, error) {
	return strconv.ParseInt(string(raw.(json.Number)), 10, 64)
}

// intOf reads an exact integer JSON number (no floats, no truncation).
func intOf(raw any) (int64, bool) {
	if !contract.IsIntegerNumber(raw) {
		return 0, false
	}
	value, err := parseInt64(raw)
	if err != nil {
		return 0, false
	}
	return value, true
}

type parsedUtility struct {
	vector replay.UtilityVector
	ok     bool
}

func utilityVectorOf(obj map[string]any) parsedUtility {
	if obj == nil {
		return parsedUtility{}
	}
	task, o1 := intOf(obj["task_success_count"])
	critical, o2 := intOf(obj["critical_branch_pass_count"])
	recovery, o3 := intOf(obj["recovery_success_count"])
	inconclusive, o4 := intOf(obj["inconclusive_case_count"])
	cost, o5 := intOf(obj["execution_cost_units"])
	if !(o1 && o2 && o3 && o4 && o5) {
		return parsedUtility{}
	}
	return parsedUtility{ok: true, vector: replay.UtilityVector{
		TaskSuccessCount: task, CriticalBranchPassCount: critical,
		RecoverySuccessCount: recovery, InconclusiveCaseCount: inconclusive,
		ExecutionCostUnits: cost,
	}}
}

// primaryDimsEqual compares the four primary dimensions of two vectors
// (cost excluded — it is budget-gated execution evidence).
func primaryDimsEqual(a, b replay.UtilityVector) bool {
	return a.TaskSuccessCount == b.TaskSuccessCount &&
		a.CriticalBranchPassCount == b.CriticalBranchPassCount &&
		a.RecoverySuccessCount == b.RecoverySuccessCount &&
		a.InconclusiveCaseCount == b.InconclusiveCaseCount
}

// headSetKey builds the canonical exact-triple set key of a head list.
func headSetKey(heads []contract.SkillArtifactRef) string {
	keys := make([]string, 0, len(heads))
	for _, head := range heads {
		keys = append(keys, contract.CanonicalKey(map[string]any{
			"lineage_id": head.LineageID, "version": jsonNumber(head.Version),
			"kind": head.Kind, "artifact_digest": head.ArtifactDigest,
		}))
	}
	return contract.CanonicalKey(keys)
}
