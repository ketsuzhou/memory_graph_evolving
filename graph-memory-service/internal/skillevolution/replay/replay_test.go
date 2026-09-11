package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Shared test wiring: the CTR-005 authority schemas and the digest-verified
// reason registry are the frozen inputs of the canonicalizer (GMS-203).
// ---------------------------------------------------------------------------

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

func loadPolicy(t *testing.T) *contract.ReasonPolicy {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load system reason policy: %v", err)
	}
	return policy
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := NewService(loadGates(t), loadPolicy(t))
	if err != nil {
		t.Fatalf("new replay service: %v", err)
	}
	return svc
}

func q29bDir(t *testing.T) string {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	root := filepath.Join(dir, "recorded", "q29b")
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("q29b corpus not found at %s", root)
	}
	return root
}

// ---------------------------------------------------------------------------
// Decoder-model helpers (integer-only: json.Number everywhere).
// ---------------------------------------------------------------------------

func num(v int64) json.Number { return json.Number(strconv.FormatInt(v, 10)) }

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

	candidateRefDoc = map[string]any{
		"schema_version": contract.SchemaCandidateArtifactRef,
		"candidate_id":   "cand-0001",
		"kind":           "step_guidance",
		"body_digest":    "sha256:5ca2d3d975102507ad670945a8b77becae6eec70ce5d9463b0716867365bad34",
		"origin_type":    "skill_proposal",
		"origin_ref": map[string]any{
			"id":      "prop-0001",
			"version": num(1),
			"digest":  "sha256:4dbfa5e4285010517492150d190b8d574bab2034e41481fff19f475fc0fedbc2",
		},
	}

	adapterRef = map[string]any{
		"id":      "rsih.fake-runtime-adapter",
		"version": num(1),
		"digest":  "sha256:b61822af49ed662c6e882237e1e85b27b3ead0f18869a079d9ba5a7697217803",
	}
	profileRef = map[string]any{
		"id":      "rsih.replay-profile",
		"version": num(1),
		"digest":  "sha256:536aa4b6b9489674b74ba2daa169650346f08911a0410a99b0d0e59a4716fb09",
	}
)

func vref(id string, version int64, digest string) map[string]any {
	return map[string]any{"id": id, "version": num(version), "digest": digest}
}

// q29bPacket is one recorded packet loaded from runtime.json.
type q29bPacket struct {
	packetID string
	family   string
	domain   string
	side     string
	terminal map[string]any
	expected map[string]any
	artifact map[string]any
	segRef   map[string]any
	pathDoms []string
	output   []byte
}

func loadQ29bPacket(t *testing.T, family, packetID string) q29bPacket {
	t.Helper()
	base := filepath.Join(q29bDir(t), family, packetID)
	raw, err := os.ReadFile(filepath.Join(base, "runtime.json"))
	if err != nil {
		t.Fatalf("read runtime.json of %s: %v", packetID, err)
	}
	value, err := contract.ParseJSONStrict(raw)
	if err != nil {
		t.Fatalf("parse runtime.json of %s: %v", packetID, err)
	}
	doc, _ := contract.AsObject(value)
	output, err := os.ReadFile(filepath.Join(base, "expected-output.canonical"))
	if err != nil {
		t.Fatalf("read expected-output.canonical of %s: %v", packetID, err)
	}
	sealed, _ := contract.AsObject(doc["sealed_inputs"])
	segments, _ := contract.AsArray(sealed["segment_refs"])
	segObj, _ := contract.AsObject(segments[0])
	paths, _ := contract.AsArray(sealed["path_refs"])
	var domains []string
	for _, raw := range paths {
		if p, ok := contract.AsObject(raw); ok {
			if d, ok := contract.AsString(p["domain"]); ok {
				domains = append(domains, d)
			}
		}
	}
	run, _ := contract.AsObject(doc["run"])
	artifact, _ := contract.AsObject(sealed["artifact_ref"])
	// The family→domain envelope mapping is frozen by the replay request
	// (Contract §10.4): alpha is the policy-selected baseline for the
	// overlap family, beta for the conflict family.
	domainFor := map[string]string{
		"baseline": "source_a", "candidate": "source_a", "overlap": "overlap",
		"conflict": "source_b", "late-output": "source_a",
		"clock-exhausted": "source_a", "extra-tool-call": "source_a",
	}
	fam, _ := contract.AsString(doc["family"])
	return q29bPacket{
		packetID: packetID,
		family:   fam,
		domain:   domainFor[fam],
		side:     mustString(t, run, "side"),
		terminal: mustObject(t, doc, "terminal"),
		expected: mustObject(t, doc, "expected"),
		artifact: artifact,
		segRef:   segObj,
		pathDoms: domains,
		output:   output,
	}
}

func mustObject(t *testing.T, doc map[string]any, key string) map[string]any {
	t.Helper()
	obj, ok := contract.AsObject(doc[key])
	if !ok {
		t.Fatalf("key %q of runtime packet is not an object", key)
	}
	return obj
}

func mustString(t *testing.T, doc map[string]any, key string) string {
	t.Helper()
	s, ok := contract.AsString(doc[key])
	if !ok {
		t.Fatalf("key %q of runtime packet is not a string", key)
	}
	return s
}

func packetExpectedDigest(t *testing.T, p q29bPacket) string {
	t.Helper()
	d, ok := contract.AsString(p.expected["output_digest"])
	if !ok {
		t.Fatalf("packet %s has no expected output digest", p.packetID)
	}
	return d
}

func packetRun(t *testing.T, p q29bPacket, cost int64) RunOutput {
	t.Helper()
	reason := ""
	if rc, ok := contract.AsString(p.terminal["reason_code"]); ok {
		reason = rc
	}
	return RunOutput{
		PacketID:     p.packetID,
		Side:         p.side,
		RunStatus:    mustString(t, p.terminal, "status"),
		ReasonCode:   reason,
		OutputDigest: packetExpectedDigest(t, p),
		Output:       p.output,
		CostUnits:    cost,
		ArtifactRef:  p.artifact,
		AdapterRef:   adapterRef,
		ProfileRef:   profileRef,
	}
}

// buildQ29bInputs assembles the frozen request document, the family plan and
// the observed runs for the whole recorded corpus.
func buildQ29bInputs(t *testing.T, cost int64) (map[string]any, *Plan, []RunOutput) {
	t.Helper()
	families := []struct {
		name     string
		baseline map[string]any
	}{
		{"baseline", refAlphaV3}, {"candidate", refAlphaV3}, {"overlap", refAlphaV3},
		{"conflict", refBetaV2}, {"late-output", refAlphaV3},
		{"clock-exhausted", refAlphaV3}, {"extra-tool-call", refAlphaV3},
	}
	var plan Plan
	var runs []RunOutput
	var segments []any
	for _, fam := range families {
		packet := loadQ29bPacket(t, fam.name, "q29b-"+fam.name+"-0001")
		if segments == nil {
			segments = []any{packet.segRef}
		}
		plan.Families = append(plan.Families, Family{
			Name:     fam.name,
			Domain:   packet.domain,
			Sides:    []string{packet.side},
			Baseline: fam.baseline,
			Packets: []Packet{{
				ID:                   packet.packetID,
				Side:                 packet.side,
				PathDomains:          packet.pathDoms,
				Critical:             true,
				ExpectedOutputDigest: packetExpectedDigest(t, packet),
				ExpectedRunStatus:    mustString(t, packet.terminal, "status"),
				ExpectedReasonCode:   reasonString(packet.terminal["reason_code"]),
				ExpectedArtifactRef:  packet.artifact,
			}},
		})
		runs = append(runs, packetRun(t, packet, cost))
	}
	manifestBytes, err := os.ReadFile(filepath.Join(q29bDir(t), "..", "manifest.json"))
	if err != nil {
		t.Fatalf("read recorded manifest: %v", err)
	}
	requestDoc := map[string]any{
		"schema_version":      "gms.replay-request.v1",
		"replay_request_id":   "replay-q29b-0001",
		"candidate_ref":       candidateRefDoc,
		"baseline_skill_refs": []any{refAlphaV3, refBetaV2},
		"fixture_set_refs":    []any{vref("fixture-q29b", 1, contract.DigestBytes(manifestBytes))},
		"segment_refs":        segments,
		"replay_profile_ref":  profileRef,
		"runtime_adapter_ref": adapterRef,
		"mode":                "causal_evaluation",
		"idempotency_key":     contract.DigestBytes([]byte("q29b-idempotency-0001")),
	}
	return requestDoc, &plan, runs
}

func reasonString(raw any) string {
	if s, ok := contract.AsString(raw); ok {
		return s
	}
	return ""
}

// parseOrFatal runs the strict §7.10 parser.
func parseOrFatal(t *testing.T, svc *Service, doc map[string]any) *Request {
	t.Helper()
	req, err := svc.ParseRequest(doc)
	if err != nil {
		t.Fatalf("parse replay request: %v", err)
	}
	return req
}

// ---------------------------------------------------------------------------
// GMS-203 Red test #2 (plan-pinned): nondeterministic reruns of the same
// packet canonicalize to status=inconclusive with REPLAY_NONDETERMINISTIC,
// never as succeeded.
// ---------------------------------------------------------------------------

func TestReplayNondeterminismIsInconclusive(t *testing.T) {
	svc := newTestService(t)
	requestDoc, plan, runs := buildQ29bInputs(t, 1)

	// Rerun the candidate packet with a different output digest than the
	// first attempt: same packet, two digests.
	rerun := runs[1]
	rerun.OutputDigest = "sha256:" + repeatHex('a', 64)
	rerun.Output = []byte(`{"tampered":true}`)
	runs = append(runs, rerun)

	req := parseOrFatal(t, svc, requestDoc)
	result, err := svc.Canonicalize(req, plan, runs)
	if err != nil {
		t.Fatalf("nondeterministic replay must still canonicalize to an inconclusive result, got error %v (code %s)", err, CodeOf(err))
	}
	if result.Status() != StatusInconclusive {
		t.Fatalf("status = %q, want %q", result.Status(), StatusInconclusive)
	}
	if result.FailureReasonCode() != ReasonReplayNondeterministic {
		t.Fatalf("failure_reason_code = %q, want %q", result.FailureReasonCode(), ReasonReplayNondeterministic)
	}
	// The affected packet must never be claimed as a semantic pass.
	for _, rec := range result.Records() {
		if rec.PacketID == "q29b-candidate-0001" {
			if rec.Category != CategoryNondeterministic {
				t.Fatalf("candidate packet record category = %q, want %q", rec.Category, CategoryNondeterministic)
			}
		}
	}
	found := false
	for _, rec := range result.Records() {
		if rec.PacketID == "q29b-candidate-0001" {
			found = true
		}
	}
	if !found {
		t.Fatalf("records must include the nondeterministic packet")
	}
	// The §7.11 document must still validate (schema + result_digest).
	if err := loadGates(t).ValidateInstance(result.Doc(), SchemaReplayResult); err != nil {
		t.Fatalf("inconclusive result document invalid: %v", err)
	}
}

// ---------------------------------------------------------------------------
// The recorded corpus canonicalizes completely and deterministically.
// ---------------------------------------------------------------------------

func TestReplayCanonicalizesQ29bCorpusDeterministically(t *testing.T) {
	svc := newTestService(t)
	requestDoc, plan, runs := buildQ29bInputs(t, 2)

	req := parseOrFatal(t, svc, requestDoc)
	result, err := svc.Canonicalize(req, plan, runs)
	if err != nil {
		t.Fatalf("canonicalize q29b corpus: %v (code %s)", err, CodeOf(err))
	}
	if result.Status() != StatusSucceeded {
		t.Fatalf("status = %q, want %q (failure reason %q)", result.Status(), StatusSucceeded, result.FailureReasonCode())
	}
	outcomes := result.Outcomes()
	if len(outcomes) != 7 {
		t.Fatalf("fixture_outcomes has %d entries, want 7 (all families)", len(outcomes))
	}
	seen := map[string]bool{}
	for _, o := range outcomes {
		seen[o.FixtureRef.ID] = true
		if o.TotalCases < 1 {
			t.Fatalf("family %s total_cases = %d, want >= 1", o.FixtureRef.ID, o.TotalCases)
		}
	}
	for _, fam := range []string{"baseline", "candidate", "overlap", "conflict", "late-output", "clock-exhausted", "extra-tool-call"} {
		if !seen[fam] {
			t.Fatalf("fixture_outcomes missing family %s", fam)
		}
	}

	// Same packet inputs → same result digest (GMS §5.2 determinism).
	again, err := svc.Canonicalize(req, plan, runs)
	if err != nil {
		t.Fatalf("second canonicalization: %v", err)
	}
	if result.ResultDigest() != again.ResultDigest() {
		t.Fatalf("result digest not stable: %s vs %s", result.ResultDigest(), again.ResultDigest())
	}

	// Input run order must not change canonical bytes.
	shuffled := append([]RunOutput{}, runs...)
	shuffled[0], shuffled[6] = shuffled[6], shuffled[0]
	shuffled[2], shuffled[5] = shuffled[5], shuffled[2]
	shuffledRes, err := svc.Canonicalize(req, plan, shuffled)
	if err != nil {
		t.Fatalf("canonicalization with shuffled runs: %v", err)
	}
	if result.ResultDigest() != shuffledRes.ResultDigest() {
		t.Fatalf("result digest depends on input run order: %s vs %s", result.ResultDigest(), shuffledRes.ResultDigest())
	}

	// The full §7.11 document must validate against the shared schema and
	// recompute its own digest.
	if err := loadGates(t).ValidateInstance(result.Doc(), SchemaReplayResult); err != nil {
		t.Fatalf("result document invalid against authority schema: %v", err)
	}

	// Utility vector is integer-only and matches the corpus semantics:
	// baseline side semantic passes = baseline, overlap, conflict (3 of 6
	// baseline-side packets; late-output is infra, clock-exhausted and
	// extra-tool-call are semantic failures).
	env := result.Envelope()
	if env.TaskSuccessCount != 3 {
		t.Fatalf("envelope task_success_count = %d, want 3", env.TaskSuccessCount)
	}
	if env.InconclusiveCaseCount != 1 {
		t.Fatalf("envelope inconclusive_case_count = %d, want 1 (late-output infra case)", env.InconclusiveCaseCount)
	}
	// Candidate side: the candidate packet succeeded semantically.
	utility := result.Utility()
	if utility.TaskSuccessCount != 1 {
		t.Fatalf("candidate task_success_count = %d, want 1", utility.TaskSuccessCount)
	}
	if utility.ExecutionCostUnits != 2 {
		t.Fatalf("candidate execution_cost_units = %d, want 2", utility.ExecutionCostUnits)
	}
}

// Missing family observations reject fail-closed with MISSING_FAMILY.
func TestReplayMissingFamilyIsRejected(t *testing.T) {
	svc := newTestService(t)
	requestDoc, plan, runs := buildQ29bInputs(t, 1)
	// Drop every run of the overlap family (index 2).
	filtered := append([]RunOutput{}, runs[:2]...)
	filtered = append(filtered, runs[3:]...)

	req := parseOrFatal(t, svc, requestDoc)
	_, err := svc.Canonicalize(req, plan, filtered)
	if code := CodeOf(err); code != ReasonMissingFamily {
		t.Fatalf("code = %q, want %q (err %v)", code, ReasonMissingFamily, err)
	}
}

// Baseline/candidate sides must not be swapped: the declared side and the
// executed artifact ref must agree (skill ref on baseline, candidate ref on
// candidate side).
func TestReplayBaselineSwapIsRejected(t *testing.T) {
	svc := newTestService(t)
	requestDoc, plan, runs := buildQ29bInputs(t, 1)

	// Swap the sides of the baseline and candidate packets (and their
	// declared packet sides with them): the baseline packet now claims the
	// candidate side while executing the skill artifact ref.
	swapped := append([]RunOutput{}, runs...)
	swapped[0].Side, swapped[1].Side = swapped[1].Side, swapped[0].Side
	swappedPlan := deepCopyPlan(plan)
	swappedPlan.Families[0].Packets[0].Side, swappedPlan.Families[1].Packets[0].Side =
		swappedPlan.Families[1].Packets[0].Side, swappedPlan.Families[0].Packets[0].Side

	req := parseOrFatal(t, svc, requestDoc)
	_, err := svc.Canonicalize(req, swappedPlan, swapped)
	if code := CodeOf(err); code != ReasonReplayRequestInvalid {
		t.Fatalf("code = %q, want %q (err %v)", code, ReasonReplayRequestInvalid, err)
	}

	// Relabeling only the run's side (packet plan intact) must reject too.
	relabel := append([]RunOutput{}, runs...)
	relabel[1].Side = "baseline" // candidate ref on the baseline side
	if _, err := svc.Canonicalize(req, plan, relabel); err == nil || CodeOf(err) != ReasonReplayRequestInvalid {
		t.Fatalf("candidate artifact ref under the baseline side must reject with %s, got %v (%s)", ReasonReplayRequestInvalid, err, CodeOf(err))
	}
}

// Digest and correlation mismatches reject fail-closed.
func TestReplayDigestAndCorrelationMismatchesAreRejected(t *testing.T) {
	svc := newTestService(t)
	requestDoc, plan, runs := buildQ29bInputs(t, 1)
	req := parseOrFatal(t, svc, requestDoc)

	// Wrong observed output digest.
	badDigest := append([]RunOutput{}, runs...)
	badDigest[0].OutputDigest = "sha256:" + repeatHex('0', 64)
	if _, err := svc.Canonicalize(req, plan, badDigest); err == nil || CodeOf(err) != contract.ReasonDigestMismatch {
		t.Fatalf("observed digest mismatch must reject with DIGEST_MISMATCH, got %v (%s)", err, CodeOf(err))
	}

	// Canonical bytes that do not hash to the declared digest.
	badBytes := append([]RunOutput{}, runs...)
	badBytes[0].Output = []byte(`{"forged":true}`)
	if _, err := svc.Canonicalize(req, plan, badBytes); err == nil || CodeOf(err) != contract.ReasonDigestMismatch {
		t.Fatalf("output bytes not hashing to the declared digest must reject with DIGEST_MISMATCH, got %v (%s)", err, CodeOf(err))
	}

	// Adapter correlation: RSIH output produced by another adapter version.
	badAdapter := append([]RunOutput{}, runs...)
	forgedAdapter := map[string]any{"id": "rsih.fake-runtime-adapter", "version": num(2), "digest": adapterRef["digest"]}
	badAdapter[3].AdapterRef = forgedAdapter
	if _, err := svc.Canonicalize(req, plan, badAdapter); err == nil || CodeOf(err) != contract.ReasonRefMismatch {
		t.Fatalf("adapter correlation mismatch must reject with REF_MISMATCH, got %v (%s)", err, CodeOf(err))
	}

	// Unknown packet id (Host correlation id maps to no planned packet).
	unknown := append([]RunOutput{}, runs...)
	unknown[4].PacketID = "q29b-ghost-0000"
	if _, err := svc.Canonicalize(req, plan, unknown); err == nil || CodeOf(err) != ReasonReplayRequestInvalid {
		t.Fatalf("unknown packet correlation must reject with REPLAY_REQUEST_INVALID, got %v (%s)", err, CodeOf(err))
	}
}

// The §7.10 request itself is validated against the closed authority schema.
func TestReplayRequestStrictSchemaEnforced(t *testing.T) {
	svc := newTestService(t)
	requestDoc, _, _ := buildQ29bInputs(t, 1)

	bad := deepCopyDoc(requestDoc)
	bad["mode"] = "live_production" // Contract §10.5: v1 causal replay only
	if _, err := svc.ParseRequest(bad); err == nil {
		t.Fatalf("non causal_evaluation mode must reject")
	}

	bad = deepCopyDoc(requestDoc)
	delete(bad, "idempotency_key")
	if _, err := svc.ParseRequest(bad); err == nil {
		t.Fatalf("missing idempotency key must reject")
	}

	// Merge candidates must freeze required source heads (conditional).
	merge := deepCopyDoc(requestDoc)
	mergeRef := deepCopyDoc(candidateRefDoc)
	mergeRef["origin_type"] = "merge_proposal"
	merge["candidate_ref"] = mergeRef
	if _, err := svc.ParseRequest(merge); err == nil {
		t.Fatalf("merge origin without required_source_heads must reject")
	}
	merge["required_source_heads"] = []any{refAlphaV3, refBetaV2}
	if _, err := svc.ParseRequest(merge); err != nil {
		t.Fatalf("merge origin with frozen source heads must parse: %v", err)
	}
}

// Cost aggregation overflow fails closed as an inconclusive result.
func TestReplayCostAggregationOverflowIsInconclusive(t *testing.T) {
	svc := newTestService(t)
	requestDoc, plan, runs := buildQ29bInputs(t, 1)
	overflowing := append([]RunOutput{}, runs...)
	overflowing[0].CostUnits = int64(1) << 62
	overflowing[1].CostUnits = int64(1) << 62
	overflowing[2].CostUnits = int64(1) << 62
	req := parseOrFatal(t, svc, requestDoc)
	result, err := svc.Canonicalize(req, plan, overflowing)
	if err != nil {
		t.Fatalf("overflow must canonicalize to inconclusive, got error %v", err)
	}
	if result.Status() != StatusInconclusive || result.FailureReasonCode() != ReasonUtilityArithmeticOverflow {
		t.Fatalf("status/reason = %s/%s, want inconclusive/%s", result.Status(), result.FailureReasonCode(), ReasonUtilityArithmeticOverflow)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func repeatHex(c byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

func deepCopyDoc(doc map[string]any) map[string]any {
	data, err := contract.JCS(doc)
	if err != nil {
		panic(fmt.Sprintf("test copy: %v", err))
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		panic(fmt.Sprintf("test copy: %v", err))
	}
	out, _ := contract.AsObject(value)
	return out
}

func deepCopyPlan(plan *Plan) *Plan {
	out := &Plan{Families: make([]Family, len(plan.Families))}
	for i, f := range plan.Families {
		out.Families[i] = Family{
			Name:     f.Name,
			Domain:   f.Domain,
			Sides:    append([]string(nil), f.Sides...),
			Baseline: deepCopyDoc(f.Baseline),
			Packets:  append([]Packet(nil), f.Packets...),
		}
	}
	return out
}
