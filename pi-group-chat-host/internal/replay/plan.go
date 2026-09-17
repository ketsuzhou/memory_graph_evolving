package replay

// HST-203 frozen replay plan (Host spec 6.3, S4).
//
// The plan is the module-private freeze of a Contract 7.10 ReplayRequest:
// exact request digest, run identities, fixture execution order, sealed
// event/path inputs, adapter/profile versions, environment allowlist,
// permissions, retry policy and the output capture/digest policy. Once
// frozen it never changes across retries; any material input change is a
// new ReplayRequest, not a retry.
//
// The Host validates schedulability here but never scores: no U1 utility,
// no ReplayResult minting, no release decision (those are GMS authority;
// Host 6.1/6.7).

import (
	"context"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// FixedFamilyOrder is the frozen scheduling order of fixture families,
// aligned with the recorded q29b manifest family set: the four paired
// families first (baseline/candidate/overlap/conflict — the merge A/B/
// overlap/conflict coverage), then the terminal-semantics families, with
// the reserved merge family last (Host 6.5; Contract 9.4.6/10.4).
var FixedFamilyOrder = []string{
	"baseline",
	"candidate",
	"overlap",
	"conflict",
	"late-output",
	"clock-exhausted",
	"extra-tool-call",
	"merge",
}

// PairedFamilies are the families every merge replay must dispatch.
var PairedFamilies = []string{"baseline", "candidate", "overlap", "conflict"}

// Run sides (paired execution, Host 6.4).
const (
	SideBaseline  = "baseline"
	SideCandidate = "candidate"
)

const (
	ModeCausalEvaluation = "causal_evaluation"

	schemaReplayRequest = "gms.replay-request.v1"
	schemaSegmentRef    = "host.segment-ref.v1"
	schemaSkillRef      = "gms.skill-artifact-ref.v2"
	schemaCandidateRef  = "gms.candidate-artifact-ref.v1"
	schemaReplayPlan    = "host.replay-plan.v1"

	neutralLocale   = "en_US_POSIX"
	neutralTimezone = "UTC"
)

// Fixed fake capability kinds (FND-002 Q29-B protocol); anything else is a
// real capability declaration and fails closed.
var fakeCapabilityKinds = map[string]string{
	"clock":      "fake-sequence",
	"random":     "fake-sequence",
	"tool":       "fake-responses",
	"provider":   "fake-responses",
	"filesystem": "fake-tree",
}

var digestRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

var artifactKinds = map[string]bool{
	"human_procedure": true, "step_guidance": true, "composite": true, "tool": true,
}

var originTypes = map[string]bool{
	"skill_proposal": true, "merge_proposal": true,
}

// ---------------------------------------------------------------------------
// Request view (Contract 7.10 ReplayRequest)
// ---------------------------------------------------------------------------

// Ref is the Contract 7.2 VersionedRef exact triple.
type Ref struct {
	ID      string
	Version int64
	Digest  string
}

// SkillRef is the Contract 7.3 exact released-skill reference.
type SkillRef struct {
	LineageID      string
	Version        int64
	Kind           string
	ArtifactDigest string
}

// CandidateRef is the Contract 7.4 exact candidate reference.
type CandidateRef struct {
	CandidateID string
	Kind        string
	BodyDigest  string
	OriginType  string
	OriginRef   Ref
}

// SegmentRefInput is the Contract 7.5 SegmentRef a ReplayRequest pins.
type SegmentRefInput struct {
	RoomID          string
	SegmentID       string
	SegmentVersion  int64
	SegmentDigest   string
	EvidenceSealRef Ref
	PathSealRef     Ref
}

// Request is the Host-side view of a Contract 7.10 ReplayRequest. The
// Host-owned scheduling extras (permissions requirement, correlation id)
// travel in the extensions slot of the canonical digest view.
type Request struct {
	ReplayRequestID     string
	CorrelationID       string
	Mode                string
	IdempotencyKey      string
	CandidateRef        CandidateRef
	BaselineSkillRefs   []SkillRef
	FixtureSetRefs      []Ref
	SegmentRefs         []SegmentRefInput
	ReplayProfileRef    Ref
	RuntimeAdapterRef   Ref
	RequiredSourceHeads []SkillRef
	Permissions         map[string]int64
}

// IsMerge reports whether this is a bilateral merge replay (source-head
// expectations present; Contract 7.10 required_source_heads).
func (r *Request) IsMerge() bool { return len(r.RequiredSourceHeads) > 0 }

// ---------------------------------------------------------------------------
// Frozen plan structures
// ---------------------------------------------------------------------------

// Seeds is the frozen deterministic seed snapshot declared by the fake
// runtime capabilities of a family's fixtures (clock ticks / random draws).
type Seeds struct {
	ClockTicks  []int64
	RandomDraws []int64
	Digest      string
}

// FixturePlan freezes one fixture packet inside the plan.
type FixturePlan struct {
	PacketRef           Ref
	PacketSide          string
	PacketRunID         string
	SegmentIDs          []string
	RequiredPathDomains []string
	SealedInputDigest   string
	EnvironmentDigest   string
}

// VariantPlan freezes one dispatch side of a family.
type VariantPlan struct {
	Side           string
	RunID          string
	ArtifactID     string
	ArtifactKind   string
	ArtifactDigest string
}

// FamilyPlan freezes one fixture family: ordered fixtures, dispatch
// variants and the merged deterministic seeds.
type FamilyPlan struct {
	Family   string
	Reserved bool
	Fixtures []FixturePlan
	Variants []VariantPlan
	Seeds    Seeds
}

// RetryPolicy is the bounded infra-retry budget frozen in the plan.
type RetryPolicy struct {
	MaxAttempts int
}

// Plan is the frozen replay plan (Host 6.3).
type Plan struct {
	ReplayRequestDigest string
	ReplayRequestID     string
	IdempotencyKey      string
	CorrelationID       string
	Mode                string
	FamilyOrder         []string
	Families            []FamilyPlan
	Candidate           CandidateRef
	Baseline            SkillRef
	Profile             Ref
	Adapter             Ref
	Permissions         map[string]int64
	PermissionCaps      map[string]int64
	SourceHeads         []SkillRef
	CapturePolicyDigest string
	RetryPolicy         RetryPolicy
	PlanDigest          string
}

// Error is a fail-closed scheduling rejection carrying a closed-registry
// reason code (Contract 13.7.1).
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.ReasonCode
	}
	return e.ReasonCode + ": " + e.Detail
}

// ReasonOf extracts the closed reason code of a replay error.
func ReasonOf(err error) string {
	if err == nil {
		return ""
	}
	if re, ok := err.(*Error); ok {
		return re.ReasonCode
	}
	return ""
}

// ---------------------------------------------------------------------------
// Canonical views (digest preimages)
// ---------------------------------------------------------------------------

func num(n int64) contract.Number { return contract.Number(strconv.FormatInt(n, 10)) }

func refObject(r Ref) *contract.Object {
	o := contract.NewObject()
	o.Set("id", contract.String(r.ID))
	o.Set("version", num(r.Version))
	o.Set("digest", contract.String(r.Digest))
	return o
}

func skillRefObject(r SkillRef) *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(schemaSkillRef))
	o.Set("lineage_id", contract.String(r.LineageID))
	o.Set("version", num(r.Version))
	o.Set("kind", contract.String(r.Kind))
	o.Set("artifact_digest", contract.String(r.ArtifactDigest))
	return o
}

func candidateRefObject(r CandidateRef) *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(schemaCandidateRef))
	o.Set("candidate_id", contract.String(r.CandidateID))
	o.Set("kind", contract.String(r.Kind))
	o.Set("body_digest", contract.String(r.BodyDigest))
	o.Set("origin_type", contract.String(r.OriginType))
	o.Set("origin_ref", refObject(r.OriginRef))
	return o
}

func segmentRefObject(r SegmentRefInput) *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(schemaSegmentRef))
	o.Set("room_id", contract.String(r.RoomID))
	o.Set("segment_id", contract.String(r.SegmentID))
	o.Set("segment_version", num(r.SegmentVersion))
	o.Set("segment_digest", contract.String(r.SegmentDigest))
	o.Set("evidence_seal_ref", refObject(r.EvidenceSealRef))
	o.Set("path_seal_ref", refObject(r.PathSealRef))
	return o
}

func stringIntMapObject(m map[string]int64) *contract.Object {
	o := contract.NewObject()
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		o.Set(k, num(m[k]))
	}
	return o
}

// requestObject is the canonical ReplayRequest view the plan freezes.
func requestObject(r *Request) *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(schemaReplayRequest))
	o.Set("replay_request_id", contract.String(r.ReplayRequestID))
	o.Set("candidate_ref", candidateRefObject(r.CandidateRef))
	baselines := contract.Array{}
	for _, b := range r.BaselineSkillRefs {
		baselines = append(baselines, skillRefObject(b))
	}
	o.Set("baseline_skill_refs", baselines)
	fixtures := contract.Array{}
	for _, f := range r.FixtureSetRefs {
		fixtures = append(fixtures, refObject(f))
	}
	o.Set("fixture_set_refs", fixtures)
	segs := contract.Array{}
	for _, s := range r.SegmentRefs {
		segs = append(segs, segmentRefObject(s))
	}
	o.Set("segment_refs", segs)
	o.Set("replay_profile_ref", refObject(r.ReplayProfileRef))
	o.Set("runtime_adapter_ref", refObject(r.RuntimeAdapterRef))
	o.Set("mode", contract.String(r.Mode))
	o.Set("idempotency_key", contract.String(r.IdempotencyKey))
	heads := contract.Array{}
	for _, h := range r.RequiredSourceHeads {
		heads = append(heads, skillRefObject(h))
	}
	o.Set("required_source_heads", heads)
	ext := contract.NewObject()
	ext.Set("permissions", stringIntMapObject(r.Permissions))
	ext.Set("correlation_id", contract.String(r.CorrelationID))
	o.Set("extensions", ext)
	return o
}

// capturePolicyObject is the frozen output capture/digest policy.
func capturePolicyObject() *contract.Object {
	o := contract.NewObject()
	o.Set("algorithm", contract.String("sha256"))
	o.Set("canonicalization", contract.String("rfc8785-jcs"))
	captures := contract.Array{}
	for _, c := range []string{
		"attempt_correlation", "frozen_inputs", "outputs", "reason_codes",
		"capability_usage", "late_audit",
	} {
		captures = append(captures, contract.String(c))
	}
	o.Set("captures", captures)
	return o
}

func seedsObject(s Seeds) *contract.Object {
	o := contract.NewObject()
	ticks := contract.Array{}
	for _, t := range s.ClockTicks {
		ticks = append(ticks, num(t))
	}
	draws := contract.Array{}
	for _, d := range s.RandomDraws {
		draws = append(draws, num(d))
	}
	o.Set("clock_ticks", ticks)
	o.Set("random_draws", draws)
	return o
}

// planObject is the plan digest preimage (everything except plan_digest).
func planObject(p *Plan) *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(schemaReplayPlan))
	o.Set("replay_request_digest", contract.String(p.ReplayRequestDigest))
	o.Set("replay_request_id", contract.String(p.ReplayRequestID))
	o.Set("idempotency_key", contract.String(p.IdempotencyKey))
	o.Set("correlation_id", contract.String(p.CorrelationID))
	o.Set("mode", contract.String(p.Mode))
	order := contract.Array{}
	for _, f := range p.FamilyOrder {
		order = append(order, contract.String(f))
	}
	o.Set("family_order", order)
	fams := contract.Array{}
	for _, f := range p.Families {
		fo := contract.NewObject()
		fo.Set("family", contract.String(f.Family))
		fo.Set("reserved", contract.Bool(f.Reserved))
		fixts := contract.Array{}
		for _, fp := range f.Fixtures {
			fpo := contract.NewObject()
			fpo.Set("packet_ref", refObject(fp.PacketRef))
			fpo.Set("packet_side", contract.String(fp.PacketSide))
			fpo.Set("packet_run_id", contract.String(fp.PacketRunID))
			segIDs := contract.Array{}
			for _, id := range fp.SegmentIDs {
				segIDs = append(segIDs, contract.String(id))
			}
			fpo.Set("segment_ids", segIDs)
			domains := contract.Array{}
			for _, d := range fp.RequiredPathDomains {
				domains = append(domains, contract.String(d))
			}
			fpo.Set("required_path_domains", domains)
			fpo.Set("sealed_input_digest", contract.String(fp.SealedInputDigest))
			fpo.Set("environment_digest", contract.String(fp.EnvironmentDigest))
			fixts = append(fixts, fpo)
		}
		fo.Set("fixtures", fixts)
		variants := contract.Array{}
		for _, v := range f.Variants {
			vo := contract.NewObject()
			vo.Set("side", contract.String(v.Side))
			vo.Set("run_id", contract.String(v.RunID))
			vo.Set("artifact_id", contract.String(v.ArtifactID))
			vo.Set("artifact_kind", contract.String(v.ArtifactKind))
			vo.Set("artifact_digest", contract.String(v.ArtifactDigest))
			variants = append(variants, vo)
		}
		fo.Set("variants", variants)
		fo.Set("seeds", seedsObject(f.Seeds))
		fams = append(fams, fo)
	}
	o.Set("families", fams)
	o.Set("candidate_ref", candidateRefObject(p.Candidate))
	o.Set("baseline_skill_ref", skillRefObject(p.Baseline))
	o.Set("profile", refObject(p.Profile))
	o.Set("adapter", refObject(p.Adapter))
	o.Set("permissions", stringIntMapObject(p.Permissions))
	o.Set("permission_caps", stringIntMapObject(p.PermissionCaps))
	heads := contract.Array{}
	for _, h := range p.SourceHeads {
		heads = append(heads, skillRefObject(h))
	}
	o.Set("source_heads", heads)
	o.Set("capture_policy", capturePolicyObject())
	retry := contract.NewObject()
	retry.Set("max_attempts", num(int64(p.RetryPolicy.MaxAttempts)))
	o.Set("retry_policy", retry)
	return o
}

// ---------------------------------------------------------------------------
// Validation helpers (pure, fail-closed)
// ---------------------------------------------------------------------------

func reject(reason, detail string) error {
	return &Error{ReasonCode: reason, Detail: detail}
}

func validDigest(d string) bool { return digestRe.MatchString(d) }

func validVersionedRef(r Ref) bool {
	return r.ID != "" && r.Version >= 1 && validDigest(r.Digest)
}

// inexactRefID reports ids the Contract forbids as exact refs: bare
// names/aliases such as latest/head or Graph node identities.
func inexactRefID(id string) bool {
	switch id {
	case "", "latest", "head", "active", "graph", "node":
		return true
	}
	return strings.HasPrefix(id, "graph:") || strings.HasPrefix(id, "node:")
}

func validSkillRef(r SkillRef) bool {
	if inexactRefID(r.LineageID) || r.Version < 1 || !validDigest(r.ArtifactDigest) {
		return false
	}
	return artifactKinds[r.Kind]
}

func validCandidateRef(r CandidateRef) bool {
	if inexactRefID(r.CandidateID) || !validDigest(r.BodyDigest) || !artifactKinds[r.Kind] {
		return false
	}
	if !originTypes[r.OriginType] || !validVersionedRef(r.OriginRef) {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Plan construction
// ---------------------------------------------------------------------------

// Deps carries the plan-time ports: settled-seal resolution, fixture
// corpus access and runtime profile availability.
type Deps struct {
	Seals    SealSource
	Fixtures FixtureSource
	Profiles ProfileSource
}

// ---------------------------------------------------------------------------
// Value readers (plan-time validation over the canonical core)
// ---------------------------------------------------------------------------

func vstr(o *contract.Object, key string) string {
	s, _ := contract.StringOf(o, key)
	return s
}

func vobj(o *contract.Object, key string) *contract.Object {
	v, ok := o.Get(key)
	if !ok {
		return nil
	}
	if ov, ok := v.(*contract.Object); ok {
		return ov
	}
	return nil
}

// ---------------------------------------------------------------------------
// Request shape validation (Host 6.2 items 1-2, 6, 8-10)
// ---------------------------------------------------------------------------

func validateRequestShape(req *Request) error {
	if req.ReplayRequestID == "" || req.CorrelationID == "" {
		return reject("REPLAY_REQUEST_INVALID", "replay request id and correlation id are required")
	}
	if req.Mode != ModeCausalEvaluation {
		return reject("REPLAY_REQUEST_INVALID", "mode must be causal_evaluation in v1")
	}
	if !validDigest(req.IdempotencyKey) {
		return reject("REPLAY_REQUEST_INVALID", "idempotency key must be a sha256 digest")
	}
	if !validCandidateRef(req.CandidateRef) {
		return reject("NON_EXACT_REF", "candidate ref is not exact")
	}
	if len(req.BaselineSkillRefs) == 0 {
		return reject("REPLAY_REQUEST_INVALID", "baseline_skill_refs minItems=1")
	}
	for i, b := range req.BaselineSkillRefs {
		if !validSkillRef(b) {
			return reject("NON_EXACT_REF", "baseline ref "+strconv.Itoa(i)+" is not exact (no latest/Graph/bare names)")
		}
	}
	if len(req.FixtureSetRefs) == 0 {
		return reject("REPLAY_REQUEST_INVALID", "fixture_set_refs minItems=1")
	}
	for i, f := range req.FixtureSetRefs {
		if !validVersionedRef(f) {
			return reject("NON_EXACT_REF", "fixture ref "+strconv.Itoa(i)+" is not exact")
		}
	}
	if len(req.SegmentRefs) == 0 {
		return reject("REPLAY_REQUEST_INVALID", "segment_refs minItems=1")
	}
	for i, s := range req.SegmentRefs {
		if s.RoomID == "" || s.SegmentID == "" || s.SegmentVersion < 1 ||
			!validDigest(s.SegmentDigest) ||
			!validVersionedRef(s.EvidenceSealRef) || !validVersionedRef(s.PathSealRef) {
			return reject("NON_EXACT_REF", "segment ref "+strconv.Itoa(i)+" is not exact")
		}
	}
	if !validVersionedRef(req.ReplayProfileRef) {
		return reject("NON_EXACT_REF", "replay profile ref is not exact")
	}
	if !validVersionedRef(req.RuntimeAdapterRef) {
		return reject("NON_EXACT_REF", "runtime adapter ref is not exact")
	}
	if n := len(req.RequiredSourceHeads); n != 0 && n != 2 {
		return reject("REPLAY_REQUEST_INVALID", "merge requests must carry exactly two required source heads")
	}
	for i, h := range req.RequiredSourceHeads {
		if !validSkillRef(h) {
			return reject("NON_EXACT_REF", "source head "+strconv.Itoa(i)+" is not exact")
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Sealed-input validation (Host 6.2 items 3-5)
// ---------------------------------------------------------------------------

func validateSealedSegments(ctx context.Context, req *Request, deps Deps) (map[string]SealedSegment, error) {
	seals := make(map[string]SealedSegment, len(req.SegmentRefs))
	for _, sr := range req.SegmentRefs {
		if _, done := seals[sr.SegmentID]; done {
			continue
		}
		ss, err := deps.Seals.SealedSegment(ctx, sr.SegmentID)
		if err != nil {
			return nil, reject("SEGMENT_NOT_SETTLED", "segment "+sr.SegmentID+": "+err.Error())
		}
		if ss.TerminalState != "settled" {
			return nil, reject("SEGMENT_NOT_SETTLED",
				"segment "+sr.SegmentID+" is "+ss.TerminalState+", not settled")
		}
		rec := ss.SegmentRef
		if rec.RoomID != sr.RoomID || rec.SegmentID != sr.SegmentID || rec.SegmentVersion != sr.SegmentVersion {
			return nil, reject("REF_MISMATCH", "segment "+sr.SegmentID+" ref disagrees with the seal record")
		}
		if rec.SegmentDigest != sr.SegmentDigest {
			return nil, reject("SEAL_DIGEST_MISMATCH", "segment "+sr.SegmentID+" digest disagrees with the seal record")
		}
		if rec.EvidenceSealRef != sr.EvidenceSealRef || rec.PathSealRef != sr.PathSealRef {
			return nil, reject("SEAL_DIGEST_MISMATCH", "segment "+sr.SegmentID+" seal refs disagree with the seal record")
		}
		seals[sr.SegmentID] = ss
	}
	return seals, nil
}

// validatePacketEnvironment enforces the frozen environment allowlist and
// fake-capability declarations (FND-002; RSIH 6.4).
func validatePacketEnvironment(pkt FixturePacket) error {
	env := vobj(pkt.Body, "environment")
	if env == nil {
		return reject("ENVIRONMENT_NOT_NEUTRAL", "packet "+pkt.Ref.ID+" declares no environment")
	}
	if vstr(env, "network") != "disabled" {
		return reject("NETWORK_ACCESS_DECLARED", "packet "+pkt.Ref.ID)
	}
	if vstr(env, "cache") != "per-run-isolated" {
		return reject("CROSS_RUN_CACHE_DECLARED", "packet "+pkt.Ref.ID)
	}
	if vstr(env, "locale") != neutralLocale || vstr(env, "timezone") != neutralTimezone {
		return reject("ENVIRONMENT_NOT_NEUTRAL", "packet "+pkt.Ref.ID)
	}
	for name, want := range fakeCapabilityKinds {
		kind, ok := pkt.CapabilityKinds[name]
		if !ok || kind != want {
			return reject("REAL_CAPABILITY_DECLARED", name+" in packet "+pkt.Ref.ID)
		}
	}
	return nil
}

// validatePacketCoverage pins fixture-to-seal consistency: packet segments
// must sit inside the request, path refs must resolve through the sealed
// Conversation Path Seal and every required success/failure/recovery
// domain must be covered (Host 6.5).
func validatePacketCoverage(pkt FixturePacket, req *Request, seals map[string]SealedSegment) error {
	reqSegs := make(map[string]SegmentRefInput, len(req.SegmentRefs))
	for _, s := range req.SegmentRefs {
		reqSegs[s.SegmentID] = s
	}
	for _, ps := range pkt.Segments {
		rs, ok := reqSegs[ps.SegmentID]
		if !ok {
			return reject("MISSING_SEALED_PATH",
				"packet "+pkt.Ref.ID+" references segment "+ps.SegmentID+" outside the request")
		}
		if rs.SegmentDigest != ps.SegmentDigest {
			return reject("SEAL_DIGEST_MISMATCH",
				"packet "+pkt.Ref.ID+" segment "+ps.SegmentID+" digest disagrees with the request")
		}
		if rs != ps {
			return reject("REF_MISMATCH",
				"packet "+pkt.Ref.ID+" segment "+ps.SegmentID+" ref disagrees with the request")
		}
	}
	resolved := map[string]bool{}
	for _, pr := range pkt.PathRefs {
		if _, ok := reqSegs[pr.SegmentID]; !ok {
			return reject("MISSING_SEALED_PATH",
				"path "+pr.PathID+" sits on segment "+pr.SegmentID+" outside the sealed refs")
		}
		ss, ok := seals[pr.SegmentID]
		if !ok {
			return reject("MISSING_SEALED_PATH", "path "+pr.PathID+" segment "+pr.SegmentID+" unsealed")
		}
		domain, ok := ss.PathDomains[pr.PathID]
		if !ok {
			return reject("MISSING_SEALED_PATH", "path "+pr.PathID+" not sealed")
		}
		if domain != pr.Domain {
			return reject("MISSING_SEALED_PATH", "path "+pr.PathID+" domain mismatch")
		}
		if pr.PathSealRef != ss.PathSealRef {
			return reject("PATH_SEAL_DIGEST_MISMATCH", "path seal ref "+pr.PathID)
		}
		resolved[domain] = true
	}
	for _, d := range pkt.RequiredPathDomains {
		if !resolved[d] {
			return reject("MISSING_SEALED_PATH",
				"required path domain "+d+" unresolved in packet "+pkt.Ref.ID)
		}
	}
	return nil
}

// runIDFor derives the frozen run identity of one variant: the packet's
// recorded identity for its native side, a deterministic derivation for
// the cross side.
func runIDFor(pkts []FixturePacket, side string) string {
	if len(pkts) == 0 {
		return ""
	}
	first := pkts[0]
	if first.Side == side {
		return first.RunID
	}
	return first.RunID + "#" + side
}

// BuildPlan validates schedulability (Host 6.2) and freezes the plan
// (Host 6.3). It is the only place the scheduler touches request inputs;
// after this the plan digest governs.
func BuildPlan(ctx context.Context, req *Request, deps Deps, retry RetryPolicy) (*Plan, error) {
	if err := validateRequestShape(req); err != nil {
		return nil, err
	}
	seals, err := validateSealedSegments(ctx, req, deps)
	if err != nil {
		return nil, err
	}

	// Profile / adapter availability (Host 6.2 item 7).
	profile, err := deps.Profiles.Profile(ctx, req.ReplayProfileRef)
	if err != nil || !profile.Available {
		return nil, reject("SCOPE_PROFILE_INVALID", "replay profile "+req.ReplayProfileRef.ID+" unavailable")
	}
	if !profile.AdapterAvailable || profile.Adapter != req.RuntimeAdapterRef {
		return nil, reject("SCOPE_PROFILE_INVALID", "runtime adapter "+req.RuntimeAdapterRef.ID+" unavailable for profile")
	}

	// Permission requirements must not exceed the Host cap (Host 6.2
	// item 9); anything the profile does not grant is denied.
	for name, amount := range req.Permissions {
		cap, granted := profile.PermissionCaps[name]
		if amount < 0 || !granted || amount > cap {
			return nil, reject("PERMISSION_CAP_EXCEEDED",
				"permission "+name+"="+strconv.FormatInt(amount, 10)+" exceeds the Host cap")
		}
	}

	// Fixture corpus: declared families plus the exact packets the
	// request pins (Host 6.2 item 5).
	manifest, err := deps.Fixtures.Manifest(ctx)
	if err != nil {
		return nil, reject("REPLAY_REQUEST_INVALID", "fixture manifest: "+err.Error())
	}
	declared := make(map[string]FamilyDeclaration, len(manifest.Families))
	for _, decl := range manifest.Families {
		declared[decl.Name] = decl
	}
	var packets []FixturePacket
	byFamily := map[string][]FixturePacket{}
	for _, ref := range req.FixtureSetRefs {
		pkt, err := deps.Fixtures.Packet(ctx, ref)
		if err != nil {
			return nil, reject("REF_MISMATCH", "fixture "+ref.ID+": "+err.Error())
		}
		if pkt.Ref.Digest != ref.Digest {
			return nil, reject("REF_MISMATCH", "fixture "+ref.ID+" digest mismatch")
		}
		decl, ok := declared[pkt.Family]
		if !ok || decl.Reserved {
			return nil, reject("MISSING_FAMILY", "family "+pkt.Family+" is not declared by the fixture set")
		}
		if err := validatePacketEnvironment(pkt); err != nil {
			return nil, err
		}
		if err := validatePacketCoverage(pkt, req, seals); err != nil {
			return nil, err
		}
		packets = append(packets, pkt)
		byFamily[pkt.Family] = append(byFamily[pkt.Family], pkt)
	}

	// Family completeness (Host 6.2 item 5; 6.5): baseline/candidate
	// always; merge adds A/B (=baseline/candidate families), overlap,
	// conflict and the reserved merge declaration (empty placeholder
	// allowed, missing declaration not).
	for _, required := range PairedFamilies[:2] {
		if len(byFamily[required]) == 0 {
			return nil, reject("MISSING_FAMILY", "fixture set does not cover the "+required+" family")
		}
	}
	if req.IsMerge() {
		for _, required := range []string{"overlap", "conflict"} {
			if len(byFamily[required]) == 0 {
				return nil, reject("MISSING_FAMILY", "merge fixture set does not cover the "+required+" family")
			}
		}
		if _, ok := declared["merge"]; !ok {
			return nil, reject("MISSING_FAMILY", "merge family is not declared by the fixture manifest")
		}
	}

	// Freeze families in the fixed order.
	baseline := req.BaselineSkillRefs[0]
	var families []FamilyPlan
	var order []string
	known := map[string]bool{}
	for _, name := range FixedFamilyOrder {
		known[name] = true
		pkts, covered := byFamily[name]
		switch {
		case covered:
			fam := FamilyPlan{Family: name}
			var seeds Seeds
			for _, p := range pkts {
				ids := make([]string, 0, len(p.Segments))
				for _, s := range p.Segments {
					ids = append(ids, s.SegmentID)
				}
				fam.Fixtures = append(fam.Fixtures, FixturePlan{
					PacketRef:           p.Ref,
					PacketSide:          p.Side,
					PacketRunID:         p.RunID,
					SegmentIDs:          ids,
					RequiredPathDomains: append([]string(nil), p.RequiredPathDomains...),
					SealedInputDigest:   p.SealedInputDigest,
					EnvironmentDigest:   p.EnvironmentDigest,
				})
				seeds.ClockTicks = append(seeds.ClockTicks, p.Seeds.ClockTicks...)
				seeds.RandomDraws = append(seeds.RandomDraws, p.Seeds.RandomDraws...)
			}
			seeds.Digest = digestOfValue(seedsObject(seeds))
			fam.Seeds = seeds
			fam.Variants = []VariantPlan{
				{
					Side:           SideBaseline,
					RunID:          runIDFor(pkts, SideBaseline),
					ArtifactID:     baseline.LineageID,
					ArtifactKind:   baseline.Kind,
					ArtifactDigest: baseline.ArtifactDigest,
				},
				{
					Side:           SideCandidate,
					RunID:          runIDFor(pkts, SideCandidate),
					ArtifactID:     req.CandidateRef.CandidateID,
					ArtifactKind:   req.CandidateRef.Kind,
					ArtifactDigest: req.CandidateRef.BodyDigest,
				},
			}
			families = append(families, fam)
			order = append(order, name)
		case req.IsMerge() && name == "merge":
			// Declared reserved placeholder: frozen, empty, never dispatched.
			families = append(families, FamilyPlan{Family: name, Reserved: true})
			order = append(order, name)
		}
	}
	for name := range byFamily {
		if !known[name] {
			return nil, reject("MISSING_FAMILY", "family "+name+" is outside the fixed family order")
		}
	}

	if retry.MaxAttempts < 1 {
		retry.MaxAttempts = 2
	}
	plan := &Plan{
		ReplayRequestDigest: digestOfValue(requestObject(req)),
		ReplayRequestID:     req.ReplayRequestID,
		IdempotencyKey:      req.IdempotencyKey,
		CorrelationID:       req.CorrelationID,
		Mode:                req.Mode,
		FamilyOrder:         order,
		Families:            families,
		Candidate:           req.CandidateRef,
		Baseline:            baseline,
		Profile:             req.ReplayProfileRef,
		Adapter:             req.RuntimeAdapterRef,
		Permissions:         copyIntMap(req.Permissions),
		PermissionCaps:      copyIntMap(profile.PermissionCaps),
		SourceHeads:         append([]SkillRef(nil), req.RequiredSourceHeads...),
		CapturePolicyDigest: digestOfValue(capturePolicyObject()),
		RetryPolicy:         retry,
	}
	plan.PlanDigest = digestOfValue(planObject(plan))
	return plan, nil
}

func copyIntMap(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func digestOfValue(v contract.Value) string {
	d, err := contract.DigestOf(v)
	if err != nil {
		return ""
	}
	return d
}
