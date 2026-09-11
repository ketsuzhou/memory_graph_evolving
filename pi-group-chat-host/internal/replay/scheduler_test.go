package replay_test

// HST-203 Red/Green tests: the replay scheduler freezes a paired plan
// over every recorded fixture family, dispatches baseline/candidate runs
// through the fake Q29-B runner protocol, keeps late outputs audit-only,
// never retries a semantic failure into a pass, and fails closed on
// unsettled segments, seal mismatches, inexact refs, missing families,
// permission/profile violations, nondeterminism and late-output
// rewrites (Host 6, S4; RSIH 6; FND-002 recorded corpus).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/replay"
)

// ---------------------------------------------------------------------------
// Recorded corpus adapter (FixtureSource + SealSource over $FIX/recorded)
// ---------------------------------------------------------------------------

type recordedCorpus struct {
	t              *testing.T
	root           string
	sealDirs       map[string]string // segment_id -> case dir
	familyDirs     map[string]string // q29b family -> packet dir
	reserved       map[string]bool
	manifestOrder  []string
	packets        map[string]replay.FixturePacket // packet_id
	packetByFamily map[string]replay.FixturePacket // family
}

func loadRecordedCorpus(t *testing.T) *recordedCorpus {
	t.Helper()
	confDir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("conformance dir: %v", err)
	}
	root := filepath.Join(confDir, "recorded")
	data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	v, err := contract.ParseJSON(data)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	man, ok := v.(*contract.Object)
	if !ok {
		t.Fatal("manifest is not an object")
	}
	c := &recordedCorpus{
		t:              t,
		root:           root,
		sealDirs:       map[string]string{},
		familyDirs:     map[string]string{},
		reserved:       map[string]bool{},
		packets:        map[string]replay.FixturePacket{},
		packetByFamily: map[string]replay.FixturePacket{},
	}
	for _, f := range arrOf(man, "q29b_families") {
		c.manifestOrder = append(c.manifestOrder, strOf(f))
	}
	for _, item := range arrOf(man, "cases") {
		cs := objOf(item)
		path := filepath.Join(root, filepath.FromSlash(strOf(Get2(cs, "path"))))
		switch strOf(Get2(cs, "corpus")) {
		case "segments":
			c.sealDirs[strOf(Get2(cs, "case_id"))] = path
		case "q29b":
			family := strOf(Get2(cs, "family"))
			if strOf(Get2(cs, "expected_outcome")) == "reserved" {
				c.reserved[family] = true
			} else {
				c.familyDirs[family] = path
			}
		}
	}
	for fam, dir := range c.familyDirs {
		pkt := c.loadPacket(dir)
		c.packets[pkt.Ref.ID] = pkt
		c.packetByFamily[fam] = pkt
	}
	return c
}

func (c *recordedCorpus) loadPacket(dir string) replay.FixturePacket {
	c.t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "runtime.json"))
	if err != nil {
		c.t.Fatalf("read runtime.json in %s: %v", dir, err)
	}
	v, err := contract.ParseJSON(data)
	if err != nil {
		c.t.Fatalf("parse runtime.json in %s: %v", dir, err)
	}
	obj := objOf(v)
	run := objOf(Get2(obj, "run"))
	sealed := objOf(Get2(obj, "sealed_inputs"))
	env := objOf(Get2(obj, "environment"))
	caps := objOf(Get2(obj, "capabilities"))
	expected := objOf(Get2(obj, "expected"))

	pkt := replay.FixturePacket{
		Ref:    replay.Ref{ID: strOf(Get2(obj, "packet_id")), Version: 1, Digest: contract.DigestBytes(data)},
		Family: strOf(Get2(obj, "family")),
		Side:   strOf(Get2(run, "side")),
		RunID:  strOf(Get2(run, "run_id")),
		Body:   obj,
	}
	pkt.RunAttempt = int(numOf(Get2(run, "attempt")))
	for _, item := range arrOf(sealed, "segment_refs") {
		sr := objOf(item)
		pkt.Segments = append(pkt.Segments, replay.SegmentRefInput{
			RoomID:          strOf(Get2(sr, "room_id")),
			SegmentID:       strOf(Get2(sr, "segment_id")),
			SegmentVersion:  numOf(Get2(sr, "segment_version")),
			SegmentDigest:   strOf(Get2(sr, "segment_digest")),
			EvidenceSealRef: refView(sr, "evidence_seal_ref"),
			PathSealRef:     refView(sr, "path_seal_ref"),
		})
	}
	for _, item := range arrOf(sealed, "path_refs") {
		pr := objOf(item)
		pkt.PathRefs = append(pkt.PathRefs, replay.PathRefView{
			PathID:      strOf(Get2(pr, "path_id")),
			Domain:      strOf(Get2(pr, "domain")),
			SegmentID:   strOf(Get2(pr, "segment_id")),
			PathSealRef: refView(pr, "path_seal_ref"),
		})
	}
	for _, d := range arrOf(sealed, "required_path_domains") {
		pkt.RequiredPathDomains = append(pkt.RequiredPathDomains, strOf(d))
	}
	pkt.SealedInputDigest = digestOf(sealed)
	pkt.EnvironmentDigest = digestOf(env)
	pkt.CapabilityKinds = map[string]string{}
	for _, name := range []string{"clock", "random", "tool", "provider", "filesystem"} {
		pkt.CapabilityKinds[name] = strOf(Get2(objOf(Get2(caps, name)), "kind"))
	}
	for _, tick := range arrOf(objOf(Get2(caps, "clock")), "ticks") {
		pkt.Seeds.ClockTicks = append(pkt.Seeds.ClockTicks, numOf(tick))
	}
	for _, draw := range arrOf(objOf(Get2(caps, "random")), "draws") {
		pkt.Seeds.RandomDraws = append(pkt.Seeds.RandomDraws, numOf(draw))
	}
	pkt.ExpectedDigest = strOf(Get2(expected, "output_digest"))
	pkt.ExpectedStatus = strOf(Get2(expected, "run_status"))
	return pkt
}

func (c *recordedCorpus) Manifest(ctx context.Context) (replay.FixtureManifest, error) {
	var m replay.FixtureManifest
	for _, name := range c.manifestOrder {
		decl := replay.FamilyDeclaration{Name: name}
		switch {
		case c.reserved[name]:
			decl.Reserved = true
		default:
			if pkt, ok := c.packetByFamily[name]; ok {
				decl.PacketRefs = []replay.Ref{pkt.Ref}
			}
		}
		m.Families = append(m.Families, decl)
	}
	return m, nil
}

func (c *recordedCorpus) Packet(ctx context.Context, ref replay.Ref) (replay.FixturePacket, error) {
	pkt, ok := c.packets[ref.ID]
	if !ok {
		return replay.FixturePacket{}, fmt.Errorf("packet %q not in recorded corpus", ref.ID)
	}
	if pkt.Ref.Digest != ref.Digest {
		return replay.FixturePacket{}, fmt.Errorf("packet %q digest mismatch", ref.ID)
	}
	return pkt, nil
}

func (c *recordedCorpus) SealedSegment(ctx context.Context, segmentID string) (replay.SealedSegment, error) {
	dir, ok := c.sealDirs[segmentID]
	if !ok {
		return replay.SealedSegment{}, fmt.Errorf("segment %q not found", segmentID)
	}
	seal := parseFile(c.t, filepath.Join(dir, "seal.json"))
	canonical, err := os.ReadFile(filepath.Join(dir, "canonical.utf8"))
	if err != nil {
		return replay.SealedSegment{}, err
	}
	if contract.DigestBytes(canonical) != strOf(Get2(seal, "segment_digest")) {
		return replay.SealedSegment{}, fmt.Errorf("segment %q seal record corrupt", segmentID)
	}
	ss := replay.SealedSegment{
		TerminalState: strOf(Get2(seal, "terminal_state")),
		SegmentDigest: strOf(Get2(seal, "segment_digest")),
	}
	if ss.TerminalState != "settled" {
		return ss, nil
	}
	sr := objOf(Get2(seal, "segment_ref"))
	ss.SegmentRef = replay.SegmentRefInput{
		RoomID:          strOf(Get2(sr, "room_id")),
		SegmentID:       strOf(Get2(sr, "segment_id")),
		SegmentVersion:  numOf(Get2(sr, "segment_version")),
		SegmentDigest:   strOf(Get2(sr, "segment_digest")),
		EvidenceSealRef: refView(sr, "evidence_seal_ref"),
		PathSealRef:     refView(sr, "path_seal_ref"),
	}
	ps := objOf(Get2(seal, "path_seal"))
	ss.PathSealRef = replay.Ref{
		ID:      strOf(Get2(ps, "seal_id")),
		Version: numOf(Get2(ps, "seal_version")),
		Digest:  strOf(Get2(ps, "seal_digest")),
	}
	seg := parseFile(c.t, filepath.Join(dir, "segment.json"))
	ss.PathDomains = map[string]string{}
	for _, item := range arrOf(objOf(Get2(seg, "path_seal_body")), "paths") {
		p := objOf(item)
		ss.PathDomains[strOf(Get2(p, "path_id"))] = strOf(Get2(p, "domain"))
	}
	return ss, nil
}

func (c *recordedCorpus) profileRef() replay.Ref {
	return refView(objOf(Get2(c.packetByFamily["baseline"].Body, "run")), "profile_ref")
}

func (c *recordedCorpus) adapterRef() replay.Ref {
	return refView(objOf(Get2(c.packetByFamily["baseline"].Body, "run")), "adapter_ref")
}

// staticProfiles implements ProfileSource with the frozen replay profile.
type staticProfiles struct{ byID map[string]replay.Profile }

func (p staticProfiles) Profile(ctx context.Context, ref replay.Ref) (replay.Profile, error) {
	prof, ok := p.byID[ref.ID]
	if !ok {
		return replay.Profile{}, fmt.Errorf("profile %q unavailable", ref.ID)
	}
	return prof, nil
}

// ---------------------------------------------------------------------------
// Fake Q29-B runner (RunnerPort): deterministic simulation over the
// recorded capabilities, mirroring validate_recorded.py semantics.
// ---------------------------------------------------------------------------

var toolFailureReason = map[string]string{
	"timeout_no_response": "FAKE_TOOL_NO_RESPONSE",
	"no_response":         "FAKE_TOOL_NO_RESPONSE",
	"cancelled":           "FAKE_TOOL_CANCELLED",
}

type q29Sim struct {
	status        string
	reason        string
	completedStep int64
	failingStep   int64
	failingSet    bool
	terminalTick  int64
	terminalSet   bool
	clockTicks    []int64
	randomDraws   []int64
	toolIdx       []int64
	providerIdx   []int64
	fsPaths       []string
	clockReads    int64
	randomCount   int64
	toolCalls     int64
	providerCalls int64
	fsAccesses    int64
	toolStatuses  []string
}

func (s *q29Sim) fail(code string, step int64) {
	s.status = "failed"
	s.reason = code
	s.failingStep = step
	s.failingSet = true
}

func simulateQ29(body *contract.Object) q29Sim {
	sim := q29Sim{status: "succeeded"}
	caps := objOf(Get2(body, "capabilities"))
	var ticks, draws []int64
	for _, t := range arrOf(objOf(Get2(caps, "clock")), "ticks") {
		ticks = append(ticks, numOf(t))
	}
	for _, d := range arrOf(objOf(Get2(caps, "random")), "draws") {
		draws = append(draws, numOf(d))
	}
	toolResponses := arrOf(objOf(Get2(caps, "tool")), "responses")
	providerResponses := arrOf(objOf(Get2(caps, "provider")), "responses")
	fsEntries := map[string]bool{}
	for _, e := range arrOf(objOf(Get2(caps, "filesystem")), "entries") {
		fsEntries[strOf(Get2(objOf(e), "path"))] = true
	}
	clockI, randomI := 0, 0

	for _, item := range arrOf(body, "call_sequence") {
		e := objOf(item)
		step := numOf(Get2(e, "step"))
		capability := strOf(Get2(e, "capability"))
		switch capability {
		case "clock":
			if clockI >= len(ticks) {
				sim.fail("FAKE_CLOCK_EXHAUSTED", step)
				return sim
			}
			sim.clockTicks = append(sim.clockTicks, ticks[clockI])
			sim.terminalTick = ticks[clockI]
			sim.terminalSet = true
			clockI++
			sim.clockReads++
		case "random":
			if randomI >= len(draws) {
				sim.fail("FAKE_RANDOM_EXHAUSTED", step)
				return sim
			}
			sim.randomDraws = append(sim.randomDraws, draws[randomI])
			randomI++
			sim.randomCount++
		case "tool":
			callIndex := numOf(Get2(e, "call_index"))
			toolName := strOf(Get2(e, "tool_name"))
			var found *contract.Object
			for _, r := range toolResponses {
				ro := objOf(r)
				if numOf(Get2(ro, "call_index")) == callIndex && strOf(Get2(ro, "tool_name")) == toolName {
					found = ro
					break
				}
			}
			if found == nil {
				sim.fail("UNDECLARED_TOOL_ACCESS", step)
				return sim
			}
			sim.toolIdx = append(sim.toolIdx, callIndex)
			sim.toolCalls++
			sim.toolStatuses = append(sim.toolStatuses, strOf(Get2(found, "status")))
		case "provider":
			callIndex := numOf(Get2(e, "call_index"))
			var found *contract.Object
			for _, r := range providerResponses {
				ro := objOf(r)
				if numOf(Get2(ro, "call_index")) != callIndex {
					continue
				}
				if mid, ok := e.Get("model_id"); ok {
					if strOf(mid) != strOf(Get2(ro, "model_id")) {
						continue
					}
				}
				found = ro
				break
			}
			if found == nil {
				sim.fail("UNDECLARED_PROVIDER_ACCESS", step)
				return sim
			}
			sim.providerIdx = append(sim.providerIdx, callIndex)
			sim.providerCalls++
		case "filesystem":
			path := strOf(Get2(e, "path"))
			if !fsEntries[path] {
				sim.fail("UNDECLARED_FS_ACCESS", step)
				return sim
			}
			sim.fsPaths = append(sim.fsPaths, path)
			sim.fsAccesses++
		default:
			sim.fail("UNDECLARED_CAPABILITY_ACCESS", step)
			return sim
		}
		sim.completedStep = step
	}
	for _, st := range sim.toolStatuses {
		if st != "succeeded" {
			sim.status = "failed"
			sim.reason = toolFailureReason[st]
			if sim.reason == "" {
				sim.reason = "UNDECLARED_TOOL_ACCESS"
			}
			break
		}
	}
	return sim
}

func bnum(n int64) contract.Number { return contract.Number(strconv.FormatInt(n, 10)) }

func refObj(r replay.Ref) *contract.Object {
	o := contract.NewObject()
	o.Set("id", contract.String(r.ID))
	o.Set("version", bnum(r.Version))
	o.Set("digest", contract.String(r.Digest))
	return o
}

func numArray(vals []int64) contract.Array {
	out := contract.Array{}
	for _, v := range vals {
		out = append(out, bnum(v))
	}
	return out
}

func strArray(vals []string) contract.Array {
	out := contract.Array{}
	for _, v := range vals {
		out = append(out, contract.String(v))
	}
	return out
}

func buildOutputDoc(pkt replay.FixturePacket, att replay.Attempt, sim q29Sim) *contract.Object {
	doc := contract.NewObject()
	doc.Set("schema_version", contract.String("rsih.q29b-run-output.v1"))
	doc.Set("packet_id", contract.String(pkt.Ref.ID))
	run := contract.NewObject()
	run.Set("mode", contract.String("replay"))
	run.Set("side", contract.String(att.Inputs.RunKey.Side))
	run.Set("run_id", contract.String(att.RunID))
	run.Set("attempt", bnum(int64(att.AttemptNo)))
	run.Set("adapter_ref", refObj(att.Inputs.Adapter))
	run.Set("profile_ref", refObj(att.Inputs.Profile))
	doc.Set("run", run)
	doc.Set("sealed_input_digest", contract.String(pkt.SealedInputDigest))
	doc.Set("environment_digest", contract.String(pkt.EnvironmentDigest))
	doc.Set("steps_executed", bnum(sim.completedStep))
	usage := contract.NewObject()
	usage.Set("clock_reads", bnum(sim.clockReads))
	usage.Set("random_draws", bnum(sim.randomCount))
	usage.Set("tool_calls", bnum(sim.toolCalls))
	usage.Set("provider_calls", bnum(sim.providerCalls))
	usage.Set("fs_accesses", bnum(sim.fsAccesses))
	doc.Set("capability_usage", usage)
	consumed := contract.NewObject()
	consumed.Set("clock_ticks", numArray(sim.clockTicks))
	consumed.Set("random_draws", numArray(sim.randomDraws))
	consumed.Set("tool_call_indexes", numArray(sim.toolIdx))
	consumed.Set("provider_call_indexes", numArray(sim.providerIdx))
	consumed.Set("fs_paths", strArray(sim.fsPaths))
	doc.Set("consumed", consumed)
	lateAudit := contract.Array{}
	for _, item := range arrOf(pkt.Body, "late_events") {
		ev := objOf(item)
		le := contract.NewObject()
		le.Set("arrival_step", bnum(numOf(Get2(ev, "arrival_step"))))
		le.Set("capability", contract.String(strOf(Get2(ev, "capability"))))
		le.Set("origin", contract.String(strOf(Get2(ev, "origin"))))
		if d, ok := ev.Get("detail"); ok {
			le.Set("detail", d)
		}
		le.Set("recorded_as", contract.String(strOf(Get2(ev, "recorded_as"))))
		lateAudit = append(lateAudit, le)
	}
	doc.Set("late_audit", lateAudit)
	terminal := contract.NewObject()
	terminal.Set("status", contract.String(sim.status))
	if sim.reason == "" {
		terminal.Set("reason_code", contract.Null{})
	} else {
		terminal.Set("reason_code", contract.String(sim.reason))
	}
	if sim.terminalSet {
		terminal.Set("terminal_tick", bnum(sim.terminalTick))
	} else {
		terminal.Set("terminal_tick", contract.Null{})
	}
	terminal.Set("completed_step", bnum(sim.completedStep))
	if sim.failingSet {
		terminal.Set("failing_step", bnum(sim.failingStep))
	} else {
		terminal.Set("failing_step", contract.Null{})
	}
	doc.Set("terminal", terminal)
	return doc
}

func lateEventsOf(pkt replay.FixturePacket) []replay.LateEvent {
	var out []replay.LateEvent
	for _, item := range arrOf(pkt.Body, "late_events") {
		ev := objOf(item)
		var detail *contract.Object
		if d, ok := ev.Get("detail"); ok {
			if do, ok := d.(*contract.Object); ok {
				detail = do
			}
		}
		out = append(out, replay.LateEvent{
			ArrivalStep: numOf(Get2(ev, "arrival_step")),
			Capability:  strOf(Get2(ev, "capability")),
			Origin:      strOf(Get2(ev, "origin")),
			Detail:      detail,
			RecordedAs:  strOf(Get2(ev, "recorded_as")),
		})
	}
	return out
}

// script injects runner-side behavior for one run key.
type script struct {
	kind      string // semantic | infra | flaky | late-rewrite
	code      string
	failCalls int
}

type fakeRunner struct {
	packets    map[string]replay.FixturePacket // family -> packet
	scripts    map[string]*script              // family/side
	calls      map[string]int
	recordedOK map[string]bool
}

func newFakeRunner(c *recordedCorpus) *fakeRunner {
	return &fakeRunner{
		packets:    c.packetByFamily,
		scripts:    map[string]*script{},
		calls:      map[string]int{},
		recordedOK: map[string]bool{},
	}
}

func syntheticFailureDigest(att replay.Attempt, code string) string {
	o := contract.NewObject()
	o.Set("correlation_id", contract.String(att.CorrelationID))
	o.Set("status", contract.String("failed"))
	o.Set("reason_code", contract.String(code))
	return digestOf(o)
}

func (f *fakeRunner) Run(ctx context.Context, att replay.Attempt) (replay.RawOutput, error) {
	key := att.RunKey.Family + "/" + att.RunKey.Side
	f.calls[key]++
	pkt, ok := f.packets[att.RunKey.Family]
	if !ok {
		return replay.RawOutput{}, fmt.Errorf("no packet for family %q", att.RunKey.Family)
	}
	sim := simulateQ29(pkt.Body)
	ref := replay.Ref{
		ID:      "rsih.q29b-run-output/" + pkt.Ref.ID + "/" + att.RunKey.Side + "/" + strconv.Itoa(att.AttemptNo),
		Version: 1,
		Digest:  digestOf(buildOutputDoc(pkt, att, sim)),
	}
	sc := f.scripts[key]
	if sc == nil && att.RunKey.Side == pkt.Side && att.AttemptNo == pkt.RunAttempt {
		// Recorded identity: the deterministic simulation must reproduce
		// the frozen expected-output.canonical digest exactly.
		if ref.Digest != pkt.ExpectedDigest {
			return replay.RawOutput{}, fmt.Errorf(
				"recorded-side output digest mismatch for %s: got %s want %s",
				key, ref.Digest, pkt.ExpectedDigest)
		}
		f.recordedOK[key] = true
	}
	status, reason := sim.status, sim.reason
	late := lateEventsOf(pkt)
	if sc != nil {
		switch sc.kind {
		case "semantic":
			status, reason = "failed", sc.code
			ref.Digest = syntheticFailureDigest(att, sc.code)
		case "infra":
			if f.calls[key] <= sc.failCalls {
				status, reason = "failed", sc.code
				ref.Digest = syntheticFailureDigest(att, sc.code)
			}
		case "flaky":
			if f.calls[key]%2 == 0 {
				ref.Digest = "sha256:" + strings.Repeat("0", 64)
			}
		case "late-rewrite":
			late = append(late, replay.LateEvent{
				ArrivalStep: sim.completedStep + 1,
				Capability:  "tool",
				Origin:      "late-upstream-response",
				RecordedAs:  "terminal-rewrite",
			})
		}
	}
	return replay.RawOutput{
		Ref:        ref,
		Digest:     ref.Digest,
		Status:     status,
		ReasonCode: reason,
		LateAudit:  late,
	}, nil
}

// ---------------------------------------------------------------------------
// Request builder and harness helpers
// ---------------------------------------------------------------------------

func buildMergeRequest(t *testing.T, c *recordedCorpus) replay.Request {
	t.Helper()
	ctx := context.Background()
	man, err := c.Manifest(ctx)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	var fixtureRefs []replay.Ref
	for _, fam := range man.Families {
		if !fam.Reserved {
			fixtureRefs = append(fixtureRefs, fam.PacketRefs...)
		}
	}
	segMap := map[string]replay.SegmentRefInput{}
	for _, r := range fixtureRefs {
		pkt, err := c.Packet(ctx, r)
		if err != nil {
			t.Fatalf("packet: %v", err)
		}
		for _, s := range pkt.Segments {
			segMap[s.SegmentID] = s
		}
	}
	var segIDs []string
	for id := range segMap {
		segIDs = append(segIDs, id)
	}
	sort.Strings(segIDs)
	segRefs := []replay.SegmentRefInput{}
	for _, id := range segIDs {
		segRefs = append(segRefs, segMap[id])
	}

	base := c.packetByFamily["baseline"]
	cand := c.packetByFamily["candidate"]
	baseArt := objOf(Get2(objOf(Get2(base.Body, "sealed_inputs")), "artifact_ref"))
	candArt := objOf(Get2(objOf(Get2(cand.Body, "sealed_inputs")), "artifact_ref"))
	bil := objOf(Get2(c.packetByFamily["overlap"].Body, "bilateral"))

	return replay.Request{
		ReplayRequestID:   "replay-req-0001",
		CorrelationID:     "sched-corr-0001",
		Mode:              replay.ModeCausalEvaluation,
		IdempotencyKey:    "sha256:" + strings.Repeat("ab", 32),
		CandidateRef:      candidateRefOf(candArt),
		BaselineSkillRefs: []replay.SkillRef{skillRefOf(baseArt)},
		FixtureSetRefs:    fixtureRefs,
		SegmentRefs:       segRefs,
		ReplayProfileRef:  c.profileRef(),
		RuntimeAdapterRef: c.adapterRef(),
		RequiredSourceHeads: []replay.SkillRef{
			skillRefOf(objOf(Get2(bil, "source_a_ref"))),
			skillRefOf(objOf(Get2(bil, "source_b_ref"))),
		},
		Permissions: map[string]int64{"memory_explore": 1, "skill_get": 1},
	}
}

func newScheduler(t *testing.T, c *recordedCorpus, runner replay.RunnerPort, bundle *contract.ReasonBundle) *replay.Scheduler {
	t.Helper()
	prof := replay.Profile{
		Ref:              c.profileRef(),
		Available:        true,
		PermissionCaps:   map[string]int64{"memory_explore": 4, "memory_expand": 2, "skill_get": 8},
		Adapter:          c.adapterRef(),
		AdapterAvailable: true,
	}
	s, err := replay.NewScheduler(replay.Deps{Seals: c, Fixtures: c, Profiles: staticProfiles{
		byID: map[string]replay.Profile{prof.Ref.ID: prof},
	}}, runner, replay.Options{Reasons: bundle})
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	return s
}

func loadBundle(t *testing.T) *contract.ReasonBundle {
	t.Helper()
	confDir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("conformance dir: %v", err)
	}
	bundle, err := contract.LoadReasonBundle(filepath.Join(confDir, "policy"))
	if err != nil {
		t.Fatalf("load reason bundle: %v", err)
	}
	return bundle
}

func findOutput(t *testing.T, res replay.ScheduleResult, family, side string) replay.OutputRecord {
	t.Helper()
	for _, o := range res.Outputs {
		if o.Family == family && o.Side == side {
			return o
		}
	}
	t.Fatalf("no output record for %s/%s", family, side)
	return replay.OutputRecord{}
}

func attemptsFor(res replay.ScheduleResult, family, side string) []replay.AttemptRecord {
	var out []replay.AttemptRecord
	for _, a := range res.Attempts {
		if a.RunKey.Family == family && a.RunKey.Side == side {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AttemptNo < out[j].AttemptNo })
	return out
}

// ---------------------------------------------------------------------------
// contract.Value convenience helpers
// ---------------------------------------------------------------------------

func objOf(v contract.Value) *contract.Object {
	if o, ok := v.(*contract.Object); ok {
		return o
	}
	return contract.NewObject()
}

func strOf(v contract.Value) string {
	if s, ok := v.(contract.String); ok {
		return string(s)
	}
	return ""
}

func numOf(v contract.Value) int64 {
	if n, ok := v.(contract.Number); ok {
		if bi, ok := n.Int(); ok {
			return bi.Int64()
		}
	}
	return 0
}

func arrOf(o *contract.Object, key string) contract.Array {
	v, ok := o.Get(key)
	if !ok {
		return nil
	}
	if a, ok := v.(contract.Array); ok {
		return a
	}
	return nil
}

// Get2 fetches a key as a Value, never nil.
func Get2(o *contract.Object, key string) contract.Value {
	v, _ := o.Get(key)
	if v == nil {
		return contract.Null{}
	}
	return v
}

func refView(o *contract.Object, key string) replay.Ref {
	ro := objOf(Get2(o, key))
	return replay.Ref{ID: strOf(Get2(ro, "id")), Version: numOf(Get2(ro, "version")), Digest: strOf(Get2(ro, "digest"))}
}

func skillRefOf(o *contract.Object) replay.SkillRef {
	return replay.SkillRef{
		LineageID:      strOf(Get2(o, "lineage_id")),
		Version:        numOf(Get2(o, "version")),
		Kind:           strOf(Get2(o, "kind")),
		ArtifactDigest: strOf(Get2(o, "artifact_digest")),
	}
}

func candidateRefOf(o *contract.Object) replay.CandidateRef {
	return replay.CandidateRef{
		CandidateID: strOf(Get2(o, "candidate_id")),
		Kind:        strOf(Get2(o, "kind")),
		BodyDigest:  strOf(Get2(o, "body_digest")),
		OriginType:  strOf(Get2(o, "origin_type")),
		OriginRef:   refView(o, "origin_ref"),
	}
}

func digestOf(v contract.Value) string {
	d, err := contract.DigestOf(v)
	if err != nil {
		return ""
	}
	return d
}

func parseFile(t *testing.T, path string) *contract.Object {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	v, err := contract.ParseJSON(data)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return objOf(v)
}

// ---------------------------------------------------------------------------
// Red test 1: paired plan freeze over all fixture families
// ---------------------------------------------------------------------------

// TestSchedulerFreezesPairedPlanAndAllFixtureFamilies is the HST-203 Red
// test: a merge ReplayRequest over the whole recorded q29b corpus must
// freeze a deterministic plan covering every family in the fixed order
// (merge reserved as a declared empty placeholder), dispatch both sides
// of every non-reserved family on identical frozen inputs, correlate raw
// outputs whose refs are stable across reruns, keep late outputs
// audit-only, and leave the terminal-semantics families fail-closed
// without ever producing accepted evaluation semantics.
func TestSchedulerFreezesPairedPlanAndAllFixtureFamilies(t *testing.T) {
	ctx := context.Background()
	c := loadRecordedCorpus(t)
	bundle := loadBundle(t)
	req := buildMergeRequest(t, c)

	s1 := newScheduler(t, c, newFakeRunner(c), bundle)
	res1 := s1.Schedule(ctx, &req)
	if !res1.Accepted {
		t.Fatalf("schedule rejected: %s %s", res1.ReasonCode, res1.Detail)
	}

	// Frozen family order: the four paired families first, then the
	// terminal-semantics families, reserved merge last.
	wantOrder := []string{
		"baseline", "candidate", "overlap", "conflict",
		"late-output", "clock-exhausted", "extra-tool-call", "merge",
	}
	if got := res1.Plan.FamilyOrder; join(got) != join(wantOrder) {
		t.Fatalf("family order = %v, want %v", got, wantOrder)
	}
	if len(res1.Plan.Families) != len(wantOrder) {
		t.Fatalf("plan families = %d, want %d", len(res1.Plan.Families), len(wantOrder))
	}
	merge := res1.Plan.Families[len(res1.Plan.Families)-1]
	if merge.Family != "merge" || !merge.Reserved || len(merge.Fixtures) != 0 || len(merge.Variants) != 0 {
		t.Fatalf("merge family must be a declared reserved placeholder, got %+v", merge)
	}

	// Paired dispatch: every non-reserved family runs baseline and
	// candidate variants — 7 families x 2 sides = 14 correlated outputs.
	if len(res1.Outputs) != 14 {
		t.Fatalf("output records = %d, want 14 (7 families x 2 sides)", len(res1.Outputs))
	}
	nonReserved := []string{"baseline", "candidate", "overlap", "conflict", "late-output", "clock-exhausted", "extra-tool-call"}
	for _, family := range nonReserved {
		famPlan := familyPlanOf(res1.Plan, family)
		if len(famPlan.Variants) != 2 || famPlan.Variants[0].Side != replay.SideBaseline || famPlan.Variants[1].Side != replay.SideCandidate {
			t.Fatalf("family %s variants = %+v, want [baseline candidate]", family, famPlan.Variants)
		}
		for _, side := range []string{replay.SideBaseline, replay.SideCandidate} {
			atts := attemptsFor(res1, family, side)
			if len(atts) == 0 {
				t.Fatalf("family %s side %s never dispatched", family, side)
			}
			out := findOutput(t, res1, family, side)
			if out.RunID == "" || out.OutputRef.Digest == "" {
				t.Fatalf("family %s side %s missing correlated output ref", family, side)
			}
			// Both sides of a family receive the same frozen inputs.
			if len(atts[0].Inputs.Fixtures) != len(famPlan.Fixtures) {
				t.Fatalf("family %s side %s frozen fixtures differ from plan", family, side)
			}
			if atts[0].Inputs.PlanDigest != res1.Plan.PlanDigest {
				t.Fatalf("family %s side %s attempt carries a foreign plan digest", family, side)
			}
		}
	}

	// Recorded-side raw outputs reproduce the frozen expected-output
	// digests bit-for-bit (the fake runner asserted equality itself; the
	// scheduler must have correlated exactly those digests). The frozen
	// expectation covers the recorded attempt identity, i.e. attempt 1
	// of the packet's native side.
	for _, family := range nonReserved {
		pkt := c.packetByFamily[family]
		native := attemptsFor(res1, family, pkt.Side)
		if len(native) == 0 || native[0].Output == nil || native[0].Output.Digest != pkt.ExpectedDigest {
			t.Fatalf("family %s native side %s attempt-1 digest != frozen expectation %s",
				family, pkt.Side, pkt.ExpectedDigest)
		}
	}
	// Non-retried native lanes correlate the recorded digest as the
	// terminal output digest; the retried lane (late-output) carries the
	// terminal attempt's digest instead.
	for _, family := range []string{"baseline", "candidate", "overlap", "conflict", "clock-exhausted", "extra-tool-call"} {
		pkt := c.packetByFamily[family]
		out := findOutput(t, res1, family, pkt.Side)
		if out.OutputDigest != pkt.ExpectedDigest {
			t.Fatalf("family %s recorded side %s digest = %s, want frozen expectation %s",
				family, pkt.Side, out.OutputDigest, pkt.ExpectedDigest)
		}
	}

	// Success families correlate succeeded outputs on both sides.
	for _, family := range []string{"baseline", "candidate", "overlap", "conflict"} {
		for _, side := range []string{replay.SideBaseline, replay.SideCandidate} {
			out := findOutput(t, res1, family, side)
			if out.TerminalStatus != "succeeded" {
				t.Fatalf("family %s side %s terminal = %s (%s), want succeeded",
					family, side, out.TerminalStatus, out.ReasonCode)
			}
		}
	}

	// Late output: infra-retryable failure (registry FAKE_TOOL_NO_RESPONSE,
	// retry_scope=same_request) retries within the frozen budget only; the
	// late event lands in the audit trail only and never rewrites the
	// terminal; no accepted semantics for this family.
	late := findOutput(t, res1, "late-output", replay.SideBaseline)
	if late.TerminalStatus != "failed" || late.ReasonCode != "FAKE_TOOL_NO_RESPONSE" {
		t.Fatalf("late-output terminal = %s (%s), want failed (FAKE_TOOL_NO_RESPONSE)",
			late.TerminalStatus, late.ReasonCode)
	}
	if late.Attempts != 2 {
		t.Fatalf("late-output attempts = %d, want 2 (bounded infra retry)", late.Attempts)
	}
	if len(late.LateAudit) != 1 || late.LateAudit[0].RecordedAs != "late-audit-only" {
		t.Fatalf("late-output late audit = %+v, want one audit-only event", late.LateAudit)
	}
	lateAtts := attemptsFor(res1, "late-output", replay.SideBaseline)
	if len(lateAtts) != 2 || lateAtts[0].AttemptNo != 1 || lateAtts[1].AttemptNo != 2 {
		t.Fatalf("late-output attempts not append-only 1,2: %+v", lateAtts)
	}
	if !lateAtts[0].Retryable || lateAtts[0].Outcome != replay.OutcomeFailed {
		t.Fatalf("late-output attempt 1 must be a retryable infra failure, got %+v", lateAtts[0])
	}

	// Terminal-semantics families fail closed, single attempt, no retry,
	// no succeeded output.
	for family, code := range map[string]string{
		"clock-exhausted": "FAKE_CLOCK_EXHAUSTED",
		"extra-tool-call": "UNDECLARED_TOOL_ACCESS",
	} {
		for _, side := range []string{replay.SideBaseline, replay.SideCandidate} {
			out := findOutput(t, res1, family, side)
			if out.TerminalStatus != "failed" || out.ReasonCode != code {
				t.Fatalf("%s/%s terminal = %s (%s), want failed (%s)",
					family, side, out.TerminalStatus, out.ReasonCode, code)
			}
			if out.Attempts != 1 {
				t.Fatalf("%s/%s attempts = %d, want 1 (semantic failures never retry)",
					family, side, out.Attempts)
			}
		}
	}

	// Plan freeze: seeds, permissions, profile/adapter, capture policy.
	base := familyPlanOf(res1.Plan, "baseline")
	if join64(base.Seeds.ClockTicks) != "1000,1001,1002,1003" || join64(base.Seeds.RandomDraws) != "7" {
		t.Fatalf("baseline family seeds = %v / %v, want declared [1000 1001 1002 1003] / [7]",
			base.Seeds.ClockTicks, base.Seeds.RandomDraws)
	}
	if base.Seeds.Digest == "" {
		t.Fatal("baseline family seeds digest missing")
	}
	if res1.Plan.Permissions["memory_explore"] != 1 || res1.Plan.PermissionCaps["memory_explore"] != 4 {
		t.Fatalf("permission snapshot/caps not frozen: %+v / %+v",
			res1.Plan.Permissions, res1.Plan.PermissionCaps)
	}
	if res1.Plan.Profile != req.ReplayProfileRef || res1.Plan.Adapter != req.RuntimeAdapterRef {
		t.Fatal("plan did not freeze the exact profile/adapter refs")
	}
	if res1.Plan.CapturePolicyDigest == "" || res1.Plan.PlanDigest == "" ||
		res1.Plan.ReplayRequestDigest == "" || res1.Plan.RetryPolicy.MaxAttempts < 1 {
		t.Fatal("plan digests/retry policy not frozen")
	}
	// Baseline variant reuses the packet run identity; the cross side
	// derives a distinct deterministic identity.
	if base.Variants[0].RunID != "run-baseline-0001" {
		t.Fatalf("baseline variant = %+v", base.Variants[0])
	}
	if base.Variants[1].RunID != "run-baseline-0001#candidate" {
		t.Fatalf("candidate variant = %+v", base.Variants[1])
	}

	// Source-head expectations are preserved verbatim and returned; the
	// Host never judges their freshness (GMS hard gate).
	if len(res1.SourceHeadExpectations) != 2 ||
		res1.SourceHeadExpectations[0] != req.RequiredSourceHeads[0] ||
		res1.SourceHeadExpectations[1] != req.RequiredSourceHeads[1] {
		t.Fatalf("source head expectations not preserved: %+v", res1.SourceHeadExpectations)
	}

	// Determinism: a fresh scheduler over the same frozen inputs
	// reproduces identical plan digest and output refs.
	s2 := newScheduler(t, c, newFakeRunner(c), bundle)
	res2 := s2.Schedule(ctx, &req)
	if !res2.Accepted {
		t.Fatalf("rerun rejected: %s %s", res2.ReasonCode, res2.Detail)
	}
	if res2.Plan.PlanDigest != res1.Plan.PlanDigest {
		t.Fatalf("plan digest drifted across reruns: %s != %s",
			res2.Plan.PlanDigest, res1.Plan.PlanDigest)
	}
	for _, o1 := range res1.Outputs {
		o2 := findOutput(t, res2, o1.Family, o1.Side)
		if o1.OutputRef != o2.OutputRef || o1.OutputDigest != o2.OutputDigest {
			t.Fatalf("output refs unstable for %s/%s: %+v vs %+v",
				o1.Family, o1.Side, o1.OutputRef, o2.OutputRef)
		}
	}
	for _, p := range res2.DeterminismProbes {
		if !p.Matched {
			t.Fatalf("determinism probe mismatch: %+v", p)
		}
	}

	// Idempotency: the same ReplayRequest under the same key reuses the
	// frozen plan and the recorded attempts; nothing is re-dispatched.
	before := len(res1.Attempts)
	res1b := s1.Schedule(ctx, &req)
	if !res1b.Accepted || !res1b.IdempotentReplay {
		t.Fatalf("idempotent replay rejected: %+v", res1b)
	}
	if res1b.Plan.PlanDigest != res1.Plan.PlanDigest || len(res1b.Attempts) != before {
		t.Fatal("idempotent replay changed the frozen plan or appended attempts")
	}
}

// ---------------------------------------------------------------------------
// Red test 2: semantic failure cannot retry to pass
// ---------------------------------------------------------------------------

// TestSemanticFailureCannotRetryToPass is the HST-203 Red test for retry
// semantics: a semantic execution failure (closed-registry code that is
// not retryable/same_request) terminates its dispatch with a single
// attempt; neither an idempotent re-schedule nor a fresh scheduler may
// append retries or turn the outcome into a pass. The Host reports the
// failure; it never converts it into accepted semantics.
func TestSemanticFailureCannotRetryToPass(t *testing.T) {
	ctx := context.Background()
	c := loadRecordedCorpus(t)
	bundle := loadBundle(t)
	req := buildMergeRequest(t, c)

	// Registry sanity: the retry whitelist and the semantic code.
	if entry, ok := bundle.Lookup("FAKE_TOOL_NO_RESPONSE"); !ok || !entry.Retryable || entry.RetryScope != "same_request" {
		t.Fatal("registry must classify FAKE_TOOL_NO_RESPONSE as retryable/same_request")
	}
	if entry, ok := bundle.Lookup("UNDECLARED_TOOL_ACCESS"); !ok || entry.Retryable {
		t.Fatal("registry must classify UNDECLARED_TOOL_ACCESS as non-retryable")
	}

	runner := newFakeRunner(c)
	runner.scripts["candidate/"+replay.SideCandidate] = &script{kind: "semantic", code: "UNDECLARED_TOOL_ACCESS"}
	s := newScheduler(t, c, runner, bundle)

	res := s.Schedule(ctx, &req)
	if !res.Accepted {
		t.Fatalf("scheduling must still be accepted (execution outcome is reported, not scored): %s", reasonOf(res))
	}
	out := findOutput(t, res, "candidate", replay.SideCandidate)
	if out.TerminalStatus != "failed" || out.ReasonCode != "UNDECLARED_TOOL_ACCESS" {
		t.Fatalf("semantic failure outcome = %s (%s), want failed (UNDECLARED_TOOL_ACCESS)",
			out.TerminalStatus, out.ReasonCode)
	}
	if out.Attempts != 1 {
		t.Fatalf("semantic failure retried: attempts = %d, want 1", out.Attempts)
	}
	atts := attemptsFor(res, "candidate", replay.SideCandidate)
	if len(atts) != 1 || atts[0].Retryable || atts[0].Outcome != replay.OutcomeFailed {
		t.Fatalf("semantic attempt misclassified: %+v", atts)
	}

	// The same request cannot be retried into a pass: idempotent
	// re-schedule reuses the frozen plan and the recorded attempts.
	res2 := s.Schedule(ctx, &req)
	if !res2.Accepted || !res2.IdempotentReplay {
		t.Fatalf("re-schedule must be an idempotent replay: %s", reasonOf(res2))
	}
	out2 := findOutput(t, res2, "candidate", replay.SideCandidate)
	if out2.Attempts != 1 || out2.TerminalStatus != "failed" {
		t.Fatalf("re-schedule appended retries or flipped the outcome: %+v", out2)
	}

	// A fresh scheduler with the same deterministic failure still cannot
	// pass: one attempt, failed, no succeeded output.
	runner3 := newFakeRunner(c)
	runner3.scripts["candidate/"+replay.SideCandidate] = &script{kind: "semantic", code: "UNDECLARED_TOOL_ACCESS"}
	s3 := newScheduler(t, c, runner3, bundle)
	res3 := s3.Schedule(ctx, &req)
	if !res3.Accepted {
		t.Fatalf("fresh scheduler rejected: %s", reasonOf(res3))
	}
	out3 := findOutput(t, res3, "candidate", replay.SideCandidate)
	if out3.Attempts != 1 || out3.TerminalStatus != "failed" || out3.ReasonCode != "UNDECLARED_TOOL_ACCESS" {
		t.Fatalf("fresh scheduler retried the semantic failure: %+v", out3)
	}

	// The failure stays correlated but never becomes an accepted
	// evaluation: no succeeded output exists for the failed lane.
	for _, o := range res3.Outputs {
		if o.Family == "candidate" && o.Side == replay.SideCandidate && o.TerminalStatus == "succeeded" {
			t.Fatal("semantic failure lane produced a succeeded output")
		}
	}
}

// ---------------------------------------------------------------------------
// Infra retry semantics
// ---------------------------------------------------------------------------

func TestSchedulerInfraRetryIsBoundedAndAppendsAttempts(t *testing.T) {
	ctx := context.Background()
	c := loadRecordedCorpus(t)
	bundle := loadBundle(t)
	req := buildMergeRequest(t, c)

	clean := newScheduler(t, c, newFakeRunner(c), bundle)
	cleanRes := clean.Schedule(ctx, &req)
	if !cleanRes.Accepted {
		t.Fatalf("clean schedule rejected: %s", reasonOf(cleanRes))
	}

	runner := newFakeRunner(c)
	// One transient infra failure, then the deterministic packet result.
	runner.scripts["baseline/"+replay.SideBaseline] = &script{kind: "infra", code: "FAKE_TOOL_NO_RESPONSE", failCalls: 1}
	// A permanently unavailable worker: retries stay bounded.
	runner.scripts["overlap/"+replay.SideBaseline] = &script{kind: "infra", code: "FAKE_TOOL_NO_RESPONSE", failCalls: 99}
	s := newScheduler(t, c, runner, bundle)
	res := s.Schedule(ctx, &req)
	if !res.Accepted {
		t.Fatalf("schedule rejected: %s", reasonOf(res))
	}

	// Plan freeze survives retries: the frozen plan is input-only, so its
	// digest equals the unscripted run's digest.
	if res.Plan.PlanDigest != cleanRes.Plan.PlanDigest {
		t.Fatal("retry changed the frozen plan digest")
	}

	recovered := findOutput(t, res, "baseline", replay.SideBaseline)
	if recovered.Attempts != 2 || recovered.TerminalStatus != "succeeded" {
		t.Fatalf("transient infra failure not recovered by a bounded retry: %+v", recovered)
	}
	atts := attemptsFor(res, "baseline", replay.SideBaseline)
	if len(atts) != 2 || !atts[0].Retryable || atts[0].Outcome != replay.OutcomeFailed || atts[1].Outcome != replay.OutcomeSucceeded {
		t.Fatalf("retry did not append an attempt over the recorded failure: %+v", atts)
	}
	if atts[0].CorrelationID == atts[1].CorrelationID {
		t.Fatal("retry must carry a distinct attempt correlation id")
	}
	// The recovered lane's terminal digest is the terminal attempt's
	// deterministic derivation (a retry re-stamps the run attempt
	// identity); it must correlate with the ledger's terminal attempt.
	term := attemptsFor(res, "baseline", replay.SideBaseline)
	if len(term) != 2 || term[1].Output == nil || term[1].Output.Digest != recovered.OutputDigest ||
		!strings.HasPrefix(recovered.OutputDigest, "sha256:") {
		t.Fatalf("recovered lane terminal digest not correlated: %+v", recovered)
	}

	exhausted := findOutput(t, res, "overlap", replay.SideBaseline)
	if exhausted.Attempts != 2 || exhausted.TerminalStatus != "failed" || exhausted.ReasonCode != "FAKE_TOOL_NO_RESPONSE" {
		t.Fatalf("permanent infra failure must stay bounded and failed: %+v", exhausted)
	}
	last := attemptsFor(res, "overlap", replay.SideBaseline)[1]
	if !last.Exhausted {
		t.Fatalf("retry budget exhaustion not recorded: %+v", last)
	}
}

// ---------------------------------------------------------------------------
// Fail-closed rejections
// ---------------------------------------------------------------------------

func TestSchedulerRejectsUnsettledSegmentAndSealMismatch(t *testing.T) {
	ctx := context.Background()
	c := loadRecordedCorpus(t)
	bundle := loadBundle(t)
	req := buildMergeRequest(t, c)
	s := newScheduler(t, c, newFakeRunner(c), bundle)

	// Unsettled segment: the failure-family fixture never settled and
	// carries no refs.
	unsettled := req
	unsettled.SegmentRefs = []replay.SegmentRefInput{{
		RoomID: "room-beta-0001", SegmentID: "seg-failed-0001", SegmentVersion: 1,
		SegmentDigest:   "sha256:" + strings.Repeat("c", 64),
		EvidenceSealRef: replay.Ref{ID: "evseal-x", Version: 1, Digest: "sha256:" + strings.Repeat("d", 64)},
		PathSealRef:     replay.Ref{ID: "pathseal-x", Version: 1, Digest: "sha256:" + strings.Repeat("e", 64)},
	}}
	res := s.Schedule(ctx, &unsettled)
	if res.Accepted || res.ReasonCode != "SEGMENT_NOT_SETTLED" {
		t.Fatalf("unsettled segment must fail closed with SEGMENT_NOT_SETTLED, got %s", res.ReasonCode)
	}
	if res.Plan != nil || len(res.Attempts) != 0 {
		t.Fatal("rejected request must not freeze a plan or dispatch")
	}

	// Seal mismatch: one hex digit of a settled segment's digest flipped.
	tampered := req
	tampered.SegmentRefs = append([]replay.SegmentRefInput(nil), req.SegmentRefs...)
	tampered.SegmentRefs[0].SegmentDigest = flipHex(tampered.SegmentRefs[0].SegmentDigest)
	res2 := s.Schedule(ctx, &tampered)
	if res2.Accepted || res2.ReasonCode != "SEAL_DIGEST_MISMATCH" {
		t.Fatalf("seal mismatch must fail closed with SEAL_DIGEST_MISMATCH, got %s", res2.ReasonCode)
	}
}

func TestSchedulerFailClosedValidation(t *testing.T) {
	ctx := context.Background()
	c := loadRecordedCorpus(t)
	bundle := loadBundle(t)
	base := buildMergeRequest(t, c)
	s := newScheduler(t, c, newFakeRunner(c), bundle)

	cases := []struct {
		name   string
		mutate func(r *replay.Request)
		code   string
	}{
		{
			name:   "mode not causal_evaluation",
			mutate: func(r *replay.Request) { r.Mode = "live" },
			code:   "REPLAY_REQUEST_INVALID",
		},
		{
			name: "baseline ref latest",
			mutate: func(r *replay.Request) {
				r.BaselineSkillRefs[0].LineageID = "latest"
			},
			code: "NON_EXACT_REF",
		},
		{
			name: "baseline ref graph node id",
			mutate: func(r *replay.Request) {
				r.BaselineSkillRefs[0].LineageID = "graph:skill-alpha"
			},
			code: "NON_EXACT_REF",
		},
		{
			name:   "baseline ref versionless",
			mutate: func(r *replay.Request) { r.BaselineSkillRefs[0].Version = 0 },
			code:   "NON_EXACT_REF",
		},
		{
			name:   "candidate ref digest missing",
			mutate: func(r *replay.Request) { r.CandidateRef.BodyDigest = "" },
			code:   "NON_EXACT_REF",
		},
		{
			name: "merge request missing overlap family",
			mutate: func(r *replay.Request) {
				var kept []replay.Ref
				for _, ref := range r.FixtureSetRefs {
					if c.packets[ref.ID].Family != "overlap" {
						kept = append(kept, ref)
					}
				}
				r.FixtureSetRefs = kept
			},
			code: "MISSING_FAMILY",
		},
		{
			name:   "merge request with one source head",
			mutate: func(r *replay.Request) { r.RequiredSourceHeads = r.RequiredSourceHeads[:1] },
			code:   "REPLAY_REQUEST_INVALID",
		},
		{
			name: "merge request with three source heads",
			mutate: func(r *replay.Request) {
				r.RequiredSourceHeads = append(r.RequiredSourceHeads, r.RequiredSourceHeads[0])
			},
			code: "REPLAY_REQUEST_INVALID",
		},
		{
			name: "profile unavailable",
			mutate: func(r *replay.Request) {
				r.ReplayProfileRef = replay.Ref{ID: "rsih.missing-profile", Version: 1,
					Digest: "sha256:" + strings.Repeat("1", 64)}
			},
			code: "SCOPE_PROFILE_INVALID",
		},
		{
			name: "runtime adapter not the profile adapter",
			mutate: func(r *replay.Request) {
				r.RuntimeAdapterRef = replay.Ref{ID: "rsih.other-adapter", Version: 2,
					Digest: "sha256:" + strings.Repeat("2", 64)}
			},
			code: "SCOPE_PROFILE_INVALID",
		},
		{
			name: "permission above host cap",
			mutate: func(r *replay.Request) {
				r.Permissions = map[string]int64{"memory_explore": 99}
			},
			code: "PERMISSION_CAP_EXCEEDED",
		},
		{
			name: "permission not granted by profile",
			mutate: func(r *replay.Request) {
				r.Permissions = map[string]int64{"root_shell": 1}
			},
			code: "PERMISSION_CAP_EXCEEDED",
		},
		{
			name:   "idempotency key malformed",
			mutate: func(r *replay.Request) { r.IdempotencyKey = "not-a-digest" },
			code:   "REPLAY_REQUEST_INVALID",
		},
	}
	for _, tc := range cases {
		r := cloneRequest(base)
		tc.mutate(&r)
		res := s.Schedule(ctx, &r)
		if res.Accepted {
			t.Fatalf("%s: accepted a request that must fail closed", tc.name)
		}
		if res.ReasonCode != tc.code {
			t.Fatalf("%s: reason = %s (%s), want %s", tc.name, res.ReasonCode, res.Detail, tc.code)
		}
	}

	// Idempotency conflict: same key, different frozen input.
	ok := base
	if res := s.Schedule(ctx, &ok); !res.Accepted {
		t.Fatalf("baseline request rejected: %s", reasonOf(res))
	}
	conflict := base
	conflict.BaselineSkillRefs[0].ArtifactDigest = flipHex(conflict.BaselineSkillRefs[0].ArtifactDigest)
	res := s.Schedule(ctx, &conflict)
	if res.Accepted || res.ReasonCode != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("same key with different input must be an idempotency conflict, got %s", res.ReasonCode)
	}
}

func TestSchedulerNondeterministicAndLateRewriteFailClosed(t *testing.T) {
	ctx := context.Background()
	c := loadRecordedCorpus(t)
	bundle := loadBundle(t)
	req := buildMergeRequest(t, c)

	flakyRunner := newFakeRunner(c)
	flakyRunner.scripts["overlap/"+replay.SideBaseline] = &script{kind: "flaky"}
	res := newScheduler(t, c, flakyRunner, bundle).Schedule(ctx, &req)
	if res.Accepted || res.ReasonCode != "REPLAY_NONDETERMINISTIC" {
		t.Fatalf("nondeterministic runner must fail closed with REPLAY_NONDETERMINISTIC, got %s", res.ReasonCode)
	}

	lateRunner := newFakeRunner(c)
	lateRunner.scripts["conflict/"+replay.SideBaseline] = &script{kind: "late-rewrite"}
	res2 := newScheduler(t, c, lateRunner, bundle).Schedule(ctx, &req)
	if res2.Accepted || res2.ReasonCode != "LATE_OUTPUT_REWRITE" {
		t.Fatalf("late output trying to rewrite terminal must fail closed with LATE_OUTPUT_REWRITE, got %s", res2.ReasonCode)
	}
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

// cloneRequest deep-copies the slice/map fields so table-driven mutators
// cannot leak into later cases through the shared backing arrays.
func cloneRequest(r replay.Request) replay.Request {
	out := r
	out.BaselineSkillRefs = append([]replay.SkillRef(nil), r.BaselineSkillRefs...)
	out.FixtureSetRefs = append([]replay.Ref(nil), r.FixtureSetRefs...)
	out.SegmentRefs = append([]replay.SegmentRefInput(nil), r.SegmentRefs...)
	out.RequiredSourceHeads = append([]replay.SkillRef(nil), r.RequiredSourceHeads...)
	perms := make(map[string]int64, len(r.Permissions))
	for k, v := range r.Permissions {
		perms[k] = v
	}
	out.Permissions = perms
	return out
}

func familyPlanOf(p *replay.Plan, family string) replay.FamilyPlan {
	for _, f := range p.Families {
		if f.Family == family {
			return f
		}
	}
	return replay.FamilyPlan{}
}

func join(xs []string) string { return strings.Join(xs, ",") }

func join64(xs []int64) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.FormatInt(x, 10)
	}
	return strings.Join(parts, ",")
}

func flipHex(digest string) string {
	b := []byte(digest)
	if b[len(b)-1] != '0' {
		b[len(b)-1] = '0'
	} else {
		b[len(b)-1] = '1'
	}
	return string(b)
}

func reasonOf(r replay.ScheduleResult) string {
	return r.ReasonCode + " " + r.Detail
}
