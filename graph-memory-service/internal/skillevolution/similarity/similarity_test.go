// Package similarity tests: MT1 canonical pair, versioned bands and the
// SimilarityAssessment record, driven by the $FIX/merge conformance corpus.
package similarity

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type stubEvidence struct {
	mu      sync.Mutex
	records map[string]contract.EvidenceRef
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

// stubHeads serves the active head of each lineage (nil = none).
type stubHeads struct {
	mu     sync.Mutex
	active map[string]*contract.SkillArtifactRef
}

func (h *stubHeads) set(ref contract.SkillArtifactRef) {
	h.mu.Lock()
	defer h.mu.Unlock()
	active := ref
	h.active[ref.LineageID] = &active
}

func (h *stubHeads) drop(lineageID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.active, lineageID)
}

func (h *stubHeads) ActiveRevision(_ context.Context, lineageID string) (*contract.SkillArtifactRef, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active[lineageID], nil
}

type simHarness struct {
	t        *testing.T
	svc      *Service
	gates    *validation.Gates
	evidence *stubEvidence
	heads    *stubHeads
}

func newSimHarness(t *testing.T) *simHarness {
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
	evidence := &stubEvidence{records: map[string]contract.EvidenceRef{}}
	heads := &stubHeads{active: map[string]*contract.SkillArtifactRef{}}
	svc, err := NewService(Config{
		Gates: gates, Store: store, Tx: mgr, Registry: registry,
		Evidence: evidence, Heads: heads,
	})
	if err != nil {
		t.Fatalf("similarity.NewService: %v", err)
	}
	return &simHarness{t: t, svc: svc, gates: gates, evidence: evidence, heads: heads}
}

// releasedRef builds one §7.3-shaped ref and marks it active.
func (h *simHarness) releasedRef(lineage, version, digest string) contract.SkillArtifactRef {
	h.t.Helper()
	ref := contract.SkillArtifactRef{
		SchemaVersion:  "gms.skill-artifact-ref.v1",
		LineageID:      lineage,
		Version:        version,
		Kind:           KindStepGuidance,
		ArtifactDigest: digest,
	}
	h.heads.set(ref)
	return ref
}

func jn(v int64) any { return json.Number(strconv.FormatInt(v, 10)) }

func mustInt(t *testing.T, raw any) int64 {
	t.Helper()
	if !contract.IsIntegerNumber(raw) {
		t.Fatalf("expected an integer, got %T", raw)
	}
	value, err := strconv.ParseInt(string(raw.(json.Number)), 10, 64)
	if err != nil {
		t.Fatalf("integer parse: %v", err)
	}
	return value
}

func fixtureJSON(t *testing.T, parts ...string) []byte {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(append([]string{dir}, parts...)...))
	if err != nil {
		t.Fatalf("read fixture %v: %v", parts, err)
	}
	return data
}

func fixtureValue(t *testing.T, parts ...string) map[string]any {
	t.Helper()
	value, err := contract.ParseJSONStrict(fixtureJSON(t, parts...))
	if err != nil {
		t.Fatalf("parse fixture %v: %v", parts, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		t.Fatalf("fixture %v is not a JSON object", parts)
	}
	return obj
}

// ---------------------------------------------------------------------------
// Fixture-driven conformance matrix ($FIX/merge similarity assessments)
// ---------------------------------------------------------------------------

// TestSimilarityFixtureMatrix drives Normalize over the $FIX/merge corpus:
// merge-001 (A+B) and merge-002 (B+A) must normalize to BYTE-IDENTICAL
// canonical documents (same digest, same canonical order sg-alpha < sg-beta,
// Contract §16.4 #11), and the below-threshold negative must parse as a
// record while its band blocks proposal creation.
func TestSimilarityFixtureMatrix(t *testing.T) {
	h := newSimHarness(t)

	ab, err := h.svc.Normalize(fixtureValue(t, "merge", "merge-001-similarity-ab", "source.json"))
	if err != nil {
		t.Fatalf("normalize merge-001: %v (code %s)", err, CodeOf(err))
	}
	ba, err := h.svc.Normalize(fixtureValue(t, "merge", "merge-002-similarity-ba", "source.json"))
	if err != nil {
		t.Fatalf("normalize merge-002: %v (code %s)", err, CodeOf(err))
	}
	if string(ab.CanonicalBytes()) != string(ba.CanonicalBytes()) {
		t.Fatalf("A+B and B+A canonical bytes differ (canonical-pair invariant, MT1)")
	}
	if ab.Digest() != ba.Digest() || ab.PairKey() != ba.PairKey() {
		t.Fatalf("A+B / B+A digest or pair key differ: %s vs %s", ab.Digest(), ba.Digest())
	}
	// The frozen corpus expectation (expected.json + canonical.utf8).
	expected := fixtureValue(t, "merge", "merge-001-similarity-ab", "expected.json")
	if want, _ := contract.AsString(expected["expected_digest"]); want != ab.Digest() {
		t.Fatalf("merge-001 digest %s != corpus expectation %s", ab.Digest(), want)
	}
	if wantLen, ok := expected["expected_canonical_byte_length"]; !ok || !contract.IsIntegerNumber(wantLen) || int64(len(ab.CanonicalBytes())) != mustInt(t, wantLen) {
		t.Fatalf("merge-001 canonical length %d != corpus expectation %v", len(ab.CanonicalBytes()), expected["expected_canonical_byte_length"])
	}
	normalizedFields, _ := contract.AsObject(expected["expected_normalized_fields"])
	order, _ := contract.AsArray(normalizedFields["source_skill_refs_canonical_order"])
	if len(order) != 2 {
		t.Fatalf("corpus expects 2 canonical sources, got %v", order)
	}
	if ab.Sources()[0].LineageID != "sg-alpha" || ab.Sources()[1].LineageID != "sg-beta" {
		t.Fatalf("canonical order = [%s, %s], want [sg-alpha, sg-beta]", ab.Sources()[0].LineageID, ab.Sources()[1].LineageID)
	}
	if ab.Sources()[0].Version != "2" || ab.Sources()[1].Version != "1" {
		t.Fatalf("canonical versions = [%s, %s], want [2, 1] (§6.3 byte order)", ab.Sources()[0].Version, ab.Sources()[1].Version)
	}
	// canonical.utf8 agreement.
	wantCanonical := fixtureJSON(t, "merge", "merge-001-similarity-ab", "canonical.utf8")
	if string(ab.CanonicalBytes()) != string(wantCanonical) {
		t.Fatalf("merge-001 canonical bytes disagree with canonical.utf8")
	}
	// The declared band agrees with the versioned policy (920000 →
	// merge_review under DefaultPolicyV1).
	if band := h.svc.Policy().Band(ab.ScoreMicros()); band != ab.Band() || band != BandMergeReview {
		t.Fatalf("band derivation: policy says %q for %d, record says %q", band, ab.ScoreMicros(), ab.Band())
	}

	// The below-threshold negative parses as a RECORD but its band can never
	// create a proposal (Contract §15.2 MT1).
	neg, err := h.svc.Normalize(fixtureValue(t, "merge", "merge-neg-001-below-threshold", "source.json"))
	if err != nil {
		t.Fatalf("normalize merge-neg-001: %v (code %s)", err, CodeOf(err))
	}
	negExpected := fixtureValue(t, "merge", "merge-neg-001-below-threshold", "expected.json")
	if want, _ := contract.AsString(negExpected["expected_reason_code"]); want != ReasonSimilarityBelowThreshold {
		t.Fatalf("corpus reason drift: %s", want)
	}
	if h.svc.Policy().MayPropose(neg.Band()) {
		t.Fatalf("band %q (score %d) must never propose", neg.Band(), neg.ScoreMicros())
	}
	if h.svc.Policy().MayAutoAdmit(neg.Band()) {
		t.Fatalf("band %q must never auto-admit (graded initiation, M1)", neg.Band())
	}
	if string(neg.CanonicalBytes()) != string(fixtureJSON(t, "merge", "merge-neg-001-below-threshold", "canonical.utf8")) {
		t.Fatalf("merge-neg-001 canonical bytes disagree with canonical.utf8")
	}
}

// ---------------------------------------------------------------------------
// Canonical pair + dedup key symmetry
// ---------------------------------------------------------------------------

func refOf(lineage, version, digest string) contract.SkillArtifactRef {
	return contract.SkillArtifactRef{
		SchemaVersion: "gms.skill-artifact-ref.v1",
		LineageID:     lineage, Version: version, Kind: KindStepGuidance, ArtifactDigest: digest,
	}
}

func TestCanonicalPairOrdersByLineageBytes(t *testing.T) {
	a := refOf("sg-alpha", "2", "sha256:"+repeatHex('a', 64))
	b := refOf("sg-beta", "1", "sha256:"+repeatHex('b', 64))
	forward := CanonicalPair(a, b)
	reverse := CanonicalPair(b, a)
	if forward != reverse || forward[0] != a || forward[1] != b {
		t.Fatalf("canonical pair mismatch: forward %+v reverse %+v", forward, reverse)
	}
	// Pair keys are symmetric.
	kf, err := SourcePairKey(forward)
	if err != nil {
		t.Fatalf("pair key: %v", err)
	}
	kr, err := SourcePairKey(reverse)
	if err != nil {
		t.Fatalf("pair key: %v", err)
	}
	if kf != kr || kf == "" {
		t.Fatalf("pair key asymmetry: %s vs %s", kf, kr)
	}
}

func repeatHex(ch byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = ch
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// Assess fail-closed gates
// ---------------------------------------------------------------------------

func (h *simHarness) assessInput(id string, sources []contract.SkillArtifactRef, score int64) AssessInput {
	support := contract.EvidenceRef{
		SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "ev-sim-" + id, Version: "1",
		EvidenceDigest: contract.DigestBytes([]byte("ev-sim-" + id)),
		CommitState:    "committed", EvidenceKind: "observation",
	}
	h.evidence.commit(support)
	return AssessInput{
		AssessmentID: id, Sources: sources, ScoreMicros: score,
		ApplicabilityOverlap: "partial", PermissionCompatible: true,
		FeatureRecordRefs:      []contract.VersionedRef{{ID: "feature-" + id, Version: "1", Digest: contract.DigestBytes([]byte("feature-" + id))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{support},
		AssessorRef:            contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	}
}

// TestAssessFailClosed pins the MT1 admission gates: n-way and self pairs,
// cross-kind sources, inactive heads, moved heads and uncommitted evidence
// all reject before any record is written.
func TestAssessFailClosed(t *testing.T) {
	h := newSimHarness(t)
	ctx := context.Background()
	a := h.releasedRef("sg-alpha", "2", contract.DigestBytes([]byte("alpha-body")))
	b := h.releasedRef("sg-beta", "1", contract.DigestBytes([]byte("beta-body")))

	// n-way rejects (v1 is binary only).
	if _, err := h.svc.Assess(ctx, h.assessInput("sa-nway", []contract.SkillArtifactRef{a, b, a}, 970_000)); CodeOf(err) != ReasonRefMismatch {
		t.Fatalf("n-way assess: want REF_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}
	// self-merge rejects.
	if _, err := h.svc.Assess(ctx, h.assessInput("sa-self", []contract.SkillArtifactRef{a, a}, 970_000)); CodeOf(err) != ReasonRefMismatch {
		t.Fatalf("self assess: want REF_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}
	// cross-kind rejects (v1 merges step_guidance only).
	procedure := refOf("proc-render", "1", contract.DigestBytes([]byte("proc-body")))
	procedure.Kind = "human_procedure"
	if _, err := h.svc.Assess(ctx, h.assessInput("sa-cross", []contract.SkillArtifactRef{a, procedure}, 970_000)); CodeOf(err) != ReasonSkillKindInvalid {
		t.Fatalf("cross-kind assess: want SKILL_KIND_INVALID, got %v (code %s)", err, CodeOf(err))
	}
	// inactive source rejects.
	h.heads.drop("sg-beta")
	if _, err := h.svc.Assess(ctx, h.assessInput("sa-inactive", []contract.SkillArtifactRef{a, b}, 970_000)); CodeOf(err) != ReasonActiveHeadConflict {
		t.Fatalf("inactive assess: want ACTIVE_HEAD_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	// moved head rejects (only CURRENT active heads may assess).
	moved := b
	moved.ArtifactDigest = contract.DigestBytes([]byte("beta-body-v2"))
	h.heads.set(moved)
	if _, err := h.svc.Assess(ctx, h.assessInput("sa-stale", []contract.SkillArtifactRef{a, b}, 970_000)); CodeOf(err) != ReasonActiveHeadConflict {
		t.Fatalf("stale assess: want ACTIVE_HEAD_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	h.heads.set(b)
	// uncommitted evidence rejects.
	in := h.assessInput("sa-uncommitted", []contract.SkillArtifactRef{a, b}, 970_000)
	in.SupportingEvidenceRefs = []contract.EvidenceRef{{
		SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "ev-never-committed", Version: "1",
		EvidenceDigest: contract.DigestBytes([]byte("never")), CommitState: "committed", EvidenceKind: "observation",
	}}
	if _, err := h.svc.Assess(ctx, in); CodeOf(err) != ReasonEvidenceNotCommitted {
		t.Fatalf("uncommitted evidence assess: want EVIDENCE_NOT_COMMITTED, got %v (code %s)", err, CodeOf(err))
	}
	// Nothing was written to the similarity ledger by the rejects.
	entries, err := h.svc.storeSnapshotFor("sa-nway")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for _, id := range []string{"sa-nway", "sa-self", "sa-cross", "sa-inactive", "sa-stale", "sa-uncommitted"} {
		if got, _ := h.svc.storeSnapshotFor(id); len(got) != 0 {
			t.Fatalf("rejected assessment %s wrote %d ledger entries (fail-closed violated)", id, len(got))
		}
	}
	_ = entries

	// The happy path: A+B and B+A commit ONE shared record identity.
	first, err := h.svc.Assess(ctx, h.assessInput("sa-0001", []contract.SkillArtifactRef{a, b}, 970_000))
	if err != nil {
		t.Fatalf("assess A+B: %v (code %s)", err, CodeOf(err))
	}
	second, err := h.svc.Assess(ctx, h.assessInput("sa-0002", []contract.SkillArtifactRef{b, a}, 970_000))
	if err != nil {
		t.Fatalf("assess B+A: %v (code %s)", err, CodeOf(err))
	}
	if first.PairKey() != second.PairKey() {
		t.Fatalf("pair keys differ between A+B and B+A live assessments")
	}
	if first.Digest() == second.Digest() {
		t.Fatalf("two different assessment ids must not share a whole-document digest (id is in the hashed core)")
	}
	if second.Sources() != first.Sources() {
		t.Fatalf("live assessments disagree on canonical order")
	}
	if band := second.Band(); band != BandAutoMergeEligible || !h.svc.Policy().MayAutoAdmit(band) {
		t.Fatalf("970000 must be auto_merge_eligible, got %q", band)
	}
	// Resolve reads the committed record with exact digest enforcement.
	resolved, err := h.svc.Resolve(ctx, "sa-0001", first.Digest())
	if err != nil || resolved.PairKey() != first.PairKey() {
		t.Fatalf("resolve sa-0001: %v", err)
	}
	if _, err := h.svc.Resolve(ctx, "sa-0001", second.Digest()); CodeOf(err) != ReasonRefMismatch {
		t.Fatalf("resolve with a foreign digest: want REF_MISMATCH, got %v (code %s)", err, CodeOf(err))
	}
}

// Resolve needs the store; expose a tiny test shim.
func (s *Service) storeSnapshotFor(id string) ([]ledger.Entry, error) {
	return s.store.Snapshot(ledger.LedgerSimilarity, id)
}

// ensure the stubs satisfy the ports.
var (
	_ EvidenceResolver   = (*stubEvidence)(nil)
	_ ActiveHeadResolver = (*stubHeads)(nil)
)
