// Package projector tests: projection behavior (plan-pinned TDD; the two
// Red tests first, then the fail-closed and rebuild coverage).
package projector

import (
	"context"
	"errors"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
)

// ---------------------------------------------------------------------------
// GMS-205 TDD Red #1 (plan-pinned): one projection batch commits the Graph
// mutation, the cursor vector, the head CAS and the watermark ATOMICALLY
// (GMS §8.5); a mid-transaction failure leaves none of them behind.
// ---------------------------------------------------------------------------

func TestProjectorCommitsGraphAndWatermarkAtomically(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)

	result, err := h.svc.ProjectRuntime(ctx)
	if err != nil {
		t.Fatalf("first projection: %v (code %s)", err, CodeOf(err))
	}
	snap := h.svc.Snapshot()
	if len(snap.Revisions) != 1 {
		t.Fatalf("first projection wrote %d revision nodes, want 1", len(snap.Revisions))
	}
	node, ok := snap.Revisions[RevisionRef(a).String()]
	if !ok {
		t.Fatalf("first projection missed the node of %s", a.LineageID)
	}
	if node.Lifecycle != LifecycleActive || node.FirstProjectedSequence != 1 {
		t.Fatalf("node metadata = %+v", node)
	}
	if result.Cursors.Activation != 1 {
		t.Fatalf("activation cursor = %d, want 1", result.Cursors.Activation)
	}
	if result.State != StateCurrent {
		t.Fatalf("state = %s, want current (ledger fully drained)", result.State)
	}
	// The watermark is a valid Contract §7.14 instance (x-digest included).
	if err := h.gates.ValidateInstance(result.Watermark, projectionWatermarkSchemaFile); err != nil {
		t.Fatalf("watermark failed the §7.14 authority schema: %v", err)
	}
	if got, _ := contract.AsString(result.Watermark["state"]); got != StateCurrent {
		t.Fatalf("watermark state = %s", got)
	}
	if got, _ := contract.AsString(result.Watermark["projection_stream"]); got != StreamRuntime {
		t.Fatalf("watermark stream = %s", got)
	}
	// The head CAS points at the watermark digest.
	headSeq, headDigest, ok, err := h.store.GetHead(ledger.HeadProjection, StreamRuntime)
	if err != nil || !ok {
		t.Fatalf("projection head missing (ok=%v err=%v)", ok, err)
	}
	if headDigest != result.WatermarkDigest || headSeq != 1 {
		t.Fatalf("projection head = (%d, %s), want (1, %s)", headSeq, headDigest, result.WatermarkDigest)
	}
	// The transactional outbox drained: the delivery of seq 1 completed.
	pending, err := h.store.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("first projection left %d outbox records pending", len(pending))
	}
	beforeSnapshot := h.svc.Snapshot()
	beforeDigest := h.svc.GraphDigest()
	beforeWatermark, beforeWatermarkDigest, _ := h.svc.Watermark()
	_ = beforeWatermark

	// Mid-transaction failure after the unit is staged but before the head
	// CAS (the irreversible commit point): the unit must leave NO trace —
	// no head advance, no mutation, no cursor, no watermark — and the
	// outbox record stays pending.
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	b := h.releaseSkill(t, "sg-beta", bodyB)
	h.storage.testApplyHook = func() error { return errors.New("simulated crash between CAS and swap") }
	_, err = h.svc.ProjectRuntime(ctx)
	if err == nil {
		t.Fatal("mid-transaction failure must surface")
	}
	afterSnapshot := h.svc.Snapshot()
	if len(afterSnapshot.Revisions) != len(beforeSnapshot.Revisions) {
		t.Fatalf("failed unit mutated the Graph: %d nodes, want %d", len(afterSnapshot.Revisions), len(beforeSnapshot.Revisions))
	}
	if _, leaked := afterSnapshot.Revisions[RevisionRef(b).String()]; leaked {
		t.Fatalf("failed unit leaked the node of %s", b.LineageID)
	}
	if afterSnapshot.Cursors != beforeSnapshot.Cursors {
		t.Fatalf("failed unit advanced the cursors: %+v", afterSnapshot.Cursors)
	}
	watermark, watermarkDigest, state := h.svc.Watermark()
	if watermarkDigest != beforeWatermarkDigest || state != StateCurrent {
		t.Fatalf("failed unit advanced the watermark (%s != %s, state %s)", watermarkDigest, beforeWatermarkDigest, state)
	}
	if got := numberString(t, watermark["projected_through_activation_sequence"]); got != "1" {
		t.Fatalf("watermark ran ahead: projected_through = %s", got)
	}
	headSeq2, headDigest2, _, _ := h.store.GetHead(ledger.HeadProjection, StreamRuntime)
	if headSeq2 != headSeq || headDigest2 != headDigest {
		t.Fatalf("failed unit left the head at (%d, %s), want (%d, %s)", headSeq2, headDigest2, headSeq, headDigest)
	}
	pending, err = h.store.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].ActivationSequence != 2 {
		t.Fatalf("failed unit drained the outbox: %+v", pending)
	}
	if h.svc.GraphDigest() != beforeDigest {
		t.Fatalf("failed unit changed the derived content digest")
	}

	// Retry after the crash: the same batch commits cleanly.
	h.storage.testApplyHook = nil
	result, err = h.svc.ProjectRuntime(ctx)
	if err != nil {
		t.Fatalf("retry projection: %v (code %s)", err, CodeOf(err))
	}
	if result.Cursors.Activation != 2 || result.Projected != 1 {
		t.Fatalf("retry result = %+v", result)
	}
	snap = h.svc.Snapshot()
	if len(snap.Revisions) != 2 {
		t.Fatalf("retry wrote %d revision nodes, want 2", len(snap.Revisions))
	}
	pending, err = h.store.Pending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("retry left outbox residue: %+v (err %v)", pending, err)
	}
}

// ---------------------------------------------------------------------------
// GMS-205 TDD Red #2 (plan-pinned): a sequence gap blocks the projection
// (state-only watermark, no Graph mutation) and nothing from the gapped
// batch — no revision, no branch, no candidate endpoint — leaks into the
// Runtime Graph (Contract §9.3, §13.6, GMS §8.3).
// ---------------------------------------------------------------------------

func TestProjectionGapBlocksWithoutCandidateLeakage(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	h.releaseSkill(t, "sg-alpha", bodyA)
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	h.releaseSkill(t, "sg-beta", bodyB)
	_, bodyC := h.guidanceEnvelope("sg-gamma", []string{"gamma-path"}, "archive")
	h.releaseSkill(t, "sg-gamma", bodyC)
	_, bodyD := h.guidanceEnvelope("sg-delta", []string{"delta-path"}, "research")
	h.releaseSkill(t, "sg-delta", bodyD)

	// The event-neg-001 shape: sequences 1, 2, 4 — the gap at 3 blocks.
	gapped := append(h.deliveredEvents(t, 1, 2), h.deliveredEvents(t, 4, 4)...)
	if len(gapped) != 3 {
		t.Fatalf("gapped batch = %d events, want 3", len(gapped))
	}
	result, err := h.svc.ProjectDelivered(ctx, gapped)
	if CodeOf(err) != ReasonProjectionSequenceGap {
		t.Fatalf("gap batch: want PROJECTION_SEQUENCE_GAP, got %v (code %s)", err, CodeOf(err))
	}
	// Corpus pin: the frozen negative case freezes the same reason code.
	negExpected := fixtureValue(t, "events", "event-neg-001-projection-gap", "expected.json")
	if want := fixtureString(t, negExpected, "expected_reason_code"); want != ReasonProjectionSequenceGap {
		t.Fatalf("corpus reason drift: %s", want)
	}
	snap := h.svc.Snapshot()
	if snap.State != StateBlocked {
		t.Fatalf("state = %s, want blocked", snap.State)
	}
	if len(snap.Revisions) != 0 || len(snap.Branches) != 0 || len(snap.Edges) != 0 || len(snap.Vertices) != 0 {
		t.Fatalf("blocked batch leaked Graph content: %+v", snap)
	}
	if snap.Cursors.Activation != 0 {
		t.Fatalf("blocked batch advanced the cursor: %+v", snap.Cursors)
	}
	watermark, _, state := h.svc.Watermark()
	if state != StateBlocked {
		t.Fatalf("watermark state = %s, want blocked", state)
	}
	if got := numberString(t, watermark["projected_through_activation_sequence"]); got != "0" {
		t.Fatalf("blocked watermark ran ahead: %s", got)
	}
	if err := h.gates.ValidateInstance(watermark, projectionWatermarkSchemaFile); err != nil {
		t.Fatalf("blocked watermark failed the §7.14 authority schema: %v", err)
	}
	if blocked := h.svc.Blocked(); blocked == nil || blocked.Code != ReasonProjectionSequenceGap {
		t.Fatalf("blocked info = %+v", blocked)
	}
	if result != nil && result.Projected != 0 {
		t.Fatalf("blocked result projected %d events", result.Projected)
	}

	// The candidate of the unprojected release is nowhere in the Graph.
	for key := range h.svc.Snapshot().Revisions {
		if strings.Contains(key, "cand-") {
			t.Fatalf("candidate endpoint leaked into the Runtime Graph: %s", key)
		}
	}

	// Repair: the contiguous prefix commits, and the stream unblocks.
	if _, err := h.svc.ProjectDelivered(ctx, h.deliveredEvents(t, 1, 3)); err != nil {
		t.Fatalf("repaired prefix: %v (code %s)", err, CodeOf(err))
	}
	snap = h.svc.Snapshot()
	if snap.State == StateBlocked || len(snap.Revisions) != 3 {
		t.Fatalf("after repair: state %s, %d nodes", snap.State, len(snap.Revisions))
	}
	if blocked := h.svc.Blocked(); blocked != nil {
		t.Fatalf("blocked info survived the repair: %+v", blocked)
	}
	if _, err := h.svc.ProjectDelivered(ctx, h.deliveredEvents(t, 4, 4)); err != nil {
		t.Fatalf("tail event: %v (code %s)", err, CodeOf(err))
	}
	if _, _, state := h.svc.Watermark(); state != StateCurrent {
		t.Fatalf("state = %s, want current", state)
	}
}

// ---------------------------------------------------------------------------
// Conformance-fixture alignment ($FIX/events)
// ---------------------------------------------------------------------------

// TestDuplicateDeliveryIsIdempotent pins Contract §16.4 #10 / the
// event-003 corpus note: re-delivering the identical event (same sequence,
// same digest) is an idempotent no-op — no new mutation, no watermark
// change, no outbox churn.
func TestDuplicateDeliveryIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	h.releaseSkill(t, "sg-alpha", bodyA)
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	h.releaseSkill(t, "sg-beta", bodyB)

	if _, err := h.svc.ProjectDelivered(ctx, h.deliveredEvents(t, 1, 2)); err != nil {
		t.Fatalf("first delivery: %v (code %s)", err, CodeOf(err))
	}
	digest := h.svc.GraphDigest()
	_, watermarkDigest, _ := h.svc.Watermark()
	result, err := h.svc.ProjectDelivered(ctx, h.deliveredEvents(t, 2, 2))
	if err != nil {
		t.Fatalf("duplicate delivery must be accepted: %v (code %s)", err, CodeOf(err))
	}
	if result.Projected != 0 {
		t.Fatalf("duplicate delivery projected %d new events", result.Projected)
	}
	if h.svc.GraphDigest() != digest {
		t.Fatalf("duplicate delivery mutated the Graph")
	}
	if _, afterDigest, _ := h.svc.Watermark(); afterDigest != watermarkDigest {
		t.Fatalf("duplicate delivery changed the watermark")
	}
	// Corpus pin: event-003 freezes accept=true for the duplicate batch.
	expected := fixtureValue(t, "events", "event-003-projection-idempotency", "expected.json")
	if accept, _ := expected["expected_accept"].(bool); !accept {
		t.Fatalf("event-003 must freeze expected_accept=true")
	}
}

// TestSameSequenceDifferentDigestBlocks pins the event-neg-002 corpus shape
// (Contract §9.3): sequence 5 delivered twice with different event digests
// is PROJECTION_EVENT_CONFLICT — blocked, nothing staged.
func TestSameSequenceDifferentDigestBlocks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for _, lineage := range []string{"sg-a1", "sg-a2", "sg-a3", "sg-a4", "sg-a5"} {
		_, body := h.guidanceEnvelope(lineage, []string{lineage + "-path"}, "research")
		h.releaseSkill(t, lineage, body)
	}
	if _, err := h.svc.ProjectDelivered(ctx, h.deliveredEvents(t, 1, 4)); err != nil {
		t.Fatalf("prefix delivery: %v (code %s)", err, CodeOf(err))
	}
	digest := h.svc.GraphDigest()

	fifth := h.deliveredEvents(t, 5, 5)[0]
	tampered := fifth
	tampered.PayloadDigest = contract.DigestBytes([]byte("tampered:" + fifth.PayloadDigest))
	_, err := h.svc.ProjectDelivered(ctx, []DeliveredEvent{fifth, tampered})
	if CodeOf(err) != ReasonProjectionEventConflict {
		t.Fatalf("same-slot different digest: want PROJECTION_EVENT_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	negExpected := fixtureValue(t, "events", "event-neg-002-projection-conflict", "expected.json")
	if want := fixtureString(t, negExpected, "expected_reason_code"); want != ReasonProjectionEventConflict {
		t.Fatalf("corpus reason drift: %s", want)
	}
	if _, _, state := h.svc.Watermark(); state != StateBlocked {
		t.Fatalf("state = %s, want blocked", state)
	}
	if h.svc.GraphDigest() != digest {
		t.Fatalf("conflicting batch mutated the Graph")
	}
	if snap := h.svc.Snapshot(); snap.Cursors.Activation != 4 || len(snap.Revisions) != 4 {
		t.Fatalf("conflicting batch advanced state: %+v", snap)
	}
}

// TestWatermarkMatchesFrozenProjectionSchema pins the event-004 corpus:
// the committed watermark is a schema-valid gms.projection-watermark.v1
// instance whose projection_schema_version equals the frozen corpus value.
func TestWatermarkMatchesFrozenProjectionSchema(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	h.releaseSkill(t, "sg-alpha", bodyA)
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}
	watermark, _, _ := h.svc.Watermark()
	if err := h.gates.ValidateInstance(watermark, projectionWatermarkSchemaFile); err != nil {
		t.Fatalf("watermark failed the authority schema (x-digest recompute): %v", err)
	}
	fixture := fixtureValue(t, "events", "event-004-projection-watermark", "source.json")
	want := fixtureString(t, fixture, "projection_schema_version")
	if want != ProjectionSchemaVersion {
		t.Fatalf("projection schema drift: corpus %s, code %s", want, ProjectionSchemaVersion)
	}
	if got := fixtureString(t, watermark, "projection_schema_version"); got != want {
		t.Fatalf("watermark projection_schema_version = %s, want %s", got, want)
	}
	if got, _ := contract.AsString(watermark["schema_version"]); got != "gms.projection-watermark.v1" {
		t.Fatalf("watermark schema_version = %s", got)
	}
}

// TestUnledgeredDeliveryBlocks pins the fail-closed list entry "watermark
// running ahead": a delivered event that does not resolve to the committed
// activation ledger entry at its sequence blocks with
// ACTIVATION_EVENT_INVALID and nothing is staged.
func TestUnledgeredDeliveryBlocks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	h.releaseSkill(t, "sg-alpha", bodyA)
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}
	digest := h.svc.GraphDigest()
	ghost := h.deliveredEvents(t, 1, 1)[0]
	ghost.Sequence = 99
	_, err := h.svc.ProjectDelivered(ctx, []DeliveredEvent{ghost})
	if CodeOf(err) != ledger.ReasonActivationEventInvalid {
		t.Fatalf("unledgered event: want ACTIVATION_EVENT_INVALID, got %v (code %s)", err, CodeOf(err))
	}
	if _, _, state := h.svc.Watermark(); state != StateBlocked {
		t.Fatalf("state = %s, want blocked", state)
	}
	if h.svc.GraphDigest() != digest {
		t.Fatalf("unledgered delivery mutated the Graph")
	}
}

// ---------------------------------------------------------------------------
// Relation projection (similar_to, supported_by/refuted_by) and fail-closed
// ---------------------------------------------------------------------------

// TestSimilarityAssessmentProjectsCanonicalEdge pins GMS §12.9: one
// committed §7.21 assessment projects exactly one canonical symmetric
// similar_to edge whose source keeps the exact assessment record (id +
// digest) for audit; even a below-threshold record projects.
func TestSimilarityAssessmentProjectsCanonicalEdge(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	b := h.releaseSkill(t, "sg-beta", bodyB)

	assessment, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID:         "sa-low",
		Sources:              []contract.SkillArtifactRef{b, a}, // deliberately reversed
		ScoreMicros:          120_000,                           // below_suggestion band
		ApplicabilityOverlap: "partial",
		PermissionCompatible: true,
		FeatureRecordRefs:    []contract.VersionedRef{{ID: "feature-low", Version: "1", Digest: contract.DigestBytes([]byte("feature-low"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{
			h.committedEvidence("ev-low-a", "observation"), h.committedEvidence("ev-low-b", "observation"),
		},
		AssessorRef: contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	})
	if err != nil {
		t.Fatalf("assess: %v (code %s)", err, similarity.CodeOf(err))
	}
	h.feedSimilarityAssessment(t, "sa-low")

	result, err := h.svc.ProjectRuntime(ctx)
	if err != nil {
		t.Fatalf("projection with similarity source: %v (code %s)", err, CodeOf(err))
	}
	if result.Cursors.Similarity != 1 {
		t.Fatalf("similarity cursor = %d, want 1", result.Cursors.Similarity)
	}
	snap := h.svc.Snapshot()
	if edgeCount(snap, RelSimilarTo) != 1 {
		t.Fatalf("similar_to edges = %d, want exactly 1 (canonical symmetric)", edgeCount(snap, RelSimilarTo))
	}
	canonical := similarity.CanonicalPair(a, b)
	var edge Edge
	for _, e := range snap.Edges {
		if e.Relation == RelSimilarTo {
			edge = e
		}
	}
	if edge.From != RevisionRef(canonical[0]) || edge.To != RevisionRef(canonical[1]) {
		t.Fatalf("similar_to endpoints not canonical: %s -> %s", edge.From, edge.To)
	}
	if edge.Source.Source != SourceSimilarity || edge.Source.RecordID != "sa-low" ||
		edge.Source.Digest != assessment.Digest() || edge.Source.Sequence != 1 {
		t.Fatalf("similar_to edge lost its audit source: %+v", edge.Source)
	}
}

// TestClaimAssessmentProjectsTypedEvidenceEdges pins GMS §9.5/§9.6: a
// supports assessment projects branch --supported_by--> evidence-ref with
// the exact record kept for audit; a refutes assessment projects
// refuted_by and KEEPS the branch; an assessment over uncommitted evidence
// blocks with EVIDENCE_NOT_COMMITTED.
func TestClaimAssessmentProjectsTypedEvidenceEdges(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}
	support := h.committedEvidence("ev-support-0001", "observation")
	supporting, _ := claimAssessment("ca-support-0001", a, "collect-context", "supports", support)
	supporting.Sequence = 1
	h.assessSource.add(supporting)

	result, err := h.svc.ProjectRuntime(ctx)
	if err != nil {
		t.Fatalf("projection with assessment source: %v (code %s)", err, CodeOf(err))
	}
	if result.Cursors.EvidenceAssessment != 1 {
		t.Fatalf("assessment cursor = %d, want 1", result.Cursors.EvidenceAssessment)
	}
	snap := h.svc.Snapshot()
	if edgeCount(snap, RelSupportedBy) != 1 {
		t.Fatalf("supported_by edges = %d, want 1", edgeCount(snap, RelSupportedBy))
	}
	if len(snap.Vertices) != 1 {
		t.Fatalf("typed evidence vertices = %d, want 1", len(snap.Vertices))
	}
	for _, edge := range snap.Edges {
		if edge.Relation == RelSupportedBy {
			if edge.From != BranchRef(a, "collect-context") {
				t.Fatalf("supported_by from %s, want branch %s", edge.From, "collect-context")
			}
			if edge.To != EvidenceVertexRef(support.EvidenceID, support.EvidenceDigest) {
				t.Fatalf("supported_by to %s", edge.To)
			}
			if edge.Source.RecordID != "ca-support-0001" || edge.Source.Sequence != 1 {
				t.Fatalf("supported_by lost its audit source: %+v", edge.Source)
			}
		}
	}

	// A refutation keeps the branch and projects refuted_by.
	refute := h.committedEvidence("ev-refute-0001", "failure_path")
	refuting, _ := claimAssessment("ca-refute-0001", a, "collect-context", "refutes", refute)
	refuting.Sequence = 2
	h.assessSource.add(refuting)
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("refuting assessment: %v (code %s)", err, CodeOf(err))
	}
	snap = h.svc.Snapshot()
	if edgeCount(snap, RelRefutedBy) != 1 || edgeCount(snap, RelSupportedBy) != 1 {
		t.Fatalf("refutation rewrote history: supported=%d refuted=%d",
			edgeCount(snap, RelSupportedBy), edgeCount(snap, RelRefutedBy))
	}
	if len(snap.Branches) != 1 || len(snap.Revisions) != 1 {
		t.Fatalf("refutation deleted the branch or revision: %+v", snap)
	}

	// Uncommitted evidence blocks the whole projection fail closed.
	digest := h.svc.GraphDigest()
	ghost := contract.EvidenceRef{
		SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "ev-ghost-0009", Version: "1",
		EvidenceDigest: contract.DigestBytes([]byte("ghost")), CommitState: "committed", EvidenceKind: "observation",
	}
	ghostRecord, _ := claimAssessment("ca-ghost-0001", a, "collect-context", "supports", ghost)
	ghostRecord.Sequence = 3
	h.assessSource.add(ghostRecord)
	_, err = h.svc.ProjectRuntime(ctx)
	if CodeOf(err) != ReasonEvidenceNotCommitted {
		t.Fatalf("uncommitted evidence: want EVIDENCE_NOT_COMMITTED, got %v (code %s)", err, CodeOf(err))
	}
	if _, _, state := h.svc.Watermark(); state != StateBlocked {
		t.Fatalf("state = %s, want blocked", state)
	}
	if h.svc.GraphDigest() != digest {
		t.Fatalf("uncommitted-evidence batch mutated the Graph")
	}
}

// TestRelationSourceGapBlocks pins the relation-source cursor discipline:
// a similarity record whose sequence skips the cursor blocks with
// PROJECTION_SOURCE_GAP (GMS §8.3 applied to every source of the vector).
func TestRelationSourceGapBlocks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	b := h.releaseSkill(t, "sg-beta", bodyB)
	_, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID: "sa-gap", Sources: []contract.SkillArtifactRef{a, b}, ScoreMicros: 900_000,
		ApplicabilityOverlap: "partial", PermissionCompatible: true,
		FeatureRecordRefs:      []contract.VersionedRef{{ID: "feature-gap", Version: "1", Digest: contract.DigestBytes([]byte("feature-gap"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{h.committedEvidence("ev-gap", "observation")},
		AssessorRef:            contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	})
	if err != nil {
		t.Fatalf("assess: %v", err)
	}
	h.feedSimilarityAssessment(t, "sa-gap")
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}
	digest := h.svc.GraphDigest()
	// Hand-craft a gap: the next similarity record claims sequence 3,
	// skipping 2 (contiguity is checked before the record is even parsed).
	h.simSource.add(SourceRecord{Sequence: 3, RecordID: "sa-gap-x", Digest: contract.DigestBytes([]byte("x")), Canonical: []byte(`{}`)})
	_, err = h.svc.ProjectRuntime(ctx)
	if CodeOf(err) != ReasonProjectionSourceGap {
		t.Fatalf("similarity source gap: want PROJECTION_SOURCE_GAP, got %v (code %s)", err, CodeOf(err))
	}
	if _, _, state := h.svc.Watermark(); state != StateBlocked {
		t.Fatalf("state = %s, want blocked", state)
	}
	if h.svc.GraphDigest() != digest {
		t.Fatalf("source-gap batch mutated the Graph")
	}
}

// ---------------------------------------------------------------------------
// Lifecycle semantics (Contract §5.4/§11.5; GMS §9.4)
// ---------------------------------------------------------------------------

// TestDeactivateRetainsHistoryAndOnlyUpdatesMetadata pins C1: after a
// protected deactivation the revision node STAYS (lifecycle=deactivated),
// its branches/edges stay, the lineage pointer clears, and a later
// reactivation reuses the historical node.
func TestDeactivateRetainsHistoryAndOnlyUpdatesMetadata(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context", "collect-extra"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}
	before := h.svc.Snapshot()

	if _, err := h.activations.Deactivate(ctx, activation.DeactivateRequest{
		LineageID: "sg-alpha", SkillRef: a,
		Authorization: contract.VersionedRef{ID: "auth-deact-0001", Version: "1", Digest: contract.DigestBytes([]byte("auth-deact-0001"))},
	}); err != nil {
		t.Fatalf("deactivate: %v (code %s)", err, activation.CodeOf(err))
	}
	result, err := h.svc.ProjectRuntime(ctx)
	if err != nil {
		t.Fatalf("projection after deactivate: %v (code %s)", err, CodeOf(err))
	}
	if result.Cursors.Activation != 2 {
		t.Fatalf("cursor = %d, want 2", result.Cursors.Activation)
	}
	after := h.svc.Snapshot()
	node, ok := after.Revisions[RevisionRef(a).String()]
	if !ok {
		t.Fatal("deactivation DELETED the revision node (history must be retained)")
	}
	if node.Lifecycle != LifecycleDeactivated {
		t.Fatalf("lifecycle = %s, want deactivated", node.Lifecycle)
	}
	if len(after.Revisions) != len(before.Revisions) || len(after.Branches) != len(before.Branches) ||
		len(after.Edges) != len(before.Edges) {
		t.Fatalf("deactivation shrank the Graph: nodes %d->%d branches %d->%d edges %d->%d",
			len(before.Revisions), len(after.Revisions), len(before.Branches), len(after.Branches), len(before.Edges), len(after.Edges))
	}
	if _, still := after.LineageActive["sg-alpha"]; still {
		t.Fatalf("lineage pointer survived the deactivation")
	}
	if _, _, state := h.svc.Watermark(); state != StateCurrent {
		t.Fatalf("state = %s, want current", state)
	}

	// Reactivation of the SAME revision reuses the historical node.
	decision := stubDecision{doc: h.decisionDoc(t, h.candidateOf(t, a), "accepted")}
	if _, err := h.activations.Reactivate(ctx, activation.ReactivateRequest{
		Decision: decision, ReleasedRef: a, ExpectedHead: nil,
	}); err != nil {
		t.Fatalf("reactivate: %v (code %s)", err, activation.CodeOf(err))
	}
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection after reactivate: %v (code %s)", err, CodeOf(err))
	}
	after = h.svc.Snapshot()
	node = after.Revisions[RevisionRef(a).String()]
	if node.Lifecycle != LifecycleActive {
		t.Fatalf("reactivated lifecycle = %s, want active", node.Lifecycle)
	}
	if node.FirstProjectedSequence != 1 {
		t.Fatalf("reactivation rewrote history: first projected = %d", node.FirstProjectedSequence)
	}
	if len(after.Revisions) != 1 {
		t.Fatalf("reactivation invented nodes: %d", len(after.Revisions))
	}
}

// TestCandidatesNeverEnterRuntimeGraph pins Q32: a committed candidate
// binding without an activation event never appears in the Runtime Graph —
// neither as a node, a branch, nor an edge endpoint.
func TestCandidatesNeverEnterRuntimeGraph(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)

	// Commit an extra candidate binding that is NEVER activated.
	ghostBody := mustJCS(t, map[string]any{"schema_version": "gms.skill-artifact.v2", "kind": "step_guidance"})
	candPayload := mustJCS(t, map[string]any{
		"schema_version": contract.SchemaCandidateArtifactRef,
		"candidate_id":   "cand-ghost-0001",
		"kind":           "step_guidance",
		"body_digest":    contract.DigestBytes(ghostBody),
		"origin_type":    "skill_proposal",
		"origin_ref":     vrefDoc("prop-ghost", contract.DigestBytes([]byte("prop-ghost"))),
	})
	if err := h.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		if _, err := tx.PutContent(ghostBody); err != nil {
			return err
		}
		_, err := tx.AppendEvent(ledger.LedgerCandidate, "", "cand-ghost-0001", candPayload)
		return err
	}); err != nil {
		t.Fatalf("commit ghost candidate: %v", err)
	}

	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}
	snap := h.svc.Snapshot()
	if len(snap.Revisions) != 1 {
		t.Fatalf("revision nodes = %d, want 1", len(snap.Revisions))
	}
	for key := range snap.Revisions {
		if strings.Contains(key, "cand-ghost") || strings.Contains(key, "cand-src") {
			t.Fatalf("candidate leaked into the Runtime Graph: %s", key)
		}
	}
	if _, ok := snap.Revisions[RevisionRef(a).String()]; !ok {
		t.Fatalf("the activated revision is missing")
	}
}

// ---------------------------------------------------------------------------
// Merge provenance (GMS §12.13; Contract §5.5/§11.4)
// ---------------------------------------------------------------------------

// TestMergeDerivedFromEdgesAreAuditable pins M3/C3: an M@1 activation
// (merge-origin event with exactly TWO derived_from refs) projects exactly
// two derived_from edges onto the source revisions, each carrying the
// activation event as its audit source, and NO supersedes edge.
func TestMergeDerivedFromEdgesAreAuditable(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	a := h.releaseSkill(t, "sg-alpha", bodyA)
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	b := h.releaseSkill(t, "sg-beta", bodyB)
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}

	// The M@1 activation event shape the merge transaction writes: two
	// derived_from refs (the sources), no previous_active_ref.
	_, mergedBody := h.guidanceEnvelope("sg-merged", []string{"collect-context", "beta-path"}, "research")
	merged := contract.SkillArtifactRef{
		SchemaVersion:  contract.SchemaSkillArtifactRef,
		LineageID:      "sg-merged",
		Version:        "1",
		Kind:           "step_guidance",
		ArtifactDigest: contract.DigestBytes(mergedBody),
	}
	event := h.appendMergeOriginActivation(t, merged, [2]contract.SkillArtifactRef{a, b}, mergedBody)
	result, err := h.svc.ProjectDelivered(ctx, []DeliveredEvent{event})
	if err != nil {
		t.Fatalf("merge-origin projection: %v (code %s)", err, CodeOf(err))
	}
	if result.Projected != 1 {
		t.Fatalf("projected %d events, want 1", result.Projected)
	}
	snap := h.svc.Snapshot()
	if edgeCount(snap, RelDerivedFrom) != 2 {
		t.Fatalf("derived_from edges = %d, want exactly 2 (M@1 derives from BOTH sources)",
			edgeCount(snap, RelDerivedFrom))
	}
	for _, edge := range snap.Edges {
		if edge.Relation != RelDerivedFrom {
			continue
		}
		if edge.From != RevisionRef(merged) {
			t.Fatalf("derived_from from %s, want the merged revision", edge.From)
		}
		if edge.To != RevisionRef(a) && edge.To != RevisionRef(b) {
			t.Fatalf("derived_from to unknown endpoint %s", edge.To)
		}
		if edge.Source.Source != SourceActivation || edge.Source.Sequence != event.Sequence ||
			edge.Source.RecordID != event.EventID || edge.Source.Digest != event.PayloadDigest {
			t.Fatalf("derived_from edge is not auditable against the activation event: %+v", edge.Source)
		}
	}
	if edgeCount(snap, RelSupersedes) != 0 {
		t.Fatalf("M@1 must carry NO supersedes edge (the sources stay active), found %d",
			edgeCount(snap, RelSupersedes))
	}
	// The merged node itself is a first-class Runtime revision.
	node, ok := snap.Revisions[RevisionRef(merged).String()]
	if !ok || node.Lifecycle != LifecycleActive {
		t.Fatalf("merged revision node = %+v (ok=%v)", node, ok)
	}
	if _, stillA := snap.LineageActive["sg-alpha"]; !stillA {
		t.Fatalf("M@1 deactivated a source lineage")
	}
}

// ---------------------------------------------------------------------------
// Rebuild identity (GMS §8.6; completion criterion 从零重建相同)
// ---------------------------------------------------------------------------

// TestRebuildFromZeroReproducesIdenticalGraphAndWatermark pins Q32: a
// from-zero rebuild over the same authoritative sources reproduces the
// identical content digest, cursor vector and watermark; the pre-rebuild
// watermark stays readable through PriorWatermark. The from-zero identity
// is proven on an independently seeded ledger (deterministic world: the
// same releases/assessments commit byte-identical records), because one
// store carries exactly ONE projection head per stream — an independent
// projector must own its own store.
func TestRebuildFromZeroReproducesIdenticalGraphAndWatermark(t *testing.T) {
	h := seedProjectionWorld(t)
	ctx := context.Background()

	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("incremental projection: %v (code %s)", err, CodeOf(err))
	}
	incrementalDigest := h.svc.GraphDigest()
	incrementalWatermark, incrementalWatermarkDigest, _ := h.svc.Watermark()
	incrementalSnapshot := h.svc.Snapshot()

	// Rebuild over the SAME state: identical content and watermark.
	if _, err := h.svc.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild: %v (code %s)", err, CodeOf(err))
	}
	if h.svc.GraphDigest() != incrementalDigest {
		t.Fatalf("rebuild changed the derived content digest: %s != %s", h.svc.GraphDigest(), incrementalDigest)
	}
	watermark, watermarkDigest, state := h.svc.Watermark()
	if watermarkDigest != incrementalWatermarkDigest {
		t.Fatalf("rebuild changed the watermark: %s != %s", watermarkDigest, incrementalWatermarkDigest)
	}
	if state != StateCurrent {
		t.Fatalf("rebuild state = %s, want current", state)
	}
	if got, _ := contract.AsString(watermark["projection_head"]); got != mustString(incrementalWatermark["projection_head"]) {
		t.Fatalf("rebuild changed the projection head: %s", got)
	}
	rebuilt := h.svc.Snapshot()
	if rebuilt.Cursors != incrementalSnapshot.Cursors || len(rebuilt.Edges) != len(incrementalSnapshot.Edges) {
		t.Fatalf("rebuild diverged: %+v vs %+v", rebuilt.Cursors, incrementalSnapshot.Cursors)
	}
	// The pre-rebuild watermark stays readable (GMS §8.6).
	if h.svc.PriorWatermark() == nil {
		t.Fatalf("rebuild dropped the prior watermark")
	}
	if got, _ := contract.AsString(h.svc.PriorWatermark()["watermark_digest"]); got != incrementalWatermarkDigest {
		t.Fatalf("prior watermark digest = %s, want the incremental one", got)
	}

	// A from-zero rebuild over an INDEPENDENTLY seeded identical world
	// (fresh store, fresh projector, same deterministic history) produces
	// the identical digest and watermark — batch-boundary independence and
	// cross-instance determinism in one.
	independent := seedProjectionWorld(t)
	if independent.svc.PriorWatermark() != nil {
		t.Fatalf("a fresh projector must not carry a prior watermark")
	}
	if _, err := independent.svc.Rebuild(ctx); err != nil {
		t.Fatalf("from-zero rebuild: %v (code %s)", err, CodeOf(err))
	}
	if independent.svc.GraphDigest() != incrementalDigest {
		t.Fatalf("from-zero rebuild digest %s != incremental %s", independent.svc.GraphDigest(), incrementalDigest)
	}
	_, freshDigest, _ := independent.svc.Watermark()
	if freshDigest != incrementalWatermarkDigest {
		t.Fatalf("from-zero rebuild watermark %s != incremental %s", freshDigest, incrementalWatermarkDigest)
	}
}

// seedProjectionWorld seeds one deterministic projection world: two source
// releases, one similarity assessment and one claim assessment — identical
// bytes and identities on every call.
func seedProjectionWorld(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	h.releaseSkill(t, "sg-alpha", bodyA)
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	b := h.releaseSkill(t, "sg-beta", bodyB)
	a := h.activeRef(t, "sg-alpha")
	if _, err := h.similaritySvc.Assess(ctx, similarity.AssessInput{
		AssessmentID: "sa-rb", Sources: []contract.SkillArtifactRef{a, b}, ScoreMicros: 900_000,
		ApplicabilityOverlap: "partial", PermissionCompatible: true,
		FeatureRecordRefs:      []contract.VersionedRef{{ID: "feature-rb", Version: "1", Digest: contract.DigestBytes([]byte("feature-rb"))}},
		SupportingEvidenceRefs: []contract.EvidenceRef{h.committedEvidence("ev-rb", "observation")},
		AssessorRef:            contract.VersionedRef{ID: "gms.assessor.v1", Version: "1", Digest: contract.DigestBytes([]byte("assessor"))},
	}); err != nil {
		t.Fatalf("assess: %v", err)
	}
	h.feedSimilarityAssessment(t, "sa-rb")
	support := h.committedEvidence("ev-rb-support", "observation")
	supporting, _ := claimAssessment("ca-rb-0001", a, "collect-context", "supports", support)
	supporting.Sequence = 1
	h.assessSource.add(supporting)
	return h
}

// ---------------------------------------------------------------------------
// Projection-stream states (Contract §9.3, §11.1)
// ---------------------------------------------------------------------------

// TestCurationStreamStaysDisabled pins U7/Contract §11.1: v1 materializes
// the Runtime stream only; the Curation stream stays disabled and
// uninitialized — no curation head is ever written.
func TestCurationStreamStaysDisabled(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	h.releaseSkill(t, "sg-alpha", bodyA)
	if _, err := h.svc.ProjectRuntime(ctx); err != nil {
		t.Fatalf("projection: %v (code %s)", err, CodeOf(err))
	}
	status := h.svc.CurationStatus()
	if status.Stream != StreamCuration || status.Materialized || status.State != "disabled" {
		t.Fatalf("curation status = %+v", status)
	}
	if _, _, ok, _ := h.store.GetHead(ledger.HeadProjection, StreamCuration); ok {
		t.Fatalf("a curation projection head exists; the stream must stay uninitialized")
	}
	if _, _, ok, _ := h.store.GetHead(ledger.HeadProjection, StreamRuntime); !ok {
		t.Fatalf("the runtime projection head must exist")
	}
}

// TestFreshProjectorStartsUninitialized pins Contract §9.3: a fresh
// projection storage is uninitialized with a zero cursor vector and no
// watermark; the first successful batch moves it through catching_up to
// current.
func TestFreshProjectorStartsUninitialized(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if state := h.svc.Snapshot().State; state != StateUninitialized {
		t.Fatalf("fresh state = %s, want uninitialized", state)
	}
	if _, digest, _ := h.svc.Watermark(); digest != "" {
		t.Fatalf("fresh watermark digest = %s, want none", digest)
	}
	_, bodyA := h.guidanceEnvelope("sg-alpha", []string{"collect-context"}, "research")
	h.releaseSkill(t, "sg-alpha", bodyA)
	_, bodyB := h.guidanceEnvelope("sg-beta", []string{"beta-path"}, "research")
	h.releaseSkill(t, "sg-beta", bodyB)
	// Delivered batch covers only seq 1: the head is catching_up (the
	// ledger holds more), then current once drained.
	result, err := h.svc.ProjectDelivered(ctx, h.deliveredEvents(t, 1, 1))
	if err != nil {
		t.Fatalf("partial delivery: %v (code %s)", err, CodeOf(err))
	}
	if result.State != StateCatchingUp {
		t.Fatalf("partial delivery state = %s, want catching_up", result.State)
	}
	result, err = h.svc.ProjectDelivered(ctx, h.deliveredEvents(t, 2, 2))
	if err != nil {
		t.Fatalf("tail delivery: %v (code %s)", err, CodeOf(err))
	}
	if result.State != StateCurrent {
		t.Fatalf("drained state = %s, want current", result.State)
	}
}

// TestStaleHeadCASDiscardsUnit pins the projection-head CAS: a unit whose
// expected head does not match the committed head is discarded with
// PROJECTION_HEAD_CONFLICT and changes nothing.
func TestStaleHeadCASDiscardsUnit(t *testing.T) {
	h := newHarness(t)
	watermark, digest, err := buildWatermark(h.registry, StateCurrent, CursorVector{Activation: 9}, PrefixState{})
	if err != nil {
		t.Fatalf("buildWatermark: %v", err)
	}
	stale := &CommitUnit{
		ExpectedHeadSeq: 41, ExpectedHeadDigest: "sha256:stale",
		State: StateCurrent, Cursors: CursorVector{Activation: 9},
		Watermark: watermark, WatermarkDigest: digest,
	}
	if err := h.storage.CommitProjection(stale); CodeOf(err) != ReasonProjectionHeadConflict {
		t.Fatalf("stale CAS: want PROJECTION_HEAD_CONFLICT, got %v (code %s)", err, CodeOf(err))
	}
	if snap := h.svc.Snapshot(); snap.State != StateUninitialized || snap.Cursors.Activation != 0 {
		t.Fatalf("stale unit advanced the fresh state: %+v", snap)
	}
	if _, _, ok, _ := h.store.GetHead(ledger.HeadProjection, StreamRuntime); ok {
		t.Fatalf("stale unit wrote a projection head")
	}
}
