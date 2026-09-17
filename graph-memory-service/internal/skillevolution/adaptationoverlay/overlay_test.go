package adaptationoverlay

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/batchconsolidation"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationexplore"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationfreeze"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
)

func TestAdaptationWithoutSubstantialDeltaIsRejectedAsDuplicate(t *testing.T) {
	h := newEpisode(t)
	source := h.sourceRef
	sourceBody := h.sourceBody
	opening := h.opening()

	cases := []struct {
		name  string
		delta Delta
	}{
		{"empty delta and empty body", Delta{}},
		{"identical generated body", Delta{
			Guard: "when retrying", Rationale: "same text", GeneratedBody: sourceBody,
		}},
		{"whitespace-only body change", Delta{
			Guard: "when retrying", Rationale: "padding", GeneratedBody: "  " + sourceBody + "\n",
		}},
		{"explicit rationale but no generated body", Delta{
			Rationale: "would only restates the source", GeneratedBody: "",
		}},
		{"different body but no explicit delta fields", Delta{
			GeneratedBody: sourceBody + "\nonly apply after a timeout",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.overlay.Choose(ChooseRequest{
				Choice:    ChoiceCreateAdaptation,
				Opening:   opening,
				SourceRef: source,
				Delta:     tc.delta,
			})
			if !errors.Is(err, ErrDuplicate) {
				t.Fatalf("Choose() error = %v, want duplicate", err)
			}
		})
	}

	if got := h.overlay.Report(); len(got) != 0 {
		t.Fatalf("rejected duplicates still entered the report: %#v", got)
	}
	resolved, err := h.overlay.SkillGet(h.skillGet(source))
	if err != nil {
		t.Fatalf("original skill_get after rejected adaptation: %v", err)
	}
	if resolved.Kind != KindOriginal || resolved.Body != sourceBody {
		t.Fatalf("canonical skill changed after duplicate rejection: %#v", resolved)
	}
}

func TestSourceRevisionBodyAndDigestRemainUnchanged(t *testing.T) {
	h := newEpisode(t)
	beforeBody, beforeDigest, ok := h.overlay.SourceSnapshot(h.sourceRef)
	if !ok || beforeBody == "" || beforeDigest == "" {
		t.Fatalf("missing opening source snapshot for %s", h.sourceRef)
	}
	beforeView := h.overlay.ViewDigest()
	beforeNodes := cloneNodes(h.view.Nodes)

	got, err := h.overlay.Choose(ChooseRequest{
		Choice:    ChoiceCreateAdaptation,
		Opening:   h.opening(),
		SourceRef: h.sourceRef,
		Delta:     substantialDeltaFor(h.sourceBody),
	})
	if err != nil {
		t.Fatalf("create adaptation: %v", err)
	}
	if got.Adaptation == nil || got.Adaptation.Body == h.sourceBody {
		t.Fatal("adaptation did not produce a distinct generated body")
	}
	if got.Adaptation.SourceBody != beforeBody || got.Adaptation.SourceDigest != beforeDigest {
		t.Fatalf("binding rewrote the source: body=%q digest=%q", got.Adaptation.SourceBody, got.Adaptation.SourceDigest)
	}

	node, digest, err := h.overlay.SourceRevision(h.sourceRef)
	if err != nil {
		t.Fatalf("source revision after adaptation: %v", err)
	}
	if node.Body != beforeBody || digest != beforeDigest {
		t.Fatalf("source revision body/digest changed: body=%q digest=%q, want %q %q", node.Body, digest, beforeBody, beforeDigest)
	}
	live, err := h.view.Get(evaluationgraph.ScopeEvaluation, mustParseRef(t, h.sourceRef))
	if err != nil {
		t.Fatalf("canonical view get: %v", err)
	}
	if live.Body != beforeBody || bodyDigest(live.Body) != beforeDigest {
		t.Fatalf("canonical view body/digest changed: %q %q", live.Body, bodyDigest(live.Body))
	}
	if !reflect.DeepEqual(h.view.Nodes, beforeNodes) {
		t.Fatalf("evaluation view nodes were mutated\nbefore: %#v\nafter: %#v", beforeNodes, h.view.Nodes)
	}
	if h.overlay.ViewDigest() != beforeView || h.view.ProjectionDigest != beforeView {
		t.Fatalf("projection digest changed from %q to %q / %q", beforeView, h.overlay.ViewDigest(), h.view.ProjectionDigest)
	}

	adapted, err := h.overlay.SkillGet(h.skillGet(got.Offer.SkillReference))
	if err != nil {
		t.Fatalf("adaptation skill_get: %v", err)
	}
	if adapted.Body == beforeBody || adapted.BodyDigest == beforeDigest {
		t.Fatal("adaptation skill_get served the original body")
	}
	original, err := h.overlay.SkillGet(h.skillGet(h.sourceRef))
	if err != nil {
		t.Fatalf("original skill_get: %v", err)
	}
	if original.Body != beforeBody || original.BodyDigest != beforeDigest {
		t.Fatalf("original skill_get body/digest = %q %q, want %q %q", original.Body, original.BodyDigest, beforeBody, beforeDigest)
	}
}

func TestAdaptationResolvesOnlyInSameAttemptManifestAndTargetScope(t *testing.T) {
	h := newEpisode(t)
	created, err := h.overlay.Choose(ChooseRequest{
		Choice:    ChoiceCreateAdaptation,
		Opening:   h.opening(),
		SourceRef: h.sourceRef,
		Delta:     substantialDeltaFor(h.sourceBody),
	})
	if err != nil {
		t.Fatalf("create adaptation: %v", err)
	}
	ref := created.Offer.SkillReference

	ok, err := h.overlay.SkillGet(h.skillGet(ref))
	if err != nil {
		t.Fatalf("same-scope skill_get: %v", err)
	}
	if ok.Kind != KindAdaptation || ok.SourceRef != h.sourceRef || ok.Body != created.Adaptation.Body {
		t.Fatalf("same-scope resolution = %#v", ok)
	}

	cases := []struct {
		name string
		req  SkillGetRequest
	}{
		{"other attempt", SkillGetRequest{SkillReference: ref, AttemptID: "attempt-other", ManifestDigest: h.pin.ManifestDigest, TargetAgentID: h.pin.TargetAgentID}},
		{"other manifest", SkillGetRequest{SkillReference: ref, AttemptID: h.pin.AttemptID, ManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", TargetAgentID: h.pin.TargetAgentID}},
		{"other target", SkillGetRequest{SkillReference: ref, AttemptID: h.pin.AttemptID, ManifestDigest: h.pin.ManifestDigest, TargetAgentID: "task-agent-other"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := h.overlay.SkillGet(tc.req); !errors.Is(err, ErrScopeMismatch) {
				t.Fatalf("SkillGet() error = %v, want scope mismatch", err)
			}
		})
	}

	original, err := h.overlay.SkillGet(h.skillGet(h.sourceRef))
	if err != nil {
		t.Fatalf("canonical skill_get in the same attempt: %v", err)
	}
	if original.Kind != KindOriginal {
		t.Fatalf("canonical resolution kind = %q", original.Kind)
	}
}

func TestAdaptationIsUnresolvableAfterEpisodeClose(t *testing.T) {
	h := newEpisode(t)
	created, err := h.overlay.Choose(ChooseRequest{
		Choice:    ChoiceCreateAdaptation,
		Opening:   h.opening(),
		SourceRef: h.sourceRef,
		Delta:     substantialDeltaFor(h.sourceBody),
	})
	if err != nil {
		t.Fatalf("create adaptation: %v", err)
	}
	if _, err := h.overlay.SkillGet(h.skillGet(created.Offer.SkillReference)); err != nil {
		t.Fatalf("skill_get before close: %v", err)
	}

	h.overlay.CloseEpisode()

	if _, err := h.overlay.SkillGet(h.skillGet(created.Offer.SkillReference)); !errors.Is(err, ErrClosed) {
		t.Fatalf("SkillGet() after close error = %v, want closed", err)
	}
	if _, err := h.overlay.Choose(ChooseRequest{
		Choice:    ChoiceCreateAdaptation,
		Opening:   h.opening(),
		SourceRef: h.sourceRef,
		Delta:     substantialDeltaFor(h.sourceBody + " again"),
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Choose() after close error = %v, want closed", err)
	}

	original, err := h.overlay.SkillGet(h.skillGet(h.sourceRef))
	if err != nil {
		t.Fatalf("canonical skill_get after close: %v", err)
	}
	if original.Kind != KindOriginal || original.Body != h.sourceBody {
		t.Fatalf("closing the overlay must not hide the canonical Skill: %#v", original)
	}
}

func TestLedgerGraphFreezeDigestsUnchangedAndReportDistinguishesOriginalFromAdaptation(t *testing.T) {
	frozen := newFrozenWorld(t)
	before := frozen.snapshot()

	overlay, err := Open(frozen.view, Pin{
		AttemptID:      "attempt-heldout-1",
		ManifestDigest: frozen.manifest.Digest,
		TargetAgentID:  "task-agent-1",
	})
	if err != nil {
		t.Fatalf("open overlay: %v", err)
	}
	opening := evaluationexplore.OpeningContext{
		SeedRefs:      []string{frozen.sourceRef},
		Query:         "advisory",
		CheckpointID:  "cp-opening-1",
		TargetAgentID: "task-agent-1",
	}

	originalChoice, err := overlay.Choose(ChooseRequest{
		Choice:    ChoiceServeOriginal,
		Opening:   opening,
		SourceRef: frozen.sourceRef,
	})
	if err != nil {
		t.Fatalf("serve original: %v", err)
	}
	if originalChoice.Offer == nil || originalChoice.Offer.Kind != KindOriginal || originalChoice.Offer.SkillReference != frozen.sourceRef {
		t.Fatalf("original offer = %#v", originalChoice.Offer)
	}
	originalResolved, err := overlay.SkillGet(SkillGetRequest{
		SkillReference: originalChoice.Offer.SkillReference,
		AttemptID:      "attempt-heldout-1",
		ManifestDigest: frozen.manifest.Digest,
		TargetAgentID:  "task-agent-1",
	})
	if err != nil {
		t.Fatalf("original skill_get: %v", err)
	}
	if originalResolved.Kind != KindOriginal {
		t.Fatalf("original skill_get kind = %q", originalResolved.Kind)
	}

	adaptedChoice, err := overlay.Choose(ChooseRequest{
		Choice:    ChoiceCreateAdaptation,
		Opening:   opening,
		SourceRef: frozen.sourceRef,
		Delta:     substantialDeltaFor(frozen.sourceBody),
	})
	if err != nil {
		t.Fatalf("create adaptation: %v", err)
	}
	if adaptedChoice.Adaptation == nil || adaptedChoice.Offer.Kind != KindAdaptation {
		t.Fatal("expected an episode-local adaptation offer")
	}
	adaptedResolved, err := overlay.SkillGet(SkillGetRequest{
		SkillReference: adaptedChoice.Offer.SkillReference,
		AttemptID:      "attempt-heldout-1",
		ManifestDigest: frozen.manifest.Digest,
		TargetAgentID:  "task-agent-1",
	})
	if err != nil {
		t.Fatalf("adaptation skill_get: %v", err)
	}
	if adaptedResolved.Kind != KindAdaptation || adaptedResolved.SourceRef != frozen.sourceRef {
		t.Fatalf("adaptation skill_get = %#v", adaptedResolved)
	}

	report := overlay.Report()
	var sawOriginal, sawAdaptation bool
	for _, entry := range report {
		switch entry.Kind {
		case KindOriginal:
			sawOriginal = true
			if entry.SkillReference != frozen.sourceRef || entry.SourceRef != "" {
				t.Fatalf("original report entry = %#v", entry)
			}
		case KindAdaptation:
			sawAdaptation = true
			if entry.SkillReference == frozen.sourceRef || entry.SourceRef != frozen.sourceRef {
				t.Fatalf("adaptation report entry = %#v", entry)
			}
			if entry.BodyDigest == originalResolved.BodyDigest {
				t.Fatal("report used the original digest for the adaptation")
			}
		default:
			t.Fatalf("unknown report kind %q", entry.Kind)
		}
	}
	if !sawOriginal || !sawAdaptation {
		t.Fatalf("report did not distinguish original from adaptation: %#v", report)
	}

	overlay.CloseEpisode()
	if _, err := overlay.SkillGet(SkillGetRequest{
		SkillReference: adaptedChoice.Offer.SkillReference,
		AttemptID:      "attempt-heldout-1",
		ManifestDigest: frozen.manifest.Digest,
		TargetAgentID:  "task-agent-1",
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed overlay still resolved adaptation: %v", err)
	}

	after := frozen.snapshot()
	if after != before {
		t.Fatalf("ledger/graph/freeze digest changed across the overlay lifecycle\nbefore: %#v\nafter: %#v", before, after)
	}
	rebuilt, err := evaluationgraph.Build(frozen.graph)
	if err != nil {
		t.Fatalf("rebuild graph: %v", err)
	}
	if rebuilt.ProjectionDigest != before.GraphDigest {
		t.Fatalf("rebuilt graph digest %q, want frozen %q", rebuilt.ProjectionDigest, before.GraphDigest)
	}
	node, err := rebuilt.Get(evaluationgraph.ScopeEvaluation, mustParseRef(t, frozen.sourceRef))
	if err != nil {
		t.Fatalf("rebuilt source: %v", err)
	}
	if node.Body != frozen.sourceBody {
		t.Fatalf("rebuilt source body %q, want %q", node.Body, frozen.sourceBody)
	}
	if frozen.storeChanged() {
		t.Fatal("overlay wrote the Skill Evolution Ledger")
	}
}

type episodeHarness struct {
	t          *testing.T
	view       *evaluationgraph.View
	overlay    *Overlay
	pin        Pin
	sourceRef  string
	sourceBody string
}

func newEpisode(t *testing.T) *episodeHarness {
	t.Helper()
	view := mustView(t, []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-alpha", "alpha", 1, "retry-backoff seed"),
		proposal(2, "proposal-beta", "beta", 1, "retry-backoff specialized"),
		consolidate(3, "consolidate-alpha", "alpha", 1),
		consolidate(4, "consolidate-beta", "beta", 1),
		relation(5, "alpha-beta", "alpha", 1, "beta", 1, "specializes"),
	})
	pin := Pin{
		AttemptID:      "attempt-1",
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetAgentID:  "task-agent-1",
	}
	overlay, err := Open(view, pin)
	if err != nil {
		t.Fatalf("open overlay: %v", err)
	}
	ref := "skill://evaluation/beta@1"
	node, err := view.Get(evaluationgraph.ScopeEvaluation, mustParseRef(t, ref))
	if err != nil {
		t.Fatalf("source node: %v", err)
	}
	return &episodeHarness{t: t, view: view, overlay: overlay, pin: pin, sourceRef: ref, sourceBody: node.Body}
}

func (h *episodeHarness) opening() evaluationexplore.OpeningContext {
	return evaluationexplore.OpeningContext{
		SeedRefs:      []string{"skill://evaluation/alpha@1"},
		Query:         "retry-backoff",
		CheckpointID:  "cp-opening-1",
		TargetAgentID: h.pin.TargetAgentID,
	}
}

func (h *episodeHarness) skillGet(ref string) SkillGetRequest {
	return SkillGetRequest{
		SkillReference: ref,
		AttemptID:      h.pin.AttemptID,
		ManifestDigest: h.pin.ManifestDigest,
		TargetAgentID:  h.pin.TargetAgentID,
	}
}

func substantialDeltaFor(sourceBody string) Delta {
	return Delta{
		Guard:         "when the held-out task mentions a timeout",
		Steps:         []string{"keep the source retry-backoff", "require an explicit timeout before retrying"},
		Rationale:     "the opening snapshot names a timeout the source Skill does not mention",
		GeneratedBody: sourceBody + "\nonly retry after the opening timeout is observed",
	}
}

type frozenWorld struct {
	t          *testing.T
	store      *ledger.MemoryStore
	freeze     *evaluationfreeze.Service
	manifest   evaluationfreeze.Manifest
	graph      evaluationgraph.CanonicalFixture
	view       *evaluationgraph.View
	sourceRef  string
	sourceBody string
	revision   uint64
	digest     string
}

type freezeSnapshot struct {
	LedgerRevision uint64
	LedgerDigest   string
	GraphDigest    string
	FreezeDigest   string
}

func newFrozenWorld(t *testing.T) *frozenWorld {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("conformance dir: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("reason policy: %v", err)
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("memory ledger: %v", err)
	}
	tx, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("transaction manager: %v", err)
	}
	source := rawproposal.NewMemoryTrajectorySource()
	source.Put(rawproposal.FrozenTrajectory{
		ID: "trajectory-01", SnapshotID: "snapshot-01",
		CompleteTrajectory: []string{"opening", "checkpoint", "outcome"},
		PublicOutcome:      "failed: file-not-found",
		Checkpoints:        map[string]rawproposal.Checkpoint{"checkpoint-01": {ID: "checkpoint-01", SnapshotID: "snapshot-01"}},
		Evidence: map[string]rawproposal.Evidence{
			"evidence-01": {ID: "evidence-01", TrajectoryID: "trajectory-01", SnapshotID: "snapshot-01", ObservableFacts: []string{"the relative path resolved outside the workspace"}},
		},
		AuthorizedDiagnosisRunIDs: map[string]bool{"diagnosis-run-01": true},
	})
	proposals, err := rawproposal.NewService(source, tx)
	if err != nil {
		t.Fatalf("raw proposal service: %v", err)
	}
	consolidate, err := batchconsolidation.NewService(proposals, store, tx)
	if err != nil {
		t.Fatalf("consolidation service: %v", err)
	}
	ids := []string{
		admitProposal(t, proposals, "raw-proposal-overlay-aaaaaaaa01"),
		admitProposal(t, proposals, "raw-proposal-overlay-aaaaaaaa02"),
	}
	result, err := consolidate.Consolidate(context.Background(), batchconsolidation.Request{
		IdempotencyKey:         "idem-overlay-freeze",
		AgentRunID:             "consolidator-01",
		ExpectedLedgerRevision: 0,
		FamilyProposalIDs:      ids,
	})
	if err != nil {
		t.Fatalf("consolidate: %v", err)
	}
	alpha := evaluationgraph.RevisionRef{LineageID: "alpha", Revision: 1}
	beta := evaluationgraph.RevisionRef{LineageID: "beta", Revision: 1}
	graph, err := evaluationgraph.NewFixture([]evaluationgraph.CanonicalRecord{
		{Sequence: 1, ID: "proposal-alpha", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: alpha, Body: "advisory alpha", Advisory: true}},
		{Sequence: 2, ID: "proposal-beta", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: beta, Body: "advisory beta", Advisory: true}},
		{Sequence: 3, ID: "consolidate-alpha", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: alpha, Disposition: "retain"}},
		{Sequence: 4, ID: "consolidate-beta", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: beta, Disposition: "merge"}},
		{Sequence: 5, ID: "alpha-beta", Kind: evaluationgraph.RecordRelation, Relation: &evaluationgraph.Relation{From: alpha, To: beta, Kind: "related_to"}},
	})
	if err != nil {
		t.Fatalf("graph fixture: %v", err)
	}
	view, err := evaluationgraph.Build(graph)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}
	freeze, err := evaluationfreeze.NewService(proposals, store)
	if err != nil {
		t.Fatalf("freeze service: %v", err)
	}
	manifest, err := freeze.Freeze(context.Background(), evaluationfreeze.Request{
		ExpectedLedgerRevision:       result.LedgerRevision,
		ExpectedLedgerDigest:         result.LedgerDigest,
		EvidenceCut:                  evaluationfreeze.EvidenceCut{Batches: []evaluationfreeze.EvidenceBatch{{ID: "evidence-01", Kind: "train"}}},
		ProposalIDs:                  ids,
		GraphFixture:                 graph,
		ExpectedGraphDigest:          view.ProjectionDigest,
		ExpectedGraphWatermark:       view.Pin.Watermark,
		ExpectedGraphWatermarkDigest: view.Pin.WatermarkDigest,
		Policy:                       fixturePolicy(),
		Scope: []evaluationfreeze.ScopeItem{
			{Kind: "train_trajectory", ID: "trajectory-01"},
			{Kind: "evidence_batch", ID: "evidence-01"},
		},
	})
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if _, err := freeze.RecordTestAttempt("attempt-heldout-1"); err != nil {
		t.Fatalf("record test attempt: %v", err)
	}
	sourceRef := beta.String()
	node, err := view.Get(evaluationgraph.ScopeEvaluation, beta)
	if err != nil {
		t.Fatalf("source node: %v", err)
	}
	return &frozenWorld{
		t: t, store: store, freeze: freeze, manifest: manifest, graph: graph, view: view,
		sourceRef: sourceRef, sourceBody: node.Body, revision: result.LedgerRevision, digest: result.LedgerDigest,
	}
}

func (w *frozenWorld) snapshot() freezeSnapshot {
	w.t.Helper()
	seq, digest, ok, err := w.store.GetHead(ledger.HeadProjection, evaluationfreeze.LedgerHeadKey)
	if err != nil || !ok {
		w.t.Fatalf("ledger head: ok=%v err=%v", ok, err)
	}
	sealed, ok := w.freeze.SealedManifest()
	if !ok {
		w.t.Fatal("sealed manifest missing")
	}
	return freezeSnapshot{
		LedgerRevision: seq,
		LedgerDigest:   digest,
		GraphDigest:    w.view.ProjectionDigest,
		FreezeDigest:   sealed.Digest,
	}
}

func (w *frozenWorld) storeChanged() bool {
	seq, digest, ok, err := w.store.GetHead(ledger.HeadProjection, evaluationfreeze.LedgerHeadKey)
	if err != nil || !ok {
		return true
	}
	return seq != w.revision || digest != w.digest
}

func admitProposal(t *testing.T, proposals *rawproposal.Service, id string) string {
	t.Helper()
	body := map[string]any{
		"proposal_id": id, "schema_version": rawproposal.SchemaVersion, "source_checkpoint_id": "checkpoint-01",
		"source_evidence_refs":     []any{"evidence-01"},
		"context_trigger":          "When the relative path resolved outside the workspace at checkpoint-01",
		"failure_or_opportunity":   "The relative path resolved outside the workspace and caused file-not-found.",
		"baseline_behavior":        "Invoke the tool with the relative path directly.",
		"non_obvious_insight":      "The relative path resolved outside the workspace, so tool invocation needs an explicit workspace-root path.",
		"decision_policy_or_steps": []any{"When the relative path resolved outside the workspace, resolve it against the workspace root before invoking the tool."},
		"expected_behavior_change": "Resolve the path against the workspace root before invoking the tool.",
		"contraindications":        []any{"Do not rewrite an already absolute path."},
		"pitfalls":                 []any{"Do not rewrite an already absolute path."},
		"outcome_observed":         "failed: file-not-found",
		"novelty_status":           rawproposal.NoveltyHypothesized,
		"created_by_agent_run_id":  "diagnosis-run-01",
		"created_at":               "2026-09-17T09:53:15Z",
	}
	digest, err := contract.DigestOf(map[string]any{
		"proposal_id": body["proposal_id"], "schema_version": body["schema_version"],
		"source_checkpoint_id": body["source_checkpoint_id"], "source_evidence_refs": body["source_evidence_refs"],
		"context_trigger": body["context_trigger"], "failure_or_opportunity": body["failure_or_opportunity"],
		"baseline_behavior": body["baseline_behavior"], "non_obvious_insight": body["non_obvious_insight"],
		"decision_policy_or_steps": body["decision_policy_or_steps"], "expected_behavior_change": body["expected_behavior_change"],
		"contraindications": body["contraindications"], "pitfalls": body["pitfalls"],
		"outcome_observed": body["outcome_observed"], "novelty_status": body["novelty_status"],
		"created_by_agent_run_id": body["created_by_agent_run_id"], "created_at": body["created_at"],
	})
	if err != nil {
		t.Fatalf("proposal digest: %v", err)
	}
	body["content_digest"] = digest
	ref, err := proposals.Admit(context.Background(), rawproposal.AdmissionRequest{
		IdempotencyKey: "idem-" + id, DiagnosisRunID: "diagnosis-run-01",
		TrajectoryID: "trajectory-01", Body: body,
	})
	if err != nil {
		t.Fatalf("admit %s: %v", id, err)
	}
	return ref.ProposalID
}

func fixturePolicy() evaluationfreeze.PolicyRevisions {
	pin := func(name string) string {
		digest, err := contract.DigestOf(name)
		if err != nil {
			panic(err)
		}
		return digest
	}
	return evaluationfreeze.PolicyRevisions{
		PromptDigest:        pin("warm-skill-graph-batch.prompt.v1"),
		SchemaDigest:        pin("warm-skill-graph-batch.schema.v1"),
		ModelDigest:         pin("warm-skill-graph-batch.model.v1"),
		ToolDigest:          pin("warm-skill-graph-batch.tool.v1"),
		ConfigDigest:        pin("warm-skill-graph-batch.config.v1"),
		GradingPolicyDigest: pin("warm-skill-graph-batch.grading.v1"),
	}
}

func proposal(seq uint64, id, lineage string, revision uint64, body string) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq, ID: id, Kind: evaluationgraph.RecordProposal,
		Proposal: &evaluationgraph.Proposal{Ref: evaluationgraph.RevisionRef{LineageID: lineage, Revision: revision}, Body: body, Advisory: true},
	}
}

func consolidate(seq uint64, id, lineage string, revision uint64) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq, ID: id, Kind: evaluationgraph.RecordConsolidation,
		Consolidation: &evaluationgraph.Consolidation{Target: evaluationgraph.RevisionRef{LineageID: lineage, Revision: revision}, Disposition: "retain"},
	}
}

func relation(seq uint64, id, fromLineage string, fromRev uint64, toLineage string, toRev uint64, kind string) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq, ID: id, Kind: evaluationgraph.RecordRelation,
		Relation: &evaluationgraph.Relation{
			From: evaluationgraph.RevisionRef{LineageID: fromLineage, Revision: fromRev},
			To:   evaluationgraph.RevisionRef{LineageID: toLineage, Revision: toRev},
			Kind: kind,
		},
	}
}

func mustView(t *testing.T, records []evaluationgraph.CanonicalRecord) *evaluationgraph.View {
	t.Helper()
	fixture, err := evaluationgraph.NewFixture(records)
	if err != nil {
		t.Fatal(err)
	}
	view, err := evaluationgraph.Build(fixture)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func mustParseRef(t *testing.T, raw string) evaluationgraph.RevisionRef {
	t.Helper()
	ref, err := evaluationexplore.ParseSkillReference(raw, evaluationgraph.ScopeEvaluation)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func cloneNodes(nodes []evaluationgraph.Node) []evaluationgraph.Node {
	out := make([]evaluationgraph.Node, len(nodes))
	copy(out, nodes)
	return out
}
