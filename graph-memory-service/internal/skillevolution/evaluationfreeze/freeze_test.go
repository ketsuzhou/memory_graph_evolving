package evaluationfreeze

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/batchconsolidation"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
)

func TestSameInputIdempotentlyProducesSameManifestIDAndDigest(t *testing.T) {
	h := newHarness(t)
	req := h.validRequest()

	first, err := h.freeze.Freeze(context.Background(), req)
	if err != nil {
		t.Fatalf("first freeze: %v", err)
	}
	second, err := h.freeze.Freeze(context.Background(), req)
	if err != nil {
		t.Fatalf("second freeze: %v", err)
	}
	if first.ID == "" || first.Digest == "" || first.ID != second.ID || first.Digest != second.Digest {
		t.Fatalf("idempotent freeze IDs/digests differ: %#v vs %#v", first, second)
	}
	if first.Graph.ProjectionDigest == "" || first.Graph.Watermark == "" || first.LedgerDigest == "" {
		t.Fatalf("manifest is missing graph or ledger pins: %#v", first)
	}
	if first.EvidenceCut.Watermark == "" || first.EvidenceCut.WatermarkDigest == "" || len(first.Proposals) == 0 {
		t.Fatalf("manifest is missing evidence cut or proposals: %#v", first)
	}
	if first.Policy.PromptDigest == "" || first.Policy.GradingPolicyDigest == "" {
		t.Fatalf("manifest is missing policy pins: %#v", first)
	}
	binding, err := h.freeze.TestBinding()
	if err != nil {
		t.Fatalf("test binding: %v", err)
	}
	if binding.ManifestDigest != first.Digest {
		t.Fatalf("tests received %q, want the sealed digest %q", binding.ManifestDigest, first.Digest)
	}
	fresh, err := NewService(h.proposals, h.store)
	if err != nil {
		t.Fatalf("fresh service: %v", err)
	}
	replay, err := fresh.Freeze(context.Background(), req)
	if err != nil {
		t.Fatalf("replay freeze: %v", err)
	}
	if replay.ID != first.ID || replay.Digest != first.Digest {
		t.Fatalf("replay ID/digest %s/%s, want %s/%s", replay.ID, replay.Digest, first.ID, first.Digest)
	}
}

func TestMovedHeadMissingProposalProvenanceHoleAndGraphDigestMismatchFailClosedBeforeTest(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*harness, *Request)
		want   error
	}{
		{
			name: "moved_head",
			mutate: func(h *harness, req *Request) {
				moved := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
				if err := h.store.CompareAndSwap(ledger.HeadProjection, LedgerHeadKey, req.ExpectedLedgerRevision, req.ExpectedLedgerDigest, req.ExpectedLedgerRevision+1, moved); err != nil {
					t.Fatalf("move ledger head: %v", err)
				}
			},
			want: ErrMovedHead,
		},
		{
			name: "missing_proposal",
			mutate: func(_ *harness, req *Request) {
				req.ProposalIDs = append(append([]string(nil), req.ProposalIDs...), "raw-proposal-does-not-exist-00000001")
			},
			want: ErrMissingProposal,
		},
		{
			name: "provenance_hole",
			mutate: func(h *harness, req *Request) {
				h.freeze = mustService(t, stubCatalog{
					proposals: map[string]rawproposal.RawSkillProposal{
						req.ProposalIDs[0]: mustProposal(h, req.ProposalIDs[0]),
					},
				}, h.store)
			},
			want: ErrProvenanceHole,
		},
		{
			name: "graph_digest_mismatch",
			mutate: func(_ *harness, req *Request) {
				req.ExpectedGraphDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			},
			want: ErrGraphDigestMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			req := h.validRequest()
			tc.mutate(h, &req)
			_, err := h.freeze.Freeze(context.Background(), req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Freeze() error = %v, want %v", err, tc.want)
			}
			if _, ok := h.freeze.SealedManifest(); ok {
				t.Fatal("failed freeze sealed a manifest")
			}
			if _, err := h.freeze.TestBinding(); !errors.Is(err, ErrNotFrozen) {
				t.Fatalf("TestBinding() error = %v, want not frozen", err)
			}
			if _, err := h.freeze.RecordTestAttempt("attempt-1"); !errors.Is(err, ErrNotFrozen) {
				t.Fatalf("RecordTestAttempt() error = %v, want fail closed before test", err)
			}
			if got := h.freeze.TestAttempts(); len(got) != 0 {
				t.Fatalf("test attempts recorded after failed freeze: %#v", got)
			}
		})
	}
}

func TestTestRoomOverlayOrHeldOutFeedbackInScopeFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*harness, *Request)
	}{
		{"test_room_scope", func(_ *harness, req *Request) {
			req.Scope = append(req.Scope, ScopeItem{Kind: "test_room", ID: "room-test-01"})
		}},
		{"overlay_scope", func(_ *harness, req *Request) {
			req.Scope = append(req.Scope, ScopeItem{Kind: "overlay", ID: "episode-overlay-01"})
		}},
		{"held_out_feedback_scope", func(_ *harness, req *Request) {
			req.Scope = append(req.Scope, ScopeItem{Kind: "held-out-feedback", ID: "feedback-01"})
		}},
		{"test_room_evidence_batch", func(_ *harness, req *Request) {
			req.EvidenceCut.Batches = append(req.EvidenceCut.Batches, EvidenceBatch{ID: "test-room-batch", Kind: "test_room"})
		}},
		{"held_out_graph_record", func(_ *harness, req *Request) {
			records := append([]evaluationgraph.CanonicalRecord(nil), req.GraphFixture.Records...)
			records = append(records, evaluationgraph.CanonicalRecord{Sequence: uint64(len(records) + 1), ID: "held-out-1", Kind: evaluationgraph.RecordHeldOutFeedback})
			fixture, err := evaluationgraph.NewFixture(records)
			if err != nil {
				t.Fatalf("held-out fixture: %v", err)
			}
			req.GraphFixture = fixture
			req.ExpectedGraphDigest = ""
			req.ExpectedGraphWatermark = ""
			req.ExpectedGraphWatermarkDigest = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			req := h.validRequest()
			tc.mutate(h, &req)
			_, err := h.freeze.Freeze(context.Background(), req)
			if !errors.Is(err, ErrScopeContamination) {
				t.Fatalf("Freeze() error = %v, want scope contamination", err)
			}
			if _, err := h.freeze.RecordTestAttempt("attempt-1"); !errors.Is(err, ErrNotFrozen) {
				t.Fatalf("contaminated freeze started a test: %v", err)
			}
		})
	}
}

func TestNTestAttemptsRecordIdenticalManifestDigest(t *testing.T) {
	h := newHarness(t)
	manifest, err := h.freeze.Freeze(context.Background(), h.validRequest())
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	binding, err := h.freeze.TestBinding()
	if err != nil {
		t.Fatalf("test binding: %v", err)
	}
	if binding != (TestBinding{ManifestDigest: manifest.Digest}) {
		t.Fatalf("tests received %#v, want only digest %q", binding, manifest.Digest)
	}

	const n = 5
	seen := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		attempt, err := h.freeze.RecordTestAttempt(fmt.Sprintf("attempt-%d", i))
		if err != nil {
			t.Fatalf("record attempt %d: %v", i, err)
		}
		if attempt.ManifestDigest != manifest.Digest {
			t.Fatalf("attempt %s digest %q, want %q", attempt.AttemptID, attempt.ManifestDigest, manifest.Digest)
		}
		seen = append(seen, attempt.ManifestDigest)
	}
	recorded := h.freeze.TestAttempts()
	if len(recorded) != n {
		t.Fatalf("recorded %d attempts, want %d", len(recorded), n)
	}
	for i, attempt := range recorded {
		if attempt.ManifestDigest != manifest.Digest || seen[i] != manifest.Digest {
			t.Fatalf("attempt[%d] digest %q, want sealed %q", i, attempt.ManifestDigest, manifest.Digest)
		}
	}
}

func TestPartialGraphFreezeDoesNotContinueAfterWarning(t *testing.T) {
	t.Run("continue_on_warning_flag", func(t *testing.T) {
		h := newHarness(t)
		req := h.validRequest()
		req.ContinueOnGraphWarning = true
		_, err := h.freeze.Freeze(context.Background(), req)
		if !errors.Is(err, ErrPartialGraphFreeze) {
			t.Fatalf("Freeze() error = %v, want partial graph freeze", err)
		}
		assertNoSealedTest(t, h)
	})
	t.Run("explicit_partial_flag", func(t *testing.T) {
		h := newHarness(t)
		req := h.validRequest()
		req.PartialGraph = true
		_, err := h.freeze.Freeze(context.Background(), req)
		if !errors.Is(err, ErrPartialGraphFreeze) {
			t.Fatalf("Freeze() error = %v, want partial graph freeze", err)
		}
		assertNoSealedTest(t, h)
	})
	t.Run("unconsolidated_proposal_is_not_partially_frozen", func(t *testing.T) {
		h := newHarness(t)
		req := h.validRequest()
		alpha := evaluationgraph.RevisionRef{LineageID: "alpha", Revision: 1}
		beta := evaluationgraph.RevisionRef{LineageID: "beta", Revision: 1}
		fixture, err := evaluationgraph.NewFixture([]evaluationgraph.CanonicalRecord{
			{Sequence: 1, ID: "proposal-alpha", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: alpha, Body: "advisory alpha", Advisory: true}},
			{Sequence: 2, ID: "proposal-beta", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: beta, Body: "advisory beta", Advisory: true}},
			{Sequence: 3, ID: "consolidate-alpha", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: alpha, Disposition: "retain"}},
		})
		if err != nil {
			t.Fatalf("partial fixture: %v", err)
		}
		req.GraphFixture = fixture
		req.ExpectedGraphDigest = ""
		req.ExpectedGraphWatermark = ""
		req.ExpectedGraphWatermarkDigest = ""
		_, err = h.freeze.Freeze(context.Background(), req)
		if !errors.Is(err, ErrPartialGraphFreeze) {
			t.Fatalf("Freeze() error = %v, want partial graph freeze", err)
		}
		assertNoSealedTest(t, h)
	})
}

func assertNoSealedTest(t *testing.T, h *harness) {
	t.Helper()
	if _, ok := h.freeze.SealedManifest(); ok {
		t.Fatal("partial graph freeze sealed a manifest after a warning-equivalent failure")
	}
	if _, err := h.freeze.RecordTestAttempt("attempt-1"); !errors.Is(err, ErrNotFrozen) {
		t.Fatalf("partial graph freeze continued into a test: %v", err)
	}
}

type harness struct {
	t           *testing.T
	proposals   *rawproposal.Service
	store       *ledger.MemoryStore
	consolidate *batchconsolidation.Service
	freeze      *Service
	family      []string
	revision    uint64
	digest      string
	graph       evaluationgraph.CanonicalFixture
	view        *evaluationgraph.View
}

func newHarness(t *testing.T) *harness {
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
	h := &harness{t: t, proposals: proposals, store: store, consolidate: consolidate, freeze: mustService(t, proposals, store)}
	h.family = []string{
		h.admit("raw-proposal-freeze-aaaaaaaaaa01"),
		h.admit("raw-proposal-freeze-aaaaaaaaaa02"),
	}
	result, err := consolidate.Consolidate(context.Background(), batchconsolidation.Request{
		IdempotencyKey:         "idem-freeze",
		AgentRunID:             "consolidator-01",
		ExpectedLedgerRevision: 0,
		FamilyProposalIDs:      h.family,
	})
	if err != nil {
		t.Fatalf("consolidate: %v", err)
	}
	h.revision, h.digest = result.LedgerRevision, result.LedgerDigest
	alpha := evaluationgraph.RevisionRef{LineageID: "alpha", Revision: 1}
	beta := evaluationgraph.RevisionRef{LineageID: "beta", Revision: 1}
	h.graph, err = evaluationgraph.NewFixture([]evaluationgraph.CanonicalRecord{
		{Sequence: 1, ID: "proposal-alpha", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: alpha, Body: "advisory alpha", Advisory: true}},
		{Sequence: 2, ID: "proposal-beta", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: beta, Body: "advisory beta", Advisory: true}},
		{Sequence: 3, ID: "consolidate-alpha", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: alpha, Disposition: "retain"}},
		{Sequence: 4, ID: "consolidate-beta", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: beta, Disposition: "merge"}},
		{Sequence: 5, ID: "alpha-beta", Kind: evaluationgraph.RecordRelation, Relation: &evaluationgraph.Relation{From: alpha, To: beta, Kind: "related_to"}},
	})
	if err != nil {
		t.Fatalf("graph fixture: %v", err)
	}
	h.view, err = evaluationgraph.Build(h.graph)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}
	return h
}

func (h *harness) validRequest() Request {
	h.t.Helper()
	return Request{
		ExpectedLedgerRevision:       h.revision,
		ExpectedLedgerDigest:         h.digest,
		EvidenceCut:                  EvidenceCut{Batches: []EvidenceBatch{{ID: "evidence-01", Kind: "train"}}},
		ProposalIDs:                  append([]string(nil), h.family...),
		GraphFixture:                 h.graph,
		ExpectedGraphDigest:          h.view.ProjectionDigest,
		ExpectedGraphWatermark:       h.view.Pin.Watermark,
		ExpectedGraphWatermarkDigest: h.view.Pin.WatermarkDigest,
		Policy:                       fixturePolicy(),
		Scope: []ScopeItem{
			{Kind: "train_trajectory", ID: "trajectory-01"},
			{Kind: "evidence_batch", ID: "evidence-01"},
		},
	}
}

func (h *harness) admit(id string) string {
	h.t.Helper()
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
		h.t.Fatalf("proposal digest: %v", err)
	}
	body["content_digest"] = digest
	ref, err := h.proposals.Admit(context.Background(), rawproposal.AdmissionRequest{
		IdempotencyKey: "idem-" + id, DiagnosisRunID: "diagnosis-run-01",
		TrajectoryID: "trajectory-01", Body: body,
	})
	if err != nil {
		h.t.Fatalf("admit %s: %v", id, err)
	}
	return ref.ProposalID
}

func fixturePolicy() PolicyRevisions {
	pin := func(name string) string {
		digest, err := contract.DigestOf(name)
		if err != nil {
			panic(err)
		}
		return digest
	}
	return PolicyRevisions{
		PromptDigest:        pin("warm-skill-graph-batch.prompt.v1"),
		SchemaDigest:        pin("warm-skill-graph-batch.schema.v1"),
		ModelDigest:         pin("warm-skill-graph-batch.model.v1"),
		ToolDigest:          pin("warm-skill-graph-batch.tool.v1"),
		ConfigDigest:        pin("warm-skill-graph-batch.config.v1"),
		GradingPolicyDigest: pin("warm-skill-graph-batch.grading.v1"),
	}
}

func mustService(t *testing.T, catalog ProposalCatalog, heads ledger.HeadStore) *Service {
	t.Helper()
	svc, err := NewService(catalog, heads)
	if err != nil {
		t.Fatalf("freeze service: %v", err)
	}
	return svc
}

func mustProposal(h *harness, id string) rawproposal.RawSkillProposal {
	h.t.Helper()
	proposal, ok := h.proposals.Get(id)
	if !ok {
		h.t.Fatalf("proposal %q missing from catalog", id)
	}
	return proposal
}

type stubCatalog struct {
	proposals  map[string]rawproposal.RawSkillProposal
	provenance map[string]rawproposal.RawProposalProvenance
}

func (s stubCatalog) Get(id string) (rawproposal.RawSkillProposal, bool) {
	p, ok := s.proposals[id]
	return p, ok
}

func (s stubCatalog) ProvenanceForProposal(id string) (rawproposal.RawProposalProvenance, bool) {
	p, ok := s.provenance[id]
	return p, ok
}
