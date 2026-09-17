// Package gmsview is a test-only adapter that seals a real
// EvaluationFreezeManifest and resolves exact Skill bodies from the
// rebuilt Evaluation Graph. Host production code never imports it.
package gmsview

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/batchconsolidation"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationfreeze"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
)

var (
	ErrPermission    = errors.New("gms view: permission denied for exact skill get")
	ErrCrossManifest = errors.New("gms view: skill revision is not in the sealed manifest")
	ErrDigest        = errors.New("gms view: body digest does not match the frozen view")
)

const (
	PinnedLineage  = "alpha"
	PinnedRevision = uint64(1)
	PinnedBody     = "advisory alpha"
)

// Request is one Host-facing exact resolve against the sealed freeze.
type Request struct {
	ManifestDigest string
	SkillReference string
	AgentID        string
}

// View is the GMS exact body plus the freeze/graph digests.
type View struct {
	Reference      string
	Body           []byte
	ViewDigest     string
	ManifestDigest string
}

// Adapter seals one EvaluationFreezeManifest and serves exact graph
// bodies. Authorized agents default to the task agent used by tests.
type Adapter struct {
	ManifestDigest string
	PinnedRef      string
	Body           []byte
	ViewDigest     string

	mu         sync.Mutex
	authorized map[string]bool
	view       *evaluationgraph.View
	manifest   evaluationfreeze.Manifest
}

func New() (*Adapter, error) {
	h, err := newHarness()
	if err != nil {
		return nil, err
	}
	req := h.validRequest()
	manifest, err := h.freeze.Freeze(context.Background(), req)
	if err != nil {
		return nil, err
	}
	ref := evaluationgraph.RevisionRef{LineageID: PinnedLineage, Revision: PinnedRevision}
	node, err := h.view.Get(evaluationgraph.ScopeEvaluation, ref)
	if err != nil {
		return nil, err
	}
	body := []byte(node.Body)
	return &Adapter{
		ManifestDigest: manifest.Digest,
		PinnedRef:      ref.String(),
		Body:           append([]byte(nil), body...),
		ViewDigest:     contract.DigestBytes(body),
		authorized:     map[string]bool{"task-agent": true},
		view:           h.view,
		manifest:       manifest,
	}, nil
}

func (a *Adapter) Authorize(agentID string, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.authorized == nil {
		a.authorized = map[string]bool{}
	}
	a.authorized[agentID] = ok
}

func (a *Adapter) CrossManifestRef() string {
	return "skill://evaluation/alpha@99"
}

func (a *Adapter) Resolve(_ context.Context, req Request) (View, error) {
	if req.ManifestDigest != a.ManifestDigest {
		return View{}, fmt.Errorf("%w: requested %s, sealed %s", ErrCrossManifest, req.ManifestDigest, a.ManifestDigest)
	}
	a.mu.Lock()
	allowed := a.authorized[req.AgentID]
	a.mu.Unlock()
	if !allowed {
		return View{}, fmt.Errorf("%w: agent %s", ErrPermission, req.AgentID)
	}

	ref, err := parseEvaluationRef(req.SkillReference)
	if err != nil {
		return View{}, err
	}
	node, err := a.view.Get(evaluationgraph.ScopeEvaluation, ref)
	if err != nil {
		return View{}, fmt.Errorf("%w: %v", ErrCrossManifest, err)
	}
	body := []byte(node.Body)
	digest := contract.DigestBytes(body)
	if a.ViewDigest != "" && ref.String() == a.PinnedRef && digest != a.ViewDigest {
		return View{}, fmt.Errorf("%w: pinned %s, got %s", ErrDigest, a.ViewDigest, digest)
	}
	return View{
		Reference:      ref.String(),
		Body:           body,
		ViewDigest:     digest,
		ManifestDigest: a.ManifestDigest,
	}, nil
}

func parseEvaluationRef(raw string) (evaluationgraph.RevisionRef, error) {
	const prefix = "skill://evaluation/"
	if !strings.HasPrefix(raw, prefix) || !strings.Contains(raw, "@") {
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrCrossManifest, raw)
	}
	rest := strings.TrimPrefix(raw, prefix)
	lineage, rev, ok := strings.Cut(rest, "@")
	if !ok || lineage == "" {
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrCrossManifest, raw)
	}
	n, err := strconv.ParseUint(rev, 10, 64)
	if err != nil || n == 0 {
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrCrossManifest, raw)
	}
	return evaluationgraph.RevisionRef{LineageID: lineage, Revision: n}, nil
}

type harness struct {
	proposals *rawproposal.Service
	store     *ledger.MemoryStore
	freeze    *evaluationfreeze.Service
	family    []string
	revision  uint64
	digest    string
	graph     evaluationgraph.CanonicalFixture
	view      *evaluationgraph.View
}

func newHarness() (*harness, error) {
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		return nil, err
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		return nil, err
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		return nil, err
	}
	tx, err := ledger.NewManager(store, registry)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	consolidate, err := batchconsolidation.NewService(proposals, store, tx)
	if err != nil {
		return nil, err
	}
	freeze, err := evaluationfreeze.NewService(proposals, store)
	if err != nil {
		return nil, err
	}
	h := &harness{proposals: proposals, store: store, freeze: freeze}
	for _, id := range []string{"raw-proposal-fence-aaaaaaaaaa01", "raw-proposal-fence-aaaaaaaaaa02"} {
		admitted, err := admit(proposals, id)
		if err != nil {
			return nil, err
		}
		h.family = append(h.family, admitted)
	}
	result, err := consolidate.Consolidate(context.Background(), batchconsolidation.Request{
		IdempotencyKey:         "idem-fence",
		AgentRunID:             "consolidator-01",
		ExpectedLedgerRevision: 0,
		FamilyProposalIDs:      h.family,
	})
	if err != nil {
		return nil, err
	}
	h.revision, h.digest = result.LedgerRevision, result.LedgerDigest
	alpha := evaluationgraph.RevisionRef{LineageID: PinnedLineage, Revision: PinnedRevision}
	beta := evaluationgraph.RevisionRef{LineageID: "beta", Revision: 1}
	h.graph, err = evaluationgraph.NewFixture([]evaluationgraph.CanonicalRecord{
		{Sequence: 1, ID: "proposal-alpha", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: alpha, Body: PinnedBody, Advisory: true}},
		{Sequence: 2, ID: "proposal-beta", Kind: evaluationgraph.RecordProposal, Proposal: &evaluationgraph.Proposal{Ref: beta, Body: "advisory beta", Advisory: true}},
		{Sequence: 3, ID: "consolidate-alpha", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: alpha, Disposition: "retain"}},
		{Sequence: 4, ID: "consolidate-beta", Kind: evaluationgraph.RecordConsolidation, Consolidation: &evaluationgraph.Consolidation{Target: beta, Disposition: "merge"}},
		{Sequence: 5, ID: "alpha-beta", Kind: evaluationgraph.RecordRelation, Relation: &evaluationgraph.Relation{From: alpha, To: beta, Kind: "related_to"}},
	})
	if err != nil {
		return nil, err
	}
	h.view, err = evaluationgraph.Build(h.graph)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *harness) validRequest() evaluationfreeze.Request {
	return evaluationfreeze.Request{
		ExpectedLedgerRevision:       h.revision,
		ExpectedLedgerDigest:         h.digest,
		EvidenceCut:                  evaluationfreeze.EvidenceCut{Batches: []evaluationfreeze.EvidenceBatch{{ID: "evidence-01", Kind: "train"}}},
		ProposalIDs:                  append([]string(nil), h.family...),
		GraphFixture:                 h.graph,
		ExpectedGraphDigest:          h.view.ProjectionDigest,
		ExpectedGraphWatermark:       h.view.Pin.Watermark,
		ExpectedGraphWatermarkDigest: h.view.Pin.WatermarkDigest,
		Policy:                       fixturePolicy(),
		Scope: []evaluationfreeze.ScopeItem{
			{Kind: "train_trajectory", ID: "trajectory-01"},
			{Kind: "evidence_batch", ID: "evidence-01"},
		},
	}
}

func admit(proposals *rawproposal.Service, id string) (string, error) {
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
		return "", err
	}
	body["content_digest"] = digest
	ref, err := proposals.Admit(context.Background(), rawproposal.AdmissionRequest{
		IdempotencyKey: "idem-" + id, DiagnosisRunID: "diagnosis-run-01",
		TrajectoryID: "trajectory-01", Body: body,
	})
	if err != nil {
		return "", err
	}
	return ref.ProposalID, nil
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
