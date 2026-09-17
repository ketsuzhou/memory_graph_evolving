package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	"river2.dev/pi-group-chat-host/internal/diagnosisfanout"
	"river2.dev/pi-group-chat-host/internal/memoryclient"
)

const graphBatchLegacyUnverified = "legacy_unverified"

// composeGraphBatchRuntime builds the production CLI runtime. Authority is
// GMS over HTTP; runner-local []skillProposal is never a writer.
func composeGraphBatchRuntime(config armConfig) (*graphBatchRuntime, error) {
	if strings.TrimSpace(config.gmsURL) == "" {
		return nil, fmt.Errorf("%s requires a GMS instance; refusing the legacy []skillProposal pipeline", graphBatchStrategyID)
	}
	trains, heldOut := partitionGraphBatchEpisodes(config.episodes)
	client := memoryclient.NewClient(config.gmsURL, config.gmsToken, &http.Client{Timeout: 30 * time.Second}, 1<<20)
	ledger := &gmsGraphBatchLedger{client: client}
	return &graphBatchRuntime{
		Config:            defaultGraphBatchFrozenConfig(),
		Parallelism:       config.batchParallelism,
		Trains:            trains,
		TrainExecutor:     scriptedTrainExecutor{},
		AuthorityKind:     graphBatchAuthorityGMS,
		DiagnosisWorker:   graphBatchCLIDiagnosisWorker{},
		DiagnosisAdmitter: newGMSProposalAdmitter(client, trains),
		Consolidator:      ledger,
		Freezer:           ledger,
		Retriever:         ledger,
		HeldOut:           heldOut,
	}, nil
}

func partitionGraphBatchEpisodes(episodes []manifestEpisode) ([]graphBatchTrainSpec, []graphBatchHeldOutSpec) {
	var trains []graphBatchTrainSpec
	var heldOut []graphBatchHeldOutSpec
	trainSeq, testSeq := 1, 1
	for _, episode := range episodes {
		item := episode
		if episode.Split == "test" {
			heldOut = append(heldOut, graphBatchHeldOutSpec{
				Sequence:  testSeq,
				AttemptID: episode.EpisodeID,
				Opening:   openingFromEpisode(item),
			})
			testSeq++
			continue
		}
		trains = append(trains, graphBatchTrainSpec{
			Sequence:   trainSeq,
			Episode:    &item,
			Trajectory: trainTrajectoryFromEpisode(trainSeq, item),
		})
		trainSeq++
	}
	return trains, heldOut
}

func openingFromEpisode(episode manifestEpisode) concurrentepisode.OpeningContext {
	messages := make([]concurrentepisode.OpeningMessage, 0, len(episode.Turns))
	for i, turn := range episode.Turns {
		messages = append(messages, concurrentepisode.OpeningMessage{
			ID:      fmt.Sprintf("%s-turn-%d", episode.EpisodeID, i+1),
			Author:  "user",
			Content: turn.Prompt,
		})
	}
	if len(messages) == 0 {
		messages = []concurrentepisode.OpeningMessage{{
			ID: "open-" + episode.EpisodeID, Author: "user", Content: episode.TaskID,
		}}
	}
	return concurrentepisode.OpeningContext{Messages: messages}
}

func trainTrajectoryFromEpisode(sequence int, episode manifestEpisode) diagnosisfanout.Trajectory {
	id := episode.EpisodeID
	if id == "" {
		id = episode.TaskID
	}
	return diagnosisfanout.Trajectory{
		Sequence:           sequence,
		ID:                 id,
		SnapshotID:         "snapshot-" + id,
		DiagnosisRunID:     "diagnosis-run-" + id,
		CompleteTrajectory: []string{"opening", "checkpoint", "outcome"},
		Checkpoints:        map[string]diagnosisfanout.Checkpoint{"checkpoint-" + id: {ID: "checkpoint-" + id, SnapshotID: "snapshot-" + id}},
		Evidence: map[string]diagnosisfanout.Evidence{
			"evidence-" + id: {ID: "evidence-" + id, TrajectoryID: id, SnapshotID: "snapshot-" + id, ObservableFacts: []string{"train task " + id}},
		},
	}
}

type graphBatchCLIDiagnosisWorker struct{}

func (graphBatchCLIDiagnosisWorker) Diagnose(_ context.Context, input diagnosisfanout.DiagnosisInput) (diagnosisfanout.WorkerDraft, error) {
	if input.PublicOutcome.Status == "" {
		return diagnosisfanout.WorkerDraft{}, fmt.Errorf("diagnosis refused empty public outcome")
	}
	body := map[string]any{
		"proposal_id":              "raw-proposal-" + input.TrajectoryID + "-00001",
		"schema_version":           "gms.raw-skill-proposal.v1",
		"source_checkpoint_id":     "checkpoint-" + input.TrajectoryID,
		"source_evidence_refs":     []string{"evidence-" + input.TrajectoryID},
		"context_trigger":          "train task " + input.TrajectoryID,
		"failure_or_opportunity":   input.PublicOutcome.CanonicalString(),
		"baseline_behavior":        "generation-0 empty graph",
		"non_obvious_insight":      "task-local hypothesized repair from public outcome",
		"decision_policy_or_steps": []string{"inspect opening", "apply hypothesized guard"},
		"expected_behavior_change": "avoid the observed public failure",
		"contraindications":        []string{},
		"pitfalls":                 []string{},
		"outcome_observed":         input.PublicOutcome.CanonicalString(),
		"novelty_status":           "hypothesized",
		"created_by_agent_run_id":  input.DiagnosisRunID,
		"created_at":               "1970-01-01T00:00:00Z",
		"content_digest":           canonicalDigest([]string{input.TrajectoryID, input.PublicOutcome.CanonicalString()}),
	}
	return diagnosisfanout.WorkerDraft{IdempotencyKey: "idem-" + input.TrajectoryID, ProposalBody: body}, nil
}

type gmsProposalAdmitter struct {
	client       *memoryclient.Client
	trajectories map[string]diagnosisfanout.Trajectory

	mu      sync.Mutex
	records map[string]diagnosisfanout.ProposalRef
}

func newGMSProposalAdmitter(client *memoryclient.Client, trains []graphBatchTrainSpec) *gmsProposalAdmitter {
	trajectories := make(map[string]diagnosisfanout.Trajectory, len(trains))
	for _, spec := range trains {
		trajectories[spec.Trajectory.ID] = spec.Trajectory
	}
	return &gmsProposalAdmitter{client: client, trajectories: trajectories}
}

func (a *gmsProposalAdmitter) Admit(ctx context.Context, req diagnosisfanout.AdmissionRequest) (diagnosisfanout.ProposalRef, error) {
	if err := a.client.RegisterGraphBatchTrajectory(ctx, memoryclient.GraphBatchTrajectoryRequest{
		DiagnosisRunID: req.DiagnosisRunID,
		Trajectory:     frozenTrajectoryWire(req.DiagnosisRunID, a.trajectories[req.TrajectoryID], req),
	}); err != nil {
		return diagnosisfanout.ProposalRef{}, err
	}
	got, err := a.client.SkillPropose(ctx, memoryclient.SkillProposeRequest{
		RequestID:      req.IdempotencyKey,
		IdempotencyKey: req.IdempotencyKey,
		DiagnosisRunID: req.DiagnosisRunID,
		TrajectoryID:   req.TrajectoryID,
		Body:           req.Body,
	})
	if err != nil {
		return diagnosisfanout.ProposalRef{}, err
	}
	ref := diagnosisfanout.ProposalRef{ProposalID: got.ProposalID, ContentDigest: got.ContentDigest, URI: got.URI}
	a.mu.Lock()
	if a.records == nil {
		a.records = map[string]diagnosisfanout.ProposalRef{}
	}
	a.records[ref.ProposalID] = ref
	a.mu.Unlock()
	return ref, nil
}

func (a *gmsProposalAdmitter) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.records)
}

func (a *gmsProposalAdmitter) Has(proposalID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.records[proposalID]
	return ok
}

type gmsGraphBatchLedger struct {
	client *memoryclient.Client

	mu           sync.Mutex
	calls        int
	sealedDigest string
	attempts     []string
	keys         []string
}

func (m *gmsGraphBatchLedger) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *gmsGraphBatchLedger) Consolidate(ctx context.Context, req graphBatchConsolidateRequest) (graphBatchConsolidateResult, error) {
	m.mu.Lock()
	m.calls++
	m.keys = append([]string(nil), req.FamilyProposalIDs...)
	m.mu.Unlock()
	got, err := m.client.SkillConsolidate(ctx, memoryclient.SkillConsolidateRequest{
		RequestID:              "consolidate-1",
		IdempotencyKey:         "consolidate-1",
		AgentRunID:             "graph-batch-diagnosis",
		ExpectedLedgerRevision: 0,
		FamilyProposalIDs:      req.FamilyProposalIDs,
	})
	if err != nil {
		return graphBatchConsolidateResult{}, err
	}
	decisions := make([]graphBatchConsolidationDecision, 0, len(got.Decisions))
	for _, item := range got.Decisions {
		decisions = append(decisions, graphBatchConsolidationDecision{
			DecisionID:        item.DecisionID,
			Operation:         item.Operation,
			SourceProposalIDs: item.SourceProposalIDs,
		})
	}
	return graphBatchConsolidateResult{
		LedgerRevision: got.LedgerRevision,
		LedgerDigest:   got.LedgerDigest,
		Decisions:      decisions,
	}, nil
}

func (m *gmsGraphBatchLedger) Freeze(ctx context.Context, req graphBatchFreezeRequest) (string, error) {
	got, err := m.client.EvaluationFreeze(ctx, memoryclient.EvaluationFreezeRequest{
		RequestID:              "freeze-1",
		ExpectedLedgerRevision: req.Ledger.LedgerRevision,
		ExpectedLedgerDigest:   req.Ledger.LedgerDigest,
		ProposalIDs:            req.ProposalIDs,
		EvidenceCut: memoryclient.EvaluationFreezeEvidenceCut{
			Batches:         evidenceBatches(req.EvidenceBatches),
			Watermark:       req.EvidenceWatermark,
			WatermarkDigest: req.EvidenceWatermark,
		},
		Scope: []memoryclient.EvaluationFreezeScopeItem{{Kind: "train_room", ID: "graph-batch-train"}},
		Policy: memoryclient.EvaluationFreezePolicy{
			PromptDigest:        req.PromptDigest,
			SchemaDigest:        req.SchemaDigest,
			ModelDigest:         req.ModelDigest,
			ToolDigest:          req.ToolDigest,
			ConfigDigest:        req.ConfigDigest,
			GradingPolicyDigest: req.GradingPolicyDigest,
		},
	})
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	m.sealedDigest = got.Digest
	m.mu.Unlock()
	return got.Digest, nil
}

func (m *gmsGraphBatchLedger) TestBinding() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealedDigest == "" {
		return "", fmt.Errorf("evaluation freeze: no sealed manifest; tests cannot start")
	}
	return m.sealedDigest, nil
}

func (m *gmsGraphBatchLedger) RecordTestAttempt(attemptID string) error {
	if strings.TrimSpace(attemptID) == "" {
		return fmt.Errorf("graph batch test attempt id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealedDigest == "" {
		return fmt.Errorf("evaluation freeze: no sealed manifest; tests cannot start")
	}
	m.attempts = append(m.attempts, attemptID)
	return nil
}

func (m *gmsGraphBatchLedger) Attempts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.attempts...)
}

func (m *gmsGraphBatchLedger) Retrieve(_ string, manifestDigest string, _ []graphBatchHeldOutTrace) (graphBatchRetrieval, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealedDigest == "" || manifestDigest != m.sealedDigest {
		return graphBatchRetrieval{}, fmt.Errorf("held-out retrieval is pinned to the sealed manifest")
	}
	return graphBatchRetrieval{ManifestDigest: m.sealedDigest, Keys: append([]string(nil), m.keys...)}, nil
}

// importLegacySkillLedger accepts old Markdown/hash-prefix rows only when
// every entry is explicitly classed legacy_unverified. The new strategy
// still refuses to treat them as write authority.
func importLegacySkillLedger(entries []graphBatchLegacyImport) ([]string, error) {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.ProvenanceClass != graphBatchLegacyUnverified {
			return nil, fmt.Errorf("graph batch refuses unmarked legacy ledger import; mark %q as %s", entry.ID, graphBatchLegacyUnverified)
		}
		ids = append(ids, entry.ID)
	}
	return ids, nil
}

type graphBatchLegacyImport struct {
	ID              string
	ProvenanceClass string
}

func frozenTrajectoryWire(diagnosisRunID string, traj diagnosisfanout.Trajectory, req diagnosisfanout.AdmissionRequest) memoryclient.GraphBatchFrozenTrajectory {
	id := traj.ID
	if id == "" {
		id = req.TrajectoryID
	}
	outcome := traj.Outcome.CanonicalString()
	if outcome == "" {
		if raw, ok := req.Body["outcome_observed"].(string); ok {
			outcome = raw
		}
	}
	checkpoints := make(map[string]memoryclient.GraphBatchCheckpoint, len(traj.Checkpoints))
	for key, item := range traj.Checkpoints {
		checkpoints[key] = memoryclient.GraphBatchCheckpoint{ID: item.ID, SnapshotID: item.SnapshotID}
	}
	if len(checkpoints) == 0 {
		checkpoints["checkpoint-"+id] = memoryclient.GraphBatchCheckpoint{ID: "checkpoint-" + id, SnapshotID: "snapshot-" + id}
	}
	evidence := make(map[string]memoryclient.GraphBatchEvidence, len(traj.Evidence))
	for key, item := range traj.Evidence {
		evidence[key] = memoryclient.GraphBatchEvidence{
			ID: item.ID, TrajectoryID: item.TrajectoryID, SnapshotID: item.SnapshotID, ObservableFacts: append([]string(nil), item.ObservableFacts...),
		}
	}
	if len(evidence) == 0 {
		evidence["evidence-"+id] = memoryclient.GraphBatchEvidence{
			ID: "evidence-" + id, TrajectoryID: id, SnapshotID: "snapshot-" + id, ObservableFacts: []string{"train task " + id},
		}
	}
	steps := append([]string(nil), traj.CompleteTrajectory...)
	if len(steps) == 0 {
		steps = []string{"opening", "checkpoint", "outcome"}
	}
	return memoryclient.GraphBatchFrozenTrajectory{
		ID:                        id,
		SnapshotID:                firstNonEmpty(traj.SnapshotID, "snapshot-"+id),
		CompleteTrajectory:        steps,
		PublicOutcome:             firstNonEmpty(outcome, "fail: cause=unknown"),
		Checkpoints:               checkpoints,
		Evidence:                  evidence,
		AuthorizedDiagnosisRunIDs: []string{diagnosisRunID},
	}
}

func evidenceBatches(ids []string) []memoryclient.EvaluationFreezeEvidenceBatch {
	out := make([]memoryclient.EvaluationFreezeEvidenceBatch, 0, len(ids))
	for _, id := range ids {
		out = append(out, memoryclient.EvaluationFreezeEvidenceBatch{ID: id, Kind: "train"})
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
