package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	hostcontract "river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/diagnosisfanout"
	"river2.dev/pi-group-chat-host/internal/memoryclient"
)

const (
	graphBatchRawProposalSchema = "gms.raw-skill-proposal.v1"
	graphBatchProposalCreatedAt = "2026-09-17T09:53:15Z"
	graphBatchProposalNovelty   = "hypothesized"
	graphBatchBaselineBehavior  = "Invoke the tool with the relative path directly."
	graphBatchExpectedChange    = "Resolve the path against the workspace root before invoking the tool."
	graphBatchInsightSuffix     = ", so tool invocation needs an explicit workspace-root path."
	graphBatchStepTemplate      = "When %s, resolve it against the workspace root before invoking the tool."
	graphBatchPitfall           = "Do not rewrite an already absolute path."
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
	ledger := &gmsGraphBatchLedger{client: client, evidenceIDs: evidenceIDsFromTrains(trains)}
	heldOutExecutor, err := newProductionGraphBatchHeldOut(config)
	if err != nil {
		return nil, err
	}
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
		HeldOutExecutor:   heldOutExecutor,
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
				Episode:   &item,
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
	fact := graphBatchTrainObservableFact(id)
	return diagnosisfanout.Trajectory{
		Sequence:           sequence,
		ID:                 id,
		SnapshotID:         "snapshot-" + id,
		DiagnosisRunID:     "diagnosis-run-" + id,
		CompleteTrajectory: []string{"opening", "checkpoint", "outcome"},
		Checkpoints:        map[string]diagnosisfanout.Checkpoint{"checkpoint-" + id: {ID: "checkpoint-" + id, SnapshotID: "snapshot-" + id}},
		Evidence: map[string]diagnosisfanout.Evidence{
			"evidence-" + id: {ID: "evidence-" + id, TrajectoryID: id, SnapshotID: "snapshot-" + id, ObservableFacts: []string{fact}},
		},
	}
}

func graphBatchTrainObservableFact(id string) string {
	return "the public train outcome of " + id + " was observed at the opening checkpoint"
}

func firstObservableFact(input diagnosisfanout.DiagnosisInput) string {
	for _, evidence := range input.Evidence {
		for _, fact := range evidence.ObservableFacts {
			if strings.TrimSpace(fact) != "" {
				return fact
			}
		}
	}
	if input.TrajectoryID != "" {
		return graphBatchTrainObservableFact(input.TrajectoryID)
	}
	return ""
}

func evidenceIDsFromTrains(trains []graphBatchTrainSpec) []string {
	var ids []string
	seen := map[string]bool{}
	for _, spec := range trains {
		for _, evidence := range spec.Trajectory.Evidence {
			if evidence.ID == "" || seen[evidence.ID] {
				continue
			}
			seen[evidence.ID] = true
			ids = append(ids, evidence.ID)
		}
	}
	return ids
}

type graphBatchCLIDiagnosisWorker struct{}

func (graphBatchCLIDiagnosisWorker) Diagnose(_ context.Context, input diagnosisfanout.DiagnosisInput) (diagnosisfanout.WorkerDraft, error) {
	if input.PublicOutcome.Status == "" {
		return diagnosisfanout.WorkerDraft{}, fmt.Errorf("diagnosis refused empty public outcome")
	}
	fact := firstObservableFact(input)
	if fact == "" {
		return diagnosisfanout.WorkerDraft{}, fmt.Errorf("diagnosis refused a trajectory with no observable evidence facts")
	}
	checkpointID := "checkpoint-" + input.TrajectoryID
	if len(input.Checkpoints) == 1 {
		for _, item := range input.Checkpoints {
			if item.ID != "" {
				checkpointID = item.ID
			}
		}
	}
	evidenceID := "evidence-" + input.TrajectoryID
	if len(input.Evidence) == 1 {
		for _, item := range input.Evidence {
			if item.ID != "" {
				evidenceID = item.ID
			}
		}
	}
	outcome := input.PublicOutcome.CanonicalString()
	body := map[string]any{
		"proposal_id":              "raw-proposal-" + input.TrajectoryID + "-00001",
		"schema_version":           graphBatchRawProposalSchema,
		"source_checkpoint_id":     checkpointID,
		"source_evidence_refs":     []any{evidenceID},
		"context_trigger":          "When " + fact + " at " + checkpointID,
		"failure_or_opportunity":   fact + " caused the observed public outcome.",
		"baseline_behavior":        graphBatchBaselineBehavior,
		"non_obvious_insight":      fact + graphBatchInsightSuffix,
		"decision_policy_or_steps": []any{fmt.Sprintf(graphBatchStepTemplate, fact)},
		"expected_behavior_change": graphBatchExpectedChange,
		"contraindications":        []any{},
		"pitfalls":                 []any{graphBatchPitfall},
		"outcome_observed":         outcome,
		"novelty_status":           graphBatchProposalNovelty,
		"created_by_agent_run_id":  input.DiagnosisRunID,
		"created_at":               graphBatchProposalCreatedAt,
	}
	digest, err := graphBatchProposalContentDigest(body)
	if err != nil {
		return diagnosisfanout.WorkerDraft{}, err
	}
	body["content_digest"] = digest
	return diagnosisfanout.WorkerDraft{IdempotencyKey: "idem-" + input.TrajectoryID, ProposalBody: body}, nil
}

func graphBatchProposalContentDigest(body map[string]any) (string, error) {
	core := map[string]any{
		"proposal_id": body["proposal_id"], "schema_version": body["schema_version"],
		"source_checkpoint_id": body["source_checkpoint_id"], "source_evidence_refs": body["source_evidence_refs"],
		"context_trigger": body["context_trigger"], "failure_or_opportunity": body["failure_or_opportunity"],
		"baseline_behavior": body["baseline_behavior"], "non_obvious_insight": body["non_obvious_insight"],
		"decision_policy_or_steps": body["decision_policy_or_steps"], "expected_behavior_change": body["expected_behavior_change"],
		"contraindications": body["contraindications"], "pitfalls": body["pitfalls"],
		"outcome_observed": body["outcome_observed"], "novelty_status": body["novelty_status"],
		"created_by_agent_run_id": body["created_by_agent_run_id"], "created_at": body["created_at"],
	}
	encoded, err := json.Marshal(core)
	if err != nil {
		return "", fmt.Errorf("proposal content digest: %w", err)
	}
	value, err := hostcontract.ParseJSON(encoded)
	if err != nil {
		return "", fmt.Errorf("proposal content digest: %w", err)
	}
	return hostcontract.DigestOf(value)
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
	client      *memoryclient.Client
	evidenceIDs []string

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
	batches := m.evidenceIDs
	if len(batches) == 0 {
		batches = req.EvidenceBatches
	}
	got, err := m.client.EvaluationFreeze(ctx, memoryclient.EvaluationFreezeRequest{
		RequestID:              "freeze-1",
		ExpectedLedgerRevision: req.Ledger.LedgerRevision,
		ExpectedLedgerDigest:   req.Ledger.LedgerDigest,
		ProposalIDs:            req.ProposalIDs,
		EvidenceCut: memoryclient.EvaluationFreezeEvidenceCut{
			Batches: evidenceBatches(batches),
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
			ID: "evidence-" + id, TrajectoryID: id, SnapshotID: "snapshot-" + id,
			ObservableFacts: []string{graphBatchTrainObservableFact(id)},
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
