// Package rawadmit is a test-only adapter that calls GMS rawproposal.Admit
// against an in-memory trajectory source. Host production code never imports it.
package rawadmit

import (
	"context"
	"path/filepath"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
)

type FrozenSpec struct {
	TrajectoryID   string
	SnapshotID     string
	DiagnosisRunID string
	Steps          []string
	PublicOutcome  string
	CheckpointID   string
	EvidenceID     string
	Fact           string
}

type Adapter struct {
	Source  *rawproposal.MemoryTrajectorySource
	Service *rawproposal.Service
}

func New() (*Adapter, error) {
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
	svc, err := rawproposal.NewService(source, tx)
	if err != nil {
		return nil, err
	}
	return &Adapter{Source: source, Service: svc}, nil
}

func (a *Adapter) Put(spec FrozenSpec) {
	a.Source.Put(rawproposal.FrozenTrajectory{
		ID:                 spec.TrajectoryID,
		SnapshotID:         spec.SnapshotID,
		CompleteTrajectory: append([]string(nil), spec.Steps...),
		PublicOutcome:      spec.PublicOutcome,
		Checkpoints: map[string]rawproposal.Checkpoint{
			spec.CheckpointID: {ID: spec.CheckpointID, SnapshotID: spec.SnapshotID},
		},
		Evidence: map[string]rawproposal.Evidence{
			spec.EvidenceID: {
				ID: spec.EvidenceID, TrajectoryID: spec.TrajectoryID, SnapshotID: spec.SnapshotID,
				ObservableFacts: []string{spec.Fact},
			},
		},
		AuthorizedDiagnosisRunIDs: map[string]bool{spec.DiagnosisRunID: true},
	})
}

func (a *Adapter) Admit(ctx context.Context, idempotencyKey, diagnosisRunID, trajectoryID string, body map[string]any) (proposalID, digest, uri string, err error) {
	ref, err := a.Service.Admit(ctx, rawproposal.AdmissionRequest{
		IdempotencyKey: idempotencyKey,
		DiagnosisRunID: diagnosisRunID,
		TrajectoryID:   trajectoryID,
		Body:           body,
	})
	if err != nil {
		return "", "", "", err
	}
	return ref.ProposalID, ref.ContentDigest, ref.URI, nil
}

func (a *Adapter) Count() int { return a.Service.Count() }

func (a *Adapter) Has(proposalID string) bool {
	_, ok := a.Service.Get(proposalID)
	return ok
}

func ContentDigest(body map[string]any) (string, error) {
	core := make(map[string]any, len(body))
	for key, value := range body {
		if key == "content_digest" {
			continue
		}
		core[key] = value
	}
	return contract.DigestOf(core)
}

func ValidProposalBody(proposalID, diagnosisRunID, checkpointID, evidenceID, fact, outcome, createdAt string) (map[string]any, error) {
	body := map[string]any{
		"proposal_id":              proposalID,
		"schema_version":           rawproposal.SchemaVersion,
		"source_checkpoint_id":     checkpointID,
		"source_evidence_refs":     []any{evidenceID},
		"context_trigger":          "When " + fact + " at " + checkpointID,
		"failure_or_opportunity":   fact + " caused the observed public outcome.",
		"baseline_behavior":        "Invoke the tool with the relative path directly.",
		"non_obvious_insight":      fact + ", so tool invocation needs an explicit workspace-root path.",
		"decision_policy_or_steps": []any{"When " + fact + ", resolve it against the workspace root before invoking the tool."},
		"expected_behavior_change": "Resolve the path against the workspace root before invoking the tool.",
		"contraindications":        []any{},
		"pitfalls":                 []any{"Do not rewrite an already absolute path."},
		"outcome_observed":         outcome,
		"novelty_status":           rawproposal.NoveltyHypothesized,
		"created_by_agent_run_id":  diagnosisRunID,
		"created_at":               createdAt,
	}
	digest, err := ContentDigest(body)
	if err != nil {
		return nil, err
	}
	body["content_digest"] = digest
	return body, nil
}
