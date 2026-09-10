package memory

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

func TestCurationSnapshotRoundTripsM3AndM4DurableState(t *testing.T) {
	now := time.Date(2026, time.September, 8, 14, 15, 0, 0, time.UTC)
	publishedVersion := domain.ProjectionVersion(2)

	t.Run("M3 consolidation state", func(t *testing.T) {
		projection := domain.DerivedProjection{
			TenantID: "tenant-snap", SpaceID: "space-snap", Version: 2, ParentVersion: 1,
			EvidenceWatermark: 50, QueryWatermark: 200, RoundID: "round-snap", Digest: "projection-digest",
			Nodes: []domain.ProjectionNode{{ID: "node-snap", Content: "Durable projection", EvidenceRefs: []domain.EvidenceRef{{BatchID: "batch-snap", EventIDs: []string{"event-snap"}}}, Tags: []string{"durable"}, EntityRefs: []string{}}},
			Edges: []domain.ProjectionEdge{}, CreatedAt: now,
		}
		result := domain.RoundResult{
			RoundID: "round-snap", Outcome: domain.RoundPublished, BaseVersion: 1, PublishedVersion: &publishedVersion,
			OperationDigest: "operation-digest", CandidateDigest: "projection-digest",
			Backtest:   domain.CandidateStats{Version: 2, Digest: "projection-digest", Passed: true, Queries: []domain.QueryBacktestStat{}, Recall: 0.98, BaselineRecall: 1},
			Rejections: []domain.OperationRejection{},
		}
		fields := map[string]any{
			"consolidation_activity":   map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationActivity{"tenant-snap": {"space-snap": {SpaceID: "space-snap", CommittedBatchOrdinal: 50, QueryOrdinal: 200}}},
			"consolidation_cursors":    map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationCursor{"tenant-snap": {"space-snap": {SpaceID: "space-snap", LastPublishedBatchOrdinal: 50, LastPublishedQueryOrdinal: 200}}},
			"projection_heads":         map[domain.TenantID]map[domain.SpaceID]domain.ProjectionHead{"tenant-snap": {"space-snap": {TenantID: "tenant-snap", SpaceID: "space-snap", Version: 2, Digest: "projection-digest", EvidenceWatermark: 50}}},
			"projection_versions":      map[domain.TenantID]map[domain.SpaceID]map[domain.ProjectionVersion]domain.DerivedProjection{"tenant-snap": {"space-snap": {2: projection}}},
			"consolidation_rounds":     map[domain.TenantID]map[domain.SpaceID]map[domain.ConsolidationRoundID]domain.RoundResult{"tenant-snap": {"space-snap": {"round-snap": result}}},
			"retrieval_replay_plans":   map[string]domain.RetrievalReplayPlan{"plan-snap": {ID: "plan-snap", BaseVersion: 1, BaseDigest: "base-digest", CandidateDigest: "projection-digest", Cases: []domain.RetrievalReplayCase{}, RetrievalPolicyVersion: "retrieval-v1", EmbeddingPolicyVersion: "embedding-v1", EvaluatorVersion: "evaluator-v1", MaxResults: 10, MaxRounds: 3}},
			"retrieval_replay_results": map[domain.ConsolidationRoundID]domain.CandidateStats{"round-snap": result.Backtest},
		}
		assertSnapshotFieldsRoundTrip(t, fields)
	})

	t.Run("M4 pattern proposal replay decision and activation state", func(t *testing.T) {
		fingerprint := domain.ProposalFingerprint("fingerprint-snap")
		patternRef := domain.PatternRef{PatternID: "pattern-snap", Revision: 3}
		proposal := domain.SkillProposal{ProposalID: "proposal-snap", RoundID: "proposal-round-snap", Fingerprint: fingerprint, PatternRefs: []domain.PatternRef{patternRef}, CandidateID: "candidate-snap", CreatedAt: now}
		candidate := domain.SkillCandidate{
			TenantID: "tenant-snap", SpaceID: "space-snap", CandidateID: "candidate-snap", ProposalID: proposal.ProposalID,
			Fingerprint: fingerprint, TargetSkillID: "skill-snap", BaseArtifactVersion: 4,
			Diff:        domain.ReviewedDiff{DiffID: "diff-snap", BaseArtifactVersion: 4, BaseArtifactHash: "base-hash", CandidateArtifactHash: "candidate-hash", DiffHash: "diff-hash", Reviewer: "reviewer-snap", ReviewPolicyVersion: "review-v1", ReviewedAt: now},
			PatternRefs: []domain.PatternRef{patternRef}, Status: domain.CandidateAccepted, ReplayResultID: "replay-snap", CreatedAt: now,
		}
		plan := domain.PairedReplayPlan{
			ReplayID: "replay-snap", CandidateID: candidate.CandidateID, CaseHash: "case-hash", CandidateArtifactHash: "candidate-hash", BaseArtifactHash: "base-hash",
			ContextSnapshotHash: "context-hash", WorkspaceImageHash: "image-hash", EnvironmentManifestHash: "environment-hash", ModelID: "model-snap", RuntimeID: "runtime-snap",
			ToolPolicyVersion: "tools-v1", EvaluatorVersion: "evaluator-v1", RandomnessPolicyHash: "randomness-hash", MinPairs: 1, MaxPairs: 1,
		}
		decision := domain.CandidateDecision{DecisionID: "decision-snap", CandidateID: candidate.CandidateID, Decision: "accepted", CandidateDigest: "candidate-digest", ReplayResultID: plan.ReplayID, PolicyVersion: "decision-v1", Reason: "passed", DecidedBy: "curator-snap", DecidedAt: now}
		activation := domain.SkillActivation{ActivationID: "activation-snap", CandidateID: candidate.CandidateID, DecisionID: decision.DecisionID, ReplayResultID: plan.ReplayID, CandidateDigest: decision.CandidateDigest, ExpectedBaseVersion: 4, NewArtifactVersion: 5, ActivatedBy: "curator-snap", ActivatedAt: now}
		fields := map[string]any{
			"pattern_revisions":          map[domain.TenantID]map[domain.SpaceID]map[string]map[int64]domain.PatternRevision{"tenant-snap": {"space-snap": {"pattern-snap": {3: {TenantID: "tenant-snap", SpaceID: "space-snap", PatternID: "pattern-snap", Revision: 3, Status: domain.PatternProposalEligible, Problem: "Repeated miss", Applicability: "Go repositories", RecommendedAction: "Run focused check", Evidence: []domain.PatternEvidence{}, PolicyVersion: "pattern-v1", ContentHash: "pattern-hash", CreatedBy: "curator-snap", CreatedAt: now}}}}},
			"pattern_latest":             map[domain.TenantID]map[domain.SpaceID]map[string]int64{"tenant-snap": {"space-snap": {"pattern-snap": 3}}},
			"proposal_rounds":            map[domain.TenantID]map[domain.SpaceID]map[string]domain.ProposalRoundOutcome{"tenant-snap": {"space-snap": {"proposal-round-snap": {RoundID: "proposal-round-snap", Proposal: &proposal, PayloadHash: "round-hash", CompletedAt: now}}}},
			"proposals":                  map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillProposal{"tenant-snap": {"space-snap": {proposal.ProposalID: proposal}}},
			"proposal_fingerprint_index": map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]string{"tenant-snap": {"space-snap": {fingerprint: proposal.ProposalID}}},
			"rejection_memory":           map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]domain.RejectionMemory{"tenant-snap": {"space-snap": {"rejected-fingerprint": {Fingerprint: "rejected-fingerprint", ProposalID: "proposal-old", CandidateID: "candidate-old", DecisionID: "decision-old", Reason: "unsafe", PayloadHash: "rejection-hash", RejectedAt: now}}}},
			"reviewed_diffs":             map[string]domain.ReviewedDiff{candidate.Diff.DiffID: candidate.Diff},
			"candidates":                 map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillCandidate{"tenant-snap": {"space-snap": {candidate.CandidateID: candidate}}},
			"candidate_by_proposal":      map[string]string{proposal.ProposalID: candidate.CandidateID},
			"paired_replay_plans":        map[string]domain.PairedReplayPlan{plan.ReplayID: plan},
			"paired_replay_trials":       map[string]map[domain.ReplayArm]map[int]domain.PairedReplayTrial{plan.ReplayID: {domain.WithSkill: {1: {ReplayID: plan.ReplayID, Arm: domain.WithSkill, Ordinal: 1, InfrastructureValid: true, Success: true, EvidenceHash: "with-evidence"}}, domain.WithoutSkill: {1: {ReplayID: plan.ReplayID, Arm: domain.WithoutSkill, Ordinal: 1, InfrastructureValid: true, Success: false, EvidenceHash: "without-evidence"}}}},
			"paired_replay_results":      map[string]domain.PairedReplayResult{plan.ReplayID: {ReplayID: plan.ReplayID, Conclusion: "conclusive", With: domain.ReplayArmStats{Trials: 1, Successes: 1, Mean: 1}, Without: domain.ReplayArmStats{Trials: 1}, Passed: true, PolicyVersion: "replay-v1"}},
			"mutation_backtest_results":  map[string]any{},
			"candidate_decisions":        map[string]domain.CandidateDecision{candidate.CandidateID: decision},
			"skill_activations":          map[string]map[string]domain.SkillActivation{candidate.CandidateID: {activation.ActivationID: activation}},
			"active_skill_versions":      map[string]int64{candidate.TargetSkillID: 5},
		}
		assertSnapshotFieldsRoundTrip(t, fields)
	})
}

func TestRestoreOfTracerSnapshotInitializesAllCurationMaps(t *testing.T) {
	store := New()
	if err := store.Restore([]byte(`{"tenants":{}}`)); err != nil {
		t.Fatalf("restore tracer snapshot: %v", err)
	}
	image, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot restored store: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(image, &got); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	fields := []string{
		"causal_trials", "causal_trial_order", "causal_trial_heads", "causal_estimates", "causal_estimate_latest", "causal_estimate_heads", "causal_rewards", "causal_reward_latest", "causal_reward_heads",
		"dive_trajectories", "dive_served_order", "dive_results", "dive_trajectory_digests",
		"consolidation_activity", "consolidation_cursors", "projection_heads", "projection_versions", "consolidation_rounds", "retrieval_replay_plans", "retrieval_replay_results",
		"pattern_revisions", "pattern_latest", "proposal_rounds", "proposals", "proposal_fingerprint_index", "rejection_memory", "reviewed_diffs", "candidates", "candidate_by_proposal", "paired_replay_plans", "paired_replay_trials", "paired_replay_results", "mutation_backtest_results", "candidate_decisions", "skill_activations", "active_skill_versions",
	}
	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			value, ok := got[field]
			if !ok {
				t.Fatalf("restored snapshot omits %q", field)
			}
			var object map[string]any
			if err := json.Unmarshal(value, &object); err != nil || object == nil || len(object) != 0 {
				t.Fatalf("%s = %s, want initialized empty object", field, value)
			}
		})
	}
}

func assertSnapshotFieldsRoundTrip(t *testing.T, fields map[string]any) {
	t.Helper()
	image, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	store := New()
	if err := store.Restore(image); err != nil {
		t.Fatalf("restore curation fixture: %v", err)
	}
	roundTrip, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot curation fixture: %v", err)
	}
	var want, got map[string]any
	if err := json.Unmarshal(image, &want); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if err := json.Unmarshal(roundTrip, &got); err != nil {
		t.Fatalf("decode round trip: %v", err)
	}
	for field, wantValue := range want {
		if !reflect.DeepEqual(got[field], wantValue) {
			t.Errorf("snapshot field %q did not round trip:\n got: %#v\nwant: %#v", field, got[field], wantValue)
		}
	}
}
