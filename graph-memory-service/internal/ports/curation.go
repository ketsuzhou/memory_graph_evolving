package ports

import (
	"context"

	"river2.dev/graph-memory-service/internal/domain"
)

// CausalEvidenceStore persists immutable hash-chained trial and revision records.
type CausalEvidenceStore interface {
	AppendTrial(context.Context, domain.CausalTrialEvent) (stored domain.CausalTrialEvent, duplicate bool, err error)
	Trial(context.Context, domain.TenantID, domain.SpaceID, domain.CausalTrialEventID) (domain.CausalTrialEvent, error)
	AppendEstimateRevision(context.Context, domain.CausalEstimateRevision) (stored domain.CausalEstimateRevision, duplicate bool, err error)
	EstimateRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalEstimateID, int64) (domain.CausalEstimateRevision, error)
	LatestEstimateRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalEstimateID) (domain.CausalEstimateRevision, error)
	AppendRewardRevision(context.Context, domain.CausalRewardRevision) (stored domain.CausalRewardRevision, duplicate bool, err error)
	RewardRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalRewardID, int64) (domain.CausalRewardRevision, error)
	LatestRewardRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalRewardID) (domain.CausalRewardRevision, error)
}

// DiveStore exposes the server-owned exploration trajectory and stores one
// idempotent terminal judgment per submitted session.
type DiveStore interface {
	DiveTrajectory(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.DiveTrajectory, error)
	PutDiveResult(context.Context, domain.DiveResult) (stored domain.DiveResult, duplicate bool, err error)
	DiveResult(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.DiveResult, error)
}

// ConsolidationStore atomically publishes immutable derived projections.
type ConsolidationStore interface {
	Activity(context.Context, domain.TenantID, domain.SpaceID) (domain.ConsolidationActivity, error)
	ProjectionHead(context.Context, domain.TenantID, domain.SpaceID) (domain.ProjectionHead, error)
	Projection(context.Context, domain.TenantID, domain.SpaceID, domain.ProjectionVersion) (domain.DerivedProjection, error)
	Round(context.Context, domain.TenantID, domain.SpaceID, domain.ConsolidationRoundID) (domain.RoundResult, bool, error)
	PublishRound(context.Context, domain.ConsolidationPublishInput) (domain.RoundResult, bool, error)
	// RecordRound durably records a terminal non-published round verdict
	// (rejected). A retry of the same round ID must return the recorded
	// verdict instead of re-executing the operation list (SC-4.8).
	RecordRound(context.Context, domain.TenantID, domain.SpaceID, domain.RoundResult) error
}

// ProjectionBuildState is one lock-consistent view of the published head and
// the activity range a projection build is allowed to consume.
type ProjectionBuildState struct {
	Space           domain.Space
	Head            domain.ProjectionHead
	Activity        domain.ConsolidationActivity
	Cursor          domain.ConsolidationCursor
	EvidenceThrough int64
	QueryThrough    int64
}

// ProjectionBuilderStore exposes ordered committed evidence and the frozen
// state needed to build a candidate without writing the projection head.
type ProjectionBuilderStore interface {
	ListProjectionSpaces(context.Context) ([]domain.Space, error)
	ProjectionBuildState(context.Context, domain.TenantID, domain.SpaceID) (ProjectionBuildState, error)
	CommittedEvidenceBatches(context.Context, domain.TenantID, domain.SpaceID, int64, int64) ([]domain.EvidenceBatch, error)
}

// RetrievalReplayStore persists frozen replay inputs and audit results.
type RetrievalReplayStore interface {
	PutReplayPlan(context.Context, domain.RetrievalReplayPlan) error
	PutReplayResult(context.Context, domain.ConsolidationRoundID, domain.CandidateStats) error
}

// PatternStore persists immutable pattern revisions.
type PatternStore interface {
	AppendPatternRevision(context.Context, domain.PatternRevision, string) (domain.PatternRevision, bool, error)
	LatestPatternRevision(context.Context, domain.TenantID, domain.SpaceID, string) (domain.PatternRevision, error)
}

// ProposalStore persists atomic proposal rounds and immutable rejection memory.
type ProposalStore interface {
	CompleteProposalRound(context.Context, domain.TenantID, domain.SpaceID, domain.ProposalRoundOutcome) (domain.ProposalRoundOutcome, bool, error)
	ProposalByFingerprint(context.Context, domain.TenantID, domain.SpaceID, domain.ProposalFingerprint) (domain.SkillProposal, error)
	PutRejection(context.Context, domain.TenantID, domain.SpaceID, domain.RejectionMemory) (bool, error)
	RejectionByFingerprint(context.Context, domain.TenantID, domain.SpaceID, domain.ProposalFingerprint) (domain.RejectionMemory, error)
}

// CandidateStore persists reviewed candidates, paired replay, decisions, and activations.
type CandidateStore interface {
	PutCandidate(context.Context, domain.SkillCandidate) (domain.SkillCandidate, bool, error)
	Candidate(context.Context, domain.TenantID, domain.SpaceID, string) (domain.SkillCandidate, error)
	PutReviewedDiff(context.Context, domain.ReviewedDiff) (bool, error)
	PutReplayPlan(context.Context, domain.PairedReplayPlan) (bool, error)
	AppendReplayTrial(context.Context, domain.PairedReplayTrial) (bool, error)
	PutReplayResult(context.Context, domain.PairedReplayResult) (bool, error)
	DecideCandidate(context.Context, domain.CandidateDecision) (bool, error)
	ActivateCandidate(context.Context, domain.SkillActivation) (bool, error)
}
