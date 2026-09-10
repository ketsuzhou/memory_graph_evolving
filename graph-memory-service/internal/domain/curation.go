package domain

import "time"

// EvidenceRef identifies immutable committed evidence within one Space.
type EvidenceRef struct {
	BatchID  BatchID
	EventIDs []string
}

type CausalTrialEventID string
type CausalEstimateID string
type CausalRewardID string

type CausalArm string

const (
	CausalArmWithIntervention    CausalArm = "with_intervention"
	CausalArmWithoutIntervention CausalArm = "without_intervention"
)

type CausalConclusion string

const (
	CausalConclusionConclusive   CausalConclusion = "conclusive"
	CausalConclusionInconclusive CausalConclusion = "inconclusive"
)

type WitnessKind string

const (
	WitnessKindNone       WitnessKind = "none"
	WitnessKindNecessity  WitnessKind = "necessity"
	WitnessKindEfficiency WitnessKind = "efficiency"
)

type WitnessStatus string

const (
	WitnessStatusNone        WitnessStatus = "none"
	WitnessStatusProvisional WitnessStatus = "provisional"
	WitnessStatusActive      WitnessStatus = "active"
	WitnessStatusContested   WitnessStatus = "contested"
	WitnessStatusRetired     WitnessStatus = "retired"
)

type WitnessTransitionReason string

const (
	WitnessReasonPairedWitness             WitnessTransitionReason = "paired_witness"
	WitnessReasonIndependentSupport        WitnessTransitionReason = "independent_support"
	WitnessReasonNonpositiveInterval       WitnessTransitionReason = "nonpositive_interval"
	WitnessReasonMandatoryRegressionFailed WitnessTransitionReason = "mandatory_regression_failed"
	WitnessReasonSourceRetracted           WitnessTransitionReason = "source_retracted"
	WitnessReasonArtifactSuperseded        WitnessTransitionReason = "artifact_superseded"
	WitnessReasonNegativeRetirementGate    WitnessTransitionReason = "negative_retirement_gate"
)

type CausalTrialEvent struct {
	ID                   CausalTrialEventID
	TenantID             TenantID
	SpaceID              SpaceID
	ExperimentID         string
	Arm                  CausalArm
	Ordinal              int
	ContextHash          string
	InterventionHash     string
	OutcomeAuthorityHash string
	InfrastructureValid  bool
	Success              bool
	Effect               float64
	EvidenceRefs         []EvidenceRef
	PreviousHash         string
	PayloadHash          string
	CreatedAt            time.Time
}

type CausalEstimateRevision struct {
	TenantID              TenantID
	SpaceID               SpaceID
	EstimateID            CausalEstimateID
	Revision              int64
	ExperimentID          string
	ArtifactHash          string
	SourceTrialIDs        []CausalTrialEventID
	SourceTrialDigest     string
	WithTrials            int
	WithoutTrials         int
	EffectMean            float64
	EffectVariance        float64
	IntervalLower         float64
	IntervalUpper         float64
	EstimatorVersion      string
	IntervalPolicyVersion string
	Conclusion            CausalConclusion
	WitnessKind           WitnessKind
	WitnessStatus         WitnessStatus
	TransitionReason      WitnessTransitionReason
	HistoricalValidity    bool
	CurrentApplicability  bool
	PreviousHash          string
	PayloadHash           string
	CreatedAt             time.Time
}

type CausalRewardRevision struct {
	TenantID            TenantID
	SpaceID             SpaceID
	RewardID            CausalRewardID
	Revision            int64
	EstimateID          CausalEstimateID
	EstimateRevision    int64
	RewardVectorHash    string
	RewardVectorJSON    []byte
	RewardPolicyVersion string
	Scalar              *float64
	HardGateDisposition string
	PreviousHash        string
	PayloadHash         string
	CreatedAt           time.Time
}

type WitnessPolicy struct {
	PolicyVersion              string
	Delta                      float64
	EfficiencyCostTolerance    float64
	MinimumIndependentLineages int
	RetirementUpperBound       float64
}

type WitnessTransitionInput struct {
	Current             *CausalEstimateRevision
	Estimate            CausalEstimateRevision
	Reason              WitnessTransitionReason
	IndependentLineages int
	RequestedStatus     WitnessStatus
	RequestedRevision   int64
}

type WitnessApplicability struct {
	HistoricalAuditEligible bool
	PromotionEligible       bool
	RecommendationEligible  bool
}

const (
	GrantPurposeCuration GrantPurpose = "curation"

	GrantOperationCausalTrialAppend    GrantOperation = "causal.trial.append"
	GrantOperationCausalEstimateAppend GrantOperation = "causal.estimate.append"
	GrantOperationCausalRewardAppend   GrantOperation = "causal.reward.append"
	GrantOperationCausalRead           GrantOperation = "causal.read"
	GrantOperationDiveRead             GrantOperation = "dive.read"
)

type DiveClassification string

const (
	DiveFound           DiveClassification = "found"
	DiveMiss            DiveClassification = "miss"
	DiveError           DiveClassification = "error"
	DiveBudgetExhausted DiveClassification = "budget_exhausted"
	DiveTimeout         DiveClassification = "timeout"
)

type DiveDimensions struct {
	Relevance    float64
	Groundedness float64
	Completeness float64
}

type DiveTrajectory struct {
	SessionID            ExplorationSessionID
	TenantID             TenantID
	PrincipalID          PrincipalID
	SpaceIDs             []SpaceID
	State                string
	Found                bool
	TerminalReason       DiveClassification
	Summary              string
	Rounds               int
	ServedItems          []RecallItem
	SubmittedCitationIDs []string
}

type DiveTrajectoryScore struct {
	SessionID      ExplorationSessionID
	Classification DiveClassification
	Relevance      float64
	Groundedness   float64
	Completeness   float64
	Overall        float64
	Rounds         int
}

type DiveInformationItem struct {
	CitationID      string
	Statement       string
	SourceSpaceID   SpaceID
	MemoryVersion   int64
	EvidenceBatchID BatchID
	EventIDs        []string
	RetrievalScore  float64
	Submitted       bool
	Authoritative   bool
}

type DiveResult struct {
	TenantID    TenantID
	SessionID   ExplorationSessionID
	Score       DiveTrajectoryScore
	Items       []DiveInformationItem
	Incomplete  bool
	EvaluatedAt time.Time
}

type ConsolidationRewardInput struct {
	Passed           bool
	GateFailures     []string
	Recall           float64
	BaselineRecall   float64
	Coverage         float64
	BaselineCoverage float64
	Regressions      int
	Queries          int
	CandidateRounds  float64
	BaselineRounds   float64
	EmbedBytes       int64
	ChangedNodes     int
	EdgeChurn        int
}

type ConsolidationRewardWeights struct {
	Quality    float64
	Efficiency float64
}

type RewardComponents struct {
	Source            string
	RecallDelta       float64
	CoverageDelta     float64
	RegressionPenalty float64
	QualityDelta      float64
	EfficiencyDelta   float64
}

// ProjectionVersion identifies one immutable derived projection revision.
type ProjectionVersion int64
type ProjectionNodeID string
type ProjectionEdgeID string
type ConsolidationRoundID string

type EdgeKind string

const (
	EdgeHierarchy EdgeKind = "hierarchy"
	EdgeRelation  EdgeKind = "relation"
)

type ProjectionNode struct {
	ID           ProjectionNodeID
	Content      string
	EvidenceRefs []EvidenceRef
	Level        int
	Tags         []string
	EntityRefs   []string
}

type ProjectionEdge struct {
	ID           ProjectionEdgeID
	Kind         EdgeKind
	Relation     string
	From         ProjectionNodeID
	To           ProjectionNodeID
	Confidence   float64
	EvidenceRefs []EvidenceRef
}

type DerivedProjection struct {
	TenantID          TenantID
	SpaceID           SpaceID
	Version           ProjectionVersion
	ParentVersion     ProjectionVersion
	EvidenceWatermark int64
	QueryWatermark    int64
	RoundID           ConsolidationRoundID
	Digest            string
	Nodes             []ProjectionNode
	Edges             []ProjectionEdge
	CreatedAt         time.Time
}

type ProjectionHead struct {
	TenantID          TenantID
	SpaceID           SpaceID
	Version           ProjectionVersion
	Digest            string
	EvidenceWatermark int64
}

type OperationKind string

const (
	OperationAddNode          OperationKind = "add_node"
	OperationUpdateNode       OperationKind = "update_node"
	OperationDeleteNode       OperationKind = "delete_node"
	OperationMergeNodes       OperationKind = "merge_nodes"
	OperationAddHierarchyEdge OperationKind = "add_hierarchy_edge"
	OperationAddRelationEdge  OperationKind = "add_relation_edge"
	OperationPruneEdge        OperationKind = "prune_edge"
	OperationSubmit           OperationKind = "submit"
)

type Operation struct {
	Kind         OperationKind
	NodeID       ProjectionNodeID
	Node         *ProjectionNode
	InputNodeIDs []ProjectionNodeID
	EdgeID       ProjectionEdgeID
	Edge         *ProjectionEdge
}

type ConsolidationActivity struct {
	SpaceID               SpaceID
	CommittedBatchOrdinal int64
	QueryOrdinal          int64
}

type ConsolidationCursor struct {
	SpaceID                   SpaceID
	LastPublishedBatchOrdinal int64
	LastPublishedQueryOrdinal int64
}

type CostWeights struct {
	Round float64
	Tail  float64
	Embed float64
	Node  float64
	Graph float64
}

type Config struct {
	TriggerCommittedBatches int64
	TriggerQueries          int64
	RecallTolerance         float64
	RoundsTolerance         int
	CostWeights             CostWeights
}

type ReplayPartition string

const (
	ReplayEvaluation ReplayPartition = "evaluation"
	ReplayHoldout    ReplayPartition = "holdout"
	ReplaySafety     ReplayPartition = "safety"
)

type BacktestItem struct {
	ID           string
	Statement    string
	NodeIDs      []ProjectionNodeID
	EvidenceRefs []EvidenceRef
}

type RetrievalReplayCase struct {
	ID              string
	TraceID         string
	NormalizedQuery string
	Items           []BacktestItem
	Partition       ReplayPartition
	BaselineFound   bool
	BaselineRounds  int
}

type RetrievalReplayPlan struct {
	ID                     string
	BaseVersion            ProjectionVersion
	BaseDigest             string
	CandidateDigest        string
	Cases                  []RetrievalReplayCase
	RetrievalPolicyVersion string
	EmbeddingPolicyVersion string
	EvaluatorVersion       string
	MaxResults             int
	MaxRounds              int
}

type ReplayDisposition string

const (
	ReplayMeasured              ReplayDisposition = "measured"
	ReplaySkipped               ReplayDisposition = "skipped"
	ReplayIneligible            ReplayDisposition = "ineligible"
	ReplayInfrastructureInvalid ReplayDisposition = "infrastructure_invalid"
)

type QueryBacktestStat struct {
	CaseID         string
	TraceID        string
	Partition      ReplayPartition
	Disposition    ReplayDisposition
	BaselineFound  bool
	BaselineRounds int
	Covered        bool
	Found          bool
	Rounds         float64
	Regressed      bool
	ItemsTotal     int
	ItemsSatisfied int
	ItemMisses     []string
	SkipReason     string
}

type CandidateStats struct {
	Version        ProjectionVersion
	Digest         string
	ValidationMode string
	Passed         bool
	GateFailures   []string
	Queries        []QueryBacktestStat
	Recall         float64
	BaselineRecall float64
	MeanRounds     float64
	P95Rounds      float64
	ChangedNodes   int
	EmbedBytes     int
	EdgeChurn      int
	Cost           float64
}

type BacktestConfig struct {
	RecallTolerance float64
	RoundsTolerance int
	CostWeights     CostWeights
}

type RoundInput struct {
	TenantID          TenantID
	SpaceID           SpaceID
	RoundID           ConsolidationRoundID
	BaseVersion       ProjectionVersion
	BaseDigest        string
	EvidenceWatermark int64
	QueryWatermark    int64
	Operations        []Operation
	ReplayPlan        RetrievalReplayPlan
}

type RoundOutcome string

const (
	RoundPublished RoundOutcome = "published"
	RoundRejected  RoundOutcome = "rejected"
	RoundFailed    RoundOutcome = "failed"
)

type OperationRejection struct {
	Index  int
	Kind   OperationKind
	Reason string
}

type RoundResult struct {
	RoundID          ConsolidationRoundID
	Outcome          RoundOutcome
	BaseVersion      ProjectionVersion
	PublishedVersion *ProjectionVersion
	OperationDigest  string
	CandidateDigest  string
	Backtest         CandidateStats
	Rejections       []OperationRejection
}

type ConsolidationPublishInput struct {
	TenantID       TenantID
	SpaceID        SpaceID
	Round          RoundResult
	Projection     DerivedProjection
	ExpectedHead   ProjectionHead
	ActivityCursor ConsolidationCursor
}

type PatternStatus string

const (
	PatternTentative        PatternStatus = "tentative"
	PatternSupported        PatternStatus = "supported"
	PatternProposalEligible PatternStatus = "proposal_eligible"
	PatternRejected         PatternStatus = "rejected"
)

type PatternEvidenceStrength string

const (
	EvidenceRepeat      PatternEvidenceStrength = "repeat"
	EvidenceContrastive PatternEvidenceStrength = "contrastive"
	EvidenceCausal      PatternEvidenceStrength = "causal"
)

type PatternRef struct {
	PatternID string
	Revision  int64
}

type CausalEstimateRef struct {
	EstimateID CausalEstimateID
	Revision   int64
}

type PatternEvidence struct {
	EvidenceRef    EvidenceRef
	SourceSpaceID  SpaceID
	LineageID      string
	Strength       PatternEvidenceStrength
	CausalEstimate *CausalEstimateRef
}

type PatternRevision struct {
	TenantID          TenantID
	SpaceID           SpaceID
	PatternID         string
	Revision          int64
	Status            PatternStatus
	Problem           string
	Applicability     string
	RecommendedAction string
	Evidence          []PatternEvidence
	PolicyVersion     string
	ContentHash       string
	RejectionReason   string
	CreatedBy         PrincipalID
	CreatedAt         time.Time
}

type PatternPolicy struct {
	PolicyVersion          string
	MinIndependentLineages int
}

type PatternAssessment struct {
	RepeatLineages    int
	HasContrastive    bool
	HasVerifiedCausal bool
	EligibleStatus    PatternStatus
	Rationale         string
}

type DraftPatternInput struct {
	TenantID          TenantID
	SpaceID           SpaceID
	PatternID         string
	Problem           string
	Applicability     string
	RecommendedAction string
	Evidence          []PatternEvidence
	Actor             PrincipalID
	CreatedAt         time.Time
}

type ProposalFingerprint string

type ProposalFingerprintInput struct {
	TargetSkillID         string
	NewSkillName          string
	BaseArtifactVersion   int64
	BaseArtifactHash      string
	CandidateArtifactHash string
	ReviewedDiffHash      string
	RequestedScope        string
	PatternRefs           []PatternRef
}

type SkillProposal struct {
	ProposalID  string
	RoundID     string
	Fingerprint ProposalFingerprint
	PatternRefs []PatternRef
	CandidateID string
	CreatedAt   time.Time
}

type ProposalRoundOutcome struct {
	RoundID        string
	Proposal       *SkillProposal
	NoActionReason string
	PayloadHash    string
	CompletedAt    time.Time
}

type RejectionMemory struct {
	Fingerprint ProposalFingerprint
	ProposalID  string
	CandidateID string
	DecisionID  string
	Reason      string
	PayloadHash string
	RejectedAt  time.Time
}

type ReviewedDiff struct {
	DiffID                string
	BaseArtifactVersion   int64
	BaseArtifactHash      string
	CandidateArtifactHash string
	DiffHash              string
	Reviewer              PrincipalID
	ReviewPolicyVersion   string
	ReviewedAt            time.Time
}

type CandidateStatus string

const (
	CandidateNeedsReview CandidateStatus = "needs_review"
	CandidateEvaluating  CandidateStatus = "evaluating"
	CandidateAccepted    CandidateStatus = "accepted"
	CandidateRejected    CandidateStatus = "rejected"
	CandidateStale       CandidateStatus = "stale"
)

type SkillCandidate struct {
	TenantID            TenantID
	SpaceID             SpaceID
	CandidateID         string
	ProposalID          string
	Fingerprint         ProposalFingerprint
	TargetSkillID       string
	NewSkillName        string
	BaseArtifactVersion int64
	Diff                ReviewedDiff
	PatternRefs         []PatternRef
	Status              CandidateStatus
	ReplayResultID      string
	CreatedAt           time.Time
}

type ReplayArm string

const (
	WithSkill    ReplayArm = "with_skill"
	WithoutSkill ReplayArm = "without_skill"
)

type PairedReplayPlan struct {
	ReplayID                string
	CandidateID             string
	CaseHash                string
	CandidateArtifactHash   string
	BaseArtifactHash        string
	ContextSnapshotHash     string
	WorkspaceImageHash      string
	EnvironmentManifestHash string
	ModelID                 string
	RuntimeID               string
	ToolPolicyVersion       string
	EvaluatorVersion        string
	RandomnessPolicyHash    string
	MinPairs                int
	MaxPairs                int
}

type ReplayTrialRequest struct {
	Plan    PairedReplayPlan
	Arm     ReplayArm
	Ordinal int
}

type ReplayObservation struct {
	InfrastructureValid bool
	Success             bool
	EvidenceHash        string
}

type PairedReplayTrial struct {
	ReplayID            string
	Arm                 ReplayArm
	Ordinal             int
	InfrastructureValid bool
	Success             bool
	EvidenceHash        string
}

type ReplayArmStats struct {
	Trials    int
	Successes int
	Mean      float64
	Variance  float64
}

type PairedReplayResult struct {
	ReplayID      string
	Conclusion    string
	With          ReplayArmStats
	Without       ReplayArmStats
	Passed        bool
	PolicyVersion string
}

type CandidateDecision struct {
	DecisionID      string
	CandidateID     string
	Decision        string
	CandidateDigest string
	ReplayResultID  string
	PolicyVersion   string
	Reason          string
	DecidedBy       PrincipalID
	DecidedAt       time.Time
}

type SkillActivation struct {
	ActivationID        string
	CandidateID         string
	DecisionID          string
	ReplayResultID      string
	CandidateDigest     string
	ExpectedBaseVersion int64
	NewArtifactVersion  int64
	ActivatedBy         PrincipalID
	ActivatedAt         time.Time
}

const (
	GrantOperationPatternRead       GrantOperation = "pattern.read"
	GrantOperationPatternReject     GrantOperation = "pattern.reject"
	GrantOperationProposalRead      GrantOperation = "proposal.read"
	GrantOperationCandidateRead     GrantOperation = "candidate.read"
	GrantOperationCandidateDecide   GrantOperation = "candidate.decide"
	GrantOperationCandidateActivate GrantOperation = "candidate.activate"
)
