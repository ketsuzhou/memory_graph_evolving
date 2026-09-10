# Pattern & Curation migration plan

## 1. Scope, authority, and package freeze

This document is the implementation authority for migrating the Pattern & Curation slice into `graph-memory-service` (GMS). The rulings in `/tmp/river2_0-gms-curation-handoff.md` override legacy implementation details. Existing Memory Protocol v1 behavior and all twelve existing routes remain frozen.

The slice contains four modules:

1. **M1 Causal Evidence Ledger**: append-only paired intervention trials, a verifiable payload-hash chain, governed witness transitions, and append-only estimate/reward revisions.
2. **M2 Dive Judge**: server-owned judgment of terminal exploration trajectories, citation-deduplicated information items, deterministic rewards, and one lifecycle read endpoint. The legacy worker and RL outbox do not migrate.
3. **M3 Consolidation Round**: server-only maintenance of immutable, versioned derived projections. It never mutates committed evidence.
4. **M4 Pattern Lifecycle + Skill Proposal**: governed pattern promotion, one atomic proposal per round, immutable rejection memory, paired replay, separate candidate decision and curator activation.

Only the Go standard library is allowed. Package names and dependency direction are frozen:

```text
internal/domain          shared value and durable-record types; imports standard library only
internal/ports           additive interfaces expressed only in domain types
internal/authz           existing exact Grant/purpose/expiry enforcement
internal/causal          package causal; M1 use case and pure witness/hash functions
internal/dive            package dive; M2 judge and pure reward functions
internal/consolidation   package consolidation; M3 shadow/replay/publication use case
internal/pattern         package pattern; M4 pattern policy and lifecycle
internal/skillproposal   package skillproposal; M4 proposer/candidate/replay/decision/activation
internal/httpapi         transport only; no domain authority
internal/store/memory    one adapter for all additive ports and full JSON snapshots
```

Use-case packages may import `domain`, `ports`, and `authz`; they must not import `httpapi` or a concrete store. `ports` must not import a use-case package. The memory adapter must not import `httpapi` or any use-case package. No package imports legacy `multica` code.

M1, M3, pattern mining, proposal generation, replay execution, and automatic Dive creation are internal services. M2 adds one read-only route. M4 adds only governed read/reject/decision/activation routes; no public route may manufacture causal evidence, consolidation operations, proposals, replay results, or an automatic activation.

## 2. Protocol and domain signatures

All signatures in this section are frozen. Implementations may add unexported helpers and fields but may not rename, remove, or weaken an exported declaration.

### 2.1 Shared grant additions

Append these values; do not alter existing values:

```go
const (
    GrantPurposeCuration GrantPurpose = "curation"

    GrantOperationCausalTrialAppend    GrantOperation = "causal.trial.append"
    GrantOperationCausalEstimateAppend GrantOperation = "causal.estimate.append"
    GrantOperationCausalRewardAppend   GrantOperation = "causal.reward.append"
    GrantOperationCausalRead           GrantOperation = "causal.read"
    GrantOperationDiveRead             GrantOperation = "dive.read"

    GrantOperationPatternRead       GrantOperation = "pattern.read"
    GrantOperationPatternReject     GrantOperation = "pattern.reject"
    GrantOperationProposalRead      GrantOperation = "proposal.read"
    GrantOperationCandidateRead     GrantOperation = "candidate.read"
    GrantOperationCandidateDecide   GrantOperation = "candidate.decide"
    GrantOperationCandidateActivate GrantOperation = "candidate.activate"
)
```

`POST /v1/grants` will accept `curation` and these exact operations additively. Existing lifecycle/tool-plane values and their two-way fence remain unchanged.

### 2.2 M1 Causal Evidence Ledger (`internal/causal`, package `causal`)

Domain declarations in `internal/domain`:

```go
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
    TenantID             TenantID
    SpaceID              SpaceID
    RewardID             CausalRewardID
    Revision             int64
    EstimateID           CausalEstimateID
    EstimateRevision     int64
    RewardVectorHash     string
    RewardVectorJSON     []byte
    RewardPolicyVersion  string
    Scalar               *float64
    HardGateDisposition  string
    PreviousHash         string
    PayloadHash          string
    CreatedAt            time.Time
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
```

Use-case declarations:

```go
var (
    ErrNotImplemented           = errors.New("causal evidence ledger is not implemented")
    ErrInvalidWitnessTransition = errors.New("invalid witness transition")
)

type Service struct { /* unexported dependencies */ }

func New(registry ports.RegistryStore, store ports.CausalEvidenceStore, clock ports.Clock) *Service
func (s *Service) AppendTrial(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, event domain.CausalTrialEvent) (domain.CausalTrialEvent, bool, error)
func (s *Service) AppendEstimateRevision(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, revision domain.CausalEstimateRevision) (domain.CausalEstimateRevision, bool, error)
func (s *Service) AppendRewardRevision(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, revision domain.CausalRewardRevision) (domain.CausalRewardRevision, bool, error)
func TrialPayloadHash(event domain.CausalTrialEvent) (string, error)
func EstimatePayloadHash(revision domain.CausalEstimateRevision) (string, error)
func RewardPayloadHash(revision domain.CausalRewardRevision) (string, error)
func ReviseWitness(input domain.WitnessTransitionInput, policy domain.WitnessPolicy) (domain.CausalEstimateRevision, error)
func EvaluateWitnessApplicability(revision domain.CausalEstimateRevision) (domain.WitnessApplicability, error)
```

The three hash functions use SHA-256 over compact UTF-8 JSON emitted from private hash-payload structs in the displayed field order. `PayloadHash` itself is excluded; `PreviousHash` is included. Times use UTC RFC3339Nano. Slices preserve order and must be non-nil where required. For `TrialPayloadHash`, the exact JSON keys are:

```text
event_id, tenant_id, space_id, experiment_id, arm, ordinal,
context_hash, intervention_hash, outcome_authority_hash,
infrastructure_valid, success, effect, evidence_refs, previous_hash, created_at
```

Each `evidence_refs` object uses `batch_id,event_ids` in that order. Estimate and reward hashes follow their Go field order with lower-snake-case keys. This canonical form is a durable contract, not ordinary struct JSON.

### 2.3 M2 Dive Judge (`internal/dive`, package `dive`)

Domain declarations:

```go
type DiveClassification string

const (
    DiveFound           DiveClassification = "found"
    DiveMiss            DiveClassification = "miss"
    DiveError           DiveClassification = "error"
    DiveBudgetExhausted DiveClassification = "budget_exhausted"
    DiveTimeout         DiveClassification = "timeout"
)

type DiveDimensions struct {
    Relevance   float64
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
```

Use-case declarations:

```go
var ErrNotImplemented = errors.New("dive judge is not implemented")

type Scorer interface {
    Score(context.Context, domain.DiveTrajectory) (domain.DiveDimensions, error)
}

type Judge struct { /* unexported dependencies */ }

func New(store ports.DiveStore, scorer Scorer, clock ports.Clock) *Judge
func (j *Judge) JudgeSubmitted(ctx context.Context, tenantID domain.TenantID, sessionID domain.ExplorationSessionID) (domain.DiveResult, bool, error)
func ClassifyTrajectory(trajectory domain.DiveTrajectory) (domain.DiveClassification, error)
func BuildInformationItems(trajectory domain.DiveTrajectory) ([]domain.DiveInformationItem, error)
func OverallScore(dimensions domain.DiveDimensions) (float64, error)
func DeterministicViolationReward(wRound float64, rounds int) float64
func DefaultConsolidationRewardWeights() domain.ConsolidationRewardWeights
func ConsolidationReward(input domain.ConsolidationRewardInput, weights domain.ConsolidationRewardWeights) (float64, domain.RewardComponents)
```

`Scorer` is an internal deterministic/evaluator seam, not an HTTP or RL-worker seam. It receives only the server-owned trajectory. It must return finite dimensions in `[0,1]`; `OverallScore` is their minimum. Found and miss trajectories are scored. Error, budget-exhausted, and timeout trajectories bypass the scorer and use `DeterministicViolationReward` where a reward is needed.

### 2.4 M3 Consolidation Round (`internal/consolidation`, package `consolidation`)

The following records are canonically declared in `internal/domain`. Package `consolidation` exports same-name type aliases for them so callers use `consolidation.ProjectionVersion`, `consolidation.RoundInput`, and the other names shown below without creating `ports -> consolidation -> ports`. The displayed struct bodies are the canonical `domain` definitions; package-qualified field types indicate already-existing domain types.

```go
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
    EvidenceRefs []domain.EvidenceRef
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
    EvidenceRefs []domain.EvidenceRef
}

type DerivedProjection struct {
    TenantID          domain.TenantID
    SpaceID           domain.SpaceID
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
    TenantID          domain.TenantID
    SpaceID           domain.SpaceID
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
    SpaceID               domain.SpaceID
    CommittedBatchOrdinal int64
    QueryOrdinal          int64
}

type ConsolidationCursor struct {
    SpaceID                   domain.SpaceID
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

func DefaultConfig() Config
func ShouldConsolidate(newCommittedBatches, queriesSinceLast int64, cfg Config) bool

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
    EvidenceRefs []domain.EvidenceRef
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

type ReplayRunner interface {
    Evaluate(ctx context.Context, base, candidate DerivedProjection, plan RetrievalReplayPlan, cfg BacktestConfig) (CandidateStats, error)
}

type RoundInput struct {
    TenantID          domain.TenantID
    SpaceID           domain.SpaceID
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
    TenantID       domain.TenantID
    SpaceID        domain.SpaceID
    Round          RoundResult
    Projection     DerivedProjection
    ExpectedHead   ProjectionHead
    ActivityCursor ConsolidationCursor
}

type Service struct { /* unexported dependencies */ }

func New(store ports.ConsolidationStore, replay ReplayRunner, clock ports.Clock, cfg Config) *Service
func (s *Service) Run(ctx context.Context, input RoundInput) (result RoundResult, duplicate bool, err error)
func SelectWinner(candidates []CandidateStats, weights CostWeights) int
```

`DefaultConfig` freezes committed-batch threshold `50`, query threshold `200`, recall regression tolerance `0.02`, rounds tolerance `1`, and weights `{Round:1.0, Tail:0.5, Embed:0.2, Node:0.1, Graph:0.05}`. `ShouldConsolidate` is inclusive OR.

### 2.5 M4 Pattern Lifecycle (`internal/pattern`, package `pattern`)

The following records are canonically declared in `internal/domain`; package `pattern` exports same-name aliases and owns only the lifecycle functions. This keeps the additive storage ports below the use case while preserving the public names shown here.

```go
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
    EstimateID domain.CausalEstimateID
    Revision   int64
}

type PatternEvidence struct {
    EvidenceRef      domain.EvidenceRef
    SourceSpaceID    domain.SpaceID
    LineageID        string
    Strength         PatternEvidenceStrength
    CausalEstimate   *CausalEstimateRef
}

type PatternRevision struct {
    TenantID          domain.TenantID
    SpaceID           domain.SpaceID
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
    CreatedBy         domain.PrincipalID
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
    TenantID          domain.TenantID
    SpaceID           domain.SpaceID
    PatternID         string
    Problem           string
    Applicability     string
    RecommendedAction string
    Evidence          []PatternEvidence
    Actor              domain.PrincipalID
    CreatedAt          time.Time
}

func DraftPattern(input DraftPatternInput) (PatternRevision, error)
func AssessPattern(evidence []PatternEvidence, policy PatternPolicy, estimates map[CausalEstimateRef]domain.CausalEstimateRevision) (PatternAssessment, error)
func AdvancePattern(current PatternRevision, assessment PatternAssessment, actor domain.PrincipalID, at time.Time) (PatternRevision, error)
func RejectPattern(current PatternRevision, reason string, actor domain.PrincipalID, at time.Time) (PatternRevision, error)
```

### 2.6 M4 Skill Proposal (`internal/skillproposal`, package `skillproposal`)

The following records are canonically declared in `internal/domain`; package `skillproposal` exports same-name aliases and owns the evaluator/service functions and interfaces.

```go
type ProposalFingerprint string

type ProposalFingerprintInput struct {
    TargetSkillID         string
    NewSkillName          string
    BaseArtifactVersion   int64
    BaseArtifactHash      string
    CandidateArtifactHash string
    ReviewedDiffHash      string
    RequestedScope        string
    PatternRefs           []pattern.PatternRef
}

type SkillProposal struct {
    ProposalID  string
    RoundID     string
    Fingerprint ProposalFingerprint
    PatternRefs []pattern.PatternRef
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

func FingerprintProposal(input ProposalFingerprintInput) (ProposalFingerprint, error)
func ValidateRoundOutcome(outcome ProposalRoundOutcome) error

type ReviewedDiff struct {
    DiffID                string
    BaseArtifactVersion   int64
    BaseArtifactHash      string
    CandidateArtifactHash string
    DiffHash              string
    Reviewer              domain.PrincipalID
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
    TenantID           domain.TenantID
    SpaceID            domain.SpaceID
    CandidateID        string
    ProposalID         string
    Fingerprint        ProposalFingerprint
    TargetSkillID      string
    NewSkillName       string
    BaseArtifactVersion int64
    Diff               ReviewedDiff
    PatternRefs        []pattern.PatternRef
    Status             CandidateStatus
    ReplayResultID     string
    CreatedAt          time.Time
}

type ReplayArm string

const (
    WithSkill    ReplayArm = "with_skill"
    WithoutSkill ReplayArm = "without_skill"
)

type PairedReplayPlan struct {
    ReplayID               string
    CandidateID            string
    CaseHash               string
    CandidateArtifactHash  string
    BaseArtifactHash       string
    ContextSnapshotHash    string
    WorkspaceImageHash     string
    EnvironmentManifestHash string
    ModelID                string
    RuntimeID              string
    ToolPolicyVersion      string
    EvaluatorVersion       string
    RandomnessPolicyHash   string
    MinPairs               int
    MaxPairs               int
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
    ReplayID           string
    Arm                ReplayArm
    Ordinal            int
    InfrastructureValid bool
    Success            bool
    EvidenceHash       string
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

type PairedReplayRunner interface {
    RunTrial(context.Context, ReplayTrialRequest) (ReplayObservation, error)
}

type PairedEvaluator struct { /* unexported dependencies */ }

func NewPairedEvaluator(runner PairedReplayRunner, clock ports.Clock) *PairedEvaluator
func (e *PairedEvaluator) Evaluate(context.Context, PairedReplayPlan) (PairedReplayResult, error)

type CandidateDecision struct {
    DecisionID     string
    CandidateID    string
    Decision       string
    CandidateDigest string
    ReplayResultID string
    PolicyVersion  string
    Reason         string
    DecidedBy      domain.PrincipalID
    DecidedAt      time.Time
}

type SkillActivation struct {
    ActivationID       string
    CandidateID        string
    DecisionID         string
    ReplayResultID     string
    CandidateDigest    string
    ExpectedBaseVersion int64
    NewArtifactVersion int64
    ActivatedBy        domain.PrincipalID
    ActivatedAt        time.Time
}

type Service struct { /* unexported dependencies */ }

func New(registry ports.RegistryStore, patterns ports.PatternStore, proposals ports.ProposalStore, candidates ports.CandidateStore, clock ports.Clock) *Service
func (s *Service) CompleteRound(context.Context, domain.TenantID, domain.SpaceID, ProposalRoundOutcome) (ProposalRoundOutcome, bool, error)
func (s *Service) Decide(context.Context, domain.TenantID, domain.PrincipalID, domain.SpaceID, CandidateDecision) (CandidateDecision, bool, error)
func (s *Service) Activate(context.Context, domain.TenantID, domain.PrincipalID, domain.SpaceID, SkillActivation) (SkillActivation, bool, error)
```

## 3. HTTP protocol additions

All additions inherit bearer authentication, server-resolved Tenant/Principal, 1–128-byte opaque IDs, strict deep JSON, no duplicate/trailing values, request-ID error envelopes, and semantic `201/200/409` behavior. Unknown query parameters are rejected. Existing routes are unchanged.

### 3.1 Dive read

`GET /v1/explorations/{session_id}/dive?request_id={request_id}` requires lifecycle `dive.read` over every Space pinned by the session. The service loads the session first only to recover its server-owned exact Space set; authorization is then rechecked. A tool-plane grant cannot cover this read.

Success `200`:

```json
{
  "request_id": "opaque",
  "session_id": "opaque",
  "classification": "found|miss|error|budget_exhausted|timeout",
  "score": {
    "relevance": 0.0,
    "groundedness": 0.0,
    "completeness": 0.0,
    "overall": 0.0,
    "rounds": 0
  },
  "items": [{
    "citation_id": "opaque",
    "statement": "text",
    "source_space_id": "opaque",
    "memory_version": 1,
    "evidence_batch_id": "opaque",
    "event_ids": ["opaque"],
    "retrieval_score": 0.0,
    "submitted": true,
    "authoritative": true
  }],
  "incomplete": false,
  "evaluated_at": "UTC RFC3339Nano"
}
```

For `error`, `budget_exhausted`, and `timeout`, `score` is JSON `null`; the durable Go score retains classification/session/rounds and zero dimensions. Unknown result is `404 DIVE_NOT_FOUND`; active session is `409 EXPLORATION_NOT_TERMINAL`.

### 3.2 M4 governed curation routes

The exact route set is:

```text
GET  /v1/patterns/{pattern_id}?space_id={space_id}&request_id={request_id}       pattern.read
POST /v1/patterns/{pattern_id}:reject                                           pattern.reject
GET  /v1/proposals/{proposal_id}?space_id={space_id}&request_id={request_id}     proposal.read
GET  /v1/candidates/{candidate_id}?space_id={space_id}&request_id={request_id}   candidate.read
POST /v1/candidates/{candidate_id}:decide                                       candidate.decide
POST /v1/candidates/{candidate_id}:activate                                     candidate.activate
```

Reject request: `request_id`, `space_id`, `operation_id`, `expected_revision`, `reason`. Decide request: `request_id`, `space_id`, `decision_id`, `decision` (`accepted|rejected`), `candidate_digest`, `replay_result_id`, `policy_version`, `reason`. Activate request: `request_id`, `space_id`, `activation_id`, `decision_id`, `replay_result_id`, `candidate_digest`, `expected_base_version`. Every field is required. Responses return the complete stored revision/decision/activation plus `duplicate`. First reject/decision/activation is `201`; identical replay is `200`; changed content under the same key is `409 IDEMPOTENCY_CONFLICT`.

Activation additionally requires an authenticated `PrincipalHuman`, a fresh exact-Space curation grant, accepted decision, passing/current paired replay and mandatory backtest, unchanged candidate digest, available source evidence, no retraction/quarantine, and CAS on the expected active artifact version. There is no automatic activation call path.

## 4. Purpose × operation matrix

| Operation | Purpose | Caller/route | Exact Space rule |
|---|---|---|---|
| `evidence.stage` | `lifecycle` | existing route | target Space |
| `evidence.commit` | `lifecycle` | existing route | batch Space |
| `recall` | `lifecycle` | existing route | complete requested set |
| `dive.read` | `lifecycle` | M2 GET | complete session-pinned set |
| `exploration.start` | `tool_plane` | existing route | complete requested set |
| `exploration.explore` | `tool_plane` | existing route | complete session-pinned set |
| `exploration.redirect` | `tool_plane` | existing route | complete session-pinned set |
| `exploration.submit` | `tool_plane` | existing route | complete session-pinned set |
| `causal.trial.append` | `curation` | M1 internal service | record Space and all evidence source Spaces |
| `causal.estimate.append` | `curation` | M1 internal service | estimate Space and all source-trial Spaces |
| `causal.reward.append` | `curation` | M1 internal service | reward Space and referenced estimate Space |
| `causal.read` | `curation` | internal M4 verification | all referenced Spaces |
| `pattern.read` | `curation` | M4 GET | pattern Space and declared source Spaces |
| `pattern.reject` | `curation` | M4 POST | pattern Space and declared source Spaces |
| `proposal.read` | `curation` | M4 GET | proposal target Space |
| `candidate.read` | `curation` | M4 GET | candidate target Space |
| `candidate.decide` | `curation` | M4 POST | candidate target Space and all source Spaces |
| `candidate.activate` | `curation` | M4 POST | candidate target Space and all source Spaces |

M3 runs under server authority and has no caller Grant operation. Its source-space, provenance, retraction, and CAS gates are still mandatory. Internal server authority is not reusable by HTTP callers.

## 5. Additive port extensions

These interfaces are added in new `internal/ports` files. Existing interfaces are not modified.

### 5.1 M1 and M2

```go
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

type DiveStore interface {
    DiveTrajectory(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.DiveTrajectory, error)
    PutDiveResult(context.Context, domain.DiveResult) (stored domain.DiveResult, duplicate bool, err error)
    DiveResult(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.DiveResult, error)
}
```

### 5.2 M3

```go
type ConsolidationStore interface {
    Activity(context.Context, domain.TenantID, domain.SpaceID) (domain.ConsolidationActivity, error)
    ProjectionHead(context.Context, domain.TenantID, domain.SpaceID) (domain.ProjectionHead, error)
    Projection(context.Context, domain.TenantID, domain.SpaceID, domain.ProjectionVersion) (domain.DerivedProjection, error)
    Round(context.Context, domain.TenantID, domain.SpaceID, domain.ConsolidationRoundID) (domain.RoundResult, bool, error)
    PublishRound(context.Context, domain.ConsolidationPublishInput) (domain.RoundResult, bool, error)
}

type RetrievalReplayStore interface {
    PutReplayPlan(context.Context, domain.RetrievalReplayPlan) error
    PutReplayResult(context.Context, domain.ConsolidationRoundID, domain.CandidateStats) error
}
```

The aliases frozen in section 2.4 make these domain records identical to the `consolidation` package surface without a dependency cycle.

### 5.3 M4

```go
type PatternStore interface {
    AppendPatternRevision(context.Context, domain.PatternRevision, string) (domain.PatternRevision, bool, error)
    LatestPatternRevision(context.Context, domain.TenantID, domain.SpaceID, string) (domain.PatternRevision, error)
}

type ProposalStore interface {
    CompleteProposalRound(context.Context, domain.TenantID, domain.SpaceID, domain.ProposalRoundOutcome) (domain.ProposalRoundOutcome, bool, error)
    ProposalByFingerprint(context.Context, domain.TenantID, domain.SpaceID, domain.ProposalFingerprint) (domain.SkillProposal, error)
    PutRejection(context.Context, domain.TenantID, domain.SpaceID, domain.RejectionMemory) (bool, error)
    RejectionByFingerprint(context.Context, domain.TenantID, domain.SpaceID, domain.ProposalFingerprint) (domain.RejectionMemory, error)
}

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
```

The aliases frozen in sections 2.5–2.6 preserve the `pattern` and `skillproposal` package API while all port signatures remain expressed only in domain types.

## 6. Idempotency keys and semantic equality

| Record/action | Key scope | Identical replay | Changed replay |
|---|---|---|---|
| M1 trial | `(tenant, space, event_id)` | original record, `duplicate=true` | `409`, no mutation |
| M1 estimate revision | `(tenant, space, estimate_id, revision)` | original revision, `duplicate=true` | `409`, no mutation |
| M1 reward revision | `(tenant, space, reward_id, revision)` | original revision, `duplicate=true` | `409`, no mutation |
| M2 judgment | `(tenant, session_id)` | original result, `duplicate=true` | `409`; server trajectory digest mismatch is corruption/conflict |
| M3 round | `(tenant, space, round_id)` plus ordered operation digest | original terminal result/version | `409`; never rebase |
| M4 pattern revision | `(tenant, space, pattern_id, revision)` | original revision | `409` |
| M4 proposal round | `(tenant, space, round_id)` | original proposal/no-action | `409` |
| M4 proposal | unique `(tenant, space, fingerprint)` | original proposal | rejected fingerprint can never be resubmitted |
| M4 candidate | `(tenant, space, candidate_id)` and one candidate per proposal | original candidate | `409` |
| paired replay plan | `(candidate_id, replay_id)` | original plan | `409` |
| paired replay trial | `(replay_id, arm, ordinal)` | original observation | `409` |
| paired replay result | `replay_id` | original result | `409` |
| candidate decision | candidate plus one terminal decision; request key `decision_id` | original decision | `409` |
| activation | `(candidate_id, activation_id)` plus expected-version CAS | original artifact version | `409`; no second version |

Semantic equality compares decoded meaning, not input byte formatting. Derived fields (`PayloadHash`, generated timestamps/versions, duplicate flags) are excluded from request equality. Ordered operation/trial/evidence sequences remain order-sensitive. Pattern refs are canonicalized by `(pattern_id,revision)` for fingerprinting.

## 7. Store and snapshot fields

`internal/store/memory.Store` gains maps under its existing mutex. `snapshot` gains matching exported JSON fields. Restore must initialize absent fields to empty maps so snapshots from the tracer remain loadable. Snapshot writes retain temp-file → fsync → rename behavior.

M1 fields:

```text
causal_trials[tenant][space][event_id]
causal_trial_order[tenant][space]
causal_trial_heads[tenant][space]
causal_estimates[tenant][space][estimate_id][revision]
causal_estimate_latest[tenant][space][estimate_id]
causal_estimate_heads[tenant][space][estimate_id]
causal_rewards[tenant][space][reward_id][revision]
causal_reward_latest[tenant][space][reward_id]
causal_reward_heads[tenant][space][reward_id]
```

M2 fields:

```text
dive_trajectories[session_id]
dive_served_order[session_id]          // first-served citation order; existing map is retained for lookup
dive_results[tenant][session_id]
dive_trajectory_digests[session_id]
```

M3 fields:

```text
consolidation_activity[tenant][space]
consolidation_cursors[tenant][space]
projection_heads[tenant][space]
projection_versions[tenant][space][projection_version]
consolidation_rounds[tenant][space][round_id]
retrieval_replay_plans[plan_id]
retrieval_replay_results[round_id]
```

M4 fields:

```text
pattern_revisions[tenant][space][pattern_id][revision]
pattern_latest[tenant][space][pattern_id]
proposal_rounds[tenant][space][round_id]
proposals[tenant][space][proposal_id]
proposal_fingerprint_index[tenant][space][fingerprint]
rejection_memory[tenant][space][fingerprint]
reviewed_diffs[diff_id]
candidates[tenant][space][candidate_id]
candidate_by_proposal[proposal_id]
paired_replay_plans[replay_id]
paired_replay_trials[replay_id][arm][ordinal]
paired_replay_results[replay_id]
mutation_backtest_results[candidate_id]
candidate_decisions[candidate_id]
skill_activations[candidate_id][activation_id]
active_skill_versions[target_skill_id]
```

All slices and nested objects are deep-cloned on read/write. Payload and chain hashes are reverified during restore; corrupt or disconnected chains fail closed. Published projection versions, revisions, rejection memories, trials, decisions, and activations are immutable.

## 8. Behavioral invariants

### 8.1 M1

- Every write calls `AuthorizeExact` before touching the causal store. Caller-provided Tenant, Principal, purpose, conclusion, or strength never substitutes for server authority.
- Tool-plane and lifecycle principals cannot append causal records even if their Grant operation list contains a causal operation. Only exact active curation Grants count.
- Trial arms are paired by experiment/context/intervention pins; ordinals are positive and unique per arm. Infrastructure-invalid observations are retained but cannot support a conclusive estimate.
- The first record in a chain has empty `PreviousHash`; every later record names the current head. Append verifies the supplied/computed payload hash and advances the head atomically. Reorder, deletion, mutation, or stale predecessor is detectable and rejected.
- Estimates and rewards append revisions; they never overwrite. Revisions start at one and increase exactly by one. Source trial digest and exact estimate revision references are immutable.
- Only conclusive estimates can carry a witness. Legal status edges are `none→provisional`, `provisional→active|contested|retired`, `active→contested|retired`, and `contested→retired`. Retired is terminal; same-revision mutation, skipping, reversal, and resurrection return `ErrInvalidWitnessTransition`.
- Paired witness creates provisional. Independent support requires the policy lineage minimum and lower interval strictly greater than `Delta`. Nonpositive interval and mandatory regression failure contest. Retraction, supersession, or passing the negative retirement gate retire.
- Provisional/active are historically auditable, promotion eligible, and recommendation eligible. Contested/retired remain historically auditable but fail closed for promotion/recommendation. None is ineligible for all three.
- Passed reward gates require a scalar; non-passed gates forbid one. Reward vector JSON is strict, canonical, bounded, and hash-verified.

### 8.2 M2

- Judgment uses only the durable server trajectory. There is no client trajectory replay and no RL worker/outbox.
- An exploration submission stores the terminal trajectory and Dive result atomically with the exploration operation. Identical submit replay cannot create another judgment.
- Submitted `found=true` classifies as `found`; submitted `found=false` as `miss`. Durable terminal failure reasons map only to `error`, `budget_exhausted`, or `timeout`. Legacy `budget` is accepted only inside a migration adapter and is persisted as `budget_exhausted`.
- Found/miss require one finite score with every dimension in `[0,1]`; `overall=min(relevance, groundedness, completeness)`. Violation classes bypass scoring.
- Information items deduplicate by `Citation.ID`, preserve first-served order, and preserve the first server-stored Recall item for that citation. Different citation IDs remain different even when they name the same batch. `Submitted` is true when the deduplicated submitted-ID set contains the citation.
- Items from a terminal found/miss judgment are authoritative; violation/incomplete judgments are non-authoritative. A never-served or foreign citation cannot appear.
- `DeterministicViolationReward(wRound, rounds)` coerces rounds to at least one and returns `-1 - wRound*rounds`.
- Default consolidation reward weights are `{Quality:1.0, Efficiency:0.01}`. Gate failure returns `-1` and source `hard_gate_failure`. Otherwise quality and efficiency formulas match the frozen legacy formulas, including `EmbedBytes/1024`.
- `dive.read` is lifecycle-only, read-only, exact-Space authorized, stable across retries/restarts, and never mutates exploration or reward state.

### 8.3 M3

- Consolidation reads committed evidence only. Staged evidence is invisible. `Space.Version` remains the evidence watermark and is never reused as projection version.
- Trigger is `new committed batches >= 50 OR recall/explore queries >= 200`. Count one successfully authorized logical query per involved Space; idempotent replay does not count again. Submit is not a query.
- A round captures evidence/query high-water marks and clones the published projection. Operations apply only to the shadow. Failed/rejected rounds do not advance cursors or change the head.
- The closed operation set is exactly the eight constants above. `merge_nodes` is plural. No `delete_edge`, `update_edge`, public CRUD, or post-submit operation is accepted.
- Update/merge provenance is a monotonic union. No operation can create a ref to staged, cross-Tenant, cross-Space, post-watermark, retracted, or quarantined evidence.
- Hierarchy edges form a strict DAG; relation edges may cycle. All endpoints exist and IDs are unique. Merge retains input nodes as superseded lineage and creates deterministic lineage relations.
- Retrieval Replay runs the same frozen query/case set against current and candidate projections under identical policy/budgets. Every required item is AND-required; node alternatives within one item are OR-equivalent. Infrastructure-invalid cases fail publication closed and are not task-negative evidence.
- Candidate recall must be at least baseline recall minus `0.02` (inclusive boundary). Hard structural/provenance/retraction gates cannot be compensated by cost. Costs and replay components are durable audit data.
- Publication atomically writes one immutable projection, CAS-moves the head, advances captured cursors, and finalizes the round. Same round ID and operation digest returns the same version; changed digest conflicts; stale base is rejected without rebase.
- Existing recall route behavior remains unchanged until a separately reviewed wiring step selects derived projection retrieval. Even then, citations resolve to immutable evidence.

### 8.4 M4

- Pattern lifecycle is one-way `tentative→supported→proposal_eligible`; any live state can become explicit terminal `rejected`. No implicit downgrade or resurrection.
- Supported requires repeat evidence from at least two independent lineages. Proposal eligibility additionally requires contrastive or verified causal support, defined applicability, and policy coverage.
- Causal strength requires an exact M1 estimate revision that is conclusive, currently applicable, paired-trial backed, and authorized in every source Space. An agent/LLM label is ignored and cannot create causal strength.
- Each proposal round produces exactly one complete atomic proposal or no action. Fingerprint canonicalization excludes prose, actor, and timestamps and includes material artifact/diff/scope/pattern identity.
- Existing or rejected fingerprint cannot be proposed again. Rejection memory is immutable; a material change must produce a different fingerprint.
- One proposal equals one candidate equals exactly one target/new skill, one base artifact version, and one reviewed diff. New skills use base version zero and the canonical empty-base hash.
- Paired replay pins every condition; arms differ only by skill presence. Infrastructure failures are invalid/inconclusive, never negative skill evidence. Partial pairs cannot pass.
- `accepted` is a proposal decision only. It does not alter an active artifact, binding, Grant, or deployment.
- Activation is a separate, human-curator, curation-purpose, freshly authorized CAS. It requires accepted decision, passing/current paired replay and mandatory backtest, exact candidate digest, source availability, no retraction/quarantine, and expected base version. No automatic activation exists.

## 9. Test strategy

Tests use `testing`, table-driven cases, subtests, deterministic clocks, in-memory/fake adapters, and `httptest`; no third-party dependency.

This delivery freezes and intentionally leaves red only M1/M2 tests:

- `internal/causal/service_test.go`: tool-plane fence and positive curation Grant, canonical trial payload/hash predecessor, legal provisional witness, illegal active skip, retired resurrection rejection.
- `internal/dive/judge_test.go`: all five classifications, first-served citation dedup/order/source identity, weakest-dimension overall, deterministic violation penalty, consolidation components, hard-gate non-compensation.

The implementing delivery adds without modifying frozen tests:

- M1 append replay/conflict for trials/estimate/reward; chain corruption/reorder/stale-head rejection; revision history/latest; strict vector validation; expiry/cross-Space fence; snapshot round trip.
- M2 submit auto-judgment and replay idempotency; scorer range/NaN/Inf rejection; foreign citation fence; lifecycle GET strict query/error envelope/purpose; snapshot round trip.
- M3 threshold boundaries, query accounting/retry, all operations, DAG/provenance gates, recall tolerance boundary, infrastructure failure, failed shadow discard, same-round same-version idempotency, stale CAS, snapshot round trip.
- M4 lifecycle/causal verification, one-proposal round, fingerprint/rejection dedup, paired replay pinning, decision versus activation, human-curator/curation fence, stale CAS, no automatic activation, snapshot round trip.

Existing tests are never edited. Snapshot coverage is added in new `internal/store/memory/*_test.go` files rather than modifying `snapshot_test.go`.

## 10. Acceptance mapping

| Acceptance | Contract/tests |
|---|---|
| Four modules have signature-level plan | Sections 1–8 of this document |
| M1 causal append-only/hash chain/revisions | `TestTrialPayloadHashFreezesCanonicalPayloadAndPredecessor`; future append/revision/snapshot tests |
| M1 witness state machine | `TestWitnessLifecycleAllowsOnlyGovernedForwardTransitions` |
| M1 ADR 0038 fence | `TestCausalEstimateAppendRequiresCurationPurpose` plus existing two-way purpose tests |
| M2 five classifications | `TestClassifyTrajectoryUsesOnlyFrozenTerminalClasses` |
| M2 citation dedup/source identity | `TestBuildInformationItemsDeduplicatesCitationByFirstServedOrder` |
| M2 pure rewards | `TestDiveRewardsAreDeterministicAndNonCompensating` |
| M2 lifecycle read and no worker | future HTTP contract test; package/route freeze in sections 1 and 3 |
| M3 immutable projection/replay rejection | future shadow, regression, CAS, and evidence immutability tests |
| M3 round idempotency | future same-round/same-version test |
| M4 lifecycle/dedup | future status/fingerprint/rejection-memory tests |
| M4 activation gate | future human-curator, curation Grant, replay/backtest, and CAS tests |
| All durable state survives restart | four-module additive memory snapshot round-trip tests |
| Existing protocol does not regress | unchanged existing tests and route conformance tests |

## 11. Done criteria

The migration slice is done only when:

1. Every frozen signature and package name in this plan exists without dependency cycles.
2. Existing twelve routes and all pre-migration tests remain unchanged and green.
3. All M1–M4 frozen and implementation-added tests are green; no frozen test was edited.
4. `curation` is additive, lifecycle/tool-plane fencing remains bidirectional, and every operation follows the matrix.
5. M1 records and revisions are immutable, chain-verifiable, authorization-fenced, and snapshot durable.
6. M2 judgment is automatic/idempotent from server trajectory, citation-deduplicated, lifecycle-readable, and snapshot durable, with no worker/RL outbox.
7. M3 never mutates evidence, publishes only replay-passing CAS-safe projections, rejects regressions/stale rounds, and is idempotent/durable.
8. M4 cannot self-assign causal strength, emits at most one proposal per round, permanently remembers rejected fingerprints, and separates acceptance from curator activation.
9. New HTTP surfaces pass strict JSON/query, opaque-ID, error-envelope, authorization, idempotency, and OpenAPI conformance tests.
10. `go test ./...`, `go vet ./...`, and `go build ./...` pass with only the standard library.
