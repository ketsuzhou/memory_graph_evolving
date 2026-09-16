package consolidation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

// The derived projection is the only consolidation surface: committed
// evidence is immutable, so every operation mutates a shadow copy and only a
// replay-passing candidate ever becomes the published head.

type ProjectionVersion = domain.ProjectionVersion
type ProjectionNodeID = domain.ProjectionNodeID
type ProjectionEdgeID = domain.ProjectionEdgeID
type ConsolidationRoundID = domain.ConsolidationRoundID
type EdgeKind = domain.EdgeKind
type ProjectionNode = domain.ProjectionNode
type ProjectionEdge = domain.ProjectionEdge
type DerivedProjection = domain.DerivedProjection
type ProjectionHead = domain.ProjectionHead
type OperationKind = domain.OperationKind
type Operation = domain.Operation
type ConsolidationActivity = domain.ConsolidationActivity
type ConsolidationCursor = domain.ConsolidationCursor
type CostWeights = domain.CostWeights
type Config = domain.Config
type ReplayPartition = domain.ReplayPartition
type BacktestItem = domain.BacktestItem
type RetrievalReplayCase = domain.RetrievalReplayCase
type RetrievalReplayPlan = domain.RetrievalReplayPlan
type ReplayDisposition = domain.ReplayDisposition
type QueryBacktestStat = domain.QueryBacktestStat
type CandidateStats = domain.CandidateStats
type BacktestConfig = domain.BacktestConfig
type RoundInput = domain.RoundInput
type RoundOutcome = domain.RoundOutcome
type OperationRejection = domain.OperationRejection
type RoundResult = domain.RoundResult
type ConsolidationPublishInput = domain.ConsolidationPublishInput

const (
	EdgeHierarchy = domain.EdgeHierarchy
	EdgeRelation  = domain.EdgeRelation

	OperationAddNode          = domain.OperationAddNode
	OperationUpdateNode       = domain.OperationUpdateNode
	OperationDeleteNode       = domain.OperationDeleteNode
	OperationMergeNodes       = domain.OperationMergeNodes
	OperationAddHierarchyEdge = domain.OperationAddHierarchyEdge
	OperationAddRelationEdge  = domain.OperationAddRelationEdge
	OperationPruneEdge        = domain.OperationPruneEdge
	OperationSubmit           = domain.OperationSubmit

	ReplayEvaluation = domain.ReplayEvaluation
	ReplayHoldout    = domain.ReplayHoldout
	ReplaySafety     = domain.ReplaySafety

	ReplayMeasured              = domain.ReplayMeasured
	ReplaySkipped               = domain.ReplaySkipped
	ReplayIneligible            = domain.ReplayIneligible
	ReplayInfrastructureInvalid = domain.ReplayInfrastructureInvalid

	RoundPublished = domain.RoundPublished
	RoundRejected  = domain.RoundRejected
	RoundFailed    = domain.RoundFailed
)

// DefaultConfig freezes the bootstrap thresholds: consolidation becomes
// eligible after 50 committed batches or 200 queries, recall may regress at
// most 0.02, rounds at most 1, and candidate costs use the frozen weights.
func DefaultConfig() Config {
	return domain.Config{
		TriggerCommittedBatches: 50,
		TriggerQueries:          200,
		RecallTolerance:         0.02,
		RoundsTolerance:         1,
		CostWeights:             domain.CostWeights{Round: 1, Tail: 0.5, Embed: 0.2, Node: 0.1, Graph: 0.05},
	}
}

// ShouldConsolidate is inclusive OR: crossing either threshold suffices.
func ShouldConsolidate(newCommittedBatches, queriesSinceLast int64, cfg Config) bool {
	return newCommittedBatches >= cfg.TriggerCommittedBatches || queriesSinceLast >= cfg.TriggerQueries
}

type ReplayRunner interface {
	Evaluate(context.Context, DerivedProjection, DerivedProjection, RetrievalReplayPlan, BacktestConfig) (CandidateStats, error)
}

type Service struct {
	store  ports.ConsolidationStore
	replay ReplayRunner
	clock  ports.Clock
	cfg    Config
}

func New(store ports.ConsolidationStore, replay ReplayRunner, clock ports.Clock, cfg Config) *Service {
	return &Service{store: store, replay: replay, clock: clock, cfg: cfg}
}

// Run executes one consolidation round: replay an already-recorded round by
// ID, apply the operation list to a shadow copy of the published projection,
// gate the candidate through Retrieval Replay, and publish exactly once under
// compare-and-swap on the head. Rejected operations and failed replays never
// touch the published projection.
func (s *Service) Run(ctx context.Context, input RoundInput) (RoundResult, bool, error) {
	// The request digest covers the full submitted operation list, including
	// post-submit operations, so a replayed round ID with a different body can
	// never be mistaken for the original request.
	requestDigest := digestOperations(input.Operations)
	if existing, found, err := s.store.Round(ctx, input.TenantID, input.SpaceID, input.RoundID); err == nil && found {
		if existing.OperationDigest != requestDigest {
			return RoundResult{RoundID: input.RoundID, Outcome: RoundFailed, BaseVersion: input.BaseVersion, OperationDigest: requestDigest}, true,
				fmt.Errorf("consolidation round %s: stored round was recorded with a different operation list", input.RoundID)
		}
		return existing, true, nil
	}

	head, err := s.store.ProjectionHead(ctx, input.TenantID, input.SpaceID)
	if err != nil {
		return RoundResult{}, false, err
	}
	if head.Version != input.BaseVersion || head.Digest != input.BaseDigest {
		return RoundResult{RoundID: input.RoundID, Outcome: RoundFailed, BaseVersion: input.BaseVersion}, false,
			fmt.Errorf("consolidation round %s: base %d/%s is not the published head %d/%s", input.RoundID, input.BaseVersion, input.BaseDigest, head.Version, head.Digest)
	}
	base, err := s.store.Projection(ctx, input.TenantID, input.SpaceID, input.BaseVersion)
	if err != nil {
		return RoundResult{}, false, err
	}

	shadow := cloneProjection(base)
	var rejections []OperationRejection
	submitted := false
	for index, operation := range input.Operations {
		if submitted {
			// Submit closes the round's operation list; anything after it is a
			// durable rejection, never a silently dropped mutation.
			rejections = append(rejections, OperationRejection{Kind: operation.Kind, Index: index, Reason: "operation follows submit"})
			continue
		}
		if rejection, ok := applyOperation(&shadow, operation); !ok {
			rejection.Index = index
			rejections = append(rejections, rejection)
		}
		if operation.Kind == OperationSubmit {
			submitted = true
		}
	}
	result := RoundResult{RoundID: input.RoundID, BaseVersion: input.BaseVersion, OperationDigest: requestDigest}
	if len(rejections) > 0 {
		result.Outcome = RoundRejected
		result.Rejections = rejections
		if recordErr := s.store.RecordRound(ctx, input.TenantID, input.SpaceID, result); recordErr != nil {
			return RoundResult{}, false, fmt.Errorf("consolidation round %s: record rejected round: %w", input.RoundID, recordErr)
		}
		return result, false, nil
	}

	candidate := shadow
	candidate.Version = head.Version + 1
	candidate.ParentVersion = input.BaseVersion
	candidate.EvidenceWatermark = input.EvidenceWatermark
	candidate.QueryWatermark = input.QueryWatermark
	candidate.RoundID = input.RoundID
	candidate.CreatedAt = s.clock.Now()
	candidate.Digest = digestProjection(candidate)
	result.CandidateDigest = candidate.Digest

	plan := input.ReplayPlan
	if plan.BaseVersion != input.BaseVersion || plan.BaseDigest != input.BaseDigest {
		result.Outcome = RoundFailed
		return result, false, fmt.Errorf("consolidation round %s: replay plan base does not match the frozen projection", input.RoundID)
	}
	if plan.CandidateDigest != "" && plan.CandidateDigest != candidate.Digest {
		result.Outcome = RoundFailed
		return result, false, fmt.Errorf("consolidation round %s: replay plan candidate digest does not match the shadow", input.RoundID)
	}
	plan.CandidateDigest = candidate.Digest
	stats, replayErr := s.replay.Evaluate(ctx, base, candidate, plan, BacktestConfig{
		RecallTolerance: s.cfg.RecallTolerance,
		RoundsTolerance: s.cfg.RoundsTolerance,
		CostWeights:     s.cfg.CostWeights,
	})
	if replayErr != nil {
		result.Outcome = RoundFailed
		result.Backtest = stats
		return result, false, nil
	}
	result.Backtest = stats
	if stats.Version != candidate.Version || stats.Digest != candidate.Digest {
		result.Outcome = RoundFailed
		return result, false, fmt.Errorf("consolidation round %s: validator result is not bound to the candidate", input.RoundID)
	}
	if !stats.Passed {
		result.Outcome = RoundRejected
		if recordErr := s.store.RecordRound(ctx, input.TenantID, input.SpaceID, result); recordErr != nil {
			return RoundResult{}, false, fmt.Errorf("consolidation round %s: record rejected round: %w", input.RoundID, recordErr)
		}
		return result, false, nil
	}

	published := candidate.Version
	result.Outcome = RoundPublished
	result.PublishedVersion = &published
	stored, duplicate, publishErr := s.store.PublishRound(ctx, ConsolidationPublishInput{
		TenantID: input.TenantID, SpaceID: input.SpaceID,
		Round:          result,
		Projection:     candidate,
		ExpectedHead:   head,
		ActivityCursor: domain.ConsolidationCursor{SpaceID: input.SpaceID, LastPublishedBatchOrdinal: input.EvidenceWatermark, LastPublishedQueryOrdinal: input.QueryWatermark},
	})
	if publishErr != nil {
		failed := RoundResult{RoundID: input.RoundID, Outcome: RoundFailed, BaseVersion: input.BaseVersion, CandidateDigest: candidate.Digest, Backtest: stats}
		return failed, false, fmt.Errorf("consolidation round %s: publish failed: %w", input.RoundID, publishErr)
	}
	return stored, duplicate, nil
}

// SelectWinner picks the cheapest passing candidate by frozen cost weights;
// identity never contributes, and the first minimum wins ties.
func SelectWinner(candidates []CandidateStats, weights CostWeights) int {
	best := -1
	var bestCost float64
	for index, candidate := range candidates {
		if !candidate.Passed {
			continue
		}
		cost := weights.Round*float64(candidate.MeanRounds) + weights.Tail*candidate.P95Rounds +
			weights.Embed*(float64(candidate.EmbedBytes)/1024) + weights.Node*float64(candidate.ChangedNodes) + weights.Graph*float64(candidate.EdgeChurn)
		if best == -1 || cost < bestCost {
			best, bestCost = index, cost
		}
	}
	return best
}

// applyOperation mutates the shadow projection in place. Provenance is
// monotonic: updates merge evidence references, and merges union them —
// nothing ever drops a committed evidence reference.
func applyOperation(shadow *DerivedProjection, operation Operation) (OperationRejection, bool) {
	switch operation.Kind {
	case OperationAddNode:
		if operation.Node == nil {
			return OperationRejection{Kind: operation.Kind, Reason: "missing node payload"}, false
		}
		if findNode(shadow, operation.Node.ID) != nil {
			return OperationRejection{Kind: operation.Kind, Reason: "node already exists"}, false
		}
		shadow.Nodes = append(shadow.Nodes, *operation.Node)
		return OperationRejection{}, true
	case OperationUpdateNode:
		if operation.Node == nil {
			return OperationRejection{Kind: operation.Kind, Reason: "missing node payload"}, false
		}
		node := findNode(shadow, operation.NodeID)
		if node == nil {
			return OperationRejection{Kind: operation.Kind, Reason: "node does not exist"}, false
		}
		node.Content = operation.Node.Content
		node.Level = operation.Node.Level
		node.Tags = operation.Node.Tags
		node.EntityRefs = operation.Node.EntityRefs
		node.EvidenceRefs = unionRefs(node.EvidenceRefs, operation.Node.EvidenceRefs)
		return OperationRejection{}, true
	case OperationDeleteNode:
		for index, node := range shadow.Nodes {
			if node.ID == operation.NodeID {
				shadow.Nodes = append(shadow.Nodes[:index], shadow.Nodes[index+1:]...)
				return OperationRejection{}, true
			}
		}
		return OperationRejection{Kind: operation.Kind, Reason: "node does not exist"}, false
	case OperationMergeNodes:
		if operation.Node == nil || len(operation.InputNodeIDs) == 0 {
			return OperationRejection{Kind: operation.Kind, Reason: "merge requires a target node and inputs"}, false
		}
		merged := *operation.Node
		merged.EvidenceRefs = append([]domain.EvidenceRef(nil), operation.Node.EvidenceRefs...)
		for _, inputID := range operation.InputNodeIDs {
			input := findNode(shadow, inputID)
			if input == nil {
				return OperationRejection{Kind: operation.Kind, Reason: "input node does not exist"}, false
			}
			merged.EvidenceRefs = unionRefs(merged.EvidenceRefs, input.EvidenceRefs)
		}
		if findNode(shadow, merged.ID) != nil {
			return OperationRejection{Kind: operation.Kind, Reason: "merged node id already exists"}, false
		}
		shadow.Nodes = append(shadow.Nodes, merged)
		return OperationRejection{}, true
	case OperationAddHierarchyEdge, OperationAddRelationEdge:
		if operation.Edge == nil {
			return OperationRejection{Kind: operation.Kind, Reason: "missing edge payload"}, false
		}
		if operation.Edge.Kind != expectedEdgeKind(operation.Kind) {
			return OperationRejection{Kind: operation.Kind, Reason: "edge kind does not match operation"}, false
		}
		if findNode(shadow, operation.Edge.From) == nil || findNode(shadow, operation.Edge.To) == nil {
			return OperationRejection{Kind: operation.Kind, Reason: "edge endpoint does not exist"}, false
		}
		for _, edge := range shadow.Edges {
			if edge.ID == operation.Edge.ID {
				return OperationRejection{Kind: operation.Kind, Reason: "edge already exists"}, false
			}
		}
		if operation.Kind == OperationAddHierarchyEdge && hierarchyReaches(shadow, operation.Edge.To, operation.Edge.From) {
			return OperationRejection{Kind: operation.Kind, Reason: "hierarchy edge creates a cycle"}, false
		}
		shadow.Edges = append(shadow.Edges, *operation.Edge)
		return OperationRejection{}, true
	case OperationPruneEdge:
		for index, edge := range shadow.Edges {
			if edge.ID == operation.EdgeID {
				shadow.Edges = append(shadow.Edges[:index], shadow.Edges[index+1:]...)
				return OperationRejection{}, true
			}
		}
		return OperationRejection{Kind: operation.Kind, Reason: "edge does not exist"}, false
	case OperationSubmit:
		return OperationRejection{}, true
	default:
		return OperationRejection{Kind: operation.Kind, Reason: "unknown operation kind"}, false
	}
}

func expectedEdgeKind(kind OperationKind) EdgeKind {
	if kind == OperationAddHierarchyEdge {
		return domain.EdgeHierarchy
	}
	return domain.EdgeRelation
}

// hierarchyReaches reports whether the shadow's hierarchy subgraph already
// contains a directed path from `from` to `to`. Adding a hierarchy edge
// from→to creates a cycle exactly when such a path exists, and the hierarchy
// relation must stay a strict DAG.
func hierarchyReaches(projection *DerivedProjection, from, to ProjectionNodeID) bool {
	if from == to {
		return true
	}
	adjacency := make(map[ProjectionNodeID][]ProjectionNodeID)
	for _, edge := range projection.Edges {
		if edge.Kind != domain.EdgeHierarchy {
			continue
		}
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
	}
	visited := map[ProjectionNodeID]bool{from: true}
	stack := []ProjectionNodeID{from}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current == to {
			return true
		}
		for _, next := range adjacency[current] {
			if !visited[next] {
				visited[next] = true
				stack = append(stack, next)
			}
		}
	}
	return false
}

func findNode(projection *DerivedProjection, id ProjectionNodeID) *ProjectionNode {
	for index := range projection.Nodes {
		if projection.Nodes[index].ID == id {
			return &projection.Nodes[index]
		}
	}
	return nil
}

func unionRefs(base, extra []domain.EvidenceRef) []domain.EvidenceRef {
	merged := append([]domain.EvidenceRef(nil), base...)
	for _, ref := range extra {
		duplicate := false
		for _, existing := range merged {
			if sameEvidenceRef(existing, ref) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			merged = append(merged, ref)
		}
	}
	return merged
}

func sameEvidenceRef(left, right domain.EvidenceRef) bool {
	if left.BatchID != right.BatchID || len(left.EventIDs) != len(right.EventIDs) {
		return false
	}
	leftIDs := append([]string(nil), left.EventIDs...)
	rightIDs := append([]string(nil), right.EventIDs...)
	sort.Strings(leftIDs)
	sort.Strings(rightIDs)
	for index := range leftIDs {
		if leftIDs[index] != rightIDs[index] {
			return false
		}
	}
	return true
}

func cloneProjection(projection DerivedProjection) DerivedProjection {
	cloned := projection
	cloned.Nodes = make([]domain.ProjectionNode, len(projection.Nodes))
	for index, node := range projection.Nodes {
		cloned.Nodes[index] = node
		cloned.Nodes[index].EvidenceRefs = append([]domain.EvidenceRef(nil), node.EvidenceRefs...)
		cloned.Nodes[index].Tags = append([]string(nil), node.Tags...)
		cloned.Nodes[index].EntityRefs = append([]string(nil), node.EntityRefs...)
	}
	cloned.Edges = make([]domain.ProjectionEdge, len(projection.Edges))
	for index, edge := range projection.Edges {
		cloned.Edges[index] = edge
		cloned.Edges[index].EvidenceRefs = append([]domain.EvidenceRef(nil), edge.EvidenceRefs...)
	}
	return cloned
}

func digestProjection(projection DerivedProjection) string {
	normalized := cloneProjection(projection)
	normalized.Digest = ""
	normalized.CreatedAt = time.Time{}
	sort.Slice(normalized.Nodes, func(i, j int) bool { return normalized.Nodes[i].ID < normalized.Nodes[j].ID })
	for index := range normalized.Nodes {
		sort.Strings(normalized.Nodes[index].Tags)
		sort.Strings(normalized.Nodes[index].EntityRefs)
		normalizeEvidenceRefs(normalized.Nodes[index].EvidenceRefs)
	}
	sort.Slice(normalized.Edges, func(i, j int) bool { return normalized.Edges[i].ID < normalized.Edges[j].ID })
	for index := range normalized.Edges {
		normalizeEvidenceRefs(normalized.Edges[index].EvidenceRefs)
	}
	return hashJSON(normalized)
}

func digestOperations(operations []Operation) string {
	normalized := make([]Operation, len(operations))
	for index, operation := range operations {
		normalized[index] = operation
		normalized[index].InputNodeIDs = append([]ProjectionNodeID(nil), operation.InputNodeIDs...)
		sort.Slice(normalized[index].InputNodeIDs, func(i, j int) bool { return normalized[index].InputNodeIDs[i] < normalized[index].InputNodeIDs[j] })
		if operation.Node != nil {
			node := *operation.Node
			node.Tags = append([]string(nil), node.Tags...)
			node.EntityRefs = append([]string(nil), node.EntityRefs...)
			node.EvidenceRefs = append([]domain.EvidenceRef(nil), node.EvidenceRefs...)
			sort.Strings(node.Tags)
			sort.Strings(node.EntityRefs)
			normalizeEvidenceRefs(node.EvidenceRefs)
			normalized[index].Node = &node
		}
		if operation.Edge != nil {
			edge := *operation.Edge
			edge.EvidenceRefs = append([]domain.EvidenceRef(nil), edge.EvidenceRefs...)
			normalizeEvidenceRefs(edge.EvidenceRefs)
			normalized[index].Edge = &edge
		}
	}
	return hashJSON(normalized)
}

func normalizeEvidenceRefs(refs []domain.EvidenceRef) {
	for index := range refs {
		refs[index].EventIDs = append([]string(nil), refs[index].EventIDs...)
		sort.Strings(refs[index].EventIDs)
	}
	sort.Slice(refs, func(i, j int) bool {
		return evidenceRefSortKey(refs[i]) < evidenceRefSortKey(refs[j])
	})
}

func evidenceRefSortKey(ref domain.EvidenceRef) string {
	parts := make([]string, 1, len(ref.EventIDs)+1)
	parts[0] = string(ref.BatchID)
	parts = append(parts, ref.EventIDs...)
	return domain.StableCitationID("", parts...)
}

func hashJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
