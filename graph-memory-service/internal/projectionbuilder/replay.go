package projectionbuilder

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"river2.dev/graph-memory-service/internal/consolidation"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

const (
	validationStructural = "structural"
	validationReplay     = "structural_and_candidate_bm25_replay"
)

// ReplayRunner performs mandatory graph/evidence validation and, when cases
// are present, an explainable BM25 replay over the candidate projection.
// It is deliberately not an LLM-quality evaluator.
type ReplayRunner struct {
	store Store
	audit ports.RetrievalReplayStore
}

func NewReplayRunner(store Store, audit ports.RetrievalReplayStore) (*ReplayRunner, error) {
	if store == nil {
		return nil, fmt.Errorf("projectionbuilder: replay store is required")
	}
	return &ReplayRunner{store: store, audit: audit}, nil
}

func (r *ReplayRunner) Evaluate(ctx context.Context, base, candidate domain.DerivedProjection, plan domain.RetrievalReplayPlan, cfg domain.BacktestConfig) (domain.CandidateStats, error) {
	stats := domain.CandidateStats{Version: candidate.Version, Digest: candidate.Digest, ValidationMode: validationStructural}
	if plan.BaseVersion != base.Version || plan.BaseDigest != base.Digest || plan.CandidateDigest != candidate.Digest {
		return stats, fmt.Errorf("projectionbuilder: replay plan is not bound to the supplied base and candidate")
	}
	if r.audit != nil {
		if err := r.audit.PutReplayPlan(ctx, plan); err != nil {
			return stats, fmt.Errorf("projectionbuilder: freeze replay plan: %w", err)
		}
	}
	batches, err := r.store.CommittedEvidenceBatches(ctx, candidate.TenantID, candidate.SpaceID, 0, candidate.EvidenceWatermark)
	if err != nil {
		return stats, fmt.Errorf("projectionbuilder: read validation evidence: %w", err)
	}
	if err := validateCandidate(candidate, batches); err != nil {
		stats.Passed = false
		stats.GateFailures = []string{"structural_validation: " + err.Error()}
		if err := r.putResult(ctx, candidate.RoundID, stats); err != nil {
			return stats, err
		}
		return stats, nil
	}
	stats.ChangedNodes = changedNodes(base, candidate)
	stats.EdgeChurn = edgeChurn(base, candidate)
	if len(plan.Cases) == 0 {
		stats.Passed = true
		stats.Queries = []domain.QueryBacktestStat{}
		stats.Cost = candidateCost(stats, cfg.CostWeights)
		if err := r.putResult(ctx, candidate.RoundID, stats); err != nil {
			return stats, err
		}
		return stats, nil
	}

	stats.ValidationMode = validationReplay
	var measuredCases int
	stats.Queries, stats.Recall, stats.BaselineRecall, stats.MeanRounds, stats.P95Rounds, measuredCases = replayCases(candidate, plan, cfg)
	if measuredCases == 0 {
		stats.GateFailures = append(stats.GateFailures, "replay_no_measurable_cases")
	}
	if measuredCases > 0 && stats.Recall+cfg.RecallTolerance < stats.BaselineRecall {
		stats.GateFailures = append(stats.GateFailures, "recall_regression")
	}
	for _, query := range stats.Queries {
		if query.Partition == domain.ReplaySafety && query.BaselineFound && !query.Found {
			stats.GateFailures = appendUnique(stats.GateFailures, "safety_case_regression")
		}
		if query.Regressed {
			stats.GateFailures = appendUnique(stats.GateFailures, "rounds_regression")
		}
	}
	stats.Passed = len(stats.GateFailures) == 0
	stats.Cost = candidateCost(stats, cfg.CostWeights)
	if err := r.putResult(ctx, candidate.RoundID, stats); err != nil {
		return stats, err
	}
	return stats, nil
}

func (r *ReplayRunner) putResult(ctx context.Context, roundID domain.ConsolidationRoundID, stats domain.CandidateStats) error {
	if r.audit == nil {
		return nil
	}
	if err := r.audit.PutReplayResult(ctx, roundID, stats); err != nil {
		return fmt.Errorf("projectionbuilder: store replay result: %w", err)
	}
	return nil
}

func validateCandidate(candidate domain.DerivedProjection, batches []domain.EvidenceBatch) error {
	evidence := make(map[domain.BatchID]map[string]struct{}, len(batches))
	for _, batch := range batches {
		if batch.MemoryVersion == nil || *batch.MemoryVersion > candidate.EvidenceWatermark {
			return fmt.Errorf("batch %s exceeds evidence watermark %d", batch.ID, candidate.EvidenceWatermark)
		}
		events := make(map[string]struct{}, len(batch.Events))
		for _, event := range batch.Events {
			events[event.ID] = struct{}{}
		}
		evidence[batch.ID] = events
	}

	nodes := make(map[domain.ProjectionNodeID]domain.ProjectionNode, len(candidate.Nodes))
	for _, node := range candidate.Nodes {
		if node.ID == "" {
			return fmt.Errorf("projection contains an empty node ID")
		}
		if _, duplicate := nodes[node.ID]; duplicate {
			return fmt.Errorf("projection contains duplicate node %s", node.ID)
		}
		if err := validateEvidenceRefs(node.EvidenceRefs, evidence); err != nil {
			return fmt.Errorf("node %s: %w", node.ID, err)
		}
		nodes[node.ID] = node
	}
	hierarchy := make(map[domain.ProjectionNodeID][]domain.ProjectionNodeID)
	edges := make(map[domain.ProjectionEdgeID]struct{}, len(candidate.Edges))
	for _, edge := range candidate.Edges {
		if edge.ID == "" {
			return fmt.Errorf("projection contains an empty edge ID")
		}
		if _, duplicate := edges[edge.ID]; duplicate {
			return fmt.Errorf("projection contains duplicate edge %s", edge.ID)
		}
		edges[edge.ID] = struct{}{}
		from, fromOK := nodes[edge.From]
		to, toOK := nodes[edge.To]
		if !fromOK || !toOK {
			return fmt.Errorf("edge %s has a dangling endpoint", edge.ID)
		}
		if err := validateEvidenceRefs(edge.EvidenceRefs, evidence); err != nil {
			return fmt.Errorf("edge %s: %w", edge.ID, err)
		}
		switch edge.Kind {
		case domain.EdgeHierarchy:
			if from.Level >= to.Level {
				return fmt.Errorf("hierarchy edge %s is not directed parent to child", edge.ID)
			}
			hierarchy[edge.From] = append(hierarchy[edge.From], edge.To)
		case domain.EdgeRelation:
			if strings.TrimSpace(edge.Relation) == "" {
				return fmt.Errorf("relation edge %s has no type", edge.ID)
			}
		default:
			return fmt.Errorf("edge %s has unknown kind %q", edge.ID, edge.Kind)
		}
	}
	if hierarchyHasCycle(nodes, hierarchy) {
		return fmt.Errorf("hierarchy is not a DAG")
	}
	return nil
}

func validateEvidenceRefs(refs []domain.EvidenceRef, evidence map[domain.BatchID]map[string]struct{}) error {
	if len(refs) == 0 {
		return fmt.Errorf("evidence refs are required")
	}
	for _, ref := range refs {
		events, ok := evidence[ref.BatchID]
		if !ok {
			return fmt.Errorf("evidence batch %s does not exist at the candidate watermark", ref.BatchID)
		}
		if len(ref.EventIDs) == 0 {
			return fmt.Errorf("evidence batch %s has an empty event reference", ref.BatchID)
		}
		for _, eventID := range ref.EventIDs {
			if _, ok := events[eventID]; !ok {
				return fmt.Errorf("evidence event %s/%s does not exist at the candidate watermark", ref.BatchID, eventID)
			}
		}
	}
	return nil
}

func hierarchyHasCycle(nodes map[domain.ProjectionNodeID]domain.ProjectionNode, graph map[domain.ProjectionNodeID][]domain.ProjectionNodeID) bool {
	const (
		visiting = 1
		visited  = 2
	)
	state := make(map[domain.ProjectionNodeID]int, len(nodes))
	var visit func(domain.ProjectionNodeID) bool
	visit = func(node domain.ProjectionNodeID) bool {
		if state[node] == visiting {
			return true
		}
		if state[node] == visited {
			return false
		}
		state[node] = visiting
		for _, child := range graph[node] {
			if visit(child) {
				return true
			}
		}
		state[node] = visited
		return false
	}
	for node := range nodes {
		if visit(node) {
			return true
		}
	}
	return false
}

func replayCases(candidate domain.DerivedProjection, plan domain.RetrievalReplayPlan, cfg domain.BacktestConfig) ([]domain.QueryBacktestStat, float64, float64, float64, float64, int) {
	maxResults := plan.MaxResults
	if maxResults <= 0 {
		maxResults = 10
	}
	maxRounds := plan.MaxRounds
	if maxRounds <= 0 {
		maxRounds = 1
	}
	queries := make([]domain.QueryBacktestStat, 0, len(plan.Cases))
	var foundCount, baselineCount int
	rounds := make([]float64, 0, len(plan.Cases))
	for _, replayCase := range plan.Cases {
		stat := domain.QueryBacktestStat{
			CaseID: replayCase.ID, TraceID: replayCase.TraceID, Partition: replayCase.Partition,
			BaselineFound: replayCase.BaselineFound, BaselineRounds: replayCase.BaselineRounds,
			ItemsTotal: len(replayCase.Items),
		}
		if len(replayCase.Items) == 0 {
			stat.Disposition = domain.ReplayIneligible
			stat.SkipReason = "replay case has no expected items"
			queries = append(queries, stat)
			continue
		}
		stat.Disposition = domain.ReplayMeasured
		hits := bm25Search(candidate.Nodes, replayCase.NormalizedQuery, maxResults)
		for _, item := range replayCase.Items {
			if itemCovered(item, hits) {
				stat.ItemsSatisfied++
			} else {
				stat.ItemMisses = append(stat.ItemMisses, item.ID)
			}
		}
		stat.Covered = stat.ItemsSatisfied == stat.ItemsTotal
		stat.Found = stat.Covered
		if stat.Found {
			stat.Rounds = 1
			foundCount++
		} else {
			stat.Rounds = float64(maxRounds)
		}
		if stat.BaselineFound {
			baselineCount++
		}
		stat.Regressed = (stat.BaselineFound && !stat.Found) || (stat.BaselineRounds > 0 && stat.Rounds > float64(stat.BaselineRounds+cfg.RoundsTolerance))
		rounds = append(rounds, stat.Rounds)
		queries = append(queries, stat)
	}
	if len(rounds) == 0 {
		return queries, 0, 0, 0, 0, 0
	}
	denominator := float64(len(rounds))
	mean := 0.0
	for _, value := range rounds {
		mean += value
	}
	mean /= denominator
	sort.Float64s(rounds)
	p95Index := int(math.Ceil(0.95*denominator)) - 1
	return queries, float64(foundCount) / denominator, float64(baselineCount) / denominator, mean, rounds[p95Index], len(rounds)
}

type replayHit struct {
	node  domain.ProjectionNode
	score float64
}

func bm25Search(nodes []domain.ProjectionNode, query string, limit int) []domain.ProjectionNode {
	queryTerms := distinctTokens(query)
	if len(queryTerms) == 0 || len(nodes) == 0 || limit <= 0 {
		return []domain.ProjectionNode{}
	}
	documents := make([]map[string]int, len(nodes))
	lengths := make([]int, len(nodes))
	documentFrequency := make(map[string]int)
	totalLength := 0
	for index, node := range nodes {
		tokens := tokenize(node.Content)
		lengths[index] = len(tokens)
		totalLength += len(tokens)
		frequencies := make(map[string]int)
		for _, token := range tokens {
			frequencies[token]++
		}
		documents[index] = frequencies
		for term := range frequencies {
			documentFrequency[term]++
		}
	}
	averageLength := float64(totalLength) / float64(len(nodes))
	if averageLength == 0 {
		averageLength = 1
	}
	const k1, b = 1.5, 0.75
	hits := make([]replayHit, 0, len(nodes))
	for index, node := range nodes {
		score := 0.0
		for _, term := range queryTerms {
			frequency := float64(documents[index][term])
			if frequency == 0 {
				continue
			}
			df := float64(documentFrequency[term])
			idf := math.Log(1 + (float64(len(nodes))-df+0.5)/(df+0.5))
			denominator := frequency + k1*(1-b+b*float64(lengths[index])/averageLength)
			score += idf * frequency * (k1 + 1) / denominator
		}
		if score > 0 {
			hits = append(hits, replayHit{node: node, score: score})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return hits[i].node.ID < hits[j].node.ID
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	result := make([]domain.ProjectionNode, len(hits))
	for index := range hits {
		result[index] = hits[index].node
	}
	return result
}

func itemCovered(item domain.BacktestItem, hits []domain.ProjectionNode) bool {
	nodeIDs := make(map[domain.ProjectionNodeID]struct{}, len(item.NodeIDs))
	for _, id := range item.NodeIDs {
		nodeIDs[id] = struct{}{}
	}
	for _, hit := range hits {
		if _, ok := nodeIDs[hit.ID]; ok {
			return true
		}
	}
	for _, expected := range item.EvidenceRefs {
		if evidenceRefCovered(expected, hits) {
			return true
		}
	}
	return false
}

func evidenceRefCovered(expected domain.EvidenceRef, hits []domain.ProjectionNode) bool {
	if len(expected.EventIDs) == 0 {
		return false
	}
	found := make(map[string]struct{}, len(expected.EventIDs))
	for _, hit := range hits {
		for _, actual := range hit.EvidenceRefs {
			if actual.BatchID != expected.BatchID {
				continue
			}
			for _, eventID := range actual.EventIDs {
				found[eventID] = struct{}{}
			}
		}
	}
	for _, eventID := range expected.EventIDs {
		if _, ok := found[eventID]; !ok {
			return false
		}
	}
	return true
}

func tokenize(value string) []string {
	tokens := make([]string, 0)
	var token []rune
	flush := func() {
		if len(token) > 0 {
			tokens = append(tokens, strings.ToLower(string(token)))
			token = token[:0]
		}
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			token = append(token, unicode.ToLower(r))
		} else {
			flush()
		}
	}
	flush()
	return tokens
}

func distinctTokens(value string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, token := range tokenize(value) {
		if _, duplicate := seen[token]; duplicate {
			continue
		}
		seen[token] = struct{}{}
		result = append(result, token)
	}
	return result
}

func changedNodes(base, candidate domain.DerivedProjection) int {
	baseNodes := make(map[domain.ProjectionNodeID]domain.ProjectionNode, len(base.Nodes))
	candidateNodes := make(map[domain.ProjectionNodeID]domain.ProjectionNode, len(candidate.Nodes))
	for _, node := range base.Nodes {
		baseNodes[node.ID] = node
	}
	for _, node := range candidate.Nodes {
		candidateNodes[node.ID] = node
	}
	changed := 0
	for id, node := range candidateNodes {
		if previous, ok := baseNodes[id]; !ok || !reflect.DeepEqual(previous, node) {
			changed++
		}
	}
	for id := range baseNodes {
		if _, ok := candidateNodes[id]; !ok {
			changed++
		}
	}
	return changed
}

func edgeChurn(base, candidate domain.DerivedProjection) int {
	baseEdges := make(map[domain.ProjectionEdgeID]domain.ProjectionEdge, len(base.Edges))
	candidateEdges := make(map[domain.ProjectionEdgeID]domain.ProjectionEdge, len(candidate.Edges))
	for _, edge := range base.Edges {
		baseEdges[edge.ID] = edge
	}
	for _, edge := range candidate.Edges {
		candidateEdges[edge.ID] = edge
	}
	changed := 0
	for id, edge := range candidateEdges {
		if previous, ok := baseEdges[id]; !ok || !reflect.DeepEqual(previous, edge) {
			changed++
		}
	}
	for id := range baseEdges {
		if _, ok := candidateEdges[id]; !ok {
			changed++
		}
	}
	return changed
}

func candidateCost(stats domain.CandidateStats, weights domain.CostWeights) float64 {
	return weights.Round*float64(stats.MeanRounds) + weights.Tail*stats.P95Rounds +
		weights.Embed*(float64(stats.EmbedBytes)/1024) + weights.Node*float64(stats.ChangedNodes) + weights.Graph*float64(stats.EdgeChurn)
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

var _ consolidation.ReplayRunner = (*ReplayRunner)(nil)
