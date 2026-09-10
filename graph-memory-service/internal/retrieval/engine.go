// Package retrieval is a deep module for immutable pinned retrieval. Callers
// supply an exact authorized view; the module owns tokenization, BM25 indexing,
// optional embeddings, fusion, stable ranking, and derived caches.
package retrieval

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

type EmbeddingMode string

const (
	EmbeddingOff      EmbeddingMode = "off"
	EmbeddingOptional EmbeddingMode = "optional"
	EmbeddingRequired EmbeddingMode = "required"
)

type Config struct {
	EmbeddingMode EmbeddingMode
	BM25Weight    float64
}

func DefaultConfig() Config {
	return Config{EmbeddingMode: EmbeddingOff, BM25Weight: 0.5}
}

func ParseEmbeddingMode(value string) (EmbeddingMode, error) {
	mode := EmbeddingMode(strings.ToLower(strings.TrimSpace(value)))
	switch mode {
	case EmbeddingOff, EmbeddingOptional, EmbeddingRequired:
		return mode, nil
	default:
		return "", fmt.Errorf("retrieval: embedding mode must be off, optional, or required")
	}
}

type Engine struct {
	store         ports.EvidenceDocumentStore
	provider      ports.EmbeddingProvider
	queryActivity ports.QueryActivityStore
	mode          EmbeddingMode
	bm25Weight    float64

	indexMu sync.RWMutex
	indexes map[string]*immutableIndex
	// indexOrder tracks insertion order so the oldest derived index is
	// evicted first; eviction only forces a rebuild from the store.
	indexOrder []string

	embeddingMu sync.RWMutex
	embeddings  map[string][]float64
	// embeddingOrder tracks insertion order for bounded eviction.
	embeddingOrder []string
}

// Cache caps bound process-local derived state. They trade recompute cost for
// memory: an evicted entry is rebuilt deterministically from the store.
const (
	maxCachedIndexes    = 16
	maxCachedEmbeddings = 8192
)

type immutableIndex struct {
	documents []ports.EvidenceDocument
	bm25      bm25Index
}

func New(store ports.EvidenceDocumentStore, provider ports.EmbeddingProvider, config Config, activity ...ports.QueryActivityStore) (*Engine, error) {
	if store == nil {
		return nil, fmt.Errorf("retrieval: evidence document store is required")
	}
	if config == (Config{}) {
		config = DefaultConfig()
	}
	mode, err := ParseEmbeddingMode(string(config.EmbeddingMode))
	if err != nil {
		return nil, err
	}
	if config.BM25Weight < 0 || config.BM25Weight > 1 || math.IsNaN(config.BM25Weight) || math.IsInf(config.BM25Weight, 0) {
		return nil, fmt.Errorf("retrieval: BM25 weight must be finite and between 0 and 1")
	}
	engine := &Engine{
		store: store, provider: provider, mode: mode, bm25Weight: config.BM25Weight,
		indexes: make(map[string]*immutableIndex), embeddings: make(map[string][]float64),
	}
	if len(activity) > 0 {
		engine.queryActivity = activity[0]
	}
	return engine, nil
}

func (engine *Engine) Retrieve(ctx context.Context, tenantID domain.TenantID, pinned []domain.PinnedSpace, query string, maxResults int) (ports.RecallOutcome, error) {
	outcome := ports.RecallOutcome{Items: []domain.RecallItem{}, Applied: []ports.RetrievalChannel{ports.RetrievalBM25}}
	if maxResults <= 0 {
		return outcome, nil
	}
	index, err := engine.index(ctx, tenantID, pinned)
	if err != nil {
		return ports.RecallOutcome{}, err
	}
	if len(index.documents) == 0 {
		outcome.Items, err = engine.completeRecall(ctx, tenantID, pinned, outcome.Items, false)
		return outcome, err
	}
	bm25Scores := index.bm25.score(query)
	vectorScores, vectorEnabled, vectorErr := engine.recallVectorScores(ctx, query, index.documents)
	if vectorErr != nil && (engine.mode != EmbeddingOptional || ctx.Err() != nil || errors.Is(vectorErr, context.Canceled) || errors.Is(vectorErr, context.DeadlineExceeded)) {
		return ports.RecallOutcome{}, vectorErr
	}
	if vectorEnabled {
		outcome.Applied = append(outcome.Applied, ports.RetrievalEmbedding)
	}

	type ranked struct {
		item  domain.RecallItem
		score float64
	}
	rankedItems := make([]ranked, 0, len(index.documents))
	for i, document := range index.documents {
		if err := ctx.Err(); err != nil {
			return ports.RecallOutcome{}, err
		}
		score := bm25Scores[i]
		if vectorEnabled {
			score = engine.bm25Weight*bm25Scores[i] + (1-engine.bm25Weight)*vectorScores[i]
		}
		if score <= 0 {
			continue
		}
		item := domain.RecallItem{
			Content: document.Content, SourceSpaceID: document.SpaceID, MemoryVersion: document.MemoryVersion,
			Citation: domain.Citation{
				ID:              domain.StableCitationID("cit-", string(document.BatchID), document.EventID),
				EvidenceBatchID: document.BatchID, EventIDs: []string{document.EventID},
			},
			Score: score,
		}
		rankedItems = append(rankedItems, ranked{item: item, score: score})
	}
	sort.SliceStable(rankedItems, func(i, j int) bool {
		if rankedItems[i].score != rankedItems[j].score {
			return rankedItems[i].score > rankedItems[j].score
		}
		return rankedItems[i].item.Citation.ID < rankedItems[j].item.Citation.ID
	})
	if len(rankedItems) > maxResults {
		rankedItems = rankedItems[:maxResults]
	}
	outcome.Items = make([]domain.RecallItem, len(rankedItems))
	for i := range rankedItems {
		outcome.Items[i] = rankedItems[i].item
	}
	outcome.Items, err = engine.completeRecall(ctx, tenantID, pinned, outcome.Items, vectorErr != nil)
	if err != nil {
		return ports.RecallOutcome{}, err
	}
	if vectorErr != nil {
		outcome.Degradation = &domain.RecallDegradation{
			State: "partial", Reasons: []string{"optional embedding unavailable; BM25-only retrieval applied"},
		}
	}
	return outcome, nil
}

func (engine *Engine) completeRecall(ctx context.Context, tenantID domain.TenantID, pinned []domain.PinnedSpace, items []domain.RecallItem, degraded bool) ([]domain.RecallItem, error) {
	// Query activity drives projection consolidation triggers; only fully
	// applied recalls count, matching the store's recorded contract.
	if engine.queryActivity != nil && !degraded {
		if err := engine.queryActivity.RecordSuccessfulRecall(ctx, tenantID, pinned); err != nil {
			return nil, fmt.Errorf("retrieval: record successful query activity: %w", err)
		}
	}
	return items, nil
}

func (engine *Engine) index(ctx context.Context, tenantID domain.TenantID, pinned []domain.PinnedSpace) (*immutableIndex, error) {
	key := pinnedIndexKey(tenantID, pinned)
	engine.indexMu.RLock()
	cached := engine.indexes[key]
	engine.indexMu.RUnlock()
	if cached != nil {
		return cached, nil
	}

	documents, err := engine.store.CommittedEvidenceDocuments(ctx, tenantID, pinned)
	if err != nil {
		return nil, err
	}
	unique := make([]ports.EvidenceDocument, 0, len(documents))
	contents := make([]string, 0, len(documents))
	seen := make(map[[sha256.Size]byte]struct{}, len(documents))
	for _, document := range documents {
		digest := sha256.Sum256([]byte(document.Content))
		if _, duplicate := seen[digest]; duplicate {
			continue
		}
		seen[digest] = struct{}{}
		unique = append(unique, document)
		contents = append(contents, document.Content)
	}
	built := &immutableIndex{documents: unique, bm25: buildBM25(contents)}
	engine.indexMu.Lock()
	if existing := engine.indexes[key]; existing != nil {
		built = existing
	} else {
		engine.indexes[key] = built
		engine.indexOrder = append(engine.indexOrder, key)
		for len(engine.indexOrder) > maxCachedIndexes {
			oldest := engine.indexOrder[0]
			engine.indexOrder = engine.indexOrder[1:]
			delete(engine.indexes, oldest)
		}
	}
	engine.indexMu.Unlock()
	return built, nil
}

func pinnedIndexKey(tenantID domain.TenantID, pinned []domain.PinnedSpace) string {
	hasher := sha256.New()
	writePart := func(value string) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(value))
	}
	writePart(string(tenantID))
	for _, pin := range pinned {
		writePart(string(pin.SpaceID))
		writePart(fmt.Sprintf("%d", pin.MemoryVersion))
		writePart(fmt.Sprintf("%d", pin.ProjectionVersion))
		writePart(pin.ProjectionDigest)
		writePart(fmt.Sprintf("%d", pin.ProjectionEvidenceWatermark))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func (engine *Engine) recallVectorScores(ctx context.Context, query string, documents []ports.EvidenceDocument) ([]float64, bool, error) {
	if engine.mode == EmbeddingOff {
		return nil, false, nil
	}
	if engine.provider == nil {
		if engine.mode == EmbeddingRequired {
			return nil, false, fmt.Errorf("retrieval: required embedding provider is unavailable")
		}
		return nil, false, fmt.Errorf("retrieval: optional embedding provider is unavailable")
	}

	vectors := make([][]float64, len(documents))
	missingInputs := []string{query}
	missingIndexes := make([]int, 0, len(documents))
	for i, document := range documents {
		if vector, ok := engine.cachedEmbedding(document.Content); ok {
			vectors[i] = vector
		} else {
			missingIndexes = append(missingIndexes, i)
			missingInputs = append(missingInputs, document.Content)
		}
	}
	provided, err := engine.provider.Embed(ctx, missingInputs)
	if err == nil {
		err = validateVectors(provided, len(missingInputs))
	}
	if err != nil {
		if contextErr := providerContextError(ctx, err); contextErr != nil {
			return nil, false, contextErr
		}
		if engine.mode == EmbeddingRequired {
			return nil, false, fmt.Errorf("retrieval: required embedding failed: %w", err)
		}
		return nil, false, fmt.Errorf("retrieval: optional embedding request failed")
	}
	queryVector := provided[0]
	for offset, documentIndex := range missingIndexes {
		vector := provided[offset+1]
		vectors[documentIndex] = vector
		engine.storeEmbedding(documents[documentIndex].Content, vector)
	}
	for _, vector := range vectors {
		if len(vector) != len(queryVector) {
			if engine.mode == EmbeddingRequired {
				return nil, false, fmt.Errorf("retrieval: required embedding returned inconsistent vector dimensions")
			}
			return nil, false, fmt.Errorf("retrieval: optional embedding returned inconsistent vector dimensions")
		}
	}
	scores := make([]float64, len(vectors))
	for i, vector := range vectors {
		scores[i] = (cosine(queryVector, vector) + 1) / 2
	}
	return scores, true, nil
}

func (engine *Engine) cachedEmbedding(content string) ([]float64, bool) {
	key := engine.embeddingCacheKey(content)
	engine.embeddingMu.RLock()
	vector, ok := engine.embeddings[key]
	engine.embeddingMu.RUnlock()
	return append([]float64(nil), vector...), ok
}

func (engine *Engine) storeEmbedding(content string, vector []float64) {
	key := engine.embeddingCacheKey(content)
	engine.embeddingMu.Lock()
	if _, exists := engine.embeddings[key]; !exists {
		engine.embeddingOrder = append(engine.embeddingOrder, key)
		for len(engine.embeddingOrder) > maxCachedEmbeddings {
			oldest := engine.embeddingOrder[0]
			engine.embeddingOrder = engine.embeddingOrder[1:]
			delete(engine.embeddings, oldest)
		}
	}
	engine.embeddings[key] = append([]float64(nil), vector...)
	engine.embeddingMu.Unlock()
}

func (engine *Engine) embeddingCacheKey(content string) string {
	contentHash := sha256.Sum256([]byte(content))
	return string(engine.mode) + "\x00" + engine.provider.CacheKey() + "\x00" + hex.EncodeToString(contentHash[:])
}

func validateVectors(vectors [][]float64, expected int) error {
	if len(vectors) != expected {
		return fmt.Errorf("embedding provider returned %d vectors for %d inputs", len(vectors), expected)
	}
	dimension := 0
	for _, vector := range vectors {
		if len(vector) == 0 {
			return fmt.Errorf("embedding provider returned an empty vector")
		}
		if dimension == 0 {
			dimension = len(vector)
		} else if len(vector) != dimension {
			return fmt.Errorf("embedding provider returned inconsistent vector dimensions")
		}
		for _, value := range vector {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return fmt.Errorf("embedding provider returned a non-finite vector value")
			}
		}
	}
	return nil
}

func providerContextError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func cosine(left, right []float64) float64 {
	if len(left) == 0 || len(left) != len(right) {
		return 0
	}
	var dot, leftNorm, rightNorm float64
	for i := range left {
		dot += left[i] * right[i]
		leftNorm += left[i] * left[i]
		rightNorm += right[i] * right[i]
	}
	if leftNorm == 0 || rightNorm == 0 {
		return 0
	}
	value := dot / math.Sqrt(leftNorm*rightNorm)
	if value < -1 {
		return -1
	}
	if value > 1 {
		return 1
	}
	return value
}

func (engine *Engine) EmbeddingNeighbors(ctx context.Context, tenantID domain.TenantID, pin domain.PinnedSpace, projection domain.DerivedProjection, anchorIDs []domain.ProjectionNodeID, excluded map[domain.ProjectionNodeID]struct{}, limit int) ([]ports.EmbeddingNeighbor, error) {
	if projection.TenantID != tenantID || projection.SpaceID != pin.SpaceID || projection.Version != pin.ProjectionVersion ||
		projection.Digest != pin.ProjectionDigest || projection.EvidenceWatermark != pin.ProjectionEvidenceWatermark {
		return nil, fmt.Errorf("retrieval: embedding neighbor projection does not match the exact pin")
	}
	if limit <= 0 || engine.mode == EmbeddingOff {
		return []ports.EmbeddingNeighbor{}, nil
	}
	if engine.provider == nil {
		if engine.mode == EmbeddingRequired {
			return nil, fmt.Errorf("retrieval: required embedding provider is unavailable")
		}
		return []ports.EmbeddingNeighbor{}, nil
	}

	nodes := make(map[domain.ProjectionNodeID]domain.ProjectionNode, len(projection.Nodes))
	for _, node := range projection.Nodes {
		nodes[node.ID] = node
	}
	anchorContents := make([]string, 0, len(anchorIDs))
	for _, id := range anchorIDs {
		if node, ok := nodes[id]; ok && strings.TrimSpace(node.Content) != "" {
			anchorContents = append(anchorContents, node.Content)
		}
	}
	if len(anchorContents) == 0 {
		return []ports.EmbeddingNeighbor{}, nil
	}
	type candidate struct {
		id      domain.ProjectionNodeID
		content string
	}
	candidates := make([]candidate, 0, len(projection.Nodes))
	for _, node := range projection.Nodes {
		if _, skip := excluded[node.ID]; skip || strings.TrimSpace(node.Content) == "" {
			continue
		}
		candidates = append(candidates, candidate{id: node.ID, content: node.Content})
	}
	if len(candidates) == 0 {
		return []ports.EmbeddingNeighbor{}, nil
	}
	contents := append([]string(nil), anchorContents...)
	for _, candidate := range candidates {
		contents = append(contents, candidate.content)
	}
	vectors, err := engine.embeddingVectors(ctx, contents)
	if err != nil {
		if contextErr := providerContextError(ctx, err); contextErr != nil {
			return nil, contextErr
		}
		if engine.mode == EmbeddingRequired {
			return nil, fmt.Errorf("retrieval: required embedding neighbor lookup failed: %w", err)
		}
		return []ports.EmbeddingNeighbor{}, nil
	}
	anchor := make([]float64, len(vectors[0]))
	for i := range anchorContents {
		for dimension, value := range vectors[i] {
			anchor[dimension] += value
		}
	}
	for i := range anchor {
		anchor[i] /= float64(len(anchorContents))
	}
	neighbors := make([]ports.EmbeddingNeighbor, len(candidates))
	for i, candidate := range candidates {
		neighbors[i] = ports.EmbeddingNeighbor{NodeID: candidate.id, Cosine: cosine(anchor, vectors[len(anchorContents)+i])}
	}
	sort.SliceStable(neighbors, func(i, j int) bool {
		if neighbors[i].Cosine != neighbors[j].Cosine {
			return neighbors[i].Cosine > neighbors[j].Cosine
		}
		return neighbors[i].NodeID < neighbors[j].NodeID
	})
	if len(neighbors) > limit {
		neighbors = neighbors[:limit]
	}
	return neighbors, nil
}

func (engine *Engine) embeddingVectors(ctx context.Context, contents []string) ([][]float64, error) {
	vectors := make([][]float64, len(contents))
	missingContents := make([]string, 0, len(contents))
	missingIndexes := make([]int, 0, len(contents))
	for i, content := range contents {
		if vector, ok := engine.cachedEmbedding(content); ok {
			vectors[i] = vector
		} else {
			missingContents = append(missingContents, content)
			missingIndexes = append(missingIndexes, i)
		}
	}
	if len(missingContents) > 0 {
		provided, err := engine.provider.Embed(ctx, missingContents)
		if err != nil {
			return nil, err
		}
		if err := validateVectors(provided, len(missingContents)); err != nil {
			return nil, err
		}
		for offset, index := range missingIndexes {
			vectors[index] = provided[offset]
			engine.storeEmbedding(contents[index], provided[offset])
		}
	}
	if err := validateVectors(vectors, len(contents)); err != nil {
		return nil, err
	}
	return vectors, nil
}

var _ ports.RecallStore = (*Engine)(nil)
var _ ports.EmbeddingNeighborFinder = (*Engine)(nil)
