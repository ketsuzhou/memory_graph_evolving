package exploration

import (
	"context"
	"sort"
	"strings"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

// explorationRepository is the read/write seam required by an exploration.
// Evidence remains authoritative; projections are immutable, derived indexes.
type explorationRepository interface {
	ports.ExplorationStore
	ports.EvidenceStore
	ProjectionHead(context.Context, domain.TenantID, domain.SpaceID) (domain.ProjectionHead, error)
	Projection(context.Context, domain.TenantID, domain.SpaceID, domain.ProjectionVersion) (domain.DerivedProjection, error)
}

func (s *Service) pinProjection(ctx context.Context, tenantID domain.TenantID, pin domain.PinnedSpace) (domain.PinnedSpace, error) {
	head, err := s.store.ProjectionHead(ctx, tenantID, pin.SpaceID)
	if err != nil {
		return domain.PinnedSpace{}, err
	}
	for version := head.Version; version > 0; version-- {
		projection, err := s.store.Projection(ctx, tenantID, pin.SpaceID, version)
		if err != nil {
			return domain.PinnedSpace{}, err
		}
		if projection.EvidenceWatermark > pin.MemoryVersion {
			continue
		}
		pin.ProjectionVersion = projection.Version
		pin.ProjectionDigest = projection.Digest
		pin.ProjectionEvidenceWatermark = projection.EvidenceWatermark
		return pin, nil
	}
	return pin, nil
}

type traversalCandidate struct {
	node           domain.ProjectionNode
	priority       int
	routeKind      string
	edgeID         domain.ProjectionEdgeID
	edgeKind       domain.EdgeKind
	relation       string
	direction      string
	confidence     float64
	embeddingScore float64
}

func (s *Service) expandProjection(ctx context.Context, tenantID domain.TenantID, session domain.ExplorationSession, anchor domain.RecallItem, relation string, limit int) ([]domain.RecallItem, error) {
	pin, ok := pinnedSpace(session.PinnedSpaces, anchor.SourceSpaceID)
	if !ok || pin.ProjectionVersion == 0 {
		return []domain.RecallItem{}, nil
	}
	projection, err := s.store.Projection(ctx, tenantID, pin.SpaceID, pin.ProjectionVersion)
	if err != nil {
		return nil, err
	}
	if projection.Digest != pin.ProjectionDigest || projection.EvidenceWatermark != pin.ProjectionEvidenceWatermark {
		return nil, domain.NewProtocolError(409, "PROJECTION_PIN_MISMATCH", "the pinned projection no longer matches the exploration session")
	}

	nodes := make(map[domain.ProjectionNodeID]domain.ProjectionNode, len(projection.Nodes))
	for _, node := range projection.Nodes {
		nodes[node.ID] = node
	}
	anchors := resolveAnchorNodes(anchor, pin, projection.Nodes)
	if len(anchors) == 0 {
		return []domain.RecallItem{}, nil
	}
	anchorSet := make(map[domain.ProjectionNodeID]bool, len(anchors))
	for _, nodeID := range anchors {
		anchorSet[nodeID] = true
	}

	candidates := make(map[domain.ProjectionNodeID]traversalCandidate)
	for _, edge := range projection.Edges {
		if !edgeMatchesRelation(edge, relation) {
			continue
		}
		var neighborID domain.ProjectionNodeID
		var direction string
		switch {
		case anchorSet[edge.From]:
			neighborID, direction = edge.To, "outgoing"
		case anchorSet[edge.To]:
			neighborID, direction = edge.From, "incoming"
		default:
			continue
		}
		if anchorSet[neighborID] {
			continue
		}
		node, found := nodes[neighborID]
		if !found {
			continue
		}
		priority, routeKind := edgePriority(edge, direction)
		candidate := traversalCandidate{
			node: node, priority: priority, routeKind: routeKind,
			edgeID: edge.ID, edgeKind: edge.Kind, relation: edge.Relation,
			direction: direction, confidence: edge.Confidence,
		}
		keepBetterCandidate(candidates, candidate)
	}

	if relation == "related" {
		anchorEntities := make(map[string]bool)
		for _, nodeID := range anchors {
			for _, entity := range nodes[nodeID].EntityRefs {
				if entity = strings.TrimSpace(entity); entity != "" {
					anchorEntities[entity] = true
				}
			}
		}
		if len(anchorEntities) > 0 {
			for nodeID, node := range nodes {
				if anchorSet[nodeID] || !sharesEntity(node.EntityRefs, anchorEntities) {
					continue
				}
				keepBetterCandidate(candidates, traversalCandidate{
					node: node, priority: 2, routeKind: "entity", relation: "entity", direction: "undirected", confidence: 1,
				})
			}
		}

		if s.neighborFinder != nil {
			excluded := make(map[domain.ProjectionNodeID]struct{}, len(anchorSet))
			for nodeID := range anchorSet {
				excluded[nodeID] = struct{}{}
			}
			neighbors, err := s.neighborFinder.EmbeddingNeighbors(ctx, tenantID, pin, projection, anchors, excluded, len(nodes))
			if err != nil {
				return nil, err
			}
			for _, neighbor := range neighbors {
				node, found := nodes[neighbor.NodeID]
				if !found || anchorSet[neighbor.NodeID] {
					continue
				}
				confidence := (neighbor.Cosine + 1) / 2
				if confidence < 0 {
					confidence = 0
				}
				if confidence > 1 {
					confidence = 1
				}
				keepBetterCandidate(candidates, traversalCandidate{
					node: node, priority: 4, routeKind: "embedding", relation: "semantic",
					direction: "undirected", confidence: confidence, embeddingScore: neighbor.Cosine,
				})
			}
		}
	}

	ordered := make([]traversalCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		ordered = append(ordered, candidate)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].priority != ordered[j].priority {
			return ordered[i].priority < ordered[j].priority
		}
		if ordered[i].priority == 4 && ordered[i].embeddingScore != ordered[j].embeddingScore {
			return ordered[i].embeddingScore > ordered[j].embeddingScore
		}
		return ordered[i].node.ID < ordered[j].node.ID
	})

	items := make([]domain.RecallItem, 0, min(limit, len(ordered)))
	for _, candidate := range ordered {
		if len(items) == limit {
			break
		}
		item, found, err := s.candidateItem(ctx, tenantID, pin, candidate)
		if err != nil {
			return nil, err
		}
		if found {
			items = append(items, item)
		}
	}
	return items, nil
}

func pinnedSpace(pins []domain.PinnedSpace, spaceID domain.SpaceID) (domain.PinnedSpace, bool) {
	for _, pin := range pins {
		if pin.SpaceID == spaceID {
			return pin, true
		}
	}
	return domain.PinnedSpace{}, false
}

func resolveAnchorNodes(anchor domain.RecallItem, pin domain.PinnedSpace, nodes []domain.ProjectionNode) []domain.ProjectionNodeID {
	if anchor.Traversal != nil &&
		anchor.Traversal.ProjectionVersion == pin.ProjectionVersion &&
		anchor.Traversal.ProjectionDigest == pin.ProjectionDigest {
		for _, node := range nodes {
			if node.ID == anchor.Traversal.ProjectionNodeID {
				return []domain.ProjectionNodeID{node.ID}
			}
		}
		return nil
	}

	matched := make([]domain.ProjectionNodeID, 0, 1)
	for _, node := range nodes {
		for _, ref := range node.EvidenceRefs {
			if ref.BatchID == anchor.Citation.EvidenceBatchID && overlaps(ref.EventIDs, anchor.Citation.EventIDs) {
				matched = append(matched, node.ID)
				break
			}
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i] < matched[j] })
	return matched
}

func edgeMatchesRelation(edge domain.ProjectionEdge, relation string) bool {
	if relation == "related" {
		return edge.Kind == domain.EdgeHierarchy || edge.Kind == domain.EdgeRelation
	}
	return edge.Kind == domain.EdgeRelation && edge.Relation == relation
}

func edgePriority(edge domain.ProjectionEdge, direction string) (int, string) {
	if edge.Kind == domain.EdgeHierarchy {
		if direction == "incoming" {
			return 0, "hierarchy_parent"
		}
		return 1, "hierarchy_child"
	}
	return 3, "relation"
}

func keepBetterCandidate(candidates map[domain.ProjectionNodeID]traversalCandidate, candidate traversalCandidate) {
	existing, found := candidates[candidate.node.ID]
	if !found || candidate.priority < existing.priority ||
		(candidate.priority == existing.priority && candidate.edgeID < existing.edgeID) {
		candidates[candidate.node.ID] = candidate
	}
}

func sharesEntity(entities []string, wanted map[string]bool) bool {
	for _, entity := range entities {
		if wanted[strings.TrimSpace(entity)] {
			return true
		}
	}
	return false
}

func overlaps(left, right []string) bool {
	wanted := make(map[string]bool, len(left))
	for _, value := range left {
		wanted[value] = true
	}
	for _, value := range right {
		if wanted[value] {
			return true
		}
	}
	return false
}

func (s *Service) candidateItem(ctx context.Context, tenantID domain.TenantID, pin domain.PinnedSpace, candidate traversalCandidate) (domain.RecallItem, bool, error) {
	for _, ref := range candidate.node.EvidenceRefs {
		batch, err := s.store.Batch(ctx, tenantID, ref.BatchID)
		if err != nil {
			return domain.RecallItem{}, false, err
		}
		if batch.SpaceID != pin.SpaceID || batch.State != domain.EvidenceBatchCommitted || batch.MemoryVersion == nil ||
			*batch.MemoryVersion > pin.MemoryVersion || *batch.MemoryVersion > pin.ProjectionEvidenceWatermark {
			continue
		}
		eventIDs, fallbackContent := existingEvents(batch.Events, ref.EventIDs)
		if len(eventIDs) == 0 {
			continue
		}
		content := candidate.node.Content
		if content == "" {
			content = fallbackContent
		}
		return domain.RecallItem{
			Content: content, SourceSpaceID: pin.SpaceID, MemoryVersion: *batch.MemoryVersion,
			Citation: domain.Citation{
				ID:              projectionCitationID(pin, candidate.node.ID, ref.BatchID, eventIDs),
				EvidenceBatchID: ref.BatchID, EventIDs: eventIDs,
			},
			Score: candidateScore(candidate),
			Traversal: &domain.TraversalMetadata{
				ProjectionNodeID: candidate.node.ID, ProjectionVersion: pin.ProjectionVersion,
				ProjectionDigest: pin.ProjectionDigest, RouteKind: candidate.routeKind,
				EdgeID: candidate.edgeID, EdgeKind: candidate.edgeKind, Relation: candidate.relation,
				Direction: candidate.direction,
			},
		}, true, nil
	}
	return domain.RecallItem{}, false, nil
}

func existingEvents(events []domain.EvidenceEvent, wanted []string) ([]string, string) {
	byID := make(map[string]string, len(events))
	for _, event := range events {
		byID[event.ID] = event.Content
	}
	ids := make([]string, 0, len(wanted))
	contents := make([]string, 0, len(wanted))
	for _, eventID := range wanted {
		if content, ok := byID[eventID]; ok {
			ids = append(ids, eventID)
			contents = append(contents, content)
		}
	}
	return ids, strings.Join(contents, "\n")
}

func candidateScore(candidate traversalCandidate) float64 {
	switch candidate.priority {
	case 0:
		return 1
	case 1:
		return 0.95
	case 2:
		return 0.85
	case 3:
		confidence := candidate.confidence
		if confidence < 0 {
			confidence = 0
		}
		if confidence > 1 {
			confidence = 1
		}
		return 0.7 + 0.2*confidence
	default:
		confidence := candidate.confidence
		if confidence < 0 {
			return 0
		}
		if confidence > 1 {
			return 1
		}
		return confidence
	}
}

func projectionCitationID(pin domain.PinnedSpace, nodeID domain.ProjectionNodeID, batchID domain.BatchID, eventIDs []string) string {
	parts := []string{string(pin.SpaceID), string(nodeID), string(batchID), pin.ProjectionDigest}
	parts = append(parts, eventIDs...)
	return domain.StableCitationID("pcit-", parts...)
}
