// Package projectionbuilder turns immutable committed evidence into a
// deterministic structural projection and publishes it only through the
// consolidation shadow/replay/CAS service.
package projectionbuilder

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

const (
	defaultMaxEntityRefs   = 16
	defaultMaxSummaryRunes = 512
)

type Store interface {
	ports.ProjectionBuilderStore
	ports.ConsolidationStore
}

type Consolidator interface {
	Run(context.Context, domain.RoundInput) (domain.RoundResult, bool, error)
}

type BuildCallback func(context.Context, domain.Space, domain.RoundResult, error)

type Config struct {
	MaxEntityRefs   int
	MaxSummaryRunes int
	OnBuild         BuildCallback
}

func DefaultConfig() Config {
	return Config{MaxEntityRefs: defaultMaxEntityRefs, MaxSummaryRunes: defaultMaxSummaryRunes}
}

type Builder struct {
	store           Store
	consolidator    Consolidator
	maxEntityRefs   int
	maxSummaryRunes int
	onBuild         BuildCallback
}

func New(store Store, consolidator Consolidator, config Config) (*Builder, error) {
	if store == nil || consolidator == nil {
		return nil, fmt.Errorf("projectionbuilder: store and consolidator are required")
	}
	defaults := DefaultConfig()
	if config.MaxEntityRefs == 0 {
		config.MaxEntityRefs = defaults.MaxEntityRefs
	}
	if config.MaxSummaryRunes == 0 {
		config.MaxSummaryRunes = defaults.MaxSummaryRunes
	}
	if config.MaxEntityRefs < 1 || config.MaxSummaryRunes < 1 {
		return nil, fmt.Errorf("projectionbuilder: entity and summary limits must be positive")
	}
	return &Builder{
		store: store, consolidator: consolidator,
		maxEntityRefs: config.MaxEntityRefs, maxSummaryRunes: config.MaxSummaryRunes,
		onBuild: config.OnBuild,
	}, nil
}

// BuildSpace freezes the current published head and activity watermarks,
// builds only evidence after the head watermark, and asks consolidation to
// validate and CAS-publish the resulting shadow. A failed CAS is returned as
// is; this method never rebases the operation list implicitly.
func (b *Builder) BuildSpace(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID) (result domain.RoundResult, duplicate bool, err error) {
	state, err := b.store.ProjectionBuildState(ctx, tenantID, spaceID)
	if err != nil {
		return domain.RoundResult{}, false, err
	}
	if b.onBuild != nil {
		defer func() { b.onBuild(ctx, state.Space, result, err) }()
	}
	batches, err := b.store.CommittedEvidenceBatches(ctx, tenantID, spaceID, state.Head.EvidenceWatermark, state.EvidenceThrough)
	if err != nil {
		return domain.RoundResult{}, false, err
	}
	// Per-source cursor semantics (SC-3.3, Q51/Q86): the consumable watermark
	// is the contiguous prefix of committed memory versions, not the frozen
	// EvidenceThrough upper bound. One source's unresolved earlier window must
	// stop every source from being claimed as consumed past it.
	watermark := state.Head.EvidenceWatermark
	covered := make(map[int64]bool, len(batches))
	for _, batch := range batches {
		if batch.MemoryVersion != nil {
			covered[*batch.MemoryVersion] = true
		}
	}
	for covered[watermark+1] {
		watermark++
	}
	consumable := make([]domain.EvidenceBatch, 0, len(batches))
	for _, batch := range batches {
		if batch.MemoryVersion == nil {
			return domain.RoundResult{}, false, fmt.Errorf("projectionbuilder: evidence batch %s is outside the frozen committed window", batch.ID)
		}
		if *batch.MemoryVersion <= watermark {
			consumable = append(consumable, batch)
		}
	}
	batches = consumable
	operations, err := b.operations(tenantID, spaceID, batches)
	if err != nil {
		return domain.RoundResult{}, false, err
	}
	roundID := domain.ConsolidationRoundID(domain.StableCitationID(
		"round-", string(tenantID), string(spaceID), fmt.Sprint(state.Head.Version), state.Head.Digest,
		fmt.Sprint(watermark), fmt.Sprint(state.QueryThrough),
	))
	plan := domain.RetrievalReplayPlan{
		ID:                     domain.StableCitationID("replay-", string(roundID), "structural-bm25-v1"),
		BaseVersion:            state.Head.Version,
		BaseDigest:             state.Head.Digest,
		Cases:                  []domain.RetrievalReplayCase{},
		RetrievalPolicyVersion: "candidate-bm25-v1",
		EmbeddingPolicyVersion: "off",
		EvaluatorVersion:       "structural-replay-v1",
		MaxResults:             10,
		MaxRounds:              1,
	}
	return b.consolidator.Run(ctx, domain.RoundInput{
		TenantID: tenantID, SpaceID: spaceID, RoundID: roundID,
		BaseVersion: state.Head.Version, BaseDigest: state.Head.Digest,
		EvidenceWatermark: watermark, QueryWatermark: state.QueryThrough,
		Operations: operations, ReplayPlan: plan,
	})
}

func (b *Builder) operations(tenantID domain.TenantID, spaceID domain.SpaceID, batches []domain.EvidenceBatch) ([]domain.Operation, error) {
	operations := make([]domain.Operation, 0)
	for _, batch := range batches {
		if batch.TenantID != tenantID || batch.SpaceID != spaceID || batch.State != domain.EvidenceBatchCommitted || batch.MemoryVersion == nil {
			return nil, fmt.Errorf("projectionbuilder: evidence batch %s is outside the frozen committed window", batch.ID)
		}
		events := append([]domain.EvidenceEvent(nil), batch.Events...)
		sort.SliceStable(events, func(i, j int) bool {
			if events[i].Sequence != events[j].Sequence {
				return events[i].Sequence < events[j].Sequence
			}
			return events[i].ID < events[j].ID
		})
		allEventIDs := make([]string, len(events))
		for index, event := range events {
			allEventIDs[index] = event.ID
		}
		parentID := batchNodeID(tenantID, spaceID, batch.ID)
		parentRef := domain.EvidenceRef{BatchID: batch.ID, EventIDs: append([]string(nil), allEventIDs...)}
		parent := domain.ProjectionNode{
			ID: parentID, Content: b.batchSummary(batch, events), EvidenceRefs: []domain.EvidenceRef{parentRef},
			Level: 0, Tags: []string{"batch_summary"}, EntityRefs: b.entityRefs(joinEventContent(events)),
		}
		operations = append(operations, domain.Operation{Kind: domain.OperationAddNode, Node: &parent})

		eventNodes := make(map[string]domain.ProjectionNodeID, len(events))
		for _, event := range events {
			leafID := eventNodeID(tenantID, spaceID, batch.ID, event.ID)
			eventNodes[event.ID] = leafID
			leafRef := domain.EvidenceRef{BatchID: batch.ID, EventIDs: []string{event.ID}}
			tags := []string{"evidence_event"}
			if kind := strings.TrimSpace(strings.ToLower(event.Kind)); kind != "" {
				tags = append(tags, kind)
			}
			leaf := domain.ProjectionNode{
				ID: leafID, Content: event.Content, EvidenceRefs: []domain.EvidenceRef{leafRef},
				Level: 1, Tags: tags, EntityRefs: b.entityRefs(event.Content),
			}
			operations = append(operations,
				domain.Operation{Kind: domain.OperationAddNode, Node: &leaf},
				domain.Operation{Kind: domain.OperationAddHierarchyEdge, Edge: &domain.ProjectionEdge{
					ID: hierarchyEdgeID(parentID, leafID), Kind: domain.EdgeHierarchy, Relation: "contains",
					From: parentID, To: leafID, Confidence: 1, EvidenceRefs: []domain.EvidenceRef{leafRef},
				}},
			)
		}
		links := append([]domain.EvidenceLink(nil), batch.Links...)
		sort.SliceStable(links, func(i, j int) bool { return links[i].ID < links[j].ID })
		for _, link := range links {
			from, fromOK := eventNodes[link.FromEventID]
			to, toOK := eventNodes[link.ToEventID]
			if !fromOK || !toOK {
				return nil, fmt.Errorf("projectionbuilder: evidence link %s has a missing endpoint", link.ID)
			}
			relation := strings.TrimSpace(link.Relation)
			if relation == "" {
				return nil, fmt.Errorf("projectionbuilder: evidence link %s has no relation type", link.ID)
			}
			ref := domain.EvidenceRef{BatchID: batch.ID, EventIDs: []string{link.FromEventID, link.ToEventID}}
			operations = append(operations, domain.Operation{Kind: domain.OperationAddRelationEdge, Edge: &domain.ProjectionEdge{
				ID: relationEdgeID(tenantID, spaceID, batch.ID, link.ID, from, to), Kind: domain.EdgeRelation,
				Relation: relation, From: from, To: to, Confidence: 1, EvidenceRefs: []domain.EvidenceRef{ref},
			}})
		}
	}
	operations = append(operations, domain.Operation{Kind: domain.OperationSubmit})
	return operations, nil
}

func batchNodeID(tenantID domain.TenantID, spaceID domain.SpaceID, batchID domain.BatchID) domain.ProjectionNodeID {
	return domain.ProjectionNodeID(domain.StableCitationID("pnb-", string(tenantID), string(spaceID), string(batchID)))
}

func eventNodeID(tenantID domain.TenantID, spaceID domain.SpaceID, batchID domain.BatchID, eventID string) domain.ProjectionNodeID {
	return domain.ProjectionNodeID(domain.StableCitationID("pne-", string(tenantID), string(spaceID), string(batchID), eventID))
}

func hierarchyEdgeID(parent, child domain.ProjectionNodeID) domain.ProjectionEdgeID {
	return domain.ProjectionEdgeID(domain.StableCitationID("phe-", string(parent), string(child)))
}

func relationEdgeID(tenantID domain.TenantID, spaceID domain.SpaceID, batchID domain.BatchID, linkID string, from, to domain.ProjectionNodeID) domain.ProjectionEdgeID {
	return domain.ProjectionEdgeID(domain.StableCitationID("pre-", string(tenantID), string(spaceID), string(batchID), linkID, string(from), string(to)))
}

func (b *Builder) batchSummary(batch domain.EvidenceBatch, events []domain.EvidenceEvent) string {
	parts := make([]string, 0, len(events))
	for _, event := range events {
		if content := strings.TrimSpace(event.Content); content != "" {
			parts = append(parts, content)
		}
	}
	summary := strings.Join(parts, " | ")
	if summary == "" {
		summary = "Committed evidence batch " + string(batch.ID)
	}
	return truncateRunes(summary, b.maxSummaryRunes)
}

func joinEventContent(events []domain.EvidenceEvent) string {
	parts := make([]string, len(events))
	for index, event := range events {
		parts[index] = event.Content
	}
	return strings.Join(parts, " ")
}

func (b *Builder) entityRefs(content string) []string {
	refs := make([]string, 0, b.maxEntityRefs)
	seen := make(map[string]struct{}, b.maxEntityRefs)
	var token []rune
	flush := func() {
		if len(token) == 0 || len(refs) >= b.maxEntityRefs {
			token = token[:0]
			return
		}
		value := strings.ToLower(string(token))
		token = token[:0]
		if utf8.RuneCountInString(value) < 2 {
			return
		}
		if _, duplicate := seen[value]; duplicate {
			return
		}
		seen[value] = struct{}{}
		refs = append(refs, value)
	}
	for _, r := range content {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			token = append(token, unicode.ToLower(r))
		} else {
			flush()
		}
		if len(refs) >= b.maxEntityRefs {
			break
		}
	}
	flush()
	return refs
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
