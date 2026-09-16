package projectionbuilder_test

import (
	"context"
	"testing"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
	"river2.dev/graph-memory-service/internal/projectionbuilder"
)

type perSourceCursorStore struct {
	ports.ConsolidationStore
	state   ports.ProjectionBuildState
	batches []domain.EvidenceBatch
}

func (s *perSourceCursorStore) ListProjectionSpaces(context.Context) ([]domain.Space, error) {
	return []domain.Space{s.state.Space}, nil
}

func (s *perSourceCursorStore) ProjectionBuildState(context.Context, domain.TenantID, domain.SpaceID) (ports.ProjectionBuildState, error) {
	return s.state, nil
}

func (s *perSourceCursorStore) CommittedEvidenceBatches(context.Context, domain.TenantID, domain.SpaceID, int64, int64) ([]domain.EvidenceBatch, error) {
	return s.batches, nil
}

type recordingConsolidator struct {
	input domain.RoundInput
}

func (c *recordingConsolidator) Run(_ context.Context, input domain.RoundInput) (domain.RoundResult, bool, error) {
	c.input = input
	return domain.RoundResult{RoundID: input.RoundID}, false, nil
}

func TestBuildSpaceDoesNotAdvanceCursorPastAnotherRoomPendingEvidence(t *testing.T) {
	t.Parallel()

	const (
		tenant = domain.TenantID("tenant-cursor")
		space  = domain.SpaceID("space-shared")
	)
	memoryVersion := int64(2)
	store := &perSourceCursorStore{
		state: ports.ProjectionBuildState{
			Space:           domain.Space{ID: space, TenantID: tenant, Scope: domain.SpaceShared},
			Head:            domain.ProjectionHead{TenantID: tenant, SpaceID: space, EvidenceWatermark: 0},
			EvidenceThrough: 2,
		},
		// Room A has an earlier unresolved source window. Room B must not make a
		// single global cursor claim every source is consumed through ordinal 2.
		batches: []domain.EvidenceBatch{{
			ID: "batch-room-b", TenantID: tenant, SpaceID: space, StreamID: "room-b", SourceSegmentID: "segment-b",
			State: domain.EvidenceBatchCommitted, MemoryVersion: &memoryVersion,
			Events: []domain.EvidenceEvent{{ID: "event-b", Sequence: 1, Kind: "room_message", Content: "room B evidence"}},
		}},
	}
	consolidator := &recordingConsolidator{}
	builder, err := projectionbuilder.New(store, consolidator, projectionbuilder.DefaultConfig())
	if err != nil {
		t.Fatalf("new projection builder: %v", err)
	}

	if _, _, err := builder.BuildSpace(context.Background(), tenant, space); err != nil {
		t.Fatalf("build shared Space: %v", err)
	}
	if consolidator.input.EvidenceWatermark != 0 {
		t.Fatalf("shared Space cursor advanced to %d past unresolved room-a evidence; want per-source cursor semantics", consolidator.input.EvidenceWatermark)
	}
}

func TestBuildSpaceConsumesOnlyContiguousEvidenceVersions(t *testing.T) {
	t.Parallel()

	const (
		tenant = domain.TenantID("tenant-contiguous")
		space  = domain.SpaceID("space-shared")
	)
	batch := func(version int64) domain.EvidenceBatch {
		return domain.EvidenceBatch{
			ID: domain.BatchID("batch-" + string(rune('0'+version))), TenantID: tenant, SpaceID: space,
			StreamID: "room-a", SourceSegmentID: "segment-a", State: domain.EvidenceBatchCommitted,
			MemoryVersion: &version,
			Events:        []domain.EvidenceEvent{{ID: "event-" + string(rune('0'+version)), Sequence: 1, Kind: "room_message", Content: "evidence"}},
		}
	}
	batchIDs := func(operations []domain.Operation) map[string]bool {
		ids := map[string]bool{}
		for _, operation := range operations {
			if operation.Node != nil {
				for _, ref := range operation.Node.EvidenceRefs {
					ids[string(ref.BatchID)] = true
				}
			}
			if operation.Edge != nil {
				for _, ref := range operation.Edge.EvidenceRefs {
					ids[string(ref.BatchID)] = true
				}
			}
		}
		return ids
	}
	tests := []struct {
		name          string
		batches       []domain.EvidenceBatch
		wantWatermark int64
		wantBatchIDs  []string
		absentBatchID string
	}{
		{
			name: "gap blocks higher committed evidence", batches: []domain.EvidenceBatch{batch(1), batch(3)},
			wantWatermark: 1, wantBatchIDs: []string{"batch-1"}, absentBatchID: "batch-3",
		},
		{
			name: "continuous prefix advances through frozen upper bound", batches: []domain.EvidenceBatch{batch(1), batch(2), batch(3)},
			wantWatermark: 3, wantBatchIDs: []string{"batch-1", "batch-2", "batch-3"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &perSourceCursorStore{state: ports.ProjectionBuildState{
				Space: domain.Space{ID: space, TenantID: tenant, Scope: domain.SpaceShared},
				Head:  domain.ProjectionHead{TenantID: tenant, SpaceID: space, EvidenceWatermark: 0}, EvidenceThrough: 3,
			}, batches: test.batches}
			consolidator := &recordingConsolidator{}
			builder, err := projectionbuilder.New(store, consolidator, projectionbuilder.DefaultConfig())
			if err != nil {
				t.Fatalf("new projection builder: %v", err)
			}
			if _, _, err := builder.BuildSpace(context.Background(), tenant, space); err != nil {
				t.Fatalf("build space: %v", err)
			}
			if consolidator.input.EvidenceWatermark != test.wantWatermark {
				t.Fatalf("evidence watermark = %d, want %d", consolidator.input.EvidenceWatermark, test.wantWatermark)
			}
			gotBatchIDs := batchIDs(consolidator.input.Operations)
			for _, batchID := range test.wantBatchIDs {
				if !gotBatchIDs[batchID] {
					t.Fatalf("operations did not materialize %q: %#v", batchID, gotBatchIDs)
				}
			}
			if test.absentBatchID != "" && gotBatchIDs[test.absentBatchID] {
				t.Fatalf("operations consumed evidence above the gap: %#v", gotBatchIDs)
			}
		})
	}
}
