package evidence_test

import (
	"context"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/evidence"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

func TestSettlingSegmentCreatesSharedAndPrivateOutboxInOneTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	service := evidence.NewService(store)
	settledAt := time.Date(2026, 9, 8, 11, 0, 0, 0, time.UTC)
	segmentID := domain.SegmentID("segment-1")
	entries := []domain.EvidenceOutboxEntry{
		{
			ID:              "outbox-shared-1",
			SourceSegmentID: segmentID,
			Projection:      domain.ProjectionRoomShared,
			SpaceID:         "space-room-shared",
			BatchID:         "batch-shared-1",
			State:           "pending",
			NextAttemptAt:   settledAt,
		},
		{
			ID:              "outbox-private-1",
			SourceSegmentID: segmentID,
			Projection:      domain.ProjectionAgentPrivate,
			SpaceID:         "space-agent-private",
			BatchID:         "batch-private-1",
			State:           "pending",
			NextAttemptAt:   settledAt,
		},
	}

	err := service.SettleAndCreateOutbox(ctx, "delivery-1", segmentID, settledAt, entries)
	if err != nil {
		t.Fatalf("SettleAndCreateOutbox() error = %v", err)
	}

	got, err := store.OutboxEntriesBySegment(ctx, segmentID)
	if err != nil {
		t.Fatalf("OutboxEntriesBySegment() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("outbox entry count = %d, want exactly 2", len(got))
	}

	byProjection := make(map[domain.EvidenceProjection]domain.EvidenceOutboxEntry, len(got))
	for _, entry := range got {
		if entry.SourceSegmentID != segmentID {
			t.Fatalf("outbox %q source segment = %q, want %q", entry.ID, entry.SourceSegmentID, segmentID)
		}
		byProjection[entry.Projection] = entry
	}
	shared, hasShared := byProjection[domain.ProjectionRoomShared]
	private, hasPrivate := byProjection[domain.ProjectionAgentPrivate]
	if !hasShared || !hasPrivate {
		t.Fatalf("outbox projections = %v, want shared and private", byProjection)
	}
	if shared.ID == private.ID {
		t.Fatalf("shared and private outbox IDs are both %q, want distinct IDs", shared.ID)
	}
	if shared.SpaceID == private.SpaceID {
		t.Fatalf("shared and private spaces are both %q, want distinct spaces", shared.SpaceID)
	}
}
