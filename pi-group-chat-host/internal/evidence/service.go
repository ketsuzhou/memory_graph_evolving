package evidence

import (
	"context"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

// Service settles a Delivery, closes its Segment, and creates the shared and
// private outbox entries in one store transaction, so a crash can never
// produce a settled segment without durable evidence projections.
type Service struct {
	store ports.DeliveryStore
}

func NewService(store ports.DeliveryStore) *Service {
	return &Service{store: store}
}

func (s *Service) SettleAndCreateOutbox(
	ctx context.Context,
	deliveryID domain.DeliveryID,
	segmentID domain.SegmentID,
	settledAt time.Time,
	entries []domain.EvidenceOutboxEntry,
) error {
	return s.store.SettleAndCloseSegment(ctx, deliveryID, segmentID, settledAt, entries)
}
