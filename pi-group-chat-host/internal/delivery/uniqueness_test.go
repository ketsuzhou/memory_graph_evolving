package delivery_test

import (
	"context"
	"testing"

	"river2.dev/pi-group-chat-host/internal/domain"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

func TestDeliveryUniquenessIsAgentIDAndMessageID(t *testing.T) {
	t.Parallel()

	store := memorystore.NewStore()
	message := domain.RoomMessage{
		ID:             "message-1",
		RoomID:         "room-1",
		Sequence:       1,
		IdempotencyKey: "append-1",
	}
	first := domain.Delivery{
		ID:        "delivery-1",
		RoomID:    message.RoomID,
		AgentID:   "agent-1",
		MessageID: message.ID,
		State:     domain.DeliveryPending,
	}
	conflict := first
	conflict.ID = "delivery-2"

	_, deliveries, _, err := store.AppendMessageAndDeliveries(
		context.Background(),
		message.RoomID,
		message.IdempotencyKey,
		message,
		[]domain.Delivery{first, conflict},
	)
	if err != nil {
		t.Fatalf("AppendMessageAndDeliveries() error = %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("delivery count = %d, want exactly 1 for duplicate (agent_id,message_id)", len(deliveries))
	}
}
