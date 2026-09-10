package memory_test

import (
	"context"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

func TestAppendMessageCreatesAtMostOneDeliveryPerAgentAndMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memorystore.NewStore()
	message := domain.RoomMessage{
		ID:             "message-1",
		RoomID:         "room-1",
		Sequence:       1,
		AuthorID:       "human-1",
		Content:        "hello",
		CreatedAt:      time.Date(2026, 9, 8, 11, 0, 0, 0, time.UTC),
		IdempotencyKey: "human-operation-1",
	}
	first := domain.Delivery{
		ID:        "delivery-1",
		RoomID:    message.RoomID,
		AgentID:   "agent-1",
		MessageID: message.ID,
		State:     domain.DeliveryPending,
	}
	sameUniqueKey := first
	sameUniqueKey.ID = "delivery-2"

	firstMessage, firstDeliveries, duplicate, err := store.AppendMessageAndDeliveries(
		ctx,
		message.RoomID,
		message.IdempotencyKey,
		message,
		[]domain.Delivery{first, sameUniqueKey},
	)
	if err != nil {
		t.Fatalf("first AppendMessageAndDeliveries() error = %v", err)
	}
	if duplicate {
		t.Fatal("first AppendMessageAndDeliveries() reported a duplicate publication")
	}
	if len(firstDeliveries) != 1 {
		t.Fatalf("delivery count = %d, want exactly 1 for one (agent_id,message_id)", len(firstDeliveries))
	}

	secondMessage, secondDeliveries, duplicate, err := store.AppendMessageAndDeliveries(
		ctx, message.RoomID, message.IdempotencyKey, message, []domain.Delivery{sameUniqueKey},
	)
	if err != nil {
		t.Fatalf("replayed AppendMessageAndDeliveries() error = %v", err)
	}
	if !duplicate {
		t.Fatal("replayed AppendMessageAndDeliveries() did not report a duplicate publication")
	}
	if firstMessage.ID != secondMessage.ID {
		t.Fatalf("replay message ID = %q, want original %q", secondMessage.ID, firstMessage.ID)
	}
	if len(secondDeliveries) != 1 || firstDeliveries[0].ID != secondDeliveries[0].ID {
		t.Fatalf("replay deliveries = %#v, want original %#v", secondDeliveries, firstDeliveries)
	}
}
