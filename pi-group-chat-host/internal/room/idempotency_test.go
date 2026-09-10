package room

import (
	"context"
	"errors"
	"testing"
)

func TestMessageDeliveryAndRoomReplyRetriesPublishOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	service := NewService()

	human := AppendMentionRequest{
		RoomID:          "room-alpha",
		AuthorID:        "human-alice",
		Content:         "@builder explain the first decision",
		IdempotencyKey:  "human-op-001",
		MentionedAgents: []string{"agent-builder"},
	}
	first, err := service.AppendMention(ctx, human)
	requireRoomImplemented(t, err)
	retried, err := service.AppendMention(ctx, human)
	requireRoomImplemented(t, err)

	if first.Message.ID == "" || first.Message.Sequence != 1 {
		t.Fatalf("first canonical message = %#v, want non-empty ID at sequence 1", first.Message)
	}
	if retried.Message != first.Message || !retried.Duplicate {
		t.Fatalf("human retry = %#v, want duplicate of %#v", retried, first)
	}
	if retried.CanonicalMessageCount != 1 || retried.DeliveryCount != 1 {
		t.Fatalf("after human retry counts = messages %d deliveries %d, want 1 and 1", retried.CanonicalMessageCount, retried.DeliveryCount)
	}
	if len(first.Deliveries) != 1 || first.Deliveries[0].AgentID != "agent-builder" || first.Deliveries[0].MessageID != first.Message.ID {
		t.Fatalf("mention deliveries = %#v, want one unique (agent-builder, %q)", first.Deliveries, first.Message.ID)
	}

	reply := ReplyRequest{
		RoomID:             "room-alpha",
		AgentID:            "agent-builder",
		ClientOperationID:  "reply-op-001",
		InReplyToMessageID: first.Message.ID,
		Content:            "The first decision is preserved as cited evidence.",
	}
	published, err := service.PublishReply(ctx, reply)
	requireRoomImplemented(t, err)
	replayed, err := service.PublishReply(ctx, reply)
	requireRoomImplemented(t, err)

	if published.Message.ID == "" || published.Message.Sequence != 2 {
		t.Fatalf("published reply = %#v, want one canonical message at sequence 2", published.Message)
	}
	if replayed.Message != published.Message || !replayed.Duplicate {
		t.Fatalf("room_reply retry = %#v, want duplicate of %#v", replayed, published)
	}
	if replayed.CanonicalMessageCount != 2 {
		t.Fatalf("visible messages after all retries = %d, want exactly 2", replayed.CanonicalMessageCount)
	}
	if replayed.DeliveryCount != 1 {
		t.Fatalf("deliveries after reply retry = %d, want original mention delivery only", replayed.DeliveryCount)
	}
}

func requireRoomImplemented(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("room orchestration contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected room orchestration error: %v", err)
	}
}
