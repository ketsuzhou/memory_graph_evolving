package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
)

var reviewNow = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func newReviewStoreWithDeliveries(t *testing.T, agentIDs ...domain.AgentID) *Store {
	t.Helper()
	store := NewStore()
	room := domain.Room{ID: "room-review", TenantID: "tenant-review", SharedSpaceID: "space-room-shared"}
	if _, err := store.CreateRoom(context.Background(), room, agentIDs[0], agentIDs[0]); err != nil {
		t.Fatalf("create room: %v", err)
	}
	message := domain.RoomMessage{ID: "human-review", AuthorID: "human", Content: "hello", CreatedAt: reviewNow}
	var deliveries []domain.Delivery
	for index, agentID := range agentIDs {
		deliveries = append(deliveries, domain.Delivery{
			ID: domain.DeliveryID("delivery-" + string(rune('a'+index))), RoomID: room.ID,
			AgentID: agentID, MessageID: message.ID, State: domain.DeliveryPending,
		})
	}
	if _, _, _, err := store.AppendMessageAndDeliveries(context.Background(), room.ID, "human-review-key", message, deliveries); err != nil {
		t.Fatalf("append message: %v", err)
	}
	return store
}

// TestReplyKeyScopeIsAgentLocal proves the tool-message idempotency scope is
// (room, agent, client_operation_id): a key reused by another agent never
// collides, while the owning agent's key is content-frozen.
func TestReplyKeyScopeIsAgentLocal(t *testing.T) {
	store := newReviewStoreWithDeliveries(t, "agent-a", "agent-b")
	ctx := context.Background()

	first, duplicate, err := store.PublishToolMessage(ctx, "room-review", "agent-a", "op-shared", domain.RoomMessage{Content: "from agent a"})
	if err != nil || duplicate {
		t.Fatalf("first publish = (%#v, %v, %v)", first, duplicate, err)
	}
	other, duplicate, err := store.PublishToolMessage(ctx, "room-review", "agent-b", "op-shared", domain.RoomMessage{Content: "from agent b"})
	if err != nil {
		t.Fatalf("another agent must own its own key scope: %v", err)
	}
	if duplicate || other.ID == first.ID || other.Sequence == first.Sequence {
		t.Fatalf("cross-agent publish = (%#v, duplicate=%v), want a distinct second message", other, duplicate)
	}
	if _, _, err := store.PublishToolMessage(ctx, "room-review", "agent-a", "op-shared", domain.RoomMessage{Content: "rewritten"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same agent key with new content = %v, want %v", err, ErrIdempotencyConflict)
	}
	replayed, duplicate, err := store.PublishToolMessage(ctx, "room-review", "agent-a", "op-shared", domain.RoomMessage{Content: "from agent a"})
	if err != nil || !duplicate || replayed.ID != first.ID {
		t.Fatalf("identical replay = (%#v, %v, %v), want the original message as duplicate", replayed, duplicate, err)
	}
}

// TestReplyKeyScopeSurvivesSnapshotRoundTrip proves the agent dimension of a
// reply key persists through the disk image, so a restarted host still treats
// the two agents' same-key operations as independent.
func TestReplyKeyScopeSurvivesSnapshotRoundTrip(t *testing.T) {
	store := newReviewStoreWithDeliveries(t, "agent-a", "agent-b")
	ctx := context.Background()
	if _, _, err := store.PublishToolMessage(ctx, "room-review", "agent-a", "op-shared", domain.RoomMessage{Content: "from agent a"}); err != nil {
		t.Fatalf("publish agent a: %v", err)
	}
	if _, _, err := store.PublishToolMessage(ctx, "room-review", "agent-b", "op-shared", domain.RoomMessage{Content: "from agent b"}); err != nil {
		t.Fatalf("publish agent b: %v", err)
	}

	image, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restarted := NewStore()
	if err := restarted.Restore(image); err != nil {
		t.Fatalf("restore: %v", err)
	}
	for agent, want := range map[domain.AgentID]string{"agent-a": "from agent a", "agent-b": "from agent b"} {
		replayed, duplicate, err := restarted.PublishToolMessage(ctx, "room-review", agent, "op-shared", domain.RoomMessage{Content: want})
		if err != nil || !duplicate || replayed.Content != want {
			t.Fatalf("agent %s replay after restore = (%q, %v, %v)", agent, replayed.Content, duplicate, err)
		}
	}
	if _, _, err := restarted.PublishToolMessage(ctx, "room-review", "agent-a", "op-shared", domain.RoomMessage{Content: "rewritten"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict after restore = %v, want %v", err, ErrIdempotencyConflict)
	}
}

// TestClaimNextIsSingleFlightPerAgent proves a second claim by the same agent
// while one delivery is in flight returns that delivery instead of letting a
// second turn run, while a different agent claims independently.
func TestClaimNextIsSingleFlightPerAgent(t *testing.T) {
	store := newReviewStoreWithDeliveries(t, "agent-a", "agent-b")
	ctx := context.Background()

	claimed, err := store.ClaimNext(ctx, "room-review", "agent-a", reviewNow)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	reclaimed, err := store.ClaimNext(ctx, "room-review", "agent-a", reviewNow)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if reclaimed.ID != claimed.ID {
		t.Fatalf("second claim = %q, want the in-flight %q", reclaimed.ID, claimed.ID)
	}
	other, err := store.ClaimNext(ctx, "room-review", "agent-b", reviewNow)
	if err != nil {
		t.Fatalf("other agent claim: %v", err)
	}
	if other.ID == claimed.ID {
		t.Fatalf("agent-b claimed agent-a's in-flight delivery %q", other.ID)
	}

	if err := store.SettleAndCloseSegment(ctx, claimed.ID, "segment-a", reviewNow, nil); err != nil {
		t.Fatalf("settle first delivery: %v", err)
	}
	fresh, err := store.ClaimNext(ctx, "room-review", "agent-a", reviewNow)
	if err == nil && fresh.ID == claimed.ID {
		t.Fatalf("settled delivery %q was handed out again", fresh.ID)
	}
}

// TestCloseWithoutOpenedSegmentNeverCreatesGhostSegment proves Fail and Abort
// on a delivery whose segment never opened (negative acknowledgement, pre-ack
// crash) finalize only the delivery — no durable segment row appears.
func TestCloseWithoutOpenedSegmentNeverCreatesGhostSegment(t *testing.T) {
	t.Run("fail closes delivery only", func(t *testing.T) {
		store := newReviewStoreWithDeliveries(t, "agent-a")
		ctx := context.Background()
		claimed, err := store.ClaimNext(ctx, "room-review", "agent-a", reviewNow)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := store.FailAndCloseSegment(ctx, claimed.ID, "segment-never-opened", "PI_ERROR", reviewNow); err != nil {
			t.Fatalf("fail and close: %v", err)
		}
		assertDeliveryTerminal(t, store, claimed.ID, domain.DeliveryFailed)
		if segments, _ := store.Segments(ctx); len(segments) != 0 {
			t.Fatalf("fail without opened segment created rows: %#v", segments)
		}
	})
	t.Run("abort closes delivery only", func(t *testing.T) {
		store := newReviewStoreWithDeliveries(t, "agent-a")
		ctx := context.Background()
		claimed, err := store.ClaimNext(ctx, "room-review", "agent-a", reviewNow)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := store.AbortAndCloseSegment(ctx, claimed.ID, "segment-never-opened", "PI_CANCELLED", reviewNow); err != nil {
			t.Fatalf("abort and close: %v", err)
		}
		assertDeliveryTerminal(t, store, claimed.ID, domain.DeliveryAborted)
		if segments, _ := store.Segments(ctx); len(segments) != 0 {
			t.Fatalf("abort without opened segment created rows: %#v", segments)
		}
	})
}

func assertDeliveryTerminal(t *testing.T, store *Store, deliveryID domain.DeliveryID, want domain.DeliveryState) {
	t.Helper()
	deliveries, err := store.RoomDeliveries(context.Background(), "room-review")
	if err != nil {
		t.Fatalf("room deliveries: %v", err)
	}
	for _, delivery := range deliveries {
		if delivery.ID == deliveryID {
			if delivery.State != want {
				t.Fatalf("delivery %q state = %q, want %q", deliveryID, delivery.State, want)
			}
			return
		}
	}
	t.Fatalf("delivery %q disappeared", deliveryID)
}
