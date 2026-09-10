package memory

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

var snapNow = time.Date(2026, time.September, 8, 12, 30, 0, 0, time.UTC)

func seedHostSnapshotStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	store := NewStore()

	const room = domain.RoomID("room-snap")
	const agent = domain.AgentID("agent-snap")
	created, err := store.CreateRoom(ctx, domain.Room{ID: room, TenantID: "tenant-snap", SharedSpaceID: "space-shared", MemoryAgentID: "agent-memory"}, agent, "agent-memory")
	if err != nil || !created {
		t.Fatalf("create room: created=%v err=%v", created, err)
	}
	message, _, _, err := store.AppendMessageAndDeliveries(ctx, room, "human-key", domain.RoomMessage{
		ID: "msg-1", AuthorID: "human", Content: "durable question", CreatedAt: snapNow,
	}, []domain.Delivery{{ID: "delivery-1", RoomID: room, AgentID: agent, MessageID: "msg-1", State: domain.DeliveryPending}})
	if err != nil {
		t.Fatalf("append message: %v", err)
	}
	_ = message
	if _, _, err := store.PublishToolMessage(ctx, room, agent, "reply-key", domain.RoomMessage{Content: "durable answer"}); err != nil {
		t.Fatalf("publish reply: %v", err)
	}
	claimed, err := store.ClaimNext(ctx, room, agent, snapNow)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.AcceptAndOpenSegment(ctx, claimed.ID, domain.InteractionSegment{ID: "segment-1", RoomID: room, AgentID: agent, DeliveryID: claimed.ID}, snapNow); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if err := store.AppendEvent(ctx, "segment-1", ports.SegmentEvent{ID: "event-1", Sequence: 1, Kind: "room_message", Content: "durable question", OccurredAt: snapNow}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := store.SettleAndCloseSegment(ctx, claimed.ID, "segment-1", snapNow, []domain.EvidenceOutboxEntry{{
		ID: "outbox-1", SourceSegmentID: "segment-1", Projection: domain.ProjectionRoomShared,
		SpaceID: "space-shared", BatchID: "batch-1", State: "pending", NextAttemptAt: snapNow,
	}}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := store.MarkStaged(ctx, "outbox-1"); err != nil {
		t.Fatalf("mark staged: %v", err)
	}
	return store
}

func TestHostSnapshotRoundTripsThroughFile(t *testing.T) {
	ctx := context.Background()
	store := seedHostSnapshotStore(t)
	path := filepath.Join(t.TempDir(), "host-state.json")
	if err := store.PersistToFile(path); err != nil {
		t.Fatalf("persist: %v", err)
	}

	restored := NewStore()
	if err := restored.LoadFromFile(path); err != nil {
		t.Fatalf("load: %v", err)
	}

	// Canonical transcript and idempotency replays survive.
	messages, err := restored.Messages(ctx, "room-snap")
	if err != nil || len(messages) != 2 {
		t.Fatalf("messages after restore = (%d, %v), want 2", len(messages), err)
	}
	if messages[0].Content != "durable question" || messages[1].Content != "durable answer" {
		t.Fatalf("message order after restore: %#v", messages)
	}
	if _, duplicate, err := restored.PublishToolMessage(ctx, "room-snap", "agent-snap", "reply-key", domain.RoomMessage{Content: "durable answer"}); err != nil || !duplicate {
		t.Fatalf("reply replay after restore = (duplicate=%v, %v), want duplicate", duplicate, err)
	}
	if _, _, _, err := restored.AppendMessageAndDeliveries(ctx, "room-snap", "human-key", domain.RoomMessage{ID: "msg-1", AuthorID: "human", Content: "durable question"}, nil); err != nil {
		t.Fatalf("human replay: %v", err)
	}

	// Delivery terminal state, segment, DAG event, and outbox rows survive.
	deliveries, err := restored.RoomDeliveries(ctx, "room-snap")
	if err != nil || len(deliveries) != 1 || deliveries[0].State != domain.DeliverySettled {
		t.Fatalf("deliveries after restore = %#v (%v)", deliveries, err)
	}
	segments, err := restored.Segments(ctx)
	if err != nil || len(segments) != 1 || segments[0].State != domain.SegmentSettled {
		t.Fatalf("segments after restore = %#v (%v)", segments, err)
	}
	entries, err := restored.RecoverPendingOutbox(ctx, snapNow)
	if err != nil || len(entries) != 1 || entries[0].State != "staged" || entries[0].BatchID != "batch-1" {
		t.Fatalf("outbox after restore = %#v (%v)", entries, err)
	}
	snapshots := restored.Entries()
	if len(snapshots) != 1 || snapshots[0].StageAttempts != 1 {
		t.Fatalf("outbox counters after restore = %#v, want stage_attempts=1", snapshots)
	}

	// Claims and single-flight leases restart empty: pending work is claimable.
	fresh := NewStore()
	if err := fresh.Restore([]byte(`{"rooms":[]}`)); err != nil {
		t.Fatalf("empty restore: %v", err)
	}
	if _, err := fresh.ClaimNext(ctx, "room-snap", "agent-snap", snapNow); err == nil {
		t.Fatal("restored-empty store must not serve claims for unknown rooms")
	}
}

func TestHostLoadMissingSnapshotIsCleanBoot(t *testing.T) {
	store := NewStore()
	if err := store.LoadFromFile(filepath.Join(t.TempDir(), "absent.json")); err != nil {
		t.Fatalf("missing snapshot must be a clean first boot: %v", err)
	}
}

func TestHostRestoreRejectsCorruptImage(t *testing.T) {
	store := NewStore()
	if err := store.Restore([]byte("{torn")); err == nil {
		t.Fatal("corrupt snapshot must fail closed")
	}
}
