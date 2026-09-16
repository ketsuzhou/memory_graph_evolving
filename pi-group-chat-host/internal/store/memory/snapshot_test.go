package memory

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
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
	outboxClaim, _, err := store.ClaimPending(ctx, snapNow)
	if err != nil {
		t.Fatalf("claim outbox row: %v", err)
	}
	if err := store.MarkStaged(ctx, outboxClaim); err != nil {
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

// R5 round 6: an exhausted outbox epoch fails closed at restore, before any
// receiver state is replaced — a rejected image must leave the store exactly
// as it was (rows still staged and present, epoch untouched), and a later
// valid image must still install cleanly.
func TestHostRestoreRejectsExhaustedEpochLeavingStoreUnchanged(t *testing.T) {
	ctx := context.Background()
	store := seedHostSnapshotStore(t)
	image, err := store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Re-encode the image with an exhausted epoch while preserving every
	// other field verbatim.
	var whole map[string]json.RawMessage
	if err := json.Unmarshal(image, &whole); err != nil {
		t.Fatalf("decode snapshot fields: %v", err)
	}
	exhausted, err := json.Marshal(math.MaxInt64)
	if err != nil {
		t.Fatalf("encode exhausted epoch: %v", err)
	}
	whole["outbox_epoch"] = exhausted
	tampered, err := json.Marshal(whole)
	if err != nil {
		t.Fatalf("re-encode exhausted snapshot: %v", err)
	}

	// The rejected image leaves the store untouched: the staged row is
	// still there with its workflow state, and nothing was replaced.
	if err := store.Restore(tampered); err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("exhausted-epoch restore = %v, want fail-closed exhaustion error", err)
	}
	pending, err := store.ListPending(ctx, snapNow)
	if err != nil {
		t.Fatalf("list pending after rejected restore: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "outbox-1" || pending[0].State != "staged" {
		t.Fatalf("rejected restore disturbed the store: %#v, want the staged row intact", pending)
	}
	// A later valid image still restores cleanly over the untouched store.
	if err := store.Restore(image); err != nil {
		t.Fatalf("restore after rejection: %v", err)
	}
	recovered, err := store.ListPending(ctx, snapNow)
	if err != nil {
		t.Fatalf("list pending after valid restore: %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != "outbox-1" || recovered[0].State != "staged" {
		t.Fatalf("post-rejection restore rows = %#v, want the staged row", recovered)
	}
}
