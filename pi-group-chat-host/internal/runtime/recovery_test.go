package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

func TestRestartRecoversPendingDeliveryAndEvidenceOutboxExactlyOnceVisible(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	stageCalls := map[string]int{}
	commitCalls := map[string]int{}
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-memory-token" {
			http.Error(w, `{"error":{"code":"UNAUTHORIZED"}}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/evidence-batches:stage":
			var body struct{ BatchID, IdempotencyKey, SpaceID string }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode stage: %v", err)
				return
			}
			mu.Lock()
			stageCalls[body.IdempotencyKey]++
			duplicate := stageCalls[body.IdempotencyKey] > 1
			mu.Unlock()
			status := http.StatusCreated
			if duplicate {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"batch_id": body.BatchID, "idempotency_key": body.IdempotencyKey, "space_id": body.SpaceID, "state": "staged", "duplicate": duplicate})
		case r.Method == http.MethodPost:
			var body struct {
				CommitID string `json:"commit_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode commit: %v", err)
				return
			}
			mu.Lock()
			commitCalls[body.CommitID]++
			duplicate := commitCalls[body.CommitID] > 1
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"batch_id": "batch-recovered", "commit_id": body.CommitID, "state": "committed", "memory_version": 1, "committed_at": "2026-09-08T11:00:00Z", "duplicate": duplicate})
		default:
			http.NotFound(w, r)
		}
	}))
	defer memory.Close()

	pi := writeFakePi(t, map[string][]string{
		"prompt-recovered": ordinaryPiFrames("prompt-recovered", "reply-recovered", "human-message-001", "Recovered exactly once."),
	})
	initial := RecoveryScenario{
		Authority:  ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", SharedSpaceID: "space-shared", PrivateSpaceID: "space-agent-a"},
		Messages:   []VisibleMessage{{ID: "human-message-001", Sequence: 1, AuthorID: "human-a", Content: "resume this", IdempotencyKey: "human-op-001"}},
		Deliveries: []DeliveryRecord{{ID: "delivery-001", AgentID: "agent-a", MessageID: "human-message-001", State: "pending", Attempts: 1}},
		Segments:   []SegmentRecord{{ID: "segment-previous", DeliveryID: "delivery-previous", State: "settled"}},
		Outbox: []OutboxRecord{
			{ID: "outbox-shared-previous", SourceSegmentID: "segment-previous", Projection: "room_shared", SpaceID: "space-shared", BatchID: "batch-shared-previous", State: "staged", StageAttempts: 1},
			{ID: "outbox-private-previous", SourceSegmentID: "segment-previous", Projection: "agent_private", SpaceID: "space-agent-a", BatchID: "batch-private-previous", State: "pending"},
		},
		DuplicateHumanKey: "human-op-001", DuplicateReplyKey: "reply-recovered",
		MemoryBaseURL: memory.URL, MemoryAuthToken: "test-memory-token", PiBinary: pi, PromptRequestID: "prompt-recovered",
	}

	durableStore := memorystore.NewStore()
	durableOutbox := memorystore.NewOutbox()
	dependencies := Dependencies{
		Clock:         fixedClock{now: time.Date(2026, 9, 8, 11, 15, 45, 446_000_000, time.UTC)},
		IDs:           &deterministicIDs{},
		RoomStore:     durableStore,
		DeliveryStore: durableStore,
		DAGStore:      durableStore,
		OutboxStore:   durableOutbox,
	}
	beforeCrash := NewOrchestrator(dependencies)
	restarted := NewOrchestrator(dependencies)
	if beforeCrash == restarted {
		t.Fatal("restart must reconstruct the runtime while retaining the same durable adapters")
	}

	got, err := restarted.RecoverAfterRestart(t.Context(), initial)
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("runtime recovery contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("recover after restart: %v", err)
	}
	if len(got.Messages) != 2 {
		t.Fatalf("visible messages = %#v, want original human plus one recovered reply", got.Messages)
	}
	if got.Messages[0].ID != "human-message-001" || got.Messages[1].IdempotencyKey != "reply-recovered" {
		t.Fatalf("canonical visible order = %#v", got.Messages)
	}
	if !reflect.DeepEqual(got.RecoveredDeliveryIDs, []string{"delivery-001"}) {
		t.Fatalf("recovered deliveries = %v", got.RecoveredDeliveryIDs)
	}
	if len(got.RecoveredOutboxIDs) != 4 {
		t.Fatalf("recovered/new durable outboxes = %v, want previous pair plus recovered segment pair", got.RecoveredOutboxIDs)
	}
	if len(got.MemoryVisibleBatchIDs) != 4 {
		t.Fatalf("recall-visible batches = %v, want each outbox committed once", got.MemoryVisibleBatchIDs)
	}
	for _, delivery := range got.Deliveries {
		if delivery.ID == "delivery-001" && delivery.State != "settled" {
			t.Fatalf("delivery after recovery = %#v, want settled", delivery)
		}
	}
	for _, outbox := range got.Outbox {
		if outbox.State != "committed" {
			t.Fatalf("outbox remains non-visible after recovery: %#v", outbox)
		}
	}
}

type fixedClock struct {
	now time.Time
}

func (clock fixedClock) Now() time.Time {
	return clock.now
}

type deterministicIDs struct {
	next int
}

func (ids *deterministicIDs) NewID(kind string) string {
	ids.next++
	return kind + "-fixed-" + string(rune('0'+ids.next))
}
