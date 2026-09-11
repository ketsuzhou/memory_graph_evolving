package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
	"river2.dev/pi-group-chat-host/internal/pi"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

// newReviewRuntime wires one durable Store through every orchestrator port so
// assertions can read the same rows the turn wrote.
func newReviewRuntime() (*Orchestrator, *memorystore.Store) {
	store := memorystore.NewStore()
	dependencies := Dependencies{
		Clock:         fixedClock{now: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)},
		IDs:           &deterministicIDs{},
		RoomStore:     store,
		DeliveryStore: store,
		DAGStore:      store,
		OutboxStore:   store.Outbox(),
	}
	return NewOrchestrator(dependencies), store
}

func reviewAuthority(kind string) ExecutionAuthority {
	return ExecutionAuthority{
		TenantID: "tenant-review", RoomID: "room-review", AgentID: "agent-review",
		ProfileKind:   kind,
		SharedSpaceID: "space-room-shared", PrivateSpaceID: "space-agent-private",
	}
}

func ordinaryTurnInput(authority ExecutionAuthority, piBinary string) turnInput {
	return turnInput{
		authority: authority, roomInput: "please reply", humanMessageID: "human-review",
		humanKey: "human-review-key", piBinary: piBinary, promptRequestID: "prompt-review",
	}
}

// TestToolSideEffectsRequireSuccessfulCompletion proves a failed, unmatched,
// or completion-frame-less tool call never publishes room messages: only the
// call whose start/end pair and success envelope both check out runs.
func TestToolSideEffectsRequireSuccessfulCompletion(t *testing.T) {
	piBinary := writeFakePi(t, map[string][]string{
		"prompt-review": {
			`{"id":"prompt-review","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"tool_execution_start","toolCallId":"call-failed","toolName":"room_reply","args":{"client_operation_id":"op-failed","in_reply_to_message_id":"human-review","content":"dropped failure"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-failed","toolName":"room_reply","isError":true}`,
			`{"type":"tool_execution_end","toolCallId":"call-orphan","toolName":"room_reply","isError":false}`,
			`{"type":"tool_execution_start","toolCallId":"call-no-envelope","toolName":"room_reply","args":{"client_operation_id":"op-no-envelope","in_reply_to_message_id":"human-review","content":"dropped envelope"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-no-envelope","toolName":"room_reply"}`,
			`{"type":"tool_execution_start","toolCallId":"call-renamed","toolName":"room_reply","args":{"client_operation_id":"op-renamed","in_reply_to_message_id":"human-review","content":"dropped rename"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-renamed","toolName":"room_send","isError":false}`,
			`{"type":"tool_execution_start","toolCallId":"call-ok","toolName":"room_reply","args":{"client_operation_id":"op-ok","in_reply_to_message_id":"human-review","content":"published"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-ok","toolName":"room_reply","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"message_id\":\"reply-ok\",\"sequence\":2}}"}]},"isError":false}`,
			`{"type":"agent_end","messages":[],"willRetry":false}`,
			`{"type":"agent_settled"}`,
		},
	})
	orchestrator, store := newReviewRuntime()
	authority := reviewAuthority("ordinary")

	trace, outcome, err := orchestrator.executeTurn(t.Context(), ordinaryTurnInput(authority, piBinary))
	if err != nil {
		t.Fatalf("executeTurn() error = %v", err)
	}
	messages, err := store.Messages(t.Context(), "room-review")
	if err != nil {
		t.Fatalf("store messages: %v", err)
	}
	if len(messages) != 2 || messages[1].Content != "published" {
		t.Fatalf("published messages = %#v, want human plus only the successful call", messages)
	}
	if outcome.segment.State != "settled" {
		t.Fatalf("segment state = %q, want settled", outcome.segment.State)
	}
	if len(trace.replies) != 1 || trace.replies[0].Content != "published" {
		t.Fatalf("trace replies = %#v, want only the successful call", trace.replies)
	}
}

// TestNegativeAcknowledgementLeavesDeliveryPendingWithoutSegment proves the
// rejected prompt path keeps the durable delivery claimable and creates no
// segment — retry is a fresh claim, not a phantom open segment.
func TestNegativeAcknowledgementLeavesDeliveryPendingWithoutSegment(t *testing.T) {
	piBinary := writeFakePi(t, map[string][]string{
		"prompt-review": {
			`{"id":"prompt-review","type":"response","command":"prompt","success":false,"error":"provider refused the turn"}`,
		},
	})
	orchestrator, store := newReviewRuntime()

	_, _, err := orchestrator.executeTurn(t.Context(), ordinaryTurnInput(reviewAuthority("ordinary"), piBinary))
	if !errors.Is(err, pi.ErrPromptRejected) {
		t.Fatalf("executeTurn() error = %v, want %v", err, pi.ErrPromptRejected)
	}
	segments, err := store.Segments(t.Context())
	if err != nil {
		t.Fatalf("store segments: %v", err)
	}
	if len(segments) != 0 {
		t.Fatalf("rejected prompt created segments: %#v", segments)
	}
	deliveries, err := store.RoomDeliveries(t.Context(), "room-review")
	if err != nil {
		t.Fatalf("store deliveries: %v", err)
	}
	if len(deliveries) != 1 || deliveries[0].State != "pending" {
		t.Fatalf("delivery after rejection = %#v, want one pending delivery", deliveries)
	}
	if deliveries[0].Attempts < 1 {
		t.Fatalf("delivery attempts = %d, want the claim attempt recorded", deliveries[0].Attempts)
	}
	reclaimed, err := store.ClaimNext(t.Context(), "room-review", "agent-review", time.Now().UTC())
	if err != nil || string(reclaimed.ID) != string(deliveries[0].ID) {
		t.Fatalf("re-claim after rejection = %#v err=%v, want the same delivery claimable again", reclaimed, err)
	}
}

// TestMemorySessionOwnershipFailsClosed proves the Memory Agent cannot point
// explore or submit at a session other than the one the host opened: a forged
// session_id in model args is dropped before any remote call.
func TestMemorySessionOwnershipFailsClosed(t *testing.T) {
	var mu sync.Mutex
	exploreCalls := 0
	submitCalls := 0
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer review-memory-token" {
			http.Error(w, `{"error":{"code":"UNAUTHORIZED"}}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/explorations":
			_ = json.NewEncoder(w).Encode(map[string]any{"session_id": "exploration-real", "state": "active", "pinned_spaces": []any{}, "items": []any{}, "remaining_steps": 3, "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/explorations/exploration-real:explore":
			mu.Lock()
			exploreCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"session_id": "exploration-real", "operation_id": "explore-1", "state": "active", "items": []any{}, "remaining_steps": 2, "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/explorations/exploration-real:submit":
			mu.Lock()
			submitCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"session_id": "exploration-real", "operation_id": "submit-1", "state": "submitted", "found": false, "summary": "", "citations": []any{}, "duplicate": false})
		default:
			http.NotFound(w, r)
		}
	}))
	defer memory.Close()

	piBinary := writeFakePi(t, map[string][]string{
		"prompt-review": {
			`{"id":"prompt-review","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"tool_execution_start","toolCallId":"start-call","toolName":"memory_start","args":{"client_operation_id":"start-op","query":"find the durable choice","max_steps":4,"max_results":5}}`,
			`{"type":"tool_execution_end","toolCallId":"start-call","toolName":"memory_start","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"session_id\":\"exploration-real\",\"items\":[],\"remaining_steps\":3}}"}]},"isError":false}`,
			`{"type":"tool_execution_start","toolCallId":"explore-call","toolName":"memory_explore","args":{"client_operation_id":"explore-op","session_id":"exploration-forged","anchor_citation_id":"citation-x","relation":"related","limit":5}}`,
			`{"type":"tool_execution_end","toolCallId":"explore-call","toolName":"memory_explore","result":{"content":[{"type":"text","text":"{}"}]},"isError":false}`,
			`{"type":"tool_execution_start","toolCallId":"submit-call","toolName":"memory_submit","args":{"client_operation_id":"submit-op","session_id":"exploration-forged","found":false,"summary":"","citation_ids":[]}}`,
			`{"type":"tool_execution_end","toolCallId":"submit-call","toolName":"memory_submit","result":{"content":[{"type":"text","text":"{}"}]},"isError":false}`,
			`{"type":"agent_end","messages":[],"willRetry":false}`,
			`{"type":"agent_settled"}`,
		},
	})
	orchestrator, store := newReviewRuntime()
	authority := reviewAuthority("memory")
	input := turnInput{
		authority: authority, roomInput: "find the evidence", humanMessageID: "human-review",
		humanKey: "human-review-key", piBinary: piBinary, promptRequestID: "prompt-review",
		memoryClient: memoryclient.NewClient(memory.URL, "review-memory-token", &http.Client{Timeout: 5 * time.Second}, 1<<20),
	}

	trace, outcome, err := orchestrator.executeTurn(t.Context(), input)
	if err != nil {
		t.Fatalf("executeTurn() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if exploreCalls != 0 || submitCalls != 0 {
		t.Fatalf("forged session reached memory: explore=%d submit=%d, want 0/0", exploreCalls, submitCalls)
	}
	if !trace.memoryStarted || trace.memoryExplored || trace.memorySubmitted {
		t.Fatalf("memory trace = started=%v explored=%v submitted=%v, want true/false/false", trace.memoryStarted, trace.memoryExplored, trace.memorySubmitted)
	}
	if outcome.segment.State != "settled" {
		t.Fatalf("segment state = %q, want settled", outcome.segment.State)
	}
	messages, err := store.Messages(t.Context(), "room-review")
	if err != nil {
		t.Fatalf("store messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %#v, want only the human message", messages)
	}
}
