package runtime

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRecallOutageContinuesTurnAndKeepsEvidencePending(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/recalls" {
			t.Errorf("outage fallback attempted unexpected route %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":"UNAVAILABLE","message":"memory offline","request_id":"recall-outage","details":[]}}`))
	}))
	defer memory.Close()
	pi := writeFakePi(t, map[string][]string{"prompt-degraded": ordinaryPiFrames("prompt-degraded", "degraded-reply", "human-degraded", "Answer produced during recall outage.")})

	got, err := ExecuteOrdinaryTurn(t.Context(), TurnRequest{
		Authority: ExecutionAuthority{TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-a", ProfileKind: "ordinary", SharedSpaceID: "space-shared", PrivateSpaceID: "space-private"},
		RoomInput: "continue even if memory is unavailable", HumanMessageID: "human-degraded",
		MemoryBaseURL: memory.URL, MemoryAuthToken: "outage-token", PiBinary: pi, PromptRequestID: "prompt-degraded",
	})
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("runtime degradation contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("degraded turn should continue, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("memory calls = %d, want one scoped Recall and no fallback", calls.Load())
	}
	if !got.PromptSent || !strings.Contains(got.Prompt, "unavailable") {
		t.Fatalf("prompt did not carry explicit unavailable snapshot: sent=%v prompt=%q", got.PromptSent, got.Prompt)
	}
	if got.Delivery.State != "settled" || got.Segment.State != "settled" {
		t.Fatalf("turn did not settle during outage: delivery=%#v segment=%#v", got.Delivery, got.Segment)
	}
	if len(got.Outbox) != 2 {
		t.Fatalf("outbox = %#v, want shared and private entries", got.Outbox)
	}
	seen := map[string]bool{}
	for _, row := range got.Outbox {
		seen[row.Projection] = true
		if row.State != "pending" {
			t.Fatalf("outbox was dropped or falsely visible during outage: %#v", row)
		}
	}
	if !seen["room_shared"] || !seen["agent_private"] {
		t.Fatalf("pending projection set = %v", seen)
	}
}
