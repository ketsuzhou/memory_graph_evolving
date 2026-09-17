package runtime

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSkipRecallOmitsPreTurnRecallAndDemotionHeader(t *testing.T) {
	t.Parallel()
	recallHits := 0
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/recalls" {
			recallHits++
		}
		http.NotFound(w, r)
	}))
	defer memory.Close()
	pi := writeFakePi(t, map[string][]string{"prompt-skip": ordinaryPiFrames("prompt-skip", "skip-reply", "human-skip", "Skipped recall.")})

	session := NewSession()
	got, err := session.Turn(t.Context(), TurnRequest{
		Authority: ExecutionAuthority{
			TenantID: "tenant-a", RoomID: "room-skip", AgentID: "agent-primary", ProfileKind: "ordinary",
			SharedSpaceID: "space-shared", PrivateSpaceID: "space-private",
		},
		RoomInput:       "Solve the task",
		HumanMessageID:  "human-skip",
		MemoryBaseURL:   memory.URL,
		MemoryAuthToken: "token",
		PiBinary:        pi,
		PromptRequestID: "prompt-skip",
		SkipRecall:      true,
	})
	if err != nil {
		t.Fatalf("skip-recall turn: %v", err)
	}
	if recallHits != 0 {
		t.Fatalf("SkipRecall still issued %d Recall HTTP calls", recallHits)
	}
	if got.Recall.State != "" || len(got.Recall.Items) != 0 {
		t.Fatalf("recall observation = %#v, want empty", got.Recall)
	}
	if !strings.Contains(got.Prompt, "no pre-turn recall for this turn") {
		t.Fatalf("prompt missing skip snapshot:\n%s", got.Prompt)
	}
	if strings.Contains(got.Prompt, "Memory from earlier sessions") {
		t.Fatalf("SkipRecall must not inject the demotion header:\n%s", got.Prompt)
	}
}

func TestDefaultTurnStillPerformsRecall(t *testing.T) {
	t.Parallel()
	recallHits := 0
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/recalls" {
			http.NotFound(w, r)
			return
		}
		recallHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"recall-prompt-keep","items":[],"degradation":{"state":"empty","reasons":[]}}`))
	}))
	defer memory.Close()
	pi := writeFakePi(t, map[string][]string{"prompt-keep": ordinaryPiFrames("prompt-keep", "keep-reply", "human-keep", "Kept recall.")})

	got, err := ExecuteOrdinaryTurn(t.Context(), TurnRequest{
		Authority: ExecutionAuthority{
			TenantID: "tenant-a", RoomID: "room-keep", AgentID: "agent-owner", ProfileKind: "ordinary",
			SharedSpaceID: "space-shared", PrivateSpaceID: "space-private",
		},
		RoomInput:       "Search memory",
		HumanMessageID:  "human-keep",
		MemoryBaseURL:   memory.URL,
		MemoryAuthToken: "token",
		PiBinary:        pi,
		PromptRequestID: "prompt-keep",
	})
	if err != nil {
		t.Fatalf("default turn: %v", err)
	}
	if recallHits != 1 {
		t.Fatalf("default turn Recall hits = %d, want 1", recallHits)
	}
	if got.Recall.State != "empty" {
		t.Fatalf("default recall state = %q, want empty", got.Recall.State)
	}
	if !strings.Contains(got.Prompt, "Context snapshot:") {
		t.Fatalf("default prompt missing snapshot:\n%s", got.Prompt)
	}
}
