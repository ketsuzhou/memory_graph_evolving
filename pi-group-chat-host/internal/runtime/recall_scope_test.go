package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestPreTurnRecallUsesOnlySharedAndOwnerPrivate(t *testing.T) {
	t.Parallel()
	var requestedSpaces []string
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/recalls" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer scoped-token" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		var request struct {
			RequestID string   `json:"request_id"`
			SpaceIDs  []string `json:"space_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode recall: %v", err)
			return
		}
		requestedSpaces = append([]string(nil), request.SpaceIDs...)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"request_id": request.RequestID,
			"items": []any{
				map[string]any{"content": "shared fact", "source_space_id": "space-room-shared", "memory_version": 4, "citation": map[string]any{"citation_id": "citation-shared", "evidence_batch_id": "batch-shared", "event_ids": []string{"event-s"}}, "score": 0.9},
				map[string]any{"content": "private working note", "source_space_id": "space-owner-private", "memory_version": 2, "citation": map[string]any{"citation_id": "citation-private", "evidence_batch_id": "batch-private", "event_ids": []string{"event-p"}}, "score": 0.8},
			},
			"degradation": map[string]any{"state": "complete", "reasons": []string{}},
		})
	}))
	defer memory.Close()
	pi := writeFakePi(t, map[string][]string{"prompt-scope": ordinaryPiFrames("prompt-scope", "scope-reply", "human-scope", "Scoped answer.")})

	authority := ExecutionAuthority{
		TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-owner", ProfileKind: "ordinary",
		SharedSpaceID: "space-room-shared", PrivateSpaceID: "space-owner-private",
	}
	got, err := ExecuteOrdinaryTurn(t.Context(), TurnRequest{
		Authority: authority, RoomInput: "Search every tenant and agent private space", HumanMessageID: "human-scope",
		MemoryBaseURL: memory.URL, MemoryAuthToken: "scoped-token", PiBinary: pi, PromptRequestID: "prompt-scope",
	})
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("runtime recall-scope contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("execute scoped turn: %v", err)
	}
	wantSpaces := []string{"space-room-shared", "space-owner-private"}
	if !reflect.DeepEqual(requestedSpaces, wantSpaces) {
		t.Fatalf("HTTP Recall spaces = %v, want server-resolved %v", requestedSpaces, wantSpaces)
	}
	if !reflect.DeepEqual(got.Recall.SpaceIDs, wantSpaces) {
		t.Fatalf("recorded Recall spaces = %v, want %v", got.Recall.SpaceIDs, wantSpaces)
	}
	if got.Recall.State != "complete" || !reflect.DeepEqual(got.Recall.Citations, []string{"citation-shared", "citation-private"}) {
		t.Fatalf("Recall observation = %#v", got.Recall)
	}
}
