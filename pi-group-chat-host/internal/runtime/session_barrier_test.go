package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSessionDrainEvidenceFailsClosedUntilEveryProjectionCommits(t *testing.T) {
	t.Setenv("FAKE_PI_VERSION", "0.85.1")

	stagedSpaces := make(map[string]string)
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/recalls":
			var request struct {
				RequestID string `json:"request_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode recall request: %v", err)
				return
			}
			writeSessionBarrierJSON(t, w, http.StatusOK, map[string]any{
				"request_id": request.RequestID,
				"items":      []any{},
				"degradation": map[string]any{
					"state":   "empty",
					"reasons": []any{},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/evidence-batches:stage":
			var request struct {
				BatchID string `json:"batch_id"`
				SpaceID string `json:"space_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode stage request: %v", err)
				return
			}
			stagedSpaces[request.BatchID] = request.SpaceID
			writeSessionBarrierJSON(t, w, http.StatusCreated, map[string]any{
				"batch_id":        request.BatchID,
				"idempotency_key": "stage-" + request.BatchID,
				"space_id":        request.SpaceID,
				"state":           "staged",
				"duplicate":       false,
			})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/evidence-batches/") && strings.HasSuffix(r.URL.Path, ":commit"):
			batchID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/evidence-batches/"), ":commit")
			if stagedSpaces[batchID] == "space-private" {
				writeSessionBarrierJSON(t, w, http.StatusServiceUnavailable, map[string]any{
					"error": map[string]any{"code": "UNAVAILABLE", "message": "private commit unavailable"},
				})
				return
			}
			writeSessionBarrierJSON(t, w, http.StatusOK, map[string]any{
				"batch_id":       batchID,
				"commit_id":      "commit-" + batchID,
				"state":          "committed",
				"memory_version": 1,
				"committed_at":   "2026-09-14T12:00:00Z",
				"duplicate":      false,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer memory.Close()

	pi := writeFakePi(t, map[string][]string{
		"prompt-barrier": ordinaryPiFrames("prompt-barrier", "barrier-reply", "human-barrier", "Barrier test reply."),
	})
	session := NewSession()
	authority := ExecutionAuthority{
		TenantID: "tenant-barrier", RoomID: "room-barrier", AgentID: "agent-barrier", ProfileKind: "ordinary",
		SharedSpaceID: "space-shared", PrivateSpaceID: "space-private",
	}
	if _, err := session.Turn(context.Background(), TurnRequest{
		Authority: authority, RoomInput: "create evidence", HumanMessageID: "human-barrier",
		MemoryBaseURL: memory.URL, MemoryAuthToken: "barrier-token", PiBinary: pi, PromptRequestID: "prompt-barrier",
	}); err != nil {
		t.Fatalf("create pending evidence: %v", err)
	}

	committed, err := session.DrainEvidence(context.Background(), DrainRequest{
		RoomID: authority.RoomID, MemoryBaseURL: memory.URL, MemoryAuthToken: "barrier-token",
	})
	if err == nil {
		t.Fatalf("DrainEvidence committed %v despite a projection commit failure; want fail-closed barrier", committed)
	}
}

func writeSessionBarrierJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("encode memory response: %v", err)
	}
}
