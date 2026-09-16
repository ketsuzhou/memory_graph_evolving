package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/pi"
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

func TestPreTurnRecallForwardsPinnedSpaceVersions(t *testing.T) {
	t.Parallel()
	var requestedSpaces []string
	var requestedVersions map[string]int64
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/recalls" {
			http.NotFound(w, r)
			return
		}
		var request struct {
			RequestID     string           `json:"request_id"`
			SpaceIDs      []string         `json:"space_ids"`
			SpaceVersions map[string]int64 `json:"space_versions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode recall: %v", err)
			return
		}
		requestedSpaces = append([]string(nil), request.SpaceIDs...)
		requestedVersions = request.SpaceVersions
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"request_id":  request.RequestID,
			"items":       []any{},
			"degradation": map[string]any{"state": "complete", "reasons": []string{}},
		})
	}))
	defer memory.Close()
	pi := writeFakePi(t, map[string][]string{"prompt-pin": ordinaryPiFrames("prompt-pin", "pin-reply", "human-pin", "Pinned answer.")})

	// A test episode reads its own throwaway spaces plus the family's frozen
	// train spaces, pinned to the consolidation snapshot's head versions. The
	// pins must reach the wire verbatim — the service filters retrieval to
	// them — and an unpinned scope stays unpinned on the wire.
	authority := ExecutionAuthority{
		TenantID: "tenant-a", RoomID: "room-a", AgentID: "agent-owner", ProfileKind: "ordinary",
		SharedSpaceID: "space-test-shared", PrivateSpaceID: "space-test-private",
		RecallSpaceIDs:      []string{"space-test-shared", "space-test-private", "space-alpha-train-1-shared"},
		RecallSpaceVersions: map[string]int64{"space-alpha-train-1-shared": 7, "space-test-private": 3},
	}
	if _, err := ExecuteOrdinaryTurn(t.Context(), TurnRequest{
		Authority: authority, RoomInput: "Use only the frozen train graph", HumanMessageID: "human-pin",
		MemoryBaseURL: memory.URL, MemoryAuthToken: "scoped-token", PiBinary: pi, PromptRequestID: "prompt-pin",
	}); err != nil {
		t.Fatalf("execute pinned turn: %v", err)
	}
	wantSpaces := []string{"space-test-shared", "space-test-private", "space-alpha-train-1-shared"}
	if !reflect.DeepEqual(requestedSpaces, wantSpaces) {
		t.Fatalf("HTTP Recall spaces = %v, want %v", requestedSpaces, wantSpaces)
	}
	wantVersions := map[string]int64{"space-alpha-train-1-shared": 7, "space-test-private": 3}
	if !reflect.DeepEqual(requestedVersions, wantVersions) {
		t.Fatalf("HTTP Recall space_versions = %v, want %v", requestedVersions, wantVersions)
	}
}

func TestMemoryProfileToolsAppendExtrasInSurfaceOrder(t *testing.T) {
	base := pi.MemoryAgentToolSurface()
	withExtras := memoryProfileTools([]string{"trajectory_read", "skills_list"})
	if len(withExtras) != len(base)+2 {
		t.Fatalf("extras must extend, not replace: %v", withExtras)
	}
	for index, tool := range base {
		if withExtras[index] != tool {
			t.Fatalf("surface order changed: %v", withExtras)
		}
	}
	if withExtras[len(base)] != "trajectory_read" || withExtras[len(base)+1] != "skills_list" {
		t.Fatalf("extras missing: %v", withExtras)
	}
	profile := domain.AgentProfile{
		Kind: domain.ProfileMemory, BuiltinToolsEnabled: false, AllowedToolNames: withExtras,
	}
	for _, tool := range withExtras {
		if err := pi.ValidateToolInvocation(profile, tool); err != nil {
			t.Fatalf("extra tool %q rejected by host validation: %v", tool, err)
		}
	}
	if err := pi.ValidateToolInvocation(profile, "exec"); err == nil {
		t.Fatal("memory profile must still reject non-surface tools")
	}
}
