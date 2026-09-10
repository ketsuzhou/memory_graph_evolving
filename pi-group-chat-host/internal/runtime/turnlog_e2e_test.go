package runtime

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTurnWritesHostEventLog runs one full settled turn and asserts the
// per-episode event log captures every mechanism stage an evaluator needs
// after a batch: turn lifecycle, recall payload shape, Pi frame stream, tool
// adjudication, settlement, and the drain boundary.
func TestTurnWritesHostEventLog(t *testing.T) {
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/recalls" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"request_id": "recall-prompt-log-1",
			"items": []any{
				map[string]any{"content": "earlier decision", "source_space_id": "space-room-shared", "memory_version": 3,
					"citation": map[string]any{"citation_id": "citation-1", "evidence_batch_id": "batch-1", "event_ids": []string{"event-1"}},
					"score": 0.75},
			},
			"degradation": map[string]any{"state": "complete", "reasons": []string{}},
		})
	}))
	defer memory.Close()
	piBinary := writeFakePi(t, map[string][]string{
		"prompt-log-1": ordinaryPiFrames("prompt-log-1", "op-log", "human-log", "Final answer."),
	})

	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "episodes", "0001-task", "host-events.jsonl")
	stderrPath := filepath.Join(dir, "episodes", "0001-task", "pi-stderr.log")

	session := NewSession()
	_, err := session.Turn(t.Context(), TurnRequest{
		Authority: ExecutionAuthority{
			TenantID: "tenant-log", RoomID: "room-log", AgentID: "agent-owner", ProfileKind: "ordinary",
			WorkingDirectory: filepath.Join(dir, "work"),
			SharedSpaceID:    "space-room-shared", PrivateSpaceID: "space-owner-private",
		},
		RoomInput: "please answer", HumanMessageID: "human-log",
		MemoryBaseURL: memory.URL, MemoryAuthToken: "token", PiBinary: piBinary,
		PromptRequestID: "prompt-log-1",
		EventLogPath:    eventLogPath,
		PiStderrPath:    stderrPath,
	})
	if err != nil {
		t.Fatalf("turn: %v", err)
	}

	content, err := os.ReadFile(eventLogPath)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	var events []map[string]any
	for _, line := range splitLines(content) {
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("event log line is not JSON: %v (%q)", err, line)
		}
		events = append(events, entry)
	}
	sequence := make([]string, 0, len(events))
	for _, entry := range events {
		sequence = append(sequence, entry["event"].(string))
	}
	for _, required := range []string{
		"turn_start", "recall", "pi_spawn", "pi_prompt_accepted", "pi_agent_start",
		"pi_tool_start", "pi_tool_end", "tool_invocation", "segment_settled", "turn_end",
	} {
		if !contains(sequence, required) {
			t.Fatalf("event %q missing from log; sequence was %v", required, sequence)
		}
	}

	var recallEvent map[string]any
	var invocationEvent map[string]any
	var turnEnd map[string]any
	for _, entry := range events {
		switch entry["event"] {
		case "recall":
			recallEvent = entry
		case "tool_invocation":
			invocationEvent = entry
		case "turn_end":
			turnEnd = entry
		}
	}
	if recallEvent["state"] != "complete" || recallEvent["duration_ms"].(float64) < 0 {
		t.Fatalf("recall event malformed: %v", recallEvent)
	}
	items := recallEvent["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("recall items = %v, want 1", items)
	}
	item := items[0].(map[string]any)
	if item["space"] != "space-room-shared" || item["score"].(float64) != 0.75 || item["preview"] != "earlier decision" {
		t.Fatalf("recall item malformed: %v", item)
	}
	if invocationEvent["tool"] != "room_reply" || invocationEvent["outcome"] != "published" {
		t.Fatalf("tool_invocation event malformed: %v", invocationEvent)
	}
	if invocationEvent["message_id"] == "" || invocationEvent["content_bytes"].(float64) == 0 {
		t.Fatalf("published invocation missing message identity: %v", invocationEvent)
	}
	if turnEnd["status"] != "settled" || turnEnd["duration_ms"].(float64) <= 0 {
		t.Fatalf("turn_end event malformed: %v", turnEnd)
	}

	if _, err := os.Stat(stderrPath); err != nil {
		t.Fatalf("pi stderr file not created: %v", err)
	}
}

func TestTurnEventLogExplainsPromptFailure(t *testing.T) {
	// A Pi that dies right after echoing frames surfaces as stdout EOF; the
	// event log must record the prompt_failure plus whatever stderr tail the
	// process left behind.
	piBinary := writeFakePi(t, map[string][]string{"prompt-die-1": {
		`{"id":"prompt-die-1","type":"response","command":"prompt","success":true}`,
	}})
	// Patch the fake Pi to announce its death on stderr, then exit.
	script, err := os.ReadFile(piBinary)
	if err != nil {
		t.Fatalf("read fake Pi: %v", err)
	}
	patched := strings.ReplaceAll(string(script), "while IFS= read -r line; do",
		"while IFS= read -r line; do\n  printf '%s\\n' 'provider endpoint reset by peer' >&2\n  exit 3")
	if err := os.WriteFile(piBinary, []byte(patched), 0o700); err != nil {
		t.Fatalf("patch fake Pi: %v", err)
	}

	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"request_id": "recall-prompt-die-1", "items": []any{},
			"degradation": map[string]any{"state": "empty", "reasons": []string{}},
		})
	}))
	defer memory.Close()

	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "host-events.jsonl")
	stderrPath := filepath.Join(dir, "pi-stderr.log")

	session := NewSession()
	_, err = session.Turn(t.Context(), TurnRequest{
		Authority: ExecutionAuthority{
			TenantID: "tenant-die", RoomID: "room-die", AgentID: "agent-owner", ProfileKind: "ordinary",
			WorkingDirectory: filepath.Join(dir, "work"),
			SharedSpaceID:    "space-room-shared", PrivateSpaceID: "space-owner-private",
		},
		RoomInput: "please answer", HumanMessageID: "human-die",
		MemoryBaseURL: memory.URL, MemoryAuthToken: "token", PiBinary: piBinary,
		PromptRequestID: "prompt-die-1",
		EventLogPath:    eventLogPath,
		PiStderrPath:    stderrPath,
	})
	if err == nil {
		t.Fatal("turn with dying Pi unexpectedly succeeded")
	}

	content, readErr := os.ReadFile(stderrPath)
	if readErr != nil {
		t.Fatalf("read stderr capture: %v", readErr)
	}
	if string(content) == "" {
		t.Fatal("pi stderr capture is empty; the death reason was lost")
	}
	logContent, readErr := os.ReadFile(eventLogPath)
	if readErr != nil {
		t.Fatalf("read event log: %v", readErr)
	}
	if string(logContent) == "" || !strings.Contains(string(logContent), "prompt_failure") {
		t.Fatalf("event log missing prompt_failure: %s", logContent)
	}
}

func splitLines(content []byte) [][]byte {
	var lines [][]byte
	start := 0
	for index, b := range content {
		if b == '\n' {
			lines = append(lines, content[start:index])
			start = index + 1
		}
	}
	if start < len(content) {
		lines = append(lines, content[start:])
	}
	return lines
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
