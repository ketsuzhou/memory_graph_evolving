package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestTurnLogWritesOneJSONLinePerEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "episodes", "0001-task", "host-events.jsonl")
	log, closeLog, err := openTurnLog(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var nilLog *turnLog
	nilLog.event("must-not-panic", nil) // nil logger is silent, not fatal

	log.event("turn_start", map[string]any{"room_id": "room-1", "room_input_bytes": 42})
	log.event("recall", map[string]any{"state": "complete", "items": 2})
	closeLog()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), content)
	}
	var first, second map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("line 1 not JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("line 2 not JSON: %v", err)
	}
	if first["event"] != "turn_start" || first["room_id"] != "room-1" || first["room_input_bytes"].(float64) != 42 {
		t.Fatalf("turn_start line malformed: %v", first)
	}
	if _, ok := first["ts"].(string); !ok || first["ts"] == "" {
		t.Fatalf("ts missing: %v", first)
	}
	if second["state"] != "complete" || second["items"].(float64) != 2 {
		t.Fatalf("recall line malformed: %v", second)
	}
}

func TestOpenTurnLogEmptyPathIsNoop(t *testing.T) {
	log, closeLog, err := openTurnLog("")
	if err != nil || log != nil {
		t.Fatalf("empty path must yield (nil, noop, nil), got (%v, _, %v)", log, err)
	}
	closeLog()
}

func TestOpenTurnLogAppendsAcrossHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host-events.jsonl")
	first, closeFirst, err := openTurnLog(path)
	if err != nil {
		t.Fatalf("open first: %v", err)
	}
	first.event("turn_end", map[string]any{"status": "settled"})
	closeFirst()

	second, closeSecond, err := openTurnLog(path)
	if err != nil {
		t.Fatalf("open second: %v", err)
	}
	second.event("drain_end", map[string]any{"committed": []string{"batch-1"}})
	closeSecond()

	content, _ := os.ReadFile(path)
	if got := strings.Count(string(content), "\n"); got != 2 {
		t.Fatalf("expected 2 total lines across handles, got %d", got)
	}
}

func TestStderrTailKeepsOnlyTheLastWindow(t *testing.T) {
	tail := &stderrTail{}
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for round := 0; round < 100; round++ {
				if _, err := tail.Write([]byte{byte('a' + worker)}); err != nil {
					t.Errorf("write: %v", err)
				}
			}
		}(worker)
	}
	group.Wait()
	if len(tail.String()) != 400 {
		t.Fatalf("tail should hold all 400 bytes, held %d", len(tail.String()))
	}
	big := strings.Repeat("x", stderrTailLimit+1000)
	if _, err := tail.Write([]byte(big)); err != nil {
		t.Fatalf("big write: %v", err)
	}
	held := tail.String()
	if len(held) != stderrTailLimit || !strings.HasSuffix(held, strings.Repeat("x", stderrTailLimit)) {
		t.Fatalf("tail should hold exactly the last %d bytes, held %d", stderrTailLimit, len(held))
	}
}

func TestTruncateString(t *testing.T) {
	if got := truncateString("short", 10); got != "short" {
		t.Fatalf("short string altered: %q", got)
	}
	if got := truncateString("0123456789", 4); got != "0123" {
		t.Fatalf("long string not truncated: %q", got)
	}
}
