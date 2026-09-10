package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// turnLog writes one JSON object per line to a per-episode file. It is the
// Host-side mirror of the graph-memory-service access log: turn lifecycle,
// Pi RPC events, tool-invocation adjudication, memory calls with durations,
// and evidence drain outcomes. A nil logger is valid and silent, so callers
// that opt out (tracer paths, tests) need no guards of their own.
type turnLog struct {
	mu sync.Mutex
	w  io.Writer
}

// openTurnLog opens (creating if needed) an append-only event log at path.
// An empty path yields a nil logger plus a no-op closer: logging is opt-in
// per turn and per drain, and both append to the same episode file.
func openTurnLog(path string) (*turnLog, func(), error) {
	if path == "" {
		return nil, func() {}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, func() {}, fmt.Errorf("create event log dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open event log: %w", err)
	}
	log := &turnLog{w: file}
	return log, func() { _ = file.Close() }, nil
}

// event emits {"ts":..., "event":event, ...fields}. Failures are dropped:
// event logging must never fail a turn.
func (l *turnLog) event(event string, fields map[string]any) {
	if l == nil {
		return
	}
	entry := make(map[string]any, len(fields)+2)
	entry["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	entry["event"] = event
	for key, value := range fields {
		entry[key] = value
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(append(line, '\n'))
}

// stderrTail is a bounded, concurrency-safe tail of everything the Pi process
// wrote to stderr. When a Pi dies mid-turn the last stderr bytes are usually
// the only witness, so they are kept both on disk (full stream) and here
// (final 16 KiB) for error enrichment.
type stderrTail struct {
	mu   sync.Mutex
	data []byte
}

const stderrTailLimit = 16 * 1024

func (t *stderrTail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = append(t.data, p...)
	if excess := len(t.data) - stderrTailLimit; excess > 0 {
		t.data = t.data[excess:]
	}
	return len(p), nil
}

func (t *stderrTail) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.data)
}

// truncateString bounds a string by bytes; a cut rune boundary is harmless
// here because the result only feeds logs and error text.
func truncateString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
