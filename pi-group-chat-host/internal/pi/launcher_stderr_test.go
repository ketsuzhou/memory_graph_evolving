package pi

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
)

// fakePiWithStderr extends the standard fake Pi with one stderr line emitted
// per received prompt, so a test can prove the launcher plumbed the stream
// through instead of letting it vanish into /dev/null.
func writeStderrFakePi(t *testing.T, marker string) string {
	t.Helper()
	path := writeFakePi(t, fakePiOptions{frames: []string{
		`{"id":"ID","type":"response","command":"prompt","success":true}`,
		`{"type":"agent_settled"}`,
	}})
	script, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fake Pi: %v", err)
	}
	// The request id is fixed per fake process; patch the canned frame to match
	// the deterministic request id the launcher uses below.
	script = []byte(strings.ReplaceAll(string(script), `"id":"ID"`, `"id":"prompt-fixed"`))
	script = []byte(strings.ReplaceAll(string(script), "while IFS= read -r line; do",
		"while IFS= read -r line; do\n  printf '%s\\n' '"+marker+"' >&2"))
	if err := os.WriteFile(path, script, 0o700); err != nil {
		t.Fatalf("patch fake Pi: %v", err)
	}
	return path
}

func TestLauncherCapturesStderr(t *testing.T) {
	executable := writeStderrFakePi(t, "fake-pi-stderr-marker")
	var sink bytes.Buffer
	launcher := NewLauncher(LauncherConfig{
		Executable: executable,
		SessionDir: t.TempDir(),
		Provider:   "test-provider",
		Model:      "test-model",
		Environment: []string{
			"FAKE_PI_VERSION=" + RequiredVersion,
			"PATH=" + os.Getenv("PATH"),
		},
		RequestID: func() string { return "prompt-fixed" },
		Stderr:    &sink,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process, err := launcher.Start(ctx, domain.AgentProfile{Kind: domain.ProfileOrdinary})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := process.Prompt(ctx, "hello", func(PiEvent) error { return nil }); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !strings.Contains(sink.String(), "fake-pi-stderr-marker") {
		t.Fatalf("stderr sink never received the marker; got %q", sink.String())
	}
}

func TestLauncherWithoutStderrSinkStillRuns(t *testing.T) {
	executable := writeStderrFakePi(t, "ignored")
	dir := t.TempDir()
	// Without a sink the child's stderr is discarded by the OS; the turn must
	// still settle normally.
	launcher := NewLauncher(LauncherConfig{
		Executable: executable,
		SessionDir: filepath.Join(dir, "session"),
		Provider:   "test-provider",
		Model:      "test-model",
		Environment: []string{
			"FAKE_PI_VERSION=" + RequiredVersion,
			"PATH=" + os.Getenv("PATH"),
		},
		RequestID: func() string { return "prompt-fixed" },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process, err := launcher.Start(ctx, domain.AgentProfile{Kind: domain.ProfileOrdinary})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	acceptance, err := process.Prompt(ctx, "hello", func(PiEvent) error { return nil })
	if err != nil || !acceptance.Success {
		t.Fatalf("prompt: acceptance=%+v err=%v", acceptance, err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
