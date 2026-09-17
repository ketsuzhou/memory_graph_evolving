package pi

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
)

func TestStartExactSessionUsesStoredSessionOnly(t *testing.T) {
	directory := t.TempDir()
	argvLog := filepath.Join(directory, "argv.log")
	executable := writeFakePi(t, fakePiOptions{})
	launcher := NewLauncher(LauncherConfig{
		Executable: executable,
		SessionDir: "SESSION",
		Provider:   "PROVIDER",
		Model:      "MODEL",
		Environment: []string{
			"FAKE_PI_ARGV_LOG=" + argvLog,
			"PATH=" + os.Getenv("PATH"),
		},
	})
	session := sessionctrl.Session{File: "/host-owned/sessions/exact.jsonl", ID: "session-exact-id"}
	controller, err := launcher.StartExactSession(context.Background(), AgentProfile{Kind: ProfileOrdinary, BuiltinToolsEnabled: true}, session, 11)
	if err != nil {
		t.Fatalf("StartExactSession() error = %v", err)
	}
	if err := controller.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	argvBytes, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatalf("read argv log: %v", err)
	}
	got := strings.Split(strings.TrimSuffix(string(argvBytes), "\n"), "\n")
	want := append(launcher.argv(AgentProfile{Kind: ProfileOrdinary, BuiltinToolsEnabled: true}), "--session", session.File)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exact-session argv = %#v, want %#v", got, want)
	}
	for _, prohibited := range []string{"--resume", "--continue"} {
		for _, argument := range got {
			if argument == prohibited {
				t.Fatalf("exact-session argv contains prohibited interactive flag %q: %#v", prohibited, got)
			}
		}
	}
	if controller.SessionFile() != session.File || controller.SessionID() != session.ID || controller.ProcessGeneration() != 11 {
		t.Fatalf("controller did not retain exact binding: file=%q id=%q generation=%d", controller.SessionFile(), controller.SessionID(), controller.ProcessGeneration())
	}
}

func TestStartExactSessionRejectsMissingExactBinding(t *testing.T) {
	launcher := NewLauncher(LauncherConfig{Executable: writeFakePi(t, fakePiOptions{})})
	_, err := launcher.StartExactSession(context.Background(), AgentProfile{}, sessionctrl.Session{File: "/host-owned/sessions/exact.jsonl"}, 1)
	if err == nil {
		t.Fatal("StartExactSession() succeeded without an exact session ID")
	}
}

func TestStartExactSessionRPCFullDuplexWithFakePi(t *testing.T) {
	directory := t.TempDir()
	argvLog := filepath.Join(directory, "argv.log")
	session := sessionctrl.Session{File: "/host-owned/sessions/exact.jsonl", ID: "session-exact-id"}
	executable := writeControlFakePi(t)
	launcher := NewLauncher(LauncherConfig{
		Executable: executable,
		SessionDir: "SESSION",
		Provider:   "PROVIDER",
		Model:      "MODEL",
		Environment: []string{
			"FAKE_PI_ARGV_LOG=" + argvLog,
			"FAKE_PI_SESSION_FILE=" + session.File,
			"FAKE_PI_SESSION_ID=" + session.ID,
			"PATH=" + os.Getenv("PATH"),
		},
	})
	controller, err := launcher.StartExactSession(context.Background(), AgentProfile{Kind: ProfileOrdinary, BuiltinToolsEnabled: true}, session, 4)
	if err != nil {
		t.Fatalf("StartExactSession() error = %v", err)
	}

	eventsDone := make(chan struct{})
	go func() {
		for range controller.Events() {
		}
		close(eventsDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	state, err := controller.GetState(ctx)
	if err != nil {
		t.Fatalf("GetState() error = %v", err)
	}
	if state.SessionFile != session.File || state.SessionID != session.ID || !state.IsStreaming || state.PendingMessageCount != 1 {
		t.Fatalf("GetState() = %+v", state)
	}
	if err := controller.ClearQueue(ctx); err != nil {
		t.Fatalf("ClearQueue() error = %v", err)
	}
	if err := controller.Abort(ctx); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := controller.WaitSettled(ctx); err != nil {
		t.Fatalf("WaitSettled() error = %v", err)
	}
	if !controller.Settled() {
		t.Fatal("Settled() = false after fake Pi emitted agent_settled")
	}

	argvBytes, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatalf("read argv log: %v", err)
	}
	got := strings.Split(strings.TrimSuffix(string(argvBytes), "\n"), "\n")
	if !containsExactFlag(got, "--session", session.File) {
		t.Fatalf("exact-session argv missing --session %q: %#v", session.File, got)
	}
	for _, prohibited := range []string{"--resume", "--continue"} {
		for _, argument := range got {
			if argument == prohibited {
				t.Fatalf("exact-session argv contains prohibited interactive flag %q: %#v", prohibited, got)
			}
		}
	}

	if err := controller.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-eventsDone:
	case <-ctx.Done():
		t.Fatal("event stream did not finish after Close")
	}
}

func containsExactFlag(argv []string, flag, value string) bool {
	for i := 0; i < len(argv)-1; i++ {
		if argv[i] == flag && argv[i+1] == value {
			return true
		}
	}
	return false
}

func writeControlFakePi(t *testing.T) string {
	t.Helper()
	script := `#!/bin/sh
if [ "${1:-}" = "--version" ]; then
  printf '%s\n' "` + RequiredVersion + `"
  exit 0
fi
if [ -n "${FAKE_PI_ARGV_LOG:-}" ]; then
  printf '%s\n' "$@" > "$FAKE_PI_ARGV_LOG"
fi
printf '%s\n' '{"type":"agent_start"}'
while IFS= read -r line; do
  id=${line#*\"id\":\"}
  id=${id%%\"*}
  typ=${line#*\"type\":\"}
  typ=${typ%%\"*}
  case "$typ" in
    get_state)
      printf '%s\n' "{\"id\":\"${id}\",\"type\":\"response\",\"command\":\"get_state\",\"success\":true,\"data\":{\"sessionFile\":\"${FAKE_PI_SESSION_FILE}\",\"sessionId\":\"${FAKE_PI_SESSION_ID}\",\"isStreaming\":true,\"pendingMessageCount\":1}}"
      ;;
    clear_queue)
      printf '%s\n' "{\"id\":\"${id}\",\"type\":\"response\",\"command\":\"clear_queue\",\"success\":true}"
      ;;
    abort)
      printf '%s\n' "{\"id\":\"${id}\",\"type\":\"response\",\"command\":\"abort\",\"success\":true}"
      printf '%s\n' '{"type":"agent_end","messages":[],"willRetry":false}'
      printf '%s\n' '{"type":"agent_settled"}'
      ;;
  esac
done
`
	path := filepath.Join(t.TempDir(), "fake-pi-control")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write control fake Pi: %v", err)
	}
	return path
}
