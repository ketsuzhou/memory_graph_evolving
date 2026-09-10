package pi

import (
	"context"
	"errors"
	"river2.dev/pi-group-chat-host/internal/domain"
	"testing"
	"time"
)

// TestContextExpiryReportsCancelledNotExited pins the classification contract:
// when the Host's own deadline kills the child, the surfaced error is
// ErrCancelled, not a spurious process exit. Retry and diagnosis policy branch
// on exactly this distinction.
func TestContextExpiryReportsCancelledNotExited(t *testing.T) {
	// The fake Pi emits the prompt acknowledgement and then goes silent, so
	// the turn only ends when the caller's context expires and the child is
	// killed.
	executable := writeFakePi(t, fakePiOptions{frames: []string{
		`{"id":"prompt-slow","type":"response","command":"prompt","success":true}`,
	}})
	launcher := NewLauncher(LauncherConfig{
		Executable: executable,
		SessionDir: t.TempDir(),
		Provider:   "test-provider",
		Model:      "test-model",
		RequestID:  func() string { return "prompt-slow" },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	process, err := launcher.Start(ctx, domain0Profile())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = process.Close() }()

	_, promptErr := process.Prompt(ctx, "work for a long time", func(PiEvent) error { return nil })
	if promptErr == nil {
		t.Fatal("silent Pi under an expiring context must fail the prompt")
	}
	if !errors.Is(promptErr, ErrCancelled) {
		t.Fatalf("error = %v, want ErrCancelled (the Host killed it), not a process exit", promptErr)
	}
	if errors.Is(promptErr, ErrProcessExited) {
		t.Fatalf("timeout kill misclassified as ErrProcessExited: %v", promptErr)
	}
}

// TestSpontaneousExitStillReportsExited keeps the other half of the contract:
// a Pi that dies on its own with the context still live surfaces as
// ErrProcessExited.
func TestSpontaneousExitStillReportsExited(t *testing.T) {
	executable := writeFakePi(t, fakePiOptions{
		frames: []string{`{"id":"prompt-die","type":"response","command":"prompt","success":true}`},
		// The script exits right after echoing its frames while stdin stays
		// open on our side: stdout EOF with a live context.
		exitAfterFrames: true,
	})
	launcher := NewLauncher(LauncherConfig{
		Executable: executable,
		SessionDir: t.TempDir(),
		Provider:   "test-provider",
		Model:      "test-model",
		RequestID:  func() string { return "prompt-die" },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process, err := launcher.Start(ctx, domain0Profile())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = process.Close() }()

	_, promptErr := process.Prompt(ctx, "hello", func(PiEvent) error { return nil })
	if !errors.Is(promptErr, ErrProcessExited) {
		t.Fatalf("error = %v, want ErrProcessExited", promptErr)
	}
}

func domain0Profile() domain.AgentProfile {
	return domain.AgentProfile{Kind: domain.ProfileOrdinary}
}
