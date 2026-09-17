package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/pi"
	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
)

func TestRealPi0851AbortResume64(t *testing.T) {
	if os.Getenv("RUN_PI_STABILITY") != "1" {
		t.Skip("set RUN_PI_STABILITY=1 to observe real Pi abort/resume")
	}
	binary := os.Getenv("PI_BINARY")
	if binary == "" {
		binary = "/home/zhoujie22/pi-0851/node_modules/.bin/pi"
	}
	n := 8
	if raw := os.Getenv("PI_STABILITY_N"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			n = parsed
		}
	}
	timeout := 45 * time.Second
	var (
		started   atomic.Int64
		aborted   atomic.Int64
		resumed   atomic.Int64
		cancelled atomic.Int64
		failed    atomic.Int64
	)
	var mu sync.Mutex
	var errs []string
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			dir := t.TempDir()
			file := filepath.Join(dir, "exact.jsonl")
			if err := os.WriteFile(file, nil, 0o644); err != nil {
				failed.Add(1)
				return
			}
			session := sessionctrl.Session{File: file, ID: "stab-" + strings.ReplaceAll(filepath.Base(dir), "/", "")}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			launcher := pi.NewLauncher(pi.LauncherConfig{
				Executable: binary, SessionDir: dir, Provider: "openrouter", Model: "qwen/qwen3.5-27b",
				Environment: os.Environ(),
			})
			ctrl, err := launcher.StartExactSession(ctx, pi.AgentProfile{Kind: pi.ProfileOrdinary, BuiltinToolsEnabled: true, WorkingDirectory: dir}, session, 1)
			if err != nil {
				noteStabError(&mu, &errs, &cancelled, &failed, err)
				return
			}
			started.Add(1)
			go func() {
				for range ctrl.Events() {
				}
			}()
			if err := ctrl.Abort(ctx); err != nil {
				_ = ctrl.Close()
				noteStabError(&mu, &errs, &cancelled, &failed, err)
				return
			}
			if err := ctrl.WaitSettled(ctx); err != nil {
				_ = ctrl.Close()
				noteStabError(&mu, &errs, &cancelled, &failed, err)
				return
			}
			aborted.Add(1)
			_ = ctrl.Close()
			ctrl2, err := launcher.StartExactSession(ctx, pi.AgentProfile{Kind: pi.ProfileOrdinary, BuiltinToolsEnabled: true, WorkingDirectory: dir}, session, 2)
			if err != nil {
				noteStabError(&mu, &errs, &cancelled, &failed, err)
				return
			}
			go func() {
				for range ctrl2.Events() {
				}
			}()
			if err := ctrl2.Abort(ctx); err != nil {
				_ = ctrl2.Close()
				noteStabError(&mu, &errs, &cancelled, &failed, err)
				return
			}
			if err := ctrl2.WaitSettled(ctx); err != nil {
				_ = ctrl2.Close()
				noteStabError(&mu, &errs, &cancelled, &failed, err)
				return
			}
			resumed.Add(1)
			_ = ctrl2.Close()
		}(i)
	}
	wg.Wait()
	t.Logf("pi_stability n=%d started=%d aborted=%d resumed=%d cancelled=%d failed=%d", n, started.Load(), aborted.Load(), resumed.Load(), cancelled.Load(), failed.Load())
	if len(errs) > 0 {
		t.Logf("sample errors: %s", strings.Join(errs, " | "))
	}
	if failed.Load() > 0 || cancelled.Load() > 0 {
		t.Fatalf("real Pi abort/resume instability at 64: failed=%d cancelled=%d", failed.Load(), cancelled.Load())
	}
}

func noteStabError(mu *sync.Mutex, errs *[]string, cancelled, failed *atomic.Int64, err error) {
	text := err.Error()
	if strings.Contains(strings.ToUpper(text), "CANCEL") {
		cancelled.Add(1)
	} else {
		failed.Add(1)
	}
	mu.Lock()
	if len(*errs) < 5 {
		*errs = append(*errs, text)
	}
	mu.Unlock()
}
