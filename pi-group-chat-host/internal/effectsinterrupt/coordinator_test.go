package effectsinterrupt

import (
	"context"
	"errors"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
)

func TestReadOnlyAndModelGenerationAbortCooperativelyImmediately(t *testing.T) {
	t.Parallel()
	for _, kind := range []ExecutionKind{KindModel, KindReadOnly} {
		kind := kind
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			toolHold := make(chan struct{})
			session := NewScriptedSession("task-agent", pinnedSession(), kind)
			session.Hold = make(chan struct{})
			session.ToolHold = toolHold

			c := NewCoordinator(Policy{InterruptGrace: 400 * time.Millisecond, KillGrace: 50 * time.Millisecond}, nil)
			startAndAttach(t, ctx, c, "task-1", "attempt-1", session, nil)

			started := time.Now()
			got, err := c.Interrupt(ctx, "task-agent")
			elapsed := time.Since(started)
			if err != nil {
				t.Fatalf("Interrupt(%s) error = %v", kind, err)
			}
			if elapsed > 150*time.Millisecond {
				t.Fatalf("Interrupt(%s) took %s, want an immediate cooperative abort", kind, elapsed)
			}
			if got.Outcome != OutcomeAborted || !got.Resumed {
				t.Fatalf("Interrupt(%s) = %#v, want cooperative abort + resume", kind, got)
			}
			if len(got.Signals) != 1 || got.Signals[0] != SignalCooperative {
				t.Fatalf("signals = %#v, want only %s", got.Signals, SignalCooperative)
			}

			obs := session.Observation()
			if obs.AbortCount != 1 {
				t.Fatalf("AbortCount = %d, want 1", obs.AbortCount)
			}
			if !obs.CompletionAt.IsZero() || obs.EffectsProven {
				t.Fatalf("observation = %#v, model/read-only must not wait on a tool", obs)
			}
			select {
			case <-toolHold:
				t.Fatal("tool hold was closed; abort must not wait for a mutating completion")
			default:
			}
		})
	}
}

func TestMutatingToolCompletionIsObservedBeforeAbortAndCompletedOpsAreNotReplayed(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	toolHold := make(chan struct{})
	session := NewScriptedSession("task-agent", pinnedSession(), KindMutating)
	session.Hold = make(chan struct{})
	session.ToolHold = toolHold
	session.Completed = []CompletedOp{{CallID: "write-1", Name: "apply_patch", Output: "wrote file"}}

	c := NewCoordinator(Policy{InterruptGrace: time.Second, KillGrace: 50 * time.Millisecond}, nil)
	startAndAttach(t, ctx, c, "task-1", "attempt-1", session, nil)

	done := make(chan resultOrErr, 1)
	go func() {
		got, err := c.Interrupt(ctx, "task-agent")
		done <- resultOrErr{result: got, err: err}
	}()

	waitUntil(t, func() bool { return session.Running() && !session.EffectsProven() })
	time.Sleep(15 * time.Millisecond)
	if session.Observation().AbortCount != 0 {
		t.Fatal("abort ran before the mutating tool completed")
	}
	close(toolHold)

	got := waitResult(t, ctx, done)
	if got.err != nil {
		t.Fatalf("Interrupt() error = %v", got.err)
	}
	if got.result.Outcome != OutcomeAborted || !got.result.Resumed {
		t.Fatalf("result = %#v, want abort after observed completion", got.result)
	}
	if len(got.result.CompletedOps) != 1 || got.result.CompletedOps[0].CallID != "write-1" {
		t.Fatalf("completed ops = %#v, want the observed mutating result", got.result.CompletedOps)
	}
	if len(got.result.ReplayedOps) != 0 {
		t.Fatalf("replayed ops = %#v, want none", got.result.ReplayedOps)
	}

	obs := session.Observation()
	if obs.CompletionAt.IsZero() || obs.AbortAt.IsZero() {
		t.Fatalf("timestamps = %#v, want completion and abort", obs)
	}
	if obs.AbortAt.Before(obs.CompletionAt) {
		t.Fatalf("abort at %s before completion at %s", obs.AbortAt, obs.CompletionAt)
	}
	if obs.ResumeCount != 1 {
		t.Fatalf("ResumeCount = %d, want 1", obs.ResumeCount)
	}
	if len(obs.ReplayedOps) != 0 {
		t.Fatalf("session replayed ops = %#v, want none", obs.ReplayedOps)
	}
}

func TestUncooperativeChildIsReclaimedAfterGraceTermAndKill(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tree, err := StartFakeTree(t.TempDir(), 2, true)
	if err != nil {
		t.Fatalf("StartFakeTree() error = %v", err)
	}
	t.Cleanup(tree.Kill)
	if live := tree.Alive(); len(live) < 3 {
		t.Fatalf("alive before interrupt = %v, want parent and two children", live)
	}

	session := NewScriptedSession("task-agent", pinnedSession(), KindMutating)
	session.Hold = make(chan struct{})
	session.ToolHold = make(chan struct{})

	c := NewCoordinator(Policy{InterruptGrace: 40 * time.Millisecond, KillGrace: 80 * time.Millisecond}, nil)
	startAndAttach(t, ctx, c, "task-1", "attempt-1", session, tree)

	got, err := c.Interrupt(ctx, "task-agent")
	if !errors.Is(err, ErrEffectsUnknown) {
		t.Fatalf("Interrupt() error = %v, want %v", err, ErrEffectsUnknown)
	}
	if got.Outcome != OutcomeEffectsUnknown || got.Resumed {
		t.Fatalf("result = %#v, uncooperative mutating interrupt must fail closed", got)
	}
	if !got.TreeReclaimed {
		t.Fatalf("TreeReclaimed = false, want the process group reclaimed")
	}
	if len(got.Signals) < 2 || got.Signals[0] != SignalTERM || got.Signals[len(got.Signals)-1] != SignalKILL {
		t.Fatalf("signals = %#v, want grace then %s then %s", got.Signals, SignalTERM, SignalKILL)
	}

	if afterTERM := tree.AliveAfter(SignalTERM); len(afterTERM) == 0 {
		t.Fatal("process tree died on SIGTERM; uncooperative children must survive until SIGKILL")
	}
	if live := tree.Alive(); len(live) != 0 {
		t.Fatalf("alive after SIGKILL = %v, want empty process tree", live)
	}
	if session.Observation().ResumeCount != 0 {
		t.Fatal("uncooperative escalate must not resume the exact session")
	}
}

func TestEffectsUnknownDoesNotResumeAndRetriesWithNewAttemptID(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runner := NewRunner(3)
	runner.Preregister("held-out-1")
	firstID, ok := runner.AllocateAttempt("held-out-1")
	if !ok || firstID == "" {
		t.Fatalf("AllocateAttempt() = %q, %v, want the first attempt", firstID, ok)
	}

	session := NewScriptedSession("task-agent", pinnedSession(), KindMutating)
	session.Hold = make(chan struct{})
	session.ToolHold = make(chan struct{})

	c := NewCoordinator(Policy{InterruptGrace: 20 * time.Millisecond, KillGrace: 20 * time.Millisecond}, runner)
	startAndAttach(t, ctx, c, "held-out-1", firstID, session, nil)

	got, err := c.Interrupt(ctx, "task-agent")
	if !errors.Is(err, ErrEffectsUnknown) {
		t.Fatalf("Interrupt() error = %v, want %v", err, ErrEffectsUnknown)
	}
	if got.Resumed || session.Observation().ResumeCount != 0 {
		t.Fatalf("effects-unknown resumed: result=%#v obs=%#v", got, session.Observation())
	}
	if got.AttemptID != firstID {
		t.Fatalf("AttemptID = %q, want original %q", got.AttemptID, firstID)
	}
	if got.NextAttemptID == "" || got.NextAttemptID == firstID {
		t.Fatalf("NextAttemptID = %q, want a new attempt id", got.NextAttemptID)
	}
	if got.Outcome != OutcomeEffectsUnknown {
		t.Fatalf("Outcome = %q, want %s", got.Outcome, OutcomeEffectsUnknown)
	}

	records := runner.Attempts("held-out-1")
	if len(records) != 1 {
		t.Fatalf("attempts = %#v, want the original failure kept", records)
	}
	if records[0].AttemptID != firstID || records[0].Outcome != OutcomeEffectsUnknown || records[0].Resumed {
		t.Fatalf("original record = %#v, want quarantined %s", records[0], firstID)
	}
	if records[0].AttemptID == got.NextAttemptID {
		t.Fatal("retry reused the failed attempt id instead of minting a new one")
	}
}

func TestRetryExhaustionKeepsPreregisteredTaskAndScoresZeroInDenominator(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const taskID = "held-out-1"
	runner := NewRunner(2)
	runner.Preregister(taskID)
	attemptID, ok := runner.AllocateAttempt(taskID)
	if !ok {
		t.Fatal("AllocateAttempt() rejected the first preregistered attempt")
	}

	var last Result
	for i := 0; i < 2; i++ {
		session := NewScriptedSession("task-agent", pinnedSession(), KindMutating)
		session.Hold = make(chan struct{})
		session.ToolHold = make(chan struct{})
		c := NewCoordinator(Policy{InterruptGrace: 15 * time.Millisecond, KillGrace: 15 * time.Millisecond}, runner)
		startAndAttach(t, ctx, c, taskID, attemptID, session, nil)
		got, err := c.Interrupt(ctx, "task-agent")
		last = got
		if session.Observation().ResumeCount != 0 {
			t.Fatalf("attempt %s resumed after effects-unknown", attemptID)
		}
		c.Detach("task-agent")
		if i == 0 {
			if !errors.Is(err, ErrEffectsUnknown) {
				t.Fatalf("first attempt error = %v, want %v", err, ErrEffectsUnknown)
			}
			if got.NextAttemptID == "" || got.NextAttemptID == attemptID {
				t.Fatalf("NextAttemptID = %q, want a distinct retry id", got.NextAttemptID)
			}
			attemptID = got.NextAttemptID
			continue
		}
		if !errors.Is(err, ErrRetryExhausted) {
			t.Fatalf("final attempt error = %v, want %v", err, ErrRetryExhausted)
		}
	}

	if _, ok := runner.AllocateAttempt(taskID); ok {
		t.Fatal("AllocateAttempt() minted an attempt after the retry budget")
	}
	if !runner.Kept(taskID) {
		t.Fatal("preregistered task was dropped from the known set")
	}
	if !runner.Exhausted(taskID) {
		t.Fatal("runner did not record retry exhaustion")
	}
	if last.Outcome != OutcomeRetryExhausted || !last.Exhausted {
		t.Fatalf("last result = %#v, want exhaustion", last)
	}

	records := runner.Attempts(taskID)
	if len(records) != 2 {
		t.Fatalf("attempts = %#v, want both failed attempts kept", records)
	}
	if records[0].AttemptID == records[1].AttemptID {
		t.Fatalf("attempt ids collided: %#v", records)
	}
	for _, rec := range records {
		if rec.Resumed || rec.Outcome != OutcomeEffectsUnknown {
			t.Fatalf("record = %#v, want a kept effects_unknown failure", rec)
		}
	}

	passed, denom := runner.PassAt1()
	if passed != 0 || denom != 1 {
		t.Fatalf("pass@1 = %d/%d, want 0/1 with the preregistered task in the denominator", passed, denom)
	}
}

type resultOrErr struct {
	result Result
	err    error
}

func TestAttachFailsClosedWhenBindingPinsMissing(t *testing.T) {
	t.Parallel()
	session := NewScriptedSession("task-agent", pinnedSession(), KindModel)

	t.Run("missing SessionDir", func(t *testing.T) {
		t.Parallel()
		c := NewCoordinator(Policy{}, nil)
		binding := directedoffer.TestBinding(session.AgentID(), session.Session())
		binding.SessionDir = ""
		if err := c.Attach(binding, "task-1", session, nil); !errors.Is(err, directedoffer.ErrBindingPinsRequired) {
			t.Fatalf("Attach() error = %v, want %v", err, directedoffer.ErrBindingPinsRequired)
		}
	})
	t.Run("missing ToolPolicyDigest", func(t *testing.T) {
		t.Parallel()
		c := NewCoordinator(Policy{}, nil)
		binding := directedoffer.TestBinding(session.AgentID(), session.Session())
		binding.ToolPolicyDigest = ""
		if err := c.Attach(binding, "task-1", session, nil); !errors.Is(err, directedoffer.ErrBindingPinsRequired) {
			t.Fatalf("Attach() error = %v, want %v", err, directedoffer.ErrBindingPinsRequired)
		}
	})
}

func startAndAttach(t *testing.T, ctx context.Context, c *Coordinator, taskID, attemptID string, session *ScriptedSession, tree ProcessTree) {
	t.Helper()
	go func() { _ = session.Start(ctx) }()
	waitUntil(t, session.Running)
	binding := directedoffer.TestBinding(session.AgentID(), session.Session())
	binding.LogicalRunID = taskID
	binding.AttemptID = attemptID
	binding.Generation = 3
	binding.SegmentID = "seg-task-open"
	if err := c.Attach(binding, taskID, session, tree); err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
}

func waitResult(t *testing.T, ctx context.Context, done <-chan resultOrErr) resultOrErr {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-ctx.Done():
		t.Fatalf("interrupt timed out: %v", ctx.Err())
		return resultOrErr{}
	}
}

func waitUntil(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for session condition")
}

func pinnedSession() sessionctrl.Session {
	return sessionctrl.Session{File: "/sessions/exact-task.jsonl", ID: "exact-session-id"}
}
