package concurrentepisode

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartBarrierBothEnterRunningBeforeEitherSettles(t *testing.T) {
	taskHold := make(chan struct{})
	memoryHold := make(chan struct{})
	task := NewScriptedSession("task-agent")
	task.Hold = taskHold
	memory := NewScriptedSession("memory-agent")
	memory.Hold = memoryHold

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan resultOrErr, 1)
	go func() {
		result, err := Run(ctx, Request{
			Opening:               sampleOpening(),
			Graph:                 EvaluationGraph{},
			Task:                  task,
			Memory:                memory,
			TaskContextMonitoring: ContextOpeningOnly,
		})
		done <- resultOrErr{result: result, err: err}
	}()

	waitPhase(t, task, PhaseRunning)
	waitPhase(t, memory, PhaseRunning)
	if task.Observation().Phase != PhaseRunning || memory.Observation().Phase != PhaseRunning {
		t.Fatalf("start-barrier lost: task=%s memory=%s", task.Observation().Phase, memory.Observation().Phase)
	}
	if !task.SettledAt().IsZero() || !memory.SettledAt().IsZero() {
		t.Fatal("a session settled before the peer entered running")
	}

	close(taskHold)
	close(memoryHold)
	got := waitResult(t, ctx, done)
	if got.err != nil {
		t.Fatalf("Run() error = %v", got.err)
	}
	assertStartBarrier(t, got.result.Timeline)
	if got.result.MemoryTerminal != TerminalNoCandidateFromEmptyGraph {
		t.Fatalf("MemoryTerminal = %q, want %s", got.result.MemoryTerminal, TerminalNoCandidateFromEmptyGraph)
	}
}

func TestMemoryInputExcludesTaskToolStateAndFollowUpMessages(t *testing.T) {
	task := NewScriptedSession("task-agent")
	task.EmitTaskProgress = true
	task.Delay = 20 * time.Millisecond
	memory := NewScriptedSession("memory-agent")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	opening := sampleOpening()
	result, err := Run(ctx, Request{
		Opening:               opening,
		Graph:                 EvaluationGraph{},
		Task:                  task,
		Memory:                memory,
		TaskContextMonitoring: ContextOpeningOnly,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(task.Observation().ToolStates) == 0 || len(task.Observation().FollowUps) == 0 {
		t.Fatal("task did not emit intermediate tool state and follow-up messages")
	}
	assertOpeningOnly(t, result.MemoryInput, opening)
	assertOpeningOnlyInput(t, memory.Observation().Input, opening)
	if len(memory.Observation().ToolStates) != 0 || len(memory.Observation().FollowUps) != 0 {
		t.Fatalf("memory session recorded task progress: tools=%v follow-ups=%v", memory.Observation().ToolStates, memory.Observation().FollowUps)
	}
}

func TestSameAgentHasNoOverlappingPiRunAndDifferentAgentsOverlap(t *testing.T) {
	t.Run("same agent", func(t *testing.T) {
		flights := NewAgentRunCoordinator()
		firstHold := make(chan struct{})
		first := NewScriptedSession("shared-agent")
		first.Hold = firstHold
		second := NewScriptedSession("shared-agent")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var overlapping atomic.Bool
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- flights.Run(ctx, first.AgentID(), func() error {
				return first.Start(ctx, SessionInput{Opening: sampleOpening()}, nil)
			})
		}()
		waitPhase(t, first, PhaseRunning)

		secondStarted := make(chan struct{})
		secondDone := make(chan error, 1)
		go func() {
			close(secondStarted)
			secondDone <- flights.Run(ctx, second.AgentID(), func() error {
				if first.Observation().Phase == PhaseRunning {
					overlapping.Store(true)
				}
				return second.Start(ctx, SessionInput{Opening: sampleOpening()}, nil)
			})
		}()
		<-secondStarted
		time.Sleep(30 * time.Millisecond)
		if second.Observation().Phase == PhaseRunning && first.Observation().Phase == PhaseRunning {
			t.Fatal("same agent had overlapping Pi runs")
		}
		if flights.InFlight("shared-agent") && second.Observation().Phase == PhaseRunning && first.Observation().Phase == PhaseRunning {
			t.Fatal("single-flight lease allowed two running turns")
		}
		close(firstHold)
		if err := <-firstDone; err != nil {
			t.Fatalf("first run error = %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second run error = %v", err)
		}
		if overlapping.Load() {
			t.Fatal("second run started while the first turn was still running")
		}
		if first.SettledAt().IsZero() || second.RunningAt().Before(first.SettledAt()) {
			t.Fatalf("same-agent turns overlapped: first settled=%v second running=%v", first.SettledAt(), second.RunningAt())
		}
	})

	t.Run("different agents", func(t *testing.T) {
		taskHold := make(chan struct{})
		memoryHold := make(chan struct{})
		task := NewScriptedSession("task-agent")
		task.Hold = taskHold
		memory := NewScriptedSession("memory-agent")
		memory.Hold = memoryHold

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done := make(chan resultOrErr, 1)
		go func() {
			result, err := Run(ctx, Request{
				Opening: sampleOpening(),
				Graph:   EvaluationGraph{},
				Task:    task,
				Memory:  memory,
			})
			done <- resultOrErr{result: result, err: err}
		}()
		waitPhase(t, task, PhaseRunning)
		waitPhase(t, memory, PhaseRunning)
		if task.Observation().Phase != PhaseRunning || memory.Observation().Phase != PhaseRunning {
			t.Fatal("distinct agents did not run in parallel")
		}
		close(taskHold)
		close(memoryHold)
		got := waitResult(t, ctx, done)
		if got.err != nil {
			t.Fatalf("Run() error = %v", got.err)
		}
		assertStartBarrier(t, got.result.Timeline)
	})
}

func TestEmptyAndNoApplicableTerminalsAreDistinctFromTimeoutAndError(t *testing.T) {
	opening := sampleOpening()
	cases := []struct {
		name       string
		graph      EvaluationGraph
		applicable []string
		memory     func() *ScriptedSession
		want       string
		class      string
		normal     bool
	}{
		{
			name:  "empty graph is normal no-skill",
			graph: EvaluationGraph{},
			memory: func() *ScriptedSession {
				return NewScriptedSession("memory-agent")
			},
			want:   TerminalNoCandidateFromEmptyGraph,
			class:  FailureNormalNoSkill,
			normal: true,
		},
		{
			name:       "non-empty graph with no applicable candidate",
			graph:      EvaluationGraph{SkillIDs: []string{"skill://eval/demo@1"}},
			applicable: nil,
			memory: func() *ScriptedSession {
				return NewScriptedSession("memory-agent")
			},
			want:   TerminalNoApplicableCandidate,
			class:  FailureNormalNoSkill,
			normal: true,
		},
		{
			name:  "timeout is not empty-graph success",
			graph: EvaluationGraph{},
			memory: func() *ScriptedSession {
				session := NewScriptedSession("memory-agent")
				session.Timeout = true
				return session
			},
			want:   TerminalMemoryTimeout,
			class:  FailureMemoryTimeout,
			normal: false,
		},
		{
			name:  "error is not empty-graph success",
			graph: EvaluationGraph{},
			memory: func() *ScriptedSession {
				session := NewScriptedSession("memory-agent")
				session.Err = errors.New("scripted memory failure")
				return session
			},
			want:   TerminalMemoryError,
			class:  FailureMemoryError,
			normal: false,
		},
	}

	seen := map[string]bool{}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result, err := Run(ctx, Request{
				Opening:                opening,
				Graph:                  testCase.graph,
				ApplicableCandidateIDs: testCase.applicable,
				Task:                   NewScriptedSession("task-agent"),
				Memory:                 testCase.memory(),
			})
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if result.MemoryTerminal != testCase.want {
				t.Fatalf("MemoryTerminal = %q, want %q", result.MemoryTerminal, testCase.want)
			}
			if result.FailureClass != testCase.class {
				t.Fatalf("FailureClass = %q, want %q", result.FailureClass, testCase.class)
			}
			if IsNormalNoSkill(result.MemoryTerminal) != testCase.normal {
				t.Fatalf("IsNormalNoSkill(%q) = %t, want %t", result.MemoryTerminal, IsNormalNoSkill(result.MemoryTerminal), testCase.normal)
			}
			if result.TaskStatus != TaskStatusCompleted {
				t.Fatalf("task status = %q, want completed even when memory is a mechanism failure", result.TaskStatus)
			}
			seen[result.MemoryTerminal] = true
		})
	}
	for _, terminal := range []string{
		TerminalNoCandidateFromEmptyGraph,
		TerminalNoApplicableCandidate,
		TerminalMemoryTimeout,
		TerminalMemoryError,
	} {
		if !seen[terminal] {
			t.Fatalf("missing terminal coverage for %s", terminal)
		}
	}
}

func TestNoOfferDoesNotAbortOrResumeTaskSession(t *testing.T) {
	taskHold := make(chan struct{})
	memoryHold := make(chan struct{})
	task := NewScriptedSession("task-agent")
	task.Hold = taskHold
	memory := NewScriptedSession("memory-agent")
	memory.Hold = memoryHold
	memory.Delay = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan resultOrErr, 1)
	go func() {
		result, err := Run(ctx, Request{
			Opening: sampleOpening(),
			Graph:   EvaluationGraph{},
			Task:    task,
			Memory:  memory,
		})
		done <- resultOrErr{result: result, err: err}
	}()

	waitPhase(t, task, PhaseRunning)
	waitPhase(t, memory, PhaseRunning)
	close(taskHold)
	waitPhase(t, task, PhaseSettled)
	if memory.Observation().Phase != PhaseRunning {
		t.Fatalf("task waited for memory: memory phase = %s after task settled", memory.Observation().Phase)
	}
	if task.Observation().AbortCount != 0 || task.Observation().ResumeCount != 0 {
		t.Fatalf("task was interrupted before memory finished: abort=%d resume=%d", task.Observation().AbortCount, task.Observation().ResumeCount)
	}

	close(memoryHold)
	got := waitResult(t, ctx, done)
	if got.err != nil {
		t.Fatalf("Run() error = %v", got.err)
	}
	if len(got.result.Offers) != 0 {
		t.Fatalf("empty graph produced offers: %#v", got.result.Offers)
	}
	if got.result.MemoryTerminal != TerminalNoCandidateFromEmptyGraph {
		t.Fatalf("MemoryTerminal = %q, want %s", got.result.MemoryTerminal, TerminalNoCandidateFromEmptyGraph)
	}
	obs := task.Observation()
	if obs.AbortCount != 0 || obs.ResumeCount != 0 {
		t.Fatalf("no-offer path abort/resume = %d/%d, want 0/0", obs.AbortCount, obs.ResumeCount)
	}
	if !task.SettledAt().Before(memory.SettledAt()) {
		t.Fatalf("task settled at %v, memory settled at %v; task must not wait for memory", task.SettledAt(), memory.SettledAt())
	}
}

func TestRunRejectsInvalidEpisodeShape(t *testing.T) {
	ctx := context.Background()
	if _, err := Run(ctx, Request{Memory: NewScriptedSession("memory-agent")}); !errors.Is(err, ErrTaskSessionRequired) {
		t.Fatalf("missing task error = %v", err)
	}
	if _, err := Run(ctx, Request{Task: NewScriptedSession("task-agent")}); !errors.Is(err, ErrMemorySessionRequired) {
		t.Fatalf("missing memory error = %v", err)
	}
	same := NewScriptedSession("shared")
	if _, err := Run(ctx, Request{Task: same, Memory: NewScriptedSession("shared")}); !errors.Is(err, ErrDistinctAgentsRequired) {
		t.Fatalf("same agent error = %v", err)
	}
	if _, err := Run(ctx, Request{
		Task:                  NewScriptedSession("task-agent"),
		Memory:                NewScriptedSession("memory-agent"),
		TaskContextMonitoring: "live",
	}); !errors.Is(err, ErrOpeningOnlyRequired) {
		t.Fatalf("live monitoring error = %v", err)
	}
}

type resultOrErr struct {
	result Result
	err    error
}

func sampleOpening() OpeningContext {
	return OpeningContext{Messages: []OpeningMessage{{
		ID:      "open-1",
		Author:  "human",
		Content: "solve the held-out task from the opening only",
	}}}
}

func waitPhase(t *testing.T, session *ScriptedSession, want Phase) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if session.Observation().Phase == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("session %s phase = %s, want %s", session.AgentID(), session.Observation().Phase, want)
}

func waitResult(t *testing.T, ctx context.Context, done <-chan resultOrErr) resultOrErr {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-ctx.Done():
		t.Fatalf("episode did not finish: %v", ctx.Err())
	}
	return resultOrErr{}
}

func assertStartBarrier(t *testing.T, events []Event) {
	t.Helper()
	var taskRunning, memoryRunning, taskSettled, memorySettled time.Time
	for _, event := range events {
		switch {
		case event.Role == RoleTask && event.Kind == EventEnteredRunning && taskRunning.IsZero():
			taskRunning = event.At
		case event.Role == RoleMemory && event.Kind == EventEnteredRunning && memoryRunning.IsZero():
			memoryRunning = event.At
		case event.Role == RoleTask && event.Kind == EventSettled && taskSettled.IsZero():
			taskSettled = event.At
		case event.Role == RoleMemory && event.Kind == EventSettled && memorySettled.IsZero():
			memorySettled = event.At
		}
	}
	if taskRunning.IsZero() || memoryRunning.IsZero() || taskSettled.IsZero() || memorySettled.IsZero() {
		t.Fatalf("incomplete timeline: %#v", events)
	}
	bothRunning := taskRunning
	if memoryRunning.After(bothRunning) {
		bothRunning = memoryRunning
	}
	firstSettled := taskSettled
	if memorySettled.Before(firstSettled) {
		firstSettled = memorySettled
	}
	if !bothRunning.Before(firstSettled) && !bothRunning.Equal(firstSettled) {
		t.Fatalf("start-barrier failed: last running=%v first settled=%v events=%#v", bothRunning, firstSettled, events)
	}
	if !taskRunning.Before(memorySettled) {
		t.Fatalf("task entered running at %v, after memory settled at %v", taskRunning, memorySettled)
	}
	if !memoryRunning.Before(taskSettled) {
		t.Fatalf("memory entered running at %v, after task settled at %v", memoryRunning, taskSettled)
	}
}

func assertOpeningOnly(t *testing.T, got MemoryInput, opening OpeningContext) {
	t.Helper()
	if !sameOpening(got.Opening, opening) {
		t.Fatalf("memory opening = %#v, want %#v", got.Opening, opening)
	}
	if len(got.ToolStates) != 0 {
		t.Fatalf("memory input contains tool state: %#v", got.ToolStates)
	}
	if len(got.FollowUps) != 0 {
		t.Fatalf("memory input contains follow-up messages: %#v", got.FollowUps)
	}
}

func assertOpeningOnlyInput(t *testing.T, got SessionInput, opening OpeningContext) {
	t.Helper()
	if !sameOpening(got.Opening, opening) {
		t.Fatalf("memory session opening = %#v, want %#v", got.Opening, opening)
	}
	if len(got.ToolStates) != 0 || len(got.FollowUps) != 0 {
		t.Fatalf("memory session input leaked later task state: %#v", got)
	}
}

func sameOpening(got, want OpeningContext) bool {
	if len(got.Messages) != len(want.Messages) {
		return false
	}
	for i := range want.Messages {
		if got.Messages[i] != want.Messages[i] {
			return false
		}
	}
	return true
}

func TestAgentRunCoordinatorDoesNotOverlapSameAgent(t *testing.T) {
	flights := NewAgentRunCoordinator()
	var current atomic.Int32
	var max atomic.Int32
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := flights.Run(ctx, "one-agent", func() error {
				active := current.Add(1)
				for {
					seen := max.Load()
					if active <= seen || max.CompareAndSwap(seen, active) {
						break
					}
				}
				time.Sleep(5 * time.Millisecond)
				current.Add(-1)
				return nil
			}); err != nil {
				t.Errorf("Run() error = %v", err)
			}
		}()
	}
	wg.Wait()
	if max.Load() != 1 {
		t.Fatalf("same agent had %d overlapping Pi runs, want 1", max.Load())
	}
}
