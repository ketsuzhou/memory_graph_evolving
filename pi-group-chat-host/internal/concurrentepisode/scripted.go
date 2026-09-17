package concurrentepisode

import (
	"context"
	"sync"
	"time"
)

// ScriptedSession is a fake exact session. It records opening input, optional
// mid-run tool state, and abort/resume counts without launching Pi.
type ScriptedSession struct {
	id string

	// Hold, if non-nil, keeps the session running until the channel is closed
	// or the run context ends. Tests use it to observe overlap and prove the
	// task does not wait for Memory.
	Hold <-chan struct{}
	// Delay is extra work after the start-barrier, used to order settlement.
	Delay time.Duration
	// Timeout makes Start return ErrMemoryTimeout after the barrier.
	Timeout bool
	// Err, if set, is returned after the barrier as a Memory/task error.
	Err error
	// EmitTaskProgress appends intermediate tool state and a follow-up
	// message after the session has entered running. Memory must not see it.
	EmitTaskProgress bool

	mu          sync.Mutex
	phase       Phase
	abortCount  int
	resumeCount int
	input       SessionInput
	toolStates  []ToolState
	followUps   []OpeningMessage
	runningAt   time.Time
	settledAt   time.Time
}

// NewScriptedSession constructs an idle fake session for agentID.
func NewScriptedSession(agentID string) *ScriptedSession {
	return &ScriptedSession{id: agentID, phase: PhaseIdle}
}

func (s *ScriptedSession) AgentID() string { return s.id }

// Start enters running, waits at the start-barrier, then performs scripted
// work. Timeout and error are applied only after the session is running so
// the barrier proof still holds.
func (s *ScriptedSession) Start(ctx context.Context, input SessionInput, gate RunGate) error {
	s.mu.Lock()
	s.input = cloneSessionInput(input)
	s.phase = PhaseRunning
	s.runningAt = time.Now()
	s.mu.Unlock()

	if gate != nil {
		if err := gate.EnterRunning(ctx); err != nil {
			s.settle()
			return err
		}
	}

	if s.EmitTaskProgress {
		s.mu.Lock()
		s.toolStates = append(s.toolStates, ToolState{
			CallID: "tool-mid-1",
			Name:   "bash",
			Status: "running",
			Output: "intermediate task tool state",
		})
		s.followUps = append(s.followUps, OpeningMessage{
			ID:      "msg-after-opening",
			Author:  s.id,
			Content: "follow-up after opening context",
		})
		s.mu.Unlock()
	}

	if s.Hold != nil {
		select {
		case <-s.Hold:
		case <-ctx.Done():
			s.settle()
			return ctx.Err()
		}
	}
	if s.Delay > 0 {
		timer := time.NewTimer(s.Delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			s.settle()
			return ctx.Err()
		}
	}
	if s.Timeout {
		s.settle()
		return ErrMemoryTimeout
	}
	if s.Err != nil {
		err := s.Err
		s.settle()
		return err
	}
	s.settle()
	return nil
}

func (s *ScriptedSession) Abort(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abortCount++
	return nil
}

func (s *ScriptedSession) Resume(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resumeCount++
	return nil
}

func (s *ScriptedSession) Observation() Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Observation{
		Phase:       s.phase,
		AbortCount:  s.abortCount,
		ResumeCount: s.resumeCount,
		Input:       cloneSessionInput(s.input),
		ToolStates:  cloneToolStates(s.toolStates),
		FollowUps:   cloneMessages(s.followUps),
	}
}

func (s *ScriptedSession) RunningAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runningAt
}

func (s *ScriptedSession) SettledAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settledAt
}

func (s *ScriptedSession) settle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase = PhaseSettled
	s.settledAt = time.Now()
}

func cloneSessionInput(src SessionInput) SessionInput {
	return SessionInput{
		Opening:    cloneOpening(src.Opening),
		ToolStates: cloneToolStates(src.ToolStates),
		FollowUps:  cloneMessages(src.FollowUps),
	}
}

func cloneToolStates(src []ToolState) []ToolState {
	if len(src) == 0 {
		return nil
	}
	out := make([]ToolState, len(src))
	copy(out, src)
	return out
}

func cloneMessages(src []OpeningMessage) []OpeningMessage {
	if len(src) == 0 {
		return nil
	}
	out := make([]OpeningMessage, len(src))
	copy(out, src)
	return out
}
