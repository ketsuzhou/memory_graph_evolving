package directedoffer

import (
	"context"
	"errors"
	"sync"

	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
)

// Phase is the coarse exact-session lifecycle observed by tests.
type Phase string

const (
	PhaseIdle    Phase = "idle"
	PhaseRunning Phase = "running"
	PhaseSettled Phase = "settled"
	PhaseResumed Phase = "resumed"
)

// ToolResult is a completed tool execution that must survive abort and
// must not be replayed on exact-session resume.
type ToolResult struct {
	CallID string
	Name   string
	Output string
}

// ResumeRequest is the Host-authored exact-session continuation. Session is
// the stored file/id passed to Pi as --session; Messages are the pending
// directed mentions in Room sequence. Completed tool results are omitted.
type ResumeRequest struct {
	Session  sessionctrl.Session
	Messages []QueuedMessage
}

// SessionObservation is a point-in-time view of a scripted exact session.
type SessionObservation struct {
	Phase          Phase
	Session        sessionctrl.Session
	AbortCount     int
	ResumeCount    int
	Injected       []QueuedMessage
	CompletedTools []ToolResult
	ReplayedTools  []ToolResult
	LastResume     ResumeRequest
}

// ExactSession is the fake/scripted exact-session seam. Later tickets may
// wrap sessionctrl; this package must not launch a real Pi process.
type ExactSession interface {
	AgentID() string
	Session() sessionctrl.Session
	Running() bool
	Start(ctx context.Context) error
	Abort(ctx context.Context) error
	WaitSettled(ctx context.Context) error
	Resume(ctx context.Context, req ResumeRequest) error
	Observation() SessionObservation
}

// ScriptedExactSession is a fixture exact session. It pins one session
// file/id, records completed tool results, and counts abort/resume without
// launching Pi.
type ScriptedExactSession struct {
	id      string
	session sessionctrl.Session

	// Hold, if non-nil, keeps Start running until the channel is closed or
	// Abort is observed. Tests use it to present a live task session.
	Hold <-chan struct{}
	// AbortHold, if non-nil, blocks the first Abort until the channel is
	// closed. Concurrent-mention tests release it after every mention has
	// been queued so one interrupt collects the full Room sequence.
	AbortHold <-chan struct{}
	// Completed, if set before Start, is the durable tool-result set that
	// resume must preserve and must not replay.
	Completed []ToolResult

	mu          sync.Mutex
	phase       Phase
	abortCount  int
	resumeCount int
	injected    []QueuedMessage
	replayed    []ToolResult
	lastResume  ResumeRequest
	abortCh     chan struct{}
	settledCh   chan struct{}
	settleOnce  sync.Once
}

// NewScriptedExactSession constructs an idle fake session bound to the
// Host-recorded exact file/id.
func NewScriptedExactSession(agentID string, session sessionctrl.Session) *ScriptedExactSession {
	return &ScriptedExactSession{
		id:        agentID,
		session:   session,
		phase:     PhaseIdle,
		abortCh:   make(chan struct{}),
		settledCh: make(chan struct{}),
	}
}

func (s *ScriptedExactSession) AgentID() string              { return s.id }
func (s *ScriptedExactSession) Session() sessionctrl.Session { return s.session }

// Running reports whether Start has entered the running phase and has not
// yet settled.
func (s *ScriptedExactSession) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phase == PhaseRunning
}

// Start enters running and blocks until Hold closes, Abort is observed, or
// ctx ends. Abort does not discard Completed tool results.
func (s *ScriptedExactSession) Start(ctx context.Context) error {
	s.mu.Lock()
	s.phase = PhaseRunning
	s.mu.Unlock()

	select {
	case <-s.Hold:
	case <-s.abortCh:
	case <-ctx.Done():
		s.settle()
		return ctx.Err()
	}
	s.settle()
	return nil
}

// Abort asks the scripted session to stop. Every call is counted so a
// coordinator that aborts twice fails the single-interrupt proof.
func (s *ScriptedExactSession) Abort(ctx context.Context) error {
	s.mu.Lock()
	s.abortCount++
	first := s.abortCount == 1
	hold := s.AbortHold
	s.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if first {
		close(s.abortCh)
	}
	return nil
}

// WaitSettled waits for Start to observe abort (or Hold) and settle.
func (s *ScriptedExactSession) WaitSettled(ctx context.Context) error {
	select {
	case <-s.settledCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Resume continues the same exact session. A mismatched Session is refused
// so callers cannot fall back to interactive --resume or a new file/id.
// Completed tool results are left untouched and are not copied into the
// replayed set.
func (s *ScriptedExactSession) Resume(_ context.Context, req ResumeRequest) error {
	if req.Session.File != s.session.File || req.Session.ID != s.session.ID {
		return errors.New("directed offer: resume must use the stored exact session file and id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resumeCount++
	s.lastResume = ResumeRequest{
		Session:  req.Session,
		Messages: cloneQueued(req.Messages),
	}
	s.injected = cloneQueued(req.Messages)
	s.phase = PhaseResumed
	return nil
}

func (s *ScriptedExactSession) Observation() SessionObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SessionObservation{
		Phase:          s.phase,
		Session:        s.session,
		AbortCount:     s.abortCount,
		ResumeCount:    s.resumeCount,
		Injected:       cloneQueued(s.injected),
		CompletedTools: cloneTools(s.Completed),
		ReplayedTools:  cloneTools(s.replayed),
		LastResume:     ResumeRequest{Session: s.lastResume.Session, Messages: cloneQueued(s.lastResume.Messages)},
	}
}

func (s *ScriptedExactSession) settle() {
	s.settleOnce.Do(func() {
		s.mu.Lock()
		s.phase = PhaseSettled
		s.mu.Unlock()
		close(s.settledCh)
	})
}

func cloneTools(src []ToolResult) []ToolResult {
	if len(src) == 0 {
		return nil
	}
	out := make([]ToolResult, len(src))
	copy(out, src)
	return out
}
