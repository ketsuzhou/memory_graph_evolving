package effectsinterrupt

import (
	"context"
	"errors"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
)

// ExecutionKind is the Host-observed agent execution that selects the
// interrupt policy (system contract §4.3).
type ExecutionKind string

const (
	KindModel    ExecutionKind = "model"
	KindReadOnly ExecutionKind = "read_only"
	KindMutating ExecutionKind = "mutating"
)

// CompletedOp is one mutating tool result that must survive abort and must
// not be replayed on a later attempt or exact-session resume.
type CompletedOp = directedoffer.ToolResult

// Session is the scripted exact-session seam. It never launches Pi.
type Session interface {
	AgentID() string
	Session() sessionctrl.Session
	Kind() ExecutionKind
	Running() bool
	Start(ctx context.Context) error
	CooperativeAbort(ctx context.Context) error
	WaitSettled(ctx context.Context) error
	WaitCompletion(ctx context.Context) error
	EffectsProven() bool
	Resume(ctx context.Context, req directedoffer.ResumeRequest) error
	CompletedOps() []CompletedOp
	ReplayedOps() []CompletedOp
	Observation() Observation
}

// Observation is a point-in-time view of a scripted session.
type Observation struct {
	Phase         directedoffer.Phase
	Kind          ExecutionKind
	Session       sessionctrl.Session
	AbortCount    int
	ResumeCount   int
	CompletedOps  []CompletedOp
	ReplayedOps   []CompletedOp
	CompletionAt  time.Time
	AbortAt       time.Time
	EffectsProven bool
}

// ScriptedSession is a fixture exact session. Kind selects the abort
// policy. A mutating ToolHold stays in-flight until the test closes it;
// completion is then observable before CooperativeAbort.
type ScriptedSession struct {
	id      string
	session sessionctrl.Session
	kind    ExecutionKind

	// Hold, if non-nil, keeps Start running after any tool work until the
	// channel is closed or Abort is observed.
	Hold <-chan struct{}
	// ToolHold, if non-nil, keeps a mutating tool in-flight until closed.
	// Model and read-only sessions ignore it so an immediate abort does not
	// wait on a tool.
	ToolHold <-chan struct{}
	// Completed is the durable mutating result set that resume must
	// preserve and must not replay.
	Completed []CompletedOp

	mu           sync.Mutex
	phase        directedoffer.Phase
	abortCount   int
	resumeCount  int
	replayed     []CompletedOp
	completedAt  time.Time
	abortAt      time.Time
	proven       bool
	abortCh      chan struct{}
	settledCh    chan struct{}
	completedCh  chan struct{}
	settleOnce   sync.Once
	completeOnce sync.Once
}

// NewScriptedSession constructs an idle fake session bound to the
// Host-recorded exact file/id and one execution kind.
func NewScriptedSession(agentID string, session sessionctrl.Session, kind ExecutionKind) *ScriptedSession {
	return &ScriptedSession{
		id:          agentID,
		session:     session,
		kind:        kind,
		phase:       directedoffer.PhaseIdle,
		abortCh:     make(chan struct{}),
		settledCh:   make(chan struct{}),
		completedCh: make(chan struct{}),
	}
}

func (s *ScriptedSession) AgentID() string              { return s.id }
func (s *ScriptedSession) Session() sessionctrl.Session { return s.session }
func (s *ScriptedSession) Kind() ExecutionKind          { return s.kind }

// Running reports whether Start has entered the running phase and has not
// yet settled.
func (s *ScriptedSession) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phase == directedoffer.PhaseRunning
}

// Start enters running. A mutating session waits for ToolHold before
// publishing completion; model and read-only never wait on a tool.
func (s *ScriptedSession) Start(ctx context.Context) error {
	s.mu.Lock()
	s.phase = directedoffer.PhaseRunning
	s.mu.Unlock()

	if s.kind == KindMutating && s.ToolHold != nil {
		select {
		case <-s.ToolHold:
			s.observeCompletion()
		case <-s.abortCh:
			s.settle()
			return nil
		case <-ctx.Done():
			s.settle()
			return ctx.Err()
		}
	}

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

// CooperativeAbort is the immediate RPC abort used for model generation
// and read-only tools. Mutating callers must observe completion first.
func (s *ScriptedSession) CooperativeAbort(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.abortCount++
	s.abortAt = time.Now()
	first := s.abortCount == 1
	s.mu.Unlock()
	if first {
		close(s.abortCh)
	}
	return nil
}

// WaitSettled waits for Start to observe abort (or Hold) and settle.
func (s *ScriptedSession) WaitSettled(ctx context.Context) error {
	select {
	case <-s.settledCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitCompletion waits until a mutating tool result has been observed.
func (s *ScriptedSession) WaitCompletion(ctx context.Context) error {
	if s.EffectsProven() {
		return nil
	}
	select {
	case <-s.completedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EffectsProven reports whether the Host observed a mutating completion
// (or the session never had an unproven side effect).
func (s *ScriptedSession) EffectsProven() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proven
}

// Resume continues the same exact session. A mismatched Session is
// refused. Completed mutating ops are left untouched and are not copied
// into the replayed set.
func (s *ScriptedSession) Resume(_ context.Context, req directedoffer.ResumeRequest) error {
	if req.Session.File != s.session.File || req.Session.ID != s.session.ID {
		return errors.New("effects interrupt: resume must use the stored exact session file and id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resumeCount++
	s.phase = directedoffer.PhaseResumed
	return nil
}

func (s *ScriptedSession) CompletedOps() []CompletedOp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneOps(s.Completed)
}

func (s *ScriptedSession) ReplayedOps() []CompletedOp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneOps(s.replayed)
}

func (s *ScriptedSession) Observation() Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Observation{
		Phase:         s.phase,
		Kind:          s.kind,
		Session:       s.session,
		AbortCount:    s.abortCount,
		ResumeCount:   s.resumeCount,
		CompletedOps:  cloneOps(s.Completed),
		ReplayedOps:   cloneOps(s.replayed),
		CompletionAt:  s.completedAt,
		AbortAt:       s.abortAt,
		EffectsProven: s.proven,
	}
}

func (s *ScriptedSession) observeCompletion() {
	s.completeOnce.Do(func() {
		s.mu.Lock()
		s.proven = true
		s.completedAt = time.Now()
		s.mu.Unlock()
		close(s.completedCh)
	})
}

func (s *ScriptedSession) settle() {
	s.settleOnce.Do(func() {
		s.mu.Lock()
		s.phase = directedoffer.PhaseSettled
		s.mu.Unlock()
		close(s.settledCh)
	})
}

func cloneOps(src []CompletedOp) []CompletedOp {
	if len(src) == 0 {
		return nil
	}
	out := make([]CompletedOp, len(src))
	copy(out, src)
	return out
}
