package skillfence

import (
	"context"
	"sync"
)

// ScriptedSession is a fixture Pi session. InjectExactBody records the
// bytes that entered the session. CorruptCapture, when set, mutates the
// captured copy so tests can prove a digest mismatch fails closed.
type ScriptedSession struct {
	id string

	// CorruptCapture, if non-nil, rewrites captured bytes after inject.
	CorruptCapture func([]byte) []byte

	mu       sync.Mutex
	captured []byte
}

func NewScriptedSession(agentID string) *ScriptedSession {
	return &ScriptedSession{id: agentID}
}

func (s *ScriptedSession) AgentID() string { return s.id }

func (s *ScriptedSession) InjectExactBody(_ context.Context, body []byte) error {
	captured := append([]byte(nil), body...)
	if s.CorruptCapture != nil {
		captured = s.CorruptCapture(captured)
	}
	s.mu.Lock()
	s.captured = captured
	s.mu.Unlock()
	return nil
}

func (s *ScriptedSession) CapturedBody() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.captured...)
}

// RecordingNotifier is a fixture Memory Agent sink for structured
// resolution_error notices.
type RecordingNotifier struct {
	mu      sync.Mutex
	notices []ResolutionNotice
}

func (n *RecordingNotifier) NotifyResolutionError(_ context.Context, notice ResolutionNotice) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notices = append(n.notices, notice)
	return nil
}

func (n *RecordingNotifier) Notices() []ResolutionNotice {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]ResolutionNotice, len(n.notices))
	copy(out, n.notices)
	return out
}
