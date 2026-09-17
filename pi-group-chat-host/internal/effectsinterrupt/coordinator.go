package effectsinterrupt

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"time"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
)

var (
	ErrSessionRequired      = errors.New("effects interrupt: session is required")
	ErrAgentMismatch        = errors.New("effects interrupt: session agent does not match the binding")
	ErrSessionMismatch      = errors.New("effects interrupt: session file/id does not match the binding")
	ErrAlreadyAttached      = errors.New("effects interrupt: agent already has an attached session")
	ErrUnknownAgent         = errors.New("effects interrupt: agent is not attached")
	ErrEffectsUnknown       = errors.New("effects interrupt: side effects cannot be proven")
	ErrRetryExhausted       = errors.New("effects interrupt: retry budget exhausted")
	ErrExactSessionRequired = errors.New("effects interrupt: exact session file and id are required")
)

// Policy is the frozen mention-interrupt timing (system contract §7).
type Policy struct {
	InterruptGrace time.Duration
	KillGrace      time.Duration
}

func (p Policy) interruptGrace() time.Duration {
	if p.InterruptGrace <= 0 {
		return 50 * time.Millisecond
	}
	return p.InterruptGrace
}

func (p Policy) killGrace() time.Duration {
	if p.KillGrace <= 0 {
		return 50 * time.Millisecond
	}
	return p.KillGrace
}

// Result is one interrupt decision: cooperative abort, process-group
// escalation, and whether the runner may resume or must retry.
type Result struct {
	AgentID       string
	TaskID        string
	AttemptID     string
	NextAttemptID string
	Kind          ExecutionKind
	Outcome       string
	Signals       []string
	Resumed       bool
	Exhausted     bool
	TreeReclaimed bool
	CompletedOps  []CompletedOp
	ReplayedOps   []CompletedOp
}

type agentRuntime struct {
	binding directedoffer.Binding
	taskID  string
	session Session
	tree    ProcessTree
}

// Coordinator distinguishes model / read-only / mutating execution and
// applies the contract abort policy. It never blindly resumes an
// effects-unknown attempt.
type Coordinator struct {
	policy Policy
	runner *Runner

	mu     sync.Mutex
	agents map[string]*agentRuntime
}

// NewCoordinator builds an empty interrupt coordinator.
func NewCoordinator(policy Policy, runner *Runner) *Coordinator {
	return &Coordinator{
		policy: policy,
		runner: runner,
		agents: map[string]*agentRuntime{},
	}
}

// Attach binds one exact task session and an optional fake process tree.
// The stored Session file/id is the only resume selector.
func (c *Coordinator) Attach(binding directedoffer.Binding, taskID string, session Session, tree ProcessTree) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if session == nil {
		return ErrSessionRequired
	}
	if session.AgentID() == "" || session.AgentID() != binding.AgentID {
		return ErrAgentMismatch
	}
	got := session.Session()
	if got.File != binding.Session.File || got.ID != binding.Session.ID {
		return ErrSessionMismatch
	}
	if taskID == "" {
		taskID = binding.LogicalRunID
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.agents[binding.AgentID]; exists {
		return ErrAlreadyAttached
	}
	c.agents[binding.AgentID] = &agentRuntime{
		binding: binding,
		taskID:  taskID,
		session: session,
		tree:    tree,
	}
	return nil
}

// Detach drops a finished or quarantined agent so a retry can attach a
// new attempt ID.
func (c *Coordinator) Detach(agentID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.agents, agentID)
}

// Interrupt applies the kind-specific abort policy to the attached
// session. Model and read-only abort immediately. Mutating waits for
// completion, then aborts; a grace timeout escalates the process group
// and fail-closes when side effects stay unproven.
func (c *Coordinator) Interrupt(ctx context.Context, agentID string) (Result, error) {
	c.mu.Lock()
	agent, ok := c.agents[agentID]
	c.mu.Unlock()
	if !ok {
		return Result{}, ErrUnknownAgent
	}

	kind := agent.session.Kind()
	result := Result{
		AgentID:   agentID,
		TaskID:    agent.taskID,
		AttemptID: agent.binding.AttemptID,
		Kind:      kind,
	}

	switch kind {
	case KindModel, KindReadOnly:
		return c.abortAndResume(ctx, agent, result)
	case KindMutating:
		return c.interruptMutating(ctx, agent, result)
	default:
		return c.quarantine(agent, result)
	}
}

func (c *Coordinator) abortAndResume(ctx context.Context, agent *agentRuntime, result Result) (Result, error) {
	result.Signals = append(result.Signals, SignalCooperative)
	if err := agent.session.CooperativeAbort(ctx); err != nil {
		return result, err
	}
	if err := agent.session.WaitSettled(ctx); err != nil {
		return result, err
	}
	if err := agent.session.Resume(ctx, directedoffer.ResumeRequest{Session: agent.binding.Session}); err != nil {
		return result, err
	}
	result.Outcome = OutcomeAborted
	result.Resumed = true
	result.CompletedOps = agent.session.CompletedOps()
	result.ReplayedOps = agent.session.ReplayedOps()
	return result, nil
}

func (c *Coordinator) interruptMutating(ctx context.Context, agent *agentRuntime, result Result) (Result, error) {
	waitCtx, cancel := context.WithTimeout(ctx, c.policy.interruptGrace())
	err := agent.session.WaitCompletion(waitCtx)
	cancel()
	if err == nil && agent.session.EffectsProven() {
		got, resumeErr := c.abortAndResume(ctx, agent, result)
		return got, resumeErr
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	signals, reclaimed := c.escalate(agent.tree)
	result.Signals = append(result.Signals, signals...)
	result.TreeReclaimed = reclaimed
	if agent.session.EffectsProven() {
		got, resumeErr := c.abortAndResume(ctx, agent, result)
		got.Signals = result.Signals
		got.TreeReclaimed = reclaimed
		return got, resumeErr
	}
	return c.quarantine(agent, result)
}

func (c *Coordinator) escalate(tree ProcessTree) (signals []string, reclaimed bool) {
	if tree == nil {
		return []string{SignalTERM, SignalKILL}, false
	}
	_ = tree.Signal(syscall.SIGTERM)
	signals = append(signals, SignalTERM)
	if tree.WaitDead(c.policy.killGrace()) {
		return signals, true
	}
	_ = tree.Signal(syscall.SIGKILL)
	signals = append(signals, SignalKILL)
	return signals, tree.WaitDead(2 * time.Second)
}

func (c *Coordinator) quarantine(agent *agentRuntime, result Result) (Result, error) {
	result.Outcome = OutcomeEffectsUnknown
	result.Resumed = false
	result.CompletedOps = agent.session.CompletedOps()
	result.ReplayedOps = agent.session.ReplayedOps()
	if c.runner != nil {
		c.runner.Quarantine(agent.taskID, result.AttemptID)
		next, ok := c.runner.AllocateAttempt(agent.taskID)
		if ok {
			result.NextAttemptID = next
			return result, ErrEffectsUnknown
		}
		c.runner.MarkExhausted(agent.taskID)
		result.Exhausted = true
		result.Outcome = OutcomeRetryExhausted
		return result, ErrRetryExhausted
	}
	return result, ErrEffectsUnknown
}
