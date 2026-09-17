// Package concurrentepisode starts a task session and an opening-only Memory
// Agent together. Memory sees only the frozen opening context; an empty graph
// ends as no_candidate_from_empty_graph without aborting the task.
//
// This package is the Host-side coordinator seam later composed with
// sessionctrl. Tests use ScriptedSession; production Pi is not required here.
package concurrentepisode

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	// TerminalNoCandidateFromEmptyGraph is the normal empty-graph Memory end.
	// It must not be reclassified as a timeout, memory error, or protocol error.
	TerminalNoCandidateFromEmptyGraph = "no_candidate_from_empty_graph"
	// TerminalNoApplicableCandidate is the normal no-skill end on a non-empty
	// graph that produced no matching seed.
	TerminalNoApplicableCandidate = "no_applicable_candidate"
	// TerminalMemoryTimeout is a mechanism failure, not a normal no-skill end.
	TerminalMemoryTimeout = "memory_timeout"
	// TerminalMemoryError is a mechanism failure, not a normal no-skill end.
	TerminalMemoryError = "memory_error"

	TaskStatusCompleted = "completed"
	TaskStatusFailed    = "failed"

	RoleTask   = "task"
	RoleMemory = "memory"

	EventEnteredRunning = "entered_running"
	EventSettled        = "settled"

	ContextOpeningOnly = "opening_only"

	FailureNormalNoSkill = "normal_no_skill"
	FailureMemoryTimeout = "memory_timeout"
	FailureMemoryError   = "memory_error"
)

var (
	ErrTaskSessionRequired    = errors.New("concurrent episode: task session is required")
	ErrMemorySessionRequired  = errors.New("concurrent episode: memory session is required")
	ErrDistinctAgentsRequired = errors.New("concurrent episode: task and memory must be distinct agents")
	ErrOpeningOnlyRequired    = errors.New("concurrent episode: task_context_monitoring must be opening_only")
	ErrMemoryTimeout          = errors.New("concurrent episode: memory timeout")
)

// OpeningMessage is one frozen opening-context message.
type OpeningMessage struct {
	ID      string
	Author  string
	Content string
}

// OpeningContext is the only task context Memory may read.
type OpeningContext struct {
	Messages []OpeningMessage
}

// ToolState is intermediate task-agent tool progress. Opening-only Memory
// input must never include it.
type ToolState struct {
	CallID string
	Name   string
	Status string
	Output string
}

// EvaluationGraph is the generation-0 snapshot Memory inspects. An empty
// graph is a normal no-skill path, not a missing-graph error.
type EvaluationGraph struct {
	SkillIDs []string
	EdgeIDs  []string
}

// Empty reports whether the graph has no nodes and no edges.
func (g EvaluationGraph) Empty() bool {
	return len(g.SkillIDs) == 0 && len(g.EdgeIDs) == 0
}

// SessionInput is what one scripted Pi run is allowed to see.
type SessionInput struct {
	Opening    OpeningContext
	ToolStates []ToolState
	FollowUps  []OpeningMessage
}

// MemoryInput is the coordinator-recorded Memory view. Opening-only mode
// copies the opening snapshot and leaves tool/follow-up slices empty.
type MemoryInput struct {
	Opening    OpeningContext
	ToolStates []ToolState
	FollowUps  []OpeningMessage
}

// Offer is a directed Skill offer. TB-06 never produces one on empty or
// no-applicable terminals, so the task session is not aborted or resumed.
type Offer struct {
	SkillReference string
	Recipient      string
}

// Event is one start-barrier / settlement observation.
type Event struct {
	AgentID string
	Role    string
	Kind    string
	At      time.Time
}

// Request starts one concurrent opening-only episode.
type Request struct {
	Opening                OpeningContext
	Graph                  EvaluationGraph
	ApplicableCandidateIDs []string
	Task                   Session
	Memory                 Session
	TaskContextMonitoring  string
	Flights                *AgentRunCoordinator
}

// Result is the episode artifact: Memory terminal, task status, and the
// observations tests use to lock concurrency and isolation.
type Result struct {
	MemoryTerminal string
	TaskStatus     string
	FailureClass   string
	Offers         []Offer
	MemoryInput    MemoryInput
	Timeline       []Event
}

// Session is the fake/scripted exact-session seam. Later tickets may wrap
// sessionctrl; this package must not depend on a real Pi process.
type Session interface {
	AgentID() string
	Start(ctx context.Context, input SessionInput, gate RunGate) error
	Abort(ctx context.Context) error
	Resume(ctx context.Context) error
	Observation() Observation
}

// RunGate is the start-barrier hook a session must call after it has entered
// running and before it does work that can settle.
type RunGate interface {
	EnterRunning(ctx context.Context) error
}

// Observation is a point-in-time view of a scripted session.
type Observation struct {
	Phase       Phase
	AbortCount  int
	ResumeCount int
	Input       SessionInput
	ToolStates  []ToolState
	FollowUps   []OpeningMessage
}

// Phase is the coarse Pi-run lifecycle used by the start-barrier proof.
type Phase string

const (
	PhaseIdle    Phase = "idle"
	PhaseRunning Phase = "running"
	PhaseSettled Phase = "settled"
)

// IsNormalNoSkill reports the contract's two normal no-skill terminals.
func IsNormalNoSkill(terminal string) bool {
	return terminal == TerminalNoCandidateFromEmptyGraph || terminal == TerminalNoApplicableCandidate
}

// Run starts task and Memory together behind a start-barrier. Memory receives
// only a copy of the opening context. The task run does not wait for Memory,
// and a no-offer Memory path never abort/resumes the task session.
func Run(ctx context.Context, req Request) (Result, error) {
	if req.Task == nil {
		return Result{}, ErrTaskSessionRequired
	}
	if req.Memory == nil {
		return Result{}, ErrMemorySessionRequired
	}
	if req.Task.AgentID() == "" || req.Memory.AgentID() == "" || req.Task.AgentID() == req.Memory.AgentID() {
		return Result{}, ErrDistinctAgentsRequired
	}
	if req.TaskContextMonitoring != "" && req.TaskContextMonitoring != ContextOpeningOnly {
		return Result{}, ErrOpeningOnlyRequired
	}

	flights := req.Flights
	if flights == nil {
		flights = NewAgentRunCoordinator()
	}
	opening := cloneOpening(req.Opening)
	memoryInput := MemoryInput{Opening: cloneOpening(opening)}
	timeline := &timeline{}
	barrier := newStartBarrier(2)

	var (
		wg      sync.WaitGroup
		taskErr error
		memErr  error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		memErr = flights.Run(ctx, req.Memory.AgentID(), func() error {
			err := req.Memory.Start(ctx, SessionInput{Opening: cloneOpening(opening)}, &runGate{
				role:     RoleMemory,
				agentID:  req.Memory.AgentID(),
				timeline: timeline,
				barrier:  barrier,
			})
			timeline.record(req.Memory.AgentID(), RoleMemory, EventSettled)
			return err
		})
	}()
	go func() {
		defer wg.Done()
		taskErr = flights.Run(ctx, req.Task.AgentID(), func() error {
			err := req.Task.Start(ctx, SessionInput{Opening: cloneOpening(opening)}, &runGate{
				role:     RoleTask,
				agentID:  req.Task.AgentID(),
				timeline: timeline,
				barrier:  barrier,
			})
			timeline.record(req.Task.AgentID(), RoleTask, EventSettled)
			return err
		})
	}()
	wg.Wait()

	terminal := classifyMemory(req.Graph, req.ApplicableCandidateIDs, memErr)
	// Empty / no-applicable / timeout / error never produce an offer, so this
	// coordinator must not abort or resume the task session.

	taskStatus := TaskStatusCompleted
	if taskErr != nil {
		taskStatus = TaskStatusFailed
	}
	return Result{
		MemoryTerminal: terminal,
		TaskStatus:     taskStatus,
		FailureClass:   failureClass(terminal),
		Offers:         nil,
		MemoryInput:    memoryInput,
		Timeline:       timeline.snapshot(),
	}, nil
}

func classifyMemory(graph EvaluationGraph, applicable []string, runErr error) string {
	if runErr != nil {
		if errors.Is(runErr, ErrMemoryTimeout) || errors.Is(runErr, context.DeadlineExceeded) || errors.Is(runErr, context.Canceled) {
			return TerminalMemoryTimeout
		}
		return TerminalMemoryError
	}
	if graph.Empty() {
		return TerminalNoCandidateFromEmptyGraph
	}
	if len(applicable) == 0 {
		return TerminalNoApplicableCandidate
	}
	// A non-empty graph with applicable candidates is owned by later explore
	// tickets. This coordinator still treats the absence of an offer as a
	// normal no-applicable end so it cannot invent a delivery.
	return TerminalNoApplicableCandidate
}

func failureClass(terminal string) string {
	switch terminal {
	case TerminalNoCandidateFromEmptyGraph, TerminalNoApplicableCandidate:
		return FailureNormalNoSkill
	case TerminalMemoryTimeout:
		return FailureMemoryTimeout
	case TerminalMemoryError:
		return FailureMemoryError
	default:
		return FailureMemoryError
	}
}

func cloneOpening(src OpeningContext) OpeningContext {
	if len(src.Messages) == 0 {
		return OpeningContext{}
	}
	out := OpeningContext{Messages: make([]OpeningMessage, len(src.Messages))}
	copy(out.Messages, src.Messages)
	return out
}

type timeline struct {
	mu     sync.Mutex
	events []Event
}

func (t *timeline) record(agentID, role, kind string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, Event{AgentID: agentID, Role: role, Kind: kind, At: time.Now()})
}

func (t *timeline) snapshot() []Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Event, len(t.events))
	copy(out, t.events)
	return out
}

type startBarrier struct {
	n     int
	mu    sync.Mutex
	count int
	ready chan struct{}
}

func newStartBarrier(n int) *startBarrier {
	return &startBarrier{n: n, ready: make(chan struct{})}
}

func (b *startBarrier) Enter(ctx context.Context) error {
	b.mu.Lock()
	b.count++
	if b.count == b.n {
		close(b.ready)
	}
	b.mu.Unlock()
	select {
	case <-b.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type runGate struct {
	role     string
	agentID  string
	timeline *timeline
	barrier  *startBarrier
}

func (g *runGate) EnterRunning(ctx context.Context) error {
	g.timeline.record(g.agentID, g.role, EventEnteredRunning)
	return g.barrier.Enter(ctx)
}
