package effectsinterrupt

import (
	"fmt"
	"sync"
)

const (
	// OutcomeAborted is a proven-safe interrupt that may resume the exact
	// session.
	OutcomeAborted = "aborted"
	// OutcomeEffectsUnknown terminates the current attempt. The runner
	// retries with a new attempt ID instead of resuming.
	OutcomeEffectsUnknown = "effects_unknown"
	// OutcomeRetryExhausted keeps the preregistered task in the main
	// pass@1 denominator as 0.
	OutcomeRetryExhausted = "retry_exhausted"
)

// AttemptRecord is one append-only attempt ledger entry. A retry never
// overwrites a prior failure.
type AttemptRecord struct {
	TaskID    string
	AttemptID string
	Outcome   string
	Resumed   bool
}

// Runner owns attempt identity, quarantine, and pass@1 accounting for
// preregistered held-out tasks.
type Runner struct {
	maxAttempts int

	mu    sync.Mutex
	tasks map[string]*taskState
}

type taskState struct {
	ID            string
	Preregistered bool
	Allocated     int
	Passed        bool
	Exhausted     bool
	Attempts      []AttemptRecord
}

// NewRunner builds an empty runner with a frozen retry budget.
func NewRunner(maxAttempts int) *Runner {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return &Runner{maxAttempts: maxAttempts, tasks: map[string]*taskState{}}
}

// Preregister freezes a held-out task into the pass@1 denominator.
func (r *Runner) Preregister(taskID string) {
	if r == nil || taskID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.ensure(taskID)
	st.Preregistered = true
}

// AllocateAttempt mints a new attempt ID. It fails closed once the retry
// budget is exhausted and keeps the preregistered task.
func (r *Runner) AllocateAttempt(taskID string) (string, bool) {
	if r == nil || taskID == "" {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.ensure(taskID)
	if st.Allocated >= r.maxAttempts {
		st.Exhausted = true
		return "", false
	}
	st.Allocated++
	return fmt.Sprintf("%s-attempt-%d", taskID, st.Allocated), true
}

// Quarantine records an effects_unknown failure without resuming. The
// original attempt row is kept when a later attempt is minted.
func (r *Runner) Quarantine(taskID, attemptID string) AttemptRecord {
	rec := AttemptRecord{
		TaskID:    taskID,
		AttemptID: attemptID,
		Outcome:   OutcomeEffectsUnknown,
		Resumed:   false,
	}
	if r == nil {
		return rec
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.ensure(taskID)
	st.Attempts = append(st.Attempts, rec)
	if st.Allocated >= r.maxAttempts {
		st.Exhausted = true
	}
	return rec
}

// MarkExhausted records that the preregistered task stays in the
// denominator with score 0.
func (r *Runner) MarkExhausted(taskID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.ensure(taskID)
	st.Exhausted = true
	st.Passed = false
}

// Attempts returns the append-only ledger for taskID.
func (r *Runner) Attempts(taskID string) []AttemptRecord {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.tasks[taskID]
	if !ok {
		return nil
	}
	out := make([]AttemptRecord, len(st.Attempts))
	copy(out, st.Attempts)
	return out
}

// Kept reports that a preregistered task is still in the known set.
func (r *Runner) Kept(taskID string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.tasks[taskID]
	return ok && st.Preregistered
}

// Exhausted reports retry-budget exhaustion for taskID.
func (r *Runner) Exhausted(taskID string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.tasks[taskID]
	return ok && st.Exhausted
}

// PassAt1 is passed / all preregistered held-out tasks. Exhausted and
// failed tasks remain in the denominator as 0.
func (r *Runner) PassAt1() (passed, denom int) {
	if r == nil {
		return 0, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, st := range r.tasks {
		if !st.Preregistered {
			continue
		}
		denom++
		if st.Passed {
			passed++
		}
	}
	return passed, denom
}

func (r *Runner) ensure(taskID string) *taskState {
	st, ok := r.tasks[taskID]
	if !ok {
		st = &taskState{ID: taskID}
		r.tasks[taskID] = st
	}
	return st
}
