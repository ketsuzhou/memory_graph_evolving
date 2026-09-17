// Package diagnosisfanout fans each train trajectory into one independent
// diagnosis job, merges terminal results by the pre-assigned sequence, and
// covers every served exposure with exactly one correctness verdict.
//
// Workers are injected (fixture/stub in this package). Canonical Raw Skill
// Proposals are minted only through ProposalAdmitter, which production and
// tests bind to GMS rawproposal.Admit. Failed jobs are recorded and never
// synthesize a proposal. Consolidation is gated by CanStartConsolidation.
package diagnosisfanout

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	StatusPass          = "pass"
	StatusFail          = "fail"
	StatusTimeout       = "timeout"
	StatusNoOutput      = "no-output"
	StatusProtocolError = "protocol-error"

	CauseUnknown = "unknown"

	StageOffered  = "offered"
	StageServed   = "served"
	StageAccepted = "accepted"
	StageRejected = "rejected"

	VerdictSupported    = "supported"
	VerdictRefuted      = "refuted"
	VerdictInconclusive = "inconclusive"

	JobSucceeded = "succeeded"
	JobFailed    = "failed"
)

// PublicOutcome is the closed, public diagnosis view of a train result.
// Hidden tests, gold/reference solutions, and invented root-cause fields
// are intentionally absent: binary-only failures carry Cause=unknown.
type PublicOutcome struct {
	Status       string `json:"status"`
	CompileError string `json:"compile_error,omitempty"`
	RuntimeError string `json:"runtime_error,omitempty"`
	Cause        string `json:"cause,omitempty"`
}

func (o PublicOutcome) CanonicalString() string {
	switch o.Status {
	case StatusFail:
		switch {
		case o.CompileError != "":
			return StatusFail + ": compile_error=" + o.CompileError
		case o.RuntimeError != "":
			return StatusFail + ": runtime_error=" + o.RuntimeError
		default:
			cause := o.Cause
			if cause == "" {
				cause = CauseUnknown
			}
			return StatusFail + ": cause=" + cause
		}
	default:
		return o.Status
	}
}

func (o PublicOutcome) binaryOnlyFailure() bool {
	return o.Status == StatusFail && o.CompileError == "" && o.RuntimeError == ""
}

// InteractionSignal is a Host-owned Skill interaction fact on one trajectory.
// accepted/rejected never replace a correctness verdict.
type InteractionSignal struct {
	OfferID    string `json:"offer_id"`
	Stage      string `json:"stage"`
	BodyDigest string `json:"body_digest,omitempty"`
	ReasonCode string `json:"reason_code,omitempty"`
}

type Checkpoint struct {
	ID         string `json:"id"`
	SnapshotID string `json:"snapshot_id"`
}

type Evidence struct {
	ID              string   `json:"id"`
	TrajectoryID    string   `json:"trajectory_id"`
	SnapshotID      string   `json:"snapshot_id"`
	ObservableFacts []string `json:"observable_facts"`
}

type ServedExposure struct {
	OfferID    string `json:"offer_id"`
	BodyDigest string `json:"body_digest"`
}

type ExposureVerdict struct {
	OfferID    string `json:"offer_id"`
	BodyDigest string `json:"body_digest"`
	Verdict    string `json:"verdict"`
}

// Trajectory is the Host-side frozen train record. Hidden/gold fields exist
// only so BuildDiagnosisInput can prove they are stripped before a worker runs.
type Trajectory struct {
	Sequence           int
	ID                 string
	SnapshotID         string
	DiagnosisRunID     string
	CompleteTrajectory []string
	Outcome            PublicOutcome
	Checkpoints        map[string]Checkpoint
	Evidence           map[string]Evidence
	Signals            []InteractionSignal
	HiddenTestInput    string
	GoldSolution       string
	ReferenceSolution  string
}

// DiagnosisInput is the exact worker view: complete public trajectory,
// structured public outcome, and this trajectory's Skill interaction signals.
type DiagnosisInput struct {
	Sequence           int                   `json:"sequence"`
	TrajectoryID       string                `json:"trajectory_id"`
	SnapshotID         string                `json:"snapshot_id"`
	DiagnosisRunID     string                `json:"diagnosis_run_id"`
	CompleteTrajectory []string              `json:"complete_trajectory"`
	PublicOutcome      PublicOutcome         `json:"public_outcome"`
	Checkpoints        map[string]Checkpoint `json:"checkpoints"`
	Evidence           map[string]Evidence   `json:"evidence"`
	Signals            []InteractionSignal   `json:"signals"`
	ServedExposures    []ServedExposure      `json:"served_exposures"`
}

type WorkerDraft struct {
	IdempotencyKey string
	ProposalBody   map[string]any
	Verdicts       []ExposureVerdict
}

// DiagnosisWorker is the per-trajectory diagnosis seam. Tests inject a stub;
// this package never talks to Pi or a model.
type DiagnosisWorker interface {
	Diagnose(ctx context.Context, input DiagnosisInput) (WorkerDraft, error)
}

type AdmissionRequest struct {
	IdempotencyKey string
	DiagnosisRunID string
	TrajectoryID   string
	Body           map[string]any
}

type ProposalRef struct {
	ProposalID    string
	ContentDigest string
	URI           string
}

// ProposalAdmitter is the Host seam for GMS rawproposal.Admit. Failed jobs
// must not call it, and a failed Admit must not leave a synthetic proposal.
type ProposalAdmitter interface {
	Admit(ctx context.Context, request AdmissionRequest) (ProposalRef, error)
	Count() int
	Has(proposalID string) bool
}

type JobResult struct {
	Sequence     int
	TrajectoryID string
	Terminal     bool
	Status       string
	Failure      string
	Proposal     *ProposalRef
	Verdicts     []ExposureVerdict
}

// Coordinator fans out one terminal diagnosis job per trajectory and merges
// results in pre-assigned sequence order. ConcurrentPeak is the observed
// in-flight job count; tests use it as the concurrency barrier proof.
type Coordinator struct {
	worker   DiagnosisWorker
	admitter ProposalAdmitter

	mu             sync.Mutex
	jobs           []JobResult
	ConcurrentPeak int32
}

func NewCoordinator(worker DiagnosisWorker, admitter ProposalAdmitter) *Coordinator {
	return &Coordinator{worker: worker, admitter: admitter}
}

// CanStartConsolidation reports whether every diagnosis job is terminal.
// It is false before Run, while any job is in flight, and true only after
// the parallel barrier — including when some jobs failed.
func (c *Coordinator) CanStartConsolidation() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.jobs) == 0 {
		return false
	}
	for _, job := range c.jobs {
		if !job.Terminal {
			return false
		}
	}
	return true
}

func (c *Coordinator) Results() []JobResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]JobResult(nil), c.jobs...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out
}

// Run starts one diagnosis job per trajectory, waits for all of them to
// become terminal, and returns the sequence-ordered records. Individual job
// failures are recorded on the result; they do not abort the remaining jobs.
func (c *Coordinator) Run(ctx context.Context, trajectories []Trajectory, parallelism int) ([]JobResult, error) {
	if c.worker == nil || c.admitter == nil {
		return nil, fmt.Errorf("diagnosis fan-out requires a worker and a proposal admitter")
	}
	jobs := append([]Trajectory(nil), trajectories...)
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].Sequence < jobs[j].Sequence })

	c.mu.Lock()
	c.jobs = make([]JobResult, len(jobs))
	for i, traj := range jobs {
		c.jobs[i] = JobResult{Sequence: traj.Sequence, TrajectoryID: traj.ID}
	}
	c.ConcurrentPeak = 0
	c.mu.Unlock()

	if parallelism < 1 || parallelism > len(jobs) {
		parallelism = len(jobs)
	}
	if parallelism < 1 {
		return c.Results(), nil
	}

	var inflight atomic.Int32
	var peak atomic.Int32
	results := make([]JobResult, len(jobs))
	indexes := make(chan int)
	var wait sync.WaitGroup
	for worker := 0; worker < parallelism; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range indexes {
				n := inflight.Add(1)
				for {
					old := peak.Load()
					if n <= old || peak.CompareAndSwap(old, n) {
						break
					}
				}
				results[index] = c.execute(ctx, jobs[index])
				inflight.Add(-1)
				c.store(results[index])
			}
		}()
	}
	for index := range jobs {
		indexes <- index
	}
	close(indexes)
	wait.Wait()
	c.mu.Lock()
	c.ConcurrentPeak = peak.Load()
	c.mu.Unlock()
	return c.Results(), nil
}

func (c *Coordinator) execute(ctx context.Context, traj Trajectory) JobResult {
	result := JobResult{Sequence: traj.Sequence, TrajectoryID: traj.ID, Terminal: true}
	input := BuildDiagnosisInput(traj)
	draft, err := c.worker.Diagnose(ctx, input)
	if err != nil {
		result.Status = JobFailed
		result.Failure = err.Error()
		return result
	}
	result.Verdicts = coverServedVerdicts(input.ServedExposures, draft.Verdicts)
	ref, err := c.admitter.Admit(ctx, AdmissionRequest{
		IdempotencyKey: draft.IdempotencyKey,
		DiagnosisRunID: traj.DiagnosisRunID,
		TrajectoryID:   traj.ID,
		Body:           draft.ProposalBody,
	})
	if err != nil {
		result.Status = JobFailed
		result.Failure = err.Error()
		return result
	}
	result.Status = JobSucceeded
	result.Proposal = &ref
	return result
}

func (c *Coordinator) store(result JobResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.jobs {
		if c.jobs[i].Sequence == result.Sequence && c.jobs[i].TrajectoryID == result.TrajectoryID {
			c.jobs[i] = result
			return
		}
	}
}

// BuildDiagnosisInput copies the public frozen scope a worker is allowed to
// see. Hidden/gold source fields are dropped; binary-only failures are
// labeled cause=unknown rather than given an invented root cause.
func BuildDiagnosisInput(traj Trajectory) DiagnosisInput {
	outcome := traj.Outcome
	if outcome.binaryOnlyFailure() {
		outcome.Cause = CauseUnknown
	}
	checkpoints := make(map[string]Checkpoint, len(traj.Checkpoints))
	for id, checkpoint := range traj.Checkpoints {
		checkpoints[id] = checkpoint
	}
	evidence := make(map[string]Evidence, len(traj.Evidence))
	for id, item := range traj.Evidence {
		item.ObservableFacts = append([]string(nil), item.ObservableFacts...)
		evidence[id] = item
	}
	return DiagnosisInput{
		Sequence:           traj.Sequence,
		TrajectoryID:       traj.ID,
		SnapshotID:         traj.SnapshotID,
		DiagnosisRunID:     traj.DiagnosisRunID,
		CompleteTrajectory: append([]string(nil), traj.CompleteTrajectory...),
		PublicOutcome:      outcome,
		Checkpoints:        checkpoints,
		Evidence:           evidence,
		Signals:            append([]InteractionSignal(nil), traj.Signals...),
		ServedExposures:    servedExposures(traj.Signals),
	}
}

func servedExposures(signals []InteractionSignal) []ServedExposure {
	out := make([]ServedExposure, 0)
	seen := make(map[string]bool)
	for _, signal := range signals {
		if signal.Stage != StageServed || seen[signal.OfferID] {
			continue
		}
		seen[signal.OfferID] = true
		out = append(out, ServedExposure{OfferID: signal.OfferID, BodyDigest: signal.BodyDigest})
	}
	return out
}

func coverServedVerdicts(served []ServedExposure, draft []ExposureVerdict) []ExposureVerdict {
	byOffer := make(map[string]string, len(draft))
	for _, verdict := range draft {
		if !validVerdict(verdict.Verdict) {
			continue
		}
		byOffer[verdict.OfferID] = verdict.Verdict
	}
	out := make([]ExposureVerdict, 0, len(served))
	for _, exposure := range served {
		verdict := byOffer[exposure.OfferID]
		if verdict == "" {
			verdict = VerdictInconclusive
		}
		out = append(out, ExposureVerdict{
			OfferID:    exposure.OfferID,
			BodyDigest: exposure.BodyDigest,
			Verdict:    verdict,
		})
	}
	return out
}

func validVerdict(verdict string) bool {
	switch verdict {
	case VerdictSupported, VerdictRefuted, VerdictInconclusive:
		return true
	default:
		return false
	}
}

// PublicJSON is a leak check helper: the worker-visible input must not carry
// hidden-test or gold/reference material.
func PublicJSON(input DiagnosisInput) ([]byte, error) {
	return json.Marshal(input)
}

func ContainsProhibitedLeak(text string) bool {
	normalized := strings.ToLower(text)
	for _, forbidden := range []string{
		"hidden test", "hidden_test", "gold solution", "gold answer",
		"reference solution", "gold output",
	} {
		if strings.Contains(normalized, forbidden) {
			return true
		}
	}
	return false
}
