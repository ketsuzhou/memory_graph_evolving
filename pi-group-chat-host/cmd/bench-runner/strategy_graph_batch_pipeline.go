package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	"river2.dev/pi-group-chat-host/internal/diagnosisfanout"
	"river2.dev/pi-group-chat-host/internal/effectsinterrupt"
)

var (
	graphBatchPartialDigestRE = regexp.MustCompile(`^sha256:[0-9a-f]{1,63}$`)
	graphBatchShortHexRE      = regexp.MustCompile(`^[0-9a-f]{7,16}$`)
)

// graphBatchRuntime is the TB-14 composition root. Host diagnosis fan-out
// and concurrent held-out episodes are wired here; GMS consolidation and
// freeze are injected ports so the runner never treats []skillProposal as
// a write authority.
type graphBatchRuntime struct {
	Config      graphBatchFrozenConfig
	Parallelism int

	Trains []graphBatchTrainSpec

	DiagnosisWorker   diagnosisfanout.DiagnosisWorker
	DiagnosisAdmitter diagnosisfanout.ProposalAdmitter

	Consolidator graphBatchConsolidator
	Freezer      graphBatchFreezer
	Retriever    graphBatchRetriever

	TrainExecutor graphBatchTrainExecutor
	AuthorityKind string

	HeldOut []graphBatchHeldOutSpec
	Probe   *graphBatchProbe
}

const (
	graphBatchAuthorityGMS     = "gms_canonical"
	graphBatchAuthorityFixture = "fixture_memory"
)

type graphBatchTrainSpec struct {
	Sequence   int
	Episode    *manifestEpisode
	Trajectory diagnosisfanout.Trajectory
	Hold       <-chan struct{}
}

// graphBatchTrainExecutor runs one generation-0 train task and returns the
// frozen trajectory plus a structured public outcome. The pipeline refuses
// to diagnose a train that was never executed.
type graphBatchTrainExecutor interface {
	Execute(ctx context.Context, spec graphBatchTrainSpec) (diagnosisfanout.Trajectory, error)
}

type graphBatchHeldOutSpec struct {
	Sequence      int
	AttemptID     string
	Opening       concurrentepisode.OpeningContext
	Graph         concurrentepisode.EvaluationGraph
	ApplicableIDs []string
	Feedback      []graphBatchTestFeedback
	TaskHold      <-chan struct{}
	MemoryHold    <-chan struct{}
	MemoryTimeout bool
	MemoryErr     error
}

type graphBatchTestFeedback struct {
	OfferID     string
	Disposition string
	ReasonCode  string
	Reason      string
}

type graphBatchCanonicalProposal struct {
	ProposalID    string
	ContentDigest string
	TrajectoryID  string
}

type graphBatchConsolidationDecision struct {
	DecisionID        string
	Operation         string
	SourceProposalIDs []string
}

type graphBatchConsolidateRequest struct {
	FamilyProposalIDs []string
}

type graphBatchConsolidateResult struct {
	LedgerRevision uint64
	LedgerDigest   string
	Decisions      []graphBatchConsolidationDecision
}

type graphBatchRetrieval struct {
	ManifestDigest string
	Keys           []string
}

type graphBatchHeldOutTrace struct {
	AttemptID      string
	ManifestDigest string
	Feedback       []graphBatchTestFeedback
}

type graphBatchTestAttempt struct {
	AttemptID      string
	ManifestDigest string
	Retrieval      graphBatchRetrieval
	MemoryTerminal string
	TaskStatus     string
	FailureClass   string
}

type graphBatchAuthorityRecord struct {
	Kind                         string
	UsedRunnerLocalSkillProposal bool
	UsedMarkdownHashPrefix       bool
	ProposalIDs                  []string
}

type graphBatchPipelineResult struct {
	Strategy               string
	FrozenConfig           graphBatchFrozenConfig
	TrainPeak              int32
	DiagnosisPeak          int32
	ConsolidationCalls     int
	ConsolidationDecisions []graphBatchConsolidationDecision
	ManifestDigest         string
	TestsStarted           int
	TestAttempts           []graphBatchTestAttempt
	HeldOutTraces          []graphBatchHeldOutTrace
	FailureAccounting      graphBatchFailureAccounting
	Authority              graphBatchAuthorityRecord
}

type graphBatchConsolidator interface {
	Consolidate(ctx context.Context, req graphBatchConsolidateRequest) (graphBatchConsolidateResult, error)
	Calls() int
}

type graphBatchFreezeRequest struct {
	ProposalIDs         []string
	Ledger              graphBatchConsolidateResult
	EvidenceBatches     []string
	EvidenceWatermark   string
	GraphDigest         string
	GraphWatermark      string
	PromptDigest        string
	SchemaDigest        string
	ModelDigest         string
	ToolDigest          string
	ConfigDigest        string
	GradingPolicyDigest string
}

type graphBatchFreezer interface {
	Freeze(ctx context.Context, req graphBatchFreezeRequest) (string, error)
	TestBinding() (string, error)
	RecordTestAttempt(attemptID string) error
	Attempts() []string
}

type graphBatchRetriever interface {
	Retrieve(attemptID, manifestDigest string, traces []graphBatchHeldOutTrace) (graphBatchRetrieval, error)
}

type graphBatchProbe struct {
	mu sync.Mutex

	trainStarted     chan int
	diagnosisStarted chan string
	testStarted      chan string
	events           []string
	testsStarted     int32
}

func newGraphBatchProbe(trains, diagnoses, tests int) *graphBatchProbe {
	return &graphBatchProbe{
		trainStarted:     make(chan int, trains),
		diagnosisStarted: make(chan string, diagnoses),
		testStarted:      make(chan string, tests),
	}
}

func (p *graphBatchProbe) event(name string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.events = append(p.events, name)
	p.mu.Unlock()
}

func (p *graphBatchProbe) snapshot() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

func (p *graphBatchProbe) startedTests() int {
	if p == nil {
		return 0
	}
	return int(atomic.LoadInt32(&p.testsStarted))
}

// runGraphBatchArm is the runArm composition seam. It never initializes the
// legacy runner-local []skillProposal ledger or emits attemptRecord rows.
func runGraphBatchArm(ctx context.Context, config armConfig, emit func(attemptRecord)) error {
	if config.graphBatch != nil {
		_, err := runGraphBatchCanonicalPipeline(ctx, config.graphBatch)
		return err
	}
	if len(config.episodes) == 0 {
		return nil
	}
	runtime, err := composeGraphBatchRuntime(config)
	if err != nil {
		return err
	}
	_, err = runGraphBatchCanonicalPipeline(ctx, runtime)
	return err
}

// runGraphBatchCanonicalPipeline is the unique composition: parallel train
// barrier → parallel diagnosis → single consolidation → unified freeze →
// parallel held-out task+memory. Test feedback is appended to traces only.
func runGraphBatchCanonicalPipeline(ctx context.Context, runtime *graphBatchRuntime) (*graphBatchPipelineResult, error) {
	if runtime == nil {
		return nil, fmt.Errorf("graph batch pipeline runtime is required")
	}
	if err := validateGraphBatchFrozenConfig(runtime.Config); err != nil {
		return nil, err
	}
	if _, err := graphBatchInterruptPolicy(runtime.Config); err != nil {
		return nil, err
	}
	if runtime.Consolidator == nil || runtime.Freezer == nil || runtime.Retriever == nil {
		return nil, fmt.Errorf("graph batch pipeline requires consolidator, freezer, and retriever ports")
	}
	if runtime.DiagnosisWorker == nil || runtime.DiagnosisAdmitter == nil {
		return nil, fmt.Errorf("graph batch pipeline requires diagnosisfanout worker and admitter")
	}
	if runtime.TrainExecutor == nil {
		return nil, fmt.Errorf("graph batch pipeline requires a train executor; train tasks must actually run")
	}

	parallelism := runtime.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}

	trainJobs := make([]parallelBatchJob[graphBatchTrainSpec], 0, len(runtime.Trains))
	for _, spec := range runtime.Trains {
		trainJobs = append(trainJobs, parallelBatchJob[graphBatchTrainSpec]{Sequence: spec.Sequence, Value: spec})
	}

	var trainPeak atomic.Int32
	var trainInflight atomic.Int32
	var trainErrMu sync.Mutex
	var trainErr error
	trainResults := runParallelBatchStage(ctx, sortedBatchJobs(trainJobs), parallelism, func(ctx context.Context, job parallelBatchJob[graphBatchTrainSpec]) diagnosisfanout.Trajectory {
		n := trainInflight.Add(1)
		for {
			old := trainPeak.Load()
			if n <= old || trainPeak.CompareAndSwap(old, n) {
				break
			}
		}
		if runtime.Probe != nil && runtime.Probe.trainStarted != nil {
			runtime.Probe.trainStarted <- job.Sequence
		}
		traj, err := runtime.TrainExecutor.Execute(ctx, job.Value)
		trainInflight.Add(-1)
		if err != nil {
			trainErrMu.Lock()
			if trainErr == nil {
				trainErr = fmt.Errorf("train task %s: %w", job.Value.Trajectory.ID, err)
			}
			trainErrMu.Unlock()
			return diagnosisfanout.Trajectory{}
		}
		return traj
	})
	runtime.Probe.event("reduce-train")
	if trainErr != nil {
		return nil, trainErr
	}

	trajectories := make([]diagnosisfanout.Trajectory, 0, len(trainResults))
	for _, result := range trainResults {
		if result.Value.ID == "" || result.Value.Outcome.Status == "" {
			return nil, fmt.Errorf("graph batch train task did not produce a structured public outcome")
		}
		trajectories = append(trajectories, result.Value)
	}

	coord := diagnosisfanout.NewCoordinator(runtime.DiagnosisWorker, runtime.DiagnosisAdmitter)
	if coord.CanStartConsolidation() {
		return nil, fmt.Errorf("diagnosis fan-out opened consolidation before any job was scheduled")
	}
	diagnosisResults, err := coord.Run(ctx, trajectories, parallelism)
	if err != nil {
		return nil, fmt.Errorf("diagnosis fan-out: %w", err)
	}
	if !coord.CanStartConsolidation() {
		return nil, fmt.Errorf("diagnosis fan-out blocked consolidation after the terminal barrier")
	}
	runtime.Probe.event("reduce-diagnosis")

	proposals := make([]graphBatchCanonicalProposal, 0, len(diagnosisResults))
	proposalIDs := make([]string, 0, len(diagnosisResults))
	for _, job := range diagnosisResults {
		if job.Proposal == nil {
			continue
		}
		if err := rejectGraphBatchNonCanonicalID(job.Proposal.ProposalID); err != nil {
			return nil, err
		}
		proposals = append(proposals, graphBatchCanonicalProposal{
			ProposalID:    job.Proposal.ProposalID,
			ContentDigest: job.Proposal.ContentDigest,
			TrajectoryID:  job.TrajectoryID,
		})
		proposalIDs = append(proposalIDs, job.Proposal.ProposalID)
	}

	if runtime.Consolidator.Calls() != 0 {
		return nil, fmt.Errorf("consolidation ran before the diagnosis barrier")
	}
	consolidation, err := runtime.Consolidator.Consolidate(ctx, graphBatchConsolidateRequest{FamilyProposalIDs: proposalIDs})
	if err != nil {
		return nil, err
	}
	if runtime.Consolidator.Calls() != 1 {
		return nil, fmt.Errorf("consolidation calls = %d; want exactly 1", runtime.Consolidator.Calls())
	}
	runtime.Probe.event("consolidate")

	digest, err := runtime.Freezer.Freeze(ctx, graphBatchFreezePins(proposalIDs, consolidation, runtime.Config))
	if err != nil {
		runtime.Probe.event("freeze-failed")
		return &graphBatchPipelineResult{
			Strategy:               graphBatchStrategyID,
			FrozenConfig:           runtime.Config,
			TrainPeak:              trainPeak.Load(),
			DiagnosisPeak:          coord.ConcurrentPeak,
			ConsolidationCalls:     runtime.Consolidator.Calls(),
			ConsolidationDecisions: consolidation.Decisions,
			TestsStarted:           runtime.Probe.startedTests(),
			Authority:              graphBatchAuthority(runtime.AuthorityKind, proposalIDs),
		}, err
	}
	binding, err := runtime.Freezer.TestBinding()
	if err != nil {
		return nil, err
	}
	if binding != digest {
		return nil, fmt.Errorf("test binding %q does not match sealed digest %q", binding, digest)
	}
	runtime.Probe.event("freeze")

	testJobs := make([]parallelBatchJob[graphBatchHeldOutSpec], 0, len(runtime.HeldOut))
	for _, spec := range runtime.HeldOut {
		testJobs = append(testJobs, parallelBatchJob[graphBatchHeldOutSpec]{Sequence: spec.Sequence, Value: spec})
	}

	var (
		traceMu   sync.Mutex
		traces    []graphBatchHeldOutTrace
		attempts  []graphBatchTestAttempt
		accounting graphBatchFailureAccounting
	)

	testResults := runParallelBatchStage(ctx, sortedBatchJobs(testJobs), parallelism, func(ctx context.Context, job parallelBatchJob[graphBatchHeldOutSpec]) error {
		if runtime.Probe != nil {
			atomic.AddInt32(&runtime.Probe.testsStarted, 1)
			if runtime.Probe.testStarted != nil {
				runtime.Probe.testStarted <- job.Value.AttemptID
			}
		}
		if err := runtime.Freezer.RecordTestAttempt(job.Value.AttemptID); err != nil {
			return err
		}
		traceMu.Lock()
		snapshot := append([]graphBatchHeldOutTrace(nil), traces...)
		traceMu.Unlock()
		retrieval, err := runtime.Retriever.Retrieve(job.Value.AttemptID, digest, snapshot)
		if err != nil {
			return err
		}
		if retrieval.ManifestDigest != digest {
			return fmt.Errorf("held-out %s retrieval digest %q != sealed %q", job.Value.AttemptID, retrieval.ManifestDigest, digest)
		}

		task := concurrentepisode.NewScriptedSession("task-" + job.Value.AttemptID)
		task.Hold = job.Value.TaskHold
		memory := concurrentepisode.NewScriptedSession("memory-" + job.Value.AttemptID)
		memory.Hold = job.Value.MemoryHold
		memory.Timeout = job.Value.MemoryTimeout
		memory.Err = job.Value.MemoryErr
		episode, err := concurrentepisode.Run(ctx, concurrentepisode.Request{
			Opening:                job.Value.Opening,
			Graph:                  job.Value.Graph,
			ApplicableCandidateIDs: job.Value.ApplicableIDs,
			Task:                   task,
			Memory:                 memory,
			TaskContextMonitoring:  concurrentepisode.ContextOpeningOnly,
		})
		if err != nil {
			return err
		}

		trace := graphBatchHeldOutTrace{
			AttemptID:      job.Value.AttemptID,
			ManifestDigest: digest,
			Feedback:       append([]graphBatchTestFeedback(nil), job.Value.Feedback...),
		}
		attempt := graphBatchTestAttempt{
			AttemptID:      job.Value.AttemptID,
			ManifestDigest: digest,
			Retrieval:      retrieval,
			MemoryTerminal: episode.MemoryTerminal,
			TaskStatus:     episode.TaskStatus,
			FailureClass:   episode.FailureClass,
		}
		traceMu.Lock()
		traces = append(traces, trace)
		attempts = append(attempts, attempt)
		switch episode.FailureClass {
		case concurrentepisode.FailureNormalNoSkill:
			accounting.NormalNoSkillEpisodes++
		case concurrentepisode.FailureMemoryTimeout:
			accounting.MemoryTimeouts++
		case concurrentepisode.FailureMemoryError:
			accounting.MemoryErrors++
		}
		traceMu.Unlock()
		return nil
	})
	runtime.Probe.event("held-out")

	for _, result := range testResults {
		if result.Value != nil {
			return nil, result.Value
		}
	}
	sort.SliceStable(attempts, func(i, j int) bool { return attempts[i].AttemptID < attempts[j].AttemptID })
	sort.SliceStable(traces, func(i, j int) bool { return traces[i].AttemptID < traces[j].AttemptID })

	return &graphBatchPipelineResult{
		Strategy:               graphBatchStrategyID,
		FrozenConfig:           runtime.Config,
		TrainPeak:              trainPeak.Load(),
		DiagnosisPeak:          coord.ConcurrentPeak,
		ConsolidationCalls:     runtime.Consolidator.Calls(),
		ConsolidationDecisions: consolidation.Decisions,
		ManifestDigest:         digest,
		TestsStarted:           runtime.Probe.startedTests(),
		TestAttempts:           attempts,
		HeldOutTraces:          traces,
		FailureAccounting:      accounting,
		Authority:              graphBatchAuthority(runtime.AuthorityKind, proposalIDs),
	}, nil
}

func graphBatchInterruptPolicy(config graphBatchFrozenConfig) (effectsinterrupt.Policy, error) {
	interrupt, err := time.ParseDuration(config.MentionInterruptGrace)
	if err != nil {
		return effectsinterrupt.Policy{}, fmt.Errorf("mention_interrupt_grace: %w", err)
	}
	kill, err := time.ParseDuration(config.MentionKillGrace)
	if err != nil {
		return effectsinterrupt.Policy{}, fmt.Errorf("mention_kill_grace: %w", err)
	}
	return effectsinterrupt.Policy{InterruptGrace: interrupt, KillGrace: kill}, nil
}

func graphBatchAuthority(kind string, proposalIDs []string) graphBatchAuthorityRecord {
	if kind == "" {
		kind = graphBatchAuthorityFixture
	}
	return graphBatchAuthorityRecord{
		Kind:                         kind,
		UsedRunnerLocalSkillProposal: false,
		UsedMarkdownHashPrefix:       false,
		ProposalIDs:                  append([]string(nil), proposalIDs...),
	}
}

func graphBatchFreezePins(proposalIDs []string, ledger graphBatchConsolidateResult, config graphBatchFrozenConfig) graphBatchFreezeRequest {
	evidence := append([]string(nil), proposalIDs...)
	sort.Strings(evidence)
	watermark := canonicalDigest(append([]string{"evidence"}, evidence...))
	graph := canonicalDigest(append([]string{"graph", ledger.LedgerDigest}, evidence...))
	return graphBatchFreezeRequest{
		ProposalIDs:         append([]string(nil), proposalIDs...),
		Ledger:              ledger,
		EvidenceBatches:     evidence,
		EvidenceWatermark:   watermark,
		GraphDigest:         graph,
		GraphWatermark:      graph,
		PromptDigest:        canonicalDigest([]string{"prompt", config.Strategy}),
		SchemaDigest:        canonicalDigest([]string{"schema", graphBatchStrategyID}),
		ModelDigest:         canonicalDigest([]string{"model", config.Strategy}),
		ToolDigest:          canonicalDigest([]string{"tool", config.Strategy}),
		ConfigDigest:        canonicalDigest([]string{"config", config.Strategy, config.InitialSkillSnapshot}),
		GradingPolicyDigest: canonicalDigest([]string{"grading", config.Strategy}),
	}
}

func rejectGraphBatchNonCanonicalID(id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("graph batch authority requires a complete proposal id")
	}
	if strings.HasPrefix(id, "sha256:") || graphBatchPartialDigestRE.MatchString(id) || graphBatchShortHexRE.MatchString(id) {
		return fmt.Errorf("graph batch refuses hash-prefix authority %q", id)
	}
	if strings.Contains(id, ".md") || strings.HasPrefix(id, "#") || strings.Contains(id, "CONSOLIDATED SKILL") {
		return fmt.Errorf("graph batch refuses Markdown ledger authority %q", id)
	}
	return nil
}

// memoryGraphBatchLedger is a test-only consolidator/freezer/retriever.
// It is not GMS authority. Production composition uses gmsGraphBatchLedger.
type memoryGraphBatchLedger struct {
	mu sync.Mutex

	calls        int
	decisions    []graphBatchConsolidationDecision
	freezeErr    error
	sealedDigest string
	attempts     []string
	keys         []string
}

func newMemoryGraphBatchLedger() *memoryGraphBatchLedger {
	return &memoryGraphBatchLedger{}
}

func (m *memoryGraphBatchLedger) FailFreeze(err error) { m.freezeErr = err }

func (m *memoryGraphBatchLedger) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *memoryGraphBatchLedger) Consolidate(_ context.Context, req graphBatchConsolidateRequest) (graphBatchConsolidateResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if len(req.FamilyProposalIDs) == 0 {
		return graphBatchConsolidateResult{}, fmt.Errorf("graph batch consolidation requires complete family proposal ids")
	}
	for _, id := range req.FamilyProposalIDs {
		if err := rejectGraphBatchNonCanonicalID(id); err != nil {
			return graphBatchConsolidateResult{}, err
		}
	}
	ids := append([]string(nil), req.FamilyProposalIDs...)
	sort.Strings(ids)
	decision := graphBatchConsolidationDecision{
		DecisionID:        "decision-retain-" + ids[0],
		Operation:         "retain",
		SourceProposalIDs: ids,
	}
	m.decisions = []graphBatchConsolidationDecision{decision}
	digest := canonicalDigest(ids)
	m.keys = append([]string(nil), ids...)
	return graphBatchConsolidateResult{
		LedgerRevision: 1,
		LedgerDigest:   digest,
		Decisions:      []graphBatchConsolidationDecision{decision},
	}, nil
}

func (m *memoryGraphBatchLedger) Freeze(_ context.Context, req graphBatchFreezeRequest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.freezeErr != nil {
		return "", m.freezeErr
	}
	if m.calls != 1 {
		return "", fmt.Errorf("unified freeze requires exactly one consolidation; got %d", m.calls)
	}
	if req.Ledger.LedgerDigest == "" || len(req.Ledger.Decisions) != 1 {
		return "", fmt.Errorf("unified freeze requires a single canonical consolidation decision")
	}
	if len(req.EvidenceBatches) == 0 || req.EvidenceWatermark == "" || req.GraphDigest == "" {
		return "", fmt.Errorf("unified freeze requires evidence cut, ledger, and graph pins")
	}
	if req.PromptDigest == "" || req.SchemaDigest == "" || req.ModelDigest == "" || req.ToolDigest == "" || req.ConfigDigest == "" || req.GradingPolicyDigest == "" {
		return "", fmt.Errorf("unified freeze requires prompt/schema/model/tool/config/grading policy pins")
	}
	for _, id := range req.ProposalIDs {
		if err := rejectGraphBatchNonCanonicalID(id); err != nil {
			return "", err
		}
	}
	digest := canonicalDigest([]string{
		strings.Join(req.ProposalIDs, ","),
		req.Ledger.LedgerDigest,
		req.EvidenceWatermark,
		req.GraphDigest,
		req.PromptDigest, req.SchemaDigest, req.ModelDigest,
		req.ToolDigest, req.ConfigDigest, req.GradingPolicyDigest,
	})
	m.sealedDigest = digest
	return digest, nil
}

func (m *memoryGraphBatchLedger) TestBinding() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealedDigest == "" {
		return "", fmt.Errorf("evaluation freeze: no sealed manifest; tests cannot start")
	}
	return m.sealedDigest, nil
}

func (m *memoryGraphBatchLedger) RecordTestAttempt(attemptID string) error {
	if strings.TrimSpace(attemptID) == "" {
		return fmt.Errorf("graph batch test attempt id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealedDigest == "" {
		return fmt.Errorf("evaluation freeze: no sealed manifest; tests cannot start")
	}
	m.attempts = append(m.attempts, attemptID)
	return nil
}

func (m *memoryGraphBatchLedger) Attempts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.attempts...)
}

func (m *memoryGraphBatchLedger) Retrieve(_ string, manifestDigest string, _ []graphBatchHeldOutTrace) (graphBatchRetrieval, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealedDigest == "" || manifestDigest != m.sealedDigest {
		return graphBatchRetrieval{}, fmt.Errorf("held-out retrieval is pinned to the sealed manifest")
	}
	return graphBatchRetrieval{
		ManifestDigest: m.sealedDigest,
		Keys:           append([]string(nil), m.keys...),
	}, nil
}

func canonicalDigest(parts []string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// scriptedTrainExecutor runs the generation-0 train task session (empty
// Skill graph) and refuses to return a trajectory without a public outcome.
type scriptedTrainExecutor struct{}

func (scriptedTrainExecutor) Execute(ctx context.Context, spec graphBatchTrainSpec) (diagnosisfanout.Trajectory, error) {
	if spec.Hold != nil {
		select {
		case <-spec.Hold:
		case <-ctx.Done():
			return diagnosisfanout.Trajectory{}, ctx.Err()
		}
	}
	task := concurrentepisode.NewScriptedSession("train-task-" + spec.Trajectory.ID)
	memory := concurrentepisode.NewScriptedSession("train-memory-" + spec.Trajectory.ID)
	episode, err := concurrentepisode.Run(ctx, concurrentepisode.Request{
		Opening: concurrentepisode.OpeningContext{Messages: []concurrentepisode.OpeningMessage{{
			ID: "train-open-" + spec.Trajectory.ID, Author: "user", Content: "train " + spec.Trajectory.ID,
		}}},
		Task:                  task,
		Memory:                memory,
		TaskContextMonitoring: concurrentepisode.ContextOpeningOnly,
	})
	if err != nil {
		return diagnosisfanout.Trajectory{}, err
	}
	traj := spec.Trajectory
	if traj.Outcome.Status == "" {
		traj.Outcome = publicOutcomeFromTaskStatus(episode.TaskStatus)
	}
	if traj.Outcome.Status == "" {
		return diagnosisfanout.Trajectory{}, fmt.Errorf("train task produced no structured public outcome")
	}
	traj.CompleteTrajectory = append(append([]string(nil), traj.CompleteTrajectory...), "train-executed:"+traj.Outcome.CanonicalString())
	return traj, nil
}

func publicOutcomeFromTaskStatus(status string) diagnosisfanout.PublicOutcome {
	switch status {
	case "completed", "pass":
		return diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusPass}
	case "timeout":
		return diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusTimeout}
	case "no-output", "no_output":
		return diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusNoOutput}
	case "protocol-error", "protocol_error":
		return diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusProtocolError}
	default:
		if status == "" {
			return diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusFail, Cause: diagnosisfanout.CauseUnknown}
		}
		return diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusFail, RuntimeError: status}
	}
}
