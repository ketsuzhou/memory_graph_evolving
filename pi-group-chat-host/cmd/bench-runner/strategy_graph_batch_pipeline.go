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

	HeldOut []graphBatchHeldOutSpec
	Probe   *graphBatchProbe
}

type graphBatchTrainSpec struct {
	Sequence   int
	Trajectory diagnosisfanout.Trajectory
	Hold       <-chan struct{}
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

type graphBatchFreezer interface {
	Freeze(ctx context.Context, proposalIDs []string, ledger graphBatchConsolidateResult) (string, error)
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
	return fmt.Errorf("%s refuses the legacy []skillProposal pipeline; supply a composed graph-batch runtime", graphBatchStrategyID)
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
		if job.Value.Hold != nil {
			select {
			case <-job.Value.Hold:
			case <-ctx.Done():
				trainInflight.Add(-1)
				return job.Value.Trajectory
			}
		}
		trainInflight.Add(-1)
		return job.Value.Trajectory
	})
	runtime.Probe.event("reduce-train")

	trajectories := make([]diagnosisfanout.Trajectory, 0, len(trainResults))
	for _, result := range trainResults {
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

	digest, err := runtime.Freezer.Freeze(ctx, proposalIDs, consolidation)
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
			Authority:              graphBatchAuthority(proposalIDs),
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
		Authority:              graphBatchAuthority(proposalIDs),
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

func graphBatchAuthority(proposalIDs []string) graphBatchAuthorityRecord {
	return graphBatchAuthorityRecord{
		Kind:                         "gms_canonical",
		UsedRunnerLocalSkillProposal: false,
		UsedMarkdownHashPrefix:       false,
		ProposalIDs:                  append([]string(nil), proposalIDs...),
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

// memoryGraphBatchLedger is the fixture consolidator/freezer/retriever. It
// seals one digest from complete proposal IDs and ignores held-out traces
// during retrieval. Production GMS services bind the same ports.
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

func (m *memoryGraphBatchLedger) Freeze(_ context.Context, proposalIDs []string, ledger graphBatchConsolidateResult) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.freezeErr != nil {
		return "", m.freezeErr
	}
	if m.calls != 1 {
		return "", fmt.Errorf("unified freeze requires exactly one consolidation; got %d", m.calls)
	}
	if ledger.LedgerDigest == "" || len(ledger.Decisions) != 1 {
		return "", fmt.Errorf("unified freeze requires a single canonical consolidation decision")
	}
	for _, id := range proposalIDs {
		if err := rejectGraphBatchNonCanonicalID(id); err != nil {
			return "", err
		}
	}
	digest := canonicalDigest(append(append([]string(nil), proposalIDs...), ledger.LedgerDigest))
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
