package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	"river2.dev/pi-group-chat-host/internal/diagnosisfanout"
)

func TestGraphBatchCanonicalPipelineStageBarriersSingleConsolidationAndHeldOutParallelism(t *testing.T) {
	trains := []string{"traj-a", "traj-b", "traj-c"}
	trainHold := map[string]chan struct{}{
		"traj-a": make(chan struct{}),
		"traj-b": make(chan struct{}),
		"traj-c": make(chan struct{}),
	}
	diagHold := map[string]chan struct{}{
		"traj-a": make(chan struct{}),
		"traj-b": make(chan struct{}),
		"traj-c": make(chan struct{}),
	}
	testHold := map[string]chan struct{}{
		"test-a": make(chan struct{}),
		"test-b": make(chan struct{}),
	}
	probe := newGraphBatchProbe(len(trains), len(trains), 2)
	ledger := newMemoryGraphBatchLedger()
	runtime := newGraphBatchFixtureRuntime(t, trains, trainHold, diagHold, probe, ledger, []graphBatchHeldOutSpec{
		heldOutSpec("test-a", 10, testHold["test-a"], nil),
		heldOutSpec("test-b", 11, testHold["test-b"], nil),
	})

	done := make(chan resultOrErr, 1)
	go func() {
		result, err := runGraphBatchCanonicalPipeline(context.Background(), runtime)
		done <- resultOrErr{result: result, err: err}
	}()

	awaitIntStarts(t, probe.trainStarted, len(trains))
	if events := probe.snapshot(); len(events) != 0 || ledger.Calls() != 0 {
		t.Fatalf("later phases ran during train barrier: events=%v consolidations=%d", events, ledger.Calls())
	}
	close(trainHold["traj-c"])
	close(trainHold["traj-a"])
	close(trainHold["traj-b"])

	awaitStringStarts(t, probe.diagnosisStarted, trains)
	if events := probe.snapshot(); !reflect.DeepEqual(events, []string{"reduce-train"}) || ledger.Calls() != 0 {
		t.Fatalf("consolidation crossed the diagnosis barrier: events=%v consolidations=%d", events, ledger.Calls())
	}
	close(diagHold["traj-b"])
	close(diagHold["traj-c"])
	close(diagHold["traj-a"])

	awaitStringStarts(t, probe.testStarted, []string{"test-a", "test-b"})
	if events := probe.snapshot(); !reflect.DeepEqual(events, []string{"reduce-train", "reduce-diagnosis", "consolidate", "freeze"}) {
		t.Fatalf("phase order before held-out = %v", events)
	}
	if ledger.Calls() != 1 {
		t.Fatalf("consolidation calls = %d; want exactly 1", ledger.Calls())
	}
	close(testHold["test-b"])
	close(testHold["test-a"])

	got := waitPipeline(t, done)
	if got.err != nil {
		t.Fatalf("pipeline: %v", got.err)
	}
	if got.result.TrainPeak < 2 {
		t.Fatalf("train peak = %d; want a parallel barrier", got.result.TrainPeak)
	}
	if got.result.DiagnosisPeak < 2 {
		t.Fatalf("diagnosis peak = %d; want a parallel barrier", got.result.DiagnosisPeak)
	}
	if got.result.ConsolidationCalls != 1 || len(got.result.ConsolidationDecisions) != 1 {
		t.Fatalf("want a single consolidation, got calls=%d decisions=%d", got.result.ConsolidationCalls, len(got.result.ConsolidationDecisions))
	}
	if got.result.TestsStarted != 2 || len(got.result.TestAttempts) != 2 {
		t.Fatalf("held-out attempts = started %d recorded %d; want 2", got.result.TestsStarted, len(got.result.TestAttempts))
	}
	if !strings.HasPrefix(got.result.ManifestDigest, "sha256:") {
		t.Fatalf("manifest digest is not sealed: %q", got.result.ManifestDigest)
	}
}

func TestGraphBatchPipelineRefusesRunnerLocalSkillProposalAndHashPrefixAuthority(t *testing.T) {
	ledger := newMemoryGraphBatchLedger()
	if _, err := ledger.Consolidate(context.Background(), graphBatchConsolidateRequest{
		FamilyProposalIDs: []string{"aaaa1111eeeeffff"},
	}); err == nil || !strings.Contains(err.Error(), "hash-prefix") {
		t.Fatalf("hash prefix was accepted as authority: %v", err)
	}
	if _, err := ledger.Consolidate(context.Background(), graphBatchConsolidateRequest{
		FamilyProposalIDs: []string{"sha256:abcdef"},
	}); err == nil || !strings.Contains(err.Error(), "hash-prefix") {
		t.Fatalf("partial digest was accepted as authority: %v", err)
	}
	if _, err := ledger.Consolidate(context.Background(), graphBatchConsolidateRequest{
		FamilyProposalIDs: []string{"skills.md"},
	}); err == nil || !strings.Contains(err.Error(), "Markdown") {
		t.Fatalf("Markdown ledger was accepted as authority: %v", err)
	}

	runtime := newGraphBatchFixtureRuntime(t, []string{"traj-a"}, nil, nil, newGraphBatchProbe(1, 1, 1), newMemoryGraphBatchLedger(), []graphBatchHeldOutSpec{
		heldOutSpec("test-a", 10, nil, nil),
	})
	result, err := runGraphBatchCanonicalPipeline(context.Background(), runtime)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if result.Authority.Kind != graphBatchAuthorityFixture || result.Authority.UsedRunnerLocalSkillProposal || result.Authority.UsedMarkdownHashPrefix {
		t.Fatalf("authority = %#v; want fixture_memory without runner-local ledger", result.Authority)
	}
	if len(result.Authority.ProposalIDs) == 0 {
		t.Fatal("canonical pipeline produced no proposal ids")
	}
	for _, id := range result.Authority.ProposalIDs {
		if err := rejectGraphBatchNonCanonicalID(id); err != nil {
			t.Fatalf("pipeline stored a non-canonical id %q: %v", id, err)
		}
	}
	if len(result.ConsolidationDecisions) != 1 {
		t.Fatalf("decisions = %d; want 1 covering the family", len(result.ConsolidationDecisions))
	}
	for _, source := range result.ConsolidationDecisions[0].SourceProposalIDs {
		if err := rejectGraphBatchNonCanonicalID(source); err != nil {
			t.Fatalf("consolidation source %q is not a complete id: %v", source, err)
		}
	}

	called := false
	err = runArm(context.Background(), armConfig{
		arm: graphBatchStrategyID,
		episodes: []manifestEpisode{{
			EpisodeID: "held-out-1", Split: "test", FamilyID: "family",
		}},
	}, func(attemptRecord) { called = true })
	if err == nil || !strings.Contains(err.Error(), "requires a GMS instance") {
		t.Fatalf("runArm error = %v; want a GMS composition error, not the legacy pipeline", err)
	}
	if called {
		t.Fatal("graph batch must not emit a legacy attemptRecord")
	}
}

func TestGraphBatchPipelineFreezeFailureStartsZeroTests(t *testing.T) {
	ledger := newMemoryGraphBatchLedger()
	ledger.FailFreeze(errors.New("evaluation freeze: moved head"))
	probe := newGraphBatchProbe(2, 2, 2)
	runtime := newGraphBatchFixtureRuntime(t, []string{"traj-a", "traj-b"}, nil, nil, probe, ledger, []graphBatchHeldOutSpec{
		heldOutSpec("test-a", 10, nil, nil),
		heldOutSpec("test-b", 11, nil, nil),
	})

	result, err := runGraphBatchCanonicalPipeline(context.Background(), runtime)
	if err == nil || !strings.Contains(err.Error(), "moved head") {
		t.Fatalf("freeze failure error = %v", err)
	}
	if result == nil {
		t.Fatal("failed freeze returned no pipeline result")
	}
	if result.TestsStarted != 0 || len(result.TestAttempts) != 0 || len(ledger.Attempts()) != 0 {
		t.Fatalf("freeze failure started tests: started=%d attempts=%d recorded=%d", result.TestsStarted, len(result.TestAttempts), len(ledger.Attempts()))
	}
	if result.ManifestDigest != "" {
		t.Fatalf("failed freeze sealed digest %q", result.ManifestDigest)
	}
	select {
	case id := <-probe.testStarted:
		t.Fatalf("held-out %s started after freeze failure", id)
	default:
	}
}

func TestGraphBatchPipelineTestFeedbackDoesNotChangeSiblingRetrievalAndSharesManifestDigest(t *testing.T) {
	ledger := newMemoryGraphBatchLedger()
	testAHold := make(chan struct{})
	probe := newGraphBatchProbe(2, 2, 2)
	runtime := newGraphBatchFixtureRuntime(t, []string{"traj-a", "traj-b"}, nil, nil, probe, ledger, []graphBatchHeldOutSpec{
		{
			Sequence:  10,
			AttemptID: "test-a",
			Opening:   openingFor("test-a"),
			Feedback: []graphBatchTestFeedback{{
				OfferID: "offer-a", Disposition: "rejected", ReasonCode: "too_generic", Reason: "too generic for test A",
			}},
			TaskHold:   testAHold,
			MemoryHold: testAHold,
		},
		heldOutSpec("test-b", 11, nil, nil),
	})

	done := make(chan resultOrErr, 1)
	go func() {
		result, err := runGraphBatchCanonicalPipeline(context.Background(), runtime)
		done <- resultOrErr{result: result, err: err}
	}()
	awaitStringStarts(t, probe.testStarted, []string{"test-a", "test-b"})
	close(testAHold)
	got := waitPipeline(t, done)
	if got.err != nil {
		t.Fatalf("pipeline: %v", got.err)
	}

	if got.result.ManifestDigest == "" {
		t.Fatal("missing sealed manifest digest")
	}
	if len(got.result.TestAttempts) != 2 || len(got.result.HeldOutTraces) != 2 {
		t.Fatalf("attempts=%d traces=%d; want 2/2", len(got.result.TestAttempts), len(got.result.HeldOutTraces))
	}
	byAttempt := map[string]graphBatchTestAttempt{}
	for _, attempt := range got.result.TestAttempts {
		byAttempt[attempt.AttemptID] = attempt
		if attempt.ManifestDigest != got.result.ManifestDigest {
			t.Fatalf("%s digest %q != sealed %q", attempt.AttemptID, attempt.ManifestDigest, got.result.ManifestDigest)
		}
	}
	if !reflect.DeepEqual(byAttempt["test-a"].Retrieval.Keys, byAttempt["test-b"].Retrieval.Keys) {
		t.Fatalf("retrieval keys differ: A=%v B=%v", byAttempt["test-a"].Retrieval.Keys, byAttempt["test-b"].Retrieval.Keys)
	}

	var testATrace *graphBatchHeldOutTrace
	for i := range got.result.HeldOutTraces {
		if got.result.HeldOutTraces[i].AttemptID == "test-a" {
			testATrace = &got.result.HeldOutTraces[i]
		}
		if got.result.HeldOutTraces[i].ManifestDigest != got.result.ManifestDigest {
			t.Fatalf("trace %s digest %q != sealed %q", got.result.HeldOutTraces[i].AttemptID, got.result.HeldOutTraces[i].ManifestDigest, got.result.ManifestDigest)
		}
	}
	if testATrace == nil || len(testATrace.Feedback) != 1 || testATrace.Feedback[0].ReasonCode != "too_generic" {
		t.Fatalf("test A feedback was not written to the held-out trace: %#v", got.result.HeldOutTraces)
	}
	replay, err := ledger.Retrieve("test-b", got.result.ManifestDigest, got.result.HeldOutTraces)
	if err != nil {
		t.Fatalf("replay retrieval: %v", err)
	}
	if !reflect.DeepEqual(replay.Keys, byAttempt["test-b"].Retrieval.Keys) {
		t.Fatalf("test B retrieval changed after test A feedback: before=%v after=%v", byAttempt["test-b"].Retrieval.Keys, replay.Keys)
	}
}

func TestGraphBatchPipelineNormalNoSkillContinuesAndClassifiesMechanismFailures(t *testing.T) {
	ledger := newMemoryGraphBatchLedger()
	runtime := newGraphBatchFixtureRuntime(t, []string{"traj-a"}, nil, nil, newGraphBatchProbe(1, 1, 2), ledger, []graphBatchHeldOutSpec{
		heldOutSpec("test-empty", 10, nil, nil),
		{
			Sequence:      11,
			AttemptID:     "test-timeout",
			Opening:       openingFor("test-timeout"),
			MemoryTimeout: true,
		},
	})
	result, err := runGraphBatchCanonicalPipeline(context.Background(), runtime)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	byID := map[string]graphBatchTestAttempt{}
	for _, attempt := range result.TestAttempts {
		byID[attempt.AttemptID] = attempt
		if attempt.TaskStatus != concurrentepisode.TaskStatusCompleted {
			t.Fatalf("%s task status = %q; want task to continue", attempt.AttemptID, attempt.TaskStatus)
		}
	}
	if byID["test-empty"].MemoryTerminal != concurrentepisode.TerminalNoCandidateFromEmptyGraph || byID["test-empty"].FailureClass != concurrentepisode.FailureNormalNoSkill {
		t.Fatalf("empty graph = %#v; want normal no-skill", byID["test-empty"])
	}
	if byID["test-timeout"].MemoryTerminal != concurrentepisode.TerminalMemoryTimeout || byID["test-timeout"].FailureClass != concurrentepisode.FailureMemoryTimeout {
		t.Fatalf("timeout = %#v; want classified mechanism failure", byID["test-timeout"])
	}
	if result.FailureAccounting.NormalNoSkillEpisodes != 1 || result.FailureAccounting.MemoryTimeouts != 1 || result.FailureAccounting.MemoryErrors != 0 || result.FailureAccounting.ProtocolErrors != 0 {
		t.Fatalf("failure accounting = %#v", result.FailureAccounting)
	}
}

func TestGraphBatchRunArmUsesComposedPipelineNotLegacySkillProposal(t *testing.T) {
	called := false
	err := runArm(context.Background(), armConfig{arm: graphBatchStrategyID}, func(attemptRecord) {
		called = true
	})
	if err != nil {
		t.Fatalf("empty composed graph-batch runArm = %v", err)
	}
	if called {
		t.Fatal("composed graph batch must not emit a legacy attempt record")
	}

	ledger := newMemoryGraphBatchLedger()
	runtime := newGraphBatchFixtureRuntime(t, []string{"traj-a"}, nil, nil, newGraphBatchProbe(1, 1, 1), ledger, []graphBatchHeldOutSpec{
		heldOutSpec("test-a", 10, nil, nil),
	})
	err = runArm(context.Background(), armConfig{arm: graphBatchStrategyID, graphBatch: runtime}, func(attemptRecord) {
		called = true
	})
	if err != nil {
		t.Fatalf("composed runArm: %v", err)
	}
	if called {
		t.Fatal("composed graph batch must not emit a legacy attempt record")
	}
}

func TestLegacyWarmSkillBatchCoordinatorRejectsGraphBatchArm(t *testing.T) {
	err := runParallelSkillStrategyBatch(context.Background(), armConfig{arm: graphBatchStrategyID}, "tenant", t.TempDir(), "", nil, func(attemptRecord) {})
	if err == nil || !strings.Contains(err.Error(), "legacy warm-skill-batch coordinator") {
		t.Fatalf("legacy coordinator error = %v; want a graph-batch reject", err)
	}
}

func newGraphBatchFixtureRuntime(t *testing.T, trainIDs []string, trainHold, diagHold map[string]chan struct{}, probe *graphBatchProbe, ledger *memoryGraphBatchLedger, heldOut []graphBatchHeldOutSpec) *graphBatchRuntime {
	t.Helper()
	trains := make([]graphBatchTrainSpec, 0, len(trainIDs))
	drafts := make(map[string]diagnosisfanout.WorkerDraft, len(trainIDs))
	for i, id := range trainIDs {
		traj := pipelineTrajectory(i+1, id)
		spec := graphBatchTrainSpec{Sequence: i + 1, Trajectory: traj}
		if trainHold != nil {
			spec.Hold = trainHold[id]
		}
		trains = append(trains, spec)
		drafts[id] = diagnosisfanout.WorkerDraft{
			IdempotencyKey: "idem-" + id,
			ProposalBody:   map[string]any{"proposal_id": "raw-proposal-" + id + "-00001"},
		}
	}
	worker := &pipelineDiagnosisWorker{drafts: drafts, release: diagHold, started: nil}
	if probe != nil {
		worker.started = probe.diagnosisStarted
	}
	return &graphBatchRuntime{
		Config:            defaultGraphBatchFrozenConfig(),
		Parallelism:       len(trainIDs),
		Trains:            trains,
		TrainExecutor:     scriptedTrainExecutor{},
		AuthorityKind:     graphBatchAuthorityFixture,
		DiagnosisWorker:   worker,
		DiagnosisAdmitter: &pipelineAdmitter{},
		Consolidator:      ledger,
		Freezer:           ledger,
		Retriever:         ledger,
		HeldOut:           heldOut,
		Probe:             probe,
	}
}

func heldOutSpec(attemptID string, sequence int, hold <-chan struct{}, feedback []graphBatchTestFeedback) graphBatchHeldOutSpec {
	return graphBatchHeldOutSpec{
		Sequence:   sequence,
		AttemptID:  attemptID,
		Opening:    openingFor(attemptID),
		Feedback:   feedback,
		TaskHold:   hold,
		MemoryHold: hold,
	}
}

func openingFor(attemptID string) concurrentepisode.OpeningContext {
	return concurrentepisode.OpeningContext{Messages: []concurrentepisode.OpeningMessage{{
		ID: "open-" + attemptID, Author: "user", Content: "solve " + attemptID,
	}}}
}

func pipelineTrajectory(sequence int, id string) diagnosisfanout.Trajectory {
	return diagnosisfanout.Trajectory{
		Sequence:           sequence,
		ID:                 id,
		SnapshotID:         "snapshot-" + id,
		DiagnosisRunID:     "diagnosis-run-" + id,
		CompleteTrajectory: []string{"opening", "checkpoint", "outcome"},
		Outcome:            diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusFail, RuntimeError: "nil pointer"},
		Checkpoints:        map[string]diagnosisfanout.Checkpoint{"checkpoint-" + id: {ID: "checkpoint-" + id, SnapshotID: "snapshot-" + id}},
		Evidence: map[string]diagnosisfanout.Evidence{
			"evidence-" + id: {ID: "evidence-" + id, TrajectoryID: id, SnapshotID: "snapshot-" + id, ObservableFacts: []string{"observable " + id}},
		},
	}
}

type pipelineDiagnosisWorker struct {
	mu       sync.Mutex
	drafts   map[string]diagnosisfanout.WorkerDraft
	release  map[string]chan struct{}
	started  chan string
}

func (w *pipelineDiagnosisWorker) Diagnose(_ context.Context, input diagnosisfanout.DiagnosisInput) (diagnosisfanout.WorkerDraft, error) {
	if w.started != nil {
		w.started <- input.TrajectoryID
	}
	if ch := w.release[input.TrajectoryID]; ch != nil {
		<-ch
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	draft, ok := w.drafts[input.TrajectoryID]
	if !ok {
		return diagnosisfanout.WorkerDraft{}, fmt.Errorf("missing diagnosis draft for %s", input.TrajectoryID)
	}
	return draft, nil
}

type pipelineAdmitter struct {
	mu       sync.Mutex
	admitted []string
}

func (a *pipelineAdmitter) Admit(_ context.Context, request diagnosisfanout.AdmissionRequest) (diagnosisfanout.ProposalRef, error) {
	id := "raw-proposal-" + request.TrajectoryID + "-00001"
	if err := rejectGraphBatchNonCanonicalID(id); err != nil {
		return diagnosisfanout.ProposalRef{}, err
	}
	a.mu.Lock()
	a.admitted = append(a.admitted, id)
	a.mu.Unlock()
	return diagnosisfanout.ProposalRef{
		ProposalID:    id,
		ContentDigest: canonicalDigest([]string{id, request.IdempotencyKey}),
		URI:           "raw-skill-proposal://gms/" + id,
	}, nil
}

func (a *pipelineAdmitter) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.admitted)
}

func (a *pipelineAdmitter) Has(proposalID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, id := range a.admitted {
		if id == proposalID {
			return true
		}
	}
	return false
}

type resultOrErr struct {
	result *graphBatchPipelineResult
	err    error
}

func waitPipeline(t *testing.T, done <-chan resultOrErr) resultOrErr {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline did not complete")
		return resultOrErr{}
	}
}

func awaitIntStarts(t *testing.T, started <-chan int, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	seen := map[int]bool{}
	for len(seen) < want {
		select {
		case id := <-started:
			seen[id] = true
		case <-deadline:
			t.Fatalf("started %d/%d train jobs: %v", len(seen), want, seen)
		}
	}
}

func awaitStringStarts(t *testing.T, started <-chan string, want []string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	need := map[string]bool{}
	for _, id := range want {
		need[id] = true
	}
	seen := map[string]bool{}
	for len(seen) < len(need) {
		select {
		case id := <-started:
			seen[id] = true
		case <-deadline:
			t.Fatalf("started %v; want %v", keysOf(seen), want)
		}
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("missing start %s in %v", id, keysOf(seen))
		}
	}
}

func keysOf(seen map[string]bool) []string {
	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	return out
}
