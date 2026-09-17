package diagnosisfanout

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	rawadmit "river2.dev/graph-memory-service/testdata/rawadmit"
)

func TestFanOutNTrajectoriesProduceNTerminalJobsWithConcurrentBarrier(t *testing.T) {
	live := newLiveAdmitter(t)
	ids := []string{"traj-a", "traj-b", "traj-c"}
	trajectories := make([]Trajectory, 0, len(ids))
	drafts := make(map[string]WorkerDraft, len(ids))
	release := map[string]chan struct{}{}
	started := make(chan string, len(ids))
	for i, id := range ids {
		traj := fixtureTrajectory(t, i+1, id, PublicOutcome{Status: StatusFail, RuntimeError: "nil pointer"}, nil)
		registerFrozen(t, live.adapter, traj)
		body := validBody(t, traj)
		drafts[id] = WorkerDraft{IdempotencyKey: "idem-" + id, ProposalBody: body}
		release[id] = make(chan struct{})
		trajectories = append(trajectories, traj)
	}
	// Dispatch out of sequence; merge must still be 1,2,3.
	trajectories[0], trajectories[2] = trajectories[2], trajectories[0]

	worker := &stubWorker{started: started, release: release, drafts: drafts}
	coord := NewCoordinator(worker, live)
	if coord.CanStartConsolidation() {
		t.Fatal("consolidation gate opened before any job was scheduled")
	}

	done := make(chan []JobResult, 1)
	go func() {
		results, err := coord.Run(context.Background(), trajectories, len(trajectories))
		if err != nil {
			t.Errorf("fan-out: %v", err)
		}
		done <- results
	}()

	awaitStarts(t, started, len(ids))
	if coord.CanStartConsolidation() {
		t.Fatal("consolidation started while diagnosis jobs were still in flight")
	}
	if got := live.Count(); got != 0 {
		t.Fatalf("Admit ran before the concurrent barrier released: count=%d", got)
	}
	close(release["traj-c"])
	close(release["traj-a"])
	close(release["traj-b"])

	var results []JobResult
	select {
	case results = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fan-out did not reach the diagnosis barrier")
	}
	if !coord.CanStartConsolidation() {
		t.Fatal("consolidation stayed blocked after every job was terminal")
	}
	if coord.ConcurrentPeak != int32(len(ids)) {
		t.Fatalf("concurrent peak = %d, want %d in-flight jobs at the barrier", coord.ConcurrentPeak, len(ids))
	}
	if len(results) != len(ids) {
		t.Fatalf("terminal jobs = %d, want %d", len(results), len(ids))
	}
	for i, result := range results {
		if result.Sequence != i+1 || !result.Terminal || result.Status != JobSucceeded || result.Proposal == nil {
			t.Fatalf("result[%d] = %#v, want sequence %d succeeded with a canonical proposal", i, result, i+1)
		}
		if !live.Has(result.Proposal.ProposalID) {
			t.Fatalf("proposal %q was not admitted by rawproposal.Admit", result.Proposal.ProposalID)
		}
	}
	if live.Count() != len(ids) {
		t.Fatalf("canonical proposals = %d, want %d", live.Count(), len(ids))
	}
}

func TestDiagnosisInputIncludesPublicOutcomesAndErrors(t *testing.T) {
	live := newLiveAdmitter(t)
	cases := []struct {
		id      string
		outcome PublicOutcome
	}{
		{"traj-pass", PublicOutcome{Status: StatusPass}},
		{"traj-fail", PublicOutcome{Status: StatusFail, RuntimeError: "index out of range"}},
		{"traj-timeout", PublicOutcome{Status: StatusTimeout}},
		{"traj-no-output", PublicOutcome{Status: StatusNoOutput}},
		{"traj-protocol", PublicOutcome{Status: StatusProtocolError}},
		{"traj-compile", PublicOutcome{Status: StatusFail, CompileError: "undefined: Foo"}},
		{"traj-runtime", PublicOutcome{Status: StatusFail, RuntimeError: "segmentation fault"}},
	}
	trajectories := make([]Trajectory, 0, len(cases))
	drafts := make(map[string]WorkerDraft, len(cases))
	for i, tc := range cases {
		traj := fixtureTrajectory(t, i+1, tc.id, tc.outcome, nil)
		traj.HiddenTestInput = "hidden test secret cases"
		traj.GoldSolution = "gold solution 42"
		traj.ReferenceSolution = "reference solution impl"
		registerFrozen(t, live.adapter, traj)
		drafts[tc.id] = WorkerDraft{IdempotencyKey: "idem-" + tc.id, ProposalBody: validBody(t, traj)}
		trajectories = append(trajectories, traj)
	}
	worker := &stubWorker{drafts: drafts}
	coord := NewCoordinator(worker, live)
	if _, err := coord.Run(context.Background(), trajectories, 3); err != nil {
		t.Fatalf("fan-out: %v", err)
	}
	if len(worker.inputs) != len(cases) {
		t.Fatalf("worker inputs = %d, want %d", len(worker.inputs), len(cases))
	}
	seen := map[string]DiagnosisInput{}
	for _, input := range worker.inputs {
		seen[input.TrajectoryID] = input
		raw, err := PublicJSON(input)
		if err != nil {
			t.Fatalf("marshal diagnosis input: %v", err)
		}
		if ContainsProhibitedLeak(string(raw)) {
			t.Fatalf("diagnosis input leaked hidden/gold material: %s", raw)
		}
		if input.PublicOutcome.Status == "" || len(input.CompleteTrajectory) == 0 {
			t.Fatalf("incomplete public diagnosis input: %#v", input)
		}
	}
	required := []string{StatusPass, StatusFail, StatusTimeout, StatusNoOutput, StatusProtocolError}
	for _, status := range required {
		found := false
		for _, input := range worker.inputs {
			if input.PublicOutcome.Status == status {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("public outcome %q did not enter diagnosis input", status)
		}
	}
	if seen["traj-compile"].PublicOutcome.CompileError != "undefined: Foo" {
		t.Fatalf("public compile error missing: %#v", seen["traj-compile"].PublicOutcome)
	}
	if seen["traj-runtime"].PublicOutcome.RuntimeError != "segmentation fault" {
		t.Fatalf("public runtime error missing: %#v", seen["traj-runtime"].PublicOutcome)
	}
}

func TestBinaryOnlyFailureUsesCauseUnknownWithoutInventedRootCause(t *testing.T) {
	allowed := map[string]bool{"Status": true, "CompileError": true, "RuntimeError": true, "Cause": true}
	typ := reflect.TypeOf(PublicOutcome{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if !allowed[name] {
			t.Fatalf("PublicOutcome has fictional root-cause field %s", name)
		}
	}

	live := newLiveAdmitter(t)
	traj := fixtureTrajectory(t, 1, "traj-binary", PublicOutcome{Status: StatusFail}, nil)
	registerFrozen(t, live.adapter, traj)
	worker := &stubWorker{drafts: map[string]WorkerDraft{
		traj.ID: {IdempotencyKey: "idem-" + traj.ID, ProposalBody: validBody(t, traj)},
	}}
	coord := NewCoordinator(worker, live)
	if _, err := coord.Run(context.Background(), []Trajectory{traj}, 1); err != nil {
		t.Fatalf("fan-out: %v", err)
	}
	if len(worker.inputs) != 1 {
		t.Fatalf("worker inputs = %d, want 1", len(worker.inputs))
	}
	got := worker.inputs[0].PublicOutcome
	if got.Cause != CauseUnknown || got.CompileError != "" || got.RuntimeError != "" {
		t.Fatalf("binary-only outcome = %#v, want cause=unknown and no public error text", got)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var dumped map[string]any
	if err := json.Unmarshal(raw, &dumped); err != nil {
		t.Fatal(err)
	}
	if dumped["cause"] != CauseUnknown {
		t.Fatalf("json cause = %v, want %q", dumped["cause"], CauseUnknown)
	}
	for _, fictional := range []string{"root_cause", "suspected_bug", "hidden_reason", "gold_hint"} {
		if _, ok := dumped[fictional]; ok {
			t.Fatalf("binary-only outcome invented field %q: %s", fictional, raw)
		}
	}
}

func TestServedExposureVerdictExactlyOnceAndUnservedOffersHaveNone(t *testing.T) {
	live := newLiveAdmitter(t)
	signals := []InteractionSignal{
		{OfferID: "offer-served-accepted", Stage: StageOffered, BodyDigest: "sha256:aa"},
		{OfferID: "offer-served-accepted", Stage: StageServed, BodyDigest: "sha256:aa"},
		{OfferID: "offer-served-accepted", Stage: StageAccepted, BodyDigest: "sha256:aa"},
		{OfferID: "offer-served-rejected", Stage: StageServed, BodyDigest: "sha256:bb"},
		{OfferID: "offer-served-rejected", Stage: StageRejected, BodyDigest: "sha256:bb", ReasonCode: "not_applicable"},
		{OfferID: "offer-never-served", Stage: StageOffered, BodyDigest: "sha256:cc"},
	}
	traj := fixtureTrajectory(t, 1, "traj-exposures", PublicOutcome{Status: StatusFail, RuntimeError: "boom"}, signals)
	registerFrozen(t, live.adapter, traj)
	worker := &stubWorker{drafts: map[string]WorkerDraft{
		traj.ID: {
			IdempotencyKey: "idem-" + traj.ID,
			ProposalBody:   validBody(t, traj),
			Verdicts: []ExposureVerdict{
				{OfferID: "offer-served-accepted", Verdict: VerdictSupported},
				{OfferID: "offer-served-rejected", Verdict: VerdictRefuted},
				{OfferID: "offer-never-served", Verdict: VerdictInconclusive},
			},
		},
	}}
	coord := NewCoordinator(worker, live)
	results, err := coord.Run(context.Background(), []Trajectory{traj}, 1)
	if err != nil {
		t.Fatalf("fan-out: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("jobs = %d, want 1", len(results))
	}
	got := results[0].Verdicts
	if len(got) != 2 {
		t.Fatalf("verdicts = %#v, want exactly the two served exposures", got)
	}
	seen := map[string]string{}
	for _, verdict := range got {
		if seen[verdict.OfferID] != "" {
			t.Fatalf("duplicate verdict for %s", verdict.OfferID)
		}
		seen[verdict.OfferID] = verdict.Verdict
	}
	if seen["offer-served-accepted"] != VerdictSupported || seen["offer-served-rejected"] != VerdictRefuted {
		t.Fatalf("served verdicts = %#v", got)
	}
	if _, ok := seen["offer-never-served"]; ok {
		t.Fatalf("unserved offer produced an exposure verdict: %#v", got)
	}
	input := worker.inputs[0]
	if len(input.ServedExposures) != 2 {
		t.Fatalf("served exposures on input = %#v", input.ServedExposures)
	}
}

func TestFailedJobIsRecordedWithoutFakeProposalAndBlocksConsolidation(t *testing.T) {
	live := newLiveAdmitter(t)
	okA := fixtureTrajectory(t, 1, "traj-ok-a", PublicOutcome{Status: StatusPass}, nil)
	fail := fixtureTrajectory(t, 2, "traj-fail-job", PublicOutcome{Status: StatusFail, RuntimeError: "boom"}, nil)
	okB := fixtureTrajectory(t, 3, "traj-ok-b", PublicOutcome{Status: StatusTimeout}, nil)
	for _, traj := range []Trajectory{okA, fail, okB} {
		registerFrozen(t, live.adapter, traj)
	}
	release := map[string]chan struct{}{
		okA.ID:  make(chan struct{}),
		fail.ID: make(chan struct{}),
		okB.ID:  make(chan struct{}),
	}
	started := make(chan string, 3)
	worker := &stubWorker{
		started:        started,
		release:        release,
		failTrajectory: fail.ID,
		drafts: map[string]WorkerDraft{
			okA.ID: {IdempotencyKey: "idem-" + okA.ID, ProposalBody: validBody(t, okA)},
			okB.ID: {IdempotencyKey: "idem-" + okB.ID, ProposalBody: validBody(t, okB)},
		},
	}
	coord := NewCoordinator(worker, live)
	done := make(chan []JobResult, 1)
	go func() {
		results, err := coord.Run(context.Background(), []Trajectory{okA, fail, okB}, 3)
		if err != nil {
			t.Errorf("fan-out: %v", err)
		}
		done <- results
	}()
	awaitStarts(t, started, 3)
	if coord.CanStartConsolidation() {
		t.Fatal("consolidation started before every diagnosis job was terminal")
	}
	close(release[fail.ID])
	waitUntil(t, func() bool {
		for _, result := range coord.Results() {
			if result.TrajectoryID == fail.ID {
				return result.Terminal && result.Status == JobFailed
			}
		}
		return false
	})
	if coord.CanStartConsolidation() {
		t.Fatal("a single finished job released consolidation before the remaining jobs were terminal")
	}
	close(release[okA.ID])
	close(release[okB.ID])

	var results []JobResult
	select {
	case results = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fan-out did not finish")
	}
	if !coord.CanStartConsolidation() {
		t.Fatal("consolidation stayed blocked after all jobs were terminal")
	}
	if live.Count() != 2 {
		t.Fatalf("canonical proposals = %d, want 2 successful Admits", live.Count())
	}
	if contains(live.admitted, fail.ID) {
		t.Fatalf("failed job called Admit: %v", live.admitted)
	}
	byID := map[string]JobResult{}
	for _, result := range results {
		byID[result.TrajectoryID] = result
	}
	failed := byID[fail.ID]
	if !failed.Terminal || failed.Status != JobFailed || failed.Proposal != nil || failed.Failure == "" {
		t.Fatalf("failed job = %#v, want a recorded failure and no proposal", failed)
	}
	for _, id := range []string{okA.ID, okB.ID} {
		if byID[id].Status != JobSucceeded || byID[id].Proposal == nil {
			t.Fatalf("successful job %s = %#v", id, byID[id])
		}
	}
}

type liveAdmitter struct {
	adapter  *rawadmit.Adapter
	mu       sync.Mutex
	admitted []string
}

func newLiveAdmitter(t *testing.T) *liveAdmitter {
	t.Helper()
	adapter, err := rawadmit.New()
	if err != nil {
		t.Fatalf("rawproposal fixture: %v", err)
	}
	return &liveAdmitter{adapter: adapter}
}

func (l *liveAdmitter) Admit(ctx context.Context, request AdmissionRequest) (ProposalRef, error) {
	l.mu.Lock()
	l.admitted = append(l.admitted, request.TrajectoryID)
	l.mu.Unlock()
	id, digest, uri, err := l.adapter.Admit(ctx, request.IdempotencyKey, request.DiagnosisRunID, request.TrajectoryID, request.Body)
	if err != nil {
		return ProposalRef{}, err
	}
	return ProposalRef{ProposalID: id, ContentDigest: digest, URI: uri}, nil
}

func (l *liveAdmitter) Count() int { return l.adapter.Count() }

func (l *liveAdmitter) Has(proposalID string) bool { return l.adapter.Has(proposalID) }

type stubWorker struct {
	mu             sync.Mutex
	inputs         []DiagnosisInput
	started        chan string
	release        map[string]chan struct{}
	drafts         map[string]WorkerDraft
	failTrajectory string
}

func (w *stubWorker) Diagnose(_ context.Context, input DiagnosisInput) (WorkerDraft, error) {
	w.mu.Lock()
	w.inputs = append(w.inputs, input)
	w.mu.Unlock()
	if w.started != nil {
		w.started <- input.TrajectoryID
	}
	if ch := w.release[input.TrajectoryID]; ch != nil {
		<-ch
	}
	if w.failTrajectory != "" && input.TrajectoryID == w.failTrajectory {
		return WorkerDraft{}, errors.New("stub diagnosis worker failed")
	}
	draft, ok := w.drafts[input.TrajectoryID]
	if !ok {
		return WorkerDraft{}, errors.New("missing stub draft")
	}
	return draft, nil
}

func fixtureTrajectory(t *testing.T, sequence int, id string, outcome PublicOutcome, signals []InteractionSignal) Trajectory {
	t.Helper()
	if outcome.binaryOnlyFailure() {
		outcome.Cause = CauseUnknown
	}
	fact := "the relative path resolved outside the workspace on " + id
	return Trajectory{
		Sequence:           sequence,
		ID:                 id,
		SnapshotID:         "snapshot-" + id,
		DiagnosisRunID:     "diagnosis-run-" + id,
		CompleteTrajectory: []string{"opening", "checkpoint", "outcome"},
		Outcome:            outcome,
		Checkpoints:        map[string]Checkpoint{"checkpoint-" + id: {ID: "checkpoint-" + id, SnapshotID: "snapshot-" + id}},
		Evidence: map[string]Evidence{
			"evidence-" + id: {ID: "evidence-" + id, TrajectoryID: id, SnapshotID: "snapshot-" + id, ObservableFacts: []string{fact}},
		},
		Signals: signals,
	}
}

func registerFrozen(t *testing.T, adapter *rawadmit.Adapter, traj Trajectory) {
	t.Helper()
	adapter.Put(rawadmit.FrozenSpec{
		TrajectoryID:   traj.ID,
		SnapshotID:     traj.SnapshotID,
		DiagnosisRunID: traj.DiagnosisRunID,
		Steps:          traj.CompleteTrajectory,
		PublicOutcome:  traj.Outcome.CanonicalString(),
		CheckpointID:   "checkpoint-" + traj.ID,
		EvidenceID:     "evidence-" + traj.ID,
		Fact:           "the relative path resolved outside the workspace on " + traj.ID,
	})
}

func validBody(t *testing.T, traj Trajectory) map[string]any {
	t.Helper()
	body, err := rawadmit.ValidProposalBody(
		"raw-proposal-"+traj.ID+"-00001",
		traj.DiagnosisRunID,
		"checkpoint-"+traj.ID,
		"evidence-"+traj.ID,
		"the relative path resolved outside the workspace on "+traj.ID,
		traj.Outcome.CanonicalString(),
		"2026-09-17T10:06:00Z",
	)
	if err != nil {
		t.Fatalf("valid proposal body: %v", err)
	}
	return body
}

func awaitStarts(t *testing.T, started <-chan string, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	seen := map[string]bool{}
	for len(seen) < want {
		select {
		case id := <-started:
			seen[id] = true
		case <-deadline:
			t.Fatalf("started %d/%d jobs before timeout: %v", len(seen), want, keys(seen))
		}
	}
}

func keys(seen map[string]bool) []string {
	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	return out
}

func waitUntil(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if ready() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for diagnosis job to become terminal")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
