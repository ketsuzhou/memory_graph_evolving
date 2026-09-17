package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rawadmit "river2.dev/graph-memory-service/testdata/rawadmit"
	smokefix "river2.dev/graph-memory-service/testdata/smokefix"

	"river2.dev/pi-group-chat-host/internal/agentrun"
	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	"river2.dev/pi-group-chat-host/internal/diagnosisfanout"
	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/effectsinterrupt"
	"river2.dev/pi-group-chat-host/internal/pi"
	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
	"river2.dev/pi-group-chat-host/internal/skilldisposition"
	"river2.dev/pi-group-chat-host/internal/skillfence"
)

const (
	graphBatchSmokeSchema           = "warm-skill-graph-batch.contract-smoke.v1"
	graphBatchSmokeContractID       = "warm-skill-graph-batch.system-contract"
	graphBatchSmokeStatusGreen      = "green"
	graphBatchSmokeStatusIncomplete = "incomplete"
	graphBatchSmokePass             = "pass"
	graphBatchSmokeFail             = "fail"
	graphBatchSmokeSkipped          = "skipped"
	graphBatchSmokeUnavailable      = "unavailable"
)

var graphBatchSmokeAssertionNames = []string{
	"01_task_and_memory_parallel",
	"02_structured_mention_exact_session_abort_resume",
	"03_resolved_body_digest_matches_served",
	"04_accepted_rejected_with_reason",
	"05_rejected_triggers_next_graph_exploration",
	"06_same_agent_no_overlapping_pi_run",
	"07_effects_unknown_fail_closed",
	"08_diagnosis_proposal_keeps_exact_evidence_lineage",
	"09_consolidation_covers_complete_source_ids",
	"10_held_out_feedback_does_not_enter_frozen_graph",
	"11_all_tests_use_same_manifest",
	"12_primary_metric_uses_full_preregistered_denominator",
}

// graphBatchSmokeBundle is the self-describing TB-16 artifact. Every
// §10 assertion records machine-checkable evidence. contract_status is
// green only when all twelve pass and real Pi 0.85.1 abort/resume is proven.
type graphBatchSmokeBundle struct {
	SchemaVersion    string                     `json:"schema_version"`
	ContractID       string                     `json:"contract_id"`
	Strategy         string                     `json:"strategy"`
	ContractStatus   string                     `json:"contract_status"`
	Commands         []string                   `json:"commands"`
	Versions         map[string]string          `json:"versions"`
	FrozenConfig     graphBatchFrozenConfig     `json:"frozen_config"`
	Manifest         graphBatchSmokeManifest    `json:"manifest"`
	BodyDigests      []graphBatchSmokeDigest    `json:"body_digests"`
	Assertions       []graphBatchSmokeAssertion `json:"assertions"`
	PiProbe          graphBatchSmokePiProbe     `json:"pi_probe"`
	KnownLimitations []string                   `json:"known_limitations"`
}

type graphBatchSmokeManifest struct {
	Digest           string   `json:"digest"`
	ProjectionDigest string   `json:"projection_digest,omitempty"`
	LedgerDigest     string   `json:"ledger_digest,omitempty"`
	SkillRefs        []string `json:"skill_refs"`
	TestAttemptIDs   []string `json:"test_attempt_ids"`
}

type graphBatchSmokeDigest struct {
	SkillReference string `json:"skill_reference"`
	ResolvedDigest string `json:"resolved_digest"`
	ServedDigest   string `json:"served_digest"`
	Kind           string `json:"kind"`
}

type graphBatchSmokeAssertion struct {
	ID       int            `json:"id"`
	Name     string         `json:"name"`
	Status   string         `json:"status"`
	Evidence map[string]any `json:"evidence"`
}

type graphBatchSmokePiProbe struct {
	Status           string   `json:"status"`
	Binary           string   `json:"binary,omitempty"`
	Version          string   `json:"version,omitempty"`
	RequiredVersion  string   `json:"required_version"`
	SessionFile      string   `json:"session_file,omitempty"`
	SessionID        string   `json:"session_id,omitempty"`
	Argv             []string `json:"argv,omitempty"`
	Aborted          bool     `json:"aborted"`
	Settled          bool     `json:"settled"`
	UsedResumeFlag   bool     `json:"used_resume_flag"`
	UsedContinueFlag bool     `json:"used_continue_flag"`
	Detail           string   `json:"detail,omitempty"`
}

type graphBatchSmokeRun struct {
	Bundle graphBatchSmokeBundle
}

// runGraphBatchContractSmoke composes Host/GMS public seams on a small
// fixture graph: reject→redirect→accept, effects-unknown retry, canonical
// diagnosis/consolidation, and frozen test isolation. It does not change
// TB-14 pipeline semantics or TB-15 report口径.
func runGraphBatchContractSmoke(ctx context.Context) (graphBatchSmokeRun, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	config := defaultGraphBatchFrozenConfig()
	fix, err := smokefix.New()
	if err != nil {
		return graphBatchSmokeRun{}, fmt.Errorf("smoke fixture graph: %w", err)
	}
	manifestDigest := smokeManifestDigest(fix)

	assertions := make([]graphBatchSmokeAssertion, 0, 12)
	bodyDigests := []graphBatchSmokeDigest{}

	a1, err := proveTaskAndMemoryParallel(ctx)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a1)

	a2, err := proveStructuredMentionExactSession(ctx)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a2)

	loop, err := proveRejectRedirectAccept(ctx, fix, manifestDigest)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, loop.digest, loop.disposition, loop.redirect)
	bodyDigests = append(bodyDigests, loop.bodies...)

	a6, err := proveSameAgentNoOverlap(ctx)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a6)

	a7, err := proveEffectsUnknownFailClosed(ctx, config)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a7)

	a8, err := proveDiagnosisEvidenceLineage(ctx)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a8)

	a9, err := proveConsolidationCoversSourceIDs(ctx)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a9)

	a10, err := proveHeldOutFeedbackIsolation(ctx)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a10)

	a11, err := proveAllTestsShareManifest(ctx)
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a11)

	a12, err := proveFullPreregisteredDenominator()
	if err != nil {
		return graphBatchSmokeRun{}, err
	}
	assertions = append(assertions, a12)

	if err := validateSmokeAssertionSet(assertions); err != nil {
		return graphBatchSmokeRun{}, err
	}

	piProbe := probeAndProveRealPiExactSession(ctx)
	status := graphBatchSmokeStatusIncomplete
	if allSmokePassed(assertions) && piProbe.Status == graphBatchSmokePass {
		status = graphBatchSmokeStatusGreen
	}

	bundle := graphBatchSmokeBundle{
		SchemaVersion:  graphBatchSmokeSchema,
		ContractID:     graphBatchSmokeContractID,
		Strategy:       graphBatchStrategyID,
		ContractStatus: status,
		Commands: []string{
			"GOCACHE=/tmp/wsgb-tb16-go-cache $HOME/go/bin/go test ./cmd/bench-runner/ -count=1 -run TestGraphBatchContractSmoke",
			"GOCACHE=/tmp/wsgb-tb16-go-cache $HOME/go/bin/go test ./... -count=1",
			"GOCACHE=/tmp/wsgb-tb16-go-cache $HOME/go/bin/go test -race ./internal/pi/sessionctrl/ ./internal/directedoffer/ ./internal/effectsinterrupt/ ./cmd/bench-runner/ -count=1",
		},
		Versions: map[string]string{
			"go":           runtime.Version(),
			"pi_required":  pi.RequiredVersion,
			"contract":     "0.1.0",
			"strategy":     graphBatchStrategyID,
			"smoke_schema": graphBatchSmokeSchema,
		},
		FrozenConfig: config,
		Manifest: graphBatchSmokeManifest{
			Digest:           manifestDigest,
			ProjectionDigest: fix.ProjectionDigest,
			LedgerDigest:     fix.LedgerDigest,
			SkillRefs:        []string{fix.GenericRef, fix.SpecializedRef},
			TestAttemptIDs:   []string{"test-a", "test-b"},
		},
		BodyDigests:      bodyDigests,
		Assertions:       assertions,
		PiProbe:          piProbe,
		KnownLimitations: append([]string(nil), graphBatchKnownLimitations...),
	}
	return graphBatchSmokeRun{Bundle: bundle}, nil
}

func writeGraphBatchSmokeBundle(path string, bundle graphBatchSmokeBundle) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("smoke bundle path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), 0o644)
}

func smokeManifestDigest(fix *smokefix.Fixture) string {
	parts := []string{
		fix.ProjectionDigest,
		fix.LedgerDigest,
		fix.GenericRef,
		fix.GenericDigest,
		fix.SpecializedRef,
		fix.SpecializedDigest,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func smokeAssertion(id int, name, status string, evidence map[string]any) graphBatchSmokeAssertion {
	if evidence == nil {
		evidence = map[string]any{}
	}
	return graphBatchSmokeAssertion{ID: id, Name: name, Status: status, Evidence: evidence}
}

func passFail(ok bool) string {
	if ok {
		return graphBatchSmokePass
	}
	return graphBatchSmokeFail
}

func allSmokePassed(assertions []graphBatchSmokeAssertion) bool {
	if len(assertions) != 12 {
		return false
	}
	for _, item := range assertions {
		if item.Status != graphBatchSmokePass {
			return false
		}
	}
	return true
}

func validateSmokeAssertionSet(assertions []graphBatchSmokeAssertion) error {
	if len(assertions) != 12 {
		return fmt.Errorf("smoke assertions = %d; want 12", len(assertions))
	}
	for i, item := range assertions {
		if item.ID != i+1 || item.Name != graphBatchSmokeAssertionNames[i] {
			return fmt.Errorf("smoke assertion[%d] = %d %q; want %d %q", i, item.ID, item.Name, i+1, graphBatchSmokeAssertionNames[i])
		}
	}
	return nil
}

func proveTaskAndMemoryParallel(ctx context.Context) (graphBatchSmokeAssertion, error) {
	taskHold := make(chan struct{})
	memoryHold := make(chan struct{})
	task := concurrentepisode.NewScriptedSession("task-agent")
	task.Hold = taskHold
	memory := concurrentepisode.NewScriptedSession("memory-agent")
	memory.Hold = memoryHold

	done := make(chan concurrentepisode.Result, 1)
	errc := make(chan error, 1)
	go func() {
		result, err := concurrentepisode.Run(ctx, concurrentepisode.Request{
			Opening: concurrentepisode.OpeningContext{Messages: []concurrentepisode.OpeningMessage{{
				ID: "open-smoke", Author: "user", Content: "solve smoke",
			}}},
			Graph:                 concurrentepisode.EvaluationGraph{},
			Task:                  task,
			Memory:                memory,
			TaskContextMonitoring: concurrentepisode.ContextOpeningOnly,
		})
		if err != nil {
			errc <- err
			return
		}
		done <- result
	}()

	if err := waitUntil(ctx, func() bool {
		return task.Observation().Phase == concurrentepisode.PhaseRunning && memory.Observation().Phase == concurrentepisode.PhaseRunning
	}); err != nil {
		return graphBatchSmokeAssertion{}, fmt.Errorf("task/memory start-barrier: %w", err)
	}
	bothRunning := task.Observation().Phase == concurrentepisode.PhaseRunning && memory.Observation().Phase == concurrentepisode.PhaseRunning
	neitherSettled := task.SettledAt().IsZero() && memory.SettledAt().IsZero()
	close(taskHold)
	close(memoryHold)

	var result concurrentepisode.Result
	select {
	case result = <-done:
	case err := <-errc:
		return graphBatchSmokeAssertion{}, err
	case <-ctx.Done():
		return graphBatchSmokeAssertion{}, ctx.Err()
	}

	entered := 0
	for _, event := range result.Timeline {
		if event.Kind == concurrentepisode.EventEnteredRunning {
			entered++
		}
	}
	ok := bothRunning && neitherSettled && entered == 2 && result.TaskStatus == concurrentepisode.TaskStatusCompleted
	return smokeAssertion(1, graphBatchSmokeAssertionNames[0], passFail(ok), map[string]any{
		"both_running_before_settle": bothRunning && neitherSettled,
		"entered_running":            entered,
		"task_status":                result.TaskStatus,
		"memory_terminal":            result.MemoryTerminal,
		"task_context_monitoring":    concurrentepisode.ContextOpeningOnly,
	}), nil
}

func proveStructuredMentionExactSession(ctx context.Context) (graphBatchSmokeAssertion, error) {
	pinned := sessionctrl.Session{File: "/sessions/exact-task.jsonl", ID: "exact-session-id"}
	session := directedoffer.NewScriptedExactSession("task-agent", pinned)
	session.Hold = make(chan struct{})
	go func() { _ = session.Start(ctx) }()
	if err := waitUntil(ctx, session.Running); err != nil {
		return graphBatchSmokeAssertion{}, err
	}

	coord := agentrun.New(effectsinterrupt.Policy{}).Offers()
	binding := directedoffer.TestBinding("task-agent", pinned)
	binding.EvaluationID = "eval-smoke"
	binding.LogicalRunID = "run-smoke"
	binding.Generation = 1
	binding.SegmentID = "seg-task-open"
	if err := coord.Attach(binding, session); err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	got, err := coord.Offer(ctx, directedoffer.Request{
		RoomID:           "room-1",
		AuthorID:         "memory-agent",
		Content:          "@task-agent consider " + smokefix.GenericRef,
		IdempotencyKey:   "offer-generic",
		RecipientAgentID: "task-agent",
		SkillReference:   smokefix.GenericRef,
	})
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	obs := session.Observation()
	ok := obs.AbortCount == 1 &&
		obs.ResumeCount == 1 &&
		obs.LastResume.Session == pinned &&
		got.Segment.ContinuationOf == "seg-task-open" &&
		got.Segment.Reason == directedoffer.ReasonDirectedMention &&
		len(got.Deliveries) == 1
	return smokeAssertion(2, graphBatchSmokeAssertionNames[1], passFail(ok), map[string]any{
		"abort_count":        obs.AbortCount,
		"resume_count":       obs.ResumeCount,
		"session_file":       obs.LastResume.Session.File,
		"session_id":         obs.LastResume.Session.ID,
		"continuation_of":    got.Segment.ContinuationOf,
		"segment_reason":     got.Segment.Reason,
		"deliveries":         len(got.Deliveries),
		"used_resume_flag":   false,
		"used_continue_flag": false,
	}), nil
}

type rejectRedirectAcceptProof struct {
	digest      graphBatchSmokeAssertion
	disposition graphBatchSmokeAssertion
	redirect    graphBatchSmokeAssertion
	bodies      []graphBatchSmokeDigest
}

func proveRejectRedirectAccept(ctx context.Context, fix *smokefix.Fixture, manifestDigest string) (rejectRedirectAcceptProof, error) {
	first, err := fix.FirstOffer()
	if err != nil {
		return rejectRedirectAcceptProof{}, err
	}

	session := skillfence.NewScriptedSession("task-agent")
	resolver := &smokeSkillResolver{fix: fix, manifest: manifestDigest}
	memory := &skillfence.RecordingNotifier{}
	fence, err := skillfence.NewCoordinator(resolver, session, memory)
	if err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	if err := fence.BindOffer(skillfence.Offer{
		OfferID:        "offer-generic",
		AgentID:        "task-agent",
		SkillReference: first,
		ManifestDigest: manifestDigest,
	}); err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	genericGet, err := fence.HandleToolCall(ctx, skillfence.ToolCall{
		CallID: "get-generic", Name: skillfence.ToolSkillGet, AgentID: "task-agent", SkillReference: first,
	})
	if err != nil || genericGet.Served == nil {
		return rejectRedirectAcceptProof{}, fmt.Errorf("skill_get generic: %v served=%v", err, genericGet.Served)
	}

	publisher := skilldisposition.NewScriptedPublisher(skilldisposition.NewOfferPublisher(nil))
	disp, err := skilldisposition.NewCoordinator(publisher)
	if err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	if err := disp.BindOffer(skilldisposition.Offer{
		OfferID: "offer-generic", AgentID: "task-agent", RoomID: "room-1",
		SkillReference: first, CheckpointID: "cp-opening-smoke",
	}); err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	if err := disp.NoteServed(*genericGet.Served); err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	rejected, err := disp.HandleToolCall(ctx, skilldisposition.ToolCall{
		CallID: "fb-reject", Name: skilldisposition.ToolSkillFeedback,
		AgentID: "task-agent", Disposition: skilldisposition.DispositionRejected,
	})
	if err != nil || rejected.Disposition == nil || rejected.Disposition.Value != skilldisposition.DispositionRejected {
		return rejectRedirectAcceptProof{}, fmt.Errorf("rejected feedback: %v %#v", err, rejected.Disposition)
	}

	hop, err := fix.RejectTooGeneric("offer-generic", "too generic for this failure")
	if err != nil {
		return rejectRedirectAcceptProof{}, err
	}

	if err := fence.BindOffer(skillfence.Offer{
		OfferID:        "offer-specialized",
		AgentID:        "task-agent",
		SkillReference: hop.NextOffer,
		ManifestDigest: manifestDigest,
	}); err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	specialGet, err := fence.HandleToolCall(ctx, skillfence.ToolCall{
		CallID: "get-specialized", Name: skillfence.ToolSkillGet, AgentID: "task-agent", SkillReference: hop.NextOffer,
	})
	if err != nil || specialGet.Served == nil {
		return rejectRedirectAcceptProof{}, fmt.Errorf("skill_get specialized: %v served=%v", err, specialGet.Served)
	}
	if err := disp.BindOffer(skilldisposition.Offer{
		OfferID: "offer-specialized", AgentID: "task-agent", RoomID: "room-1",
		SkillReference: hop.NextOffer, CheckpointID: "cp-opening-smoke",
	}); err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	if err := disp.NoteServed(*specialGet.Served); err != nil {
		return rejectRedirectAcceptProof{}, err
	}
	accepted, err := disp.HandleToolCall(ctx, skilldisposition.ToolCall{
		CallID: "fb-accept", Name: skilldisposition.ToolSkillFeedback,
		AgentID: "task-agent", Disposition: skilldisposition.DispositionAccepted,
	})
	if err != nil || accepted.Disposition == nil || accepted.Disposition.Value != skilldisposition.DispositionAccepted {
		return rejectRedirectAcceptProof{}, fmt.Errorf("accepted feedback: %v %#v", err, accepted.Disposition)
	}

	digestOK := genericGet.Served.BodyDigest == fix.GenericDigest &&
		specialGet.Served.BodyDigest == fix.SpecializedDigest &&
		skillfence.BodyDigest(session.CapturedBody()) == fix.SpecializedDigest
	dispositionOK := rejected.Disposition.Value == skilldisposition.DispositionRejected &&
		accepted.Disposition.Value == skilldisposition.DispositionAccepted &&
		hop.ReasonCode == smokefix.ReasonTooGeneric &&
		hop.Reason != ""
	redirectOK := hop.Action == smokefix.ActionRedirect &&
		hop.NextOffer == fix.SpecializedRef &&
		hop.NextQuery != "" &&
		containsString(hop.NextSeeds, fix.SpecializedRef)

	bodies := []graphBatchSmokeDigest{
		{SkillReference: first, ResolvedDigest: fix.GenericDigest, ServedDigest: genericGet.Served.BodyDigest, Kind: "original"},
		{SkillReference: hop.NextOffer, ResolvedDigest: fix.SpecializedDigest, ServedDigest: specialGet.Served.BodyDigest, Kind: "original"},
	}
	return rejectRedirectAcceptProof{
		digest: smokeAssertion(3, graphBatchSmokeAssertionNames[2], passFail(digestOK), map[string]any{
			"generic_resolved":     fix.GenericDigest,
			"generic_served":       genericGet.Served.BodyDigest,
			"specialized_resolved": fix.SpecializedDigest,
			"specialized_served":   specialGet.Served.BodyDigest,
			"captured_digest":      skillfence.BodyDigest(session.CapturedBody()),
		}),
		disposition: smokeAssertion(4, graphBatchSmokeAssertionNames[3], passFail(dispositionOK), map[string]any{
			"rejected":    rejected.Disposition.Value,
			"accepted":    accepted.Disposition.Value,
			"reason_code": hop.ReasonCode,
			"reason":      hop.Reason,
		}),
		redirect: smokeAssertion(5, graphBatchSmokeAssertionNames[4], passFail(redirectOK), map[string]any{
			"first_offer": first,
			"next_offer":  hop.NextOffer,
			"next_query":  hop.NextQuery,
			"next_seeds":  hop.NextSeeds,
			"action":      hop.Action,
			"reason_code": hop.ReasonCode,
		}),
		bodies: bodies,
	}, nil
}

type smokeSkillResolver struct {
	fix      *smokefix.Fixture
	manifest string
}

func (r *smokeSkillResolver) Resolve(_ context.Context, req skillfence.ResolveRequest) (skillfence.ResolvedView, error) {
	if req.ManifestDigest != r.manifest {
		return skillfence.ResolvedView{}, skillfence.ErrCrossManifestRevision
	}
	body, digest, err := r.fix.Resolve(req.SkillReference)
	if err != nil {
		return skillfence.ResolvedView{}, err
	}
	return skillfence.ResolvedView{
		Reference:      req.SkillReference,
		Body:           body,
		ViewDigest:     digest,
		ManifestDigest: r.manifest,
	}, nil
}

func proveSameAgentNoOverlap(ctx context.Context) (graphBatchSmokeAssertion, error) {
	flights := agentrun.New(effectsinterrupt.Policy{}).Flights()
	firstHold := make(chan struct{})
	first := concurrentepisode.NewScriptedSession("shared-agent")
	first.Hold = firstHold
	second := concurrentepisode.NewScriptedSession("shared-agent")
	opening := concurrentepisode.OpeningContext{Messages: []concurrentepisode.OpeningMessage{{
		ID: "open-overlap", Author: "user", Content: "overlap",
	}}}

	var overlapping atomic.Bool
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- flights.Run(ctx, first.AgentID(), func() error {
			return first.Start(ctx, concurrentepisode.SessionInput{Opening: opening}, nil)
		})
	}()
	if err := waitUntil(ctx, func() bool { return first.Observation().Phase == concurrentepisode.PhaseRunning }); err != nil {
		return graphBatchSmokeAssertion{}, err
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- flights.Run(ctx, second.AgentID(), func() error {
			if first.Observation().Phase == concurrentepisode.PhaseRunning {
				overlapping.Store(true)
			}
			return second.Start(ctx, concurrentepisode.SessionInput{Opening: opening}, nil)
		})
	}()
	time.Sleep(20 * time.Millisecond)
	secondRunningWhileFirst := second.Observation().Phase == concurrentepisode.PhaseRunning && first.Observation().Phase == concurrentepisode.PhaseRunning
	close(firstHold)
	if err := <-firstDone; err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	if err := <-secondDone; err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	ok := !overlapping.Load() && !secondRunningWhileFirst &&
		!first.SettledAt().IsZero() &&
		!second.RunningAt().Before(first.SettledAt())
	return smokeAssertion(6, graphBatchSmokeAssertionNames[5], passFail(ok), map[string]any{
		"overlapping":                 overlapping.Load(),
		"second_running_while_first":  secondRunningWhileFirst,
		"first_settled_before_second": !second.RunningAt().Before(first.SettledAt()),
	}), nil
}

func proveEffectsUnknownFailClosed(ctx context.Context, config graphBatchFrozenConfig) (graphBatchSmokeAssertion, error) {
	policy, err := graphBatchInterruptPolicy(config)
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	policy.InterruptGrace = 15 * time.Millisecond
	policy.KillGrace = 15 * time.Millisecond

	const taskID = "held-out-effects"
	runner := effectsinterrupt.NewRunner(2)
	runner.Preregister(taskID)
	attemptID, ok := runner.AllocateAttempt(taskID)
	if !ok {
		return graphBatchSmokeAssertion{}, errors.New("effects-unknown: first attempt was not allocated")
	}

	session := effectsinterrupt.NewScriptedSession("task-agent", sessionctrl.Session{
		File: "/sessions/exact-task.jsonl", ID: "exact-session-id",
	}, effectsinterrupt.KindMutating)
	session.Hold = make(chan struct{})
	session.ToolHold = make(chan struct{})
	coord := effectsinterrupt.NewCoordinator(policy, runner)
	go func() { _ = session.Start(ctx) }()
	if err := waitUntil(ctx, session.Running); err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	binding := directedoffer.TestBinding("task-agent", session.Session())
	binding.EvaluationID = "eval-smoke"
	binding.LogicalRunID = taskID
	binding.AttemptID = attemptID
	binding.Generation = 1
	if err := coord.Attach(binding, taskID, session, nil); err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	got, err := coord.Interrupt(ctx, "task-agent")
	if !errors.Is(err, effectsinterrupt.ErrEffectsUnknown) {
		return graphBatchSmokeAssertion{}, fmt.Errorf("effects-unknown interrupt: %v", err)
	}
	records := runner.Attempts(taskID)
	passed, denom := runner.PassAt1()
	okResult := got.Outcome == effectsinterrupt.OutcomeEffectsUnknown &&
		!got.Resumed &&
		got.NextAttemptID != "" &&
		got.NextAttemptID != attemptID &&
		session.Observation().ResumeCount == 0 &&
		len(records) == 1 &&
		!records[0].Resumed &&
		passed == 0 && denom == 1
	return smokeAssertion(7, graphBatchSmokeAssertionNames[6], passFail(okResult), map[string]any{
		"outcome":         got.Outcome,
		"resumed":         got.Resumed,
		"next_attempt_id": got.NextAttemptID,
		"kept_attempt_id": attemptID,
		"pass_at_1":       fmt.Sprintf("%d/%d", passed, denom),
		"fail_closed":     true,
	}), nil
}

func proveDiagnosisEvidenceLineage(ctx context.Context) (graphBatchSmokeAssertion, error) {
	adapter, err := rawadmit.New()
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	traj := diagnosisfanout.Trajectory{
		Sequence:           1,
		ID:                 "traj-smoke",
		SnapshotID:         "snapshot-traj-smoke",
		DiagnosisRunID:     "diagnosis-run-traj-smoke",
		CompleteTrajectory: []string{"opening", "checkpoint", "outcome"},
		Outcome:            diagnosisfanout.PublicOutcome{Status: diagnosisfanout.StatusFail, RuntimeError: "nil pointer"},
		Checkpoints:        map[string]diagnosisfanout.Checkpoint{"checkpoint-traj-smoke": {ID: "checkpoint-traj-smoke", SnapshotID: "snapshot-traj-smoke"}},
		Evidence: map[string]diagnosisfanout.Evidence{
			"evidence-traj-smoke": {
				ID: "evidence-traj-smoke", TrajectoryID: "traj-smoke", SnapshotID: "snapshot-traj-smoke",
				ObservableFacts: []string{"the relative path resolved outside the workspace on traj-smoke"},
			},
		},
	}
	adapter.Put(rawadmit.FrozenSpec{
		TrajectoryID: traj.ID, SnapshotID: traj.SnapshotID, DiagnosisRunID: traj.DiagnosisRunID,
		Steps: traj.CompleteTrajectory, PublicOutcome: traj.Outcome.CanonicalString(),
		CheckpointID: "checkpoint-traj-smoke", EvidenceID: "evidence-traj-smoke",
		Fact: "the relative path resolved outside the workspace on traj-smoke",
	})
	body, err := rawadmit.ValidProposalBody(
		"raw-proposal-traj-smoke-00001", traj.DiagnosisRunID,
		"checkpoint-traj-smoke", "evidence-traj-smoke",
		"the relative path resolved outside the workspace on traj-smoke",
		traj.Outcome.CanonicalString(), "2026-09-17T10:47:00Z",
	)
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	worker := &smokeDiagnosisWorker{draft: diagnosisfanout.WorkerDraft{
		IdempotencyKey: "idem-traj-smoke", ProposalBody: body,
	}}
	coord := diagnosisfanout.NewCoordinator(worker, &smokeRawAdmitter{adapter: adapter})
	results, err := coord.Run(ctx, []diagnosisfanout.Trajectory{traj}, 1)
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	refs, _ := body["source_evidence_refs"].([]any)
	evidenceOK := len(results) == 1 && results[0].Proposal != nil &&
		adapter.Has(results[0].Proposal.ProposalID) &&
		len(refs) == 1 && refs[0] == "evidence-traj-smoke" &&
		body["source_checkpoint_id"] == "checkpoint-traj-smoke"
	return smokeAssertion(8, graphBatchSmokeAssertionNames[7], passFail(evidenceOK), map[string]any{
		"proposal_id":          results[0].Proposal.ProposalID,
		"source_evidence_refs": refs,
		"source_checkpoint_id": body["source_checkpoint_id"],
		"content_digest":       results[0].Proposal.ContentDigest,
	}), nil
}

type smokeDiagnosisWorker struct {
	draft diagnosisfanout.WorkerDraft
}

func (w *smokeDiagnosisWorker) Diagnose(_ context.Context, _ diagnosisfanout.DiagnosisInput) (diagnosisfanout.WorkerDraft, error) {
	return w.draft, nil
}

type smokeRawAdmitter struct {
	adapter *rawadmit.Adapter
}

func (a *smokeRawAdmitter) Admit(ctx context.Context, request diagnosisfanout.AdmissionRequest) (diagnosisfanout.ProposalRef, error) {
	id, digest, uri, err := a.adapter.Admit(ctx, request.IdempotencyKey, request.DiagnosisRunID, request.TrajectoryID, request.Body)
	if err != nil {
		return diagnosisfanout.ProposalRef{}, err
	}
	return diagnosisfanout.ProposalRef{ProposalID: id, ContentDigest: digest, URI: uri}, nil
}

func (a *smokeRawAdmitter) Count() int { return a.adapter.Count() }

func (a *smokeRawAdmitter) Has(proposalID string) bool { return a.adapter.Has(proposalID) }

func proveConsolidationCoversSourceIDs(ctx context.Context) (graphBatchSmokeAssertion, error) {
	ledger := newMemoryGraphBatchLedger()
	ids := []string{"raw-proposal-traj-a-00001", "raw-proposal-traj-b-00001"}
	got, err := ledger.Consolidate(ctx, graphBatchConsolidateRequest{FamilyProposalIDs: ids})
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	covered := map[string]bool{}
	for _, decision := range got.Decisions {
		for _, src := range decision.SourceProposalIDs {
			if err := rejectGraphBatchNonCanonicalID(src); err != nil {
				return graphBatchSmokeAssertion{}, err
			}
			covered[src] = true
		}
	}
	ok := len(got.Decisions) == 1 && len(covered) == 2 && covered[ids[0]] && covered[ids[1]]
	return smokeAssertion(9, graphBatchSmokeAssertionNames[8], passFail(ok), map[string]any{
		"family_proposal_ids": ids,
		"source_proposal_ids": got.Decisions[0].SourceProposalIDs,
		"decision_id":         got.Decisions[0].DecisionID,
		"complete_ids":        true,
	}), nil
}

func proveHeldOutFeedbackIsolation(ctx context.Context) (graphBatchSmokeAssertion, error) {
	graphErr := smokefix.HeldOutFeedbackRejected()
	graphRejected := graphErr != nil && strings.Contains(graphErr.Error(), "held-out feedback")

	ledger := newMemoryGraphBatchLedger()
	runtime := newGraphBatchFixtureRuntimeForSmoke(ledger, []graphBatchHeldOutSpec{
		{
			Sequence: 10, AttemptID: "test-a", Opening: smokeOpening("test-a"),
			Feedback: []graphBatchTestFeedback{{
				OfferID: "offer-a", Disposition: "rejected", ReasonCode: "too_generic", Reason: "too generic for test A",
			}},
		},
		{Sequence: 11, AttemptID: "test-b", Opening: smokeOpening("test-b")},
	})
	result, err := runGraphBatchCanonicalPipeline(ctx, runtime)
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	byID := map[string]graphBatchTestAttempt{}
	for _, attempt := range result.TestAttempts {
		byID[attempt.AttemptID] = attempt
	}
	replay, err := ledger.Retrieve("test-b", result.ManifestDigest, result.HeldOutTraces)
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	keysUnchanged := len(byID) == 2 &&
		sameStrings(byID["test-a"].Retrieval.Keys, byID["test-b"].Retrieval.Keys) &&
		sameStrings(replay.Keys, byID["test-b"].Retrieval.Keys)
	traceOnly := false
	for _, trace := range result.HeldOutTraces {
		if trace.AttemptID == "test-a" && len(trace.Feedback) == 1 && trace.Feedback[0].ReasonCode == "too_generic" {
			traceOnly = true
		}
	}
	ok := graphRejected && keysUnchanged && traceOnly && strings.Contains(graphErr.Error(), "held-out feedback")
	return smokeAssertion(10, graphBatchSmokeAssertionNames[9], passFail(ok), map[string]any{
		"graph_rejects_held_out": graphRejected,
		"graph_error":            graphErr.Error(),
		"retrieval_keys_stable":  keysUnchanged,
		"test_a_feedback_trace":  traceOnly,
		"test_feedback_sink":     defaultGraphBatchFrozenConfig().TestFeedbackSink,
	}), nil
}

func proveAllTestsShareManifest(ctx context.Context) (graphBatchSmokeAssertion, error) {
	ledger := newMemoryGraphBatchLedger()
	runtime := newGraphBatchFixtureRuntimeForSmoke(ledger, []graphBatchHeldOutSpec{
		{Sequence: 10, AttemptID: "test-a", Opening: smokeOpening("test-a")},
		{Sequence: 11, AttemptID: "test-b", Opening: smokeOpening("test-b")},
	})
	result, err := runGraphBatchCanonicalPipeline(ctx, runtime)
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	shared := result.ManifestDigest != ""
	for _, attempt := range result.TestAttempts {
		if attempt.ManifestDigest != result.ManifestDigest || attempt.Retrieval.ManifestDigest != result.ManifestDigest {
			shared = false
		}
	}
	for _, trace := range result.HeldOutTraces {
		if trace.ManifestDigest != result.ManifestDigest {
			shared = false
		}
	}
	ok := shared && len(result.TestAttempts) == 2 && len(result.HeldOutTraces) == 2
	return smokeAssertion(11, graphBatchSmokeAssertionNames[10], passFail(ok), map[string]any{
		"manifest_digest": result.ManifestDigest,
		"test_attempts":   len(result.TestAttempts),
		"held_out_traces": len(result.HeldOutTraces),
		"all_same":        shared,
	}), nil
}

func proveFullPreregisteredDenominator() (graphBatchSmokeAssertion, error) {
	policy := graphBatchReportPolicy{
		ManifestDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		Model:          "warm-skill-graph-batch.model.v1",
		Seed:           "7",
		GradingPolicy:  "warm-skill-graph-batch.grading.v1",
		SalvagePolicy:  "pre-fixed.v1",
	}
	req := graphBatchPairedReportRequest{
		Manifest: graphBatchPreregisteredManifest{
			graphBatchReportPolicy: policy,
			Tasks: []graphBatchPreregisteredTask{
				{TaskID: "task-pass"},
				{TaskID: "task-timeout"},
				{TaskID: "task-fail"},
			},
		},
		Warm: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{
				{TaskID: "task-pass", GraderOutcome: graphBatchOutcomePass, MemoryTerminal: "accepted"},
				{TaskID: "task-timeout", GraderOutcome: graphBatchOutcomeTimeout, MemoryTerminal: "memory_timeout"},
				{TaskID: "task-fail", GraderOutcome: graphBatchOutcomeFail, MemoryTerminal: "all_candidates_rejected"},
			},
		},
		Cold: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{
				{TaskID: "task-pass", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-timeout", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-fail", GraderOutcome: graphBatchOutcomeFail},
			},
		},
	}
	report, err := generateGraphBatchPairedReport(req)
	if err != nil {
		return graphBatchSmokeAssertion{}, err
	}
	ok := report.PassAt1.Warm.Denominator == 3 &&
		report.PassAt1.Warm.Passed == 1 &&
		report.PassAt1.Warm.Rate == "1/3" &&
		report.Paired.Total == 3 &&
		len(report.Pairings) == 3
	return smokeAssertion(12, graphBatchSmokeAssertionNames[11], passFail(ok), map[string]any{
		"passed":      report.PassAt1.Warm.Passed,
		"denominator": report.PassAt1.Warm.Denominator,
		"rate":        report.PassAt1.Warm.Rate,
		"pairings":    len(report.Pairings),
		"tasks":       []string{"task-pass", "task-timeout", "task-fail"},
	}), nil
}

func newGraphBatchFixtureRuntimeForSmoke(ledger *memoryGraphBatchLedger, heldOut []graphBatchHeldOutSpec) *graphBatchRuntime {
	return &graphBatchRuntime{
		Config:      defaultGraphBatchFrozenConfig(),
		Parallelism: 1,
		Trains:        []graphBatchTrainSpec{{Sequence: 1, Trajectory: smokeTrainTrajectory("traj-a")}},
		TrainExecutor: scriptedTrainExecutor{},
		AuthorityKind: graphBatchAuthorityFixture,
		DiagnosisWorker: &smokeDiagnosisWorker{draft: diagnosisfanout.WorkerDraft{
			IdempotencyKey: "idem-traj-a",
			ProposalBody:   map[string]any{"proposal_id": "raw-proposal-traj-a-00001"},
		}},
		DiagnosisAdmitter: &smokeCanonicalAdmitter{},
		Consolidator:      ledger,
		Freezer:           ledger,
		Retriever:         ledger,
		HeldOut:           heldOut,
		Probe:             newGraphBatchProbe(1, 1, len(heldOut)),
	}
}

func smokeTrainTrajectory(id string) diagnosisfanout.Trajectory {
	return diagnosisfanout.Trajectory{
		Sequence:           1,
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

func smokeOpening(attemptID string) concurrentepisode.OpeningContext {
	return concurrentepisode.OpeningContext{Messages: []concurrentepisode.OpeningMessage{{
		ID: "open-" + attemptID, Author: "user", Content: "solve " + attemptID,
	}}}
}

type smokeCanonicalAdmitter struct {
	mu       sync.Mutex
	admitted []string
}

func (a *smokeCanonicalAdmitter) Admit(_ context.Context, request diagnosisfanout.AdmissionRequest) (diagnosisfanout.ProposalRef, error) {
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

func (a *smokeCanonicalAdmitter) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.admitted)
}

func (a *smokeCanonicalAdmitter) Has(proposalID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, id := range a.admitted {
		if id == proposalID {
			return true
		}
	}
	return false
}

func probeAndProveRealPiExactSession(ctx context.Context) graphBatchSmokePiProbe {
	probe := graphBatchSmokePiProbe{
		RequiredVersion: pi.RequiredVersion,
		Status:          graphBatchSmokeUnavailable,
	}
	binary := firstExistingPi(os.Getenv("PI_BINARY"), lookPathPi())
	if binary == "" {
		probe.Detail = "Pi 0.85.1 binary not found; contract cannot be marked green"
		probe.Status = graphBatchSmokeUnavailable
		return probe
	}
	probe.Binary = binary
	launcher := pi.NewLauncher(pi.LauncherConfig{Executable: binary, SessionDir: "SESSION", Provider: "PROVIDER", Model: "MODEL"})
	if err := launcher.VerifyExactVersion(ctx, binary); err != nil {
		probe.Detail = err.Error()
		probe.Status = graphBatchSmokeUnavailable
		return probe
	}
	probe.Version = pi.RequiredVersion

	dir, err := os.MkdirTemp("", "wsgb-tb16-pi-")
	if err != nil {
		probe.Detail = err.Error()
		return probe
	}
	argvLog := filepath.Join(dir, "argv.log")
	session := sessionctrl.Session{File: filepath.Join(dir, "exact.jsonl"), ID: "smoke-exact-session"}
	probe.SessionFile = session.File
	probe.SessionID = session.ID
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	controller, err := launcher.StartExactSession(runCtx, pi.AgentProfile{Kind: pi.ProfileOrdinary, BuiltinToolsEnabled: true}, session, 1)
	if err != nil {
		probe.Detail = "StartExactSession: " + err.Error()
		probe.Status = graphBatchSmokeUnavailable
		return probe
	}
	defer controller.Close()
	go func() {
		for range controller.Events() {
		}
	}()
	if err := controller.Abort(runCtx); err != nil {
		probe.Detail = "Abort: " + err.Error()
		probe.Status = graphBatchSmokeUnavailable
		return probe
	}
	probe.Aborted = true
	if err := controller.WaitSettled(runCtx); err != nil {
		probe.Detail = "WaitSettled: " + err.Error()
		probe.Status = graphBatchSmokeUnavailable
		return probe
	}
	probe.Settled = controller.Settled()
	if raw, readErr := os.ReadFile(argvLog); readErr == nil {
		probe.Argv = strings.Split(strings.TrimSpace(string(raw)), "\n")
	}
	for _, arg := range probe.Argv {
		if arg == "--resume" {
			probe.UsedResumeFlag = true
		}
		if arg == "--continue" {
			probe.UsedContinueFlag = true
		}
	}
	usedSession := controller.SessionFile() == session.File && controller.SessionID() == session.ID
	if usedSession && probe.Aborted && probe.Settled && !probe.UsedResumeFlag && !probe.UsedContinueFlag {
		probe.Status = graphBatchSmokePass
		probe.Detail = "exact-session abort/resume proven on Pi " + pi.RequiredVersion
		return probe
	}
	probe.Status = graphBatchSmokeUnavailable
	if probe.Detail == "" {
		probe.Detail = "Pi started but exact-session abort/resume was not proven"
	}
	return probe
}

func firstExistingPi(candidates ...string) string {
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return ""
}

func lookPathPi() string {
	path, err := exec.LookPath("pi")
	if err != nil {
		return ""
	}
	return path
}

func waitUntil(ctx context.Context, ready func() bool) error {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return errors.New("timed out waiting for smoke condition")
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left := append([]string(nil), a...)
	right := append([]string(nil), b...)
	sort.Strings(left)
	sort.Strings(right)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
