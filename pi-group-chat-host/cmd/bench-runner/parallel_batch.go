package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/runtime"
)

// parallelBatchJob carries the sequence assigned before any work starts.
// Results are always reduced in this order, never in goroutine completion order.
type parallelBatchJob[T any] struct {
	Sequence int
	Value    T
}

type parallelBatchResult[T any] struct {
	Sequence int
	Value    T
}

type parallelBatchHooks[Train, TrainResult, Diagnosis, DiagnosisResult, Test any] struct {
	RunTrain        func(context.Context, parallelBatchJob[Train]) TrainResult
	ReduceTrain     func([]parallelBatchResult[TrainResult]) ([]parallelBatchJob[Diagnosis], error)
	RunDiagnosis    func(context.Context, parallelBatchJob[Diagnosis]) DiagnosisResult
	ReduceDiagnosis func([]parallelBatchResult[DiagnosisResult]) error
	Consolidate     func() error
	RunTest         func(context.Context, parallelBatchJob[Test]) error
}

// coordinateParallelBatch is the protocol barrier. It intentionally has no
// runtime or persistence dependencies so its ordering can be tested without Pi
// or GMS. Worker callbacks must own all per-job state (notably Sessions).
func coordinateParallelBatch[Train, TrainResult, Diagnosis, DiagnosisResult, Test any](
	ctx context.Context,
	parallelism int,
	train []parallelBatchJob[Train],
	tests []parallelBatchJob[Test],
	hooks parallelBatchHooks[Train, TrainResult, Diagnosis, DiagnosisResult, Test],
) error {
	train = sortedBatchJobs(train)
	trainResults := runParallelBatchStage(ctx, train, parallelism, hooks.RunTrain)
	diagnosis, err := hooks.ReduceTrain(trainResults)
	if err != nil {
		return err
	}
	diagnosis = sortedBatchJobs(diagnosis)
	diagnosisResults := runParallelBatchStage(ctx, diagnosis, parallelism, hooks.RunDiagnosis)
	if err := hooks.ReduceDiagnosis(diagnosisResults); err != nil {
		return err
	}
	// This call site is deliberately singular: no worker can consolidate and
	// no test can start until both parallel stages and reducers have returned.
	if err := hooks.Consolidate(); err != nil {
		return err
	}
	// Tests are independent once consolidation has completed: every test owns
	// a one-shot Session, a preassigned sequence, and throwaway spaces, and
	// nothing mutates family state during the test phase. One failing test
	// must not starve the remaining held-out episodes, so the stage always
	// runs to completion and the first failure in sequence order is reported.
	testResults := runParallelBatchStage(ctx, sortedBatchJobs(tests), parallelism, func(ctx context.Context, job parallelBatchJob[Test]) error {
		return hooks.RunTest(ctx, job)
	})
	for _, result := range testResults {
		if result.Value != nil {
			return result.Value
		}
	}
	return nil
}

func sortedBatchJobs[T any](jobs []parallelBatchJob[T]) []parallelBatchJob[T] {
	out := append([]parallelBatchJob[T](nil), jobs...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out
}

func runParallelBatchStage[Job, Result any](ctx context.Context, jobs []parallelBatchJob[Job], parallelism int, run func(context.Context, parallelBatchJob[Job]) Result) []parallelBatchResult[Result] {
	results := make([]parallelBatchResult[Result], len(jobs))
	if parallelism < 1 {
		parallelism = 1
	}
	if parallelism > len(jobs) {
		parallelism = len(jobs)
	}
	var wait sync.WaitGroup
	jobsCh := make(chan int)
	for worker := 0; worker < parallelism; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobsCh {
				job := jobs[index]
				results[index] = parallelBatchResult[Result]{Sequence: job.Sequence, Value: run(ctx, job)}
			}
		}()
	}
	for index := range jobs {
		jobsCh <- index
	}
	close(jobsCh)
	wait.Wait()
	// jobs are sorted before dispatch, but sort again to make this helper safe
	// when used independently and to document the reducer contract.
	sort.SliceStable(results, func(i, j int) bool { return results[i].Sequence < results[j].Sequence })
	return results
}

type scheduledEpisode struct {
	episode  manifestEpisode
	sequence int
}

func preassignFamilyEpisodes(episodes []manifestEpisode, family string, next *int) []scheduledEpisode {
	ordered := episodesOfFamily(episodes, family)
	assigned := make([]scheduledEpisode, 0, len(ordered))
	for _, episode := range ordered {
		*next++
		assigned = append(assigned, scheduledEpisode{episode: episode, sequence: *next})
	}
	return assigned
}

type batchEpisodeResult struct {
	record     attemptRecord
	transcript []transcriptEntry
	err        error
}

type batchDiagnosisJob struct {
	trajectory savedTrajectory
	record     *attemptRecord
}

type batchDiagnosisResult struct {
	sequence     int
	episodeID    string
	reply        *string
	recallState  string
	recallCites  int
	workerStatus string
}

// batchArmEpisodes narrows the manifest for arms the batch coordinator runs
// without a train phase. Cold mirrors the official Vanilla baseline, so its
// sequence counter must advance only over test episodes, matching the
// test-1..test-N throwaway spaces runArm registers up front.
func batchArmEpisodes(config armConfig, episodes []manifestEpisode) []manifestEpisode {
	if config.arm != "cold" {
		return episodes
	}
	testOnly := make([]manifestEpisode, 0, len(episodes))
	for _, episode := range episodes {
		if episode.Split == "test" {
			testOnly = append(testOnly, episode)
		}
	}
	return testOnly
}

// runParallelSkillStrategyBatch coordinates the official two-phase protocol
// (parallel train → diagnosis → consolidation → parallel held-out test) for
// the warm-skill-batch arm. The cold arm rides the same coordinator with an
// empty train phase: it is the official Vanilla baseline shape, test tasks
// only, each in its own throwaway spaces with recall always empty.
func runParallelSkillStrategyBatch(ctx context.Context, config armConfig, tenantID, workDir, extensionPath string, familyOrder []string, emit func(attemptRecord)) error {
	ledger := map[string][]skillProposal{}
	sequence := 0
	batchEpisodes := batchArmEpisodes(config, config.episodes)
	for _, family := range familyOrder {
		// The family's episodes consume the global sequence counter from this
		// value; the train-scope cut derivation must see the same base.
		sequenceAtFamilyStart := sequence
		assigned := preassignFamilyEpisodes(batchEpisodes, family, &sequence)
		trainJobs := make([]parallelBatchJob[scheduledEpisode], 0, len(assigned))
		testJobs := make([]parallelBatchJob[scheduledEpisode], 0, len(assigned))
		for _, scheduled := range assigned {
			job := parallelBatchJob[scheduledEpisode]{Sequence: scheduled.sequence, Value: scheduled}
			if scheduled.episode.Split == "test" {
				testJobs = append(testJobs, job)
			} else {
				trainJobs = append(trainJobs, job)
			}
		}

		var trajectories []savedTrajectory
		var records []*attemptRecord
		frozenVersion := ""
		var frozenSnapshot *graphSnapshot
		consolidationRoomID, consolidationShared, _, consolidationMemoryPrivate := warmSkillConsolidationRoom(family)
		hooks := parallelBatchHooks[scheduledEpisode, batchEpisodeResult, batchDiagnosisJob, batchDiagnosisResult, scheduledEpisode]{
			RunTrain: func(ctx context.Context, job parallelBatchJob[scheduledEpisode]) batchEpisodeResult {
				// Session owns a process-global outbox drain. Sharing it between
				// concurrent rooms can consume another room's rows, so every worker
				// gets a one-shot Session.
				return executeBatchEpisode(ctx, runtime.NewSession(), config, tenantID, workDir, extensionPath, job.Value, nil, nil)
			},
			ReduceTrain: func(results []parallelBatchResult[batchEpisodeResult]) ([]parallelBatchJob[batchDiagnosisJob], error) {
				var diagnosis []parallelBatchJob[batchDiagnosisJob]
				for _, result := range results {
					if result.Value.err != nil {
						return nil, result.Value.err
					}
					record := result.Value.record
					record.ConsolidationRoomID = consolidationRoomID
					recordCopy := new(attemptRecord)
					*recordCopy = record
					records = append(records, recordCopy)
					if len(result.Value.transcript) == 0 {
						continue
					}
					saved := savedTrajectory{Sequence: result.Sequence, EpisodeID: record.EpisodeID, TaskID: record.TaskID, RoomID: record.RoomID, Transcript: result.Value.transcript}
					// Persistence is intentionally reducer-owned and sequence ordered.
					if err := appendSavedTrajectory(config.outDir, config.arm, family, saved); err != nil {
						return nil, fmt.Errorf("save train trajectory %s: %w", saved.EpisodeID, err)
					}
					if err := saveFirstTrialTrajectory(config.outDir, firstTrialCacheScope(skillStrategyBatch), family, saved); err != nil {
						return nil, fmt.Errorf("save first train trial %s: %w", saved.EpisodeID, err)
					}
					trajectories = append(trajectories, saved)
					diagnosis = append(diagnosis, parallelBatchJob[batchDiagnosisJob]{Sequence: result.Sequence, Value: batchDiagnosisJob{trajectory: saved, record: recordCopy}})
				}
				return diagnosis, nil
			},
			RunDiagnosis: func(ctx context.Context, job parallelBatchJob[batchDiagnosisJob]) batchDiagnosisResult {
				roomID, shared, _, memoryPrivate := warmSkillDiagnosisRoom(family, job.Sequence)
				return executeBatchDiagnosis(ctx, runtime.NewSession(), diagnosisInput{
					config: config, tenantID: tenantID, roomID: roomID, sharedSpaceID: shared,
					memoryPrivateSpace: memoryPrivate, family: family, sequence: job.Sequence,
					episodeID: job.Value.trajectory.EpisodeID, extensionPath: extensionPath,
					transcript: job.Value.trajectory.Transcript,
				})
			},
			ReduceDiagnosis: func(results []parallelBatchResult[batchDiagnosisResult]) error {
				recordByEpisode := make(map[string]*attemptRecord, len(records))
				for _, record := range records {
					recordByEpisode[record.EpisodeID] = record
				}
				for _, result := range results {
					record := recordByEpisode[result.Value.episodeID]
					if record == nil {
						continue
					}
					if err := reduceBatchDiagnosis(config, family, result.Value, &ledger, record); err != nil {
						return err
					}
				}
				for _, record := range records {
					if !hasTrajectory(trajectories, record.EpisodeID) {
						record.SkillProposalStatus = "no_trajectory"
						record.SkillProposalsTotal = len(ledger[family])
					}
				}
				return nil
			},
			Consolidate: func() error {
				if config.arm == "cold" {
					// Vanilla baseline: nothing was trained, so there is no
					// skill to merge and no train graph to freeze.
					return nil
				}
				input := consolidationPhaseInput{config: config, forceMerge: true, tenantID: tenantID, family: family, extensionPath: extensionPath, trajectories: trajectories, records: records, ledger: &ledger}
				rawCount := len(ledger[family])
				// Exactly one serialized merge Session, and it is never reused by a
				// diagnosis or test room.
				status, sourceMap := runConsolidationTurn(ctx, runtime.NewSession(), input, consolidationRoomID, consolidationShared, consolidationMemoryPrivate)
				frozenVersion = frozenSkillVersion(ledger[family])
				// Freeze the family's train-scope GMS rooms into one version
				// vector before the first test task starts. A freeze failure
				// downgrades to a warning: the ledger is already frozen, and
				// tests simply run without the pinned snapshot.
				if armFreezesGraph(config.arm) {
					next := sequenceAtFamilyStart
					rooms := trainScopeRoomsForFamily(config.episodes, family, &next, skillStrategyBatch)
					snapshot, err := freezeGraphSnapshot(ctx, snapshotCutClient(config.gmsURL, config.gmsToken), tenantID, snapshotKeyPrefix(config, family), rooms)
					if err != nil {
						fmt.Printf("[%s] graph snapshot %s warning: %v\n", config.arm, family, err)
					} else {
						frozenSnapshot = &snapshot
					}
				}
				for _, record := range records {
					record.FrozenSkillVersion = frozenVersion
					record.ConsolidationRoomID = consolidationRoomID
				}
				if err := persistConsolidationSummary(config, family, consolidationRoomID, trajectories, rawCount, len(ledger[family]), status, frozenVersion, sourceMap, frozenSnapshot); err != nil {
					fmt.Printf("[%s] consolidation %s warning: %v\n", config.arm, family, err)
				}
				for _, record := range records {
					emit(*record)
				}
				return nil
			},
			RunTest: func(ctx context.Context, job parallelBatchJob[scheduledEpisode]) error {
				result := executeBatchEpisode(ctx, runtime.NewSession(), config, tenantID, workDir, extensionPath, job.Value, ledger[family], frozenSnapshot)
				if result.err != nil {
					return result.err
				}
				result.record.TestWritePolicy = "disposable_spaces_only"
				if config.arm != "cold" {
					result.record.FrozenSkillVersion = frozenVersion
					result.record.ConsolidationRoomID = consolidationRoomID
					if frozenSnapshot != nil {
						result.record.GraphSnapshot = frozenSnapshot
					}
				}
				emit(result.record)
				return nil
			},
		}
		if err := coordinateParallelBatch(ctx, config.batchParallelism, trainJobs, testJobs, hooks); err != nil {
			return err
		}
	}
	return nil
}

func executeBatchEpisode(ctx context.Context, session *runtime.Session, config armConfig, tenantID, workDir, extensionPath string, scheduled scheduledEpisode, ledger []skillProposal, snapshot *graphSnapshot) batchEpisodeResult {
	episode, sequence := scheduled.episode, scheduled.sequence
	roomID, shared, private, memoryPrivate := warmSkillIsolatedRoom(episode.FamilyID, sequence, episode.Split)
	episodeDir := filepath.Join(workDir, "episodes", fmt.Sprintf("%04d-%s", sequence, sanitizeID(episode.EpisodeID)))
	record := attemptRecord{
		RunID: fmt.Sprintf("%s:%s:%d:%s", config.evaluationID, episode.TaskID, config.seed, config.arm), EvaluationID: config.evaluationID,
		Benchmark: episode.Benchmark, Domain: episode.Domain, TaskID: episode.TaskID, EpisodeID: episode.EpisodeID, FamilyID: episode.FamilyID,
		Role: episode.Role, Arm: config.arm, RoomID: roomID, MemoryPolicy: config.policy, Seed: config.seed, Attempt: 1,
		WorkDir: episodeDir, PublishReminder: config.publishReminder != "",
	}
	if err := os.MkdirAll(episodeDir, 0o755); err != nil {
		return batchEpisodeResult{record: record, err: fmt.Errorf("episode work dir: %w", err)}
	}
	startTiming := time.Now()
	staged, err := stageReferenceFiles(config.refsRoot, episodeDir, episode.StageFiles)
	record.StagedFiles = staged
	if err != nil {
		record.Status, record.FailureKind, record.Error = "failed", "infrastructure", truncate(err.Error(), 4000)
		record.DurationSeconds = time.Since(startTiming).Seconds()
		return batchEpisodeResult{record: record}
	}
	if episode.Split == "test" && len(ledger) > 0 {
		localLedger := map[string][]skillProposal{episode.FamilyID: ledger}
		if err := runRetrievalTurn(ctx, session, retrievalInput{config: config, tenantID: tenantID, roomID: roomID, sharedSpaceID: shared, privateSpaceID: memoryPrivate, family: episode.FamilyID, sequence: sequence, episodeID: episode.EpisodeID, episodeDir: episodeDir, extensionPath: extensionPath, taskPrompt: episode.Turns[0].Prompt, ledger: &localLedger, record: &record}); err != nil {
			record.Status, record.FailureKind, record.Error = "failed", "infrastructure", truncate("skill retrieval turn: "+err.Error(), 4000)
			return batchEpisodeResult{record: record}
		}
	}

	started := time.Now()
	episodeCtx, cancel := context.WithTimeout(ctx, config.episodeTimeout)
	defer cancel()
	// Held-out episodes read the frozen train graph through the pinned
	// snapshot versions; train episodes read only their own isolated spaces.
	recallSpaceIDs := []string{shared, private}
	var recallSpaceVersions map[string]int64
	if snapshot != nil && episode.Split == "test" {
		recallSpaceIDs = append(append([]string(nil), recallSpaceIDs...), snapshot.sortedSpaces()...)
		recallSpaceVersions = snapshot.Spaces
	}
	base := func(turnIndex int) runtime.TurnRequest {
		return runtime.TurnRequest{
			Authority:     runtime.ExecutionAuthority{TenantID: tenantID, RoomID: roomID, AgentID: "agent-primary", ProfileKind: "ordinary", WorkingDirectory: episodeDir, EnvironmentAllowlist: config.piEnv, Provider: config.provider, Model: config.model, SharedSpaceID: shared, PrivateSpaceID: private, RecallSpaceIDs: recallSpaceIDs, RecallSpaceVersions: recallSpaceVersions},
			MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken, PiBinary: config.piBinary, PiExtensionPath: extensionPath,
			PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-t%d", sanitizeID(config.evaluationID), config.arm, sequence, sanitizeID(episode.EpisodeID), turnIndex), EventLogPath: filepath.Join(episodeDir, "host-events.jsonl"), PiStderrPath: filepath.Join(episodeDir, "pi-stderr.log"),
		}
	}
	var turn runtime.TurnResult
	var transcript []transcriptEntry
	var termination string
	var textFallbacks int
	if episode.UserSimulator != nil {
		if config.userSimURL == "" || config.userSimModel == "" {
			record.Status, record.FailureKind = "failed", "infrastructure"
			record.Error = "episode declares user_simulator; pass --user-sim-url and --user-sim-model"
			record.DurationSeconds = time.Since(started).Seconds()
			return batchEpisodeResult{record: record}
		}
		turn, transcript, termination, textFallbacks, err = runDualControlEpisode(episodeCtx, session, dualControlInput{episode: episode, config: config, base: base, episodeDir: episodeDir, sequence: sequence})
	} else {
		request := base(0)
		request.RoomInput = episode.Turns[0].Prompt
		if config.publishReminder != "" {
			request.RoomInput += "\n\n" + config.publishReminder
		}
		request.HumanMessageID = fmt.Sprintf("human-%04d-%s", sequence, sanitizeID(episode.EpisodeID))
		turn, err = session.Turn(episodeCtx, request)
		if err == nil {
			transcript = roomTranscript(turn.Messages)
		}
	}
	if err != nil {
		record.Status, record.FailureKind, record.Error = "failed", "task", truncate(err.Error(), 4000)
		record.DurationSeconds = time.Since(started).Seconds()
		return batchEpisodeResult{record: record}
	}
	record.Status, record.DurationSeconds = "success", time.Since(started).Seconds()
	record.RecallState, record.RecallCitations = turn.Recall.State, len(turn.Recall.Citations)
	record.Transcript, record.Termination, record.TextFallbacks = transcript, termination, textFallbacks
	for _, entry := range transcript {
		if entry.Role == "user" {
			record.UserTurns++
		}
	}
	if len(turn.Recall.Items) > 0 {
		record.RecallSpaces = map[string]int{}
		for _, item := range turn.Recall.Items {
			record.RecallSpaces[item.SourceSpaceID]++
		}
	}
	capture := captureTaskOutput(episodeDir, turn.Messages)
	record.FinalCodeOutput = capture.graded
	if capture.final != nil {
		record.FinalOutput = capture.final
		if capture.fromSession {
			transcript = append(transcript, transcriptEntry{Role: "agent", Content: *capture.final})
			record.Transcript = transcript
		}
	}
	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	_, drainErr := session.DrainEvidence(drainCtx, runtime.DrainRequest{RoomID: roomID, MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken, EventLogPath: filepath.Join(episodeDir, "host-events.jsonl")})
	drainCancel()
	if drainErr != nil {
		record.Status, record.FailureKind, record.Error = "failed", "infrastructure", truncate("drain evidence: "+drainErr.Error(), 4000)
		return batchEpisodeResult{record: record}
	}
	if episode.UserSimulator != nil && config.sidecarURL != "" && episode.Split == "test" {
		closeSidecarEnv(ctx, config.sidecarURL, episode.EpisodeID, episodeDir)
	}
	return batchEpisodeResult{record: record, transcript: transcript}
}

func warmSkillDiagnosisRoom(family string, sequence int) (roomID, shared, private, memoryPrivate string) {
	id := sanitizeID(family) + "-diagnosis-" + strconv.Itoa(sequence)
	return "room-" + id, "space-" + id + "-shared", "space-" + id + "-private", "space-" + id + "-memory-private"
}

func executeBatchDiagnosis(ctx context.Context, session *runtime.Session, input diagnosisInput) batchDiagnosisResult {
	result := batchDiagnosisResult{sequence: input.sequence, episodeID: input.episodeID}
	config := input.config
	diagDir := filepath.Join(config.outDir, "work-"+config.arm, "diagnosis", fmt.Sprintf("%04d-%s", input.sequence, sanitizeID(input.episodeID)))
	if err := os.MkdirAll(diagDir, 0o755); err != nil {
		result.workerStatus = "error"
		return result
	}
	chunks, err := json.Marshal(buildTrajectoryChunks(input.transcript))
	if err != nil || os.WriteFile(filepath.Join(diagDir, "trajectory-chunks.json"), chunks, 0o644) != nil {
		result.workerStatus = "error"
		return result
	}
	extensionPath := filepath.Join(diagDir, "diagnosis-extension.mjs")
	if err := writeMemoryTurnExtension(input.extensionPath, extensionPath, trajectoryToolJS(buildTrajectoryChunks(input.transcript))); err != nil {
		result.workerStatus = "error"
		return result
	}
	diagCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	turn, err := session.Turn(diagCtx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{TenantID: input.tenantID, RoomID: input.roomID, AgentID: "agent-memory", ProfileKind: "memory", WorkingDirectory: diagDir, EnvironmentAllowlist: config.piEnv, Provider: config.provider, Model: config.model, SharedSpaceID: input.sharedSpaceID, PrivateSpaceID: input.memoryPrivateSpace, ExtraMemoryTools: []string{trajectoryReadToolName}},
		RoomInput: diagnosisPromptFor(input.episodeID) + compactTrajectoryFallback(input.transcript), HumanMessageID: fmt.Sprintf("human-%04d-%s-diag", input.sequence, sanitizeID(input.episodeID)), MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken, PiBinary: config.piBinary, PiExtensionPath: extensionPath,
		PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-diag", sanitizeID(config.evaluationID), config.arm, input.sequence, sanitizeID(input.episodeID)), EventLogPath: filepath.Join(diagDir, "host-events.jsonl"), PiStderrPath: filepath.Join(diagDir, "pi-stderr.log"),
	})
	if err != nil {
		result.workerStatus = "error"
		return result
	}
	result.reply = finalAgentOutput(turn.Messages, "agent-memory")
	result.recallState, result.recallCites = turn.Recall.State, len(turn.Recall.Citations)
	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	_, err = session.DrainEvidence(drainCtx, runtime.DrainRequest{RoomID: input.roomID, MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken, EventLogPath: filepath.Join(diagDir, "host-events.jsonl")})
	drainCancel()
	if err != nil {
		result.workerStatus = "error"
	}
	return result
}

func reduceBatchDiagnosis(config armConfig, family string, result batchDiagnosisResult, ledger *map[string][]skillProposal, record *attemptRecord) error {
	defer func() { record.SkillProposalsTotal = len((*ledger)[family]) }()
	if result.workerStatus == "error" {
		record.SkillProposalStatus = "error"
		return nil
	}
	if result.reply == nil {
		record.SkillProposalStatus = "no_reply"
		return nil
	}
	proposals := splitSkillProposals(*result.reply)
	fingerprints := make([]string, 0, len(proposals))
	added := make([]skillProposal, 0, len(proposals))
	for _, text := range proposals {
		fingerprint := skillFingerprint(text)
		fingerprints = append(fingerprints, fingerprint)
		if !skillLedgerHas(*ledger, family, fingerprint) {
			added = append(added, skillProposal{Sequence: result.sequence, EpisodeID: result.episodeID, SHA256: fingerprint, Text: text})
		}
	}
	record.SkillProposalSHA256 = strings.Join(fingerprints, ",")
	switch {
	case len(proposals) == 0:
		record.SkillProposalStatus = "declined"
	case len(added) == 0:
		record.SkillProposalStatus = "duplicate"
	default:
		record.SkillProposalStatus, record.SkillProposalCount = "published", len(added)
		skillsDir := filepath.Join(config.outDir, "work-"+config.arm, "skills")
		if err := os.MkdirAll(skillsDir, 0o755); err != nil {
			return fmt.Errorf("skill proposal dir: %w", err)
		}
		var saved strings.Builder
		for _, proposal := range added {
			record.SkillProposalBytes += len(proposal.Text)
			(*ledger)[family] = append((*ledger)[family], proposal)
			saved.WriteString(proposal.Text)
			saved.WriteString("\n\n")
		}
		if err := os.WriteFile(filepath.Join(skillsDir, fmt.Sprintf("%04d-%s.md", result.sequence, sanitizeID(result.episodeID))), []byte(strings.TrimSpace(saved.String())+"\n"), 0o644); err != nil {
			return fmt.Errorf("record skill proposal: %w", err)
		}
	}
	record.SkillProposalsTotal = len((*ledger)[family])
	fmt.Printf("[%s] diagnosis %04d recall=%s(%d) %s total=%d\n", config.arm, result.sequence, result.recallState, result.recallCites, record.SkillProposalStatus, record.SkillProposalsTotal)
	return nil
}

func persistConsolidationSummary(config armConfig, family, roomID string, trajectories []savedTrajectory, rawCount, consolidatedCount int, status, version string, sourceMap map[string][]string, snapshot *graphSnapshot) error {
	trainRoomIDs, seen := make([]string, 0, len(trajectories)), map[string]bool{}
	for _, trajectory := range trajectories {
		if trajectory.RoomID != "" && !seen[trajectory.RoomID] {
			trainRoomIDs = append(trainRoomIDs, trajectory.RoomID)
			seen[trajectory.RoomID] = true
		}
	}
	summary := consolidationSummary{Family: family, ConsolidationRoomID: roomID, TrainRoomIDs: trainRoomIDs, RawSkillCount: rawCount, ConsolidatedSkillCount: consolidatedCount, Status: status, FrozenSkillVersion: version, ConsolidatedSourceMap: sourceMap, GraphSnapshot: snapshot}
	dir := filepath.Join(config.outDir, "work-"+config.arm, "consolidation")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, sanitizeID(family)+"-summary.json"), append(body, '\n'), 0o644)
}
