package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/agentrun"
	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/effectsinterrupt"
	"river2.dev/pi-group-chat-host/internal/pi"
	"river2.dev/pi-group-chat-host/internal/pi/roombridge"
	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
	"river2.dev/pi-group-chat-host/internal/runtime"
)

const (
	deliveryStatusDelivered         = "delivered"
	deliveryStatusDeclined          = "declined"
	deliveryStatusTaskFinishedFirst = "task_finished_before_delivery"
	deliveryStatusEffectsUnknown    = "effects_unknown"
	graphBatchOpenSegmentID         = "seg-task-open"
)

type graphBatchHeldOutExecutor interface {
	Execute(ctx context.Context, digest string, retrieval graphBatchRetrieval, spec graphBatchHeldOutSpec, prior []graphBatchHeldOutTrace) (graphBatchHeldOutEpisodeResult, error)
}

type graphBatchHeldOutEpisodeResult struct {
	MemoryTerminal string
	TaskStatus     string
	FailureClass   string
	Record         *attemptRecord
}

type productionGraphBatchHeldOut struct {
	config armConfig
	ledger *pinnedLedger
}

func newProductionGraphBatchHeldOut(config armConfig) (*productionGraphBatchHeldOut, error) {
	ledger, err := loadPinned0917Ledger()
	if err != nil {
		return nil, err
	}
	if config.piBinary == "" || config.provider == "" || config.model == "" {
		return nil, fmt.Errorf("%s production held-out requires pi-binary, provider, and model", graphBatchStrategyID)
	}
	if config.extensionFile == "" {
		if config.outDir == "" {
			return nil, fmt.Errorf("%s production held-out requires --out-dir to write the room-bridge extension", graphBatchStrategyID)
		}
		path, err := roombridge.WriteTo(filepath.Join(config.outDir, "work-"+config.arm, "extension"))
		if err != nil {
			return nil, fmt.Errorf("write room bridge extension: %w", err)
		}
		config.extensionFile = path
	}
	return &productionGraphBatchHeldOut{config: config, ledger: ledger}, nil
}

func (p *productionGraphBatchHeldOut) Execute(ctx context.Context, digest string, retrieval graphBatchRetrieval, spec graphBatchHeldOutSpec, _ []graphBatchHeldOutTrace) (graphBatchHeldOutEpisodeResult, error) {
	if spec.Episode == nil {
		return graphBatchHeldOutEpisodeResult{}, fmt.Errorf("production held-out %s missing episode", spec.AttemptID)
	}
	episode := *spec.Episode
	if len(episode.Turns) == 0 {
		return graphBatchHeldOutEpisodeResult{}, fmt.Errorf("production held-out %s has no task prompt", spec.AttemptID)
	}
	session := runtime.NewSession()
	tenantID := sanitizeID(p.config.evaluationID) + "-" + p.config.arm + "-t"
	workDir := filepath.Join(p.config.outDir, "work-"+p.config.arm)
	roomID, shared, private, memoryPrivate := warmSkillIsolatedRoom(episode.FamilyID, spec.Sequence, episode.Split)
	episodeDir := filepath.Join(workDir, "episodes", fmt.Sprintf("%04d-%s", spec.Sequence, sanitizeID(episode.EpisodeID)))
	record := attemptRecord{
		RunID:        fmt.Sprintf("%s:%s:%d:%s", p.config.evaluationID, episode.TaskID, p.config.seed, p.config.arm),
		EvaluationID: p.config.evaluationID, Benchmark: episode.Benchmark, Domain: episode.Domain,
		TaskID: episode.TaskID, EpisodeID: episode.EpisodeID, FamilyID: episode.FamilyID,
		Role: episode.Role, Split: episode.Split, Arm: p.config.arm, DeliveryClass: graphBatchAuthorityGMS,
		RoomID: roomID, MemoryPolicy: p.config.policy, Seed: p.config.seed, Attempt: 1,
		WorkDir: episodeDir, FrozenSkillVersion: digest, TestWritePolicy: "throwaway_spaces",
	}
	if err := os.MkdirAll(episodeDir, 0o755); err != nil {
		return graphBatchHeldOutEpisodeResult{}, fmt.Errorf("episode work dir: %w", err)
	}
	taskPrompt := episode.Turns[0].Prompt
	if p.config.publishReminder != "" {
		taskPrompt += "\n\n" + p.config.publishReminder
	}
	timeout := p.config.episodeTimeout
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	task, err := newProductionTaskExactSession(productionTaskConfig{
		config: p.config, episodeDir: episodeDir, episodeID: episode.EpisodeID, firstPrompt: taskPrompt,
	})
	if err != nil {
		return graphBatchHeldOutEpisodeResult{}, err
	}
	defer task.Close()
	started := time.Now()
	err = runGraphBatchParallelHeldOut(runCtx, graphBatchParallelRequest{
		Record:     &record,
		AttemptID:  spec.AttemptID,
		RoomID:     roomID,
		Evaluation: p.config.evaluationID,
		RunID:      record.RunID,
		Task:       task,
		Memory: func(memCtx context.Context) graphBatchMemoryOutcome {
			if err := p.runProductionRetrieval(memCtx, session, tenantID, episodeDir, spec, episode, &record); err != nil {
				return graphBatchMemoryOutcome{Status: "error", Err: err}
			}
			return graphBatchMemoryOutcome{
				Status:     record.SkillRetrievalStatus,
				Selections: resolvePinnedSelections(selectionsFromRecord(record), p.ledger),
				Text:       record.SkillRetrievalText,
				Recall:     record.SkillRetrievalRecall,
			}
		},
		PrepareOffer: func(assembly graphBatchOfferAssembly) (string, *graphBatchSkillProtocol, error) {
			offerExt := filepath.Join(episodeDir, "task-skill-extension.mjs")
			if err := writeMemoryTurnExtension(p.config.extensionFile, offerExt, skillOfferToolsJS(assembly.Offers)); err != nil {
				return "", nil, err
			}
			return offerExt, newGraphBatchSkillProtocol(assembly.Offers), nil
		},
	})
	record.DurationSeconds = time.Since(started).Seconds()
	result := graphBatchHeldOutEpisodeResult{
		MemoryTerminal: concurrentepisode.TerminalNoCandidateFromEmptyGraph,
		TaskStatus:     record.Status,
		Record:         &record,
	}
	if err != nil && record.Status == "" {
		record.Status, record.FailureKind, record.Error = "failed", "infrastructure", truncate(err.Error(), 4000)
		result.TaskStatus = "failed"
		result.FailureClass = "task_error"
		return result, nil
	}
	if record.DeliveryStatus == deliveryStatusDeclined || record.SkillRetrievalStatus == "declined" || record.SkillRetrievalStatus == "no_reply" {
		result.FailureClass = concurrentepisode.FailureNormalNoSkill
		result.MemoryTerminal = concurrentepisode.TerminalNoApplicableCandidate
	}
	if record.DeliveryStatus == deliveryStatusEffectsUnknown {
		result.FailureClass = deliveryStatusEffectsUnknown
	}
	if record.DeliveryStatus == deliveryStatusTaskFinishedFirst {
		result.MemoryTerminal = deliveryStatusTaskFinishedFirst
	}
	_ = retrieval
	_ = shared
	_ = private
	_ = memoryPrivate
	return result, nil
}

func (p *productionGraphBatchHeldOut) runProductionRetrieval(ctx context.Context, session *runtime.Session, tenantID, episodeDir string, spec graphBatchHeldOutSpec, episode manifestEpisode, record *attemptRecord) error {
	retDir := filepath.Join(p.config.outDir, "work-"+p.config.arm, "retrieval",
		fmt.Sprintf("%04d-%s", spec.Sequence, sanitizeID(episode.EpisodeID)))
	if err := os.MkdirAll(retDir, 0o755); err != nil {
		return fmt.Errorf("retrieval work dir: %w", err)
	}
	extensionPath := filepath.Join(retDir, "retrieval-extension.mjs")
	ledger := p.ledger.asSkillProposals()
	if err := writeMemoryTurnExtension(p.config.extensionFile, extensionPath, skillsToolJS(ledger)); err != nil {
		return err
	}
	scratchRoom, scratchShared, scratchPrivate, _ := warmSkillRetrievalRoom(episode.FamilyID, episode.EpisodeID)
	retCtx, retCancel := context.WithTimeout(ctx, retrievalTurnDeadline(len(buildLedgerChunks(ledger))))
	defer retCancel()
	turn, err := session.Turn(retCtx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{
			TenantID: tenantID, RoomID: scratchRoom, AgentID: graphBatchMemoryAgentID, ProfileKind: "memory",
			WorkingDirectory: retDir, EnvironmentAllowlist: p.config.piEnv,
			Provider: p.config.provider, Model: p.config.model,
			SharedSpaceID: scratchShared, PrivateSpaceID: scratchPrivate,
			ExtraMemoryTools: []string{skillsListToolName},
		},
		RoomInput:       graphBatchRetrievalPromptHead + "\n" + episode.Turns[0].Prompt,
		HumanMessageID:  fmt.Sprintf("human-%04d-%s-retr", spec.Sequence, sanitizeID(episode.EpisodeID)),
		MemoryBaseURL:   p.config.gmsURL,
		MemoryAuthToken: p.config.gmsToken,
		PiBinary:        p.config.piBinary,
		PiExtensionPath: extensionPath,
		PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-retr", sanitizeID(p.config.evaluationID), p.config.arm, spec.Sequence, sanitizeID(episode.EpisodeID)),
		EventLogPath:    filepath.Join(episodeDir, "host-events.jsonl"),
		PiStderrPath:    filepath.Join(retDir, "pi-stderr.log"),
	})
	if err != nil {
		return err
	}
	record.SkillRetrievalRecall = turn.Recall.State
	reply := finalAgentOutput(turn.Messages, graphBatchMemoryAgentID)
	if reply == nil {
		record.SkillRetrievalStatus = "no_reply"
		return nil
	}
	record.SkillRetrievalText = reply
	selections := parseGraphBatchSkillSelections(*reply, p.ledger)
	if len(selections) == 0 {
		record.SkillRetrievalStatus = "declined"
		return nil
	}
	offers := make([]skillOfferAttempt, 0, len(selections))
	fingerprints := make([]string, 0, len(selections))
	for _, selection := range selections {
		skill, ok := p.ledger.lookup(selection.SkillReference)
		if !ok {
			skill, ok = p.ledger.lookup(selection.SHA256)
		}
		if !ok {
			continue
		}
		fingerprints = append(fingerprints, skill.SHA256)
		offers = append(offers, skillOfferAttempt{
			SkillReference: skill.SkillReference,
			SHA256:         skill.SHA256,
			Trigger:        skill.Trigger,
			Name:           skill.Name,
			BodyDigest:     skill.BodyDigest,
		})
	}
	if len(offers) == 0 {
		record.SkillRetrievalStatus = "declined"
		return nil
	}
	record.SkillRetrievalStatus = "published"
	record.SkillRetrievalCount = len(offers)
	record.SkillRetrievalSHA256 = strings.Join(fingerprints, ",")
	record.Offers = offers
	return nil
}

func (p *productionGraphBatchHeldOut) finishProtocol(record *attemptRecord, protocol *graphBatchSkillProtocol, namedText string) {
	if protocol == nil {
		return
	}
	offers, served, disposition, reason := protocol.snapshot(false)
	if len(offers) > 0 {
		record.Offers = offers
	}
	record.Served = served
	record.Disposition = disposition
	record.DispositionReason = reason
	record.NamedInReasoning = namedInReasoning(namedText, protocol.offers)
	if record.DeliveryStatus == deliveryStatusDelivered && len(record.Offers) > 0 && record.Disposition == "" {
		record.Disposition = "protocol_error"
		record.DispositionReason = "offer without terminal disposition"
	}
}

func selectionsFromRecord(record attemptRecord) []graphBatchSkillSelection {
	out := make([]graphBatchSkillSelection, 0, len(record.Offers))
	for _, offer := range record.Offers {
		out = append(out, graphBatchSkillSelection{
			SkillReference: offer.SkillReference,
			SHA256:         offer.SHA256,
			Trigger:        offer.Trigger,
		})
	}
	return out
}

func offerAttemptsFromAssembly(assembly graphBatchOfferAssembly) []skillOfferAttempt {
	out := make([]skillOfferAttempt, 0, len(assembly.Offers))
	for _, offer := range assembly.Offers {
		out = append(out, skillOfferAttempt{
			OfferID:        offer.OfferID,
			SkillReference: offer.SkillReference,
			SHA256:         offer.SHA256,
			Trigger:        offer.Trigger,
			Name:           offer.Name,
			BodyDigest:     offer.BodyDigest,
		})
	}
	return out
}

type graphBatchMemoryOutcome struct {
	Status     string
	Selections []pinnedSkill
	Text       *string
	Recall     string
	Err        error
}

type graphBatchParallelTask interface {
	directedoffer.ExactSession
	UnprovenMutating() bool
	EnteredRunning() <-chan struct{}
	BindResume(protocol *graphBatchSkillProtocol, prompt, extension string)
	VisibleMessages() []runtime.VisibleMessage
	RecallLeaked() bool
	UsedResumeFlag() bool
	UsedContinueFlag() bool
}

type graphBatchParallelRequest struct {
	Record        *attemptRecord
	AttemptID     string
	RoomID        string
	Evaluation    string
	RunID         string
	Task          graphBatchParallelTask
	Memory        func(context.Context) graphBatchMemoryOutcome
	PrepareOffer  func(graphBatchOfferAssembly) (string, *graphBatchSkillProtocol, error)
	InterruptWait time.Duration
}

func runGraphBatchParallelHeldOut(ctx context.Context, req graphBatchParallelRequest) error {
	if req.Record == nil || req.Task == nil || req.Memory == nil {
		return fmt.Errorf("parallel held-out requires record, task, and memory")
	}
	if req.InterruptWait <= 0 {
		req.InterruptWait = 50 * time.Millisecond
	}
	coord := agentrun.New(effectsinterrupt.Policy{InterruptGrace: req.InterruptWait, KillGrace: req.InterruptWait})
	barrier := newGraphBatchStartBarrier(2)
	memResult := make(chan graphBatchMemoryOutcome, 1)
	taskResult := make(chan error, 1)
	go func() {
		var outcome graphBatchMemoryOutcome
		_ = coord.Run(ctx, graphBatchMemoryAgentID, func() error {
			if err := barrier.Enter(ctx); err != nil {
				outcome = graphBatchMemoryOutcome{Status: "error", Err: err}
				return err
			}
			outcome = req.Memory(ctx)
			return outcome.Err
		})
		memResult <- outcome
	}()
	go func() {
		taskResult <- coord.Run(ctx, graphBatchTaskAgentID, func() error {
			if err := barrier.Enter(ctx); err != nil {
				return err
			}
			return req.Task.Start(ctx)
		})
	}()
	var memOutcome graphBatchMemoryOutcome
	select {
	case memOutcome = <-memResult:
	case <-ctx.Done():
		return ctx.Err()
	}
	req.Record.StartOverlap = barrier.Released()

	var (
		taskErr    error
		taskWaited bool
	)
	waitTask := func() error {
		if taskWaited {
			return taskErr
		}
		select {
		case taskErr = <-taskResult:
			taskWaited = true
			return taskErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	noteTaskFailure := func(err error) {
		if err == nil {
			return
		}
		req.Record.Status, req.Record.FailureKind, req.Record.Error = "failed", "task", truncate(err.Error(), 4000)
	}

	var protocol *graphBatchSkillProtocol
	switch {
	case memOutcome.Err != nil:
		req.Record.Status, req.Record.FailureKind, req.Record.Error = "failed", "infrastructure", truncate("skill retrieval turn: "+memOutcome.Err.Error(), 4000)
		_ = req.Task.Abort(ctx)
		_ = waitTask()
		return nil
	case memOutcome.Status != "published" || len(memOutcome.Selections) == 0:
		req.Record.DeliveryStatus = deliveryStatusDeclined
		_ = waitTask()
	default:
		select {
		case <-req.Task.EnteredRunning():
			if !req.Task.Running() {
				if err := waitTask(); err != nil {
					noteTaskFailure(err)
				} else {
					req.Record.DeliveryStatus = deliveryStatusTaskFinishedFirst
				}
				break
			}
			if req.Task.UnprovenMutating() {
				waitCtx, cancel := context.WithTimeout(ctx, req.InterruptWait)
				waitUnprovenMutating(waitCtx, req.Task)
				cancel()
			}
			if req.Task.UnprovenMutating() {
				req.Record.DeliveryStatus = deliveryStatusEffectsUnknown
				req.Record.EffectsUnknown = true
				req.Record.Status, req.Record.FailureKind, req.Record.Error = "failed", deliveryStatusEffectsUnknown, effectsinterrupt.ErrEffectsUnknown.Error()
				_ = req.Task.Abort(ctx)
				_ = waitTask()
				break
			}
			assembly, err := assembleDirectedSkillOffer(req.AttemptID, memOutcome.Selections)
			if err != nil {
				return err
			}
			resumePrompt := graphBatchTaskUptakePrompt(assembly, "")
			extension := ""
			if req.PrepareOffer != nil {
				extension, protocol, err = req.PrepareOffer(assembly)
				if err != nil {
					return err
				}
			} else {
				protocol = newGraphBatchSkillProtocol(assembly.Offers)
			}
			req.Task.BindResume(protocol, resumePrompt, extension)
			req.Record.Offers = offerAttemptsFromAssembly(assembly)
			if err := coord.AttachOffer(parallelTaskBinding(req, req.Task.Session()), req.Task); err != nil {
				return fmt.Errorf("attach exact task session: %w", err)
			}
			published, err := publishDirectedSkillOffer(ctx, coord.Offers(), req.RoomID, req.AttemptID, assembly)
			if err != nil {
				return fmt.Errorf("publish directed offer: %w", err)
			}
			if len(published.Deliveries) != 1 || published.Deliveries[0].AgentID != graphBatchTaskAgentID {
				return fmt.Errorf("directed offer must create exactly one task-agent delivery, got %#v", published.Deliveries)
			}
			if published.Segment.Reason == "" && !req.Task.Running() {
				req.Record.DeliveryStatus = deliveryStatusTaskFinishedFirst
			} else {
				req.Record.DeliveryStatus = deliveryStatusDelivered
				req.Record.ContinuationOf = published.Segment.ContinuationOf
				req.Record.SegmentReason = published.Segment.Reason
			}
			_ = waitTask()
		case err := <-taskResult:
			taskWaited = true
			taskErr = err
			if err != nil {
				noteTaskFailure(err)
			} else {
				req.Record.DeliveryStatus = deliveryStatusTaskFinishedFirst
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	obs := req.Task.Observation()
	req.Record.AbortCount = obs.AbortCount
	req.Record.ResumeCount = obs.ResumeCount
	req.Record.UsedResumeFlag = req.Task.UsedResumeFlag()
	req.Record.UsedContinueFlag = req.Task.UsedContinueFlag()
	if req.Record.UsedResumeFlag || req.Record.UsedContinueFlag {
		req.Record.Status, req.Record.FailureKind, req.Record.Error = "failed", "infrastructure", "exact-session resume used --resume or --continue"
		return nil
	}
	if req.Task.RecallLeaked() {
		req.Record.Status, req.Record.FailureKind, req.Record.Error = "failed", "infrastructure", "task prompt contained Memory from earlier sessions"
		return nil
	}
	if !taskWaited {
		_ = waitTask()
	}
	if req.Record.Status == "" {
		if taskErr != nil {
			req.Record.Status, req.Record.FailureKind, req.Record.Error = "failed", "task", truncate(taskErr.Error(), 4000)
		} else {
			req.Record.Status = "success"
		}
	}
	req.Record.RecallState = "skipped"
	req.Record.RecallCitations = 0
	settleText := strings.TrimSpace(req.Task.Observation().LastAssistantText)
	messages := req.Task.VisibleMessages()
	if protocol != nil {
		messages = protocol.PublishFeedback(messages)
	}
	if settleText != "" {
		messages = append(messages, runtime.VisibleMessage{AuthorID: graphBatchTaskAgentID, Content: settleText})
	}
	req.Record.Transcript = roomTranscript(messages)
	capture := captureTaskOutput(req.Record.WorkDir, messages)
	req.Record.FinalCodeOutput = capture.graded
	if settleText != "" {
		req.Record.FinalOutput = &settleText
	} else if capture.final != nil {
		req.Record.FinalOutput = capture.final
	}
	namedText := ""
	if req.Record.FinalOutput != nil {
		namedText = *req.Record.FinalOutput
	}
	for _, entry := range req.Record.Transcript {
		namedText += "\n" + entry.Content
	}
	if protocol != nil {
		(&productionGraphBatchHeldOut{}).finishProtocol(req.Record, protocol, namedText)
	}
	return nil
}

func waitUnprovenMutating(ctx context.Context, task graphBatchParallelTask) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !task.UnprovenMutating() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func parallelTaskBinding(req graphBatchParallelRequest, session sessionctrl.Session) directedoffer.Binding {
	return directedoffer.Binding{
		EvaluationID:     firstNonEmpty(req.Evaluation, "eval-graph-batch"),
		LogicalRunID:     firstNonEmpty(req.RunID, req.AttemptID),
		AttemptID:        req.AttemptID,
		RoomID:           req.RoomID,
		AgentID:          graphBatchTaskAgentID,
		Session:          session,
		SessionDir:       filepath.Dir(session.File),
		WorkDir:          firstNonEmpty(req.Record.WorkDir, filepath.Dir(session.File)),
		ProfileDigest:    digestString("ordinary"),
		ProviderDigest:   digestString("provider"),
		ModelDigest:      digestString("model"),
		ToolPolicyDigest: digestString("tools"),
		Generation:       1,
		SegmentID:        graphBatchOpenSegmentID,
	}
}

func digestString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

type graphBatchStartBarrier struct {
	n        int
	mu       sync.Mutex
	count    int
	ready    chan struct{}
	released bool
}

func newGraphBatchStartBarrier(n int) *graphBatchStartBarrier {
	return &graphBatchStartBarrier{n: n, ready: make(chan struct{})}
}

func (b *graphBatchStartBarrier) Released() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.released
}

func (b *graphBatchStartBarrier) Enter(ctx context.Context) error {
	b.mu.Lock()
	b.count++
	if b.count == b.n {
		b.released = true
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

type productionTaskConfig struct {
	config      armConfig
	episodeDir  string
	episodeID   string
	firstPrompt string
}

type productionTaskExactSession struct {
	cfg             productionTaskConfig
	session         sessionctrl.Session
	profile         pi.AgentProfile
	baseExt         string
	env             []string
	mu              sync.Mutex
	ctrl            *sessionctrl.Controller
	running         bool
	abortCount      int
	resumeCount     int
	inflightMut     int
	messages        []runtime.VisibleMessage
	recallLeak      bool
	usedResume      bool
	usedContinue    bool
	protocol        *graphBatchSkillProtocol
	resumePrompt    string
	resumeExt       string
	entered         chan struct{}
	enterOnce       sync.Once
	consumedSettled chan struct{}
	settled         bool
	lastAssistant   string
}

func newProductionTaskExactSession(cfg productionTaskConfig) (*productionTaskExactSession, error) {
	sessionDir := filepath.Join(cfg.episodeDir, "sessions")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		return nil, fmt.Errorf("task session dir: %w", err)
	}
	file := filepath.Join(sessionDir, "task.jsonl")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		return nil, fmt.Errorf("task session file: %w", err)
	}
	profile, err := pi.NewOrdinaryAgentProfile(cfg.episodeDir, cfg.config.piEnv)
	if err != nil {
		return nil, err
	}
	return &productionTaskExactSession{
		cfg:     cfg,
		session: sessionctrl.Session{File: file, ID: "exact-" + sanitizeID(cfg.episodeID)},
		profile: profile,
		baseExt: cfg.config.extensionFile,
		env:     graphBatchProcessEnv(cfg.config.piEnv),
		entered: make(chan struct{}),
	}, nil
}

func (s *productionTaskExactSession) AgentID() string                 { return graphBatchTaskAgentID }
func (s *productionTaskExactSession) Session() sessionctrl.Session    { return s.session }
func (s *productionTaskExactSession) EnteredRunning() <-chan struct{} { return s.entered }
func (s *productionTaskExactSession) UsedResumeFlag() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usedResume
}
func (s *productionTaskExactSession) UsedContinueFlag() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usedContinue
}
func (s *productionTaskExactSession) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}
func (s *productionTaskExactSession) UnprovenMutating() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflightMut > 0
}
func (s *productionTaskExactSession) RecallLeaked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recallLeak
}
func (s *productionTaskExactSession) VisibleMessages() []runtime.VisibleMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]runtime.VisibleMessage, len(s.messages))
	copy(out, s.messages)
	return out
}
func (s *productionTaskExactSession) BindResume(protocol *graphBatchSkillProtocol, prompt, extension string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protocol = protocol
	s.resumePrompt = prompt
	s.resumeExt = extension
}
func (s *productionTaskExactSession) Observation() directedoffer.SessionObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	phase := directedoffer.PhaseIdle
	if s.running {
		phase = directedoffer.PhaseRunning
	} else if s.resumeCount > 0 {
		phase = directedoffer.PhaseResumed
	} else if s.settled {
		phase = directedoffer.PhaseSettled
	}
	return directedoffer.SessionObservation{
		Phase: phase, Session: s.session, AbortCount: s.abortCount, ResumeCount: s.resumeCount,
		LastAssistantText: s.lastAssistant,
	}
}
func (s *productionTaskExactSession) Start(ctx context.Context) error {
	if err := s.launch(ctx, s.baseExt, 1); err != nil {
		return err
	}
	if err := s.prompt(ctx, s.cfg.firstPrompt); err != nil {
		return err
	}
	s.enterOnce.Do(func() { close(s.entered) })
	s.setRunning(true)
	return s.finishGeneration(ctx)
}
func (s *productionTaskExactSession) Abort(ctx context.Context) error {
	s.mu.Lock()
	s.abortCount++
	ctrl := s.ctrl
	s.mu.Unlock()
	if ctrl == nil {
		return nil
	}
	return ctrl.Abort(ctx)
}
func (s *productionTaskExactSession) WaitSettled(ctx context.Context) error {
	return s.waitSettled(ctx)
}
func (s *productionTaskExactSession) Resume(ctx context.Context, req directedoffer.ResumeRequest) error {
	if req.Session.File != s.session.File || req.Session.ID != s.session.ID {
		return errors.New("directed offer: resume must use the stored exact session file and id")
	}
	s.mu.Lock()
	s.resumeCount++
	prompt := s.resumePrompt
	if prompt == "" {
		prompt = s.cfg.firstPrompt
	}
	ext := s.resumeExt
	if ext == "" {
		ext = s.baseExt
	}
	s.mu.Unlock()
	for _, message := range req.Messages {
		if strings.TrimSpace(message.Content) != "" {
			prompt += "\n\n" + message.Content
		}
	}
	_ = s.closeCtrl()
	if err := s.launch(ctx, ext, 2); err != nil {
		return err
	}
	if err := s.prompt(ctx, prompt); err != nil {
		return err
	}
	s.setRunning(true)
	return s.finishGeneration(ctx)
}
func (s *productionTaskExactSession) finishGeneration(ctx context.Context) error {
	err := s.waitSettled(ctx)
	s.waitConsumed(ctx)
	s.observeLastAssistant()
	s.setRunning(false)
	return err
}
func (s *productionTaskExactSession) observeLastAssistant() {
	text := lastAssistantFromExactSessionFile(s.session.File)
	s.mu.Lock()
	defer s.mu.Unlock()
	if text != "" {
		s.lastAssistant = text
	}
	s.settled = true
}
func (s *productionTaskExactSession) Close() error { return s.closeCtrl() }
func (s *productionTaskExactSession) launch(ctx context.Context, extension string, generation uint64) error {
	stderrPath := filepath.Join(s.cfg.episodeDir, fmt.Sprintf("pi-task-%d-stderr.log", generation))
	stderr, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	launcher := pi.NewLauncher(pi.LauncherConfig{
		Executable: s.cfg.config.piBinary, SessionDir: filepath.Dir(s.session.File),
		Provider: s.cfg.config.provider, Model: s.cfg.config.model, ExtensionPath: extension,
		Environment: append([]string(nil), s.env...), Stderr: stderr,
	})
	ctrl, err := launcher.StartExactSession(ctx, s.profile, s.session, generation)
	if err != nil {
		_ = stderr.Close()
		return err
	}
	s.mu.Lock()
	s.ctrl = ctrl
	s.consumedSettled = make(chan struct{})
	s.mu.Unlock()
	go func() {
		s.consume(ctrl)
		_ = stderr.Close()
	}()
	return nil
}
func (s *productionTaskExactSession) prompt(ctx context.Context, message string) error {
	if strings.Contains(message, "Memory from earlier sessions") {
		s.mu.Lock()
		s.recallLeak = true
		s.mu.Unlock()
		return fmt.Errorf("task prompt contained Memory from earlier sessions")
	}
	s.mu.Lock()
	ctrl := s.ctrl
	s.mu.Unlock()
	if ctrl == nil {
		return errors.New("exact session controller is missing")
	}
	return ctrl.Prompt(ctx, message)
}
func (s *productionTaskExactSession) waitSettled(ctx context.Context) error {
	s.mu.Lock()
	ctrl := s.ctrl
	s.mu.Unlock()
	if ctrl == nil {
		return nil
	}
	return ctrl.WaitSettled(ctx)
}
func (s *productionTaskExactSession) closeCtrl() error {
	s.mu.Lock()
	ctrl := s.ctrl
	s.ctrl = nil
	s.mu.Unlock()
	if ctrl == nil {
		return nil
	}
	return ctrl.Close()
}
func (s *productionTaskExactSession) setRunning(running bool) {
	s.mu.Lock()
	s.running = running
	s.mu.Unlock()
}
func (s *productionTaskExactSession) waitConsumed(ctx context.Context) {
	s.mu.Lock()
	done := s.consumedSettled
	s.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	case <-time.After(200 * time.Millisecond):
	}
}
func (s *productionTaskExactSession) consume(ctrl *sessionctrl.Controller) {
	for event := range ctrl.Events() {
		s.handleEvent(event)
	}
}
func (s *productionTaskExactSession) handleEvent(event sessionctrl.Event) {
	var frame struct {
		Type       string          `json:"type"`
		ToolCallID string          `json:"toolCallId"`
		ToolName   string          `json:"toolName"`
		Args       json.RawMessage `json:"args"`
		Result     json.RawMessage `json:"result"`
		IsError    *bool           `json:"isError"`
	}
	if err := json.Unmarshal(event.Raw, &frame); err != nil {
		return
	}
	args := string(frame.Args)
	s.mu.Lock()
	protocol := s.protocol
	s.mu.Unlock()
	switch frame.Type {
	case "agent_settled":
		text := lastAssistantFromExactSessionFile(s.session.File)
		s.mu.Lock()
		if text != "" {
			s.lastAssistant = text
		}
		s.settled = true
		if s.consumedSettled != nil {
			select {
			case <-s.consumedSettled:
			default:
				close(s.consumedSettled)
			}
		}
		s.mu.Unlock()
	case "tool_execution_start":
		if protocol != nil {
			protocol.ObserveToolStart(frame.ToolName, frame.ToolCallID, args)
		}
		if isMutatingExactTool(frame.ToolName) {
			s.mu.Lock()
			s.inflightMut++
			s.mu.Unlock()
		}
		if frame.ToolName == "room_send" || frame.ToolName == "room_reply" {
			if content := protocolArg(args, "content"); content != "" {
				s.mu.Lock()
				s.messages = append(s.messages, runtime.VisibleMessage{AuthorID: graphBatchTaskAgentID, Content: content})
				s.mu.Unlock()
			}
		}
	case "tool_execution_end":
		isError := frame.IsError != nil && *frame.IsError
		if protocol != nil {
			protocol.ObserveToolEnd(frame.ToolName, frame.ToolCallID, isError, string(frame.Result))
		}
		if isMutatingExactTool(frame.ToolName) {
			s.mu.Lock()
			if s.inflightMut > 0 {
				s.inflightMut--
			}
			s.mu.Unlock()
		}
	}
}

func lastAssistantFromExactSessionFile(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	type sessionEntry struct {
		Type    string `json:"type"`
		Message struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	last := ""
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry sessionEntry
		if json.Unmarshal([]byte(line), &entry) != nil || entry.Message.Role != "assistant" {
			continue
		}
		if entry.Type != "" && entry.Type != "message" {
			continue
		}
		var text []string
		for _, part := range entry.Message.Content {
			if part.Type == "text" && strings.TrimSpace(part.Text) != "" {
				text = append(text, part.Text)
			}
		}
		if value := strings.TrimSpace(strings.Join(text, "\n")); value != "" {
			last = value
		}
	}
	return last
}

func isMutatingExactTool(name string) bool {
	switch name {
	case skillGetToolName, skillFeedbackToolName, "room_send", "room_reply", "room_react",
		"read", "grep", "glob", "ls", "memory_start", "memory_explore", "memory_submit", "memory_redirect":
		return false
	default:
		return name != ""
	}
}

func graphBatchProcessEnv(allowlist []string) []string {
	if len(allowlist) == 0 {
		return os.Environ()
	}
	out := make([]string, 0, len(allowlist))
	for _, name := range allowlist {
		out = append(out, name+"="+os.Getenv(name))
	}
	return out
}
