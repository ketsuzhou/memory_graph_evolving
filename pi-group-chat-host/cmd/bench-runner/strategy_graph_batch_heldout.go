package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/pi/roombridge"
	"river2.dev/pi-group-chat-host/internal/runtime"
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
		RunID: fmt.Sprintf("%s:%s:%d:%s", p.config.evaluationID, episode.TaskID, p.config.seed, p.config.arm),
		EvaluationID: p.config.evaluationID, Benchmark: episode.Benchmark, Domain: episode.Domain,
		TaskID: episode.TaskID, EpisodeID: episode.EpisodeID, FamilyID: episode.FamilyID,
		Role: episode.Role, Split: episode.Split, Arm: p.config.arm, DeliveryClass: graphBatchAuthorityGMS,
		RoomID: roomID, MemoryPolicy: p.config.policy, Seed: p.config.seed, Attempt: 1,
		WorkDir: episodeDir, FrozenSkillVersion: digest, TestWritePolicy: "throwaway_spaces",
	}
	if err := os.MkdirAll(episodeDir, 0o755); err != nil {
		return graphBatchHeldOutEpisodeResult{}, fmt.Errorf("episode work dir: %w", err)
	}
	started := time.Now()
	if err := p.runProductionRetrieval(ctx, session, tenantID, episodeDir, spec, episode, &record); err != nil {
		record.Status, record.FailureKind, record.Error = "failed", "infrastructure", truncate("skill retrieval turn: "+err.Error(), 4000)
		record.DurationSeconds = time.Since(started).Seconds()
		return graphBatchHeldOutEpisodeResult{TaskStatus: "failed", FailureClass: "retrieval_error", Record: &record}, nil
	}

	taskPrompt := episode.Turns[0].Prompt
	if p.config.publishReminder != "" {
		taskPrompt += "\n\n" + p.config.publishReminder
	}
	extensionPath := p.config.extensionFile
	var protocol *graphBatchSkillProtocol
	roomInput := taskPrompt
	selected := resolvePinnedSelections(selectionsFromRecord(record), p.ledger)
	if record.SkillRetrievalStatus == "published" && len(selected) > 0 {
		assembly, err := assembleDirectedSkillOffer(spec.AttemptID, selected)
		if err != nil {
			return graphBatchHeldOutEpisodeResult{}, err
		}
		published, err := publishDirectedSkillOffer(ctx, directedoffer.NewCoordinator(), roomID, spec.AttemptID, assembly)
		if err != nil {
			return graphBatchHeldOutEpisodeResult{}, fmt.Errorf("publish directed offer: %w", err)
		}
		if len(published.Deliveries) != 1 || published.Deliveries[0].AgentID != graphBatchTaskAgentID {
			return graphBatchHeldOutEpisodeResult{}, fmt.Errorf("directed offer must create exactly one task-agent delivery, got %#v", published.Deliveries)
		}
		offerExt := filepath.Join(episodeDir, "task-skill-extension.mjs")
		if err := writeMemoryTurnExtension(p.config.extensionFile, offerExt, skillOfferToolsJS(assembly.Offers)); err != nil {
			return graphBatchHeldOutEpisodeResult{}, err
		}
		extensionPath = offerExt
		protocol = newGraphBatchSkillProtocol(assembly.Offers)
		roomInput = graphBatchTaskUptakePrompt(assembly, taskPrompt)
		record.Offers = offerAttemptsFromAssembly(assembly)
	}

	timeout := p.config.episodeTimeout
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	turn, err := session.Turn(taskCtx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{
			TenantID: tenantID, RoomID: roomID, AgentID: graphBatchTaskAgentID, ProfileKind: "ordinary",
			WorkingDirectory: episodeDir, EnvironmentAllowlist: p.config.piEnv,
			Provider: p.config.provider, Model: p.config.model,
			SharedSpaceID: shared, PrivateSpaceID: private,
			ExtraOrdinaryTools: []string{skillGetToolName, skillFeedbackToolName},
		},
		RoomInput:       roomInput,
		HumanMessageID:  fmt.Sprintf("human-%04d-%s", spec.Sequence, sanitizeID(episode.EpisodeID)),
		MemoryBaseURL:   p.config.gmsURL,
		MemoryAuthToken: p.config.gmsToken,
		PiBinary:        p.config.piBinary,
		PiExtensionPath: extensionPath,
		PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-t0", sanitizeID(p.config.evaluationID), p.config.arm, spec.Sequence, sanitizeID(episode.EpisodeID)),
		EventLogPath:    filepath.Join(episodeDir, "host-events.jsonl"),
		PiStderrPath:    filepath.Join(episodeDir, "pi-stderr.log"),
		SkipRecall:      true,
		SkillProtocol:   protocol,
	})
	if err != nil {
		record.Status, record.FailureKind, record.Error = "failed", "task", truncate(err.Error(), 4000)
		record.DurationSeconds = time.Since(started).Seconds()
		p.finishProtocol(&record, protocol, "")
		return graphBatchHeldOutEpisodeResult{TaskStatus: "failed", FailureClass: "task_error", Record: &record}, nil
	}
	if strings.Contains(turn.Prompt, "Memory from earlier sessions") {
		record.Status, record.FailureKind, record.Error = "failed", "infrastructure", "task prompt contained Memory from earlier sessions"
		record.DurationSeconds = time.Since(started).Seconds()
		return graphBatchHeldOutEpisodeResult{TaskStatus: "failed", FailureClass: "recall_leak", Record: &record}, nil
	}
	record.Status, record.DurationSeconds = "success", time.Since(started).Seconds()
	record.RecallState, record.RecallCitations = turn.Recall.State, len(turn.Recall.Citations)
	record.Transcript = roomTranscript(turn.Messages)
	capture := captureTaskOutput(episodeDir, turn.Messages)
	record.FinalCodeOutput = capture.graded
	if capture.final != nil {
		record.FinalOutput = capture.final
	}
	namedText := ""
	if capture.final != nil {
		namedText = *capture.final
	}
	for _, entry := range record.Transcript {
		namedText += "\n" + entry.Content
	}
	p.finishProtocol(&record, protocol, namedText)
	result := graphBatchHeldOutEpisodeResult{
		MemoryTerminal: concurrentepisode.TerminalNoCandidateFromEmptyGraph,
		TaskStatus:     record.Status,
		Record:         &record,
	}
	if record.SkillRetrievalStatus == "declined" || record.SkillRetrievalStatus == "no_reply" {
		result.FailureClass = concurrentepisode.FailureNormalNoSkill
	}
	_ = retrieval
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
	if len(record.Offers) > 0 && record.Disposition == "" {
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
