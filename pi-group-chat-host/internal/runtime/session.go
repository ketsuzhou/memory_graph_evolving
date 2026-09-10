package runtime

import (
	"context"
	"net/http"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/memoryclient"
	memorystore "river2.dev/pi-group-chat-host/internal/store/memory"
)

// Session is the long-lived form of the Host turn path: one orchestrator over
// one durable store instance, so evidence outbox entries accumulate across
// turns and callers decide when to drain them into Graph Memory. Evaluation
// runners use it to replay multi-episode scenarios where memory written by an
// early episode must be recall-visible to later episodes; each Turn still
// spawns a fresh Pi process, keeping per-episode context session-fresh.
type Session struct {
	orchestrator *Orchestrator
	store        *memorystore.Store
}

// NewSession creates a session with the production clock and random ID source.
func NewSession() *Session {
	store := memorystore.NewStore()
	return &Session{
		orchestrator: NewOrchestrator(Dependencies{
			Clock:         systemClock{},
			IDs:           &randomIDs{},
			RoomStore:     store,
			DeliveryStore: store,
			DAGStore:      store,
			OutboxStore:   store.Outbox(),
		}),
		store: store,
	}
}

// Turn executes one agent turn against the session's durable store. The
// evidence outbox entries it produces stay pending until DrainEvidence.
// EventLogPath and PiStderrPath from the request stay off unless set, so
// existing callers keep the old silent behavior.
func (s *Session) Turn(ctx context.Context, request TurnRequest) (TurnResult, error) {
	log, closeLog, err := openTurnLog(request.EventLogPath)
	if err != nil {
		return TurnResult{}, err
	}
	defer closeLog()
	client := memoryclient.NewClient(request.MemoryBaseURL, request.MemoryAuthToken, &http.Client{Timeout: 5 * time.Second}, 1<<20)
	trace, outcome, err := s.orchestrator.executeTurn(ctx, turnInput{
		authority:       request.Authority,
		roomInput:       request.RoomInput,
		humanMessageID:  request.HumanMessageID,
		humanKey:        request.HumanMessageID,
		memoryClient:    client,
		performRecall:   true,
		piBinary:        request.PiBinary,
		piExtensionPath: request.PiExtensionPath,
		promptRequestID: request.PromptRequestID,
		log:             log,
		piStderrPath:    request.PiStderrPath,
	})
	if err != nil {
		return TurnResult{}, err
	}
	return TurnResult{
		ResolvedAuthority: request.Authority,
		Recall:            trace.recall,
		PromptSent:        trace.promptSent,
		Prompt:            trace.prompt,
		Messages:          trace.visibleMessages,
		Delivery:          outcome.delivery,
		Segment:           outcome.segment,
		Outbox:            outboxRecords(s.store.Entries()),
	}, nil
}

// DrainRequest identifies the room whose evidence is drained, the Memory
// Protocol endpoint, and optionally the same event log file the room's turns
// wrote, so a drain's stage/commit mechanics append to the episode history.
type DrainRequest struct {
	RoomID          string
	MemoryBaseURL   string
	MemoryAuthToken string
	EventLogPath    string
}

// DrainEvidence stages and commits every pending evidence entry of one room,
// returning the committed batch IDs. It is the recall-visibility boundary
// between episodes.
func (s *Session) DrainEvidence(ctx context.Context, request DrainRequest) ([]string, error) {
	log, closeLog, err := openTurnLog(request.EventLogPath)
	if err != nil {
		return nil, err
	}
	defer closeLog()
	client := memoryclient.NewClient(request.MemoryBaseURL, request.MemoryAuthToken, &http.Client{Timeout: 5 * time.Second}, 1<<20)
	return s.orchestrator.drainOutbox(ctx, domain.RoomID(request.RoomID), client, log)
}

// RecoverAfterRestart replays a crash scenario against the session's durable
// store. It forwards to the orchestrator so external drivers can exercise
// restart recovery on the same store their turns ran against.
func (s *Session) RecoverAfterRestart(ctx context.Context, scenario RecoveryScenario) (RecoveryResult, error) {
	return s.orchestrator.RecoverAfterRestart(ctx, scenario)
}
