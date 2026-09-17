package directedoffer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
)

func TestFreeTextAtAgentDoesNotRouteStructuredTargetCreatesDelivery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := NewCoordinator()

	display, err := c.Offer(ctx, Request{
		RoomID:         "room-1",
		AuthorID:       "memory-agent",
		Content:        "@task-agent please consider skill://demo/lineage@rev1",
		IdempotencyKey: "offer-display",
	})
	if err != nil {
		t.Fatalf("display Offer() error = %v", err)
	}
	if display.Message.ID == "" || display.Message.Content == "" {
		t.Fatalf("display message = %#v, want a stored Room message", display.Message)
	}
	if len(display.Deliveries) != 0 {
		t.Fatalf("free-text @agent deliveries = %#v, want none", display.Deliveries)
	}

	targeted, err := c.Offer(ctx, Request{
		RoomID:           "room-1",
		AuthorID:         "memory-agent",
		Content:          "@task-agent please consider skill://demo/lineage@rev1",
		IdempotencyKey:   "offer-structured",
		RecipientAgentID: "task-agent",
		SkillReference:   "skill://demo/lineage@rev1",
	})
	if err != nil {
		t.Fatalf("structured Offer() error = %v", err)
	}
	if targeted.Message.ID == "" || targeted.Message.ID == display.Message.ID {
		t.Fatalf("structured message = %#v, want a new canonical message", targeted.Message)
	}
	if targeted.Message.Sequence <= display.Message.Sequence {
		t.Fatalf("structured sequence = %d, want after display %d", targeted.Message.Sequence, display.Message.Sequence)
	}
	if len(targeted.Deliveries) != 1 {
		t.Fatalf("structured deliveries = %#v, want exactly one", targeted.Deliveries)
	}
	delivery := targeted.Deliveries[0]
	if delivery.AgentID != "task-agent" || delivery.MessageID != targeted.Message.ID || delivery.State != "pending" {
		t.Fatalf("structured delivery = %#v, want unique pending (task-agent, %q)", delivery, targeted.Message.ID)
	}
}

func TestSameKeyBodyReplayReturnsOriginalAndDifferentBodyConflicts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := NewCoordinator()
	req := Request{
		RoomID:           "room-1",
		AuthorID:         "memory-agent",
		Content:          "@task-agent skill body v1",
		IdempotencyKey:   "offer-key-1",
		RecipientAgentID: "task-agent",
		SkillReference:   "skill://demo/lineage@rev1",
	}

	first, err := c.Offer(ctx, req)
	if err != nil {
		t.Fatalf("first Offer() error = %v", err)
	}
	if first.Duplicate || first.Message.ID == "" || len(first.Deliveries) != 1 {
		t.Fatalf("first Offer() = %#v, want a new message and unique delivery", first)
	}

	replay, err := c.Offer(ctx, req)
	if err != nil {
		t.Fatalf("replay Offer() error = %v", err)
	}
	if !replay.Duplicate {
		t.Fatal("same key/body replay did not report Duplicate")
	}
	if replay.Message != first.Message {
		t.Fatalf("replay message = %#v, want original %#v", replay.Message, first.Message)
	}
	if len(replay.Deliveries) != 1 || replay.Deliveries[0] != first.Deliveries[0] {
		t.Fatalf("replay deliveries = %#v, want original %#v", replay.Deliveries, first.Deliveries)
	}

	conflict := req
	conflict.Content = "@task-agent skill body v2"
	if _, err := c.Offer(ctx, conflict); !errors.Is(err, ErrOfferBodyConflict) {
		t.Fatalf("same key different body error = %v, want %v", err, ErrOfferBodyConflict)
	}

	next, err := c.Offer(ctx, Request{
		RoomID:           "room-1",
		AuthorID:         "memory-agent",
		Content:          "later offer",
		IdempotencyKey:   "offer-key-2",
		RecipientAgentID: "task-agent",
	})
	if err != nil {
		t.Fatalf("follow-up Offer() error = %v", err)
	}
	if next.Message.Sequence != first.Message.Sequence+1 {
		t.Fatalf("follow-up sequence = %d, want %d (conflict must not write)", next.Message.Sequence, first.Message.Sequence+1)
	}
}

func TestConcurrentMentionsAbortOnceAndResumeInjectsInRoomSequence(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	session := newRunningTask(t, ctx)
	abortHold := make(chan struct{})
	session.AbortHold = abortHold
	c := NewCoordinator()
	attachTask(t, c, session)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []Result
	)
	wg.Add(2)
	for _, key := range []string{"mention-a", "mention-b"} {
		key := key
		go func() {
			defer wg.Done()
			got, err := c.Offer(ctx, Request{
				RoomID:           "room-1",
				AuthorID:         "memory-agent",
				Content:          "@task-agent concurrent " + key,
				IdempotencyKey:   key,
				RecipientAgentID: "task-agent",
				SkillReference:   "skill://demo/lineage@rev1",
			})
			if err != nil {
				t.Errorf("Offer(%s) error = %v", key, err)
				return
			}
			if len(got.Deliveries) != 1 {
				t.Errorf("Offer(%s) deliveries = %#v, want 1", key, got.Deliveries)
			}
			mu.Lock()
			results = append(results, got)
			mu.Unlock()
		}()
	}
	waitQueued(t, c, "task-agent", 2)
	close(abortHold)
	wg.Wait()
	if len(results) != 2 {
		t.Fatalf("completed offers = %d, want 2", len(results))
	}

	obs := session.Observation()
	if obs.AbortCount != 1 {
		t.Fatalf("AbortCount = %d, want 1", obs.AbortCount)
	}
	if obs.ResumeCount != 1 {
		t.Fatalf("ResumeCount = %d, want 1", obs.ResumeCount)
	}
	if len(obs.Injected) != 2 {
		t.Fatalf("injected = %#v, want both mentions", obs.Injected)
	}
	if obs.Injected[0].Sequence >= obs.Injected[1].Sequence {
		t.Fatalf("injected order = seq %d then %d, want Room sequence", obs.Injected[0].Sequence, obs.Injected[1].Sequence)
	}
	if obs.Injected[0].MessageID == "" || obs.Injected[1].MessageID == "" || obs.Injected[0].MessageID == obs.Injected[1].MessageID {
		t.Fatalf("injected message ids = %#v, want two distinct Room messages", obs.Injected)
	}
}

func TestResumeKeepsExactSessionAndDoesNotReplayCompletedToolResults(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pinned := sessionctrl.Session{File: "/sessions/exact-task.jsonl", ID: "exact-session-id"}
	session := NewScriptedExactSession("task-agent", pinned)
	session.Hold = make(chan struct{})
	session.Completed = []ToolResult{{CallID: "tool-1", Name: "bash", Output: "already done"}}
	before := session.Observation()

	c := NewCoordinator()
	attachTask(t, c, session)

	startErr := make(chan error, 1)
	go func() { startErr <- session.Start(ctx) }()
	waitPhase(t, session, PhaseRunning)

	if _, err := c.Offer(ctx, Request{
		RoomID:           "room-1",
		AuthorID:         "memory-agent",
		Content:          "@task-agent resume exact session",
		IdempotencyKey:   "offer-resume",
		RecipientAgentID: "task-agent",
		SkillReference:   "skill://demo/lineage@rev1",
	}); err != nil {
		t.Fatalf("Offer() error = %v", err)
	}
	if err := <-startErr; err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	obs := session.Observation()
	if obs.Session != pinned || obs.LastResume.Session != pinned {
		t.Fatalf("resumed session = %#v last resume = %#v, want pinned %#v", obs.Session, obs.LastResume.Session, pinned)
	}
	if obs.Session.File != before.Session.File || obs.Session.ID != before.Session.ID {
		t.Fatalf("session identity changed: before %#v after %#v", before.Session, obs.Session)
	}
	if len(obs.CompletedTools) != 1 || obs.CompletedTools[0].CallID != "tool-1" {
		t.Fatalf("completed tools = %#v, want the pre-interrupt result", obs.CompletedTools)
	}
	if len(obs.ReplayedTools) != 0 {
		t.Fatalf("replayed tools = %#v, want none", obs.ReplayedTools)
	}
	if obs.ResumeCount != 1 || len(obs.Injected) != 1 {
		t.Fatalf("resume observation = %#v, want one injected mention", obs)
	}
}

func TestContinuationSegmentRecordsContinuationOfAndDirectedMentionReason(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	session := newRunningTask(t, ctx)
	c := NewCoordinator()
	binding := TestBinding("task-agent", session.Session())
	binding.Generation = 3
	binding.SegmentID = "seg-task-open"
	if err := c.Attach(binding, session); err != nil {
		t.Fatalf("Attach() error = %v", err)
	}

	got, err := c.Offer(ctx, Request{
		RoomID:           "room-1",
		AuthorID:         "memory-agent",
		Content:          "@task-agent directed mention",
		IdempotencyKey:   "offer-segment",
		RecipientAgentID: "task-agent",
		SkillReference:   "skill://demo/lineage@rev1",
	})
	if err != nil {
		t.Fatalf("Offer() error = %v", err)
	}

	if got.Segment.ID == "" || got.Segment.ID == "seg-task-open" {
		t.Fatalf("continuation segment = %#v, want a new Segment", got.Segment)
	}
	if got.Segment.ContinuationOf != "seg-task-open" {
		t.Fatalf("continuation_of = %q, want %q", got.Segment.ContinuationOf, "seg-task-open")
	}
	if got.Segment.Reason != ReasonDirectedMention {
		t.Fatalf("segment reason = %q, want %s", got.Segment.Reason, ReasonDirectedMention)
	}

	segments := c.Segments("task-agent")
	if len(segments) != 2 {
		t.Fatalf("segments = %#v, want opening + continuation", segments)
	}
	if segments[0].ID != "seg-task-open" || segments[0].ContinuationOf != "" || segments[0].Reason != "" {
		t.Fatalf("opening segment = %#v", segments[0])
	}
	if segments[1] != got.Segment {
		t.Fatalf("stored continuation = %#v, want %#v", segments[1], got.Segment)
	}
}

func newRunningTask(t *testing.T, ctx context.Context) *ScriptedExactSession {
	t.Helper()
	session := NewScriptedExactSession("task-agent", sessionctrl.Session{
		File: "/sessions/exact-task.jsonl",
		ID:   "exact-session-id",
	})
	session.Hold = make(chan struct{})
	session.Completed = []ToolResult{{CallID: "tool-1", Name: "bash", Output: "already done"}}
	go func() { _ = session.Start(ctx) }()
	waitPhase(t, session, PhaseRunning)
	return session
}

func TestAttachFailsClosedWhenBindingPinsMissing(t *testing.T) {
	t.Parallel()
	session := NewScriptedExactSession("task-agent", sessionctrl.Session{
		File: "/sessions/exact-task.jsonl",
		ID:   "exact-session-id",
	})

	t.Run("missing SessionDir", func(t *testing.T) {
		t.Parallel()
		c := NewCoordinator()
		binding := TestBinding(session.AgentID(), session.Session())
		binding.SessionDir = ""
		if err := c.Attach(binding, session); !errors.Is(err, ErrBindingPinsRequired) {
			t.Fatalf("Attach() error = %v, want %v", err, ErrBindingPinsRequired)
		}
	})
	t.Run("missing ToolPolicyDigest", func(t *testing.T) {
		t.Parallel()
		c := NewCoordinator()
		binding := TestBinding(session.AgentID(), session.Session())
		binding.ToolPolicyDigest = ""
		if err := c.Attach(binding, session); !errors.Is(err, ErrBindingPinsRequired) {
			t.Fatalf("Attach() error = %v, want %v", err, ErrBindingPinsRequired)
		}
	})
}

func attachTask(t *testing.T, c *Coordinator, session *ScriptedExactSession) {
	t.Helper()
	binding := TestBinding(session.AgentID(), session.Session())
	binding.Generation = 3
	binding.SegmentID = "seg-task-open"
	if err := c.Attach(binding, session); err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
}

func waitPhase(t *testing.T, session *ScriptedExactSession, want Phase) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if session.Observation().Phase == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("phase = %s, want %s", session.Observation().Phase, want)
}

func waitQueued(t *testing.T, c *Coordinator, agentID string, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(c.queued(agentID)) == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued(%s) = %#v, want %d pending mentions", agentID, c.queued(agentID), n)
}
