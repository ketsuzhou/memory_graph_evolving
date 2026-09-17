package skilldisposition

import (
	"context"
	"errors"
	"strings"
	"testing"

	"river2.dev/pi-group-chat-host/internal/skillfence"
)

func TestUnservedOfferCannotBeAccepted(t *testing.T) {
	t.Parallel()
	h := newHarness(t, false)
	ctx := context.Background()

	result, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-unserved"))
	if !errors.Is(err, ErrServedRequired) {
		t.Fatalf("accepted before serve error = %v, want served required", err)
	}
	if result.Allowed || result.Disposition != nil || result.Signal != nil || result.Message != nil || result.Delivery != nil {
		t.Fatalf("result = %#v, want no accepted publication", result)
	}
	assertNoPublication(t, h.coord)
	if h.coord.Fence("task-agent") != FenceFeedback {
		t.Fatalf("fence = %q, want %q for an unserved offer", h.coord.Fence("task-agent"), FenceFeedback)
	}

	_, err = h.coord.HandleToolCall(ctx, ToolCall{CallID: "bash-unserved", Name: "bash", AgentID: "task-agent"})
	if !errors.Is(err, ErrOrdinaryToolFenced) {
		t.Fatalf("ordinary tool on unserved offer error = %v, want fenced", err)
	}
}

func TestSameOfferAgentHasOneTerminalDispositionSameReplayIdempotentConflictRejected(t *testing.T) {
	t.Parallel()
	h := newHarness(t, true)
	ctx := context.Background()

	first, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-1"))
	if err != nil || first.Duplicate || first.Disposition == nil || first.Signal == nil || first.Message == nil || first.Delivery == nil {
		t.Fatalf("first accepted = %#v err=%v, want a new terminal publication", first, err)
	}
	if first.Disposition.Value != DispositionAccepted || first.Signal.Stage != StageAccepted {
		t.Fatalf("first disposition = %#v signal=%#v, want accepted", first.Disposition, first.Signal)
	}
	if h.coord.Fence("task-agent") != FenceOpen {
		t.Fatalf("fence after accepted = %q, want open", h.coord.Fence("task-agent"))
	}

	replaySameCall, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-1"))
	if err != nil || !replaySameCall.Duplicate {
		t.Fatalf("same-call replay = %#v err=%v, want idempotent original", replaySameCall, err)
	}
	assertSamePublication(t, first, replaySameCall)

	replayNewCall, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-1-again"))
	if err != nil || !replayNewCall.Duplicate {
		t.Fatalf("same accepted payload replay = %#v err=%v, want idempotent original", replayNewCall, err)
	}
	assertSamePublication(t, first, replayNewCall)

	conflict, err := h.coord.HandleToolCall(ctx, ToolCall{
		CallID:      "fb-reject",
		Name:        ToolSkillFeedback,
		AgentID:     "task-agent",
		Disposition: DispositionRejected,
	})
	if !errors.Is(err, ErrDispositionConflict) {
		t.Fatalf("conflicting rejected replay error = %v, want conflict", err)
	}
	if conflict.Disposition == nil || conflict.Disposition.Value != DispositionAccepted {
		t.Fatalf("conflict result = %#v, want the original accepted disposition retained", conflict)
	}

	if got := h.coord.Dispositions(); len(got) != 1 || got[0].Value != DispositionAccepted {
		t.Fatalf("dispositions = %#v, want exactly one accepted terminal", got)
	}
	if pubs := h.coord.Publications(); len(pubs) != 1 {
		t.Fatalf("publications = %#v, want the original triple only", pubs)
	}
	if len(h.coord.Signals()) != 1 {
		t.Fatalf("signals = %#v, want one accepted signal", h.coord.Signals())
	}

	ordinary, err := h.coord.HandleToolCall(ctx, ToolCall{CallID: "bash-1", Name: "bash", AgentID: "task-agent"})
	if err != nil || !ordinary.Allowed || ordinary.Fence != FenceOpen {
		t.Fatalf("ordinary tool after accepted = %#v err=%v, want fence lifted", ordinary, err)
	}
}

func TestSignalRoomMessageMemoryDeliveryAreAllOrNone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("success_commits_all_three", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, true)
		result, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-all"))
		if err != nil {
			t.Fatalf("accepted: %v", err)
		}
		if result.Signal == nil || result.Message == nil || result.Delivery == nil {
			t.Fatalf("result = %#v, want signal, room message, and memory delivery", result)
		}
		if result.Signal.Stage != StageAccepted || result.Signal.OfferID != "offer-1" || result.Signal.BodyDigest != h.served.BodyDigest {
			t.Fatalf("signal = %#v, want accepted for the served offer", result.Signal)
		}
		if !strings.Contains(result.Message.Content, "@memory-agent") || !strings.Contains(result.Message.Content, MarkerAccepted) {
			t.Fatalf("room message = %#v, want visible @memory-agent SKILL_ACCEPTED", result.Message)
		}
		if result.Message.AuthorID != "task-agent" || result.Message.RoomID != "room-1" {
			t.Fatalf("room message routing = %#v, want task-agent in room-1", result.Message)
		}
		if result.Delivery.AgentID != DefaultMemoryAgentID || result.Delivery.MessageID != result.Message.ID || result.Delivery.State != "pending" {
			t.Fatalf("memory delivery = %#v, want unique pending memory-agent delivery of %q", result.Delivery, result.Message.ID)
		}

		pubs := h.coord.Publications()
		if len(pubs) != 1 || len(h.coord.Signals()) != 1 {
			t.Fatalf("committed pubs=%#v signals=%#v, want one triple", pubs, h.coord.Signals())
		}
		if pubs[0].Signal != *result.Signal || pubs[0].Message != *result.Message || pubs[0].Delivery != *result.Delivery {
			t.Fatalf("committed triple %#v, want the returned all-or-none result", pubs[0])
		}
		if h.publisher.Calls() != 1 {
			t.Fatalf("publisher calls = %d, want 1", h.publisher.Calls())
		}
	})

	t.Run("publish_failure_commits_none", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, true)
		h.publisher.Fail = errors.New("directed offer exploded")

		result, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-fail"))
		if err == nil || errors.Is(err, ErrServedRequired) {
			t.Fatalf("accepted with failing publisher error = %v, want the publish failure", err)
		}
		if result.Disposition != nil || result.Signal != nil || result.Message != nil || result.Delivery != nil {
			t.Fatalf("result = %#v, want none of the triple after publish failure", result)
		}
		assertNoPublication(t, h.coord)
		if len(h.coord.Dispositions()) != 0 {
			t.Fatalf("dispositions = %#v, want none when publication rolls back", h.coord.Dispositions())
		}
		if h.coord.Fence("task-agent") != FenceFeedback {
			t.Fatalf("fence = %q, want still fenced after a rolled-back accept", h.coord.Fence("task-agent"))
		}
		if got := h.publisher.Results(); len(got) != 0 {
			t.Fatalf("publisher results = %#v, want no durable mention or delivery", got)
		}

		_, err = h.coord.HandleToolCall(ctx, ToolCall{CallID: "bash-fail", Name: "bash", AgentID: "task-agent"})
		if !errors.Is(err, ErrOrdinaryToolFenced) {
			t.Fatalf("ordinary tool after rolled-back accept error = %v, want still fenced", err)
		}
	})
}

func TestAcceptedDoesNotAutoProduceAdoptedVerifiedActiveOrMarginalGainSupported(t *testing.T) {
	t.Parallel()
	h := newHarness(t, true)
	ctx := context.Background()

	accepted, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-accept"))
	if err != nil || accepted.Disposition == nil || accepted.Disposition.Value != DispositionAccepted {
		t.Fatalf("accepted = %#v err=%v, want an accepted disposition", accepted, err)
	}
	if accepted.Signal != nil && (accepted.Signal.Stage == StageAdopted || accepted.Signal.Stage == StageVerified || accepted.Signal.Stage == ForbiddenActiveSkillLabel) {
		t.Fatalf("accepted signal = %#v, must not imply a later stage", accepted.Signal)
	}
	assertNoLaterStage(t, h.coord, StageAdopted, StageVerified, ForbiddenActiveSkillLabel, StatusMarginalGainSupported)
	if got := h.coord.Adoptions(); len(got) != 0 {
		t.Fatalf("adoptions = %#v, want none from accepted alone", got)
	}

	adopted, err := h.coord.HandleToolCall(ctx, ToolCall{
		CallID:         "adopt-1",
		Name:           ToolSkillAdoption,
		AgentID:        "task-agent",
		SkillReference: h.served.SkillReference,
		CheckpointID:   "checkpoint-1",
		BehaviorChange: "prefer the served specialization at this checkpoint",
		AffectedAction: "write-patch",
	})
	if err != nil || adopted.Adoption == nil {
		t.Fatalf("skill_adoption = %#v err=%v, want an explicit adoption record", adopted, err)
	}
	if adopted.Adoption.OfferID != "offer-1" || adopted.Adoption.SkillReference != h.served.SkillReference {
		t.Fatalf("adoption identity = %#v, want the exact served skill and offer", adopted.Adoption)
	}
	if adopted.Adoption.CheckpointID != "checkpoint-1" || adopted.Adoption.BehaviorChange == "" || adopted.Adoption.AffectedAction != "write-patch" {
		t.Fatalf("adoption = %#v, want checkpoint, behavior change, and affected action", adopted.Adoption)
	}

	if got := h.coord.LaterStages(); len(got) != 1 || got[0] != StageAdopted {
		t.Fatalf("later stages = %#v, want only the explicit adopted record", got)
	}
	assertNoLaterStage(t, h.coord, StageVerified, ForbiddenActiveSkillLabel, StatusMarginalGainSupported)
	if len(h.coord.Adoptions()) != 1 {
		t.Fatalf("adoptions = %#v, want the single explicit record", h.coord.Adoptions())
	}
	if len(h.coord.Dispositions()) != 1 || h.coord.Dispositions()[0].Value != DispositionAccepted {
		t.Fatalf("dispositions = %#v, want accepted unchanged by adoption", h.coord.Dispositions())
	}
}

func TestInvalidFeedbackRepairsOnceThenProtocolErrorLiftsFence(t *testing.T) {
	t.Parallel()
	h := newHarness(t, true)
	ctx := context.Background()

	first, err := h.coord.HandleToolCall(ctx, ToolCall{
		CallID:      "fb-bad-1",
		Name:        ToolSkillFeedback,
		AgentID:     "task-agent",
		Disposition: "not-a-disposition",
	})
	if !errors.Is(err, ErrInvalidFeedback) {
		t.Fatalf("first invalid feedback error = %v, want one protocol repair", err)
	}
	if !first.Repair || first.ProtocolError || first.Disposition != nil {
		t.Fatalf("first invalid = %#v, want a single repair without protocol_error", first)
	}
	assertNoPublication(t, h.coord)
	if h.coord.Fence("task-agent") != FenceFeedback {
		t.Fatalf("fence after first invalid = %q, want still fenced", h.coord.Fence("task-agent"))
	}

	replayRepair, err := h.coord.HandleToolCall(ctx, ToolCall{
		CallID:      "fb-bad-1",
		Name:        ToolSkillFeedback,
		AgentID:     "task-agent",
		Disposition: "not-a-disposition",
	})
	if !errors.Is(err, ErrInvalidFeedback) || !replayRepair.Duplicate || !replayRepair.Repair {
		t.Fatalf("repair replay = %#v err=%v, want the original repair without a second strike", replayRepair, err)
	}
	if len(h.coord.Dispositions()) != 0 {
		t.Fatalf("dispositions after repair replay = %#v, want none", h.coord.Dispositions())
	}

	_, err = h.coord.HandleToolCall(ctx, ToolCall{CallID: "bash-still-fenced", Name: "bash", AgentID: "task-agent"})
	if !errors.Is(err, ErrOrdinaryToolFenced) {
		t.Fatalf("ordinary tool after one repair error = %v, want still fenced", err)
	}

	second, err := h.coord.HandleToolCall(ctx, ToolCall{
		CallID:      "fb-bad-2",
		Name:        ToolSkillFeedback,
		AgentID:     "task-agent",
		Disposition: "",
	})
	if !errors.Is(err, ErrProtocolError) {
		t.Fatalf("second invalid feedback error = %v, want protocol_error", err)
	}
	if !second.ProtocolError || second.Repair || second.Disposition == nil || second.Disposition.Value != DispositionProtocolError {
		t.Fatalf("second invalid = %#v, want a recorded protocol_error disposition", second)
	}
	if second.Signal != nil || second.Message != nil || second.Delivery != nil {
		t.Fatalf("protocol_error publication = %#v, want no accepted triple", second)
	}
	assertNoPublication(t, h.coord)
	if h.coord.Fence("task-agent") != FenceOpen {
		t.Fatalf("fence after protocol_error = %q, want lifted", h.coord.Fence("task-agent"))
	}

	ordinary, err := h.coord.HandleToolCall(ctx, ToolCall{CallID: "bash-after-protocol", Name: "bash", AgentID: "task-agent"})
	if err != nil || !ordinary.Allowed || ordinary.Fence != FenceOpen {
		t.Fatalf("ordinary tool after protocol_error = %#v err=%v, want fence lifted", ordinary, err)
	}

	conflict, err := h.coord.HandleToolCall(ctx, acceptedCall("fb-after-protocol"))
	if !errors.Is(err, ErrDispositionConflict) {
		t.Fatalf("accepted after protocol_error error = %v, want the terminal protocol_error retained", err)
	}
	if conflict.Disposition == nil || conflict.Disposition.Value != DispositionProtocolError {
		t.Fatalf("conflict after protocol_error = %#v, want protocol_error kept", conflict)
	}
}

type harness struct {
	publisher *ScriptedPublisher
	coord     *Coordinator
	served    skillfence.ServedFact
}

func newHarness(t *testing.T, serve bool) *harness {
	t.Helper()
	publisher := NewScriptedPublisher(NewOfferPublisher(nil))
	coord, err := NewCoordinator(publisher)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	offer := Offer{
		OfferID:        "offer-1",
		AgentID:        "task-agent",
		RoomID:         "room-1",
		SkillReference: "skill://evaluation/alpha@1",
		CheckpointID:   "checkpoint-1",
		MemoryAgentID:  DefaultMemoryAgentID,
	}
	if err := coord.BindOffer(offer); err != nil {
		t.Fatalf("bind offer: %v", err)
	}
	served := skillfence.ServedFact{
		OfferID:        offer.OfferID,
		AgentID:        offer.AgentID,
		SkillReference: offer.SkillReference,
		ToolCallID:     "get-1",
		BodyDigest:     "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		ManifestDigest: "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		Stage:          skillfence.StageServed,
	}
	if serve {
		if err := coord.NoteServed(served); err != nil {
			t.Fatalf("note served: %v", err)
		}
	}
	return &harness{publisher: publisher, coord: coord, served: served}
}

func acceptedCall(callID string) ToolCall {
	return ToolCall{
		CallID:      callID,
		Name:        ToolSkillFeedback,
		AgentID:     "task-agent",
		Disposition: DispositionAccepted,
	}
}

func assertNoPublication(t *testing.T, coord *Coordinator) {
	t.Helper()
	if got := coord.Signals(); len(got) != 0 {
		t.Fatalf("signals = %#v, want none", got)
	}
	if got := coord.Publications(); len(got) != 0 {
		t.Fatalf("publications = %#v, want none", got)
	}
}

func assertSamePublication(t *testing.T, first, replay Result) {
	t.Helper()
	if first.Disposition == nil || replay.Disposition == nil || first.Disposition.Value != replay.Disposition.Value {
		t.Fatalf("replay disposition %#v, want the original %#v", replay.Disposition, first.Disposition)
	}
	if first.Message == nil || replay.Message == nil || first.Message.ID != replay.Message.ID {
		t.Fatalf("replay message %#v, want the original %#v", replay.Message, first.Message)
	}
	if first.Delivery == nil || replay.Delivery == nil || first.Delivery.ID != replay.Delivery.ID {
		t.Fatalf("replay delivery %#v, want the original %#v", replay.Delivery, first.Delivery)
	}
	if first.Signal == nil || replay.Signal == nil || *first.Signal != *replay.Signal {
		t.Fatalf("replay signal %#v, want the original %#v", replay.Signal, first.Signal)
	}
}

func assertNoLaterStage(t *testing.T, coord *Coordinator, names ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, name := range coord.LaterStages() {
		got[name] = true
	}
	for _, name := range names {
		if got[name] {
			t.Fatalf("later stages = %v, must not include %q", coord.LaterStages(), name)
		}
	}
}
