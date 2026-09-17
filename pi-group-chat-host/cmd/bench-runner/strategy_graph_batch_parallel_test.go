package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
	"river2.dev/pi-group-chat-host/internal/runtime"
)

func TestGraphBatchParallelStartBarrierOverlap(t *testing.T) {
	taskHold := make(chan struct{})
	task := newScriptedParallelTask(t, taskHold, false)
	memoryStarted := make(chan struct{})
	memoryHold := make(chan struct{})
	record := &attemptRecord{WorkDir: t.TempDir()}
	errCh := make(chan error, 1)
	go func() {
		errCh <- runGraphBatchParallelHeldOut(context.Background(), graphBatchParallelRequest{
			Record:    record,
			AttemptID: "overlap-1",
			RoomID:    "room-1",
			Task:      task,
			Memory: func(context.Context) graphBatchMemoryOutcome {
				close(memoryStarted)
				<-memoryHold
				return graphBatchMemoryOutcome{Status: "declined"}
			},
		})
	}()
	waitParallel(t, func() bool {
		select {
		case <-task.EnteredRunning():
		default:
			return false
		}
		select {
		case <-memoryStarted:
			return task.Running()
		default:
			return false
		}
	})
	if !task.Running() {
		t.Fatal("task should still be running while memory is in-flight")
	}
	close(taskHold)
	close(memoryHold)
	if err := <-errCh; err != nil {
		t.Fatalf("parallel episode: %v", err)
	}
	if !record.StartOverlap {
		t.Fatal("expected start-barrier overlap")
	}
	if record.DeliveryStatus != deliveryStatusDeclined {
		t.Fatalf("delivery = %q, want declined", record.DeliveryStatus)
	}
	if record.AbortCount != 0 {
		t.Fatalf("declined path must not interrupt, abort=%d", record.AbortCount)
	}
}

func TestGraphBatchParallelInterruptResumeDirectedMention(t *testing.T) {
	task := newScriptedParallelTask(t, nil, false)
	skill := pinnedSkill{
		SkillReference: "skill://evaluation/pin0917-01@1",
		SHA256:         "aa", Name: "demo", BodyDigest: "digest-1", Trigger: "math", Body: "print(1)",
	}
	record := &attemptRecord{WorkDir: t.TempDir()}
	if err := runGraphBatchParallelHeldOut(context.Background(), graphBatchParallelRequest{
		Record:    record,
		AttemptID: "offer-1",
		RoomID:    "room-1",
		Task:      task,
		Memory: func(context.Context) graphBatchMemoryOutcome {
			return graphBatchMemoryOutcome{Status: "published", Selections: []pinnedSkill{skill}}
		},
	}); err != nil {
		t.Fatalf("parallel episode: %v", err)
	}
	if !record.StartOverlap {
		t.Fatal("expected start-barrier overlap")
	}
	if record.DeliveryStatus != deliveryStatusDelivered {
		t.Fatalf("delivery = %q", record.DeliveryStatus)
	}
	if record.ContinuationOf != graphBatchOpenSegmentID {
		t.Fatalf("continuation_of = %q", record.ContinuationOf)
	}
	if record.SegmentReason != directedoffer.ReasonDirectedMention {
		t.Fatalf("segment reason = %q", record.SegmentReason)
	}
	if record.AbortCount != 1 || record.ResumeCount != 1 {
		t.Fatalf("abort/resume = %d/%d", record.AbortCount, record.ResumeCount)
	}
	if record.UsedResumeFlag || record.UsedContinueFlag {
		t.Fatal("exact-session resume must not use --resume or --continue")
	}
}

func TestGraphBatchParallelTaskFinishedBeforeDelivery(t *testing.T) {
	settled := make(chan struct{})
	close(settled)
	task := newScriptedParallelTask(t, settled, false)
	record := &attemptRecord{WorkDir: t.TempDir()}
	if err := runGraphBatchParallelHeldOut(context.Background(), graphBatchParallelRequest{
		Record:    record,
		AttemptID: "race-1",
		RoomID:    "room-1",
		Task:      task,
		Memory: func(context.Context) graphBatchMemoryOutcome {
			time.Sleep(30 * time.Millisecond)
			return graphBatchMemoryOutcome{Status: "published", Selections: []pinnedSkill{{
				SkillReference: "skill://evaluation/pin0917-01@1", SHA256: "aa", Name: "demo", BodyDigest: "d",
			}}}
		},
	}); err != nil {
		t.Fatalf("parallel episode: %v", err)
	}
	if record.DeliveryStatus != deliveryStatusTaskFinishedFirst {
		t.Fatalf("delivery = %q, want %s", record.DeliveryStatus, deliveryStatusTaskFinishedFirst)
	}
	if record.AbortCount != 0 || record.ResumeCount != 0 {
		t.Fatalf("discarded selection must not interrupt, abort/resume=%d/%d", record.AbortCount, record.ResumeCount)
	}
	if record.Status != "success" {
		t.Fatalf("task finishing first is not a failure: status=%q err=%q", record.Status, record.Error)
	}
}

func TestGraphBatchParallelEffectsUnknownFailClosed(t *testing.T) {
	task := newScriptedParallelTask(t, nil, true)
	record := &attemptRecord{WorkDir: t.TempDir()}
	if err := runGraphBatchParallelHeldOut(context.Background(), graphBatchParallelRequest{
		Record:        record,
		AttemptID:     "effects-1",
		RoomID:        "room-1",
		Task:          task,
		InterruptWait: 15 * time.Millisecond,
		Memory: func(context.Context) graphBatchMemoryOutcome {
			return graphBatchMemoryOutcome{Status: "published", Selections: []pinnedSkill{{
				SkillReference: "skill://evaluation/pin0917-01@1", SHA256: "aa", Name: "demo", BodyDigest: "d",
			}}}
		},
	}); err != nil {
		t.Fatalf("parallel episode: %v", err)
	}
	if record.DeliveryStatus != deliveryStatusEffectsUnknown || !record.EffectsUnknown {
		t.Fatalf("delivery = %q effects_unknown=%t", record.DeliveryStatus, record.EffectsUnknown)
	}
	if record.ResumeCount != 0 {
		t.Fatalf("effects_unknown must fail closed without resume, resume=%d", record.ResumeCount)
	}
	if record.Status != "failed" {
		t.Fatalf("status = %q, want failed", record.Status)
	}
}

type scriptedParallelTask struct {
	inner    *directedoffer.ScriptedExactSession
	unproven bool
	entered  chan struct{}
	once     sync.Once
}

func newScriptedParallelTask(t *testing.T, hold <-chan struct{}, unproven bool) *scriptedParallelTask {
	t.Helper()
	inner := directedoffer.NewScriptedExactSession(graphBatchTaskAgentID, sessionctrl.Session{
		File: "/sessions/exact-task.jsonl", ID: "exact-session-id",
	})
	inner.Hold = hold
	return &scriptedParallelTask{inner: inner, unproven: unproven, entered: make(chan struct{})}
}

func (s *scriptedParallelTask) AgentID() string                    { return s.inner.AgentID() }
func (s *scriptedParallelTask) Session() sessionctrl.Session       { return s.inner.Session() }
func (s *scriptedParallelTask) Running() bool                      { return s.inner.Running() }
func (s *scriptedParallelTask) UnprovenMutating() bool             { return s.unproven }
func (s *scriptedParallelTask) EnteredRunning() <-chan struct{}    { return s.entered }
func (s *scriptedParallelTask) BindResume(*graphBatchSkillProtocol, string, string) {
}
func (s *scriptedParallelTask) VisibleMessages() []runtime.VisibleMessage { return nil }
func (s *scriptedParallelTask) RecallLeaked() bool                         { return false }
func (s *scriptedParallelTask) UsedResumeFlag() bool                       { return false }
func (s *scriptedParallelTask) UsedContinueFlag() bool                     { return false }
func (s *scriptedParallelTask) Observation() directedoffer.SessionObservation {
	return s.inner.Observation()
}
func (s *scriptedParallelTask) Abort(ctx context.Context) error { return s.inner.Abort(ctx) }
func (s *scriptedParallelTask) WaitSettled(ctx context.Context) error {
	return s.inner.WaitSettled(ctx)
}
func (s *scriptedParallelTask) Resume(ctx context.Context, req directedoffer.ResumeRequest) error {
	return s.inner.Resume(ctx, req)
}
func (s *scriptedParallelTask) Start(ctx context.Context) error {
	s.once.Do(func() { close(s.entered) })
	return s.inner.Start(ctx)
}

func waitParallel(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for parallel start")
}
