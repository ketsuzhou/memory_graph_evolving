package agentrun

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/effectsinterrupt"
	"river2.dev/pi-group-chat-host/internal/pi/sessionctrl"
)

func TestRunDoesNotOverlapSameAgent(t *testing.T) {
	t.Parallel()
	c := New(effectsinterrupt.Policy{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var firstActive atomic.Bool
	var overlapping atomic.Bool
	firstHold := make(chan struct{})
	firstEntered := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- c.Run(ctx, "task-agent", func() error {
			firstActive.Store(true)
			close(firstEntered)
			<-firstHold
			firstActive.Store(false)
			return nil
		})
	}()
	<-firstEntered

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- c.Run(ctx, "task-agent", func() error {
			if firstActive.Load() {
				overlapping.Store(true)
			}
			close(secondEntered)
			return nil
		})
	}()

	time.Sleep(30 * time.Millisecond)
	select {
	case <-secondEntered:
		t.Fatal("same agent had overlapping Run calls")
	default:
	}
	if !c.Flights().InFlight("task-agent") {
		t.Fatal("first Run released the single-flight lease before settling")
	}

	close(firstHold)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	select {
	case <-secondEntered:
	default:
		t.Fatal("second Run() did not start after the first settled")
	}
	if overlapping.Load() {
		t.Fatal("second Run started while the first turn was still running")
	}
}

func TestAttachOfferFailsClosedWhenPinMissing(t *testing.T) {
	t.Parallel()
	c := New(effectsinterrupt.Policy{})
	session := directedoffer.NewScriptedExactSession("task-agent", sessionctrl.Session{
		File: "/sessions/exact-task.jsonl",
		ID:   "exact-session-id",
	})

	t.Run("missing SessionDir", func(t *testing.T) {
		t.Parallel()
		binding := directedoffer.TestBinding(session.AgentID(), session.Session())
		binding.SessionDir = ""
		if err := c.AttachOffer(binding, session); !errors.Is(err, directedoffer.ErrBindingPinsRequired) {
			t.Fatalf("AttachOffer() error = %v, want %v", err, directedoffer.ErrBindingPinsRequired)
		}
	})
	t.Run("missing ToolPolicyDigest", func(t *testing.T) {
		t.Parallel()
		binding := directedoffer.TestBinding(session.AgentID(), session.Session())
		binding.ToolPolicyDigest = ""
		if err := c.AttachOffer(binding, session); !errors.Is(err, directedoffer.ErrBindingPinsRequired) {
			t.Fatalf("AttachOffer() error = %v, want %v", err, directedoffer.ErrBindingPinsRequired)
		}
	})
}
