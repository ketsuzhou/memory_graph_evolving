package room

import (
	"context"
	"errors"
	"sync"
	"testing"

	"river2.dev/pi-group-chat-host/internal/domain"
)

func benchmarkClosingRequest() EnterClosingRequest {
	return EnterClosingRequest{
		TenantID:      "tenant-benchmark",
		RoomID:        "room-benchmark",
		RoomKind:      BenchmarkRoom,
		ExpectedEpoch: 7,
		RegisteredAgentRuns: []RegisteredAgentRun{
			{AgentRunID: "agent-run-terminal", Terminal: true},
			{AgentRunID: "agent-run-in-flight", Terminal: true},
		},
		Reason: ReasonBarrierReached,
	}
}

func requireLifecycleImplemented(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ErrLifecycleNotImplemented) {
		t.Fatalf("PG-10 Room lifecycle contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected lifecycle error: %v", err)
	}
}

func TestLifecycleClosingAtomicallyRejectsNewWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lifecycle := NewLifecycle(nil)
	entered, err := lifecycle.EnterClosing(ctx, benchmarkClosingRequest())
	requireLifecycleImplemented(t, err)
	if entered.State != LifecycleClosing || entered.RoomEpoch != 8 {
		t.Fatalf("closing event = %#v, want closing at CAS-advanced epoch 8", entered)
	}

	queries := []struct {
		name  string
		check func() (bool, error)
	}{
		{"root turn", func() (bool, error) { return lifecycle.CanAcceptNewRootTurn(ctx, entered.RoomID, entered.RoomEpoch) }},
		{"delegate", func() (bool, error) { return lifecycle.CanAcceptNewDelegate(ctx, entered.RoomID, entered.RoomEpoch) }},
		{"delivery", func() (bool, error) { return lifecycle.CanAcceptNewDelivery(ctx, entered.RoomID, entered.RoomEpoch) }},
		{"segment", func() (bool, error) { return lifecycle.CanAcceptNewSegment(ctx, entered.RoomID, entered.RoomEpoch) }},
	}
	var group sync.WaitGroup
	for _, query := range queries {
		query := query
		group.Add(1)
		go func() {
			defer group.Done()
			accepted, queryErr := query.check()
			if queryErr != nil || accepted {
				t.Errorf("concurrent closing race accepted new %s: accepted=%v err=%v", query.name, accepted, queryErr)
			}
		}()
	}
	group.Wait()
}

func TestLifecycleAllowsRegisteredInFlightWorkToFinish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lifecycle := NewLifecycle(nil)
	entered, err := lifecycle.EnterClosing(ctx, benchmarkClosingRequest())
	requireLifecycleImplemented(t, err)

	allowed, err := lifecycle.CanRegisteredWorkFinish(ctx, entered.RoomID, "agent-run-in-flight", entered.RoomEpoch)
	requireLifecycleImplemented(t, err)
	if !allowed {
		t.Fatal("closing Room rejected an already-registered in-flight run from terminating")
	}
}

func TestLifecycleCloseRequiresReceiptsAndNoOpenWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lifecycle := NewLifecycle(nil)
	entered, err := lifecycle.EnterClosing(ctx, benchmarkClosingRequest())
	requireLifecycleImplemented(t, err)

	for _, request := range []CloseRoomRequest{
		{TenantID: entered.TenantID, RoomID: entered.RoomID, ExpectedEpoch: entered.RoomEpoch, ReceiptsComplete: false, OpenDeliveries: 0, OpenSegments: 0, Reason: ReasonBarrierReached},
		{TenantID: entered.TenantID, RoomID: entered.RoomID, ExpectedEpoch: entered.RoomEpoch, ReceiptsComplete: true, OpenDeliveries: 1, OpenSegments: 0, Reason: ReasonBarrierReached},
		{TenantID: entered.TenantID, RoomID: entered.RoomID, ExpectedEpoch: entered.RoomEpoch, ReceiptsComplete: true, OpenDeliveries: 0, OpenSegments: 1, Reason: ReasonBarrierReached},
	} {
		closed, closeErr := lifecycle.CloseRoom(ctx, request)
		if closeErr == nil || closed.State == LifecycleClosed {
			t.Fatalf("closing→closed bypassed receipts/no-open-work guard: request=%#v result=%#v err=%v", request, closed, closeErr)
		}
	}
	closed, err := lifecycle.CloseRoom(ctx, CloseRoomRequest{
		TenantID: entered.TenantID, RoomID: entered.RoomID, ExpectedEpoch: entered.RoomEpoch,
		ReceiptsComplete: true, OpenDeliveries: 0, OpenSegments: 0, Reason: ReasonBarrierReached,
	})
	requireLifecycleImplemented(t, err)
	if closed.State != LifecycleClosed {
		t.Fatalf("closed event = %#v, want closed after complete receipts and no open work", closed)
	}
}

func TestLifecycleEnterClosingRequiresBarrierAndCatalogReason(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lifecycle := NewLifecycle(nil)

	entered, err := lifecycle.EnterClosing(ctx, benchmarkClosingRequest())
	requireLifecycleImplemented(t, err)
	if entered.Reason != ReasonBarrierReached || entered.State != LifecycleClosing {
		t.Fatalf("barrier closing event = %#v, want closing with BARRIER_REACHED", entered)
	}

	missingBarrier := benchmarkClosingRequest()
	missingBarrier.RegisteredAgentRuns[1].Terminal = false
	if event, err := lifecycle.EnterClosing(ctx, missingBarrier); err == nil || event.State == LifecycleClosing {
		t.Fatalf("active→closing accepted non-terminal registered run: event=%#v err=%v", event, err)
	}
	for _, reason := range []LifecycleReason{"OTHER", "BUDGET_EXHAUSTED", "", ReasonRoomEpochStale} {
		request := benchmarkClosingRequest()
		request.Reason = reason
		if event, err := lifecycle.EnterClosing(ctx, request); err == nil || event.State == LifecycleClosing {
			t.Fatalf("active→closing accepted non-catalog reason %q: event=%#v err=%v", reason, event, err)
		}
	}
	// ROOM_EPOCH_STALE is a rejection reason, never a transition authority:
	// a fresh benchmark Room carrying it must not enter closing.
	staleLifecycle := NewLifecycle(nil)
	fresh := benchmarkClosingRequest()
	fresh.Reason = ReasonRoomEpochStale
	if event, err := staleLifecycle.EnterClosing(ctx, fresh); err == nil || event.State == LifecycleClosing {
		t.Fatalf("fresh benchmark Room entered closing via ROOM_EPOCH_STALE: event=%#v err=%v", event, err)
	}
	// Production rooms have no evaluation barrier and never enter this machine.
	production := benchmarkClosingRequest()
	production.RoomKind = ProductionRoom
	if event, err := lifecycle.EnterClosing(ctx, production); err == nil || event.State == LifecycleClosing {
		t.Fatalf("production Room entered the benchmark close machine: event=%#v err=%v", event, err)
	}
}

func TestLifecycleProductionCutNeverClosesProductionRoom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lifecycle := NewLifecycle(nil)
	canClose, err := lifecycle.CanProductionCutCloseRoom(ctx, domain.RoomID("room-production"))
	requireLifecycleImplemented(t, err)
	if canClose {
		t.Fatal("production ConsolidationCut closed a production Room")
	}
}
