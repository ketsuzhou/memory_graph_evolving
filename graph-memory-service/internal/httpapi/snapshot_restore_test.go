package httpapi_test

import (
	"context"
	"net/http"
	"testing"

	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/httpapi"
)

// TestConsolidationCutCompositionSurvivesRestart exercises the PG-50A
// start/stop restorability contract (acceptance item 5): the cut service job
// state AND the production freezer's threshold ledger are snapshotted and
// restored together, so a process restart resumes the authoritative cut and
// every room's sealed SEGMENT history rather than resealing from scratch.
func TestConsolidationCutCompositionSurvivesRestart(t *testing.T) {
	composition := cutContractComposition(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, composition.server)

	// Create a force cut; it seals consolidation-segment-1 and lands frozen.
	status, _, raw := cutRequest(t, composition.server, http.MethodPost,
		"/v1/rooms/consolidation-room/consolidation-cuts", "key-restart", `{"mode":"force","trigger_source":"restart"}`)
	if status != http.StatusAccepted {
		t.Fatalf("create status = %d; body=%s", status, raw)
	}
	dto := cutDecode(t, raw)
	cutID := dto["cut_id"].(string)
	stage := dto["stage"].(string)
	if stage != "frozen" {
		t.Fatalf("expected a frozen cut for force seal, got stage %q", stage)
	}

	// Capture the durable composition image (the <state>.cuts.json payload).
	serviceImage, err := composition.service.Snapshot()
	if err != nil {
		t.Fatalf("service snapshot: %v", err)
	}
	freezerImage, err := composition.freezer.Snapshot()
	if err != nil {
		t.Fatalf("freezer snapshot: %v", err)
	}

	// A fresh process: a new Service and new RoomFreezer over the same
	// (persisted) store, with the images loaded at boot.
	freshFreezer := httpapi.NewRoomFreezer(composition.store, composition.store,
		map[string][]domain.SpaceID{"consolidation-room": {domain.SpaceID(cutSpacePath)}})
	fresh := consolidationcut.NewService(nil, freshFreezer)
	if err := fresh.Restore(serviceImage); err != nil {
		t.Fatalf("service restore: %v", err)
	}
	if err := freshFreezer.Restore(freezerImage); err != nil {
		t.Fatalf("freezer restore: %v", err)
	}

	// The authoritative cut is still readable with identical identity.
	got, err := fresh.Get(context.Background(), "consolidation-tenant", consolidationcut.CutID(cutID))
	if err != nil {
		t.Fatalf("restored get: %v", err)
	}
	if got.CutID != consolidationcut.CutID(cutID) || got.Stage != consolidationcut.Stage(stage) {
		t.Fatalf("restored job mismatch: got stage %s cut %s, want %s/%s", got.Stage, got.CutID, stage, cutID)
	}

	manifest, err := fresh.GetManifest(context.Background(), "consolidation-tenant", consolidationcut.CutID(cutID))
	if err != nil {
		t.Fatalf("restored manifest: %v", err)
	}
	if len(manifest.SealedSegmentIDs) != 1 || manifest.SealedSegmentIDs[0] != "consolidation-segment-1" {
		t.Fatalf("restored manifest sealed segments = %v, want [consolidation-segment-1]", manifest.SealedSegmentIDs)
	}

	// Idempotency survives restart: the exact original trigger replays the
	// restored job rather than minting a new cut.
	replayed, replay, err := fresh.Trigger(context.Background(), consolidationcut.TriggerRequest{
		TenantID:       "consolidation-tenant",
		PrincipalID:    "host-service",
		RoomID:         "consolidation-room",
		Mode:           consolidationcut.ModeForce,
		TriggerSource:  "restart",
		IdempotencyKey: "key-restart",
	})
	if err != nil {
		t.Fatalf("restored idempotent trigger: %v", err)
	}
	if !replay || replayed.CutID != consolidationcut.CutID(cutID) {
		t.Fatalf("restored idempotency broken: replay=%v cut=%s want %s", replay, replayed.CutID, cutID)
	}

	// Freezer threshold ledger survives: the already-sealed segment is
	// excluded from a fresh threshold freeze, so a restart does not reseal.
	thresholdManifest, err := freshFreezer.Freeze(context.Background(), consolidationcut.TriggerRequest{
		TenantID:       "consolidation-tenant",
		PrincipalID:    "host-service",
		RoomID:         "consolidation-room",
		Mode:           consolidationcut.ModeThreshold,
		TriggerSource:  "restart-threshold",
		IdempotencyKey: "key-restart-threshold",
	})
	if err != nil {
		t.Fatalf("restored threshold freeze: %v", err)
	}
	if len(thresholdManifest.SealedSegmentIDs) != 0 {
		t.Fatalf("restored threshold freeze resealed segments %v, want none (already sealed)", thresholdManifest.SealedSegmentIDs)
	}

	// Cancellation also survives: cancel the restored (frozen) cut.
	cancelled, err := fresh.Cancel(context.Background(), consolidationcut.CancelRequest{
		TenantID:           "consolidation-tenant",
		PrincipalID:        "host-service",
		CutID:              consolidationcut.CutID(cutID),
		ExpectedJobVersion: got.JobVersion,
	})
	if err != nil {
		t.Fatalf("cancel restored cut: %v", err)
	}
	if cancelled.Stage != consolidationcut.StageCancelling {
		t.Fatalf("restored cancel stage = %s, want cancelling", cancelled.Stage)
	}
}
