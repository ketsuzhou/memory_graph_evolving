package consolidationcut

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func triggerRequest(key string, mode Mode) TriggerRequest {
	return TriggerRequest{
		TenantID: "tenant-1", PrincipalID: "operator-1", RoomID: "room-1",
		Mode: mode, TriggerSource: "host-service", IdempotencyKey: key,
	}
}

// fixtureReceiptsDigest computes the canonical receipts digest the
// deterministic fixture freezer freezes for req, so a test can register the
// receipts fact with its stub authority BEFORE triggering (the frozen guard
// consults the authority once the job is recorded at freezing).
func fixtureReceiptsDigest(t *testing.T, req TriggerRequest) string {
	t.Helper()
	probe := NewService(nil, nil)
	manifest, err := probe.freezeLocked(context.Background(), req)
	if err != nil {
		t.Fatalf("fixture freeze: %v", err)
	}
	return ManifestReceiptsDigest(manifest)
}

func requireCutImplemented(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ErrCutNotImplemented) {
		t.Fatalf("PG-20 Cut job contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected Cut job error: %v", err)
	}
}

func TestTriggerSerializesFreezePerRoom(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	requests := []TriggerRequest{
		triggerRequest("room-serial-1", ModeForce),
		triggerRequest("room-serial-2", ModeForce),
	}
	results := make(chan struct {
		job Job
		err error
	}, len(requests))
	var group sync.WaitGroup
	for _, request := range requests {
		request := request
		group.Add(1)
		go func() {
			defer group.Done()
			job, _, err := service.Trigger(ctx, request)
			results <- struct {
				job Job
				err error
			}{job, err}
		}()
	}
	group.Wait()
	close(results)
	for result := range results {
		requireCutImplemented(t, result.err)
	}
	// Green contract: exactly one job freezes; the competing Room trigger is
	// rejected with an auditable conflict rather than overlapping a freeze.
}

func TestTriggerThresholdReturnsAuditableNoChangeAndForceBypassesThreshold(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	threshold, _, err := service.Trigger(context.Background(), triggerRequest("threshold-empty", ModeThreshold))
	requireCutImplemented(t, err)
	if threshold.Outcome != SpaceResultNoChange || threshold.Stage != StageCompleted {
		t.Fatalf("below-threshold trigger = %#v, want auditable completed no_change", threshold)
	}
	forced, _, err := service.Trigger(context.Background(), triggerRequest("force-empty", ModeForce))
	requireCutImplemented(t, err)
	if forced.CutID == "" || forced.Outcome == SpaceResultNoChange {
		t.Fatalf("force trigger did not bypass threshold: %#v", forced)
	}
}

func TestFrozenManifestIsImmutableAndPinsExactEvidence(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	job, _, err := service.Trigger(context.Background(), triggerRequest("immutable-manifest", ModeForce))
	requireCutImplemented(t, err)
	first, err := service.GetManifest(context.Background(), job.TenantID, job.CutID)
	requireCutImplemented(t, err)
	second, err := service.GetManifest(context.Background(), job.TenantID, job.CutID)
	requireCutImplemented(t, err)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("frozen manifest changed across reads: first=%#v second=%#v", first, second)
	}
	if len(first.SpaceScopes) == 0 || len(first.SealedSegmentIDs) == 0 || len(first.EvidenceCommitReceipts) != len(first.SealedSegmentIDs) {
		t.Fatalf("manifest omitted immutable exact evidence/receipt facts: %#v", first)
	}
	for _, scope := range first.SpaceScopes {
		if len(scope.EvidenceBatchIDs) == 0 || scope.ProjectionHeadVersion == nil || scope.QueryWatermark == nil {
			t.Fatalf("space scope is not pinned to exact batches/head/query state: %#v", scope)
		}
	}
}

func TestJobStateMachineRejectsIllegalTransitionsAndRecordsPartialDiagnosisFailure(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	authority := newStubGuardAuthority()
	authority.register("projection-record-state-machine")
	authority.register(fixtureReceiptsDigest(t, triggerRequest("state-machine", ModeForce)))
	service.SetGuardAuthority(authority)
	job, _, err := service.Trigger(context.Background(), triggerRequest("state-machine", ModeForce))
	requireCutImplemented(t, err)
	// The frozen cut_job bundle has no frozen→completed edge: completion
	// without consolidation is illegal.
	if _, err := service.Transition(context.Background(), TransitionRequest{TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: job.JobVersion, To: StageCompleted}); err == nil {
		t.Fatal("illegal frozen→completed transition was accepted")
	}
	diagnosing, err := service.Transition(context.Background(), TransitionRequest{
		TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: job.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	// SC-4.3: a diagnosis failure terminates the cut partially_failed.
	failedDiagnosis, err := service.Transition(context.Background(), TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StagePartiallyFailed, Reason: "DIAGNOSIS_FAILED_OR_INCONCLUSIVE",
	})
	requireCutImplemented(t, err)
	if failedDiagnosis.Stage != StagePartiallyFailed {
		t.Fatalf("diagnosis failure terminal stage = %q, want partially_failed", failedDiagnosis.Stage)
	}
	// SC-4.3 projection-only continuation then exposes consolidating_partial.
	partial, err := service.Transition(context.Background(), TransitionRequest{
		TenantID: failedDiagnosis.TenantID, CutID: failedDiagnosis.CutID, ExpectedJobVersion: failedDiagnosis.JobVersion,
		To: StageConsolidatingPartial,
	})
	requireCutImplemented(t, err)
	if partial.Stage != StageConsolidatingPartial {
		t.Fatalf("diagnosis failure did not expose consolidating_partial: %#v", partial)
	}
	terminal, err := service.Transition(context.Background(), TransitionRequest{
		TenantID: partial.TenantID, CutID: partial.CutID, ExpectedJobVersion: partial.JobVersion,
		To: StagePartiallyFailed, Reason: "DIAGNOSIS_FAILED_OR_INCONCLUSIVE",
		Evidence: TransitionEvidence{ProjectionRecordDigest: "projection-record-state-machine"},
	})
	requireCutImplemented(t, err)
	if terminal.Stage != StagePartiallyFailed {
		t.Fatalf("diagnosis failure terminal stage = %q, want partially_failed", terminal.Stage)
	}
	// P0 fix recorded in the frozen bundle: partially_failed is terminal and
	// MUST NOT complete.
	if _, err := service.Transition(context.Background(), TransitionRequest{TenantID: terminal.TenantID, CutID: terminal.CutID, ExpectedJobVersion: terminal.JobVersion, To: StageCompleted}); err == nil {
		t.Fatal("illegal partially_failed→completed transition was accepted")
	}
}
