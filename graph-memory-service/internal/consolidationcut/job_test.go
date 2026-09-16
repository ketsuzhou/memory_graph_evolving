package consolidationcut

import (
	"context"
	"errors"
	"testing"
)

func TestResumeOnlyRestartsFailedJobAndCancelIsCooperative(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	authority := newStubGuardAuthority()
	authority.register("annotation-resume-cancel", "diagnosis-v1")
	authority.register(fixtureReceiptsDigest(t, triggerRequest("resume-cancel", ModeForce)))
	service.SetGuardAuthority(authority)
	ctx := context.Background()
	job, _, err := service.Trigger(ctx, triggerRequest("resume-cancel", ModeForce))
	requireCutImplemented(t, err)

	// The frozen bundle routes infrastructure failure from diagnosing (the
	// failed edge set excludes frozen), so walk frozen→diagnosing first.
	diagnosing, err := service.Transition(ctx, TransitionRequest{
		TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: job.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	failed, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StageFailed, Reason: "INFRASTRUCTURE_FAILURE",
	})
	requireCutImplemented(t, err)
	resumed, err := service.Resume(ctx, ResumeRequest{
		TenantID: failed.TenantID, CutID: failed.CutID, ExpectedJobVersion: failed.JobVersion,
	})
	requireCutImplemented(t, err)
	if resumed.Stage != StageFreezing && resumed.Stage != StageDiagnosing && resumed.Stage != StageConsolidating && resumed.Stage != StageReplaying && resumed.Stage != StageActivating {
		t.Fatalf("failed job resumed to illegal stage: %#v", resumed)
	}
	// SC-4.8 ordering: a cancel carrying a stale version is rejected by the
	// state machine, and the current version cancels cooperatively.
	if _, err := service.Cancel(ctx, CancelRequest{TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: resumed.JobVersion - 1}); err == nil {
		t.Fatal("stale-version cancel was not rejected by version CAS")
	}
	cancelled, err := service.Cancel(ctx, CancelRequest{TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: resumed.JobVersion})
	requireCutImplemented(t, err)
	if cancelled.Stage != StageCancelling && cancelled.Stage != StageCancelled && cancelled.Stage != StagePartiallyCancelled {
		t.Fatalf("cooperative cancel produced illegal stage: %#v", cancelled)
	}
	// The worker observes the cancel signal and finishes cancellation
	// without side effects, releasing the room-serial freeze slot.
	if _, err := service.Transition(ctx, TransitionRequest{
		TenantID: cancelled.TenantID, CutID: cancelled.CutID, ExpectedJobVersion: cancelled.JobVersion,
		To: StageCancelled,
	}); err != nil {
		t.Fatalf("cancelling→cancelled: %v", err)
	}
	// Drive a fresh job to each terminal stage and assert resume is refused
	// at that terminal job's CURRENT version — ErrNotResumable semantics,
	// not a stale-version artifact.
	refuseResume := func(t *testing.T, terminal Job) {
		t.Helper()
		if _, err := service.Resume(ctx, ResumeRequest{TenantID: terminal.TenantID, CutID: terminal.CutID, ExpectedJobVersion: terminal.JobVersion}); !errors.Is(err, ErrNotResumable) {
			t.Fatalf("terminal %q job resumed: err=%v", terminal.Stage, err)
		}
	}
	completed := triggerForcedJob(t, service, ctx, "resume-terminal-completed")
	d1, err := service.Transition(ctx, TransitionRequest{TenantID: completed.TenantID, CutID: completed.CutID, ExpectedJobVersion: completed.JobVersion, To: StageDiagnosing})
	requireCutImplemented(t, err)
	c1, err := service.Transition(ctx, TransitionRequest{TenantID: d1.TenantID, CutID: d1.CutID, ExpectedJobVersion: d1.JobVersion, To: StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-resume-cancel", DiagnosisPolicyRevision: "diagnosis-v1"}})
	requireCutImplemented(t, err)
	done, err := service.Transition(ctx, TransitionRequest{
		TenantID: c1.TenantID, CutID: c1.CutID, ExpectedJobVersion: c1.JobVersion, To: StageCompleted,
		SpaceResults: []SpaceResult{{SpaceID: "space-1", Result: SpaceResultPublished}},
	})
	requireCutImplemented(t, err)
	refuseResume(t, done)

	cancelledJob := triggerForcedJob(t, service, ctx, "resume-terminal-cancelled")
	d2, err := service.Transition(ctx, TransitionRequest{TenantID: cancelledJob.TenantID, CutID: cancelledJob.CutID, ExpectedJobVersion: cancelledJob.JobVersion, To: StageDiagnosing})
	requireCutImplemented(t, err)
	cancelling, err := service.Cancel(ctx, CancelRequest{TenantID: d2.TenantID, CutID: d2.CutID, ExpectedJobVersion: d2.JobVersion})
	requireCutImplemented(t, err)
	cancelledTerminal, err := service.Transition(ctx, TransitionRequest{
		TenantID: cancelling.TenantID, CutID: cancelling.CutID, ExpectedJobVersion: cancelling.JobVersion,
		To: StageCancelled,
	})
	requireCutImplemented(t, err)
	refuseResume(t, cancelledTerminal)

	partiallyCancelled := triggerForcedJob(t, service, ctx, "resume-terminal-partial-cancel")
	cancelling2, err := service.Cancel(ctx, CancelRequest{TenantID: partiallyCancelled.TenantID, CutID: partiallyCancelled.CutID, ExpectedJobVersion: partiallyCancelled.JobVersion})
	requireCutImplemented(t, err)
	authority.setSideEffects(cancelling2.CutID, true)
	partialCancelledTerminal, err := service.Transition(ctx, TransitionRequest{
		TenantID: cancelling2.TenantID, CutID: cancelling2.CutID, ExpectedJobVersion: cancelling2.JobVersion,
		To: StagePartiallyCancelled,
	})
	requireCutImplemented(t, err)
	refuseResume(t, partialCancelledTerminal)

	partiallyFailed := triggerForcedJob(t, service, ctx, "resume-terminal-partial-failed")
	d3, err := service.Transition(ctx, TransitionRequest{TenantID: partiallyFailed.TenantID, CutID: partiallyFailed.CutID, ExpectedJobVersion: partiallyFailed.JobVersion, To: StageDiagnosing})
	requireCutImplemented(t, err)
	partialFailedTerminal, err := service.Transition(ctx, TransitionRequest{
		TenantID: d3.TenantID, CutID: d3.CutID, ExpectedJobVersion: d3.JobVersion,
		To: StagePartiallyFailed, Reason: "DIAGNOSIS_FAILED_OR_INCONCLUSIVE",
	})
	requireCutImplemented(t, err)
	refuseResume(t, partialFailedTerminal)
}

func triggerForcedJob(t *testing.T, service *Service, ctx context.Context, key string) Job {
	t.Helper()
	// The frozen guard consults the configured authority for the receipts
	// fact once the job is recorded at freezing, so a stub whitelist
	// authority must have that fact registered before the trigger lands.
	if registrar, ok := service.guard.(interface{ register(refs ...string) }); ok {
		registrar.register(fixtureReceiptsDigest(t, triggerRequest(key, ModeForce)))
	}
	job, _, err := service.Trigger(ctx, triggerRequest(key, ModeForce))
	requireCutImplemented(t, err)
	return job
}

func TestTriggerIdempotencyReplaysSameBodyAndRejectsChangedBody(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	first, duplicate, err := service.Trigger(context.Background(), triggerRequest("same-key", ModeThreshold))
	requireCutImplemented(t, err)
	if duplicate || first.CutID == "" {
		t.Fatalf("first trigger = (%#v, duplicate=%v), want new cut", first, duplicate)
	}
	retry, duplicate, err := service.Trigger(context.Background(), triggerRequest("same-key", ModeThreshold))
	requireCutImplemented(t, err)
	if !duplicate || retry.CutID != first.CutID {
		t.Fatalf("same key/body trigger = (%#v, duplicate=%v), want original cut %q", retry, duplicate, first.CutID)
	}
	if _, _, err := service.Trigger(context.Background(), triggerRequest("same-key", ModeForce)); err == nil {
		t.Fatal("same idempotency key with changed body did not conflict with CUT_IDEMPOTENCY_CONFLICT semantics")
	}
}

func TestRediagnoseCreatesNewRevisionNotResume(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	job := walkToPartiallyFailed(t, service, ctx, "rediagnose")
	rediagnosed, err := service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: job.TenantID, PrincipalID: "governance-1", CutID: job.CutID, ExpectedJobVersion: job.JobVersion,
		DiagnosisPolicyRevision: "diagnosis-v2", NewDiagnosisRunRef: "diagnosis-run-rediagnose-2",
	})
	requireCutImplemented(t, err)
	if rediagnosed.Stage != StageDiagnosing || rediagnosed.CutID != job.CutID {
		t.Fatalf("rediagnose must re-enter diagnosing against the same frozen Cut: original=%#v rediagnosed=%#v", job, rediagnosed)
	}
}

// SC-4.1: Resume and Rediagnose re-enter an in-flight stage, so they must be
// refused while the room is occupied by ANOTHER in-flight cut — and allowed
// once the room clears, rebuilding their own room occupancy and clearing the
// stale last_cut recorded at their failure (the room-serial ledgers keep
// holding for state reached without a restart).
func TestResumeAndRediagnoseRejectRoomOccupiedByAnotherCut(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	authority := newStubGuardAuthority()
	authority.register("annotation-room-busy", "diagnosis-v1")
	service.SetGuardAuthority(authority)
	ctx := context.Background()

	// Failed B: with the room still free, a transient authority failure fails
	// it at the frozen guard, which releases the room — B stays
	// resumable-eligible but boxed out once anyone else takes the room.
	authority.setErr(errors.New("receipts store unavailable"))
	failedJob, _, err := service.Trigger(ctx, triggerRequest("room-busy-failed", ModeForce))
	authority.setErr(nil)
	if !errors.Is(err, ErrGuardAuthorityUnavailable) || failedJob.Stage != StageFailed {
		t.Fatalf("seed failed job = (job=%#v, err=%v), want transiently failed", failedJob, err)
	}

	// Rediagnose seed X: frozen, then driven to partially_failed — a terminal
	// stage, so it releases the room.
	rediagnoseSeed := triggerForcedJob(t, service, ctx, "room-busy-rediagnose")
	diagnosing, err := service.Transition(ctx, TransitionRequest{
		TenantID: rediagnoseSeed.TenantID, CutID: rediagnoseSeed.CutID, ExpectedJobVersion: rediagnoseSeed.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	partiallyFailed, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StagePartiallyFailed, Reason: "DIAGNOSIS_FAILED_OR_INCONCLUSIVE",
	})
	requireCutImplemented(t, err)

	// Occupant A: frozen and now holding the room both re-entries want.
	occupant := triggerForcedJob(t, service, ctx, "room-busy-occupant")
	if _, err := service.Resume(ctx, ResumeRequest{
		TenantID: failedJob.TenantID, CutID: failedJob.CutID, ExpectedJobVersion: failedJob.JobVersion,
	}); !errors.Is(err, ErrRoomOccupied) {
		t.Fatalf("resume while room occupied by %s = %v, want ErrRoomOccupied", occupant.CutID, err)
	}
	if _, err := service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: partiallyFailed.TenantID, PrincipalID: "governance-1", CutID: partiallyFailed.CutID, ExpectedJobVersion: partiallyFailed.JobVersion,
		DiagnosisPolicyRevision: "diagnosis-v2", NewDiagnosisRunRef: "diagnosis-run-room-busy-2",
	}); !errors.Is(err, ErrRoomOccupied) {
		t.Fatalf("rediagnose while room occupied by %s = %v, want ErrRoomOccupied", occupant.CutID, err)
	}

	// Once the room clears, both re-entries succeed and rebuild their room
	// occupancy (one at a time — the room stays single-occupancy).
	dOccupant, err := service.Transition(ctx, TransitionRequest{
		TenantID: occupant.TenantID, CutID: occupant.CutID, ExpectedJobVersion: occupant.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	if _, err := service.Transition(ctx, TransitionRequest{
		TenantID: dOccupant.TenantID, CutID: dOccupant.CutID, ExpectedJobVersion: dOccupant.JobVersion,
		To: StagePartiallyFailed, Reason: "DIAGNOSIS_FAILED_OR_INCONCLUSIVE",
	}); err != nil {
		t.Fatalf("clear the room: %v", err)
	}
	rediagnosed, err := service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: partiallyFailed.TenantID, PrincipalID: "governance-1", CutID: partiallyFailed.CutID, ExpectedJobVersion: partiallyFailed.JobVersion,
		DiagnosisPolicyRevision: "diagnosis-v2", NewDiagnosisRunRef: "diagnosis-run-room-busy-2",
	})
	requireCutImplemented(t, err)
	if rediagnosed.Stage != StageDiagnosing {
		t.Fatalf("rediagnosed stage = %s, want diagnosing", rediagnosed.Stage)
	}
	dx, err := service.Transition(ctx, TransitionRequest{
		TenantID: rediagnosed.TenantID, CutID: rediagnosed.CutID, ExpectedJobVersion: rediagnosed.JobVersion,
		To: StagePartiallyFailed, Reason: "DIAGNOSIS_FAILED_OR_INCONCLUSIVE",
	})
	requireCutImplemented(t, err)
	if dx.Stage != StagePartiallyFailed {
		t.Fatalf("post-rediagnose walk stage = %s, want partially_failed", dx.Stage)
	}
	resumed, err := service.Resume(ctx, ResumeRequest{
		TenantID: failedJob.TenantID, CutID: failedJob.CutID, ExpectedJobVersion: failedJob.JobVersion,
	})
	requireCutImplemented(t, err)
	if !inFlight(resumed.Stage) {
		t.Fatalf("resumed stage = %s, want an in-flight stage", resumed.Stage)
	}
}

// The room-serial ledger converges on ANY exit from the in-flight set —
// failed included: the next trigger for the room must not be blocked by a
// dead cut, and a snapshot taken after failures must not restore stale
// occupancy (a restored service accepts a fresh trigger for the room).
func TestFailedTransitionReleasesRoomForNextTrigger(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()

	first := triggerForcedJob(t, service, ctx, "failed-release-first")
	diagnosing, err := service.Transition(ctx, TransitionRequest{
		TenantID: first.TenantID, CutID: first.CutID, ExpectedJobVersion: first.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	if _, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StageFailed, Reason: "INFRASTRUCTURE_FAILURE",
	}); err != nil {
		t.Fatalf("diagnosing→failed: %v", err)
	}

	// Same room, NEW key: the failed cut must not block the room.
	next := triggerForcedJob(t, service, ctx, "failed-release-next")
	if next.CutID == first.CutID {
		t.Fatal("trigger after a failed cut returned the dead cut: room was not released")
	}
	nextDiagnosing, err := service.Transition(ctx, TransitionRequest{
		TenantID: next.TenantID, CutID: next.CutID, ExpectedJobVersion: next.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	if _, err := service.Transition(ctx, TransitionRequest{
		TenantID: nextDiagnosing.TenantID, CutID: nextDiagnosing.CutID, ExpectedJobVersion: nextDiagnosing.JobVersion,
		To: StageFailed, Reason: "INFRASTRUCTURE_FAILURE",
	}); err != nil {
		t.Fatalf("second diagnosing→failed: %v", err)
	}

	// Snapshot/restore after the failures: neither failed cut may come back
	// occupying the room.
	image, err := service.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := NewService(nil, nil)
	if err := restored.Restore(image); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restoredFirst, err := restored.Get(ctx, first.TenantID, first.CutID)
	requireCutImplemented(t, err)
	if restoredFirst.Stage != StageFailed {
		t.Fatalf("restored failed cut stage = %s, want failed", restoredFirst.Stage)
	}
	fresh := triggerForcedJob(t, restored, ctx, "failed-release-third")
	if fresh.CutID == first.CutID || fresh.CutID == next.CutID {
		t.Fatalf("restored service handed back a dead cut %s: stale occupancy was restored", fresh.CutID)
	}
}

// The legal failed→resume round trip survives snapshot/restore exactly: the
// restored failed job resumes into ITS OWN failure origin (diagnosing here),
// not merely "some in-flight stage".
func TestFailedSnapshotRestoreResumeRestoresExactFailureOrigin(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	authority := newStubGuardAuthority()
	authority.register("annotation-exact-origin", "diagnosis-v1")
	service.SetGuardAuthority(authority)
	ctx := context.Background()

	job := triggerForcedJob(t, service, ctx, "exact-origin")
	diagnosing, err := service.Transition(ctx, TransitionRequest{
		TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: job.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	if _, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StageFailed, Reason: "INFRASTRUCTURE_FAILURE",
	}); err != nil {
		t.Fatalf("diagnosing→failed: %v", err)
	}

	image, err := service.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := NewService(nil, nil)
	if err := restored.Restore(image); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restoredJob, err := restored.Get(ctx, job.TenantID, job.CutID)
	requireCutImplemented(t, err)
	if restoredJob.Stage != StageFailed || restoredJob.FailedFrom != StageDiagnosing {
		t.Fatalf("restored failed job = (stage=%s, failedFrom=%s), want (failed, diagnosing)", restoredJob.Stage, restoredJob.FailedFrom)
	}
	resumed, err := restored.Resume(ctx, ResumeRequest{
		TenantID: restoredJob.TenantID, CutID: restoredJob.CutID, ExpectedJobVersion: restoredJob.JobVersion,
	})
	requireCutImplemented(t, err)
	if resumed.Stage != StageDiagnosing {
		t.Fatalf("resumed stage = %s, want the exact failure origin diagnosing", resumed.Stage)
	}
}
