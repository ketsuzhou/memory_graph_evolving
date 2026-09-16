package consolidationcut

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

// stubGuardAuthority resolves evidence references against registered fact
// sets: an unregistered (forged) reference fails closed, mirroring the real
// authorities. It is safe for concurrent use (the service queries it between
// two CAS-checked lock windows). The mutate hooks fire after the Nth call of
// a method so tests can revoke or drift facts between Phase B resolution and
// the Phase C re-resolve. Fact mutations (register/withdraw/proposals/side
// effects) serialize behind active reservations (ReserveFacts), mirroring the
// R3 apply-time authority CAS contract.
type stubGuardAuthority struct {
	mu              sync.Mutex
	mutatorsWaiting *sync.Cond
	reserved        int
	refs            map[string]bool
	proposals       map[CutID][]string
	sideEffects     map[CutID]bool
	err             error
	diagnosisCalls  int
	// revokeDiagnosisAfter makes DiagnosisVerified fail closed once the
	// call count exceeds it (0 = never).
	revokeDiagnosisAfter int
	// driftDiagnosisAfter makes DiagnosisVerified resolve the same verdict
	// over drifted facts once the call count exceeds it (0 = never).
	driftDiagnosisAfter int
}

func newStubGuardAuthority() *stubGuardAuthority {
	g := &stubGuardAuthority{
		refs:        map[string]bool{},
		proposals:   map[CutID][]string{},
		sideEffects: map[CutID]bool{},
	}
	g.mutatorsWaiting = sync.NewCond(&g.mu)
	return g
}

// lockForMutation acquires the fact lock and blocks while any transition
// holds a fact reservation: a revocation cannot interleave between the final
// guard validation and the apply (R3).
func (g *stubGuardAuthority) lockForMutation() {
	g.mu.Lock()
	for g.reserved > 0 {
		g.mutatorsWaiting.Wait()
	}
}

func (g *stubGuardAuthority) register(refs ...string) {
	g.lockForMutation()
	defer g.mu.Unlock()
	for _, ref := range refs {
		g.refs[ref] = true
	}
}

// withdraw revokes registered fact references (the governance revocation
// path): it blocks while a transition holds a fact reservation.
func (g *stubGuardAuthority) withdraw(refs ...string) {
	g.lockForMutation()
	defer g.mu.Unlock()
	for _, ref := range refs {
		delete(g.refs, ref)
	}
}

func (g *stubGuardAuthority) setSideEffects(cutID CutID, completed bool) {
	g.lockForMutation()
	defer g.mu.Unlock()
	g.sideEffects[cutID] = completed
}

func (g *stubGuardAuthority) registerProposal(cutID CutID, ref string) {
	g.lockForMutation()
	defer g.mu.Unlock()
	g.proposals[cutID] = append(g.proposals[cutID], ref)
}

// ReserveFacts pins the facts for one transition apply: mutations block until
// the matching release, and the service's final guard validation runs under
// the reservation (R3).
func (g *stubGuardAuthority) ReserveFacts(context.Context, domain.TenantID, CutID) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reserved++
	return nil
}

func (g *stubGuardAuthority) ReleaseFacts(context.Context, domain.TenantID, CutID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reserved > 0 {
		g.reserved--
	}
	g.mutatorsWaiting.Broadcast()
}

func (g *stubGuardAuthority) setErr(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.err = err
}

func (g *stubGuardAuthority) known(ref string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return ref != "" && g.refs[ref]
}

func (g *stubGuardAuthority) failIfSet() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

func (g *stubGuardAuthority) QuotaGranted(_ context.Context, _ domain.TenantID, _ CutID, grantRef string) (bool, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return false, GuardConfirmation{}, err
	}
	ok := g.known(grantRef)
	return ok, ConfirmFacts("quota_budget_granted", map[string]string{"grant": grantRef, "resolved": strconv.FormatBool(ok)}), nil
}

func (g *stubGuardAuthority) ReceiptsComplete(_ context.Context, _ domain.TenantID, _ CutID, receiptsDigest string) (bool, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return false, GuardConfirmation{}, err
	}
	ok := g.known(receiptsDigest)
	return ok, ConfirmFacts("receipts_complete_for_all_sealed_segments", map[string]string{"receipts": receiptsDigest, "resolved": strconv.FormatBool(ok)}), nil
}

func (g *stubGuardAuthority) DiagnosisVerified(_ context.Context, _ domain.TenantID, _ CutID, annotationRef, policyRevision string) (bool, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return false, GuardConfirmation{}, err
	}
	g.mu.Lock()
	g.diagnosisCalls++
	calls := g.diagnosisCalls
	revokeAfter, driftAfter := g.revokeDiagnosisAfter, g.driftDiagnosisAfter
	g.mu.Unlock()
	ok := g.known(annotationRef) && g.known(policyRevision)
	facts := map[string]string{"annotation": annotationRef, "policy": policyRevision, "resolved": strconv.FormatBool(ok)}
	if revokeAfter > 0 && calls > revokeAfter {
		return false, ConfirmFacts("diagnosis_verified", map[string]string{"annotation": annotationRef, "revoked": "true"}), nil
	}
	if driftAfter > 0 && calls > driftAfter {
		facts["revision_seen"] = fmt.Sprintf("call-%d", calls)
	}
	return ok, ConfirmFacts("diagnosis_verified", facts), nil
}

func (g *stubGuardAuthority) ProjectionComplete(_ context.Context, _ domain.TenantID, _ CutID, recordDigest string) (bool, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return false, GuardConfirmation{}, err
	}
	ok := g.known(recordDigest)
	return ok, ConfirmFacts("projection_complete", map[string]string{"projection": recordDigest, "resolved": strconv.FormatBool(ok)}), nil
}

func (g *stubGuardAuthority) ProposalRefs(_ context.Context, _ domain.TenantID, cutID CutID) ([]string, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return nil, GuardConfirmation{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	refs := append([]string(nil), g.proposals[cutID]...)
	return refs, ConfirmFacts("proposal_refs", map[string]string{"cut": string(cutID), "refs": strconv.Itoa(len(refs))}), nil
}

func (g *stubGuardAuthority) ReplayPassed(_ context.Context, _ domain.TenantID, _ CutID, replayResultRef string) (bool, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return false, GuardConfirmation{}, err
	}
	ok := g.known(replayResultRef)
	return ok, ConfirmFacts("replay_pass_and_target_binding_exact", map[string]string{"replay": replayResultRef, "resolved": strconv.FormatBool(ok)}), nil
}

func (g *stubGuardAuthority) CASBaseRevisionMatches(_ context.Context, _ domain.TenantID, _ CutID, baseRevision string) (bool, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return false, GuardConfirmation{}, err
	}
	ok := g.known(baseRevision)
	return ok, ConfirmFacts("cas_base_revision_matches", map[string]string{"base": baseRevision, "resolved": strconv.FormatBool(ok)}), nil
}

func (g *stubGuardAuthority) SideEffectsCompleted(_ context.Context, _ domain.TenantID, cutID CutID) (bool, GuardConfirmation, error) {
	if err := g.failIfSet(); err != nil {
		return false, GuardConfirmation{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	completed := g.sideEffects[cutID]
	return completed, ConfirmFacts("side_effects_completed", map[string]string{"cut": string(cutID), "completed": strconv.FormatBool(completed)}), nil
}

// walkToDiagnosing drives a freshly triggered frozen job to diagnosing.
func walkToDiagnosing(t *testing.T, service *Service, ctx context.Context, key string) Job {
	t.Helper()
	job := triggerForcedJob(t, service, ctx, key)
	diagnosing, err := service.Transition(ctx, TransitionRequest{
		TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: job.JobVersion, To: StageDiagnosing,
	})
	requireCutImplemented(t, err)
	return diagnosing
}

// walkToPartiallyFailed drives a job to partially_failed via the canonical
// diagnosis-failure reason (a request-local guard, no authority involved).
func walkToPartiallyFailed(t *testing.T, service *Service, ctx context.Context, key string) Job {
	t.Helper()
	diagnosing := walkToDiagnosing(t, service, ctx, key)
	partiallyFailed, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StagePartiallyFailed, Reason: reasonDiagnosisFailed,
	})
	requireCutImplemented(t, err)
	return partiallyFailed
}

// P0-5: guard booleans are gone; a nil authority fails every guarded edge
// closed even when the request carries plausible-looking evidence refs.
func TestGuardedTransitionFailsClosedWithoutAuthority(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	diagnosing := walkToDiagnosing(t, service, ctx, "guard-nil-authority")
	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-any", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("guarded edge without authority = %v, want ErrGuardNotSatisfied", err)
	}
	// The rejection is auditable.
	found := false
	for _, entry := range service.AuditTrail(diagnosing.TenantID) {
		if entry.Kind == "transition_guard_rejected" && strings.Contains(entry.Detail, "no guard authority configured") {
			found = true
		}
	}
	if !found {
		t.Fatal("nil-authority guard rejection was not audited")
	}
}

// The initial freeze must not bypass the guard authority: the recorded
// freezing→frozen edge consults the same protocol as Transition, so a
// receipts fact the authority rejects fails the freeze closed (job failed
// RECEIPT_MISSING, audited, idempotent replay lands on the failed job), and
// an admitted freeze carries the authority-stamped evidence digest.
func TestInitialFreezeConsultsGuardAuthorityAtFrozenGuard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("rejected receipts fact fails the freeze closed", func(t *testing.T) {
		service := NewService(nil, nil)
		authority := newStubGuardAuthority()
		authority.register("sha256:wrong-receipts-fact")
		service.SetGuardAuthority(authority)

		job, _, err := service.Trigger(ctx, triggerRequest("authority-frozen-reject", ModeForce))
		if err == nil || !strings.Contains(err.Error(), "receipts_complete_for_all_sealed_segments") {
			t.Fatalf("trigger with unregistered receipts fact = (%#v, %v), want frozen-guard rejection", job, err)
		}
		stored, getErr := service.Get(ctx, job.TenantID, job.CutID)
		if getErr != nil {
			t.Fatalf("get frozen-rejected job: %v", getErr)
		}
		if stored.Stage != StageFailed {
			t.Fatalf("rejected freeze job stage = %s, want failed", stored.Stage)
		}
		reasons := map[string]bool{}
		for _, reason := range stored.FailureReasons {
			reasons[reason] = true
		}
		if !reasons["RECEIPT_MISSING"] {
			t.Fatalf("rejected freeze failure reasons = %#v, want RECEIPT_MISSING", stored.FailureReasons)
		}
		audited := false
		for _, entry := range service.AuditTrail(job.TenantID) {
			if entry.Kind == "trigger_frozen_rejected" && strings.Contains(entry.Detail, "receipts_complete_for_all_sealed_segments") {
				audited = true
			}
		}
		if !audited {
			t.Fatal("frozen-guard rejection was not audited")
		}
		// The failed trigger is idempotency-bound: the same key+body replays
		// the failed job instead of re-freezing over the rejection.
		replay, duplicate, err := service.Trigger(ctx, triggerRequest("authority-frozen-reject", ModeForce))
		if err != nil || !duplicate || replay.CutID != job.CutID || replay.Stage != StageFailed {
			t.Fatalf("replay of rejected freeze = (%#v, %v, %v), want failed duplicate", replay, duplicate, err)
		}
	})

	t.Run("admitted receipts fact stamps the authority evidence", func(t *testing.T) {
		service := NewService(nil, nil)
		authority := newStubGuardAuthority()
		authority.register(fixtureReceiptsDigest(t, triggerRequest("authority-frozen-admit", ModeForce)))
		service.SetGuardAuthority(authority)

		job, _, err := service.Trigger(ctx, triggerRequest("authority-frozen-admit", ModeForce))
		requireCutImplemented(t, err)
		if job.Stage != StageFrozen {
			t.Fatalf("admitted freeze job stage = %s, want frozen", job.Stage)
		}
		if job.LastTransitionEvidence.ReceiptsDigest == "" || job.LastTransitionEvidence.AuthorityFactsDigest == "" {
			t.Fatalf("admitted freeze was not stamped with authority evidence: %#v", job.LastTransitionEvidence)
		}
		manifest, err := service.GetManifest(ctx, job.TenantID, job.CutID)
		if err != nil {
			t.Fatalf("get admitted manifest: %v", err)
		}
		if ManifestReceiptsDigest(manifest) != job.LastTransitionEvidence.ReceiptsDigest {
			t.Fatalf("stamped receipts digest %q does not match the frozen manifest %q", job.LastTransitionEvidence.ReceiptsDigest, ManifestReceiptsDigest(manifest))
		}
	})
}

// P0-5: a forged evidence reference the authority does not recognize is
// rejected; only a registered immutable fact admits the edge. The admitted
// transition stamps the evidence into the job and the audit trail.
func TestForgedEvidenceRefsRejectedAndRealEvidenceStamped(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-real", "diagnosis-v1")
	service.SetGuardAuthority(authority)

	diagnosing := walkToDiagnosing(t, service, ctx, "guard-forged-evidence")
	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-forged", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("forged annotation ref = %v, want ErrGuardNotSatisfied", err)
	}
	// Missing evidence refs also fail closed.
	_, err = service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StageConsolidating,
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("empty evidence = %v, want ErrGuardNotSatisfied", err)
	}
	consolidating, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	requireCutImplemented(t, err)
	if consolidating.LastTransitionEvidence.DiagnosisAnnotationRef != "annotation-real" {
		t.Fatalf("admitted transition did not stamp evidence into the job: %#v", consolidating.LastTransitionEvidence)
	}
	stamped := false
	for _, entry := range service.AuditTrail(diagnosing.TenantID) {
		if entry.Kind == "transition" && strings.Contains(entry.Detail, "annotation=annotation-real") {
			stamped = true
		}
	}
	if !stamped {
		t.Fatal("admitted transition audit detail does not carry the evidence ref")
	}
	// A forged non-terminal SpaceResult still fails the local guard even
	// with a perfectly cooperative authority.
	_, err = service.Transition(ctx, TransitionRequest{
		TenantID: consolidating.TenantID, CutID: consolidating.CutID, ExpectedJobVersion: consolidating.JobVersion,
		To:           StageCompleted,
		SpaceResults: []SpaceResult{{SpaceID: "space-1", Result: SpaceResultPending}},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("forged non-terminal SpaceResult = %v, want ErrGuardNotSatisfied", err)
	}
}

// P0-5: an authority error fails closed (wrapped), never admits the edge.
func TestGuardAuthorityErrorFailsClosed(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-x", "diagnosis-v1")
	service.SetGuardAuthority(authority)
	diagnosing := walkToDiagnosing(t, service, ctx, "guard-authority-error")
	authority.setErr(errors.New("diagnosis store unavailable"))
	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-x", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardAuthorityUnavailable) || !strings.Contains(err.Error(), "diagnosis store unavailable") {
		t.Fatalf("authority error = %v, want wrapped ErrGuardAuthorityUnavailable", err)
	}
}

// P1: a TRANSIENT authority read failure during the initial freeze classifies
// as INFRASTRUCTURE_FAILURE (the guard could not be consulted), while a
// deterministic authority verdict (unregistered receipts digest) classifies
// as RECEIPT_MISSING. Both fail the freeze closed, but only the transient
// class is resumable as infrastructure-retry (SC-5.6).
func TestInitialFreezeClassifiesTransientAuthorityFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	transient := NewService(nil, nil)
	transientAuthority := newStubGuardAuthority()
	transient.SetGuardAuthority(transientAuthority)
	transientAuthority.setErr(errors.New("receipts store unavailable"))
	failedJob, _, err := transient.Trigger(ctx, triggerRequest("freeze-transient", ModeForce))
	if !errors.Is(err, ErrGuardAuthorityUnavailable) {
		t.Fatalf("transient authority failure = %v, want ErrGuardAuthorityUnavailable", err)
	}
	if failedJob.Stage != StageFailed {
		t.Fatalf("stage after transient authority failure = %s, want failed", failedJob.Stage)
	}
	for _, reason := range failedJob.FailureReasons {
		if reason == "RECEIPT_MISSING" {
			t.Fatal("transient authority failure was misclassified as RECEIPT_MISSING")
		}
	}

	deterministic := NewService(nil, nil)
	deterministicAuthority := newStubGuardAuthority()
	deterministic.SetGuardAuthority(deterministicAuthority)
	// No digest registration: the authority's verdict is deterministic
	// rejection, not unavailability.
	rejectedJob, _, err := deterministic.Trigger(ctx, triggerRequest("freeze-deterministic", ModeForce))
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("deterministic authority rejection = %v, want ErrGuardNotSatisfied", err)
	}
	if rejectedJob.Stage != StageFailed {
		t.Fatalf("stage after deterministic rejection = %s, want failed", rejectedJob.Stage)
	}
	sawMissing := false
	for _, reason := range rejectedJob.FailureReasons {
		if reason == "RECEIPT_MISSING" {
			sawMissing = true
		}
	}
	if !sawMissing {
		t.Fatalf("failure reasons = %v, want RECEIPT_MISSING", rejectedJob.FailureReasons)
	}
}

// P0-6: Rediagnose is only legal from partially_failed, must advance the
// policy revision, requires a governance principal, and must bind the new
// immutable diagnosis run reference.
func TestRediagnoseRejectsFrozenSourceSameRevisionAndMissingBindings(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()

	frozen := triggerForcedJob(t, service, ctx, "rediagnose-frozen-source")
	_, err := service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: frozen.TenantID, PrincipalID: "governance-1", CutID: frozen.CutID,
		ExpectedJobVersion: frozen.JobVersion, DiagnosisPolicyRevision: "diagnosis-v2", NewDiagnosisRunRef: "run-2",
	})
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("rediagnose from frozen = %v, want ErrIllegalTransition", err)
	}

	partiallyFailed := walkToPartiallyFailed(t, service, ctx, "rediagnose-rejections")
	_, err = service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: partiallyFailed.TenantID, PrincipalID: "governance-1", CutID: partiallyFailed.CutID,
		ExpectedJobVersion: partiallyFailed.JobVersion, DiagnosisPolicyRevision: partiallyFailed.DiagnosisPolicyRevision, NewDiagnosisRunRef: "run-2",
	})
	if err == nil {
		t.Fatal("rediagnose with the same policy revision was accepted")
	}
	_, err = service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: partiallyFailed.TenantID, CutID: partiallyFailed.CutID,
		ExpectedJobVersion: partiallyFailed.JobVersion, DiagnosisPolicyRevision: "diagnosis-v2", NewDiagnosisRunRef: "run-2",
	})
	if err == nil {
		t.Fatal("rediagnose without a governance principal was accepted")
	}
	_, err = service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: partiallyFailed.TenantID, PrincipalID: "governance-1", CutID: partiallyFailed.CutID,
		ExpectedJobVersion: partiallyFailed.JobVersion, DiagnosisPolicyRevision: "diagnosis-v2",
	})
	if err == nil {
		t.Fatal("rediagnose without the new diagnosis run ref was accepted")
	}

	rediagnosed, err := service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: partiallyFailed.TenantID, PrincipalID: "governance-1", CutID: partiallyFailed.CutID,
		ExpectedJobVersion: partiallyFailed.JobVersion, DiagnosisPolicyRevision: "diagnosis-v2", NewDiagnosisRunRef: "run-2",
	})
	requireCutImplemented(t, err)
	if rediagnosed.Stage != StageDiagnosing || rediagnosed.NewDiagnosisRunRef != "run-2" || rediagnosed.DiagnosisPolicyRevision != "diagnosis-v2" {
		t.Fatalf("rediagnose did not bind the new run/revision: %#v", rediagnosed)
	}
	bound := false
	for _, entry := range service.AuditTrail(rediagnosed.TenantID) {
		if entry.Kind == "rediagnose" && strings.Contains(entry.Detail, "run=run-2") {
			bound = true
		}
	}
	if !bound {
		t.Fatal("rediagnose audit detail does not carry the new diagnosis run ref")
	}
}

// SC-4.2 binding: after Rediagnose advances the active policy revision,
// evidence citing the superseded revision fails closed locally (before the
// authority is consulted); only evidence under the new revision is admitted.
func TestStalePolicyEvidenceRejectedAfterRediagnose(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-v1", "diagnosis-v1", "annotation-v2", "diagnosis-v2")
	service.SetGuardAuthority(authority)

	partiallyFailed := walkToPartiallyFailed(t, service, ctx, "stale-policy-evidence")
	rediagnosed, err := service.Rediagnose(ctx, RediagnoseRequest{
		TenantID: partiallyFailed.TenantID, PrincipalID: "governance-1", CutID: partiallyFailed.CutID,
		ExpectedJobVersion: partiallyFailed.JobVersion, DiagnosisPolicyRevision: "diagnosis-v2", NewDiagnosisRunRef: "run-2",
	})
	requireCutImplemented(t, err)
	_, err = service.Transition(ctx, TransitionRequest{
		TenantID: rediagnosed.TenantID, CutID: rediagnosed.CutID, ExpectedJobVersion: rediagnosed.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-v1", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("stale v1 evidence after rediagnose = %v, want ErrGuardNotSatisfied", err)
	}
	consolidating, err := service.Transition(ctx, TransitionRequest{
		TenantID: rediagnosed.TenantID, CutID: rediagnosed.CutID, ExpectedJobVersion: rediagnosed.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-v2", DiagnosisPolicyRevision: "diagnosis-v2"},
	})
	requireCutImplemented(t, err)
	if consolidating.LastTransitionEvidence.DiagnosisPolicyRevision != "diagnosis-v2" {
		t.Fatalf("v2 evidence was not stamped into the job: %#v", consolidating.LastTransitionEvidence)
	}
}

// blockingThenErrorFreezer blocks inside Freeze until released, then fails —
// the P0-7 shape: the phase-2 error path runs unlocked and must not race the
// reservation map against concurrent same-room triggers.
type blockingThenErrorFreezer struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingThenErrorFreezer) Freeze(context.Context, TriggerRequest) (Manifest, error) {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return Manifest{}, errors.New("freeze infrastructure failure")
}

func TestConcurrentTriggerPhase2ErrorReleasesReservationWithoutRace(t *testing.T) {
	t.Parallel()
	freezer := &blockingThenErrorFreezer{entered: make(chan struct{}), release: make(chan struct{})}
	service := NewService(nil, freezer)
	ctx := context.Background()

	result := make(chan error, 1)
	go func() {
		_, _, err := service.Trigger(ctx, triggerRequest("race-error-path", ModeForce))
		result <- err
	}()
	<-freezer.entered
	// While the freeze is in flight the room reservation rejects a competing
	// same-room trigger without minting a cut.
	competitor, duplicate, err := service.Trigger(ctx, triggerRequest("race-error-path-competitor", ModeForce))
	if err != nil || duplicate || competitor.CutID != "" {
		t.Fatalf("competitor during in-flight freeze = (%#v, %v, %v), want empty rejection", competitor, duplicate, err)
	}
	close(freezer.release)
	if err := <-result; err == nil {
		t.Fatal("blocked freezer did not surface its error")
	}
	// The error path released the reservation under the lock: the same room
	// can freeze again. The freezer now fails immediately (release is
	// closed), so a retry that reaches the freezer surfaces its error; a
	// leaked reservation would instead produce the empty rejection.
	_, _, err = service.Trigger(ctx, triggerRequest("race-error-path-retry", ModeForce))
	if err == nil || !strings.Contains(err.Error(), "freeze infrastructure failure") {
		t.Fatalf("post-error retry = %v, want the freezer error (reservation must be released)", err)
	}
}

// R3: an authority fact revoked between the Phase B resolution and the Phase C
// apply must fail the transition closed even though JobVersion never moved —
// the admission is bound to the re-confirmed fact snapshot, not to a stale
// boolean.
func TestGuardFactsRevokedBetweenPhasesFailClosed(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-real", "diagnosis-v1")
	// The first DiagnosisVerified call (Phase B) resolves verified; every
	// later call reports the annotation revoked.
	authority.revokeDiagnosisAfter = 1
	service.SetGuardAuthority(authority)

	diagnosing := walkToDiagnosing(t, service, ctx, "guard-revoke-between-phases")
	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("transition admitted on revoked authority facts = %v, want ErrGuardNotSatisfied", err)
	}
	after, err := service.Get(ctx, diagnosing.TenantID, diagnosing.CutID)
	if err != nil {
		t.Fatalf("job lookup: %v", err)
	}
	if after.Stage != StageDiagnosing || after.JobVersion != diagnosing.JobVersion {
		t.Fatalf("rejected transition mutated the job: stage=%s version=%d", after.Stage, after.JobVersion)
	}
	if after.LastTransitionEvidence.AuthorityFactsDigest != "" {
		t.Fatalf("rejected transition stamped an authority facts digest: %q", after.LastTransitionEvidence.AuthorityFactsDigest)
	}
	found := false
	for _, entry := range service.AuditTrail(diagnosing.TenantID) {
		if entry.Kind == "transition_guard_rejected" && strings.Contains(entry.Detail, "phase C re-resolve") {
			found = true
		}
	}
	if !found {
		t.Fatal("phase C revocation rejection was not audited")
	}
}

// R3: an authority that still answers "satisfied" but over drifted facts (the
// snapshot behind the verdict changed) must fail the apply: the Phase B
// confirmation digest and the Phase C re-resolve must match exactly.
func TestGuardFactsDriftBetweenPhasesFailClosed(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-real", "diagnosis-v1")
	// The second DiagnosisVerified call (Phase C) resolves the same verdict
	// over drifted facts (a new revision_seen fact).
	authority.driftDiagnosisAfter = 1
	service.SetGuardAuthority(authority)

	diagnosing := walkToDiagnosing(t, service, ctx, "guard-drift-between-phases")
	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) || !strings.Contains(err.Error(), "changed between resolution and application") {
		t.Fatalf("transition admitted on drifted authority facts = %v, want digest-drift ErrGuardNotSatisfied", err)
	}
	after, err := service.Get(ctx, diagnosing.TenantID, diagnosing.CutID)
	if err != nil {
		t.Fatalf("job lookup: %v", err)
	}
	if after.Stage != StageDiagnosing || after.JobVersion != diagnosing.JobVersion {
		t.Fatalf("drift-rejected transition mutated the job: stage=%s version=%d", after.Stage, after.JobVersion)
	}
	// Sanity: without drift hooks the same transition admits and stamps the
	// confirmed digest, never a caller-declared one.
	stable := newStubGuardAuthority()
	stable.register("annotation-real", "diagnosis-v1")
	service.SetGuardAuthority(stable)
	consolidating, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: StageConsolidating,
		Evidence: TransitionEvidence{
			DiagnosisAnnotationRef:  "annotation-real",
			DiagnosisPolicyRevision: "diagnosis-v1",
			AuthorityFactsDigest:    "sha256:caller-declared-junk",
		},
	})
	requireCutImplemented(t, err)
	if consolidating.LastTransitionEvidence.AuthorityFactsDigest == "" ||
		consolidating.LastTransitionEvidence.AuthorityFactsDigest == "sha256:caller-declared-junk" {
		t.Fatalf("admitted transition did not stamp the service-resolved authority digest: %q", consolidating.LastTransitionEvidence.AuthorityFactsDigest)
	}
}

// reentrantGuardAuthority resolves every guard by first reading the cut
// manifest from the very service whose transition is in flight — the callback
// shape the integration tracer's authority uses. The service never consults
// an authority while holding its lock, so this must not self-deadlock.
type reentrantGuardAuthority struct {
	*stubGuardAuthority
	service *Service
}

func (g *reentrantGuardAuthority) DiagnosisVerified(ctx context.Context, tenantID domain.TenantID, cutID CutID, annotationRef, policyRevision string) (bool, GuardConfirmation, error) {
	if _, err := g.service.GetManifest(ctx, tenantID, cutID); err != nil {
		return false, GuardConfirmation{}, err
	}
	return g.stubGuardAuthority.DiagnosisVerified(ctx, tenantID, cutID, annotationRef, policyRevision)
}

// R3 round 3: a reentrant authority adapter — one whose guard resolution
// calls back into the cut service (GetManifest takes the same mutex the
// transition applies under) — must not deadlock the guarded transition. The
// test fails on a watchdog instead of hanging the suite.
func TestReentrantAuthorityAdapterDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := &reentrantGuardAuthority{stubGuardAuthority: newStubGuardAuthority(), service: service}
	authority.register("annotation-real", "diagnosis-v1")
	service.SetGuardAuthority(authority)

	diagnosing := walkToDiagnosing(t, service, ctx, "guard-reentrant-authority")
	type transitionOutcome struct {
		job Job
		err error
	}
	done := make(chan transitionOutcome, 1)
	go func() {
		job, err := service.Transition(ctx, TransitionRequest{
			TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
			To:       StageConsolidating,
			Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
		})
		done <- transitionOutcome{job: job, err: err}
	}()
	select {
	case result := <-done:
		requireCutImplemented(t, result.err)
		if result.job.Stage != StageConsolidating {
			t.Fatalf("reentrant-authority transition stage = %s, want consolidating", result.job.Stage)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("guarded transition deadlocked: the authority adapter called back into the cut service while the service held its lock")
	}
}

// lyingDigestGuardAuthority answers with a plausible-looking digest that does
// not canonically derive from its own fact set.
type lyingDigestGuardAuthority struct {
	*stubGuardAuthority
}

func (g *lyingDigestGuardAuthority) DiagnosisVerified(ctx context.Context, tenantID domain.TenantID, cutID CutID, annotationRef, policyRevision string) (bool, GuardConfirmation, error) {
	ok, confirmation, err := g.stubGuardAuthority.DiagnosisVerified(ctx, tenantID, cutID, annotationRef, policyRevision)
	if err != nil {
		return ok, confirmation, err
	}
	confirmation.FactsDigest = "sha256:" + strings.Repeat("0", 64)
	return ok, confirmation, nil
}

// R3 round 3: the confirmation digest must actually cover the reported facts.
// An authority that answers "satisfied" with a digest the service cannot
// recompute over the reported facts (a constant digest over drifting facts)
// fails closed at every resolution — the digest is derived server-side, never
// trusted from the authority's word.
func TestAuthorityDigestMustCoverItsFacts(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := &lyingDigestGuardAuthority{stubGuardAuthority: newStubGuardAuthority()}
	authority.register("annotation-real", "diagnosis-v1")
	service.SetGuardAuthority(authority)

	diagnosing := walkToDiagnosing(t, service, ctx, "guard-lying-digest")
	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) || !strings.Contains(err.Error(), "does not cover its facts") {
		t.Fatalf("transition on a digest that does not cover its facts = %v, want ErrGuardNotSatisfied", err)
	}
	after, err := service.Get(ctx, diagnosing.TenantID, diagnosing.CutID)
	if err != nil {
		t.Fatalf("job lookup: %v", err)
	}
	if after.Stage != StageDiagnosing || after.JobVersion != diagnosing.JobVersion {
		t.Fatalf("rejected transition mutated the job: stage=%s version=%d", after.Stage, after.JobVersion)
	}
}

// R3 round 4: a fact revoked AFTER the Phase B resolution (before the
// reservation) must fail the transition closed at the apply-time validation —
// the deterministic interleave the C-pre→apply TOCTOU review asked for. The
// job must be untouched.
func TestGuardFactsRevokedAfterResolveFailClosedAtApply(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-real", "diagnosis-v1")
	service.SetGuardAuthority(authority)
	diagnosing := walkToDiagnosing(t, service, ctx, "guard-revoke-after-resolve")
	service.transitionHookAfterResolve = func() {
		// Fires after Phase B resolved verified, before the reservation and
		// the apply-time validation: governance withdraws the annotation.
		authority.withdraw("annotation-real")
	}
	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("transition admitted a fact revoked after resolution = %v, want ErrGuardNotSatisfied", err)
	}
	after, err := service.Get(ctx, diagnosing.TenantID, diagnosing.CutID)
	if err != nil {
		t.Fatalf("job lookup: %v", err)
	}
	if after.Stage != StageDiagnosing || after.JobVersion != diagnosing.JobVersion {
		t.Fatalf("rejected transition mutated the job: stage=%s version=%d", after.Stage, after.JobVersion)
	}
	found := false
	for _, entry := range service.AuditTrail(diagnosing.TenantID) {
		if entry.Kind == "transition_guard_rejected" && strings.Contains(entry.Detail, "phase C re-resolve") {
			found = true
		}
	}
	if !found {
		t.Fatal("apply-time revocation rejection was not audited")
	}
}

// R3 round 4: the fact reservation serializes a concurrent revocation BEHIND
// the apply. A withdrawal racing the reserved apply window blocks until the
// transition has committed — the admission serialized before the revocation,
// which is the only sound order — and the withdrawal then takes effect.
func TestGuardReservationSerializesRevocationBehindApply(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-real", "diagnosis-v1")
	service.SetGuardAuthority(authority)
	diagnosing := walkToDiagnosing(t, service, ctx, "guard-reservation-serializes")

	withdrawn := make(chan struct{})
	service.transitionHookUnderReservation = func() {
		// Fires after the apply-time validation, while the reservation is
		// held: the concurrent withdrawal must block until the apply ends.
		go func() {
			authority.withdraw("annotation-real")
			close(withdrawn)
		}()
		select {
		case <-withdrawn:
			t.Error("revocation serialized AHEAD of the reserved apply: the reservation did not pin the facts")
		case <-time.After(50 * time.Millisecond):
		}
	}
	consolidating, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	requireCutImplemented(t, err)
	if consolidating.Stage != StageConsolidating {
		t.Fatalf("reserved apply stage = %s, want consolidating", consolidating.Stage)
	}
	// After the transition (and the deferred release) the withdrawal
	// completes and the fact is gone: the NEXT admission on the same
	// evidence now fails closed. (Fresh service: the room-serial freeze
	// still holds room-1 for the non-terminal consolidating job.)
	select {
	case <-withdrawn:
	case <-time.After(5 * time.Second):
		t.Fatal("withdrawal did not complete after the reservation was released")
	}
	service2 := NewService(nil, nil)
	service2.SetGuardAuthority(authority)
	diagnosing2 := walkToDiagnosing(t, service2, ctx, "guard-reservation-serializes-2")
	_, err = service2.Transition(ctx, TransitionRequest{
		TenantID: diagnosing2.TenantID, CutID: diagnosing2.CutID, ExpectedJobVersion: diagnosing2.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("post-withdrawal transition on the revoked fact = %v, want ErrGuardNotSatisfied", err)
	}
}

// refusingReserveAuthority fails every reservation acquisition and delegates
// everything else to the wrapped authority.
type refusingReserveAuthority struct {
	GuardAuthority
}

func (a refusingReserveAuthority) ReserveFacts(context.Context, domain.TenantID, CutID) error {
	return errors.New("authority reservation backend unavailable")
}

// R3 round 5 note: a failed reservation acquisition must fail the
// transition closed — audited as a phase C re-resolve rejection with the job
// untouched — never an apply on unreserved facts.
func TestGuardReservationAcquisitionFailsClosed(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil)
	ctx := context.Background()
	authority := newStubGuardAuthority()
	authority.register("annotation-real", "diagnosis-v1")
	service.SetGuardAuthority(authority)
	diagnosing := walkToDiagnosing(t, service, ctx, "guard-reserve-refused")
	service.SetGuardAuthority(refusingReserveAuthority{authority})

	_, err := service.Transition(ctx, TransitionRequest{
		TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To:       StageConsolidating,
		Evidence: TransitionEvidence{DiagnosisAnnotationRef: "annotation-real", DiagnosisPolicyRevision: "diagnosis-v1"},
	})
	if !errors.Is(err, ErrGuardAuthorityUnavailable) {
		t.Fatalf("transition applied without a fact reservation = %v, want ErrGuardAuthorityUnavailable", err)
	}
	if errors.Is(err, ErrGuardNotSatisfied) {
		t.Fatalf("reservation failure misclassified as deterministic rejection: %v", err)
	}
	if !strings.Contains(err.Error(), "authority fact reservation") {
		t.Fatalf("reservation failure lost its root cause: %v", err)
	}
	after, err := service.Get(ctx, diagnosing.TenantID, diagnosing.CutID)
	if err != nil {
		t.Fatalf("job lookup: %v", err)
	}
	if after.Stage != StageDiagnosing || after.JobVersion != diagnosing.JobVersion {
		t.Fatalf("rejected transition mutated the job: stage=%s version=%d", after.Stage, after.JobVersion)
	}
	found := false
	for _, entry := range service.AuditTrail(diagnosing.TenantID) {
		if entry.Kind == "transition_guard_rejected" &&
			strings.Contains(entry.Detail, "phase C re-resolve") &&
			strings.Contains(entry.Detail, "reservation backend unavailable") {
			found = true
		}
	}
	if !found {
		t.Fatal("reservation failure rejection was not audited")
	}
}
