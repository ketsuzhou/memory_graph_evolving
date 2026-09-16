package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"river2.dev/graph-memory-service/internal/domain"
)

// testDigest renders a well-shaped sha256 digest for evidence fields.
func testDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// activateEvaluation drives the full P0-4 evidence chain for one proposal:
// prepared isolated replay session → immutable pass verdict → evaluation
// activation record. It returns the activation Ref, the only EvaluationRef
// a production Promote admits.
func activateEvaluation(t *testing.T, svc *Service, ctx context.Context, proposal Proposal, base, candidate, sessionSuffix string) string {
	t.Helper()
	session, err := svc.PrepareReplay(ctx, ReplayRequest{
		Proposal: proposal, SourceRoomID: "room-source", CloneRoomID: "room-replay-" + sessionSuffix,
		AncestorRoomID: "room-source", MutationBranch: "replay_skill_mutation", SandboxOnlyEffects: true,
		MatchedBaselineRef: "skill-baseline@7", CandidateRef: candidate, Budget: ReplayBudget{MaxCostUnits: 100},
	})
	if err != nil {
		t.Fatalf("prepare replay (%s): %v", sessionSuffix, err)
	}
	result, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: proposal.ID, Outcome: ReplayPass,
		EvaluationPolicyDigest: testDigest("evaluation-policy-v1"), PassThreshold: "pass_rate>=0.95",
		EvidenceDigest: testDigest("replay-evidence-" + sessionSuffix),
	})
	if err != nil {
		t.Fatalf("record replay result (%s): %v", sessionSuffix, err)
	}
	activation, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: result.ID, ProposalID: proposal.ID,
		EvaluationTarget:  proposal.Target,
		CandidateRef:      candidate,
		ExactBaseRevision: base,
	})
	if err != nil {
		t.Fatalf("activate evaluation (%s): %v", sessionSuffix, err)
	}
	return activation.Ref
}

func TestDiagnosisAgentOwnerVisibilityAndTargetRemainIndependent(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	proposal, err := svc.RegisterProposal(context.Background(), Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"),
		Visibility: VisibilityPrivate,
		Target:     Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	})
	if errors.Is(err, ErrPromotionNotImplemented) {
		t.Fatal("RED: stable Diagnosis Agent ownership and independent visibility/target are not implemented")
	}
	if err != nil {
		t.Fatalf("register proposal: %v", err)
	}
	if proposal.Owner != domain.PrincipalID("diagnosis-agent-stable") || proposal.Visibility != VisibilityPrivate || proposal.Target.Namespace != NamespaceEvaluation {
		t.Fatalf("owner, visibility, and target were coupled or changed: %#v", proposal)
	}
	// Registration is write-once: an identical replay is idempotent, and a
	// same-ID registration changing owner/visibility/target is rejected.
	replay, err := svc.RegisterProposal(ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	})
	if err != nil || replay.Owner != proposal.Owner {
		t.Fatalf("identical proposal replay was not idempotent: %#v err=%v", replay, err)
	}
	if _, err := svc.RegisterProposal(ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("attacker-agent"), Visibility: VisibilityShared,
		Target: Target{Namespace: NamespaceProduction, TenantID: domain.TenantID("tenant-b"), Name: "hijacked-skill"},
	}); !errors.Is(err, ErrProposalIdentityConflict) {
		t.Fatalf("proposal overwrite with a different identity was accepted: %v", err)
	}
	// The stored registration is untouched by the rejected overwrite.
	after, err := svc.RegisterProposal(ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	})
	if err != nil || after.Visibility != VisibilityPrivate || after.Owner != domain.PrincipalID("diagnosis-agent-stable") {
		t.Fatalf("rejected overwrite mutated the registered proposal: %#v err=%v", after, err)
	}
}

func TestReplayUsesIsolatedCloneMutationAncestrySandboxAndBudget(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	registered, err := svc.RegisterProposal(ctx, Proposal{ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable")})
	if err != nil {
		t.Fatalf("register proposal: %v", err)
	}
	_, err = svc.PrepareReplay(ctx, ReplayRequest{
		Proposal: *registered, SourceRoomID: "room-source", CloneRoomID: "room-replay", AncestorRoomID: "room-source",
		MutationBranch: "replay_skill_mutation", SandboxOnlyEffects: true, MatchedBaselineRef: "skill-baseline@7", CandidateRef: "candidate@1",
		Budget: ReplayBudget{MaxCostUnits: 100},
	})
	if errors.Is(err, ErrPromotionNotImplemented) {
		t.Fatal("RED: isolated Room clone, replay_skill_mutation ancestry, sandbox-only effects, matched baseline, and execution budget are not implemented")
	}
	if err != nil {
		t.Fatalf("prepare replay: %v", err)
	}
}

func TestProductionPromotionRequiresExactBaseAndPromotionRecord(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	if _, err := svc.RegisterProposal(ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	}); err != nil {
		t.Fatalf("register proposal: %v", err)
	}
	target := Target{Namespace: NamespaceProduction, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"}
	evaluationRef := activateEvaluation(t, svc, ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	}, "skill@42", "candidate@1", "prod")
	// A promotion must name a new revision distinct from the base.
	if _, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: evaluationRef, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@42",
		ProductionTarget: target,
	}); !errors.Is(err, ErrPromotionIncomplete) {
		t.Fatalf("promotion without a distinct promoted revision was accepted: %v", err)
	}
	// P0-4: a free-form/forged evaluation ref that resolves to no activation
	// record is rejected.
	if _, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: "evaluation/replay-forged", CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@43",
		ProductionTarget: target,
	}); !errors.Is(err, ErrEvaluationActivationUnknown) {
		t.Fatalf("forged evaluation ref was not rejected: %v", err)
	}
	// A candidate binding that differs from the activated one is rejected.
	if _, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: evaluationRef, CandidateRef: "candidate@forged",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@43",
		ProductionTarget: target,
	}); !errors.Is(err, ErrEvaluationBindingMismatch) {
		t.Fatalf("mismatched candidate binding was not rejected: %v", err)
	}
	record, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: evaluationRef, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@43",
		ProductionTarget: target,
	})
	if errors.Is(err, ErrPromotionNotImplemented) {
		t.Fatal("RED: evaluation-to-production promotion record and exact base-revision CAS gate are not implemented")
	}
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if record.ExactBaseRevision != "skill@42" || record.PromotedRevision != "skill@43" || record.RollbackRevision != "skill@42" || record.ProductionTarget.Namespace != NamespaceProduction {
		t.Fatalf("promotion record omitted exact base/promoted/rollback revisions or production target: %#v", record)
	}
	// The base CAS is spent once the promotion lands: replaying the same
	// evaluated base is now stale against the promoted active revision.
	if _, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: evaluationRef, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@44",
		ProductionTarget: target,
	}); !errors.Is(err, ErrStaleBaseRevision) {
		t.Fatalf("replay of the consumed base revision was not rejected as stale: %v", err)
	}
}

func TestPromotionRecordSupportsCASProtectedRollback(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	if _, err := svc.RegisterProposal(ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	}); err != nil {
		t.Fatalf("register proposal: %v", err)
	}
	target := Target{Namespace: NamespaceProduction, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"}
	evaluationRef := activateEvaluation(t, svc, ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	}, "skill@42", "candidate@1", "rollback-1")
	record, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: evaluationRef, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@43",
		ProductionTarget: target,
	})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	// Rollback is CAS-protected on the promoted revision: neither an
	// unrelated revision nor the pre-promotion base may roll it back, and
	// unknown records do not roll back.
	if _, err := svc.Rollback(ctx, RollbackRequest{PromotionRecordID: record.ID, ExpectedRevision: "skill@99"}); !errors.Is(err, ErrStaleBaseRevision) {
		t.Fatalf("stale rollback revision was not rejected: err=%v", err)
	}
	if _, err := svc.Rollback(ctx, RollbackRequest{PromotionRecordID: record.ID, ExpectedRevision: "skill@42"}); !errors.Is(err, ErrStaleBaseRevision) {
		t.Fatalf("base revision was not rejected as rollback CAS: err=%v", err)
	}
	if _, err := svc.Rollback(ctx, RollbackRequest{PromotionRecordID: "promotion-unknown", ExpectedRevision: "skill@43"}); !errors.Is(err, ErrPromotionRecordUnknown) {
		t.Fatalf("unknown promotion record rollback was not rejected: err=%v", err)
	}
	rolledBack, err := svc.Rollback(ctx, RollbackRequest{PromotionRecordID: record.ID, ExpectedRevision: "skill@43"})
	if errors.Is(err, ErrPromotionNotImplemented) {
		t.Fatal("RED: promotion-record rollback protected by an exact expected revision is not implemented")
	}
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rolledBack.RollbackRevision != "skill@42" {
		t.Fatalf("rollback did not restore the recorded revision: %#v", rolledBack)
	}
	// The CAS is consumed: rolling the same record back again fails because
	// the active revision is no longer the promoted revision.
	if _, err := svc.Rollback(ctx, RollbackRequest{PromotionRecordID: record.ID, ExpectedRevision: "skill@43"}); !errors.Is(err, ErrStaleBaseRevision) {
		t.Fatalf("double rollback of the same record was not rejected: %v", err)
	}
	// A newer activation supersedes the old record permanently: after
	// skill@42 → skill@44, the stale record (promoted skill@43) must not
	// overwrite the current activation.
	newerRef := activateEvaluation(t, svc, ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	}, "skill@42", "candidate@2", "rollback-2")
	newer, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: newerRef, CandidateRef: "candidate@2",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@44",
		ProductionTarget: target,
	})
	if err != nil {
		t.Fatalf("second promote: %v", err)
	}
	if _, err := svc.Rollback(ctx, RollbackRequest{PromotionRecordID: record.ID, ExpectedRevision: "skill@43"}); !errors.Is(err, ErrStaleBaseRevision) {
		t.Fatalf("superseded record was allowed to roll back a newer activation: %v", err)
	}
	if _, err := svc.Rollback(ctx, RollbackRequest{PromotionRecordID: newer.ID, ExpectedRevision: "skill@44"}); err != nil {
		t.Fatalf("rollback of the current activation failed: %v", err)
	}
}

// P0-4: replay verdicts are immutable records with a closed outcome
// vocabulary; only a pass can activate evaluation scope, and an activation
// never leaks into production without an exact-binding Promote.
func TestReplayResultAndActivationEvidenceChain(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	registered, err := svc.RegisterProposal(ctx, Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	})
	if err != nil {
		t.Fatalf("register proposal: %v", err)
	}
	session, err := svc.PrepareReplay(ctx, ReplayRequest{
		Proposal: *registered, SourceRoomID: "room-source", CloneRoomID: "room-replay-chain",
		AncestorRoomID: "room-source", MutationBranch: "replay_skill_mutation", SandboxOnlyEffects: true,
		MatchedBaselineRef: "skill-baseline@7", CandidateRef: "candidate@1", Budget: ReplayBudget{MaxCostUnits: 100},
	})
	if err != nil {
		t.Fatalf("prepare replay: %v", err)
	}
	// Unknown session, closed-vocabulary violation, and malformed digests
	// are all rejected.
	if _, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: "replay-unknown", ProposalID: "proposal-1", Outcome: ReplayPass,
		EvaluationPolicyDigest: testDigest("p"), PassThreshold: "pass_rate>=0.95", EvidenceDigest: testDigest("e"),
	}); !errors.Is(err, ErrReplaySessionUnknown) {
		t.Fatalf("unknown session verdict was accepted: %v", err)
	}
	if _, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: "proposal-1", Outcome: ReplayOutcome("passed-ish"),
		EvaluationPolicyDigest: testDigest("p"), PassThreshold: "pass_rate>=0.95", EvidenceDigest: testDigest("e"),
	}); !errors.Is(err, ErrReplayResultIncomplete) {
		t.Fatalf("outcome outside the closed vocabulary was accepted: %v", err)
	}
	if _, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: "proposal-1", Outcome: ReplayPass,
		EvaluationPolicyDigest: "policy-v1", PassThreshold: "pass_rate>=0.95", EvidenceDigest: testDigest("e"),
	}); !errors.Is(err, ErrReplayResultIncomplete) {
		t.Fatalf("malformed policy digest was accepted: %v", err)
	}
	// A fail verdict is immutable and cannot activate evaluation scope.
	failed, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: "proposal-1", Outcome: ReplayFail,
		EvaluationPolicyDigest: testDigest("evaluation-policy-v1"), PassThreshold: "pass_rate>=0.95",
		EvidenceDigest: testDigest("replay-evidence-fail"),
	})
	if err != nil {
		t.Fatalf("record fail verdict: %v", err)
	}
	if _, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: failed.ID, ProposalID: "proposal-1",
		EvaluationTarget: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
		CandidateRef:     "candidate@1", ExactBaseRevision: "skill@42",
	}); !errors.Is(err, ErrReplayNotPassed) {
		t.Fatalf("non-pass verdict activated evaluation scope: %v", err)
	}
	// A disagreeing second verdict for the same session/proposal conflicts
	// rather than superseding the immutable record.
	if _, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: "proposal-1", Outcome: ReplayPass,
		EvaluationPolicyDigest: testDigest("evaluation-policy-v1"), PassThreshold: "pass_rate>=0.95",
		EvidenceDigest: testDigest("replay-evidence-pass"),
	}); !errors.Is(err, ErrReplayResultConflict) {
		t.Fatalf("conflicting second verdict for the same session was accepted: %v", err)
	}
	// Re-recording identical facts is idempotent.
	replay, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: "proposal-1", Outcome: ReplayFail,
		EvaluationPolicyDigest: testDigest("evaluation-policy-v1"), PassThreshold: "pass_rate>=0.95",
		EvidenceDigest: testDigest("replay-evidence-fail"),
	})
	if err != nil || replay.ID != failed.ID {
		t.Fatalf("identical verdict replay was not idempotent: %#v err=%v", replay, err)
	}
	// A separate session passes and activates; a production target on the
	// activation call is rejected (evaluation scope only).
	session2, err := svc.PrepareReplay(ctx, ReplayRequest{
		Proposal: *registered, SourceRoomID: "room-source", CloneRoomID: "room-replay-chain-2",
		AncestorRoomID: "room-source", MutationBranch: "replay_skill_mutation", SandboxOnlyEffects: true,
		MatchedBaselineRef: "skill-baseline@7", CandidateRef: "candidate@1", Budget: ReplayBudget{MaxCostUnits: 100},
	})
	if err != nil {
		t.Fatalf("prepare second replay: %v", err)
	}
	passed, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session2.ID, ProposalID: "proposal-1", Outcome: ReplayPass,
		EvaluationPolicyDigest: testDigest("evaluation-policy-v1"), PassThreshold: "pass_rate>=0.95",
		EvidenceDigest: testDigest("replay-evidence-pass-2"),
	})
	if err != nil {
		t.Fatalf("record pass verdict: %v", err)
	}
	if _, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: passed.ID, ProposalID: "proposal-1",
		EvaluationTarget: Target{Namespace: NamespaceProduction, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
		CandidateRef:     "candidate@1", ExactBaseRevision: "skill@42",
	}); err == nil {
		t.Fatal("evaluation activation with a production target was accepted")
	}
	activation, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: passed.ID, ProposalID: "proposal-1",
		EvaluationTarget: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
		CandidateRef:     "candidate@1", ExactBaseRevision: "skill@42",
	})
	if err != nil {
		t.Fatalf("activate evaluation: %v", err)
	}
	// Activation is idempotent and its ref is stable.
	again, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: passed.ID, ProposalID: "proposal-1",
		EvaluationTarget: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
		CandidateRef:     "candidate@1", ExactBaseRevision: "skill@42",
	})
	if err != nil || again.Ref != activation.Ref {
		t.Fatalf("activation was not idempotent: %#v err=%v", again, err)
	}
	// Promote admits exactly this activation binding: a wrong base revision
	// or a wrong production target name is a binding mismatch.
	if _, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: activation.Ref, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@41", PromotedRevision: "skill@43",
		ProductionTarget: Target{Namespace: NamespaceProduction, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	}); !errors.Is(err, ErrEvaluationBindingMismatch) {
		t.Fatalf("wrong base binding was not rejected: %v", err)
	}
	if _, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: activation.Ref, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@43",
		ProductionTarget: Target{Namespace: NamespaceProduction, TenantID: domain.TenantID("tenant-a"), Name: "other-skill"},
	}); !errors.Is(err, ErrEvaluationBindingMismatch) {
		t.Fatalf("wrong production target was not rejected: %v", err)
	}
	record, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: "proposal-1", EvaluationRef: activation.Ref, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@43",
		ProductionTarget: Target{Namespace: NamespaceProduction, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	})
	if err != nil {
		t.Fatalf("promote with activation evidence: %v", err)
	}
	if record.CandidateRef != "candidate@1" || record.EvaluationRef != activation.Ref {
		t.Fatalf("promotion record did not retain the activation/candidate binding: %#v", record)
	}
}

// (R2) a passed session's activation is bound to the immutable snapshot:
// a verdict recorded against a foreign proposal, a candidate or target
// outside the snapshot at activation time, a divergent ancestor rebinding
// the session identity, and a base revision the production target has
// already moved past are all rejected.
func TestActivateEvaluationBindsImmutableSessionSnapshot(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	proposal := Proposal{
		ID: "proposal-1", Owner: domain.PrincipalID("diagnosis-agent-stable"), Visibility: VisibilityPrivate,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: domain.TenantID("tenant-a"), Name: "evaluation-skill"},
	}
	if _, err := svc.RegisterProposal(ctx, proposal); err != nil {
		t.Fatalf("register proposal: %v", err)
	}
	if _, err := svc.RegisterProposal(ctx, Proposal{
		ID: "proposal-2", Owner: proposal.Owner, Visibility: proposal.Visibility,
		Target: Target{Namespace: NamespaceEvaluation, TenantID: proposal.Target.TenantID, Name: "other-skill"},
	}); err != nil {
		t.Fatalf("register second proposal: %v", err)
	}
	request := ReplayRequest{
		Proposal: proposal, SourceRoomID: "room-source", CloneRoomID: "room-replay",
		AncestorRoomID: "room-source", MutationBranch: "replay_skill_mutation", SandboxOnlyEffects: true,
		MatchedBaselineRef: "skill-baseline@7", CandidateRef: "candidate@1", Budget: ReplayBudget{MaxCostUnits: 100},
	}
	session, err := svc.PrepareReplay(ctx, request)
	if err != nil {
		t.Fatalf("prepare replay: %v", err)
	}
	replayed, err := svc.PrepareReplay(ctx, request)
	if err != nil || replayed.ID != session.ID {
		t.Fatalf("identical replay request did not replay the session: %#v err=%v", replayed, err)
	}
	diverged := request
	diverged.AncestorRoomID = "room-elsewhere"
	rebound, err := svc.PrepareReplay(ctx, diverged)
	if err != nil || rebound.ID == session.ID {
		t.Fatalf("divergent ancestor reused the session identity: %#v err=%v", rebound, err)
	}
	if _, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: "proposal-2", Outcome: ReplayPass,
		EvaluationPolicyDigest: testDigest("p"), PassThreshold: "pass_rate>=0.95", EvidenceDigest: testDigest("e"),
	}); !errors.Is(err, ErrReplaySessionMismatch) {
		t.Fatalf("verdict for a foreign proposal was accepted on this session: %v", err)
	}
	result, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: session.ID, ProposalID: proposal.ID, Outcome: ReplayPass,
		EvaluationPolicyDigest: testDigest("p"), PassThreshold: "pass_rate>=0.95", EvidenceDigest: testDigest("e"),
	})
	if err != nil {
		t.Fatalf("record pass verdict: %v", err)
	}
	if _, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: result.ID, ProposalID: proposal.ID,
		EvaluationTarget: proposal.Target, CandidateRef: "candidate@9", ExactBaseRevision: "skill@42",
	}); !errors.Is(err, ErrEvaluationBindingMismatch) {
		t.Fatalf("activation with a candidate outside the session snapshot was accepted: %v", err)
	}
	if _, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: result.ID, ProposalID: proposal.ID,
		EvaluationTarget: Target{Namespace: NamespaceEvaluation, TenantID: proposal.Target.TenantID, Name: "other-skill"},
		CandidateRef:     "candidate@1", ExactBaseRevision: "skill@42",
	}); !errors.Is(err, ErrEvaluationBindingMismatch) {
		t.Fatalf("activation with a target outside the registered proposal was accepted: %v", err)
	}
	activation, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: result.ID, ProposalID: proposal.ID,
		EvaluationTarget: proposal.Target, CandidateRef: "candidate@1", ExactBaseRevision: "skill@42",
	})
	if err != nil {
		t.Fatalf("activate with the snapshot bindings: %v", err)
	}
	if _, err := svc.Promote(ctx, PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: activation.Ref, CandidateRef: "candidate@1",
		ExactBaseRevision: "skill@42", PromotedRevision: "skill@43",
		ProductionTarget: Target{Namespace: NamespaceProduction, TenantID: proposal.Target.TenantID, Name: proposal.Target.Name},
	}); err != nil {
		t.Fatalf("promote: %v", err)
	}
	// After the promotion the production projection's active revision is
	// skill@43: a fresh pass session can only activate against that base.
	refresh, err := svc.PrepareReplay(ctx, ReplayRequest{
		Proposal: proposal, SourceRoomID: "room-source", CloneRoomID: "room-replay-refresh",
		AncestorRoomID: "room-source", MutationBranch: "replay_skill_mutation", SandboxOnlyEffects: true,
		MatchedBaselineRef: "skill-baseline@8", CandidateRef: "candidate@2", Budget: ReplayBudget{MaxCostUnits: 100},
	})
	if err != nil {
		t.Fatalf("prepare refresh replay: %v", err)
	}
	refreshed, err := svc.RecordReplayResult(ctx, ReplayResultRequest{
		SessionID: refresh.ID, ProposalID: proposal.ID, Outcome: ReplayPass,
		EvaluationPolicyDigest: testDigest("p"), PassThreshold: "pass_rate>=0.95", EvidenceDigest: testDigest("e2"),
	})
	if err != nil {
		t.Fatalf("record refresh verdict: %v", err)
	}
	if _, err := svc.ActivateEvaluation(ctx, EvaluationActivationRequest{
		ReplayResultRef: refreshed.ID, ProposalID: proposal.ID,
		EvaluationTarget: proposal.Target, CandidateRef: "candidate@2", ExactBaseRevision: "skill@42",
	}); !errors.Is(err, ErrStaleBaseRevision) {
		t.Fatalf("activation against a base the target already moved past was accepted: %v", err)
	}
}
