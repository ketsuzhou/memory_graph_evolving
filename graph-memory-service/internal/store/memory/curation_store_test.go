package memory

import (
	"context"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

var storeNow = time.Date(2026, time.September, 8, 15, 0, 0, 0, time.UTC)

func TestCausalChainsRejectGapsAndForks(t *testing.T) {
	ctx := context.Background()
	store := New()
	first := domain.CausalTrialEvent{
		ID: "trial-1", TenantID: "tenant-1", SpaceID: "space-1",
		ExperimentID: "exp-1", Arm: domain.CausalArmWithIntervention, Ordinal: 1,
		PayloadHash: "hash-1", CreatedAt: storeNow,
	}
	if _, duplicate, err := store.AppendTrial(ctx, first); err != nil || duplicate {
		t.Fatalf("append first trial = (duplicate=%v, %v), want fresh append", duplicate, err)
	}
	if _, duplicate, err := store.AppendTrial(ctx, first); err != nil || !duplicate {
		t.Fatalf("replay first trial = (duplicate=%v, %v), want idempotent duplicate", duplicate, err)
	}
	fork := first
	fork.Success = true
	if _, _, err := store.AppendTrial(ctx, fork); err == nil {
		t.Fatal("same event id with different content must fork-conflict")
	}
	orphan := domain.CausalTrialEvent{
		ID: "trial-2", TenantID: "tenant-1", SpaceID: "space-1",
		PreviousHash: "wrong-head", PayloadHash: "hash-2", CreatedAt: storeNow,
	}
	if _, _, err := store.AppendTrial(ctx, orphan); err == nil {
		t.Fatal("trial not extending the chain head must be rejected")
	}

	estimate := domain.CausalEstimateRevision{
		TenantID: "tenant-1", SpaceID: "space-1", EstimateID: "estimate-1", Revision: 1,
		PayloadHash: "estimate-hash-1", CreatedAt: storeNow,
	}
	if _, _, err := store.AppendEstimateRevision(ctx, estimate); err != nil {
		t.Fatalf("append first estimate revision: %v", err)
	}
	skip := estimate
	skip.Revision = 3
	skip.PayloadHash = "estimate-hash-3"
	if _, _, err := store.AppendEstimateRevision(ctx, skip); err == nil {
		t.Fatal("estimate revision skipping 2 must be rejected")
	}
	if _, err := store.LatestEstimateRevision(ctx, "tenant-1", "space-1", "estimate-1"); err != nil {
		t.Fatalf("latest estimate revision: %v", err)
	}
	if _, err := store.LatestEstimateRevision(ctx, "tenant-1", "space-1", "absent"); err == nil {
		t.Fatal("unknown estimate must 404")
	}
}

func TestDiveTrajectoryDerivesFromSessionState(t *testing.T) {
	ctx := context.Background()
	store := New()
	session := domain.ExplorationSession{
		ID: "session-1", TenantID: "tenant-1", PrincipalID: "agent-1",
		SpaceIDs: []domain.SpaceID{"space-1"}, State: "timeout", StepsUsed: 4,
	}
	if _, _, err := store.Start(ctx, session, "start-1"); err != nil {
		t.Fatalf("start session: %v", err)
	}
	items := []domain.RecallItem{
		{Content: "first", SourceSpaceID: "space-1", Citation: domain.Citation{ID: "cit-b"}},
		{Content: "second", SourceSpaceID: "space-1", Citation: domain.Citation{ID: "cit-a"}},
	}
	if err := store.RecordServed(ctx, session.ID, items); err != nil {
		t.Fatalf("record served: %v", err)
	}
	trajectory, err := store.DiveTrajectory(ctx, "tenant-1", "session-1")
	if err != nil {
		t.Fatalf("derive trajectory: %v", err)
	}
	// Start forces the active state; an active session derives no terminal
	// reason, and served items keep their first-serve order.
	if trajectory.State != "active" || trajectory.TerminalReason != "" || trajectory.Rounds != 4 {
		t.Fatalf("trajectory = %#v, want active with 4 rounds and no terminal reason", trajectory)
	}
	if len(trajectory.ServedItems) != 2 || trajectory.ServedItems[0].Citation.ID != "cit-b" {
		t.Fatalf("served items must preserve first-serve order: %#v", trajectory.ServedItems)
	}

	terminal := domain.DiveTrajectory{
		SessionID: "session-1", TenantID: "tenant-1", State: "submitted", Found: true,
		ServedItems: items, SubmittedCitationIDs: []string{"cit-b"},
	}
	if err := store.RecordDiveTrajectory(ctx, terminal); err != nil {
		t.Fatalf("record terminal trajectory: %v", err)
	}
	if err := store.RecordDiveTrajectory(ctx, terminal); err != nil {
		t.Fatalf("identical trajectory replay must be a no-op: %v", err)
	}
	conflicting := terminal
	conflicting.Found = false
	if err := store.RecordDiveTrajectory(ctx, conflicting); err == nil {
		t.Fatal("a different trajectory for a judged session must conflict")
	}
	recorded, err := store.DiveTrajectory(ctx, "tenant-1", "session-1")
	if err != nil || !recorded.Found || len(recorded.SubmittedCitationIDs) != 1 {
		t.Fatalf("recorded trajectory = (%#v, %v), want the terminal recording", recorded, err)
	}

	result := domain.DiveResult{TenantID: "tenant-1", SessionID: "session-1"}
	if _, duplicate, err := store.PutDiveResult(ctx, result); err != nil || duplicate {
		t.Fatalf("put dive result = (duplicate=%v, %v)", duplicate, err)
	}
	if _, duplicate, err := store.PutDiveResult(ctx, result); err != nil || !duplicate {
		t.Fatalf("dive result replay = (duplicate=%v, %v), want duplicate", duplicate, err)
	}
}

func TestProjectionPublishIsHeadCasAndRoundIdempotent(t *testing.T) {
	ctx := context.Background()
	store := New()
	head, err := store.ProjectionHead(ctx, "tenant-1", "space-1")
	if err != nil || head.Version != 0 || head.Digest != "" {
		t.Fatalf("genesis head = (%#v, %v), want version 0 empty digest", head, err)
	}
	projection := domain.DerivedProjection{
		TenantID: "tenant-1", SpaceID: "space-1", Version: 1, RoundID: "round-1", Digest: "digest-1",
		Nodes: []domain.ProjectionNode{}, Edges: []domain.ProjectionEdge{},
	}
	published := domain.ProjectionVersion(1)
	input := domain.ConsolidationPublishInput{
		TenantID: "tenant-1", SpaceID: "space-1",
		Round:          domain.RoundResult{RoundID: "round-1", Outcome: domain.RoundPublished, BaseVersion: 0, PublishedVersion: &published, CandidateDigest: "digest-1"},
		Projection:     projection,
		ExpectedHead:   domain.ProjectionHead{TenantID: "tenant-1", SpaceID: "space-1"},
		ActivityCursor: domain.ConsolidationCursor{SpaceID: "space-1", LastPublishedBatchOrdinal: 50},
	}
	if _, duplicate, err := store.PublishRound(ctx, input); err != nil || duplicate {
		t.Fatalf("publish round = (duplicate=%v, %v)", duplicate, err)
	}
	if _, duplicate, err := store.PublishRound(ctx, input); err != nil || !duplicate {
		t.Fatalf("round replay = (duplicate=%v, %v), want idempotent duplicate", duplicate, err)
	}
	if head, err := store.ProjectionHead(ctx, "tenant-1", "space-1"); err != nil || head.Version != 1 || head.Digest != "digest-1" {
		t.Fatalf("head after publish = (%#v, %v)", head, err)
	}
	if _, found, err := store.Round(ctx, "tenant-1", "space-1", "round-1"); err != nil || !found {
		t.Fatalf("stored round lookup = (found=%v, %v)", found, err)
	}

	// A second round against the stale genesis head must fail the CAS.
	input.Round.RoundID = "round-2"
	input.Projection.RoundID = "round-2"
	if _, _, err := store.PublishRound(ctx, input); err == nil {
		t.Fatal("publishing against a stale expected head must conflict")
	}
}

func TestProposalFingerprintsAreFrozenPerSpace(t *testing.T) {
	ctx := context.Background()
	store := New()
	proposal := domain.SkillProposal{ProposalID: "proposal-1", RoundID: "round-1", Fingerprint: "fp-1", CandidateID: "candidate-1"}
	if _, duplicate, err := store.CompleteProposalRound(ctx, "tenant-1", "space-1", domain.ProposalRoundOutcome{RoundID: "round-1", Proposal: &proposal, CompletedAt: storeNow}); err != nil || duplicate {
		t.Fatalf("complete round = (duplicate=%v, %v)", duplicate, err)
	}
	replay := domain.SkillProposal{ProposalID: "proposal-1b", RoundID: "round-2", Fingerprint: "fp-1", CandidateID: "candidate-1b"}
	if _, _, err := store.CompleteProposalRound(ctx, "tenant-1", "space-1", domain.ProposalRoundOutcome{RoundID: "round-2", Proposal: &replay, CompletedAt: storeNow}); err == nil {
		t.Fatal("re-proposing a frozen fingerprint under a new round must conflict")
	}
	if _, err := store.ProposalByFingerprint(ctx, "tenant-1", "space-1", "fp-1"); err != nil {
		t.Fatalf("resolve fingerprint: %v", err)
	}
	if _, err := store.RejectionByFingerprint(ctx, "tenant-1", "space-1", "fp-9"); err == nil {
		t.Fatal("unknown rejection must 404")
	}
}

func TestCandidateDecisionThenActivationVersionCas(t *testing.T) {
	ctx := context.Background()
	store := New()
	candidate := domain.SkillCandidate{
		TenantID: "tenant-1", SpaceID: "space-1", CandidateID: "candidate-1",
		ProposalID: "proposal-1", TargetSkillID: "skill-1", BaseArtifactVersion: 4,
		Status: domain.CandidateNeedsReview,
	}
	if _, duplicate, err := store.PutCandidate(ctx, candidate); err != nil || duplicate {
		t.Fatalf("put candidate = (duplicate=%v, %v)", duplicate, err)
	}
	if candidate, err := store.Candidate(ctx, "tenant-1", "space-1", "candidate-1"); err != nil || candidate.Status != domain.CandidateNeedsReview {
		t.Fatalf("candidate lookup = (%q, %v)", candidate.Status, err)
	}
	decision := domain.CandidateDecision{DecisionID: "decision-1", CandidateID: "candidate-1", Decision: "accepted"}
	if created, err := store.DecideCandidate(ctx, decision); err != nil || !created {
		t.Fatalf("decide = (created=%v, %v)", created, err)
	}
	if candidate, err := store.Candidate(ctx, "tenant-1", "space-1", "candidate-1"); err != nil || candidate.Status != domain.CandidateAccepted {
		t.Fatalf("decided candidate status = (%q, %v), want accepted", candidate.Status, err)
	}
	if created, err := store.DecideCandidate(ctx, decision); err != nil || created {
		t.Fatalf("decision replay = (created=%v, %v), want idempotent", created, err)
	}

	activation := domain.SkillActivation{ActivationID: "activation-1", CandidateID: "candidate-1", DecisionID: "decision-1", ExpectedBaseVersion: 4, NewArtifactVersion: 5}
	if created, err := store.ActivateCandidate(ctx, activation); err != nil || !created {
		t.Fatalf("activate = (created=%v, %v)", created, err)
	}
	stale := domain.SkillActivation{ActivationID: "activation-2", CandidateID: "candidate-1", DecisionID: "decision-1", ExpectedBaseVersion: 4, NewArtifactVersion: 6}
	if _, err := store.ActivateCandidate(ctx, stale); err == nil {
		t.Fatal("activation against a moved skill version must conflict")
	}
	if created, err := store.ActivateCandidate(ctx, activation); err != nil || created {
		t.Fatalf("activation replay = (created=%v, %v), want idempotent", created, err)
	}

	unactivated := domain.SkillCandidate{
		TenantID: "tenant-1", SpaceID: "space-1", CandidateID: "candidate-2",
		ProposalID: "proposal-2", TargetSkillID: "skill-2", Status: domain.CandidateNeedsReview,
	}
	if _, _, err := store.PutCandidate(ctx, unactivated); err != nil {
		t.Fatalf("put second candidate: %v", err)
	}
	if _, err := store.ActivateCandidate(ctx, domain.SkillActivation{ActivationID: "activation-3", CandidateID: "candidate-2", ExpectedBaseVersion: 0, NewArtifactVersion: 1}); err == nil {
		t.Fatal("activating without an accepted decision must fail")
	}
}
