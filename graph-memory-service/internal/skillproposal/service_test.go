package skillproposal_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillproposal"
	"river2.dev/graph-memory-service/internal/store/memory"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

var proposalNow = time.Date(2026, time.September, 8, 14, 0, 0, 0, time.UTC)

func TestProposalFingerprintCanonicalizesPatternRefsAndRemembersRejection(t *testing.T) {
	base := skillproposal.ProposalFingerprintInput{
		TargetSkillID: "skill-1", BaseArtifactVersion: 4, BaseArtifactHash: "base-hash",
		CandidateArtifactHash: "candidate-hash", ReviewedDiffHash: "diff-hash", RequestedScope: "workspace",
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-b", Revision: 2}, {PatternID: "pattern-a", Revision: 1}},
	}
	first, err := skillproposal.FingerprintProposal(base)
	if err != nil {
		t.Fatalf("fingerprint proposal: %v", err)
	}
	reordered := base
	reordered.PatternRefs = []domain.PatternRef{{PatternID: "pattern-a", Revision: 1}, {PatternID: "pattern-b", Revision: 2}}
	second, err := skillproposal.FingerprintProposal(reordered)
	if err != nil || second != first {
		t.Fatalf("canonical fingerprint = (%q, %v), want %q", second, err, first)
	}
	changed := base
	changed.CandidateArtifactHash = "materially-different"
	third, err := skillproposal.FingerprintProposal(changed)
	if err != nil || third == first {
		t.Fatalf("materially changed fingerprint = (%q, %v), must differ from %q", third, err, first)
	}

	proposals := newProposalStore()
	proposals.rejections[first] = domain.RejectionMemory{Fingerprint: first, ProposalID: "proposal-old", CandidateID: "candidate-old", DecisionID: "decision-old", Reason: "unsafe", PayloadHash: "rejection-hash", RejectedAt: proposalNow.Add(-time.Hour)}
	service := skillproposal.New(nil, patternStore{}, proposals, &candidateStore{}, fixedClock{now: proposalNow})
	outcome := domain.ProposalRoundOutcome{RoundID: "round-rejected", Proposal: &domain.SkillProposal{
		ProposalID: "proposal-new", RoundID: "round-rejected", Fingerprint: first,
		PatternRefs: reordered.PatternRefs, CandidateID: "candidate-new", CreatedAt: proposalNow,
	}, CompletedAt: proposalNow}
	if _, _, err := service.CompleteRound(context.Background(), "tenant-1", "space-1", outcome); err == nil {
		t.Fatal("rejected fingerprint was proposed again")
	}
	if proposals.completeCalls != 0 {
		t.Fatalf("rejected fingerprint reached durable completion %d times", proposals.completeCalls)
	}
}

func TestProposalRoundContainsOneCompleteAtomicProposalOrNoAction(t *testing.T) {
	proposal := domain.SkillProposal{
		ProposalID: "proposal-1", RoundID: "round-1", Fingerprint: "fingerprint-1",
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 2}}, CandidateID: "candidate-1", CreatedAt: proposalNow,
	}
	tests := []struct {
		name    string
		outcome domain.ProposalRoundOutcome
		valid   bool
	}{
		{name: "one complete proposal", outcome: domain.ProposalRoundOutcome{RoundID: "round-1", Proposal: &proposal, CompletedAt: proposalNow}, valid: true},
		{name: "explicit no action", outcome: domain.ProposalRoundOutcome{RoundID: "round-2", NoActionReason: "no eligible pattern", CompletedAt: proposalNow}, valid: true},
		{name: "proposal and no action are mutually exclusive", outcome: domain.ProposalRoundOutcome{RoundID: "round-3", Proposal: &proposal, NoActionReason: "also none", CompletedAt: proposalNow}},
		{name: "empty round is not atomic", outcome: domain.ProposalRoundOutcome{RoundID: "round-4", CompletedAt: proposalNow}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := skillproposal.ValidateRoundOutcome(test.outcome)
			if test.valid && err != nil {
				t.Fatalf("valid outcome rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid outcome accepted")
			}
		})
	}

	store := newProposalStore()
	service := skillproposal.New(nil, patternStore{}, store, &candidateStore{}, fixedClock{now: proposalNow})
	outcome := domain.ProposalRoundOutcome{RoundID: "round-atomic", Proposal: &domain.SkillProposal{
		ProposalID: "proposal-atomic", RoundID: "round-atomic", Fingerprint: "fingerprint-atomic",
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 3}}, CandidateID: "candidate-atomic", CreatedAt: proposalNow,
	}, CompletedAt: proposalNow}
	got, duplicate, err := service.CompleteRound(context.Background(), "tenant-1", "space-1", outcome)
	if err != nil || duplicate || got.Proposal == nil || store.completeCalls != 1 {
		t.Fatalf("complete round = (%#v, duplicate=%v, %v), calls=%d", got, duplicate, err, store.completeCalls)
	}
	if len(store.rounds) != 1 || store.rounds["round-atomic"].Proposal.CandidateID != "candidate-atomic" {
		t.Fatalf("proposal round was not stored atomically: %#v", store.rounds)
	}
}

type replayRunner struct {
	calls       []domain.ReplayTrialRequest
	observation func(domain.ReplayTrialRequest) (domain.ReplayObservation, error)
}

func (r *replayRunner) RunTrial(_ context.Context, request domain.ReplayTrialRequest) (domain.ReplayObservation, error) {
	r.calls = append(r.calls, request)
	return r.observation(request)
}

func TestPairedReplayPinsEveryConditionAndRequiresCompleteValidPairs(t *testing.T) {
	plan := domain.PairedReplayPlan{
		ReplayID: "replay-1", CandidateID: "candidate-1", CaseHash: "case-hash",
		CandidateArtifactHash: "candidate-hash", BaseArtifactHash: "base-hash", ContextSnapshotHash: "context-hash",
		WorkspaceImageHash: "image-hash", EnvironmentManifestHash: "environment-hash", ModelID: "model-1", RuntimeID: "runtime-1",
		ToolPolicyVersion: "tools-v1", EvaluatorVersion: "evaluator-v1", RandomnessPolicyHash: "randomness-hash", MinPairs: 2, MaxPairs: 2,
	}

	t.Run("arms differ only by skill presence", func(t *testing.T) {
		runner := &replayRunner{observation: func(request domain.ReplayTrialRequest) (domain.ReplayObservation, error) {
			return domain.ReplayObservation{InfrastructureValid: true, Success: request.Arm == domain.WithSkill, EvidenceHash: string(request.Arm) + "-evidence"}, nil
		}}
		got, err := skillproposal.NewPairedEvaluator(runner, fixedClock{now: proposalNow}).Evaluate(context.Background(), plan)
		if err != nil {
			t.Fatalf("evaluate paired replay: %v", err)
		}
		if !got.Passed || got.With.Trials != 2 || got.With.Successes != 2 || got.Without.Trials != 2 || got.Without.Successes != 0 {
			t.Fatalf("paired result = %#v", got)
		}
		if len(runner.calls) != 4 {
			t.Fatalf("trial calls = %d, want two complete pairs", len(runner.calls))
		}
		seen := make(map[int]map[domain.ReplayArm]bool)
		for _, call := range runner.calls {
			if !reflect.DeepEqual(call.Plan, plan) {
				t.Fatalf("arm changed a pinned condition:\n got: %#v\nwant: %#v", call.Plan, plan)
			}
			if call.Arm != domain.WithSkill && call.Arm != domain.WithoutSkill {
				t.Fatalf("unexpected replay arm %q", call.Arm)
			}
			if seen[call.Ordinal] == nil {
				seen[call.Ordinal] = make(map[domain.ReplayArm]bool)
			}
			seen[call.Ordinal][call.Arm] = true
		}
		for ordinal := 1; ordinal <= 2; ordinal++ {
			if !seen[ordinal][domain.WithSkill] || !seen[ordinal][domain.WithoutSkill] {
				t.Fatalf("ordinal %d is not a complete pair: %#v", ordinal, seen[ordinal])
			}
		}
	})

	t.Run("infrastructure invalid arm makes replay inconclusive", func(t *testing.T) {
		runner := &replayRunner{observation: func(request domain.ReplayTrialRequest) (domain.ReplayObservation, error) {
			valid := !(request.Ordinal == 2 && request.Arm == domain.WithoutSkill)
			return domain.ReplayObservation{InfrastructureValid: valid, Success: request.Arm == domain.WithSkill, EvidenceHash: "evidence"}, nil
		}}
		got, err := skillproposal.NewPairedEvaluator(runner, fixedClock{now: proposalNow}).Evaluate(context.Background(), plan)
		if err != nil {
			t.Fatalf("evaluate invalid pair: %v", err)
		}
		if got.Passed {
			t.Fatalf("partial or infrastructure-invalid pairs passed: %#v", got)
		}
	})
}

func TestAcceptedDecisionDoesNotActivateAndActivationRequiresHumanCuration(t *testing.T) {
	t.Run("accepted is only a decision", func(t *testing.T) {
		registry := registryFor(t, domain.PrincipalHuman, domain.GrantPurposeCuration, domain.GrantOperationCandidateDecide)
		candidates := &candidateStore{candidate: candidateFixture(domain.CandidateEvaluating)}
		service := skillproposal.New(registry, patternStore{}, newProposalStore(), candidates, fixedClock{now: proposalNow})
		decision := domain.CandidateDecision{
			DecisionID: "decision-1", CandidateID: "candidate-1", Decision: "accepted", CandidateDigest: "candidate-digest",
			ReplayResultID: "replay-1", PolicyVersion: "decision-v1", Reason: "paired replay passed",
		}
		got, duplicate, err := service.Decide(context.Background(), "tenant-1", "curator", "space-1", decision)
		if err != nil || duplicate || got.Decision != "accepted" {
			t.Fatalf("decide = (%#v, duplicate=%v, %v)", got, duplicate, err)
		}
		if candidates.decideCalls != 1 || candidates.activateCalls != 0 {
			t.Fatalf("accepted decision caused activation: decide=%d activate=%d", candidates.decideCalls, candidates.activateCalls)
		}
	})

	tests := []struct {
		name      string
		kind      domain.PrincipalKind
		purpose   domain.GrantPurpose
		wantAllow bool
	}{
		{name: "agent with curation grant is not a human curator", kind: domain.PrincipalAgent, purpose: domain.GrantPurposeCuration},
		{name: "human lifecycle grant cannot activate", kind: domain.PrincipalHuman, purpose: domain.GrantPurposeLifecycle},
		{name: "human curator with exact curation grant may activate", kind: domain.PrincipalHuman, purpose: domain.GrantPurposeCuration, wantAllow: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := registryFor(t, test.kind, test.purpose, domain.GrantOperationCandidateActivate)
			candidates := &candidateStore{candidate: candidateFixture(domain.CandidateAccepted)}
			service := skillproposal.New(registry, patternStore{}, newProposalStore(), candidates, fixedClock{now: proposalNow})
			activation := domain.SkillActivation{
				ActivationID: "activation-1", CandidateID: "candidate-1", DecisionID: "decision-1", ReplayResultID: "replay-1",
				CandidateDigest: "candidate-digest", ExpectedBaseVersion: 4,
			}
			got, duplicate, err := service.Activate(context.Background(), "tenant-1", "curator", "space-1", activation)
			if !test.wantAllow {
				if err == nil || candidates.activateCalls != 0 {
					t.Fatalf("forbidden activation = (%#v, duplicate=%v, %v), calls=%d", got, duplicate, err, candidates.activateCalls)
				}
				return
			}
			if err != nil || duplicate || candidates.activateCalls != 1 || got.CandidateID != "candidate-1" {
				t.Fatalf("curator activation = (%#v, duplicate=%v, %v), calls=%d", got, duplicate, err, candidates.activateCalls)
			}
		})
	}
}

type patternStore struct{}

func (patternStore) AppendPatternRevision(_ context.Context, revision domain.PatternRevision, _ string) (domain.PatternRevision, bool, error) {
	return revision, false, nil
}
func (patternStore) LatestPatternRevision(context.Context, domain.TenantID, domain.SpaceID, string) (domain.PatternRevision, error) {
	return domain.PatternRevision{}, errors.New("pattern not found")
}

type proposalStore struct {
	rounds        map[string]domain.ProposalRoundOutcome
	proposals     map[domain.ProposalFingerprint]domain.SkillProposal
	rejections    map[domain.ProposalFingerprint]domain.RejectionMemory
	completeCalls int
}

func newProposalStore() *proposalStore {
	return &proposalStore{
		rounds: make(map[string]domain.ProposalRoundOutcome), proposals: make(map[domain.ProposalFingerprint]domain.SkillProposal),
		rejections: make(map[domain.ProposalFingerprint]domain.RejectionMemory),
	}
}
func (s *proposalStore) CompleteProposalRound(_ context.Context, _ domain.TenantID, _ domain.SpaceID, outcome domain.ProposalRoundOutcome) (domain.ProposalRoundOutcome, bool, error) {
	s.completeCalls++
	if existing, ok := s.rounds[outcome.RoundID]; ok {
		return existing, true, nil
	}
	s.rounds[outcome.RoundID] = outcome
	if outcome.Proposal != nil {
		s.proposals[outcome.Proposal.Fingerprint] = *outcome.Proposal
	}
	return outcome, false, nil
}
func (s *proposalStore) ProposalByFingerprint(_ context.Context, _ domain.TenantID, _ domain.SpaceID, fingerprint domain.ProposalFingerprint) (domain.SkillProposal, error) {
	if proposal, ok := s.proposals[fingerprint]; ok {
		return proposal, nil
	}
	return domain.SkillProposal{}, domain.NewProtocolError(404, "PROPOSAL_NOT_FOUND", "proposal not found")
}
func (s *proposalStore) PutRejection(_ context.Context, _ domain.TenantID, _ domain.SpaceID, rejection domain.RejectionMemory) (bool, error) {
	if _, ok := s.rejections[rejection.Fingerprint]; ok {
		return false, nil
	}
	s.rejections[rejection.Fingerprint] = rejection
	return true, nil
}
func (s *proposalStore) RejectionByFingerprint(_ context.Context, _ domain.TenantID, _ domain.SpaceID, fingerprint domain.ProposalFingerprint) (domain.RejectionMemory, error) {
	if rejection, ok := s.rejections[fingerprint]; ok {
		return rejection, nil
	}
	return domain.RejectionMemory{}, domain.NewProtocolError(404, "REJECTION_NOT_FOUND", "rejection not found")
}

type candidateStore struct {
	candidate     domain.SkillCandidate
	decideCalls   int
	activateCalls int
}

func (s *candidateStore) PutCandidate(_ context.Context, candidate domain.SkillCandidate) (domain.SkillCandidate, bool, error) {
	s.candidate = candidate
	return candidate, false, nil
}
func (s *candidateStore) Candidate(context.Context, domain.TenantID, domain.SpaceID, string) (domain.SkillCandidate, error) {
	return s.candidate, nil
}
func (s *candidateStore) PutReviewedDiff(context.Context, domain.ReviewedDiff) (bool, error) {
	return true, nil
}
func (s *candidateStore) PutReplayPlan(context.Context, domain.PairedReplayPlan) (bool, error) {
	return true, nil
}
func (s *candidateStore) AppendReplayTrial(context.Context, domain.PairedReplayTrial) (bool, error) {
	return true, nil
}
func (s *candidateStore) PutReplayResult(context.Context, domain.PairedReplayResult) (bool, error) {
	return true, nil
}
func (s *candidateStore) DecideCandidate(context.Context, domain.CandidateDecision) (bool, error) {
	s.decideCalls++
	return false, nil
}
func (s *candidateStore) ActivateCandidate(context.Context, domain.SkillActivation) (bool, error) {
	s.activateCalls++
	return false, nil
}

func candidateFixture(status domain.CandidateStatus) domain.SkillCandidate {
	return domain.SkillCandidate{
		TenantID: "tenant-1", SpaceID: "space-1", CandidateID: "candidate-1", ProposalID: "proposal-1", Fingerprint: "fingerprint-1",
		TargetSkillID: "skill-1", BaseArtifactVersion: 4,
		Diff:        domain.ReviewedDiff{DiffID: "diff-1", BaseArtifactVersion: 4, BaseArtifactHash: "base-hash", CandidateArtifactHash: "candidate-hash", DiffHash: "diff-hash", Reviewer: "reviewer", ReviewPolicyVersion: "review-v1", ReviewedAt: proposalNow.Add(-time.Hour)},
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 3}}, Status: status, ReplayResultID: "replay-1", CreatedAt: proposalNow.Add(-time.Hour),
	}
}

func registryFor(t *testing.T, kind domain.PrincipalKind, purpose domain.GrantPurpose, operation domain.GrantOperation) *memory.Store {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	if _, err := store.InitializeTenant(ctx, domain.Tenant{ID: "tenant-1", DisplayName: "Tenant", BootstrapPrincipalID: "bootstrap"}, domain.Principal{ID: "bootstrap", TenantID: "tenant-1", Kind: domain.PrincipalService, DisplayName: "Bootstrap"}); err != nil {
		t.Fatalf("initialize tenant: %v", err)
	}
	if _, err := store.PutSpace(ctx, "tenant-1", domain.Space{ID: "space-1", TenantID: "tenant-1", Scope: domain.SpaceShared, DisplayName: "Shared"}); err != nil {
		t.Fatalf("put space: %v", err)
	}
	if _, err := store.PutPrincipal(ctx, "tenant-1", domain.Principal{ID: "curator", TenantID: "tenant-1", Kind: kind, DisplayName: "Curator"}); err != nil {
		t.Fatalf("put principal: %v", err)
	}
	if _, err := store.PutGrant(ctx, "tenant-1", domain.Grant{
		ID: "grant-curator", TenantID: "tenant-1", PrincipalID: "curator", SpaceIDs: []domain.SpaceID{"space-1"},
		Purpose: purpose, Operations: []domain.GrantOperation{operation}, ExpiresAt: proposalNow.Add(time.Hour),
	}); err != nil {
		t.Fatalf("put grant: %v", err)
	}
	return store
}

func TestExistingProposalFingerprintCannotBeSubmittedByAnotherRound(t *testing.T) {
	store := newProposalStore()
	store.proposals["fingerprint-existing"] = domain.SkillProposal{
		ProposalID: "proposal-existing", RoundID: "round-existing", Fingerprint: "fingerprint-existing",
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 3}}, CandidateID: "candidate-existing", CreatedAt: proposalNow.Add(-time.Hour),
	}
	outcome := domain.ProposalRoundOutcome{RoundID: "round-new", Proposal: &domain.SkillProposal{
		ProposalID: "proposal-new", RoundID: "round-new", Fingerprint: "fingerprint-existing",
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 3}}, CandidateID: "candidate-new", CreatedAt: proposalNow,
	}, CompletedAt: proposalNow}
	service := skillproposal.New(nil, patternStore{}, store, &candidateStore{}, fixedClock{now: proposalNow})
	_, _, err := service.CompleteRound(context.Background(), "tenant-1", "space-1", outcome)
	var protocol *domain.ProtocolError
	if !errors.As(err, &protocol) || protocol.Code() != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("existing fingerprint error = %v, want IDEMPOTENCY_CONFLICT", err)
	}
	if store.completeCalls != 0 || len(store.proposals) != 1 {
		t.Fatalf("duplicate fingerprint mutated proposal state: calls=%d proposals=%#v", store.completeCalls, store.proposals)
	}
}

func TestRejectedProposalFingerprintIsPermanentlyBlocked(t *testing.T) {
	store := newProposalStore()
	store.rejections["fingerprint-rejected"] = domain.RejectionMemory{
		Fingerprint: "fingerprint-rejected", ProposalID: "proposal-old", CandidateID: "candidate-old",
		DecisionID: "decision-old", Reason: "unsafe", PayloadHash: "rejection-hash", RejectedAt: proposalNow.Add(-time.Hour),
	}
	outcome := domain.ProposalRoundOutcome{RoundID: "round-new", Proposal: &domain.SkillProposal{
		ProposalID: "proposal-new", RoundID: "round-new", Fingerprint: "fingerprint-rejected",
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 3}}, CandidateID: "candidate-new", CreatedAt: proposalNow,
	}, CompletedAt: proposalNow}
	service := skillproposal.New(nil, patternStore{}, store, &candidateStore{}, fixedClock{now: proposalNow})
	_, _, err := service.CompleteRound(context.Background(), "tenant-1", "space-1", outcome)
	var protocol *domain.ProtocolError
	if !errors.As(err, &protocol) || protocol.Code() != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("rejected fingerprint error = %v, want IDEMPOTENCY_CONFLICT", err)
	}
	if store.completeCalls != 0 || len(store.rejections) != 1 {
		t.Fatalf("rejected fingerprint mutated proposal state: calls=%d rejections=%#v", store.completeCalls, store.rejections)
	}
}
