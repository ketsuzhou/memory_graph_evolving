package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

// Curation adapter state and the seven M1–M4 port implementations. Every
// mutation is one critical section under the store mutex, matching the
// atomicity the production adapter must provide: appends are idempotent on
// identical content, conflicting replays fail closed with 409, and the
// causal/revision chains and projection head move only through their
// governed transitions.

var (
	errCausalChain        = domain.NewProtocolError(409, "CAUSAL_CHAIN_CONFLICT", "causal record does not extend the stored hash chain")
	errCausalRevision     = domain.NewProtocolError(409, "CAUSAL_REVISION_CONFLICT", "causal revision number conflicts with the stored ledger")
	errTrialNotFound      = domain.NewProtocolError(404, "CAUSAL_TRIAL_NOT_FOUND", "causal trial event not found")
	errEstimateNotFound   = domain.NewProtocolError(404, "CAUSAL_ESTIMATE_NOT_FOUND", "causal estimate revision not found")
	errRewardNotFound     = domain.NewProtocolError(404, "CAUSAL_REWARD_NOT_FOUND", "causal reward revision not found")
	errDiveConflict       = domain.NewProtocolError(409, "DIVE_CONFLICT", "dive record was already stored with different content")
	errHeadMoved          = domain.NewProtocolError(409, "PROJECTION_HEAD_MOVED", "published projection head changed since the round started")
	errProjectionMissing  = domain.NewProtocolError(404, "PROJECTION_NOT_FOUND", "derived projection not found")
	errRoundConflict      = domain.NewProtocolError(409, "ROUND_CONFLICT", "consolidation round was already recorded with different content")
	errReplayPlanConflict = domain.NewProtocolError(409, "REPLAY_PLAN_CONFLICT", "replay plan was already frozen with different content")
	errReplayResultConf   = domain.NewProtocolError(409, "REPLAY_RESULT_CONFLICT", "replay result was already recorded with different content")
	errPatternMissing     = domain.NewProtocolError(404, "PATTERN_NOT_FOUND", "pattern revision not found")
	errPatternConflict    = domain.NewProtocolError(409, "PATTERN_REVISION_CONFLICT", "pattern revision conflicts with stored revisions")
	errFingerprintFrozen  = domain.NewProtocolError(409, "IDEMPOTENCY_CONFLICT", "proposal fingerprint is frozen and cannot be reused")
	errProposalMissing    = domain.NewProtocolError(404, "PROPOSAL_NOT_FOUND", "proposal not found")
	errRejectionMissing   = domain.NewProtocolError(404, "REJECTION_NOT_FOUND", "rejection memory not found")
	errRejectionConflict  = domain.NewProtocolError(409, "REJECTION_CONFLICT", "rejection memory was already recorded with different content")
	errDiffConflict       = domain.NewProtocolError(409, "REVIEWED_DIFF_CONFLICT", "reviewed diff was already recorded with different content")
	errCandidateMissing   = domain.NewProtocolError(404, "CANDIDATE_NOT_FOUND", "skill candidate not found")
	errCandidateConflict  = domain.NewProtocolError(409, "CANDIDATE_CONFLICT", "skill candidate was already recorded with different content")
	errDecisionConflict   = domain.NewProtocolError(409, "CANDIDATE_DECISION_CONFLICT", "candidate was already decided with different content")
	errDecisionRequired   = domain.NewProtocolError(409, "DECISION_NOT_ACCEPTED", "activation requires an accepted decision")
	errActivationConflict = domain.NewProtocolError(409, "ACTIVATION_VERSION_CONFLICT", "active skill version changed since activation was prepared")
	errTrialKeyConflict   = domain.NewProtocolError(409, "REPLAY_TRIAL_CONFLICT", "replay trial was already recorded with different content")
)

var (
	_ ports.CausalEvidenceStore    = (*Store)(nil)
	_ ports.DiveStore              = (*Store)(nil)
	_ ports.ConsolidationStore     = (*Store)(nil)
	_ ports.ProjectionBuilderStore = (*Store)(nil)
	_ ports.RetrievalReplayStore   = (*Store)(nil)
	_ ports.PatternStore           = (*Store)(nil)
	_ ports.ProposalStore          = (*Store)(nil)
	_ ports.CandidateStore         = CandidateStore{}
	_ ports.ActivationPolicyStore  = CandidateStore{}
)

// CandidateStore adapts the store onto the CandidateStore port. The port's
// PutReplayPlan(PairedReplayPlan) and PutReplayResult(PairedReplayResult)
// cannot share names with the RetrievalReplayStore's same-named methods on a
// single Go type, so the paired-replay pair is renamed on the store and
// restored to the port shape here.
type CandidateStore struct {
	Store *Store
}

func (c CandidateStore) PutCandidate(ctx context.Context, candidate domain.SkillCandidate) (domain.SkillCandidate, bool, error) {
	return c.Store.PutCandidate(ctx, candidate)
}

func (c CandidateStore) Candidate(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, candidateID string) (domain.SkillCandidate, error) {
	return c.Store.Candidate(ctx, tenantID, spaceID, candidateID)
}

func (c CandidateStore) PutReviewedDiff(ctx context.Context, diff domain.ReviewedDiff) (bool, error) {
	return c.Store.PutReviewedDiff(ctx, diff)
}

func (c CandidateStore) PutReplayPlan(ctx context.Context, plan domain.PairedReplayPlan) (bool, error) {
	return c.Store.PutCandidateReplayPlan(ctx, plan)
}

func (c CandidateStore) AppendReplayTrial(ctx context.Context, trial domain.PairedReplayTrial) (bool, error) {
	return c.Store.AppendReplayTrial(ctx, trial)
}

func (c CandidateStore) PutReplayResult(ctx context.Context, result domain.PairedReplayResult) (bool, error) {
	return c.Store.PutCandidateReplayResult(ctx, result)
}

func (c CandidateStore) DecideCandidate(ctx context.Context, decision domain.CandidateDecision) (bool, error) {
	return c.Store.DecideCandidate(ctx, decision)
}

func (c CandidateStore) ActivateCandidate(ctx context.Context, activation domain.SkillActivation) (bool, error) {
	return c.Store.ActivateCandidate(ctx, activation)
}

func (c CandidateStore) RegisterArmCCandidate(ctx context.Context, registration domain.ArmCCandidateRegistration) (bool, error) {
	return c.Store.RegisterArmCCandidate(ctx, registration)
}
func (c CandidateStore) ArmCCandidate(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, candidateID string) (domain.ArmCCandidateRegistration, error) {
	return c.Store.ArmCCandidate(ctx, tenantID, spaceID, candidateID)
}

func (c CandidateStore) PutActivationPolicyDecision(ctx context.Context, decision domain.ActivationPolicyDecision) (bool, error) {
	return c.Store.PutActivationPolicyDecision(ctx, decision)
}

func (c CandidateStore) ActivationPolicyDecision(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, ref domain.ActivationPolicyDecisionRef) (domain.ActivationPolicyDecision, error) {
	return c.Store.ActivationPolicyDecision(ctx, tenantID, spaceID, ref)
}

func (c CandidateStore) PutArmCEvaluation(ctx context.Context, evaluation domain.ArmCEvaluation) (bool, error) {
	return c.Store.PutArmCEvaluation(ctx, evaluation)
}

func (c CandidateStore) ArmCEvaluation(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, ref domain.ArmCEvaluationRef) (domain.ArmCEvaluation, error) {
	return c.Store.ArmCEvaluation(ctx, tenantID, spaceID, ref)
}

func (c CandidateStore) PutCoverageProof(ctx context.Context, proof domain.CoverageProof) (bool, error) {
	return c.Store.PutCoverageProof(ctx, proof)
}

func (c CandidateStore) CoverageProof(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, ref domain.CoverageProofRef) (domain.CoverageProof, error) {
	return c.Store.CoverageProof(ctx, tenantID, spaceID, ref)
}

func (c CandidateStore) ActivatePolicyDecision(ctx context.Context, activation domain.SkillActivation) (bool, error) {
	return c.Store.ActivatePolicyDecision(ctx, activation)
}

func (c CandidateStore) ActivateArmCCandidatePolicyDecision(ctx context.Context, registration domain.ArmCCandidateRegistration, activation domain.SkillActivation) (bool, error) {
	return c.Store.ActivateArmCCandidatePolicyDecision(ctx, registration, activation)
}

func (c CandidateStore) CandidateOutcomes(ctx context.Context, refs []contract.CandidateArtifactRef) ([]domain.CandidateLifecycleOutcome, error) {
	return c.Store.CandidateOutcomes(ctx, refs)
}

func (s *Store) initCuration() {
	s.causalTrials = make(map[domain.TenantID]map[domain.SpaceID]map[domain.CausalTrialEventID]domain.CausalTrialEvent)
	s.causalTrialOrder = make(map[domain.TenantID]map[domain.SpaceID][]domain.CausalTrialEventID)
	s.causalTrialHeads = make(map[domain.TenantID]map[domain.SpaceID]string)
	s.causalEstimates = make(map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]map[int64]domain.CausalEstimateRevision)
	s.causalEstimateLatest = make(map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]int64)
	s.causalEstimateHeads = make(map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]string)
	s.causalRewards = make(map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]map[int64]domain.CausalRewardRevision)
	s.causalRewardLatest = make(map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]int64)
	s.causalRewardHeads = make(map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]string)

	s.diveTrajectories = make(map[domain.TenantID]map[domain.ExplorationSessionID]domain.DiveTrajectory)
	s.diveServedOrder = make(map[domain.TenantID]map[domain.ExplorationSessionID][]string)
	s.diveResults = make(map[domain.TenantID]map[domain.ExplorationSessionID]domain.DiveResult)
	s.diveTrajectoryDigests = make(map[domain.TenantID]map[domain.ExplorationSessionID]string)

	s.consolidationActivity = make(map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationActivity)
	s.consolidationCursors = make(map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationCursor)
	s.projectionHeads = make(map[domain.TenantID]map[domain.SpaceID]domain.ProjectionHead)
	s.projectionVersions = make(map[domain.TenantID]map[domain.SpaceID]map[domain.ProjectionVersion]domain.DerivedProjection)
	s.consolidationRounds = make(map[domain.TenantID]map[domain.SpaceID]map[domain.ConsolidationRoundID]domain.RoundResult)
	s.retrievalReplayPlans = make(map[string]domain.RetrievalReplayPlan)
	s.retrievalReplayResults = make(map[domain.ConsolidationRoundID]domain.CandidateStats)

	s.patternRevisions = make(map[domain.TenantID]map[domain.SpaceID]map[string]map[int64]domain.PatternRevision)
	s.patternLatest = make(map[domain.TenantID]map[domain.SpaceID]map[string]int64)
	s.proposalRounds = make(map[domain.TenantID]map[domain.SpaceID]map[string]domain.ProposalRoundOutcome)
	s.proposals = make(map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillProposal)
	s.proposalFingerprintIndex = make(map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]string)
	s.rejectionMemory = make(map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]domain.RejectionMemory)
	s.reviewedDiffs = make(map[string]domain.ReviewedDiff)
	s.candidates = make(map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillCandidate)
	s.armCCandidates = make(map[domain.TenantID]map[domain.SpaceID]map[string]domain.ArmCCandidateRegistration)
	s.candidateByProposal = make(map[string]string)
	s.pairedReplayPlans = make(map[string]domain.PairedReplayPlan)
	s.pairedReplayTrials = make(map[string]map[domain.ReplayArm]map[int]domain.PairedReplayTrial)
	s.pairedReplayResults = make(map[string]domain.PairedReplayResult)
	s.mutationBacktestResults = make(map[string]any)
	s.candidateDecisions = make(map[string]domain.CandidateDecision)
	s.activationPolicyDecisions = make(map[string]domain.ActivationPolicyDecision)
	s.armCEvaluations = make(map[string]domain.ArmCEvaluation)
	s.coverageProofs = make(map[string]domain.CoverageProof)
	s.skillActivations = make(map[string]map[string]domain.SkillActivation)
	s.activeSkillVersions = make(map[string]int64)
}

// CausalEvidenceStore: append-only hash chains. The store is the only
// component that can see the chain head, so it is the one that rejects a
// record whose PreviousHash does not extend it.

func (s *Store) AppendTrial(ctx context.Context, event domain.CausalTrialEvent) (domain.CausalTrialEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bySpace := ensure3(s.causalTrials, event.TenantID, event.SpaceID, event.ID)
	if existing, ok := bySpace[event.ID]; ok {
		return reuse(existing, event, cloneTrial, errCausalChain)
	}
	if event.PreviousHash != s.causalTrialHeads[event.TenantID][event.SpaceID] {
		return domain.CausalTrialEvent{}, false, errCausalChain
	}
	bySpace[event.ID] = cloneTrial(event)
	order := ensure2(s.causalTrialOrder, event.TenantID, event.SpaceID)
	s.causalTrialOrder[event.TenantID][event.SpaceID] = append(order[event.SpaceID], event.ID)
	setHead2(s.causalTrialHeads, event.TenantID, event.SpaceID, event.PayloadHash)
	return cloneTrial(event), false, nil
}

func (s *Store) Trial(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, id domain.CausalTrialEventID) (domain.CausalTrialEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	event, ok := s.causalTrials[tenantID][spaceID][id]
	if !ok {
		return domain.CausalTrialEvent{}, errTrialNotFound
	}
	return cloneTrial(event), nil
}

func (s *Store) AppendEstimateRevision(ctx context.Context, revision domain.CausalEstimateRevision) (domain.CausalEstimateRevision, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byEstimate := ensure4(s.causalEstimates, revision.TenantID, revision.SpaceID, revision.EstimateID, revision.Revision)
	if existing, ok := byEstimate[revision.Revision]; ok {
		return reuse(existing, revision, cloneEstimate, errCausalRevision)
	}
	latest := s.causalEstimateLatest[revision.TenantID][revision.SpaceID][revision.EstimateID]
	if revision.Revision != latest+1 {
		return domain.CausalEstimateRevision{}, false, errCausalRevision
	}
	if revision.PreviousHash != s.causalEstimateHeads[revision.TenantID][revision.SpaceID][revision.EstimateID] {
		return domain.CausalEstimateRevision{}, false, errCausalChain
	}
	byEstimate[revision.Revision] = cloneEstimate(revision)
	ensure3(s.causalEstimateLatest, revision.TenantID, revision.SpaceID, revision.EstimateID)
	s.causalEstimateLatest[revision.TenantID][revision.SpaceID][revision.EstimateID] = revision.Revision
	setHead3(s.causalEstimateHeads, revision.TenantID, revision.SpaceID, revision.EstimateID, revision.PayloadHash)
	return cloneEstimate(revision), false, nil
}

func (s *Store) EstimateRevision(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, estimateID domain.CausalEstimateID, revision int64) (domain.CausalEstimateRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.causalEstimates[tenantID][spaceID][estimateID][revision]
	if !ok {
		return domain.CausalEstimateRevision{}, errEstimateNotFound
	}
	return cloneEstimate(stored), nil
}

func (s *Store) LatestEstimateRevision(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, estimateID domain.CausalEstimateID) (domain.CausalEstimateRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	revision := s.causalEstimateLatest[tenantID][spaceID][estimateID]
	stored, ok := s.causalEstimates[tenantID][spaceID][estimateID][revision]
	if !ok {
		return domain.CausalEstimateRevision{}, errEstimateNotFound
	}
	return cloneEstimate(stored), nil
}

func (s *Store) AppendRewardRevision(ctx context.Context, revision domain.CausalRewardRevision) (domain.CausalRewardRevision, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byReward := ensure4(s.causalRewards, revision.TenantID, revision.SpaceID, revision.RewardID, revision.Revision)
	if existing, ok := byReward[revision.Revision]; ok {
		return reuse(existing, revision, cloneReward, errCausalRevision)
	}
	latest := s.causalRewardLatest[revision.TenantID][revision.SpaceID][revision.RewardID]
	if revision.Revision != latest+1 {
		return domain.CausalRewardRevision{}, false, errCausalRevision
	}
	if revision.PreviousHash != s.causalRewardHeads[revision.TenantID][revision.SpaceID][revision.RewardID] {
		return domain.CausalRewardRevision{}, false, errCausalChain
	}
	byReward[revision.Revision] = cloneReward(revision)
	ensure3(s.causalRewardLatest, revision.TenantID, revision.SpaceID, revision.RewardID)
	s.causalRewardLatest[revision.TenantID][revision.SpaceID][revision.RewardID] = revision.Revision
	setHead3(s.causalRewardHeads, revision.TenantID, revision.SpaceID, revision.RewardID, revision.PayloadHash)
	return cloneReward(revision), false, nil
}

func (s *Store) RewardRevision(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, rewardID domain.CausalRewardID, revision int64) (domain.CausalRewardRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.causalRewards[tenantID][spaceID][rewardID][revision]
	if !ok {
		return domain.CausalRewardRevision{}, errRewardNotFound
	}
	return cloneReward(stored), nil
}

func (s *Store) LatestRewardRevision(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, rewardID domain.CausalRewardID) (domain.CausalRewardRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	revision := s.causalRewardLatest[tenantID][spaceID][rewardID]
	stored, ok := s.causalRewards[tenantID][spaceID][rewardID][revision]
	if !ok {
		return domain.CausalRewardRevision{}, errRewardNotFound
	}
	return cloneReward(stored), nil
}

// DiveStore: the trajectory is derived from server-owned state (explicit
// recording first, then the session itself), and one terminal judgment is
// stored per submitted session.

func (s *Store) DiveTrajectory(ctx context.Context, tenantID domain.TenantID, sessionID domain.ExplorationSessionID) (domain.DiveTrajectory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if trajectory, ok := s.diveTrajectories[tenantID][sessionID]; ok {
		return cloneTrajectory(trajectory), nil
	}
	session, ok := s.sessions[sessionID]
	if !ok || session.TenantID != tenantID {
		return domain.DiveTrajectory{}, errSession
	}
	order := s.diveServedOrder[tenantID][sessionID]
	items := servedItemsInOrder(s.servedItems[sessionID], order)
	return domain.DiveTrajectory{
		SessionID: sessionID, TenantID: session.TenantID, PrincipalID: session.PrincipalID,
		SpaceIDs:       append([]domain.SpaceID(nil), session.SpaceIDs...),
		State:          session.State,
		TerminalReason: terminalReasonForState(session.State),
		Rounds:         session.StepsUsed,
		ServedItems:    items,
	}, nil
}

// RecordDiveTrajectory freezes the full trajectory of a submitted or
// otherwise terminal session before judging. Re-recording the identical
// trajectory is a no-op; a different trajectory for the same session fails
// closed.
func (s *Store) RecordDiveTrajectory(ctx context.Context, trajectory domain.DiveTrajectory) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bySession := ensure2(s.diveTrajectories, trajectory.TenantID, trajectory.SessionID)
	digest := digestJSON(trajectory)
	if existing, ok := bySession[trajectory.SessionID]; ok {
		if s.diveTrajectoryDigests[trajectory.TenantID][trajectory.SessionID] == digest {
			return nil
		}
		if reflect.DeepEqual(existing, trajectory) {
			return nil
		}
		return errDiveConflict
	}
	bySession[trajectory.SessionID] = cloneTrajectory(trajectory)
	setHead2(s.diveTrajectoryDigests, trajectory.TenantID, trajectory.SessionID, digest)
	return nil
}

func (s *Store) PutDiveResult(ctx context.Context, result domain.DiveResult) (domain.DiveResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bySession := ensure2(s.diveResults, result.TenantID, result.SessionID)
	if existing, ok := bySession[result.SessionID]; ok {
		return reuse(existing, result, cloneDiveResult, errDiveConflict)
	}
	bySession[result.SessionID] = cloneDiveResult(result)
	return cloneDiveResult(result), false, nil
}

func (s *Store) DiveResult(ctx context.Context, tenantID domain.TenantID, sessionID domain.ExplorationSessionID) (domain.DiveResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, ok := s.diveResults[tenantID][sessionID]
	if !ok {
		return domain.DiveResult{}, domain.NewProtocolError(404, "DIVE_NOT_FOUND", "dive result not found")
	}
	return cloneDiveResult(result), nil
}

func terminalReasonForState(state string) domain.DiveClassification {
	switch domain.DiveClassification(state) {
	case domain.DiveError, domain.DiveBudgetExhausted, domain.DiveTimeout:
		return domain.DiveClassification(state)
	}
	return ""
}

// servedItemsInOrder projects the served-citation map onto first-serve order;
// citations missing from the order list (restored from older snapshots) are
// appended in citation-ID order so the result is deterministic.
func servedItemsInOrder(served map[string]domain.RecallItem, order []string) []domain.RecallItem {
	items := make([]domain.RecallItem, 0, len(served))
	seen := make(map[string]bool, len(served))
	for _, citationID := range order {
		if item, ok := served[citationID]; ok && !seen[citationID] {
			seen[citationID] = true
			items = append(items, item)
		}
	}
	var rest []string
	for citationID := range served {
		if !seen[citationID] {
			rest = append(rest, citationID)
		}
	}
	sort.Strings(rest)
	for _, citationID := range rest {
		items = append(items, served[citationID])
	}
	return items
}

// noteServedCitation records the first-serve order of a citation; the caller
// holds the store mutex.
func (s *Store) noteServedCitation(tenantID domain.TenantID, sessionID domain.ExplorationSessionID, citationID string) {
	bySession := ensure2(s.diveServedOrder, tenantID, sessionID)
	order := bySession[sessionID]
	for _, existing := range order {
		if existing == citationID {
			return
		}
	}
	bySession[sessionID] = append(order, citationID)
}

// ConsolidationStore: the derived projection is published once per round
// under compare-and-swap on the head. A space with no published projection
// has the genesis head: version 0 with an empty digest.

func (s *Store) Activity(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID) (domain.ConsolidationActivity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if activity, ok := s.consolidationActivity[tenantID][spaceID]; ok {
		return activity, nil
	}
	return domain.ConsolidationActivity{SpaceID: spaceID}, nil
}

func (s *Store) ListProjectionSpaces(ctx context.Context) ([]domain.Space, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	spaces := make([]domain.Space, 0)
	for _, byID := range s.spaces {
		for _, space := range byID {
			spaces = append(spaces, space)
		}
	}
	sort.Slice(spaces, func(i, j int) bool {
		if spaces[i].TenantID != spaces[j].TenantID {
			return spaces[i].TenantID < spaces[j].TenantID
		}
		return spaces[i].ID < spaces[j].ID
	})
	return spaces, nil
}

func (s *Store) ProjectionBuildState(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID) (ports.ProjectionBuildState, error) {
	if err := ctx.Err(); err != nil {
		return ports.ProjectionBuildState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	space, ok := s.spaces[tenantID][spaceID]
	if !ok {
		return ports.ProjectionBuildState{}, domain.NewProtocolError(404, "SPACE_NOT_FOUND", "space not found")
	}
	head := domain.ProjectionHead{TenantID: tenantID, SpaceID: spaceID}
	if stored, ok := s.projectionHeads[tenantID][spaceID]; ok {
		head = stored
	}
	activity := domain.ConsolidationActivity{SpaceID: spaceID}
	if stored, ok := s.consolidationActivity[tenantID][spaceID]; ok {
		activity = stored
	}
	cursor := domain.ConsolidationCursor{SpaceID: spaceID}
	if stored, ok := s.consolidationCursors[tenantID][spaceID]; ok {
		cursor = stored
	}
	through := head.EvidenceWatermark
	for _, batchID := range s.publishedOrder[evidenceSpaceKey{tenantID: tenantID, spaceID: spaceID}] {
		batch, exists := s.batches[tenantID][batchID]
		if exists && batch.State == domain.EvidenceBatchCommitted && batch.MemoryVersion != nil && *batch.MemoryVersion > through {
			through = *batch.MemoryVersion
		}
	}
	return ports.ProjectionBuildState{
		Space: space, Head: head, Activity: activity, Cursor: cursor,
		EvidenceThrough: through,
		QueryThrough:    cursor.LastPublishedQueryOrdinal + activity.QueryOrdinal,
	}, nil
}

// bumpCommittedBatch advances the space's committed-batch ordinal exactly
// once per batch: Commit replays return before reaching here. The caller
// holds the store mutex.
func (s *Store) bumpCommittedBatch(tenantID domain.TenantID, spaceID domain.SpaceID) {
	bySpace := ensure2(s.consolidationActivity, tenantID, spaceID)
	activity := bySpace[spaceID]
	activity.SpaceID = spaceID
	activity.CommittedBatchOrdinal++
	bySpace[spaceID] = activity
}

func (s *Store) ProjectionHead(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID) (domain.ProjectionHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if head, ok := s.projectionHeads[tenantID][spaceID]; ok {
		return head, nil
	}
	return domain.ProjectionHead{TenantID: tenantID, SpaceID: spaceID}, nil
}

func (s *Store) Projection(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, version domain.ProjectionVersion) (domain.DerivedProjection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if projection, ok := s.projectionVersions[tenantID][spaceID][version]; ok {
		return cloneProjection(projection), nil
	}
	if version == 0 {
		return domain.DerivedProjection{
			TenantID: tenantID, SpaceID: spaceID,
			Nodes: []domain.ProjectionNode{}, Edges: []domain.ProjectionEdge{},
		}, nil
	}
	return domain.DerivedProjection{}, errProjectionMissing
}

func (s *Store) Round(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, roundID domain.ConsolidationRoundID) (domain.RoundResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, ok := s.consolidationRounds[tenantID][spaceID][roundID]
	if !ok {
		return domain.RoundResult{}, false, nil
	}
	return cloneRoundResult(result), true, nil
}

func (s *Store) PublishRound(ctx context.Context, input domain.ConsolidationPublishInput) (domain.RoundResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rounds := ensure3level2(s.consolidationRounds, input.TenantID, input.SpaceID)
	if existing, ok := rounds[input.Round.RoundID]; ok {
		return reuse(existing, input.Round, cloneRoundResult, errRoundConflict)
	}
	var current domain.ProjectionHead
	if stored, ok := s.projectionHeads[input.TenantID][input.SpaceID]; ok {
		current = stored
	} else {
		current = domain.ProjectionHead{TenantID: input.TenantID, SpaceID: input.SpaceID}
	}
	if current != input.ExpectedHead {
		return domain.RoundResult{}, false, errHeadMoved
	}
	if input.Projection.Version != current.Version+1 {
		return domain.RoundResult{}, false, errHeadMoved
	}

	rounds[input.Round.RoundID] = cloneRoundResult(input.Round)
	versions := ensure3(s.projectionVersions, input.TenantID, input.SpaceID, input.Projection.Version)
	versions[input.Projection.Version] = cloneProjection(input.Projection)
	heads := ensure2(s.projectionHeads, input.TenantID, input.SpaceID)
	heads[input.SpaceID] = domain.ProjectionHead{
		TenantID: input.TenantID, SpaceID: input.SpaceID,
		Version: input.Projection.Version, Digest: input.Projection.Digest,
		EvidenceWatermark: input.Projection.EvidenceWatermark,
	}
	cursors := ensure2(s.consolidationCursors, input.TenantID, input.SpaceID)
	previousCursor := cursors[input.SpaceID]
	cursors[input.SpaceID] = input.ActivityCursor
	activities := ensure2(s.consolidationActivity, input.TenantID, input.SpaceID)
	currentActivity := activities[input.SpaceID]
	consumedBatches := input.ActivityCursor.LastPublishedBatchOrdinal - previousCursor.LastPublishedBatchOrdinal
	consumedQueries := input.ActivityCursor.LastPublishedQueryOrdinal - previousCursor.LastPublishedQueryOrdinal
	if consumedBatches < 0 {
		consumedBatches = 0
	}
	if consumedQueries < 0 {
		consumedQueries = 0
	}
	currentActivity.SpaceID = input.SpaceID
	currentActivity.CommittedBatchOrdinal = max(0, currentActivity.CommittedBatchOrdinal-consumedBatches)
	currentActivity.QueryOrdinal = max(0, currentActivity.QueryOrdinal-consumedQueries)
	activities[input.SpaceID] = currentActivity
	return cloneRoundResult(input.Round), false, nil
}

// RecordRound durably records a terminal rejected round. Re-recording an
// identical verdict is a no-op; a different body under the same round ID is a
// conflict, never a silent overwrite.
func (s *Store) RecordRound(_ context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, round domain.RoundResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rounds := ensure3level2(s.consolidationRounds, tenantID, spaceID)
	if existing, ok := rounds[round.RoundID]; ok {
		if existing.Outcome == round.Outcome && existing.OperationDigest == round.OperationDigest {
			return nil
		}
		return errRoundConflict
	}
	rounds[round.RoundID] = cloneRoundResult(round)
	return nil
}

// RetrievalReplayStore: frozen inputs and audit outputs, idempotent on
// identical content.

func (s *Store) PutReplayPlan(ctx context.Context, plan domain.RetrievalReplayPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.retrievalReplayPlans[plan.ID]; ok {
		if reflect.DeepEqual(existing, plan) {
			return nil
		}
		return errReplayPlanConflict
	}
	s.retrievalReplayPlans[plan.ID] = cloneReplayPlan(plan)
	return nil
}

func (s *Store) PutReplayResult(ctx context.Context, roundID domain.ConsolidationRoundID, stats domain.CandidateStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.retrievalReplayResults[roundID]; ok {
		if reflect.DeepEqual(existing, stats) {
			return nil
		}
		return errReplayResultConf
	}
	s.retrievalReplayResults[roundID] = cloneCandidateStats(stats)
	return nil
}

// PatternStore: append-only pattern revisions, one per monotonic revision
// number.

func (s *Store) AppendPatternRevision(ctx context.Context, revision domain.PatternRevision, patternID string) (domain.PatternRevision, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	revisions := ensure4(s.patternRevisions, revision.TenantID, revision.SpaceID, patternID, revision.Revision)
	if existing, ok := revisions[revision.Revision]; ok {
		return reuse(existing, revision, clonePatternRevision, errPatternConflict)
	}
	latest := s.patternLatest[revision.TenantID][revision.SpaceID][patternID]
	if revision.Revision != latest+1 {
		return domain.PatternRevision{}, false, errPatternConflict
	}
	revisions[revision.Revision] = clonePatternRevision(revision)
	ensure3(s.patternLatest, revision.TenantID, revision.SpaceID, patternID)
	s.patternLatest[revision.TenantID][revision.SpaceID][patternID] = revision.Revision
	return clonePatternRevision(revision), false, nil
}

func (s *Store) LatestPatternRevision(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, patternID string) (domain.PatternRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	revision := s.patternLatest[tenantID][spaceID][patternID]
	stored, ok := s.patternRevisions[tenantID][spaceID][patternID][revision]
	if !ok {
		return domain.PatternRevision{}, errPatternMissing
	}
	return clonePatternRevision(stored), nil
}

// ProposalStore: one atomic outcome per round; fingerprints are content
// frozen for the space's lifetime.

func (s *Store) CompleteProposalRound(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, outcome domain.ProposalRoundOutcome) (domain.ProposalRoundOutcome, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rounds := ensure3level2(s.proposalRounds, tenantID, spaceID)
	if existing, ok := rounds[outcome.RoundID]; ok {
		if reflect.DeepEqual(existing, outcome) {
			return existing, true, nil
		}
		return domain.ProposalRoundOutcome{}, false, errConflict
	}
	if outcome.Proposal != nil {
		index := ensure3(s.proposalFingerprintIndex, tenantID, spaceID, outcome.Proposal.Fingerprint)
		if mapped, ok := index[outcome.Proposal.Fingerprint]; ok && mapped != outcome.Proposal.ProposalID {
			return domain.ProposalRoundOutcome{}, false, errFingerprintFrozen
		}
		index[outcome.Proposal.Fingerprint] = outcome.Proposal.ProposalID
		proposals := ensure3level2(s.proposals, tenantID, spaceID)
		proposals[outcome.Proposal.ProposalID] = cloneProposal(*outcome.Proposal)
	}
	rounds[outcome.RoundID] = outcome
	return outcome, false, nil
}

func (s *Store) ProposalByFingerprint(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, fingerprint domain.ProposalFingerprint) (domain.SkillProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	proposalID, ok := s.proposalFingerprintIndex[tenantID][spaceID][fingerprint]
	if !ok {
		return domain.SkillProposal{}, errProposalMissing
	}
	proposal, ok := s.proposals[tenantID][spaceID][proposalID]
	if !ok {
		return domain.SkillProposal{}, errProposalMissing
	}
	return cloneProposal(proposal), nil
}

// Proposal resolves one stored proposal by ID for the governed read route.
func (s *Store) Proposal(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, proposalID string) (domain.SkillProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	proposal, ok := s.proposals[tenantID][spaceID][proposalID]
	if !ok {
		return domain.SkillProposal{}, errProposalMissing
	}
	return cloneProposal(proposal), nil
}

func (s *Store) PutRejection(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, rejection domain.RejectionMemory) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := ensure3(s.rejectionMemory, tenantID, spaceID, rejection.Fingerprint)
	if existing, ok := index[rejection.Fingerprint]; ok {
		if reflect.DeepEqual(existing, rejection) {
			return false, nil
		}
		return false, errRejectionConflict
	}
	index[rejection.Fingerprint] = rejection
	return true, nil
}

func (s *Store) RejectionByFingerprint(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, fingerprint domain.ProposalFingerprint) (domain.RejectionMemory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rejection, ok := s.rejectionMemory[tenantID][spaceID][fingerprint]
	if !ok {
		return domain.RejectionMemory{}, errRejectionMissing
	}
	return rejection, nil
}

// CandidateStore: reviewed diffs, paired replay, one immutable decision per
// candidate, and version-CAS activation.

func (s *Store) PutCandidate(ctx context.Context, candidate domain.SkillCandidate) (domain.SkillCandidate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := ensure3level2(s.candidates, candidate.TenantID, candidate.SpaceID)
	if existing, ok := byID[candidate.CandidateID]; ok {
		return reuse(existing, candidate, cloneCandidate, errCandidateConflict)
	}
	if mapped, ok := s.candidateByProposal[candidate.ProposalID]; ok && mapped != candidate.CandidateID {
		return domain.SkillCandidate{}, false, errCandidateConflict
	}
	byID[candidate.CandidateID] = cloneCandidate(candidate)
	s.candidateByProposal[candidate.ProposalID] = candidate.CandidateID
	// A candidate's base artifact version is the active version at review
	// time; the first candidate for a skill seeds its activation baseline.
	if _, seeded := s.activeSkillVersions[candidate.TargetSkillID]; !seeded {
		s.activeSkillVersions[candidate.TargetSkillID] = candidate.BaseArtifactVersion
	}
	return cloneCandidate(candidate), false, nil
}

func (s *Store) Candidate(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, candidateID string) (domain.SkillCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate, ok := s.candidates[tenantID][spaceID][candidateID]
	if !ok {
		return domain.SkillCandidate{}, errCandidateMissing
	}
	return cloneCandidate(candidate), nil
}

func (s *Store) PutReviewedDiff(ctx context.Context, diff domain.ReviewedDiff) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.reviewedDiffs[diff.DiffID]; ok {
		if existing == diff {
			return false, nil
		}
		return false, errDiffConflict
	}
	s.reviewedDiffs[diff.DiffID] = diff
	return true, nil
}

func (s *Store) PutCandidateReplayPlan(ctx context.Context, plan domain.PairedReplayPlan) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.pairedReplayPlans[plan.ReplayID]; ok {
		if existing == plan {
			return false, nil
		}
		return false, errReplayPlanConflict
	}
	s.pairedReplayPlans[plan.ReplayID] = plan
	return true, nil
}

func (s *Store) AppendReplayTrial(ctx context.Context, trial domain.PairedReplayTrial) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byArm := ensure3(s.pairedReplayTrials, trial.ReplayID, trial.Arm, trial.Ordinal)
	if existing, ok := byArm[trial.Ordinal]; ok {
		if existing == trial {
			return false, nil
		}
		return false, errTrialKeyConflict
	}
	byArm[trial.Ordinal] = trial
	return true, nil
}

func (s *Store) PutCandidateReplayResult(ctx context.Context, result domain.PairedReplayResult) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.pairedReplayResults[result.ReplayID]; ok {
		if existing == result {
			return false, nil
		}
		return false, errReplayResultConf
	}
	s.pairedReplayResults[result.ReplayID] = result
	return true, nil
}

func (s *Store) DecideCandidate(ctx context.Context, decision domain.CandidateDecision) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.candidateDecisions[decision.CandidateID]; ok {
		if existing == decision {
			return false, nil
		}
		return false, errDecisionConflict
	}
	if candidate, ok := s.findCandidate(decision.CandidateID); ok {
		switch decision.Decision {
		case "accepted":
			candidate.Status = domain.CandidateAccepted
		case "rejected":
			candidate.Status = domain.CandidateRejected
		}
		s.candidates[candidate.TenantID][candidate.SpaceID][candidate.CandidateID] = candidate
	}
	s.candidateDecisions[decision.CandidateID] = decision
	return true, nil
}

func (s *Store) ActivateCandidate(ctx context.Context, activation domain.SkillActivation) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byActivation := ensure2(s.skillActivations, activation.CandidateID, activation.ActivationID)
	if existing, ok := byActivation[activation.ActivationID]; ok {
		if existing == activation {
			return false, nil
		}
		return false, errActivationConflict
	}
	candidate, ok := s.findCandidate(activation.CandidateID)
	if !ok {
		return false, errCandidateMissing
	}
	decision, decided := s.candidateDecisions[candidate.CandidateID]
	if !decided || decision.Decision != "accepted" {
		return false, errDecisionRequired
	}
	if current := s.activeSkillVersions[candidate.TargetSkillID]; current != activation.ExpectedBaseVersion {
		return false, errActivationConflict
	}
	s.activeSkillVersions[candidate.TargetSkillID] = activation.NewArtifactVersion
	byActivation[activation.ActivationID] = activation
	return true, nil
}

func (s *Store) PutActivationPolicyDecision(_ context.Context, decision domain.ActivationPolicyDecision) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if decision.Digest != domain.ActivationPolicyDecisionDigest(decision) {
		return false, errDecisionConflict
	}
	if existing, ok := s.activationPolicyDecisions[decision.DecisionID]; ok {
		if existing == decision {
			return false, nil
		}
		return false, errDecisionConflict
	}
	s.activationPolicyDecisions[decision.DecisionID] = decision
	return true, nil
}

func (s *Store) ActivationPolicyDecision(_ context.Context, _ domain.TenantID, _ domain.SpaceID, ref domain.ActivationPolicyDecisionRef) (domain.ActivationPolicyDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	decision, ok := s.activationPolicyDecisions[ref.DecisionID]
	if !ok || decision.Ref() != ref {
		return domain.ActivationPolicyDecision{}, errDecisionRequired
	}
	return decision, nil
}

func (s *Store) PutArmCEvaluation(_ context.Context, evaluation domain.ArmCEvaluation) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.armCEvaluations[evaluation.EvaluationID]; ok {
		if reflect.DeepEqual(existing, evaluation) {
			return false, nil
		}
		return false, errReplayResultConf
	}
	s.armCEvaluations[evaluation.EvaluationID] = evaluation
	return true, nil
}

func (s *Store) ArmCEvaluation(_ context.Context, _ domain.TenantID, _ domain.SpaceID, ref domain.ArmCEvaluationRef) (domain.ArmCEvaluation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evaluation, ok := s.armCEvaluations[ref.EvaluationID]
	if !ok || evaluation.Ref() != ref {
		return domain.ArmCEvaluation{}, errReplayResultConf
	}
	return evaluation, nil
}

func (s *Store) PutCoverageProof(_ context.Context, proof domain.CoverageProof) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.coverageProofs[proof.ProofID]; ok {
		if existing == proof {
			return false, nil
		}
		return false, errDecisionConflict
	}
	s.coverageProofs[proof.ProofID] = proof
	return true, nil
}

func (s *Store) CoverageProof(_ context.Context, _ domain.TenantID, _ domain.SpaceID, ref domain.CoverageProofRef) (domain.CoverageProof, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	proof, ok := s.coverageProofs[ref.ProofID]
	if !ok || proof.Ref() != ref {
		return domain.CoverageProof{}, errDecisionRequired
	}
	return proof, nil
}

// ActivatePolicyDecision performs the final atomic compare-and-swap against
// the immutable policy decision and active skill version. The service has
// already independently validated evidence; the store repeats binding and head
// checks under one mutex to close races before publishing activation.
func (s *Store) ActivatePolicyDecision(_ context.Context, activation domain.SkillActivation) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.skillActivations[activation.CandidateID][activation.ActivationID]; ok {
		if existing == activation {
			return false, nil
		}
		return false, errActivationConflict
	}
	decision, ok := s.activationPolicyDecisions[activation.DecisionRef.DecisionID]
	if !ok || decision.Ref() != activation.DecisionRef || decision.Outcome != domain.ActivationOutcomeActivate {
		return false, errDecisionRequired
	}
	candidate, ok := s.findCandidate(activation.CandidateID)
	if !ok || candidate.Diff.CandidateArtifactHash != activation.CandidateDigest || candidate.BaseArtifactVersion != activation.ExpectedBaseVersion {
		return false, errActivationConflict
	}
	evaluation, ok := s.armCEvaluations[activation.EvaluationRef.EvaluationID]
	if !ok || evaluation.Ref() != activation.EvaluationRef || !evaluation.Passed || evaluation.CandidateID != candidate.CandidateID || evaluation.CandidateDigest != activation.CandidateDigest {
		return false, errDecisionRequired
	}
	coverage, ok := s.coverageProofs[activation.CoverageRef.ProofID]
	if !ok || coverage.Ref() != activation.CoverageRef || coverage.ThresholdPolicyRef != activation.PolicyRef {
		return false, errDecisionRequired
	}
	if current := s.activeSkillVersions[candidate.TargetSkillID]; current != activation.ExpectedBaseVersion {
		return false, errActivationConflict
	}
	byActivation := ensure2(s.skillActivations, activation.CandidateID, activation.ActivationID)
	s.activeSkillVersions[candidate.TargetSkillID] = activation.NewArtifactVersion
	byActivation[activation.ActivationID] = activation
	return true, nil
}

// findCandidate resolves a candidate ID across spaces; the caller holds the
// store mutex.
func (s *Store) findCandidate(candidateID string) (domain.SkillCandidate, bool) {
	for _, bySpace := range s.candidates {
		for _, byID := range bySpace {
			if candidate, ok := byID[candidateID]; ok {
				return candidate, true
			}
		}
	}
	return domain.SkillCandidate{}, false
}

// Nested-map helpers. Each returns the innermost map, creating intermediate
// levels on demand; the caller holds the store mutex. The trailing key
// parameters document the level being addressed.

func ensure2[K1, K2 comparable, V any](outer map[K1]map[K2]V, k1 K1, _ K2) map[K2]V {
	inner, ok := outer[k1]
	if !ok {
		inner = make(map[K2]V)
		outer[k1] = inner
	}
	return inner
}

func ensure3[K1, K2, K3 comparable, V any](outer map[K1]map[K2]map[K3]V, k1 K1, k2 K2, _ K3) map[K3]V {
	middle, ok := outer[k1]
	if !ok {
		middle = make(map[K2]map[K3]V)
		outer[k1] = middle
	}
	inner, ok := middle[k2]
	if !ok {
		inner = make(map[K3]V)
		middle[k2] = inner
	}
	return inner
}

// ensure3level2 ensures the middle map of a three-level map of values (not
// maps), e.g. rounds[tenant][space] -> map[roundID]RoundResult.
func ensure3level2[K1, K2, K3 comparable, V any](outer map[K1]map[K2]map[K3]V, k1 K1, k2 K2) map[K3]V {
	middle, ok := outer[k1]
	if !ok {
		middle = make(map[K2]map[K3]V)
		outer[k1] = middle
	}
	inner, ok := middle[k2]
	if !ok {
		inner = make(map[K3]V)
		middle[k2] = inner
	}
	return inner
}

func ensure4[K1, K2, K3, K4 comparable, V any](outer map[K1]map[K2]map[K3]map[K4]V, k1 K1, k2 K2, k3 K3, _ K4) map[K4]V {
	middle, ok := outer[k1]
	if !ok {
		middle = make(map[K2]map[K3]map[K4]V)
		outer[k1] = middle
	}
	inner, ok := middle[k2]
	if !ok {
		inner = make(map[K3]map[K4]V)
		middle[k2] = inner
	}
	leaf, ok := inner[k3]
	if !ok {
		leaf = make(map[K4]V)
		inner[k3] = leaf
	}
	return leaf
}

func setHead2[K1, K2 comparable](heads map[K1]map[K2]string, k1 K1, k2 K2, value string) {
	inner, ok := heads[k1]
	if !ok {
		inner = make(map[K2]string)
		heads[k1] = inner
	}
	inner[k2] = value
}

func setHead3[K1, K2, K3 comparable](heads map[K1]map[K2]map[K3]string, k1 K1, k2 K2, k3 K3, value string) {
	middle, ok := heads[k1]
	if !ok {
		middle = make(map[K2]map[K3]string)
		heads[k1] = middle
	}
	inner, ok := middle[k2]
	if !ok {
		inner = make(map[K3]string)
		middle[k2] = inner
	}
	inner[k3] = value
}

// reuse resolves an idempotent replay: identical content returns the stored
// record as a duplicate, different content fails with the given conflict.
func reuse[T any, C func(T) T](stored, incoming T, clone C, conflict error) (T, bool, error) {
	if reflect.DeepEqual(stored, incoming) {
		return clone(stored), true, nil
	}
	var zero T
	return zero, false, conflict
}

func digestJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// Clone helpers keep stored records isolated from caller mutations on both
// write and read paths.

func cloneTrial(event domain.CausalTrialEvent) domain.CausalTrialEvent {
	cloned := event
	cloned.EvidenceRefs = cloneEvidenceRefs(event.EvidenceRefs)
	return cloned
}

func cloneEvidenceRefs(refs []domain.EvidenceRef) []domain.EvidenceRef {
	if refs == nil {
		return nil
	}
	cloned := make([]domain.EvidenceRef, len(refs))
	for index, ref := range refs {
		cloned[index] = ref
		cloned[index].EventIDs = append([]string(nil), ref.EventIDs...)
	}
	return cloned
}

func cloneEstimate(revision domain.CausalEstimateRevision) domain.CausalEstimateRevision {
	cloned := revision
	cloned.SourceTrialIDs = append([]domain.CausalTrialEventID(nil), revision.SourceTrialIDs...)
	return cloned
}

func cloneReward(revision domain.CausalRewardRevision) domain.CausalRewardRevision {
	cloned := revision
	cloned.RewardVectorJSON = append([]byte(nil), revision.RewardVectorJSON...)
	if revision.Scalar != nil {
		scalar := *revision.Scalar
		cloned.Scalar = &scalar
	}
	return cloned
}

func cloneTrajectory(trajectory domain.DiveTrajectory) domain.DiveTrajectory {
	cloned := trajectory
	cloned.SpaceIDs = append([]domain.SpaceID(nil), trajectory.SpaceIDs...)
	cloned.SubmittedCitationIDs = append([]string(nil), trajectory.SubmittedCitationIDs...)
	cloned.ServedItems = append([]domain.RecallItem(nil), trajectory.ServedItems...)
	return cloned
}

func cloneDiveResult(result domain.DiveResult) domain.DiveResult {
	cloned := result
	cloned.Items = append([]domain.DiveInformationItem(nil), result.Items...)
	return cloned
}

func cloneProjection(projection domain.DerivedProjection) domain.DerivedProjection {
	cloned := projection
	cloned.Nodes = make([]domain.ProjectionNode, len(projection.Nodes))
	for index, node := range projection.Nodes {
		cloned.Nodes[index] = node
		cloned.Nodes[index].EvidenceRefs = cloneEvidenceRefs(node.EvidenceRefs)
		cloned.Nodes[index].Tags = append([]string(nil), node.Tags...)
		cloned.Nodes[index].EntityRefs = append([]string(nil), node.EntityRefs...)
	}
	cloned.Edges = make([]domain.ProjectionEdge, len(projection.Edges))
	for index, edge := range projection.Edges {
		cloned.Edges[index] = edge
		cloned.Edges[index].EvidenceRefs = cloneEvidenceRefs(edge.EvidenceRefs)
	}
	return cloned
}

func cloneRoundResult(result domain.RoundResult) domain.RoundResult {
	cloned := result
	if result.PublishedVersion != nil {
		version := *result.PublishedVersion
		cloned.PublishedVersion = &version
	}
	cloned.Rejections = append([]domain.OperationRejection(nil), result.Rejections...)
	cloned.Backtest = cloneCandidateStats(result.Backtest)
	return cloned
}

func cloneCandidateStats(stats domain.CandidateStats) domain.CandidateStats {
	cloned := stats
	cloned.GateFailures = append([]string(nil), stats.GateFailures...)
	if stats.Queries == nil {
		cloned.Queries = nil
		return cloned
	}
	cloned.Queries = make([]domain.QueryBacktestStat, len(stats.Queries))
	for index, query := range stats.Queries {
		cloned.Queries[index] = query
		cloned.Queries[index].ItemMisses = append([]string(nil), query.ItemMisses...)
	}
	return cloned
}

func cloneReplayPlan(plan domain.RetrievalReplayPlan) domain.RetrievalReplayPlan {
	cloned := plan
	cloned.Cases = make([]domain.RetrievalReplayCase, len(plan.Cases))
	for index, replayCase := range plan.Cases {
		cloned.Cases[index] = replayCase
		cloned.Cases[index].Items = make([]domain.BacktestItem, len(replayCase.Items))
		for itemIndex, item := range replayCase.Items {
			cloned.Cases[index].Items[itemIndex] = item
			cloned.Cases[index].Items[itemIndex].NodeIDs = append([]domain.ProjectionNodeID(nil), item.NodeIDs...)
			cloned.Cases[index].Items[itemIndex].EvidenceRefs = cloneEvidenceRefs(item.EvidenceRefs)
		}
	}
	return cloned
}

func clonePatternRevision(revision domain.PatternRevision) domain.PatternRevision {
	cloned := revision
	cloned.Evidence = make([]domain.PatternEvidence, len(revision.Evidence))
	for index, evidence := range revision.Evidence {
		cloned.Evidence[index] = evidence
		cloned.Evidence[index].EvidenceRef.EventIDs = append([]string(nil), evidence.EvidenceRef.EventIDs...)
		if evidence.CausalEstimate != nil {
			ref := *evidence.CausalEstimate
			cloned.Evidence[index].CausalEstimate = &ref
		}
	}
	return cloned
}

func cloneProposal(proposal domain.SkillProposal) domain.SkillProposal {
	cloned := proposal
	cloned.PatternRefs = append([]domain.PatternRef(nil), proposal.PatternRefs...)
	return cloned
}

func cloneCandidate(candidate domain.SkillCandidate) domain.SkillCandidate {
	cloned := candidate
	cloned.PatternRefs = append([]domain.PatternRef(nil), candidate.PatternRefs...)
	return cloned
}

// RegisterArmCCandidate stores the immutable GMS-202 candidate binding used
// exclusively by the direct Arm C path. It never creates a legacy SkillCandidate.
func (s *Store) RegisterArmCCandidate(_ context.Context, registration domain.ArmCCandidateRegistration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if registration.TenantID == "" || registration.SpaceID == "" || registration.TargetSkillID == "" || registration.ExpectedActiveVersion < 0 || registration.CandidateRef.CandidateID == "" || registration.CandidateRef.BodyDigest == "" {
		return false, errCandidateConflict
	}
	byID := ensure3level2(s.armCCandidates, registration.TenantID, registration.SpaceID)
	if existing, ok := byID[registration.CandidateRef.CandidateID]; ok {
		if existing == registration {
			return false, nil
		}
		return false, errCandidateConflict
	}
	if current, exists := s.activeSkillVersions[registration.TargetSkillID]; !exists {
		// A new lineage has one explicit authoritative creation baseline: zero.
		// A candidate cannot invent an arbitrary expected head for an unknown target.
		if registration.ExpectedActiveVersion != 0 {
			return false, errActivationConflict
		}
	} else if current != registration.ExpectedActiveVersion {
		return false, errActivationConflict
	}
	byID[registration.CandidateRef.CandidateID] = registration
	return true, nil
}

func (s *Store) ArmCCandidate(_ context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, candidateID string) (domain.ArmCCandidateRegistration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	registration, ok := s.armCCandidates[tenantID][spaceID][candidateID]
	if !ok {
		return domain.ArmCCandidateRegistration{}, errCandidateMissing
	}
	return registration, nil
}

// ActivateArmCCandidatePolicyDecision repeats the direct registration,
// decision, evaluation, coverage, and expected-head checks under one mutex.
func (s *Store) ActivateArmCCandidatePolicyDecision(_ context.Context, registration domain.ArmCCandidateRegistration, activation domain.SkillActivation) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.armCCandidates[registration.TenantID][registration.SpaceID][registration.CandidateRef.CandidateID]
	if !ok || stored != registration || activation.CandidateID != registration.CandidateRef.CandidateID || activation.CandidateDigest != registration.CandidateRef.BodyDigest || activation.ExpectedBaseVersion != registration.ExpectedActiveVersion {
		return false, errActivationConflict
	}
	if existing, ok := s.skillActivations[activation.CandidateID][activation.ActivationID]; ok {
		if existing == activation {
			return false, nil
		}
		return false, errActivationConflict
	}
	decision, ok := s.activationPolicyDecisions[activation.DecisionRef.DecisionID]
	if !ok || decision.Ref() != activation.DecisionRef || decision.Outcome != domain.ActivationOutcomeActivate {
		return false, errDecisionRequired
	}
	evaluation, ok := s.armCEvaluations[activation.EvaluationRef.EvaluationID]
	if !ok || evaluation.Ref() != activation.EvaluationRef || !evaluation.Passed || evaluation.CandidateID != registration.CandidateRef.CandidateID || evaluation.CandidateDigest != registration.CandidateRef.BodyDigest {
		return false, errDecisionRequired
	}
	coverage, ok := s.coverageProofs[activation.CoverageRef.ProofID]
	if !ok || coverage.Ref() != activation.CoverageRef || coverage.ThresholdPolicyRef != activation.PolicyRef {
		return false, errDecisionRequired
	}
	if s.activeSkillVersions[registration.TargetSkillID] != registration.ExpectedActiveVersion {
		return false, errActivationConflict
	}
	byActivation := ensure2(s.skillActivations, activation.CandidateID, activation.ActivationID)
	s.activeSkillVersions[registration.TargetSkillID] = activation.NewArtifactVersion
	byActivation[activation.ActivationID] = activation
	return true, nil
}

// CandidateOutcomes reads only authoritative direct Arm C registrations,
// policy decisions, and activation records. Absence is reported explicitly;
// callers must never infer a lifecycle conclusion from usage data.
func (s *Store) CandidateOutcomes(_ context.Context, refs []contract.CandidateArtifactRef) ([]domain.CandidateLifecycleOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.CandidateLifecycleOutcome, 0, len(refs))
	for _, ref := range refs {
		outcome := domain.CandidateLifecycleOutcome{CandidateRef: ref, Status: "no_evidence"}
		var registration domain.ArmCCandidateRegistration
		found := false
		for _, bySpace := range s.armCCandidates {
			for _, byID := range bySpace {
				if value, ok := byID[ref.CandidateID]; ok && value.CandidateRef == ref {
					registration, found = value, true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			out = append(out, outcome)
			continue
		}
		outcome.Status = "pending"
		if activations, ok := s.skillActivations[ref.CandidateID]; ok {
			activationIDs := make([]string, 0, len(activations))
			for activationID := range activations { activationIDs = append(activationIDs, activationID) }
			sort.Strings(activationIDs)
			for _, activationID := range activationIDs {
				activation := activations[activationID]
				if activation.CandidateDigest != ref.BodyDigest { continue }
				if decision, ok := s.activationPolicyDecisions[activation.DecisionID]; ok && decision.Ref() == activation.DecisionRef && decision.Outcome == domain.ActivationOutcomeActivate {
					decisionRef := domain.VersionedArtifactRef{ID: decision.DecisionID, Version: decision.Version, Digest: decision.Digest}
					outcome.Status, outcome.DecisionRef = "activated", &decisionRef
					break
				}
			}
		}
		if outcome.Status != "activated" {
			decisionIDs := make([]string, 0)
			for decisionID, decision := range s.activationPolicyDecisions { if decision.CandidateID == ref.CandidateID && decision.CandidateDigest == ref.BodyDigest { decisionIDs = append(decisionIDs, decisionID) } }
			sort.Strings(decisionIDs)
			for _, decisionID := range decisionIDs {
				decision := s.activationPolicyDecisions[decisionID]
				decisionRef := domain.VersionedArtifactRef{ID: decision.DecisionID, Version: decision.Version, Digest: decision.Digest}
				outcome.DecisionRef = &decisionRef
				if decision.Outcome == domain.ActivationOutcomeReject { outcome.Status, outcome.Reason = "rejected", decision.Reason; break }
			}
		}
		_ = registration
		out = append(out, outcome)
	}
	return out, nil
}
