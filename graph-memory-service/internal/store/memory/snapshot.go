package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

// snapshot is the full durable image of the store. It exists so a single-node
// deployment can survive restarts with the tracer's ADR 0040 simplification:
// one JSON file, written atomically after every mutation, reloaded at boot.
// Struct-keyed maps (stageKey, sessionKey) are serialized as slices because
// encoding/json only accepts string-kinded map keys.
type snapshot struct {
	Tenants    map[domain.TenantID]domain.Tenant                           `json:"tenants"`
	Principals map[domain.TenantID]map[domain.PrincipalID]domain.Principal `json:"principals"`
	Spaces     map[domain.TenantID]map[domain.SpaceID]domain.Space         `json:"spaces"`
	Grants     map[domain.TenantID]map[domain.GrantID]domain.Grant         `json:"grants"`

	Batches        map[domain.TenantID]map[domain.BatchID]domain.EvidenceBatch `json:"batches"`
	StageKeys      []snapshotStageKey                                          `json:"stage_keys"`
	PublishedOrder snapshotPublishedOrders                                     `json:"published_order"`
	BatchVersion   map[domain.BatchID]int64                                    `json:"batch_version"`

	Sessions       map[domain.ExplorationSessionID]domain.ExplorationSession             `json:"sessions"`
	SessionByKey   []snapshotSessionKey                                                  `json:"session_by_key"`
	Operations     map[domain.ExplorationSessionID]map[string]storedOperation            `json:"operations"`
	ServedItems    map[domain.ExplorationSessionID]map[string]domain.RecallItem          `json:"served_items"`
	NavigationRuns map[domain.ExplorationSessionID]map[string]ports.NavigationRunJournal `json:"navigation_runs"`

	// Curation durable state (M1–M4). Domain records serialize with their Go
	// field names; only these top-level keys are part of the snapshot wire
	// contract, and every field must render as a JSON object, never null,
	// even when empty — a tracer-era snapshot restores into zeroed maps.
	CausalTrials         map[domain.TenantID]map[domain.SpaceID]map[domain.CausalTrialEventID]domain.CausalTrialEvent               `json:"causal_trials"`
	CausalTrialOrder     map[domain.TenantID]map[domain.SpaceID][]domain.CausalTrialEventID                                         `json:"causal_trial_order"`
	CausalTrialHeads     map[domain.TenantID]map[domain.SpaceID]string                                                              `json:"causal_trial_heads"`
	CausalEstimates      map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]map[int64]domain.CausalEstimateRevision `json:"causal_estimates"`
	CausalEstimateLatest map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]int64                                   `json:"causal_estimate_latest"`
	CausalEstimateHeads  map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]string                                  `json:"causal_estimate_heads"`
	CausalRewards        map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]map[int64]domain.CausalRewardRevision     `json:"causal_rewards"`
	CausalRewardLatest   map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]int64                                     `json:"causal_reward_latest"`
	CausalRewardHeads    map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]string                                    `json:"causal_reward_heads"`

	DiveTrajectories      map[domain.TenantID]map[domain.ExplorationSessionID]domain.DiveTrajectory `json:"dive_trajectories"`
	DiveServedOrder       map[domain.TenantID]map[domain.ExplorationSessionID][]string              `json:"dive_served_order"`
	DiveResults           map[domain.TenantID]map[domain.ExplorationSessionID]domain.DiveResult     `json:"dive_results"`
	DiveTrajectoryDigests map[domain.TenantID]map[domain.ExplorationSessionID]string                `json:"dive_trajectory_digests"`

	ConsolidationActivity  map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationActivity                          `json:"consolidation_activity"`
	ConsolidationCursors   map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationCursor                            `json:"consolidation_cursors"`
	ProjectionHeads        map[domain.TenantID]map[domain.SpaceID]domain.ProjectionHead                                 `json:"projection_heads"`
	ProjectionVersions     map[domain.TenantID]map[domain.SpaceID]map[domain.ProjectionVersion]domain.DerivedProjection `json:"projection_versions"`
	ConsolidationRounds    map[domain.TenantID]map[domain.SpaceID]map[domain.ConsolidationRoundID]domain.RoundResult    `json:"consolidation_rounds"`
	RetrievalReplayPlans   map[string]domain.RetrievalReplayPlan                                                        `json:"retrieval_replay_plans"`
	RetrievalReplayResults map[domain.ConsolidationRoundID]domain.CandidateStats                                        `json:"retrieval_replay_results"`

	PatternRevisions          map[domain.TenantID]map[domain.SpaceID]map[string]map[int64]domain.PatternRevision           `json:"pattern_revisions"`
	PatternLatest             map[domain.TenantID]map[domain.SpaceID]map[string]int64                                      `json:"pattern_latest"`
	ProposalRounds            map[domain.TenantID]map[domain.SpaceID]map[string]domain.ProposalRoundOutcome                `json:"proposal_rounds"`
	Proposals                 map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillProposal                       `json:"proposals"`
	ProposalFingerprintIndex  map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]string                 `json:"proposal_fingerprint_index"`
	RejectionMemory           map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]domain.RejectionMemory `json:"rejection_memory"`
	ReviewedDiffs             map[string]domain.ReviewedDiff                                                               `json:"reviewed_diffs"`
	Candidates                map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillCandidate                      `json:"candidates"`
	ArmCCandidates            map[domain.TenantID]map[domain.SpaceID]map[string]domain.ArmCCandidateRegistration           `json:"arm_c_candidates"`
	CandidateByProposal       map[string]string                                                                            `json:"candidate_by_proposal"`
	PairedReplayPlans         map[string]domain.PairedReplayPlan                                                           `json:"paired_replay_plans"`
	PairedReplayTrials        map[string]map[domain.ReplayArm]map[int]domain.PairedReplayTrial                             `json:"paired_replay_trials"`
	PairedReplayResults       map[string]domain.PairedReplayResult                                                         `json:"paired_replay_results"`
	MutationBacktestResults   map[string]any                                                                               `json:"mutation_backtest_results"`
	CandidateDecisions        map[string]domain.CandidateDecision                                                          `json:"candidate_decisions"`
	ActivationPolicyDecisions map[string]domain.ActivationPolicyDecision                                                   `json:"activation_policy_decisions"`
	ArmCEvaluations           map[string]domain.ArmCEvaluation                                                             `json:"arm_c_evaluations"`
	CoverageProofs            map[string]domain.CoverageProof                                                              `json:"coverage_proofs"`
	SkillActivations          map[string]map[string]domain.SkillActivation                                                 `json:"skill_activations"`
	ActiveSkillVersions       map[string]int64                                                                             `json:"active_skill_versions"`
}

type snapshotPublishedOrder struct {
	Tenant   domain.TenantID  `json:"tenant"`
	SpaceID  domain.SpaceID   `json:"space_id"`
	BatchIDs []domain.BatchID `json:"batch_ids"`
}

type snapshotPublishedOrders []snapshotPublishedOrder

// UnmarshalJSON accepts both the tenant-aware array and the legacy
// space-keyed object used by single-tenant snapshots.
func (orders *snapshotPublishedOrders) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*orders = snapshotPublishedOrders{}
		return nil
	}
	if trimmed[0] == '{' {
		var legacy map[domain.SpaceID][]domain.BatchID
		if err := json.Unmarshal(trimmed, &legacy); err != nil {
			return err
		}
		converted := make(snapshotPublishedOrders, 0, len(legacy))
		for spaceID, batchIDs := range legacy {
			converted = append(converted, snapshotPublishedOrder{SpaceID: spaceID, BatchIDs: batchIDs})
		}
		*orders = converted
		return nil
	}
	type plain snapshotPublishedOrders
	var decoded plain
	if err := json.Unmarshal(trimmed, &decoded); err != nil {
		return err
	}
	*orders = snapshotPublishedOrders(decoded)
	return nil
}

type snapshotStageKey struct {
	Tenant  domain.TenantID `json:"tenant"`
	SpaceID domain.SpaceID  `json:"space_id"`
	Key     string          `json:"key"`
	BatchID domain.BatchID  `json:"batch_id"`
}

type snapshotSessionKey struct {
	Tenant      domain.TenantID             `json:"tenant"`
	Principal   domain.PrincipalID          `json:"principal"`
	Idempotency string                      `json:"idempotency"`
	SessionID   domain.ExplorationSessionID `json:"session_id"`
}

// Snapshot returns the authoritative JSON image of the store under one lock.
// Retrieval BM25 indexes, embedding caches, and other process-local derived
// accelerators live outside Store and are deliberately not serialized.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.toSnapshot())
}

// Restore replaces the store contents with a snapshot image.
func (s *Store) Restore(image []byte) error {
	var data snapshot
	if err := json.Unmarshal(image, &data); err != nil {
		return fmt.Errorf("snapshot is not valid store JSON: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tenants = orEmptyTenants(data.Tenants)
	legacyUnsafeOperations := data.Operations == nil || hasIncompleteOperations(data.Operations)
	s.principals = orEmptyMap(data.Principals)
	s.spaces = orEmptyMap(data.Spaces)
	s.grants = orEmptyMap(data.Grants)
	s.batches = orEmptyMap(data.Batches)
	s.stageKeys = make(map[domain.TenantID]map[stageKey]domain.BatchID, len(data.StageKeys))
	for _, entry := range data.StageKeys {
		byTenant, ok := s.stageKeys[entry.Tenant]
		if !ok {
			byTenant = make(map[stageKey]domain.BatchID)
			s.stageKeys[entry.Tenant] = byTenant
		}
		byTenant[stageKey{spaceID: entry.SpaceID, key: entry.Key}] = entry.BatchID
	}
	s.publishedOrder = make(map[evidenceSpaceKey][]domain.BatchID, len(data.PublishedOrder))
	for _, entry := range data.PublishedOrder {
		tenantID := entry.Tenant
		if tenantID == "" {
			tenantID = snapshotOrderTenant(s.batches, entry.SpaceID, entry.BatchIDs)
		}
		if tenantID == "" {
			// Dropping the entry would silently reassign that Space's
			// memory versions on the next commit, so restoration fails closed.
			return fmt.Errorf("snapshot published order for space %s has an unresolvable tenant", entry.SpaceID)
		}
		key := evidenceSpaceKey{tenantID: tenantID, spaceID: entry.SpaceID}
		s.publishedOrder[key] = append(s.publishedOrder[key], entry.BatchIDs...)
	}
	s.batchVersion = orEmptyMap(data.BatchVersion)
	s.sessions = orEmptyMap(data.Sessions)
	if legacyUnsafeOperations {
		// A legacy image has no complete durable operation journal, so an active
		// trajectory cannot safely distinguish a replay from a new mutation.
		// Expire it fail-closed while preserving terminal sessions for audit.
		for sessionID, session := range s.sessions {
			if session.State == "active" {
				session.ExpiresAt = time.Time{}
				s.sessions[sessionID] = session
			}
		}
	}
	s.sessionByKey = make(map[sessionKey]domain.ExplorationSessionID, len(data.SessionByKey))
	for _, entry := range data.SessionByKey {
		s.sessionByKey[sessionKey{tenant: entry.Tenant, principal: entry.Principal, idempotency: entry.Idempotency}] = entry.SessionID
	}
	s.operations = orEmptyMap(data.Operations)
	s.servedItems = orEmptyMap(data.ServedItems)
	s.navigationRuns = orEmptyMap(data.NavigationRuns)

	// Curation state always materializes as objects: a tracer-era snapshot
	// (before M1–M4) restores into initialized empty maps, never nil ones.
	s.causalTrials = orEmptyMap(data.CausalTrials)
	s.causalTrialOrder = orEmptyMap(data.CausalTrialOrder)
	s.causalTrialHeads = orEmptyMap(data.CausalTrialHeads)
	s.causalEstimates = orEmptyMap(data.CausalEstimates)
	s.causalEstimateLatest = orEmptyMap(data.CausalEstimateLatest)
	s.causalEstimateHeads = orEmptyMap(data.CausalEstimateHeads)
	s.causalRewards = orEmptyMap(data.CausalRewards)
	s.causalRewardLatest = orEmptyMap(data.CausalRewardLatest)
	s.causalRewardHeads = orEmptyMap(data.CausalRewardHeads)

	s.diveTrajectories = orEmptyMap(data.DiveTrajectories)
	s.diveServedOrder = orEmptyMap(data.DiveServedOrder)
	s.diveResults = orEmptyMap(data.DiveResults)
	s.diveTrajectoryDigests = orEmptyMap(data.DiveTrajectoryDigests)

	s.consolidationActivity = orEmptyMap(data.ConsolidationActivity)
	s.consolidationCursors = orEmptyMap(data.ConsolidationCursors)
	s.projectionHeads = orEmptyMap(data.ProjectionHeads)
	s.projectionVersions = orEmptyMap(data.ProjectionVersions)
	s.consolidationRounds = orEmptyMap(data.ConsolidationRounds)
	s.retrievalReplayPlans = orEmptyMap(data.RetrievalReplayPlans)
	s.retrievalReplayResults = orEmptyMap(data.RetrievalReplayResults)

	s.patternRevisions = orEmptyMap(data.PatternRevisions)
	s.patternLatest = orEmptyMap(data.PatternLatest)
	s.proposalRounds = orEmptyMap(data.ProposalRounds)
	s.proposals = orEmptyMap(data.Proposals)
	s.proposalFingerprintIndex = orEmptyMap(data.ProposalFingerprintIndex)
	s.rejectionMemory = orEmptyMap(data.RejectionMemory)
	s.reviewedDiffs = orEmptyMap(data.ReviewedDiffs)
	s.candidates = orEmptyMap(data.Candidates)
	s.armCCandidates = orEmptyMap(data.ArmCCandidates)
	s.candidateByProposal = orEmptyMap(data.CandidateByProposal)
	s.pairedReplayPlans = orEmptyMap(data.PairedReplayPlans)
	s.pairedReplayTrials = orEmptyMap(data.PairedReplayTrials)
	s.pairedReplayResults = orEmptyMap(data.PairedReplayResults)
	s.mutationBacktestResults = orEmptyMap(data.MutationBacktestResults)
	s.candidateDecisions = orEmptyMap(data.CandidateDecisions)
	s.activationPolicyDecisions = orEmptyMap(data.ActivationPolicyDecisions)
	s.armCEvaluations = orEmptyMap(data.ArmCEvaluations)
	s.coverageProofs = orEmptyMap(data.CoverageProofs)
	s.skillActivations = orEmptyMap(data.SkillActivations)
	s.activeSkillVersions = orEmptyMap(data.ActiveSkillVersions)
	return nil
}

// PersistToFile writes the snapshot atomically: a temp file in the same
// directory, fsynced, then renamed over the target so a crash mid-write can
// never leave a torn image.
func (s *Store) PersistToFile(path string) error {
	image, err := s.Snapshot()
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	if _, err := temp.Write(image); err != nil {
		temp.Close()
		os.Remove(tempName)
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		os.Remove(tempName)
		return err
	}
	if err := temp.Close(); err != nil {
		os.Remove(tempName)
		return err
	}
	return os.Rename(tempName, path)
}

// LoadFromFile restores from a snapshot file. A missing file means a first
// boot and leaves the store empty; anything else (unreadable, corrupt) fails.
func (s *Store) LoadFromFile(path string) error {
	image, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.Restore(image)
}

// Binding reports the deployment binding (tenant plus bootstrap principal) so
// a server restarting from disk can re-establish its token binding.
func (s *Store) Binding() (domain.TenantID, domain.PrincipalID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tenants) == 0 {
		return "", "", false
	}
	// InitializeTenant enforces exactly one tenant per instance.
	for _, tenant := range s.tenants {
		return tenant.ID, tenant.BootstrapPrincipalID, true
	}
	return "", "", false
}

func (s *Store) toSnapshot() *snapshot {
	data := &snapshot{
		Tenants:        s.tenants,
		Principals:     s.principals,
		Spaces:         s.spaces,
		Grants:         s.grants,
		Batches:        s.batches,
		PublishedOrder: snapshotPublishedOrderEntries(s.publishedOrder),
		BatchVersion:   s.batchVersion,
		Sessions:       s.sessions,
		Operations:     s.operations,
		ServedItems:    s.servedItems,
		NavigationRuns: s.navigationRuns,

		CausalTrials:         s.causalTrials,
		CausalTrialOrder:     s.causalTrialOrder,
		CausalTrialHeads:     s.causalTrialHeads,
		CausalEstimates:      s.causalEstimates,
		CausalEstimateLatest: s.causalEstimateLatest,
		CausalEstimateHeads:  s.causalEstimateHeads,
		CausalRewards:        s.causalRewards,
		CausalRewardLatest:   s.causalRewardLatest,
		CausalRewardHeads:    s.causalRewardHeads,

		DiveTrajectories:      s.diveTrajectories,
		DiveServedOrder:       s.diveServedOrder,
		DiveResults:           s.diveResults,
		DiveTrajectoryDigests: s.diveTrajectoryDigests,

		ConsolidationActivity:  s.consolidationActivity,
		ConsolidationCursors:   s.consolidationCursors,
		ProjectionHeads:        s.projectionHeads,
		ProjectionVersions:     s.projectionVersions,
		ConsolidationRounds:    s.consolidationRounds,
		RetrievalReplayPlans:   s.retrievalReplayPlans,
		RetrievalReplayResults: s.retrievalReplayResults,

		PatternRevisions:          s.patternRevisions,
		PatternLatest:             s.patternLatest,
		ProposalRounds:            s.proposalRounds,
		Proposals:                 s.proposals,
		ProposalFingerprintIndex:  s.proposalFingerprintIndex,
		RejectionMemory:           s.rejectionMemory,
		ReviewedDiffs:             s.reviewedDiffs,
		Candidates:                s.candidates,
		ArmCCandidates:            s.armCCandidates,
		CandidateByProposal:       s.candidateByProposal,
		PairedReplayPlans:         s.pairedReplayPlans,
		PairedReplayTrials:        s.pairedReplayTrials,
		PairedReplayResults:       s.pairedReplayResults,
		MutationBacktestResults:   s.mutationBacktestResults,
		CandidateDecisions:        s.candidateDecisions,
		ActivationPolicyDecisions: s.activationPolicyDecisions,
		ArmCEvaluations:           s.armCEvaluations,
		CoverageProofs:            s.coverageProofs,
		SkillActivations:          s.skillActivations,
		ActiveSkillVersions:       s.activeSkillVersions,
	}
	data.StageKeys = make([]snapshotStageKey, 0)
	for tenant, byKey := range s.stageKeys {
		for key, batchID := range byKey {
			data.StageKeys = append(data.StageKeys, snapshotStageKey{Tenant: tenant, SpaceID: key.spaceID, Key: key.key, BatchID: batchID})
		}
	}
	data.SessionByKey = make([]snapshotSessionKey, 0)
	for key, sessionID := range s.sessionByKey {
		data.SessionByKey = append(data.SessionByKey, snapshotSessionKey{
			Tenant: key.tenant, Principal: key.principal, Idempotency: key.idempotency, SessionID: sessionID,
		})
	}
	return data
}

func hasIncompleteOperations(operations map[domain.ExplorationSessionID]map[string]storedOperation) bool {
	for _, byID := range operations {
		for _, operation := range byID {
			if len(operation.RequestJSON) == 0 || len(operation.Response) == 0 {
				return true
			}
		}
	}
	return false
}

func orEmptyTenants(tenants map[domain.TenantID]domain.Tenant) map[domain.TenantID]domain.Tenant {
	if tenants == nil {
		return make(map[domain.TenantID]domain.Tenant)
	}
	return tenants
}

// orEmptyMap guarantees restored curation state is always a usable map so
// snapshots never render a curation field as null.
func orEmptyMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return make(map[K]V)
	}
	return m
}

func snapshotOrderTenant(batches map[domain.TenantID]map[domain.BatchID]domain.EvidenceBatch, spaceID domain.SpaceID, batchIDs []domain.BatchID) domain.TenantID {
	for _, batchID := range batchIDs {
		for tenantID, byID := range batches {
			if batch, ok := byID[batchID]; ok && batch.SpaceID == spaceID {
				return tenantID
			}
		}
	}
	return ""
}

func snapshotPublishedOrderEntries(orders map[evidenceSpaceKey][]domain.BatchID) snapshotPublishedOrders {
	entries := make(snapshotPublishedOrders, 0, len(orders))
	for key, batchIDs := range orders {
		entries = append(entries, snapshotPublishedOrder{
			Tenant: key.tenantID, SpaceID: key.spaceID, BatchIDs: append([]domain.BatchID(nil), batchIDs...),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Tenant != entries[j].Tenant {
			return entries[i].Tenant < entries[j].Tenant
		}
		return entries[i].SpaceID < entries[j].SpaceID
	})
	return entries
}
