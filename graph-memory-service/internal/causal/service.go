package causal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

var (
	ErrNotImplemented           = errors.New("causal evidence ledger is not implemented")
	ErrInvalidWitnessTransition = errors.New("invalid witness transition")
)

// Service appends hash-chained causal records behind the curation purpose
// fence. The agent tool plane can never write causal conclusions (ADR 0038);
// authorization is enforced server-side before the store is touched.
type Service struct {
	authorizer *authz.Authorizer
	store      ports.CausalEvidenceStore
	clock      ports.Clock
}

func New(registry ports.RegistryStore, store ports.CausalEvidenceStore, clock ports.Clock) *Service {
	return &Service{authorizer: authz.NewAuthorizer(registry, clock), store: store, clock: clock}
}

func (s *Service) AppendTrial(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, event domain.CausalTrialEvent) (domain.CausalTrialEvent, bool, error) {
	if err := s.authorize(ctx, tenantID, principalID, []domain.SpaceID{event.SpaceID}, domain.GrantOperationCausalTrialAppend); err != nil {
		return domain.CausalTrialEvent{}, false, err
	}
	event.TenantID = tenantID
	hash, err := TrialPayloadHash(event)
	if err != nil {
		return domain.CausalTrialEvent{}, false, err
	}
	event.PayloadHash = hash
	return s.store.AppendTrial(ctx, event)
}

func (s *Service) AppendEstimateRevision(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, revision domain.CausalEstimateRevision) (domain.CausalEstimateRevision, bool, error) {
	spaceIDs := []domain.SpaceID{revision.SpaceID}
	if err := s.authorize(ctx, tenantID, principalID, spaceIDs, domain.GrantOperationCausalEstimateAppend); err != nil {
		return domain.CausalEstimateRevision{}, false, err
	}
	revision.TenantID = tenantID
	hash, err := EstimatePayloadHash(revision)
	if err != nil {
		return domain.CausalEstimateRevision{}, false, err
	}
	revision.PayloadHash = hash
	return s.store.AppendEstimateRevision(ctx, revision)
}

func (s *Service) AppendRewardRevision(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, revision domain.CausalRewardRevision) (domain.CausalRewardRevision, bool, error) {
	spaceIDs := []domain.SpaceID{revision.SpaceID}
	if err := s.authorize(ctx, tenantID, principalID, spaceIDs, domain.GrantOperationCausalRewardAppend); err != nil {
		return domain.CausalRewardRevision{}, false, err
	}
	revision.TenantID = tenantID
	hash, err := RewardPayloadHash(revision)
	if err != nil {
		return domain.CausalRewardRevision{}, false, err
	}
	revision.PayloadHash = hash
	return s.store.AppendRewardRevision(ctx, revision)
}

func (s *Service) authorize(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, spaceIDs []domain.SpaceID, operation domain.GrantOperation) error {
	_, err := s.authorizer.AuthorizeExact(ctx, authz.Identity{TenantID: tenantID, PrincipalID: principalID}, dedupeSpaces(spaceIDs), domain.GrantPurposeCuration, operation)
	return err
}

func dedupeSpaces(spaceIDs []domain.SpaceID) []domain.SpaceID {
	seen := map[domain.SpaceID]bool{}
	deduped := make([]domain.SpaceID, 0, len(spaceIDs))
	for _, spaceID := range spaceIDs {
		if seen[spaceID] {
			continue
		}
		seen[spaceID] = true
		deduped = append(deduped, spaceID)
	}
	return deduped
}

// Canonical hash payloads freeze the durable contract: compact JSON in the
// displayed field order, UTC RFC3339Nano times, PayloadHash excluded and
// PreviousHash included. Changing any key or order breaks every stored chain.

type trialHashPayload struct {
	EventID              string               `json:"event_id"`
	TenantID             string               `json:"tenant_id"`
	SpaceID              string               `json:"space_id"`
	ExperimentID         string               `json:"experiment_id"`
	Arm                  string               `json:"arm"`
	Ordinal              int                  `json:"ordinal"`
	ContextHash          string               `json:"context_hash"`
	InterventionHash     string               `json:"intervention_hash"`
	OutcomeAuthorityHash string               `json:"outcome_authority_hash"`
	InfrastructureValid  bool                 `json:"infrastructure_valid"`
	Success              bool                 `json:"success"`
	Effect               float64              `json:"effect"`
	EvidenceRefs         []evidenceRefPayload `json:"evidence_refs"`
	PreviousHash         string               `json:"previous_hash"`
	CreatedAt            string               `json:"created_at"`
}

type evidenceRefPayload struct {
	BatchID  string   `json:"batch_id"`
	EventIDs []string `json:"event_ids"`
}

type estimateHashPayload struct {
	TenantID              string   `json:"tenant_id"`
	SpaceID               string   `json:"space_id"`
	EstimateID            string   `json:"estimate_id"`
	Revision              int64    `json:"revision"`
	ExperimentID          string   `json:"experiment_id"`
	ArtifactHash          string   `json:"artifact_hash"`
	SourceTrialIDs        []string `json:"source_trial_ids"`
	SourceTrialDigest     string   `json:"source_trial_digest"`
	WithTrials            int      `json:"with_trials"`
	WithoutTrials         int      `json:"without_trials"`
	EffectMean            float64  `json:"effect_mean"`
	EffectVariance        float64  `json:"effect_variance"`
	IntervalLower         float64  `json:"interval_lower"`
	IntervalUpper         float64  `json:"interval_upper"`
	EstimatorVersion      string   `json:"estimator_version"`
	IntervalPolicyVersion string   `json:"interval_policy_version"`
	Conclusion            string   `json:"conclusion"`
	WitnessKind           string   `json:"witness_kind"`
	WitnessStatus         string   `json:"witness_status"`
	TransitionReason      string   `json:"transition_reason"`
	HistoricalValidity    bool     `json:"historical_validity"`
	CurrentApplicability  bool     `json:"current_applicability"`
	PreviousHash          string   `json:"previous_hash"`
	CreatedAt             string   `json:"created_at"`
}

type rewardHashPayload struct {
	TenantID            string   `json:"tenant_id"`
	SpaceID             string   `json:"space_id"`
	RewardID            string   `json:"reward_id"`
	Revision            int64    `json:"revision"`
	EstimateID          string   `json:"estimate_id"`
	EstimateRevision    int64    `json:"estimate_revision"`
	RewardVectorHash    string   `json:"reward_vector_hash"`
	RewardVectorJSON    []byte   `json:"reward_vector_json"`
	RewardPolicyVersion string   `json:"reward_policy_version"`
	Scalar              *float64 `json:"scalar"`
	HardGateDisposition string   `json:"hard_gate_disposition"`
	PreviousHash        string   `json:"previous_hash"`
	CreatedAt           string   `json:"created_at"`
}

func TrialPayloadHash(event domain.CausalTrialEvent) (string, error) {
	refs := make([]evidenceRefPayload, 0, len(event.EvidenceRefs))
	for _, ref := range event.EvidenceRefs {
		if len(ref.EventIDs) == 0 {
			return "", errors.New("causal hash: evidence ref event ids must not be empty")
		}
		refs = append(refs, evidenceRefPayload{BatchID: string(ref.BatchID), EventIDs: ref.EventIDs})
	}
	payload := trialHashPayload{
		EventID: string(event.ID), TenantID: string(event.TenantID), SpaceID: string(event.SpaceID),
		ExperimentID: event.ExperimentID, Arm: string(event.Arm), Ordinal: event.Ordinal,
		ContextHash: event.ContextHash, InterventionHash: event.InterventionHash, OutcomeAuthorityHash: event.OutcomeAuthorityHash,
		InfrastructureValid: event.InfrastructureValid, Success: event.Success, Effect: event.Effect,
		EvidenceRefs: refs, PreviousHash: event.PreviousHash, CreatedAt: event.CreatedAt.UTC().Format(timeFormatNano),
	}
	return hashPayload(payload, event.Effect)
}

func EstimatePayloadHash(revision domain.CausalEstimateRevision) (string, error) {
	trials := make([]string, 0, len(revision.SourceTrialIDs))
	for _, id := range revision.SourceTrialIDs {
		trials = append(trials, string(id))
	}
	payload := estimateHashPayload{
		TenantID: string(revision.TenantID), SpaceID: string(revision.SpaceID), EstimateID: string(revision.EstimateID),
		Revision: revision.Revision, ExperimentID: revision.ExperimentID, ArtifactHash: revision.ArtifactHash,
		SourceTrialIDs: trials, SourceTrialDigest: revision.SourceTrialDigest,
		WithTrials: revision.WithTrials, WithoutTrials: revision.WithoutTrials,
		EffectMean: revision.EffectMean, EffectVariance: revision.EffectVariance,
		IntervalLower: revision.IntervalLower, IntervalUpper: revision.IntervalUpper,
		EstimatorVersion: revision.EstimatorVersion, IntervalPolicyVersion: revision.IntervalPolicyVersion,
		Conclusion: string(revision.Conclusion), WitnessKind: string(revision.WitnessKind), WitnessStatus: string(revision.WitnessStatus),
		TransitionReason: string(revision.TransitionReason), HistoricalValidity: revision.HistoricalValidity,
		CurrentApplicability: revision.CurrentApplicability, PreviousHash: revision.PreviousHash,
		CreatedAt: revision.CreatedAt.UTC().Format(timeFormatNano),
	}
	return hashPayload(payload, revision.EffectMean)
}

func RewardPayloadHash(revision domain.CausalRewardRevision) (string, error) {
	payload := rewardHashPayload{
		TenantID: string(revision.TenantID), SpaceID: string(revision.SpaceID), RewardID: string(revision.RewardID),
		Revision: revision.Revision, EstimateID: string(revision.EstimateID), EstimateRevision: revision.EstimateRevision,
		RewardVectorHash: revision.RewardVectorHash, RewardVectorJSON: revision.RewardVectorJSON,
		RewardPolicyVersion: revision.RewardPolicyVersion, Scalar: revision.Scalar, HardGateDisposition: revision.HardGateDisposition,
		PreviousHash: revision.PreviousHash, CreatedAt: revision.CreatedAt.UTC().Format(timeFormatNano),
	}
	return hashPayload(payload, 0)
}

const timeFormatNano = "2006-01-02T15:04:05.999999999Z07:00"

func hashPayload(payload any, effect float64) (string, error) {
	if math.IsNaN(effect) || math.IsInf(effect, 0) {
		return "", errors.New("causal hash: effect values must be finite")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("causal hash: encode canonical payload: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// ReviseWitness applies the frozen witness state machine. Only governed
// forward transitions exist: retirement is terminal, independent support must
// clear the policy lineage minimum and the interval delta, and every reason
// fixes its one legal target status.
func ReviseWitness(input domain.WitnessTransitionInput, policy domain.WitnessPolicy) (domain.CausalEstimateRevision, error) {
	if current := input.Current; current != nil && current.WitnessStatus == domain.WitnessStatusRetired {
		return domain.CausalEstimateRevision{}, fmt.Errorf("%w: retired witness cannot transition", ErrInvalidWitnessTransition)
	}
	var want domain.WitnessStatus
	switch input.Reason {
	case domain.WitnessReasonPairedWitness:
		want = domain.WitnessStatusProvisional
	case domain.WitnessReasonIndependentSupport:
		want = domain.WitnessStatusActive
		if input.IndependentLineages < policy.MinimumIndependentLineages || input.Estimate.IntervalLower <= policy.Delta {
			return domain.CausalEstimateRevision{}, fmt.Errorf("%w: independent support requires %d lineages and interval lower above %v", ErrInvalidWitnessTransition, policy.MinimumIndependentLineages, policy.Delta)
		}
	case domain.WitnessReasonNonpositiveInterval, domain.WitnessReasonMandatoryRegressionFailed:
		want = domain.WitnessStatusContested
	case domain.WitnessReasonSourceRetracted, domain.WitnessReasonArtifactSuperseded, domain.WitnessReasonNegativeRetirementGate:
		want = domain.WitnessStatusRetired
	default:
		return domain.CausalEstimateRevision{}, fmt.Errorf("%w: unknown transition reason %q", ErrInvalidWitnessTransition, input.Reason)
	}
	if input.RequestedStatus != want {
		return domain.CausalEstimateRevision{}, fmt.Errorf("%w: reason %q cannot request status %q", ErrInvalidWitnessTransition, input.Reason, input.RequestedStatus)
	}
	revised := input.Estimate
	revised.WitnessKind = input.Estimate.WitnessKind
	revised.WitnessStatus = want
	revised.TransitionReason = input.Reason
	revised.Revision = input.RequestedRevision
	revised.HistoricalValidity = want != domain.WitnessStatusRetired
	revised.CurrentApplicability = want == domain.WitnessStatusProvisional || want == domain.WitnessStatusActive
	return revised, nil
}

// EvaluateWitnessApplicability maps one revision's witness status onto the
// frozen applicability gates.
func EvaluateWitnessApplicability(revision domain.CausalEstimateRevision) (domain.WitnessApplicability, error) {
	switch revision.WitnessStatus {
	case domain.WitnessStatusActive:
		return domain.WitnessApplicability{HistoricalAuditEligible: true, PromotionEligible: true, RecommendationEligible: true}, nil
	case domain.WitnessStatusProvisional:
		return domain.WitnessApplicability{HistoricalAuditEligible: true}, nil
	case domain.WitnessStatusNone, domain.WitnessStatusContested, domain.WitnessStatusRetired:
		return domain.WitnessApplicability{}, nil
	default:
		return domain.WitnessApplicability{}, fmt.Errorf("unknown witness status %q", revision.WitnessStatus)
	}
}
