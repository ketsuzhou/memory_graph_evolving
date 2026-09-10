package causal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/causal"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/store/memory"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type recordingCausalStore struct {
	estimateAppends int
}

func (s *recordingCausalStore) AppendTrial(context.Context, domain.CausalTrialEvent) (domain.CausalTrialEvent, bool, error) {
	return domain.CausalTrialEvent{}, false, nil
}
func (s *recordingCausalStore) Trial(context.Context, domain.TenantID, domain.SpaceID, domain.CausalTrialEventID) (domain.CausalTrialEvent, error) {
	return domain.CausalTrialEvent{}, nil
}
func (s *recordingCausalStore) AppendEstimateRevision(_ context.Context, revision domain.CausalEstimateRevision) (domain.CausalEstimateRevision, bool, error) {
	s.estimateAppends++
	return revision, false, nil
}
func (s *recordingCausalStore) EstimateRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalEstimateID, int64) (domain.CausalEstimateRevision, error) {
	return domain.CausalEstimateRevision{}, nil
}
func (s *recordingCausalStore) LatestEstimateRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalEstimateID) (domain.CausalEstimateRevision, error) {
	return domain.CausalEstimateRevision{}, nil
}
func (s *recordingCausalStore) AppendRewardRevision(context.Context, domain.CausalRewardRevision) (domain.CausalRewardRevision, bool, error) {
	return domain.CausalRewardRevision{}, false, nil
}
func (s *recordingCausalStore) RewardRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalRewardID, int64) (domain.CausalRewardRevision, error) {
	return domain.CausalRewardRevision{}, nil
}
func (s *recordingCausalStore) LatestRewardRevision(context.Context, domain.TenantID, domain.SpaceID, domain.CausalRewardID) (domain.CausalRewardRevision, error) {
	return domain.CausalRewardRevision{}, nil
}

func TestCausalEstimateAppendRequiresCurationPurpose(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	registry := memory.New()
	tenantID := domain.TenantID("tenant-curation")
	spaceID := domain.SpaceID("space-curation")
	if _, err := registry.InitializeTenant(ctx, domain.Tenant{
		ID: tenantID, DisplayName: "Curation", BootstrapPrincipalID: "bootstrap",
	}, domain.Principal{ID: "bootstrap", TenantID: tenantID, Kind: domain.PrincipalService, DisplayName: "Bootstrap"}); err != nil {
		t.Fatalf("initialize tenant: %v", err)
	}
	if _, err := registry.PutSpace(ctx, tenantID, domain.Space{ID: spaceID, TenantID: tenantID, Scope: domain.SpaceShared, DisplayName: "Shared"}); err != nil {
		t.Fatalf("put space: %v", err)
	}

	tests := []struct {
		name      string
		principal domain.Principal
		purpose   domain.GrantPurpose
		wantCode  string
	}{
		{
			name:      "tool plane agent is fenced out",
			principal: domain.Principal{ID: "memory-agent", TenantID: tenantID, Kind: domain.PrincipalAgent, DisplayName: "Memory Agent"},
			purpose:   domain.GrantPurposeToolPlane,
			wantCode:  "GRANT_MISSING",
		},
		{
			name:      "curator with exact grant can append",
			principal: domain.Principal{ID: "curator", TenantID: tenantID, Kind: domain.PrincipalHuman, DisplayName: "Curator"},
			purpose:   domain.GrantPurposeCuration,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := registry.PutPrincipal(ctx, tenantID, test.principal); err != nil {
				t.Fatalf("put principal: %v", err)
			}
			if _, err := registry.PutGrant(ctx, tenantID, domain.Grant{
				ID: domain.GrantID("grant-" + string(test.principal.ID)), TenantID: tenantID, PrincipalID: test.principal.ID,
				SpaceIDs: []domain.SpaceID{spaceID}, Purpose: test.purpose,
				Operations: []domain.GrantOperation{domain.GrantOperationCausalEstimateAppend}, ExpiresAt: now.Add(time.Hour),
			}); err != nil {
				t.Fatalf("put grant: %v", err)
			}
			store := &recordingCausalStore{}
			service := causal.New(registry, store, fixedClock{now: now})
			_, _, err := service.AppendEstimateRevision(ctx, tenantID, test.principal.ID, domain.CausalEstimateRevision{
				TenantID: tenantID, SpaceID: spaceID, EstimateID: "estimate-1", Revision: 1,
				ExperimentID: "experiment-1", ArtifactHash: hashOf('d'), SourceTrialIDs: []domain.CausalTrialEventID{"trial-1", "trial-2"},
				SourceTrialDigest: hashOf('e'), WithTrials: 1, WithoutTrials: 1, EffectMean: 0.5,
				IntervalLower: 0.2, IntervalUpper: 0.8, EstimatorVersion: "estimator-v1", IntervalPolicyVersion: "interval-v1",
				Conclusion: domain.CausalConclusionConclusive, WitnessKind: domain.WitnessKindNecessity,
				WitnessStatus: domain.WitnessStatusProvisional, CreatedAt: now,
			})
			if test.wantCode != "" {
				var protocol *domain.ProtocolError
				if !errors.As(err, &protocol) || protocol.Code() != test.wantCode {
					t.Fatalf("append error = %v, want protocol code %s", err, test.wantCode)
				}
				if store.estimateAppends != 0 {
					t.Fatalf("denied append reached store %d times", store.estimateAppends)
				}
				return
			}
			if err != nil {
				t.Fatalf("curation append: %v", err)
			}
			if store.estimateAppends != 1 {
				t.Fatalf("store appends = %d, want 1", store.estimateAppends)
			}
		})
	}
}

func TestTrialPayloadHashFreezesCanonicalPayloadAndPredecessor(t *testing.T) {
	event := domain.CausalTrialEvent{
		ID: "trial-1", TenantID: "tenant-1", SpaceID: "space-1", ExperimentID: "experiment-1",
		Arm: domain.CausalArmWithIntervention, Ordinal: 1,
		ContextHash: hashOf('a'), InterventionHash: hashOf('b'), OutcomeAuthorityHash: hashOf('c'),
		InfrastructureValid: true, Success: true, Effect: 0.75,
		EvidenceRefs: []domain.EvidenceRef{{BatchID: "batch-1", EventIDs: []string{"event-1"}}},
		CreatedAt:    time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC),
	}

	got, err := causal.TrialPayloadHash(event)
	if err != nil {
		t.Fatalf("hash trial payload: %v", err)
	}
	const want = "fedbe30eef6b7db56d9992f4c7a27489a3da2b879052fe7721f311cbe58d7ea4"
	if got != want {
		t.Fatalf("payload hash = %q, want %q", got, want)
	}

	changed := event
	changed.PreviousHash = want
	changed.Effect = 0.76
	changedHash, err := causal.TrialPayloadHash(changed)
	if err != nil {
		t.Fatalf("hash chained payload: %v", err)
	}
	if changedHash == got {
		t.Fatal("changed payload and predecessor retained the old hash")
	}
}

func TestWitnessLifecycleAllowsOnlyGovernedForwardTransitions(t *testing.T) {
	policy := domain.WitnessPolicy{
		PolicyVersion: "witness-v1", Delta: 0.1, MinimumIndependentLineages: 2, RetirementUpperBound: 0,
	}
	base := domain.CausalEstimateRevision{
		EstimateID: "estimate-witness", Revision: 1, Conclusion: domain.CausalConclusionConclusive,
		WitnessKind: domain.WitnessKindNecessity, WitnessStatus: domain.WitnessStatusNone,
		IntervalLower: 0.2, IntervalUpper: 0.8,
	}

	t.Run("paired witness becomes provisional", func(t *testing.T) {
		got, err := causal.ReviseWitness(domain.WitnessTransitionInput{
			Estimate: base, Reason: domain.WitnessReasonPairedWitness,
			RequestedStatus: domain.WitnessStatusProvisional, RequestedRevision: 1,
		}, policy)
		if err != nil {
			t.Fatalf("revise witness: %v", err)
		}
		if got.WitnessStatus != domain.WitnessStatusProvisional || !got.HistoricalValidity || !got.CurrentApplicability {
			t.Fatalf("provisional revision = %#v", got)
		}
	})

	t.Run("insufficient support cannot skip to active", func(t *testing.T) {
		_, err := causal.ReviseWitness(domain.WitnessTransitionInput{
			Estimate: base, Reason: domain.WitnessReasonIndependentSupport, IndependentLineages: 1,
			RequestedStatus: domain.WitnessStatusActive, RequestedRevision: 1,
		}, policy)
		if !errors.Is(err, causal.ErrInvalidWitnessTransition) {
			t.Fatalf("transition error = %v, want ErrInvalidWitnessTransition", err)
		}
	})

	t.Run("retired witness cannot resurrect", func(t *testing.T) {
		retired := base
		retired.Revision = 3
		retired.WitnessStatus = domain.WitnessStatusRetired
		_, err := causal.ReviseWitness(domain.WitnessTransitionInput{
			Current: &retired, Estimate: base, Reason: domain.WitnessReasonPairedWitness,
			RequestedStatus: domain.WitnessStatusProvisional, RequestedRevision: 4,
		}, policy)
		if !errors.Is(err, causal.ErrInvalidWitnessTransition) {
			t.Fatalf("transition error = %v, want ErrInvalidWitnessTransition", err)
		}
	})
}

func hashOf(value byte) string {
	bytes := make([]byte, 64)
	for index := range bytes {
		bytes[index] = value
	}
	return string(bytes)
}
