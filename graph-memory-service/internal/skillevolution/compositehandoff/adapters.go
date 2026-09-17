package compositehandoff

import (
	"context"
	"fmt"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/policyactivation"
)

// StrictGovernance moves a generated ordinary proposal through the real
// proposal state machine before invoking GMS-202 candidate binding.
type StrictGovernance struct{ binding *candidate.BindingService }

func NewStrictGovernance(binding *candidate.BindingService) (*StrictGovernance, error) {
	if binding == nil {
		return nil, fmt.Errorf("composite handoff: binding service required")
	}
	return &StrictGovernance{binding: binding}, nil
}
func (g *StrictGovernance) Bind(ctx context.Context, request GovernanceRequest) (*candidate.CandidateView, error) {
	proposalDoc := request.ProposalDoc
	digest, err := g.binding.Proposals().ComputeDigest(proposalDoc)
	if err != nil {
		return nil, err
	}
	proposalDoc["proposal_digest"] = digest
	if _, err := g.binding.Proposals().AppendTransition(ctx, proposalDoc, "none", "proposed"); err != nil {
		return nil, err
	}
	if _, err := g.binding.Proposals().AppendTransition(ctx, proposalDoc, "proposed", "admitted"); err != nil {
		return nil, err
	}
	return g.binding.Bind(ctx, candidate.BindRequest{ProposalDoc: proposalDoc, ArtifactDoc: request.ArtifactDoc, CandidateID: request.CandidateID})
}

// ArmCPlanRegistrar registers all exact records necessary for the existing
// server-owned CompleteArmC entry. Registration is idempotent by the Arm C
// store's immutable IDs; it performs no activation itself.
type ArmCPlanRegistrar struct {
	store      armc.Store
	candidates policyactivation.DirectActivationStore
	complete   func(context.Context, policyactivation.Job) (policyactivation.Result, error)
}

func NewArmCPlanRegistrar(store armc.Store, candidates policyactivation.DirectActivationStore, complete func(context.Context, policyactivation.Job) (policyactivation.Result, error)) (*ArmCPlanRegistrar, error) {
	if store == nil || candidates == nil || complete == nil {
		return nil, fmt.Errorf("composite handoff: Arm C store, direct candidate registry, and completion required")
	}
	return &ArmCPlanRegistrar{store: store, candidates: candidates, complete: complete}, nil
}
func (r *ArmCPlanRegistrar) Register(ctx context.Context, registration ArmCRegistration) error {
	if registration.Candidate == nil {
		return fmt.Errorf("composite handoff: direct candidate view required")
	}
	ref := registration.Candidate.Ref()
	direct := domain.ArmCCandidateRegistration{TenantID: registration.Target.TenantID, SpaceID: registration.Target.SpaceID, TargetSkillID: registration.Target.TargetSkillID, ExpectedActiveVersion: registration.Target.BaseVersion, CandidateRef: ref}
	if _, err := r.candidates.RegisterArmCCandidate(ctx, direct); err != nil {
		return err
	}
	if _, err := r.store.PutValidationContract(ctx, registration.Contract); err != nil {
		return err
	}
	if _, err := r.store.PutTaskFamilyManifest(ctx, registration.Manifest); err != nil {
		return err
	}
	for _, fixture := range registration.Fixtures {
		if _, err := r.store.PutFixture(ctx, fixture); err != nil {
			return err
		}
	}
	plan := armc.EvaluationPlan{CandidateID: ref.CandidateID, DecisionID: registration.DecisionID, DecisionVersion: registration.DecisionVersion, ManifestRef: registration.Manifest.Ref(), FixtureIDs: fixtureIDs(registration.Fixtures), PolicyRef: registration.PolicyRef, ExpectedActiveVersion: registration.Target.BaseVersion, Coverage: registration.Coverage}
	_, err := r.store.PutPlan(ctx, plan)
	return err
}
func (r *ArmCPlanRegistrar) Complete(ctx context.Context, registration ArmCRegistration) error {
	_, err := r.complete(ctx, policyactivation.Job{TenantID: registration.Target.TenantID, SpaceID: registration.Target.SpaceID, CandidateID: registration.Candidate.Ref().CandidateID})
	return err
}
func fixtureIDs(fixtures []armc.PairedFixture) []string {
	out := make([]string, len(fixtures))
	for i, f := range fixtures {
		out[i] = f.FixtureID
	}
	return out
}
