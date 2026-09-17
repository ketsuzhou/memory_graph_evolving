package compositehandoff_test

import (
	"context"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/compositehandoff"
	"river2.dev/graph-memory-service/internal/skillevolution/compositeproposal"
)

func TestCompositeHandoffRejectsIncompleteEvidenceAndPermissionMismatch(t *testing.T) {
	t.Run("rejects threshold evidence that is not committed", func(t *testing.T) {
		governance := &recordingGovernance{}
		service := compositehandoff.NewService(&evidenceStore{}, childStore(), governance, &recordingArmC{})
		result, err := service.Submit(context.Background(), request())
		if err != nil {
			t.Fatalf("submit draft: %v", err)
		}
		if result.Accepted || result.Reason != compositehandoff.ReasonEvidenceNotCommitted || governance.binds != 0 {
			t.Fatalf("result = %#v, binds=%d", result, governance.binds)
		}
	})

	t.Run("rejects caller permission union that differs from active children", func(t *testing.T) {
		evidence := &evidenceStore{records: map[string]contract.EvidenceRef{"evidence-1": evidence("evidence-1")}}
		governance := &recordingGovernance{}
		req := request()
		req.Supplement.DeclaredPermissionUnion = []compositehandoff.Permission{{Capability: "memory_expand", Scope: "room"}}
		req.Supplement.Digest = compositehandoff.SupplementDigest(req.Supplement)
		result, err := compositehandoff.NewService(evidence, childStore(), governance, &recordingArmC{}).Submit(context.Background(), req)
		if err != nil {
			t.Fatalf("submit draft: %v", err)
		}
		if result.Accepted || result.Reason != compositehandoff.ReasonPermissionUnionMismatch || governance.binds != 0 {
			t.Fatalf("result = %#v, binds=%d", result, governance.binds)
		}
	})
}

func TestCompositeHandoffBindsBridgesRegistersArmCAndRechecksAtRelease(t *testing.T) {
	evidence := &evidenceStore{records: map[string]contract.EvidenceRef{"evidence-1": evidence("evidence-1")}}
	children := childStore()
	governance := &recordingGovernance{candidate: candidateView(t, "candidate-1", "candidate-body")}
	armc := &recordingArmC{}
	service := compositehandoff.NewService(evidence, children, governance, armc)

	submission, err := service.Submit(context.Background(), request())
	if err != nil {
		t.Fatalf("submit complete draft: %v", err)
	}
	if !submission.Accepted || governance.binds != 1 || armc.registrations != 1 {
		t.Fatalf("submission = %#v, binds=%d armc=%d", submission, governance.binds, armc.registrations)
	}
	if got := governance.last.ProposalDoc["proposed_kind"]; got != "composite" {
		t.Fatalf("governance proposal kind = %#v, want composite", got)
	}
	if got := governance.last.ArtifactDoc["schema_version"]; got != "gms.skill-artifact.v2" {
		t.Fatalf("governance artifact schema = %#v, want v2", got)
	}
	if submission.Candidate.BodyDigest() != governance.candidate.BodyDigest() {
		t.Fatalf("candidate = %#v, want digest %q", submission.Candidate, governance.candidate.BodyDigest())
	}

	children.refs[children.second.LineageID] = child{Ref: movedRef(children.second), Permissions: children.permissions[children.second.LineageID]}
	release, err := service.Release(context.Background(), submission, request().Supplement)
	if err != nil {
		t.Fatalf("release handoff: %v", err)
	}
	if release.Released || release.Reason != compositehandoff.ReasonCompositeChildNotActive || armc.completions != 0 {
		t.Fatalf("release = %#v, completions=%d", release, armc.completions)
	}
}

type evidenceStore struct {
	records map[string]contract.EvidenceRef
}

func (s *evidenceStore) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	r, ok := s.records[id]
	return r, ok, nil
}

type child struct {
	Ref         contract.SkillArtifactRef
	Permissions []compositehandoff.Permission
}
type childrenStore struct {
	refs          map[string]child
	permissions   map[string][]compositehandoff.Permission
	first, second contract.SkillArtifactRef
}

func (s *childrenStore) ResolveChild(ref contract.SkillArtifactRef) (compositehandoff.ChildArtifact, error) {
	child, ok := s.refs[ref.LineageID]
	if !ok || child.Ref != ref {
		return compositehandoff.ChildArtifact{}, compositehandoff.ErrChildNotActive
	}
	return compositehandoff.ChildArtifact{Ref: child.Ref, Permissions: child.Permissions}, nil
}
func childStore() *childrenStore {
	a, b := childRef("lineage-a", "a"), childRef("lineage-b", "b")
	perms := map[string][]compositehandoff.Permission{"lineage-a": {{Capability: "memory_expand", Scope: "room"}}, "lineage-b": {{Capability: "skill_get", Scope: "room"}}}
	return &childrenStore{refs: map[string]child{"lineage-a": {Ref: a, Permissions: perms["lineage-a"]}, "lineage-b": {Ref: b, Permissions: perms["lineage-b"]}}, permissions: perms, first: a, second: b}
}

type recordingGovernance struct {
	binds     int
	candidate *candidate.CandidateView
	last      compositehandoff.GovernanceRequest
}

func (g *recordingGovernance) Bind(_ context.Context, request compositehandoff.GovernanceRequest) (*candidate.CandidateView, error) {
	g.binds++
	g.last = request
	return g.candidate, nil
}

type recordingArmC struct{ registrations, completions int }

func (a *recordingArmC) Register(context.Context, compositehandoff.ArmCRegistration) error {
	a.registrations++
	return nil
}
func (a *recordingArmC) Complete(context.Context, compositehandoff.ArmCRegistration) error {
	a.completions++
	return nil
}

func request() compositehandoff.Request {
	a, b := childRef("lineage-a", "a"), childRef("lineage-b", "b")
	req := compositehandoff.Request{Draft: compositeproposal.CompositeProposalDraft{DraftID: "draft-1", Status: compositeproposal.DraftStatusDrafted, NonAuthoritative: true, PolicyRef: versioned("policy"), SourceRefs: []contract.SkillArtifactRef{a, b}, ThresholdEvidence: []contract.EvidenceRef{evidence("evidence-1")}, Plan: compositeproposal.CompositeDraftPlan{Guard: "when both child skills apply", ControlFlow: []compositeproposal.ControlFlowStep{{StepID: "first", SkillRef: &a}, {StepID: "second", SkillRef: &b, DependsOn: []string{"first"}}}, PermissionUnion: []string{"memory_expand", "skill_get"}}}, Supplement: compositehandoff.CompositeHandoffSupplement{SupplementID: "supplement-1", Version: 1, SourcePorts: []compositehandoff.ChildPort{{Ref: a, ChildID: "first", InputPort: "in", OutputPort: "out"}, {Ref: b, ChildID: "second", InputPort: "in", OutputPort: "out"}}, OuterInputSchema: versioned("input"), OuterOutputSchema: versioned("output"), RetryPolicies: []compositehandoff.RetryPolicy{}, FailureHandlers: []compositehandoff.FailureHandler{}, DeclaredPermissionUnion: []compositehandoff.Permission{{Capability: "memory_expand", Scope: "room"}, {Capability: "skill_get", Scope: "room"}}, Target: compositehandoff.Target{TenantID: "tenant", SpaceID: "space", TargetSkillID: "target", BaseVersion: 1}, ArmC: compositehandoff.ArmCRegistration{DecisionID: "decision-1", PolicyRef: domain.PolicyArtifactRef{PolicyID: "arm-c", Version: 1, Digest: digest("policy")}}}}
	req.Supplement.Digest = compositehandoff.SupplementDigest(req.Supplement)
	return req
}
func childRef(lineage, seed string) contract.SkillArtifactRef {
	return contract.SkillArtifactRef{SchemaVersion: contract.SchemaSkillArtifactRef, LineageID: lineage, Version: "1", Kind: "step_guidance", ArtifactDigest: digest(seed)}
}
func movedRef(ref contract.SkillArtifactRef) contract.SkillArtifactRef {
	ref.ArtifactDigest = digest("moved")
	return ref
}
func evidence(id string) contract.EvidenceRef {
	return contract.EvidenceRef{SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: id, Version: "1", EvidenceDigest: digest(id), CommitState: "committed", EvidenceKind: "observation"}
}
func versioned(id string) contract.VersionedRef {
	return contract.VersionedRef{ID: id, Version: "1", Digest: digest(id)}
}
func digest(seed string) string {
	for len(seed) < 1 {
		seed = "a"
	}
	return "sha256:" + strings.Repeat(string(seed[0]), 64)
}

func candidateView(t *testing.T, id, body string) *candidate.CandidateView {
	t.Helper()
	bytes := []byte(body)
	view, err := candidate.NewCandidateView(contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: id, Kind: "composite", BodyDigest: contract.DigestBytes(bytes), OriginType: "merge_proposal", OriginRef: versioned("proposal-" + id)}, bytes)
	if err != nil {
		t.Fatal(err)
	}
	return view
}
