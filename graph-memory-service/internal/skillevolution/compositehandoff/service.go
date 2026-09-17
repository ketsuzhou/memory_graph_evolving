// Package compositehandoff moves a non-authoritative co-usage draft into the
// ordinary governance chain. It never writes the graph or activates directly.
package compositehandoff

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/compositeproposal"
)

const (
	ReasonEvidenceNotCommitted    = "EVIDENCE_NOT_COMMITTED"
	ReasonPermissionUnionMismatch = "PERMISSION_UNION_MISMATCH"
	ReasonCompositeChildNotActive = "COMPOSITE_CHILD_NOT_ACTIVE"
	ReasonDraftIncomplete         = "COMPOSITE_DRAFT_INCOMPLETE"
)

var ErrChildNotActive = fmt.Errorf("%s", ReasonCompositeChildNotActive)

type Permission struct{ Capability, Scope string }

type ChildPort struct {
	Ref                            contract.SkillArtifactRef
	ChildID, InputPort, OutputPort string
}
type RetryPolicy struct {
	ChildID              string
	MaxAttempts          int
	RetryableReasonCodes []string
	BackoffUnits         int
}
type FailureHandler struct {
	ChildID                 string
	ReasonCodes             []string
	Action, FallbackChildID string
}

type Target struct {
	TenantID      domain.TenantID
	SpaceID       domain.SpaceID
	TargetSkillID string
	BaseVersion   int64
}

// CompositeHandoffSupplement is independently versioned and digest-bound. It
// contains the orchestration facts intentionally absent from co-usage drafts.
type CompositeHandoffSupplement struct {
	SupplementID            string
	Version                 int64
	Digest                  string
	SourcePorts             []ChildPort
	OuterInputSchema        contract.VersionedRef
	OuterOutputSchema       contract.VersionedRef
	RetryPolicies           []RetryPolicy
	FailureHandlers         []FailureHandler
	DeclaredPermissionUnion []Permission
	Target                  Target
	ArmC                    ArmCRegistration
}

func SupplementDigest(s CompositeHandoffSupplement) string {
	copy := s
	copy.Digest = ""
	encoded, _ := json.Marshal(copy)
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

type Request struct {
	Draft      compositeproposal.CompositeProposalDraft
	Supplement CompositeHandoffSupplement
}

type ChildArtifact struct {
	Ref         contract.SkillArtifactRef
	Permissions []Permission
}
type ChildResolver interface {
	ResolveChild(contract.SkillArtifactRef) (ChildArtifact, error)
}
type EvidenceResolver interface {
	GetEvidence(string) (contract.EvidenceRef, bool, error)
}

type GovernanceRequest struct {
	ProposalDoc, ArtifactDoc map[string]any
	CandidateID              string
	Target                   Target
}
type Governance interface {
	Bind(context.Context, GovernanceRequest) (*candidate.CandidateView, error)
}
type ArmCRegistration struct {
	Candidate       *candidate.CandidateView
	Target          Target
	DecisionID      string
	DecisionVersion int64
	PolicyRef       domain.PolicyArtifactRef
	Contract        armc.ValidationContract
	Manifest        armc.TaskFamilyManifest
	Fixtures        []armc.PairedFixture
	Coverage        domain.CoverageProof
}
type ArmCRegistrar interface {
	Register(context.Context, ArmCRegistration) error
	Complete(context.Context, ArmCRegistration) error
}

type Submission struct {
	Accepted            bool
	Reason              string
	Candidate           *candidate.CandidateView
	ReleaseRegistration ArmCRegistration
}
type ReleaseResult struct {
	Released bool
	Reason   string
}

type Service struct {
	evidence   EvidenceResolver
	children   ChildResolver
	governance Governance
	armc       ArmCRegistrar
}

func NewService(e EvidenceResolver, c ChildResolver, g Governance, a ArmCRegistrar) *Service {
	return &Service{evidence: e, children: c, governance: g, armc: a}
}

func (s *Service) Submit(ctx context.Context, request Request) (Submission, error) {
	if reason := validateDraft(request.Draft, request.Supplement); reason != "" {
		return Submission{Reason: reason}, nil
	}
	if err := s.requireEvidence(request.Draft.ThresholdEvidence); err != nil {
		return Submission{Reason: ReasonEvidenceNotCommitted}, nil
	}
	permissions, err := s.recomputePermissions(request.Supplement.SourcePorts)
	if err != nil {
		return Submission{Reason: ReasonCompositeChildNotActive}, nil
	}
	if !samePermissions(permissions, request.Supplement.DeclaredPermissionUnion) {
		return Submission{Reason: ReasonPermissionUnionMismatch}, nil
	}
	proposalDoc, artifactDoc := buildDocuments(request.Draft, request.Supplement, permissions)
	candidate, err := s.governance.Bind(ctx, GovernanceRequest{ProposalDoc: proposalDoc, ArtifactDoc: artifactDoc, CandidateID: "candidate:" + request.Draft.DraftID, Target: request.Supplement.Target})
	if err != nil {
		return Submission{Reason: err.Error()}, nil
	}
	registration := request.Supplement.ArmC
	registration.Candidate = candidate
	registration.Target = request.Supplement.Target
	if err := s.armc.Register(ctx, registration); err != nil {
		return Submission{Reason: "arm_c_registration_failed", Candidate: candidate}, nil
	}
	return Submission{Accepted: true, Candidate: candidate, ReleaseRegistration: registration}, nil
}

// Release rechecks active child refs after candidate binding and before the
// server-owned Arm C completion call, closing child-head drift during review.
func (s *Service) Release(ctx context.Context, submission Submission, supplement CompositeHandoffSupplement) (ReleaseResult, error) {
	if !submission.Accepted {
		return ReleaseResult{Reason: ReasonDraftIncomplete}, nil
	}
	if _, err := s.recomputePermissions(supplement.SourcePorts); err != nil {
		return ReleaseResult{Reason: ReasonCompositeChildNotActive}, nil
	}
	if err := s.armc.Complete(ctx, submission.ReleaseRegistration); err != nil {
		return ReleaseResult{Reason: "arm_c_completion_failed"}, nil
	}
	return ReleaseResult{Released: true}, nil
}

func validateDraft(d compositeproposal.CompositeProposalDraft, s CompositeHandoffSupplement) string {
	if d.Status != compositeproposal.DraftStatusDrafted || !d.NonAuthoritative || len(d.SourceRefs) < 2 || len(d.ThresholdEvidence) == 0 || len(d.Counterevidence) != 0 || strings.TrimSpace(d.Plan.Guard) == "" {
		return ReasonDraftIncomplete
	}
	if s.SupplementID == "" || s.Version < 1 || s.Digest == "" || s.Digest != SupplementDigest(s) || len(s.SourcePorts) != len(d.SourceRefs) || s.OuterInputSchema.ID == "" || s.OuterOutputSchema.ID == "" {
		return ReasonDraftIncomplete
	}
	return ""
}
func (s *Service) requireEvidence(refs []contract.EvidenceRef) error {
	for _, ref := range refs {
		got, ok, err := s.evidence.GetEvidence(ref.EvidenceID)
		if err != nil || !ok || got.Version != ref.Version || got.EvidenceDigest != ref.EvidenceDigest || (got.CommitState != "committed" && got.CommitState != "sealed") {
			return ErrChildNotActive
		}
	}
	return nil
}
func (s *Service) recomputePermissions(ports []ChildPort) ([]Permission, error) {
	set := map[string]Permission{}
	for _, port := range ports {
		child, err := s.children.ResolveChild(port.Ref)
		if err != nil || child.Ref != port.Ref {
			return nil, ErrChildNotActive
		}
		for _, p := range child.Permissions {
			set[p.Capability+":"+p.Scope] = p
		}
	}
	out := make([]Permission, 0, len(set))
	for _, p := range set {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Capability+out[i].Scope < out[j].Capability+out[j].Scope })
	return out, nil
}
func samePermissions(a, b []Permission) bool {
	if len(a) != len(b) {
		return false
	}
	ac := append([]Permission(nil), a...)
	bc := append([]Permission(nil), b...)
	sort.Slice(ac, func(i, j int) bool { return ac[i].Capability+ac[i].Scope < ac[j].Capability+ac[j].Scope })
	sort.Slice(bc, func(i, j int) bool { return bc[i].Capability+bc[i].Scope < bc[j].Capability+bc[j].Scope })
	for i := range ac {
		if ac[i] != bc[i] {
			return false
		}
	}
	return true
}

func buildDocuments(d compositeproposal.CompositeProposalDraft, s CompositeHandoffSupplement, permissions []Permission) (map[string]any, map[string]any) {
	children := []any{}
	for _, p := range s.SourcePorts {
		children = append(children, map[string]any{"child_id": p.ChildID, "skill_ref": skillRefDoc(p.Ref), "input_port": p.InputPort, "output_port": p.OutputPort})
	}
	edges := []any{}
	stepChildren := map[string]string{}
	for _, step := range d.Plan.ControlFlow {
		for _, port := range s.SourcePorts {
			if step.SkillRef != nil && *step.SkillRef == port.Ref {
				stepChildren[step.StepID] = port.ChildID
			}
		}
	}
	for _, flow := range d.Plan.DataFlow {
		edges = append(edges, map[string]any{"from_child_id": stepChildren[flow.FromStep], "from_port": "out", "to_child_id": stepChildren[flow.ToStep], "to_port": "in", "mapping": flow.Mapping})
	}
	perms := []any{}
	for _, p := range permissions {
		perms = append(perms, map[string]any{"capability": p.Capability, "scope": p.Scope})
	}
	evidence := []any{}
	for _, e := range d.ThresholdEvidence {
		evidence = append(evidence, map[string]any{"schema_version": e.SchemaVersion, "evidence_id": e.EvidenceID, "version": e.Version, "evidence_digest": e.EvidenceDigest, "commit_state": e.CommitState, "evidence_kind": e.EvidenceKind})
	}
	proposal := map[string]any{"schema_version": "gms.skill-proposal.v1", "proposal_id": "proposal:" + d.DraftID, "proposal_version": json.Number("1"), "proposed_kind": "composite", "source_segment_refs": []any{map[string]any{"schema_version": "host.segment-ref.v1", "room_id": "handoff", "segment_id": d.DraftID, "segment_version": json.Number("1"), "segment_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "evidence_seal_ref": versionedDoc(d.PolicyRef), "path_seal_ref": versionedDoc(d.PolicyRef)}}, "evidence_refs": evidence, "requested_operation": "bind_composite", "origin": map[string]any{"initiator_type": "automatic", "initiator_ref": "composite-handoff", "request_ref": d.DraftID}, "policy_refs": []any{versionedDoc(d.PolicyRef)}}
	artifact := map[string]any{"schema_version": "gms.skill-artifact.v2", "kind": "composite", "title": "Co-usage composite " + d.DraftID, "description": d.Plan.Guard, "applicability": map[string]any{"predicates": []any{}, "exclusions": []any{}}, "permissions": perms, "body": map[string]any{"children": children, "edges": edges, "retry_policies": []any{}, "failure_handlers": []any{}, "orchestration_permissions": []any{}, "input_port_schema": schemaRefDoc(s.OuterInputSchema), "output_port_schema": schemaRefDoc(s.OuterOutputSchema)}}
	return proposal, artifact
}
func versionedDoc(r contract.VersionedRef) map[string]any {
	return map[string]any{"id": r.ID, "version": json.Number(r.Version), "digest": r.Digest}
}
func schemaRefDoc(r contract.VersionedRef) map[string]any {
	return map[string]any{"schema_id": r.ID, "version": json.Number(r.Version), "digest": r.Digest}
}
func skillRefDoc(r contract.SkillArtifactRef) map[string]any {
	return map[string]any{"schema_version": r.SchemaVersion, "lineage_id": r.LineageID, "version": json.Number(r.Version), "kind": r.Kind, "artifact_digest": r.ArtifactDigest}
}
