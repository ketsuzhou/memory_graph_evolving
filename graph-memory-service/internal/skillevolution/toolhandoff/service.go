package toolhandoff

import (
	"context"
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/compositehandoff"
)

const (
	ReasonNotEligible          = "TOOL_CANDIDATE_NOT_ARM_C_ELIGIBLE"
	ReasonEvidenceNotCommitted = "EVIDENCE_NOT_COMMITTED"
)

type ToolArtifactSpec struct {
	Title                 string
	Description           string
	Permissions           []compositehandoff.Permission
	PackageRef            contract.VersionedRef
	InputSchema           contract.VersionedRef
	OutputSchema          contract.VersionedRef
	ErrorSchema           contract.VersionedRef
	ValidationContractRef contract.VersionedRef
	EvidenceRefs          []contract.EvidenceRef
}

type Target struct {
	TenantID      domain.TenantID
	SpaceID       domain.SpaceID
	TargetSkillID string
	BaseVersion   int64
}
type PendingArmCRegistration struct {
	Candidate  *candidate.CandidateView
	Target     Target
	DecisionID string
	PolicyRef  domain.PolicyArtifactRef
	Contract   armc.ValidationContract
	Manifest   armc.TaskFamilyManifest
	Coverage   domain.CoverageProof
}
type PendingRegistrar interface {
	RegisterPending(context.Context, PendingArmCRegistration) error
}
type EvidenceResolver interface {
	GetEvidence(string) (contract.EvidenceRef, bool, error)
}
type Governance interface {
	Bind(context.Context, compositehandoff.GovernanceRequest) (*candidate.CandidateView, error)
}

type Request struct {
	Candidate  *candidate.ToolCandidateView
	Artifact   ToolArtifactSpec
	Target     Target
	DecisionID string
	PolicyRef  domain.PolicyArtifactRef
	Contract   armc.ValidationContract
	Manifest   armc.TaskFamilyManifest
	Coverage   domain.CoverageProof
}

type Submission struct {
	Accepted  bool
	Pending   bool
	Reason    string
	Candidate *candidate.CandidateView
}
type Service struct {
	evidence   EvidenceResolver
	governance Governance
	armc       PendingRegistrar
}

func NewService(e EvidenceResolver, g Governance, a PendingRegistrar) *Service {
	return &Service{evidence: e, governance: g, armc: a}
}

func (s *Service) Submit(ctx context.Context, request Request) (Submission, error) {
	if request.Candidate == nil || !request.Candidate.ArmCEligible() {
		return Submission{Reason: ReasonNotEligible}, nil
	}
	if err := s.requireEvidence(request.Artifact.EvidenceRefs); err != nil {
		return Submission{Reason: ReasonEvidenceNotCommitted}, nil
	}
	proposal, artifact := documents(request)
	bound, err := s.governance.Bind(ctx, compositehandoff.GovernanceRequest{ProposalDoc: proposal, ArtifactDoc: artifact, CandidateID: request.Candidate.CandidateID(), Target: compositehandoff.Target{TenantID: request.Target.TenantID, SpaceID: request.Target.SpaceID, TargetSkillID: request.Target.TargetSkillID, BaseVersion: request.Target.BaseVersion}})
	if err != nil {
		return Submission{Reason: err.Error()}, nil
	}
	pending := PendingArmCRegistration{Candidate: bound, Target: request.Target, DecisionID: request.DecisionID, PolicyRef: request.PolicyRef, Contract: request.Contract, Manifest: request.Manifest, Coverage: request.Coverage}
	if err := s.armc.RegisterPending(ctx, pending); err != nil {
		return Submission{Reason: "arm_c_pending_registration_failed", Candidate: bound}, nil
	}
	return Submission{Accepted: true, Pending: true, Candidate: bound}, nil
}

func (s *Service) requireEvidence(refs []contract.EvidenceRef) error {
	if len(refs) == 0 {
		return fmt.Errorf("no evidence")
	}
	for _, r := range refs {
		got, ok, err := s.evidence.GetEvidence(r.EvidenceID)
		if err != nil || !ok || got.Version != r.Version || got.EvidenceDigest != r.EvidenceDigest || (got.CommitState != "committed" && got.CommitState != "sealed") {
			return fmt.Errorf("evidence")
		}
	}
	return nil
}
func documents(r Request) (map[string]any, map[string]any) {
	perms := []any{}
	caps := []any{}
	for _, p := range r.Artifact.Permissions {
		perms = append(perms, map[string]any{"capability": p.Capability, "scope": p.Scope})
		caps = append(caps, p.Capability)
	}
	evidence := []any{}
	for _, e := range r.Artifact.EvidenceRefs {
		evidence = append(evidence, map[string]any{"schema_version": e.SchemaVersion, "evidence_id": e.EvidenceID, "version": json.Number(e.Version), "evidence_digest": e.EvidenceDigest, "commit_state": e.CommitState, "evidence_kind": e.EvidenceKind})
	}
	vref := func(x contract.VersionedRef) map[string]any {
		return map[string]any{"id": x.ID, "version": json.Number(x.Version), "digest": x.Digest}
	}
	proposal := map[string]any{"schema_version": "gms.skill-proposal.v1", "proposal_id": "proposal:" + r.Candidate.CandidateID(), "proposal_version": json.Number("1"), "proposed_kind": "tool", "source_segment_refs": []any{map[string]any{"schema_version": "host.segment-ref.v1", "room_id": "tool-handoff", "segment_id": r.Candidate.CandidateID(), "segment_version": json.Number("1"), "segment_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "evidence_seal_ref": vref(contract.VersionedRef{ID: "seal", Version: "1", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}), "path_seal_ref": vref(contract.VersionedRef{ID: "path", Version: "1", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})}}, "evidence_refs": evidence, "requested_operation": "create_lineage", "origin": map[string]any{"initiator_type": "automatic", "initiator_ref": "tool-handoff", "request_ref": r.Candidate.CandidateID()}, "policy_refs": []any{vref(contract.VersionedRef{ID: r.PolicyRef.PolicyID, Version: jsonVersion(r.PolicyRef.Version), Digest: r.PolicyRef.Digest})}}
	artifact := map[string]any{"schema_version": "gms.skill-artifact.v2", "kind": "tool", "title": r.Artifact.Title, "description": r.Artifact.Description, "applicability": map[string]any{"predicates": []any{}, "exclusions": []any{}}, "permissions": perms, "body": map[string]any{"tool_package": map[string]any{"schema_version": "gms.tool-package-ref.v2", "oci_image_digest": r.Artifact.PackageRef.Digest, "entrypoint": []any{"/tool/run"}, "build_attestation_ref": vref(r.Artifact.PackageRef)}, "interface": map[string]any{"input_schema_ref": vref(r.Artifact.InputSchema), "output_schema_ref": vref(r.Artifact.OutputSchema), "error_schema_ref": vref(r.Artifact.ErrorSchema)}, "execution_policy": map[string]any{"network": "disabled", "cpu_millis": json.Number("1"), "memory_bytes": json.Number("1"), "timeout_millis": json.Number("1"), "workspace_mode": "read_only"}, "declared_capabilities": caps, "validation_contract_ref": vref(r.Artifact.ValidationContractRef), "candidate_provenance_ref": map[string]any{"schema_version": "gms.candidate-artifact-ref.v2", "candidate_id": r.Candidate.CandidateID(), "kind": "tool", "body_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "origin_type": "skill_proposal", "origin_ref": vref(contract.VersionedRef{ID: "source-proposal", Version: "1", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})}}}
	return proposal, artifact
}
func jsonVersion(v int64) string { return fmt.Sprintf("%d", v) }
