package toolhandoff_test

import (
	"context"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/compositehandoff"
	"river2.dev/graph-memory-service/internal/skillevolution/toolhandoff"
)

func TestToolHandoffGatesAndRegistersDirectCandidateViewForPendingArmC(t *testing.T) {
	t.Run("rejects not eligible", func(t *testing.T) {
		view := candidate.NewToolCandidateService()
		draft, _ := view.SubmitLLMSynthesized(context.Background(), candidate.LLMSynthesizedToolCandidateRequest{CandidateID: "tool-1"})
		gov, pending := &governance{}, &pendingRecorder{}
		result, _ := toolhandoff.NewService(&evidenceStore{}, gov, pending).Submit(context.Background(), request(draft))
		if result.Accepted || result.Reason != toolhandoff.ReasonNotEligible || gov.calls != 0 || pending.calls != 0 {
			t.Fatalf("result=%#v", result)
		}
	})
	t.Run("rejects uncommitted evidence", func(t *testing.T) {
		gov, pending := &governance{}, &pendingRecorder{}
		result, _ := toolhandoff.NewService(&evidenceStore{}, gov, pending).Submit(context.Background(), request(eligible(t)))
		if result.Accepted || result.Reason != toolhandoff.ReasonEvidenceNotCommitted || gov.calls != 0 || pending.calls != 0 {
			t.Fatalf("result=%#v", result)
		}
	})
	t.Run("eligible binds direct candidate view and remains ArmC pending", func(t *testing.T) {
		e := evidence("evidence-1")
		gov, pending := &governance{candidate: boundView(t, "bound-1", "body")}, &pendingRecorder{}
		result, _ := toolhandoff.NewService(&evidenceStore{records: map[string]contract.EvidenceRef{e.EvidenceID: e}}, gov, pending).Submit(context.Background(), request(eligible(t)))
		if !result.Accepted || !result.Pending || gov.calls != 1 || pending.calls != 1 || result.Candidate == nil {
			t.Fatalf("result=%#v calls=%d/%d", result, gov.calls, pending.calls)
		}
		if pending.last.Candidate == nil || pending.last.Candidate.Ref() != result.Candidate.Ref() {
			t.Fatalf("pending direct view=%#v result=%#v", pending.last.Candidate, result.Candidate)
		}
		if gov.last.ArtifactDoc["kind"] != "tool" || gov.last.ProposalDoc["proposed_kind"] != "tool" {
			t.Fatalf("docs=%#v", gov.last)
		}
	})
}

type evidenceStore struct {
	records map[string]contract.EvidenceRef
}

func (s *evidenceStore) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	r, ok := s.records[id]
	return r, ok, nil
}

type governance struct {
	calls     int
	candidate *candidate.CandidateView
	last      compositehandoff.GovernanceRequest
}

func (g *governance) Bind(_ context.Context, r compositehandoff.GovernanceRequest) (*candidate.CandidateView, error) {
	g.calls++
	g.last = r
	return g.candidate, nil
}

type pendingRecorder struct {
	calls int
	last  toolhandoff.PendingArmCRegistration
}

func (a *pendingRecorder) RegisterPending(_ context.Context, registration toolhandoff.PendingArmCRegistration) error {
	a.calls++
	a.last = registration
	return nil
}

func request(view *candidate.ToolCandidateView) toolhandoff.Request {
	e := evidence("evidence-1")
	return toolhandoff.Request{Candidate: view, Artifact: toolhandoff.ToolArtifactSpec{Title: "tool", Description: "tool", Permissions: []compositehandoff.Permission{{Capability: "tool_execute", Scope: "task-private"}}, PackageRef: vref("package"), InputSchema: vref("input"), OutputSchema: vref("output"), ErrorSchema: vref("error"), ValidationContractRef: vref("contract"), EvidenceRefs: []contract.EvidenceRef{e}}, Target: toolhandoff.Target{TenantID: "tenant", SpaceID: "space", TargetSkillID: "tool", BaseVersion: 1}, DecisionID: "decision", PolicyRef: domain.PolicyArtifactRef{PolicyID: "policy", Version: 1, Digest: digest("policy")}, Contract: armc.ValidationContract{ContractID: "contract", Version: 1, Digest: digest("contract")}, Manifest: armc.TaskFamilyManifest{ManifestID: "manifest", Version: 1, Digest: digest("manifest")}, Coverage: domain.CoverageProof{ProofID: "coverage", Version: 1, Digest: digest("coverage")}}
}
func eligible(t *testing.T) *candidate.ToolCandidateView {
	t.Helper()
	s := candidate.NewToolCandidateService()
	v, _ := s.SubmitLLMSynthesized(context.Background(), candidate.LLMSynthesizedToolCandidateRequest{CandidateID: "tool-1", Opportunities: []candidate.ToolOpportunityEvidence{{OpportunityID: "a", SourceLineageID: "a", ContextProfileID: "a", ContractValidation: candidate.ToolContractValidationEvidence{ValidationID: "a", Passed: true}}, {OpportunityID: "b", SourceLineageID: "b", ContextProfileID: "b", ContractValidation: candidate.ToolContractValidationEvidence{ValidationID: "b", Passed: true}}}})
	return v
}
func boundView(t *testing.T, id, body string) *candidate.CandidateView {
	t.Helper()
	bytes := []byte(body)
	view, err := candidate.NewCandidateView(contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: id, Kind: "tool", BodyDigest: contract.DigestBytes(bytes), OriginType: "skill_proposal", OriginRef: vref("proposal-" + id)}, bytes)
	if err != nil {
		t.Fatal(err)
	}
	return view
}
func evidence(id string) contract.EvidenceRef {
	return contract.EvidenceRef{SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: id, Version: "1", EvidenceDigest: digest(id), CommitState: "committed", EvidenceKind: "observation"}
}
func vref(id string) contract.VersionedRef {
	return contract.VersionedRef{ID: id, Version: "1", Digest: digest(id)}
}
func digest(seed string) string { return "sha256:" + strings.Repeat(string(seed[0]), 64) }
