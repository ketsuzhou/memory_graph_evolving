package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

type probationVisibility struct{ visible map[string]bool }

func (v probationVisibility) AdvisoryVisible(candidateID string, _ usageprojection.ContextProfile) bool {
	return v.visible[candidateID]
}

func TestAdvisoryReadHidesProbationCandidateOutsideEnvelope(t *testing.T) {
	world := newSkillEvolutionWorld(t)
	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{
		Token: contractToken, Reader: world.service(t, world.store), Usage: usage, WriterIdentity: "server-diagnostician",
		Advisory:  sevAdvisoryReader{candidates: []candidate.AdvisoryCandidate{{CandidateID: "candidate-inside", Kind: "human_procedure", Guidance: "inside"}, {CandidateID: "candidate-outside", Kind: "step_guidance", Guidance: "outside"}}},
		Probation: probationVisibility{visible: map[string]bool{"candidate-inside": true}},
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	profile := map[string]any{"schema_version": "context-profile/1.0", "task_family": "code", "runtime_class": "go", "workspace_feature_tags": []any{}, "observable_guard_facts": []any{}, "tool_policy_ref": sevVersionedRef("tool-policy"), "environment_class": "sandboxed"}
	status, _, payload := contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionAdvisoryReadPath, map[string]any{"request_id": "probation-advisory", "context_profile": profile})
	contractRequireStatus(t, status, http.StatusOK, payload)
	response := contractDecodeObject(t, payload)
	candidates, _ := response["candidates"].([]any)
	if len(candidates) != 1 || candidates[0].(map[string]any)["candidate_id"] != "candidate-inside" {
		t.Fatalf("envelope-filtered candidates = %#v", candidates)
	}
}

var _ = context.Background

func TestHeldOutBatchCannotDisableDiagnosticIsolation(t *testing.T) {
	world := newSkillEvolutionWorld(t)
	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{Token: contractToken, Reader: world.service(t, world.store), Usage: usage, WriterIdentity: "server"}))
	defer server.Close()
	profile := map[string]any{"schema_version": "context-profile/1.0", "task_family": "code", "runtime_class": "go", "workspace_feature_tags": []any{}, "observable_guard_facts": []any{}, "tool_policy_ref": sevVersionedRef("tool-policy"), "environment_class": "sandboxed"}
	subject := map[string]any{"candidate_ref": map[string]any{"schema_version": "gms.candidate-artifact-ref.v2", "candidate_id": "candidate-held-out", "kind": "step_guidance", "body_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "origin_type": "skill_proposal", "origin_ref": sevVersionedRef("proposal")}}
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionInteractionRecordPath, map[string]any{"request_id": "held-out-false", "subject": subject, "context_profile": profile, "stage": "verified", "evidence_refs": []any{}, "evaluation_batch_id": "held-out", "diagnostic_only": false})
	contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
}
