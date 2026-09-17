package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

func TestUsageSummaryReadIsAuthorizedInteractionOnlyAndAggregateOnly(t *testing.T) {
	world := newSkillEvolutionWorld(t)
	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{Token: contractToken, Reader: world.service(t, world.store), Usage: usage, WriterIdentity: "server-arm-b"}))
	defer server.Close()

	profile := usageProfileDoc("summary-family")
	subject := usageCandidateSubject("candidate-summary")
	internalProfile := usageprojection.ContextProfile{SchemaVersion: "context-profile/1.0", TaskFamily: "summary-family", RuntimeClass: "go-test", WorkspaceFeatureTags: []string{}, ObservableGuardFacts: []string{}, ToolPolicyRef: contract.VersionedRef{ID: "tool-policy", Version: "1", Digest: contract.DigestBytes([]byte("tool-policy"))}, EnvironmentClass: "sandboxed"}
	internalSubject := usageprojection.UsageSubjectRef{CandidateRef: &contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: "candidate-summary", Kind: "step_guidance", BodyDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", OriginType: "skill_proposal", OriginRef: contract.VersionedRef{ID: "proposal-candidate-summary", Version: "1", Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}}
	for _, stage := range []usageprojection.InteractionStage{usageprojection.InteractionAdopted, usageprojection.InteractionVerified} {
		if err := usage.RecordInteraction(context.Background(), usageprojection.Interaction{Subject: internalSubject, ContextProfile: internalProfile, SourceLineageID: "server-arm-b", Stage: stage}); err != nil {
			t.Fatal(err)
		}
	}
	if err := usage.RecordInteraction(context.Background(), usageprojection.Interaction{Subject: internalSubject, ContextProfile: internalProfile, SourceLineageID: "held-out", Stage: usageprojection.InteractionOutcomeCorrelated, DiagnosticOnly: true}); err != nil {
		t.Fatal(err)
	}
	if err := usage.RecordDiagnosis(context.Background(), usageprojection.DiagnosisUtilityAssessment{AssessmentID: "diagnosis-private", Subject: internalSubject, ContextProfile: internalProfile, SourceLineageID: "diagnostician", ReturnedPathID: "private-path", AddressedAgentID: "private-agent", ContributionScoreMicros: 999999, ConfidenceMicros: 999999, Rationale: "private rationale must never leave aggregate", EvidenceRefs: []contract.EvidenceRef{{SchemaVersion: contract.SchemaEvidenceRef, EvidenceID: "private-evidence", Version: "1", EvidenceDigest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", CommitState: "committed", EvidenceKind: "observation"}}, RubricRef: contract.VersionedRef{ID: "rubric", Version: "1", Digest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}}); err != nil {
		t.Fatal(err)
	}

	unauthorized, err := http.NewRequest(http.MethodPost, server.URL+httpapi.SkillEvolutionUsageSummaryReadPath, strings.NewReader(`{"schema_version":"gms.usage-summary-read.v1"}`))
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(unauthorized)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	contractRequireError(t, response.StatusCode, response.Header, body, http.StatusUnauthorized, "UNAUTHORIZED")

	status, header, payload := contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionUsageSummaryReadPath, map[string]any{"schema_version": "gms.usage-summary-read.v1", "request_id": "summary-1", "context_profile": profile, "subject_refs": []any{subject}})
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	out := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, out, "schema_version", "request_id", "context_profile_digest", "usage_policy_version", "diagnostic_policy", "authority_tier_aggregates", "entries")
	if out["schema_version"] != "gms.usage-summary-read.v1" || out["diagnostic_policy"] != "interaction_only" {
		t.Fatalf("summary envelope = %#v", out)
	}
	entries, ok := out["entries"].([]any)
	if !ok || len(entries) != 1 {
		t.Fatalf("entries = %#v", out["entries"])
	}
	entry := entries[0].(map[string]any)
	contractRequireExactKeys(t, entry, "authority_tier", "subject_ref", "independent_context_count", "independent_lineage_count", "adopted_count", "verified_count", "outcome_correlated_count", "deduplicated_reuse_count")
	if entry["authority_tier"] != "advisory" || entry["adopted_count"].(json.Number).String() != "1" || entry["verified_count"].(json.Number).String() != "1" || entry["outcome_correlated_count"].(json.Number).String() != "0" {
		t.Fatalf("entry aggregates = %#v", entry)
	}
	for _, forbidden := range []string{"\"rationale\"", "\"evidence_refs\"", "\"assessment_id\"", "\"returned_path_id\"", "\"addressed_agent_id\"", "\"contribution_score_micros\"", "\"confidence_micros\"", "\"guidance\""} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("aggregate response leaks %q: %s", forbidden, payload)
		}
	}
}

func usageProfileDoc(family string) map[string]any {
	return map[string]any{"schema_version": "context-profile/1.0", "task_family": family, "runtime_class": "go-test", "workspace_feature_tags": []any{}, "observable_guard_facts": []any{}, "tool_policy_ref": sevVersionedRef("tool-policy"), "environment_class": "sandboxed"}
}
func usageCandidateSubject(id string) map[string]any {
	return map[string]any{"candidate_ref": map[string]any{"schema_version": "gms.candidate-artifact-ref.v2", "candidate_id": id, "kind": "step_guidance", "body_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "origin_type": "skill_proposal", "origin_ref": sevVersionedRef("proposal-" + id)}}
}

func TestUsageSummaryReadRejectsNonCanonicalContextProfile(t *testing.T) {
	world := newSkillEvolutionWorld(t)
	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{Token: contractToken, Reader: world.service(t, world.store), Usage: usage, WriterIdentity: "server-arm-b"}))
	defer server.Close()
	profile := usageProfileDoc("strict")
	profile["workspace_feature_tags"] = "not-an-array"
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionUsageSummaryReadPath, map[string]any{"schema_version": "gms.usage-summary-read.v1", "request_id": "summary-strict", "context_profile": profile, "subject_refs": []any{usageCandidateSubject("candidate-strict")}})
	contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
}
