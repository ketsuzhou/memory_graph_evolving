package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

type fixedCandidateOutcomes struct {
	outcomes []domain.CandidateLifecycleOutcome
}

func (f fixedCandidateOutcomes) CandidateOutcomes(context.Context, []contract.CandidateArtifactRef) ([]domain.CandidateLifecycleOutcome, error) {
	return f.outcomes, nil
}

func TestCandidateOutcomesReadReturnsAuthoritativeLifecycleWithoutUsageInference(t *testing.T) {
	world := newSkillEvolutionWorld(t)
	usage, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageprojection.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ref := contract.CandidateArtifactRef{SchemaVersion: contract.SchemaCandidateArtifactRef, CandidateID: "candidate-life", Kind: "step_guidance", BodyDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", OriginType: "skill_proposal", OriginRef: contract.VersionedRef{ID: "proposal-life", Version: "1", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}
	reader := fixedCandidateOutcomes{outcomes: []domain.CandidateLifecycleOutcome{{CandidateRef: ref, Status: "rejected", DecisionRef: &domain.VersionedArtifactRef{ID: "decision-life", Version: 1, Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}, Reason: "paired_candidate_not_better"}}}
	server := httptest.NewServer(httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{Token: contractToken, Reader: world.service(t, world.store), Usage: usage, Outcomes: reader, WriterIdentity: "server-arm-b"}))
	defer server.Close()
	requestRef := map[string]any{"schema_version": ref.SchemaVersion, "candidate_id": ref.CandidateID, "kind": ref.Kind, "body_digest": ref.BodyDigest, "origin_type": ref.OriginType, "origin_ref": sevVersionedRef(ref.OriginRef.ID)}
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, httpapi.SkillEvolutionCandidateOutcomesReadPath, map[string]any{"request_id": "outcomes-1", "candidate_refs": []any{requestRef}})
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	out := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, out, "schema_version", "request_id", "outcomes")
	outcomes := out["outcomes"].([]any)
	if len(outcomes) != 1 {
		t.Fatalf("outcomes=%#v", outcomes)
	}
	outcome := outcomes[0].(map[string]any)
	contractRequireExactKeys(t, outcome, "candidate_ref", "status", "decision_ref", "reason")
	if outcome["status"] != "rejected" || outcome["reason"] != "paired_candidate_not_better" {
		t.Fatalf("outcome=%#v", outcome)
	}
}
