package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/dive"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/pattern"
	"river2.dev/graph-memory-service/internal/recall"
	"river2.dev/graph-memory-service/internal/skillproposal"
	"river2.dev/graph-memory-service/internal/store/memory"
)

// curationNumber renders a decoded JSON number for comparison; the contract
// decoder hands out json.Number, not float64.
func curationNumber(t *testing.T, value any) string {
	t.Helper()
	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("value %#v is not a JSON number", value)
	}
	return number.String()
}

// Curation route contract tests (M2 dive read, M4 governed surface). Pattern
// drafts, proposals, and candidates are seeded through the store the way
// server-side pipelines would; the HTTP surface proves authorization,
// judgment wiring, and the 201/200/409 idempotency discipline.

func newCurationServer(t *testing.T) (*httptest.Server, *memory.Store) {
	t.Helper()
	clock := contractClock{now: contractNow}
	store := memory.New()
	candidates := memory.CandidateStore{Store: store}
	handler := httpapi.NewHandler(httpapi.Dependencies{
		Token:       contractToken,
		Registry:    store,
		Evidence:    evidence.New(store, store, clock),
		Recall:      recall.New(store, store, clock),
		Exploration: exploration.New(store, store, store, clock),
		DiveStore:   store,
		DiveJudge:   dive.New(store, dive.NewDeterministicScorer(), clock),
		Patterns:    store,
		Proposals:   store,
		Candidates:  candidates,
		Curation:    skillproposal.New(store, store, store, candidates, clock),
		Clock:       clock,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, store
}

func curationGrant(t *testing.T, server *httptest.Server, grantID, purpose string, operations []string) {
	t.Helper()
	contractCreateGrant(t, server, grantID, purpose, operations, contractNow.Add(2*time.Hour).Format(time.RFC3339Nano))
}

func TestDiveReadReturnsServerJudgedTerminalExploration(t *testing.T) {
	server, _ := newCurationServer(t)
	contractInitializeTenant(t, server)
	contractCreateSharedSpace(t, server)
	curationGrant(t, server, "grant-lifecycle-dive", "lifecycle", []string{"evidence.stage", "evidence.commit", "recall", "dive.read"})
	curationGrant(t, server, "grant-tool-plane", "tool_plane", []string{"exploration.start", "exploration.submit"})

	capturedAt := contractNow.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	occurredAt := contractNow.Add(-time.Minute).Format(time.RFC3339Nano)
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/evidence-batches:stage", map[string]any{
		"batch_id":          "batch-dive",
		"idempotency_key":   "outbox-dive",
		"space_id":          "room-shared",
		"stream_id":         "stream-room-shared",
		"source_segment_id": "segment-dive",
		"provenance": map[string]any{
			"host_type":        "pi-group-chat-host",
			"host_instance_id": "host-instance-1",
			"source_kind":      "room_shared",
			"captured_at":      capturedAt,
			"content_sha256":   strings.Repeat("c", 64),
		},
		"events": []map[string]any{{
			"event_id":    "event-dive-1",
			"sequence":    1,
			"kind":        "room_message",
			"content":     "The deployment window opens Friday.",
			"occurred_at": occurredAt,
		}},
		"links":            []map[string]any{},
		"terminal_outcome": "settled",
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/evidence-batches/batch-dive:commit", map[string]any{"commit_id": "commit-dive"})
	contractRequireStatus(t, status, http.StatusOK, payload)

	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations", map[string]any{
		"request_id": "req-start-dive", "idempotency_key": "start-dive",
		"space_ids": []string{"room-shared"}, "query": "deployment window",
		"max_steps": 3, "max_results": 10,
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	started := contractDecodeObject(t, payload)
	sessionID := started["session_id"].(string)
	items := started["items"].([]any)
	if len(items) == 0 {
		t.Fatalf("exploration start served no items: %#v", started)
	}
	firstItem := items[0].(map[string]any)
	citation := firstItem["citation"].(map[string]any)
	citationID := citation["citation_id"].(string)

	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations/"+sessionID+":submit", map[string]any{
		"operation_id": "op-submit-1", "found": true,
		"summary": "The deployment window opens Friday.", "citation_ids": []string{citationID},
	})
	contractRequireStatus(t, status, http.StatusOK, payload)

	// The judge ran server-side during submit; the read route only reports it.
	status, header, payload = contractRequest(t, server, http.MethodGet, "/v1/explorations/"+sessionID+"/dive?request_id=req-dive-read", "Bearer "+contractToken, "")
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	result := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, result, "request_id", "session_id", "classification", "score", "items", "incomplete", "evaluated_at")
	if result["classification"] != "found" || result["session_id"] != sessionID || result["incomplete"] != false {
		t.Fatalf("dive result = %#v, want found judgment for the submitted session", result)
	}
	score := result["score"].(map[string]any)
	contractRequireExactKeys(t, score, "relevance", "groundedness", "completeness", "overall", "rounds")
	for _, dimension := range []string{"relevance", "groundedness", "completeness", "overall"} {
		if got := curationNumber(t, score[dimension]); got != "1" {
			t.Errorf("score.%s = %s, want 1 for a fully cited found submission", dimension, got)
		}
	}
	judged := result["items"].([]any)
	if len(judged) != 1 {
		t.Fatalf("judged items = %#v, want exactly the served citation", judged)
	}
	judgedItem := judged[0].(map[string]any)
	if judgedItem["citation_id"] != citationID || judgedItem["submitted"] != true || judgedItem["authoritative"] != true {
		t.Errorf("judged item = %#v, want the submitted authoritative citation", judgedItem)
	}
	contractRequireUTCNano(t, "evaluated_at", result["evaluated_at"])

	// An active exploration has no judgment yet and cannot fake one.
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations", map[string]any{
		"request_id": "req-start-active", "idempotency_key": "start-active",
		"space_ids": []string{"room-shared"}, "query": "deployment window",
		"max_steps": 3, "max_results": 10,
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	active := contractDecodeObject(t, payload)
	status, _, payload = contractRequest(t, server, http.MethodGet, "/v1/explorations/"+active["session_id"].(string)+"/dive?request_id=req-dive-active", "Bearer "+contractToken, "")
	contractRequireError(t, status, header, payload, http.StatusConflict, "EXPLORATION_NOT_TERMINAL")

	status, _, payload = contractRequest(t, server, http.MethodGet, "/v1/explorations/session-absent/dive?request_id=req-dive-absent", "Bearer "+contractToken, "")
	contractRequireError(t, status, header, payload, http.StatusNotFound, "EXPLORATION_NOT_FOUND")
}

func TestPatternRoutesReadAndRejectBehindCurationPurpose(t *testing.T) {
	server, store := newCurationServer(t)
	contractInitializeTenant(t, server)
	contractCreateSharedSpace(t, server)

	status, header, payload := contractRequest(t, server, http.MethodGet, "/v1/patterns/pattern-1?space_id=room-shared&request_id=req-pattern", "Bearer "+contractToken, "")
	contractRequireError(t, status, header, payload, http.StatusForbidden, "GRANT_MISSING")

	curationGrant(t, server, "grant-curation-pattern", "curation", []string{"pattern.read", "pattern.reject"})
	draft, err := pattern.DraftPattern(pattern.DraftPatternInput{
		TenantID: "tenant-A", SpaceID: "room-shared", PatternID: "pattern-1",
		Problem: "Repeated miss", Applicability: "Go repositories", RecommendedAction: "Run the focused check",
		Evidence: []pattern.PatternEvidence{
			{EvidenceRef: domain.EvidenceRef{BatchID: "batch-a", EventIDs: []string{"event-a"}}, SourceSpaceID: "room-shared", LineageID: "lineage-a", Strength: pattern.EvidenceRepeat},
			{EvidenceRef: domain.EvidenceRef{BatchID: "batch-b", EventIDs: []string{"event-b"}}, SourceSpaceID: "room-shared", LineageID: "lineage-b", Strength: pattern.EvidenceRepeat},
		},
		Actor: "host-service", CreatedAt: contractNow,
	})
	if err != nil {
		t.Fatalf("draft pattern: %v", err)
	}
	if _, _, err := store.AppendPatternRevision(context.Background(), draft, "pattern-1"); err != nil {
		t.Fatalf("seed pattern draft: %v", err)
	}

	status, header, payload = contractRequest(t, server, http.MethodGet, "/v1/patterns/pattern-1?space_id=room-shared&request_id=req-pattern", "Bearer "+contractToken, "")
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	revision := contractDecodeObject(t, payload)
	if revision["pattern_id"] != "pattern-1" || curationNumber(t, revision["revision"]) != "1" || revision["status"] != "tentative" {
		t.Fatalf("pattern read = %#v, want revision 1 tentative", revision)
	}

	reject := map[string]any{
		"request_id": "req-reject", "space_id": "room-shared", "operation_id": "op-reject-1",
		"expected_revision": 1, "reason": "not general",
	}
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/patterns/pattern-1:reject", reject)
	contractRequireStatus(t, status, http.StatusCreated, payload)
	rejected := contractDecodeObject(t, payload)["pattern"].(map[string]any)
	if rejected["status"] != "rejected" || curationNumber(t, rejected["revision"]) != "2" || rejected["rejection_reason"] != "not general" {
		t.Fatalf("rejected pattern = %#v, want terminal revision 2", rejected)
	}
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/patterns/pattern-1:reject", reject)
	contractRequireStatus(t, status, http.StatusOK, payload)
	if duplicate := contractDecodeObject(t, payload)["duplicate"]; duplicate != true {
		t.Fatalf("reject replay duplicate = %#v, want true", duplicate)
	}
	changed := map[string]any{
		"request_id": "req-reject-2", "space_id": "room-shared", "operation_id": "op-reject-2",
		"expected_revision": 1, "reason": "different reason",
	}
	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/patterns/pattern-1:reject", changed)
	contractRequireError(t, status, header, payload, http.StatusConflict, "PATTERN_REVISION_CONFLICT")

	status, header, payload = contractRequest(t, server, http.MethodGet, "/v1/patterns/pattern-absent?space_id=room-shared&request_id=req-pattern-absent", "Bearer "+contractToken, "")
	contractRequireError(t, status, header, payload, http.StatusNotFound, "PATTERN_NOT_FOUND")
}

func TestProposalAndCandidateGovernedRoutes(t *testing.T) {
	server, store := newCurationServer(t)
	contractInitializeTenant(t, server)
	contractCreateSharedSpace(t, server)
	curationGrant(t, server, "grant-curation-governed", "curation", []string{
		"proposal.read", "candidate.read", "candidate.decide", "candidate.activate",
	})

	ctx := context.Background()
	proposalRecord := domain.SkillProposal{
		ProposalID: "proposal-1", RoundID: "round-1", Fingerprint: "fp-governed-1",
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 1}},
		CandidateID: "candidate-1", CreatedAt: contractNow,
	}
	if _, _, err := store.CompleteProposalRound(ctx, "tenant-A", "room-shared", domain.ProposalRoundOutcome{
		RoundID: "round-1", Proposal: &proposalRecord, PayloadHash: "payload-hash", CompletedAt: contractNow,
	}); err != nil {
		t.Fatalf("seed proposal round: %v", err)
	}
	if _, _, err := store.PutCandidate(ctx, domain.SkillCandidate{
		TenantID: "tenant-A", SpaceID: "room-shared", CandidateID: "candidate-1", ProposalID: "proposal-1",
		Fingerprint: "fp-governed-1", TargetSkillID: "skill-1", BaseArtifactVersion: 4,
		PatternRefs: []domain.PatternRef{{PatternID: "pattern-1", Revision: 1}},
		Status:      domain.CandidateNeedsReview, CreatedAt: contractNow,
	}); err != nil {
		t.Fatalf("seed candidate: %v", err)
	}

	status, header, payload := contractRequest(t, server, http.MethodGet, "/v1/proposals/proposal-1?space_id=room-shared&request_id=req-proposal", "Bearer "+contractToken, "")
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	proposalRead := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, proposalRead, "proposal_id", "round_id", "fingerprint", "pattern_refs", "candidate_id", "created_at")
	if proposalRead["proposal_id"] != "proposal-1" || proposalRead["candidate_id"] != "candidate-1" {
		t.Fatalf("proposal read = %#v", proposalRead)
	}
	status, header, payload = contractRequest(t, server, http.MethodGet, "/v1/proposals/proposal-absent?space_id=room-shared&request_id=req-proposal-absent", "Bearer "+contractToken, "")
	contractRequireError(t, status, header, payload, http.StatusNotFound, "PROPOSAL_NOT_FOUND")

	status, header, payload = contractRequest(t, server, http.MethodGet, "/v1/candidates/candidate-1?space_id=room-shared&request_id=req-candidate", "Bearer "+contractToken, "")
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	candidateRead := contractDecodeObject(t, payload)
	if candidateRead["status"] != "needs_review" || candidateRead["target_skill_id"] != "skill-1" {
		t.Fatalf("candidate read = %#v", candidateRead)
	}

	for _, path := range []string{
		"/v1/candidates/candidate-1:decide",
		"/v1/candidates/candidate-1:activate",
	} {
		status, header, payload = contractJSONRequest(t, server, http.MethodPost, path, map[string]any{"request_id": "req-legacy"})
		contractRequireError(t, status, header, payload, http.StatusNotFound, "NOT_FOUND")
	}
}

func TestCurationRoutesRejectUnknownQueryParameters(t *testing.T) {
	server, _ := newCurationServer(t)
	contractInitializeTenant(t, server)
	contractCreateSharedSpace(t, server)

	status, header, payload := contractRequest(t, server, http.MethodGet, "/v1/patterns/pattern-1?space_id=room-shared&request_id=req&extra=1", "Bearer "+contractToken, "")
	contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")

	status, header, payload = contractRequest(t, server, http.MethodGet, "/v1/explorations/session-1/dive", "Bearer "+contractToken, "")
	contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
}
