package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/recall"
	"river2.dev/graph-memory-service/internal/store/memory"
)

const tracerToken = "deterministic-tracer-token"

type tracerClock struct {
	now time.Time
}

func (c tracerClock) Now() time.Time {
	return c.now
}

type tracerClient struct {
	t      *testing.T
	base   string
	client *http.Client
}

func (c tracerClient) json(method, path string, requestBody, responseBody any, wantStatus int) {
	c.t.Helper()

	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			c.t.Fatalf("encode %s %s request: %v", method, path, err)
		}
		body = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		c.t.Fatalf("create %s %s request: %v", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+tracerToken)
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		c.t.Fatalf("send %s %s request: %v", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != wantStatus {
		payload, _ := io.ReadAll(resp.Body)
		c.t.Fatalf("%s %s status = %d, want %d; body: %s", method, path, resp.StatusCode, wantStatus, payload)
	}
	if responseBody == nil {
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(responseBody); err != nil {
		c.t.Fatalf("decode %s %s response: %v", method, path, err)
	}
}

type stageResponse struct {
	BatchID        string `json:"batch_id"`
	IdempotencyKey string `json:"idempotency_key"`
	SpaceID        string `json:"space_id"`
	State          string `json:"state"`
	Duplicate      bool   `json:"duplicate"`
}

type commitResponse struct {
	BatchID       string `json:"batch_id"`
	CommitID      string `json:"commit_id"`
	State         string `json:"state"`
	MemoryVersion int64  `json:"memory_version"`
	CommittedAt   string `json:"committed_at"`
	Duplicate     bool   `json:"duplicate"`
}

type batchStatusResponse struct {
	BatchID         string  `json:"batch_id"`
	IdempotencyKey  string  `json:"idempotency_key"`
	SpaceID         string  `json:"space_id"`
	SourceSegmentID string  `json:"source_segment_id"`
	State           string  `json:"state"`
	MemoryVersion   *int64  `json:"memory_version"`
	CommittedAt     *string `json:"committed_at"`
}

type recallResponse struct {
	RequestID   string       `json:"request_id"`
	Items       []recallItem `json:"items"`
	Degradation struct {
		State   string   `json:"state"`
		Reasons []string `json:"reasons"`
	} `json:"degradation"`
}

type recallItem struct {
	Content       string `json:"content"`
	SourceSpaceID string `json:"source_space_id"`
	MemoryVersion int64  `json:"memory_version"`
	Citation      struct {
		CitationID      string   `json:"citation_id"`
		EvidenceBatchID string   `json:"evidence_batch_id"`
		EventIDs        []string `json:"event_ids"`
	} `json:"citation"`
	Score float64 `json:"score"`
}

func TestGMSTracerRecallsFirstTurnSharedAndPrivateEvidenceWithoutCrossSpaceLeakage(t *testing.T) {
	clock := tracerClock{now: time.Date(2026, time.September, 8, 11, 0, 0, 123456789, time.UTC)}
	store := memory.New()
	handler := httpapi.NewHandler(httpapi.Dependencies{
		Token:       tracerToken,
		Registry:    store,
		Evidence:    evidence.New(store, store, clock),
		Recall:      recall.New(store, store, clock),
		Exploration: exploration.New(store, store, store, clock),
		Clock:       clock,
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	api := tracerClient{t: t, base: server.URL, client: server.Client()}

	const (
		tenantID       = "tenant-tracer"
		hostPrincipal  = "host-service"
		ownerPrincipal = "agent-owner"
		sharedSpace    = "room-shared"
		privateSpace   = "agent-private"
		segmentID      = "opaque-host-segment-first-turn"
		sharedBatch    = "batch-first-turn-shared"
		privateBatch   = "batch-first-turn-private"
		sharedEvent    = "shared-event-1"
		privateEvent   = "private-event-1"
		sharedFact     = "The team agreed that launch is Friday."
		privateFact    = "The owner privately noted a budget risk."
	)

	api.json(http.MethodPut, "/v1/tenant", map[string]any{
		"tenant_id":              tenantID,
		"display_name":           "Tracer tenant",
		"bootstrap_principal_id": hostPrincipal,
	}, nil, http.StatusCreated)
	api.json(http.MethodPost, "/v1/principals", map[string]any{
		"principal_id": ownerPrincipal,
		"kind":         "agent",
		"display_name": "Owner agent",
	}, nil, http.StatusCreated)
	api.json(http.MethodPost, "/v1/spaces", map[string]any{
		"space_id":           sharedSpace,
		"scope":              "shared",
		"owner_principal_id": nil,
		"display_name":       "Room shared",
	}, nil, http.StatusCreated)
	api.json(http.MethodPost, "/v1/spaces", map[string]any{
		"space_id":           privateSpace,
		"scope":              "private",
		"owner_principal_id": ownerPrincipal,
		"display_name":       "Owner private",
	}, nil, http.StatusCreated)
	api.json(http.MethodPost, "/v1/grants", map[string]any{
		"grant_id":     "host-lifecycle",
		"principal_id": hostPrincipal,
		"space_ids":    []string{sharedSpace, privateSpace},
		"purpose":      "lifecycle",
		"operations":   []string{"evidence.stage", "evidence.commit", "recall"},
		"expires_at":   clock.now.Add(time.Hour).Format(time.RFC3339Nano),
	}, nil, http.StatusCreated)

	capturedAt := clock.now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	occurredAt := clock.now.Add(-time.Minute).Format(time.RFC3339Nano)
	sharedStage := stageEvidenceRequest(sharedBatch, "outbox-shared", sharedSpace, segmentID, "room_shared", sharedEvent, sharedFact, capturedAt, occurredAt, strings.Repeat("a", 64))
	privateStage := stageEvidenceRequest(privateBatch, "outbox-private", privateSpace, segmentID, "agent_private", privateEvent, privateFact, capturedAt, occurredAt, strings.Repeat("b", 64))

	var stagedShared stageResponse
	api.json(http.MethodPost, "/v1/evidence-batches:stage", sharedStage, &stagedShared, http.StatusCreated)
	assertStageResponse(t, stagedShared, sharedBatch, "outbox-shared", sharedSpace)
	var stagedPrivate stageResponse
	api.json(http.MethodPost, "/v1/evidence-batches:stage", privateStage, &stagedPrivate, http.StatusCreated)
	assertStageResponse(t, stagedPrivate, privateBatch, "outbox-private", privateSpace)

	var committedShared commitResponse
	api.json(http.MethodPost, "/v1/evidence-batches/"+sharedBatch+":commit", map[string]any{
		"commit_id": "commit-shared",
	}, &committedShared, http.StatusOK)
	assertCommitResponse(t, committedShared, sharedBatch, "commit-shared", clock.now)
	var committedPrivate commitResponse
	api.json(http.MethodPost, "/v1/evidence-batches/"+privateBatch+":commit", map[string]any{
		"commit_id": "commit-private",
	}, &committedPrivate, http.StatusOK)
	assertCommitResponse(t, committedPrivate, privateBatch, "commit-private", clock.now)

	var sharedStatus, privateStatus batchStatusResponse
	api.json(http.MethodGet, "/v1/evidence-batches/"+sharedBatch, nil, &sharedStatus, http.StatusOK)
	api.json(http.MethodGet, "/v1/evidence-batches/"+privateBatch, nil, &privateStatus, http.StatusOK)
	assertCommittedStatus(t, sharedStatus, sharedBatch, "outbox-shared", sharedSpace, segmentID)
	assertCommittedStatus(t, privateStatus, privateBatch, "outbox-private", privateSpace, segmentID)
	if sharedStatus.SpaceID == privateStatus.SpaceID {
		t.Fatalf("same source_segment_id collapsed distinct source spaces: shared=%#v private=%#v", sharedStatus, privateStatus)
	}

	var secondTurn recallResponse
	api.json(http.MethodPost, "/v1/recalls", recallRequest("second-turn-recall", []string{sharedSpace, privateSpace}), &secondTurn, http.StatusOK)
	if secondTurn.RequestID != "second-turn-recall" {
		t.Errorf("recall request_id = %q, want second-turn-recall", secondTurn.RequestID)
	}
	if secondTurn.Degradation.State != "complete" || len(secondTurn.Degradation.Reasons) != 0 {
		t.Errorf("second-turn degradation = %#v, want complete with no reasons", secondTurn.Degradation)
	}
	assertCitedEvidence(t, secondTurn.Items, map[string]expectedEvidence{
		sharedBatch:  {spaceID: sharedSpace, eventID: sharedEvent, content: sharedFact},
		privateBatch: {spaceID: privateSpace, eventID: privateEvent, content: privateFact},
	})

	// Exact-Space probes prove that correlation through source_segment_id never
	// widens a read from one projection into the other projection.
	var sharedOnly recallResponse
	api.json(http.MethodPost, "/v1/recalls", recallRequest("shared-scope-probe", []string{sharedSpace}), &sharedOnly, http.StatusOK)
	assertCitedEvidence(t, sharedOnly.Items, map[string]expectedEvidence{
		sharedBatch: {spaceID: sharedSpace, eventID: sharedEvent, content: sharedFact},
	})
	assertAbsentEvidence(t, sharedOnly.Items, privateSpace, privateBatch, privateFact)

	var privateOnly recallResponse
	api.json(http.MethodPost, "/v1/recalls", recallRequest("private-scope-probe", []string{privateSpace}), &privateOnly, http.StatusOK)
	assertCitedEvidence(t, privateOnly.Items, map[string]expectedEvidence{
		privateBatch: {spaceID: privateSpace, eventID: privateEvent, content: privateFact},
	})
	assertAbsentEvidence(t, privateOnly.Items, sharedSpace, sharedBatch, sharedFact)
}

func stageEvidenceRequest(batchID, key, spaceID, segmentID, sourceKind, eventID, content, capturedAt, occurredAt, hash string) map[string]any {
	return map[string]any{
		"batch_id":          batchID,
		"idempotency_key":   key,
		"space_id":          spaceID,
		"stream_id":         "stream-" + spaceID,
		"source_segment_id": segmentID,
		"provenance": map[string]any{
			"host_type":        "pi-group-chat-host",
			"host_instance_id": "host-instance-1",
			"source_kind":      sourceKind,
			"captured_at":      capturedAt,
			"content_sha256":   hash,
		},
		"events": []map[string]any{{
			"event_id":    eventID,
			"sequence":    1,
			"kind":        "room_message",
			"content":     content,
			"occurred_at": occurredAt,
		}},
		"links":            []map[string]any{},
		"terminal_outcome": "settled",
	}
}

func recallRequest(requestID string, spaceIDs []string) map[string]any {
	return map[string]any{
		"request_id":  requestID,
		"query":       "What did we learn in the first turn?",
		"space_ids":   spaceIDs,
		"max_results": 10,
		"deadline_ms": 1000,
	}
}

func assertStageResponse(t *testing.T, got stageResponse, batchID, key, spaceID string) {
	t.Helper()
	if got.BatchID != batchID || got.IdempotencyKey != key || got.SpaceID != spaceID || got.State != "staged" || got.Duplicate {
		t.Fatalf("stage response = %#v, want batch=%q key=%q space=%q state=staged duplicate=false", got, batchID, key, spaceID)
	}
}

func assertCommitResponse(t *testing.T, got commitResponse, batchID, commitID string, committedAt time.Time) {
	t.Helper()
	if got.BatchID != batchID || got.CommitID != commitID || got.State != "committed" || got.MemoryVersion != 1 || got.Duplicate {
		t.Fatalf("commit response = %#v, want batch=%q commit=%q state=committed version=1 duplicate=false", got, batchID, commitID)
	}
	parsed, err := time.Parse(time.RFC3339Nano, got.CommittedAt)
	if err != nil {
		t.Fatalf("commit response committed_at %q is not RFC3339Nano: %v", got.CommittedAt, err)
	}
	if !parsed.Equal(committedAt) {
		t.Fatalf("commit response committed_at = %v, want deterministic clock time %v", parsed, committedAt)
	}
}

func assertCommittedStatus(t *testing.T, got batchStatusResponse, batchID, key, spaceID, segmentID string) {
	t.Helper()
	if got.BatchID != batchID || got.IdempotencyKey != key || got.SpaceID != spaceID || got.SourceSegmentID != segmentID || got.State != "committed" {
		t.Fatalf("batch status = %#v, want batch=%q key=%q space=%q segment=%q state=committed", got, batchID, key, spaceID, segmentID)
	}
	if got.MemoryVersion == nil || *got.MemoryVersion != 1 || got.CommittedAt == nil {
		t.Fatalf("committed batch status lacks immutable commit metadata: %#v", got)
	}
}

type expectedEvidence struct {
	spaceID string
	eventID string
	content string
}

func assertCitedEvidence(t *testing.T, items []recallItem, expected map[string]expectedEvidence) {
	t.Helper()
	if len(items) != len(expected) {
		t.Fatalf("recall returned %d items, want exactly %d: %#v", len(items), len(expected), items)
	}
	seen := make(map[string]bool)
	for _, item := range items {
		want, ok := expected[item.Citation.EvidenceBatchID]
		if !ok {
			t.Errorf("unexpected evidence batch %q in recall item %#v", item.Citation.EvidenceBatchID, item)
			continue
		}
		seen[item.Citation.EvidenceBatchID] = true
		if item.SourceSpaceID != want.spaceID {
			t.Errorf("batch %q source space = %q, want %q", item.Citation.EvidenceBatchID, item.SourceSpaceID, want.spaceID)
		}
		if item.MemoryVersion != 1 {
			t.Errorf("batch %q memory version = %d, want 1", item.Citation.EvidenceBatchID, item.MemoryVersion)
		}
		if item.Citation.CitationID == "" {
			t.Errorf("batch %q has empty citation_id", item.Citation.EvidenceBatchID)
		}
		if len(item.Citation.EventIDs) != 1 || item.Citation.EventIDs[0] != want.eventID {
			t.Errorf("batch %q event IDs = %v, want [%s]", item.Citation.EvidenceBatchID, item.Citation.EventIDs, want.eventID)
		}
		if item.Content != want.content {
			t.Errorf("batch %q content = %q, want %q", item.Citation.EvidenceBatchID, item.Content, want.content)
		}
		if item.Score < 0 || item.Score > 1 {
			t.Errorf("batch %q score = %v, want within [0,1]", item.Citation.EvidenceBatchID, item.Score)
		}
	}
	for batchID := range expected {
		if !seen[batchID] {
			t.Errorf("recall omitted cited evidence batch %q", batchID)
		}
	}
}

func assertAbsentEvidence(t *testing.T, items []recallItem, forbiddenSpace, forbiddenBatch, forbiddenContent string) {
	t.Helper()
	for _, item := range items {
		if item.SourceSpaceID == forbiddenSpace || item.Citation.EvidenceBatchID == forbiddenBatch || strings.Contains(item.Content, forbiddenContent) {
			t.Errorf("exact-Space recall leaked forbidden evidence: forbidden space=%q batch=%q content=%q; item=%s", forbiddenSpace, forbiddenBatch, forbiddenContent, fmt.Sprintf("%#v", item))
		}
	}
}
