package httpapi_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// reviewStageBody builds a valid stage request the tests then corrupt.
func reviewStageBody(eventID string) map[string]any {
	capturedAt := contractNow.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	occurredAt := contractNow.Add(-time.Minute).Format(time.RFC3339Nano)
	return map[string]any{
		"batch_id":          "batch-review",
		"idempotency_key":   "outbox-review",
		"space_id":          "room-shared",
		"stream_id":         "stream-room-shared",
		"source_segment_id": "segment-review",
		"provenance": map[string]any{
			"host_type":        "pi-group-chat-host",
			"host_instance_id": "host-instance-review",
			"source_kind":      "room_shared",
			"captured_at":      capturedAt,
			"content_sha256":   strings.Repeat("c", 64),
		},
		"events": []any{map[string]any{
			"event_id":    eventID,
			"sequence":    1,
			"kind":        "room_message",
			"content":     "Review fact.",
			"occurred_at": occurredAt,
		}},
		"links":            []any{},
		"terminal_outcome": "settled",
	}
}

func reviewBootstrap(t *testing.T, server *httptest.Server) {
	t.Helper()
	contractInitializeTenant(t, server)
	status, _, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/spaces", map[string]any{
		"space_id": "room-shared", "scope": "shared", "owner_principal_id": nil, "display_name": "Room shared",
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/spaces", map[string]any{
		"space_id": "other-space", "scope": "shared", "owner_principal_id": nil, "display_name": "Other",
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	// The grant deliberately covers only the lifecycle operations over a
	// different space, so recall over room-shared is unauthorized below.
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/grants", map[string]any{
		"grant_id":     "grant-review",
		"principal_id": "host-service",
		"space_ids":    []string{"other-space"},
		"purpose":      "lifecycle",
		"operations":   []string{"evidence.stage", "evidence.commit", "recall"},
		"expires_at":   contractNow.Add(time.Hour).Format(time.RFC3339Nano),
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
}

// TestReviewStageRejectsUnknownEventField: strict JSON extends to nested
// objects; events[0].tenant_id must be 400 INVALID_REQUEST, not silently
// staged.
func TestReviewStageRejectsUnknownEventField(t *testing.T) {
	server := newContractServer(t)
	reviewBootstrap(t, server)

	body := reviewStageBody("review-event-1")
	body["events"].([]any)[0].(map[string]any)["tenant_id"] = "tenant-A"
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/evidence-batches:stage", body)
	contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
	if !strings.Contains(string(payload), "events[0].tenant_id") {
		t.Errorf("details do not point at events[0].tenant_id: %s", payload)
	}
}

// TestReviewStageRejectsUnknownLinkField: links[i].unexpected is likewise a
// field-level rejection.
func TestReviewStageRejectsUnknownLinkField(t *testing.T) {
	server := newContractServer(t)
	reviewBootstrap(t, server)

	body := reviewStageBody("review-event-2")
	body["links"] = []any{map[string]any{
		"link_id": "link-review", "from_event_id": "review-event-2", "to_event_id": "review-event-2",
		"relation": "mentions", "unexpected": true,
	}}
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/evidence-batches:stage", body)
	contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
	if !strings.Contains(string(payload), "links[0].unexpected") {
		t.Errorf("details do not point at links[0].unexpected: %s", payload)
	}
}

// TestReviewContentTypeEnforced: body-carrying routes accept only
// application/json; text/plain and a missing media type are 400.
func TestReviewContentTypeEnforced(t *testing.T) {
	server := newContractServer(t)
	reviewBootstrap(t, server)

	for name, contentType := range map[string]string{
		"wrong media type": "text/plain",
		"missing header":   "",
	} {
		request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/recalls", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer "+contractToken)
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("%s: perform request: %v", name, err)
		}
		payload, _ := io.ReadAll(response.Body)
		response.Body.Close()
		contractRequireStatus(t, response.StatusCode, http.StatusBadRequest, payload)
		if !strings.Contains(string(payload), "INVALID_REQUEST") {
			t.Errorf("%s: body = %s, want INVALID_REQUEST", name, payload)
		}
	}

	// application/json with a charset parameter stays acceptable.
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/recalls", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build charset request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+contractToken)
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("charset request: %v", err)
	}
	payload, _ := io.ReadAll(response.Body)
	response.Body.Close()
	contractRequireStatus(t, response.StatusCode, http.StatusBadRequest, payload) // strict JSON rejects `{}`, but not with a media-type message
	if strings.Contains(string(payload), "Content-Type") {
		t.Errorf("charset=utf-8 rejected as wrong media type: %s", payload)
	}
}

// TestReviewRecallErrorEchoesRequestID: a 403 raised after a successful decode
// must echo the caller's request_id instead of minting a new one.
func TestReviewRecallErrorEchoesRequestID(t *testing.T) {
	server := newContractServer(t)
	reviewBootstrap(t, server)

	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/recalls", map[string]any{
		"request_id":  "request-echo-1",
		"query":       "launch code",
		"space_ids":   []string{"room-shared"},
		"max_results": 5,
		"deadline_ms": 1000,
	})
	contractRequireError(t, status, header, payload, http.StatusForbidden, "GRANT_MISSING")
	if !strings.Contains(string(payload), "request-echo-1") {
		t.Errorf("error envelope did not echo request-echo-1: %s", payload)
	}
}

// TestReviewPathOpaqueIDsValidated: path batch_id/session_id outside 1-128
// bytes are 400 INVALID_REQUEST, not 404.
func TestReviewPathOpaqueIDsValidated(t *testing.T) {
	server := newContractServer(t)
	reviewBootstrap(t, server)

	long := strings.Repeat("x", 129)
	status, _, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/evidence-batches/"+long+":commit", map[string]any{
		"commit_id": "commit-review",
	})
	contractRequireStatus(t, status, http.StatusBadRequest, payload)

	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations/"+long+":submit", map[string]any{
		"operation_id": "op-review", "found": false, "summary": "", "citation_ids": []string{},
	})
	contractRequireStatus(t, status, http.StatusBadRequest, payload)
}

// TestReviewExplorationStartEchoesRequestIDOnConflict: the typed 409 for a
// reused idempotency key echoes the decoded request_id.
func TestReviewExplorationStartEchoesRequestIDOnConflict(t *testing.T) {
	server := newContractServer(t)
	contractInitializeTenant(t, server)

	status, _, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/spaces", map[string]any{
		"space_id": "room-shared", "scope": "shared", "owner_principal_id": nil, "display_name": "Room shared",
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/grants", map[string]any{
		"grant_id":     "grant-tool-plane",
		"principal_id": "host-service",
		"space_ids":    []string{"room-shared"},
		"purpose":      "tool_plane",
		"operations":   []string{"exploration.start", "exploration.explore", "exploration.redirect", "exploration.submit"},
		"expires_at":   contractNow.Add(time.Hour).Format(time.RFC3339Nano),
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)

	start := map[string]any{
		"request_id": "request-expl-echo", "idempotency_key": "start-expl-echo",
		"space_ids": []string{"room-shared"}, "query": "first wording",
		"max_steps": 3, "max_results": 10,
	}
	status, _, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations", start)
	contractRequireStatus(t, status, http.StatusCreated, payload)

	conflict := start
	conflict["query"] = "changed wording"
	var conflictHeader http.Header
	status, conflictHeader, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations", conflict)
	contractRequireError(t, status, conflictHeader, payload, http.StatusConflict, "IDEMPOTENCY_CONFLICT")
	if !strings.Contains(string(payload), "request-expl-echo") {
		t.Errorf("conflict envelope did not echo request-expl-echo: %s", payload)
	}
}
