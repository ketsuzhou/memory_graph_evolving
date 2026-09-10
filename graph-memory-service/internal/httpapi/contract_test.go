package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMissingAndInvalidAuthenticationReturnExactUnauthorizedEnvelope(t *testing.T) {
	server := newContractServer(t)
	tests := []struct {
		name          string
		authorization string
	}{
		{name: "missing"},
		{name: "wrong token", authorization: "Bearer wrong-token"},
		{name: "wrong scheme", authorization: "Basic " + contractToken},
		{name: "extra bearer material", authorization: "Bearer " + contractToken + " extra"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, header, payload := contractRequest(t, server, http.MethodPost, "/v1/recalls", test.authorization, `{}`)
			contractRequireError(t, status, header, payload, http.StatusUnauthorized, "UNAUTHORIZED")
		})
	}
}

func TestValidBearerTokenAllowsTenantInitialization(t *testing.T) {
	server := newContractServer(t)
	contractInitializeTenant(t, server)
}

func TestStrictJSONRequestValidation(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode string
	}{
		{
			name:     "unknown field",
			body:     `{"tenant_id":"tenant-A","display_name":"Tenant","bootstrap_principal_id":"service-A","unexpected":true}`,
			wantCode: "INVALID_REQUEST",
		},
		{
			name:     "duplicate object key",
			body:     `{"tenant_id":"tenant-A","tenant_id":"tenant-B","display_name":"Tenant","bootstrap_principal_id":"service-A"}`,
			wantCode: "INVALID_JSON",
		},
		{
			name:     "trailing JSON value",
			body:     `{"tenant_id":"tenant-A","display_name":"Tenant","bootstrap_principal_id":"service-A"} {"second":true}`,
			wantCode: "INVALID_JSON",
		},
		{
			name:     "wrong field type",
			body:     `{"tenant_id":17,"display_name":"Tenant","bootstrap_principal_id":"service-A"}`,
			wantCode: "INVALID_JSON",
		},
		{
			name:     "missing required field",
			body:     `{"tenant_id":"tenant-A","bootstrap_principal_id":"service-A"}`,
			wantCode: "INVALID_REQUEST",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newContractServer(t)
			status, header, payload := contractRequest(t, server, http.MethodPut, "/v1/tenant", "Bearer "+contractToken, test.body)
			contractRequireError(t, status, header, payload, http.StatusBadRequest, test.wantCode)
		})
	}
}

func TestOpaqueIDsAreNonEmptyAndLimitedByUTF8ByteLength(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "129 ASCII bytes", id: strings.Repeat("a", 129)},
		{name: "130 UTF-8 bytes despite 65 runes", id: strings.Repeat("é", 65)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newContractServer(t)
			status, header, payload := contractJSONRequest(t, server, http.MethodPut, "/v1/tenant", map[string]any{
				"tenant_id":              test.id,
				"display_name":           "Tenant",
				"bootstrap_principal_id": "service-A",
			})
			contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
		})
	}
}

func TestGrantTimestampsRequireCanonicalUTCRFC3339Nano(t *testing.T) {
	server := newContractServer(t)
	contractInitializeTenant(t, server)
	contractCreateSharedSpace(t, server)

	invalid := []struct {
		name  string
		value string
	}{
		{name: "non UTC offset", value: "2031-01-02T03:04:05.123456789+01:00"},
		{name: "UTC expressed as offset", value: "2031-01-02T03:04:05.123456789+00:00"},
		{name: "not RFC3339", value: "2031-01-02 03:04:05Z"},
	}
	for index, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/grants", map[string]any{
				"grant_id":     "invalid-time-" + string(rune('a'+index)),
				"principal_id": "host-service",
				"space_ids":    []string{"room-shared"},
				"purpose":      "lifecycle",
				"operations":   []string{"recall"},
				"expires_at":   test.value,
			})
			contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
		})
	}

	valid := "2031-01-02T03:04:05.123456789Z"
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/grants", map[string]any{
		"grant_id":     "valid-time",
		"principal_id": "host-service",
		"space_ids":    []string{"room-shared"},
		"purpose":      "lifecycle",
		"operations":   []string{"recall"},
		"expires_at":   valid,
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	response := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, response, "grant_id", "principal_id", "space_ids", "purpose", "operations", "expires_at", "duplicate")
	if got := contractRequireUTCNano(t, "expires_at", response["expires_at"]); !got.Equal(time.Date(2031, 1, 2, 3, 4, 5, 123456789, time.UTC)) {
		t.Errorf("expires_at = %v, want exact nanosecond instant", got)
	}
}

func TestSuccessfulProtocolResponsesContainEveryRequiredField(t *testing.T) {
	server := newContractServer(t)
	contractInitializeTenant(t, server)

	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/principals", map[string]any{
		"principal_id": "agent-A",
		"kind":         "agent",
		"display_name": "Agent A",
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	principal := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, principal, "principal_id", "kind", "duplicate")
	contractRequireOpaqueID(t, "principal_id", principal["principal_id"])
	if principal["kind"] != "agent" || principal["duplicate"] != false {
		t.Errorf("principal response = %#v", principal)
	}

	space := contractCreateSharedSpace(t, server)
	contractRequireExactKeys(t, space, "space_id", "scope", "owner_principal_id", "duplicate")

	expiry := "2031-01-02T03:04:05.123456789Z"
	contractCreateGrant(t, server, "lifecycle-grant", "lifecycle", []string{"evidence.stage", "evidence.commit", "recall"}, expiry)
	contractCreateGrant(t, server, "tool-grant", "tool_plane", []string{"exploration.start", "exploration.explore", "exploration.redirect", "exploration.submit"}, expiry)

	capturedAt := "2029-12-31T23:59:59.987654321Z"
	stageRequest := map[string]any{
		"batch_id":          "batch-A",
		"idempotency_key":   "outbox-A",
		"space_id":          "room-shared",
		"stream_id":         "stream-A",
		"source_segment_id": "opaque-segment-A",
		"provenance": map[string]any{
			"host_type":        "pi-group-chat-host",
			"host_instance_id": "host-A",
			"source_kind":      "room_shared",
			"captured_at":      capturedAt,
			"content_sha256":   strings.Repeat("a", 64),
		},
		"events": []map[string]any{{
			"event_id":    "event-A",
			"sequence":    1,
			"kind":        "room_message",
			"content":     "alpha memory",
			"occurred_at": capturedAt,
		}},
		"links":            []any{},
		"terminal_outcome": "settled",
	}
	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/evidence-batches:stage", stageRequest)
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	stage := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, stage, "batch_id", "idempotency_key", "space_id", "state", "duplicate")
	if stage["batch_id"] != "batch-A" || stage["idempotency_key"] != "outbox-A" || stage["space_id"] != "room-shared" || stage["state"] != "staged" || stage["duplicate"] != false {
		t.Errorf("stage response = %#v", stage)
	}

	status, header, payload = contractRequest(t, server, http.MethodGet, "/v1/evidence-batches/batch-A", "Bearer "+contractToken, "")
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	stagedStatus := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, stagedStatus, "batch_id", "idempotency_key", "space_id", "source_segment_id", "state", "memory_version", "committed_at")
	if stagedStatus["memory_version"] != nil || stagedStatus["committed_at"] != nil {
		t.Errorf("staged status must use explicit nulls: %#v", stagedStatus)
	}

	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/evidence-batches/batch-A:commit", map[string]any{"commit_id": "commit-A"})
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	commit := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, commit, "batch_id", "commit_id", "state", "memory_version", "committed_at", "duplicate")
	if commit["batch_id"] != "batch-A" || commit["commit_id"] != "commit-A" || commit["state"] != "committed" || commit["duplicate"] != false {
		t.Errorf("commit response = %#v", commit)
	}
	if commit["memory_version"] != json.Number("1") {
		t.Errorf("memory_version = %#v, want 1", commit["memory_version"])
	}
	if got := contractRequireUTCNano(t, "committed_at", commit["committed_at"]); !got.Equal(contractNow) {
		t.Errorf("committed_at = %v, want fake clock %v", got, contractNow)
	}

	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/recalls", map[string]any{
		"request_id":  "recall-A",
		"query":       "alpha",
		"space_ids":   []string{"room-shared"},
		"max_results": 10,
		"deadline_ms": 1000,
	})
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	recallResponse := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, recallResponse, "request_id", "items", "degradation")
	if recallResponse["request_id"] != "recall-A" {
		t.Errorf("request_id = %#v, want recall-A", recallResponse["request_id"])
	}
	items := contractRequireRecallItems(t, recallResponse["items"])
	degradation, ok := recallResponse["degradation"].(map[string]any)
	if !ok {
		t.Fatalf("degradation = %#v, want object", recallResponse["degradation"])
	}
	contractRequireExactKeys(t, degradation, "state", "reasons")
	if degradation["state"] != "complete" {
		t.Errorf("degradation.state = %#v, want complete", degradation["state"])
	}
	if _, ok := degradation["reasons"].([]any); !ok {
		t.Errorf("degradation.reasons = %#v, want array", degradation["reasons"])
	}

	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations", map[string]any{
		"request_id":      "exploration-A",
		"idempotency_key": "exploration-key-A",
		"space_ids":       []string{"room-shared"},
		"query":           "alpha",
		"max_steps":       3,
		"max_results":     10,
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	start := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, start, "session_id", "request_id", "state", "pinned_spaces", "items", "remaining_steps", "duplicate")
	sessionID := contractRequireOpaqueID(t, "session_id", start["session_id"])
	if start["request_id"] != "exploration-A" || start["state"] != "active" || start["duplicate"] != false {
		t.Errorf("exploration start response = %#v", start)
	}
	pinned, ok := start["pinned_spaces"].([]any)
	if !ok || len(pinned) != 1 {
		t.Fatalf("pinned_spaces = %#v, want one item", start["pinned_spaces"])
	}
	pinnedSpace, ok := pinned[0].(map[string]any)
	if !ok {
		t.Fatalf("pinned_spaces[0] = %#v, want object", pinned[0])
	}
	contractRequireExactKeys(t, pinnedSpace, "space_id", "memory_version")
	if pinnedSpace["space_id"] != "room-shared" || pinnedSpace["memory_version"] != json.Number("1") {
		t.Errorf("pinned space = %#v", pinnedSpace)
	}
	startItems := contractRequireRecallItems(t, start["items"])
	if len(startItems) == 0 || len(items) == 0 {
		t.Fatal("committed matching evidence must produce recall and exploration items")
	}
	citation := startItems[0]["citation"].(map[string]any)
	citationID := citation["citation_id"].(string)

	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations/"+sessionID+":explore", map[string]any{
		"operation_id":       "explore-A",
		"anchor_citation_id": citationID,
		"relation":           "related",
		"limit":              10,
	})
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	contractRequireStepResponse(t, contractDecodeObject(t, payload), sessionID, "explore-A")

	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations/"+sessionID+":redirect", map[string]any{
		"operation_id":        "redirect-A",
		"query":               "alpha again",
		"anchor_citation_ids": []string{citationID},
		"reason":              "follow related evidence",
	})
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	contractRequireStepResponse(t, contractDecodeObject(t, payload), sessionID, "redirect-A")

	status, header, payload = contractJSONRequest(t, server, http.MethodPost, "/v1/explorations/"+sessionID+":submit", map[string]any{
		"operation_id": "submit-A",
		"found":        true,
		"summary":      "Found alpha memory.",
		"citation_ids": []string{citationID},
	})
	contractRequireStatus(t, status, http.StatusOK, payload)
	contractRequireJSON(t, header)
	submit := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, submit, "session_id", "operation_id", "state", "found", "summary", "citations", "duplicate")
	if submit["session_id"] != sessionID || submit["operation_id"] != "submit-A" || submit["state"] != "submitted" || submit["found"] != true || submit["summary"] != "Found alpha memory." || submit["duplicate"] != false {
		t.Errorf("submit response = %#v", submit)
	}
	citations, ok := submit["citations"].([]any)
	if !ok || len(citations) != 1 {
		t.Fatalf("submit citations = %#v, want one", submit["citations"])
	}
	submittedCitation, ok := citations[0].(map[string]any)
	if !ok {
		t.Fatalf("submit citation = %#v, want object", citations[0])
	}
	contractRequireExactKeys(t, submittedCitation, "citation_id", "source_space_id", "memory_version", "evidence_batch_id")
	contractRequireOpaqueID(t, "citation_id", submittedCitation["citation_id"])
	contractRequireOpaqueID(t, "source_space_id", submittedCitation["source_space_id"])
	contractRequireOpaqueID(t, "evidence_batch_id", submittedCitation["evidence_batch_id"])
}

func contractCreateSharedSpace(t *testing.T, server *httptest.Server) map[string]any {
	t.Helper()
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/spaces", map[string]any{
		"space_id":           "room-shared",
		"scope":              "shared",
		"owner_principal_id": nil,
		"display_name":       "Room shared",
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	response := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, response, "space_id", "scope", "owner_principal_id", "duplicate")
	contractRequireOpaqueID(t, "space_id", response["space_id"])
	if response["scope"] != "shared" || response["owner_principal_id"] != nil || response["duplicate"] != false {
		t.Errorf("space response = %#v", response)
	}
	return response
}

func contractCreateGrant(t *testing.T, server *httptest.Server, grantID, purpose string, operations []string, expiry string) map[string]any {
	t.Helper()
	status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/grants", map[string]any{
		"grant_id":     grantID,
		"principal_id": "host-service",
		"space_ids":    []string{"room-shared"},
		"purpose":      purpose,
		"operations":   operations,
		"expires_at":   expiry,
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	response := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, response, "grant_id", "principal_id", "space_ids", "purpose", "operations", "expires_at", "duplicate")
	contractRequireOpaqueID(t, "grant_id", response["grant_id"])
	contractRequireOpaqueID(t, "principal_id", response["principal_id"])
	contractRequireUTCNano(t, "expires_at", response["expires_at"])
	if response["purpose"] != purpose || response["duplicate"] != false {
		t.Errorf("grant response = %#v", response)
	}
	if _, ok := response["space_ids"].([]any); !ok {
		t.Errorf("space_ids = %#v, want array", response["space_ids"])
	}
	if _, ok := response["operations"].([]any); !ok {
		t.Errorf("operations = %#v, want array", response["operations"])
	}
	return response
}

func contractRequireRecallItems(t *testing.T, raw any) []map[string]any {
	t.Helper()
	array, ok := raw.([]any)
	if !ok {
		t.Fatalf("items = %#v, want array", raw)
	}
	items := make([]map[string]any, 0, len(array))
	for index, value := range array {
		item, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("items[%d] = %#v, want object", index, value)
		}
		contractRequireExactKeys(t, item, "content", "source_space_id", "memory_version", "citation", "score")
		if _, ok := item["content"].(string); !ok {
			t.Errorf("items[%d].content = %#v, want string", index, item["content"])
		}
		contractRequireOpaqueID(t, "source_space_id", item["source_space_id"])
		version, ok := item["memory_version"].(json.Number)
		if !ok || version == "0" || strings.HasPrefix(string(version), "-") {
			t.Errorf("items[%d].memory_version = %#v, want positive integer", index, item["memory_version"])
		}
		citation, ok := item["citation"].(map[string]any)
		if !ok {
			t.Fatalf("items[%d].citation = %#v, want object", index, item["citation"])
		}
		contractRequireExactKeys(t, citation, "citation_id", "evidence_batch_id", "event_ids")
		contractRequireOpaqueID(t, "citation_id", citation["citation_id"])
		contractRequireOpaqueID(t, "evidence_batch_id", citation["evidence_batch_id"])
		eventIDs, ok := citation["event_ids"].([]any)
		if !ok || len(eventIDs) == 0 {
			t.Errorf("items[%d].citation.event_ids = %#v, want non-empty array", index, citation["event_ids"])
		}
		if _, ok := item["score"].(json.Number); !ok {
			t.Errorf("items[%d].score = %#v, want number", index, item["score"])
		}
		items = append(items, item)
	}
	return items
}

func contractRequireStepResponse(t *testing.T, response map[string]any, sessionID, operationID string) {
	t.Helper()
	contractRequireExactKeys(t, response, "session_id", "operation_id", "state", "items", "remaining_steps", "duplicate")
	if response["session_id"] != sessionID || response["operation_id"] != operationID || response["state"] != "active" || response["duplicate"] != false {
		t.Errorf("step response = %#v", response)
	}
	contractRequireRecallItems(t, response["items"])
	if _, ok := response["remaining_steps"].(json.Number); !ok {
		t.Errorf("remaining_steps = %#v, want integer", response["remaining_steps"])
	}
}
