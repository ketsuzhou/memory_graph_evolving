package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestLoggerRecordsRecallAggregates(t *testing.T) {
	var sink bytes.Buffer
	logger := NewJSONLogger(&sink)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The body must still be readable downstream after the logger buffered it.
		body, err := ioReadAll(r)
		if err != nil {
			t.Fatalf("downstream body read: %v", err)
		}
		if len(body) == 0 {
			t.Fatal("downstream handler saw an empty body")
		}
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, http.StatusOK, map[string]any{
			"request_id": "recall-1",
			"items": []map[string]any{
				{"content": "alpha", "score": 0.9},
				{"content": "beta", "score": 0.4},
			},
			"degradation": map[string]any{"state": "complete", "reasons": []string{}},
		})
	})
	handler := RequestLogger(logger)(next)

	request := httptest.NewRequest(http.MethodPost, "/v1/recalls", strings.NewReader(
		`{"request_id":"recall-1","query":"how do I parse stdin in go","space_ids":["s-shared","s-private"],"max_results":5,"deadline_ms":500}`,
	))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if content := recorder.Header().Get("Content-Type"); content != "application/json" {
		t.Fatalf("content type %q lost by the recorder", content)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("response body corrupted by the recorder: %v", err)
	}
	if len(response["items"].([]any)) != 2 {
		t.Fatalf("response items lost: %v", response)
	}

	lines := strings.Split(strings.TrimSpace(sink.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one log line, got %d", len(lines))
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	assertField(t, entry, "event", "http_request")
	assertField(t, entry, "path", "/v1/recalls")
	assertField(t, entry, "status", float64(http.StatusOK))
	assertField(t, entry, "recall_request_id", "recall-1")
	assertField(t, entry, "recall_state", "complete")
	assertField(t, entry, "recall_query_prefix", "how do I parse stdin in go")
	if entry["recall_spaces"].(float64) != 2 {
		t.Fatalf("recall_spaces = %v, want 2", entry["recall_spaces"])
	}
	if entry["recall_items"].(float64) != 2 {
		t.Fatalf("recall_items = %v, want 2", entry["recall_items"])
	}
	scores := entry["recall_scores"].([]any)
	if len(scores) != 2 || scores[0].(float64) != 0.9 {
		t.Fatalf("recall_scores = %v, want [0.9 0.4]", entry["recall_scores"])
	}
	// The recalled content itself must stay out of the log.
	if strings.Contains(lines[0], "alpha") || strings.Contains(lines[0], "beta") {
		t.Fatal("recall item content leaked into the access log")
	}
}

func TestRequestLoggerRecordsStageShapeAndErrorCodes(t *testing.T) {
	var sink bytes.Buffer
	logger := NewJSONLogger(&sink)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusCreated, map[string]any{
			"batch_id": "batch-7", "state": "staged", "duplicate": false,
		})
	})
	handler := RequestLogger(logger)(next)

	request := httptest.NewRequest(http.MethodPost, "/v1/evidence-batches:stage", strings.NewReader(
		`{"batch_id":"batch-7","space_id":"s-shared","events":[{"event_id":"e1"},{"event_id":"e2"},{"event_id":"e3"}],"links":[],"terminal_outcome":"settled","provenance":{"source_kind":"room_shared"}}`,
	))
	handler.ServeHTTP(httptest.NewRecorder(), request)

	entry := decodeSingleLine(t, &sink)
	assertField(t, entry, "batch_id", "batch-7")
	assertField(t, entry, "space_id", "s-shared")
	assertField(t, entry, "state", "staged")
	assertField(t, entry, "terminal_outcome", "settled")
	if entry["events"].(float64) != 3 {
		t.Fatalf("events = %v, want 3", entry["events"])
	}
	if entry["duplicate"].(bool) {
		t.Fatal("duplicate = true, want false")
	}

	sink.Reset()
	failing := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusForbidden, "GRANT_MISSING", "no grant", nil)
	})
	RequestLogger(logger)(failing).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/recalls", strings.NewReader(`{}`)))
	entry = decodeSingleLine(t, &sink)
	assertField(t, entry, "error_code", "GRANT_MISSING")
	if entry["status"].(float64) != http.StatusForbidden {
		t.Fatalf("status = %v, want 403", entry["status"])
	}
}

func TestRequestLoggerSkipsHealthz(t *testing.T) {
	var sink bytes.Buffer
	logger := NewJSONLogger(&sink)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	RequestLogger(logger)(next).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if sink.Len() != 0 {
		t.Fatalf("healthz probe produced a log line: %s", sink.String())
	}
}

func TestNilJSONLoggerIsSilent(t *testing.T) {
	var nilLogger *JSONLogger
	nilLogger.Log("event", map[string]any{"k": "v"})
	RequestLogger(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/patterns/x", nil))
}

func ioReadAll(r *http.Request) ([]byte, error) {
	buffer := &bytes.Buffer{}
	if r.Body == nil {
		return nil, nil
	}
	if _, err := buffer.ReadFrom(r.Body); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func decodeSingleLine(t *testing.T, sink *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(sink.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one log line, got %d", len(lines))
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	return entry
}

func assertField(t *testing.T, entry map[string]any, key string, want any) {
	t.Helper()
	value, ok := entry[key]
	if !ok {
		t.Fatalf("log line missing field %q: %v", key, entry)
	}
	if value != want {
		t.Fatalf("field %q = %v (%T), want %v (%T)", key, value, value, want, want)
	}
}
