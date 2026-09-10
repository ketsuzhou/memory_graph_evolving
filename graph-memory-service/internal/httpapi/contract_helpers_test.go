package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/recall"
	"river2.dev/graph-memory-service/internal/store/memory"
)

const contractToken = "test-graph-memory-token"

var contractNow = time.Date(2030, time.January, 2, 3, 4, 5, 123456789, time.UTC)

type contractClock struct {
	now time.Time
}

func (c contractClock) Now() time.Time { return c.now }

func newContractServer(t *testing.T) *httptest.Server {
	t.Helper()
	clock := contractClock{now: contractNow}
	store := memory.New()
	handler := httpapi.NewHandler(httpapi.Dependencies{
		Token:       contractToken,
		Registry:    store,
		Evidence:    evidence.New(store, store, clock),
		Recall:      recall.New(store, store, clock),
		Exploration: exploration.New(store, store, store, clock),
		Clock:       clock,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func contractRequest(t *testing.T, server *httptest.Server, method, path, authorization, body string) (int, http.Header, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return response.StatusCode, response.Header, payload
}

func contractJSONRequest(t *testing.T, server *httptest.Server, method, path string, request any) (int, http.Header, []byte) {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return contractRequest(t, server, method, path, "Bearer "+contractToken, string(payload))
}

func contractDecodeObject(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		t.Fatalf("response is not a JSON object: %v; body=%q", err, payload)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("response contains trailing JSON: %q", payload)
	}
	return object
}

func contractRequireJSON(t *testing.T, header http.Header) {
	t.Helper()
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(header.Get("Content-Type"), ";")[0]))
	if mediaType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", header.Get("Content-Type"))
	}
}

func contractRequireStatus(t *testing.T, got, want int, payload []byte) {
	t.Helper()
	if got != want {
		t.Fatalf("status = %d, want %d; body=%q", got, want, payload)
	}
}

func contractRequireExactKeys(t *testing.T, object map[string]any, want ...string) {
	t.Helper()
	got := make([]string, 0, len(object))
	for key := range object {
		got = append(got, key)
	}
	sort.Strings(got)
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("JSON keys = %v, want exactly %v", got, want)
	}
}

func contractRequireOpaqueID(t *testing.T, field string, value any) string {
	t.Helper()
	text, ok := value.(string)
	if !ok {
		t.Fatalf("%s = %#v, want string", field, value)
	}
	if len([]byte(text)) < 1 || len([]byte(text)) > 128 {
		t.Errorf("%s byte length = %d, want 1..128", field, len([]byte(text)))
	}
	return text
}

func contractRequireUTCNano(t *testing.T, field string, value any) time.Time {
	t.Helper()
	text, ok := value.(string)
	if !ok {
		t.Fatalf("%s = %#v, want timestamp string", field, value)
	}
	if !strings.HasSuffix(text, "Z") {
		t.Errorf("%s = %q, want UTC Z suffix", field, text)
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		t.Fatalf("%s = %q, want RFC3339Nano: %v", field, text, err)
	}
	if parsed.Location() != time.UTC {
		t.Errorf("%s location = %v, want UTC", field, parsed.Location())
	}
	if parsed.Format(time.RFC3339Nano) != text {
		t.Errorf("%s = %q, want canonical RFC3339Nano %q", field, text, parsed.Format(time.RFC3339Nano))
	}
	return parsed
}

func contractRequireError(t *testing.T, status int, header http.Header, payload []byte, wantStatus int, wantCode string) {
	t.Helper()
	contractRequireStatus(t, status, wantStatus, payload)
	contractRequireJSON(t, header)
	envelope := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, envelope, "error")
	errorObject, ok := envelope["error"].(map[string]any)
	if !ok {
		t.Fatalf("error = %#v, want object", envelope["error"])
	}
	contractRequireExactKeys(t, errorObject, "code", "message", "request_id", "details")
	if errorObject["code"] != wantCode {
		t.Errorf("error.code = %#v, want %q", errorObject["code"], wantCode)
	}
	message, ok := errorObject["message"].(string)
	if !ok || message == "" {
		t.Errorf("error.message = %#v, want non-empty string", errorObject["message"])
	}
	contractRequireOpaqueID(t, "error.request_id", errorObject["request_id"])
	details, ok := errorObject["details"].([]any)
	if !ok {
		t.Fatalf("error.details = %#v, want array (not null)", errorObject["details"])
	}
	for index, rawDetail := range details {
		detail, ok := rawDetail.(map[string]any)
		if !ok {
			t.Errorf("error.details[%d] = %#v, want object", index, rawDetail)
			continue
		}
		contractRequireExactKeys(t, detail, "field", "reason")
		if _, ok := detail["field"].(string); !ok {
			t.Errorf("error.details[%d].field = %#v, want string", index, detail["field"])
		}
		if reason, ok := detail["reason"].(string); !ok || reason == "" {
			t.Errorf("error.details[%d].reason = %#v, want non-empty string", index, detail["reason"])
		}
	}
}

func contractInitializeTenant(t *testing.T, server *httptest.Server) map[string]any {
	t.Helper()
	status, header, payload := contractJSONRequest(t, server, http.MethodPut, "/v1/tenant", map[string]any{
		"tenant_id":              "tenant-A",
		"display_name":           "Test tenant",
		"bootstrap_principal_id": "host-service",
	})
	contractRequireStatus(t, status, http.StatusCreated, payload)
	contractRequireJSON(t, header)
	response := contractDecodeObject(t, payload)
	contractRequireExactKeys(t, response, "tenant_id", "bootstrap_principal_id", "duplicate")
	if response["tenant_id"] != "tenant-A" || response["bootstrap_principal_id"] != "host-service" || response["duplicate"] != false {
		t.Errorf("initialization response = %#v, want exact created identities and duplicate=false", response)
	}
	contractRequireOpaqueID(t, "tenant_id", response["tenant_id"])
	contractRequireOpaqueID(t, "bootstrap_principal_id", response["bootstrap_principal_id"])
	return response
}
