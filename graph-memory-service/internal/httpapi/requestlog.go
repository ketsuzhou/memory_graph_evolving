package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// JSONLogger writes one JSON object per line to a sink. It is the service's
// operational event channel: protocol handling never depends on it, and every
// method tolerates a nil logger so wiring mistakes degrade to silence instead
// of panics.
type JSONLogger struct {
	mu sync.Mutex
	w  io.Writer
}

func NewJSONLogger(sink io.Writer) *JSONLogger {
	return &JSONLogger{w: sink}
}

// Log emits {"ts":..., "event":event, ...fields}. Marshal failures and write
// errors are dropped: logging must never turn into a request failure.
func (l *JSONLogger) Log(event string, fields map[string]any) {
	if l == nil || l.w == nil {
		return
	}
	entry := make(map[string]any, len(fields)+2)
	entry["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	entry["event"] = event
	for key, value := range fields {
		entry[key] = value
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(append(line, '\n'))
}

// RequestLogger wraps a handler so every request produces one JSON line:
// method, route, status, duration, and byte sizes, plus endpoint-specific
// aggregates (recall item counts and scores, evidence batch shapes, error
// codes). Response and request bodies are never logged verbatim — only
// bounded aggregates and, for recalls, a query prefix short enough to
// correlate with caller-side logs.
func RequestLogger(log *JSONLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The bench harness polls /healthz before the tenant exists; those
			// probes are infrastructure, not protocol traffic.
			if r.URL.Path == "/healthz" {
				next.ServeHTTP(w, r)
				return
			}
			started := time.Now()
			body, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))

			recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(recorder, r)
			// The recorder intercepts WriteHeader without delegating, so
			// the real writer gets the status exactly once, here — whether
			// the handler set it explicitly or left the 200 default.
			w.WriteHeader(recorder.status)
			if recorder.body.Len() > 0 {
				_, _ = w.Write(recorder.body.Bytes())
			}

			fields := map[string]any{
				"method":      r.Method,
				"path":        r.URL.Path,
				"status":      recorder.status,
				"duration_ms": roundMillis(time.Since(started)),
				"req_bytes":   len(body),
				"resp_bytes":  recorder.body.Len(),
			}
			addEndpointFields(fields, r.Method, r.URL.Path, body, recorder.body.Bytes())
			log.Log("http_request", fields)
		})
	}
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
	body   bytes.Buffer
}

func (rr *responseRecorder) WriteHeader(status int) {
	if rr.wrote {
		return
	}
	rr.status = status
	rr.wrote = true
}

func (rr *responseRecorder) Write(p []byte) (int, error) {
	if !rr.wrote {
		rr.WriteHeader(rr.status)
	}
	rr.body.Write(p)
	return len(p), nil
}

func roundMillis(duration time.Duration) float64 {
	return float64(duration.Microseconds()) / 1000
}

// addEndpointFields extracts bounded, endpoint-specific aggregates from the
// decoded request and response bodies. Malformed JSON contributes nothing —
// the base fields still describe the request.
func addEndpointFields(fields map[string]any, method, path string, reqBody, respBody []byte) {
	var request, response map[string]any
	if len(reqBody) > 0 && reqBody[0] == '{' {
		_ = json.Unmarshal(reqBody, &request)
	}
	if len(respBody) > 0 && respBody[0] == '{' {
		_ = json.Unmarshal(respBody, &response)
	}
	if code := errorString(response, "code"); code != "" {
		fields["error_code"] = code
	}
	switch {
	case method == http.MethodPost && path == "/v1/recalls":
		if request != nil {
			query, _ := request["query"].(string)
			fields["recall_request_id"] = stringOf(request, "request_id")
			fields["recall_spaces"] = len(arrayOf(request, "space_ids"))
			fields["recall_max_results"] = numberField(request, "max_results")
			fields["recall_query_bytes"] = len(query)
			fields["recall_query_prefix"] = bytePrefix(query, 240)
		}
		items := arrayOf(response, "items")
		fields["recall_items"] = len(items)
		if degradation, ok := response["degradation"].(map[string]any); ok {
			fields["recall_state"] = stringOf(degradation, "state")
		}
		scores := make([]float64, 0, len(items))
		for _, item := range items {
			if object, ok := item.(map[string]any); ok {
				scores = append(scores, numberField(object, "score"))
			}
		}
		fields["recall_scores"] = scores
	case method == http.MethodPost && path == "/v1/evidence-batches:stage":
		if request != nil {
			fields["batch_id"] = stringOf(request, "batch_id")
			fields["space_id"] = stringOf(request, "space_id")
			fields["source_segment_id"] = stringOf(request, "source_segment_id")
			fields["events"] = len(arrayOf(request, "events"))
			fields["links"] = len(arrayOf(request, "links"))
			fields["terminal_outcome"] = stringOf(request, "terminal_outcome")
		}
		fields["state"] = stringOf(response, "state")
		fields["duplicate"] = boolOf(response, "duplicate")
	case method == http.MethodPost && strings.HasPrefix(path, "/v1/evidence-batches/") && strings.HasSuffix(path, ":commit"):
		fields["batch_id"] = strings.TrimSuffix(strings.TrimPrefix(path, "/v1/evidence-batches/"), ":commit")
		fields["memory_version"] = numberField(response, "memory_version")
		fields["duplicate"] = boolOf(response, "duplicate")
	case method == http.MethodGet && strings.HasPrefix(path, "/v1/evidence-batches/"):
		fields["batch_id"] = strings.TrimPrefix(path, "/v1/evidence-batches/")
		fields["state"] = stringOf(response, "state")
	case method == http.MethodPost && (strings.HasPrefix(path, "/v1/explorations")):
		fields["session_state"] = stringOf(response, "state")
		fields["items"] = len(arrayOf(response, "items"))
		fields["remaining_steps"] = numberField(response, "remaining_steps")
	case method == http.MethodPut && path == "/v1/tenant",
		method == http.MethodPost && (path == "/v1/principals" || path == "/v1/spaces" || path == "/v1/grants"):
		fields["duplicate"] = boolOf(response, "duplicate")
	}
}

func stringOf(object map[string]any, key string) string {
	value, _ := object[key].(string)
	return value
}

func errorString(object map[string]any, key string) string {
	nested, ok := object["error"].(map[string]any)
	if !ok {
		return ""
	}
	return stringOf(nested, key)
}

func arrayOf(object map[string]any, key string) []any {
	value, _ := object[key].([]any)
	return value
}

func numberField(object map[string]any, key string) float64 {
	value, _ := object[key].(float64)
	return value
}

func boolOf(object map[string]any, key string) bool {
	value, _ := object[key].(bool)
	return value
}

func bytePrefix(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
