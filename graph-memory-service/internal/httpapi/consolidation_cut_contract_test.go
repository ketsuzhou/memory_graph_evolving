package httpapi_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/store/memory"
)

// cutComposition is the fully-wired PG-50A composition under test, exposing
// the service and freezer for direct snapshot/restore assertions.
type cutComposition struct {
	server     *httptest.Server
	eventsPath string
	service    *consolidationcut.Service
	freezer    *httpapi.RoomFreezer
	store      *memory.Store
}

// cutContractComposition builds the composition and returns its internals.
func cutContractComposition(t *testing.T, roomSpaces map[string][]string) cutComposition {
	t.Helper()
	store := memory.New()
	eventsPath := filepath.Join(t.TempDir(), "cut-events.jsonl")
	bindings := make(map[string][]domain.SpaceID, len(roomSpaces))
	for room, spaces := range roomSpaces {
		ids := make([]domain.SpaceID, 0, len(spaces))
		for _, space := range spaces {
			ids = append(ids, domain.SpaceID(space))
		}
		bindings[room] = ids
	}
	cutFreezer := httpapi.NewRoomFreezer(store, store, bindings)
	cutService := consolidationcut.NewService(nil, cutFreezer)
	// Mirror production main.go: the shared composition carries the durable
	// guard authority, so every contract trigger is admitted through the
	// committed-ledger receipts proof, not auto-accepted.
	cutService.SetGuardAuthority(httpapi.NewDurableGuardAuthority(store, cutService.GetManifest, bindings))
	root := httpapi.NewHandler(httpapi.Dependencies{
		Token:             contractToken,
		Registry:          store,
		Evidence:          evidence.New(store, store, contractClock{now: contractNow}),
		Clock:             contractClock{now: contractNow},
		ConsolidationCuts: cutService,
		CutFreezer:        cutFreezer,
		CutEvents:         httpapi.NewCutEventNotifier("test-secret", httpapi.NewFileCutEventSink(eventsPath)),
	})
	server := httptest.NewServer(root)
	t.Cleanup(server.Close)
	return cutComposition{
		server: server, eventsPath: eventsPath,
		service: cutService, freezer: cutFreezer, store: store,
	}
}

// cutContractServer builds the composition and returns the server + the
// signed-event sink path (the surface the pure HTTP contract tests need).
func cutContractServer(t *testing.T, roomSpaces map[string][]string) (*httptest.Server, string) {
	t.Helper()
	composition := cutContractComposition(t, roomSpaces)
	return composition.server, composition.eventsPath
}

// cutRequest performs a bearer-authenticated request; content-type is set
// only when body is non-empty (mirroring the empty-body cancel route).
func cutRequest(t *testing.T, server *httptest.Server, method, path, idempotencyKey, body string) (int, http.Header, []byte) {
	t.Helper()
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	request, err := http.NewRequest(method, server.URL+path, payload)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	request.Header.Set("Authorization", "Bearer "+contractToken)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("perform %s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, path, err)
	}
	return response.StatusCode, response.Header, raw
}

func cutDecode(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		t.Fatalf("decode cut response %q: %v", payload, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("response has trailing JSON: %q", payload)
	}
	return object
}

// cutBootstrap wires the tenant, the bound space, the lifecycle grant, and a
// single committed evidence batch on that space so a freeze can seal it.
func cutBootstrap(t *testing.T, server *httptest.Server) {
	t.Helper()
	hostPrincipal := "host-service"
	spaceID := "consolidation-space"
	cutRequest(t, server, http.MethodPut, "/v1/tenant", "", `{
		"tenant_id":"consolidation-tenant",
		"display_name":"Consolidation",
		"bootstrap_principal_id":"`+hostPrincipal+`"
	}`)
	cutRequest(t, server, http.MethodPost, "/v1/spaces", "", `{
		"space_id":"`+spaceID+`",
		"scope":"shared",
		"owner_principal_id":null,
		"display_name":"Consolidation space"
	}`)
	cutRequest(t, server, http.MethodPost, "/v1/grants", "", `{
		"grant_id":"consolidation-lifecycle",
		"principal_id":"`+hostPrincipal+`",
		"space_ids":["`+spaceID+`"],
		"purpose":"lifecycle",
		"operations":["evidence.stage","evidence.commit"],
		"expires_at":"2031-01-01T00:00:00Z"
	}`)
	capturedAt := contractNow.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	occurredAt := contractNow.Add(-time.Minute).Format(time.RFC3339Nano)
	stage := map[string]any{
		"batch_id":          "consolidation-batch-1",
		"idempotency_key":   "stream-key-1",
		"space_id":          spaceID,
		"stream_id":         "stream-" + spaceID,
		"source_segment_id": "consolidation-segment-1",
		"provenance": map[string]any{
			"host_type":        "pi-group-chat-host",
			"host_instance_id": "host-instance-1",
			"source_kind":      "room_shared",
			"captured_at":      capturedAt,
			"content_sha256":   strings.Repeat("a", 64),
		},
		"events": []map[string]any{{
			"event_id":    "consolidation-event-1",
			"sequence":    1,
			"kind":        "room_message",
			"content":     "the room settled on a consolidated fact",
			"occurred_at": occurredAt,
		}},
		"links":            []map[string]any{},
		"terminal_outcome": "settled",
	}
	stageBody, err := json.Marshal(stage)
	if err != nil {
		t.Fatalf("marshal stage: %v", err)
	}
	if status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/evidence-batches:stage", "", string(stageBody)); status != http.StatusCreated {
		t.Fatalf("stage status = %d, want 201; body=%s", status, raw)
	}
	if status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/evidence-batches/consolidation-batch-1:commit", "", `{"commit_id":"consolidation-commit-1"}`); status != http.StatusOK {
		t.Fatalf("commit status = %d, want 200; body=%s", status, raw)
	}
}

const cutSpacePath = "consolidation-space"

func TestConsolidationCutCreateAndGetAuthoritative(t *testing.T) {
	server, _ := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)

	status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-alpha", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusAccepted {
		t.Fatalf("create status = %d, want 202; body=%s", status, raw)
	}
	dto := cutDecode(t, raw)
	cutID, _ := dto["cut_id"].(string)
	if cutID == "" {
		t.Fatalf("create response missing cut_id: %v", dto)
	}
	if dto["tenant_id"] != "consolidation-tenant" || dto["room_id"] != "consolidation-room" {
		t.Fatalf("create DTO identity fields wrong: %v", dto)
	}
	if stage, _ := dto["stage"].(string); stage == "" {
		t.Fatalf("create DTO missing stage: %v", dto)
	}
	if _, ok := dto["space_results"]; !ok {
		t.Fatalf("create DTO missing space_results: %v", dto)
	}
	// The authoritative GET returns the identical job.
	status, _, raw = cutRequest(t, server, http.MethodGet, "/v1/consolidation-cuts/"+cutID, "", "")
	if status != http.StatusOK {
		t.Fatalf("get status = %d, want 200; body=%s", status, raw)
	}
	got := cutDecode(t, raw)
	if got["cut_id"] != cutID {
		t.Fatalf("get cut_id = %v, want %s", got["cut_id"], cutID)
	}
	// Idempotent replay with the same key and identical body returns 202 with
	// the same cut.
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-alpha", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusAccepted {
		t.Fatalf("replay status = %d, want 202; body=%s", status, raw)
	}
	if replayed := cutDecode(t, raw); replayed["cut_id"] != cutID {
		t.Fatalf("replay cut_id = %v, want original %s", replayed["cut_id"], cutID)
	}
	// Same key, same trigger_source, CHANGED mode → an idempotency conflict.
	// (trigger_source participates in the service's composite key, so it is
	// mode that distinguishes a same-key conflict.)
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-alpha", `{"mode":"threshold","trigger_source":"t"}`)
	if status != http.StatusConflict {
		t.Fatalf("idempotency-conflict status = %d, want 409; body=%s", status, raw)
	}
	if errBody := cutDecode(t, raw); errBody["code"] != "CUT_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("idempotency-conflict code = %v, want CUT_IDEMPOTENCY_CONFLICT; body=%s", errBody["code"], raw)
	}
}

func TestConsolidationCutRoomInFlightConflict(t *testing.T) {
	server, _ := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)

	// First freeze leaves a frozen (in-flight) cut on the room.
	status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-one", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusAccepted {
		t.Fatalf("first create status = %d, want 202; body=%s", status, raw)
	}
	first := cutDecode(t, raw)
	firstID, _ := first["cut_id"].(string)
	if firstID == "" {
		t.Fatalf("first create missing cut_id: %v", first)
	}

	// A fresh key on the same room while the first freeze is still in flight is
	// a stale-view conflict, not a replay (SC-4.1 room-serial freeze): GET is
	// authoritative and the caller retries after the in-flight cut settles.
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-two", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusConflict {
		t.Fatalf("in-flight conflict status = %d, want 409; body=%s", status, raw)
	}
	errBody := cutDecode(t, raw)
	if errBody["code"] != "ROOM_EPOCH_STALE" {
		t.Fatalf("in-flight conflict code = %v, want ROOM_EPOCH_STALE; body=%s", errBody["code"], raw)
	}
}

func TestConsolidationCutRejectsBadCreateBodies(t *testing.T) {
	server, _ := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)

	cases := []struct {
		name      string
		headerKey string
		body      string
		wantCode  string
	}{
		{"missing idempotency key", "", `{"mode":"force","trigger_source":"t"}`, "MISSING_MANIFEST_FIELD"},
		{"unknown field", "k1", `{"mode":"force","trigger_source":"t","extra":1}`, "UNKNOWN_CORE_FIELD"},
		{"missing fields", "k2", `{}`, "MISSING_MANIFEST_FIELD"},
		{"invalid mode", "k3", `{"mode":"sideways","trigger_source":"t"}`, "PAYLOAD_VALIDATION_FAILED"},
		{"empty trigger source", "k4", `{"mode":"force","trigger_source":""}`, "PAYLOAD_VALIDATION_FAILED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", tc.headerKey, tc.body)
			if status != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body=%s", status, raw)
			}
			// Errors are the flat two-field envelope, not the wire wrapper.
			decoded := cutDecode(t, raw)
			if code, _ := decoded["code"].(string); code != tc.wantCode {
				t.Fatalf("code = %q, want %q; body=%s", code, tc.wantCode, raw)
			}
			if len(decoded) > 2 {
				got := make([]string, 0, len(decoded))
				for k := range decoded {
					got = append(got, k)
				}
				t.Fatalf("cut error envelope leaked fields %v (want only code+message); body=%s", got, raw)
			}
		})
	}
}

func TestConsolidationCutForbiddenWithoutGrant(t *testing.T) {
	// room-b is bound to a space the acting principal holds no grant on.
	server, _ := cutContractServer(t, map[string][]string{
		"consolidation-room": {cutSpacePath},
		"room-b":             {"ungranted-space"},
	})
	cutBootstrap(t, server)
	// Create the ungranted space so it resolves (exists) but has no covering
	// grant, exercising the GRANT_MISSING 403 path rather than a 404.
	if status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/spaces", "", `{"space_id":"ungranted-space","scope":"shared","owner_principal_id":null,"display_name":"Ungranted"}`); status != http.StatusCreated {
		t.Fatalf("create space status = %d; body=%s", status, raw)
	}
	status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/room-b/consolidation-cuts", "key-forbidden", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", status, raw)
	}
	if code := cutDecode(t, raw)["code"]; code != "GRANT_MISSING" {
		t.Fatalf("code = %v, want GRANT_MISSING; body=%s", code, raw)
	}
}

func TestConsolidationCutRoomWithNoBindingFailsClosed(t *testing.T) {
	// The freezer has no binding for this room; even with a valid body the
	// freeze fails closed at 422.
	server, _ := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)
	status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/unbound-room/consolidation-cuts", "key-no-binding", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", status, raw)
	}
	if code := cutDecode(t, raw)["code"]; code != "MISSING_MANIFEST_FIELD" {
		t.Fatalf("code = %v, want MISSING_MANIFEST_FIELD; body=%s", code, raw)
	}
}

func TestConsolidationCutCancelEmptyBodyCooperative(t *testing.T) {
	server, _ := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)
	status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-cancel", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusAccepted {
		t.Fatalf("create status = %d; body=%s", status, raw)
	}
	cutID := cutDecode(t, raw)["cut_id"].(string)

	// cancel is an EMPTY-BODY POST with no Content-Type; the relaxed gate must
	// let it through and the handler accepts 202.
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/consolidation-cuts/"+cutID+":cancel", "", "")
	if status != http.StatusAccepted {
		t.Fatalf("cancel status = %d, want 202; body=%s", status, raw)
	}
	if stage := cutDecode(t, raw)["stage"]; stage != "cancelling" {
		t.Fatalf("post-cancel stage = %v, want cancelling; body=%s", stage, raw)
	}
	// Any request body on the empty-body route is a contract violation.
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/consolidation-cuts/"+cutID+":cancel", "", `{"unexpected":true}`)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("cancel-with-body status = %d, want 422; body=%s", status, raw)
	}
	if code := cutDecode(t, raw)["code"]; code != "PAYLOAD_VALIDATION_FAILED" {
		t.Fatalf("cancel-with-body code = %v, want PAYLOAD_VALIDATION_FAILED; body=%s", code, raw)
	}
}

func TestConsolidationCutRediagnoseOnlyFromPartiallyFailed(t *testing.T) {
	server, _ := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)
	status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-rediag", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusAccepted {
		t.Fatalf("create status = %d; body=%s", status, raw)
	}
	cutID := cutDecode(t, raw)["cut_id"].(string)
	// The freshly frozen cut is not partially_failed → rediagnose is illegal.
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/consolidation-cuts/"+cutID+":rediagnose", "", `{"diagnosis_policy_revision":"diagnosis-v2"}`)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("rediagnose status = %d, want 422; body=%s", status, raw)
	}
	if code := cutDecode(t, raw)["code"]; code != "PAYLOAD_VALIDATION_FAILED" {
		t.Fatalf("rediagnose code = %v, want PAYLOAD_VALIDATION_FAILED; body=%s", code, raw)
	}
	// Missing revision field.
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/consolidation-cuts/"+cutID+":rediagnose", "", `{}`)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("rediagnose-missing-revision status = %d, want 422; body=%s", status, raw)
	}
	if code := cutDecode(t, raw)["code"]; code != "MISSING_MANIFEST_FIELD" {
		t.Fatalf("rediagnose-missing-revision code = %v, want MISSING_MANIFEST_FIELD; body=%s", code, raw)
	}
}

func TestConsolidationCutGetNotFoundAndUnauthorized(t *testing.T) {
	server, _ := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)

	status, _, raw := cutRequest(t, server, http.MethodGet, "/v1/consolidation-cuts/cut-does-not-exist", "", "")
	if status != http.StatusNotFound {
		t.Fatalf("get-missing status = %d, want 404; body=%s", status, raw)
	}
	if code := cutDecode(t, raw)["code"]; code != "NOT_FOUND" {
		t.Fatalf("get-missing code = %v, want NOT_FOUND; body=%s", code, raw)
	}

	// No bearer token → 401 before any route dispatch.
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/rooms/consolidation-room/consolidation-cuts", strings.NewReader(`{"mode":"force","trigger_source":"t"}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("send unauthenticated: %v", err)
	}
	defer response.Body.Close()
	raw, _ = io.ReadAll(response.Body)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401; body=%s", response.StatusCode, raw)
	}
}

func TestConsolidationCutNotifierWritesSignedJSONL(t *testing.T) {
	const secret = "test-secret"
	server, eventsPath := cutContractServer(t, map[string][]string{"consolidation-room": {cutSpacePath}})
	cutBootstrap(t, server)

	status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-notify", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusAccepted {
		t.Fatalf("create status = %d; body=%s", status, raw)
	}
	firstCutID := cutDecode(t, raw)["cut_id"].(string)

	// Identical create retries are at-least-once notifications: the event ID
	// remains stable so a consumer can deduplicate replay delivery.
	status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-notify", `{"mode":"force","trigger_source":"t"}`)
	if status != http.StatusAccepted || cutDecode(t, raw)["cut_id"] != firstCutID {
		t.Fatalf("replay status/body = %d/%s, want original accepted cut", status, raw)
	}

	lines, err := readCutEventLines(t, eventsPath)
	if err != nil {
		t.Fatalf("read cut events: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("cut event lines = %d, want at-least-once create + replay delivery", len(lines))
	}
	allowed := map[string]bool{
		"event_id": true, "cut_id": true, "stage": true,
		"job_version": true, "ts": true, "signature": true,
	}
	for index, event := range lines {
		if len(event) != len(allowed) {
			t.Fatalf("event %d keys = %#v, want only whitelist", index, event)
		}
		for key := range event {
			if !allowed[key] {
				t.Fatalf("event %d leaked non-notification field %q: %#v", index, key, event)
			}
		}
		cutID, ok := event["cut_id"].(string)
		if !ok || cutID != firstCutID {
			t.Fatalf("event %d cut_id = %#v, want %q", index, event["cut_id"], firstCutID)
		}
		stage, ok := event["stage"].(string)
		if !ok || stage == "" {
			t.Fatalf("event %d stage = %#v", index, event["stage"])
		}
		version, ok := event["job_version"].(float64)
		if !ok || version < 1 {
			t.Fatalf("event %d job_version = %#v", index, event["job_version"])
		}
		ts, ok := event["ts"].(string)
		if !ok || ts == "" {
			t.Fatalf("event %d ts = %#v", index, event["ts"])
		}
		wantEventID := cutID + "@" + stage + "#" + strconv.FormatInt(int64(version), 10)
		if event["event_id"] != wantEventID {
			t.Fatalf("event %d event_id = %#v, want %q", index, event["event_id"], wantEventID)
		}
		if signature, _ := event["signature"].(string); signature != expectedCutEventSignature(secret, cutID, stage, int64(version), ts) {
			t.Fatalf("event %d signature = %q, want recomputed HMAC", index, signature)
		}
	}
	if lines[0]["event_id"] != lines[1]["event_id"] {
		t.Fatalf("replayed notification event IDs differ: %q vs %q", lines[0]["event_id"], lines[1]["event_id"])
	}
}

type failingCutEventSink struct{}

func (failingCutEventSink) AppendLine([]byte) error {
	return errors.New("event sink unavailable")
}

func expectedCutEventSignature(secret, cutID, stage string, jobVersion int64, ts string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(cutID))
	mac.Write([]byte("|"))
	mac.Write([]byte(stage))
	mac.Write([]byte("|"))
	mac.Write([]byte(strconv.FormatInt(jobVersion, 10)))
	mac.Write([]byte("|"))
	mac.Write([]byte(ts))
	return hex.EncodeToString(mac.Sum(nil))
}

func TestCutEventNotifierLogsSinkFailureWithoutChangingAuthority(t *testing.T) {
	var log bytes.Buffer
	notifier := httpapi.NewCutEventNotifierWithLogger(
		"test-secret",
		failingCutEventSink{},
		httpapi.NewJSONLogger(&log),
	)
	notifier.Notify("cut-notify-failure", consolidationcut.StageFrozen, 3, contractNow)

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(log.Bytes()), &entry); err != nil {
		t.Fatalf("decode sink failure log: %v; raw=%q", err, log.String())
	}
	allowed := map[string]bool{
		"ts": true, "event": true, "cut_id": true, "stage": true,
		"job_version": true, "event_id": true, "error": true,
	}
	if len(entry) != len(allowed) || entry["event"] != "cut_event_sink_error" {
		t.Fatalf("sink failure log = %#v", entry)
	}
	for key := range entry {
		if !allowed[key] {
			t.Fatalf("sink failure log leaked field %q: %#v", key, entry)
		}
	}
	if entry["event_id"] != "cut-notify-failure@frozen#3" || entry["error"] != "event sink unavailable" {
		t.Fatalf("sink failure log facts = %#v", entry)
	}
}

// readCutEventLines decodes each JSONL cut-event line in order. A missing
// file is treated as an empty event stream.
func readCutEventLines(t *testing.T, path string) ([]map[string]any, error) {
	t.Helper()
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var lines []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, err
		}
		lines = append(lines, event)
	}
	return lines, scanner.Err()
}

// phantomSegmentFreezer appends a structurally self-consistent receipt for a
// segment that has no committed batches. Local structural validation passes;
// only the durable guard authority's independent re-derivation from the
// committed evidence ledger can reject it — the production untrusted-freezer
// threat model the durable authority exists to close.
type phantomSegmentFreezer struct{ inner *httpapi.RoomFreezer }

func (f *phantomSegmentFreezer) Freeze(ctx context.Context, req consolidationcut.TriggerRequest) (consolidationcut.Manifest, error) {
	manifest, err := f.inner.Freeze(ctx, req)
	if err != nil {
		return consolidationcut.Manifest{}, err
	}
	phantom := consolidationcut.SegmentID("segment-phantom-001")
	manifest.SealedSegmentIDs = append(manifest.SealedSegmentIDs, phantom)
	manifest.EvidenceCommitReceipts = append(manifest.EvidenceCommitReceipts, consolidationcut.EvidenceCommitReceipt{
		ReceiptID:   consolidationcut.ReceiptID("receipt-" + phantom),
		SegmentID:   phantom,
		BatchIDs:    []domain.BatchID{"batch-phantom-001"},
		BatchDigest: "sha256:" + strings.Repeat("00", 32),
	})
	return manifest, nil
}

// The production durable authority wired into the composition must admit or
// fail the initial frozen edge: an unverifiable receipts proof surfaces as
// 422 RECEIPT_MISSING, the authoritative failed job stays readable via GET,
// and the idempotency binding replays the failed job instead of re-freezing.
func TestConsolidationCutDurableAuthorityRejectsUnverifiableReceipts(t *testing.T) {
	store := memory.New()
	realFreezer := httpapi.NewRoomFreezer(store, store, map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath}})
	cutService := consolidationcut.NewService(nil, &phantomSegmentFreezer{inner: realFreezer})
	cutService.SetGuardAuthority(httpapi.NewDurableGuardAuthority(store, cutService.GetManifest, map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath}}))
	root := httpapi.NewHandler(httpapi.Dependencies{
		Token:             contractToken,
		Registry:          store,
		Evidence:          evidence.New(store, store, contractClock{now: contractNow}),
		Clock:             contractClock{now: contractNow},
		ConsolidationCuts: cutService,
		CutFreezer:        realFreezer,
		CutEvents:         httpapi.NewCutEventNotifier("test-secret", httpapi.NewFileCutEventSink(filepath.Join(t.TempDir(), "cut-events.jsonl"))),
	})
	server := httptest.NewServer(root)
	t.Cleanup(server.Close)
	cutBootstrap(t, server)

	const createPath = "/v1/rooms/consolidation-room/consolidation-cuts"
	createBody := `{"mode":"force","trigger_source":"t"}`
	status, _, raw := cutRequest(t, server, http.MethodPost, createPath, "key-durable-reject", createBody)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("create with unverifiable receipts status = %d, want 422; body=%s", status, raw)
	}
	if body := cutDecode(t, raw); body["code"] != "RECEIPT_MISSING" {
		t.Fatalf("create rejection code = %#v, want RECEIPT_MISSING", body["code"])
	}

	// The failed job is authoritative: GET reads it with RECEIPT_MISSING.
	var failedID consolidationcut.CutID
	for _, entry := range cutService.AuditTrail("consolidation-tenant") {
		if entry.Kind == "trigger_frozen_rejected" {
			failedID = entry.CutID
		}
	}
	if failedID == "" {
		t.Fatal("frozen-guard rejection was not audited as trigger_frozen_rejected")
	}
	status, _, raw = cutRequest(t, server, http.MethodGet, "/v1/consolidation-cuts/"+string(failedID), "", "")
	if status != http.StatusOK {
		t.Fatalf("get failed cut status = %d; body=%s", status, raw)
	}
	job := cutDecode(t, raw)
	if job["stage"] != "failed" {
		t.Fatalf("failed cut stage = %#v, want failed", job["stage"])
	}
	reasons, ok := job["failure_reasons"].([]any)
	if !ok || len(reasons) == 0 || reasons[0] != "RECEIPT_MISSING" {
		t.Fatalf("failed cut failure_reasons = %#v, want RECEIPT_MISSING", job["failure_reasons"])
	}

	// The same key+body replays the failed job instead of re-freezing over
	// the rejection.
	status, _, raw = cutRequest(t, server, http.MethodPost, createPath, "key-durable-reject", createBody)
	if status != http.StatusAccepted {
		t.Fatalf("replay status = %d; body=%s", status, raw)
	}
	replay := cutDecode(t, raw)
	if replay["cut_id"] != string(failedID) || replay["stage"] != "failed" {
		t.Fatalf("replay = %#v, want the failed cut %q", replay, failedID)
	}
}

// flakyCommittedLedger makes the committed evidence ledger transiently
// unreadable so the durable authority's independent re-derivation fails as an
// availability fault instead of a deterministic verdict.
type flakyCommittedLedger struct {
	*memory.Store
	unavailable bool
}

func (f *flakyCommittedLedger) CommittedEvidenceBatches(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, afterMemoryVersion, throughMemoryVersion int64) ([]domain.EvidenceBatch, error) {
	if f.unavailable {
		return nil, errors.New("committed ledger temporarily unavailable")
	}
	return f.Store.CommittedEvidenceBatches(ctx, tenantID, spaceID, afterMemoryVersion, throughMemoryVersion)
}

// A TRANSIENT durable-authority failure (ledger read error) classifies as 500
// INFRASTRUCTURE_FAILURE — never as a deterministic 422 receipt rejection —
// and once the ledger recovers a fresh trigger freezes through the same guard.
func TestConsolidationCutTransientAuthorityFailureIs500AndRecovers(t *testing.T) {
	store := memory.New()
	ledger := &flakyCommittedLedger{Store: store, unavailable: true}
	realFreezer := httpapi.NewRoomFreezer(store, store, map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath}})
	cutService := consolidationcut.NewService(nil, realFreezer)
	cutService.SetGuardAuthority(httpapi.NewDurableGuardAuthority(ledger, cutService.GetManifest, map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath}}))
	root := httpapi.NewHandler(httpapi.Dependencies{
		Token:             contractToken,
		Registry:          store,
		Evidence:          evidence.New(store, store, contractClock{now: contractNow}),
		Clock:             contractClock{now: contractNow},
		ConsolidationCuts: cutService,
		CutFreezer:        realFreezer,
		CutEvents:         httpapi.NewCutEventNotifier("test-secret", httpapi.NewFileCutEventSink(filepath.Join(t.TempDir(), "cut-events.jsonl"))),
	})
	server := httptest.NewServer(root)
	t.Cleanup(server.Close)
	cutBootstrap(t, server)

	const createPath = "/v1/rooms/consolidation-room/consolidation-cuts"
	createBody := `{"mode":"force","trigger_source":"t"}`
	status, _, raw := cutRequest(t, server, http.MethodPost, createPath, "key-transient-fail", createBody)
	if status != http.StatusInternalServerError {
		t.Fatalf("create under transient authority failure status = %d, want 500; body=%s", status, raw)
	}
	if body := cutDecode(t, raw); body["code"] != "INFRASTRUCTURE_FAILURE" {
		t.Fatalf("transient rejection code = %#v, want INFRASTRUCTURE_FAILURE", body["code"])
	}

	// The authoritative failed job carries the transient reason, not
	// RECEIPT_MISSING.
	var failedID consolidationcut.CutID
	for _, entry := range cutService.AuditTrail("consolidation-tenant") {
		if entry.Kind == "trigger_frozen_rejected" {
			failedID = entry.CutID
		}
	}
	status, _, raw = cutRequest(t, server, http.MethodGet, "/v1/consolidation-cuts/"+string(failedID), "", "")
	if status != http.StatusOK {
		t.Fatalf("get transiently failed cut status = %d; body=%s", status, raw)
	}
	job := cutDecode(t, raw)
	if job["stage"] != "failed" {
		t.Fatalf("transiently failed cut stage = %#v, want failed", job["stage"])
	}
	reasons, ok := job["failure_reasons"].([]any)
	if !ok || len(reasons) == 0 || reasons[0] != "INFRASTRUCTURE_FAILURE" {
		t.Fatalf("transiently failed cut failure_reasons = %#v, want INFRASTRUCTURE_FAILURE", job["failure_reasons"])
	}

	// After the ledger recovers, a fresh trigger freezes through the same
	// durable authority.
	ledger.unavailable = false
	status, _, raw = cutRequest(t, server, http.MethodPost, createPath, "key-transient-recovered", createBody)
	if status != http.StatusAccepted {
		t.Fatalf("create after recovery status = %d, want 202; body=%s", status, raw)
	}
	if body := cutDecode(t, raw); body["stage"] != "frozen" {
		t.Fatalf("recovered cut stage = %#v, want frozen", body["stage"])
	}
}

// cutCommitExtraBatch stages and commits a second evidence batch on the
// bootstrap space under its own segment, so a manifest has two independent
// segment/receipt rows to lie about.
func cutCommitExtraBatch(t *testing.T, server *httptest.Server, index int) {
	t.Helper()
	batchID := fmt.Sprintf("consolidation-batch-%d", index)
	segmentID := fmt.Sprintf("consolidation-segment-%d", index)
	stage := map[string]any{
		"batch_id":          batchID,
		"idempotency_key":   fmt.Sprintf("stream-key-%d", index),
		"space_id":          cutSpacePath,
		"stream_id":         "stream-" + cutSpacePath,
		"source_segment_id": segmentID,
		"provenance": map[string]any{
			"host_type":        "pi-group-chat-host",
			"host_instance_id": "host-instance-1",
			"source_kind":      "room_shared",
			"captured_at":      contractNow.Add(-2 * time.Minute).Format(time.RFC3339Nano),
			"content_sha256":   fmt.Sprintf("%064x", index),
		},
		"events": []map[string]any{{
			"event_id":    fmt.Sprintf("consolidation-event-%d", index),
			"sequence":    index,
			"kind":        "room_message",
			"content":     fmt.Sprintf("extra settled fact %d", index),
			"occurred_at": contractNow.Add(-time.Minute).Format(time.RFC3339Nano),
		}},
		"links":            []map[string]any{},
		"terminal_outcome": "settled",
	}
	stageBody, err := json.Marshal(stage)
	if err != nil {
		t.Fatalf("marshal extra stage: %v", err)
	}
	if status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/evidence-batches:stage", "", string(stageBody)); status != http.StatusCreated {
		t.Fatalf("extra stage status = %d, want 201; body=%s", status, raw)
	}
	commitBody := fmt.Sprintf(`{"commit_id":"consolidation-commit-%d"}`, index)
	if status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/evidence-batches/"+batchID+":commit", "", commitBody); status != http.StatusOK {
		t.Fatalf("extra commit status = %d, want 200; body=%s", status, raw)
	}
}

// lyingFreezer lets a test post-process the otherwise-real freeze manifest:
// local structure stays self-consistent, so only the durable authority's
// independent committed-ledger re-derivation can reject the lie.
type lyingFreezer struct {
	inner  *httpapi.RoomFreezer
	mutate func(*consolidationcut.Manifest)
}

func (f *lyingFreezer) Freeze(ctx context.Context, req consolidationcut.TriggerRequest) (consolidationcut.Manifest, error) {
	manifest, err := f.inner.Freeze(ctx, req)
	if err != nil {
		return consolidationcut.Manifest{}, err
	}
	f.mutate(&manifest)
	return manifest, nil
}

// cutDurableComposition wires the production composition with the given room
// bindings and an optional freezer wrapper, mirroring main.go's durable
// authority injection.
func cutDurableComposition(t *testing.T, bindings map[string][]domain.SpaceID, wrap func(inner *httpapi.RoomFreezer) consolidationcut.Freezer) (*httptest.Server, *consolidationcut.Service) {
	t.Helper()
	store := memory.New()
	realFreezer := httpapi.NewRoomFreezer(store, store, bindings)
	freezer := consolidationcut.Freezer(realFreezer)
	if wrap != nil {
		freezer = wrap(realFreezer)
	}
	cutService := consolidationcut.NewService(nil, freezer)
	cutService.SetGuardAuthority(httpapi.NewDurableGuardAuthority(store, cutService.GetManifest, bindings))
	root := httpapi.NewHandler(httpapi.Dependencies{
		Token:             contractToken,
		Registry:          store,
		Evidence:          evidence.New(store, store, contractClock{now: contractNow}),
		Clock:             contractClock{now: contractNow},
		ConsolidationCuts: cutService,
		CutFreezer:        realFreezer,
		CutEvents:         httpapi.NewCutEventNotifier("test-secret", httpapi.NewFileCutEventSink(filepath.Join(t.TempDir(), "cut-events.jsonl"))),
	})
	server := httptest.NewServer(root)
	t.Cleanup(server.Close)
	return server, cutService
}

// The durable authority's committed-ledger re-derivation is EXACT in both
// directions: a lying freezer that omits a committed batch, duplicates a
// scope, declares a batch across scopes, or freezes a scope set that does not
// match the room's deployment binding must fail closed at the frozen guard —
// while an honest two-space manifest freezes through.
func TestConsolidationCutDurableAuthorityRejectsIncompleteManifests(t *testing.T) {
	singleSpace := map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath}}
	twoSpaces := map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath, "consolidation-space-b"}}

	dropSecondSegment := func(manifest *consolidationcut.Manifest) {
		last := len(manifest.SpaceScopes) - 1
		manifest.SpaceScopes[last].EvidenceBatchIDs = manifest.SpaceScopes[last].EvidenceBatchIDs[:0:0]
		manifest.SpaceScopes[last].EvidenceBatchIDs = nil
		manifest.SealedSegmentIDs = manifest.SealedSegmentIDs[:1]
		manifest.EvidenceCommitReceipts = manifest.EvidenceCommitReceipts[:1]
	}
	duplicateScope := func(manifest *consolidationcut.Manifest) {
		manifest.SpaceScopes = append(manifest.SpaceScopes, manifest.SpaceScopes[0])
	}
	unboundScope := func(manifest *consolidationcut.Manifest) {
		manifest.SpaceScopes[0].SpaceID = "consolidation-space-rogue"
	}
	missingScope := func(manifest *consolidationcut.Manifest) {
		manifest.SpaceScopes = nil
	}
	crossScopeDuplicate := func(manifest *consolidationcut.Manifest) {
		manifest.SpaceScopes[1].EvidenceBatchIDs = append(manifest.SpaceScopes[1].EvidenceBatchIDs, manifest.SpaceScopes[0].EvidenceBatchIDs[0])
	}

	t.Run("honest two-space manifest freezes through", func(t *testing.T) {
		server, _ := cutDurableComposition(t, twoSpaces, nil)
		cutBootstrap(t, server)
		cutBootstrapSecondSpace(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-two-space-honest", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusAccepted {
			t.Fatalf("honest two-space create status = %d, want 202; body=%s", status, raw)
		}
		if body := cutDecode(t, raw); body["stage"] != "frozen" {
			t.Fatalf("honest two-space stage = %#v, want frozen", body["stage"])
		}
	})

	t.Run("omitted committed batch", func(t *testing.T) {
		server, _ := cutDurableComposition(t, singleSpace, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: dropSecondSegment}
		})
		cutBootstrap(t, server)
		cutCommitExtraBatch(t, server, 2)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-omit-batch", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("omitted-batch create status = %d, want 422; body=%s", status, raw)
		}
		if body := cutDecode(t, raw); body["code"] != "RECEIPT_MISSING" {
			t.Fatalf("omitted-batch code = %#v, want RECEIPT_MISSING", body["code"])
		}
	})

	t.Run("duplicate scope", func(t *testing.T) {
		server, _ := cutDurableComposition(t, singleSpace, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: duplicateScope}
		})
		cutBootstrap(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-dup-scope", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("duplicate-scope create status = %d, want 422; body=%s", status, raw)
		}
	})

	t.Run("unbound scope", func(t *testing.T) {
		server, _ := cutDurableComposition(t, singleSpace, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: unboundScope}
		})
		cutBootstrap(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-unbound-scope", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("unbound-scope create status = %d, want 422; body=%s", status, raw)
		}
	})

	t.Run("missing scope", func(t *testing.T) {
		server, _ := cutDurableComposition(t, singleSpace, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: missingScope}
		})
		cutBootstrap(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-missing-scope", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("missing-scope create status = %d, want 422; body=%s", status, raw)
		}
	})

	t.Run("cross-scope duplicate batch", func(t *testing.T) {
		server, _ := cutDurableComposition(t, twoSpaces, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: crossScopeDuplicate}
		})
		cutBootstrap(t, server)
		cutBootstrapSecondSpace(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-cross-dup", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("cross-scope duplicate create status = %d, want 422; body=%s", status, raw)
		}
		if body := cutDecode(t, raw); body["code"] != "RECEIPT_MISSING" {
			t.Fatalf("cross-scope duplicate code = %#v, want RECEIPT_MISSING", body["code"])
		}
	})
}

// cutBootstrapSecondSpace registers the second bound space and extends the
// host lifecycle grant to it (the two-space durable-authority subtests).
func cutBootstrapSecondSpace(t *testing.T, server *httptest.Server) {
	t.Helper()
	if status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/spaces", "", `{
		"space_id":"consolidation-space-b",
		"scope":"shared",
		"owner_principal_id":null,
		"display_name":"Consolidation space B"
	}`); status != http.StatusCreated {
		t.Fatalf("space-b status = %d; body=%s", status, raw)
	}
	if status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/grants", "", `{
		"grant_id":"consolidation-lifecycle-b",
		"principal_id":"host-service",
		"space_ids":["consolidation-space-b"],
		"purpose":"lifecycle",
		"operations":["evidence.stage","evidence.commit"],
		"expires_at":"2031-01-01T00:00:00Z"
	}`); status != http.StatusCreated {
		t.Fatalf("grant-b status = %d; body=%s", status, raw)
	}
}

// cutBootstrapShell wires only the tenant, the bound space, and the lifecycle
// grant — no evidence batch is staged or committed (zero-sealed subtests).
func cutBootstrapShell(t *testing.T, server *httptest.Server) {
	t.Helper()
	cutRequest(t, server, http.MethodPut, "/v1/tenant", "", `{
		"tenant_id":"consolidation-tenant",
		"display_name":"Consolidation",
		"bootstrap_principal_id":"host-service"
	}`)
	cutRequest(t, server, http.MethodPost, "/v1/spaces", "", `{
		"space_id":"consolidation-space",
		"scope":"shared",
		"owner_principal_id":null,
		"display_name":"Consolidation space"
	}`)
	cutRequest(t, server, http.MethodPost, "/v1/grants", "", `{
		"grant_id":"consolidation-lifecycle",
		"principal_id":"host-service",
		"space_ids":["consolidation-space"],
		"purpose":"lifecycle",
		"operations":["evidence.stage","evidence.commit"],
		"expires_at":"2031-01-01T00:00:00Z"
	}`)
}

// Zero-sealed manifests are NOT exempt from the durable authority: a freezer
// holding a room with committed evidence cannot present an empty manifest and
// slip into frozen, while an honest no-evidence freeze still freezes through.
func TestConsolidationCutDurableAuthorityValidatesZeroSealedManifests(t *testing.T) {
	singleSpace := map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath}}

	t.Run("empty manifest cannot hide committed evidence", func(t *testing.T) {
		emptyOut := func(manifest *consolidationcut.Manifest) {
			manifest.SealedSegmentIDs = nil
			manifest.EvidenceCommitReceipts = nil
			manifest.SpaceScopes = nil
		}
		server, cutService := cutDurableComposition(t, singleSpace, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: emptyOut}
		})
		cutBootstrap(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-zero-sealed-lie", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("zero-sealed lie status = %d, want 422; body=%s", status, raw)
		}
		if body := cutDecode(t, raw); body["code"] != "RECEIPT_MISSING" {
			t.Fatalf("zero-sealed lie code = %#v, want RECEIPT_MISSING", body["code"])
		}
		var failedID consolidationcut.CutID
		for _, entry := range cutService.AuditTrail("consolidation-tenant") {
			if entry.Kind == "trigger_frozen_rejected" {
				failedID = entry.CutID
			}
		}
		status, _, raw = cutRequest(t, server, http.MethodGet, "/v1/consolidation-cuts/"+string(failedID), "", "")
		if status != http.StatusOK {
			t.Fatalf("get zero-sealed failed cut status = %d; body=%s", status, raw)
		}
		if job := cutDecode(t, raw); job["stage"] != "failed" {
			t.Fatalf("zero-sealed failed cut stage = %#v, want failed", job["stage"])
		}
		// The same key+body replays the failed job instead of re-freezing.
		status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-zero-sealed-lie", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusAccepted {
			t.Fatalf("zero-sealed replay status = %d; body=%s", status, raw)
		}
		if replay := cutDecode(t, raw); replay["cut_id"] != string(failedID) {
			t.Fatalf("zero-sealed replay cut = %#v, want %q", replay["cut_id"], failedID)
		}
	})

	t.Run("honest no-evidence freeze freezes through", func(t *testing.T) {
		server, _ := cutDurableComposition(t, singleSpace, nil)
		cutBootstrapShell(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-zero-sealed-honest", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusAccepted {
			t.Fatalf("honest zero-sealed create status = %d, want 202; body=%s", status, raw)
		}
		if body := cutDecode(t, raw); body["stage"] != "frozen" {
			t.Fatalf("honest zero-sealed stage = %#v, want frozen", body["stage"])
		}
	})
}

// The canonical receipts digest covers only each receipt's BatchDigest, so a
// freezer that swaps two receipts' batch lists or substitutes phantom batch
// IDs (keeping the digests) would still present a matching aggregate. The
// durable authority must bind each receipt's provenance to the committed
// ledger per segment: same SegmentID, same batch set, same digest.
func TestConsolidationCutDurableAuthorityBindsReceiptProvenance(t *testing.T) {
	singleSpace := map[string][]domain.SpaceID{"consolidation-room": {cutSpacePath}}

	t.Run("swapped receipt batch lists", func(t *testing.T) {
		swapBatchLists := func(manifest *consolidationcut.Manifest) {
			if len(manifest.EvidenceCommitReceipts) < 2 {
				t.Fatal("fixture expects two sealed segments")
			}
			manifest.EvidenceCommitReceipts[0].BatchIDs, manifest.EvidenceCommitReceipts[1].BatchIDs =
				manifest.EvidenceCommitReceipts[1].BatchIDs, manifest.EvidenceCommitReceipts[0].BatchIDs
		}
		server, cutService := cutDurableComposition(t, singleSpace, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: swapBatchLists}
		})
		cutBootstrap(t, server)
		cutCommitExtraBatch(t, server, 2)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-receipt-swap", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("swapped-receipt create status = %d, want 422; body=%s", status, raw)
		}
		if body := cutDecode(t, raw); body["code"] != "RECEIPT_MISSING" {
			t.Fatalf("swapped-receipt code = %#v, want RECEIPT_MISSING", body["code"])
		}
		var failedID consolidationcut.CutID
		for _, entry := range cutService.AuditTrail("consolidation-tenant") {
			if entry.Kind == "trigger_frozen_rejected" {
				failedID = entry.CutID
			}
		}
		status, _, raw = cutRequest(t, server, http.MethodGet, "/v1/consolidation-cuts/"+string(failedID), "", "")
		if status != http.StatusOK {
			t.Fatalf("get swapped-receipt failed cut status = %d; body=%s", status, raw)
		}
		if job := cutDecode(t, raw); job["stage"] != "failed" {
			t.Fatalf("swapped-receipt failed cut stage = %#v, want failed", job["stage"])
		}
		status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-receipt-swap", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusAccepted {
			t.Fatalf("swapped-receipt replay status = %d; body=%s", status, raw)
		}
		if replay := cutDecode(t, raw); replay["cut_id"] != string(failedID) {
			t.Fatalf("swapped-receipt replay cut = %#v, want %q", replay["cut_id"], failedID)
		}
	})

	t.Run("phantom batch IDs with real digests", func(t *testing.T) {
		phantomBatchIDs := func(manifest *consolidationcut.Manifest) {
			if len(manifest.EvidenceCommitReceipts) == 0 {
				t.Fatal("fixture expects at least one sealed segment")
			}
			manifest.EvidenceCommitReceipts[0].BatchIDs = []domain.BatchID{"batch-fake-1", "batch-fake-2"}
		}
		server, cutService := cutDurableComposition(t, singleSpace, func(inner *httpapi.RoomFreezer) consolidationcut.Freezer {
			return &lyingFreezer{inner: inner, mutate: phantomBatchIDs}
		})
		cutBootstrap(t, server)
		status, _, raw := cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-receipt-phantom", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("phantom-batch-IDs create status = %d, want 422; body=%s", status, raw)
		}
		if body := cutDecode(t, raw); body["code"] != "RECEIPT_MISSING" {
			t.Fatalf("phantom-batch-IDs code = %#v, want RECEIPT_MISSING", body["code"])
		}
		var failedID consolidationcut.CutID
		for _, entry := range cutService.AuditTrail("consolidation-tenant") {
			if entry.Kind == "trigger_frozen_rejected" {
				failedID = entry.CutID
			}
		}
		status, _, raw = cutRequest(t, server, http.MethodGet, "/v1/consolidation-cuts/"+string(failedID), "", "")
		if status != http.StatusOK {
			t.Fatalf("get phantom-batch-IDs failed cut status = %d; body=%s", status, raw)
		}
		if job := cutDecode(t, raw); job["stage"] != "failed" {
			t.Fatalf("phantom-batch-IDs failed cut stage = %#v, want failed", job["stage"])
		}
		status, _, raw = cutRequest(t, server, http.MethodPost, "/v1/rooms/consolidation-room/consolidation-cuts", "key-receipt-phantom", `{"mode":"force","trigger_source":"t"}`)
		if status != http.StatusAccepted {
			t.Fatalf("phantom-batch-IDs replay status = %d; body=%s", status, raw)
		}
		if replay := cutDecode(t, raw); replay["cut_id"] != string(failedID) {
			t.Fatalf("phantom-batch-IDs replay cut = %#v, want %q", replay["cut_id"], failedID)
		}
	})
}
