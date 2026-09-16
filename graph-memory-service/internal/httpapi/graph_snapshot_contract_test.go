package httpapi_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/recall"
	"river2.dev/graph-memory-service/internal/retrieval"
	"river2.dev/graph-memory-service/internal/store/memory"
)

// snapshotComposition wires the full frozen-snapshot loop the bench runner
// drives: committed evidence, consolidation-cut freezing with the durable
// receipts authority, and recall — so a test can freeze a cut, read the
// manifest's per-space head versions off the job body, pin recall to them,
// and prove later commits stay invisible to the pinned read.
type snapshotComposition struct {
	server *httptest.Server
	store  *memory.Store
}

func snapshotCompositionNew(t *testing.T, roomSpaces map[string][]string) snapshotComposition {
	t.Helper()
	store := memory.New()
	clock := contractClock{now: contractNow}
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
	cutService.SetGuardAuthority(httpapi.NewDurableGuardAuthority(store, cutService.GetManifest, bindings))
	engine, err := retrieval.New(store, nil, retrieval.DefaultConfig())
	if err != nil {
		t.Fatalf("build retrieval engine: %v", err)
	}
	handler := httpapi.NewHandler(httpapi.Dependencies{
		Token:             contractToken,
		Registry:          store,
		Evidence:          evidence.New(store, store, clock),
		Recall:            recall.New(store, engine, clock),
		Clock:             clock,
		ConsolidationCuts: cutService,
		CutFreezer:        cutFreezer,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return snapshotComposition{server: server, store: store}
}

// snapshotRequest performs one bearer-authenticated call and decodes the JSON
// body with exact numbers.
func snapshotRequest(t *testing.T, composition snapshotComposition, method, path, idempotencyKey string, body any) (int, map[string]any) {
	t.Helper()
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal %s %s: %v", method, path, err)
		}
		payload = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequest(method, composition.server.URL+path, payload)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	request.Header.Set("Authorization", "Bearer "+contractToken)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := composition.server.Client().Do(request)
	if err != nil {
		t.Fatalf("perform %s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, path, err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return response.StatusCode, map[string]any{}
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode %s %s response %q: %v", method, path, raw, err)
	}
	return response.StatusCode, decoded
}

// snapshotBootstrap wires the tenant, one shared space, and a lifecycle grant
// covering evidence + recall for it.
func snapshotBootstrap(t *testing.T, composition snapshotComposition, spaceID string) {
	t.Helper()
	status, body := snapshotRequest(t, composition, http.MethodPut, "/v1/tenant", "", map[string]any{
		"tenant_id": "snapshot-tenant", "display_name": "Snapshot", "bootstrap_principal_id": "host-service",
	})
	if status != http.StatusCreated {
		t.Fatalf("tenant status = %d: %v", status, body)
	}
	status, body = snapshotRequest(t, composition, http.MethodPost, "/v1/spaces", "", map[string]any{
		"space_id": spaceID, "scope": "shared", "owner_principal_id": nil, "display_name": "Snapshot space",
	})
	if status != http.StatusCreated {
		t.Fatalf("space status = %d: %v", status, body)
	}
	status, body = snapshotRequest(t, composition, http.MethodPost, "/v1/grants", "", map[string]any{
		"grant_id": "snapshot-lifecycle", "principal_id": "host-service", "space_ids": []string{spaceID},
		"purpose": "lifecycle", "operations": []string{"evidence.stage", "evidence.commit", "recall"},
		"expires_at": contractNow.Add(time.Hour).Format(time.RFC3339Nano),
	})
	if status != http.StatusCreated {
		t.Fatalf("grant status = %d: %v", status, body)
	}
}

// snapshotCommit stages and commits one single-event batch, returning the
// memory version the commit assigned.
func snapshotCommit(t *testing.T, composition snapshotComposition, spaceID, segmentID, batchID, key, fact string) int64 {
	t.Helper()
	status, body := snapshotRequest(t, composition, http.MethodPost, "/v1/evidence-batches:stage", "", map[string]any{
		"batch_id": batchID, "idempotency_key": key, "space_id": spaceID,
		"stream_id": "stream-" + spaceID, "source_segment_id": segmentID,
		"provenance": map[string]any{
			"host_type": "pi-group-chat-host", "host_instance_id": "host-1", "source_kind": "room_shared",
			"captured_at":    contractNow.Add(-2 * time.Minute).Format(time.RFC3339Nano),
			"content_sha256": strings.Repeat(fmt.Sprintf("%x", len(fact)), 32)[:64],
		},
		"events": []map[string]any{{
			"event_id": batchID + "-event", "sequence": 1, "kind": "room_message",
			"content": fact, "occurred_at": contractNow.Add(-time.Minute).Format(time.RFC3339Nano),
		}},
		"links": []map[string]any{}, "terminal_outcome": "settled",
	})
	if status != http.StatusCreated {
		t.Fatalf("stage %s status = %d: %v", batchID, status, body)
	}
	status, body = snapshotRequest(t, composition, http.MethodPost, "/v1/evidence-batches/"+batchID+":commit", "", map[string]any{
		"commit_id": "commit-" + batchID,
	})
	if status != http.StatusOK {
		t.Fatalf("commit %s status = %d: %v", batchID, status, body)
	}
	version, ok := body["memory_version"].(json.Number)
	if !ok {
		t.Fatalf("commit %s missing memory_version: %v", batchID, body)
	}
	parsed, err := version.Int64()
	if err != nil {
		t.Fatalf("commit %s memory_version %v: %v", batchID, version, err)
	}
	return parsed
}

func snapshotScope(scopes []any, spaceID string) map[string]any {
	for _, raw := range scopes {
		scope, _ := raw.(map[string]any)
		if scope["space_id"] == spaceID {
			return scope
		}
	}
	return nil
}

// TestConsolidationCutJobBodiesCarryFrozenSpaceScopes proves the contract the
// bench runner depends on: every job body exposes the immutable manifest's
// per-space frozen head versions, the replayed create returns the ORIGINAL
// snapshot (never a live re-freeze), a room holding an in-flight (frozen) cut
// rejects a second freeze with ROOM_EPOCH_STALE, and a later cut — on another
// room bound to the same space — pins the advanced head.
func TestConsolidationCutJobBodiesCarryFrozenSpaceScopes(t *testing.T) {
	composition := snapshotCompositionNew(t, map[string][]string{
		"snapshot-room":   {"snapshot-space"},
		"snapshot-room-2": {"snapshot-space"},
	})
	snapshotBootstrap(t, composition, "snapshot-space")

	head1 := snapshotCommit(t, composition, "snapshot-space", "segment-1", "batch-1", "key-1", "the first frozen fact")

	status, create := snapshotRequest(t, composition, http.MethodPost, "/v1/rooms/snapshot-room/consolidation-cuts", "freeze-key-1", map[string]any{
		"mode": "force", "trigger_source": "bench-runner-consolidation",
	})
	if status != http.StatusAccepted {
		t.Fatalf("create status = %d: %v", status, create)
	}
	if create["stage"] != string(consolidationcut.StageFrozen) {
		t.Fatalf("create stage = %v, want frozen", create["stage"])
	}
	scopes, _ := create["space_scopes"].([]any)
	scope := snapshotScope(scopes, "snapshot-space")
	if scope == nil {
		t.Fatalf("create body missing space_scopes for snapshot-space: %v", create)
	}
	if scope["projection_head_version"] != json.Number(fmt.Sprintf("%d", head1)) {
		t.Fatalf("frozen head = %v, want %d", scope["projection_head_version"], head1)
	}
	if scope["query_watermark"] != json.Number("1") {
		t.Fatalf("frozen watermark = %v, want 1", scope["query_watermark"])
	}

	cutID, _ := create["cut_id"].(string)
	_, got := snapshotRequest(t, composition, http.MethodGet, "/v1/consolidation-cuts/"+cutID, "", nil)
	gotScope := snapshotScope(toSlice(got["space_scopes"]), "snapshot-space")
	if gotScope == nil || gotScope["projection_head_version"] != scope["projection_head_version"] {
		t.Fatalf("GET space_scopes = %v, want the frozen create scopes %v", got["space_scopes"], scopes)
	}

	// Later evidence and a second cut advance the head; the first cut's
	// snapshot must stay exactly where it froze.
	head2 := snapshotCommit(t, composition, "snapshot-space", "segment-2", "batch-2", "key-2", "a later fact the frozen read must never see")
	if head2 <= head1 {
		t.Fatalf("second commit version %d did not advance past %d", head2, head1)
	}
	// The first room still holds its frozen (in-flight) cut: SC-4.1 room-serial
	// freeze rejects a second freeze for it — GET is authoritative.
	statusStale, stale := snapshotRequest(t, composition, http.MethodPost, "/v1/rooms/snapshot-room/consolidation-cuts", "freeze-key-1b", map[string]any{
		"mode": "force", "trigger_source": "bench-runner-consolidation",
	})
	if statusStale != http.StatusConflict || stale["code"] != "ROOM_EPOCH_STALE" {
		t.Fatalf("second freeze on the frozen room = %d %v, want 409 ROOM_EPOCH_STALE", statusStale, stale)
	}
	status2, create2 := snapshotRequest(t, composition, http.MethodPost, "/v1/rooms/snapshot-room-2/consolidation-cuts", "freeze-key-2", map[string]any{
		"mode": "force", "trigger_source": "bench-runner-consolidation",
	})
	if status2 != http.StatusAccepted {
		t.Fatalf("second cut status = %d: %v", status2, create2)
	}
	scope2 := snapshotScope(toSlice(create2["space_scopes"]), "snapshot-space")
	if scope2 == nil || scope2["projection_head_version"] != json.Number(fmt.Sprintf("%d", head2)) {
		t.Fatalf("second cut head = %v, want %d", create2["space_scopes"], head2)
	}
	if scope2["query_watermark"] != json.Number("2") {
		t.Fatalf("second cut watermark = %v, want 2", scope2["query_watermark"])
	}

	// Same idempotency key replays the original frozen job and its ORIGINAL
	// snapshot — a replay must never re-freeze at the live head.
	_, replay := snapshotRequest(t, composition, http.MethodPost, "/v1/rooms/snapshot-room/consolidation-cuts", "freeze-key-1", map[string]any{
		"mode": "force", "trigger_source": "bench-runner-consolidation",
	})
	replayScope := snapshotScope(toSlice(replay["space_scopes"]), "snapshot-space")
	if replay["cut_id"] != cutID || replayScope == nil || replayScope["projection_head_version"] != scope["projection_head_version"] {
		t.Fatalf("replayed create = cut %v scopes %v, want original cut %s scopes %v", replay["cut_id"], replay["space_scopes"], cutID, scopes)
	}
}

// TestRecallPinnedToSnapshotFiltersLaterEvidence proves version pinning is a
// retrieval filter, not an annotation: a read pinned to a cut's frozen head
// keeps returning exactly the frozen evidence after the live head advances,
// and a pin ahead of the head fails closed instead of widening.
func TestRecallPinnedToSnapshotFiltersLaterEvidence(t *testing.T) {
	composition := snapshotCompositionNew(t, map[string][]string{"snapshot-room": {"snapshot-space"}})
	snapshotBootstrap(t, composition, "snapshot-space")

	head1 := snapshotCommit(t, composition, "snapshot-space", "segment-1", "batch-1", "key-1", "the first frozen fact")
	head2 := snapshotCommit(t, composition, "snapshot-space", "segment-2", "batch-2", "key-2", "a later unfrozen fact")

	recallBody := func(requestID string, versions map[string]any) map[string]any {
		body := map[string]any{
			"request_id": requestID, "query": "fact", "space_ids": []string{"snapshot-space"},
			"max_results": 10, "deadline_ms": 5000,
		}
		if versions != nil {
			body["space_versions"] = versions
		}
		return body
	}

	// Unpinned read at the live head: both facts visible.
	status, live := snapshotRequest(t, composition, http.MethodPost, "/v1/recalls", "", recallBody("live-read", nil))
	if status != http.StatusOK {
		t.Fatalf("live recall status = %d: %v", status, live)
	}
	if got := len(toSlice(live["items"])); got != 2 {
		t.Fatalf("live recall items = %d, want 2", got)
	}

	// Pinned at head1: the later fact is filtered out, and every served item
	// sits at or below the pinned version.
	status, pinned := snapshotRequest(t, composition, http.MethodPost, "/v1/recalls", "", recallBody("pinned-read", map[string]any{"snapshot-space": head1}))
	if status != http.StatusOK {
		t.Fatalf("pinned recall status = %d: %v", status, pinned)
	}
	items := toSlice(pinned["items"])
	if len(items) != 1 {
		t.Fatalf("pinned recall items = %d, want exactly the frozen fact", len(items))
	}
	first, _ := items[0].(map[string]any)
	if !strings.Contains(fmt.Sprint(first["content"]), "first frozen fact") {
		t.Fatalf("pinned recall served %v, want the frozen fact", first["content"])
	}
	if first["memory_version"] != json.Number(fmt.Sprintf("%d", head1)) {
		t.Fatalf("pinned item memory_version = %v, want %d", first["memory_version"], head1)
	}

	// Pinned at the live head: equivalent to no pin.
	status, atHead := snapshotRequest(t, composition, http.MethodPost, "/v1/recalls", "", recallBody("head-read", map[string]any{"snapshot-space": head2}))
	if status != http.StatusOK || len(toSlice(atHead["items"])) != 2 {
		t.Fatalf("head-pinned recall = %d items (status %d), want 2", len(toSlice(atHead["items"])), status)
	}

	// A pin ahead of the head fails closed with STALE_PROJECTION_HEAD.
	status, ahead := snapshotRequest(t, composition, http.MethodPost, "/v1/recalls", "", recallBody("ahead-read", map[string]any{"snapshot-space": head2 + 1}))
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("future-pin recall status = %d: %v", status, ahead)
	}
	errBody, _ := ahead["error"].(map[string]any)
	if errBody == nil || errBody["code"] != "STALE_PROJECTION_HEAD" {
		t.Fatalf("future-pin error = %v, want STALE_PROJECTION_HEAD", ahead)
	}

	// Ingress validation: unknown keys, negative pins.
	status, unknown := snapshotRequest(t, composition, http.MethodPost, "/v1/recalls", "", recallBody("unknown-read", map[string]any{"other-space": 1}))
	if status != http.StatusBadRequest {
		t.Fatalf("unknown-pin recall status = %d: %v", status, unknown)
	}
	status, negative := snapshotRequest(t, composition, http.MethodPost, "/v1/recalls", "", recallBody("negative-read", map[string]any{"snapshot-space": -1}))
	if status != http.StatusBadRequest {
		t.Fatalf("negative-pin recall status = %d: %v", status, negative)
	}

	// An empty pins object is absent, not a constraint.
	status, empty := snapshotRequest(t, composition, http.MethodPost, "/v1/recalls", "", recallBody("empty-read", map[string]any{}))
	if status != http.StatusOK || len(toSlice(empty["items"])) != 2 {
		t.Fatalf("empty-pins recall = %d items (status %d), want 2", len(toSlice(empty["items"])), status)
	}
}

func toSlice(value any) []any {
	slice, _ := value.([]any)
	return slice
}
