package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
)

func snapshotTestEpisodes() []manifestEpisode {
	order := func(value int) *int { return &value }
	return []manifestEpisode{
		{TaskID: "a1", EpisodeID: "a1", FamilyID: "alpha", Split: "train", Order: order(0)},
		{TaskID: "a2", EpisodeID: "a2", FamilyID: "alpha", Split: "train", Order: order(1)},
		{TaskID: "a3", EpisodeID: "a3", FamilyID: "alpha", Split: "test", Order: order(2)},
		{TaskID: "a4", EpisodeID: "a4", FamilyID: "alpha", Split: "train", Order: order(3)},
		{TaskID: "b1", EpisodeID: "b1", FamilyID: "beta", Split: "train", Order: order(0)},
		{TaskID: "b2", EpisodeID: "b2", FamilyID: "beta", Split: "test", Order: order(1)},
	}
}

// registrationOrderBindings re-derives the room=space bindings straight from
// the registration phase's loop (registerSpaces): one global sequence counter
// advancing for every episode of every family, task rooms per train episode,
// diagnosis rooms for batch train episodes, the consolidation room per family.
// It exists to catch drift between that loop and trainScopeRoomsForFamily.
func registrationOrderBindings(episodes []manifestEpisode, arm string) []string {
	strategy, _ := skillStrategyFor(arm)
	var bindings []string
	appendRoom := func(room, shared, private, memoryPrivate string) {
		bindings = append(bindings, room+"="+shared+","+private+","+memoryPrivate)
	}
	next := 0
	for _, family := range familySequence(episodes) {
		for _, episode := range episodesOfFamily(episodes, family) {
			next++
			if episode.Split == "test" {
				continue
			}
			_, shared, private, memoryPrivate := warmSkillIsolatedRoom(family, next, episode.Split)
			appendRoom(episodeRoom(family, next, episode.Split), shared, private, memoryPrivate)
			if strategy == skillStrategyBatch {
				room, diagShared, diagPrivate, diagMemoryPrivate := warmSkillDiagnosisRoom(family, next)
				appendRoom(room, diagShared, diagPrivate, diagMemoryPrivate)
			}
		}
		room, consShared, consPrivate, consMemoryPrivate := warmSkillConsolidationRoom(family)
		appendRoom(room, consShared, consPrivate, consMemoryPrivate)
	}
	return bindings
}

func TestArmCutBindingsMirrorRegistrationDerivation(t *testing.T) {
	episodes := snapshotTestEpisodes()
	batch := armCutBindings(episodes, "warm-skill-batch")
	if !reflect.DeepEqual(batch, registrationOrderBindings(episodes, "warm-skill-batch")) {
		t.Fatalf("batch bindings drifted from registration order:\n got %v\nwant %v", batch, registrationOrderBindings(episodes, "warm-skill-batch"))
	}
	// The shared counter must advance through test episodes: after alpha's
	// train(1) test(2) train(3) train(4), beta's train room is sequence 5.
	if !strings.Contains(strings.Join(batch, " "), "room-beta-train-5=space-beta-train-5-shared") {
		t.Errorf("beta train room must carry the global sequence 5, bindings: %v", batch)
	}
	// Test rooms are throwaway and never frozen: alpha's test episode was
	// sequence 2, so no binding may reference it.
	joined := strings.Join(batch, " ")
	if strings.Contains(joined, "-test-2") || strings.Contains(joined, "-test-6") {
		t.Errorf("test rooms must not be cut targets: %v", batch)
	}
	// The family consolidation room closes every family's scope, after its
	// train rooms — batch diagnosis turns run there.
	if !strings.HasSuffix(joined, "room-beta-consolidation=space-beta-consolidation-shared,space-beta-consolidation-private,space-beta-consolidation-memory-private") {
		t.Errorf("beta consolidation room must close the bindings: %v", batch)
	}
	// Alpha's episodes run train(1) train(2) test(3) train(4): the test
	// episode still consumes sequence 3, so the third train room is 4.
	alphaTrain := "room-alpha-train-4=space-alpha-train-4-shared,space-alpha-train-4-private,space-alpha-train-4-memory-private"
	if !strings.Contains(joined, alphaTrain) {
		t.Errorf("alpha train room binding missing: %v", batch)
	}

	replay := armCutBindings(episodes, "warm-skill-replay")
	if !reflect.DeepEqual(replay, registrationOrderBindings(episodes, "warm-skill-replay")) {
		t.Fatalf("replay bindings drifted from registration order: %v", replay)
	}
	if strings.Contains(strings.Join(replay, " "), "diagnosis") {
		t.Errorf("replay strategy seats no diagnosis rooms, bindings: %v", replay)
	}
}

func TestArmCutBindingsExcludedForNonFreezingArms(t *testing.T) {
	episodes := snapshotTestEpisodes()
	for _, arm := range []string{"warm-skill-continual", "warm", "reset", "cold", "warm-ma"} {
		if bindings := armCutBindings(episodes, arm); bindings != nil {
			t.Errorf("arm %q must not derive cut bindings, got %v", arm, bindings)
		}
	}
	for _, arm := range []string{"warm-skill", "warm-skill-batch", "warm-skill-replay", "warm-skill-online"} {
		if !armFreezesGraph(arm) {
			t.Errorf("arm %q must freeze the graph at consolidation", arm)
		}
	}
	if armFreezesGraph("warm-skill-continual") || armFreezesGraph("warm") || armFreezesGraph("reset") {
		t.Error("continual stream and non-skill arms never freeze the graph")
	}
}

func TestTrainScopeRoomsFreezeConsolidationRoomLast(t *testing.T) {
	episodes := snapshotTestEpisodes()
	next := 0
	rooms := trainScopeRoomsForFamily(episodes, "alpha", &next, skillStrategyBatch)
	if next != 4 {
		t.Fatalf("family alpha must advance the counter to 4, got %d", next)
	}
	last := rooms[len(rooms)-1]
	wantRoom, _, _, _ := warmSkillConsolidationRoom("alpha")
	if last.RoomID != wantRoom {
		t.Errorf("consolidation room must close the train scope: last = %q, want %q", last.RoomID, wantRoom)
	}
	for _, room := range rooms[:len(rooms)-1] {
		if room.RoomID == wantRoom {
			t.Errorf("consolidation room duplicated inside the train scope: %v", rooms)
		}
	}
}

type recordedCutTrigger struct {
	method         string
	path           string
	idempotencyKey string
	mode           string
	triggerSource  string
}

// fakeCutGMS stands in for the GMS consolidation-cut surface. When freezeOn
// is false the trigger answers stage "freezing" and the poll endpoint moves
// the job to frozen, exercising the polling path.
type fakeCutGMS struct {
	mu       sync.Mutex
	server   *httptest.Server
	triggers []recordedCutTrigger
	polls    int
	freezeOn bool
}

func newFakeCutGMS(t *testing.T, freezeOn bool) *fakeCutGMS {
	fake := &fakeCutGMS{freezeOn: freezeOn}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/rooms/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/consolidation-cuts") {
			http.NotFound(w, r)
			return
		}
		roomID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/rooms/"), "/consolidation-cuts")
		var body struct {
			Mode          string `json:"mode"`
			TriggerSource string `json:"trigger_source"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		fake.mu.Lock()
		fake.triggers = append(fake.triggers, recordedCutTrigger{
			method: r.Method, path: r.URL.Path,
			idempotencyKey: r.Header.Get("Idempotency-Key"),
			mode:           body.Mode, triggerSource: body.TriggerSource,
		})
		fake.mu.Unlock()
		cutID := "cut-" + roomID
		stage := "frozen"
		if !fake.freezeOn {
			stage = "freezing"
		}
		fake.writeJob(w, fake.frozenJob(cutID, roomID, stage))
	})
	mux.HandleFunc("/v1/consolidation-cuts/", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		fake.polls++
		fake.mu.Unlock()
		cutID := strings.TrimPrefix(r.URL.Path, "/v1/consolidation-cuts/")
		roomID := strings.TrimPrefix(cutID, "cut-")
		fake.writeJob(w, fake.frozenJob(cutID, roomID, "frozen"))
	})
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeCutGMS) frozenJob(cutID, roomID, stage string) memoryclient.CutJob {
	head := int64(7)
	watermark := int64(3)
	return memoryclient.CutJob{
		CutID: cutID, TenantID: "tenant-a", RoomID: roomID, Stage: stage, JobVersion: 1,
		SpaceScopes: []memoryclient.CutSpaceScope{
			{SpaceID: "space-alpha-shared", ProjectionHeadVersion: &head, QueryWatermark: &watermark},
		},
		UpdatedAt: "2026-09-16T00:00:00Z",
	}
}

func (f *fakeCutGMS) writeJob(w http.ResponseWriter, job memoryclient.CutJob) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(job)
}

func runFreeze(t *testing.T, fake *fakeCutGMS, prefix string, rooms []trainScopeRoom) (graphSnapshot, error) {
	t.Helper()
	client := snapshotCutClient(fake.server.URL, "token-a")
	return freezeGraphSnapshot(context.Background(), client, "tenant-a", prefix, rooms)
}

func TestFreezeGraphSnapshotTriggersAndMerges(t *testing.T) {
	fake := newFakeCutGMS(t, true)
	rooms := []trainScopeRoom{
		{RoomID: "room-alpha-train-1", Spaces: []string{"space-alpha-shared", "space-alpha-private"}},
		{RoomID: "room-alpha-consolidation", Spaces: []string{"space-alpha-consolidation-shared"}},
	}
	snapshot, err := runFreeze(t, fake, "eval1-warm-skill-batch-alpha", rooms)
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.triggers) != 2 {
		t.Fatalf("expected one trigger per room, got %v", fake.triggers)
	}
	if fake.triggers[0].path != "/v1/rooms/room-alpha-train-1/consolidation-cuts" || fake.triggers[1].path != "/v1/rooms/room-alpha-consolidation/consolidation-cuts" {
		t.Errorf("trigger paths wrong: %v", fake.triggers)
	}
	for index, room := range rooms {
		wantKey := "eval1-warm-skill-batch-alpha-" + sanitizeID(room.RoomID)
		if fake.triggers[index].idempotencyKey != wantKey {
			t.Errorf("idempotency key %q, want %q", fake.triggers[index].idempotencyKey, wantKey)
		}
		if fake.triggers[index].mode != "force" || fake.triggers[index].triggerSource != "bench-runner-consolidation" {
			t.Errorf("trigger body wrong: %+v", fake.triggers[index])
		}
	}
	if !reflect.DeepEqual(snapshot.CutIDs, []string{"cut-room-alpha-train-1", "cut-room-alpha-consolidation"}) {
		t.Errorf("cut ids wrong: %v", snapshot.CutIDs)
	}
	if !reflect.DeepEqual(snapshot.RoomIDs, []string{"room-alpha-train-1", "room-alpha-consolidation"}) {
		t.Errorf("room ids wrong: %v", snapshot.RoomIDs)
	}
	if !reflect.DeepEqual(snapshot.Spaces, map[string]int64{"space-alpha-shared": 7}) {
		t.Errorf("merged vector wrong: %v", snapshot.Spaces)
	}
	if got := snapshot.sortedSpaces(); len(got) != 1 || got[0] != "space-alpha-shared" {
		t.Errorf("sortedSpaces wrong: %v", got)
	}
	// The persisted shape must decode back into the same snapshot: records and
	// consolidation summaries serialize this struct verbatim.
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded graphSnapshot
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Errorf("snapshot round-trip mismatch: %v vs %v", decoded, snapshot)
	}
	if !strings.Contains(string(encoded), "cut-room-alpha-train-1") || !strings.Contains(string(encoded), `"spaces"`) {
		t.Errorf("encoded snapshot missing cut ids or spaces: %s", encoded)
	}
}

func TestFreezeGraphSnapshotPollsFreezingJobToFrozen(t *testing.T) {
	fake := newFakeCutGMS(t, false)
	snapshot, err := runFreeze(t, fake, "eval1-arm-alpha", []trainScopeRoom{
		{RoomID: "room-alpha-train-1", Spaces: []string{"space-alpha-shared"}},
	})
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.polls == 0 {
		t.Fatal("a freezing job must be polled before the snapshot freezes")
	}
	if snapshot.Spaces["space-alpha-shared"] != 7 {
		t.Errorf("polled snapshot lost the frozen head: %v", snapshot.Spaces)
	}
}

func TestFreezeGraphSnapshotFailsClosed(t *testing.T) {
	emptyClient := snapshotCutClient("http://127.0.0.1:1", "token")
	if snapshot, err := freezeGraphSnapshot(context.Background(), emptyClient, "tenant-a", "prefix", nil); err != nil || len(snapshot.Spaces) != 0 {
		t.Errorf("no rooms must freeze an empty snapshot without calls, got %v %v", snapshot, err)
	}

	// A failing job stage must abort the freeze.
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(memoryclient.CutJob{
			CutID: "cut-x", Stage: "failed", FailureReasons: []string{"ROOM_EPOCH_STALE"},
			UpdatedAt: "2026-09-16T00:00:00Z",
		})
	}))
	defer failServer.Close()
	client := snapshotCutClient(failServer.URL, "token")
	if _, err := freezeGraphSnapshot(context.Background(), client, "tenant-a", "p", []trainScopeRoom{{RoomID: "room-1"}}); err == nil || !strings.Contains(err.Error(), "failed") {
		t.Errorf("failed stage must error with the stage, got %v", err)
	}

	// A frozen scope without a head version must error, not silently skip.
	headless := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(memoryclient.CutJob{
			CutID: "cut-y", Stage: "frozen",
			SpaceScopes: []memoryclient.CutSpaceScope{{SpaceID: "space-alpha-shared"}},
			UpdatedAt:   "2026-09-16T00:00:00Z",
		})
	}))
	defer headless.Close()
	if _, err := freezeGraphSnapshot(context.Background(), snapshotCutClient(headless.URL, "token"), "tenant-a", "p", []trainScopeRoom{{RoomID: "room-1"}}); err == nil || !strings.Contains(err.Error(), "projection head version") {
		t.Errorf("missing head version must error, got %v", err)
	}

	// Two cuts freezing the same space at different heads are contradictory
	// state — the vector must refuse to merge them.
	disagree := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		head := int64(7)
		if strings.Contains(r.URL.Path, "room-2") {
			head = 9
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(memoryclient.CutJob{
			CutID: "cut-" + r.URL.Path, Stage: "frozen",
			SpaceScopes: []memoryclient.CutSpaceScope{{SpaceID: "space-shared", ProjectionHeadVersion: &head}},
			UpdatedAt:   "2026-09-16T00:00:00Z",
		})
	}))
	defer disagree.Close()
	rooms := []trainScopeRoom{{RoomID: "room-1"}, {RoomID: "room-2"}}
	if _, err := freezeGraphSnapshot(context.Background(), snapshotCutClient(disagree.URL, "token"), "tenant-a", "p", rooms); err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Errorf("disagreeing heads must error, got %v", err)
	}
}
