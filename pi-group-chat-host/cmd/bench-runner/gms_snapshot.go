package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
)

// graphSnapshot is the runner-side version vector of one family's frozen GMS
// state: every consolidation cut the consolidation phase triggered (one per
// train-scope room) and, per space, the frozen projection head version the
// cut's immutable manifest pinned. Test attempts record it verbatim and pin
// their recall reads to it, so a held-out episode reads exactly the frozen
// train graph even if anything wrote behind it later.
type graphSnapshot struct {
	CutIDs  []string         `json:"cut_ids"`
	RoomIDs []string         `json:"room_ids"`
	Spaces  map[string]int64 `json:"spaces"`
}

// sortedSpaces returns the snapshot's space ids in a stable order — the same
// list is handed to the recall scope and stored in records, and map iteration
// order must never leak into either.
func (s *graphSnapshot) sortedSpaces() []string {
	ids := make([]string, 0, len(s.Spaces))
	for spaceID := range s.Spaces {
		ids = append(ids, spaceID)
	}
	sort.Strings(ids)
	return ids
}

// trainScopeRoom is one room the family's train phase drained evidence into,
// together with the spaces a consolidation cut on it freezes.
type trainScopeRoom struct {
	RoomID string
	Spaces []string
}

// trainScopeRoomsForFamily mirrors the arm's space-registration derivation
// exactly: the same iteration order and the same global sequence counter, so
// every returned room/space id is one the arm actually registered. The counter
// advances for every episode of the family (train and test alike), matching
// the registration phase; test rooms are skipped because their throwaway
// spaces are never part of the frozen train scope. The family consolidation
// room closes the scope: batch diagnosis turns run there, and replay/online
// diagnosis turns drain their proposals into it during training.
func trainScopeRoomsForFamily(episodes []manifestEpisode, family string, next *int, strategy skillStrategy) []trainScopeRoom {
	rooms := make([]trainScopeRoom, 0, len(episodes)+1)
	for _, episode := range episodesOfFamily(episodes, family) {
		*next++
		if episode.Split == "test" {
			continue
		}
		_, shared, private, memoryPrivate := warmSkillIsolatedRoom(family, *next, episode.Split)
		rooms = append(rooms, trainScopeRoom{
			RoomID: episodeRoom(family, *next, episode.Split),
			Spaces: []string{shared, private, memoryPrivate},
		})
		if strategy == skillStrategyBatch {
			diagRoom, diagShared, diagPrivate, diagMemoryPrivate := warmSkillDiagnosisRoom(family, *next)
			rooms = append(rooms, trainScopeRoom{
				RoomID: diagRoom,
				Spaces: []string{diagShared, diagPrivate, diagMemoryPrivate},
			})
		}
	}
	consRoom, consShared, consPrivate, consMemoryPrivate := warmSkillConsolidationRoom(family)
	rooms = append(rooms, trainScopeRoom{RoomID: consRoom, Spaces: []string{consShared, consPrivate, consMemoryPrivate}})
	return rooms
}

// cutRoomSpaceBindings renders the family→room→spaces map as the repeatable
// -cut-room-space room=space1,space2 server-flag values a local GMS must be
// started with (and a remote GMS deployment must be configured with).
func cutRoomSpaceBindings(rooms []trainScopeRoom) []string {
	bindings := make([]string, 0, len(rooms))
	for _, room := range rooms {
		bindings = append(bindings, room.RoomID+"="+strings.Join(room.Spaces, ","))
	}
	return bindings
}

// graphSnapshotFreezeTimeout bounds the whole per-room trigger/poll exchange;
// the freeze itself is synchronous server-side, so this is generous.
const graphSnapshotFreezeTimeout = 5 * time.Minute

// freezeGraphSnapshot triggers one force consolidation cut per train-scope
// room, polls each job to its frozen stage, and merges the frozen manifests'
// per-space head versions into one vector. Every trigger carries a
// deterministic idempotency key, so a retried consolidation replays the
// original frozen job instead of re-freezing at a moved head.
func freezeGraphSnapshot(ctx context.Context, client *memoryclient.Client, tenantID, idempotencyPrefix string, rooms []trainScopeRoom) (graphSnapshot, error) {
	snapshot := graphSnapshot{CutIDs: []string{}, RoomIDs: []string{}, Spaces: map[string]int64{}}
	if len(rooms) == 0 {
		return snapshot, nil
	}
	freezeCtx, cancel := context.WithTimeout(ctx, graphSnapshotFreezeTimeout)
	defer cancel()
	for _, room := range rooms {
		key := idempotencyPrefix + "-" + sanitizeID(room.RoomID)
		job, err := client.TriggerConsolidationCut(freezeCtx, room.RoomID, key, memoryclient.TriggerConsolidationCutRequest{
			Mode: "force", TriggerSource: "bench-runner-consolidation",
		})
		if err != nil {
			return graphSnapshot{}, fmt.Errorf("trigger cut for %s: %w", room.RoomID, err)
		}
		job, err = pollCutToFrozen(freezeCtx, client, job)
		if err != nil {
			return graphSnapshot{}, fmt.Errorf("freeze cut for %s: %w", room.RoomID, err)
		}
		for _, scope := range job.SpaceScopes {
			if scope.ProjectionHeadVersion == nil {
				return graphSnapshot{}, fmt.Errorf("cut %s froze space %s without a projection head version", job.CutID, scope.SpaceID)
			}
			if previous, ok := snapshot.Spaces[scope.SpaceID]; ok && previous != *scope.ProjectionHeadVersion {
				return graphSnapshot{}, fmt.Errorf("cuts disagree on space %s: %d vs %d", scope.SpaceID, previous, *scope.ProjectionHeadVersion)
			}
			snapshot.Spaces[scope.SpaceID] = *scope.ProjectionHeadVersion
		}
		snapshot.CutIDs = append(snapshot.CutIDs, job.CutID)
		snapshot.RoomIDs = append(snapshot.RoomIDs, room.RoomID)
	}
	return snapshot, nil
}

// pollCutToFrozen waits for the job to leave the in-flight set. A locally
// triggered force freeze lands on frozen (or a failure stage) before the
// create response returns, so the poll is a robustness net for remote GMS and
// restart-restored compositions, not the expected path.
func pollCutToFrozen(ctx context.Context, client *memoryclient.Client, job memoryclient.CutJob) (memoryclient.CutJob, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		switch job.Stage {
		case "frozen", "completed":
			return job, nil
		case "failed", "cancelled", "partially_cancelled", "partially_failed":
			return memoryclient.CutJob{}, fmt.Errorf("cut %s ended in stage %s (reasons %v)", job.CutID, job.Stage, job.FailureReasons)
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return memoryclient.CutJob{}, fmt.Errorf("cut %s did not reach frozen in time (stage %s)", job.CutID, job.Stage)
		}
		time.Sleep(200 * time.Millisecond)
		updated, err := client.ConsolidationCut(ctx, job.CutID)
		if err != nil {
			return memoryclient.CutJob{}, fmt.Errorf("poll cut %s: %w", job.CutID, err)
		}
		job = updated
	}
}

// snapshotCutClient builds the dedicated HTTP client for cut calls: freeze
// exchanges are longer than the recall-path timeout the shared client uses.
func snapshotCutClient(baseURL, token string) *memoryclient.Client {
	return memoryclient.NewClient(baseURL, token, &http.Client{Timeout: 60 * time.Second}, 1<<20)
}

// armFreezesGraph reports whether the arm freezes GMS state at consolidation
// and pins test reads to it. The continual stream never freezes.
func armFreezesGraph(arm string) bool {
	strategy, ok := skillStrategyFor(arm)
	return ok && strategy != skillStrategyContinual
}

// armCutBindings derives the -cut-room-space server bindings every skill arm
// that freezes needs, across all families, in arm registration order.
func armCutBindings(episodes []manifestEpisode, arm string) []string {
	strategy, ok := skillStrategyFor(arm)
	if !ok || strategy == skillStrategyContinual {
		return nil
	}
	var bindings []string
	next := 0
	for _, family := range familySequence(episodes) {
		bindings = append(bindings, cutRoomSpaceBindings(trainScopeRoomsForFamily(episodes, family, &next, strategy))...)
	}
	return bindings
}
