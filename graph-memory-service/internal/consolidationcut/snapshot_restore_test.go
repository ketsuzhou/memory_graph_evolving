package consolidationcut

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func snapshotFixture(t *testing.T) (*Service, Job, []byte) {
	t.Helper()
	service := NewService(nil, nil)
	job, _, err := service.Trigger(context.Background(), triggerRequest("snapshot-round-trip", ModeForce))
	requireCutImplemented(t, err)
	image, err := service.Snapshot()
	if err != nil {
		t.Fatalf("snapshot service: %v", err)
	}
	return service, job, image
}

func decodeServiceSnapshot(t *testing.T, image []byte) serviceSnapshot {
	t.Helper()
	var snapshot serviceSnapshot
	if err := json.Unmarshal(image, &snapshot); err != nil {
		t.Fatalf("decode service snapshot: %v", err)
	}
	return snapshot
}

func encodeServiceSnapshot(t *testing.T, snapshot serviceSnapshot) []byte {
	t.Helper()
	image, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode service snapshot: %v", err)
	}
	return image
}

func requireRestoreRejectedWithoutMutation(t *testing.T, service *Service, original Job, image []byte, want string) {
	t.Helper()
	if err := service.Restore(image); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("restore error = %v, want failure containing %q", err, want)
	}
	after, err := service.Get(context.Background(), original.TenantID, original.CutID)
	if err != nil {
		t.Fatalf("get job after rejected restore: %v", err)
	}
	if after.Stage != original.Stage || after.JobVersion != original.JobVersion || after.CutDigest != original.CutDigest {
		t.Fatalf("rejected restore mutated live job: got %#v, want %#v", after, original)
	}
}

func TestServiceRestoreRoundTripAndRejectsBrokenReferences(t *testing.T) {
	t.Parallel()

	service, job, image := snapshotFixture(t)

	t.Run("valid round trip", func(t *testing.T) {
		restored := NewService(nil, nil)
		if err := restored.Restore(image); err != nil {
			t.Fatalf("restore valid image: %v", err)
		}
		got, err := restored.Get(context.Background(), job.TenantID, job.CutID)
		if err != nil {
			t.Fatalf("get restored job: %v", err)
		}
		if got.Stage != job.Stage || got.JobVersion != job.JobVersion || got.CutDigest != job.CutDigest {
			t.Fatalf("restored job = %#v, want %#v", got, job)
		}
		manifest, err := restored.GetManifest(context.Background(), job.TenantID, job.CutID)
		if err != nil {
			t.Fatalf("get restored manifest: %v", err)
		}
		if manifest.CutID != job.CutID || manifest.TenantID != job.TenantID || manifest.RoomID != job.RoomID {
			t.Fatalf("restored manifest identity = %#v", manifest)
		}
		replayed, duplicate, err := restored.Trigger(context.Background(), triggerRequest("snapshot-round-trip", ModeForce))
		if err != nil || !duplicate || replayed.CutID != job.CutID {
			t.Fatalf("restored idempotency replay = (%#v, %v, %v), want original duplicate", replayed, duplicate, err)
		}
	})

	t.Run("dangling idempotency target", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		for key := range snapshot.Idempotency {
			snapshot.Idempotency[key] = "cut-missing"
			break
		}
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "targets unknown cut")
	})

	t.Run("room ledger tenant room mismatch", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		snapshot.RoomInFlight[string(job.TenantID)] = map[string]string{"other-room": string(job.CutID)}
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "does not match job")
	})

	t.Run("idempotency request mismatch", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		for key, cutID := range snapshot.Idempotency {
			if cutID == "" {
				continue
			}
			snapshot.Requests[key] = TriggerRequest{
				TenantID:       "tenant-other",
				PrincipalID:    "operator-other",
				RoomID:         "room-other",
				Mode:           ModeForce,
				TriggerSource:  "other-source",
				IdempotencyKey: "other-key",
			}
			break
		}
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "does not match its recorded request")
	})

	t.Run("job key does not match CutID", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		for key := range snapshot.Jobs {
			if key != string(job.CutID) {
				continue
			}
			snapshot.Jobs[key] = Job{CutID: CutID("cut-renamed"), TenantID: job.TenantID, RoomID: job.RoomID}
			break
		}
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "does not match job.CutID")
	})
}

// A snapshot whose job and manifest sets are not in exact 1:1 identity
// correspondence would restore a split authoritative state (GET job
// succeeds while GET manifest misses, or vice versa); every such image must
// fail closed leaving the receiver untouched.
func TestServiceRestoreRejectsOrphanJobsAndManifests(t *testing.T) {
	t.Parallel()

	service, job, image := snapshotFixture(t)

	t.Run("job without manifest", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		delete(snapshot.Manifests, string(job.CutID))
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "has no manifest")
	})

	t.Run("manifest without job", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		delete(snapshot.Jobs, string(job.CutID))
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "has no job")
	})

	t.Run("manifest identity mismatch", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		manifest := snapshot.Manifests[string(job.CutID)]
		manifest.TenantID = "tenant-other"
		snapshot.Manifests[string(job.CutID)] = manifest
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "identity does not match job")
	})

	t.Run("manifest cut digest mismatch", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		manifest := snapshot.Manifests[string(job.CutID)]
		manifest.CutDigest = "sha256:" + strings.Repeat("ab", 32)
		snapshot.Manifests[string(job.CutID)] = manifest
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "cut digest does not match job")
	})

	t.Run("missing core map", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		snapshot.RoomLastCut = nil
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "invalid or inconsistent shape")
	})
}

// A snapshot injected with a forged room-serial ledger would resurrect a
// blocked or double-active room; Restore must reject every direction of the
// runtime invariants maintained by Trigger/Transition/Cancel/Resume/
// Rediagnose, leaving the receiver untouched.
func TestServiceRestoreRejectsBrokenRoomSerialLedgers(t *testing.T) {
	t.Parallel()

	service, job, image := snapshotFixture(t)

	t.Run("in-flight job does not occupy its room", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		delete(snapshot.RoomInFlight, string(job.TenantID))
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "does not occupy its room")
	})

	t.Run("room_in_flight points at non-in-flight job", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		frozen := snapshot.Jobs[string(job.CutID)]
		frozen.Stage = StageCompleted
		snapshot.Jobs[string(job.CutID)] = frozen
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "points at non-in-flight job")
	})

	// failed is neither in-flight nor terminal: an occupancy entry pointing
	// at a failed job would restore a permanently blocked room. (FailedFrom
	// is given a plausible origin so the FailedFrom check is not what
	// rejects — this subtest exercises the occupancy invariant.)
	t.Run("room_in_flight points at failed job", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		frozen := snapshot.Jobs[string(job.CutID)]
		frozen.Stage = StageFailed
		frozen.FailedFrom = StageFreezing
		snapshot.Jobs[string(job.CutID)] = frozen
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "points at non-in-flight job")
	})

	t.Run("room_last_cut points at in-flight job", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		if snapshot.RoomLastCut[string(job.TenantID)] == nil {
			snapshot.RoomLastCut[string(job.TenantID)] = map[string]string{}
		}
		snapshot.RoomLastCut[string(job.TenantID)][string(job.RoomID)] = string(job.CutID)
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "points at in-flight job")
	})

	// FailedFrom is Resume's re-entry target: a corrupted non-in-flight value
	// would resume the job into an illegal stage.
	t.Run("failed-from stage not resumable", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		frozen := snapshot.Jobs[string(job.CutID)]
		frozen.FailedFrom = StageCompleted
		snapshot.Jobs[string(job.CutID)] = frozen
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "is not resumable")
	})
}

// Every restored cut was minted by exactly one idempotent trigger, so the
// binding must be exact in both directions: a snapshot stripped of a cut's
// idempotency+request pair (or with a tampered request identity) would
// silently change what replaying that key re-runs — Restore must reject it.
func TestServiceRestoreRejectsBrokenIdempotencyBindings(t *testing.T) {
	t.Parallel()

	service, job, image := snapshotFixture(t)

	t.Run("binding pair stripped", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		for key := range snapshot.Idempotency {
			delete(snapshot.Idempotency, key)
			delete(snapshot.Requests, key)
			break
		}
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "want exactly 1")
	})

	t.Run("request mode tampered", func(t *testing.T) {
		snapshot := decodeServiceSnapshot(t, image)
		for key := range snapshot.Requests {
			request := snapshot.Requests[key]
			request.Mode = ModeThreshold
			snapshot.Requests[key] = request
			break
		}
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "does not match the frozen manifest")
	})
}

// FailedFrom is Resume's re-entry target and must name an ACTUAL failure
// origin — the transition table's failed-edge sources (plus the internal
// freezing failure point), not merely any in-flight stage. frozen, queued,
// and cancelling are in-flight but unreachable as failure origins.
func TestServiceRestoreRejectsIllegalFailedFromStages(t *testing.T) {
	t.Parallel()

	for _, stage := range []Stage{StageFrozen, StageQueued, StageCancelling} {
		service, job, image := snapshotFixture(t)
		snapshot := decodeServiceSnapshot(t, image)
		frozen := snapshot.Jobs[string(job.CutID)]
		frozen.FailedFrom = stage
		snapshot.Jobs[string(job.CutID)] = frozen
		requireRestoreRejectedWithoutMutation(t, service, job, encodeServiceSnapshot(t, snapshot), "is not resumable")
	}
}

// A FAILED job must record its actual failure origin: stripping FailedFrom
// from an otherwise-legal snapshot must not restore — Resume would otherwise
// default the corruption to diagnosing. Both failure lineages are covered:
// the internal frozen-guard failure (FailedFrom=freezing) and the
// diagnosing→failed transition.
func TestServiceRestoreRejectsFailedJobWithoutFailedFrom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	buildFailedService := func(t *testing.T, key string, internalFailure bool) (*Service, Job, []byte) {
		t.Helper()
		service := NewService(nil, nil)
		authority := newStubGuardAuthority()
		authority.register("annotation-no-failedfrom", "diagnosis-v1")
		service.SetGuardAuthority(authority)
		var failed Job
		if internalFailure {
			authority.setErr(errors.New("receipts store unavailable"))
			job, _, err := service.Trigger(ctx, triggerRequest(key, ModeForce))
			authority.setErr(nil)
			if !errors.Is(err, ErrGuardAuthorityUnavailable) || job.Stage != StageFailed {
				t.Fatalf("seed internal failure = (job=%#v, err=%v), want transiently failed", job, err)
			}
			failed = job
		} else {
			job := triggerForcedJob(t, service, ctx, key)
			diagnosing, err := service.Transition(ctx, TransitionRequest{
				TenantID: job.TenantID, CutID: job.CutID, ExpectedJobVersion: job.JobVersion, To: StageDiagnosing,
			})
			requireCutImplemented(t, err)
			failed, err = service.Transition(ctx, TransitionRequest{
				TenantID: diagnosing.TenantID, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
				To: StageFailed, Reason: "INFRASTRUCTURE_FAILURE",
			})
			requireCutImplemented(t, err)
		}
		image, err := service.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		return service, failed, image
	}

	t.Run("internal frozen-guard failure lineage", func(t *testing.T) {
		service, failed, image := buildFailedService(t, "no-failedfrom-internal", true)
		if failed.FailedFrom != StageFreezing {
			t.Fatalf("seed FailedFrom = %s, want freezing", failed.FailedFrom)
		}
		snapshot := decodeServiceSnapshot(t, image)
		stripped := snapshot.Jobs[string(failed.CutID)]
		stripped.FailedFrom = ""
		snapshot.Jobs[string(failed.CutID)] = stripped
		requireRestoreRejectedWithoutMutation(t, service, failed, encodeServiceSnapshot(t, snapshot), "does not record a resumable failed-from stage")
	})

	t.Run("diagnosing transition failure lineage", func(t *testing.T) {
		service, failed, image := buildFailedService(t, "no-failedfrom-transition", false)
		if failed.FailedFrom != StageDiagnosing {
			t.Fatalf("seed FailedFrom = %s, want diagnosing", failed.FailedFrom)
		}
		snapshot := decodeServiceSnapshot(t, image)
		stripped := snapshot.Jobs[string(failed.CutID)]
		stripped.FailedFrom = ""
		snapshot.Jobs[string(failed.CutID)] = stripped
		requireRestoreRejectedWithoutMutation(t, service, failed, encodeServiceSnapshot(t, snapshot), "does not record a resumable failed-from stage")
	})

	// Defense in depth at the runtime boundary: with the corrupt state that
	// Restore now rejects unreachable through the public API, inject it
	// directly and confirm Resume still fails closed instead of defaulting to
	// diagnosing.
	t.Run("runtime resume without failure origin is refused", func(t *testing.T) {
		_, failed, image := buildFailedService(t, "no-failedfrom-runtime", false)
		restored := NewService(nil, nil)
		if err := restored.Restore(image); err != nil {
			t.Fatalf("restore: %v", err)
		}
		restored.mu.Lock()
		stored := restored.jobs[failed.CutID]
		stored.FailedFrom = ""
		restored.mu.Unlock()
		if _, err := restored.Resume(ctx, ResumeRequest{
			TenantID: failed.TenantID, CutID: failed.CutID, ExpectedJobVersion: failed.JobVersion,
		}); !errors.Is(err, ErrNotResumable) {
			t.Fatalf("resume without failure origin = %v, want ErrNotResumable", err)
		}
	})
}
