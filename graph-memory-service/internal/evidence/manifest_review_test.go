package evidence_test

import (
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

// TestReviewStageValidatesManifestInUseCase proves the manifest invariants live
// in the evidence use case, not only at the HTTP edge: a direct service caller
// staging a dangling link is rejected and nothing is stored.
func TestReviewStageValidatesManifestInUseCase(t *testing.T) {
	fx := newEvidenceFixture(t)

	batch := fx.validBatch("batch-manifest-review", "outbox-manifest-review")
	batch.Links = []domain.EvidenceLink{{
		ID: "link-dangling", FromEventID: "event-missing", ToEventID: batch.Events[0].ID, Relation: "mentions",
	}}
	_, _, err := fx.service.Stage(fx.ctx, fx.tenantID, fx.principal, batch)
	requireManifestRejection(t, err, "dangling link")

	staged, duplicate, err := fx.service.Stage(fx.ctx, fx.tenantID, fx.principal, fx.validBatch("batch-manifest-review", "outbox-manifest-review"))
	if err != nil || duplicate {
		t.Fatalf("valid stage after rejection = (%v, %v)", duplicate, err)
	}
	if staged.State != domain.EvidenceBatchStaged {
		t.Fatalf("staged state = %q, want staged", staged.State)
	}
}

// TestReviewCommitRevalidatesStagedManifest proves commit re-runs the manifest
// validation: even a batch that somehow reached the store in an invalid state
// cannot be published.
func TestReviewCommitRevalidatesStagedManifest(t *testing.T) {
	fx := newEvidenceFixture(t)

	invalid := fx.validBatch("batch-commit-review", "outbox-commit-review")
	invalid.Events = nil
	// Bypass the use case so the store holds an invalid staged batch.
	if _, _, err := fx.store.Stage(fx.ctx, invalid); err != nil {
		t.Fatalf("seed invalid staged batch: %v", err)
	}

	_, _, err := fx.service.Commit(fx.ctx, fx.tenantID, fx.principal, invalid.ID, "commit-commit-review")
	requireManifestRejection(t, err, "empty events")

	stillStaged, err := fx.service.Batch(fx.ctx, fx.tenantID, fx.principal, invalid.ID)
	if err != nil {
		t.Fatalf("batch after rejected commit: %v", err)
	}
	if stillStaged.State != domain.EvidenceBatchStaged {
		t.Fatalf("state after rejected commit = %q, want staged (nothing published)", stillStaged.State)
	}
}

func (f evidenceFixture) validBatch(id, key string) domain.EvidenceBatch {
	return domain.EvidenceBatch{
		ID:              domain.BatchID(id),
		TenantID:        f.tenantID,
		IdempotencyKey:  key,
		SpaceID:         f.shared,
		StreamID:        "stream-" + id,
		SourceSegmentID: "segment-" + id,
		Provenance: domain.Provenance{
			HostType:       "pi-group-chat-host",
			HostInstanceID: "host-review",
			SourceKind:     "room_shared",
			CapturedAt:     f.clock.Now().Add(-time.Minute),
			ContentSHA256:  "d41d8cd98f00b204e9800998ecf8427ed41d8cd98f00b204e9800998ecf8427e",
		},
		Events: []domain.EvidenceEvent{{
			ID: "event-" + id, Sequence: 1, Kind: "room_message",
			Content: "Manifest review fact.", OccurredAt: f.clock.Now().Add(-time.Minute),
		}},
		Links:           []domain.EvidenceLink{},
		TerminalOutcome: "settled",
	}
}

func requireManifestRejection(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: stage/commit succeeded, want manifest rejection", context)
	}
	var typed *domain.ProtocolError
	if !asProtocol(err, &typed) {
		t.Fatalf("%s: error %T (%v) is not a protocol error", context, err, err)
	}
	if typed.Status != 400 || typed.Code() != "INVALID_REQUEST" {
		t.Fatalf("%s: rejection = (%d, %q), want (400, INVALID_REQUEST)", context, typed.Status, typed.Code())
	}
}

func asProtocol(err error, target **domain.ProtocolError) bool {
	typed, ok := err.(*domain.ProtocolError)
	if ok {
		*target = typed
	}
	return ok
}
