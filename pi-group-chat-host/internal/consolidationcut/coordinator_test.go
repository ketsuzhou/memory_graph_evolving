package consolidationcut

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"river2.dev/pi-group-chat-host/internal/domain"
)

func freezeRequest(key string) FreezeRequest {
	return FreezeRequest{
		TenantID: "tenant-production", RoomID: "room-production", Mode: "force", IdempotencyKey: key,
	}
}

func requireCoordinatorImplemented(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ErrCoordinatorNotImplemented) {
		t.Fatalf("PG-13 Cut coordinator contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected coordinator error: %v", err)
	}
}

// fixtureFreezeStore pins the deterministic production-room state the red
// tests assert on; Freeze must read every pinned value from the store rather
// than inventing it. Receipt generation can be overridden to exercise
// defective receipt coverage.
type fixtureFreezeStore struct {
	point    RoomFreezePoint
	sealed   []domain.SegmentID
	open     []domain.SegmentID
	receipts func(sealed []domain.SegmentID) []CommitReceipt

	sealedEpochs []int64
}

func (f *fixtureFreezeStore) RoomFreezePoint(context.Context, domain.TenantID, domain.RoomID) (RoomFreezePoint, error) {
	return f.point, nil
}

func (f *fixtureFreezeStore) SealedSegments(_ context.Context, _ domain.RoomID, epoch int64) ([]domain.SegmentID, error) {
	f.sealedEpochs = append(f.sealedEpochs, epoch)
	return append([]domain.SegmentID(nil), f.sealed...), nil
}

func (f *fixtureFreezeStore) OpenSegments(context.Context, domain.RoomID, int64) ([]domain.SegmentID, error) {
	return append([]domain.SegmentID(nil), f.open...), nil
}

func (f *fixtureFreezeStore) WaitForCommitReceipts(_ context.Context, _ domain.RoomID, sealed []domain.SegmentID) ([]CommitReceipt, error) {
	if f.receipts != nil {
		return f.receipts(sealed), nil
	}
	receipts := make([]CommitReceipt, 0, len(sealed))
	for _, segment := range sealed {
		receipts = append(receipts, CommitReceipt{
			ReceiptID: domain.SegmentID("receipt-" + segment),
			SegmentID: segment,
			BatchIDs:  []string{"batch-" + string(segment)},
			Digest:    "sha256:" + string(segment),
		})
	}
	return receipts, nil
}

func newFixtureCoordinator(open ...domain.SegmentID) *Coordinator {
	return NewCoordinator(&fixtureFreezeStore{
		point:  RoomFreezePoint{TenantID: "tenant-production", RoomID: "room-production", Epoch: 11, Sequence: 42},
		sealed: []domain.SegmentID{"segment-1", "segment-2"},
		open:   open,
	})
}

func TestFreezeSealsQueriesAtPinnedEpoch(t *testing.T) {
	t.Parallel()
	store := &fixtureFreezeStore{
		point:  RoomFreezePoint{TenantID: "tenant-production", RoomID: "room-production", Epoch: 11, Sequence: 42},
		sealed: []domain.SegmentID{"segment-1", "segment-2"},
	}
	coordinator := NewCoordinator(store)
	if _, err := coordinator.Freeze(context.Background(), freezeRequest("cut-request-epoch")); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if len(store.sealedEpochs) == 0 || store.sealedEpochs[0] != 11 {
		t.Fatalf("segment enumeration did not read at the pinned freeze epoch: %v", store.sealedEpochs)
	}
}

func TestFreezeRejectsDefectiveReceiptCoverage(t *testing.T) {
	t.Parallel()
	store := &fixtureFreezeStore{
		point:  RoomFreezePoint{TenantID: "tenant-production", RoomID: "room-production", Epoch: 11, Sequence: 42},
		sealed: []domain.SegmentID{"segment-1", "segment-2"},
		receipts: func(sealed []domain.SegmentID) []CommitReceipt {
			// Two receipts for the same segment, one with no digest: the
			// exact-coverage gate must refuse submission.
			return []CommitReceipt{
				{ReceiptID: "receipt-dup", SegmentID: sealed[0], BatchIDs: []string{"batch-1"}, Digest: "sha256:1"},
				{ReceiptID: "receipt-dup", SegmentID: sealed[0], BatchIDs: []string{"batch-1"}, Digest: "sha256:1"},
			}
		},
	}
	snapshot, err := NewCoordinator(store).Freeze(context.Background(), freezeRequest("cut-request-bad-receipts"))
	if err == nil {
		t.Fatalf("defective receipt coverage produced a submit-ready snapshot: %#v", snapshot)
	}
	if snapshot.SubmissionPermitted {
		t.Fatalf("defective receipts permitted submission: %#v", snapshot)
	}
}

func TestFreezeSameKeyDifferentBodyConflicts(t *testing.T) {
	t.Parallel()
	coordinator := newFixtureCoordinator()
	ctx := context.Background()
	if _, err := coordinator.Freeze(ctx, freezeRequest("cut-request-body")); err != nil {
		t.Fatalf("first freeze: %v", err)
	}
	changed := freezeRequest("cut-request-body")
	changed.Mode = ModeThreshold
	if _, err := coordinator.Freeze(ctx, changed); !errors.Is(err, ErrFreezeRequestBodyConflict) {
		t.Fatalf("same key with changed body did not conflict: err=%v", err)
	}
}

func TestFreezePinsImmutableHostHalfSnapshot(t *testing.T) {
	t.Parallel()
	coordinator := newFixtureCoordinator()

	snapshot, err := coordinator.Freeze(context.Background(), freezeRequest("cut-request-1"))
	requireCoordinatorImplemented(t, err)
	if snapshot.RoomEpoch != 11 || snapshot.RoomSequence != 42 {
		t.Fatalf("freeze did not pin room epoch/sequence: %#v", snapshot)
	}
	wantSealed := []domain.SegmentID{"segment-1", "segment-2"}
	if !reflect.DeepEqual(snapshot.SealedSegmentIDs, wantSealed) {
		t.Fatalf("sealed segment snapshot = %#v, want %#v", snapshot.SealedSegmentIDs, wantSealed)
	}
	if len(snapshot.DeferredSegmentIDs) != 0 {
		t.Fatalf("sealed-only snapshot unexpectedly deferred segments: %#v", snapshot.DeferredSegmentIDs)
	}
}

func TestFreezeWaitsForExactReceiptsBeforeSubmission(t *testing.T) {
	t.Parallel()
	coordinator := newFixtureCoordinator()

	snapshot, err := coordinator.Freeze(context.Background(), freezeRequest("cut-request-receipts"))
	requireCoordinatorImplemented(t, err)
	if !snapshot.ReceiptsComplete || !snapshot.SubmissionPermitted {
		t.Fatalf("freeze returned a submit-ready manifest without exact complete receipts: %#v", snapshot)
	}
	if len(snapshot.EvidenceReceipts) != len(snapshot.SealedSegmentIDs) {
		t.Fatalf("receipt coverage = %d, sealed segments = %d; expected exact 1:1 coverage", len(snapshot.EvidenceReceipts), len(snapshot.SealedSegmentIDs))
	}
}

func TestFreezeDefersOpenSegmentsFailClosed(t *testing.T) {
	t.Parallel()
	coordinator := newFixtureCoordinator("segment-open")

	snapshot, err := coordinator.Freeze(context.Background(), freezeRequest("cut-request-open-segment"))
	requireCoordinatorImplemented(t, err)
	wantDeferred := []domain.SegmentID{"segment-open"}
	if !reflect.DeepEqual(snapshot.DeferredSegmentIDs, wantDeferred) {
		t.Fatalf("open segment was silently omitted rather than explicitly deferred: got %#v want %#v", snapshot.DeferredSegmentIDs, wantDeferred)
	}
	if snapshot.SubmissionPermitted {
		t.Fatalf("freeze allowed submission while open segments were deferred: %#v", snapshot)
	}
}

func TestFreezeIsStableForSameIdempotencyKeyAndDistinctForDifferentInputs(t *testing.T) {
	t.Parallel()
	coordinator := newFixtureCoordinator()
	ctx := context.Background()

	first, err := coordinator.Freeze(ctx, freezeRequest("cut-request-stable"))
	requireCoordinatorImplemented(t, err)
	retry, err := coordinator.Freeze(ctx, freezeRequest("cut-request-stable"))
	requireCoordinatorImplemented(t, err)
	if !reflect.DeepEqual(retry, first) {
		t.Fatalf("same idempotency key changed frozen snapshot: first=%#v retry=%#v", first, retry)
	}

	different, err := coordinator.Freeze(ctx, freezeRequest("cut-request-different"))
	requireCoordinatorImplemented(t, err)
	if reflect.DeepEqual(different, first) {
		t.Fatalf("different freeze input reused the prior frozen snapshot: first=%#v different=%#v", first, different)
	}
}
