package consolidationcut

import (
	"context"
	"errors"
	"sync"

	"river2.dev/pi-group-chat-host/internal/domain"
)

// ErrCoordinatorNotImplemented is kept as the red-stage detector for the PG-13
// contract tests; no green-path code returns it.
var ErrCoordinatorNotImplemented = errors.New("consolidation cut coordinator not implemented")

// Freeze request modes, mirroring the frozen Cut HTTP seam.
type Mode string

const (
	ModeThreshold Mode = "threshold"
	ModeForce     Mode = "force"
)

var (
	// ErrFreezeStoreRequired is returned when the coordinator is constructed
	// without the freeze-point store it reads room state from.
	ErrFreezeStoreRequired = errors.New("consolidation cut coordinator requires a freeze store")
	// ErrIdempotencyKeyRequired guards the stability contract: a freeze
	// without a key has no identity to be stable under.
	ErrIdempotencyKeyRequired = errors.New("consolidation cut freeze requires an idempotency key")
	// ErrInvalidMode rejects freeze requests outside threshold|force.
	ErrInvalidMode = errors.New("consolidation cut freeze mode must be threshold or force")
	// ErrFreezeRequestBodyConflict is returned when the same idempotency key
	// is reused for a different (tenant, room, mode) freeze body.
	ErrFreezeRequestBodyConflict = errors.New("consolidation cut freeze body conflicts with the stored snapshot for this key")
	// ErrRoomIdentityMismatch rejects a store freeze point that does not
	// match the requested tenant/room identity.
	ErrRoomIdentityMismatch = errors.New("freeze point does not match the requested tenant/room")
	// ErrReceiptCoverageIncomplete is returned when commit receipts do not
	// cover the sealed segment set exactly 1:1 (SC-4.4).
	ErrReceiptCoverageIncomplete = errors.New("commit receipts do not cover the sealed segments exactly")
)

type RoomFreezePoint struct {
	TenantID domain.TenantID
	RoomID   domain.RoomID
	Epoch    int64
	Sequence int64
}

type CommitReceipt struct {
	ReceiptID domain.SegmentID
	SegmentID domain.SegmentID
	BatchIDs  []string
	Digest    string
}

// FreezeStore is consumer-owned: the coordinator reads the Host freeze point,
// enumerates sealed/open segments, and waits for exact commit receipts.
type FreezeStore interface {
	RoomFreezePoint(context.Context, domain.TenantID, domain.RoomID) (RoomFreezePoint, error)
	SealedSegments(context.Context, domain.RoomID, int64) ([]domain.SegmentID, error)
	OpenSegments(context.Context, domain.RoomID, int64) ([]domain.SegmentID, error)
	WaitForCommitReceipts(context.Context, domain.RoomID, []domain.SegmentID) ([]CommitReceipt, error)
}

type FreezeRequest struct {
	TenantID       domain.TenantID
	RoomID         domain.RoomID
	Mode           Mode
	IdempotencyKey string
}

type HostFreezeSnapshot struct {
	TenantID            domain.TenantID
	RoomID              domain.RoomID
	RoomEpoch           int64
	RoomSequence        int64
	SealedSegmentIDs    []domain.SegmentID
	DeferredSegmentIDs  []domain.SegmentID
	EvidenceReceipts    []CommitReceipt
	IdempotencyKey      string
	ReceiptsComplete    bool
	SubmissionPermitted bool
}

// Coordinator is the PG-13 Host half of the two-phase Cut freeze (SC-4.4): it
// pins one immutable snapshot of the room's epoch, sequence, and sealed
// segment set, explicitly defers open segments fail-closed, and only permits
// submission once commit receipts cover the sealed set exactly. Snapshots are
// memoized per (tenant, room, mode, idempotency key) so retries observe the
// identical freeze; the store calls run outside the mutex so one room's slow
// receipt wait never blocks unrelated freezes.
type Coordinator struct {
	store FreezeStore

	mu     sync.Mutex
	frozen map[string]HostFreezeSnapshot
	bodies map[string]FreezeRequest
}

func NewCoordinator(store FreezeStore) *Coordinator {
	return &Coordinator{store: store, frozen: map[string]HostFreezeSnapshot{}, bodies: map[string]FreezeRequest{}}
}

func (c *Coordinator) Freeze(ctx context.Context, req FreezeRequest) (HostFreezeSnapshot, error) {
	if req.IdempotencyKey == "" {
		return HostFreezeSnapshot{}, ErrIdempotencyKeyRequired
	}
	if req.Mode != ModeThreshold && req.Mode != ModeForce {
		return HostFreezeSnapshot{}, ErrInvalidMode
	}
	key := freezeMemoKey(req)

	c.mu.Lock()
	if snapshot, ok := c.frozen[key]; ok {
		stored := c.bodies[key]
		c.mu.Unlock()
		if stored != req {
			return HostFreezeSnapshot{}, ErrFreezeRequestBodyConflict
		}
		return cloneSnapshot(snapshot), nil
	}
	if c.store == nil {
		c.mu.Unlock()
		return HostFreezeSnapshot{}, ErrFreezeStoreRequired
	}
	c.mu.Unlock()

	point, err := c.store.RoomFreezePoint(ctx, req.TenantID, req.RoomID)
	if err != nil {
		return HostFreezeSnapshot{}, err
	}
	if point.TenantID != req.TenantID || point.RoomID != req.RoomID {
		return HostFreezeSnapshot{}, ErrRoomIdentityMismatch
	}
	sealed, err := c.store.SealedSegments(ctx, req.RoomID, point.Epoch)
	if err != nil {
		return HostFreezeSnapshot{}, err
	}
	open, err := c.store.OpenSegments(ctx, req.RoomID, point.Epoch)
	if err != nil {
		return HostFreezeSnapshot{}, err
	}
	snapshot := HostFreezeSnapshot{
		TenantID:           req.TenantID,
		RoomID:             req.RoomID,
		RoomEpoch:          point.Epoch,
		RoomSequence:       point.Sequence,
		SealedSegmentIDs:   sealed,
		DeferredSegmentIDs: open,
		IdempotencyKey:     req.IdempotencyKey,
	}
	// Fail closed on open segments: they are named explicitly in the
	// snapshot, receipts are not awaited, and submission stays forbidden
	// until a fresh freeze after they seal.
	if len(open) == 0 {
		receipts, err := c.store.WaitForCommitReceipts(ctx, req.RoomID, sealed)
		if err != nil {
			return HostFreezeSnapshot{}, err
		}
		complete, err := receiptsCoverExactly(sealed, receipts)
		if err != nil {
			return HostFreezeSnapshot{}, err
		}
		snapshot.EvidenceReceipts = receipts
		snapshot.ReceiptsComplete = complete
		snapshot.SubmissionPermitted = complete
	}

	c.mu.Lock()
	if existing, ok := c.frozen[key]; ok {
		// A concurrent freeze with the same key won the memo; it observed
		// the same room state, so it is the canonical snapshot.
		c.mu.Unlock()
		_ = existing
		return cloneSnapshot(c.frozen[key]), nil
	}
	c.frozen[key] = cloneSnapshot(snapshot)
	c.bodies[key] = req
	c.mu.Unlock()
	return cloneSnapshot(snapshot), nil
}

func freezeMemoKey(req FreezeRequest) string {
	// The idempotency key is the request identity; the stored body (which
	// includes mode) is what detects same-key/different-body conflicts.
	return string(req.TenantID) + "\x00" + string(req.RoomID) + "\x00" + req.IdempotencyKey
}

// receiptsCoverExactly enforces the exact-coverage seam: one receipt per
// sealed segment with a unique non-empty receipt identity and non-empty
// batch/digest provenance, no duplicate receipt identities, and no receipt
// for a segment outside the frozen set.
func receiptsCoverExactly(sealed []domain.SegmentID, receipts []CommitReceipt) (bool, error) {
	if len(receipts) != len(sealed) {
		return false, nil
	}
	covered := make(map[domain.SegmentID]bool, len(sealed))
	receiptIDs := make(map[domain.SegmentID]bool, len(sealed))
	for _, receipt := range receipts {
		if receipt.ReceiptID == "" || receipt.Digest == "" || len(receipt.BatchIDs) == 0 {
			return false, ErrReceiptCoverageIncomplete
		}
		if covered[receipt.SegmentID] || receiptIDs[receipt.ReceiptID] {
			return false, ErrReceiptCoverageIncomplete
		}
		covered[receipt.SegmentID] = true
		receiptIDs[receipt.ReceiptID] = true
	}
	for _, segment := range sealed {
		if !covered[segment] {
			return false, nil
		}
	}
	return true, nil
}

func cloneSnapshot(snapshot HostFreezeSnapshot) HostFreezeSnapshot {
	frozen := snapshot
	frozen.SealedSegmentIDs = append([]domain.SegmentID(nil), snapshot.SealedSegmentIDs...)
	frozen.DeferredSegmentIDs = append([]domain.SegmentID(nil), snapshot.DeferredSegmentIDs...)
	frozen.EvidenceReceipts = make([]CommitReceipt, len(snapshot.EvidenceReceipts))
	for index, receipt := range snapshot.EvidenceReceipts {
		frozen.EvidenceReceipts[index] = receipt
		frozen.EvidenceReceipts[index].BatchIDs = append([]string(nil), receipt.BatchIDs...)
	}
	return frozen
}
