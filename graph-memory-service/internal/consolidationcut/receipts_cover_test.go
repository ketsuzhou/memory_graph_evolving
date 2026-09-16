package consolidationcut

import (
	"errors"
	"testing"
)

// TestReceiptsCoverExactlyBidirectional locks the frozen receipt-coverage
// guard (SC-4.2): the receipt set must be EXACTLY the sealed set — one receipt
// per sealed segment, no duplicate receipts, no duplicate sealed segments, and
// no receipt for a segment outside the frozen set. These were tightened after
// review: the OLD check let sealed [A,A] with receipts [A,B] pass, silently
// accepting coverage beyond the sealed surface.
func TestReceiptsCoverExactlyBidirectional(t *testing.T) {
	seg := func(ids ...SegmentID) []SegmentID { return ids }
	receipt := func(segment SegmentID) EvidenceCommitReceipt {
		return EvidenceCommitReceipt{ReceiptID: ReceiptID("receipt-" + string(segment)), SegmentID: segment}
	}

	// Accept: one receipt per distinct sealed segment.
	if err := receiptsCoverExactly(Manifest{
		SealedSegmentIDs:       seg("A", "B"),
		EvidenceCommitReceipts: []EvidenceCommitReceipt{receipt("A"), receipt("B")},
	}); err != nil {
		t.Fatalf("exact coverage rejected: %v", err)
	}

	// Reject: duplicate sealed segment + out-of-set receipt (old bug).
	if err := receiptsCoverExactly(Manifest{
		SealedSegmentIDs:       seg("A", "A"),
		EvidenceCommitReceipts: []EvidenceCommitReceipt{receipt("A"), receipt("B")},
	}); !errors.Is(err, ErrReceiptCoverageIncomplete) {
		t.Fatalf("duplicate sealed + out-of-set receipt: want ErrReceiptCoverageIncomplete, got %v", err)
	}

	// Reject: receipt for a segment not sealed (cardinality matched via dup).
	if err := receiptsCoverExactly(Manifest{
		SealedSegmentIDs:       seg("A", "A"),
		EvidenceCommitReceipts: []EvidenceCommitReceipt{receipt("A"), receipt("A")},
	}); !errors.Is(err, ErrReceiptCoverageIncomplete) {
		t.Fatalf("unsealed segment receipt: want ErrReceiptCoverageIncomplete, got %v", err)
	}

	// Reject: duplicate receipts.
	if err := receiptsCoverExactly(Manifest{
		SealedSegmentIDs:       seg("A", "B"),
		EvidenceCommitReceipts: []EvidenceCommitReceipt{receipt("A"), receipt("A")},
	}); !errors.Is(err, ErrReceiptCoverageIncomplete) {
		t.Fatalf("duplicate receipt: want ErrReceiptCoverageIncomplete, got %v", err)
	}

	// Reject: sealed segment with no receipt and a receipt outside the set.
	if err := receiptsCoverExactly(Manifest{
		SealedSegmentIDs:       seg("A", "B"),
		EvidenceCommitReceipts: []EvidenceCommitReceipt{receipt("A"), receipt("C")},
	}); !errors.Is(err, ErrReceiptCoverageIncomplete) {
		t.Fatalf("missing B + extra C: want ErrReceiptCoverageIncomplete, got %v", err)
	}

	// Reject: count mismatch (one receipt, two sealed).
	if err := receiptsCoverExactly(Manifest{
		SealedSegmentIDs:       seg("A", "B"),
		EvidenceCommitReceipts: []EvidenceCommitReceipt{receipt("A")},
	}); !errors.Is(err, ErrReceiptCoverageIncomplete) {
		t.Fatalf("missing receipt: want ErrReceiptCoverageIncomplete, got %v", err)
	}
}
