package memory

// HST-202 store-adapter tests: the segment/seal store is append-only, its
// committed seal records reread with identical digests, close idempotency
// keys reject different frozen inputs, and open-segment appends enforce the
// DAG freeze invariants (Host 3.2/3.7).

import (
	"context"
	"testing"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/segment"
)

func testOpenSegment(id string, start int64) segment.OpenSegment {
	return segment.OpenSegment{
		RoomID:                  "room-test",
		SegmentID:               id,
		SegmentVersion:          1,
		FrontierStart:           start,
		ClosePolicyRef:          segment.Ref{ID: "host.segment-close-policy", Version: 1, Digest: "sha256:" + repeat("a", 64)},
		ScopeProfileRef:         segment.Ref{ID: "host.scope-profile-alpha", Version: 1, Digest: "sha256:" + repeat("b", 64)},
		PathExtractionPolicyRef: segment.Ref{ID: "host.path-extraction-policy", Version: 1, Digest: "sha256:" + repeat("c", 64)},
	}
}

func repeat(ch string, n int) string {
	out := make([]byte, 0, n)
	for len(out) < n {
		out = append(out, ch[0])
	}
	return string(out)
}

func member(id string, seq, tick int64) segment.Member {
	payload := contract.NewObject()
	payload.Set("text", contract.String("body of "+id))
	digest, _ := contract.DigestOf(payload)
	return segment.Member{
		EventID: id, EventKind: "agent_message", RoomSequence: seq, LogicalTick: tick,
		ActorID: "user-u1", PayloadDigest: digest, Payload: payload,
	}
}

func settledResult(digest string) segment.CloseResult {
	ref := segment.Ref{ID: "evseal-seg-x", Version: 1, Digest: digest}
	segRef := segment.SegmentRef{
		SchemaVersion: "host.segment-ref.v1", RoomID: "room-test", SegmentID: "seg-x",
		SegmentVersion: 1, SegmentDigest: digest, EvidenceSealRef: ref, PathSealRef: ref,
	}
	return segment.CloseResult{
		TerminalState: segment.StateSettled, SegmentDigest: digest,
		CanonicalBytes: []byte(`{"settled":true}`), CanonicalByteLength: 16,
		SegmentRef: &segRef,
	}
}

func reason(err error) string { return segment.ReasonOf(err) }

func TestSegmentSealStoreAppendOnlyAndRereadDigest(t *testing.T) {
	ctx := context.Background()
	store := NewSegmentSeals()
	if err := store.OpenSegment(ctx, testOpenSegment("seg-x", 10)); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendMember(ctx, "seg-x", member("evt-1", 11, 101)); err != nil {
		t.Fatal(err)
	}
	snap, err := store.Snapshot(ctx, "seg-x")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State != segment.StateOpen || snap.HeadSequence != 11 || len(snap.Members) != 1 {
		t.Fatalf("snapshot %+v", snap)
	}

	wantDigest := contract.DigestBytes([]byte(`{"settled":true}`))
	res, err := store.CommitClose(ctx, segment.Commit{
		SegmentID: "seg-x", ExpectedState: segment.StateOpen, ExpectedHead: 11,
		CloseIDempotencyKey: "closekey-1", InputDigest: "sha256:" + repeat("1", 64),
		Result: settledResult(wantDigest),
	})
	if err != nil || res.TerminalState != segment.StateSettled {
		t.Fatalf("commit close: %v %+v", err, res)
	}

	// Every append kind fails closed after the terminal write.
	if err := store.AppendMember(ctx, "seg-x", member("evt-2", 12, 102)); reason(err) != "APPEND_AFTER_SETTLED" {
		t.Fatalf("append member after settle: %v", err)
	}
	if err := store.PutLink(ctx, "seg-x", segment.CausalLink{LinkID: "l", LinkKind: "caused_by", FromEvent: "evt-1", ToEvent: "evt-1"}); reason(err) != "APPEND_AFTER_SETTLED" {
		t.Fatalf("put link after settle: %v", err)
	}
	if err := store.RecordDelivery(ctx, "seg-x", segment.Delivery{DeliveryID: "d", TerminalState: "delivered"}); reason(err) != "APPEND_AFTER_SETTLED" {
		t.Fatalf("record delivery after settle: %v", err)
	}
	if err := store.RecordToolExecution(ctx, "seg-x", segment.ToolExecution{ToolCallEvent: "evt-1"}); reason(err) != "APPEND_AFTER_SETTLED" {
		t.Fatalf("record execution after settle: %v", err)
	}

	// Reread: canonical bytes and digests identical across reads.
	first, err := store.SealRecord(ctx, "seg-x")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.SealRecord(ctx, "seg-x")
	if err != nil {
		t.Fatal(err)
	}
	if contract.DigestBytes(first.CanonicalBytes) != first.SegmentDigest || first.SegmentDigest != wantDigest {
		t.Fatalf("reread digest %s != %s", first.SegmentDigest, wantDigest)
	}
	if string(first.CanonicalBytes) != string(second.CanonicalBytes) || first.Result.SegmentRef == nil || second.Result.SegmentRef == nil {
		t.Fatalf("reread drift: %+v vs %+v", first, second)
	}

	// Idempotency: same key + same frozen input replays the original refs.
	replay, err := store.CommitClose(ctx, segment.Commit{
		SegmentID: "seg-x", ExpectedState: segment.StateSettled, ExpectedHead: 11,
		CloseIDempotencyKey: "closekey-1", InputDigest: "sha256:" + repeat("1", 64),
		Result: segment.CloseResult{},
	})
	if err != nil || !replay.Idempotent || replay.SegmentRef == nil || replay.SegmentDigest != wantDigest {
		t.Fatalf("idempotent replay: %v %+v", err, replay)
	}

	// Same key + different frozen input is a conflict; the original stays.
	if _, err := store.CommitClose(ctx, segment.Commit{
		SegmentID: "seg-x", ExpectedState: segment.StateSettled, ExpectedHead: 11,
		CloseIDempotencyKey: "closekey-1", InputDigest: "sha256:" + repeat("2", 64),
		Result: segment.CloseResult{},
	}); reason(err) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("same key different input: %v", err)
	}

	// A different key cannot re-close a terminal segment.
	if _, err := store.CommitClose(ctx, segment.Commit{
		SegmentID: "seg-x", ExpectedState: segment.StateOpen, ExpectedHead: 11,
		CloseIDempotencyKey: "closekey-2", InputDigest: "sha256:" + repeat("1", 64),
		Result: settledResult(wantDigest),
	}); reason(err) != "ILLEGAL_STATE_TRANSITION" {
		t.Fatalf("re-close with new key: %v", err)
	}
	again, _ := store.SealRecord(ctx, "seg-x")
	if again.CloseIDempotencyKey != "closekey-1" || again.SegmentDigest != wantDigest {
		t.Fatalf("terminal record mutated: %+v", again)
	}

	// A stale frontier head CAS leaves the segment open and unwritten.
	if err := store.OpenSegment(ctx, testOpenSegment("seg-y", 20)); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendMember(ctx, "seg-y", member("evt-a", 21, 201)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitClose(ctx, segment.Commit{
		SegmentID: "seg-y", ExpectedState: segment.StateOpen, ExpectedHead: 20,
		CloseIDempotencyKey: "closekey-y", InputDigest: "sha256:" + repeat("3", 64),
		Result: settledResult(wantDigest),
	}); reason(err) != "FRONTIER_MISMATCH" {
		t.Fatalf("stale CAS head: %v", err)
	}
	snapY, _ := store.Snapshot(ctx, "seg-y")
	if snapY.State != segment.StateOpen {
		t.Fatalf("stale CAS must keep the segment open, got %q", snapY.State)
	}
	if _, err := store.SealRecord(ctx, "seg-y"); reason(err) != "NOT_FOUND" {
		t.Fatalf("rejected commit must leave no seal record: %v", err)
	}
}

func TestSegmentSealStoreOpenAndAppendInvariants(t *testing.T) {
	ctx := context.Background()
	store := NewSegmentSeals()

	if err := store.OpenSegment(ctx, testOpenSegment("seg-o", 5)); err != nil {
		t.Fatal(err)
	}
	// Idempotent re-open with identical input; drift fails closed.
	if err := store.OpenSegment(ctx, testOpenSegment("seg-o", 5)); err != nil {
		t.Fatalf("idempotent re-open: %v", err)
	}
	drifted := testOpenSegment("seg-o", 6)
	if err := store.OpenSegment(ctx, drifted); reason(err) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("drifted re-open: %v", err)
	}

	if _, err := store.Snapshot(ctx, "missing"); reason(err) != "NOT_FOUND" {
		t.Fatalf("unknown segment: %v", err)
	}

	if err := store.AppendMember(ctx, "seg-o", member("evt-1", 6, 10)); err != nil {
		t.Fatal(err)
	}
	// Monotone room sequence and logical clock, unique event ids.
	if err := store.AppendMember(ctx, "seg-o", member("evt-1b", 6, 11)); reason(err) != "MEMBER_ORDER_INVALID" {
		t.Fatalf("non-monotone sequence: %v", err)
	}
	if err := store.AppendMember(ctx, "seg-o", member("evt-2", 7, 10)); reason(err) != "LOGICAL_CLOCK_INVALID" {
		t.Fatalf("non-monotone tick: %v", err)
	}
	if err := store.AppendMember(ctx, "seg-o", member("evt-1", 8, 12)); reason(err) != "DUPLICATE_EVENT_ID" {
		t.Fatalf("duplicate event id: %v", err)
	}

	// Links may only reference frozen members and stay unique.
	if err := store.PutLink(ctx, "seg-o", segment.CausalLink{LinkID: "l1", LinkKind: "response_to", FromEvent: "evt-1", ToEvent: "evt-ghost"}); reason(err) != "MISSING_CAUSAL_LINK" {
		t.Fatalf("link endpoint outside membership: %v", err)
	}
	if err := store.PutLink(ctx, "seg-o", segment.CausalLink{LinkID: "l1", LinkKind: "response_to", FromEvent: "evt-1", ToEvent: "evt-1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutLink(ctx, "seg-o", segment.CausalLink{LinkID: "l1", LinkKind: "caused_by", FromEvent: "evt-1", ToEvent: "evt-1"}); reason(err) != "DUPLICATE_LINK_ID" {
		t.Fatalf("duplicate link id: %v", err)
	}

	// Deliveries and tool executions stay unique per identity.
	if err := store.RecordDelivery(ctx, "seg-o", segment.Delivery{DeliveryID: "d1", TerminalState: "delivered"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordDelivery(ctx, "seg-o", segment.Delivery{DeliveryID: "d1", TerminalState: "failed"}); reason(err) != "DUPLICATE_DELIVERY_ID" {
		t.Fatalf("duplicate delivery: %v", err)
	}
	tpr := contract.NewObject()
	tpr.Set("schema_version", contract.String("host.tool-proxy-result.v1"))
	if err := store.RecordToolExecution(ctx, "seg-o", segment.ToolExecution{ToolCallEvent: "evt-1", ToolResultEvent: "evt-1", DeliveryID: "d1", ToolProxyResult: tpr}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordToolExecution(ctx, "seg-o", segment.ToolExecution{ToolCallEvent: "evt-1", ToolResultEvent: "evt-1", DeliveryID: "d1", ToolProxyResult: tpr}); reason(err) != "TOOL_EXECUTION_INVALID" {
		t.Fatalf("duplicate execution: %v", err)
	}

	snap, err := store.Snapshot(ctx, "seg-o")
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Members) != 1 || len(snap.Links) != 1 || len(snap.Deliveries) != 1 || len(snap.Executions) != 1 {
		t.Fatalf("snapshot contents %+v", snap)
	}
}
