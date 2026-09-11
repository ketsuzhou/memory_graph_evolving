package segment_test

// HST-202 Red/Green tests: the Host protected close transaction must seal a
// recorded open Segment exactly as the FND-002 frozen corpus declares
// (byte-for-byte canonical equality for the settled families) and must never
// publish refs for failed/aborted/negative closes (Host spec 3-4, S3).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/segment"
	"river2.dev/pi-group-chat-host/internal/store/memory"
)

// ---------------------------------------------------------------------------
// Fixture loading (FND-002 recorded corpus, read-only)
// ---------------------------------------------------------------------------

type fixture struct {
	segmentID string
	roomID    string
	version   int64

	frontierStart int64
	closeKey      string

	closePolicy      segment.Ref
	scopeProfile     segment.Ref
	extractionPolicy segment.Ref

	members     []segment.Member
	links       []segment.CausalLink
	deliveries  []segment.Delivery
	executions  []segment.ToolExecution
	audit       *segment.TerminalAudit
	auditPhase  string
	stateWanted string

	seal      *contract.Object
	canonical []byte
}

func recordedDir(t *testing.T) string {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("conformance dir: %v", err)
	}
	return filepath.Join(dir, "recorded", "segments")
}

func fstr(t *testing.T, o *contract.Object, key string) string {
	t.Helper()
	v, ok := o.Get(key)
	if !ok {
		t.Fatalf("fixture object missing %q", key)
	}
	s, ok := v.(contract.String)
	if !ok {
		t.Fatalf("fixture %q is not a string", key)
	}
	return string(s)
}

func fint(t *testing.T, o *contract.Object, key string) int64 {
	t.Helper()
	v, ok := o.Get(key)
	if !ok {
		t.Fatalf("fixture object missing %q", key)
	}
	n, ok := v.(contract.Number)
	if !ok {
		t.Fatalf("fixture %q is not a number", key)
	}
	bi, ok := n.Int()
	if !ok {
		t.Fatalf("fixture %q is not an integer", key)
	}
	return bi.Int64()
}

func fobj(t *testing.T, o *contract.Object, key string) *contract.Object {
	t.Helper()
	v, ok := o.Get(key)
	if !ok {
		t.Fatalf("fixture object missing %q", key)
	}
	obj, ok := v.(*contract.Object)
	if !ok {
		t.Fatalf("fixture %q is not an object", key)
	}
	return obj
}

func farray(t *testing.T, o *contract.Object, key string) contract.Array {
	t.Helper()
	v, ok := o.Get(key)
	if !ok {
		t.Fatalf("fixture object missing %q", key)
	}
	arr, ok := v.(contract.Array)
	if !ok {
		t.Fatalf("fixture %q is not an array", key)
	}
	return arr
}

func fref(t *testing.T, o *contract.Object, key string) segment.Ref {
	t.Helper()
	r := fobj(t, o, key)
	return segment.Ref{
		ID:      fstr(t, r, "id"),
		Version: fint(t, r, "version"),
		Digest:  fstr(t, r, "digest"),
	}
}

func parseFile(t *testing.T, path string) *contract.Object {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	v, err := contract.ParseJSON(data)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	obj, ok := v.(*contract.Object)
	if !ok {
		t.Fatalf("%s is not an object", path)
	}
	return obj
}

func loadFixture(t *testing.T, family, caseName string) fixture {
	t.Helper()
	caseDir := filepath.Join(recordedDir(t), family, caseName)
	doc := parseFile(t, filepath.Join(caseDir, "segment.json"))

	f := fixture{
		segmentID:     fstr(t, doc, "segment_id"),
		roomID:        fstr(t, doc, "room_id"),
		version:       fint(t, doc, "segment_version"),
		frontierStart: fint(t, fobj(t, doc, "frontier"), "start_room_sequence"),
		stateWanted:   fstr(t, doc, "terminal_state"),
	}
	closeMeta := fobj(t, doc, "close")
	f.closeKey = fstr(t, closeMeta, "close_idempotency_key")
	f.closePolicy = fref(t, closeMeta, "close_policy_ref")

	for _, raw := range farray(t, doc, "members") {
		m := raw.(*contract.Object)
		member := segment.Member{
			EventID:       fstr(t, m, "event_id"),
			EventKind:     fstr(t, m, "event_kind"),
			RoomSequence:  fint(t, m, "room_sequence"),
			LogicalTick:   fint(t, m, "logical_tick"),
			ActorID:       fstr(t, m, "actor_id"),
			PayloadDigest: fstr(t, m, "payload_digest"),
			Payload:       fobj(t, m, "payload"),
		}
		f.members = append(f.members, member)
	}
	for _, raw := range farray(t, doc, "causal_links") {
		l := raw.(*contract.Object)
		f.links = append(f.links, segment.CausalLink{
			LinkID:    fstr(t, l, "link_id"),
			LinkKind:  fstr(t, l, "link_kind"),
			FromEvent: fstr(t, l, "from_event"),
			ToEvent:   fstr(t, l, "to_event"),
		})
	}
	for _, raw := range farray(t, doc, "deliveries") {
		d := raw.(*contract.Object)
		f.deliveries = append(f.deliveries, segment.Delivery{
			DeliveryID:    fstr(t, d, "delivery_id"),
			SubjectEvent:  fstr(t, d, "subject_event"),
			TerminalState: fstr(t, d, "terminal_state"),
		})
	}
	for _, raw := range farray(t, doc, "tool_executions") {
		e := raw.(*contract.Object)
		exec := segment.ToolExecution{
			ToolCallEvent:   fstr(t, e, "tool_call_event"),
			ToolResultEvent: fstr(t, e, "tool_result_event"),
			DeliveryID:      fstr(t, e, "delivery_id"),
			ToolProxyResult: fobj(t, e, "tool_proxy_result"),
		}
		if v, ok := e.Get("upstream_attempt_record"); ok {
			exec.UpstreamAttemptRecord, _ = v.(*contract.Object)
		}
		if v, ok := e.Get("retry_correlation"); ok {
			exec.RetryCorrelation, _ = v.(*contract.Object)
		}
		f.executions = append(f.executions, exec)
	}
	if auditVal, ok := doc.Get("terminal_audit"); ok {
		a := auditVal.(*contract.Object)
		audit := segment.TerminalAudit{
			ReasonCode: fstr(t, a, "reason_code"),
			Phase:      fstr(t, a, "phase"),
			Detail:     fstr(t, a, "detail"),
		}
		f.audit = &audit
		f.auditPhase = audit.Phase
	}
	// Policy refs live in the recorded seal bodies; the loader treats them as
	// close-policy configuration input, never as expected output.
	if evBody, ok := doc.Get("evidence_seal_body"); ok {
		body := evBody.(*contract.Object)
		f.scopeProfile = fref(t, body, "scope_profile_ref")
	}
	if psBody, ok := doc.Get("path_seal_body"); ok {
		body := psBody.(*contract.Object)
		paths := farray(t, body, "paths")
		first := paths[0].(*contract.Object)
		f.extractionPolicy = fref(t, first, "extraction_policy_ref")
	}

	f.seal = parseFile(t, filepath.Join(caseDir, "seal.json"))
	var err error
	f.canonical, err = os.ReadFile(filepath.Join(caseDir, "canonical.utf8"))
	if err != nil {
		t.Fatalf("read canonical.utf8 for %s: %v", caseName, err)
	}
	return f
}

func seed(t *testing.T, store segment.Store, f fixture) {
	t.Helper()
	ctx := context.Background()
	err := store.OpenSegment(ctx, segment.OpenSegment{
		RoomID:                  f.roomID,
		SegmentID:               f.segmentID,
		SegmentVersion:          f.version,
		FrontierStart:           f.frontierStart,
		ClosePolicyRef:          f.closePolicy,
		ScopeProfileRef:         f.scopeProfile,
		PathExtractionPolicyRef: f.extractionPolicy,
	})
	if err != nil {
		t.Fatalf("open %s: %v", f.segmentID, err)
	}
	for _, m := range f.members {
		if err := store.AppendMember(ctx, f.segmentID, m); err != nil {
			t.Fatalf("append member %s: %v", m.EventID, err)
		}
	}
	for _, l := range f.links {
		if err := store.PutLink(ctx, f.segmentID, l); err != nil {
			t.Fatalf("put link %s: %v", l.LinkID, err)
		}
	}
	for _, d := range f.deliveries {
		if err := store.RecordDelivery(ctx, f.segmentID, d); err != nil {
			t.Fatalf("record delivery %s: %v", d.DeliveryID, err)
		}
	}
	for _, e := range f.executions {
		if err := store.RecordToolExecution(ctx, f.segmentID, e); err != nil {
			t.Fatalf("record execution %s: %v", e.ToolCallEvent, err)
		}
	}
}

func jcs(t *testing.T, v contract.Value) []byte {
	t.Helper()
	b, err := contract.JCS(v)
	if err != nil {
		t.Fatalf("JCS: %v", err)
	}
	return b
}

func sealSub(t *testing.T, f fixture, key string) *contract.Object {
	t.Helper()
	return fobj(t, f.seal, key)
}

func jcsEqual(t *testing.T, name string, got contract.Value, want *contract.Object) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: got nil value, want fixture block", name)
	}
	if !bytes.Equal(jcs(t, got), jcs(t, want)) {
		t.Fatalf("%s: canonical bytes differ from frozen fixture\n got: %s\nwant: %s", name, jcs(t, got), jcs(t, want))
	}
}

func assertZeroRefs(t *testing.T, res segment.CloseResult, where string) {
	t.Helper()
	if res.SegmentRef != nil || res.CheckpointRef != nil || res.EvidenceSeal != nil || res.PathSeal != nil || len(res.Paths) != 0 {
		t.Fatalf("%s: non-settled close published refs: %+v", where, res)
	}
}

// ---------------------------------------------------------------------------
// Green target 1: atomic settled close over the frozen success/recovery corpus
// ---------------------------------------------------------------------------

func TestCloseSegmentAtomicallySealsSuccessFailureRecovery(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		family, caseName string
		wantPaths        map[string][]string // domain -> ordered path ids
	}{
		{"success", "seg-success-0001", map[string][]string{"success": {"path-success-0001"}}},
		{"recovery", "seg-recovery-0001", map[string][]string{"failure": {"path-failure-0001"}, "recovery": {"path-recovery-0001"}}},
	} {
		t.Run(tc.caseName, func(t *testing.T) {
			f := loadFixture(t, tc.family, tc.caseName)
			store := memory.NewSegmentSeals()
			svc := segment.New(store)
			seed(t, store, f)

			// Single close transaction returns every ref at once.
			res, err := svc.Close(ctx, f.segmentID, segment.CloseRequest{
				Intent:              segment.IntentSettle,
				CloseIDempotencyKey: f.closeKey,
			})
			if err != nil {
				t.Fatalf("close: %v", err)
			}
			if res.TerminalState != segment.StateSettled {
				t.Fatalf("terminal state %q, want settled", res.TerminalState)
			}

			// Hard protocol evidence: derived canonical bytes equal the frozen
			// canonical.utf8 byte for byte, so segment_digest equality follows.
			if !bytes.Equal(res.CanonicalBytes, f.canonical) {
				t.Fatalf("canonical bytes differ from %s canonical.utf8 (got %d bytes, want %d)",
					tc.caseName, len(res.CanonicalBytes), len(f.canonical))
			}
			wantDigest := fstr(t, f.seal, "segment_digest")
			if res.SegmentDigest != wantDigest {
				t.Fatalf("segment_digest %s, want frozen %s", res.SegmentDigest, wantDigest)
			}
			if res.CanonicalByteLength != int64(len(f.canonical)) {
				t.Fatalf("canonical byte length %d, want %d", res.CanonicalByteLength, len(f.canonical))
			}

			// Evidence Seal / Path Seal digests equal the frozen seal.json.
			evBlock := sealSub(t, f, "evidence_seal")
			if res.EvidenceSeal == nil || res.EvidenceSeal.SealDigest != fstr(t, evBlock, "seal_digest") {
				t.Fatalf("evidence seal digest %v, want %s", res.EvidenceSeal, fstr(t, evBlock, "seal_digest"))
			}
			psBlock := sealSub(t, f, "path_seal")
			if res.PathSeal == nil || res.PathSeal.SealDigest != fstr(t, psBlock, "seal_digest") {
				t.Fatalf("path seal digest %v, want %s", res.PathSeal, fstr(t, psBlock, "seal_digest"))
			}

			// SegmentRef / CheckpointRef / EvidenceRef deep equality with the
			// frozen DTO projections (Contract 7.5-7.7).
			jcsEqual(t, "segment_ref", res.SegmentRef.Object(), fobj(t, f.seal, "segment_ref"))
			jcsEqual(t, "checkpoint_ref", res.CheckpointRef.Object(), fobj(t, f.seal, "checkpoint_ref"))
			jcsEqual(t, "evidence_ref", res.EvidenceSeal.EvidenceRef.Object(), fobj(t, evBlock, "evidence_ref"))

			// Branch preservation: every expected path family sealed.
			byDomain := map[string][]string{}
			for _, p := range res.Paths {
				byDomain[p.Domain] = append(byDomain[p.Domain], p.PathID)
			}
			for domain, ids := range tc.wantPaths {
				got := byDomain[domain]
				if len(got) != len(ids) {
					t.Fatalf("domain %s sealed %v, want %v", domain, got, ids)
				}
				for i := range ids {
					if got[i] != ids[i] {
						t.Fatalf("domain %s sealed %v, want %v", domain, got, ids)
					}
				}
			}
			pathIDs := make([]string, 0, len(res.Paths))
			for _, p := range res.Paths {
				pathIDs = append(pathIDs, p.PathID)
			}
			if fmt.Sprint(pathIDs) != fmt.Sprint(res.PathSeal.PathIDs) {
				t.Fatalf("path seal ids %v disagree with sealed paths %v", res.PathSeal.PathIDs, pathIDs)
			}

			// Reread: canonical seal bytes and digests are immutable on reread.
			rec, err := store.SealRecord(ctx, f.segmentID)
			if err != nil {
				t.Fatalf("seal record reread: %v", err)
			}
			if contract.DigestBytes(rec.CanonicalBytes) != wantDigest {
				t.Fatalf("reread digest %s, want %s", contract.DigestBytes(rec.CanonicalBytes), wantDigest)
			}
			if rec.Result.SegmentDigest != wantDigest || rec.Result.SegmentRef == nil {
				t.Fatalf("reread result lost refs or digest")
			}

			// Idempotent close: same key + same frozen input returns the
			// original refs (Host 3.7).
			again, err := svc.Close(ctx, f.segmentID, segment.CloseRequest{
				Intent:              segment.IntentSettle,
				CloseIDempotencyKey: f.closeKey,
			})
			if err != nil || !again.Idempotent {
				t.Fatalf("idempotent replay: err=%v idempotent=%v", err, again.Idempotent)
			}
			if again.SegmentDigest != wantDigest || again.SegmentRef == nil || again.CheckpointRef == nil {
				t.Fatalf("idempotent replay lost refs")
			}

			// Same key, different intent (different frozen close input) is a
			// conflict, not a silent rewrite.
			if _, err := svc.Close(ctx, f.segmentID, segment.CloseRequest{
				Intent:              segment.IntentFail,
				CloseIDempotencyKey: f.closeKey,
			}); segment.ReasonOf(err) != "IDEMPOTENCY_CONFLICT" {
				t.Fatalf("same key different intent: reason %q, want IDEMPOTENCY_CONFLICT", segment.ReasonOf(err))
			}

			// A different close key on a terminal segment cannot re-close.
			if _, err := svc.Close(ctx, f.segmentID, segment.CloseRequest{
				Intent:              segment.IntentSettle,
				CloseIDempotencyKey: f.closeKey + "-second",
			}); segment.ReasonOf(err) != "ILLEGAL_STATE_TRANSITION" {
				t.Fatalf("re-close with new key: reason %q, want ILLEGAL_STATE_TRANSITION", segment.ReasonOf(err))
			}

			// Append after settle fails closed (Host 3.2/9.2).
			last := f.members[len(f.members)-1]
			if err := store.AppendMember(ctx, f.segmentID, segment.Member{
				EventID: "evt-post-settle", EventKind: "model_response",
				RoomSequence: last.RoomSequence + 1, LogicalTick: last.LogicalTick + 1,
				ActorID: last.ActorID, PayloadDigest: "sha256:" + strings.Repeat("0", 64),
				Payload: contract.NewObject(),
			}); segment.ReasonOf(err) != "APPEND_AFTER_SETTLED" {
				t.Fatalf("append after settle: reason %q, want APPEND_AFTER_SETTLED", segment.ReasonOf(err))
			}
		})
	}

	// Frontier CAS: a stale expected frontier head keeps the segment open.
	f := loadFixture(t, "success", "seg-success-0001")
	store := memory.NewSegmentSeals()
	svc := segment.New(store)
	seed(t, store, f)
	_, err := svc.Close(ctx, f.segmentID, segment.CloseRequest{
		Intent:              segment.IntentSettle,
		CloseIDempotencyKey: f.closeKey,
		ExpectedFrontierEnd: 18, // actual head is 19
	})
	if segment.ReasonOf(err) != "FRONTIER_MISMATCH" {
		t.Fatalf("stale frontier CAS: reason %q, want FRONTIER_MISMATCH", segment.ReasonOf(err))
	}
	snap, err := store.Snapshot(ctx, f.segmentID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.State != segment.StateOpen {
		t.Fatalf("stale CAS must keep the segment open, got %q", snap.State)
	}
	if _, err := store.SealRecord(ctx, f.segmentID); segment.ReasonOf(err) != "NOT_FOUND" {
		t.Fatalf("rejected close must leave no seal record, got %v", err)
	}
	// A correct CAS view then settles.
	if _, err := svc.Close(ctx, f.segmentID, segment.CloseRequest{
		Intent: segment.IntentSettle, CloseIDempotencyKey: f.closeKey, ExpectedFrontierEnd: 19,
	}); err != nil {
		t.Fatalf("close after CAS correction: %v", err)
	}

	// Cancellation before commit must not partially settle.
	f2 := loadFixture(t, "recovery", "seg-recovery-0001")
	store2 := memory.NewSegmentSeals()
	svc2 := segment.New(store2)
	seed(t, store2, f2)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc2.Close(cancelled, f2.segmentID, segment.CloseRequest{
		Intent: segment.IntentSettle, CloseIDempotencyKey: f2.closeKey,
	}); err == nil {
		t.Fatalf("cancelled close must fail")
	}
	snap2, _ := store2.Snapshot(ctx, f2.segmentID)
	if snap2.State != segment.StateOpen {
		t.Fatalf("cancelled close partially settled: state=%q", snap2.State)
	}
	if _, err := store2.SealRecord(ctx, f2.segmentID); segment.ReasonOf(err) != "NOT_FOUND" {
		t.Fatalf("cancelled close left a seal record: %v", err)
	}
	if _, err := svc2.Close(ctx, f2.segmentID, segment.CloseRequest{
		Intent: segment.IntentSettle, CloseIDempotencyKey: f2.closeKey,
	}); err != nil {
		t.Fatalf("close after cancellation: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Green target 2: failed/aborted/negative closes publish zero refs
// ---------------------------------------------------------------------------

// negativeExpectation reads the manifest's frozen expectation for a case.
func negativeExpectation(t *testing.T, caseID string) (string, string) {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "recorded", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Cases []struct {
			CaseID             string `json:"case_id"`
			Corpus             string `json:"corpus"`
			ExpectedOutcome    string `json:"expected_outcome"`
			ExpectedReasonCode string `json:"expected_reason_code"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, c := range manifest.Cases {
		if c.CaseID == caseID {
			return c.ExpectedOutcome, c.ExpectedReasonCode
		}
	}
	t.Fatalf("case %s not in recorded manifest", caseID)
	return "", ""
}

func TestFailedClosePublishesNoRefs(t *testing.T) {
	ctx := context.Background()

	// seg-failed-0001: a settle attempt meets a tool call without any terminal
	// tool result; the deterministic integrity failure freezes a failed
	// terminal audit and publishes zero refs.
	failed := loadFixture(t, "failure", "seg-failed-0001")
	if outcome, _ := negativeExpectation(t, "seg-failed-0001"); outcome != "accept" {
		t.Fatalf("manifest expectation for seg-failed-0001 changed: %s", outcome)
	}
	store := memory.NewSegmentSeals()
	svc := segment.New(store)
	seed(t, store, failed)
	res, err := svc.Close(ctx, failed.segmentID, segment.CloseRequest{
		Intent: segment.IntentSettle, CloseIDempotencyKey: failed.closeKey,
	})
	if err != nil {
		t.Fatalf("failed close: %v", err)
	}
	if res.TerminalState != segment.StateFailed {
		t.Fatalf("terminal state %q, want failed", res.TerminalState)
	}
	if res.TerminalAudit == nil || res.TerminalAudit.ReasonCode != "MISSING_TOOL_RESULT" {
		t.Fatalf("terminal audit %+v, want MISSING_TOOL_RESULT", res.TerminalAudit)
	}
	if res.TerminalAudit.Phase != failed.auditPhase {
		t.Fatalf("audit phase %q, want %q", res.TerminalAudit.Phase, failed.auditPhase)
	}
	assertZeroRefs(t, res, "seg-failed-0001")
	rec, err := store.SealRecord(ctx, failed.segmentID)
	if err != nil {
		t.Fatal(err)
	}
	assertZeroRefs(t, rec.Result, "seg-failed-0001 reread")
	// Terminal segments accept no further appends either (Host 3.2).
	if err := store.PutLink(ctx, failed.segmentID, segment.CausalLink{
		LinkID: "lnk-post", LinkKind: "caused_by",
		FromEvent: "evt-201", ToEvent: "evt-202",
	}); segment.ReasonOf(err) != "APPEND_AFTER_SETTLED" {
		t.Fatalf("append to failed segment: reason %q, want APPEND_AFTER_SETTLED", segment.ReasonOf(err))
	}

	// seg-aborted-0001: a legitimate operator cancel aborts before close
	// conditions are met; aborted terminals publish zero refs.
	aborted := loadFixture(t, "failure", "seg-aborted-0001")
	if outcome, _ := negativeExpectation(t, "seg-aborted-0001"); outcome != "accept" {
		t.Fatalf("manifest expectation for seg-aborted-0001 changed: %s", outcome)
	}
	store2 := memory.NewSegmentSeals()
	svc2 := segment.New(store2)
	seed(t, store2, aborted)
	res2, err := svc2.Close(ctx, aborted.segmentID, segment.CloseRequest{
		Intent:              segment.IntentAbort,
		CloseIDempotencyKey: aborted.closeKey,
		ReasonCode:          aborted.audit.ReasonCode,
		Phase:               aborted.audit.Phase,
		Detail:              aborted.audit.Detail,
	})
	if err != nil {
		t.Fatalf("abort close: %v", err)
	}
	if res2.TerminalState != segment.StateAborted {
		t.Fatalf("terminal state %q, want aborted", res2.TerminalState)
	}
	if res2.TerminalAudit == nil || res2.TerminalAudit.ReasonCode != "ROOM_CANCELLED" {
		t.Fatalf("terminal audit %+v, want ROOM_CANCELLED", res2.TerminalAudit)
	}
	assertZeroRefs(t, res2, "seg-aborted-0001")

	// The five frozen negative inputs must each be rejected by the close
	// transaction with exactly the manifest reason code, and none of them may
	// publish refs.
	negatives := []struct {
		caseID, family, dirName string
	}{
		{"seg-neg-dag-cycle", "negative", "seg-neg-dag-cycle"},
		{"seg-neg-missing-causal-link", "negative", "seg-neg-missing-causal-link"},
		{"seg-neg-nonterminal-tool", "negative", "seg-neg-nonterminal-tool"},
		{"seg-neg-seal-mismatch", "negative", "seg-neg-seal-mismatch"},
	}
	for _, neg := range negatives {
		_, wantReason := negativeExpectation(t, neg.caseID)
		t.Run(neg.caseID, func(t *testing.T) {
			f := loadFixture(t, neg.family, neg.dirName)
			store := memory.NewSegmentSeals()
			svc := segment.New(store)
			seed(t, store, f)
			req := segment.CloseRequest{
				Intent: segment.IntentSettle, CloseIDempotencyKey: f.closeKey,
			}
			if neg.caseID == "seg-neg-seal-mismatch" {
				// The recorded seal claims a segment digest that does not
				// cover its own canonical bytes; close must fail closed.
				req.ExpectedSegmentDigest = fstr(t, f.seal, "segment_digest")
			}
			res, err := svc.Close(ctx, f.segmentID, req)
			if err != nil {
				t.Fatalf("negative close: %v", err)
			}
			if res.TerminalState != segment.StateFailed {
				t.Fatalf("terminal state %q, want failed", res.TerminalState)
			}
			if res.TerminalAudit == nil || res.TerminalAudit.ReasonCode != wantReason {
				t.Fatalf("reason %v, want frozen %s", res.TerminalAudit, wantReason)
			}
			assertZeroRefs(t, res, neg.caseID)
			if rec, err := store.SealRecord(ctx, f.segmentID); err != nil {
				t.Fatal(err)
			} else {
				assertZeroRefs(t, rec.Result, neg.caseID+" reread")
			}
		})
	}

	// seg-neg-append-after-settled: settle the frozen frontier first, then the
	// recorded post-frontier event append must be rejected.
	t.Run("seg-neg-append-after-settled", func(t *testing.T) {
		_, wantReason := negativeExpectation(t, "seg-neg-append-after-settled")
		f := loadFixture(t, "negative", "seg-neg-append-after-settled")
		postSettle := f.members[len(f.members)-1]
		f.members = f.members[:len(f.members)-1]
		store := memory.NewSegmentSeals()
		svc := segment.New(store)
		seed(t, store, f)
		if _, err := svc.Close(ctx, f.segmentID, segment.CloseRequest{
			Intent: segment.IntentSettle, CloseIDempotencyKey: f.closeKey,
		}); err != nil {
			t.Fatalf("close inside frontier: %v", err)
		}
		err := store.AppendMember(ctx, f.segmentID, postSettle)
		if segment.ReasonOf(err) != wantReason {
			t.Fatalf("append beyond frontier: reason %q, want %s", segment.ReasonOf(err), wantReason)
		}
	})
}
