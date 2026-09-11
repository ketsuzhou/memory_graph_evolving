package evidence

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// ---------------------------------------------------------------------------
// FND-002 recorded corpus loading ($FIX/recorded/segments is the frozen
// admission input boundary of GMS-201: Host Sealed Segment records, seal
// envelopes and canonical bytes, driven through the manifest).
// ---------------------------------------------------------------------------

const testSubmitter = "conformance-harness"

func loadRegistry(t *testing.T) *ledger.ContractReasonRegistry {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load system reason policy: %v", err)
	}
	return &ledger.ContractReasonRegistry{Policy: policy}
}

func recordedRoot(t *testing.T) string {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	root := filepath.Join(dir, "recorded")
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("FND-002 recorded corpus not found at %s", root)
	}
	return root
}

type segCase struct {
	caseID         string
	family         string
	path           string
	expected       string // accept | reject | reserved
	expectedReason string
}

func segmentsCases(t *testing.T) []segCase {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(recordedRoot(t), "manifest.json"))
	if err != nil {
		t.Fatalf("read recorded manifest: %v", err)
	}
	manifest := parseJSON(t, data)
	rawCases, ok := contract.AsArray(manifest["cases"])
	if !ok {
		t.Fatal("recorded manifest has no cases array")
	}
	var out []segCase
	families := map[string]bool{}
	rejects := 0
	for _, raw := range rawCases {
		entry, isObj := contract.AsObject(raw)
		if !isObj {
			continue
		}
		if corpus, _ := contract.AsString(entry["corpus"]); corpus != "segments" {
			continue
		}
		c := segCase{}
		c.caseID, _ = contract.AsString(entry["case_id"])
		c.family, _ = contract.AsString(entry["family"])
		c.path, _ = contract.AsString(entry["path"])
		c.expected, _ = contract.AsString(entry["expected_outcome"])
		c.expectedReason, _ = contract.AsString(entry["expected_reason_code"])
		out = append(out, c)
		if c.expected == "accept" {
			families[c.family] = true
		}
		if c.expected == "reject" {
			rejects++
		}
	}
	for _, family := range []string{"success", "failure", "recovery"} {
		if !families[family] {
			t.Fatalf("recorded corpus is missing accept family %q", family)
		}
	}
	if rejects != 5 {
		t.Fatalf("recorded corpus must register exactly 5 negative segment cases, got %d", rejects)
	}
	return out
}

func parseJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		t.Fatalf("fixture is not strict JSON: %v", err)
	}
	obj, ok := contract.AsObject(value)
	if !ok {
		t.Fatal("fixture must be a JSON object")
	}
	return obj
}

// nested fetches keys…keys→string from a fixture object.
func nested(t *testing.T, obj map[string]any, keys ...string) map[string]any {
	t.Helper()
	current := obj
	for _, key := range keys {
		next, ok := contract.AsObject(current[key])
		if !ok {
			t.Fatalf("fixture field %v (under %v) is not an object", key, keys)
		}
		current = next
	}
	return current
}

func nestedString(t *testing.T, obj map[string]any, keys ...string) string {
	t.Helper()
	holder := nested(t, obj, keys[:len(keys)-1]...)
	s, ok := contract.AsString(holder[keys[len(keys)-1]])
	if !ok {
		t.Fatalf("fixture field %v is not a string", keys)
	}
	return s
}

func nestedStrings(t *testing.T, obj map[string]any, keys ...string) []string {
	t.Helper()
	holder := nested(t, obj, keys[:len(keys)-1]...)
	raw, ok := contract.AsArray(holder[keys[len(keys)-1]])
	if !ok {
		t.Fatalf("fixture field %v is not an array", keys)
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		s, ok := contract.AsString(item)
		if !ok {
			t.Fatalf("fixture field %v has a non-string item", keys)
		}
		out = append(out, s)
	}
	return out
}

func readCase(t *testing.T, c segCase) (segmentJSON, sealJSON, canonical []byte) {
	t.Helper()
	dir := filepath.Join(recordedRoot(t), c.path)
	var err error
	if segmentJSON, err = os.ReadFile(filepath.Join(dir, "segment.json")); err != nil {
		t.Fatalf("read segment.json of %s: %v", c.caseID, err)
	}
	if sealJSON, err = os.ReadFile(filepath.Join(dir, "seal.json")); err != nil {
		t.Fatalf("read seal.json of %s: %v", c.caseID, err)
	}
	if canonical, err = os.ReadFile(filepath.Join(dir, "canonical.utf8")); err != nil {
		t.Fatalf("read canonical.utf8 of %s: %v", c.caseID, err)
	}
	return segmentJSON, sealJSON, canonical
}

func firstCase(cases []segCase, family string) segCase {
	for _, c := range cases {
		if c.family == family {
			return c
		}
	}
	panic("missing family " + family)
}

func versionedRefOf(t *testing.T, obj map[string]any) contract.VersionedRef {
	t.Helper()
	ref, err := contract.ParseVersionedRef(obj)
	if err != nil {
		t.Fatalf("fixture scope profile ref is not an exact VersionedRef: %v", err)
	}
	return ref
}

// newTestService builds the admission service over the GMS-102 ledger ports
// with a scope policy pinned to the recorded corpus (room + sealed scope
// profile read from the success fixture, never hand-copied).
func newTestService(t *testing.T) (*AdmissionService, *ledger.MemoryStore, contract.VersionedRef, string) {
	t.Helper()
	registry := loadRegistry(t)
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	mgr, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	segmentJSON, _, _ := readCase(t, firstCase(segmentsCases(t), "success"))
	seg := parseJSON(t, segmentJSON)
	profile := versionedRefOf(t, nested(t, seg, "evidence_seal_body", "scope_profile_ref"))
	room := nestedString(t, seg, "room_id")
	svc, err := NewAdmissionService(store, mgr, registry, ScopePolicy{
		AllowedRooms:    []string{room},
		AllowedProfiles: []contract.VersionedRef{profile},
	})
	if err != nil {
		t.Fatalf("NewAdmissionService: %v", err)
	}
	return svc, store, profile, room
}

func submissionFor(t *testing.T, c segCase, room string, profile contract.VersionedRef) SubmittedSegment {
	t.Helper()
	segmentJSON, sealJSON, canonical := readCase(t, c)
	return SubmittedSegment{
		SegmentJSON: segmentJSON,
		SealJSON:    sealJSON,
		Canonical:   canonical,
		Submitter:   testSubmitter,
		Scope:       AdmissionScope{RoomID: room, ScopeProfileRef: profile},
	}
}

func evidenceLedgerSize(t *testing.T, store *ledger.MemoryStore) int {
	t.Helper()
	entries, err := store.Snapshot(ledger.LedgerEvidence, "")
	if err != nil {
		t.Fatalf("snapshot evidence ledger: %v", err)
	}
	return len(entries)
}

func requireAdmitted(t *testing.T, svc *AdmissionService, sub SubmittedSegment) string {
	t.Helper()
	staged, err := svc.Stage(sub)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	out, err := svc.Validate(staged)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !out.Valid {
		t.Fatalf("expected sealed evidence to validate, got status=%s reason=%s detail=%s", out.Status, out.ReasonCode, out.Detail)
	}
	return staged
}

// ---------------------------------------------------------------------------
// GMS-201 TDD anchor: only valid sealed evidence is committed, atomically.
// ---------------------------------------------------------------------------

func TestAdmissionCommitsOnlyValidSealedEvidenceAtomically(t *testing.T) {
	cases := segmentsCases(t)
	svc, store, profile, room := newTestService(t)
	ctx := context.Background()

	// Settled accept families (success + recovery) commit sealed evidence whose
	// EvidenceRef digests are exactly the digests the Host seal.json declares.
	t.Run("SettledFamiliesCommitWithSealDigestParity", func(t *testing.T) {
		committed := 0
		for _, c := range cases {
			if c.expected != "accept" {
				continue
			}
			segmentJSON, sealJSON, _ := readCase(t, c)
			seg := parseJSON(t, segmentJSON)
			if state, _ := contract.AsString(seg["terminal_state"]); state != "settled" {
				continue
			}
			seal := parseJSON(t, sealJSON)
			staged := requireAdmitted(t, svc, submissionFor(t, c, room, profile))
			ce, err := svc.Commit(ctx, staged)
			if err != nil {
				t.Fatalf("%s: Commit: %v", c.caseID, err)
			}
			wantEvidenceDigest := nestedString(t, seal, "evidence_seal", "seal_digest")
			if ce.EvidenceDigest != wantEvidenceDigest {
				t.Errorf("%s: EvidenceRef digest %s != seal.json evidence seal digest %s", c.caseID, ce.EvidenceDigest, wantEvidenceDigest)
			}
			if ce.EvidenceSealDigest != wantEvidenceDigest {
				t.Errorf("%s: provenance evidence seal digest %s != %s", c.caseID, ce.EvidenceSealDigest, wantEvidenceDigest)
			}
			if ce.PathSealDigest != nestedString(t, seal, "path_seal", "seal_digest") {
				t.Errorf("%s: provenance path seal digest mismatch", c.caseID)
			}
			if ce.Segment.SegmentDigest != nestedString(t, seal, "segment_ref", "segment_digest") {
				t.Errorf("%s: source SegmentRef digest mismatch", c.caseID)
			}
			if ce.Checkpoint.CheckpointDigest != nestedString(t, seal, "checkpoint_ref", "checkpoint_digest") {
				t.Errorf("%s: checkpoint digest mismatch", c.caseID)
			}
			if ce.CommitState != "committed" {
				t.Errorf("%s: minted commit_state %q, want committed", c.caseID, ce.CommitState)
			}
			if ce.EvidenceKind != nestedString(t, seal, "evidence_seal", "evidence_ref", "evidence_kind") {
				t.Errorf("%s: evidence kind mismatch", c.caseID)
			}
			if want := nestedStrings(t, seal, "path_seal", "path_ids"); !reflect.DeepEqual(ce.PathIDs, want) {
				t.Errorf("%s: sealed path ids %v != %v", c.caseID, ce.PathIDs, want)
			}
			if ce.Submitter != testSubmitter || ce.CommitSequence == 0 {
				t.Errorf("%s: provenance incomplete: submitter=%q sequence=%d", c.caseID, ce.Submitter, ce.CommitSequence)
			}
			committed++
			// Readable through the committed-only reader.
			got, ok, err := svc.GetEvidence(ce.EvidenceID)
			if err != nil || !ok || got.EvidenceDigest != ce.EvidenceDigest {
				t.Errorf("%s: committed evidence %s not readable: ok=%v err=%v", c.caseID, ce.EvidenceID, ok, err)
			}
		}
		if committed != 2 {
			t.Fatalf("expected exactly the success and recovery families to commit, got %d", committed)
		}
		all, err := svc.CommittedEvidence()
		if err != nil {
			t.Fatalf("CommittedEvidence: %v", err)
		}
		if len(all) != committed {
			t.Fatalf("CommittedEvidence returned %d refs, want %d", len(all), committed)
		}
	})

	// Failed/aborted Host terminals are valid records but never mint refs
	// (Contract §7.5-7.7): admission records a non-ref validation outcome.
	t.Run("FailedAndAbortedSegmentsMintNoEvidenceRef", func(t *testing.T) {
		for _, c := range cases {
			if c.family != "failure" || c.expected != "accept" {
				continue
			}
			segmentJSON, _, _ := readCase(t, c)
			seg := parseJSON(t, segmentJSON)
			staged, err := svc.Stage(submissionFor(t, c, room, profile))
			if err != nil {
				t.Fatalf("%s: Stage: %v", c.caseID, err)
			}
			out, err := svc.Validate(staged)
			if err != nil {
				t.Fatalf("%s: Validate: %v", c.caseID, err)
			}
			if out.Valid || out.Status != StatusRejected || out.ReasonCode != ReasonSegmentNotSettled {
				t.Errorf("%s: want rejected/%s, got valid=%v status=%s reason=%s", c.caseID, ReasonSegmentNotSettled, out.Valid, out.Status, out.ReasonCode)
			}
			audit := nested(t, seg, "terminal_audit")
			wantReason := nestedString(t, seg, "terminal_audit", "reason_code")
			if out.TerminalAudit == nil || !contract.EqualJSON(out.TerminalAudit, audit) {
				t.Errorf("%s: outcome must record the Host terminal audit verbatim, got %v", c.caseID, out.TerminalAudit)
			}
			if wantReason == "" || out.TerminalAudit["reason_code"] != wantReason {
				t.Errorf("%s: terminal audit reason mismatch", c.caseID)
			}
			if _, err := svc.Commit(ctx, staged); err == nil || ReasonOf(err) != ReasonIllegalStateTransition {
				t.Errorf("%s: Commit of rejected evidence must fail closed with %s, got %v", c.caseID, ReasonIllegalStateTransition, err)
			}
		}
		if got := evidenceLedgerSize(t, store); got != 2 {
			t.Fatalf("failure family minted evidence refs: ledger size %d != 2", got)
		}
	})

	// The 5 negative corpus cases reject with exactly the manifest reason.
	t.Run("NegativeCorpusRejectedWithManifestReasons", func(t *testing.T) {
		rejected := 0
		for _, c := range cases {
			if c.expected != "reject" {
				continue
			}
			staged, err := svc.Stage(submissionFor(t, c, room, profile))
			if err != nil {
				t.Fatalf("%s: Stage: %v", c.caseID, err)
			}
			out, err := svc.Validate(staged)
			if err != nil {
				t.Fatalf("%s: Validate: %v", c.caseID, err)
			}
			if out.Valid || out.Status != StatusRejected {
				t.Errorf("%s: want rejection, got valid=%v status=%s", c.caseID, out.Valid, out.Status)
			}
			if out.ReasonCode != c.expectedReason {
				t.Errorf("%s: reason %s != manifest expected %s (detail %s)", c.caseID, out.ReasonCode, c.expectedReason, out.Detail)
			}
			if _, err := svc.Commit(ctx, staged); err == nil {
				t.Errorf("%s: Commit of rejected evidence must fail closed", c.caseID)
			}
			rejected++
		}
		if rejected != 5 {
			t.Fatalf("drove %d negative cases, want 5", rejected)
		}
		if got := evidenceLedgerSize(t, store); got != 2 {
			t.Fatalf("negative cases minted evidence refs: ledger size %d != 2", got)
		}
	})

	// A forged checkpoint digest (seal.json tampered only) never mints a ref.
	t.Run("ForgedCheckpointDigestRejected", func(t *testing.T) {
		success := firstCase(cases, "success")
		segmentJSON, sealJSON, canonical := readCase(t, success)
		seal := parseJSON(t, sealJSON)
		nested(t, seal, "checkpoint_ref")["checkpoint_digest"] = "sha256:" + repeat('0', 64)
		tampered, err := json.Marshal(seal)
		if err != nil {
			t.Fatalf("marshal tampered seal: %v", err)
		}
		staged, err := svc.Stage(SubmittedSegment{
			SegmentJSON: segmentJSON, SealJSON: tampered, Canonical: canonical,
			Submitter: testSubmitter,
			Scope:     AdmissionScope{RoomID: room, ScopeProfileRef: profile},
		})
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		out, err := svc.Validate(staged)
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if out.Valid || out.Status != StatusRejected || out.ReasonCode != ReasonCheckpointNotSealed {
			t.Errorf("forged checkpoint: want rejected/%s, got valid=%v status=%s reason=%s", ReasonCheckpointNotSealed, out.Valid, out.Status, out.ReasonCode)
		}
		if _, err := svc.Commit(ctx, staged); err == nil {
			t.Error("forged checkpoint must not commit")
		}
		if got := evidenceLedgerSize(t, store); got != 2 {
			t.Fatalf("forged checkpoint minted a ref: ledger size %d != 2", got)
		}
	})

	// Scope overreach (declared room / profile not authorized) is denied.
	t.Run("ScopeOverreachRejected", func(t *testing.T) {
		success := firstCase(cases, "success")
		base := submissionFor(t, success, room, profile)

		overreachRoom := base
		overreachRoom.Scope.RoomID = "room-unauthorized"
		staged, err := svc.Stage(overreachRoom)
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		out, err := svc.Validate(staged)
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if out.Valid || out.Status != StatusRejected || out.ReasonCode != ReasonEvidenceScopeDenied {
			t.Errorf("scope room overreach: want rejected/%s, got valid=%v status=%s reason=%s", ReasonEvidenceScopeDenied, out.Valid, out.Status, out.ReasonCode)
		}

		overreachProfile := base
		overreachProfile.Scope.ScopeProfileRef.Digest = "sha256:" + repeat('1', 64)
		staged, err = svc.Stage(overreachProfile)
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		out, err = svc.Validate(staged)
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if out.Valid || out.Status != StatusRejected || out.ReasonCode != ReasonEvidenceScopeDenied {
			t.Errorf("scope profile overreach: want rejected/%s, got valid=%v status=%s reason=%s", ReasonEvidenceScopeDenied, out.Valid, out.Status, out.ReasonCode)
		}
		if got := evidenceLedgerSize(t, store); got != 2 {
			t.Fatalf("scope-overreach submissions minted refs: ledger size %d != 2", got)
		}
	})

	// Staged evidence is GMS-private: invisible to every committed reader.
	t.Run("StagedEvidenceInvisibleUntilCommitted", func(t *testing.T) {
		fresh, freshStore, freshProfile, freshRoom := newTestService(t)
		success := firstCase(cases, "success")
		staged, err := fresh.Stage(submissionFor(t, success, freshRoom, freshProfile))
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		all, err := fresh.CommittedEvidence()
		if err != nil {
			t.Fatalf("CommittedEvidence: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("staged submission leaked into committed reads: %d refs", len(all))
		}
		if got := evidenceLedgerSize(t, freshStore); got != 0 {
			t.Fatalf("staged submission wrote the evidence ledger: %d entries", got)
		}
		if _, ok, _ := fresh.GetEvidence("evseal-seg-success-0001"); ok {
			t.Fatal("staged evidence id resolved before commit")
		}
		rec, ok := fresh.StagingRecord(staged)
		if !ok || rec.Status != StatusStaged {
			t.Fatalf("staging record must expose the private staged state, got ok=%v status=%s", ok, rec.Status)
		}
		// After commit the same sealed submission becomes the only committed ref.
		committedStaged := requireAdmitted(t, fresh, submissionFor(t, success, freshRoom, freshProfile))
		if _, err := fresh.Commit(context.Background(), committedStaged); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		all, err = fresh.CommittedEvidence()
		if err != nil {
			t.Fatalf("CommittedEvidence: %v", err)
		}
		if len(all) != 1 {
			t.Fatalf("want exactly 1 committed ref, got %d", len(all))
		}
	})
}

// repeat is a tiny strings.Repeat stand-in to keep imports tight.
func repeat(ch byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = ch
	}
	return string(out)
}

// Idempotent commit: the same sealed input committed twice returns the same
// EvidenceRef and appends no second ledger entry (GMS §12.2, Contract §13.2).
func TestAdmissionCommitIsIdempotent(t *testing.T) {
	cases := segmentsCases(t)
	svc, store, profile, room := newTestService(t)
	success := firstCase(cases, "success")

	staged := requireAdmitted(t, svc, submissionFor(t, success, room, profile))
	first, err := svc.Commit(context.Background(), staged)
	if err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	second, err := svc.Commit(context.Background(), staged)
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("idempotent commit drifted:\nfirst  = %+v\nsecond = %+v", first, second)
	}
	if got := evidenceLedgerSize(t, store); got != 1 {
		t.Fatalf("idempotent commit appended %d ledger entries, want 1", got)
	}

	// A different sealed input reusing the same mutation key conflicts and
	// leaves zero partial writes (no new content, no ledger append).
	recovery := firstCase(cases, "recovery")
	key := "harness-mutation-key-1"
	successSub := submissionFor(t, success, room, profile)
	successSub.IdempotencyKey = key
	stagedKeyed := requireAdmitted(t, svc, successSub)
	if _, err := svc.Commit(context.Background(), stagedKeyed); err != nil {
		t.Fatalf("keyed first commit: %v", err)
	}
	recoverySub := submissionFor(t, recovery, room, profile)
	recoverySub.IdempotencyKey = key
	stagedRecovery := requireAdmitted(t, svc, recoverySub)
	_, err = svc.Commit(context.Background(), stagedRecovery)
	if err == nil || ReasonOf(err) != ReasonIdempotencyConflict {
		t.Fatalf("duplicate key + different digest must %s, got %v", ReasonIdempotencyConflict, err)
	}
	if got := evidenceLedgerSize(t, store); got != 1 {
		t.Fatalf("conflicting commit left partial writes: ledger size %d != 1", got)
	}
	recoverySeal := parseJSON(t, func() []byte { _, seal, _ := readCase(t, recovery); return seal }())
	recoveryDigest := nestedString(t, recoverySeal, "segment_ref", "segment_digest")
	if _, found, _ := store.Get(recoveryDigest); found {
		t.Fatal("conflicting commit stored the rejected evidence body")
	}
}

// A settled segment whose path seal carries an incomplete path is valid Host
// data that admission cannot trust as complete evidence: the outcome is
// inconclusive (mark_inconclusive, Contract §13.3), never committed.
func TestAdmissionIncompletePathIsInconclusive(t *testing.T) {
	cases := segmentsCases(t)
	svc, store, profile, room := newTestService(t)
	success := firstCase(cases, "success")
	segmentJSON, _, _ := readCase(t, success)
	seg := parseJSON(t, segmentJSON)
	nested(t, seg, "path_seal_body")["paths"].([]any)[0].(map[string]any)["completeness"] = "incomplete"

	canonical, err := contract.JCS(seg)
	if err != nil {
		t.Fatalf("JCS of mutated segment: %v", err)
	}
	mutatedSegmentJSON, err := json.Marshal(seg)
	if err != nil {
		t.Fatalf("marshal mutated segment: %v", err)
	}
	seal := deriveSealEnvelope(t, seg, canonical)
	sealJSON, err := json.Marshal(seal)
	if err != nil {
		t.Fatalf("marshal re-derived seal: %v", err)
	}
	staged, err := svc.Stage(SubmittedSegment{
		SegmentJSON: mutatedSegmentJSON, SealJSON: sealJSON, Canonical: canonical,
		Submitter: testSubmitter,
		Scope:     AdmissionScope{RoomID: room, ScopeProfileRef: profile},
	})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	out, err := svc.Validate(staged)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if out.Valid || out.Status != StatusInconclusive || out.ReasonCode != ReasonMissingSealedPath {
		t.Fatalf("incomplete path: want inconclusive/%s, got valid=%v status=%s reason=%s", ReasonMissingSealedPath, out.Valid, out.Status, out.ReasonCode)
	}
	if _, err := svc.Commit(context.Background(), staged); err == nil || ReasonOf(err) != ReasonIllegalStateTransition {
		t.Fatalf("inconclusive evidence must not commit, got %v", err)
	}
	if got := evidenceLedgerSize(t, store); got != 0 {
		t.Fatalf("inconclusive evidence minted %d refs", got)
	}
}

// deriveSealEnvelope re-derives the Host seal envelope for a mutated settled
// segment exactly the way $FIX/validate_recorded.py derive_seal_envelope
// does, so the tampered fixture stays internally consistent except for the
// one field under test.
func deriveSealEnvelope(t *testing.T, seg map[string]any, canonical []byte) map[string]any {
	t.Helper()
	segmentDigest := contract.DigestBytes(canonical)
	evidenceBody := nested(t, seg, "evidence_seal_body")
	pathBody := nested(t, seg, "path_seal_body")
	evidenceDigest := digestOf(t, evidenceBody)
	pathDigest := digestOf(t, pathBody)
	segmentID := nestedString(t, seg, "segment_id")
	segmentRef := map[string]any{
		"schema_version":  "host.segment-ref.v1",
		"room_id":         seg["room_id"],
		"segment_id":      seg["segment_id"],
		"segment_version": seg["segment_version"],
		"segment_digest":  segmentDigest,
		"evidence_seal_ref": map[string]any{
			"id": "evseal-" + segmentID, "version": json.Number("1"), "digest": evidenceDigest,
		},
		"path_seal_ref": map[string]any{
			"id": "pathseal-" + segmentID, "version": json.Number("1"), "digest": pathDigest,
		},
	}
	identity := nested(t, seg, "checkpoint_identity")
	preimage := map[string]any{
		"room_id":             seg["room_id"],
		"segment_id":          seg["segment_id"],
		"segment_version":     seg["segment_version"],
		"segment_digest":      segmentDigest,
		"checkpoint_id":       identity["checkpoint_id"],
		"checkpoint_sequence": identity["checkpoint_sequence"],
		"close_intent":        nested(t, seg, "close")["close_intent"],
	}
	domains := map[string]bool{}
	for _, raw := range pathBody["paths"].([]any) {
		path := raw.(map[string]any)
		if domain, ok := contract.AsString(path["domain"]); ok {
			domains[domain] = true
		}
	}
	kind := "success_path"
	switch {
	case domains["recovery"]:
		kind = "recovery_path"
	case domains["failure"]:
		kind = "failure_path"
	}
	pathIDs := []any{}
	for _, raw := range pathBody["paths"].([]any) {
		pathIDs = append(pathIDs, raw.(map[string]any)["path_id"])
	}
	return map[string]any{
		"schema_version":        "host.segment-seal.v1",
		"segment_id":            seg["segment_id"],
		"terminal_state":        seg["terminal_state"],
		"canonical_byte_length": json.Number(itoa(len(canonical))),
		"segment_digest":        segmentDigest,
		"evidence_seal": map[string]any{
			"seal_id": "evseal-" + segmentID, "seal_version": json.Number("1"), "seal_digest": evidenceDigest,
			"evidence_ref": map[string]any{
				"schema_version": "gms.evidence-ref.v1", "evidence_id": "evseal-" + segmentID,
				"version": json.Number("1"), "evidence_digest": evidenceDigest,
				"commit_state": "sealed", "evidence_kind": kind, "source_segment_ref": segmentRef,
			},
		},
		"path_seal": map[string]any{
			"seal_id": "pathseal-" + segmentID, "seal_version": json.Number("1"), "seal_digest": pathDigest,
			"path_ids": pathIDs,
		},
		"segment_ref": segmentRef,
		"checkpoint_ref": map[string]any{
			"schema_version": "host.checkpoint-ref.v1", "room_id": seg["room_id"],
			"segment_ref": segmentRef, "checkpoint_id": identity["checkpoint_id"],
			"checkpoint_sequence": identity["checkpoint_sequence"], "checkpoint_digest": digestOf(t, preimage),
		},
	}
}

func digestOf(t *testing.T, value any) string {
	t.Helper()
	digest, err := contract.DigestOf(value)
	if err != nil {
		t.Fatalf("digest fixture value: %v", err)
	}
	return digest
}

func itoa(n int) string {
	return json.Number(jsonInt(n)).String()
}

func jsonInt(n int) string {
	raw, err := json.Marshal(n)
	if err != nil {
		panic(err)
	}
	return string(raw)
}
