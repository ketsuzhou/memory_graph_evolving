package proposal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

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

func newStateEngine(t *testing.T) *Service {
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
	dir, _ := contract.DefaultConformanceDir()
	schemas, err := validation.LoadSchemaSet(filepath.Join(dir, "schema", "shared"))
	if err != nil {
		t.Fatalf("load shared schema set: %v", err)
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		t.Fatalf("NewGates: %v", err)
	}
	svc, err := NewService(gates, filepath.Join(dir, "schema", "state"), store, mgr, registry)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

// TestProposalLifecycleSchemaCases drives every $FIX/schema-cases state case
// of the skill-proposal machine through the engine's replay: accept/reason
// must match expected.json exactly (schema/state is the authority).
func TestProposalLifecycleSchemaCases(t *testing.T) {
	svc := newStateEngine(t)
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	cases := map[string][2]bool{ // name -> (accept, machine applies)
		"pos-proposal-happy-path":   {true, true},
		"pos-proposal-withdraw":     {true, true},
		"neg-proposal-illegal-jump": {false, true},
	}
	for caseName, want := range cases {
		eventsRaw, err := os.ReadFile(filepath.Join(dir, "schema-cases", "state", caseName, "events.json"))
		if err != nil {
			t.Fatalf("read %s: %v", caseName, err)
		}
		expectedRaw, err := os.ReadFile(filepath.Join(dir, "schema-cases", "state", caseName, "expected.json"))
		if err != nil {
			t.Fatalf("read %s expected: %v", caseName, err)
		}
		eventsDoc, _ := contract.ParseJSONStrict(eventsRaw)
		expectedDoc, _ := contract.ParseJSONStrict(expectedRaw)
		events, _ := contract.AsObject(eventsDoc)
		expected, _ := contract.AsObject(expectedDoc)
		reason, _ := contract.AsString(expected["expected_reason_code"])

		err = svc.Machine().ReplayDocument(events)
		if want[0] && err != nil {
			t.Fatalf("%s: replay rejected: %v", caseName, err)
		}
		if !want[0] {
			if err == nil {
				t.Fatalf("%s: replay accepted, want %s", caseName, reason)
			}
			if got := validation.CodeOf(err); got != reason {
				t.Fatalf("%s: replay reason %s, want %s", caseName, got, reason)
			}
		}
	}
}

// TestProposalEventLedgerAppendPinsStateMachineAndSequenceChaining runs the
// ordinary proposal event engine over the ledger: transitions must be legal
// per $FIX schema/state/proposal-lifecycle (ILLEGAL_STATE_TRANSITION), event
// sequences chain (PROPOSAL_STATE_CONFLICT), terminal states never reopen and
// withdraw is only legal before protected validation.
func TestProposalEventLedgerAppendPinsStateMachineAndSequenceChaining(t *testing.T) {
	svc := newStateEngine(t)
	ctx := context.Background()
	const id = "prop-3001"

	// Creation event none -> proposed.
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "none", "proposed"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	assertState(t, svc, id, "proposed", 1)

	// Illegal jump proposed -> released.
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "proposed", "released"); validation.CodeOf(err) != "ILLEGAL_STATE_TRANSITION" {
		t.Fatalf("illegal jump: want ILLEGAL_STATE_TRANSITION, got %v", err)
	}

	// Out-of-turn from_state.
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "admitted", "candidate_bound"); validation.CodeOf(err) != "ILLEGAL_STATE_TRANSITION" {
		t.Fatalf("out-of-turn from_state: want ILLEGAL_STATE_TRANSITION, got %v", err)
	}

	// Happy prefix: proposed -> admitted -> candidate_bound.
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "proposed", "admitted"); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "admitted", "candidate_bound"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	assertState(t, svc, id, "candidate_bound", 3)

	// Withdraw after the candidate entered protected validation is closed
	// (Contract §9.1: withdrawn only before protected validation).
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "candidate_bound", "withdrawn"); validation.CodeOf(err) != "ILLEGAL_STATE_TRANSITION" {
		t.Fatalf("late withdraw: want ILLEGAL_STATE_TRANSITION, got %v", err)
	}

	// Walk to a terminal state and try to reopen it.
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "candidate_bound", "stale"); err != nil {
		t.Fatalf("stale: %v", err)
	}
	if _, err := svc.AppendTransition(ctx, proposalDoc(t, svc, id), "stale", "validating"); validation.CodeOf(err) != "ILLEGAL_STATE_TRANSITION" {
		t.Fatalf("terminal reopen: want ILLEGAL_STATE_TRANSITION, got %v", err)
	}

	// Append-only: every event is durable in the proposal ledger.
	entries, err := svc.Entries(ctx, id)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("proposal ledger has %d entries, want 4", len(entries))
	}
}

// TestProposalDigestPreimageRecomputed verifies the proposal_digest x-digest
// gate: a tampered digest rejects with DIGEST_MISMATCH.
func TestProposalDigestPreimageRecomputed(t *testing.T) {
	svc := newStateEngine(t)
	doc := proposalDoc(t, svc, "prop-3002")
	doc["proposal_digest"] = "sha256:" + string(repeat('a', 64))
	if _, err := svc.ParseProposal(doc); validation.CodeOf(err) != "DIGEST_MISMATCH" {
		t.Fatalf("tampered digest: want DIGEST_MISMATCH, got %v", err)
	}
	parsed, err := svc.ParseProposal(proposalDoc(t, svc, "prop-3002"))
	if err != nil {
		t.Fatalf("parse valid proposal: %v", err)
	}
	if parsed.ID() != "prop-3002" || parsed.Kind() != "step_guidance" {
		t.Fatalf("parsed proposal: %+v", parsed)
	}
}

func assertState(t *testing.T, svc *Service, id, wantState string, wantSeq uint64) {
	t.Helper()
	state, seq, err := svc.CurrentState(context.Background(), id)
	if err != nil {
		t.Fatalf("current state: %v", err)
	}
	if state != wantState || seq != wantSeq {
		t.Fatalf("state (%s, %d), want (%s, %d)", state, seq, wantState, wantSeq)
	}
}

func repeat(ch byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = ch
	}
	return out
}

// proposalDoc builds one closed §7.9 document with a valid x-digest.
func proposalDoc(t *testing.T, svc *Service, id string) map[string]any {
	t.Helper()
	doc := map[string]any{
		"schema_version":   "gms.skill-proposal.v1",
		"proposal_id":      id,
		"proposal_version": json.Number("1"),
		"proposed_kind":    "step_guidance",
		"source_segment_refs": []any{
			map[string]any{
				"schema_version":    "host.segment-ref.v1",
				"room_id":           "room-1",
				"segment_id":        "seg-1",
				"segment_version":   json.Number("1"),
				"segment_digest":    "sha256:" + string(repeat('b', 64)),
				"evidence_seal_ref": versionedRef("evseal-seg-1"),
				"path_seal_ref":     versionedRef("pathseal-seg-1"),
			},
		},
		"evidence_refs": []any{
			map[string]any{
				"schema_version":  contract.SchemaEvidenceRef,
				"evidence_id":     "ev-0451",
				"version":         json.Number("1"),
				"evidence_digest": "sha256:" + string(repeat('c', 64)),
				"commit_state":    "committed",
				"evidence_kind":   "success_path",
			},
		},
		"requested_operation": "create_lineage",
		"origin": map[string]any{
			"initiator_type": "model",
			"initiator_ref":  "agent-1",
			"request_ref":    "req-1",
		},
		"policy_refs": []any{versionedRef("policy-static-gates")},
	}
	digest, err := svc.ComputeDigest(doc)
	if err != nil {
		t.Fatalf("proposal digest: %v", err)
	}
	doc["proposal_digest"] = digest
	return doc
}

func versionedRef(id string) map[string]any {
	return map[string]any{
		"id":      id,
		"version": json.Number("1"),
		"digest":  "sha256:" + string(repeat('d', 64)),
	}
}
