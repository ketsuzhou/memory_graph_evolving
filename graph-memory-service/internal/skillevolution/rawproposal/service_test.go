package rawproposal

import (
	"context"
	"path/filepath"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

func TestAdmissionValidFixtureReadsByFullIDAndTracesBothWays(t *testing.T) {
	svc := newFixtureService(t)
	trajectory, err := svc.ReadTrajectory(context.Background(), "diagnosis-run-01", "trajectory-01")
	if err != nil { t.Fatalf("read complete trajectory: %v", err) }
	if len(trajectory.CompleteTrajectory) != 3 || trajectory.PublicOutcome == "" { t.Fatalf("diagnosis read was not a complete public frozen trajectory: %#v", trajectory) }

	ref, err := svc.Admit(context.Background(), validRequest(t, "idem-valid", "raw-proposal-0000000000000001"))
	if err != nil { t.Fatalf("admit valid fixture: %v", err) }
	if ref.ProposalID != "raw-proposal-0000000000000001" || ref.URI == "" { t.Fatalf("returned ref is not full canonical identity: %#v", ref) }
	proposal, ok := svc.Get(ref.ProposalID)
	if !ok || proposal.ContentDigest != ref.ContentDigest || proposal.NoveltyStatus != NoveltyHypothesized { t.Fatalf("full-id readback = %#v, %v", proposal, ok) }
	if prefix := ref.ProposalID[:len(ref.ProposalID)-1]; prefix != "" {
		if _, hit := svc.Get(prefix); hit {
			t.Fatalf("truncated proposal id %q must not identify the record", prefix)
		}
	}
	provenance, ok := svc.ProvenanceForProposal(ref.ProposalID)
	if !ok || provenance.SourceCheckpoint.ID != "checkpoint-01" || len(provenance.SourceEvidence) != 1 || provenance.SourceEvidence[0].ID != "evidence-01" { t.Fatalf("proposal provenance = %#v, %v", provenance, ok) }
	if got := svc.ProposalsForCheckpoint("checkpoint-01"); len(got) != 1 || got[0] != ref { t.Fatalf("checkpoint reverse trace = %#v", got) }
	if got := svc.ProposalsForEvidence("evidence-01"); len(got) != 1 || got[0] != ref { t.Fatalf("evidence reverse trace = %#v", got) }
}

func TestAdmissionRejectsInvalidEvidenceWithoutPartialAppend(t *testing.T) {
	cases := []struct { name string; mutate func(AdmissionRequest) AdmissionRequest }{
		{"missing", func(r AdmissionRequest) AdmissionRequest { r.Body["source_evidence_refs"] = []any{"missing-evidence"}; refreshDigest(t, r.Body); return r }},
		{"cross_snapshot", func(r AdmissionRequest) AdmissionRequest { r.Body["source_evidence_refs"] = []any{"evidence-cross-snapshot"}; refreshDigest(t, r.Body); return r }},
		{"unauthorized", func(r AdmissionRequest) AdmissionRequest { r.DiagnosisRunID = "unauthorized-run"; r.Body["created_by_agent_run_id"] = "unauthorized-run"; refreshDigest(t, r.Body); return r }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newFixtureService(t)
			r := validRequest(t, "idem-"+tc.name, "raw-proposal-"+tc.name+"-00001")
			if _, err := svc.Admit(context.Background(), tc.mutate(r)); err == nil { t.Fatal("admission succeeded, want exact-reference rejection") }
			if got := svc.Count(); got != 0 { t.Fatalf("failed admission produced %d partial records", got) }
		})
	}
}

func TestAdmissionRejectsNonSpecificOrUnevidencedAdvice(t *testing.T) {
	cases := []struct { name string; mutate func(map[string]any) }{
		{"global_trigger", func(b map[string]any) { b["context_trigger"] = "Always apply this to every task" }},
		{"generic_advice", func(b map[string]any) { b["decision_policy_or_steps"] = []any{"be careful"} }},
		{"no_baseline_delta", func(b map[string]any) { b["expected_behavior_change"] = b["baseline_behavior"] }},
		{"unevidenced_insight", func(b map[string]any) { b["non_obvious_insight"] = "The database is always the bottleneck." }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newFixtureService(t)
			r := validRequest(t, "idem-"+tc.name, "raw-proposal-"+tc.name+"-00002")
			tc.mutate(r.Body); refreshDigest(t, r.Body)
			if _, err := svc.Admit(context.Background(), r); err == nil { t.Fatal("admission succeeded, want semantic rejection") }
			if got := svc.Count(); got != 0 { t.Fatalf("semantic rejection produced %d records", got) }
		})
	}
}

func TestAdmissionRejectsHiddenGoldAndUnknownFields(t *testing.T) {
	cases := []struct { name string; mutate func(map[string]any) }{
		{"hidden_test_field", func(b map[string]any) { b["hidden_test"] = "secret" }},
		{"gold_content", func(b map[string]any) { b["non_obvious_insight"] = "the relative path failed; use the gold solution" }},
		{"unknown_core_field", func(b map[string]any) { b["validated"] = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newFixtureService(t)
			r := validRequest(t, "idem-"+tc.name, "raw-proposal-"+tc.name+"-00003")
			tc.mutate(r.Body)
			if _, err := svc.Admit(context.Background(), r); validation.CodeOf(err) != validation.CodeSchemaFieldUnknown { t.Fatalf("error code = %q (%v), want %q", validation.CodeOf(err), err, validation.CodeSchemaFieldUnknown) }
			if got := svc.Count(); got != 0 { t.Fatalf("leak/unknown-field rejection produced %d records", got) }
		})
	}
}

func TestAdmissionIdempotencySameBodyReplaysAndDifferentBodyConflicts(t *testing.T) {
	svc := newFixtureService(t)
	request := validRequest(t, "idem-replay", "raw-proposal-idempotency-00004")
	first, err := svc.Admit(context.Background(), request)
	if err != nil { t.Fatalf("first admission: %v", err) }
	second, err := svc.Admit(context.Background(), request)
	if err != nil { t.Fatalf("same-body replay: %v", err) }
	if first != second || svc.Count() != 1 { t.Fatalf("replay ref/count = %#v %#v / %d", first, second, svc.Count()) }

	conflict := validRequest(t, "idem-replay", "raw-proposal-different-body-00005")
	if _, err := svc.Admit(context.Background(), conflict); ledger.ReasonOf(err) != ledger.ReasonIdempotencyConflict { t.Fatalf("different-body error = %v, want %s", err, ledger.ReasonIdempotencyConflict) }
	if svc.Count() != 1 { t.Fatalf("conflicting request appended record count %d", svc.Count()) }
}

func newFixtureService(t *testing.T) *Service {
	t.Helper()
	dir, err := contract.DefaultConformanceDir(); if err != nil { t.Fatalf("conformance dir: %v", err) }
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy")); if err != nil { t.Fatalf("reason policy: %v", err) }
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	store, err := ledger.NewMemoryStore(registry); if err != nil { t.Fatalf("memory ledger: %v", err) }
	tx, err := ledger.NewManager(store, registry); if err != nil { t.Fatalf("transaction manager: %v", err) }
	source := NewMemoryTrajectorySource()
	source.Put(FrozenTrajectory{
		ID: "trajectory-01", SnapshotID: "snapshot-01", CompleteTrajectory: []string{"opening", "checkpoint", "outcome"}, PublicOutcome: "failed: file-not-found",
		Checkpoints: map[string]Checkpoint{"checkpoint-01": {ID: "checkpoint-01", SnapshotID: "snapshot-01"}},
		Evidence: map[string]Evidence{
			"evidence-01": {ID: "evidence-01", TrajectoryID: "trajectory-01", SnapshotID: "snapshot-01", ObservableFacts: []string{"the relative path resolved outside the workspace"}},
			"evidence-cross-snapshot": {ID: "evidence-cross-snapshot", TrajectoryID: "trajectory-01", SnapshotID: "snapshot-other", ObservableFacts: []string{"the relative path resolved outside the workspace"}},
		},
		AuthorizedDiagnosisRunIDs: map[string]bool{"diagnosis-run-01": true},
	})
	svc, err := NewService(source, tx); if err != nil { t.Fatalf("raw proposal service: %v", err) }
	return svc
}

func validRequest(t *testing.T, idempotencyKey, proposalID string) AdmissionRequest {
	t.Helper()
	body := map[string]any{
		"proposal_id": proposalID, "schema_version": SchemaVersion, "source_checkpoint_id": "checkpoint-01", "source_evidence_refs": []any{"evidence-01"},
		"context_trigger": "When the relative path resolved outside the workspace at checkpoint-01", "failure_or_opportunity": "The relative path resolved outside the workspace and caused file-not-found.",
		"baseline_behavior": "Invoke the tool with the relative path directly.", "non_obvious_insight": "The relative path resolved outside the workspace, so tool invocation needs an explicit workspace-root path.",
		"decision_policy_or_steps": []any{"When the relative path resolved outside the workspace, resolve it against the workspace root before invoking the tool."},
		"expected_behavior_change": "Resolve the path against the workspace root before invoking the tool.", "contraindications": []any{}, "pitfalls": []any{"Do not rewrite an already absolute path."},
		"outcome_observed": "failed: file-not-found", "novelty_status": NoveltyHypothesized, "created_by_agent_run_id": "diagnosis-run-01", "created_at": "2026-09-17T09:53:15Z",
	}
	refreshDigest(t, body)
	return AdmissionRequest{IdempotencyKey: idempotencyKey, DiagnosisRunID: "diagnosis-run-01", TrajectoryID: "trajectory-01", Body: body}
}

func refreshDigest(t *testing.T, body map[string]any) {
	t.Helper()
	proposal, err := parseBody(withDigestPlaceholder(body)); if err != nil { t.Fatalf("parse fixture body: %v", err) }
	digest, err := contentDigest(proposal); if err != nil { t.Fatalf("content digest: %v", err) }
	body["content_digest"] = digest
}
func withDigestPlaceholder(body map[string]any) map[string]any { if _, ok := body["content_digest"]; !ok { body["content_digest"] = "sha256:placeholder" }; return body }
