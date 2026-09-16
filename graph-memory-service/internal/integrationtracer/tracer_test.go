package integrationtracer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The PG-41 input boundary must fail closed before any cut is minted:
// strict single-document parses (no unknown fields, no duplicate keys, no
// trailing data), closed enums, digest shapes, internal consistency, and an
// exact known-set bijection against the frozen manifest (kiro review R1).

type jsonDoc = map[string]any

func loadDoc(t *testing.T, path string) jsonDoc {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc jsonDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return doc
}

func saveDoc(t *testing.T, name string, doc jsonDoc) string {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

const goldenFacts = "testdata/pg41_barrier_facts.json"
const goldenManifest = "testdata/pg41_batch_manifest.json"

func factsDoc(t *testing.T) jsonDoc    { return loadDoc(t, goldenFacts) }
func manifestDoc(t *testing.T) jsonDoc { return loadDoc(t, goldenManifest) }

func runs(doc jsonDoc) []any {
	return doc["logical_runs"].([]any)
}

func runAt(doc jsonDoc, index int) jsonDoc {
	return runs(doc)[index].(jsonDoc)
}

func expectReject(t *testing.T, name string, facts jsonDoc, manifest jsonDoc, wantErr string) {
	t.Helper()
	factsPath := saveDoc(t, "facts.json", facts)
	manifestPath := saveDoc(t, "manifest.json", manifest)
	_, err := BatchDiagnosis(factsPath, manifestPath)
	if err == nil {
		t.Fatalf("%s: BatchDiagnosis accepted forged input", name)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("%s: error %q does not contain %q", name, err.Error(), wantErr)
	}
	// The boundary must reject before minting cuts: the old failure mode was
	// a late "cut did not freeze" after the driver had already started.
	if strings.Contains(err.Error(), "cut did not freeze") {
		t.Fatalf("%s: rejection happened after cut minting: %v", name, err)
	}
}

func TestBatchDiagnosisGoldenFactsSucceed(t *testing.T) {
	facts, err := BatchDiagnosis(goldenFacts, goldenManifest)
	if err != nil {
		t.Fatalf("golden facts rejected: %v", err)
	}
	batch := facts["batch"].(map[string]any)
	if batch["manifest_match"] != true {
		t.Fatalf("manifest_match = %v, want true", batch["manifest_match"])
	}
	ledger := facts["ledger"].(map[string]any)
	if ledger["failed_preserved"].(int) != 1 || ledger["all_terminal"] != true {
		t.Fatalf("ledger = %v, want all_terminal with 1 preserved failure", ledger)
	}
}

func TestBatchDiagnosisRejectsUnknownTopLevelField(t *testing.T) {
	facts := factsDoc(t)
	facts["unexpected_future_contract_field"] = true
	expectReject(t, "unknown top-level field", facts, manifestDoc(t), "unexpected_future_contract_field")
}

func TestBatchDiagnosisRejectsUnknownRunField(t *testing.T) {
	facts := factsDoc(t)
	runAt(facts, 0)["room_score"] = 0.5
	expectReject(t, "unknown run field", facts, manifestDoc(t), "room_score")
}

func TestBatchDiagnosisRejectsDuplicateFactsRunID(t *testing.T) {
	facts := factsDoc(t)
	// Forge run-2 into a second run-1: copy the whole run so every per-run
	// validation still passes and the duplicate-ID boundary is what fires.
	source := runAt(facts, 0)
	runs(facts)[1] = source
	expectReject(t, "duplicate facts run id", facts, manifestDoc(t), "duplicate logical_run_id")
}

func TestBatchDiagnosisRejectsMissingKnownSetRun(t *testing.T) {
	facts := factsDoc(t)
	facts["logical_runs"] = runs(facts)[:2]
	// Keep the outbox accounting consistent (2 runs x 1 receipt x 2 rows) so
	// the known-set bijection is the check that rejects.
	ids := facts["restart"].(jsonDoc)["recovered_ids"].([]any)
	facts["restart"].(jsonDoc)["recovered_rows"] = 4
	facts["restart"].(jsonDoc)["recovered_ids"] = ids[:4]
	expectReject(t, "missing known-set run", facts, manifestDoc(t), "known-set size mismatch")
}

func TestBatchDiagnosisRejectsExtraFactsRun(t *testing.T) {
	facts := factsDoc(t)
	extra := runAt(facts, 0)
	clone := map[string]any{}
	for key, value := range extra {
		clone[key] = value
	}
	clone["logical_run_id"] = "run-ghost"
	facts["logical_runs"] = append(runs(facts), clone)
	// Keep the outbox accounting consistent (4 runs x 1 receipt x 2 rows) so
	// the known-set bijection is the check that rejects.
	ids := facts["restart"].(jsonDoc)["recovered_ids"].([]any)
	ids = append(ids, "outbox-bench-ghost-shared", "outbox-bench-ghost-private")
	facts["restart"].(jsonDoc)["recovered_rows"] = 8
	facts["restart"].(jsonDoc)["recovered_ids"] = ids
	expectReject(t, "extra facts run", facts, manifestDoc(t), "known-set size mismatch")
}

func TestBatchDiagnosisRejectsWrongExpectedAgentSet(t *testing.T) {
	facts := factsDoc(t)
	children := runAt(facts, 0)["children"].([]any)
	children[0].(jsonDoc)["agent_run_id"] = "agent-run-9z"
	expectReject(t, "wrong expected agent set", facts, manifestDoc(t), "expected agent runs")
}

func TestBatchDiagnosisRejectsIdentityTupleMismatch(t *testing.T) {
	facts := factsDoc(t)
	runAt(facts, 1)["episode_id"] = "ep-forged"
	expectReject(t, "identity tuple mismatch", facts, manifestDoc(t), "identity tuple")
}

func TestBatchDiagnosisRejectsManifestDuplicateRunID(t *testing.T) {
	manifest := manifestDoc(t)
	manifestRuns := manifest["logical_runs"].([]any)
	first := manifestRuns[0].(jsonDoc)
	clone := map[string]any{}
	for key, value := range first {
		clone[key] = value
	}
	manifest["logical_runs"] = append(manifestRuns, clone)
	expectReject(t, "manifest duplicate run id", factsDoc(t), manifest, "duplicate logical_run_id")
}

func TestBatchDiagnosisRejectsUnreleasedBarrier(t *testing.T) {
	facts := factsDoc(t)
	facts["barrier"].(jsonDoc)["released_when_durable"] = false
	expectReject(t, "unreleased barrier", facts, manifestDoc(t), "not released")
}

func TestBatchDiagnosisRejectsAdmissionInconsistentWithRelease(t *testing.T) {
	facts := factsDoc(t)
	facts["admission"].(jsonDoc)["next_episode_admitted"] = false
	expectReject(t, "admission inconsistent", facts, manifestDoc(t), "admit the next episode")
}

func TestBatchDiagnosisRejectsRestartRowMismatch(t *testing.T) {
	facts := factsDoc(t)
	facts["restart"].(jsonDoc)["recovered_rows"] = 5
	expectReject(t, "restart row mismatch", facts, manifestDoc(t), "recovered_rows")
}

func TestBatchDiagnosisRejectsMissingReceipts(t *testing.T) {
	facts := factsDoc(t)
	runAt(facts, 0)["receipts"] = []any{}
	expectReject(t, "missing receipts", facts, manifestDoc(t), "receipts are required")
}

func TestBatchDiagnosisRejectsBadDigestShape(t *testing.T) {
	facts := factsDoc(t)
	receipts := runAt(facts, 0)["receipts"].([]any)
	receipts[0].(jsonDoc)["batch_digest"] = "sha256:not-hex"
	expectReject(t, "bad digest shape", facts, manifestDoc(t), "not a sha256 digest")
}

func TestBatchDiagnosisRejectsReceiptOutsideSealedSet(t *testing.T) {
	facts := factsDoc(t)
	receipts := runAt(facts, 0)["receipts"].([]any)
	receipts[0].(jsonDoc)["segment_id"] = "segment-elsewhere"
	expectReject(t, "receipt outside sealed set", facts, manifestDoc(t), "outside the sealed set")
}

func TestBatchDiagnosisRejectsSpaceBatchNotFromReceipts(t *testing.T) {
	facts := factsDoc(t)
	spaces := runAt(facts, 0)["spaces"].([]any)
	spaces[0].(jsonDoc)["evidence_batch_ids"] = []any{"batch-forged-001"}
	expectReject(t, "space batch not from receipts", facts, manifestDoc(t), "no commit receipt produced")
}

func TestBatchDiagnosisRejectsNonTerminalChildEnum(t *testing.T) {
	facts := factsDoc(t)
	children := runAt(facts, 0)["children"].([]any)
	children[0].(jsonDoc)["status"] = "running"
	expectReject(t, "non-terminal child", facts, manifestDoc(t), "closed enum")
}

func TestBatchDiagnosisRejectsTrailingJSON(t *testing.T) {
	raw, err := os.ReadFile(goldenFacts)
	if err != nil {
		t.Fatalf("read golden facts: %v", err)
	}
	path := filepath.Join(t.TempDir(), "facts.json")
	if err := os.WriteFile(path, append(append([]byte{}, raw...), []byte("\n{}")...), 0o600); err != nil {
		t.Fatalf("write facts: %v", err)
	}
	_, err = BatchDiagnosis(path, goldenManifest)
	if err == nil || !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("trailing JSON: err = %v, want trailing data rejection", err)
	}
}

func TestBatchDiagnosisRejectsDuplicateJSONKey(t *testing.T) {
	raw, err := os.ReadFile(goldenFacts)
	if err != nil {
		t.Fatalf("read golden facts: %v", err)
	}
	// Inject a duplicate batch_id key at the head of the top-level object.
	forged := `{"batch_id":"batch-eval-forged",` + strings.TrimPrefix(string(raw), "{")
	path := filepath.Join(t.TempDir(), "facts.json")
	if err := os.WriteFile(path, []byte(forged), 0o600); err != nil {
		t.Fatalf("write facts: %v", err)
	}
	_, err = BatchDiagnosis(path, goldenManifest)
	if err == nil || !strings.Contains(err.Error(), `duplicate JSON key "batch_id"`) {
		t.Fatalf("duplicate key: err = %v, want duplicate JSON key rejection", err)
	}
}
