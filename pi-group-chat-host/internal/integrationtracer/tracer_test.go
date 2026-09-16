package integrationtracer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

var goldenManifest = filepath.Join("testdata", "pg41_batch_manifest.json")

// TestFreezeEmitsStoreBackedContractFacts drives the PG-40 Host half and
// pins its R4 contract: the sealed room's evidence settles through the real
// evidence service (two durable outbox rows per receipt), the space scopes
// derive from rows read back out of the store, and the projection
// head/watermark values are explicitly labeled fixture-authority.
func TestFreezeEmitsStoreBackedContractFacts(t *testing.T) {
	freezePath := filepath.Join(t.TempDir(), "host_freeze.json")
	facts, err := Freeze(freezePath)
	if err != nil {
		t.Fatalf("freeze scenario: %v", err)
	}
	sealed := facts["sealed_room"].(map[string]any)
	if !sealed["submission"].(bool) || !sealed["idempotent_replay"].(bool) || !sealed["body_conflict"].(bool) {
		t.Fatalf("freeze invariants missing: %#v", sealed)
	}
	receipts := int(sealed["receipts"].(int))
	if sealed["outbox_rows_persisted"].(int) != receipts*2 {
		t.Fatalf("expected %d durable outbox rows for %d receipts, got %v", receipts*2, receipts, sealed["outbox_rows_persisted"])
	}
	if !sealed["batches_match_receipts"].(bool) {
		t.Fatal("store-derived space batches do not match the receipt batches")
	}
	if sealed["projection_watermark_source"] != "fixture_authority" {
		t.Fatalf("watermark source = %v, want fixture_authority", sealed["projection_watermark_source"])
	}
	open := facts["open_room"].(map[string]any)
	if open["submission"].(bool) || open["deferred_segments"].(int) == 0 {
		t.Fatalf("open room was not deferred fail-closed: %#v", open)
	}

	raw, err := os.ReadFile(freezePath)
	if err != nil {
		t.Fatalf("read freeze output: %v", err)
	}
	var freeze struct {
		SealedSegmentIDs []string `json:"sealed_segment_ids"`
		Submission       bool     `json:"submission_permitted"`
		Receipts         []struct {
			SegmentID string   `json:"segment_id"`
			BatchIDs  []string `json:"batch_ids"`
		} `json:"receipts"`
		Spaces []struct {
			Scope            string   `json:"scope"`
			EvidenceBatchIDs []string `json:"evidence_batch_ids"`
		} `json:"spaces"`
	}
	if err := json.Unmarshal(raw, &freeze); err != nil {
		t.Fatalf("decode freeze output: %v", err)
	}
	if !freeze.Submission || len(freeze.Receipts) != len(freeze.SealedSegmentIDs) {
		t.Fatalf("receipts do not cover the sealed set exactly: %#v", freeze)
	}
	if len(freeze.Spaces) != 2 || freeze.Spaces[0].Scope != "shared" || freeze.Spaces[1].Scope != "private" {
		t.Fatalf("expected one shared and one private space scope: %#v", freeze.Spaces)
	}
	private := map[string]bool{}
	for _, batch := range freeze.Spaces[1].EvidenceBatchIDs {
		private[batch] = true
	}
	receiptBatches := map[string]bool{}
	covered := map[string]int{}
	for _, receipt := range freeze.Receipts {
		covered[receipt.SegmentID]++
		for _, batch := range receipt.BatchIDs {
			receiptBatches[batch] = true
		}
	}
	for _, segment := range freeze.SealedSegmentIDs {
		if covered[segment] != 1 {
			t.Fatalf("segment %s covered %d times, want exactly one receipt", segment, covered[segment])
		}
	}
	for _, batch := range freeze.Spaces[0].EvidenceBatchIDs {
		if private[batch] || !receiptBatches[batch] {
			t.Fatalf("shared batch %s overlaps private space or has no commit receipt", batch)
		}
	}
	for _, batch := range freeze.Spaces[1].EvidenceBatchIDs {
		if !receiptBatches[batch] {
			t.Fatalf("private batch %s has no commit receipt", batch)
		}
	}
}

// TestBenchmarkBarrierAdmitsThroughStoreAuthority drives the PG-41 Host half
// and pins its R5 contract: admission is the store's batch-gate authority,
// only a live claim lease advances an outbox row, a crashed worker's token
// dies at the restart boundary, one batch admits exactly once, and a late
// settlement blocks the gate under the same lock.
func TestBenchmarkBarrierAdmitsThroughStoreAuthority(t *testing.T) {
	factsPath := filepath.Join(t.TempDir(), "host_barrier_facts.json")
	facts, err := BenchmarkBarrier(goldenManifest, factsPath)
	if err != nil {
		t.Fatalf("benchmark barrier scenario: %v", err)
	}
	admission := facts["admission"].(map[string]any)
	for _, probe := range []string{
		"refused_while_pending", "refused_while_partial", "admitted_when_released",
		"unleased_stage_rejected", "stale_worker_rejected", "restage_refused",
		"double_admission_rejected", "check_vs_new_outbox_blocked",
		"double_worker_distinct", "foreign_stage_rejected",
	} {
		if !admission[probe].(bool) {
			t.Fatalf("admission probe %s = false: %#v", probe, admission)
		}
	}
	restart := facts["restart"].(map[string]any)
	if !restart["staged_blocked"].(bool) || !restart["crashed_worker_rejected"].(bool) {
		t.Fatalf("restart probes failed: %#v", restart)
	}
	barrier := facts["barrier"].(map[string]any)
	if !barrier["released_when_durable"].(bool) {
		t.Fatalf("barrier did not release: %#v", barrier)
	}

	raw, err := os.ReadFile(factsPath)
	if err != nil {
		t.Fatalf("read barrier facts: %v", err)
	}
	var file struct {
		Barrier struct {
			ReleasedWhenDurable bool `json:"released_when_durable"`
		} `json:"barrier"`
		Admission struct {
			NextEpisodeAdmitted bool   `json:"next_episode_admitted"`
			Reason              string `json:"reason"`
		} `json:"admission"`
		Restart struct {
			RecoveredRows int `json:"recovered_rows"`
		} `json:"restart"`
		LogicalRuns []struct {
			LogicalRunID  string `json:"logical_run_id"`
			TerminalState string `json:"terminal_state"`
			Children      []struct {
				AgentRunID string `json:"agent_run_id"`
				Status     string `json:"status"`
			} `json:"children"`
		} `json:"logical_runs"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode barrier facts: %v", err)
	}
	if !file.Barrier.ReleasedWhenDurable || !file.Admission.NextEpisodeAdmitted || file.Admission.Reason != "BARRIER_RELEASED" {
		t.Fatalf("facts file barrier/admission invariants failed: %#v", file)
	}
	if len(file.LogicalRuns) != 3 || file.Restart.RecoveredRows != 6 {
		t.Fatalf("expected 3 logical runs over 6 outbox rows: %#v", file)
	}
	failed := 0
	for _, run := range file.LogicalRuns {
		for _, child := range run.Children {
			if child.Status != "succeeded" && child.Status != "failed" {
				t.Fatalf("child %s has non-terminal status %q", child.AgentRunID, child.Status)
			}
		}
		if run.TerminalState == "failed" {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("expected exactly one failed logical run preserved, got %d", failed)
	}
}

func TestSameStringSet(t *testing.T) {
	if !sameStringSet([]string{"a", "b"}, []string{"b", "a"}) || sameStringSet([]string{"a"}, []string{"a", "b"}) || sameStringSet(nil, []string{""}) {
		t.Fatal("sameStringSet set equality is wrong")
	}
}
