package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	smokefix "river2.dev/graph-memory-service/testdata/smokefix"

	"river2.dev/pi-group-chat-host/internal/pi"
)

func TestGraphBatchContractSmokeTwelveInvariants(t *testing.T) {
	run := mustRunGraphBatchContractSmoke(t)
	if len(run.Bundle.Assertions) != 12 {
		t.Fatalf("assertions = %d; want 12", len(run.Bundle.Assertions))
	}
	for i, assertion := range run.Bundle.Assertions {
		assertion := assertion
		t.Run(assertion.Name, func(t *testing.T) {
			if assertion.ID != i+1 || assertion.Name != graphBatchSmokeAssertionNames[i] {
				t.Fatalf("assertion identity = %d %q; want %d %q", assertion.ID, assertion.Name, i+1, graphBatchSmokeAssertionNames[i])
			}
			if assertion.Status != graphBatchSmokePass {
				t.Fatalf("%s status = %q evidence=%v; want pass", assertion.Name, assertion.Status, assertion.Evidence)
			}
			assertSmokeEvidence(t, assertion)
		})
	}
}

func TestGraphBatchContractSmokeWritesSelfDescribingBundle(t *testing.T) {
	run := mustRunGraphBatchContractSmoke(t)
	path := filepath.Join("testdata", "contract-smoke", "bundle.json")
	if err := writeGraphBatchSmokeBundle(path, run.Bundle); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}
	var decoded graphBatchSmokeBundle
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("bundle is not JSON: %v", err)
	}
	if decoded.SchemaVersion != graphBatchSmokeSchema || decoded.Strategy != graphBatchStrategyID || decoded.ContractID != graphBatchSmokeContractID {
		t.Fatalf("bundle identity = %#v", decoded)
	}
	if decoded.FrozenConfig != defaultGraphBatchFrozenConfig() {
		t.Fatalf("frozen config = %#v; want the contract default", decoded.FrozenConfig)
	}
	if decoded.Manifest.Digest == "" || !strings.HasPrefix(decoded.Manifest.Digest, "sha256:") {
		t.Fatalf("manifest digest = %q", decoded.Manifest.Digest)
	}
	if len(decoded.BodyDigests) == 0 {
		t.Fatal("bundle omitted body digests")
	}
	for _, digest := range decoded.BodyDigests {
		if digest.ResolvedDigest == "" || digest.ResolvedDigest != digest.ServedDigest || !graphBatchExactDigestRE.MatchString(digest.ResolvedDigest) {
			t.Fatalf("body digest = %#v; want matching exact sha256", digest)
		}
	}
	if len(decoded.Commands) == 0 || decoded.Versions["go"] == "" || decoded.Versions["pi_required"] != pi.RequiredVersion {
		t.Fatalf("commands/versions = %#v %#v", decoded.Commands, decoded.Versions)
	}
	if len(decoded.Assertions) != 12 {
		t.Fatalf("bundle assertions = %d; want 12", len(decoded.Assertions))
	}
	if decoded.PiProbe.RequiredVersion != pi.RequiredVersion {
		t.Fatalf("pi probe required version = %q", decoded.PiProbe.RequiredVersion)
	}
}

func TestGraphBatchContractSmokeKnownLimitationsAreFrozen(t *testing.T) {
	run := mustRunGraphBatchContractSmoke(t)
	got := strings.Join(run.Bundle.KnownLimitations, "\n")
	for _, want := range []string{
		"generation-0 empty graph: train episodes inherit no Skill feedback",
		"task_context_monitoring=opening_only",
		"protocol token/time unmatched",
		"crash uncertainty fail closed",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("known limitations omitted %q:\n%s", want, got)
		}
	}
	if run.Bundle.FrozenConfig.TaskContextMonitoring != "opening_only" || run.Bundle.FrozenConfig.InitialSkillSnapshot != "empty" {
		t.Fatalf("frozen config = %#v", run.Bundle.FrozenConfig)
	}
}

func TestGraphBatchContractSmokeRealPiExactSessionAbortResume(t *testing.T) {
	run := mustRunGraphBatchContractSmoke(t)
	probe := run.Bundle.PiProbe
	t.Logf("pi probe status=%s binary=%q detail=%q", probe.Status, probe.Binary, probe.Detail)
	switch probe.Status {
	case graphBatchSmokePass:
		if probe.Version != pi.RequiredVersion || !probe.Aborted || !probe.Settled {
			t.Fatalf("real Pi probe = %#v; want 0.85.1 abort+settled", probe)
		}
		if probe.UsedResumeFlag || probe.UsedContinueFlag {
			t.Fatalf("real Pi used interactive resume/continue: %#v", probe)
		}
		if run.Bundle.ContractStatus != graphBatchSmokeStatusGreen {
			t.Fatalf("contract_status = %q; want green when real Pi abort/resume is proven", run.Bundle.ContractStatus)
		}
	case graphBatchSmokeUnavailable, graphBatchSmokeSkipped:
		if run.Bundle.ContractStatus == graphBatchSmokeStatusGreen {
			t.Fatal("contract was marked green while real Pi 0.85.1 abort/resume was unavailable")
		}
		if strings.TrimSpace(probe.Detail) == "" {
			t.Fatal("unavailable Pi probe omitted a skipped/unavailable detail")
		}
	default:
		t.Fatalf("unexpected Pi probe status %q", probe.Status)
	}
}

func TestGraphBatchContractSmokeMarksIncompleteWhenPiUnavailable(t *testing.T) {
	run := mustRunGraphBatchContractSmoke(t)
	if run.Bundle.PiProbe.Status == graphBatchSmokePass {
		if run.Bundle.ContractStatus != graphBatchSmokeStatusGreen {
			t.Fatalf("all twelve passed and Pi proven, but contract_status = %q", run.Bundle.ContractStatus)
		}
		return
	}
	if run.Bundle.ContractStatus != graphBatchSmokeStatusIncomplete {
		t.Fatalf("contract_status = %q; want incomplete when Pi is %s", run.Bundle.ContractStatus, run.Bundle.PiProbe.Status)
	}
	if !allSmokePassed(run.Bundle.Assertions) {
		t.Fatal("scripted twelve-item smoke must still pass when Pi is unavailable")
	}
}

func assertSmokeEvidence(t *testing.T, assertion graphBatchSmokeAssertion) {
	t.Helper()
	ev := assertion.Evidence
	switch assertion.Name {
	case "01_task_and_memory_parallel":
		if ev["both_running_before_settle"] != true || ev["entered_running"] != float64(2) && ev["entered_running"] != 2 {
			t.Fatalf("parallel evidence = %#v", ev)
		}
	case "02_structured_mention_exact_session_abort_resume":
		if ev["abort_count"] != 1 && ev["abort_count"] != float64(1) {
			t.Fatalf("abort/resume evidence = %#v", ev)
		}
		if ev["session_file"] != "/sessions/exact-task.jsonl" || ev["session_id"] != "exact-session-id" {
			t.Fatalf("exact session = %#v", ev)
		}
		if ev["segment_reason"] != "DIRECTED_MENTION" {
			t.Fatalf("segment reason = %#v", ev)
		}
	case "03_resolved_body_digest_matches_served":
		if ev["generic_resolved"] != ev["generic_served"] || ev["specialized_resolved"] != ev["specialized_served"] {
			t.Fatalf("digest evidence = %#v", ev)
		}
	case "04_accepted_rejected_with_reason":
		if ev["rejected"] != "rejected" || ev["accepted"] != "accepted" || ev["reason_code"] != smokefix.ReasonTooGeneric {
			t.Fatalf("disposition evidence = %#v", ev)
		}
	case "05_rejected_triggers_next_graph_exploration":
		if ev["first_offer"] != smokefix.GenericRef || ev["next_offer"] != smokefix.SpecializedRef {
			t.Fatalf("redirect evidence = %#v", ev)
		}
	case "06_same_agent_no_overlapping_pi_run":
		if ev["overlapping"] != false {
			t.Fatalf("overlap evidence = %#v", ev)
		}
	case "07_effects_unknown_fail_closed":
		if ev["outcome"] != "effects_unknown" || ev["resumed"] != false || ev["pass_at_1"] != "0/1" {
			t.Fatalf("effects-unknown evidence = %#v", ev)
		}
	case "08_diagnosis_proposal_keeps_exact_evidence_lineage":
		if ev["source_checkpoint_id"] != "checkpoint-traj-smoke" {
			t.Fatalf("lineage evidence = %#v", ev)
		}
	case "09_consolidation_covers_complete_source_ids":
		if ev["complete_ids"] != true {
			t.Fatalf("consolidation evidence = %#v", ev)
		}
	case "10_held_out_feedback_does_not_enter_frozen_graph":
		if ev["graph_rejects_held_out"] != true || ev["retrieval_keys_stable"] != true {
			t.Fatalf("held-out evidence = %#v", ev)
		}
	case "11_all_tests_use_same_manifest":
		if ev["all_same"] != true {
			t.Fatalf("manifest evidence = %#v", ev)
		}
	case "12_primary_metric_uses_full_preregistered_denominator":
		if ev["denominator"] != 3 && ev["denominator"] != float64(3) {
			t.Fatalf("denominator evidence = %#v", ev)
		}
		if ev["rate"] != "1/3" {
			t.Fatalf("rate = %#v; want 1/3 with timeout/fail kept in the denominator", ev)
		}
	default:
		t.Fatalf("unknown smoke assertion %q", assertion.Name)
	}
}

func mustRunGraphBatchContractSmoke(t *testing.T) graphBatchSmokeRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	run, err := runGraphBatchContractSmoke(ctx)
	if err != nil {
		t.Fatalf("runGraphBatchContractSmoke: %v", err)
	}
	return run
}
