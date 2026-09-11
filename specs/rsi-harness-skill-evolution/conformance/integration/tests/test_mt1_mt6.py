"""INT-004 MT1-MT6 merge tracer tests.

Fast tests use fixture-derived protocol drivers.  ``RSIH_INT004_FULL=1``
runs the production adapter, which executes the real GMS-208 package suite
and the RSIH materializer/lock/Composite driver.
"""
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[6]
INTEGRATION = Path(__file__).resolve().parents[1]
FIXTURES = INTEGRATION.parent

spec = importlib.util.spec_from_file_location("int004_run", INTEGRATION / "run_mt1_mt6.py")
run_mt1_mt6 = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = run_mt1_mt6
spec.loader.exec_module(run_mt1_mt6)

REPOS = {"gms": "/nonexistent-gms", "rsih": "/nonexistent-rsih"}


class FakeGms:
    def __init__(self, fixtures, mutate=None, protocol_error=False):
        self.fx = run_mt1_mt6.Fixtures(fixtures)
        self.mutate, self.protocol_error, self.calls = mutate, protocol_error, []

    def call(self, op, params):
        self.calls.append((op, dict(params)))
        if self.protocol_error:
            return {"ok": False, "error": {"code": "DRIVER_PROTOCOL_VIOLATION", "reason": "injected"}}
        mode = params.get("mode", "ok")
        table = {
            ("mt1.assess", "ok"): {"ok": True, "accepted": True, "canonical_pair": True, "ab_equals_ba": True, "assessment_digest": self.fx.digest("merge-001-similarity-ab"), "similarity_edge_recorded": True},
            ("mt1.assess", "below_threshold"): {"ok": True, "accepted": False, "reason_code": "SIMILARITY_BELOW_THRESHOLD"},
            ("mt1.assess", "cross_kind"): {"ok": True, "accepted": False, "reason_code": "SKILL_KIND_INVALID"},
            ("mt1.assess", "n_way"): {"ok": True, "accepted": False, "reason_code": "REF_MISMATCH"},
            ("mt1.assess", "uncommitted_evidence"): {"ok": True, "accepted": False, "reason_code": "EVIDENCE_NOT_COMMITTED"},
            ("mt2.admit", "ok"): {"ok": True, "accepted": True, "proposal_state": "admitted", "proposal_digest": self.fx.digest("merge-003-merge-proposal"), "single_group_winner": True, "source_heads_pinned": 2},
            ("mt2.admit", "duplicate"): {"ok": True, "accepted": False, "reason_code": "MERGE_GROUP_IN_FLIGHT", "canonical_winner_preserved": True},
            ("mt3.synthesize", "ok"): {"ok": True, "candidate_bound": True, "candidate_visible": False, "branches_preserved": True, "state": "replaying", "candidate_digest": run_mt1_mt6.sha(b"candidate")},
            ("mt3.synthesize", "blocking_conflict"): {"ok": True, "candidate_bound": False, "reason_code": "MERGE_BLOCKING_CONFLICT", "candidate_visible": False},
            ("mt4.evaluate", "ok"): {"ok": True, "accepted": True, "state": "activation_pending", "families": ["source_a", "source_b", "overlap", "conflict"], "all_families_present": True, "decision_digest": run_mt1_mt6.sha(b"decision")},
            ("mt4.evaluate", "missing_family"): {"ok": True, "accepted": False, "reason_code": "MISSING_FAMILY", "state": "inconclusive"},
            ("mt4.evaluate", "critical_regression"): {"ok": True, "accepted": False, "reason_code": "CRITICAL_REGRESSION", "state": "rejected"},
            ("mt4.evaluate", "cost_only"): {"ok": True, "accepted": False, "reason_code": "UTILITY_NOT_PARETO_IMPROVED", "state": "rejected"},
            ("mt5.activate", "ok"): {"ok": True, "activated": True, "proposal_state": "released", "released_ref": {"lineage_id": "sg-merged", "version": 1, "artifact_digest": run_mt1_mt6.sha(b"candidate")}, "derived_from": ["sg-alpha@1", "sg-beta@1"], "sources_retained": True, "supersedes_sources": False, "event_digest": run_mt1_mt6.sha(b"event"), "outbox_key": run_mt1_mt6.sha(b"outbox")},
            ("mt5.activate", "source_stale"): {"ok": True, "activated": False, "reason_code": "MERGE_SOURCE_HEAD_STALE", "residue_unchanged": True, "sources_retained": True},
            ("mt6.inspect", "ok"): {"ok": True, "servable": True, "runtime_lineages": ["sg-alpha", "sg-beta", "sg-merged"], "candidate_visible": False, "derived_from_count": 2, "similar_to_count": 1, "closure_exact": True, "closure_digest": run_mt1_mt6.sha(b"candidate")},
            ("mt6.inspect", "projection_gap"): {"ok": True, "servable": False, "reason_code": "PROJECTION_SEQUENCE_GAP", "watermark_unchanged": True},
            ("mt6.inspect", "candidate_root"): {"ok": True, "closure_read": False, "reason_code": "CANDIDATE_NOT_EXECUTABLE"},
        }
        response = dict(table[(op, mode)])
        if self.mutate:
            response = self.mutate(op, params, response)
        return response

    def close(self):
        pass


class FakeRsih:
    def __init__(self, fixtures, mutate=None):
        self.fx, self.mutate = run_mt1_mt6.Fixtures(fixtures), mutate
    def call(self, op, params):
        if op == "materialize.run":
            response = {"ok": True, "manifest_digest": self.fx.lock["expected_manifest_digest"], "published_atomically": True}
        elif op == "lock.freeze":
            response = {"ok": True, "lock_digest": self.fx.lock["expected_lock_digest"], "ref_is_manifest_digest": True}
        elif op == "composite.run":
            response = {"ok": True, "status": "succeeded", "whole_replay_passed": True, "ports_respected": True, "trace_digest": run_mt1_mt6.sha(b"trace")}
        else:
            return {"ok": False, "error": {"code": "DRIVER_OP_UNKNOWN", "reason": op}}
        return self.mutate(op, params, response) if self.mutate else response
    def close(self):
        pass


class Kit:
    def __init__(self, gms_mutate=None, rsih_mutate=None, protocol_error=False, write_fixture=None):
        self.gms_mutate, self.rsih_mutate = gms_mutate, rsih_mutate
        self.protocol_error, self.write_fixture, self.sessions = protocol_error, write_fixture, {}
    def __call__(self, name, _tools, fixtures, _repos):
        if self.write_fixture and name == "gms":
            Path(self.write_fixture).write_text("mutated", encoding="utf-8")
        driver = FakeGms(fixtures, self.gms_mutate, self.protocol_error) if name == "gms" else FakeRsih(fixtures, self.rsih_mutate)
        self.sessions[name] = driver
        return driver


def run_with(kit, evidence):
    return run_mt1_mt6.run_tracer(REPOS, FIXTURES, evidence, kit)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="int004-test-")
        self.addCleanup(self.tmp.cleanup)
        self.evidence = Path(self.tmp.name) / "evidence"


class TestGreen(Base):
    def test_merge_tracer_enforces_bilateral_replay_source_cas_retention_and_locked_materialization(self):
        result = run_with(Kit(), self.evidence)
        self.assertTrue(result["ok"], result["failures"])
        self.assertTrue(result["summary"]["fixtures_immutable"])
        self.assertEqual(result["summary"]["negatives_total"], 12)
        chain = result["summary"]["digest_chain"]
        for stage in run_mt1_mt6.CHAIN_STAGES:
            self.assertIn(stage, chain)
        for filename in [stage + ".json" for stage in run_mt1_mt6.CHAIN_STAGES] + ["negatives.json", "digest_chain.json", "summary.json"]:
            self.assertTrue((self.evidence / filename).is_file(), filename)


class TestFailClosed(Base):
    def assert_red(self, mutate, needle, rsih=False):
        result = run_with(Kit(rsih_mutate=mutate) if rsih else Kit(gms_mutate=mutate), self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any(needle.lower() in item.lower() for item in result["failures"]), result["failures"])

    def test_reversed_similarity_is_not_canonical(self):
        self.assert_red(lambda op, p, r: dict(r, ab_equals_ba=False) if op == "mt1.assess" and p.get("mode") == "ok" else r, "MT1")

    def test_duplicate_group_winner_is_accepted(self):
        self.assert_red(lambda op, p, r: dict(r, reason_code="") if op == "mt2.admit" and p.get("mode") == "duplicate" else r, "MERGE_GROUP_IN_FLIGHT")

    def test_blocking_conflict_binds_candidate(self):
        self.assert_red(lambda op, p, r: dict(r, reason_code="") if op == "mt3.synthesize" and p.get("mode") == "blocking_conflict" else r, "MERGE_BLOCKING_CONFLICT")

    def test_missing_replay_family_is_accepted(self):
        self.assert_red(lambda op, p, r: dict(r, reason_code="") if op == "mt4.evaluate" and p.get("mode") == "missing_family" else r, "MISSING_FAMILY")

    def test_source_stale_activation_is_accepted(self):
        self.assert_red(lambda op, p, r: dict(r, activated=True) if op == "mt5.activate" and p.get("mode") == "source_stale" else r, "MERGE_SOURCE_HEAD_STALE")

    def test_candidate_is_visible_at_runtime(self):
        self.assert_red(lambda op, p, r: dict(r, candidate_visible=True) if op == "mt6.inspect" and p.get("mode") == "ok" else r, "MT6")

    def test_lock_identity_is_not_manifest_identity(self):
        self.assert_red(lambda op, p, r: dict(r, ref_is_manifest_digest=False) if op == "lock.freeze" else r, "SkillLock", rsih=True)

    def test_scoring_leak_turns_red(self):
        self.assert_red(lambda op, p, r: dict(r, utility_vector=[1]) if op == "mt4.evaluate" and p.get("mode") == "ok" else r, "scoring")

    def test_protocol_error_turns_red(self):
        result = run_with(Kit(protocol_error=True), self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("DRIVER_PROTOCOL_VIOLATION" in item for item in result["failures"]))

    def test_fixture_mutation_turns_red(self):
        copied = Path(self.tmp.name) / "fixtures"
        shutil.copytree(FIXTURES, copied, ignore=shutil.ignore_patterns("__pycache__"))
        target = copied / "int004-unexpected-fixture-write.txt"
        result = run_mt1_mt6.run_tracer(REPOS, copied, self.evidence, Kit(write_fixture=target))
        self.assertFalse(result["ok"])
        self.assertTrue(any("mutated" in item for item in result["failures"]))

    def test_evidence_inside_fixture_tree_is_refused(self):
        result = run_mt1_mt6.run_tracer(REPOS, FIXTURES, FIXTURES / "int004-evidence", Kit())
        self.assertFalse(result["ok"])
        self.assertTrue(any("inside fixtures" in item for item in result["failures"]))


@unittest.skipUnless(os.environ.get("RSIH_INT004_FULL"), "set RSIH_INT004_FULL=1 for real GMS/RSIH components")
class TestRealComponents(Base):
    def test_real_components_green(self):
        result = run_mt1_mt6.run_tracer({"gms": str(ROOT / "memory_graph_evolving" / "graph-memory-service"), "rsih": str(ROOT / "RSI-Harness")}, FIXTURES, self.evidence)
        self.assertTrue(result["ok"], result["failures"])


if __name__ == "__main__":
    unittest.main()
