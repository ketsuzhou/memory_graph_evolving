"""INT-003 S5-S9 GMS/RSIH/Host conformance tracer tests.

Stdlib ``unittest`` only. The suite never spawns the real ``go``/``node``
drivers on the fast path: every test injects fake driver sessions that
materialise canned instruction responses derived from the frozen fixtures
tree (the frozen materialization manifests/locks/artifact digests keep the
fakes honest). The real three-repo run is an opt-in test behind the
``RSIH_INT003_FULL`` environment variable.
"""

import hashlib
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

INTEGRATION_ROOT = Path(__file__).resolve().parents[1]
CONFORMANCE_ROOT = INTEGRATION_ROOT.parent

_SPEC = importlib.util.spec_from_file_location(
    "int003_run_s5_s9", INTEGRATION_ROOT / "run_s5_s9.py"
)
run_s5_s9 = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = run_s5_s9
_SPEC.loader.exec_module(run_s5_s9)

TOOLCHAIN = {"go": "go1.26.1", "node": "v22.22.2", "python": "3.12.3"}

FIXTURES = CONFORMANCE_ROOT

REPO_DIRS = {
    "host": "/nonexistent-host",
    "gms": "/nonexistent-gms",
    "rsih": "/nonexistent-rsih",
}


def sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def label_digest(label: str) -> str:
    return sha(("int003-fake:" + label).encode("utf-8"))


def jcs(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# The good chain: canned driver responses derived from the frozen fixtures
# ---------------------------------------------------------------------------

class GoodChain:
    """Builds the green-path response of every driver instruction.

    Fixture-frozen digests (materialization manifests/locks, composite
    artifact refs) are read from the fixtures themselves so the fakes stay
    honest; driver-private digests are stable labelled fakes exactly like
    the real drivers' internal records.
    """

    def __init__(self, fixtures):
        self.fixtures = Path(fixtures)
        # Frozen materialization expectations (three-layer identity).
        self._mat_cases = {}
        manifest = load_json(self.fixtures / "materialization" / "manifest.json")
        for case in manifest["cases"]:
            case_dir = self.fixtures / "materialization" / case["path"]
            expected = load_json(case_dir / "expected.json")
            self._mat_cases[case["case_id"]] = {
                "case": case,
                "expected": expected,
                "manifest": load_json(case_dir / "manifest-document.json"),
            }
        # Frozen composite artifact (S9 child/ports/permissions authority).
        self._composite = load_json(
            self.fixtures / "artifacts" / "artifact-003-composite" / "source.json")

    # -- S5 (gms release) ---------------------------------------------------

    def release_activate(self, params):
        mode = params.get("mode", "ok")
        if mode == "stale_cas":
            return {
                "mode": mode,
                "activated": False,
                "reason_code": "ACTIVE_HEAD_CONFLICT",
                "residue": {
                    "activation_entries": 0, "lineage_head_unchanged": True,
                    "sequence_head_unchanged": True, "outbox_pending": 0,
                    "proposal_states": "activation_pending",
                },
                "residue_unchanged": True,
            }
        if mode == "not_accepted":
            return {
                "mode": mode,
                "activated": False,
                "reason_code": "RELEASE_NOT_ACCEPTED",
                "residue_unchanged": True,
            }
        if mode == "body_mismatch":
            return {
                "mode": mode,
                "activated": False,
                "reason_code": "CANDIDATE_RELEASED_BODY_MISMATCH",
                "residue_unchanged": True,
            }
        # ok: the atomic release over the first lineage
        released = label_digest("released-body-v1")
        return {
            "mode": "ok",
            "activated": True,
            "lineage_id": params.get("lineage_id", "sg-alpha"),
            "version": params.get("version", 1),
            "released_ref": {
                "schema_version": "gms.skill-artifact-ref.v2",
                "lineage_id": params.get("lineage_id", "sg-alpha"),
                "version": params.get("version", 1),
                "kind": params.get("kind", "step_guidance"),
                "artifact_digest": released,
            },
            "activation_sequence": params.get("activation_sequence", 1),
            "event_id": "evt-" + params.get("lineage_id", "sg-alpha") + "-1",
            "event_digest": label_digest("activation-event-1"),
            "outbox_key": label_digest("outbox-1"),
            "outbox_pending": 1,
            "previous_active_ref": None,
            "residue": {
                "activation_entries": 1, "lineage_head_version": 1,
                "candidate_mapped": True, "outbox_pending": 1,
                "proposal_state": "released",
            },
            "residue_atomic": True,
        }

    # -- S6 (gms projector) ---------------------------------------------------

    def projector_project(self, params):
        mode = params.get("mode", "runtime")
        if mode == "gap":
            return {
                "mode": mode,
                "state": "blocked",
                "blocked_code": "PROJECTION_SEQUENCE_GAP",
                "watermark_unchanged": True,
                "graph_digest": label_digest("graph-empty"),
            }
        if mode == "conflict":
            return {
                "mode": mode,
                "state": "blocked",
                "blocked_code": "PROJECTION_EVENT_CONFLICT",
                "watermark_unchanged": True,
            }
        # runtime / delivered / duplicate
        projected = params.get("projected", 1)
        return {
            "mode": mode,
            "state": "current",
            "projected": projected,
            "cursor_activation": params.get("cursor", projected),
            "watermark": {
                "schema_version": "gms.projection-watermark.v1",
                "projected_through_activation_sequence": params.get(
                    "cursor", projected),
            },
            "watermark_digest": label_digest("watermark-%d"
                                             % params.get("cursor", projected)),
            "graph_digest": label_digest("graph-%d"
                                         % params.get("cursor", projected)),
            "duplicate_noop": mode == "duplicate",
            "blocked": False,
        }

    # -- S7 (gms retrieval tools + host same-call) ----------------------------

    def tools_invoke(self, params):
        mode = params.get("mode", "ok")
        if mode == "behind":
            return {
                "mode": mode,
                "status": "failed",
                "reason_code": "PROJECTION_BEHIND_REQUIRED_SEQUENCE",
                "failed_closed": True,
            }
        return {
            "mode": mode,
            "tool_name": params.get("tool_name", "memory_explore"),
            # The real GMS-206 retrieval Response.Status is the frozen
            # "success" (see internal/skillevolution/retrieval/service.go);
            # the Host toolproxy's own vocabulary is "succeeded" — two
            # different layers, two different frozen words.
            "status": "success",
            "upstream_result_digest": label_digest(
                "explore-result-" + params.get("session", "exp-s5s9")),
            "result_carry_watermark": True,
            "read_audit_present": True,
        }

    def toolproxy_explore(self, params):
        mode = params.get("mode", "ok")
        if mode == "rewrite":
            # The correct same-call outcome of a Host-side rewrite attempt:
            # the rewritten bytes never reach the Pi return channel — the
            # frozen terminal replays and the delivery fails closed.
            return {
                "mode": mode,
                "status": "failed",
                "reason_code": "TOOLPROXY_REWRITE_REFUSED",
                "rewrite_rejected": True,
                "pi_bytes_equal_upstream": False,
                "pi_digest": label_digest("rewritten"),
            }
        return {
            "mode": mode,
            "case_id": params.get("case_id", "explore-ok"),
            "status": "succeeded",
            "upstream_result_digest": label_digest("explore-result-exp-s5s9"),
            "pi_bytes_equal_upstream": True,
            "pi_digest": label_digest("explore-canonical"),
            "canonical_digest": label_digest("explore-canonical"),
            "pi_deliveries": 1,
            "delivered_before_end": True,
            "result_order_preserved": True,
            "watermark_present": True,
        }

    # -- S8 (gms closure + rsih materialization) ------------------------------

    def closure_read(self, params):
        mode = params.get("mode", "ok")
        if mode == "graph_root":
            return {
                "mode": mode,
                "reason_code": "NON_EXACT_REF",
                "read_failed": True,
            }
        if mode == "torn":
            return {
                "mode": mode,
                "reason_code": "ACTIVATION_SEQUENCE_CONFLICT",
                "read_failed": True,
            }
        if mode == "not_current":
            return {
                "mode": mode,
                "reason_code": "SKILL_NOT_CURRENT_ACTIVE",
                "read_failed": True,
            }
        nodes = params.get("nodes") or [
            "comp-review-orchestrator", "proc-diff-summarizer",
            "sg-commit-checklist",
        ]
        return {
            "mode": "ok",
            "schema_version": "gms.materialization-closure-read.v1",
            "roots": [params.get("root_lineage", nodes[0])],
            "node_lineages": nodes,
            "activation_sequence": params.get("activation_sequence", 21),
            "activation_head_digest": label_digest("activation-head-21"),
            "torn_read_token": label_digest("torn-token-21"),
            "canonical_bytes_digest_match": True,
            "watermark_informational": True,
        }

    def materialize(self, params):
        case_id = params.get("case_id", "lock-002-composite-freeze")
        mode = params.get("mode", "ok")
        case = self._mat_cases.get(case_id)
        expected = (case or {}).get("expected", {})
        manifest_digest = expected.get("expected_manifest_digest",
                                       label_digest("manifest-" + case_id))
        bundle_digest = expected.get("expected_bundle_digest",
                                     label_digest("bundle-" + case_id))
        if mode == "bundle_digest_as_ref":
            return {
                "mode": mode,
                "ok": False,
                "reason_code": "MATERIALIZATION_REF_INVALID",
                "materialization_ref_digest": bundle_digest,
                "ref_is_manifest_digest": False,
            }
        return {
            "mode": mode,
            "ok": True,
            "case_id": case_id,
            "materialization_id": params.get(
                "materialization_id", "mat-comp-0002"),
            "manifest_version": 1,
            "manifest_digest": manifest_digest,
            "bundle_digest": bundle_digest,
            "digests_distinct": manifest_digest != bundle_digest,
            "published_atomically": True,
            "published_files": 3,
            "created_from_activation_sequence": params.get(
                "activation_sequence", 21),
        }

    def lock_freeze(self, params):
        mode = params.get("mode", "ok")
        case_id = params.get("case_id", "lock-002-composite-freeze")
        case = self._mat_cases.get(case_id)
        expected = (case or {}).get("expected", {})
        manifest_digest = expected.get("expected_manifest_digest",
                                       label_digest("manifest-" + case_id))
        if mode == "bundle_ref":
            case = self._mat_cases.get(case_id)
            bundle_digest = expected.get(
                "expected_bundle_digest", label_digest("bundle-" + case_id))
            return {
                "mode": mode,
                "ok": False,
                "reason_code": "MATERIALIZATION_REF_INVALID",
                "ref_digest_used": bundle_digest,
            }
        if mode == "stale_sequence":
            return {
                "mode": mode,
                "ok": False,
                "reason_code": "MANIFEST_SEQUENCE_STALE",
            }
        if mode == "partial_publish":
            # The correct lock outcome over a partially published bundle
            # (2 of 3 files landed): the freeze refuses — a SkillLock may
            # never freeze over an incomplete publish.
            return {
                "mode": mode,
                "ok": False,
                "reason_code": "MANIFEST_PARTIAL",
                "materialization_ref_digest": manifest_digest,
                "ref_is_manifest_digest": True,
                "bundle_files_published": 2,
                "bundle_files_expected": 3,
            }
        return {
            "mode": "ok",
            "ok": True,
            "case_id": case_id,
            "lock_digest": expected.get(
                "expected_lock_digest", label_digest("lock-" + case_id)),
            "materialization_ref_digest": manifest_digest,
            "ref_is_manifest_digest": True,
            "activation_sequence_at_freeze": params.get(
                "activation_sequence", 21),
            "locked_closure_entries": 3,
            "bundle_files_published": 3,
            "bundle_files_expected": 3,
        }

    # -- S9 (rsih composite) --------------------------------------------------

    def composite_run(self, params):
        mode = params.get("mode", "ok")
        composite = self._composite
        child_ids = [c["child_id"] for c in
                     composite["body"]["children"]]
        if mode == "dynamic_child":
            return {
                "mode": mode,
                "status": "failed",
                "reason_code": "NON_EXACT_REF",
                "child_ids": child_ids,
            }
        if mode == "cycle":
            return {
                "mode": mode,
                "status": "failed",
                "reason_code": "COMPOSITE_CYCLE",
            }
        if mode == "drift":
            return {
                "mode": mode,
                "status": "failed",
                "reason_code": "REPLAY_NONDETERMINISTIC",
            }
        return {
            "mode": mode,
            "status": "succeeded",
            "child_ids": child_ids,
            "schedule": child_ids,
            "attempts": len(child_ids),
            "permissions_within_host_cap": True,
            "permission_union": [
                {"capability": "skill_get", "scope": "pi-session"}],
            "host_cap": [{"capability": "skill_get", "scope": "pi-session"}],
            "output_digest": label_digest("composite-output"),
            "trace_digest": label_digest("composite-trace"),
            "trace_digest_stable": True,
            "run_id": label_digest("composite-run"),
            "whole_replay_passed": True,
            "committed_child_outputs": len(child_ids),
            "ports_respected": True,
        }

    def registry_resolve(self, params):
        mode = params.get("mode", "exact")
        if mode == "candidate":
            return {
                "mode": mode,
                "ok": False,
                "reason_code": "CANDIDATE_NOT_EXECUTABLE",
            }
        if mode == "latest":
            return {
                "mode": mode,
                "ok": False,
                "reason_code": "NON_EXACT_REF",
            }
        return {
            "mode": mode,
            "ok": True,
            "lineage_id": params.get("lineage_id", "sg-alpha"),
            "materialized_path": "skills/%s/SKILL.md"
                                 % params.get("lineage_id", "sg-alpha"),
            "file_digest": label_digest("file-%s"
                                        % params.get("lineage_id", "sg-alpha")),
        }


class FakeDriver:
    """One driver session; optional hooks mutate the good responses."""

    def __init__(self, name, fixtures, mutate=None, fail_ops=(),
                 write_fixture=None, protocol_error=False):
        self.name = name
        self.good = GoodChain(fixtures)
        self.mutate = mutate
        self.fail_ops = tuple(fail_ops)
        self.write_fixture = write_fixture
        self.protocol_error = protocol_error
        self.calls = []

    def call(self, op, params):
        self.calls.append((op, dict(params)))
        if self.write_fixture is not None and op == self.write_fixture[0]:
            path = Path(self.write_fixture[1])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("mutated", encoding="utf-8")
        if self.protocol_error:
            return {"ok": False, "id": 1,
                    "error": {"code": "DRIVER_PROTOCOL_VIOLATION",
                              "reason": "fake driver protocol error"}}
        if op in self.fail_ops:
            return {"ok": False, "id": 1,
                    "error": {"code": "DRIVER_OP_FAILED",
                              "reason": "fake driver failure for " + op}}
        response = self._good(op, params)
        if self.mutate is not None:
            response = self.mutate(op, params, response)
        # The protocol envelope defaults to ok=true but NEVER clobbers a
        # semantic outcome: the canned refusal responses (lock.freeze
        # bundle_ref, registry.resolve candidate, ...) carry their own
        # ok=false + reason_code -- the op ran, the operation itself
        # refused fail-closed.
        response.setdefault("ok", True)
        response["op"] = op
        return response

    def _good(self, op, params):
        if self.name == "gms":
            if op == "release.activate":
                return self.good.release_activate(params)
            if op == "projector.project":
                return self.good.projector_project(params)
            if op == "tools.invoke":
                return self.good.tools_invoke(params)
            if op == "closure.read":
                return self.good.closure_read(params)
        elif self.name == "rsih":
            if op == "materialize.run":
                return self.good.materialize(params)
            if op == "lock.freeze":
                return self.good.lock_freeze(params)
            if op == "composite.run":
                return self.good.composite_run(params)
            if op == "registry.resolve":
                return self.good.registry_resolve(params)
        elif self.name == "host":
            if op == "toolproxy.explore":
                return self.good.toolproxy_explore(params)
        raise AssertionError("unexpected driver op %r for %s" % (op, self.name))

    def close(self):
        return None


class DriverKit:
    """Builds the three fake sessions; per-test hooks apply to one driver."""

    def __init__(self, gms_mutate=None, rsih_mutate=None, host_mutate=None,
                 fail_gms_ops=(), fail_rsih_ops=(), fail_host_ops=(),
                 write_fixture=None, protocol_error=None):
        self.gms_mutate = gms_mutate
        self.rsih_mutate = rsih_mutate
        self.host_mutate = host_mutate
        self.fail_gms_ops = fail_gms_ops
        self.fail_rsih_ops = fail_rsih_ops
        self.fail_host_ops = fail_host_ops
        self.write_fixture = write_fixture
        self.protocol_error = protocol_error
        self.sessions = {}

    def __call__(self, name, tools, fixtures, repo_dirs):
        kwargs = {}
        if name == "gms":
            kwargs["mutate"] = self.gms_mutate
            kwargs["fail_ops"] = self.fail_gms_ops
        elif name == "rsih":
            kwargs["mutate"] = self.rsih_mutate
            kwargs["fail_ops"] = self.fail_rsih_ops
        else:
            kwargs["mutate"] = self.host_mutate
            kwargs["fail_ops"] = self.fail_host_ops
        if self.write_fixture is not None and (
                self.write_fixture[2] == name or self.write_fixture[2] == "any"):
            kwargs["write_fixture"] = (self.write_fixture[0],
                                       self.write_fixture[1])
        if self.protocol_error is not None and (
                self.protocol_error == name or self.protocol_error == "any"):
            kwargs["protocol_error"] = True
        driver = FakeDriver(name, fixtures, **kwargs)
        self.sessions[name] = driver
        return driver


def run_with(kit, evidence_out=None):
    return run_s5_s9.run_tracer(
        repo_dirs=dict(REPO_DIRS), fixtures=FIXTURES,
        evidence_out=evidence_out, drivers=kit, toolchain=dict(TOOLCHAIN))


class TracerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="int003-test-")
        self.evidence = Path(self.tmp.name) / "evidence"
        self.addCleanup(self.tmp.cleanup)

    def run_green(self, kit=None):
        return run_with(kit or DriverKit(), evidence_out=self.evidence)


# ---------------------------------------------------------------------------
# TDD Red test (INT-003): partial release, projection gap, stale closure,
# partial publish must all turn the tracer red.
# ---------------------------------------------------------------------------

class TestRedPartialReleaseGapStaleClosurePartialPublish(TracerTestCase):

    def test_tracer_rejects_partial_release_projection_gap_stale_closure_and_partial_publish(self):
        """The named Red test: all four violations in one run -> RED."""
        seen = {"materialize": 0}

        def gms_mutate(op, params, response):
            if op == "release.activate" and params.get("mode") == "stale_cas":
                # a partial release survived: residue moved despite the
                # ACTIVE_HEAD_CONFLICT refusal
                response["residue_unchanged"] = False
                response["residue"] = {
                    "activation_entries": 1, "lineage_head_unchanged": False,
                    "sequence_head_unchanged": False, "outbox_pending": 1,
                    "proposal_states": "released",
                }
            if op == "projector.project" and params.get("mode") == "gap":
                # the gap advanced the watermark anyway (blocked state lied)
                response["state"] = "current"
                response["blocked_code"] = ""
                response["watermark_unchanged"] = False
            return response

        def rsih_mutate(op, params, response):
            if op == "closure.bridge" or op == "materialize.run":
                pass
            if op == "materialize.run":
                seen["materialize"] += 1
            if op == "lock.freeze" and params.get("mode") == "partial_publish":
                # partial publish accepted: lock frozen over an incomplete
                # bundle with the manifest digest guessed anyway
                response["ok"] = True
            return response

        def host_mutate(op, params, response):
            return response

        # Stale closure: the closure read that fed S8 came from a
        # non-authoritative (Graph/latest) root instead of the active head.
        def gms_closure_mutate(op, params, response):
            if op == "closure.read" and params.get("mode") == "graph_root":
                response["read_failed"] = False
                response["node_lineages"] = [
                    "comp-review-orchestrator", "proc-diff-summarizer",
                    "sg-commit-checklist"]
            return response

        def gms_mutate_all(op, params, response):
            response = gms_mutate(op, params, response)
            response = gms_closure_mutate(op, params, response)
            return response

        result = run_with(DriverKit(gms_mutate=gms_mutate_all,
                                    rsih_mutate=rsih_mutate,
                                    host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"], "tracer must be red: %s"
                         % result["failures"])
        joined = "\n".join(result["failures"])
        self.assertIn("partial release", joined)
        self.assertIn("gap", joined.lower())
        self.assertIn("stale closure", joined)
        self.assertIn("partial publish", joined)


# ---------------------------------------------------------------------------
# Green path: the whole S5-S9 chain over canned (fixture-derived) fakes
# ---------------------------------------------------------------------------

class TestGreenChain(TracerTestCase):

    def test_full_chain_green_and_digest_chain_complete(self):
        result = self.run_green()
        self.assertEqual(result["failures"], [])
        self.assertTrue(result["ok"])
        summary = result["summary"]
        self.assertEqual(summary["schema_version"],
                         run_s5_s9.SUMMARY_SCHEMA_VERSION)
        chain = summary["digest_chain"]
        for stage in run_s5_s9.CHAIN_STAGES:
            self.assertIn(stage, chain, "digest chain missing %s" % stage)
        self.assertEqual(summary["slices"],
                         {"s5": True, "s6": True, "s7": True, "s8": True,
                          "s9": True})
        self.assertTrue(summary["fixtures_immutable"])
        for stage, entry in chain.items():
            if isinstance(entry, dict):
                for key, value in entry.items():
                    if "digest" in key and value:
                        self.assertRegex(value, r"^sha256:[0-9a-f]{64}$",
                                         "%s.%s" % (stage, key))

    def test_evidence_dir_written_outside_fixtures(self):
        result = self.run_green()
        self.assertTrue(result["evidence_dir"])
        evidence = Path(result["evidence_dir"])
        self.assertTrue(evidence.is_dir())
        names = sorted(p.name for p in evidence.iterdir())
        for expected in ("s5_release.json", "s6_projection.json",
                         "s7_explore.json", "s8_materialization.json",
                         "s9_composite.json", "negatives.json",
                         "digest_chain.json", "summary.json"):
            self.assertIn(expected, names)
        # the per-slice collectors are referenced by the master manifest
        manifest = load_json(evidence / "manifest.json")
        for name in ("s5_release", "s6_projection", "s7_explore",
                     "s8_materialization", "s9_composite"):
            self.assertIn(name + ".json", manifest["slices"])


# ---------------------------------------------------------------------------
# Fail-closed negative matrix
# ---------------------------------------------------------------------------

class TestNegativeMatrix(TracerTestCase):

    def test_partial_release_residue_is_red(self):
        def gms_mutate(op, params, response):
            if op == "release.activate" and params.get("mode") == "stale_cas":
                response["residue_unchanged"] = False
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("partial release" in f
                            for f in result["failures"]), result["failures"])

    def test_accepted_release_without_atomic_residue_is_red(self):
        def gms_mutate(op, params, response):
            if op == "release.activate" and params.get("mode") == "ok":
                response["residue_atomic"] = False
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("atomic" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_not_accepted_release_activating_is_red(self):
        def gms_mutate(op, params, response):
            if op == "release.activate" and params.get("mode") == "not_accepted":
                response["activated"] = True
                response["reason_code"] = ""
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("not accepted" in f.lower() or "RELEASE_NOT_ACCEPTED" in f
                            for f in result["failures"]), result["failures"])

    def test_projection_gap_advancing_watermark_is_red(self):
        def gms_mutate(op, params, response):
            if op == "projector.project" and params.get("mode") == "gap":
                response["state"] = "current"
                response["watermark_unchanged"] = False
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("gap" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_projection_duplicate_advancing_is_red(self):
        def gms_mutate(op, params, response):
            if op == "projector.project" and params.get("mode") == "duplicate":
                response["duplicate_noop"] = False
                response["projected"] = 99
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("duplicate" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_explore_behind_watermark_serving_is_red(self):
        def gms_mutate(op, params, response):
            if op == "tools.invoke" and params.get("mode") == "behind":
                response["status"] = "succeeded"
                response["reason_code"] = ""
                response["failed_closed"] = False
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("behind" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_host_rewriting_explore_result_is_red(self):
        def host_mutate(op, params, response):
            if op == "toolproxy.explore" and params.get("mode") == "rewrite":
                response["status"] = "succeeded"
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("Pi" in f or "rewrite" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_graph_form_closure_root_accepted_is_red(self):
        def gms_mutate(op, params, response):
            if op == "closure.read" and params.get("mode") == "graph_root":
                response["read_failed"] = False
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("Graph" in f or "graph" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_torn_closure_guessed_is_red(self):
        def gms_mutate(op, params, response):
            if op == "closure.read" and params.get("mode") == "torn":
                response["read_failed"] = False
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("torn" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_bundle_digest_as_manifest_ref_is_red(self):
        def rsih_mutate(op, params, response):
            if op == "lock.freeze" and params.get("mode") == "bundle_ref":
                response["ok"] = True
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("bundle digest" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_partial_publish_accepted_is_red(self):
        def rsih_mutate(op, params, response):
            if op == "lock.freeze" and params.get("mode") == "partial_publish":
                response["ok"] = True
                response["bundle_files_published"] = 3
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("partial publish" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_stale_freeze_sequence_accepted_is_red(self):
        def rsih_mutate(op, params, response):
            if op == "lock.freeze" and params.get("mode") == "stale_sequence":
                response["ok"] = True
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("stale" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_composite_dynamic_child_accepted_is_red(self):
        def rsih_mutate(op, params, response):
            if op == "composite.run" and params.get("mode") == "dynamic_child":
                response["status"] = "succeeded"
                response["reason_code"] = ""
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("dynamic" in f.lower() or "non-exact" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_composite_cycle_accepted_is_red(self):
        def rsih_mutate(op, params, response):
            if op == "composite.run" and params.get("mode") == "cycle":
                response["status"] = "succeeded"
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("cycle" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_composite_drift_accepted_is_red(self):
        def rsih_mutate(op, params, response):
            if op == "composite.run" and params.get("mode") == "drift":
                response["status"] = "succeeded"
                response["whole_replay_passed"] = True
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("nondeterministic" in f.lower() or "drift" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_scoring_semantics_leak_into_gms_release_output(self):
        def gms_mutate(op, params, response):
            if op == "release.activate":
                response["utility_vector"] = [1, 2, 3]
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("scoring" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_scoring_semantics_leak_into_rsih_composite_output(self):
        def rsih_mutate(op, params, response):
            if op == "composite.run":
                response["u1_decision"] = "winner"
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("scoring" in f.lower()
                            for f in result["failures"]), result["failures"])


# ---------------------------------------------------------------------------
# Driver protocol failures
# ---------------------------------------------------------------------------

class TestDriverProtocol(TracerTestCase):

    def test_driver_error_response_turns_tracer_red_with_code(self):
        result = run_with(DriverKit(protocol_error="gms"),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("DRIVER_PROTOCOL_VIOLATION" in f
                            for f in result["failures"]), result["failures"])

    def test_driver_op_failure_reported(self):
        result = run_with(DriverKit(fail_rsih_ops=("materialize.run",)),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("DRIVER_OP_FAILED" in f
                            for f in result["failures"]), result["failures"])

    def test_unknown_op_is_red(self):
        class UnknownOpDriver(FakeDriver):
            def _good(self, op, params):
                if op == "no.such.op":
                    return {}
                return super()._good(op, params)

        class _Kit:
            def __init__(self):
                self.sessions = {}

            def __call__(self, name, tools, fixtures, repo_dirs):
                driver = UnknownOpDriver(name, fixtures)
                self.sessions[name] = driver
                return driver

        result = run_s5_s9.run_tracer(
            repo_dirs=dict(REPO_DIRS), fixtures=FIXTURES,
            evidence_out=self.evidence, drivers=_Kit(),
            toolchain=dict(TOOLCHAIN))
        self.assertFalse(result["ok"])

    def test_fixtures_mutation_is_red(self):
        mutated = Path(self.tmp.name) / "fixtures-copy"
        shutil.copytree(FIXTURES, mutated,
                        ignore=shutil.ignore_patterns("__pycache__"))
        target = mutated / "materialization" / "manifest.json"
        kit = DriverKit(write_fixture=("closure.read", target, "gms"))
        result = run_s5_s9.run_tracer(
            repo_dirs=dict(REPO_DIRS), fixtures=mutated,
            evidence_out=self.evidence, drivers=kit,
            toolchain=dict(TOOLCHAIN))
        self.assertFalse(result["ok"])
        self.assertTrue(any("mutated" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_evidence_dir_inside_fixtures_refused(self):
        inside = Path(FIXTURES) / "int003-inside-evidence"
        result = run_with(DriverKit(), evidence_out=inside)
        self.assertFalse(result["ok"])
        self.assertTrue(any("inside the fixtures tree" in f
                            for f in result["failures"]), result["failures"])
        self.assertFalse(inside.exists())


# ---------------------------------------------------------------------------
# Optional real three-repo run
# ---------------------------------------------------------------------------

@unittest.skipUnless(os.environ.get("RSIH_INT003_FULL"),
                     "set RSIH_INT003_FULL=1 to run the real three-repo tracer")
class TestRealThreeRepoRun(TracerTestCase):
    ROOT = Path(__file__).resolve().parents[6]

    def test_real_run_green(self):
        repo_dirs = {
            "host": str(self.ROOT / "memory_graph_evolving" / "pi-group-chat-host"),
            "gms": str(self.ROOT / "memory_graph_evolving" / "graph-memory-service"),
            "rsih": str(self.ROOT / "RSI-Harness"),
        }
        result = run_s5_s9.run_tracer(
            repo_dirs=repo_dirs, fixtures=FIXTURES,
            evidence_out=self.evidence, toolchain=None)
        self.assertEqual(result["failures"], [])
        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
