"""INT-002 S2-S4 Host/GMS/RSIH tracer tests.

Stdlib ``unittest`` only. The suite never spawns the real ``go``/``node``
drivers on the fast path: every test injects fake driver sessions that
materialise canned instruction responses derived from the frozen fixtures
tree. The real three-repo run is an opt-in test behind the
``RSIH_INT002_FULL`` environment variable.
"""

import base64
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
    "int002_run_s2_s4", INTEGRATION_ROOT / "run_s2_s4.py"
)
run_s2_s4 = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = run_s2_s4
_SPEC.loader.exec_module(run_s2_s4)

TOOLCHAIN = {"go": "go1.26.1", "node": "v22.22.2", "python": "3.12.3"}

FIXTURES = CONFORMANCE_ROOT

REPO_DIRS = {
    "host": "/nonexistent-host",
    "gms": "/nonexistent-gms",
    "rsih": "/nonexistent-rsih",
}

NONRESERVED_FAMILIES = (
    "baseline", "candidate", "overlap", "conflict",
    "late-output", "clock-exhausted", "extra-tool-call",
)

# The GMS-208 merge family is a real replay family now (the recorded
# manifest no longer reserves it); its FIRST packet is the family's
# replay representative, exactly like the RSIH runner resolves it.
REPLAY_FAMILIES = NONRESERVED_FAMILIES + ("merge",)


def sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def label_digest(label: str) -> str:
    return sha(("int002-fake:" + label).encode("utf-8"))


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

    Every digest that the tracer cross-checks against the fixtures is read
    from the fixtures themselves (read-only), so the fakes stay honest;
    driver-private digests are stable labelled fakes exactly like the real
    drivers' internal records.
    """

    def __init__(self, fixtures):
        self.fixtures = Path(fixtures)
        self.tools_manifest = load_json(self.fixtures / "tools" / "manifest.json")
        self.recorded_manifest = load_json(
            self.fixtures / "recorded" / "manifest.json")
        self._tool_expectations = {}
        for case in self.tools_manifest["cases"]:
            base = self.fixtures / "tools"
            expected = load_json(base / case["expected_path"])
            self._tool_expectations[case["case_id"]] = expected
        self._segment_cases = {}
        for case in self.recorded_manifest["cases"]:
            if case["corpus"] != "segments":
                continue
            case_dir = self.fixtures / "recorded" / case["path"]
            segment = load_json(case_dir / "segment.json")
            seal = load_json(case_dir / "seal.json")
            canonical = (case_dir / "canonical.utf8").read_bytes()
            self._segment_cases[case["case_id"]] = {
                "manifest": case, "segment": segment, "seal": seal,
                "canonical": canonical,
            }
        self._packets = {}
        for case in self.recorded_manifest["cases"]:
            if case["corpus"] != "q29b" or case["expected_outcome"] == "reserved":
                continue
            if case["family"] in self._packets:
                # first packet per family (the merge pair's baseline side is
                # the family's replay representative)
                continue
            case_dir = self.fixtures / "recorded" / case["path"]
            runtime = load_json(case_dir / "runtime.json")
            expected_bytes = (case_dir / "expected-output.canonical").read_bytes()
            self._packets[case["family"]] = {
                "case": case, "runtime": runtime,
                "expected_bytes": expected_bytes,
                "expected_digest": runtime["expected"]["output_digest"],
                "expected_status": runtime["terminal"]["status"],
                "expected_reason": runtime["terminal"]["reason_code"],
            }

    # -- S2 ---------------------------------------------------------------

    def toolproxy_invoke(self, params):
        case_id = params["case_id"]
        expected = self._tool_expectations[case_id]
        canonical = ('{"int002-fake-toolproxy":"' + case_id + '"}').encode("utf-8")
        accept = bool(expected["expected_accept"])
        if params.get("mode") == "late_rewrite":
            first_digest = label_digest("terminal-timeout-" + case_id)
            return {
                "case_id": case_id, "mode": "late_rewrite",
                "first_status": "failed",
                "first_reason_code": "HOST_PROXY_TIMEOUT",
                "first_digest": first_digest,
                "late_entries": 1,
                "terminal_status_after_late": "failed",
                "terminal_digest_after_late": first_digest,
                "replay_status": "failed",
                "replay_reason_code": "HOST_PROXY_TIMEOUT",
                "replay_digest": first_digest,
                "replay_canonical_equal": True,
                "pi_deliveries": 1,
                "upstream_calls_after_replay": 1,
                "audit_kinds": ["request", "terminal", "late_upstream_audit",
                                "request", "terminal"],
                "scoring_fields_present": False,
            }
        response = {
            "case_id": case_id, "mode": "ok",
            "expected_accept": accept,
            "status": "succeeded" if accept else "failed",
            "reason_code": "" if accept else "HOST_UPSTREAM_INVALID",
            "canonical_b64": base64.b64encode(canonical).decode("ascii"),
            "canonical_len": len(canonical),
            "proxy_result_digest": sha(canonical),
            "upstream_result_digest": expected["expected_result_digest"],
            "result_payload_digest": expected["expected_result_digest"],
            "pi_deliveries": 1 if accept else 1,
            "pi_bytes_equal_result": True,
            "pi_digest": sha(canonical),
            "delivered_before_end": True,
            "upstream_calls": 1,
            "audit_kinds": ["request", "terminal", "pi_return_delivery"],
            "late_entries": 0,
            "scoring_fields_present": False,
        }
        if not accept:
            response["upstream_result_digest"] = None
            response["result_payload_digest"] = None
            response["pi_digest"] = sha(canonical)
        return response

    # -- S3 ---------------------------------------------------------------

    def segment_close(self, params):
        case_id = params["case_id"]
        data = self._segment_cases[case_id]
        segment = data["segment"]
        seal = data["seal"]
        canonical = data["canonical"]
        manifest = data["manifest"]
        reject_reason = manifest.get("expected_reason_code")
        failed_family = segment["terminal_state"] in ("failed", "aborted")
        negative = reject_reason is not None
        refs = None
        terminal_audit = segment.get("terminal_audit")
        if negative:
            terminal_state = "failed"
            terminal_audit = {"reason_code": reject_reason,
                              "phase": "close_integrity_check",
                              "detail": "negative corpus case"}
        else:
            terminal_state = segment["terminal_state"]
        zero_refs = failed_family or negative
        if not zero_refs:
            seg_ref = seal["segment_ref"]
            refs = {
                "segment_ref": {
                    "room_id": seg_ref["room_id"],
                    "segment_id": seg_ref["segment_id"],
                    "segment_version": seg_ref["segment_version"],
                    "segment_digest": seg_ref["segment_digest"],
                    "evidence_seal_ref": seg_ref["evidence_seal_ref"],
                    "path_seal_ref": seg_ref["path_seal_ref"],
                },
                "checkpoint_ref": {
                    "checkpoint_id": seal["checkpoint_ref"]["checkpoint_id"],
                    "checkpoint_sequence":
                        seal["checkpoint_ref"]["checkpoint_sequence"],
                    "checkpoint_digest":
                        seal["checkpoint_ref"]["checkpoint_digest"],
                },
                "evidence_seal": {
                    "seal_id": seal["evidence_seal"]["seal_id"],
                    "seal_version": seal["evidence_seal"]["seal_version"],
                    "seal_digest": seal["evidence_seal"]["seal_digest"],
                    "evidence_ref": {
                        "evidence_id":
                            seal["evidence_seal"]["evidence_ref"]["evidence_id"],
                        "version":
                            seal["evidence_seal"]["evidence_ref"]["version"],
                        "evidence_digest":
                            seal["evidence_seal"]["evidence_ref"]["evidence_digest"],
                        "commit_state": "uncommitted",
                        "evidence_kind":
                            seal["evidence_seal"]["evidence_ref"]["evidence_kind"],
                    },
                },
                "path_seal": {
                    "seal_id": seal["path_seal"]["seal_id"],
                    "seal_digest": seal["path_seal"]["seal_digest"],
                    "path_ids": seal["path_seal"]["path_ids"],
                },
            }
        probes = {}
        for name in params.get("probes") or ():
            if name == "append_after_settled":
                probes[name] = {"rejected": True,
                                "code": "APPEND_AFTER_SETTLED"}
            elif name == "idempotency_conflict":
                probes[name] = {"rejected": True,
                                "code": "IDEMPOTENCY_CONFLICT"}
            elif name == "seal_mismatch":
                probes[name] = {"rejected": True,
                                "code": "SEAL_DIGEST_MISMATCH"}
            else:
                probes[name] = {"rejected": False, "code": ""}
        return {
            "case_id": case_id,
            "terminal_state": terminal_state,
            "segment_digest": seal["segment_digest"] if not zero_refs else (
                label_digest("failed-" + case_id)),
            "canonical_b64": base64.b64encode(
                canonical if not zero_refs else
                ('{"failed":"' + case_id + '"}').encode("utf-8")
            ).decode("ascii"),
            "canonical_len": len(canonical) if not zero_refs else 0,
            "seal_readback_digest": seal["segment_digest"]
                if not zero_refs else None,
            "zero_refs": zero_refs,
            "refs": refs,
            "paths": [{"path_id": p["path_id"], "domain": p["domain"]}
                      for p in segment["path_seal_body"]["paths"]]
            if not zero_refs else [],
            "terminal_audit": terminal_audit,
            "probes": probes,
            "scoring_fields_present": False,
        }

    def evidence_admit(self, params):
        case_id = params["case_id"]
        mode = params.get("mode", "commit")
        data = self._segment_cases[case_id]
        seal = data["seal"]
        committed = None
        valid = True
        status = "validating"
        reason = ""
        if mode == "commit":
            committed = {
                "evidence_id": seal["evidence_seal"]["evidence_ref"]["evidence_id"],
                "version": str(seal["evidence_seal"]["evidence_ref"]["version"]),
                "evidence_digest": seal["evidence_seal"]["seal_digest"],
                "commit_state": "committed",
                "evidence_kind":
                    seal["evidence_seal"]["evidence_ref"]["evidence_kind"],
                "segment_digest": seal["segment_ref"]["segment_digest"],
                "checkpoint_digest":
                    seal["checkpoint_ref"]["checkpoint_digest"],
                "path_seal_digest": seal["path_seal"]["seal_digest"],
                "path_ids": seal["path_seal"]["path_ids"],
                "commit_sequence": 1,
            }
        elif mode == "stage_only":
            status = "validating"
        elif mode == "non_settled":
            valid = False
            status = "rejected"
            reason = "SEGMENT_NOT_SETTLED"
        elif mode == "tamper":
            valid = False
            status = "rejected"
            reason = "SEAL_DIGEST_MISMATCH"
        response = {
            "case_id": case_id, "mode": mode,
            "staged": True, "valid": valid, "status": status,
            "reason_code": reason,
            "visible_before_commit": False,
            "committed": committed,
            "commit_rejected": mode in ("non_settled", "tamper"),
            "commit_code": {
                "non_settled": "ILLEGAL_STATE_TRANSITION",
                "tamper": "SEGMENT_NOT_SETTLED",
            }.get(mode, ""),
            "lookup_after_stage": ({"found": False}
                                   if mode == "stage_only" else None),
            "ledger_size": 1 if mode == "commit" else 0,
            "chain_segment_digest_match": True,
            "chain_evidence_digest_match": True,
            "scoring_fields_present": False,
        }
        return response

    def candidate_bind(self, params):
        mode = params.get("mode", "ok")
        artifact = self._packets["candidate"]["runtime"]["sealed_inputs"]["artifact_ref"]
        if mode == "uncommitted":
            return {
                "mode": mode, "bound": False, "candidate_ref": None,
                "bind_code": "EVIDENCE_NOT_COMMITTED",
                "chain_evidence_digest_match": True,
                "runtime_input_refused": True,
                "runtime_input_code": "CANDIDATE_NOT_EXECUTABLE",
                "ledger_candidate_entries": 0,
                "scoring_fields_present": False,
            }
        return {
            "mode": mode, "bound": True,
            "candidate_ref": artifact,
            "chain_evidence_digest_match": True,
            "runtime_input_refused": True,
            "runtime_input_code": "CANDIDATE_NOT_EXECUTABLE",
            "ledger_candidate_entries": 1,
            "scoring_fields_present": False,
        }

    # -- S4 ---------------------------------------------------------------

    def build_request(self, params):
        candidate = self._packets["candidate"]["runtime"]["sealed_inputs"]["artifact_ref"]
        baseline = self._packets["baseline"]["runtime"]["sealed_inputs"]["artifact_ref"]
        conflict = self._packets["conflict"]["runtime"]["sealed_inputs"]["artifact_ref"]
        run_block = self._packets["baseline"]["runtime"]["run"]
        segment_refs = []
        seen = set()
        for family in REPLAY_FAMILIES:
            for seg in self._packets[family]["runtime"]["sealed_inputs"]["segment_refs"]:
                if seg["segment_id"] not in seen:
                    seen.add(seg["segment_id"])
                    segment_refs.append(seg)
        manifest_bytes = (self.fixtures / "recorded" / "manifest.json").read_bytes()
        request_doc = {
            "schema_version": "gms.replay-request.v1",
            "replay_request_id": "replay-int002-0001",
            "candidate_ref": candidate,
            "baseline_skill_refs": [baseline, conflict],
            "fixture_set_refs": [
                {"id": "fixture-q29b", "version": 1, "digest": sha(manifest_bytes)}],
            "segment_refs": segment_refs,
            "replay_profile_ref": run_block["profile_ref"],
            "runtime_adapter_ref": run_block["adapter_ref"],
            "mode": "causal_evaluation",
            "idempotency_key": label_digest("idempotency-int002-0001"),
        }
        domain_for = {
            "baseline": "source_a", "candidate": "source_a",
            "overlap": "overlap", "conflict": "source_b",
            "late-output": "source_a", "clock-exhausted": "source_a",
            "extra-tool-call": "source_a", "merge": "overlap",
        }
        baseline_for = {
            "conflict": conflict,
        }
        families = []
        for family in REPLAY_FAMILIES:
            packet = self._packets[family]
            runtime = packet["runtime"]
            families.append({
                "name": family,
                "domain": domain_for[family],
                "sides": [runtime["run"]["side"]],
                "baseline": baseline_for.get(family, baseline),
                "packets": [{
                    "id": runtime["packet_id"],
                    "side": runtime["run"]["side"],
                    "expected_output_digest": packet["expected_digest"],
                    "expected_run_status": packet["expected_status"],
                    "expected_reason_code": packet["expected_reason"] or "",
                    "artifact_ref": runtime["sealed_inputs"]["artifact_ref"],
                    "path_domains": [p["domain"] for p in
                                     runtime["sealed_inputs"]["path_refs"]],
                }],
            })
        return {
            "request_doc": request_doc,
            "request_digest": sha(jcs(request_doc).encode("utf-8")),
            "fixture_manifest_digest": sha(manifest_bytes),
            "segment_chain_match": True,
            "evidence_chain_match": True,
            "candidate_chain_match": True,
            "families": families,
            "reserved_families": [],
            "scoring_fields_present": False,
        }

    def canonicalize(self, params):
        mode = params.get("mode", "ok")
        if mode == "missing_family":
            return {"mode": mode, "rejected": True,
                    "reason_code": "MISSING_FAMILY"}
        if mode == "cross_run":
            return {"mode": mode, "rejected": True,
                    "reason_code": "REPLAY_REQUEST_INVALID"}
        if mode == "nondeterministic":
            return {"mode": mode, "rejected": False,
                    "status": "inconclusive",
                    "failure_reason_code": "REPLAY_NONDETERMINISTIC",
                    "result_digest": label_digest("inconclusive"),
                    "rerun_digest_equal": False,
                    "outcome_families": [], "outcome_count": 0,
                    "records": []}
        request = params["request_doc"]
        runs = params["runs"]
        result_digest = sha(jcs({
            "request": request["replay_request_id"],
            "digests": [r["output_digest"] for r in runs],
        }).encode("utf-8"))
        records = []
        for run in runs:
            records.append({
                "packet_id": run["packet_id"],
                "category": "semantic_pass" if run["run_status"] == "succeeded"
                           else "semantic_fail",
                "run_status": run["run_status"],
                "reason_code": run["reason_code"],
                "output_digest": run["output_digest"],
            })
        return {
            "mode": "ok", "rejected": False,
            "status": "succeeded", "failure_reason_code": "",
            "result_digest": result_digest,
            "rerun_digest_equal": True,
            "outcome_families": [f["name"] for f in
                                 self.build_request({})["families"]],
            "outcome_count": len(NONRESERVED_FAMILIES),
            "records": records,
            "utility": {"task_success_count": 4,
                        "critical_branch_pass_count": 4,
                        "recovery_success_count": 1,
                        "inconclusive_case_count": 0,
                        "execution_cost_units": 14},
            "scoring_fields_present": False,
        }

    # -- RSIH --------------------------------------------------------------

    def rsih_run(self, params):
        family = params["family"]
        packet = self._packets[family]
        runtime = packet["runtime"]
        terminal = runtime["terminal"]
        attempts = [{
            "attempt_index": 1,
            "outcome": "succeeded" if terminal["status"] == "succeeded"
                       else "failed",
            "reason_code": terminal["reason_code"],
            "steps_executed": terminal["completed_step"],
        }]
        if family == "late-output":
            attempts.append(dict(attempts[0], attempt_index=2))
        return {
            "family": family,
            "packet_id": runtime["packet_id"],
            "side": runtime["run"]["side"],
            "mode": runtime["run"]["mode"],
            "status": terminal["status"],
            "reason_code": terminal["reason_code"] or "",
            "output_digest": packet["expected_digest"],
            "canonical_b64": base64.b64encode(
                packet["expected_bytes"]).decode("ascii"),
            "expected_digest": packet["expected_digest"],
            "digest_matches_expected": True,
            "attempts": attempts,
            "late_audit": [{"arrival_step": e["arrival_step"],
                            "recorded_as": e["recorded_as"]}
                           for e in runtime.get("late_events", [])],
            "repeat_pair_digest_equal": True,
            "repeat_run_digest": packet["expected_digest"],
            "scoring_fields_present": False,
            "raw_output_keys": ["schema_version", "packet_id", "run",
                                "sealed_input_digest", "environment_digest",
                                "steps_executed", "capability_usage",
                                "consumed", "late_audit", "terminal"],
        }

    def rsih_list(self, params):
        cases = []
        for family in REPLAY_FAMILIES:
            packet = self._packets[family]
            cases.append({
                "case_id": packet["case"]["case_id"],
                "family": family,
                "packet_id": packet["runtime"]["packet_id"],
                "side": packet["runtime"]["run"]["side"],
                "expected_digest": packet["expected_digest"],
                "expected_status": packet["expected_status"],
            })
        return {"cases": cases, "reserved": ["merge"]}

    def host_schedule(self, params):
        mode = params.get("mode", "ok")
        if mode == "missing_family":
            return {"mode": mode, "accepted": False,
                    "reason_code": "MISSING_FAMILY", "detail": "",
                    "outputs": [], "probes": []}
        if mode == "cross_run":
            return {"mode": mode, "accepted": True, "reason_code": "",
                    "second_accepted": False,
                    "second_reason_code": "IDEMPOTENCY_CONFLICT",
                    "outputs": [], "probes": []}
        outputs = []
        probes = []
        for family in REPLAY_FAMILIES:
            packet = self._packets[family]
            native = packet["runtime"]["run"]["side"]
            other = "candidate" if native == "baseline" else "baseline"
            for side in (native, other):
                outputs.append({
                    "family": family, "side": side,
                    "run_id": packet["runtime"]["run"]["run_id"],
                    "attempts": 2 if family == "late-output" else 1,
                    "terminal_status": packet["expected_status"],
                    "reason_code": packet["expected_reason"] or "",
                    "output_digest": packet["expected_digest"],
                    "late_audit": [
                        {"recorded_as": e["recorded_as"]}
                        for e in packet["runtime"].get("late_events", [])],
                })
                probes.append({
                    "family": family, "side": side,
                    "recorded_digest": packet["expected_digest"],
                    "probed_digest": packet["expected_digest"],
                    "matched": True,
                })
        return {
            "mode": "ok", "accepted": True, "reason_code": "", "detail": "",
            "plan_digest": label_digest("plan-int002"),
            "family_order": list(NONRESERVED_FAMILIES) + ["merge"],
            "outputs": outputs, "probes": probes,
            "runner_kind": "rsih-subprocess",
            "rsih_calls": len(outputs) + len(probes),
            "idempotent_replay": False,
            "scoring_fields_present": False,
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
        response["ok"] = True
        response["op"] = op
        return response

    def _good(self, op, params):
        if self.name == "host":
            if op == "toolproxy.invoke":
                return self.good.toolproxy_invoke(params)
            if op == "segment.close":
                return self.good.segment_close(params)
            if op == "replay.schedule":
                return self.good.host_schedule(params)
        elif self.name == "gms":
            if op == "evidence.admit":
                return self.good.evidence_admit(params)
            if op == "candidate.bind":
                return self.good.candidate_bind(params)
            if op == "replay.build_request":
                return self.good.build_request(params)
            if op == "replay.canonicalize":
                return self.good.canonicalize(params)
        elif self.name == "rsih":
            if op == "runner.run":
                return self.good.rsih_run(params)
            if op == "corpus.list":
                return self.good.rsih_list(params)
        raise AssertionError("unexpected driver op %r for %s" % (op, self.name))

    def close(self):
        return None


class DriverKit:
    """Builds the three fake sessions; per-test hooks apply to one driver."""

    def __init__(self, host_mutate=None, gms_mutate=None, rsih_mutate=None,
                 fail_host_ops=(), write_fixture=None, protocol_error=None):
        self.host_mutate = host_mutate
        self.gms_mutate = gms_mutate
        self.rsih_mutate = rsih_mutate
        self.fail_host_ops = fail_host_ops
        self.write_fixture = write_fixture
        self.protocol_error = protocol_error
        self.sessions = {}

    def __call__(self, name, tools, fixtures, repo_dirs):
        kwargs = {}
        if name == "host":
            kwargs["mutate"] = self.host_mutate
            kwargs["fail_ops"] = self.fail_host_ops
        elif name == "gms":
            kwargs["mutate"] = self.gms_mutate
        else:
            kwargs["mutate"] = self.rsih_mutate
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
    return run_s2_s4.run_tracer(
        repo_dirs=dict(REPO_DIRS), fixtures=FIXTURES,
        evidence_out=evidence_out, drivers=kit, toolchain=dict(TOOLCHAIN))


class TracerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="int002-test-")
        self.evidence = Path(self.tmp.name) / "evidence"
        self.addCleanup(self.tmp.cleanup)

    def run_green(self, kit=None):
        return run_with(kit or DriverKit(), evidence_out=self.evidence)


# ---------------------------------------------------------------------------
# TDD Red test (INT-002): post-end result, unsealed evidence,
# nondeterministic replay must all turn the tracer red.
# ---------------------------------------------------------------------------

class TestRedPostEndUnsealedEvidenceNondeterministicReplay(TracerTestCase):

    def test_tracer_rejects_post_end_result_unsealed_evidence_and_nondeterministic_replay(self):
        """The named Red test: all three violations in one run -> RED."""
        seen_rounds = {"rsih": 0}

        def host_mutate(op, params, response):
            if op == "toolproxy.invoke" and params.get("mode") == "late_rewrite":
                # post-end upstream result rewrote the terminal and reached
                # Pi a second time
                response["terminal_digest_after_late"] = label_digest(
                    "rewritten-by-late-result")
                response["replay_digest"] = label_digest("rewritten-by-late-result")
                response["replay_canonical_equal"] = False
                response["pi_deliveries"] = 2
            return response

        def gms_mutate(op, params, response):
            if op == "evidence.admit" and params.get("mode") == "non_settled":
                # unsealed (failed-segment) evidence committed anyway
                response["valid"] = True
                response["status"] = "validating"
                response["reason_code"] = ""
                response["committed"] = {
                    "evidence_id": "evseal-seg-failed-0001",
                    "version": "1",
                    "evidence_digest": label_digest("unsealed"),
                    "commit_state": "committed",
                    "evidence_kind": "failure_path",
                    "segment_digest": label_digest("unsealed-segment"),
                    "checkpoint_digest": label_digest("unsealed-ckpt"),
                    "path_seal_digest": label_digest("unsealed-path"),
                    "path_ids": [], "commit_sequence": 3,
                }
                response["commit_rejected"] = False
                response["commit_code"] = ""
                response["ledger_size"] = 3
            if op == "replay.canonicalize" and params.get("mode") == "ok":
                seen_rounds["gms"] = seen_rounds.get("gms", 0) + 1
                if seen_rounds["gms"] >= 2:
                    # nondeterministic replay: the same inputs canonicalize
                    # to a different digest on the second round
                    response["result_digest"] = label_digest("drifted")
                    response["rerun_digest_equal"] = False
            return response

        def rsih_mutate(op, params, response):
            if op == "runner.run":
                seen_rounds["rsih"] += 1
                if seen_rounds["rsih"] > 7:
                    response["output_digest"] = label_digest("nondeterministic")
                    response["digest_matches_expected"] = False
                    response["repeat_pair_digest_equal"] = False
            return response

        result = run_with(DriverKit(host_mutate=host_mutate,
                                    gms_mutate=gms_mutate,
                                    rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"], "tracer must be red: %s"
                         % result["failures"])
        joined = "\n".join(result["failures"])
        self.assertIn("post-end", joined)
        self.assertIn("unsealed", joined)
        self.assertIn("nondeterministic", joined.lower())


# ---------------------------------------------------------------------------
# Green path: the whole S2-S4 chain over canned (fixture-derived) fakes
# ---------------------------------------------------------------------------

class TestGreenChain(TracerTestCase):

    def test_full_chain_green_and_digest_chain_complete(self):
        result = self.run_green()
        self.assertEqual(result["failures"], [])
        self.assertTrue(result["ok"])
        summary = result["summary"]
        self.assertEqual(summary["schema_version"],
                         run_s2_s4.SUMMARY_SCHEMA_VERSION)
        chain = summary["digest_chain"]
        for stage in ("s2_toolproxy", "s3_segment", "s3_evidence",
                      "s3_candidate", "s4_request", "s4_host_plan",
                      "s4_rsih_raw", "s4_gms_result"):
            self.assertIn(stage, chain, "digest chain missing %s" % stage)
        self.assertEqual(summary["slices"], {"s2": True, "s3": True, "s4": True})
        self.assertTrue(summary["fixtures_immutable"])
        self.assertEqual(summary["replay_invariance"]["later_transcript_ignored"],
                         True)
        # every digest in the chain is sha256-shaped
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
        self.assertNotIn(str(FIXTURES), str(evidence.resolve())[:len(str(FIXTURES))] +
                         "/x"[:0])  # outside the fixtures tree
        names = sorted(p.name for p in evidence.iterdir())
        for expected in ("s2_toolproxy.json", "s3_segments.json",
                         "s3_evidence.json", "s3_candidate.json",
                         "s4_request.json", "s4_rsih_runs.json",
                         "s4_host_schedule.json", "s4_gms_result.json",
                         "negatives.json", "digest_chain.json",
                         "summary.json"):
            self.assertIn(expected, names)


# ---------------------------------------------------------------------------
# Fail-closed negative matrix: every smuggled acceptance turns the tracer red
# ---------------------------------------------------------------------------

class TestNegativeMatrix(TracerTestCase):

    def test_pi_bytes_diverge_from_proxy_result(self):
        def host_mutate(op, params, response):
            if op == "toolproxy.invoke" and params.get("mode", "ok") == "ok":
                response["pi_bytes_equal_result"] = False
                response["pi_digest"] = label_digest("pi-bypass")
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("Pi" in f or "pi_bytes" in f
                            for f in result["failures"]), result["failures"])

    def test_placeholder_result_accepted(self):
        def host_mutate(op, params, response):
            if (op == "toolproxy.invoke" and params.get("mode", "ok") == "ok"
                    and not response["expected_accept"]):
                response["status"] = "succeeded"
                response["reason_code"] = ""
                response["upstream_result_digest"] = label_digest("placeholder")
                response["result_payload_digest"] = label_digest("placeholder")
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("placeholder" in f.lower() or "reject" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_result_after_tool_execution_end(self):
        def host_mutate(op, params, response):
            if op == "toolproxy.invoke" and params.get("mode", "ok") == "ok":
                response["delivered_before_end"] = False
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("tool_execution_end" in f or "before" in f
                            for f in result["failures"]), result["failures"])

    def test_mutable_seal_append_after_settled_accepted(self):
        def host_mutate(op, params, response):
            if op == "segment.close" and "append_after_settled" in (
                    params.get("probes") or ()):
                response["probes"]["append_after_settled"] = {
                    "rejected": False, "code": ""}
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("mutable seal" in f.lower() or
                            "append_after_settled" in f
                            for f in result["failures"]), result["failures"])

    def test_host_seal_digest_mismatch_vs_gms_evidence(self):
        def host_mutate(op, params, response):
            if op == "segment.close" and response.get("refs"):
                response["refs"]["evidence_seal"]["seal_digest"] = \
                    label_digest("drifted-host-seal")
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("seal digest" in f.lower() or "mismatch" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_uncommitted_evidence_leaks_into_reads(self):
        def gms_mutate(op, params, response):
            if op == "evidence.admit" and params.get("mode") == "stage_only":
                response["lookup_after_stage"] = {"found": True}
                response["visible_before_commit"] = True
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("uncommitted" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_candidate_live_leakage_into_runtime(self):
        def gms_mutate(op, params, response):
            if op == "candidate.bind" and params.get("mode", "ok") == "ok":
                response["runtime_input_refused"] = False
                response["runtime_input_code"] = ""
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("candidate" in f.lower() and
                            ("runtime" in f.lower() or "leak" in f.lower())
                            for f in result["failures"]), result["failures"])

    def test_missing_family_accepted_by_host_scheduler(self):
        def host_mutate(op, params, response):
            if op == "replay.schedule" and params.get("mode") == "missing_family":
                response["accepted"] = True
                response["reason_code"] = ""
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("missing family" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_missing_family_accepted_by_gms_canonicalizer(self):
        def gms_mutate(op, params, response):
            if op == "replay.canonicalize" and params.get("mode") == "missing_family":
                response["rejected"] = False
                response["status"] = "succeeded"
                response["outcome_count"] = 7
            return response
        result = run_with(DriverKit(gms_mutate=gms_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("missing family" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_cross_run_contamination_between_replay_rounds(self):
        def host_mutate(op, params, response):
            if op == "replay.schedule" and params.get("mode") == "cross_run":
                response["second_accepted"] = True
                response["second_reason_code"] = ""
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("cross-run" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_host_rsih_digest_mismatch_on_the_chain(self):
        def host_mutate(op, params, response):
            if op == "replay.schedule" and params.get("mode", "ok") == "ok":
                response["outputs"][0]["output_digest"] = \
                    label_digest("foreign-digest")
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("digest mismatch" in f.lower() or
                            "diverge" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_scoring_semantics_leak_into_host_outputs(self):
        def host_mutate(op, params, response):
            if op == "replay.schedule" and params.get("mode", "ok") == "ok":
                response["outputs"][0]["u1_release_recommendation"] = "accept"
            return response
        result = run_with(DriverKit(host_mutate=host_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("scoring" in f.lower() or "u1" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_scoring_semantics_leak_into_rsih_raw_output(self):
        def rsih_mutate(op, params, response):
            if op == "runner.run":
                response["utility_vector"] = {"task_success_count": 1}
                response["raw_output_keys"] = response["raw_output_keys"] + [
                    "utility_vector"]
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("scoring" in f.lower() or "u1" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_later_transcript_changes_replay_digest(self):
        state = {"round": 0}

        def rsih_mutate(op, params, response):
            if op == "runner.run":
                state["round"] += 1
                if state["round"] > 7:
                    response["output_digest"] = label_digest("post-transcript")
            return response
        result = run_with(DriverKit(rsih_mutate=rsih_mutate),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("transcript" in f.lower()
                            for f in result["failures"]), result["failures"])


# ---------------------------------------------------------------------------
# Driver protocol handling
# ---------------------------------------------------------------------------

class TestDriverProtocol(TracerTestCase):

    def test_driver_error_response_turns_tracer_red_with_code(self):
        result = run_with(DriverKit(protocol_error="gms"),
                          evidence_out=self.evidence)
        self.assertFalse(result["ok"])
        self.assertTrue(any("DRIVER_PROTOCOL_VIOLATION" in f
                            for f in result["failures"]), result["failures"])

    def test_driver_op_failure_reported(self):
        result = run_with(DriverKit(fail_host_ops=("segment.close",)),
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

        kit = DriverKit()

        def factory(name, tools, fixtures, repo_dirs):
            return UnknownOpDriver(name, fixtures)

        result = run_s2_s4.run_tracer(
            repo_dirs=dict(REPO_DIRS), fixtures=FIXTURES,
            evidence_out=self.evidence, drivers=None if False else _KitWrapper(factory),
            toolchain=dict(TOOLCHAIN))
        # an unknown op surfaces as a driver failure (fake asserts instead)
        self.assertFalse(result["ok"])

    def test_fixtures_mutation_is_red(self):
        mutated = Path(self.tmp.name) / "fixtures-copy"
        shutil.copytree(FIXTURES, mutated,
                        ignore=shutil.ignore_patterns("__pycache__"))
        target = mutated / "recorded" / "README.md"
        kit = DriverKit(write_fixture=("segment.close", target, "host"))
        result = run_s2_s4.run_tracer(
            repo_dirs=dict(REPO_DIRS), fixtures=mutated,
            evidence_out=self.evidence, drivers=kit,
            toolchain=dict(TOOLCHAIN))
        self.assertFalse(result["ok"])
        self.assertTrue(any("mutated" in f.lower()
                            for f in result["failures"]), result["failures"])

    def test_evidence_dir_inside_fixtures_refused(self):
        inside = Path(FIXTURES) / "int002-inside-evidence"
        result = run_with(DriverKit(), evidence_out=inside)
        self.assertFalse(result["ok"])
        self.assertTrue(any("inside the fixtures tree" in f
                            for f in result["failures"]), result["failures"])
        self.assertFalse(inside.exists())


class _KitWrapper:
    def __init__(self, factory):
        self.factory = factory
        self.sessions = {}

    def __call__(self, name, tools, fixtures, repo_dirs):
        driver = self.factory(name, tools, fixtures, repo_dirs)
        self.sessions[name] = driver
        return driver


# ---------------------------------------------------------------------------
# Optional real three-repo run
# ---------------------------------------------------------------------------

@unittest.skipUnless(os.environ.get("RSIH_INT002_FULL"),
                     "set RSIH_INT002_FULL=1 to run the real three-repo tracer")
class TestRealThreeRepoRun(TracerTestCase):
    ROOT = Path(__file__).resolve().parents[6]

    def test_real_run_green(self):
        repo_dirs = {
            "host": str(self.ROOT / "memory_graph_evolving" / "pi-group-chat-host"),
            "gms": str(self.ROOT / "memory_graph_evolving" / "graph-memory-service"),
            "rsih": str(self.ROOT / "RSI-Harness"),
        }
        result = run_s2_s4.run_tracer(
            repo_dirs=repo_dirs, fixtures=FIXTURES,
            evidence_out=self.evidence, toolchain=None)
        self.assertEqual(result["failures"], [])
        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
