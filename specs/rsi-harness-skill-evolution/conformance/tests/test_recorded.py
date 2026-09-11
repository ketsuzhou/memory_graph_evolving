"""FND-002 recorded corpus tests: Host Sealed Segments + RSIH Q29-B fake runtime.

Covers implementation-plan FND-002:
- Red/Green driver: ``test_q29b_rejects_unsealed_or_nondeterministic_inputs``
  asserts that missing sealed path, flipped seal byte, exhausted fake clock,
  an extra undeclared tool call and a late output rewriting the terminal are
  all rejected.
- segments success/failure/recovery end-to-end, failed/aborted produce no refs.
- repeated validation of the same recorded packet is bit-identical.

Pure stdlib ``unittest``; loads the validator from ``../validate_recorded.py``.
"""

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

FIX = Path(__file__).resolve().parents[1]
RECORDED = FIX / "recorded"
ROOT = FIX.parents[3]


def _load_validator():
    spec = importlib.util.spec_from_file_location(
        "validate_recorded_fnd002", str(FIX / "validate_recorded.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


VR = _load_validator()


def read_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(obj, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


class TempRecordedRootMixin(unittest.TestCase):
    """Provides a disposable copy of recorded/ for mutation tests."""

    def make_root(self):
        tmp = tempfile.mkdtemp(prefix="fnd002-tmp-")
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        dst = Path(tmp) / "recorded"
        shutil.copytree(RECORDED, dst)
        return dst

    def mutate_and_validate(self, mutator):
        root = self.make_root()
        mutator(root)
        return VR.validate_root(root)


class CleanCorpusTests(TempRecordedRootMixin):
    def test_corpus_validates_clean(self):
        report = VR.validate_root(RECORDED)
        self.assertTrue(report.ok, report.render())
        by_case = report.by_case
        for case_id in (
            "seg-success-0001",
            "seg-failed-0001",
            "seg-aborted-0001",
            "seg-recovery-0001",
            "q29b-baseline-0001",
            "q29b-candidate-0001",
            "q29b-overlap-0001",
            "q29b-conflict-0001",
            "q29b-late-output-0001",
            "q29b-clock-exhausted-0001",
            "q29b-extra-tool-call-0001",
        ):
            self.assertEqual(
                by_case[case_id].result, "accept", (case_id, by_case[case_id].reason)
            )
        self.assertEqual(by_case["q29b-merge-reserved"].result, "reserved")

    def test_negative_segment_cases_rejected_with_exact_reasons(self):
        report = VR.validate_root(RECORDED)
        expected = {
            "seg-neg-dag-cycle": "DAG_CYCLE",
            "seg-neg-missing-causal-link": "MISSING_CAUSAL_LINK",
            "seg-neg-nonterminal-tool": "NONTERMINAL_TOOL",
            "seg-neg-seal-mismatch": "SEAL_DIGEST_MISMATCH",
            "seg-neg-append-after-settled": "APPEND_AFTER_SETTLED",
        }
        for case_id, reason in expected.items():
            entry = report.by_case[case_id]
            self.assertEqual(entry.result, "reject", case_id)
            self.assertEqual(entry.reason, reason, case_id)


class SealedSegmentTests(TempRecordedRootMixin):
    success_dir = RECORDED / "segments" / "success" / "seg-success-0001"
    failure_dir = RECORDED / "segments" / "failure" / "seg-failed-0001"
    aborted_dir = RECORDED / "segments" / "failure" / "seg-aborted-0001"
    recovery_dir = RECORDED / "segments" / "recovery" / "seg-recovery-0001"

    def test_success_segment_end_to_end(self):
        segment = read_json(self.success_dir / "segment.json")
        seal = read_json(self.success_dir / "seal.json")
        canonical = (self.success_dir / "canonical.utf8").read_bytes()

        self.assertEqual(segment["terminal_state"], "settled")
        # canonical.utf8 is the exact RFC 8785 canonicalization of segment.json
        self.assertEqual(VR.canonical_bytes(segment), canonical)
        # seal covers canonical bytes byte-for-byte
        self.assertEqual(VR.digest_bytes(canonical), seal["segment_digest"])
        self.assertEqual(seal["segment_digest"], seal["segment_ref"]["segment_digest"])
        # Evidence Seal + Conversation Path Seal + SegmentRef + CheckpointRef all present
        self.assertIn("evidence_seal_body", segment)
        self.assertIn("path_seal_body", segment)
        self.assertEqual(seal["evidence_seal"]["seal_digest"],
                         VR.digest_of(segment["evidence_seal_body"]))
        self.assertEqual(seal["path_seal"]["seal_digest"],
                         VR.digest_of(segment["path_seal_body"]))
        self.assertEqual(seal["segment_ref"]["schema_version"], "host.segment-ref.v1")
        self.assertEqual(seal["checkpoint_ref"]["schema_version"],
                         "host.checkpoint-ref.v1")
        # exactly one exact ToolProxyResult recorded
        self.assertEqual(len(segment["tool_executions"]), 1)
        tpr = segment["tool_executions"][0]["tool_proxy_result"]
        self.assertEqual(tpr["schema_version"], "host.tool-proxy-result.v1")
        self.assertEqual(tpr["status"], "succeeded")
        preimage = {k: v for k, v in tpr.items() if k != "proxy_result_digest"}
        self.assertEqual(VR.digest_of(preimage), tpr["proxy_result_digest"])
        # frontier + DAG shape
        members = segment["members"]
        self.assertEqual(segment["frontier"]["end_room_sequence"],
                         members[-1]["room_sequence"])
        domains = {p["domain"] for p in segment["path_seal_body"]["paths"]}
        self.assertEqual(domains, {"success"})

    def test_failed_and_aborted_segments_produce_no_refs(self):
        for case_dir in (self.failure_dir, self.aborted_dir):
            segment = read_json(case_dir / "segment.json")
            seal = read_json(case_dir / "seal.json")
            self.assertIn(segment["terminal_state"], ("failed", "aborted"))
            for forbidden in ("segment_ref", "checkpoint_ref", "evidence_seal",
                              "path_seal"):
                self.assertNotIn(forbidden, seal, (case_dir, forbidden))
            for forbidden in ("evidence_seal_body", "path_seal_body",
                              "checkpoint_identity"):
                self.assertNotIn(forbidden, segment, (case_dir, forbidden))
            self.assertIn("terminal_audit", seal)
            # the immutable audit record is still digest-covered
            canonical = (case_dir / "canonical.utf8").read_bytes()
            self.assertEqual(VR.digest_bytes(canonical), seal["segment_digest"])

    def test_recovery_segment_keeps_both_attempts(self):
        segment = read_json(self.recovery_dir / "segment.json")
        self.assertEqual(segment["terminal_state"], "settled")
        executions = segment["tool_executions"]
        self.assertEqual(len(executions), 2)
        first, second = executions
        self.assertEqual(first["tool_proxy_result"]["status"], "failed")
        self.assertTrue(first["tool_proxy_result"]["error"]["retryable"])
        self.assertEqual(second["tool_proxy_result"]["status"], "succeeded")
        self.assertTrue(second.get("retry_correlation", {}).get("is_retry"))
        # path seal preserves failure branch AND recovery path with failure prefix
        paths = segment["path_seal_body"]["paths"]
        by_domain = {p["domain"]: p for p in paths}
        self.assertEqual(set(by_domain), {"failure", "recovery"})
        recovery_refs = by_domain["recovery"]["ordered_event_refs"]
        self.assertIn(first["tool_result_event"], recovery_refs)
        self.assertIn(second["tool_result_event"], recovery_refs)

    def test_append_after_settled_and_seal_immutability(self):
        # appending an event beyond the frozen frontier is rejected even if
        # the attacker re-canonicalizes and re-seals the record afterwards
        def append_event(root):
            case_dir = root / "segments" / "success" / "seg-success-0001"
            segment = read_json(case_dir / "segment.json")
            evt = dict(segment["members"][-1])
            evt["event_id"] = "evt-append-999"
            evt["room_sequence"] = segment["frontier"]["end_room_sequence"] + 1
            segment["members"].append(evt)
            write_json(case_dir / "segment.json", segment)
            canonical = VR.canonical_bytes(segment)
            (case_dir / "canonical.utf8").write_bytes(canonical)
            seal = read_json(case_dir / "seal.json")
            seal["segment_digest"] = VR.digest_bytes(canonical)
            seal["canonical_byte_length"] = len(canonical)
            write_json(case_dir / "seal.json", seal)

        report = self.mutate_and_validate(append_event)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["seg-success-0001"].reason,
                         "APPEND_AFTER_SETTLED")

        # flipping one byte of canonical.utf8 breaks the seal
        def flip_canonical_byte(root):
            canonical_path = (root / "segments" / "success" / "seg-success-0001" /
                             "canonical.utf8")
            data = bytearray(canonical_path.read_bytes())
            data[0] = data[0] ^ 0x01
            canonical_path.write_bytes(bytes(data))

        report = self.mutate_and_validate(flip_canonical_byte)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["seg-success-0001"].reason,
                         "SEAL_DIGEST_MISMATCH")


class Q29BFakeRuntimeTests(TempRecordedRootMixin):
    """The FND-002 Red/Green driver."""

    def _case_dir(self, root, family, case_id):
        return root / "q29b" / family / case_id

    def test_q29b_rejects_unsealed_or_nondeterministic_inputs(self):
        # 1. missing sealed path: drop the failure-domain path reference
        def drop_failure_path(root):
            case_dir = self._case_dir(root, "candidate", "q29b-candidate-0001")
            packet = read_json(case_dir / "runtime.json")
            packet["sealed_inputs"]["path_refs"] = [
                p for p in packet["sealed_inputs"]["path_refs"]
                if p["domain"] != "failure"
            ]
            write_json(case_dir / "runtime.json", packet)

        report = self.mutate_and_validate(drop_failure_path)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["q29b-candidate-0001"].reason,
                         "MISSING_SEALED_PATH")

        # 2. seal byte flipped in a referenced sealed segment
        def flip_seal_digest(root):
            seal_path = (root / "segments" / "success" / "seg-success-0001" /
                         "seal.json")
            seal = read_json(seal_path)
            digest = seal["segment_digest"]
            flipped = ("0" if digest[7] != "0" else "1")
            seal["segment_digest"] = digest[:7] + flipped + digest[8:]
            write_json(seal_path, seal)

        report = self.mutate_and_validate(flip_seal_digest)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["seg-success-0001"].reason,
                         "SEAL_DIGEST_MISMATCH")
        # every q29b packet consuming that sealed segment must fail closed too
        self.assertEqual(report.by_case["q29b-baseline-0001"].reason,
                         "SEAL_DIGEST_MISMATCH")

        # 3. fake clock exhausted while terminal claims success
        def exhaust_clock(root):
            case_dir = self._case_dir(root, "baseline", "q29b-baseline-0001")
            packet = read_json(case_dir / "runtime.json")
            packet["capabilities"]["clock"]["ticks"] = \
                packet["capabilities"]["clock"]["ticks"][:1]
            write_json(case_dir / "runtime.json", packet)

        report = self.mutate_and_validate(exhaust_clock)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["q29b-baseline-0001"].reason,
                         "FAKE_CLOCK_EXHAUSTED")

        # 4. extra undeclared tool call appended to the call sequence
        def extra_tool_call(root):
            case_dir = self._case_dir(root, "baseline", "q29b-baseline-0001")
            packet = read_json(case_dir / "runtime.json")
            last = packet["call_sequence"][-1]["step"]
            packet["call_sequence"].append({
                "step": last + 1,
                "capability": "tool",
                "operation": "invoke",
                "call_index": 1,
                "tool_name": "memory_expand",
            })
            write_json(case_dir / "runtime.json", packet)

        report = self.mutate_and_validate(extra_tool_call)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["q29b-baseline-0001"].reason,
                         "UNDECLARED_TOOL_ACCESS")

        # 5a. late output claims it was applied to the terminal record
        def late_output_rewrites_terminal(root):
            case_dir = self._case_dir(root, "late-output", "q29b-late-output-0001")
            packet = read_json(case_dir / "runtime.json")
            packet["late_events"][0]["recorded_as"] = "applied-to-terminal"
            write_json(case_dir / "runtime.json", packet)

        report = self.mutate_and_validate(late_output_rewrites_terminal)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["q29b-late-output-0001"].reason,
                         "LATE_OUTPUT_REWRITE")

        # 5b. late output arriving before the terminal is equally rejected
        def late_output_before_terminal(root):
            case_dir = self._case_dir(root, "late-output", "q29b-late-output-0001")
            packet = read_json(case_dir / "runtime.json")
            packet["late_events"][0]["arrival_step"] = 1
            write_json(case_dir / "runtime.json", packet)

        report = self.mutate_and_validate(late_output_before_terminal)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["q29b-late-output-0001"].reason,
                         "LATE_OUTPUT_REWRITE")

    def test_q29b_recorded_fail_closed_families(self):
        report = VR.validate_root(RECORDED)
        self.assertTrue(report.q29b_output_digests)
        for case_id, status, reason in (
            ("q29b-clock-exhausted-0001", "failed", "FAKE_CLOCK_EXHAUSTED"),
            ("q29b-extra-tool-call-0001", "failed", "UNDECLARED_TOOL_ACCESS"),
            ("q29b-late-output-0001", "failed", "FAKE_TOOL_NO_RESPONSE"),
        ):
            terminal = report.q29b_terminal_records[case_id]
            self.assertEqual(terminal["status"], status, case_id)
            self.assertEqual(terminal["reason_code"], reason, case_id)

    def test_q29b_rejects_live_mode_and_real_capabilities(self):
        def live_mode(root):
            case_dir = self._case_dir(root, "candidate", "q29b-candidate-0001")
            packet = read_json(case_dir / "runtime.json")
            packet["run"]["mode"] = "live"
            write_json(case_dir / "runtime.json", packet)

        report = self.mutate_and_validate(live_mode)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["q29b-candidate-0001"].reason,
                         "CANDIDATE_IN_LIVE_REGISTRY")

        def real_clock(root):
            case_dir = self._case_dir(root, "baseline", "q29b-baseline-0001")
            packet = read_json(case_dir / "runtime.json")
            packet["capabilities"]["clock"]["kind"] = "wall-clock"
            write_json(case_dir / "runtime.json", packet)

        report = self.mutate_and_validate(real_clock)
        self.assertFalse(report.ok)
        self.assertEqual(report.by_case["q29b-baseline-0001"].reason,
                         "REAL_CAPABILITY_DECLARED")

    def test_repeated_validation_is_bit_identical(self):
        first = VR.validate_root(RECORDED)
        second = VR.validate_root(RECORDED)
        self.assertTrue(first.ok)
        self.assertEqual(first.render(), second.render())
        self.assertEqual(first.q29b_output_digests, second.q29b_output_digests)


class CorpusManifestTests(TempRecordedRootMixin):
    def test_missing_family_rejected(self):
        def remove_conflict(root):
            shutil.rmtree(root / "q29b" / "conflict")
            manifest_path = root / "manifest.json"
            manifest = read_json(manifest_path)
            manifest["cases"] = [
                c for c in manifest["cases"] if c["family"] != "conflict"
            ]
            write_json(manifest_path, manifest)

        report = self.mutate_and_validate(remove_conflict)
        self.assertFalse(report.ok)
        self.assertIn("MISSING_FAMILY", report.corpus_errors)

    def test_duplicate_case_id_rejected(self):
        def duplicate(root):
            manifest_path = root / "manifest.json"
            manifest = read_json(manifest_path)
            manifest["cases"].append(dict(manifest["cases"][0]))
            write_json(manifest_path, manifest)

        report = self.mutate_and_validate(duplicate)
        self.assertFalse(report.ok)
        self.assertIn("DUPLICATE_CASE_ID", report.corpus_errors)

    def test_unregistered_case_dir_rejected(self):
        def stray_case(root):
            src = root / "q29b" / "baseline" / "q29b-baseline-0001"
            dst = root / "q29b" / "baseline" / "q29b-baseline-9999"
            shutil.copytree(src, dst)

        report = self.mutate_and_validate(stray_case)
        self.assertFalse(report.ok)
        self.assertIn("UNREGISTERED_CASE", report.corpus_errors)


class CliTests(unittest.TestCase):
    def test_exact_command_exit_zero(self):
        result = subprocess.run(
            [sys.executable, str(FIX / "validate_recorded.py"),
             "--root", str(RECORDED)],
            cwd=str(ROOT), capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("status=CLEAN", result.stdout)

    def test_validator_runs_twice_with_identical_stdout(self):
        cmd = [sys.executable, str(FIX / "validate_recorded.py"),
               "--root", str(RECORDED)]
        first = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
        second = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
        self.assertEqual(first.returncode, 0)
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(first.stderr, second.stderr)


if __name__ == "__main__":
    unittest.main()
