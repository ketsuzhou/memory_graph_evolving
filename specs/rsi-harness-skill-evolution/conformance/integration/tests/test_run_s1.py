"""INT-001 S1 Go<->TS conformance gate tests.

Stdlib ``unittest`` only. The suite never invokes the real ``go``/``node``
adapter CLIs on the slow path: every test injects fake runner functions that
materialise canned ``rsih-s1-report.v1`` JSON reports, plus a fixed toolchain
description. The real three-repo run is an opt-in test behind the
``RSIH_INT001_FULL`` environment variable.
"""

import base64
import copy
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

INTEGRATION_ROOT = Path(__file__).resolve().parents[1]

# Load the gate by explicit path so a same-named module elsewhere can never
# shadow it (same convention as the other conformance test suites).
_SPEC = importlib.util.spec_from_file_location(
    "int001_run_s1", INTEGRATION_ROOT / "run_s1.py"
)
run_s1 = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = run_s1
_SPEC.loader.exec_module(run_s1)

ADAPTERS = ("host", "gms", "rsih")
TOOLCHAIN = {"go": "go1.26.1", "node": "v22.22.2", "python": "3.12.3"}

ALPHA_BYTES = b'{"alpha":1}'
ALPHA_BASE64 = base64.b64encode(ALPHA_BYTES).decode("ascii")
ALPHA_DIGEST = "sha256:" + hashlib.sha256(ALPHA_BYTES).hexdigest()
BETA_BYTES = b'{"beta":2}'
BETA_BASE64 = base64.b64encode(BETA_BYTES).decode("ascii")
BETA_DIGEST = "sha256:" + hashlib.sha256(BETA_BYTES).hexdigest()


def make_case(case_id, category, source_path, *, accept=True):
    """Build one report case; rejects carry null digest/length/reason alias."""
    if accept:
        return {
            "id": case_id,
            "category": category,
            "source_path": source_path,
            "derived_accept": True,
            "derived_reason": None,
            "derived_digest": ALPHA_DIGEST,
            "derived_length": len(ALPHA_BYTES),
            "derived_base64": ALPHA_BASE64,
            "matches_expected": True,
        }
    return {
        "id": case_id,
        "category": category,
        "source_path": source_path,
        "derived_accept": False,
        "derived_reason": "DIGEST_MISMATCH",
        "derived_digest": None,
        "derived_length": None,
        "derived_base64": "",
        "matches_expected": True,
    }


def baseline_cases():
    return [
        make_case("alpha-accept", "canonicalization",
                  "canonicalization/alpha-accept/source.json"),
        make_case("beta-reject", "negative",
                  "negative/beta-reject/source.json", accept=False),
    ]


def make_report(adapter, cases=None, *, runtime=None, all_passed=True,
                schema_version=None):
    if cases is None:
        cases = baseline_cases()
    if runtime is None:
        runtime = {"host": "go1.26.1", "gms": "go1.26.1",
                   "rsih": "nodev22.22.2"}[adapter]
    language = "go" if adapter in ("host", "gms") else "typescript"
    return {
        "schema_version": schema_version or run_s1.REPORT_SCHEMA_VERSION,
        "adapter": {"language": language, "runtime": runtime, "repo": adapter},
        "manifest_digest": "sha256:" + "0" * 64,
        "case_count": len(cases),
        "all_passed": all_passed,
        "cases": cases,
    }


class SimpleProc:
    def __init__(self, returncode, stderr):
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = ""


class FakeRunner:
    """Runner double: writes canned per-adapter reports, one per call.

    ``reports`` maps adapter name -> list of report dicts (or the string
    ``"FAIL:<stderr tail>"``), consumed left to right across rounds.
    """

    def __init__(self, reports):
        self.reports = {name: list(queue) for name, queue in reports.items()}
        self.calls = []

    def __call__(self, adapter, fixtures, report_path):
        self.calls.append((adapter, str(fixtures), str(report_path)))
        queue = self.reports[adapter]
        item = queue.pop(0) if queue else None
        if isinstance(item, str) and item.startswith("FAIL:"):
            return SimpleProc(1, item[len("FAIL:"):])
        if item is None:
            return SimpleProc(1, "fake runner has no report queued")
        Path(report_path).write_text(json.dumps(item), encoding="utf-8")
        return SimpleProc(0, "")


class Int001GateTest(unittest.TestCase):
    """Shared scaffolding: temp fixtures tree, temp evidence dir, fake gate."""

    def setUp(self):
        self.fixtures = self.make_fixtures()
        self.evidence = Path(tempfile.mkdtemp(prefix="int001-evidence-"))
        self.addCleanup(shutil.rmtree, self.evidence, ignore_errors=True)

    def make_fixtures(self, case_ids=("alpha-accept", "beta-reject")):
        root = Path(tempfile.mkdtemp(prefix="int001-fixtures-"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        entries = []
        for case_id in case_ids:
            category = "canonicalization" if case_id.startswith("alpha") else "negative"
            case_dir = category + "/" + case_id
            (root / case_dir).mkdir(parents=True, exist_ok=True)
            (root / case_dir / "source.json").write_text('{"x":1}', encoding="utf-8")
            (root / case_dir / "expected.json").write_text('{"y":2}', encoding="utf-8")
            entries.append({
                "case_id": case_id,
                "category": category,
                "source_path": case_dir + "/source.json",
                "expected_path": case_dir + "/expected.json",
                "canonical_utf8_path": case_dir + "/canonical.utf8",
                "canonical_base64_path": case_dir + "/canonical.base64",
                "expected_accept": True,
                "expected_digest": ALPHA_DIGEST,
                "expected_canonical_byte_length": len(ALPHA_BYTES),
                "expected_reason_code": None,
            })
        (root / "manifest.json").write_text(json.dumps({
            "schema_version": "rsih-skill-evolution.conformance-manifest.v1",
            "cases": entries}), encoding="utf-8")
        return root

    def gate(self, reports=None, *, toolchain=TOOLCHAIN, fixtures=None,
             evidence_out=None, runner=None):
        """Run the gate with fake reports; repeats each report for round 2."""
        if runner is None:
            canned = {}
            for adapter in ADAPTERS:
                spec = (reports or {}).get(adapter)
                if spec is None:
                    spec = make_report(adapter)
                canned[adapter] = spec if isinstance(spec, list) else [spec, spec]
            runner = FakeRunner(canned)
        return run_s1.run_gate(
            repo_dirs={name: "/tmp/repo-" + name for name in ADAPTERS},
            fixtures=fixtures or self.fixtures,
            evidence_out=evidence_out or self.evidence,
            runner=runner,
            toolchain=toolchain,
        )

    def failures_text(self, result):
        return "\n".join(result["failures"])

    def read_comparison(self, result):
        with open(result["comparison_path"], encoding="utf-8") as handle:
            return json.load(handle)


class DetectsByteDigestAcceptAndReasonDivergence(Int001GateTest):
    """INT-001 Red test: the comparator must flag every semantic divergence.

    Byte-only or digest-only comparison is forbidden: a flipped canonical
    byte payload, a flipped digest, an accept/reject flip and a reason alias
    (DIGEST_MISMATCH -> REF_MISMATCH) must each be reported with the case id,
    the field name and the per-adapter values.
    """

    def test_detects_byte_digest_accept_and_reason_divergence(self):
        base = {a: make_report(a) for a in ADAPTERS}

        byte_div = copy.deepcopy(base["rsih"])
        byte_div["cases"][0]["derived_base64"] = base64.b64encode(
            b'{"alpha":2}').decode("ascii")

        digest_div = copy.deepcopy(base["gms"])
        digest_div["cases"][0]["derived_digest"] = "sha256:" + "9" * 64

        accept_div = copy.deepcopy(base["gms"])
        accept_div["cases"][0]["derived_accept"] = False
        accept_div["cases"][0]["derived_reason"] = "DIGEST_MISMATCH"

        reason_div = copy.deepcopy(base["rsih"])
        reason_div["cases"][1]["derived_reason"] = "REF_MISMATCH"

        result = self.gate({"host": base["host"], "gms": digest_div,
                            "rsih": byte_div})
        self.assertFalse(result["ok"], "divergent reports must fail the gate")

        result2 = self.gate({"host": base["host"], "gms": accept_div,
                             "rsih": reason_div})
        self.assertFalse(result2["ok"])

        text = self.failures_text(result) + "\n" + self.failures_text(result2)
        self.assertIn("derived_base64", text)
        self.assertIn("derived_digest", text)
        self.assertIn("derived_accept", text)
        self.assertIn("derived_reason", text)
        self.assertIn("alpha-accept", text)
        self.assertIn("beta-reject", text)
        # reason alias must show both spellings so the divergence is auditable
        self.assertIn("DIGEST_MISMATCH", text)
        self.assertIn("REF_MISMATCH", text)
        # identical rounds: divergence must not be blamed on nondeterminism
        self.assertNotIn("nondeterministic", text)


class GreenPath(Int001GateTest):

    def test_green_path_writes_evidence_and_leaves_fixtures_untouched(self):
        before = run_s1.snapshot_tree(self.fixtures)
        result = self.gate()
        self.assertTrue(result["ok"], self.failures_text(result))
        self.assertEqual(result["failures"], [])

        self.assertEqual(run_s1.snapshot_tree(self.fixtures), before,
                         "fixtures tree must be byte-identical after the gate")
        evidence = Path(result["evidence_dir"])
        self.assertTrue((evidence / "comparison.json").is_file())
        for rnd in (1, 2):
            for adapter in ADAPTERS:
                self.assertTrue(
                    (evidence / ("%s.round%d.json" % (adapter, rnd))).is_file(),
                    "adapter report copy missing for %s round %d" % (adapter, rnd))
        # nothing escaped into the fixtures tree
        new_files = set(run_s1.snapshot_tree(self.fixtures)) - set(before)
        self.assertEqual(new_files, set())

        comparison = self.read_comparison(result)
        self.assertEqual(comparison["schema_version"],
                         run_s1.COMPARISON_SCHEMA_VERSION)
        self.assertEqual(comparison["toolchain"], TOOLCHAIN)
        self.assertEqual(sorted(comparison["report_copies"]),
                         sorted(ADAPTERS))
        self.assertTrue(comparison["all_passed"])
        self.assertTrue(comparison["comparison"]["case_set_equal_across_adapters"])
        self.assertTrue(comparison["determinism"]["consistent"])
        self.assertTrue(comparison["fixtures_immutable"])
        per_case = comparison["comparison"]["per_case_conclusion"]
        self.assertEqual([c["id"] for c in per_case],
                         ["alpha-accept", "beta-reject"])
        for entry in per_case:
            self.assertEqual(sorted(entry["fields"]),
                             sorted(run_s1.COMPARED_CASE_FIELDS))
            for field in entry["fields"].values():
                self.assertTrue(field["all_equal"])
                self.assertEqual(sorted(field["values"]), sorted(ADAPTERS))


class CaseSetFailures(Int001GateTest):

    def test_missing_case_detected(self):
        cases = baseline_cases()
        short = make_report("gms", cases=cases[:1])
        result = self.gate({"gms": short})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        self.assertIn("case missing from report: beta-reject", text)
        self.assertIn("gms", text)

    def test_case_count_mismatch_detected(self):
        bad = make_report("rsih")
        bad["case_count"] = 7
        result = self.gate({"rsih": bad})
        self.assertFalse(result["ok"])
        self.assertIn("case_count", self.failures_text(result))

    def test_extra_case_not_in_manifest_detected(self):
        cases = baseline_cases()
        cases.append(make_case("gamma-ghost", "canonicalization",
                               "canonicalization/gamma-ghost/source.json"))
        result = self.gate({"host": make_report("host", cases=cases)})
        self.assertFalse(result["ok"])
        self.assertIn("case not in manifest: gamma-ghost",
                      self.failures_text(result))

    def test_all_passed_false_detected(self):
        bad = make_report("gms", all_passed=False)
        result = self.gate({"gms": bad})
        self.assertFalse(result["ok"])
        self.assertIn("all_passed", self.failures_text(result))

    def test_adapter_nonzero_exit_is_red_with_stderr_tail(self):
        result = self.gate({"rsih": ["FAIL:boom stack trace tail", None]})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        self.assertIn("rsih round 1 exited 1", text)
        self.assertIn("boom stack trace tail", text)


class ComparatorFieldCoverage(Int001GateTest):
    """Byte-only or digest-only comparison must be impossible."""

    SEMANTIC_FIELDS = {
        "derived_base64", "derived_digest", "derived_length",
        "derived_accept", "derived_reason", "matches_expected",
    }

    def test_compared_field_list_covers_all_semantic_fields(self):
        compared = set(run_s1.COMPARED_CASE_FIELDS)
        self.assertTrue(
            self.SEMANTIC_FIELDS.issubset(compared),
            "COMPARED_CASE_FIELDS must include every semantic field; got %r"
            % sorted(compared))
        self.assertTrue(len(compared) >= len(self.SEMANTIC_FIELDS))
        # a byte-only or digest-only comparator would fail exactly here
        self.assertIn("derived_base64", compared)
        self.assertIn("derived_digest", compared)
        self.assertIn("derived_reason", compared)

    def test_digest_only_divergence_detected(self):
        bad = make_report("gms")
        bad["cases"][0]["derived_digest"] = "sha256:" + "9" * 64
        result = self.gate({"gms": bad})
        self.assertFalse(result["ok"])
        self.assertIn("field=derived_digest", self.failures_text(result))

    def test_byte_only_divergence_detected(self):
        bad = make_report("host")
        bad["cases"][0]["derived_base64"] = base64.b64encode(
            b'{"alpha":9}').decode("ascii")
        # keep digest/length identical: a digest-only comparator would pass
        result = self.gate({"host": bad})
        self.assertFalse(result["ok"])
        self.assertIn("field=derived_base64", self.failures_text(result))

    def test_length_only_and_matches_expected_only_divergence_detected(self):
        bad_len = make_report("rsih")
        bad_len["cases"][0]["derived_length"] = 999
        result = self.gate({"rsih": bad_len})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        self.assertIn("field=derived_length", text)
        self.assertIn("derived_length 999", text)  # internal consistency too

        bad_match = make_report("gms")
        bad_match["cases"][1]["matches_expected"] = False
        result2 = self.gate({"gms": bad_match})
        self.assertFalse(result2["ok"])
        self.assertIn("field=matches_expected", self.failures_text(result2))

    def test_null_semantics_divergence_detected(self):
        # digest null vs digest present must be a divergence, not equal
        bad = make_report("gms")
        bad["cases"][1]["derived_digest"] = BETA_DIGEST
        bad["cases"][1]["derived_length"] = len(BETA_BYTES)
        bad["cases"][1]["derived_base64"] = BETA_BASE64
        result = self.gate({"gms": bad})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        for field in ("derived_base64", "derived_digest", "derived_length"):
            self.assertIn("field=%s" % field, text)

    def test_non_canonical_base64_detected(self):
        bad = make_report("host")
        bad["cases"][0]["derived_base64"] = ALPHA_BASE64 + "\n"
        result = self.gate({"host": bad})
        self.assertFalse(result["ok"])
        self.assertIn("not valid base64", self.failures_text(result))

    def test_reason_null_vs_code_divergence_detected(self):
        bad = make_report("rsih")
        bad["cases"][0]["derived_reason"] = "UNEXPECTED_REASON"
        result = self.gate({"rsih": bad})
        self.assertFalse(result["ok"])
        self.assertIn("field=derived_reason", self.failures_text(result))


class ExpectedCopyDetection(Int001GateTest):

    def test_source_path_pointing_at_expected_file_is_red(self):
        bad = make_report("rsih")
        bad["cases"][0]["source_path"] = \
            "canonicalization/alpha-accept/expected.json"
        result = self.gate({"rsih": bad})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        self.assertIn("expected copy", text)
        self.assertIn("alpha-accept", text)

    def test_source_path_diverging_from_manifest_is_red(self):
        bad = make_report("gms")
        bad["cases"][0]["source_path"] = "somewhere/else/source.json"
        result = self.gate({"gms": bad})
        self.assertFalse(result["ok"])
        self.assertIn("diverges from manifest source", self.failures_text(result))


class SchemaValidation(Int001GateTest):

    def test_unknown_report_schema_is_red(self):
        bad = make_report("host", schema_version="rsih-s1-report.v2")
        result = self.gate({"host": bad})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        self.assertIn("unknown report schema", text)
        self.assertIn("rsih-s1-report.v2", text)
        self.assertIn("rsih-s1-report.v1", text)

    def test_report_repo_metadata_mismatch_is_red(self):
        bad = make_report("gms")
        bad["adapter"]["repo"] = "someone-else"
        result = self.gate({"gms": bad})
        self.assertFalse(result["ok"])
        self.assertIn("claims repo", self.failures_text(result))


class Determinism(Int001GateTest):

    def test_nondeterministic_rerun_is_red(self):
        round1 = make_report("host")
        round2 = make_report("host")
        round2["cases"][0]["derived_base64"] = base64.b64encode(
            b'{"alpha":3}').decode("ascii")
        result = self.gate({"host": [round1, round2]})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        self.assertIn("nondeterministic rerun", text)
        self.assertIn("field=derived_base64", text)
        self.assertIn("round1=", text)
        comparison = self.read_comparison(result)
        self.assertFalse(comparison["determinism"]["consistent"])

    def test_case_appearing_only_in_round_two_is_red(self):
        round1 = make_report("gms", cases=baseline_cases()[:1])
        round2 = make_report("gms")
        result = self.gate({"gms": [round1, round2]})
        self.assertFalse(result["ok"])
        self.assertIn("nondeterministic", self.failures_text(result))


class Toolchain(Int001GateTest):

    def test_toolchain_mismatch_is_red(self):
        bad = make_report("host", runtime="go1.99.9")
        result = self.gate({"host": bad})
        self.assertFalse(result["ok"])
        text = self.failures_text(result)
        self.assertIn("toolchain mismatch", text)
        self.assertIn("go1.99.9", text)
        self.assertIn("go1.26.1", text)

    def test_node_runtime_must_match_reported_node_version(self):
        bad = make_report("rsih", runtime="nodev18.0.0")
        result = self.gate({"rsih": bad})
        self.assertFalse(result["ok"])
        self.assertIn("toolchain mismatch", self.failures_text(result))

    def test_collect_toolchain_uses_subprocess_and_parses_versions(self):
        commands = []

        def fake_run(argv, **kwargs):
            commands.append(list(argv))
            if argv[0].endswith("go"):
                out = "go version go1.26.1 linux/amd64"
            elif argv[0].endswith("/node"):
                out = "v22.22.2"
            else:
                out = "Python 3.12.3"
            return subprocess.CompletedProcess(argv, 0, stdout=out, stderr="")

        original_run = run_s1.subprocess.run
        original_resolve = run_s1.resolve_tool
        run_s1.subprocess.run = fake_run
        run_s1.resolve_tool = lambda name: "/fake/bin/" + name
        try:
            toolchain, failures = run_s1.collect_toolchain()
        finally:
            run_s1.subprocess.run = original_run
            run_s1.resolve_tool = original_resolve
        self.assertEqual(failures, [])
        self.assertEqual(toolchain,
                         {"go": "go1.26.1", "node": "v22.22.2",
                          "python": "3.12.3"})
        flattened = [cmd for cmd in commands]
        self.assertTrue(any(c[1:] == ["version"] and c[0].endswith("/go")
                            for c in flattened), flattened)
        self.assertTrue(any(c[1:] == ["--version"] and c[0].endswith("/node")
                            for c in flattened), flattened)
        self.assertTrue(any(sys.executable in c[0] and "--version" in c
                            for c in flattened), flattened)

    def test_collect_toolchain_missing_tool_is_failure(self):
        original_resolve = run_s1.resolve_tool
        run_s1.resolve_tool = lambda name: None
        try:
            toolchain, failures = run_s1.collect_toolchain()
        finally:
            run_s1.resolve_tool = original_resolve
        self.assertEqual(toolchain, {"go": None, "node": None, "python": None})
        self.assertEqual(len(failures), 3)
        self.assertIn("not found", "\n".join(failures))


class EvidenceContainment(Int001GateTest):

    def test_evidence_out_inside_fixtures_is_refused(self):
        inside = self.fixtures / "leaked-evidence"
        result = self.gate(evidence_out=inside)
        self.assertFalse(result["ok"])
        self.assertIn("inside the fixtures tree", self.failures_text(result))
        self.assertFalse(inside.exists(), "nothing may be created in fixtures")

    def test_evidence_contains_no_fixture_paths(self):
        result = self.gate()
        self.assertTrue(result["ok"], self.failures_text(result))
        evidence_files = sorted(
            p.relative_to(result["evidence_dir"]).as_posix()
            for p in Path(result["evidence_dir"]).rglob("*") if p.is_file())
        self.assertEqual(
            evidence_files,
            ["comparison.json", "gms.round1.json", "gms.round2.json",
             "host.round1.json", "host.round2.json",
             "rsih.round1.json", "rsih.round2.json"])


class RunnerInvocation(Int001GateTest):
    """The runner seam: SubprocessAdapterRunner builds the documented CLIs."""

    def test_adapter_argv_matches_documented_commands(self):
        argv = run_s1.adapter_argv(
            "host", {"go": "/go", "node": "/node"})
        self.assertEqual(argv, ["/go", "run", "./cmd/contract-conformance"])
        self.assertEqual(
            run_s1.adapter_argv("gms", {"go": "/go", "node": "/node"}),
            ["/go", "run", "./cmd/contract-conformance"])
        self.assertEqual(
            run_s1.adapter_argv("rsih", {"go": "/go", "node": "/node"}),
            ["/node", "--experimental-strip-types",
             "scripts/skill-evolution-s1.ts"])

    def test_subprocess_runner_invokes_cli_with_cwd_and_flags(self):
        recorded = {}

        def fake_run(argv, **kwargs):
            recorded["argv"] = list(argv)
            recorded["cwd"] = kwargs.get("cwd")
            recorded["capture"] = kwargs.get("capture_output")
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        original_run = run_s1.subprocess.run
        run_s1.subprocess.run = fake_run
        try:
            runner = run_s1.SubprocessAdapterRunner(
                {"rsih": "/repo/rsih"}, {"go": "/go", "node": "/node"})
            proc = runner("rsih", "/fix", "/tmp/report.json")
        finally:
            run_s1.subprocess.run = original_run
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(recorded["argv"], [
            "/node", "--experimental-strip-types",
            "scripts/skill-evolution-s1.ts",
            "--fixtures", "/fix", "--report", "/tmp/report.json"])
        self.assertEqual(recorded["cwd"], "/repo/rsih")
        self.assertTrue(recorded["capture"])

    def test_runner_and_comparison_are_separate_units(self):
        """Refactor contract: invocation lives in SubprocessAdapterRunner;
        comparison functions never touch subprocess state."""
        source = (INTEGRATION_ROOT / "run_s1.py").read_text(encoding="utf-8")
        for pure in ("compare_reports", "compare_rounds", "validate_report",
                     "check_no_expected_copy", "per_case_conclusion"):
            start = source.index("def %s(" % pure)
            end = source.index("\ndef ", start + 1)
            body = source[start:end]
            code_only = "\n".join(
                line for line in body.splitlines()
                if not line.lstrip().startswith("#"))
            self.assertNotIn("subprocess", code_only,
                             "%s must stay free of subprocess concerns" % pure)
        self.assertIn("class SubprocessAdapterRunner", source)


@unittest.skipUnless(os.environ.get("RSIH_INT001_FULL"),
                     "set RSIH_INT001_FULL=1 to run the real three-repo gate")
class FullThreeRepoRun(unittest.TestCase):
    """Opt-in: executes the real go/node adapter CLIs (slow path)."""

    ROOT = Path("/home/zhoujie22/river2_0")

    def test_real_run_is_green(self):
        result = run_s1.run_gate(
            repo_dirs={
                "host": str(self.ROOT / "memory_graph_evolving/pi-group-chat-host"),
                "gms": str(self.ROOT / "memory_graph_evolving/graph-memory-service"),
                "rsih": str(self.ROOT / "RSI-Harness"),
            },
            fixtures=str(self.ROOT / "memory_graph_evolving/specs/"
                         "rsi-harness-skill-evolution/conformance"),
        )
        self.assertTrue(result["ok"], "\n".join(result["failures"]))


if __name__ == "__main__":
    unittest.main()
