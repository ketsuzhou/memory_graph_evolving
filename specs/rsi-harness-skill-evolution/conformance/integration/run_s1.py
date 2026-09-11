#!/usr/bin/env python3
"""INT-001 -- S1 Go<->TS conformance gate.

Orchestrates the three S1 adapters (Host Go, GMS Go, RSIH TypeScript) against
one shared fixture manifest and compares their JSON reports field by field:

* canonical bytes (base64, byte-for-byte), digest, length (null semantics),
  accept/reject decision, reject reason code, and matches_expected
  -- comparing only bytes or only digest is a defect this gate rejects.

Fail-closed by construction. Any of the following turns the gate red:
adapter process failure (nonzero exit), unreadable/invalid report, unknown
report schema, all_passed=false, case-count or case-set mismatch against the
manifest, per-case field divergence, an adapter reading an expected/golden
file instead of its source, a toolchain mismatch between the report runtimes
and the toolchain actually used, a non-deterministic rerun (two full rounds
must reach identical per-case conclusions), or any mutation of the fixtures
tree. Evidence (the three report copies, the per-field comparison conclusion
and the toolchain versions) is written only to a temporary directory outside
the fixtures tree; golden files are never updated.

Exit code 0 = gate green, 1 = divergence or failure.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPORT_SCHEMA_VERSION = "rsih-s1-report.v1"
COMPARISON_SCHEMA_VERSION = "rsih-int001-comparison.v1"
MANIFEST_NAME = "manifest.json"
ROUNDS = 2

ADAPTER_ORDER = ("host", "gms", "rsih")

# Every field below is compared for every case across all three adapters.
# Byte-only or digest-only comparison is forbidden: the negative tests in
# tests/test_run_s1.py pin this exact list by reflection and by behaviour.
COMPARED_CASE_FIELDS = (
    "derived_base64",    # canonical bytes, base64 encoded (byte-for-byte)
    "derived_digest",    # sha256 of the canonical bytes (None when rejected)
    "derived_length",    # canonical byte length (None when rejected)
    "derived_accept",    # accept/reject decision
    "derived_reason",    # reject reason code (None when accepted)
    "matches_expected",  # the adapter's own golden comparison
)
REQUIRED_CASE_KEYS = ("id", "category", "source_path") + COMPARED_CASE_FIELDS

MISSING = object()  # sentinel: field absent from a report case

GO_FALLBACK_PATHS = (
    os.path.join(os.path.expanduser("~"), "go", "bin"),
    "/usr/local/go/bin",
)


# ---------------------------------------------------------------------------
# Runner invocation (subprocess side; no report interpretation here)
# ---------------------------------------------------------------------------

def resolve_tool(name):
    """Locate an executable, honouring the usual Go install locations."""
    found = shutil.which(name)
    if found:
        return found
    if name == "go":
        for directory in GO_FALLBACK_PATHS:
            candidate = os.path.join(directory, name)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
    return None


def adapter_argv(adapter, tools):
    if adapter in ("host", "gms"):
        return [tools["go"], "run", "./cmd/contract-conformance"]
    return [tools["node"], "--experimental-strip-types",
            "scripts/skill-evolution-s1.ts"]


HERMETIC_GOCACHE = os.path.join(tempfile.gettempdir(), "int001-gocache-rsih")


class SubprocessAdapterRunner:
    """Invokes the real adapter CLIs, one subprocess per adapter per round.

    Deliberately free of report comparison logic: it only runs the command
    and returns the CompletedProcess. Comparison lives in compare_reports.

    ``GOCACHE`` is pinned to a stable temp directory when the caller has not
    set it: host go-build caches on shared machines can hold unreadable
    entries that make ``go run`` fail independently of the fixtures, which
    would be a false red. The choice is recorded in the evidence output.
    """

    def __init__(self, repo_dirs, tools):
        self.repo_dirs = repo_dirs
        self.tools = tools
        self.env = dict(os.environ)
        self.gocache_note = "inherited from environment"
        if "GOCACHE" not in self.env:
            os.makedirs(HERMETIC_GOCACHE, exist_ok=True)
            self.env["GOCACHE"] = HERMETIC_GOCACHE
            self.gocache_note = "hermetic %s (GOCACHE was unset)" % HERMETIC_GOCACHE

    def __call__(self, adapter, fixtures, report_path):
        argv = adapter_argv(adapter, self.tools) + [
            "--fixtures", str(fixtures), "--report", str(report_path),
        ]
        return subprocess.run(
            argv, cwd=self.repo_dirs[adapter], capture_output=True,
            text=True, check=False, env=self.env,
        )


def collect_toolchain():
    """Gather go/node/python versions via subprocess; fail-closed on errors."""
    toolchain = {}
    failures = []
    specs = (
        ("go", ["go", "version"], lambda out: out.split()[2] if len(out.split()) >= 3 else out.strip()),
        ("node", ["node", "--version"], lambda out: out.strip()),
        ("python", [sys.executable, "--version"],
         lambda out: out.strip().split()[-1]),
    )
    for name, argv, parse in specs:
        exe = resolve_tool(argv[0])
        if exe is None:
            toolchain[name] = None
            failures.append("toolchain: %s executable not found" % name)
            continue
        try:
            proc = subprocess.run([exe] + argv[1:], capture_output=True,
                                  text=True, check=False, timeout=120)
        except (OSError, subprocess.SubprocessError) as exc:
            toolchain[name] = None
            failures.append("toolchain: %s version probe failed: %s" % (name, exc))
            continue
        if proc.returncode != 0:
            toolchain[name] = None
            failures.append(
                "toolchain: %s version probe exited %d: %s"
                % (name, proc.returncode, proc.stderr.strip()[-200:]))
            continue
        toolchain[name] = parse(proc.stdout.strip()) or None
        if toolchain[name] is None:
            failures.append("toolchain: %s version output unparseable" % name)
    return toolchain, failures


def check_toolchain(reports_by_round, toolchain):
    """Report runtimes must match the toolchain that actually ran the gate."""
    failures = []
    for rnd_reports in reports_by_round:
        for adapter in ADAPTER_ORDER:
            report = rnd_reports.get(adapter)
            if not isinstance(report, dict):
                continue
            runtime = (report.get("adapter") or {}).get("runtime")
            if adapter in ("host", "gms"):
                expected = toolchain.get("go")
                ok = expected is not None and runtime == expected
            else:
                expected = toolchain.get("node")
                ok = (expected is not None and isinstance(runtime, str)
                      and runtime.startswith("node")
                      and runtime.endswith(expected))
            if not ok:
                failures.append(
                    "toolchain mismatch: adapter %s report runtime %r does "
                    "not match %s %r" % (adapter, runtime, adapter,
                                         expected))
    return failures


# ---------------------------------------------------------------------------
# Fixtures immutability
# ---------------------------------------------------------------------------

def snapshot_tree(root):
    """Map every file under root (relative posix path -> sha256 hex)."""
    root = Path(root)
    snapshot = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            snapshot[path.relative_to(root).as_posix()] = sha256_file(path)
    return snapshot


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Manifest / report loading and structural validation
# ---------------------------------------------------------------------------

def load_manifest(fixtures):
    path = Path(fixtures) / MANIFEST_NAME
    try:
        raw = path.read_bytes()
    except OSError as exc:
        return {"failures": ["manifest unreadable: %s" % exc],
                "ids": set(), "entries": {}, "count": None, "sha256": None}
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {"failures": ["manifest not valid JSON: %s" % exc],
                "ids": set(), "entries": {}, "count": None,
                "sha256": hashlib.sha256(raw).hexdigest()}
    failures = []
    if not isinstance(manifest, dict) or not isinstance(manifest.get("cases"), list):
        return {"failures": ["manifest must be an object with a cases list"],
                "ids": set(), "entries": {}, "count": None,
                "sha256": hashlib.sha256(raw).hexdigest()}
    ids = []
    entries = {}
    for entry in manifest["cases"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("case_id"), str):
            failures.append("manifest entry without string case_id: %r" % (entry,))
            continue
        ids.append(entry["case_id"])
        entries[entry["case_id"]] = entry
    if len(set(ids)) != len(ids):
        failures.append("manifest contains duplicate case ids")
    return {"failures": failures, "ids": set(ids), "entries": entries,
            "count": len(ids), "sha256": hashlib.sha256(raw).hexdigest()}


def validate_report(adapter, report, manifest):
    """Structural, schema, case-set and internal-consistency checks."""
    failures = []
    if not isinstance(report, dict):
        return ["adapter %s: report is not a JSON object" % adapter]

    schema = report.get("schema_version")
    if schema != REPORT_SCHEMA_VERSION:
        failures.append(
            "adapter %s: unknown report schema %r (expected %r); refusing "
            "to compare" % (adapter, schema, REPORT_SCHEMA_VERSION))

    block = report.get("adapter")
    if not isinstance(block, dict):
        failures.append("adapter %s: missing adapter metadata block" % adapter)
    else:
        if block.get("repo") != adapter:
            failures.append(
                "adapter %s: report claims repo %r" % (adapter, block.get("repo")))
        if not isinstance(block.get("language"), str):
            failures.append("adapter %s: adapter.language missing" % adapter)
        if not isinstance(block.get("runtime"), str):
            failures.append("adapter %s: adapter.runtime missing" % adapter)

    if report.get("all_passed") is not True:
        failures.append(
            "adapter %s: all_passed is %r, expected true (adapter skip or "
            "self-check failure)" % (adapter, report.get("all_passed")))

    cases = report.get("cases")
    if not isinstance(cases, list):
        failures.append("adapter %s: cases is not a list" % adapter)
        return failures

    declared = report.get("case_count")
    if declared != len(cases) or not isinstance(declared, int):
        failures.append(
            "adapter %s: case_count %r != %d case entries"
            % (adapter, declared, len(cases)))

    seen = set()
    for case in cases:
        if not isinstance(case, dict):
            failures.append("adapter %s: case entry is not an object" % adapter)
            continue
        missing_keys = [k for k in REQUIRED_CASE_KEYS if k not in case]
        if missing_keys:
            failures.append(
                "adapter %s: case %r missing keys %s"
                % (adapter, case.get("id"), missing_keys))
            continue
        case_id = case["id"]
        if not isinstance(case_id, str):
            failures.append("adapter %s: non-string case id %r" % (adapter, case_id))
            continue
        if case_id in seen:
            failures.append("adapter %s: duplicate case id %s" % (adapter, case_id))
        seen.add(case_id)
        failures.extend(validate_case_consistency(adapter, case))

    for case_id in sorted(manifest["ids"] - seen):
        failures.append("adapter %s: case missing from report: %s" % (adapter, case_id))
    for case_id in sorted(seen - manifest["ids"]):
        failures.append("adapter %s: case not in manifest: %s" % (adapter, case_id))
    return failures


def validate_case_consistency(adapter, case):
    """Internal per-case consistency (base64 canonicality, length agreement)."""
    failures = []
    encoded = case.get("derived_base64")
    digest = case.get("derived_digest")
    length = case.get("derived_length")
    if not isinstance(encoded, str):
        failures.append(
            "adapter %s: case %s derived_base64 is not a string" % (adapter, case["id"]))
        return failures
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        failures.append(
            "adapter %s: case %s derived_base64 is not valid base64"
            % (adapter, case["id"]))
        return failures
    if base64.b64encode(decoded).decode("ascii") != encoded:
        failures.append(
            "adapter %s: case %s derived_base64 is not canonical base64"
            % (adapter, case["id"]))
    if digest is None:
        if length is not None:
            failures.append(
                "adapter %s: case %s has null digest but length %r"
                % (adapter, case["id"], length))
    else:
        if not isinstance(length, int) or isinstance(length, bool):
            failures.append(
                "adapter %s: case %s derived_length %r is not an integer"
                % (adapter, case["id"], length))
        elif length != len(decoded):
            failures.append(
                "adapter %s: case %s derived_length %d != %d decoded bytes"
                % (adapter, case["id"], length, len(decoded)))
    return failures


def check_no_expected_copy(adapter, report, manifest):
    """An adapter must read the declared source, never an expected/golden file."""
    failures = []
    if not isinstance(report, dict) or not isinstance(report.get("cases"), list):
        return failures
    for case in report["cases"]:
        if not isinstance(case, dict):
            continue
        case_id = case.get("id")
        entry = manifest["entries"].get(case_id)
        if entry is None:
            continue
        source = case.get("source_path")
        expected_path = entry.get("expected_path")
        if isinstance(source, str) and isinstance(expected_path, str):
            if source == expected_path:
                failures.append(
                    "adapter %s: expected copy: case %s source_path %r points "
                    "at the expected file, not the source"
                    % (adapter, case_id, source))
                continue
        declared_source = entry.get("source_path")
        if isinstance(source, str) and isinstance(declared_source, str):
            if source != declared_source:
                failures.append(
                    "adapter %s: case %s source_path %r diverges from "
                    "manifest source %r (expected-copy guard)"
                    % (adapter, case_id, source, declared_source))
    return failures


# ---------------------------------------------------------------------------
# Report comparison (no subprocess knowledge here)
# ---------------------------------------------------------------------------

def canonical_value(value):
    if value is MISSING:
        return "<<missing>>"
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def compare_reports(reports, manifest):
    """Per-case, per-field comparison across every adapter present.

    Returns a list of structured divergences: {id, field, values}. All
    COMPARED_CASE_FIELDS are checked for every shared case id; comparing a
    subset (byte-only, digest-only) is not permitted by construction.
    """
    divergences = []
    present = [a for a in ADAPTER_ORDER if a in reports]
    if len(present) < 2:
        return divergences
    all_ids = set()
    by_adapter = {}
    for adapter in present:
        cases = reports[adapter].get("cases") or []
        mapping = {c["id"]: c for c in cases
                   if isinstance(c, dict) and isinstance(c.get("id"), str)}
        by_adapter[adapter] = mapping
        all_ids.update(mapping)
    for case_id in sorted(all_ids):
        for field in COMPARED_CASE_FIELDS:
            values = {a: by_adapter[a].get(case_id, {}).get(field, MISSING)
                      for a in present}
            if len({canonical_value(v) for v in values.values()}) > 1:
                divergences.append({
                    "id": case_id,
                    "field": field,
                    "values": {a: (None if v is MISSING else v)
                               for a, v in values.items()},
                })
    return divergences


def format_divergence(divergence):
    values = " ".join(
        "%s=%r" % (adapter, divergence["values"][adapter])
        for adapter in ADAPTER_ORDER if adapter in divergence["values"])
    return ("divergence: case=%s field=%s %s"
            % (divergence["id"], divergence["field"], values))


def compare_rounds(rounds_data):
    """The per-case conclusions of every round must be identical."""
    failures = []
    if len(rounds_data) < 2:
        return failures
    first = rounds_data[0]["reports"]
    for rnd in rounds_data[1:]:
        current = rnd["reports"]
        for adapter in ADAPTER_ORDER:
            if adapter not in first or adapter not in current:
                continue  # already failed as an adapter/runner failure
            prev_cases = {c.get("id"): c
                          for c in first[adapter].get("cases") or []}
            curr_cases = {c.get("id"): c
                          for c in current[adapter].get("cases") or []}
            for case_id in sorted(set(prev_cases) | set(curr_cases)):
                for field in COMPARED_CASE_FIELDS:
                    v1 = prev_cases.get(case_id, {}).get(field, MISSING)
                    v2 = curr_cases.get(case_id, {}).get(field, MISSING)
                    if canonical_value(v1) != canonical_value(v2):
                        failures.append(
                            "nondeterministic rerun: adapter %s case %s field "
                            "%s round1=%r round%d=%r"
                            % (adapter, case_id, field, v1, rnd["round"], v2))
                if (case_id in prev_cases) != (case_id in curr_cases):
                    failures.append(
                        "nondeterministic rerun: adapter %s case %s present "
                        "in round1=%s round%d=%s"
                        % (adapter, case_id, case_id in prev_cases,
                           rnd["round"], case_id in curr_cases))
    return failures


def per_case_conclusion(reports, manifest):
    """Evidence block: for every manifest case, each field's per-adapter view."""
    present = [a for a in ADAPTER_ORDER if a in reports]
    conclusion = []
    for case_id in sorted(manifest["ids"]):
        entry = {"id": case_id, "fields": {}}
        for field in COMPARED_CASE_FIELDS:
            values = {}
            for adapter in present:
                case = next((c for c in reports[adapter].get("cases") or []
                             if isinstance(c, dict) and c.get("id") == case_id), None)
                values[adapter] = None if case is None else case.get(field)
            entry["fields"][field] = {
                "all_equal": len({canonical_value(v) for v in values.values()}) <= 1,
                "values": values,
            }
        conclusion.append(entry)
    return conclusion


# ---------------------------------------------------------------------------
# Gate orchestration
# ---------------------------------------------------------------------------

def run_gate(repo_dirs, fixtures, evidence_out=None, runner=None,
             toolchain=None):
    """Run the whole gate; returns a result dict, never raises on bad input.

    ``runner`` (subprocess invocation) and the comparison functions are
    deliberately separate: tests inject fake runners here while the
    comparison logic stays pure over parsed report objects.
    """
    failures = []
    fixtures = Path(fixtures).resolve()
    if not fixtures.is_dir():
        return _result(False, ["fixtures directory not found: %s" % fixtures],
                       evidence_dir=None, toolchain=toolchain or {})
    evidence_dir = Path(evidence_out).resolve() if evidence_out else \
        Path(tempfile.mkdtemp(prefix="int001-s1-evidence-"))
    try:
        evidence_dir.relative_to(fixtures)
        return _result(False,
                       ["evidence directory %s is inside the fixtures tree %s; "
                        "refusing to write evidence near golden files"
                        % (evidence_dir, fixtures)],
                       evidence_dir=str(evidence_dir), toolchain=toolchain or {})
    except ValueError:
        pass
    evidence_dir.mkdir(parents=True, exist_ok=True)

    tools = {"go": resolve_tool("go"), "node": resolve_tool("node")}
    missing_tools = [n for n, p in tools.items() if p is None]
    if missing_tools:
        failures.append("required tools not found: %s" % ", ".join(missing_tools))

    if toolchain is None:
        toolchain, tool_failures = collect_toolchain()
        failures.extend(tool_failures)
    if runner is None:
        runner = SubprocessAdapterRunner(repo_dirs, tools)

    manifest = load_manifest(fixtures)
    failures.extend(manifest["failures"])

    before = snapshot_tree(fixtures)

    rounds_data = []
    for rnd in range(1, ROUNDS + 1):
        reports = {}
        for adapter in ADAPTER_ORDER:
            report_path = evidence_dir / ("%s.round%d.json" % (adapter, rnd))
            proc = runner(adapter, fixtures, report_path)
            if getattr(proc, "returncode", 1) != 0:
                stderr = (getattr(proc, "stderr", "") or "")[-2000:]
                failures.append(
                    "adapter %s round %d exited %s; stderr tail: %r"
                    % (adapter, rnd, getattr(proc, "returncode", "?"), stderr))
                continue
            try:
                report = json.loads(
                    Path(report_path).read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                failures.append(
                    "adapter %s round %d: report unreadable (%s): %s"
                    % (adapter, rnd, report_path, exc))
                continue
            reports[adapter] = report
        for adapter, report in reports.items():
            failures.extend(validate_report(adapter, report, manifest))
            failures.extend(check_no_expected_copy(adapter, report, manifest))
        divergences = compare_reports(reports, manifest)
        failures.extend(format_divergence(d) for d in divergences)
        rounds_data.append({"round": rnd, "reports": reports,
                            "divergences": divergences})

    failures.extend(compare_rounds(rounds_data))
    failures.extend(check_toolchain([rd["reports"] for rd in rounds_data],
                                    toolchain))

    after = snapshot_tree(fixtures)
    fixtures_immutable = before == after
    if not fixtures_immutable:
        changed = sorted(set(before) ^ set(after)) or [
            p for p in before if before[p] != after.get(p)]
        failures.append(
            "fixtures tree mutated during the run (files added/removed/"
            "changed: %s); golden files must never be written"
            % ", ".join(changed[:10]))

    comparison = _build_comparison(fixtures, manifest, toolchain, rounds_data,
                                   divergences, fixtures_immutable, failures,
                                   environment={
                                       "gocache": getattr(
                                           runner, "gocache_note",
                                           "injected runner (no go cache "
                                           "policy)")})
    comparison_path = evidence_dir / "comparison.json"
    comparison_path.write_text(
        json.dumps(comparison, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8")

    return _result(ok=not failures, failures=failures,
                   evidence_dir=str(evidence_dir), toolchain=toolchain,
                   comparison_path=str(comparison_path), comparison=comparison)


def _build_comparison(fixtures, manifest, toolchain, rounds_data,
                      divergences, fixtures_immutable, failures,
                      environment=None):
    last_reports = rounds_data[-1]["reports"] if rounds_data else {}
    present = [a for a in ADAPTER_ORDER if a in last_reports]
    case_sets = {}
    for adapter in present:
        case_sets[adapter] = sorted(
            {c.get("id") for c in last_reports[adapter].get("cases") or []
             if isinstance(c, dict)})
    sets_equal = len({json.dumps(case_sets.get(a)) for a in present}) <= 1
    return {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "gate": "INT-001",
        "generated_utc": datetime.datetime.now(
            datetime.timezone.utc).isoformat(timespec="seconds"),
        "fixtures_root": str(fixtures),
        "manifest": {
            "path": str(fixtures / MANIFEST_NAME),
            "sha256": manifest["sha256"],
            "case_count": manifest["count"],
        },
        "toolchain": toolchain,
        "environment": environment or {},
        "rounds": [
            {"round": rd["round"],
             "report_files": ["%s.round%d.json" % (a, rd["round"])
                              for a in ADAPTER_ORDER],
             "case_count": {a: len(rd["reports"][a].get("cases") or [])
                            for a in rd["reports"]},
             "divergences": rd["divergences"]}
            for rd in rounds_data
        ],
        "report_copies": rounds_data[0]["reports"] if rounds_data else {},
        "comparison": {
            "compared_fields": list(COMPARED_CASE_FIELDS),
            "case_set_equal_across_adapters": sets_equal,
            "case_sets": case_sets,
            "per_case_conclusion": per_case_conclusion(last_reports, manifest),
            "divergences": divergences,
        },
        "determinism": {
            "rounds": ROUNDS,
            "consistent": not any("nondeterministic" in f for f in failures),
        },
        "fixtures_immutable": fixtures_immutable,
        "all_passed": not failures,
        "failures": failures,
    }


def _result(ok, failures, evidence_dir, toolchain, comparison_path=None,
            comparison=None):
    return {
        "ok": ok,
        "failures": list(failures),
        "evidence_dir": evidence_dir,
        "comparison_path": comparison_path,
        "comparison": comparison,
        "toolchain": toolchain,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="INT-001 S1 Go<->TS conformance gate")
    parser.add_argument("--host", required=True,
                        help="pi-group-chat-host repository directory")
    parser.add_argument("--gms", required=True,
                        help="graph-memory-service repository directory")
    parser.add_argument("--rsih", required=True,
                        help="RSI-Harness repository directory")
    parser.add_argument("--fixtures", required=True,
                        help="shared conformance fixtures directory")
    parser.add_argument("--evidence-out", default=None,
                        help="evidence directory (default: fresh temp dir; "
                             "must live outside the fixtures tree)")
    args = parser.parse_args(argv)

    repo_dirs = {"host": args.host, "gms": args.gms, "rsih": args.rsih}
    for name, path in repo_dirs.items():
        if not Path(path).is_dir():
            print("INT-001 S1 gate: RED")
            print("failure: %s repository directory not found: %s" % (name, path))
            return 1

    result = run_gate(repo_dirs=repo_dirs, fixtures=args.fixtures,
                      evidence_out=args.evidence_out)

    toolchain = result.get("toolchain") or {}
    print("INT-001 S1 gate: %s" % ("GREEN" if result["ok"] else "RED"))
    print("adapters: host=%s gms=%s rsih=%s" % (
        args.host, args.gms, args.rsih))
    print("fixtures: %s" % args.fixtures)
    print("toolchain: go=%s node=%s python=%s" % (
        toolchain.get("go"), toolchain.get("node"), toolchain.get("python")))
    comparison = result.get("comparison") or {}
    environment = comparison.get("environment") or {}
    if environment.get("gocache"):
        print("go cache: %s" % environment["gocache"])
    manifest_info = comparison.get("manifest") or {}
    if manifest_info.get("case_count") is not None:
        print("manifest: sha256:%s cases=%s" % (
            (manifest_info.get("sha256") or "")[:16],
            manifest_info.get("case_count")))
    determinism = comparison.get("determinism")
    if determinism:
        print("determinism: %d rounds, consistent=%s" % (
            determinism["rounds"], determinism["consistent"]))
    if result.get("comparison_path"):
        print("evidence: %s" % result["evidence_dir"])
        print("comparison: %s" % result["comparison_path"])
    for failure in result["failures"]:
        print("failure: %s" % failure)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
