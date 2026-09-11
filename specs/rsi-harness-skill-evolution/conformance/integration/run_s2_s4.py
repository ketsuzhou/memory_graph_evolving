#!/usr/bin/env python3
"""INT-002 -- S2-S4 Host/GMS/RSIH conformance tracer.

Drives the three contract drivers (Host cmd/s2s4-driver, GMS
cmd/s2s4-driver, RSIH scripts/s2s4-driver.ts) over the frozen shared
fixtures and traces the authoritative chain end to end:

    Host ToolProxy/DAG seal (S2) -> Host segment Close + GMS Evidence/
    Candidate commit (S3) -> GMS ReplayRequest -> Host replay Plan ->
    RSIH raw runs -> GMS ReplayResult canonicalization (S4)

Fail-closed by construction. Any of the following turns the tracer red:
a driver protocol violation or op failure; a Pi result that diverges from
the frozen ToolProxy result bytes, arrives after tool_execution_end or is
a placeholder acceptance; a post-end upstream rewrite of a frozen
terminal; a mutable seal (append after settled, idempotency conflict,
seal digest mismatch); GMS evidence whose digest does not equal the Host
seal digest, unsealed/uncommitted evidence that commits or leaks into
reads, candidate artifacts leaking into Runtime execution; a replay
request that does not pin the fixture manifest; Host plan or RSIH raw
digests that diverge from the fixture expectations; missing families,
cross-run contamination, nondeterministic replay (including a later
transcript changing a replay digest); scoring/U1/release semantics
smuggled into a raw driver output; or any mutation of the fixtures tree.

Evidence (per-stage driver responses, the negative matrix, the digest
chain and the summary) is written only to a directory outside the
fixtures tree. Exit code 0 = green, 1 = red.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SUMMARY_SCHEMA_VERSION = "rsih-int002-s2s4-summary.v1"

# Stages of the authoritative digest chain (evidence digest_chain.json).
CHAIN_STAGES = (
    "s2_toolproxy", "s3_segment", "s3_evidence", "s3_candidate",
    "s4_request", "s4_host_plan", "s4_rsih_raw", "s4_gms_result",
)

# Keys no raw driver response may ever carry (Host §6.7 / RSIH §6.5 /
# GMS §5.4: scoring, U1 and release semantics live in the decision layer,
# never in raw execution or canonicalization outputs). Substring match on
# object keys, recursive over the whole response.
FORBIDDEN_SCORING_TOKENS = (
    "u1", "release", "winner", "utility_vector", "recommend", "score",
)

GO_FALLBACK_PATHS = (
    os.path.join(os.path.expanduser("~"), "go", "bin"),
    "/usr/local/go/bin",
)

HERMETIC_GOCACHE = os.path.join(tempfile.gettempdir(), "int002-gocache-rsih")

SETTLED_PROBES = ("append_after_settled", "idempotency_conflict",
                  "seal_mismatch")


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------

def sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def jcs(obj) -> str:
    """RFC 8785-shaped canonical JSON for integer-only documents."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def resolve_tool(name):
    found = shutil.which(name)
    if found:
        return found
    if name == "go":
        for directory in GO_FALLBACK_PATHS:
            candidate = os.path.join(directory, name)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
    return None


def snapshot_tree(root):
    """Map every file under root (relative posix path -> sha256 hex)."""
    root = Path(root)
    snapshot = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            digest = hashlib.sha256()
            try:
                with open(path, "rb") as handle:
                    for chunk in iter(lambda: handle.read(1 << 16), b""):
                        digest.update(chunk)
            except OSError:
                continue
            snapshot[path.relative_to(root).as_posix()] = digest.hexdigest()
    return snapshot


def forbidden_scoring_keys(value, prefix=""):
    """Deep scan for scoring semantics smuggled into a driver response."""
    found = []
    if isinstance(value, list):
        for item in value:
            found.extend(forbidden_scoring_keys(item, prefix))
        return found
    if not isinstance(value, dict):
        return found
    for key, sub in value.items():
        key_str = key if isinstance(key, str) else str(key)
        for token in FORBIDDEN_SCORING_TOKENS:
            if token in key_str:
                found.append(prefix + key_str)
                break
        found.extend(forbidden_scoring_keys(sub, prefix + key_str + "."))
    return found


def collect_toolchain():
    toolchain, failures = {}, []
    specs = (
        ("go", ["go", "version"],
         lambda out: out.split()[2] if len(out.split()) >= 3 else out.strip()),
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
            failures.append("toolchain: %s version probe failed: %s"
                            % (name, exc))
            continue
        if proc.returncode != 0:
            toolchain[name] = None
            failures.append("toolchain: %s version probe exited %d"
                            % (name, proc.returncode))
            continue
        toolchain[name] = parse(proc.stdout.strip()) or None
    return toolchain, failures


# ---------------------------------------------------------------------------
# Real driver sessions (subprocess side)
# ---------------------------------------------------------------------------

class SubprocessDriver:
    """One long-lived driver process speaking the line-JSON stdio protocol."""

    def __init__(self, name, argv, cwd, env=None):
        self.name = name
        self.argv = argv
        self.cwd = cwd
        self.env = env
        self.counter = 0
        self.proc = None
        self.stderr_path = None
        self.launch_error = None

    def _ensure_started(self):
        if self.proc is not None or self.launch_error is not None:
            return
        if not self.argv:
            self.launch_error = self.launch_error or "no driver command"
            return
        try:
            self.stderr_path = tempfile.NamedTemporaryFile(
                prefix="int002-%s-stderr-" % self.name, delete=False)
            self.proc = subprocess.Popen(
                self.argv, cwd=self.cwd, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=self.stderr_path,
                env=self.env, text=True)
        except OSError as exc:
            self.launch_error = "cannot launch %s driver (%s): %s" % (
                self.name, " ".join(self.argv), exc)

    def call(self, op, params):
        self._ensure_started()
        if self.launch_error is not None:
            return {"ok": False, "error": {"code": "DRIVER_LAUNCH_FAILED",
                                           "reason": self.launch_error}}
        self.counter += 1
        instruction = dict(params)
        instruction["op"] = op
        instruction["id"] = self.counter
        try:
            self.proc.stdin.write(json.dumps(instruction) + "\n")
            self.proc.stdin.flush()
            line = self.proc.stdout.readline()
        except (OSError, ValueError) as exc:
            return {"ok": False, "error": {"code": "DRIVER_PROTOCOL_VIOLATION",
                                           "reason": "stdio broken: %s" % exc}}
        if not line.strip():
            return {"ok": False,
                    "error": {"code": "DRIVER_PROTOCOL_VIOLATION",
                              "reason": "driver exited before responding "
                                        "(op %s)" % op}}
        try:
            response = json.loads(line)
        except json.JSONDecodeError as exc:
            return {"ok": False,
                    "error": {"code": "DRIVER_PROTOCOL_VIOLATION",
                              "reason": "unparseable response line for op "
                                        "%s: %s" % (op, exc)}}
        if not isinstance(response, dict):
            return {"ok": False,
                    "error": {"code": "DRIVER_PROTOCOL_VIOLATION",
                              "reason": "response for op %s is not an "
                                        "object" % op}}
        return response

    def close(self):
        if self.proc is not None:
            try:
                self.proc.stdin.close()
                self.proc.wait(timeout=30)
            except (OSError, subprocess.SubprocessError):
                self.proc.kill()
        if self.stderr_path is not None:
            try:
                self.stderr_path.close()
                os.unlink(self.stderr_path.name)
            except OSError:
                pass
        return None


class RealDriverKit:
    """Builds the three real driver subprocesses (go build once, stdio)."""

    def __init__(self, gocache_note="inherited from environment"):
        self.gocache_note = gocache_note
        self.env = dict(os.environ)
        if "GOCACHE" not in self.env:
            os.makedirs(HERMETIC_GOCACHE, exist_ok=True)
            self.env["GOCACHE"] = HERMETIC_GOCACHE
            self.gocache_note = "hermetic %s (GOCACHE was unset)" % HERMETIC_GOCACHE
        self.sessions = {}

    def _build_go_driver(self, repo, name):
        binary = os.path.join(tempfile.mkdtemp(prefix="int002-driver-"),
                              "s2s4-driver-" + name)
        proc = subprocess.run(
            [resolve_tool("go"), "build", "-o", binary, "./cmd/s2s4-driver"],
            cwd=repo, capture_output=True, text=True, check=False,
            env=self.env, timeout=600)
        if proc.returncode != 0:
            raise RuntimeError("go build %s/cmd/s2s4-driver exited %d: %s"
                               % (repo, proc.returncode, proc.stderr[-800:]))
        return binary

    def __call__(self, name, tools, fixtures, repo_dirs):
        if name in ("host", "gms"):
            try:
                binary = self._build_go_driver(repo_dirs[name], name)
                driver = SubprocessDriver(name, [binary], repo_dirs[name],
                                          env=self.env)
            except (RuntimeError, subprocess.SubprocessError) as exc:
                driver = SubprocessDriver(name, [], repo_dirs.get(name) or ".")
                driver.launch_error = str(exc)
        else:
            argv = [tools.get("node") or "node", "--experimental-strip-types",
                    os.path.join("scripts", "s2s4-driver.ts")]
            driver = SubprocessDriver(name, argv, repo_dirs["rsih"],
                                      env=self.env)
        self.sessions[name] = driver
        return driver


class _FailingFactory:
    """Factory fallback when required tools are missing (fail-closed)."""

    def __init__(self, reason):
        self.reason = reason
        self.sessions = {}

    def __call__(self, name, tools, fixtures, repo_dirs):
        driver = SubprocessDriver(name, [], ".")
        driver.launch_error = self.reason
        self.sessions[name] = driver
        return driver


# ---------------------------------------------------------------------------
# Frozen fixtures (read-only authority the tracer cross-checks against)
# ---------------------------------------------------------------------------

class Fixtures:
    def __init__(self, root):
        self.root = Path(root)
        self.tools_manifest = load_json(self.root / "tools" / "manifest.json")
        self.recorded_manifest = load_json(
            self.root / "recorded" / "manifest.json")
        self.manifest_bytes = (self.root / "recorded" /
                               "manifest.json").read_bytes()
        self.manifest_digest = sha(self.manifest_bytes)
        self.tool_expectations = {}
        for case in self.tools_manifest["cases"]:
            self.tool_expectations[case["case_id"]] = load_json(
                self.root / "tools" / case["expected_path"])
        self.segments = {}
        for case in self.recorded_manifest["cases"]:
            if case["corpus"] != "segments":
                continue
            case_dir = self.root / "recorded" / case["path"]
            segment = load_json(case_dir / "segment.json")
            seal = load_json(case_dir / "seal.json")
            self.segments[case["case_id"]] = {
                "manifest": case,
                "segment": segment,
                "seal": seal,
                "terminal_state": segment.get("terminal_state"),
                "expect_reason": case.get("expected_reason_code"),
            }
        self.packets = {}
        reserved = set(self.recorded_manifest.get(
            "q29b_reserved_families", []))
        for case in self.recorded_manifest["cases"]:
            if case["corpus"] != "q29b" or case.get("family") in reserved:
                continue
            # Reference semantics (validate_recorded.py / the RSIH
            # loader): a reserved-outcome entry is a placeholder directory
            # -- it never carries a runnable runtime.json and never enters
            # the replay corpus.
            if case.get("expected_outcome") == "reserved":
                continue
            if case.get("family") in self.packets:
                # A family with several recorded packets (the GMS-208 merge
                # pair) replays through its FIRST packet -- the packet the
                # RSIH runner resolves for the family name.
                continue
            runtime = load_json(
                self.root / "recorded" / case["path"] / "runtime.json")
            self.packets[case["family"]] = {
                "case": case,
                "runtime": runtime,
                "packet_id": runtime["packet_id"],
                "expected_digest": runtime["expected"]["output_digest"],
                "expected_status": runtime["terminal"]["status"],
                "expected_reason": runtime["terminal"].get("reason_code") or "",
            }
        self.reserved_families = list(
            self.recorded_manifest.get("q29b_reserved_families", []))
        self.replay_families = [f for f in
                                self.recorded_manifest["q29b_families"]
                                if f not in self.reserved_families]

    def settled_segment_cases(self):
        return sorted(cid for cid, data in self.segments.items()
                      if data["terminal_state"] == "settled")

    def unsettled_segment_cases(self):
        return sorted(cid for cid, data in self.segments.items()
                      if data["terminal_state"] != "settled")


# ---------------------------------------------------------------------------
# The tracer
# ---------------------------------------------------------------------------

class Tracer:
    def __init__(self, repo_dirs, fixtures, evidence_dir, drivers, toolchain,
                 tools):
        self.repo_dirs = dict(repo_dirs or {})
        self.fixtures = fixtures
        self.evidence_dir = evidence_dir
        self.drivers = drivers
        self.toolchain = toolchain
        self.tools = tools
        self.failures = []
        self.slice_failures = {"s2": 0, "s3": 0, "s4": 0}
        self.evidence = {}
        self.digest_chain = {stage: {} for stage in CHAIN_STAGES}
        self.negatives = []
        self._sessions = {}

    # -- plumbing ----------------------------------------------------------

    def fail(self, slice_name, message):
        self.failures.append(message)
        if slice_name in self.slice_failures:
            self.slice_failures[slice_name] += 1

    def record_negative(self, name, expected, observed, rejected):
        self.negatives.append({"name": name, "expected": expected,
                               "observed": observed, "rejected": rejected})

    def _session_of(self, name):
        if name not in self._sessions:
            self._sessions[name] = self.drivers(
                name, self.tools, str(self.fixtures.root), self.repo_dirs)
        return self._sessions[name]

    def call(self, driver_name, op, params, slice_name, context):
        """One driver instruction with protocol, scoring and evidence checks."""
        session = self._session_of(driver_name)
        instruction = dict(params)
        instruction.setdefault("fixtures", str(self.fixtures.root))
        try:
            response = session.call(op, instruction)
        except Exception as exc:  # fake drivers may assert on unknown ops
            self.fail(slice_name, "driver %s instruction %s raised: %r"
                      % (driver_name, op, exc))
            return None
        if not isinstance(response, dict):
            self.fail(slice_name, "driver %s instruction %s returned a "
                                  "non-object response" % (driver_name, op))
            return None
        smuggled = forbidden_scoring_keys(response)
        if smuggled:
            self.fail(slice_name,
                      "scoring semantics leaked into raw %s output (op %s): "
                      "keys %s -- U1/release scoring never belongs in raw "
                      "driver facts" % (driver_name, op,
                                        ", ".join(sorted(set(smuggled)))))
        if response.get("ok") is not True:
            error = response.get("error") or {}
            code = error.get("code") or "DRIVER_PROTOCOL_VIOLATION"
            reason = error.get("reason") or "no reason given"
            self.fail(slice_name, "driver %s instruction %s (%s) failed: "
                      "%s: %s" % (driver_name, op, context, code, reason))
            self.record_negative("driver_" + op, "ok=true",
                                 {"code": code, "reason": reason}, True)
            return None
        self.evidence.setdefault("driver_calls", []).append(
            {"driver": driver_name, "op": op, "context": context,
             "response": _trim_response(response)})
        return response

    # -- S2: same-call ToolProxy -------------------------------------------

    def run_s2(self):
        fx = self.fixtures
        cases = [c["case_id"] for c in fx.tools_manifest["cases"]]
        if not cases:
            self.fail("s2", "tools manifest declares no cases")
        s2_evidence = []
        accept_case = None
        for case_id in cases:
            expectation = fx.tool_expectations[case_id]
            expect_accept = bool(expectation.get("expected_accept"))
            expect_digest = expectation.get("expected_result_digest")
            response = self.call("host", "toolproxy.invoke",
                                 {"case_id": case_id, "mode": "ok"},
                                 "s2", "tool case " + case_id)
            if response is None:
                continue
            entry = {"case_id": case_id, "expected_accept": expect_accept}
            s2_evidence.append(entry)

            if response.get("expected_accept") is not expect_accept:
                self.fail("s2", "tool case %s: driver reports "
                          "expected_accept=%r but the frozen fixture "
                          "declares %r"
                          % (case_id, response.get("expected_accept"),
                             expect_accept))
            status = response.get("status")
            if expect_accept:
                if status != "succeeded":
                    self.fail("s2", "tool case %s: frozen accept case ended "
                              "%r (reason %r)"
                              % (case_id, status, response.get("reason_code")))
                payload = response.get("result_payload_digest")
                if payload != expect_digest:
                    self.fail("s2", "tool case %s: accepted payload digest "
                              "%r does not match the frozen expectation %r"
                              % (case_id, payload, expect_digest))
                if accept_case is None:
                    accept_case = case_id
                if payload:
                    self.digest_chain["s2_toolproxy"][
                        case_id + "_payload_digest"] = payload
                entry["payload_digest"] = payload
            else:
                if status == "succeeded":
                    self.fail("s2", "tool case %s: frozen reject case was "
                              "accepted (placeholder result smuggled "
                              "through the proxy)" % case_id)
                payload = response.get("result_payload_digest")
                if payload is not None:
                    self.fail("s2", "tool case %s: rejected case carries a "
                              "result payload digest (placeholder result)"
                              % case_id)
                canonical = response.get("canonical_digest") or \
                    response.get("proxy_result_digest")
                if canonical:
                    self.digest_chain["s2_toolproxy"][
                        case_id + "_canonical_digest"] = canonical
                entry["canonical_digest"] = canonical

            # Exactly-once, same-call delivery into the Pi return channel.
            if response.get("pi_deliveries") != 1:
                self.fail("s2", "tool case %s: Pi return channel saw %r "
                          "deliveries (must be exactly once)"
                          % (case_id, response.get("pi_deliveries")))
            if response.get("pi_bytes_equal_result") is not True:
                self.fail("s2", "tool case %s: Pi bytes diverge from the "
                          "frozen ToolProxy result bytes (pi digest %r)"
                          % (case_id, response.get("pi_digest")))
            if response.get("delivered_before_end") is not True:
                self.fail("s2", "tool case %s: result was NOT completed "
                          "before tool_execution_end (post-end delivery)"
                          % case_id)
            # Accepted cases invoke the upstream exactly once. Reject cases
            # may refuse before invocation (e.g. TOOL_UNSUPPORTED: the tool
            # is outside the closed set, zero calls is the fail-closed
            # outcome) or invoke once and reject the payload -- but never
            # more than once.
            upstream = response.get("upstream_calls")
            if expect_accept and upstream != 1:
                self.fail("s2", "tool case %s: upstream invoked %r times "
                          "(must be exactly once)"
                          % (case_id, upstream))
            if upstream not in (0, 1):
                self.fail("s2", "tool case %s: upstream invoked %r times "
                          "(same-call proxy must never re-invoke)"
                          % (case_id, upstream))
            if response.get("late_entries"):
                self.fail("s2", "tool case %s: %r late upstream entries on "
                          "the same-call path"
                          % (case_id, response.get("late_entries")))
            entry.update({
                "status": status,
                "reason_code": response.get("reason_code"),
                "pi_deliveries": response.get("pi_deliveries"),
                "pi_bytes_equal_result": response.get("pi_bytes_equal_result"),
                "delivered_before_end": response.get("delivered_before_end"),
            })

        # The post-end negative: the late upstream success must stay
        # audit-only; the frozen terminal may never be rewritten.
        late_case = accept_case or (cases[0] if cases else None)
        if late_case is not None:
            response = self.call("host", "toolproxy.invoke",
                                 {"case_id": late_case, "mode": "late_rewrite"},
                                 "s2", "late rewrite " + late_case)
            if response is not None:
                first_digest = response.get("first_digest")
                if first_digest:
                    self.digest_chain["s2_toolproxy"][
                        "late_rewrite_terminal_digest"] = first_digest
                checks = (
                    (response.get("first_status") == "failed",
                     "late-rewrite first terminal not failed"),
                    (bool(response.get("first_reason_code")),
                     "late-rewrite first terminal carries no reason code"),
                    ((response.get("late_entries") or 0) >= 1,
                     "late upstream success left no audit entry"),
                    (response.get("terminal_status_after_late") == "failed",
                     "terminal status changed after the late upstream result"),
                    (response.get("terminal_digest_after_late") == first_digest,
                     "post-end upstream result rewrote the frozen terminal "
                     "digest (%r -> %r)"
                     % (first_digest,
                        response.get("terminal_digest_after_late"))),
                    (response.get("replay_digest") == first_digest,
                     "post-end upstream result rewrote the idempotent replay "
                     "digest (%r -> %r)"
                     % (first_digest, response.get("replay_digest"))),
                    (response.get("replay_canonical_equal") is True,
                     "post-end upstream result changed the replay canonical "
                     "bytes"),
                    ((response.get("pi_deliveries") or 0) <= 1,
                     "post-end upstream result reached Pi a second time "
                     "(pi_deliveries=%r)" % response.get("pi_deliveries")),
                    (response.get("upstream_calls_after_replay") == 1,
                     "replay re-invoked the upstream (calls=%r)"
                     % response.get("upstream_calls_after_replay")),
                )
                for ok, message in checks:
                    if not ok:
                        self.fail("s2", "tool case %s (late_rewrite): %s"
                                  % (late_case, message))
                self.record_negative(
                    "s2_late_rewrite",
                    "terminal frozen, late result audit-only",
                    {"terminal_digest_after_late":
                     response.get("terminal_digest_after_late"),
                     "replay_digest": response.get("replay_digest"),
                     "pi_deliveries": response.get("pi_deliveries")},
                    rejected=(response.get("terminal_digest_after_late")
                              == first_digest))
        self.evidence["s2_toolproxy"] = s2_evidence

    # -- S3: segment close + GMS evidence/candidate -------------------------

    def run_s3(self):
        fx = self.fixtures
        segments_evidence = []
        settled_refs = {}
        for case_id in sorted(fx.segments):
            data = fx.segments[case_id]
            seal = data["seal"]
            terminal_state = data["terminal_state"]
            params = {"case_id": case_id}
            if terminal_state == "settled":
                params["probes"] = list(SETTLED_PROBES)
            response = self.call("host", "segment.close", params, "s3",
                                 "segment " + case_id)
            if response is None:
                continue
            entry = {"case_id": case_id,
                     "fixture_terminal_state": terminal_state}
            segments_evidence.append(entry)
            negative = data["expect_reason"] is not None
            expect_zero_refs = negative or terminal_state != "settled"

            if bool(response.get("zero_refs")) is not expect_zero_refs:
                self.fail("s3", "segment %s: zero_refs=%r but the fixture "
                          "terminal is %r (manifest reason %r)"
                          % (case_id, response.get("zero_refs"),
                             terminal_state, data["expect_reason"]))
            if negative:
                audit = response.get("terminal_audit") or {}
                if audit.get("reason_code") != data["expect_reason"]:
                    self.fail("s3", "segment %s: close audit reason %r != "
                              "manifest %r"
                              % (case_id, audit.get("reason_code"),
                                 data["expect_reason"]))
                self.record_negative(
                    "s3_close_" + case_id, data["expect_reason"],
                    audit.get("reason_code"),
                    rejected=audit.get("reason_code") == data["expect_reason"])
                entry["terminal_audit"] = audit
                continue
            if expect_zero_refs:
                entry["terminal_state"] = response.get("terminal_state")
                continue

            refs = response.get("refs") or {}
            seg_ref = refs.get("segment_ref") or {}
            evidence_seal = refs.get("evidence_seal") or {}
            entry["terminal_state"] = response.get("terminal_state")
            entry["segment_digest"] = response.get("segment_digest")
            # The Host-close digests must equal the frozen seal record.
            if response.get("segment_digest") != seal.get("segment_digest"):
                self.fail("s3", "segment %s: Host close digest %r != frozen "
                          "seal record %r"
                          % (case_id, response.get("segment_digest"),
                             seal.get("segment_digest")))
            if seg_ref.get("segment_digest") != seal.get("segment_digest"):
                self.fail("s3", "segment %s: SegmentRef digest diverges from "
                          "the frozen seal record" % case_id)
            if evidence_seal.get("seal_digest") != \
                    (seal.get("evidence_seal") or {}).get("seal_digest"):
                self.fail("s3", "segment %s: Evidence Seal digest %r != "
                          "frozen seal record %r (seal digest mismatch)"
                          % (case_id, evidence_seal.get("seal_digest"),
                             (seal.get("evidence_seal") or {}).get(
                                 "seal_digest")))
            if response.get("seal_readback_digest") != \
                    response.get("segment_digest"):
                self.fail("s3", "segment %s: seal readback digest diverges "
                          "from the close digest" % case_id)
            settled_refs[case_id] = {
                "segment_digest": response.get("segment_digest"),
                "evidence_seal_digest": evidence_seal.get("seal_digest"),
                "checkpoint_digest":
                    (refs.get("checkpoint_ref") or {}).get("checkpoint_digest"),
                "path_seal_digest":
                    (refs.get("path_seal") or {}).get("seal_digest"),
            }
            if response.get("segment_digest"):
                self.digest_chain["s3_segment"][
                    case_id + "_segment_digest"] = response["segment_digest"]
            if evidence_seal.get("seal_digest"):
                self.digest_chain["s3_segment"][
                    case_id + "_evidence_seal_digest"] = \
                    evidence_seal["seal_digest"]

            # Mutable-seal probes: every one must be rejected fail-closed.
            probes = response.get("probes") or {}
            for probe in SETTLED_PROBES:
                outcome = probes.get(probe) or {}
                if outcome.get("rejected") is not True:
                    self.fail("s3", "segment %s: mutable seal -- probe %s "
                              "was not rejected (code %r)"
                              % (case_id, probe, outcome.get("code")))
                    self.record_negative("s3_probe_" + probe + "_" + case_id,
                                         "rejected", outcome, False)
                else:
                    self.record_negative("s3_probe_" + probe + "_" + case_id,
                                         "rejected", outcome, True)
            entry["probes"] = probes

        # GMS evidence admission over the settled corpus.
        evidence_evidence = []
        for case_id in sorted(settled_refs):
            host_refs = settled_refs[case_id]
            response = self.call("gms", "evidence.admit",
                                 {"case_id": case_id, "mode": "commit"}, "s3",
                                 "evidence commit " + case_id)
            if response is None:
                continue
            entry = {"case_id": case_id}
            evidence_evidence.append(entry)
            committed = response.get("committed") or {}
            if response.get("commit_rejected") or not committed:
                self.fail("s3", "evidence %s: settled segment evidence was "
                          "not committed (code %r)"
                          % (case_id, response.get("commit_code")))
                continue
            # The authoritative S3 chain link: the GMS EvidenceRef digest
            # must equal the Host Evidence Seal digest, and the committed
            # segment digest must equal the Host close digest.
            if committed.get("evidence_digest") != \
                    host_refs["evidence_seal_digest"]:
                self.fail("s3", "evidence %s: GMS EvidenceRef digest %r != "
                          "Host Evidence Seal digest %r (seal digest "
                          "mismatch on the authority chain)"
                          % (case_id, committed.get("evidence_digest"),
                             host_refs["evidence_seal_digest"]))
            if committed.get("segment_digest") != \
                    host_refs["segment_digest"]:
                self.fail("s3", "evidence %s: GMS committed segment digest "
                          "%r != Host close digest %r"
                          % (case_id, committed.get("segment_digest"),
                             host_refs["segment_digest"]))
            for flag in ("chain_segment_digest_match",
                         "chain_evidence_digest_match"):
                if response.get(flag) is not True:
                    self.fail("s3", "evidence %s: driver reports %s=%r"
                              % (case_id, flag, response.get(flag)))
            entry["committed"] = committed
            if committed.get("evidence_digest"):
                self.digest_chain["s3_evidence"][
                    case_id + "_evidence_digest"] = \
                    committed["evidence_digest"]

        settled_ids = sorted(settled_refs)
        first_settled = settled_ids[0] if settled_ids else None

        # Uncommitted evidence must be invisible to readers before commit.
        if first_settled:
            response = self.call("gms", "evidence.admit",
                                 {"case_id": first_settled,
                                  "mode": "stage_only"}, "s3",
                                 "evidence stage-only " + first_settled)
            if response is not None:
                lookup = response.get("lookup_after_stage")
                found = bool((lookup or {}).get("found")) or \
                    bool(response.get("visible_before_commit"))
                if found:
                    self.fail("s3", "evidence %s: uncommitted evidence "
                              "leaked into committed reads before commit "
                              "(lookup %r)" % (first_settled, lookup))
                self.record_negative(
                    "s3_stage_only_visibility", "not found before commit",
                    lookup, rejected=not found)
                evidence_evidence.append({
                    "case_id": first_settled, "mode": "stage_only",
                    "lookup_after_stage": lookup,
                })

        # Non-settled (unsealed) evidence must never commit.
        unsettled = fx.unsettled_segment_cases()
        if unsettled:
            case_id = unsettled[0]
            response = self.call("gms", "evidence.admit",
                                 {"case_id": case_id, "mode": "non_settled"},
                                 "s3", "evidence non-settled " + case_id)
            if response is not None:
                ok = (response.get("commit_rejected") is True and
                      response.get("valid") is not True)
                if not ok:
                    self.fail("s3", "evidence %s: unsealed (non-settled) "
                              "segment evidence was committed (valid=%r "
                              "commit_rejected=%r committed=%r)"
                              % (case_id, response.get("valid"),
                                 response.get("commit_rejected"),
                                 bool(response.get("committed"))))
                self.record_negative(
                    "s3_non_settled_commit", "commit rejected",
                    {"commit_rejected": response.get("commit_rejected"),
                     "reason_code": response.get("reason_code"),
                     "commit_code": response.get("commit_code")}, ok)
                evidence_evidence.append({
                    "case_id": case_id, "mode": "non_settled",
                    "reason_code": response.get("reason_code"),
                    "commit_code": response.get("commit_code"),
                })

        # One flipped seal byte must fail the commit closed.
        if first_settled:
            response = self.call("gms", "evidence.admit",
                                 {"case_id": first_settled, "mode": "tamper"},
                                 "s3", "evidence tamper " + first_settled)
            if response is not None:
                ok = (response.get("commit_rejected") is True and
                      response.get("reason_code") in
                      ("SEAL_DIGEST_MISMATCH", "SEGMENT_NOT_SETTLED"))
                if not ok:
                    self.fail("s3", "evidence %s: tampered seal (one byte "
                              "flipped) was admitted (reason %r commit %r)"
                              % (first_settled, response.get("reason_code"),
                                 response.get("commit_code")))
                self.record_negative(
                    "s3_seal_tamper", "SEAL_DIGEST_MISMATCH",
                    {"reason_code": response.get("reason_code"),
                     "commit_code": response.get("commit_code")}, ok)
                evidence_evidence.append({
                    "case_id": first_settled, "mode": "tamper",
                    "reason_code": response.get("reason_code"),
                })

        # GMS-202: the immutable candidate, bound only over committed
        # evidence, never executable as Runtime input.
        candidate_evidence = {}
        ok_bind = self.call("gms", "candidate.bind", {"mode": "ok"}, "s3",
                            "candidate bind")
        if ok_bind is not None:
            if ok_bind.get("bound") is not True:
                self.fail("s3", "candidate.bind(ok) did not bind over "
                          "committed evidence (code %r)"
                          % ok_bind.get("bind_code"))
            else:
                ref = ok_bind.get("candidate_ref") or {}
                candidate_evidence["candidate_ref"] = ref
                if ref.get("body_digest"):
                    self.digest_chain["s3_candidate"][
                        "candidate_body_digest"] = ref["body_digest"]
                if ok_bind.get("runtime_input_refused") is not True:
                    self.fail("s3", "candidate live leakage into Runtime: "
                              "bound candidate was handed out as runtime "
                              "input (code %r)"
                              % ok_bind.get("runtime_input_code"))
                    self.record_negative("s3_candidate_runtime_input",
                                         "refused",
                                         ok_bind.get("runtime_input_code"),
                                         False)
                else:
                    self.record_negative("s3_candidate_runtime_input",
                                         "refused",
                                         ok_bind.get("runtime_input_code"),
                                         True)
                if ok_bind.get("chain_evidence_digest_match") is not True:
                    self.fail("s3", "candidate.bind chain check failed")
        bad_bind = self.call("gms", "candidate.bind", {"mode": "uncommitted"},
                             "s3", "candidate bind uncommitted")
        if bad_bind is not None:
            ok = (bad_bind.get("bound") is not True and
                  bad_bind.get("bind_code") == "EVIDENCE_NOT_COMMITTED")
            if not ok:
                self.fail("s3", "uncommitted evidence supported a candidate "
                          "binding (bound=%r code=%r)"
                          % (bad_bind.get("bound"), bad_bind.get("bind_code")))
            self.record_negative("s3_candidate_uncommitted",
                                 "EVIDENCE_NOT_COMMITTED",
                                 bad_bind.get("bind_code"), ok)
            candidate_evidence["uncommitted"] = {
                "bound": bad_bind.get("bound"),
                "bind_code": bad_bind.get("bind_code"),
            }
        self.evidence["s3_segments"] = segments_evidence
        self.evidence["s3_evidence"] = evidence_evidence
        self.evidence["s3_candidate"] = candidate_evidence

    # -- S4: replay request -> plan -> raw runs -> result -------------------

    def run_s4(self):
        fx = self.fixtures
        built = self.call("gms", "replay.build_request", {}, "s4",
                          "build replay request")
        if built is None:
            return
        request_doc = built.get("request_doc") or {}
        families = built.get("families") or []
        by_name = {f.get("name"): f for f in families if isinstance(f, dict)}

        # The request must pin the frozen fixture manifest.
        manifest_digest = fx.manifest_digest
        if built.get("fixture_manifest_digest") != manifest_digest:
            self.fail("s4", "replay request fixture manifest digest %r != "
                      "recorded manifest %r"
                      % (built.get("fixture_manifest_digest"),
                         manifest_digest))
        pinned = [r for r in request_doc.get("fixture_set_refs", [])
                  if isinstance(r, dict) and r.get("id") == "fixture-q29b"]
        if not pinned or pinned[0].get("digest") != manifest_digest:
            self.fail("s4", "replay request does not pin the recorded "
                      "fixture manifest digest")
        recomputed = sha(jcs(request_doc).encode("utf-8"))
        if built.get("request_digest") != recomputed:
            self.fail("s4", "replay request digest %r != canonical JCS "
                      "digest %r" % (built.get("request_digest"), recomputed))
        for flag in ("segment_chain_match", "evidence_chain_match",
                     "candidate_chain_match"):
            if built.get(flag) is not True:
                self.fail("s4", "replay request %s=%r (authority chain "
                          "broken before replay)" % (flag, built.get(flag)))
        self.digest_chain["s4_request"]["request_digest"] = \
            built.get("request_digest")
        self.digest_chain["s4_request"]["fixture_manifest_digest"] = \
            manifest_digest

        missing = [f for f in fx.replay_families if f not in by_name]
        if missing:
            self.fail("s4", "replay request families missing: %s" % missing)
        reserved = set(built.get("reserved_families") or [])
        if reserved != set(fx.reserved_families):
            self.fail("s4", "replay request reserved families %r != manifest "
                      "%r" % (sorted(reserved), sorted(fx.reserved_families)))
        for family, packet in fx.packets.items():
            if family in fx.reserved_families:
                continue
            declared = ((by_name.get(family) or {}).get("packets") or [{}])[0]
            if declared.get("expected_output_digest") != \
                    packet["expected_digest"]:
                self.fail("s4", "replay request family %s expected digest %r "
                          "!= fixture %r"
                          % (family, declared.get("expected_output_digest"),
                             packet["expected_digest"]))
            if declared.get("expected_run_status") != packet["expected_status"]:
                self.fail("s4", "replay request family %s expected status %r "
                          "!= fixture %r"
                          % (family, declared.get("expected_run_status"),
                             packet["expected_status"]))
            reason = declared.get("expected_reason_code") or ""
            if reason != packet["expected_reason"]:
                self.fail("s4", "replay request family %s expected reason %r "
                          "!= fixture %r"
                          % (family, reason, packet["expected_reason"]))
        self.evidence["s4_request"] = {
            "request_doc": request_doc,
            "request_digest": built.get("request_digest"),
            "families": families,
        }

        # Host replay plan (frozen) over the RSIH subprocess runner.
        schedule_params = {
            "mode": "ok",
            "request": request_doc,
            "node": (self.tools or {}).get("node") or "node",
            "rsih": self.repo_dirs.get("rsih", ""),
        }
        schedule = self.call("host", "replay.schedule", schedule_params, "s4",
                             "replay schedule ok")
        if schedule is not None:
            self.evidence["s4_host_schedule"] = schedule
            if schedule.get("accepted") is not True:
                self.fail("s4", "replay schedule rejected: %r %r"
                          % (schedule.get("reason_code"),
                             schedule.get("detail")))
            else:
                if schedule.get("plan_digest"):
                    self.digest_chain["s4_host_plan"]["plan_digest"] = \
                        schedule["plan_digest"]
                for output in schedule.get("outputs") or []:
                    family = output.get("family")
                    packet = fx.packets.get(family)
                    if packet is None:
                        self.fail("s4", "replay schedule produced output for "
                                  "unknown family %r" % family)
                        continue
                    if output.get("output_digest") != packet["expected_digest"]:
                        self.fail("s4", "digest mismatch: Host plan output "
                                  "%s/%s digest %r != fixture expectation %r"
                                  % (family, output.get("side"),
                                     output.get("output_digest"),
                                     packet["expected_digest"]))
                    if output.get("output_digest"):
                        self.digest_chain["s4_host_plan"].setdefault(
                            "%s_%s_output_digest"
                            % (family, output.get("side")),
                            output["output_digest"])
                covered = {o.get("family")
                           for o in schedule.get("outputs") or []}
                for family in fx.replay_families:
                    if family not in covered:
                        self.fail("s4", "replay plan never ran family %r"
                                  % family)
                for probe in schedule.get("probes") or []:
                    if probe.get("matched") is not True:
                        self.fail("s4", "Host determinism probe drifted: %r"
                                  % probe)

        # RSIH corpus cross-check.
        corpus = self.call("rsih", "corpus.list", {}, "s4", "corpus list")
        if corpus is not None:
            if corpus.get("corpus_errors"):
                self.fail("s4", "RSIH corpus errors: %r"
                          % corpus.get("corpus_errors"))
            listed = {}
            for c in corpus.get("cases") or []:
                # first packet per family (multi-packet families replay
                # through their first packet, exactly like the runner)
                if isinstance(c, dict) and c.get("family") not in listed:
                    listed[c.get("family")] = c
            if set(listed) != set(fx.replay_families):
                self.fail("s4", "RSIH corpus families %r != manifest %r"
                          % (sorted(listed), sorted(fx.replay_families)))
            for family, case in listed.items():
                packet = fx.packets.get(family) or {}
                if case.get("expected_digest") != packet.get("expected_digest"):
                    self.fail("s4", "RSIH corpus expected digest for %s "
                              "diverges from the fixture" % family)

        # RSIH raw runs, round 1 (before any later transcript).
        round1 = self._rsih_round(1)
        result1 = None
        if round1:
            result1 = self._canonicalize(request_doc, round1, "round 1")
            if result1 is not None and result1.get("result_digest"):
                self.digest_chain["s4_gms_result"]["round1_result_digest"] = \
                    result1["result_digest"]

        # Negative probes: missing family, cross-run contamination and
        # nondeterminism must all fail closed on both sides.
        neg = self.call("host", "replay.schedule",
                        dict(schedule_params, mode="missing_family"), "s4",
                        "replay schedule missing_family")
        if neg is not None:
            ok = (neg.get("accepted") is not True and
                  neg.get("reason_code") == "MISSING_FAMILY")
            if not ok:
                self.fail("s4", "missing family accepted by the Host "
                          "scheduler (accepted=%r reason=%r)"
                          % (neg.get("accepted"), neg.get("reason_code")))
            self.record_negative("s4_host_missing_family", "MISSING_FAMILY",
                                 neg.get("reason_code"), ok)
        neg = self.call("host", "replay.schedule",
                        dict(schedule_params, mode="cross_run"), "s4",
                        "replay schedule cross_run")
        if neg is not None:
            ok = neg.get("second_accepted") is not True
            if not ok:
                self.fail("s4", "cross-run contamination: the replay lease "
                          "double-dispatched a mutated request identity "
                          "(second_accepted=%r reason=%r)"
                          % (neg.get("second_accepted"),
                             neg.get("second_reason_code")))
            self.record_negative("s4_host_cross_run", "second rejected",
                                 neg.get("second_reason_code"), ok)
        neg = self.call("gms", "replay.canonicalize",
                        {"mode": "missing_family", "request_doc": request_doc,
                         "runs": self._runs_list(round1 or {})}, "s4",
                        "canonicalize missing_family")
        if neg is not None:
            ok = (neg.get("rejected") is True and
                  neg.get("reason_code") == "MISSING_FAMILY")
            if not ok:
                self.fail("s4", "missing family accepted by the GMS "
                          "canonicalizer (rejected=%r reason=%r status=%r)"
                          % (neg.get("rejected"), neg.get("reason_code"),
                             neg.get("status")))
            self.record_negative("s4_gms_missing_family", "MISSING_FAMILY",
                                 neg.get("reason_code"), ok)
        neg = self.call("gms", "replay.canonicalize",
                        {"mode": "cross_run", "request_doc": request_doc,
                         "runs": self._runs_list(round1 or {})}, "s4",
                        "canonicalize cross_run")
        if neg is not None:
            ok = neg.get("rejected") is True
            if not ok:
                self.fail("s4", "cross-run contamination accepted by the GMS "
                          "canonicalizer (foreign packet runs)")
            self.record_negative("s4_gms_cross_run", "rejected",
                                 neg.get("reason_code"), ok)
        neg = self.call("gms", "replay.canonicalize",
                        {"mode": "nondeterministic",
                         "request_doc": request_doc,
                         "runs": self._runs_list(round1 or {})}, "s4",
                        "canonicalize nondeterministic")
        if neg is not None:
            ok = (neg.get("status") == "inconclusive" and
                  neg.get("failure_reason_code") == "REPLAY_NONDETERMINISTIC")
            if not ok:
                self.fail("s4", "nondeterministic replay canonicalized as "
                          "%r (reason %r) instead of inconclusive/"
                          "REPLAY_NONDETERMINISTIC"
                          % (neg.get("status"),
                             neg.get("failure_reason_code")))
            self.record_negative(
                "s4_gms_nondeterministic",
                "inconclusive/REPLAY_NONDETERMINISTIC",
                {"status": neg.get("status"),
                 "reason": neg.get("failure_reason_code")}, ok)

        # Replay invariance: a later transcript appended after the seal
        # must not change any replay digest.
        later = {"appended_after_seal": True}
        probe_case = (fx.settled_segment_cases() or sorted(fx.segments))[0]
        probe = self.call("host", "segment.close",
                          {"case_id": probe_case,
                           "probes": ["append_after_settled"]},
                          "s4", "later transcript append probe")
        if probe is not None:
            outcome = (probe.get("probes") or {}).get(
                "append_after_settled") or {}
            later["append_after_settled"] = outcome
            if outcome.get("rejected") is not True:
                self.fail("s4", "later transcript entered a sealed segment "
                          "(mutable seal after close)")
        round2 = self._rsih_round(2, previous=round1 or None)
        result2 = None
        if round2:
            result2 = self._canonicalize(
                request_doc, round2, "round 2 (after later transcript)")
        if result1 is not None and result2 is not None:
            d1 = result1.get("result_digest")
            d2 = result2.get("result_digest")
            if d1 and d2 and d1 != d2:
                self.fail("s4", "nondeterministic replay result digest "
                          "drifted between rounds (%r -> %r): a later "
                          "transcript must not change replay"
                          % (d1, d2))
            if d2:
                self.digest_chain["s4_gms_result"]["round2_result_digest"] = d2
        self.evidence["s4_gms_result"] = {
            "round1": result1, "round2": result2,
            "later_transcript": later,
        }

    def _rsih_round(self, number, previous=None):
        """One RSIH raw round over every non-reserved family."""
        fx = self.fixtures
        runs = {}
        evidence = []
        for family in fx.replay_families:
            response = self.call("rsih", "runner.run", {"family": family},
                                 "s4", "runner round %d %s" % (number, family))
            if response is None:
                return None
            packet = fx.packets.get(family) or {}
            digest = response.get("output_digest")
            evidence.append({
                "family": family,
                "packet_id": response.get("packet_id"),
                "output_digest": digest,
                "status": response.get("status"),
            })
            if response.get("packet_id") != packet.get("packet_id"):
                self.fail("s4", "RSIH round %d family %s ran packet %r != "
                          "fixture %r"
                          % (number, family, response.get("packet_id"),
                             packet.get("packet_id")))
            if digest != packet.get("expected_digest"):
                self.fail("s4", "digest mismatch: RSIH raw output of %s is "
                          "%r, fixture expects %r"
                          % (family, digest, packet.get("expected_digest")))
            if response.get("digest_matches_expected") is not True:
                self.fail("s4", "RSIH runner reports "
                          "digest_matches_expected=%r for %s"
                          % (response.get("digest_matches_expected"), family))
            if response.get("repeat_pair_digest_equal") is not True:
                self.fail("s4", "nondeterministic RSIH raw pair: family %s "
                          "produced two different digests in one round"
                          % family)
            if previous and family in previous:
                if digest != previous[family]["output_digest"]:
                    self.fail("s4", "later transcript changed the replay "
                              "digest of family %s (%r -> %r)"
                              % (family, previous[family]["output_digest"],
                                 digest))
            if digest:
                self.digest_chain["s4_rsih_raw"][
                    "%s_output_digest" % family] = digest
            runs[family] = {
                "packet_id": response.get("packet_id"),
                "run_status": response.get("status"),
                "reason_code": response.get("reason_code") or "",
                "output_digest": digest,
            }
        self.evidence.setdefault("s4_rsih_runs", {})[str(number)] = evidence
        return runs

    @staticmethod
    def _runs_list(round_runs):
        return [round_runs[f] for f in sorted(round_runs)]

    def _canonicalize(self, request_doc, round_runs, context):
        fx = self.fixtures
        response = self.call("gms", "replay.canonicalize",
                             {"mode": "ok", "request_doc": request_doc,
                              "runs": self._runs_list(round_runs)},
                             "s4", "canonicalize " + context)
        if response is None:
            return None
        if response.get("rejected") is True:
            self.fail("s4", "canonicalize %s was rejected: %r"
                      % (context, response.get("reason_code")))
            return response
        if response.get("status") != "succeeded":
            self.fail("s4", "canonicalize %s ended %r (reason %r)"
                      % (context, response.get("status"),
                         response.get("failure_reason_code")))
        if response.get("rerun_digest_equal") is not True:
            self.fail("s4", "nondeterministic canonicalization (%s): the "
                      "same inputs produced different digests" % context)
        outcomes = response.get("outcome_families") or []
        missing = [f for f in fx.replay_families if f not in outcomes]
        if missing:
            self.fail("s4", "canonicalize %s outcome families missing: %s"
                      % (context, missing))
        records = response.get("records") or []
        by_packet = {r.get("packet_id"): r for r in records
                     if isinstance(r, dict)}
        for family, run in round_runs.items():
            record = by_packet.get(run["packet_id"])
            if record is None:
                self.fail("s4", "canonicalize %s has no run record for %s"
                          % (context, family))
                continue
            if record.get("output_digest") != run["output_digest"]:
                self.fail("s4", "canonicalize %s record digest for %s "
                          "diverges from the observed raw digest"
                          % (context, family))
        return response

    # -- driver protocol probe ----------------------------------------------

    def probe_unknown_op(self):
        """An unknown op must fail closed (DRIVER_OP_UNKNOWN for the real
        drivers; a raised refusal or an error response also fails closed).
        Accepting the instruction (ok=true) turns the tracer red."""
        session = self._session_of("host")
        try:
            response = session.call(
                "no.such.op", {"fixtures": str(self.fixtures.root)})
        except Exception:
            return  # a raised refusal is fail-closed (fake drivers)
        error = response.get("error") or {}
        if response.get("ok") is True:
            self.fail("s4", "driver host accepted unknown op no.such.op "
                      "(ok=true; unknown instructions must fail closed)")
        elif not error.get("code"):
            self.fail("s4", "driver host refused unknown op without a "
                      "closed error code (got %r)" % (response,))


def _trim_response(response):
    """Keep evidence copies readable (drop multi-KB canonical blobs)."""
    trimmed = {}
    for key, value in response.items():
        if key == "canonical_b64" and isinstance(value, str) and \
                len(value) > 120:
            trimmed[key] = value[:64] + "...(%d b64 chars)" % len(value)
        else:
            trimmed[key] = value
    return trimmed


def run_tracer(repo_dirs, fixtures, evidence_out=None, drivers=None,
               toolchain=None):
    """Run the whole tracer; returns a result dict, never raises."""
    failures = []
    fixtures = Path(fixtures).resolve()
    if not fixtures.is_dir():
        return _result(False, ["fixtures directory not found: %s" % fixtures],
                       evidence_dir=None, summary=None, toolchain=toolchain or {})
    # Driver subprocesses run with their repository as cwd, so every path
    # handed to them (repo dirs included) must be absolute.
    repo_dirs = {name: str(Path(path).resolve())
                 for name, path in (repo_dirs or {}).items()}
    evidence_dir = Path(evidence_out).resolve() if evidence_out else \
        Path(tempfile.mkdtemp(prefix="int002-s2s4-evidence-"))
    try:
        evidence_dir.relative_to(fixtures)
        inside = True
    except ValueError:
        inside = False
    if inside:
        return _result(False,
                       ["evidence directory %s is inside the fixtures tree "
                        "%s; refusing to write evidence near golden files"
                        % (evidence_dir, fixtures)],
                       evidence_dir=str(evidence_dir), summary=None,
                       toolchain=toolchain or {})

    tools = {"go": resolve_tool("go"), "node": resolve_tool("node")}
    if toolchain is None:
        toolchain, tool_failures = collect_toolchain()
        failures.extend(tool_failures)

    if drivers is None:
        missing = [n for n, p in tools.items() if p is None]
        if missing:
            failures.append("required tools not found: %s" % ", ".join(missing))
            kit = _FailingFactory("required tools not found: %s"
                                  % ", ".join(missing))
        else:
            for name in ("host", "gms", "rsih"):
                if not Path(repo_dirs.get(name, "")).is_dir():
                    failures.append("%s repository directory not found: %s"
                                    % (name, repo_dirs.get(name)))
            kit = RealDriverKit()
    else:
        kit = drivers

    evidence_dir.mkdir(parents=True, exist_ok=True)
    fx = Fixtures(fixtures)
    tracer = Tracer(repo_dirs, fx, evidence_dir, kit, toolchain, tools)
    before = snapshot_tree(fixtures)

    tracer.run_s2()
    tracer.run_s3()
    tracer.run_s4()
    tracer.probe_unknown_op()

    failures.extend(tracer.failures)

    # Close every spawned driver session.
    for driver in getattr(kit, "sessions", {}).values():
        try:
            driver.close()
        except Exception:
            pass

    after = snapshot_tree(fixtures)
    fixtures_immutable = before == after
    if not fixtures_immutable:
        changed = sorted(set(before) ^ set(after)) or [
            p for p in before if before[p] != after.get(p)]
        failures.append(
            "fixtures tree mutated during the run (files added/removed/"
            "changed: %s); golden files must never be written"
            % ", ".join(changed[:10]))

    transcript_failures = [f for f in failures if "transcript" in f.lower()]
    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "gate": "INT-002",
        "generated_utc": datetime.datetime.now(
            datetime.timezone.utc).isoformat(timespec="seconds"),
        "fixtures_root": str(fixtures),
        "slices": {
            "s2": tracer.slice_failures["s2"] == 0,
            "s3": tracer.slice_failures["s3"] == 0,
            "s4": tracer.slice_failures["s4"] == 0,
        },
        "fixtures_immutable": fixtures_immutable,
        "replay_invariance": {
            "later_transcript_ignored": not transcript_failures,
        },
        "digest_chain": tracer.digest_chain,
        "negatives_total": len(tracer.negatives),
        "toolchain": toolchain,
    }
    evidence_docs = (
        ("s2_toolproxy", tracer.evidence.get("s2_toolproxy")),
        ("s3_segments", tracer.evidence.get("s3_segments")),
        ("s3_evidence", tracer.evidence.get("s3_evidence")),
        ("s3_candidate", tracer.evidence.get("s3_candidate")),
        ("s4_request", tracer.evidence.get("s4_request")),
        ("s4_rsih_runs", tracer.evidence.get("s4_rsih_runs")),
        ("s4_host_schedule", tracer.evidence.get("s4_host_schedule")),
        ("s4_gms_result", tracer.evidence.get("s4_gms_result")),
        ("negatives", tracer.negatives),
        ("digest_chain", tracer.digest_chain),
        ("summary", summary),
    )
    for name, doc in evidence_docs:
        _write_json(evidence_dir / (name + ".json"), doc)

    return _result(ok=not failures, failures=failures,
                   evidence_dir=str(evidence_dir), summary=summary,
                   toolchain=toolchain)


def _write_json(path, doc):
    try:
        path.write_text(
            json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False,
                       default=str) + "\n", encoding="utf-8")
    except (OSError, TypeError) as exc:
        print("warning: cannot write evidence %s: %s" % (path, exc),
              file=sys.stderr)


def _result(ok, failures, evidence_dir, summary, toolchain):
    return {
        "ok": ok,
        "failures": list(failures),
        "evidence_dir": evidence_dir,
        "summary": summary,
        "toolchain": toolchain,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="INT-002 S2-S4 Host/GMS/RSIH conformance tracer")
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
            print("INT-002 S2-S4 tracer: RED")
            print("failure: %s repository directory not found: %s"
                  % (name, path))
            return 1

    result = run_tracer(repo_dirs=repo_dirs, fixtures=args.fixtures,
                        evidence_out=args.evidence_out)

    toolchain = result.get("toolchain") or {}
    print("INT-002 S2-S4 tracer: %s" % ("GREEN" if result["ok"] else "RED"))
    print("drivers: host=%s gms=%s rsih=%s"
          % (args.host, args.gms, args.rsih))
    print("fixtures: %s" % args.fixtures)
    print("toolchain: go=%s node=%s python=%s"
          % (toolchain.get("go"), toolchain.get("node"),
             toolchain.get("python")))
    summary = result.get("summary") or {}
    if summary:
        print("slices: %s" % json.dumps(summary.get("slices"), sort_keys=True))
        print("fixtures immutable: %s" % summary.get("fixtures_immutable"))
        print("replay invariance (later transcript ignored): %s"
              % (summary.get("replay_invariance") or {}).get(
                  "later_transcript_ignored"))
        chain = summary.get("digest_chain") or {}
        if chain:
            print("digest chain: %s" % ", ".join(
                "%s(%d)" % (stage, len(entries))
                for stage, entries in sorted(chain.items())))
        print("negatives exercised: %s" % summary.get("negatives_total"))
    if result.get("evidence_dir"):
        print("evidence: %s" % result["evidence_dir"])
    for failure in result["failures"]:
        print("failure: %s" % failure)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
