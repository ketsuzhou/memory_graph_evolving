#!/usr/bin/env python3
"""INT-003 -- S5-S9 GMS/RSIH/Host conformance tracer.

Drives the three contract drivers (GMS cmd/s5s9-driver, Host
cmd/s5s9-driver, RSIH scripts/s5s9-driver.ts) over the frozen shared
fixtures and traces the authoritative chain end to end:

    S5 atomic release/activation (GMS-204) ->
    S6 projector watermark/gap/duplicate (GMS-205) ->
    S7 same-call Explore through GMS-206 + the Host ToolProxy bridge ->
    S8 authoritative closure read + RSIH materialization bundle/lock ->
    S9 exact Composite children/ports/whole-replay + Pi registry

Fail-closed by construction. Any of the following turns the tracer red:
a driver protocol violation, op failure or unknown-op acceptance; a
partial release (refused activation that still moved residue); a release
activating without atomic residue or over a not-accepted decision; a
projection gap that advanced the watermark or a duplicate that was not a
no-op; an Explore served behind the required activation sequence; a Host
rewrite of the Pi-delivered ExploreResult bytes; a Graph-form/stale/torn
closure root accepted instead of the exact active head; a bundle digest
used as the materialization/lock ref; a stale freeze sequence; a partial
publish frozen anyway; a dynamic child, composite cycle or replay drift
accepted; a candidate/latest registry resolution; scoring/U1 semantics
smuggled into a raw driver output; or any mutation of the fixtures tree.

Evidence (per-slice driver responses, the negative matrix, the digest
chain, the summary and the master manifest) is written only to a
directory outside the fixtures tree. Exit code 0 = green, 1 = red.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SUMMARY_SCHEMA_VERSION = "rsih-int003-s5s9-summary.v1"
EVIDENCE_MANIFEST_SCHEMA_VERSION = "rsih-int003-evidence-manifest.v1"

# Stages of the authoritative digest chain (evidence digest_chain.json).
CHAIN_STAGES = (
    "s5_release", "s6_projection", "s7_explore",
    "s8_materialization", "s9_composite",
)

SHA_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

# Keys no raw driver response may ever carry (Host §6.7 / RSIH §6.5 /
# GMS §5.4: scoring, U1 and release semantics live in the decision layer,
# never in raw release/projection/retrieval/materialization facts). Exact
# key match, recursive over the whole response. NOTE: "released_ref" and
# friends are legitimate release facts, so unlike the S2-S4 raw-output
# scan this list never substring-matches "release".
FORBIDDEN_SCORING_KEYS = frozenset((
    "u1", "u1_decision", "utility_vector", "utility", "winner",
    "score", "scoring", "recommend", "recommendation",
    "release_decision",
))

GO_FALLBACK_PATHS = (
    os.path.join(os.path.expanduser("~"), "go", "bin"),
    "/usr/local/go/bin",
)

HERMETIC_GOCACHE = os.path.join(tempfile.gettempdir(), "int003-gocache-rsih")

# The fixture-frozen materialization case the S8/S9 slices freeze over.
LOCK_CASE_ID = "lock-002-composite-freeze"
LOCK_MATERIALIZATION_ID = "mat-comp-0002"
LOCK_ACTIVATION_SEQUENCE = 21

# The fixture-frozen composite the S9 slice runs.
COMPOSITE_ARTIFACT = "artifact-003-composite"

# The fixture-frozen memory_explore case the Host same-call bridge
# delivers (the request/result_payload pair is frozen together in the tools
# corpus; artifact-006-explore-result carries the canonical ExploreResult
# artifact for the S1-style corpus, not the paired request).
EXPLORE_CASE_ID = "pos-explore-full-success"

SLICE_NAMES = {
    "s5": "s5_release", "s6": "s6_projection", "s7": "s7_explore",
    "s8": "s8_materialization", "s9": "s9_composite",
}


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


def is_sha_digest(value):
    return isinstance(value, str) and bool(SHA_DIGEST_RE.match(value))


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
        if key_str in FORBIDDEN_SCORING_KEYS:
            found.append(prefix + key_str)
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
                prefix="int003-%s-stderr-" % self.name, delete=False)
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
        binary = os.path.join(tempfile.mkdtemp(prefix="int003-driver-"),
                              "s5s9-driver-" + name)
        proc = subprocess.run(
            [resolve_tool("go"), "build", "-o", binary, "./cmd/s5s9-driver"],
            cwd=repo, capture_output=True, text=True, check=False,
            env=self.env, timeout=600)
        if proc.returncode != 0:
            raise RuntimeError("go build %s/cmd/s5s9-driver exited %d: %s"
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
                    os.path.join("scripts", "s5s9-driver.ts")]
            driver = SubprocessDriver(name, argv, repo_dirs["rsih"],
                                      env=self.env)
        self.sessions[name] = driver
        return driver


class _BrokenSession:
    """Stand-in for a driver session whose kit could not build it; every
    instruction raises so the tracer fails closed without crashing."""

    def __init__(self, name, error):
        self.name = name
        self.error = error
        self.launch_error = str(error)

    def call(self, op, params):
        raise RuntimeError("driver %s session is broken: %s"
                           % (self.name, self.error))

    def close(self):
        return None


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
        manifest = load_json(self.root / "materialization" / "manifest.json")
        self.mat_cases = {}
        for case in manifest["cases"]:
            case_dir = self.root / "materialization" / case["path"]
            self.mat_cases[case["case_id"]] = {
                "case": case,
                "expected": load_json(case_dir / "expected.json"),
                "manifest": load_json(case_dir / "manifest-document.json"),
            }
        lock = self.mat_cases[LOCK_CASE_ID]
        self.lock_expected = lock["expected"]
        self.lock_manifest = lock["manifest"]
        self.lock_file_count = self.lock_manifest.get("file_count")
        composite = load_json(self.root / "artifacts" / COMPOSITE_ARTIFACT
                              / "source.json")
        self.composite_child_ids = [child["child_id"] for child in
                                   composite["body"]["children"]]
        self.composite_nodes = [
            (child["skill_ref"] or {}).get("lineage_id")
            for child in composite["body"]["children"]
        ]
        # Fixture-frozen closure vocabulary for the S8 read probes.
        self.closure_nodes = [
            "comp-review-orchestrator", "proc-diff-summarizer",
            "sg-commit-checklist",
        ]

    def lock_digests(self):
        return {
            "manifest_digest": self.lock_expected.get("expected_manifest_digest"),
            "bundle_digest": self.lock_expected.get("expected_bundle_digest"),
            "lock_digest": self.lock_expected.get("expected_lock_digest"),
        }


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
        self.slice_failures = {"s5": 0, "s6": 0, "s7": 0, "s8": 0, "s9": 0}
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

    def _session_of(self, name, slice_name=None):
        if name not in self._sessions:
            try:
                self._sessions[name] = self.drivers(
                    name, self.tools, str(self.fixtures.root), self.repo_dirs)
            except Exception as exc:
                # A kit that cannot even build a session fails closed:
                # every instruction on it reports a driver failure instead
                # of crashing the tracer (the run must always reach the
                # fixtures-tree snapshot comparison).
                message = ("driver %s session could not be built: %r"
                           % (name, exc))
                if slice_name:
                    self.fail(slice_name, message)
                else:
                    self.failures.append(message)
                self._sessions[name] = _BrokenSession(name, exc)
        return self._sessions[name]

    def call(self, driver_name, op, params, slice_name, context):
        """One driver instruction with protocol, scoring and evidence checks."""
        session = self._session_of(driver_name, slice_name)
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
            error = response.get("error")
            if isinstance(error, dict) and error.get("code"):
                # Driver/protocol failure: the instruction itself failed.
                code = error.get("code")
                reason = error.get("reason") or "no reason given"
                self.fail(slice_name, "driver %s instruction %s (%s) failed: "
                          "%s: %s" % (driver_name, op, context, code, reason))
                self.record_negative("driver_" + op, "ok=true",
                                     {"code": code, "reason": reason}, True)
                return None
            if response.get("reason_code"):
                # Semantic fail-closed refusal: the op ran and the
                # operation itself refused (a lock rejecting a
                # bundle-digest ref, a registry rejecting a candidate).
                # Not a driver failure -- the probe site interprets the
                # outcome.
                self.evidence.setdefault(
                    SLICE_NAMES.get(slice_name, slice_name), {}) \
                    .setdefault("calls", []).append(
                        {"driver": driver_name, "op": op,
                         "context": context,
                         "response": _trim_response(response)})
                return response
            self.fail(slice_name, "driver %s instruction %s (%s) failed: "
                      "DRIVER_PROTOCOL_VIOLATION: ok=false response without "
                      "an error code or a semantic reason_code"
                      % (driver_name, op, context))
            self.record_negative("driver_" + op, "ok=true",
                                 {"code": "DRIVER_PROTOCOL_VIOLATION",
                                  "reason": "no code, no reason"}, True)
            return None
        self.evidence.setdefault(SLICE_NAMES.get(slice_name, slice_name), {}) \
            .setdefault("calls", []).append(
                {"driver": driver_name, "op": op, "context": context,
                 "response": _trim_response(response)})
        return response

    def reset_world(self):
        """Rebuild the GMS driver's authority world before the gap probe.

        The REAL driver keeps one causal world per session; the gap probe
        needs a pristine projector (cursor 0) to deliver a genuine
        sequence gap after the runtime/duplicate/conflict probes drained
        the shared one. Fake kits refuse unknown ops by raising — a raised
        refusal is fail-closed and simply leaves their canned world alone.
        """
        session = self._session_of("gms")
        try:
            session.call("world.reset",
                         {"fixtures": str(self.fixtures.root)})
        except Exception:
            pass
    # -- S5: atomic release/activation --------------------------------------

    def run_s5(self):
        fx = self.fixtures
        ev = {}
        ok = self.call("gms", "release.activate",
                       {"lineage_id": "sg-alpha", "kind": "step_guidance",
                        "mode": "ok"},
                       "s5", "atomic release sg-alpha")
        if ok is not None:
            ev["release"] = ok
            if ok.get("activated") is not True:
                self.fail("s5", "release did not activate the lineage "
                          "(activated=%r reason=%r)"
                          % (ok.get("activated"), ok.get("reason_code")))
            if ok.get("residue_atomic") is not True:
                self.fail("s5", "release was not atomic: the post-release "
                          "residue is not the exact all-or-nothing set "
                          "(residue_atomic=%r, residue=%r)"
                          % (ok.get("residue_atomic"), ok.get("residue")))
            if ok.get("outbox_pending") != 1:
                self.fail("s5", "release left %r pending outbox records "
                          "(must be exactly one event, one record)"
                          % ok.get("outbox_pending"))
            ref = ok.get("released_ref") or {}
            if ref.get("schema_version") != "gms.skill-artifact-ref.v2":
                self.fail("s5", "released_ref schema_version %r is not the "
                          "frozen skill-artifact-ref form"
                          % ref.get("schema_version"))
            if ref.get("lineage_id") != "sg-alpha":
                self.fail("s5", "released_ref lineage %r != requested "
                          "sg-alpha" % ref.get("lineage_id"))
            if not is_sha_digest(ref.get("artifact_digest")):
                self.fail("s5", "released_ref artifact_digest %r is not a "
                          "sha256 digest" % ref.get("artifact_digest"))
            if not is_sha_digest(ok.get("event_digest")):
                self.fail("s5", "activation event_digest %r is not a sha256 "
                          "digest" % ok.get("event_digest"))
            if not ok.get("event_id"):
                self.fail("s5", "release produced no activation event id")
            residue = ok.get("residue") or {}
            if residue.get("proposal_state") != "released":
                self.fail("s5", "proposal state after release is %r (must "
                          "be terminal released)"
                          % residue.get("proposal_state"))
            if residue.get("activation_entries") != 1:
                self.fail("s5", "release left %r activation ledger entries "
                          "(must be exactly the one release event)"
                          % residue.get("activation_entries"))
            if residue.get("outbox_pending") != 1:
                self.fail("s5", "release residue carries %r pending outbox "
                          "records" % residue.get("outbox_pending"))
            if is_sha_digest(ref.get("artifact_digest")):
                self.digest_chain["s5_release"][
                    "released_artifact_digest"] = ref["artifact_digest"]
            if is_sha_digest(ok.get("event_digest")):
                self.digest_chain["s5_release"][
                    "activation_event_digest"] = ok["event_digest"]
            if is_sha_digest(ok.get("outbox_key")):
                self.digest_chain["s5_release"]["outbox_key"] = \
                    ok["outbox_key"]

        # stale_cas: a wrong frozen head expectation must refuse with zero
        # residue movement (the zero-partial release guarantee, GMS §6.1).
        neg = self.call("gms", "release.activate",
                        {"lineage_id": "sg-alpha", "kind": "step_guidance",
                         "mode": "stale_cas"},
                        "s5", "stale CAS release refusal")
        if neg is not None:
            ev["stale_cas"] = neg
            if neg.get("activated") is not False:
                self.fail("s5", "stale CAS probe activated the lineage "
                          "(activated=%r)" % neg.get("activated"))
            if neg.get("reason_code") != "ACTIVE_HEAD_CONFLICT":
                self.fail("s5", "stale CAS refusal reason %r != "
                          "ACTIVE_HEAD_CONFLICT" % neg.get("reason_code"))
            unchanged = neg.get("residue_unchanged") is True
            if not unchanged:
                self.fail("s5", "partial release: the refused release "
                          "(ACTIVE_HEAD_CONFLICT) still moved the "
                          "authoritative residue (residue_unchanged=%r, "
                          "residue=%r) -- a refused release must leave "
                          "exactly zero partial state"
                          % (neg.get("residue_unchanged"), neg.get("residue")))
            self.record_negative("s5_stale_cas", "zero residue",
                                 {"reason_code": neg.get("reason_code"),
                                  "residue": neg.get("residue")},
                                 rejected=unchanged)

        # not_accepted: a rejected decision must never activate.
        neg = self.call("gms", "release.activate",
                        {"lineage_id": "sg-alpha", "kind": "step_guidance",
                         "mode": "not_accepted"},
                        "s5", "not-accepted release refusal")
        if neg is not None:
            ev["not_accepted"] = neg
            refused = neg.get("activated") is not False
            if refused:
                self.fail("s5", "release not accepted: a RELEASE_NOT_ACCEPTED "
                          "decision still activated the lineage "
                          "(activated=%r, reason=%r)"
                          % (neg.get("activated"), neg.get("reason_code")))
            self.record_negative("s5_not_accepted", "not activated",
                                 {"activated": neg.get("activated"),
                                  "reason_code": neg.get("reason_code")},
                                 rejected=not refused)

        # body_mismatch: a tampered released body must never activate.
        neg = self.call("gms", "release.activate",
                        {"lineage_id": "sg-alpha", "kind": "step_guidance",
                         "mode": "body_mismatch"},
                        "s5", "body mismatch release refusal")
        if neg is not None:
            ev["body_mismatch"] = neg
            refused = neg.get("activated") is not False
            if refused:
                self.fail("s5", "released body mismatch: a tampered candidate "
                          "body still activated (activated=%r, reason=%r)"
                          % (neg.get("activated"), neg.get("reason_code")))
            self.record_negative("s5_body_mismatch", "not activated",
                                 {"activated": neg.get("activated"),
                                  "reason_code": neg.get("reason_code")},
                                 rejected=not refused)
        self.evidence["s5_release"] = ev

    # -- S6: projector watermark/gap/duplicate/conflict ----------------------

    def run_s6(self):
        ev = {}
        ok = self.call("gms", "projector.project", {"mode": "runtime"},
                       "s6", "runtime projection")
        if ok is not None:
            ev["runtime"] = ok
            if ok.get("state") != "current":
                self.fail("s6", "runtime projection state is %r (must be "
                          "current after the release)"
                          % ok.get("state"))
            if ok.get("blocked") is not False:
                self.fail("s6", "runtime projection is blocked (%r)"
                          % ok.get("blocked_code"))
            watermark = ok.get("watermark")
            if not isinstance(watermark, dict) or \
                    watermark.get("schema_version") != "gms.projection-watermark.v1":
                self.fail("s6", "projection result carries no frozen "
                          "projection watermark document (%r)" % (watermark,))
            else:
                sequence = watermark.get(
                    "projected_through_activation_sequence")
                if not isinstance(sequence, int) or sequence < 1:
                    self.fail("s6", "watermark projected_through_activation_"
                              "sequence %r is not a positive integer"
                              % sequence)
            if not is_sha_digest(ok.get("watermark_digest")):
                self.fail("s6", "watermark digest %r is not a sha256 digest"
                          % ok.get("watermark_digest"))
            if not is_sha_digest(ok.get("graph_digest")):
                self.fail("s6", "graph digest %r is not a sha256 digest"
                          % ok.get("graph_digest"))
            if is_sha_digest(ok.get("watermark_digest")):
                self.digest_chain["s6_projection"]["watermark_digest"] = \
                    ok["watermark_digest"]
            if is_sha_digest(ok.get("graph_digest")):
                self.digest_chain["s6_projection"]["graph_digest"] = \
                    ok["graph_digest"]

        # duplicate: the same delivered batch must be an idempotent no-op.
        dup = self.call("gms", "projector.project", {"mode": "duplicate"},
                        "s6", "duplicate delivery")
        if dup is not None:
            ev["duplicate"] = dup
            noop = dup.get("duplicate_noop") is True
            if not noop or dup.get("state") != "current":
                self.fail("s6", "duplicate delivery was not a no-op "
                          "(duplicate_noop=%r, projected=%r, state=%r) -- "
                          "re-delivering the same activation batch must "
                          "commit nothing"
                          % (dup.get("duplicate_noop"),
                             dup.get("projected"), dup.get("state")))
            self.record_negative("s6_duplicate", "idempotent no-op",
                                 {"duplicate_noop": dup.get("duplicate_noop"),
                                  "projected": dup.get("projected")},
                                 rejected=noop)

        # conflict: same activation sequence re-delivered with a different
        # digest must block with PROJECTION_EVENT_CONFLICT.
        conflict = self.call("gms", "projector.project", {"mode": "conflict"},
                             "s6", "conflicting redelivery")
        if conflict is not None:
            ev["conflict"] = conflict
            code = conflict.get("blocked_code") or conflict.get("error_code")
            blocked = code == "PROJECTION_EVENT_CONFLICT"
            if not blocked:
                self.fail("s6", "conflicting redelivery did not block with "
                          "PROJECTION_EVENT_CONFLICT (state=%r, code=%r, "
                          "watermark_unchanged=%r)"
                          % (conflict.get("state"), code,
                             conflict.get("watermark_unchanged")))
            self.record_negative("s6_conflict", "PROJECTION_EVENT_CONFLICT",
                                 {"code": code}, rejected=blocked)

        # The gap probe needs a pristine projector: rebuild the authority
        # world first (fake kits refuse the reset by raising, which is
        # their own fail-closed answer and leaves canned worlds intact).
        self.reset_world()

        # gap: a batch that skips an activation sequence must block with
        # PROJECTION_SEQUENCE_GAP and never advance the watermark.
        gap = self.call("gms", "projector.project", {"mode": "gap"},
                        "s6", "gapped delivery")
        if gap is not None:
            ev["gap"] = gap
            code = gap.get("blocked_code") or gap.get("error_code")
            blocked = code == "PROJECTION_SEQUENCE_GAP"
            unchanged = gap.get("watermark_unchanged") is True
            if not blocked or not unchanged:
                self.fail("s6", "projection gap was not blocked fail-closed "
                          "(blocked_code=%r, watermark_unchanged=%r) -- a "
                          "sequence gap must block and never advance the "
                          "watermark" % (code, gap.get("watermark_unchanged")))
            self.record_negative("s6_gap", "PROJECTION_SEQUENCE_GAP blocked",
                                 {"code": code,
                                  "watermark_unchanged":
                                      gap.get("watermark_unchanged")},
                                 rejected=blocked and unchanged)
        self.evidence["s6_projection"] = ev

    # -- S7: same-call Explore (GMS retrieval + Host ToolProxy) ---------------

    def run_s7(self):
        ev = {}
        ok = self.call("gms", "tools.invoke",
                       {"mode": "ok", "session": "exp-s5s9",
                        "tool_name": "memory_explore"},
                       "s7", "memory_explore ok")
        if ok is not None:
            ev["gms_explore"] = ok
            if ok.get("status") != "success":
                self.fail("s7", "memory_explore ended %r (reason %r); the "
                          "GMS retrieval tool status is the frozen \"success\""
                          % (ok.get("status"), ok.get("reason_code")))
            if ok.get("result_carry_watermark") is not True:
                self.fail("s7", "Explore result does not carry the projection "
                          "watermark (result_carry_watermark=%r)"
                          % ok.get("result_carry_watermark"))
            if ok.get("read_audit_present") is not True:
                self.fail("s7", "memory_explore produced no read audit")
            if not is_sha_digest(ok.get("upstream_result_digest")):
                self.fail("s7", "upstream result digest %r is not a sha256 "
                          "digest" % ok.get("upstream_result_digest"))
            if is_sha_digest(ok.get("upstream_result_digest")):
                self.digest_chain["s7_explore"][
                    "gms_upstream_result_digest"] = \
                    ok["upstream_result_digest"]

        # behind: an explore whose required min activation sequence runs
        # ahead of the watermark must fail closed, never serve.
        behind = self.call("gms", "tools.invoke",
                           {"mode": "behind", "session": "exp-s5s9",
                            "min_activation_sequence": 99},
                           "s7", "memory_explore behind watermark")
        if behind is not None:
            ev["gms_behind"] = behind
            served = behind.get("status") != "failed"
            if served or behind.get("failed_closed") is not True:
                self.fail("s7", "explore served behind the required "
                          "activation sequence (status=%r, reason=%r, "
                          "failed_closed=%r) -- the read side must fail "
                          "closed while the watermark is behind"
                          % (behind.get("status"),
                             behind.get("reason_code"),
                             behind.get("failed_closed")))
            self.record_negative("s7_behind",
                                 "failed/PROJECTION_BEHIND_REQUIRED_SEQUENCE",
                                 {"status": behind.get("status"),
                                  "reason_code": behind.get("reason_code")},
                                 rejected=not served)

        # Host same-call ToolProxy: the Pi return channel receives exactly
        # the upstream canonical ExploreResult bytes, before execution end.
        host_ok = self.call("host", "toolproxy.explore",
                            {"mode": "ok", "case_id": EXPLORE_CASE_ID,
                             "session": "exp-s5s9"},
                            "s7", "host same-call explore")
        if host_ok is not None:
            ev["host_explore"] = host_ok
            if host_ok.get("status") != "succeeded":
                self.fail("s7", "host explore case ended %r (reason %r)"
                          % (host_ok.get("status"),
                             host_ok.get("reason_code")))
            if host_ok.get("pi_bytes_equal_upstream") is not True:
                self.fail("s7", "Pi return channel bytes diverge from the "
                          "upstream ToolProxy result (pi_digest=%r)"
                          % host_ok.get("pi_digest"))
            if host_ok.get("pi_deliveries") != 1:
                self.fail("s7", "Pi return channel saw %r deliveries (must "
                          "be exactly once, same call)"
                          % host_ok.get("pi_deliveries"))
            if host_ok.get("delivered_before_end") is not True:
                self.fail("s7", "explore result was NOT delivered before "
                          "tool_execution_end (post-end delivery)")
            if host_ok.get("pi_digest") != host_ok.get("canonical_digest"):
                self.fail("s7", "Pi digest %r != canonical ToolProxy digest "
                          "%r" % (host_ok.get("pi_digest"),
                                  host_ok.get("canonical_digest")))
            if host_ok.get("watermark_present") is not True:
                self.fail("s7", "delivered ExploreResult carries no "
                          "projection watermark")
            if host_ok.get("result_order_preserved") is not True:
                self.fail("s7", "result order was not preserved on the "
                          "same-call path")
            if is_sha_digest(host_ok.get("canonical_digest")):
                self.digest_chain["s7_explore"]["pi_canonical_digest"] = \
                    host_ok["canonical_digest"]
            if is_sha_digest(host_ok.get("upstream_result_digest")):
                self.digest_chain["s7_explore"][
                    "host_upstream_result_digest"] = \
                    host_ok["upstream_result_digest"]

        # rewrite: a Host attempt to rewrite the delivered bytes must fail
        # closed -- the rewritten bytes may never reach the Pi channel.
        rewrite = self.call("host", "toolproxy.explore",
                            {"mode": "rewrite", "case_id": EXPLORE_CASE_ID,
                             "session": "exp-s5s9"},
                            "s7", "host rewrite attempt")
        if rewrite is not None:
            ev["host_rewrite"] = rewrite
            refused = rewrite.get("status") != "succeeded"
            if not refused:
                self.fail("s7", "Pi return channel accepted the Host's "
                          "rewritten ExploreResult bytes (rewrite mode "
                          "ended status=%r, pi_bytes_equal_upstream=%r) -- "
                          "a rewrite attempt must fail closed, the frozen "
                          "terminal replays" % (rewrite.get("status"),
                                                rewrite.get("pi_bytes_equal_upstream")))
            self.record_negative("s7_host_rewrite", "rewrite refused",
                                 {"status": rewrite.get("status"),
                                  "reason_code": rewrite.get("reason_code")},
                                 rejected=refused)
        self.evidence["s7_explore"] = ev

    # -- S8: authoritative closure + bundle/manifest/lock ---------------------

    def run_s8(self):
        fx = self.fixtures
        digests = fx.lock_digests()
        ev = {}
        ok = self.call("gms", "closure.read",
                       {"mode": "ok", "root_lineage": "cmp-orchestrator",
                        "root_kind": "composite",
                        "activation_sequence": LOCK_ACTIVATION_SEQUENCE,
                        "nodes": fx.closure_nodes},
                       "s8", "authoritative closure read")
        if ok is not None:
            ev["closure"] = ok
            if ok.get("schema_version") != "gms.materialization-closure-read.v1":
                self.fail("s8", "closure read schema_version %r is not the "
                          "frozen materialization-closure-read form"
                          % ok.get("schema_version"))
            if ok.get("roots") != ["cmp-orchestrator"]:
                self.fail("s8", "closure roots %r != the requested exact "
                          "active head cmp-orchestrator" % (ok.get("roots"),))
            nodes = ok.get("node_lineages")
            if not nodes or not all(isinstance(n, str) for n in nodes):
                self.fail("s8", "closure node_lineages %r is not a "
                          "non-empty lineage list" % (nodes,))
            if not isinstance(ok.get("activation_sequence"), int) or \
                    ok.get("activation_sequence", 0) < 1:
                self.fail("s8", "closure activation_sequence %r is not a "
                          "positive integer" % ok.get("activation_sequence"))
            if not is_sha_digest(ok.get("activation_head_digest")):
                self.fail("s8", "closure activation head digest %r is not a "
                          "sha256 digest" % ok.get("activation_head_digest"))
            if not is_sha_digest(ok.get("torn_read_token")):
                self.fail("s8", "closure torn-read token %r is not a sha256 "
                          "digest" % ok.get("torn_read_token"))
            if ok.get("canonical_bytes_digest_match") is not True:
                self.fail("s8", "closure canonical bytes do not digest to "
                          "their exact refs (canonical_bytes_digest_match=%r)"
                          % ok.get("canonical_bytes_digest_match"))
            if is_sha_digest(ok.get("activation_head_digest")):
                self.digest_chain["s8_materialization"][
                    "closure_activation_head_digest"] = \
                    ok["activation_head_digest"]

        # graph_root: a Graph node id form must be refused (never guessed
        # into a closure).
        neg = self.call("gms", "closure.read",
                        {"mode": "graph_root", "root_lineage":
                         "cmp-orchestrator", "root_kind": "composite"},
                        "s8", "graph-form root refusal")
        if neg is not None:
            ev["graph_root"] = neg
            refused = neg.get("read_failed") is True
            if not refused:
                self.fail("s8", "stale closure: a Graph-form root was "
                          "accepted by the closure read (read_failed=%r, "
                          "reason=%r) -- closure reads resolve exact active "
                          "heads only, never Graph node ids or latest forms"
                          % (neg.get("read_failed"),
                             neg.get("reason_code")))
            self.record_negative("s8_graph_root", "read failed",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # torn: a head that moved mid-read must fail, never guess.
        neg = self.call("gms", "closure.read",
                        {"mode": "torn", "root_lineage": "cmp-orchestrator",
                         "root_kind": "composite"},
                        "s8", "torn read refusal")
        if neg is not None:
            ev["torn"] = neg
            refused = neg.get("read_failed") is True
            if not refused:
                self.fail("s8", "torn closure read was guessed into a "
                          "closure (read_failed=%r) -- a head that moves "
                          "mid-read must fail closed, never guess"
                          % neg.get("read_failed"))
            self.record_negative("s8_torn", "read failed",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # not_current: a stale exact root must refuse with
        # SKILL_NOT_CURRENT_ACTIVE instead of resolving the head.
        neg = self.call("gms", "closure.read",
                        {"mode": "not_current",
                         "root_lineage": "cmp-orchestrator",
                         "root_kind": "composite"},
                        "s8", "stale root refusal")
        if neg is not None:
            ev["not_current"] = neg
            refused = neg.get("read_failed") is True
            if not refused:
                self.fail("s8", "stale (not-current-active) root was read "
                          "as a closure (read_failed=%r)"
                          % neg.get("read_failed"))
            self.record_negative("s8_not_current", "SKILL_NOT_CURRENT_ACTIVE",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # RSIH materialization over the frozen composite-freeze case.
        mat = self.call("rsih", "materialize.run",
                        {"mode": "ok", "case_id": LOCK_CASE_ID,
                         "materialization_id": LOCK_MATERIALIZATION_ID,
                         "activation_sequence": LOCK_ACTIVATION_SEQUENCE},
                        "s8", "materialize " + LOCK_CASE_ID)
        if mat is not None:
            ev["materialize"] = mat
            if mat.get("ok") is not True:
                self.fail("s8", "materialization of %s failed (reason %r)"
                          % (LOCK_CASE_ID, mat.get("reason_code")))
            else:
                if mat.get("manifest_digest") != digests["manifest_digest"]:
                    self.fail("s8", "materialized manifest digest %r != "
                              "frozen fixture expectation %r"
                              % (mat.get("manifest_digest"),
                                 digests["manifest_digest"]))
                if mat.get("bundle_digest") != digests["bundle_digest"]:
                    self.fail("s8", "materialized bundle digest %r != "
                              "frozen fixture expectation %r"
                              % (mat.get("bundle_digest"),
                                 digests["bundle_digest"]))
                if mat.get("digests_distinct") is not True:
                    self.fail("s8", "manifest digest and bundle rollup "
                              "digest collide (three-layer identity broken)")
                if mat.get("published_atomically") is not True:
                    self.fail("s8", "bundle publish was not atomic")
                if fx.lock_file_count is not None and \
                        mat.get("published_files") != fx.lock_file_count:
                    self.fail("s8", "published %r files, fixture manifest "
                              "declares %r"
                              % (mat.get("published_files"),
                                 fx.lock_file_count))
            if is_sha_digest(mat.get("manifest_digest")):
                self.digest_chain["s8_materialization"][
                    "manifest_digest"] = mat["manifest_digest"]
            if is_sha_digest(mat.get("bundle_digest")):
                self.digest_chain["s8_materialization"][
                    "bundle_digest"] = mat["bundle_digest"]

        # bundle_digest_as_ref: the bundle rollup digest must never pass
        # as the materialization ref (which is the MANIFEST digest).
        neg = self.call("rsih", "materialize.run",
                        {"mode": "bundle_digest_as_ref",
                         "case_id": LOCK_CASE_ID,
                         "materialization_id": LOCK_MATERIALIZATION_ID,
                         "activation_sequence": LOCK_ACTIVATION_SEQUENCE},
                        "s8", "bundle digest as materialization ref")
        if neg is not None:
            ev["bundle_digest_as_ref"] = neg
            refused = neg.get("ok") is not True
            if not refused:
                self.fail("s8", "bundle digest used as the materialization "
                          "ref (ok=%r, ref=%r) -- the ref digest must be "
                          "the manifest digest, never the bundle rollup"
                          % (neg.get("ok"),
                             neg.get("materialization_ref_digest")))
            self.record_negative("s8_bundle_digest_as_ref", "refused",
                                 {"reason_code": neg.get("reason_code"),
                                  "ref_is_manifest_digest":
                                      neg.get("ref_is_manifest_digest")},
                                 rejected=refused)

        # lock.freeze ok: the SkillLock ref is the manifest digest and the
        # bundle is complete.
        lock = self.call("rsih", "lock.freeze",
                         {"mode": "ok", "case_id": LOCK_CASE_ID,
                          "activation_sequence": LOCK_ACTIVATION_SEQUENCE},
                         "s8", "lock freeze " + LOCK_CASE_ID)
        if lock is not None:
            ev["lock"] = lock
            if lock.get("ok") is not True:
                self.fail("s8", "lock freeze of %s failed (reason %r)"
                          % (LOCK_CASE_ID, lock.get("reason_code")))
            else:
                if lock.get("lock_digest") != digests["lock_digest"]:
                    self.fail("s8", "lock digest %r != frozen fixture "
                              "expectation %r"
                              % (lock.get("lock_digest"),
                                 digests["lock_digest"]))
                if lock.get("materialization_ref_digest") != \
                        digests["manifest_digest"]:
                    self.fail("s8", "lock materialization ref digest %r != "
                              "manifest digest %r (identity guessing)"
                              % (lock.get("materialization_ref_digest"),
                                 digests["manifest_digest"]))
                if lock.get("ref_is_manifest_digest") is not True:
                    self.fail("s8", "lock ref is not the manifest digest "
                              "(ref_is_manifest_digest=%r)"
                              % lock.get("ref_is_manifest_digest"))
                if lock.get("bundle_files_published") != \
                        lock.get("bundle_files_expected"):
                    self.fail("s8", "lock froze over an incomplete bundle "
                              "(published %r of %r expected)"
                              % (lock.get("bundle_files_published"),
                                 lock.get("bundle_files_expected")))
            if is_sha_digest(lock.get("lock_digest")):
                self.digest_chain["s8_materialization"][
                    "lock_digest"] = lock["lock_digest"]

        # bundle_ref: freezing with the bundle digest as the ref must
        # refuse.
        neg = self.call("rsih", "lock.freeze",
                        {"mode": "bundle_ref", "case_id": LOCK_CASE_ID,
                         "activation_sequence": LOCK_ACTIVATION_SEQUENCE},
                        "s8", "bundle digest as lock ref")
        if neg is not None:
            ev["lock_bundle_ref"] = neg
            refused = neg.get("ok") is not True
            if not refused:
                self.fail("s8", "bundle digest used as the materialization "
                          "lock ref (ok=%r, ref_digest_used=%r) -- the lock "
                          "identity is the manifest digest, guessing the "
                          "bundle rollup must refuse"
                          % (neg.get("ok"), neg.get("ref_digest_used")))
            self.record_negative("s8_lock_bundle_ref", "refused",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # stale_sequence: freezing at a sequence the manifest did not create
        # from must refuse with MANIFEST_SEQUENCE_STALE.
        neg = self.call("rsih", "lock.freeze",
                        {"mode": "stale_sequence", "case_id": LOCK_CASE_ID,
                         "activation_sequence": LOCK_ACTIVATION_SEQUENCE},
                        "s8", "stale freeze sequence")
        if neg is not None:
            ev["lock_stale_sequence"] = neg
            refused = neg.get("ok") is not True
            if not refused or \
                    neg.get("reason_code") != "MANIFEST_SEQUENCE_STALE":
                self.fail("s8", "stale freeze sequence accepted by the lock "
                          "(ok=%r, reason=%r) -- the head moved between "
                          "closure read and freeze"
                          % (neg.get("ok"), neg.get("reason_code")))
            self.record_negative("s8_lock_stale_sequence",
                                 "MANIFEST_SEQUENCE_STALE",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # partial_publish: a bundle that landed only partially must never
        # freeze.
        neg = self.call("rsih", "lock.freeze",
                        {"mode": "partial_publish", "case_id": LOCK_CASE_ID,
                         "activation_sequence": LOCK_ACTIVATION_SEQUENCE},
                        "s8", "partial publish freeze")
        if neg is not None:
            ev["lock_partial_publish"] = neg
            refused = neg.get("ok") is not True
            if not refused:
                self.fail("s8", "partial publish: the lock froze over an "
                          "incomplete bundle (ok=%r, published %r of %r "
                          "expected, reason=%r) -- a partially published "
                          "bundle must refuse the freeze and stay visible"
                          % (neg.get("ok"),
                             neg.get("bundle_files_published"),
                             neg.get("bundle_files_expected"),
                             neg.get("reason_code")))
            self.record_negative("s8_lock_partial_publish", "refused",
                                 {"reason_code": neg.get("reason_code"),
                                  "bundle_files_published":
                                      neg.get("bundle_files_published"),
                                  "bundle_files_expected":
                                      neg.get("bundle_files_expected")},
                                 rejected=refused)
        self.evidence["s8_materialization"] = ev

    # -- S9: exact Composite + Pi registry -------------------------------------

    def run_s9(self):
        fx = self.fixtures
        child_ids = fx.composite_child_ids
        ev = {}
        ok = self.call("rsih", "composite.run", {"mode": "ok"},
                       "s9", "composite whole run")
        if ok is not None:
            ev["composite"] = ok
            if ok.get("status") != "succeeded":
                self.fail("s9", "composite run ended %r (reason %r)"
                          % (ok.get("status"), ok.get("reason_code")))
            else:
                if ok.get("child_ids") != child_ids:
                    self.fail("s9", "composite children %r != the frozen "
                              "exact children %r"
                              % (ok.get("child_ids"), child_ids))
                if ok.get("schedule") != child_ids:
                    self.fail("s9", "composite schedule %r != child_ids %r "
                              "(exact children, deterministic order)"
                              % (ok.get("schedule"), child_ids))
                if ok.get("permissions_within_host_cap") is not True:
                    self.fail("s9", "composite permissions exceeded the "
                              "host cap (union %r vs cap %r)"
                              % (ok.get("permission_union"),
                                 ok.get("host_cap")))
                if ok.get("ports_respected") is not True:
                    self.fail("s9", "composite run did not respect child "
                              "ports")
                if ok.get("committed_child_outputs") != len(child_ids):
                    self.fail("s9", "composite committed %r child outputs "
                              "(expected one per exact child)"
                              % ok.get("committed_child_outputs"))
            if ok.get("whole_replay_passed") is not True:
                self.fail("s9", "whole-composite replay did not pass "
                          "(whole_replay_passed=%r)"
                          % ok.get("whole_replay_passed"))
            if ok.get("trace_digest_stable") is not True:
                self.fail("s9", "composite trace digest is not stable "
                          "across the whole replay (nondeterministic "
                          "drift)")
            if not is_sha_digest(ok.get("trace_digest")):
                self.fail("s9", "composite trace digest %r is not a sha256 "
                          "digest" % ok.get("trace_digest"))
            if not is_sha_digest(ok.get("output_digest")):
                self.fail("s9", "composite output digest %r is not a "
                          "sha256 digest" % ok.get("output_digest"))
            if is_sha_digest(ok.get("trace_digest")):
                self.digest_chain["s9_composite"]["trace_digest"] = \
                    ok["trace_digest"]
            if is_sha_digest(ok.get("output_digest")):
                self.digest_chain["s9_composite"]["output_digest"] = \
                    ok["output_digest"]

        # dynamic_child: a dynamic (non-exact) child must fail closed.
        neg = self.call("rsih", "composite.run", {"mode": "dynamic_child"},
                        "s9", "dynamic child refusal")
        if neg is not None:
            ev["dynamic_child"] = neg
            refused = neg.get("status") != "succeeded"
            if not refused or neg.get("reason_code") != "NON_EXACT_REF":
                self.fail("s9", "dynamic (non-exact) child accepted by the "
                          "composite run (status=%r, reason=%r) -- children "
                          "must be exact locked refs"
                          % (neg.get("status"), neg.get("reason_code")))
            self.record_negative("s9_dynamic_child", "NON_EXACT_REF",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # cycle: a composite that composes a cycle must fail closed.
        neg = self.call("rsih", "composite.run", {"mode": "cycle"},
                        "s9", "composite cycle refusal")
        if neg is not None:
            ev["cycle"] = neg
            refused = neg.get("status") != "succeeded"
            if not refused or neg.get("reason_code") != "COMPOSITE_CYCLE":
                self.fail("s9", "composite cycle accepted by the run "
                          "(status=%r, reason=%r)" % (neg.get("status"),
                                                       neg.get("reason_code")))
            self.record_negative("s9_cycle", "COMPOSITE_CYCLE",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # drift: a nondeterministic replay must fail the whole replay.
        neg = self.call("rsih", "composite.run", {"mode": "drift"},
                        "s9", "replay drift refusal")
        if neg is not None:
            ev["drift"] = neg
            refused = neg.get("status") != "succeeded"
            if not refused or \
                    neg.get("reason_code") != "REPLAY_NONDETERMINISTIC":
                self.fail("s9", "nondeterministic composite drift accepted "
                          "(status=%r, reason=%r) -- whole replay must be "
                          "deterministic" % (neg.get("status"),
                                             neg.get("reason_code")))
            self.record_negative("s9_drift", "REPLAY_NONDETERMINISTIC",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # registry.resolve exact: the lock-backed registry resolves an
        # exact ref to its materialized file digest.
        exact = self.call("rsih", "registry.resolve",
                         {"mode": "exact", "lineage_id": "sg-alpha"},
                         "s9", "registry exact resolve")
        if exact is not None:
            ev["registry_exact"] = exact
            if exact.get("ok") is not True:
                self.fail("s9", "exact registry resolve failed (reason %r)"
                          % exact.get("reason_code"))
            else:
                if not exact.get("materialized_path"):
                    self.fail("s9", "exact resolve returned no materialized "
                              "path")
                if not is_sha_digest(exact.get("file_digest")):
                    self.fail("s9", "registry file digest %r is not a "
                              "sha256 digest" % exact.get("file_digest"))
            if is_sha_digest(exact.get("file_digest")):
                self.digest_chain["s9_composite"][
                    "registry_file_digest"] = exact["file_digest"]

        # candidate: a candidate channel is never executable input.
        neg = self.call("rsih", "registry.resolve",
                        {"mode": "candidate", "lineage_id": "sg-alpha"},
                        "s9", "registry candidate refusal")
        if neg is not None:
            ev["registry_candidate"] = neg
            refused = neg.get("ok") is not True
            if not refused or \
                    neg.get("reason_code") != "CANDIDATE_NOT_EXECUTABLE":
                self.fail("s9", "candidate ref resolved as executable input "
                          "(ok=%r, reason=%r) -- candidates never resolve"
                          % (neg.get("ok"), neg.get("reason_code")))
            self.record_negative("s9_registry_candidate",
                                 "CANDIDATE_NOT_EXECUTABLE",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)

        # latest: a latest/ambiguous form must refuse (exact refs only).
        neg = self.call("rsih", "registry.resolve",
                        {"mode": "latest", "lineage_id": "sg-alpha"},
                        "s9", "registry latest refusal")
        if neg is not None:
            ev["registry_latest"] = neg
            refused = neg.get("ok") is not True
            if not refused or neg.get("reason_code") != "NON_EXACT_REF":
                self.fail("s9", "latest/ambiguous form resolved by the "
                          "registry (ok=%r, reason=%r) -- only exact refs "
                          "resolve" % (neg.get("ok"),
                                       neg.get("reason_code")))
            self.record_negative("s9_registry_latest", "NON_EXACT_REF",
                                 {"reason_code": neg.get("reason_code")},
                                 rejected=refused)
        self.evidence["s9_composite"] = ev

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
            self.fail("s7", "driver host accepted unknown op no.such.op "
                      "(ok=true; unknown instructions must fail closed)")
        elif not error.get("code"):
            self.fail("s7", "driver host refused unknown op without a "
                      "closed error code (got %r)" % (response,))


def _trim_response(response):
    """Keep evidence copies readable (drop multi-KB canonical blobs)."""
    trimmed = {}
    for key, value in response.items():
        if key in ("canonical_b64", "closure_nodes_b64") and \
                isinstance(value, (str, list)) and len(str(value)) > 120:
            trimmed[key] = str(value)[:64] + "...(%d chars)" % len(str(value))
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
        Path(tempfile.mkdtemp(prefix="int003-s5s9-evidence-"))
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

    tracer.run_s5()
    tracer.run_s6()
    tracer.run_s7()
    tracer.run_s8()
    tracer.run_s9()
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

    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "gate": "INT-003",
        "generated_utc": datetime.datetime.now(
            datetime.timezone.utc).isoformat(timespec="seconds"),
        "fixtures_root": str(fixtures),
        "slices": {
            "s5": tracer.slice_failures["s5"] == 0,
            "s6": tracer.slice_failures["s6"] == 0,
            "s7": tracer.slice_failures["s7"] == 0,
            "s8": tracer.slice_failures["s8"] == 0,
            "s9": tracer.slice_failures["s9"] == 0,
        },
        "fixtures_immutable": fixtures_immutable,
        "digest_chain": tracer.digest_chain,
        "negatives_total": len(tracer.negatives),
        "toolchain": toolchain,
    }
    manifest = {
        "schema_version": EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "gate": "INT-003",
        "generated_utc": summary["generated_utc"],
        "slices": {name + ".json": name for name in CHAIN_STAGES},
        "negatives": "negatives.json",
        "digest_chain": "digest_chain.json",
        "summary": "summary.json",
    }
    evidence_docs = (
        ("s5_release", tracer.evidence.get("s5_release")),
        ("s6_projection", tracer.evidence.get("s6_projection")),
        ("s7_explore", tracer.evidence.get("s7_explore")),
        ("s8_materialization", tracer.evidence.get("s8_materialization")),
        ("s9_composite", tracer.evidence.get("s9_composite")),
        ("negatives", tracer.negatives),
        ("digest_chain", tracer.digest_chain),
        ("summary", summary),
        ("manifest", manifest),
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
        description="INT-003 S5-S9 GMS/RSIH/Host conformance tracer")
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
            print("INT-003 S5-S9 tracer: RED")
            print("failure: %s repository directory not found: %s"
                  % (name, path))
            return 1

    result = run_tracer(repo_dirs=repo_dirs, fixtures=args.fixtures,
                        evidence_out=args.evidence_out)

    toolchain = result.get("toolchain") or {}
    print("INT-003 S5-S9 tracer: %s" % ("GREEN" if result["ok"] else "RED"))
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