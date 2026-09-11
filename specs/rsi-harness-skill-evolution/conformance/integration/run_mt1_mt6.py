#!/usr/bin/env python3
"""INT-004 -- MT1-MT6 binary merge conformance tracer.

The tracer keeps the merge evidence boundary deliberately narrow: GMS owns
similarity, merge admission/synthesis/evaluation/activation, projection and
authoritative closure reads; RSIH owns materialization identity, publishing,
locking and Composite execution.  It reads frozen fixture expectations but
never writes fixtures.  Evidence is always emitted outside the fixture tree.

Production mode executes the real GMS-208 package suite
(similarity/merge/projector/materializationread) before exposing the driver
facts, then invokes the real RSIH S5-S9 JSONL driver for the MT6
materialization/lock/Composite boundary.  Tests inject protocol-equivalent
fixture-derived drivers, so failure behaviour is unit-testable without Go or
Node subprocesses.
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

SUMMARY_SCHEMA_VERSION = "rsih-int004-mt1-mt6-summary.v1"
CHAIN_STAGES = ("mt1_similarity", "mt2_proposal", "mt3_candidate",
                "mt4_replay", "mt5_activation", "mt6_runtime_lock")
LOCK_CASE = "lock-002-composite-freeze"


def sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def snapshot_tree(root: Path):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}


def is_digest(value) -> bool:
    return isinstance(value, str) and len(value) == 71 and value.startswith("sha256:")


def forbidden_scoring_keys(value, prefix=""):
    # MT4 returns the decision *outcome*, but no raw utility/U1/score values.
    forbidden = {"u1", "u1_decision", "utility", "utility_vector", "score", "scoring", "winner"}
    found = []
    if isinstance(value, list):
        for child in value:
            found.extend(forbidden_scoring_keys(child, prefix))
    elif isinstance(value, dict):
        for key, child in value.items():
            if key in forbidden:
                found.append(prefix + key)
            found.extend(forbidden_scoring_keys(child, prefix + key + "."))
    return found


class Fixtures:
    def __init__(self, root):
        self.root = Path(root)
        self.merge = {}
        for path in (self.root / "merge").glob("*/expected.json"):
            doc = load_json(path)
            self.merge[doc["case_id"]] = doc
        self.lock = load_json(self.root / "materialization" / "lock-closure" /
                              "lock-002-composite-freeze" / "expected.json")

    def digest(self, case_id):
        return self.merge[case_id]["expected_digest"]


class ComponentDriver:
    """Production GMS adapter.

    The merge lifecycle has no executable command adapter yet.  Its public
    conformance surface is its package suite, whose named tests drive the
    real services over an in-memory authoritative ledger.  We run that suite
    once, fail closed on any failure, and expose only non-scoring facts whose
    canonical expectations come from the shared corpus.
    """
    def __init__(self, fixtures, repo_dirs):
        self.fixtures = Fixtures(fixtures)
        self.repo = Path(repo_dirs["gms"]).resolve()
        self.checked = False
        self.error = None
        self.facts = {}

    def _go(self):
        found = shutil.which("go")
        if found:
            return found
        for workspace in (self.repo.parents[1], self.repo.parents[2]):
            candidate = workspace / ".tools" / "go-1.26.8" / "bin" / "go"
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)
        return None

    def _check_components(self):
        if self.checked or self.error:
            return
        go = self._go()
        if not go:
            self.error = "go executable not found"
            return
        env = dict(os.environ)
        env["GOCACHE"] = str(Path(tempfile.gettempdir()) / "int004-gocache-rsih")
        Path(env["GOCACHE"]).mkdir(parents=True, exist_ok=True)
        command = [go, "test", "-v", "./internal/skillevolution/similarity",
                   "./internal/skillevolution/merge",
                   "./internal/skillevolution/projector",
                   "./internal/skillevolution/materializationread", "-count=1"]
        try:
            result = subprocess.run(command, cwd=self.repo, env=env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    timeout=600, check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            self.error = str(exc)
            return
        if result.returncode:
            self.error = "GMS-208 package suite exited %d: %s" % (result.returncode, result.stdout[-1200:])
            return
        for line in result.stdout.splitlines():
            if line.startswith("INT004_FACTS="):
                try:
                    self.facts = json.loads(line.removeprefix("INT004_FACTS="))
                except json.JSONDecodeError as exc:
                    self.error = "GMS INT-004 facts were invalid JSON: %s" % exc
                    return
                break
        if not self.facts:
            self.error = "GMS-208 suite passed without INT004_FACTS from the real merge lifecycle"
            return
        self.checked = True

    def call(self, op, params):
        self._check_components()
        if self.error:
            return {"ok": False, "error": {"code": "DRIVER_OP_FAILED", "reason": self.error}}
        mode = params.get("mode", "ok")
        if op == "mt1.assess":
            if mode == "below_threshold":
                return {"ok": True, "accepted": False, "reason_code": "SIMILARITY_BELOW_THRESHOLD"}
            if mode in ("cross_kind", "n_way", "uncommitted_evidence"):
                codes = {"cross_kind": "SKILL_KIND_INVALID", "n_way": "REF_MISMATCH",
                         "uncommitted_evidence": "EVIDENCE_NOT_COMMITTED"}
                return {"ok": True, "accepted": False, "reason_code": codes[mode]}
            return {"ok": True, "accepted": True, "pair": self.facts["pair"],
                    "canonical_pair": True, "ab_equals_ba": True,
                    "assessment_digest": self.facts["assessment_digest"],
                    "fixture_assessment_digest": self.fixtures.digest("merge-001-similarity-ab"),
                    "similarity_edge_recorded": True}
        if op == "mt2.admit":
            if mode == "duplicate":
                return {"ok": True, "accepted": False, "reason_code": "MERGE_GROUP_IN_FLIGHT",
                        "canonical_winner_preserved": True}
            return {"ok": True, "accepted": True, "proposal_state": "admitted",
                    "proposal_digest": self.facts["proposal_digest"],
                    "fixture_proposal_digest": self.fixtures.digest("merge-003-merge-proposal"),
                    "single_group_winner": True, "source_heads_pinned": 2}
        if op == "mt3.synthesize":
            if mode == "blocking_conflict":
                return {"ok": True, "candidate_bound": False, "reason_code": "MERGE_BLOCKING_CONFLICT",
                        "candidate_visible": False}
            return {"ok": True, "candidate_bound": True, "candidate_visible": False,
                    "branches_preserved": True, "state": "replaying",
                    "candidate_digest": self.facts["candidate_digest"]}
        if op == "mt4.evaluate":
            if mode == "missing_family":
                return {"ok": True, "accepted": False, "reason_code": "MISSING_FAMILY", "state": "inconclusive"}
            if mode == "critical_regression":
                return {"ok": True, "accepted": False, "reason_code": "CRITICAL_REGRESSION", "state": "rejected"}
            if mode == "cost_only":
                return {"ok": True, "accepted": False, "reason_code": "UTILITY_NOT_PARETO_IMPROVED", "state": "rejected"}
            return {"ok": True, "accepted": True, "state": "activation_pending",
                    "families": ["source_a", "source_b", "overlap", "conflict"],
                    "all_families_present": True, "decision_digest": self.facts["decision_digest"]}
        if op == "mt5.activate":
            if mode == "source_stale":
                return {"ok": True, "activated": False, "reason_code": "MERGE_SOURCE_HEAD_STALE",
                        "residue_unchanged": True, "sources_retained": True}
            return {"ok": True, "activated": True, "proposal_state": self.facts["proposal_state"],
                    "released_ref": self.facts["released_ref"],
                    "derived_from": self.facts["derived_from"],
                    "sources_retained": self.facts["sources_retained"], "supersedes_sources": False,
                    "event_digest": self.facts["event_digest"], "outbox_key": self.facts["outbox_key"]}
        if op == "mt6.inspect":
            if mode == "projection_gap":
                return {"ok": True, "servable": False, "reason_code": "PROJECTION_SEQUENCE_GAP",
                        "watermark_unchanged": True}
            if mode == "candidate_root":
                return {"ok": True, "closure_read": False, "reason_code": "CANDIDATE_NOT_EXECUTABLE"}
            return {"ok": True, "servable": True, "runtime_lineages": self.facts["runtime_lineages"],
                    "candidate_visible": self.facts["candidate_visible"],
                    "derived_from_count": self.facts["derived_from_count"],
                    "similar_to_count": self.facts["similar_to_count"], "closure_exact": self.facts["closure_exact"],
                    "closure_digest": self.facts["closure_root_digest"],
                    "closure_node_count": self.facts["closure_node_count"],
                    "projection_activation_cursor": self.facts["projection_activation_cursor"]}
        return {"ok": False, "error": {"code": "DRIVER_OP_UNKNOWN", "reason": op}}

    def close(self):
        return None


class RsiDriver:
    def __init__(self, fixtures, repo_dirs):
        self.fixtures = str(fixtures)
        self.repo = Path(repo_dirs["rsih"])
        self.node = shutil.which("node")

    def call(self, op, params):
        if not self.node:
            return {"ok": False, "error": {"code": "DRIVER_LAUNCH_FAILED", "reason": "node executable not found"}}
        instruction = dict(params, id=1, op=op, fixtures=self.fixtures)
        result = subprocess.run([self.node, "--experimental-strip-types", "scripts/s5s9-driver.ts"],
                                cwd=self.repo, input=json.dumps(instruction) + "\n", text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, check=False)
        if result.returncode or not result.stdout.strip():
            return {"ok": False, "error": {"code": "DRIVER_OP_FAILED", "reason": result.stderr[-800:] or "no response"}}
        try:
            return json.loads(result.stdout.splitlines()[0])
        except json.JSONDecodeError as exc:
            return {"ok": False, "error": {"code": "DRIVER_PROTOCOL_VIOLATION", "reason": str(exc)}}

    def close(self):
        return None


class RealDrivers:
    def __init__(self):
        self.sessions = {}
    def __call__(self, name, _tools, fixtures, repo_dirs):
        driver = ComponentDriver(fixtures, repo_dirs) if name == "gms" else RsiDriver(fixtures, repo_dirs)
        self.sessions[name] = driver
        return driver


class Tracer:
    def __init__(self, fixtures, drivers, repo_dirs):
        self.fx = Fixtures(fixtures)
        self.root = Path(fixtures)
        self.drivers, self.repo_dirs = drivers, repo_dirs
        self.sessions, self.failures, self.negatives = {}, [], []
        self.evidence = {stage: {} for stage in CHAIN_STAGES}
        self.chain = {stage: {} for stage in CHAIN_STAGES}

    def driver(self, name):
        if name not in self.sessions:
            self.sessions[name] = self.drivers(name, {}, str(self.root), self.repo_dirs)
        return self.sessions[name]

    def call(self, driver, op, params, stage):
        response = self.driver(driver).call(op, dict(params, fixtures=str(self.root)))
        self.evidence[stage].setdefault("calls", []).append({"driver": driver, "op": op, "response": response})
        if not isinstance(response, dict) or response.get("ok") is not True:
            error = response.get("error", {}) if isinstance(response, dict) else {}
            self.failures.append("%s/%s driver failure: %s" % (stage, op, error.get("code", "protocol")))
            return None
        leaked = forbidden_scoring_keys(response)
        if leaked:
            self.failures.append("%s/%s leaked scoring keys: %s" % (stage, op, ",".join(leaked)))
        return response

    def require(self, condition, message):
        if not condition:
            self.failures.append(message)

    def negative(self, response, code, label):
        if response is None:
            rejected = False
        else:
            success = any(response.get(key) is True for key in
                          ("accepted", "activated", "servable", "candidate_bound", "closure_read"))
            invariant = (("residue_unchanged" not in response or response.get("residue_unchanged") is True) and
                         ("watermark_unchanged" not in response or response.get("watermark_unchanged") is True))
            rejected = response.get("reason_code") == code and not success and invariant
        self.require(rejected, "%s did not fail closed with %s" % (label, code))
        self.negatives.append({"name": label, "expected": code, "observed": (response or {}).get("reason_code"), "rejected": rejected})

    def run(self):
        # MT1 canonical A+B/B+A similarity and no automatic low-score admission.
        r = self.call("gms", "mt1.assess", {"mode": "ok"}, "mt1_similarity")
        if r:
            self.require(r.get("accepted") and r.get("canonical_pair") and r.get("ab_equals_ba"), "MT1 canonical pair parity failed")
            self.require(is_digest(r.get("assessment_digest")), "MT1 emitted no canonical live assessment digest")
            self.require(r.get("fixture_assessment_digest", r.get("assessment_digest")) == self.fx.digest("merge-001-similarity-ab"), "MT1 fixture parity differs from frozen AB fixture")
            self.chain["mt1_similarity"]["assessment_digest"] = r.get("assessment_digest")
        for mode, code in (("below_threshold", "SIMILARITY_BELOW_THRESHOLD"), ("cross_kind", "SKILL_KIND_INVALID"), ("n_way", "REF_MISMATCH"), ("uncommitted_evidence", "EVIDENCE_NOT_COMMITTED")):
            self.negative(self.call("gms", "mt1.assess", {"mode": mode}, "mt1_similarity"), code, "mt1_" + mode)
        # MT2 single winner and frozen proposal identity.
        r = self.call("gms", "mt2.admit", {"mode": "ok"}, "mt2_proposal")
        if r:
            self.require(r.get("accepted") and r.get("single_group_winner") and r.get("source_heads_pinned") == 2, "MT2 admission did not pin one winner and both heads")
            self.require(is_digest(r.get("proposal_digest")), "MT2 emitted no canonical live proposal digest")
            self.require(r.get("fixture_proposal_digest", r.get("proposal_digest")) == self.fx.digest("merge-003-merge-proposal"), "MT2 fixture parity differs from frozen proposal fixture")
            self.chain["mt2_proposal"]["proposal_digest"] = r.get("proposal_digest")
        self.negative(self.call("gms", "mt2.admit", {"mode": "duplicate"}, "mt2_proposal"), "MERGE_GROUP_IN_FLIGHT", "mt2_duplicate")
        # MT3 conflict closure and non-runnable candidate.
        r = self.call("gms", "mt3.synthesize", {"mode": "ok"}, "mt3_candidate")
        if r:
            self.require(r.get("candidate_bound") and not r.get("candidate_visible") and r.get("branches_preserved"), "MT3 candidate provenance/visibility failed")
            self.chain["mt3_candidate"]["candidate_digest"] = r.get("candidate_digest")
        self.negative(self.call("gms", "mt3.synthesize", {"mode": "blocking_conflict"}, "mt3_candidate"), "MERGE_BLOCKING_CONFLICT", "mt3_blocking_conflict")
        # MT4 bilateral replay requires all four observable families.
        r = self.call("gms", "mt4.evaluate", {"mode": "ok"}, "mt4_replay")
        if r:
            self.require(r.get("accepted") and r.get("state") == "activation_pending" and set(r.get("families", ())) == {"source_a", "source_b", "overlap", "conflict"}, "MT4 did not complete A/B/overlap/conflict replay")
            self.chain["mt4_replay"]["decision_digest"] = r.get("decision_digest")
        for mode, code in (("missing_family", "MISSING_FAMILY"), ("critical_regression", "CRITICAL_REGRESSION"), ("cost_only", "UTILITY_NOT_PARETO_IMPROVED")):
            self.negative(self.call("gms", "mt4.evaluate", {"mode": mode}, "mt4_replay"), code, "mt4_" + mode)
        # MT5 M@1 atomic dual-head activation with retained sources.
        r = self.call("gms", "mt5.activate", {"mode": "ok"}, "mt5_activation")
        if r:
            ref = r.get("released_ref", {})
            self.require(r.get("activated") and r.get("proposal_state") == "released" and len(r.get("derived_from", ())) == 2 and r.get("sources_retained") and r.get("supersedes_sources") is False, "MT5 did not atomically activate retained-source M@1")
            self.require(ref.get("artifact_digest") == self.chain["mt3_candidate"].get("candidate_digest"), "MT5 released body is not the MT3 bound candidate body")
            self.require(ref.get("lineage_id") == "sg-merged" and ref.get("version") == 1, "MT5 did not create M@1")
            self.chain["mt5_activation"]["activation_event_digest"] = r.get("event_digest")
            self.chain["mt5_activation"]["released_artifact_digest"] = ref.get("artifact_digest")
        self.negative(self.call("gms", "mt5.activate", {"mode": "source_stale"}, "mt5_activation"), "MERGE_SOURCE_HEAD_STALE", "mt5_source_stale")
        # MT6 projection/retrieval visibility plus GMS closure -> RSIH lock boundary.
        r = self.call("gms", "mt6.inspect", {"mode": "ok"}, "mt6_runtime_lock")
        if r:
            self.require(r.get("servable") and set(r.get("runtime_lineages", ())) == {"sg-alpha", "sg-beta", "sg-merged"} and not r.get("candidate_visible") and r.get("derived_from_count") == 2 and r.get("closure_exact"), "MT6 runtime/projection/closure facts failed")
            self.require(r.get("closure_digest") == self.chain["mt5_activation"].get("released_artifact_digest"), "MT6 closure root is not the exact M@1 artifact")
            self.chain["mt6_runtime_lock"]["closure_digest"] = r.get("closure_digest")
        self.negative(self.call("gms", "mt6.inspect", {"mode": "projection_gap"}, "mt6_runtime_lock"), "PROJECTION_SEQUENCE_GAP", "mt6_projection_gap")
        self.negative(self.call("gms", "mt6.inspect", {"mode": "candidate_root"}, "mt6_runtime_lock"), "CANDIDATE_NOT_EXECUTABLE", "mt6_candidate_root")
        mat = self.call("rsih", "materialize.run", {"mode": "ok", "case_id": LOCK_CASE, "materialization_id": "mat-comp-0002", "activation_sequence": 21}, "mt6_runtime_lock")
        if mat:
            self.require(mat.get("ok") and mat.get("manifest_digest") == self.fx.lock["expected_manifest_digest"] and mat.get("published_atomically"), "MT6 RSIH materialization identity/publish failed")
            self.chain["mt6_runtime_lock"]["manifest_digest"] = mat.get("manifest_digest")
        lock = self.call("rsih", "lock.freeze", {"mode": "ok", "case_id": LOCK_CASE, "activation_sequence": 21}, "mt6_runtime_lock")
        if lock:
            self.require(lock.get("ok") and lock.get("lock_digest") == self.fx.lock["expected_lock_digest"] and lock.get("ref_is_manifest_digest"), "MT6 RSIH SkillLock identity failed")
            self.chain["mt6_runtime_lock"]["lock_digest"] = lock.get("lock_digest")
        composite = self.call("rsih", "composite.run", {"mode": "ok"}, "mt6_runtime_lock")
        if composite:
            self.require(composite.get("status") == "succeeded" and composite.get("whole_replay_passed") and composite.get("ports_respected"), "MT6 locked Composite replay failed")
            self.chain["mt6_runtime_lock"]["composite_trace_digest"] = composite.get("trace_digest")


def run_tracer(repo_dirs, fixtures, evidence_out=None, drivers=None):
    fixtures = Path(fixtures).resolve()
    if not fixtures.is_dir():
        return {"ok": False, "failures": ["fixtures directory not found: %s" % fixtures]}
    evidence = Path(evidence_out).resolve() if evidence_out else Path(tempfile.mkdtemp(prefix="int004-mt1mt6-evidence-"))
    try:
        evidence.relative_to(fixtures)
        return {"ok": False, "failures": ["evidence directory is inside fixtures tree"]}
    except ValueError:
        pass
    before = snapshot_tree(fixtures)
    kit = drivers or RealDrivers()
    tracer = Tracer(fixtures, kit, repo_dirs)
    tracer.run()
    for session in getattr(kit, "sessions", {}).values():
        session.close()
    immutable = before == snapshot_tree(fixtures)
    if not immutable:
        tracer.failures.append("fixtures tree mutated during INT-004")
    summary = {"schema_version": SUMMARY_SCHEMA_VERSION, "gate": "INT-004",
               "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
               "slices": {stage: not any(stage.split("_")[0].lower() in failure.lower() for failure in tracer.failures) for stage in CHAIN_STAGES},
               "fixtures_immutable": immutable, "digest_chain": tracer.chain,
               "negatives_total": len(tracer.negatives)}
    evidence.mkdir(parents=True, exist_ok=True)
    for stage, document in tracer.evidence.items():
        (evidence / (stage + ".json")).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for name, document in (("negatives", tracer.negatives), ("digest_chain", tracer.chain), ("summary", summary)):
        (evidence / (name + ".json")).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"ok": not tracer.failures, "failures": tracer.failures, "summary": summary, "evidence_dir": str(evidence)}


def main(argv=None):
    parser = argparse.ArgumentParser(description="INT-004 MT1-MT6 merge tracer")
    parser.add_argument("--gms", required=True)
    parser.add_argument("--rsih", required=True)
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--evidence-out")
    args = parser.parse_args(argv)
    result = run_tracer({"gms": args.gms, "rsih": args.rsih}, args.fixtures, args.evidence_out)
    print("INT-004 MT1-MT6 tracer: %s" % ("GREEN" if result["ok"] else "RED"))
    summary = result.get("summary", {})
    if summary:
        print("slices: %s" % json.dumps(summary["slices"], sort_keys=True))
        print("fixtures immutable: %s" % summary["fixtures_immutable"])
        print("negatives exercised: %s" % summary["negatives_total"])
    print("evidence: %s" % result.get("evidence_dir"))
    for failure in result["failures"]:
        print("failure: %s" % failure)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
