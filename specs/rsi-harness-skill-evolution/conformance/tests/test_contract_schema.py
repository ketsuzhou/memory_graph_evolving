"""CTR-005 machine-readable contract schema tests (TDD driver + conformance).

Covers implementation-plan CTR-005 (Contract 7-10/13-16 revision 16.6,
Host 5-7, GMS 2-11, RSIH 3-8/9.6):

- Red driver: ``test_all_dto_states_and_profiles_are_closed_and_digestable``
  asserts that an ordinary-proposal illegal state jump, a Composite
  terminal-state reopen, a negative and a floating profile limit, an
  undefined unit and an incomplete digest preimage are all rejected,
  while the frozen corpus (shared DTO schemas, state machines, profiles)
  is closed, total and digestable.
- Shared DTO conformance: every Contract 7/8 DTO (plus the ordinary
  ``SkillProposalEvent`` gap fill and the GMS 10.2-10.4 tool arguments)
  has a closed schema file whose ``schema_version`` const matches the
  Contract-declared value; no float bounds; no dangling or cyclic
  ``$ref``; conditional required fields enforced.
- State machine conformance: proposal/candidate/merge/composite/segment/
  projection machines are total (every referenced state defined, every
  non-terminal state has a legal exit, every state reachable from an
  initial state) and terminals can never reopen.
- Profile conformance: render/resource/permission/runtime profiles carry
  explicit units from the closed unit registry, integer-only min/max/
  default bounds, mandatory permission scopes, no nondeterministic
  runtime capability, and a complete, recomputable digest preimage.
- Drift check: the frozen CTR-002/CTR-004 policy digests recompute to
  their declared (and Contract-frozen) values; the schema-cases corpus
  validates positive and negative expectations exactly.
- Provenance: ``--emit-provenance`` lists every schema/policy file with
  its SHA-256.

Pure stdlib ``unittest``; loads the validator from
``../validate_contract.py``.
"""

import importlib.util
import json
import subprocess
import sys
import unittest
from pathlib import Path

FIX = Path(__file__).resolve().parents[1]

# The 21 Contract 7 shared DTOs + the CTR-005 additions, pinned to the
# exact schema_version const declared by the Contract / module specs.
EXPECTED_SHARED_DTOS = {
    "skill-artifact-ref.schema.json": "gms.skill-artifact-ref.v2",
    "candidate-artifact-ref.schema.json": "gms.candidate-artifact-ref.v2",
    "segment-ref.schema.json": "host.segment-ref.v1",
    "checkpoint-ref.schema.json": "host.checkpoint-ref.v1",
    "evidence-ref.schema.json": "gms.evidence-ref.v1",
    "skill-anchor.schema.json": "gms.skill-anchor.v1",
    "skill-proposal.schema.json": "gms.skill-proposal.v1",
    "proposal-event.schema.json": "gms.proposal-event.v1",
    "replay-request.schema.json": "gms.replay-request.v1",
    "replay-result.schema.json": "gms.replay-result.v1",
    "release-decision.schema.json": "gms.release-decision.v1",
    "activation-event.schema.json": "gms.activation-event.v1",
    "deactivation-event.schema.json": "gms.activation-event.v1",
    "projection-watermark.schema.json": "gms.projection-watermark.v1",
    "guidance-view.schema.json": "gms.guidance-view.v1",
    "explore-result.schema.json": "gms.explore-result.v1",
    "tool-proxy-request.schema.json": "host.tool-proxy-request.v1",
    "tool-proxy-result.schema.json": "host.tool-proxy-result.v1",
    "materialization-manifest.schema.json": "rsih.materialization-manifest.v1",
    "skill-lock.schema.json": "rsih.skill-lock.v1",
    "similarity-assessment.schema.json": "gms.similarity-assessment.v1",
    "merge-proposal.schema.json": "gms.merge-proposal.v1",
    "merge-proposal-event.schema.json": "gms.merge-proposal-event.v1",
    "memory-explore-arguments.schema.json": "gms.memory-explore-arguments.v1",
    "memory-expand-arguments.schema.json": "gms.memory-expand-arguments.v1",
    "skill-get-arguments.schema.json": "gms.skill-get-arguments.v1",
    "skill-artifact-envelope.schema.json": "gms.skill-artifact.v2",
}

EXPECTED_STATE_BUNDLES = {
    "proposal-lifecycle.schema.json": {"skill-proposal"},
    "candidate-lifecycle.schema.json": {"candidate-artifact", "runtime-revision"},
    "merge-lifecycle.schema.json": {"merge-proposal"},
    "composite-run.schema.json": {
        "composite-artifact",
        "composite-run",
        "composite-child-attempt",
    },
    "segment-lifecycle.schema.json": {"conversation-segment"},
    "projection-lifecycle.schema.json": {"projector"},
}

EXPECTED_PROFILES = {
    "render-profile.v1.json",
    "resource-profile.v1.json",
    "permission-profile.v1.json",
    "runtime-profile.v1.json",
}

# Contract 12.7.1 M1 / 13.7.1 R2 frozen policy digests (drift check).
FROZEN_POLICY_DIGESTS = {
    "policy/tool-success-validation.v1.json": (
        "policy_digest",
        "sha256:dde91eff2aaac31057512beba1c667e0b07b7cb4c36037db4e07eaced6f702f5",
    ),
    "policy/system-reason-codes.v1.json": (
        "registry_digest",
        "sha256:16410afa27498bb425885d4629c65309389f15adeb975f97f98674170ad00a2c",
    ),
    "policy/host-proxy-reason-codes.v1.json": (
        "registry_digest",
        "sha256:e48f27252bff3fd34868ef4bc5b56a678cf2a35d59f4cd7f2c71f78290485f5e",
    ),
}

_VR = None


def load_validator():
    """Load (and cache) the CTR-005 validator module."""
    global _VR
    if _VR is None:
        path = FIX / "validate_contract.py"
        if not path.exists():
            raise AssertionError(
                "CTR-005 Green missing: %s does not exist yet" % path
            )
        spec = importlib.util.spec_from_file_location(
            "validate_contract_ctr005", str(path)
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        _VR = module
    return _VR


class RedDriver(unittest.TestCase):
    """The implementation-plan named Red test (must fail before Green)."""

    def test_all_dto_states_and_profiles_are_closed_and_digestable(self):
        vr = load_validator()

        # (a) ordinary proposal illegal jump (proposed -> released).
        machine = vr.load_state_bundle(FIX / "schema" / "state"
                                       / "proposal-lifecycle.schema.json")
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_event_sequence(machine, "skill-proposal",
                                    "proposed",
                                    [{"from_state": "proposed",
                                      "to_state": "released",
                                      "trigger": "release"}])
        self.assertEqual(ctx.exception.code, "ILLEGAL_STATE_TRANSITION")

        # (b) Composite child terminal reopen (succeeded -> running).
        machine = vr.load_state_bundle(FIX / "schema" / "state"
                                       / "composite-run.schema.json")
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_event_sequence(machine, "composite-child-attempt",
                                    "pending",
                                    [{"from_state": "pending",
                                      "to_state": "ready",
                                      "trigger": "dependencies_satisfied"},
                                     {"from_state": "ready",
                                      "to_state": "running",
                                      "trigger": "scheduled"},
                                     {"from_state": "running",
                                      "to_state": "succeeded",
                                      "trigger": "child_succeeded"},
                                     {"from_state": "succeeded",
                                      "to_state": "running",
                                      "trigger": "rerun"}])
        self.assertEqual(ctx.exception.code, "TERMINAL_STATE_REOPEN")

        # (c) negative profile limit.
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_profile_limits([
                {"field": "max_file_bytes", "unit": "bytes",
                 "min": -1, "max": 1024, "default": 512},
            ])
        self.assertEqual(ctx.exception.code, "PROFILE_LIMIT_INVALID")

        # (d) floating profile limit.
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_profile_limits([
                {"field": "max_file_bytes", "unit": "bytes",
                 "min": 0, "max": 4096, "default": 512.5},
            ])
        self.assertEqual(ctx.exception.code, "PROFILE_LIMIT_INVALID")

        # (e) undefined unit.
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_profile_limits([
                {"field": "max_file_bytes", "unit": "kilobytes",
                 "min": 0, "max": 4096, "default": 512},
            ])
        self.assertEqual(ctx.exception.code, "PROFILE_UNIT_UNDEFINED")

        # (f) incomplete digest preimage (limits omitted).
        profile = {
            "schema_version": "rsih-skill-evolution.render-profile.v1",
            "profile_id": "render-profile",
            "profile_version": 1,
            "renderers": [],
            "limits": [{"field": "max_guidance_token_count", "unit": "tokens",
                        "min": 0, "max": 8192, "default": 2048}],
            "default_materialization": {"rule": "omit-fails-closed"},
            "digest_preimage": ["schema_version", "profile_id",
                                "profile_version", "renderers",
                                "default_materialization"],
            "profile_digest": "sha256:" + "0" * 64,
        }
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_profile_preimage(profile, required_groups=("renderers",
                                                                "limits"))
        self.assertEqual(ctx.exception.code, "DIGEST_PREIMAGE_INCOMPLETE")


class SharedDtoConformance(unittest.TestCase):
    def test_all_contract_dto_files_exist_with_matching_schema_version(self):
        vr = load_validator()
        shared_dir = FIX / "schema" / "shared"
        self.assertTrue(shared_dir.is_dir(),
                        "schema/shared/ missing (CTR-005 Green)")
        present = {p.name for p in shared_dir.glob("*.schema.json")}
        for name, const in EXPECTED_SHARED_DTOS.items():
            self.assertIn(name, present,
                          "missing shared DTO schema %s" % name)
            doc = json.loads((shared_dir / name).read_text(encoding="utf-8"))
            vr.validate_schema_document(doc, shared_dir / name)
            self.assertEqual(
                vr.declared_schema_version(doc, shared_dir / name), const,
                "%s schema_version const must equal the Contract value"
                % name,
            )
        # common value-type library must exist too.
        self.assertIn("common-types.schema.json", present)
        common = json.loads(
            (shared_dir / "common-types.schema.json").read_text("utf-8"))
        vr.validate_schema_document(common,
                                   shared_dir / "common-types.schema.json")

    def test_schemas_are_closed_integer_only_and_ref_acyclic(self):
        vr = load_validator()
        for sub in ("shared", "state"):
            for path in sorted((FIX / "schema" / sub).glob("*.schema.json")):
                if sub == "state":
                    vr.validate_state_document(
                        json.loads(path.read_text("utf-8")), path)
                else:
                    vr.validate_schema_document(
                        json.loads(path.read_text("utf-8")), path)

    def test_conditional_required_fields(self):
        vr = load_validator()
        shared = FIX / "schema" / "shared"

        activate = json.loads(
            (shared / "activation-event.schema.json").read_text("utf-8"))
        instance = {
            "schema_version": "gms.activation-event.v1",
            "activation_sequence": 1,
            "event_id": "act-x",
            "event_digest": "sha256:" + "a" * 64,
            "event_type": "activate",
            "lineage_id": "lin",
            "skill_ref": {
                "schema_version": "gms.skill-artifact-ref.v2",
                "lineage_id": "lin", "version": 1, "kind": "human_procedure",
                "artifact_digest": "sha256:" + "b" * 64,
            },
            "outbox_key": "sha256:" + "c" * 64,
        }
        # missing release_decision_ref for activate -> rejected
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.validate_instance(instance, activate, shared
                                 / "activation-event.schema.json")
        self.assertEqual(ctx.exception.code,
                         "SCHEMA_CONDITIONAL_REQUIRED_MISSING")
        # body_digest_equal must be true for activate
        instance["release_decision_ref"] = {"id": "d", "version": 1,
                                            "digest": "sha256:" + "d" * 64}
        instance["body_digest_equal"] = False
        with self.assertRaises(vr.ContractCheckError):
            vr.validate_instance(instance, activate, shared
                                 / "activation-event.schema.json")
        # deactivate profile must reject a release_decision_ref
        deactivate = json.loads(
            (shared / "deactivation-event.schema.json").read_text("utf-8"))
        bad = dict(instance)
        bad["event_type"] = "deactivate"
        with self.assertRaises(vr.ContractCheckError):
            vr.validate_instance(bad, deactivate, shared
                                 / "deactivation-event.schema.json")

        # ToolProxyResult: error required when failed.
        tpr = json.loads(
            (shared / "tool-proxy-result.schema.json").read_text("utf-8"))
        failed = {
            "schema_version": "host.tool-proxy-result.v1",
            "proxy_request_id": "pr-1",
            "status": "failed",
            "upstream_result_digest": "sha256:" + "e" * 64,
            "proxy_result_digest": "sha256:" + "f" * 64,
        }
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.validate_instance(failed, tpr, shared
                                 / "tool-proxy-result.schema.json")
        self.assertEqual(ctx.exception.code,
                         "SCHEMA_CONDITIONAL_REQUIRED_MISSING")


class StateMachineConformance(unittest.TestCase):
    def test_expected_bundles_total_and_terminated(self):
        vr = load_validator()
        state_dir = FIX / "schema" / "state"
        present = {p.name for p in state_dir.glob("*.schema.json")}
        self.assertEqual(present, set(EXPECTED_STATE_BUNDLES),
                         "state schema file set must match the frozen set")
        for name, machines in EXPECTED_STATE_BUNDLES.items():
            bundle = vr.load_state_bundle(state_dir / name)
            self.assertEqual({m["machine_id"] for m in bundle["machines"]},
                             machines)
            for machine in bundle["machines"]:
                vr.check_state_machine_totality(machine, state_dir / name)

    def test_happy_paths_accepted(self):
        vr = load_validator()
        state_dir = FIX / "schema" / "state"
        # ordinary proposal full happy path (Contract 9.1)
        machine = vr.load_state_bundle(state_dir
                                       / "proposal-lifecycle.schema.json")
        vr.check_event_sequence(
            machine, "skill-proposal", "proposed",
            [{"from_state": f, "to_state": t, "trigger": "ok"}
             for f, t in [
                 ("proposed", "admitted"),
                 ("admitted", "candidate_bound"),
                 ("candidate_bound", "validating"),
                 ("validating", "replay_pending"),
                 ("replay_pending", "replaying"),
                 ("replaying", "decision_pending"),
                 ("decision_pending", "activation_pending"),
                 ("activation_pending", "released"),
             ]])
        # merge happy path (Contract 9.4)
        machine = vr.load_state_bundle(state_dir
                                       / "merge-lifecycle.schema.json")
        vr.check_event_sequence(
            machine, "merge-proposal", "proposed",
            [{"from_state": f, "to_state": t, "trigger": "ok"}
             for f, t in [
                 ("proposed", "admitted"),
                 ("admitted", "synthesizing"),
                 ("synthesizing", "candidate_bound"),
                 ("candidate_bound", "validating"),
                 ("validating", "replaying"),
                 ("replaying", "decision_pending"),
                 ("decision_pending", "activation_pending"),
                 ("activation_pending", "released"),
             ]])


class ProfileConformance(unittest.TestCase):
    def test_four_profiles_present_units_limits_preimage(self):
        vr = load_validator()
        profiles_dir = FIX / "policy" / "profiles"
        present = {p.name for p in profiles_dir.glob("*.json")}
        self.assertEqual(present, EXPECTED_PROFILES,
                         "policy/profiles must contain exactly the four "
                         "frozen profiles")
        for name in sorted(EXPECTED_PROFILES):
            doc = json.loads((profiles_dir / name).read_text("utf-8"))
            vr.validate_profile_document(doc, profiles_dir / name)

    def test_permission_scope_and_runtime_determinism_guards(self):
        vr = load_validator()
        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_permission_entries(
                [{"capability": "memory.read", "effect": "allow"},
                 {"capability": "memory.read", "scope": "room",
                  "effect": "allow"}])
        self.assertEqual(ctx.exception.code, "PERMISSION_SCOPE_MISSING")

        with self.assertRaises(vr.ContractCheckError) as ctx:
            vr.check_runtime_capabilities(
                {"clock": {"binding": "wall_clock", "ambient_forbidden": True},
                 "random": {"binding": "injected", "source": "fake",
                            "ambient_forbidden": True}})
        self.assertEqual(ctx.exception.code,
                         "RUNTIME_NONDETERMINISM_CAPABILITY")


class DriftAndFixtureConformance(unittest.TestCase):
    def test_frozen_policy_digests_recompute(self):
        vr = load_validator()
        for rel, (field, frozen) in FROZEN_POLICY_DIGESTS.items():
            path = FIX / rel
            doc = json.loads(path.read_text("utf-8"))
            computed = vr.digest_of({k: v for k, v in doc.items()
                                     if k != field})
            self.assertEqual(doc[field], frozen,
                             "%s declared digest drifted from the Contract "
                             "freeze" % rel)
            self.assertEqual(computed, frozen,
                             "%s recomputed digest drift" % rel)

    def test_schema_cases_corpus_validates_against_expectations(self):
        vr = load_validator()
        report = vr.validate_schema_cases(FIX)
        self.assertGreaterEqual(report["total"], 40,
                                "schema-cases corpus too small")
        self.assertEqual(report["failures"], [],
                         "schema-cases expectation mismatches: %r"
                         % report["failures"])

    def test_full_root_validation_report_clean(self):
        vr = load_validator()
        report = vr.run_all_checks(FIX)
        self.assertEqual(report["failures"], [])
        self.assertGreaterEqual(len(report["checks"]),
                                4, "expected the four check families")


class CliAndProvenance(unittest.TestCase):
    def test_cli_exit_zero_with_provenance(self):
        proc = subprocess.run(
            [sys.executable, str(FIX / "validate_contract.py"),
             "--root", str(FIX), "--emit-provenance"],
            capture_output=True, text=True, cwd=str(FIX),
        )
        self.assertEqual(proc.returncode, 0,
                         "validate_contract.py failed:\n%s\n%s"
                         % (proc.stdout, proc.stderr))
        # provenance JSON is the trailing stdout document
        marker = "-----PROVENANCE-JSON-----"
        self.assertIn(marker, proc.stdout)
        provenance = json.loads(
            proc.stdout.split(marker, 1)[1].strip().splitlines()[0])
        listed = {entry["path"] for entry in provenance["files"]}
        for rel in list(EXPECTED_SHARED_DTOS) + \
                ["common-types.schema.json"]:
            self.assertIn("schema/shared/" + rel, listed)
        for rel in EXPECTED_STATE_BUNDLES:
            self.assertIn("schema/state/" + rel, listed)
        for rel in EXPECTED_PROFILES:
            self.assertIn("policy/profiles/" + rel, listed)
        for rel in FROZEN_POLICY_DIGESTS:
            self.assertIn(rel, listed)
        self.assertEqual(provenance["generated_by"]["artifact_type"],
                         "generated")
        self.assertEqual(
            provenance["source_of_truth"],
            "hand-written schemas under schema/ and policy/ are the "
            "source of truth; this manifest is a derived artifact",
        )


if __name__ == "__main__":
    unittest.main()
