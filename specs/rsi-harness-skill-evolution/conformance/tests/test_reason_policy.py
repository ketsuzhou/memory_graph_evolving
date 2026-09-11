"""CTR-004 system reason-code registry policy tests (TDD driver + conformance).

Covers implementation-plan CTR-004 (Contract 13.2-13.7 revision 13.7.1,
Host 5.8-5.9/8, GMS 11.2-11.5, RSIH 9.1-9.4):

- Red driver: ``test_precedence_status_retry_and_new_attempt_are_total``
  asserts that duplicate codes, codes without a complete status/retry/
  retry_scope/terminal mapping, a Host registry forging a GMS-owned code
  and message-text-driven fallback are all rejected, while precedence
  resolution stays total over every code in both registries.
- Registry conformance: the two policy JSONs under ``../policy`` match the
  frozen spec enums (GMS 11.3-11.4 canonical value domain incl. the frozen
  retry/new-attempt/inconclusive classification, the Host 5.9 closed set,
  and the FND-001 / FND-002 adoptions), their JCS digests are
  self-consistent, and the GMS canonical policy body
  (``gms.reason-codes.v1``) is mechanically derivable.
- Fixture conformance: every case under ``../reasons`` validates.
- Fail-closed: digest tampering, cross-registry ownership forging,
  same-request retry of a stale-CAS code, retrying a semantic failure into
  pass, late results rewriting terminal state and free-message fallback
  are all rejected.

Pure stdlib ``unittest``; loads the validator from
``../validate_reason_policy.py``.
"""

import copy
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

FIX = Path(__file__).resolve().parents[1]
POLICY_DIR = FIX / "policy"
REASONS_DIR = FIX / "reasons"

# Host spec 5.9 closed v1 set (independently pinned here): 12 Host-owned
# codes plus the two GMS-owned shared codes the Host MAY emit/passthrough.
HOST_SPEC_5_9_SET = frozenset(
    {
        "HOST_PROXY_INVALID_REQUEST",
        "HOST_AGENT_NOT_IN_ROOM",
        "HOST_SCOPE_DENIED",
        "HOST_TOOL_NOT_ALLOWED",
        "HOST_PROXY_TIMEOUT",
        "HOST_PROXY_CANCELLED",
        "GMS_UNAVAILABLE",
        "UPSTREAM_SCHEMA_INVALID",
        "UPSTREAM_DIGEST_MISMATCH",
        "UPSTREAM_SCOPE_VIOLATION",
        "UPSTREAM_BUDGET_VIOLATION",
        "PROJECTION_BEHIND_REQUIRED_SEQUENCE",
        "IDEMPOTENCY_CONFLICT",
        "PI_RETURN_CHANNEL_FAILED",
    }
)

_VR = None


def load_validator():
    """Load (and cache) the CTR-004 validator module."""
    global _VR
    if _VR is None:
        path = FIX / "validate_reason_policy.py"
        if not path.exists():
            raise AssertionError(
                "CTR-004 Green missing: %s does not exist yet" % path
            )
        spec = importlib.util.spec_from_file_location(
            "validate_reason_policy_ctr004", str(path)
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module  # dataclasses needs it registered
        spec.loader.exec_module(module)
        _VR = module
    return _VR


def read_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(obj, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


class PolicyMutationMixin:
    """Helpers for mutation (fail-closed) tests over policy documents.

    Mutations are RE-SIGNED (``registry_digest`` recomputed) by default so
    that behavioral checks - ownership, totality, precedence, spec drift -
    are exercised against a self-consistently digested registry; only the
    dedicated digest-tamper test skips the re-signing.
    """

    VR = None

    def load_pristine_documents(self):
        return {
            "system": read_json(POLICY_DIR / self.VR.SYSTEM_POLICY_FILENAME),
            "host-proxy": read_json(POLICY_DIR / self.VR.HOST_POLICY_FILENAME),
        }

    def resign(self, docs):
        for doc in docs.values():
            body = {k: v for k, v in doc.items() if k != "registry_digest"}
            doc["registry_digest"] = self.VR.digest_of(body)

    def expect_reject(self, mutator, reason_code, resign=True):
        docs = self.load_pristine_documents()
        mutator(docs)
        if resign:
            self.resign(docs)
        with self.assertRaises(self.VR.PolicyError) as ctx:
            self.VR.ReasonPolicyBundle.from_documents(
                docs["system"], docs["host-proxy"])
        self.assertEqual(ctx.exception.reason_code, reason_code,
                         "unexpected rejection detail: %s" % ctx.exception)


class ReasonPolicyTestCase(PolicyMutationMixin, unittest.TestCase):
    """Base fixture: validator module + pristine policy bundle."""

    def setUp(self):
        self.VR = load_validator()
        self.bundle = self.VR.ReasonPolicyBundle.load(POLICY_DIR)
        self.resolver = self.bundle.resolver


class PrecedenceTotalityTests(ReasonPolicyTestCase):
    """The CTR-004 Red driver: precedence/status/retry/new-attempt totality."""

    def test_precedence_status_retry_and_new_attempt_are_total(self):
        vr = self.VR
        bundle = self.bundle

        # --- totality: every code in both registries yields a complete,
        # internally consistent decision (no missing mapping anywhere).
        observed = 0
        for registry in (bundle.system, bundle.host):
            for code in registry.entries():
                if code.status == "success":
                    # Success/truncation metadata is deterministically
                    # refused entry into any error slot - that refusal is
                    # part of the total mapping.
                    with self.assertRaises(vr.PolicyError) as ctx:
                        bundle.resolver.resolve(upstream_code=code.name)
                    self.assertEqual(ctx.exception.reason_code,
                                     "POLICY_PRECEDENCE_INVALID", code.name)
                    continue
                if registry is bundle.system:
                    decision = bundle.resolver.resolve(upstream_code=code.name)
                else:
                    decision = bundle.resolver.resolve(host_code=code.name)
                observed += 1
                self.assertIn(decision.registry_status, vr.STATUS_VALUES,
                              code.name)
                self.assertIsInstance(decision.retryable, bool, code.name)
                self.assertIn(decision.retry_scope, vr.RETRY_SCOPE_VALUES,
                              code.name)
                self.assertIsInstance(decision.terminal, bool, code.name)
                self.assertIn(decision.wire_status, (
                    "succeeded", "failed", "inconclusive"), code.name)
                self.assertEqual(
                    decision.same_request_retry_permitted,
                    decision.retry_scope == "same_request", code.name)
                self.assertEqual(
                    decision.new_attempt_required,
                    decision.retry_scope == "new_attempt", code.name)
                self.assertFalse(decision.may_rewrite_terminal, code.name)
        expected_observed = sum(
            1 for registry in (bundle.system, bundle.host)
            for code in registry.entries() if code.status != "success")
        self.assertEqual(observed, expected_observed)

        # --- rejection: duplicate code inside one registry.
        def duplicate_code(docs):
            doc = docs["system"]
            cloned = copy.deepcopy(next(
                c for c in doc["codes"] if c["name"] == "SEAL_DIGEST_MISMATCH"))
            doc["codes"].append(cloned)

        self.expect_reject(duplicate_code, "POLICY_CODE_DUPLICATED")

        # --- rejection: code without a complete mapping (missing field).
        def drop_mapping_field(docs):
            doc = docs["system"]
            code = next(c for c in doc["codes"]
                        if c["name"] == "SOURCE_HEAD_STALE")
            del code["retry_scope"]

        self.expect_reject(drop_mapping_field, "POLICY_MAPPING_NOT_TOTAL")

        # --- rejection: Host registry forging a GMS-owned code.
        def host_forges_gms_code(docs):
            stolen = copy.deepcopy(next(
                c for c in docs["system"]["codes"]
                if c["name"] == "DIGEST_MISMATCH"))
            stolen["owner"] = "host-proxy"
            docs["host-proxy"]["codes"].append(stolen)

        self.expect_reject(host_forges_gms_code, "POLICY_CODE_OVERLAP")

        # --- rejection: message-driven inference is structurally impossible;
        # a forged "timeout" message must not change the unknown-code result.
        plain = bundle.resolver.resolve(upstream_code="DEFINITELY_NOT_A_CODE")
        spiced = bundle.resolver.resolve(
            upstream_code="DEFINITELY_NOT_A_CODE",
            message="honestly, this is just a slow GMS timeout")
        self.assertEqual(plain, spiced)
        self.assertEqual(plain.wire_reason_code, "UPSTREAM_SCHEMA_INVALID")
        self.assertFalse(plain.passthrough)
        self.assertEqual(plain.upstream_code_as_metadata,
                         "DEFINITELY_NOT_A_CODE")

    def test_precedence_order_is_host_local_first(self):
        # Host-local validation failure wins over any upstream code; the
        # upstream code is demoted to non-behavioral metadata.
        decision = self.resolver.resolve(
            host_code="UPSTREAM_DIGEST_MISMATCH",
            upstream_code="DIGEST_MISMATCH")
        self.assertEqual(decision.origin, "host-local-validation")
        self.assertEqual(decision.wire_reason_code, "UPSTREAM_DIGEST_MISMATCH")
        self.assertFalse(decision.passthrough)
        self.assertEqual(decision.upstream_code_as_metadata, "DIGEST_MISMATCH")

    def test_known_upstream_code_passes_through_verbatim(self):
        decision = self.resolver.resolve(upstream_code="MERGE_BLOCKING_CONFLICT")
        self.assertEqual(decision.origin, "upstream-passthrough")
        self.assertEqual(decision.wire_reason_code, "MERGE_BLOCKING_CONFLICT")
        self.assertTrue(decision.passthrough)
        self.assertIsNone(decision.upstream_code_as_metadata)
        self.assertEqual(decision.registry_status, "failure")
        self.assertFalse(decision.retryable)
        self.assertEqual(decision.retry_scope, "none")
        self.assertTrue(decision.terminal)

    def test_success_truncation_codes_never_enter_error_slot(self):
        with self.assertRaises(self.VR.PolicyError) as ctx:
            self.resolver.resolve(upstream_code="TOTAL_CAP_REACHED")
        self.assertEqual(ctx.exception.reason_code, "POLICY_PRECEDENCE_INVALID")


class RegistryConformanceTests(ReasonPolicyTestCase):
    """The two policy JSONs must match the frozen spec enums and digests."""

    def test_policy_digests_self_consistent(self):
        for filename in (self.VR.SYSTEM_POLICY_FILENAME,
                         self.VR.HOST_POLICY_FILENAME):
            doc = read_json(POLICY_DIR / filename)
            declared = doc["registry_digest"]
            body = {k: v for k, v in doc.items() if k != "registry_digest"}
            self.assertEqual(
                self.VR.digest_of(body), declared,
                "%s registry_digest mismatch" % filename)
        # Loading the pristine policy dir already passed (setUp), i.e. the
        # validator itself recomputes and accepts the same digests.

    def test_system_registry_contains_frozen_spec_enums(self):
        vr = self.VR
        system = self.bundle.system
        names = system.names()
        # Exact membership and canonical order (GMS 11.4 groups first).
        self.assertEqual(names, list(vr.SYSTEM_CODE_ORDER))
        gms_failure = [n for n in names
                       if system.entry(n).group in vr.GMS_FAILURE_GROUPS]
        self.assertEqual(gms_failure, list(vr.GMS_FAILURE_CODE_ORDER))
        self.assertEqual(len(gms_failure), 112)
        truncation = [n for n in names
                      if system.entry(n).group == vr.GMS_TRUNCATION_GROUP]
        self.assertEqual(truncation, list(vr.GMS_TRUNCATION_CODES))
        for name in truncation:
            self.assertEqual(system.entry(name).status, "success", name)
        for name in vr.FND001_ADOPTED_CODES:
            self.assertIn(name, names)
            self.assertEqual(system.entry(name).status, "failure", name)

    def test_gms_frozen_classification_is_derivable(self):
        vr = self.VR
        system = self.bundle.system
        retry_same = tuple(
            n for n in vr.GMS_FAILURE_CODE_ORDER
            if system.entry(n).retry_scope == "same_request")
        new_attempt = tuple(
            n for n in vr.GMS_FAILURE_CODE_ORDER
            if system.entry(n).retry_scope == "new_attempt")
        inconclusive = tuple(
            n for n in vr.GMS_FAILURE_CODE_ORDER
            if system.entry(n).status == "inconclusive")
        self.assertEqual(retry_same, vr.GMS_RETRY_SAME_REQUEST_EXPECTED)
        self.assertEqual(new_attempt, vr.GMS_NEW_ATTEMPT_EXPECTED)
        self.assertEqual(inconclusive, vr.GMS_INCONCLUSIVE_EXPECTED)

    def test_same_request_scope_is_limited_to_infra_whitelist(self):
        same_request = sorted(
            n for registry in (self.bundle.system, self.bundle.host)
            for n in registry.names()
            if registry.entry(n).retry_scope == "same_request")
        self.assertEqual(same_request,
                         sorted(self.VR.INFRA_SAME_REQUEST_WHITELIST))

    def test_host_registry_matches_host_spec_5_9(self):
        host = self.bundle.host
        self.assertEqual(host.names(), list(self.VR.HOST_OWNED_CODES_EXPECTED))
        shared = self.bundle.host_shared_emit_codes()
        self.assertEqual(shared, ["IDEMPOTENCY_CONFLICT",
                                 "PROJECTION_BEHIND_REQUIRED_SEQUENCE"])
        self.assertTrue(set(shared) <= set(self.bundle.system.names()))
        self.assertFalse(set(host.names()) & set(self.bundle.system.names()))
        self.assertEqual(
            frozenset(host.names()) | frozenset(shared), HOST_SPEC_5_9_SET)

    def test_gms_canonical_policy_body_derivation(self):
        vr = self.VR
        body = self.bundle.derive_gms_policy_body()
        self.assertEqual(
            set(body.keys()),
            {"schema_version", "policy_id", "version", "failure_codes",
             "truncation_codes", "retry_same_request_codes",
             "new_attempt_required_codes", "inconclusive_status_codes"})
        self.assertEqual(body["schema_version"], "gms.reason-policy.v1")
        self.assertEqual(body["policy_id"], "gms.reason-codes")
        self.assertEqual(body["version"], 1)
        self.assertEqual(len(body["failure_codes"]), 112)
        self.assertEqual(len(body["truncation_codes"]), 4)
        self.assertEqual(body["failure_codes"], list(vr.GMS_FAILURE_CODE_ORDER))
        self.assertEqual(vr.digest_of(body), vr.GMS_POLICY_BODY_DIGEST)


class ThreeEndAndRetryTests(ReasonPolicyTestCase):
    """Same failure -> identical status/retry/new-attempt on all three ends."""

    def test_three_end_consistency_for_same_failure(self):
        decision = self.resolver.resolve(upstream_code="DIGEST_MISMATCH")
        views = self.resolver.three_end_view(decision)
        self.assertEqual(views["gms"], views["host"])
        self.assertEqual(views["host"], views["rsih"])
        self.assertEqual(views["gms"], {
            "status": "failure",
            "wire_status": "failed",
            "retryable": False,
            "retry_scope": "none",
            "same_request_retry_permitted": False,
            "new_attempt_required": False,
        })

    def test_infra_failure_retries_same_request(self):
        for observation in ({"upstream_code": "PROJECTION_BEHIND_REQUIRED_SEQUENCE"},
                            {"host_code": "GMS_UNAVAILABLE"},
                            {"host_code": "HOST_PROXY_TIMEOUT"}):
            decision = self.resolver.resolve(**observation)
            self.assertTrue(decision.retryable, observation)
            self.assertEqual(decision.retry_scope, "same_request", observation)
            self.assertFalse(decision.terminal, observation)
            self.assertFalse(decision.new_attempt_required, observation)
            self.assertTrue(self.resolver.same_request_retry_is_legal(decision))
            self.resolver.assert_same_request_retry_legal(decision)  # no raise

    def test_stale_cas_requires_new_attempt_not_same_request(self):
        decision = self.resolver.resolve(upstream_code="SOURCE_HEAD_STALE")
        self.assertFalse(decision.retryable)
        self.assertEqual(decision.retry_scope, "new_attempt")
        self.assertTrue(decision.terminal)
        self.assertTrue(decision.new_attempt_required)
        self.assertFalse(decision.same_request_retry_permitted)
        with self.assertRaises(self.VR.PolicyError) as ctx:
            self.resolver.assert_same_request_retry_legal(decision)
        self.assertEqual(ctx.exception.reason_code, "POLICY_RETRY_ILLEGAL")

    def test_semantic_failure_cannot_retry_into_pass(self):
        decision = self.resolver.resolve(upstream_code="DIGEST_MISMATCH")
        self.assertFalse(decision.retryable)
        self.assertEqual(decision.retry_scope, "none")
        self.assertTrue(decision.terminal)
        # Resolution is a pure function of the code: re-"retrying" the same
        # semantic failure on the same request deterministically re-derives
        # the identical failure decision; it can never clear into pass.
        self.assertEqual(decision, self.resolver.resolve(
            upstream_code="DIGEST_MISMATCH"))
        with self.assertRaises(self.VR.PolicyError) as ctx:
            self.resolver.assert_same_request_retry_legal(decision)
        self.assertEqual(ctx.exception.reason_code, "POLICY_RETRY_ILLEGAL")

    def test_idempotency_conflict_is_new_attempt_shared_code(self):
        decision = self.resolver.resolve(host_code="IDEMPOTENCY_CONFLICT")
        self.assertEqual(decision.origin, "host-local-validation")
        self.assertEqual(decision.retry_scope, "new_attempt")
        self.assertTrue(decision.new_attempt_required)
        self.assertFalse(decision.same_request_retry_permitted)

    def test_inconclusive_mapping(self):
        decision = self.resolver.resolve(upstream_code="REPLAY_INCONCLUSIVE")
        self.assertEqual(decision.registry_status, "inconclusive")
        self.assertEqual(decision.wire_status, "inconclusive")
        self.assertFalse(decision.retryable)
        self.assertEqual(decision.retry_scope, "none")
        self.assertTrue(decision.terminal)

    def test_late_result_is_audit_only_and_cannot_rewrite_terminal(self):
        prior = self.resolver.resolve(host_code="UPSTREAM_DIGEST_MISMATCH")
        merged = self.resolver.apply_late_result(prior, late_upstream=True)
        self.assertEqual(merged.wire_reason_code, prior.wire_reason_code)
        self.assertEqual(merged.registry_status, prior.registry_status)
        self.assertEqual(merged.retryable, prior.retryable)
        self.assertEqual(merged.retry_scope, prior.retry_scope)
        self.assertEqual(merged.terminal, prior.terminal)
        self.assertFalse(merged.may_rewrite_terminal)
        late = self.resolver.resolve(late_upstream=True)
        self.assertTrue(late.audit_only)
        self.assertIsNone(late.wire_reason_code)
        self.assertFalse(late.may_rewrite_terminal)

    def test_unknown_upstream_code_fail_closed(self):
        decision = self.resolver.resolve(upstream_code="GMS_FUTURE_V2_CODE")
        self.assertEqual(decision.origin, "unknown-upstream-replaced")
        self.assertEqual(decision.wire_reason_code, "UPSTREAM_SCHEMA_INVALID")
        self.assertFalse(decision.passthrough)
        self.assertEqual(decision.upstream_code_as_metadata,
                         "GMS_FUTURE_V2_CODE")
        self.assertEqual(decision.registry_status, "failure")
        self.assertFalse(decision.retryable)
        self.assertEqual(decision.retry_scope, "none")
        self.assertTrue(decision.terminal)


class FailClosedMutationTests(PolicyMutationMixin, unittest.TestCase):
    """Mutated registries must be rejected with the closed validator codes."""

    def setUp(self):
        self.VR = load_validator()

    def test_digest_tampering_rejected(self):
        def tamper(docs):
            code = next(c for c in docs["system"]["codes"]
                        if c["name"] == "DIGEST_MISMATCH")
            code["notes"] = "silently rewritten note"
        # NOT re-signed: the tampered body must fail digest verification.
        self.expect_reject(tamper, "POLICY_DIGEST_MISMATCH", resign=False)

    def test_host_forging_recorded_system_code_rejected(self):
        def forge(docs):
            stolen = copy.deepcopy(next(
                c for c in docs["system"]["codes"]
                if c["name"] == "CANDIDATE_IN_LIVE_REGISTRY"))
            stolen["owner"] = "host-proxy"
            docs["host-proxy"]["codes"].append(stolen)
        self.expect_reject(forge, "POLICY_CODE_OVERLAP")

    def test_inconsistent_totality_rejected(self):
        def flip(docs):
            code = next(c for c in docs["system"]["codes"]
                        if c["name"] == "SOURCE_HEAD_STALE")
            code["retryable"] = True  # new_attempt scope must not be retryable
        self.expect_reject(flip, "POLICY_MAPPING_NOT_TOTAL")

    def test_host_unknown_replacement_drift_rejected(self):
        def drift(docs):
            docs["host-proxy"]["transport_mapping"][
                "unknown_upstream_replacement"]["replacement_code"] = \
                "HOST_PROXY_TIMEOUT"
        self.expect_reject(drift, "POLICY_PRECEDENCE_INVALID")

    def test_spec_enum_drift_rejected(self):
        def drop_code(docs):
            docs["system"]["codes"] = [
                c for c in docs["system"]["codes"]
                if c["name"] != "SEAL_DIGEST_MISMATCH"]
        self.expect_reject(drop_code, "POLICY_SPEC_ENUM_DRIFT")


class ReasonFixtureTests(unittest.TestCase):
    """The $FIX/reasons fixture corpus must validate against the policy."""

    @classmethod
    def setUpClass(cls):
        cls.VR = load_validator()
        cls.bundle = cls.VR.ReasonPolicyBundle.load(POLICY_DIR)

    def test_all_fixture_cases_pass(self):
        report = self.VR.validate_reasons(self.bundle, REASONS_DIR)
        self.assertTrue(report.ok, report.render())
        case_ids = [result.case_id for result in report.results]
        for required in (
            "pos-three-end-digest-mismatch",
            "pos-infra-retry-same-request-projection-behind",
            "pos-stale-cas-new-attempt-source-head-stale",
            "pos-late-result-audit-only",
            "pos-unknown-upstream-code-replaced",
            "neg-host-forges-gms-code",
            "neg-message-driven-fallback",
        ):
            self.assertIn(required, case_ids)

    def test_negative_cases_reject_as_expected(self):
        bundle = self.VR.ReasonPolicyBundle.load(POLICY_DIR)
        for case_id, reason_code in (
            ("neg-host-forges-gms-code", "POLICY_CODE_OVERLAP"),
            ("neg-cross-registry-duplicate-code", "POLICY_CODE_OVERLAP"),
            ("neg-duplicate-code-within-registry", "POLICY_CODE_DUPLICATED"),
            ("neg-missing-mapping-field", "POLICY_MAPPING_NOT_TOTAL"),
            ("neg-policy-digest-tampered", "POLICY_DIGEST_MISMATCH"),
            ("neg-message-driven-fallback", "FIXTURE_MESSAGE_DRIVEN_FORBIDDEN"),
        ):
            result = self.VR.evaluate_fixture_case(
                bundle, REASONS_DIR, case_id)
            self.assertTrue(result.ok, "%s: %s" % (case_id, result.message))
            # The rejection itself was verified inside the case evaluation.

    def test_manifest_is_self_registered(self):
        manifest = read_json(REASONS_DIR / "manifest.json")
        self.assertEqual(
            manifest["schema_version"],
            "rsih-skill-evolution.reason-fixtures.v1")
        ids = [case["case_id"] for case in manifest["cases"]]
        self.assertEqual(len(ids), len(set(ids)))
        for case in manifest["cases"]:
            self.assertIn(case["kind"], (
                "resolution", "policy-reject", "message-driven-attempt",
                "derivation"))
            for key in ("input_path", "expected_path"):
                self.assertTrue((REASONS_DIR / case[key]).is_file(),
                                "%s missing %s" % (case["case_id"], key))


class CliTests(unittest.TestCase):
    def test_validator_cli_exit_zero(self):
        proc = subprocess.run(
            [sys.executable, str(FIX / "validate_reason_policy.py"),
             "--policy-dir", str(POLICY_DIR)],
            capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("SUMMARY:", proc.stdout)
        self.assertIn("failed=0", proc.stdout)
        self.assertIn("corpus_errors=0", proc.stdout)


if __name__ == "__main__":
    unittest.main()
