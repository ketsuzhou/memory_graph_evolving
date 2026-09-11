"""CTR-002 tool-specific success validation matrix tests (Contract 12.7.1 v1.1).

Stdlib ``unittest`` only. The validator is imported by explicit path (same
discipline as ``test_validate.py``/``test_materialization_identity.py``).
Tests exercise the golden corpus under ``conformance/tools/`` plus throwaway
temp copies; the golden fixtures are never mutated.

TDD anchor (implementation-plan CTR-002):
    Red   - test_tool_binding.py::test_skill_get_closed_guidance_view_is_validated_by_applicable_rules_only
            fails while ``validate_tool_binding.py`` does not exist (import
            error) or while skill_get success still demands watermark.
    Green - minimal tool-name -> result-schema/rule binding.
    Refactor - shared schema/digest/scope checks layered below tool-specific
            ruleset checks.
"""

import copy
import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

FIXTURE_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = FIXTURE_ROOT / "policy" / "tool-success-validation.v1.json"
SYSTEM_REGISTRY_PATH = FIXTURE_ROOT / "policy" / "system-reason-codes.v1.json"
HOST_REGISTRY_PATH = FIXTURE_ROOT / "policy" / "host-proxy-reason-codes.v1.json"
TOOLS_ROOT = FIXTURE_ROOT / "tools"
MANIFEST_PATH = TOOLS_ROOT / "manifest.json"

# Load the validator by explicit path so a dist-packages module with a similar
# name can never shadow it.
_SPEC = importlib.util.spec_from_file_location(
    "ctr002_validate_tool_binding",
    FIXTURE_ROOT / "validate_tool_binding.py",
)
vtb = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = vtb
_SPEC.loader.exec_module(vtb)

# Contract 13.7.1 R2 frozen registry digests (CTR-004); the matrix policy may
# only reference codes from these two registries.
FROZEN_SYSTEM_REGISTRY_DIGEST = (
    "sha256:16410afa27498bb425885d4629c65309389f15adeb975f97f98674170ad00a2c"
)
FROZEN_HOST_REGISTRY_DIGEST = (
    "sha256:e48f27252bff3fd34868ef4bc5b56a678cf2a35d59f4cd7f2c71f78290485f5e"
)

CASE_SCHEMA = "rsih-skill-evolution.tool-binding-case.v1"

REQUEST_TEMPLATE = {
    "schema_version": "host.tool-proxy-request.v1",
    "proxy_request_id": "pr-ctr002-0001",
    "room_id": "room-ctr002-r1",
    "agent_id": "agent-ctr002-a1",
    "delivery_id": "delivery-ctr002-0001",
    "tool_name": "memory_explore",
    "arguments": {},
    "scope_profile_ref": {
        "id": "scope-memory-room-ctr002",
        "version": 1,
        "digest": "sha256:" + "11" * 32,
    },
    "idempotency_key": "sha256:" + "22" * 32,
    "timeout_millis": 5000,
}

AUDIT_TEMPLATE = {
    "schema_version": "host.read-audit.v1",
    "proxy_request_id": "pr-ctr002-0001",
    "room_id": "room-ctr002-r1",
    "agent_id": "agent-ctr002-a1",
    "scope_profile_ref": {
        "id": "scope-memory-room-ctr002",
        "version": 1,
        "digest": "sha256:" + "11" * 32,
    },
    "active_head_activation_sequence": 12,
}


def load_json(path):
    return json.loads(Path(path).read_bytes())


def load_policy():
    return vtb.load_policy(POLICY_PATH)


def load_case(rel):
    """Load a golden case input.json under tools/."""
    return load_json(TOOLS_ROOT / rel / "input.json")


def make_case(tool_name, payload, request=None, audit=None):
    request = copy.deepcopy(REQUEST_TEMPLATE) if request is None else request
    if request is not None:
        request = copy.deepcopy(request)
        request["tool_name"] = tool_name
    audit = copy.deepcopy(AUDIT_TEMPLATE) if audit is None else copy.deepcopy(audit)
    return {
        "schema_version": CASE_SCHEMA,
        "case_id": "synthetic",
        "tool_name": tool_name,
        "request": request,
        "read_audit": audit,
        "result_payload": payload,
    }


def run_cli(policy=POLICY_PATH, fixtures=None):
    """Run the validator CLI in-process; return (exit_code, stdout_text)."""
    argv = ["--policy", str(policy)]
    if fixtures is not None:
        argv += ["--fixtures", str(fixtures)]
    buf = StringIO()
    with redirect_stdout(buf):
        code = vtb.main(argv)
    return code, buf.getvalue()


class SelfContainedJcsTests(unittest.TestCase):
    """The validator ships its own RFC 8785 JCS; pin its core behaviors."""

    def test_key_order_uses_utf16_code_units(self):
        out = vtb.jcs({"\uff04": 3, "\U0001d11e": 2})
        self.assertEqual(out, b'{"\xf0\x9d\x84\x9e":2,"\xef\xbc\x84":3}')

    def test_string_escaping_and_raw_non_ascii(self):
        self.assertEqual(
            vtb.jcs({"k": 'a\u0007\t"c\\d'}),
            b'{"k":"a\\u0007\\t\\"c\\\\d"}',
        )
        self.assertEqual(vtb.jcs({"k": "中文"}), '{"k":"中文"}'.encode("utf-8"))

    def test_integer_only_hashed_core(self):
        with self.assertRaises(vtb.CanonicalizationError) as ctx:
            vtb.jcs({"k": 1.5})
        self.assertEqual(ctx.exception.reason_code, "NON_INTEGER_NUMBER")


class PolicyClosureTests(unittest.TestCase):
    """The matrix policy is closed, digest-frozen and registry-compatible."""

    def setUp(self):
        self.policy = load_policy()

    def test_policy_digest_is_frozen_and_self_consistent(self):
        document = json.loads(POLICY_PATH.read_bytes())
        declared = document["policy_digest"]
        body = {k: v for k, v in document.items() if k != "policy_digest"}
        self.assertEqual(declared, vtb.digest_bytes(vtb.jcs(body)))
        self.assertEqual(declared, self.policy.digest)

    def test_policy_pins_frozen_reason_registry_digests(self):
        document = json.loads(POLICY_PATH.read_bytes())
        self.assertEqual(
            document["reason_registry_digests"]["system"],
            FROZEN_SYSTEM_REGISTRY_DIGEST,
        )
        self.assertEqual(
            document["reason_registry_digests"]["host-proxy"],
            FROZEN_HOST_REGISTRY_DIGEST,
        )
        system = load_json(SYSTEM_REGISTRY_PATH)
        host = load_json(HOST_REGISTRY_PATH)
        self.assertEqual(system["registry_digest"], FROZEN_SYSTEM_REGISTRY_DIGEST)
        self.assertEqual(host["registry_digest"], FROZEN_HOST_REGISTRY_DIGEST)

    def test_every_rule_code_exists_in_frozen_registries(self):
        system = {c["name"] for c in load_json(SYSTEM_REGISTRY_PATH)["codes"]}
        host = {c["name"] for c in load_json(HOST_REGISTRY_PATH)["codes"]}
        rules = list(self.policy.document["common_rules"])
        for ruleset in self.policy.document["rulesets"].values():
            rules += ruleset["rules"]
        for gate in self.policy.document["readiness_gates"].values():
            rules.append(
                {
                    "reason_code": gate["failure_code"],
                    "reason_registry": gate["failure_code_registry"],
                    "host_code": gate["host_failure_code"],
                    "host_registry": gate["host_failure_code_registry"],
                }
            )
        for rule in rules:
            self.assertIn(rule["reason_code"], system | host)
            self.assertIn(rule["host_code"], system | host)

    def test_closed_tool_set_and_binding_uniqueness(self):
        self.assertEqual(
            set(self.policy.tools),
            {"memory_explore", "memory_expand", "skill_get"},
        )
        for name, binding in self.policy.tools.items():
            self.assertEqual(binding.tool_name, name)
            self.assertTrue(binding.result_schema_version)
            self.assertIn(binding.ruleset, self.policy.rulesets)
            self.assertIn("schema_version", binding.required_fields)
            # closed field set, required subset, forbidden disjoint
            required = set(binding.required_fields)
            closed = set(binding.closed_fields)
            self.assertTrue(required <= closed)
            forbidden = set(binding.forbidden_extraneous_fields)
            self.assertFalse(forbidden & closed)
            if name == "skill_get":
                self.assertEqual(
                    set(binding.non_applicable_rules),
                    {"require_watermark", "require_budgets", "require_citations"},
                )
                self.assertEqual(
                    set(binding.applicable_rules),
                    {"closed_dto_completeness", "digest_consistency"},
                )
            else:
                self.assertEqual(
                    set(binding.applicable_rules),
                    {"require_watermark", "require_budgets", "require_citations"},
                )

    def test_tampered_policy_digest_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "tool-success-validation.v1.json"
            document = json.loads(POLICY_PATH.read_bytes())
            document["tools"]["skill_get"]["result_schema_version"] = (
                "gms.tampered.v1"
            )
            bad.write_text(json.dumps(document))
            with self.assertRaises(vtb.PolicyError):
                vtb.load_policy(bad)
            code, out = run_cli(policy=bad)
            self.assertEqual(code, 2)
            self.assertIn("digest", out.lower())


class SkillGetApplicableRulesOnlyTests(unittest.TestCase):
    """TDD anchor: skill_get validates only applicable closed GuidanceView
    rules; Explore still fails when budgets are missing."""

    def test_skill_get_closed_guidance_view_is_validated_by_applicable_rules_only(self):
        policy = load_policy()
        positive = load_case("skill-get/pos-skill-get-guidance-view")
        # The positive payload is a legal closed GuidanceView and carries none
        # of watermark/budgets/citations - and that is a success.
        for forbidden in ("watermark", "budgets", "citations", "served_fences"):
            self.assertNotIn(forbidden, positive["result_payload"])
        outcome = vtb.derive_outcome(positive, policy)
        self.assertTrue(outcome.accept)
        self.assertIsNone(outcome.reason_code)

        # Smuggling a watermark into the closed GuidanceView fails closed with
        # the DTO_FIELD_NOT_IN_CLOSED_SCHEMA semantics (SCHEMA_FIELD_UNKNOWN).
        smuggled = copy.deepcopy(positive)
        smuggled["result_payload"]["watermark"] = {
            "schema_version": "gms.projection-watermark.v1",
            "projection_stream": "runtime",
            "projection_schema_version": "gms.skill-graph.v1",
            "projection_head": "head-ctr002",
            "projected_through_activation_sequence": 12,
            "source_ledger_digest": "sha256:" + "33" * 32,
            "state": "current",
            "watermark_digest": "sha256:" + "44" * 32,
        }
        outcome = vtb.derive_outcome(smuggled, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_FIELD_UNKNOWN")

        # Simultaneously: Explore missing budgets must fail closed.
        explore = load_case("explore/basic/neg-explore-missing-budgets")
        outcome = vtb.derive_outcome(explore, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_REQUIRED_FIELD_MISSING")

    def test_guidance_view_hash_mismatch_fails_closed(self):
        policy = load_policy()
        case = load_case("skill-get/pos-skill-get-guidance-view")
        case = copy.deepcopy(case)
        case["result_payload"]["content"] = case["result_payload"]["content"] + " tampered"
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "GUIDANCE_VIEW_HASH_MISMATCH")

    def test_stale_render_profile_fails_closed(self):
        policy = load_policy()
        case = load_case("skill-get/pos-skill-get-guidance-view")
        case = copy.deepcopy(case)
        case["result_payload"]["render_profile_ref"]["version"] = 1
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_VERSION_UNSUPPORTED")

    def test_free_artifact_body_and_unknown_tool_fail_closed(self):
        policy = load_policy()
        free_body = {"artifact_markdown": "# raw skill body", "ok": True}
        outcome = vtb.derive_outcome(make_case("skill_get", free_body), policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "ARTIFACT_BODY_INVALID")
        good = load_case("skill-get/pos-skill-get-guidance-view")
        outcome = vtb.derive_outcome(make_case("memory_delete", good["result_payload"]), policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_UNSUPPORTED")

    def test_result_type_mismatch_for_skill_get_fails_closed(self):
        policy = load_policy()
        explore_payload = load_case("explore/basic/pos-explore-full-success")["result_payload"]
        outcome = vtb.derive_outcome(make_case("skill_get", explore_payload), policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

    def test_scope_violation_carried_by_request_and_audit(self):
        policy = load_policy()
        case = load_case("skill-get/pos-skill-get-guidance-view")
        case = copy.deepcopy(case)
        case["read_audit"]["room_id"] = "room-ctr002-other"
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "EXPLORE_SCOPE_VIOLATION")


class ExploreFamilyRulesTests(unittest.TestCase):
    """Explore/Expand require watermark + budgets + citations."""

    def test_missing_watermark_fails_closed(self):
        policy = load_policy()
        case = load_case("explore/basic/neg-explore-missing-watermark")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_REQUIRED_FIELD_MISSING")

    def test_missing_citations_fails_closed(self):
        policy = load_policy()
        case = load_case("explore/basic/neg-explore-missing-citations")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "CITATION_INVALID")

    def test_budget_used_exceeds_cap_fails_closed(self):
        policy = load_policy()
        case = load_case("explore/basic/neg-explore-budget-used-exceeds-cap")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "BUDGET_INVALID")

    def test_expand_missing_budgets_fails_closed(self):
        policy = load_policy()
        case = load_case("expand/neg-expand-missing-budgets")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_REQUIRED_FIELD_MISSING")

    def test_wrapper_payload_fails_closed(self):
        policy = load_policy()
        case = load_case("explore/basic/neg-explore-wrapper-payload")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_FIELD_UNKNOWN")

    def test_free_json_body_fails_closed(self):
        policy = load_policy()
        case = load_case("explore/basic/neg-explore-free-json-body")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "ARTIFACT_BODY_INVALID")

    def test_unknown_required_extension_fails_closed(self):
        policy = load_policy()
        case = load_case("explore/basic/neg-explore-unknown-required-extension")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "UNKNOWN_REQUIRED_EXTENSION")

    def test_freshness_behind_requested_sequence_fails_closed(self):
        policy = load_policy()
        case = load_case("explore/basic/neg-explore-freshness-behind")
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "PROJECTION_BEHIND_REQUIRED_SEQUENCE")

    def test_truncation_without_omission_carrier_fails_closed(self):
        policy = load_policy()
        case = load_case(
            "explore/basic/neg-explore-truncation-without-omission-carrier"
        )
        outcome = vtb.derive_outcome(case, policy)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

    def test_explore_positive_digest_is_exact(self):
        policy = load_policy()
        case = load_case("explore/basic/pos-explore-full-success")
        expected = load_json(
            TOOLS_ROOT / "explore/basic/pos-explore-full-success/expected.json"
        )
        outcome = vtb.derive_outcome(case, policy)
        self.assertTrue(outcome.accept)
        self.assertEqual(outcome.result_digest, expected["expected_result_digest"])


class CorpusAndCliTests(unittest.TestCase):
    """tools/ manifest completeness, readiness gate and CLI exit codes."""

    def test_manifest_registers_every_basic_case_exactly_once(self):
        manifest = load_json(MANIFEST_PATH)
        declared = [entry["case_id"] for entry in manifest["cases"]]
        self.assertEqual(len(declared), len(set(declared)))
        prefix = {"explore": "explore/basic/", "expand": "expand/", "skill-get": "skill-get/"}
        for entry in manifest["cases"]:
            self.assertEqual(
                entry["input_path"],
                "%s%s/input.json" % (prefix[entry["category"]], entry["case_id"]),
            )
        on_disk = set()
        for category, rel in prefix.items():
            base = TOOLS_ROOT / rel
            self.assertTrue(base.is_dir(), "missing fixture dir %s" % rel)
            for child in sorted(base.iterdir()):
                if child.is_dir():
                    on_disk.add(child.name)
                    self.assertTrue((child / "input.json").is_file())
                    self.assertTrue((child / "expected.json").is_file())
        self.assertEqual(on_disk, set(declared))

    def test_cli_validates_golden_corpus_and_reports_gate(self):
        code, out = run_cli()
        self.assertEqual(code, 0)
        self.assertIn("skill_get readiness gate: GREEN", out)

    def test_cli_fails_closed_when_expected_flipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixtures = Path(tmp) / "tools"
            shutil.copytree(TOOLS_ROOT, fixtures)
            target = fixtures / "skill-get/pos-skill-get-guidance-view/expected.json"
            expected = json.loads(target.read_bytes())
            expected["expected_accept"] = False
            expected["expected_reason_code"] = "SCHEMA_FIELD_UNKNOWN"
            target.write_text(json.dumps(expected))
            code, out = run_cli(fixtures=fixtures)
            self.assertEqual(code, 1)
            self.assertIn("skill_get readiness gate: NOT GREEN", out)

    def test_cli_fails_closed_on_missing_fixture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, out = run_cli(fixtures=Path(tmp) / "nowhere")
            self.assertEqual(code, 2)

    def test_readiness_gate_defaults_to_disabled_until_green(self):
        document = json.loads(POLICY_PATH.read_bytes())
        gate = document["readiness_gates"]["skill_get"]
        self.assertEqual(gate["state"], "disabled-until-green")
        self.assertEqual(gate["failure_code"], "TOOL_UNSUPPORTED")
        self.assertEqual(gate["host_failure_code"], "HOST_TOOL_NOT_ALLOWED")


if __name__ == "__main__":
    unittest.main()
