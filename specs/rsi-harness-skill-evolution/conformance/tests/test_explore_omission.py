"""CTR-003 ExploreResult top-level omission carrier tests (Contract 12.7.2 v1.1).

Stdlib ``unittest`` only. The validator is imported by explicit path (same
discipline as ``test_tool_binding.py``/``test_materialization_identity.py``).
Tests exercise the golden corpus under ``conformance/tools/explore/`` plus
throwaway temp copies; the golden fixtures are never mutated.

TDD anchor (implementation-plan CTR-003):
    Red   - test_explore_omission.py::test_cap_truncation_requires_exact_top_level_omitted_refs
            fails while ``validate_explore_omission.py`` does not exist
            (import error) or while reason-only truncation / mixed-kind
            carriers are still accepted.
    Green - minimal schema + accounting validator.
    Refactor - omission-list canonicalization/ranking separated from the
            consistency/accounting checks.
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
SPEC_DIR = FIXTURE_ROOT.parent
EXPLORE_ROOT = FIXTURE_ROOT / "tools" / "explore"
TOOLS_ROOT = FIXTURE_ROOT / "tools"
MANIFEST_PATH = TOOLS_ROOT / "manifest.json"
POLICY_PATH = FIXTURE_ROOT / "policy" / "tool-success-validation.v1.json"
SYSTEM_REGISTRY_PATH = FIXTURE_ROOT / "policy" / "system-reason-codes.v1.json"
SCHEMA_PATH = FIXTURE_ROOT / "schema" / "explore-result.schema.json"
CONTRACT_PATH = SPEC_DIR / "system-contract.md"
HOST_SPEC_PATH = SPEC_DIR / "pi-group-chat-host.md"
GMS_SPEC_PATH = SPEC_DIR / "graph-memory-service.md"
RSIH_SPEC_PATH = SPEC_DIR / "rsi-harness.md"

# Load the validator by explicit path so a dist-packages module with a similar
# name can never shadow it.
_SPEC = importlib.util.spec_from_file_location(
    "ctr003_validate_explore_omission",
    FIXTURE_ROOT / "validate_explore_omission.py",
)
veo = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = veo
_SPEC.loader.exec_module(veo)

FROZEN_TRUNCATION_CODES = (
    "TOTAL_CAP_REACHED",
    "EVIDENCE_SUBCAP_REACHED",
    "SKILL_SUBCAP_REACHED",
    "GUIDANCE_TOKEN_BUDGET_REACHED",
)

OMISSION_SUBTREES = ("top-level-omission", "no-omission", "negative")


def load_json(path):
    return json.loads(Path(path).read_bytes())


def load_case(rel):
    """Load a golden case input.json under tools/explore/."""
    return load_json(EXPLORE_ROOT / rel / "input.json")


def omission_case_entries():
    manifest = load_json(MANIFEST_PATH)
    return manifest["omission_cases"]


def run_cli(fixtures=EXPLORE_ROOT):
    """Run the validator CLI in-process; return (exit_code, stdout_text)."""
    buf = StringIO()
    with redirect_stdout(buf):
        code = veo.main(["--fixtures", str(fixtures)])
    return code, buf.getvalue()


def base_explore_payload():
    """A well-formed truncated ExploreResult used as the mutation base."""
    return copy.deepcopy(
        load_case("top-level-omission/pos-omission-total-cap")["result_payload"]
    )


class FrozenEnumTests(unittest.TestCase):
    """The truncation code enum reuses only frozen registry codes."""

    def test_truncation_codes_equal_the_registry_success_group(self):
        registry = load_json(SYSTEM_REGISTRY_PATH)
        group_names = {
            code["name"]
            for code in registry["codes"]
            if code["group"] == "gms-truncation-success"
        }
        self.assertEqual(group_names, set(FROZEN_TRUNCATION_CODES))
        self.assertEqual(set(veo.TRUNCATION_REASON_CODES), group_names)
        for name in group_names:
            code = next(c for c in registry["codes"] if c["name"] == name)
            self.assertEqual(code["status"], "success")


class CanonicalizationTests(unittest.TestCase):
    """Refactor anchor: canonicalization/ranking is separated from checks."""

    def test_canonical_order_sorts_by_kind_then_jcs_bytes(self):
        evidence = veo.mk_evidence_ref("ev-canonical-a")
        skill = veo.mk_skill_ref("lin-canonical-a")
        entries = [
            {"kind": "skill", "ref": skill, "reason_code": "TOTAL_CAP_REACHED"},
            {"kind": "evidence", "ref": evidence, "reason_code": "TOTAL_CAP_REACHED"},
        ]
        ordered = veo.canonical_omission_order(entries)
        self.assertEqual([e["kind"] for e in ordered], ["evidence", "skill"])

    def test_canonical_order_is_stable_within_kind(self):
        first = veo.mk_evidence_ref("ev-canonical-first")
        second = veo.mk_evidence_ref("ev-canonical-second")
        entries = [
            {"kind": "evidence", "ref": second, "reason_code": "TOTAL_CAP_REACHED"},
            {"kind": "evidence", "ref": first, "reason_code": "TOTAL_CAP_REACHED"},
        ]
        ordered = veo.canonical_omission_order(entries)
        self.assertEqual(
            [veo.canonical_ref_key(e["ref"]) for e in ordered],
            sorted(veo.canonical_ref_key(e["ref"]) for e in entries),
        )

    def test_truncation_code_sequence_is_order_preserving_dedupe(self):
        entries = [
            {"reason_code": "EVIDENCE_SUBCAP_REACHED"},
            {"reason_code": "TOTAL_CAP_REACHED"},
            {"reason_code": "EVIDENCE_SUBCAP_REACHED"},
        ]
        self.assertEqual(
            veo.derive_truncation_codes(entries),
            ["EVIDENCE_SUBCAP_REACHED", "TOTAL_CAP_REACHED"],
        )


class CapTruncationAnchorTests(unittest.TestCase):
    """TDD anchor: cap truncation requires exact typed top-level refs."""

    def test_cap_truncation_requires_exact_top_level_omitted_refs(self):
        policy_codes = veo.TRUNCATION_REASON_CODES
        self.assertIn("TOTAL_CAP_REACHED", policy_codes)

        # Reason-only truncation (codes without refs) fails closed.
        reason_only = base_explore_payload()
        reason_only["omissions"] = []
        outcome = veo.derive_omission_outcome(reason_only, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

        golden_reason_only = load_case(
            "negative/neg-omission-reason-without-refs"
        )["result_payload"]
        outcome = veo.derive_omission_outcome(golden_reason_only, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

        # The same silent truncation with the carrier entirely absent (the
        # CTR-002 interim shape) still fails closed with the same code.
        absent = base_explore_payload()
        del absent["omissions"]
        outcome = veo.derive_omission_outcome(absent, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

        # A complete exact-ref carrier for a total-cap truncation succeeds.
        complete = base_explore_payload()
        outcome = veo.derive_omission_outcome(complete, None)
        self.assertTrue(outcome.accept)
        self.assertIsNone(outcome.reason_code)

    def test_evidence_skill_kind_must_match_ref_schema(self):
        mixed = load_case("negative/neg-omission-kind-ref-mismatch")["result_payload"]
        outcome = veo.derive_omission_outcome(mixed, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "REF_MISMATCH")

        # kind must stay inside the evidence|skill enum: branch omission
        # belongs to GuidanceView, never to the top-level carrier.
        branch = load_case("negative/neg-omission-kind-branch-not-allowed")["result_payload"]
        outcome = veo.derive_omission_outcome(branch, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_ENUM_INVALID")

        # An illegal kind value on a synthetic payload is likewise rejected.
        payload = base_explore_payload()
        payload["omissions"][0]["kind"] = "checkpoint"
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_ENUM_INVALID")


class CarrierConsistencyTests(unittest.TestCase):
    """Bidirectional consistency, exactness, dedup, ordering, code enum."""

    def test_refs_without_reason_code_fails_closed(self):
        payload = base_explore_payload()
        payload["truncation_reason_codes"] = []
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

    def test_orphan_code_without_matching_entry_fails_closed(self):
        payload = base_explore_payload()
        payload["truncation_reason_codes"] = ["TOTAL_CAP_REACHED", "SKILL_SUBCAP_REACHED"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

    def test_unknown_truncation_code_fails_closed(self):
        payload = load_case("negative/neg-omission-unknown-truncation-code")["result_payload"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_ENUM_INVALID")

    def test_kind_code_incompatibility_fails_closed(self):
        payload = load_case("negative/neg-omission-kind-code-mismatch")["result_payload"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_ENUM_INVALID")

    def test_non_exact_latest_and_graph_refs_fail_closed(self):
        latest = load_case("negative/neg-omission-ref-latest-form")["result_payload"]
        outcome = veo.derive_omission_outcome(latest, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "NON_EXACT_REF")

        graph = load_case("negative/neg-omission-ref-graph-node")["result_payload"]
        outcome = veo.derive_omission_outcome(graph, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "NON_EXACT_REF")

    def test_duplicate_exact_ref_fails_closed(self):
        payload = load_case("negative/neg-omission-duplicate-ref")["result_payload"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

    def test_already_served_ref_relisted_fails_closed(self):
        payload = load_case("negative/neg-omission-served-ref-relisted")["result_payload"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "EXPLORE_FENCE_CONFLICT")

    def test_misordered_omissions_fail_closed(self):
        payload = load_case("negative/neg-omission-misordered")["result_payload"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

    def test_wrong_result_type_fails_closed(self):
        payload = load_case("negative/neg-omission-wrong-result-type")["result_payload"]
        case = load_case("negative/neg-omission-wrong-result-type")
        outcome = veo.derive_outcome(case)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "TOOL_RESULT_BINDING_INVALID")

    def test_unknown_top_level_field_fails_closed(self):
        payload = base_explore_payload()
        payload["omitted"] = payload.pop("omissions")
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_FIELD_UNKNOWN")

    def test_unknown_entry_field_fails_closed(self):
        payload = base_explore_payload()
        payload["omissions"][0]["rank_hint"] = 1
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SCHEMA_FIELD_UNKNOWN")


class AccountingAndFenceTests(unittest.TestCase):
    """Budget accounting replay and fence/watermark advancement."""

    def test_total_used_mismatch_fails_closed(self):
        payload = load_case("negative/neg-omission-budget-used-mismatch")["result_payload"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "BUDGET_INVALID")

    def test_untruthful_cap_claim_fails_closed(self):
        payload = load_case("negative/neg-omission-cap-claim-untruthful")["result_payload"]
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "BUDGET_INVALID")

    def test_fence_not_advancing_fails_closed(self):
        case = load_case("negative/neg-omission-fence-not-advancing")
        outcome = veo.derive_outcome(case)
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "EXPLORE_FENCE_CONFLICT")

    def test_positive_accounting_is_replayable(self):
        payload = load_case("top-level-omission/pos-omission-total-cap")["result_payload"]
        served = len(payload["evidence_results"]) + len(payload["skill_results"])
        self.assertEqual(payload["budgets"]["total_used"], served)
        self.assertEqual(payload["budgets"]["total_used"], payload["budgets"]["total_cap"])
        self.assertEqual(
            payload["truncation_reason_codes"],
            veo.derive_truncation_codes(payload["omissions"]),
        )
        self.assertEqual(
            [veo.canonical_omission_key(e) for e in payload["omissions"]],
            sorted(veo.canonical_omission_key(e) for e in payload["omissions"]),
        )

    def test_pagination_page_two_advances_watermark_and_keeps_fences_disjoint(self):
        page1 = load_case("top-level-omission/pos-omission-pagination-page1")
        page2 = load_case("top-level-omission/pos-omission-pagination-page2")
        prior = page2["session"]["prior_watermark"]
        new = page2["result_payload"]["watermark"]
        self.assertGreater(
            new["projected_through_activation_sequence"],
            prior["projected_through_activation_sequence"],
        )
        served_keys = {
            veo.canonical_ref_key(page1["result_payload"]["evidence_results"][0]["evidence_ref"])
        }
        for entry in page2["result_payload"]["omissions"]:
            self.assertNotIn(veo.canonical_ref_key(entry["ref"]), served_keys)
        self.assertEqual(page1["result_payload"]["omissions"], [])
        self.assertEqual(page1["result_payload"]["truncation_reason_codes"], [])


class GoldenCorpusTests(unittest.TestCase):
    """Every registered omission case derives exactly its expected outcome."""

    def test_every_positive_case_is_accepted_with_exact_digest(self):
        for entry in omission_case_entries():
            if not entry["expected_accept"]:
                continue
            case = load_json(TOOLS_ROOT / entry["input_path"])
            expected = load_json(TOOLS_ROOT / entry["expected_path"])
            outcome = veo.derive_outcome(case)
            self.assertTrue(outcome.accept, entry["case_id"])
            self.assertEqual(
                outcome.result_digest, expected["expected_result_digest"], entry["case_id"]
            )

    def test_every_negative_case_derives_its_expected_reason(self):
        for entry in omission_case_entries():
            if entry["expected_accept"]:
                continue
            case = load_json(TOOLS_ROOT / entry["input_path"])
            expected = load_json(TOOLS_ROOT / entry["expected_path"])
            outcome = veo.derive_outcome(case)
            self.assertFalse(outcome.accept, entry["case_id"])
            self.assertEqual(
                outcome.reason_code,
                expected["expected_reason_code"],
                entry["case_id"],
            )

    def test_no_omission_positive_has_determined_empty_semantics(self):
        payload = load_case("no-omission/pos-no-omission-empty-carrier")["result_payload"]
        self.assertIn("omissions", payload)
        self.assertEqual(payload["omissions"], [])
        self.assertEqual(payload["truncation_reason_codes"], [])
        outcome = veo.derive_omission_outcome(payload, None)
        self.assertTrue(outcome.accept)


class ManifestAndCliTests(unittest.TestCase):
    """omission_cases registration, completeness and CLI exit codes."""

    def test_manifest_appends_omission_cases_without_touching_cases(self):
        manifest = load_json(MANIFEST_PATH)
        self.assertIn("cases", manifest)
        self.assertEqual(manifest["schema_version"], "rsih-skill-evolution.tool-binding-manifest.v1")
        # The CTR-002 cases array is untouched: no omission case leaks in and
        # no CTR-002 case directory is re-registered under the omission_cases
        # append (their paths stay inside explore/basic/, expand/, skill-get/).
        ctr002_ids = {e["case_id"] for e in manifest["cases"]}
        omission_ids = {e["case_id"] for e in manifest["omission_cases"]}
        self.assertFalse(ctr002_ids & omission_ids)
        prefixes = {
            "top-level-omission": "explore/top-level-omission/",
            "no-omission": "explore/no-omission/",
            "negative": "explore/negative/",
        }
        on_disk = set()
        for category, rel in prefixes.items():
            base = EXPLORE_ROOT / category
            self.assertTrue(base.is_dir(), "missing fixture dir %s" % rel)
            for child in sorted(base.iterdir()):
                if child.is_dir():
                    on_disk.add(child.name)
                    self.assertTrue((child / "input.json").is_file())
                    self.assertTrue((child / "expected.json").is_file())
        declared = {e["case_id"] for e in manifest["omission_cases"]}
        self.assertEqual(on_disk, declared)
        for entry in manifest["omission_cases"]:
            self.assertEqual(
                entry["input_path"],
                "%s%s/input.json" % (prefixes[entry["category"]], entry["case_id"]),
            )
            self.assertEqual(
                entry["expected_path"],
                "%s%s/expected.json" % (prefixes[entry["category"]], entry["case_id"]),
            )
            self.assertIn(entry["tool_name"], {"memory_explore", "memory_expand"})

    def test_cli_validates_golden_corpus(self):
        code, out = run_cli()
        self.assertEqual(code, 0)
        self.assertIn("cases: 24, failed: 0", out)

    def test_cli_fails_closed_when_expected_flipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            tools = Path(tmp) / "tools"
            shutil.copytree(TOOLS_ROOT, tools)
            target = (
                tools
                / "explore/top-level-omission/pos-omission-total-cap/expected.json"
            )
            expected = json.loads(target.read_bytes())
            expected["expected_accept"] = False
            expected["expected_reason_code"] = "TOOL_RESULT_BINDING_INVALID"
            expected["expected_result_digest"] = None
            target.write_text(json.dumps(expected))
            # The manifest mirror must be flipped as well; leaving it stale
            # is itself a corpus integrity error (exit 2), asserted below.
            manifest_path = tools / "manifest.json"
            manifest = json.loads(manifest_path.read_bytes())
            for entry in manifest["omission_cases"]:
                if entry["case_id"] == "pos-omission-total-cap":
                    entry["expected_accept"] = False
                    entry["expected_reason_code"] = "TOOL_RESULT_BINDING_INVALID"
            manifest_path.write_text(json.dumps(manifest))
            code, _ = run_cli(fixtures=tools / "explore")
            self.assertEqual(code, 1)
            # A stale manifest mirror (expected.json flipped, mirror not)
            # fails closed as a corpus integrity error instead.
            manifest = json.loads(manifest_path.read_bytes())
            for entry in manifest["omission_cases"]:
                if entry["case_id"] == "pos-omission-total-cap":
                    entry["expected_accept"] = True
                    entry["expected_reason_code"] = None
            manifest_path.write_text(json.dumps(manifest))
            code, _ = run_cli(fixtures=tools / "explore")
            self.assertEqual(code, 2)

    def test_cli_fails_closed_on_undeclared_case_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            tools = Path(tmp) / "tools"
            shutil.copytree(TOOLS_ROOT, tools)
            extra = tools / "explore/negative/neg-omission-not-registered"
            extra.mkdir()
            (extra / "input.json").write_text("{}")
            (extra / "expected.json").write_text("{}")
            code, _ = run_cli(fixtures=tools / "explore")
            self.assertEqual(code, 2)

    def test_cli_fails_closed_on_missing_fixture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, _ = run_cli(fixtures=Path(tmp) / "nowhere")
            self.assertEqual(code, 2)


class SchemaFileTests(unittest.TestCase):
    """schema/explore-result.schema.json freezes the closed carrier shape."""

    def setUp(self):
        self.schema = load_json(SCHEMA_PATH)

    def test_schema_freezes_explore_result_with_required_omissions(self):
        result = self.schema["$defs"]["explore_result"]
        self.assertIn("omissions", result["required"])
        for field in (
            "schema_version",
            "explore_session_id",
            "query_digest",
            "ranker_policy_ref",
            "watermark",
            "evidence_results",
            "skill_results",
            "served_fences",
            "budgets",
            "truncation_reason_codes",
            "omissions",
        ):
            self.assertIn(field, result["properties"])
        self.assertEqual(result["properties"]["schema_version"]["const"], "gms.explore-result.v1")

    def test_schema_freezes_the_closed_omission_entry(self):
        entry = self.schema["$defs"]["omission_entry"]
        self.assertFalse(entry.get("additionalProperties", True))
        self.assertEqual(entry["properties"]["kind"]["enum"], ["evidence", "skill"])
        self.assertEqual(
            sorted(entry["properties"]["reason_code"]["enum"]),
            sorted(FROZEN_TRUNCATION_CODES),
        )
        refs = entry["properties"]["ref"]
        self.assertIn("evidence", refs["properties"])
        self.assertIn("skill", refs["properties"])

    def test_schema_freezes_exact_ref_defs_and_case_envelope(self):
        defs = self.schema["$defs"]
        self.assertEqual(
            defs["evidence_ref"]["properties"]["schema_version"]["const"],
            "gms.evidence-ref.v1",
        )
        self.assertEqual(
            defs["skill_artifact_ref"]["properties"]["schema_version"]["const"],
            "gms.skill-artifact-ref.v1",
        )
        self.assertEqual(
            defs["evidence_ref"]["properties"]["commit_state"]["enum"],
            ["committed", "sealed"],
        )
        self.assertFalse(defs["evidence_ref"].get("additionalProperties", True))
        self.assertFalse(defs["skill_artifact_ref"].get("additionalProperties", True))
        # The corpus envelope and the tools/manifest.json omission_cases
        # registration are frozen alongside the DTO.
        self.assertIn("omission_case", defs)
        self.assertIn("omission_expected", defs)
        self.assertIn("omission_cases_registration", self.schema["properties"])


class PolicyAndContractAlignmentTests(unittest.TestCase):
    """Policy digest re-frozen; contract and module specs carry the revision."""

    def test_policy_digest_is_self_consistent_and_omissions_are_closed(self):
        document = json.loads(POLICY_PATH.read_bytes())
        body = {k: v for k, v in document.items() if k != "policy_digest"}
        self.assertEqual(
            document["policy_digest"], veo.digest_bytes(veo.jcs(body))
        )
        for tool in ("memory_explore", "memory_expand"):
            binding = document["tools"][tool]
            self.assertIn("omissions", binding["closed_fields"])
            self.assertNotIn("omissions", binding["required_fields"])
            self.assertTrue(set(binding["required_fields"]) <= set(binding["closed_fields"]))
        obligation = document["tools"]["memory_explore"]["omitted_refs_obligation"]
        self.assertEqual(obligation.get("carrier_field"), "omissions")
        self.assertIn("12.7.2", obligation.get("frozen_by", ""))

    def test_contract_1271_pins_the_refrozen_policy_digest(self):
        text = CONTRACT_PATH.read_text(encoding="utf-8")
        document = json.loads(POLICY_PATH.read_bytes())
        self.assertIn(document["policy_digest"], text)

    def test_contract_carries_the_1272_revision_and_module_spec_pointers(self):
        contract = CONTRACT_PATH.read_text(encoding="utf-8")
        self.assertIn("§12.7.2 ExploreResult top-level omission carrier", contract)
        self.assertIn("（修订 v1.1，CTR-003）", contract)
        # The interim hold-out in 12.7.1 M2 is lifted and points at 12.7.2.
        m2 = contract.split("top-level omitted refs 存在性义务", 1)[1][:1200]
        self.assertIn("§12.7.2", m2)
        for path in (HOST_SPEC_PATH, GMS_SPEC_PATH, RSIH_SPEC_PATH):
            self.assertIn(
                "【修订 v1.1，CTR-003】",
                path.read_text(encoding="utf-8"),
                "missing CTR-003 pointer in %s" % path.name,
            )


if __name__ == "__main__":
    unittest.main()
