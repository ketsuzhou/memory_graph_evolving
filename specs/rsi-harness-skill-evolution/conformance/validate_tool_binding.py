#!/usr/bin/env python3
"""CTR-002 tool-specific success validation matrix validator (Contract 12.7.1 v1.1).

Validates the closed matrix frozen by System Contract 12.7.1 (revision v1.1,
CTR-002) against the golden corpus under ``conformance/tools/``. stdlib only.
Read-only: it never writes or self-updates fixtures.

Matrix summary (authoritative file: policy/tool-success-validation.v1.json,
policy digest = SHA-256(JCS(document minus policy_digest)))
---------------------------------------------------------------------
Binding declaration (M1): every tool name binds exactly one result schema
and one ruleset; the closed tool set v1 is memory_explore, memory_expand,
skill_get. Unknown tool names fail closed.

- memory_explore / memory_expand (M2/M3, ruleset explore-family):
  success requires watermark (Contract 7.14 closed subfields), budgets
  (used<=cap, subcaps, guidance token sum) and typed citations
  (evidence citation.evidence_ref+claim; skill artifact_identity_citation +
  evidence_citations). Top-level omitted refs are a declared presence
  obligation whose carrier shape is frozen later by CTR-003; until then a
  non-empty truncation_reason_codes fails closed (GMS 10.8 interim rule).
- skill_get (M4, ruleset guidance-view-closed): success is validated ONLY by
  the applicable closed GuidanceView rules - closed-DTO completeness,
  view_hash digest consistency and policy-frozen profile version floors.
  watermark/budgets/citations are NON-applicable: freshness (sequence /
  watermark comparison) and authorization (scope/room) are carried by
  ToolProxyRequest fields plus the authoritative read audit record, never by
  adding fields to the closed DTO. Smuggled fields fail closed with
  SCHEMA_FIELD_UNKNOWN (DTO_FIELD_NOT_IN_CLOSED_SCHEMA semantics).

Common layer (M5, all tools, this order):
 1. tool_name not bound                      -> TOOL_UNSUPPORTED
 2. unknown required extension (6.4)         -> UNKNOWN_REQUIRED_EXTENSION
 3. result body not a closed DTO object      -> ARTIFACT_BODY_INVALID
    (free JSON body / free artifact body)
 4. result schema_version != bound schema    -> TOOL_RESULT_BINDING_INVALID
    (tool/result type mismatch)
 5. unknown top-level field in the closed DTO-> SCHEMA_FIELD_UNKNOWN
    (incl. wrapper keys; GuidanceView private watermark)
 6. required top-level field missing         -> SCHEMA_REQUIRED_FIELD_MISSING
 7. read audit vs request room/agent/scope
    mismatch (authorization is request+audit) -> EXPLORE_SCOPE_VIOLATION
Then the bound ruleset rules in policy order (first closed failure wins),
finally accepted with result_digest = SHA-256(JCS(result_payload)).

Reason codes: the matrix reuses only codes already frozen by the two
Contract 13.7.1 (CTR-004) registries - the system registry
(policy/system-reason-codes.v1.json) for the canonical GMS/system-side code
and the host-proxy registry (policy/host-proxy-reason-codes.v1.json) for the
Host-side counterpart Host emits per 13.7.1 R3 precedence rule 1. This
validator refuses to load a policy whose codes are absent from those
registries or whose pinned registry digests drift.

Readiness gate (M6): skill_get stays disabled-until-green; the gate is green
only when the policy digest verifies and every skill-get fixture case
derives accept/reason equal to its expected.json.

CLI
---
    python3 validate_tool_binding.py --policy <tool-success-validation.v1.json>
                                     [--fixtures <tools-dir>]

``--fixtures`` defaults to <policy dir>/../tools. Exit codes: 0 all cases
pass and the gate is green; 1 at least one case fails; 2 policy/corpus
integrity error. Output is deterministic (manifest order).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

POLICY_SCHEMA_VERSION = "rsih-skill-evolution.tool-success-validation-policy.v1"
CASE_SCHEMA_VERSION = "rsih-skill-evolution.tool-binding-case.v1"
EXPECTED_SCHEMA_VERSION = "rsih-skill-evolution.tool-binding-expected.v1"
MANIFEST_SCHEMA_VERSION = "rsih-skill-evolution.tool-binding-manifest.v1"
CONTRACT_SCHEMA_VERSION = "rsih-skill-evolution.system-contract.v1"
CONTRACT_REVISION = "system-contract 12.7.1 v1.1 (CTR-002)"

# Contract 13.7.1 R2 frozen reason-registry digests (CTR-004).
FROZEN_REGISTRY_DIGESTS = {
    "system": "sha256:16410afa27498bb425885d4629c65309389f15adeb975f97f98674170ad00a2c",
    "host-proxy": "sha256:e48f27252bff3fd34868ef4bc5b56a678cf2a35d59f4cd7f2c71f78290485f5e",
}
SYSTEM_REGISTRY_FILENAME = "system-reason-codes.v1.json"
HOST_REGISTRY_FILENAME = "host-proxy-reason-codes.v1.json"

# Category -> path prefix under the tools/ fixture root. Only these prefixes
# are owned by this corpus; CTR-003 will append tools/explore/* subtrees.
TOOL_CATEGORIES = {
    "explore": "explore/basic/",
    "expand": "expand/",
    "skill-get": "skill-get/",
}
GATED_TOOL = "skill_get"

# v1 conformance policy (mirrors FND-001): no required extension registered,
# so required:true + unknown key always fails closed (Contract 6.4).
KNOWN_EXTENSIONS_V1 = frozenset()

# Extensions are permitted on any closed DTO (Contract 7.1/6.4) and are
# therefore excluded from the closed field-set check.
IMPLICIT_PERMITTED_FIELDS = frozenset({"extensions"})

BOM = b"\xef\xbb\xbf"


class CanonicalizationError(Exception):
    """Raised when a value cannot enter the integer-only hashed core."""

    def __init__(self, reason_code: str):
        super().__init__(reason_code)
        self.reason_code = reason_code


class PolicyError(Exception):
    """The matrix policy itself is not closed / digest-verified."""


class CorpusError(Exception):
    """The tools/ fixture corpus (manifest or case files) is inconsistent."""


@dataclass(frozen=True)
class Outcome:
    """Derived evaluation result, computed independently of expected.json."""

    accept: bool
    reason_code: "str | None"
    result_digest: "str | None" = None


@dataclass(frozen=True)
class CaseResult:
    case_id: str
    ok: bool
    message: str
    accept: "str | None" = None
    reason: str = "-"
    result_digest: str = "-"


@dataclass(frozen=True)
class Binding:
    """One tool-name -> result-schema/ruleset binding (matrix M1)."""

    tool_name: str
    result_schema_version: str
    ruleset: str
    closed_fields: frozenset
    required_fields: tuple
    forbidden_extraneous_fields: frozenset
    applicable_rules: tuple
    non_applicable_rules: tuple
    profile_constraints: dict
    raw: dict


@dataclass(frozen=True)
class Ruleset:
    ruleset_id: str
    rules: tuple  # ordered rule dicts from the policy
    raw: dict


@dataclass(frozen=True)
class Policy:
    document: dict
    digest: str
    tools: dict
    rulesets: dict
    common_rules: tuple
    common_rule_ids: dict  # rule_id -> rule dict
    known_result_schemas: frozenset

    def common_code(self, rule_id: str) -> str:
        return self.common_rule_ids[rule_id]["reason_code"]

    def ruleset_code(self, ruleset_id: str, rule_id: str) -> str:
        for rule in self.rulesets[ruleset_id].rules:
            if rule["rule_id"] == rule_id:
                return rule["reason_code"]
        raise PolicyError("unknown rule %r in ruleset %r" % (rule_id, ruleset_id))


# ---------------------------------------------------------------------------
# Self-contained RFC 8785 JCS (Contract 6.1): integer-only, UTF-16 key order.
# ---------------------------------------------------------------------------


def _escape_string(value: str) -> str:
    out = ['"']
    for ch in value:
        code = ord(ch)
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif code < 0x20:
            if ch == "\b":
                out.append("\\b")
            elif ch == "\t":
                out.append("\\t")
            elif ch == "\n":
                out.append("\\n")
            elif ch == "\f":
                out.append("\\f")
            elif ch == "\r":
                out.append("\\r")
            else:
                out.append("\\u%04x" % code)
        else:
            out.append(ch)  # raw, incl. U+007F and all non-ASCII
    out.append('"')
    return "".join(out)


def jcs(value) -> bytes:
    """RFC 8785 JCS canonicalization, integer-only hashed core (6.1.4)."""
    parts: "list[str]" = []

    def emit(node) -> None:
        if node is None:
            parts.append("null")
        elif node is True:
            parts.append("true")
        elif node is False:
            parts.append("false")
        elif isinstance(node, int):  # bool handled above
            parts.append(str(node))
        elif isinstance(node, float):
            raise CanonicalizationError("NON_INTEGER_NUMBER")
        elif isinstance(node, str):
            parts.append(_escape_string(node))
        elif isinstance(node, list):
            parts.append("[")
            for index, item in enumerate(node):
                if index:
                    parts.append(",")
                emit(item)
            parts.append("]")
        elif isinstance(node, dict):
            parts.append("{")
            keys = sorted(node.keys(), key=lambda k: k.encode("utf-16-be"))
            for index, key in enumerate(keys):
                if index:
                    parts.append(",")
                parts.append(_escape_string(key))
                parts.append(":")
                emit(node[key])
            parts.append("}")
        else:
            raise TypeError("unsupported JSON value type: %r" % type(node))

    emit(value)
    return "".join(parts).encode("utf-8")


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Policy layer: load the closed matrix, verify its digest and registry use.
# ---------------------------------------------------------------------------


def _read_json_file(path: Path, what: str) -> dict:
    data = path.read_bytes()
    if data.startswith(BOM):
        raise PolicyError("%s %s starts with a UTF-8 BOM" % (what, path))
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("%s %s is not valid UTF-8/JSON: %s" % (what, path, exc))


def _registry_code_names(policy_dir: Path) -> set:
    """Load the two frozen 13.7.1 registries and collect their code names."""
    names: set = set()
    for registry, filename in (
        ("system", SYSTEM_REGISTRY_FILENAME),
        ("host-proxy", HOST_REGISTRY_FILENAME),
    ):
        path = policy_dir / filename
        if not path.is_file():
            raise PolicyError(
                "reason registry %s missing next to the policy (%s)"
                % (filename, policy_dir)
            )
        document = _read_json_file(path, "reason registry")
        if document.get("registry_digest") != FROZEN_REGISTRY_DIGESTS[registry]:
            raise PolicyError(
                "reason registry %s digest does not match the Contract 13.7.1 "
                "R2 frozen value" % filename
            )
        names.update(code["name"] for code in document["codes"])
    return names


def load_policy(path) -> Policy:
    """Load and fully closure-check the matrix policy (digest verified)."""
    path = Path(path)
    document = _read_json_file(path, "matrix policy")
    if not isinstance(document, dict):
        raise PolicyError("policy %s must be a JSON object" % path)
    if document.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise PolicyError("policy schema_version must be %r" % POLICY_SCHEMA_VERSION)

    declared = document.get("policy_digest")
    body = {k: v for k, v in document.items() if k != "policy_digest"}
    computed = digest_bytes(jcs(body))
    if not isinstance(declared, str) or declared != computed:
        raise PolicyError(
            "policy digest mismatch: declared %r, computed %s" % (declared, computed)
        )

    for registry, digest in FROZEN_REGISTRY_DIGESTS.items():
        if document.get("reason_registry_digests", {}).get(registry) != digest:
            raise PolicyError(
                "policy must pin the Contract 13.7.1 R2 frozen digest of the "
                "%s reason registry" % registry
            )

    tools_raw = document.get("tools")
    rulesets_raw = document.get("rulesets")
    if not isinstance(tools_raw, dict) or not tools_raw:
        raise PolicyError("policy.tools must be a non-empty object")
    if not isinstance(rulesets_raw, dict) or not rulesets_raw:
        raise PolicyError("policy.rulesets must be a non-empty object")

    rulesets = {}
    for ruleset_id, raw in rulesets_raw.items():
        if not isinstance(raw.get("rules"), list) or not raw["rules"]:
            raise PolicyError("ruleset %r must carry a non-empty rules array" % ruleset_id)
        rulesets[ruleset_id] = Ruleset(
            ruleset_id=ruleset_id, rules=tuple(raw["rules"]), raw=raw
        )

    tools = {}
    for name, raw in tools_raw.items():
        if raw.get("tool_name") != name:
            raise PolicyError("tool entry %r must echo tool_name" % name)
        ruleset = raw.get("ruleset")
        if ruleset not in rulesets:
            raise PolicyError("tool %r references unknown ruleset %r" % (name, ruleset))
        closed = frozenset(raw.get("closed_fields") or [])
        required = tuple(raw.get("required_fields") or [])
        forbidden = frozenset(raw.get("forbidden_extraneous_fields") or [])
        if not raw.get("result_schema_version"):
            raise PolicyError("tool %r must bind a result schema_version" % name)
        if "schema_version" not in required:
            raise PolicyError("tool %r must require schema_version" % name)
        if not set(required) <= closed:
            raise PolicyError(
                "tool %r required_fields must be a subset of closed_fields" % name
            )
        if forbidden & closed:
            raise PolicyError(
                "tool %r forbidden_extraneous_fields overlap closed_fields" % name
            )
        tools[name] = Binding(
            tool_name=name,
            result_schema_version=raw["result_schema_version"],
            ruleset=ruleset,
            closed_fields=closed,
            required_fields=required,
            forbidden_extraneous_fields=forbidden,
            applicable_rules=tuple(raw.get("applicable_rules") or []),
            non_applicable_rules=tuple(raw.get("non_applicable_rules") or []),
            profile_constraints=dict(raw.get("profile_constraints") or {}),
            raw=raw,
        )
    if set(tools) != {"memory_explore", "memory_expand", "skill_get"}:
        raise PolicyError("closed tool set v1 must be explore/expand/skill_get")

    common_rules = document.get("common_rules")
    if not isinstance(common_rules, list) or not common_rules:
        raise PolicyError("policy.common_rules must be a non-empty ordered array")
    rule_ids = [rule["rule_id"] for rule in common_rules]
    if len(rule_ids) != len(set(rule_ids)):
        raise PolicyError("common rule ids must be unique")

    gate = document.get("readiness_gates", {}).get(GATED_TOOL)
    if not isinstance(gate, dict) or gate.get("state") != "disabled-until-green":
        raise PolicyError(
            "readiness gate for %s must stay disabled-until-green until the "
            "gate is green" % GATED_TOOL
        )

    # The matrix may only reuse codes frozen by the 13.7.1 registries.
    registry_names = _registry_code_names(path.parent)
    for rule in common_rules:
        for field in ("reason_code", "host_code"):
            if rule.get(field) not in registry_names:
                raise PolicyError(
                    "common rule %r %s=%r is not in the frozen reason registries"
                    % (rule.get("rule_id"), field, rule.get(field))
                )
    for ruleset in rulesets.values():
        for rule in ruleset.rules:
            for field in ("reason_code", "host_code"):
                if rule.get(field) not in registry_names:
                    raise PolicyError(
                        "ruleset %r rule %r %s=%r is not in the frozen reason "
                        "registries" % (ruleset.ruleset_id, rule.get("rule_id"), field, rule.get(field))
                    )

    return Policy(
        document=document,
        digest=computed,
        tools=tools,
        rulesets=rulesets,
        common_rules=tuple(common_rules),
        common_rule_ids={rule["rule_id"]: rule for rule in common_rules},
        known_result_schemas=frozenset(t.result_schema_version for t in tools.values()),
    )


# ---------------------------------------------------------------------------
# Shared layer: common rules that apply to every tool (matrix M5).
# ---------------------------------------------------------------------------


def _unknown_required_extension(container) -> bool:
    extensions = container.get("extensions")
    if not isinstance(extensions, dict):
        return False
    for key, entry in extensions.items():
        if isinstance(entry, dict) and entry.get("required") is True:
            if key not in KNOWN_EXTENSIONS_V1:
                return True
    return False


def _check_common(case: dict, policy: Policy, binding: Binding) -> "str | None":
    """Return the firing common rule_id, or None when the shared layer passes.

    Order follows policy.common_rules: unknown tool (resolved by the caller),
    extensions, closed body, schema binding, field set, required fields,
    scope.
    """
    payload = case.get("result_payload")
    request = case.get("request") if isinstance(case.get("request"), dict) else {}
    audit = case.get("read_audit") if isinstance(case.get("read_audit"), dict) else {}

    if _unknown_required_extension(request):
        return "unknown_required_extension"
    if isinstance(payload, dict) and _unknown_required_extension(payload):
        return "unknown_required_extension"
    if not isinstance(payload, dict):
        return "result_body_not_closed"
    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, str) or schema_version not in policy.known_result_schemas:
        return "result_body_not_closed"
    if schema_version != binding.result_schema_version:
        return "result_schema_mismatch"
    extra = set(payload) - binding.closed_fields - IMPLICIT_PERMITTED_FIELDS
    if extra:
        return "closed_field_unknown"
    for field in binding.required_fields:
        if field not in payload:
            return "required_field_missing"
    # Authorization is carried by request + authoritative read audit record.
    for field in ("room_id", "agent_id", "scope_profile_ref"):
        if field in audit and field in request and audit[field] != request[field]:
            return "scope_violation"
    return None


# ---------------------------------------------------------------------------
# Tool-specific layer: ruleset evaluators (matrix M2/M3/M4).
# ---------------------------------------------------------------------------


def _is_non_negative_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _check_profile_constraints(payload: dict, binding: Binding) -> bool:
    """Generic profile version floors: min_<field>_version (M5/M4)."""
    for key, floor in binding.profile_constraints.items():
        if not (key.startswith("min_") and key.endswith("_version")):
            raise PolicyError("unsupported profile constraint key %r" % key)
        field = key[4:-8]  # strip "min_" prefix and "_version" suffix
        ref = payload.get(field)
        if isinstance(ref, dict) and isinstance(ref.get("version"), int):
            if ref["version"] < floor:
                return False
    return True


def _check_explore_family(payload, request, audit, binding, ruleset) -> "str | None":
    """Explore/Expand: watermark + budgets + typed citations (M2/M3)."""
    raw = ruleset.raw
    watermark = payload.get("watermark") or {}
    for field in raw["required_watermark_fields"]:
        if field not in watermark:
            return "nested_required_field_missing"
    budgets = payload.get("budgets") or {}
    for field in raw["required_budget_fields"]:
        if field not in budgets:
            return "nested_required_field_missing"
    fences = payload.get("served_fences") or {}
    for field in raw["required_served_fence_fields"]:
        if field not in fences:
            return "nested_required_field_missing"

    # Budgets consistency (Contract 12.4).
    budget_values = [budgets[f] for f in raw["required_budget_fields"]]
    if not all(_is_non_negative_int(v) for v in budget_values):
        return "budget_inconsistent"
    evidence_results = payload.get("evidence_results")
    skill_results = payload.get("skill_results")
    if not isinstance(evidence_results, list) or not isinstance(skill_results, list):
        return "nested_required_field_missing"
    if (
        budgets["total_used"] > budgets["total_cap"]
        or len(evidence_results) > budgets["evidence_subcap"]
        or len(skill_results) > budgets["skill_subcap"]
    ):
        return "budget_inconsistent"
    token_sum = 0
    for entry in skill_results:
        view = entry.get("guidance_view")
        if isinstance(view, dict) and _is_non_negative_int(view.get("content_token_count")):
            token_sum += view["content_token_count"]
    if token_sum > budgets["guidance_token_budget"]:
        return "budget_inconsistent"

    # Typed results (Contract 7.16).
    for entry in evidence_results:
        if entry.get("result_type") != "evidence":
            return "result_type_invalid"
    for entry in skill_results:
        if entry.get("result_type") != "skill":
            return "result_type_invalid"

    # Typed citations (Contract 12.6).
    for entry in evidence_results:
        citation = entry.get("citation")
        if (
            not isinstance(citation, dict)
            or not isinstance(citation.get("evidence_ref"), dict)
            or not isinstance(citation.get("claim"), str)
        ):
            return "citation_invalid"
    for entry in skill_results:
        identity = entry.get("artifact_identity_citation")
        if not isinstance(identity, dict) or not isinstance(identity.get("skill_ref"), dict):
            return "citation_invalid"
        evidence_citations = entry.get("evidence_citations")
        if not isinstance(evidence_citations, list):
            return "citation_invalid"
        for citation in evidence_citations:
            if (
                not isinstance(citation, dict)
                or not isinstance(citation.get("evidence_ref"), dict)
                or not isinstance(citation.get("claim"), str)
            ):
                return "citation_invalid"

    # Interim top-level-omission carrier rule (GMS 10.8) until CTR-003.
    truncation_codes = payload.get("truncation_reason_codes")
    if isinstance(truncation_codes, list) and truncation_codes:
        return "truncation_without_omission_carrier"

    if not _check_profile_constraints(payload, binding):
        return "profile_version_stale"

    # Freshness is carried by request fields vs the result watermark.
    requested = request.get("requested_min_activation_sequence")
    if _is_non_negative_int(requested):
        projected = watermark.get("projected_through_activation_sequence")
        if not _is_non_negative_int(projected) or projected < requested:
            return "freshness_behind"
    return None


def _check_guidance_view_closed(payload, request, audit, binding, ruleset) -> "str | None":
    """skill_get: only applicable closed GuidanceView rules (M4)."""
    if not _check_profile_constraints(payload, binding):
        return "profile_version_stale"

    # Digest consistency inside the closed schema.
    preimage = {k: v for k, v in payload.items() if k != "view_hash"}
    if payload.get("view_hash") != digest_bytes(jcs(preimage)):
        return "guidance_view_hash_mismatch"

    # Freshness is carried by request vs the authoritative read audit record
    # (skill_get current_active checks the authoritative head directly).
    requested = request.get("requested_min_activation_sequence")
    if _is_non_negative_int(requested):
        active_head = audit.get("active_head_activation_sequence")
        if not _is_non_negative_int(active_head) or active_head < requested:
            return "freshness_behind"
    return None


RULESET_CHECKS = {
    "explore-family": _check_explore_family,
    "guidance-view-closed": _check_guidance_view_closed,
}


# ---------------------------------------------------------------------------
# Derivation: policy-driven accept/reason for one case, independent of
# expected.json (matrix M1-M6, first closed failure wins).
# ---------------------------------------------------------------------------


def derive_outcome(case: dict, policy: Policy) -> Outcome:
    if not isinstance(case, dict):
        raise CorpusError("case must be a JSON object")
    tool_name = case.get("tool_name")
    binding = policy.tools.get(tool_name) if isinstance(tool_name, str) else None
    if binding is None:
        return Outcome(False, policy.common_code("unknown_tool_name"))

    rule_id = _check_common(case, policy, binding)
    if rule_id is None:
        payload = case["result_payload"]
        request = case.get("request") if isinstance(case.get("request"), dict) else {}
        audit = case.get("read_audit") if isinstance(case.get("read_audit"), dict) else {}
        ruleset = policy.rulesets[binding.ruleset]
        rule_id = RULESET_CHECKS[binding.ruleset](
            payload, request, audit, binding, ruleset
        )
    if rule_id is not None:
        reason = (
            policy.common_code(rule_id)
            if rule_id in policy.common_rule_ids
            else policy.ruleset_code(binding.ruleset, rule_id)
        )
        return Outcome(False, reason)
    return Outcome(True, None, digest_bytes(jcs(case["result_payload"])))


# ---------------------------------------------------------------------------
# Corpus runner: tools/manifest.json + per-case exact expected comparison.
# ---------------------------------------------------------------------------


def _load_case_document(entry: dict, fixtures_root: Path) -> dict:
    path = fixtures_root / entry["input_path"]
    data = path.read_bytes()
    if data.startswith(BOM):
        raise CorpusError("case input %s starts with a UTF-8 BOM" % path)
    try:
        document = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CorpusError("case input %s is not valid UTF-8/JSON: %s" % (path, exc))
    if document.get("schema_version") != CASE_SCHEMA_VERSION:
        raise CorpusError("case input %s schema_version must be %r" % (path, CASE_SCHEMA_VERSION))
    if document.get("case_id") != entry["case_id"]:
        raise CorpusError("case input %s case_id mismatch" % path)
    return document


def _load_expected(entry: dict, fixtures_root: Path) -> dict:
    path = fixtures_root / entry["expected_path"]
    document = json.loads(path.read_bytes())
    if document.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        raise CorpusError("expected %s schema_version must be %r" % (path, EXPECTED_SCHEMA_VERSION))
    if document.get("case_id") != entry["case_id"]:
        raise CorpusError("expected %s case_id mismatch" % path)
    if not isinstance(document.get("expected_accept"), bool):
        raise CorpusError("expected %s expected_accept must be a boolean" % path)
    if document["expected_accept"]:
        if document.get("expected_reason_code") is not None:
            raise CorpusError("accepted case %s must not carry a reason" % entry["case_id"])
        if not isinstance(document.get("expected_result_digest"), str):
            raise CorpusError("accepted case %s must carry expected_result_digest" % entry["case_id"])
    else:
        if not isinstance(document.get("expected_reason_code"), str):
            raise CorpusError("rejected case %s must carry expected_reason_code" % entry["case_id"])
    return document


def _load_manifest(fixtures_root: Path) -> list:
    manifest_path = fixtures_root / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise CorpusError("tools manifest schema_version must be %r" % MANIFEST_SCHEMA_VERSION)
    if manifest.get("contract_schema_version") != CONTRACT_SCHEMA_VERSION:
        raise CorpusError("tools manifest contract_schema_version mismatch")
    entries = manifest.get("cases")
    if not isinstance(entries, list) or not entries:
        raise CorpusError("tools manifest must register a non-empty cases array")
    seen = set()
    for entry in entries:
        case_id = entry.get("case_id")
        category = entry.get("category")
        if case_id in seen:
            raise CorpusError("duplicate case id %r" % case_id)
        seen.add(case_id)
        if category not in TOOL_CATEGORIES:
            raise CorpusError("case %r has unknown category %r" % (case_id, category))
        prefix = TOOL_CATEGORIES[category]
        expected_prefix = "%s%s/" % (prefix, case_id)
        if not entry.get("input_path", "").startswith(expected_prefix):
            raise CorpusError("case %r input_path must live under %s" % (case_id, expected_prefix))
        if entry.get("input_path") != "%sinput.json" % expected_prefix:
            raise CorpusError("case %r input_path must be exactly %sinput.json" % (case_id, expected_prefix))
        if entry.get("expected_path") != "%sexpected.json" % expected_prefix:
            raise CorpusError("case %r expected_path must be exactly %sexpected.json" % (case_id, expected_prefix))
    # Corpus completeness for the prefixes owned by this corpus (CTR-003 will
    # append further tools/explore/* subtrees with its own manifest update).
    for category, prefix in TOOL_CATEGORIES.items():
        base = fixtures_root / prefix
        if not base.is_dir():
            raise CorpusError("fixture category directory missing: %s" % prefix)
        for child in sorted(base.iterdir()):
            if child.is_dir() and child.name not in seen:
                raise CorpusError("undeclared case directory not in manifest: %s" % child.name)
    return entries


def evaluate_corpus(policy: Policy, fixtures_root: Path) -> "tuple[list, bool]":
    """Derive every case and compare against expected.json exactly."""
    entries = _load_manifest(fixtures_root)
    results = []
    gate_green = True
    for entry in entries:
        case = _load_case_document(entry, fixtures_root)
        expected = _load_expected(entry, fixtures_root)
        outcome = derive_outcome(case, policy)
        problems = []
        if outcome.accept != expected["expected_accept"]:
            problems.append(
                "accept %s != expected %s"
                % (outcome.accept, expected["expected_accept"])
            )
        if outcome.accept:
            if outcome.result_digest != expected["expected_result_digest"]:
                problems.append("result digest mismatch")
        else:
            if outcome.reason_code != expected["expected_reason_code"]:
                problems.append(
                    "reason %s != expected %s"
                    % (outcome.reason_code, expected["expected_reason_code"])
                )
        ok = not problems
        # The readiness gate covers every skill-get corpus case (category,
        # not tool_name: unknown-tool negatives deliberately carry other names).
        if entry.get("category") == "skill-get" and not ok:
            gate_green = False
        results.append(
            CaseResult(
                case_id=entry["case_id"],
                ok=ok,
                message="; ".join(problems) if problems else "OK",
                accept=str(outcome.accept),
                reason=outcome.reason_code or "-",
                result_digest=outcome.result_digest or "-",
            )
        )
    return results, gate_green


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="CTR-002 tool-specific success validation matrix validator "
        "(Contract 12.7.1 v1.1)"
    )
    parser.add_argument(
        "--policy", required=True, type=Path, help="path to tool-success-validation.v1.json"
    )
    parser.add_argument(
        "--fixtures",
        type=Path,
        default=None,
        help="tools/ fixture root (default: <policy dir>/../tools)",
    )
    args = parser.parse_args(argv)

    try:
        policy = load_policy(args.policy)
    except PolicyError as exc:
        print("POLICY ERROR: %s" % exc)
        return 2

    fixtures_root = args.fixtures
    if fixtures_root is None:
        fixtures_root = args.policy.resolve().parent.parent / "tools"
    if not fixtures_root.is_dir():
        print("CORPUS ERROR: fixtures root not found: %s" % fixtures_root)
        return 2

    try:
        results, gate_green = evaluate_corpus(policy, fixtures_root)
    except (CorpusError, OSError) as exc:
        print("CORPUS ERROR: %s" % exc)
        return 2

    print("policy digest: %s (%s)" % (policy.digest, CONTRACT_REVISION))
    failed = 0
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        if not result.ok:
            failed += 1
        print(
            "[%s] %-56s accept=%-5s reason=%-38s %s (%s)"
            % (
                status,
                result.case_id,
                result.accept,
                result.reason,
                result.result_digest,
                result.message,
            )
        )
    print(
        "%s readiness gate: %s"
        % (GATED_TOOL, "GREEN" if gate_green else "NOT GREEN")
    )
    print("cases: %d, failed: %d" % (len(results), failed))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
