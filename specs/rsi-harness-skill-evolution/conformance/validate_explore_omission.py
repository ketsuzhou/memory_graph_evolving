#!/usr/bin/env python3
"""CTR-003 ExploreResult top-level omission carrier validator (Contract 12.7.2 v1.1).

Validates the carrier frozen by System Contract 12.7.2 (revision v1.1,
CTR-003) against the golden corpus under ``conformance/tools/explore/
{top-level-omission,no-omission,negative}``. stdlib only. Read-only: it never
writes or self-updates fixtures. The corpus shares ``tools/manifest.json``
with CTR-002 but is registered in the appended ``omission_cases`` array; the
CTR-002 ``cases`` array and ``validate_tool_binding.py`` stay untouched.

Carrier summary (Contract 12.7.2 v1.1, CTR-003)
-----------------------------------------------
ExploreResult gains the REQUIRED closed top-level field ``omissions``
(empty list when nothing was omitted - determined semantics, never absent).
Each entry is the closed object ``{kind, ref, reason_code}``:

- kind: closed enum evidence|skill; binds the ref schema (evidence ->
  Contract 7.7 EvidenceRef, committed|sealed only; skill -> Contract 7.3
  SkillArtifactRef). kind/ref schema mismatch -> REF_MISMATCH; latest /
  naked-id / Graph-node shapes -> NON_EXACT_REF.
- reason_code: exactly the four gms-truncation-success registry codes
  (TOTAL_CAP_REACHED, EVIDENCE_SUBCAP_REACHED, SKILL_SUBCAP_REACHED,
  GUIDANCE_TOKEN_BUDGET_REACHED); unknown code -> SCHEMA_ENUM_INVALID; the
  code must be kind-compatible (evidence: total|evidence subcap; skill:
  total|skill subcap|guidance token budget).
- Branch omission stays inside GuidanceView (7.15 omitted_branch_refs /
  expandable_refs); any branch-shaped top-level entry fails closed.

Cross-field obligations (validator-enforced; no conditional JSON keys):
- ordering: (kind ascending, RFC 8785 JCS canonical bytes of the exact ref
  ascending) - replayable ranking, misordered -> TOOL_RESULT_BINDING_INVALID.
- dedup: one entry per exact ref; duplicates -> TOOL_RESULT_BINDING_INVALID.
- served disjointness: refs served by this payload or by an earlier page of
  the same ExploreSession (session.prior_served) MUST NOT be omitted ->
  EXPLORE_FENCE_CONFLICT.
- bidirectional consistency: truncation_reason_codes is non-empty iff
  omissions is non-empty and equals the order-preserving dedupe of the entry
  reason_codes (silent truncation and reason-less refs are both
  TOOL_RESULT_BINDING_INVALID).
- budget accounting replay: budgets.total_used == len(evidence_results) +
  len(skill_results) (omitted items never consume budget); every claimed cap
  code must be truthful (cap actually filled plus a matching entry) ->
  BUDGET_INVALID.
- fence/watermark advancement: within one ExploreSession each newly served
  page strictly advances projected_through_activation_sequence beyond the
  prior page watermark; non-advancing -> EXPLORE_FENCE_CONFLICT.

Fail-closed derivation order (first violation wins)
---------------------------------------------------
 1. result body not a closed DTO object        -> ARTIFACT_BODY_INVALID
 2. result schema_version != explore-result    -> TOOL_RESULT_BINDING_INVALID
    (wrong result type)
 3. unknown top-level field / wrapper key      -> SCHEMA_FIELD_UNKNOWN
 4. required field missing                     -> SCHEMA_REQUIRED_FIELD_MISSING
    (omissions missing while truncation codes are non-empty is the silent
    truncation shape -> TOOL_RESULT_BINDING_INVALID)
 5. per-entry closed shape / kind enum / code enum / kind-ref binding /
    exactness / kind-code compatibility
    -> SCHEMA_FIELD_UNKNOWN / SCHEMA_ENUM_INVALID / REF_MISMATCH /
       NON_EXACT_REF / EVIDENCE_NOT_COMMITTED
 6. duplicate exact ref                        -> TOOL_RESULT_BINDING_INVALID
 7. already-served ref omitted                 -> EXPLORE_FENCE_CONFLICT
 8. non-canonical ordering                     -> TOOL_RESULT_BINDING_INVALID
 9. truncation_reason_codes != dedupe(entries) -> TOOL_RESULT_BINDING_INVALID
10. entry result_type enums                    -> SCHEMA_ENUM_INVALID
11. nested watermark/budgets/fences subfields  -> SCHEMA_REQUIRED_FIELD_MISSING
12. budget values / accounting / truthfulness  -> BUDGET_INVALID
13. session watermark not advancing            -> EXPLORE_FENCE_CONFLICT
14. requested_min_activation_sequence behind   -> PROJECTION_BEHIND_REQUIRED_SEQUENCE
15. read audit vs request room/agent/scope     -> EXPLORE_SCOPE_VIOLATION
16. otherwise accepted, result_digest = SHA-256(JCS(result_payload))

Reason codes: only codes already frozen by the Contract 13.7.1 (CTR-004)
registries are reused; no new code is introduced.

CLI
---
    python3 validate_explore_omission.py --fixtures <tools/explore dir>

The manifest is read from ``<fixtures>/../manifest.json``. Exit codes: 0 all
cases pass; 1 at least one case fails; 2 corpus/manifest integrity error.
Output is deterministic (manifest order).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

CASE_SCHEMA_VERSION = "rsih-skill-evolution.explore-omission-case.v1"
EXPECTED_SCHEMA_VERSION = "rsih-skill-evolution.explore-omission-expected.v1"
MANIFEST_SCHEMA_VERSION = "rsih-skill-evolution.tool-binding-manifest.v1"
CONTRACT_SCHEMA_VERSION = "rsih-skill-evolution.system-contract.v1"
CONTRACT_REVISION = "system-contract 12.7.2 v1.1 (CTR-003)"

EXPLORE_RESULT_SCHEMA_VERSION = "gms.explore-result.v1"
GUIDANCE_VIEW_SCHEMA_VERSION = "gms.guidance-view.v1"
KNOWN_RESULT_SCHEMAS = frozenset({EXPLORE_RESULT_SCHEMA_VERSION, GUIDANCE_VIEW_SCHEMA_VERSION})
OMISSION_TOOL_NAMES = frozenset({"memory_explore", "memory_expand"})

# Contract 7.16 closed field group as extended by 12.7.2 v1.1 (CTR-003).
EXPLORE_RESULT_CLOSED_FIELDS = frozenset(
    {
        "schema_version",
        "explore_session_id",
        "query_digest",
        "ranker_policy_ref",
        "watermark",
        "min_activation_sequence",
        "evidence_results",
        "skill_results",
        "served_fences",
        "budgets",
        "truncation_reason_codes",
        "omissions",
    }
)
EXPLORE_RESULT_REQUIRED_FIELDS = tuple(
    EXPLORE_RESULT_CLOSED_FIELDS - {"min_activation_sequence"}
)

OMISSION_ENTRY_FIELDS = frozenset({"kind", "ref", "reason_code"})
OMISSION_KINDS = ("evidence", "skill")
REF_SCHEMA_BY_KIND = {
    "evidence": "gms.evidence-ref.v1",
    "skill": "gms.skill-artifact-ref.v1",
}

# Contract 11.4 / 13.7.1 registry group gms-truncation-success (success
# truncation metadata; MUST NOT appear in error.reason_code slots).
TRUNCATION_REASON_CODES = frozenset(
    {
        "TOTAL_CAP_REACHED",
        "EVIDENCE_SUBCAP_REACHED",
        "SKILL_SUBCAP_REACHED",
        "GUIDANCE_TOKEN_BUDGET_REACHED",
    }
)
KIND_CODE_COMPAT = {
    "evidence": frozenset({"TOTAL_CAP_REACHED", "EVIDENCE_SUBCAP_REACHED"}),
    "skill": frozenset(
        {"TOTAL_CAP_REACHED", "SKILL_SUBCAP_REACHED", "GUIDANCE_TOKEN_BUDGET_REACHED"}
    ),
}

EVIDENCE_REF_REQUIRED = (
    "schema_version",
    "evidence_id",
    "version",
    "evidence_digest",
    "commit_state",
    "evidence_kind",
)
EVIDENCE_REF_CLOSED = frozenset(EVIDENCE_REF_REQUIRED) | {"source_segment_ref"}
EVIDENCE_KINDS = frozenset(
    {
        "success_path",
        "failure_path",
        "recovery_path",
        "observation",
        "replay_result",
        "human_attestation",
    }
)
EVIDENCE_COMMIT_STATES = frozenset({"committed", "sealed"})

SKILL_REF_REQUIRED = ("schema_version", "lineage_id", "version", "kind", "artifact_digest")
SKILL_REF_CLOSED = frozenset(SKILL_REF_REQUIRED)
SKILL_KINDS = frozenset({"human_procedure", "step_guidance", "composite"})

REQUIRED_WATERMARK_FIELDS = (
    "schema_version",
    "projection_stream",
    "projection_schema_version",
    "projection_head",
    "projected_through_activation_sequence",
    "source_ledger_digest",
    "state",
    "watermark_digest",
)
REQUIRED_BUDGET_FIELDS = (
    "total_cap",
    "evidence_subcap",
    "skill_subcap",
    "guidance_token_budget",
    "total_used",
)
REQUIRED_SERVED_FENCE_FIELDS = ("evidence_fence_digest", "skill_fence_digest")

# Extensions are permitted on any closed DTO (Contract 7.1/6.4).
IMPLICIT_PERMITTED_FIELDS = frozenset({"extensions"})

OMISSION_CATEGORIES = {
    "top-level-omission": "top-level-omission/",
    "no-omission": "no-omission/",
    "negative": "negative/",
}

# Closed set of failure/success-metadata codes this validator may derive; all
# of them are reused from the Contract 13.7.1 frozen registries.
OMISSION_REASON_CODES = frozenset(
    {
        "TOOL_UNSUPPORTED",
        "ARTIFACT_BODY_INVALID",
        "TOOL_RESULT_BINDING_INVALID",
        "SCHEMA_FIELD_UNKNOWN",
        "SCHEMA_REQUIRED_FIELD_MISSING",
        "SCHEMA_ENUM_INVALID",
        "REF_MISMATCH",
        "NON_EXACT_REF",
        "EVIDENCE_NOT_COMMITTED",
        "BUDGET_INVALID",
        "EXPLORE_FENCE_CONFLICT",
        "PROJECTION_BEHIND_REQUIRED_SEQUENCE",
        "EXPLORE_SCOPE_VIOLATION",
    }
)

SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
BOM = b"\xef\xbb\xbf"


class CanonicalizationError(Exception):
    """Raised when a value cannot enter the integer-only hashed core."""

    def __init__(self, reason_code: str):
        super().__init__(reason_code)
        self.reason_code = reason_code


class CorpusError(Exception):
    """The omission corpus (manifest or case files) is inconsistent."""


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
# Deterministic exact-ref constructors (shared by the corpus generator and
# the unit tests; the validator itself never constructs refs at runtime).
# ---------------------------------------------------------------------------


def _label_digest(namespace: str, label: str) -> str:
    return digest_bytes(("%s:%s" % (namespace, label)).encode("utf-8"))


def mk_evidence_ref(
    label: str, version: int = 1, commit_state: str = "committed",
    evidence_kind: str = "observation",
) -> dict:
    """Deterministic closed EvidenceRef for corpus/tests (Contract 7.7)."""
    return {
        "schema_version": "gms.evidence-ref.v1",
        "evidence_id": "ev-ctr003-%s" % label,
        "version": version,
        "evidence_digest": _label_digest("evidence", label),
        "commit_state": commit_state,
        "evidence_kind": evidence_kind,
    }


def mk_skill_ref(label: str, version: int = 1, kind: str = "human_procedure") -> dict:
    """Deterministic closed SkillArtifactRef for corpus/tests (7.3)."""
    return {
        "schema_version": "gms.skill-artifact-ref.v1",
        "lineage_id": "lin-ctr003-%s" % label,
        "version": version,
        "kind": kind,
        "artifact_digest": _label_digest("skill", label),
    }


# ---------------------------------------------------------------------------
# Canonicalization and ranking (pure; separated from the checks below per the
# CTR-003 refactor step: omission-list canonicalization vs ranking checks).
# ---------------------------------------------------------------------------


def canonical_ref_key(ref) -> bytes:
    """Exact-ref identity/canonical form: the RFC 8785 JCS bytes."""
    return jcs(ref)


def canonical_omission_key(entry: dict) -> tuple:
    """Ranking key: (kind ascending, JCS(ref) ascending) - 12.7.2 C2."""
    return (entry["kind"], canonical_ref_key(entry["ref"]))


def canonical_omission_order(entries) -> list:
    """Return the entries in the frozen canonical (kind, JCS ref) order."""
    return sorted(entries, key=canonical_omission_key)


def derive_truncation_codes(entries) -> list:
    """Order-preserving dedupe of entry reason_codes - 12.7.2 C4."""
    codes: "list[str]" = []
    for entry in entries:
        code = entry.get("reason_code")
        if code not in codes:
            codes.append(code)
    return codes


# ---------------------------------------------------------------------------
# Closed-shape checks: exact refs (7.3/7.7) and carrier entries.
# ---------------------------------------------------------------------------


def _is_sha256(value) -> bool:
    return isinstance(value, str) and SHA256_RE.match(value) is not None


def _is_non_negative_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _check_exact_evidence_ref(ref: dict) -> "str | None":
    """Exact closed EvidenceRef shape; staged evidence fails closed."""
    extra = set(ref) - EVIDENCE_REF_CLOSED
    if extra or any(field not in ref for field in EVIDENCE_REF_REQUIRED):
        return "NON_EXACT_REF"
    if not isinstance(ref["evidence_id"], str) or not ref["evidence_id"]:
        return "NON_EXACT_REF"
    if not isinstance(ref["version"], int) or isinstance(ref["version"], bool):
        return "NON_EXACT_REF"
    if ref["version"] < 1 or not _is_sha256(ref["evidence_digest"]):
        return "NON_EXACT_REF"
    if ref["evidence_kind"] not in EVIDENCE_KINDS:
        return "NON_EXACT_REF"
    if ref["commit_state"] not in EVIDENCE_COMMIT_STATES:
        return "EVIDENCE_NOT_COMMITTED"
    if "source_segment_ref" in ref and not isinstance(ref["source_segment_ref"], dict):
        return "NON_EXACT_REF"
    return None


def _check_exact_skill_ref(ref: dict) -> "str | None":
    """Exact closed SkillArtifactRef shape."""
    extra = set(ref) - SKILL_REF_CLOSED
    if extra or any(field not in ref for field in SKILL_REF_REQUIRED):
        return "NON_EXACT_REF"
    if not isinstance(ref["lineage_id"], str) or not ref["lineage_id"]:
        return "NON_EXACT_REF"
    if not isinstance(ref["version"], int) or isinstance(ref["version"], bool):
        return "NON_EXACT_REF"
    if ref["version"] < 1 or ref["kind"] not in SKILL_KINDS:
        return "NON_EXACT_REF"
    if not _is_sha256(ref["artifact_digest"]):
        return "NON_EXACT_REF"
    return None


def _check_omission_entry(entry) -> "str | None":
    """Closed carrier entry: shape, enums, kind<->ref binding, exactness."""
    if not isinstance(entry, dict):
        return "SCHEMA_ENUM_INVALID"
    extra = set(entry) - OMISSION_ENTRY_FIELDS
    if extra:
        return "SCHEMA_FIELD_UNKNOWN"
    kind = entry.get("kind")
    if kind not in OMISSION_KINDS:
        # Includes branch-shaped entries: branch omission belongs to the
        # GuidanceView closed DTO, never to the top-level carrier (12.7.2).
        return "SCHEMA_ENUM_INVALID"
    ref = entry.get("ref")
    if not isinstance(ref, dict):
        return "NON_EXACT_REF"
    reason_code = entry.get("reason_code")
    if reason_code not in TRUNCATION_REASON_CODES:
        return "SCHEMA_ENUM_INVALID"
    if not isinstance(ref.get("schema_version"), str):
        return "NON_EXACT_REF"
    if ref.get("schema_version") != REF_SCHEMA_BY_KIND[kind]:
        return "REF_MISMATCH"
    exact = (
        _check_exact_evidence_ref(ref)
        if kind == "evidence"
        else _check_exact_skill_ref(ref)
    )
    if exact is not None:
        return exact
    if reason_code not in KIND_CODE_COMPAT[kind]:
        return "SCHEMA_ENUM_INVALID"
    return None


# ---------------------------------------------------------------------------
# Served-set construction (fence accounting input; separate from ranking).
# ---------------------------------------------------------------------------


def _served_keys(payload: dict) -> set:
    """Exact-ref JCS keys served by THIS payload's top-level results."""
    served: set = set()
    for entry in payload.get("evidence_results") or []:
        ref = entry.get("evidence_ref") if isinstance(entry, dict) else None
        if isinstance(ref, dict):
            served.add(canonical_ref_key(ref))
    for entry in payload.get("skill_results") or []:
        ref = entry.get("skill_ref") if isinstance(entry, dict) else None
        if isinstance(ref, dict):
            served.add(canonical_ref_key(ref))
    return served


def _prior_served_keys(session) -> set:
    """Exact-ref JCS keys already served by earlier pages of the session."""
    if not isinstance(session, dict):
        return set()
    prior = session.get("prior_served")
    if not isinstance(prior, dict):
        return set()
    served: set = set()
    for field in ("evidence_refs", "skill_refs"):
        for ref in prior.get(field) or []:
            if isinstance(ref, dict):
                served.add(canonical_ref_key(ref))
    return served


# ---------------------------------------------------------------------------
# Derivation: payload-level carrier/accounting/fence semantics (12.7.2 C1-C6).
# ---------------------------------------------------------------------------


def derive_omission_outcome(payload, session=None) -> Outcome:
    """Validate one ExploreResult payload against 12.7.2 v1.1 (C1-C6)."""
    try:
        return _derive_omission_outcome(payload, session)
    except CanonicalizationError:
        return Outcome(False, "NON_EXACT_REF")


def _fail(reason_code: str) -> Outcome:
    return Outcome(False, reason_code)


def _derive_omission_outcome(payload, session) -> Outcome:
    if not isinstance(payload, dict):
        return _fail("ARTIFACT_BODY_INVALID")
    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, str) or schema_version not in KNOWN_RESULT_SCHEMAS:
        return _fail("ARTIFACT_BODY_INVALID")
    if schema_version != EXPLORE_RESULT_SCHEMA_VERSION:
        return _fail("TOOL_RESULT_BINDING_INVALID")  # wrong result type

    extra = set(payload) - EXPLORE_RESULT_CLOSED_FIELDS - IMPLICIT_PERMITTED_FIELDS
    if extra:
        return _fail("SCHEMA_FIELD_UNKNOWN")
    codes_field = payload.get("truncation_reason_codes")
    for field in EXPLORE_RESULT_REQUIRED_FIELDS:
        if field not in payload:
            if field == "omissions" and isinstance(codes_field, list) and codes_field:
                # Silent truncation: codes without the carrier (12.7.1 M2
                # interim shape) keeps the original closed failure code.
                return _fail("TOOL_RESULT_BINDING_INVALID")
            return _fail("SCHEMA_REQUIRED_FIELD_MISSING")
    omissions = payload["omissions"]
    if not isinstance(omissions, list) or not isinstance(codes_field, list):
        return _fail("SCHEMA_ENUM_INVALID")

    # C1: per-entry closed shape, enums, binding, exactness, compatibility.
    for entry in omissions:
        problem = _check_omission_entry(entry)
        if problem is not None:
            return _fail(problem)

    # C2: dedup (one entry per exact ref).
    keys = [canonical_ref_key(entry["ref"]) for entry in omissions]
    if len(set(keys)) != len(keys):
        return _fail("TOOL_RESULT_BINDING_INVALID")

    # C3: served disjointness (this payload + earlier session pages).
    served = _served_keys(payload) | _prior_served_keys(session)
    for key in keys:
        if key in served:
            return _fail("EXPLORE_FENCE_CONFLICT")

    # C2: canonical ordering (kind asc, JCS ref asc) - replayable ranking.
    if [canonical_omission_key(entry) for entry in omissions] != sorted(
        canonical_omission_key(entry) for entry in omissions
    ):
        return _fail("TOOL_RESULT_BINDING_INVALID")

    # C4: bidirectional consistency with truncation_reason_codes.
    if codes_field != derive_truncation_codes(omissions):
        return _fail("TOOL_RESULT_BINDING_INVALID")

    # C7: typed result arrays.
    evidence_results = payload["evidence_results"]
    skill_results = payload["skill_results"]
    if not isinstance(evidence_results, list) or not isinstance(skill_results, list):
        return _fail("SCHEMA_REQUIRED_FIELD_MISSING")
    for entry in evidence_results:
        if not isinstance(entry, dict) or entry.get("result_type") != "evidence":
            return _fail("SCHEMA_ENUM_INVALID")
    for entry in skill_results:
        if not isinstance(entry, dict) or entry.get("result_type") != "skill":
            return _fail("SCHEMA_ENUM_INVALID")

    # Nested closed sub-objects needed by the accounting/fence obligations.
    watermark = payload.get("watermark") or {}
    for field in REQUIRED_WATERMARK_FIELDS:
        if field not in watermark:
            return _fail("SCHEMA_REQUIRED_FIELD_MISSING")
    budgets = payload.get("budgets") or {}
    for field in REQUIRED_BUDGET_FIELDS:
        if field not in budgets:
            return _fail("SCHEMA_REQUIRED_FIELD_MISSING")
    fences = payload.get("served_fences") or {}
    for field in REQUIRED_SERVED_FENCE_FIELDS:
        if field not in fences:
            return _fail("SCHEMA_REQUIRED_FIELD_MISSING")

    # C5: replayable budget accounting - only served items consume budget.
    budget_values = [budgets[field] for field in REQUIRED_BUDGET_FIELDS]
    if not all(_is_non_negative_int(value) for value in budget_values):
        return _fail("BUDGET_INVALID")
    token_sum = 0
    for entry in skill_results:
        view = entry.get("guidance_view")
        if isinstance(view, dict) and _is_non_negative_int(view.get("content_token_count")):
            token_sum += view["content_token_count"]
    if (
        budgets["total_used"] != len(evidence_results) + len(skill_results)
        or budgets["total_used"] > budgets["total_cap"]
        or len(evidence_results) > budgets["evidence_subcap"]
        or len(skill_results) > budgets["skill_subcap"]
        or token_sum > budgets["guidance_token_budget"]
    ):
        return _fail("BUDGET_INVALID")

    # C5: truncation claims must be truthful (cap actually filled + entry).
    for code in codes_field:
        carried = [entry for entry in omissions if entry["reason_code"] == code]
        if code == "TOTAL_CAP_REACHED":
            if budgets["total_used"] != budgets["total_cap"] or not carried:
                return _fail("BUDGET_INVALID")
        elif code == "EVIDENCE_SUBCAP_REACHED":
            if (
                len(evidence_results) != budgets["evidence_subcap"]
                or not carried
                or not any(entry["kind"] == "evidence" for entry in carried)
            ):
                return _fail("BUDGET_INVALID")
        elif code == "SKILL_SUBCAP_REACHED":
            if (
                len(skill_results) != budgets["skill_subcap"]
                or not carried
                or not any(entry["kind"] == "skill" for entry in carried)
            ):
                return _fail("BUDGET_INVALID")
        elif code == "GUIDANCE_TOKEN_BUDGET_REACHED":
            if not any(entry["kind"] == "skill" for entry in carried):
                return _fail("BUDGET_INVALID")

    # C6: within one ExploreSession each newly served page strictly advances
    # the watermark beyond the prior served page.
    projected = watermark.get("projected_through_activation_sequence")
    if not _is_non_negative_int(projected):
        return _fail("SCHEMA_REQUIRED_FIELD_MISSING")
    prior = session.get("prior_watermark") if isinstance(session, dict) else None
    if isinstance(prior, dict):
        prior_projected = prior.get("projected_through_activation_sequence")
        if _is_non_negative_int(prior_projected) and projected <= prior_projected:
            return _fail("EXPLORE_FENCE_CONFLICT")

    return Outcome(True, None, digest_bytes(jcs(payload)))


# ---------------------------------------------------------------------------
# Derivation: case-level (tool binding + request-carried obligations).
# ---------------------------------------------------------------------------


def derive_outcome(case) -> Outcome:
    """Derive one corpus case (envelope -> payload -> request checks)."""
    if not isinstance(case, dict):
        raise CorpusError("case must be a JSON object")
    tool_name = case.get("tool_name")
    if tool_name not in OMISSION_TOOL_NAMES:
        return Outcome(False, "TOOL_UNSUPPORTED")
    session = case.get("session") if isinstance(case.get("session"), dict) else None
    outcome = derive_omission_outcome(case.get("result_payload"), session)
    if not outcome.accept:
        return outcome
    payload = case["result_payload"]
    request = case.get("request") if isinstance(case.get("request"), dict) else {}
    audit = case.get("read_audit") if isinstance(case.get("read_audit"), dict) else {}

    # Freshness floor is carried by request fields (12.7.1 explore-family).
    requested = request.get("requested_min_activation_sequence")
    if _is_non_negative_int(requested):
        projected = payload["watermark"]["projected_through_activation_sequence"]
        if projected < requested:
            return Outcome(False, "PROJECTION_BEHIND_REQUIRED_SEQUENCE")

    # Authorization is carried by request + authoritative read audit record.
    for field in ("room_id", "agent_id", "scope_profile_ref"):
        if field in audit and field in request and audit[field] != request[field]:
            return Outcome(False, "EXPLORE_SCOPE_VIOLATION")
    return outcome


# ---------------------------------------------------------------------------
# Corpus runner: tools/manifest.json omission_cases + exact expected match.
# ---------------------------------------------------------------------------


def _read_json(path: Path, what: str) -> dict:
    data = path.read_bytes()
    if data.startswith(BOM):
        raise CorpusError("%s %s starts with a UTF-8 BOM" % (what, path))
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CorpusError("%s %s is not valid UTF-8/JSON: %s" % (what, path, exc))


def _load_manifest(fixtures_root: Path) -> list:
    """Load the appended omission_cases registration from tools/manifest.json."""
    manifest_path = fixtures_root.parent / "manifest.json"
    if not manifest_path.is_file():
        raise CorpusError("tools manifest not found next to %s" % fixtures_root)
    manifest = _read_json(manifest_path, "tools manifest")
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise CorpusError("tools manifest schema_version must be %r" % MANIFEST_SCHEMA_VERSION)
    if manifest.get("contract_schema_version") != CONTRACT_SCHEMA_VERSION:
        raise CorpusError("tools manifest contract_schema_version mismatch")
    entries = manifest.get("omission_cases")
    if not isinstance(entries, list) or not entries:
        raise CorpusError("tools manifest must append a non-empty omission_cases array")
    tools_root = fixtures_root.parent
    seen = set()
    for entry in entries:
        case_id = entry.get("case_id")
        category = entry.get("category")
        if not isinstance(case_id, str) or not case_id:
            raise CorpusError("omission_cases entry without case_id")
        if case_id in seen:
            raise CorpusError("duplicate omission case id %r" % case_id)
        seen.add(case_id)
        if category not in OMISSION_CATEGORIES:
            raise CorpusError("omission case %r has unknown category %r" % (case_id, category))
        prefix = OMISSION_CATEGORIES[category]
        if entry.get("input_path") != "explore/%s%s/input.json" % (prefix, case_id):
            raise CorpusError(
                "omission case %r input_path must be explore/%s%s/input.json"
                % (case_id, prefix, case_id)
            )
        if entry.get("expected_path") != "explore/%s%s/expected.json" % (prefix, case_id):
            raise CorpusError(
                "omission case %r expected_path must be explore/%s%s/expected.json"
                % (case_id, prefix, case_id)
            )
        if entry.get("tool_name") not in OMISSION_TOOL_NAMES:
            raise CorpusError("omission case %r carries an unbound tool_name" % case_id)
        if not isinstance(entry.get("expected_accept"), bool):
            raise CorpusError("omission case %r must mirror expected_accept" % case_id)
        mirror = entry.get("expected_reason_code")
        if entry["expected_accept"] and mirror is not None:
            raise CorpusError("accepted omission case %r must mirror a null reason" % case_id)
        if not entry["expected_accept"] and not isinstance(mirror, str):
            raise CorpusError("rejected omission case %r must mirror its reason" % case_id)
        for key in ("input_path", "expected_path"):
            if not (tools_root / entry[key]).is_file():
                raise CorpusError("omission case %r %s missing on disk" % (case_id, key))
    if seen & {e.get("case_id") for e in manifest.get("cases") or []}:
        raise CorpusError("omission_cases must not re-register a cases entry")
    # Corpus completeness: every case directory under the three owned subtrees
    # is declared (tools/explore/basic stays owned by the CTR-002 corpus).
    for category, prefix in OMISSION_CATEGORIES.items():
        base = fixtures_root / prefix
        if not base.is_dir():
            raise CorpusError("omission fixture category directory missing: %s" % prefix)
        for child in sorted(base.iterdir()):
            if child.is_dir() and child.name not in seen:
                raise CorpusError(
                    "undeclared omission case directory not in manifest: %s" % child.name
                )
    return entries


def _load_case(entry: dict, tools_root: Path) -> dict:
    case = _read_json(tools_root / entry["input_path"], "case input")
    if case.get("schema_version") != CASE_SCHEMA_VERSION:
        raise CorpusError(
            "case input %s schema_version must be %r"
            % (entry["input_path"], CASE_SCHEMA_VERSION)
        )
    if case.get("case_id") != entry["case_id"]:
        raise CorpusError("case input %s case_id mismatch" % entry["input_path"])
    if case.get("tool_name") != entry["tool_name"]:
        raise CorpusError("case input %s tool_name mismatch" % entry["input_path"])
    session = case.get("session")
    if session is not None:
        if not isinstance(session, dict) or set(session) != {
            "prior_watermark",
            "prior_served",
        }:
            raise CorpusError("case %s session block is not closed" % entry["case_id"])
        prior_watermark = session.get("prior_watermark")
        if not isinstance(prior_watermark, dict) or not _is_non_negative_int(
            prior_watermark.get("projected_through_activation_sequence")
        ):
            raise CorpusError("case %s session.prior_watermark invalid" % entry["case_id"])
        prior_served = session.get("prior_served")
        if not isinstance(prior_served, dict) or set(prior_served) != {
            "evidence_refs",
            "skill_refs",
        }:
            raise CorpusError("case %s session.prior_served invalid" % entry["case_id"])
        for field in ("evidence_refs", "skill_refs"):
            if not isinstance(prior_served[field], list) or not all(
                isinstance(ref, dict) for ref in prior_served[field]
            ):
                raise CorpusError(
                    "case %s session.prior_served.%s invalid" % (entry["case_id"], field)
                )
    return case


def _load_expected(entry: dict, tools_root: Path) -> dict:
    expected = _read_json(tools_root / entry["expected_path"], "expected")
    if expected.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        raise CorpusError(
            "expected %s schema_version must be %r"
            % (entry["expected_path"], EXPECTED_SCHEMA_VERSION)
        )
    if expected.get("case_id") != entry["case_id"]:
        raise CorpusError("expected %s case_id mismatch" % entry["expected_path"])
    if not isinstance(expected.get("expected_accept"), bool):
        raise CorpusError("expected %s expected_accept must be a boolean" % entry["expected_path"])
    if expected["expected_accept"] != entry["expected_accept"]:
        raise CorpusError("expected %s disagrees with the manifest mirror" % entry["expected_path"])
    if expected["expected_accept"]:
        if expected.get("expected_reason_code") is not None:
            raise CorpusError(
                "accepted case %s must not carry a reason" % entry["case_id"]
            )
        if not isinstance(expected.get("expected_result_digest"), str):
            raise CorpusError(
                "accepted case %s must carry expected_result_digest" % entry["case_id"]
            )
    else:
        reason = expected.get("expected_reason_code")
        if not isinstance(reason, str):
            raise CorpusError(
                "rejected case %s must carry expected_reason_code" % entry["case_id"]
            )
        if reason != entry["expected_reason_code"]:
            raise CorpusError(
                "expected %s disagrees with the manifest mirror" % entry["expected_path"]
            )
        if reason not in OMISSION_REASON_CODES:
            raise CorpusError(
                "expected reason %r is outside the closed reason set" % reason
            )
    return expected


def evaluate_corpus(fixtures_root: Path) -> "tuple[list, str]":
    """Derive every omission case and compare against expected.json exactly."""
    entries = _load_manifest(fixtures_root)
    tools_root = fixtures_root.parent
    results = []
    for entry in entries:
        case = _load_case(entry, tools_root)
        expected = _load_expected(entry, tools_root)
        outcome = derive_outcome(case)
        problems = []
        if outcome.accept != expected["expected_accept"]:
            problems.append(
                "accept %s != expected %s" % (outcome.accept, expected["expected_accept"])
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
        results.append(
            CaseResult(
                case_id=entry["case_id"],
                ok=not problems,
                message="; ".join(problems) if problems else "OK",
                accept=str(outcome.accept),
                reason=outcome.reason_code or "-",
                result_digest=outcome.result_digest or "-",
            )
        )
    return results, CONTRACT_REVISION


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="CTR-003 ExploreResult top-level omission carrier validator "
        "(Contract 12.7.2 v1.1)"
    )
    parser.add_argument(
        "--fixtures",
        required=True,
        type=Path,
        help="tools/explore fixture root (manifest read from ../manifest.json)",
    )
    args = parser.parse_args(argv)

    fixtures_root = args.fixtures
    if not fixtures_root.is_dir():
        print("CORPUS ERROR: fixtures root not found: %s" % fixtures_root)
        return 2
    try:
        results, revision = evaluate_corpus(fixtures_root)
    except CorpusError as exc:
        print("CORPUS ERROR: %s" % exc)
        return 2

    print("contract revision: %s" % revision)
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
    print("cases: %d, failed: %d" % (len(results), failed))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
