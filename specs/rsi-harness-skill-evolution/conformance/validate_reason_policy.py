#!/usr/bin/env python3
"""CTR-004 system reason-code registry policy validator (Python stdlib only).

Reference checker for the two frozen reason-code registries under
``$FIX/policy`` (Contract 13.7.1, revision v1.1):

- ``system-reason-codes.v1.json``  - owner ``gms-system``: the complete
  GMS 11.4 canonical value domain (112 failure + 4 truncation codes, in
  11.4 group order), the FND-001 canonicalization extensions adopted by
  CTR-004 (BOM_NOT_ALLOWED / INVALID_JSON / ILLEGAL_STATE_TRANSITION) and
  the FND-002 recorded-corpus semantic codes (38). Every entry carries a
  total ``status``/``retryable``/``retry_scope``/``terminal`` mapping.
- ``host-proxy-reason-codes.v1.json`` - owner ``host-proxy``: the 12
  Host-owned codes of Host 5.9 plus the transport mapping (precedence,
  shared-code emission, unknown-upstream replacement, stale-CAS and
  late-result rules). It owns no code name that the system registry owns.

Layering (Refactor discipline):
- Registry layer (``ReasonRegistry`` / ``ReasonPolicyBundle``): schema
  closure, uniqueness, ownership, totality invariants, JCS digest
  verification and spec-enum pinning. Knows nothing about transport.
- Transport layer (``TransportResolver``): the total precedence order and
  the status/retry/new-attempt projection. Reads only the frozen mapping
  sections and registry lookups; the free-text ``message`` is accepted
  solely so it can be demonstrably ignored.
- Fixture layer (``validate_reasons``): derives every ``$FIX/reasons``
  case from the policy and compares against the recorded expectation.

Precedence (total order, Contract 13.7.1):
 1. host-local-validation   - a Host-side validation failure wins; any
                              upstream code is demoted to metadata.
 2. upstream-passthrough    - upstream code in the digest-verified system
                              registry (status failure|inconclusive) is
                              passed through verbatim.
 3. unknown-upstream-replaced - any other upstream code is replaced by the
                              Host-local UPSTREAM_SCHEMA_INVALID (the
                              CTR-004 closed unknown-upstream failure,
                              alias UNKNOWN_UPSTREAM_CODE); the original
                              code survives as non-behavioral metadata only.
 4. late-audit-only         - a late upstream result never produces or
                              rewrites a code; audit only.

Totality invariants (per code, mechanically checked):
- retryable == (status == "failure" and retry_scope == "same_request")
- terminal  == (retry_scope != "same_request")
- status in {"success","inconclusive"} => retry_scope == "none",
  retryable is False, terminal is True.
- same_request is restricted to the infra whitelist
  {PROJECTION_BEHIND_REQUIRED_SEQUENCE, FAKE_TOOL_NO_RESPONSE,
   GMS_UNAVAILABLE, HOST_PROXY_TIMEOUT}.

GMS 11.3 canonical policy body (``gms.reason-codes.v1``) is DERIVED from
the system registry (failure/truncation arrays in 11.4 order plus the
three classified arrays); its JCS digest must equal the frozen
GMS_POLICY_BODY_DIGEST below - never hand-copied.

CLI
---
    python3 validate_reason_policy.py --policy-dir <dir-with-the-two-jsons>
                                      [--reasons-dir <dir-with-manifest>]

Exit codes: 0 all checks and fixture cases pass; 1 at least one fixture
case fails; 2 policy/registry/manifest integrity error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Closed validator vocabulary
# ---------------------------------------------------------------------------

SYSTEM_POLICY_FILENAME = "system-reason-codes.v1.json"
HOST_POLICY_FILENAME = "host-proxy-reason-codes.v1.json"

SYSTEM_SCHEMA_VERSION = "rsih-skill-evolution.system-reason-policy.v1"
HOST_SCHEMA_VERSION = "rsih-skill-evolution.host-proxy-reason-policy.v1"
SYSTEM_POLICY_ID = "system-reason-codes"
HOST_POLICY_ID = "host-proxy-reason-codes"
SYSTEM_REGISTRY_OWNER = "gms-system"
HOST_REGISTRY_OWNER = "host-proxy"

SYSTEM_SOURCE_NAME = "system-reason-codes.v1.json"
HOST_SOURCE_NAME = "host-proxy-reason-codes.v1.json"

STATUS_VALUES = ("success", "failure", "inconclusive")
RETRY_SCOPE_VALUES = ("same_request", "new_attempt", "none")
WIRE_STATUS_VALUES = {
    "success": "succeeded",
    "failure": "failed",
    "inconclusive": "inconclusive",
}

TOP_LEVEL_KEYS = frozenset(
    {"schema_version", "policy_id", "version", "registry_owner",
     "transport_mapping", "codes", "registry_digest"}
)
CODE_REQUIRED_KEYS = frozenset(
    {"name", "group", "status", "retryable", "retry_scope", "terminal",
     "owner", "spec_ref"}
)
CODE_OPTIONAL_KEYS = frozenset({"notes"})
CODE_MAPPING_KEYS = frozenset({"status", "retryable", "retry_scope", "terminal"})

CODE_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]+$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

REASON_FIXTURES_SCHEMA_VERSION = "rsih-skill-evolution.reason-fixtures.v1"
REASON_EXPECTED_SCHEMA_VERSION = \
    "rsih-skill-evolution.reason-fixture-expected.v1"
FIXTURE_KINDS = ("resolution", "policy-reject", "message-driven-attempt",
                 "derivation")

# The unknown-upstream replacement is the frozen Host 5.9 code; CTR-004
# documents it as the closed "UNKNOWN_UPSTREAM_CODE" failure.
UNKNOWN_UPSTREAM_REPLACEMENT = "UPSTREAM_SCHEMA_INVALID"
UNKNOWN_UPSTREAM_ALIAS = "UNKNOWN_UPSTREAM_CODE"

# ---------------------------------------------------------------------------
# Frozen expected model - GMS 11.4 canonical value domain (group order and
# in-group order are normative for the derived policy body).
# ---------------------------------------------------------------------------

GMS_GROUP_SPECS = (
    ("gms-schema-canonical-ref",
     "GMS 11.4 schema/canonical/ref; Contract 13.7",
     ("SCHEMA_VERSION_UNSUPPORTED", "SCHEMA_FIELD_UNKNOWN",
      "SCHEMA_REQUIRED_FIELD_MISSING", "SCHEMA_ENUM_INVALID",
      "CANONICALIZATION_FAILED", "NON_INTEGER_NUMBER",
      "UNKNOWN_REQUIRED_EXTENSION", "EXTENSION_SCHEMA_INVALID",
      "DIGEST_MISMATCH", "REF_MISMATCH", "NON_EXACT_REF",
      "IDEMPOTENCY_CONFLICT")),
    ("gms-evidence-provenance",
     "GMS 11.4 evidence/Host provenance; Contract 13.7",
     ("EVIDENCE_STAGING_INVALID", "EVIDENCE_NOT_COMMITTED",
      "EVIDENCE_SEAL_INVALID", "SEGMENT_NOT_SETTLED", "CHECKPOINT_NOT_SEALED",
      "EVIDENCE_SCOPE_DENIED", "EVIDENCE_PROVENANCE_INCOMPLETE")),
    ("gms-artifact-proposal-candidate",
     "GMS 11.4 artifact/proposal/candidate; Contract 13.7",
     ("SKILL_KIND_INVALID", "LINEAGE_KIND_MISMATCH", "LINEAGE_VERSION_CONFLICT",
      "ARTIFACT_BODY_INVALID", "BRANCH_PROVENANCE_INCOMPLETE",
      "PORT_SCHEMA_MISSING", "PORT_SCHEMA_INCOMPATIBLE", "COMPOSITE_CYCLE",
      "COMPOSITE_CHILD_NOT_ACTIVE", "COMPOSITE_RETRY_INVALID",
      "COMPOSITE_FAILURE_ACTION_INVALID", "DEPENDENCY_CYCLE",
      "PROPOSAL_INVALID", "PROPOSAL_DUPLICATE", "PROPOSAL_STATE_CONFLICT",
      "PROPOSAL_STALE", "PROPOSAL_WITHDRAWAL_FORBIDDEN", "CANDIDATE_NOT_FOUND",
      "CANDIDATE_IMMUTABLE", "CANDIDATE_NOT_EXECUTABLE",
      "CANDIDATE_RELEASED_BODY_MISMATCH")),
    ("gms-replay-evaluation-release",
     "GMS 11.4 replay/evaluation/release; Contract 13.7",
     ("VALIDATION_FAILED", "VALIDATION_INPUT_STALE", "FIXTURE_SET_INCOMPLETE",
      "REPLAY_REQUEST_INVALID", "REPLAY_NONDETERMINISTIC",
      "REPLAY_RESULT_INVALID", "REPLAY_INCONCLUSIVE", "CRITICAL_REGRESSION",
      "SAFETY_GATE_FAILED", "HARD_GATE_FAILED", "REFERENCE_ENVELOPE_INVALID",
      "UTILITY_NOT_PARETO_IMPROVED", "UTILITY_COST_BUDGET_EXCEEDED",
      "UTILITY_ARITHMETIC_OVERFLOW", "RELEASE_DECISION_INVALID",
      "RELEASE_NOT_ACCEPTED", "RELEASE_TRANSACTION_CONFLICT")),
    ("gms-activation",
     "GMS 11.4 activation; Contract 13.7",
     ("ACTIVE_HEAD_CONFLICT", "SOURCE_HEAD_STALE",
      "ACTIVATION_AUTHORIZATION_INVALID", "ACTIVATION_EVENT_INVALID",
      "ACTIVATION_SEQUENCE_CONFLICT", "DEACTIVATION_AUTHORIZATION_INVALID",
      "NO_ACTIVE_HEAD", "PROBATION_UNSUPPORTED",
      "REACTIVATION_AUTHORIZATION_REQUIRED")),
    ("gms-merge-similarity",
     "GMS 11.4 merge/similarity; Contract 13.7",
     ("SIMILARITY_ASSESSMENT_INVALID", "SIMILARITY_BELOW_THRESHOLD",
      "MERGE_SOURCE_INVALID", "MERGE_KIND_UNSUPPORTED",
      "MERGE_SOURCE_NOT_ACTIVE", "MERGE_SOURCE_PROBATIONARY",
      "MERGE_PROPOSAL_DUPLICATE", "MERGE_GROUP_IN_FLIGHT",
      "MERGE_STATE_CONFLICT", "MERGE_BLOCKING_CONFLICT",
      "MERGE_EVIDENCE_INCOMPLETE", "MERGE_BRANCH_REGRESSION",
      "MERGE_SOURCE_HEAD_STALE", "MERGE_SYNTHESIS_INCONCLUSIVE",
      "MERGE_WITHDRAWAL_FORBIDDEN")),
    ("gms-projector-graph",
     "GMS 11.4 projector/Graph; Contract 13.6/13.7",
     ("PROJECTION_SEQUENCE_GAP", "PROJECTION_EVENT_CONFLICT",
      "PROJECTION_SOURCE_GAP", "PROJECTION_SOURCE_CONFLICT",
      "PROJECTION_SCHEMA_UNSUPPORTED", "PROJECTION_RELATION_INVALID",
      "PROJECTION_HEAD_CONFLICT", "PROJECTION_BLOCKED",
      "PROJECTION_REBUILD_REQUIRED", "PROJECTION_BEHIND_REQUIRED_SEQUENCE")),
    ("gms-explore-tool-scope-budget",
     "GMS 11.4 explore/tool/scope/budget; Contract 13.7",
     ("TOOL_ARGUMENTS_INVALID", "TOOL_RESULT_BINDING_INVALID",
      "TOOL_UNSUPPORTED", "EXPLORE_SESSION_INVALID", "EXPLORE_SCOPE_VIOLATION",
      "EXPLORE_QUERY_INVALID", "EXPLORE_CONTINUATION_INVALID",
      "EXPLORE_FILTER_INVALID", "EXPLORE_FENCE_CONFLICT",
      "EXPAND_TARGET_NOT_SERVED", "EXPAND_SOURCE_VIEW_MISMATCH",
      "EXPAND_DEPTH_UNSUPPORTED", "SKILL_NOT_CURRENT_ACTIVE",
      "HISTORICAL_READ_NOT_AUTHORIZED", "GUIDANCE_RENDER_FAILED",
      "GUIDANCE_VIEW_HASH_MISMATCH", "BUDGET_INVALID", "BUDGET_EXCEEDED",
      "CITATION_INVALID", "PERMISSION_CAP_EXCEEDED", "SCOPE_PROFILE_INVALID")),
)
GMS_TRUNCATION_GROUP = "gms-truncation-success"
GMS_TRUNCATION_CODES = (
    "TOTAL_CAP_REACHED", "EVIDENCE_SUBCAP_REACHED", "SKILL_SUBCAP_REACHED",
    "GUIDANCE_TOKEN_BUDGET_REACHED",
)
GMS_FAILURE_GROUPS = tuple(spec[0] for spec in GMS_GROUP_SPECS)
GMS_FAILURE_CODE_ORDER = tuple(
    name for spec in GMS_GROUP_SPECS for name in spec[2]
)

FND001_ADOPTED_CODES = ("BOM_NOT_ALLOWED", "INVALID_JSON",
                        "ILLEGAL_STATE_TRANSITION")

RECORDED_GROUP_SPECS = (
    ("recorded-segment-integrity",
     "FND-002 recorded corpus (segments); adopted by CTR-004 Contract 13.7.1",
     ("DAG_CYCLE", "MISSING_CAUSAL_LINK", "NONTERMINAL_TOOL",
      "APPEND_AFTER_SETTLED", "NONSETTLED_SEGMENT_HAS_REFS",
      "MISSING_SEALED_PATH", "FRONTIER_MISMATCH", "MISSING_FAMILY",
      "FAMILY_MISMATCH", "TERMINAL_MISMATCH", "TERMINAL_AUDIT_MISMATCH",
      "MEMBER_ORDER_INVALID")),
    ("recorded-seal-digest",
     "FND-002 recorded corpus (seals); adopted by CTR-004 Contract 13.7.1",
     ("SEAL_DIGEST_MISMATCH", "EVIDENCE_SEAL_DIGEST_MISMATCH",
      "PATH_SEAL_DIGEST_MISMATCH", "EVIDENCE_SEAL_BODY_MISMATCH",
      "PAYLOAD_DIGEST_MISMATCH", "TOOL_PROXY_DIGEST_MISMATCH")),
    ("recorded-registry-integrity",
     "FND-002 recorded corpus (registry); adopted by CTR-004 Contract 13.7.1",
     ("CANDIDATE_IN_LIVE_REGISTRY",)),
    ("recorded-environment-declaration",
     "FND-002 recorded corpus (environment); adopted by CTR-004 Contract 13.7.1",
     ("REAL_CAPABILITY_DECLARED", "UNDECLARED_TOOL_ACCESS",
      "UNDECLARED_PROVIDER_ACCESS", "UNDECLARED_FS_ACCESS",
      "UNDECLARED_CAPABILITY_ACCESS", "NETWORK_ACCESS_DECLARED",
      "CROSS_RUN_CACHE_DECLARED", "ENVIRONMENT_NOT_NEUTRAL")),
    ("recorded-fake-runtime",
     "FND-002 recorded corpus (Q29-B fake runtime); adopted by CTR-004 Contract 13.7.1",
     ("FAKE_CLOCK_EXHAUSTED", "FAKE_RANDOM_EXHAUSTED",
      "FAKE_TOOL_NO_RESPONSE", "FAKE_TOOL_CANCELLED")),
    ("recorded-late-determinism-expectation",
     "FND-002 recorded corpus (late/determinism/expectation); adopted by CTR-004 Contract 13.7.1",
     ("LATE_OUTPUT_REWRITE", "LATE_EVENT_FIELDS", "NONDETERMINISTIC_OUTPUT",
      "EXPECTED_OUTPUT_MISMATCH", "EXPECTED_OUTPUT_DIGEST_MISMATCH",
      "RUN_STATUS_EXPECTATION_MISMATCH", "RUN_REASON_EXPECTATION_MISMATCH")),
)

# GMS 11.3 frozen v1 classification (derived codes must equal these).
GMS_RETRY_SAME_REQUEST_EXPECTED = ("PROJECTION_BEHIND_REQUIRED_SEQUENCE",)
GMS_NEW_ATTEMPT_EXPECTED = (
    "IDEMPOTENCY_CONFLICT", "PROPOSAL_STATE_CONFLICT", "PROPOSAL_STALE",
    "VALIDATION_INPUT_STALE", "RELEASE_TRANSACTION_CONFLICT",
    "ACTIVE_HEAD_CONFLICT", "SOURCE_HEAD_STALE", "ACTIVATION_SEQUENCE_CONFLICT",
    "MERGE_STATE_CONFLICT", "MERGE_SOURCE_HEAD_STALE",
    "PROJECTION_HEAD_CONFLICT",
)
GMS_INCONCLUSIVE_EXPECTED = ("REPLAY_INCONCLUSIVE", "MERGE_SYNTHESIS_INCONCLUSIVE")

# Infra-only same-request retry whitelist across BOTH registries.
INFRA_SAME_REQUEST_WHITELIST = (
    "PROJECTION_BEHIND_REQUIRED_SEQUENCE", "FAKE_TOOL_NO_RESPONSE",
    "GMS_UNAVAILABLE", "HOST_PROXY_TIMEOUT",
)

# Host 5.9 owned codes (shared passthrough codes excluded - they are
# owned by the system registry and only referenced by the Host policy).
HOST_OWNED_CODES_EXPECTED = (
    "HOST_PROXY_INVALID_REQUEST", "HOST_AGENT_NOT_IN_ROOM", "HOST_SCOPE_DENIED",
    "HOST_TOOL_NOT_ALLOWED", "HOST_PROXY_TIMEOUT", "HOST_PROXY_CANCELLED",
    "GMS_UNAVAILABLE", "UPSTREAM_SCHEMA_INVALID", "UPSTREAM_DIGEST_MISMATCH",
    "UPSTREAM_SCOPE_VIOLATION", "UPSTREAM_BUDGET_VIOLATION",
    "PI_RETURN_CHANNEL_FAILED",
)
HOST_SHARED_EMIT_EXPECTED = ("IDEMPOTENCY_CONFLICT",
                             "PROJECTION_BEHIND_REQUIRED_SEQUENCE")

# Frozen digest of the derived gms.reason-codes.v1 canonical policy body
# (GMS 11.3). Machine-derived; never hand-edited.
GMS_POLICY_BODY_DIGEST = "sha256:998d42eba6b4b9e338497d4ed161e57eb0d804aba1e2f81c9317804cf70a98dd"


def _expected_system_model():
    """name -> (group, status, retryable, retry_scope, terminal)."""
    model = {}
    for group, _spec_ref, names in GMS_GROUP_SPECS:
        for name in names:
            model[name] = (group, "failure", False, "none", True)
    for name in GMS_TRUNCATION_CODES:
        model[name] = (GMS_TRUNCATION_GROUP, "success", False, "none", True)
    for name in FND001_ADOPTED_CODES:
        model[name] = ("contract-canonicalization", "failure", False, "none",
                       True)
    for group, _spec_ref, names in RECORDED_GROUP_SPECS:
        for name in names:
            model[name] = (group, "failure", False, "none", True)
    # Classification overrides (GMS 11.3 + CTR-004 infra whitelist).
    for name in GMS_RETRY_SAME_REQUEST_EXPECTED:
        group = model[name][0]
        model[name] = (group, "failure", True, "same_request", False)
    for name in GMS_NEW_ATTEMPT_EXPECTED:
        group = model[name][0]
        model[name] = (group, "failure", False, "new_attempt", True)
    for name in GMS_INCONCLUSIVE_EXPECTED:
        group = model[name][0]
        model[name] = (group, "inconclusive", False, "none", True)
    for name in ("FAKE_TOOL_NO_RESPONSE",):
        group = model[name][0]
        model[name] = (group, "failure", True, "same_request", False)
    return model


EXPECTED_SYSTEM_MODEL = _expected_system_model()
SYSTEM_CODE_ORDER = tuple(EXPECTED_SYSTEM_MODEL.keys())

# Host owned model: name -> (group, status, retryable, retry_scope, terminal).
EXPECTED_HOST_MODEL = {
    "HOST_PROXY_INVALID_REQUEST": ("host-proxy-request-validation", "failure",
                                   False, "none", True),
    "HOST_AGENT_NOT_IN_ROOM": ("host-proxy-request-validation", "failure",
                               False, "none", True),
    "HOST_SCOPE_DENIED": ("host-proxy-request-validation", "failure", False,
                          "none", True),
    "HOST_TOOL_NOT_ALLOWED": ("host-proxy-request-validation", "failure",
                              False, "none", True),
    "HOST_PROXY_TIMEOUT": ("host-proxy-infra", "failure", True,
                           "same_request", False),
    "HOST_PROXY_CANCELLED": ("host-proxy-delivery", "failure", False, "none",
                             True),
    "GMS_UNAVAILABLE": ("host-proxy-infra", "failure", True, "same_request",
                        False),
    "UPSTREAM_SCHEMA_INVALID": ("host-proxy-upstream-validation", "failure",
                                False, "none", True),
    "UPSTREAM_DIGEST_MISMATCH": ("host-proxy-upstream-validation", "failure",
                                 False, "none", True),
    "UPSTREAM_SCOPE_VIOLATION": ("host-proxy-upstream-validation", "failure",
                                 False, "none", True),
    "UPSTREAM_BUDGET_VIOLATION": ("host-proxy-upstream-validation", "failure",
                                  False, "none", True),
    "PI_RETURN_CHANNEL_FAILED": ("host-proxy-delivery", "failure", False,
                                 "none", True),
}


class PolicyError(Exception):
    """Fail-closed validation error carrying a closed validator code."""

    def __init__(self, reason_code: str, detail: str = ""):
        super().__init__("%s: %s" % (reason_code, detail) if detail
                         else reason_code)
        self.reason_code = reason_code
        self.detail = detail


# ---------------------------------------------------------------------------
# RFC 8785 JCS canonicalization (integer-only hashed core, Contract 6.1)
# ---------------------------------------------------------------------------

_SHORT_ESCAPES = {
    '"': '\\"',
    "\\": "\\\\",
    "\b": "\\b",
    "\f": "\\f",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def _escape_string(value: str) -> str:
    out = ['"']
    for ch in value:
        if ch in _SHORT_ESCAPES:
            out.append(_SHORT_ESCAPES[ch])
        elif ord(ch) < 0x20 or 0xD800 <= ord(ch) <= 0xDFFF:
            out.append("\\u%04x" % ord(ch))
        else:
            out.append(ch)  # raw, incl. U+007F and all non-ASCII
    out.append('"')
    return "".join(out)


def jcs(value) -> bytes:
    """Pure RFC 8785 JCS; floats are rejected (integer-only core)."""
    parts: "list[str]" = []

    def emit(node) -> None:
        if node is None:
            parts.append("null")
        elif node is True:
            parts.append("true")
        elif node is False:
            parts.append("false")
        elif isinstance(node, int):
            parts.append(str(node))
        elif isinstance(node, float):
            raise PolicyError("POLICY_NON_INTEGER_NUMBER",
                              "float %r in hashed core" % node)
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
            raise PolicyError("POLICY_JSON_INVALID",
                              "unsupported JSON value %r" % type(node))

    emit(value)
    return "".join(parts).encode("utf-8")


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def digest_of(value) -> str:
    return digest_bytes(jcs(value))


# ---------------------------------------------------------------------------
# Registry layer
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReasonCode:
    name: str
    group: str
    status: str
    retryable: bool
    retry_scope: str
    terminal: bool
    owner: str
    spec_ref: str
    notes: "str | None" = None

    def behavior(self):
        return (self.group, self.status, self.retryable, self.retry_scope,
                self.terminal)


class ReasonRegistry:
    """One policy document: schema closure, totality and digest."""

    def __init__(self, document, source_name: str, kind: str):
        if not isinstance(document, dict):
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: document is not an object" % source_name)
        self.source_name = source_name
        self.kind = kind
        self._check_top_level(document)

        codes_field = document["codes"]
        if not isinstance(codes_field, list) or not codes_field:
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: codes must be a non-empty array"
                              % source_name)
        entries: "list[ReasonCode]" = []
        seen: set = set()
        for raw in codes_field:
            entry = self._check_code(raw, seen)
            seen.add(entry.name)
            entries.append(entry)
        self._entries = entries
        self._by_name = {entry.name: entry for entry in entries}

        declared = document["registry_digest"]
        body = {k: v for k, v in document.items() if k != "registry_digest"}
        recomputed = digest_of(body)
        if recomputed != declared:
            raise PolicyError(
                "POLICY_DIGEST_MISMATCH",
                "%s: declared %s but recomputed %s"
                % (source_name, declared, recomputed))
        self.registry_digest = declared
        self.document = document

    # -- construction helpers ------------------------------------------------

    def _check_top_level(self, document: dict) -> None:
        keys = set(document.keys())
        missing = TOP_LEVEL_KEYS - keys
        extra = keys - TOP_LEVEL_KEYS
        if missing or extra:
            raise PolicyError(
                "POLICY_SCHEMA_INVALID",
                "%s: top-level keys missing=%s extra=%s"
                % (self.source_name, sorted(missing), sorted(extra)))
        expected_version = (SYSTEM_SCHEMA_VERSION if self.kind == "system"
                            else HOST_SCHEMA_VERSION)
        expected_policy_id = (SYSTEM_POLICY_ID if self.kind == "system"
                              else HOST_POLICY_ID)
        expected_owner = (SYSTEM_REGISTRY_OWNER if self.kind == "system"
                          else HOST_REGISTRY_OWNER)
        if document["schema_version"] != expected_version:
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: schema_version %r" % (self.source_name,
                                                         document["schema_version"]))
        if document["policy_id"] != expected_policy_id:
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: policy_id %r" % (self.source_name,
                                                    document["policy_id"]))
        if document["version"] != 1 or isinstance(document["version"], bool):
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: version must be integer 1" % self.source_name)
        if document["registry_owner"] != expected_owner:
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: registry_owner %r"
                              % (self.source_name, document["registry_owner"]))
        if not isinstance(document["transport_mapping"], dict):
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: transport_mapping must be an object"
                              % self.source_name)
        if not isinstance(document["registry_digest"], str) \
                or not DIGEST_RE.match(document["registry_digest"]):
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: registry_digest malformed" % self.source_name)

    def _check_code(self, raw, seen: set) -> ReasonCode:
        where = "%s codes[%d]" % (self.source_name, len(seen))
        if not isinstance(raw, dict):
            raise PolicyError("POLICY_SCHEMA_INVALID", "%s: not an object" % where)
        keys = set(raw.keys())
        missing = CODE_REQUIRED_KEYS - keys
        extra = keys - (CODE_REQUIRED_KEYS | CODE_OPTIONAL_KEYS)
        if extra:
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: unknown keys %s" % (where, sorted(extra)))
        mapping_missing = missing & CODE_MAPPING_KEYS
        if mapping_missing:
            raise PolicyError("POLICY_MAPPING_NOT_TOTAL",
                              "%s: missing mapping fields %s"
                              % (where, sorted(mapping_missing)))
        if missing:
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: missing fields %s" % (where, sorted(missing)))
        name = raw["name"]
        if not isinstance(name, str) or not CODE_NAME_RE.match(name):
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: malformed code name %r" % (where, name))
        if name in seen:
            raise PolicyError("POLICY_CODE_DUPLICATED",
                              "%s: %s appears more than once" % (where, name))
        for text_field in ("group", "spec_ref"):
            if not isinstance(raw[text_field], str) or not raw[text_field]:
                raise PolicyError("POLICY_SCHEMA_INVALID",
                                  "%s: %s must be a non-empty string"
                                  % (where, text_field))
        if raw["owner"] != (SYSTEM_REGISTRY_OWNER if self.kind == "system"
                            else HOST_REGISTRY_OWNER):
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: owner %r does not match registry owner"
                              % (where, raw["owner"]))
        status = raw["status"]
        retry_scope = raw["retry_scope"]
        retryable = raw["retryable"]
        terminal = raw["terminal"]
        if status not in STATUS_VALUES:
            raise PolicyError("POLICY_MAPPING_NOT_TOTAL",
                              "%s: status %r" % (where, status))
        if retry_scope not in RETRY_SCOPE_VALUES:
            raise PolicyError("POLICY_MAPPING_NOT_TOTAL",
                              "%s: retry_scope %r" % (where, retry_scope))
        if not isinstance(retryable, bool) or not isinstance(terminal, bool):
            raise PolicyError("POLICY_MAPPING_NOT_TOTAL",
                              "%s: retryable/terminal must be booleans" % where)
        expected_retryable = status == "failure" and retry_scope == "same_request"
        if retryable is not expected_retryable:
            raise PolicyError(
                "POLICY_MAPPING_NOT_TOTAL",
                "%s: retryable=%s but status=%s retry_scope=%s"
                % (where, retryable, status, retry_scope))
        if terminal is not (retry_scope != "same_request"):
            raise PolicyError(
                "POLICY_MAPPING_NOT_TOTAL",
                "%s: terminal=%s but retry_scope=%s"
                % (where, terminal, retry_scope))
        if status in ("success", "inconclusive") and retry_scope != "none":
            raise PolicyError("POLICY_MAPPING_NOT_TOTAL",
                              "%s: %s must use retry_scope=none"
                              % (where, status))
        notes = raw.get("notes")
        if notes is not None and not isinstance(notes, str):
            raise PolicyError("POLICY_SCHEMA_INVALID",
                              "%s: notes must be a string" % where)
        return ReasonCode(name=name, group=raw["group"], status=status,
                          retryable=retryable, retry_scope=retry_scope,
                          terminal=terminal, owner=raw["owner"],
                          spec_ref=raw["spec_ref"], notes=notes)

    # -- accessors -----------------------------------------------------------

    def entries(self) -> "list[ReasonCode]":
        return list(self._entries)

    def names(self) -> "list[str]":
        return [entry.name for entry in self._entries]

    def entry(self, name: str) -> "ReasonCode | None":
        return self._by_name.get(name)


class ReasonPolicyBundle:
    """Both registries plus cross-registry ownership and spec pinning."""

    def __init__(self, system: ReasonRegistry, host: ReasonRegistry):
        self.system = system
        self.host = host
        self._check_ownership()
        self._check_system_spec()
        self._check_host_spec()
        self._check_transport_mapping()
        self._check_gms_policy_body()
        self.resolver = TransportResolver(self)

    # -- loading --------------------------------------------------------------

    @staticmethod
    def load(policy_dir: Path) -> "ReasonPolicyBundle":
        system_doc = _load_policy_document(
            Path(policy_dir) / SYSTEM_POLICY_FILENAME, SYSTEM_SOURCE_NAME)
        host_doc = _load_policy_document(
            Path(policy_dir) / HOST_POLICY_FILENAME, HOST_SOURCE_NAME)
        return ReasonPolicyBundle.from_documents(system_doc, host_doc)

    @staticmethod
    def from_documents(system_doc, host_doc) -> "ReasonPolicyBundle":
        system = ReasonRegistry(system_doc, SYSTEM_SOURCE_NAME, "system")
        host = ReasonRegistry(host_doc, HOST_SOURCE_NAME, "host-proxy")
        return ReasonPolicyBundle(system, host)

    # -- cross checks -----------------------------------------------------------

    def _check_ownership(self) -> None:
        overlap = sorted(set(self.host.names()) & set(self.system.names()))
        if overlap:
            raise PolicyError(
                "POLICY_CODE_OVERLAP",
                "host registry re-owns system codes: %s" % ", ".join(overlap))

    def _check_system_spec(self) -> None:
        names = self.system.names()
        if names != list(SYSTEM_CODE_ORDER):
            missing = [n for n in SYSTEM_CODE_ORDER if n not in names]
            extra = [n for n in names if n not in EXPECTED_SYSTEM_MODEL]
            raise PolicyError(
                "POLICY_SPEC_ENUM_DRIFT",
                "system registry membership/order drift "
                "(missing=%s extra=%s)" % (missing, extra))
        for name in names:
            entry = self.system.entry(name)
            if entry.owner != SYSTEM_REGISTRY_OWNER:
                raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                                  "%s owner %r" % (name, entry.owner))
            if entry.behavior() != EXPECTED_SYSTEM_MODEL[name]:
                raise PolicyError(
                    "POLICY_SPEC_ENUM_DRIFT",
                    "%s behavior %r != expected %r"
                    % (name, entry.behavior(), EXPECTED_SYSTEM_MODEL[name]))
        retry_same = tuple(
            n for n in GMS_FAILURE_CODE_ORDER
            if self.system.entry(n).retry_scope == "same_request")
        new_attempt = tuple(
            n for n in GMS_FAILURE_CODE_ORDER
            if self.system.entry(n).retry_scope == "new_attempt")
        inconclusive = tuple(
            n for n in GMS_FAILURE_CODE_ORDER
            if self.system.entry(n).status == "inconclusive")
        if retry_same != GMS_RETRY_SAME_REQUEST_EXPECTED:
            raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                              "retry_same_request_codes %s" % (retry_same,))
        if new_attempt != GMS_NEW_ATTEMPT_EXPECTED:
            raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                              "new_attempt_required_codes %s" % (new_attempt,))
        if inconclusive != GMS_INCONCLUSIVE_EXPECTED:
            raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                              "inconclusive_status_codes %s" % (inconclusive,))
        same_request_all = sorted(
            n for registry in (self.system, self.host)
            for n in registry.names()
            if registry.entry(n).retry_scope == "same_request")
        if same_request_all != sorted(INFRA_SAME_REQUEST_WHITELIST):
            raise PolicyError(
                "POLICY_SPEC_ENUM_DRIFT",
                "same_request scope outside the infra whitelist: %s"
                % (same_request_all,))

    def _check_host_spec(self) -> None:
        names = self.host.names()
        if names != list(HOST_OWNED_CODES_EXPECTED):
            raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                              "host owned codes drift: %s" % (names,))
        for name in names:
            entry = self.host.entry(name)
            expected = EXPECTED_HOST_MODEL[name]
            if (entry.group, entry.status, entry.retryable, entry.retry_scope,
                    entry.terminal) != expected:
                raise PolicyError(
                    "POLICY_SPEC_ENUM_DRIFT",
                    "%s behavior drift %r != %r"
                    % (name, entry.behavior(), expected))

    def host_shared_emit_codes(self) -> "list[str]":
        shared = self.host.document["transport_mapping"].get(
            "shared_codes_emitted_by_host", {})
        codes = shared.get("codes", []) if isinstance(shared, dict) else None
        return list(codes) if isinstance(codes, list) else []

    def _check_transport_mapping(self) -> None:
        system_tm = self.system.document["transport_mapping"]
        _require_keys(system_tm, {"mode", "wire_status_values", "passthrough",
                                  "success_codes", "unknown_code",
                                  "late_result", "message",
                                  "infra_same_request_whitelist"},
                      "system transport_mapping")
        if system_tm["mode"] != "system-cross-end":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "system transport mode %r" % system_tm["mode"])
        if system_tm["wire_status_values"] != WIRE_STATUS_VALUES:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "system wire_status_values drift")
        passthrough = system_tm["passthrough"]
        _require_keys(passthrough,
                      {"eligibility", "meaning_source",
                       "receiver_precondition"},
                      "system passthrough")
        if "failure|inconclusive" not in passthrough["eligibility"]:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "passthrough eligibility must exclude success")
        if "registry entry" not in passthrough["meaning_source"]:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "passthrough meaning must come from the registry")
        if "digest" not in passthrough["receiver_precondition"]:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "passthrough requires digest verification")
        if "MUST NOT appear" not in system_tm["success_codes"]["rule"]:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "success codes must stay out of error slots")
        if "fail closed" not in system_tm["unknown_code"]["rule"]:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "unknown-code rule must fail closed")
        if system_tm["late_result"]["rule"] != \
                "audit-only; MUST NOT create, replace or rewrite any terminal decision":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "system late_result rule drift")
        if system_tm["message"]["rule"] != \
                "free-text message is diagnostic only and MUST NOT drive status, retry or permission behavior":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "system message rule drift")
        system_whitelist = system_tm["infra_same_request_whitelist"]
        system_same_request = [
            n for n in self.system.names()
            if self.system.entry(n).retry_scope == "same_request"]
        if sorted(system_whitelist) != sorted(system_same_request):
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "system infra whitelist drift")

        host_tm = self.host.document["transport_mapping"]
        _require_keys(host_tm,
                      {"mode", "precedence", "shared_codes_emitted_by_host",
                       "unknown_upstream_replacement", "stale_cas",
                       "late_result", "semantic_failure_retry",
                       "infra_same_request_whitelist", "wire_status_values"},
                      "host transport_mapping")
        if host_tm["mode"] != "host-proxy-precedence":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "host transport mode %r" % host_tm["mode"])
        rules = host_tm["precedence"]
        if not isinstance(rules, list) or len(rules) != 4:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "precedence must list exactly 4 total rules")
        expected_rules = ("host-local-validation", "upstream-known-passthrough",
                          "unknown-upstream-replaced", "late-result-audit-only")
        if tuple(rule.get("rule") for rule in rules) != expected_rules:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "precedence rule order drift: %s" % (rules,))
        for index, rule in enumerate(rules):
            if rule.get("order") != index + 1 or not rule.get("description"):
                raise PolicyError("POLICY_PRECEDENCE_INVALID",
                                  "precedence rule %d malformed" % (index + 1))
        shared = host_tm["shared_codes_emitted_by_host"]
        _require_keys(shared, {"codes", "owner", "meaning_source"},
                      "shared_codes_emitted_by_host")
        if shared["codes"] != list(HOST_SHARED_EMIT_EXPECTED):
            raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                              "shared emit codes %s" % (shared["codes"],))
        if shared["owner"] != SYSTEM_REGISTRY_OWNER:
            raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                              "shared codes stay owned by the system registry")
        unknown_shared = [n for n in shared["codes"]
                          if self.system.entry(n) is None]
        if unknown_shared:
            raise PolicyError("POLICY_SPEC_ENUM_DRIFT",
                              "shared emit codes unknown to system registry: %s"
                              % unknown_shared)
        replacement = host_tm["unknown_upstream_replacement"]
        _require_keys(replacement,
                      {"replacement_code", "ctr004_alias",
                       "original_code_handling", "message_driven_inference"},
                      "unknown_upstream_replacement")
        if replacement["replacement_code"] != UNKNOWN_UPSTREAM_REPLACEMENT:
            raise PolicyError(
                "POLICY_PRECEDENCE_INVALID",
                "unknown upstream replacement %r"
                % replacement["replacement_code"])
        if replacement["ctr004_alias"] != UNKNOWN_UPSTREAM_ALIAS:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "CTR-004 alias drift")
        if replacement["original_code_handling"] != \
                "diagnostic_metadata_only_non_behavioral":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "original code must be non-behavioral metadata")
        if replacement["message_driven_inference"] != "forbidden":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "message-driven inference must be forbidden")
        stale = host_tm["stale_cas"]
        _require_keys(stale, {"codes_derived_from", "same_request_retry",
                              "recovery"}, "stale_cas")
        if stale["same_request_retry"] != "forbidden":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "stale CAS same-request retry must be forbidden")
        if stale["codes_derived_from"] != \
                "system registry retry_scope=new_attempt":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "stale CAS set must derive from the system registry")
        late = host_tm["late_result"]
        _require_keys(late, {"mode", "may_rewrite_terminal", "violation_code"},
                      "late_result")
        if late["mode"] != "audit_only" or late["may_rewrite_terminal"] is not False:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "late result must be audit-only")
        if late["violation_code"] != "LATE_OUTPUT_REWRITE":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "late-result violation code drift")
        semantic = host_tm["semantic_failure_retry"]
        _require_keys(semantic, {"same_request_retry", "new_attempt_rejudge",
                                 "note"}, "semantic_failure_retry")
        if semantic["same_request_retry"] != "forbidden":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "semantic same-request retry must be forbidden")
        if semantic["new_attempt_rejudge"] != "forbidden for the original request":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "semantic new-attempt rejudge must be forbidden")
        if host_tm["wire_status_values"] != WIRE_STATUS_VALUES:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "host wire_status_values drift")
        host_whitelist = host_tm["infra_same_request_whitelist"]
        host_same_request = [
            n for n in self.host.names()
            if self.host.entry(n).retry_scope == "same_request"]
        if sorted(host_whitelist) != sorted(host_same_request):
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "host infra whitelist drift")

    # -- GMS 11.3 canonical policy body derivation ------------------------------

    def derive_gms_policy_body(self) -> dict:
        names = self.system.names()
        failure_codes = [
            n for n in names
            if self.system.entry(n).group in GMS_FAILURE_GROUPS]
        truncation_codes = [
            n for n in names
            if self.system.entry(n).group == GMS_TRUNCATION_GROUP]
        return {
            "schema_version": "gms.reason-policy.v1",
            "policy_id": "gms.reason-codes",
            "version": 1,
            "failure_codes": failure_codes,
            "truncation_codes": truncation_codes,
            "retry_same_request_codes": [
                n for n in failure_codes
                if self.system.entry(n).retry_scope == "same_request"],
            "new_attempt_required_codes": [
                n for n in failure_codes
                if self.system.entry(n).retry_scope == "new_attempt"],
            "inconclusive_status_codes": [
                n for n in failure_codes
                if self.system.entry(n).status == "inconclusive"],
        }

    def _check_gms_policy_body(self) -> None:
        body = self.derive_gms_policy_body()
        derived = digest_of(body)
        if derived != GMS_POLICY_BODY_DIGEST:
            raise PolicyError(
                "POLICY_SPEC_ENUM_DRIFT",
                "derived gms.reason-codes.v1 body digest %s != frozen %s"
                % (derived, GMS_POLICY_BODY_DIGEST))


def _require_keys(mapping: dict, expected: set, where: str) -> None:
    keys = set(mapping.keys())
    if keys != expected:
        raise PolicyError("POLICY_PRECEDENCE_INVALID",
                          "%s keys missing=%s extra=%s"
                          % (where, sorted(expected - keys),
                             sorted(keys - expected)))


def _load_policy_document(path: Path, source_name: str) -> dict:
    if not path.is_file():
        raise PolicyError("POLICY_FILE_MISSING", str(path))
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raise PolicyError("POLICY_BOM_NOT_ALLOWED", source_name)
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("POLICY_JSON_INVALID",
                          "%s: %s" % (source_name, exc))
    _reject_floats(document, source_name)
    return document


def _reject_floats(value, where: str) -> None:
    if isinstance(value, float):
        raise PolicyError("POLICY_NON_INTEGER_NUMBER", where)
    if isinstance(value, dict):
        for key, item in value.items():
            _reject_floats(key, where)
            _reject_floats(item, where)
    elif isinstance(value, list):
        for item in value:
            _reject_floats(item, where)


# ---------------------------------------------------------------------------
# Transport layer (registry entries + frozen mapping only; never messages)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReasonDecision:
    wire_reason_code: "str | None"
    registry_status: "str | None"
    wire_status: "str | None"
    retryable: bool
    retry_scope: str
    terminal: bool
    passthrough: bool
    origin: str
    upstream_code_as_metadata: "str | None"
    same_request_retry_permitted: bool
    new_attempt_required: bool
    may_rewrite_terminal: bool
    audit_only: bool = False

    def as_dict(self) -> dict:
        return {
            "wire_reason_code": self.wire_reason_code,
            "registry_status": self.registry_status,
            "wire_status": self.wire_status,
            "retryable": self.retryable,
            "retry_scope": self.retry_scope,
            "terminal": self.terminal,
            "passthrough": self.passthrough,
            "origin": self.origin,
            "upstream_code_as_metadata": self.upstream_code_as_metadata,
            "same_request_retry_permitted": self.same_request_retry_permitted,
            "new_attempt_required": self.new_attempt_required,
            "may_rewrite_terminal": self.may_rewrite_terminal,
            "audit_only": self.audit_only,
        }


class TransportResolver:
    """Total precedence + status/retry/new-attempt projection."""

    ORIGINS = ("host-local-validation", "upstream-passthrough",
               "unknown-upstream-replaced", "late-audit-only")

    def __init__(self, bundle: ReasonPolicyBundle):
        self.bundle = bundle
        self._shared_emit = frozenset(bundle.host_shared_emit_codes())

    # -- core resolution -------------------------------------------------------

    def resolve(self, *, host_code=None, upstream_code=None,
                late_upstream=False, message=None) -> ReasonDecision:
        """Resolve one failure observation.

        ``message`` is free text accepted ONLY so it can be demonstrably
        ignored: it must never change any field of the decision.
        """
        del message  # diagnostic only; structurally non-behavioral
        if host_code is not None:
            return self._resolve_host(host_code, upstream_code)
        if upstream_code is not None:
            return self._resolve_upstream(upstream_code)
        if late_upstream:
            return self._resolve_late()
        raise PolicyError("POLICY_PRECEDENCE_INVALID",
                          "resolve requires exactly one failure observation")

    def _decision_for(self, entry: ReasonCode, *, origin: str,
                      passthrough: bool,
                      metadata: "str | None") -> ReasonDecision:
        return ReasonDecision(
            wire_reason_code=entry.name,
            registry_status=entry.status,
            wire_status=WIRE_STATUS_VALUES[entry.status],
            retryable=entry.retryable,
            retry_scope=entry.retry_scope,
            terminal=entry.terminal,
            passthrough=passthrough,
            origin=origin,
            upstream_code_as_metadata=metadata,
            same_request_retry_permitted=entry.retry_scope == "same_request",
            new_attempt_required=entry.retry_scope == "new_attempt",
            may_rewrite_terminal=False,
        )

    def _resolve_host(self, host_code: str,
                      upstream_code) -> ReasonDecision:
        entry = self.bundle.host.entry(host_code)
        origin = "host-local-validation"
        if entry is None:
            if host_code in self._shared_emit:
                entry = self.bundle.system.entry(host_code)
            else:
                raise PolicyError(
                    "POLICY_PRECEDENCE_INVALID",
                    "Host may only emit Host-owned or shared codes, not %r"
                    % host_code)
        if entry.status == "success":
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "%s is success metadata and MUST NOT enter the "
                              "error slot" % host_code)
        # Precedence 1: the Host-local validation failure wins; a
        # simultaneously observed upstream code is demoted to metadata.
        return self._decision_for(entry, origin=origin, passthrough=False,
                                  metadata=upstream_code)

    def _resolve_upstream(self, upstream_code: str) -> ReasonDecision:
        entry = self.bundle.system.entry(upstream_code)
        if entry is not None:
            if entry.status == "success":
                raise PolicyError(
                    "POLICY_PRECEDENCE_INVALID",
                    "%s is success/truncation metadata and MUST NOT appear in "
                    "any error.reason_code slot" % upstream_code)
            # Precedence 2: known upstream code, passthrough verbatim.
            return self._decision_for(entry, origin="upstream-passthrough",
                                      passthrough=True, metadata=None)
        # Precedence 3: unknown upstream code -> closed replacement.
        replacement = self.bundle.host.entry(UNKNOWN_UPSTREAM_REPLACEMENT)
        return self._decision_for(
            replacement, origin="unknown-upstream-replaced",
            passthrough=False, metadata=upstream_code)

    def _resolve_late(self) -> ReasonDecision:
        # Precedence 4 (lowest): audit-only, produces and rewrites nothing.
        return ReasonDecision(
            wire_reason_code=None,
            registry_status=None,
            wire_status=None,
            retryable=False,
            retry_scope="none",
            terminal=True,
            passthrough=False,
            origin="late-audit-only",
            upstream_code_as_metadata=None,
            same_request_retry_permitted=False,
            new_attempt_required=False,
            may_rewrite_terminal=False,
            audit_only=True,
        )

    # -- projections -------------------------------------------------------------

    def three_end_view(self, decision: ReasonDecision) -> dict:
        view = {
            "status": decision.registry_status,
            "wire_status": decision.wire_status,
            "retryable": decision.retryable,
            "retry_scope": decision.retry_scope,
            "same_request_retry_permitted":
                decision.same_request_retry_permitted,
            "new_attempt_required": decision.new_attempt_required,
        }
        return {"gms": view, "host": dict(view), "rsih": dict(view)}

    def same_request_retry_is_legal(self, decision: ReasonDecision) -> bool:
        return decision.same_request_retry_permitted

    def assert_same_request_retry_legal(self,
                                        decision: ReasonDecision) -> None:
        if decision.retry_scope == "new_attempt":
            raise PolicyError(
                "POLICY_RETRY_ILLEGAL",
                "%s requires a new attempt; same-request retry is forbidden"
                % decision.wire_reason_code)
        if not decision.same_request_retry_permitted:
            raise PolicyError(
                "POLICY_RETRY_ILLEGAL",
                "%s is terminal for the same request (retry_scope=%s); "
                "retrying it into pass is forbidden"
                % (decision.wire_reason_code, decision.retry_scope))

    def apply_late_result(self, prior: ReasonDecision,
                          late_upstream: bool = True) -> ReasonDecision:
        """A late upstream result merges as audit only: prior unchanged."""
        if not late_upstream:
            return prior
        late = self._resolve_late()
        if not late.audit_only or late.may_rewrite_terminal:
            raise PolicyError("POLICY_PRECEDENCE_INVALID",
                              "late result must stay audit-only")
        return prior


# ---------------------------------------------------------------------------
# Fixture layer ($FIX/reasons)
# ---------------------------------------------------------------------------


@dataclass
class FixtureCaseResult:
    case_id: str
    ok: bool
    message: str
    detail: str = "-"


@dataclass
class ReasonReport:
    errors: "list[str]" = field(default_factory=list)
    results: "list[FixtureCaseResult]" = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors and all(r.ok for r in self.results)

    def render(self) -> str:
        lines = ["ERROR: %s" % e for e in self.errors]
        for result in self.results:
            if result.ok:
                lines.append("PASS %s (%s)" % (result.case_id, result.detail))
            else:
                lines.append("FAIL %s: %s" % (result.case_id, result.message))
        failed = [r for r in self.results if not r.ok]
        lines.append(
            "SUMMARY: cases=%d passed=%d failed=%d corpus_errors=%d"
            % (len(self.results), len(self.results) - len(failed),
               len(failed), len(self.errors)))
        return "\n".join(lines)


def _read_fixture_json(path: Path, report: ReasonReport):
    try:
        raw = path.read_bytes()
    except OSError as exc:
        report.errors.append("%s unreadable: %s" % (path, exc))
        return None
    if raw.startswith(b"\xef\xbb\xbf"):
        report.errors.append("%s: BOM not allowed" % path)
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        report.errors.append("%s: invalid JSON: %s" % (path, exc))
        return None


def _load_reasons_manifest(reasons_dir: Path,
                           report: ReasonReport) -> "list[dict] | None":
    manifest = _read_fixture_json(reasons_dir / "manifest.json", report)
    if manifest is None:
        return None
    if not isinstance(manifest, dict) \
            or manifest.get("schema_version") != REASON_FIXTURES_SCHEMA_VERSION:
        report.errors.append("reasons manifest schema_version invalid")
        return None
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        report.errors.append("reasons manifest has no cases")
        return None
    seen = set()
    for case in cases:
        if not isinstance(case, dict):
            report.errors.append("reasons manifest case is not an object")
            return None
        case_id = case.get("case_id")
        if not isinstance(case_id, str) or not case_id:
            report.errors.append("reasons manifest case_id invalid")
            return None
        if case_id in seen:
            report.errors.append("duplicate case_id %s" % case_id)
            return None
        seen.add(case_id)
        if case.get("kind") not in FIXTURE_KINDS:
            report.errors.append(
                "%s: unknown fixture kind %r" % (case_id, case.get("kind")))
            return None
        for key in ("input_path", "expected_path"):
            value = case.get(key)
            if not isinstance(value, str) or not (reasons_dir / value).is_file():
                report.errors.append("%s: %s missing (%r)"
                                     % (case_id, key, value))
                return None
    return cases


def _observation_to_kwargs(observation: dict) -> dict:
    allowed = {"host_code", "upstream_code", "late_upstream", "message"}
    unknown = set(observation.keys()) - allowed
    if unknown:
        raise PolicyError("FIXTURE_INPUT_INVALID",
                          "observation keys %s" % sorted(unknown))
    kwargs = dict(observation)
    if "late_upstream" in kwargs and not isinstance(kwargs["late_upstream"],
                                                    bool):
        raise PolicyError("FIXTURE_INPUT_INVALID", "late_upstream must be bool")
    return kwargs


def evaluate_resolution_case(bundle: ReasonPolicyBundle, case: dict,
                             reasons_dir: Path) -> FixtureCaseResult:
    case_id = case["case_id"]
    input_doc = json.loads(
        (reasons_dir / case["input_path"]).read_bytes().decode("utf-8"))
    expected = json.loads(
        (reasons_dir / case["expected_path"]).read_bytes().decode("utf-8"))
    if expected.get("case_id") != case_id:
        return FixtureCaseResult(case_id, False, "expected case_id mismatch")
    observation = input_doc.get("observation")
    if not isinstance(observation, dict):
        return FixtureCaseResult(case_id, False, "input lacks observation")
    if observation.get("late_upstream"):
        prior_observation = input_doc.get("prior_observation")
        if not isinstance(prior_observation, dict):
            return FixtureCaseResult(case_id, False,
                                     "late case lacks prior_observation")
        prior = bundle.resolver.resolve(
            **_observation_to_kwargs(prior_observation))
        decision = bundle.resolver.apply_late_result(prior, late_upstream=True)
    else:
        decision = bundle.resolver.resolve(
            **_observation_to_kwargs(observation))
    expected_decision = expected.get("expected_decision")
    if expected_decision != decision.as_dict():
        return FixtureCaseResult(
            case_id, False,
            "decision drift: derived %s != expected %s"
            % (decision.as_dict(), expected_decision))
    if expected.get("three_end_consistent"):
        views = bundle.resolver.three_end_view(decision)
        if not (views["gms"] == views["host"] == views["rsih"]):
            return FixtureCaseResult(case_id, False,
                                     "three-end views disagree")
    return FixtureCaseResult(case_id, True, "ok",
                             decision.wire_reason_code or "-")


def _apply_mutation(bundle_pristine: "tuple[dict, dict]",
                    mutation: dict) -> "tuple[dict, dict]":
    system_doc = json.loads(json.dumps(bundle_pristine[0]))
    host_doc = json.loads(json.dumps(bundle_pristine[1]))
    registry = mutation.get("registry")
    docs = {"system": system_doc, "host-proxy": host_doc}
    target = docs.get(registry)
    if target is None:
        raise PolicyError("FIXTURE_INPUT_INVALID",
                          "mutation registry %r" % registry)
    kind = mutation.get("kind")

    def _find(name):
        for code in target["codes"]:
            if code["name"] == name:
                return code
        raise PolicyError("FIXTURE_INPUT_INVALID",
                          "mutation code %s not found" % name)

    resign = True
    if kind == "add-owned-code":
        code = mutation.get("code")
        if not isinstance(code, dict):
            raise PolicyError("FIXTURE_INPUT_INVALID", "mutation lacks code")
        target["codes"].append(json.loads(json.dumps(code)))
    elif kind == "duplicate-code":
        cloned = json.loads(json.dumps(_find(mutation["name"])))
        target["codes"].append(cloned)
    elif kind == "remove-code-field":
        del _find(mutation["name"])[mutation["field"]]
    elif kind == "tamper-code-field":
        _find(mutation["name"])[mutation["field"]] = mutation["value"]
    elif kind == "remove-code":
        target["codes"] = [c for c in target["codes"]
                           if c["name"] != mutation["name"]]
    elif kind == "tamper-without-resign":
        _find(mutation["name"])["notes"] = mutation.get(
            "value", "tampered note")
        resign = False
    else:
        raise PolicyError("FIXTURE_INPUT_INVALID",
                          "mutation kind %r" % kind)
    if resign:
        for doc in docs.values():
            body = {k: v for k, v in doc.items() if k != "registry_digest"}
            doc["registry_digest"] = digest_of(body)
    return system_doc, host_doc


def evaluate_policy_reject_case(bundle: ReasonPolicyBundle, case: dict,
                                reasons_dir: Path) -> FixtureCaseResult:
    case_id = case["case_id"]
    input_doc = json.loads(
        (reasons_dir / case["input_path"]).read_bytes().decode("utf-8"))
    expected = json.loads(
        (reasons_dir / case["expected_path"]).read_bytes().decode("utf-8"))
    if expected.get("case_id") != case_id:
        return FixtureCaseResult(case_id, False, "expected case_id mismatch")
    if expected.get("expected_reject") is not True:
        return FixtureCaseResult(case_id, False,
                                 "policy-reject case must expect rejection")
    mutation = input_doc.get("mutation")
    if not isinstance(mutation, dict):
        return FixtureCaseResult(case_id, False, "input lacks mutation")
    pristine = (bundle.system.document, bundle.host.document)
    system_doc, host_doc = _apply_mutation(pristine, mutation)
    try:
        ReasonPolicyBundle.from_documents(system_doc, host_doc)
    except PolicyError as exc:
        if exc.reason_code == expected.get("expected_reason_code"):
            return FixtureCaseResult(case_id, True, "rejected as expected",
                                     exc.reason_code)
        return FixtureCaseResult(
            case_id, False,
            "rejected with %s, expected %s"
            % (exc.reason_code, expected.get("expected_reason_code")))
    return FixtureCaseResult(
        case_id, False,
        "mutation was NOT rejected (expected %s)"
        % expected.get("expected_reason_code"))


def evaluate_message_driven_case(bundle: ReasonPolicyBundle, case: dict,
                                 reasons_dir: Path) -> FixtureCaseResult:
    case_id = case["case_id"]
    input_doc = json.loads(
        (reasons_dir / case["input_path"]).read_bytes().decode("utf-8"))
    expected = json.loads(
        (reasons_dir / case["expected_path"]).read_bytes().decode("utf-8"))
    if expected.get("case_id") != case_id:
        return FixtureCaseResult(case_id, False, "expected case_id mismatch")
    if expected.get("expected_reject") is not True:
        return FixtureCaseResult(case_id, False,
                                 "message-driven case must expect rejection")
    observation = input_doc.get("observation")
    claimed = input_doc.get("claimed_fallback_code")
    if not isinstance(observation, dict) or not isinstance(claimed, str):
        return FixtureCaseResult(case_id, False, "malformed input")
    kwargs = _observation_to_kwargs(observation)
    with_message = bundle.resolver.resolve(**kwargs)
    without_message = bundle.resolver.resolve(
        **{k: v for k, v in kwargs.items() if k != "message"})
    reason = expected.get("expected_reason_code")
    if with_message != without_message:
        return FixtureCaseResult(case_id, False,
                                 "message changed the resolution")
    if with_message.wire_reason_code == claimed:
        return FixtureCaseResult(
            case_id, False,
            "message-driven fallback to %s was honored" % claimed)
    if reason != "FIXTURE_MESSAGE_DRIVEN_FORBIDDEN":
        return FixtureCaseResult(case_id, False,
                                 "unexpected expected_reason_code %r" % reason)
    return FixtureCaseResult(case_id, True, "message-driven fallback rejected",
                             with_message.wire_reason_code)


def evaluate_derivation_case(bundle: ReasonPolicyBundle, case: dict,
                             reasons_dir: Path) -> FixtureCaseResult:
    case_id = case["case_id"]
    expected = json.loads(
        (reasons_dir / case["expected_path"]).read_bytes().decode("utf-8"))
    if expected.get("case_id") != case_id:
        return FixtureCaseResult(case_id, False, "expected case_id mismatch")
    body = bundle.derive_gms_policy_body()
    derived_digest = digest_of(body)
    checks = (
        ("expected_failure_code_count",
         len(body["failure_codes"]),
         expected.get("expected_failure_code_count")),
        ("expected_truncation_code_count",
         len(body["truncation_codes"]),
         expected.get("expected_truncation_code_count")),
        ("expected_body_digest", derived_digest,
         expected.get("expected_body_digest")),
    )
    for name, derived, want in checks:
        if derived != want:
            return FixtureCaseResult(
                case_id, False, "%s: derived %s != expected %s"
                % (name, derived, want))
    if derived_digest != GMS_POLICY_BODY_DIGEST:
        return FixtureCaseResult(case_id, False,
                                 "derived digest != frozen validator constant")
    return FixtureCaseResult(case_id, True, "ok", derived_digest)


_CASE_EVALUATORS = {
    "resolution": evaluate_resolution_case,
    "policy-reject": evaluate_policy_reject_case,
    "message-driven-attempt": evaluate_message_driven_case,
    "derivation": evaluate_derivation_case,
}


def evaluate_fixture_case(bundle: ReasonPolicyBundle, reasons_dir: Path,
                          case_id: str) -> FixtureCaseResult:
    """Evaluate a single manifest case (test helper)."""
    manifest = json.loads(
        (Path(reasons_dir) / "manifest.json").read_bytes().decode("utf-8"))
    for case in manifest["cases"]:
        if case["case_id"] == case_id:
            return _CASE_EVALUATORS[case["kind"]](bundle, case,
                                                  Path(reasons_dir))
    raise KeyError(case_id)


def validate_reasons(bundle: ReasonPolicyBundle,
                     reasons_dir: Path) -> ReasonReport:
    report = ReasonReport()
    cases = _load_reasons_manifest(Path(reasons_dir), report)
    if cases is None:
        return report
    for case in cases:
        try:
            report.results.append(
                _CASE_EVALUATORS[case["kind"]](bundle, case,
                                               Path(reasons_dir)))
        except PolicyError as exc:
            report.results.append(
                FixtureCaseResult(case["case_id"], False,
                                  "%s: %s" % (exc.reason_code, exc.detail)))
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv=None) -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Validate the CTR-004 system reason-code registry policy.")
    parser.add_argument("--policy-dir", type=Path,
                        default=here / "policy",
                        help="directory holding the two policy JSONs")
    parser.add_argument("--reasons-dir", type=Path,
                        default=here / "reasons",
                        help="directory holding the reasons fixture corpus")
    args = parser.parse_args(argv)

    try:
        bundle = ReasonPolicyBundle.load(args.policy_dir)
    except PolicyError as exc:
        print("ERROR: %s: %s" % (exc.reason_code, exc.detail))
        print("SUMMARY: policy_invalid=1")
        return 2

    print("PASS system-reason-codes.v1 codes=%d digest=%s"
          % (len(bundle.system.names()), bundle.system.registry_digest))
    print("PASS host-proxy-reason-codes.v1 codes=%d digest=%s"
          % (len(bundle.host.names()), bundle.host.registry_digest))
    print("PASS gms.reason-codes.v1 body digest=%s"
          % digest_of(bundle.derive_gms_policy_body()))

    report = validate_reasons(bundle, args.reasons_dir)
    output = report.render()
    if output:
        print(output)
    if report.errors:
        return 2
    if not report.ok:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
