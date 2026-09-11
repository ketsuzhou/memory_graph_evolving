#!/usr/bin/env python3
"""CTR-005 machine-readable contract validator (Python stdlib only).

Freezes the whole System Contract as closed machine-readable artifacts
(Contract 16.6, revision v1.1, CTR-005) and checks four families:

1. Schema self-closure (``validate_schema_document``): every hand-written
   schema under ``schema/shared`` is a closed JSON-Schema subset document -
   ``additionalProperties: false`` at every shape-defining position, no
   ``type: number``, integer-only bounds and enums, explicit enums, only
   the frozen keyword subset, resolvable non-cyclic ``$ref`` links (file
   local ``#/...`` and sibling relative refs, including the frozen CTR-001
   / CTR-003 documents), and structurally sound ``x-digest`` preimage
   declarations.
2. State transition totality (``validate_state_document`` /
   ``check_state_machine_totality``): the bundles under ``schema/state``
   (proposal 9.1, candidate 9.2, merge 9.4 M1-M9, composite 9.5, segment
   Host 3.3, projector 9.3) declare every state exactly once, every
   transition references a defined state, ``legal_exits`` equals the
   declared outgoing edges, terminal states have no outgoing edges
   (TERMINAL_STATE_REOPEN is impossible), every non-terminal state has a
   legal exit, and every state is reachable from an initial state.
3. Profile executability (``validate_profile_document``): the four
   profiles under ``policy/profiles`` carry explicit units from the closed
   unit registry, integer min/max/default bounds with min <= default <=
   max and no negative/float limits, permission entries with mandatory
   scopes over a closed capability set, runtime capabilities bound only to
   injected/forbidden (never wall-clock/ambient/real), and a complete,
   recomputable ``digest_preimage`` -> ``profile_digest`` pair.
4. Drift check (``check_policy_drift`` / ``validate_schema_cases``): the
   frozen CTR-002 / CTR-004 policy digests (tool-success-validation,
   system-reason-codes, host-proxy-reason-codes) recompute to their
   declared and Contract-frozen values using each policy's own digest
   declaration mechanism (JCS of the document minus its digest field),
   and the ``schema-cases`` corpus derives the expected accept/reject
   reason for every positive/negative case.

``--emit-provenance`` prints a provenance manifest (SHA-256 of every
schema/policy file) to stdout after the marker line. The manifest is a
derived artifact; the hand-written schemas under ``schema/`` and
``policy/`` are the source of truth.

Validator-internal failure vocabulary (closed set; these are conformance
meta-codes, NOT members of the 13.7.1 runtime reason registries and are
never emitted into a runtime ``reason_code`` slot):

    SCHEMA_FILE_INVALID_JSON  SCHEMA_KEYWORD_UNKNOWN   SCHEMA_NOT_CLOSED
    SCHEMA_LIMIT_NON_INTEGER  SCHEMA_REF_UNRESOLVED    SCHEMA_REF_CYCLE
    SCHEMA_DIGEST_META_INVALID
    SCHEMA_TYPE_INVALID       SCHEMA_REQUIRED_FIELD_MISSING
    SCHEMA_FIELD_UNKNOWN      SCHEMA_ENUM_INVALID      SCHEMA_CONST_MISMATCH
    SCHEMA_PATTERN_INVALID    SCHEMA_MINIMUM_VIOLATED  SCHEMA_MINITEMS_VIOLATED
    NON_INTEGER_NUMBER        UNKNOWN_REQUIRED_EXTENSION
    SCHEMA_CONDITIONAL_REQUIRED_MISSING
    SCHEMA_CONDITIONAL_FORBIDDEN_PRESENT
    DIGEST_MISMATCH           DIGEST_PREIMAGE_INCOMPLETE
    STATE_BUNDLE_INVALID      UNDEFINED_STATE
    ILLEGAL_STATE_TRANSITION  TERMINAL_STATE_REOPEN
    PROFILE_DOC_INVALID       PROFILE_LIMIT_INVALID    PROFILE_UNIT_UNDEFINED
    PROFILE_DIGEST_MISMATCH   PERMISSION_SCOPE_MISSING
    RUNTIME_NONDETERMINISM_CAPABILITY
    POLICY_DIGEST_MISMATCH    FIXTURE_EXPECTATION_MISMATCH

CLI
---
    python3 validate_contract.py --root <FIX root> [--emit-provenance]

Exit codes: 0 all checks and fixture cases pass; 1 fixture case or
expectation mismatch; 2 structural/corpus integrity error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

CONTRACT_SCHEMA_VERSION = "rsih-skill-evolution.system-contract.v1"

STATE_BUNDLE_SCHEMA_VERSION = "rsih-skill-evolution.state-machine-bundle.v1"
SCHEMA_CASES_SCHEMA_VERSION = "rsih-skill-evolution.schema-cases.v1"
SCHEMA_CASE_EXPECTED_VERSION = \
    "rsih-skill-evolution.schema-case-expected.v1"
STATE_EVENTS_VERSION = "rsih-skill-evolution.state-events.v1"
PROVENANCE_VERSION = "rsih-skill-evolution.contract-provenance.v1"

# Closed unit registry (CTR-005): every numeric profile limit MUST use one.
UNIT_REGISTRY = frozenset(
    {"tokens", "bytes", "count", "ms", "units", "micros", "depth"}
)

# Frozen policy digests (Contract 12.7.1 M1 / 13.7.1 R2); drift check.
FROZEN_POLICIES = (
    ("policy/tool-success-validation.v1.json", "policy_digest",
     "sha256:dde91eff2aaac31057512beba1c667e0b07b7cb4c36037db4e07eaced6f702f5"),
    ("policy/system-reason-codes.v1.json", "registry_digest",
     "sha256:16410afa27498bb425885d4629c65309389f15adeb975f97f98674170ad00a2c"),
    ("policy/host-proxy-reason-codes.v1.json", "registry_digest",
     "sha256:e48f27252bff3fd34868ef4bc5b56a678cf2a35d59f4cd7f2c71f78290485f5e"),
)

# Per-profile-kind closed key sets and mandatory digest-preimage groups.
PROFILE_KINDS = {
    "render-profile": {
        "schema_version": "rsih-skill-evolution.render-profile.v1",
        "keys": frozenset({"schema_version", "profile_id",
                           "profile_version", "contract_refs", "renderers",
                           "limits", "default_materialization",
                           "digest_preimage", "profile_digest"}),
        "preimage_groups": ("renderers", "limits",
                            "default_materialization"),
    },
    "resource-profile": {
        "schema_version": "rsih-skill-evolution.resource-profile.v1",
        "keys": frozenset({"schema_version", "profile_id",
                           "profile_version", "contract_refs", "limits",
                           "default_materialization", "digest_preimage",
                           "profile_digest"}),
        "preimage_groups": ("limits", "default_materialization"),
    },
    "permission-profile": {
        "schema_version": "rsih-skill-evolution.permission-profile.v1",
        "keys": frozenset({"schema_version", "profile_id",
                           "profile_version", "contract_refs",
                           "authority_cap", "capabilities_closed_set",
                           "permissions", "default_materialization",
                           "digest_preimage", "profile_digest"}),
        "preimage_groups": ("authority_cap", "capabilities_closed_set",
                            "permissions", "default_materialization"),
    },
    "runtime-profile": {
        "schema_version": "rsih-skill-evolution.runtime-profile.v1",
        "keys": frozenset({"schema_version", "profile_id",
                           "profile_version", "contract_refs", "mode",
                           "capabilities", "determinism", "limits",
                           "default_materialization", "digest_preimage",
                           "profile_digest"}),
        "preimage_groups": ("mode", "capabilities", "determinism",
                            "limits", "default_materialization"),
    },
}

CANONICAL_KINDS = ("human_procedure", "step_guidance", "composite")

# Frozen JSON-Schema subset accepted in hand-written documents.
SCHEMA_KEYWORDS = frozenset({
    "$ref", "$defs", "type", "enum", "const", "properties", "required",
    "additionalProperties", "items", "minItems", "maxItems", "minimum",
    "maximum", "minLength", "pattern", "allOf", "if", "then", "else",
    "not", "description", "title",
})
SCHEMA_META_KEYWORDS = frozenset({
    "$schema", "$id", "title", "description", "contract_refs", "x-digest",
})
ALLOWED_TYPES = frozenset({
    "object", "array", "string", "integer", "boolean", "null",
})

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

# State dialect closed key sets.
BUNDLE_KEYS = frozenset({"schema_version", "bundle_id", "title",
                         "contract_ref", "machines"})
MACHINE_KEYS = frozenset({"machine_id", "entity", "contract_ref",
                          "initial_states", "states", "transitions"})
STATE_KEYS = frozenset({"name", "terminal", "entry_condition",
                        "legal_exits"})
TRANSITION_KEYS = frozenset({"from", "to", "trigger", "authority"})
KNOWN_AUTHORITIES = frozenset({"host", "gms", "rsih"})

# schema-cases closed key sets.
CASE_MANIFEST_KEYS = frozenset({"schema_version",
                                "contract_schema_version", "revision",
                                "cases"})
CASE_KEYS = frozenset({"case_id", "category", "kind", "target",
                       "instance_path", "expected_path"})
CASE_CATEGORIES = frozenset({"dto", "state", "profile"})
CASE_KINDS = frozenset({"positive", "negative"})
EXPECTED_KEYS = frozenset({"schema_version", "case_id", "expected_accept",
                           "expected_reason_code", "notes"})
EVENTS_KEYS = frozenset({"schema_version", "case_id", "bundle", "machine",
                         "initial_state", "events"})


class ContractCheckError(Exception):
    """Closed-vocabulary failure (``code`` + human detail)."""

    def __init__(self, code: str, detail: str):
        super().__init__("%s: %s" % (code, detail))
        self.code = code
        self.detail = detail


# ---------------------------------------------------------------------------
# RFC 8785 JCS + SHA-256 (integer-only core; floats rejected)
# ---------------------------------------------------------------------------

_SHORT_ESCAPES = {
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
            out.append(ch)
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
        elif isinstance(node, int) and not isinstance(node, bool):
            parts.append(str(node))
        elif isinstance(node, float):
            raise ContractCheckError(
                "NON_INTEGER_NUMBER", "float %r in hashed core" % node)
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
            keys = sorted(node.keys(),
                          key=lambda k: k.encode("utf-16-be"))
            for index, key in enumerate(keys):
                if index:
                    parts.append(",")
                parts.append(_escape_string(key))
                parts.append(":")
                emit(node[key])
            parts.append("}")
        else:
            raise ContractCheckError(
                "SCHEMA_FILE_INVALID_JSON",
                "unsupported JSON value %r" % type(node))

    emit(value)
    return "".join(parts).encode("utf-8")


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def digest_of(value) -> str:
    return digest_bytes(jcs(value))


def _reject_floats(node, where: str):
    if isinstance(node, float):
        raise ContractCheckError(
            "SCHEMA_LIMIT_NON_INTEGER",
            "float %r in %s" % (node, where))
    if isinstance(node, dict):
        for key, value in node.items():
            _reject_floats(value, where)
    elif isinstance(node, list):
        for value in node:
            _reject_floats(value, where)


# ---------------------------------------------------------------------------
# Schema document loading / $ref resolution
# ---------------------------------------------------------------------------

_SCHEMA_CACHE: "dict[Path, dict]" = {}


def load_json(path: Path):
    """Load a JSON document WITHOUT float rejection.

    Floats must survive loading so each check family can attribute them
    to the precise root cause: NON_INTEGER_NUMBER for DTO instance
    cores (``_floats_in``), PROFILE_LIMIT_INVALID for profile limits
    (``check_profile_limits``). Schema documents reject floats at
    ``load_schema_file`` with SCHEMA_LIMIT_NON_INTEGER; digests reject
    them inside ``jcs`` itself.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ContractCheckError("SCHEMA_FILE_INVALID_JSON",
                                 "cannot read %s: %s" % (path, exc))
    if text.startswith("﻿"):
        raise ContractCheckError("SCHEMA_FILE_INVALID_JSON",
                                 "BOM in %s" % path)
    try:
        doc = json.loads(text)
    except ValueError as exc:
        raise ContractCheckError("SCHEMA_FILE_INVALID_JSON",
                                 "invalid JSON in %s: %s" % (path, exc))
    return doc


def load_schema_file(path: Path) -> dict:
    cached = _SCHEMA_CACHE.get(path)
    if cached is None:
        cached = load_json(path)
        if not isinstance(cached, dict):
            raise ContractCheckError("SCHEMA_FILE_INVALID_JSON",
                                     "%s is not a JSON object" % path)
        # Schema documents are integer-only at every position
        # (SCHEMA_LIMIT_NON_INTEGER); instance/profile files defer to
        # their family-specific reason codes.
        _reject_floats(cached, str(path))
        _SCHEMA_CACHE[path] = cached
    return cached


class RefResolver:
    """File-local ``#/...`` and sibling-relative ``$ref`` resolution."""

    def __init__(self, base: Path):
        self.base = base

    def resolve(self, ref: str):
        if not isinstance(ref, str) or not ref:
            raise ContractCheckError("SCHEMA_REF_UNRESOLVED",
                                     "bad $ref %r in %s" % (ref, self.base))
        if ref.startswith("#/"):
            doc = load_schema_file(self.base)
            node = doc
            for token in ref[2:].split("/"):
                token = token.replace("~1", "/").replace("~0", "~")
                if not isinstance(node, dict) or token not in node:
                    raise ContractCheckError(
                        "SCHEMA_REF_UNRESOLVED",
                        "%s: pointer %s missing token %r"
                        % (self.base, ref, token))
                node = node[token]
            return node, self.base
        if "#" in ref:
            file_part, pointer = ref.split("#", 1)
        else:
            file_part, pointer = ref, ""
        target = (self.base.parent / file_part).resolve()
        if not target.exists():
            raise ContractCheckError("SCHEMA_REF_UNRESOLVED",
                                     "%s: $ref target %s does not exist"
                                     % (self.base, ref))
        doc = load_schema_file(target)
        if pointer == "":
            # whole-document ref: meta keywords are not schema body
            node = {key: value for key, value in doc.items()
                    if key not in SCHEMA_META_KEYWORDS}
        else:
            node = doc
        if pointer:
            if not pointer.startswith("/"):
                raise ContractCheckError("SCHEMA_REF_UNRESOLVED",
                                         "%s: bad pointer in %r"
                                         % (self.base, ref))
            for token in pointer[1:].split("/"):
                token = token.replace("~1", "/").replace("~0", "~")
                if not isinstance(node, dict) or token not in node:
                    raise ContractCheckError(
                        "SCHEMA_REF_UNRESOLVED",
                        "%s: pointer %s missing token %r"
                        % (self.base, ref, token))
                node = node[token]
        return node, target


# ---------------------------------------------------------------------------
# Family 1: schema self-closure
# ---------------------------------------------------------------------------

def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_schema_node(node, path: Path, resolver: RefResolver,
                         closure_required: bool,
                         stack: "list[tuple[Path, str]]"):
    """Walk one schema node: keyword closure, integer bounds, $ref health.

    ``closure_required`` distinguishes shape-defining positions from
    applicator fragments. The closure spine - the document body, every
    ``properties``/``items`` subschema reached from the body or from a
    ``$defs`` entry, and every ``$ref`` target - MUST declare
    ``additionalProperties: false`` wherever it has ``properties``.
    Constraint overlays under ``allOf``/``if``/``then``/``else``/``not``
    are fragments: they address a SUBSET of already-closed spine
    properties (e.g. ``if.properties.target.properties.ref_type``), so
    demanding ``additionalProperties: false`` there would invert their
    JSON Schema meaning (an ``if`` fragment could never match, a
    ``then`` fragment would reject every real payload). Fragments and
    their nested ``properties``/``items`` therefore inherit
    ``closure_required=False``; fragments may only tighten, never
    define, field sets.
    """
    if node is False or node is True:
        return
    if not isinstance(node, dict):
        raise ContractCheckError("SCHEMA_KEYWORD_UNKNOWN",
                                 "%s: schema node is not an object: %r"
                                 % (path, node))
    unknown = set(node) - SCHEMA_KEYWORDS
    if unknown:
        raise ContractCheckError(
            "SCHEMA_KEYWORD_UNKNOWN",
            "%s: keyword(s) %s outside the frozen subset"
            % (path, sorted(unknown)))
    if "type" in node:
        types = node["type"]
        if isinstance(types, str):
            types = [types]
        for t in types:
            if t not in ALLOWED_TYPES:
                raise ContractCheckError(
                    "SCHEMA_NOT_CLOSED",
                    "%s: type %r outside integer-only closed subset"
                    % (path, t))
            if t == "number":
                raise ContractCheckError(
                    "SCHEMA_NOT_CLOSED",
                    "%s: type 'number' forbidden (integer-only cores)"
                    % path)
    if "enum" in node:
        if not isinstance(node["enum"], list) or not node["enum"]:
            raise ContractCheckError("SCHEMA_NOT_CLOSED",
                                     "%s: empty enum" % path)
        for value in node["enum"]:
            if isinstance(value, float):
                raise ContractCheckError(
                    "SCHEMA_LIMIT_NON_INTEGER",
                    "%s: float %r in enum" % (path, value))
    for bound in ("minimum", "maximum", "minItems", "maxItems",
                  "minLength"):
        if bound in node and not _is_int(node[bound]):
            raise ContractCheckError(
                "SCHEMA_LIMIT_NON_INTEGER",
                "%s: %s %r is not an integer" % (path, bound, node[bound]))
    if "required" in node:
        if not isinstance(node["required"], list):
            raise ContractCheckError("SCHEMA_NOT_CLOSED",
                                     "%s: required not a list" % path)
    if "additionalProperties" in node and node["additionalProperties"] \
            is not False:
        if node["additionalProperties"] is True:
            raise ContractCheckError(
                "SCHEMA_NOT_CLOSED",
                "%s: additionalProperties true is not closed" % path)
        raise ContractCheckError(
            "SCHEMA_NOT_CLOSED",
            "%s: additionalProperties must be an explicit false" % path)
    if closure_required and "properties" in node:
        if node.get("additionalProperties") is not False:
            raise ContractCheckError(
                "SCHEMA_NOT_CLOSED",
                "%s: object schema with properties but no explicit "
                "additionalProperties:false" % path)
        for name in node.get("required", []):
            if name not in node["properties"]:
                raise ContractCheckError(
                    "SCHEMA_NOT_CLOSED",
                    "%s: required field %r has no property schema"
                    % (path, name))
    if "$ref" in node:
        target_node, target_file = resolver.resolve(node["$ref"])
        key = (target_file, node["$ref"])
        if key in stack:
            raise ContractCheckError(
                "SCHEMA_REF_CYCLE",
                "%s: cyclic $ref %s (stack %r)"
                % (path, node["$ref"], [s[1] for s in stack]))
        sub = RefResolver(target_file)
        # $ref targets are named full shapes ($defs / whole documents),
        # never fragments: always demand closure of the target spine.
        validate_schema_node(target_node, target_file, sub, True,
                             stack + [key])
    for name, sub in sorted(node.get("properties", {}).items()):
        validate_schema_node(sub, path / name, resolver, closure_required,
                             stack)
    for defname, sub in sorted(node.get("$defs", {}).items()):
        validate_schema_node(sub, path / "$defs" / defname, resolver, True,
                             stack)
    if "items" in node:
        validate_schema_node(node["items"], path / "items", resolver,
                             closure_required, stack)
    for entry in node.get("allOf", []):
        validate_schema_node(entry, path / "allOf", resolver, False, stack)
    if "if" in node:
        validate_schema_node(node["if"], path / "if", resolver, False,
                             stack)
    if "then" in node:
        validate_schema_node(node["then"], path / "then", resolver, False,
                             stack)
    if "else" in node:
        validate_schema_node(node["else"], path / "else", resolver, False,
                             stack)
    if "not" in node:
        validate_schema_node(node["not"], path / "not", resolver, False,
                             stack)


def _validate_xdigest(doc: dict, path: Path):
    meta = doc.get("x-digest")
    if not isinstance(meta, dict):
        raise ContractCheckError("SCHEMA_DIGEST_META_INVALID",
                                 "%s: x-digest must be an object" % path)
    keys = set(meta)
    if not keys <= {"digest_field", "algorithm", "preimage_fields",
                    "preimage_rule"}:
        raise ContractCheckError(
            "SCHEMA_DIGEST_META_INVALID",
            "%s: x-digest unknown keys %s" % (path, sorted(keys)))
    if meta.get("algorithm") != "sha256-over-jcs":
        raise ContractCheckError(
            "SCHEMA_DIGEST_META_INVALID",
            "%s: x-digest algorithm must be sha256-over-jcs" % path)
    digest_field = meta.get("digest_field")
    preimage = meta.get("preimage_fields")
    if not isinstance(digest_field, str) or not isinstance(preimage, list):
        raise ContractCheckError("SCHEMA_DIGEST_META_INVALID",
                                 "%s: x-digest shape invalid" % path)
    props = _schema_properties(doc, path)
    if digest_field not in props:
        raise ContractCheckError(
            "SCHEMA_DIGEST_META_INVALID",
            "%s: x-digest digest_field %r is not a property"
            % (path, digest_field))
    if digest_field in preimage:
        raise ContractCheckError(
            "SCHEMA_DIGEST_META_INVALID",
            "%s: digest field %r in its own preimage"
            % (path, digest_field))
    for name in preimage:
        if name not in props:
            raise ContractCheckError(
                "SCHEMA_DIGEST_META_INVALID",
                "%s: preimage field %r is not a property" % (path, name))
    if len(set(preimage)) != len(preimage):
        raise ContractCheckError("SCHEMA_DIGEST_META_INVALID",
                                 "%s: duplicate preimage fields" % path)


def _schema_properties(doc: dict, path: Path) -> dict:
    node = doc
    if "$ref" in doc:
        resolver = RefResolver(path)
        node, _target = resolver.resolve(doc["$ref"])
    if not isinstance(node, dict) or "properties" not in node:
        raise ContractCheckError(
            "SCHEMA_DIGEST_META_INVALID",
            "%s: cannot locate properties for x-digest" % path)
    return node["properties"]


def declared_schema_version(doc: dict, path: Path):
    """The ``schema_version`` const/enum-of-one this schema pins, if any."""
    props = doc.get("properties", {})
    node = props.get("schema_version") if isinstance(props, dict) else None
    if node is None and "$ref" in doc:
        node = _schema_properties(doc, path).get("schema_version")
    if not isinstance(node, dict):
        return None
    if "const" in node:
        return node["const"]
    if isinstance(node.get("enum"), list) and len(node["enum"]) == 1:
        return node["enum"][0]
    return None


def validate_schema_document(doc, path: Path):
    """Family 1 entry: closure walk over a hand-written shared schema."""
    if not isinstance(doc, dict):
        raise ContractCheckError("SCHEMA_FILE_INVALID_JSON",
                                 "%s is not a JSON object" % path)
    unknown_meta = set(doc) - SCHEMA_KEYWORDS - SCHEMA_META_KEYWORDS
    if unknown_meta:
        raise ContractCheckError(
            "SCHEMA_KEYWORD_UNKNOWN",
            "%s: unknown top-level key(s) %s"
            % (path, sorted(unknown_meta)))
    if "contract_refs" in doc and (
            not isinstance(doc["contract_refs"], list)
            or not doc["contract_refs"]):
        raise ContractCheckError("SCHEMA_NOT_CLOSED",
                                 "%s: contract_refs must be a non-empty "
                                 "list" % path)
    resolver = RefResolver(path)
    body = {key: value for key, value in doc.items()
            if key not in SCHEMA_META_KEYWORDS}
    if "type" in body or "$ref" in body:
        validate_schema_node(body, path, resolver, True, [])
    elif "$defs" in body:
        for defname in sorted(body["$defs"]):
            validate_schema_node(body["$defs"][defname],
                                 path / "$defs" / defname, resolver, True,
                                 [])
    else:
        raise ContractCheckError("SCHEMA_NOT_CLOSED",
                                 "%s: no type/$ref/$defs schema body"
                                 % path)
    if "x-digest" in doc:
        _validate_xdigest(doc, path)


# ---------------------------------------------------------------------------
# Family 2: state machine totality
# ---------------------------------------------------------------------------

def validate_state_document(doc, path: Path) -> dict:
    if not isinstance(doc, dict):
        raise ContractCheckError("STATE_BUNDLE_INVALID",
                                 "%s is not a JSON object" % path)
    if set(doc) != BUNDLE_KEYS:
        raise ContractCheckError(
            "STATE_BUNDLE_INVALID",
            "%s: bundle keys %s != %s"
            % (path, sorted(doc), sorted(BUNDLE_KEYS)))
    if doc["schema_version"] != STATE_BUNDLE_SCHEMA_VERSION:
        raise ContractCheckError(
            "STATE_BUNDLE_INVALID",
            "%s: schema_version must be %s"
            % (path, STATE_BUNDLE_SCHEMA_VERSION))
    for field in ("bundle_id", "title", "contract_ref"):
        if not isinstance(doc[field], str) or not doc[field]:
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s: %s must be non-empty" %
                                     (path, field))
    if not isinstance(doc["machines"], list) or not doc["machines"]:
        raise ContractCheckError("STATE_BUNDLE_INVALID",
                                 "%s: machines must be non-empty" % path)
    seen = set()
    for machine in doc["machines"]:
        machine_id = machine.get("machine_id")
        if machine_id in seen:
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s: duplicate machine %r"
                                     % (path, machine_id))
        seen.add(machine_id)
        check_state_machine_totality(machine, path)
    return doc


def load_state_bundle(path: Path) -> dict:
    return validate_state_document(load_json(path), path)


def check_state_machine_totality(machine, path: Path):
    if not isinstance(machine, dict) or set(machine) != MACHINE_KEYS:
        raise ContractCheckError(
            "STATE_BUNDLE_INVALID",
            "%s: machine keys %s != %s"
            % (path, sorted(machine) if isinstance(machine, dict)
               else machine, sorted(MACHINE_KEYS)))
    name = machine.get("machine_id", "<unknown>")
    for field in ("machine_id", "entity", "contract_ref"):
        if not isinstance(machine[field], str) or not machine[field]:
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s/%s: %s must be non-empty"
                                     % (path, name, field))
    states = machine["states"]
    if not isinstance(states, list) or not states:
        raise ContractCheckError("STATE_BUNDLE_INVALID",
                                 "%s/%s: states empty" % (path, name))
    defined = {}
    for state in states:
        if not isinstance(state, dict) or set(state) != STATE_KEYS:
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s: state keys %s != %s"
                % (path, name, sorted(state) if isinstance(state, dict)
                   else state, sorted(STATE_KEYS)))
        sname = state["name"]
        if not isinstance(sname, str) or not sname:
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s/%s: state name empty" %
                                     (path, name))
        if sname in defined:
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s/%s: duplicate state %r"
                                     % (path, name, sname))
        if not isinstance(state["terminal"], bool):
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s/%s/%s: terminal not boolean"
                                     % (path, name, sname))
        if not isinstance(state["entry_condition"], str):
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s/%s/%s: entry_condition not a "
                                     "string" % (path, name, sname))
        if not isinstance(state["legal_exits"], list):
            raise ContractCheckError("STATE_BUNDLE_INVALID",
                                     "%s/%s/%s: legal_exits not a list"
                                     % (path, name, sname))
        defined[sname] = state
    initials = machine["initial_states"]
    if not isinstance(initials, list) or not initials:
        raise ContractCheckError("STATE_BUNDLE_INVALID",
                                 "%s/%s: initial_states empty" %
                                 (path, name))
    for initial in initials:
        if initial not in defined:
            raise ContractCheckError(
                "UNDEFINED_STATE",
                "%s/%s: initial state %r undefined" % (path, name, initial))
        if defined[initial]["terminal"]:
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s: initial state %r is terminal" %
                (path, name, initial))
    transitions = machine["transitions"]
    if not isinstance(transitions, list):
        raise ContractCheckError("STATE_BUNDLE_INVALID",
                                 "%s/%s: transitions not a list" %
                                 (path, name))
    outgoing = {sname: [] for sname in defined}
    edge_set = set()
    for transition in transitions:
        if not isinstance(transition, dict) or \
                set(transition) != TRANSITION_KEYS:
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s: transition keys %s != %s"
                % (path, name, sorted(transition)
                   if isinstance(transition, dict) else transition,
                   sorted(TRANSITION_KEYS)))
        if transition["from"] not in defined:
            raise ContractCheckError(
                "UNDEFINED_STATE",
                "%s/%s: transition from undefined state %r"
                % (path, name, transition["from"]))
        if transition["to"] not in defined:
            raise ContractCheckError(
                "UNDEFINED_STATE",
                "%s/%s: transition to undefined state %r"
                % (path, name, transition["to"]))
        if transition["authority"] not in KNOWN_AUTHORITIES:
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s: unknown authority %r"
                % (path, name, transition["authority"]))
        for field in ("trigger",):
            if not isinstance(transition[field], str) or \
                    not transition[field]:
                raise ContractCheckError(
                    "STATE_BUNDLE_INVALID",
                    "%s/%s: %s must be non-empty" %
                    (path, name, field))
        key = (transition["from"], transition["to"], transition["trigger"])
        if key in edge_set:
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s: duplicate transition %r" % (path, name, key))
        edge_set.add(key)
        outgoing[transition["from"]].append(transition["to"])
    for sname, state in defined.items():
        declared_exits = state["legal_exits"]
        if len(set(declared_exits)) != len(declared_exits):
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s/%s: duplicate legal_exits" % (path, name, sname))
        for target in declared_exits:
            if target not in defined:
                raise ContractCheckError(
                    "UNDEFINED_STATE",
                    "%s/%s/%s: legal exit %r undefined"
                    % (path, name, sname, target))
        actual = sorted(set(outgoing[sname]))
        if sorted(set(declared_exits)) != actual:
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s/%s: legal_exits %s != declared transitions %s"
                % (path, name, sname, sorted(set(declared_exits)), actual))
        if state["terminal"] and outgoing[sname]:
            raise ContractCheckError(
                "TERMINAL_STATE_REOPEN",
                "%s/%s/%s: terminal state has outgoing transitions"
                % (path, name, sname))
        if not state["terminal"] and not outgoing[sname]:
            raise ContractCheckError(
                "STATE_BUNDLE_INVALID",
                "%s/%s/%s: non-terminal state has no legal exit"
                % (path, name, sname))
    # reachability: every state reachable from some initial state
    reachable = set(initials)
    frontier = list(initials)
    while frontier:
        current = frontier.pop()
        for target in outgoing[current]:
            if target not in reachable:
                reachable.add(target)
                frontier.append(target)
    unreachable = sorted(set(defined) - reachable)
    if unreachable:
        raise ContractCheckError(
            "STATE_BUNDLE_INVALID",
            "%s/%s: unreachable states %s" % (path, name, unreachable))
    return machine


def _find_machine(bundle: dict, machine_id: str):
    for machine in bundle["machines"]:
        if machine["machine_id"] == machine_id:
            return machine
    raise ContractCheckError("UNDEFINED_STATE",
                             "machine %r not in bundle %r"
                             % (machine_id, bundle.get("bundle_id")))


def check_event_sequence(bundle: dict, machine_id: str, initial_state: str,
                         events):
    """Replay an event sequence; every step must be a declared edge."""
    machine = _find_machine(bundle, machine_id)
    if not isinstance(events, list):
        raise ContractCheckError("ILLEGAL_STATE_TRANSITION",
                                 "events must be a list")
    current = initial_state
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ContractCheckError("ILLEGAL_STATE_TRANSITION",
                                     "event %d is not an object" % index)
        source = event.get("from_state")
        target = event.get("to_state")
        defined = {state["name"] for state in machine["states"]}
        if source not in defined:
            raise ContractCheckError(
                "UNDEFINED_STATE",
                "event %d from undefined state %r" % (index, source))
        if target not in defined:
            raise ContractCheckError(
                "UNDEFINED_STATE",
                "event %d to undefined state %r" % (index, target))
        if source != current:
            raise ContractCheckError(
                "ILLEGAL_STATE_TRANSITION",
                "event %d from %r but current state is %r"
                % (index, source, current))
        terminal = {state["name"]: state["terminal"]
                    for state in machine["states"]}
        if terminal[source]:
            raise ContractCheckError(
                "TERMINAL_STATE_REOPEN",
                "event %d reopens terminal state %r" % (index, source))
        edges = {(transition["from"], transition["to"])
                 for transition in machine["transitions"]}
        if (source, target) not in edges:
            raise ContractCheckError(
                "ILLEGAL_STATE_TRANSITION",
                "event %d %r -> %r is not a legal edge of %s"
                % (index, source, target, machine_id))
        current = target
    return current


# ---------------------------------------------------------------------------
# Family 3: profiles
# ---------------------------------------------------------------------------

def check_profile_limits(limits):
    if not isinstance(limits, list) or not limits:
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "limits must be a non-empty list")
    seen = set()
    for limit in limits:
        if not isinstance(limit, dict) or \
                set(limit) != {"field", "unit", "min", "max", "default"}:
            raise ContractCheckError(
                "PROFILE_LIMIT_INVALID",
                "limit entry keys %s invalid"
                % (sorted(limit) if isinstance(limit, dict) else limit))
        field = limit["field"]
        if not isinstance(field, str) or not field:
            raise ContractCheckError("PROFILE_LIMIT_INVALID",
                                     "limit field name empty")
        if field in seen:
            raise ContractCheckError("PROFILE_LIMIT_INVALID",
                                     "duplicate limit field %r" % field)
        seen.add(field)
        unit = limit["unit"]
        if unit not in UNIT_REGISTRY:
            raise ContractCheckError(
                "PROFILE_UNIT_UNDEFINED",
                "limit %r uses unit %r outside the closed registry %s"
                % (field, unit, sorted(UNIT_REGISTRY)))
        for bound in ("min", "max", "default"):
            value = limit[bound]
            if not _is_int(value):
                raise ContractCheckError(
                    "PROFILE_LIMIT_INVALID",
                    "limit %r %s %r is not an integer"
                    % (field, bound, value))
            if value < 0:
                raise ContractCheckError(
                    "PROFILE_LIMIT_INVALID",
                    "limit %r %s %r is negative"
                    % (field, bound, value))
        if limit["min"] > limit["max"]:
            raise ContractCheckError(
                "PROFILE_LIMIT_INVALID",
                "limit %r min %d > max %d"
                % (field, limit["min"], limit["max"]))
        if not (limit["min"] <= limit["default"] <= limit["max"]):
            raise ContractCheckError(
                "PROFILE_LIMIT_INVALID",
                "limit %r default %d outside [%d, %d]"
                % (field, limit["default"], limit["min"], limit["max"]))
    return limits


def check_permission_entries(entries):
    if not isinstance(entries, list):
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "permissions must be a list")
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict) or \
                set(entry) != {"capability", "scope", "effect"}:
            raise ContractCheckError(
                "PERMISSION_SCOPE_MISSING",
                "permission entry keys %s invalid (capability/scope/"
                "effect all required)"
                % (sorted(entry) if isinstance(entry, dict) else entry))
        capability = entry["capability"]
        scope = entry["scope"]
        effect = entry["effect"]
        if not isinstance(capability, str) or not capability:
            raise ContractCheckError("PERMISSION_SCOPE_MISSING",
                                     "permission capability empty")
        if not isinstance(scope, str) or not scope:
            raise ContractCheckError(
                "PERMISSION_SCOPE_MISSING",
                "permission %r carries no explicit scope" % capability)
        if effect not in ("allow", "deny"):
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "permission effect %r invalid"
                                     % effect)
        key = (capability, scope)
        if key in seen:
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "duplicate permission %r" % (key,))
        seen.add(key)
    return entries


def check_runtime_capabilities(capabilities):
    if not isinstance(capabilities, dict) or not capabilities:
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "capabilities must be a non-empty object")
    for name, cap in sorted(capabilities.items()):
        if not isinstance(cap, dict) or not (
                set(cap) <= {"binding", "source", "ambient_forbidden"}):
            raise ContractCheckError(
                "RUNTIME_NONDETERMINISM_CAPABILITY",
                "capability %r has unknown keys" % name)
        binding = cap.get("binding")
        if binding not in ("injected", "forbidden"):
            raise ContractCheckError(
                "RUNTIME_NONDETERMINISM_CAPABILITY",
                "capability %r binding %r is not injected/forbidden "
                "(nondeterminism is not declarable)" % (name, binding))
        if cap.get("ambient_forbidden") is not True:
            raise ContractCheckError(
                "RUNTIME_NONDETERMINISM_CAPABILITY",
                "capability %r must forbid ambient access" % name)
        if binding == "injected":
            source = cap.get("source")
            if not isinstance(source, str) or not source:
                raise ContractCheckError(
                    "RUNTIME_NONDETERMINISM_CAPABILITY",
                    "injected capability %r has no fake source" % name)
    return capabilities


def check_profile_preimage(profile: dict, required_groups):
    preimage = profile.get("digest_preimage")
    if not isinstance(preimage, list) or not preimage:
        raise ContractCheckError("DIGEST_PREIMAGE_INCOMPLETE",
                                 "digest_preimage must be a non-empty list")
    if len(set(preimage)) != len(preimage):
        raise ContractCheckError("DIGEST_PREIMAGE_INCOMPLETE",
                                 "digest_preimage has duplicates")
    if "profile_digest" in preimage:
        raise ContractCheckError("DIGEST_PREIMAGE_INCOMPLETE",
                                 "profile_digest in its own preimage")
    mandatory = ["schema_version", "profile_id", "profile_version"]
    mandatory.extend(required_groups)
    missing = [name for name in mandatory if name not in preimage]
    if missing:
        raise ContractCheckError(
            "DIGEST_PREIMAGE_INCOMPLETE",
            "digest_preimage omits semantic field(s) %s" % missing)
    for name in preimage:
        if name not in profile:
            raise ContractCheckError(
                "DIGEST_PREIMAGE_INCOMPLETE",
                "digest_preimage lists unknown field %r" % name)
    computed = digest_of({name: profile[name] for name in preimage})
    declared = profile.get("profile_digest")
    if not isinstance(declared, str) or not DIGEST_RE.match(declared):
        raise ContractCheckError("PROFILE_DIGEST_MISMATCH",
                                 "profile_digest %r malformed" % declared)
    if computed != declared:
        raise ContractCheckError(
            "PROFILE_DIGEST_MISMATCH",
            "profile_digest %s != recomputed %s" % (declared, computed))
    return preimage


def validate_profile_document(doc, path: Path):
    if not isinstance(doc, dict):
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "%s is not a JSON object" % path)
    profile_id = doc.get("profile_id")
    kind = PROFILE_KINDS.get(profile_id)
    if kind is None:
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "%s: unknown profile_id %r"
                                 % (path, profile_id))
    if set(doc) != kind["keys"]:
        raise ContractCheckError(
            "PROFILE_DOC_INVALID",
            "%s: keys %s != closed set %s"
            % (path, sorted(doc), sorted(kind["keys"])))
    if doc["schema_version"] != kind["schema_version"]:
        raise ContractCheckError(
            "PROFILE_DOC_INVALID",
            "%s: schema_version must be %s" % (path, kind["schema_version"]))
    if not _is_int(doc.get("profile_version")) or \
            doc["profile_version"] < 1:
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "%s: profile_version must be integer>=1"
                                 % path)
    if not isinstance(doc.get("contract_refs"), list) or \
            not doc["contract_refs"]:
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "%s: contract_refs must be non-empty"
                                 % path)
    dm = doc.get("default_materialization")
    if not isinstance(dm, dict) or set(dm) != {"rule"} or \
            not isinstance(dm["rule"], str) or not dm["rule"]:
        raise ContractCheckError("PROFILE_DOC_INVALID",
                                 "%s: default_materialization.rule "
                                 "missing" % path)
    if "limits" in doc:
        check_profile_limits(doc.get("limits"))
    if profile_id == "render-profile":
        renderers = doc.get("renderers")
        if not isinstance(renderers, list) or not renderers:
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "%s: renderers must be non-empty"
                                     % path)
        kinds = set()
        for renderer in renderers:
            if not isinstance(renderer, dict) or \
                    "kind" not in renderer or \
                    renderer["kind"] not in CANONICAL_KINDS:
                raise ContractCheckError(
                    "PROFILE_DOC_INVALID",
                    "%s: renderer kind %r invalid" % (path, renderer))
            if renderer["kind"] in kinds:
                raise ContractCheckError(
                    "PROFILE_DOC_INVALID",
                    "%s: duplicate renderer for kind %r"
                    % (path, renderer["kind"]))
            kinds.add(renderer["kind"])
        if kinds != set(CANONICAL_KINDS):
            raise ContractCheckError(
                "PROFILE_DOC_INVALID",
                "%s: renderers must cover exactly %s" %
                (path, sorted(CANONICAL_KINDS)))
    if profile_id == "permission-profile":
        cap = doc.get("authority_cap")
        if not isinstance(cap, dict) or \
                set(cap) != {"issuer", "max_total_permissions",
                             "max_orchestration_permissions"} or \
                cap["issuer"] != "host":
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "%s: authority_cap invalid" % path)
        for field in ("max_total_permissions",
                      "max_orchestration_permissions"):
            entry = cap[field]
            if not isinstance(entry, dict) or \
                    set(entry) != {"value", "unit"} or \
                    not _is_int(entry["value"]) or entry["value"] < 0 or \
                    entry["unit"] not in UNIT_REGISTRY:
                raise ContractCheckError(
                    "PROFILE_DOC_INVALID",
                    "%s: authority_cap.%s invalid" % (path, field))
        closed = doc.get("capabilities_closed_set")
        if not isinstance(closed, list) or not closed or \
                len(set(closed)) != len(closed):
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "%s: capabilities_closed_set invalid"
                                     % path)
        for capability in closed:
            if not isinstance(capability, str) or not capability:
                raise ContractCheckError("PROFILE_DOC_INVALID",
                                         "%s: capability name empty" % path)
        check_permission_entries(doc.get("permissions"))
        for entry in doc["permissions"]:
            if entry["capability"] not in closed:
                raise ContractCheckError(
                    "PROFILE_DOC_INVALID",
                    "%s: permission capability %r outside closed set"
                    % (path, entry["capability"]))
    if profile_id == "runtime-profile":
        if doc.get("mode") not in ("replay", "live"):
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "%s: mode must be replay|live" % path)
        check_runtime_capabilities(doc.get("capabilities"))
        det = doc.get("determinism")
        if not isinstance(det, dict) or \
                set(det) != {"paired_isolation", "capture_policy",
                             "cross_run_cache",
                             "execution_order_swap_invariant"} or \
                det["paired_isolation"] is not True or \
                det["capture_policy"] != "full_immutable_raw_output" or \
                det["cross_run_cache"] != "forbidden" or \
                det["execution_order_swap_invariant"] is not True:
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "%s: determinism block invalid" % path)
    check_profile_preimage(doc, kind["preimage_groups"])
    return doc


# ---------------------------------------------------------------------------
# Instance validation (closed JSON-Schema subset + semantic rules)
# ---------------------------------------------------------------------------

def _floats_in(instance, where: str):
    if isinstance(instance, float):
        raise ContractCheckError(
            "NON_INTEGER_NUMBER", "float %r in %s" % (instance, where))
    if isinstance(instance, dict):
        for key, value in instance.items():
            _floats_in(value, "%s.%s" % (where, key))
    elif isinstance(instance, list):
        for index, value in enumerate(instance):
            _floats_in(value, "%s[%d]" % (where, index))


def validate_instance(instance, schema, path: Path,
                      _depth: int = 0):
    """Validate ``instance`` against the closed subset schema ``schema``.

    ``schema`` is the loaded schema document (dict); root ``$ref`` and
    ``x-digest`` metadata are honored. Raises ContractCheckError with a
    closed code on the first violation.
    """
    if _depth == 0:
        _floats_in(instance, "instance")
    if _depth > 64:
        raise ContractCheckError("SCHEMA_REF_CYCLE",
                                 "%s: schema nesting too deep" % path)
    meta = schema.get("x-digest") if isinstance(schema, dict) else None
    node = schema
    resolver = RefResolver(path)
    if isinstance(schema, dict) and "$ref" in schema:
        node, target = resolver.resolve(schema["$ref"])
        # Local ``#/...`` pointers inside the resolved target belong to
        # the TARGET document: rebind the resolver, mirroring the
        # schema-side walk (validate_schema_node $ref branch).
        resolver = RefResolver(target)
    _validate_node(instance, node, path, resolver, _depth)
    if _depth == 0 and isinstance(meta, dict):
        _check_instance_digest(instance, meta, path)
    if _depth == 0 and isinstance(instance, dict) and \
            "extensions" in instance:
        _check_extensions(instance["extensions"], path)
    return instance


def _check_instance_digest(instance: dict, meta: dict, path: Path):
    digest_field = meta.get("digest_field")
    preimage = meta.get("preimage_fields") or []
    if digest_field not in instance:
        return
    declared = instance[digest_field]
    if not isinstance(declared, str) or not DIGEST_RE.match(declared):
        raise ContractCheckError("DIGEST_MISMATCH",
                                 "%s: %s malformed" % (path, digest_field))
    core = {name: instance[name] for name in preimage
            if name in instance}
    computed = digest_of(core)
    if computed != declared:
        raise ContractCheckError(
            "DIGEST_MISMATCH",
            "%s: %s %s != recomputed %s"
            % (path, digest_field, declared, computed))


def _check_extensions(extensions, path: Path):
    if not isinstance(extensions, dict):
        raise ContractCheckError("UNKNOWN_REQUIRED_EXTENSION",
                                 "%s: extensions must be an object" % path)
    for key, value in sorted(extensions.items()):
        if isinstance(value, dict) and value.get("required") is True:
            raise ContractCheckError(
                "UNKNOWN_REQUIRED_EXTENSION",
                "%s: required extension %r is not registered in v1"
                % (path, key))


def _validate_node(instance, node, path: Path, resolver: RefResolver,
                   depth: int):
    if node is False:
        raise ContractCheckError("SCHEMA_TYPE_INVALID",
                                 "%s: schema false rejects everything"
                                 % path)
    if node is True or node is None:
        return
    if not isinstance(node, dict):
        raise ContractCheckError("SCHEMA_KEYWORD_UNKNOWN",
                                 "%s: invalid schema node %r" % (path, node))
    if "$ref" in node:
        target, _file = resolver.resolve(node["$ref"])
        _validate_node(instance, target, path, RefResolver(_file), depth + 1)
        # sibling keywords are assertions too in our subset
        rest = {k: v for k, v in node.items() if k != "$ref"}
        if rest:
            _validate_node(instance, rest, path, resolver, depth + 1)
        return
    if "type" in node:
        types = node["type"]
        if isinstance(types, str):
            types = [types]
        matched = False
        for expected in types:
            if _type_matches(instance, expected):
                matched = True
                break
        if not matched:
            raise ContractCheckError(
                "SCHEMA_TYPE_INVALID",
                "%s: expected type %s, got %r"
                % (path, types, instance))
    if "const" in node and not _json_equal(instance, node["const"]):
        raise ContractCheckError(
            "SCHEMA_CONST_MISMATCH",
            "%s: %r != const %r" % (path, instance, node["const"]))
    if "enum" in node and not _enum_contains(node["enum"], instance):
        raise ContractCheckError(
            "SCHEMA_ENUM_INVALID",
            "%s: %r not in enum %s" % (path, instance, node["enum"]))
    if "pattern" in node:
        if not isinstance(instance, str) or \
                not re.search(node["pattern"], instance):
            raise ContractCheckError(
                "SCHEMA_PATTERN_INVALID",
                "%s: %r does not match %s" % (path, instance,
                                              node["pattern"]))
    if "minLength" in node and isinstance(instance, str) and \
            len(instance) < node["minLength"]:
        raise ContractCheckError(
            "SCHEMA_PATTERN_INVALID",
            "%s: length %d < %d" % (path, len(instance),
                                    node["minLength"]))
    if "minimum" in node and _is_int(instance) and \
            instance < node["minimum"]:
        raise ContractCheckError(
            "SCHEMA_MINIMUM_VIOLATED",
            "%s: %r < minimum %r" % (path, instance, node["minimum"]))
    if isinstance(instance, list):
        if "minItems" in node and len(instance) < node["minItems"]:
            raise ContractCheckError(
                "SCHEMA_MINITEMS_VIOLATED",
                "%s: %d items < minItems %d"
                % (path, len(instance), node["minItems"]))
        if "maxItems" in node and len(instance) > node["maxItems"]:
            raise ContractCheckError(
                "SCHEMA_MINITEMS_VIOLATED",
                "%s: %d items > maxItems %d"
                % (path, len(instance), node["maxItems"]))
        if "items" in node:
            for index, item in enumerate(instance):
                _validate_node(item, node["items"],
                               path / ("items[%d]" % index), resolver,
                               depth + 1)
    if isinstance(instance, dict):
        for name in node.get("required", []):
            if name not in instance:
                raise ContractCheckError(
                    "SCHEMA_REQUIRED_FIELD_MISSING",
                    "%s: required field %r missing" % (path, name))
        props = node.get("properties", {})
        if node.get("additionalProperties") is False:
            for key in instance:
                if key not in props:
                    raise ContractCheckError(
                        "SCHEMA_FIELD_UNKNOWN",
                        "%s: unknown core field %r" % (path, key))
        for key, sub in sorted(props.items()):
            if key in instance:
                _validate_node(instance[key], sub, path / key, resolver,
                               depth + 1)
    if "allOf" in node:
        for entry in node["allOf"]:
            _validate_node(instance, entry, path / "allOf", resolver,
                           depth + 1)
    if "if" in node:
        try:
            _validate_node(instance, node["if"], path / "if", resolver,
                           depth + 1)
            branch = node.get("then")
        except ContractCheckError:
            branch = node.get("else")
        if branch is not None:
            try:
                _validate_node(instance, branch, path / "branch",
                               resolver, depth + 1)
            except ContractCheckError as exc:
                if exc.code == "SCHEMA_REQUIRED_FIELD_MISSING":
                    raise ContractCheckError(
                        "SCHEMA_CONDITIONAL_REQUIRED_MISSING",
                        exc.detail) from None
                raise
    if "not" in node:
        try:
            _validate_node(instance, node["not"], path / "not", resolver,
                           depth + 1)
        except ContractCheckError:
            pass
        else:
            raise ContractCheckError(
                "SCHEMA_CONDITIONAL_FORBIDDEN_PRESENT",
                "%s: conditionally forbidden field present (%s)"
                % (path, node["not"]))


def _json_equal(left, right) -> bool:
    """JSON value equality with strict bool/int discrimination."""
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) \
            and left == right
    if _is_int(left) and _is_int(right):
        return left == right
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, (str, list, dict)) and \
            isinstance(right, (str, list, dict)) and \
            type(left) is type(right):
        if isinstance(left, list):
            return len(left) == len(right) and all(
                _json_equal(a, b) for a, b in zip(left, right))
        if isinstance(left, dict):
            return set(left) == set(right) and all(
                _json_equal(left[k], right[k]) for k in left)
        return left == right
    return False


def _enum_contains(enum, value) -> bool:
    return any(_json_equal(candidate, value) for candidate in enum)


def _type_matches(instance, expected: str) -> bool:
    if expected == "object":
        return isinstance(instance, dict)
    if expected == "array":
        return isinstance(instance, list)
    if expected == "string":
        return isinstance(instance, str)
    if expected == "integer":
        return _is_int(instance)
    if expected == "boolean":
        return isinstance(instance, bool)
    if expected == "null":
        return instance is None
    return False


# ---------------------------------------------------------------------------
# Family 4: drift + schema-cases fixture corpus
# ---------------------------------------------------------------------------

def check_policy_drift(root: Path):
    """Recompute the frozen CTR-002/CTR-004 policy digests."""
    results = []
    for rel, field, frozen in FROZEN_POLICIES:
        path = root / rel
        doc = load_json(path)
        if not isinstance(doc, dict) or field not in doc:
            raise ContractCheckError(
                "POLICY_DIGEST_MISMATCH",
                "%s: digest field %r missing" % (path, field))
        declared = doc[field]
        computed = digest_of({k: v for k, v in doc.items() if k != field})
        if declared != frozen:
            raise ContractCheckError(
                "POLICY_DIGEST_MISMATCH",
                "%s: declared %s != Contract-frozen %s"
                % (path, declared, frozen))
        if computed != frozen:
            raise ContractCheckError(
                "POLICY_DIGEST_MISMATCH",
                "%s: recomputed %s != Contract-frozen %s"
                % (path, computed, frozen))
        results.append({"policy": rel, "digest": computed})
    return results


def _load_case_expected(path: Path, case_id: str):
    doc = load_json(path)
    if not isinstance(doc, dict) or set(doc) - EXPECTED_KEYS:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "%s: expected keys invalid" % path)
    if doc.get("schema_version") != SCHEMA_CASE_EXPECTED_VERSION:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "%s: expected schema_version invalid"
                                 % path)
    if doc.get("case_id") != case_id:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "%s: case_id mismatch" % path)
    if not isinstance(doc.get("expected_accept"), bool):
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "%s: expected_accept not boolean" % path)
    reason = doc.get("expected_reason_code")
    if reason is not None and not isinstance(reason, str):
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "%s: expected_reason_code invalid" % path)
    return doc


def _run_dto_case(fix_root: Path, cases_root: Path, case: dict):
    target = fix_root / case["target"]
    schema = load_schema_file(target)
    validate_schema_document(schema, target)
    instance = load_json(cases_root / case["instance_path"])
    validate_instance(instance, schema, target)


def _run_state_case(fix_root: Path, cases_root: Path, case: dict):
    events_doc = load_json(cases_root / case["instance_path"])
    if not isinstance(events_doc, dict) or set(events_doc) != EVENTS_KEYS:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "%s: events keys invalid"
                                 % case["instance_path"])
    if events_doc.get("schema_version") != STATE_EVENTS_VERSION:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "%s: events schema_version invalid"
                                 % case["instance_path"])
    for event in events_doc["events"]:
        if not isinstance(event, dict) or \
                set(event) != {"event_sequence", "from_state",
                               "to_state"}:
            raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                     "%s: event entry keys invalid"
                                     % case["instance_path"])
    bundle_path = fix_root / "schema" / "state" / \
        (events_doc["bundle"] + ".schema.json")
    bundle = load_state_bundle(bundle_path)
    machine_id = events_doc["machine"]
    machine = _find_machine(bundle, machine_id)
    if events_doc["initial_state"] not in machine["initial_states"]:
        raise ContractCheckError(
            "ILLEGAL_STATE_TRANSITION",
            "initial_state %r is not an initial state of %s"
            % (events_doc["initial_state"], machine_id))
    events = sorted(events_doc["events"], key=lambda e: e["event_sequence"])
    sequences = [event["event_sequence"] for event in events]
    if sequences != list(range(1, len(sequences) + 1)):
        raise ContractCheckError("ILLEGAL_STATE_TRANSITION",
                                 "event_sequence must be 1..n unique")
    check_event_sequence(bundle, machine_id,
                         events_doc["initial_state"], events)


def _run_profile_case(fix_root: Path, cases_root: Path, case: dict):
    doc = load_json(cases_root / case["instance_path"])
    validate_profile_document(doc, cases_root / case["instance_path"])


def validate_schema_cases(root: Path):
    """Family 4b: derive accept/reject for every schema-cases case."""
    cases_root = root / "schema-cases"
    manifest = load_json(cases_root / "manifest.json")
    if not isinstance(manifest, dict) or \
            set(manifest) != CASE_MANIFEST_KEYS:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "schema-cases manifest keys invalid")
    if manifest["schema_version"] != SCHEMA_CASES_SCHEMA_VERSION:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "schema-cases manifest schema_version "
                                 "invalid")
    if manifest["contract_schema_version"] != CONTRACT_SCHEMA_VERSION:
        raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                 "schema-cases contract_schema_version "
                                 "invalid")
    seen = set()
    runners = {"dto": _run_dto_case, "state": _run_state_case,
               "profile": _run_profile_case}
    failures = []
    for case in manifest["cases"]:
        if not isinstance(case, dict) or set(case) != CASE_KEYS:
            raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                     "case registration keys invalid")
        if case["category"] not in CASE_CATEGORIES or \
                case["kind"] not in CASE_KINDS:
            raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                     "case category/kind invalid: %r"
                                     % case)
        if case["case_id"] in seen:
            raise ContractCheckError("FIXTURE_EXPECTATION_MISMATCH",
                                     "duplicate case_id %r"
                                     % case["case_id"])
        seen.add(case["case_id"])
        expected = _load_case_expected(cases_root / case["expected_path"],
                                       case["case_id"])
        try:
            runners[case["category"]](root, cases_root, case)
            actual_accept, actual_reason = True, None
        except ContractCheckError as exc:
            actual_accept, actual_reason = False, exc.code
        if actual_accept != expected["expected_accept"] or \
                actual_reason != expected["expected_reason_code"]:
            failures.append(
                "%s: derived accept=%s reason=%s, expected accept=%s "
                "reason=%s" % (case["case_id"], actual_accept,
                               actual_reason,
                               expected["expected_accept"],
                               expected["expected_reason_code"]))
    # orphan check: every case directory must be registered
    for group in ("dto", "state", "profile"):
        group_dir = cases_root / group
        if not group_dir.is_dir():
            continue
        for case_dir in sorted(p for p in group_dir.iterdir()
                               if p.is_dir()):
            payload = sorted(p.name for p in case_dir.iterdir())
            registered = any(
                case["instance_path"].startswith("%s/%s/" % (group,
                                                             case_dir.name))
                for case in manifest["cases"])
            if not registered:
                raise ContractCheckError(
                    "FIXTURE_EXPECTATION_MISMATCH",
                    "unregistered schema-cases directory %s (%s)"
                    % (case_dir, payload))
    return {"total": len(manifest["cases"]), "failures": failures}


# ---------------------------------------------------------------------------
# Orchestration + provenance
# ---------------------------------------------------------------------------

def _iter_contract_files(root: Path):
    for pattern in ("schema/*.json", "schema/shared/*.json",
                    "schema/state/*.json", "policy/*.json",
                    "policy/profiles/*.json"):
        for path in sorted(root.glob(pattern)):
            yield path


def build_provenance(root: Path) -> dict:
    files = []
    for path in _iter_contract_files(root):
        data = path.read_bytes()
        files.append({
            "path": path.relative_to(root).as_posix(),
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
        })
    manifest = {
        "schema_version": PROVENANCE_VERSION,
        "generated_by": {
            "artifact_type": "generated",
            "generator": "validate_contract.py (CTR-005)",
            "task": "CTR-005 machine-readable contract freeze",
        },
        "contract_schema_version": CONTRACT_SCHEMA_VERSION,
        "source_of_truth": (
            "hand-written schemas under schema/ and policy/ are the "
            "source of truth; this manifest is a derived artifact"),
        "files": files,
    }
    manifest["manifest_digest"] = digest_of(manifest)
    return manifest


def run_all_checks(root: Path) -> dict:
    """The four check families; failures is empty iff everything passes."""
    checks = []
    failures = []

    def record(name, fn):
        try:
            detail = fn()
            checks.append({"check": name, "ok": True, "detail": detail})
        except ContractCheckError as exc:
            checks.append({"check": name, "ok": False, "detail": str(exc)})
            failures.append("%s: %s" % (name, exc))

    shared_dir = root / "schema" / "shared"
    state_dir = root / "schema" / "state"
    profiles_dir = root / "policy" / "profiles"

    def check_shared():
        count = 0
        for path in sorted(shared_dir.glob("*.schema.json")):
            validate_schema_document(load_schema_file(path), path)
            count += 1
        if count < 21:
            raise ContractCheckError(
                "SCHEMA_NOT_CLOSED",
                "expected >=21 shared DTO schemas, found %d" % count)
        return {"files": count}

    def check_state():
        machines = 0
        for path in sorted(state_dir.glob("*.schema.json")):
            bundle = validate_state_document(load_json(path), path)
            machines += len(bundle["machines"])
        return {"bundles": len(sorted(state_dir.glob(
            "*.schema.json"))), "machines": machines}

    def check_profiles():
        found = []
        for path in sorted(profiles_dir.glob("*.json")):
            validate_profile_document(load_json(path), path)
            found.append(path.name)
        if len(found) != 4:
            raise ContractCheckError("PROFILE_DOC_INVALID",
                                     "expected 4 profiles, found %r"
                                     % found)
        return {"profiles": found}

    def check_drift_and_cases():
        policies = check_policy_drift(root)
        report = validate_schema_cases(root)
        if report["failures"]:
            raise ContractCheckError(
                "FIXTURE_EXPECTATION_MISMATCH",
                "; ".join(report["failures"]))
        return {"policies": policies, "schema_cases": report["total"]}

    record("schema-closure", check_shared)
    record("state-totality", check_state)
    record("profiles", check_profiles)
    record("drift-and-schema-cases", check_drift_and_cases)
    return {"checks": checks, "failures": failures}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="CTR-005 machine-readable contract validator")
    parser.add_argument("--root", required=True,
                        help="conformance root (the $FIX directory)")
    parser.add_argument("--emit-provenance", action="store_true",
                        help="print a provenance manifest (SHA-256 of "
                             "every schema/policy file) to stdout")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    if not root.is_dir():
        print("validate_contract: root %s is not a directory" % root,
              file=sys.stderr)
        return 2
    try:
        report = run_all_checks(root)
    except ContractCheckError as exc:
        print("STRUCTURAL-ERROR %s" % exc, file=sys.stderr)
        return 2
    for check in report["checks"]:
        status = "OK  " if check["ok"] else "FAIL"
        print("[%s] %-24s %s" % (status, check["check"], check["detail"]))
    if args.emit_provenance:
        provenance = build_provenance(root)
        print("-----PROVENANCE-JSON-----")
        print(json.dumps(provenance, sort_keys=True))
    if report["failures"]:
        for failure in report["failures"]:
            print("FAILURE %s" % failure, file=sys.stderr)
        return 1
    print("validate_contract: all %d check families green"
          % len(report["checks"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
