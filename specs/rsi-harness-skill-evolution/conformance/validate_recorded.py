#!/usr/bin/env python3
"""FND-002 recorded corpus validator: Host Sealed Segments + RSIH Q29-B fake runtime.

Validates the frozen recorded fixture corpus under ``recorded/``:

- ``recorded/segments/**`` — Host sealed Segment records (Contract 7.5-7.7 DTO
  projections, Host spec 3-4 seals): canonical bytes, immutable seals, DAG
  causal links, terminal tools, frontier, and the fail-closed negative cases.
- ``recorded/q29b/**`` — RSIH Q29-B fake runtime packets (RSIH spec 6.2-6.7):
  declared fake clock/random/tool/provider/filesystem capabilities, neutral
  locale/timezone, expected call sequence, late-output audit rules and the
  deterministic run output document.

Design notes (Refactor discipline):

- Recorded *data* validation (schema shape, seals, refs, manifest) is separated
  from *runtime semantics* (``Q29Runner`` simulation of the fake runtime).
- Fixture data carries no executable instructions; a purity scan enforces it.
- Canonicalization is RFC 8785 JCS with an integer-only hashed core.

stdlib only. CLI: ``validate_recorded.py --root <recorded-dir>``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

MANIFEST_SCHEMA_VERSION = "rsih-skill-evolution.recorded-manifest.v1"
CONTRACT_SCHEMA_VERSION = "rsih-skill-evolution.system-contract.v1"
SEGMENT_SCHEMA_VERSION = "host.sealed-segment.v1"
SEAL_SCHEMA_VERSION = "host.segment-seal.v1"
SEGMENT_REF_SCHEMA_VERSION = "host.segment-ref.v1"
CHECKPOINT_REF_SCHEMA_VERSION = "host.checkpoint-ref.v1"
EVIDENCE_REF_SCHEMA_VERSION = "gms.evidence-ref.v1"
TOOL_PROXY_RESULT_SCHEMA_VERSION = "host.tool-proxy-result.v1"
SKILL_REF_SCHEMA_VERSION = "gms.skill-artifact-ref.v1"
CANDIDATE_REF_SCHEMA_VERSION = "gms.candidate-artifact-ref.v1"
Q29B_PACKET_SCHEMA_VERSION = "rsih.q29b-runtime.v1"
Q29B_OUTPUT_SCHEMA_VERSION = "rsih.q29b-run-output.v1"

TERMINAL_SEGMENT_STATES = ("settled", "failed", "aborted")
TERMINAL_DELIVERY_STATES = ("delivered", "failed", "cancelled")
EVENT_KINDS = (
    "agent_message",
    "model_response",
    "tool_call",
    "tool_result",
    "delivery_transition",
    "decision_checkpoint",
    "replay_dispatch",
    "replay_completion",
)
LINK_KINDS = (
    "room_sequence",
    "response_to",
    "caused_by",
    "tool_call_to_result",
    "delivery_of",
    "segment_membership",
    "replay_of",
)
PATH_DOMAINS = ("success", "failure", "recovery")
Q29B_CAPABILITIES = ("clock", "random", "tool", "provider", "filesystem")
Q29B_RUN_SIDES = ("baseline", "candidate")
NEUTRAL_LOCALE = "en_US_POSIX"
NEUTRAL_TIMEZONE = "UTC"
TOOL_FAILURE_REASON_BY_STATUS = {
    "timeout_no_response": "FAKE_TOOL_NO_RESPONSE",
    "no_response": "FAKE_TOOL_NO_RESPONSE",
    "cancelled": "FAKE_TOOL_CANCELLED",
}
FORBIDDEN_DATA_KEYS = frozenset(
    {"script", "program", "executable", "eval", "command", "shell", "run_command"}
)
FORBIDDEN_DATA_SUBSTRINGS = (
    "javascript:",
    "#!/",
    "import os",
    "subprocess",
    "eval(",
    "exec(",
)


class CorpusError(Exception):
    """Fail-closed validation error carrying a closed reason code."""

    def __init__(self, reason_code: str, detail: str = ""):
        super().__init__(f"{reason_code}: {detail}" if detail else reason_code)
        self.reason_code = reason_code
        self.detail = detail


# ---------------------------------------------------------------------------
# RFC 8785 JCS canonicalization (integer-only hashed core)
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


def _jcs_string(value: str) -> str:
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


def _utf16_key(value: str) -> bytes:
    return value.encode("utf-16-be", "surrogatepass")


def jcs_serialize(obj) -> str:
    """Serialize to the RFC 8785 canonical form; non-integer numbers reject."""
    if obj is True:
        return "true"
    if obj is False:
        return "false"
    if obj is None:
        return "null"
    if isinstance(obj, str):
        return _jcs_string(obj)
    if isinstance(obj, int):
        return str(obj)
    if isinstance(obj, float):
        raise CorpusError("NON_INTEGER_NUMBER", "float in hashed core")
    if isinstance(obj, list):
        return "[" + ",".join(jcs_serialize(item) for item in obj) + "]"
    if isinstance(obj, dict):
        for key in obj:
            if not isinstance(key, str):
                raise CorpusError("NON_INTEGER_NUMBER", "non-string object key")
        items = sorted(obj.items(), key=lambda kv: _utf16_key(kv[0]))
        return "{" + ",".join(
            _jcs_string(key) + ":" + jcs_serialize(value) for key, value in items
        ) + "}"
    raise CorpusError("CANONICALIZATION_FAILURE", f"unsupported type {type(obj)!r}")


def canonical_bytes(obj) -> bytes:
    return jcs_serialize(obj).encode("utf-8")


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def digest_of(obj) -> str:
    return digest_bytes(canonical_bytes(obj))


def is_digest(value) -> bool:
    return isinstance(value, str) and DIGEST_RE.fullmatch(value) is not None


def strict_load_json_bytes(data: bytes, origin: str):
    if data.startswith(b"\xef\xbb\xbf"):
        raise CorpusError("BOM_FORBIDDEN", origin)

    def _no_float(text):
        raise CorpusError("NON_INTEGER_NUMBER", f"{origin}: {text}")

    def _no_constant(text):
        raise CorpusError("NON_INTEGER_NUMBER", f"{origin}: {text}")

    def _no_duplicate_keys(pairs):
        seen = set()
        for key, _ in pairs:
            if key in seen:
                raise CorpusError("DUPLICATE_JSON_KEY", f"{origin}: {key}")
            seen.add(key)
        return dict(pairs)

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorpusError("UTF8_INVALID", f"{origin}: {exc}") from exc
    return json.loads(
        text,
        parse_float=_no_float,
        parse_constant=_no_constant,
        object_pairs_hook=_no_duplicate_keys,
    )


def load_json_file(path: Path):
    return strict_load_json_bytes(path.read_bytes(), str(path))


def scan_data_purity(obj, origin: str) -> None:
    """Fixture data must be pure data: no executable instructions."""

    def scan(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in FORBIDDEN_DATA_KEYS:
                    raise CorpusError("FIXTURE_DATA_NOT_PURE", f"{origin}: key {key}")
                scan(value)
        elif isinstance(node, list):
            for item in node:
                scan(item)
        elif isinstance(node, str):
            for marker in FORBIDDEN_DATA_SUBSTRINGS:
                if marker in node:
                    raise CorpusError(
                        "FIXTURE_DATA_NOT_PURE", f"{origin}: substring {marker!r}"
                    )

    scan(obj)


# ---------------------------------------------------------------------------
# Minimal stdlib JSON-Schema subset checker
# ---------------------------------------------------------------------------

_TYPE_CHECKS = {
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
}


class MiniSchema:
    """Checks the JSON-Schema keywords the corpus schemas rely on.

    Supported: type, enum, const, required, properties,
    additionalProperties, items, minItems, maxItems, uniqueItems, minimum,
    maximum, minLength, maxLength, pattern and local ``#/$defs/...`` $ref.
    """

    def __init__(self, document):
        self.document = document

    def _resolve(self, schema):
        while isinstance(schema, dict) and "$ref" in schema:
            ref = schema["$ref"]
            if not ref.startswith("#/$defs/"):
                raise CorpusError("SCHEMA_INVALID", f"unsupported $ref {ref}")
            target = self.document.get("$defs", {}).get(ref[len("#/$defs/"):])
            if target is None:
                raise CorpusError("SCHEMA_INVALID", f"unresolvable $ref {ref}")
            schema = target
        return schema

    def validate(self, obj, path="$"):
        errors = []
        self._check(obj, self.document, path, errors)
        return errors

    def _check(self, obj, schema, path, errors):
        schema = self._resolve(schema)
        if not isinstance(schema, dict):
            return
        if "type" in schema:
            expected = schema["type"]
            checks = expected if isinstance(expected, list) else [expected]
            if not any(_TYPE_CHECKS[c](obj) for c in checks):
                errors.append(f"{path}: expected type {expected}")
                return
        if "enum" in schema and obj not in schema["enum"]:
            errors.append(f"{path}: {obj!r} not in enum {schema['enum']!r}")
        if "const" in schema and obj != schema["const"]:
            errors.append(f"{path}: {obj!r} != const {schema['const']!r}")
        if isinstance(obj, str):
            if "minLength" in schema and len(obj) < schema["minLength"]:
                errors.append(f"{path}: shorter than minLength")
            if "maxLength" in schema and len(obj) > schema["maxLength"]:
                errors.append(f"{path}: longer than maxLength")
            if "pattern" in schema and re.search(schema["pattern"], obj) is None:
                errors.append(f"{path}: {obj!r} fails pattern {schema['pattern']}")
        if isinstance(obj, int) and not isinstance(obj, bool):
            if "minimum" in schema and obj < schema["minimum"]:
                errors.append(f"{path}: below minimum")
            if "maximum" in schema and obj > schema["maximum"]:
                errors.append(f"{path}: above maximum")
        if isinstance(obj, list):
            if "minItems" in schema and len(obj) < schema["minItems"]:
                errors.append(f"{path}: fewer than minItems")
            if "maxItems" in schema and len(obj) > schema["maxItems"]:
                errors.append(f"{path}: more than maxItems")
            if schema.get("uniqueItems") and len(obj) != len(
                {json.dumps(item, sort_keys=True) for item in obj}
            ):
                errors.append(f"{path}: items not unique")
            if "items" in schema:
                for index, item in enumerate(obj):
                    self._check(item, schema["items"], f"{path}[{index}]", errors)
        if isinstance(obj, dict):
            for name in schema.get("required", []):
                if name not in obj:
                    errors.append(f"{path}: missing required property {name!r}")
            properties = schema.get("properties", {})
            for name, value in obj.items():
                if name in properties:
                    self._check(value, properties[name], f"{path}.{name}", errors)
                elif "additionalProperties" in schema:
                    extra = schema["additionalProperties"]
                    if extra is False:
                        errors.append(f"{path}: undeclared property {name!r}")
                    elif isinstance(extra, dict):
                        self._check(value, extra, f"{path}.{name}", errors)


def load_schema(path: Path) -> MiniSchema:
    return MiniSchema(load_json_file(path))


# ---------------------------------------------------------------------------
# Sealed segment derivation (shared by validator and fixture generation)
# ---------------------------------------------------------------------------

def _primary_evidence_kind(segment: dict) -> str:
    domains = {p.get("domain") for p in segment.get("path_seal_body", {}).get("paths", [])}
    if "recovery" in domains:
        return "recovery_path"
    if "failure" in domains:
        return "failure_path"
    return "success_path"


def checkpoint_digest_preimage(segment: dict, segment_digest: str) -> dict:
    identity = segment["checkpoint_identity"]
    return {
        "room_id": segment["room_id"],
        "segment_id": segment["segment_id"],
        "segment_version": segment["segment_version"],
        "segment_digest": segment_digest,
        "checkpoint_id": identity["checkpoint_id"],
        "checkpoint_sequence": identity["checkpoint_sequence"],
        "close_intent": segment["close"]["close_intent"],
    }


def derive_segment_ref(segment: dict, segment_digest: str) -> dict:
    evidence_digest = digest_of(segment["evidence_seal_body"])
    path_digest = digest_of(segment["path_seal_body"])
    return {
        "schema_version": SEGMENT_REF_SCHEMA_VERSION,
        "room_id": segment["room_id"],
        "segment_id": segment["segment_id"],
        "segment_version": segment["segment_version"],
        "segment_digest": segment_digest,
        "evidence_seal_ref": {
            "id": "evseal-" + segment["segment_id"],
            "version": 1,
            "digest": evidence_digest,
        },
        "path_seal_ref": {
            "id": "pathseal-" + segment["segment_id"],
            "version": 1,
            "digest": path_digest,
        },
    }


def derive_seal_envelope(segment: dict, canonical: bytes) -> dict:
    """Derive the complete seal envelope for a segment record.

    settled  -> Evidence Seal + Path Seal + SegmentRef + terminal CheckpointRef
    failed/aborted -> terminal audit only, never any refs.
    """
    state = segment["terminal_state"]
    envelope = {
        "schema_version": SEAL_SCHEMA_VERSION,
        "segment_id": segment["segment_id"],
        "terminal_state": state,
        "canonical_byte_length": len(canonical),
        "segment_digest": digest_bytes(canonical),
    }
    if state != "settled":
        envelope["terminal_audit"] = segment["terminal_audit"]
        return envelope

    segment_digest = envelope["segment_digest"]
    segment_ref = derive_segment_ref(segment, segment_digest)
    evidence_body = segment["evidence_seal_body"]
    path_body = segment["path_seal_body"]
    envelope["evidence_seal"] = {
        "seal_id": "evseal-" + segment["segment_id"],
        "seal_version": 1,
        "seal_digest": digest_of(evidence_body),
        "evidence_ref": {
            "schema_version": EVIDENCE_REF_SCHEMA_VERSION,
            "evidence_id": "evseal-" + segment["segment_id"],
            "version": 1,
            "evidence_digest": digest_of(evidence_body),
            "commit_state": "sealed",
            "evidence_kind": _primary_evidence_kind(segment),
            "source_segment_ref": segment_ref,
        },
    }
    envelope["path_seal"] = {
        "seal_id": "pathseal-" + segment["segment_id"],
        "seal_version": 1,
        "seal_digest": digest_of(path_body),
        "path_ids": [p["path_id"] for p in path_body["paths"]],
    }
    envelope["segment_ref"] = segment_ref
    identity = segment["checkpoint_identity"]
    envelope["checkpoint_ref"] = {
        "schema_version": CHECKPOINT_REF_SCHEMA_VERSION,
        "room_id": segment["room_id"],
        "segment_ref": segment_ref,
        "checkpoint_id": identity["checkpoint_id"],
        "checkpoint_sequence": identity["checkpoint_sequence"],
        "checkpoint_digest": digest_of(checkpoint_digest_preimage(segment, segment_digest)),
    }
    return envelope


# ---------------------------------------------------------------------------
# Sealed segment validation (recorded data)
# ---------------------------------------------------------------------------

class SegmentCaseResult:
    def __init__(self):
        self.accepted = False
        self.reason = None
        self.detail = ""
        self.segment = None
        self.seal = None
        self.canonical = b""


def _require(condition: bool, reason: str, detail: str = "") -> None:
    if not condition:
        raise CorpusError(reason, detail)


def _validate_dag(segment: dict) -> None:
    members = segment["members"]
    links = segment["causal_links"]
    member_ids = {m["event_id"] for m in members}
    _require(len(member_ids) == len(members), "DUPLICATE_EVENT_ID")

    link_ids = set()
    edges = []
    for link in links:
        _require(link["link_id"] not in link_ids, "DUPLICATE_LINK_ID", link["link_id"])
        link_ids.add(link["link_id"])
        _require(
            link["from_event"] in member_ids and link["to_event"] in member_ids,
            "MISSING_CAUSAL_LINK",
            f"{link['link_id']} endpoint not in frozen membership",
        )
        _require(link["link_kind"] in LINK_KINDS, "UNKNOWN_LINK_KIND", link["link_kind"])
        edges.append((link["from_event"], link["to_event"], link["link_kind"]))

    # room_sequence chain must connect consecutive members
    by_seq = sorted(members, key=lambda m: m["room_sequence"])
    chain = {(f, t) for f, t, kind in edges if kind == "room_sequence"}
    for left, right in zip(by_seq, by_seq[1:]):
        _require(
            (left["event_id"], right["event_id"]) in chain,
            "MISSING_CAUSAL_LINK",
            f"room_sequence gap {left['event_id']}->{right['event_id']}",
        )

    # per-kind causal requirements. Edge direction convention: every causal
    # edge points from cause to effect (forward w.r.t. the room sequence
    # chain), so response_to runs original->response, delivery_of runs
    # delivered-event->delivery-transition and replay_of runs
    # dispatch->completion.
    outgoing = {}
    incoming = {}
    for f, t, kind in edges:
        outgoing.setdefault((f, kind), []).append(t)
        incoming.setdefault((t, kind), []).append(f)

    def outgoing_count(event_id, kind):
        return len(outgoing.get((event_id, kind), []))

    def incoming_count(event_id, kind):
        return len(incoming.get((event_id, kind), []))

    for member in by_seq:
        kind = member["event_kind"]
        event_id = member["event_id"]
        if kind in ("model_response", "decision_checkpoint"):
            _require(
                incoming_count(event_id, "response_to") >= 1,
                "MISSING_CAUSAL_LINK",
                f"{event_id} lacks response_to",
            )
        elif kind == "tool_call":
            _require(
                outgoing_count(event_id, "tool_call_to_result") == 1,
                "MISSING_CAUSAL_LINK",
                f"{event_id} must have exactly one tool result",
            )
        elif kind == "tool_result":
            _require(
                incoming_count(event_id, "tool_call_to_result") == 1,
                "MISSING_CAUSAL_LINK",
                f"{event_id} lacks tool_call_to_result",
            )
        elif kind == "delivery_transition":
            _require(
                incoming_count(event_id, "delivery_of") == 1,
                "MISSING_CAUSAL_LINK",
                f"{event_id} lacks delivery_of",
            )
        elif kind == "replay_dispatch":
            _require(
                outgoing_count(event_id, "replay_of") >= 1,
                "MISSING_CAUSAL_LINK",
                f"{event_id} lacks replay_of",
            )
        elif kind == "replay_completion":
            _require(
                incoming_count(event_id, "replay_of") == 1,
                "MISSING_CAUSAL_LINK",
                f"{event_id} lacks replay_of",
            )

    # cycle detection (iterative DFS, deterministic order)
    adjacency = {}
    for f, t, _kind in edges:
        adjacency.setdefault(f, []).append(t)
    WHITE, GREY, BLACK = 0, 1, 2
    color = {event_id: WHITE for event_id in member_ids}
    for start in sorted(member_ids):
        if color[start] != WHITE:
            continue
        stack = [(start, iter(sorted(adjacency.get(start, []))))]
        color[start] = GREY
        while stack:
            node, neighbours = stack[-1]
            advanced = False
            for nxt in neighbours:
                if color[nxt] == GREY:
                    raise CorpusError("DAG_CYCLE", f"cycle through {nxt}")
                if color[nxt] == WHITE:
                    color[nxt] = GREY
                    stack.append((nxt, iter(sorted(adjacency.get(nxt, [])))))
                    advanced = True
                    break
            if not advanced:
                color[node] = BLACK
                stack.pop()


def _validate_tool_proxy_result(execution: dict) -> None:
    tpr = execution["tool_proxy_result"]
    _require(
        tpr.get("schema_version") == TOOL_PROXY_RESULT_SCHEMA_VERSION,
        "TOOL_PROXY_DIGEST_MISMATCH",
        "unexpected ToolProxyResult schema_version",
    )
    _require(
        is_digest(tpr.get("proxy_result_digest")),
        "TOOL_PROXY_DIGEST_MISMATCH",
        "proxy_result_digest malformed",
    )
    preimage = {k: v for k, v in tpr.items() if k != "proxy_result_digest"}
    _require(
        digest_of(preimage) == tpr["proxy_result_digest"],
        "TOOL_PROXY_DIGEST_MISMATCH",
        "proxy_result_digest preimage mismatch",
    )
    status = tpr.get("status")
    if status == "succeeded":
        _require("result" in tpr, "TOOL_PROXY_DIGEST_MISMATCH", "missing result")
        _require(
            digest_of(tpr["result"]) == tpr.get("upstream_result_digest"),
            "TOOL_PROXY_DIGEST_MISMATCH",
            "upstream_result_digest must cover exact upstream payload",
        )
    else:
        _require(status in ("failed", "inconclusive"), "TOOL_PROXY_STATUS_INVALID")
        error = tpr.get("error", {})
        for field in ("reason_code", "message", "retryable"):
            _require(field in error, "TOOL_PROXY_DIGEST_MISMATCH", f"error.{field}")
        attempt = execution.get("upstream_attempt_record")
        _require(isinstance(attempt, dict), "TOOL_PROXY_DIGEST_MISMATCH",
                 "failed result needs upstream attempt record")
        _require(
            digest_of(attempt) == tpr.get("upstream_result_digest"),
            "TOOL_PROXY_DIGEST_MISMATCH",
            "upstream_result_digest must cover the upstream attempt record",
        )
        refs = error.get("record_refs", [])
        _require(
            len(refs) == 1
            and refs[0].get("id") == attempt.get("attempt_id")
            and refs[0].get("digest") == digest_of(attempt),
            "TOOL_PROXY_DIGEST_MISMATCH",
            "error.record_refs must reference the attempt record",
        )


def _validate_settled_close(segment: dict, derived: dict) -> None:
    members = segment["members"]
    frontier = segment["frontier"]

    # frontier / membership freeze (append-after-settled check)
    sequences = [m["room_sequence"] for m in members]
    _require(
        all(frontier["start_room_sequence"] < s <= frontier["end_room_sequence"] for s in sequences),
        "APPEND_AFTER_SETTLED",
        "member room_sequence outside frozen frontier",
    )
    _require(
        sequences == sorted(sequences) and len(set(sequences)) == len(sequences),
        "MEMBER_ORDER_INVALID",
        "member room_sequence must strictly increase",
    )
    _require(
        sequences and sequences[-1] == frontier["end_room_sequence"],
        "FRONTIER_MISMATCH",
        "frontier end must equal last member sequence",
    )
    ticks = [m["logical_tick"] for m in members]
    _require(ticks == sorted(ticks) and len(set(ticks)) == len(ticks),
             "LOGICAL_CLOCK_INVALID", "logical ticks must strictly increase")

    # payload digests
    for member in members:
        _require(member["event_kind"] in EVENT_KINDS, "UNKNOWN_EVENT_KIND",
                 member["event_kind"])
        _require(
            digest_of(member["payload"]) == member["payload_digest"],
            "PAYLOAD_DIGEST_MISMATCH",
            member["event_id"],
        )

    _validate_dag(segment)

    # terminal tools and deliveries
    deliveries = {d["delivery_id"]: d for d in segment["deliveries"]}
    _require(len(deliveries) == len(segment["deliveries"]), "DUPLICATE_DELIVERY_ID")
    for delivery in segment["deliveries"]:
        _require(
            delivery["terminal_state"] in TERMINAL_DELIVERY_STATES,
            "NONTERMINAL_TOOL",
            f"{delivery['delivery_id']} state {delivery['terminal_state']}",
        )
    members_by_id = {m["event_id"]: m for m in members}
    for execution in segment["tool_executions"]:
        call = members_by_id.get(execution["tool_call_event"])
        result = members_by_id.get(execution["tool_result_event"])
        _require(call is not None and call["event_kind"] == "tool_call",
                 "TOOL_EXECUTION_INVALID", "tool_call_event")
        _require(result is not None and result["event_kind"] == "tool_result",
                 "TOOL_EXECUTION_INVALID", "tool_result_event")
        delivery = deliveries.get(execution["delivery_id"])
        _require(delivery is not None, "TOOL_EXECUTION_INVALID", "unknown delivery")
        _require(
            delivery["terminal_state"] in TERMINAL_DELIVERY_STATES,
            "NONTERMINAL_TOOL",
            execution["delivery_id"],
        )
        _validate_tool_proxy_result(execution)
    tool_call_ids = [e["tool_call_event"] for e in segment["tool_executions"]]
    expected_calls = [m["event_id"] for m in members if m["event_kind"] == "tool_call"]
    _require(
        sorted(tool_call_ids) == sorted(expected_calls),
        "TOOL_EXECUTION_INVALID",
        "every tool_call must have exactly one execution record",
    )

    # seals + derived refs
    seal = derived
    evidence_body = segment["evidence_seal_body"]
    path_body = segment["path_seal_body"]
    ordered = [
        {"event_id": m["event_id"], "room_sequence": m["room_sequence"],
         "payload_digest": m["payload_digest"]}
        for m in members
    ]
    _require(
        evidence_body.get("ordered_event_refs") == ordered,
        "EVIDENCE_SEAL_BODY_MISMATCH",
        "ordered event refs must match frozen membership",
    )
    _require(
        evidence_body.get("sequence_interval") == frontier,
        "EVIDENCE_SEAL_BODY_MISMATCH",
        "sequence interval must equal frontier",
    )
    _require(
        evidence_body.get("causal_link_set_digest")
        == digest_of(segment["causal_links"]),
        "EVIDENCE_SEAL_BODY_MISMATCH",
        "causal link set digest",
    )
    _require(
        evidence_body.get("deliveries")
        == [{"delivery_id": d["delivery_id"], "terminal_state": d["terminal_state"]}
            for d in segment["deliveries"]],
        "EVIDENCE_SEAL_BODY_MISMATCH",
        "delivery identities and terminal states",
    )
    _require(
        evidence_body.get("tool_correlations")
        == [{
            "tool_call_event": e["tool_call_event"],
            "tool_result_event": e["tool_result_event"],
            "tool_proxy_result_digest": e["tool_proxy_result"]["proxy_result_digest"],
        } for e in segment["tool_executions"]],
        "EVIDENCE_SEAL_BODY_MISMATCH",
        "tool call/result correlation",
    )
    _require(
        sorted(evidence_body.get("actor_ids", []))
        == sorted({m["actor_id"] for m in members}),
        "EVIDENCE_SEAL_BODY_MISMATCH",
        "actor identities",
    )
    link_ids = {l["link_id"] for l in segment["causal_links"]}
    checkpoint_id = segment["checkpoint_identity"]["checkpoint_id"]
    for path in path_body.get("paths", []):
        _require(path["domain"] in PATH_DOMAINS, "UNKNOWN_PATH_DOMAIN",
                 path.get("domain"))
        _require(len(path["ordered_event_refs"]) >= 1, "PATH_BODY_MISMATCH",
                 "path must reference events")
        for event_id in path["ordered_event_refs"]:
            _require(event_id in members_by_id, "PATH_BODY_MISMATCH",
                     f"unknown event {event_id}")
        for link_id in path.get("causal_link_refs", []):
            _require(link_id in link_ids, "PATH_BODY_MISMATCH",
                     f"unknown link {link_id}")
        _require(path.get("checkpoint_ref_id") == checkpoint_id,
                 "PATH_BODY_MISMATCH", "checkpoint correlation")
        _require(path.get("completeness") in ("complete", "incomplete"),
                 "PATH_BODY_MISMATCH", "completeness")
    _require(len(path_body.get("paths", [])) >= 1, "PATH_BODY_MISMATCH",
             "settled close must seal at least one path")
    _require(seal["evidence_seal"]["seal_digest"] == digest_of(evidence_body),
             "EVIDENCE_SEAL_DIGEST_MISMATCH", "recomputed evidence seal digest")
    _require(seal["path_seal"]["seal_digest"] == digest_of(path_body),
             "PATH_SEAL_DIGEST_MISMATCH", "recomputed path seal digest")


def validate_segment_case(case_dir: Path, schema: MiniSchema) -> SegmentCaseResult:
    result = SegmentCaseResult()
    try:
        segment = load_json_file(case_dir / "segment.json")
        seal = load_json_file(case_dir / "seal.json")
        canonical = (case_dir / "canonical.utf8").read_bytes()
        result.segment = segment
        result.seal = seal
        result.canonical = canonical

        errors = schema.validate(segment)
        _require(not errors, "SCHEMA_VIOLATION", "; ".join(errors[:4]))
        scan_data_purity(segment, str(case_dir / "segment.json"))
        scan_data_purity(seal, str(case_dir / "seal.json"))

        # seal immutability: digest covers canonical bytes byte-for-byte
        _require(seal.get("segment_digest") == digest_bytes(canonical),
                 "SEAL_DIGEST_MISMATCH", "seal digest does not cover canonical bytes")
        _require(seal.get("canonical_byte_length") == len(canonical),
                 "SEAL_DIGEST_MISMATCH", "canonical byte length")
        _require(
            canonical == canonical_bytes(segment),
            "CANONICAL_MISMATCH",
            "canonical.utf8 is not the JCS form of segment.json",
        )
        _require(
            seal.get("terminal_state") == segment.get("terminal_state"),
            "SEAL_STATE_MISMATCH",
        )

        derived = derive_seal_envelope(segment, canonical)
        if segment["terminal_state"] != "settled":
            for forbidden in ("segment_ref", "checkpoint_ref", "evidence_seal",
                              "path_seal"):
                _require(forbidden not in seal, "NONSETTLED_SEGMENT_HAS_REFS",
                         f"{forbidden} present on {segment['terminal_state']}")
            for forbidden in ("evidence_seal_body", "path_seal_body",
                              "checkpoint_identity"):
                _require(forbidden not in segment, "NONSETTLED_SEGMENT_HAS_REFS",
                         forbidden)
            _require("terminal_audit" in seal, "TERMINAL_AUDIT_MISSING")
            _require(
                seal.get("terminal_audit") == segment.get("terminal_audit"),
                "TERMINAL_AUDIT_MISMATCH",
            )
        else:
            _validate_settled_close(segment, derived)
            _require(
                seal.get("evidence_seal") == derived["evidence_seal"],
                "EVIDENCE_SEAL_DIGEST_MISMATCH", "evidence seal block",
            )
            _require(
                seal.get("path_seal") == derived["path_seal"],
                "PATH_SEAL_DIGEST_MISMATCH", "path seal block",
            )
            _require(seal.get("segment_ref") == derived["segment_ref"],
                     "SEGMENT_REF_MISMATCH")
            _require(seal.get("checkpoint_ref") == derived["checkpoint_ref"],
                     "CHECKPOINT_REF_MISMATCH")
        result.accepted = True
    except CorpusError as exc:
        result.reason = exc.reason_code
        result.detail = exc.detail
    except (OSError, KeyError, TypeError, ValueError) as exc:
        result.reason = "SEGMENT_RECORD_INVALID"
        result.detail = str(exc)
    return result


# ---------------------------------------------------------------------------
# Q29-B fake runtime semantics (deterministic simulation)
# ---------------------------------------------------------------------------

class Q29Simulation:
    def __init__(self):
        self.status = "succeeded"
        self.reason_code = None
        self.completed_step = 0
        self.failing_step = None
        self.terminal_tick = None
        self.consumed = {
            "clock_ticks": [],
            "random_draws": [],
            "tool_call_indexes": [],
            "provider_call_indexes": [],
            "fs_paths": [],
        }
        self.usage = {
            "clock_reads": 0,
            "random_draws": 0,
            "tool_calls": 0,
            "provider_calls": 0,
            "fs_accesses": 0,
        }
        self.tool_statuses = []

    def fail(self, reason_code: str, step: int):
        self.status = "failed"
        self.reason_code = reason_code
        self.failing_step = step
        return self

    def terminal_record(self) -> dict:
        return {
            "status": self.status,
            "reason_code": self.reason_code,
            "terminal_tick": self.terminal_tick,
            "completed_step": self.completed_step,
            "failing_step": self.failing_step,
        }


def simulate_packet(packet: dict) -> Q29Simulation:
    """Deterministically replay the declared fake capability usage."""
    sim = Q29Simulation()
    capabilities = packet["capabilities"]
    clock = capabilities.get("clock") or {}
    ticks = clock.get("ticks") or []
    random = capabilities.get("random") or {}
    draws = random.get("draws") or []
    tool_responses = (capabilities.get("tool") or {}).get("responses") or []
    provider_responses = (capabilities.get("provider") or {}).get("responses") or []
    fs_entries = {
        e["path"]: e for e in (capabilities.get("filesystem") or {}).get("entries") or []
    }
    clock_i = 0
    random_i = 0

    for entry in packet["call_sequence"]:
        step = entry["step"]
        capability = entry["capability"]
        operation = entry["operation"]
        if capability not in Q29B_CAPABILITIES:
            return sim.fail("UNDECLARED_CAPABILITY_ACCESS", step)
        if capability == "clock":
            if clock_i >= len(ticks):
                return sim.fail("FAKE_CLOCK_EXHAUSTED", step)
            sim.consumed["clock_ticks"].append(ticks[clock_i])
            sim.terminal_tick = ticks[clock_i]
            clock_i += 1
            sim.usage["clock_reads"] += 1
        elif capability == "random":
            if random_i >= len(draws):
                return sim.fail("FAKE_RANDOM_EXHAUSTED", step)
            sim.consumed["random_draws"].append(draws[random_i])
            random_i += 1
            sim.usage["random_draws"] += 1
        elif capability == "tool":
            response = None
            for candidate in tool_responses:
                if (candidate.get("call_index") == entry.get("call_index")
                        and candidate.get("tool_name") == entry.get("tool_name")):
                    response = candidate
                    break
            if response is None:
                return sim.fail("UNDECLARED_TOOL_ACCESS", step)
            sim.consumed["tool_call_indexes"].append(entry["call_index"])
            sim.usage["tool_calls"] += 1
            sim.tool_statuses.append(response.get("status"))
        elif capability == "provider":
            response = None
            for candidate in provider_responses:
                if candidate.get("call_index") == entry.get("call_index"):
                    if ("model_id" in entry
                            and candidate.get("model_id") != entry["model_id"]):
                        continue
                    response = candidate
                    break
            if response is None:
                return sim.fail("UNDECLARED_PROVIDER_ACCESS", step)
            sim.consumed["provider_call_indexes"].append(entry["call_index"])
            sim.usage["provider_calls"] += 1
        elif capability == "filesystem":
            path = entry.get("path")
            if path not in fs_entries:
                return sim.fail("UNDECLARED_FS_ACCESS", step)
            sim.consumed["fs_paths"].append(path)
            sim.usage["fs_accesses"] += 1
        else:  # pragma: no cover - guarded by the membership check above
            return sim.fail("UNDECLARED_CAPABILITY_ACCESS", step)
        sim.completed_step = step

    for status in sim.tool_statuses:
        if status != "succeeded":
            sim.status = "failed"
            sim.reason_code = TOOL_FAILURE_REASON_BY_STATUS.get(
                status, "UNDECLARED_TOOL_ACCESS")
            break
    return sim


LATE_EVENT_FIELDS = ("arrival_step", "capability", "origin", "detail", "recorded_as")
RUN_FIELDS = ("mode", "side", "run_id", "attempt", "adapter_ref", "profile_ref")


def build_output_document(packet: dict, sim: Q29Simulation) -> dict:
    """Deterministic raw execution output record (RSIH-private, replay-only)."""
    late_audit = [
        {field: event.get(field) for field in LATE_EVENT_FIELDS}
        for event in packet.get("late_events") or []
    ]
    return {
        "schema_version": Q29B_OUTPUT_SCHEMA_VERSION,
        "packet_id": packet["packet_id"],
        "run": {field: packet["run"][field] for field in RUN_FIELDS},
        "sealed_input_digest": digest_of(packet["sealed_inputs"]),
        "environment_digest": digest_of(packet["environment"]),
        "steps_executed": sim.completed_step,
        "capability_usage": dict(sim.usage),
        "consumed": {key: list(value) for key, value in sim.consumed.items()},
        "late_audit": late_audit,
        "terminal": sim.terminal_record(),
    }


# ---------------------------------------------------------------------------
# Q29-B packet validation (recorded data against runtime semantics)
# ---------------------------------------------------------------------------

class Q29CaseResult:
    def __init__(self):
        self.accepted = False
        self.reason = None
        self.detail = ""
        self.output_digest = None
        self.terminal_record = None


def _validate_q29b_environment(packet: dict) -> None:
    environment = packet["environment"]
    _require(environment.get("network") == "disabled", "NETWORK_ACCESS_DECLARED")
    _require(environment.get("cache") == "per-run-isolated",
             "CROSS_RUN_CACHE_DECLARED")
    _require(
        environment.get("locale") == NEUTRAL_LOCALE
        and environment.get("timezone") == NEUTRAL_TIMEZONE,
        "ENVIRONMENT_NOT_NEUTRAL",
    )


def _validate_q29b_capabilities(packet: dict) -> None:
    capabilities = packet["capabilities"]
    fake_kinds = {
        "clock": "fake-sequence",
        "random": "fake-sequence",
        "tool": "fake-responses",
        "provider": "fake-responses",
        "filesystem": "fake-tree",
    }
    for capability, fake_kind in fake_kinds.items():
        declaration = capabilities.get(capability)
        _require(isinstance(declaration, dict), "REAL_CAPABILITY_DECLARED",
                 f"{capability} capability not declared")
        _require(declaration.get("kind") == fake_kind, "REAL_CAPABILITY_DECLARED",
                 f"{capability} kind {declaration.get('kind')!r}")


def _validate_q29b_sealed_inputs(packet: dict, segment_index: dict) -> None:
    sealed = packet["sealed_inputs"]
    refs = sealed.get("segment_refs") or []
    _require(len(refs) >= 1, "MISSING_SEALED_PATH", "no sealed segment refs")
    referenced = set()
    for ref in refs:
        entry = segment_index.get(ref.get("segment_id"))
        if entry is None:
            raise CorpusError("MISSING_SEALED_PATH",
                              f"segment {ref.get('segment_id')!r} not in sealed corpus")
        if not entry["settled"]:
            raise CorpusError("MISSING_SEALED_PATH",
                              f"segment {ref.get('segment_id')!r} is not settled")
        if not entry["accepted"]:
            raise CorpusError("SEAL_DIGEST_MISMATCH",
                              f"segment {ref.get('segment_id')!r} seal is unverifiable")
        if ref != entry["segment_ref"]:
            raise CorpusError("SEAL_DIGEST_MISMATCH",
                              f"segment ref {ref.get('segment_id')!r} does not match seals")
        referenced.add(ref["segment_id"])
    resolved_domains = set()
    for path_ref in sealed.get("path_refs") or []:
        segment_id = path_ref.get("segment_id")
        if segment_id not in referenced:
            raise CorpusError("MISSING_SEALED_PATH",
                              f"path {path_ref.get('path_id')!r} outside sealed refs")
        entry = segment_index[segment_id]
        path = entry["paths"].get(path_ref.get("path_id"))
        if path is None:
            raise CorpusError("MISSING_SEALED_PATH",
                              f"path {path_ref.get('path_id')!r} not sealed")
        if path.get("domain") != path_ref.get("domain"):
            raise CorpusError("MISSING_SEALED_PATH",
                              f"path {path_ref.get('path_id')!r} domain mismatch")
        if path_ref.get("path_seal_ref") != entry["path_seal_ref"]:
            raise CorpusError("SEAL_DIGEST_MISMATCH",
                              f"path seal ref {path_ref.get('path_id')!r}")
        resolved_domains.add(path["domain"])
    for domain in sealed.get("required_path_domains") or []:
        _require(domain in resolved_domains, "MISSING_SEALED_PATH",
                 f"required path domain {domain!r} unresolved")
    if packet.get("family") == "candidate":
        for domain in PATH_DOMAINS:
            _require(domain in resolved_domains, "MISSING_SEALED_PATH",
                     f"candidate replay must cover {domain} path")


def _validate_q29b_run_identity(packet: dict) -> None:
    run = packet["run"]
    artifact_ref = packet["sealed_inputs"].get("artifact_ref") or {}
    artifact_schema = artifact_ref.get("schema_version")
    if run.get("mode") == "live":
        if artifact_schema == CANDIDATE_REF_SCHEMA_VERSION:
            raise CorpusError("CANDIDATE_IN_LIVE_REGISTRY",
                              "candidate artifact in live run")
        raise CorpusError("RUN_MODE_NOT_REPLAY", "recorded corpus is replay-only")
    _require(run.get("mode") == "replay", "RUN_MODE_NOT_REPLAY", run.get("mode"))
    side = run.get("side")
    _require(side in Q29B_RUN_SIDES, "RUN_SIDE_INVALID", side)
    expected_schema = (SKILL_REF_SCHEMA_VERSION if side == "baseline"
                       else CANDIDATE_REF_SCHEMA_VERSION)
    _require(artifact_schema == expected_schema, "ARTIFACT_REF_SIDE_MISMATCH",
             f"{side} side with {artifact_schema}")


def _validate_q29b_bilateral(packet: dict) -> None:
    family = packet.get("family")
    if family not in ("overlap", "conflict"):
        return
    bilateral = packet.get("bilateral")
    _require(isinstance(bilateral, dict), "BILATERAL_CONTEXT_INVALID",
             "overlap/conflict families need bilateral context")
    _require(bilateral.get("family_domain") == family, "BILATERAL_CONTEXT_INVALID",
             "family domain")
    source_a = bilateral.get("source_a_ref") or {}
    source_b = bilateral.get("source_b_ref") or {}
    for source in (source_a, source_b):
        _require(source.get("schema_version") == SKILL_REF_SCHEMA_VERSION,
                 "BILATERAL_CONTEXT_INVALID", "source refs must be exact SkillArtifactRefs")
    _require(source_a != source_b, "BILATERAL_CONTEXT_INVALID",
             "sources must be distinct")
    if family == "overlap":
        _require(len(bilateral.get("shared_context_refs") or []) >= 1,
                 "BILATERAL_CONTEXT_INVALID", "shared_context_refs")
    else:
        points = bilateral.get("decision_points") or []
        _require(len(points) >= 1, "BILATERAL_CONTEXT_INVALID",
                 "conflict family needs a decision point")
        for point in points:
            _require(
                point.get("source_a_decision") != point.get("source_b_decision"),
                "BILATERAL_CONTEXT_INVALID",
                f"decision point {point.get('decision_point_id')!r} does not diverge",
            )


def validate_q29b_case(case_dir: Path, schema: MiniSchema,
                       segment_index: dict) -> Q29CaseResult:
    result = Q29CaseResult()
    try:
        packet = load_json_file(case_dir / "runtime.json")
        errors = schema.validate(packet)
        _require(not errors, "SCHEMA_VIOLATION", "; ".join(errors[:4]))
        scan_data_purity(packet, str(case_dir / "runtime.json"))
        expected_bytes = (case_dir / "expected-output.canonical").read_bytes()

        _validate_q29b_run_identity(packet)
        _validate_q29b_environment(packet)
        _validate_q29b_capabilities(packet)
        _validate_q29b_sealed_inputs(packet, segment_index)
        _validate_q29b_bilateral(packet)

        # deterministic call sequence simulation
        sim = simulate_packet(packet)
        recorded_terminal = packet["terminal"]
        if (recorded_terminal.get("status") != sim.status
                or recorded_terminal.get("reason_code") != sim.reason_code
                or recorded_terminal.get("completed_step") != sim.completed_step
                or recorded_terminal.get("failing_step") != sim.failing_step):
            raise CorpusError(
                sim.reason_code if sim.status == "failed" else "TERMINAL_MISMATCH",
                "recorded terminal disagrees with deterministic simulation",
            )

        # late outputs are audit-only and strictly post-terminal
        for event in packet.get("late_events") or []:
            _require(event.get("recorded_as") == "late-audit-only",
                     "LATE_OUTPUT_REWRITE", "late event must be audit-only")
            _require(
                event.get("arrival_step") > recorded_terminal.get("completed_step", 0),
                "LATE_OUTPUT_REWRITE",
                "late event must arrive after the terminal step",
            )

        output = build_output_document(packet, sim)
        output_again = build_output_document(packet, simulate_packet(packet))
        output_digest = digest_of(output)
        _require(output_digest == digest_of(output_again),
                 "NONDETERMINISTIC_OUTPUT", "repeat simulation digest differs")

        expected = packet.get("expected") or {}
        _require(expected.get("run_status") == sim.status,
                 "RUN_STATUS_EXPECTATION_MISMATCH")
        _require(expected.get("reason_code") == sim.reason_code,
                 "RUN_REASON_EXPECTATION_MISMATCH")
        _require(canonical_bytes(output) == expected_bytes,
                 "EXPECTED_OUTPUT_MISMATCH",
                 "expected-output.canonical is not the simulated output",
                 )
        _require(expected.get("output_digest") == output_digest,
                 "EXPECTED_OUTPUT_DIGEST_MISMATCH")

        result.output_digest = output_digest
        result.terminal_record = sim.terminal_record()
        result.accepted = True
    except CorpusError as exc:
        result.reason = exc.reason_code
        result.detail = exc.detail
    except (OSError, KeyError, TypeError, ValueError) as exc:
        result.reason = "Q29B_PACKET_INVALID"
        result.detail = str(exc)
    return result


# ---------------------------------------------------------------------------
# Corpus manifest orchestration
# ---------------------------------------------------------------------------

class CaseOutcome:
    def __init__(self, case_id, corpus, family, expected, result, reason, detail,
                 output_digest, expected_reason=None):
        self.case_id = case_id
        self.corpus = corpus
        self.family = family
        self.expected = expected
        self.expected_reason = expected_reason
        self.result = result
        self.reason = reason
        self.detail = detail
        self.output_digest = output_digest


class CorpusReport:
    def __init__(self):
        self.case_outcomes = []
        self.corpus_errors = []
        self.q29b_output_digests = {}
        self.q29b_terminal_records = {}

    @property
    def ok(self) -> bool:
        return not self.corpus_errors and all(
            self._case_ok(outcome) for outcome in self.case_outcomes
        )

    @staticmethod
    def _case_ok(outcome: CaseOutcome) -> bool:
        if outcome.expected == "reserved":
            return outcome.result == "reserved"
        if outcome.expected == "accept":
            return outcome.result == "accept"
        return (outcome.result == "reject"
                and outcome.reason == outcome.expected_reason)

    @property
    def by_case(self) -> dict:
        return {outcome.case_id: outcome for outcome in self.case_outcomes}

    def render(self) -> str:
        lines = []
        for code in self.corpus_errors:
            lines.append(f"error {code}")
        for outcome in self.case_outcomes:
            digest = ""
            if outcome.output_digest:
                digest = " outdigest=" + outcome.output_digest.split(":")[1][:12]
            reason = outcome.reason or "-"
            lines.append(
                f"case {outcome.case_id} corpus={outcome.corpus} "
                f"family={outcome.family} expect={outcome.expected} "
                f"result={outcome.result} reason={reason}{digest}"
            )
        bad = sum(0 if self._case_ok(o) else 1 for o in self.case_outcomes)
        status = "CLEAN" if (self.ok and not self.corpus_errors) else "FAILED"
        lines.append(
            f"summary cases={len(self.case_outcomes)} "
            f"ok={len(self.case_outcomes) - bad} bad={bad} "
            f"errors={len(self.corpus_errors)} status={status}"
        )
        return "\n".join(lines) + "\n"


REQUIRED_SEGMENT_FAMILIES = ("success", "failure", "recovery")
REQUIRED_Q29B_FAMILIES = (
    "baseline", "candidate", "overlap", "conflict", "late-output",
    "clock-exhausted", "extra-tool-call",
)


def _manifest_case_pathsafe(entry_path: str) -> bool:
    parts = Path(entry_path).parts
    return bool(entry_path) and not entry_path.startswith("/") and ".." not in parts


def validate_root(root) -> CorpusReport:
    root = Path(root)
    report = CorpusReport()
    try:
        _validate_root_inner(root, report)
    except CorpusError as exc:
        report.corpus_errors.append(exc.reason_code)
    return report


def _validate_root_inner(root: Path, report: CorpusReport) -> None:
    manifest_path = root / "manifest.json"
    _require(manifest_path.is_file(), "MANIFEST_MISSING")
    manifest = load_json_file(manifest_path)
    scan_data_purity(manifest, str(manifest_path))
    _require(manifest.get("schema_version") == MANIFEST_SCHEMA_VERSION,
             "MANIFEST_INVALID", "schema_version")
    _require(manifest.get("contract_schema_version") == CONTRACT_SCHEMA_VERSION,
             "MANIFEST_INVALID", "contract_schema_version")
    cases = manifest.get("cases")
    _require(isinstance(cases, list) and cases, "MANIFEST_INVALID", "cases")

    seen_ids = set()
    seen_paths = set()
    for entry in cases:
        for field in ("case_id", "corpus", "family", "path", "expected_outcome"):
            _require(field in entry, "MANIFEST_INVALID", f"case missing {field}")
        _require(entry["case_id"] not in seen_ids, "DUPLICATE_CASE_ID",
                 entry["case_id"])
        seen_ids.add(entry["case_id"])
        _require(_manifest_case_pathsafe(entry["path"]), "MANIFEST_INVALID",
                 f"unsafe path {entry['path']!r}")
        _require(entry["path"] not in seen_paths, "DUPLICATE_CASE_PATH",
                 entry["path"])
        seen_paths.add(entry["path"])
        _require(entry["corpus"] in ("segments", "q29b"), "MANIFEST_INVALID",
                 entry["corpus"])
        _require(entry["expected_outcome"] in ("accept", "reject", "reserved"),
                 "MANIFEST_INVALID", entry["expected_outcome"])
        if entry["expected_outcome"] == "reject":
            _require(isinstance(entry.get("expected_reason_code"), str)
                     and entry["expected_reason_code"], "MANIFEST_INVALID",
                     f"{entry['case_id']} reject needs expected_reason_code")

    segment_families = {e["family"] for e in cases if e["corpus"] == "segments"
                        and e["expected_outcome"] == "accept"}
    for family in REQUIRED_SEGMENT_FAMILIES:
        _require(family in segment_families, "MISSING_FAMILY",
                 f"segments/{family}")
    q29b_families = {e["family"] for e in cases if e["corpus"] == "q29b"
                     and e["expected_outcome"] == "accept"}
    for family in REQUIRED_Q29B_FAMILIES:
        _require(family in q29b_families, "MISSING_FAMILY", f"q29b/{family}")
    for family in manifest.get("q29b_reserved_families", []):
        reserved_dir = root / "q29b" / family
        _require(reserved_dir.is_dir(), "MISSING_FAMILY", f"reserved q29b/{family}")
        _require((reserved_dir / "README.md").is_file(), "RESERVED_FAMILY_INVALID",
                 f"q29b/{family} needs README")
        _require(
            not any(p.name == "runtime.json" for p in reserved_dir.rglob("*")),
            "RESERVED_FAMILY_INVALID", f"q29b/{family} must stay unimplemented",
        )

    # every on-disk case directory must be registered
    for corpus, marker in (("segments", "segment.json"), ("q29b", "runtime.json")):
        for marker_file in sorted((root / corpus).rglob(marker)):
            rel = marker_file.parent.relative_to(root).as_posix()
            _require(rel in seen_paths, "UNREGISTERED_CASE", rel)

    schema_dir = Path(__file__).resolve().parent / "schema"
    segment_schema = load_schema(schema_dir / "sealed-segment.schema.json")
    q29b_schema = load_schema(schema_dir / "q29b-runtime.schema.json")

    # phase 1: segments
    segment_index = {}
    for entry in cases:
        if entry["corpus"] != "segments":
            continue
        case_dir = root / entry["path"]
        _require(case_dir.is_dir(), "CASE_DIR_MISSING", entry["path"])
        parts = Path(entry["path"]).parts
        _require(len(parts) == 3 and parts[0] == "segments", "FAMILY_MISMATCH",
                 entry["path"])
        _require(parts[1] == entry["family"], "FAMILY_MISMATCH", entry["path"])
        outcome = validate_segment_case(case_dir, segment_schema)
        report.case_outcomes.append(
            CaseOutcome(entry["case_id"], "segments", entry["family"],
                        entry["expected_outcome"],
                        "accept" if outcome.accepted else "reject",
                        outcome.reason, outcome.detail, None,
                        expected_reason=entry.get("expected_reason_code"))
        )
        # index every recorded segment so that Q29-B resolution can
        # distinguish "not in the sealed corpus" (MISSING_SEALED_PATH) from
        # "present but its seal cannot be verified" (SEAL_DIGEST_MISMATCH)
        if outcome.segment is None:
            continue
        settled_state = outcome.segment.get("terminal_state") == "settled"
        entry = {
            "accepted": outcome.accepted,
            "settled": settled_state,
            "segment_ref": None,
            "paths": {},
            "path_seal_ref": None,
        }
        if settled_state and outcome.accepted:
            entry["segment_ref"] = outcome.seal["segment_ref"]
            entry["paths"] = {p["path_id"]: p
                              for p in outcome.segment["path_seal_body"]["paths"]}
            entry["path_seal_ref"] = {
                "id": outcome.seal["path_seal"]["seal_id"],
                "version": 1,
                "digest": outcome.seal["path_seal"]["seal_digest"],
            }
        segment_index[outcome.segment["segment_id"]] = entry

    # phase 2: q29b packets (resolve only against verified settled segments)
    for entry in cases:
        if entry["corpus"] != "q29b":
            continue
        if entry["expected_outcome"] == "reserved":
            reserved_dir = root / entry["path"]
            _require(reserved_dir.is_dir(), "CASE_DIR_MISSING", entry["path"])
            report.case_outcomes.append(
                CaseOutcome(entry["case_id"], "q29b", entry["family"], "reserved",
                            "reserved", None, "", None)
            )
            continue
        case_dir = root / entry["path"]
        _require(case_dir.is_dir(), "CASE_DIR_MISSING", entry["path"])
        parts = Path(entry["path"]).parts
        _require(len(parts) == 3 and parts[0] == "q29b", "FAMILY_MISMATCH",
                 entry["path"])
        _require(parts[1] == entry["family"], "FAMILY_MISMATCH", entry["path"])
        outcome = validate_q29b_case(case_dir, q29b_schema, segment_index)
        report.case_outcomes.append(
            CaseOutcome(entry["case_id"], "q29b", entry["family"],
                        entry["expected_outcome"],
                        "accept" if outcome.accepted else "reject",
                        outcome.reason, outcome.detail, outcome.output_digest,
                        expected_reason=entry.get("expected_reason_code"))
        )
        if outcome.accepted:
            report.q29b_output_digests[entry["case_id"]] = outcome.output_digest
            report.q29b_terminal_records[entry["case_id"]] = outcome.terminal_record


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the FND-002 recorded sealed-segment/Q29-B corpus.")
    parser.add_argument("--root", required=True,
                        help="path to the recorded/ fixture root")
    args = parser.parse_args(argv)
    report = validate_root(args.root)
    sys.stdout.write(report.render())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
