#!/usr/bin/env python3
"""FND-001 shared JCS/SHA-256 golden corpus validator (Python stdlib only).

This validator is the reference checker for the single canonicalization
golden source consumed by Go (GMS/Host), TypeScript (RSIH) and Python
runners. It reads `$FIX/manifest.json` (Contract §16.2), walks every declared
case, derives accept/reject and the reason code INDEPENDENTLY of the golden
expectations, and only then compares its derivation against `expected.json`
(Contract §16.3). It never writes, rewrites or "self-updates" any expected
value; it is a read-only verifier.

Canonicalization model (Contract §6.1, RFC 8785)
------------------------------------------------
- Hashed core bytes are UTF-8 JSON without BOM.
- Canonical form is RFC 8785 JCS with these pinned behaviors:
  * object keys sorted by UTF-16 code units (not codepoints, not UTF-8 bytes);
  * string escaping only for `"` and `\\` and U+0000..U+001F (short forms
    \\b \\t \\n \\f \\r, otherwise \\u00xx with lowercase hex); U+007F and all
    non-ASCII characters are emitted as raw UTF-8, never \\u-escaped;
  * no Unicode normalization is applied at any point (no NFC/NFKC);
  * no insignificant whitespace.
- Numbers: the hashed core is integer-only (Contract §6.1.4). Any JSON number
  that is not a plain integer - including fraction or exponent forms such as
  `1.0` or `1e2`, even when integer-valued - fails closed with
  NON_INTEGER_NUMBER. Integers are serialized as exact decimal digits, so
  integers beyond 2^53 keep full precision (fixtures pin this for
  cross-language runners, which must use big-integer handling).
- DTO-level normalization BEFORE JCS (Contract §6.3): a `source_skill_refs`
  array of exactly two SkillArtifactRefs is sorted ascending by
  (lineage_id UTF-8 bytes, version, artifact_digest). This is how the
  SimilarityAssessment A+B / B+A pair-normalization fixtures (§16.4 #11)
  canonicalize to identical bytes.

Derived evaluation order (per case, fail-closed at the first violation)
------------------------------------------------------------------------
 1. source bytes start with a UTF-8 BOM  -> BOM_NOT_ALLOWED (not canonicalizable)
 2. source is not valid UTF-8/JSON       -> INVALID_JSON   (not canonicalizable)
 3. canonicalization hits a non-integer  -> NON_INTEGER_NUMBER (not canonicalizable)
 4. semantic checks (see below)          -> the matching closed reason code
 5. canonical.utf8 file bytes differ from the canonicalization of the source
                                          -> DIGEST_MISMATCH
 6. otherwise                            -> accepted

Semantic checks, in evaluation order within step 4 (documented dispatch)
------------------------------------------------------------------------
Generic (all categories):
- extensions walk (Contract §6.4/§1.3.3): an extension entry with
  `required: true` whose key is not in KNOWN_EXTENSIONS_V1
  -> UNKNOWN_REQUIRED_EXTENSION. Unknown *optional* extensions are ignored
  (they MAY be ignored) and the case stays accepted.
- evidence commit-state walk (Contract §7.7/§13.7): any object carrying
  `commit_state` with a value outside {committed, sealed}
  -> EVIDENCE_NOT_COMMITTED.

Category `ref` (Contract §6.2, §5.1.5):
- exact-ref shape: Graph node form (`graph_node_id`/`node_id`), `latest` or
  non-integer version, or a naked id (identifier without version and/or
  digest) -> NON_EXACT_REF. CandidateArtifactRef is exact via
  (candidate_id, body_digest) and intentionally carries no version (§7.4).
- candidate/released body equality (§16.4 #7): when a value contains both
  `candidate_ref` and `released_ref`, candidate body_digest must equal
  released artifact_digest -> else DIGEST_MISMATCH.

Category `artifact` (Contract §8, §13.7, §14.2):
- kind drift: two SkillArtifactRefs sharing (lineage_id, version,
  artifact_digest) with different `kind` -> REF_MISMATCH (§6.2).
- composite child port completeness (§8.4): a child entry missing
  `input_port` or `output_port` -> PORT_SCHEMA_MISSING.
- composite DAG acyclicity (§5.2.3/§8.4): a cycle over child edges
  -> COMPOSITE_CYCLE.
- permission cap: any permission/orchestration_permissions capability outside
  HOST_CAPABILITIES_V1 (the v1 Host tool surface, Contract §7.17)
  -> PERMISSION_CAP_EXCEEDED.

Category `event` (Contract §7.13, §7.23, §9.2-§9.4):
- projection batch (a JSON array of events carrying `activation_sequence`):
  same sequence with different `event_digest` -> PROJECTION_EVENT_CONFLICT
  (§9.3); non-contiguous unique sequences -> PROJECTION_SEQUENCE_GAP (§9.3);
  duplicated identical events are idempotent and accepted (§13.2).
- MergeProposalEvent transition outside LEGAL_PROPOSAL_TRANSITIONS or out of
  a terminal state (§9.4) -> ILLEGAL_STATE_TRANSITION.

Category `merge` (Contract §5.5, §7.21-§7.22, §15.2 MT1/MT3):
- SimilarityAssessment with band `below_suggestion`
  -> SIMILARITY_BELOW_THRESHOLD (below-threshold must not auto-admit).
- a conflict with `blocking: true` and resolution action `unresolved`
  -> MERGE_BLOCKING_CONFLICT.

Category `negative` / `canonicalization`: only the generic checks plus the
byte-level digest check apply.

Reason-code registry
--------------------
CONTRACT_REASON_CODES are defined by the System Contract (§6.2, §9.3, §13.7,
§16.4). EXTENSION_REASON_CODES exist because the fixtures must also pin
fail-closed behaviors that Contract §16.4 does not assign a code
(BOM §6.1.1, malformed JSON, §9.4 terminal-state transitions, §15.2 MT1
below-threshold admission); they are fixture-level extensions until the
system reason-code registry policy (CTR-004) centralizes them. The registry
is closed: an expected reason outside it is a corpus error.

File conventions (declared in schema/conformance-manifest.schema.json)
----------------------------------------------------------------------
- `<case>/source.json`: the case input, arbitrary valid JSON formatting
  (key order, whitespace and \\u-escaping variations are the point of several
  canonicalization cases); UTF-8 without BOM except the dedicated BOM
  negative case; single trailing newline.
- `<case>/canonical.utf8`: EXACT canonical bytes, no BOM, no trailing
  newline - except the digest-tamper negative where the file itself is the
  tampered artifact (its digest no longer matches the canonicalization of the
  source, which is exactly the DIGEST_MISMATCH fixture). Cases that are not
  canonicalizable (BOM/JSON/non-integer negatives) have a 0-byte file.
- `<case>/canonical.base64`: standard Base64 of canonical.utf8, single line,
  trailing newline (0-byte file when canonical.utf8 is empty). Decoding it
  must reproduce canonical.utf8 byte for byte (Contract §16.1).
- `<case>/expected.json` and `manifest.json`: canonical JSON (this JCS) plus
  a single trailing newline.
- expected.json carries expected_digest / expected_canonical_byte_length
  exactly when the case is canonicalizable; for the digest-tamper negative
  they describe the UNTAMPERED canonicalization of the source.

CLI
---
    python3 validate.py --root <conformance-dir>

Exit codes: 0 all cases pass; 1 at least one case fails; 2 corpus/manifest
integrity error. Output is deterministic (manifest order), so reruns are
bit-identical.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

MANIFEST_SCHEMA_VERSION = "rsih-skill-evolution.conformance-manifest.v1"
EXPECTED_SCHEMA_VERSION = "rsih-skill-evolution.conformance-expected.v1"
CONTRACT_SCHEMA_VERSION = "rsih-skill-evolution.system-contract.v1"

CATEGORIES = ("canonicalization", "ref", "artifact", "event", "merge", "negative")
CATEGORY_DIRS = {
    "canonicalization": "canonicalization",
    "ref": "refs",
    "artifact": "artifacts",
    "event": "events",
    "merge": "merge",
    "negative": "negative",
}
CASE_FILES = ("source.json", "canonical.utf8", "canonical.base64", "expected.json")

CONTRACT_REASON_CODES = frozenset(
    {
        "NON_INTEGER_NUMBER",        # Contract §6.1.4, §13.7, §16.4
        "UNKNOWN_REQUIRED_EXTENSION",  # Contract §1.3.3, §6.4, §13.7, §16.4
        "DIGEST_MISMATCH",           # Contract §6.2, §13.7, §16.4
        "REF_MISMATCH",              # Contract §6.2
        "NON_EXACT_REF",             # Contract §5.1.5, §13.7, §16.4
        "COMPOSITE_CYCLE",           # Contract §8.4, §13.7, §16.4
        "PORT_SCHEMA_MISSING",       # Contract §5.2.5, §13.7, §16.4
        "MERGE_BLOCKING_CONFLICT",   # Contract §9.4.4, §13.7, §16.4
        "EVIDENCE_NOT_COMMITTED",    # Contract §7.7, §13.7, §16.4
        "PROJECTION_SEQUENCE_GAP",   # Contract §9.3, §13.6, §13.7, §16.4
        "PERMISSION_CAP_EXCEEDED",   # Contract §14.2, §13.7, §16.4
        "PROJECTION_EVENT_CONFLICT",  # Contract §9.3
    }
)
EXTENSION_REASON_CODES = frozenset(
    {
        "BOM_NOT_ALLOWED",            # Contract §6.1.1 (no code assigned by §16.4)
        "INVALID_JSON",               # Contract §6.1.1/§6.1.2 precondition
        "ILLEGAL_STATE_TRANSITION",   # Contract §9.4 (terminal states must not reopen)
        "SIMILARITY_BELOW_THRESHOLD",  # Contract §15.2 MT1 negative path
    }
)
REASON_CODES = CONTRACT_REASON_CODES | EXTENSION_REASON_CODES

# v1 conformance policy constants (documented, versioned with this file):
KNOWN_EXTENSIONS_V1 = frozenset(
    {
        # No required extension is registered in v1; the mechanism exists so
        # required:true + unknown key always fails closed (Contract §1.3.3).
    }
)
HOST_CAPABILITIES_V1 = frozenset(
    {
        # v1 Host tool surface (Contract §7.17) doubles as the Host authority
        # capability cap for fixture permission checking (Contract §14.2).
        "memory_explore",
        "memory_expand",
        "skill_get",
    }
)
EVIDENCE_COMMIT_STATES = frozenset({"committed", "sealed"})

BOM = b"\xef\xbb\xbf"

# Contract §9.4 MergeProposal state machine (terminal states have no exits).
LEGAL_PROPOSAL_TRANSITIONS = frozenset(
    {
        ("none", "proposed"),
        ("proposed", "admitted"),
        ("proposed", "duplicate"),
        ("proposed", "rejected"),
        ("proposed", "stale"),
        ("proposed", "withdrawn"),
        ("admitted", "synthesizing"),
        ("admitted", "rejected"),
        ("admitted", "stale"),
        ("admitted", "withdrawn"),
        ("synthesizing", "candidate_bound"),
        ("synthesizing", "inconclusive"),
        ("synthesizing", "rejected"),
        ("synthesizing", "stale"),
        ("candidate_bound", "validating"),
        ("candidate_bound", "stale"),
        ("validating", "replaying"),
        ("validating", "rejected"),
        ("validating", "stale"),
        ("replaying", "decision_pending"),
        ("replaying", "rejected"),
        ("replaying", "inconclusive"),
        ("replaying", "stale"),
        ("decision_pending", "activation_pending"),
        ("decision_pending", "rejected"),
        ("decision_pending", "inconclusive"),
        ("activation_pending", "released"),
        ("activation_pending", "rejected"),
        ("activation_pending", "stale"),
    }
)


class CanonicalizationError(Exception):
    """Raised when a value cannot enter the integer-only hashed core."""

    def __init__(self, reason_code: str):
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True)
class Outcome:
    """Derived evaluation result, computed independently of expected.json."""

    accept: bool
    reason_code: "str | None"
    canonical: "bytes | None"  # canonical bytes when the case is canonicalizable


@dataclass
class CaseResult:
    case_id: str
    ok: bool
    message: str
    digest: str = "-"
    length: int = -1
    accept: "str | None" = None  # derived accept as "true"/"false" (None = undetermined)
    reason: str = "-"


# ---------------------------------------------------------------------------
# RFC 8785 / Contract §6.1 canonicalization
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
    """Pure RFC 8785 JCS canonicalization, integer-only (Contract §6.1.4)."""
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


def normalize_for_hashing(value):
    """Contract §6.3 symmetric-pair normalization applied before JCS."""
    if isinstance(value, dict):
        out = {key: normalize_for_hashing(item) for key, item in value.items()}
        refs = out.get("source_skill_refs")
        if (
            isinstance(refs, list)
            and len(refs) == 2
            and all(
                isinstance(item, dict) and isinstance(item.get("lineage_id"), str)
                for item in refs
            )
        ):
            out["source_skill_refs"] = sorted(
                refs,
                key=lambda item: (
                    item["lineage_id"].encode("utf-8"),
                    item.get("version", 0),
                    item.get("artifact_digest", ""),
                ),
            )
        return out
    if isinstance(value, list):
        return [normalize_for_hashing(item) for item in value]
    return value


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Semantic checks (fail-closed, Contract §13.7)
# ---------------------------------------------------------------------------


def iter_dicts(value):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from iter_dicts(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_dicts(item)


def _is_int(node) -> bool:
    return isinstance(node, int) and not isinstance(node, bool)


def _extensions_issue(value):
    for holder in iter_dicts(value):
        extensions = holder.get("extensions")
        if not isinstance(extensions, dict):
            continue
        for key, entry in extensions.items():
            if (
                isinstance(entry, dict)
                and entry.get("required") is True
                and key not in KNOWN_EXTENSIONS_V1
            ):
                return "UNKNOWN_REQUIRED_EXTENSION"
    return None


def _commit_state_issue(value):
    for holder in iter_dicts(value):
        if "commit_state" in holder and holder["commit_state"] not in EVIDENCE_COMMIT_STATES:
            return "EVIDENCE_NOT_COMMITTED"
    return None


def _ref_shape_issue(value):
    for holder in iter_dicts(value):
        if "graph_node_id" in holder or "node_id" in holder:
            return "NON_EXACT_REF"  # Graph internal form used as a ref (§5.1.5)
        if "candidate_id" in holder:
            if "body_digest" not in holder:
                return "NON_EXACT_REF"  # naked candidate id (exact via candidate_id+body_digest)
            continue
        if "lineage_id" in holder:
            if not _is_int(holder.get("version")):
                return "NON_EXACT_REF"  # missing version or `latest`
            if "artifact_digest" not in holder:
                return "NON_EXACT_REF"  # naked lineage name
            continue
        if "evidence_id" in holder:
            if not _is_int(holder.get("version")) or "evidence_digest" not in holder:
                return "NON_EXACT_REF"
            continue
        if "id" in holder:
            if not _is_int(holder.get("version")) or "digest" not in holder:
                return "NON_EXACT_REF"  # naked generic id / latest version
    return None


def _candidate_released_equality_issue(value):
    if not isinstance(value, dict):
        return None
    candidate = value.get("candidate_ref")
    released = value.get("released_ref")
    if isinstance(candidate, dict) and isinstance(released, dict):
        if candidate.get("body_digest") != released.get("artifact_digest"):
            return "DIGEST_MISMATCH"  # §16.4 #7: same body digest required
    return None


def _kind_drift_issue(value):
    groups: "dict[tuple, set]" = {}
    for holder in iter_dicts(value):
        if holder.get("schema_version") == "gms.skill-artifact-ref.v2":
            key = (
                holder.get("lineage_id"),
                holder.get("version"),
                holder.get("artifact_digest"),
            )
            groups.setdefault(key, set()).add(holder.get("kind"))
    for kinds in groups.values():
        if len(kinds) > 1:
            return "REF_MISMATCH"  # same exact identity, inconsistent field (§6.2)
    return None


def _has_cycle(nodes, adjacency) -> bool:
    color = {node: 0 for node in nodes}  # 0 white, 1 gray, 2 black

    def visit(node) -> bool:
        color[node] = 1
        for nxt in adjacency.get(node, ()):
            if nxt not in color:
                continue
            if color[nxt] == 1:
                return True
            if color[nxt] == 0 and visit(nxt):
                return True
        color[node] = 2
        return False

    return any(color[node] == 0 and visit(node) for node in sorted(nodes, key=str))


def _composite_ports_issue(value):
    for holder in iter_dicts(value):
        children = holder.get("children")
        if isinstance(children, list) and isinstance(holder.get("edges"), list):
            for child in children:
                if isinstance(child, dict) and (
                    "input_port" not in child or "output_port" not in child
                ):
                    return "PORT_SCHEMA_MISSING"  # §5.2.5/§8.4
    return None


def _composite_cycle_issue(value):
    for holder in iter_dicts(value):
        children = holder.get("children")
        edges = holder.get("edges")
        if isinstance(children, list) and isinstance(edges, list):
            child_ids = {
                child.get("child_id")
                for child in children
                if isinstance(child, dict)
            }
            adjacency: "dict[object, list]" = {}
            for edge in edges:
                if isinstance(edge, dict):
                    adjacency.setdefault(edge.get("from_child_id"), []).append(
                        edge.get("to_child_id")
                    )
            if _has_cycle(child_ids, adjacency):
                return "COMPOSITE_CYCLE"  # §5.2.3/§8.4
    return None


def _permission_cap_issue(value):
    for holder in iter_dicts(value):
        for key in ("permissions", "orchestration_permissions"):
            entries = holder.get(key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if (
                    isinstance(entry, dict)
                    and entry.get("capability") not in HOST_CAPABILITIES_V1
                ):
                    return "PERMISSION_CAP_EXCEEDED"  # §14.2
    return None


def _projection_batches(value):
    if isinstance(value, list):
        yield value
    elif isinstance(value, dict) and isinstance(value.get("events"), list):
        yield value["events"]


def _projection_conflict_issue(value):
    for batch in _projection_batches(value):
        events = [
            item
            for item in batch
            if isinstance(item, dict) and _is_int(item.get("activation_sequence"))
        ]
        if len(events) < 2 or len(events) != len(batch):
            continue
        by_sequence: "dict[int, set]" = {}
        for event in events:
            by_sequence.setdefault(event["activation_sequence"], set()).add(
                event.get("event_digest")
            )
        if any(len(digests) > 1 for digests in by_sequence.values()):
            return "PROJECTION_EVENT_CONFLICT"  # §9.3: same sequence, different digest
    return None


def _projection_gap_issue(value):
    for batch in _projection_batches(value):
        events = [
            item
            for item in batch
            if isinstance(item, dict) and _is_int(item.get("activation_sequence"))
        ]
        if len(events) < 2 or len(events) != len(batch):
            continue
        sequences = sorted({event["activation_sequence"] for event in events})
        if any(b - a != 1 for a, b in zip(sequences, sequences[1:])):
            return "PROJECTION_SEQUENCE_GAP"  # §9.3: consume contiguously, stop on gap
    return None


def _proposal_transition_issue(value):
    for holder in iter_dicts(value):
        if holder.get("schema_version") == "gms.merge-proposal-event.v1":
            transition = (holder.get("from_state"), holder.get("to_state"))
            if transition not in LEGAL_PROPOSAL_TRANSITIONS:
                return "ILLEGAL_STATE_TRANSITION"  # §9.4 (terminal states never reopen)
    return None


def _below_threshold_issue(value):
    for holder in iter_dicts(value):
        if (
            holder.get("schema_version") == "gms.similarity-assessment.v1"
            and holder.get("band") == "below_suggestion"
        ):
            return "SIMILARITY_BELOW_THRESHOLD"  # §15.2 MT1 negative path
    return None


def _blocking_conflict_issue(value):
    for holder in iter_dicts(value):
        conflicts = holder.get("conflicts")
        if not isinstance(conflicts, list):
            continue
        for conflict in conflicts:
            if not isinstance(conflict, dict) or conflict.get("blocking") is not True:
                continue
            resolution = conflict.get("resolution")
            if isinstance(resolution, dict) and resolution.get("action") == "unresolved":
                return "MERGE_BLOCKING_CONFLICT"  # §9.4.4/§13.7
    return None


_GENERIC_CHECKS = (_extensions_issue, _commit_state_issue)
_CATEGORY_CHECKS = {
    "canonicalization": (),
    "ref": (_ref_shape_issue, _candidate_released_equality_issue),
    "artifact": (
        _kind_drift_issue,
        _composite_ports_issue,
        _composite_cycle_issue,
        _permission_cap_issue,
    ),
    "event": (
        _projection_conflict_issue,
        _projection_gap_issue,
        _proposal_transition_issue,
    ),
    "merge": (_below_threshold_issue, _blocking_conflict_issue),
    "negative": (),
}


def _semantic_issue(value, category: str):
    for check in _GENERIC_CHECKS + _CATEGORY_CHECKS[category]:
        code = check(value)
        if code is not None:
            return code
    return None


def derive_outcome(source_bytes: bytes, category: str, canonical_file_bytes: bytes) -> Outcome:
    """Derive (accept, reason, canonical bytes) from the case input alone."""
    if source_bytes.startswith(BOM):
        return Outcome(False, "BOM_NOT_ALLOWED", None)
    try:
        parsed = json.loads(source_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return Outcome(False, "INVALID_JSON", None)
    try:
        canonical = jcs(normalize_for_hashing(parsed))
    except CanonicalizationError as exc:
        return Outcome(False, exc.reason_code, None)
    issue = _semantic_issue(parsed, category)
    if issue is not None:
        return Outcome(False, issue, canonical)
    if canonical_file_bytes != canonical:
        return Outcome(False, "DIGEST_MISMATCH", canonical)
    return Outcome(True, None, canonical)


# ---------------------------------------------------------------------------
# Corpus / manifest validation
# ---------------------------------------------------------------------------


def _read_bytes(path: Path):
    return path.read_bytes()


def _parse_json_bytes(data: bytes, what: str, errors: list):
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        errors.append("%s is not valid UTF-8 JSON: %s" % (what, exc))
        return None


def _safe_relative(root: Path, relative: str) -> "Path | None":
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts or "\\" in relative:
        return None
    resolved = root / candidate
    try:
        resolved.resolve().relative_to(root.resolve())
    except ValueError:
        return None
    return resolved


def _load_manifest(root: Path, errors: list):
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        errors.append("manifest.json missing under %s" % root)
        return None
    manifest = _parse_json_bytes(_read_bytes(manifest_path), "manifest.json", errors)
    if manifest is None:
        return None
    if not isinstance(manifest, dict):
        errors.append("manifest.json must be a JSON object")
        return None
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        errors.append(
            "manifest schema_version must be %r, got %r"
            % (MANIFEST_SCHEMA_VERSION, manifest.get("schema_version"))
        )
    if manifest.get("contract_schema_version") != CONTRACT_SCHEMA_VERSION:
        errors.append(
            "manifest contract_schema_version must be %r, got %r"
            % (CONTRACT_SCHEMA_VERSION, manifest.get("contract_schema_version"))
        )
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        errors.append("manifest cases must be a non-empty array")
        return None
    return manifest


def _validate_manifest_entries(root: Path, manifest, errors: list):
    required_paths = (
        "source_path",
        "canonical_utf8_path",
        "canonical_base64_path",
        "expected_path",
    )
    entries = []
    seen_ids: "set[str]" = set()
    for index, raw in enumerate(manifest["cases"]):
        if not isinstance(raw, dict):
            errors.append("manifest cases[%d] must be an object" % index)
            continue
        case_id = raw.get("case_id")
        category = raw.get("category")
        if not isinstance(case_id, str) or not case_id:
            errors.append("manifest cases[%d] has invalid case_id" % index)
            continue
        if case_id in seen_ids:
            errors.append("duplicate case id in manifest: %s" % case_id)
            continue
        seen_ids.add(case_id)
        if category not in CATEGORIES:
            errors.append(
                "case %s has unknown category %r (expected one of %s)"
                % (case_id, category, "|".join(CATEGORIES))
            )
            continue
        paths_ok = True
        for key in required_paths:
            value = raw.get(key)
            if not isinstance(value, str) or not value:
                errors.append("case %s missing %s" % (case_id, key))
                paths_ok = False
        if not paths_ok:
            continue
        path_map = {}
        for key in required_paths:
            resolved = _safe_relative(root, raw[key])
            if resolved is None:
                errors.append(
                    "case %s %s is not a safe relative path: %r" % (case_id, key, raw[key])
                )
                paths_ok = False
                continue
            if not resolved.is_file():
                errors.append("case %s %s does not exist: %s" % (case_id, key, raw[key]))
                paths_ok = False
            path_map[key] = resolved
        if not paths_ok:
            continue
        expected_dir = CATEGORY_DIRS[category]
        if not raw["source_path"].startswith(expected_dir + "/"):
            errors.append(
                "case %s category %r must live under %s/" % (case_id, category, expected_dir)
            )
            continue
        entries.append(
            {
                "case_id": case_id,
                "category": category,
                "source_path": raw["source_path"],
                "canonical_utf8_path": raw["canonical_utf8_path"],
                "canonical_base64_path": raw["canonical_base64_path"],
                "expected_path": raw["expected_path"],
                "mirrors": {
                    key: raw[key]
                    for key in (
                        "expected_accept",
                        "expected_digest",
                        "expected_canonical_byte_length",
                        "expected_reason_code",
                    )
                    if key in raw
                },
            }
        )
    # Corpus completeness: every case directory must be declared exactly once
    # and contain all four files (Contract §16.1/§16.2).
    declared_dirs = {
        "/".join(entry["source_path"].split("/")[:-1]) for entry in entries
    }
    for category, dirname in sorted(CATEGORY_DIRS.items()):
        base = root / dirname
        if not base.is_dir():
            if any(entry["category"] == category for entry in entries):
                errors.append("category directory missing: %s/" % dirname)
            continue  # no declared cases and no directory: partial corpus is fine
        for child in sorted(base.iterdir()):
            if not child.is_dir():
                continue
            rel = "%s/%s" % (dirname, child.name)
            if rel not in declared_dirs:
                errors.append("undeclared case directory not in manifest: %s" % rel)
                continue
            for name in CASE_FILES:
                if not (child / name).is_file():
                    errors.append("case directory %s missing %s" % (rel, name))
    return entries


def _check_expected_shape(entry, expected):
    """Return a list of problems with expected.json itself (corpus errors)."""
    problems = []
    if not isinstance(expected, dict):
        return ["%s: expected.json must be a JSON object" % entry["case_id"]]
    if expected.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        problems.append(
            "%s: expected schema_version must be %r"
            % (entry["case_id"], EXPECTED_SCHEMA_VERSION)
        )
    if expected.get("case_id") != entry["case_id"]:
        problems.append(
            "%s: expected case_id %r does not match manifest" % (entry["case_id"], expected.get("case_id"))
        )
    if not isinstance(expected.get("expected_accept"), bool):
        problems.append("%s: expected_accept must be a boolean" % entry["case_id"])
    reason = expected.get("expected_reason_code")
    if expected.get("expected_accept") is False:
        if not isinstance(reason, str) or reason not in REASON_CODES:
            problems.append(
                "%s: rejected case must carry expected_reason_code from the closed registry"
                % entry["case_id"]
            )
    elif reason is not None:
        problems.append(
            "%s: accepted case must not carry expected_reason_code" % entry["case_id"]
        )
    return problems


def _check_manifest_mirrors(entry, expected, errors):
    for key, value in entry["mirrors"].items():
        if key in expected and expected[key] != value:
            errors.append(
                "case %s: manifest mirror %s=%r disagrees with expected.json %r"
                % (entry["case_id"], key, value, expected[key])
            )


def _check_digest_expectations(expected, outcome, canonical_file_bytes, problems):
    """Digest/length expectations: present exactly when canonicalizable.

    For the digest-tamper fixture the expectations describe the UNTAMPERED
    canonicalization of the source (the truth the file no longer matches).
    """
    if outcome.canonical is not None:
        expected_digest = expected.get("expected_digest")
        expected_length = expected.get("expected_canonical_byte_length")
        if not isinstance(expected_digest, str):
            problems.append("canonicalizable case missing expected_digest")
        elif expected_digest != digest_bytes(outcome.canonical):
            problems.append(
                "expected_digest %s != derived %s"
                % (expected_digest, digest_bytes(outcome.canonical))
            )
        if not _is_int(expected_length):
            problems.append("canonicalizable case missing expected_canonical_byte_length")
        elif expected_length != len(outcome.canonical):
            problems.append(
                "expected_canonical_byte_length %r != derived %d"
                % (expected_length, len(outcome.canonical))
            )
    else:
        if "expected_digest" in expected or "expected_canonical_byte_length" in expected:
            problems.append("non-canonicalizable case must not declare digest/length")
        if canonical_file_bytes != b"":
            problems.append("non-canonicalizable case must have empty canonical files")


def _check_outcome_parity(expected, outcome, canonical_file_bytes, problems):
    """Derived accept/reject and reason must match the golden expectations."""
    if outcome.accept != expected["expected_accept"]:
        problems.append(
            "derived accept=%s (reason %s) but expected accept=%s"
            % (
                str(outcome.accept).lower(),
                outcome.reason_code,
                expected["expected_accept"],
            )
        )
    elif not outcome.accept:
        if outcome.reason_code != expected["expected_reason_code"]:
            problems.append(
                "derived reason %s != expected reason %s"
                % (outcome.reason_code, expected["expected_reason_code"])
            )
        # Except for the digest-tamper fixture itself, the canonical file must
        # still faithfully hold the canonicalization of the source.
        if (
            outcome.reason_code != "DIGEST_MISMATCH"
            and outcome.canonical is not None
            and canonical_file_bytes != outcome.canonical
        ):
            problems.append("canonical.utf8 bytes do not match canonicalized source")


def _evaluate_entry(root: Path, entry) -> CaseResult:
    case_id = entry["case_id"]
    source_bytes = _read_bytes(root / entry["source_path"])
    canonical_file_bytes = _read_bytes(root / entry["canonical_utf8_path"])
    base64_text = _read_bytes(root / entry["canonical_base64_path"])

    problems: "list[str]" = []

    # Base64 sidecar must decode to exactly the canonical.utf8 bytes (§16.1).
    try:
        decoded = base64.b64decode(base64_text.strip(), validate=True)
    except Exception as exc:  # binascii.Error
        problems.append("canonical.base64 is not valid Base64: %s" % exc)
        decoded = None
    if decoded is not None and decoded != canonical_file_bytes:
        problems.append("canonical.base64 does not decode to canonical.utf8 bytes")

    if canonical_file_bytes.startswith(BOM):
        problems.append("canonical.utf8 must not start with a UTF-8 BOM")

    expected = _parse_json_bytes(
        _read_bytes(root / entry["expected_path"]),
        "%s expected.json" % case_id,
        problems,
    )
    if expected is None:
        return CaseResult(case_id, False, "; ".join(problems) or "expected.json unreadable")

    problems.extend(_check_expected_shape(entry, expected))
    _check_manifest_mirrors(entry, expected, problems)

    outcome = derive_outcome(source_bytes, entry["category"], canonical_file_bytes)

    _check_digest_expectations(expected, outcome, canonical_file_bytes, problems)
    _check_outcome_parity(expected, outcome, canonical_file_bytes, problems)

    digest = digest_bytes(outcome.canonical) if outcome.canonical is not None else "-"
    length = len(outcome.canonical) if outcome.canonical is not None else -1
    reason = outcome.reason_code if outcome.reason_code is not None else "-"
    return CaseResult(
        case_id,
        not problems,
        "; ".join(problems),
        digest,
        length,
        "true" if outcome.accept else "false",
        reason,
    )


def validate_corpus(root: Path):
    """Validate the corpus under root; return (corpus_errors, case_results)."""
    errors: "list[str]" = []
    results: "list[CaseResult]" = []
    manifest = _load_manifest(root, errors)
    if manifest is None:
        return errors, results
    entries = _validate_manifest_entries(root, manifest, errors)
    for entry in entries:
        results.append(_evaluate_entry(root, entry))
    return errors, results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the FND-001 JCS/SHA-256 golden conformance corpus."
    )
    parser.add_argument("--root", required=True, type=Path, help="conformance directory")
    args = parser.parse_args(argv)

    root: Path = args.root
    if not root.is_dir():
        print("ERROR: --root %s is not a directory" % root)
        return 2

    errors, results = validate_corpus(root)
    for error in errors:
        print("ERROR: %s" % error)
    for result in results:
        if result.ok:
            print(
                "PASS %s accept=%s digest=%s length=%d reason=%s"
                % (
                    result.case_id,
                    result.accept if result.accept is not None else "-",
                    result.digest,
                    max(result.length, 0),
                    result.reason,
                )
            )
        else:
            print("FAIL %s: %s" % (result.case_id, result.message))
    failed = [result for result in results if not result.ok]
    print(
        "SUMMARY: cases=%d passed=%d failed=%d corpus_errors=%d"
        % (len(results), len(results) - len(failed), len(failed), len(errors))
    )
    if errors:
        return 2
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
