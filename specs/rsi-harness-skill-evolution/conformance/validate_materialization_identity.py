#!/usr/bin/env python3
"""CTR-001 materialization exact identity validator (Contract 6.2.1 v1.1).

Validates the golden corpus under ``conformance/materialization/`` and freezes
the three-layer identity rules of System Contract 6.2.1 (revision v1.1,
CTR-001). stdlib only. Read-only: it never writes or self-updates fixtures.

Three-layer identity (Contract 6.2.1 v1.1)
------------------------------------------
- Layer 1 - bundle bytes: the content-addressed files themselves.
  ``content_digest = SHA-256(file bytes)`` per file;
  ``bundle_digest = SHA-256(JCS([{content_digest, path}] sorted by path))``
  is the bundle rollup / CAS address derived from the file list alone.
- Layer 2 - manifest: the closed manifest document (schema
  ``rsih.materialization-manifest.v1`` plus the v1.1 fields
  manifest_version/skill/producer/files.size_bytes/file_count/total_bytes).
  ``manifest_digest = SHA-256(JCS(manifest_document))``. Any preimage field
  change changes the digest; same (materialization_id, manifest_version)
  admits exactly one manifest_digest.
- Layer 3 - lock: ``SkillLock.materialization_ref`` IS the manifest exact
  identity ``(materialization_id, manifest_version, manifest_digest)`` plus
  the freeze-time activation sequence and session id. The lock carries its
  own ``lock_digest = SHA-256(JCS(lock_document minus lock_digest))``.

The three digests never substitute for each other. In particular
``bundle_digest`` (or any per-file content digest) in
``materialization_ref.digest`` fails with BUNDLE_DIGEST_NOT_MANIFEST_REF.

Fail-closed derivation order (first violation wins)
---------------------------------------------------
 1. BOM in a JSON source                  -> BOM_NOT_ALLOWED
 2. source not valid UTF-8/JSON           -> INVALID_JSON
 3. unknown required extension (6.4)      -> UNKNOWN_REQUIRED_EXTENSION
 4. manifest_version / ref version missing
    or not an integer >= 1                -> MANIFEST_VERSION_MISSING
 5. manifest closure inconsistencies:
    file_count != len(files), total_bytes != sum(size_bytes),
    duplicate/unsorted/unsafe file paths, root count != 1, skill != root,
    root not in transitive, transitive unsorted/duplicated
                                           -> MANIFEST_PARTIAL
 6. same (materialization_id, manifest_version) with a different
    manifest_digest (manifest-document-conflicting.json present)
                                           -> SAME_REF_DIFFERENT_BYTES
 7. bundle bytes layer: declared file missing from bundle/, undeclared
    extra file in bundle/, content digest or size mismatch,
    bundle_digest rollup mismatch         -> DIGEST_MISMATCH
    (declared-vs-bundle set mismatch      -> MANIFEST_PARTIAL)
 8. lock ref id/version != manifest       -> REF_MISMATCH
 9. lock ref digest == bundle_digest or any per-file content digest
                                           -> BUNDLE_DIGEST_NOT_MANIFEST_REF
10. lock ref digest != SHA-256(JCS(manifest_document))
                                           -> MANIFEST_DIGEST_MISMATCH
11. activation_sequence_at_freeze != created_from_activation_sequence
                                           -> MANIFEST_SEQUENCE_STALE
12. lock closure misses a manifest-declared file/skill, or an entry
    references an undeclared path/skill   -> LOCK_CLOSURE_INCOMPLETE
13. lock_digest mismatch                  -> DIGEST_MISMATCH
14. otherwise                             -> accepted

Legacy compatibility (6.2.1 v1.1): a manifest document without
``manifest_version`` is the pre-v1.1 Contract 7.19-only shape. It is
classified ``legacy_unlocked`` and fails closed with
MANIFEST_VERSION_MISSING; such locks stay readable as history but MUST NOT
publish, freeze or cache-serve, and are never auto-upgraded.

Reason-code registry: MATERIALIZATION_REASON_CODES are frozen by Contract
6.2.1 revision v1.1 (CTR-001) and are delegated to the system reason-code
registry policy (CTR-004) when it lands. The reused CONTRACT_REASON_CODES
come from Contract 6.2/6.4/13.7. The registry is closed: an expected reason
outside it is a corpus error.

CLI
---
    python3 validate_materialization_identity.py --root <materialization-dir>

Exit codes: 0 all cases pass; 1 at least one case fails; 2 corpus/sub-manifest
integrity error. Output is deterministic (sub-manifest order).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

MANIFEST_SCHEMA_VERSION = "rsih-skill-evolution.materialization-manifest.v1"
EXPECTED_SCHEMA_VERSION = "rsih-skill-evolution.materialization-expected.v1"
CONTRACT_SCHEMA_VERSION = "rsih-skill-evolution.system-contract.v1"
CONTRACT_REVISION = "system-contract 6.2.1 v1.1 (CTR-001)"

MANIFEST_DOCUMENT_SCHEMA_VERSION = "rsih.materialization-manifest.v1"
LOCK_SCHEMA_VERSION = "rsih.skill-lock.v1"

CASE_CATEGORIES = ("identity", "lock-closure", "negative")
CATEGORY_DIRS = {
    "identity": "identity",
    "lock-closure": "lock-closure",
    "negative": "negative",
}
SUB_MANIFEST_NAME = "manifest.json"
MANIFEST_DOCUMENT_NAME = "manifest-document.json"
MANIFEST_CONFLICTING_NAME = "manifest-document-conflicting.json"
LOCK_NAME = "lock.json"
BUNDLE_DIR_NAME = "bundle"

MATERIALIZATION_REASON_CODES = frozenset(
    {
        "MANIFEST_VERSION_MISSING",      # Contract 6.2.1 v1.1 (missing/non-int version)
        "BUNDLE_DIGEST_NOT_MANIFEST_REF",  # Contract 6.2.1 v1.1 (layer confusion)
        "MANIFEST_DIGEST_MISMATCH",      # Contract 6.2.1 v1.1 (stale ref digest)
        "MANIFEST_PARTIAL",              # Contract 6.2.1 v1.1 (open/inconsistent manifest)
        "MANIFEST_SEQUENCE_STALE",       # Contract 6.2.1 v1.1 (freeze != manifest sequence)
        "LOCK_CLOSURE_INCOMPLETE",       # Contract 6.2.1 v1.1 (closure < manifest)
        "SAME_REF_DIFFERENT_BYTES",      # Contract 6.2.1 v1.1 (uniqueness)
    }
)
CONTRACT_REASON_CODES = frozenset(
    {
        "UNKNOWN_REQUIRED_EXTENSION",    # Contract 1.3.3/6.4/13.7
        "DIGEST_MISMATCH",               # Contract 6.2/13.7
        "REF_MISMATCH",                  # Contract 6.2
    }
)
EXTENSION_REASON_CODES = frozenset(
    {
        "BOM_NOT_ALLOWED",               # Contract 6.1.1
        "INVALID_JSON",                  # Contract 6.1.1/6.1.2 precondition
    }
)
REASON_CODES = MATERIALIZATION_REASON_CODES | CONTRACT_REASON_CODES | EXTENSION_REASON_CODES

# v1 conformance policy (mirrors FND-001): no required extension is registered
# in v1, so required:true + unknown key always fails closed.
KNOWN_EXTENSIONS_V1 = frozenset(set())

SKILL_KINDS = frozenset({"human_procedure", "step_guidance", "composite"})
BOM = b"\xef\xbb\xbf"


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
    manifest_digest: "str | None"
    bundle_digest: "str | None" = None
    lock_digest: "str | None" = None
    legacy: bool = False


@dataclass
class CaseResult:
    case_id: str
    ok: bool
    message: str
    manifest_digest: str = "-"
    bundle_digest: str = "-"
    lock_digest: str = "-"
    accept: "str | None" = None
    reason: str = "-"


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


def manifest_digest(document) -> str:
    """Layer 2 identity: SHA-256 over the closed manifest document."""
    return digest_bytes(jcs(document))


def bundle_rollup_digest(files) -> str:
    """Layer 1 rollup: SHA-256(JCS([{content_digest, path}] by path UTF-16))."""
    rollup = sorted(
        ({"content_digest": f["content_digest"], "path": f["path"]} for f in files),
        key=lambda entry: entry["path"].encode("utf-16-be"),
    )
    return digest_bytes(jcs(rollup))


def lock_preimage_digest(lock_document) -> str:
    """Layer 3 identity: digest over the lock document minus lock_digest."""
    preimage = {k: v for k, v in lock_document.items() if k != "lock_digest"}
    return digest_bytes(jcs(preimage))


# ---------------------------------------------------------------------------
# Shared exact-ref / digest helpers (Refactor layer, no RSIH layout leaks).
# ---------------------------------------------------------------------------


def _is_int(node) -> bool:
    return isinstance(node, int) and not isinstance(node, bool)


def _is_digest(value) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == 71
        and all(c in "0123456789abcdef" for c in value[7:])
    )


def _extensions_issue(*documents):
    for document in documents:
        if document is None or not isinstance(document, dict):
            continue
        extensions = document.get("extensions")
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


def _safe_relative_path(path) -> bool:
    if not isinstance(path, str) or not path:
        return False
    if path.startswith("/") or path.endswith("/") or "\\" in path:
        return False
    parts = path.split("/")
    return all(part not in ("", ".", "..") for part in parts) and "\x00" not in path


def _skill_ref_key(ref):
    return (
        ref.get("lineage_id", "").encode("utf-8"),
        ref.get("version", 0),
        ref.get("artifact_digest", ""),
    )


def _manifest_partial_issue(manifest) -> "str | None":
    """Closed-manifest internal consistency (Contract 6.2.1 v1.1)."""
    if not isinstance(manifest, dict):
        return "MANIFEST_PARTIAL"
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        return "MANIFEST_PARTIAL"
    paths = []
    for entry in files:
        if not isinstance(entry, dict):
            return "MANIFEST_PARTIAL"
        if not _safe_relative_path(entry.get("path")):
            return "MANIFEST_PARTIAL"
        if not _is_int(entry.get("size_bytes")) or entry["size_bytes"] < 0:
            return "MANIFEST_PARTIAL"
        if not _is_digest(entry.get("content_digest")):
            return "MANIFEST_PARTIAL"
        paths.append(entry["path"])
    if len(set(paths)) != len(paths):
        return "MANIFEST_PARTIAL"  # duplicate paths
    if sorted(paths, key=lambda p: p.encode("utf-16-be")) != paths:
        return "MANIFEST_PARTIAL"  # files must be path-sorted
    if manifest.get("file_count") != len(files):
        return "MANIFEST_PARTIAL"
    if manifest.get("total_bytes") != sum(entry["size_bytes"] for entry in files):
        return "MANIFEST_PARTIAL"
    roots = manifest.get("root_skill_refs")
    if not isinstance(roots, list) or len(roots) != 1:
        return "MANIFEST_PARTIAL"
    root = roots[0]
    skill = manifest.get("skill")
    if not isinstance(skill, dict) or skill.get("kind") not in SKILL_KINDS:
        return "MANIFEST_PARTIAL"
    if (
        root.get("lineage_id") != skill.get("lineage_id")
        or root.get("version") != skill.get("version")
        or root.get("kind") != skill.get("kind")
    ):
        return "MANIFEST_PARTIAL"
    transitive = manifest.get("transitive_skill_refs")
    if not isinstance(transitive, list) or not transitive:
        return "MANIFEST_PARTIAL"
    identities = [
        (r.get("lineage_id"), r.get("version"), r.get("artifact_digest"))
        for r in transitive
        if isinstance(r, dict)
    ]
    if len(identities) != len(transitive):
        return "MANIFEST_PARTIAL"
    if len(set(identities)) != len(identities):
        return "MANIFEST_PARTIAL"
    if (root.get("lineage_id"), root.get("version"), root.get("artifact_digest")) not in identities:
        return "MANIFEST_PARTIAL"  # root must be covered by the closure
    if [ _skill_ref_key(r) for r in transitive ] != sorted(
        _skill_ref_key(r) for r in transitive
    ):
        return "MANIFEST_PARTIAL"  # transitive refs must be sorted (6.3)
    for ref in (manifest.get("render_profile_ref"), manifest.get("permission_profile_ref")):
        if not isinstance(ref, dict) or not _is_digest(ref.get("digest")):
            return "MANIFEST_PARTIAL"
    producer = manifest.get("producer")
    if (
        not isinstance(producer, dict)
        or not isinstance(producer.get("producer_id"), str)
        or not producer.get("producer_id")
        or not _is_int(producer.get("producer_version"))
    ):
        return "MANIFEST_PARTIAL"
    if not _is_digest(manifest.get("bundle_digest")):
        return "MANIFEST_PARTIAL"
    if not _is_int(manifest.get("created_from_activation_sequence")):
        return "MANIFEST_PARTIAL"
    return None


def _bundle_bytes_issue(manifest, bundle_files) -> "tuple[str | None, str | None]":
    """Layer 1 verification against actual bundle bytes (when provided)."""
    if bundle_files is None:
        return None, bundle_rollup_digest(manifest["files"])
    declared = {entry["path"]: entry for entry in manifest["files"]}
    actual = set(bundle_files)
    if actual != set(declared):
        return "MANIFEST_PARTIAL", bundle_rollup_digest(manifest["files"])
    for path, entry in declared.items():
        blob = bundle_files[path]
        if digest_bytes(blob) != entry["content_digest"]:
            return "DIGEST_MISMATCH", bundle_rollup_digest(manifest["files"])
        if entry["size_bytes"] != len(blob):
            return "DIGEST_MISMATCH", bundle_rollup_digest(manifest["files"])
    rollup = bundle_rollup_digest(manifest["files"])
    if rollup != manifest.get("bundle_digest"):
        return "DIGEST_MISMATCH", rollup
    return None, rollup


def _lock_issue(manifest, lock, computed_manifest_digest, computed_bundle_digest):
    """Layer 3 checks; first violation wins (order documented above)."""
    if not isinstance(lock, dict):
        return "REF_MISMATCH"
    ref = lock.get("materialization_ref")
    if not isinstance(ref, dict):
        return "REF_MISMATCH"
    if ref.get("id") != manifest.get("materialization_id"):
        return "REF_MISMATCH"
    # Missing/non-integer version was already handled at step 4; here the
    # version must also match the manifest's manifest_version exactly.
    if not _is_int(ref.get("version")) or ref["version"] < 1:
        return "MANIFEST_VERSION_MISSING"
    if ref["version"] != manifest.get("manifest_version"):
        return "REF_MISMATCH"
    if not _is_digest(ref.get("digest")):
        return "REF_MISMATCH"
    per_file = {entry["content_digest"] for entry in manifest["files"]}
    if ref["digest"] == computed_bundle_digest or ref["digest"] in per_file:
        return "BUNDLE_DIGEST_NOT_MANIFEST_REF"  # layer confusion, precise code
    if ref["digest"] != computed_manifest_digest:
        return "MANIFEST_DIGEST_MISMATCH"
    if lock.get("activation_sequence_at_freeze") != manifest.get(
        "created_from_activation_sequence"
    ):
        return "MANIFEST_SEQUENCE_STALE"
    lock_roots = lock.get("root_skill_refs")
    manifest_roots = manifest.get("root_skill_refs")
    if lock_roots != manifest_roots:
        return "REF_MISMATCH"
    closure = lock.get("locked_closure")
    if not isinstance(closure, list) or not closure:
        return "LOCK_CLOSURE_INCOMPLETE"
    declared_paths = {entry["path"] for entry in manifest["files"]}
    closure_paths = set()
    for entry in closure:
        if not isinstance(entry, dict):
            return "LOCK_CLOSURE_INCOMPLETE"
        path = entry.get("materialized_path")
        if path not in declared_paths:
            return "LOCK_CLOSURE_INCOMPLETE"  # undeclared node/path
        if not _is_digest(entry.get("file_digest")):
            return "LOCK_CLOSURE_INCOMPLETE"
        if entry["file_digest"] != next(
            e["content_digest"] for e in manifest["files"] if e["path"] == path
        ):
            return "LOCK_CLOSURE_INCOMPLETE"
        closure_paths.add(path)
    if not declared_paths <= closure_paths:
        return "LOCK_CLOSURE_INCOMPLETE"  # closure misses a manifest node
    transitive = {
        (r.get("lineage_id"), r.get("version"), r.get("artifact_digest"))
        for r in manifest["transitive_skill_refs"]
    }
    for entry in closure:
        ref_entry = entry.get("skill_ref")
        if not isinstance(ref_entry, dict):
            return "LOCK_CLOSURE_INCOMPLETE"
        identity = (
            ref_entry.get("lineage_id"),
            ref_entry.get("version"),
            ref_entry.get("artifact_digest"),
        )
        if identity not in transitive:
            return "LOCK_CLOSURE_INCOMPLETE"
    if not _is_digest(lock.get("lock_digest")):
        return "DIGEST_MISMATCH"
    if lock["lock_digest"] != lock_preimage_digest(lock):
        return "DIGEST_MISMATCH"
    return None


def derive_outcome(manifest, lock=None, conflicting=None, bundle_files=None) -> Outcome:
    """Derive (accept, reason, three-layer digests) from case inputs alone."""
    legacy = isinstance(manifest, dict) and not _is_int(manifest.get("manifest_version"))
    try:
        computed_manifest_digest = manifest_digest(manifest)
        computed_lock_digest = lock_preimage_digest(lock) if lock is not None else None
    except CanonicalizationError as exc:
        return Outcome(False, exc.reason_code, None, None, None, legacy)

    # Step 3 - extensions (Contract 6.4).
    issue = _extensions_issue(manifest, lock)
    # Step 4 - manifest_version exactness (Contract 6.2.1 v1.1).
    if issue is None and legacy:
        issue = "MANIFEST_VERSION_MISSING"
    if issue is None and lock is not None:
        ref = lock.get("materialization_ref") if isinstance(lock, dict) else None
        if not isinstance(ref, dict) or not _is_int(ref.get("version")) or ref["version"] < 1:
            issue = "MANIFEST_VERSION_MISSING"
    # Step 5 - closed-manifest consistency.
    if issue is None:
        issue = _manifest_partial_issue(manifest)
    # Step 6 - same ref different bytes (conflicting document).
    if issue is None and conflicting is not None:
        try:
            conflicting_digest = manifest_digest(conflicting)
        except CanonicalizationError as exc:
            return Outcome(False, exc.reason_code, computed_manifest_digest, None, None, legacy)
        if (
            conflicting.get("materialization_id") == manifest.get("materialization_id")
            and conflicting.get("manifest_version") == manifest.get("manifest_version")
            and conflicting_digest != computed_manifest_digest
        ):
            issue = "SAME_REF_DIFFERENT_BYTES"
    # Step 7 - bundle bytes layer.
    computed_bundle_digest = bundle_rollup_digest(manifest["files"]) if isinstance(
        manifest, dict
    ) and isinstance(manifest.get("files"), list) else None
    if issue is None:
        issue, computed_bundle_digest = _bundle_bytes_issue(manifest, bundle_files)
    # Steps 8-13 - lock layer.
    if issue is None and lock is not None:
        issue = _lock_issue(manifest, lock, computed_manifest_digest, computed_bundle_digest)
    if issue is not None:
        return Outcome(
            False, issue, computed_manifest_digest, computed_bundle_digest,
            computed_lock_digest, legacy,
        )
    return Outcome(
        True, None, computed_manifest_digest, computed_bundle_digest,
        computed_lock_digest, legacy,
    )


def derive_case_outcome(case_dir: Path) -> Outcome:
    """Read a case directory and derive its outcome (files on disk only)."""
    manifest_document = json.loads(
        (case_dir / MANIFEST_DOCUMENT_NAME).read_bytes().decode("utf-8")
    )
    lock_document = None
    if (case_dir / LOCK_NAME).is_file():
        lock_document = json.loads(
            (case_dir / LOCK_NAME).read_bytes().decode("utf-8")
        )
    conflicting_document = None
    if (case_dir / MANIFEST_CONFLICTING_NAME).is_file():
        conflicting_document = json.loads(
            (case_dir / MANIFEST_CONFLICTING_NAME).read_bytes().decode("utf-8")
        )
    bundle_files = None
    if (case_dir / BUNDLE_DIR_NAME).is_dir():
        bundle_files = {
            path.relative_to(case_dir / BUNDLE_DIR_NAME).as_posix(): path.read_bytes()
            for path in sorted((case_dir / BUNDLE_DIR_NAME).rglob("*"))
            if path.is_file()
        }
    return derive_outcome(
        manifest_document, lock_document, conflicting_document, bundle_files
    )


# ---------------------------------------------------------------------------
# Corpus / sub-manifest validation
# ---------------------------------------------------------------------------


def _parse_json_bytes(data: bytes, what: str, errors: list):
    if data.startswith(BOM):
        errors.append("%s starts with a UTF-8 BOM" % what)
        return None
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


def _read_bundle(bundle_dir: Path, errors: list):
    files = {}
    for path in sorted(bundle_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(bundle_dir).as_posix()
        if not _safe_relative_path(rel):
            errors.append("bundle contains unsafe relative path: %s" % rel)
            continue
        files[rel] = path.read_bytes()
    return files


def _canonical_file_issue(document, canonical_bytes, what):
    try:
        expected = jcs(document)
    except CanonicalizationError:
        return None  # surfaced as the derivation reason instead
    if canonical_bytes.startswith(BOM):
        return "%s starts with a UTF-8 BOM" % what
    if canonical_bytes != expected:
        return "%s bytes are not the JCS canonicalization of %s" % (what, what)
    return None


def _load_manifest(root: Path, errors: list):
    manifest_path = root / SUB_MANIFEST_NAME
    if not manifest_path.is_file():
        errors.append("manifest.json missing under %s" % root)
        return None
    manifest = _parse_json_bytes(manifest_path.read_bytes(), "manifest.json", errors)
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
    entries = []
    seen_ids: "set[str]" = set()
    mirror_keys = (
        "expected_accept",
        "expected_manifest_digest",
        "expected_bundle_digest",
        "expected_lock_digest",
        "expected_reason_code",
    )
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
        if category not in CASE_CATEGORIES:
            errors.append(
                "case %s has unknown category %r (expected one of %s)"
                % (case_id, category, "|".join(CASE_CATEGORIES))
            )
            continue
        path = raw.get("path")
        resolved = _safe_relative(root, path) if isinstance(path, str) else None
        if resolved is None or not resolved.is_dir():
            errors.append("case %s path is not a safe existing directory: %r" % (case_id, path))
            continue
        if not path.startswith(CATEGORY_DIRS[category] + "/"):
            errors.append(
                "case %s category %r must live under %s/"
                % (case_id, category, CATEGORY_DIRS[category])
            )
            continue
        if not (resolved / MANIFEST_DOCUMENT_NAME).is_file():
            errors.append("case %s missing %s" % (case_id, MANIFEST_DOCUMENT_NAME))
            continue
        if not (resolved / "expected.json").is_file():
            errors.append("case %s missing expected.json" % case_id)
            continue
        if category == "lock-closure" and not (resolved / LOCK_NAME).is_file():
            errors.append("lock-closure case %s missing %s" % (case_id, LOCK_NAME))
            continue
        entries.append(
            {
                "case_id": case_id,
                "category": category,
                "path": path,
                "dir": resolved,
                "mirrors": {key: raw[key] for key in mirror_keys if key in raw},
            }
        )
    # Corpus completeness: every case directory is declared exactly once.
    declared_dirs = {entry["path"] for entry in entries}
    for category, dirname in sorted(CATEGORY_DIRS.items()):
        base = root / dirname
        if not base.is_dir():
            if any(entry["category"] == category for entry in entries):
                errors.append("category directory missing: %s/" % dirname)
            continue
        for child in sorted(base.iterdir()):
            if not child.is_dir():
                continue
            rel = "%s/%s" % (dirname, child.name)
            if rel not in declared_dirs:
                errors.append("undeclared case directory not in manifest: %s" % rel)
    return entries


def _check_expected_shape(entry, expected, problems):
    case_id = entry["case_id"]
    if expected.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        problems.append(
            "%s: expected schema_version must be %r" % (case_id, EXPECTED_SCHEMA_VERSION)
        )
    if expected.get("case_id") != case_id:
        problems.append(
            "%s: expected case_id %r does not match manifest" % (case_id, expected.get("case_id"))
        )
    if not isinstance(expected.get("expected_accept"), bool):
        problems.append("%s: expected_accept must be a boolean" % case_id)
    if not _is_digest(expected.get("expected_manifest_digest")):
        problems.append("%s: expected_manifest_digest must be a sha256 digest" % case_id)
    if expected.get("expected_accept") is False:
        reason = expected.get("expected_reason_code")
        if not isinstance(reason, str) or reason not in REASON_CODES:
            problems.append(
                "%s: rejected case must carry expected_reason_code from the closed registry"
                % case_id
            )
    elif expected.get("expected_reason_code") is not None:
        problems.append("%s: accepted case must not carry expected_reason_code" % case_id)


def _evaluate_entry(entry) -> CaseResult:
    case_id = entry["case_id"]
    case_dir = entry["dir"]
    problems: "list[str]" = []

    manifest_document = _parse_json_bytes(
        (case_dir / MANIFEST_DOCUMENT_NAME).read_bytes(),
        "%s %s" % (case_id, MANIFEST_DOCUMENT_NAME),
        problems,
    )
    lock_document = None
    if (case_dir / LOCK_NAME).is_file():
        lock_document = _parse_json_bytes(
            (case_dir / LOCK_NAME).read_bytes(),
            "%s %s" % (case_id, LOCK_NAME),
            problems,
        )
    conflicting_document = None
    if (case_dir / MANIFEST_CONFLICTING_NAME).is_file():
        conflicting_document = _parse_json_bytes(
            (case_dir / MANIFEST_CONFLICTING_NAME).read_bytes(),
            "%s %s" % (case_id, MANIFEST_CONFLICTING_NAME),
            problems,
        )
    bundle_files = None
    if (case_dir / BUNDLE_DIR_NAME).is_dir():
        bundle_files = _read_bundle(case_dir / BUNDLE_DIR_NAME, problems)

    canonical_manifest = (case_dir / "manifest-document.canonical.utf8")
    if manifest_document is not None and canonical_manifest.is_file():
        issue = _canonical_file_issue(
            manifest_document, canonical_manifest.read_bytes(),
            "manifest-document.canonical.utf8",
        )
        if issue:
            problems.append(issue)
    canonical_lock = case_dir / "lock.canonical.utf8"
    if lock_document is not None and canonical_lock.is_file():
        issue = _canonical_file_issue(
            lock_document, canonical_lock.read_bytes(), "lock.canonical.utf8"
        )
        if issue:
            problems.append(issue)

    expected = _parse_json_bytes(
        (case_dir / "expected.json").read_bytes(), "%s expected.json" % case_id, problems
    )
    if expected is None or manifest_document is None:
        return CaseResult(case_id, False, "; ".join(problems) or "case unreadable")

    _check_expected_shape(entry, expected, problems)
    for key, value in entry["mirrors"].items():
        if key in expected and expected[key] != value:
            problems.append(
                "case %s: manifest mirror %s=%r disagrees with expected.json %r"
                % (case_id, key, value, expected[key])
            )

    outcome = derive_outcome(manifest_document, lock_document, conflicting_document, bundle_files)

    if expected.get("expected_manifest_digest") != outcome.manifest_digest:
        problems.append(
            "expected_manifest_digest %s != derived %s"
            % (expected.get("expected_manifest_digest"), outcome.manifest_digest)
        )
    if "expected_bundle_digest" in expected and expected["expected_bundle_digest"] is not None:
        if expected["expected_bundle_digest"] != outcome.bundle_digest:
            problems.append(
                "expected_bundle_digest %s != derived %s"
                % (expected["expected_bundle_digest"], outcome.bundle_digest)
            )
    if "expected_lock_digest" in expected and expected["expected_lock_digest"] is not None:
        if expected["expected_lock_digest"] != outcome.lock_digest:
            problems.append(
                "expected_lock_digest %s != derived %s"
                % (expected["expected_lock_digest"], outcome.lock_digest)
            )
    if outcome.accept != expected["expected_accept"]:
        problems.append(
            "derived accept=%s (reason %s) but expected accept=%s"
            % (str(outcome.accept).lower(), outcome.reason_code, expected["expected_accept"])
        )
    elif not outcome.accept and outcome.reason_code != expected["expected_reason_code"]:
        problems.append(
            "derived reason %s != expected reason %s"
            % (outcome.reason_code, expected["expected_reason_code"])
        )

    return CaseResult(
        case_id,
        not problems,
        "; ".join(problems),
        outcome.manifest_digest or "-",
        outcome.bundle_digest or "-",
        outcome.lock_digest or "-",
        "true" if outcome.accept else "false",
        outcome.reason_code or "-",
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
        results.append(_evaluate_entry(entry))
    return errors, results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the CTR-001 materialization exact identity corpus "
        "(Contract 6.2.1 revision v1.1)."
    )
    parser.add_argument("--root", required=True, type=Path, help="materialization directory")
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
                "PASS %s accept=%s manifest=%s bundle=%s lock=%s reason=%s"
                % (
                    result.case_id,
                    result.accept if result.accept is not None else "-",
                    result.manifest_digest,
                    result.bundle_digest,
                    result.lock_digest,
                    result.reason,
                )
            )
        else:
            print("FAIL %s: %s" % (result.case_id, result.message))
    failed = [result for result in results if not result.ok]
    print(
        "SUMMARY: cases=%d passed=%d failed=%d corpus_errors=%d revision=%s"
        % (
            len(results),
            len(results) - len(failed),
            len(failed),
            len(errors),
            CONTRACT_REVISION,
        )
    )
    if errors:
        return 2
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
