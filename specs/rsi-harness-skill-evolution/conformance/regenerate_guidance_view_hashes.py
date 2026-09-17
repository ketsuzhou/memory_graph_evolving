#!/usr/bin/env python3
"""Regenerate GuidanceView hashes after an exact SkillArtifactRef migration.

The generator rewrites only derived fixture values:
- every non-intentional GuidanceView ``view_hash``;
- accepted tool-case ``expected_result_digest`` values;
- artifact source canonical UTF-8/Base64 sidecars, expected digest/length, and
  their root-manifest mirrors.

A case intentionally expecting GUIDANCE_VIEW_HASH_MISMATCH remains stale so
that it continues to prove the rejection path.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
from pathlib import Path

from validate_explore_omission import jcs

ROOT = Path(__file__).resolve().parent
GUIDANCE_VIEW_SCHEMA = "gms.guidance-view.v1"
HASH_MISMATCH = "GUIDANCE_VIEW_HASH_MISMATCH"


def digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(jcs(value)).hexdigest()


def expected_for(source: Path) -> dict:
    expected = source.with_name("expected.json")
    if not expected.is_file():
        return {}
    return json.loads(expected.read_text(encoding="utf-8"))


def replacements(value: object, skip: bool) -> dict[str, str]:
    out: dict[str, str] = {}

    def walk(node: object) -> None:
        if isinstance(node, dict):
            if not skip and node.get("schema_version") == GUIDANCE_VIEW_SCHEMA:
                old = node.get("view_hash")
                new = digest({key: val for key, val in node.items() if key != "view_hash"})
                if not isinstance(old, str):
                    raise ValueError("GuidanceView view_hash must be a string")
                prior = out.setdefault(old, new)
                if prior != new:
                    raise ValueError("one fixture carries one stale hash for distinct GuidanceView preimages")
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return {old: new for old, new in out.items() if old != new}


def replace_text(path: Path, changes: dict[str, str], write: bool) -> bool:
    text = path.read_text(encoding="utf-8")
    updated = text
    for old, new in changes.items():
        count = updated.count(old)
        if count != 1:
            raise ValueError(f"{path}: expected one occurrence of {old}, found {count}")
        updated = updated.replace(old, new)
    if updated == text:
        return False
    if write:
        path.write_text(updated, encoding="utf-8")
    return True


def replace_expected_digest(expected_path: Path, new_digest: str, write: bool) -> bool:
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    old = expected.get("expected_result_digest")
    if old == new_digest:
        return False
    if not isinstance(old, str):
        raise ValueError(f"{expected_path}: accepted result has no expected_result_digest")
    return replace_text(expected_path, {old: new_digest}, write)


def update_artifact_sidecars(source: Path, value: object, write: bool) -> list[Path]:
    case_dir = source.parent
    case_id = case_dir.name
    canonical = jcs(value)
    digest_value = "sha256:" + hashlib.sha256(canonical).hexdigest()
    changed: list[Path] = []
    canonical_path = case_dir / "canonical.utf8"
    base64_path = case_dir / "canonical.base64"
    expected_path = case_dir / "expected.json"
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    if canonical_path.read_bytes() != canonical:
        changed.append(canonical_path)
        if write:
            canonical_path.write_bytes(canonical)
    encoded = base64.b64encode(canonical).decode("ascii")
    if base64_path.read_text(encoding="utf-8").strip() != encoded:
        changed.append(base64_path)
        if write:
            base64_path.write_text(encoded + "\n", encoding="utf-8")
    changes: dict[str, str] = {}
    if expected.get("expected_digest") != digest_value:
        changes[str(expected.get("expected_digest"))] = digest_value
    if expected.get("expected_canonical_byte_length") != len(canonical):
        changes[str(expected.get("expected_canonical_byte_length"))] = str(len(canonical))
    if changes and replace_text(expected_path, changes, write):
        changed.append(expected_path)

    manifest_path = ROOT / "manifest.json"
    manifest = manifest_path.read_text(encoding="utf-8")
    match = re.search(r'\{[^{}]*"case_id"\s*:\s*"' + re.escape(case_id) + r'"[^{}]*\}', manifest, flags=re.S)
    if match is None:
        raise ValueError(f"{manifest_path}: missing flat entry for {case_id}")
    entry = match.group(0)
    updated_entry = entry
    for key, value_text in (("expected_digest", digest_value), ("expected_canonical_byte_length", str(len(canonical)))):
        key_match = re.search(r'("' + key + r'"\s*:\s*)("[^"]*"|\d+)', updated_entry)
        if key_match is None:
            raise ValueError(f"{manifest_path}: {case_id} lacks {key}")
        replacement = '"' + value_text + '"' if key_match.group(2).startswith('"') else value_text
        updated_entry = updated_entry[:key_match.start(2)] + replacement + updated_entry[key_match.end(2):]
    if updated_entry != entry:
        changed.append(manifest_path)
        if write:
            manifest_path.write_text(manifest[:match.start()] + updated_entry + manifest[match.end():], encoding="utf-8")
    return changed


def regenerate(write: bool) -> list[Path]:
    changed: list[Path] = []
    sources = sorted(ROOT.glob("artifacts/**/source.json")) + sorted(ROOT.glob("tools/**/input.json"))
    for source in sources:
        value = json.loads(source.read_text(encoding="utf-8"))
        expected = expected_for(source)
        skip = expected.get("expected_reason_code") == HASH_MISMATCH
        updates = replacements(value, skip)
        if updates:
            if replace_text(source, updates, write):
                changed.append(source)
            value = json.loads(source.read_text(encoding="utf-8")) if write else apply_updates(value, updates)
        if source.parts[-3] == "artifacts":
            if updates:
                changed.extend(update_artifact_sidecars(source, value, write))
        elif expected.get("expected_accept") is True:
            payload = value.get("result_payload")
            if not isinstance(payload, dict):
                raise ValueError(f"{source}: accepted tool case has no result_payload")
            if replace_expected_digest(source.with_name("expected.json"), digest(payload), write):
                changed.append(source.with_name("expected.json"))
    return sorted(set(changed))


def apply_updates(value: object, updates: dict[str, str]) -> object:
    if isinstance(value, dict):
        return {key: apply_updates(val, updates) for key, val in value.items()}
    if isinstance(value, list):
        return [apply_updates(item, updates) for item in value]
    if isinstance(value, str):
        return updates.get(value, value)
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="apply the deterministic derived-value updates")
    args = parser.parse_args()
    changed = regenerate(args.write)
    for path in changed:
        print(path.relative_to(ROOT))
    print(f"{'updated' if args.write else 'would update'} {len(changed)} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
