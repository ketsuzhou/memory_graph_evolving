"""Golden-corpus conformance runner for the frozen PG-00D contract bundle."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Callable

import jsonschema

CONFORMANCE_ROOT = Path(__file__).resolve().parents[1]
_REASON_CODE = re.compile(r"^\s*-\s+code:\s*([A-Z][A-Z0-9_]*)\s*$")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def _reason_codes() -> set[str]:
    codes: set[str] = set()
    for line in (CONFORMANCE_ROOT / "reasons.yaml").read_text(encoding="utf-8").splitlines():
        match = _REASON_CODE.match(line)
        if match:
            codes.add(match.group(1))
    if not codes:
        raise AssertionError("reasons.yaml has no reason codes")
    return codes


def _schema_verdict(fixture: dict[str, Any], target: str) -> bool:
    schema = _read_json(CONFORMANCE_ROOT / "schema" / f"{target}.json")
    validator = jsonschema.Draft202012Validator(
        schema,
        format_checker=jsonschema.FormatChecker(),
    )
    return not list(validator.iter_errors(fixture))


def _logical_run_episode_unique(fixture: dict[str, Any]) -> bool:
    identities: set[tuple[Any, Any]] = set()
    for logical_run in fixture.get("logical_runs", []):
        identity = (logical_run.get("task_id"), logical_run.get("episode_id"))
        if identity in identities:
            return False
        identities.add(identity)
    return True


def _logical_run_id_unique(fixture: dict[str, Any]) -> bool:
    run_ids: set[Any] = set()
    for logical_run in fixture.get("logical_runs", []):
        run_id = logical_run.get("logical_run_id")
        if run_id in run_ids:
            return False
        run_ids.add(run_id)
    return True


def _cut_receipts_cover_sealed_segments(fixture: dict[str, Any]) -> bool:
    sealed = fixture.get("sealed_segment_ids", [])
    receipt_segments = [
        receipt.get("segment_id") for receipt in fixture.get("evidence_commit_receipts", [])
    ]
    return (
        len(sealed) == len(set(sealed))
        and len(receipt_segments) == len(set(receipt_segments))
        and set(receipt_segments) == set(sealed)
    )


def _sink_receipt_matches(fixture: dict[str, Any]) -> bool:
    receipt = fixture.get("sink_receipt")
    if receipt is None:
        return True
    return (
        receipt.get("sink_id") == fixture.get("required_sink_id")
        and receipt.get("verified_digest") == fixture.get("content_digest")
    )


def _capability_scope_within_cut(fixture: dict[str, Any]) -> bool:
    cut = fixture.get("cut")
    capability = fixture.get("capability")
    if not isinstance(cut, dict) or not isinstance(capability, dict):
        return False
    if (
        capability.get("tenant_id") != cut.get("tenant_id")
        or capability.get("cut_id") != cut.get("cut_id")
    ):
        return False

    frozen_private_spaces = {
        scope.get("space_id")
        for scope in cut.get("space_scopes", [])
        if scope.get("scope") == "private"
    }
    bound_spaces = capability.get("bound_private_spaces", [])
    if not isinstance(bound_spaces, list):
        return False
    bound_space_ids: set[Any] = set()
    for space in bound_spaces:
        if not isinstance(space, dict) or space.get("room_id") != cut.get("room_id"):
            return False
        space_id = space.get("space_id")
        if space_id in bound_space_ids:
            return False
        bound_space_ids.add(space_id)
    return bound_space_ids == frozen_private_spaces


EXTRA_RULES: dict[str, Callable[[dict[str, Any]], bool]] = {
    "logical_run_episode_unique": _logical_run_episode_unique,
    "logical_run_id_unique": _logical_run_id_unique,
    "cut_receipts_cover_sealed_segments": _cut_receipts_cover_sealed_segments,
    "sink_receipt_matches": _sink_receipt_matches,
    "capability_scope_within_cut": _capability_scope_within_cut,
}


def _state_values(value: str | list[str]) -> list[str]:
    return value if isinstance(value, list) else [value]


def _state_verdict(
    fixture: dict[str, Any], target: str, reason_codes: set[str]
) -> bool:
    machine = _read_json(CONFORMANCE_ROOT / "state" / f"{target}.json")
    if fixture.get("machine") != machine["machine_id"]:
        return False

    legal_edges = {
        (source, destination)
        for transition in machine["transitions"]
        for source in _state_values(transition["from"])
        for destination in _state_values(transition["to"])
    }
    if any(
        reason not in reason_codes
        for transition in machine["transitions"]
        if (reason := transition.get("reason")) is not None
    ):
        return False

    current: str | None = None
    for step in fixture.get("sequence", []):
        source, destination = step.get("from"), step.get("to")
        if (current is not None and source != current) or (source, destination) not in legal_edges:
            return False
        current = destination
    return True


def corpus_verdicts() -> dict[str, bool]:
    manifest = _read_json(CONFORMANCE_ROOT / "manifest.json")
    reason_codes = _reason_codes()
    verdicts: dict[str, bool] = {}
    for entry in manifest["fixtures"]:
        fixture = _read_json(CONFORMANCE_ROOT / "fixtures" / entry["file"])
        if entry["kind"] == "schema":
            accepted = _schema_verdict(fixture, entry["target"])
            rules = entry.get("extra_rules", [])
        elif entry["kind"] == "cross":
            accepted = True
            rules = [entry["target"]]
        elif entry["kind"] == "state":
            accepted = _state_verdict(fixture, entry["target"], reason_codes)
            rules = []
        else:
            raise AssertionError(f"unknown fixture kind: {entry['kind']}")
        for rule_name in rules:
            accepted = accepted and EXTRA_RULES[rule_name](fixture)
        verdicts[entry["file"]] = accepted
    return verdicts


def test_corpus_matches_declared_verdicts() -> None:
    manifest = _read_json(CONFORMANCE_ROOT / "manifest.json")
    actual = corpus_verdicts()
    expected = {
        entry["file"]: entry["verdict"] == "accept"
        for entry in manifest["fixtures"]
    }
    assert actual == expected


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", action="store_true")
    if parser.parse_args().report:
        print(json.dumps(corpus_verdicts(), sort_keys=True))
        return
    parser.error("use --report, or run this file through pytest")


if __name__ == "__main__":
    main()
