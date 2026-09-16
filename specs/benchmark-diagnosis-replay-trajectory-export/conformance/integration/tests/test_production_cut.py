"""PG-40 red tests for the Production Cut integration tracer."""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import SPEC_ROOT


EXPECTED_STEPS = [
    "host-freeze",
    "receipts-complete",
    "gms-trigger",
    "diagnosis-reads",
    "annotations-aggregate",
    "space-publish",
    "audit-review",
]
EXPECTED_ASSERTIONS = [
    "exact_batch_coverage",
    "open_segment_deferred",
    "concurrent_trigger_serialized",
    "threshold_no_change_auditable",
    "partial_failure_retry_auditable",
]


def _tracer() -> tuple[str, object]:
    from run_production_cut import TRACER_NAME, run

    return TRACER_NAME, run


def test_production_cut_end_to_end() -> None:
    tracer_name, run = _tracer()

    trace = run(SPEC_ROOT / "conformance" / "integration")

    assert trace["tracer"] == tracer_name
    assert trace["ok"] is True
    assert [step["name"] for step in trace["steps"]][: len(EXPECTED_STEPS)] == EXPECTED_STEPS

    assertions = {assertion["id"]: assertion for assertion in trace["assertions"]}
    for assertion_id in EXPECTED_ASSERTIONS:
        assert assertion_id in assertions
        assert assertions[assertion_id]["ok"] is True


def test_production_cut_trace_shape() -> None:
    _, run = _tracer()

    with pytest.raises((FileNotFoundError, ValueError)):
        run(Path("/definitely-missing/production-cut-tracer-input"))
