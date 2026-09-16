"""PG-43 red tests for the revocation and training integration tracer."""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import SPEC_ROOT


EXPECTED_STEPS = [
    "export-ack",
    "payload-erase",
    "epoch-invalidation",
    "training-stop",
    "lineage-tombstone",
]
EXPECTED_ASSERTIONS = [
    "durable_sink_ack_before_cleanup",
    "payload_unreadable_after_erase",
    "cached_epoch_invalidated",
    "running_training_stops_cooperatively",
    "affected_lineage_complete",
]


def _tracer() -> tuple[str, object]:
    from run_revocation import TRACER_NAME, run

    return TRACER_NAME, run


def test_revocation_end_to_end() -> None:
    tracer_name, run = _tracer()

    trace = run(SPEC_ROOT / "conformance" / "integration")

    assert trace["tracer"] == tracer_name
    assert trace["ok"] is True
    assert [step["name"] for step in trace["steps"]][: len(EXPECTED_STEPS)] == EXPECTED_STEPS

    assertions = {assertion["id"]: assertion for assertion in trace["assertions"]}
    for assertion_id in EXPECTED_ASSERTIONS:
        assert assertion_id in assertions
        assert assertions[assertion_id]["ok"] is True


def test_revocation_trace_shape() -> None:
    _, run = _tracer()

    with pytest.raises((FileNotFoundError, ValueError)):
        run(Path("/definitely-missing/revocation-training-tracer-input"))
