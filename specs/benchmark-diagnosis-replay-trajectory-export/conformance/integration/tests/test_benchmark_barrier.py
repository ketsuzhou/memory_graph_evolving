"""PG-41 red tests for the benchmark barrier integration tracer."""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import SPEC_ROOT


EXPECTED_STEPS = [
    "manifest-parse",
    "agents-run",
    "room-close",
    "batch-barrier",
    "diagnosis",
    "per-agent-export",
]
EXPECTED_ASSERTIONS = [
    "known_set_all_terminal",
    "failed_runs_preserved",
    "next_episode_blocked_by_evidence",
    "per_agent_reconstructed_export",
    "room_close_requires_receipts",
]


def _tracer() -> tuple[str, object]:
    from run_benchmark_barrier import TRACER_NAME, run

    return TRACER_NAME, run


def test_benchmark_barrier_end_to_end() -> None:
    tracer_name, run = _tracer()

    trace = run(SPEC_ROOT / "conformance" / "integration")

    assert trace["tracer"] == tracer_name
    assert trace["ok"] is True
    assert [step["name"] for step in trace["steps"]][: len(EXPECTED_STEPS)] == EXPECTED_STEPS

    assertions = {assertion["id"]: assertion for assertion in trace["assertions"]}
    for assertion_id in EXPECTED_ASSERTIONS:
        assert assertion_id in assertions
        assert assertions[assertion_id]["ok"] is True


def test_benchmark_barrier_trace_shape() -> None:
    _, run = _tracer()

    with pytest.raises((FileNotFoundError, ValueError)):
        run(Path("/definitely-missing/benchmark-barrier-tracer-input"))
