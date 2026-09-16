"""PG-42 red tests for the replay and activation integration tracer."""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import SPEC_ROOT


EXPECTED_STEPS = [
    "proposal-registration",
    "disclosure-approval",
    "isolated-replay",
    "taint-propagation",
    "evaluation-activation",
    "production-cas",
]
EXPECTED_ASSERTIONS = [
    "diagnosis_owned_stable_proposal",
    "private_derived_disclosure_gated",
    "replay_isolated_clone",
    "descendant_taint_complete",
    "pass_activates_evaluation_only",
    "stale_base_cas_rejected",
]


def _tracer() -> tuple[str, object]:
    from run_replay_activation import TRACER_NAME, run

    return TRACER_NAME, run


def test_replay_activation_end_to_end() -> None:
    tracer_name, run = _tracer()

    trace = run(SPEC_ROOT / "conformance" / "integration")

    assert trace["tracer"] == tracer_name
    assert trace["ok"] is True
    assert [step["name"] for step in trace["steps"]][: len(EXPECTED_STEPS)] == EXPECTED_STEPS

    assertions = {assertion["id"]: assertion for assertion in trace["assertions"]}
    for assertion_id in EXPECTED_ASSERTIONS:
        assert assertion_id in assertions
        assert assertions[assertion_id]["ok"] is True


def test_replay_activation_trace_shape() -> None:
    _, run = _tracer()

    with pytest.raises((FileNotFoundError, ValueError)):
        run(Path("/definitely-missing/replay-activation-tracer-input"))
