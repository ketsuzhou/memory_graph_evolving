"""PG-43 Revocation and training integration tracer.

AReaL half: the durable export lifecycle authorizes cleanup only after a
verified receipt from the required sink, with zero-pop sources. GMS half:
cryptographic payload erasure gated by a closed reason-code enum (PII and
free-text reasons rejected) with non-sensitive tombstones and a stable
tenant identity, authorization-epoch cache invalidation, cooperative
training stop at poll points, and the complete lineage tombstone cascade
that records the erased root first and blocks regrow under any tombstone.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from tracer_common import (
    AREAL_REPO,
    GMS_REPO,
    assertion,
    build_trace,
    ensure_areal_on_path,
    run_driver,
    step,
    validate_spec_root,
)

TRACER_NAME = "revocation-training"


def _export_ack() -> dict:
    ensure_areal_on_path()
    # The stubz shim owns sys.modules["areal"] (it is not the real package),
    # so the stdlib-only lifecycle module is loaded directly by path.
    import importlib.util
    import sys

    lifecycle_path = AREAL_REPO / "areal" / "v2" / "trajectory_export" / "lifecycle.py"
    if not lifecycle_path.is_file():
        raise FileNotFoundError(f"missing AReaL export lifecycle module: {lifecycle_path}")
    spec = importlib.util.spec_from_file_location("pg43_export_lifecycle", lifecycle_path)
    lifecycle_module = importlib.util.module_from_spec(spec)
    # dataclasses resolves string annotations through sys.modules, so the
    # module must be registered before it is executed.
    sys.modules[spec.name] = lifecycle_module
    spec.loader.exec_module(lifecycle_module)
    DurableExportLifecycle = lifecycle_module.DurableExportLifecycle
    ExportNotWrittenError = lifecycle_module.ExportNotWrittenError
    ExportRequest = lifecycle_module.ExportRequest
    ReceiptRequiredError = lifecycle_module.ReceiptRequiredError
    SinkReceipt = lifecycle_module.SinkReceipt

    lifecycle = DurableExportLifecycle()
    frozen_input = "sha256:" + hashlib.sha256(b"frozen-input-pg43").hexdigest()
    request = ExportRequest(
        export_id="export-pg43",
        frozen_input_digest=frozen_input,
        source_session_ids=("session-1", "session-2"),
        required_sink_id="sink-durable-1",
    )
    frozen = lifecycle.freeze(request)

    # Acknowledge before a durable write is rejected.
    try:
        lifecycle.acknowledge(
            "export-pg43",
            SinkReceipt(sink_id="sink-durable-1", verified_digest=frozen.content_digest, receipt_id="rcpt-early"),
        )
        ack_before_write_rejected = False
    except ExportNotWrittenError:
        ack_before_write_rejected = True

    lifecycle.write(frozen)
    # Zero-pop invariant: sources survive the write until cleanup is authorized.
    sources_intact = lifecycle.source_sessions("export-pg43") == ("session-1", "session-2")
    cleanup_before_ack = lifecycle.cleanup_authorized("export-pg43")

    # A receipt from the wrong sink or without verification is rejected.
    try:
        lifecycle.acknowledge(
            "export-pg43",
            SinkReceipt(sink_id="sink-other", verified_digest=frozen.content_digest, receipt_id="rcpt-wrong-sink"),
        )
        wrong_sink_rejected = False
    except ReceiptRequiredError:
        wrong_sink_rejected = True

    acknowledged = lifecycle.acknowledge(
        "export-pg43",
        SinkReceipt(sink_id="sink-durable-1", verified_digest=frozen.content_digest, receipt_id="rcpt-1"),
    )
    return {
        "export_id": acknowledged.export_id,
        "content_digest": acknowledged.content_digest,
        "ack_before_write_rejected": ack_before_write_rejected,
        "wrong_sink_rejected": wrong_sink_rejected,
        "sources_intact": sources_intact,
        "cleanup_before_ack": cleanup_before_ack,
        "cleanup_after_ack": lifecycle.cleanup_authorized("export-pg43"),
    }


def run(spec_root: Path | None = None) -> dict:
    validate_spec_root(spec_root)

    export = _export_ack()
    gms = run_driver(GMS_REPO, ["revocation"])
    erase = gms["erase"]
    epoch = gms["epoch"]
    training = gms["training"]
    lineage = gms["lineage"]

    reason_enum_ok = erase["pii_reason_rejected"] and erase["free_text_rejected"]
    cascade_ok = (
        lineage["affected_complete"]
        and lineage["root_recorded"]
        and lineage["tombstones"] == lineage["descendants"] + 1
        and lineage["regrow_blocked"]
        and lineage["root_regrow_blocked"]
        and lineage["cascade_reason_closed"]
    )

    steps = [
        step(
            "export-ack",
            export["ack_before_write_rejected"]
            and export["wrong_sink_rejected"]
            and export["sources_intact"]
            and not export["cleanup_before_ack"]
            and export["cleanup_after_ack"],
            {
                "export_id": export["export_id"],
                "content_digest": export["content_digest"],
                "ack_before_write_rejected": export["ack_before_write_rejected"],
                "wrong_sink_rejected": export["wrong_sink_rejected"],
                "sources_intact": export["sources_intact"],
                "cleanup_after_ack": export["cleanup_after_ack"],
            },
        ),
        step(
            "payload-erase",
            erase["opened_before"]
            and erase["payload_unreadable"]
            and erase["tombstone_clean"]
            and reason_enum_ok
            and erase["tombstone_reason"] == "USER_DELETION_REQUEST",
            {
                "unreadable_after_erase": erase["payload_unreadable"],
                "tombstone_key": erase["tombstone_key_id"],
                "tombstone_reason": erase["tombstone_reason"],
                "identity_stable": erase["identity_stable"],
                "pii_reason_rejected": erase["pii_reason_rejected"],
                "free_text_reason_rejected": erase["free_text_rejected"],
            },
        ),
        step(
            "epoch-invalidation",
            epoch["cache_invalidated"] and epoch["monotonic"],
            {
                "revoked_cursor": epoch["revoked_cursor"],
                "cached_epoch_stale": epoch["cache_invalidated"],
                "epoch_regression_rejected": epoch["monotonic"],
            },
        ),
        step(
            "training-stop",
            training["stopped"] and training["polls"] == ["continue@1", "stop@1(stale)"],
            {"polls": training["polls"], "stopped": training["stopped"]},
        ),
        step(
            "lineage-tombstone",
            cascade_ok,
            {
                "descendants": lineage["descendants"],
                "tombstones": lineage["tombstones"],
                "root_recorded": lineage["root_recorded"],
                "affected_complete": lineage["affected_complete"],
                "regrow_blocked": lineage["regrow_blocked"],
                "root_regrow_blocked": lineage["root_regrow_blocked"],
            },
        ),
    ]

    assertions = [
        assertion(
            "durable_sink_ack_before_cleanup",
            export["ack_before_write_rejected"]
            and export["wrong_sink_rejected"]
            and not export["cleanup_before_ack"]
            and export["cleanup_after_ack"]
            and export["sources_intact"],
            f"cleanup for {export['export_id']} is authorized only after a "
            f"verified receipt from the required durable sink "
            f"(cleanup_before_ack={export['cleanup_before_ack']} -> "
            f"cleanup_after_ack={export['cleanup_after_ack']}); ack before "
            f"write and wrong-sink receipts are rejected and sources are "
            f"never popped early",
        ),
        assertion(
            "payload_unreadable_after_erase",
            erase["opened_before"]
            and erase["payload_unreadable"]
            and erase["identity_stable"]
            and reason_enum_ok,
            f"the sealed payload opens before erasure and is "
            f"cryptographically unreadable after; the tombstone keeps only the "
            f"non-sensitive reason code {erase['tombstone_reason']} and key id "
            f"{erase['tombstone_key_id']} (PII-bearing and free-text reasons "
            f"are rejected by the closed enum) while the tenant identity "
            f"survives",
        ),
        assertion(
            "cached_epoch_invalidated",
            epoch["cache_invalidated"] and epoch["monotonic"],
            f"an authorization cached under the pre-revocation epoch is stale "
            f"at cursor {epoch['revoked_cursor']}; epochs never regress",
        ),
        assertion(
            "running_training_stops_cooperatively",
            training["stopped"],
            f"the in-flight poller continues at its exact epoch and "
            f"voluntarily stops once a revocation advances it "
            f"(polls={'->'.join(training['polls'])})",
        ),
        assertion(
            "affected_lineage_complete",
            cascade_ok,
            f"the tombstone cascade records the erased root first and all "
            f"{lineage['descendants']} descendants "
            f"({lineage['tombstones']} tombstones total, each exactly once "
            f"with its cascade source); regrow under a tombstoned descendant "
            f"or under the erased root is rejected",
        ),
    ]
    return build_trace(TRACER_NAME, steps, assertions)
