"""PG-40 Production Cut integration tracer.

Host half: the coordinator freeze pins epoch/sequence/sealed segments with
per-space distinct evidence batches and projection cursors, defers the open
segment fail-closed, and replays identically under the same idempotency key.
GMS half: the production-cut driver triggers over that freeze, proves
Room-serial admission under concurrency, the auditable threshold no_change,
the cut-scoped diagnosis chain (Q53=A exact private scope, masked
aggregation, partitioned disclosure), and the guarded stage walk whose
transitions carry real evidence the GuardAuthority resolves — including
forged/stale-evidence rejections and a partial-failure retry.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from tracer_common import (
    GMS_REPO,
    HOST_REPO,
    assertion,
    build_trace,
    run_driver,
    step,
    validate_spec_root,
)

TRACER_NAME = "production-cut"


def run(spec_root: Path | None = None) -> dict:
    validate_spec_root(spec_root)

    with tempfile.TemporaryDirectory(prefix="pg40-") as tmp:
        freeze_path = str(Path(tmp) / "host_freeze.json")
        host = run_driver(HOST_REPO, ["freeze", freeze_path])
        gms = run_driver(GMS_REPO, ["production-cut", freeze_path])

    sealed = host["sealed_room"]
    opened = host["open_room"]
    trigger = gms["trigger"]
    concurrent = gms["concurrent"]
    threshold = gms["threshold"]
    diagnosis = gms["diagnosis"]
    walk = gms["walk"]

    # Reads are per private space x frozen evidence batch, hash-chained, and
    # never touch a shared batch (Q53=A exact Room-bound private scope).
    reads = diagnosis["reads"]
    spaces_read = sorted({read["space_id"] for read in reads})
    private_batches_only = all("-private-" in read["batch_id"] for read in reads)
    hash_chained = all(
        reads[index]["previous"] == reads[index - 1]["digest"]
        for index in range(1, len(reads))
    )
    guard_probes = walk["guard_probes"]
    stale_probes = walk["stale_policy_probes"]

    steps = [
        step(
            "host-freeze",
            sealed["submission"]
            and sealed["idempotent_replay"]
            and sealed["body_conflict"]
            and sealed["distinct_evidence_batches"]
            and sealed["shared_batches"] >= 1
            and sealed["private_batches"] >= 1
            and sealed["outbox_rows_persisted"] == sealed["receipts"] * 2
            and sealed["batches_match_receipts"]
            and sealed["projection_watermark_source"] == "fixture_authority",
            {
                "room_id": sealed["room_id"],
                "epoch": sealed["epoch"],
                "sequence": sealed["sequence"],
                "sealed_segments": sealed["sealed_segments"],
                "idempotent_replay": sealed["idempotent_replay"],
                "body_conflict": sealed["body_conflict"],
                "distinct_evidence_batches": sealed["distinct_evidence_batches"],
                "shared_batches": sealed["shared_batches"],
                "private_batches": sealed["private_batches"],
                "projection_heads": sealed["projection_heads"],
                "query_watermarks": sealed["query_watermarks"],
                "outbox_rows_persisted": sealed["outbox_rows_persisted"],
                "batches_match_receipts": sealed["batches_match_receipts"],
                "projection_watermark_source": sealed["projection_watermark_source"],
            },
        ),
        step(
            "receipts-complete",
            sealed["receipts_complete"] and sealed["receipts"] == sealed["sealed_segments"],
            {
                "receipts": sealed["receipts"],
                "sealed_segments": sealed["sealed_segments"],
                "receipts_complete": sealed["receipts_complete"],
            },
        ),
        step(
            "gms-trigger",
            trigger["stage"] == "frozen" and not trigger["duplicate"],
            {
                "cut_id": trigger["cut_id"],
                "stage": trigger["stage"],
                "sealed_segments": trigger["sealed_segments"],
                "receipts": trigger["receipts"],
                "duplicate": trigger["duplicate"],
            },
        ),
        step(
            "diagnosis-reads",
            diagnosis["private_spaces"] >= 1
            and len(spaces_read) == diagnosis["private_spaces"]
            and len(reads) > diagnosis["private_spaces"]
            and private_batches_only
            and hash_chained
            and all(read["digest"].startswith("sha256:") for read in reads),
            {
                "private_spaces": diagnosis["private_spaces"],
                "reads": len(reads),
                "spaces_read": spaces_read,
                "private_batches_only": private_batches_only,
                "hash_chained": hash_chained,
            },
        ),
        step(
            "annotations-aggregate",
            diagnosis["masked_count"] == 1
            and diagnosis["source_count"] == 1
            and diagnosis["confidence"] == "low",
            {
                "masked": diagnosis["masked_count"],
                "source": diagnosis["source_count"],
                "confidence": diagnosis["confidence"],
                "audit_chain_digest": diagnosis["audit_chain_digest"],
            },
        ),
        step(
            "space-publish",
            diagnosis["disclosure"]
            and diagnosis["reason_code"] == "DISCLOSURE_PARTITIONED"
            and diagnosis["public_rationale"].startswith("disclosure://")
            and diagnosis["private_rationale"] == ""
            and walk["space_results"] == sealed["spaces"],
            {
                "disclosure": diagnosis["reason_code"],
                "public_rationale": diagnosis["public_rationale"],
                "space_results": walk["space_results"],
                "spaces": sealed["spaces"],
                "outcome": walk["outcome"],
            },
        ),
        step(
            "audit-review",
            walk["rediagnose_audited"]
            and walk["guard_rejected"]
            and walk["evidence_stamped"]
            and walk["authority_facts_bound"]
            and all(guard_probes.values())
            and all(stale_probes.values())
            and gms["audit_kinds"].get("trigger_frozen", 0) == 1,
            {
                "audit_kinds": gms["audit_kinds"],
                "guard_probes": guard_probes,
                "stale_policy_probes": stale_probes,
                "evidence_stamped": walk["evidence_stamped"],
                "authority_facts_bound": walk["authority_facts_bound"],
                "rediagnose_run_ref": walk["rediagnose_run_ref"],
            },
        ),
    ]

    assertions = [
        assertion(
            "exact_batch_coverage",
            sealed["receipts_complete"]
            and sealed["receipts"] == sealed["sealed_segments"]
            and sealed["distinct_evidence_batches"]
            and trigger["stage"] == "frozen"
            and trigger["receipts"] == sealed["receipts"],
            f"receipts={sealed['receipts']} cover sealed={sealed['sealed_segments']} "
            f"exactly 1:1 with per-space distinct batches "
            f"(shared={sealed['shared_batches']} private={sealed['private_batches']}); "
            f"the GMS trigger froze on the same receipt set (stage={trigger['stage']})",
        ),
        assertion(
            "open_segment_deferred",
            opened["deferred_segments"] == 1
            and opened["receipts_awaited"] == 0
            and opened["submission"] is False,
            f"room={opened['room_id']}: open segment deferred "
            f"({opened['deferred_segments']}), receipts not awaited, submission forbidden",
        ),
        assertion(
            "concurrent_trigger_serialized",
            concurrent["rejected_same_body"]
            and concurrent["no_new_cut_minted"]
            and concurrent["audit_kind"],
            f"same-room concurrent trigger rejected against the reservation "
            f"(no_new_cut_minted={concurrent['no_new_cut_minted']}, "
            f"audit_kind={concurrent['audit_kind']})",
        ),
        assertion(
            "threshold_no_change_auditable",
            threshold["stage"] == "completed"
            and threshold["outcome"] == "no_change"
            and threshold["audit_kind"],
            f"threshold trigger without accumulated evidence lands "
            f"{threshold['stage']}/{threshold['outcome']} with audit "
            f"kind={threshold['audit_kind']}",
        ),
        assertion(
            "partial_failure_retry_auditable",
            walk["failure_reasons"] == ["DIAGNOSIS_FAILED_OR_INCONCLUSIVE"]
            and walk["policy_walked"]
            and walk["guard_rejected"]
            and walk["rediagnose_audited"]
            and walk["evidence_stamped"]
            and walk["rediagnose_run_ref"] == "diagnosis-run-pg40-2"
            and all(stale_probes.values())
            and walk["stages"]
            == ["diagnosing", "partially_failed", "diagnosing", "consolidating", "completed"],
            f"stages={'->'.join(walk['stages'])}; rediagnose run "
            f"{walk['rediagnose_run_ref']} walks policy to {walk['policy_revision']}; "
            f"stale v1 evidence rejected after rediagnose "
            f"({sorted(stale_probes)}) and the admitted v2 evidence is stamped "
            f"(evidence_stamped={walk['evidence_stamped']})",
        ),
    ]
    return build_trace(TRACER_NAME, steps, assertions)
