"""PG-41 Benchmark barrier integration tracer.

Host half: strict batch-manifest parse over the frozen known set, the
logical-run ledger keyed by identity (failed runs preserved, never dropped),
all registered agent runs terminal before room closing, the SC-4.5 receipts
guard, the fail-closed evidence barrier that blocks the next episode until
every projection commits, and crash/restart outbox recovery. The Host writes
its released-barrier facts to a JSON file. GMS half: the batch-diagnosis
driver consumes that facts file plus the frozen manifest and runs one real
guarded cut + diagnosis per logical run (Q53=A exact private scope,
capability-bounded reads). AReaL half: per-agent reconstructed export via
the DAG-preserving projection and the Q60=B structured retokenization.
"""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from types import SimpleNamespace

from tracer_common import (
    GMS_REPO,
    HOST_REPO,
    assertion,
    build_trace,
    ensure_areal_on_path,
    run_driver,
    step,
    validate_spec_root,
)

TRACER_NAME = "benchmark-barrier"

EXPECTED_RUN_STAGES = ["frozen", "diagnosing", "consolidating", "completed"]


def _encode_messages(messages: list[dict]) -> tuple:
    """Deterministic stand-in encoder: stable token ids from content bytes."""
    return tuple(
        tuple(bytearray(hashlib.sha256(str(message).encode("utf-8")).digest())[:8])
        for message in messages
    )


def _per_agent_export() -> dict:
    ensure_areal_on_path()
    from customized_areal.tree_search.agents.dag import event_codec
    from customized_areal.tree_search.agents.dag.execution_dag import (
        EdgeType,
        ExecutionDAG,
        SuperNode,
    )
    from customized_areal.tree_search.core.structured_retokenization import (
        retokenize_structured_trajectory_bound,
    )

    def node(node_id: str, agent_run_id: str) -> SuperNode:
        return SuperNode(
            node_id=node_id,
            agent_id=agent_run_id,
            issue_id=f"issue-{node_id}",
            task_id="task-a",
            nodes=[
                SimpleNamespace(
                    node_id=f"turn-{node_id}",
                    messages=[{"role": "assistant", "content": node_id}],
                )
            ],
        )

    dag = ExecutionDAG()
    for super_node in (
        node("seg-1a-1", "agent-run-1a"),
        node("seg-1b-1", "agent-run-1b"),
        node("seg-1a-2", "agent-run-1a"),
    ):
        dag.add_event(super_node)
    dag.add_edge("seg-1a-1", "seg-1b-1", EdgeType.MENTION)
    dag.add_edge("seg-1b-1", "seg-1a-2", EdgeType.COMPLETION)
    supers = event_codec.dag_to_supernodes(
        dag, ordering=["seg-1a-1", "seg-1b-1", "seg-1a-2"]
    )

    tokenizer_digest = "sha256:" + hashlib.sha256(b"tokenizer-pg41").hexdigest()
    template_digest = "sha256:" + hashlib.sha256(b"chat-template-pg41").hexdigest()
    exports = {}
    for agent_run_id in ("agent-run-1a", "agent-run-1b"):
        projected = event_codec.supernodes_for_agent_run(supers, agent_run_id=agent_run_id)
        records = []
        for super_node in projected:
            for turn in super_node.nodes:
                for message in turn.messages:
                    records.append(
                        {
                            "sequence": len(records) + 1,
                            "role": message["role"],
                            "content": message["content"],
                            "message_id": f"msg-{super_node.node_id}-{len(records) + 1}",
                            "agent_run_id": super_node.agent_id,
                        }
                    )
        trajectory = retokenize_structured_trajectory_bound(
            records,
            encode_messages=_encode_messages,
            tokenizer_digest=tokenizer_digest,
            chat_template_digest=template_digest,
            loss_mask_agent_run_id=agent_run_id,
        )
        exports[agent_run_id] = {
            "segments": len(projected),
            "records": len(records),
            "tokens": len(trajectory.payload),
            "loss_mask_all_true": trajectory.loss_mask is not None
            and all(trajectory.loss_mask),
            "recipe_bound": trajectory.tokenizer_digest == tokenizer_digest
            and trajectory.chat_template_digest == template_digest,
            "provenance": len(trajectory.provenance) == len(records),
        }
    # Source DAG unchanged: the projection never mutates the assembled log.
    source_intact = [super_node.node_id for super_node in supers] == [
        "seg-1a-1",
        "seg-1b-1",
        "seg-1a-2",
    ]
    return {"exports": exports, "source_intact": source_intact}


def run(spec_root: Path | None = None) -> dict:
    root = validate_spec_root(spec_root)
    manifest_path = root / "fixtures" / "pg41_batch_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"missing PG-41 batch manifest fixture: {manifest_path}")

    # The Host runs the barrier and writes its released-barrier facts to a
    # JSON file; GMS consumes that file (facts are the cross-repo contract,
    # never a cross-repo import).
    with tempfile.TemporaryDirectory(prefix="pg41-") as tmp:
        facts_path = str(Path(tmp) / "host_barrier_facts.json")
        host = run_driver(
            HOST_REPO, ["benchmark-barrier", str(manifest_path), facts_path]
        )
        gms = run_driver(GMS_REPO, ["batch-diagnosis", facts_path, str(manifest_path)])

    manifest = host["manifest"]
    agents = host["agents"]
    rooms = host["rooms"]
    barrier = host["barrier"]
    admission = host["admission"]
    restart = host["restart"]
    host_ledger = host["ledger"]

    batch = gms["batch"]
    gms_ledger = gms["ledger"]
    gms_runs = gms["runs"]

    export = _per_agent_export()
    exports = export["exports"]

    failed_runs = [entry for entry in gms_runs if entry["terminal_state"] == "failed"]
    diagnosis_walked = all(
        entry["stages"] == EXPECTED_RUN_STAGES
        and entry["annotations"] == 1
        and entry["reads"] >= entry["private_spaces"] >= 1
        for entry in gms_runs
    )
    barrier_ok = (
        barrier["blocked_while_pending"]
        and barrier["blocked_while_partial"]
        and barrier["released_when_durable"]
    )
    admission_ok = (
        admission["refused_while_pending"]
        and admission["refused_while_partial"]
        and admission["admitted_when_released"]
        and restart["recovered_rows"] == restart["expected_rows"] > 0
        and restart["staged_blocked"]
        and restart["crashed_worker_rejected"]
        and admission["unleased_stage_rejected"]
        and admission["stale_worker_rejected"]
        and admission["restage_refused"]
        and admission["double_admission_rejected"]
        and admission["check_vs_new_outbox_blocked"]
        and admission["double_worker_distinct"]
        and admission["foreign_stage_rejected"]
    )

    steps = [
        step(
            "manifest-parse",
            manifest["logical_runs"] == 3
            and manifest["strict_parse"]
            and batch["manifest_match"],
            {
                "batch": manifest["evaluation_batch_id"],
                "arm": manifest["arm_id"],
                "seed": manifest["seed"],
                "logical_runs": manifest["logical_runs"],
                "strict_parse": manifest["strict_parse"],
                "gms_manifest_match": batch["manifest_match"],
            },
        ),
        step(
            "agents-run",
            agents["rooms"] == 3
            and agents["failed_preserved"]
            and agents["non_terminal_rejected"]
            and host_ledger["keyed_by"] == "logical_run_id"
            and host_ledger["entries"] == 3
            and host_ledger["all_terminal"]
            and host_ledger["failed"] == 1
            and not host_ledger["failed_dropped"],
            {
                "rooms": agents["rooms"],
                "failed_preserved": agents["failed_preserved"],
                "non_terminal_closing_rejected": agents["non_terminal_rejected"],
                "ledger": host_ledger,
            },
        ),
        step(
            "room-close",
            rooms["closed"] == rooms["expected"] and rooms["guard_rejected"],
            {
                "closed": rooms["closed"],
                "expected": rooms["expected"],
                "receipts_guard_rejections": rooms["guard_rejected"],
            },
        ),
        step(
            "batch-barrier",
            barrier_ok and admission_ok,
            {
                "barrier": barrier,
                "admission": admission,
                "restart": restart,
            },
        ),
        step(
            "diagnosis",
            gms_ledger["all_terminal"]
            and gms_ledger["failed_preserved"] == 1
            and len(gms_runs) == manifest["logical_runs"]
            and diagnosis_walked
            and len(failed_runs) == 1
            and failed_runs[0]["failure_reason"] == "AGENT_TIMEOUT",
            {
                "ledger": gms_ledger,
                "runs": [
                    {
                        "logical_run_id": entry["logical_run_id"],
                        "terminal_state": entry["terminal_state"],
                        "failure_reason": entry["failure_reason"],
                        "cut_id": entry["cut_id"],
                        "private_spaces": entry["private_spaces"],
                        "reads": entry["reads"],
                        "annotations": entry["annotations"],
                        "confidence": entry["confidence"],
                    }
                    for entry in gms_runs
                ],
            },
        ),
        step(
            "per-agent-export",
            len(exports) == 2
            and export["source_intact"]
            and all(entry["records"] > 0 and entry["recipe_bound"] for entry in exports.values()),
            {
                "agents": sorted(exports),
                "source_dag_intact": export["source_intact"],
                "records": {name: entry["records"] for name, entry in exports.items()},
            },
        ),
    ]

    assertions = [
        assertion(
            "known_set_all_terminal",
            agents["non_terminal_rejected"]
            and rooms["closed"] == rooms["expected"]
            and host_ledger["all_terminal"]
            and gms_ledger["all_terminal"]
            and gms_ledger["known_set_size"] == manifest["logical_runs"],
            f"rooms with non-terminal registered runs cannot enter closing; the "
            f"whole known set ({host_ledger['entries']} logical runs keyed by "
            f"{host_ledger['keyed_by']}) is terminal and all "
            f"{rooms['closed']}/{rooms['expected']} rooms closed; GMS re-verifies "
            f"known_set_size={gms_ledger['known_set_size']} before batch diagnosis",
        ),
        assertion(
            "failed_runs_preserved",
            agents["failed_preserved"]
            and host_ledger["failed"] == 1
            and not host_ledger["failed_dropped"]
            and gms_ledger["failed_preserved"] == 1
            and len(failed_runs) == 1
            and failed_runs[0]["failure_reason"] == "AGENT_TIMEOUT",
            f"the failed run {failed_runs[0]['logical_run_id']} keeps its "
            f"reason={failed_runs[0]['failure_reason']} after "
            f"{failed_runs[0]['attempts']} attempts in both ledgers "
            f"(failed_dropped={host_ledger['failed_dropped']}), and GMS still "
            f"diagnoses it (failed_preserved={gms_ledger['failed_preserved']})",
        ),
        assertion(
            "next_episode_blocked_by_evidence",
            barrier_ok
            and admission["refused_while_pending"]
            and len(admission["blocking_while_pending"]) == restart["expected_rows"]
            and admission["refused_while_partial"]
            and admission["admitted_when_released"]
            and restart["staged_blocked"]
            and restart["crashed_worker_rejected"]
            and admission["unleased_stage_rejected"]
            and admission["stale_worker_rejected"]
            and admission["restage_refused"]
            and admission["double_admission_rejected"]
            and admission["check_vs_new_outbox_blocked"]
            and admission["double_worker_distinct"]
            and admission["foreign_stage_rejected"],
            f"admission refuses while {len(admission['blocking_while_pending'])} "
            f"outbox rows are pending and while draining is partial, and admits "
            f"the next episode only once all {restart['expected_rows']} rows "
            f"commit; after a crash the {restart['recovered_rows']} recovered "
            f"rows (staged included) keep the barrier closed "
            f"(staged_blocked={restart['staged_blocked']}, "
            f"crashed_worker_rejected={restart['crashed_worker_rejected']}); "
            f"only a live claim lease stages or commits a row "
            f"(unleased={admission['unleased_stage_rejected']}, "
            f"stale={admission['stale_worker_rejected']}, "
            f"foreign={admission['foreign_stage_rejected']}, "
            f"restage_refused={admission['restage_refused']}), two workers "
            f"hold distinct leases "
            f"(double_worker_distinct={admission['double_worker_distinct']}), "
            f"and the batch gate admits once under the store lock "
            f"(double_admission_rejected={admission['double_admission_rejected']}, "
            f"check_vs_new_outbox_blocked={admission['check_vs_new_outbox_blocked']})",
        ),
        assertion(
            "per_agent_reconstructed_export",
            export["source_intact"]
            and all(
                entry["records"] > 0
                and entry["tokens"] == entry["records"]
                and entry["loss_mask_all_true"]
                and entry["recipe_bound"]
                and entry["provenance"]
                for entry in exports.values()
            ),
            f"per-agent projections for {sorted(exports)} retokenize from "
            f"structured records under the frozen recipe with an aligned loss "
            f"mask; the source DAG stays intact "
            f"(source_intact={export['source_intact']})",
        ),
        assertion(
            "room_close_requires_receipts",
            rooms["guard_rejected"],
            f"closing->closed without complete receipts is rejected by the "
            f"SC-4.5 guard before all {rooms['closed']}/{rooms['expected']} "
            f"rooms close on real receipt facts",
        ),
    ]
    return build_trace(TRACER_NAME, steps, assertions)
