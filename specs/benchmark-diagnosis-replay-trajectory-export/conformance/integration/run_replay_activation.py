"""PG-42 Replay and activation integration tracer.

GMS half: the Diagnosis-owned stable proposal (Q90=C), the gated
private->shared disclosure of private-derived rationale, the isolated
clone-room replay with a closed replay-verdict vocabulary (one verdict per
session+proposal: idempotent re-record, conflicting re-record rejected),
evaluation-scope-only activation on pass, promotion binding checks
(candidate/base/target must match the activation exactly), and the exact
base-revision production CAS with a stale probe. AReaL half: descendant
taint propagation computed by a real outgoing-edge walk over the execution
DAG, checked against an independently expected set, with the replay prefix
projection as the exclusion authority.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tracer_common import (
    GMS_REPO,
    assertion,
    build_trace,
    ensure_areal_on_path,
    run_driver,
    step,
    validate_spec_root,
)

TRACER_NAME = "replay-activation"

BRANCH_SEGMENT = "replay-root"
# Stated independently of the walk below: taint must cover the branch
# segment and every transitive descendant, and nothing else.
EXPECTED_TAINT = {"replay-root", "replay-child", "replay-grandchild"}
EXPECTED_PREFIX = ["root", "replay-root"]


def _taint_propagation() -> dict:
    ensure_areal_on_path()
    from customized_areal.tree_search.agents.dag import event_codec
    from customized_areal.tree_search.agents.dag.execution_dag import (
        DAGError,
        EdgeType,
        ExecutionDAG,
        SuperNode,
    )

    def node(node_id: str, agent_run_id: str) -> SuperNode:
        return SuperNode(
            node_id=node_id,
            agent_id=agent_run_id,
            issue_id=f"issue-{node_id}",
            task_id="task-replay",
            nodes=[
                SimpleNamespace(
                    node_id=f"turn-{node_id}",
                    messages=[{"role": "assistant", "content": node_id}],
                )
            ],
        )

    dag = ExecutionDAG()
    for super_node in (
        node("root", "agent-a"),
        node(BRANCH_SEGMENT, "agent-explore"),
        node("replay-child", "agent-b"),
        node("replay-grandchild", "agent-b"),
        node("sibling", "agent-c"),
    ):
        dag.add_event(super_node)
    dag.add_edge(
        "root",
        BRANCH_SEGMENT,
        EdgeType.BRANCH,
        branch_from_segment_id="root",
        branch_from_checkpoint_id="checkpoint-root",
    )
    dag.add_edge(BRANCH_SEGMENT, "replay-child", EdgeType.COMPLETION)
    dag.add_edge("replay-child", "replay-grandchild", EdgeType.COMPLETION)
    dag.add_edge("root", "sibling", EdgeType.COMPLETION)
    supers = event_codec.dag_to_supernodes(
        dag, ordering=["root", BRANCH_SEGMENT, "replay-child", "replay-grandchild", "sibling"]
    )

    prefix = event_codec.replay_prefix_for(supers, branch_segment_id=BRANCH_SEGMENT)
    prefix_ids = [super_node.node_id for super_node in prefix]

    # Real taint walk: the branch segment plus its transitive outgoing-edge
    # descendants, computed by the DAG itself — not asserted into existence.
    tainted = {BRANCH_SEGMENT} | dag.descendants(BRANCH_SEGMENT)
    descendants_complete = tainted == EXPECTED_TAINT
    # The prefix projection is the exclusion authority: it keeps ancestors +
    # the branch segment and excludes every descendant, so taint and prefix
    # overlap exactly at the branch segment; ancestors and the completion-
    # order sibling stay untainted.
    excluded_from_taint = {"root", "sibling"}
    exclusion_holds = (
        set(prefix_ids) & tainted == {BRANCH_SEGMENT}
        and not (excluded_from_taint & tainted)
    )

    # An unmarked legacy branch fails closed (SC-5.8): strict consumers
    # never guess branches from structure.
    legacy = ExecutionDAG()
    for super_node in (node("legacy-root", "agent-a"), node("legacy-branch", "agent-b")):
        legacy.add_event(super_node)
    legacy.add_edge("legacy-root", "legacy-branch", EdgeType.BRANCH)
    legacy_supers = event_codec.dag_to_supernodes(
        legacy, ordering=["legacy-root", "legacy-branch"]
    )
    try:
        event_codec.replay_prefix_for(legacy_supers, branch_segment_id="legacy-branch")
        legacy_rejected = False
    except DAGError:
        legacy_rejected = True

    source_intact = [super_node.node_id for super_node in supers] == [
        "root",
        BRANCH_SEGMENT,
        "replay-child",
        "replay-grandchild",
        "sibling",
    ]
    return {
        "prefix_ids": prefix_ids,
        "tainted": sorted(tainted),
        "expected_taint": sorted(EXPECTED_TAINT),
        "descendants_complete": descendants_complete,
        "exclusion_holds": exclusion_holds,
        "legacy_rejected": legacy_rejected,
        "source_intact": source_intact,
    }


def run(spec_root: Path | None = None) -> dict:
    validate_spec_root(spec_root)

    gms = run_driver(GMS_REPO, ["replay-activation"])
    proposal = gms["proposal"]
    disclosure = gms["disclosure"]
    replay = gms["replay"]
    activation = gms["activation"]
    taint = _taint_propagation()

    replay_vocabulary_ok = (
        replay["idempotent"]
        and replay["conflict_rejected"]
        and replay["vocabulary_closed"]
        and replay["session_rebinding_rejected"]
    )
    activation_binding_ok = (
        activation["free_form_ref_rejected"]
        and activation["forged_ref_rejected"]
        and activation["candidate_mismatch"]
        and activation["base_mismatch"]
        and activation["target_mismatch"]
        and activation["candidate_binding_rejected"]
        and activation["target_binding_rejected"]
        and activation["stale_base_activation_rejected"]
    )

    steps = [
        step(
            "proposal-registration",
            proposal["owner"] == "diagnosis-agent"
            and proposal["visibility"] == "private"
            and proposal["namespace"] == "evaluation"
            and proposal["owner_stable"],
            {
                "id": proposal["id"],
                "owner": proposal["owner"],
                "visibility": proposal["visibility"],
                "namespace": proposal["namespace"],
                "owner_stable": proposal["owner_stable"],
            },
        ),
        step(
            "disclosure-approval",
            disclosure["allowed"]
            and disclosure["reason_code"] == "DISCLOSURE_PARTITIONED"
            and disclosure["gated"],
            {
                "reason": disclosure["reason_code"],
                "citations": disclosure["citations"],
                "audit_digest": disclosure["audit_digest"],
                "public_rationale": disclosure["public_rationale"],
            },
        ),
        step(
            "isolated-replay",
            replay["session_id"] != ""
            and replay["isolated"]
            and replay["sandbox_only"]
            and replay_vocabulary_ok,
            {
                "session": replay["session_id"],
                "clone_room": replay["clone_room"],
                "baseline": replay["baseline"],
                "outcome": replay["outcome"],
                "idempotent_rerecord": replay["idempotent"],
                "conflict_rejected": replay["conflict_rejected"],
                "vocabulary_closed": replay["vocabulary_closed"],
                "session_rebinding_rejected": replay["session_rebinding_rejected"],
            },
        ),
        step(
            "taint-propagation",
            taint["prefix_ids"] == EXPECTED_PREFIX
            and taint["descendants_complete"]
            and taint["exclusion_holds"]
            and taint["legacy_rejected"]
            and taint["source_intact"],
            {
                "prefix": taint["prefix_ids"],
                "tainted": taint["tainted"],
                "expected_taint": taint["expected_taint"],
                "descendants_complete": taint["descendants_complete"],
                "exclusion_holds": taint["exclusion_holds"],
                "legacy_branch_rejected": taint["legacy_rejected"],
            },
        ),
        step(
            "evaluation-activation",
            activation["evaluation_only"]
            and replay["fail_activation_rejected"]
            and activation_binding_ok,
            {
                "evaluation_only": activation["evaluation_only"],
                "fail_verdict_activation_rejected": replay["fail_activation_rejected"],
                "free_form_ref_rejected": activation["free_form_ref_rejected"],
                "forged_ref_rejected": activation["forged_ref_rejected"],
                "candidate_mismatch_rejected": activation["candidate_mismatch"],
                "base_mismatch_rejected": activation["base_mismatch"],
                "target_mismatch_rejected": activation["target_mismatch"],
                "session_candidate_binding_rejected": activation["candidate_binding_rejected"],
                "session_target_binding_rejected": activation["target_binding_rejected"],
                "stale_base_activation_rejected": activation["stale_base_activation_rejected"],
                "activation_ref": activation["activation_ref"],
            },
        ),
        step(
            "production-cas",
            activation["stale_rejected"]
            and activation["promoted"] == "skill-pg42@2"
            and activation["rollback"] == "skill-pg42@1"
            and activation["refresh_promoted"] == "skill-pg42@3",
            {
                "record_id": activation["record_id"],
                "base_revision": activation["base_revision"],
                "promoted": activation["promoted"],
                "rollback": activation["rollback"],
                "stale_rejected": activation["stale_rejected"],
                "refresh_promoted": activation["refresh_promoted"],
                "refresh_rollback": activation["refresh_rollback"],
            },
        ),
    ]

    assertions = [
        assertion(
            "diagnosis_owned_stable_proposal",
            proposal["owner"] == "diagnosis-agent" and proposal["owner_stable"],
            f"proposal {proposal['id']} is owned by the stable Diagnosis Agent "
            f"identity ({proposal['owner']}, {proposal['visibility']}/"
            f"{proposal['namespace']}); a same-ID re-registration under a "
            f"different owner is an identity conflict (Q90=C)",
        ),
        assertion(
            "private_derived_disclosure_gated",
            disclosure["gated"]
            and disclosure["public_rationale"].startswith("disclosure://")
            and disclosure["reason_code"] == "DISCLOSURE_PARTITIONED",
            f"private-derived rationale crosses to shared only through the "
            f"partitioning gate ({disclosure['reason_code']}): derived public "
            f"rationale {disclosure['public_rationale']}, "
            f"{disclosure['citations']} citation(s), plaintext never leaves; "
            f"narrowing denial audited ({disclosure['audit_digest'][:23]}...)",
        ),
        assertion(
            "replay_isolated_clone",
            replay["isolated"]
            and replay["sandbox_only"]
            and replay["session_id"] != ""
            and replay_vocabulary_ok,
            f"replay session {replay['session_id']} runs in clone room "
            f"{replay['clone_room']} with sandbox-only side effects; one "
            f"verdict per (session, proposal): identical re-record idempotent, "
            f"conflicting re-record rejected, closed vocabulary rejects "
            f"non-enum outcomes",
        ),
        assertion(
            "descendant_taint_complete",
            taint["descendants_complete"]
            and taint["exclusion_holds"]
            and taint["legacy_rejected"]
            and taint["source_intact"],
            f"the outgoing-edge walk from {BRANCH_SEGMENT} taints exactly "
            f"{taint['tainted']} == independently expected "
            f"{taint['expected_taint']}; the replay prefix "
            f"{'->'.join(taint['prefix_ids'])} excludes every descendant and "
            f"untainted siblings stay clean; unmarked legacy branches fail "
            f"closed and the source DAG is unchanged",
        ),
        assertion(
            "pass_activates_evaluation_only",
            activation["evaluation_only"]
            and replay["fail_activation_rejected"]
            and activation["free_form_ref_rejected"]
            and activation["forged_ref_rejected"],
            f"a passing replay activates the evaluation scope only "
            f"(activation {activation['activation_ref']}); a fail verdict "
            f"cannot activate, production-namespace activation is rejected, "
            f"and free-form or forged evaluation refs never resolve",
        ),
        assertion(
            "stale_base_cas_rejected",
            activation["stale_rejected"]
            and activation_binding_ok
            and activation["refresh_promoted"] == "skill-pg42@3"
            and activation["refresh_rollback"] == "skill-pg42@2",
            f"promotion binds proposal/candidate/base/target to the activation "
            f"exactly (candidate/base/target mismatches all rejected), CAS "
            f"consumes base {activation['base_revision']} -> "
            f"{activation['promoted']}, a second claim on the consumed base is "
            f"rejected, and a fresh pass promotes "
            f"{activation['refresh_promoted']} with rollback to "
            f"{activation['refresh_rollback']}",
        ),
    ]
    return build_trace(TRACER_NAME, steps, assertions)
