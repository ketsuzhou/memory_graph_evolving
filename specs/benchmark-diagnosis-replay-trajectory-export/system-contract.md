# System Contract — Benchmark Diagnosis, Production Cut, Replay & Multi-Agent Trajectory Export

```yaml
contract_id: benchmark-diagnosis-replay-export.system-contract
version: 1.1.0
status: documentation-only
binding_gate: PG-00D conformance green
decision_baseline: Q27-Q112
parallel_group: PG-00A
upstream_plan: implementation-plan.md
```

> **Status semantics.** This contract is *documentation-only* until the PG-00D
> conformance suite (golden fixtures, Go/Python parity runners) is green. The
> rulings below are binding as design decisions now; they become *executable*
> only through the frozen schemas (PG-00B), OpenAPI (PG-00C), and conformance
> corpus (PG-00D). No implementation group may claim conformance to this
> contract before that gate.

The key words MUST, MUST NOT, SHOULD, and MAY are to be interpreted as
described in RFC 2119.

Three rulings are **user-fixed verbatim** and MUST NOT be "corrected" by any
implementation:

1. **Q53 = A.** Diagnosis 默认拥有所有 private Spaces（范围见 SC-6.1：当前 Cut
   manifest 绑定的、当前 Room 关联的全部 private Spaces；不跨 Room、不跨租户），
   使用 Cut-scoped、只读、短期 capability，并执行分区输出、disclosure gate 和审计。
2. **Q60 = B.** Durable training payload 是从 canonical structured transcript
   重新 tokenized 的数据，不保存原始 rollout tensors；必须标记
   `trajectory_fidelity=reconstructed`，仅允许 SFT、偏好学习或显式支持重建数据
   的离线训练，禁止伪造 behavior logprobs、weight versions 或 on-policy fidelity。
3. **Q90 = C.** Proposal 默认归属稳定 Diagnosis Agent identity；ownership、
   visibility、evidence permissions、approval 与 activation target 是独立字段。

---

## SC-1. Authority model

- **SC-1.1** All durable artifacts belong to exactly one `tenant_id`. Cross-tenant
  reads, writes, and identity comparisons MUST be impossible at the capability
  layer (Q50, Q53, Q68, Q95, Q110).
- **SC-1.2** Every pipeline stage executes under an explicit, short-lived,
  purpose-bound capability granted to a registered principal (Q50, Q70).
  Capabilities MUST enumerate tenant, room/cut/space scope, allowed operations,
  and expiry. Long-lived universal tokens MUST NOT exist (Q70).
- **SC-1.3** A Diagnosis execution inside one Cut MAY read every private Space
  bound to that Room in the frozen Cut manifest (Q53=A, Q68). It MUST NOT read
  private Spaces of other Rooms, other tenants, or Spaces bound after the
  manifest froze (Q68). Diagnosis capabilities are read-only (Q70, Q94).
- **SC-1.4** Triggering a Production Consolidation Cut requires a Room
  consolidation capability. The caller declares room, trigger reason, and mode
  only; the server freezes the legally bound shared/private Space set and
  checks tenant/principal/grant per Space (Q50). Callers MUST NOT submit
  target Space IDs directly (Q50).
- **SC-1.5** Writing to a shared Space, expanding Proposal visibility, or
  activating into a target Skill domain requires either a deterministic
  disclosure-gate pass (SC-6.3) or an approval by the governance principal of
  the target authority domain (Q79, Q106). Prompt-level instructions MUST NOT
  be treated as access control (Q69, Q94).
- **SC-1.6** Frozen permission is an upper bound that cannot be enlarged;
  current permission governs revocation. Effective access is the intersection
  of both (Q99).

## SC-2. Identity model

- **SC-2.1** Logical run identity is the frozen field tuple
  `evaluation_batch_id, task_id, episode_id, arm_id, seed, logical_run_id`.
  Retries create new `attempt_id` values under the same logical run; they
  MUST NOT create new logical runs (Q36, Q45). Display strings MAY be encoded
  from these fields but MUST NOT be the source of truth (Q45).
- **SC-2.2** Every Agent execution is an `agent_run_id`. Dynamic delegation
  MUST atomically register the child agent run into the parent logical run's
  completion ledger before any Delivery/Segment of the child is created
  (Q37). Rosters MUST NOT be inferred by scanning final messages (Q37).
- **SC-2.3** Idempotency keys for Cut requests are scoped to
  `(tenant_id, room_id, trigger_source, idempotency_key)`; identical key +
  body returns the original Cut, identical key + different body returns a
  conflict (Q48). The server additionally derives a deterministic `cut_digest`
  from frozen inputs (Q48, Q61).
- **SC-2.4** Content identity uses digests as defined in the PG-00B schemas:
  each artifact object is canonically serialized and hashed; top-level
  manifests combine child digests (Q61). Private-content identity exposed
  across tenants MUST use tenant-scoped keyed digests (HMAC/content ID);
  canonical content digests live only in encrypted manifests (Q110).
- **SC-2.5** Skill Lineage, Skill artifact kinds, revisions, and active
  pointers keep their ADR 0041 meanings. Activation creates a new immutable
  revision and atomically moves the active pointer; rollback is a new
  activation event, never deletion (Q59, Q83).
- **SC-2.6** Export identity is `export_id` + `schema_version` + combined
  content digest (Q61). Schema upgrades create new exports; in-place rewrites
  of published artifacts MUST NOT occur (Q61, Q63).

## SC-3. Freeze, watermark, and incremental-window semantics

- **SC-3.1** A Production Consolidation Cut freezes, in one atomic manifest:
  Room sequence watermark, eligible sealed Segment IDs, per-Space exact
  evidence batch IDs, per-Space projection head and query state, previous Cut
  reference, and policy revisions (Q27, Q40, Q51, Q52). The frozen manifest is
  immutable for the life of the system (Q49, Q88).
- **SC-3.2** Room-level Cut, Space-level publication: each target Space runs
  its own CAS round; cross-Space atomic database transactions MUST NOT be
  required. A Cut reaches terminal state only when every target Space reports
  `published | duplicate | no_change | failed`; partial failures are recorded
  per Space (Q40).
- **SC-3.3** Evidence selection is by provenance, not by a single global
  watermark: evidence batches carry immutable
  `room_id/segment_id/cut provenance`, and consumption is tracked as
  per-source cursors plus explicit sparse gap sets so one Room's Cut cannot
  consume another Room's pending evidence (Q51, Q86). Compaction of consumed
  sets MUST be provably lossless for unconsumed evidence and keep a
  round→checkpoint audit chain (Q86).
- **SC-3.4** Host→GMS cut consistency uses causal receipt freezing, not
  cross-service transactions: Host freezes room sequence + sealed segments,
  waits for evidence commit receipts of exactly those segments, then GMS
  freezes batch IDs/heads and emits the immutable manifest. Missing receipts
  keep the Cut in `freezing`; timeout is a retryable failure. Silent omission
  MUST NOT occur (Q52).
- **SC-3.5** A `no_change` result MUST be returned (idempotently identified)
  when a trigger has no new sealed evidence; empty rounds MUST NOT be
  published (Q39, Q76).
- **SC-3.6** Segments still open at watermark time are deferred to the next
  Cut; they are never truncated into the current one (Q27).

## SC-4. Pipeline stages and completion semantics

- **SC-4.1** Production pipeline order is fixed:
  `freeze cut → diagnose frozen trajectories → consolidate evidence / produce
  proposal → isolated replay verification → automatic activation on pass`
  (Q41). Diagnosis MUST read the frozen Cut input, never a later latest view
  (Q41, Q111); retries of a Diagnosis revision reproduce its original frozen
  input (Q111).
- **SC-4.2** Explicit rediagnose creates a new immutable annotation revision
  bound to the source Cut and a new diagnosis policy revision; it MUST NOT
  masquerade as new evidence increment (Q76). Training views pin exactly one
  annotation revision (Q84).
- **SC-4.3** If Diagnosis fails or is inconclusive, Skill Proposal, Replay,
  and Activation MUST stop; deterministic evidence projection may continue
  (Q89). The Cut is `partially_failed` and lists per-Space/proposal results
  (Q49, Q89).
- **SC-4.4** Benchmark Completion Barrier: the barrier closes over the frozen
  `EvaluationBatchManifest` known set only. `succeeded | failed | aborted`
  are all terminal Agent states; logical runs terminal after success or
  retry exhaustion. Failed runs are first-class records for Diagnosis (Q35,
  Q36). Dynamic children join via SC-2.2 registration.
- **SC-4.5** Benchmark Rooms close atomically `active → closing → closed` via
  Room epoch CAS: closing rejects new root turns, delegations, Deliveries,
  and Segments; in-flight registered work may finish; late writes fail with a
  stale-room-epoch error; `closed` requires complete receipts (Q72, Q80).
  Production Rooms are never closed by a Cut (Q27, plan §1).
- **SC-4.6** Batch-level diagnosis starts only after the whole
  EvaluationBatch barrier; per-Room/per-run diagnosis may proceed in parallel
  inside; batch aggregation publishes only after all partitions reach
  terminal state. Batch verdicts are `complete | partial | inconclusive`;
  strict training views accept only `complete` (Q73, Q81).
- **SC-4.7** Cut jobs are cooperative-cancellation only: cancellable before
  freeze; after freeze the manifest persists and only not-yet-started stages
  stop. Published rounds/annotations never roll back; jobs end in
  `cancelled | partially_cancelled` listing completed side effects (Q88).
  Quotas are versioned per tenant/room/stage and end in auditable
  `queued | budget_exhausted` states — never silent drops (Q97).
- **SC-4.8** Cross-service stage events use transactional outbox/inbox with
  idempotent consumers; duplicate delivery MUST NOT re-publish rounds,
  annotations, replays, or Skill revisions; out-of-order events are rejected
  by state-machine version (Q112).

## SC-5. Fidelity, retokenization, and training views

- **SC-5.1** Durable training payloads are reconstructed (Q60=B): tokenized
  from the canonical structured transcript under a frozen recipe — tokenizer
  digest, chat-template digest, special-token map, truncation policy (Q66,
  Q67). Same source + recipe MUST yield identical token IDs; recipe upgrades
  create new Training Views (Q67).
- **SC-5.2** The canonical structured transcript is the stored source of
  record: role, message ID, agent run, provider call, tool call/result,
  attachment references, Segment identity (Q66, Q78). Rendered text and token
  IDs are derived artifacts (Q66). Large/binary content lives in durable
  blobs referenced by digest; irrecoverable content marks the trajectory
  `incomplete_context` and excludes it from strict views (Q78).
- **SC-5.3** Exported artifacts MUST mark `trajectory_fidelity=reconstructed`
  and the allowed training purposes (SFT, preference learning, explicit
  offline-reconstruction training). Fabricating behavior logprobs, weight
  versions, or original loss masks MUST NOT occur (Q60=B, Q65).
- **SC-5.4** Default loss-mask policy: only the target agent run's own
  assistant outputs and its generated tool-call arguments enter the loss;
  everything else is conditioning context. Mask policy is versioned; token
  spans carry source call/message IDs (Q77). Ownership MUST NOT be inferred
  from role strings alone (Q77).
- **SC-5.5** Export lifecycle is `freeze → write → acknowledge`: freeze all
  sessions/IDs/DAG/provenance; non-destructive read to temporary artifacts;
  validate every trainable segment has tensor-equivalent payload and loss
  mask; publish atomically after digest; cleanup only after the required
  durable sink acknowledges with a verified receipt (Q32, Q47, Q62). The
  same `export_id` retried MUST return the same digest; changed inputs are a
  conflict (Q32). Mid-export failure MUST pop nothing (Q32).
- **SC-5.6** Export structure is one shared DAG topology plus per-`agent_run_id`
  payload files (business, memory explore, consolidation, replay agents;
  Diagnosis annotations stored separately). Agents without trainable payload
  keep a topology-only stub with `trainable=false` (Q28, Q43). Shared
  ancestors MUST NOT be copied into per-agent paths; linear paths are derived
  on demand from pinned leaves (Q43). The existing DAG codec/assembler/merge
  code is reused; the parser MUST NOT be rewritten (Q29).
- **SC-5.7** Training Views pin: one Diagnosis annotation revision, one
  aggregation policy, one mask policy, role eligibility and mixture policy
  (Q82, Q84, Q85). Default views exclude replay branches via
  `exclude_branch_kinds[]` with `exclude_replay_branches` as the
  `replay_skill_mutation` convenience flag; the immutable archive always
  contains all branches (Q30, Q31). Replay data re-enters training only
  through explicit counterfactual/preference views that keep provenance and
  never pose as natural production distribution (Q92).
- **SC-5.8** Replay-branch exclusion is ancestry-based: exclude the branch
  root and every cross-agent descendant carrying the ancestry; keep shared
  pre-fork ancestors; descendants' reward/advantage MUST NOT back-propagate
  to ancestors; a node that read replay-branch content stays tainted after
  merges (Q30). Legacy branches without `branch_kind` are kept as `unknown`
  in archives; strict exclusion fails closed on `unknown` (Q44, Q100).
- **SC-5.9** Benchmark split governance: test-split data of one evaluation
  generation MUST NOT train models or promote Proposals before the generation
  is released; unlocks create new Training Views and record which future
  generations they pollute (Q91). Role mixing requires an explicit mixture
  policy; replay is excluded by default (Q85).

## SC-6. Diagnosis, proposals, replay, activation

- **SC-6.1** (implements Q53=A) A Diagnosis execution's read scope is exactly
  the private Spaces bound to the Room in the frozen Cut manifest, within one
  tenant, under a cut-scoped read-only capability with full audit logging of
  every private read (Q53=A, Q68, Q70).
- **SC-6.2** Original annotations bind to `call_id` or
  `(segment_id, assistant_turn_seq)` with states
  `verified | failed | inconclusive | missing`; higher levels (Segment, Agent
  run, logical run, task, Batch) are derived by versioned aggregation policy
  only (Q46, Q74, Q82). Missing evidence is `missing | inconclusive` with a
  loss mask exclusion — never reward zero or segment-average inheritance
  (Q54). Raw annotations never propagate along edges; advantages derive from
  versioned edge policies per edge type; `mentions` do not propagate without
  Skill-exposure evidence (Q55).
- **SC-6.3** Disclosure: every evidence citation, rationale fragment, and
  Proposal element carries a disclosure label; publication to a wider
  authority domain passes a deterministic disclosure gate (fail closed),
  optionally followed by explicit human approval; private rationale stays in
  its source domain (Q69, Q79). Evidence is untrusted input: control/data
  planes are separated and output is schema/citation-validated (Q94).
- **SC-6.4** Proposals are owned by the stable tenant-scoped Diagnosis Agent
  identity (`owner_agent_id`); the executing run is recorded as
  `created_by_agent_run_id` author provenance (Q90=C, Q104). Default
  visibility is the owner plus authorized governance principals only (Q103);
  initial visibility of private-derived proposals is the narrowest source
  domain (Q90=C context). Ownership, visibility, evidence permissions,
  approval, and activation target are independent fields (Q90=C).
- **SC-6.5** Proposal → Activation: proposals declare an explicit activation
  target (`target_agent/room/skill_scope`) and exact base Skill revision;
  replay verification runs in an isolated Room clone against a frozen
  Replayable Context Snapshot; activation is a CAS on the current revision
  equal to base, else `stale` (Q57, Q58, Q101, Q105). Target-domain
  governance or pre-authorized policy approves disclosure + target + base
  revision; replay pass alone grants nothing (Q106).
- **SC-6.6** Replay policy is versioned and frozen per proposal: seeds,
  baseline/intervention comparison, token/tool/time/trial budgets, pass
  thresholds; outcomes are `pass | fail | inconclusive | budget_exhausted`;
  only threshold-satisfying `pass` auto-activates (Q57). Replay tool calls
  replay recorded results; new calls are read-only or sandbox-only; denied
  and simulated calls are recorded in the ReplayResult (Q56). Unfreezable
  dependencies are marked nondeterministic and may force `inconclusive`
  (Q101).
- **SC-6.7** Benchmark activations land in the evaluation namespace only;
  production promotion requires an independent Promotion Record validating
  source coverage, privacy clearance, and target base revision (Q75, Q83).
- **SC-6.8** Diagnosis and replay model recipes (model revision, provider,
  prompt digest, tool policy, sampling config, seed) are frozen per revision;
  retries reuse the recipe; upgrades create new revisions (Q93).

## SC-7. Privacy, revocation, retention, logging

- **SC-7.1** Private evidence and derived artifacts default to their own
  authority domain: they enter training only through the producing agent's
  Training View; cross-agent or shared-model training needs explicit
  authorization and auditable sanitization (Q71).
- **SC-7.2** Encryption: tenant-level envelope encryption with independent
  key scopes for high-sensitivity private domains; manifests hold only
  non-sensitive indexes and ciphertext digests; key rotation does not change
  content identity (Q95).
- **SC-7.3** Deletion: sensitive payload is deleted or cryptographically
  erased; minimal non-sensitive tombstones and deletion receipts persist;
  dependent Training Views become `revoked` and stop consuming; model-level
  consequences follow model lineage governance (Q96).
- **SC-7.4** Revocation: artifacts carry authorization epochs; deletion or
  permission withdrawal bumps the epoch and emits a revocation event;
  consumers re-validate at training start and checkpoint boundaries; stale
  caches MUST NOT be used; running training stops at checkpoint boundaries
  and affected checkpoints/models are recorded and dispositioned by
  versioned governance policy (Q102, Q108, Q109). Single-sample gradient
  reversal MUST NOT be claimed (Q108).
- **SC-7.5** Retention is layered: tensor artifacts expire by policy;
  identity/audit tombstones, digests, cut/barrier records, segment/agent
  identity, skill revisions, exclusion summaries, and deletion receipts are
  long-lived; rebuildable Training Views keep only policy + digest (Q63).
- **SC-7.6** Logs and metrics carry IDs, states, durations, counters, and
  digests only — never message content, tool arguments/results, private
  rationale, tokens, or Skill bodies. Content debugging uses explicitly
  authorized, expiring secure traces with access audit (Q98).

## SC-8. API surface and error semantics

- **SC-8.1** Production Cut API (GMS): `POST /rooms/{room_id}/consolidation-cuts`
  returns `202` with `cut_id`, frozen watermarks, and initial state;
  `GET /consolidation-cuts/{cut_id}` is the authoritative status source;
  optional signed webhooks/event streams carry only `cut_id`, state, and
  version, with at-least-once + idempotent redelivery (Q38, Q87). Modes are
  `threshold | force` (Q39). Cancellation and explicit rediagnose are first-
  class operations (Q76, Q88).
- **SC-8.2** Export API (AReaL data plane): freeze/write/acknowledge/read/
  revoke per SC-5.5 and the frozen `export.yaml` (PG-00C).
- **SC-8.3** Error completeness levels are explicit `strict | partial`.
  Default is `strict`; identity conflicts, permission denials, digest
  mismatches, unknown replay provenance, and causal-completeness problems
  always fail closed. `partial` may skip only explicitly listed non-critical
  payloads, records exclusions in the manifest, and MUST NOT label partial
  artifacts complete (Q64).

## SC-9. Prohibited shortcuts

The following MUST NOT appear in any implementation of this contract:

- latest-view or wall-clock quiet-window consolidation in place of frozen
  watermarks (Q27, Q41);
- on-policy / original-rollout fidelity claims over reconstructed data
  (Q60=B, Q65);
- partial success reported as complete (Q49, Q64, Q81);
- prompt-based access control or model-judged sensitivity (Q69, Q79, Q94);
- message-text or Skill-diff guessing of replay provenance (Q44, Q100);
- destructive pop-on-read exports (Q32);
- silent zero-fill or average inheritance of missing rewards (Q54);
- DAG parser rewrites or shared-ancestor copying into per-agent paths
  (Q29, Q43);
- automatic cross-namespace Skill promotion from benchmark runs (Q75, Q83);
- cross-tenant digest correlation of private content (Q110).

## SC-10. Traceability index (Q27–Q112)

| Q | Ruling | Contract anchor |
|---|--------|-----------------|
| Q27 | Cut C: watermark + sealed-only, in-flight deferred | SC-3.1, SC-3.6 |
| Q28 | Export agents: business/explore/consolidation/replay; diagnosis separate | SC-5.6 |
| Q29 | Reuse DAG codec; add per-agent grouping, sidecar, replay filter | SC-5.6 |
| Q30 | Ancestry-based exclusion; merge-read taint; no ancestor reward backflow | SC-5.8 |
| Q31 | Archive keeps all; training view excludes by default; extensible kinds | SC-5.7 |
| Q32 | freeze→write→acknowledge; same digest on retry; conflict on change | SC-5.5 |
| Q33 | Layered immutable annotations; manifest references both digests | SC-6.2, SC-5.7 |
| Q34 | Isolated Room clone; lineage marking; never touches real Room | SC-6.5 |
| Q35 | Barrier closes over frozen manifest known set | SC-4.4 |
| Q36 | succeeded/failed/aborted terminal; attempts under logical run | SC-2.1, SC-4.4 |
| Q37 | Delegation registers child run atomically | SC-2.2 |
| Q38 | Async job API; POST 202; GET authoritative | SC-8.1 |
| Q39 | mode=threshold\|force; no_change idempotent; no empty rounds | SC-3.5, SC-8.1 |
| Q40 | Room-level cut, Space-level CAS publication; per-Space terminal states | SC-3.2 |
| Q41 | diagnose→consolidate→replay→activate on frozen cut | SC-4.1 |
| Q42 | Unified export partitioned by agent_role | SC-5.6 |
| Q43 | Shared DAG + per-agent references; no ancestor copying; paths on demand | SC-5.6 |
| Q44 | unknown branch_kind kept; strict mode fails closed | SC-5.8 |
| Q45 | Canonical run identity field tuple; retries = attempts | SC-2.1 |
| Q46 | call/turn-bound annotations; states verified/failed/inconclusive/missing | SC-6.2 |
| Q47 | Receiver digest verification + durable receipt before cleanup | SC-5.5 |
| Q48 | Idempotency key scope + cut_digest; Room-serial freeze | SC-2.3 |
| Q49 | Explicit stage state machine; resume from failed stage; partial listed | SC-3.2, SC-4.7 |
| Q50 | Capability-gated trigger; server computes target Spaces | SC-1.4 |
| Q51 | Provenance-selected evidence; exact batch IDs; sparse cursors | SC-3.3 |
| Q52 | Receipt-based two-phase freeze; no silent omission | SC-3.4 |
| Q53 | **A (user-fixed)**: Diagnosis reads Room-bound private Spaces | SC-1.3, SC-6.1 |
| Q54 | missing/inconclusive masked; never zero or average | SC-6.2 |
| Q55 | Reward bound to nodes; versioned edge-policy advantage | SC-6.2 |
| Q56 | Recorded tool results; sandbox-only new calls; denials recorded | SC-6.6 |
| Q57 | Versioned replay policy; frozen thresholds; 4-state outcome | SC-6.6 |
| Q58 | CAS activation on exact base revision; stale must reverify | SC-6.5 |
| Q59 | Immutable revisions + active pointer; rollback = new event | SC-2.5 |
| Q60 | **B (user-fixed)**: reconstructed retokenized payload | SC-5.1–SC-5.4 |
| Q61 | Immutable composable manifest; unknown schema fail closed | SC-2.4, SC-2.6 |
| Q62 | required_sink_id frozen at freeze; consumers read durable artifact | SC-5.5 |
| Q63 | Layered retention; tombstones + receipts persist | SC-7.5 |
| Q64 | strict\|partial explicit; security never degrades | SC-8.3 |
| Q65 | reconstructed → SFT/preference/offline only; no fabricated tensors | SC-5.3 |
| Q66 | Canonical structured transcript is the stored source | SC-5.2 |
| Q67 | Frozen retokenization recipe; deterministic IDs | SC-5.1 |
| Q68 | Private read scope = Room-bound Spaces in manifest, per tenant | SC-1.3, SC-6.1 |
| Q69 | Output partitioned by source authority; disclosure labels | SC-6.3 |
| Q70 | Cut-scoped system principal; short capabilities; read audit | SC-1.2, SC-6.1 |
| Q71 | Private evidence stays in same-authority training views | SC-7.1 |
| Q72 | Room active→closing→closed | SC-4.5 |
| Q73 | Batch barrier before batch-level diagnosis | SC-4.6 |
| Q74 | call/turn base + derived aggregation levels | SC-6.2 |
| Q75 | Benchmark activation = evaluation namespace only | SC-6.7 |
| Q76 | no_change returns original; rediagnose = new revision | SC-4.2, SC-3.5 |
| Q77 | Only target agent output/tool args in loss; versioned mask policy | SC-5.4 |
| Q78 | Content-addressed attachments; incomplete_context excludes strict | SC-5.2 |
| Q79 | Deterministic fail-closed disclosure gate (+ optional approval) | SC-6.3 |
| Q80 | Room epoch CAS; stale-epoch rejection | SC-4.5 |
| Q81 | Batch verdict complete/partial/inconclusive | SC-4.6 |
| Q82 | Versioned aggregation policy; same digest for comparisons | SC-6.2, SC-5.7 |
| Q83 | Promotion Record; authority separation | SC-6.7 |
| Q84 | Training view pins diagnosis revision | SC-4.2, SC-5.7 |
| Q85 | Role eligibility + mixture policy; replay excluded by default | SC-5.7, SC-5.9 |
| Q86 | Per-source cursors + sparse gaps; provable compaction | SC-3.3 |
| Q87 | GET authoritative; optional signed webhooks; idempotent redelivery | SC-8.1 |
| Q88 | Cooperative cancellation; frozen facts irreversible | SC-4.7 |
| Q89 | Diagnosis failure blocks proposal/replay/activation; projection continues | SC-4.3 |
| Q90 | **C (user-fixed)**: owner = stable Diagnosis Agent; fields separated | SC-6.4 |
| Q91 | Test-split freeze per evaluation generation | SC-5.9 |
| Q92 | Replay training via explicit counterfactual views only | SC-5.7 |
| Q93 | Frozen evaluator/replay model recipe | SC-6.8 |
| Q94 | Evidence untrusted; capability-layer injection defense | SC-1.5, SC-6.3 |
| Q95 | Tenant envelope encryption; private key scopes | SC-7.2 |
| Q96 | Erasure + tombstone + revoked views | SC-7.3 |
| Q97 | Versioned quotas; auditable budget_exhausted | SC-4.7 |
| Q98 | Logs carry IDs/digests only; gated secure traces | SC-7.6 |
| Q99 | Effective access = frozen ceiling ∩ current permission | SC-1.6 |
| Q100 | Legacy → unknown/legacy_unverified; strict fails closed | SC-5.8 |
| Q101 | Freeze replay environment; nondeterministic → inconclusive | SC-6.5, SC-6.6 |
| Q102 | Authorization epochs + revocation ledger | SC-7.4 |
| Q103 | Proposal default visibility minimal | SC-6.4 |
| Q104 | Stable owner identity; run as author provenance | SC-6.4 |
| Q105 | Explicit activation target; immutable revision in target domain | SC-6.5 |
| Q106 | Target-domain approval; replay pass grants nothing | SC-6.5 |
| Q107 | Matched pairs: same snapshot/env/seed policy | SC-6.6 |
| Q108 | Checkpoint-boundary stop; affected checkpoints isolated | SC-7.4 |
| Q109 | Model lineage; risk-based disposition; no fake unlearning | SC-7.4 |
| Q110 | Tenant-scoped keyed digest for private identity | SC-2.4 |
| Q111 | Diagnosis retry reproduces frozen input | SC-4.1 |
| Q112 | Transactional outbox/inbox; idempotent consumers | SC-4.8 |

## SC-11. Change control

- **SC-11.1** This contract changes only through versioned PG-00 series
  revisions. Schema, state-machine, or OpenAPI changes invalidate the PG-00D
  digest and require all direct consumers to re-run conformance (plan §8.5).
- **SC-11.2** Golden fixtures are append-only after PG-00D freezes (plan §5,
  PG-00D acceptance).
- **SC-11.3** Every normative rule above cites its origin Q decisions; new
  rulings enter as new Q numbers and traceability rows, never as silent
  edits.

## SC-11.4 Pre-freeze amendment record

The bundle had not yet been declared frozen when the kiro(terra) contract
review (gpt-5.6-terra) returned 1 P0 / 8 P1 / 3 P2 findings. All fixes were
applied **before** freezing, so SC-11.2 append-only semantics are intact. No
Q-ruling changed; every amendment tightens the machine-readable expression of
existing rulings.

| # | Finding | Amendment |
|---|---------|-----------|
| P0-1 | `diagnosing → partially_failed → consolidating → completed` let a diagnosis-failed Cut end `completed`, violating SC-4.3 | `cut_job` v2: new `consolidating_partial` stage; projection-only continuation ends at terminal `partially_failed`; explicit rediagnose `partially_failed → diagnosing` (SC-4.2); reject fixture `diagnosis_failure_reaches_completed` |
| P1-1 | Logical Run identity drift (contract 6-tuple vs CONTEXT 5 fields vs runner 2 fields) | Runner rules renamed to `logical_run_episode_unique` + `logical_run_id_unique` (SC-2.1 six-field tuple; per-manifest batch fields constant); CONTEXT.md aligned; new reject fixture `duplicate_task_episode_diff_run_id` |
| P1-2 | Cut could not prove receipts exist for exactly the sealed segments | `consolidation_cut` v2 adds required `evidence_commit_receipts[]`; rule `cut_receipts_cover_sealed_segments`; accept/missing/extra fixtures |
| P1-3 | `allowed_training_purposes` optional; `diagnosis` in `agent_role` enum | `trajectory_export` v2: purposes required (SC-5.3); diagnosis role removed (SC-5.6, annotations stored separately); 2 reject fixtures |
| P1-4 | OpenAPI→schema traceability broken | Both files: resolvable `x-contract-schema` paths; export published response exposes full immutable manifest (`PublishedExport.manifest` $ref); Error `code` enums snapshotted from `reasons.yaml` |
| P1-5 | State-machine reason codes missing from catalog; reason fields free-form | `reasons.yaml` 26→33 codes; `room_lifecycle.reason` enum-enforced; OpenAPI enums added |
| P1-6 | `active → closing` guard demanded zero open work, contradicting SC-4.5 in-flight completion | `room_lifecycle` v2: closing requires barrier+epoch CAS only; `closed` requires receipts + drained work |
| P1-7 | `queued|budget_exhausted` quota states unexpressed; `activating` not cancellable | `cut_job` v2: `queued` initial stage, `queued → cancelled (BUDGET_EXHAUSTED)` (Q97), `activating → cancelling` pre-commit guard; fixtures |
| P1-8 | Q53=A had no verifiable frozen private-scope capability model | New closed schema `diagnosis_capability.schema.json` (SC-1.2/1.3/6.1: read-only ops, room-bound spaces, expiry, audit-chain digest); cross-rule `capability_scope_within_cut`; 3 schema fixtures + 2 cross fixtures |
| P2-1 | annotationTarget description vs schema mismatch | `diagnosis_annotation` v2: closed `oneOf` call-bound / turn-bound; `target_neither` + `target_both` reject fixtures |
| P2-2 | Sink receipt equality unverifiable | Rule `sink_receipt_matches` (`sink_id == required_sink_id`, `verified_digest == content_digest`); reason `SINK_RECEIPT_MISMATCH`; reject fixture |
| P2-3 | revocation fixture name/action mismatch | Renamed `epoch_bump.json` → `payload_erased.json` |

Corpus: conformance/manifest.json v1.0.0 → v1.1.0 (35 → 53 fixtures).
State machines: `cut_job` v2, `room_lifecycle` v2; schemas amended are
self-labeled with `version_note`.
