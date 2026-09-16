# Graph Memory Service Domain Context

Graph Memory Service preserves the distinction between historical interaction evidence, diagnosis-derived guidance, and executable Skill authority.

## Step Guidance Language

**Skill artifact**:
The authoritative, immutable, versioned guidance that an Agent may use after replay evaluation, review, and activation. Every artifact has exactly one stable kind: Procedure Skill, Step Guidance, or Composite Skill.
_Avoid_: Skill Node, mutable Graph content

**Skill Lineage**:
The stable identity of one reusable Skill across immutable revisions of one Skill artifact kind. Artifact kind never changes within a Skill Lineage; converting a Procedure Skill into Step Guidance creates a derived Skill Lineage.
_Avoid_: artifact version, content hash, cross-kind in-place upgrade

**Procedure Skill**:
A named, versioned sequence of process instructions that does not claim checkpoint-derived Causal Context or Future Path Summary. It may be evaluated and composed like any other Skill artifact.
_Avoid_: evidence-free Step Guidance, unversioned runbook

**Skill Node**:
A non-authoritative Graph projection of one exact Skill artifact version and its evidence, evaluation, applicability, and relationships.
_Avoid_: executable Skill, Skill authority

**Observed Step Instance**:
An immutable record of one Agent decision or action at one Decision Checkpoint in one historical Conversation Path.
_Avoid_: reusable guidance, Skill Step

**Decision Checkpoint**:
A server-authoritative Interaction DAG Segment close at which the information available to the working Agent is frozen for diagnosis and replay.
_Avoid_: wall-clock timestamp, arbitrary message offset

**Conversation Path**:
A versioned, deterministic linear view of one group conversation derived from its Host-owned Interaction DAG and conversation message order for incremental diagnosis.
_Avoid_: lossless Interaction DAG, Skill control flow, causal proof

**Diagnosis Agent**:
A post-execution analyzer that examines a frozen Conversation Path through Segment Analysis Artifacts, reads original Segment evidence on demand, and proposes or revises Step Guidance at selected Decision Checkpoints.
_Avoid_: Segment summarizer, Skill activator, Interaction DAG authority

**Step Guidance**:
Reusable, versioned guidance for an Agent decision at an applicable Decision Checkpoint, consisting of Causal Context, Decision Policy, and Future Path Summary.
_Avoid_: raw trajectory, hidden chain of thought, observed action

**Causal Context**:
The relevant prior information at a Decision Checkpoint that can causally affect the current decision.
_Avoid_: full history dump, unrelated context

**Decision Policy**:
The structured, reviewable rationale and action guidance for choosing what to do at a Decision Checkpoint, without relying on hidden chain-of-thought text.
_Avoid_: raw private reasoning, final action only

**Future Critical Step**:
A later step whose observed possibility or outcome is material to the current decision and therefore useful to the Agent when choosing the current action.
_Avoid_: complete future transcript, irrelevant later event

**Future Path Summary**:
A structured, similar-merged hindsight summary of Future Critical Steps observed after a Decision Checkpoint on successful or failed Conversation Paths.
_Avoid_: unstructured trajectory suffix, deploy-time forecast

**Path Discovery Replay**:
A replay mode that samples additional Conversation Paths from a Decision Checkpoint so diagnosis can discover successful and failed future branches for a successor Skill candidate.
_Avoid_: causal proof, in-place mutation of an evaluated Skill

**Causal Evaluation Replay**:
A replay mode that holds one Skill candidate version fixed while comparing its intervention against a pinned baseline.
_Avoid_: path discovery, mutable candidate evaluation

**Privileged Distillation Context**:
Hindsight-only Step Guidance, including Future Path Summary, supplied as additional information during on-policy distillation rather than claimed as information originally visible at the Decision Checkpoint.
_Avoid_: Causal Context, original Agent observation

**Skill Path Graph**:
The normative, versioned control-flow view of Step Guidance and its possible future branches, kept distinct from the historical Interaction DAG that supplies its evidence.
_Avoid_: Interaction DAG copy, observed-history authority


**Skill Anchor**:
The immutable association between one Step Guidance lineage and one concrete Decision Checkpoint on one Conversation Path.
_Avoid_: reusable Skill identity, mutable checkpoint

**Experience Tree**:
A derived tree formed only by exact shared prefixes of immutable Conversation Paths; semantic similarity across different trees creates suggestions or relationships rather than shared identity.
_Avoid_: embedding-clustered identity, Interaction DAG authority

**Decision Policy Delta**:
A proposed evidence-grounded change to an existing Decision Policy that must be synthesized with accumulated support and contradiction rather than applied as last-write-wins text.
_Avoid_: direct overwrite, partial activation

**Future Path Branch**:
A canonical successful or failure-risk branch in a Future Path Summary, retaining its conditions, ordered Future Critical Steps, outcome distribution, and evidence support.
_Avoid_: raw trajectory suffix, evidence-free recommendation

**Observed Future Path Evidence**:
An immutable reference to the concrete Conversation Path, Decision Checkpoint, critical steps, outcome, and replay conditions supporting or contradicting a Future Path Branch.
_Avoid_: merged-away provenance, aggregate-only score

**Skill Similarity Suggestion**:
A version-pinned, evidence-backed proposal that two Skill Anchors or Step Guidance artifacts may be related, merged, specialized, or composed; it has no mutation authority.
_Avoid_: automatic merge, executable relation

**Guidance Exposure Policy**:
The explicit policy distinguishing Step Guidance shown to an online Agent from Step Guidance supplied as Privileged Distillation Context to a teacher or critic.
_Avoid_: implicit prompt injection, untracked training context


**Segment Evidence Seal**:
A Host-authoritative declaration that the exact scoped evidence set for one closed Interaction DAG Segment is complete and immutable for a given policy version.
_Avoid_: timeout-based completeness, partial EvidenceBatch

**Segment Analysis Artifact**:
An immutable, future-blind, scope-bound summary of one sealed Segment's visible inputs, observed actions, outputs, local outcome, and evidence references, consumed by projection and terminal diagnosis without becoming historical fact.
_Avoid_: Future Path Summary, terminal diagnosis, Skill artifact

**Terminal Diagnosis**:
The post-execution analysis of a frozen Conversation Path that uses Segment Analysis Artifacts and on-demand source evidence to select Decision Checkpoints and propose Step Guidance revisions.
_Avoid_: per-Segment summarization, automatic activation


**Diagnosis Working Set**:
A durable, bounded record of candidate checkpoints, dependencies, outcome transitions, and unresolved questions accumulated while Terminal Diagnosis scans a Conversation Path.
_Avoid_: prompt transcript, replacement for source evidence

**Guidance View**:
A context-budgeted rendering of a complete Step Guidance artifact that selects applicable, high-value, and high-risk branches while preserving expandable references to omitted structure.
_Avoid_: truncated artifact, independent Skill version

**Future Horizon**:
The immediate, phase, or terminal distance at which a Future Critical Step matters to the current decision.
_Avoid_: raw message distance, wall-clock estimate

**Decision Utility Test**:
The requirement that a Future Critical Step may enter Step Guidance only when knowing it can change the current action, preparation, validation, fallback, delegation, or stopping decision.
_Avoid_: future-event importance alone, trajectory recap

**Guidance Revision Proposal**:
A complete immutable successor candidate produced by Terminal Diagnosis from structured Part 1, Part 2, and Part 3 deltas; it has no execution authority before evaluation, review, and activation.
_Avoid_: active Skill mutation, partial candidate

**Composite Skill**:
A versioned Skill artifact that references exact lower-level Skill artifact versions and adds only their acyclic control flow, data flow, permission requirements, bounded retry policies, and failure handling. It has no independent Causal Context, Decision Policy, or Future Path Summary, and it never copies child guidance.
_Avoid_: copied child guidance, unvalidated Skill bundle, cyclic control-flow graph, top-level Step Guidance


**Guidance Lineage**:
The stable identity of one reusable Step Guidance across immutable revisions, distinct from its content hash and concrete Skill Anchors.
_Avoid_: artifact revision, checkpoint identity

**Skill Branch**:
A stable lineage of one conditional successful or failure-risk route within a Step Guidance artifact, preserved across revisions and mergers through explicit aliases and provenance.
_Avoid_: array position, content hash identity

**Branch Guard**:
A structured observable condition and human-readable explanation controlling the applicability of one Skill Branch at a Decision Checkpoint.
_Avoid_: hidden future condition, unstructured confidence

**Guidance Merge Proposal**:
An immutable multi-source proposal that pins every source Guidance Lineage revision and supplies a complete successor artifact with explicit conflict resolution.
_Avoid_: projection node merge, silent Skill absorption


**Historical Evidence Disposition**:
The observed-success, observed-failure, or mixed-outcome classification of the historical paths supporting a Skill Branch, visible to an Agent independently of whether the branch is active.
_Avoid_: execution permission, causal proof

**Evaluation Disposition**:
The active, advisory, uncovered, inconclusive, refuted, or superseded status controlling how a Skill Branch may appear in a Guidance View.
_Avoid_: historical outcome, hidden activation state

**Source Fidelity**:
The requirement that every Future Critical Step, ordering claim, outcome label, and merged condition remain entailed by its cited Conversation Path evidence.
_Avoid_: replay utility, semantic plausibility

**Branch Coverage**:
The replay evidence that an applicable Skill Branch was successfully exercised, avoided, recovered from, or shown inapplicable under pinned conditions.
_Avoid_: whole-artifact success alone, raw trial count

**Probation Skill**:
An activated Skill artifact whose applicability remains limited to contexts compatible with its initial evidence until a separately evaluated scope expansion is activated.
_Avoid_: draft Skill, globally validated Skill

**On-Policy Distillation**:
Privileged-information distillation in which a Student generates an output from ordinary input, a Teacher scores the same output prefixes with an exact Skill artifact added to its context, and an output-token reverse KL objective trains the Student toward the Teacher.
_Avoid_: offline imitation on unrelated outputs, forward-KL distillation, Skill activation


**Activation Policy Decision**:
An immutable automatic pass or reject decision derived from exact proposal, evaluation, coverage, authority, and policy versions without human judgment or waiver.
_Avoid_: curator approval, proposer self-approval

**Evolution Activator**:
The restricted authority that atomically activates an exact candidate only after independently revalidating its matching Activation Policy Decision and expected active version.
_Avoid_: Skill proposer, replay evaluator

**Pi Group Chat Host**:
The sole Host authority for Room, Agent, Delivery, Segment, message order, observed interaction events, and terminal conversation-run outcomes admitted by this Graph Memory Service.
_Avoid_: Multica Host, payload-asserted Host identity

**Conversation Path Seal**:
A Pi Group Chat Host declaration that freezes the exact Segment Evidence Seals, deterministic order, terminal outcome, scope, and derivation policy of one completed conversation run.
_Avoid_: entire long-lived Room, GMS-inferred terminality


## Runtime Advisory Skill Evolution Language

**Runtime Advisory Skill Proposal**:
An immutable, evidence-linked Skill change proposal that may be retrieved and explicitly adopted during a task, while remaining visibly non-authoritative until its exact revision passes the required evaluation, decision, and activation path.
_Avoid_: Memory Note, Active Skill, implicit authority

**Memory Explore Agent**:
A persistent Room participant that explores memory in response to the evolving Room context or an explicit mention, then publishes exact references to applicable authoritative Skills or Runtime Advisory Skill Proposals. It may draft an adaptation proposal but does not rank, evaluate, or activate its own output authoritatively.
_Avoid_: deterministic retrieval service, Skill activator, unbounded search loop

**Replayable Context Snapshot**:
An immutable, scope-bound capture of the task context, observable environment, and pinned state needed to reproduce a Decision Checkpoint and compare replay branches.
_Avoid_: raw prompt only, mutable Room state, semantic context label

**Skill Interaction Signal**:
An immutable observation of one distinct stage in a Skill's relationship to a task context: matched, served, selected, adopted, verified, outcome-correlated, or causally supported. No earlier stage implies a later one.
_Avoid_: undifferentiated usage count, exposure-as-success, self-reported causal proof

**Diagnosis Verdict**:
A post-execution critic disposition of supported, refuted, or inconclusive for an exact Skill revision in one frozen trajectory, with uncertainty and evidence references. It may contribute evaluation evidence but does not authorize replay or activation.
_Avoid_: replay trigger, causal proof, activation decision

**Replay Experiment Control**:
An explicit, auditable authorization to run a pinned Skill proposal revision against a pinned replay case manifest. It is independent of the Diagnosis Verdict and does not itself imply successful evaluation.
_Avoid_: Diagnosis Agent command, implicit replay enqueue, causal result


**Skill Adoption Declaration**:
An Agent's immutable statement that one exact Skill or proposal revision influenced its decision at one Decision Checkpoint in one Replayable Context Snapshot. A later diagnosis may support or refute the declaration without rewriting it.
_Avoid_: recommendation, retrieval exposure, inferred success

**Trajectory Diagnosis**:
A Terminal Diagnosis result describing the overall problems, critical failures, recoveries, and outcome transitions of one frozen Conversation Path.
_Avoid_: per-Skill causal verdict, replay result, aggregate reward only

**Skill Exposure Diagnosis**:
A Diagnosis Verdict scoped to one exact Skill or proposal revision at one Decision Checkpoint, distinct from the Trajectory Diagnosis and from other Skill exposures in the same Conversation Path.
_Avoid_: whole-run attribution, exposure count, causal proof

**Applicability Envelope**:
The explicit, versioned boundary of task context, environment, permissions, and observable conditions within which an authoritative Skill revision has evaluation support.
_Avoid_: global validity by default, embedding cluster alone, mutable runtime guess


**Benchmark Completion Barrier**:
The frozen boundary reached when every Agent assigned to one known evaluation task or task batch has reached a terminal state, allowing its conversation paths and trainable trajectories to be sealed for diagnosis and export.
_Avoid_: long-lived Room termination, timeout-only completion, partial Agent set

**Consolidation Cut**:
An externally requested, watermark-pinned boundary over a long-lived production Room that selects immutable closed evidence for diagnosis and memory consolidation without declaring the Room terminal.
_Avoid_: Room shutdown, mutable latest view, implicit task completion

**Critic Verification**:
A Diagnosis Agent's evidence-grounded judgment that an adopted Skill's expected behavior and claimed outcome are supported by the observable frozen trajectory. Missing or insufficient source evidence yields an inconclusive result rather than verification.
_Avoid_: self-report, causal proof, evidence-free success label

**Multi-Agent Trainable Trajectory Export**:
An immutable export that preserves each Agent Run's trainable trajectories together with the shared Interaction DAG topology and exact Agent, Session, Segment, and branch provenance needed to interpret them.
_Avoid_: flattened merged tensor stream, author inference from messages, destructive partial export

**Replay Skill Mutation Branch**:
A counterfactual branch whose intervention changes the Skill that a Memory Explore Agent publishes into a Room, together with every descendant interaction across Agents that may have observed that changed Skill.
_Avoid_: Explore Agent message only, ordinary search branch, unmarked replay lineage

**Replay-Excluded Training View**:
A derived, auditable view of a Multi-Agent Trainable Trajectory Export that excludes selected Replay Skill Mutation Branch roots and all provenance-tainted descendants while preserving their shared pre-fork ancestors.
_Avoid_: mutation of the source export, text-based branch guessing, descendant reward leakage

**EvaluationBatch Manifest**:
The frozen known set of logical runs (task/episode × arm × seed with their expected Agent rosters) that one benchmark EvaluationBatch's completion barrier closes over; it is fixed before the batch starts and never inferred from observed traffic.
_Avoid_: dynamic completion guessing, post-hoc roster scan, mutable known set

**Logical Run**:
The canonical unit of benchmark completion identified by the frozen six-field tuple evaluation batch, task, episode, arm, seed, and logical run ID; retries create new Attempts under the same Logical Run and never new Logical Runs.
_Avoid_: concatenated run-id string as source of truth, retry as new run, treating logical_run_id as display-only

**Attempt**:
One execution pass of a Logical Run with its own attempt identity and terminal state (succeeded, failed, or aborted); retry budget exhaustion is what terminates the Logical Run.
_Avoid_: silent re-execution, attempt-less reruns

**Room Epoch**:
The monotonically bumped version guarding Room lifecycle transitions; the active→closing CAS atomically rejects new root turns, delegations, deliveries, and segments, and stale-epoch writes fail with an explicit error.
_Avoid_: check-then-act closing, agent-initiated shutdown, best-effort quiet period

**Consolidation Cut Job**:
The asynchronously executed, explicitly staged lifecycle of one Production Consolidation Cut (queued, freezing, frozen, diagnosing, consolidating, consolidating_partial, replaying, activating, completed) that resumes from the failed stage, never mutates its frozen manifest, and records partial failures per Space; a diagnosis-failed Cut ends terminal `partially_failed`, never `completed`.
_Avoid_: synchronous all-or-nothing request, hidden retry semantics, partial success as complete

**Diagnosis Capability**:
The short-lived, read-only, cut-scoped credential that enumerates exactly which Room-bound private Spaces one Diagnosis execution may read, with expiry and a digest binding it to the append-only private-read audit chain.
_Avoid_: standing diagnosis token, scope inferred from latest Room bindings, unaudited private reads

**Per-Source Cursor**:
The per-(Space, Room/source) consumption position over evidence batches, kept as a contiguous prefix plus explicit sparse gap set, so one Room's Cut never consumes another Room's pending evidence.
_Avoid_: single global watermark, gap-free skipping, unproven compaction

**Required Durable Sink**:
The one consumer identity frozen in an export manifest whose verified digest receipt is the only license to clean up source AReaL Sessions; all other consumers read the durable artifact.
_Avoid_: first-consumer cleanup, TTL-as-acknowledgement, reader-held session lifetime

**Trajectory Fidelity**:
The declared class of a training payload's provenance; `reconstructed` marks data retokenized from the canonical structured transcript under a frozen recipe and restricts it to SFT, preference learning, or explicitly reconstruction-tolerant offline training.
_Avoid_: on-policy claims over reconstructed data, fabricated logprobs or weight versions, fidelity inference from content

**Disclosure Gate**:
The deterministic, fail-closed decision point that judges whether a Diagnosis output element may move from its source authority domain to a wider one, based on declared disclosure labels rather than model judgment.
_Avoid_: prompt-based secrecy, keyword masking as policy, post-hoc sensitivity review

**Authorization Epoch**:
The tenant/object-scoped counter bumped on deletion or permission withdrawal and validated by consumers at training start and checkpoint boundaries, so revoked artifacts and stale caches stop being consumed.
_Avoid_: cache TTL as revocation, delete-the-row-only semantics, mid-run silent continuation

**Promotion Record**:
The independent, auditable record that moves a benchmark-activated Skill Proposal into the production namespace after validating source EvaluationBatch coverage, privacy clearance, and the exact target base Skill revision.
_Avoid_: benchmark score as promotion license, cross-namespace pointer overwrite, unverified base revision

**Replayable Context Snapshot**:
The content-addressed freeze of every controllable dependency of a replay — Skill revisions, Agent configs, tool schemas and versions, sandbox image, file and database snapshot references, time/random source policy, and recorded tool results.
_Avoid_: message-prefix-only replay, latest-environment reuse, unfrozen external dependencies
