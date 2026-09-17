# Graph Memory Service Domain Context

Graph Memory Service preserves the distinction between historical interaction evidence, diagnosis-derived guidance, and executable Skill authority.

## Step Guidance Language

**Skill artifact**:
The authoritative, immutable, versioned guidance or executable capability that an Agent may use after evaluation, review, and activation. Every artifact has exactly one stable kind: Procedure Skill, Step Guidance, Composite Skill, or Tool Skill.
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
The server-controlled authority that atomically activates an exact candidate in every environment only after independently revalidating its matching immutable Activation Policy Decision, candidate/evaluation evidence, coverage, policy version, and expected active version. It does not require or accept human curator approval.
_Avoid_: Skill proposer, replay evaluator, human approval gate

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



**Candidate Replay Gate**:
The Arm C, pre-activation causal evaluation boundary that runs a frozen Candidate Skill artifact set or Skill Graph delta against pinned replay cases before it may become authoritative. It distinguishes reusable cross-task capability from task-local Runtime Advisory Skill use.
_Avoid_: same-task retry, held-out benchmark evaluation, implicit activation

**Skill Generalization Outcome**:
The explicit semantic form selected when a set of locally valid Guidance Revision Proposals is generalized: a new generalized Skill Lineage, a successor revision, a guarded branch or specialization, or a Composite Skill. Textual similarity alone is insufficient to select a merge.
_Avoid_: mandatory merge, widest-applicability rule, silent source replacement

**Co-Usage Signal**:
An evidence-backed, non-authoritative observation that exact Skill revisions are beneficially selected or adopted together in a Replayable Context Snapshot. It may suggest retrieval, composition, or future relation analysis, but does not itself create an executable dependency or compatibility guarantee.
_Avoid_: causal dependency, unconditional compatibility, LLM-invented graph edge

**Conditional Conflict Record**:
A merge-review fact that two Skill claims are incompatible only under a declared applicability overlap and semantic location, together with its evidence and required resolution. It remains a proposal-level gating fact until a separately designed graph relation proves broader semantics.
_Avoid_: unconditional conflicts-with edge, rejected candidate, generic disagreement



**Guided Continuation Validation**:
A non-rewindable validation mode that introduces an exact Candidate Skill's guidance only into the remaining executable state of its source task and records observable completion or recovery. Its success is continuation support, not a paired counterfactual proof; it may prioritize clustering and replay or support restricted probation, but cannot independently authorize authoritative activation.
_Avoid_: Replay Validation, same-start baseline, automatic activation

**Validation Contract**:
A versioned, task-family-declared specification of the runtime-observable commands, artifact invariants, protocol milestones, resource budgets, and safety constraints used by Candidate Replay Gate evaluation. It excludes official benchmark evaluators, gold ground truth, and LLM self-declared success.
_Avoid_: hidden evaluator access, runtime-selected success criterion, benchmark-label leakage



**Tool Skill**:
An immutable, versioned executable Skill artifact that exposes a typed interface and pins a content-addressed Tool Package, execution policy, capability set, and Validation Contract. Only an activated exact Tool Skill revision may be invoked at runtime.
_Avoid_: inline shell snippet, unpinned executable, textual Step Guidance

**Tool Package**:
The content-addressed executable payload and entrypoint referenced by a Tool Skill, such as a sandboxed OCI image, Wasm module, or other approved runtime package. It is distinct from the Skill artifact that governs its identity, permissions, applicability, and activation.
_Avoid_: mutable latest tag, candidate body, arbitrary local binary

**Controlled Workspace Patch**:
A Tool Skill result that proposes an auditable workspace change bound to an exact base workspace digest, declared allowed paths, a structured change manifest, and Validation Contract invariants. It is the only Tool Skill write effect in the first release.
_Avoid_: unrestricted filesystem write, untracked mutation, external side effect

**Tool Candidate Provenance**:
The immutable evidence that a Tool Skill candidate was extracted from repeated successful executions or synthesized by a model from a cluster, together with its source artifacts, interface, fixtures, permissions, and Validation Contract. Candidate provenance does not grant runtime execution authority.
_Avoid_: model assertion, one-off source snippet, activation approval



**OCI Tool Package**:
The sole first-release Tool Package format: a content-addressed OCI image run with a pinned entrypoint, no network, bounded CPU/memory/time, read-only inputs, and a copy-on-write workspace output layer. Other executable formats require a separate compatibility and replay decision.
_Avoid_: mutable image tag, host-process execution, multi-runtime first release

**Guarded Tool Exposure**:
The runtime rule that exposes an activated Tool Skill to an Agent only through an applicable Step Guidance or Composite Skill's exact Tool Skill reference. Runtime revalidates the artifact version, capability grant, workspace base digest, and execution policy before invocation.
_Avoid_: global unfiltered tool palette, candidate invocation, retrieval-as-execution

**Tool Consolidation Compatibility**:
The requirement that Tool Candidates may merge only when their typed input/output/error interfaces are isomorphic or safely compatible, their execution policies are compatible, their capability envelope does not expand, and they share one Validation Contract. Semantically similar but operationally distinct tools remain separate Skills.
_Avoid_: universal tool, permission expansion by merge, prose-only similarity merge



**Tool Patch Application Transaction**:
The runtime operation that validates and applies an activated Tool Skill's Controlled Workspace Patch only to a task-private copy-on-write workspace. It records the exact Tool Skill revision, base workspace digest, change manifest, Validation Contract result, and rollback point; it never commits to a shared or external target.
_Avoid_: agent-retyped patch, shared workspace mutation, external deployment

**Tool Candidate Eligibility**:
The admission threshold for spending Candidate Replay Gate budget on a Tool Candidate. A directly extracted candidate requires at least two independently provenanced successful executions with the same typed interface, capability envelope, and Validation Contract. An LLM-synthesized candidate instead requires at least two independent Tool Opportunity Evidence records and its own successful Tool Contract Validation on the associated frozen fixtures. A single source execution or opportunity may create a draft but cannot become eligible for authoritative activation.
_Avoid_: one-off active tool, repeated retries as independent evidence, threshold-free promotion

**Tool Opportunity Evidence**:
An immutable, independently provenanced observation that a task required or successfully performed a normalized, potentially reusable operation, with its source execution, command/patch structure or failure pattern, expected interface, and frozen fixture reference. It is source evidence for extraction or synthesis, not evidence that a new Tool Candidate has executed successfully.
_Avoid_: Tool Candidate Verification, model intuition, raw transcript similarity

**Tool Candidate Verification**:
The Tool Contract Validation evidence produced by actually running one constructed Tool Candidate on its fixed fixtures. It establishes tool-level behavior but not Agent-level task benefit; Candidate Replay Gate remains required before authoritative activation.
_Avoid_: source opportunity, continuation success alone, benchmark-label evaluation

**Draft Tool Opportunity**:
A non-executable, automatically created record at a Consolidation Cut that groups normalized Tool Opportunity Evidence and proposes a candidate interface, permission envelope, and fixture references. It cannot build, execute, or activate a Tool Package until it meets Tool Candidate Eligibility.
_Avoid_: automatic tool build, activation request, authoritative Skill

**Tool Interaction Signal**:
An immutable observation of one exact Tool Skill revision at one task context, recorded as matched, exposed, selected, invoked, completed, patch_applied, contract_verified, or outcome_correlated. No earlier stage implies a later stage or tool contribution; these signals are the evidence basis for Co-Usage Signal and future Tool Opportunity Evidence.
_Avoid_: exposure-as-success, invocation-as-benefit, inferred causal proof

**Tool Build Attestation**:
The signed, immutable proof binding a Tool Package's source snapshot digest, build recipe digest, dependency lock digest, OCI image digest, builder identity, build-log digest, and SBOM or dependency manifest. Only an attested package may be bound to a Tool Candidate.
_Avoid_: registry image digest alone, mutable build, unverifiable generated package



**Tool Contract Validation**:
The fixture-level validation of one exact Tool Skill revision under its Validation Contract. It verifies typed input/output/error behavior, resource budgets, patch invariants, and declared determinism or idempotency boundaries; it does not by itself prove task-level Agent benefit.
_Avoid_: benchmark evaluator, agent-level causal evaluation, tool exit code alone

**Tool Execution Data Boundary**:
The rule that an OCI Tool Package may access only explicit runtime-mounted task-private inputs and workspace paths. Its outputs, logs, and patch manifests are disclosure-classified evidence; only allowed structured results, digests, and redacted fragments may persist beyond the task.
_Avoid_: implicit GMS memory access, cross-Room read, host filesystem access, full-workspace log export

**Tool Invocation Failure**:
A fail-closed result of an activated Tool Skill invocation caused by timeout, schema failure, policy violation, unappliable patch, or Validation Contract failure. No partial patch is retained; recovery is an explicit bounded policy in the referencing Step Guidance or Composite Skill.
_Avoid_: runtime infinite retry, partial mutation, silent fallback



**Probation Tool Skill**:
An activated Tool Skill revision initially exposed only within its evidence-backed Applicability Envelope. It may widen that envelope only under a versioned promotion policy after further independent validation; it never becomes globally available merely because its typed interface matches a task.
_Avoid_: global availability on first activation, schema-match authorization, candidate tool

**Tool Safety Circuit Breaker**:
The fail-closed runtime control that immediately stops invocation of a Tool Skill revision on attestation failure, capability escalation, execution-policy violation, or Tool Execution Data Boundary breach while preserving immutable history and audit evidence. Ordinary task failure or isolated effectiveness loss triggers review, applicability narrowing, or successor-candidate generation rather than immediate deletion.
_Avoid_: erasing history, one-task functional auto-revocation, delayed safety response



**Tool Probation Promotion Policy**:
The versioned rule for widening a Probation Tool Skill's Applicability Envelope using independent cases excluded from its source evidence, Tool Contract Validation, and initial Candidate Replay Gate. A read-only Tool Skill requires one additional independent non-degrading case; a Controlled Workspace Patch Tool Skill requires two. Every promotion case must satisfy its Validation Contract and have no safety event.
_Avoid_: source-case double counting, schema-only expansion, unbounded global rollout



**Advisory Candidate Exposure**:
The explicitly non-authoritative retrieval of an unvalidated Procedure Skill or Step Guidance candidate with its status and usage evidence. It may inform an Agent and accumulate adoption or outcome-correlated observations, but cannot execute a Tool Package, apply a patch, grant authority, or imply causal validation.
_Avoid_: active Skill, executable Tool Candidate, silent prompt injection

**Skill Usage Projection**:
A privacy-controlled, dynamic projection of staged Skill Interaction Signals between exact Skill or candidate revisions and derived Context Profiles. It records evidence-backed matching, selection, adoption, verification, and outcome correlation without altering the normative Skill Path Graph or granting execution authority.
_Avoid_: normative dependency graph, raw prompt graph, exposure-as-utility

**Evaluation Usage Isolation**:
The rule that interaction signals from held-out benchmark evaluation are recorded only for diagnostic reporting during that EvaluationBatch. They cannot influence its candidate consolidation, validation, activation, Applicability Envelope, retrieval ranking, or promotion; any later reuse requires an explicit post-evaluation promotion record.
_Avoid_: test-to-train feedback, online benchmark tuning, hidden ledger mutation

**Usage Evidence Policy**:
The policy that Diagnosis Utility Assessments, reuse frequency, independent Context Profile coverage, and outcome-correlated observations may prioritize advisory retrieval, exploration, consolidation, Composite Skill proposal generation, and Candidate Replay Gate scheduling, but never automatically activate, promote, or expand the authority of a Skill.
_Avoid_: popularity-based activation, unqualified reuse count, causal claim from exposure

**Diagnosis Utility Assessment**:
An immutable, evidence-linked post-execution assessment by a Diagnosis Agent of one exact Skill or Advisory Candidate revision in one derived Context Profile. It records the skill's returned path, addressed Agent, explicit adoption evidence, observed contribution score, confidence, counterevidence, and rationale under a versioned rubric. It is observational utility evidence, not paired causal proof or activation authority.
_Avoid_: self-reported success, exposure count, benchmark gold label

**Co-Usage Composite Proposal**:
A governed Composite Skill proposal automatically drafted by consolidation when a versioned co-usage policy finds sufficient independent, evidence-backed co-usage of exact active, probationary, or advisory text Skill revisions in compatible Context Profiles. The draft must name its source refs, guard, intended control flow, evidence, and policy threshold, then enters the ordinary Arm A generalization, validation, and publication pipeline; co-usage alone never creates an active Composite Skill.
_Avoid_: direct graph mutation, popularity-based composition, Tool Candidate execution

**Skill Artifact Schema Cutover**:
The destructive replacement of the canonical SkillArtifact schema with `skill-artifact/2.0`, including Tool Skill support. GMS rejects `skill-artifact/1.0` artifacts and references; there is no dual-read compatibility path or data migration.
_Avoid_: v1/v2 coexistence, lazy migration, compatibility shim
