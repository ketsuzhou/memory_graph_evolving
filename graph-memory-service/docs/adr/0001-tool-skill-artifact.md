# ADR 0001: Model executable tools as governed Tool Skills

## Status
Accepted — 2026-09-16

## Context
GMS currently treats Skills as immutable, versioned artifacts with a governed candidate, evaluation, review, and activation lifecycle. The legacy `skill-artifact/1.0` schema defines Procedure Skill, Step Guidance, and Composite Skill. Agents repeatedly reconstruct code from textual guidance for operations that can have a stable typed interface and reusable implementation.

An executable capability cannot be treated as ordinary prompt text: arbitrary generated code, unpinned packages, or unrestricted shell access would bypass candidate immutability, replay evaluation, activation policy, provenance, and capability controls.

## Decision
Introduce **Tool Skill** as a fourth immutable `SkillArtifact` kind.

This is a destructive schema cutover: `skill-artifact/2.0` becomes the sole accepted artifact and reference schema. GMS removes parsing, validation, reference handling, migration, and compatibility support for `skill-artifact/1.0`; persisted v1 artifacts are invalid after deployment.

A Tool Skill artifact stores its identity and executable contract, not inline source code. It must pin:

- a content-addressed Tool Package reference and entrypoint;
- typed input, output, and error schemas;
- an execution policy, resource limits, and declared capabilities;
- a versioned Validation Contract; and
- source provenance from repeated successful executions and/or an LLM-generated Tool Candidate.

Every Tool Candidate is immutable and non-executable. Both extraction from repeated executions and LLM synthesis use the same candidate lifecycle and must pass static gates plus the Candidate Replay Gate before authoritative activation. Arm C policy-driven activation applies in every environment: the server activates only after independently revalidating the immutable Activation Policy Decision, exact candidate/evaluation evidence, policy version, coverage, and expected active version. Human curator approval is not an activation gate. A single source execution or opportunity may create only a draft. A directly extracted candidate becomes Candidate Replay Gate eligible only after at least two independently provenanced successful executions with the same typed interface, capability envelope, and Validation Contract. An LLM-synthesized candidate instead requires at least two independent Tool Opportunity Evidence records plus its own successful Tool Contract Validation on the associated frozen fixtures.

At each Consolidation Cut, GMS may automatically create a non-executable Draft Tool Opportunity from normalized source evidence, candidate interface, permission-envelope, and fixture suggestions. A draft cannot build, execute, or activate a Tool Package. Runtime records each exact Tool Skill revision as a staged Tool Interaction Signal — matched, exposed, selected, invoked, completed, patch_applied, contract_verified, or outcome_correlated — and never infers contribution from an earlier stage.

A Tool Package must be produced by a controlled builder. Its signed Tool Build Attestation binds its source snapshot digest, build recipe digest, dependency lock digest, OCI image digest, builder identity, build-log digest, and SBOM or dependency manifest. A registry image digest alone is not sufficient provenance.

The first release permits only content-addressed OCI Tool Packages. They run with a pinned entrypoint, no network, bounded CPU/memory/time, read-only inputs, and a copy-on-write workspace output layer. The only write effect is a sandboxed, auditable workspace patch. Runtime automatically applies it only to the task-private copy-on-write workspace after validation, records the exact tool revision, base workspace digest, structured change manifest, Validation Contract result, and rollback point, and never commits to a shared or external target. A patch must bind to a base workspace digest, declare allowed paths, produce a structured change manifest, and satisfy its Validation Contract. Network access and external-system side effects are disabled by default and out of scope.

An activated Tool Skill is not exposed as an unfiltered global tool. An applicable Step Guidance or Composite Skill must reference its exact revision, and runtime revalidates its artifact version, capability grant, workspace base digest, and execution policy before invocation. Tool Package access is limited to runtime-mounted task-private inputs and workspace paths; outputs and logs are disclosure-classified evidence, from which only allowed structured results, digests, and redacted fragments may persist.

Authoritative activation requires both Tool Contract Validation on fixed fixtures and Agent-level Candidate Replay Gate evaluation on relevant task cases. Tool Contract Validation verifies typed input/output/error behavior, resource budgets, patch invariants, and declared determinism or idempotency boundaries. It does not substitute for task-level agent benefit.

A Tool invocation fails closed on timeout, schema failure, policy violation, unappliable patch, or Validation Contract failure: no partial patch persists. Any retry, fallback, or recovery must be an explicit bounded policy in the referencing Step Guidance or Composite Skill.

Tool Candidates may consolidate only when their typed interfaces, execution policies, capability envelopes, and Validation Contract are compatible. Semantically similar but operationally distinct tools remain separate Skills.

Tool Skills activate first as probationary revisions: Guarded Tool Exposure limits them to their evidence-backed Applicability Envelope, and a versioned promotion policy must require further independent validation before that envelope widens. A read-only Tool Skill requires one additional independent, non-degrading case; a Controlled Workspace Patch Tool Skill requires two. Promotion cases are excluded from the Skill's source evidence, Tool Contract Validation, and initial Candidate Replay Gate, and must satisfy their Validation Contract with no safety event. Typed interface compatibility alone never grants global availability.

A Tool Safety Circuit Breaker immediately stops invocation on attestation failure, capability escalation, execution-policy violation, or Tool Execution Data Boundary breach, preserving immutable history and audit evidence. Ordinary task failure or isolated effectiveness loss triggers review, applicability narrowing, or successor-candidate generation rather than immediate deletion.

Composite Skills may compose activated Tool Skills through exact artifact references. Step Guidance remains responsible for applicability, interpretation, and fallback; Tool Skills encapsulate the bounded execution.

## Consequences
- Agents can invoke activated reusable implementations with structured inputs rather than regenerate equivalent code from prose.
- Tool packages, permissions, and validation become replayable and auditable versioned authority.
- Tool extraction has a higher admission burden than textual guidance: package provenance, interface, fixtures, capability declaration, and validation contract are mandatory.
- External side-effecting tools require a future ADR and separate authority model.
- The artifact schema, validation gates, projector, runtime retrieval, and sandbox adapter must be extended consistently before this decision can be implemented.

## Alternatives considered

### Keep tools as Step Guidance text
Rejected. It retains repeated text-to-code translation, cannot pin executable provenance, and cannot enforce a typed runtime interface.

### Allow arbitrary generated shell commands as tools
Rejected. It bypasses capability controls and makes replay, audit, and rollback unreliable.

### Create an ungoverned tool registry separate from Skills
Rejected. It duplicates lifecycle, provenance, and activation policy while breaking composition with existing Skills.
