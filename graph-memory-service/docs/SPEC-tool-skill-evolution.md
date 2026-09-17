# Tool Skill Evolution and Advisory Usage Projection Specification

## Status
Draft for implementation planning — 2026-09-16

## Goal
Integrate GSE-style proposal generalization, relation-aware consolidation, and validation into GMS without bypassing GMS artifact authority. The system learns from replayable and non-rewindable tasks, distinguishes advisory reuse from executable authority, and supports governed reusable Tool Skills.

## Protocol layers
The protocol names are independent of current `bench-runner` strategy names:

- **Arm A — Proposal and Generalization**: derive evidence-linked proposals, cluster them, construct candidate artifacts and candidate graph deltas, and draft relation-aware auxiliary proposals.
- **Arm B — Advisory and Guided Continuation Evidence**: retrieve text candidates as explicitly non-authoritative advice; record interaction signals and Diagnosis Utility Assessments. In non-rewindable environments, Guided Continuation provides continuation support, not paired causal proof.
- **Arm C — Candidate Validation Gate**: evaluate frozen candidates with a versioned Validation Contract. Replayable cases use paired candidate/baseline execution; incomplete pairs are inconclusive. Non-rewindable evidence may prioritize or support restricted probation but does not constitute paired proof.

Current runner names such as `warm-skill-batch`, `warm-skill-replay`, and `warm-skill-online` are implementation strategies, not protocol authority names.

## Authority and retrieval
### Candidate states

```text
Draft Candidate
  -> Advisory Candidate
  -> continuation-supported
  -> Probation Skill
  -> Authoritative Skill
```

- Procedure Skill and Step Guidance candidates may be retrieved as Advisory Candidates, including in held-out evaluation, with their non-authoritative status and usage evidence visible.
- Tool Candidates are never callable or patch-applying before activation as a Probation Tool Skill.
- Held-out evaluation interaction and diagnosis evidence is diagnostic-only during that EvaluationBatch. It must not affect consolidation, validation, activation, retrieval ranking, promotion, or Applicability Envelope expansion in that batch.

### Policy-driven activation
Arm C policy-driven activation applies in every environment. A frozen candidate activates automatically only when the server independently verifies its matching immutable Activation Policy Decision, exact candidate digest, required Tool Contract Validation, Arm C result, coverage, policy version, and expected active version. Human curator approval is not an activation gate. The legacy human-curator path is replaced during implementation; see [Activation authority decision and legacy evidence](#activation-authority-decision-and-legacy-evidence).

## Proposal generalization
- Existing-skill proposals cluster by affected exact skill set.
- New-skill proposals cluster by reason and expected-effect similarity.
- A cluster may produce one of: new generalized lineage, successor revision, guarded branch/specialization, or Composite Skill.
- LLMs may synthesize semantic candidate content, but schemas, provenance, graph relations, validation, and publication gates remain deterministic and auditable.
- A proposed update enumerates affected `depends_on`, `composes`, `similar_to`, and provenance neighbors. Auxiliary proposals require their own evidence and enter the same validation path.

## Skill Usage Projection
The normative Skill Path Graph remains limited to authority-bearing structural and provenance relations. Dynamic observations live in a separate privacy-controlled Skill Usage Projection.

### Context Profile
A Context Profile is a versioned, derived, privacy-controlled summary, not a raw prompt/workspace node. Initial fields are:

```text
task_family
language/runtime class
workspace feature tags
observable guard facts
tool-policy version
environment class
profile schema version
```

Every projection edge references exact Skill/Candidate revision, Context Profile digest, evidence refs, lineage identity, and policy/model/environment versions.

### Diagnosis Utility Assessment
After a skill path is returned and addressed to an Agent, the Diagnosis Agent records a rubric-versioned assessment for that exact skill × context observation:

```text
returned path and addressed agent
interaction stage through explicit adoption
contribution score
confidence
counterevidence
rationale
evidence refs
```

The score is observational utility evidence. It is not a benchmark-label judgment, a self-report, a causal claim, or activation authority.

### Aggregation and priority
A versioned aggregation policy ranks candidates/skills using, in priority order:

1. Diagnosis Utility Assessment score and confidence;
2. independent Context Profile and lineage coverage;
3. explicit adoption and verified/outcome-correlated observations;
4. negative assessments, refutations, and contract failures;
5. reuse frequency, deduplicated by independent lineage.

The aggregate may prioritize retrieval, explore-agent attention, consolidation, Composite Skill proposal drafting, and Arm C scheduling. It cannot activate, promote, or enlarge an Applicability Envelope.

### Co-usage to Composite Skill proposal
A versioned co-usage policy may automatically draft a **Composite Skill proposal** when exact eligible source skills show thresholded, independent, positive Diagnosis Utility Assessments in compatible Context Profiles. The proposal must contain:

```text
exact source refs
co-usage policy version and threshold evidence
compatible Context Profile summaries
guard
intended control flow and data flow
permission union
evidence refs and counterevidence
```

The draft does not directly create a graph edge or active composite. It enters Arm A, then validation and publication through the same governed pipeline as all other proposals.

## Validation Contract
Each task family references an independent, versioned Validation Contract artifact. It defines only runtime-observable signals:

```text
permitted commands and result interpretation
artifact invariants
protocol milestones
resource budgets
safety constraints
```

It must not read official benchmark evaluators, gold ground truth, or LLM-declared success. Arm C evaluates only the referenced contract.

## Tool Skill
### Schema cutover
`skill-artifact/2.0` is the only accepted SkillArtifact schema. `skill-artifact/1.0` parsing, references, dual-read compatibility, and migration are removed. Version 2 adds `tool` alongside `human_procedure`, `step_guidance`, and `composite`.

### Tool artifact and package
A Tool Skill pins:

```text
content-addressed OCI Tool Package and entrypoint
typed input/output/error schemas
execution policy, resource limits, and capabilities
Validation Contract reference
Tool Candidate provenance
```

The package is built by a controlled builder and requires a signed Tool Build Attestation containing source snapshot, build recipe, dependency lock, OCI image digest, builder identity, build log digest, and SBOM/dependency manifest.

### Runtime and patch semantics
A public `ToolRuntime` seam receives an exact activated Tool Skill ref, typed input, and task-private workspace snapshot. The first adapter runs a pinned OCI image with no network, bounded CPU/memory/time, read-only inputs, and copy-on-write output.

The sole write effect is a Controlled Workspace Patch with base workspace digest, allowed paths, structured manifest, Validation Contract result, and rollback point. Runtime may automatically apply it only to task-private copy-on-write state. Failures are typed and fail closed: no partial patch persists.

### Tool candidate eligibility
- Direct extraction requires two independently provenanced successful executions with the same interface, capability envelope, and Validation Contract.
- LLM synthesis requires two independent Tool Opportunity Evidence records plus successful Tool Contract Validation on the associated frozen fixtures.
- A single opportunity or execution can create only a Draft Tool Opportunity.

Tool Contract Validation and Agent-level Arm C validation are both required before authoritative activation.

### Probation and safety
Every Tool Skill first activates as probationary. A read-only tool requires one additional independent non-degrading promotion case; a Controlled Workspace Patch tool requires two. Promotion cases cannot overlap source evidence, contract fixtures, or initial Arm C cases.

A Tool Safety Circuit Breaker immediately stops a revision on attestation failure, capability escalation, execution-policy violation, or data-boundary breach. Ordinary task failure triggers review, applicability narrowing, or successor proposal rather than history deletion.

## TDD plan
No production implementation or test file is created until activation authority is chosen below. Once decided, implement vertically through the confirmed public seam:

```text
ToolCandidateService
```

First red tracer-bullet test behavior:

```text
Given exactly one Tool Opportunity Evidence record,
when a caller submits an LLM-synthesized Tool Candidate,
then ToolCandidateService returns Draft / not Arm-C-eligible,
and the candidate remains non-executable.
```

Follow-up tests, each as a separate red-green slice:

1. two independent direct-extraction executions qualify an extracted candidate for Arm C;
2. two independent opportunities plus successful Tool Contract Validation qualify an LLM-synthesized candidate;
3. Tool Candidate invocation is rejected before probation activation;
4. Diagnosis Utility Assessments aggregate by exact revision × Context Profile without treating exposure as adoption;
5. qualifying co-usage drafts a governed Composite Skill proposal but does not activate it.

Tests must call public service interfaces and assert returned states/errors, never private stores, hashes, or helper functions.

## Activation authority migration note
**Decision: C — policy-driven activation in every environment.** Arm C verification is the activation authority. The server independently revalidates immutable ActivationPolicyDecision, candidate and evaluation digests, required coverage, policy version, and expected active version before activation. Agents, proposers, Diagnosis Agents and HTTP callers cannot self-activate because they cannot mint or alter this decision.

The former human-curator / curation-grant activation path is historical context only. It was replaced by the ref-only `skillproposal.Service.Activate(ctx, tenant, space, decisionRef)` path and the server-owned Arm C → policyactivation worker flow. New implementations must not reintroduce `PrincipalHuman`, curator approval, `GrantOperationCandidateActivate`, or caller-supplied activation payload as activation authority.

## Non-goals for first delivery

- arbitrary shell snippets or host-process execution;
- network and external-system side effects;
- raw prompt/workspace nodes in usage projection;
- direct co-usage-to-active-composite mutation;
- held-out evaluation feedback into the active EvaluationBatch;
- legacy `skill-artifact/1.0` compatibility or migration.
