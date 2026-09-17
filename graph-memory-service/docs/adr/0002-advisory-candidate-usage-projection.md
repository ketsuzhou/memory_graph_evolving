# ADR 0002: Expose advisory candidates through an isolated usage projection

## Status
Accepted — 2026-09-16

## Context
Some task environments cannot rewind to a frozen checkpoint. If unvalidated candidates are never visible, such tasks cannot contribute reuse, adoption, or continuation evidence, and useful candidates may never receive validation priority. Conversely, treating exposure, selection, or repeat use as activation evidence would let unvalidated guidance and executable packages bypass the governed Skill lifecycle.

The normative Skill Path Graph expresses immutable artifact lineage, structure, dependencies, evidence, and activation-derived relationships. High-volume, context-specific usage observations have different semantics, privacy requirements, and retention characteristics.

## Decision
Procedure Skill and Step Guidance candidates may be retrieved as explicitly labelled **Advisory Candidates**. The Agent can read and explicitly adopt them, while runtime records evidence-backed interaction stages and context profiles. Advisory exposure never grants execution authority or causal validation.

Tool Candidates remain non-executable. They may be inspected by explore and consolidation flows, but are never exposed to the task Agent as callable tools and cannot run a package or apply a patch before becoming an activated Probation Tool Skill.

GMS maintains a separate, privacy-controlled **Skill Usage Projection** derived from immutable interaction signals. It links exact revisions to derived Context Profiles and records stages such as matched, exposed, selected, adopted, verified, and outcome-correlated. A Diagnosis Agent adds an immutable, evidence-linked, rubric-versioned **Diagnosis Utility Assessment** for every returned skill path directed to a specific Agent. The assessment records adoption evidence, contribution score, confidence, counterevidence, and rationale. It is observational utility evidence, not paired causal proof.

A versioned aggregation policy combines Diagnosis Utility Assessments with independent Context Profile coverage, explicit adoption, outcome-correlated observations, counterevidence, and reuse frequency. It may prioritize advisory retrieval, exploration, consolidation, Composite Skill proposal generation, and Candidate Replay Gate scheduling. Reuse, score, or aggregation cannot independently activate, promote, or expand a Skill's Applicability Envelope; only server-controlled Arm C policy-driven activation may do so.

When a versioned co-usage policy reaches its evidence threshold across compatible independent Context Profiles, consolidation may automatically draft a governed Composite Skill proposal. The proposal must include exact source refs, guards, intended control/data flow, policy version, and evidence refs; it then follows the same Arm A generalization, validation, and publication pipeline as every other proposal. Co-usage never directly writes a normative relation or active Composite Skill.

Held-out evaluation signals, including Advisory Candidate retrieval and Diagnosis Utility Assessments, are recorded for diagnostics only. They are isolated from candidate consolidation, validation, activation, promotion, ranking, and envelope expansion for the active EvaluationBatch. Reusing them later requires an explicit promotion record.

## Consequences
- Non-rewindable tasks can provide evidence for candidate discovery and validation prioritization without being misrepresented as paired replay proof.
- Explore and consolidation agents gain a contextual evidence view instead of an unqualified reuse counter.
- Tool Candidate execution remains behind the existing build, validation, activation, probation, and safety gates.
- Usage storage must derive privacy-safe Context Profiles and retain evidence references, rather than persist raw prompts or workspaces as graph nodes.
- Benchmark transfer reports can distinguish authoritative/probation/advisory exposure while preventing test-to-train feedback.

## Alternatives considered

### Hide all unvalidated candidates
Rejected. It starves useful candidates of observational evidence in non-rewindable environments.

### Treat repeated use as activation evidence
Rejected. Reuse can result from repeated exposure, task simplicity, or selection bias and is not causal validation.

### Add usage edges to the normative Skill Path Graph
Rejected. Dynamic, privacy-sensitive observations would blur graph semantics and could accidentally affect execution authority or dependency reasoning.
