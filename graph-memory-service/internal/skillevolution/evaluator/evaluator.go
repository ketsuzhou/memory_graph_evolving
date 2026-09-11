// Package evaluator is the GMS-203 protected decision evaluator
// (GMS §5.4–§5.6, Contract §7.12, §10.2–§10.4).
//
// It consumes the canonical ReplayResult document plus the per-run
// classifications minted by the replay canonicalizer, re-derives every
// declared count independently (an infra failure or a nondeterministic
// rerun counted as a pass is REPLAY_RESULT_INVALID), applies the §10.2
// hard gates, evaluates the §10.3 U1 constrained Pareto comparator with
// integer cross multiplication only (no floats; any unprovable comparison
// is UTILITY_ARITHMETIC_OVERFLOW / inconclusive), and writes the
// authoritative §7.12 ReleaseDecision with its JCS/SHA-256 digest.
//
// Decision inputs are closed at the type level: there is no field that can
// carry a model score, a subjective utility weight or any float — utility
// evidence enters only as the replay canonicalizer's integer vectors and
// classifications (the candidate or its generating model can never
// self-evaluate, GMS §5.1).
//
// Outcome semantics (GMS §5.6): a clearly violated policy rejects; input
// that cannot support a decision (inconclusive replay, stale source heads,
// arithmetic overflow, unusable reference envelope) is inconclusive;
// otherwise accepted. `accepted` authorizes activation_pending only — it
// never equals released/active.
package evaluator

import (
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Schema file names of the decision authority shapes.
const (
	SchemaReplayResult    = replay.SchemaReplayResult
	SchemaReleaseDecision = "release-decision.schema.json"
)

// Closed reason codes (frozen system registry, GMS §11.4
// replay/evaluation/release and activation groups).
const (
	ReasonReplayResultInvalid       = replay.ReasonReplayResultInvalid
	ReasonReplayInconclusive        = replay.ReasonReplayInconclusive
	ReasonMissingFamily             = replay.ReasonMissingFamily
	ReasonCriticalRegression        = "CRITICAL_REGRESSION"
	ReasonHardGateFailed            = "HARD_GATE_FAILED"
	ReasonReferenceEnvelopeInvalid  = "REFERENCE_ENVELOPE_INVALID"
	ReasonUtilityNotParetoImproved  = "UTILITY_NOT_PARETO_IMPROVED"
	ReasonUtilityCostBudgetExceeded = "UTILITY_COST_BUDGET_EXCEEDED"
	ReasonUtilityArithmeticOverflow = replay.ReasonUtilityArithmeticOverflow
	ReasonSourceHeadStale           = "SOURCE_HEAD_STALE"
	ReasonReleaseDecisionInvalid    = "RELEASE_DECISION_INVALID"
	ReasonRefMismatch               = contract.ReasonRefMismatch
)

// Decision outcomes (Contract §7.12).
const (
	OutcomeAccepted     = "accepted"
	OutcomeRejected     = "rejected"
	OutcomeInconclusive = "inconclusive"
)

// Error is the registry-code evaluation failure; Detail is diagnostic only.
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "evaluator: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err, or "" when err is nil or
// not an evaluator/replay/validation error.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if ee, ok := err.(*Error); ok {
		return ee.ReasonCode
	}
	if code := replay.CodeOf(err); code != "" {
		return code
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	return ""
}

// StaticGate is one §10.2 static hard-gate record produced upstream by the
// GMS-202 pipeline (schema/canonicalization/digest, exact refs, required
// extensions, authority/provenance, evidence committed, permission/Host
// cap, kind/lineage, ports/DAG/retry/failure, merge conflict closure).
// The evaluator re-records them; it does not re-derive them.
type StaticGate struct {
	Code       string
	Passed     bool
	RecordRefs []contract.VersionedRef
}

// ReleaseRule is the exact release rule frozen by VersionedRef: the
// fixture domains the frozen fixture set must cover (source_a+source_b+
// overlap for merges, the single-source domain for ordinary revisions).
type ReleaseRule struct {
	Ref             contract.VersionedRef
	RequiredDomains []string
}

// Input is the closed decision input. No field carries a model score, a
// float, or any subjective weight.
type Input struct {
	DecisionID           string
	DecisionVersion      int64 // defaults to 1
	Candidate            contract.CandidateArtifactRef
	ResultDoc            map[string]any       // §7.11 document (replay.Result.Doc())
	Records              []replay.RunRecord   // per-run classifications (replay.Result.Records())
	Envelope             replay.UtilityVector // reference-envelope vector (replay.Result.Envelope())
	EnvelopedCases       int64                // baseline-side total cases
	CandidateCases       int64                // candidate-side total cases
	RequestBaselines     []contract.SkillArtifactRef
	ValidationRecordRefs []contract.VersionedRef
	StaticGates          []StaticGate
	ReleaseRule          ReleaseRule
	ExpectedSourceHeads  []contract.SkillArtifactRef // frozen expectations (merge: both sources)
	ObservedSourceHeads  []contract.SkillArtifactRef // currently active heads
}

// Comparator is the U1 comparator policy frozen by the exact
// UtilityComparatorRef (GMS §5.5): the cost budget, the critical-slice
// domains, the dimension comparison mode and the frozen improvement-ratio
// definition. All integers.
type Comparator struct {
	Ref                    contract.VersionedRef
	CostBudgetUnits        int64
	CriticalDomains        []string
	ComparisonMode         string // ModeRate | ModeRaw
	MinImprovementRatioNum int64  // frozen ratio numerator (>= 1)
	MinImprovementRatioDen int64  // frozen ratio denominator (>= 1)
}

// Evaluator is the protected decision service.
type Evaluator struct {
	gates *validation.Gates
}

// New wires the evaluator to the shared validation gates (the authority
// schemas of §7.11/§7.12 and their x-digest preimages).
func New(gates *validation.Gates) (*Evaluator, error) {
	if gates == nil {
		return nil, fmt.Errorf("evaluator: nil validation gates")
	}
	return &Evaluator{gates: gates}, nil
}
