package replay

import (
	"encoding/json"

	"river2.dev/graph-memory-service/internal/contract"
)

// UtilityVector is the Contract §10.3 integer utility vector: five
// dimensions, integers only, no floats anywhere (Contract §6.1.4).
type UtilityVector struct {
	TaskSuccessCount        int64 // maximize (primary)
	CriticalBranchPassCount int64 // maximize (primary)
	RecoverySuccessCount    int64 // maximize (primary)
	InconclusiveCaseCount   int64 // minimize (primary)
	ExecutionCostUnits      int64 // minimize, budget-constrained, never a primary improvement
}

// Doc returns the decoder-model §7.11 utility vector object.
func (u UtilityVector) Doc() map[string]any {
	return map[string]any{
		"task_success_count":         num64(u.TaskSuccessCount),
		"critical_branch_pass_count": num64(u.CriticalBranchPassCount),
		"recovery_success_count":     num64(u.RecoverySuccessCount),
		"inconclusive_case_count":    num64(u.InconclusiveCaseCount),
		"execution_cost_units":       num64(u.ExecutionCostUnits),
	}
}

// FixtureOutcome is one Contract §7.11 fixture_outcomes entry: the exact
// fixture/baseline pair, the paired integer counts, the critical-regression
// count of the slice and the deterministic paired output digest.
type FixtureOutcome struct {
	FixtureRef              contract.VersionedRef
	Domain                  string
	BaselineRef             map[string]any // decoder-model §7.3 ref
	BaselinePassed          int64
	CandidatePassed         int64
	TotalCases              int64
	CriticalRegressionCount int64
	OutputDigest            string
}

// Doc returns the decoder-model §7.11 fixture outcome object.
func (o FixtureOutcome) Doc() map[string]any {
	return map[string]any{
		"fixture_ref":               versionedRefDoc(o.FixtureRef),
		"domain":                    o.Domain,
		"baseline_ref":              deepCopyObject(o.BaselineRef),
		"baseline_passed":           num64(o.BaselinePassed),
		"candidate_passed":          num64(o.CandidatePassed),
		"total_cases":               num64(o.TotalCases),
		"critical_regression_count": num64(o.CriticalRegressionCount),
		"output_digest":             o.OutputDigest,
	}
}

// RunRecord is the per-run classification the canonicalizer hands to the
// decision evaluator so every declared count can be re-derived
// independently (GMS §5.3: nothing may masquerade as success).
type RunRecord struct {
	PacketID   string
	Family     string
	Domain     string
	Side       string
	Critical   bool
	Recovery   bool
	Category   string // CategorySemanticPass | CategorySemanticFail | CategoryInfra | CategoryNondeterministic
	ReasonCode string
}

// Passed reports whether the run is a semantic pass of its case.
func (r RunRecord) Passed() bool { return r.Category == CategorySemanticPass }

// Result is the authoritative canonical Contract §7.11 ReplayResult minted
// by the canonicalizer. The Doc is the wire truth; the aggregate vectors
// and run records are the evaluator inputs derived from the same
// canonicalization.
type Result struct {
	replayResultID    string
	replayRequestRef  contract.VersionedRef
	candidateDoc      map[string]any
	status            string
	outcomes          []FixtureOutcome
	utility           UtilityVector
	envelope          UtilityVector
	candidateCases    int64
	envelopeCases     int64
	failureReasonCode string
	resultDigest      string
	records           []RunRecord
}

// Status returns succeeded | failed | inconclusive.
func (r *Result) Status() string { return r.status }

// FailureReasonCode returns the closed failure reason (set iff status !=
// succeeded).
func (r *Result) FailureReasonCode() string { return r.failureReasonCode }

// ResultDigest returns the JCS/SHA-256 digest of the §7.11 core.
func (r *Result) ResultDigest() string { return r.resultDigest }

// Outcomes returns the per-family fixture outcomes (sorted by fixture id).
func (r *Result) Outcomes() []FixtureOutcome {
	out := make([]FixtureOutcome, len(r.outcomes))
	copy(out, r.outcomes)
	return out
}

// Utility returns the candidate-side integer utility vector.
func (r *Result) Utility() UtilityVector { return r.utility }

// Envelope returns the reference-envelope (baseline-side) integer utility
// vector (Contract §10.4).
func (r *Result) Envelope() UtilityVector { return r.envelope }

// CandidateTotalCases returns the number of observed candidate-side runs.
func (r *Result) CandidateTotalCases() int64 { return r.candidateCases }

// EnvelopeTotalCases returns the number of observed baseline-side runs.
func (r *Result) EnvelopeTotalCases() int64 { return r.envelopeCases }

// Records returns the per-run classifications (sorted deterministically).
func (r *Result) Records() []RunRecord {
	out := make([]RunRecord, len(r.records))
	copy(out, r.records)
	return out
}

// Doc returns the decoder-model §7.11 document including result_digest.
func (r *Result) Doc() map[string]any {
	outcomes := make([]any, 0, len(r.outcomes))
	for _, outcome := range r.outcomes {
		outcomes = append(outcomes, outcome.Doc())
	}
	doc := map[string]any{
		"schema_version":     "gms.replay-result.v1",
		"replay_result_id":   r.replayResultID,
		"replay_request_ref": versionedRefDoc(r.replayRequestRef),
		"candidate_ref":      deepCopyObject(r.candidateDoc),
		"status":             r.status,
		"fixture_outcomes":   outcomes,
		"utility_vector":     r.utility.Doc(),
		"result_digest":      r.resultDigest,
	}
	if r.failureReasonCode != "" {
		doc["failure_reason_code"] = r.failureReasonCode
	}
	return doc
}

// versionedRefDoc renders a VersionedRef as decoder model (exact digits).
func versionedRefDoc(ref contract.VersionedRef) map[string]any {
	return map[string]any{
		"id":      ref.ID,
		"version": json.Number(ref.Version),
		"digest":  ref.Digest,
	}
}
