package evaluator

import (
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
)

// Comparison modes frozen by the UtilityComparatorRef:
//
//   - ModeRate: every primary dimension is compared as a rate
//     count/total-cases via integer cross multiplication (paired sides
//     with different fixture coverage stay comparable);
//   - ModeRaw: primary dimensions are compared as raw integer counts
//     (denominator 1).
const (
	ModeRate = "rate"
	ModeRaw  = "raw"
)

// Verdict is the U1 constrained-Pareto outcome of the primary dimensions.
type Verdict string

const (
	// VerdictAccept: all primary dimensions no worse than the reference
	// envelope, at least one strictly better (meeting the frozen
	// improvement ratio), and cost within its budget (checked separately).
	VerdictAccept Verdict = "accept"
	// VerdictNotImproved: some primary dimension regressed, or nothing
	// strictly improved (all-equal and cost-only improvements both land
	// here: cost is not a primary dimension, §10.3.7).
	VerdictNotImproved Verdict = "not_improved"
)

// primaryDimension is one §10.3 primary semantic dimension.
type primaryDimension struct {
	name     string
	value    func(v replay.UtilityVector) int64
	maximize bool
}

// primaryDimensions is the frozen §10.3 primary set in canonical order;
// execution_cost_units is deliberately absent (minimize, budget-bound,
// never a strict improvement).
func primaryDimensions() []primaryDimension {
	return []primaryDimension{
		{"task_success_count", func(v replay.UtilityVector) int64 { return v.TaskSuccessCount }, true},
		{"critical_branch_pass_count", func(v replay.UtilityVector) int64 { return v.CriticalBranchPassCount }, true},
		{"recovery_success_count", func(v replay.UtilityVector) int64 { return v.RecoverySuccessCount }, true},
		{"inconclusive_case_count", func(v replay.UtilityVector) int64 { return v.InconclusiveCaseCount }, false},
	}
}

// CompareUtility applies the §10.3 constrained-Pareto comparison of the
// candidate vector against the reference-envelope vector with integer
// cross multiplication only:
//
//   - rate mode compares count/total per dimension (aNum*bDen vs
//     bNum*aDen), raw mode compares counts with denominator 1;
//   - every primary dimension must be no worse (direction-aware);
//   - at least one primary dimension must be strictly better, and for
//     maximize dimensions the improvement must meet the frozen
//     improvement ratio candidate/envelope >= ratioNum/ratioDen;
//   - cost is not a primary dimension: its reduction alone is never a
//     strict improvement (checked by the cost gate separately);
//   - any negative count, non-positive denominator or overflowing cross
//     product returns UTILITY_ARITHMETIC_OVERFLOW — the comparison is
//     unprovable and MUST NOT decide.
func CompareUtility(candidate replay.UtilityVector, candidateCases int64, envelope replay.UtilityVector, envelopeCases int64, mode string, ratioNum, ratioDen int64) (Verdict, error) {
	var candidateDen, envelopeDen int64 = 1, 1
	switch mode {
	case ModeRate:
		candidateDen, envelopeDen = candidateCases, envelopeCases
	case ModeRaw:
		// denominators stay 1; the case totals are still validated — an
		// empty comparison basis is an illegal denominator (GMS §5.5:
		// denominator非法 → fail closed).
	default:
		return "", newError(ReasonUtilityArithmeticOverflow, "comparison mode %q outside {rate,raw}", mode)
	}
	if candidateCases < 1 || envelopeCases < 1 {
		return "", newError(ReasonUtilityArithmeticOverflow, "case totals %d/%d invalid (an empty comparison basis is an illegal denominator)", candidateCases, envelopeCases)
	}
	if ratioNum < 1 || ratioDen < 1 {
		return "", newError(ReasonUtilityArithmeticOverflow, "frozen improvement ratio %d/%d invalid (numerator and denominator must be >= 1)", ratioNum, ratioDen)
	}

	strict := false
	for _, dim := range primaryDimensions() {
		cand := dim.value(candidate)
		env := dim.value(envelope)
		order, err := compareRatios(cand, candidateDen, env, envelopeDen)
		if err != nil {
			return "", newError(ReasonUtilityArithmeticOverflow, "dimension %s: rate comparison %d/%d vs %d/%d is unprovable (checked cross multiplication overflowed)", dim.name, cand, candidateDen, env, envelopeDen)
		}
		// Direction-aware "no worse".
		if dim.maximize {
			if order < 0 {
				return VerdictNotImproved, nil
			}
			if order > 0 {
				// Strict improvement must also meet the frozen ratio
				// candidate/env >= ratioNum/ratioDen, proven by cross
				// multiplication only: candidate*ratioDen >= env*ratioNum.
				// (env == 0 with candidate > 0 is an unbounded
				// improvement: the cross product with env is 0 and any
				// frozen ratio is met — no division ever happens.)
				met, err := crossAtLeast(cand, env, ratioNum, ratioDen)
				if err != nil {
					return "", newError(ReasonUtilityArithmeticOverflow, "dimension %s: improvement ratio %d/%d over %d unprovable (checked cross multiplication overflowed)", dim.name, ratioNum, ratioDen, env)
				}
				if met {
					strict = true
				}
			}
		} else {
			// Minimize dimension: no worse means candidate rate <= envelope
			// rate; strictly better means strictly smaller.
			if order > 0 {
				return VerdictNotImproved, nil
			}
			if order < 0 {
				strict = true
			}
		}
	}
	if !strict {
		// Includes the all-equal and the cost-only-improvement cases:
		// cost is not a primary dimension, so its reduction alone never
		// sets strict (§10.3.7).
		return VerdictNotImproved, nil
	}
	return VerdictAccept, nil
}

// withinCostBudget checks the versioned integer cost constraint.
func withinCostBudget(costUnits, budget int64) bool {
	return costUnits >= 0 && budget >= 0 && costUnits <= budget
}
