package evaluator

import (
	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
)

// Decide runs the protected evaluation pipeline over one frozen input:
//
//  1. input sanity (nil/malformed input is a programming error, not a
//     decision);
//  2. the §7.11 document validates against the authority schema with its
//     digest recomputation — a malformed/forged document is a
//     deterministic REPLAY_RESULT_INVALID rejection;
//  3. a non-succeeded replay (nondeterminism, overflow) cannot support a
//     decision → inconclusive with the canonicalizer's specific failure
//     code (Contract §13.7.1 R4: unattributable failures are
//     inconclusive; the regression evidence is not trustworthy);
//  4. static gates re-recorded; exact refs; fixture completeness; the
//     count recompute (infra separation); critical slices; cost budget —
//     any failure rejects, utility never overrides a hard gate;
//  5. source-head freshness and reference-envelope usability — unusable
//     decision inputs are inconclusive, not candidate verdicts;
//  6. the U1 constrained-Pareto comparison with integer cross
//     multiplication — unprovable arithmetic is inconclusive
//     (UTILITY_ARITHMETIC_OVERFLOW); no strict primary improvement
//     (including cost-only) rejects;
//  7. otherwise accepted.
func (e *Evaluator) Decide(cmp Comparator, in Input) (*Decision, error) {
	if in.ResultDoc == nil {
		return nil, newError(ReasonReplayResultInvalid, "nil replay result document")
	}
	if in.Candidate.CandidateID == "" || in.Candidate.BodyDigest == "" {
		return nil, newError(ReasonReplayResultInvalid, "decision input carries no exact candidate ref")
	}
	if err := validateComparator(cmp); err != nil {
		return nil, err
	}

	findings := newFindings()
	e.evaluateSchemaDigest(findings, in.ResultDoc)

	status, _ := contract.AsString(in.ResultDoc["status"])
	if status != replay.StatusSucceeded && status != replay.StatusFailed && status != replay.StatusInconclusive {
		// Already caught by the schema gate; fail closed regardless.
		return e.writeDecision(cmp, in, findings, OutcomeRejected, []string{ReasonReplayResultInvalid})
	}

	if findings.results[GateSchemaDigest].Passed && status != replay.StatusSucceeded {
		// The replay evidence itself is not usable: the specific closed
		// failure code travels into the decision reason codes (Contract
		// §13.7.1 R4).
		reason, _ := contract.AsString(in.ResultDoc["failure_reason_code"])
		if reason == "" {
			reason = ReasonReplayInconclusive
		}
		findings.inconclusive = appendUnique(findings.inconclusive, reason)
		return e.writeDecision(cmp, in, findings, OutcomeInconclusive, findings.inconclusive)
	}

	findings.evaluateStaticGates(&in)
	findings.evaluateExactRefs(&in, in.ResultDoc)
	findings.evaluateFixtureCompleteness(in.ResultDoc, in.ReleaseRule)
	findings.evaluateInfraSeparation(&in, in.ResultDoc)
	findings.evaluateCriticalSlices(cmp, in.ResultDoc, in.Records)
	findings.evaluateCostBudget(cmp, in.ResultDoc)
	findings.evaluateSourceHeads(&in)
	findings.evaluateEnvelope(&in, in.ResultDoc)

	// Deterministic rejections outrank inconclusive inputs; the U1
	// comparison only runs when everything else is green (its overflow is
	// the remaining inconclusive path).
	if len(findings.reasons) > 0 {
		return e.writeDecision(cmp, in, findings, OutcomeRejected, findings.reasons)
	}
	if len(findings.inconclusive) > 0 {
		return e.writeDecision(cmp, in, findings, OutcomeInconclusive, findings.inconclusive)
	}

	utilityRaw, _ := contract.AsObject(in.ResultDoc["utility_vector"])
	parsed := utilityVectorOf(utilityRaw)
	if !parsed.ok {
		findings.fail(GateUtilityPareto, ReasonReplayResultInvalid)
		return e.writeDecision(cmp, in, findings, OutcomeRejected, findings.reasons)
	}
	verdict, err := CompareUtility(parsed.vector, in.CandidateCases, in.Envelope, in.EnvelopedCases,
		cmp.ComparisonMode, cmp.MinImprovementRatioNum, cmp.MinImprovementRatioDen)
	if err != nil {
		findings.fail(GateUtilityPareto, ReasonUtilityArithmeticOverflow)
		return e.writeDecision(cmp, in, findings, OutcomeInconclusive, []string{ReasonUtilityArithmeticOverflow})
	}
	if verdict == VerdictNotImproved {
		findings.fail(GateUtilityPareto, ReasonUtilityNotParetoImproved)
		return e.writeDecision(cmp, in, findings, OutcomeRejected, findings.reasons)
	}
	findings.pass(GateUtilityPareto)
	return e.writeDecision(cmp, in, findings, OutcomeAccepted, nil)
}

// validateComparator checks the frozen policy shape (integers, closed
// mode, non-empty critical domains).
func validateComparator(cmp Comparator) error {
	if cmp.CostBudgetUnits < 0 {
		return newError(ReasonReplayResultInvalid, "comparator cost budget %d negative", cmp.CostBudgetUnits)
	}
	if cmp.ComparisonMode != ModeRate && cmp.ComparisonMode != ModeRaw {
		return newError(ReasonReplayResultInvalid, "comparator mode %q outside {rate,raw}", cmp.ComparisonMode)
	}
	if cmp.MinImprovementRatioNum < 1 || cmp.MinImprovementRatioDen < 1 {
		return newError(ReasonReplayResultInvalid, "comparator improvement ratio %d/%d invalid", cmp.MinImprovementRatioNum, cmp.MinImprovementRatioDen)
	}
	if len(cmp.CriticalDomains) == 0 {
		return newError(ReasonReplayResultInvalid, "comparator freezes no critical slice domains")
	}
	return nil
}
