package merge

import (
	"context"
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluator"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
)

// EvaluateInput is the closed MT4 bilateral replay handoff (GMS §12.13.2):
// the frozen §7.10 request document (merge candidate with
// required_source_heads), the frozen family plan, the observed runs, and
// the U1 comparator frozen by the proposal's utility_comparator_ref.
type EvaluateInput struct {
	RequestDoc any
	Plan       *replay.Plan
	Runs       []replay.RunOutput
	Comparator evaluator.Comparator
	// DecisionID defaults to "decision-" + proposal id.
	DecisionID string
}

// EvaluateResult reports the committed §7.12 decision and the resulting
// lifecycle state (activation_pending after an accept).
type EvaluateResult struct {
	Proposal *Proposal
	Decision *evaluator.Decision
	State    string
}

// Evaluate runs the protected MT4 handoff (GMS §12.13.2; machine 9.4.6-9.4.7):
//
//  1. the §7.10 request parses strictly, is a merge request and freezes
//     exactly the proposal's canonical pair as required_source_heads;
//  2. source-head freshness (replaying -> stale on movement);
//  3. the GMS-203 canonicalizer mints the §7.11 result: a hard error or a
//     non-succeeded status is terminal inconclusive (replaying ->
//     inconclusive with the closed code — unattributable evidence never
//     decides);
//  4. the request and result freeze into the replay ledger and the
//     proposal moves replaying -> decision_pending;
//  5. the GMS-203 U1 evaluator decides over the bilateral envelope with
//     the merge release rule (source_a + source_b + overlap domains, both
//     source families critical): accepted -> decision_pending ->
//     activation_pending; rejected -> rejected; inconclusive ->
//     inconclusive. The decision commits to the evaluation ledger in the
//     same transaction as its state event.
func (s *Service) Evaluate(ctx context.Context, proposalID string, in EvaluateInput) (*EvaluateResult, error) {
	prop, _, err := s.loadInState(proposalID, StateReplaying)
	if err != nil {
		return nil, err
	}
	req, err := s.replay.ParseRequest(in.RequestDoc)
	if err != nil {
		return nil, err
	}
	if !req.IsMerge() {
		return nil, newError(ReasonRefMismatch,
			"replay request %s does not carry a merge candidate (origin_type %q)", req.ID(), req.CandidateRef().OriginType)
	}
	if err := checkRequiredHeads(req, prop.Sources()); err != nil {
		return nil, err
	}
	if err := s.requireFreshSources(ctx, prop.Sources()); err != nil {
		if CodeOf(err) == ReasonMergeSourceNotActive {
			return nil, s.staleProposal(ctx, prop, StateReplaying, []string{ReasonMergeSourceHeadStale}, nil, "%v", err)
		}
		return nil, err
	}

	// --- Bilateral canonicalization (GMS-203).
	result, cerr := s.replay.Canonicalize(req, in.Plan, in.Runs)
	if cerr != nil {
		code := replay.CodeOf(cerr)
		return nil, s.inconclusiveProposal(ctx, prop, StateReplaying,
			[]string{code}, nil, "bilateral replay canonicalization failed: %v", cerr)
	}
	if result.Status() != replay.StatusSucceeded {
		code := result.FailureReasonCode()
		if code == "" {
			code = replay.ReasonReplayInconclusive
		}
		return nil, s.inconclusiveProposal(ctx, prop, StateReplaying,
			[]string{code}, nil, "bilateral replay status %q", result.Status())
	}

	// --- Freeze the request + result and enter decision_pending.
	cand, err := s.boundCandidate(prop)
	if err != nil {
		return nil, err
	}
	if req.CandidateRef() != cand {
		return nil, newError(ReasonRefMismatch,
			"replay request addresses candidate %s but the proposal bound %s", req.CandidateRef().CandidateID, cand.CandidateID)
	}
	if err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		requestBytes, err := marshalJSON(req.Doc())
		if err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ledger.LedgerReplay, req.ID(), req.ID(), requestBytes); err != nil {
			return err
		}
		resultBytes, err := marshalJSON(result.Doc())
		if err != nil {
			return err
		}
		resultID := stringOf(result.Doc()["replay_result_id"])
		if _, err := tx.AppendEvent(ledger.LedgerReplay, req.ID(), resultID, resultBytes); err != nil {
			return err
		}
		recordRefs := []contract.VersionedRef{req.Ref(), {ID: resultID, Version: "1", Digest: result.ResultDigest()}}
		_, err = s.engine.appendEventTx(tx, prop, StateReplaying, StateDecisionPending, nil, recordRefs, pairSlice(prop.Sources()))
		return err
	}); err != nil {
		return nil, err
	}

	// --- The protected U1 decision.
	observed, err := s.activeHeads(ctx, prop.Sources())
	if err != nil {
		return nil, err
	}
	observedRefs := make([]contract.SkillArtifactRef, 0, len(observed))
	for _, head := range observed {
		if head != nil {
			observedRefs = append(observedRefs, *head)
		}
	}
	policyRefs := policiesOf(prop.Doc())
	decisionID := in.DecisionID
	if decisionID == "" {
		decisionID = "decision-" + prop.ID()
	}
	decision, err := s.evaluator.Decide(in.Comparator, evaluator.Input{
		DecisionID:           decisionID,
		DecisionVersion:      1,
		Candidate:            cand,
		ResultDoc:            result.Doc(),
		Records:              result.Records(),
		Envelope:             result.Envelope(),
		EnvelopedCases:       result.EnvelopeTotalCases(),
		CandidateCases:       result.CandidateTotalCases(),
		RequestBaselines:     req.BaselineRefs(),
		ValidationRecordRefs: []contract.VersionedRef{req.Ref(), {ID: stringOf(result.Doc()["replay_result_id"]), Version: "1", Digest: result.ResultDigest()}},
		StaticGates: []evaluator.StaticGate{
			{Code: "schema_canonicalization_digest", Passed: true, RecordRefs: []contract.VersionedRef{prop.Ref()}},
			{Code: "exact_ref_resolution", Passed: true, RecordRefs: prop.AssessmentRefs()},
			{Code: "merge_conflict_closure", Passed: true, RecordRefs: []contract.VersionedRef{prop.Ref()}},
			{Code: "merge_provenance_coverage", Passed: true, RecordRefs: []contract.VersionedRef{{ID: cand.CandidateID, Version: "1", Digest: cand.BodyDigest}}},
		},
		ReleaseRule: evaluator.ReleaseRule{
			Ref:             policyRefs.ReleaseRuleRef,
			RequiredDomains: []string{replay.DomainSourceA, replay.DomainSourceB, replay.DomainOverlap},
		},
		ExpectedSourceHeads: pairSlice(prop.Sources()),
		ObservedSourceHeads: observedRefs,
	})
	if err != nil {
		return nil, err
	}

	// --- Commit the decision + its state event atomically.
	var to string
	switch decision.Outcome() {
	case evaluator.OutcomeAccepted:
		to = StateActivationPending
	case evaluator.OutcomeRejected:
		to = StateRejected
	case evaluator.OutcomeInconclusive:
		to = StateInconclusive
	default:
		return nil, newError(ReasonReleaseDecisionInvalid, "decision outcome %q outside the closed enum", decision.Outcome())
	}
	decisionRef := contract.VersionedRef{ID: decisionID, Version: "1", Digest: decision.DecisionDigest()}
	decisionPayload, err := marshalJSON(decision.Doc())
	if err != nil {
		return nil, err
	}
	decisionPayloadDigest, err := payloadDigestOf(decision.Doc())
	if err != nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "decision does not canonicalize: %v", err)
	}
	err = s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		// Decision into the evaluation ledger (deduplicated; the ledger
		// identity of the decision is its canonical payload digest — the
		// same notion GMS-204 freezes for ordinary releases).
		entries, err := s.store.Snapshot(ledger.LedgerEvaluation, decisionID)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.PayloadDigest != decisionPayloadDigest {
				return newError(ReasonRefMismatch,
					"decision id %s already committed with a different digest (%s != %s)", decisionID, entry.PayloadDigest, decisionPayloadDigest)
			}
			break
		}
		if len(entries) == 0 {
			if _, err := tx.AppendEvent(ledger.LedgerEvaluation, decisionID, decisionID, decisionPayload); err != nil {
				return err
			}
		}
		// Winner terminal transitions release the group/pins; activation_pending keeps them.
		if to == StateActivationPending {
			_, err := s.engine.appendEventTx(tx, prop, StateDecisionPending, to, nil,
				[]contract.VersionedRef{decisionRef}, pairSlice(prop.Sources()))
			return err
		}
		staged, err := s.engine.appendEventTx(tx, prop, StateDecisionPending, to, decision.ReasonCodes(),
			[]contract.VersionedRef{decisionRef}, pairSlice(prop.Sources()))
		if err != nil {
			return err
		}
		if err := s.releaseGroupWinnerTx(tx, prop, staged.EventDigest); err != nil {
			return err
		}
		return s.releaseSourcePinsTx(tx, prop, staged.EventDigest)
	})
	if err != nil {
		return nil, err
	}
	state, _, err := s.CurrentState(proposalID)
	if err != nil {
		return nil, err
	}
	return &EvaluateResult{Proposal: prop, Decision: decision, State: state}, nil
}

// checkRequiredHeads verifies the frozen §7.10 required_source_heads equal
// the proposal's canonical pair (both sources, exact refs).
func checkRequiredHeads(req *replay.Request, pair [2]contract.SkillArtifactRef) error {
	heads := req.RequiredSourceHeads()
	if len(heads) != 2 {
		return newError(ReasonRefMismatch,
			"merge replay requests must freeze exactly 2 required_source_heads, got %d", len(heads))
	}
	for i, want := range pair {
		head, _ := contract.AsObject(heads[i])
		got, err := contract.ParseSkillArtifactRef(head)
		if err != nil {
			return newError(ReasonRefMismatch, "required_source_heads[%d]: %v", i, err)
		}
		if got != want {
			return newError(ReasonRefMismatch,
				"required_source_heads[%d] is %s v%s but the proposal freezes %s v%s",
				i, got.LineageID, got.Version, want.LineageID, want.Version)
		}
	}
	return nil
}

// inconclusiveProposal writes a -> inconclusive terminal event and returns
// the carrier error.
func (s *Service) inconclusiveProposal(ctx context.Context, prop *Proposal, from string, reasons []string, recordRefs []contract.VersionedRef, format string, args ...any) error {
	carrier := newError(reasons[0], format, args...)
	if _, err := s.terminalTx(ctx, prop, from, StateInconclusive, reasons, recordRefs, pairSlice(prop.Sources())); err != nil {
		return fmt.Errorf("%v (terminal inconclusive event also failed: %v)", carrier, err)
	}
	return carrier
}

// marshalJSON serializes one decoder-model document.
func marshalJSON(doc map[string]any) ([]byte, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "document marshal: %v", err)
	}
	return data, nil
}

// policiesOf extracts the six policy refs of a committed proposal doc.
func policiesOf(proposalDoc map[string]any) PolicyRefs {
	raw, _ := contract.AsObject(proposalDoc["policies"])
	parse := func(field string) contract.VersionedRef {
		obj, _ := contract.AsObject(raw[field])
		if ref, err := contract.ParseVersionedRef(obj); err == nil {
			return ref
		}
		return contract.VersionedRef{}
	}
	return PolicyRefs{
		SimilarityPolicyRef:  parse("similarity_policy_ref"),
		MergePolicyRef:       parse("merge_policy_ref"),
		ValidationProfileRef: parse("validation_profile_ref"),
		ReplayProfileRef:     parse("replay_profile_ref"),
		ReleaseRuleRef:       parse("release_rule_ref"),
		UtilityComparatorRef: parse("utility_comparator_ref"),
	}
}
