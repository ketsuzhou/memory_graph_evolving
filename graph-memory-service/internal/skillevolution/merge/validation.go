package merge

import (
	"context"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// Validate runs the merge-specific static validation under its lease
// (GMS §12.13.1; machine 9.4.5 candidate_bound -> validating -> replaying):
//
//  1. source-head freshness (candidate_bound -> stale otherwise);
//  2. the bound candidate resolves committed with body bytes whose
//     canonical digest equals the declared body digest;
//  3. required branch preservation: every source branch appears in the
//     merged body under its a-/b- prefix (MERGE_BRANCH_REGRESSION);
//  4. applicability no-expansion: every merged predicate is a predicate of
//     one of the sources (the merge never widens applicability);
//  5. the merged envelope re-canonicalizes byte-identically through the
//     GMS-202 artifact gate.
//
// Any hard failure ends the attempt: validating -> rejected with the closed
// code; success commits validating -> replaying.
func (s *Service) Validate(ctx context.Context, proposalID string) error {
	prop, _, err := s.loadInState(proposalID, StateCandidateBound)
	if err != nil {
		return err
	}
	if err := s.requireFreshSources(ctx, prop.Sources()); err != nil {
		if CodeOf(err) == ReasonMergeSourceNotActive {
			return s.staleProposal(ctx, prop, StateCandidateBound, []string{ReasonMergeSourceHeadStale}, nil, "%v", err)
		}
		return err
	}
	// The validation lease event commits first (9.4.5).
	if err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		_, err := s.engine.appendEventTx(tx, prop, StateCandidateBound, StateValidating, nil, nil, pairSlice(prop.Sources()))
		return err
	}); err != nil {
		return err
	}
	fail := func(code, format string, args ...any) error {
		return s.rejectProposal(ctx, prop, StateValidating, []string{code}, nil, code, format, args...)
	}

	cand, err := s.boundCandidate(prop)
	if err != nil {
		return fail(ReasonCandidateNotBound, "%v", err)
	}
	bodyDigest, bytes, err := s.resolveBody(cand)
	if err != nil {
		return fail(ReasonCandidateReleasedBodyMismatch, "%v", err)
	}
	if bodyDigest != cand.BodyDigest {
		return fail(ReasonCandidateReleasedBodyMismatch,
			"committed body digest %s disagrees with the declared candidate body digest %s", bodyDigest, cand.BodyDigest)
	}
	mergedEnvelope, err := parseEnvelope(bytes)
	if err != nil {
		return fail(ReasonCandidateReleasedBodyMismatch, "%v", err)
	}
	envelopeA, err := s.resolveEnvelope(prop.Sources()[0])
	if err != nil {
		return fail(CodeOf(err), "%v", err)
	}
	envelopeB, err := s.resolveEnvelope(prop.Sources()[1])
	if err != nil {
		return fail(CodeOf(err), "%v", err)
	}
	if err := checkBranchPreservation(envelopeA, envelopeB, mergedEnvelope); err != nil {
		return fail(ReasonMergeBranchRegression, "%v", err)
	}
	if err := checkApplicabilityNoExpansion(envelopeA, envelopeB, mergedEnvelope); err != nil {
		return fail(ReasonMergeBlockingConflict, "%v", err)
	}
	if _, err := s.artifacts.Canonicalize(mergedEnvelope); err != nil {
		return fail(CodeOf(err), "merged envelope re-canonicalization: %v", err)
	}

	// validating -> replaying (merge_validation_passed).
	return s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		recordRefs := []contract.VersionedRef{{ID: cand.CandidateID, Version: "1", Digest: cand.BodyDigest}}
		_, err := s.engine.appendEventTx(tx, prop, StateValidating, StateReplaying, nil, recordRefs, pairSlice(prop.Sources()))
		return err
	})
}

// boundCandidate resolves the candidate the candidate_bound event froze.
func (s *Service) boundCandidate(prop *Proposal) (contract.CandidateArtifactRef, error) {
	entries, err := s.store.Snapshot(ledger.LedgerMergeEvent, prop.ID())
	if err != nil {
		return contract.CandidateArtifactRef{}, err
	}
	for _, entry := range entries {
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return contract.CandidateArtifactRef{}, fmt.Errorf("merge: event %s payload unresolved", entry.EventID)
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return contract.CandidateArtifactRef{}, err
		}
		obj, _ := contract.AsObject(value)
		if stringOf(obj["to_state"]) != StateCandidateBound {
			continue
		}
		refs, _ := contract.AsArray(obj["record_refs"])
		if len(refs) == 0 {
			return contract.CandidateArtifactRef{}, newError(ReasonCandidateNotBound,
				"candidate_bound event of %s freezes no candidate ref", prop.ID())
		}
		ref, _ := contract.AsObject(refs[0])
		candidateID := stringOf(ref["id"])
		bodyDigest := stringOf(ref["digest"])
		committed, err := s.resolveCandidateRecord(candidateID)
		if err != nil {
			return contract.CandidateArtifactRef{}, err
		}
		if committed.BodyDigest != bodyDigest || committed.OriginType != "merge_proposal" ||
			committed.OriginRef != prop.Ref() {
			return contract.CandidateArtifactRef{}, newError(ReasonCandidateNotBound,
				"committed candidate %s disagrees with the candidate_bound freeze", candidateID)
		}
		return committed, nil
	}
	return contract.CandidateArtifactRef{}, newError(ReasonCandidateNotBound,
		"proposal %s has no candidate_bound event", prop.ID())
}

// resolveCandidateRecord finds one committed candidate in the candidate
// ledger by candidate id.
func (s *Service) resolveCandidateRecord(candidateID string) (contract.CandidateArtifactRef, error) {
	entries, err := s.store.Snapshot(ledger.LedgerCandidate, "")
	if err != nil {
		return contract.CandidateArtifactRef{}, err
	}
	for _, entry := range entries {
		if entry.EventID != candidateID {
			continue
		}
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return contract.CandidateArtifactRef{}, newError(ReasonCandidateNotBound,
				"candidate %s payload unresolved", candidateID)
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return contract.CandidateArtifactRef{}, err
		}
		obj, _ := contract.AsObject(value)
		return contract.ParseCandidateArtifactRef(obj)
	}
	return contract.CandidateArtifactRef{}, newError(ReasonCandidateNotBound,
		"candidate %s does not resolve committed", candidateID)
}

// resolveBody loads the canonical body bytes of a candidate and re-derives
// their digest.
func (s *Service) resolveBody(cand contract.CandidateArtifactRef) (string, []byte, error) {
	bytes, ok, err := s.store.Get(cand.BodyDigest)
	if err != nil {
		return "", nil, err
	}
	if !ok {
		return "", nil, newError(ReasonCandidateReleasedBodyMismatch,
			"candidate %s body %s does not resolve in the content store", cand.CandidateID, cand.BodyDigest)
	}
	value, err := contract.ParseJSONStrict(bytes)
	if err != nil {
		return "", nil, newError(ReasonCandidateReleasedBodyMismatch, "candidate body invalid: %v", err)
	}
	canonical, err := contract.JCS(contract.NormalizeForHashing(value))
	if err != nil {
		return "", nil, newError(ReasonCandidateReleasedBodyMismatch, "candidate body cannot re-canonicalize: %v", err)
	}
	return contract.DigestBytes(canonical), bytes, nil
}

func parseEnvelope(bytes []byte) (map[string]any, error) {
	value, err := contract.ParseJSONStrict(bytes)
	if err != nil {
		return nil, err
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		return nil, newError(ReasonCandidateReleasedBodyMismatch, "candidate body is not an object")
	}
	return obj, nil
}

// checkBranchPreservation verifies every source branch survives in the
// merged body under its provenance prefix (required_branch_preservation,
// M-decisions; a dropped branch is a branch regression).
func checkBranchPreservation(a, b, merged map[string]any) error {
	mergedBranches := branchIDSet(policyBranches(asObject(merged["body"])))
	for _, src := range []struct {
		envelope map[string]any
		prefix   string
		kind     string
	}{
		{a, "a", "source_a"}, {b, "b", "source_b"},
	} {
		body, _ := contract.AsObject(src.envelope["body"])
		for _, item := range policyBranches(body) {
			branch := asObject(item)
			id := src.prefix + "-" + stringOf(branch["branch_id"])
			if !mergedBranches[id] {
				return newError(ReasonMergeBranchRegression,
					"%s branch %q is not preserved in the merged body (required_branch_preservation)", src.kind, stringOf(branch["branch_id"]))
			}
		}
	}
	return nil
}

func branchIDSet(branches []any) map[string]bool {
	out := map[string]bool{}
	for _, item := range branches {
		branch := asObject(item)
		out[stringOf(branch["branch_id"])] = true
	}
	return out
}

// checkApplicabilityNoExpansion verifies every merged predicate is a
// predicate of one of the sources: the merge never widens applicability
// (M-decisions; only title/description may be derived freely).
func checkApplicabilityNoExpansion(a, b, merged map[string]any) error {
	allowed := map[string]bool{}
	appA, _ := contract.AsObject(a["applicability"])
	appB, _ := contract.AsObject(b["applicability"])
	for _, predicates := range [][]any{predicatesOf(appA), predicatesOf(appB)} {
		for _, predicate := range predicates {
			allowed[contract.CanonicalKey(predicate)] = true
		}
	}
	mergedApp, _ := contract.AsObject(merged["applicability"])
	for _, predicate := range predicatesOf(mergedApp) {
		if !allowed[contract.CanonicalKey(predicate)] {
			return newError(ReasonMergeBlockingConflict,
				"merged applicability carries a predicate of neither source (applicability expansion is forbidden)")
		}
	}
	return nil
}
