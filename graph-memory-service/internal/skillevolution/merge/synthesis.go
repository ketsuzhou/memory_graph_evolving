package merge

import (
	"context"
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
)

// SynthesisResult reports the committed merge candidate.
type SynthesisResult struct {
	Proposal  *Proposal
	Candidate contract.CandidateArtifactRef
	Canonical []byte // canonical merged envelope bytes
}

// Synthesize runs the protected MT3 builder (GMS §12.12). Fail-closed order:
//
//  1. the proposal is in admitted and both sources are STILL the pinned
//     active heads (a moved head stales: admitted -> stale, no candidate);
//  2. admitted -> synthesizing (the synthesis lease event);
//  3. conflict closure: a blocking unresolved conflict can never produce a
//     candidate (synthesizing -> rejected, MERGE_BLOCKING_CONFLICT);
//  4. the GMS-202 merge gates over the frozen assessment, the closure and
//     the provenance coverage;
//  5. the deterministic builder derives the merged envelope: branch union
//     with disjoint a-/b- provenance prefixes, evidence and claim refs
//     preserved, conflict claims folded into causal_context (conflict
//     segments stay marked), applicability predicates/exclusions unioned
//     with NO expansion, permissions unioned;
//  6. artifact.Canonicalize runs the full three-kind gate;
//  7. ONE transaction puts the canonical bytes, appends the merge Candidate
//     (origin_type=merge_proposal) and writes synthesizing ->
//     candidate_bound.
func (s *Service) Synthesize(ctx context.Context, proposalID string) (*SynthesisResult, error) {
	prop, state, err := s.loadInState(proposalID, StateAdmitted)
	if err != nil {
		return nil, err
	}
	_ = state
	if err := s.requireFreshSources(ctx, prop.Sources()); err != nil {
		if CodeOf(err) == ReasonMergeSourceNotActive {
			return nil, s.staleProposal(ctx, prop, StateAdmitted, []string{ReasonMergeSourceHeadStale}, nil, "%v", err)
		}
		return nil, err
	}
	// The synthesis lease event commits first (9.4.3).
	if err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		_, err := s.engine.appendEventTx(tx, prop, StateAdmitted, StateSynthesizing, nil, nil, pairSlice(prop.Sources()))
		return err
	}); err != nil {
		return nil, err
	}

	// --- Conflict closure: an unresolved blocking conflict ends the attempt.
	if err := s.candidates.CheckMergeClosure(prop.Doc()); err != nil {
		return nil, s.rejectProposal(ctx, prop, StateSynthesizing,
			[]string{ReasonMergeBlockingConflict}, nil, ReasonMergeBlockingConflict,
			"blocking conflict unresolved: %v", err)
	}
	assessment, err := s.resolvePrimaryAssessment(ctx, prop)
	if err != nil {
		return nil, err
	}
	if err := s.candidates.CheckSimilarityAssessment(assessment.Doc()); err != nil {
		return nil, s.rejectProposal(ctx, prop, StateSynthesizing,
			[]string{CodeOf(err)}, []contract.VersionedRef{assessment.Ref()}, CodeOf(err),
			"assessment gate: %v", err)
	}
	if err := s.candidates.CheckMergeProvenance(mergeProvenanceOf(prop)); err != nil {
		return nil, s.rejectProposal(ctx, prop, StateSynthesizing,
			[]string{CodeOf(err)}, nil, CodeOf(err), "provenance gate: %v", err)
	}

	// --- The deterministic builder.
	envelopeA, err := s.resolveEnvelope(prop.Sources()[0])
	if err != nil {
		return nil, err
	}
	envelopeB, err := s.resolveEnvelope(prop.Sources()[1])
	if err != nil {
		return nil, err
	}
	merged, err := synthesizeEnvelopes(prop, envelopeA, envelopeB)
	if err != nil {
		return nil, err
	}
	canonical, err := s.artifacts.Canonicalize(merged)
	if err != nil {
		return nil, s.rejectProposal(ctx, prop, StateSynthesizing,
			[]string{CodeOf(err)}, nil, CodeOf(err), "merged artifact failed the canonical gate: %v", err)
	}
	cand := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   deriveMergeCandidateID(prop.Ref(), canonical.BodyDigest()),
		Kind:          similarity.KindStepGuidance,
		BodyDigest:    canonical.BodyDigest(),
		OriginType:    "merge_proposal",
		OriginRef:     prop.Ref(),
	}

	var out *SynthesisResult
	err = s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		if _, err := tx.PutContent(canonical.Canonical()); err != nil {
			return err
		}
		payload, err := json.Marshal(candidateRefJSONDoc(cand))
		if err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ledger.LedgerCandidate, "", cand.CandidateID, payload); err != nil {
			return err
		}
		recordRefs := []contract.VersionedRef{
			{ID: cand.CandidateID, Version: "1", Digest: cand.BodyDigest},
			assessment.Ref(),
		}
		if _, err := s.engine.appendEventTx(tx, prop, StateSynthesizing, StateCandidateBound, nil, recordRefs, pairSlice(prop.Sources())); err != nil {
			return err
		}
		out = &SynthesisResult{Proposal: prop, Candidate: cand, Canonical: canonical.Canonical()}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// loadInState loads a proposal and requires its derived state to equal want
// (idempotent passthrough when the state already moved past via replay is
// NOT granted: every lifecycle step is explicit).
func (s *Service) loadInState(proposalID, want string) (*Proposal, string, error) {
	prop, err := s.LoadProposal(proposalID, "")
	if err != nil {
		return nil, "", err
	}
	state, _, err := s.CurrentState(proposalID)
	if err != nil {
		return nil, "", err
	}
	if state != want {
		return nil, state, transitionError("this step requires %s, current state of %s is %q", want, proposalID, state)
	}
	return prop, state, nil
}

// resolveEnvelope loads the committed canonical envelope of one released
// source ref from the content store.
func (s *Service) resolveEnvelope(source contract.SkillArtifactRef) (map[string]any, error) {
	payload, ok, err := s.store.Get(source.ArtifactDigest)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newError(ReasonMergeSourceInvalid,
			"source %s v%s canonical envelope %s does not resolve in the content store",
			source.LineageID, source.Version, source.ArtifactDigest)
	}
	value, err := contract.ParseJSONStrict(payload)
	if err != nil {
		return nil, newError(ReasonMergeSourceInvalid, "source %s envelope invalid: %v", source.LineageID, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		return nil, newError(ReasonMergeSourceInvalid, "source %s envelope is not an object", source.LineageID)
	}
	if kind := stringOf(obj["kind"]); kind != similarity.KindStepGuidance {
		return nil, newError(ReasonMergeKindUnsupported,
			"source %s envelope kind is %q; v1 merges step_guidance only", source.LineageID, kind)
	}
	return obj, nil
}

// synthesizeEnvelopes is the deterministic protected builder (GMS §12.12.2):
// the branch union with disjoint a-/b- prefixes (required branch
// preservation), unioned evidence and claims with the conflict claims
// folded into causal_context, the applicability union with NO expansion and
// the permission union. The suggestion only ever affects title/description.
func synthesizeEnvelopes(prop *Proposal, a, b map[string]any) (map[string]any, error) {
	bodyA, _ := contract.AsObject(a["body"])
	bodyB, _ := contract.AsObject(b["body"])
	if bodyA == nil || bodyB == nil {
		return nil, newError(ReasonMergeEvidenceIncomplete, "a source envelope carries no body")
	}
	branches, err := mergeBranches(bodyA, "a", bodyB, "b")
	if err != nil {
		return nil, err
	}
	conflictClaims := conflictClaimRefs(prop)
	claimRefs := append(versionedRefDocs(bodyClaimRefs(bodyA)), versionedRefDocs(bodyClaimRefs(bodyB))...)
	claimRefs = append(claimRefs, versionedRefDocs(conflictClaims)...)
	body := map[string]any{
		"causal_context": map[string]any{
			"summary": fmt.Sprintf("Symmetric merge of %s and %s: branch union with preserved provenance; %d recorded conflict(s) marked by claim refs.",
				prop.Sources()[0].LineageID, prop.Sources()[1].LineageID, len(prop.Conflicts())),
			"claim_refs": dedupeByKey(claimRefs),
		},
		"branches": branches,
	}
	appA, _ := contract.AsObject(a["applicability"])
	appB, _ := contract.AsObject(b["applicability"])
	predicates := append(predicatesOf(appA), predicatesOf(appB)...)
	exclusions := append(exclusionsOf(appA), exclusionsOf(appB)...)
	return map[string]any{
		"schema_version": "gms.skill-artifact.v1",
		"kind":           similarity.KindStepGuidance,
		"title":          fmt.Sprintf("Merged step guidance: %s + %s", prop.Sources()[0].LineageID, prop.Sources()[1].LineageID),
		"description":    "Symmetric new-lineage merge (strategy symmetric_new_lineage); sources retained.",
		"applicability": map[string]any{
			"predicates": dedupeByKey(predicates),
			"exclusions": dedupeByKey(exclusions),
		},
		"permissions": mergePermissions(a, b),
		"body":        body,
	}, nil
}

// mergeBranches unions both decision policies with disjoint branch-id
// prefixes; the evidence refs of every branch are preserved verbatim
// (branch provenance never regenerates).
func mergeBranches(bodyA map[string]any, prefixA string, bodyB map[string]any, prefixB string) ([]any, error) {
	branchesA := policyBranches(bodyA)
	branchesB := policyBranches(bodyB)
	if len(branchesA) == 0 || len(branchesB) == 0 {
		return nil, newError(ReasonMergeEvidenceIncomplete, "a source decision policy carries no branches")
	}
	out := make([]any, 0, len(branchesA)+len(branchesB))
	seen := map[string]bool{}
	for _, item := range append(append([]any{}, prefixedBranches(branchesA, prefixA)...), prefixedBranches(branchesB, prefixB)...) {
		branch, _ := contract.AsObject(item)
		id := stringOf(branch["branch_id"])
		if seen[id] {
			return nil, newError(ReasonMergeBlockingConflict, "branch identity collision after prefixing: %s", id)
		}
		seen[id] = true
		out = append(out, branch)
	}
	return out, nil
}

// prefixedBranches rewrites branch ids under one provenance prefix,
// preserving predicate/guidance/evidence verbatim.
func prefixedBranches(branches []any, prefix string) []any {
	out := make([]any, 0, len(branches))
	for _, item := range branches {
		branch := deepCopyDoc(asObject(item))
		if id := stringOf(branch["branch_id"]); id != "" {
			branch["branch_id"] = prefix + "-" + id
		}
		out = append(out, branch)
	}
	return out
}

func policyBranches(body map[string]any) []any {
	branches, _ := contract.AsArray(body["branches"])
	return branches
}

// conflictClaimRefs collects both claim refs of every recorded conflict
// (the conflict segments stay marked inside causal_context).
func conflictClaimRefs(prop *Proposal) []contract.VersionedRef {
	var out []contract.VersionedRef
	for _, conflict := range prop.Conflicts() {
		for _, field := range []string{"source_a_claim_ref", "source_b_claim_ref"} {
			refObj, _ := contract.AsObject(conflict[field])
			if refObj == nil {
				continue
			}
			if ref, err := contract.ParseVersionedRef(refObj); err == nil {
				out = append(out, ref)
			}
		}
	}
	return out
}

func bodyClaimRefs(body map[string]any) []contract.VersionedRef {
	causal, _ := contract.AsObject(body["causal_context"])
	if causal == nil {
		return nil
	}
	raw, _ := contract.AsArray(causal["claim_refs"])
	var out []contract.VersionedRef
	for _, item := range raw {
		refObj, _ := contract.AsObject(item)
		if ref, err := contract.ParseVersionedRef(refObj); err == nil {
			out = append(out, ref)
		}
	}
	return out
}

func predicatesOf(applicability map[string]any) []any {
	raw, _ := contract.AsArray(applicability["predicates"])
	return raw
}

func exclusionsOf(applicability map[string]any) []any {
	raw, _ := contract.AsArray(applicability["exclusions"])
	return raw
}

// mergePermissions unions both permission lists by canonical key (each still
// under the Host v1 cap — the artifact gate re-verifies).
func mergePermissions(a, b map[string]any) []any {
	var out []any
	for _, envelope := range []map[string]any{a, b} {
		raw, _ := contract.AsArray(envelope["permissions"])
		out = append(out, raw...)
	}
	return dedupeByKey(out)
}

// mergeProvenanceOf assembles the GMS-202 provenance coverage summary of a
// proposal.
func mergeProvenanceOf(prop *Proposal) candidate.MergeProvenance {
	return candidate.MergeProvenance{
		SourceRefs:     pairSlice(prop.Sources()),
		AssessmentRefs: prop.AssessmentRefs(),
		Conflicts:      prop.Conflicts(),
	}
}

// candidateRefJSONDoc renders the §7.4 candidate ref payload.
func candidateRefJSONDoc(ref contract.CandidateArtifactRef) map[string]any {
	return map[string]any{
		"schema_version": ref.SchemaVersion,
		"candidate_id":   ref.CandidateID,
		"kind":           ref.Kind,
		"body_digest":    ref.BodyDigest,
		"origin_type":    ref.OriginType,
		"origin_ref":     versionedRefDoc(ref.OriginRef),
	}
}

// deriveMergeCandidateID mints the deterministic merge candidate id from the
// proposal ref and body digest.
func deriveMergeCandidateID(origin contract.VersionedRef, bodyDigest string) string {
	digest, err := contract.DigestOf(map[string]any{
		"origin_type": "merge_proposal",
		"origin_ref": map[string]any{
			"id":      origin.ID,
			"version": json.Number(origin.Version),
			"digest":  origin.Digest,
		},
		"body_digest": bodyDigest,
	})
	if err != nil {
		// decoder-model values with exact digests always canonicalize
		panic(fmt.Sprintf("merge: derive candidate id: %v", err))
	}
	return "cand-" + digest[len("sha256:"):len("sha256:")+16]
}

// --- generic decoder-model helpers ------------------------------------------------

func asObject(v any) map[string]any {
	obj, _ := contract.AsObject(v)
	return obj
}

// dedupeByKey removes duplicate objects/arrays by canonical key, keeping
// first occurrence (stable).
func dedupeByKey(values []any) []any {
	out := make([]any, 0, len(values))
	seen := map[string]bool{}
	for _, v := range values {
		key := contract.CanonicalKey(v)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, v)
	}
	return out
}
