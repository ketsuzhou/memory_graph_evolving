package merge

import (
	"context"
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
)

// ProposeInput is the closed MT2 proposal-creation input. The sources come
// from the referenced similarity assessment (never re-declared), so a
// proposal can never widen or reorder its pair.
type ProposeInput struct {
	ProposalID string
	// Assessment addresses the committed §7.21 record exactly.
	Assessment contract.VersionedRef
	// EvidenceSets holds exactly 2 sets in canonical pair order.
	EvidenceSets []SourceEvidenceSet
	Joint        JointEvidence
	// Conflicts are schema-shaped §7.22 conflict documents.
	Conflicts []map[string]any
	Policies  PolicyRefs
	Origin    Origin
	// PriorProposalRefs point at earlier attempts over the same pair.
	PriorProposalRefs []contract.VersionedRef
}

// Propose runs the protected §7.22 mint (GMS §12.10). Fail-closed order:
//
//  1. the assessment resolves committed with its exact digest and covers a
//     canonical step_guidance pair (SIMILARITY_ASSESSMENT_INVALID /
//     MERGE_KIND_UNSUPPORTED);
//  2. the derived band reaches the merge threshold under the versioned
//     policy (SIMILARITY_BELOW_THRESHOLD — a below-threshold pair may only
//     raise a suggestion, never a proposal; an automatic initiator
//     additionally needs auto_merge_eligible, M1);
//  3. both sources are STILL the current active heads of their lineages
//     (MERGE_SOURCE_NOT_ACTIVE — active-only sources, M2);
//  4. the two evidence sets match the pair order and every evidence ref
//     resolves committed (EVIDENCE_NOT_COMMITTED);
//  5. the dedup keys re-derive, the document digests under the §7.22
//     x-digest preimage and validates against the authority schema;
//  6. ONE transaction appends the immutable proposal and its genesis
//     none -> proposed event, idempotent per (proposal id, digest).
func (s *Service) Propose(ctx context.Context, in ProposeInput) (*Proposal, error) {
	if in.ProposalID == "" {
		return nil, newError(ReasonRefMismatch, "proposal id required")
	}
	assessment, err := s.similarity.Resolve(ctx, in.Assessment.ID, in.Assessment.Digest)
	if err != nil {
		return nil, newError(ReasonSimilarityAssessmentInvalid, "assessment %s: %v", in.Assessment.ID, err)
	}
	policy := s.similarity.Policy()
	band := assessment.Band()
	if !policy.MayPropose(band) {
		return nil, newError(ReasonSimilarityBelowThreshold,
			"band %q (score %d) is below the merge threshold of policy %s v%s; the pair may only raise a suggestion",
			band, assessment.ScoreMicros(), policy.Ref.ID, policy.Ref.Version)
	}
	switch in.Origin.InitiatorType {
	case "automatic":
		if !policy.MayAutoAdmit(band) {
			return nil, newError(ReasonSimilarityBelowThreshold,
				"automatic initiation needs auto_merge_eligible, band is %q (graded initiation, M1)", band)
		}
	case "model", "human":
		// merge_review and above may propose through the protected gate.
	default:
		return nil, newError(ReasonRefMismatch, "initiator_type %q outside {automatic, model, human}", in.Origin.InitiatorType)
	}
	pair := assessment.Sources()
	for _, source := range pair {
		if source.Kind != similarity.KindStepGuidance {
			return nil, newError(ReasonMergeKindUnsupported,
				"source %s is %q; v1 merges step_guidance only (cross-kind and n-way merges are out of scope)", source.LineageID, source.Kind)
		}
	}
	if err := s.requireFreshSources(ctx, pair); err != nil {
		return nil, err
	}
	if len(in.EvidenceSets) != 2 {
		return nil, newError(ReasonMergeEvidenceIncomplete,
			"exactly 2 source evidence sets are required (one per canonical source), got %d", len(in.EvidenceSets))
	}
	for i, set := range in.EvidenceSets {
		if set.Source != pair[i] {
			return nil, newError(ReasonMergeEvidenceIncomplete,
				"evidence set %d covers %s v%s but the canonical source %d is %s v%s",
				i, set.Source.LineageID, set.Source.Version, i, pair[i].LineageID, pair[i].Version)
		}
		if err := s.checkEvidenceSet(set); err != nil {
			return nil, err
		}
	}
	intent := map[string]any{
		"target_kind":                  IntentTargetKind,
		"strategy":                     IntentStrategy,
		"normalized_applicability_key": s.applicabilityKey(assessment, pair),
		"source_disposition":           IntentSourceDisposition,
		"required_branch_preservation": true,
	}
	groupKey, intentKey, err := deriveDedupKeys(pair, intent)
	if err != nil {
		return nil, err
	}
	pairKey, err := similarity.SourcePairKey(pair)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "%v", err)
	}
	conflicts := make([]any, 0, len(in.Conflicts))
	for _, conflict := range in.Conflicts {
		conflicts = append(conflicts, deepCopyDoc(conflict))
	}
	doc := map[string]any{
		"schema_version":             SchemaMergeProposalDoc,
		"proposal_id":                in.ProposalID,
		"proposal_version":           json.Number("1"),
		"source_skill_refs":          []any{similarity.SkillRefDoc(pair[0]), similarity.SkillRefDoc(pair[1])},
		"similarity_assessment_refs": versionedRefDocs([]contract.VersionedRef{assessment.Ref()}),
		"source_evidence_sets":       evidenceSetDocs(in.EvidenceSets),
		"joint_evidence":             jointEvidenceDoc(in.Joint),
		"merge_intent":               intent,
		"conflicts":                  conflicts,
		"policies":                   in.Policies.doc(),
		"dedup": map[string]any{
			"source_pair_key":    pairKey,
			"proposal_group_key": groupKey,
			"intent_key":         intentKey,
		},
		"origin":              in.Origin.doc(),
		"prior_proposal_refs": versionedRefDocs(in.PriorProposalRefs),
	}
	digest, err := s.gates.ComputeDigestPreimage(doc, SchemaMergeProposal)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "proposal digest preimage: %v", err)
	}
	doc["proposal_digest"] = digest
	if err := s.gates.ValidateInstance(doc, SchemaMergeProposal); err != nil {
		return nil, newError(ReasonDigestMismatch, "minted proposal failed its authority schema: %v", err)
	}
	// proposal_digest is the x-digest field; the ledger identity is the
	// whole-document canonical digest (the same two notions the §7.21
	// assessment carries: assessment_digest vs the record digest).
	prop, err := ParseProposal(s.gates, doc)
	if err != nil {
		return nil, err
	}
	if err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		replayed, recorded, err := tx.RecordIdempotency("merge-propose:"+prop.ID(), prop.Digest(), prop.CanonicalBytes())
		if err != nil {
			return err
		}
		if replayed {
			if string(recorded) != string(prop.CanonicalBytes()) {
				return newError(ReasonIdempotencyConflict, "proposal %s replays with different canonical bytes", prop.ID())
			}
			return nil
		}
		if _, err := tx.AppendEvent(ledger.LedgerMergeProposal, prop.ID(), prop.ID(), prop.CanonicalBytes()); err != nil {
			return err
		}
		_, err = s.engine.appendEventTx(tx, prop, StateNone, StateProposed, nil,
			[]contract.VersionedRef{assessment.Ref()}, pair[:])
		return err
	}); err != nil {
		return nil, err
	}
	return s.LoadProposal(prop.ID(), prop.Digest())
}

// Admit runs the MT2 admission gate (GMS §12.11): the graded band re-check
// under the versioned policy, the M2 compatibility gates, the active-only
// source freshness and the single in-flight group winner. On success the
// proposal is admitted and both source heads are pinned by merge-source
// head CAS records. Fail-closed outcomes:
//
//   - band below threshold / applicability none / permission incompatible
//     → proposed → rejected (incompatible_or_below_policy);
//   - a source head moved → proposed → stale (head_or_policy_invalid);
//   - another proposal already won the group → proposed → duplicate
//     (exact_duplicate) pointing at the canonical winner (M6).
func (s *Service) Admit(ctx context.Context, proposalID string) (*Proposal, string, error) {
	prop, err := s.LoadProposal(proposalID, "")
	if err != nil {
		return nil, "", err
	}
	state, _, err := s.CurrentState(proposalID)
	if err != nil {
		return nil, "", err
	}
	switch state {
	case StateAdmitted:
		return prop, StateAdmitted, nil // idempotent winner replay
	case StateProposed:
		// proceed below
	case StateNone:
		return nil, "", transitionError("proposal %s has no event stream; propose it first", proposalID)
	default:
		return nil, "", transitionError("admission requires %s, current state is %q", StateProposed, state)
	}

	// --- Graded band re-check under the versioned policy (M1/M7).
	assessment, err := s.resolvePrimaryAssessment(ctx, prop)
	if err != nil {
		return nil, "", err
	}
	policy := s.similarity.Policy()
	if !policy.MayPropose(assessment.Band()) {
		return nil, "", s.rejectProposal(ctx, prop, StateProposed,
			[]string{ReasonSimilarityBelowThreshold}, nil, CodeOf(err),
			"band %q is below the merge threshold under policy %s v%s", assessment.Band(), policy.Ref.ID, policy.Ref.Version)
	}
	if stringOf(originOf(prop.doc)["initiator_type"]) == "automatic" && !policy.MayAutoAdmit(assessment.Band()) {
		return nil, "", s.rejectProposal(ctx, prop, StateProposed,
			[]string{ReasonSimilarityBelowThreshold}, nil, CodeOf(err),
			"automatic initiation needs auto_merge_eligible, band is %q", assessment.Band())
	}

	// --- M2 compatibility gates over the frozen assessment observations.
	compat := compatibilityOf(assessment.Doc())
	if stringOf(compat["applicability_overlap"]) == "none" {
		return nil, "", s.rejectProposal(ctx, prop, StateProposed,
			[]string{ReasonMergeBlockingConflict}, []contract.VersionedRef{assessment.Ref()}, ReasonMergeBlockingConflict,
			"applicability overlap is none: the two skills never apply together and nothing can merge")
	}
	if !boolOf(compat["permission_compatible"]) {
		return nil, "", s.rejectProposal(ctx, prop, StateProposed,
			[]string{ReasonMergeBlockingConflict}, []contract.VersionedRef{assessment.Ref()}, ReasonMergeBlockingConflict,
			"the sources declare incompatible permissions")
	}

	// --- Active-only sources (a moved head stales the attempt, M2/M5).
	if err := s.requireFreshSources(ctx, prop.Sources()); err != nil {
		if CodeOf(err) == ReasonMergeSourceNotActive {
			return nil, "", s.staleProposal(ctx, prop, StateProposed,
				[]string{ReasonMergeSourceNotActive}, nil, "%v", err)
		}
		return nil, "", err
	}

	// --- Group-winner read phase (single in-flight winner, M6).
	winnerSeq, winnerDigest, winnerState, winnerRef, werr := s.groupWinner(prop.GroupKey())
	if werr != nil {
		return nil, "", werr
	}
	if winnerState == groupInFlight && winnerDigest != prop.Digest() {
		// Exact duplicate loser: point at the canonical winner.
		err := newError(ReasonMergeGroupInFlight,
			"proposal group %s already has an in-flight winner (%s); this attempt is an exact duplicate",
			prop.GroupKey(), winnerRef)
		return nil, "", s.duplicateProposal(ctx, prop, winnerSeq, winnerDigest, err)
	}

	// --- ONE atomic admission transaction: winner CAS, both source-head
	// expectation pins, proposed -> admitted event.
	admitErr := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		// 1. Group winner CAS (occupied-terminal heads advance).
		if winnerState == groupAbsent {
			if err := tx.CompareAndSwapHead(ledger.HeadMergeGroupWinner, prop.GroupKey(), 0, "", 1, prop.Digest()); err != nil {
				return err
			}
		} else if winnerState == groupTerminal {
			if err := tx.CompareAndSwapHead(ledger.HeadMergeGroupWinner, prop.GroupKey(), winnerSeq, winnerDigest, winnerSeq+1, prop.Digest()); err != nil {
				return err
			}
		}
		// 2. Source-head expectation pins (merge-source head CAS).
		refs := []contract.VersionedRef{assessment.Ref()}
		for _, source := range prop.Sources() {
			expectation := sourceExpectationRecord(prop, source)
			expDigest, err := tx.PutContent(marshalDoc(expectation))
			if err != nil {
				return err
			}
			key := mergeSourceHeadKey(source.LineageID, prop.GroupKey())
			seq, digest, state, perr := s.pinState(key)
			if perr != nil {
				return perr
			}
			switch {
			case state == pinAbsent:
				if err := tx.CompareAndSwapHead(ledger.HeadMergeSource, key, 0, "", 1, expDigest); err != nil {
					return err
				}
			case state == pinTerminal:
				// the previous attempt over this group died: advance the pin
				if err := tx.CompareAndSwapHead(ledger.HeadMergeSource, key, seq, digest, seq+1, expDigest); err != nil {
					return err
				}
			case state == pinExpectation && digest == expDigest:
				// idempotent re-pin of the identical expectation
			default:
				return newError(ReasonMergeSourceHeadStale,
					"merge-source head of %s is pinned by another in-flight proposal; single in-flight winner per group (M6)", source.LineageID)
			}
			refs = append(refs, contract.VersionedRef{ID: "merge-source-expectation", Version: "1", Digest: expDigest})
		}
		// 3. proposed -> admitted.
		_, err := s.engine.appendEventTx(tx, prop, StateProposed, StateAdmitted, nil, refs, pairSlice(prop.Sources()))
		return err
	})
	if admitErr != nil {
		code := CodeOf(admitErr)
		switch code {
		case ReasonMergeGroupInFlight:
			return nil, "", s.duplicateProposal(ctx, prop, winnerSeq, winnerDigest, admitErr)
		case ReasonMergeSourceHeadStale, ReasonActiveHeadConflict:
			return nil, "", s.staleProposal(ctx, prop, StateProposed, []string{code}, nil, "%v", admitErr)
		}
		return nil, "", admitErr
	}
	newState, _, err := s.CurrentState(proposalID)
	if err != nil {
		return nil, "", err
	}
	return prop, newState, nil
}

// --- group-winner head bookkeeping ---------------------------------------------

// group states of the merge-group-winner head.
const (
	groupAbsent   = 0 // no head: free
	groupInFlight = 1 // head points at a gms.merge-proposal.v1 payload
	groupTerminal = 2 // head points at a terminal/released event: free again
)

// states of one merge-source head (same lifecycle as the group winner).
const (
	pinAbsent      = 0 // no head: free
	pinExpectation = 1 // head points at a gms.merge-source-expectation.v1 payload
	pinTerminal    = 2 // head points at a terminal/released event: free again
)

// pinState classifies the current merge-source head of one (lineage, group):
// absent, a frozen expectation (in-flight) or a terminal event digest (the
// previous attempt over this group died and the pin is free again).
func (s *Service) pinState(key string) (seq uint64, digest string, state int, err error) {
	seq, digest, ok, err := s.store.GetHead(ledger.HeadMergeSource, key)
	if err != nil || !ok || digest == "" {
		return 0, "", pinAbsent, err
	}
	payload, found, err := s.store.Get(digest)
	if err != nil || !found {
		return seq, digest, pinExpectation, err
	}
	value, perr := contract.ParseJSONStrict(payload)
	if perr != nil {
		return seq, digest, pinExpectation, perr
	}
	obj, _ := contract.AsObject(value)
	switch stringOf(obj["schema_version"]) {
	case SchemaMergeSourceExpectation:
		return seq, digest, pinExpectation, nil
	case contract.SchemaMergeProposalEvent:
		to := stringOf(obj["to_state"])
		if to == StateReleased || to == StateDuplicate || to == StateRejected ||
			to == StateInconclusive || to == StateStale || to == StateWithdrawn {
			return seq, digest, pinTerminal, nil
		}
		return seq, digest, pinExpectation, nil
	default:
		return seq, digest, pinExpectation, nil
	}
}

// groupWinner classifies the current winner head of one proposal group.
func (s *Service) groupWinner(groupKey string) (seq uint64, digest string, state int, ref string, err error) {
	seq, digest, ok, err := s.store.GetHead(ledger.HeadMergeGroupWinner, groupKey)
	if err != nil || !ok || digest == "" {
		return 0, "", groupAbsent, "", err
	}
	payload, found, err := s.store.Get(digest)
	if err != nil || !found {
		return seq, digest, groupInFlight, "", err
	}
	value, perr := contract.ParseJSONStrict(payload)
	if perr != nil {
		return seq, digest, groupInFlight, "", perr
	}
	obj, _ := contract.AsObject(value)
	switch stringOf(obj["schema_version"]) {
	case SchemaMergeProposalDoc:
		return seq, digest, groupInFlight, stringOf(obj["proposal_id"]), nil
	case contract.SchemaMergeProposalEvent:
		to := stringOf(obj["to_state"])
		if to == StateReleased || to == StateDuplicate || to == StateRejected ||
			to == StateInconclusive || to == StateStale || to == StateWithdrawn {
			return seq, digest, groupTerminal, "", nil
		}
		return seq, digest, groupInFlight, "", nil
	default:
		return seq, digest, groupInFlight, "", newError(ReasonMergeGroupInFlight,
			"merge-group-winner head points at unknown payload schema %q", stringOf(obj["schema_version"]))
	}
}

// releaseGroupWinnerTx advances the winner head off this in-flight
// proposal onto the terminal event digest, so a new attempt over the same
// group may win again. Only the recorded winner itself releases.
func (s *Service) releaseGroupWinnerTx(tx *ledger.Tx, prop *Proposal, eventDigest string) error {
	seq, digest, state, _, err := s.groupWinner(prop.GroupKey())
	if err != nil {
		return err
	}
	if state != groupInFlight || digest != prop.Digest() {
		return nil // not the recorded winner (an exact-duplicate loser): never touch the head
	}
	return tx.CompareAndSwapHead(ledger.HeadMergeGroupWinner, prop.GroupKey(), seq, digest, seq+1, eventDigest)
}

// releaseSourcePinsTx advances both merge-source heads off their
// expectations onto a terminal event digest (the pins die with the attempt).
func (s *Service) releaseSourcePinsTx(tx *ledger.Tx, prop *Proposal, eventDigest string) error {
	for _, source := range prop.Sources() {
		key := mergeSourceHeadKey(source.LineageID, prop.GroupKey())
		seq, digest, ok, err := s.store.GetHead(ledger.HeadMergeSource, key)
		if err != nil || !ok || digest == "" {
			continue
		}
		payload, found, err := s.store.Get(digest)
		if err != nil {
			return err
		}
		if found {
			value, perr := contract.ParseJSONStrict(payload)
			if perr != nil {
				return perr
			}
			obj, _ := contract.AsObject(value)
			if stringOf(obj["schema_version"]) == SchemaMergeSourceExpectation &&
				stringOf(nested(obj, "proposal_ref", "id")) != prop.ID() {
				continue // pinned by another proposal: never touch it
			}
		}
		if err := tx.CompareAndSwapHead(ledger.HeadMergeSource, key, seq, digest, seq+1, eventDigest); err != nil {
			return err
		}
	}
	return nil
}

// --- terminal helpers -----------------------------------------------------------

// terminalTx writes one terminal transition of the winner and releases the
// group-winner head and both source pins onto the terminal event digest.
func (s *Service) terminalTx(ctx context.Context, prop *Proposal, from, to string, extraReasons []string, recordRefs []contract.VersionedRef, observed []contract.SkillArtifactRef) (EventInfo, error) {
	var info EventInfo
	err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		staged, err := s.engine.appendEventTx(tx, prop, from, to, extraReasons, recordRefs, observed)
		if err != nil {
			return err
		}
		info = staged
		if err := s.releaseGroupWinnerTx(tx, prop, staged.EventDigest); err != nil {
			return err
		}
		return s.releaseSourcePinsTx(tx, prop, staged.EventDigest)
	})
	return info, err
}

// rejectProposal writes proposed|admitted|validating|decision_pending ->
// rejected and returns the carrier error.
func (s *Service) rejectProposal(ctx context.Context, prop *Proposal, from string, reasons []string, recordRefs []contract.VersionedRef, _ string, format string, args ...any) error {
	carrier := newError(reasons[0], format, args...)
	if _, err := s.terminalTx(ctx, prop, from, StateRejected, reasons, recordRefs, pairSlice(prop.Sources())); err != nil {
		return fmt.Errorf("%v (terminal rejected event also failed: %v)", carrier, err)
	}
	return carrier
}

// staleProposal writes a -> stale and returns the carrier error.
func (s *Service) staleProposal(ctx context.Context, prop *Proposal, from string, reasons []string, recordRefs []contract.VersionedRef, format string, args ...any) error {
	carrier := newError(reasons[0], format, args...)
	if _, err := s.terminalTx(ctx, prop, from, StateStale, reasons, recordRefs, pairSlice(prop.Sources())); err != nil {
		return fmt.Errorf("%v (terminal stale event also failed: %v)", carrier, err)
	}
	return carrier
}

// duplicateProposal writes proposed -> duplicate pointing at the canonical
// winner and returns the carrier error (the loser never releases heads).
func (s *Service) duplicateProposal(ctx context.Context, prop *Proposal, winnerSeq uint64, winnerDigest string, cause error) error {
	recordRefs := []contract.VersionedRef{{ID: "merge-group-winner", Version: "1", Digest: winnerDigest}}
	err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		_, err := s.engine.appendEventTx(tx, prop, StateProposed, StateDuplicate,
			[]string{ReasonMergeProposalDuplicate}, recordRefs, pairSlice(prop.Sources()))
		return err
	})
	if err != nil {
		return fmt.Errorf("%v (terminal duplicate event also failed: %v)", cause, err)
	}
	return cause
}

// --- assessment helpers ---------------------------------------------------------

// resolvePrimaryAssessment resolves the first frozen assessment ref of the
// proposal and verifies it covers exactly the proposal's canonical pair.
func (s *Service) resolvePrimaryAssessment(ctx context.Context, prop *Proposal) (*similarity.Assessment, error) {
	refs := prop.AssessmentRefs()
	if len(refs) == 0 {
		return nil, newError(ReasonMergeEvidenceIncomplete, "proposal freezes no similarity assessment ref")
	}
	assessment, err := s.similarity.Resolve(ctx, refs[0].ID, refs[0].Digest)
	if err != nil {
		return nil, newError(ReasonSimilarityAssessmentInvalid, "assessment %s: %v", refs[0].ID, err)
	}
	pair := assessment.Sources()
	if pair != prop.Sources() {
		return nil, newError(ReasonSimilarityAssessmentInvalid,
			"assessment %s covers a different pair than the proposal", refs[0].ID)
	}
	return assessment, nil
}

// checkEvidenceSet resolves every evidence ref of one source evidence set
// against the committed-evidence port.
func (s *Service) checkEvidenceSet(set SourceEvidenceSet) error {
	refs := append(append([]contract.EvidenceRef{}, set.SupportingEvidenceRefs...), set.RefutingEvidenceRefs...)
	for _, ref := range refs {
		record, found, err := s.evidence.GetEvidence(ref.EvidenceID)
		if err != nil {
			return err
		}
		if !found || record.Version != ref.Version || record.EvidenceDigest != ref.EvidenceDigest ||
			(record.CommitState != "committed" && record.CommitState != "sealed") {
			return newError(ReasonEvidenceNotCommitted,
				"evidence %s v%s does not resolve committed with digest %s (staged evidence cannot support a merge proposal)",
				ref.EvidenceID, ref.Version, ref.EvidenceDigest)
		}
	}
	return nil
}

// applicabilityKey derives merge_intent.normalized_applicability_key: the
// digest of the pair plus the assessment's applicability observation —
// the same pair under the same overlap always addresses the same group.
func (s *Service) applicabilityKey(assessment *similarity.Assessment, pair [2]contract.SkillArtifactRef) string {
	overlap := stringOf(compatibilityOf(assessment.Doc())["applicability_overlap"])
	digest, err := contract.DigestOf([]any{
		IntentNormalizedApplicabilitySalt,
		pair[0].LineageID,
		pair[1].LineageID,
		overlap,
	})
	if err != nil {
		// integer-free constant inputs always canonicalize
		panic(fmt.Sprintf("merge: applicability key: %v", err))
	}
	return digest
}

// --- decoder-model helpers --------------------------------------------------------

func evidenceSetDoc(set SourceEvidenceSet) map[string]any {
	return map[string]any{
		"source_skill_ref":         similarity.SkillRefDoc(set.Source),
		"supporting_evidence_refs": evidenceRefDocs(set.SupportingEvidenceRefs),
		"refuting_evidence_refs":   evidenceRefDocs(set.RefutingEvidenceRefs),
		"success_path_refs":        versionedRefDocs(set.SuccessPathRefs),
		"failure_path_refs":        versionedRefDocs(set.FailurePathRefs),
		"recovery_path_refs":       versionedRefDocs(set.RecoveryPathRefs),
		"branch_claim_refs":        versionedRefDocs(set.BranchClaimRefs),
	}
}

func evidenceSetDocs(sets []SourceEvidenceSet) []any {
	out := make([]any, 0, len(sets))
	for _, set := range sets {
		out = append(out, evidenceSetDoc(set))
	}
	return out
}

func jointEvidenceDoc(joint JointEvidence) map[string]any {
	return map[string]any{
		"overlap_evidence_refs":    evidenceRefDocs(joint.OverlapEvidenceRefs),
		"divergence_evidence_refs": evidenceRefDocs(joint.DivergenceEvidenceRefs),
		"shared_anchor_refs":       versionedRefDocs(joint.SharedAnchorRefs),
		"paired_fixture_refs":      versionedRefDocs(joint.PairedFixtureRefs),
	}
}

func compatibilityOf(assessmentDoc map[string]any) map[string]any {
	compat, _ := contract.AsObject(assessmentDoc["compatibility"])
	return compat
}

func originOf(proposalDoc map[string]any) map[string]any {
	origin, _ := contract.AsObject(proposalDoc["origin"])
	return origin
}

func nested(obj map[string]any, field, key string) any {
	inner, _ := contract.AsObject(obj[field])
	if inner == nil {
		return nil
	}
	return inner[key]
}
