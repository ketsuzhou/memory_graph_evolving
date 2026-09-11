package merge

import (
	"context"
	"encoding/json"
	"strconv"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluator"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
)

// ActivateInput is the closed MT5 M@1 activation input.
type ActivateInput struct {
	// Decision is the committed §7.12 release decision (the evaluator
	// Decision or an equivalent activation.ReleaseDecision view).
	Decision evaluatorLikeDecision
	// LineageID names the NEW derived lineage of M (v1: version 1).
	LineageID string
}

// evaluatorLikeDecision is the minimal decision port MT5 needs (the GMS-203
// evaluator.Decision satisfies it directly).
type evaluatorLikeDecision interface {
	Outcome() string
	DecisionDigest() string
	Doc() map[string]any
}

// MergeReleaseResult reports the committed M@1 activation.
type MergeReleaseResult struct {
	ReleasedRef        contract.SkillArtifactRef
	ActivationSequence uint64
	EventID            string
	EventDigest        string
	OutboxKey          string
	DerivedFromRefs    []contract.SkillArtifactRef
	Replayed           bool
}

// outboxProjectionTarget mirrors the GMS-204 projection target vocabulary.
const outboxProjectionTarget = "runtime"

// Activate runs the M@1 derived-lineage activation transaction (GMS
// §12.13.3; machine 9.4.8 -> 9.4.9). The ordinary GMS-204 Activate refuses
// merge-origin candidates by design, so this transaction reuses the same
// exported ledger primitives with the merge-specific write set:
//
//	reads -> deterministic artifacts -> idempotency (replays the recorded
//	outcome, zero new writes) -> frozen guards -> [ decision ledger append,
//	mapping record + lineage-version CAS (M@1), skill-active CAS (M@1),
//	activation-sequence CAS, §7.13 ActivationEvent append with TWO
//	derived_from refs and NO supersedes, outbox enqueue, DUAL merge-source
//	head CAS onto the activation, merge event activation_pending ->
//	released, group-winner head release ] -> commit
//
// Sources A/B are never superseded or deactivated (source_disposition
// retain): they stay active heads of their own lineages. Any failure rolls
// the whole transaction back; a source-head CAS failure then stales the
// proposal (activation_pending -> stale, trigger source_head_cas_failed)
// with ZERO partial activation.
func (s *Service) Activate(ctx context.Context, proposalID string, in ActivateInput) (*MergeReleaseResult, error) {
	prop, _, err := s.loadInState(proposalID, StateActivationPending)
	if err != nil {
		return nil, err
	}
	if in.LineageID == "" {
		return nil, newError(ReasonRefMismatch, "the derived lineage id of M is required")
	}
	for _, source := range prop.Sources() {
		if source.LineageID == in.LineageID {
			return nil, newError(ReasonRefMismatch,
				"derived lineage %q collides with a source lineage; symmetric_new_lineage needs a new lineage", in.LineageID)
		}
	}

	// --- The decision re-validates (§7.12 shape + x-digest + accepted +
	// covers the bound candidate).
	doc := in.Decision.Doc()
	if err := s.gates.ValidateInstance(doc, SchemaReleaseDecision); err != nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "decision re-validation: %v", err)
	}
	if in.Decision.Outcome() != evaluator.OutcomeAccepted {
		return nil, newError(ReasonReleaseNotAccepted,
			"only an accepted decision authorizes M@1, outcome is %q", in.Decision.Outcome())
	}
	decisionID := stringOf(doc["decision_id"])
	decisionRef := contract.VersionedRef{ID: decisionID, Version: "1", Digest: in.Decision.DecisionDigest()}
	cand, err := s.boundCandidate(prop)
	if err != nil {
		return nil, err
	}
	decisionCandRaw, _ := contract.AsObject(doc["candidate_ref"])
	decisionCand, err := contract.ParseCandidateArtifactRef(decisionCandRaw)
	if err != nil || decisionCand != cand {
		return nil, newError(ReasonRefMismatch,
			"decision %s does not cover the bound candidate %s", decisionID, cand.CandidateID)
	}
	bodyDigest, _, err := s.resolveBody(cand)
	if err != nil || bodyDigest != cand.BodyDigest {
		return nil, newError(ReasonCandidateReleasedBodyMismatch,
			"candidate %s body does not re-resolve byte-identically", cand.CandidateID)
	}

	// --- Pre-transaction freshness: both pinned source heads must still be
	// the active heads (M5). A moved head stales without partial effects.
	if err := s.requireFreshSources(ctx, prop.Sources()); err != nil {
		if CodeOf(err) == ReasonMergeSourceNotActive {
			return nil, s.staleProposal(ctx, prop, StateActivationPending,
				[]string{ReasonMergeSourceHeadStale, ReasonMergeSourceNotActive}, nil,
				"source head moved before M@1: %v", err)
		}
		return nil, err
	}

	var result *MergeReleaseResult
	err = s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		// --- Read model.
		version := uint64(1) // symmetric_new_lineage: M always starts at v1
		_, prevMappingDigest, lvOK, err := s.store.GetHead(ledger.HeadLineageVersion, in.LineageID)
		if err != nil {
			return err
		}
		if lvOK && prevMappingDigest != "" {
			return newError(ReasonActiveHeadConflict,
				"derived lineage %s already carries a version assignment; symmetric_new_lineage needs an empty lineage", in.LineageID)
		}
		activeSeq, activeDigest, saOK, err := s.store.GetHead(ledger.HeadSkillActive, in.LineageID)
		if err != nil {
			return err
		}
		if saOK && (activeSeq != 0 || activeDigest != "") {
			return newError(ReasonActiveHeadConflict,
				"derived lineage %s already has an active head; symmetric_new_lineage needs an empty lineage", in.LineageID)
		}
		sequence, err := tx.NextSequence(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}
		ledgerHead, hasHead, err := s.store.Head(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}
		if !hasHead {
			ledgerHead = ledger.Entry{}
		}

		// --- Deterministic artifacts of this release.
		releasedRef := contract.SkillArtifactRef{
			SchemaVersion:  contract.SchemaSkillArtifactRef,
			LineageID:      in.LineageID,
			Version:        strconv.FormatUint(version, 10),
			Kind:           cand.Kind,
			ArtifactDigest: cand.BodyDigest, // candidate and released bodies digest equal
		}
		mappingDoc := map[string]any{
			"schema_version":       SchemaReleaseMapping,
			"lineage_id":           in.LineageID,
			"version":              json.Number(strconv.FormatUint(version, 10)),
			"kind":                 releasedRef.Kind,
			"released_ref":         similarity.SkillRefDoc(releasedRef),
			"candidate_ref":        candidateRefJSONDoc(cand),
			"release_decision_ref": versionedRefDoc(decisionRef),
			"body_digest_equal":    true,
		}
		mappingBytes, err := json.Marshal(mappingDoc)
		if err != nil {
			return err
		}
		eventID := "act-" + decisionID + "-" + in.LineageID + "-v1"
		outboxKey, err := contract.DigestOf(map[string]any{
			"outbox_for":          "activate",
			"identity":            decisionID + "/" + cand.CandidateID + "/" + in.LineageID + "/1",
			"activation_sequence": json.Number(strconv.FormatUint(sequence, 10)),
		})
		if err != nil {
			return err
		}
		event, err := s.buildMergeActivationEvent(releasedRef, decisionRef, cand, prop.Sources(), eventID, sequence, in.LineageID, outboxKey)
		if err != nil {
			return err
		}
		eventPayloadDigest, err := payloadDigestOf(event)
		if err != nil {
			return err
		}

		// --- Idempotency: same key + same request digest replays the
		// recorded outcome with zero new writes.
		requestDigest, err := contract.DigestOf(map[string]any{
			"op":                "activate",
			"proposal_id":       prop.ID(),
			"decision_id":       decisionID,
			"decision_digest":   in.Decision.DecisionDigest(),
			"candidate_id":      cand.CandidateID,
			"body_digest":       cand.BodyDigest,
			"lineage_id":        in.LineageID,
			"activation_source": "gms-208",
		})
		if err != nil {
			return newError(ReasonDigestMismatch, "activation request digest: %v", err)
		}
		outcome := map[string]any{
			"released_ref":        similarity.SkillRefDoc(releasedRef),
			"activation_sequence": json.Number(strconv.FormatUint(sequence, 10)),
			"event_id":            eventID,
			"event_digest":        eventPayloadDigest,
			"outbox_key":          outboxKey,
		}
		outcomeBytes, err := json.Marshal(outcome)
		if err != nil {
			return err
		}
		replayed, recorded, err := tx.RecordIdempotency("merge-activate:"+prop.ID(), requestDigest, outcomeBytes)
		if err != nil {
			return err
		}
		if replayed {
			recordedDoc := decodeJSON(recorded)
			recordedRef, rerr := contract.ParseSkillArtifactRef(asObject(recordedDoc["released_ref"]))
			if rerr != nil {
				return rerr
			}
			result = &MergeReleaseResult{
				ReleasedRef:        recordedRef,
				ActivationSequence: parseDigits(recordedDoc["activation_sequence"]),
				EventID:            stringOf(recordedDoc["event_id"]),
				EventDigest:        stringOf(recordedDoc["event_digest"]),
				OutboxKey:          stringOf(recordedDoc["outbox_key"]),
				DerivedFromRefs:    pairSlice(prop.Sources()),
				Replayed:           true,
			}
			return nil
		}

		// --- The atomic write set.
		// 1. Decision into the evaluation ledger (deduplicated; the ledger
		//    identity of the decision is its canonical payload digest).
		entries, err := s.store.Snapshot(ledger.LedgerEvaluation, decisionID)
		if err != nil {
			return err
		}
		decisionPayloadDigest, err := payloadDigestOf(doc)
		if err != nil {
			return newError(ReasonReleaseDecisionInvalid, "decision does not canonicalize: %v", err)
		}
		for _, entry := range entries {
			if entry.PayloadDigest != decisionPayloadDigest {
				return newError(ReasonRefMismatch,
					"decision id %s already committed with a different digest (%s != %s)", decisionID, entry.PayloadDigest, decisionPayloadDigest)
			}
			break
		}
		if len(entries) == 0 {
			docBytes, err := json.Marshal(doc)
			if err != nil {
				return err
			}
			if _, err := tx.AppendEvent(ledger.LedgerEvaluation, decisionID, decisionID, docBytes); err != nil {
				return err
			}
		}
		// 2. Mapping record + lineage-version CAS (M@1: version 1 of the
		//    derived lineage assigned exactly once).
		mappingDigest, err := tx.PutContent(mappingBytes)
		if err != nil {
			return err
		}
		if err := tx.CompareAndSwapHead(ledger.HeadLineageVersion, in.LineageID, 0, "", version, mappingDigest); err != nil {
			return err
		}
		// 3. Skill-active head CAS of the derived lineage (M@1).
		if err := tx.CompareAndSwapHead(ledger.HeadSkillActive, in.LineageID, 0, "", 1, releasedRef.ArtifactDigest); err != nil {
			return err
		}
		// 4. Global activation sequence head CAS.
		if err := tx.CompareAndSwapHead(ledger.HeadActivationSequence, "activation", ledgerHead.Sequence, ledgerHead.PayloadDigest, sequence, eventPayloadDigest); err != nil {
			return err
		}
		// 5. §7.13 ActivationEvent append: TWO derived_from refs, NO
		//    supersedes of A/B (retain).
		eventBytes, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := tx.AppendEntry(ledger.Entry{
			Ledger:        ledger.LedgerActivation,
			Stream:        "",
			Sequence:      sequence,
			EventID:       eventID,
			PayloadDigest: eventPayloadDigest,
		}, eventBytes); err != nil {
			return err
		}
		// 6. Transactional outbox (delivery after commit).
		if err := tx.EnqueueOutbox(ledger.OutboxRecord{
			OutboxKey:          outboxKey,
			Ledger:             ledger.LedgerActivation,
			ActivationSequence: sequence,
			EventDigest:        eventPayloadDigest,
			ProjectionTarget:   outboxProjectionTarget,
			DeliveryState:      ledger.DeliveryPending,
		}); err != nil {
			return err
		}
		// 7. DUAL merge-source head CAS: both frozen expectations advance
		//    onto this activation (the machine's
		//    dual_source_head_cas_committed).
		for _, source := range prop.Sources() {
			key := mergeSourceHeadKey(source.LineageID, prop.GroupKey())
			pinSeq, pinDigest, ok, err := s.store.GetHead(ledger.HeadMergeSource, key)
			if err != nil {
				return err
			}
			if !ok || pinDigest == "" {
				return newError(ReasonMergeSourceHeadStale,
					"merge-source head of %s was never pinned by this proposal; admission is torn", source.LineageID)
			}
			payload, found, err := s.store.Get(pinDigest)
			if err != nil {
				return err
			}
			if found {
				value, perr := contract.ParseJSONStrict(payload)
				if perr != nil {
					return perr
				}
				pinObj, _ := contract.AsObject(value)
				if stringOf(pinObj["schema_version"]) != SchemaMergeSourceExpectation ||
					stringOf(nested(pinObj, "proposal_ref", "id")) != prop.ID() {
					return newError(ReasonMergeSourceHeadStale,
						"merge-source head of %s is pinned by another proposal; dual source-head CAS impossible", source.LineageID)
				}
			}
			if err := tx.CompareAndSwapHead(ledger.HeadMergeSource, key, pinSeq, pinDigest, pinSeq+1, eventPayloadDigest); err != nil {
				return err
			}
		}
		// 8. Merge terminal event activation_pending -> released, releasing
		//    the group-winner head onto the released event.
		recordRefs := []contract.VersionedRef{
			{ID: releasedRef.LineageID + "@v" + releasedRef.Version, Version: releasedRef.Version, Digest: releasedRef.ArtifactDigest},
			decisionRef,
		}
		staged, err := s.engine.appendEventTx(tx, prop, StateActivationPending, StateReleased, nil, recordRefs, pairSlice(prop.Sources()))
		if err != nil {
			return err
		}
		if err := s.releaseGroupWinnerTx(tx, prop, staged.EventDigest); err != nil {
			return err
		}
		result = &MergeReleaseResult{
			ReleasedRef:        releasedRef,
			ActivationSequence: sequence,
			EventID:            eventID,
			EventDigest:        eventPayloadDigest,
			OutboxKey:          outboxKey,
			DerivedFromRefs:    pairSlice(prop.Sources()),
		}
		return nil
	})
	if err != nil {
		code := CodeOf(err)
		if code == ReasonMergeSourceHeadStale || code == ReasonActiveHeadConflict || code == ReasonMergeGroupInFlight {
			// The transaction rolled back with zero partial activation;
			// the proposal stales terminally (M5).
			state, _, serr := s.CurrentState(proposalID)
			if serr == nil && state == StateActivationPending {
				return nil, s.staleProposal(ctx, prop, StateActivationPending, []string{code}, nil, "%v", err)
			}
		}
		return nil, err
	}
	return result, nil
}

// buildMergeActivationEvent assembles the §7.13 activate profile for the
// derived M lineage: exactly one release_decision_ref, body_digest_equal
// true, TWO derived_from refs (both sources), no supersedes; recomputes
// event_digest over the frozen x-digest preimage and re-validates the whole
// document against the authority schema.
func (s *Service) buildMergeActivationEvent(released contract.SkillArtifactRef, decision contract.VersionedRef, cand contract.CandidateArtifactRef, derivedFrom [2]contract.SkillArtifactRef, eventID string, sequence uint64, lineageID, outboxKey string) (map[string]any, error) {
	refs := make([]any, 0, len(derivedFrom))
	for _, source := range derivedFrom {
		refs = append(refs, similarity.SkillRefDoc(source))
	}
	payload := map[string]any{
		"schema_version":       ledger.SchemaActivationEvent,
		"activation_sequence":  json.Number(strconv.FormatUint(sequence, 10)),
		"event_id":             eventID,
		"event_type":           "activate",
		"lineage_id":           lineageID,
		"skill_ref":            similarity.SkillRefDoc(released),
		"release_decision_ref": versionedRefDoc(decision),
		"candidate_ref":        candidateRefJSONDoc(cand),
		"body_digest_equal":    true,
		"derived_from_refs":    refs,
		"outbox_key":           outboxKey,
	}
	digest, err := s.gates.ComputeDigestPreimage(payload, SchemaActivationEvent)
	if err != nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "activation event digest preimage: %v", err)
	}
	payload["event_digest"] = digest
	if err := s.gates.ValidateInstance(payload, SchemaActivationEvent); err != nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "activation event failed its authority schema: %v", err)
	}
	return payload, nil
}

// payloadDigestOf computes the whole-document canonical digest of the event
// payload (the ledger payload digest).
func payloadDigestOf(payload map[string]any) (string, error) {
	canonical, err := contract.JCS(contract.NormalizeForHashing(payload))
	if err != nil {
		return "", newError(ReasonDigestMismatch, "activation event canonicalization: %v", err)
	}
	return contract.DigestBytes(canonical), nil
}

// decodeJSON strict-parses recorded bytes (idempotent outcomes).
func decodeJSON(data []byte) map[string]any {
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return map[string]any{}
	}
	obj, _ := contract.AsObject(value)
	return obj
}

func parseDigits(v any) uint64 {
	s := digitsOf(v)
	out, _ := strconv.ParseUint(s, 10, 64)
	return out
}
