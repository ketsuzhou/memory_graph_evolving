// transaction.go is the GMS-204 atomic release transaction (GMS §6.2/§2.9,
// Contract §13.1): every write of one activation — version assignment,
// Candidate→Released mapping, skill-active head CAS, ActivationEvent with
// its global sequence, transactional outbox record, proposal terminal event
// — is staged in ONE ledger.TxManager.WithinTx unit of work. A failure in
// ANY step rolls the whole write set back; nothing partial ever becomes
// visible. Graph projection is never staged here (GMS-205 delivers it from
// the outbox after commit).
//
// Refactor seams (plan GMS-204): replayLineage is the read model,
// nextVersion the version allocator, buildMappingRecord the mapping writer
// and buildActivationEvent the single §7.13 event constructor.
package activation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
)

// ---------------------------------------------------------------------------
// Read model: authoritative lineage state replayed from the activation ledger
// ---------------------------------------------------------------------------

// lineageState is the replayed state of one lineage (GMS §2.6: the ledger is
// the complete history source; the head table is the exact CAS guard).
//
// headSeq counts this lineage's events in the global stream and headDigest
// mirrors exactly what the activation transaction last wrote into the
// skill-active head: the active artifact digest after activate/reactivate,
// the deactivate event digest after deactivation (the cleared marker).
type lineageState struct {
	active     *contract.SkillArtifactRef
	headSeq    uint64
	headDigest string
}

// replayLineage walks the single global activation stream and derives the
// current state of one lineage. Torn streams (unresolvable payloads, digest
// mismatches, unknown event types) fail closed.
func (s *ActivationService) replayLineage(lineageID string) (lineageState, error) {
	entries, err := s.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		return lineageState{}, err
	}
	state := lineageState{}
	for _, entry := range entries {
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil {
			return lineageState{}, err
		}
		if !ok {
			return lineageState{}, newError(ReasonActivationEventInvalid,
				"activation entry %d payload %s unresolved (torn stream)", entry.Sequence, entry.PayloadDigest)
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return lineageState{}, newError(ReasonActivationEventInvalid, "activation entry %d payload invalid: %v", entry.Sequence, err)
		}
		canonical, err := contract.JCS(contract.NormalizeForHashing(value))
		if err != nil || contract.DigestBytes(canonical) != entry.PayloadDigest {
			return lineageState{}, newError(ReasonActivationEventInvalid,
				"activation entry %d does not canonicalize to its ledger digest (torn stream)", entry.Sequence)
		}
		obj, _ := contract.AsObject(value)
		if lid, _ := contract.AsString(obj["lineage_id"]); lid != lineageID {
			continue
		}
		eventType, _ := contract.AsString(obj["event_type"])
		state.headSeq++
		switch eventType {
		case EventTypeActivate:
			refRaw, _ := contract.AsObject(obj["skill_ref"])
			ref, err := contract.ParseSkillArtifactRef(refRaw)
			if err != nil {
				return lineageState{}, newError(ReasonActivationEventInvalid, "activate event %d skill_ref invalid: %v", entry.Sequence, err)
			}
			state.active = &ref
			state.headDigest = ref.ArtifactDigest
		case EventTypeDeactivate:
			state.active = nil
			// The cleared marker the deactivation transaction wrote into the
			// head: the deactivate entry's whole-payload ledger digest (not
			// the event x-digest) — exactly what the next CAS must expect.
			state.headDigest = entry.PayloadDigest
		default:
			return lineageState{}, newError(ReasonProbationUnsupported,
				"committed activation entry %d carries event_type %q outside the closed {activate, deactivate} set", entry.Sequence, eventType)
		}
	}
	return state, nil
}

// expectActiveHead enforces the frozen CAS expectation against the replayed
// state (Contract §13.4: stale does not negate history, it rejects the
// frozen expectation). nil expected means "no active revision".
func expectActiveHead(state lineageState, expected *contract.SkillArtifactRef, lineageID string) error {
	if expected == nil {
		if state.active != nil {
			return newError(ReasonActiveHeadConflict,
				"lineage %s already has active %s v%s; expected no active head (stale expectation, Contract §13.4)", lineageID, state.active.LineageID, state.active.Version)
		}
		return nil
	}
	if state.active == nil {
		return newError(ReasonActiveHeadConflict,
			"lineage %s has no active revision but the frozen expectation is %s v%s", lineageID, expected.LineageID, expected.Version)
	}
	if *state.active != *expected {
		return newError(ReasonActiveHeadConflict,
			"lineage %s active head is %s v%s (%s) but the frozen expectation is %s v%s (%s)",
			lineageID, state.active.LineageID, state.active.Version, state.active.ArtifactDigest,
			expected.LineageID, expected.Version, expected.ArtifactDigest)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Version allocator (GMS §2.3/§2.9)
// ---------------------------------------------------------------------------

// nextVersion derives the next lineage version from the lineage-version
// head: versions are monotone, contiguous and never reused — the head CAS is
// the sole allocator guard, so a duplicate or raced version can never be
// written through it. A head whose mapping record does not resolve (a racing
// writer consumed the slot without content) still counts as consumed: the
// allocator returns seq+1 and never hands the taken slot out again.
func (s *ActivationService) nextVersion(lineageID string) (version, prevSeq uint64, prevDigest string, err error) {
	seq, digest, ok, err := s.store.GetHead(ledger.HeadLineageVersion, lineageID)
	if err != nil {
		return 0, 0, "", err
	}
	if !ok {
		return 1, 0, "", nil // new lineage: expected head absent
	}
	if seq == 0 || digest == "" {
		return 0, 0, "", newError(ReasonLineageVersionConflict, "lineage %s version head (%d, %s) is malformed", lineageID, seq, digest)
	}
	return seq + 1, seq, digest, nil
}

// resolveMapping reads and parses one content-addressed mapping record.
func (s *ActivationService) resolveMapping(digest string) (map[string]any, error) {
	raw, ok, err := s.store.Get(digest)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newError(ReasonLineageVersionConflict,
			"lineage-version head points at mapping %s which resolves to no committed content (torn version assignment)", digest)
	}
	value, err := contract.ParseJSONStrict(raw)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "release mapping %s is not valid JSON: %v", digest, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		return nil, newError(ReasonDigestMismatch, "release mapping %s is not an object", digest)
	}
	if sv, _ := contract.AsString(obj["schema_version"]); sv != SchemaReleaseMapping {
		return nil, newError(ReasonDigestMismatch, "release mapping %s schema_version %q is not %s", digest, sv, SchemaReleaseMapping)
	}
	return obj, nil
}

// findMappingForVersion walks the mapping chain (each record carries its
// predecessor's digest) from the lineage-version head down to the wanted
// version. Reactivation resolves historical mappings this way; nothing is
// ever mutated.
func (s *ActivationService) findMappingForVersion(lineageID, version string) (map[string]any, bool, error) {
	_, digest, ok, err := s.store.GetHead(ledger.HeadLineageVersion, lineageID)
	if err != nil || !ok {
		return nil, false, err
	}
	for hop := 0; digest != "" && hop < 1<<20; hop++ {
		mapping, err := s.resolveMapping(digest)
		if err != nil {
			return nil, false, err
		}
		if jsonDigits(mapping["version"]) == version {
			return mapping, true, nil
		}
		prev, _ := contract.AsString(mapping["previous_mapping_digest"])
		digest = prev
	}
	return nil, false, nil
}

// ---------------------------------------------------------------------------
// Record builders (mapping, event, outbox key)
// ---------------------------------------------------------------------------

func skillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         json.Number(ref.Version),
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

func versionedRefDoc(ref contract.VersionedRef) map[string]any {
	return map[string]any{"id": ref.ID, "version": json.Number(ref.Version), "digest": ref.Digest}
}

func candidateRefDoc(ref contract.CandidateArtifactRef) map[string]any {
	return map[string]any{
		"schema_version": ref.SchemaVersion,
		"candidate_id":   ref.CandidateID,
		"kind":           ref.Kind,
		"body_digest":    ref.BodyDigest,
		"origin_type":    ref.OriginType,
		"origin_ref":     versionedRefDoc(ref.OriginRef),
	}
}

// buildMappingRecord renders the release mapping: the immutable proof that
// lineage/version was assigned exactly once, to exactly this candidate, with
// body_digest_equal=true (the Candidate→Released byte-equality mapping,
// GMS §2.9). The record is content-addressed and bound by the
// lineage-version head CAS.
func buildMappingRecord(lineageID string, version uint64, released contract.SkillArtifactRef, cand contract.CandidateArtifactRef, decision contract.VersionedRef, previousMappingDigest string) (map[string]any, []byte, error) {
	doc := map[string]any{
		"schema_version":       SchemaReleaseMapping,
		"lineage_id":           lineageID,
		"version":              json.Number(strconv.FormatUint(version, 10)),
		"kind":                 released.Kind,
		"released_ref":         skillRefDoc(released),
		"candidate_ref":        candidateRefDoc(cand),
		"release_decision_ref": versionedRefDoc(decision),
		"body_digest_equal":    true,
	}
	if previousMappingDigest != "" {
		doc["previous_mapping_digest"] = previousMappingDigest
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, nil, newError(ReasonDigestMismatch, "release mapping marshal: %v", err)
	}
	return doc, data, nil
}

// activationEventSpec is the frozen input of one §7.13 ActivationEvent.
// Optional §7.13 fields are nil/absent unless explicitly set — the event is
// exactly what the spec carries, nothing more.
type activationEventSpec struct {
	eventType                    string // activate | deactivate (closed set)
	eventID                      string
	sequence                     uint64
	lineageID                    string
	skillRef                     contract.SkillArtifactRef
	previousActiveRef            *contract.SkillArtifactRef
	releaseDecisionRef           *contract.VersionedRef
	deactivationAuthorizationRef *contract.VersionedRef
	candidateRef                 *contract.CandidateArtifactRef
	bodyDigestEqual              *bool
	derivedFromRefs              []contract.SkillArtifactRef
	expectedActiveHead           *contract.SkillArtifactRef
	outboxKey                    string
}

// buildActivationEvent is the SINGLE Contract §7.13 event constructor:
// it assembles the payload for the event-type profile (activate via
// activation-event.schema.json, deactivate via deactivation-event.schema.json),
// recomputes event_digest over the frozen x-digest preimage and re-validates
// the whole document against the authority schema. Any event_type outside
// {activate, deactivate} — probation included — fails closed with
// PROBATION_UNSUPPORTED (Contract §9.2.6, GMS §6.4): there is no other way
// to construct a ledger event in this package.
func (s *ActivationService) buildActivationEvent(spec activationEventSpec) (map[string]any, string, error) {
	if !activationEventTypes[spec.eventType] {
		return nil, "", newError(ReasonProbationUnsupported,
			"event_type %q is outside the closed v1 set {activate, deactivate}; probation is a reserved, unreachable state and no activation-side door produces it (Contract §9.2.6, GMS §6.4)", spec.eventType)
	}
	if spec.eventID == "" {
		return nil, "", newError(ReasonActivationEventInvalid, "event id required")
	}
	if spec.outboxKey == "" {
		return nil, "", newError(ReasonActivationEventInvalid, "outbox key required (Contract §7.13)")
	}
	payload := map[string]any{
		"schema_version":      ledger.SchemaActivationEvent,
		"activation_sequence": json.Number(strconv.FormatUint(spec.sequence, 10)),
		"event_id":            spec.eventID,
		"event_type":          spec.eventType,
		"lineage_id":          spec.lineageID,
		"skill_ref":           skillRefDoc(spec.skillRef),
	}
	if spec.previousActiveRef != nil {
		payload["previous_active_ref"] = skillRefDoc(*spec.previousActiveRef)
	}
	if spec.releaseDecisionRef != nil {
		payload["release_decision_ref"] = versionedRefDoc(*spec.releaseDecisionRef)
	}
	if spec.deactivationAuthorizationRef != nil {
		payload["deactivation_authorization_ref"] = versionedRefDoc(*spec.deactivationAuthorizationRef)
	}
	if spec.candidateRef != nil {
		payload["candidate_ref"] = candidateRefDoc(*spec.candidateRef)
	}
	if spec.bodyDigestEqual != nil {
		payload["body_digest_equal"] = *spec.bodyDigestEqual
	}
	if len(spec.derivedFromRefs) > 0 {
		refs := make([]any, 0, len(spec.derivedFromRefs))
		for _, ref := range spec.derivedFromRefs {
			refs = append(refs, skillRefDoc(ref))
		}
		payload["derived_from_refs"] = refs
	}
	if spec.expectedActiveHead != nil {
		payload["expected_active_head"] = skillRefDoc(*spec.expectedActiveHead)
	}
	payload["outbox_key"] = spec.outboxKey

	schemaFile := SchemaActivationEvent
	if spec.eventType == EventTypeDeactivate {
		schemaFile = SchemaDeactivationEvent
	}
	digest, err := s.gates.ComputeDigestPreimage(payload, schemaFile)
	if err != nil {
		return nil, "", newError(ReasonActivationEventInvalid, "event digest preimage: %v", err)
	}
	payload["event_digest"] = digest
	if err := s.gates.ValidateInstance(payload, schemaFile); err != nil {
		return nil, "", newError(ReasonActivationEventInvalid, "activation event failed its authority schema: %v", err)
	}
	return payload, digest, nil
}

// deriveOutboxKey mints the fixed outbox key of one activation/deactivation
// from pre-event inputs only (never from the event digest — the digest
// preimage contains the outbox key, so deriving it from the digest would be
// circular). The same operation inputs always derive the same key: same key
// stable (GMS §2.6).
func deriveOutboxKey(op string, identity string, sequence uint64) (string, error) {
	return contract.DigestOf(map[string]any{
		"outbox_for":          op,
		"identity":            identity,
		"activation_sequence": json.Number(strconv.FormatUint(sequence, 10)),
	})
}

// payloadDigest computes the ledger payload digest (whole canonical event
// document) that the entry, outbox record and sequence head all bind to.
func payloadDigest(payload map[string]any) (string, error) {
	canonical, err := contract.JCS(contract.NormalizeForHashing(deepCopyValue(payload)))
	if err != nil {
		return "", newError(ReasonActivationEventInvalid, "event payload canonicalization: %v", err)
	}
	return contract.DigestBytes(canonical), nil
}

func marshalDoc(doc map[string]any) ([]byte, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, newError(ReasonActivationEventInvalid, "payload marshal: %v", err)
	}
	return data, nil
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[k] = deepCopyValue(item)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}

// ---------------------------------------------------------------------------
// Activate transaction
// ---------------------------------------------------------------------------

type activateInput struct {
	decision      *decisionView
	candidate     contract.CandidateArtifactRef
	lineageID     string
	expected      *contract.SkillArtifactRef
	proposalID    string
	proposal      *proposal.Proposal
	proposalState string // committed state when Activate entered ("released" only legal as an idempotent replay)
}

// commitActivate runs the single atomic release transaction (GMS §6.2):
//
//	reads -> deterministic artifacts -> idempotency (replay returns the
//	recorded outcome, zero new writes) -> frozen guards -> [ decision
//	ledger append, mapping content put + lineage-version CAS, skill-active
//	CAS, activation-sequence CAS, ActivationEvent append, outbox enqueue,
//	proposal released terminal event ] -> commit
//
// The idempotency probe precedes the head-expectation and proposal-state
// guards on purpose: a retry of the SAME request lands in a world the first
// attempt already changed (an active head exists, the proposal is released),
// and Contract §13.2 requires it to replay the recorded outcome, not to
// collide with its own effects. Every bracketed write stages in the same
// WithinTx unit: any failure rolls all of them back (Manager commit order:
// content -> CAS -> appends -> outbox -> idempotency; a mid-apply failure
// compensates every applied write). The Graph is never touched.
func (s *ActivationService) commitActivate(ctx context.Context, in activateInput) (*ReleaseResult, error) {
	var result *ReleaseResult
	err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		// --- Read model (reads only; nothing is staged yet).
		state, err := s.replayLineage(in.lineageID)
		if err != nil {
			return err
		}
		version, prevSeq, prevMappingDigest, err := s.nextVersion(in.lineageID)
		if err != nil {
			return err
		}
		sequence, err := tx.NextSequence(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}
		ledgerHead, _, err := s.store.Head(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}

		// --- Deterministic artifacts of this release.
		releasedRef := contract.SkillArtifactRef{
			SchemaVersion:  contract.SchemaSkillArtifactRef,
			LineageID:      in.lineageID,
			Version:        strconv.FormatUint(version, 10),
			Kind:           in.candidate.Kind,
			ArtifactDigest: in.candidate.BodyDigest, // candidate and released bodies digest equal
		}
		decisionRef := contract.VersionedRef{ID: in.decision.id, Version: in.decision.version, Digest: in.decision.digest}
		if in.decision.version == "" {
			decisionRef.Version = "1"
		}
		mappingDoc, mappingBytes, err := buildMappingRecord(in.lineageID, version, releasedRef, in.candidate, decisionRef, prevMappingDigest)
		if err != nil {
			return err
		}
		eventID := fmt.Sprintf("act-%s-%s-v%d", in.decision.id, in.lineageID, version)
		outboxKey, err := deriveOutboxKey(EventTypeActivate, in.decision.id+"/"+in.candidate.CandidateID+"/"+in.lineageID+"/"+strconv.FormatUint(version, 10), sequence)
		if err != nil {
			return err
		}
		event, _, err := s.buildActivationEvent(activationEventSpec{
			eventType:          EventTypeActivate,
			eventID:            eventID,
			sequence:           sequence,
			lineageID:          in.lineageID,
			skillRef:           releasedRef,
			previousActiveRef:  state.active,
			releaseDecisionRef: &decisionRef,
			candidateRef:       &in.candidate,
			bodyDigestEqual:    boolPtr(true), // proven by verifyBodyEquality
			expectedActiveHead: in.expected,
			outboxKey:          outboxKey,
		})
		if err != nil {
			return err
		}
		eventPayloadDigest, err := payloadDigest(event)
		if err != nil {
			return err
		}

		// --- Idempotency: same key + same request digest replays the
		// recorded outcome with zero new writes (Contract §13.2).
		requestDigest, err := contract.DigestOf(map[string]any{
			"op":                EventTypeActivate,
			"decision_id":       in.decision.id,
			"decision_digest":   in.decision.digest,
			"candidate_id":      in.candidate.CandidateID,
			"body_digest":       in.candidate.BodyDigest,
			"lineage_id":        in.lineageID,
			"expected_head":     expectedHeadDigestInput(in.expected),
			"activation_source": "gms-204",
		})
		if err != nil {
			return newError(ReasonDigestMismatch, "activation request digest: %v", err)
		}
		outcome := map[string]any{
			"released_ref":         skillRefDoc(releasedRef),
			"activation_sequence":  json.Number(strconv.FormatUint(sequence, 10)),
			"event_id":             eventID,
			"event_digest":         eventPayloadDigest,
			"outbox_key":           outboxKey,
			"previous_active_head": previousHeadDoc(state.active),
		}
		recorded, err := marshalDoc(outcome)
		if err != nil {
			return err
		}
		replayed, priorOutcome, err := tx.RecordIdempotency("activation:"+in.decision.id, requestDigest, recorded)
		if err != nil {
			return err
		}
		if replayed {
			result, err = decodeReleaseResult(priorOutcome)
			return err
		}

		// --- Frozen guards (only reached by a first-time request: a replay
		// returned above).
		if in.proposalState != StateActivationPending {
			return newError(ReasonProposalStateConflict,
				"release requires proposal %s in %s, current state is %q", in.proposalID, StateActivationPending, in.proposalState)
		}
		if err := expectActiveHead(state, in.expected, in.lineageID); err != nil {
			return err
		}

		// --- The atomic write set.
		// 1. Release decision into the evaluation ledger (deduplicated; a
		//    reused decision id with a different digest is a conflict).
		if err := s.stageDecision(tx, in.decision); err != nil {
			return err
		}
		// 2. Mapping record content + lineage-version CAS (version
		//    assignment: monotone, no reuse; a raced version conflicts).
		mappingDigest, err := tx.PutContent(mappingBytes)
		if err != nil {
			return err
		}
		if err := tx.CompareAndSwapHead(ledger.HeadLineageVersion, in.lineageID, prevSeq, prevMappingDigest, version, mappingDigest); err != nil {
			return err
		}
		// 3. Skill-active head CAS (exact expected head, GMS §2.6).
		if err := tx.CompareAndSwapHead(ledger.HeadSkillActive, in.lineageID, state.headSeq, state.headDigest, state.headSeq+1, releasedRef.ArtifactDigest); err != nil {
			return err
		}
		// 4. Global activation sequence head CAS (single stream guard).
		if err := tx.CompareAndSwapHead(ledger.HeadActivationSequence, activationStreamKey, ledgerHead.Sequence, ledgerHead.PayloadDigest, sequence, eventPayloadDigest); err != nil {
			return err
		}
		// 5. ActivationEvent append (the authoritative history).
		eventBytes, err := marshalDoc(event)
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
		// 6. Transactional outbox (projection delivery happens AFTER commit).
		if err := tx.EnqueueOutbox(ledger.OutboxRecord{
			OutboxKey:          outboxKey,
			Ledger:             ledger.LedgerActivation,
			ActivationSequence: sequence,
			EventDigest:        eventPayloadDigest,
			ProjectionTarget:   ProjectionTargetRuntime,
			DeliveryState:      ledger.DeliveryPending,
		}); err != nil {
			return err
		}
		// 7. Proposal terminal event: activation_pending -> released.
		recordRefs := []contract.VersionedRef{
			{ID: releasedRef.LineageID + "@v" + releasedRef.Version, Version: releasedRef.Version, Digest: releasedRef.ArtifactDigest},
			{ID: in.decision.id, Version: decisionRef.Version, Digest: in.decision.digest},
		}
		if _, err := s.proposals.AppendTransitionTx(tx, in.proposal, StateActivationPending, StateReleased, recordRefs); err != nil {
			return err
		}
		_ = mappingDoc
		result = &ReleaseResult{
			ReleasedRef:        releasedRef,
			ActivationSequence: sequence,
			EventID:            eventID,
			EventDigest:        eventPayloadDigest,
			OutboxKey:          outboxKey,
			PreviousActiveRef:  state.active,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// stageDecision appends the §7.12 decision into the evaluation ledger unless
// the identical decision is already committed (idempotent); the same
// decision id with a different digest is REF_MISMATCH.
func (s *ActivationService) stageDecision(tx *ledger.Tx, dv *decisionView) error {
	entries, err := s.store.Snapshot(ledger.LedgerEvaluation, dv.id)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.PayloadDigest != dv.digest {
			return newError(ReasonRefMismatch,
				"decision id %s already committed with a different digest (%s != %s)", dv.id, entry.PayloadDigest, dv.digest)
		}
		return nil
	}
	payload, err := marshalDoc(dv.doc)
	if err != nil {
		return err
	}
	if _, err := tx.AppendEvent(ledger.LedgerEvaluation, dv.id, dv.id, payload); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Deactivate transaction
// ---------------------------------------------------------------------------

// commitDeactivate runs the protected compensating transaction (Contract
// §9.2.3/§13.5): current-head CAS expectation, deactivate event append with
// a fresh global sequence, outbox enqueue, skill-active head CAS to the
// cleared marker. The mapping, lineage-version head, artifacts, prior events
// and the proposal terminal state are NOT touched — deactivation only ever
// APPENDS.
//
// Deactivation carries no idempotency record: its own read model is the
// guard — after a successful deactivation the lineage has no active head, so
// replaying the same request fails closed with NO_ACTIVE_HEAD and can never
// append a second event (Contract §13.2 satisfied by state, not by replay).
func (s *ActivationService) commitDeactivate(ctx context.Context, req DeactivateRequest) (*DeactivationResult, error) {
	var result *DeactivationResult
	err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		state, err := s.replayLineage(req.LineageID)
		if err != nil {
			return err
		}
		if state.active == nil {
			return newError(ReasonNoActiveHead, "lineage %s has no active revision to deactivate", req.LineageID)
		}
		if *state.active != req.SkillRef {
			return newError(ReasonActiveHeadConflict,
				"deactivation target %s v%s (%s) is not the current active revision %s v%s (%s); deactivate MUST CAS the current head",
				req.SkillRef.LineageID, req.SkillRef.Version, req.SkillRef.ArtifactDigest,
				state.active.LineageID, state.active.Version, state.active.ArtifactDigest)
		}
		sequence, err := tx.NextSequence(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}
		ledgerHead, _, err := s.store.Head(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}

		eventID := fmt.Sprintf("deact-%s-%s", req.Authorization.ID, req.LineageID)
		outboxKey, err := deriveOutboxKey(EventTypeDeactivate, req.Authorization.ID+"/"+req.LineageID+"/"+req.SkillRef.Version+"/"+req.SkillRef.ArtifactDigest, sequence)
		if err != nil {
			return err
		}
		event, _, err := s.buildActivationEvent(activationEventSpec{
			eventType:                    EventTypeDeactivate,
			eventID:                      eventID,
			sequence:                     sequence,
			lineageID:                    req.LineageID,
			skillRef:                     req.SkillRef,
			previousActiveRef:            state.active,
			deactivationAuthorizationRef: &req.Authorization,
			expectedActiveHead:           state.active,
			outboxKey:                    outboxKey,
		})
		if err != nil {
			return err
		}
		eventPayloadDigest, err := payloadDigest(event)
		if err != nil {
			return err
		}

		// Atomic write set: sequence head CAS, event append, outbox, head
		// clear. No mapping, no version, no proposal write, no idempotency
		// record (the absent active head IS the replay guard).
		if err := tx.CompareAndSwapHead(ledger.HeadActivationSequence, activationStreamKey, ledgerHead.Sequence, ledgerHead.PayloadDigest, sequence, eventPayloadDigest); err != nil {
			return err
		}
		eventBytes, err := marshalDoc(event)
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
		if err := tx.EnqueueOutbox(ledger.OutboxRecord{
			OutboxKey:          outboxKey,
			Ledger:             ledger.LedgerActivation,
			ActivationSequence: sequence,
			EventDigest:        eventPayloadDigest,
			ProjectionTarget:   ProjectionTargetRuntime,
			DeliveryState:      ledger.DeliveryPending,
		}); err != nil {
			return err
		}
		// The cleared marker: the head advances (monotone CAS) and points at
		// the deactivate event digest; the read model resolves "no active".
		if err := tx.CompareAndSwapHead(ledger.HeadSkillActive, req.LineageID, state.headSeq, state.headDigest, state.headSeq+1, eventPayloadDigest); err != nil {
			return err
		}
		result = &DeactivationResult{
			DeactivatedRef:     req.SkillRef,
			ActivationSequence: sequence,
			EventID:            eventID,
			EventDigest:        eventPayloadDigest,
			OutboxKey:          outboxKey,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Reactivate transaction
// ---------------------------------------------------------------------------

type reactivateInput struct {
	decision  *decisionView
	released  contract.SkillArtifactRef
	body      string // released body digest
	candidate contract.CandidateArtifactRef
	expected  *contract.SkillArtifactRef
}

// commitReactivate writes a NEW activate event for an already-released
// revision (Contract §9.2.7): fresh global sequence, head CAS back onto the
// released ref, outbox record. No version is assigned, no mapping written,
// no prior event rewritten — compensating activation, append-only. The
// idempotency probe precedes the head-expectation guard so a retry of the
// same reactivation replays its recorded outcome instead of colliding with
// its own committed effects (Contract §13.2).
func (s *ActivationService) commitReactivate(ctx context.Context, in reactivateInput) (*ReleaseResult, error) {
	var result *ReleaseResult
	err := s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		state, err := s.replayLineage(in.released.LineageID)
		if err != nil {
			return err
		}
		sequence, err := tx.NextSequence(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}
		ledgerHead, _, err := s.store.Head(ledger.LedgerActivation, "")
		if err != nil {
			return err
		}

		decisionRef := contract.VersionedRef{ID: in.decision.id, Version: in.decision.version, Digest: in.decision.digest}
		if in.decision.version == "" {
			decisionRef.Version = "1"
		}
		eventID := fmt.Sprintf("act-%s-%s-v%s-reactivate", in.decision.id, in.released.LineageID, in.released.Version)
		outboxKey, err := deriveOutboxKey(EventTypeActivate, in.decision.id+"/reactivate/"+in.released.LineageID+"/"+in.released.Version, sequence)
		if err != nil {
			return err
		}
		event, _, err := s.buildActivationEvent(activationEventSpec{
			eventType:          EventTypeActivate,
			eventID:            eventID,
			sequence:           sequence,
			lineageID:          in.released.LineageID,
			skillRef:           in.released,
			previousActiveRef:  state.active,
			releaseDecisionRef: &decisionRef,
			bodyDigestEqual:    boolPtr(true), // re-proven against the stored mapping
			expectedActiveHead: in.expected,
			outboxKey:          outboxKey,
			// candidate_ref intentionally absent: not the first activation
			// of this released revision (Contract §7.13).
		})
		if err != nil {
			return err
		}
		eventPayloadDigest, err := payloadDigest(event)
		if err != nil {
			return err
		}

		requestDigest, err := contract.DigestOf(map[string]any{
			"op":                "reactivate",
			"decision_id":       in.decision.id,
			"decision_digest":   in.decision.digest,
			"lineage_id":        in.released.LineageID,
			"version":           json.Number(in.released.Version),
			"artifact_digest":   in.released.ArtifactDigest,
			"expected_head":     expectedHeadDigestInput(in.expected),
			"activation_source": "gms-204",
		})
		if err != nil {
			return newError(ReasonDigestMismatch, "reactivation request digest: %v", err)
		}
		outcome := map[string]any{
			"released_ref":         skillRefDoc(in.released),
			"activation_sequence":  json.Number(strconv.FormatUint(sequence, 10)),
			"event_id":             eventID,
			"event_digest":         eventPayloadDigest,
			"outbox_key":           outboxKey,
			"previous_active_head": previousHeadDoc(state.active),
		}
		recorded, err := marshalDoc(outcome)
		if err != nil {
			return err
		}
		replayed, priorOutcome, err := tx.RecordIdempotency("reactivation:"+in.decision.id, requestDigest, recorded)
		if err != nil {
			return err
		}
		if replayed {
			result, err = decodeReleaseResult(priorOutcome)
			return err
		}
		if err := expectActiveHead(state, in.expected, in.released.LineageID); err != nil {
			return err
		}

		// Atomic write set: sequence head CAS, activate event append, outbox,
		// skill-active head CAS back onto the released ref. No version, no
		// mapping, no proposal event.
		if err := tx.CompareAndSwapHead(ledger.HeadActivationSequence, activationStreamKey, ledgerHead.Sequence, ledgerHead.PayloadDigest, sequence, eventPayloadDigest); err != nil {
			return err
		}
		eventBytes, err := marshalDoc(event)
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
		if err := tx.EnqueueOutbox(ledger.OutboxRecord{
			OutboxKey:          outboxKey,
			Ledger:             ledger.LedgerActivation,
			ActivationSequence: sequence,
			EventDigest:        eventPayloadDigest,
			ProjectionTarget:   ProjectionTargetRuntime,
			DeliveryState:      ledger.DeliveryPending,
		}); err != nil {
			return err
		}
		if err := tx.CompareAndSwapHead(ledger.HeadSkillActive, in.released.LineageID, state.headSeq, state.headDigest, state.headSeq+1, in.released.ArtifactDigest); err != nil {
			return err
		}
		result = &ReleaseResult{
			ReleasedRef:        in.released,
			ActivationSequence: sequence,
			EventID:            eventID,
			EventDigest:        eventPayloadDigest,
			OutboxKey:          outboxKey,
			PreviousActiveRef:  state.active,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Recorded-outcome codecs (idempotent replay)
// ---------------------------------------------------------------------------

func decodeReleaseResult(recorded []byte) (*ReleaseResult, error) {
	value, err := contract.ParseJSONStrict(recorded)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "recorded activation outcome invalid: %v", err)
	}
	obj, _ := contract.AsObject(value)
	refRaw, _ := contract.AsObject(obj["released_ref"])
	ref, err := contract.ParseSkillArtifactRef(refRaw)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "recorded activation outcome invalid: %v", err)
	}
	res := &ReleaseResult{
		ReleasedRef:        ref,
		ActivationSequence: digitsOf(obj["activation_sequence"]),
		EventID:            stringOf(obj["event_id"]),
		EventDigest:        stringOf(obj["event_digest"]),
		OutboxKey:          stringOf(obj["outbox_key"]),
		Replayed:           true,
	}
	if prevRaw, present := obj["previous_active_head"]; present {
		if prevObj, ok := contract.AsObject(prevRaw); ok {
			if prev, err := contract.ParseSkillArtifactRef(prevObj); err == nil {
				res.PreviousActiveRef = &prev
			}
		}
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func boolPtr(v bool) *bool { return &v }

func stringOf(v any) string {
	s, _ := contract.AsString(v)
	return s
}

func digitsOf(v any) uint64 {
	n, ok := v.(json.Number)
	if !ok {
		return 0
	}
	u, err := strconv.ParseUint(string(n), 10, 64)
	if err != nil {
		return 0
	}
	return u
}

// expectedHeadDigestInput renders the frozen expectation for request
// digests (nil-safe: absent head is part of the frozen request).
func expectedHeadDigestInput(expected *contract.SkillArtifactRef) any {
	if expected == nil {
		return nil
	}
	return skillRefDoc(*expected)
}

func previousHeadDoc(active *contract.SkillArtifactRef) any {
	if active == nil {
		return nil
	}
	return skillRefDoc(*active)
}
