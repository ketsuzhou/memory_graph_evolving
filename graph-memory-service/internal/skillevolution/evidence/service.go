package evidence

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// AdmissionService is the protected evidence committer (GMS §2.2/§12.2): it
// stages Host sealed Segment submissions GMS-privately, runs the fail-closed
// admission gates against the FND-002 sealed-segment protocol and — only for
// valid settled evidence — commits the canonical evidence body, the Contract
// §7.7 EvidenceRef and the idempotency result in ONE atomic transaction over
// the GMS-102 ledger ports (Contract §13.1). A failed or conflicting commit
// leaves no partial record: no content blob, no ledger append, no idempotency
// key consumed.
type AdmissionService struct {
	mu       sync.Mutex // guards the staging map, counters and provenance index
	commitMu sync.Mutex // serializes commits within this service instance

	nextSeq    uint64
	staging    map[string]*stagingState
	provenance map[string]*commitProvenance // evidenceID → committed view

	store    ledger.Store
	mgr      ledger.TxManager
	registry ledger.ReasonRegistry
	scope    ScopePolicy
}

// stagingState is the private intake record of one submission (never exposed
// outside StagingRecordView copies). The parsed values are frozen at Stage
// time; Validate only reads them.
type stagingState struct {
	id               string
	sub              SubmittedSegment
	segmentValue     map[string]any
	sealValue        map[string]any
	submissionDigest string
	status           Status
	outcome          *ValidationOutcome
	committedID      string // evidence id once committed
	events           []StateEvent
}

// commitProvenance is the derived, rebuildable provenance index of one
// committed EvidenceRef; the evidence ledger stays the authority.
type commitProvenance struct {
	key           string
	requestDigest string
	digest        string
	evidence      CommittedEvidence
}

// NewAdmissionService wires the committer to the GMS-102 transaction manager
// and reason registry. It fails closed at construction time unless every
// reason code the pipeline can emit exists in the digest-verified system
// registry (Contract §13.7.1 R5).
func NewAdmissionService(store ledger.Store, mgr ledger.TxManager, registry ledger.ReasonRegistry, scope ScopePolicy) (*AdmissionService, error) {
	if store == nil {
		return nil, fmt.Errorf("evidence: nil store")
	}
	if mgr == nil {
		return nil, fmt.Errorf("evidence: nil transaction manager")
	}
	if err := verifyAdmissionReasonCodes(registry); err != nil {
		return nil, err
	}
	return &AdmissionService{
		staging:    make(map[string]*stagingState),
		provenance: make(map[string]*commitProvenance),
		store:      store,
		mgr:        mgr,
		registry:   registry,
		scope:      scope,
	}, nil
}

// ---------------------------------------------------------------------------
// Stage: private intake (GMS §2.2 "received")
// ---------------------------------------------------------------------------

// Stage records the raw submission with status staged and returns the staged
// id. Intake is deliberately shallow — shape, schema versions and identity
// only; every seal, scope and integrity decision belongs to Validate. Staged
// submissions are invisible to CommittedEvidence/GetEvidence and can never be
// referenced by projection or proposal callers.
func (s *AdmissionService) Stage(sub SubmittedSegment) (string, error) {
	if sub.Submitter == "" {
		return "", newAdmissionError(s.registry, ReasonEvidenceProvenanceIncomplete, "submitter identity is required (GMS §2.2)")
	}
	if sub.Scope.RoomID == "" || sub.Scope.ScopeProfileRef.ID == "" ||
		sub.Scope.ScopeProfileRef.Version == "" || sub.Scope.ScopeProfileRef.Digest == "" {
		return "", newAdmissionError(s.registry, ReasonEvidenceProvenanceIncomplete, "scope declaration (room + exact scope profile ref) is required")
	}
	if len(sub.Canonical) == 0 {
		return "", newAdmissionError(s.registry, ReasonEvidenceStagingInvalid, "canonical bytes are required")
	}

	segmentValue, err := s.strictParse(sub.SegmentJSON, "segment.json")
	if err != nil {
		return "", err
	}
	sealValue, err := s.strictParse(sub.SealJSON, "seal.json")
	if err != nil {
		return "", err
	}
	segment, okSeg := contract.AsObject(segmentValue)
	seal, okSeal := contract.AsObject(sealValue)
	if !okSeg || !okSeal {
		return "", newAdmissionError(s.registry, ReasonEvidenceStagingInvalid, "segment record and seal envelope must be JSON objects")
	}
	if sv, _ := contract.AsString(segment["schema_version"]); sv != schemaSealedSegment {
		return "", newAdmissionError(s.registry, ReasonSchemaVersionUnsupported, "segment schema_version %q is not %q", sv, schemaSealedSegment)
	}
	if sv, _ := contract.AsString(seal["schema_version"]); sv != schemaSegmentSeal {
		return "", newAdmissionError(s.registry, ReasonSchemaVersionUnsupported, "seal schema_version %q is not %q", sv, schemaSegmentSeal)
	}
	segmentID, _ := contract.AsString(segment["segment_id"])
	roomID, _ := contract.AsString(segment["room_id"])
	terminal, _ := contract.AsString(segment["terminal_state"])
	if segmentID == "" || roomID == "" {
		return "", newAdmissionError(s.registry, ReasonEvidenceStagingInvalid, "segment identity (room_id, segment_id) is required")
	}
	if !segmentTerminals[terminal] {
		return "", newAdmissionError(s.registry, ReasonSchemaEnumInvalid, "terminal_state %q outside settled|failed|aborted", terminal)
	}
	if sealID, _ := contract.AsString(seal["segment_id"]); sealID != segmentID {
		return "", newAdmissionError(s.registry, ReasonRefMismatch, "seal envelope names segment %q but the record is %q", sealID, segmentID)
	}
	// The record must be able to enter the hashed core at all.
	if _, err := contract.JCS(segment); err != nil {
		return "", newAdmissionError(s.registry, ReasonNonIntegerNumber, "segment record cannot enter the hashed core: %v", err)
	}

	submissionDigest, err := contract.DigestOf(map[string]any{
		"schema_version": "gms.evidence-staging.v1",
		"segment_bytes":  contract.DigestBytes(sub.SegmentJSON),
		"seal_bytes":     contract.DigestBytes(sub.SealJSON),
		"canonical":      contract.DigestBytes(sub.Canonical),
		"submitter":      sub.Submitter,
	})
	if err != nil {
		return "", newAdmissionError(s.registry, ReasonNonIntegerNumber, "submission digest: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSeq++
	id := fmt.Sprintf("staged-%06d", s.nextSeq)
	s.staging[id] = &stagingState{
		id:               id,
		sub:              sub,
		segmentValue:     segment,
		sealValue:        seal,
		submissionDigest: submissionDigest,
		status:           StatusStaged,
		events:           []StateEvent{{From: StatusStaged, To: StatusStaged, Note: "raw submission recorded"}},
	}
	return id, nil
}

func (s *AdmissionService) strictParse(data []byte, origin string) (any, error) {
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return nil, newAdmissionError(s.registry, ReasonInvalidJSON, "%s is not strict UTF-8 JSON: %v", origin, err)
	}
	return value, nil
}

// ---------------------------------------------------------------------------
// Validate: the admission gates (staged → validating → terminal/valid)
// ---------------------------------------------------------------------------

// Validate runs the full fail-closed gate suite once per submission:
//
//  1. provenance shape and declared-scope authorization (rooms and exact
//     scope profiles are pinned by the ScopePolicy — authorization gates
//     integrity detail);
//  2. seal envelope identity and the segment digest binding
//     (segment_digest = SHA-256(canonical), canonical = JCS(segment));
//  3. terminal split — failed/aborted records verify their terminal audit and
//     are recorded as rejected/SEGMENT_NOT_SETTLED without any ref;
//  4. for settled records: frontier freeze, member ordering, payload digests,
//     DAG/causal completeness, terminal tools and ToolProxyResult digests,
//     Evidence/Path Seal body coherence, recomputed seal digests, derived
//     §7.5/§7.6 refs, the Host §7.7 evidence declaration and the sealed scope
//     profile binding.
//
// Valid evidence stays in validating (the only Commit-acceptable state);
// invalid evidence lands in a terminal rejected/inconclusive record carrying
// the closed reason code. Terminal states never reopen.
func (s *AdmissionService) Validate(stagedID string) (ValidationOutcome, error) {
	s.mu.Lock()
	rec, ok := s.staging[stagedID]
	if !ok {
		s.mu.Unlock()
		return ValidationOutcome{}, newAdmissionError(s.registry, ReasonEvidenceStagingInvalid, "unknown staged id %q", stagedID)
	}
	if !legalAdmissionTransition(rec.status, StatusValidating) {
		status := rec.status
		s.mu.Unlock()
		return ValidationOutcome{}, newAdmissionError(s.registry, ReasonIllegalStateTransition,
			"staged record %q is %s; validation requires the staged state (terminal states never reopen, GMS §2.2)", stagedID, status)
	}
	rec.status = StatusValidating
	rec.events = append(rec.events, StateEvent{From: StatusStaged, To: StatusValidating})
	s.mu.Unlock()

	outcome := s.runGates(rec)

	s.mu.Lock()
	rec.outcome = &outcome
	if !outcome.Valid {
		rec.status = outcome.Status
		rec.events = append(rec.events, StateEvent{
			From: StatusValidating, To: outcome.Status, ReasonCode: outcome.ReasonCode, Note: outcome.Detail,
		})
	}
	s.mu.Unlock()
	return outcome, nil
}

// runGates executes validateSubmission and converts any gate panic raised by
// the linear gate helpers into a rejected outcome (gates are total: a gate
// that does not fail simply falls through).
func (s *AdmissionService) runGates(rec *stagingState) (outcome ValidationOutcome) {
	defer func() {
		if recovered := recover(); recovered != nil {
			adm, isAdmission := recovered.(*AdmissionError)
			if !isAdmission {
				panic(recovered)
			}
			outcome = ValidationOutcome{StagedID: rec.id, Status: StatusRejected, ReasonCode: adm.ReasonCode, Detail: adm.Detail}
		}
	}()
	return s.validateSubmission(rec)
}

// validateSubmission executes the gates over the immutable staged record.
func (s *AdmissionService) validateSubmission(rec *stagingState) ValidationOutcome {
	reject := func(err error) ValidationOutcome {
		adm, ok := err.(*AdmissionError)
		if !ok {
			adm = &AdmissionError{ReasonCode: ReasonEvidenceStagingInvalid, Detail: err.Error()}
		}
		return ValidationOutcome{StagedID: rec.id, Status: StatusRejected, ReasonCode: adm.ReasonCode, Detail: adm.Detail}
	}
	terminal := func(status Status, code, detail string, audit map[string]any) ValidationOutcome {
		return ValidationOutcome{StagedID: rec.id, Status: status, ReasonCode: code, Detail: detail, TerminalAudit: audit}
	}

	seg := rec.segmentValue
	seal := rec.sealValue

	// Gate 1: provenance and declared scope.
	if err := s.scope.CheckDeclared(s.registry, rec.sub.Scope, stringField(seg, "room_id")); err != nil {
		return reject(err)
	}
	// Gate 2: seal envelope identity and digest binding.
	segmentDigest, err := s.checkSealBinding(seg, seal, rec.sub.Canonical)
	if err != nil {
		return reject(err)
	}
	// Gate 3: terminal split.
	state, _ := contract.AsString(seg["terminal_state"])
	if state != "settled" {
		audit, err := s.checkNonSettled(seg, seal)
		if err != nil {
			return reject(err)
		}
		reason, _ := contract.AsString(audit["reason_code"])
		return terminal(StatusRejected, ReasonSegmentNotSettled,
			fmt.Sprintf("segment terminal is %q (%s): no seals exist and no EvidenceRef may be minted", state, reason), audit)
	}
	// Gate 4: the settled close.
	candidate, inconclusive, err := s.validateSettledClose(rec, segmentDigest)
	if err != nil {
		return reject(err)
	}
	if err := s.scope.CheckSealed(s.registry, rec.sub.Scope, sealedScopeProfile(seg)); err != nil {
		return reject(err)
	}
	if inconclusive {
		return terminal(StatusInconclusive, ReasonMissingSealedPath,
			"settled close seals an incomplete path: the evidence cannot be trusted as complete (mark_inconclusive)", nil)
	}
	return ValidationOutcome{StagedID: rec.id, Status: StatusValidating, Valid: true, Candidate: candidate}
}

// checkSealBinding verifies the envelope identity and that the seal digest
// covers the canonical bytes byte-for-byte, and that those bytes are the JCS
// form of the segment record. It returns the recomputed segment digest.
func (s *AdmissionService) checkSealBinding(seg, seal map[string]any, canonical []byte) (string, error) {
	sealTerminal, _ := contract.AsString(seal["terminal_state"])
	segmentTerminal, _ := contract.AsString(seg["terminal_state"])
	if sealTerminal != segmentTerminal {
		return "", newAdmissionError(s.registry, ReasonTerminalMismatch,
			"seal terminal_state %q != segment terminal_state %q", sealTerminal, segmentTerminal)
	}
	rawLength, hasLength := seal["canonical_byte_length"].(json.Number)
	if !hasLength {
		return "", newAdmissionError(s.registry, ReasonSealDigestMismatch, "seal canonical_byte_length must be an integer")
	}
	declaredLength, err := strconv.Atoi(rawLength.String())
	if err != nil || declaredLength != len(canonical) {
		return "", newAdmissionError(s.registry, ReasonSealDigestMismatch,
			"seal canonical_byte_length %s != %d canonical bytes", rawLength.String(), len(canonical))
	}
	declaredDigest, _ := contract.AsString(seal["segment_digest"])
	if !digestShape.MatchString(declaredDigest) {
		return "", newAdmissionError(s.registry, ReasonSealDigestMismatch, "seal segment_digest %q is not sha256:<64 lowercase hex>", declaredDigest)
	}
	segmentDigest := contract.DigestBytes(canonical)
	if segmentDigest != declaredDigest {
		return "", newAdmissionError(s.registry, ReasonSealDigestMismatch,
			"seal segment_digest %s does not cover the canonical bytes (recomputed %s)", declaredDigest, segmentDigest)
	}
	recomputed, err := contract.JCS(seg)
	if err != nil {
		return "", newAdmissionError(s.registry, ReasonNonIntegerNumber, "segment record cannot enter the hashed core: %v", err)
	}
	if string(recomputed) != string(canonical) {
		return "", newAdmissionError(s.registry, ReasonCanonicalizationFailed,
			"canonical bytes are not the RFC 8785 JCS form of the segment record")
	}
	return segmentDigest, nil
}

// checkNonSettled verifies a failed/aborted record: such terminals carry the
// audit and MUST NOT carry any refs or seal bodies (Host 3.8). It returns the
// frozen terminal audit.
func (s *AdmissionService) checkNonSettled(seg, seal map[string]any) (map[string]any, error) {
	for _, forbidden := range []string{"segment_ref", "checkpoint_ref", "evidence_seal", "path_seal"} {
		if _, present := seal[forbidden]; present {
			return nil, newAdmissionError(s.registry, ReasonNonsettledSegmentHasRefs, "seal envelope carries %q on a non-settled segment", forbidden)
		}
	}
	for _, forbidden := range []string{"evidence_seal_body", "path_seal_body", "checkpoint_identity"} {
		if _, present := seg[forbidden]; present {
			return nil, newAdmissionError(s.registry, ReasonNonsettledSegmentHasRefs, "segment record carries %q on a non-settled segment", forbidden)
		}
	}
	segAudit, okSeg := contract.AsObject(seg["terminal_audit"])
	sealAudit, okSeal := contract.AsObject(seal["terminal_audit"])
	if !okSeg || !okSeal || !contract.EqualJSON(segAudit, sealAudit) {
		return nil, newAdmissionError(s.registry, ReasonTerminalAuditMismatch, "terminal audit missing or divergent between record and seal")
	}
	return segAudit, nil
}

// validateSettledClose runs the full settled-close gate suite and assembles
// the candidate. inconclusive=true marks otherwise-valid evidence whose
// sealed paths are incomplete.
func (s *AdmissionService) validateSettledClose(rec *stagingState, segmentDigest string) (*EvidenceCandidate, bool, error) {
	seg := rec.segmentValue
	seal := rec.sealValue

	for _, required := range []string{"evidence_seal_body", "path_seal_body", "checkpoint_identity"} {
		if _, ok := contract.AsObject(seg[required]); !ok {
			return nil, false, newAdmissionError(s.registry, ReasonSchemaRequiredMissing, "settled segment missing %s", required)
		}
	}
	if _, present := seg["terminal_audit"]; present {
		return nil, false, newAdmissionError(s.registry, ReasonTerminalMismatch, "settled segment carries a terminal_audit")
	}
	for _, required := range []string{"evidence_seal", "path_seal", "segment_ref", "checkpoint_ref"} {
		if _, ok := contract.AsObject(seal[required]); !ok {
			return nil, false, newAdmissionError(s.registry, ReasonSchemaRequiredMissing, "seal envelope missing %s", required)
		}
	}

	members := s.checkMembersAndFrontier(seg)
	links, memberKinds := s.checkDag(seg, members)
	s.checkTerminalTools(seg, members, memberKinds)
	evidenceBody := s.checkEvidenceSealBody(seg, members)
	pathIDs, domains, incomplete := s.checkPathSealBody(seg, memberKinds, links)
	derivedSegRef, err := deriveSegmentRefJSON(seg, s.registry, segmentDigest)
	if err != nil {
		return nil, false, err
	}
	s.checkSealEnvelopeBlocks(seg, seal, derivedSegRef, evidenceBody, pathIDs, domains)
	checkpointDigest, checkpointRef := s.checkCheckpoint(seg, seal, derivedSegRef, segmentDigest)

	segmentID, _ := contract.AsString(seg["segment_id"])
	roomID, _ := contract.AsString(seg["room_id"])
	candidate := &EvidenceCandidate{
		EvidenceID:        "evseal-" + segmentID,
		Version:           "1",
		EvidenceKind:      primaryEvidenceKind(domains),
		EvidenceDigest:    digestOfValue(s.registry, evidenceBody),
		EvidenceSealID:    "evseal-" + segmentID,
		PathSealID:        "pathseal-" + segmentID,
		PathSealDigest:    digestOfValue(s.registry, seg["path_seal_body"]),
		SegmentDigest:     segmentDigest,
		RoomID:            roomID,
		SegmentID:         segmentID,
		SegmentVersion:    stringVersion(seg["segment_version"]),
		SegmentRefJSON:    derivedSegRef,
		CheckpointRefJSON: checkpointRef,
		CheckpointDigest:  checkpointDigest,
		PathIDs:           pathIDs,
		PathDomains:       sortedStrings(domainsList(domains)),
		CanonicalSegment:  append([]byte(nil), rec.sub.Canonical...),
	}
	return candidate, incomplete, nil
}

// checkMembersAndFrontier enforces the frozen frontier: member sequences live
// strictly inside (start, end], increase strictly, end at the frontier, ticks
// are strictly monotone, ids unique, kinds closed and payload digests cover
// the exact payloads (validate_recorded.py _validate_settled_close).
func (s *AdmissionService) checkMembersAndFrontier(seg map[string]any) []map[string]any {
	rawMembers, ok := contract.AsArray(seg["members"])
	if !ok || len(rawMembers) == 0 {
		fail(s.registry, ReasonSchemaRequiredMissing, "members must be a non-empty array")
	}
	frontier, ok := contract.AsObject(seg["frontier"])
	if !ok {
		fail(s.registry, ReasonSchemaRequiredMissing, "frontier is required")
	}
	start, okStart := intField(frontier, "start_room_sequence")
	end, okEnd := intField(frontier, "end_room_sequence")
	if !okStart || !okEnd {
		fail(s.registry, ReasonSchemaEnumInvalid, "frontier bounds must be integers")
	}
	members := make([]map[string]any, 0, len(rawMembers))
	ids := make(map[string]bool, len(rawMembers))
	var lastSeq, lastTick int64
	for index, raw := range rawMembers {
		member, isObj := contract.AsObject(raw)
		if !isObj {
			fail(s.registry, ReasonSchemaEnumInvalid, "members[%d] must be an object", index)
		}
		id, _ := contract.AsString(member["event_id"])
		if id == "" || ids[id] {
			fail(s.registry, ReasonRefMismatch, "members[%d]: event_id empty or duplicate", index)
		}
		ids[id] = true
		kind, _ := contract.AsString(member["event_kind"])
		if !eventKinds[kind] {
			fail(s.registry, ReasonSchemaEnumInvalid, "members[%d]: unknown event_kind %q", index, kind)
		}
		seq, okSeq := intField(member, "room_sequence")
		tick, okTick := intField(member, "logical_tick")
		if !okSeq || !okTick {
			fail(s.registry, ReasonSchemaEnumInvalid, "members[%d]: room_sequence/logical_tick must be integers", index)
		}
		if !(start < seq && seq <= end) {
			fail(s.registry, ReasonAppendAfterSettled, "member %s at room_sequence %d outside the frozen frontier (%d, %d]", id, seq, start, end)
		}
		if index > 0 {
			if seq <= lastSeq {
				fail(s.registry, ReasonMemberOrderInvalid, "member %s room_sequence %d does not strictly increase", id, seq)
			}
			if tick <= lastTick {
				fail(s.registry, ReasonMemberOrderInvalid, "member %s logical_tick %d does not strictly increase", id, tick)
			}
		}
		lastSeq, lastTick = seq, tick
		declaredPayload, _ := contract.AsString(member["payload_digest"])
		if !digestShape.MatchString(declaredPayload) {
			fail(s.registry, ReasonPayloadDigestMismatch, "member %s payload_digest malformed", id)
		}
		if digestOfValue(s.registry, member["payload"]) != declaredPayload {
			fail(s.registry, ReasonPayloadDigestMismatch, "member %s payload_digest does not cover its payload", id)
		}
		members = append(members, member)
	}
	if lastSeq != end {
		fail(s.registry, ReasonFrontierMismatch, "frontier end %d != last member room_sequence %d", end, lastSeq)
	}
	return members
}

// checkDag enforces causal completeness and acyclicity with the per-kind
// requirements of validate_recorded.py _validate_dag. It returns the parsed
// links and the member id → event_kind index.
func (s *AdmissionService) checkDag(seg map[string]any, members []map[string]any) ([]map[string]any, map[string]string) {
	rawLinks, _ := contract.AsArray(seg["causal_links"])
	links := make([]map[string]any, 0, len(rawLinks))
	linkIDs := make(map[string]bool, len(rawLinks))
	memberKinds := make(map[string]string, len(members))
	for _, member := range members {
		memberKinds[stringField(member, "event_id")] = stringField(member, "event_kind")
	}
	type edge struct{ from, to, kind string }
	edges := make([]edge, 0, len(rawLinks))
	for index, raw := range rawLinks {
		link, isObj := contract.AsObject(raw)
		if !isObj {
			fail(s.registry, ReasonSchemaEnumInvalid, "causal_links[%d] must be an object", index)
		}
		id, _ := contract.AsString(link["link_id"])
		if id == "" || linkIDs[id] {
			fail(s.registry, ReasonRefMismatch, "causal_links[%d]: link_id empty or duplicate", index)
		}
		linkIDs[id] = true
		kind, _ := contract.AsString(link["link_kind"])
		if !linkKinds[kind] {
			fail(s.registry, ReasonSchemaEnumInvalid, "causal_links[%d]: unknown link_kind %q", index, kind)
		}
		from, _ := contract.AsString(link["from_event"])
		to, _ := contract.AsString(link["to_event"])
		if _, known := memberKinds[from]; !known {
			fail(s.registry, ReasonMissingCausalLink, "link %s endpoint %q outside the frozen membership", id, from)
		}
		if _, known := memberKinds[to]; !known {
			fail(s.registry, ReasonMissingCausalLink, "link %s endpoint %q outside the frozen membership", id, to)
		}
		edges = append(edges, edge{from: from, to: to, kind: kind})
		links = append(links, link)
	}
	// The room_sequence chain must connect consecutive members.
	chain := make(map[string]bool, len(edges))
	for _, e := range edges {
		if e.kind == "room_sequence" {
			chain[e.from+"\x1f"+e.to] = true
		}
	}
	for i := 1; i < len(members); i++ {
		left := stringField(members[i-1], "event_id")
		right := stringField(members[i], "event_id")
		if !chain[left+"\x1f"+right] {
			fail(s.registry, ReasonMissingCausalLink, "room_sequence gap %s -> %s", left, right)
		}
	}
	outgoing := map[string]map[string]int{}
	incoming := map[string]map[string]int{}
	count := func(table map[string]map[string]int, event, kind string) int { return table[event][kind] }
	bump := func(table map[string]map[string]int, event, kind string) {
		if table[event] == nil {
			table[event] = map[string]int{}
		}
		table[event][kind]++
	}
	adjacency := map[string][]string{}
	nodes := make(map[string]bool, len(memberKinds))
	for id := range memberKinds {
		nodes[id] = true
	}
	for _, e := range edges {
		bump(outgoing, e.from, e.kind)
		bump(incoming, e.to, e.kind)
		adjacency[e.from] = append(adjacency[e.from], e.to)
	}
	for _, member := range members {
		id := stringField(member, "event_id")
		switch memberKinds[id] {
		case "model_response", "decision_checkpoint":
			if count(incoming, id, "response_to") < 1 {
				fail(s.registry, ReasonMissingCausalLink, "%s (%s) lacks response_to", id, memberKinds[id])
			}
		case "tool_call":
			if count(outgoing, id, "tool_call_to_result") != 1 {
				fail(s.registry, ReasonMissingCausalLink, "tool_call %s must have exactly one tool result", id)
			}
		case "tool_result":
			if count(incoming, id, "tool_call_to_result") != 1 {
				fail(s.registry, ReasonMissingCausalLink, "tool_result %s lacks tool_call_to_result", id)
			}
		case "delivery_transition":
			if count(incoming, id, "delivery_of") != 1 {
				fail(s.registry, ReasonMissingCausalLink, "delivery_transition %s lacks delivery_of", id)
			}
		case "replay_dispatch":
			if count(outgoing, id, "replay_of") < 1 {
				fail(s.registry, ReasonMissingCausalLink, "replay_dispatch %s lacks replay_of", id)
			}
		case "replay_completion":
			if count(incoming, id, "replay_of") != 1 {
				fail(s.registry, ReasonMissingCausalLink, "replay_completion %s lacks replay_of", id)
			}
		}
	}
	if hasCycle(nodes, adjacency) {
		fail(s.registry, ReasonDAGCycle, "causal graph contains a cycle")
	}
	return links, memberKinds
}

// checkTerminalTools enforces terminal deliveries, exact ToolProxyResult
// digests and the one-execution-per-tool-call invariant.
func (s *AdmissionService) checkTerminalTools(seg map[string]any, members []map[string]any, memberKinds map[string]string) {
	rawDeliveries, _ := contract.AsArray(seg["deliveries"])
	deliveries := make(map[string]bool, len(rawDeliveries))
	for index, raw := range rawDeliveries {
		delivery, isObj := contract.AsObject(raw)
		if !isObj {
			fail(s.registry, ReasonSchemaEnumInvalid, "deliveries[%d] must be an object", index)
		}
		id, _ := contract.AsString(delivery["delivery_id"])
		if id == "" {
			fail(s.registry, ReasonSchemaRequiredMissing, "deliveries[%d].delivery_id", index)
		}
		if deliveries[id] {
			fail(s.registry, ReasonRefMismatch, "duplicate delivery id %s", id)
		}
		state, _ := contract.AsString(delivery["terminal_state"])
		if !terminalDeliveryStates[state] {
			fail(s.registry, ReasonNonterminalTool, "delivery %s is %q at close", id, state)
		}
		deliveries[id] = true
	}
	rawExecutions, _ := contract.AsArray(seg["tool_executions"])
	toolCalls := make([]string, 0)
	for _, member := range members {
		if kind, _ := contract.AsString(member["event_kind"]); kind == "tool_call" {
			toolCalls = append(toolCalls, stringField(member, "event_id"))
		}
	}
	executedCalls := make([]string, 0, len(rawExecutions))
	for index, raw := range rawExecutions {
		execution, isObj := contract.AsObject(raw)
		if !isObj {
			fail(s.registry, ReasonSchemaEnumInvalid, "tool_executions[%d] must be an object", index)
		}
		callID, _ := contract.AsString(execution["tool_call_event"])
		resultID, _ := contract.AsString(execution["tool_result_event"])
		if memberKinds[callID] != "tool_call" {
			fail(s.registry, ReasonRefMismatch, "tool_executions[%d]: tool_call_event %q is not a tool_call member", index, callID)
		}
		if memberKinds[resultID] != "tool_result" {
			fail(s.registry, ReasonRefMismatch, "tool_executions[%d]: tool_result_event %q is not a tool_result member", index, resultID)
		}
		deliveryID, _ := contract.AsString(execution["delivery_id"])
		if !deliveries[deliveryID] {
			fail(s.registry, ReasonRefMismatch, "tool_executions[%d]: unknown delivery %q", index, deliveryID)
		}
		s.checkToolProxyResult(execution)
		executedCalls = append(executedCalls, callID)
	}
	sort.Strings(toolCalls)
	sort.Strings(executedCalls)
	if !equalSets(toolCalls, executedCalls) {
		fail(s.registry, ReasonRefMismatch, "every tool_call must have exactly one execution record")
	}
}

func (s *AdmissionService) checkToolProxyResult(execution map[string]any) {
	tpr, ok := contract.AsObject(execution["tool_proxy_result"])
	if !ok {
		fail(s.registry, ReasonToolProxyDigestMismatch, "tool execution lacks a ToolProxyResult")
	}
	if sv, _ := contract.AsString(tpr["schema_version"]); sv != "host.tool-proxy-result.v1" {
		fail(s.registry, ReasonToolProxyDigestMismatch, "ToolProxyResult schema_version %q", sv)
	}
	declared, _ := contract.AsString(tpr["proxy_result_digest"])
	if !digestShape.MatchString(declared) {
		fail(s.registry, ReasonToolProxyDigestMismatch, "proxy_result_digest malformed")
	}
	preimage := make(map[string]any, len(tpr))
	for key, value := range tpr {
		if key != "proxy_result_digest" {
			preimage[key] = value
		}
	}
	if digestOfValue(s.registry, preimage) != declared {
		fail(s.registry, ReasonToolProxyDigestMismatch, "proxy_result_digest does not cover the result preimage")
	}
	status, _ := contract.AsString(tpr["status"])
	upstream, _ := contract.AsString(tpr["upstream_result_digest"])
	if status == "succeeded" {
		result, isObj := contract.AsObject(tpr["result"])
		if !isObj {
			fail(s.registry, ReasonToolProxyDigestMismatch, "succeeded ToolProxyResult lacks a result object")
		}
		if digestOfValue(s.registry, result) != upstream {
			fail(s.registry, ReasonToolProxyDigestMismatch, "upstream_result_digest does not cover the exact upstream payload")
		}
		return
	}
	if status != "failed" && status != "inconclusive" {
		fail(s.registry, ReasonSchemaEnumInvalid, "ToolProxyResult status %q", status)
	}
	errObj, isObj := contract.AsObject(tpr["error"])
	if !isObj {
		fail(s.registry, ReasonToolProxyDigestMismatch, "non-success ToolProxyResult lacks an error object")
	}
	for _, field := range []string{"reason_code", "message", "retryable"} {
		if _, present := errObj[field]; !present {
			fail(s.registry, ReasonToolProxyDigestMismatch, "error.%s missing", field)
		}
	}
	attempt, isObj := contract.AsObject(execution["upstream_attempt_record"])
	if !isObj {
		fail(s.registry, ReasonToolProxyDigestMismatch, "failed result needs an upstream attempt record")
	}
	attemptDigest := digestOfValue(s.registry, attempt)
	if attemptDigest != upstream {
		fail(s.registry, ReasonToolProxyDigestMismatch, "upstream_result_digest does not cover the upstream attempt record")
	}
	refs, isArr := contract.AsArray(errObj["record_refs"])
	if !isArr || len(refs) != 1 {
		fail(s.registry, ReasonToolProxyDigestMismatch, "error.record_refs must hold exactly one ref")
	}
	ref, isObj := contract.AsObject(refs[0])
	if !isObj {
		fail(s.registry, ReasonToolProxyDigestMismatch, "error.record_refs[0] must be an object")
	}
	refID, _ := contract.AsString(ref["id"])
	attemptID, _ := contract.AsString(attempt["attempt_id"])
	refDigest, _ := contract.AsString(ref["digest"])
	if refID != attemptID || refDigest != attemptDigest {
		fail(s.registry, ReasonToolProxyDigestMismatch, "error.record_refs must reference the attempt record")
	}
}

// checkEvidenceSealBody enforces that the Evidence Seal body mirrors the
// frozen record exactly (validate_recorded.py EVIDENCE_SEAL_BODY_MISMATCH
// group) and returns the verified body.
func (s *AdmissionService) checkEvidenceSealBody(seg map[string]any, members []map[string]any) map[string]any {
	body, ok := contract.AsObject(seg["evidence_seal_body"])
	if !ok {
		fail(s.registry, ReasonSchemaRequiredMissing, "evidence_seal_body is required on a settled segment")
	}
	if sv, _ := contract.AsString(body["schema_version"]); sv != "host.evidence-seal.v1" {
		fail(s.registry, ReasonSchemaVersionUnsupported, "evidence seal schema_version %q", sv)
	}
	for _, field := range []string{"room_id", "segment_id", "segment_version"} {
		if !contract.EqualJSON(body[field], seg[field]) {
			fail(s.registry, ReasonRefMismatch, "evidence seal body %s does not match the segment identity", field)
		}
	}
	ordered := make([]any, 0, len(members))
	for _, member := range members {
		ordered = append(ordered, map[string]any{
			"event_id":       member["event_id"],
			"room_sequence":  member["room_sequence"],
			"payload_digest": member["payload_digest"],
		})
	}
	if !contract.EqualJSON(body["ordered_event_refs"], ordered) {
		fail(s.registry, ReasonEvidenceSealBodyMismatch, "ordered event refs must match the frozen membership")
	}
	if !contract.EqualJSON(body["sequence_interval"], seg["frontier"]) {
		fail(s.registry, ReasonEvidenceSealBodyMismatch, "sequence interval must equal the frontier")
	}
	if declared, _ := contract.AsString(body["causal_link_set_digest"]); declared != digestOfValue(s.registry, seg["causal_links"]) {
		fail(s.registry, ReasonEvidenceSealBodyMismatch, "causal link set digest")
	}
	rawDeliveries, _ := contract.AsArray(seg["deliveries"])
	mirroredDeliveries := make([]any, 0, len(rawDeliveries))
	for _, raw := range rawDeliveries {
		delivery, _ := contract.AsObject(raw)
		mirroredDeliveries = append(mirroredDeliveries, map[string]any{
			"delivery_id":    delivery["delivery_id"],
			"terminal_state": delivery["terminal_state"],
		})
	}
	if !contract.EqualJSON(body["deliveries"], mirroredDeliveries) {
		fail(s.registry, ReasonEvidenceSealBodyMismatch, "delivery identities and terminal states")
	}
	rawExecutions, _ := contract.AsArray(seg["tool_executions"])
	correlations := make([]any, 0, len(rawExecutions))
	for _, raw := range rawExecutions {
		execution, _ := contract.AsObject(raw)
		tpr, _ := contract.AsObject(execution["tool_proxy_result"])
		correlations = append(correlations, map[string]any{
			"tool_call_event":          execution["tool_call_event"],
			"tool_result_event":        execution["tool_result_event"],
			"tool_proxy_result_digest": tpr["proxy_result_digest"],
		})
	}
	if !contract.EqualJSON(body["tool_correlations"], correlations) {
		fail(s.registry, ReasonEvidenceSealBodyMismatch, "tool call/result correlation")
	}
	rawActors, isArr := contract.AsArray(body["actor_ids"])
	if !isArr {
		fail(s.registry, ReasonEvidenceSealBodyMismatch, "actor_ids must be an array")
	}
	actors := make([]string, 0, len(rawActors))
	for _, raw := range rawActors {
		actor, _ := contract.AsString(raw)
		actors = append(actors, actor)
	}
	memberActors := make([]string, 0, len(members))
	for _, member := range members {
		memberActors = append(memberActors, stringField(member, "actor_id"))
	}
	if !equalSets(sortedStrings(actors), sortedStrings(unique(memberActors))) {
		fail(s.registry, ReasonEvidenceSealBodyMismatch, "actor identities")
	}
	if _, err := contract.ParseVersionedRef(mustObject(s.registry, body, "scope_profile_ref")); err != nil {
		fail(s.registry, ReasonSchemaEnumInvalid, "scope_profile_ref is not an exact VersionedRef")
	}
	return body
}

// checkPathSealBody validates the sealed paths and returns (ordered path ids,
// domains, incomplete-flag).
func (s *AdmissionService) checkPathSealBody(seg map[string]any, memberKinds map[string]string, links []map[string]any) ([]string, map[string]bool, bool) {
	body, ok := contract.AsObject(seg["path_seal_body"])
	if !ok {
		fail(s.registry, ReasonSchemaRequiredMissing, "path_seal_body is required on a settled segment")
	}
	if sv, _ := contract.AsString(body["schema_version"]); sv != "host.path-seal.v1" {
		fail(s.registry, ReasonSchemaVersionUnsupported, "path seal schema_version %q", sv)
	}
	for _, field := range []string{"room_id", "segment_id", "segment_version"} {
		if !contract.EqualJSON(body[field], seg[field]) {
			fail(s.registry, ReasonRefMismatch, "path seal body %s does not match the segment identity", field)
		}
	}
	rawPaths, isArr := contract.AsArray(body["paths"])
	if !isArr || len(rawPaths) == 0 {
		fail(s.registry, ReasonMissingSealedPath, "settled close must seal at least one path")
	}
	linkIDs := make(map[string]bool, len(links))
	for _, link := range links {
		linkIDs[stringField(link, "link_id")] = true
	}
	identity, _ := contract.AsObject(seg["checkpoint_identity"])
	checkpointID, _ := contract.AsString(identity["checkpoint_id"])
	pathIDs := make([]string, 0, len(rawPaths))
	domains := make(map[string]bool)
	incomplete := false
	for index, raw := range rawPaths {
		path, isObj := contract.AsObject(raw)
		if !isObj {
			fail(s.registry, ReasonSchemaEnumInvalid, "paths[%d] must be an object", index)
		}
		pathID, _ := contract.AsString(path["path_id"])
		if pathID == "" {
			fail(s.registry, ReasonSchemaRequiredMissing, "paths[%d].path_id", index)
		}
		domain, _ := contract.AsString(path["domain"])
		if !pathDomains[domain] {
			fail(s.registry, ReasonSchemaEnumInvalid, "paths[%d]: unknown domain %q", index, domain)
		}
		events, isArr := contract.AsArray(path["ordered_event_refs"])
		if !isArr || len(events) == 0 {
			fail(s.registry, ReasonMissingSealedPath, "path %s must reference events", pathID)
		}
		for _, rawEvent := range events {
			eventID, _ := contract.AsString(rawEvent)
			if _, known := memberKinds[eventID]; !known {
				fail(s.registry, ReasonMissingSealedPath, "path %s references unknown event %q", pathID, eventID)
			}
		}
		if refs, isArr := contract.AsArray(path["causal_link_refs"]); isArr {
			for _, rawRef := range refs {
				linkID, _ := contract.AsString(rawRef)
				if !linkIDs[linkID] {
					fail(s.registry, ReasonMissingSealedPath, "path %s references unknown link %q", pathID, linkID)
				}
			}
		}
		if refID, _ := contract.AsString(path["checkpoint_ref_id"]); refID != checkpointID {
			fail(s.registry, ReasonRefMismatch, "path %s checkpoint correlation", pathID)
		}
		completeness, _ := contract.AsString(path["completeness"])
		if completeness != "complete" && completeness != "incomplete" {
			fail(s.registry, ReasonSchemaEnumInvalid, "path %s completeness %q", pathID, completeness)
		}
		if completeness != "complete" {
			incomplete = true
		}
		pathIDs = append(pathIDs, pathID)
		domains[domain] = true
	}
	return pathIDs, domains, incomplete
}

// checkSealEnvelopeBlocks recomputes both seal digests and verifies the
// derived §7.5 SegmentRef plus the Host §7.7 evidence declaration the seal
// envelope carries.
func (s *AdmissionService) checkSealEnvelopeBlocks(seg, seal, derivedSegRef, evidenceBody map[string]any, pathIDs []string, domains map[string]bool) {
	segmentID, _ := contract.AsString(seg["segment_id"])
	evidenceDigest := digestOfValue(s.registry, evidenceBody)
	pathDigest := digestOfValue(s.registry, seg["path_seal_body"])

	evSeal := mustObject(s.registry, seal, "evidence_seal")
	if id, _ := contract.AsString(evSeal["seal_id"]); id != "evseal-"+segmentID {
		fail(s.registry, ReasonRefMismatch, "evidence seal id %q does not follow the Host protocol", id)
	}
	if v := stringVersion(evSeal["seal_version"]); v != "1" {
		fail(s.registry, ReasonRefMismatch, "evidence seal version %q", v)
	}
	if declared, _ := contract.AsString(evSeal["seal_digest"]); declared != evidenceDigest {
		fail(s.registry, ReasonEvidenceSealDigestMismatch, "recomputed evidence seal digest %s != declared %s", evidenceDigest, declared)
	}
	evRef := mustObject(s.registry, evSeal, "evidence_ref")
	if sv, _ := contract.AsString(evRef["schema_version"]); sv != schemaEvidenceRef {
		fail(s.registry, ReasonSchemaVersionUnsupported, "sealed evidence_ref schema_version %q", sv)
	}
	if id, _ := contract.AsString(evRef["evidence_id"]); id != "evseal-"+segmentID {
		fail(s.registry, ReasonRefMismatch, "sealed evidence_ref id %q", id)
	}
	if v := stringVersion(evRef["version"]); v != "1" {
		fail(s.registry, ReasonRefMismatch, "sealed evidence_ref version %q", v)
	}
	if declared, _ := contract.AsString(evRef["evidence_digest"]); declared != evidenceDigest {
		fail(s.registry, ReasonEvidenceSealDigestMismatch, "sealed evidence_ref digest %q != recomputed %s", declared, evidenceDigest)
	}
	if state, _ := contract.AsString(evRef["commit_state"]); state != "sealed" {
		fail(s.registry, ReasonEvidenceSealInvalid, "Host evidence declaration commit_state %q must be sealed", state)
	}
	if kind, _ := contract.AsString(evRef["evidence_kind"]); kind != primaryEvidenceKind(domains) {
		fail(s.registry, ReasonEvidenceSealInvalid,
			"sealed evidence_kind %q disagrees with the sealed path domains (%s)", kind, primaryEvidenceKind(domains))
	}
	if !contract.EqualJSON(evRef["source_segment_ref"], derivedSegRef) {
		fail(s.registry, ReasonRefMismatch, "sealed evidence_ref source_segment_ref")
	}

	ptSeal := mustObject(s.registry, seal, "path_seal")
	if id, _ := contract.AsString(ptSeal["seal_id"]); id != "pathseal-"+segmentID {
		fail(s.registry, ReasonRefMismatch, "path seal id %q does not follow the Host protocol", id)
	}
	if v := stringVersion(ptSeal["seal_version"]); v != "1" {
		fail(s.registry, ReasonRefMismatch, "path seal version %q", v)
	}
	if declared, _ := contract.AsString(ptSeal["seal_digest"]); declared != pathDigest {
		fail(s.registry, ReasonPathSealDigestMismatch, "recomputed path seal digest %s != declared %s", pathDigest, declared)
	}
	declaredIDs := make([]string, 0)
	if rawIDs, isArr := contract.AsArray(ptSeal["path_ids"]); isArr {
		for _, raw := range rawIDs {
			id, _ := contract.AsString(raw)
			declaredIDs = append(declaredIDs, id)
		}
	}
	if !equalSets(declaredIDs, pathIDs) {
		fail(s.registry, ReasonRefMismatch, "path seal path_ids must list exactly the sealed paths")
	}

	if !contract.EqualJSON(seal["segment_ref"], derivedSegRef) {
		fail(s.registry, ReasonRefMismatch, "seal segment_ref differs from the derived SegmentRef")
	}
}

// checkCheckpoint recomputes the checkpoint digest over the FND-002 preimage
// and returns (digest, derived §7.6 ref JSON).
func (s *AdmissionService) checkCheckpoint(seg, seal, derivedSegRef map[string]any, segmentDigest string) (string, map[string]any) {
	identity := mustObject(s.registry, seg, "checkpoint_identity")
	preimage, err := checkpointPreimageJSON(seg, segmentDigest)
	if err != nil {
		fail(s.registry, ReasonSchemaRequiredMissing, "checkpoint identity: %v", err)
	}
	digest := digestOfValue(s.registry, preimage)
	declaredRef := mustObject(s.registry, seal, "checkpoint_ref")
	if declared, _ := contract.AsString(declaredRef["checkpoint_digest"]); declared != digest {
		fail(s.registry, ReasonCheckpointNotSealed,
			"checkpoint digest %s does not cover the recomputed preimage %s", declared, digest)
	}
	derived := map[string]any{
		"schema_version":      schemaCheckpointRef,
		"room_id":             seg["room_id"],
		"segment_ref":         derivedSegRef,
		"checkpoint_id":       identity["checkpoint_id"],
		"checkpoint_sequence": identity["checkpoint_sequence"],
		"checkpoint_digest":   digest,
	}
	if !contract.EqualJSON(declaredRef, derived) {
		fail(s.registry, ReasonRefMismatch, "checkpoint_ref differs from the derived CheckpointRef")
	}
	if _, err := ParseCheckpointRef(s.registry, declaredRef); err != nil {
		fail(s.registry, ReasonRefMismatch, "checkpoint_ref is not an exact ref: %v", err)
	}
	return digest, derived
}

// ---------------------------------------------------------------------------
// Commit: the atomic evidence transaction (Contract §13.1, GMS §2.2/§2.9)
// ---------------------------------------------------------------------------

// Commit mints the EvidenceRef of a validated submission in ONE ledger
// transaction: content-store put of the canonical evidence body, evidence
// ledger append of the exact §7.7 ref (commit_state=committed) and the
// idempotency record — all or nothing. Re-committing the same sealed input
// returns the identical EvidenceRef without a second append; the same
// idempotency key with a different digest conflicts and leaves zero partial
// writes. Committing staged/rejected/inconclusive evidence fails closed.
func (s *AdmissionService) Commit(ctx context.Context, stagedID string) (CommittedEvidence, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.mu.Lock()
	rec, ok := s.staging[stagedID]
	if !ok {
		s.mu.Unlock()
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonEvidenceStagingInvalid, "unknown staged id %q", stagedID)
	}
	var candidate *EvidenceCandidate
	switch rec.status {
	case StatusCommitted:
		s.mu.Unlock()
		return s.replayCommitted(rec)
	case StatusValidating:
		if rec.outcome == nil || !rec.outcome.Valid || rec.outcome.Candidate == nil {
			status := rec.status
			s.mu.Unlock()
			return CommittedEvidence{}, newAdmissionError(s.registry, ReasonIllegalStateTransition,
				"staged record %q in state %s has no valid candidate", stagedID, status)
		}
		candidate = rec.outcome.Candidate
	default:
		status := rec.status
		s.mu.Unlock()
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonIllegalStateTransition,
			"staged record %q is %s; only validated evidence may commit (staged/rejected/inconclusive evidence never mints a ref, GMS §2.2)", stagedID, status)
	}
	submitter := rec.sub.Submitter
	idempotencyKey := rec.sub.IdempotencyKey
	s.mu.Unlock()

	key := resolvedIdempotencyKey(idempotencyKey, candidate)
	requestDigest := s.requestDigest(candidate)
	refBytes, err := mintEvidenceRef(s.registry, candidate)
	if err != nil {
		return CommittedEvidence{}, err
	}
	refDigest := contract.DigestBytes(refBytes)

	// Cross-key dedup: the evidence ledger is the authority — the same sealed
	// evidence never appends twice, whatever mutation key carried it in.
	if found, ok, err := s.findEvidenceEntry(candidate.EvidenceID); err != nil {
		return CommittedEvidence{}, err
	} else if ok {
		if found.PayloadDigest != refDigest {
			return CommittedEvidence{}, newAdmissionError(s.registry, ReasonDigestMismatch,
				"evidence %s already committed with a different payload digest", candidate.EvidenceID)
		}
		if err := s.bindIdempotencyKey(ctx, key, requestDigest, refBytes); err != nil {
			return CommittedEvidence{}, err
		}
		committed, err := s.committedFromLedger(found, candidate, submitter, stagedID)
		if err != nil {
			return CommittedEvidence{}, err
		}
		s.recordCommit(rec, key, requestDigest, committed)
		return committed, nil
	}

	var outcome []byte
	var sequence uint64
	err = s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		replayed, recorded, err := tx.RecordIdempotency(key, requestDigest, refBytes)
		if err != nil {
			return err
		}
		if replayed {
			outcome = recorded
			return nil // a retry MUST NOT stage new writes (Contract §13.2)
		}
		if _, err := tx.PutContent(candidate.CanonicalSegment); err != nil {
			return err
		}
		entry, err := tx.AppendEvent(ledger.LedgerEvidence, "", candidate.EvidenceID, refBytes)
		if err != nil {
			return err
		}
		outcome = refBytes
		sequence = entry.Sequence
		return nil
	})
	if err != nil {
		return CommittedEvidence{}, err
	}
	if len(outcome) == 0 {
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonEvidenceStagingInvalid, "commit produced no EvidenceRef outcome")
	}

	committed, err := buildCommittedEvidence(s.registry, candidate, outcome, submitter, stagedID, sequence)
	if err != nil {
		return CommittedEvidence{}, err
	}
	s.recordCommit(rec, key, requestDigest, committed)
	return committed, nil
}

// replayCommitted returns the recorded outcome of an already-committed
// submission (stable idempotent commit; no new authority writes).
func (s *AdmissionService) replayCommitted(rec *stagingState) (CommittedEvidence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	recorded, ok := s.provenance[rec.committedID]
	if !ok {
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonEvidenceStagingInvalid,
			"committed record %q has no provenance index entry", rec.id)
	}
	return copyEvidence(recorded.evidence), nil
}

// bindIdempotencyKey records (key → request digest → outcome) without any
// authority write, so a later different digest under the same key conflicts.
func (s *AdmissionService) bindIdempotencyKey(ctx context.Context, key, requestDigest string, outcome []byte) error {
	return s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		_, _, err := tx.RecordIdempotency(key, requestDigest, outcome)
		return err
	})
}

func (s *AdmissionService) findEvidenceEntry(evidenceID string) (ledger.Entry, bool, error) {
	entries, err := s.store.Snapshot(ledger.LedgerEvidence, "")
	if err != nil {
		return ledger.Entry{}, false, err
	}
	for _, entry := range entries {
		if entry.EventID == evidenceID {
			return entry, true, nil
		}
	}
	return ledger.Entry{}, false, nil
}

// committedFromLedger rebuilds the committed view of an existing ledger entry
// (cross-key dedup path).
func (s *AdmissionService) committedFromLedger(entry ledger.Entry, candidate *EvidenceCandidate, submitter, stagedID string) (CommittedEvidence, error) {
	payload, ok, err := s.store.Get(entry.PayloadDigest)
	if err != nil || !ok {
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonEvidenceStagingInvalid,
			"evidence ledger entry %s payload unresolved", entry.EventID)
	}
	return buildCommittedEvidence(s.registry, candidate, payload, submitter, stagedID, entry.Sequence)
}

func (s *AdmissionService) recordCommit(rec *stagingState, key, requestDigest string, committed CommittedEvidence) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.status = StatusCommitted
	rec.committedID = committed.EvidenceID
	rec.events = append(rec.events, StateEvent{
		From: StatusValidating, To: StatusCommitted,
		Note: fmt.Sprintf("EvidenceRef %s committed at evidence ledger sequence %d", committed.EvidenceID, committed.CommitSequence),
	})
	s.provenance[committed.EvidenceID] = &commitProvenance{
		key:           key,
		requestDigest: requestDigest,
		digest:        committed.EvidenceDigest,
		evidence:      committed,
	}
}

// requestDigest digests the admissible identity of the sealed input: the same
// sealed input always yields the same digest, any tampering yields a
// different one (Contract §13.2).
func (s *AdmissionService) requestDigest(candidate *EvidenceCandidate) string {
	return digestOfValue(s.registry, map[string]any{
		"schema_version":    "gms.evidence-commit.v1",
		"evidence_id":       candidate.EvidenceID,
		"version":           json.Number(candidate.Version),
		"evidence_kind":     candidate.EvidenceKind,
		"evidence_digest":   candidate.EvidenceDigest,
		"path_seal_digest":  candidate.PathSealDigest,
		"segment_digest":    candidate.SegmentDigest,
		"checkpoint_digest": candidate.CheckpointDigest,
		"room_id":           candidate.RoomID,
		"segment_id":        candidate.SegmentID,
		"segment_version":   json.Number(candidate.SegmentVersion),
	})
}

// ---------------------------------------------------------------------------
// Committed-only readers (GMS §2.2: staged evidence is unreadable)
// ---------------------------------------------------------------------------

// CommittedEvidence returns every committed EvidenceRef in evidence-ledger
// sequence order, with its auditable provenance. Staged, rejected and
// inconclusive submissions are structurally absent — they never entered the
// ledger. A payload that is not a committed|sealed §7.7 ref fails closed with
// EVIDENCE_NOT_COMMITTED.
func (s *AdmissionService) CommittedEvidence() ([]CommittedEvidence, error) {
	entries, err := s.store.Snapshot(ledger.LedgerEvidence, "")
	if err != nil {
		return nil, err
	}
	out := make([]CommittedEvidence, 0, len(entries))
	for _, entry := range entries {
		s.mu.Lock()
		recorded := s.provenance[entry.EventID]
		s.mu.Unlock()
		if recorded != nil {
			out = append(out, copyEvidence(recorded.evidence))
			continue
		}
		committed, err := s.minimalCommitted(entry)
		if err != nil {
			return nil, err
		}
		out = append(out, committed)
	}
	return out, nil
}

// GetEvidence resolves one committed EvidenceRef by evidence id.
func (s *AdmissionService) GetEvidence(evidenceID string) (CommittedEvidence, bool, error) {
	all, err := s.CommittedEvidence()
	if err != nil {
		return CommittedEvidence{}, false, err
	}
	for _, item := range all {
		if item.EvidenceID == evidenceID {
			return item, true, nil
		}
	}
	return CommittedEvidence{}, false, nil
}

// StagingRecord exposes the private staging view of one submission (audit
// intake only; staged ids are never valid evidence references).
func (s *AdmissionService) StagingRecord(stagedID string) (StagingRecordView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.staging[stagedID]
	if !ok {
		return StagingRecordView{}, false
	}
	view := StagingRecordView{
		StagedID:         rec.id,
		Status:           rec.status,
		RoomID:           stringField(rec.segmentValue, "room_id"),
		SegmentID:        stringField(rec.segmentValue, "segment_id"),
		TerminalState:    stringField(rec.segmentValue, "terminal_state"),
		SubmissionDigest: rec.submissionDigest,
		Submitter:        rec.sub.Submitter,
		Events:           append([]StateEvent(nil), rec.events...),
	}
	if rec.outcome != nil {
		view.ValidationReason = rec.outcome.ReasonCode
	}
	if rec.status == StatusCommitted {
		if p, ok := s.provenance[rec.committedID]; ok {
			view.IdempotencyKey = p.key
		}
	}
	return view, true
}

// minimalCommitted rebuilds a committed view straight from one ledger entry
// when the derived provenance index has no record (e.g. after a restart);
// provenance fields outside the payload stay empty rather than invented.
func (s *AdmissionService) minimalCommitted(entry ledger.Entry) (CommittedEvidence, error) {
	payload, ok, err := s.store.Get(entry.PayloadDigest)
	if err != nil || !ok {
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonEvidenceStagingInvalid,
			"evidence ledger entry %s payload unresolved", entry.EventID)
	}
	value, err := contract.ParseJSONStrict(payload)
	if err != nil {
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonInvalidJSON, "committed payload of %s: %v", entry.EventID, err)
	}
	obj, isObj := contract.AsObject(value)
	if !isObj {
		return CommittedEvidence{}, newAdmissionError(s.registry, ReasonEvidenceNotCommitted, "committed payload of %s is not an object", entry.EventID)
	}
	ref, err := ParseEvidenceRef(s.registry, obj)
	if err != nil {
		return CommittedEvidence{}, err
	}
	committed := CommittedEvidence{
		EvidenceID:     ref.EvidenceID,
		Version:        ref.Version,
		EvidenceDigest: ref.EvidenceDigest,
		CommitState:    ref.CommitState,
		EvidenceKind:   ref.EvidenceKind,
		CommitSequence: entry.Sequence,
	}
	if ref.SourceSegmentRef != nil {
		committed.Segment = *ref.SourceSegmentRef
	}
	return committed, nil
}

// ---------------------------------------------------------------------------
// Minting and view assembly
// ---------------------------------------------------------------------------

// mintEvidenceRef builds the committed Contract §7.7 EvidenceRef of a
// candidate: the Host seal's evidence identity and recomputed digests, with
// GMS's own committer state (commit_state=committed) and the derived §7.5
// source SegmentRef. Returns the canonical JCS bytes.
func mintEvidenceRef(registry ledger.ReasonRegistry, candidate *EvidenceCandidate) ([]byte, error) {
	ref := map[string]any{
		"schema_version":     schemaEvidenceRef,
		"evidence_id":        candidate.EvidenceID,
		"version":            json.Number(candidate.Version),
		"evidence_digest":    candidate.EvidenceDigest,
		"commit_state":       "committed",
		"evidence_kind":      candidate.EvidenceKind,
		"source_segment_ref": candidate.SegmentRefJSON,
	}
	bytes, err := contract.JCS(ref)
	if err != nil {
		return nil, newAdmissionError(registry, ReasonNonIntegerNumber, "minted EvidenceRef cannot enter the hashed core: %v", err)
	}
	return bytes, nil
}

// buildCommittedEvidence parses the committed outcome bytes back through the
// strict §7.7/§7.5/§7.6 parsers and attaches the candidate's provenance.
func buildCommittedEvidence(registry ledger.ReasonRegistry, candidate *EvidenceCandidate, outcome []byte, submitter, stagedID string, sequence uint64) (CommittedEvidence, error) {
	value, err := contract.ParseJSONStrict(outcome)
	if err != nil {
		return CommittedEvidence{}, newAdmissionError(registry, ReasonInvalidJSON, "committed outcome is not strict JSON: %v", err)
	}
	obj, ok := contract.AsObject(value)
	if !ok {
		return CommittedEvidence{}, newAdmissionError(registry, ReasonEvidenceNotCommitted, "committed outcome is not an object")
	}
	ref, err := ParseEvidenceRef(registry, obj)
	if err != nil {
		return CommittedEvidence{}, err
	}
	committed := CommittedEvidence{
		EvidenceID:         ref.EvidenceID,
		Version:            ref.Version,
		EvidenceDigest:     ref.EvidenceDigest,
		CommitState:        ref.CommitState,
		EvidenceKind:       ref.EvidenceKind,
		EvidenceSealID:     candidate.EvidenceSealID,
		EvidenceSealDigest: candidate.EvidenceDigest,
		PathSealID:         candidate.PathSealID,
		PathSealDigest:     candidate.PathSealDigest,
		PathIDs:            append([]string(nil), candidate.PathIDs...),
		Submitter:          submitter,
		StagedID:           stagedID,
		CommitSequence:     sequence,
	}
	if ref.SourceSegmentRef != nil {
		committed.Segment = *ref.SourceSegmentRef
	}
	if checkpoint, err := ParseCheckpointRef(registry, candidate.CheckpointRefJSON); err == nil {
		committed.Checkpoint = checkpoint
	}
	return committed, nil
}

func copyEvidence(evidence CommittedEvidence) CommittedEvidence {
	evidence.PathIDs = append([]string(nil), evidence.PathIDs...)
	return evidence
}

func resolvedIdempotencyKey(externalKey string, candidate *EvidenceCandidate) string {
	if externalKey != "" {
		return externalKey
	}
	return "evidence-commit/" + candidate.EvidenceID
}

// ---------------------------------------------------------------------------
// Closed enums and small helpers
// ---------------------------------------------------------------------------

var segmentTerminals = map[string]bool{"settled": true, "failed": true, "aborted": true}

var terminalDeliveryStates = map[string]bool{"delivered": true, "failed": true, "cancelled": true}

var eventKinds = map[string]bool{
	"agent_message": true, "model_response": true, "tool_call": true,
	"tool_result": true, "delivery_transition": true, "decision_checkpoint": true,
	"replay_dispatch": true, "replay_completion": true,
}

var linkKinds = map[string]bool{
	"room_sequence": true, "response_to": true, "caused_by": true,
	"tool_call_to_result": true, "delivery_of": true, "segment_membership": true,
	"replay_of": true,
}

var pathDomains = map[string]bool{"success": true, "failure": true, "recovery": true}

// fail panics with the verified AdmissionError; every gate runs inside
// runGates, which recovers the panic into a rejected outcome.
func fail(registry ledger.ReasonRegistry, code, detail string, args ...any) {
	panic(newAdmissionError(registry, code, detail, args...))
}

func stringField(obj map[string]any, key string) string {
	value, _ := contract.AsString(obj[key])
	return value
}

func intField(obj map[string]any, key string) (int64, bool) {
	number, ok := obj[key].(json.Number)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func stringVersion(value any) string {
	switch v := value.(type) {
	case json.Number:
		return v.String()
	case string:
		return v
	default:
		return ""
	}
}

func digestOfValue(registry ledger.ReasonRegistry, value any) string {
	digest, err := contract.DigestOf(value)
	if err != nil {
		fail(registry, ReasonNonIntegerNumber, "value cannot enter the hashed core: %v", err)
	}
	return digest
}

func mustObject(registry ledger.ReasonRegistry, obj map[string]any, key string) map[string]any {
	value, ok := contract.AsObject(obj[key])
	if !ok {
		fail(registry, ReasonSchemaRequiredMissing, "%s must be an object", key)
	}
	return value
}

func sealedScopeProfile(seg map[string]any) map[string]any {
	if body, ok := contract.AsObject(seg["evidence_seal_body"]); ok {
		if ref, ok := contract.AsObject(body["scope_profile_ref"]); ok {
			return ref
		}
	}
	return nil
}

func hasCycle(nodes map[string]bool, adjacency map[string][]string) bool {
	const (
		white, gray, black = 0, 1, 2
	)
	color := make(map[string]int, len(nodes))
	starts := make([]string, 0, len(nodes))
	for node := range nodes {
		starts = append(starts, node)
	}
	sort.Strings(starts)
	var visit func(string) bool
	visit = func(node string) bool {
		color[node] = gray
		for _, next := range adjacency[node] {
			if !nodes[next] {
				continue
			}
			if color[next] == gray {
				return true
			}
			if color[next] == white && visit(next) {
				return true
			}
		}
		color[node] = black
		return false
	}
	for _, node := range starts {
		if color[node] == white && visit(node) {
			return true
		}
	}
	return false
}

func equalSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func unique(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func domainsList(domains map[string]bool) []string {
	out := make([]string, 0, len(domains))
	for domain := range domains {
		out = append(out, domain)
	}
	return out
}
