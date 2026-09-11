package segment

// Seal construction and DTO derivation over the frozen sealed-segment data
// protocol (FND-002): the canonical segment record, the Evidence Seal body
// (Host 4.2, eleven semantic fields), the Conversation Path Seal body
// (Host 4.3, thirteen fields per path), and the Contract 7.5-7.7 ref
// projections. All canonical bytes come from internal/contract JCS;
// encoding/json is never a canonicalization path.

import (
	"sort"
	"strconv"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// ---------------------------------------------------------------------------
// Canonical JSON building blocks (exact frozen shapes)
// ---------------------------------------------------------------------------

func number(n int64) contract.Number { return contract.Number(strconv.FormatInt(n, 10)) }

func refObject(r Ref) *contract.Object {
	o := contract.NewObject()
	o.Set("id", contract.String(r.ID))
	o.Set("version", number(r.Version))
	o.Set("digest", contract.String(r.Digest))
	return o
}

func memberObject(m Member) *contract.Object {
	o := contract.NewObject()
	o.Set("event_id", contract.String(m.EventID))
	o.Set("event_kind", contract.String(m.EventKind))
	o.Set("room_sequence", number(m.RoomSequence))
	o.Set("logical_tick", number(m.LogicalTick))
	o.Set("actor_id", contract.String(m.ActorID))
	o.Set("payload_digest", contract.String(m.PayloadDigest))
	o.Set("payload", m.Payload)
	return o
}

func linkObject(l CausalLink) *contract.Object {
	o := contract.NewObject()
	o.Set("link_id", contract.String(l.LinkID))
	o.Set("link_kind", contract.String(l.LinkKind))
	o.Set("from_event", contract.String(l.FromEvent))
	o.Set("to_event", contract.String(l.ToEvent))
	return o
}

func deliveryObject(d Delivery) *contract.Object {
	o := contract.NewObject()
	o.Set("delivery_id", contract.String(d.DeliveryID))
	o.Set("subject_event", contract.String(d.SubjectEvent))
	o.Set("terminal_state", contract.String(d.TerminalState))
	return o
}

func executionObject(e ToolExecution) *contract.Object {
	o := contract.NewObject()
	o.Set("tool_call_event", contract.String(e.ToolCallEvent))
	o.Set("tool_result_event", contract.String(e.ToolResultEvent))
	o.Set("delivery_id", contract.String(e.DeliveryID))
	o.Set("tool_proxy_result", e.ToolProxyResult)
	if e.UpstreamAttemptRecord != nil {
		o.Set("upstream_attempt_record", e.UpstreamAttemptRecord)
	}
	if e.RetryCorrelation != nil {
		o.Set("retry_correlation", e.RetryCorrelation)
	}
	return o
}

func frontierObject(start, end int64) *contract.Object {
	o := contract.NewObject()
	o.Set("start_room_sequence", number(start))
	o.Set("end_room_sequence", number(end))
	return o
}

type closeMeta struct {
	Policy Ref
	Key    string
	Intent string
}

func (c closeMeta) object() *contract.Object {
	o := contract.NewObject()
	o.Set("close_policy_ref", refObject(c.Policy))
	o.Set("close_idempotency_key", contract.String(c.Key))
	o.Set("close_intent", contract.String(c.Intent))
	return o
}

type checkpointIdentity struct {
	ID       string
	Sequence int64
}

func (c checkpointIdentity) object() *contract.Object {
	o := contract.NewObject()
	o.Set("checkpoint_id", contract.String(c.ID))
	o.Set("checkpoint_sequence", number(c.Sequence))
	return o
}

// ---------------------------------------------------------------------------
// Evidence Seal body (Host 4.2)
// ---------------------------------------------------------------------------

// buildEvidenceSealBody freezes the eleven semantic Evidence Seal fields over
// the frozen membership: identity + sequence interval, ordered event refs
// with payload digests, actor identities, delivery terminals, exact
// ToolProxyResult digest correlations, the causal link set digest,
// included/excluded evidence candidates, scope profile, close policy and
// canonicalization profile.
func buildEvidenceSealBody(snap Snapshot, paths []Path, frontierEnd int64) (*contract.Object, error) {
	seg := snap.Segment
	body := contract.NewObject()
	body.Set("schema_version", contract.String(schemaEvidenceSeal))
	body.Set("room_id", contract.String(seg.RoomID))
	body.Set("segment_id", contract.String(seg.SegmentID))
	body.Set("segment_version", number(seg.SegmentVersion))
	body.Set("sequence_interval", frontierObject(seg.FrontierStart, frontierEnd))

	refs := make(contract.Array, 0, len(snap.Members))
	for _, m := range snap.Members {
		entry := contract.NewObject()
		entry.Set("event_id", contract.String(m.EventID))
		entry.Set("room_sequence", number(m.RoomSequence))
		entry.Set("payload_digest", contract.String(m.PayloadDigest))
		refs = append(refs, entry)
	}
	body.Set("ordered_event_refs", refs)

	actorSet := map[string]bool{}
	for _, m := range snap.Members {
		actorSet[m.ActorID] = true
	}
	actors := make([]string, 0, len(actorSet))
	for a := range actorSet {
		actors = append(actors, a)
	}
	sort.Strings(actors)
	actorArr := make(contract.Array, 0, len(actors))
	for _, a := range actors {
		actorArr = append(actorArr, contract.String(a))
	}
	body.Set("actor_ids", actorArr)

	deliveries := make(contract.Array, 0, len(snap.Deliveries))
	for _, d := range snap.Deliveries {
		entry := contract.NewObject()
		entry.Set("delivery_id", contract.String(d.DeliveryID))
		entry.Set("terminal_state", contract.String(d.TerminalState))
		deliveries = append(deliveries, entry)
	}
	body.Set("deliveries", deliveries)

	correlations := make(contract.Array, 0, len(snap.Executions))
	for _, e := range snap.Executions {
		digest, _ := contract.StringOf(e.ToolProxyResult, "proxy_result_digest")
		entry := contract.NewObject()
		entry.Set("tool_call_event", contract.String(e.ToolCallEvent))
		entry.Set("tool_result_event", contract.String(e.ToolResultEvent))
		entry.Set("tool_proxy_result_digest", contract.String(digest))
		correlations = append(correlations, entry)
	}
	body.Set("tool_correlations", correlations)

	linkObjects := make(contract.Array, 0, len(snap.Links))
	for _, l := range snap.Links {
		linkObjects = append(linkObjects, linkObject(l))
	}
	linkSetDigest, err := contract.DigestOf(linkObjects)
	if err != nil {
		return nil, canonicalFailure(err)
	}
	body.Set("causal_link_set_digest", contract.String(linkSetDigest))

	// Included evidence candidates: the terminal model response of the
	// primary path, classified by the sealed path families.
	primary := primaryPath(paths)
	candidateID := ""
	for _, id := range primary.OrderedEventRefs {
		for _, m := range snap.Members {
			if m.EventID == id && m.EventKind == "model_response" {
				candidateID = id
			}
		}
	}
	if candidateID == "" {
		candidateID = primary.OrderedEventRefs[len(primary.OrderedEventRefs)-1]
	}
	candidates := contract.Array{}
	candidate := contract.NewObject()
	candidate.Set("event_id", contract.String(candidateID))
	candidate.Set("evidence_kind", contract.String(primaryEvidenceKind(paths)))
	candidates = append(candidates, candidate)
	body.Set("included_evidence_candidates", candidates)
	body.Set("excluded_event_refs", contract.Array{})

	body.Set("scope_profile_ref", refObject(seg.ScopeProfileRef))
	policy := contract.NewObject()
	policy.Set("close_policy_ref", refObject(seg.ClosePolicyRef))
	policy.Set("canonicalization_profile", contract.String("rfc8785-jcs"))
	policy.Set("schema_version", contract.String(schemaSealedSegment))
	body.Set("close_policy", policy)
	return body, nil
}

// ---------------------------------------------------------------------------
// Conversation Path Seal body (Host 4.3)
// ---------------------------------------------------------------------------

func buildPathSealBody(snap Snapshot, paths []Path) (*contract.Object, error) {
	seg := snap.Segment
	body := contract.NewObject()
	body.Set("schema_version", contract.String(schemaPathSeal))
	body.Set("room_id", contract.String(seg.RoomID))
	body.Set("segment_id", contract.String(seg.SegmentID))
	body.Set("segment_version", number(seg.SegmentVersion))

	pathArr := make(contract.Array, 0, len(paths))
	for _, p := range paths {
		o := contract.NewObject()
		o.Set("path_id", contract.String(p.PathID))
		o.Set("domain", contract.String(p.Domain))
		events := make(contract.Array, 0, len(p.OrderedEventRefs))
		for _, id := range p.OrderedEventRefs {
			events = append(events, contract.String(id))
		}
		o.Set("ordered_event_refs", events)
		linkRefs := make(contract.Array, 0, len(p.CausalLinkRefs))
		for _, id := range p.CausalLinkRefs {
			linkRefs = append(linkRefs, contract.String(id))
		}
		o.Set("causal_link_refs", linkRefs)
		o.Set("causal_links_digest", contract.String(p.CausalLinksDigest))
		o.Set("start_room_sequence", number(p.StartRoomSequence))
		o.Set("end_room_sequence", number(p.EndRoomSequence))
		o.Set("terminal_outcome", contract.String(p.TerminalOutcome))
		tools := make(contract.Array, 0, len(p.ToolOutcomes))
		for _, t := range p.ToolOutcomes {
			to := contract.NewObject()
			to.Set("tool_call_event", contract.String(t.ToolCallEvent))
			to.Set("outcome", contract.String(t.Outcome))
			if t.Attempts > 0 {
				to.Set("attempts", number(t.Attempts))
			}
			tools = append(tools, to)
		}
		o.Set("tool_outcomes", tools)
		deliveries := make(contract.Array, 0, len(p.DeliveryOutcomes))
		for _, d := range p.DeliveryOutcomes {
			dv := contract.NewObject()
			dv.Set("delivery_id", contract.String(d.DeliveryID))
			dv.Set("outcome", contract.String(d.Outcome))
			deliveries = append(deliveries, dv)
		}
		o.Set("delivery_outcomes", deliveries)
		o.Set("checkpoint_ref_id", contract.String(p.CheckpointRefID))
		o.Set("completeness", contract.String(p.Completeness))
		o.Set("extraction_policy_ref", refObject(seg.PathExtractionPolicyRef))
		pathArr = append(pathArr, o)
	}
	body.Set("paths", pathArr)
	return body, nil
}

// ---------------------------------------------------------------------------
// Canonical segment record
// ---------------------------------------------------------------------------

func logicalClockObject(snap Snapshot) *contract.Object {
	first, last := int64(0), int64(0)
	if len(snap.Members) > 0 {
		first = snap.Members[0].LogicalTick
		last = snap.Members[len(snap.Members)-1].LogicalTick
		for _, m := range snap.Members {
			if m.LogicalTick < first {
				first = m.LogicalTick
			}
			if m.LogicalTick > last {
				last = m.LogicalTick
			}
		}
	}
	o := contract.NewObject()
	o.Set("kind", contract.String("monotonic-integer"))
	o.Set("first_tick", number(first))
	o.Set("last_tick", number(last))
	return o
}

func baseRecord(snap Snapshot, meta closeMeta, frontierEnd int64) *contract.Object {
	seg := snap.Segment
	record := contract.NewObject()
	record.Set("schema_version", contract.String(schemaSealedSegment))
	record.Set("room_id", contract.String(seg.RoomID))
	record.Set("segment_id", contract.String(seg.SegmentID))
	record.Set("segment_version", number(seg.SegmentVersion))
	record.Set("logical_clock", logicalClockObject(snap))
	record.Set("close", meta.object())
	record.Set("frontier", frontierObject(seg.FrontierStart, frontierEnd))
	members := make(contract.Array, 0, len(snap.Members))
	for _, m := range snap.Members {
		members = append(members, memberObject(m))
	}
	record.Set("members", members)
	links := make(contract.Array, 0, len(snap.Links))
	for _, l := range snap.Links {
		links = append(links, linkObject(l))
	}
	record.Set("causal_links", links)
	deliveries := make(contract.Array, 0, len(snap.Deliveries))
	for _, d := range snap.Deliveries {
		deliveries = append(deliveries, deliveryObject(d))
	}
	record.Set("deliveries", deliveries)
	executions := make(contract.Array, 0, len(snap.Executions))
	for _, e := range snap.Executions {
		executions = append(executions, executionObject(e))
	}
	record.Set("tool_executions", executions)
	return record
}

// buildSettledRecord assembles the settled sealed-segment record: the frozen
// input plus checkpoint identity and both seal bodies. The checkpoint digest
// preimage never includes a checkpoint_digest (Host 3.6 no-digest-cycle
// rule).
func buildSettledRecord(snap Snapshot, closeKey string, checkpoint checkpointIdentity,
	evidenceBody, pathBody *contract.Object, frontierEnd int64) (*contract.Object, error) {
	record := baseRecord(snap, closeMeta{
		Policy: snap.Segment.ClosePolicyRef, Key: closeKey, Intent: string(IntentSettle),
	}, frontierEnd)
	record.Set("terminal_state", contract.String(StateSettled))
	record.Set("checkpoint_identity", checkpoint.object())
	record.Set("evidence_seal_body", evidenceBody)
	record.Set("path_seal_body", pathBody)
	return record, nil
}

// buildTerminalRecord assembles a failed/aborted record: frozen input plus
// the terminal audit, never any seal body, checkpoint identity or ref.
func buildTerminalRecord(snap Snapshot, meta closeMeta, audit TerminalAudit, state string) (*contract.Object, error) {
	record := baseRecord(snap, meta, snap.HeadSequence)
	record.Set("terminal_state", contract.String(state))
	auditObj := contract.NewObject()
	auditObj.Set("reason_code", contract.String(audit.ReasonCode))
	auditObj.Set("phase", contract.String(audit.Phase))
	auditObj.Set("detail", contract.String(audit.Detail))
	record.Set("terminal_audit", auditObj)
	return record, nil
}

// ---------------------------------------------------------------------------
// DTO derivations (Contract 7.5-7.7)
// ---------------------------------------------------------------------------

// deriveSegmentRef builds the Contract 7.5 SegmentRef carrying both seal
// refs. Seal identity is versioned per segment identity (v1).
func deriveSegmentRef(seg OpenSegment, segmentDigest, evidenceDigest, pathDigest string) SegmentRef {
	return SegmentRef{
		SchemaVersion:   schemaSegmentRef,
		RoomID:          seg.RoomID,
		SegmentID:       seg.SegmentID,
		SegmentVersion:  seg.SegmentVersion,
		SegmentDigest:   segmentDigest,
		EvidenceSealRef: Ref{ID: "evseal-" + seg.SegmentID, Version: 1, Digest: evidenceDigest},
		PathSealRef:     Ref{ID: "pathseal-" + seg.SegmentID, Version: 1, Digest: pathDigest},
	}
}

// deriveCheckpointRef builds the terminal Contract 7.6 CheckpointRef. The
// frozen checkpoint_digest preimage is (room_id, segment_id, segment_version,
// segment_digest, checkpoint_id, checkpoint_sequence, close_intent); it
// never contains a checkpoint_digest of itself.
func deriveCheckpointRef(roomID string, segRef SegmentRef, checkpoint checkpointIdentity,
	segmentDigest, closeIntent string) CheckpointRef {
	preimage := contract.NewObject()
	preimage.Set("room_id", contract.String(roomID))
	preimage.Set("segment_id", contract.String(segRef.SegmentID))
	preimage.Set("segment_version", number(segRef.SegmentVersion))
	preimage.Set("segment_digest", contract.String(segmentDigest))
	preimage.Set("checkpoint_id", contract.String(checkpoint.ID))
	preimage.Set("checkpoint_sequence", number(checkpoint.Sequence))
	preimage.Set("close_intent", contract.String(closeIntent))
	digest, err := contract.DigestOf(preimage)
	if err != nil {
		digest = "" // string-only preimage: unreachable; callers treat "" as invalid
	}
	return CheckpointRef{
		SchemaVersion:      schemaCheckpointRef,
		RoomID:             roomID,
		SegmentRef:         segRef,
		CheckpointID:       checkpoint.ID,
		CheckpointSequence: checkpoint.Sequence,
		CheckpointDigest:   digest,
	}
}

// deriveEvidenceRef builds the Contract 7.7 EvidenceRef projection: the
// sealed evidence identity bound to its source SegmentRef.
func deriveEvidenceRef(segRef SegmentRef, evidenceDigest, evidenceKind string) EvidenceRef {
	return EvidenceRef{
		SchemaVersion:    schemaEvidenceRef,
		EvidenceID:       "evseal-" + segRef.SegmentID,
		Version:          1,
		EvidenceDigest:   evidenceDigest,
		CommitState:      "sealed",
		EvidenceKind:     evidenceKind,
		SourceSegmentRef: segRef,
	}
}

// Object renders the SegmentRef as its canonical DTO JSON shape.
func (r SegmentRef) Object() *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(r.SchemaVersion))
	o.Set("room_id", contract.String(r.RoomID))
	o.Set("segment_id", contract.String(r.SegmentID))
	o.Set("segment_version", number(r.SegmentVersion))
	o.Set("segment_digest", contract.String(r.SegmentDigest))
	o.Set("evidence_seal_ref", refObject(r.EvidenceSealRef))
	o.Set("path_seal_ref", refObject(r.PathSealRef))
	return o
}

// Object renders the CheckpointRef as its canonical DTO JSON shape.
func (r CheckpointRef) Object() *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(r.SchemaVersion))
	o.Set("room_id", contract.String(r.RoomID))
	o.Set("segment_ref", r.SegmentRef.Object())
	o.Set("checkpoint_id", contract.String(r.CheckpointID))
	o.Set("checkpoint_sequence", number(r.CheckpointSequence))
	o.Set("checkpoint_digest", contract.String(r.CheckpointDigest))
	return o
}

// Object renders the EvidenceRef as its canonical DTO JSON shape.
func (r EvidenceRef) Object() *contract.Object {
	o := contract.NewObject()
	o.Set("schema_version", contract.String(r.SchemaVersion))
	o.Set("evidence_id", contract.String(r.EvidenceID))
	o.Set("version", number(r.Version))
	o.Set("evidence_digest", contract.String(r.EvidenceDigest))
	o.Set("commit_state", contract.String(r.CommitState))
	o.Set("evidence_kind", contract.String(r.EvidenceKind))
	o.Set("source_segment_ref", r.SourceSegmentRef.Object())
	return o
}
