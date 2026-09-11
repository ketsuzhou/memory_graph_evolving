// rules.go freezes the projection relation rules: the nine-relation closed
// set and the deterministic mapping from authority source records to Graph
// mutations (GMS §9.2 rule table; Contract §11.3/§11.4). The rules are the
// ONLY constructor of Graph edges — a Graph-invented edge, an unknown
// relation, a candidate/rejected endpoint, uncommitted evidence or missing
// provenance fails closed with PROJECTION_RELATION_INVALID and blocks the
// projection (Contract §13.6).
package projector

import (
	"encoding/json"
	"strconv"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
)

// The nine-relation closed set (Contract §11.3). Any relation outside this
// set is rejected — v1 MUST NOT accept free relation strings.
const (
	RelSupersedes  = "supersedes"
	RelComposes    = "composes"
	RelDependsOn   = "depends_on"
	RelHasBranch   = "has_branch"
	RelDerivedFrom = "derived_from"
	RelAnchoredAt  = "anchored_at"
	RelSupportedBy = "supported_by"
	RelRefutedBy   = "refuted_by"
	RelSimilarTo   = "similar_to"
)

// ClosedRelations is the frozen relation set.
var ClosedRelations = map[string]bool{
	RelSupersedes: true, RelComposes: true, RelDependsOn: true,
	RelHasBranch: true, RelDerivedFrom: true, RelAnchoredAt: true,
	RelSupportedBy: true, RelRefutedBy: true, RelSimilarTo: true,
}

// activationEventView is one strictly parsed Contract §7.13 event.
type activationEventView struct {
	sequence    uint64
	eventID     string
	eventType   string
	lineageID   string
	skillRef    contract.SkillArtifactRef
	previous    *contract.SkillArtifactRef
	derivedFrom []contract.SkillArtifactRef
	mergeOrigin bool // candidate_ref.origin_type == merge_proposal
	source      SourceRef
}

// parseActivationEvent strictly re-derives one delivered event from its
// canonical payload. The event was authority-validated when the activation
// transaction committed; the projector re-validates the shape (closed field
// set through the authority schema, without the corpus-stale x-digest
// recompute) and every field it consumes, and proves the delivered bytes
// canonicalize to the declared ledger digest.
func (s *Service) parseActivationEvent(sequence uint64, eventID string, payload []byte, payloadDigest string) (*activationEventView, error) {
	value, err := contract.ParseJSONStrict(payload)
	if err != nil {
		return nil, newError(s.registry, ledger.ReasonInvalidJSON, "activation event %d is not valid UTF-8 JSON: %v", sequence, err)
	}
	canonical, err := contract.JCS(contract.NormalizeForHashing(value))
	if err != nil {
		return nil, newError(s.registry, ReasonDigestMismatch, "activation event %d cannot canonicalize: %v", sequence, err)
	}
	if contract.DigestBytes(canonical) != payloadDigest {
		return nil, newError(s.registry, ReasonDigestMismatch,
			"activation event %d canonicalizes to %s but the delivery declares %s", sequence, contract.DigestBytes(canonical), payloadDigest)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		return nil, newError(s.registry, ReasonSchemaFieldUnknown, "activation event %d is not a JSON object", sequence)
	}
	if sv, _ := contract.AsString(obj["schema_version"]); sv != ledger.SchemaActivationEvent {
		return nil, newError(s.registry, ReasonProjectionSchemaUnsupported,
			"activation event %d schema_version %q is not %q (unknown schema blocks, Contract §13.6)", sequence, sv, ledger.SchemaActivationEvent)
	}
	eventType, _ := contract.AsString(obj["event_type"])
	if eventType != activationTypeActivate && eventType != activationTypeDeactivate {
		// probation included: the reserved state is unreachable and unknown
		// event types block the projection (Contract §9.2.6, GMS §6.4).
		return nil, newError(s.registry, ReasonProbationUnsupported,
			"activation event %d event_type %q is outside the closed {activate, deactivate} set", sequence, eventType)
	}
	schemaFile := activationSchemaActivate
	if eventType == activationTypeDeactivate {
		schemaFile = activationSchemaDeactivate
	}
	if err := s.gates.ValidateShape(obj, schemaFile); err != nil {
		return nil, newError(s.registry, ReasonProjectionSchemaUnsupported,
			"activation event %d failed its authority shape: %v", sequence, err)
	}
	if id, _ := contract.AsString(obj["event_id"]); id != eventID {
		return nil, newError(s.registry, ReasonRefMismatch,
			"activation event %d declares event_id %q but the delivery carries %q", sequence, id, eventID)
	}
	if !integerEquals(obj["activation_sequence"], sequence) {
		return nil, newError(s.registry, ReasonRefMismatch,
			"activation event payload sequence %v does not match the delivered sequence %d", obj["activation_sequence"], sequence)
	}
	lineage, _ := contract.AsString(obj["lineage_id"])
	refRaw, _ := contract.AsObject(obj["skill_ref"])
	ref, err := contract.ParseSkillArtifactRef(refRaw)
	if err != nil {
		return nil, newError(s.registry, ReasonRefMismatch, "activation event %d skill_ref is not an exact §7.3 ref: %v", sequence, err)
	}
	if ref.LineageID != lineage {
		return nil, newError(s.registry, ReasonRefMismatch,
			"activation event %d lineage_id %q disagrees with skill_ref lineage %q", sequence, lineage, ref.LineageID)
	}
	view := &activationEventView{
		sequence:  sequence,
		eventID:   eventID,
		eventType: eventType,
		lineageID: lineage,
		skillRef:  ref,
		source:    SourceRef{Source: SourceActivation, Sequence: sequence, RecordID: eventID, Digest: payloadDigest},
	}
	if raw, present := obj["previous_active_ref"]; present {
		prevRaw, _ := contract.AsObject(raw)
		previous, err := contract.ParseSkillArtifactRef(prevRaw)
		if err != nil {
			return nil, newError(s.registry, ReasonRefMismatch, "activation event %d previous_active_ref invalid: %v", sequence, err)
		}
		view.previous = &previous
	}
	if raw, present := obj["derived_from_refs"]; present {
		refs, ok := contract.AsArray(raw)
		if !ok {
			return nil, newError(s.registry, ReasonRefMismatch, "activation event %d derived_from_refs must be an array", sequence)
		}
		for _, item := range refs {
			itemObj, _ := contract.AsObject(item)
			derived, err := contract.ParseSkillArtifactRef(itemObj)
			if err != nil {
				return nil, newError(s.registry, ReasonRefMismatch, "activation event %d derived_from_refs entry invalid: %v", sequence, err)
			}
			view.derivedFrom = append(view.derivedFrom, derived)
		}
	}
	if raw, present := obj["candidate_ref"]; present {
		candRaw, _ := contract.AsObject(raw)
		cand, err := contract.ParseCandidateArtifactRef(candRaw)
		if err != nil {
			return nil, newError(s.registry, ReasonRefMismatch, "activation event %d candidate_ref invalid: %v", sequence, err)
		}
		view.mergeOrigin = cand.OriginType == "merge_proposal"
	}
	return view, nil
}

// activationMutations derives the Graph mutations of one activation event
// (GMS §9.2/§9.4, Contract §11.4):
//
//   - activate: revision node upsert (reactivation reuses the historical
//     node), supersedes over the event's previous_active_ref (never onto the
//     merge sources: M@1 events carry no previous ref), derived_from
//     provenance edges, the body-derived structural relations
//     (has_branch/composes/depends_on) and the lineage active pointer;
//   - deactivate: lifecycle metadata update only — nodes, edges and
//     history are retained (C1, Contract §11.5).
//
// Every failure is a blocking projection error; nothing is staged.
func (s *Service) activationMutations(staged *graphState, ev *activationEventView) ([]Mutation, error) {
	var mutations []Mutation
	source := ev.source
	if ev.eventType == activationTypeDeactivate {
		key := RevisionRef(ev.skillRef).String()
		if _, ok := staged.revisions[key]; !ok {
			return nil, newError(s.registry, ReasonProjectionRelationInvalid,
				"deactivate event %d targets %s which was never projected; the Graph cannot invent a revision (Contract §5.3.3)", ev.sequence, key)
		}
		if active, ok := staged.lineageActive[ev.lineageID]; ok && active != ev.skillRef {
			return nil, newError(s.registry, ReasonRefMismatch,
				"deactivate event %d targets %s v%s but the projected lineage head is v%s; derived state diverged from the authority prefix",
				ev.sequence, ev.lineageID, ev.skillRef.Version, active.Version)
		}
		mutations = append(mutations,
			setLifecycle{Revision: ev.skillRef, Lifecycle: LifecycleDeactivated},
			clearLineageActive{LineageID: ev.lineageID, Ref: ev.skillRef},
			recordSourceDigest{Source: SourceActivation, Sequence: ev.sequence, Digest: source.Digest},
		)
		return mutations, nil
	}

	// activate: the revision node (historical node reuse on reactivation).
	node, existed := staged.revisions[RevisionRef(ev.skillRef).String()]
	if existed {
		node.Lifecycle = LifecycleActive
	} else {
		node = RevisionNode{
			Ref:                     ev.skillRef,
			Lifecycle:               LifecycleActive,
			FirstProjectedSequence:  ev.sequence,
			ProjectionSchemaVersion: ProjectionSchemaVersion,
		}
	}
	mutations = append(mutations, upsertRevisionNode{Node: node})

	// The authority prefix must agree with the derived lineage pointer: an
	// event claiming no predecessor over a lineage the Graph still shows
	// active (or a predecessor other than the projected head) means the
	// watermark would diverge from the ledger replay.
	if projected, ok := staged.lineageActive[ev.skillRef.LineageID]; ok {
		if ev.previous == nil {
			return nil, newError(s.registry, ReasonRefMismatch,
				"activate event %d declares no previous_active_ref but the projected head of %s is v%s",
				ev.sequence, ev.lineageID, projected.Version)
		}
		if *ev.previous != projected {
			return nil, newError(s.registry, ReasonRefMismatch,
				"activate event %d supersedes %s v%s but the projected head is v%s",
				ev.sequence, ev.lineageID, ev.previous.Version, projected.Version)
		}
	}

	// supersedes: only from a successful activation transition with a
	// previous active ref that differs from the activated revision
	// (GMS §9.4: deactivate never deletes edges; M@1 never supersedes A/B).
	if ev.previous != nil && *ev.previous != ev.skillRef {
		if ev.previous.LineageID != ev.skillRef.LineageID || ev.previous.Kind != ev.skillRef.Kind {
			return nil, newError(s.registry, ReasonProjectionRelationInvalid,
				"activate event %d supersedes %s with %s: supersedes MUST stay within one lineage and kind (Contract §11.4)",
				ev.sequence, ev.previous.LineageID+"/"+ev.previous.Kind, ev.skillRef.LineageID+"/"+ev.skillRef.Kind)
		}
		if _, ok := staged.revisions[RevisionRef(*ev.previous).String()]; !ok {
			return nil, newError(s.registry, ReasonProjectionRelationInvalid,
				"activate event %d supersedes %s v%s which was never projected (torn activation history)", ev.sequence, ev.previous.LineageID, ev.previous.Version)
		}
		mutations = append(mutations,
			upsertEdge{Edge: Edge{Relation: RelSupersedes, From: RevisionRef(ev.skillRef), To: RevisionRef(*ev.previous), Source: source}},
			setLifecycle{Revision: *ev.previous, Lifecycle: LifecycleSuperseded},
		)
	}

	// derived_from: authoritative release provenance only. A merge-origin
	// activation MUST carry exactly the two sources (M3: M@1 has exactly
	// A/B); every derived ref MUST already be a Runtime revision node — the
	// Graph never invents provenance (C3).
	if ev.mergeOrigin && len(ev.derivedFrom) != 2 {
		return nil, newError(s.registry, ReasonProjectionRelationInvalid,
			"merge-origin activate event %d carries %d derived_from refs; M@1 MUST carry exactly the two sources (GMS §7.7/§12.13)", ev.sequence, len(ev.derivedFrom))
	}
	for _, derived := range ev.derivedFrom {
		if _, ok := staged.revisions[RevisionRef(derived).String()]; !ok {
			return nil, newError(s.registry, ReasonProjectionRelationInvalid,
				"activate event %d derives from %s v%s which was never projected; missing provenance endpoint blocks (C3)", ev.sequence, derived.LineageID, derived.Version)
		}
		mutations = append(mutations,
			upsertEdge{Edge: Edge{Relation: RelDerivedFrom, From: RevisionRef(ev.skillRef), To: RevisionRef(derived), Source: source}},
		)
	}

	// Body-derived structural relations: resolve the released canonical
	// body from the content store and re-verify its digest (GMS §9.3).
	bodyMutations, err := s.bodyMutations(staged, ev)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, bodyMutations...)

	mutations = append(mutations,
		setLineageActive{LineageID: ev.lineageID, Ref: ev.skillRef},
		recordSourceDigest{Source: SourceActivation, Sequence: ev.sequence, Digest: source.Digest},
	)
	return mutations, nil
}

// bodyMutations derives has_branch/composes/depends_on from the released
// canonical artifact body (GMS §9.3: structural relations are derived only
// from the released canonical body, and the projector re-verifies the body
// digest; illegal free relations, floating children and cycles block).
func (s *Service) bodyMutations(staged *graphState, ev *activationEventView) ([]Mutation, error) {
	body, ok, err := s.store.Get(ev.skillRef.ArtifactDigest)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newError(s.registry, ReasonDigestMismatch,
			"activate event %d: released body %s resolves to no committed content; the projector cannot verify the body digest (GMS §9.3)", ev.sequence, ev.skillRef.ArtifactDigest)
	}
	if contract.DigestBytes(body) != ev.skillRef.ArtifactDigest {
		return nil, newError(s.registry, ReasonDigestMismatch,
			"activate event %d: committed body under %s digests to %s (GMS §9.3 body digest re-verification)", ev.sequence, ev.skillRef.ArtifactDigest, contract.DigestBytes(body))
	}
	value, err := contract.ParseJSONStrict(body)
	if err != nil {
		return nil, newError(s.registry, ReasonDigestMismatch, "released body %s is not valid JSON: %v", ev.skillRef.ArtifactDigest, err)
	}
	envelope, _ := contract.AsObject(value)
	if envelope == nil {
		return nil, newError(s.registry, ReasonDigestMismatch, "released body %s is not an object", ev.skillRef.ArtifactDigest)
	}
	kind, _ := contract.AsString(envelope["kind"])
	if kind != ev.skillRef.Kind {
		return nil, newError(s.registry, ReasonProjectionRelationInvalid,
			"activate event %d: body kind %q disagrees with the exact ref kind %q (kind drift)", ev.sequence, kind, ev.skillRef.Kind)
	}
	bodyObj, _ := contract.AsObject(envelope["body"])
	if bodyObj == nil {
		return nil, newError(s.registry, ReasonProjectionRelationInvalid, "released body %s carries no body object", ev.skillRef.ArtifactDigest)
	}

	var mutations []Mutation
	source := ev.source
	seenBranches := map[string]bool{}
	switch kind {
	case "step_guidance":
		branches, _ := contract.AsArray(bodyObj["branches"])
		for _, raw := range branches {
			branch, _ := contract.AsObject(raw)
			if branch == nil {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid, "guidance %s carries a non-object branch", ev.skillRef.ArtifactDigest)
			}
			branchID, _ := contract.AsString(branch["branch_id"])
			if branchID == "" {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid, "guidance %s carries a branch without branch_id", ev.skillRef.ArtifactDigest)
			}
			if seenBranches[branchID] {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid,
					"guidance %s repeats branch_id %q; branch ids are unique per revision", ev.skillRef.ArtifactDigest, branchID)
			}
			seenBranches[branchID] = true
			digest, err := contract.DigestOf(branch)
			if err != nil {
				return nil, newError(s.registry, ReasonDigestMismatch, "branch %s canonicalization: %v", branchID, err)
			}
			mutations = append(mutations,
				upsertBranchNode{Node: BranchNode{Revision: ev.skillRef, BranchID: branchID, BranchDigest: digest}},
				upsertEdge{Edge: Edge{Relation: RelHasBranch, From: RevisionRef(ev.skillRef), To: BranchRef(ev.skillRef, branchID), Source: source}},
			)
		}
	case "composite":
		children, _ := contract.AsArray(bodyObj["children"])
		for _, raw := range children {
			child, _ := contract.AsObject(raw)
			if child == nil {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid, "composite %s carries a non-object child", ev.skillRef.ArtifactDigest)
			}
			childRaw, _ := contract.AsObject(child["skill_ref"])
			childRef, err := contract.ParseSkillArtifactRef(childRaw)
			if err != nil {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid,
					"composite %s child skill_ref is not an exact §7.3 ref: %v", ev.skillRef.ArtifactDigest, err)
			}
			if _, ok := staged.revisions[RevisionRef(childRef).String()]; !ok {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid,
					"composite %s references child %s which was never activated; a floating child blocks (GMS §9.3)", ev.skillRef.ArtifactDigest, childRef.LineageID)
			}
			mutations = append(mutations,
				upsertEdge{Edge: Edge{Relation: RelComposes, From: RevisionRef(ev.skillRef), To: RevisionRef(childRef), Source: source}},
			)
		}
	}
	// depends_on: only from an explicitly declared dependency list in the
	// body (never inferred — GMS §9.2). The frozen v1 artifact schemas carry
	// no such field, so no v1 body produces a depends_on edge; the rule
	// exists so a future schema version cannot smuggle inferred edges past
	// the projector.
	if raw, present := envelope["depends_on"]; present {
		declared, ok := contract.AsArray(raw)
		if !ok {
			return nil, newError(s.registry, ReasonProjectionRelationInvalid, "artifact %s declares a non-array depends_on", ev.skillRef.ArtifactDigest)
		}
		for _, item := range declared {
			itemObj, _ := contract.AsObject(item)
			deps, err := contract.ParseSkillArtifactRef(itemObj)
			if err != nil {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid,
					"artifact %s depends_on entry is not an exact §7.3 ref: %v", ev.skillRef.ArtifactDigest, err)
			}
			if _, ok := staged.revisions[RevisionRef(deps).String()]; !ok {
				return nil, newError(s.registry, ReasonProjectionRelationInvalid,
					"artifact %s depends on %s which was never projected (missing endpoint)", ev.skillRef.ArtifactDigest, deps.LineageID)
			}
			mutations = append(mutations,
				upsertEdge{Edge: Edge{Relation: RelDependsOn, From: RevisionRef(ev.skillRef), To: RevisionRef(deps), Source: source}},
			)
		}
	}
	if err := detectStructuralCycle(staged, mutations); err != nil {
		return nil, err
	}
	return mutations, nil
}

// detectStructuralCycle rejects a composes/depends_on closure that closes a
// cycle (GMS §9.3; COMPOSITE_CYCLE for composed children, DEPENDENCY_CYCLE
// for declared dependencies). Edges staged in this unit are included in the
// closure.
func detectStructuralCycle(staged *graphState, mutations []Mutation) error {
	adjacency := map[string]map[string]string{}
	addEdge := func(from, to, relation string) {
		if relation != RelComposes && relation != RelDependsOn {
			return
		}
		if adjacency[from] == nil {
			adjacency[from] = map[string]string{}
		}
		adjacency[from][to] = relation
	}
	for _, edge := range staged.edges {
		addEdge(edge.From.String(), edge.To.String(), edge.Relation)
	}
	for _, mutation := range mutations {
		if upsert, ok := mutation.(upsertEdge); ok {
			addEdge(upsert.Edge.From.String(), upsert.Edge.To.String(), upsert.Edge.Relation)
		}
	}
	visiting := map[string]bool{}
	done := map[string]bool{}
	var visit func(node string) error
	visit = func(node string) error {
		if done[node] {
			return nil
		}
		if visiting[node] {
			code := ReasonCompositeCycle
			for _, relation := range adjacency[node] {
				if relation == RelDependsOn {
					code = ReasonDependencyCycle
				}
			}
			return newError(nil, code,
				"composes/depends_on closure reaches %s twice: a structural cycle blocks the projection (GMS §9.3)", node)
		}
		visiting[node] = true
		defer func() { visiting[node] = false }()
		for next := range adjacency[node] {
			if err := visit(next); err != nil {
				return err
			}
		}
		done[node] = true
		return nil
	}
	// Deterministic start order over the union of endpoints.
	var nodes []string
	for from, tos := range adjacency {
		nodes = append(nodes, from)
		for to := range tos {
			nodes = append(nodes, to)
		}
	}
	sortStrings(nodes)
	for _, node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

// similarityMutations derives the canonical symmetric similar_to edge of
// one committed Contract §7.21 assessment (GMS §9.2/§12.9; the edge carries
// the exact assessment record ref and digest for audit). Both endpoints
// MUST already be Runtime revision nodes — an assessment can never pull a
// non-activated revision into the Runtime Graph.
func (s *Service) similarityMutations(staged *graphState, record SourceRecord) ([]Mutation, error) {
	value, err := contract.ParseJSONStrict(record.Canonical)
	if err != nil {
		return nil, newError(s.registry, ledger.ReasonInvalidJSON, "similarity record %s is not valid JSON: %v", record.RecordID, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		return nil, newError(s.registry, ReasonSchemaFieldUnknown, "similarity record %s is not an object", record.RecordID)
	}
	if sv, _ := contract.AsString(obj["schema_version"]); sv != contract.SchemaSimilarityAssessment {
		return nil, newError(s.registry, ReasonProjectionSchemaUnsupported,
			"similarity record %s schema_version %q is not %q", record.RecordID, sv, contract.SchemaSimilarityAssessment)
	}
	if err := s.gates.ValidateShape(obj, similarityAssessmentSchema); err != nil {
		return nil, newError(s.registry, ReasonProjectionSchemaUnsupported,
			"similarity record %s failed its authority shape: %v", record.RecordID, err)
	}
	// Delivery integrity: the record bytes must canonicalize to the digest
	// the source committed (the internal assessment_digest is corpus data
	// under the §7.21 x-digest rule, not the ledger payload digest).
	recordCanonical, err := contract.JCS(contract.NormalizeForHashing(value))
	if err != nil {
		return nil, newError(s.registry, ReasonDigestMismatch, "similarity record %s cannot canonicalize: %v", record.RecordID, err)
	}
	if contract.DigestBytes(recordCanonical) != record.Digest {
		return nil, newError(s.registry, ReasonDigestMismatch,
			"similarity record %s canonicalizes to %s but the source committed %s", record.RecordID, contract.DigestBytes(recordCanonical), record.Digest)
	}
	sources, _ := contract.AsArray(obj["source_skill_refs"])
	if len(sources) != 2 {
		return nil, newError(s.registry, ReasonProjectionRelationInvalid,
			"similarity record %s carries %d source refs; similar_to is a canonical symmetric pair", record.RecordID, len(sources))
	}
	var pair [2]contract.SkillArtifactRef
	for i, raw := range sources {
		rawObj, _ := contract.AsObject(raw)
		ref, err := contract.ParseSkillArtifactRef(rawObj)
		if err != nil {
			return nil, newError(s.registry, ReasonRefMismatch, "similarity record %s source[%d] invalid: %v", record.RecordID, i, err)
		}
		pair[i] = ref
	}
	canonical := similarity.CanonicalPair(pair[0], pair[1])
	for _, endpoint := range canonical {
		if _, ok := staged.revisions[RevisionRef(endpoint).String()]; !ok {
			return nil, newError(s.registry, ReasonProjectionRelationInvalid,
				"similarity record %s endpoints must be Runtime revision nodes; %s v%s was never projected", record.RecordID, endpoint.LineageID, endpoint.Version)
		}
	}
	return []Mutation{
		upsertEdge{Edge: Edge{
			Relation: RelSimilarTo,
			From:     RevisionRef(canonical[0]),
			To:       RevisionRef(canonical[1]),
			Source:   SourceRef{Source: SourceSimilarity, Stream: record.RecordID, Sequence: record.Sequence, RecordID: record.RecordID, Digest: record.Digest},
		}},
		recordSourceDigest{Source: SourceSimilarity, Sequence: record.Sequence, Digest: record.Digest},
	}, nil
}

// The private claim-assessment record shape (GMS §2.8 evidence/claim
// assessment source; a GMS-private record, not a shared Contract §7 DTO).
// One record binds an exact revision-scoped branch to one committed
// evidence ref under a versioned assessor; the projector derives a
// supported_by/refuted_by edge that keeps the exact source record for audit
// (GMS §9.5: new assessments append new edge versions, never rewrite).
const schemaClaimAssessment = "gms.claim-assessment.v1"

// assessmentMutations derives the supported_by/refuted_by edge of one
// claim-assessment record. The branch endpoint MUST be a projected branch
// node (candidate/rejected endpoints cannot appear — the shape only
// addresses released revisions); the evidence ref MUST resolve committed
// through the wired EvidenceResolver, else the projection blocks with
// EVIDENCE_NOT_COMMITTED (GMS §9.2/§9.6).
func (s *Service) assessmentMutations(staged *graphState, record SourceRecord) ([]Mutation, error) {
	value, err := contract.ParseJSONStrict(record.Canonical)
	if err != nil {
		return nil, newError(s.registry, ledger.ReasonInvalidJSON, "claim assessment %s is not valid JSON: %v", record.RecordID, err)
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		return nil, newError(s.registry, ReasonSchemaFieldUnknown, "claim assessment %s is not an object", record.RecordID)
	}
	for _, field := range []string{"schema_version", "assessment_id", "assessment_digest", "branch_ref", "assessment_kind", "evidence_ref", "assessor_ref"} {
		if _, present := obj[field]; !present {
			return nil, newError(s.registry, ReasonSchemaRequiredFieldMissing, "claim assessment %s misses %q", record.RecordID, field)
		}
	}
	if sv, _ := contract.AsString(obj["schema_version"]); sv != schemaClaimAssessment {
		return nil, newError(s.registry, ReasonProjectionSchemaUnsupported,
			"claim assessment %s schema_version %q is not %q", record.RecordID, sv, schemaClaimAssessment)
	}
	branchRaw, _ := contract.AsObject(obj["branch_ref"])
	if branchRaw == nil {
		return nil, newError(s.registry, ReasonSchemaFieldUnknown, "claim assessment %s branch_ref must be an object", record.RecordID)
	}
	revisionRaw, _ := contract.AsObject(branchRaw["source_skill_ref"])
	revision, err := contract.ParseSkillArtifactRef(revisionRaw)
	if err != nil {
		return nil, newError(s.registry, ReasonRefMismatch,
			"claim assessment %s branch endpoint is not an exact §7.3 ref (candidate/rejected endpoints are rejected): %v", record.RecordID, err)
	}
	branchID, _ := contract.AsString(branchRaw["branch_id"])
	if branchID == "" {
		return nil, newError(s.registry, ReasonSchemaRequiredFieldMissing, "claim assessment %s branch_id required", record.RecordID)
	}
	if _, ok := staged.branches[BranchRef(revision, branchID).String()]; !ok {
		return nil, newError(s.registry, ReasonProjectionRelationInvalid,
			"claim assessment %s targets branch %s/%s which was never projected", record.RecordID, revision.LineageID, branchID)
	}
	kind, _ := contract.AsString(obj["assessment_kind"])
	if kind != "supports" && kind != "refutes" {
		return nil, newError(s.registry, ReasonSchemaEnumInvalid,
			"claim assessment %s assessment_kind %q outside {supports, refutes}", record.RecordID, kind)
	}
	evidenceRaw, _ := contract.AsObject(obj["evidence_ref"])
	evidence, err := contract.ParseEvidenceRef(evidenceRaw)
	if err != nil {
		return nil, newError(s.registry, ReasonRefMismatch, "claim assessment %s evidence_ref invalid: %v", record.RecordID, err)
	}
	if err := s.requireCommittedEvidence(evidence); err != nil {
		return nil, err
	}
	relation := RelSupportedBy
	if kind == "refutes" {
		relation = RelRefutedBy // refutation keeps the branch and the evidence (GMS §9.2)
	}
	source := SourceRef{Source: SourceEvidenceAssessment, Stream: record.RecordID, Sequence: record.Sequence, RecordID: record.RecordID, Digest: record.Digest}
	return []Mutation{
		upsertRefVertex{Vertex: RefVertex{Type: "evidence", RefID: evidence.EvidenceID, Digest: evidence.EvidenceDigest}},
		upsertEdge{Edge: Edge{Relation: relation, From: BranchRef(revision, branchID), To: EvidenceVertexRef(evidence.EvidenceID, evidence.EvidenceDigest), Source: source}},
		recordSourceDigest{Source: SourceEvidenceAssessment, Sequence: record.Sequence, Digest: record.Digest},
	}, nil
}

// requireCommittedEvidence resolves one exact EvidenceRef through the wired
// EvidenceResolver: staged/rejected/inconclusive or unresolvable evidence
// blocks with EVIDENCE_NOT_COMMITTED (GMS §2.2/§9.2; fail-closed list
// "uncommitted evidence endpoint").
func (s *Service) requireCommittedEvidence(ref contract.EvidenceRef) error {
	if s.evidence == nil {
		return newError(s.registry, ReasonEvidenceNotCommitted,
			"no evidence resolver wired: assessment evidence %s cannot be proven committed and the projection blocks fail closed", ref.EvidenceID)
	}
	committed, ok, err := s.evidence.GetEvidence(ref.EvidenceID)
	if err != nil {
		return err
	}
	if !ok || committed.EvidenceDigest != ref.EvidenceDigest || committed.Version != ref.Version ||
		(committed.CommitState != "committed" && committed.CommitState != "sealed") {
		return newError(s.registry, ReasonEvidenceNotCommitted,
			"assessment evidence %s v%s (%s) does not resolve to a committed/sealed EvidenceRef (GMS §9.2)", ref.EvidenceID, ref.Version, ref.EvidenceDigest)
	}
	return nil
}

// integerEquals reports whether a decoder-model JSON number equals seq.
func integerEquals(raw any, seq uint64) bool {
	number, ok := raw.(json.Number)
	if !ok {
		return false
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		return false
	}
	return value == seq
}

func sortStrings(items []string) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j] < items[j-1]; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
}

// Authority schema file names and event types (frozen in
// $FIX/schema/shared; the event-type set mirrors the activation package).
const (
	activationSchemaActivate      = "activation-event.schema.json"
	activationSchemaDeactivate    = "deactivation-event.schema.json"
	similarityAssessmentSchema    = "similarity-assessment.schema.json"
	projectionWatermarkSchemaFile = "projection-watermark.schema.json"
	activationTypeActivate        = "activate"
	activationTypeDeactivate      = "deactivate"
)
