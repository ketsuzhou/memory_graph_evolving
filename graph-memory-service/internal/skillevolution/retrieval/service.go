// service.go is the GMS-206 retrieval service core: the three v1 Memory
// tool entry points (memory_explore / memory_expand / skill_get) over the
// authoritative read side, with ExploreSession/served-fence continuation,
// budget allocation with the typed omitted-refs carrier, the closed
// GuidanceView binding and the §12.7.1 matrix self-validation of every
// success payload (fail-closed order documented on the package).
package retrieval

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
)

// ToolRequest is one Contract §7.18 ToolProxyRequest (the Host wire shape;
// decoded and closed-validated by the HTTP layer, passed here as the
// authoritative request identity).
type ToolRequest struct {
	ProxyRequestID string
	RoomID         string
	AgentID        string
	DeliveryID     string
	ToolName       string
	Arguments      map[string]any
	ScopeProfile   contract.VersionedRef
	IdempotencyKey string
	// RequestedMinActivationSequence is the request freshness floor
	// (0 = absent; HasMinActivationSequence distinguishes 0-present).
	RequestedMinActivationSequence uint64
	HasMinActivationSequence       bool
	TimeoutMillis                  uint64
}

// Response is the exact upstream typed payload for the Host adapter
// (HST-201 Upstream port): the success result, the authoritative read
// audit record and the canonical result digest (idempotency identity).
type Response struct {
	ToolName             string
	Status               string
	Result               map[string]any
	ReadAudit            map[string]any
	UpstreamResultDigest string
}

// Config wires the retrieval service (every dependency is an authority
// product: GMS-205 projection, GMS-204 heads, GMS-102 store, GMS-202 gate).
type Config struct {
	Projection RuntimeProjection
	Heads      ActiveHeads
	Artifacts  ArtifactBytes
	Gate       ArtifactGate
	Evidence   EvidenceResolver
	Gates      SchemaGate
	Registry   ledger.ReasonRegistry
	Policy     *ToolPolicy
	// SkillGetEnabled is the M6 readiness-gate state: VerifySkillGetReadiness
	// MUST be green before this is true (disabled-until-green).
	SkillGetEnabled bool
}

// exploreSession is the server-side ExploreSession state (Contract §12.1):
// scope binding, per-query exact-page cache, cumulative served sets and
// fences, and the page watermark lineage (C6 strict advancement).
type exploreSession struct {
	id                 string
	roomID             string
	agentID            string
	scope              contract.VersionedRef
	runtimeContextHash string
	ranker             versionedRef

	pages          int
	lastWatermark  uint64
	queries        map[string]*Response      // explore query digest -> cached exact page
	expansions     map[string]*Response      // expansion identity -> cached exact page
	servedEvidence map[string]map[string]any // canonical key -> exact ref doc
	servedSkills   map[string]map[string]any // canonical key -> exact ref doc
	evidenceFence  string
	skillFence     string
	viewHashes     map[string]string // skill canonical key -> served view_hash
}

// Service is the retrieval service.
type Service struct {
	projection      RuntimeProjection
	heads           ActiveHeads
	artifacts       ArtifactBytes
	gate            ArtifactGate
	evidence        EvidenceResolver
	gates           SchemaGate
	registry        ledger.ReasonRegistry
	policy          *ToolPolicy
	skillGetEnabled bool

	mu       sync.Mutex
	sessions map[string]*exploreSession
}

// NewService fails closed at construction time unless every dependency is
// wired, the matrix policy is loaded and every reason code the service can
// emit exists in the digest-verified registry (Contract §13.7.1 R5).
func NewService(cfg Config) (*Service, error) {
	if cfg.Projection == nil || cfg.Heads == nil || cfg.Artifacts == nil ||
		cfg.Gate == nil || cfg.Evidence == nil || cfg.Gates == nil ||
		cfg.Registry == nil || cfg.Policy == nil {
		return nil, newError(ReasonToolResultBindingInvalid, "nil dependency in retrieval config")
	}
	for _, code := range []string{
		ReasonToolUnsupported, ReasonToolArgumentsInvalid, ReasonToolResultBindingInvalid,
		ReasonExploreSessionInvalid, ReasonExploreScopeViolation, ReasonExploreQueryInvalid,
		ReasonExploreContinuationInvalid, ReasonExploreFilterInvalid, ReasonExploreFenceConflict,
		ReasonExpandTargetNotServed, ReasonExpandSourceViewMismatch, ReasonExpandDepthUnsupported,
		ReasonSkillNotCurrentActive, ReasonHistoricalReadNotAuthorized, ReasonGuidanceRenderFailed,
		ReasonGuidanceViewHashMismatch, ReasonBudgetInvalid, ReasonCitationInvalid,
		ReasonProjectionBehindRequiredSequence, ReasonNonExactRef, ReasonCandidateNotExecutable,
		ReasonEvidenceNotCommitted, ReasonArtifactBodyInvalid, ReasonSchemaVersionUnsupported,
		ReasonNoActiveHead, contract.ReasonUnknownRequiredExtension,
	} {
		if err := cfg.Registry.Verify(code); err != nil {
			return nil, newError(ReasonToolResultBindingInvalid, "reason code outside the closed registry: %v", err)
		}
	}
	return &Service{
		projection:      cfg.Projection,
		heads:           cfg.Heads,
		artifacts:       cfg.Artifacts,
		gate:            cfg.Gate,
		evidence:        cfg.Evidence,
		gates:           cfg.Gates,
		registry:        cfg.Registry,
		policy:          cfg.Policy,
		skillGetEnabled: cfg.SkillGetEnabled,
		sessions:        map[string]*exploreSession{},
	}, nil
}

// SkillGetEnabled reports the M6 readiness-gate state.
func (s *Service) SkillGetEnabled() bool { return s.skillGetEnabled }

// profileFloor resolves one policy-frozen profile version floor.
func (p *ToolPolicy) profileFloor(tool, field string) (int64, bool) {
	binding, ok := p.tools[tool]
	if !ok {
		return 0, false
	}
	floor, ok := binding.profileConstraints["min_"+field+"_version"]
	return floor, ok
}

// ---------------------------------------------------------------------------
// memory_explore (GMS §10.2)
// ---------------------------------------------------------------------------

// Explore serves one deterministic explore page over the Runtime projection.
func (s *Service) Explore(ctx context.Context, req ToolRequest) (*Response, error) {
	if req.ToolName != ToolExplore {
		return nil, newError(ReasonToolUnsupported, "tool %q is not bound in the closed v1 set", req.ToolName)
	}
	args, err := s.parseExploreArguments(req.Arguments)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.bindSession(req, args.SessionID, args.RuntimeContextHash, args.RankerPolicy)
	if session == nil {
		return nil, newError(ReasonExploreScopeViolation,
			"ExploreSession %q is bound to another room/agent/scope (Contract 12.1)", args.SessionID)
	}

	snapshot := s.projection.Snapshot()
	watermark, _, _ := s.projection.Watermark()
	if watermark == nil {
		return nil, newError(ReasonNoActiveHead, "no projection watermark; the read side is not servable")
	}
	sequence, err := watermarkSequence(watermark)
	if err != nil {
		return nil, newError(ReasonNoActiveHead, "watermark carries no integer sequence: %v", err)
	}
	if err := s.checkFreshness(req, sequence); err != nil {
		return nil, err
	}

	// Retry identity: the exact page of this query, served from the cache —
	// the fences are not re-consumed (same digest, same fences).
	if cached, ok := session.queries[args.QueryDigest]; ok {
		if args.HasServedFences {
			if err := session.checkFences(args.ServedFences); err != nil {
				return nil, err
			}
		}
		return cloneResponse(cached), nil
	}
	// Continuation: a session that already served a page MUST continue with
	// the exact cumulative fences, and a new page advances the watermark
	// strictly (Contract §12.7.2 C6).
	if session.pages > 0 {
		if !args.HasServedFences {
			return nil, newError(ReasonExploreFenceConflict,
				"ExploreSession %q already served a page; a new page must continue with the served fences", args.SessionID)
		}
		if err := session.checkFences(args.ServedFences); err != nil {
			return nil, err
		}
		if sequence <= session.lastWatermark {
			return nil, newError(ReasonExploreFenceConflict,
				"page watermark %d did not advance past the session watermark %d (C6)", sequence, session.lastWatermark)
		}
	}

	response, err := s.buildExplorePage(req, session, args, snapshot, watermark, sequence)
	if err != nil {
		return nil, err
	}
	session.lastWatermark = sequence
	session.queries[args.QueryDigest] = response
	session.pages++
	return cloneResponse(response), nil
}

// buildExplorePage ranks, allocates and renders one explore result page.
func (s *Service) buildExplorePage(req ToolRequest, session *exploreSession, args *exploreArguments, snapshot projector.GraphSnapshot, watermark map[string]any, sequence uint64) (*Response, error) {
	skills, evidence, err := s.collectCandidates(snapshot, args.Filters, args.QueryTerms)
	if err != nil {
		return nil, err
	}
	views := make(map[string]*renderedView, len(skills))
	branches := make(map[string][]guidanceBranch, len(skills))
	for _, skill := range skills {
		list := guidanceBranches(skill.envelope)
		branches[skillKey(skill.ref)] = list
		view, err := renderGuidanceView(skill, list, s.defaultRenderProfile(), s.defaultGuidancePolicy(), args.RuntimeContextHash, args.Budgets.GuidanceTokenBudget)
		if err != nil {
			return nil, err
		}
		views[skillKey(skill.ref)] = view
	}
	rankScores(skills, evidence, args.QueryTerms, snapshot)
	rankOrderSkills(skills)
	rankOrderEvidence(evidence)

	// Merged serve order: total desc, evidence class before skill class,
	// then the per-class frozen chains.
	merged := mergeCandidates(skills, evidence)
	alloc := &allocation{
		totalCap:       args.Budgets.TotalCap,
		evidenceSubcap: args.Budgets.EvidenceSubcap,
		skillSubcap:    args.Budgets.SkillSubcap,
		tokenBudget:    args.Budgets.GuidanceTokenBudget,
	}
	for _, candidate := range merged {
		switch candidate.class {
		case "evidence":
			// A ref already served earlier in the session is never re-served.
			if session.servedEvidence[contract.CanonicalKey(evidenceRefDoc(candidate.evidence.ref))] != nil {
				continue
			}
			alloc.takeEvidence(candidate.evidence)
		case "skill":
			key := skillKey(candidate.skill.ref)
			if session.servedSkills[contract.CanonicalKey(skillRefDoc(candidate.skill.ref))] != nil {
				continue
			}
			alloc.takeSkill(candidate.skill, views[key].tokens)
		}
	}
	alloc.finalizeOmissions()

	// Commit the session page BEFORE rendering: the page's served_fences
	// are the cumulative fences INCLUDING this page's served refs, so the
	// Host continues the next page with exactly what this result carries.
	for _, served := range alloc.servedEvidence {
		doc := evidenceRefDoc(served.ref)
		session.servedEvidence[contract.CanonicalKey(doc)] = doc
	}
	for _, served := range alloc.servedSkills {
		doc := skillRefDoc(served.ref)
		session.servedSkills[contract.CanonicalKey(doc)] = doc
		session.viewHashes[skillKey(served.ref)] = views[skillKey(served.ref)].doc["view_hash"].(string)
	}
	session.refreshFences()

	result, err := s.renderExploreResult(session, args, alloc, views, watermark, req)
	if err != nil {
		return nil, err
	}
	audit := s.readAudit(req, watermark, sequence)
	if err := s.selfValidate(ToolExplore, result, req, audit); err != nil {
		return nil, err
	}
	digest, err := contract.DigestOf(result)
	if err != nil {
		return nil, newError(ReasonToolResultBindingInvalid, "explore result cannot canonicalize: %v", err)
	}
	return &Response{ToolName: ToolExplore, Status: "success", Result: result, ReadAudit: audit, UpstreamResultDigest: digest}, nil
}

// renderExploreResult assembles the closed ExploreResult document.
func (s *Service) renderExploreResult(session *exploreSession, args *exploreArguments, alloc *allocation, views map[string]*renderedView, watermark map[string]any, req ToolRequest) (map[string]any, error) {
	evidenceResults := make([]any, 0, len(alloc.servedEvidence))
	for _, served := range alloc.servedEvidence {
		doc := evidenceRefDoc(served.ref)
		evidenceResults = append(evidenceResults, map[string]any{
			"result_type":       "evidence",
			"evidence_ref":      doc,
			"rank_score_micros": json.Number(fmt.Sprintf("%d", served.total)),
			"citation": map[string]any{
				"evidence_ref": doc,
				"claim":        served.claim,
			},
		})
	}
	skillResults := make([]any, 0, len(alloc.servedSkills))
	for _, served := range alloc.servedSkills {
		doc := skillRefDoc(served.ref)
		view := views[skillKey(served.ref)].doc
		citations := make([]any, 0)
		for _, ref := range served.refs {
			citations = append(citations, map[string]any{
				"evidence_ref": evidenceRefDoc(ref),
				"claim":        fmt.Sprintf("released provenance of %s v%s cites this committed evidence", served.ref.LineageID, served.ref.Version),
			})
		}
		skillResults = append(skillResults, map[string]any{
			"result_type":       "skill",
			"skill_ref":         doc,
			"rank_score_micros": json.Number(fmt.Sprintf("%d", served.total)),
			"guidance_view":     view,
			"artifact_identity_citation": map[string]any{
				"skill_ref": doc,
			},
			"evidence_citations": citations,
		})
	}
	result := map[string]any{
		"schema_version":     "gms.explore-result.v1",
		"explore_session_id": args.SessionID,
		"query_digest":       args.QueryDigest,
		"ranker_policy_ref":  args.RankerPolicy.asDoc(),
		"watermark":          deepCopyDoc(watermark),
		"evidence_results":   evidenceResults,
		"skill_results":      skillResults,
		"served_fences": map[string]any{
			"evidence_fence_digest": session.evidenceFence,
			"skill_fence_digest":    session.skillFence,
		},
		"budgets": map[string]any{
			"total_cap":             json.Number(fmt.Sprintf("%d", args.Budgets.TotalCap)),
			"evidence_subcap":       json.Number(fmt.Sprintf("%d", args.Budgets.EvidenceSubcap)),
			"skill_subcap":          json.Number(fmt.Sprintf("%d", args.Budgets.SkillSubcap)),
			"guidance_token_budget": json.Number(fmt.Sprintf("%d", args.Budgets.GuidanceTokenBudget)),
			"total_used":            json.Number(fmt.Sprintf("%d", alloc.usedTotal)),
		},
		"truncation_reason_codes": alloc.truncationCodes(),
		"omissions":               alloc.omissionDocs(),
	}
	if req.HasMinActivationSequence {
		result["min_activation_sequence"] = json.Number(fmt.Sprintf("%d", req.RequestedMinActivationSequence))
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// memory_expand (GMS §10.3)
// ---------------------------------------------------------------------------

// Expand serves one depth-1 expansion around a legally exposed target.
func (s *Service) Expand(ctx context.Context, req ToolRequest) (*Response, error) {
	if req.ToolName != ToolExpand {
		return nil, newError(ReasonToolUnsupported, "tool %q is not bound in the closed v1 set", req.ToolName)
	}
	args, err := s.parseExpandArguments(req.Arguments)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[args.SessionID]
	if !ok {
		return nil, newError(ReasonExploreSessionInvalid,
			"ExploreSession %q was never opened; memory_expand must continue one", args.SessionID)
	}
	if !session.sameScope(req) {
		return nil, newError(ReasonExploreScopeViolation,
			"ExploreSession %q is bound to another room/agent/scope (Contract 12.1)", args.SessionID)
	}
	// Fences must continue the session exactly (before continuation
	// resolution: a wrong fence is a fence conflict even for an unknown
	// source query).
	if err := session.checkFences(args.ServedFences); err != nil {
		return nil, err
	}
	if _, served := session.queries[args.SourceQueryDigest]; !served {
		return nil, newError(ReasonExploreContinuationInvalid,
			"source_query_digest %s was not served by ExploreSession %q", args.SourceQueryDigest, args.SessionID)
	}

	snapshot := s.projection.Snapshot()
	watermark, _, _ := s.projection.Watermark()
	if watermark == nil {
		return nil, newError(ReasonNoActiveHead, "no projection watermark; the read side is not servable")
	}
	sequence, err := watermarkSequence(watermark)
	if err != nil {
		return nil, newError(ReasonNoActiveHead, "watermark carries no integer sequence: %v", err)
	}
	if err := s.checkFreshness(req, sequence); err != nil {
		return nil, err
	}
	// An expansion may not travel back behind the session's page watermark
	// (strict advancement binds explore pages; expansions observe the same
	// or a newer head).
	if sequence < session.lastWatermark {
		return nil, newError(ReasonExploreFenceConflict,
			"expansion watermark %d is behind the session watermark %d", sequence, session.lastWatermark)
	}

	cacheKey := expansionIdentity(args)
	if cached, ok := session.expansions[cacheKey]; ok {
		return cloneResponse(cached), nil
	}

	response, err := s.buildExpandPage(req, session, args, snapshot, watermark, sequence)
	if err != nil {
		return nil, err
	}
	session.expansions[cacheKey] = response
	return cloneResponse(response), nil
}

// buildExpandPage resolves the target and renders one expansion page.
func (s *Service) buildExpandPage(req ToolRequest, session *exploreSession, args *expandArguments, snapshot projector.GraphSnapshot, watermark map[string]any, sequence uint64) (*Response, error) {
	var skills []*skillCandidate
	var evidence []*evidenceCandidate
	switch args.Target.RefType {
	case "branch":
		target := args.Target.Branch
		branchKey := projector.BranchRef(target.Revision, target.BranchID).String()
		node, exists := snapshot.Branches[branchKey]
		if !exists {
			return nil, newError(ReasonExpandTargetNotServed,
				"branch %s of %s v%s was not projected; the Graph cannot serve it", target.BranchID, target.Revision.LineageID, target.Revision.Version)
		}
		if node.BranchDigest != target.BranchDigest {
			return nil, newError(ReasonExpandTargetNotServed,
				"branch %s digest %s disagrees with the projected %s", target.BranchID, target.BranchDigest, node.BranchDigest)
		}
		if servedHash, ok := session.viewHashes[skillKey(target.Revision)]; ok && servedHash != args.SourceViewHash {
			return nil, newError(ReasonExpandSourceViewMismatch,
				"source_view_hash does not match the served guidance view of %s v%s", target.Revision.LineageID, target.Revision.Version)
		}
		// Depth-1 neighborhood: the evidence attached to this branch through
		// its claim assessments (supported_by / refuted_by).
		for _, edge := range snapshot.Edges {
			if edge.From.String() != branchKey {
				continue
			}
			if edge.Relation != projector.RelSupportedBy && edge.Relation != projector.RelRefutedBy {
				continue
			}
			if edge.To.Kind != projector.NodeEvidenceVertex {
				continue
			}
			candidate, err := s.evidenceCandidateFor(edge.To.RefID, edge.To.Digest, snapshot,
				fmt.Sprintf("claim assessment %s: branch %s of %s v%s", edge.Relation, target.BranchID, target.Revision.LineageID, target.Revision.Version))
			if err != nil {
				return nil, err
			}
			evidence = append(evidence, candidate)
		}
	case "evidence":
		target := args.Target.Evidence
		candidate, err := s.evidenceCandidateFor(target.EvidenceID, target.EvidenceDigest, snapshot,
			fmt.Sprintf("expanded committed evidence %s", target.EvidenceID))
		if err != nil {
			return nil, err
		}
		evidence = append(evidence, candidate)
	case "child_skill":
		target := args.Target.Skill
		node, exists := snapshot.Revisions[projector.RevisionRef(*target).String()]
		if !exists || node.Lifecycle != projector.LifecycleActive {
			return nil, newError(ReasonExpandTargetNotServed,
				"child skill %s v%s is not an active Runtime revision", target.LineageID, target.Version)
		}
		candidate, err := s.skillCandidateFor(node)
		if err != nil {
			return nil, err
		}
		skills = append(skills, candidate)
	case "checkpoint":
		return nil, newError(ReasonExpandTargetNotServed, "checkpoint expansion is not servable in v1")
	}

	rankScores(skills, evidence, []string{}, snapshot)
	rankOrderSkills(skills)
	rankOrderEvidence(evidence)

	views := make(map[string]*renderedView, len(skills))
	for _, skill := range skills {
		view, err := renderGuidanceView(skill, guidanceBranches(skill.envelope), s.defaultRenderProfile(), s.defaultGuidancePolicy(), args.RuntimeContextHash, args.Budgets.GuidanceTokenBudget)
		if err != nil {
			return nil, err
		}
		views[skillKey(skill.ref)] = view
	}
	merged := mergeCandidates(skills, evidence)
	alloc := &allocation{
		totalCap:       args.Budgets.TotalCap,
		evidenceSubcap: args.Budgets.EvidenceSubcap,
		skillSubcap:    args.Budgets.SkillSubcap,
		tokenBudget:    args.Budgets.GuidanceTokenBudget,
	}
	for _, candidate := range merged {
		switch candidate.class {
		case "evidence":
			if session.servedEvidence[contract.CanonicalKey(evidenceRefDoc(candidate.evidence.ref))] != nil {
				continue // fences forbid re-serving a session-served ref
			}
			alloc.takeEvidence(candidate.evidence)
		case "skill":
			if session.servedSkills[contract.CanonicalKey(skillRefDoc(candidate.skill.ref))] != nil {
				continue
			}
			alloc.takeSkill(candidate.skill, views[skillKey(candidate.skill.ref)].tokens)
		}
	}
	alloc.finalizeOmissions()

	evidenceResults := make([]any, 0, len(alloc.servedEvidence))
	for _, served := range alloc.servedEvidence {
		doc := evidenceRefDoc(served.ref)
		evidenceResults = append(evidenceResults, map[string]any{
			"result_type":       "evidence",
			"evidence_ref":      doc,
			"rank_score_micros": json.Number(fmt.Sprintf("%d", served.total)),
			"citation": map[string]any{
				"evidence_ref": doc,
				"claim":        served.claim,
			},
		})
	}
	skillResults := make([]any, 0, len(alloc.servedSkills))
	for _, served := range alloc.servedSkills {
		doc := skillRefDoc(served.ref)
		view := views[skillKey(served.ref)].doc
		citations := make([]any, 0)
		for _, ref := range served.refs {
			citations = append(citations, map[string]any{
				"evidence_ref": evidenceRefDoc(ref),
				"claim":        fmt.Sprintf("released provenance of %s v%s cites this committed evidence", served.ref.LineageID, served.ref.Version),
			})
		}
		skillResults = append(skillResults, map[string]any{
			"result_type":                "skill",
			"skill_ref":                  doc,
			"rank_score_micros":          json.Number(fmt.Sprintf("%d", served.total)),
			"guidance_view":              view,
			"artifact_identity_citation": map[string]any{"skill_ref": doc},
			"evidence_citations":         citations,
		})
	}
	// Commit the expansion's served refs BEFORE rendering so the cumulative
	// fences carried by this result include them (the Host continues with
	// exactly what the result carries).
	for _, served := range alloc.servedEvidence {
		doc := evidenceRefDoc(served.ref)
		session.servedEvidence[contract.CanonicalKey(doc)] = doc
	}
	for _, served := range alloc.servedSkills {
		doc := skillRefDoc(served.ref)
		session.servedSkills[contract.CanonicalKey(doc)] = doc
		session.viewHashes[skillKey(served.ref)] = views[skillKey(served.ref)].doc["view_hash"].(string)
	}
	session.refreshFences()

	result := map[string]any{
		"schema_version":     "gms.explore-result.v1",
		"explore_session_id": args.SessionID,
		"query_digest":       args.SourceQueryDigest,
		"ranker_policy_ref":  args.RankerPolicy.asDoc(),
		"watermark":          deepCopyDoc(watermark),
		"evidence_results":   evidenceResults,
		"skill_results":      skillResults,
		"served_fences": map[string]any{
			"evidence_fence_digest": session.evidenceFence,
			"skill_fence_digest":    session.skillFence,
		},
		"budgets": map[string]any{
			"total_cap":             json.Number(fmt.Sprintf("%d", args.Budgets.TotalCap)),
			"evidence_subcap":       json.Number(fmt.Sprintf("%d", args.Budgets.EvidenceSubcap)),
			"skill_subcap":          json.Number(fmt.Sprintf("%d", args.Budgets.SkillSubcap)),
			"guidance_token_budget": json.Number(fmt.Sprintf("%d", args.Budgets.GuidanceTokenBudget)),
			"total_used":            json.Number(fmt.Sprintf("%d", alloc.usedTotal)),
		},
		"truncation_reason_codes": alloc.truncationCodes(),
		"omissions":               alloc.omissionDocs(),
	}
	if req.HasMinActivationSequence {
		result["min_activation_sequence"] = json.Number(fmt.Sprintf("%d", req.RequestedMinActivationSequence))
	}

	audit := s.readAudit(req, watermark, sequence)
	if err := s.selfValidate(ToolExpand, result, req, audit); err != nil {
		return nil, err
	}
	digest, err := contract.DigestOf(result)
	if err != nil {
		return nil, newError(ReasonToolResultBindingInvalid, "expand result cannot canonicalize: %v", err)
	}
	return &Response{ToolName: ToolExpand, Status: "success", Result: result, ReadAudit: audit, UpstreamResultDigest: digest}, nil
}

// ---------------------------------------------------------------------------
// skill_get (GMS §10.4)
// ---------------------------------------------------------------------------

// SkillGet renders one closed GuidanceView of the authoritative active head
// (readiness gate M6: TOOL_UNSUPPORTED until the gate is green).
func (s *Service) SkillGet(ctx context.Context, req ToolRequest) (*Response, error) {
	if req.ToolName != ToolSkillGet {
		return nil, newError(ReasonToolUnsupported, "tool %q is not bound in the closed v1 set", req.ToolName)
	}
	if !s.skillGetEnabled {
		return nil, newError(s.policy.gateFailureCodeOrDefault(),
			"skill_get stays disabled-until-green (Contract 12.7.1 M6); the frozen corpus gate is not green")
	}
	args, err := s.parseSkillGetArguments(req.Arguments)
	if err != nil {
		return nil, err
	}
	if args.Visibility == "historical_exact" {
		// v1 carries no historical-read authorization profile on the
		// request; the only servable visibility is current_active.
		return nil, newError(ReasonHistoricalReadNotAuthorized,
			"historical_exact requires an exact Host authorization profile not carried by this request (GMS 10.4)")
	}
	if req.RoomID == "" || req.AgentID == "" || req.ScopeProfile.ID == "" {
		return nil, newError(ReasonExploreScopeViolation, "skill_get requires room/agent/scope identity")
	}

	head, err := s.heads.ActiveRevision(ctx, args.SkillRef.LineageID)
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, newError(ReasonNoActiveHead, "lineage %s has no active revision", args.SkillRef.LineageID)
	}
	if *head != args.SkillRef {
		return nil, newError(ReasonSkillNotCurrentActive,
			"skill_ref %s v%s is not the authoritative active head %s v%s of its lineage",
			args.SkillRef.LineageID, args.SkillRef.Version, head.LineageID, head.Version)
	}

	snapshot := s.projection.Snapshot()
	node, exists := snapshot.Revisions[projector.RevisionRef(args.SkillRef).String()]
	if !exists {
		return nil, newError(ReasonSkillNotCurrentActive,
			"active head %s v%s was never projected; the read side is stale", args.SkillRef.LineageID, args.SkillRef.Version)
	}

	// Freshness rides the request + the authoritative read audit record
	// (current_active checks the head's activation sequence directly).
	headSequence := node.FirstProjectedSequence
	if req.HasMinActivationSequence && req.RequestedMinActivationSequence > headSequence {
		return nil, newError(ReasonProjectionBehindRequiredSequence,
			"requested_min_activation_sequence %d exceeds the active head sequence %d",
			req.RequestedMinActivationSequence, headSequence)
	}

	candidate, err := s.skillCandidateFor(node)
	if err != nil {
		return nil, err
	}
	branches := guidanceBranches(candidate.envelope)
	if len(args.IncludedBranchRefs) > 0 {
		included := map[string]bool{}
		for _, id := range args.IncludedBranchRefs {
			included[id] = true
		}
		filtered := make([]guidanceBranch, 0, len(branches))
		for _, branch := range branches {
			if included[branch.id] {
				filtered = append(filtered, branch)
			}
		}
		branches = filtered
	}
	view, err := renderGuidanceView(candidate, branches, args.RenderProfile, args.PolicyRef, args.RuntimeContextHash, args.GuidanceTokenBudget)
	if err != nil {
		return nil, err
	}

	audit := map[string]any{
		"schema_version":                  "host.read-audit.v1",
		"proxy_request_id":                req.ProxyRequestID,
		"room_id":                         req.RoomID,
		"agent_id":                        req.AgentID,
		"scope_profile_ref":               versionedRefDoc(req.ScopeProfile),
		"active_head_activation_sequence": json.Number(fmt.Sprintf("%d", headSequence)),
	}
	if err := s.selfValidate(ToolSkillGet, view.doc, req, audit); err != nil {
		return nil, err
	}
	digest, err := contract.DigestOf(view.doc)
	if err != nil {
		return nil, newError(ReasonGuidanceRenderFailed, "guidance view cannot canonicalize: %v", err)
	}
	return &Response{
		ToolName: ToolSkillGet, Status: "success",
		Result: view.doc, ReadAudit: audit, UpstreamResultDigest: digest,
	}, nil
}

// ---------------------------------------------------------------------------
// Shared internals
// ---------------------------------------------------------------------------

// bindSession returns the session bound to this request scope, creating it
// on first use; nil means a scope conflict (fail closed).
func (s *Service) bindSession(req ToolRequest, sessionID, runtimeContextHash string, ranker versionedRef) *exploreSession {
	if req.RoomID == "" || req.AgentID == "" || req.ScopeProfile.ID == "" {
		return nil
	}
	session, ok := s.sessions[sessionID]
	if !ok {
		session = &exploreSession{
			id: sessionID, roomID: req.RoomID, agentID: req.AgentID,
			scope: req.ScopeProfile, runtimeContextHash: runtimeContextHash,
			ranker:  ranker,
			queries: map[string]*Response{}, expansions: map[string]*Response{},
			servedEvidence: map[string]map[string]any{}, servedSkills: map[string]map[string]any{},
			viewHashes:    map[string]string{},
			evidenceFence: servedFenceDigest(sessionID, "evidence", nil),
			skillFence:    servedFenceDigest(sessionID, "skill", nil),
		}
		s.sessions[sessionID] = session
		return session
	}
	if !session.sameScope(req) {
		return nil
	}
	return session
}

func (session *exploreSession) sameScope(req ToolRequest) bool {
	return session.roomID == req.RoomID && session.agentID == req.AgentID && session.scope == req.ScopeProfile
}

// checkFences verifies the request continues the session fences exactly.
func (session *exploreSession) checkFences(fences servedFences) error {
	if fences.Evidence != session.evidenceFence || fences.Skill != session.skillFence {
		return newError(ReasonExploreFenceConflict,
			"served fences do not continue ExploreSession %q exactly (expected evidence=%s skill=%s)",
			session.id, session.evidenceFence, session.skillFence)
	}
	return nil
}

// refreshFences recomputes the cumulative served-set fences.
func (session *exploreSession) refreshFences() {
	evidenceDocs := make([]map[string]any, 0, len(session.servedEvidence))
	for _, doc := range session.servedEvidence {
		evidenceDocs = append(evidenceDocs, doc)
	}
	skillDocs := make([]map[string]any, 0, len(session.servedSkills))
	for _, doc := range session.servedSkills {
		skillDocs = append(skillDocs, doc)
	}
	session.evidenceFence = servedFenceDigest(session.id, "evidence", evidenceDocs)
	session.skillFence = servedFenceDigest(session.id, "skill", skillDocs)
}

// checkFreshness applies the request freshness floor against the projection
// watermark (Contract §7.17 requested_min_activation_sequence).
func (s *Service) checkFreshness(req ToolRequest, sequence uint64) error {
	if req.HasMinActivationSequence && req.RequestedMinActivationSequence > sequence {
		return newError(ReasonProjectionBehindRequiredSequence,
			"requested_min_activation_sequence %d exceeds the projected sequence %d",
			req.RequestedMinActivationSequence, sequence)
	}
	return nil
}

// readAudit builds the authoritative read audit record of one read.
func (s *Service) readAudit(req ToolRequest, watermark map[string]any, sequence uint64) map[string]any {
	audit := map[string]any{
		"schema_version":                "host.read-audit.v1",
		"proxy_request_id":              req.ProxyRequestID,
		"room_id":                       req.RoomID,
		"agent_id":                      req.AgentID,
		"scope_profile_ref":             versionedRefDoc(req.ScopeProfile),
		"watermark_projection_sequence": json.Number(fmt.Sprintf("%d", sequence)),
	}
	return audit
}

// selfValidate runs the frozen §12.7.1 matrix over the success payload
// before it leaves the service (fail closed, never a partial result).
func (s *Service) selfValidate(toolName string, result map[string]any, req ToolRequest, audit map[string]any) error {
	request := map[string]any{
		"room_id":           req.RoomID,
		"agent_id":          req.AgentID,
		"scope_profile_ref": versionedRefDoc(req.ScopeProfile),
	}
	if req.HasMinActivationSequence {
		request["requested_min_activation_sequence"] = json.Number(fmt.Sprintf("%d", req.RequestedMinActivationSequence))
	}
	outcome := s.policy.Evaluate(toolName, result, request, audit)
	if !outcome.Accept {
		return newError(ReasonToolResultBindingInvalid,
			"success payload failed the frozen tool-success validation matrix: %s", outcome.ReasonCode)
	}
	return nil
}

// collectCandidates gathers the active-revision skill candidates and the
// committed evidence candidates of one snapshot under the filters.
func (s *Service) collectCandidates(snapshot projector.GraphSnapshot, filters exploreFilters, terms []string) ([]*skillCandidate, []*evidenceCandidate, error) {
	skills := make([]*skillCandidate, 0)
	for _, node := range snapshot.Revisions {
		if filters.ActiveOnly {
			if node.Lifecycle != projector.LifecycleActive {
				continue
			}
			if active, ok := snapshot.LineageActive[node.Ref.LineageID]; !ok || active != node.Ref {
				continue
			}
		}
		if len(filters.Kinds) > 0 && !containsString(filters.Kinds, node.Ref.Kind) {
			continue
		}
		if len(filters.LineageIDs) > 0 && !containsString(filters.LineageIDs, node.Ref.LineageID) {
			continue
		}
		if len(filters.BranchRefs) > 0 {
			owns := false
			for _, branchRef := range filters.BranchRefs {
				owner, err := contract.ParseSkillArtifactRef(mustObject(branchRef["source_skill_ref"]))
				if err == nil && owner == node.Ref {
					owns = true
					break
				}
			}
			if !owns {
				continue
			}
		}
		candidate, err := s.skillCandidateFor(node)
		if err != nil {
			return nil, nil, err
		}
		skills = append(skills, candidate)
	}
	evidence := make([]*evidenceCandidate, 0)
	for _, vertex := range snapshot.Vertices {
		if vertex.Type != "evidence" {
			continue
		}
		candidate, err := s.evidenceCandidateFor(vertex.RefID, vertex.Digest, snapshot, "")
		if err != nil {
			return nil, nil, err
		}
		evidence = append(evidence, candidate)
	}
	return skills, evidence, nil
}

// skillCandidateFor loads and re-verifies one released artifact through the
// canonical gate (GMS-202) and prepares its lexical fields.
func (s *Service) skillCandidateFor(node projector.RevisionNode) (*skillCandidate, error) {
	body, ok, err := s.artifacts.Get(node.Ref.ArtifactDigest)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newError(ReasonArtifactBodyInvalid,
			"released body %s resolves to no committed content", node.Ref.ArtifactDigest)
	}
	value, err := contract.ParseJSONStrict(body)
	if err != nil {
		return nil, newError(ReasonArtifactBodyInvalid, "released body %s is not valid JSON: %v", node.Ref.ArtifactDigest, err)
	}
	if contract.DigestBytes(body) != node.Ref.ArtifactDigest {
		return nil, newError(ReasonDigestMismatch,
			"released body under %s digests to %s", node.Ref.ArtifactDigest, contract.DigestBytes(body))
	}
	envelope, _ := contract.AsObject(value)
	if envelope == nil {
		return nil, newError(ReasonArtifactBodyInvalid, "released body %s is not an object", node.Ref.ArtifactDigest)
	}
	canonical, err := s.gate.Canonicalize(value)
	if err != nil {
		return nil, newError(ReasonArtifactBodyInvalid, "released body %s failed the canonical gate: %v", node.Ref.ArtifactDigest, err)
	}
	refs, err := canonical.EvidenceRefs()
	if err != nil {
		return nil, newError(ReasonArtifactBodyInvalid, "released body %s provenance invalid: %v", node.Ref.ArtifactDigest, err)
	}
	title, _ := contract.AsString(envelope["title"])
	description, _ := contract.AsString(envelope["description"])
	bodyObj, _ := contract.AsObject(envelope["body"])
	bodyBytes, err := contract.JCS(bodyObj)
	if err != nil {
		return nil, newError(ReasonArtifactBodyInvalid, "released body %s cannot canonicalize: %v", node.Ref.ArtifactDigest, err)
	}
	return &skillCandidate{
		ref: node.Ref, node: node, title: title, description: description,
		bodyText: string(bodyBytes), envelope: envelope, refs: refs,
	}, nil
}

// evidenceCandidateFor resolves one committed EvidenceRef behind a
// projected evidence vertex (unresolvable endpoints fail closed).
func (s *Service) evidenceCandidateFor(id, digest string, snapshot projector.GraphSnapshot, claim string) (*evidenceCandidate, error) {
	vertex, ok := snapshot.Vertices[vertexNodeRef(projector.RefVertex{Type: "evidence", RefID: id, Digest: digest}).String()]
	if !ok {
		return nil, newError(ReasonExpandTargetNotServed, "evidence %s (%s) was not projected", id, digest)
	}
	ref, found, err := s.evidence.GetEvidence(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, newError(ReasonEvidenceNotCommitted,
			"projected evidence %s does not resolve to a committed EvidenceRef", id)
	}
	if ref.CommitState != "committed" && ref.CommitState != "sealed" {
		return nil, newError(ReasonEvidenceNotCommitted,
			"projected evidence %s is %s, not committed/sealed", id, ref.CommitState)
	}
	if claim == "" {
		claim = fmt.Sprintf("committed evidence %s linked to the served projection neighborhood", id)
	}
	return &evidenceCandidate{ref: ref, vertex: vertex, claim: claim}, nil
}

// mergedCandidate is one entry of the merged serve order.
type mergedCandidate struct {
	class    string // "evidence" | "skill"
	evidence *evidenceCandidate
	skill    *skillCandidate
}

// mergeCandidates interleaves the ranked candidates into one serve order:
// total micros desc, evidence class first, then exact-ref canonical order.
func mergeCandidates(skills []*skillCandidate, evidence []*evidenceCandidate) []mergedCandidate {
	merged := make([]mergedCandidate, 0, len(skills)+len(evidence))
	for _, ev := range evidence {
		merged = append(merged, mergedCandidate{class: "evidence", evidence: ev})
	}
	for _, skill := range skills {
		merged = append(merged, mergedCandidate{class: "skill", skill: skill})
	}
	classOrder := map[string]int{"evidence": 0, "skill": 1}
	sort.SliceStable(merged, func(i, j int) bool {
		a, b := merged[i], merged[j]
		if a.total() != b.total() {
			return a.total() > b.total()
		}
		if classOrder[a.class] != classOrder[b.class] {
			return classOrder[a.class] < classOrder[b.class]
		}
		return a.refKey() < b.refKey()
	})
	return merged
}

func (c mergedCandidate) total() int64 {
	if c.class == "evidence" {
		return c.evidence.total
	}
	return c.skill.total
}

func (c mergedCandidate) refKey() string {
	if c.class == "evidence" {
		return contract.CanonicalKey(evidenceRefDoc(c.evidence.ref))
	}
	return contract.CanonicalKey(skillRefDoc(c.skill.ref))
}

// expansionIdentity is the query identity of one expansion (JCS of the
// arguments core, GMS §10.3).
func expansionIdentity(args *expandArguments) string {
	doc := map[string]any{
		"schema_version":      SchemaExpandArguments,
		"explore_session_id":  args.SessionID,
		"source_query_digest": args.SourceQueryDigest,
		"target": map[string]any{
			"ref_type": args.Target.RefType,
			"ref":      args.Target.refDoc(),
		},
		"source_view_hash":     args.SourceViewHash,
		"runtime_context_hash": args.RuntimeContextHash,
		"max_graph_depth":      json.Number("1"),
	}
	digest, err := contract.DigestOf(doc)
	if err != nil {
		return "expansion-uncanonicalizable"
	}
	return digest
}

func (t expandTarget) refDoc() map[string]any {
	switch t.RefType {
	case "branch":
		return map[string]any{
			"schema_version":   "gms.exact-branch-ref.v1",
			"source_skill_ref": skillRefDoc(t.Branch.Revision),
			"branch_id":        t.Branch.BranchID,
			"branch_digest":    t.Branch.BranchDigest,
		}
	case "evidence":
		return evidenceRefDoc(*t.Evidence)
	case "child_skill":
		return skillRefDoc(*t.Skill)
	case "checkpoint":
		return map[string]any{"checkpoint_id": t.CheckpointID}
	}
	return map[string]any{}
}

// defaultRenderProfile is the deployment-wide render profile v2 ref
// (frozen by the conformance corpus; the minimum the §12.7.1 policy serves).
func (s *Service) defaultRenderProfile() versionedRef {
	return versionedRef{
		ID:      "rsih.render-profile.default.v1",
		Version: 2,
		Digest:  "sha256:9b3794ee3e2f935abc7ab8291ab89310e04898315e992154c7e320bdd124de72",
	}
}

// defaultGuidancePolicy is the deployment-wide guidance policy v1 ref.
func (s *Service) defaultGuidancePolicy() versionedRef {
	return versionedRef{
		ID:      "gms.guidance-policy.default.v1",
		Version: 1,
		Digest:  "sha256:e8c0891959144f05f533813f43f8b5760726f4a635bdfc3d46230294be08403f",
	}
}

// gateFailureCodeOrDefault returns the frozen M6 failure code.
func (p *ToolPolicy) gateFailureCodeOrDefault() string {
	if p.gateFailureCode != "" {
		return p.gateFailureCode
	}
	return ReasonToolUnsupported
}

// watermarkSequence extracts the integer projection sequence of one §7.14
// watermark document.
func watermarkSequence(watermark map[string]any) (uint64, error) {
	raw, ok := watermark["projected_through_activation_sequence"]
	if !ok || !contract.IsIntegerNumber(raw) {
		return 0, fmt.Errorf("watermark carries no integer projected_through_activation_sequence")
	}
	n, err := raw.(json.Number).Int64()
	if err != nil || n < 0 {
		return 0, fmt.Errorf("watermark sequence %v is not a non-negative integer", raw)
	}
	return uint64(n), nil
}

// versionedRefDoc renders one VersionedRef as its decoder-model document.
func versionedRefDoc(ref contract.VersionedRef) map[string]any {
	return map[string]any{
		"id":      ref.ID,
		"version": json.Number(ref.Version),
		"digest":  ref.Digest,
	}
}

func mustObject(v any) map[string]any {
	obj, _ := contract.AsObject(v)
	if obj == nil {
		return map[string]any{}
	}
	return obj
}

func skillKey(ref contract.SkillArtifactRef) string {
	return contract.CanonicalKey(skillRefDoc(ref))
}

// cloneResponse deep-copies one response through the decoder model (the
// cached page must never be mutable by a caller).
func cloneResponse(response *Response) *Response {
	if response == nil {
		return nil
	}
	clone := &Response{
		ToolName:             response.ToolName,
		Status:               response.Status,
		Result:               deepCopyDoc(response.Result),
		ReadAudit:            deepCopyDoc(response.ReadAudit),
		UpstreamResultDigest: response.UpstreamResultDigest,
	}
	return clone
}

// deepCopyDoc copies one decoder-model document (json.Number preserved).
func deepCopyDoc(doc map[string]any) map[string]any {
	if doc == nil {
		return nil
	}
	out := make(map[string]any, len(doc))
	for key, value := range doc {
		out[key] = deepCopyValue(value)
	}
	return out
}

func deepCopyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return deepCopyDoc(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}
