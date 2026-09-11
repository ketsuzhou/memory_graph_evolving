// arguments.go parses and normalizes the three frozen GMS §10.2–§10.4 tool
// argument payloads. Every payload has a closed field set, integer-only
// numbers and a deterministic normalization (set-like arrays sorted,
// materialized defaults); the query identity digest is SHA-256 over the JCS
// of the effective payload so identical queries collide regardless of the
// raw argument order (GMS §10.2 normalization rules).
package retrieval

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"river2.dev/graph-memory-service/internal/contract"
)

// Closed argument schema versions (GMS §10.2–§10.4).
const (
	SchemaExploreArguments  = "gms.memory-explore-arguments.v1"
	SchemaExpandArguments   = "gms.memory-expand-arguments.v1"
	SchemaSkillGetArguments = "gms.skill-get-arguments.v1"
)

// Service-side request caps (GMS §10.8): a request budget above these is
// BUDGET_INVALID before any retrieval work starts.
const (
	MaxTotalCapRequest      = 64
	MaxSubCapRequest        = 64
	MaxGuidanceTokenRequest = 1000000
	MaxQueryTextBytes       = 4096
	MaxSessionIDBytes       = 256
)

// budgetRequest is one parsed request budgets sub-object.
type budgetRequest struct {
	TotalCap            int64
	EvidenceSubcap      int64
	SkillSubcap         int64
	GuidanceTokenBudget int64
}

// servedFences is one parsed served_fences sub-object.
type servedFences struct {
	Evidence string
	Skill    string
}

func (f servedFences) asDoc() map[string]any {
	return map[string]any{
		"evidence_fence_digest": f.Evidence,
		"skill_fence_digest":    f.Skill,
	}
}

// exploreFilters is one parsed, normalized filters sub-object.
type exploreFilters struct {
	ActiveOnly bool
	Kinds      []string
	LineageIDs []string
	BranchRefs []map[string]any
}

// versionedRef is one parsed id/version/digest ref (canonical fields kept
// for digest identity).
type versionedRef struct {
	ID      string
	Version int64
	Digest  string
}

func (r versionedRef) asDoc() map[string]any {
	return map[string]any{
		"id":      r.ID,
		"version": json.Number(fmt.Sprintf("%d", r.Version)),
		"digest":  r.Digest,
	}
}

// exploreArguments is one parsed memory_explore arguments payload.
type exploreArguments struct {
	SessionID          string
	QueryText          string
	QueryTerms         []string
	RuntimeContextHash string
	RankerPolicy       versionedRef
	Budgets            budgetRequest
	Filters            exploreFilters
	HasServedFences    bool
	ServedFences       servedFences
	ContinuationOf     string
	QueryDigest        string
}

// expandArguments is one parsed memory_expand arguments payload.
type expandArguments struct {
	SessionID          string
	SourceQueryDigest  string
	Target             expandTarget
	SourceViewHash     string
	RuntimeContextHash string
	RankerPolicy       versionedRef
	Budgets            budgetRequest
	ServedFences       servedFences
}

// expandTarget is one parsed expansion target (tagged union).
type expandTarget struct {
	RefType      string // branch | evidence | checkpoint | child_skill
	Branch       *branchTarget
	Evidence     *contract.EvidenceRef
	Skill        *contract.SkillArtifactRef
	CheckpointID string
}

type branchTarget struct {
	Revision     contract.SkillArtifactRef
	BranchID     string
	BranchDigest string
}

// skillGetArguments is one parsed skill_get arguments payload.
type skillGetArguments struct {
	SkillRef            contract.SkillArtifactRef
	RuntimeContextHash  string
	RenderProfile       versionedRef
	PolicyRef           versionedRef
	GuidanceTokenBudget int64
	Visibility          string
	IncludedBranchRefs  []string
}

// ---------------------------------------------------------------------------
// Shared strict-parse helpers
// ---------------------------------------------------------------------------

func parseVersionedRefField(where string, raw any) (versionedRef, error) {
	obj, ok := contract.AsObject(raw)
	if !ok {
		return versionedRef{}, newError(ReasonToolArgumentsInvalid, "%s: not an object", where)
	}
	ref, err := contract.ParseVersionedRef(obj)
	if err != nil {
		return versionedRef{}, err
	}
	version, perr := parseVersionInt(ref.Version)
	if perr != nil {
		return versionedRef{}, newError(ReasonToolArgumentsInvalid, "%s: version %q is not an integer", where, ref.Version)
	}
	return versionedRef{ID: ref.ID, Version: version, Digest: ref.Digest}, nil
}

func parseVersionInt(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return 0, err
	}
	if fmt.Sprintf("%d", n) != s {
		return 0, fmt.Errorf("non-canonical integer %q", s)
	}
	return n, nil
}

func requireIntField(where string, obj map[string]any, field string) (int64, error) {
	raw, present := obj[field]
	if !present {
		return 0, newError(ReasonToolArgumentsInvalid, "%s: missing required field %q", where, field)
	}
	if !contract.IsIntegerNumber(raw) {
		return 0, newError(ReasonToolArgumentsInvalid, "%s: field %q must be an integer", where, field)
	}
	n, err := raw.(json.Number).Int64()
	if err != nil {
		return 0, newError(ReasonToolArgumentsInvalid, "%s: field %q out of integer range", where, field)
	}
	return n, nil
}

func requireStringField(where string, obj map[string]any, field string) (string, error) {
	raw, present := obj[field]
	if !present {
		return "", newError(ReasonToolArgumentsInvalid, "%s: missing required field %q", where, field)
	}
	s, ok := contract.AsString(raw)
	if !ok {
		return "", newError(ReasonToolArgumentsInvalid, "%s: field %q must be a string", where, field)
	}
	return s, nil
}

// rejectArgumentFields enforces one closed field set.
func rejectArgumentFields(where string, obj map[string]any, closed map[string]bool) error {
	for key := range obj {
		if !closed[key] {
			return newError(ReasonToolArgumentsInvalid, "%s: unknown field %q outside the closed set", where, key)
		}
	}
	return nil
}

// checkUnknownRequiredExtension fails closed on a required:true extension
// key (v1 registers none, Contract §6.4).
func checkUnknownRequiredExtension(args map[string]any) error {
	if unknownRequiredExtension(args) {
		return newError(contract.ReasonUnknownRequiredExtension, "arguments carry a required extension key not registered in v1")
	}
	return nil
}

// parseBudgets parses and validates one request budgets sub-object.
func parseBudgets(obj map[string]any) (budgetRequest, error) {
	raw, present := obj["budgets"]
	if !present {
		return budgetRequest{}, newError(ReasonToolArgumentsInvalid, "arguments: missing required field \"budgets\"")
	}
	budgetsObj, ok := contract.AsObject(raw)
	if !ok {
		return budgetRequest{}, newError(ReasonToolArgumentsInvalid, "budgets: not an object")
	}
	closed := map[string]bool{"total_cap": true, "evidence_subcap": true, "skill_subcap": true, "guidance_token_budget": true}
	if err := rejectArgumentFields("budgets", budgetsObj, closed); err != nil {
		return budgetRequest{}, err
	}
	budgets := budgetRequest{}
	var err error
	if budgets.TotalCap, err = requireIntField("budgets", budgetsObj, "total_cap"); err != nil {
		return budgetRequest{}, err
	}
	if budgets.EvidenceSubcap, err = requireIntField("budgets", budgetsObj, "evidence_subcap"); err != nil {
		return budgetRequest{}, err
	}
	if budgets.SkillSubcap, err = requireIntField("budgets", budgetsObj, "skill_subcap"); err != nil {
		return budgetRequest{}, err
	}
	if budgets.GuidanceTokenBudget, err = requireIntField("budgets", budgetsObj, "guidance_token_budget"); err != nil {
		return budgetRequest{}, err
	}
	if budgets.TotalCap < 0 || budgets.EvidenceSubcap < 0 || budgets.SkillSubcap < 0 || budgets.GuidanceTokenBudget < 0 {
		return budgetRequest{}, newError(ReasonBudgetInvalid, "budgets must be non-negative integers")
	}
	// Subcaps are independent pools that must not overrun the shared total
	// (GMS §10.2/§10.8; allocation enforces the per-pool caps against the
	// served counts, exactly like the Contract §12.4 result accounting).
	if budgets.EvidenceSubcap > budgets.TotalCap || budgets.SkillSubcap > budgets.TotalCap {
		return budgetRequest{}, newError(ReasonBudgetInvalid,
			"subcap %d/%d overruns total_cap %d", budgets.EvidenceSubcap, budgets.SkillSubcap, budgets.TotalCap)
	}
	if budgets.TotalCap > MaxTotalCapRequest || budgets.EvidenceSubcap > MaxSubCapRequest || budgets.SkillSubcap > MaxSubCapRequest {
		return budgetRequest{}, newError(ReasonBudgetInvalid, "budgets exceed the service request caps")
	}
	if budgets.GuidanceTokenBudget > MaxGuidanceTokenRequest {
		return budgetRequest{}, newError(ReasonBudgetInvalid, "guidance_token_budget exceeds the service request cap")
	}
	return budgets, nil
}

// parseServedFences parses one served_fences sub-object (strict shape).
func parseServedFences(where string, obj map[string]any) (servedFences, error) {
	raw, present := obj["served_fences"]
	if !present {
		return servedFences{}, newError(ReasonToolArgumentsInvalid, "%s: missing required field \"served_fences\"", where)
	}
	fencesObj, ok := contract.AsObject(raw)
	if !ok {
		return servedFences{}, newError(ReasonToolArgumentsInvalid, "%s.served_fences: not an object", where)
	}
	closed := map[string]bool{"evidence_fence_digest": true, "skill_fence_digest": true}
	if err := rejectArgumentFields(where+".served_fences", fencesObj, closed); err != nil {
		return servedFences{}, err
	}
	evidence, err := requireStringField(where+".served_fences", fencesObj, "evidence_fence_digest")
	if err != nil {
		return servedFences{}, err
	}
	skill, err := requireStringField(where+".served_fences", fencesObj, "skill_fence_digest")
	if err != nil {
		return servedFences{}, err
	}
	if !isSHA256(evidence) || !isSHA256(skill) {
		return servedFences{}, newError(ReasonToolArgumentsInvalid, "%s.served_fences: digests must match sha256:<64 hex>", where)
	}
	return servedFences{Evidence: evidence, Skill: skill}, nil
}

func isSHA256(s string) bool {
	if len(s) != 71 || s[:7] != "sha256:" {
		return false
	}
	for _, c := range s[7:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// memory_explore arguments (GMS §10.2)
// ---------------------------------------------------------------------------

var exploreArgumentFields = map[string]bool{
	"schema_version": true, "explore_session_id": true, "query_text": true,
	"runtime_context_hash": true, "ranker_policy_ref": true, "budgets": true,
	"filters": true, "continuation_of_query_digest": true, "served_fences": true,
	"extensions": true,
}

var expandArgumentFields = map[string]bool{
	"schema_version": true, "explore_session_id": true, "source_query_digest": true,
	"target": true, "source_view_hash": true, "runtime_context_hash": true,
	"ranker_policy_ref": true, "budgets": true, "served_fences": true,
	"max_graph_depth": true, "extensions": true,
}

var skillGetArgumentFields = map[string]bool{
	"schema_version": true, "skill_ref": true, "runtime_context_hash": true,
	"render_profile_ref": true, "policy_ref": true, "guidance_token_budget": true,
	"visibility": true, "included_branch_refs": true, "extensions": true,
}

// parseExploreArguments parses, normalizes and digests one explore payload.
func (s *Service) parseExploreArguments(args map[string]any) (*exploreArguments, error) {
	if args == nil {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments payload is required")
	}
	if err := checkUnknownRequiredExtension(args); err != nil {
		return nil, err
	}
	if err := rejectArgumentFields("arguments", args, exploreArgumentFields); err != nil {
		return nil, err
	}
	if sv, _ := contract.AsString(args["schema_version"]); sv != SchemaExploreArguments {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments schema_version %q is not %q", sv, SchemaExploreArguments)
	}
	parsed := &exploreArguments{}
	var err error

	parsed.SessionID, err = requireStringField("arguments", args, "explore_session_id")
	if err != nil {
		return nil, err
	}
	if parsed.SessionID == "" || len(parsed.SessionID) > MaxSessionIDBytes {
		return nil, newError(ReasonExploreSessionInvalid, "explore_session_id must be 1..%d bytes", MaxSessionIDBytes)
	}
	parsed.QueryText, err = requireStringField("arguments", args, "query_text")
	if err != nil {
		return nil, err
	}
	if parsed.QueryText == "" || len(parsed.QueryText) > MaxQueryTextBytes {
		return nil, newError(ReasonExploreQueryInvalid, "query_text must be 1..%d bytes", MaxQueryTextBytes)
	}
	parsed.QueryTerms = queryTerms(parsed.QueryText)
	if len(parsed.QueryTerms) == 0 {
		return nil, newError(ReasonExploreQueryInvalid, "query_text carries no indexable terms")
	}
	parsed.RuntimeContextHash, err = requireStringField("arguments", args, "runtime_context_hash")
	if err != nil {
		return nil, err
	}
	if !isSHA256(parsed.RuntimeContextHash) {
		return nil, newError(ReasonToolArgumentsInvalid, "runtime_context_hash must match sha256:<64 hex>")
	}

	rankerRaw, present := args["ranker_policy_ref"]
	if !present {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments: missing required field \"ranker_policy_ref\"")
	}
	parsed.RankerPolicy, err = parseVersionedRefField("ranker_policy_ref", rankerRaw)
	if err != nil {
		return nil, err
	}
	if floor, ok := s.policy.profileFloor(ToolExplore, "ranker_policy_ref"); ok && parsed.RankerPolicy.Version < floor {
		return nil, newError(ReasonSchemaVersionUnsupported,
			"ranker_policy_ref version %d is below the policy floor %d", parsed.RankerPolicy.Version, floor)
	}

	parsed.Budgets, err = parseBudgets(args)
	if err != nil {
		return nil, err
	}

	parsed.Filters, err = parseExploreFilters(args["filters"])
	if err != nil {
		return nil, err
	}

	if raw, present := args["continuation_of_query_digest"]; present {
		continuation, ok := contract.AsString(raw)
		if !ok || continuation == "" {
			return nil, newError(ReasonExploreContinuationInvalid, "continuation_of_query_digest must be a non-empty string")
		}
		parsed.ContinuationOf = continuation
	}
	if _, present := args["served_fences"]; present {
		parsed.HasServedFences = true
		parsed.ServedFences, err = parseServedFences("arguments", args)
		if err != nil {
			return nil, err
		}
	}

	parsed.QueryDigest = exploreQueryDigest(parsed)
	return parsed, nil
}

var canonicalKinds = map[string]bool{"human_procedure": true, "step_guidance": true, "composite": true}

// parseExploreFilters parses one filters sub-object with normalization:
// set-like arrays sorted+deduped, active_only materialized to true.
func parseExploreFilters(raw any) (exploreFilters, error) {
	filters := exploreFilters{ActiveOnly: true}
	obj, ok := contract.AsObject(raw)
	if !ok {
		return filters, newError(ReasonToolArgumentsInvalid, "filters: not an object")
	}
	closed := map[string]bool{"active_only": true, "kinds": true, "lineage_ids": true, "branch_refs": true}
	if err := rejectArgumentFields("filters", obj, closed); err != nil {
		return filters, err
	}
	if raw, present := obj["active_only"]; present {
		active, ok := raw.(bool)
		if !ok {
			return filters, newError(ReasonExploreFilterInvalid, "filters.active_only must be a boolean")
		}
		if !active {
			return filters, newError(ReasonExploreFilterInvalid,
				"filters.active_only=false is not servable in v1; the normalized value MUST be true (GMS 10.2)")
		}
		filters.ActiveOnly = true // materialized
	}
	if raw, present := obj["kinds"]; present {
		items, ok := contract.AsArray(raw)
		if !ok {
			return filters, newError(ReasonExploreFilterInvalid, "filters.kinds must be an array")
		}
		seen := map[string]bool{}
		for _, item := range items {
			kind, ok := contract.AsString(item)
			if !ok || !canonicalKinds[kind] {
				return filters, newError(ReasonExploreFilterInvalid, "filters.kinds entry %v outside the closed kind enum", item)
			}
			seen[kind] = true
		}
		for kind := range seen {
			filters.Kinds = append(filters.Kinds, kind)
		}
		sort.Strings(filters.Kinds)
	}
	if raw, present := obj["lineage_ids"]; present {
		items, ok := contract.AsArray(raw)
		if !ok {
			return filters, newError(ReasonExploreFilterInvalid, "filters.lineage_ids must be an array")
		}
		seen := map[string]bool{}
		for _, item := range items {
			id, ok := contract.AsString(item)
			if !ok || id == "" {
				return filters, newError(ReasonExploreFilterInvalid, "filters.lineage_ids entries must be non-empty strings")
			}
			seen[id] = true
		}
		for id := range seen {
			filters.LineageIDs = append(filters.LineageIDs, id)
		}
		sort.Strings(filters.LineageIDs)
	}
	if raw, present := obj["branch_refs"]; present {
		items, ok := contract.AsArray(raw)
		if !ok {
			return filters, newError(ReasonExploreFilterInvalid, "filters.branch_refs must be an array")
		}
		for _, item := range items {
			refObj, ok := contract.AsObject(item)
			if !ok {
				return filters, newError(ReasonExploreFilterInvalid, "filters.branch_refs entries must be exact branch-ref objects")
			}
			if sv, _ := contract.AsString(refObj["schema_version"]); sv != "gms.exact-branch-ref.v1" {
				return filters, newError(ReasonExploreFilterInvalid, "branch ref schema_version %q is not gms.exact-branch-ref.v1 (naked branch strings are rejected)", sv)
			}
			ownerRaw, _ := contract.AsObject(refObj["source_skill_ref"])
			owner, err := contract.ParseSkillArtifactRef(ownerRaw)
			if err != nil {
				return filters, newError(ReasonExploreFilterInvalid, "branch ref source_skill_ref invalid: %v", err)
			}
			branchID, _ := contract.AsString(refObj["branch_id"])
			digest, _ := contract.AsString(refObj["branch_digest"])
			if branchID == "" || !isSHA256(digest) {
				return filters, newError(ReasonExploreFilterInvalid, "branch ref branch_id/branch_digest malformed")
			}
			filters.BranchRefs = append(filters.BranchRefs, map[string]any{
				"schema_version":   "gms.exact-branch-ref.v1",
				"source_skill_ref": skillRefDoc(owner),
				"branch_id":        branchID,
				"branch_digest":    digest,
			})
		}
		sort.Slice(filters.BranchRefs, func(i, j int) bool {
			return contract.CanonicalKey(filters.BranchRefs[i]) < contract.CanonicalKey(filters.BranchRefs[j])
		})
	}
	return filters, nil
}

// exploreQueryDigest computes the query identity: SHA-256 over the JCS of
// the effective (normalized) payload.
func exploreQueryDigest(parsed *exploreArguments) string {
	filters := map[string]any{
		"active_only": true,
		"kinds":       stringsToAny(parsed.Filters.Kinds),
		"lineage_ids": stringsToAny(parsed.Filters.LineageIDs),
		"branch_refs": refsToAny(parsed.Filters.BranchRefs),
	}
	effective := map[string]any{
		"schema_version":       SchemaExploreArguments,
		"explore_session_id":   parsed.SessionID,
		"query_text":           parsed.QueryText,
		"runtime_context_hash": parsed.RuntimeContextHash,
		"ranker_policy_ref":    parsed.RankerPolicy.asDoc(),
		"budgets": map[string]any{
			"total_cap":             json.Number(fmt.Sprintf("%d", parsed.Budgets.TotalCap)),
			"evidence_subcap":       json.Number(fmt.Sprintf("%d", parsed.Budgets.EvidenceSubcap)),
			"skill_subcap":          json.Number(fmt.Sprintf("%d", parsed.Budgets.SkillSubcap)),
			"guidance_token_budget": json.Number(fmt.Sprintf("%d", parsed.Budgets.GuidanceTokenBudget)),
		},
		"filters": filters,
	}
	digest, err := contract.DigestOf(effective)
	if err != nil {
		// Integer-only decoder-model values always canonicalize; a failure
		// here would be a construction bug, not a caller error.
		return contract.DigestBytes([]byte("explore-query-uncanonicalizable"))
	}
	return digest
}

// ---------------------------------------------------------------------------
// memory_expand arguments (GMS §10.3)
// ---------------------------------------------------------------------------

var expandTargetTypes = map[string]bool{"branch": true, "evidence": true, "checkpoint": true, "child_skill": true}

// parseExpandArguments parses one expand payload. The closed-order rule is
// deliberate: depth (v1: exactly 1) and the fence shape are argument-level
// contracts, then the continuation resolves against the session.
func (s *Service) parseExpandArguments(args map[string]any) (*expandArguments, error) {
	if args == nil {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments payload is required")
	}
	if err := checkUnknownRequiredExtension(args); err != nil {
		return nil, err
	}
	if err := rejectArgumentFields("arguments", args, expandArgumentFields); err != nil {
		return nil, err
	}
	if sv, _ := contract.AsString(args["schema_version"]); sv != SchemaExpandArguments {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments schema_version %q is not %q", sv, SchemaExpandArguments)
	}
	parsed := &expandArguments{}
	var err error

	parsed.SessionID, err = requireStringField("arguments", args, "explore_session_id")
	if err != nil {
		return nil, err
	}
	if parsed.SessionID == "" || len(parsed.SessionID) > MaxSessionIDBytes {
		return nil, newError(ReasonExploreSessionInvalid, "explore_session_id must be 1..%d bytes", MaxSessionIDBytes)
	}
	parsed.SourceQueryDigest, err = requireStringField("arguments", args, "source_query_digest")
	if err != nil {
		return nil, err
	}
	if parsed.SourceQueryDigest == "" {
		return nil, newError(ReasonExploreContinuationInvalid, "source_query_digest must be a non-empty string")
	}

	// v1: exactly one expansion hop (GMS §10.3).
	depth, err := requireIntField("arguments", args, "max_graph_depth")
	if err != nil {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments: missing/invalid required field \"max_graph_depth\"")
	}
	if depth != 1 {
		return nil, newError(ReasonExpandDepthUnsupported, "max_graph_depth %d is not the v1-served depth 1", depth)
	}

	parsed.RuntimeContextHash, err = requireStringField("arguments", args, "runtime_context_hash")
	if err != nil {
		return nil, err
	}
	if !isSHA256(parsed.RuntimeContextHash) {
		return nil, newError(ReasonToolArgumentsInvalid, "runtime_context_hash must match sha256:<64 hex>")
	}
	rankerRaw, present := args["ranker_policy_ref"]
	if !present {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments: missing required field \"ranker_policy_ref\"")
	}
	parsed.RankerPolicy, err = parseVersionedRefField("ranker_policy_ref", rankerRaw)
	if err != nil {
		return nil, err
	}
	if floor, ok := s.policy.profileFloor(ToolExpand, "ranker_policy_ref"); ok && parsed.RankerPolicy.Version < floor {
		return nil, newError(ReasonSchemaVersionUnsupported,
			"ranker_policy_ref version %d is below the policy floor %d", parsed.RankerPolicy.Version, floor)
	}
	parsed.Budgets, err = parseBudgets(args)
	if err != nil {
		return nil, err
	}
	parsed.ServedFences, err = parseServedFences("arguments", args)
	if err != nil {
		return nil, err
	}

	targetRaw, present := args["target"]
	if !present {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments: missing required field \"target\"")
	}
	targetObj, ok := contract.AsObject(targetRaw)
	if !ok {
		return nil, newError(ReasonToolArgumentsInvalid, "target: not an object")
	}
	if err := rejectArgumentFields("target", targetObj, map[string]bool{"ref_type": true, "ref": true}); err != nil {
		return nil, err
	}
	refType, _ := contract.AsString(targetObj["ref_type"])
	if !expandTargetTypes[refType] {
		return nil, newError(ReasonToolArgumentsInvalid, "target.ref_type %q outside the closed enum", refType)
	}
	refObj, ok := contract.AsObject(targetObj["ref"])
	if !ok || len(refObj) == 0 {
		return nil, newError(ReasonToolArgumentsInvalid, "target.ref must be a non-empty object")
	}
	parsed.Target, err = parseExpandTargetRef(refType, refObj)
	if err != nil {
		return nil, err
	}
	if hashRaw, present := args["source_view_hash"]; present {
		hash, ok := contract.AsString(hashRaw)
		if !ok || !isSHA256(hash) {
			return nil, newError(ReasonToolArgumentsInvalid, "source_view_hash must match sha256:<64 hex>")
		}
		parsed.SourceViewHash = hash
	}
	if (refType == "branch" || refType == "child_skill") && parsed.SourceViewHash == "" {
		return nil, newError(ReasonToolArgumentsInvalid,
			"target.ref_type %q requires source_view_hash (GMS 10.3)", refType)
	}
	return parsed, nil
}

// parseExpandTargetRef binds the tagged union's ref to its exact DTO.
func parseExpandTargetRef(refType string, refObj map[string]any) (expandTarget, error) {
	target := expandTarget{RefType: refType}
	switch refType {
	case "branch":
		if sv, _ := contract.AsString(refObj["schema_version"]); sv != "gms.exact-branch-ref.v1" {
			return target, newError(ReasonToolArgumentsInvalid, "branch target schema_version %q is not gms.exact-branch-ref.v1", sv)
		}
		ownerRaw, _ := contract.AsObject(refObj["source_skill_ref"])
		owner, err := contract.ParseSkillArtifactRef(ownerRaw)
		if err != nil {
			return target, newError(ReasonToolArgumentsInvalid, "branch target source_skill_ref invalid: %v", err)
		}
		branchID, _ := contract.AsString(refObj["branch_id"])
		digest, _ := contract.AsString(refObj["branch_digest"])
		if branchID == "" || !isSHA256(digest) {
			return target, newError(ReasonToolArgumentsInvalid, "branch target branch_id/branch_digest malformed")
		}
		target.Branch = &branchTarget{Revision: owner, BranchID: branchID, BranchDigest: digest}
	case "evidence":
		ref, err := contract.ParseEvidenceRef(refObj)
		if err != nil {
			return target, newError(ReasonToolArgumentsInvalid, "evidence target is not an exact §7.7 ref: %v", err)
		}
		target.Evidence = &ref
	case "checkpoint":
		id, _ := contract.AsString(refObj["checkpoint_id"])
		if id == "" {
			return target, newError(ReasonToolArgumentsInvalid, "checkpoint target checkpoint_id missing")
		}
		target.CheckpointID = id
	case "child_skill":
		ref, err := contract.ParseSkillArtifactRef(refObj)
		if err != nil {
			return target, newError(ReasonToolArgumentsInvalid, "child_skill target is not an exact §7.3 ref: %v", err)
		}
		target.Skill = &ref
	}
	return target, nil
}

// ---------------------------------------------------------------------------
// skill_get arguments (GMS §10.4)
// ---------------------------------------------------------------------------

// parseSkillGetArguments parses one skill_get payload against the frozen
// tool-specific binding (render profile floor v2, guidance policy floor v1,
// current_active/historical_exact visibility).
func (s *Service) parseSkillGetArguments(args map[string]any) (*skillGetArguments, error) {
	if args == nil {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments payload is required")
	}
	if err := checkUnknownRequiredExtension(args); err != nil {
		return nil, err
	}
	if err := rejectArgumentFields("arguments", args, skillGetArgumentFields); err != nil {
		return nil, err
	}
	if sv, _ := contract.AsString(args["schema_version"]); sv != SchemaSkillGetArguments {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments schema_version %q is not %q", sv, SchemaSkillGetArguments)
	}
	parsed := &skillGetArguments{}
	var err error

	skillRaw, present := args["skill_ref"]
	if !present {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments: missing required field \"skill_ref\"")
	}
	skillObj, ok := contract.AsObject(skillRaw)
	if !ok {
		return nil, newError(ReasonToolArgumentsInvalid, "skill_ref: not an object")
	}
	parsed.SkillRef, err = parseExecutableSkillRef(skillObj)
	if err != nil {
		return nil, err
	}

	parsed.RuntimeContextHash, err = requireStringField("arguments", args, "runtime_context_hash")
	if err != nil {
		return nil, err
	}
	if !isSHA256(parsed.RuntimeContextHash) {
		return nil, newError(ReasonToolArgumentsInvalid, "runtime_context_hash must match sha256:<64 hex>")
	}

	renderRaw, present := args["render_profile_ref"]
	if !present {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments: missing required field \"render_profile_ref\"")
	}
	parsed.RenderProfile, err = parseVersionedRefField("render_profile_ref", renderRaw)
	if err != nil {
		return nil, err
	}
	if floor, ok := s.policy.profileFloor(ToolSkillGet, "render_profile_ref"); ok && parsed.RenderProfile.Version < floor {
		return nil, newError(ReasonSchemaVersionUnsupported,
			"render_profile_ref version %d is below the policy floor %d", parsed.RenderProfile.Version, floor)
	}
	policyRaw, present := args["policy_ref"]
	if !present {
		return nil, newError(ReasonToolArgumentsInvalid, "arguments: missing required field \"policy_ref\"")
	}
	parsed.PolicyRef, err = parseVersionedRefField("policy_ref", policyRaw)
	if err != nil {
		return nil, err
	}
	if floor, ok := s.policy.profileFloor(ToolSkillGet, "policy_ref"); ok && parsed.PolicyRef.Version < floor {
		return nil, newError(ReasonSchemaVersionUnsupported,
			"policy_ref version %d is below the policy floor %d", parsed.PolicyRef.Version, floor)
	}

	parsed.GuidanceTokenBudget, err = requireIntField("arguments", args, "guidance_token_budget")
	if err != nil {
		return nil, err
	}
	if parsed.GuidanceTokenBudget < 0 || parsed.GuidanceTokenBudget > MaxGuidanceTokenRequest {
		return nil, newError(ReasonBudgetInvalid, "guidance_token_budget outside the servable range")
	}

	parsed.Visibility, err = requireStringField("arguments", args, "visibility")
	if err != nil {
		return nil, err
	}
	if parsed.Visibility != "current_active" && parsed.Visibility != "historical_exact" {
		return nil, newError(ReasonToolArgumentsInvalid, "visibility %q outside {current_active, historical_exact}", parsed.Visibility)
	}

	if raw, present := args["included_branch_refs"]; present {
		items, ok := contract.AsArray(raw)
		if !ok {
			return nil, newError(ReasonToolArgumentsInvalid, "included_branch_refs must be an array")
		}
		seen := map[string]bool{}
		for _, item := range items {
			id, ok := contract.AsString(item)
			if !ok || id == "" {
				return nil, newError(ReasonToolArgumentsInvalid, "included_branch_refs entries must be non-empty strings")
			}
			seen[id] = true
		}
		for id := range seen {
			parsed.IncludedBranchRefs = append(parsed.IncludedBranchRefs, id)
		}
		sort.Strings(parsed.IncludedBranchRefs)
	}
	return parsed, nil
}

// parseExecutableSkillRef accepts ONLY an exact released §7.3 ref: candidate
// refs never execute, non-integer versions ("latest") are NON_EXACT_REF.
func parseExecutableSkillRef(obj map[string]any) (contract.SkillArtifactRef, error) {
	if sv, _ := contract.AsString(obj["schema_version"]); sv == contract.SchemaCandidateArtifactRef {
		return contract.SkillArtifactRef{}, newError(ReasonCandidateNotExecutable,
			"candidate refs are never executable by the runtime (Contract §7.4)")
	}
	if raw, present := obj["version"]; present {
		if !contract.IsIntegerNumber(raw) {
			if _, isString := contract.AsString(raw); isString {
				return contract.SkillArtifactRef{}, newError(ReasonNonExactRef,
					"skill_ref version %q is not an exact integer version; only exact refs are servable", raw)
			}
		}
	}
	ref, err := contract.ParseSkillArtifactRef(obj)
	if err != nil {
		return contract.SkillArtifactRef{}, err
	}
	return ref, nil
}

// ---------------------------------------------------------------------------
// Term extraction (deterministic lexical index input)
// ---------------------------------------------------------------------------

// queryTerms lowercases and splits the query into unique alphanumeric terms
// (sorted; drop-letters > 1 char). Pure function of the input text.
func queryTerms(text string) []string {
	terms := map[string]bool{}
	var current strings.Builder
	flush := func() {
		if current.Len() > 1 {
			terms[strings.ToLower(current.String())] = true
		}
		current.Reset()
	}
	for _, r := range text {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			current.WriteRune(unicode.ToLower(r))
		default:
			flush()
		}
	}
	flush()
	out := make([]string, 0, len(terms))
	for term := range terms {
		out = append(out, term)
	}
	sort.Strings(out)
	return out
}

func stringsToAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, v := range values {
		out = append(out, v)
	}
	return out
}

func refsToAny(refs []map[string]any) []any {
	out := make([]any, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref)
	}
	return out
}

// skillRefDoc renders one exact §7.3 ref as its decoder-model document.
func skillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         json.Number(ref.Version),
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

// evidenceRefDoc renders one exact §7.7 ref as its decoder-model document.
func evidenceRefDoc(ref contract.EvidenceRef) map[string]any {
	doc := map[string]any{
		"schema_version":  ref.SchemaVersion,
		"evidence_id":     ref.EvidenceID,
		"version":         json.Number(ref.Version),
		"evidence_digest": ref.EvidenceDigest,
		"commit_state":    ref.CommitState,
		"evidence_kind":   ref.EvidenceKind,
	}
	if ref.SourceSegmentRef != nil {
		doc["source_segment_ref"] = map[string]any{
			"id":      ref.SourceSegmentRef.ID,
			"version": json.Number(ref.SourceSegmentRef.Version),
			"digest":  ref.SourceSegmentRef.Digest,
		}
	}
	return doc
}

// sortStrings is the shared deterministic string sort.
func sortStrings(values []string) { sort.Strings(values) }
