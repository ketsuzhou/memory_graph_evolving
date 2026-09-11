package explore

// Response exact validation for the ExploreSession layer (HST-204).
//
// Two derivations are ported verbatim from the frozen conformance
// validators and are the hard parity evidence that this implementation
// conforms to the frozen protocol:
//
//   - DeriveMatrixCaseOutcome ports $FIX/validate_tool_binding.py
//     (Contract §12.7.1, CTR-002): the tool-specific success validation
//     matrix that drives the tools/explore/basic corpus (10 cases).
//   - DeriveOmissionCaseOutcome ports $FIX/validate_explore_omission.py
//     (Contract §12.7.2, CTR-003): the top-level omission carrier
//     semantics that drive the tools/explore/{top-level-omission,
//     no-omission,negative} corpus (24 cases).
//
// The session pipeline (validateSessionResponse) layers the CTR-002 matrix
// binding rules under the full CTR-003 carrier semantics and adds the
// Host-session obligations (scope/Room binding, candidate rejection,
// embedded GuidanceView digest and profile floors, effective-budget caps).
// The Host validates; it never reranks, filters or reorders served content
// (Host Spec §7.7): result digests are computed over the payload exactly as
// delivered, served arrays are not order-validated, and only the frozen
// omission carrier is order-checked (misordered carriers are rejected, not
// re-sorted).

import (
	"math/big"
	"regexp"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/toolproxy"
)

// Closed reason codes (all reused from the Contract §13.7.1 registries; no
// new code is invented here).
const (
	reasonToolUnsupported     = "TOOL_UNSUPPORTED"
	reasonArtifactBody        = "ARTIFACT_BODY_INVALID"
	reasonBindingInvalid      = "TOOL_RESULT_BINDING_INVALID"
	reasonFieldUnknown        = "SCHEMA_FIELD_UNKNOWN"
	reasonFieldMissing        = "SCHEMA_REQUIRED_FIELD_MISSING"
	reasonEnumInvalid         = "SCHEMA_ENUM_INVALID"
	reasonVersionUnsupported  = "SCHEMA_VERSION_UNSUPPORTED"
	reasonExtensionUnknown    = "UNKNOWN_REQUIRED_EXTENSION"
	reasonRefMismatch         = "REF_MISMATCH"
	reasonNonExactRef         = "NON_EXACT_REF"
	reasonEvidenceUncommitted = "EVIDENCE_NOT_COMMITTED"
	reasonBudgetInvalid       = "BUDGET_INVALID"
	reasonBudgetExceeded      = "BUDGET_EXCEEDED"
	reasonCitationInvalid     = "CITATION_INVALID"
	reasonFenceConflict       = "EXPLORE_FENCE_CONFLICT"
	reasonScopeViolation      = "EXPLORE_SCOPE_VIOLATION"
	reasonBehindSequence      = "PROJECTION_BEHIND_REQUIRED_SEQUENCE"
	reasonViewHashMismatch    = "GUIDANCE_VIEW_HASH_MISMATCH"
	reasonQueryInvalid        = "EXPLORE_QUERY_INVALID"
	reasonArgumentsInvalid    = "TOOL_ARGUMENTS_INVALID"
	reasonIdempotency         = "IDEMPOTENCY_CONFLICT"
	reasonCandidate           = "CANDIDATE_NOT_EXECUTABLE"
	// reasonHostToolNotAllowed is the Host-owned counterpart of
	// TOOL_UNSUPPORTED frozen by the matrix policy (M5).
	reasonHostToolNotAllowed = "HOST_TOOL_NOT_ALLOWED"
)

// Frozen schema markers (Contract §7.15/§7.16).
const (
	exploreResultSchema = "gms.explore-result.v1"
	guidanceViewSchema  = "gms.guidance-view.v1"
	evidenceRefSchema   = "gms.evidence-ref.v1"
	skillRefSchema      = "gms.skill-artifact-ref.v1"
	candidateRefSchema  = "gms.candidate-artifact-ref.v1"
)

// exploreResultClosed is the §7.16 closed field group as extended by
// §12.7.2 v1.1 (CTR-003), in the frozen document order.
var exploreResultClosed = []string{
	"schema_version",
	"explore_session_id",
	"query_digest",
	"ranker_policy_ref",
	"watermark",
	"min_activation_sequence",
	"evidence_results",
	"skill_results",
	"served_fences",
	"budgets",
	"truncation_reason_codes",
	"omissions",
}

// exploreResultRequired is the closed group minus the optional
// min_activation_sequence (§12.7.2 C1: omissions is REQUIRED).
var exploreResultRequired = []string{
	"schema_version",
	"explore_session_id",
	"query_digest",
	"ranker_policy_ref",
	"watermark",
	"evidence_results",
	"skill_results",
	"served_fences",
	"budgets",
	"truncation_reason_codes",
	"omissions",
}

var omissionEntryFields = map[string]struct{}{
	"kind": {}, "ref": {}, "reason_code": {},
}

var omissionKinds = map[string]struct{}{"evidence": {}, "skill": {}}

var refSchemaByKind = map[string]string{
	"evidence": evidenceRefSchema,
	"skill":    skillRefSchema,
}

// The four gms-truncation-success registry codes (§11.4/§13.7.1) that may
// appear as omission reason codes, with the kind compatibility table
// (§12.7.2 C1/C4).
var truncationReasonCodes = map[string]struct{}{
	"TOTAL_CAP_REACHED":             {},
	"EVIDENCE_SUBCAP_REACHED":       {},
	"SKILL_SUBCAP_REACHED":          {},
	"GUIDANCE_TOKEN_BUDGET_REACHED": {},
}

var kindCodeCompat = map[string]map[string]struct{}{
	"evidence": {"TOTAL_CAP_REACHED": {}, "EVIDENCE_SUBCAP_REACHED": {}},
	"skill": {
		"TOTAL_CAP_REACHED":             {},
		"SKILL_SUBCAP_REACHED":          {},
		"GUIDANCE_TOKEN_BUDGET_REACHED": {},
	},
}

var evidenceRefRequired = []string{
	"schema_version",
	"evidence_id",
	"version",
	"evidence_digest",
	"commit_state",
	"evidence_kind",
}

var evidenceRefClosed = append([]string{}, evidenceRefRequired...)
var evidenceKinds = map[string]struct{}{
	"success_path": {}, "failure_path": {}, "recovery_path": {},
	"observation": {}, "replay_result": {}, "human_attestation": {},
}
var evidenceCommitStates = map[string]struct{}{"committed": {}, "sealed": {}}

var skillRefRequired = []string{
	"schema_version",
	"lineage_id",
	"version",
	"kind",
	"artifact_digest",
}

var skillKinds = map[string]struct{}{"human_procedure": {}, "step_guidance": {}, "composite": {}}

var requiredWatermarkFields = []string{
	"schema_version",
	"projection_stream",
	"projection_schema_version",
	"projection_head",
	"projected_through_activation_sequence",
	"source_ledger_digest",
	"state",
	"watermark_digest",
}

var requiredBudgetFields = []string{
	"total_cap",
	"evidence_subcap",
	"skill_subcap",
	"guidance_token_budget",
	"total_used",
}

var requiredFenceFields = []string{"evidence_fence_digest", "skill_fence_digest"}

// Candidate-marker fields: a ref carrying any of these belongs to the
// proposal/candidate plane and is never executable (Contract §3.1/§9.1).
var candidateMarkerFields = []string{"candidate_id", "proposal_ref", "candidate_digest"}

var sha256Pattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// omitTools is the closed explore tool set for the omission corpus.
var omitTools = map[string]struct{}{"memory_explore": {}, "memory_expand": {}}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// Outcome mirrors the reference Outcome: the derived accept decision, the
// closed reason code for rejections and the canonical result digest for
// acceptances.
type Outcome struct {
	Accept       bool
	ReasonCode   string
	ResultDigest string
	Canonical    []byte
	Replay       bool
}

func acceptedOutcome(payload contract.Value) Outcome {
	canonical, err := contract.JCS(payload)
	if err != nil {
		return Outcome{Accept: false, ReasonCode: reasonNonExactRef}
	}
	return Outcome{
		Accept:       true,
		ResultDigest: contract.DigestBytes(canonical),
		Canonical:    canonical,
	}
}

func rejectedOutcome(reason string) Outcome {
	return Outcome{Accept: false, ReasonCode: reason}
}

func isNonNegativeInt(v contract.Value) (*big.Int, bool) {
	n, ok := v.(contract.Number)
	if !ok || !n.IsInteger() {
		return nil, false
	}
	bi, ok := n.Int()
	if !ok || bi.Sign() < 0 {
		return nil, false
	}
	return bi, true
}

func isSHA256(v contract.Value) bool {
	s, ok := v.(contract.String)
	return ok && sha256Pattern.MatchString(string(s))
}

func arrayOf(obj *contract.Object, key string) (contract.Array, bool) {
	if obj == nil {
		return nil, false
	}
	v, present := obj.Get(key)
	if !present {
		return nil, false
	}
	arr, ok := v.(contract.Array)
	return arr, ok
}

func objectOf(obj *contract.Object, key string) (*contract.Object, bool) {
	if obj == nil {
		return nil, false
	}
	v, present := obj.Get(key)
	if !present {
		return nil, false
	}
	nested, ok := v.(*contract.Object)
	return nested, ok
}

func intFieldOf(obj *contract.Object, key string) (int64, bool) {
	if obj == nil {
		return 0, false
	}
	v, present := obj.Get(key)
	if !present {
		return 0, false
	}
	bi, ok := isNonNegativeInt(v)
	if !ok {
		return 0, false
	}
	if !bi.IsInt64() {
		return 0, false
	}
	return bi.Int64(), true
}

// jsonEqual compares two values by canonical bytes.
func jsonEqual(a, b contract.Value) bool {
	ab, errA := contract.JCS(a)
	bb, errB := contract.JCS(b)
	return errA == nil && errB == nil && string(ab) == string(bb)
}

// hasUnknownRequiredExtension mirrors Contract §6.4 with the empty v1
// extension registry: any required:true extension fails closed.
func hasUnknownRequiredExtension(obj *contract.Object) bool {
	if obj == nil {
		return false
	}
	extVal, present := obj.Get("extensions")
	if !present {
		return false
	}
	ext, ok := extVal.(*contract.Object)
	if !ok {
		return false
	}
	for _, key := range ext.Keys() {
		entryVal, _ := ext.Get(key)
		entry, ok := entryVal.(*contract.Object)
		if !ok {
			continue
		}
		if reqVal, hasReq := entry.Get("required"); hasReq {
			if req, isBool := reqVal.(contract.Bool); isBool && bool(req) {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Derivation 1: the CTR-002 tool-success validation matrix (§12.7.1)
// ---------------------------------------------------------------------------

// DeriveMatrixCaseOutcome derives one tools/ basic corpus case (input.json
// document) exactly like $FIX/validate_tool_binding.py: common rules in
// policy order, then the bound ruleset's rules in policy order; first
// closed failure wins, otherwise accepted with the canonical digest.
func DeriveMatrixCaseOutcome(policy *toolproxy.Policy, caseDoc contract.Value) Outcome {
	doc, ok := caseDoc.(*contract.Object)
	if !ok {
		return rejectedOutcome(reasonArtifactBody)
	}
	toolName, _ := contract.StringOf(doc, "tool_name")
	binding, bound := policy.Tools[toolName]
	if !bound {
		return rejectedOutcome(reasonToolUnsupported)
	}
	request, _ := objectOf(doc, "request")
	audit, _ := objectOf(doc, "read_audit")
	payloadVal, _ := doc.Get("result_payload")
	payload, payloadIsObject := payloadVal.(*contract.Object)

	// Common rule: unknown required extension (request, then payload).
	if hasUnknownRequiredExtension(request) || hasUnknownRequiredExtension(payload) {
		return rejectedOutcome(reasonExtensionUnknown)
	}
	// Common rule: result body not a closed DTO object.
	schemaVersion := ""
	if payloadIsObject {
		schemaVersion, _ = contract.StringOf(payload, "schema_version")
	}
	knownResultSchemas := map[string]struct{}{}
	for _, toolBinding := range policy.Tools {
		knownResultSchemas[toolBinding.ResultSchemaVersion] = struct{}{}
	}
	if _, known := knownResultSchemas[schemaVersion]; !payloadIsObject || !known {
		return rejectedOutcome(reasonArtifactBody)
	}
	// Common rule: tool/result type mismatch.
	if schemaVersion != binding.ResultSchemaVersion {
		return rejectedOutcome(reasonBindingInvalid)
	}
	// Common rule: closed field-set violation (wrappers, smuggled fields).
	closed := map[string]struct{}{}
	for _, field := range binding.ClosedFields {
		closed[field] = struct{}{}
	}
	for _, key := range payload.Keys() {
		if key == "extensions" {
			continue
		}
		if _, allowed := closed[key]; !allowed {
			return rejectedOutcome(reasonFieldUnknown)
		}
	}
	// Common rule: required top-level field missing.
	for _, field := range binding.RequiredFields {
		if _, present := payload.Get(field); !present {
			return rejectedOutcome(reasonFieldMissing)
		}
	}
	// Common rule: authorization carried by request + read audit.
	if reason := scopeRuleReason(request, audit); reason != "" {
		return rejectedOutcome(reason)
	}

	switch binding.Ruleset {
	case "explore-family":
		return deriveExploreFamilyMatrix(payload, request, audit, binding, policy)
	case "guidance-view-closed":
		return deriveGuidanceViewClosedMatrix(payload, request, audit, binding)
	default:
		return rejectedOutcome(reasonBindingInvalid)
	}
}

// scopeRuleReason compares room_id/agent_id/scope_profile_ref between the
// request and the authoritative read audit record.
func scopeRuleReason(request, audit *contract.Object) string {
	if request == nil || audit == nil {
		return ""
	}
	for _, field := range []string{"room_id", "agent_id", "scope_profile_ref"} {
		auditVal, inAudit := audit.Get(field)
		reqVal, inReq := request.Get(field)
		if inAudit && inReq && !jsonEqual(auditVal, reqVal) {
			return reasonScopeViolation
		}
	}
	return ""
}

// deriveExploreFamilyMatrix ports _check_explore_family in frozen policy
// rule order.
func deriveExploreFamilyMatrix(payload, request, audit *contract.Object, binding toolproxy.ToolBinding, policy *toolproxy.Policy) Outcome {
	ruleset := policy.Rulesets[binding.Ruleset]

	watermark, _ := objectOf(payload, "watermark")
	for _, field := range ruleset.RequiredWatermarkFields {
		if watermark == nil {
			return rejectedOutcome(reasonFieldMissing)
		}
		if _, present := watermark.Get(field); !present {
			return rejectedOutcome(reasonFieldMissing)
		}
	}
	budgets, _ := objectOf(payload, "budgets")
	for _, field := range ruleset.RequiredBudgetFields {
		if budgets == nil {
			return rejectedOutcome(reasonFieldMissing)
		}
		if _, present := budgets.Get(field); !present {
			return rejectedOutcome(reasonFieldMissing)
		}
	}
	fences, _ := objectOf(payload, "served_fences")
	for _, field := range ruleset.RequiredFenceFields {
		if fences == nil {
			return rejectedOutcome(reasonFieldMissing)
		}
		if _, present := fences.Get(field); !present {
			return rejectedOutcome(reasonFieldMissing)
		}
	}

	// Budgets consistency (Contract §12.4).
	budgetValues := map[string]*big.Int{}
	for _, field := range ruleset.RequiredBudgetFields {
		val, _ := budgets.Get(field)
		n, ok := isNonNegativeInt(val)
		if !ok {
			return rejectedOutcome(reasonBudgetInvalid)
		}
		budgetValues[field] = n
	}
	evidenceResults, evidenceOK := arrayOf(payload, "evidence_results")
	skillResults, skillOK := arrayOf(payload, "skill_results")
	if !evidenceOK || !skillOK {
		return rejectedOutcome(reasonFieldMissing)
	}
	compareLen := func(arr contract.Array, capField string) bool {
		return big.NewInt(int64(len(arr))).Cmp(budgetValues[capField]) > 0
	}
	if budgetValues["total_used"].Cmp(budgetValues["total_cap"]) > 0 ||
		compareLen(evidenceResults, "evidence_subcap") ||
		compareLen(skillResults, "skill_subcap") {
		return rejectedOutcome(reasonBudgetInvalid)
	}
	tokenSum := big.NewInt(0)
	for _, item := range skillResults {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		if view, ok := objectOf(entry, "guidance_view"); ok {
			if tokens, ok := intFieldOf(view, "content_token_count"); ok {
				tokenSum.Add(tokenSum, big.NewInt(tokens))
			}
		}
	}
	if tokenSum.Cmp(budgetValues["guidance_token_budget"]) > 0 {
		return rejectedOutcome(reasonBudgetInvalid)
	}

	// Typed result enums (Contract §7.16).
	for _, item := range evidenceResults {
		if entry, ok := item.(*contract.Object); !ok {
			return rejectedOutcome(reasonEnumInvalid)
		} else if rt, _ := contract.StringOf(entry, "result_type"); rt != "evidence" {
			return rejectedOutcome(reasonEnumInvalid)
		}
	}
	for _, item := range skillResults {
		if entry, ok := item.(*contract.Object); !ok {
			return rejectedOutcome(reasonEnumInvalid)
		} else if rt, _ := contract.StringOf(entry, "result_type"); rt != "skill" {
			return rejectedOutcome(reasonEnumInvalid)
		}
	}

	// Typed citations (Contract §12.6).
	if reason := citationRuleReason(evidenceResults, skillResults); reason != "" {
		return rejectedOutcome(reason)
	}

	// Silent-truncation guard: non-empty truncation codes require a
	// non-empty omissions carrier (the matrix-level under-approximation of
	// §12.7.2 C4; full carrier validation is DeriveOmissionCaseOutcome).
	if codes, ok := arrayOf(payload, "truncation_reason_codes"); ok && len(codes) > 0 {
		omissions, hasCarrier := arrayOf(payload, "omissions")
		if !hasCarrier || len(omissions) == 0 {
			return rejectedOutcome(reasonBindingInvalid)
		}
	}

	// Profile version floors.
	if !profileFloorsSatisfied(payload, binding.ProfileConstraints) {
		return rejectedOutcome(reasonVersionUnsupported)
	}

	// Freshness carried by request fields vs the result watermark.
	if request != nil {
		if requested, ok := intFieldOf(request, "requested_min_activation_sequence"); ok {
			projected, present := watermark.Get("projected_through_activation_sequence")
			projectedInt, isInt := isNonNegativeInt(projected)
			if !present || !isInt || projectedInt.Cmp(big.NewInt(requested)) < 0 {
				return rejectedOutcome(reasonBehindSequence)
			}
		}
	}
	return acceptedOutcome(payload)
}

// citationRuleReason validates the typed citations of both result arrays.
func citationRuleReason(evidenceResults, skillResults contract.Array) string {
	for _, item := range evidenceResults {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		citation, ok := objectOf(entry, "citation")
		if !ok {
			return reasonCitationInvalid
		}
		if _, ok := objectOf(citation, "evidence_ref"); !ok {
			return reasonCitationInvalid
		}
		if _, ok := contract.StringOf(citation, "claim"); !ok {
			return reasonCitationInvalid
		}
	}
	for _, item := range skillResults {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		identity, ok := objectOf(entry, "artifact_identity_citation")
		if !ok {
			return reasonCitationInvalid
		}
		if _, ok := objectOf(identity, "skill_ref"); !ok {
			return reasonCitationInvalid
		}
		citationsRaw, present := entry.Get("evidence_citations")
		if !present {
			return reasonCitationInvalid
		}
		citations, isArr := citationsRaw.(contract.Array)
		if !isArr {
			return reasonCitationInvalid
		}
		for _, citationItem := range citations {
			citation, ok := citationItem.(*contract.Object)
			if !ok {
				return reasonCitationInvalid
			}
			if _, ok := objectOf(citation, "evidence_ref"); !ok {
				return reasonCitationInvalid
			}
			if _, ok := contract.StringOf(citation, "claim"); !ok {
				return reasonCitationInvalid
			}
		}
	}
	return ""
}

// deriveGuidanceViewClosedMatrix ports _check_guidance_view_closed (M4):
// only the applicable closed GuidanceView rules.
func deriveGuidanceViewClosedMatrix(payload, request, audit *contract.Object, binding toolproxy.ToolBinding) Outcome {
	if !profileFloorsSatisfied(payload, binding.ProfileConstraints) {
		return rejectedOutcome(reasonVersionUnsupported)
	}
	if reason := guidanceViewHashReason(payload); reason != "" {
		return rejectedOutcome(reason)
	}
	if request != nil {
		if requested, ok := intFieldOf(request, "requested_min_activation_sequence"); ok {
			activeHead := int64(-1)
			if audit != nil {
				if head, ok := intFieldOf(audit, "active_head_activation_sequence"); ok {
					activeHead = head
				}
			}
			if activeHead < requested {
				return rejectedOutcome(reasonBehindSequence)
			}
		}
	}
	return acceptedOutcome(payload)
}

// guidanceViewHashReason checks view_hash == SHA-256(JCS(view minus
// view_hash)) inside the closed schema.
func guidanceViewHashReason(view *contract.Object) string {
	if view == nil {
		return reasonViewHashMismatch
	}
	viewHash, _ := contract.StringOf(view, "view_hash")
	preimage := contract.NewObject()
	for _, key := range view.Keys() {
		if key == "view_hash" {
			continue
		}
		val, _ := view.Get(key)
		preimage.Set(key, val)
	}
	want, err := contract.DigestOf(preimage)
	if err != nil || want != viewHash {
		return reasonViewHashMismatch
	}
	return ""
}

// profileFloorsSatisfied enforces the min_<field>_version floors frozen by
// the matrix policy.
func profileFloorsSatisfied(payload *contract.Object, constraints map[string]int64) bool {
	for key, floor := range constraints {
		const prefix, suffix = "min_", "_version"
		if len(key) <= len(prefix)+len(suffix) {
			return false
		}
		field := key[len(prefix) : len(key)-len(suffix)]
		ref, ok := objectOf(payload, field)
		if !ok {
			continue
		}
		if version, ok := intFieldOf(ref, "version"); ok && version < floor {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Derivation 2: the CTR-003 top-level omission carrier (§12.7.2)
// ---------------------------------------------------------------------------

// DeriveOmissionCaseOutcome derives one tools/explore omission corpus case
// exactly like $FIX/validate_explore_omission.py derive_outcome(case).
func DeriveOmissionCaseOutcome(caseDoc contract.Value) Outcome {
	doc, ok := caseDoc.(*contract.Object)
	if !ok {
		return rejectedOutcome(reasonArtifactBody)
	}
	toolName, _ := contract.StringOf(doc, "tool_name")
	if _, ok := omitTools[toolName]; !ok {
		return rejectedOutcome(reasonToolUnsupported)
	}
	sessionVal, _ := doc.Get("session")
	session, _ := sessionVal.(*contract.Object)
	payload, _ := doc.Get("result_payload")
	outcome := deriveCarrierOutcome(payload, session)
	if !outcome.Accept {
		return outcome
	}
	request, _ := objectOf(doc, "request")
	audit, _ := objectOf(doc, "read_audit")
	if request != nil {
		if requested, ok := intFieldOf(request, "requested_min_activation_sequence"); ok {
			payloadObj := payload.(*contract.Object)
			if watermark, ok := objectOf(payloadObj, "watermark"); ok {
				projectedRaw, _ := watermark.Get("projected_through_activation_sequence")
				projected, isInt := isNonNegativeInt(projectedRaw)
				if !isInt || projected.Cmp(big.NewInt(requested)) < 0 {
					return rejectedOutcome(reasonBehindSequence)
				}
			}
		}
	}
	if reason := scopeRuleReason(request, audit); reason != "" {
		return rejectedOutcome(reason)
	}
	return outcome
}

// deriveCarrierOutcome ports _derive_omission_outcome: the §12.7.2 C1-C7
// obligations in the frozen derivation order.
func deriveCarrierOutcome(payload contract.Value, session *contract.Object) Outcome {
	obj, isObject := payload.(*contract.Object)
	if !isObject {
		return rejectedOutcome(reasonArtifactBody)
	}
	schemaVersion, _ := contract.StringOf(obj, "schema_version")
	if schemaVersion != exploreResultSchema && schemaVersion != guidanceViewSchema {
		return rejectedOutcome(reasonArtifactBody)
	}
	if schemaVersion != exploreResultSchema {
		return rejectedOutcome(reasonBindingInvalid) // wrong result type
	}

	closed := map[string]struct{}{}
	for _, field := range exploreResultClosed {
		closed[field] = struct{}{}
	}
	for _, key := range obj.Keys() {
		if key == "extensions" {
			continue
		}
		if _, allowed := closed[key]; !allowed {
			return rejectedOutcome(reasonFieldUnknown)
		}
	}
	codesRaw, _ := obj.Get("truncation_reason_codes")
	codesArray, codesIsArray := codesRaw.(contract.Array)
	for _, field := range exploreResultRequired {
		if _, present := obj.Get(field); !present {
			if field == "omissions" && codesIsArray && len(codesArray) > 0 {
				// Silent truncation: codes without the carrier keep the
				// original closed failure code.
				return rejectedOutcome(reasonBindingInvalid)
			}
			return rejectedOutcome(reasonFieldMissing)
		}
	}
	omissionsRaw, _ := obj.Get("omissions")
	omissions, omissionsIsArray := omissionsRaw.(contract.Array)
	if !omissionsIsArray || !codesIsArray {
		return rejectedOutcome(reasonEnumInvalid)
	}

	// C1: per-entry closed shape, enums, binding, exactness, compatibility.
	for _, item := range omissions {
		if reason := omissionEntryReason(item); reason != "" {
			return rejectedOutcome(reason)
		}
	}

	// C2: dedup (one entry per exact ref).
	keys, keyErr := refKeys(omissions)
	if keyErr != "" {
		return rejectedOutcome(keyErr)
	}
	seen := map[string]struct{}{}
	for _, key := range keys {
		if _, dup := seen[key]; dup {
			return rejectedOutcome(reasonBindingInvalid)
		}
		seen[key] = struct{}{}
	}

	// C3: served disjointness (this payload + earlier session pages).
	served, servedErr := servedKeys(obj)
	if servedErr != "" {
		return rejectedOutcome(servedErr)
	}
	prior, priorErr := priorServedKeys(session)
	if priorErr != "" {
		return rejectedOutcome(priorErr)
	}
	for _, key := range prior {
		served[key] = struct{}{}
	}
	for _, key := range keys {
		if _, conflict := served[key]; conflict {
			return rejectedOutcome(reasonFenceConflict)
		}
	}

	// C2: canonical ordering (kind asc, JCS ref asc).
	ordered, orderErr := omissionsCanonicallyOrdered(omissions)
	if orderErr != "" {
		return rejectedOutcome(orderErr)
	}
	if !ordered {
		return rejectedOutcome(reasonBindingInvalid)
	}

	// C4: bidirectional consistency with truncation_reason_codes.
	codeStrings := make([]string, 0, len(codesArray))
	for _, item := range codesArray {
		s, _ := item.(contract.String)
		codeStrings = append(codeStrings, string(s))
	}
	if !stringSlicesEqual(codeStrings, deriveTruncationCodes(omissions)) {
		return rejectedOutcome(reasonBindingInvalid)
	}

	// C7: typed result arrays.
	evidenceResults, evidenceOK := arrayOf(obj, "evidence_results")
	skillResults, skillOK := arrayOf(obj, "skill_results")
	if !evidenceOK || !skillOK {
		return rejectedOutcome(reasonFieldMissing)
	}
	for _, item := range evidenceResults {
		if entry, ok := item.(*contract.Object); !ok {
			return rejectedOutcome(reasonEnumInvalid)
		} else if rt, _ := contract.StringOf(entry, "result_type"); rt != "evidence" {
			return rejectedOutcome(reasonEnumInvalid)
		}
	}
	for _, item := range skillResults {
		if entry, ok := item.(*contract.Object); !ok {
			return rejectedOutcome(reasonEnumInvalid)
		} else if rt, _ := contract.StringOf(entry, "result_type"); rt != "skill" {
			return rejectedOutcome(reasonEnumInvalid)
		}
	}

	// Nested closed sub-objects needed by the accounting obligations.
	watermark, _ := objectOf(obj, "watermark")
	for _, field := range requiredWatermarkFields {
		if watermark == nil {
			return rejectedOutcome(reasonFieldMissing)
		}
		if _, present := watermark.Get(field); !present {
			return rejectedOutcome(reasonFieldMissing)
		}
	}
	budgets, _ := objectOf(obj, "budgets")
	for _, field := range requiredBudgetFields {
		if budgets == nil {
			return rejectedOutcome(reasonFieldMissing)
		}
		if _, present := budgets.Get(field); !present {
			return rejectedOutcome(reasonFieldMissing)
		}
	}
	fences, _ := objectOf(obj, "served_fences")
	for _, field := range requiredFenceFields {
		if fences == nil {
			return rejectedOutcome(reasonFieldMissing)
		}
		if _, present := fences.Get(field); !present {
			return rejectedOutcome(reasonFieldMissing)
		}
	}

	// C5: replayable budget accounting - only served items consume budget.
	budgetValues := map[string]*big.Int{}
	for _, field := range requiredBudgetFields {
		val, _ := budgets.Get(field)
		n, ok := isNonNegativeInt(val)
		if !ok {
			return rejectedOutcome(reasonBudgetInvalid)
		}
		budgetValues[field] = n
	}
	tokenSum := big.NewInt(0)
	for _, item := range skillResults {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		if view, ok := objectOf(entry, "guidance_view"); ok {
			if tokens, ok := intFieldOf(view, "content_token_count"); ok {
				tokenSum.Add(tokenSum, big.NewInt(tokens))
			}
		}
	}
	totalUsed := big.NewInt(int64(len(evidenceResults) + len(skillResults)))
	if budgetValues["total_used"].Cmp(totalUsed) != 0 ||
		budgetValues["total_used"].Cmp(budgetValues["total_cap"]) > 0 ||
		big.NewInt(int64(len(evidenceResults))).Cmp(budgetValues["evidence_subcap"]) > 0 ||
		big.NewInt(int64(len(skillResults))).Cmp(budgetValues["skill_subcap"]) > 0 ||
		tokenSum.Cmp(budgetValues["guidance_token_budget"]) > 0 {
		return rejectedOutcome(reasonBudgetInvalid)
	}

	// C5: truncation claims must be truthful (cap actually filled + entry).
	for _, code := range codeStrings {
		carried := 0
		carriedEvidence := false
		carriedSkill := false
		for _, item := range omissions {
			entry := item.(*contract.Object)
			entryCode, _ := contract.StringOf(entry, "reason_code")
			if entryCode != code {
				continue
			}
			carried++
			kind, _ := contract.StringOf(entry, "kind")
			if kind == "evidence" {
				carriedEvidence = true
			}
			if kind == "skill" {
				carriedSkill = true
			}
		}
		switch code {
		case "TOTAL_CAP_REACHED":
			if budgetValues["total_used"].Cmp(budgetValues["total_cap"]) != 0 || carried == 0 {
				return rejectedOutcome(reasonBudgetInvalid)
			}
		case "EVIDENCE_SUBCAP_REACHED":
			if big.NewInt(int64(len(evidenceResults))).Cmp(budgetValues["evidence_subcap"]) != 0 ||
				carried == 0 || !carriedEvidence {
				return rejectedOutcome(reasonBudgetInvalid)
			}
		case "SKILL_SUBCAP_REACHED":
			if big.NewInt(int64(len(skillResults))).Cmp(budgetValues["skill_subcap"]) != 0 ||
				carried == 0 || !carriedSkill {
				return rejectedOutcome(reasonBudgetInvalid)
			}
		case "GUIDANCE_TOKEN_BUDGET_REACHED":
			if !carriedSkill {
				return rejectedOutcome(reasonBudgetInvalid)
			}
		}
	}

	// C6: each newly served page strictly advances the session watermark.
	projectedRaw, _ := watermark.Get("projected_through_activation_sequence")
	projected, isInt := isNonNegativeInt(projectedRaw)
	if !isInt {
		return rejectedOutcome(reasonFieldMissing)
	}
	if session != nil {
		if prior, ok := objectOf(session, "prior_watermark"); ok {
			if priorRaw, present := prior.Get("projected_through_activation_sequence"); present {
				if priorProjected, isInt := isNonNegativeInt(priorRaw); isInt && projected.Cmp(priorProjected) <= 0 {
					return rejectedOutcome(reasonFenceConflict)
				}
			}
		}
	}
	return acceptedOutcome(obj)
}

// omissionEntryReason ports _check_omission_entry.
func omissionEntryReason(item contract.Value) string {
	entry, ok := item.(*contract.Object)
	if !ok {
		return reasonEnumInvalid
	}
	for _, key := range entry.Keys() {
		if _, allowed := omissionEntryFields[key]; !allowed {
			return reasonFieldUnknown
		}
	}
	kind, _ := contract.StringOf(entry, "kind")
	if _, ok := omissionKinds[kind]; !ok {
		// Includes branch-shaped entries: branch omission belongs to the
		// GuidanceView closed DTO, never to the top-level carrier.
		return reasonEnumInvalid
	}
	refVal, hasRef := entry.Get("ref")
	ref, refIsObject := refVal.(*contract.Object)
	if !hasRef || !refIsObject {
		return reasonNonExactRef
	}
	reasonCode, _ := contract.StringOf(entry, "reason_code")
	if _, ok := truncationReasonCodes[reasonCode]; !ok {
		return reasonEnumInvalid
	}
	refSchema, _ := contract.StringOf(ref, "schema_version")
	if refSchema == "" {
		return reasonNonExactRef
	}
	if refSchema != refSchemaByKind[kind] {
		return reasonRefMismatch
	}
	var exact string
	if kind == "evidence" {
		exact = exactEvidenceRefReason(ref)
	} else {
		exact = exactSkillRefReason(ref)
	}
	if exact != "" {
		return exact
	}
	if _, ok := kindCodeCompat[kind][reasonCode]; !ok {
		return reasonEnumInvalid
	}
	return ""
}

// exactEvidenceRefReason ports _check_exact_evidence_ref.
func exactEvidenceRefReason(ref *contract.Object) string {
	closed := map[string]struct{}{}
	for _, field := range evidenceRefClosed {
		closed[field] = struct{}{}
	}
	closed["source_segment_ref"] = struct{}{}
	for _, key := range ref.Keys() {
		if _, allowed := closed[key]; !allowed {
			return reasonNonExactRef
		}
	}
	for _, field := range evidenceRefRequired {
		if _, present := ref.Get(field); !present {
			return reasonNonExactRef
		}
	}
	if id, ok := contract.StringOf(ref, "evidence_id"); !ok || id == "" {
		return reasonNonExactRef
	}
	versionRaw, _ := ref.Get("version")
	if _, ok := isNonNegativeInt(versionRaw); !ok {
		return reasonNonExactRef
	}
	if version, ok := intFieldOf(ref, "version"); !ok || version < 1 {
		return reasonNonExactRef
	}
	if digestVal, _ := ref.Get("evidence_digest"); !isSHA256(digestVal) {
		return reasonNonExactRef
	}
	kindVal, _ := ref.Get("evidence_kind")
	if kind, ok := kindVal.(contract.String); !ok {
		return reasonNonExactRef
	} else if _, known := evidenceKinds[string(kind)]; !known {
		return reasonNonExactRef
	}
	stateVal, _ := ref.Get("commit_state")
	if state, ok := stateVal.(contract.String); !ok {
		return reasonNonExactRef
	} else if _, known := evidenceCommitStates[string(state)]; !known {
		return reasonEvidenceUncommitted
	}
	if segRaw, present := ref.Get("source_segment_ref"); present {
		if _, isObj := segRaw.(*contract.Object); !isObj {
			return reasonNonExactRef
		}
	}
	return ""
}

// exactSkillRefReason ports _check_exact_skill_ref.
func exactSkillRefReason(ref *contract.Object) string {
	closed := map[string]struct{}{}
	for _, field := range skillRefRequired {
		closed[field] = struct{}{}
	}
	for _, key := range ref.Keys() {
		if _, allowed := closed[key]; !allowed {
			return reasonNonExactRef
		}
	}
	for _, field := range skillRefRequired {
		if _, present := ref.Get(field); !present {
			return reasonNonExactRef
		}
	}
	if id, ok := contract.StringOf(ref, "lineage_id"); !ok || id == "" {
		return reasonNonExactRef
	}
	versionRaw, _ := ref.Get("version")
	if _, ok := isNonNegativeInt(versionRaw); !ok {
		return reasonNonExactRef
	}
	if version, ok := intFieldOf(ref, "version"); !ok || version < 1 {
		return reasonNonExactRef
	}
	if kindVal, _ := ref.Get("kind"); true {
		kind, ok := kindVal.(contract.String)
		if !ok {
			return reasonNonExactRef
		}
		if _, known := skillKinds[string(kind)]; !known {
			return reasonNonExactRef
		}
	}
	if digestVal, _ := ref.Get("artifact_digest"); !isSHA256(digestVal) {
		return reasonNonExactRef
	}
	return ""
}

// refKeys returns the JCS bytes of every omission entry ref.
func refKeys(omissions contract.Array) ([]string, string) {
	keys := make([]string, 0, len(omissions))
	for _, item := range omissions {
		entry := item.(*contract.Object)
		ref, _ := entry.Get("ref")
		bytes, err := contract.JCS(ref)
		if err != nil {
			return nil, reasonNonExactRef
		}
		keys = append(keys, string(bytes))
	}
	return keys, ""
}

// servedKeys returns the exact-ref JCS keys served by this payload's
// top-level results. A ref that cannot enter the hashed core fails closed
// (CanonicalizationError in the reference pipeline).
func servedKeys(payload *contract.Object) (map[string]struct{}, string) {
	served := map[string]struct{}{}
	for _, field := range []string{"evidence_results", "skill_results"} {
		arr, ok := arrayOf(payload, field)
		if !ok {
			continue
		}
		refField := "evidence_ref"
		if field == "skill_results" {
			refField = "skill_ref"
		}
		for _, item := range arr {
			if entry, ok := item.(*contract.Object); ok {
				if ref, present := entry.Get(refField); present {
					bytes, err := contract.JCS(ref)
					if err != nil {
						return nil, reasonNonExactRef
					}
					served[string(bytes)] = struct{}{}
				}
			}
		}
	}
	return served, ""
}

// priorServedKeys returns the exact-ref JCS keys already served by earlier
// pages of the session (fixture session.prior_served block).
func priorServedKeys(session *contract.Object) ([]string, string) {
	if session == nil {
		return nil, ""
	}
	prior, ok := objectOf(session, "prior_served")
	if !ok {
		return nil, ""
	}
	var keys []string
	for _, field := range []string{"evidence_refs", "skill_refs"} {
		arr, ok := arrayOf(prior, field)
		if !ok {
			continue
		}
		for _, item := range arr {
			bytes, err := contract.JCS(item)
			if err != nil {
				return nil, reasonNonExactRef
			}
			keys = append(keys, string(bytes))
		}
	}
	return keys, ""
}

// omissionsCanonicallyOrdered checks the frozen (kind asc, JCS ref asc)
// order. The Host validates this order; it never re-sorts content.
func omissionsCanonicallyOrdered(omissions contract.Array) (bool, string) {
	type key struct {
		kind string
		ref  string
	}
	prev := key{"", ""}
	for i, item := range omissions {
		entry := item.(*contract.Object)
		kind, _ := contract.StringOf(entry, "kind")
		ref, _ := entry.Get("ref")
		refBytes, err := contract.JCS(ref)
		if err != nil {
			return false, reasonNonExactRef
		}
		current := key{kind, string(refBytes)}
		if i > 0 {
			if prev.kind > current.kind || (prev.kind == current.kind && prev.ref >= current.ref) {
				return false, ""
			}
		}
		prev = current
	}
	return true, ""
}

// deriveTruncationCodes is the order-preserving dedupe of the entry
// reason codes (§12.7.2 C4).
func deriveTruncationCodes(omissions contract.Array) []string {
	codes := make([]string, 0, len(omissions))
	for _, item := range omissions {
		entry := item.(*contract.Object)
		code, _ := contract.StringOf(entry, "reason_code")
		found := false
		for _, existing := range codes {
			if existing == code {
				found = true
				break
			}
		}
		if !found {
			codes = append(codes, code)
		}
	}
	return codes
}

func stringSlicesEqual(a, b []string) bool {
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

// ---------------------------------------------------------------------------
// Session-layer additions beyond the frozen derivations
// ---------------------------------------------------------------------------

// candidateRefReason rejects results sourced from the candidate plane: no
// candidate artifact is executable or servable as an exact released ref
// (Contract §3.1, §9.1; GMS §10.4 historical_exact forbids candidates).
func candidateRefReason(payload *contract.Object) string {
	check := func(ref *contract.Object) bool {
		if ref == nil {
			return false
		}
		if sv, _ := contract.StringOf(ref, "schema_version"); sv == candidateRefSchema {
			return true
		}
		for _, marker := range candidateMarkerFields {
			if _, present := ref.Get(marker); present {
				return true
			}
		}
		return false
	}
	if arr, ok := arrayOf(payload, "skill_results"); ok {
		for _, item := range arr {
			entry, ok := item.(*contract.Object)
			if !ok {
				continue
			}
			if ref, ok := objectOf(entry, "skill_ref"); ok && check(ref) {
				return reasonCandidate
			}
			if identity, ok := objectOf(entry, "artifact_identity_citation"); ok {
				if ref, ok := objectOf(identity, "skill_ref"); ok && check(ref) {
					return reasonCandidate
				}
			}
		}
	}
	if omissions, ok := arrayOf(payload, "omissions"); ok {
		for _, item := range omissions {
			entry, ok := item.(*contract.Object)
			if !ok {
				continue
			}
			kind, _ := contract.StringOf(entry, "kind")
			if kind != "skill" {
				continue
			}
			if ref, ok := objectOf(entry, "ref"); ok && check(ref) {
				return reasonCandidate
			}
		}
	}
	return ""
}

// embeddedGuidanceViewReason checks every skill result's GuidanceView:
// view_hash digest consistency and the render/policy profile floors
// (skill_get matrix floors apply Host-side to embedded views too).
func embeddedGuidanceViewReason(payload *contract.Object, minRenderVersion, minPolicyVersion int64) string {
	arr, ok := arrayOf(payload, "skill_results")
	if !ok {
		return ""
	}
	for _, item := range arr {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		view, ok := objectOf(entry, "guidance_view")
		if !ok {
			continue
		}
		if render, ok := objectOf(view, "render_profile_ref"); ok {
			if version, ok := intFieldOf(render, "version"); ok && version < minRenderVersion {
				return reasonVersionUnsupported
			}
		}
		if policy, ok := objectOf(view, "policy_ref"); ok {
			if version, ok := intFieldOf(policy, "version"); ok && version < minPolicyVersion {
				return reasonVersionUnsupported
			}
		}
		if reason := guidanceViewHashReason(view); reason != "" {
			return reason
		}
	}
	return ""
}

// budgetsWithinCaps checks the returned budgets against the Host effective
// (clamped) budget ceilings: GMS was told the effective budgets; exceeding
// them is a budget violation, never something the Host trims locally.
func budgetsWithinCaps(payload *contract.Object, caps BudgetCaps) string {
	budgets, ok := objectOf(payload, "budgets")
	if !ok {
		return ""
	}
	check := func(field string, cap int64) bool {
		val, ok := intFieldOf(budgets, field)
		return ok && val > cap
	}
	if check("total_cap", caps.TotalCap) ||
		check("evidence_subcap", caps.EvidenceSubcap) ||
		check("skill_subcap", caps.SkillSubcap) ||
		check("guidance_token_budget", caps.GuidanceTokenBudget) {
		return reasonBudgetInvalid
	}
	return ""
}
