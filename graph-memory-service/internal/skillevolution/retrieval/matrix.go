// matrix.go is the digest-verified Contract §12.7.1 tool-success validation
// matrix (CTR-002): the frozen policy document, its Go evaluator and the
// skill_get readiness gate (M6). Every success payload the retrieval service
// returns is self-validated through Evaluate BEFORE it reaches the caller;
// a payload the frozen matrix rejects never leaves the service
// (TOOL_RESULT_BINDING_INVALID fail-closed).
package retrieval

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"river2.dev/graph-memory-service/internal/contract"
)

// Frozen matrix identifiers (policy schema, contract revision and the
// contract-frozen policy digest; a re-signed-but-unfrozen matrix is
// rejected, mirroring the Contract §13.7.1 R2 pinning discipline).
const (
	ToolPolicySchemaVersion  = "rsih-skill-evolution.tool-success-validation-policy.v1"
	ToolPolicyContractFreeze = "system-contract 12.7.1 v1.1 (CTR-002)"
	ToolPolicyDigestFrozen   = "sha256:dde91eff2aaac31057512beba1c667e0b07b7cb4c36037db4e07eaced6f702f5"
	ToolPolicyFile           = "tool-success-validation.v1.json"

	// The closed tool set v1 (matrix M1) and the gated tool.
	ToolExplore  = "memory_explore"
	ToolExpand   = "memory_expand"
	ToolSkillGet = "skill_get"

	// Fixture corpus shapes (VerifySkillGetReadiness walks the frozen
	// $FIX/tools corpus exactly like validate_tool_binding.py).
	caseSchemaVersion     = "rsih-skill-evolution.tool-binding-case.v1"
	expectedSchemaVersion = "rsih-skill-evolution.tool-binding-expected.v1"
	manifestSchemaVersion = "rsih-skill-evolution.tool-binding-manifest.v1"

	knownExtensionsV1 = 0 // no required extension is registered in v1 (Contract §6.4)
)

// toolBinding is one tool-name -> result-schema/ruleset binding (matrix M1).
type toolBinding struct {
	name                string
	resultSchemaVersion string
	ruleset             string
	closedFields        map[string]bool
	requiredFields      []string
	forbiddenFields     map[string]bool
	profileConstraints  map[string]int64 // min_<field>_version floors
}

// toolRuleset is one ordered rules array of the policy.
type toolRuleset struct {
	id    string
	rules []map[string]any
}

// ToolPolicy is one digest-verified §12.7.1 matrix document.
type ToolPolicy struct {
	Path   string
	Digest string

	document     map[string]any
	commonRules  []map[string]any
	commonIDs    map[string]map[string]any
	rulesets     map[string]*toolRuleset
	tools        map[string]*toolBinding
	knownSchemas map[string]bool

	// explore-family required sub-object fields (frozen by the policy).
	requiredWatermarkFields []string
	requiredBudgetFields    []string
	requiredFenceFields     []string

	gateFailureCode string
}

// Outcome is the derived matrix result of one (tool, payload) pair:
// Accept carries the canonical result digest; a rejection carries the
// closed reason code. Nothing here reads expected fixtures.
type Outcome struct {
	Accept       bool
	ReasonCode   string
	ResultDigest string
}

// ToolNames returns the closed tool set in policy document order
// (deterministic; the closed set is exactly explore/expand/skill_get).
func (p *ToolPolicy) ToolNames() []string {
	names := make([]string, 0, len(p.tools))
	for name := range p.tools {
		names = append(names, name)
	}
	sortStrings(names)
	return names
}

// GateFailureCode returns the skill_get readiness-gate failure code.
func (p *ToolPolicy) GateFailureCode() string { return p.gateFailureCode }

// commonCode resolves one common rule id to its reason code.
func (p *ToolPolicy) commonCode(ruleID string) string {
	rule, ok := p.commonIDs[ruleID]
	if !ok {
		return ""
	}
	code, _ := contract.AsString(rule["reason_code"])
	return code
}

// rulesetCode resolves one ruleset rule id to its reason code.
func (p *ToolPolicy) rulesetCode(rulesetID, ruleID string) string {
	rs := p.rulesets[rulesetID]
	if rs == nil {
		return ""
	}
	for _, rule := range rs.rules {
		if id, _ := contract.AsString(rule["rule_id"]); id == ruleID {
			code, _ := contract.AsString(rule["reason_code"])
			return code
		}
	}
	return ""
}

// ruleCode resolves a firing rule id (common or bound-ruleset) to its code.
func (p *ToolPolicy) ruleCode(binding *toolBinding, ruleID string) string {
	if _, common := p.commonIDs[ruleID]; common {
		return p.commonCode(ruleID)
	}
	return p.rulesetCode(binding.ruleset, ruleID)
}

// LoadToolPolicy loads and closure-checks the matrix policy: strict JSON,
// frozen schema_version, digest recomputation (JCS of the document minus
// policy_digest) pinned to the contract-frozen value, the two pinned
// §13.7.1 registry digests with every rule code present in them, closed
// tool set v1, and the skill_get gate staying disabled-until-green.
func LoadToolPolicy(path string) (*ToolPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("retrieval: read tool matrix policy: %w", err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return nil, fmt.Errorf("retrieval: %s is not valid UTF-8 JSON: %w", path, err)
	}
	document, ok := contract.AsObject(value)
	if !ok {
		return nil, fmt.Errorf("retrieval: %s: policy is not a JSON object", path)
	}
	if sv, _ := contract.AsString(document["schema_version"]); sv != ToolPolicySchemaVersion {
		return nil, fmt.Errorf("retrieval: %s: schema_version %q is not %q", path, sv, ToolPolicySchemaVersion)
	}
	declared, _ := contract.AsString(document["policy_digest"])
	body := make(map[string]any, len(document))
	for k, v := range document {
		if k != "policy_digest" {
			body[k] = v
		}
	}
	computed, err := contract.DigestOf(body)
	if err != nil {
		return nil, fmt.Errorf("retrieval: %s: policy body failed integer-only JCS: %w", path, err)
	}
	if declared != computed {
		return nil, fmt.Errorf("retrieval: POLICY_DIGEST_MISMATCH: %s: declared %s but recomputed %s", path, declared, computed)
	}
	if declared != ToolPolicyDigestFrozen {
		return nil, fmt.Errorf("retrieval: %s: policy digest %s does not match the contract-frozen %s", path, declared, ToolPolicyDigestFrozen)
	}

	// The policy MUST pin the Contract §13.7.1 R2 frozen registry digests,
	// and every rule code must exist in those registries.
	registryDigests, _ := contract.AsObject(document["reason_registry_digests"])
	systemDigest, _ := contract.AsString(registryDigests["system"])
	hostDigest, _ := contract.AsString(registryDigests["host-proxy"])
	if systemDigest != contract.SystemRegistryDigestFrozen || hostDigest != contract.HostProxyRegistryDigestFrozen {
		return nil, fmt.Errorf("retrieval: %s: reason_registry_digests must pin the Contract 13.7.1 R2 frozen values", path)
	}
	policyDir := filepath.Dir(path)
	systemPolicy, err := contract.LoadSystemReasonPolicy(policyDir)
	if err != nil {
		return nil, fmt.Errorf("retrieval: %s: %w", path, err)
	}
	hostPolicy, err := contract.LoadHostProxyReasonPolicy(policyDir)
	if err != nil {
		return nil, fmt.Errorf("retrieval: %s: %w", path, err)
	}
	codeRegistered := func(code string) bool {
		return systemPolicy.Has(code) || hostPolicy.Has(code)
	}

	// Rulesets: non-empty ordered rules arrays with registered codes.
	rulesetsRaw, _ := contract.AsObject(document["rulesets"])
	if len(rulesetsRaw) == 0 {
		return nil, fmt.Errorf("retrieval: %s: policy.rulesets must be a non-empty object", path)
	}
	rulesets := make(map[string]*toolRuleset, len(rulesetsRaw))
	for id, raw := range rulesetsRaw {
		obj, _ := contract.AsObject(raw)
		if obj == nil {
			return nil, fmt.Errorf("retrieval: %s: ruleset %q is not an object", path, id)
		}
		rulesRaw, _ := contract.AsArray(obj["rules"])
		if len(rulesRaw) == 0 {
			return nil, fmt.Errorf("retrieval: %s: ruleset %q must carry a non-empty rules array", path, id)
		}
		rules := make([]map[string]any, 0, len(rulesRaw))
		for i, ruleRaw := range rulesRaw {
			rule, _ := contract.AsObject(ruleRaw)
			if rule == nil {
				return nil, fmt.Errorf("retrieval: %s: ruleset %q rules[%d] is not an object", path, id, i)
			}
			for _, field := range []string{"reason_code", "host_code"} {
				code, _ := contract.AsString(rule[field])
				if !codeRegistered(code) {
					return nil, fmt.Errorf("retrieval: %s: ruleset %q rule %v %s=%q is not in the frozen reason registries",
						path, id, rule["rule_id"], field, code)
				}
			}
			rules = append(rules, rule)
		}
		rulesets[id] = &toolRuleset{id: id, rules: rules}
	}

	// Tools: closed set v1 with closure-consistent bindings.
	toolsRaw, _ := contract.AsObject(document["tools"])
	if len(toolsRaw) == 0 {
		return nil, fmt.Errorf("retrieval: %s: policy.tools must be a non-empty object", path)
	}
	tools := make(map[string]*toolBinding, len(toolsRaw))
	knownSchemas := map[string]bool{}
	for name, raw := range toolsRaw {
		obj, _ := contract.AsObject(raw)
		if obj == nil {
			return nil, fmt.Errorf("retrieval: %s: tool %q is not an object", path, name)
		}
		if echo, _ := contract.AsString(obj["tool_name"]); echo != name {
			return nil, fmt.Errorf("retrieval: %s: tool entry %q must echo tool_name", path, name)
		}
		rulesetID, _ := contract.AsString(obj["ruleset"])
		if _, ok := rulesets[rulesetID]; !ok {
			return nil, fmt.Errorf("retrieval: %s: tool %q references unknown ruleset %q", path, name, rulesetID)
		}
		resultSchema, _ := contract.AsString(obj["result_schema_version"])
		if resultSchema == "" {
			return nil, fmt.Errorf("retrieval: %s: tool %q must bind a result schema_version", path, name)
		}
		closed := map[string]bool{}
		for _, rawField := range arrayOfStrings(obj["closed_fields"]) {
			closed[rawField] = true
		}
		required := arrayOfStrings(obj["required_fields"])
		if !closed["schema_version"] || !containsString(required, "schema_version") {
			return nil, fmt.Errorf("retrieval: %s: tool %q must require schema_version", path, name)
		}
		for _, field := range required {
			if !closed[field] {
				return nil, fmt.Errorf("retrieval: %s: tool %q required_fields must be a subset of closed_fields", path, name)
			}
		}
		forbidden := map[string]bool{}
		for _, field := range arrayOfStrings(obj["forbidden_extraneous_fields"]) {
			forbidden[field] = true
			if closed[field] {
				return nil, fmt.Errorf("retrieval: %s: tool %q forbidden_extraneous_fields overlap closed_fields", path, name)
			}
		}
		constraints := map[string]int64{}
		constraintsRaw, _ := contract.AsObject(obj["profile_constraints"])
		for key, rawFloor := range constraintsRaw {
			if len(key) <= 12 || key[:4] != "min_" || key[len(key)-8:] != "_version" {
				return nil, fmt.Errorf("retrieval: %s: tool %q unsupported profile constraint key %q", path, name, key)
			}
			if !contract.IsIntegerNumber(rawFloor) {
				return nil, fmt.Errorf("retrieval: %s: tool %q profile constraint %q must be an integer", path, name, key)
			}
			floor, _ := rawFloor.(json.Number).Int64()
			constraints[key] = floor
		}
		tools[name] = &toolBinding{
			name: name, resultSchemaVersion: resultSchema, ruleset: rulesetID,
			closedFields: closed, requiredFields: required,
			forbiddenFields: forbidden, profileConstraints: constraints,
		}
		knownSchemas[resultSchema] = true
	}
	for _, want := range []string{ToolExplore, ToolExpand, ToolSkillGet} {
		if _, ok := tools[want]; !ok {
			return nil, fmt.Errorf("retrieval: %s: closed tool set v1 must be explore/expand/skill_get (missing %q)", path, want)
		}
	}
	if len(tools) != 3 {
		return nil, fmt.Errorf("retrieval: %s: closed tool set v1 must be exactly explore/expand/skill_get", path)
	}

	// Common rules: ordered, unique, registered codes.
	commonRaw, _ := contract.AsArray(document["common_rules"])
	if len(commonRaw) == 0 {
		return nil, fmt.Errorf("retrieval: %s: policy.common_rules must be a non-empty ordered array", path)
	}
	commonRules := make([]map[string]any, 0, len(commonRaw))
	commonIDs := map[string]map[string]any{}
	for i, ruleRaw := range commonRaw {
		rule, _ := contract.AsObject(ruleRaw)
		if rule == nil {
			return nil, fmt.Errorf("retrieval: %s: common_rules[%d] is not an object", path, i)
		}
		id, _ := contract.AsString(rule["rule_id"])
		if _, dup := commonIDs[id]; dup {
			return nil, fmt.Errorf("retrieval: %s: common rule id %q repeated", path, id)
		}
		for _, field := range []string{"reason_code", "host_code"} {
			code, _ := contract.AsString(rule[field])
			if !codeRegistered(code) {
				return nil, fmt.Errorf("retrieval: %s: common rule %q %s=%q is not in the frozen reason registries", path, id, field, code)
			}
		}
		commonRules = append(commonRules, rule)
		commonIDs[id] = rule
	}

	// The skill_get readiness gate must stay disabled-until-green (M6).
	gates, _ := contract.AsObject(document["readiness_gates"])
	gate, _ := contract.AsObject(gates[ToolSkillGet])
	if gate == nil {
		return nil, fmt.Errorf("retrieval: %s: readiness gate for %s missing", path, ToolSkillGet)
	}
	if state, _ := contract.AsString(gate["state"]); state != "disabled-until-green" {
		return nil, fmt.Errorf("retrieval: %s: readiness gate for %s must stay disabled-until-green until green", path, ToolSkillGet)
	}
	gateFailure, _ := contract.AsString(gate["failure_code"])

	// Explore-family required sub-object fields (frozen by the policy).
	exploreRuleset := rulesets["explore-family"]
	if exploreRuleset == nil {
		return nil, fmt.Errorf("retrieval: %s: explore-family ruleset missing", path)
	}
	exploreRaw, _ := contract.AsObject(rulesetsRaw["explore-family"])
	requiredWatermark := arrayOfStrings(exploreRaw["required_watermark_fields"])
	requiredBudgets := arrayOfStrings(exploreRaw["required_budget_fields"])
	requiredFences := arrayOfStrings(exploreRaw["required_served_fence_fields"])

	return &ToolPolicy{
		Path: path, Digest: declared, document: document,
		commonRules: commonRules, commonIDs: commonIDs,
		rulesets: rulesets, tools: tools, knownSchemas: knownSchemas,
		requiredWatermarkFields: requiredWatermark,
		requiredBudgetFields:    requiredBudgets,
		requiredFenceFields:     requiredFences,
		gateFailureCode:         gateFailure,
	}, nil
}

// ---------------------------------------------------------------------------
// Evaluator (Go port of the frozen derivation; first closed failure wins)
// ---------------------------------------------------------------------------

// Evaluate derives the matrix outcome of one (tool, result payload) pair.
// request and audit are the Contract §7.18 ToolProxyRequest and the
// authoritative read audit record (maps, decoder model); nil maps are
// treated as empty. An accepted outcome carries the canonical result digest.
func (p *ToolPolicy) Evaluate(toolName string, result, request, audit any) Outcome {
	requestMap, _ := contract.AsObject(request)
	if requestMap == nil {
		requestMap = map[string]any{}
	}
	auditMap, _ := contract.AsObject(audit)
	if auditMap == nil {
		auditMap = map[string]any{}
	}

	binding, bound := p.tools[toolName]
	if !bound {
		return Outcome{Accept: false, ReasonCode: p.commonCode("unknown_tool_name")}
	}

	if ruleID := p.checkCommon(result, requestMap, auditMap, binding); ruleID != "" {
		return Outcome{Accept: false, ReasonCode: p.ruleCode(binding, ruleID)}
	}
	var ruleID string
	switch binding.ruleset {
	case "explore-family":
		ruleID = p.checkExploreFamily(result, requestMap, binding)
	case "guidance-view-closed":
		ruleID = p.checkGuidanceViewClosed(result, requestMap, auditMap, binding)
	default:
		ruleID = "unknown_ruleset"
	}
	if ruleID != "" {
		return Outcome{Accept: false, ReasonCode: p.ruleCode(binding, ruleID)}
	}
	payload, _ := contract.AsObject(result)
	digest, err := contract.DigestOf(payload)
	if err != nil {
		return Outcome{Accept: false, ReasonCode: ReasonArtifactBodyInvalid}
	}
	return Outcome{Accept: true, ResultDigest: digest}
}

// checkCommon is the shared layer (matrix M5) in policy.common_rules order.
func (p *ToolPolicy) checkCommon(result any, request, audit map[string]any, binding *toolBinding) string {
	if unknownRequiredExtension(request) {
		return "unknown_required_extension"
	}
	payload, isObject := contract.AsObject(result)
	if isObject && unknownRequiredExtension(payload) {
		return "unknown_required_extension"
	}
	if !isObject {
		return "result_body_not_closed"
	}
	schemaVersion, _ := contract.AsString(payload["schema_version"])
	if !p.knownSchemas[schemaVersion] {
		return "result_body_not_closed"
	}
	if schemaVersion != binding.resultSchemaVersion {
		return "result_schema_mismatch"
	}
	for key := range payload {
		// extensions are permitted on any closed DTO (Contract §7.1/§6.4).
		if key == "extensions" || binding.closedFields[key] {
			continue
		}
		return "closed_field_unknown"
	}
	for _, field := range binding.requiredFields {
		if _, present := payload[field]; !present {
			return "required_field_missing"
		}
	}
	// Authorization is carried by request + authoritative read audit record.
	for _, field := range []string{"room_id", "agent_id", "scope_profile_ref"} {
		auditValue, inAudit := audit[field]
		requestValue, inRequest := request[field]
		if inAudit && inRequest && !contract.EqualJSON(auditValue, requestValue) {
			return "scope_violation"
		}
	}
	return ""
}

// checkExploreFamily is the explore/expand ruleset (matrix M2/M3).
func (p *ToolPolicy) checkExploreFamily(result any, request map[string]any, binding *toolBinding) string {
	payload, _ := contract.AsObject(result)
	watermark, _ := contract.AsObject(payload["watermark"])
	for _, field := range p.requiredWatermarkFields {
		if watermark == nil {
			break
		}
		if _, present := watermark[field]; !present {
			return "nested_required_field_missing"
		}
	}
	if watermark == nil {
		return "nested_required_field_missing"
	}
	budgets, _ := contract.AsObject(payload["budgets"])
	if budgets == nil {
		return "nested_required_field_missing"
	}
	for _, field := range p.requiredBudgetFields {
		if _, present := budgets[field]; !present {
			return "nested_required_field_missing"
		}
	}
	fences, _ := contract.AsObject(payload["served_fences"])
	if fences == nil {
		return "nested_required_field_missing"
	}
	for _, field := range p.requiredFenceFields {
		if _, present := fences[field]; !present {
			return "nested_required_field_missing"
		}
	}

	// Budgets consistency (Contract §12.4).
	for _, field := range p.requiredBudgetFields {
		if !nonNegativeInt(budgets[field]) {
			return "budget_inconsistent"
		}
	}
	evidenceResults, evidenceOK := contract.AsArray(payload["evidence_results"])
	skillResults, skillOK := contract.AsArray(payload["skill_results"])
	if !evidenceOK || !skillOK {
		return "nested_required_field_missing"
	}
	totalUsed, _ := intValue(budgets["total_used"])
	totalCap, _ := intValue(budgets["total_cap"])
	evidenceSubcap, _ := intValue(budgets["evidence_subcap"])
	skillSubcap, _ := intValue(budgets["skill_subcap"])
	tokenBudget, _ := intValue(budgets["guidance_token_budget"])
	if totalUsed > totalCap || int64(len(evidenceResults)) > evidenceSubcap || int64(len(skillResults)) > skillSubcap {
		return "budget_inconsistent"
	}
	var tokenSum int64
	for _, raw := range skillResults {
		entry, _ := contract.AsObject(raw)
		view, _ := contract.AsObject(entry["guidance_view"])
		if count, ok := intValue(view["content_token_count"]); ok {
			tokenSum += count
		}
	}
	if tokenSum > tokenBudget {
		return "budget_inconsistent"
	}

	// Typed results (Contract §7.16).
	for _, raw := range evidenceResults {
		entry, _ := contract.AsObject(raw)
		if resultType, _ := contract.AsString(entry["result_type"]); resultType != "evidence" {
			return "result_type_invalid"
		}
	}
	for _, raw := range skillResults {
		entry, _ := contract.AsObject(raw)
		if resultType, _ := contract.AsString(entry["result_type"]); resultType != "skill" {
			return "result_type_invalid"
		}
	}

	// Typed citations (Contract §12.6).
	for _, raw := range evidenceResults {
		entry, _ := contract.AsObject(raw)
		if !validEvidenceCitation(entry["citation"]) {
			return "citation_invalid"
		}
	}
	for _, raw := range skillResults {
		entry, _ := contract.AsObject(raw)
		identity, _ := contract.AsObject(entry["artifact_identity_citation"])
		skillRef, _ := contract.AsObject(identity["skill_ref"])
		if identity == nil || skillRef == nil {
			return "citation_invalid"
		}
		citations, ok := contract.AsArray(entry["evidence_citations"])
		if !ok {
			return "citation_invalid"
		}
		for _, citationRaw := range citations {
			if !validEvidenceCitation(citationRaw) {
				return "citation_invalid"
			}
		}
	}

	// Top-level omitted-refs carrier (Contract §12.7.2 v1.1, CTR-003):
	// non-empty truncation codes without a non-empty omissions carrier is
	// silent truncation and fails closed.
	codes, _ := contract.AsArray(payload["truncation_reason_codes"])
	omissions, _ := contract.AsArray(payload["omissions"])
	if len(codes) > 0 && len(omissions) == 0 {
		return "truncation_without_omission_carrier"
	}

	if !profileFloorsSatisfied(payload, binding) {
		return "profile_version_stale"
	}

	// Freshness rides the request fields vs the result watermark.
	if requested, ok := intValue(request["requested_min_activation_sequence"]); ok {
		projected, projectedOK := intValue(watermark["projected_through_activation_sequence"])
		if !projectedOK || projected < requested {
			return "freshness_behind"
		}
	}
	return ""
}

// checkGuidanceViewClosed is the skill_get ruleset (matrix M4): only the
// applicable closed GuidanceView rules; freshness/authorization ride the
// request + read audit, never DTO fields.
func (p *ToolPolicy) checkGuidanceViewClosed(result any, request, audit map[string]any, binding *toolBinding) string {
	payload, _ := contract.AsObject(result)
	if !profileFloorsSatisfied(payload, binding) {
		return "profile_version_stale"
	}
	preimage := make(map[string]any, len(payload))
	for key, value := range payload {
		if key != "view_hash" {
			preimage[key] = value
		}
	}
	digest, err := contract.DigestOf(preimage)
	if err != nil {
		return "guidance_view_hash_mismatch"
	}
	if declared, _ := contract.AsString(payload["view_hash"]); declared != digest {
		return "guidance_view_hash_mismatch"
	}
	// Freshness rides the request vs the authoritative read audit record
	// (current_active checks the head directly, GMS §10.10).
	if requested, ok := intValue(request["requested_min_activation_sequence"]); ok {
		activeHead, headOK := intValue(audit["active_head_activation_sequence"])
		if !headOK || activeHead < requested {
			return "freshness_behind"
		}
	}
	return ""
}

// unknownRequiredExtension reports a required:true extension key that is not
// registered in v1 (no extension is registered, Contract §6.4).
func unknownRequiredExtension(container map[string]any) bool {
	extensions, _ := contract.AsObject(container["extensions"])
	for key, raw := range extensions {
		entry, _ := contract.AsObject(raw)
		if entry == nil {
			continue
		}
		if required, ok := entry["required"].(bool); ok && required {
			_ = key
			return true // KNOWN_EXTENSIONS_V1 is empty in v1
		}
	}
	return false
}

// profileFloorsSatisfied checks the min_<field>_version floors.
func profileFloorsSatisfied(payload map[string]any, binding *toolBinding) bool {
	for key, floor := range binding.profileConstraints {
		field := key[4 : len(key)-8]
		ref, _ := contract.AsObject(payload[field])
		if ref == nil {
			continue
		}
		if version, ok := intValue(ref["version"]); ok && version < floor {
			return false
		}
	}
	return true
}

func validEvidenceCitation(raw any) bool {
	citation, _ := contract.AsObject(raw)
	if citation == nil {
		return false
	}
	evidenceRef, _ := contract.AsObject(citation["evidence_ref"])
	claim, claimOK := contract.AsString(citation["claim"])
	return evidenceRef != nil && claimOK && claim != ""
}

func nonNegativeInt(v any) bool {
	n, ok := intValue(v)
	return ok && n >= 0
}

func intValue(v any) (int64, bool) {
	if !contract.IsIntegerNumber(v) {
		return 0, false
	}
	n, err := v.(json.Number).Int64()
	if err != nil {
		return 0, false
	}
	return n, true
}

func arrayOfStrings(raw any) []string {
	items, _ := contract.AsArray(raw)
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, _ := contract.AsString(item)
		out = append(out, s)
	}
	return out
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Readiness gate (M6): the gate is green only when the policy digest
// verifies and every frozen tools/ skill-get case derives exactly its
// expected accept/reason (the Go mirror of validate_tool_binding.py).
// ---------------------------------------------------------------------------

// VerifySkillGetReadiness evaluates the frozen $FIX/tools corpus with the
// digest-verified matrix and reports whether the skill_get readiness gate
// is green. Any corpus/derivation inconsistency is an error (the gate never
// turns green on a broken corpus).
func VerifySkillGetReadiness(conformanceDir string) (bool, error) {
	policyPath := filepath.Join(conformanceDir, "policy", ToolPolicyFile)
	policy, err := LoadToolPolicy(policyPath)
	if err != nil {
		return false, err
	}
	toolsDir := filepath.Join(conformanceDir, "tools")
	manifestValue, err := readStrictJSON(filepath.Join(toolsDir, "manifest.json"))
	if err != nil {
		return false, fmt.Errorf("retrieval: readiness gate: %w", err)
	}
	manifest, _ := contract.AsObject(manifestValue)
	if manifest == nil {
		return false, fmt.Errorf("retrieval: readiness gate: tools manifest is not an object")
	}
	if sv, _ := contract.AsString(manifest["schema_version"]); sv != manifestSchemaVersion {
		return false, fmt.Errorf("retrieval: readiness gate: tools manifest schema_version %q is not %q", sv, manifestSchemaVersion)
	}
	entries, _ := contract.AsArray(manifest["cases"])
	if len(entries) == 0 {
		return false, fmt.Errorf("retrieval: readiness gate: tools manifest carries no cases")
	}
	skillGetCases := 0
	gateGreen := true
	for i, raw := range entries {
		entry, _ := contract.AsObject(raw)
		if entry == nil {
			return false, fmt.Errorf("retrieval: readiness gate: manifest cases[%d] is not an object", i)
		}
		caseID, _ := contract.AsString(entry["case_id"])
		category, _ := contract.AsString(entry["category"])
		inputValue, err := readStrictJSON(filepath.Join(toolsDir, mustString(entry, "input_path")))
		if err != nil {
			return false, fmt.Errorf("retrieval: readiness gate: case %s: %w", caseID, err)
		}
		input, _ := contract.AsObject(inputValue)
		if input == nil {
			return false, fmt.Errorf("retrieval: readiness gate: case %s input is not an object", caseID)
		}
		if sv, _ := contract.AsString(input["schema_version"]); sv != caseSchemaVersion {
			return false, fmt.Errorf("retrieval: readiness gate: case %s input schema_version %q is not %q", caseID, sv, caseSchemaVersion)
		}
		if id, _ := contract.AsString(input["case_id"]); id != caseID {
			return false, fmt.Errorf("retrieval: readiness gate: case %s input case_id mismatch", caseID)
		}
		expectedValue, err := readStrictJSON(filepath.Join(toolsDir, mustString(entry, "expected_path")))
		if err != nil {
			return false, fmt.Errorf("retrieval: readiness gate: case %s: %w", caseID, err)
		}
		expected, _ := contract.AsObject(expectedValue)
		if expected == nil {
			return false, fmt.Errorf("retrieval: readiness gate: case %s expected is not an object", caseID)
		}
		if sv, _ := contract.AsString(expected["schema_version"]); sv != expectedSchemaVersion {
			return false, fmt.Errorf("retrieval: readiness gate: case %s expected schema_version %q is not %q", caseID, sv, expectedSchemaVersion)
		}
		toolName, _ := contract.AsString(input["tool_name"])
		outcome := policy.Evaluate(toolName, input["result_payload"], input["request"], input["read_audit"])
		wantAccept, _ := expected["expected_accept"].(bool)
		wantReason, _ := contract.AsString(expected["expected_reason_code"])
		wantDigest, _ := contract.AsString(expected["expected_result_digest"])
		ok := outcome.Accept == wantAccept
		if ok && outcome.Accept {
			ok = outcome.ResultDigest == wantDigest
		} else if ok {
			ok = outcome.ReasonCode == wantReason
		}
		if !ok {
			// A diverging evaluator never turns the gate green: every case
			// of the frozen corpus must derive its expected outcome.
			return false, fmt.Errorf("retrieval: readiness gate: case %s (%s) derives accept=%v reason=%q digest=%s, diverging from the frozen expectation",
				caseID, category, outcome.Accept, outcome.ReasonCode, outcome.ResultDigest)
		}
		if category == "skill-get" {
			skillGetCases++
		}
	}
	return gateGreen && skillGetCases > 0, nil
}

func mustString(obj map[string]any, field string) string {
	s, _ := contract.AsString(obj[field])
	return s
}

func readStrictJSON(path string) (any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return contract.ParseJSONStrict(data)
}
