package toolproxy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// Frozen DTO markers (Contract §7.17/§7.18) and module-private record forms.
const (
	// RequestSchemaVersion is the Contract §7.17 ToolProxyRequest marker.
	RequestSchemaVersion = "host.tool-proxy-request.v1"
	// ResultSchemaVersion is the Contract §7.18 ToolProxyResult marker.
	ResultSchemaVersion = "host.tool-proxy-result.v1"
	// upstreamAttemptSchema marks the Host-module-private immutable
	// upstream-attempt record digested into upstream_result_digest when no
	// upstream payload exists (Host §5.5.4: not_started|no_response|cancelled,
	// never a forged GMS payload).
	upstreamAttemptSchema = "host.tool-proxy-upstream-attempt.v1"
	// observedUpstreamFailureSchema marks the module-private canonical form
	// of an observed upstream failure envelope (the exact observation class
	// the Host saw, when no success payload was returned).
	observedUpstreamFailureSchema = "host.observed-upstream-failure.v1"

	policyFileName  = "tool-success-validation.v1.json"
	policyDigestKey = "policy_digest"

	// DefaultTimeoutMillis is the profile default request timeout.
	DefaultTimeoutMillis = 5000
)

// Host-local reason codes (Host Spec §5.9). The constants only name codes;
// behavioral semantics (status/retryable/terminal) always come from the
// digest-verified registries, never from this file.
const (
	CodeInvalidRequest      = "HOST_PROXY_INVALID_REQUEST"
	CodeAgentNotInRoom      = "HOST_AGENT_NOT_IN_ROOM"
	CodeScopeDenied         = "HOST_SCOPE_DENIED"
	CodeToolNotAllowed      = "HOST_TOOL_NOT_ALLOWED"
	CodeTimeout             = "HOST_PROXY_TIMEOUT"
	CodeCancelled           = "HOST_PROXY_CANCELLED"
	CodeGMSUnavailable      = "GMS_UNAVAILABLE"
	CodeUpstreamSchema      = "UPSTREAM_SCHEMA_INVALID"
	CodeUpstreamDigest      = "UPSTREAM_DIGEST_MISMATCH"
	CodeUpstreamScope       = "UPSTREAM_SCOPE_VIOLATION"
	CodeUpstreamBudget      = "UPSTREAM_BUDGET_VIOLATION"
	CodeBehindSequence      = "PROJECTION_BEHIND_REQUIRED_SEQUENCE"
	CodeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	CodeReturnChannelFailed = "PI_RETURN_CHANNEL_FAILED"
)

// ---------------------------------------------------------------------------
// Tool-success validation matrix policy (Contract §12.7.1, CTR-002)
// ---------------------------------------------------------------------------

// ToolRule is one matrix rule: the system-side canonical reason code plus the
// Host-owned code the Host emits under §13.7.1 R3 precedence 1.
type ToolRule struct {
	ID         string
	ReasonCode string // system registry canonical value
	HostCode   string // host-proxy registry value the Host emits
}

// ToolBinding is one tool name's matrix entry (M1): result schema, closed and
// required field sets, wrapper prohibitions and profile version floors.
type ToolBinding struct {
	Name                string
	ResultSchemaVersion string
	Ruleset             string
	ClosedFields        []string
	RequiredFields      []string
	ForbiddenFields     []string
	ApplicableRules     []string
	NonApplicableRules  []string
	ProfileConstraints  map[string]int64 // min_<field>_version floors
}

// Ruleset is one ordered rule collection plus its closed nested-field sets.
type Ruleset struct {
	ID                      string
	Rules                   []ToolRule
	RequiredWatermarkFields []string
	RequiredBudgetFields    []string
	RequiredFenceFields     []string
}

// Policy is the digest-verified tool-success validation matrix.
type Policy struct {
	Digest      string
	Tools       map[string]ToolBinding
	ToolOrder   []string
	Rulesets    map[string]Ruleset
	CommonRules []ToolRule
	// HostCodeFor maps a system canonical reason code to the Host-emitted
	// code frozen for it by the policy's rules.
	HostCodeFor map[string]string

	knownResultSchemas map[string]struct{}
}

// IsProxyTool reports whether tool is in the closed tool set v1.
func (p *Policy) IsProxyTool(tool string) bool {
	_, ok := p.Tools[tool]
	return ok
}

// LoadToolPolicy loads and digest-verifies the CTR-002 matrix policy. Any
// drift — tampered file, reordered keys, unknown tool set, a host/system code
// missing from the digest-verified registries — fails closed (Contract
// §12.7.1 M1: loaders MUST recompute the digest).
func LoadToolPolicy(path string) (*Policy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read tool policy: %w", err)
	}
	v, err := contract.ParseJSON(data)
	if err != nil {
		return nil, fmt.Errorf("tool policy %s is not valid JSON: %w", filepath.Base(path), err)
	}
	doc, ok := v.(*contract.Object)
	if !ok {
		return nil, fmt.Errorf("tool policy %s: document is not an object", filepath.Base(path))
	}
	if sv, _ := contract.StringOf(doc, "schema_version"); sv != "rsih-skill-evolution.tool-success-validation-policy.v1" {
		return nil, fmt.Errorf("tool policy %s: unknown schema_version", filepath.Base(path))
	}
	// Digest verification: JCS over the document minus policy_digest.
	declared, _ := contract.StringOf(doc, policyDigestKey)
	body := contract.NewObject()
	for _, key := range doc.Keys() {
		if key == policyDigestKey {
			continue
		}
		val, _ := doc.Get(key)
		body.Set(key, val)
	}
	recomputed, err := contract.DigestOf(body)
	if err != nil {
		return nil, fmt.Errorf("tool policy %s: digest preimage not canonicalizable: %w", filepath.Base(path), err)
	}
	if recomputed != declared {
		return nil, fmt.Errorf("tool policy %s: policy digest mismatch: declared %s but recomputed %s (fail closed)", filepath.Base(path), declared, recomputed)
	}

	policy := &Policy{
		Digest:             declared,
		Tools:              map[string]ToolBinding{},
		Rulesets:           map[string]Ruleset{},
		HostCodeFor:        map[string]string{},
		knownResultSchemas: map[string]struct{}{},
	}
	rulesetsVal, _ := doc.Get("rulesets")
	rulesets, ok := rulesetsVal.(*contract.Object)
	if !ok {
		return nil, fmt.Errorf("tool policy %s: rulesets must be an object", filepath.Base(path))
	}
	for _, id := range rulesets.Keys() {
		rawVal, _ := rulesets.Get(id)
		raw, ok := rawVal.(*contract.Object)
		if !ok {
			return nil, fmt.Errorf("tool policy %s: ruleset %s must be an object", filepath.Base(path), id)
		}
		ruleset := Ruleset{ID: id}
		rulesVal, _ := raw.Get("rules")
		rules, ok := rulesVal.(contract.Array)
		if !ok || len(rules) == 0 {
			return nil, fmt.Errorf("tool policy %s: ruleset %s must carry a non-empty rules array", filepath.Base(path), id)
		}
		for _, ruleVal := range rules {
			rule, ok := ruleVal.(*contract.Object)
			if !ok {
				return nil, fmt.Errorf("tool policy %s: ruleset %s has a non-object rule", filepath.Base(path), id)
			}
			parsed, err := parseToolRule(rule)
			if err != nil {
				return nil, fmt.Errorf("tool policy %s: ruleset %s: %w", filepath.Base(path), id, err)
			}
			ruleset.Rules = append(ruleset.Rules, parsed)
		}
		ruleset.RequiredWatermarkFields = stringList(raw, "required_watermark_fields")
		ruleset.RequiredBudgetFields = stringList(raw, "required_budget_fields")
		ruleset.RequiredFenceFields = stringList(raw, "required_served_fence_fields")
		policy.Rulesets[id] = ruleset
	}

	toolsVal, _ := doc.Get("tools")
	tools, ok := toolsVal.(*contract.Object)
	if !ok || len(tools.Keys()) == 0 {
		return nil, fmt.Errorf("tool policy %s: tools must be a non-empty object", filepath.Base(path))
	}
	for _, name := range tools.Keys() {
		rawVal, _ := tools.Get(name)
		raw, ok := rawVal.(*contract.Object)
		if !ok {
			return nil, fmt.Errorf("tool policy %s: tool %s must be an object", filepath.Base(path), name)
		}
		if echo, _ := contract.StringOf(raw, "tool_name"); echo != name {
			return nil, fmt.Errorf("tool policy %s: tool entry %q must echo tool_name", filepath.Base(path), name)
		}
		binding := ToolBinding{
			Name:                name,
			ResultSchemaVersion: stringField(raw, "result_schema_version"),
			Ruleset:             stringField(raw, "ruleset"),
			ClosedFields:        stringList(raw, "closed_fields"),
			RequiredFields:      stringList(raw, "required_fields"),
			ForbiddenFields:     stringList(raw, "forbidden_extraneous_fields"),
			ApplicableRules:     stringList(raw, "applicable_rules"),
			NonApplicableRules:  stringList(raw, "non_applicable_rules"),
			ProfileConstraints:  map[string]int64{},
		}
		if binding.ResultSchemaVersion == "" {
			return nil, fmt.Errorf("tool policy %s: tool %s must bind a result schema_version", filepath.Base(path), name)
		}
		if _, known := policy.Rulesets[binding.Ruleset]; !known {
			return nil, fmt.Errorf("tool policy %s: tool %s references unknown ruleset %q", filepath.Base(path), name, binding.Ruleset)
		}
		closed := map[string]struct{}{}
		for _, f := range binding.ClosedFields {
			closed[f] = struct{}{}
		}
		for _, f := range binding.RequiredFields {
			if _, ok := closed[f]; !ok {
				return nil, fmt.Errorf("tool policy %s: tool %s required_fields must be a subset of closed_fields", filepath.Base(path), name)
			}
		}
		if constraintsVal, present := raw.Get("profile_constraints"); present {
			constraints, ok := constraintsVal.(*contract.Object)
			if !ok {
				return nil, fmt.Errorf("tool policy %s: tool %s profile_constraints must be an object", filepath.Base(path), name)
			}
			for _, key := range constraints.Keys() {
				floorVal, _ := constraints.Get(key)
				floor, ok := floorVal.(contract.Number)
				if !ok || !floor.IsInteger() {
					return nil, fmt.Errorf("tool policy %s: tool %s profile constraint %s must be an integer", filepath.Base(path), name, key)
				}
				bi, _ := floor.Int()
				binding.ProfileConstraints[key] = bi.Int64()
			}
		}
		policy.Tools[name] = binding
		policy.ToolOrder = append(policy.ToolOrder, name)
		policy.knownResultSchemas[binding.ResultSchemaVersion] = struct{}{}
	}
	if len(policy.Tools) != 3 || !policy.IsProxyTool("memory_explore") || !policy.IsProxyTool("memory_expand") || !policy.IsProxyTool("skill_get") {
		return nil, fmt.Errorf("tool policy %s: closed tool set v1 must be memory_explore/memory_expand/skill_get", filepath.Base(path))
	}

	commonVal, _ := doc.Get("common_rules")
	common, ok := commonVal.(contract.Array)
	if !ok || len(common) == 0 {
		return nil, fmt.Errorf("tool policy %s: common_rules must be a non-empty ordered array", filepath.Base(path))
	}
	seen := map[string]struct{}{}
	for _, ruleVal := range common {
		rule, ok := ruleVal.(*contract.Object)
		if !ok {
			return nil, fmt.Errorf("tool policy %s: common_rules entries must be objects", filepath.Base(path))
		}
		parsed, err := parseToolRule(rule)
		if err != nil {
			return nil, fmt.Errorf("tool policy %s: common_rules: %w", filepath.Base(path), err)
		}
		if _, dup := seen[parsed.ID]; dup {
			return nil, fmt.Errorf("tool policy %s: duplicate common rule id %q", filepath.Base(path), parsed.ID)
		}
		seen[parsed.ID] = struct{}{}
		policy.CommonRules = append(policy.CommonRules, parsed)
	}
	for _, rule := range policy.CommonRules {
		policy.HostCodeFor[rule.ReasonCode] = rule.HostCode
	}
	for _, ruleset := range policy.Rulesets {
		for _, rule := range ruleset.Rules {
			policy.HostCodeFor[rule.ReasonCode] = rule.HostCode
		}
	}
	return policy, nil
}

func parseToolRule(rule *contract.Object) (ToolRule, error) {
	parsed := ToolRule{
		ID:         stringField(rule, "rule_id"),
		ReasonCode: stringField(rule, "reason_code"),
		HostCode:   stringField(rule, "host_code"),
	}
	if parsed.ID == "" || parsed.ReasonCode == "" || parsed.HostCode == "" {
		return ToolRule{}, fmt.Errorf("rule %q must carry rule_id, reason_code and host_code", parsed.ID)
	}
	return parsed, nil
}

func stringField(obj *contract.Object, key string) string {
	s, _ := contract.StringOf(obj, key)
	return s
}

func stringList(obj *contract.Object, key string) []string {
	val, present := obj.Get(key)
	if !present {
		return nil
	}
	arr, ok := val.(contract.Array)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(contract.String); ok {
			out = append(out, string(s))
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Upstream port (the GMS Tool Proxy boundary)
// ---------------------------------------------------------------------------

// UpstreamStatus is the wire status of an upstream response.
type UpstreamStatus string

// Upstream statuses on the proxy boundary.
const (
	UpstreamSucceeded    UpstreamStatus = "succeeded"
	UpstreamFailed       UpstreamStatus = "failed"
	UpstreamInconclusive UpstreamStatus = "inconclusive"
)

// UpstreamResponse is what the upstream (GMS) returned for one proxy request.
// Result is the raw success payload (validated by the matrix); ReasonCode is
// the upstream failure envelope code; ReadAudit is the authoritative read
// audit record that accompanies a read (Contract §12.7.1 M4/M5).
type UpstreamResponse struct {
	Status     UpstreamStatus
	Result     contract.Value
	ReadAudit  contract.Value
	ReasonCode string
	Message    string
}

// Upstream is the GMS Tool Proxy caller port. Production wiring (GMS-206 /
// INT-002) adapts the HTTP endpoint; tests use in-process fakes.
type Upstream interface {
	Call(ctx context.Context, request contract.Value) (*UpstreamResponse, error)
}

// ---------------------------------------------------------------------------
// Service: the proxy core
// ---------------------------------------------------------------------------

type lease struct {
	requestDigest string
	done          chan struct{}
	terminal      *Result
}

// Service executes ToolProxyRequests: request validation, idempotency lease,
// upstream call with timeout/cancel, CTR-002 success validation, §13.7.1
// reason precedence, and exact §7.18 result construction with terminal CAS.
type Service struct {
	policy   *Policy
	reasons  *contract.ReasonBundle
	upstream Upstream
	audit    *AuditStore

	mu        sync.Mutex
	leases    map[string]*lease  // idempotency key -> in-flight/finished lease
	terminals map[string]*Result // proxy_request_id -> fixed terminal (CAS)
}

// NewService builds a proxy core over a digest-verified matrix policy, the
// frozen reason registries, an upstream caller and an audit store.
func NewService(policy *Policy, reasons *contract.ReasonBundle, upstream Upstream, audit *AuditStore) (*Service, error) {
	if policy == nil || reasons == nil || audit == nil {
		return nil, errors.New("toolproxy: policy, reason bundle and audit store are required")
	}
	for _, rule := range policy.CommonRules {
		if err := verifyRegistryCodes(reasons, rule); err != nil {
			return nil, err
		}
	}
	for _, ruleset := range policy.Rulesets {
		for _, rule := range ruleset.Rules {
			if err := verifyRegistryCodes(reasons, rule); err != nil {
				return nil, err
			}
		}
	}
	if upstream == nil {
		return nil, errors.New("toolproxy: upstream caller is required")
	}
	return &Service{
		policy:    policy,
		reasons:   reasons,
		upstream:  upstream,
		audit:     audit,
		leases:    map[string]*lease{},
		terminals: map[string]*Result{},
	}, nil
}

func verifyRegistryCodes(bundle *contract.ReasonBundle, rule ToolRule) error {
	if _, ok := bundle.System.Lookup(rule.ReasonCode); !ok {
		return fmt.Errorf("toolproxy: matrix rule %q system code %q is not in the frozen system registry", rule.ID, rule.ReasonCode)
	}
	if _, ok := bundle.Lookup(rule.HostCode); !ok {
		return fmt.Errorf("toolproxy: matrix rule %q host code %q is not in a frozen registry", rule.ID, rule.HostCode)
	}
	return nil
}

// Policy exposes the loaded matrix (tool-set membership checks live there).
func (s *Service) Policy() *Policy { return s.policy }

// Audit exposes the authoritative audit store.
func (s *Service) Audit() *AuditStore { return s.audit }

// Result is one exact Contract §7.18 ToolProxyResult: the ordered value tree,
// its canonical bytes and the frozen digests. CanonicalBytes is the only form
// that may enter the Pi return channel.
type Result struct {
	object         *contract.Object
	canonical      []byte
	proxyDigest    string
	upstreamDigest string
}

// Object returns the result value tree (Contract §7.18).
func (r *Result) Object() *contract.Object { return r.object }

// CanonicalBytes returns the RFC 8785 canonical serialization.
func (r *Result) CanonicalBytes() []byte { return r.canonical }

// ProxyResultDigest returns proxy_result_digest (x-digest rule: digest of
// the core minus the digest field; CanonicalBytes carries the full object).
func (r *Result) ProxyResultDigest() string { return r.proxyDigest }

// UpstreamResultDigest returns upstream_result_digest.
func (r *Result) UpstreamResultDigest() string { return r.upstreamDigest }

// Status returns the wire status (succeeded|failed|inconclusive).
func (r *Result) Status() string {
	if r == nil || r.object == nil {
		return ""
	}
	s, _ := contract.StringOf(r.object, "status")
	return s
}

// ReasonCode returns error.reason_code for failed/inconclusive results.
func (r *Result) ReasonCode() string {
	if r == nil || r.object == nil {
		return ""
	}
	return stringOfErrorField(r.object, "reason_code")
}

// Retryable returns the error.retryable flag derived from the owning frozen
// registry entry, never from message text.
func (r *Result) Retryable() bool {
	if r == nil || r.object == nil {
		return false
	}
	errVal, present := r.object.Get("error")
	if !present {
		return false
	}
	errObj, ok := errVal.(*contract.Object)
	if !ok {
		return false
	}
	flag, _ := errObj.Get("retryable")
	b, ok := flag.(contract.Bool)
	return ok && bool(b)
}

// ResultPayload returns the success result payload when status is succeeded.
func (r *Result) ResultPayload() contract.Value {
	if r == nil || r.object == nil {
		return nil
	}
	payload, present := r.object.Get("result")
	if !present {
		return nil
	}
	return payload
}

func stringOfErrorField(obj *contract.Object, key string) string {
	errVal, present := obj.Get("error")
	if !present {
		return ""
	}
	errObj, ok := errVal.(*contract.Object)
	if !ok {
		return ""
	}
	value, _ := contract.StringOf(errObj, key)
	return value
}

// ---------------------------------------------------------------------------
// Request parsing and validation (Host §5.3 / Contract §7.17)
// ---------------------------------------------------------------------------

var sha256Pattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// requestFields is the validated view of one ToolProxyRequest.
type requestFields struct {
	proxyRequestID string
	roomID         string
	agentID        string
	deliveryID     string
	toolName       string
	leaseKey       string
	requestDigest  string
	timeoutMillis  int64
	upstreamDigest string
}

// parseRequest validates the closed §7.17 shape. Tool-set membership is NOT
// checked here: an unbound tool name is a HOST_TOOL_NOT_ALLOWED result, not a
// malformed request. The second return is the Host-local failure code, empty
// when the request is well formed. The canonical request digest excludes
// proxy_request_id (the per-attempt identifier): idempotency binds on the
// semantic request content plus the idempotency key (Contract §13.2).
func parseRequest(request contract.Value) (requestFields, string) {
	fields := requestFields{}
	obj, ok := request.(*contract.Object)
	if !ok {
		return fields, CodeInvalidRequest
	}
	if sv, _ := contract.StringOf(obj, "schema_version"); sv != RequestSchemaVersion {
		return fields, CodeInvalidRequest
	}
	fields.proxyRequestID = nonEmptyString(obj, "proxy_request_id")
	fields.roomID = nonEmptyString(obj, "room_id")
	fields.agentID = nonEmptyString(obj, "agent_id")
	fields.deliveryID = nonEmptyString(obj, "delivery_id")
	fields.toolName = nonEmptyString(obj, "tool_name")
	if fields.proxyRequestID == "" || fields.roomID == "" || fields.agentID == "" || fields.deliveryID == "" || fields.toolName == "" {
		return fields, CodeInvalidRequest
	}
	if argsVal, present := obj.Get("arguments"); present {
		if _, isObj := argsVal.(*contract.Object); !isObj {
			return fields, CodeInvalidRequest
		}
	} else {
		return fields, CodeInvalidRequest
	}
	if scopeVal, present := obj.Get("scope_profile_ref"); present {
		if _, isObj := scopeVal.(*contract.Object); !isObj {
			return fields, CodeInvalidRequest
		}
	} else {
		return fields, CodeInvalidRequest
	}
	key, hasKey := contract.StringOf(obj, "idempotency_key")
	if !hasKey || !sha256Pattern.MatchString(key) {
		return fields, CodeInvalidRequest
	}
	timeout, hasTimeout := intField(obj, "timeout_millis")
	if !hasTimeout || timeout < 1 {
		return fields, CodeInvalidRequest
	}
	fields.timeoutMillis = timeout
	if minVal, present := obj.Get("requested_min_activation_sequence"); present {
		min, isInt := intOf(minVal)
		if !isInt || min.Sign() < 0 {
			return fields, CodeInvalidRequest
		}
	}

	// Lease key: (room_id, agent_id, delivery_id, idempotency_key).
	fields.leaseKey = fields.roomID + "\x1f" + fields.agentID + "\x1f" + fields.deliveryID + "\x1f" + key

	// Canonical request digest over the semantic content minus the
	// per-attempt proxy_request_id.
	preimage := contract.NewObject()
	for _, fieldKey := range obj.Keys() {
		if fieldKey == "proxy_request_id" {
			continue
		}
		val, _ := obj.Get(fieldKey)
		preimage.Set(fieldKey, val)
	}
	if digest, err := contract.DigestOf(preimage); err == nil {
		fields.requestDigest = digest
	}
	return fields, ""
}

func nonEmptyString(obj *contract.Object, key string) string {
	s, ok := contract.StringOf(obj, key)
	if !ok || s == "" {
		return ""
	}
	return s
}

func intField(obj *contract.Object, key string) (int64, bool) {
	val, present := obj.Get(key)
	if !present {
		return 0, false
	}
	n, ok := intOf(val)
	if !ok {
		return 0, false
	}
	return n.Int64(), true
}

func intOf(val contract.Value) (*big.Int, bool) {
	n, ok := val.(contract.Number)
	if !ok || !n.IsInteger() {
		return nil, false
	}
	bi, ok := n.Int()
	return bi, ok
}

// ---------------------------------------------------------------------------
// Invoke: the proxy execution path
// ---------------------------------------------------------------------------

// Invoke executes one ToolProxyRequest to its exact terminal result. The
// returned Result is always non-nil: every failure path (invalid request,
// unknown tool, timeout, cancel, upstream outage, validation failure, unknown
// upstream code, idempotency conflict) is an exact failed/inconclusive
// ToolProxyResult — never a placeholder success (Host §5.4/§5.8).
func (s *Service) Invoke(ctx context.Context, request contract.Value) *Result {
	fields, invalid := parseRequest(request)
	if invalid != "" {
		// The request never reached the upstream plane: the digest preimage
		// is the Host-private upstream-attempt record, state not_started.
		fields.upstreamDigest = attemptRecordDigest(fields, "not_started", invalid)
		result := s.localFailure(fields, invalid, "tool proxy request rejected: not a closed Contract 7.17 ToolProxyRequest")
		s.recordRequestAudit(fields, result)
		s.commitTerminal(fields, result)
		return result
	}
	if !s.policy.IsProxyTool(fields.toolName) {
		// Contract 12.7.1 M1: unbound tool names are closed failures; the
		// Host emits HOST_TOOL_NOT_ALLOWED, the frozen Host counterpart of
		// TOOL_UNSUPPORTED. No upstream call is attempted.
		fields.upstreamDigest = attemptRecordDigest(fields, "not_started", CodeToolNotAllowed)
		result := s.localFailure(fields, CodeToolNotAllowed, fmt.Sprintf("tool %q is not in the closed tool set v1", fields.toolName))
		s.recordRequestAudit(fields, result)
		s.commitTerminal(fields, result)
		return result
	}

	s.recordRequestAudit(fields, nil)

	// Idempotency lease (Host 5.7): bind on (room, agent, delivery,
	// idempotency_key). Same key + same canonical request digest returns the
	// saved exact terminal without a second upstream call or a second
	// Agent-visible observation; same key + different digest fails closed
	// with IDEMPOTENCY_CONFLICT.
	s.mu.Lock()
	if existing, ok := s.leases[fields.leaseKey]; ok {
		if existing.requestDigest != fields.requestDigest {
			s.mu.Unlock()
			result := s.localFailure(fields, CodeIdempotencyConflict, "idempotency key replayed with a different canonical request digest")
			s.audit.Append(AuditEntry{
				Kind:           AuditTerminal,
				ProxyRequestID: fields.proxyRequestID,
				LeaseKey:       fields.leaseKey,
				ToolName:       fields.toolName,
				RequestDigest:  fields.requestDigest,
				ResultDigest:   result.ProxyResultDigest(),
				ReasonCode:     CodeIdempotencyConflict,
				CASOutcome:     CASConflictObserved,
				RecordedAt:     timeNowUTC(),
			})
			return result
		}
		s.mu.Unlock()
		// Same request: read the leader's terminal — one upstream call, one
		// Agent observation, exact bytes (Contract 13.2 idempotent replay).
		<-existing.done
		if existing.terminal == nil {
			fields.upstreamDigest = attemptRecordDigest(fields, "no_response", CodeGMSUnavailable)
			return s.localFailure(fields, CodeGMSUnavailable, "idempotent leader produced no terminal result")
		}
		s.audit.Append(AuditEntry{
			Kind:           AuditTerminal,
			ProxyRequestID: proxyRequestIDOfResult(existing.terminal),
			LeaseKey:       fields.leaseKey,
			ToolName:       fields.toolName,
			RequestDigest:  fields.requestDigest,
			ResultDigest:   existing.terminal.ProxyResultDigest(),
			Status:         existing.terminal.Status(),
			ReasonCode:     existing.terminal.ReasonCode(),
			CASOutcome:     CASReplayedSaved,
			RecordedAt:     timeNowUTC(),
		})
		return existing.terminal
	}
	leader := &lease{requestDigest: fields.requestDigest, done: make(chan struct{})}
	s.leases[fields.leaseKey] = leader
	s.mu.Unlock()

	result := s.execute(ctx, fields, request)
	committed := s.commitTerminal(fields, result)

	s.mu.Lock()
	leader.terminal = committed
	close(leader.done)
	s.mu.Unlock()
	return committed
}

// execute performs the upstream call and applies the frozen reason
// precedence (Contract 13.7.1 R3) and success-validation matrix (12.7.1).
func (s *Service) execute(ctx context.Context, fields requestFields, request contract.Value) *Result {
	tctx, cancel := context.WithTimeout(ctx, time.Duration(fields.timeoutMillis)*time.Millisecond)
	defer cancel()

	// Fail closed before any upstream dispatch: a caller that already
	// cancelled (or whose deadline passed) observes the exact cancellation
	// or timeout failure even if the transport ignores the context.
	if err := ctx.Err(); err != nil {
		code, state := classifyTransport(ctx, tctx, err)
		fields.upstreamDigest = attemptRecordDigest(fields, state, code)
		return s.localFailure(fields, code, fmt.Sprintf("proxy context ended before upstream dispatch: %s", truncate(err.Error(), 200)))
	}

	response, err := s.upstream.Call(tctx, request)
	if err != nil || response == nil {
		code, state := classifyTransport(ctx, tctx, err)
		fields.upstreamDigest = attemptRecordDigest(fields, state, code)
		detail := "no upstream response"
		if err != nil {
			detail = err.Error()
		}
		return s.localFailure(fields, code, fmt.Sprintf("upstream attempt ended without a response: %s", truncate(detail, 300)))
	}

	switch response.Status {
	case UpstreamSucceeded:
		payloadDigest, digestErr := contract.DigestOf(response.Result)
		if digestErr != nil {
			// The upstream payload cannot enter the hashed core at all
			// (e.g. non-integer numbers): exact failure with the observed
			// envelope digest, never a rewrite of the payload.
			fields.upstreamDigest = observedEnvelopeDigest(response)
			return s.localFailure(fields, CodeUpstreamSchema, "upstream success payload is not canonicalizable (non-integer number)")
		}
		if systemReason := s.evaluateMatrix(fields, request, response); systemReason != "" {
			hostCode := s.policy.HostCodeFor[systemReason]
			if hostCode == "" {
				hostCode = CodeUpstreamSchema
			}
			fields.upstreamDigest = payloadDigest
			return s.localFailure(fields, hostCode, fmt.Sprintf("upstream success payload rejected by the frozen validation matrix: %s", systemReason))
		}
		return successResult(fields, response.Result, payloadDigest)
	case UpstreamFailed, UpstreamInconclusive:
		fields.upstreamDigest = observedEnvelopeDigest(response)
		entry, known := s.reasons.System.Lookup(response.ReasonCode)
		if known && (entry.Status == "failure" || entry.Status == "inconclusive") {
			// R3 precedence 2: known system code passes through verbatim;
			// status/retryable/retry_scope come from the registry entry and
			// are never overridden by the receiver.
			return s.localFailure(fields, response.ReasonCode, response.Message)
		}
		// R3 precedence 3: unknown upstream code — replaced, never
		// transported; the original code survives as non-behavioral
		// diagnostic metadata in the human-readable message only.
		return s.localFailure(fields, CodeUpstreamSchema, fmt.Sprintf("unknown upstream code %q replaced by %s (upstream message: %s)", response.ReasonCode, CodeUpstreamSchema, truncate(response.Message, 200)))
	default:
		fields.upstreamDigest = observedEnvelopeDigest(response)
		return s.localFailure(fields, CodeUpstreamSchema, fmt.Sprintf("unknown upstream status %q", string(response.Status)))
	}
}

// RecordUpstreamLate records an upstream response that arrived after the
// proxy call already reached its terminal result: audit-only. It never
// produces, replaces or rewrites any code or terminal state (§13.7.1 R3-4).
func (s *Service) RecordUpstreamLate(request contract.Value, response *UpstreamResponse) AuditEntry {
	fields, _ := parseRequest(request)
	upstreamDigest := observedEnvelopeDigest(response)
	if response != nil && response.Status == UpstreamSucceeded {
		if digest, err := contract.DigestOf(response.Result); err == nil {
			upstreamDigest = digest
		}
	}
	entry := AuditEntry{
		Kind:           AuditLateUpstream,
		ProxyRequestID: fields.proxyRequestID,
		LeaseKey:       fields.leaseKey,
		ToolName:       fields.toolName,
		RequestDigest:  fields.requestDigest,
		UpstreamDigest: upstreamDigest,
		CASOutcome:     CASLateOnly,
		Detail:         map[string]string{"mode": "audit_only"},
		RecordedAt:     timeNowUTC(),
	}
	s.audit.Append(entry)
	return entry
}

func (s *Service) recordRequestAudit(fields requestFields, result *Result) {
	entry := AuditEntry{
		Kind:           AuditProxyRequest,
		ProxyRequestID: fields.proxyRequestID,
		LeaseKey:       fields.leaseKey,
		ToolName:       fields.toolName,
		RequestDigest:  fields.requestDigest,
		RecordedAt:     timeNowUTC(),
	}
	if result != nil {
		entry.ResultDigest = result.ProxyResultDigest()
		entry.ReasonCode = result.ReasonCode()
	}
	s.audit.Append(entry)
}

// commitTerminal fixes the terminal result for the proxy_request_id: the
// first terminal wins; a later commit for the same id returns the already
// fixed result unchanged (Contract 13.7.1 R3-4 late-rewrite prohibition).
func (s *Service) commitTerminal(fields requestFields, result *Result) *Result {
	s.mu.Lock()
	if existing, seen := s.terminals[fields.proxyRequestID]; seen {
		s.mu.Unlock()
		s.audit.Append(AuditEntry{
			Kind:           AuditTerminal,
			ProxyRequestID: fields.proxyRequestID,
			LeaseKey:       fields.leaseKey,
			ToolName:       fields.toolName,
			ResultDigest:   existing.ProxyResultDigest(),
			Status:         existing.Status(),
			ReasonCode:     existing.ReasonCode(),
			CASOutcome:     "cas_lost_returned_fixed_terminal",
			Detail:         map[string]string{"attempted_digest": result.ProxyResultDigest()},
			RecordedAt:     timeNowUTC(),
		})
		return existing
	}
	s.terminals[fields.proxyRequestID] = result
	s.mu.Unlock()

	// Mirror the CAS into the authoritative audit store (same first-wins
	// semantics; the audit Terminal record carries the canonical bytes).
	fixed, _ := s.audit.CommitTerminal(Terminal{
		ProxyRequestID: fields.proxyRequestID,
		ResultDigest:   result.ProxyResultDigest(),
		Status:         result.Status(),
		ReasonCode:     result.ReasonCode(),
		Canonical:      result.CanonicalBytes(),
	})
	s.audit.Append(AuditEntry{
		Kind:           AuditTerminal,
		ProxyRequestID: fields.proxyRequestID,
		LeaseKey:       fields.leaseKey,
		ToolName:       fields.toolName,
		RequestDigest:  fields.requestDigest,
		ResultDigest:   result.ProxyResultDigest(),
		Status:         result.Status(),
		ReasonCode:     result.ReasonCode(),
		CASOutcome:     CASFirstTerminal,
		Detail:         map[string]string{"audit_terminal_digest": fixed.ResultDigest},
		RecordedAt:     timeNowUTC(),
	})
	return result
}

func proxyRequestIDOfResult(r *Result) string {
	if r == nil || r.object == nil {
		return ""
	}
	id, _ := contract.StringOf(r.object, "proxy_request_id")
	return id
}

// classifyTransport maps a failed upstream attempt onto the exact Host-local
// code and upstream-attempt record state (Host 5.5.4/5.8).
func classifyTransport(parent, tctx context.Context, err error) (code, state string) {
	if parent.Err() == context.Canceled || (err != nil && errors.Is(err, context.Canceled)) {
		return CodeCancelled, "cancelled"
	}
	if tctx.Err() == context.DeadlineExceeded || (err != nil && errors.Is(err, context.DeadlineExceeded)) {
		return CodeTimeout, "no_response"
	}
	return CodeGMSUnavailable, "no_response"
}

// attemptRecordDigest digests the Host-module-private immutable
// upstream-attempt record for a request that produced no upstream payload
// (Host 5.5.4): the record explicitly distinguishes
// not_started|no_response|cancelled and never forges a GMS payload.
func attemptRecordDigest(fields requestFields, state, code string) string {
	record := contract.NewObject()
	record.Set("schema_version", contract.String(upstreamAttemptSchema))
	record.Set("proxy_request_id", contract.String(fields.proxyRequestID))
	record.Set("state", contract.String(state))
	record.Set("host_reason_code", contract.String(code))
	digest, err := contract.DigestOf(record)
	if err != nil {
		return ""
	}
	return digest
}

// observedEnvelopeDigest digests the module-private canonical form of an
// observed upstream failure envelope (the exact observation class the Host
// saw when no success payload was returned).
func observedEnvelopeDigest(response *UpstreamResponse) string {
	envelope := contract.NewObject()
	envelope.Set("schema_version", contract.String(observedUpstreamFailureSchema))
	if response != nil {
		envelope.Set("status", contract.String(string(response.Status)))
		envelope.Set("reason_code", contract.String(response.ReasonCode))
		envelope.Set("message", contract.String(truncate(response.Message, 500)))
	}
	digest, err := contract.DigestOf(envelope)
	if err != nil {
		return ""
	}
	return digest
}

// localFailure builds an exact failed/inconclusive result from a Host-local
// or shared code whose semantics live in the frozen registries.
func (s *Service) localFailure(fields requestFields, code, message string) *Result {
	entry, known := s.reasons.Lookup(code)
	status := "failed"
	if known && entry.Status == "inconclusive" {
		status = "inconclusive"
	}
	retryable := known && entry.Retryable
	errObj := contract.NewObject()
	errObj.Set("reason_code", contract.String(code))
	errObj.Set("message", contract.String(message))
	errObj.Set("retryable", contract.Bool(retryable))
	body := contract.NewObject()
	body.Set("schema_version", contract.String(ResultSchemaVersion))
	body.Set("proxy_request_id", contract.String(fields.proxyRequestID))
	body.Set("status", contract.String(status))
	body.Set("error", errObj)
	body.Set("upstream_result_digest", contract.String(fields.upstreamDigest))
	return finishResult(body, fields.upstreamDigest)
}

func successResult(fields requestFields, payload contract.Value, payloadDigest string) *Result {
	body := contract.NewObject()
	body.Set("schema_version", contract.String(ResultSchemaVersion))
	body.Set("proxy_request_id", contract.String(fields.proxyRequestID))
	body.Set("status", contract.String("succeeded"))
	body.Set("result", payload)
	body.Set("upstream_result_digest", contract.String(payloadDigest))
	return finishResult(body, payloadDigest)
}

// finishResult computes proxy_result_digest over the core minus the digest
// field (the x-digest rule frozen by schema/shared/tool-proxy-result.v1) and
// canonicalizes the full result.
func finishResult(body *contract.Object, upstreamDigest string) *Result {
	digest, err := contract.DigestOf(body)
	if err != nil {
		body = minimalInvalidResult(body)
		digest, _ = contract.DigestOf(body)
	}
	body.Set("proxy_result_digest", contract.String(digest))
	canonical, err := contract.JCS(body)
	if err != nil {
		canonical = nil
	}
	return &Result{object: body, canonical: canonical, proxyDigest: digest, upstreamDigest: upstreamDigest}
}

func minimalInvalidResult(body *contract.Object) *contract.Object {
	fallback := contract.NewObject()
	fallback.Set("schema_version", contract.String(ResultSchemaVersion))
	if id, ok := contract.StringOf(body, "proxy_request_id"); ok {
		fallback.Set("proxy_request_id", contract.String(id))
	}
	fallback.Set("status", contract.String("failed"))
	errObj := contract.NewObject()
	errObj.Set("reason_code", contract.String(CodeInvalidRequest))
	errObj.Set("message", contract.String("tool proxy result was not canonicalizable"))
	errObj.Set("retryable", contract.Bool(false))
	fallback.Set("error", errObj)
	return fallback
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func timeNowUTC() time.Time { return time.Now().UTC() }

// ---------------------------------------------------------------------------
// CTR-002 success-validation matrix (Contract §12.7.1 M1-M5)
// ---------------------------------------------------------------------------
//
// The evaluation order mirrors the frozen derivation order: common rules in
// policy order, then the bound ruleset's rules in policy order; the first
// closed failure wins, otherwise the payload is accepted. The Go port is
// semantic-parity with $FIX/validate_tool_binding.py (fixtures corpus green
// in service_test.go).

// evaluateMatrix derives the system canonical reason code for a rejected
// upstream success payload ("" when accepted).
func (s *Service) evaluateMatrix(fields requestFields, request contract.Value, response *UpstreamResponse) string {
	binding, bound := s.policy.Tools[fields.toolName]
	if !bound {
		return "TOOL_UNSUPPORTED" // caller already guards; defensive total
	}
	payload, isObject := response.Result.(*contract.Object)
	reqObj, _ := request.(*contract.Object)

	// Common rule: unknown required extension (request, then payload).
	if hasUnknownRequiredExtension(reqObj) || hasUnknownRequiredExtension(payload) {
		return "UNKNOWN_REQUIRED_EXTENSION"
	}
	// Common rule: result body not a closed DTO object.
	schemaVersion := ""
	if isObject {
		schemaVersion, _ = contract.StringOf(payload, "schema_version")
	}
	if _, known := s.policy.knownResultSchemas[schemaVersion]; !isObject || !known {
		return "ARTIFACT_BODY_INVALID"
	}
	// Common rule: tool/result type mismatch.
	if schemaVersion != binding.ResultSchemaVersion {
		return "TOOL_RESULT_BINDING_INVALID"
	}
	// Common rule: closed field-set violation (wrappers, smuggled fields).
	closed := map[string]struct{}{}
	for _, field := range binding.ClosedFields {
		closed[field] = struct{}{}
	}
	for _, key := range payload.Keys() {
		if key == "extensions" {
			continue // extensions permitted on any closed DTO (Contract 6.4)
		}
		if _, allowed := closed[key]; !allowed {
			return "SCHEMA_FIELD_UNKNOWN"
		}
	}
	// Common rule: required top-level field missing.
	for _, field := range binding.RequiredFields {
		if _, present := payload.Get(field); !present {
			return "SCHEMA_REQUIRED_FIELD_MISSING"
		}
	}
	// Common rule: scope violation — authorization is request + authoritative
	// read audit record (room/agent/scope profile must agree).
	if auditObj, ok := response.ReadAudit.(*contract.Object); ok && reqObj != nil {
		for _, field := range []string{"room_id", "agent_id", "scope_profile_ref"} {
			auditVal, inAudit := auditObj.Get(field)
			reqVal, inReq := reqObj.Get(field)
			if inAudit && inReq && !jsonValueEqual(auditVal, reqVal) {
				return "EXPLORE_SCOPE_VIOLATION"
			}
		}
	}

	switch binding.Ruleset {
	case "explore-family":
		return s.evaluateExploreFamily(payload, reqObj, binding, response)
	case "guidance-view-closed":
		return s.evaluateGuidanceViewClosed(payload, reqObj, binding, response)
	default:
		return "TOOL_RESULT_BINDING_INVALID"
	}
}

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
	// v1 registers no extensions: required:true on any key fails closed
	// (Contract 6.4, same KNOWN_EXTENSIONS_V1 = {} as the frozen validator).
	for _, key := range ext.Keys() {
		entryVal, _ := ext.Get(key)
		entry, ok := entryVal.(*contract.Object)
		if !ok {
			continue
		}
		reqVal, hasReq := entry.Get("required")
		req, isBool := reqVal.(contract.Bool)
		if hasReq && isBool && bool(req) {
			_ = key
			return true
		}
	}
	return false
}

func jsonValueEqual(a, b contract.Value) bool {
	ab, errA := contract.JCS(a)
	bb, errB := contract.JCS(b)
	return errA == nil && errB == nil && string(ab) == string(bb)
}

// evaluateExploreFamily implements the explore-family ruleset (M2/M3):
// watermark + budgets + served fences sub-objects, budget consistency,
// typed result enums, typed citations, the silent-truncation guard,
// profile version floors and freshness.
func (s *Service) evaluateExploreFamily(payload *contract.Object, reqObj *contract.Object, binding ToolBinding, response *UpstreamResponse) string {
	ruleset := s.policy.Rulesets[binding.Ruleset]

	watermark := objectFieldOrEmpty(payload, "watermark")
	for _, field := range ruleset.RequiredWatermarkFields {
		if _, present := watermark.Get(field); !present {
			return "SCHEMA_REQUIRED_FIELD_MISSING"
		}
	}
	budgets := objectFieldOrEmpty(payload, "budgets")
	for _, field := range ruleset.RequiredBudgetFields {
		if _, present := budgets.Get(field); !present {
			return "SCHEMA_REQUIRED_FIELD_MISSING"
		}
	}
	fences := objectFieldOrEmpty(payload, "served_fences")
	for _, field := range ruleset.RequiredFenceFields {
		if _, present := fences.Get(field); !present {
			return "SCHEMA_REQUIRED_FIELD_MISSING"
		}
	}

	// Budgets consistency (Contract 12.4).
	budgetValues := map[string]*big.Int{}
	for _, field := range ruleset.RequiredBudgetFields {
		val, present := budgets.Get(field)
		if !present {
			continue
		}
		n, ok := intOf(val)
		if !ok || n.Sign() < 0 {
			return "BUDGET_INVALID"
		}
		budgetValues[field] = n
	}
	evidenceResults, evidenceOK := arrayField(payload, "evidence_results")
	skillResults, skillOK := arrayField(payload, "skill_results")
	if !evidenceOK || !skillOK {
		return "SCHEMA_REQUIRED_FIELD_MISSING"
	}
	exceeds := func(used, cap string) bool {
		u, uOK := budgetValues[used]
		c, cOK := budgetValues[cap]
		return uOK && cOK && u.Cmp(c) > 0
	}
	if exceeds("total_used", "total_cap") ||
		big.NewInt(int64(len(evidenceResults))).Cmp(budgetValues["evidence_subcap"]) > 0 ||
		big.NewInt(int64(len(skillResults))).Cmp(budgetValues["skill_subcap"]) > 0 {
		return "BUDGET_INVALID"
	}
	tokenSum := big.NewInt(0)
	for _, item := range skillResults {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		viewVal, present := entry.Get("guidance_view")
		view, ok := viewVal.(*contract.Object)
		if !present || !ok {
			continue
		}
		if tokens, ok := intField(view, "content_token_count"); ok && tokens >= 0 {
			tokenSum.Add(tokenSum, big.NewInt(tokens))
		}
	}
	if tokenSum.Cmp(budgetValues["guidance_token_budget"]) > 0 {
		return "BUDGET_INVALID"
	}

	// Typed result enums (Contract 7.16).
	for _, item := range evidenceResults {
		if entry, ok := item.(*contract.Object); ok {
			if rt, _ := contract.StringOf(entry, "result_type"); rt != "evidence" {
				return "SCHEMA_ENUM_INVALID"
			}
		}
	}
	for _, item := range skillResults {
		if entry, ok := item.(*contract.Object); ok {
			if rt, _ := contract.StringOf(entry, "result_type"); rt != "skill" {
				return "SCHEMA_ENUM_INVALID"
			}
		}
	}

	// Typed citations (Contract 12.6).
	for _, item := range evidenceResults {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		citationVal, present := entry.Get("citation")
		citation, ok := citationVal.(*contract.Object)
		if !present || !ok {
			return "CITATION_INVALID"
		}
		refVal, hasRef := citation.Get("evidence_ref")
		_, refIsObject := refVal.(*contract.Object)
		claim, hasClaim := contract.StringOf(citation, "claim")
		if !hasRef || !refIsObject || !hasClaim || claim == "" {
			return "CITATION_INVALID"
		}
	}
	for _, item := range skillResults {
		entry, ok := item.(*contract.Object)
		if !ok {
			continue
		}
		identityVal, present := entry.Get("artifact_identity_citation")
		identity, ok := identityVal.(*contract.Object)
		if !present || !ok {
			return "CITATION_INVALID"
		}
		skillRefVal, hasRef := identity.Get("skill_ref")
		if _, refIsObject := skillRefVal.(*contract.Object); !hasRef || !refIsObject {
			return "CITATION_INVALID"
		}
		citationsVal, hasCitations := entry.Get("evidence_citations")
		citations, citationsIsArray := citationsVal.(contract.Array)
		if !hasCitations || !citationsIsArray {
			return "CITATION_INVALID"
		}
		for _, citationItem := range citations {
			citation, ok := citationItem.(*contract.Object)
			if !ok {
				return "CITATION_INVALID"
			}
			refVal, hasRef := citation.Get("evidence_ref")
			_, refIsObject := refVal.(*contract.Object)
			claim, hasClaim := contract.StringOf(citation, "claim")
			if !hasRef || !refIsObject || !hasClaim || claim == "" {
				return "CITATION_INVALID"
			}
		}
	}

	// Silent-truncation guard (Contract 12.7.1 M2 / 12.7.2 C4): non-empty
	// truncation_reason_codes require a non-empty frozen omissions carrier.
	if codes, ok := arrayField(payload, "truncation_reason_codes"); ok && len(codes) > 0 {
		omissions, hasCarrier := arrayField(payload, "omissions")
		if !hasCarrier || len(omissions) == 0 {
			return "TOOL_RESULT_BINDING_INVALID"
		}
	}

	if !profileFloorsSatisfied(payload, binding.ProfileConstraints) {
		return "SCHEMA_VERSION_UNSUPPORTED"
	}

	// Freshness is carried by request fields vs the result watermark.
	if reqObj != nil {
		if requested, ok := intField(reqObj, "requested_min_activation_sequence"); ok && requested >= 0 {
			projected, present := watermark.Get("projected_through_activation_sequence")
			projectedInt, isInt := intOf(projected)
			if !present || !isInt || projectedInt.Cmp(big.NewInt(requested)) < 0 {
				return "PROJECTION_BEHIND_REQUIRED_SEQUENCE"
			}
		}
	}
	return ""
}

// evaluateGuidanceViewClosed implements the skill_get ruleset (M4): only the
// applicable closed GuidanceView rules — profile floors, view_hash digest
// consistency and freshness against the authoritative read audit record.
func (s *Service) evaluateGuidanceViewClosed(payload *contract.Object, reqObj *contract.Object, binding ToolBinding, response *UpstreamResponse) string {
	if !profileFloorsSatisfied(payload, binding.ProfileConstraints) {
		return "SCHEMA_VERSION_UNSUPPORTED"
	}
	// Digest consistency inside the closed schema: view_hash must be the JCS
	// SHA-256 of the view document minus view_hash.
	viewHash, _ := contract.StringOf(payload, "view_hash")
	preimage := contract.NewObject()
	for _, key := range payload.Keys() {
		if key == "view_hash" {
			continue
		}
		val, _ := payload.Get(key)
		preimage.Set(key, val)
	}
	want, err := contract.DigestOf(preimage)
	if err != nil || want != viewHash {
		return "GUIDANCE_VIEW_HASH_MISMATCH"
	}
	// Freshness is carried by request fields vs the authoritative read audit
	// record (skill_get checks the authoritative head directly).
	if reqObj != nil {
		if requested, ok := intField(reqObj, "requested_min_activation_sequence"); ok && requested >= 0 {
			activeHead := int64(-1)
			if auditObj, isObj := response.ReadAudit.(*contract.Object); isObj {
				if head, ok := intField(auditObj, "active_head_activation_sequence"); ok {
					activeHead = head
				}
			}
			if activeHead < requested {
				return "PROJECTION_BEHIND_REQUIRED_SEQUENCE"
			}
		}
	}
	return ""
}

// profileFloorsSatisfied enforces the min_<field>_version floors frozen by
// the policy: a versioned profile ref below the floor is stale and must not
// enable (old profiles never enable).
func profileFloorsSatisfied(payload *contract.Object, constraints map[string]int64) bool {
	for key, floor := range constraints {
		if len(key) <= len("min_")+len("_version") {
			return false
		}
		field := key[len("min_") : len(key)-len("_version")]
		refVal, present := payload.Get(field)
		if !present {
			continue
		}
		ref, ok := refVal.(*contract.Object)
		if !ok {
			continue
		}
		if version, ok := intField(ref, "version"); ok && version < floor {
			return false
		}
	}
	return true
}

func objectFieldOrEmpty(obj *contract.Object, key string) *contract.Object {
	val, present := obj.Get(key)
	if !present {
		return contract.NewObject()
	}
	if nested, ok := val.(*contract.Object); ok {
		return nested
	}
	return contract.NewObject()
}

func arrayField(obj *contract.Object, key string) (contract.Array, bool) {
	val, present := obj.Get(key)
	if !present {
		return nil, false
	}
	arr, ok := val.(contract.Array)
	return arr, ok
}
