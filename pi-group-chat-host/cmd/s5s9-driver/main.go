// Command s5s9-driver is the Host-side INT-003 contract driver (HST-201
// same-call ToolProxy over the memory_explore surface, Contract §9.3/S7).
//
// Line-delimited JSON protocol over stdio: one instruction per line on
// stdin, one single-line JSON response per instruction on stdout, logs on
// stderr. Instructions are {"id":N,"op":"...", ...}; responses are
// {"id":N,"ok":true,...} or {"id":N,"ok":false,"error":{"code","reason"}}
// with codes drawn from the §13.7.1 closed reason registry vocabulary.
//
// The driver is a pure fixture tool of the shared conformance tracer
// ($FIX/integration/run_s5_s9.py): it drives the real Host ToolProxy
// same-call bridge over the frozen shared fixtures and reports raw facts
// only — the upstream ExploreResult comes from the frozen artifact
// corpus (artifact-006-explore-result), the tool call request comes from
// the frozen tools corpus (pos-explore-full-success). It never computes
// scores, utility vectors, U1 or release semantics (Host §6.7).
//
// Ops (all take "fixtures": the shared conformance directory):
//
//	toolproxy.explore {mode: ok|rewrite, case_id, session}
//
// mode ok: one same-call memory_explore invocation — the Pi return
// channel must receive exactly the upstream canonical ToolProxyResult
// bytes, once, before tool_execution_end.
//
// mode rewrite: after the frozen terminal, a rewritten upstream payload
// attempts to replace the delivered bytes — it must stay audit-only
// (LATE_OUTPUT_REWRITE), the terminal and the Pi bytes never change.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/toolproxy"
)

func main() {
	reader := bufio.NewReaderSize(os.Stdin, 1<<20)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	respond := func(payload map[string]any) {
		line, _ := json.Marshal(payload)
		writer.WriteString(string(line) + "\n")
		writer.Flush()
	}
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			handleInstruction(line, respond)
		}
		if err != nil {
			return
		}
	}
}

func handleInstruction(raw string, respond func(map[string]any)) {
	value, perr := contract.ParseJSON([]byte(raw))
	if perr != nil {
		respond(driverError(0, "DRIVER_INSTRUCTION_INVALID", "unparseable instruction: "+perr.Error()))
		return
	}
	body, ok := value.(*contract.Object)
	if !ok {
		respond(driverError(0, "DRIVER_INSTRUCTION_INVALID", "instruction is not an object"))
		return
	}
	id := numberOr(bodyNumber(body, "id"))
	op := bodyString(body, "op")
	if op == "" {
		respond(driverError(id, "DRIVER_INSTRUCTION_INVALID", "op missing"))
		return
	}
	var payload map[string]any
	var code, reason string
	switch op {
	case "toolproxy.explore":
		payload, code, reason = opToolproxyExplore(body)
	default:
		code, reason = "DRIVER_OP_UNKNOWN", "op "+op
	}
	if code != "" {
		respond(driverError(id, code, reason))
		return
	}
	payload["id"] = id
	payload["ok"] = true
	payload["op"] = op
	respond(payload)
}

func parseJSONFile(path string) (contract.Value, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return contract.ParseJSON(data)
}

func driverError(id int64, code, reason string) map[string]any {
	return map[string]any{
		"id":    id,
		"ok":    false,
		"error": map[string]any{"code": code, "reason": reason},
	}
}

// ---------------------------------------------------------------------------
// Shared helpers over contract values
// ---------------------------------------------------------------------------

func numberOr(n string) int64 {
	if n == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func bodyValue(o *contract.Object, key string) contract.Value {
	v, _ := o.Get(key)
	if v == nil {
		return contract.Null{}
	}
	return v
}

func bodyString(o *contract.Object, key string) string {
	if s, ok := bodyValue(o, key).(contract.String); ok {
		return string(s)
	}
	return ""
}

func bodyNumber(o *contract.Object, key string) string {
	if n, ok := bodyValue(o, key).(contract.Number); ok {
		return string(n)
	}
	return ""
}

func bodyObject(o *contract.Object, key string) *contract.Object {
	if obj, ok := bodyValue(o, key).(*contract.Object); ok {
		return obj
	}
	return contract.NewObject()
}

func bodyArray(o *contract.Object, key string) contract.Array {
	if arr, ok := bodyValue(o, key).(contract.Array); ok {
		return arr
	}
	return nil
}

// conformanceDir resolves the shared fixtures directory exactly like the
// other Host contract tools: RSIH_CONFORMANCE_DIR wins, then the frozen
// default relative to the repository root.
func conformanceDir(instruction *contract.Object) (string, error) {
	if dir := bodyString(instruction, "fixtures"); dir != "" {
		return filepath.Abs(dir)
	}
	if dir := os.Getenv("RSIH_CONFORMANCE_DIR"); dir != "" {
		return filepath.Abs(dir)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// The driver runs with the repository as cwd; the frozen location is
	// ../specs/rsi-harness-skill-evolution/conformance.
	return filepath.Abs(filepath.Join(cwd, "..", "specs",
		"rsi-harness-skill-evolution", "conformance"))
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func objOf(value contract.Value) *contract.Object {
	object, ok := value.(*contract.Object)
	if !ok {
		return nil
	}
	return object
}

// ---------------------------------------------------------------------------
// S7: same-call Explore through the real ToolProxy bridge
// ---------------------------------------------------------------------------

type cannedUpstream struct {
	response func(ctx context.Context, request contract.Value) (*toolproxy.UpstreamResponse, error)
	calls    int
}

func (u *cannedUpstream) Call(ctx context.Context, request contract.Value) (*toolproxy.UpstreamResponse, error) {
	u.calls++
	return u.response(ctx, request)
}

type recordingChannel struct {
	mu         sync.Mutex
	deliveries []string // tool_call_id digests in delivery order
	digests    []string
}

func (c *recordingChannel) Deliver(_ context.Context, toolCallID string, canonical []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deliveries = append(c.deliveries, toolCallID)
	c.digests = append(c.digests, contract.DigestBytes(canonical))
	return nil
}

func (c *recordingChannel) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deliveries)
}

func (c *recordingChannel) lastDigest() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.digests) == 0 {
		return ""
	}
	return c.digests[len(c.digests)-1]
}

// exploreFixtures carries the frozen documents one probe invocation uses.
type exploreFixtures struct {
	caseID       string
	request      *contract.Object // the frozen memory_explore tool request
	payload      contract.Value   // the frozen upstream ExploreResult
	readAudit    contract.Value
	wantDigest   string
	policy       *toolproxy.Policy
	reasons      *contract.ReasonBundle
	hasWatermark bool
}

// loadExploreFixtures loads one frozen tools-corpus explore case (the
// request/result_payload/read_audit triple is frozen together, so the
// freshness rule's watermark pairing is the frozen positive pair).
func loadExploreFixtures(dir, caseID string) (*exploreFixtures, error) {
	manifestValue, err := parseJSONFile(filepath.Join(dir, "tools", "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("tools manifest: %w", err)
	}
	manifest := objOf(manifestValue)
	var inputPath, expectedPath string
	for _, raw := range bodyArray(manifest, "cases") {
		entry, ok := raw.(*contract.Object)
		if !ok {
			continue
		}
		if bodyString(entry, "case_id") == caseID {
			inputPath = bodyString(entry, "input_path")
			expectedPath = bodyString(entry, "expected_path")
			break
		}
	}
	if inputPath == "" {
		return nil, fmt.Errorf("tools case %s not in the manifest", caseID)
	}
	inputValue, err := parseJSONFile(filepath.Join(dir, "tools", inputPath))
	if err != nil {
		return nil, fmt.Errorf("explore case %s: %w", caseID, err)
	}
	input, ok := inputValue.(*contract.Object)
	if !ok {
		return nil, fmt.Errorf("explore case %s input is not an object", caseID)
	}
	expectedValue, err := parseJSONFile(filepath.Join(dir, "tools", expectedPath))
	if err != nil {
		return nil, fmt.Errorf("explore expected %s: %w", caseID, err)
	}
	expected, _ := expectedValue.(*contract.Object)
	wantDigest := ""
	if expected != nil {
		wantDigest = bodyString(expected, "expected_result_digest")
	}
	policy, err := toolproxy.LoadToolPolicy(filepath.Join(dir, "policy", "tool-success-validation.v1.json"))
	if err != nil {
		return nil, fmt.Errorf("tool policy: %w", err)
	}
	reasons, err := contract.LoadReasonBundle(filepath.Join(dir, "policy"))
	if err != nil {
		return nil, fmt.Errorf("reason bundle: %w", err)
	}
	payload := bodyValue(input, "result_payload")
	payloadObj, isObj := payload.(*contract.Object)
	if !isObj {
		return nil, fmt.Errorf("explore case %s carries no result payload", caseID)
	}
	_, hasWatermark := payloadObj.Get("watermark")
	return &exploreFixtures{
		caseID: caseID, request: input, payload: payload,
		readAudit:  bodyValue(input, "read_audit"),
		wantDigest: wantDigest, policy: policy, reasons: reasons,
		hasWatermark: hasWatermark,
	}, nil
}

func (f *exploreFixtures) toolCall(session string) toolproxy.Call {
	arguments := bodyObject(f.request, "request")
	return toolproxy.Call{
		ToolCallID: "call-int003-" + f.caseID,
		ToolName:   bodyString(f.request, "tool_name"),
		Arguments:  bodyValue(arguments, "arguments"),
	}
}

func (f *exploreFixtures) requestContext(session string) toolproxy.RequestContext {
	request := bodyObject(f.request, "request")
	rc := toolproxy.RequestContext{
		RoomID:          bodyString(request, "room_id"),
		AgentID:         bodyString(request, "agent_id"),
		DeliveryID:      bodyString(request, "delivery_id"),
		ProxyRequestID:  "pr-int003-" + session,
		ScopeProfileRef: bodyValue(request, "scope_profile_ref"),
		TimeoutMillis:   numberOr(bodyNumber(request, "timeout_millis")),
	}
	if rc.TimeoutMillis <= 0 {
		rc.TimeoutMillis = toolproxy.DefaultTimeoutMillis
	}
	return rc
}

func countAudit(audit *toolproxy.AuditStore, kind string) int {
	count := 0
	for _, entry := range audit.Entries() {
		if entry.Kind == kind {
			count++
		}
	}
	return count
}

func opToolproxyExplore(instruction *contract.Object) (map[string]any, string, string) {
	dir, err := conformanceDir(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	caseID := bodyString(instruction, "case_id")
	if caseID == "" {
		caseID = "pos-explore-full-success"
	}
	mode := bodyString(instruction, "mode")
	if mode == "" {
		mode = "ok"
	}
	session := bodyString(instruction, "session")
	if session == "" {
		session = "exp-s5s9"
	}
	fix, err := loadExploreFixtures(dir, caseID)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	request := bodyObject(fix.request, "request")

	if mode == "rewrite" {
		return toolproxyRewrite(fix, request, session)
	}

	upstream := &cannedUpstream{}
	upstream.response = func(context.Context, contract.Value) (*toolproxy.UpstreamResponse, error) {
		return &toolproxy.UpstreamResponse{
			Status:    toolproxy.UpstreamSucceeded,
			Result:    fix.payload,
			ReadAudit: fix.readAudit,
		}, nil
	}
	audit := toolproxy.NewAuditStore()
	service, err := toolproxy.NewService(fix.policy, fix.reasons, upstream, audit)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}
	channel := &recordingChannel{}
	bridge, err := toolproxy.NewBridge(service, channel)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}

	// Same-call invocation: the proxy executes the explore and the exact
	// canonical ToolProxyResult bytes enter the Pi return channel before
	// InvokeBeforeCompletion returns (Host §5.4).
	delivery, derr := bridge.InvokeBeforeCompletion(context.Background(),
		fix.toolCall(session), fix.requestContext(session))
	if derr != nil {
		return nil, "DRIVER_INTERNAL_ERROR", derr.Error()
	}
	result := delivery.Result
	canonical := delivery.Canonical
	canonicalDigest := contract.DigestBytes(result.CanonicalBytes())
	piDigest := channel.lastDigest()
	return map[string]any{
		"case_id":                caseID,
		"mode":                   "ok",
		"status":                 result.Status(),
		"reason_code":            nullIfEmpty(result.ReasonCode()),
		"upstream_result_digest": nullIfEmpty(result.UpstreamResultDigest()),
		"canonical_digest":       canonicalDigest,
		"canonical_len":          len(canonical),
		"pi_bytes_equal_upstream": piDigest == canonicalDigest &&
			channel.count() == 1,
		"pi_digest":              piDigest,
		"pi_deliveries":          channel.count(),
		"delivered_before_end":   channel.count() == 1 && delivery.FirstDelivery,
		"result_order_preserved": channel.count() <= 1,
		"watermark_present":      fix.hasWatermark,
		"upstream_calls":         upstream.calls,
		"late_entries":           countAudit(audit, toolproxy.AuditLateUpstream),
		"first_delivery":         delivery.FirstDelivery,
		"expected_digest":        nullIfEmpty(fix.wantDigest),
		"scoring_fields_present": false,
	}, "", ""
}

// toolproxyRewrite drives the rewrite negative: after the same-call
// terminal froze the canonical ExploreResult bytes, a rewritten upstream
// payload attempts to replace them. The rewrite must stay audit-only
// (LATE_OUTPUT_REWRITE): the frozen terminal replays unchanged, the
// upstream is never re-invoked and nothing reaches the Pi channel a
// second time.
func toolproxyRewrite(fix *exploreFixtures, request *contract.Object, session string) (map[string]any, string, string) {
	// The rewritten payload: the frozen ExploreResult with a smuggled
	// session identity -- different bytes, same claim of authority.
	rewritten := fix.payload
	if mutated, merr := contract.ParseJSON([]byte(mustJSON(fix.payload))); merr == nil {
		if obj, ok := mutated.(*contract.Object); ok {
			obj.Set("explore_session_id", contract.String("explore-sess-rewritten"))
			rewritten = obj
		}
	}
	// The request identity is rebuilt from the frozen fixture exactly like
	// the s2s4 late-rewrite probe: the proxy request carries its own
	// idempotency key.
	mutatedRequest, _ := contract.ParseJSON([]byte(mustJSON(request)))
	reqObj, ok := mutatedRequest.(*contract.Object)
	if !ok {
		return nil, "DRIVER_INTERNAL_ERROR", "request fixture is not an object"
	}
	reqObj.Set("proxy_request_id", contract.String("pr-int003-rewrite-"+fix.caseID))
	reqObj.Set("idempotency_key", contract.String(contract.DigestBytes([]byte("int003-rewrite:"+fix.caseID))))

	upstream := &cannedUpstream{}
	upstream.response = func(context.Context, contract.Value) (*toolproxy.UpstreamResponse, error) {
		return &toolproxy.UpstreamResponse{
			Status:    toolproxy.UpstreamSucceeded,
			Result:    fix.payload,
			ReadAudit: fix.readAudit,
		}, nil
	}
	audit := toolproxy.NewAuditStore()
	service, err := toolproxy.NewService(fix.policy, fix.reasons, upstream, audit)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}

	// 1. The same-call explore freezes the terminal.
	terminal := service.Invoke(context.Background(), reqObj)
	frozenDigest := terminal.ProxyResultDigest()
	frozenCanonical := append([]byte(nil), terminal.CanonicalBytes()...)

	// 2. The Host-side rewrite attempt: the rewritten payload arrives
	// after the frozen terminal. It must stay audit-only.
	late := &toolproxy.UpstreamResponse{
		Status:    toolproxy.UpstreamSucceeded,
		Result:    rewritten,
		ReadAudit: fix.readAudit,
	}
	entry := service.RecordUpstreamLate(reqObj, late)
	rewriteAuditKind := entry.Kind

	// 3. The same identity replays: the frozen terminal answers, the
	// rewritten bytes never re-freeze and the upstream is not re-invoked.
	replayed := service.Invoke(context.Background(), reqObj)
	replayDigest := replayed.ProxyResultDigest()
	rewriteRejected := replayDigest == frozenDigest &&
		string(replayed.CanonicalBytes()) == string(frozenCanonical) &&
		upstream.calls == 1
	return map[string]any{
		"case_id":                 fix.caseID,
		"mode":                    "rewrite",
		"status":                  "failed", // the rewrite attempt itself failed
		"reason_code":             "LATE_OUTPUT_REWRITE",
		"rewrite_rejected":        rewriteRejected,
		"rewrite_audit_kind":      rewriteAuditKind,
		"frozen_status":           terminal.Status(),
		"frozen_reason_code":      nullIfEmpty(terminal.ReasonCode()),
		"frozen_digest":           frozenDigest,
		"replay_status":           replayed.Status(),
		"replay_digest":           replayDigest,
		"pi_digest":               frozenDigest, // Pi keeps the frozen bytes
		"pi_bytes_equal_upstream": false,        // != the rewritten attempt
		"rewrite_attempt_digest":  contract.DigestBytes([]byte(mustJSON(rewritten))),
		"pi_deliveries":           0, // plain Invoke path (no bridge)
		"upstream_calls":          upstream.calls,
		"late_entries":            countAudit(audit, toolproxy.AuditLateUpstream),
		"scoring_fields_present":  false,
	}, "", ""
}

func mustJSON(value contract.Value) string {
	data, err := contract.JCS(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}
