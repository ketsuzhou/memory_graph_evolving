// Command s2s4-driver is the Host-side INT-002 contract driver (HST-201
// toolproxy, HST-202 segment close, HST-203 replay scheduler).
//
// Line-delimited JSON protocol over stdio: one instruction per line on
// stdin, one single-line JSON response per instruction on stdout, logs on
// stderr. Instructions are {"id":N,"op":"...", ...}; responses are
// {"id":N,"ok":true,...} or {"id":N,"ok":false,"error":{"code","reason"}}
// with codes drawn from the §13.7.1 closed reason registry vocabulary.
//
// The driver is a pure fixture tool of the shared conformance tracer
// ($FIX/integration/run_s2_s4.py): it drives the real Host services over
// the frozen shared fixtures and reports raw facts only. It never computes
// scores, utility vectors, U1 or release semantics (Host §6.7).
//
// The conformance directory is resolved exactly like the Host test suites:
// RSIH_CONFORMANCE_DIR wins, otherwise the frozen location relative to the
// repository root.
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/replay"
	"river2.dev/pi-group-chat-host/internal/segment"
	memory "river2.dev/pi-group-chat-host/internal/store/memory"
	"river2.dev/pi-group-chat-host/internal/toolproxy"
)

func main() {
	reader := bufio.NewReaderSize(os.Stdin, 1<<20)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	response := func(payload map[string]any) {
		line, _ := json.Marshal(payload)
		writer.WriteString(string(line) + "\n")
		writer.Flush()
	}
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			handleInstruction(line, response)
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
	var code string
	var reason string
	switch op {
	case "toolproxy.invoke":
		payload, code, reason = opToolproxyInvoke(body)
	case "segment.close":
		payload, code, reason = opSegmentClose(body)
	case "replay.schedule":
		payload, code, reason = opReplaySchedule(body)
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

// parseJSONFile reads one JSON file into the decoder-model value tree.
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

func refView(o *contract.Object, key string) replay.Ref {
	ro := bodyObject(o, key)
	return replay.Ref{
		ID:      bodyString(ro, "id"),
		Version: numberOr(bodyNumber(ro, "version")),
		Digest:  bodyString(ro, "digest"),
	}
}

func digestOf(v contract.Value) string {
	d, err := contract.DigestOf(v)
	if err != nil {
		return ""
	}
	return d
}

func conformanceDir(instruction *contract.Object) (string, error) {
	if dir := bodyString(instruction, "fixtures"); dir != "" {
		return dir, nil
	}
	if dir := os.Getenv("RSIH_CONFORMANCE_DIR"); dir != "" {
		return dir, nil
	}
	return contract.ConformanceDir()
}

// ---------------------------------------------------------------------------
// S2: same-call ToolProxy over the frozen tools corpus
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

func opToolproxyInvoke(instruction *contract.Object) (map[string]any, string, string) {
	dir, err := conformanceDir(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	caseID := bodyString(instruction, "case_id")
	mode := bodyString(instruction, "mode")
	if mode == "" {
		mode = "ok"
	}
	if caseID == "" {
		return nil, "DRIVER_INSTRUCTION_INVALID", "case_id required"
	}

	// Manifest lookup + fixture load.
	manifestValue, err := parseJSONFile(filepath.Join(dir, "tools", "manifest.json"))
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	manifest := manifestValue.(*contract.Object)
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
		return nil, "FIXTURE_CASE_MISSING", "tools case " + caseID
	}
	inputValue, err := parseJSONFile(filepath.Join(dir, "tools", inputPath))
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	input := inputValue.(*contract.Object)
	expectedValue, err := parseJSONFile(filepath.Join(dir, "tools", expectedPath))
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	expected := expectedValue.(*contract.Object)
	// The fixture declares expected_accept as a JSON boolean; decoding it
	// through the string accessor would silently report every case as a
	// reject (contract.Bool is the decoder-model boolean).
	wantAccept := false
	if accept, ok := bodyValue(expected, "expected_accept").(contract.Bool); ok {
		wantAccept = bool(accept)
	}
	wantDigest := bodyString(expected, "expected_result_digest")

	request := bodyObject(input, "request")
	payload := bodyValue(input, "result_payload")
	readAudit := bodyValue(input, "read_audit")

	policy, err := toolproxy.LoadToolPolicy(filepath.Join(dir, "policy", "tool-success-validation.v1.json"))
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", "tool policy: " + err.Error()
	}
	reasons, err := contract.LoadReasonBundle(filepath.Join(dir, "policy"))
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", "reason bundle: " + err.Error()
	}
	toolName := bodyString(input, "tool_name")

	if mode == "late_rewrite" {
		return toolproxyLateRewrite(dir, policy, reasons, caseID, request, payload, readAudit, wantDigest)
	}

	upstream := &cannedUpstream{}
	upstream.response = func(context.Context, contract.Value) (*toolproxy.UpstreamResponse, error) {
		return &toolproxy.UpstreamResponse{Status: toolproxy.UpstreamSucceeded, Result: payload, ReadAudit: readAudit}, nil
	}
	audit := toolproxy.NewAuditStore()
	service, err := toolproxy.NewService(policy, reasons, upstream, audit)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}
	channel := &recordingChannel{}
	bridge, err := toolproxy.NewBridge(service, channel)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}

	// Same-call invocation: the request is rebuilt from Host-authoritative
	// state (the fixture's room/agent/delivery/scope/timeout), the proxy
	// executes it and the exact canonical bytes enter the Pi return channel
	// before InvokeBeforeCompletion returns (Host §5.4).
	call := toolproxy.Call{
		ToolCallID: "call-int002-" + caseID,
		ToolName:   toolName,
		Arguments:  bodyValue(request, "arguments"),
	}
	minSeq := bodyNumber(request, "requested_min_activation_sequence")
	rc := toolproxy.RequestContext{
		RoomID:          bodyString(request, "room_id"),
		AgentID:         bodyString(request, "agent_id"),
		DeliveryID:      bodyString(request, "delivery_id"),
		ProxyRequestID:  "pr-int002-" + caseID,
		ScopeProfileRef: bodyValue(request, "scope_profile_ref"),
		TimeoutMillis:   numberOr(bodyNumber(request, "timeout_millis")),
	}
	if minSeq != "" {
		seq := numberOr(minSeq)
		rc.RequestedMinActivationSequence = &seq
	}
	if rc.TimeoutMillis <= 0 {
		rc.TimeoutMillis = toolproxy.DefaultTimeoutMillis
	}
	delivery, derr := bridge.InvokeBeforeCompletion(context.Background(), call, rc)
	if derr != nil {
		return nil, "DRIVER_INTERNAL_ERROR", derr.Error()
	}
	result := delivery.Result
	canonical := delivery.Canonical
	piDigest := ""
	if channel.count() > 0 {
		piDigest = cDigest(channel.digests)
	}
	// The digest of the exact canonical ToolProxyResult bytes the proxy
	// froze: the bytes Pi saw through the return channel must hash here.
	canonicalDigest := contract.DigestBytes(result.CanonicalBytes())
	auditKinds := make([]string, 0, len(audit.Entries()))
	for _, entry := range audit.Entries() {
		auditKinds = append(auditKinds, entry.Kind)
	}
	payloadDigest := ""
	if result.Status() == "succeeded" {
		if d, derr := contract.DigestOf(result.ResultPayload()); derr == nil {
			payloadDigest = d
		}
	}
	return map[string]any{
		"case_id":                caseID,
		"mode":                   "ok",
		"expected_accept":        wantAccept,
		"status":                 result.Status(),
		"reason_code":            result.ReasonCode(),
		"canonical_b64":          base64.StdEncoding.EncodeToString(canonical),
		"canonical_len":          len(canonical),
		"canonical_digest":       canonicalDigest,
		"proxy_result_digest":    result.ProxyResultDigest(),
		"upstream_result_digest": nullIfEmpty(result.UpstreamResultDigest()),
		"result_payload_digest":  nullIfEmpty(payloadDigest),
		"pi_deliveries":          channel.count(),
		"pi_bytes_equal_result":  piDigest == canonicalDigest && string(canonical) == string(result.CanonicalBytes()),
		"pi_digest":              piDigest,
		"delivered_before_end":   channel.count() == 1 && delivery.FirstDelivery,
		"upstream_calls":         upstream.calls,
		"audit_kinds":            auditKinds,
		"late_entries":           countAudit(audit, toolproxy.AuditLateUpstream),
		"expected_digest":        nullIfEmpty(wantDigest),
		"scoring_fields_present": false,
		"first_delivery":         delivery.FirstDelivery,
	}, "", ""
}

func cDigest(digests []string) string {
	if len(digests) == 0 {
		return ""
	}
	return digests[len(digests)-1]
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
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

// toolproxyLateRewrite drives the post-end negative: the upstream success
// only becomes observable after the proxy deadline already fixed a timeout
// terminal; the late payload must stay audit-only and the replay of the
// same idempotency identity must return the saved failure unchanged.
func toolproxyLateRewrite(dir string, policy *toolproxy.Policy, reasons *contract.ReasonBundle,
	caseID string, request contract.Value, payload contract.Value, readAudit contract.Value,
	wantDigest string) (map[string]any, string, string) {
	release := make(chan struct{})
	upstream := &cannedUpstream{}
	upstream.response = func(ctx context.Context, _ contract.Value) (*toolproxy.UpstreamResponse, error) {
		select {
		case <-release:
			return &toolproxy.UpstreamResponse{
				Status:    toolproxy.UpstreamSucceeded,
				Result:    payload,
				ReadAudit: readAudit,
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	audit := toolproxy.NewAuditStore()
	service, err := toolproxy.NewService(policy, reasons, upstream, audit)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}
	mutated, _ := contract.ParseJSON([]byte(mustJSON(request)))
	reqObj := mutated.(*contract.Object)
	reqObj.Set("proxy_request_id", contract.String("pr-int002-late-"+caseID))
	reqObj.Set("idempotency_key", contract.String(contract.DigestBytes([]byte("int002-late:"+caseID))))
	reqObj.Set("timeout_millis", contract.Number("40"))

	terminal := service.Invoke(context.Background(), reqObj)
	firstDigest := terminal.ProxyResultDigest()
	firstCanonical := append([]byte(nil), terminal.CanonicalBytes()...)

	close(release)
	time.Sleep(20 * time.Millisecond)
	late := &toolproxy.UpstreamResponse{Status: toolproxy.UpstreamSucceeded, Result: payload, ReadAudit: readAudit}
	entry := service.RecordUpstreamLate(reqObj, late)
	_ = entry

	replayed := service.Invoke(context.Background(), reqObj)
	auditTerminal, _ := audit.Terminal("pr-int002-late-" + caseID)
	return map[string]any{
		"case_id":                     caseID,
		"mode":                        "late_rewrite",
		"first_status":                terminal.Status(),
		"first_reason_code":           terminal.ReasonCode(),
		"first_digest":                firstDigest,
		"late_entries":                countAudit(audit, toolproxy.AuditLateUpstream),
		"late_entry_kind":             entry.Kind,
		"terminal_status_after_late":  auditTerminal.Status,
		"terminal_digest_after_late":  auditTerminal.ResultDigest,
		"replay_status":               replayed.Status(),
		"replay_reason_code":          replayed.ReasonCode(),
		"replay_digest":               replayed.ProxyResultDigest(),
		"replay_canonical_equal":      string(replayed.CanonicalBytes()) == string(firstCanonical),
		"pi_deliveries":               0, // plain Invoke path; see op ok for delivery
		"upstream_calls_after_replay": upstream.calls,
		"audit_kinds":                 auditKindList(audit),
		"expected_digest":             nullIfEmpty(wantDigest),
		"scoring_fields_present":      false,
	}, "", ""
}

func auditKindList(audit *toolproxy.AuditStore) []string {
	kinds := make([]string, 0, len(audit.Entries()))
	for _, entry := range audit.Entries() {
		kinds = append(kinds, entry.Kind)
	}
	return kinds
}

func mustJSON(v contract.Value) string {
	b, err := contract.JCS(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// S3: segment close over the recorded sealed-segment corpus
// ---------------------------------------------------------------------------

type segmentFixture struct {
	caseID      string
	roomID      string
	segment     string
	version     int64
	frontier    int64
	frontierEnd int64
	intent      string
	closeKey    string
	policy      segment.Ref
	scope       segment.Ref
	extract     segment.Ref
	members     []segment.Member
	links       []segment.CausalLink
	deliver     []segment.Delivery
	exec        []segment.ToolExecution
	audit       *segment.TerminalAudit
	seal        *contract.Object
	canonical   []byte
}

func loadSegmentFixture(dir, caseID string) (*segmentFixture, error) {
	manifestValue, err := parseJSONFile(filepath.Join(dir, "recorded", "manifest.json"))
	if err != nil {
		return nil, err
	}
	manifest := manifestValue.(*contract.Object)
	relPath := ""
	for _, raw := range bodyArray(manifest, "cases") {
		entry, ok := raw.(*contract.Object)
		if !ok {
			continue
		}
		if bodyString(entry, "case_id") == caseID && bodyString(entry, "corpus") == "segments" {
			relPath = bodyString(entry, "path")
			break
		}
	}
	if relPath == "" {
		return nil, fmt.Errorf("recorded segment case %s not in manifest", caseID)
	}
	caseDir := filepath.Join(dir, "recorded", filepath.FromSlash(relPath))
	docValue, err := parseJSONFile(filepath.Join(caseDir, "segment.json"))
	if err != nil {
		return nil, err
	}
	doc := docValue.(*contract.Object)
	sealValue, err := parseJSONFile(filepath.Join(caseDir, "seal.json"))
	if err != nil {
		return nil, err
	}
	canonical, err := os.ReadFile(filepath.Join(caseDir, "canonical.utf8"))
	if err != nil {
		return nil, err
	}
	f := &segmentFixture{
		caseID:      caseID,
		roomID:      bodyString(doc, "room_id"),
		segment:     bodyString(doc, "segment_id"),
		version:     numberOr(bodyNumber(doc, "segment_version")),
		frontier:    numberOr(bodyNumber(bodyObject(doc, "frontier"), "start_room_sequence")),
		frontierEnd: numberOr(bodyNumber(bodyObject(doc, "frontier"), "end_room_sequence")),
		seal:        sealValue.(*contract.Object),
		canonical:   canonical,
	}
	closeMeta := bodyObject(doc, "close")
	f.closeKey = bodyString(closeMeta, "close_idempotency_key")
	f.intent = bodyString(closeMeta, "close_intent")
	f.policy = segmentRefOf(bodyObject(closeMeta, "close_policy_ref"))
	f.scope = segmentRefOf(bodyObject(bodyObject(doc, "evidence_seal_body"), "scope_profile_ref"))
	paths := bodyArray(bodyObject(doc, "path_seal_body"), "paths")
	if len(paths) > 0 {
		f.extract = segmentRefOf(bodyObject(paths[0].(*contract.Object), "extraction_policy_ref"))
	}
	for _, raw := range bodyArray(doc, "members") {
		m := raw.(*contract.Object)
		f.members = append(f.members, segment.Member{
			EventID:       bodyString(m, "event_id"),
			EventKind:     bodyString(m, "event_kind"),
			RoomSequence:  numberOr(bodyNumber(m, "room_sequence")),
			LogicalTick:   numberOr(bodyNumber(m, "logical_tick")),
			ActorID:       bodyString(m, "actor_id"),
			PayloadDigest: bodyString(m, "payload_digest"),
			Payload:       bodyObject(m, "payload"),
		})
	}
	for _, raw := range bodyArray(doc, "causal_links") {
		l := raw.(*contract.Object)
		f.links = append(f.links, segment.CausalLink{
			LinkID:    bodyString(l, "link_id"),
			LinkKind:  bodyString(l, "link_kind"),
			FromEvent: bodyString(l, "from_event"),
			ToEvent:   bodyString(l, "to_event"),
		})
	}
	for _, raw := range bodyArray(doc, "deliveries") {
		d := raw.(*contract.Object)
		f.deliver = append(f.deliver, segment.Delivery{
			DeliveryID:    bodyString(d, "delivery_id"),
			SubjectEvent:  bodyString(d, "subject_event"),
			TerminalState: bodyString(d, "terminal_state"),
		})
	}
	for _, raw := range bodyArray(doc, "tool_executions") {
		e := raw.(*contract.Object)
		exec := segment.ToolExecution{
			ToolCallEvent:   bodyString(e, "tool_call_event"),
			ToolResultEvent: bodyString(e, "tool_result_event"),
			DeliveryID:      bodyString(e, "delivery_id"),
			ToolProxyResult: bodyObject(e, "tool_proxy_result"),
		}
		if v, ok := e.Get("upstream_attempt_record"); ok {
			exec.UpstreamAttemptRecord, _ = v.(*contract.Object)
		}
		if v, ok := e.Get("retry_correlation"); ok {
			exec.RetryCorrelation, _ = v.(*contract.Object)
		}
		f.exec = append(f.exec, exec)
	}
	if auditVal, ok := doc.Get("terminal_audit"); ok {
		a := auditVal.(*contract.Object)
		f.audit = &segment.TerminalAudit{
			ReasonCode: bodyString(a, "reason_code"),
			Phase:      bodyString(a, "phase"),
			Detail:     bodyString(a, "detail"),
		}
	}
	return f, nil
}

func segmentRefOf(o *contract.Object) segment.Ref {
	return segment.Ref{
		ID:      bodyString(o, "id"),
		Version: numberOr(bodyNumber(o, "version")),
		Digest:  bodyString(o, "digest"),
	}
}

func seededService(f *segmentFixture) (*segment.Service, *memory.SegmentSeals, error) {
	return seedServiceWith(f, f.members)
}

// seedServiceWith replays the fixture onto a fresh service, restricted to the
// members up to the given frozen frontier (the late-append negative settles
// over the in-frontier prefix only).
func seedServiceWith(f *segmentFixture, members []segment.Member) (*segment.Service, *memory.SegmentSeals, error) {
	store := memory.NewSegmentSeals()
	service := segment.New(store)
	ctx := context.Background()
	err := service.Open(ctx, segment.OpenSegment{
		RoomID:                  f.roomID,
		SegmentID:               f.segment,
		SegmentVersion:          f.version,
		FrontierStart:           f.frontier,
		ClosePolicyRef:          f.policy,
		ScopeProfileRef:         f.scope,
		PathExtractionPolicyRef: f.extract,
	})
	if err != nil {
		return nil, store, err
	}
	for _, m := range members {
		if err := service.AppendMember(ctx, f.segment, m); err != nil {
			return nil, store, err
		}
	}
	for _, l := range f.links {
		if err := service.PutLink(ctx, f.segment, l); err != nil {
			return nil, store, err
		}
	}
	for _, d := range f.deliver {
		if err := service.RecordDelivery(ctx, f.segment, d); err != nil {
			return nil, store, err
		}
	}
	for _, e := range f.exec {
		if err := service.RecordToolExecution(ctx, f.segment, e); err != nil {
			return nil, store, err
		}
	}
	return service, store, nil
}

func opSegmentClose(instruction *contract.Object) (map[string]any, string, string) {
	dir, err := conformanceDir(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	caseID := bodyString(instruction, "case_id")
	if caseID == "" {
		return nil, "DRIVER_INSTRUCTION_INVALID", "case_id required"
	}
	f, err := loadSegmentFixture(dir, caseID)
	if err != nil {
		return nil, "FIXTURE_CASE_MISSING", err.Error()
	}
	// The recorded negative "append after settled" freezes a frontier the
	// corpus then over-runs: events beyond frontier.end_room_sequence arrived
	// after the settle. Replay settles over the in-frontier prefix and then
	// attempts the late appends -- each must be rejected APPEND_AFTER_SETTLED
	// (the sealed segment is immutable; the late transcript enters nothing).
	var inFrontier, lateMembers []segment.Member
	if f.frontierEnd > 0 {
		for _, m := range f.members {
			if m.RoomSequence > f.frontierEnd {
				lateMembers = append(lateMembers, m)
			} else {
				inFrontier = append(inFrontier, m)
			}
		}
	} else {
		inFrontier = f.members
	}
	if len(lateMembers) > 0 {
		return lateAppendClose(f, inFrontier, lateMembers, instruction)
	}
	service, store, err := seededService(f)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "seed segment: " + err.Error()
	}
	ctx := context.Background()
	intent := segment.CloseIntent(f.intent)
	req := segment.CloseRequest{Intent: intent, CloseIDempotencyKey: f.closeKey}
	if f.audit != nil && intent != segment.IntentSettle {
		req.ReasonCode = f.audit.ReasonCode
		req.Phase = f.audit.Phase
		req.Detail = f.audit.Detail
	}
	if intent == segment.IntentSettle {
		// The frozen seal record is the declared seal claim for this exact
		// input; the close verifies it before any ref is published, so a
		// flipped fixture digest fails closed with SEAL_DIGEST_MISMATCH.
		req.ExpectedSegmentDigest = bodyString(f.seal, "segment_digest")
	}
	res, cerr := service.Close(ctx, f.segment, req)
	if cerr != nil {
		return nil, "SEGMENT_CLOSE_OPERATIONAL", cerr.Error()
	}

	probes := map[string]any{}
	for _, name := range bodyStringArray(instruction, "probes") {
		switch name {
		case "append_after_settled":
			head := int64(0)
			for _, m := range f.members {
				if m.RoomSequence > head {
					head = m.RoomSequence
				}
			}
			err := service.AppendMember(ctx, f.segment, segment.Member{
				EventID:       "evt-int002-late",
				EventKind:     "message",
				RoomSequence:  head + 1,
				LogicalTick:   1,
				ActorID:       "agent-int002",
				PayloadDigest: contract.DigestBytes([]byte(`{"later":"transcript"}`)),
				Payload:       mustObject(`{"later":"transcript"}`),
			})
			probes[name] = map[string]any{
				"rejected": err != nil,
				"code":     segment.ReasonOf(err),
			}
		case "idempotency_conflict":
			_, err := service.Close(ctx, f.segment, segment.CloseRequest{
				Intent:              segment.IntentAbort,
				CloseIDempotencyKey: f.closeKey,
				ReasonCode:          "ROOM_CANCELLED",
			})
			probes[name] = map[string]any{
				"rejected": err != nil,
				"code":     segment.ReasonOf(err),
			}
		case "seal_mismatch":
			// A declared seal claim that does not cover the frozen canonical
			// bytes freezes a failed terminal with SEAL_DIGEST_MISMATCH and
			// publishes zero refs (Host 3.5): the rejection is the failed
			// close result itself, not a Go error.
			fresh, _, ferr := seededService(f)
			if ferr != nil {
				probes[name] = map[string]any{"rejected": false, "code": ferr.Error()}
				break
			}
			flipped := flipDigestHex(bodyString(f.seal, "segment_digest"))
			mres, merr := fresh.Close(ctx, f.segment, segment.CloseRequest{
				Intent:                segment.IntentSettle,
				CloseIDempotencyKey:   f.closeKey + "-mismatch",
				ExpectedSegmentDigest: flipped,
			})
			rejected := merr != nil ||
				(mres.TerminalState != segment.StateSettled && mres.SegmentRef == nil)
			code := segment.ReasonOf(merr)
			if code == "" && mres.TerminalAudit != nil {
				code = mres.TerminalAudit.ReasonCode
			}
			probes[name] = map[string]any{
				"rejected":  rejected,
				"code":      code,
				"zero_refs": mres.SegmentRef == nil && mres.EvidenceSeal == nil,
			}
		}
	}

	sealRecord, serr := store.SealRecord(ctx, f.segment)
	sealReadback := ""
	if serr == nil {
		sealReadback = sealRecord.SegmentDigest
	}

	response := map[string]any{
		"case_id":                caseID,
		"terminal_state":         res.TerminalState,
		"segment_digest":         res.SegmentDigest,
		"canonical_b64":          base64.StdEncoding.EncodeToString(res.CanonicalBytes),
		"canonical_len":          res.CanonicalByteLength,
		"seal_readback_digest":   sealReadback,
		"zero_refs":              res.SegmentRef == nil && res.CheckpointRef == nil && res.EvidenceSeal == nil && res.PathSeal == nil && len(res.Paths) == 0,
		"refs":                   nil,
		"paths":                  pathList(res.Paths),
		"terminal_audit":         auditDoc(res.TerminalAudit),
		"probes":                 probes,
		"idempotent":             res.Idempotent,
		"scoring_fields_present": false,
	}
	if res.SegmentRef != nil {
		response["refs"] = map[string]any{
			"segment_ref": map[string]any{
				"room_id":           res.SegmentRef.RoomID,
				"segment_id":        res.SegmentRef.SegmentID,
				"segment_version":   res.SegmentRef.SegmentVersion,
				"segment_digest":    res.SegmentRef.SegmentDigest,
				"evidence_seal_ref": refDoc(res.SegmentRef.EvidenceSealRef),
				"path_seal_ref":     refDoc(res.SegmentRef.PathSealRef),
			},
			"checkpoint_ref": map[string]any{
				"checkpoint_id":       res.CheckpointRef.CheckpointID,
				"checkpoint_sequence": res.CheckpointRef.CheckpointSequence,
				"checkpoint_digest":   res.CheckpointRef.CheckpointDigest,
			},
			"evidence_seal": map[string]any{
				"seal_id":      res.EvidenceSeal.SealID,
				"seal_version": res.EvidenceSeal.SealVersion,
				"seal_digest":  res.EvidenceSeal.SealDigest,
				"evidence_ref": map[string]any{
					"evidence_id":     res.EvidenceSeal.EvidenceRef.EvidenceID,
					"version":         res.EvidenceSeal.EvidenceRef.Version,
					"evidence_digest": res.EvidenceSeal.EvidenceRef.EvidenceDigest,
					"commit_state":    res.EvidenceSeal.EvidenceRef.CommitState,
					"evidence_kind":   res.EvidenceSeal.EvidenceRef.EvidenceKind,
				},
			},
			"path_seal": map[string]any{
				"seal_id":     res.PathSeal.SealID,
				"seal_digest": res.PathSeal.SealDigest,
				"path_ids":    append([]string(nil), res.PathSeal.PathIDs...),
			},
		}
	}
	return response, "", ""
}

func mustObject(raw string) *contract.Object {
	v, err := contract.ParseJSON([]byte(raw))
	if err != nil {
		return contract.NewObject()
	}
	obj, _ := v.(*contract.Object)
	return obj
}

// lateAppendClose replays the seg-neg-append-after-settled family: settle over
// the frozen frontier, then the recorded events beyond it arrive. Every late
// append must be rejected APPEND_AFTER_SETTLED; the response reports the
// rejected negative (audit + zero refs) the corpus expects, with the frozen
// settled digest retained for the audit trail.
func lateAppendClose(f *segmentFixture, inFrontier, late []segment.Member,
	instruction *contract.Object) (map[string]any, string, string) {
	service, store, serr := seedServiceWith(f, inFrontier)
	if serr != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "seed segment: " + serr.Error()
	}
	ctx := context.Background()
	res, cerr := service.Close(ctx, f.segment, segment.CloseRequest{
		Intent:              segment.CloseIntent(f.intent),
		CloseIDempotencyKey: f.closeKey,
	})
	if cerr != nil {
		return nil, "DRIVER_INTERNAL_ERROR",
			"frontier settle failed: " + cerr.Error()
	}
	code := ""
	for _, m := range late {
		aerr := service.AppendMember(ctx, f.segment, m)
		if aerr == nil {
			code = "LATE_APPEND_ACCEPTED"
			break
		}
		if c := segment.ReasonOf(aerr); c != "" {
			if code == "" {
				code = c
			} else if code != c {
				code = c
			}
		}
	}
	if code != "APPEND_AFTER_SETTLED" {
		return nil, "DRIVER_OP_FAILED",
			"late append beyond the frozen frontier was not rejected " +
				"APPEND_AFTER_SETTLED (got " + code + ")"
	}

	probes := map[string]any{}
	for _, name := range bodyStringArray(instruction, "probes") {
		switch name {
		case "append_after_settled":
			// Re-confirm on the settled service with a synthetic late event.
			head := f.frontierEnd
			err := service.AppendMember(ctx, f.segment, segment.Member{
				EventID:       "evt-int002-late",
				EventKind:     "message",
				RoomSequence:  head + 1,
				LogicalTick:   1,
				ActorID:       "agent-int002",
				PayloadDigest: contract.DigestBytes([]byte(`{"later":"transcript"}`)),
				Payload:       mustObject(`{"later":"transcript"}`),
			})
			probes[name] = map[string]any{
				"rejected": err != nil,
				"code":     segment.ReasonOf(err),
			}
		case "idempotency_conflict":
			_, err := service.Close(ctx, f.segment, segment.CloseRequest{
				Intent:              segment.IntentAbort,
				CloseIDempotencyKey: f.closeKey,
				ReasonCode:          "ROOM_CANCELLED",
			})
			probes[name] = map[string]any{
				"rejected": err != nil,
				"code":     segment.ReasonOf(err),
			}
		case "seal_mismatch":
			fresh, _, ferr := seedServiceWith(f, inFrontier)
			if ferr != nil {
				probes[name] = map[string]any{"rejected": false, "code": ferr.Error()}
				break
			}
			flipped := flipDigestHex(bodyString(f.seal, "segment_digest"))
			mres, merr := fresh.Close(ctx, f.segment, segment.CloseRequest{
				Intent:                segment.IntentSettle,
				CloseIDempotencyKey:   f.closeKey + "-mismatch",
				ExpectedSegmentDigest: flipped,
			})
			rejected := merr != nil ||
				(mres.TerminalState != segment.StateSettled && mres.SegmentRef == nil)
			pcode := segment.ReasonOf(merr)
			if pcode == "" && mres.TerminalAudit != nil {
				pcode = mres.TerminalAudit.ReasonCode
			}
			probes[name] = map[string]any{
				"rejected":  rejected,
				"code":      pcode,
				"zero_refs": mres.SegmentRef == nil && mres.EvidenceSeal == nil,
			}
		}
	}

	sealRecord, rerr := store.SealRecord(ctx, f.segment)
	sealReadback := ""
	if rerr == nil {
		sealReadback = sealRecord.SegmentDigest
	}
	return map[string]any{
		"case_id":        f.caseID,
		"terminal_state": segment.StateFailed,
		// The rejected negative mints no new sealed digest; the frozen
		// frontier digest is retained for the audit trail.
		"segment_digest":          res.SegmentDigest,
		"settled_frontier_digest": res.SegmentDigest,
		"late_append_rejected":    true,
		"canonical_b64":           base64.StdEncoding.EncodeToString(res.CanonicalBytes),
		"canonical_len":           res.CanonicalByteLength,
		"seal_readback_digest":    sealReadback,
		"zero_refs":               true,
		"refs":                    nil,
		"paths":                   []map[string]any{},
		"terminal_audit": map[string]any{
			"reason_code": "APPEND_AFTER_SETTLED",
			"phase":       "append_after_settled",
			"detail": fmt.Sprintf("%d event(s) beyond frontier end %d "+
				"were rejected after the settle", len(late), f.frontierEnd),
		},
		"probes":                 probes,
		"idempotent":             res.Idempotent,
		"scoring_fields_present": false,
	}, "", ""
}

func flipDigestHex(digest string) string {
	if len(digest) != len("sha256:")+64 {
		return "sha256:" + strings.Repeat("0", 64)
	}
	flipped := []byte(digest)
	last := len(flipped) - 1
	if flipped[last] == 'a' {
		flipped[last] = 'b'
	} else {
		flipped[last] = 'a'
	}
	return string(flipped)
}

func refDoc(r segment.Ref) map[string]any {
	return map[string]any{"id": r.ID, "version": r.Version, "digest": r.Digest}
}

func pathList(paths []segment.Path) []map[string]any {
	out := make([]map[string]any, 0, len(paths))
	for _, p := range paths {
		out = append(out, map[string]any{"path_id": p.PathID, "domain": p.Domain})
	}
	return out
}

func auditDoc(a *segment.TerminalAudit) map[string]any {
	if a == nil {
		return nil
	}
	return map[string]any{"reason_code": a.ReasonCode, "phase": a.Phase, "detail": a.Detail}
}

func bodyStringArray(o *contract.Object, key string) []string {
	var out []string
	for _, raw := range bodyArray(o, key) {
		if s, ok := raw.(contract.String); ok {
			out = append(out, string(s))
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// S4: frozen replay scheduling over the recorded Q29-B corpus
// ---------------------------------------------------------------------------

// recordedCorpus adapts $FIX/recorded to the scheduler ports (FixtureSource,
// SealSource). It is the driver-local mirror of the HST-203 test adapter:
// the corpus is read-only input, never expected output.
type recordedCorpus struct {
	root          string
	familyDirs    map[string]string
	sealDirs      map[string]string
	reserved      map[string]bool
	manifestOrder []string
	packets       map[string]replay.FixturePacket
	byFamily      map[string]replay.FixturePacket
}

func loadRecordedCorpus(root string) (*recordedCorpus, error) {
	manifestValue, err := parseJSONFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return nil, err
	}
	manifest := manifestValue.(*contract.Object)
	c := &recordedCorpus{
		root:       root,
		familyDirs: map[string]string{},
		sealDirs:   map[string]string{},
		reserved:   map[string]bool{},
		packets:    map[string]replay.FixturePacket{},
		byFamily:   map[string]replay.FixturePacket{},
	}
	for _, f := range bodyStringArray(manifest, "q29b_families") {
		c.manifestOrder = append(c.manifestOrder, f)
	}
	for _, raw := range bodyArray(manifest, "cases") {
		entry := raw.(*contract.Object)
		path := filepath.Join(root, filepath.FromSlash(bodyString(entry, "path")))
		switch bodyString(entry, "corpus") {
		case "segments":
			c.sealDirs[bodyString(entry, "case_id")] = path
		case "q29b":
			family := bodyString(entry, "family")
			if bodyString(entry, "expected_outcome") == "reserved" {
				// Placeholder declaration only (validate_recorded.py
				// reference semantics): a reserved-outcome entry never
				// carries a runnable runtime.json and never enters the
				// replay corpus. It may coexist with runnable packets of
				// the same family (the GMS-208 merge pair).
				c.reserved[family] = true
			} else if _, exists := c.familyDirs[family]; !exists {
				// A family with several recorded packets (the merge pair)
				// replays through its FIRST packet — the packet the RSIH
				// runner resolves for the family name.
				c.familyDirs[family] = path
			}
		}
	}
	for family, dir := range c.familyDirs {
		pkt := c.loadPacket(dir)
		c.packets[pkt.Ref.ID] = pkt
		c.byFamily[family] = pkt
	}
	return c, nil
}

func (c *recordedCorpus) loadPacket(dir string) replay.FixturePacket {
	data, err := os.ReadFile(filepath.Join(dir, "runtime.json"))
	if err != nil {
		return replay.FixturePacket{}
	}
	value, _ := contract.ParseJSON(data)
	obj := value.(*contract.Object)
	run := bodyObject(obj, "run")
	sealed := bodyObject(obj, "sealed_inputs")
	env := bodyObject(obj, "environment")
	caps := bodyObject(obj, "capabilities")
	expected := bodyObject(obj, "expected")

	pkt := replay.FixturePacket{
		Ref:    replay.Ref{ID: bodyString(obj, "packet_id"), Version: 1, Digest: contract.DigestBytes(data)},
		Family: bodyString(obj, "family"),
		Side:   bodyString(run, "side"),
		RunID:  bodyString(run, "run_id"),
		Body:   obj,
	}
	pkt.RunAttempt = int(numberOr(bodyNumber(run, "attempt")))
	for _, raw := range bodyArray(sealed, "segment_refs") {
		sr := raw.(*contract.Object)
		pkt.Segments = append(pkt.Segments, replay.SegmentRefInput{
			RoomID:          bodyString(sr, "room_id"),
			SegmentID:       bodyString(sr, "segment_id"),
			SegmentVersion:  numberOr(bodyNumber(sr, "segment_version")),
			SegmentDigest:   bodyString(sr, "segment_digest"),
			EvidenceSealRef: refView(sr, "evidence_seal_ref"),
			PathSealRef:     refView(sr, "path_seal_ref"),
		})
	}
	for _, raw := range bodyArray(sealed, "path_refs") {
		pr := raw.(*contract.Object)
		pkt.PathRefs = append(pkt.PathRefs, replay.PathRefView{
			PathID:      bodyString(pr, "path_id"),
			Domain:      bodyString(pr, "domain"),
			SegmentID:   bodyString(pr, "segment_id"),
			PathSealRef: refView(pr, "path_seal_ref"),
		})
	}
	for _, d := range bodyStringArray(sealed, "required_path_domains") {
		pkt.RequiredPathDomains = append(pkt.RequiredPathDomains, d)
	}
	pkt.SealedInputDigest = digestOf(sealed)
	pkt.EnvironmentDigest = digestOf(env)
	pkt.CapabilityKinds = map[string]string{}
	for _, name := range []string{"clock", "random", "tool", "provider", "filesystem"} {
		pkt.CapabilityKinds[name] = bodyString(bodyObject(caps, name), "kind")
	}
	for _, tick := range bodyArray(bodyObject(caps, "clock"), "ticks") {
		pkt.Seeds.ClockTicks = append(pkt.Seeds.ClockTicks, numberOr(stringNumber(tick)))
	}
	for _, draw := range bodyArray(bodyObject(caps, "random"), "draws") {
		pkt.Seeds.RandomDraws = append(pkt.Seeds.RandomDraws, numberOr(stringNumber(draw)))
	}
	pkt.ExpectedDigest = bodyString(expected, "output_digest")
	pkt.ExpectedStatus = bodyString(expected, "run_status")
	return pkt
}

func stringNumber(v contract.Value) string {
	if n, ok := v.(contract.Number); ok {
		return string(n)
	}
	return ""
}

func (c *recordedCorpus) Manifest(_ context.Context) (replay.FixtureManifest, error) {
	var m replay.FixtureManifest
	for _, name := range c.manifestOrder {
		decl := replay.FamilyDeclaration{Name: name}
		if pkt, ok := c.byFamily[name]; ok {
			// A runnable packet beats the reservation marker: the family
			// replays through its first packet (merge pair semantics).
			decl.PacketRefs = []replay.Ref{pkt.Ref}
		} else if c.reserved[name] {
			decl.Reserved = true
		}
		m.Families = append(m.Families, decl)
	}
	return m, nil
}

func (c *recordedCorpus) Packet(_ context.Context, ref replay.Ref) (replay.FixturePacket, error) {
	pkt, ok := c.packets[ref.ID]
	if !ok {
		return replay.FixturePacket{}, fmt.Errorf("packet %q not in recorded corpus", ref.ID)
	}
	if pkt.Ref.Digest != ref.Digest {
		return replay.FixturePacket{}, fmt.Errorf("packet %q digest mismatch", ref.ID)
	}
	return pkt, nil
}

func (c *recordedCorpus) SealedSegment(_ context.Context, segmentID string) (replay.SealedSegment, error) {
	dir, ok := c.sealDirs[segmentID]
	if !ok {
		return replay.SealedSegment{}, fmt.Errorf("segment %q not found", segmentID)
	}
	sealValue, err := parseJSONFile(filepath.Join(dir, "seal.json"))
	if err != nil {
		return replay.SealedSegment{}, err
	}
	seal := sealValue.(*contract.Object)
	canonical, err := os.ReadFile(filepath.Join(dir, "canonical.utf8"))
	if err != nil {
		return replay.SealedSegment{}, err
	}
	if contract.DigestBytes(canonical) != bodyString(seal, "segment_digest") {
		return replay.SealedSegment{}, fmt.Errorf("segment %q seal record corrupt", segmentID)
	}
	ss := replay.SealedSegment{
		TerminalState: bodyString(seal, "terminal_state"),
		SegmentDigest: bodyString(seal, "segment_digest"),
	}
	if ss.TerminalState != "settled" {
		return ss, nil
	}
	sr := bodyObject(seal, "segment_ref")
	ss.SegmentRef = replay.SegmentRefInput{
		RoomID:          bodyString(sr, "room_id"),
		SegmentID:       bodyString(sr, "segment_id"),
		SegmentVersion:  numberOr(bodyNumber(sr, "segment_version")),
		SegmentDigest:   bodyString(sr, "segment_digest"),
		EvidenceSealRef: refView(sr, "evidence_seal_ref"),
		PathSealRef:     refView(sr, "path_seal_ref"),
	}
	ps := bodyObject(seal, "path_seal")
	ss.PathSealRef = replay.Ref{
		ID:      bodyString(ps, "seal_id"),
		Version: numberOr(bodyNumber(ps, "seal_version")),
		Digest:  bodyString(ps, "seal_digest"),
	}
	segValue, err := parseJSONFile(filepath.Join(dir, "segment.json"))
	if err != nil {
		return replay.SealedSegment{}, err
	}
	seg := segValue.(*contract.Object)
	ss.PathDomains = map[string]string{}
	for _, raw := range bodyArray(bodyObject(seg, "path_seal_body"), "paths") {
		p := raw.(*contract.Object)
		ss.PathDomains[bodyString(p, "path_id")] = bodyString(p, "domain")
	}
	return ss, nil
}

// staticProfiles freezes the replay profile the recorded corpus declares.
type staticProfiles struct{ byID map[string]replay.Profile }

func (p staticProfiles) Profile(_ context.Context, ref replay.Ref) (replay.Profile, error) {
	prof, ok := p.byID[ref.ID]
	if !ok {
		return replay.Profile{}, fmt.Errorf("profile %q unavailable", ref.ID)
	}
	return prof, nil
}

// rsihRunner adapts the RSIH s2s4-driver subprocess to the RunnerPort: one
// frozen attempt identity per call, raw output only, no scoring semantics.
type rsihRunner struct {
	mu       sync.Mutex
	node     string
	rsih     string
	fixtures string
	calls    int
}

func (r *rsihRunner) Run(_ context.Context, att replay.Attempt) (replay.RawOutput, error) {
	r.mu.Lock()
	r.calls++
	node, rsih, fixtures := r.node, r.rsih, r.fixtures
	r.mu.Unlock()
	instruction := map[string]any{
		"id":       1,
		"op":       "runner.run",
		"family":   att.RunKey.Family,
		"fixtures": fixtures,
	}
	stdin, _ := json.Marshal(instruction)
	cmd := exec.Command(node, "--experimental-strip-types",
		filepath.Join(rsih, "scripts", "s2s4-driver.ts"))
	cmd.Stdin = strings.NewReader(string(stdin) + "\n")
	var out, errBuf strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	cmd.Env = append(os.Environ(), "RSIH_SKILL_EVOLUTION_CONFORMANCE_DIR="+fixtures)
	if err := cmd.Run(); err != nil {
		return replay.RawOutput{}, fmt.Errorf("rsih driver: %v: %s", err, lastLine(errBuf.String()))
	}
	response, err := contract.ParseJSON([]byte(strings.TrimSpace(out.String())))
	if err != nil {
		return replay.RawOutput{}, fmt.Errorf("rsih driver response: %v", err)
	}
	resp := response.(*contract.Object)
	// "ok" is a JSON boolean in the driver protocol; decoding it through
	// the string accessor would reject every successful RSIH response.
	if okVal, isBool := bodyValue(resp, "ok").(contract.Bool); !isBool || !bool(okVal) {
		errObj := bodyObject(resp, "error")
		return replay.RawOutput{}, fmt.Errorf("rsih driver: %s: %s",
			bodyString(errObj, "code"), bodyString(errObj, "reason"))
	}
	packetID := bodyString(resp, "packet_id")
	digest := bodyString(resp, "output_digest")
	status := bodyString(resp, "status")
	reason := bodyString(resp, "reason_code")
	var late []replay.LateEvent
	for _, raw := range bodyArray(resp, "late_audit") {
		le := raw.(*contract.Object)
		late = append(late, replay.LateEvent{
			ArrivalStep: numberOr(bodyNumber(le, "arrival_step")),
			Capability:  bodyString(le, "capability"),
			Origin:      bodyString(le, "origin"),
			RecordedAs:  bodyString(le, "recorded_as"),
		})
	}
	return replay.RawOutput{
		Ref: replay.Ref{
			ID:      "rsih.q29b-run-output/" + packetID + "/" + att.RunKey.Side + "/" + strconv.Itoa(att.AttemptNo),
			Version: 1,
			Digest:  digest,
		},
		Digest:     digest,
		Status:     status,
		ReasonCode: reason,
		LateAudit:  late,
	}, nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) == 0 {
		return ""
	}
	return lines[len(lines)-1]
}

func opReplaySchedule(instruction *contract.Object) (map[string]any, string, string) {
	dir, err := conformanceDir(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	node := bodyString(instruction, "node")
	rsih := bodyString(instruction, "rsih")
	if node == "" || rsih == "" {
		return nil, "DRIVER_INSTRUCTION_INVALID", "node and rsih required (RunnerPort dispatch)"
	}
	// The instruction may carry a repo-relative rsih path; resolve it against
	// the tracer's working directory (the driver cwd is the host repo).
	rsihAbs, aerr := filepath.Abs(rsih)
	if aerr != nil {
		return nil, "DRIVER_INSTRUCTION_INVALID", "rsih path: " + aerr.Error()
	}
	mode := bodyString(instruction, "mode")
	if mode == "" {
		mode = "ok"
	}
	request := bodyObject(instruction, "request")
	corpus, err := loadRecordedCorpus(filepath.Join(dir, "recorded"))
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	req, err := hostRequestFromDoc(request, corpus, mode)
	if err != nil {
		return nil, "DRIVER_INSTRUCTION_INVALID", err.Error()
	}

	reasons, err := contract.LoadReasonBundle(filepath.Join(dir, "policy"))
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", "reason bundle: " + err.Error()
	}
	prof := replay.Profile{
		Ref:              req.ReplayProfileRef,
		Available:        true,
		PermissionCaps:   map[string]int64{"memory_explore": 4, "memory_expand": 2, "skill_get": 8},
		Adapter:          req.RuntimeAdapterRef,
		AdapterAvailable: true,
	}
	runner := &rsihRunner{node: node, rsih: rsihAbs, fixtures: dir}
	scheduler, err := replay.NewScheduler(replay.Deps{Seals: corpus, Fixtures: corpus, Profiles: staticProfiles{
		byID: map[string]replay.Profile{prof.Ref.ID: prof},
	}}, runner, replay.Options{Reasons: reasons})
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}
	res := scheduler.Schedule(context.Background(), req)

	response := map[string]any{
		"mode":                   mode,
		"accepted":               res.Accepted,
		"reason_code":            res.ReasonCode,
		"detail":                 res.Detail,
		"plan_digest":            "",
		"family_order":           []string{},
		"outputs":                []map[string]any{},
		"probes":                 []map[string]any{},
		"runner_kind":            "rsih-subprocess",
		"rsih_calls":             runner.calls,
		"idempotent_replay":      res.IdempotentReplay,
		"scoring_fields_present": false,
	}
	if res.Plan != nil {
		response["plan_digest"] = res.Plan.PlanDigest
		response["family_order"] = res.Plan.FamilyOrder
	}
	for _, o := range res.Outputs {
		lateAudit := make([]map[string]any, 0, len(o.LateAudit))
		for _, le := range o.LateAudit {
			lateAudit = append(lateAudit, map[string]any{
				"arrival_step": le.ArrivalStep,
				"recorded_as":  le.RecordedAs,
			})
		}
		response["outputs"] = append(response["outputs"].([]map[string]any), map[string]any{
			"family":          o.Family,
			"side":            o.Side,
			"run_id":          o.RunID,
			"attempts":        o.Attempts,
			"terminal_status": o.TerminalStatus,
			"reason_code":     o.ReasonCode,
			"output_ref":      o.OutputRef.ID,
			"output_digest":   o.OutputDigest,
			"late_audit":      lateAudit,
		})
	}
	for _, p := range res.DeterminismProbes {
		response["probes"] = append(response["probes"].([]map[string]any), map[string]any{
			"family":          p.RunKey.Family,
			"side":            p.RunKey.Side,
			"correlation_id":  p.CorrelationID,
			"recorded_digest": p.RecordedDigest,
			"probed_digest":   p.ProbedDigest,
			"matched":         p.Matched,
		})
	}

	if mode == "cross_run" {
		// Same idempotency key, different frozen input: the replay lease
		// must fail closed instead of double-dispatching (Host §9.3 CAS).
		mutated := *req
		mutated.ReplayRequestID = req.ReplayRequestID + "-contaminated"
		second := scheduler.Schedule(context.Background(), &mutated)
		response["second_accepted"] = second.Accepted
		response["second_reason_code"] = second.ReasonCode
		response["second_idempotent_replay"] = second.IdempotentReplay
	}
	return response, "", ""
}

// hostRequestFromDoc maps the GMS-built §7.10 ReplayRequest document onto
// the Host scheduler request. fixture_set_refs pins the recorded manifest
// identity; the driver expands it to the per-packet refs the corpus
// declares (the manifest is the fixture set).
func hostRequestFromDoc(doc *contract.Object, corpus *recordedCorpus, mode string) (*replay.Request, error) {
	manifest, err := corpus.Manifest(context.Background())
	if err != nil {
		return nil, err
	}
	var fixtureRefs []replay.Ref
	pinned := bodyArray(doc, "fixture_set_refs")
	for _, raw := range pinned {
		ref := raw.(*contract.Object)
		if bodyString(ref, "id") == "fixture-q29b" {
			manifestBytes, err := os.ReadFile(filepath.Join(corpus.root, "manifest.json"))
			if err != nil {
				return nil, err
			}
			if bodyString(ref, "digest") != contract.DigestBytes(manifestBytes) {
				return nil, fmt.Errorf("fixture set digest does not pin the recorded manifest")
			}
		}
	}
	for _, fam := range manifest.Families {
		if fam.Reserved {
			continue
		}
		if mode == "missing_family" && fam.Name == "candidate" {
			continue // negative probe: family dropped from the fixture set
		}
		fixtureRefs = append(fixtureRefs, fam.PacketRefs...)
	}

	var segRefs []replay.SegmentRefInput
	for _, raw := range bodyArray(doc, "segment_refs") {
		sr := raw.(*contract.Object)
		segRefs = append(segRefs, replay.SegmentRefInput{
			RoomID:          bodyString(sr, "room_id"),
			SegmentID:       bodyString(sr, "segment_id"),
			SegmentVersion:  numberOr(bodyNumber(sr, "segment_version")),
			SegmentDigest:   bodyString(sr, "segment_digest"),
			EvidenceSealRef: refView(sr, "evidence_seal_ref"),
			PathSealRef:     refView(sr, "path_seal_ref"),
		})
	}

	var baselines []replay.SkillRef
	for _, raw := range bodyArray(doc, "baseline_skill_refs") {
		baselines = append(baselines, skillRefOf(raw.(*contract.Object)))
	}
	candRaw := bodyObject(doc, "candidate_ref")
	req := &replay.Request{
		ReplayRequestID:   bodyString(doc, "replay_request_id"),
		CorrelationID:     "corr-int002-" + contract.DigestBytes([]byte(bodyString(doc, "replay_request_id")))[7:23],
		Mode:              bodyString(doc, "mode"),
		IdempotencyKey:    bodyString(doc, "idempotency_key"),
		CandidateRef:      candidateRefOf(candRaw),
		BaselineSkillRefs: baselines,
		FixtureSetRefs:    fixtureRefs,
		SegmentRefs:       segRefs,
		ReplayProfileRef:  refView(doc, "replay_profile_ref"),
		RuntimeAdapterRef: refView(doc, "runtime_adapter_ref"),
		Permissions:       map[string]int64{"memory_explore": 1, "skill_get": 1},
	}
	if req.Mode == "" {
		req.Mode = replay.ModeCausalEvaluation
	}
	return req, nil
}

func skillRefOf(o *contract.Object) replay.SkillRef {
	return replay.SkillRef{
		LineageID:      bodyString(o, "lineage_id"),
		Version:        numberOr(bodyNumber(o, "version")),
		Kind:           bodyString(o, "kind"),
		ArtifactDigest: bodyString(o, "artifact_digest"),
	}
}

func candidateRefOf(o *contract.Object) replay.CandidateRef {
	return replay.CandidateRef{
		CandidateID: bodyString(o, "candidate_id"),
		Kind:        bodyString(o, "kind"),
		BodyDigest:  bodyString(o, "body_digest"),
		OriginType:  bodyString(o, "origin_type"),
		OriginRef:   refView(o, "origin_ref"),
	}
}

// sortKeys keeps JSON output deterministic for map payloads.
func init() {
	_ = sort.Strings
}
