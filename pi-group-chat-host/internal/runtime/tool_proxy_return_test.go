package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/toolproxy"
)

// ---------------------------------------------------------------------------
// Same-call Tool Proxy wiring fixtures (HST-201)
// ---------------------------------------------------------------------------

// proxyUpstreamFake is the fake GMS for the same-call wiring tests: it counts
// calls and answers with a matrix-valid ExploreResult from the shared golden
// corpus.
type proxyUpstreamFake struct {
	mu    sync.Mutex
	calls []string
}

func (f *proxyUpstreamFake) Call(ctx context.Context, request contract.Value) (*toolproxy.UpstreamResponse, error) {
	obj, ok := request.(*contract.Object)
	if !ok {
		return nil, fmt.Errorf("fake GMS: request is not an object")
	}
	proxyID, _ := contract.StringOf(obj, "proxy_request_id")
	toolName, _ := contract.StringOf(obj, "tool_name")
	f.mu.Lock()
	f.calls = append(f.calls, proxyID+"/"+toolName)
	f.mu.Unlock()

	payload, readAudit := goldenExploreResult()
	// A well-behaved GMS answers within the caller's exact scope: the
	// authoritative read audit carries the request's own room/agent/scope
	// authority so the frozen scope rule observes agreement, not forgery.
	if audit, ok := readAudit.(*contract.Object); ok {
		for _, field := range []string{"room_id", "agent_id", "scope_profile_ref", "proxy_request_id"} {
			if val, present := obj.Get(field); present {
				audit.Set(field, val)
			}
		}
	}
	return &toolproxy.UpstreamResponse{
		Status:    toolproxy.UpstreamSucceeded,
		Result:    payload,
		ReadAudit: readAudit,
	}, nil
}

func (f *proxyUpstreamFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// goldenExploreResult loads the pos-explore-full-success payload from the
// conformance corpus: the same bytes the frozen validator accepts.
func goldenExploreResult() (contract.Value, contract.Value) {
	dir, err := contract.ConformanceDir()
	if err != nil {
		panic(fmt.Sprintf("resolve conformance dir: %v", err))
	}
	data, err := os.ReadFile(filepath.Join(dir, "tools", "explore", "basic", "pos-explore-full-success", "input.json"))
	if err != nil {
		panic(fmt.Sprintf("read golden explore fixture: %v", err))
	}
	parsed, err := contract.ParseJSON(data)
	if err != nil {
		panic(fmt.Sprintf("parse golden explore fixture: %v", err))
	}
	obj := parsed.(*contract.Object)
	payload, _ := obj.Get("result_payload")
	readAudit, _ := obj.Get("read_audit")
	return payload, readAudit
}

// proxyDeliveryRecord keeps one same-call delivery verbatim: which Pi tool
// call it resolved and the exact canonical bytes the return channel carried.
type proxyDeliveryRecord struct {
	ToolCallID string
	Canonical  []byte
}

// proxyReturnChannelFake records what Pi would observe per tool call.
type proxyReturnChannelFake struct {
	mu      sync.Mutex
	records []proxyDeliveryRecord
}

func (f *proxyReturnChannelFake) Deliver(ctx context.Context, toolCallID string, canonical []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, proxyDeliveryRecord{
		ToolCallID: toolCallID,
		Canonical:  append([]byte(nil), canonical...),
	})
	return nil
}

func (f *proxyReturnChannelFake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

func (f *proxyReturnChannelFake) record(i int) proxyDeliveryRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[i]
}

// newProxyBridge loads the digest-verified policy and registries and wires a
// same-call bridge over the fake upstream and return channel.
func newProxyBridge(t *testing.T, upstream toolproxy.Upstream, channel toolproxy.ReturnChannel) *toolproxy.Bridge {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	policy, err := toolproxy.LoadToolPolicy(filepath.Join(dir, "policy", "tool-success-validation.v1.json"))
	if err != nil {
		t.Fatalf("load tool policy: %v", err)
	}
	reasons, err := contract.LoadReasonBundle(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load reason bundle: %v", err)
	}
	service, err := toolproxy.NewService(policy, reasons, upstream, toolproxy.NewAuditStore())
	if err != nil {
		t.Fatalf("new proxy service: %v", err)
	}
	bridge, err := toolproxy.NewBridge(service, channel)
	if err != nil {
		t.Fatalf("new proxy bridge: %v", err)
	}
	return bridge
}

// eventLogLines reads one turn's host-events.jsonl.
func eventLogLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parse event log line %q: %v", line, err)
		}
		lines = append(lines, entry)
	}
	return lines
}

func eventIndex(lines []map[string]any, event string, match func(map[string]any) bool) int {
	for i, line := range lines {
		if line["event"] != event {
			continue
		}
		if match == nil || match(line) {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// HST-201 Red test #1: exact result returned before tool_execution_end
// ---------------------------------------------------------------------------

// TestToolProxyReturnsExactResultBeforeToolExecutionEnd proves the same-call
// contract (Host Spec §5.4, Contract §7.18/§12.7, §15 S2): a Memory tool call
// entering the proxy plane is resolved by Pi → Host proxy → GMS → exact
// ToolProxyResult while the Pi tool future is still open. The fake GMS must
// be called before the tool completion frame is processed, the exact result
// is delivered exactly once, its canonical digest equals the audit digest,
// and the legacy post-completion Memory path neither runs nor claims success.
func TestToolProxyReturnsExactResultBeforeToolExecutionEnd(t *testing.T) {
	upstream := &proxyUpstreamFake{}
	channel := &proxyReturnChannelFake{}
	bridge := newProxyBridge(t, upstream, channel)

	piBinary := writeFakePi(t, map[string][]string{
		"prompt-proxy": {
			`{"id":"prompt-proxy","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"tool_execution_start","toolCallId":"call-explore-1","toolName":"memory_explore","args":{"explore_session_id":"exp-1","query_text":"Decision Alpha evidence"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-explore-1","toolName":"memory_explore","result":{"content":[{"type":"text","text":"{\"ok\":true}"}]},"isError":false}`,
			`{"type":"agent_end","messages":[],"willRetry":false}`,
			`{"type":"agent_settled"}`,
		},
	})

	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "host-events.jsonl")
	log, closeLog, err := openTurnLog(eventLogPath)
	if err != nil {
		t.Fatalf("open event log: %v", err)
	}
	defer closeLog()

	orchestrator, _ := newReviewRuntime()
	trace, outcome, err := orchestrator.executeTurn(t.Context(), turnInput{
		authority:       reviewAuthority("memory"),
		roomInput:       "find the evidence",
		humanMessageID:  "human-proxy",
		humanKey:        "human-proxy-key",
		piBinary:        piBinary,
		promptRequestID: "prompt-proxy",
		log:             log,
		toolProxy:       bridge,
	})
	if err != nil {
		t.Fatalf("executeTurn() error = %v", err)
	}
	_ = trace
	if outcome.segment.State != "settled" {
		t.Fatalf("segment state = %q, want settled", outcome.segment.State)
	}

	// The fake GMS was called exactly once, for the proxy tool.
	if upstream.callCount() != 1 {
		t.Fatalf("fake GMS calls = %d, want exactly 1", upstream.callCount())
	}
	// Pi observed exactly one result for the call, through the same-call
	// return channel (no post-end backfill, no second delivery).
	if channel.count() != 1 {
		t.Fatalf("return-channel deliveries = %d, want exactly 1", channel.count())
	}
	delivery := channel.record(0)
	if delivery.ToolCallID != "call-explore-1" {
		t.Fatalf("delivery is not for the proxied tool call: %+v", delivery.ToolCallID)
	}

	// The delivered bytes are the exact §7.18 DTO with canonical digests.
	var observed map[string]any
	if err := json.Unmarshal(delivery.Canonical, &observed); err != nil {
		t.Fatalf("delivered bytes are not JSON: %v (%s)", err, delivery.Canonical)
	}
	if observed["schema_version"] != "host.tool-proxy-result.v1" {
		t.Fatalf("delivered schema_version = %v, want host.tool-proxy-result.v1", observed["schema_version"])
	}
	if observed["status"] != "succeeded" {
		t.Fatalf("delivered status = %v, want succeeded (exact upstream success); error=%#v", observed["status"], observed["error"])
	}
	resultPayload, ok := observed["result"].(map[string]any)
	if !ok || resultPayload["schema_version"] != "gms.explore-result.v1" {
		t.Fatalf("delivered result is not the exact ExploreResult: %#v", observed["result"])
	}
	// x-digest rule frozen by schema/shared/tool-proxy-result.v1: the digest
	// covers the delivered core minus proxy_result_digest itself.
	parsedResult, err := contract.ParseJSON(delivery.Canonical)
	if err != nil {
		t.Fatalf("delivered canonical bytes are not JSON: %v", err)
	}
	parsedObj, ok := parsedResult.(*contract.Object)
	if !ok {
		t.Fatalf("delivered canonical bytes are not an object")
	}
	core := contract.NewObject()
	for _, key := range parsedObj.Keys() {
		if key == "proxy_result_digest" || key == "extensions" {
			continue
		}
		val, _ := parsedObj.Get(key)
		core.Set(key, val)
	}
	coreDigest, err := contract.DigestOf(core)
	if err != nil {
		t.Fatalf("core digest: %v", err)
	}
	deliveredDigest, _ := observed["proxy_result_digest"].(string)
	if deliveredDigest == "" || deliveredDigest != coreDigest {
		t.Fatalf("delivered proxy_result_digest %q violates the x-digest rule (core digest %s)", observed["proxy_result_digest"], coreDigest)
	}

	// Pi-visible digest == authoritative audit digest, exactly once.
	terminalCount := 0
	for _, entry := range bridge.Audit().Entries() {
		switch entry.Kind {
		case toolproxy.AuditTerminal, toolproxy.AuditDelivery:
			terminalCount++
			if entry.ResultDigest != deliveredDigest {
				t.Fatalf("audit digest %s != Pi-observed digest %s", entry.ResultDigest, deliveredDigest)
			}
		}
	}
	if terminalCount < 2 {
		t.Fatalf("audit must carry the terminal result and the delivery (got %d entries)", terminalCount)
	}

	// The proxy executed before the tool completion frame was processed.
	lines := eventLogLines(t, eventLogPath)
	proxyDone := eventIndex(lines, "tool_proxy", func(line map[string]any) bool { return line["tool_call_id"] == "call-explore-1" })
	toolEnd := eventIndex(lines, "pi_tool_end", func(line map[string]any) bool { return line["tool_call_id"] == "call-explore-1" })
	if proxyDone < 0 {
		t.Fatalf("no tool_proxy event for the proxied call; events: %v", lines)
	}
	if toolEnd < 0 {
		t.Fatalf("no pi_tool_end event for the proxied call")
	}
	if proxyDone > toolEnd {
		t.Fatalf("tool_proxy event (index %d) must precede the completion frame (index %d): GMS was not called before tool completion", proxyDone, toolEnd)
	}

	// The legacy post-end Memory path neither executed nor claimed success.
	for _, line := range lines {
		if line["event"] != "tool_invocation" {
			continue
		}
		if line["tool"] == "memory_explore" {
			outcome, _ := line["outcome"].(string)
			reason, _ := line["reason"].(string)
			if outcome == "published" || outcome == "explored" || outcome == "started" || outcome == "submitted" {
				t.Fatalf("legacy post-end path claimed success for a proxied tool: %v", line)
			}
			if reason != "same_call_proxy_terminal" && reason != "post_end_memory_path_disabled" {
				t.Fatalf("unexpected post-end handling of proxied tool: %v", line)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Room side-effect tools keep their post-completion safety model
// ---------------------------------------------------------------------------

// TestRoomToolSemanticsUnchangedWithProxyBridge pins the Room/Memory split:
// with the same-call proxy plane active for the turn, Room tools keep their
// existing post-completion side-effect semantics (publish only after a
// successful paired completion frame), while the Memory tool resolves
// same-call. Room tools must never share the Memory result semantics.
func TestRoomToolSemanticsUnchangedWithProxyBridge(t *testing.T) {
	upstream := &proxyUpstreamFake{}
	channel := &proxyReturnChannelFake{}
	bridge := newProxyBridge(t, upstream, channel)

	piBinary := writeFakePi(t, map[string][]string{
		"prompt-split": {
			`{"id":"prompt-split","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"tool_execution_start","toolCallId":"call-room-1","toolName":"room_reply","args":{"client_operation_id":"op-room-1","in_reply_to_message_id":"human-split","content":"published after completion"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-room-1","toolName":"room_reply","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"message_id\":\"reply-1\",\"sequence\":2}}"}]},"isError":false}`,
			`{"type":"tool_execution_start","toolCallId":"call-room-2","toolName":"room_reply","args":{"client_operation_id":"op-room-2","in_reply_to_message_id":"human-split","content":"dropped failed completion"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-room-2","toolName":"room_reply","isError":true}`,
			`{"type":"tool_execution_start","toolCallId":"call-explore-2","toolName":"memory_explore","args":{"explore_session_id":"exp-2","query_text":"cite the decision"}}`,
			`{"type":"tool_execution_end","toolCallId":"call-explore-2","toolName":"memory_explore","result":{"content":[{"type":"text","text":"{}"}]},"isError":false}`,
			`{"type":"agent_end","messages":[],"willRetry":false}`,
			`{"type":"agent_settled"}`,
		},
	})

	orchestrator, store := newReviewRuntime()
	trace, _, err := orchestrator.executeTurn(t.Context(), turnInput{
		authority:       reviewAuthority("memory"),
		roomInput:       "reply and explore",
		humanMessageID:  "human-split",
		humanKey:        "human-split-key",
		piBinary:        piBinary,
		promptRequestID: "prompt-split",
		toolProxy:       bridge,
	})
	if err != nil {
		t.Fatalf("executeTurn() error = %v", err)
	}

	// Room semantics: only the successful paired completion published; the
	// failed completion frame dropped its side effect exactly as before.
	messages, err := store.Messages(t.Context(), "room-review")
	if err != nil {
		t.Fatalf("store messages: %v", err)
	}
	if len(messages) != 2 || messages[1].Content != "published after completion" {
		t.Fatalf("room messages = %#v, want human plus exactly the successful reply", messages)
	}
	// Room semantics: only the successful paired completion published; the
	// failed completion frame dropped its side effect exactly as before. In a
	// memory-profile turn the successful room reply is the memory reply
	// (frozen trace semantics), and it is the only visible tool output.
	if trace.memoryReply == nil || trace.memoryReply.Content != "published after completion" {
		t.Fatalf("memory reply = %#v, want the successful room reply", trace.memoryReply)
	}
	for _, m := range trace.visibleMessages {
		if m.Content == "dropped failed completion" {
			t.Fatalf("failed room completion leaked into visible messages: %#v", m)
		}
	}

	// Memory semantics: the proxy tool resolved same-call exactly once.
	if upstream.callCount() != 1 || channel.count() != 1 {
		t.Fatalf("proxy calls/deliveries = %d/%d, want 1/1", upstream.callCount(), channel.count())
	}
	if channel.record(0).ToolCallID != "call-explore-2" {
		t.Fatalf("proxy delivery is not for the memory tool call: %+v", channel.record(0).ToolCallID)
	}
}
