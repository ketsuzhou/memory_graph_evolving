package toolproxy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// fakeUpstream is the in-process fake GMS: canned responses plus a call log.
type fakeUpstream struct {
	mu       sync.Mutex
	calls    []string // proxy_request_id per call
	response func(ctx context.Context, request contract.Value) (*UpstreamResponse, error)
}

func (f *fakeUpstream) Call(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
	f.mu.Lock()
	proxyID := proxyRequestIDOf(request)
	f.calls = append(f.calls, proxyID)
	response := f.response
	f.mu.Unlock()
	if response == nil {
		return nil, fmt.Errorf("fake upstream: no response configured")
	}
	return response(ctx, request)
}

func (f *fakeUpstream) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// recordingChannel captures same-call deliveries Pi would observe.
type recordingChannel struct {
	mu         sync.Mutex
	deliveries []recordedDelivery
	fail       bool
}

type recordedDelivery struct {
	ToolCallID string
	Canonical  []byte
}

func (r *recordingChannel) Deliver(ctx context.Context, toolCallID string, canonical []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return fmt.Errorf("return channel broken")
	}
	r.deliveries = append(r.deliveries, recordedDelivery{ToolCallID: toolCallID, Canonical: append([]byte(nil), canonical...)})
	return nil
}

func (r *recordingChannel) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.deliveries)
}

// newTestService loads the digest-verified policy and registries from the
// shared conformance directory (HST-101 loader conventions).
func newTestService(t *testing.T, upstream Upstream) (*Service, *contract.ReasonBundle) {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	policy, err := LoadToolPolicy(filepath.Join(dir, "policy", policyFileName))
	if err != nil {
		t.Fatalf("load tool policy: %v", err)
	}
	reasons, err := contract.LoadReasonBundle(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load reason bundle: %v", err)
	}
	service, err := NewService(policy, reasons, upstream, NewAuditStore())
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service, reasons
}

// objectOf converts a Go value into a contract value tree (test-side sugar).
func objectOf(t *testing.T, value any) *contract.Object {
	t.Helper()
	v := valueOf(t, value)
	obj, ok := v.(*contract.Object)
	if !ok {
		t.Fatalf("valueOf(%v) is not an object", value)
	}
	return obj
}

func valueOf(t *testing.T, value any) contract.Value {
	t.Helper()
	switch typed := value.(type) {
	case nil:
		return contract.Null{}
	case bool:
		return contract.Bool(typed)
	case int:
		return contract.Number(fmt.Sprintf("%d", typed))
	case int64:
		return contract.Number(fmt.Sprintf("%d", typed))
	case string:
		return contract.String(typed)
	case map[string]any:
		obj := contract.NewObject()
		for key, item := range typed {
			obj.Set(key, valueOf(t, item))
		}
		return obj
	case []any:
		arr := contract.Array{}
		for _, item := range typed {
			arr = append(arr, valueOf(t, item))
		}
		return arr
	default:
		t.Fatalf("unsupported test value %T", value)
		return nil
	}
}

// cloneValue deep-copies a contract value through canonical bytes.
func cloneValue(t *testing.T, v contract.Value) contract.Value {
	t.Helper()
	canonical, err := contract.JCS(v)
	if err != nil {
		t.Fatalf("clone: canonicalize: %v", err)
	}
	cloned, err := contract.ParseJSON(canonical)
	if err != nil {
		t.Fatalf("clone: parse: %v", err)
	}
	return cloned
}

// fixtureRequest loads one $FIX/tools case and returns its §7.17 request
// object, result payload and read audit record.
func fixtureCase(t *testing.T, inputPath string) (request contract.Value, payload contract.Value, readAudit contract.Value) {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "tools", inputPath))
	if err != nil {
		t.Fatalf("read fixture %s: %v", inputPath, err)
	}
	parsed, err := contract.ParseJSON(data)
	if err != nil {
		t.Fatalf("parse fixture %s: %v", inputPath, err)
	}
	obj, ok := parsed.(*contract.Object)
	if !ok {
		t.Fatalf("fixture %s is not an object", inputPath)
	}
	requestVal, _ := obj.Get("request")
	payloadVal, _ := obj.Get("result_payload")
	auditVal, _ := obj.Get("read_audit")
	return requestVal, payloadVal, auditVal
}

// exploreSuccessResponse is a minimal matrix-valid ExploreResult upstream
// response used by the behavioral tests.
func exploreSuccessResponse(t *testing.T) *UpstreamResponse {
	watermark := map[string]any{
		"schema_version":                        "gms.projection-watermark.v1",
		"projection_stream":                     "runtime",
		"projection_schema_version":             "gms.skill-graph.v1",
		"projection_head":                       "head-0001",
		"projected_through_activation_sequence": 12,
		"source_ledger_digest":                  "sha256:ff7394ac79f8864017b3ad5dab62c4604e216aa4dd6c969d6300cdebaaa3f5cfd",
		"state":                                 "current",
		"watermark_digest":                      "sha256:5ed66715f9a45f3b7c9286fefdfd9898129a6eed7237d32d33b504dbb5b3415e",
	}
	payload := map[string]any{
		"schema_version":          "gms.explore-result.v1",
		"explore_session_id":      "exp-session-0001",
		"query_digest":            "sha256:ff4d81de49a996a1926f98296fa4d708cfba2865239c4d3aa8f8f8868622c1d6",
		"ranker_policy_ref":       map[string]any{"id": "gms.ranker.lexical-graph.v1", "version": 1, "digest": "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
		"watermark":               watermark,
		"evidence_results":        []any{},
		"skill_results":           []any{},
		"served_fences":           map[string]any{"evidence_fence_digest": "sha256:61eb8ed84abc1de2a89cdddfc2aa8af86a75fb9e8947a4c8838eff267ea70da8", "skill_fence_digest": "sha256:583bf68d36a7851c0bdad8255acb878353f09b7b7638cf8ea26031ab6ddbfaf4"},
		"budgets":                 map[string]any{"total_cap": 10, "evidence_subcap": 4, "skill_subcap": 4, "guidance_token_budget": 2000, "total_used": 0},
		"truncation_reason_codes": []any{},
		"omissions":               []any{},
	}
	return &UpstreamResponse{Status: UpstreamSucceeded, Result: valueOf(t, payload)}
}

// validExploreRequest builds one §7.17 request for the behavioral tests.
func validExploreRequest(t *testing.T, proxyRequestID, idempotencyKey string) contract.Value {
	t.Helper()
	return objectOf(t, map[string]any{
		"schema_version":   RequestSchemaVersion,
		"proxy_request_id": proxyRequestID,
		"room_id":          "room-proxy-test",
		"agent_id":         "agent-memory-test",
		"delivery_id":      "delivery-proxy-test",
		"tool_name":        "memory_explore",
		"arguments":        map[string]any{},
		"scope_profile_ref": map[string]any{
			"id": "scope-memory-room-proxy", "version": 1,
			"digest": "sha256:a9c06482a59005dc88a590d7a9df512396e4f776455a23649e8c09ec21719726",
		},
		"idempotency_key": idempotencyKey,
		"timeout_millis":  5000,
	})
}

var testSHA256 = "sha256:79da9f33a1d180e06b800862feb6c356d9ce86894ef847b2f271e83919ab3705"

// ---------------------------------------------------------------------------
// HST-201 Red test #2: late upstream result cannot rewrite terminal
// ---------------------------------------------------------------------------

// TestToolProxyLateResultCannotRewriteTerminal proves the terminal CAS: once
// a proxy call has returned its exact terminal result (here a timeout
// failure), a late GMS response is recorded as audit only — the terminal
// result, its digest, and every later replay stay unchanged, and no second
// Pi-visible result can appear (Host §5.8, Contract §13.7.1 R3-4).
func TestToolProxyLateResultCannotRewriteTerminal(t *testing.T) {
	release := make(chan struct{})
	upstream := &fakeUpstream{}
	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		// The GMS response is only observable after the Host deadline hit:
		// honor the proxy context so the late answer never blocks the turn.
		select {
		case <-release:
			resp := exploreSuccessResponse(t)
			resp.ReadAudit = objectOf(t, map[string]any{
				"schema_version": "host.read-audit.v1",
				"room_id":        "room-proxy-test",
				"agent_id":       "agent-memory-test",
			})
			return resp, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	service, _ := newTestService(t, upstream)

	// A short timeout so the terminal failure is deterministic.
	request := validExploreRequest(t, "pr-late-0001", testSHA256)
	reqObj := request.(*contract.Object)
	reqObj.Set("timeout_millis", contract.Number("40"))

	terminal := service.Invoke(context.Background(), request)
	if status := terminal.Status(); status != "failed" {
		t.Fatalf("terminal status = %q, want failed (exact timeout failure)", status)
	}
	if code := terminal.ReasonCode(); code != CodeTimeout {
		t.Fatalf("terminal reason = %q, want %q", code, CodeTimeout)
	}
	if !terminal.Retryable() {
		t.Fatalf("HOST_PROXY_TIMEOUT must be retryable per the frozen registry")
	}
	terminalDigest := terminal.ProxyResultDigest()
	terminalCanonical := append([]byte(nil), terminal.CanonicalBytes()...)

	// The late upstream response arrives after the terminal result.
	close(release)
	time.Sleep(20 * time.Millisecond)
	lateResponse := exploreSuccessResponse(t)
	entry := service.RecordUpstreamLate(request, lateResponse)
	if entry.Kind != AuditLateUpstream {
		t.Fatalf("late entry kind = %q, want %q", entry.Kind, AuditLateUpstream)
	}

	// The terminal audit stays the timeout failure, byte for byte.
	auditTerminal, ok := service.Audit().Terminal("pr-late-0001")
	if !ok {
		t.Fatalf("terminal audit entry missing for pr-late-0001")
	}
	if auditTerminal.ResultDigest != terminalDigest || auditTerminal.ReasonCode != CodeTimeout {
		t.Fatalf("terminal audit rewritten by late result: %+v", auditTerminal)
	}
	if got := len(service.Audit().Entries()); got < 3 {
		t.Fatalf("audit entries = %d, want request + terminal + late entries", got)
	}
	lateEntries := 0
	for _, e := range service.Audit().Entries() {
		if e.Kind == AuditLateUpstream {
			lateEntries++
		}
	}
	if lateEntries != 1 {
		t.Fatalf("late audit entries = %d, want exactly 1", lateEntries)
	}

	// Replay with the same key and digest returns the saved terminal — the
	// late success can never become the observed result.
	replayed := service.Invoke(context.Background(), request)
	if replayed.Status() != "failed" || replayed.ReasonCode() != CodeTimeout {
		t.Fatalf("replay after late result = %s/%s, want the saved timeout failure", replayed.Status(), replayed.ReasonCode())
	}
	if replayed.ProxyResultDigest() != terminalDigest {
		t.Fatalf("replay digest %s != terminal digest %s (terminal rewrite)", replayed.ProxyResultDigest(), terminalDigest)
	}
	if string(replayed.CanonicalBytes()) != string(terminalCanonical) {
		t.Fatalf("replay canonical bytes differ from the terminal canonical bytes")
	}
	if upstream.callCount() != 1 {
		t.Fatalf("upstream calls = %d, want exactly 1 (replay must not re-call GMS)", upstream.callCount())
	}
}

// ---------------------------------------------------------------------------
// CTR-002 matrix parity over the golden corpus
// ---------------------------------------------------------------------------

// TestInvokeMatchesCtr002FixtureCorpus runs every §12.7.1 fixture case
// through the Service: accepted payloads must become exact succeeded results
// with the frozen result digest, rejected payloads must become exact failures
// carrying the Host-owned code the policy freezes for the expected system
// reason (missing watermark, smuggled GuidanceView fields, wrappers, budget
// violation, freshness, view-hash mismatch, unknown tool, ...).
func TestInvokeMatchesCtr002FixtureCorpus(t *testing.T) {
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	manifestData, err := os.ReadFile(filepath.Join(dir, "tools", "manifest.json"))
	if err != nil {
		t.Fatalf("read tools manifest: %v", err)
	}
	manifest, err := contract.ParseJSON(manifestData)
	if err != nil {
		t.Fatalf("parse tools manifest: %v", err)
	}
	manifestObj := manifest.(*contract.Object)
	casesVal, _ := manifestObj.Get("cases")
	cases, ok := casesVal.(contract.Array)
	if !ok || len(cases) == 0 {
		t.Fatalf("tools manifest carries no cases")
	}

	upstream := &fakeUpstream{}
	service, _ := newTestService(t, upstream)
	for _, raw := range cases {
		entry, ok := raw.(*contract.Object)
		if !ok {
			t.Fatalf("manifest case is not an object")
		}
		caseID, _ := contract.StringOf(entry, "case_id")
		inputPath, _ := contract.StringOf(entry, "input_path")
		expectedPath, _ := contract.StringOf(entry, "expected_path")
		request, payload, readAudit := fixtureCase(t, inputPath)
		// Every corpus case is its own logical proxy call: give it its own
		// proxy_request_id (one attempt, one terminal) and its own
		// idempotency identity so neither the terminal CAS nor the lease can
		// replay a sibling case's result (the fixtures reuse both because
		// they are independent files).
		if reqObj, ok := request.(*contract.Object); ok {
			reqObj.Set("proxy_request_id", contract.String("pr-case-"+caseID))
			reqObj.Set("idempotency_key", contract.String(contract.DigestBytes([]byte("case:"+caseID))))
		}

		expectedData, err := os.ReadFile(filepath.Join(dir, "tools", expectedPath))
		if err != nil {
			t.Fatalf("case %s: read expected: %v", caseID, err)
		}
		expectedValue, err := contract.ParseJSON(expectedData)
		if err != nil {
			t.Fatalf("case %s: parse expected: %v", caseID, err)
		}
		expected := expectedValue.(*contract.Object)
		wantAccept, _ := expected.Get("expected_accept")
		wantAcceptBool, ok := wantAccept.(contract.Bool)
		if !ok {
			t.Fatalf("case %s: expected_accept must be a boolean", caseID)
		}

		upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
			return &UpstreamResponse{Status: UpstreamSucceeded, Result: payload, ReadAudit: readAudit}, nil
		}
		result := service.Invoke(context.Background(), request)
		if bool(wantAcceptBool) {
			if result.Status() != "succeeded" {
				t.Fatalf("case %s: status = %s (%s), want succeeded", caseID, result.Status(), result.ReasonCode())
			}
			wantDigest, _ := contract.StringOf(expected, "expected_result_digest")
			if result.UpstreamResultDigest() != wantDigest {
				t.Fatalf("case %s: upstream digest %s != frozen expected digest %s", caseID, result.UpstreamResultDigest(), wantDigest)
			}
			payloadDigest, err := contract.DigestOf(result.ResultPayload())
			if err != nil || payloadDigest != wantDigest {
				t.Fatalf("case %s: delivered result payload digest mismatch: %v %s", caseID, err, payloadDigest)
			}
		} else {
			wantReason, _ := contract.StringOf(expected, "expected_reason_code")
			wantHostCode := service.Policy().HostCodeFor[wantReason]
			if wantHostCode == "" {
				t.Fatalf("case %s: expected reason %s has no frozen Host code mapping", caseID, wantReason)
			}
			if result.Status() == "succeeded" {
				t.Fatalf("case %s: status = succeeded, want exact failure", caseID)
			}
			if got := result.ReasonCode(); got != wantHostCode {
				t.Fatalf("case %s: reason = %s, want Host code %s (system %s)", caseID, got, wantHostCode, wantReason)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Timeout / cancel exact failures
// ---------------------------------------------------------------------------

// TestTimeoutAndCancelProduceExactFailures pins Host §5.4/§5.5.4: timeout and
// cancellation return exact failed results whose upstream_result_digest is
// the canonical digest of the Host-private upstream-attempt record — never a
// placeholder success and never a forged GMS payload.
func TestTimeoutAndCancelProduceExactFailures(t *testing.T) {
	upstream := &fakeUpstream{}
	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return exploreSuccessResponse(t), nil
		}
	}
	service, _ := newTestService(t, upstream)

	request := validExploreRequest(t, "pr-timeout-0001", testSHA256)
	request.(*contract.Object).Set("timeout_millis", contract.Number("30"))
	start := time.Now()
	result := service.Invoke(context.Background(), request)
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("timeout result took %v, deadline was 30ms", elapsed)
	}
	if result.Status() != "failed" || result.ReasonCode() != CodeTimeout {
		t.Fatalf("timeout result = %s/%s, want failed/%s", result.Status(), result.ReasonCode(), CodeTimeout)
	}
	if !result.Retryable() {
		t.Fatalf("timeout failure must be retryable per the frozen registry")
	}
	if payload := result.ResultPayload(); payload != nil {
		t.Fatalf("timeout result carries a success payload — placeholder success forbidden")
	}
	wantAttempt := objectOf(t, map[string]any{
		"schema_version":   upstreamAttemptSchema,
		"proxy_request_id": "pr-timeout-0001",
		"state":            "no_response",
		"host_reason_code": CodeTimeout,
	})
	wantDigest, err := contract.DigestOf(wantAttempt)
	if err != nil {
		t.Fatalf("attempt digest: %v", err)
	}
	if result.UpstreamResultDigest() != wantDigest {
		t.Fatalf("timeout upstream digest %s != upstream-attempt record digest %s", result.UpstreamResultDigest(), wantDigest)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		return exploreSuccessResponse(t), nil
	}
	// The cancelled call is a distinct logical request: its own idempotency
	// identity, or the lease would replay the timeout terminal.
	cancelled := service.Invoke(cancelCtx, validExploreRequest(t, "pr-cancel-0001", contract.DigestBytes([]byte("pr-cancel-0001"))))
	if cancelled.Status() != "failed" || cancelled.ReasonCode() != CodeCancelled {
		t.Fatalf("cancelled result = %s/%s, want failed/%s", cancelled.Status(), cancelled.ReasonCode(), CodeCancelled)
	}
	if cancelled.Retryable() {
		t.Fatalf("HOST_PROXY_CANCELLED must not be retryable per the frozen registry")
	}
	cancelAttempt := objectOf(t, map[string]any{
		"schema_version":   upstreamAttemptSchema,
		"proxy_request_id": "pr-cancel-0001",
		"state":            "cancelled",
		"host_reason_code": CodeCancelled,
	})
	cancelDigest, _ := contract.DigestOf(cancelAttempt)
	if cancelled.UpstreamResultDigest() != cancelDigest {
		t.Fatalf("cancelled upstream digest %s != attempt record digest %s", cancelled.UpstreamResultDigest(), cancelDigest)
	}
}

// ---------------------------------------------------------------------------
// Idempotency lease (Host §5.7)
// ---------------------------------------------------------------------------

// TestIdempotencyLeaseReplayAndConflict pins the lease behavior: same key
// and same canonical request digest return the saved terminal without a
// second upstream call or a second Agent observation; same key with a
// different digest fails closed with IDEMPOTENCY_CONFLICT.
func TestIdempotencyLeaseReplayAndConflict(t *testing.T) {
	upstream := &fakeUpstream{}
	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		resp := exploreSuccessResponse(t)
		resp.ReadAudit = objectOf(t, map[string]any{"schema_version": "host.read-audit.v1", "room_id": "room-proxy-test", "agent_id": "agent-memory-test"})
		return resp, nil
	}
	service, _ := newTestService(t, upstream)

	request := validExploreRequest(t, "pr-idem-0001", testSHA256)
	first := service.Invoke(context.Background(), request)
	if first.Status() != "succeeded" {
		t.Fatalf("first invoke = %s (%s), want succeeded", first.Status(), first.ReasonCode())
	}

	replay := service.Invoke(context.Background(), cloneValue(t, request))
	if string(replay.CanonicalBytes()) != string(first.CanonicalBytes()) {
		t.Fatalf("idempotent replay returned different canonical bytes")
	}
	if replay.ProxyResultDigest() != first.ProxyResultDigest() {
		t.Fatalf("idempotent replay digest changed")
	}
	if upstream.callCount() != 1 {
		t.Fatalf("upstream calls = %d, want exactly 1 across replay", upstream.callCount())
	}

	// Same key, different request digest -> closed conflict.
	conflicting := cloneValue(t, request).(*contract.Object)
	conflicting.Set("arguments", objectOf(t, map[string]any{"query_text": "different"}))
	conflict := service.Invoke(context.Background(), conflicting)
	if conflict.Status() != "failed" || conflict.ReasonCode() != CodeIdempotencyConflict {
		t.Fatalf("conflict result = %s/%s, want failed/%s", conflict.Status(), conflict.ReasonCode(), CodeIdempotencyConflict)
	}
	if conflict.Retryable() {
		t.Fatalf("IDEMPOTENCY_CONFLICT must not be retryable per the frozen registry")
	}
	if upstream.callCount() != 1 {
		t.Fatalf("conflicting request must not reach upstream (calls = %d)", upstream.callCount())
	}
	// The original terminal is untouched by the conflict.
	if saved := service.Invoke(context.Background(), request); saved.ProxyResultDigest() != first.ProxyResultDigest() {
		t.Fatalf("saved terminal changed after idempotency conflict")
	}
}

// ---------------------------------------------------------------------------
// Upstream reason-code precedence (Contract §13.7.1 R3)
// ---------------------------------------------------------------------------

// TestUpstreamReasonPrecedenceReplacesUnknownCodes pins R3: a known system
// failure code passes through verbatim with registry retry semantics, while
// an unknown upstream code is replaced by UPSTREAM_SCHEMA_INVALID and only
// survives as non-behavioral diagnostic metadata.
func TestUpstreamReasonPrecedenceReplacesUnknownCodes(t *testing.T) {
	upstream := &fakeUpstream{}
	service, _ := newTestService(t, upstream)

	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		return &UpstreamResponse{Status: UpstreamFailed, ReasonCode: "GMS_TOTALLY_MADE_UP", Message: "mystery"}, nil
	}
	unknown := service.Invoke(context.Background(), validExploreRequest(t, "pr-unknown-0001", contract.DigestBytes([]byte("pr-unknown-0001"))))
	if unknown.ReasonCode() != CodeUpstreamSchema {
		t.Fatalf("unknown upstream code result = %s, want replaced by %s", unknown.ReasonCode(), CodeUpstreamSchema)
	}
	if unknown.Retryable() {
		t.Fatalf("unknown-upstream replacement must not be retryable")
	}
	errObj := unknown.Object()
	errVal, _ := errObj.Get("error")
	errEnvelope := errVal.(*contract.Object)
	message, _ := contract.StringOf(errEnvelope, "message")
	if !strings.Contains(message, "GMS_TOTALLY_MADE_UP") {
		t.Fatalf("original code must survive as diagnostic metadata in the message, got %q", message)
	}
	if code, _ := contract.StringOf(errEnvelope, "reason_code"); strings.Contains(code, "GMS_TOTALLY_MADE_UP") {
		t.Fatalf("unknown upstream code leaked into the behavioral reason_code slot")
	}

	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		return &UpstreamResponse{Status: UpstreamFailed, ReasonCode: "EVIDENCE_NOT_COMMITTED", Message: "evidence pending"}, nil
	}
	known := service.Invoke(context.Background(), validExploreRequest(t, "pr-known-0001", contract.DigestBytes([]byte("pr-known-0001"))))
	if known.ReasonCode() != "EVIDENCE_NOT_COMMITTED" {
		t.Fatalf("known upstream code = %s, want verbatim EVIDENCE_NOT_COMMITTED", known.ReasonCode())
	}
	if known.Retryable() {
		t.Fatalf("EVIDENCE_NOT_COMMITTED retryable must come from the registry (false)")
	}

	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		return &UpstreamResponse{Status: UpstreamFailed, ReasonCode: "PROJECTION_BEHIND_REQUIRED_SEQUENCE", Message: "behind"}, nil
	}
	retryable := service.Invoke(context.Background(), validExploreRequest(t, "pr-known-0002", contract.DigestBytes([]byte("pr-known-0002"))))
	if retryable.ReasonCode() != "PROJECTION_BEHIND_REQUIRED_SEQUENCE" || !retryable.Retryable() {
		t.Fatalf("shared infra code must pass through with registry retry semantics, got %s retryable=%v", retryable.ReasonCode(), retryable.Retryable())
	}
}

// ---------------------------------------------------------------------------
// Frozen digest preimage invariant
// ---------------------------------------------------------------------------

// TestProxyResultDigestPreimageIsFrozenCore verifies the x-digest rule frozen
// by schema/shared/tool-proxy-result.v1: proxy_result_digest is the JCS
// SHA-256 of the result core minus the digest field itself.
func TestProxyResultDigestPreimageIsFrozenCore(t *testing.T) {
	upstream := &fakeUpstream{}
	upstream.response = func(ctx context.Context, request contract.Value) (*UpstreamResponse, error) {
		return exploreSuccessResponse(t), nil
	}
	service, _ := newTestService(t, upstream)

	result := service.Invoke(context.Background(), validExploreRequest(t, "pr-digest-0001", testSHA256))
	if result.Status() != "succeeded" {
		t.Fatalf("status = %s (%s), want succeeded", result.Status(), result.ReasonCode())
	}
	core := contract.NewObject()
	for _, key := range result.Object().Keys() {
		if key == "proxy_result_digest" {
			continue
		}
		val, _ := result.Object().Get(key)
		core.Set(key, val)
	}
	want, err := contract.DigestOf(core)
	if err != nil {
		t.Fatalf("core digest: %v", err)
	}
	if result.ProxyResultDigest() != want {
		t.Fatalf("proxy_result_digest %s != frozen preimage digest %s", result.ProxyResultDigest(), want)
	}
	// The canonical bytes round-trip and still satisfy the x-digest rule:
	// hashing the delivered object minus the digest field reproduces it.
	reparsed, err := contract.ParseJSON(result.CanonicalBytes())
	if err != nil {
		t.Fatalf("canonical bytes are not JSON: %v", err)
	}
	stripped := contract.NewObject()
	for _, key := range reparsed.(*contract.Object).Keys() {
		if key == "proxy_result_digest" || key == "extensions" {
			continue
		}
		val, _ := reparsed.(*contract.Object).Get(key)
		stripped.Set(key, val)
	}
	roundTrip, err := contract.DigestOf(stripped)
	if err != nil {
		t.Fatalf("stripped core digest: %v", err)
	}
	if roundTrip != result.ProxyResultDigest() {
		t.Fatalf("x-digest rule violated over canonical bytes: %s != %s", roundTrip, result.ProxyResultDigest())
	}
}
