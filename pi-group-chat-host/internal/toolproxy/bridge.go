package toolproxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// ReturnChannel is the same-call Pi return path: the exact canonical
// ToolProxyResult bytes for one tool call must enter this channel while the
// Pi tool future is still open (Host §5.4). Production wiring backs this with
// the Host HTTP endpoint the Pi room-bridge extension calls from execute().
type ReturnChannel interface {
	Deliver(ctx context.Context, toolCallID string, canonical []byte) error
}

// Call identifies one Pi tool call entering the proxy plane. Arguments are
// untrusted data (Contract §14.3): they ride the request as the `arguments`
// object and are never a source of authority.
type Call struct {
	ToolCallID string
	ToolName   string
	Arguments  contract.Value // *contract.Object; defaults to {}
}

// RequestContext carries the Host-authoritative ToolProxyRequest fields
// (Host §5.3): resolved from durable room/delivery state and profiles, never
// from model arguments.
type RequestContext struct {
	RoomID                         string
	AgentID                        string
	DeliveryID                     string
	ProxyRequestID                 string
	ScopeProfileRef                contract.Value // exact VersionedRef
	RequestedMinActivationSequence *int64
	TimeoutMillis                  int64
}

// Delivery is the outcome of one same-call proxy invocation.
type Delivery struct {
	ToolCallID    string
	Result        *Result
	Canonical     []byte
	Digest        string
	FirstDelivery bool
}

// Bridge connects the proxy core to the Pi tool-call path: it builds the
// ToolProxyRequest from Host-authoritative state, invokes the Service
// synchronously while the tool future is open, and delivers the exact
// canonical result into the Pi return channel exactly once per tool call.
// A tool call's terminal delivery is fixed on first delivery; every later
// attempt for the same call ID is audit-only (no second Pi-visible result).
type Bridge struct {
	service *Service
	channel ReturnChannel

	mu        sync.Mutex
	delivered map[string]Delivery
}

// NewBridge wires a service to a Pi return channel.
func NewBridge(service *Service, channel ReturnChannel) (*Bridge, error) {
	if service == nil {
		return nil, fmt.Errorf("toolproxy: bridge requires a service")
	}
	if channel == nil {
		return nil, fmt.Errorf("toolproxy: bridge requires a return channel")
	}
	return &Bridge{service: service, channel: channel, delivered: map[string]Delivery{}}, nil
}

// IsProxyTool reports whether a tool name belongs to the closed proxy tool
// set (Contract §7.17 v1: memory_explore, memory_expand, skill_get).
func (b *Bridge) IsProxyTool(tool string) bool {
	return b.service.policy.IsProxyTool(tool)
}

// Audit exposes the authoritative audit store behind the bridge.
func (b *Bridge) Audit() *AuditStore { return b.service.audit }

// ProxyTools lists the closed proxy tool set in registry order.
func (b *Bridge) ProxyTools() []string {
	return append([]string(nil), b.service.policy.ToolOrder...)
}

// InvokeBeforeCompletion runs one same-call proxy invocation: the request is
// built from Host-authoritative state, executed to its exact terminal result,
// and delivered into the Pi return channel before the tool future completes.
// The returned Delivery always carries a complete Result — failures included
// travel the same channel (Host §5.4).
func (b *Bridge) InvokeBeforeCompletion(ctx context.Context, call Call, rc RequestContext) (*Delivery, error) {
	request, err := buildRequest(call, rc)
	if err != nil {
		return nil, err
	}
	result := b.service.Invoke(ctx, request)

	b.mu.Lock()
	if existing, ok := b.delivered[call.ToolCallID]; ok {
		// Terminal delivery already fixed for this call: exactly-once wins.
		// The late outcome is audit-only and never reaches Pi again.
		b.mu.Unlock()
		b.service.audit.Append(AuditEntry{
			Kind:           AuditDelivery,
			ProxyRequestID: proxyRequestIDOf(request),
			ToolName:       call.ToolName,
			ResultDigest:   existing.Digest,
			CASOutcome:     CASDuplicateSuppressed,
			Detail:         map[string]string{"tool_call_id": call.ToolCallID},
			RecordedAt:     time.Now().UTC(),
		})
		existing.FirstDelivery = false
		return &existing, nil
	}
	b.mu.Unlock()

	if err := b.channel.Deliver(ctx, call.ToolCallID, result.CanonicalBytes()); err != nil {
		// The return channel failed: the exact result could not reach Pi.
		// Record the failure; the call's terminal stays what the Service
		// fixed. PI_RETURN_CHANNEL_FAILED is itself exact and closed.
		b.service.audit.Append(AuditEntry{
			Kind:           AuditDelivery,
			ProxyRequestID: proxyRequestIDOf(request),
			ToolName:       call.ToolName,
			ResultDigest:   result.ProxyResultDigest(),
			CASOutcome:     CASDeliveryFailed,
			Delivered:      false,
			ReasonCode:     CodeReturnChannelFailed,
			Detail:         map[string]string{"tool_call_id": call.ToolCallID, "error": err.Error()},
			RecordedAt:     time.Now().UTC(),
		})
		return &Delivery{
			ToolCallID:    call.ToolCallID,
			Result:        result,
			Canonical:     result.CanonicalBytes(),
			Digest:        result.ProxyResultDigest(),
			FirstDelivery: false,
		}, fmt.Errorf("%w: deliver tool call %s: %v", ErrReturnChannel, call.ToolCallID, err)
	}

	delivery := Delivery{
		ToolCallID:    call.ToolCallID,
		Result:        result,
		Canonical:     result.CanonicalBytes(),
		Digest:        result.ProxyResultDigest(),
		FirstDelivery: true,
	}
	b.mu.Lock()
	b.delivered[call.ToolCallID] = delivery
	b.mu.Unlock()
	b.service.audit.Append(AuditEntry{
		Kind:           AuditDelivery,
		ProxyRequestID: proxyRequestIDOf(request),
		ToolName:       call.ToolName,
		ResultDigest:   delivery.Digest,
		Status:         result.Status(),
		ReasonCode:     result.ReasonCode(),
		CASOutcome:     CASFirstTerminal,
		Delivered:      true,
		Detail:         map[string]string{"tool_call_id": call.ToolCallID},
		RecordedAt:     time.Now().UTC(),
	})
	return &delivery, nil
}

// ErrReturnChannel reports a Pi return channel delivery failure.
var ErrReturnChannel = fmt.Errorf("toolproxy: pi return channel failed")

// buildRequest constructs the Contract §7.17 ToolProxyRequest. Every
// authority-bearing field comes from the RequestContext; the idempotency key
// is the canonical digest of the call identity, so a replay of the same tool
// call reuses the same key (Host §5.7).
func buildRequest(call Call, rc RequestContext) (contract.Value, error) {
	if rc.ProxyRequestID == "" || rc.RoomID == "" || rc.AgentID == "" || rc.DeliveryID == "" {
		return nil, fmt.Errorf("toolproxy: request context requires proxy_request_id, room_id, agent_id and delivery_id")
	}
	if _, ok := rc.ScopeProfileRef.(*contract.Object); !ok {
		return nil, fmt.Errorf("toolproxy: request context requires an exact scope_profile_ref object")
	}
	arguments, ok := call.Arguments.(*contract.Object)
	if call.Arguments != nil && !ok {
		return nil, fmt.Errorf("toolproxy: tool arguments must be a JSON object")
	}
	if arguments == nil {
		arguments = contract.NewObject()
	}
	timeout := rc.TimeoutMillis
	if timeout < 1 {
		timeout = DefaultTimeoutMillis
	}
	idempotencyPreimage := contract.NewObject()
	idempotencyPreimage.Set("tool_call_id", contract.String(call.ToolCallID))
	idempotencyPreimage.Set("tool_name", contract.String(call.ToolName))
	idempotencyPreimage.Set("room_id", contract.String(rc.RoomID))
	idempotencyPreimage.Set("agent_id", contract.String(rc.AgentID))
	idempotencyPreimage.Set("delivery_id", contract.String(rc.DeliveryID))
	idempotencyPreimage.Set("arguments", arguments)
	idempotencyKey, err := contract.DigestOf(idempotencyPreimage)
	if err != nil {
		return nil, fmt.Errorf("toolproxy: idempotency preimage not canonicalizable: %w", err)
	}
	request := contract.NewObject()
	request.Set("schema_version", contract.String(RequestSchemaVersion))
	request.Set("proxy_request_id", contract.String(rc.ProxyRequestID))
	request.Set("room_id", contract.String(rc.RoomID))
	request.Set("agent_id", contract.String(rc.AgentID))
	request.Set("delivery_id", contract.String(rc.DeliveryID))
	request.Set("tool_name", contract.String(call.ToolName))
	request.Set("arguments", arguments)
	request.Set("scope_profile_ref", rc.ScopeProfileRef)
	request.Set("idempotency_key", contract.String(idempotencyKey))
	if rc.RequestedMinActivationSequence != nil {
		request.Set("requested_min_activation_sequence", contract.Number(fmt.Sprintf("%d", *rc.RequestedMinActivationSequence)))
	}
	request.Set("timeout_millis", contract.Number(fmt.Sprintf("%d", timeout)))
	return request, nil
}

func proxyRequestIDOf(request contract.Value) string {
	obj, ok := request.(*contract.Object)
	if !ok {
		return ""
	}
	id, _ := contract.StringOf(obj, "proxy_request_id")
	return id
}
