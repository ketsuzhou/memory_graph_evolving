// Package toolproxy implements the Host local Tool Proxy core (HST-201):
// the same-call bridge that resolves Contract §7.17 ToolProxyRequests into
// exact §7.18 ToolProxyResults while the Pi tool future is still open
// (Host Spec §5.4, Contract §12.7, §15 S2).
//
// Layering:
//
//	Service — proxy core: request validation, CTR-002 success matrix,
//	          §13.7.1 reason precedence, idempotency lease, timeout/cancel.
//	Bridge  — Pi wiring: builds the request from Host-authoritative state,
//	          invokes the Service before tool completion, delivers the exact
//	          canonical result bytes into the Pi return channel exactly once.
//	AuditStore — authoritative audit: request digests, terminal results under
//	          compare-and-swap, and late-upstream entries that can never
//	          rewrite a terminal result (§13.7.1 R3-4, Host §5.8).
package toolproxy

import (
	"sync"
	"time"
)

// Audit entry kinds, mirroring the Host Spec §5.10 tool audit chain:
//
//	tool_call_event -> proxy_request_audit -> upstream_completion_or_failure
//	-> tool_result_event -> Delivery terminal event
//
// Only the exact ToolProxyResult a Pi call actually received may be recorded
// as a tool result; late or side-channel upstream responses use a different
// audit category (AuditLateUpstream) and never connect as tool results.
const (
	// AuditProxyRequest records one accepted ToolProxyRequest (request audit
	// and idempotency binding).
	AuditProxyRequest = "proxy_request_audit"
	// AuditTerminal records the CAS'd terminal ToolProxyResult of one proxy
	// call (the tool_result_event payload digest).
	AuditTerminal = "tool_result_event"
	// AuditDelivery records the delivery of the terminal result into the Pi
	// return channel (Host §5.4/§5.5.6: delivered digest == Pi-visible digest).
	AuditDelivery = "pi_return_delivery"
	// AuditLateUpstream records an upstream response that arrived after the
	// terminal result was already fixed: audit-only, never behavioral.
	AuditLateUpstream = "late_upstream_audit"
)

// Terminal CAS outcomes (Host §5.7/§5.8).
const (
	CASFirstTerminal       = "first_terminal"
	CASReplayedSaved       = "replayed_saved_terminal"
	CASConflictObserved    = "idempotency_conflict_observed"
	CASLateOnly            = "late_only"
	CASDuplicateSuppressed = "duplicate_delivery_suppressed"
	CASDeliveryFailed      = "pi_return_channel_failed"
)

// AuditEntry is one immutable audit record. Free-text detail is diagnostic
// only; every behavioral field is a digest or a closed enum value.
type AuditEntry struct {
	Kind           string
	ProxyRequestID string
	LeaseKey       string
	ToolName       string
	RequestDigest  string
	ResultDigest   string
	UpstreamDigest string
	Status         string // terminal status for AuditTerminal entries
	CASOutcome     string
	ReasonCode     string // terminal reason code for failed/inconclusive results
	Delivered      bool   // AuditDelivery: bytes reached the Pi return channel
	Detail         map[string]string
	RecordedAt     time.Time
}

// Terminal is the CAS-fixed terminal state of one proxy call. Once committed,
// only its exact canonical bytes may ever be delivered as that call's result.
type Terminal struct {
	ProxyRequestID string
	ResultDigest   string
	Status         string
	ReasonCode     string
	Canonical      []byte
}

// AuditStore is the in-process authoritative audit for proxy calls. It is the
// Host-side record Contract §15 S2 checks against: the digest Pi observed must
// equal the digest recorded here, exactly once.
type AuditStore struct {
	mu       sync.Mutex
	entries  []AuditEntry
	terminal map[string]Terminal
}

// NewAuditStore returns an empty audit store.
func NewAuditStore() *AuditStore {
	return &AuditStore{terminal: make(map[string]Terminal)}
}

// Append records one entry verbatim.
func (a *AuditStore) Append(entry AuditEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if entry.RecordedAt.IsZero() {
		entry.RecordedAt = time.Now().UTC()
	}
	a.entries = append(a.entries, entry)
}

// CommitTerminal performs the terminal compare-and-swap: the first terminal
// result for a proxy_request_id wins; every later commit attempt returns the
// already-fixed terminal with ok=false and changes nothing (§13.7.1 R3-4).
func (a *AuditStore) CommitTerminal(candidate Terminal) (Terminal, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if existing, ok := a.terminal[candidate.ProxyRequestID]; ok {
		return existing, false
	}
	a.terminal[candidate.ProxyRequestID] = candidate
	return candidate, true
}

// Terminal returns the fixed terminal state of one proxy call.
func (a *AuditStore) Terminal(proxyRequestID string) (Terminal, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.terminal[proxyRequestID]
	return t, ok
}

// Entries returns a snapshot of every audit entry in record order.
func (a *AuditStore) Entries() []AuditEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]AuditEntry(nil), a.entries...)
}
