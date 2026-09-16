// Package diagnosis implements cut-scoped diagnosis authority: capability-
// bounded private reads (Q53=A), oneOf annotation targets, mask-respecting
// aggregation, and partitioned disclosure with a verifiable audit chain.
package diagnosis

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/domain"
)

// ErrDiagnosisNotImplemented is kept as the red-stage detector for the PG-22
// contract tests; no green-path code returns it.
var ErrDiagnosisNotImplemented = errors.New("diagnosis not implemented")

var (
	ErrCapabilityUnknown          = errors.New("capability was not issued by this service")
	ErrCapabilityExpired          = errors.New("capability expired before the private read")
	ErrCapabilityNotReadOnly      = errors.New("capability operations outside the read-only catalog")
	ErrCapabilityScopeViolation   = errors.New("capability scope outside the frozen cut manifest")
	ErrAnnotationTargetInvalid    = errors.New("annotation must bind exactly one of call_id or (segment_id, assistant_turn_seq)")
	ErrAnnotationUnknown          = errors.New("annotation reference does not resolve to a stored annotation")
	ErrAnnotationInvalid          = errors.New("annotation violates the frozen diagnosis_annotation schema")
	ErrAnnotationRevisionConflict = errors.New("annotation revision already stored with different content")
	ErrAggregationPolicyMissing   = errors.New("aggregation requires a policy digest")
	ErrDisclosureDenied           = errors.New("disclosure denied")
)

type CutID = consolidationcut.CutID
type RoomID = consolidationcut.RoomID
type CapabilityID string
type AnnotationID string
type AgentRunID string
type DisclosureLabel string

type CapabilityOperation string

const CapabilityOperationRead CapabilityOperation = "read"

type PrivateSpaceBinding struct {
	SpaceID   domain.SpaceID
	RoomID    RoomID
	RoomEpoch int64
}

type Capability struct {
	CapabilityID         CapabilityID
	TenantID             domain.TenantID
	CutID                CutID
	PrincipalID          domain.PrincipalID
	AgentRunID           *AgentRunID
	BoundPrivateSpaces   []PrivateSpaceBinding
	Operations           []CapabilityOperation
	IssuedAt             time.Time
	ExpiresAt            time.Time
	ReadAuditChainDigest string
}

type AnnotationTarget struct {
	SegmentID        string
	CallID           *string
	AssistantTurnSeq *int64
}

type AnnotationStatus string

const (
	StatusVerified     AnnotationStatus = "verified"
	StatusFailed       AnnotationStatus = "failed"
	StatusInconclusive AnnotationStatus = "inconclusive"
	StatusMissing      AnnotationStatus = "missing"
)

type RewardStatus string

const (
	Rewarded           RewardStatus = "rewarded"
	MaskedMissing      RewardStatus = "masked_missing"
	MaskedInconclusive RewardStatus = "masked_inconclusive"
)

type EvidenceCitation struct {
	BatchID  domain.BatchID
	EventIDs []string
	SpaceID  *domain.SpaceID
}

type Annotation struct {
	AnnotationID            AnnotationID
	Revision                int64
	SupersedesRevision      *int64
	CutID                   CutID
	TenantID                domain.TenantID
	Target                  AnnotationTarget
	Status                  AnnotationStatus
	RewardStatus            *RewardStatus
	EvidenceCitations       []EvidenceCitation
	DisclosureLabels        []DisclosureLabel
	DiagnosisPolicyRevision string
	ExecutionRecipeDigest   string
	CreatedByAgentRunID     *AgentRunID
	CreatedAt               time.Time
}

type PrivateReadAuditEntry struct {
	CapabilityID   CapabilityID
	CutID          CutID
	TenantID       domain.TenantID
	SpaceID        domain.SpaceID
	BatchID        domain.BatchID
	EventIDs       []string
	OccurredAt     time.Time
	PreviousDigest string
	Digest         string
}

type PrivateReadRequest struct {
	Capability Capability
	SpaceID    domain.SpaceID
	BatchID    domain.BatchID
	EventIDs   []string
	ReadAt     time.Time
}

type AnnotationRef struct {
	AnnotationID AnnotationID
	Revision     int64
}

type AggregationRequest struct {
	TenantID       domain.TenantID
	CutID          CutID
	AnnotationRefs []AnnotationRef
	PolicyDigest   string
}

type AggregationResult struct {
	PolicyDigest      string
	SourceAnnotations []AnnotationRef
	MaskedAnnotations []AnnotationRef
	Confidence        string
}

type DisclosureRequest struct {
	Annotation  Annotation
	SourceScope domain.SpaceScope
	TargetScope domain.SpaceScope
	Rationale   string
}

type DisclosureDecision struct {
	Allowed    bool
	ReasonCode string
	Labels     []DisclosureLabel
}

type DisclosureOutput struct {
	PublicRationale  string
	PrivateRationale string
	Citations        []EvidenceCitation
	Decision         DisclosureDecision
	AuditDigest      string
}

// Repository is the consumer-owned persistence seam; the in-memory authority
// below drives the current increment and PG-50A wires durable persistence in
// through this interface.
type Repository interface {
	IssueCapability(context.Context, Capability) (Capability, bool, error)
	AppendPrivateRead(context.Context, PrivateReadAuditEntry) (PrivateReadAuditEntry, bool, error)
	AppendAnnotation(context.Context, Annotation) (Annotation, bool, error)
	Annotation(context.Context, domain.TenantID, AnnotationID, int64) (Annotation, error)
	AnnotationsForCut(context.Context, domain.TenantID, CutID) ([]Annotation, error)
}

type DisclosureGate interface {
	Decide(context.Context, DisclosureRequest) (DisclosureDecision, error)
}

type Aggregator interface {
	Aggregate(context.Context, AggregationRequest) (AggregationResult, error)
}

// auditLink is one disclosure audit-chain record.
type auditLink struct {
	TenantID       domain.TenantID
	CutID          CutID
	Kind           string
	Subject        string
	PreviousDigest string
	Digest         string
	At             time.Time
}

// cutScopeKey identifies the frozen private-space authority set of one cut
// manifest; annotationKey addresses one immutable annotation revision.
type cutScopeKey struct {
	tenant domain.TenantID
	cut    CutID
}

type annotationKey struct {
	tenant   domain.TenantID
	id       AnnotationID
	revision int64
}

// Service is the cut-scoped diagnosis authority. With nil dependencies it
// runs on the built-in in-memory store, default partitioning gate, and
// deterministic policy aggregator; injected adapters replace any of them.
type Service struct {
	repository Repository
	gate       DisclosureGate
	aggregator Aggregator

	mu           sync.Mutex
	cutScopes    map[cutScopeKey][]PrivateSpaceBinding
	capabilities map[CapabilityID]*Capability
	annotations  map[annotationKey]*Annotation
	readChains   map[CapabilityID]string
	auditChain   map[domain.TenantID]string
	now          func() time.Time
}

func NewService(repository Repository, gate DisclosureGate, aggregator Aggregator) *Service {
	return NewServiceWithClock(repository, gate, aggregator, time.Now)
}

// NewServiceWithClock injects the authority clock: capability expiry is
// judged against this clock, never against the caller-supplied read event
// time (which is recorded as provenance only).
func NewServiceWithClock(repository Repository, gate DisclosureGate, aggregator Aggregator, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{
		repository:   repository,
		gate:         gate,
		aggregator:   aggregator,
		cutScopes:    map[cutScopeKey][]PrivateSpaceBinding{},
		capabilities: map[CapabilityID]*Capability{},
		annotations:  map[annotationKey]*Annotation{},
		readChains:   map[CapabilityID]string{},
		auditChain:   map[domain.TenantID]string{},
		now:          now,
	}
}

// RegisterCutScope records the frozen private-space authority set of one cut
// manifest; capabilities can only be issued exactly inside it (Q53=A,
// diagnosis_capability.capability_scope_within_cut).
func (s *Service) RegisterCutScope(tenantID domain.TenantID, cutID CutID, bindings []PrivateSpaceBinding) error {
	if len(bindings) == 0 {
		return fmt.Errorf("%w: cut manifest declares no private spaces", ErrCapabilityScopeViolation)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := cutScopeKey{tenantID, cutID}
	if existing, ok := s.cutScopes[key]; ok {
		if sameBindings(existing, bindings) {
			return nil
		}
		return fmt.Errorf("%w: cut scope already registered with different bindings", ErrCapabilityScopeViolation)
	}
	s.cutScopes[key] = cloneBindings(bindings)
	return nil
}

// IssueCapability validates and registers a capability against the frozen
// diagnosis_capability contract: read-only operations, bounded private
// spaces, expiry after issuance, and an audit-chain anchor.
func (s *Service) IssueCapability(_ context.Context, capability Capability) (Capability, bool, error) {
	if len(capability.Operations) == 0 {
		return Capability{}, false, ErrCapabilityNotReadOnly
	}
	for _, operation := range capability.Operations {
		if operation != CapabilityOperationRead {
			return Capability{}, false, ErrCapabilityNotReadOnly
		}
	}
	if len(capability.BoundPrivateSpaces) == 0 {
		return Capability{}, false, ErrCapabilityScopeViolation
	}
	if !capability.ExpiresAt.After(capability.IssuedAt) {
		return Capability{}, false, ErrCapabilityExpired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Issue-time validity is judged against the authority clock too: a
	// capability whose expiry has already passed at issuance is never
	// registered.
	if !capability.ExpiresAt.After(s.now()) {
		return Capability{}, false, ErrCapabilityExpired
	}
	scope, ok := s.cutScopes[cutScopeKey{capability.TenantID, capability.CutID}]
	if !ok {
		return Capability{}, false, fmt.Errorf("%w: no frozen cut scope registered for %s/%s", ErrCapabilityScopeViolation, capability.TenantID, capability.CutID)
	}
	if !sameBindings(scope, capability.BoundPrivateSpaces) {
		return Capability{}, false, fmt.Errorf("%w: capability bindings do not exactly match the frozen cut manifest", ErrCapabilityScopeViolation)
	}
	stored := cloneCapability(capability)
	if stored.ReadAuditChainDigest == "" {
		stored.ReadAuditChainDigest = "sha256:" + fullDigest(string(stored.TenantID)+"|"+string(stored.CutID)+"|"+string(stored.CapabilityID))
	}
	s.capabilities[stored.CapabilityID] = &stored
	return cloneCapability(stored), true, nil
}

// ReadPrivate enforces the Q53=A scope exactly: the capability must be one
// this service issued, unexpired, and the read space must be inside the
// bound private set; every accepted read extends the per-capability audit
// hash chain anchored at the capability digest.
func (s *Service) ReadPrivate(_ context.Context, req PrivateReadRequest) (PrivateReadAuditEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.capabilities[req.Capability.CapabilityID]
	if !ok || !capabilityMatches(*stored, req.Capability) {
		return PrivateReadAuditEntry{}, ErrCapabilityUnknown
	}
	// Authorization uses the authority clock; req.ReadAt is event provenance
	// and can never resurrect an expired capability.
	if !s.now().Before(stored.ExpiresAt) {
		return PrivateReadAuditEntry{}, ErrCapabilityExpired
	}
	bound := false
	for _, binding := range stored.BoundPrivateSpaces {
		if binding.SpaceID == req.SpaceID {
			bound = true
			break
		}
	}
	if !bound {
		return PrivateReadAuditEntry{}, ErrCapabilityScopeViolation
	}
	previous := s.readChains[stored.CapabilityID]
	if previous == "" {
		previous = stored.ReadAuditChainDigest
	}
	entry := PrivateReadAuditEntry{
		CapabilityID:   stored.CapabilityID,
		CutID:          stored.CutID,
		TenantID:       stored.TenantID,
		SpaceID:        req.SpaceID,
		BatchID:        req.BatchID,
		EventIDs:       append([]string(nil), req.EventIDs...),
		OccurredAt:     req.ReadAt,
		PreviousDigest: previous,
		Digest:         "sha256:" + shortDigest(previous+"|"+string(req.SpaceID)+"|"+string(req.BatchID)+"|"+fmt.Sprint(req.EventIDs)),
	}
	s.readChains[stored.CapabilityID] = entry.Digest
	return entry, nil
}

// AppendAnnotation stores an immutable annotation revision: the target must
// satisfy the diagnosis_annotation oneOf (exactly one of call binding or
// (segment_id, assistant_turn_seq) binding), the frozen schema fields must
// validate, and an existing (id, revision) can never be rewritten — only
// idempotently replayed with identical content.
func (s *Service) AppendAnnotation(_ context.Context, annotation Annotation) (Annotation, bool, error) {
	if err := validateTarget(annotation.Target); err != nil {
		return Annotation{}, false, err
	}
	if annotation.DiagnosisPolicyRevision == "" || annotation.ExecutionRecipeDigest == "" {
		return Annotation{}, false, errors.New("annotation requires the frozen policy revision and recipe digest")
	}
	switch annotation.Status {
	case StatusVerified, StatusFailed, StatusInconclusive, StatusMissing:
	default:
		return Annotation{}, false, fmt.Errorf("%w: unknown status %q", ErrAnnotationInvalid, annotation.Status)
	}
	if annotation.Revision < 1 {
		return Annotation{}, false, fmt.Errorf("%w: revision must be positive", ErrAnnotationInvalid)
	}
	if !digestShape(annotation.ExecutionRecipeDigest) {
		return Annotation{}, false, fmt.Errorf("%w: execution recipe digest is not sha256-shaped", ErrAnnotationInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := annotationKey{annotation.TenantID, annotation.AnnotationID, annotation.Revision}
	if existing, ok := s.annotations[key]; ok {
		if sameAnnotationContent(*existing, annotation) {
			return cloneAnnotation(*existing), true, nil
		}
		return Annotation{}, false, fmt.Errorf("%w: %s@%d", ErrAnnotationRevisionConflict, annotation.AnnotationID, annotation.Revision)
	}
	stored := cloneAnnotation(annotation)
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = s.now()
	}
	s.annotations[key] = &stored
	return cloneAnnotation(stored), false, nil
}

// Aggregate resolves annotation refs and applies the fixed policy: missing
// and inconclusive annotations are masked out of the confidence-bearing
// source set, so they can never produce a high-confidence conclusion.
func (s *Service) Aggregate(ctx context.Context, req AggregationRequest) (AggregationResult, error) {
	if req.PolicyDigest == "" {
		return AggregationResult{}, ErrAggregationPolicyMissing
	}
	if s.aggregator != nil {
		return s.aggregator.Aggregate(ctx, req)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	source := make([]AnnotationRef, 0, len(req.AnnotationRefs))
	masked := make([]AnnotationRef, 0)
	for _, ref := range req.AnnotationRefs {
		annotation, ok := s.annotations[annotationKey{req.TenantID, ref.AnnotationID, ref.Revision}]
		if !ok || annotation.CutID != req.CutID {
			return AggregationResult{}, ErrAnnotationUnknown
		}
		if annotation.Status == StatusMissing || annotation.Status == StatusInconclusive ||
			annotation.RewardStatus != nil && (*annotation.RewardStatus == MaskedMissing || *annotation.RewardStatus == MaskedInconclusive) {
			masked = append(masked, ref)
			continue
		}
		source = append(source, ref)
	}
	confidence := "low"
	if len(masked) == 0 && len(source) > 0 {
		confidence = "high"
	}
	return AggregationResult{
		PolicyDigest:      req.PolicyDigest,
		SourceAnnotations: source,
		MaskedAnnotations: masked,
		Confidence:        confidence,
	}, nil
}

// Disclose partitions a diagnosis output: the public partition carries only
// a derived, non-plaintext rationale plus ID/digest citations; the private
// plaintext never leaves the private partition. The disclosed annotation is
// resolved from the stored immutable revision — caller-carried annotation
// content is never trusted. A configured deterministic gate owns every
// allow/deny; no gate configured fails closed with DISCLOSURE_DENIED, as do
// policies missing their frozen revision/recipe digests.
func (s *Service) Disclose(ctx context.Context, req DisclosureRequest) (DisclosureOutput, error) {
	if req.Annotation.DiagnosisPolicyRevision == "" || req.Annotation.ExecutionRecipeDigest == "" {
		return DisclosureOutput{}, fmt.Errorf("%w: annotation lacks frozen policy provenance", ErrDisclosureDenied)
	}
	if s.gate == nil {
		return DisclosureOutput{}, fmt.Errorf("%w: no disclosure gate configured", ErrDisclosureDenied)
	}
	s.mu.Lock()
	stored, ok := s.annotations[annotationKey{req.Annotation.TenantID, req.Annotation.AnnotationID, req.Annotation.Revision}]
	if !ok {
		s.mu.Unlock()
		return DisclosureOutput{}, ErrAnnotationUnknown
	}
	// The gate sees — and the output partitions — the stored revision's
	// citations and labels, so nothing outside the frozen annotation can
	// slip through the disclosure boundary. The lock is not held across the
	// external gate call.
	source := cloneAnnotation(*stored)
	s.mu.Unlock()
	gateReq := req
	gateReq.Annotation = source
	decision, err := s.gate.Decide(ctx, gateReq)
	if err != nil {
		return DisclosureOutput{}, err
	}
	if !decision.Allowed {
		return DisclosureOutput{}, fmt.Errorf("%w: %s", ErrDisclosureDenied, decision.ReasonCode)
	}
	output := DisclosureOutput{
		// The public rationale is a derived digest reference, never the
		// plaintext; the private partition is not returned across the gate.
		PublicRationale:  "disclosure://" + shortDigest(req.Rationale+"|"+string(source.AnnotationID)+"|"+string(source.TenantID)),
		PrivateRationale: "",
		Citations:        cloneCitations(source.EvidenceCitations),
		Decision:         decision,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.auditChain[source.TenantID]
	link := auditLink{
		TenantID: source.TenantID, CutID: source.CutID, Kind: "disclosure",
		Subject: string(source.AnnotationID), PreviousDigest: previous, At: s.now(),
	}
	link.Digest = "sha256:" + shortDigest(previous+"|disclosure|"+string(source.AnnotationID)+"|"+output.PublicRationale)
	s.auditChain[source.TenantID] = link.Digest
	output.AuditDigest = link.Digest
	return output, nil
}

// AuditChainDigest returns the current disclosure audit-chain head for a
// tenant so external verifiers can check chain membership.
func (s *Service) AuditChainDigest(tenantID domain.TenantID) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.auditChain[tenantID]
}

// LookupAnnotation resolves one immutable annotation revision. Trusted
// server-side adapters (e.g. the cut job's GuardAuthority) use it to verify
// evidence references against the annotations actually on file; unknown or
// cross-tenant refs report false rather than an error so callers fail
// closed.
func (s *Service) LookupAnnotation(_ context.Context, tenantID domain.TenantID, annotationID AnnotationID, revision int64) (Annotation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	annotation, ok := s.annotations[annotationKey{tenantID, annotationID, revision}]
	if !ok {
		return Annotation{}, false
	}
	return cloneAnnotation(*annotation), true
}

func validateTarget(target AnnotationTarget) error {
	hasCall := target.CallID != nil
	hasTurn := target.AssistantTurnSeq != nil
	if target.SegmentID == "" {
		return ErrAnnotationTargetInvalid
	}
	if hasCall == hasTurn {
		return ErrAnnotationTargetInvalid
	}
	return nil
}

// capabilityMatches compares the request capability against the issued one
// without failing on audit-chain anchor normalization.
func capabilityMatches(issued, presented Capability) bool {
	if issued.ReadAuditChainDigest != presented.ReadAuditChainDigest && presented.ReadAuditChainDigest != "" {
		return false
	}
	presented.ReadAuditChainDigest = issued.ReadAuditChainDigest
	return reflect.DeepEqual(issued, presented)
}

// sameBindings is exact multiset equality of private-space bindings: the
// capability scope must equal the frozen cut scope, not merely sit inside it.
func sameBindings(a, b []PrivateSpaceBinding) bool {
	if len(a) != len(b) {
		return false
	}
	left := cloneBindings(a)
	right := cloneBindings(b)
	sort.Slice(left, func(i, j int) bool { return bindingLess(left[i], left[j]) })
	sort.Slice(right, func(i, j int) bool { return bindingLess(right[i], right[j]) })
	return reflect.DeepEqual(left, right)
}

func bindingLess(a, b PrivateSpaceBinding) bool {
	if a.SpaceID != b.SpaceID {
		return a.SpaceID < b.SpaceID
	}
	if a.RoomID != b.RoomID {
		return a.RoomID < b.RoomID
	}
	return a.RoomEpoch < b.RoomEpoch
}

func cloneBindings(bindings []PrivateSpaceBinding) []PrivateSpaceBinding {
	return append([]PrivateSpaceBinding(nil), bindings...)
}

// digestShape enforces the frozen digest contract: "sha256:" plus 64
// lowercase hex characters.
func digestShape(digest string) bool {
	if !strings.HasPrefix(digest, "sha256:") {
		return false
	}
	hexPart := strings.TrimPrefix(digest, "sha256:")
	if len(hexPart) != 64 || hexPart != strings.ToLower(hexPart) {
		return false
	}
	_, err := hex.DecodeString(hexPart)
	return err == nil
}

// sameAnnotationContent compares two revisions ignoring the server-assigned
// creation timestamp, so idempotent replays are recognized while any content
// difference is a revision conflict.
func sameAnnotationContent(a, b Annotation) bool {
	a.CreatedAt, b.CreatedAt = time.Time{}, time.Time{}
	normalize := func(in Annotation) Annotation {
		if len(in.EvidenceCitations) == 0 {
			in.EvidenceCitations = nil
		}
		if len(in.DisclosureLabels) == 0 {
			in.DisclosureLabels = nil
		}
		return in
	}
	return reflect.DeepEqual(normalize(a), normalize(b))
}

func shortDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])[:16]
}

// fullDigest is the contract-shaped digest ("sha256:" + 64 lowercase hex)
// used for durable anchors such as capability audit-chain heads.
func fullDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// auditMAC derives keyed digests where a caller-supplied key is required;
// kept package-local for tombstone/lineage reuse in PG-24 wiring.
func auditMAC(key, input string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(input))
	return hex.EncodeToString(mac.Sum(nil))[:16]
}

func cloneCapability(capability Capability) Capability {
	cloned := capability
	cloned.BoundPrivateSpaces = append([]PrivateSpaceBinding(nil), capability.BoundPrivateSpaces...)
	cloned.Operations = append([]CapabilityOperation(nil), capability.Operations...)
	if capability.AgentRunID != nil {
		agentRun := *capability.AgentRunID
		cloned.AgentRunID = &agentRun
	}
	return cloned
}

func cloneAnnotation(annotation Annotation) Annotation {
	cloned := annotation
	cloned.EvidenceCitations = cloneCitations(annotation.EvidenceCitations)
	cloned.DisclosureLabels = append([]DisclosureLabel(nil), annotation.DisclosureLabels...)
	if annotation.SupersedesRevision != nil {
		revision := *annotation.SupersedesRevision
		cloned.SupersedesRevision = &revision
	}
	if annotation.RewardStatus != nil {
		status := *annotation.RewardStatus
		cloned.RewardStatus = &status
	}
	if annotation.CreatedByAgentRunID != nil {
		agentRun := *annotation.CreatedByAgentRunID
		cloned.CreatedByAgentRunID = &agentRun
	}
	if annotation.Target.CallID != nil {
		callID := *annotation.Target.CallID
		cloned.Target.CallID = &callID
	}
	if annotation.Target.AssistantTurnSeq != nil {
		seq := *annotation.Target.AssistantTurnSeq
		cloned.Target.AssistantTurnSeq = &seq
	}
	return cloned
}

func cloneCitations(citations []EvidenceCitation) []EvidenceCitation {
	cloned := make([]EvidenceCitation, len(citations))
	for index, citation := range citations {
		cloned[index] = citation
		cloned[index].EventIDs = append([]string(nil), citation.EventIDs...)
		if citation.SpaceID != nil {
			spaceID := *citation.SpaceID
			cloned[index].SpaceID = &spaceID
		}
	}
	return cloned
}
