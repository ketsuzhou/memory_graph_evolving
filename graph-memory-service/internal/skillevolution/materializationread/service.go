// Package materializationread is the GMS-207 exact materialization closure
// read (Contract §5.3.6/§13.7, GMS §8.1/§11.1/§12.7–§12.8/§12.14, RSIH §4.2).
//
// The read is the authoritative S8/MT6 materialization INPUT service: from
// the authoritative active heads (the activation ledger, never the Graph —
// GMS §8.1) and the content-addressed artifact store it walks the exact root
// and its transitive explicit dependencies / Composite exact children and
// returns the frozen canonical bytes, ports, permissions, the freeze
// activation sequence and a torn-read token. It is read-only: GMS writes no
// manifest, no lock, no bundle (RSIH's materializer owns those, Contract
// §6.2.1). The read never waits for the Graph watermark to catch up; the
// watermark rides along as information only.
//
// Fail-closed order (every case returns a closed-registry reason code and
// NO partial closure):
//
//  1. root shape: candidate root → CANDIDATE_NOT_EXECUTABLE; latest / naked
//     name / Graph node form → NON_EXACT_REF (Contract §13.7: naked
//     name/latest/Graph node execution MUST fail closed);
//  2. root authority: no active head → NO_ACTIVE_HEAD; requested ref is not
//     the current head → SKILL_NOT_CURRENT_ACTIVE;
//  3. walk: unresolved/tampered content → DIGEST_MISMATCH; unusable body →
//     ARTIFACT_BODY_INVALID; ref/body kind drift → SKILL_KIND_INVALID;
//     unknown required extension / permission cap / composite body gates →
//     their closed codes (via the GMS-202 gate); child not the current
//     active head → COMPOSITE_CHILD_NOT_ACTIVE; child without port schemas →
//     PORT_SCHEMA_MISSING; cross-lineage cycle → DEPENDENCY_CYCLE; node
//     budget → BUDGET_EXCEEDED;
//  4. freeze: the activation head moved between the pre-read baseline and
//     the post-walk double-check → ACTIVATION_SEQUENCE_CONFLICT (torn read;
//     no guess, no closure).
package materializationread

import (
	"context"
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// DefaultMaxNodes is the default closure node budget (protects the read from
// unbounded graphs; requests may lower it, never raise it above the service
// cap).
const DefaultMaxNodes = 256

// Closed reason codes of the closure gates (frozen system registry, GMS
// §11.4 groups; codes already frozen elsewhere are aliased, never
// redefined).
const (
	// ReasonCandidateNotExecutable: a CandidateArtifactRef was requested as
	// materialization root — candidates never enter Runtime (GMS §11.4).
	ReasonCandidateNotExecutable = "CANDIDATE_NOT_EXECUTABLE"
	// ReasonSkillNotCurrentActive: the requested root is not the lineage's
	// current authoritative active head (stale/superseded).
	ReasonSkillNotCurrentActive = "SKILL_NOT_CURRENT_ACTIVE"
	// ReasonCompositeChildNotActive: a Composite child ref does not resolve
	// to the child lineage's current active head.
	ReasonCompositeChildNotActive = "COMPOSITE_CHILD_NOT_ACTIVE"
	// ReasonDependencyCycle: the transitive dependency walk revisits a node
	// on the active path (loops may only exist as bounded retries).
	ReasonDependencyCycle = "DEPENDENCY_CYCLE"
	// ReasonActivationSequenceConflict: the activation head moved between
	// the read's baseline and its double-check (torn read).
	ReasonActivationSequenceConflict = ledger.ReasonActivationSequenceConflict
	// ReasonBudgetExceeded: the closure exceeded its node budget.
	ReasonBudgetExceeded = "BUDGET_EXCEEDED"

	// Aliases frozen in other layers (single source).
	ReasonNonExactRef              = contract.ReasonNonExactRef
	ReasonNoActiveHead             = activationNoActiveHead
	ReasonDigestMismatch           = contract.ReasonDigestMismatch
	ReasonRefMismatch              = contract.ReasonRefMismatch
	ReasonPortSchemaMissing        = contract.ReasonPortSchemaMissing
	ReasonPermissionCapExceeded    = contract.ReasonPermissionCapExceeded
	ReasonUnknownRequiredExtension = contract.ReasonUnknownRequiredExtension
	ReasonArtifactBodyInvalid      = "ARTIFACT_BODY_INVALID"
	ReasonSkillKindInvalid         = "SKILL_KIND_INVALID"
)

// activationNoActiveHead names the GMS §11.4 activation-group code; it is
// declared once to avoid importing the activation package for a constant.
const activationNoActiveHead = "NO_ACTIVE_HEAD"

// Error is the registry-code closure failure; Detail is diagnostic only and
// never drives behavior (GMS §11.5).
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "materializationread: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err ("" when nil). Artifact-gate
// and validation codes surface unchanged; unknown failures carry no code and
// the transport maps them to INTERNAL.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce.ReasonCode
	}
	if code := artifact.CodeOf(err); code != "" {
		return code
	}
	if code := ledger.ReasonOf(err); code != "" {
		return code
	}
	var se *contract.SchemaError
	if errors.As(err, &se) {
		return se.ReasonCode
	}
	return ""
}

// ---------------------------------------------------------------------------
// Ports (the authoritative sources; satisfied by GMS-204/GMS-102 products)
// ---------------------------------------------------------------------------

// ActiveHeads resolves the authoritative active head of one lineage by
// replaying the activation ledger (satisfied by *activation.ActivationService).
// nil means the lineage has no active revision.
type ActiveHeads interface {
	ActiveRevision(ctx context.Context, lineageID string) (*contract.SkillArtifactRef, error)
}

// ArtifactBytes is the content-addressed artifact store seam (satisfied by
// ledger.Store's ContentStore).
type ArtifactBytes interface {
	Get(digest string) ([]byte, bool, error)
}

// ArtifactGate re-validates one canonical artifact envelope (satisfied by the
// GMS-202 *artifact.Service: envelope shape, integer-only JCS digest, the
// extension whitelist gate, the permission cap, kind bodies, composite
// ports/DAG/retry/fallback).
type ArtifactGate interface {
	Canonicalize(doc any) (*artifact.CanonicalArtifact, error)
}

// ActivationStream reads the single global activation stream head (satisfied
// by ledger.Store): the freeze sequence authority.
type ActivationStream interface {
	Head(kind ledger.LedgerKind, stream string) (ledger.Entry, bool, error)
}

// WatermarkSource supplies the then-visible Contract §7.14 watermark as
// INFORMATION (GMS §11.1). ok=false or a nil source omits the field; the
// read never blocks on it. GMS-205's projector is the production source.
type WatermarkSource interface {
	ProjectionWatermark(ctx context.Context) (doc map[string]any, ok bool)
}

// Config wires the closure read service.
type Config struct {
	Heads       ActiveHeads
	Artifacts   ArtifactBytes
	Gate        ArtifactGate
	Activations ActivationStream
	// Registry verifies every reason code this service can emit against the
	// digest-verified closed system registry (Contract §13.7.1 R5).
	Registry ledger.ReasonRegistry
	// Watermark is the optional informational §7.14 source.
	Watermark WatermarkSource
	// MaxNodes caps the closure budget; <=0 means DefaultMaxNodes.
	MaxNodes int
}

// Service is the exact materialization closure read.
type Service struct {
	heads       ActiveHeads
	artifacts   ArtifactBytes
	gate        ArtifactGate
	activations ActivationStream
	watermark   WatermarkSource
	maxNodes    int
}

// NewService fails closed at construction unless every reason code the
// service can emit exists in the digest-verified registry (Contract
// §13.7.1 R5).
func NewService(cfg Config) (*Service, error) {
	if cfg.Heads == nil || cfg.Artifacts == nil || cfg.Gate == nil || cfg.Activations == nil || cfg.Registry == nil {
		return nil, errors.New("materializationread: nil dependency in closure config")
	}
	for _, code := range []string{
		ReasonCandidateNotExecutable, ReasonSkillNotCurrentActive,
		ReasonCompositeChildNotActive, ReasonDependencyCycle,
		ReasonActivationSequenceConflict, ReasonBudgetExceeded,
		ReasonNonExactRef, ReasonNoActiveHead, ReasonDigestMismatch,
		ReasonRefMismatch, ReasonPortSchemaMissing, ReasonPermissionCapExceeded,
		ReasonUnknownRequiredExtension, ReasonArtifactBodyInvalid,
		ReasonSkillKindInvalid,
	} {
		if err := cfg.Registry.Verify(code); err != nil {
			return nil, fmt.Errorf("materializationread: %w", err)
		}
	}
	maxNodes := cfg.MaxNodes
	if maxNodes <= 0 {
		maxNodes = DefaultMaxNodes
	}
	return &Service{
		heads:       cfg.Heads,
		artifacts:   cfg.Artifacts,
		gate:        cfg.Gate,
		activations: cfg.Activations,
		watermark:   cfg.Watermark,
		maxNodes:    maxNodes,
	}, nil
}

// ReadRequest is one closure read: the exact roots (each MUST be the current
// authoritative active head of its lineage) and an optional per-request node
// budget (never above the service cap).
type ReadRequest struct {
	Roots    []contract.SkillArtifactRef
	MaxNodes int
}

// Read walks the exact closure from the authoritative active heads. The walk
// is deterministic: roots and dependency edges are visited in the canonical
// §6.3 order, so the ordered closure depends only on the ledger content —
// never on response arrival, map iteration or filesystem order. Same
// activation sequence over the same roots yields the same response
// (root/sequence-stable idempotency, GMS §12.7).
func (s *Service) Read(ctx context.Context, req ReadRequest) (*Closure, error) {
	if len(req.Roots) == 0 {
		return nil, newError(ReasonRefMismatch, "a closure read requires at least one exact root ref")
	}
	// Duplicate roots violate exact-set semantics (GMS §12.7: roots are an
	// exact set, not a multiset).
	seenRoots := make(map[string]bool, len(req.Roots))
	for _, ref := range req.Roots {
		if seenRoots[refKey(ref)] {
			return nil, newError(ReasonRefMismatch, "duplicate root ref %s v%s", ref.LineageID, ref.Version)
		}
		seenRoots[refKey(ref)] = true
	}
	maxNodes := s.maxNodes
	if req.MaxNodes > 0 && req.MaxNodes < maxNodes {
		maxNodes = req.MaxNodes
	}

	// Freeze baseline: the activation head the whole read is checked against.
	baseline, err := s.activationHead()
	if err != nil {
		return nil, err
	}

	// Root authority: every root must be the lineage's CURRENT active head
	// (canonical order so a multi-root failure is deterministic).
	roots := make([]contract.SkillArtifactRef, len(req.Roots))
	copy(roots, req.Roots)
	SortRefs(roots)
	for _, root := range roots {
		active, err := s.heads.ActiveRevision(ctx, root.LineageID)
		if err != nil {
			return nil, err
		}
		if active == nil {
			return nil, newError(ReasonNoActiveHead,
				"root lineage %s has no active revision (deactivated or never activated); materialization inputs require a current active head", root.LineageID)
		}
		if *active != root {
			return nil, newError(ReasonSkillNotCurrentActive,
				"root %s v%s (%s) is not the current active head %s v%s (%s) of its lineage (GMS §12.7 precondition)",
				root.LineageID, root.Version, root.ArtifactDigest,
				active.LineageID, active.Version, active.ArtifactDigest)
		}
	}

	// Walk.
	w := &walker{service: s, ctx: ctx, maxNodes: maxNodes, visiting: map[string]bool{}, visited: map[string]*ClosureNode{}}
	for _, root := range roots {
		node, err := w.walk(root, false)
		if err != nil {
			return nil, err
		}
		_ = node
	}
	if err := w.permissionUnion(); err != nil {
		return nil, err
	}

	// Double-check: the activation head must be exactly the baseline — a
	// head that moved between the baseline read and now tore this read.
	after, err := s.activationHead()
	if err != nil {
		return nil, err
	}
	if after.sequence != baseline.sequence || after.headDigest != baseline.headDigest {
		return nil, newError(ReasonActivationSequenceConflict,
			"torn closure read: activation head moved from (sequence %d, %s) to (sequence %d, %s) during the walk; re-read with a fresh baseline instead of guessing (GMS §12.7)",
			baseline.sequence, baseline.headDigest, after.sequence, after.headDigest)
	}

	token, err := TornReadToken(baseline.sequence, baseline.headDigest, roots, w.order)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "torn-read token preimage: %v", err)
	}
	closure := &Closure{
		SchemaVersion:        SchemaClosureRead,
		Roots:                roots,
		Nodes:                w.order,
		ActivationSequence:   baseline.sequence,
		ActivationHeadDigest: baseline.headDigest,
		TornReadToken:        token,
	}
	if s.watermark != nil {
		if doc, ok := s.watermark.ProjectionWatermark(ctx); ok && doc != nil {
			closure.Watermark = doc
		}
	}
	return closure, nil
}

// ParseRootRef parses one requested root from its raw wire object before any
// authority lookup, failing closed on the non-exact forms (Contract §13.7):
// a CandidateArtifactRef is exact in its own domain but never Runtime
// executable; latest versions, naked names and Graph node forms are not
// exact refs at all.
func ParseRootRef(obj map[string]any) (contract.SkillArtifactRef, error) {
	if obj == nil {
		return contract.SkillArtifactRef{}, newError(ReasonRefMismatch, "root ref must be a JSON object")
	}
	if _, ok := obj["candidate_id"]; ok {
		return contract.SkillArtifactRef{}, newError(ReasonCandidateNotExecutable,
			"a CandidateArtifactRef is never a materialization root: candidates carry no released version and MUST NOT be executed by the runtime (Contract §7.4)")
	}
	for _, alias := range []string{"graph_node_id", "node_id"} {
		if _, ok := obj[alias]; ok {
			return contract.SkillArtifactRef{}, newError(ReasonNonExactRef,
				"root ref carries %q: Graph node forms are not exact refs and the Graph is never materialization authority (GMS §8.1)", alias)
		}
	}
	if raw, ok := obj["version"]; ok && !contract.IsIntegerNumber(raw) {
		return contract.SkillArtifactRef{}, newError(ReasonNonExactRef,
			"root version %v is not an exact integer (latest/naked versions are not exact refs)", raw)
	}
	if _, hasLineage := obj["lineage_id"]; hasLineage {
		if _, hasVersion := obj["version"]; !hasVersion {
			return contract.SkillArtifactRef{}, newError(ReasonNonExactRef,
				"naked lineage name without an exact version is not an exact ref (Contract §6.2)")
		}
		if _, hasDigest := obj["artifact_digest"]; !hasDigest {
			return contract.SkillArtifactRef{}, newError(ReasonNonExactRef,
				"lineage ref without artifact_digest is not an exact ref (Contract §6.2)")
		}
	}
	for _, naked := range []string{"skill_id", "name", "id"} {
		if _, ok := obj[naked]; ok {
			if _, hasLineage := obj["lineage_id"]; !hasLineage {
				return contract.SkillArtifactRef{}, newError(ReasonNonExactRef,
					"naked %q is not an exact ref: materialization requires (lineage_id, version, artifact_digest)", naked)
			}
		}
	}
	if _, ok := obj["lineage_id"]; !ok {
		return contract.SkillArtifactRef{}, newError(ReasonNonExactRef,
			"root ref carries no lineage_id: materialization requires an exact §7.3 ref")
	}
	return contract.ParseSkillArtifactRef(obj)
}

// activationHead reads the global activation stream head (sequence, payload
// digest). An empty stream means no activation has ever committed: there is
// no active head to freeze from.
type headMark struct {
	sequence   uint64
	headDigest string
}

func (s *Service) activationHead() (headMark, error) {
	entry, ok, err := s.activations.Head(ledger.LedgerActivation, "")
	if err != nil {
		return headMark{}, err
	}
	if !ok || entry.Sequence == 0 {
		return headMark{}, newError(ReasonNoActiveHead,
			"the global activation stream is empty: no active head exists to materialize from")
	}
	return headMark{sequence: entry.Sequence, headDigest: entry.PayloadDigest}, nil
}

// ---------------------------------------------------------------------------
// Closure walker (deterministic DFS; the GMS-207 refactor seam)
// ---------------------------------------------------------------------------

// walker carries one Read's walk state. The emitted order is DFS preorder
// from the canonically sorted roots with children visited in canonical
// order; shared subtrees are emitted once (visited map) and the visiting
// stack detects cycles (DEPENDENCY_CYCLE).
type walker struct {
	service  *Service
	ctx      context.Context
	maxNodes int
	visiting map[string]bool
	visited  map[string]*ClosureNode
	order    []ClosureNode
}

// walk resolves one node and recurses into its explicit dependencies.
// asChild marks nodes reached as Composite children (they must declare both
// port schemas, Contract §8.2/§8.3: ports are REQUIRED when used as a
// Composite child).
func (w *walker) walk(ref contract.SkillArtifactRef, asChild bool) (*ClosureNode, error) {
	key := refKey(ref)
	if w.visiting[key] {
		return nil, newError(ReasonDependencyCycle,
			"closure walk revisits %s v%s on the active path; loops may only exist as bounded retries (Contract §8.4)", ref.LineageID, ref.Version)
	}
	if node, done := w.visited[key]; done {
		// Shared subtree: ports requirement still applies to every
		// child-typed edge, even when the node was first reached as a root.
		if asChild && (node.InputPort == nil || node.OutputPort == nil) {
			return nil, newError(ReasonPortSchemaMissing,
				"child %s v%s declares no input/output port schemas (required for every Composite child)", ref.LineageID, ref.Version)
		}
		return node, nil
	}
	w.visiting[key] = true
	defer delete(w.visiting, key)

	node, err := w.resolve(ref)
	if err != nil {
		return nil, err
	}
	if asChild && (node.InputPort == nil || node.OutputPort == nil) {
		return nil, newError(ReasonPortSchemaMissing,
			"child %s v%s declares no input/output port schemas (required for every Composite child)", ref.LineageID, ref.Version)
	}

	// Explicit dependencies: Composite exact children (the only explicit
	// skill-ref edges of the canonical artifact model, Contract §8.4/§9.5).
	deps, err := w.dependencies(node)
	if err != nil {
		return nil, err
	}
	node.Dependencies = deps

	w.visited[key] = node
	w.order = append(w.order, *node)
	if len(w.order) > w.maxNodes {
		return nil, newError(ReasonBudgetExceeded,
			"closure exceeded its node budget of %d (GMS §12.7: bounded closure traversal)", w.maxNodes)
	}

	for _, dep := range deps {
		// Every dependency must itself be the lineage's CURRENT active head:
		// the release-time activation condition re-verified at read time
		// (Contract §8.4 "active at Composite release", GMS §12.8).
		active, err := w.service.heads.ActiveRevision(w.ctx, dep.LineageID)
		if err != nil {
			return nil, err
		}
		if active == nil || *active != dep {
			return nil, newError(ReasonCompositeChildNotActive,
				"dependency %s v%s (%s) is not the current active head of its lineage (active=%v); the closure must resolve every child/dependency exactly and currently",
				dep.LineageID, dep.Version, dep.ArtifactDigest, active)
		}
		if _, err := w.walk(dep, true); err != nil {
			return nil, err
		}
	}
	return node, nil
}

// resolve fetches and re-validates one node's canonical artifact from the
// content-addressed store. The stored bytes must digest to the ref's exact
// artifact_digest and re-canonicalize to the same digest through the GMS-202
// gate (defense in depth: store address == declared digest == recomputed
// canonical digest).
func (w *walker) resolve(ref contract.SkillArtifactRef) (*ClosureNode, error) {
	stored, ok, err := w.service.artifacts.Get(ref.ArtifactDigest)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newError(ReasonDigestMismatch,
			"artifact %s v%s content %s resolves to no committed bytes (torn store)", ref.LineageID, ref.Version, ref.ArtifactDigest)
	}
	if digest := contract.DigestBytes(stored); digest != ref.ArtifactDigest {
		return nil, newError(ReasonDigestMismatch,
			"stored bytes under %s digest to %s (same address, different bytes)", ref.ArtifactDigest, digest)
	}
	value, err := contract.ParseJSONStrict(stored)
	if err != nil {
		return nil, newError(ReasonArtifactBodyInvalid, "artifact %s is not valid JSON: %v", ref.ArtifactDigest, err)
	}
	obj, isObj := contract.AsObject(value)
	if !isObj {
		return nil, newError(ReasonArtifactBodyInvalid, "artifact %s is not a JSON object", ref.ArtifactDigest)
	}
	canonical, err := w.service.gate.Canonicalize(value)
	if err != nil {
		code := CodeOf(err)
		if code == "" {
			code = ReasonArtifactBodyInvalid
		}
		return nil, &Error{ReasonCode: code, Detail: fmt.Sprintf("artifact %s failed the canonical gate: %v", ref.ArtifactDigest, err)}
	}
	if canonical.BodyDigest() != ref.ArtifactDigest {
		return nil, newError(ReasonDigestMismatch,
			"re-canonicalized body digests to %s but the ref pins %s (stale or drifted ref)", canonical.BodyDigest(), ref.ArtifactDigest)
	}
	if canonical.Kind() != ref.Kind {
		return nil, newError(ReasonSkillKindInvalid,
			"ref kind %s disagrees with the canonical body kind %s of %s v%s", ref.Kind, canonical.Kind(), ref.LineageID, ref.Version)
	}
	body := canonical.Body()
	inputPort, err := portRefOf(body["input_port_schema"])
	if err != nil {
		return nil, newError(ReasonPortSchemaMissing, "%s v%s input_port_schema: %v", ref.LineageID, ref.Version, err)
	}
	outputPort, err := portRefOf(body["output_port_schema"])
	if err != nil {
		return nil, newError(ReasonPortSchemaMissing, "%s v%s output_port_schema: %v", ref.LineageID, ref.Version, err)
	}
	bytesCopy := make([]byte, len(stored))
	copy(bytesCopy, stored)
	_ = obj
	return &ClosureNode{
		Ref:            ref,
		Kind:           canonical.Kind(),
		ArtifactDigest: ref.ArtifactDigest,
		CanonicalBytes: bytesCopy,
		InputPort:      inputPort,
		OutputPort:     outputPort,
		Permissions:    permissionsOf(canonicalEnvelopeOf(value)),
	}, nil
}

// canonicalEnvelopeOf returns the envelope object of a parsed artifact (the
// permissions live on the envelope, not the body).
func canonicalEnvelopeOf(value any) map[string]any {
	obj, _ := contract.AsObject(value)
	return obj
}

// dependencies extracts and canonically orders the explicit dependency refs
// of one resolved node (Composite children only in the canonical v1 model).
func (w *walker) dependencies(node *ClosureNode) ([]contract.SkillArtifactRef, error) {
	if node.Kind != "composite" {
		return nil, nil
	}
	// The packet was frozen by the gate; re-read the children from the
	// canonical body view for the strict exact-ref parse (NON_EXACT_REF for
	// latest/Graph forms inside a committed body).
	body := w.bodyOf(node)
	if body == nil {
		return nil, nil
	}
	children, _ := contract.AsArray(body["children"])
	deps := make([]contract.SkillArtifactRef, 0, len(children))
	for i, raw := range children {
		child, _ := contract.AsObject(raw)
		refRaw, _ := contract.AsObject(child["skill_ref"])
		if refRaw == nil {
			return nil, newError(ReasonArtifactBodyInvalid, "composite child %d carries no skill_ref", i)
		}
		ref, err := ParseRootRef(refRaw)
		if err != nil {
			return nil, err
		}
		deps = append(deps, ref)
	}
	SortRefs(deps)
	return deps, nil
}

// bodyOf re-parses a node's stored bytes to its kind body (the gate hands
// out deep copies; a fresh strict parse keeps the walker independent).
func (w *walker) bodyOf(node *ClosureNode) map[string]any {
	value, err := contract.ParseJSONStrict(node.CanonicalBytes)
	if err != nil {
		return nil
	}
	obj, _ := contract.AsObject(value)
	if obj == nil {
		return nil
	}
	body, _ := contract.AsObject(obj["body"])
	return body
}

// permissionUnion enforces the closure-level permission computation
// (Contract §14.2): the union of every node's envelope permissions plus
// composite orchestration permissions stays under the Host v1 cap.
func (w *walker) permissionUnion() error {
	union := map[string]bool{}
	for _, node := range w.order {
		for _, permission := range node.Permissions {
			union[permission.Capability] = true
		}
		body := w.bodyOf(&node)
		if entries, ok := contract.AsArray(body["orchestration_permissions"]); ok {
			for _, raw := range entries {
				entry, _ := contract.AsObject(raw)
				if capability, ok := contract.AsString(entry["capability"]); ok {
					union[capability] = true
				}
			}
		}
	}
	capabilities := make([]string, 0, len(union))
	for capability := range union {
		capabilities = append(capabilities, capability)
	}
	sortStrings(capabilities)
	for _, capability := range capabilities {
		if !validation.HostCapabilityCapV1[capability] {
			return newError(ReasonPermissionCapExceeded,
				"closure permission union exceeds the Host v1 cap at capability %q (Contract §14.2)", capability)
		}
	}
	return nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
