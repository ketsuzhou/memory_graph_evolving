// Package retrieval is the GMS-206 Explore/Guidance/tools service (Contract
// §5.6/§7.14–§7.18/§12.7.1–§12.7.2; GMS §10–§11, §12.6/§12.14; Host §5/§7).
//
// It serves the three v1 Memory tool payloads over the REAL authorities: the
// GMS-205 Runtime projection (the derived read side — never a writer here),
// the GMS-204 active heads, the content-addressed artifact store and the
// GMS-202 canonical gate. Every response is the exact upstream typed payload
// the Host wraps into a Contract §7.18 ToolProxyResult (GMS §10.5); the
// service self-validates each success against the digest-verified
// Contract §12.7.1 tool-success validation matrix BEFORE returning it.
//
// Determinism is structural: candidates are collected into slices, scored
// with integer arithmetic only, and ordered through one frozen comparator
// chain (policy score desc, graph distance asc, exact-ref canonical order);
// no map iteration order ever reaches a response (GMS §10.7).
//
// Fail-closed order (each failure carries a closed-registry reason code and
// returns NO partial result): tool binding / readiness gate → argument
// payload shape → scope & session binding → freshness → budgets → fences →
// retrieval/ranking → budget allocation with typed omissions → rendering →
// matrix self-validation.
package retrieval

import (
	"context"
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Closed reason codes of the retrieval gates (the frozen system registry,
// GMS §11.4 explore/tool group; codes frozen elsewhere are aliased).
const (
	// ReasonToolUnsupported: the tool name is outside the closed v1 set, or
	// the skill_get readiness gate (Contract §12.7.1 M6) is not green.
	ReasonToolUnsupported = "TOOL_UNSUPPORTED"
	// ReasonToolArgumentsInvalid: the tool-specific arguments payload violates
	// its closed GMS §10.2–§10.4 binding.
	ReasonToolArgumentsInvalid = "TOOL_ARGUMENTS_INVALID"
	// ReasonToolResultBindingInvalid: a success payload failed the frozen
	// result binding self-check (never returned to a compliant caller).
	ReasonToolResultBindingInvalid = "TOOL_RESULT_BINDING_INVALID"
	// ReasonExploreSessionInvalid: the ExploreSession binding is missing,
	// unreadable or disagrees with the session's frozen ranker/context.
	ReasonExploreSessionInvalid = "EXPLORE_SESSION_INVALID"
	// ReasonExploreScopeViolation: cross-Room/agent/profile reuse of one
	// ExploreSession (Contract §12.1; the read audit disagrees with the
	// request scope).
	ReasonExploreScopeViolation = "EXPLORE_SCOPE_VIOLATION"
	// ReasonExploreQueryInvalid: the query text violates its policy bound.
	ReasonExploreQueryInvalid = "EXPLORE_QUERY_INVALID"
	// ReasonExploreContinuationInvalid: the source query digest of an
	// expansion does not resolve to a served query of this session.
	ReasonExploreContinuationInvalid = "EXPLORE_CONTINUATION_INVALID"
	// ReasonExploreFilterInvalid: a filter requests non-active visibility or
	// carries an out-of-enum value (GMS §10.2 filters).
	ReasonExploreFilterInvalid = "EXPLORE_FILTER_INVALID"
	// ReasonExploreFenceConflict: served fences do not continue the session
	// exactly, a served ref would repeat, or a new page did not advance the
	// projection watermark (Contract §12.7.2 C3/C6).
	ReasonExploreFenceConflict = "EXPLORE_FENCE_CONFLICT"
	// ReasonExpandTargetNotServed: the expansion target was not legally
	// exposed by the source result (GMS §10.3).
	ReasonExpandTargetNotServed = "EXPAND_TARGET_NOT_SERVED"
	// ReasonExpandSourceViewMismatch: source_view_hash does not match the
	// source GuidanceView.
	ReasonExpandSourceViewMismatch = "EXPAND_SOURCE_VIEW_MISMATCH"
	// ReasonExpandDepthUnsupported: max_graph_depth is not exactly 1 (v1).
	ReasonExpandDepthUnsupported = "EXPAND_DEPTH_UNSUPPORTED"
	// ReasonSkillNotCurrentActive: a current_active skill_get target is not
	// the lineage's authoritative active head.
	ReasonSkillNotCurrentActive = "SKILL_NOT_CURRENT_ACTIVE"
	// ReasonHistoricalReadNotAuthorized: historical_exact visibility requires
	// exact Host profile authorization (GMS §10.4).
	ReasonHistoricalReadNotAuthorized = "HISTORICAL_READ_NOT_AUTHORIZED"
	// ReasonGuidanceRenderFailed: the Guidance renderer could not derive a
	// closed §7.15 view from the canonical artifact.
	ReasonGuidanceRenderFailed = "GUIDANCE_RENDER_FAILED"
	// ReasonGuidanceViewHashMismatch: the rendered view hash does not
	// recompute (self-check; never returned by a healthy renderer).
	ReasonGuidanceViewHashMismatch = "GUIDANCE_VIEW_HASH_MISMATCH"
	// ReasonBudgetInvalid: the request budgets are inconsistent or exceed the
	// service/profile caps (Contract §12.4).
	ReasonBudgetInvalid = "BUDGET_INVALID"
	// ReasonCitationInvalid: a typed citation is missing or mixes identity and
	// evidence citation classes (Contract §12.6).
	ReasonCitationInvalid = "CITATION_INVALID"
	// ReasonProjectionBehindRequiredSequence: the freshness floor was not met.
	ReasonProjectionBehindRequiredSequence = projector.ReasonProjectionBehindRequiredSequence

	// Aliases frozen in other layers (single source).
	ReasonNonExactRef              = contract.ReasonNonExactRef
	ReasonCandidateNotExecutable   = "CANDIDATE_NOT_EXECUTABLE"
	ReasonEvidenceNotCommitted     = contract.ReasonEvidenceNotCommitted
	ReasonDigestMismatch           = contract.ReasonDigestMismatch
	ReasonArtifactBodyInvalid      = "ARTIFACT_BODY_INVALID"
	ReasonSchemaVersionUnsupported = contract.ReasonSchemaVersionUnsupported
	ReasonNoActiveHead             = "NO_ACTIVE_HEAD"
)

// The four truncation success-metadata codes (GMS §11.4; they MUST NEVER
// appear in an error envelope — only in truncation_reason_codes/omissions).
const (
	CodeTotalCapReached            = "TOTAL_CAP_REACHED"
	CodeEvidenceSubcapReached      = "EVIDENCE_SUBCAP_REACHED"
	CodeSkillSubcapReached         = "SKILL_SUBCAP_REACHED"
	CodeGuidanceTokenBudgetReached = "GUIDANCE_TOKEN_BUDGET_REACHED"
)

// Error is the registry-code retrieval failure; Detail is diagnostic only and
// never drives behavior (GMS §11.5).
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "retrieval: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err ("" when nil). Ledger,
// validation and contract-schema codes surface unchanged; codeless failures
// map to the transport's INTERNAL.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	var re *Error
	if errors.As(err, &re) {
		return re.ReasonCode
	}
	if code := ledger.ReasonOf(err); code != "" {
		return code
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	var se *contract.SchemaError
	if errors.As(err, &se) {
		return se.ReasonCode
	}
	return ""
}

// ---------------------------------------------------------------------------
// Ports (the authoritative sources; satisfied by GMS-204/205/102/202 products)
// ---------------------------------------------------------------------------

// RuntimeProjection is the derived Runtime Graph read side plus the
// Contract §7.14 watermark (satisfied by *projector.Service). The projection
// is read-only here: Explore never mutates the Graph or the ledgers
// (GMS §12.6 authoritative writes are session/fence audit only).
type RuntimeProjection interface {
	Snapshot() projector.GraphSnapshot
	Watermark() (doc map[string]any, digest string, state string)
}

// ActiveHeads resolves the authoritative active head of one lineage
// (satisfied by *activation.ActivationService); nil means no active revision.
type ActiveHeads interface {
	ActiveRevision(ctx context.Context, lineageID string) (*contract.SkillArtifactRef, error)
}

// ArtifactBytes is the content-addressed artifact store seam (satisfied by
// ledger.Store's ContentStore).
type ArtifactBytes interface {
	Get(digest string) ([]byte, bool, error)
}

// ArtifactGate re-validates one canonical artifact envelope (satisfied by the
// GMS-202 *artifact.Service).
type ArtifactGate interface {
	Canonicalize(doc any) (*artifact.CanonicalArtifact, error)
}

// EvidenceResolver resolves one committed EvidenceRef by id (the same port
// the projector and candidate services consume; satisfied in production by
// the GMS-201 evidence admission service).
type EvidenceResolver interface {
	GetEvidence(id string) (contract.EvidenceRef, bool, error)
}

// SchemaGate exposes the authority schema helpers (digest preimage over the
// frozen guidance-view x-digest).
type SchemaGate interface {
	ComputeDigestPreimage(instance map[string]any, schemaFile string) (string, error)
	ValidateInstance(instance any, schemaFile string) error
}

var _ SchemaGate = (*validation.Gates)(nil)
