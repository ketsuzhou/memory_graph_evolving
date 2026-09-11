// Package replay is the GMS-203 paired-replay canonicalization boundary
// (GMS §5.1–§5.3, Contract §7.10–§7.11, §10.4–§10.5).
//
// GMS freezes the Contract §7.10 ReplayRequest (validated against the
// CTR-005 authority schema), verifies the Host/RSIH correlation of every
// observed packet run against the frozen plan, and mints the authoritative
// Contract §7.11 ReplayResult: per-family fixture outcomes with exact
// baseline refs, integer counts, paired output digests, the five-dimension
// integer utility vector (candidate side) and the reference-envelope vector
// (baseline side, Contract §10.4), digested with SHA-256 over JCS.
//
// Fail-closed order (nothing may masquerade as success, GMS §5.3):
//
//  1. the request parses strictly against the shared schema (SCHEMA_* /
//     conditional required_source_heads for merge candidates);
//  2. the frozen family plan is closed (families, domains, sides, packet
//     expectations, envelope baselines ⊆ request baselines);
//  3. every observed run correlates to a planned packet on the declared
//     side, with the exact adapter/profile refs of the request and the
//     artifact ref of that side — a baseline/candidate side swap or a
//     foreign artifact rejects (REPLAY_REQUEST_INVALID / REF_MISMATCH);
//  4. deterministic packets must reproduce their expected output digest
//     (DIGEST_MISMATCH, bytes re-hashed when supplied);
//  5. two runs of the same packet+side with different digests are
//     nondeterministic: the result canonicalizes as status=inconclusive
//     with REPLAY_NONDETERMINISTIC and the packet never counts as a pass;
//  6. infra failures are separated from semantic outcomes by the frozen
//     reason registry (retryable+same_request ⟺ infra, Contract §13.7.1
//     R4): an infra-faulted case is an inconclusive case, never a
//     semantic pass and never a critical-regression attribution;
//  7. every aggregation uses checked int64 arithmetic; any overflow
//     canonicalizes as status=inconclusive with
//     UTILITY_ARITHMETIC_OVERFLOW.
//
// The package is a pure canonicalizer: it neither schedules Host runs nor
// writes ledgers; the decision evaluator (internal/skillevolution/evaluator)
// re-derives every count from the recorded classifications before trusting
// them.
package replay

import (
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Schema file names of the paired-replay authority shapes.
const (
	SchemaReplayRequest = "replay-request.schema.json"
	SchemaReplayResult  = "replay-result.schema.json"
)

// Closed reason codes of the canonicalizer (frozen system registry,
// GMS §11.4 replay/evaluation/release group plus the shared contract codes).
const (
	ReasonReplayRequestInvalid      = "REPLAY_REQUEST_INVALID"
	ReasonReplayResultInvalid       = "REPLAY_RESULT_INVALID"
	ReasonMissingFamily             = "MISSING_FAMILY"
	ReasonReplayNondeterministic    = "REPLAY_NONDETERMINISTIC"
	ReasonReplayInconclusive        = "REPLAY_INCONCLUSIVE"
	ReasonUtilityArithmeticOverflow = "UTILITY_ARITHMETIC_OVERFLOW"
)

// Aliases of codes frozen in internal/contract (single source).
const (
	ReasonDigestMismatch = contract.ReasonDigestMismatch
	ReasonRefMismatch    = contract.ReasonRefMismatch
)

// ReplayResult status values (Contract §7.11).
const (
	StatusSucceeded    = "succeeded"
	StatusFailed       = "failed"
	StatusInconclusive = "inconclusive"
)

// Paired-run sides (GMS §5.2).
const (
	SideBaseline  = "baseline"
	SideCandidate = "candidate"
)

// Fixture domains (Contract §7.11 fixture_outcomes[].domain).
const (
	DomainSourceA = "source_a"
	DomainSourceB = "source_b"
	DomainOverlap = "overlap"
	DomainGeneral = "general"
)

// Run classification categories. Only CategorySemanticPass counts toward
// any *_passed count or utility success dimension.
const (
	CategorySemanticPass     = "semantic_pass"
	CategorySemanticFail     = "semantic_fail"
	CategoryInfra            = "infra"
	CategoryNondeterministic = "nondeterministic"
)

// Error is the registry-code canonicalization failure; Detail is
// diagnostic only and never drives behavior.
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "replay: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err, or "" when err is nil or
// not a replay error. Validation-layer codes surface with their own closed
// codes (single registry).
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if re, ok := err.(*Error); ok {
		return re.ReasonCode
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	return ""
}

// Service is the protected ReplayResult canonicalizer.
type Service struct {
	gates  *validation.Gates
	policy *contract.ReasonPolicy
}

// NewService wires the canonicalizer to the shared validation gates and the
// digest-verified reason registry (the infra/semantic classification is
// registry-driven, Contract §13.7.1 R4).
func NewService(gates *validation.Gates, policy *contract.ReasonPolicy) (*Service, error) {
	if gates == nil {
		return nil, fmt.Errorf("replay: nil validation gates")
	}
	if policy == nil {
		return nil, fmt.Errorf("replay: nil reason policy (infra/semantic classification is registry-driven)")
	}
	return &Service{gates: gates, policy: policy}, nil
}

// isInfraCode reports whether code is an infra failure per the frozen
// registry: retryable with retry_scope=same_request (the Contract §13.7.1
// R4 infra whitelist, e.g. FAKE_TOOL_NO_RESPONSE). Everything else —
// including unknown codes — is treated as a semantic terminal failure.
func (s *Service) isInfraCode(code string) bool {
	info, err := s.policy.Lookup(code)
	if err != nil {
		return false
	}
	return info.Retryable && info.RetryScope == "same_request"
}

// closedDomains is the §7.11 fixture domain enum.
var closedDomains = map[string]bool{
	DomainSourceA: true, DomainSourceB: true, DomainOverlap: true, DomainGeneral: true,
}

var closedSides = map[string]bool{SideBaseline: true, SideCandidate: true}
