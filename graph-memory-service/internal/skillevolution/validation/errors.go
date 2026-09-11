// Package validation is the shared fail-closed static-gate library of the
// GMS-202 artifact/proposal/candidate pipeline.
//
// Authority: the CTR-005 machine-readable conformance schemas under
// $FIX/schema/shared (DTO shapes, x-digest preimages, closed vocabularies)
// and $FIX/schema/state (state-machine transition tables). This package
// never hand-codes a DTO shape or a transition: it loads the frozen schema
// documents and interprets them, mirroring the closed validation semantics
// of $FIX/validate_contract.py (validate_schema_node / validate_instance /
// check_event_sequence). Reason codes emitted here are the closed set of
// that validator plus the shared contract codes frozen in internal/contract.
//
// The package is deliberately pure (internal/contract is the only import):
// artifact, proposal and candidate services layer their registry-verified
// gate errors on top of it.
package validation

// Error is the closed-code validation failure.
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string { return "validation: " + e.Code + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{Code: code, Detail: sprintf(format, args...)}
}

// CodeOf returns the closed code of err, or "" when err is nil or not a
// validation error.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if ve, ok := err.(*Error); ok {
		return ve.Code
	}
	return ""
}

// Closed code set: the validate_contract.py ContractCheckError codes plus
// the shared contract reason codes reused verbatim (single source in
// internal/contract for the latter).
const (
	CodeSchemaVersionUnsupported   = "SCHEMA_VERSION_UNSUPPORTED"
	CodeSchemaFieldUnknown         = "SCHEMA_FIELD_UNKNOWN"
	CodeSchemaRequiredFieldMissing = "SCHEMA_REQUIRED_FIELD_MISSING"
	CodeSchemaEnumInvalid          = "SCHEMA_ENUM_INVALID"
	CodeSchemaTypeInvalid          = "SCHEMA_TYPE_INVALID"
	CodeSchemaConstMismatch        = "SCHEMA_CONST_MISMATCH"
	CodeSchemaPatternInvalid       = "SCHEMA_PATTERN_INVALID"
	CodeSchemaMinimumViolated      = "SCHEMA_MINIMUM_VIOLATED"
	CodeSchemaMinItemsViolated     = "SCHEMA_MINITEMS_VIOLATED"
	CodeSchemaConditionalRequired  = "SCHEMA_CONDITIONAL_REQUIRED_MISSING"
	CodeSchemaConditionalForbidden = "SCHEMA_CONDITIONAL_FORBIDDEN_PRESENT"
	CodeSchemaRefCycle             = "SCHEMA_REF_CYCLE"
	CodeSchemaKeywordUnknown       = "SCHEMA_KEYWORD_UNKNOWN"
	CodeNonIntegerNumber           = "NON_INTEGER_NUMBER"
	CodeDigestMismatch             = "DIGEST_MISMATCH"
	CodeUnknownRequiredExtension   = "UNKNOWN_REQUIRED_EXTENSION"
	CodeIllegalStateTransition     = "ILLEGAL_STATE_TRANSITION"
	CodeUndefinedState             = "UNDEFINED_STATE"
	CodeTerminalStateReopen        = "TERMINAL_STATE_REOPEN"
	CodePermissionCapExceeded      = "PERMISSION_CAP_EXCEEDED"
	CodeNonExactRef                = "NON_EXACT_REF"
	CodeFixtureExpectationMismatch = "FIXTURE_EXPECTATION_MISMATCH"
)
