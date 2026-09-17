package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"unicode/utf8"
)

// Schema version constants of the shared DTOs referenced by this adapter
// (Contract §7).
const (
	SchemaSkillArtifactRef     = "gms.skill-artifact-ref.v2"
	SchemaCandidateArtifactRef = "gms.candidate-artifact-ref.v2"
	SchemaEvidenceRef          = "gms.evidence-ref.v1"
	SchemaSimilarityAssessment = "gms.similarity-assessment.v1"
	SchemaMergeProposalEvent   = "gms.merge-proposal-event.v1"
)

// bigInt / newBigInt / parseBigInt keep integer values exact (no float64
// round-trip), so versions and sequences beyond 2^53 compare correctly.
type bigInt = big.Int

func newBigInt(v int64) *bigInt { return big.NewInt(v) }

func parseBigInt(v any) (*bigInt, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return nil, false
	}
	z, ok := new(big.Int).SetString(string(n), 10)
	return z, ok
}

// digestPattern is the Contract §7.2 digest form.
var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// AsObject reports whether v is a JSON object in the decoder model.
func AsObject(v any) (map[string]any, bool) {
	obj, ok := v.(map[string]any)
	return obj, ok
}

// AsArray reports whether v is a JSON array in the decoder model.
func AsArray(v any) ([]any, bool) {
	arr, ok := v.([]any)
	return arr, ok
}

// AsString reports whether v is a JSON string.
func AsString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// IsIntegerNumber reports whether v is a JSON number in plain integer form
// (no fraction, no exponent). Booleans are not integers here, mirroring the
// reference validator's _is_int.
func IsIntegerNumber(v any) bool {
	n, ok := v.(json.Number)
	return ok && integerForm.MatchString(string(n))
}

// IterDicts walks the value tree and calls fn for every JSON object, parent
// first. Map iteration order inside one object is unspecified in Go; the
// semantic checks only classify per-check reason codes, so their outcome does
// not depend on traversal order (the ORDER BETWEEN checks is what decides the
// code when several violations coexist, and that order is fixed).
func IterDicts(v any, fn func(map[string]any)) {
	switch t := v.(type) {
	case map[string]any:
		fn(t)
		for _, item := range t {
			IterDicts(item, fn)
		}
	case []any:
		for _, item := range t {
			IterDicts(item, fn)
		}
	}
}

// CanonicalKey returns the JCS encoding of v, used as an exact-equality /
// grouping key for JSON values (Python-side set and dict semantics).
func CanonicalKey(v any) string {
	data, err := JCS(v)
	if err != nil {
		// Values reaching semantic checks have already canonicalized
		// successfully; keep a deterministic fallback rather than panicking.
		return fmt.Sprintf("\x00uncanonicalizable:%v", v)
	}
	return string(data)
}

// EqualJSON reports deep JSON equality of two decoder-model values.
func EqualJSON(a, b any) bool { return CanonicalKey(a) == CanonicalKey(b) }

// ParseJSONStrict parses data as a single UTF-8 JSON document using
// json.Number for numbers (exact digits, no float64) and rejects invalid
// UTF-8, trailing data and everything else Python's json.loads would reject
// before canonicalization.
func ParseJSONStrict(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("source is not valid UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	// json.loads rejects extra values after the document ("Extra data").
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after JSON value")
	}
	return v, nil
}

// ---------------------------------------------------------------------------
// Strict exact-ref DTO surface (Contract §7.2/§7.3/§7.4/§7.7)
// ---------------------------------------------------------------------------

// SchemaError is a fail-closed DTO parse error carrying a closed-registry
// reason code from the system reason registry (GMS §11.4).
type SchemaError struct {
	ReasonCode string
	Detail     string
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("contract: %s: %s", e.ReasonCode, e.Detail)
}

// Closed-registry codes used by the strict parsers (GMS §11.4
// schema/canonical/ref group).
const (
	ReasonSchemaVersionUnsupported  = "SCHEMA_VERSION_UNSUPPORTED"
	ReasonSchemaFieldUnknown        = "SCHEMA_FIELD_UNKNOWN"
	ReasonSchemaRequiredFieldMissin = "SCHEMA_REQUIRED_FIELD_MISSING"
	ReasonSchemaEnumInvalid         = "SCHEMA_ENUM_INVALID"
)

// legacySkillArtifactRefAliases are field names of the frozen legacy tracer
// wire model (internal/domain/skill_artifact.go) that MUST NOT be accepted as
// aliases by the shared-contract parsers: accepting them would let old wire
// objects slip into the new digest domain unnoticed.
var legacySkillArtifactRefAliases = []string{"skill_id", "artifact_id", "digest", "body_sha", "parents"}

var skillKinds = map[string]bool{
	"human_procedure": true,
	"step_guidance":   true,
	"composite":       true,
	"tool":            true,
}

// SkillArtifactRef is the Contract §7.3 exact ref: identity is the triple
// (LineageID, Version, ArtifactDigest). Version keeps its exact decimal
// digits.
type SkillArtifactRef struct {
	SchemaVersion  string
	LineageID      string
	Version        string
	Kind           string
	ArtifactDigest string
}

// VersionInt returns the exact integer value of Version.
func (r SkillArtifactRef) VersionInt() (*bigInt, bool) {
	return new(big.Int).SetString(r.Version, 10)
}

// ParseSkillArtifactRef parses one §7.3 ref object with a closed field set.
// Unknown fields, legacy aliases, non-integer versions and enum violations
// fail closed.
func ParseSkillArtifactRef(obj map[string]any) (SkillArtifactRef, error) {
	closed := map[string]bool{
		"schema_version": true, "lineage_id": true, "version": true,
		"kind": true, "artifact_digest": true,
	}
	if err := rejectUnknownFields(SchemaSkillArtifactRef, obj, closed, legacySkillArtifactRefAliases); err != nil {
		return SkillArtifactRef{}, err
	}
	if sv, _ := AsString(obj["schema_version"]); sv != SchemaSkillArtifactRef {
		return SkillArtifactRef{}, &SchemaError{ReasonSchemaVersionUnsupported, fmt.Sprintf("schema_version %q is not %q", sv, SchemaSkillArtifactRef)}
	}
	lineage, err := requireNonEmptyString(SchemaSkillArtifactRef, obj, "lineage_id")
	if err != nil {
		return SkillArtifactRef{}, err
	}
	version, err := requireVersionField(obj)
	if err != nil {
		return SkillArtifactRef{}, err
	}
	kind, err := requireEnumField(obj, "kind", skillKinds)
	if err != nil {
		return SkillArtifactRef{}, err
	}
	digest, err := requireDigestField(obj, "artifact_digest")
	if err != nil {
		return SkillArtifactRef{}, err
	}
	return SkillArtifactRef{
		SchemaVersion:  SchemaSkillArtifactRef,
		LineageID:      lineage,
		Version:        version,
		Kind:           kind,
		ArtifactDigest: digest,
	}, nil
}

// VersionedRef is the Contract §7.2 common shape (id, version, digest).
type VersionedRef struct {
	ID      string
	Version string
	Digest  string
}

// ParseVersionedRef parses one §7.2 ref with a closed field set.
func ParseVersionedRef(obj map[string]any) (VersionedRef, error) {
	closed := map[string]bool{"id": true, "version": true, "digest": true}
	if err := rejectUnknownFields("versioned-ref", obj, closed, nil); err != nil {
		return VersionedRef{}, err
	}
	id, err := requireNonEmptyString("versioned-ref", obj, "id")
	if err != nil {
		return VersionedRef{}, err
	}
	version, err := requireVersionField(obj)
	if err != nil {
		return VersionedRef{}, err
	}
	digest, err := requireDigestField(obj, "digest")
	if err != nil {
		return VersionedRef{}, err
	}
	return VersionedRef{ID: id, Version: version, Digest: digest}, nil
}

// CandidateArtifactRef is the Contract §7.4 exact ref: identity is the pair
// (CandidateID, BodyDigest). It intentionally carries no released version and
// must never be executed by the runtime.
type CandidateArtifactRef struct {
	SchemaVersion string
	CandidateID   string
	Kind          string
	BodyDigest    string
	OriginType    string
	OriginRef     VersionedRef
}

var originTypes = map[string]bool{
	"skill_proposal": true,
	"merge_proposal": true,
}

// ParseCandidateArtifactRef parses one §7.4 ref with a closed field set.
func ParseCandidateArtifactRef(obj map[string]any) (CandidateArtifactRef, error) {
	closed := map[string]bool{
		"schema_version": true, "candidate_id": true, "kind": true,
		"body_digest": true, "origin_type": true, "origin_ref": true,
	}
	if err := rejectUnknownFields(SchemaCandidateArtifactRef, obj, closed, legacySkillArtifactRefAliases); err != nil {
		return CandidateArtifactRef{}, err
	}
	if sv, _ := AsString(obj["schema_version"]); sv != SchemaCandidateArtifactRef {
		return CandidateArtifactRef{}, &SchemaError{ReasonSchemaVersionUnsupported, fmt.Sprintf("schema_version %q is not %q", sv, SchemaCandidateArtifactRef)}
	}
	id, err := requireNonEmptyString(SchemaCandidateArtifactRef, obj, "candidate_id")
	if err != nil {
		return CandidateArtifactRef{}, err
	}
	kind, err := requireEnumField(obj, "kind", skillKinds)
	if err != nil {
		return CandidateArtifactRef{}, err
	}
	bodyDigest, err := requireDigestField(obj, "body_digest")
	if err != nil {
		return CandidateArtifactRef{}, err
	}
	originType, err := requireEnumField(obj, "origin_type", originTypes)
	if err != nil {
		return CandidateArtifactRef{}, err
	}
	originRaw, ok := AsObject(obj["origin_ref"])
	if !ok {
		return CandidateArtifactRef{}, &SchemaError{ReasonSchemaRequiredFieldMissin, "origin_ref must be an object"}
	}
	origin, err := ParseVersionedRef(originRaw)
	if err != nil {
		return CandidateArtifactRef{}, err
	}
	return CandidateArtifactRef{
		SchemaVersion: SchemaCandidateArtifactRef,
		CandidateID:   id,
		Kind:          kind,
		BodyDigest:    bodyDigest,
		OriginType:    originType,
		OriginRef:     origin,
	}, nil
}

// EvidenceRef is the Contract §7.7 exact ref (evidence_id, version,
// evidence_digest) plus commit state and kind.
type EvidenceRef struct {
	SchemaVersion    string
	EvidenceID       string
	Version          string
	EvidenceDigest   string
	CommitState      string
	EvidenceKind     string
	SourceSegmentRef *VersionedRef
}

var commitStates = map[string]bool{"committed": true, "sealed": true}

var evidenceKinds = map[string]bool{
	"success_path": true, "failure_path": true, "recovery_path": true,
	"observation": true, "replay_result": true, "human_attestation": true,
}

// ParseEvidenceRef parses one §7.7 ref with a closed field set.
func ParseEvidenceRef(obj map[string]any) (EvidenceRef, error) {
	closed := map[string]bool{
		"schema_version": true, "evidence_id": true, "version": true,
		"evidence_digest": true, "commit_state": true, "evidence_kind": true,
		"source_segment_ref": true,
	}
	if err := rejectUnknownFields(SchemaEvidenceRef, obj, closed, nil); err != nil {
		return EvidenceRef{}, err
	}
	if sv, _ := AsString(obj["schema_version"]); sv != SchemaEvidenceRef {
		return EvidenceRef{}, &SchemaError{ReasonSchemaVersionUnsupported, fmt.Sprintf("schema_version %q is not %q", sv, SchemaEvidenceRef)}
	}
	id, err := requireNonEmptyString(SchemaEvidenceRef, obj, "evidence_id")
	if err != nil {
		return EvidenceRef{}, err
	}
	version, err := requireVersionField(obj)
	if err != nil {
		return EvidenceRef{}, err
	}
	digest, err := requireDigestField(obj, "evidence_digest")
	if err != nil {
		return EvidenceRef{}, err
	}
	commit, err := requireEnumField(obj, "commit_state", commitStates)
	if err != nil {
		return EvidenceRef{}, err
	}
	kind, err := requireEnumField(obj, "evidence_kind", evidenceKinds)
	if err != nil {
		return EvidenceRef{}, err
	}
	var segment *VersionedRef
	if raw, present := obj["source_segment_ref"]; present {
		segRaw, ok := AsObject(raw)
		if !ok {
			return EvidenceRef{}, &SchemaError{ReasonSchemaEnumInvalid, "source_segment_ref must be an object"}
		}
		seg, err := ParseVersionedRef(segRaw)
		if err != nil {
			return EvidenceRef{}, err
		}
		segment = &seg
	}
	return EvidenceRef{
		SchemaVersion:    SchemaEvidenceRef,
		EvidenceID:       id,
		Version:          version,
		EvidenceDigest:   digest,
		CommitState:      commit,
		EvidenceKind:     kind,
		SourceSegmentRef: segment,
	}, nil
}

// --- strict parsing helpers -------------------------------------------------

func rejectUnknownFields(where string, obj map[string]any, closed map[string]bool, legacyAliases []string) error {
	for _, alias := range legacyAliases {
		if _, ok := obj[alias]; ok {
			return &SchemaError{ReasonSchemaFieldUnknown, fmt.Sprintf("%s: field %q is a legacy wire-model alias; the shared contract accepts no alias, convert explicitly", where, alias)}
		}
	}
	for key := range obj {
		if !closed[key] {
			return &SchemaError{ReasonSchemaFieldUnknown, fmt.Sprintf("%s: unknown field %q outside the closed set", where, key)}
		}
	}
	return nil
}

func requireNonEmptyString(where string, obj map[string]any, field string) (string, error) {
	raw, present := obj[field]
	if !present {
		return "", &SchemaError{ReasonSchemaRequiredFieldMissin, fmt.Sprintf("%s: missing required field %q", where, field)}
	}
	s, ok := AsString(raw)
	if !ok || s == "" {
		return "", &SchemaError{ReasonSchemaEnumInvalid, fmt.Sprintf("%s: field %q must be a non-empty string", where, field)}
	}
	return s, nil
}

func requireVersionField(obj map[string]any) (string, error) {
	raw, present := obj["version"]
	if !present {
		return "", &SchemaError{ReasonSchemaRequiredFieldMissin, "missing required field \"version\""}
	}
	if !IsIntegerNumber(raw) {
		return "", &SchemaError{ReasonSchemaEnumInvalid, fmt.Sprintf("field \"version\" must be an integer, got %v", raw)}
	}
	z, ok := parseBigInt(raw)
	if !ok || z.Cmp(newBigInt(1)) < 0 {
		return "", &SchemaError{ReasonSchemaEnumInvalid, "field \"version\" must be an integer >= 1"}
	}
	return string(raw.(json.Number)), nil
}

func requireEnumField(obj map[string]any, field string, allowed map[string]bool) (string, error) {
	raw, present := obj[field]
	if !present {
		return "", &SchemaError{ReasonSchemaRequiredFieldMissin, fmt.Sprintf("missing required field %q", field)}
	}
	s, ok := AsString(raw)
	if !ok || !allowed[s] {
		return "", &SchemaError{ReasonSchemaEnumInvalid, fmt.Sprintf("field %q value %v outside the closed enum", field, raw)}
	}
	return s, nil
}

func requireDigestField(obj map[string]any, field string) (string, error) {
	raw, present := obj[field]
	if !present {
		return "", &SchemaError{ReasonSchemaRequiredFieldMissin, fmt.Sprintf("missing required field %q", field)}
	}
	s, ok := AsString(raw)
	if !ok || !digestPattern.MatchString(s) {
		return "", &SchemaError{ReasonSchemaEnumInvalid, fmt.Sprintf("field %q must match sha256:<64 lowercase hex>", field)}
	}
	return s, nil
}
