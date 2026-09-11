// Package contract is the GMS adapter for the RSIH skill-evolution shared
// contract (Contract §6–§8, §13.7, §16): integer-only RFC 8785 JCS
// canonicalization, the strict exact-ref DTO surface, the digest-verified
// reason-code registry policy and the S1 golden conformance corpus runner.
//
// Anti-corruption layer (GMS-101):
//   - This package MUST NOT import the legacy wire model in
//     internal/domain (skill_artifact.go / curation.go) or
//     internal/skillproposal. Those packages predate the shared contract and
//     their json.Marshal-based semantic bytes are NOT canonical bytes.
//     Callers convert between the two models explicitly.
//   - The legacy tracer field aliases ("skill_id", "artifact_id", the bare
//     "digest" on skill refs, float versions) are rejected fail-closed by the
//     strict DTO parsers instead of being silently aliased in.
//   - Canonical bytes are produced ONLY by JCS in this file, never by
//     encoding/json Marshal (numbers must keep exact decimal digits, so
//     float64 round-trips are structurally impossible here).
package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"unicode/utf16"
)

// Reason codes used by canonicalization itself (closed registry, Contract
// §6.1.4/§13.7/§16.4 plus the FND-001 extensions adopted by CTR-004).
const (
	ReasonBOMNotAllowed = "BOM_NOT_ALLOWED"
	ReasonInvalidJSON   = "INVALID_JSON"
)

// ReasonNonIntegerNumber marks a JSON number that cannot enter the
// integer-only hashed core (fraction or exponent form, even when
// integer-valued such as 1.0 or 1e2).
const ReasonNonIntegerNumber = "NON_INTEGER_NUMBER"

// bomUTF8 is the UTF-8 byte-order mark rejected by Contract §6.1.1.
var bomUTF8 = []byte{0xEF, 0xBB, 0xBF}

// integerForm matches the JSON grammar productions that denote plain decimal
// integers. json.Number values that fail this pattern fail closed.
var integerForm = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

// CanonicalizationError reports a value that cannot enter the integer-only
// hashed core. ReasonCode is a closed-registry reason code.
type CanonicalizationError struct {
	ReasonCode string
}

func (e *CanonicalizationError) Error() string {
	return "contract: canonicalization failed: " + e.ReasonCode
}

// JCS serializes value into RFC 8785 JSON Canonicalization Scheme bytes with
// the behaviors pinned by Contract §6.1 and the FND-001 reference validator:
//
//   - object keys sorted by UTF-16 code units (not code points, not UTF-8
//     bytes), which differs from code point order for astral keys;
//   - string escaping only for '"', '\' and U+0000..U+001F (short forms
//     \b \t \n \f \r, otherwise \u00xx with lowercase hex); U+007F and all
//     non-ASCII characters are emitted as raw UTF-8, never \u-escaped;
//   - no Unicode normalization (no NFC/NFKC) and no insignificant whitespace;
//   - integer-only numbers: any non-integer JSON number fails closed with
//     NON_INTEGER_NUMBER. Integers keep their exact decimal digits, so values
//     beyond 2^53 (canon-006) survive without big-float precision loss.
//
// The value tree must be the decoder model (nil, bool, json.Number, string,
// []any, map[string]any) as produced by ParseJSONStrict.
func JCS(value any) ([]byte, error) {
	var buf bytes.Buffer
	if err := emit(&buf, value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func emit(buf *bytes.Buffer, node any) error {
	switch v := node.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if v {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case json.Number:
		s := string(v)
		if !integerForm.MatchString(s) {
			return &CanonicalizationError{ReasonCode: ReasonNonIntegerNumber}
		}
		// Python's int model normalizes the JSON "-0" token to 0; mirror it so
		// both runners emit identical canonical bytes.
		if s == "-0" {
			s = "0"
		}
		buf.WriteString(s)
	case string:
		writeEscapedString(buf, v)
	case []any:
		buf.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := emit(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		buf.WriteByte('{')
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return utf16CodeUnitLess(keys[i], keys[j]) })
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeEscapedString(buf, k)
			buf.WriteByte(':')
			if err := emit(buf, v[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case int:
		buf.WriteString(strconv.Itoa(v))
	case int64:
		buf.WriteString(strconv.FormatInt(v, 10))
	case uint64:
		buf.WriteString(strconv.FormatUint(v, 10))
	case float32, float64:
		// A float in the tree is a non-integer number for the hashed core,
		// even if it happens to hold an integral value.
		return &CanonicalizationError{ReasonCode: ReasonNonIntegerNumber}
	default:
		return fmt.Errorf("contract: unsupported JSON value type %T", node)
	}
	return nil
}

// writeEscapedString emits a JSON string with the pinned escape table:
// `"` and `\` escaped, control characters U+0000..U+001F as short forms
// \b \t \n \f \r or \u00xx (lowercase hex); everything else raw UTF-8
// including U+007F (canon-003 pins this byte).
func writeEscapedString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			buf.WriteString("\\\"")
		case r == '\\':
			buf.WriteString("\\\\")
		case r == '\b':
			buf.WriteString("\\b")
		case r == '\t':
			buf.WriteString("\\t")
		case r == '\n':
			buf.WriteString("\\n")
		case r == '\f':
			buf.WriteString("\\f")
		case r == '\r':
			buf.WriteString("\\r")
		case r < 0x20:
			buf.WriteString(fmt.Sprintf("\\u%04x", r))
		default:
			buf.WriteRune(r)
		}
	}
	buf.WriteByte('"')
}

// utf16CodeUnitLess orders strings by UTF-16 code units, which is the RFC
// 8785 key order (equivalent to comparing big-endian UTF-16 bytes). For the
// BMP prefix it equals code point order; astral characters order by their
// high surrogate, below U+E000..U+FFFD characters (canon-002).
func utf16CodeUnitLess(a, b string) bool {
	ua := utf16.Encode([]rune(a))
	ub := utf16.Encode([]rune(b))
	n := len(ua)
	if len(ub) < n {
		n = len(ub)
	}
	for i := 0; i < n; i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

// NormalizeForHashing applies the Contract §6.3 DTO-level normalization that
// runs BEFORE JCS: any object whose "source_skill_refs" is exactly a
// two-element array of objects carrying a string "lineage_id" has that pair
// sorted ascending by (lineage_id UTF-8 bytes, version, artifact_digest).
// This is how SimilarityAssessment A+B / B+A canonicalize to identical bytes
// (§16.4 #11); for a full-value port the sort key details live in
// skillRefPairLess.
func NormalizeForHashing(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = NormalizeForHashing(item)
		}
		if refs, ok := out["source_skill_refs"].([]any); ok && len(refs) == 2 {
			first, ok1 := AsObject(refs[0])
			second, ok2 := AsObject(refs[1])
			if ok1 && ok2 && hasStringKey(first, "lineage_id") && hasStringKey(second, "lineage_id") {
				pair := []any{refs[0], refs[1]}
				sort.SliceStable(pair, func(i, j int) bool {
					pi, _ := AsObject(pair[i])
					pj, _ := AsObject(pair[j])
					return skillRefPairLess(pi, pj)
				})
				out["source_skill_refs"] = pair
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = NormalizeForHashing(item)
		}
		return out
	default:
		return value
	}
}

func hasStringKey(obj map[string]any, key string) bool {
	_, ok := AsString(obj[key])
	return ok
}

// skillRefPairLess compares two normalized source refs by
// (lineage_id UTF-8 bytes, version, artifact_digest), mirroring the reference
// validator's tuple key: a missing version sorts as 0, a missing digest as "".
// Integer versions compare numerically (exact, beyond 2^53); a non-integer
// version is unorderable in the reference model and sorts last, which cannot
// occur in any case that reaches canonicalization successfully.
func skillRefPairLess(a, b map[string]any) bool {
	al, _ := AsString(a["lineage_id"])
	bl, _ := AsString(b["lineage_id"])
	switch {
	case al < bl:
		return true
	case al > bl:
		return false
	}
	av, aInt := refVersionSortKey(a)
	bv, bInt := refVersionSortKey(b)
	if aInt != bInt {
		return bInt // integers first; a non-integer only sorts after
	}
	if aInt && bv != nil && bv.Cmp(av) != 0 {
		return av.Cmp(bv) < 0
	}
	ad, aok := AsString(a["artifact_digest"])
	bd, bok := AsString(b["artifact_digest"])
	if !aok || !bok {
		return CanonicalKey(a["artifact_digest"]) < CanonicalKey(b["artifact_digest"])
	}
	return ad < bd
}

func refVersionSortKey(ref map[string]any) (*bigInt, bool) {
	raw, present := ref["version"]
	if !present {
		return newBigInt(0), true
	}
	if IsIntegerNumber(raw) {
		if n, ok := parseBigInt(raw); ok {
			return n, true
		}
	}
	return nil, false
}

// DigestBytes returns the Contract §6.1.3 digest of data:
// "sha256:" + 64 lowercase hex digits.
func DigestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// DigestOf canonicalizes value with JCS and returns its digest.
func DigestOf(value any) (string, error) {
	data, err := JCS(value)
	if err != nil {
		return "", err
	}
	return DigestBytes(data), nil
}
