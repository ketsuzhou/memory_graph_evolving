// Package contract implements the Host-side Go adapter for the shared
// system contract (Contract §6–§7, §13.7, §16): an integer-only RFC 8785 JCS
// canonicalizer with SHA-256 digesting, the shared DTO views used by the
// semantic validators, the closed reason-code registry loader, and the
// conformance runner over the FND-001 golden corpus.
//
// The canonical core is deliberately independent of encoding/json marshaling:
// Go's default float64 number decoding silently rounds integers beyond 2^53,
// and json.Marshal emits Go-map-ordered, Go-escaped JSON. Neither may act as
// canonical bytes (HST-101 fail-closed: json.Marshal byte shortcut). Numbers
// are therefore kept as literal decimal text and re-serialized through
// math/big, keys are ordered by UTF-16 code units and strings are escaped
// with the pinned JCS rules ported from the reference validator.
package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"unicode/utf8"
)

// Minimal UTF-16 helpers (the pinned runtime image does not ship
// encoding/utf16; the semantics below are exactly that package's).

const (
	surrSelf = 0x10000
	replChar = utf8.RuneError
)

func isSurrogate(r rune) bool { return r >= 0xD800 && r < 0xE000 }

func decodeSurrogatePair(r1, r2 rune) rune {
	if 0xD800 <= r1 && r1 < 0xDC00 && 0xDC00 <= r2 && r2 < 0xE000 {
		return (r1-0xD800)<<10 | (r2 - 0xDC00) + surrSelf
	}
	return replChar
}

func encodeUTF16(s string) []uint16 {
	units := make([]uint16, 0, len(s))
	for _, r := range s {
		if r < surrSelf {
			units = append(units, uint16(r))
		} else {
			r -= surrSelf
			units = append(units, uint16(0xD800+(r>>10)), uint16(0xDC00+(r&0x3FF)))
		}
	}
	return units
}

// CanonicalizationError reports a value that cannot enter the integer-only
// hashed core (Contract §6.1.4). ReasonCode is a closed reason code.
type CanonicalizationError struct {
	ReasonCode string
}

func (e *CanonicalizationError) Error() string {
	return "canonicalization failed: " + e.ReasonCode
}

// Value is a parsed JSON value. Objects preserve document key order (last
// value wins on duplicate keys, mirroring the reference decoder).
type Value interface{ isValue() }

// Null is the JSON null literal.
type Null struct{}

// Bool is a JSON boolean.
type Bool bool

// Number is a JSON number kept as its exact literal text (UseNumber
// semantics). It is a distinct type so integers beyond float64 precision
// survive round-trips unharmed.
type Number string

// String is a JSON string.
type String string

// Array is a JSON array.
type Array []Value

// Object is a JSON object with insertion-ordered keys.
type Object struct {
	keys []string
	vals map[string]Value
}

func (Null) isValue()    {}
func (Bool) isValue()    {}
func (Number) isValue()  {}
func (String) isValue()  {}
func (Array) isValue()   {}
func (*Object) isValue() {}

// NewObject returns an empty ordered object.
func NewObject() *Object {
	return &Object{vals: make(map[string]Value)}
}

// Set inserts or replaces a key, preserving first-insertion position on
// replacement (dict-update semantics).
func (o *Object) Set(key string, v Value) {
	if _, exists := o.vals[key]; !exists {
		o.keys = append(o.keys, key)
	}
	o.vals[key] = v
}

// Get returns the value for key.
func (o *Object) Get(key string) (Value, bool) {
	v, ok := o.vals[key]
	return v, ok
}

// Keys returns the keys in document order.
func (o *Object) Keys() []string {
	return o.keys
}

// IsInteger reports whether the literal is a plain JSON integer
// (-?(0|[1-9][0-9]*)). Fraction/exponent forms - even integer valued ones
// such as 1.0 or 1e2 - are not integers of the hashed core.
func (n Number) IsInteger() bool {
	s := string(n)
	if len(s) > 0 && s[0] == '-' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	if s[0] == '0' {
		return len(s) == 1
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Int returns the exact integer value of an integer-form literal, of any
// magnitude (math/big; never float64).
func (n Number) Int() (*big.Int, bool) {
	if !n.IsInteger() {
		return nil, false
	}
	bi, ok := new(big.Int).SetString(string(n), 10)
	return bi, ok
}

// ---------------------------------------------------------------------------
// Strict JSON parsing (order-preserving, number-literal preserving)
// ---------------------------------------------------------------------------

type jsonParser struct {
	data []byte
	pos  int
}

// ParseJSON parses exactly one JSON document (plus surrounding whitespace).
// It mirrors the reference decoder: strict number grammar, no raw control
// characters in strings, NaN/Infinity accepted only as non-integer number
// constants, duplicate keys keep the first position and the last value, and
// trailing garbage is rejected.
func ParseJSON(data []byte) (Value, error) {
	// The hashed core is UTF-8; invalid UTF-8 is not parseable JSON here
	// (UnicodeDecodeError in the reference pipeline).
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("invalid UTF-8")
	}
	p := &jsonParser{data: data}
	p.skipWS()
	v, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.pos != len(p.data) {
		return nil, fmt.Errorf("unexpected trailing data at offset %d", p.pos)
	}
	return v, nil
}

func (p *jsonParser) skipWS() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *jsonParser) parseValue() (Value, error) {
	if p.pos >= len(p.data) {
		return nil, fmt.Errorf("unexpected end of input")
	}
	switch c := p.data[p.pos]; {
	case c == '{':
		return p.parseObject()
	case c == '[':
		return p.parseArray()
	case c == '"':
		s, err := p.parseString()
		if err != nil {
			return nil, err
		}
		return String(s), nil
	case c == '-' || (c >= '0' && c <= '9'):
		return p.parseNumber()
	case c == 't':
		if err := p.literal("true"); err != nil {
			return nil, err
		}
		return Bool(true), nil
	case c == 'f':
		if err := p.literal("false"); err != nil {
			return nil, err
		}
		return Bool(false), nil
	case c == 'n':
		if err := p.literal("null"); err != nil {
			return nil, err
		}
		return Null{}, nil
	// The reference decoder accepts NaN/Infinity constants; they classify as
	// non-integer numbers and fail closed at canonicalization time.
	case c == 'N':
		if err := p.literal("NaN"); err != nil {
			return nil, err
		}
		return Number("nan"), nil
	case c == 'I':
		if err := p.literal("Infinity"); err != nil {
			return nil, err
		}
		return Number("inf"), nil
	default:
		return nil, fmt.Errorf("unexpected character %q at offset %d", c, p.pos)
	}
}

func (p *jsonParser) literal(want string) error {
	if p.pos+len(want) > len(p.data) || string(p.data[p.pos:p.pos+len(want)]) != want {
		return fmt.Errorf("invalid literal at offset %d", p.pos)
	}
	p.pos += len(want)
	return nil
}

func (p *jsonParser) parseObject() (Value, error) {
	p.pos++ // '{'
	obj := NewObject()
	p.skipWS()
	if p.pos < len(p.data) && p.data[p.pos] == '}' {
		p.pos++
		return obj, nil
	}
	for {
		p.skipWS()
		if p.pos >= len(p.data) || p.data[p.pos] != '"' {
			return nil, fmt.Errorf("expected object key at offset %d", p.pos)
		}
		key, err := p.parseString()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		if p.pos >= len(p.data) || p.data[p.pos] != ':' {
			return nil, fmt.Errorf("expected ':' at offset %d", p.pos)
		}
		p.pos++
		p.skipWS()
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		obj.Set(key, val)
		p.skipWS()
		if p.pos >= len(p.data) {
			return nil, fmt.Errorf("unterminated object")
		}
		switch p.data[p.pos] {
		case ',':
			p.pos++
		case '}':
			p.pos++
			return obj, nil
		default:
			return nil, fmt.Errorf("expected ',' or '}' at offset %d", p.pos)
		}
	}
}

func (p *jsonParser) parseArray() (Value, error) {
	p.pos++ // '['
	arr := Array{}
	p.skipWS()
	if p.pos < len(p.data) && p.data[p.pos] == ']' {
		p.pos++
		return arr, nil
	}
	for {
		p.skipWS()
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		arr = append(arr, val)
		p.skipWS()
		if p.pos >= len(p.data) {
			return nil, fmt.Errorf("unterminated array")
		}
		switch p.data[p.pos] {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return arr, nil
		default:
			return nil, fmt.Errorf("expected ',' or ']' at offset %d", p.pos)
		}
	}
}

func (p *jsonParser) parseString() (string, error) {
	p.pos++ // opening '"'
	var sb []byte
	for {
		if p.pos >= len(p.data) {
			return "", fmt.Errorf("unterminated string")
		}
		c := p.data[p.pos]
		switch {
		case c == '"':
			p.pos++
			return string(sb), nil
		case c == '\\':
			p.pos++
			if p.pos >= len(p.data) {
				return "", fmt.Errorf("unterminated escape")
			}
			e := p.data[p.pos]
			p.pos++
			switch e {
			case '"':
				sb = append(sb, '"')
			case '\\':
				sb = append(sb, '\\')
			case '/':
				sb = append(sb, '/')
			case 'b':
				sb = append(sb, '\b')
			case 'f':
				sb = append(sb, '\f')
			case 'n':
				sb = append(sb, '\n')
			case 'r':
				sb = append(sb, '\r')
			case 't':
				sb = append(sb, '\t')
			case 'u':
				r, err := p.parseHex4()
				if err != nil {
					return "", err
				}
				if isSurrogate(rune(r)) {
					// Combine a valid pair; lone surrogates become U+FFFD
					// exactly like encoding/json.
					if p.pos+1 < len(p.data) && p.data[p.pos] == '\\' && p.data[p.pos+1] == 'u' {
						save := p.pos
						p.pos += 2
						r2, err := p.parseHex4()
						if err == nil && isSurrogate(rune(r2)) {
							if combined := decodeSurrogatePair(rune(r), rune(r2)); combined != utf8.RuneError {
								sb = utf8.AppendRune(sb, combined)
								continue
							}
						}
						p.pos = save
					}
					sb = utf8.AppendRune(sb, utf8.RuneError)
				} else {
					sb = utf8.AppendRune(sb, rune(r))
				}
			default:
				return "", fmt.Errorf("invalid escape %q at offset %d", e, p.pos-1)
			}
		case c < 0x20:
			return "", fmt.Errorf("raw control character in string at offset %d", p.pos)
		default:
			sb = append(sb, c)
			p.pos++
		}
	}
}

func (p *jsonParser) parseHex4() (uint32, error) {
	if p.pos+4 > len(p.data) {
		return 0, fmt.Errorf("truncated \\u escape")
	}
	var v uint32
	for i := 0; i < 4; i++ {
		c := p.data[p.pos+i]
		var d uint32
		switch {
		case c >= '0' && c <= '9':
			d = uint32(c - '0')
		case c >= 'a' && c <= 'f':
			d = uint32(c-'a') + 10
		case c >= 'A' && c <= 'F':
			d = uint32(c-'A') + 10
		default:
			return 0, fmt.Errorf("invalid \\u escape at offset %d", p.pos)
		}
		v = v<<4 | d
	}
	p.pos += 4
	return v, nil
}

func (p *jsonParser) parseNumber() (Value, error) {
	start := p.pos
	if p.pos < len(p.data) && p.data[p.pos] == '-' {
		p.pos++
		// "-Infinity" constant.
		if p.pos+7 < len(p.data)+1 && p.pos+8 <= len(p.data) && string(p.data[p.pos:p.pos+8]) == "Infinity" {
			p.pos += 8
			return Number("-inf"), nil
		}
	}
	// integer part: 0 | [1-9][0-9]*
	if p.pos >= len(p.data) {
		return nil, fmt.Errorf("truncated number at offset %d", start)
	}
	if p.data[p.pos] == '0' {
		p.pos++
	} else if p.data[p.pos] >= '1' && p.data[p.pos] <= '9' {
		p.pos++
		for p.pos < len(p.data) && isDigit(p.data[p.pos]) {
			p.pos++
		}
	} else {
		return nil, fmt.Errorf("invalid number at offset %d", start)
	}
	// fraction
	if p.pos < len(p.data) && p.data[p.pos] == '.' {
		p.pos++
		if p.pos >= len(p.data) || !isDigit(p.data[p.pos]) {
			return nil, fmt.Errorf("invalid fraction at offset %d", p.pos)
		}
		for p.pos < len(p.data) && isDigit(p.data[p.pos]) {
			p.pos++
		}
	}
	// exponent
	if p.pos < len(p.data) && (p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.data) && (p.data[p.pos] == '+' || p.data[p.pos] == '-') {
			p.pos++
		}
		if p.pos >= len(p.data) || !isDigit(p.data[p.pos]) {
			return nil, fmt.Errorf("invalid exponent at offset %d", p.pos)
		}
		for p.pos < len(p.data) && isDigit(p.data[p.pos]) {
			p.pos++
		}
	}
	return Number(p.data[start:p.pos]), nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// ---------------------------------------------------------------------------
// RFC 8785 JCS (Contract §6.1): integer-only, UTF-16 code-unit key order,
// pinned escaping, no whitespace
// ---------------------------------------------------------------------------

// JCS serializes v to canonical bytes. Non-integer numbers fail closed with
// a *CanonicalizationError carrying NON_INTEGER_NUMBER.
func JCS(v Value) ([]byte, error) {
	var buf []byte
	if err := emitJCS(&buf, v); err != nil {
		return nil, err
	}
	return buf, nil
}

func emitJCS(buf *[]byte, v Value) error {
	switch t := v.(type) {
	case Null:
		*buf = append(*buf, "null"...)
	case Bool:
		if t {
			*buf = append(*buf, "true"...)
		} else {
			*buf = append(*buf, "false"...)
		}
	case Number:
		// Integers are serialized as exact decimal digits: parse through
		// math/big so magnitudes beyond 2^53 keep full precision and -0
		// normalizes to 0, matching the reference integer round-trip.
		bi, ok := t.Int()
		if !ok {
			return &CanonicalizationError{ReasonCode: "NON_INTEGER_NUMBER"}
		}
		*buf = append(*buf, bi.String()...)
	case String:
		appendEscaped(buf, string(t))
	case Array:
		*buf = append(*buf, '[')
		for i, item := range t {
			if i > 0 {
				*buf = append(*buf, ',')
			}
			if err := emitJCS(buf, item); err != nil {
				return err
			}
		}
		*buf = append(*buf, ']')
	case *Object:
		*buf = append(*buf, '{')
		keys := append([]string(nil), t.keys...)
		sort.Slice(keys, func(i, j int) bool { return utf16KeyLess(keys[i], keys[j]) })
		for i, key := range keys {
			if i > 0 {
				*buf = append(*buf, ',')
			}
			appendEscaped(buf, key)
			*buf = append(*buf, ':')
			val := t.vals[key]
			if err := emitJCS(buf, val); err != nil {
				return err
			}
		}
		*buf = append(*buf, '}')
	default:
		return fmt.Errorf("unsupported JSON value type %T", v)
	}
	return nil
}

// appendEscaped escapes only '"' and '\' and C0 controls (short forms for
// \b \t \n \f \r, lowercase \u00xx otherwise); U+007F and all non-ASCII are
// emitted as raw UTF-8.
func appendEscaped(buf *[]byte, s string) {
	*buf = append(*buf, '"')
	for _, r := range s {
		switch {
		case r == '"':
			*buf = append(*buf, '\\', '"')
		case r == '\\':
			*buf = append(*buf, '\\', '\\')
		case r < 0x20:
			switch r {
			case '\b':
				*buf = append(*buf, '\\', 'b')
			case '\t':
				*buf = append(*buf, '\\', 't')
			case '\n':
				*buf = append(*buf, '\\', 'n')
			case '\f':
				*buf = append(*buf, '\\', 'f')
			case '\r':
				*buf = append(*buf, '\\', 'r')
			default:
				*buf = append(*buf, '\\', 'u', '0', '0',
					lowerHex(byte(r>>4)), lowerHex(byte(r&0xF)))
			}
		default:
			*buf = utf8.AppendRune(*buf, r)
		}
	}
	*buf = append(*buf, '"')
}

func lowerHex(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'a' + b - 10
}

// utf16KeyLess orders strings by their UTF-16 code unit sequences (the JCS
// rule), which differs from codepoint order around astral characters:
// e.g. U+10000 (D800 DC00) sorts before U+FFFD.
func utf16KeyLess(a, b string) bool {
	ua := encodeUTF16(a)
	ub := encodeUTF16(b)
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

// ---------------------------------------------------------------------------
// DTO-level normalization (Contract §6.3) and digesting
// ---------------------------------------------------------------------------

// NormalizeForHashing applies the symmetric-pair normalization that runs
// BEFORE JCS: a source_skill_refs array of exactly two refs (each an object
// with a string lineage_id) is sorted ascending by (lineage_id UTF-8 bytes,
// version, artifact_digest). This is how SimilarityAssessment A+B / B+A
// fixtures canonicalize to identical bytes (§16.4 #11). Children are
// normalized first; only the pair order changes at this level.
func NormalizeForHashing(v Value) Value {
	switch t := v.(type) {
	case *Object:
		out := NewObject()
		for _, k := range t.keys {
			out.Set(k, NormalizeForHashing(t.vals[k]))
		}
		if refsVal, ok := out.Get("source_skill_refs"); ok {
			if refs, ok := refsVal.(Array); ok && len(refs) == 2 {
				r0, ok0 := refs[0].(*Object)
				r1, ok1 := refs[1].(*Object)
				if ok0 && ok1 && isStringKey(r0, "lineage_id") && isStringKey(r1, "lineage_id") {
					ordered := Array{refs[0], refs[1]}
					if skillRefPairLess(r1, r0) {
						ordered = Array{refs[1], refs[0]}
					}
					out.Set("source_skill_refs", ordered)
				}
			}
		}
		return out
	case Array:
		out := make(Array, len(t))
		for i, item := range t {
			out[i] = NormalizeForHashing(item)
		}
		return out
	default:
		return v
	}
}

func isStringKey(o *Object, key string) bool {
	v, ok := o.Get(key)
	if !ok {
		return false
	}
	_, isStr := v.(String)
	return isStr
}

// skillRefPairLess compares (lineage_id UTF-8 bytes, version, artifact_digest)
// with the reference defaults version->0 and artifact_digest->"".
func skillRefPairLess(a, b *Object) bool {
	la, _ := StringOf(a, "lineage_id")
	lb, _ := StringOf(b, "lineage_id")
	if la != lb {
		return la < lb // Go string < is bytewise over UTF-8: matches bytes order
	}
	if c := compareRefVersion(a, b); c != 0 {
		return c < 0
	}
	da, _ := StringOf(a, "artifact_digest")
	db, _ := StringOf(b, "artifact_digest")
	if da != db {
		return da < db
	}
	return false
}

// compareRefVersion compares version with missing -> integer 0. All corpus
// versions are integers; non-integer values get a stable type rank so the
// order stays total and deterministic (the reference crashes on such mixes,
// so any total order is a fail-safe extension).
func compareRefVersion(a, b *Object) int {
	rank := func(v Value, ok bool) int {
		if !ok || v == nil {
			return 0 // missing -> integer 0
		}
		switch v.(type) {
		case Number:
			return 0
		case String:
			return 1
		default:
			return 2
		}
	}
	va, oka := a.Get("version")
	vb, okb := b.Get("version")
	ra, rb := rank(va, oka), rank(vb, okb)
	if ra != rb {
		if ra < rb {
			return -1
		}
		return 1
	}
	if ra == 0 {
		ia, _ := refVersionInt(va, oka)
		ib, _ := refVersionInt(vb, okb)
		return ia.Cmp(ib)
	}
	// Same non-number rank: deterministic raw-literal order.
	sa, _ := StringOf(a, "version")
	sb, _ := StringOf(b, "version")
	if sa != sb {
		if sa < sb {
			return -1
		}
		return 1
	}
	return 0
}

func refVersionInt(v Value, ok bool) (*big.Int, bool) {
	if !ok {
		return new(big.Int), true // missing -> 0
	}
	if n, isNum := v.(Number); isNum {
		return n.Int()
	}
	return new(big.Int), true
}

// StringOf returns the string value at key.
func StringOf(o *Object, key string) (string, bool) {
	v, ok := o.Get(key)
	if !ok {
		return "", false
	}
	s, isStr := v.(String)
	return string(s), isStr
}

// DigestBytes returns the Contract §6.2 digest form: "sha256:<lowercase hex>".
func DigestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// DigestOf returns the digest of the JCS canonicalization of v.
func DigestOf(v Value) (string, error) {
	b, err := JCS(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(b), nil
}

// MarshalJSON lets ordered objects embed into encoding/json consumers
// (transport side only; canonical bytes always come from JCS).
func (o *Object) MarshalJSON() ([]byte, error) {
	m := make(map[string]json.RawMessage, len(o.keys))
	for _, k := range o.keys {
		raw, err := json.Marshal(o.vals[k])
		if err != nil {
			return nil, err
		}
		m[k] = raw
	}
	return json.Marshal(m)
}
