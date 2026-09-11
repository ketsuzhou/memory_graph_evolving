package contract

import (
	"bytes"
	"encoding/base64"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// HST-101 Red/Green test: the Go adapter must reproduce the shared JCS/SHA-256
// golden corpus (FND-001, Contract §16) byte for byte, digest for digest,
// accept/reject for accept/reject and reason code for reason code, without
// ever consulting a copied expectation table inside the implementation.

func mustConformanceDir(t *testing.T) string {
	t.Helper()
	dir, err := ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("conformance dir %s unusable: %v", dir, err)
	}
	return dir
}

func corpusCase(t *testing.T, corpus *CorpusResult, id string) CaseResult {
	t.Helper()
	for _, c := range corpus.Cases {
		if c.CaseID == id {
			return c
		}
	}
	t.Fatalf("case %s not present in corpus result", id)
	return CaseResult{}
}

// expectedJSON loads a case's expected.json via the contract JSON parser.
func expectedJSON(t *testing.T, dir, sourcePath string) *Object {
	t.Helper()
	caseDir := filepath.Dir(sourcePath)
	data, err := os.ReadFile(filepath.Join(dir, caseDir, "expected.json"))
	if err != nil {
		t.Fatalf("read expected.json for %s: %v", sourcePath, err)
	}
	v, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("parse expected.json for %s: %v", sourcePath, err)
	}
	obj, ok := v.(*Object)
	if !ok {
		t.Fatalf("expected.json for %s is not an object", sourcePath)
	}
	return obj
}

func objectString(t *testing.T, obj *Object, key string) string {
	t.Helper()
	v, ok := obj.Get(key)
	if !ok {
		t.Fatalf("expected.json missing %s", key)
	}
	s, ok := v.(String)
	if !ok {
		t.Fatalf("expected.json %s is not a string", key)
	}
	return string(s)
}

// TestSharedConformance_ExactBytesDigestAndReasons is the HST-101 conformance
// entry point: all 48 manifest cases must pass with byte-exact canonical
// derivations, digests, accept decisions and reason codes.
func TestSharedConformance_ExactBytesDigestAndReasons(t *testing.T) {
	dir := mustConformanceDir(t)
	corpus, err := RunCorpus(dir)
	if err != nil {
		t.Fatalf("RunCorpus: %v", err)
	}
	for _, ce := range corpus.CorpusErrors {
		t.Errorf("corpus error: %s", ce)
	}
	if len(corpus.CorpusErrors) != 0 {
		t.Fatalf("corpus integrity errors block case evaluation")
	}
	if len(corpus.Cases) != 48 {
		t.Fatalf("manifest declares %d cases, want 48", len(corpus.Cases))
	}

	// Every case, one sub-test each, in manifest order.
	for _, c := range corpus.Cases {
		c := c
		t.Run(c.CaseID, func(t *testing.T) {
			if !c.OK {
				t.Fatalf("case %s failed: %s", c.CaseID, strings.Join(c.Problems, "; "))
			}
			// Byte-exactness: derived canonical bytes equal the golden file
			// for every canonicalizable accepted case.
			if c.Derived.Accept && c.Derived.Canonical != nil {
				if !bytes.Equal(c.Derived.Canonical, c.CanonicalFileBytes) {
					t.Fatalf("derived canonical bytes differ from canonical.utf8 for accepted case")
				}
				if c.Derived.Digest != DigestBytes(c.Derived.Canonical) {
					t.Fatalf("derived digest is not sha256 of derived canonical bytes")
				}
			}
		})
	}

	// Unicode / key order (§16.4): UTF-16 code-unit order, differing from
	// codepoint order for U+1D11E vs U+FF04.
	canon2 := corpusCase(t, corpus, "canon-002-utf16-key-order")
	exp2 := expectedJSON(t, dir, canon2.SourcePath)
	if canon2.Derived.Digest != objectString(t, exp2, "expected_digest") {
		t.Fatalf("canon-002 derived digest %s != expected %s", canon2.Derived.Digest, objectString(t, exp2, "expected_digest"))
	}
	if !bytes.Contains(canon2.Derived.Canonical, []byte("𝄞")) {
		t.Fatalf("canon-002 canonical bytes must carry raw non-ASCII (no \\u escaping)")
	}

	// Same bytes, three sources: key order, pretty whitespace and \u escapes
	// canonicalize to identical bytes and digests.
	var trio [][]byte
	for _, id := range []string{"canon-001-key-order", "canon-007-same-bytes-pretty", "canon-008-same-bytes-escaped"} {
		c := corpusCase(t, corpus, id)
		if !c.Derived.Accept {
			t.Fatalf("%s must be accepted", id)
		}
		trio = append(trio, c.Derived.Canonical)
	}
	for i := 1; i < len(trio); i++ {
		if !bytes.Equal(trio[0], trio[i]) {
			t.Fatalf("same-bytes trio diverged: %q vs %q", trio[0], trio[i])
		}
	}
	if want := corpusCase(t, corpus, "canon-001-key-order").Derived.Digest; corpusCase(t, corpus, "canon-008-same-bytes-escaped").Derived.Digest != want {
		t.Fatalf("same-bytes trio digests diverged")
	}

	// Non-integer numbers fail closed (§6.1.4), including exponent forms that
	// are integer valued.
	for _, id := range []string{"canon-neg-001-float-value", "canon-neg-002-exponent-number"} {
		c := corpusCase(t, corpus, id)
		if c.Derived.Accept || c.Derived.ReasonCode != "NON_INTEGER_NUMBER" {
			t.Fatalf("%s: got accept=%v reason=%q, want reject NON_INTEGER_NUMBER", id, c.Derived.Accept, c.Derived.ReasonCode)
		}
		if c.Derived.Canonical != nil || len(c.CanonicalFileBytes) != 0 {
			t.Fatalf("%s: non-canonicalizable case must have no canonical bytes", id)
		}
	}

	// BOM fails closed (§6.1.1).
	if c := corpusCase(t, corpus, "canon-neg-003-bom-source"); c.Derived.Accept || c.Derived.ReasonCode != "BOM_NOT_ALLOWED" {
		t.Fatalf("canon-neg-003: got accept=%v reason=%q, want reject BOM_NOT_ALLOWED", c.Derived.Accept, c.Derived.ReasonCode)
	}

	// Unknown *required* extension fails closed; unknown optional is ignored.
	if c := corpusCase(t, corpus, "artifact-neg-001-required-extension"); c.Derived.Accept || c.Derived.ReasonCode != "UNKNOWN_REQUIRED_EXTENSION" {
		t.Fatalf("artifact-neg-001: got accept=%v reason=%q, want reject UNKNOWN_REQUIRED_EXTENSION", c.Derived.Accept, c.Derived.ReasonCode)
	}
	if c := corpusCase(t, corpus, "artifact-004-optional-extension"); !c.Derived.Accept {
		t.Fatalf("artifact-004 optional extension must stay accepted")
	}

	// Non-exact refs: naked id, `latest` version, Graph node form (§5.1.5).
	for id, reason := range map[string]string{
		"ref-neg-001-naked-id":             "NON_EXACT_REF",
		"ref-neg-002-latest":               "NON_EXACT_REF",
		"ref-neg-003-graph-node":           "NON_EXACT_REF",
		"ref-neg-004-uncommitted-evidence": "EVIDENCE_NOT_COMMITTED",
	} {
		c := corpusCase(t, corpus, id)
		if c.Derived.Accept || c.Derived.ReasonCode != reason {
			t.Fatalf("%s: got accept=%v reason=%q, want reject %s", id, c.Derived.Accept, c.Derived.ReasonCode, reason)
		}
	}

	// Negative reason codes pinned exactly for every rejected case.
	rejectReasons := map[string]string{
		"canon-neg-001-float-value":           "NON_INTEGER_NUMBER",
		"canon-neg-002-exponent-number":       "NON_INTEGER_NUMBER",
		"canon-neg-003-bom-source":            "BOM_NOT_ALLOWED",
		"ref-neg-001-naked-id":                "NON_EXACT_REF",
		"ref-neg-002-latest":                  "NON_EXACT_REF",
		"ref-neg-003-graph-node":              "NON_EXACT_REF",
		"ref-neg-004-uncommitted-evidence":    "EVIDENCE_NOT_COMMITTED",
		"artifact-neg-001-required-extension": "UNKNOWN_REQUIRED_EXTENSION",
		"artifact-neg-002-composite-cycle":    "COMPOSITE_CYCLE",
		"artifact-neg-003-port-missing":       "PORT_SCHEMA_MISSING",
		"artifact-neg-004-permission-cap":     "PERMISSION_CAP_EXCEEDED",
		"artifact-neg-005-kind-drift":         "REF_MISMATCH",
		"event-neg-001-projection-gap":        "PROJECTION_SEQUENCE_GAP",
		"event-neg-002-projection-conflict":   "PROJECTION_EVENT_CONFLICT",
		"event-neg-003-illegal-transition":    "ILLEGAL_STATE_TRANSITION",
		"merge-neg-001-below-threshold":       "SIMILARITY_BELOW_THRESHOLD",
		"merge-neg-002-blocking-conflict":     "MERGE_BLOCKING_CONFLICT",
		"negative-001-digest-tamper":          "DIGEST_MISMATCH",
	}
	accepted, rejected := 0, 0
	for _, c := range corpus.Cases {
		if c.Derived.Accept {
			if c.Derived.ReasonCode != "" {
				t.Fatalf("accepted case %s carries reason %q", c.CaseID, c.Derived.ReasonCode)
			}
			accepted++
			continue
		}
		rejected++
		want, ok := rejectReasons[c.CaseID]
		if !ok {
			t.Fatalf("rejected case %s missing from pinned reason table", c.CaseID)
		}
		if c.Derived.ReasonCode != want {
			t.Fatalf("case %s derived reason %q, want %q", c.CaseID, c.Derived.ReasonCode, want)
		}
	}
	if accepted != 30 || rejected != 18 {
		t.Fatalf("corpus split accepted=%d rejected=%d, want 30/18", accepted, rejected)
	}
	if len(rejectReasons) != 18 {
		t.Fatalf("pinned reason table has %d entries, want 18", len(rejectReasons))
	}

	// Digest-tamper detection: the tampered canonical.utf8 file must NOT match
	// the canonicalization of the source, while expected.json still describes
	// the untampered truth (§16.4 negative).
	tamper := corpusCase(t, corpus, "negative-001-digest-tamper")
	if bytes.Equal(tamper.Derived.Canonical, tamper.CanonicalFileBytes) {
		t.Fatalf("digest-tamper fixture was not detected: file bytes equal derived canonical")
	}
	expT := expectedJSON(t, dir, tamper.SourcePath)
	if tamper.Derived.Digest != objectString(t, expT, "expected_digest") {
		t.Fatalf("tamper: derived digest %s != untampered expected %s", tamper.Derived.Digest, objectString(t, expT, "expected_digest"))
	}
	if base64.StdEncoding.EncodeToString(tamper.CanonicalFileBytes) == base64.StdEncoding.EncodeToString(tamper.Derived.Canonical) {
		t.Fatalf("tamper: base64 of file must differ from base64 of derived canonical")
	}

	// Symmetric pair normalization (§16.4 #11): A+B and B+A sources
	// canonicalize to identical bytes.
	ab := corpusCase(t, corpus, "merge-001-similarity-ab").Derived
	ba := corpusCase(t, corpus, "merge-002-similarity-ba").Derived
	if !ab.Accept || !ba.Accept {
		t.Fatalf("similarity pair cases must be accepted")
	}
	if !bytes.Equal(ab.Canonical, ba.Canonical) || ab.Digest != ba.Digest {
		t.Fatalf("merge pair normalization failed: A+B != B+A")
	}
}

// ---------------------------------------------------------------------------
// Unit tests: JCS vectors, big-integer decoding, pair normalization,
// fail-closed derivations and reason-policy digest verification.
// ---------------------------------------------------------------------------

func parseOne(t *testing.T, data string) Value {
	t.Helper()
	v, err := ParseJSON([]byte(data))
	if err != nil {
		t.Fatalf("ParseJSON(%q): %v", data, err)
	}
	return v
}

func jcsOK(t *testing.T, v Value) []byte {
	t.Helper()
	b, err := JCS(v)
	if err != nil {
		t.Fatalf("JCS: %v", err)
	}
	return b
}

func TestJCSKeysSortByUTF16CodeUnits(t *testing.T) {
	v := parseOne(t, `{"\uff04": 3, "\ud834\udd1e": 2, "é": 1, "z": 0}`)
	// UTF-16 code-unit order: z(U+007A) < é(U+00E9) < U+1D11E(D834 DD1E) < U+FF04.
	// Codepoint order would place U+FF04 before U+1D11E; JCS must not.
	want := "{\"z\":0,\"é\":1,\"𝄞\":2,\"＄\":3}"
	if got := string(jcsOK(t, v)); got != want {
		t.Fatalf("JCS key order: got %q want %q", got, want)
	}
}

func TestJCSStringEscaping(t *testing.T) {
	// JSON input escapes: \" \\ \u0007 \b \t \n \u000c \r \u001f plus raw DEL
	// and a non-ASCII rune. JCS emits short forms for \b\t\n\f\r, \u00xx
	// lowercase for the rest of C0, and everything >= U+007F raw UTF-8.
	v := parseOne(t, "{\"s\": \"a\\\"b\\\\c\\u0007\\b\\t\\n\\u000c\\r\\u001fd\\u007f€\"}")
	want := "{\"s\":\"a\\\"b\\\\c\\u0007\\b\\t\\n\\f\\r\\u001fd\u007f€\"}"
	if got := string(jcsOK(t, v)); got != want {
		t.Fatalf("JCS escaping: got %q want %q", got, want)
	}
}

func TestJCSBigIntegerPrecision(t *testing.T) {
	// 2^53+1 and uint64 max must survive without float64 rounding and
	// without json.Marshal-style number formatting.
	v := parseOne(t, `{"big_53": 9007199254740993, "big_u64": 18446744073709551615, "max_i64": 9223372036854775807}`)
	got := string(jcsOK(t, v))
	want := `{"big_53":9007199254740993,"big_u64":18446744073709551615,"max_i64":9223372036854775807}`
	if got != want {
		t.Fatalf("JCS big integers: got %s want %s", got, want)
	}
	// Negative zero normalizes to 0 like Python int(str) round-tripping.
	if got := string(jcsOK(t, parseOne(t, `{"n": -0}`))); got != `{"n":0}` {
		t.Fatalf("JCS -0: got %s", got)
	}
	n := Number("9007199254740993")
	bi, ok := n.Int()
	if !ok || bi.Cmp(big.NewInt(9007199254740993)) != 0 {
		t.Fatalf("Number.Int exactness failed")
	}
}

func TestJCSRejectsNonIntegerNumbers(t *testing.T) {
	for _, src := range []string{`1.0`, `1e2`, `-1.5`, `0.1`, `1E+2`} {
		if _, err := JCS(parseOne(t, src)); err == nil {
			t.Fatalf("JCS(%q) must fail", src)
		} else if ce, ok := err.(*CanonicalizationError); !ok || ce.ReasonCode != "NON_INTEGER_NUMBER" {
			t.Fatalf("JCS(%q) error = %v, want NON_INTEGER_NUMBER", src, err)
		}
	}
	if !Number("42").IsInteger() || Number("1.0").IsInteger() || Number("1e2").IsInteger() || Number("nan").IsInteger() {
		t.Fatalf("Number.IsInteger misclassifies")
	}
}

func TestParseJSONFailClosed(t *testing.T) {
	for _, src := range []string{"", "{", `{"a":1,}`, "01", `+1`, ".5", `1.`, `{"a":1} x`, `nan`, "\x00", "[1,2] 3"} {
		if _, err := ParseJSON([]byte(src)); err == nil {
			t.Fatalf("ParseJSON(%q) must fail", src)
		}
	}
	if _, err := ParseJSON([]byte("\xff\xfe{")); err == nil {
		t.Fatalf("ParseJSON must reject invalid UTF-8")
	}
}

func TestNormalizeForHashingSortsRefPair(t *testing.T) {
	ab := parseOne(t, `{"source_skill_refs": [
		{"lineage_id": "sg-alpha", "version": 2, "artifact_digest": "sha256:a"},
		{"lineage_id": "sg-beta", "version": 1, "artifact_digest": "sha256:b"}], "k": 1}`)
	ba := parseOne(t, `{"k": 1, "source_skill_refs": [
		{"lineage_id": "sg-beta", "version": 1, "artifact_digest": "sha256:b"},
		{"lineage_id": "sg-alpha", "version": 2, "artifact_digest": "sha256:a"}]}`)
	if !bytes.Equal(jcsOK(t, NormalizeForHashing(ab)), jcsOK(t, NormalizeForHashing(ba))) {
		t.Fatalf("pair normalization must canonicalize A+B and B+A identically")
	}
	// The raw (un-normalized) trees must differ so the test cannot pass vacuously.
	if bytes.Equal(jcsOK(t, ab), jcsOK(t, ba)) {
		t.Fatalf("un-normalized trees unexpectedly equal")
	}
	// Non-pair lists (len != 2) keep document order.
	three := parseOne(t, `{"source_skill_refs": [
		{"lineage_id": "b"}, {"lineage_id": "a"}, {"lineage_id": "c"}]}`)
	got := string(jcsOK(t, NormalizeForHashing(three)))
	if !(strings.Index(got, `"b"`) < strings.Index(got, `"a"`)) {
		t.Fatalf("3-element source_skill_refs must not be sorted: %s", got)
	}
}

func TestDeriveOutcomeFailClosedVectors(t *testing.T) {
	// BOM
	if o := DeriveOutcome([]byte("\xef\xbb\xbf{}"), "canonicalization", nil); o.Accept || o.ReasonCode != "BOM_NOT_ALLOWED" || o.Canonical != nil {
		t.Fatalf("BOM handling: %+v", o)
	}
	// Invalid JSON
	if o := DeriveOutcome([]byte("{\"a\": }"), "canonicalization", nil); o.Accept || o.ReasonCode != "INVALID_JSON" {
		t.Fatalf("invalid JSON handling: %+v", o)
	}
	// Non-integer number
	if o := DeriveOutcome([]byte("{\"x\": 1.5}"), "canonicalization", nil); o.Accept || o.ReasonCode != "NON_INTEGER_NUMBER" {
		t.Fatalf("float handling: %+v", o)
	}
	// Graph node form in a ref case
	if o := DeriveOutcome([]byte("{\"node_id\": \"n1\"}"), "ref", []byte("{\"node_id\":\"n1\"}")); o.Accept || o.ReasonCode != "NON_EXACT_REF" {
		t.Fatalf("graph form handling: %+v", o)
	}
	// Digest mismatch against the canonical sidecar
	src := []byte("{\"a\":1}")
	canonical := jcsOK(t, parseOne(t, string(src)))
	if o := DeriveOutcome(src, "canonicalization", append([]byte{}, canonical...)); !o.Accept {
		t.Fatalf("matching sidecar must accept: %+v", o)
	}
	tampered := append([]byte{}, canonical...)
	tampered[len(tampered)-1] = '2'
	if o := DeriveOutcome(src, "canonicalization", tampered); o.Accept || o.ReasonCode != "DIGEST_MISMATCH" {
		t.Fatalf("tampered sidecar must reject with DIGEST_MISMATCH: %+v", o)
	}
}

func TestReasonPolicyDigestVerification(t *testing.T) {
	dir := mustConformanceDir(t)
	bundle, err := LoadReasonBundle(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("LoadReasonBundle: %v", err)
	}
	if bundle.System.RegistryDigest != "sha256:16410afa27498bb425885d4629c65309389f15adeb975f97f98674170ad00a2c" {
		t.Fatalf("system registry digest drifted: %s", bundle.System.RegistryDigest)
	}
	if bundle.HostProxy.RegistryDigest != "sha256:e48f27252bff3fd34868ef4bc5b56a678cf2a35d59f4cd7f2c71f78290485f5e" {
		t.Fatalf("host-proxy registry digest drifted: %s", bundle.HostProxy.RegistryDigest)
	}

	// Lookup semantics: status/retryable/retry_scope/terminal from the registry.
	e, ok := bundle.Lookup("PROJECTION_BEHIND_REQUIRED_SEQUENCE")
	if !ok || e.Status != "failure" || !e.Retryable || e.RetryScope != "same_request" || e.Terminal {
		t.Fatalf("lookup PROJECTION_BEHIND_REQUIRED_SEQUENCE: %+v ok=%v", e, ok)
	}
	e, ok = bundle.Lookup("IDEMPOTENCY_CONFLICT")
	if !ok || e.Status != "failure" || e.Retryable || e.RetryScope != "new_attempt" || !e.Terminal {
		t.Fatalf("lookup IDEMPOTENCY_CONFLICT: %+v ok=%v", e, ok)
	}
	e, ok = bundle.Lookup("HOST_PROXY_TIMEOUT")
	if !ok || e.Owner != "host-proxy" || !e.Retryable || e.RetryScope != "same_request" {
		t.Fatalf("lookup HOST_PROXY_TIMEOUT: %+v ok=%v", e, ok)
	}
	// Unknown codes fail closed (no entry, no inferred meaning).
	if _, ok := bundle.Lookup("DEFINITELY_NOT_A_CODE"); ok {
		t.Fatalf("unknown code must not resolve")
	}

	// Tampered policy file must fail digest verification.
	tampered := t.TempDir()
	data, err := os.ReadFile(filepath.Join(dir, "policy", "host-proxy-reason-codes.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"retry_scope":"none","retryable":false,"spec_ref":"Host 5.1/5.6`), []byte(`"retry_scope":"same_request","retryable":true,"spec_ref":"Host 5.1/5.6`), 1)
	if err := os.WriteFile(filepath.Join(tampered, "host-proxy-reason-codes.v1.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tampered, "system-reason-codes.v1.json"), mustPolicyBytes(t, dir, "system-reason-codes.v1.json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadReasonBundle(tampered); err == nil {
		t.Fatalf("tampered policy must fail closed with a digest mismatch")
	} else if !strings.Contains(err.Error(), "digest") {
		t.Fatalf("tampered policy error should mention digest: %v", err)
	}
}

func mustPolicyBytes(t *testing.T, dir, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "policy", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestManifestAndCorpusIntegrityViaEnvOverride(t *testing.T) {
	// RSIH_CONFORMANCE_DIR must override the default resolution.
	dir := mustConformanceDir(t)
	t.Setenv("RSIH_CONFORMANCE_DIR", dir)
	if got, err := ConformanceDir(); err != nil || got != dir {
		t.Fatalf("env override failed: %s %v", got, err)
	}
}
