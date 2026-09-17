package contract

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// corpusDir resolves the shared FND-001 conformance corpus (Contract §16)
// consumed verbatim by Go/TS/Python runners. It must never be a
// repository-local copy of the expectations.
func corpusDir(t *testing.T) string {
	t.Helper()
	dir, err := DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("conformance manifest.json not found under %s: %v", dir, err)
	}
	return dir
}

// TestContractConformance_JCSDigestAcceptRejectReasonParity is the GMS-101
// Red test. It walks every manifest case in the shared golden corpus, derives
// accept/reject, the reason code, canonical bytes and digest independently of
// the golden expectations, and only then compares against expected.json plus
// the manifest mirrors (Contract §16.3/§16.5). Every derived reject reason
// must additionally be backed by the digest-verified system reason registry
// (Contract §13.7.1).
func TestContractConformance_JCSDigestAcceptRejectReasonParity(t *testing.T) {
	dir := corpusDir(t)

	res := RunCorpus(dir)
	for _, e := range res.CorpusErrors {
		t.Errorf("corpus error: %s", e)
	}
	if len(res.CorpusErrors) > 0 {
		t.Fatalf("corpus integrity errors under %s; aborting parity checks", dir)
	}

	const wantCases = 48
	if len(res.Cases) != wantCases {
		t.Errorf("manifest declares %d cases, want %d", len(res.Cases), wantCases)
	}

	byCategory := map[string]int{}
	failed := 0
	for _, c := range res.Cases {
		byCategory[c.Category]++
		if len(c.Problems) > 0 {
			failed++
			t.Errorf("case %s FAILED: %s", c.CaseID, strings.Join(c.Problems, "; "))
			continue
		}
		// Structural sanity on the derived values that feed the S1 report.
		// Digest/length exist exactly when the case is canonicalizable; the
		// BOM/JSON/non-integer negatives are not canonicalizable and report
		// null/0.
		if c.Canonical != nil {
			if c.DerivedDigest == nil || !strings.HasPrefix(*c.DerivedDigest, "sha256:") {
				t.Errorf("case %s: derived digest missing/malformed: %v", c.CaseID, c.DerivedDigest)
			}
			if c.DerivedLength != len(c.Canonical) {
				t.Errorf("case %s: derived length %d != canonical len %d", c.CaseID, c.DerivedLength, len(c.Canonical))
			}
		} else if c.DerivedDigest != nil || c.DerivedLength != 0 {
			t.Errorf("case %s: non-canonicalizable case must not carry digest/length: %v/%d", c.CaseID, c.DerivedDigest, c.DerivedLength)
		}
		if c.DerivedBase64 != base64.StdEncoding.EncodeToString(c.CanonicalFileBytes) {
			t.Errorf("case %s: derived base64 is not the canonical.utf8 file bytes", c.CaseID)
		}
		if !c.DerivedAccept && c.DerivedReason == nil {
			t.Errorf("case %s: rejected without derived reason", c.CaseID)
		}
		if c.DerivedAccept && c.DerivedReason != nil {
			t.Errorf("case %s: accepted but carries derived reason %s", c.CaseID, *c.DerivedReason)
		}
	}
	if failed > 0 {
		t.Fatalf("%d/%d cases failed", failed, len(res.Cases))
	}
	for _, cat := range Categories {
		if byCategory[cat] == 0 {
			t.Errorf("category %s has no cases in the corpus run", cat)
		}
	}

	// Reason parity: each derived reject code must exist in the digest-verified
	// system registry and map to a failure status.
	sys, err := LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("load system reason policy: %v", err)
	}
	derivedReasons := map[string]bool{}
	for _, c := range res.Cases {
		if c.DerivedReason == nil {
			continue
		}
		code := *c.DerivedReason
		if derivedReasons[code] {
			continue
		}
		derivedReasons[code] = true
		info, err := sys.Lookup(code)
		if err != nil {
			t.Errorf("derived reason %s not in system registry: %v", code, err)
			continue
		}
		if info.Status != "failure" || !info.Terminal {
			t.Errorf("derived reason %s maps to status=%q terminal=%v, want failure/terminal", code, info.Status, info.Terminal)
		}
	}
	if len(derivedReasons) < 10 {
		t.Errorf("expected the corpus to exercise at least 10 distinct reject reasons, got %d", len(derivedReasons))
	}
}

// TestJCS_UTF16CodeUnitKeyOrder pins the RFC 8785 key ordering: UTF-16 code
// units, not code points (canon-002 vector: z < é < U+1D11E < U+FF04).
func TestJCS_UTF16CodeUnitKeyOrder(t *testing.T) {
	src := []byte(`{"z": 0, "é": 1, "𝄞": 2, "＄": 3}`)
	parsed, err := ParseJSONStrict(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := JCS(parsed)
	if err != nil {
		t.Fatalf("jcs: %v", err)
	}
	want := `{"z":0,"é":1,"𝄞":2,"＄":3}`
	if string(got) != want {
		t.Errorf("jcs key order = %q, want %q", got, want)
	}
}

// TestJCS_IntegerOnlyPreservesExactDecimalDigits pins the integer-only hashed
// core (Contract §6.1.4): integers beyond 2^53 keep full precision and every
// non-integer number fails closed with NON_INTEGER_NUMBER.
func TestJCS_IntegerOnlyPreservesExactDecimalDigits(t *testing.T) {
	src := []byte(`{"zero": 0, "negzero": -0, "big_53": 9007199254740993, "big_u64": 18446744073709551615, "max_i64": 9223372036854775807}`)
	parsed, err := ParseJSONStrict(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := JCS(parsed)
	if err != nil {
		t.Fatalf("jcs: %v", err)
	}
	want := `{"big_53":9007199254740993,"big_u64":18446744073709551615,"max_i64":9223372036854775807,"negzero":0,"zero":0}`
	if string(got) != want {
		t.Errorf("jcs integers = %q, want %q", got, want)
	}

	for _, bad := range []string{`1.0`, `-0.0`, `1e2`, `1E2`, `[1.5]`, `{"a":2.5}`} {
		v, err := ParseJSONStrict([]byte(bad))
		if err != nil {
			t.Fatalf("parse %s: %v", bad, err)
		}
		_, err = JCS(v)
		var ce *CanonicalizationError
		if err == nil || !errorsAs(err, &ce) || ce.ReasonCode != ReasonNonIntegerNumber {
			t.Errorf("jcs(%s) error = %v, want NON_INTEGER_NUMBER", bad, err)
		}
	}
}

// TestJCS_StringEscaping pins the escape table: short forms for
// \b \t \n \f \r, \u00xx lowercase hex for other control chars, raw
// U+007F and raw non-ASCII UTF-8 (canon-003 vector).
func TestJCS_StringEscaping(t *testing.T) {
	v := map[string]any{"k": "a\u0007b\bc\td\ne\ff\rg\u000bh\"i\\jx\u007fé𝄞"}
	got, err := JCS(v)
	if err != nil {
		t.Fatalf("jcs: %v", err)
	}
	want := "{\"k\":\"a\\u0007b\\bc\\td\\ne\\ff\\rg\\u000bh\\\"i\\\\jx\x7fé𝄞\"}"
	if string(got) != want {
		t.Errorf("jcs escaping:\n got %q\nwant %q", got, want)
	}
}

// TestNormalizeForHashing_SourceSkillRefPairOrder pins Contract §6.3: the
// SimilarityAssessment A+B and B+A fixtures canonicalize to identical bytes
// because the two-element source_skill_refs array is sorted by
// (lineage_id UTF-8 bytes, version, artifact_digest) before JCS.
func TestNormalizeForHashing_SourceSkillRefPairOrder(t *testing.T) {
	dir := corpusDir(t)
	ab := readFixtureBytes(t, dir, "merge/merge-001-similarity-ab")
	ba := readFixtureBytes(t, dir, "merge/merge-002-similarity-ba")

	canonical := func(raw []byte) []byte {
		t.Helper()
		parsed, err := ParseJSONStrict(raw)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		got, err := JCS(NormalizeForHashing(parsed))
		if err != nil {
			t.Fatalf("jcs: %v", err)
		}
		return got
	}

	cab, cba := canonical(ab), canonical(ba)
	if !bytes.Equal(cab, cba) {
		t.Errorf("A+B and B+A source_skill_refs must normalize to identical bytes:\n %s\n %s", cab, cba)
	}
	golden := readGoldenCanonical(t, dir, "merge/merge-001-similarity-ab")
	if !bytes.Equal(cab, golden) {
		t.Errorf("normalized canonical bytes differ from golden canonical.utf8:\n got  %s\n want %s", cab, golden)
	}
}

// TestDeriveOutcome_FailClosedPaths pins the derived evaluation order for the
// canonical fail-closed preconditions (Contract §6.1.1/§6.1.4, §13.7).
func TestDeriveOutcome_FailClosedPaths(t *testing.T) {
	cases := []struct {
		name     string
		source   []byte
		category string
		file     []byte
		accept   bool
		reason   string
	}{
		{"bom", append(append([]byte{}, bomUTF8...), []byte(`{}`)...), "canonicalization", nil, false, ReasonBOMNotAllowed},
		{"invalid json", []byte(`{"a":`), "canonicalization", nil, false, ReasonInvalidJSON},
		{"invalid utf-8", []byte("\"a\xffb\""), "canonicalization", nil, false, ReasonInvalidJSON},
		{"trailing data", []byte(`{"a":1} {"b":2}`), "canonicalization", nil, false, ReasonInvalidJSON},
		{"non-integer", []byte(`[1.0]`), "canonicalization", nil, false, ReasonNonIntegerNumber},
		{"exponent", []byte(`[1e2]`), "canonicalization", nil, false, ReasonNonIntegerNumber},
		{"unknown required extension", []byte(`{"extensions":{"x":{"required":true}}}`), "canonicalization", nil, false, ReasonUnknownRequiredExtension},
		{"uncommitted evidence", []byte(`{"commit_state":"staged"}`), "ref", nil, false, ReasonEvidenceNotCommitted},
		{"graph node ref", []byte(`{"node_id":"n1"}`), "ref", nil, false, ReasonNonExactRef},
		{"latest ref", []byte(`{"lineage_id":"sg","version":"latest","artifact_digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000"}`), "ref", nil, false, ReasonNonExactRef},
		{"digest mismatch", []byte(`{"a":1}`), "canonicalization", []byte(`{"a":2}`), false, ReasonDigestMismatch},
		{"accept", []byte(`{"a":1}`), "canonicalization", []byte(`{"a":1}`), true, ""},
	}
	for _, tc := range cases {
		out := DeriveOutcome(tc.source, tc.category, tc.file)
		if out.Accept != tc.accept || out.ReasonCode != tc.reason {
			t.Errorf("%s: derive = (accept=%v, reason=%q), want (accept=%v, reason=%q)", tc.name, out.Accept, out.ReasonCode, tc.accept, tc.reason)
		}
		if tc.accept && !bytes.Equal(out.Canonical, tc.file) {
			t.Errorf("%s: accepted canonical = %s, want %s", tc.name, out.Canonical, tc.file)
		}
	}
}

// TestReasonPolicy_DigestVerifiedLoadAndLookup pins the CTR-004 load path:
// both frozen registries load only when the recomputed JCS digest of the
// document minus registry_digest matches the declared (and contract-frozen)
// value, and unknown codes fail closed.
func TestReasonPolicy_DigestVerifiedLoadAndLookup(t *testing.T) {
	dir := corpusDir(t)
	policyDir := filepath.Join(dir, "policy")

	sys, err := LoadSystemReasonPolicy(policyDir)
	if err != nil {
		t.Fatalf("load system policy: %v", err)
	}
	if sys.RegistryDigest != SystemRegistryDigestFrozen {
		t.Errorf("system registry digest = %s, want frozen %s", sys.RegistryDigest, SystemRegistryDigestFrozen)
	}
	info, err := sys.Lookup("PROJECTION_BEHIND_REQUIRED_SEQUENCE")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if info.Status != "failure" || !info.Retryable || info.RetryScope != "same_request" || info.Terminal {
		t.Errorf("PROJECTION_BEHIND_REQUIRED_SEQUENCE mapping = %+v, want failure/retryable/same_request/non-terminal", info)
	}
	if inc, err := sys.Lookup("REPLAY_INCONCLUSIVE"); err != nil || inc.Status != "inconclusive" || inc.Retryable {
		t.Errorf("REPLAY_INCONCLUSIVE mapping = %+v err=%v, want inconclusive/non-retryable", inc, err)
	}
	if _, err := sys.Lookup("NOT_A_REAL_CODE"); err == nil {
		t.Errorf("unknown code must fail closed")
	}

	host, err := LoadHostProxyReasonPolicy(policyDir)
	if err != nil {
		t.Fatalf("load host-proxy policy: %v", err)
	}
	if host.RegistryDigest != HostProxyRegistryDigestFrozen {
		t.Errorf("host registry digest = %s, want frozen %s", host.RegistryDigest, HostProxyRegistryDigestFrozen)
	}
	hinfo, err := host.Lookup("HOST_PROXY_TIMEOUT")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !hinfo.Retryable || hinfo.RetryScope != "same_request" || hinfo.Terminal {
		t.Errorf("HOST_PROXY_TIMEOUT mapping = %+v, want retryable/same_request/non-terminal", hinfo)
	}
	if _, err := host.Lookup("DIGEST_MISMATCH"); err == nil {
		t.Errorf("host registry must not own the gms-system code DIGEST_MISMATCH")
	}

	// Tamper path: any edit without re-signing must be rejected at load time.
	tampered := t.TempDir()
	raw, err := os.ReadFile(filepath.Join(policyDir, SystemReasonPolicyFile))
	if err != nil {
		t.Fatalf("read policy: %v", err)
	}
	doc, err := ParseJSONStrict(raw)
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	obj := doc.(map[string]any)
	codes := obj["codes"].([]any)
	codes[0].(map[string]any)["spec_ref"] = "tampered by test"
	if err := os.WriteFile(filepath.Join(tampered, SystemReasonPolicyFile), marshalForTest(obj), 0o644); err != nil {
		t.Fatalf("write tampered policy: %v", err)
	}
	if _, err := LoadReasonPolicy(filepath.Join(tampered, SystemReasonPolicyFile)); err == nil {
		t.Errorf("tampered policy with stale registry_digest must fail closed")
	}
}

// TestParseExactRefDTOs_RejectLegacyAliases pins the anti-corruption surface:
// the new shared-contract ref parsers only understand the Contract §7 field
// names; the frozen legacy tracer wire-model aliases (skill_id, artifact_id,
// bare digest) are rejected instead of being aliased in.
func TestParseExactRefDTOs_RejectLegacyAliases(t *testing.T) {
	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	good := fmt.Sprintf(`{"schema_version":"gms.skill-artifact-ref.v2","lineage_id":"sg-x","version":3,"kind":"step_guidance","artifact_digest":%q}`, digest)
	ref, err := ParseSkillArtifactRef(parseObjectForTest(t, good))
	if err != nil {
		t.Fatalf("parse good ref: %v", err)
	}
	if ref.LineageID != "sg-x" || ref.Version != "3" || ref.Kind != "step_guidance" || ref.ArtifactDigest != digest {
		t.Errorf("parsed ref = %+v", ref)
	}

	for _, legacy := range []string{
		fmt.Sprintf(`{"schema_version":"gms.skill-artifact-ref.v2","skill_id":"sg-x","version":3,"kind":"step_guidance","artifact_digest":%q}`, digest),
		fmt.Sprintf(`{"schema_version":"gms.skill-artifact-ref.v2","lineage_id":"sg-x","version":3,"kind":"step_guidance","digest":%q}`, digest),
		`{"schema_version":"gms.skill-artifact-ref.v2","artifact_id":"a-1","version":3,"kind":"human_procedure","artifact_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}`,
		`{"schema_version":"gms.skill-artifact-ref.v2","lineage_id":"sg-x","version":1.5,"kind":"human_procedure","artifact_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}`,
		`{"schema_version":"gms.skill-artifact-ref.v2","lineage_id":"sg-x","version":1.5,"kind":"human_procedure","artifact_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}`,
	} {
		if _, err := ParseSkillArtifactRef(parseObjectForTest(t, legacy)); err == nil {
			t.Errorf("legacy/invalid ref must be rejected: %s", legacy)
		} else {
			var se *SchemaError
			if !errorsAs(err, &se) {
				t.Errorf("ref parse error must be a SchemaError with a closed reason code: %v", err)
			}
		}
	}
}

// TestContractPackage_AntiCorruptionNoLegacyImports enforces the GMS-101
// anti-corruption layer mechanically: internal/contract must not import the
// legacy internal/domain wire model or internal/skillproposal, and must never
// produce canonical bytes via encoding/json Marshal.
func TestContractPackage_AntiCorruptionNoLegacyImports(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	entries, err := os.ReadDir(wd)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	banned := map[string]bool{
		"river2.dev/graph-memory-service/internal/domain":        true,
		"river2.dev/graph-memory-service/internal/skillproposal": true,
	}
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(wd, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range file.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if banned[path] {
				t.Errorf("%s imports banned legacy package %s", name, path)
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "json" && sel.Sel.Name == "Marshal" {
					t.Errorf("%s uses json.Marshal; canonical bytes must only come from JCS", name)
				}
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no package sources found to guard")
	}
}

// --- test helpers -----------------------------------------------------------

func readFixtureBytes(t *testing.T, dir, rel string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, rel, "source.json"))
	if err != nil {
		t.Fatalf("read fixture %s: %v", rel, err)
	}
	return data
}

func readGoldenCanonical(t *testing.T, dir, rel string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, rel, "canonical.utf8"))
	if err != nil {
		t.Fatalf("read golden canonical %s: %v", rel, err)
	}
	return data
}

func parseObjectForTest(t *testing.T, raw string) map[string]any {
	t.Helper()
	v, err := ParseJSONStrict([]byte(raw))
	if err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	obj, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("parse %s: not an object", raw)
	}
	return obj
}

// marshalForTest re-serializes a parsed document for tamper tests. This is
// test-only: the package itself never marshals canonical bytes.
func marshalForTest(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func errorsAs(err error, target any) bool { return errors.As(err, target) }
