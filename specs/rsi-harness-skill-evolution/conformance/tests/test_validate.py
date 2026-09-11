"""FND-001 shared JCS/SHA-256 golden corpus validator tests.

Stdlib ``unittest`` only. Tests are self-contained: they import ``validate``
from the conformance root (parent of this tests directory) and build throwaway
corpora in ``tempfile`` directories so the golden corpus is never mutated.
"""

import hashlib
import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

FIXTURE_ROOT = Path(__file__).resolve().parents[1]
if str(FIXTURE_ROOT) not in sys.path:
    sys.path.insert(0, str(FIXTURE_ROOT))

# Load the validator by explicit path: a dist-packages "validate" package
# exists on this host and must never shadow the golden-corpus validator.
_SPEC = importlib.util.spec_from_file_location(
    "fnd001_validate", FIXTURE_ROOT / "validate.py"
)
validate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = validate  # dataclasses needs the module registered
_SPEC.loader.exec_module(validate)


def run_validator(root):
    """Run the validator CLI in-process; return (exit_code, stdout_text)."""
    buf = StringIO()
    with redirect_stdout(buf):
        code = validate.main(["--root", str(root)])
    return code, buf.getvalue()


def manifest_entry(case_id, category, case_dir):
    return {
        "case_id": case_id,
        "category": category,
        "source_path": "%s/source.json" % case_dir,
        "canonical_utf8_path": "%s/canonical.utf8" % case_dir,
        "canonical_base64_path": "%s/canonical.base64" % case_dir,
        "expected_path": "%s/expected.json" % case_dir,
    }


def build_temp_corpus(case_specs, mutate=None):
    """Copy golden case directories into a temp corpus with a small manifest.

    ``case_specs`` is a sequence of ``(category, case_dirname)`` tuples.
    ``mutate(root)`` is an optional hook applied after the copy.
    Returns the temp root Path (caller must clean up).
    """
    tmp = Path(tempfile.mkdtemp(prefix="fnd001-corpus-"))
    try:
        entries = []
        for category, dirname in case_specs:
            src = FIXTURE_ROOT / dirname
            dst = tmp / dirname
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src, dst)
            case_id = Path(dirname).name
            entries.append(manifest_entry(case_id, category, dirname))
        manifest = {
            "schema_version": validate.MANIFEST_SCHEMA_VERSION,
            "contract_schema_version": validate.CONTRACT_SCHEMA_VERSION,
            "cases": entries,
        }
        (tmp / "manifest.json").write_bytes(validate.jcs(manifest) + b"\n")
        if mutate is not None:
            mutate(tmp)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return tmp


class JcsPureFunctionTests(unittest.TestCase):
    """Unit tests for the RFC 8785 / Contract §6.1 canonicalizer."""

    def test_key_order_uses_utf16_code_units_not_codepoints(self):
        # U+FF04 (＄) is a BMP char; U+1D11E (𝄞) is supplementary. Codepoint
        # order puts ＄ first, but UTF-16 code-unit order (D834 < FF04) puts 𝄞
        # first. JCS/RFC 8785 requires UTF-16 code-unit order.
        out = validate.jcs({"\uff04": 3, "\U0001d11e": 2})
        expected = b'{"\xf0\x9d\x84\x9e":2,"\xef\xbc\x84":3}'
        self.assertEqual(out, expected)

    def test_ascii_vs_latin1_key_order(self):
        self.assertEqual(validate.jcs({"\u00e9": 1, "z": 0}), b'{"z":0,"\xc3\xa9":1}')

    def test_string_escaping_rfc8785(self):
        value = {"k": 'a\u0007\u0008\t\n\u000bc"\\d\rf'}
        expected = b'{"k":"a\\u0007\\b\\t\\n\\u000bc\\"\\\\d\\rf"}'
        self.assertEqual(validate.jcs(value), expected)

    def test_non_ascii_characters_are_emitted_as_raw_utf8(self):
        self.assertEqual(validate.jcs({"k": "中文"}), '{"k":"中文"}'.encode("utf-8"))
        self.assertEqual(validate.jcs({"k": "😊"}), '{"k":"😊"}'.encode("utf-8"))
        # U+007F (DEL) is >= 0x20 and must be emitted raw, not escaped.
        self.assertEqual(validate.jcs({"k": "x\x7f"}), b'{"k":"x\x7f"}')

    def test_no_unicode_normalization_of_combining_characters(self):
        precomposed = validate.jcs({"k": "é"})
        decomposed = validate.jcs({"k": "e\u0301"})
        self.assertNotEqual(precomposed, decomposed)

    def test_rejects_non_integer_numbers(self):
        for bad in ({"score": 0.5}, {"count": float(json.loads("1e2"))}, {"x": -0.25}):
            with self.assertRaises(validate.CanonicalizationError) as ctx:
                validate.jcs(bad)
            self.assertEqual(ctx.exception.reason_code, "NON_INTEGER_NUMBER")

    def test_integers_big_and_small_are_exact(self):
        value = {"zero": 0, "neg": -42, "big": 9007199254740993, "big64": 18446744073709551615}
        self.assertEqual(
            validate.jcs(value),
            b'{"big":9007199254740993,"big64":18446744073709551615,'
            b'"neg":-42,"zero":0}',
        )

    def test_empty_containers_and_array_order(self):
        self.assertEqual(validate.jcs({"e": {}, "a": []}), b'{"a":[],"e":{}}')
        self.assertEqual(validate.jcs([3, 1, 2]), b"[3,1,2]")


class GoldenCorpusTests(unittest.TestCase):
    """End-to-end tests against the golden corpus itself."""

    def test_manifest_requires_exact_bytes_digest_and_reason(self):
        # (a) Different input key order / Unicode escaping must produce the
        #     exact same canonical bytes, digest and length.
        ids = [
            "canonicalization/canon-001-key-order",
            "canonicalization/canon-007-same-bytes-pretty",
            "canonicalization/canon-008-same-bytes-escaped",
        ]
        blobs = [(FIXTURE_ROOT / cid / "canonical.utf8").read_bytes() for cid in ids]
        expecteds = [json.loads((FIXTURE_ROOT / cid / "expected.json").read_bytes()) for cid in ids]
        self.assertEqual(len(set(blobs)), 1)
        self.assertEqual(len({e["expected_digest"] for e in expecteds}), 1)
        self.assertEqual(len({e["expected_canonical_byte_length"] for e in expecteds}), 1)
        self.assertEqual(
            hashlib.sha256(blobs[0]).hexdigest(),
            expecteds[0]["expected_digest"].split(":", 1)[1],
        )

        # (b) Tampering a single byte of the canonical bytes must make the
        #     derived outcome a DIGEST_MISMATCH rejection, not an accept.
        source = (FIXTURE_ROOT / ids[0] / "source.json").read_bytes()
        tampered = bytearray(blobs[0])
        tampered[-1] = tampered[-1] ^ 0x01
        outcome = validate.derive_outcome(source, "canonicalization", bytes(tampered))
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "DIGEST_MISMATCH")
        self.assertNotEqual(
            hashlib.sha256(bytes(tampered)).hexdigest(),
            expecteds[0]["expected_digest"].split(":", 1)[1],
        )

        # (c) A negative case whose manifest/expected reason does not match
        #     the derived reason must fail the validator run.
        def wrong_reason(root):
            path = root / "refs/ref-neg-002-latest/expected.json"
            expected = json.loads(path.read_bytes())
            expected["expected_reason_code"] = "NON_INTEGER_NUMBER"  # wrong: derived is NON_EXACT_REF
            path.write_bytes(validate.jcs(expected) + b"\n")

        tmp = build_temp_corpus([("ref", "refs/ref-neg-002-latest")], mutate=wrong_reason)
        try:
            code, out = run_validator(tmp)
            self.assertNotEqual(code, 0)
            self.assertIn("ref-neg-002-latest", out)
            self.assertIn("reason", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_golden_corpus_validates_end_to_end(self):
        code, out = run_validator(FIXTURE_ROOT)
        self.assertEqual(code, 0, out)

    def test_golden_corpus_manifest_declares_expected_contract_versions(self):
        manifest = json.loads((FIXTURE_ROOT / "manifest.json").read_bytes())
        self.assertEqual(manifest["schema_version"], validate.MANIFEST_SCHEMA_VERSION)
        self.assertEqual(manifest["contract_schema_version"], validate.CONTRACT_SCHEMA_VERSION)
        case_ids = [c["case_id"] for c in manifest["cases"]]
        self.assertEqual(len(case_ids), len(set(case_ids)), "duplicate case ids")

    def test_negative_cases_cover_contract_reason_codes(self):
        manifest = json.loads((FIXTURE_ROOT / "manifest.json").read_bytes())
        reasons = set()
        for entry in manifest["cases"]:
            expected = json.loads((FIXTURE_ROOT / entry["expected_path"]).read_bytes())
            if expected["expected_accept"] is False:
                reasons.add(expected["expected_reason_code"])
        for code in (
            "NON_INTEGER_NUMBER",
            "UNKNOWN_REQUIRED_EXTENSION",
            "DIGEST_MISMATCH",
            "NON_EXACT_REF",
            "COMPOSITE_CYCLE",
            "PORT_SCHEMA_MISSING",
            "MERGE_BLOCKING_CONFLICT",
            "EVIDENCE_NOT_COMMITTED",
            "PROJECTION_SEQUENCE_GAP",
            "PERMISSION_CAP_EXCEEDED",
        ):
            self.assertIn(code, reasons)


class TempCorpusTests(unittest.TestCase):
    """Corpus-integrity tests on throwaway copies; golden files untouched."""

    def test_single_byte_tamper_in_canonical_file_fails(self):
        def tamper(root):
            path = root / "canonicalization/canon-001-key-order/canonical.utf8"
            data = bytearray(path.read_bytes())
            data[-1] = data[-1] ^ 0x01
            path.write_bytes(bytes(data))

        tmp = build_temp_corpus([("canonicalization", "canonicalization/canon-001-key-order")], mutate=tamper)
        try:
            code, out = run_validator(tmp)
            self.assertNotEqual(code, 0)
            self.assertIn("canon-001-key-order", out)
            self.assertIn("DIGEST_MISMATCH", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_negative_reason_mismatch_fails(self):
        def wrong_reason(root):
            path = root / "refs/ref-neg-001-naked-id/expected.json"
            expected = json.loads(path.read_bytes())
            expected["expected_reason_code"] = "DIGEST_MISMATCH"  # wrong: derived is NON_EXACT_REF
            path.write_bytes(validate.jcs(expected) + b"\n")

        tmp = build_temp_corpus([("ref", "refs/ref-neg-001-naked-id")], mutate=wrong_reason)
        try:
            code, out = run_validator(tmp)
            self.assertNotEqual(code, 0)
            self.assertIn("reason", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_duplicate_case_id_fails(self):
        def duplicate(root):
            path = root / "manifest.json"
            manifest = json.loads(path.read_bytes())
            manifest["cases"].append(dict(manifest["cases"][0]))
            path.write_bytes(validate.jcs(manifest) + b"\n")

        tmp = build_temp_corpus([("canonicalization", "canonicalization/canon-001-key-order")], mutate=duplicate)
        try:
            code, out = run_validator(tmp)
            self.assertNotEqual(code, 0)
            self.assertIn("duplicate", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_unknown_category_fails(self):
        def bogus_category(root):
            path = root / "manifest.json"
            manifest = json.loads(path.read_bytes())
            manifest["cases"][0]["category"] = "pretty-print"
            path.write_bytes(validate.jcs(manifest) + b"\n")

        tmp = build_temp_corpus([("canonicalization", "canonicalization/canon-001-key-order")], mutate=bogus_category)
        try:
            code, out = run_validator(tmp)
            self.assertNotEqual(code, 0)
            self.assertIn("category", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_expected_digest_field_fails(self):
        def drop_digest(root):
            path = root / "canonicalization/canon-001-key-order/expected.json"
            expected = json.loads(path.read_bytes())
            del expected["expected_digest"]
            path.write_bytes(validate.jcs(expected) + b"\n")

        tmp = build_temp_corpus([("canonicalization", "canonicalization/canon-001-key-order")], mutate=drop_digest)
        try:
            code, out = run_validator(tmp)
            self.assertNotEqual(code, 0)
            self.assertIn("expected_digest", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_expected_file_fails(self):
        def drop_file(root):
            (root / "canonicalization/canon-001-key-order/expected.json").unlink()

        tmp = build_temp_corpus([("canonicalization", "canonicalization/canon-001-key-order")], mutate=drop_file)
        try:
            code, out = run_validator(tmp)
            self.assertNotEqual(code, 0)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_small_synthetic_corpus_passes(self):
        specs = [
            ("canonicalization", "canonicalization/canon-001-key-order"),
            ("ref", "refs/ref-neg-001-naked-id"),
            ("negative", "negative/negative-001-digest-tamper"),
        ]
        tmp = build_temp_corpus(specs)
        try:
            code, out = run_validator(tmp)
            self.assertEqual(code, 0, out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_validator_never_modifies_corpus_files(self):
        specs = [
            ("canonicalization", "canonicalization/canon-001-key-order"),
            ("negative", "negative/negative-001-digest-tamper"),
        ]
        tmp = build_temp_corpus(specs)

        def fingerprint(root):
            return {
                str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(root.rglob("*"))
                if p.is_file()
            }

        try:
            before = fingerprint(tmp)
            code, out = run_validator(tmp)
            self.assertEqual(code, 0, out)
            self.assertEqual(before, fingerprint(tmp))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
