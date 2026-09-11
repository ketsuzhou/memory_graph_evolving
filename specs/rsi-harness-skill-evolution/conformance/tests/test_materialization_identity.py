"""CTR-001 materialization exact identity validator tests (Contract 6.2.1 v1.1).

Stdlib ``unittest`` only. The tests import the validator by explicit path
(like ``test_validate.py``) and exercise the golden materialization corpus
under ``conformance/materialization/`` plus throwaway temp copies, so the
golden fixtures are never mutated.

TDD anchor (implementation-plan CTR-001):
    Red   - test_materialization_identity.py::test_bundle_digest_is_not_manifest_ref_digest
            fails while ``validate_materialization_identity.py`` does not exist
            or accepts a bundle digest guessed into ``materialization_ref``.
    Green - minimal exact-ref/preimage validator.
    Refactor - shared Digest/VersionedRef checks; no RSIH private directory
            layout leaks into the frozen rules.
"""

import copy
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
MATERIALIZATION_ROOT = FIXTURE_ROOT / "materialization"

# Load the validator by explicit path so a dist-packages module with a similar
# name can never shadow it (same discipline as test_validate.py).
_SPEC = importlib.util.spec_from_file_location(
    "ctr001_validate_materialization_identity",
    FIXTURE_ROOT / "validate_materialization_identity.py",
)
mi = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = mi
_SPEC.loader.exec_module(mi)


def run_validator(root):
    """Run the validator CLI in-process; return (exit_code, stdout_text)."""
    buf = StringIO()
    with redirect_stdout(buf):
        code = mi.main(["--root", str(root)])
    return code, buf.getvalue()


def load_json(path):
    return json.loads(path.read_bytes())


def load_case(rel):
    """Return {'manifest': doc, 'lock': doc|None, 'expected': doc} for a case."""
    case_dir = MATERIALIZATION_ROOT / rel
    bundle_dir = case_dir / "bundle"
    bundle_files = None
    if bundle_dir.is_dir():
        bundle_files = {
            str(p.relative_to(bundle_dir).as_posix()): p.read_bytes()
            for p in sorted(bundle_dir.rglob("*"))
            if p.is_file()
        }
    return {
        "dir": case_dir,
        "manifest": load_json(case_dir / "manifest-document.json"),
        "lock": (
            load_json(case_dir / "lock.json")
            if (case_dir / "lock.json").is_file()
            else None
        ),
        "conflicting": (
            load_json(case_dir / "manifest-document-conflicting.json")
            if (case_dir / "manifest-document-conflicting.json").is_file()
            else None
        ),
        "bundle_files": bundle_files,
        "expected": load_json(case_dir / "expected.json"),
    }


def sha256_hex(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


class SelfContainedJcsTests(unittest.TestCase):
    """The validator ships its own RFC 8785 JCS; pin its core behaviors."""

    def test_key_order_uses_utf16_code_units(self):
        out = mi.jcs({"\uff04": 3, "\U0001d11e": 2})
        self.assertEqual(out, b'{"\xf0\x9d\x84\x9e":2,"\xef\xbc\x84":3}')

    def test_string_escaping_and_raw_non_ascii(self):
        self.assertEqual(
            mi.jcs({"k": 'a\u0007\t"c\\d'}),
            b'{"k":"a\\u0007\\t\\"c\\\\d"}',
        )
        self.assertEqual(mi.jcs({"k": "中文"}), '{"k":"中文"}'.encode("utf-8"))

    def test_rejects_non_integer_numbers(self):
        with self.assertRaises(mi.CanonicalizationError) as ctx:
            mi.jcs({"x": 1.5})
        self.assertEqual(ctx.exception.reason_code, "NON_INTEGER_NUMBER")

    def test_integers_are_exact_decimal_digits(self):
        self.assertEqual(mi.jcs({"n": 9007199254740993}), b'{"n":9007199254740993}')


class BundleDigestIsNotManifestRefDigestTests(unittest.TestCase):
    """The plan-mandated Red test (CTR-001 TDD anchor)."""

    def test_bundle_digest_is_not_manifest_ref_digest(self):
        # (a) Golden negative: the lock guesses the bundle-layer digest into
        #     materialization_ref.digest and must be rejected with the precise
        #     closed code, not accepted and not a generic mismatch.
        case = load_case("negative/mat-neg-001-bundle-digest-as-ref")
        outcome = mi.derive_case_outcome(case["dir"])
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "BUNDLE_DIGEST_NOT_MANIFEST_REF")
        # The guess really was the bundle digest (the fixture is honest).
        self.assertEqual(
            case["lock"]["materialization_ref"]["digest"],
            case["manifest"]["bundle_digest"],
        )
        self.assertNotEqual(
            case["manifest"]["bundle_digest"], outcome.manifest_digest
        )

        # (b) In-memory: an otherwise valid positive lock, mutated only by
        #     swapping materialization_ref.digest to the bundle digest, flips
        #     from accept to BUNDLE_DIGEST_NOT_MANIFEST_REF.
        base = load_case("lock-closure/lock-001-procedure-freeze")
        self.assertTrue(
            mi.derive_outcome(base["manifest"], base["lock"], None, base["bundle_files"]).accept
        )
        tampered = copy.deepcopy(base["lock"])
        tampered["materialization_ref"]["digest"] = base["manifest"]["bundle_digest"]
        outcome = mi.derive_outcome(
            base["manifest"], tampered, None, base["bundle_files"]
        )
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "BUNDLE_DIGEST_NOT_MANIFEST_REF")

        # (c) Guessing a per-file content digest is the same confusion.
        per_file = copy.deepcopy(base["lock"])
        per_file["materialization_ref"]["digest"] = base["manifest"]["files"][0][
            "content_digest"
        ]
        outcome = mi.derive_outcome(
            base["manifest"], per_file, None, base["bundle_files"]
        )
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "BUNDLE_DIGEST_NOT_MANIFEST_REF")

    def test_manifest_field_change_triggers_ref_mismatch(self):
        # (a) Golden negative: a manifest field changed after the lock was
        #     minted, so materialization_ref.digest no longer equals
        #     SHA-256(JCS(manifest_document)) -> MANIFEST_DIGEST_MISMATCH.
        case = load_case("negative/mat-neg-008-manifest-field-changed-ref-mismatch")
        outcome = mi.derive_case_outcome(case["dir"])
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "MANIFEST_DIGEST_MISMATCH")
        self.assertNotEqual(
            case["lock"]["materialization_ref"]["digest"], outcome.manifest_digest
        )

        # (b) In-memory: bump exactly one manifest preimage field of a valid
        #     positive case; only the ref digest is now stale.
        base = load_case("lock-closure/lock-001-procedure-freeze")
        drifted = copy.deepcopy(base["manifest"])
        drifted["render_profile_ref"]["version"] += 1
        self.assertNotEqual(
            mi.manifest_digest(drifted), mi.manifest_digest(base["manifest"])
        )
        outcome = mi.derive_outcome(drifted, base["lock"], None, base["bundle_files"])
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "MANIFEST_DIGEST_MISMATCH")


class PreimageSensitivityTests(unittest.TestCase):
    """Changing ANY preimage field must change the manifest digest."""

    def test_any_preimage_field_change_changes_manifest_digest(self):
        # Composite base: every collection field has >1 element, so order
        # mutations are real mutations (arrays are semantically ordered).
        base = load_case("lock-closure/lock-002-composite-freeze")["manifest"]
        self.assertTrue(len(base["files"]) >= 2)
        self.assertTrue(len(base["transitive_skill_refs"]) >= 2)
        base_digest = mi.manifest_digest(base)
        mutations = {
            "materialization_id": lambda d: d.update(materialization_id="mat-other"),
            "manifest_version": lambda d: d.update(manifest_version=d["manifest_version"] + 1),
            "skill.lineage_id": lambda d: d["skill"].update(lineage_id="proc-other"),
            "skill.version": lambda d: d["skill"].update(version=d["skill"]["version"] + 1),
            "skill.kind": lambda d: d["skill"].update(kind="step_guidance"),
            "root_skill_refs[0].artifact_digest": lambda d: d["root_skill_refs"][0].update(
                artifact_digest=sha256_hex(b"other-root")
            ),
            "transitive_skill_refs order": lambda d: d["transitive_skill_refs"].reverse(),
            "render_profile_ref.version": lambda d: d["render_profile_ref"].update(
                version=d["render_profile_ref"]["version"] + 1
            ),
            "render_profile_ref.digest": lambda d: d["render_profile_ref"].update(
                digest=sha256_hex(b"other-render")
            ),
            "permission_profile_ref.digest": lambda d: d["permission_profile_ref"].update(
                digest=sha256_hex(b"other-perm")
            ),
            "producer.producer_id": lambda d: d["producer"].update(producer_id="other"),
            "producer.producer_version": lambda d: d["producer"].update(
                producer_version=d["producer"]["producer_version"] + 1
            ),
            "files[0].path": lambda d: d["files"][0].update(path="skills/other/SKILL.md"),
            "files[0].content_digest": lambda d: d["files"][0].update(
                content_digest=sha256_hex(b"other-file")
            ),
            "files[0].size_bytes": lambda d: d["files"][0].update(
                size_bytes=d["files"][0]["size_bytes"] + 1
            ),
            "files order": lambda d: d["files"].reverse(),
            "file_count": lambda d: d.update(file_count=d["file_count"] + 1),
            "total_bytes": lambda d: d.update(total_bytes=d["total_bytes"] + 1),
            "bundle_digest": lambda d: d.update(bundle_digest=sha256_hex(b"other-bundle")),
            "created_from_activation_sequence": lambda d: d.update(
                created_from_activation_sequence=d["created_from_activation_sequence"] + 1
            ),
        }
        self.assertTrue(len(mutations) >= 18)
        for name, mutate in mutations.items():
            with self.subTest(field=name):
                mutated = copy.deepcopy(base)
                mutate(mutated)
                self.assertNotEqual(mi.manifest_digest(mutated), base_digest)

    def test_manifest_digest_is_sha256_of_jcs_canonical_bytes(self):
        case = load_case("identity/mat-identity-001-procedure")
        canonical = (case["dir"] / "manifest-document.canonical.utf8").read_bytes()
        self.assertEqual(canonical, mi.jcs(case["manifest"]))
        self.assertEqual(sha256_hex(canonical), mi.manifest_digest(case["manifest"]))
        self.assertEqual(
            sha256_hex(canonical), case["expected"]["expected_manifest_digest"]
        )


class ThreeLayerIdentityTests(unittest.TestCase):
    """Bundle bytes, manifest and lock identities cross-validate and differ."""

    def test_three_layers_cross_validate(self):
        case = load_case("lock-closure/lock-001-procedure-freeze")
        manifest = case["manifest"]
        lock = case["lock"]
        outcome = mi.derive_case_outcome(case["dir"])
        self.assertTrue(outcome.accept, outcome.reason_code)

        # Layer 1 - bundle bytes: per-file SHA-256 of the actual stored bytes.
        for entry in manifest["files"]:
            blob = case["bundle_files"][entry["path"]]
            self.assertEqual(sha256_hex(blob), entry["content_digest"])
            self.assertEqual(entry["size_bytes"], len(blob))

        # Layer 2 - manifest: digest of the closed manifest document, and the
        # bundle rollup digest derived from the file list alone.
        self.assertEqual(
            mi.bundle_rollup_digest(manifest["files"]), manifest["bundle_digest"]
        )
        self.assertEqual(
            outcome.manifest_digest, case["expected"]["expected_manifest_digest"]
        )
        self.assertEqual(
            manifest["bundle_digest"], case["expected"]["expected_bundle_digest"]
        )

        # Layer 3 - lock: references the manifest identity exactly and carries
        # its own digest over its own preimage.
        self.assertEqual(
            lock["materialization_ref"],
            {
                "id": manifest["materialization_id"],
                "version": manifest["manifest_version"],
                "digest": outcome.manifest_digest,
            },
        )
        self.assertEqual(
            outcome.lock_digest, case["expected"]["expected_lock_digest"]
        )
        # Independent hashlib check: the lock digest is the SHA-256 of the JCS
        # bytes of the lock document EXCLUDING lock_digest itself, while
        # lock.canonical.utf8 pins the full document bytes.
        canonical_lock = (case["dir"] / "lock.canonical.utf8").read_bytes()
        self.assertEqual(canonical_lock, mi.jcs(lock))
        preimage = {k: v for k, v in lock.items() if k != "lock_digest"}
        self.assertEqual(sha256_hex(mi.jcs(preimage)), outcome.lock_digest)

        # Cross-validation: every locked closure file is a manifest-declared
        # file with the same digest as the actual bundle bytes.
        by_path = {e["path"]: e["content_digest"] for e in manifest["files"]}
        for entry in lock["locked_closure"]:
            self.assertEqual(by_path[entry["materialized_path"]], entry["file_digest"])
            self.assertEqual(
                sha256_hex(case["bundle_files"][entry["materialized_path"]]),
                entry["file_digest"],
            )

    def test_three_layer_digests_are_pairwise_distinct(self):
        case = load_case("lock-closure/lock-001-procedure-freeze")
        manifest = case["manifest"]
        lock = case["lock"]
        per_file = [e["content_digest"] for e in manifest["files"]]
        manifest_digest = mi.manifest_digest(manifest)
        bundle_digest = mi.bundle_rollup_digest(manifest["files"])
        lock_digest = mi.lock_preimage_digest(lock)
        for digest in per_file:
            self.assertNotEqual(digest, manifest_digest)
            self.assertNotEqual(digest, bundle_digest)
            self.assertNotEqual(digest, lock_digest)
        self.assertNotEqual(manifest_digest, bundle_digest)
        self.assertNotEqual(manifest_digest, lock_digest)
        self.assertNotEqual(bundle_digest, lock_digest)

    def test_cache_lookup_reverification_beats_lock_only_comparison(self):
        # A cached copy whose manifest digest drifted must be rejected even
        # though the lock itself is perfectly self-consistent (lock-only
        # comparison is forbidden by Contract 6.2.1 v1.1).
        case = load_case("lock-closure/lock-001-procedure-freeze")
        drifted_manifest = copy.deepcopy(case["manifest"])
        # An opaque preimage field: the drifted copy stays internally
        # consistent (so only the re-verified manifest digest can catch it),
        # but its manifest_digest differs from the lock's frozen ref.
        drifted_manifest["render_profile_ref"]["digest"] = sha256_hex(b"drifted")
        self.assertTrue(
            mi.derive_case_outcome(case["dir"]).accept  # sanity: base accepted
        )
        outcome = mi.derive_outcome(
            drifted_manifest, case["lock"], None, case["bundle_files"]
        )
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "MANIFEST_DIGEST_MISMATCH")


class UniqueManifestIdentityTests(unittest.TestCase):
    """Same (manifest_id, manifest_version) maps to exactly one digest."""

    def test_same_identity_new_version_changes_digest(self):
        v1 = load_case("identity/mat-identity-001-procedure")["manifest"]
        v2 = load_case("identity/mat-identity-003-procedure-v2")["manifest"]
        self.assertEqual(
            v1["materialization_id"], v2["materialization_id"]
        )
        self.assertNotEqual(v1["manifest_version"], v2["manifest_version"])
        self.assertNotEqual(mi.manifest_digest(v1), mi.manifest_digest(v2))
        self.assertTrue(
            mi.derive_case_outcome(
                MATERIALIZATION_ROOT / "identity/mat-identity-001-procedure"
            ).accept
        )
        self.assertTrue(
            mi.derive_case_outcome(
                MATERIALIZATION_ROOT / "identity/mat-identity-003-procedure-v2"
            ).accept
        )

    def test_same_ref_different_bytes_is_rejected(self):
        case = load_case("negative/mat-neg-006-same-ref-different-bytes")
        conflicting = case["conflicting"]
        base = case["manifest"]
        self.assertEqual(
            (base["materialization_id"], base["manifest_version"]),
            (conflicting["materialization_id"], conflicting["manifest_version"]),
        )
        self.assertNotEqual(
            mi.manifest_digest(base), mi.manifest_digest(conflicting)
        )
        outcome = mi.derive_case_outcome(case["dir"])
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "SAME_REF_DIFFERENT_BYTES")

    def test_positive_and_composite_kinds_exist(self):
        for rel in (
            "identity/mat-identity-001-procedure",
            "identity/mat-identity-002-composite",
        ):
            case = load_case(rel)
            self.assertTrue(
                mi.derive_case_outcome(case["dir"]).accept,
                "%s must be accepted" % rel,
            )
        self.assertEqual(
            load_case("identity/mat-identity-001-procedure")["manifest"]["skill"][
                "kind"
            ],
            "human_procedure",
        )
        self.assertEqual(
            load_case("identity/mat-identity-002-composite")["manifest"]["skill"][
                "kind"
            ],
            "composite",
        )


class FailClosedNegativeTests(unittest.TestCase):
    """Each fail-closed rule of Contract 6.2.1 v1.1 has a golden negative."""

    NEGATIVES = {
        "negative/mat-neg-001-bundle-digest-as-ref": "BUNDLE_DIGEST_NOT_MANIFEST_REF",
        "negative/mat-neg-002-missing-manifest-version": "MANIFEST_VERSION_MISSING",
        "negative/mat-neg-003-partial-manifest": "MANIFEST_PARTIAL",
        "negative/mat-neg-004-stale-freeze-sequence": "MANIFEST_SEQUENCE_STALE",
        "negative/mat-neg-005-lock-closure-missing-node": "LOCK_CLOSURE_INCOMPLETE",
        "negative/mat-neg-006-same-ref-different-bytes": "SAME_REF_DIFFERENT_BYTES",
        "negative/mat-neg-007-unknown-required-extension": "UNKNOWN_REQUIRED_EXTENSION",
        "negative/mat-neg-008-manifest-field-changed-ref-mismatch": "MANIFEST_DIGEST_MISMATCH",
    }

    def test_every_negative_case_rejects_with_its_frozen_code(self):
        self.assertTrue(len(self.NEGATIVES) >= 6)
        for rel, code in self.NEGATIVES.items():
            with self.subTest(case=rel):
                case = load_case(rel)
                outcome = mi.derive_case_outcome(case["dir"])
                self.assertFalse(outcome.accept)
                self.assertEqual(outcome.reason_code, code)

    def test_partial_manifest_fixture_is_internally_inconsistent(self):
        case = load_case("negative/mat-neg-003-partial-manifest")
        manifest = case["manifest"]
        self.assertNotEqual(manifest["file_count"], len(manifest["files"]))
        self.assertNotEqual(
            manifest["total_bytes"], sum(f["size_bytes"] for f in manifest["files"])
        )

    def test_stale_sequence_fixture_really_stale(self):
        case = load_case("negative/mat-neg-004-stale-freeze-sequence")
        self.assertLess(
            case["lock"]["activation_sequence_at_freeze"],
            case["manifest"]["created_from_activation_sequence"],
        )

    def test_missing_closure_node_fixture_really_missing(self):
        case = load_case("negative/mat-neg-005-lock-closure-missing-node")
        manifest_paths = {e["path"] for e in case["manifest"]["files"]}
        lock_paths = {
            e["materialized_path"] for e in case["lock"]["locked_closure"]
        }
        self.assertTrue(manifest_paths - lock_paths)


class LegacyCompatibilityTests(unittest.TestCase):
    """Old/new schema compatibility conclusion: legacy_unlocked, fail-closed."""

    def test_legacy_manifest_without_manifest_version_fails_closed(self):
        case = load_case("negative/mat-neg-002-missing-manifest-version")
        self.assertNotIn("manifest_version", case["manifest"])
        outcome = mi.derive_case_outcome(case["dir"])
        self.assertFalse(outcome.accept)
        self.assertEqual(outcome.reason_code, "MANIFEST_VERSION_MISSING")
        self.assertTrue(outcome.legacy)

    def test_modern_manifest_is_not_classified_legacy(self):
        case = load_case("identity/mat-identity-001-procedure")
        outcome = mi.derive_case_outcome(case["dir"])
        self.assertTrue(outcome.accept)
        self.assertFalse(outcome.legacy)


class GoldenCorpusTests(unittest.TestCase):
    """End-to-end runs against the golden materialization corpus itself."""

    def test_golden_corpus_validates_end_to_end(self):
        code, out = run_validator(MATERIALIZATION_ROOT)
        self.assertEqual(code, 0, out)

    def test_reason_registry_is_closed_and_covers_frozen_codes(self):
        for code in (
            "MANIFEST_VERSION_MISSING",
            "BUNDLE_DIGEST_NOT_MANIFEST_REF",
            "MANIFEST_DIGEST_MISMATCH",
            "MANIFEST_PARTIAL",
            "MANIFEST_SEQUENCE_STALE",
            "LOCK_CLOSURE_INCOMPLETE",
            "SAME_REF_DIFFERENT_BYTES",
            "UNKNOWN_REQUIRED_EXTENSION",
            "DIGEST_MISMATCH",
            "REF_MISMATCH",
        ):
            self.assertIn(code, mi.REASON_CODES)
        # The registry is closed: a corpus expectation outside it is an error.

    def test_sub_manifest_declares_all_cases_with_categories(self):
        sub = load_json(MATERIALIZATION_ROOT / "manifest.json")
        self.assertEqual(sub["schema_version"], mi.MANIFEST_SCHEMA_VERSION)
        self.assertEqual(sub["contract_schema_version"], mi.CONTRACT_SCHEMA_VERSION)
        categories = {case["category"] for case in sub["cases"]}
        self.assertEqual(categories, {"identity", "lock-closure", "negative"})
        ids = [case["case_id"] for case in sub["cases"]]
        self.assertEqual(len(ids), len(set(ids)))

    def test_case_files_match_canonical_bytes(self):
        for rel in (
            "identity/mat-identity-001-procedure",
            "identity/mat-identity-002-composite",
            "identity/mat-identity-003-procedure-v2",
            "lock-closure/lock-001-procedure-freeze",
            "lock-closure/lock-002-composite-freeze",
        ):
            with self.subTest(case=rel):
                case = load_case(rel)
                self.assertEqual(
                    (case["dir"] / "manifest-document.canonical.utf8").read_bytes(),
                    mi.jcs(case["manifest"]),
                )


class TempCorpusTests(unittest.TestCase):
    """Corpus tampering on throwaway copies; golden files untouched."""

    def _temp_copy(self, mutate=None):
        tmp = Path(tempfile.mkdtemp(prefix="ctr001-mat-"))
        try:
            shutil.copytree(MATERIALIZATION_ROOT, tmp / "materialization")
            if mutate is not None:
                mutate(tmp / "materialization")
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        return tmp

    def test_tampered_bundle_bytes_fail_with_digest_mismatch(self):
        def tamper(root):
            path = root / "identity/mat-identity-001-procedure/bundle/skills/proc-release-notes/SKILL.md"
            data = bytearray(path.read_bytes())
            data[0] = data[0] ^ 0x01
            path.write_bytes(bytes(data))

        tmp = self._temp_copy(tamper)
        try:
            code, out = run_validator(tmp / "materialization")
            self.assertNotEqual(code, 0)
            self.assertIn("mat-identity-001-procedure", out)
            self.assertIn("DIGEST_MISMATCH", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_wrong_expected_reason_fails(self):
        def wrong_reason(root):
            path = root / "negative/mat-neg-001-bundle-digest-as-ref/expected.json"
            expected = load_json(path)
            expected["expected_reason_code"] = "MANIFEST_DIGEST_MISMATCH"
            path.write_bytes(mi.jcs(expected) + b"\n")

        tmp = self._temp_copy(wrong_reason)
        try:
            code, out = run_validator(tmp / "materialization")
            self.assertNotEqual(code, 0)
            self.assertIn("mat-neg-001", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_validator_never_modifies_corpus_files(self):
        def fingerprint(root):
            return {
                str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(root.rglob("*"))
                if p.is_file()
            }

        tmp = self._temp_copy()
        try:
            before = fingerprint(tmp / "materialization")
            code, out = run_validator(tmp / "materialization")
            self.assertEqual(code, 0, out)
            self.assertEqual(before, fingerprint(tmp / "materialization"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_sub_manifest_is_corpus_error(self):
        tmp = self._temp_copy()
        (tmp / "materialization" / "manifest.json").unlink()
        try:
            code, out = run_validator(tmp / "materialization")
            self.assertEqual(code, 2)
            self.assertIn("manifest.json", out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
