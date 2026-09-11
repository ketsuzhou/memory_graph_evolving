// Package similarity is the GMS-208 MT1 similarity-assessment subsystem
// (GMS §7.1, §12.9; Contract §5.5, §6.3, §7.21).
//
// It owns the canonical source pair (Contract §6.3: symmetric pairs sort by
// (lineage_id UTF-8 bytes, version, artifact_digest) ascending, so A+B and
// B+A address the exact same pair), the versioned integer band thresholds
// (Contract §5.5.7: exact versioned policy ref and integer scaling, never
// floats) and the protected SimilarityAssessment mint: sources MUST be two
// distinct currently-active step_guidance heads, evidence MUST resolve
// committed, and the band is DERIVED from score_micros under the frozen
// policy — a below-threshold pair may only raise a suggestion and can never
// feed a merge proposal.
package similarity

import (
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
)

// Skill kind constants of the merge-eligible set (Contract §5.5.1: v1 only
// merges step_guidance).
const (
	KindStepGuidance = "step_guidance"
)

// CanonicalPair returns the two refs in Contract §6.3 canonical order:
// ascending by (lineage_id UTF-8 bytes, version integer value, artifact_digest).
// The same two refs in any input order always yield the same ordered pair.
func CanonicalPair(a, b contract.SkillArtifactRef) [2]contract.SkillArtifactRef {
	pair := [2]contract.SkillArtifactRef{a, b}
	if pairLess(pair[1], pair[0]) {
		pair[0], pair[1] = pair[1], pair[0]
	}
	return pair
}

// pairLess reports whether a sorts strictly before b under the §6.3 rule.
func pairLess(a, b contract.SkillArtifactRef) bool {
	if a.LineageID != b.LineageID {
		return a.LineageID < b.LineageID // UTF-8 byte order == Go string order
	}
	av, aok := a.VersionInt()
	bv, bok := b.VersionInt()
	if aok && bok && av.Cmp(bv) != 0 {
		return av.Cmp(bv) < 0
	}
	if a.Version != b.Version { // non-integer or mixed forms: exact digits
		return a.Version < b.Version
	}
	return a.ArtifactDigest < b.ArtifactDigest
}

// PairSorted reports whether refs (exactly 2) already sit in canonical order.
func PairSorted(refs []contract.SkillArtifactRef) bool {
	if len(refs) != 2 {
		return false
	}
	return !pairLess(refs[1], refs[0])
}

// SkillRefDoc renders one exact §7.3 ref as the decoder model (exact digits).
func SkillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         json.Number(ref.Version),
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

// SourcePairKey computes the Contract §7.22 dedup.source_pair_key: the
// JCS/SHA-256 of the canonically ordered exact refs (GMS §7.2). The key is
// computed from the normalized pair, so A+B and B+A share one key.
func SourcePairKey(pair [2]contract.SkillArtifactRef) (string, error) {
	digest, err := contract.DigestOf([]any{
		SkillRefDoc(pair[0]),
		SkillRefDoc(pair[1]),
	})
	if err != nil {
		return "", fmt.Errorf("similarity: source pair key: %w", err)
	}
	return digest, nil
}
