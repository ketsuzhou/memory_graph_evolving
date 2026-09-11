// Merge-provenance and conflict-closure gates of GMS-202 (for the GMS-208
// merge path): every merge candidate must carry dual exact source refs, at
// least one similarity assessment that is not below the suggestion band,
// and a fully resolved conflict set — a blocking conflict left unresolved
// can never produce a candidate.
package candidate

import (
	"river2.dev/graph-memory-service/internal/contract"
)

// Shared schema files of the merge DTOs (CTR-005 authority).
const (
	SchemaSimilarityAssessment = "similarity-assessment.schema.json"
	SchemaMergeProposal        = "merge-proposal.schema.json"
)

// MergeProvenance is the coverage summary a merge candidate must show:
// exactly two source skill refs, at least one similarity assessment ref,
// and the conflict closure of the merge proposal.
type MergeProvenance struct {
	SourceRefs     []contract.SkillArtifactRef
	AssessmentRefs []contract.VersionedRef
	Conflicts      []map[string]any
}

// CheckSimilarityAssessment runs the closed merge-category gate over one
// gms.similarity-assessment.v1 document (validate.py parity: semantic
// overlay first, then shape) and fails with SIMILARITY_BELOW_THRESHOLD
// when the band is below the merge threshold: below-band pairs may only
// raise a suggestion, never a merge proposal.
func (s *BindingService) CheckSimilarityAssessment(doc any) error {
	obj, ok := contract.AsObject(doc)
	if !ok {
		return newError(ReasonSimilarityBelowThreshold, "similarity assessment must be a JSON object")
	}
	if band, _ := contract.AsString(obj["band"]); band == "below_suggestion" {
		return newError(ReasonSimilarityBelowThreshold,
			"similarity band %q is below the merge threshold: the pair may only raise a model suggestion, never a merge proposal", band)
	}
	return s.proposals.Gates().ValidateShape(obj, SchemaSimilarityAssessment)
}

// CheckMergeClosure runs the closed merge-category gate over one
// gms.merge-proposal.v1 document (validate.py parity: semantic overlay
// first, then shape) and fails with MERGE_BLOCKING_CONFLICT when any
// conflict is blocking and its resolution action is unresolved: a
// candidate minted over an unresolved blocking conflict is impossible.
func (s *BindingService) CheckMergeClosure(doc any) error {
	obj, ok := contract.AsObject(doc)
	if !ok {
		return newError(ReasonMergeBlockingConflict, "merge proposal must be a JSON object")
	}
	if err := checkConflictClosure(obj["conflicts"]); err != nil {
		return err
	}
	return s.proposals.Gates().ValidateShape(obj, SchemaMergeProposal)
}

// CheckMergeProvenance verifies the provenance coverage a merge candidate
// must show (GMS §4.3/§4.5): exactly two source skill refs, at least one
// similarity assessment ref, and a fully resolved conflict set
// (MERGE_EVIDENCE_INCOMPLETE / MERGE_BLOCKING_CONFLICT otherwise).
func (s *BindingService) CheckMergeProvenance(p MergeProvenance) error {
	if len(p.SourceRefs) != 2 {
		return newError(ReasonMergeEvidenceIncomplete,
			"merge provenance needs exactly 2 source skill refs (dual parent lineages), got %d", len(p.SourceRefs))
	}
	if len(p.AssessmentRefs) < 1 {
		return newError(ReasonMergeEvidenceIncomplete,
			"merge provenance needs at least 1 similarity assessment ref, got 0")
	}
	for _, conflict := range p.Conflicts {
		if !boolField(conflict["blocking"]) {
			continue
		}
		resolution, _ := contract.AsObject(conflict["resolution"])
		action, _ := contract.AsString(resolution["action"])
		if action == "unresolved" || action == "" {
			return newError(ReasonMergeBlockingConflict,
				"blocking conflict %v has no protected resolution; a merge candidate is impossible until closed",
				conflict["conflict_id"])
		}
	}
	return nil
}

// boolField reads one boolean field (false when absent or non-boolean).
func boolField(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

// checkConflictClosure walks one merge proposal's conflicts array.
func checkConflictClosure(raw any) error {
	conflicts, _ := contract.AsArray(raw)
	for _, item := range conflicts {
		conflict, ok := contract.AsObject(item)
		if !ok {
			continue
		}
		blocking := boolField(conflict["blocking"])
		if !blocking {
			continue
		}
		resolution, _ := contract.AsObject(conflict["resolution"])
		action, _ := contract.AsString(resolution["action"])
		if action == "unresolved" || action == "" {
			return newError(ReasonMergeBlockingConflict,
				"blocking conflict %v is unresolved; a candidate cannot bind over an open conflict",
				conflict["conflict_id"])
		}
	}
	return nil
}
