package contract

// Shared DTO views over the generic canonical Value tree (Contract §7).
//
// The canonical core (canonical.go) is deliberately generic: validators
// dispatch over arbitrary JSON so that unknown shapes fail closed rather
// than round-peg into Go structs. This file provides the typed adapter
// layer the Host uses on the transport side: schema-version markers plus
// best-effort typed views for the DTOs the corpus exercises. Every As*
// constructor is total on its schema marker (never panics, never invents
// missing fields); fields that a DTO does not carry keep their zero value
// and a false Has* flag.

// Frozen schema_version markers (Contract §7, §16 fixtures).
const (
	SchemaSkillArtifactRef   = "gms.skill-artifact-ref.v1"
	SchemaCandidateRef       = "gms.candidate-artifact-ref.v1"
	SchemaEvidenceRef        = "gms.evidence-ref.v1"
	SchemaActivationEvent    = "gms.activation-event.v1"
	SchemaMergeProposalEvent = "gms.merge-proposal-event.v1"
	SchemaSimilarityAssess   = "gms.similarity-assessment.v1"
)

// RefIdentity is the exact identity triple of a SkillArtifactRef
// (Contract §6.2/§7.4): (lineage_id, version, artifact_digest). Two refs
// sharing an identity must agree on every other field, kind included.
type RefIdentity struct {
	LineageID      string
	Version        Number // literal preserved; integer-form at this layer
	ArtifactDigest string
}

// SkillArtifactRef is the exact released-skill reference DTO.
type SkillArtifactRef struct {
	SchemaVersion  string
	LineageID      string
	HasLineageID   bool
	Version        Number
	HasVersion     bool
	Kind           Value // kind drift compares raw values; usually String
	ArtifactDigest string
}

// AsSkillArtifactRef views v as a SkillArtifactRef. ok requires an object
// carrying the gms.skill-artifact-ref.v1 marker.
func AsSkillArtifactRef(v Value) (SkillArtifactRef, bool) {
	obj, ok := v.(*Object)
	if !ok {
		return SkillArtifactRef{}, false
	}
	sv, _ := StringOf(obj, "schema_version")
	if sv != SchemaSkillArtifactRef {
		return SkillArtifactRef{}, false
	}
	ref := SkillArtifactRef{SchemaVersion: sv}
	ref.LineageID, ref.HasLineageID = StringOf(obj, "lineage_id")
	if version, present := obj.Get("version"); present {
		if n, isNum := version.(Number); isNum {
			ref.Version, ref.HasVersion = n, true
		}
	}
	if kind, present := obj.Get("kind"); present {
		ref.Kind = kind
	}
	ref.ArtifactDigest, _ = StringOf(obj, "artifact_digest")
	return ref, true
}

// Identity returns the exact-ref identity triple of the ref.
func (r SkillArtifactRef) Identity() RefIdentity {
	return RefIdentity{LineageID: r.LineageID, Version: r.Version, ArtifactDigest: r.ArtifactDigest}
}

// IsExact reports whether the ref carries the full exact form: integer
// version and artifact digest present (Contract §5.1.5).
func (r SkillArtifactRef) IsExact() bool {
	return r.HasVersion && r.Version.IsInteger() && r.ArtifactDigest != ""
}

// MergeProposalEvent is the §9.4 proposal state-machine event DTO.
type MergeProposalEvent struct {
	SchemaVersion string
	FromState     string
	HasFromState  bool
	ToState       string
	HasToState    bool
}

// AsMergeProposalEvent views v as a MergeProposalEvent (schema marker required).
func AsMergeProposalEvent(v Value) (MergeProposalEvent, bool) {
	obj, ok := v.(*Object)
	if !ok {
		return MergeProposalEvent{}, false
	}
	sv, _ := StringOf(obj, "schema_version")
	if sv != SchemaMergeProposalEvent {
		return MergeProposalEvent{}, false
	}
	ev := MergeProposalEvent{SchemaVersion: sv}
	ev.FromState, ev.HasFromState = StringOf(obj, "from_state")
	ev.ToState, ev.HasToState = StringOf(obj, "to_state")
	return ev, true
}

// SimilarityAssessment is the §15.2 MT1 similarity DTO.
type SimilarityAssessment struct {
	SchemaVersion string
	Band          string
	HasBand       bool
}

// AsSimilarityAssessment views v as a SimilarityAssessment (schema marker required).
func AsSimilarityAssessment(v Value) (SimilarityAssessment, bool) {
	obj, ok := v.(*Object)
	if !ok {
		return SimilarityAssessment{}, false
	}
	sv, _ := StringOf(obj, "schema_version")
	if sv != SchemaSimilarityAssess {
		return SimilarityAssessment{}, false
	}
	sa := SimilarityAssessment{SchemaVersion: sv}
	sa.Band, sa.HasBand = StringOf(obj, "band")
	return sa, true
}

// CandidateArtifactRef is the §7.4 candidate reference: exact via
// (candidate_id, body_digest), intentionally versionless.
type CandidateArtifactRef struct {
	SchemaVersion string
	CandidateID   string
	HasCandidate  bool
	BodyDigest    string
}

// AsCandidateArtifactRef views v as a CandidateArtifactRef (schema marker required).
func AsCandidateArtifactRef(v Value) (CandidateArtifactRef, bool) {
	obj, ok := v.(*Object)
	if !ok {
		return CandidateArtifactRef{}, false
	}
	sv, _ := StringOf(obj, "schema_version")
	if sv != SchemaCandidateRef {
		return CandidateArtifactRef{}, false
	}
	cr := CandidateArtifactRef{SchemaVersion: sv}
	cr.CandidateID, cr.HasCandidate = StringOf(obj, "candidate_id")
	cr.BodyDigest, _ = StringOf(obj, "body_digest")
	return cr, true
}
