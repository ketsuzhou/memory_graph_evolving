package usageprojection

import (
	"context"
	"sort"

	"river2.dev/graph-memory-service/internal/contract"
)

// CoUsageProfileEvidence is the privacy-safe, profile-scoped read model used
// by Arm-A composition drafting. It is observational evidence only and never
// mutates the normative Skill Path Graph.
type CoUsageProfileEvidence struct {
	ContextProfile  ContextProfile
	Sources         []CoUsageSourceEvidence
	Counterevidence []contract.EvidenceRef
}

// CoUsageSourceEvidence combines explicit adoption interaction evidence with
// diagnosis utility evidence for one exact released Skill revision in one
// derived Context Profile.
type CoUsageSourceEvidence struct {
	SkillRef                contract.SkillArtifactRef
	SourceLineageID         string
	AdoptionEvidenceRefs    []contract.EvidenceRef
	DiagnosisEvidenceRefs   []contract.EvidenceRef
	ContributionScoreMicros int64
	ConfidenceMicros        int64
}

// CoUsageEvidence returns only non-diagnostic, exact Skill-revision evidence.
// Candidate observations and held-out evaluation records are deliberately
// excluded so callers cannot use advisory or test observations to draft an
// authoritative composition path.
func (s *UsageProjectionService) CoUsageEvidence(ctx context.Context) []CoUsageProfileEvidence {
	snapshot, err := s.store.Read(ctx)
	if err != nil {
		return nil
	}

	profiles := make(map[string]*coUsageProfileAggregate)
	for _, interaction := range snapshot.Interactions {
		if interaction.DiagnosticOnly || interaction.Stage != InteractionAdopted || interaction.Subject.SkillRef == nil || interaction.Subject.CandidateRef != nil {
			continue
		}
		profile := ensureCoUsageProfile(profiles, interaction.ContextProfile)
		source := profile.source(interaction.Subject.SkillRef, interaction.SourceLineageID)
		source.adoptionRefs = append(source.adoptionRefs, interaction.EvidenceRefs...)
	}
	for _, assessment := range snapshot.Assessments {
		if assessment.DiagnosticOnly {
			continue
		}
		profile := ensureCoUsageProfile(profiles, assessment.ContextProfile)
		profile.counterevidence = append(profile.counterevidence, assessment.CounterevidenceRefs...)
		if assessment.Subject.SkillRef == nil || assessment.Subject.CandidateRef != nil {
			continue
		}
		source := profile.source(assessment.Subject.SkillRef, assessment.SourceLineageID)
		source.diagnosisRefs = append(source.diagnosisRefs, assessment.EvidenceRefs...)
		source.scoreTotal += assessment.ContributionScoreMicros
		source.confidenceTotal += assessment.ConfidenceMicros
		source.assessmentCount++
	}

	result := make([]CoUsageProfileEvidence, 0, len(profiles))
	for _, profile := range profiles {
		sources := make([]CoUsageSourceEvidence, 0, len(profile.sources))
		for _, source := range profile.sources {
			if len(source.adoptionRefs) == 0 || source.assessmentCount == 0 {
				continue
			}
			sources = append(sources, CoUsageSourceEvidence{
				SkillRef:                *source.skillRef,
				SourceLineageID:         source.sourceLineageID,
				AdoptionEvidenceRefs:    append([]contract.EvidenceRef(nil), source.adoptionRefs...),
				DiagnosisEvidenceRefs:   append([]contract.EvidenceRef(nil), source.diagnosisRefs...),
				ContributionScoreMicros: source.scoreTotal / source.assessmentCount,
				ConfidenceMicros:        source.confidenceTotal / source.assessmentCount,
			})
		}
		if len(sources) == 0 {
			continue
		}
		sort.Slice(sources, func(i, j int) bool { return skillRefKey(sources[i].SkillRef) < skillRefKey(sources[j].SkillRef) })
		result = append(result, CoUsageProfileEvidence{
			ContextProfile:  profile.contextProfile,
			Sources:         sources,
			Counterevidence: append([]contract.EvidenceRef(nil), profile.counterevidence...),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ContextProfile.Digest() < result[j].ContextProfile.Digest() })
	return result
}

type coUsageProfileAggregate struct {
	contextProfile  ContextProfile
	sources         map[string]*coUsageSourceAggregate
	counterevidence []contract.EvidenceRef
}

type coUsageSourceAggregate struct {
	skillRef        *contract.SkillArtifactRef
	sourceLineageID string
	adoptionRefs    []contract.EvidenceRef
	diagnosisRefs   []contract.EvidenceRef
	scoreTotal      int64
	confidenceTotal int64
	assessmentCount int64
}

func ensureCoUsageProfile(profiles map[string]*coUsageProfileAggregate, contextProfile ContextProfile) *coUsageProfileAggregate {
	key := contextProfile.Digest()
	if profile := profiles[key]; profile != nil {
		return profile
	}
	profile := &coUsageProfileAggregate{contextProfile: contextProfile, sources: make(map[string]*coUsageSourceAggregate)}
	profiles[key] = profile
	return profile
}

func (p *coUsageProfileAggregate) source(ref *contract.SkillArtifactRef, lineage string) *coUsageSourceAggregate {
	key := skillRefKey(*ref)
	if source := p.sources[key]; source != nil {
		return source
	}
	source := &coUsageSourceAggregate{skillRef: ref, sourceLineageID: lineage}
	p.sources[key] = source
	return source
}

func skillRefKey(ref contract.SkillArtifactRef) string {
	return ref.LineageID + ":" + ref.Version + ":" + ref.ArtifactDigest
}
