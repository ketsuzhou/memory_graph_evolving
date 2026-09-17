// Package usageprojection maintains the privacy-controlled, observational
// usage read model for exact Skill and Candidate revisions. It intentionally
// does not mutate the normative Skill Path Graph or grant execution authority.
package usageprojection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
)

const contextProfileSchemaV1 = "context-profile/1.0"

// ContextProfile is a versioned, privacy-safe derived task summary. It has no
// raw prompt, workspace content, or tool output field; source material stays
// behind exact evidence references recorded with observations.
type ContextProfile struct {
	SchemaVersion        string
	TaskFamily           string
	RuntimeClass         string
	WorkspaceFeatureTags []string
	ObservableGuardFacts []string
	ToolPolicyRef        contract.VersionedRef
	EnvironmentClass     string
}

// Digest returns the stable identity of this derived profile. Order in feature
// and guard lists is intentionally irrelevant to context identity.
func (p ContextProfile) Digest() string {
	features := sortedStrings(p.WorkspaceFeatureTags)
	guards := sortedStrings(p.ObservableGuardFacts)
	payload := struct {
		SchemaVersion string
		TaskFamily    string
		RuntimeClass  string
		Features      []string
		Guards        []string
		ToolPolicy    contract.VersionedRef
		Environment   string
	}{
		SchemaVersion: p.SchemaVersion,
		TaskFamily:    p.TaskFamily,
		RuntimeClass:  p.RuntimeClass,
		Features:      features,
		Guards:        guards,
		ToolPolicy:    p.ToolPolicyRef,
		Environment:   p.EnvironmentClass,
	}
	encoded, _ := json.Marshal(payload)
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

// UsageSubjectRef identifies exactly one observed subject: an immutable
// released Skill revision or an immutable, non-executable Candidate revision.
type UsageSubjectRef struct {
	SkillRef     *contract.SkillArtifactRef
	CandidateRef *contract.CandidateArtifactRef
}

// CanonicalKey returns a stable exact-ref key suitable only for projection
// grouping and deterministic ties. It never implies activation or execution.
func (r UsageSubjectRef) CanonicalKey() string {
	if r.SkillRef != nil && r.CandidateRef == nil {
		return strings.Join([]string{"skill", r.SkillRef.LineageID, r.SkillRef.Version, r.SkillRef.ArtifactDigest}, ":")
	}
	if r.CandidateRef != nil && r.SkillRef == nil {
		return strings.Join([]string{"candidate", r.CandidateRef.CandidateID, r.CandidateRef.BodyDigest}, ":")
	}
	return ""
}

func (r UsageSubjectRef) valid() bool { return r.CanonicalKey() != "" }

// InteractionStage records an observational stage. Earlier stages do not
// imply later stages, utility, causal contribution, or execution authority.
type InteractionStage string

const (
	InteractionMatched           InteractionStage = "matched"
	InteractionExposed           InteractionStage = "exposed"
	InteractionSelected          InteractionStage = "selected"
	InteractionAdopted           InteractionStage = "adopted"
	InteractionVerified          InteractionStage = "verified"
	InteractionOutcomeCorrelated InteractionStage = "outcome_correlated"
)

func (s InteractionStage) valid() bool {
	switch s {
	case InteractionMatched, InteractionExposed, InteractionSelected, InteractionAdopted, InteractionVerified, InteractionOutcomeCorrelated:
		return true
	default:
		return false
	}
}

// Interaction is one immutable usage observation between an exact subject and
// a privacy-safe Context Profile.
type Interaction struct {
	Subject           UsageSubjectRef
	ContextProfile    ContextProfile
	SourceLineageID   string
	Stage             InteractionStage
	EvidenceRefs      []contract.EvidenceRef
	PolicyRef         contract.VersionedRef
	ModelRef          contract.VersionedRef
	EnvironmentRef    contract.VersionedRef
	EvaluationBatchID string
	DiagnosticOnly    bool
}

// DiagnosisUtilityAssessment is a diagnosis-agent's observational assessment
// of one exact subject in one Context Profile. Scores are integer micros to
// preserve deterministic comparisons and avoid float rounding ambiguity.
type DiagnosisUtilityAssessment struct {
	AssessmentID            string
	Subject                 UsageSubjectRef
	ContextProfile          ContextProfile
	SourceLineageID         string
	ReturnedPathID          string
	AddressedAgentID        string
	AdoptionEvidenceRefs    []contract.EvidenceRef
	ContributionScoreMicros int64
	ConfidenceMicros        int64
	CounterevidenceRefs     []contract.EvidenceRef
	Rationale               string
	EvidenceRefs            []contract.EvidenceRef
	RubricRef               contract.VersionedRef
	EvaluationBatchID       string
	DiagnosticOnly          bool
}

// UsageRankingPolicy identifies the versioned, deterministic comparator used
// to produce observational-priority summaries.
type UsageRankingPolicy struct {
	Version string
}

// UsageProjectionService is the public seam for the separate usage read
// model. It delegates durable observation storage to UsageProjectionStore;
// it is not a normative graph, activation service, or runtime tool executor.
type UsageProjectionService struct {
	policy UsageRankingPolicy
	store  UsageProjectionStore
}

func NewUsageProjectionService(policy UsageRankingPolicy, store UsageProjectionStore) (*UsageProjectionService, error) {
	if strings.TrimSpace(policy.Version) == "" {
		return nil, fmt.Errorf("usage projection: ranking policy version is required")
	}
	if store == nil {
		return nil, fmt.Errorf("usage projection: durable store is required")
	}
	return &UsageProjectionService{policy: policy, store: store}, nil
}

func (s *UsageProjectionService) RecordInteraction(ctx context.Context, interaction Interaction) error {
	if err := validateInteraction(interaction); err != nil {
		return err
	}
	return s.store.AppendInteraction(ctx, interaction)
}

func validateInteraction(interaction Interaction) error {
	if !interaction.Subject.valid() {
		return fmt.Errorf("usage projection: interaction requires exactly one exact skill or candidate subject")
	}
	if err := validateContextProfile(interaction.ContextProfile); err != nil {
		return err
	}
	if !interaction.Stage.valid() {
		return fmt.Errorf("usage projection: unsupported interaction stage %q", interaction.Stage)
	}
	if strings.TrimSpace(interaction.SourceLineageID) == "" {
		return fmt.Errorf("usage projection: interaction source lineage is required")
	}
	return nil
}

func (s *UsageProjectionService) RecordDiagnosis(ctx context.Context, assessment DiagnosisUtilityAssessment) error {
	if err := validateDiagnosis(assessment); err != nil {
		return err
	}
	return s.store.AppendDiagnosis(ctx, assessment)
}

func validateDiagnosis(assessment DiagnosisUtilityAssessment) error {
	if strings.TrimSpace(assessment.AssessmentID) == "" || !assessment.Subject.valid() {
		return fmt.Errorf("usage projection: diagnosis assessment id and exact subject are required")
	}
	if err := validateContextProfile(assessment.ContextProfile); err != nil {
		return err
	}
	if strings.TrimSpace(assessment.ReturnedPathID) == "" || strings.TrimSpace(assessment.AddressedAgentID) == "" || strings.TrimSpace(assessment.Rationale) == "" {
		return fmt.Errorf("usage projection: diagnosis assessment requires returned path, addressed agent, and rationale")
	}
	if assessment.ContributionScoreMicros < 0 || assessment.ConfidenceMicros < 0 {
		return fmt.Errorf("usage projection: diagnosis scores must be non-negative")
	}
	return nil
}

func validateContextProfile(profile ContextProfile) error {
	if profile.SchemaVersion != contextProfileSchemaV1 {
		return fmt.Errorf("usage projection: unsupported context profile schema %q", profile.SchemaVersion)
	}
	if strings.TrimSpace(profile.TaskFamily) == "" || strings.TrimSpace(profile.RuntimeClass) == "" || strings.TrimSpace(profile.EnvironmentClass) == "" {
		return fmt.Errorf("usage projection: task family, runtime class, and environment class are required")
	}
	return nil
}

func cloneInteraction(interaction Interaction) Interaction {
	cloned := interaction
	cloned.ContextProfile.WorkspaceFeatureTags = append([]string(nil), interaction.ContextProfile.WorkspaceFeatureTags...)
	cloned.ContextProfile.ObservableGuardFacts = append([]string(nil), interaction.ContextProfile.ObservableGuardFacts...)
	cloned.EvidenceRefs = append([]contract.EvidenceRef(nil), interaction.EvidenceRefs...)
	return cloned
}

func cloneAssessment(assessment DiagnosisUtilityAssessment) DiagnosisUtilityAssessment {
	cloned := assessment
	cloned.ContextProfile.WorkspaceFeatureTags = append([]string(nil), assessment.ContextProfile.WorkspaceFeatureTags...)
	cloned.ContextProfile.ObservableGuardFacts = append([]string(nil), assessment.ContextProfile.ObservableGuardFacts...)
	cloned.AdoptionEvidenceRefs = append([]contract.EvidenceRef(nil), assessment.AdoptionEvidenceRefs...)
	cloned.CounterevidenceRefs = append([]contract.EvidenceRef(nil), assessment.CounterevidenceRefs...)
	cloned.EvidenceRefs = append([]contract.EvidenceRef(nil), assessment.EvidenceRefs...)
	return cloned
}

// UsageSummaryRequest optionally narrows a query to one derived Context
// Profile. Diagnostic-only held-out evaluation records are omitted unless the
// caller explicitly requests a diagnostic view.
type UsageSummaryRequest struct {
	ContextProfile    *ContextProfile
	IncludeDiagnostic bool
}

// UsageSummary is an auditable, observational ranking. It contains no
// activation, promotion, applicability, dependency, or execution field.
type UsageSummary struct {
	PolicyVersion string
	Entries       []UsageSummaryEntry
}

// UsageSummaryEntry explains the deterministic rank of one exact subject.
type UsageSummaryEntry struct {
	Subject                        UsageSubjectRef
	AverageContributionScoreMicros int64
	AverageConfidenceMicros        int64
	IndependentContextCount        int
	IndependentLineageCount        int
	AdoptedCount                   int
	VerifiedCount                  int
	OutcomeCorrelatedCount         int
	CounterevidenceCount           int
	DeduplicatedReuseCount         int
}

func (s *UsageProjectionService) UsageSummary(ctx context.Context, request UsageSummaryRequest) (UsageSummary, error) {
	if request.ContextProfile != nil {
		if err := validateContextProfile(*request.ContextProfile); err != nil {
			return UsageSummary{}, err
		}
	}
	filterDigest := ""
	if request.ContextProfile != nil {
		filterDigest = request.ContextProfile.Digest()
	}
	snapshot, err := s.store.Read(ctx)
	if err != nil {
		return UsageSummary{}, err
	}

	entries := map[string]*usageAggregate{}
	for _, interaction := range snapshot.Interactions {
		if !includeObservation(interaction.ContextProfile.Digest(), interaction.DiagnosticOnly, filterDigest, request.IncludeDiagnostic) {
			continue
		}
		key := interaction.Subject.CanonicalKey()
		aggregate := ensureAggregate(entries, key, interaction.Subject)
		aggregate.contexts[interaction.ContextProfile.Digest()] = struct{}{}
		aggregate.lineages[interaction.SourceLineageID] = struct{}{}
		switch interaction.Stage {
		case InteractionAdopted:
			aggregate.adopted++
		case InteractionVerified:
			aggregate.verified++
		case InteractionOutcomeCorrelated:
			aggregate.outcomes++
		}
	}
	for _, assessment := range snapshot.Assessments {
		if !includeObservation(assessment.ContextProfile.Digest(), assessment.DiagnosticOnly, filterDigest, request.IncludeDiagnostic) {
			continue
		}
		key := assessment.Subject.CanonicalKey()
		aggregate := ensureAggregate(entries, key, assessment.Subject)
		aggregate.contexts[assessment.ContextProfile.Digest()] = struct{}{}
		if assessment.SourceLineageID != "" {
			aggregate.lineages[assessment.SourceLineageID] = struct{}{}
		}
		aggregate.scoreTotal += assessment.ContributionScoreMicros
		aggregate.confidenceTotal += assessment.ConfidenceMicros
		aggregate.assessmentCount++
		aggregate.counterevidence += len(assessment.CounterevidenceRefs)
	}

	result := UsageSummary{PolicyVersion: s.policy.Version, Entries: make([]UsageSummaryEntry, 0, len(entries))}
	for _, aggregate := range entries {
		entry := aggregate.summary()
		result.Entries = append(result.Entries, entry)
	}
	sort.Slice(result.Entries, func(i, j int) bool { return ranksBefore(result.Entries[i], result.Entries[j]) })
	return result, nil
}

func includeObservation(profileDigest string, diagnosticOnly bool, filterDigest string, includeDiagnostic bool) bool {
	if filterDigest != "" && profileDigest != filterDigest {
		return false
	}
	return includeDiagnostic || !diagnosticOnly
}

type usageAggregate struct {
	subject         UsageSubjectRef
	contexts        map[string]struct{}
	lineages        map[string]struct{}
	scoreTotal      int64
	confidenceTotal int64
	assessmentCount int64
	adopted         int
	verified        int
	outcomes        int
	counterevidence int
}

func ensureAggregate(entries map[string]*usageAggregate, key string, subject UsageSubjectRef) *usageAggregate {
	if aggregate := entries[key]; aggregate != nil {
		return aggregate
	}
	aggregate := &usageAggregate{subject: subject, contexts: make(map[string]struct{}), lineages: make(map[string]struct{})}
	entries[key] = aggregate
	return aggregate
}

func (a *usageAggregate) summary() UsageSummaryEntry {
	entry := UsageSummaryEntry{
		Subject:                 a.subject,
		IndependentContextCount: len(a.contexts),
		IndependentLineageCount: len(a.lineages),
		AdoptedCount:            a.adopted,
		VerifiedCount:           a.verified,
		OutcomeCorrelatedCount:  a.outcomes,
		CounterevidenceCount:    a.counterevidence,
		DeduplicatedReuseCount:  len(a.lineages),
	}
	if a.assessmentCount > 0 {
		entry.AverageContributionScoreMicros = a.scoreTotal / a.assessmentCount
		entry.AverageConfidenceMicros = a.confidenceTotal / a.assessmentCount
	}
	return entry
}

// ranksBefore implements the spec's fixed priority order: diagnosis utility
// score/confidence, independent context/lineage coverage, adoption and
// verification evidence, counterevidence, then lineage-deduplicated reuse.
func ranksBefore(left, right UsageSummaryEntry) bool {
	for _, compare := range []func() int{
		func() int {
			return compareDescending(left.AverageContributionScoreMicros, right.AverageContributionScoreMicros)
		},
		func() int { return compareDescending(left.AverageConfidenceMicros, right.AverageConfidenceMicros) },
		func() int {
			return compareDescending(int64(left.IndependentContextCount), int64(right.IndependentContextCount))
		},
		func() int {
			return compareDescending(int64(left.IndependentLineageCount), int64(right.IndependentLineageCount))
		},
		func() int { return compareDescending(int64(left.AdoptedCount), int64(right.AdoptedCount)) },
		func() int {
			return compareDescending(int64(left.VerifiedCount+left.OutcomeCorrelatedCount), int64(right.VerifiedCount+right.OutcomeCorrelatedCount))
		},
		func() int {
			return compareDescending(int64(right.CounterevidenceCount), int64(left.CounterevidenceCount))
		},
		func() int {
			return compareDescending(int64(left.DeduplicatedReuseCount), int64(right.DeduplicatedReuseCount))
		},
	} {
		if result := compare(); result != 0 {
			return result > 0
		}
	}
	return left.Subject.CanonicalKey() < right.Subject.CanonicalKey()
}

func compareDescending(left, right int64) int {
	switch {
	case left > right:
		return 1
	case left < right:
		return -1
	default:
		return 0
	}
}
