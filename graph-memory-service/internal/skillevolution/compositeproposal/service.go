// Package compositeproposal converts evidence-backed co-adoption patterns into
// non-authoritative Composite Skill proposal drafts. It never binds a
// candidate, writes the normative Skill Path Graph, or activates a Skill.
package compositeproposal

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

// CoUsageReader is the read-only usage projection seam used for Arm-A draft
// generation. It deliberately exposes no mutation or authority operation.
type CoUsageReader interface {
	CoUsageEvidence(context.Context) []usageprojection.CoUsageProfileEvidence
}

// CoUsagePolicy is a versioned, deterministic threshold policy. The policy
// does not infer order or permissions; those remain explicit plan inputs.
type CoUsagePolicy struct {
	Ref                       contract.VersionedRef
	MinIndependentProfiles    int
	MinimumContributionMicros int64
	MinimumConfidenceMicros   int64
	EligibleKinds             []string
}

// ControlFlowStep plans one composite child invocation without copying child
// guidance into the draft.
type ControlFlowStep struct {
	StepID    string
	SkillRef  *contract.SkillArtifactRef
	DependsOn []string
}

type DataFlow struct {
	FromStep string
	ToStep   string
	Mapping  map[string]string
}

type FailureHandling struct {
	StepID    string
	OnFailure string
	Action    string
}

// CompositeDraftPlan is an explicit, non-authoritative orchestration plan.
type CompositeDraftPlan struct {
	Guard           string
	ControlFlow     []ControlFlowStep
	DataFlow        []DataFlow
	PermissionUnion []string
	FailureHandling []FailureHandling
}

type DraftRequest struct {
	Policy CoUsagePolicy
	Plan   CompositeDraftPlan
}

type DraftStatus string

const (
	DraftStatusDrafted                DraftStatus = "drafted"
	DraftStatusInsufficientEvidence   DraftStatus = "insufficient_evidence"
	DraftStatusBlockedCounterevidence DraftStatus = "blocked_counterevidence"
)

// CompositeProposalDraft is the governed Arm-A input. It has no candidate
// ref, activation state, graph operation, or runtime execution handle.
type CompositeProposalDraft struct {
	DraftID                  string
	Status                   DraftStatus
	NonAuthoritative         bool
	PolicyRef                contract.VersionedRef
	SourceRefs               []contract.SkillArtifactRef
	CompatibleProfileDigests []string
	ThresholdEvidence        []contract.EvidenceRef
	Counterevidence          []contract.EvidenceRef
	Plan                     CompositeDraftPlan
}

type Service struct {
	usage CoUsageReader
}

func NewService(usage CoUsageReader) *Service { return &Service{usage: usage} }

// Draft applies the deterministic co-usage policy and validates the explicit
// plan. Successful output remains a draft for ordinary Arm-A proposal,
// candidate, validation, and publication governance.
func (s *Service) Draft(ctx context.Context, request DraftRequest) ([]CompositeProposalDraft, error) {
	if s.usage == nil {
		return nil, fmt.Errorf("composite proposal: co-usage reader is required")
	}
	if err := validatePolicy(request.Policy); err != nil {
		return nil, err
	}
	evidence := s.usage.CoUsageEvidence(ctx)
	groups := make(map[string]*draftGroup)
	for _, profile := range evidence {
		qualifying := qualifyingSources(profile.Sources, request.Policy)
		if len(qualifying) < 2 {
			continue
		}
		key := sourceSetKey(qualifying)
		group := groups[key]
		if group == nil {
			group = &draftGroup{sources: sourceRefs(qualifying)}
			groups[key] = group
		}
		group.profileDigests = append(group.profileDigests, profile.ContextProfile.Digest())
		for _, source := range qualifying {
			group.thresholdEvidence = append(group.thresholdEvidence, source.AdoptionEvidenceRefs...)
			group.thresholdEvidence = append(group.thresholdEvidence, source.DiagnosisEvidenceRefs...)
		}
		group.counterevidence = append(group.counterevidence, profile.Counterevidence...)
	}

	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	drafts := make([]CompositeProposalDraft, 0, len(keys))
	for _, key := range keys {
		group := groups[key]
		sort.Strings(group.profileDigests)
		group.thresholdEvidence = uniqueEvidence(group.thresholdEvidence)
		group.counterevidence = uniqueEvidence(group.counterevidence)
		draft := CompositeProposalDraft{
			DraftID:                  "co-usage:" + key,
			Status:                   DraftStatusInsufficientEvidence,
			NonAuthoritative:         true,
			PolicyRef:                request.Policy.Ref,
			SourceRefs:               append([]contract.SkillArtifactRef(nil), group.sources...),
			CompatibleProfileDigests: append([]string(nil), group.profileDigests...),
			ThresholdEvidence:        append([]contract.EvidenceRef(nil), group.thresholdEvidence...),
			Counterevidence:          append([]contract.EvidenceRef(nil), group.counterevidence...),
			Plan:                     clonePlan(request.Plan),
		}
		if len(group.profileDigests) >= request.Policy.MinIndependentProfiles {
			if len(group.counterevidence) > 0 {
				draft.Status = DraftStatusBlockedCounterevidence
			} else if err := validatePlan(draft.Plan, draft.SourceRefs); err != nil {
				return nil, err
			} else {
				draft.Status = DraftStatusDrafted
			}
		}
		drafts = append(drafts, draft)
	}
	return drafts, nil
}

func validatePolicy(policy CoUsagePolicy) error {
	if policy.Ref.ID == "" || policy.Ref.Version == "" || policy.Ref.Digest == "" {
		return fmt.Errorf("composite proposal: versioned co-usage policy ref is required")
	}
	if policy.MinIndependentProfiles < 1 || policy.MinimumContributionMicros < 0 || policy.MinimumConfidenceMicros < 0 || len(policy.EligibleKinds) == 0 {
		return fmt.Errorf("composite proposal: invalid co-usage threshold policy")
	}
	return nil
}

func qualifyingSources(sources []usageprojection.CoUsageSourceEvidence, policy CoUsagePolicy) []usageprojection.CoUsageSourceEvidence {
	out := make([]usageprojection.CoUsageSourceEvidence, 0, len(sources))
	for _, source := range sources {
		if !allowedKind(source.SkillRef.Kind, policy.EligibleKinds) || len(source.AdoptionEvidenceRefs) == 0 || len(source.DiagnosisEvidenceRefs) == 0 || source.ContributionScoreMicros < policy.MinimumContributionMicros || source.ConfidenceMicros < policy.MinimumConfidenceMicros {
			continue
		}
		out = append(out, source)
	}
	sort.Slice(out, func(i, j int) bool { return exactSkillKey(out[i].SkillRef) < exactSkillKey(out[j].SkillRef) })
	return out
}

func allowedKind(kind string, allowed []string) bool {
	for _, candidate := range allowed {
		if kind == candidate {
			return true
		}
	}
	return false
}

func sourceSetKey(sources []usageprojection.CoUsageSourceEvidence) string {
	keys := make([]string, len(sources))
	for index, source := range sources {
		keys[index] = exactSkillKey(source.SkillRef)
	}
	return strings.Join(keys, "|")
}

func exactSkillKey(ref contract.SkillArtifactRef) string {
	return ref.LineageID + ":" + ref.Version + ":" + ref.ArtifactDigest
}

func sourceRefs(sources []usageprojection.CoUsageSourceEvidence) []contract.SkillArtifactRef {
	refs := make([]contract.SkillArtifactRef, len(sources))
	for index, source := range sources {
		refs[index] = source.SkillRef
	}
	return refs
}

type draftGroup struct {
	sources           []contract.SkillArtifactRef
	profileDigests    []string
	thresholdEvidence []contract.EvidenceRef
	counterevidence   []contract.EvidenceRef
}

func uniqueEvidence(refs []contract.EvidenceRef) []contract.EvidenceRef {
	seen := make(map[string]bool)
	out := make([]contract.EvidenceRef, 0, len(refs))
	for _, ref := range refs {
		key := ref.EvidenceID + ":" + ref.Version + ":" + ref.EvidenceDigest
		if !seen[key] {
			seen[key] = true
			out = append(out, ref)
		}
	}
	return out
}

func validatePlan(plan CompositeDraftPlan, sources []contract.SkillArtifactRef) error {
	if strings.TrimSpace(plan.Guard) == "" || len(plan.ControlFlow) == 0 || len(plan.PermissionUnion) == 0 {
		return fmt.Errorf("composite proposal: guard, control flow, and permission union are required")
	}
	sourceSet := make(map[string]bool)
	for _, source := range sources {
		sourceSet[exactSkillKey(source)] = true
	}
	steps := make(map[string]bool)
	for _, step := range plan.ControlFlow {
		if step.StepID == "" || step.SkillRef == nil || steps[step.StepID] || !sourceSet[exactSkillKey(*step.SkillRef)] {
			return fmt.Errorf("composite proposal: control flow must reference each declared exact source")
		}
		steps[step.StepID] = true
	}
	for _, step := range plan.ControlFlow {
		for _, dependency := range step.DependsOn {
			if dependency == step.StepID || !steps[dependency] {
				return fmt.Errorf("composite proposal: unknown or self control-flow dependency")
			}
		}
	}
	for _, flow := range plan.DataFlow {
		if !steps[flow.FromStep] || !steps[flow.ToStep] {
			return fmt.Errorf("composite proposal: data flow references an unknown step")
		}
	}
	for _, failure := range plan.FailureHandling {
		if !steps[failure.StepID] || strings.TrimSpace(failure.OnFailure) == "" || strings.TrimSpace(failure.Action) == "" {
			return fmt.Errorf("composite proposal: invalid failure handling")
		}
	}
	return nil
}

func clonePlan(plan CompositeDraftPlan) CompositeDraftPlan {
	cloned := plan
	cloned.ControlFlow = append([]ControlFlowStep(nil), plan.ControlFlow...)
	cloned.DataFlow = append([]DataFlow(nil), plan.DataFlow...)
	cloned.PermissionUnion = append([]string(nil), plan.PermissionUnion...)
	cloned.FailureHandling = append([]FailureHandling(nil), plan.FailureHandling...)
	return cloned
}
