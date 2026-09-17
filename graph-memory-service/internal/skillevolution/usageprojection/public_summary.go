package usageprojection

import (
	"context"
	"fmt"
	"sort"
)

// InteractionOnlySummary is the privacy-bounded C2 read model. It never
// reads DiagnosisUtilityAssessment records, therefore neither ordinary nor
// diagnostic diagnoses can influence the returned aggregates.
type InteractionOnlySummary struct {
	PolicyVersion string
	Entries       []InteractionOnlyEntry
}
type InteractionOnlyEntry struct {
	Subject                 UsageSubjectRef
	IndependentContextCount int
	IndependentLineageCount int
	AdoptedCount            int
	VerifiedCount           int
	OutcomeCorrelatedCount  int
	DeduplicatedReuseCount  int
}

func (s *UsageProjectionService) InteractionOnlySummary(ctx context.Context, profile ContextProfile, subjects []UsageSubjectRef) (InteractionOnlySummary, error) {
	if err := validateContextProfile(profile); err != nil {
		return InteractionOnlySummary{}, err
	}
	allowed := map[string]UsageSubjectRef{}
	for _, subject := range subjects {
		if !subject.valid() {
			return InteractionOnlySummary{}, fmt.Errorf("usage projection: summary requires exact skill or candidate subjects")
		}
		allowed[subject.CanonicalKey()] = subject
	}
	if len(allowed) == 0 {
		return InteractionOnlySummary{}, fmt.Errorf("usage projection: summary requires at least one exact subject")
	}
	snapshot, err := s.store.Read(ctx)
	if err != nil {
		return InteractionOnlySummary{}, err
	}
	aggregates := map[string]*interactionAggregate{}
	filter := profile.Digest()
	for _, observation := range snapshot.Interactions {
		if observation.DiagnosticOnly || observation.ContextProfile.Digest() != filter {
			continue
		}
		key := observation.Subject.CanonicalKey()
		subject, selected := allowed[key]
		if !selected {
			continue
		}
		aggregate := aggregates[key]
		if aggregate == nil {
			aggregate = &interactionAggregate{subject: subject, contexts: map[string]bool{}, lineages: map[string]bool{}}
			aggregates[key] = aggregate
		}
		aggregate.contexts[observation.ContextProfile.Digest()] = true
		aggregate.lineages[observation.SourceLineageID] = true
		switch observation.Stage {
		case InteractionAdopted:
			aggregate.adopted++
		case InteractionVerified:
			aggregate.verified++
		case InteractionOutcomeCorrelated:
			aggregate.outcomes++
		}
	}
	entries := make([]InteractionOnlyEntry, 0, len(aggregates))
	for _, aggregate := range aggregates {
		entries = append(entries, InteractionOnlyEntry{Subject: aggregate.subject, IndependentContextCount: len(aggregate.contexts), IndependentLineageCount: len(aggregate.lineages), AdoptedCount: aggregate.adopted, VerifiedCount: aggregate.verified, OutcomeCorrelatedCount: aggregate.outcomes, DeduplicatedReuseCount: len(aggregate.lineages)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Subject.CanonicalKey() < entries[j].Subject.CanonicalKey() })
	return InteractionOnlySummary{PolicyVersion: s.policy.Version, Entries: entries}, nil
}

type interactionAggregate struct {
	subject                     UsageSubjectRef
	contexts, lineages          map[string]bool
	adopted, verified, outcomes int
}
