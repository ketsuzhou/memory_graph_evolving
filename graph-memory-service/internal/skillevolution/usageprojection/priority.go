package usageprojection

import "sort"

// AuthorityTier is a retrieval/scheduling visibility tier. Usage is only a
// tiebreaker within a tier; it cannot make advisory or probation work outrank
// active work.
type AuthorityTier string

const (
	AuthorityTierActive    AuthorityTier = "active"
	AuthorityTierProbation AuthorityTier = "probation"
	AuthorityTierAdvisory  AuthorityTier = "advisory"
)

// PriorityInput identifies one consumer-owned item that may be associated
// with an exact usage subject. Key is the consumer's deterministic fallback.
type PriorityInput struct {
	Key           string
	AuthorityTier AuthorityTier
	Subject       UsageSubjectRef
}

// PriorityExplanation is the auditable ranking carrier a consumer can render
// without granting lifecycle, promotion, or activation authority to usage.
type PriorityExplanation struct {
	PolicyVersion                  string
	AuthorityTier                  AuthorityTier
	UsageApplied                   bool
	SubjectKey                     string
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

// PriorityDecision is one input together with its non-authoritative usage
// explanation. Decisions are returned in deterministic dequeue/render order.
type PriorityDecision struct {
	Key         string
	Explanation PriorityExplanation
}

// Prioritize applies the fixed UsageSummary policy only inside each authority
// tier. Summary callers decide diagnostic isolation; this function never
// accepts raw observations and therefore cannot accidentally bypass it.
func Prioritize(summary UsageSummary, inputs []PriorityInput) []PriorityDecision {
	bySubject := make(map[string]UsageSummaryEntry, len(summary.Entries))
	for _, entry := range summary.Entries {
		bySubject[entry.Subject.CanonicalKey()] = entry
	}
	decisions := make([]priorityDecision, 0, len(inputs))
	for _, input := range inputs {
		explanation := PriorityExplanation{
			PolicyVersion: summary.PolicyVersion,
			AuthorityTier: input.AuthorityTier,
			SubjectKey:    input.Subject.CanonicalKey(),
		}
		if entry, ok := bySubject[explanation.SubjectKey]; ok {
			explanation.UsageApplied = true
			explanation.AverageContributionScoreMicros = entry.AverageContributionScoreMicros
			explanation.AverageConfidenceMicros = entry.AverageConfidenceMicros
			explanation.IndependentContextCount = entry.IndependentContextCount
			explanation.IndependentLineageCount = entry.IndependentLineageCount
			explanation.AdoptedCount = entry.AdoptedCount
			explanation.VerifiedCount = entry.VerifiedCount
			explanation.OutcomeCorrelatedCount = entry.OutcomeCorrelatedCount
			explanation.CounterevidenceCount = entry.CounterevidenceCount
			explanation.DeduplicatedReuseCount = entry.DeduplicatedReuseCount
			decisions = append(decisions, priorityDecision{PriorityDecision: PriorityDecision{Key: input.Key, Explanation: explanation}, entry: &entry})
			continue
		}
		decisions = append(decisions, priorityDecision{PriorityDecision: PriorityDecision{Key: input.Key, Explanation: explanation}})
	}
	sort.SliceStable(decisions, func(i, j int) bool {
		left, right := decisions[i], decisions[j]
		if tierRank(left.Explanation.AuthorityTier) != tierRank(right.Explanation.AuthorityTier) {
			return tierRank(left.Explanation.AuthorityTier) > tierRank(right.Explanation.AuthorityTier)
		}
		if left.entry != nil && right.entry != nil && left.entry.Subject.CanonicalKey() != right.entry.Subject.CanonicalKey() {
			return ranksBefore(*left.entry, *right.entry)
		}
		if left.entry != nil && right.entry == nil {
			return true
		}
		if left.entry == nil && right.entry != nil {
			return false
		}
		return left.Key < right.Key
	})
	out := make([]PriorityDecision, len(decisions))
	for index, decision := range decisions {
		out[index] = decision.PriorityDecision
	}
	return out
}

type priorityDecision struct {
	PriorityDecision
	entry *UsageSummaryEntry
}

func tierRank(tier AuthorityTier) int {
	switch tier {
	case AuthorityTierActive:
		return 3
	case AuthorityTierProbation:
		return 2
	case AuthorityTierAdvisory:
		return 1
	default:
		return 0
	}
}
