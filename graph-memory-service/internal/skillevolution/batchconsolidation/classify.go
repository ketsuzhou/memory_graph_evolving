package batchconsolidation

import (
	"sort"
	"strings"

	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
)

type pairKind int

const (
	kindNone pairKind = iota
	kindDuplicate
	kindSpecialize
	kindMerge
	kindConflict
)

func classifyFamily(family map[string]rawproposal.RawSkillProposal, ids []string) []DecisionDraft {
	ordered := append([]string(nil), ids...)
	assigned := make(map[string]bool, len(ordered))
	var drafts []DecisionDraft

	emit := func(op string, members []string, conflicts []ConditionalConflict) {
		sort.Strings(members)
		for _, id := range members {
			assigned[id] = true
		}
		drafts = append(drafts, DecisionDraft{
			DecisionID:           decisionIDFor(op, members),
			Operation:            op,
			SourceProposalIDs:    members,
			ConditionalConflicts: conflicts,
		})
	}

	takeClusters := func(want pairKind, op string) {
		parent := newUnionFind(ordered)
		for i := 0; i < len(ordered); i++ {
			if assigned[ordered[i]] {
				continue
			}
			for j := i + 1; j < len(ordered); j++ {
				if assigned[ordered[j]] {
					continue
				}
				if pairRelation(family[ordered[i]], family[ordered[j]]) == want {
					parent.union(ordered[i], ordered[j])
				}
			}
		}
		groups := parent.groups(ordered, assigned)
		for _, members := range groups {
			if len(members) < 2 {
				continue
			}
			var conflicts []ConditionalConflict
			if op == OpRetain && want == kindConflict {
				conflicts = conflictRecords(family, members)
			}
			emit(op, members, conflicts)
		}
	}

	takeClusters(kindDuplicate, OpRetain)
	takeClusters(kindSpecialize, OpSpecialize)
	takeClusters(kindMerge, OpMerge)
	takeClusters(kindConflict, OpRetain)

	for _, id := range ordered {
		if assigned[id] {
			continue
		}
		p := family[id]
		switch {
		case isInsufficient(p):
			emit(OpInsufficientEvidence, []string{id}, nil)
		case isRetire(p):
			emit(OpRetire, []string{id}, nil)
		default:
			emit(OpRetain, []string{id}, nil)
		}
	}
	return drafts
}

func pairRelation(a, b rawproposal.RawSkillProposal) pairKind {
	if isInsufficient(a) || isInsufficient(b) || isRetire(a) || isRetire(b) {
		return kindNone
	}
	sameInsight := norm(a.NonObviousInsight) == norm(b.NonObviousInsight)
	samePolicy := norm(strings.Join(a.DecisionPolicyOrSteps, "\n")) == norm(strings.Join(b.DecisionPolicyOrSteps, "\n"))
	sameGuards := norm(a.ContextTrigger) == norm(b.ContextTrigger) &&
		norm(strings.Join(a.Contraindications, "\n")) == norm(strings.Join(b.Contraindications, "\n"))
	overlap := overlappingApplicability(a, b)
	conflict := policiesConflict(a.DecisionPolicyOrSteps, b.DecisionPolicyOrSteps)

	if sameInsight && samePolicy && sameGuards {
		return kindDuplicate
	}
	if sameInsight && !sameGuards {
		return kindSpecialize
	}
	if overlap && !conflict {
		return kindMerge
	}
	if overlap && conflict {
		return kindConflict
	}
	return kindNone
}

func overlappingApplicability(a, b rawproposal.RawSkillProposal) bool {
	seen := make(map[string]bool, len(a.SourceEvidenceRefs))
	for _, id := range a.SourceEvidenceRefs {
		seen[id] = true
	}
	for _, id := range b.SourceEvidenceRefs {
		if seen[id] {
			return true
		}
	}
	return a.SourceCheckpointID != "" && a.SourceCheckpointID == b.SourceCheckpointID
}

func policiesConflict(a, b []string) bool {
	left, right := norm(strings.Join(a, " ")), norm(strings.Join(b, " "))
	contradicts := func(x, y string) bool {
		return strings.Contains(x, "never rewrite") && (strings.Contains(y, "resolve") || strings.Contains(y, "rewrite"))
	}
	return contradicts(left, right) || contradicts(right, left)
}

func isInsufficient(p rawproposal.RawSkillProposal) bool {
	text := norm(p.OutcomeObserved + " " + p.FailureOrOpportunity)
	return strings.Contains(text, "insufficient evidence") || strings.Contains(text, "cause=unknown")
}

func isRetire(p rawproposal.RawSkillProposal) bool {
	return strings.Contains(norm(p.FailureOrOpportunity), "no new information")
}

func conflictRecords(family map[string]rawproposal.RawSkillProposal, members []string) []ConditionalConflict {
	var out []ConditionalConflict
	for i := 0; i < len(members); i++ {
		for j := i + 1; j < len(members); j++ {
			a, b := family[members[i]], family[members[j]]
			if pairRelation(a, b) != kindConflict {
				continue
			}
			out = append(out, ConditionalConflict{
				LeftSourceID:  members[i],
				RightSourceID: members[j],
				Condition:     "policy conflict under overlapping applicability",
				EvidenceRefs:  evidenceUnion(a, b),
			})
		}
	}
	return out
}

func evidenceUnion(a, b rawproposal.RawSkillProposal) []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range append(append([]string(nil), a.SourceEvidenceRefs...), b.SourceEvidenceRefs...) {
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func norm(text string) string {
	return strings.Join(strings.Fields(strings.ToLower(text)), " ")
}

type unionFind struct {
	parent map[string]string
}

func newUnionFind(ids []string) *unionFind {
	parent := make(map[string]string, len(ids))
	for _, id := range ids {
		parent[id] = id
	}
	return &unionFind{parent: parent}
}

func (u *unionFind) find(id string) string {
	for u.parent[id] != id {
		u.parent[id] = u.parent[u.parent[id]]
		id = u.parent[id]
	}
	return id
}

func (u *unionFind) union(a, b string) {
	ra, rb := u.find(a), u.find(b)
	if ra != rb {
		u.parent[rb] = ra
	}
}

func (u *unionFind) groups(ids []string, skip map[string]bool) [][]string {
	buckets := make(map[string][]string)
	var order []string
	for _, id := range ids {
		if skip[id] {
			continue
		}
		root := u.find(id)
		if _, ok := buckets[root]; !ok {
			order = append(order, root)
		}
		buckets[root] = append(buckets[root], id)
	}
	out := make([][]string, 0, len(order))
	for _, root := range order {
		out = append(out, buckets[root])
	}
	return out
}
