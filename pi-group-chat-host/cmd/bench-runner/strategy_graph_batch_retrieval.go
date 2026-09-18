package main

import (
	"regexp"
	"strings"
)

// graphBatchRetrievalPromptHead is the production held-out retrieval brief.
// It is intentionally not the legacy retrievalPromptHead: the agent only
// nominates structured skill references. Host then directs those skills to
// the upcoming task agent, which accepts or rejects via skill_feedback.
// Legacy warm-skill-batch keeps the old prose-note prompt byte-for-byte.
const graphBatchRetrievalPromptHead = "Skill retrieval turn. For this one turn you act as this room's diagnosis agent. A test task is about to start; its prompt is quoted at the end of this message. The frozen skill ledger is available through the skills_list tool: chunk 0 is a compact index — one line per skill with its entry number, name, trigger, sha256 prefix, and the chunk holding its full text. Start from the index, then read the detail chunks whose trigger or body is even weakly related to this task; every response header states total_chunks.\n\nYour job is candidate nomination only, not the final applicability verdict. Do not write a prose note and do not solve the task. The skills you nominate will be delivered by the Host as a directed offer to the upcoming task agent. The task agent decides applicability through skill_feedback: rejecting an offer is a normal signal, not a failure.\n\n1. Nominate ledger skills whose trigger or content is reasonably related to this task's problem type, algorithm family, or likely pitfalls. Exact trigger match is not required — weak but plausible relatedness is enough. Prefer over-nominating: when in doubt, include the skill. Rank by relatedness and publish at most 3.\n2. Publish NO_SKILL only when every ledger skill is completely unrelated to this task — not even weakly related by problem type, algorithm family, or pitfalls.\n3. Publish via room_send one message that is exactly one of:\nNO_SKILL\n\nor one or more blocks in this shape (no other prose):\nSKILL_SELECTION\nskill_id: skill://evaluation/<id>@<revision>\nsha256: <64 lowercase hex>\ntrigger: <the skill's trigger, copied verbatim>\n\nQuote skill_id and sha256 exactly as the ledger states them. Solving the quoted task belongs to the task agent's upcoming turn, not to this retrieval turn.\n\n--- upcoming test task ---\n"

var (
	graphBatchSelectionBlockRE = regexp.MustCompile(`(?is)SKILL_SELECTION\s+skill_id:\s*(\S+)\s+sha256:\s*([0-9a-f]{64})\s+trigger:\s*(.+?)(?:\n\s*\n|\nSKILL_SELECTION|\z)`)
	graphBatchNoSkillRE        = regexp.MustCompile(`(?i)\bNO_SKILL(?:_APPLICABLE)?\b`)
)

type graphBatchSkillSelection struct {
	SkillReference string
	SHA256         string
	Trigger        string
}

func parseGraphBatchSkillSelections(reply string, ledger *pinnedLedger) []graphBatchSkillSelection {
	trimmed := strings.TrimSpace(reply)
	if trimmed == "" || graphBatchNoSkillRE.MatchString(trimmed) && !strings.Contains(strings.ToUpper(trimmed), "SKILL_SELECTION") {
		return nil
	}
	matches := graphBatchSelectionBlockRE.FindAllStringSubmatch(trimmed, 3)
	if len(matches) == 0 {
		return parseGraphBatchSelectionFallback(trimmed, ledger)
	}
	seen := map[string]bool{}
	var out []graphBatchSkillSelection
	for _, match := range matches {
		ref := strings.TrimSpace(match[1])
		sum := strings.ToLower(strings.TrimSpace(match[2]))
		trigger := strings.TrimSpace(match[3])
		if ledger != nil {
			if skill, ok := ledger.lookup(ref); ok {
				ref = skill.SkillReference
				sum = skill.SHA256
				if trigger == "" {
					trigger = skill.Trigger
				}
			} else if skill, ok := ledger.lookup(sum); ok {
				ref = skill.SkillReference
				sum = skill.SHA256
				if trigger == "" {
					trigger = skill.Trigger
				}
			} else {
				continue
			}
		}
		if ref == "" || sum == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		out = append(out, graphBatchSkillSelection{SkillReference: ref, SHA256: sum, Trigger: trigger})
	}
	return out
}

func parseGraphBatchSelectionFallback(reply string, ledger *pinnedLedger) []graphBatchSkillSelection {
	if ledger == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []graphBatchSkillSelection
	for _, skill := range ledger.Skills {
		if strings.Contains(reply, skill.SkillReference) || strings.Contains(strings.ToLower(reply), skill.SHA256) {
			if seen[skill.SkillReference] {
				continue
			}
			seen[skill.SkillReference] = true
			out = append(out, graphBatchSkillSelection{
				SkillReference: skill.SkillReference,
				SHA256:         skill.SHA256,
				Trigger:        skill.Trigger,
			})
			if len(out) == 3 {
				break
			}
		}
	}
	return out
}

func resolvePinnedSelections(selections []graphBatchSkillSelection, ledger *pinnedLedger) []pinnedSkill {
	if ledger == nil {
		return nil
	}
	out := make([]pinnedSkill, 0, len(selections))
	seen := map[string]bool{}
	for _, selection := range selections {
		skill, ok := ledger.lookup(selection.SkillReference)
		if !ok {
			skill, ok = ledger.lookup(selection.SHA256)
		}
		if !ok || seen[skill.SkillReference] {
			continue
		}
		seen[skill.SkillReference] = true
		out = append(out, skill)
	}
	return out
}
