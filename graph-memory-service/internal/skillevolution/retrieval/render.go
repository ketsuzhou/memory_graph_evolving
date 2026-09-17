// render.go is the Guidance renderer (GMS §10.9, Contract §7.15): it
// derives one closed GuidanceView from a canonical released artifact under
// a render profile and guidance policy. The view carries EXACTLY the
// frozen twelve fields — freshness and authorization never appear on the
// DTO (they ride the request + read audit) — and its view_hash is
// SHA-256(JCS(view minus view_hash)).
package retrieval

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
)

// GuidanceView schema version and the closed field set (Contract §7.15).
const (
	SchemaGuidanceView = "gms.guidance-view.v1"
)

// renderedView is one rendered closed GuidanceView plus its token count.
type renderedView struct {
	doc    map[string]any
	tokens int64
}

// guidanceBranch is one decision branch of a step_guidance body.
type guidanceBranch struct {
	id       string
	guidance string
	evidence []contract.EvidenceRef
}

// renderGuidanceView renders the closed GuidanceView of one skill candidate
// under the token budget: branches are included in canonical branch order
// until the budget fills; overflow branches are omitted (truncated=true,
// carried by omitted_branch_refs — never silently dropped).
func renderGuidanceView(candidate *skillCandidate, branches []guidanceBranch, renderProfile, policyRef versionedRef, runtimeContextHash string, tokenBudget int64) (*renderedView, error) {
	if tokenBudget < 0 {
		return nil, newError(ReasonBudgetInvalid, "guidance token budget must be non-negative")
	}
	included := make([]string, 0, len(branches))
	omitted := make([]string, 0, len(branches))
	contentLines := []string{
		candidate.title,
		candidate.description,
	}
	truncated := false

	sortedBranches := make([]guidanceBranch, len(branches))
	copy(sortedBranches, branches)
	sort.SliceStable(sortedBranches, func(i, j int) bool { return sortedBranches[i].id < sortedBranches[j].id })
	for _, branch := range sortedBranches {
		line := fmt.Sprintf("branch %s: %s", branch.id, branch.guidance)
		trial := append(append([]string{}, contentLines...), line)
		if countTokens(strings.Join(trial, "\n")) > tokenBudget {
			// Token budget full: the branch is omitted and carried by
			// omitted_branch_refs (never a silent drop), truncated=true.
			omitted = append(omitted, branch.id)
			truncated = true
			continue
		}
		contentLines = trial
		included = append(included, branch.id)
	}

	// Expandable refs: the exact branch refs (continuable via memory_expand
	// branch targets) and the branch provenance evidence.
	expandable := make([]any, 0, len(sortedBranches))
	for _, branch := range sortedBranches {
		expandable = append(expandable, map[string]any{
			"ref_type": "branch",
			"ref": map[string]any{
				"schema_version":   "gms.exact-branch-ref.v1",
				"source_skill_ref": skillRefDoc(candidate.ref),
				"branch_id":        branch.id,
				"branch_digest":    branchDigest(candidate.ref, branch.id, candidate.envelope),
			},
		})
		for _, ref := range branch.evidence {
			expandable = append(expandable, map[string]any{
				"ref_type": "evidence",
				"ref":      evidenceRefDoc(ref),
			})
		}
	}
	// Composite children expand as child_skill targets.
	if children := compositeChildren(candidate.envelope); len(children) > 0 {
		for _, child := range children {
			expandable = append(expandable, map[string]any{
				"ref_type": "child_skill",
				"ref":      skillRefDoc(child),
			})
		}
	}

	content := strings.Join(contentLines, "\n")
	tokens := countTokens(content)
	if tokens > tokenBudget {
		truncated = true
	}
	view := map[string]any{
		"schema_version":       SchemaGuidanceView,
		"source_skill_ref":     skillRefDoc(candidate.ref),
		"render_profile_ref":   renderProfile.asDoc(),
		"policy_ref":           policyRef.asDoc(),
		"runtime_context_hash": runtimeContextHash,
		"included_branch_refs": stringsToAny(included),
		"omitted_branch_refs":  stringsToAny(omitted),
		"expandable_refs":      expandable,
		"content":              content,
		"content_token_count":  json.Number(strconv.FormatInt(tokens, 10)),
		"truncated":            truncated,
	}
	digest, err := contract.DigestOf(view)
	if err != nil {
		return nil, newError(ReasonGuidanceRenderFailed, "guidance view cannot canonicalize: %v", err)
	}
	view["view_hash"] = digest
	return &renderedView{doc: view, tokens: tokens}, nil
}

// branchDigest derives one branch digest from the canonical body exactly
// like the projector rule (SHA-256 over the JCS of the branch object).
func branchDigest(ref contract.SkillArtifactRef, branchID string, envelope map[string]any) string {
	for _, branch := range guidanceBranches(envelope) {
		if branch.id == branchID {
			// Re-derive from the raw branch object for the exact digest.
			if raw := rawBranchObject(envelope, branchID); raw != nil {
				if digest, err := contract.DigestOf(raw); err == nil {
					return digest
				}
			}
		}
	}
	_ = ref
	return contract.DigestBytes([]byte("branch:" + branchID))
}

// guidanceBranches extracts the decision branches of one canonical
// step_guidance envelope (deterministic body order).
func guidanceBranches(envelope map[string]any) []guidanceBranch {
	body, _ := contract.AsObject(envelope["body"])
	rawBranches, _ := contract.AsArray(body["branches"])
	out := make([]guidanceBranch, 0, len(rawBranches))
	for _, raw := range rawBranches {
		branch, _ := contract.AsObject(raw)
		if branch == nil {
			continue
		}
		id, _ := contract.AsString(branch["branch_id"])
		action, _ := contract.AsObject(branch["action"])
		guidance, _ := contract.AsString(action["guidance"])
		var evidence []contract.EvidenceRef
		if rawRefs, ok := contract.AsArray(action["evidence_refs"]); ok {
			for _, rawRef := range rawRefs {
				refObj, _ := contract.AsObject(rawRef)
				if ref, err := contract.ParseEvidenceRef(refObj); err == nil {
					evidence = append(evidence, ref)
				}
			}
		}
		out = append(out, guidanceBranch{id: id, guidance: guidance, evidence: evidence})
	}
	return out
}

func rawBranchObject(envelope map[string]any, branchID string) map[string]any {
	body, _ := contract.AsObject(envelope["body"])
	rawBranches, _ := contract.AsArray(body["branches"])
	for _, raw := range rawBranches {
		branch, _ := contract.AsObject(raw)
		if branch == nil {
			continue
		}
		if id, _ := contract.AsString(branch["branch_id"]); id == branchID {
			return branch
		}
	}
	return nil
}

// compositeChildren extracts the composes children of one composite body.
func compositeChildren(envelope map[string]any) []contract.SkillArtifactRef {
	body, _ := contract.AsObject(envelope["body"])
	children, _ := contract.AsArray(body["children"])
	out := make([]contract.SkillArtifactRef, 0, len(children))
	for _, raw := range children {
		child, _ := contract.AsObject(raw)
		refObj, _ := contract.AsObject(child["skill_ref"])
		if ref, err := contract.ParseSkillArtifactRef(refObj); err == nil {
			out = append(out, ref)
		}
	}
	return out
}

// countTokens counts whitespace-separated tokens of the rendered content
// (integer-only token accounting).
func countTokens(content string) int64 {
	fields := strings.Fields(content)
	return int64(len(fields))
}
