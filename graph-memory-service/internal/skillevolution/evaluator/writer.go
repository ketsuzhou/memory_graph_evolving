package evaluator

import (
	"encoding/json"
	"strconv"

	"river2.dev/graph-memory-service/internal/contract"
)

// writeDecision is the §7.12 decision writer (the GMS-203 refactor seam):
// assemble the closed document, digest the preimage and self-validate
// against the authority schema before handing it out.
func (e *Evaluator) writeDecision(cmp Comparator, in Input, findings *gateFindings, outcome string, reasons []string) (*Decision, error) {
	decision := &Decision{
		decisionID:             in.DecisionID,
		decisionVersion:        in.DecisionVersion,
		candidate:              in.Candidate,
		validationRecordRefs:   in.ValidationRecordRefs,
		replayResultRefs:       replayResultRefs(in.ResultDoc),
		releaseRuleRef:         in.ReleaseRule.Ref,
		comparatorRef:          cmp.Ref,
		gates:                  findings.ordered(),
		outcome:                outcome,
		reasonCodes:            append([]string(nil), reasons...),
		sourceHeadExpectations: in.ExpectedSourceHeads,
	}
	if decision.decisionID == "" {
		decision.decisionID = deriveDecisionID(in, cmp)
	}
	if decision.decisionVersion < 1 {
		decision.decisionVersion = 1
	}
	digest, err := contract.DigestOf(decisionPreimage(decision.Doc()))
	if err != nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "decision digest: %v", err)
	}
	decision.decisionDigest = digest
	if err := e.gates.ValidateInstance(decision.Doc(), SchemaReleaseDecision); err != nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "written decision failed its own authority schema: %v", err)
	}
	return decision, nil
}

// replayResultRefs derives the exact §7.12 replay_result_refs from the
// §7.11 document (id + version 1 + the result digest).
func replayResultRefs(doc map[string]any) []contract.VersionedRef {
	id, _ := contract.AsString(doc["replay_result_id"])
	digest, _ := contract.AsString(doc["result_digest"])
	if id == "" || digest == "" {
		return nil
	}
	return []contract.VersionedRef{{ID: id, Version: "1", Digest: digest}}
}

// deriveDecisionID mints the deterministic decision identity from the
// frozen inputs.
func deriveDecisionID(in Input, cmp Comparator) string {
	digest, err := contract.DigestOf(map[string]any{
		"candidate":      candidateRefDoc(in.Candidate),
		"release_rule":   versionedRefDoc(in.ReleaseRule.Ref),
		"comparator":     versionedRefDoc(cmp.Ref),
		"replay_results": replayResultRefs(in.ResultDoc),
	})
	if err != nil {
		return "decision-unknown"
	}
	return "decision-" + digest[len("sha256:"):len("sha256:")+16]
}

// decisionPreimage extracts the §7.12 x-digest preimage fields.
func decisionPreimage(doc map[string]any) map[string]any {
	fields := []string{
		"schema_version", "decision_id", "decision_version", "candidate_ref",
		"validation_record_refs", "replay_result_refs", "release_rule_ref",
		"utility_comparator_ref", "hard_gate_results", "outcome", "reason_codes",
	}
	core := make(map[string]any, len(fields))
	for _, field := range fields {
		core[field] = doc[field]
	}
	return core
}

// GateResult is one §7.12 hard_gate_results entry.
type GateResult struct {
	GateCode   string
	Passed     bool
	RecordRefs []contract.VersionedRef
}

// Decision is the authoritative §7.12 ReleaseDecision. `accepted`
// authorizes activation_pending only (never released/active by itself).
type Decision struct {
	decisionID             string
	decisionVersion        int64
	decisionDigest         string
	candidate              contract.CandidateArtifactRef
	validationRecordRefs   []contract.VersionedRef
	replayResultRefs       []contract.VersionedRef
	releaseRuleRef         contract.VersionedRef
	comparatorRef          contract.VersionedRef
	gates                  []GateResult
	outcome                string
	reasonCodes            []string
	sourceHeadExpectations []contract.SkillArtifactRef
}

// Outcome returns accepted | rejected | inconclusive.
func (d *Decision) Outcome() string { return d.outcome }

// ReasonCodes returns the closed failure reasons (empty iff accepted).
func (d *Decision) ReasonCodes() []string {
	out := make([]string, len(d.reasonCodes))
	copy(out, d.reasonCodes)
	return out
}

// DecisionDigest returns the JCS/SHA-256 digest of the §7.12 core.
func (d *Decision) DecisionDigest() string { return d.decisionDigest }

// HardGateResults returns the recorded hard-gate outcomes in frozen order.
func (d *Decision) HardGateResults() []GateResult {
	out := make([]GateResult, len(d.gates))
	copy(out, d.gates)
	return out
}

// Doc returns the decoder-model §7.12 document including decision_digest.
func (d *Decision) Doc() map[string]any {
	gates := make([]any, 0, len(d.gates))
	for _, gate := range d.gates {
		entry := map[string]any{"gate_code": gate.GateCode, "passed": gate.Passed}
		if len(gate.RecordRefs) > 0 {
			refs := make([]any, 0, len(gate.RecordRefs))
			for _, ref := range gate.RecordRefs {
				refs = append(refs, versionedRefDoc(ref))
			}
			entry["record_refs"] = refs
		}
		gates = append(gates, entry)
	}
	reasons := make([]any, 0, len(d.reasonCodes))
	for _, reason := range d.reasonCodes {
		reasons = append(reasons, reason)
	}
	doc := map[string]any{
		"schema_version":         "gms.release-decision.v1",
		"decision_id":            d.decisionID,
		"decision_version":       num(d.decisionVersion),
		"decision_digest":        d.decisionDigest,
		"candidate_ref":          candidateRefDoc(d.candidate),
		"validation_record_refs": versionedRefDocs(d.validationRecordRefs),
		"replay_result_refs":     versionedRefDocs(d.replayResultRefs),
		"release_rule_ref":       versionedRefDoc(d.releaseRuleRef),
		"utility_comparator_ref": versionedRefDoc(d.comparatorRef),
		"hard_gate_results":      gates,
		"outcome":                d.outcome,
		"reason_codes":           reasons,
	}
	if len(d.sourceHeadExpectations) > 0 {
		heads := make([]any, 0, len(d.sourceHeadExpectations))
		for _, head := range d.sourceHeadExpectations {
			heads = append(heads, skillRefDoc(head))
		}
		doc["source_head_expectations"] = heads
	}
	return doc
}

// --- decoder-model renderers -------------------------------------------------

func num(v int64) any { return json.Number(strconv.FormatInt(v, 10)) }

func jsonNumber(v string) any { return json.Number(v) }

func versionedRefDoc(ref contract.VersionedRef) map[string]any {
	return map[string]any{"id": ref.ID, "version": json.Number(ref.Version), "digest": ref.Digest}
}

func versionedRefDocs(refs []contract.VersionedRef) []any {
	out := make([]any, 0, len(refs))
	for _, ref := range refs {
		out = append(out, versionedRefDoc(ref))
	}
	return out
}

func skillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         json.Number(ref.Version),
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

func candidateRefDoc(ref contract.CandidateArtifactRef) map[string]any {
	return map[string]any{
		"schema_version": ref.SchemaVersion,
		"candidate_id":   ref.CandidateID,
		"kind":           ref.Kind,
		"body_digest":    ref.BodyDigest,
		"origin_type":    ref.OriginType,
		"origin_ref":     versionedRefDoc(ref.OriginRef),
	}
}

func deepCopyValue(obj map[string]any) map[string]any {
	data, err := contract.JCS(obj)
	if err != nil {
		return nil
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return nil
	}
	out, _ := contract.AsObject(value)
	return out
}
