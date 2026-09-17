package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
)

const (
	armBReportTimeout        = 5 * time.Second
	armBToolPolicyRefID      = "pi-group-chat-host-tool-policy"
	armBDiagnosisRubricRefID = "pi-group-chat-host-diagnosis-rubric"
)

type armBClient interface {
	RecordSkillEvolutionInteraction(context.Context, memoryclient.SkillEvolutionInteractionRecordRequest) (memoryclient.SkillEvolutionWriteResponse, error)
	RecordSkillEvolutionDiagnosis(context.Context, memoryclient.SkillEvolutionDiagnosisRecordRequest) (memoryclient.SkillEvolutionWriteResponse, error)
	ReadSkillEvolutionAdvisoryCandidates(context.Context, memoryclient.SkillEvolutionAdvisoryReadRequest) (memoryclient.SkillEvolutionAdvisoryReadResponse, error)
	ReadSkillEvolutionCandidateOutcomes(context.Context, memoryclient.SkillEvolutionCandidateOutcomesReadRequest) (memoryclient.SkillEvolutionCandidateOutcomesReadResponse, error)
}

// armBReporter is a fail-open observability adapter. It never returns an
// error to a benchmark control-flow caller and keeps all reporting state in
// non-grading attempt telemetry.
type armBReporter struct {
	client armBClient
	logf   func(string, ...any)
}

func newArmBReporter(client armBClient, logf func(string, ...any)) *armBReporter {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &armBReporter{client: client, logf: logf}
}

func (r *armBReporter) reportRetrievedSkills(ctx context.Context, record *attemptRecord, ledger []skillProposal, reply string, drained bool) {
	if r == nil || r.client == nil || !retrievalAddressedToTeammates(reply) {
		return
	}
	for _, proposal := range retrievedLedgerProposals(ledger, reply) {
		source := "local_comparison"
		if proposal.AdvisoryRef != nil {
			source = "advisory_read"
		}
		c3RecordExposure(record, proposal, source, "selected")
		r.reportInteraction(ctx, record, proposal, "selected")
		// A quoted selection becomes exposure only when the addressed publish has
		// been drained. This is intentionally not an adoption declaration.
		if drained {
			c3RecordExposure(record, proposal, source, "exposed")
			r.reportInteraction(ctx, record, proposal, "exposed")
		}
	}
}

func (r *armBReporter) reportInteraction(ctx context.Context, record *attemptRecord, proposal skillProposal, stage string) {
	requestCtx, cancel := context.WithTimeout(ctx, armBReportTimeout)
	defer cancel()
	response, err := r.client.RecordSkillEvolutionInteraction(requestCtx, memoryclient.SkillEvolutionInteractionRecordRequest{
		RequestID:      armBRequestID(record, proposal, "interaction-"+stage),
		Subject:        armBSubject(proposal),
		ContextProfile: armBContextProfile(record),
		Stage:          stage,
		EvidenceRefs:   []memoryclient.SkillEvolutionEvidenceRef{},
	})
	if err == nil && !response.Recorded {
		err = fmt.Errorf("Arm B interaction response was not recorded")
	}
	if err != nil {
		record.SkillEvolutionInteractionFailures++
		r.logf("[%s] Arm B interaction %s warning: %v\n", record.Arm, stage, err)
		return
	}
	record.SkillEvolutionInteractionReports++
}

func (r *armBReporter) reportDiagnosis(ctx context.Context, record *attemptRecord, proposal skillProposal, evaluationBatchID string) {
	if r == nil || r.client == nil {
		return
	}
	requestCtx, cancel := context.WithTimeout(ctx, armBReportTimeout)
	defer cancel()
	diagnosticOnly := strings.Contains(evaluationBatchID, "held-out") || strings.Contains(evaluationBatchID, ":test:")
	response, err := r.client.RecordSkillEvolutionDiagnosis(requestCtx, memoryclient.SkillEvolutionDiagnosisRecordRequest{
		RequestID:               armBRequestID(record, proposal, "diagnosis"),
		AssessmentID:            armBRequestID(record, proposal, "assessment"),
		Subject:                 armBSubject(proposal),
		ContextProfile:          armBContextProfile(record),
		ReturnedPathID:          "runner-local-path-" + proposal.SHA256[:16],
		AddressedAgentID:        "agent-primary",
		AdoptionEvidenceRefs:    []memoryclient.SkillEvolutionEvidenceRef{},
		ContributionScoreMicros: 0,
		ConfidenceMicros:        0,
		CounterevidenceRefs:     []memoryclient.SkillEvolutionEvidenceRef{},
		Rationale:               "Runner-observed diagnosis proposal only; it does not declare adoption or activation authority.",
		EvidenceRefs:            []memoryclient.SkillEvolutionEvidenceRef{},
		RubricRef:               armBVersionedRef(armBDiagnosisRubricRefID),
		EvaluationBatchID:       evaluationBatchID,
		DiagnosticOnly:          diagnosticOnly,
	})
	if err == nil && !response.Recorded {
		err = fmt.Errorf("Arm B diagnosis response was not recorded")
	}
	if err != nil {
		record.SkillEvolutionDiagnosisFailures++
		r.logf("[%s] Arm B diagnosis warning: %v\n", record.Arm, err)
		return
	}
	record.SkillEvolutionDiagnosisReports++
}

// advisoryLedger converts only text advisory candidates to ephemeral runner
// proposals. The caller supplements a copied ledger; the local ledger remains
// the frozen reference/fallback and is never written by this read path.
func (r *armBReporter) advisoryLedger(ctx context.Context, record *attemptRecord) ([]skillProposal, error) {
	if r == nil || r.client == nil {
		record.SkillAdvisoryReadStatus = "not_configured"
		return nil, nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, armBReportTimeout)
	defer cancel()
	response, err := r.client.ReadSkillEvolutionAdvisoryCandidates(requestCtx, memoryclient.SkillEvolutionAdvisoryReadRequest{
		RequestID:      armBRequestID(record, skillProposal{SHA256: "advisory"}, "advisory-read"),
		ContextProfile: armBContextProfile(record),
	})
	if err != nil {
		record.SkillEvolutionAdvisoryFailures++
		record.SkillAdvisoryReadStatus = "unavailable"
		r.logf("[%s] Arm B advisory read warning: %v\n", record.Arm, err)
		return nil, nil
	}
	byID := make(map[string]memoryclient.SkillEvolutionAdvisoryCandidate)
	for _, candidate := range response.Candidates {
		if candidate.CandidateID == "" || candidate.CandidateRef == nil || candidate.CandidateRef.CandidateID != candidate.CandidateID || candidate.CandidateRef.Kind != candidate.Kind || strings.TrimSpace(candidate.Guidance) == "" || (candidate.Kind != "human_procedure" && candidate.Kind != "step_guidance") {
			continue
		}
		byID[candidate.CandidateID] = candidate
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]skillProposal, 0, len(ids))
	for _, id := range ids {
		candidate := byID[id]
		out = append(out, skillProposal{EpisodeID: "gms-advisory-" + sanitizeID(id), SHA256: skillFingerprint(candidate.Guidance), Text: candidate.Guidance, AdvisoryRef: candidate.CandidateRef})
	}
	record.SkillEvolutionAdvisoryCandidates += len(out)
	record.SkillAdvisoryReadStatus = "ok"
	return out, nil
}

func supplementLedger(local, advisory []skillProposal) []skillProposal {
	out := append([]skillProposal(nil), local...)
	seen := make(map[string]bool, len(out))
	for _, proposal := range out {
		seen[proposal.SHA256] = true
	}
	for _, proposal := range advisory {
		if proposal.SHA256 == "" || seen[proposal.SHA256] {
			continue
		}
		seen[proposal.SHA256] = true
		out = append(out, proposal)
	}
	return out
}

func retrievedLedgerProposals(ledger []skillProposal, reply string) []skillProposal {
	var out []skillProposal
	seen := map[string]bool{}
	for _, selection := range extractRetrievedSelections(reply) {
		var matched *skillProposal
		for index := range ledger {
			proposal := &ledger[index]
			if !strings.HasPrefix(proposal.SHA256, selection.Fingerprint) || proposal.EpisodeID != selection.EpisodeID {
				continue
			}
			matched = proposal
			break
		}
		if matched != nil {
			key := matched.SHA256 + "\x1f" + matched.EpisodeID
			if matched.AdvisoryRef != nil {
				key = matched.AdvisoryRef.CandidateID + "\x1f" + matched.AdvisoryRef.BodyDigest
			}
			if !seen[key] {
				seen[key] = true
				out = append(out, *matched)
			}
		}
	}
	return out
}

func retrievalAddressedToTeammates(reply string) bool {
	return strings.HasPrefix(strings.TrimSpace(reply), "@task-agent @memory-agent")
}

func armBContextProfile(record *attemptRecord) memoryclient.SkillEvolutionContextProfile {
	return memoryclient.SkillEvolutionContextProfile{
		SchemaVersion: "context-profile/1.0", TaskFamily: record.FamilyID, RuntimeClass: "pi-group-chat-host/go-linux",
		WorkspaceFeatureTags: []string{"bench-runner", "warm-skill"},
		ObservableGuardFacts: []string{"gms-advisory-primary", "local-ledger-comparison-only"},
		ToolPolicyRef:        armBVersionedRef(armBToolPolicyRefID), EnvironmentClass: "sandboxed-linux",
	}
}

func armBSubject(proposal skillProposal) memoryclient.SkillEvolutionSubject {
	if proposal.AdvisoryRef != nil {
		return memoryclient.SkillEvolutionSubject{CandidateRef: proposal.AdvisoryRef}
	}
	fingerprint := proposal.SHA256
	if len(fingerprint) != sha256.Size*2 {
		fingerprint = skillFingerprint(proposal.Text)
	}
	return memoryclient.SkillEvolutionSubject{CandidateRef: &memoryclient.SkillEvolutionCandidateRef{
		SchemaVersion: "gms.candidate-artifact-ref.v2", CandidateID: "runner-local-skill-" + fingerprint[:16], Kind: "human_procedure", BodyDigest: "sha256:" + fingerprint,
		OriginType: "skill_proposal", OriginRef: armBVersionedRef("runner-proposal-" + sanitizeID(proposal.EpisodeID)),
	}}
}

func armBVersionedRef(id string) memoryclient.SkillEvolutionVersionedRef {
	return memoryclient.SkillEvolutionVersionedRef{ID: id, Version: 1, Digest: armBDigest(id)}
}

func armBDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("sha256:%x", digest[:])
}

func armBRequestID(record *attemptRecord, proposal skillProposal, kind string) string {
	identity := strings.Join([]string{record.EvaluationID, record.Arm, record.EpisodeID, proposal.SHA256, kind}, ":")
	return "armb-" + sanitizeID(kind) + "-" + skillFingerprint(identity)[:20]
}

func armBEvaluationBatchID(record *attemptRecord) string {
	if record.Split == "test" {
		return "held-out:" + sanitizeID(record.EvaluationID) + ":" + sanitizeID(record.Arm) + ":" + sanitizeID(record.EpisodeID)
	}
	return "training:" + sanitizeID(record.EvaluationID) + ":" + sanitizeID(record.Arm) + ":" + sanitizeID(record.EpisodeID)
}

// c3RetrievalGuidance makes formal GMS advisory material the sole model-visible
// skill source. The runner-local ledger is returned only as comparison data by
// the caller and is never reinserted when GMS is unavailable.
func c3RetrievalGuidance(advisory, local []skillProposal, advisoryStatus string) ([]skillProposal, string) {
	if advisoryStatus != "ok" || len(advisory) == 0 {
		return nil, "none"
	}
	return append([]skillProposal(nil), advisory...), "advisory_read"
}

func c3RecordExposure(record *attemptRecord, proposal skillProposal, source, stage string) {
	exposure := skillExposure{Source: source, Stage: stage, CandidateRef: proposal.AdvisoryRef}
	if proposal.AdvisoryRef == nil {
		exposure.LocalSHA256 = proposal.SHA256
	}
	record.SkillExposure = append(record.SkillExposure, exposure)
}

type c3LifecycleSummary struct {
	Status   string
	Outcomes []memoryclient.SkillEvolutionCandidateOutcome
}

func (r *armBReporter) c3LifecycleSummary(ctx context.Context, refs []memoryclient.SkillEvolutionCandidateRef) c3LifecycleSummary {
	if r == nil || r.client == nil || len(refs) == 0 {
		return c3LifecycleSummary{Status: "no_refs"}
	}
	requestCtx, cancel := context.WithTimeout(ctx, armBReportTimeout)
	defer cancel()
	response, err := r.client.ReadSkillEvolutionCandidateOutcomes(requestCtx, memoryclient.SkillEvolutionCandidateOutcomesReadRequest{RequestID: "arm-c3-lifecycle", CandidateRefs: refs})
	if err != nil {
		r.logf("C3 lifecycle outcome read warning: %v\n", err)
		return c3LifecycleSummary{Status: "unavailable"}
	}
	return c3LifecycleSummary{Status: "ok", Outcomes: response.Outcomes}
}
