package main

import (
	"context"
	"errors"
	"testing"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
)

func TestC3AdvisoryIsPrimaryAndLocalLedgerIsComparisonOnly(t *testing.T) {
	local := []skillProposal{{EpisodeID: "train-1", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Text: "local guidance"}}
	advisory := []skillProposal{{EpisodeID: "gms-advisory-candidate-1", SHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Text: "gms guidance", AdvisoryRef: &memoryclient.SkillEvolutionCandidateRef{SchemaVersion: "gms.candidate-artifact-ref.v2", CandidateID: "candidate-1", Kind: "step_guidance", BodyDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", OriginType: "skill_proposal", OriginRef: memoryclient.SkillEvolutionVersionedRef{ID: "proposal-1", Version: 1, Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}}}}
	guidance, source := c3RetrievalGuidance(advisory, local, "ok")
	if source != "advisory_read" || len(guidance) != 1 || guidance[0].Text != "gms guidance" || guidance[0].AdvisoryRef == nil {
		t.Fatalf("advisory-primary guidance = %#v source=%q", guidance, source)
	}
	if local[0].Text != "local guidance" {
		t.Fatalf("local comparison ledger mutated: %#v", local)
	}
}

func TestC3AdvisoryUnavailableDoesNotReinsertLocalLedger(t *testing.T) {
	local := []skillProposal{{EpisodeID: "train-1", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Text: "local guidance"}}
	guidance, source := c3RetrievalGuidance(nil, local, "unavailable")
	if source != "none" || len(guidance) != 0 {
		t.Fatalf("unavailable must fail open without local authority: %#v source=%q", guidance, source)
	}
}

func TestC3ExposureRecordKeepsGMSCandidateRevision(t *testing.T) {
	ref := &memoryclient.SkillEvolutionCandidateRef{SchemaVersion: "gms.candidate-artifact-ref.v2", CandidateID: "candidate-1", Kind: "step_guidance", BodyDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", OriginType: "skill_proposal", OriginRef: memoryclient.SkillEvolutionVersionedRef{ID: "proposal-1", Version: 1, Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}}
	proposal := skillProposal{EpisodeID: "gms-advisory-candidate-1", SHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Text: "gms guidance", AdvisoryRef: ref}
	record := attemptRecord{}
	c3RecordExposure(&record, proposal, "advisory_read", "selected")
	if len(record.SkillExposure) != 1 || record.SkillExposure[0].Source != "advisory_read" || record.SkillExposure[0].CandidateRef == nil || record.SkillExposure[0].CandidateRef.CandidateID != "candidate-1" {
		t.Fatalf("exposure = %#v", record.SkillExposure)
	}
}

func TestC3LifecycleReadIsNonGradingOnGMSUnavailable(t *testing.T) {
	fake := &fakeArmBClient{err: errors.New("GMS unavailable")}
	reporter := newArmBReporter(fake, func(string, ...any) {})
	summary := reporter.c3LifecycleSummary(context.Background(), []memoryclient.SkillEvolutionCandidateRef{{CandidateID: "candidate-1"}})
	if summary.Status != "unavailable" || len(summary.Outcomes) != 0 {
		t.Fatalf("summary = %#v", summary)
	}
	_ = context.Background
}

func TestC3ArmSummaryCollectsCandidateRefsAndLifecycleOutcomes(t *testing.T) {
	ref := memoryclient.SkillEvolutionCandidateRef{SchemaVersion: "gms.candidate-artifact-ref.v2", CandidateID: "candidate-1", Kind: "step_guidance", BodyDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", OriginType: "skill_proposal", OriginRef: memoryclient.SkillEvolutionVersionedRef{ID: "proposal-1", Version: 1, Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}}
	summary := &c3ArmSummary{ExposureSources: map[string]int{}}
	summary.observe(attemptRecord{SkillAdvisoryReadStatus: "ok", SkillRetrievalAdvisoryCount: 1, SkillExposure: []skillExposure{{Source: "advisory_read", CandidateRef: &ref}}})
	fake := &fakeArmBClient{outcomes: memoryclient.SkillEvolutionCandidateOutcomesReadResponse{Outcomes: []memoryclient.SkillEvolutionCandidateOutcome{{CandidateRef: ref, Status: "activated"}}}}
	lifecycle := newArmBReporter(fake, func(string, ...any) {}).c3LifecycleSummary(context.Background(), summary.CandidateRefs)
	if summary.AdvisoryReadOK != 1 || summary.ExposureSources["advisory_read"] != 1 || len(summary.CandidateRefs) != 1 || lifecycle.Status != "ok" || lifecycle.Outcomes[0].Status != "activated" {
		t.Fatalf("summary/lifecycle = %#v %#v", summary, lifecycle)
	}
}

func TestC3SameGuidanceAdvisoriesRetainExactCandidateAttribution(t *testing.T) {
	guidance := "same guidance"
	fingerprint := skillFingerprint(guidance)
	first := skillProposal{EpisodeID: "gms-advisory-candidate-a", SHA256: fingerprint, Text: guidance, AdvisoryRef: &memoryclient.SkillEvolutionCandidateRef{CandidateID: "candidate-a", BodyDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
	second := skillProposal{EpisodeID: "gms-advisory-candidate-b", SHA256: fingerprint, Text: guidance, AdvisoryRef: &memoryclient.SkillEvolutionCandidateRef{CandidateID: "candidate-b", BodyDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}
	selected := retrievedLedgerProposals([]skillProposal{first, second}, "@task-agent @memory-agent\n[skill "+fingerprint[:12]+" from episode gms-advisory-candidate-b]")
	if len(selected) != 1 || selected[0].AdvisoryRef == nil || selected[0].AdvisoryRef.CandidateID != "candidate-b" {
		t.Fatalf("selected=%#v", selected)
	}
}
