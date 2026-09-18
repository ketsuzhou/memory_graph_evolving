package main

import (
	"strings"
	"testing"

	"river2.dev/pi-group-chat-host/internal/skillfence"
)

func TestLoadPinned0917LedgerFailClosed(t *testing.T) {
	t.Parallel()
	ledger, err := loadPinned0917Ledger()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(ledger.Skills) != pinned0917ExpectedCount {
		t.Fatalf("skills = %d, want %d", len(ledger.Skills), pinned0917ExpectedCount)
	}
	for i, skill := range ledger.Skills {
		if _, err := skillfence.ParseExactSkillReference(skill.SkillReference); err != nil {
			t.Fatalf("skill[%d] ref %s: %v", i, skill.SkillReference, err)
		}
		if !strings.HasPrefix(skill.BodyDigest, "sha256:") || len(skill.SHA256) != 64 {
			t.Fatalf("skill[%d] digest malformed: %#v", i, skill)
		}
		if skill.Name == "" || skill.Trigger == "" || !strings.HasPrefix(skill.Body, "CONSOLIDATED SKILL") {
			t.Fatalf("skill[%d] incomplete: %#v", i, skill)
		}
	}
	if _, err := parsePinnedLedger([]byte("not a ledger")); err == nil {
		t.Fatal("empty ledger must fail closed")
	}
}

func TestParseGraphBatchSkillSelections(t *testing.T) {
	t.Parallel()
	ledger, err := loadPinned0917Ledger()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	skill := ledger.Skills[0]
	reply := "SKILL_SELECTION\nskill_id: " + skill.SkillReference + "\nsha256: " + skill.SHA256 + "\ntrigger: " + skill.Trigger + "\n"
	got := parseGraphBatchSkillSelections(reply, ledger)
	if len(got) != 1 || got[0].SkillReference != skill.SkillReference {
		t.Fatalf("selections = %#v", got)
	}
	if parseGraphBatchSkillSelections("NO_SKILL", ledger) != nil {
		t.Fatal("NO_SKILL must yield no selections")
	}
	if strings.Contains(graphBatchRetrievalPromptHead, "background material only, not addressed to any agent") {
		t.Fatal("graph-batch retrieval prompt must not reuse the legacy demotion header")
	}
	if !strings.Contains(graphBatchRetrievalPromptHead, "directed offer") {
		t.Fatal("graph-batch retrieval prompt must say Host will direct the offer")
	}
	if strings.Contains(graphBatchRetrievalPromptHead, "the trigger must match the task's shape") {
		t.Fatal("graph-batch retrieval must not require exact trigger match")
	}
	if !strings.Contains(graphBatchRetrievalPromptHead, "candidate nomination") {
		t.Fatal("graph-batch retrieval must say selection is a nomination, not a verdict")
	}
	if !strings.Contains(graphBatchRetrievalPromptHead, "skill_feedback") || !strings.Contains(graphBatchRetrievalPromptHead, "normal signal") {
		t.Fatal("graph-batch retrieval must defer applicability to the task agent's accept/reject fence")
	}
	if !strings.Contains(graphBatchRetrievalPromptHead, "completely unrelated") {
		t.Fatal("graph-batch retrieval must reserve NO_SKILL for complete unrelatedness")
	}
	if !strings.Contains(retrievalPromptHead, "background material only, not addressed to any agent") {
		t.Fatal("legacy retrieval prompt must stay unchanged")
	}
	if !strings.Contains(retrievalPromptHead, "the trigger must match the task's shape") {
		t.Fatal("legacy retrieval prompt must keep its exact-match selection rule")
	}
}

func TestPartitionAttachesEpisodeToHeldOut(t *testing.T) {
	t.Parallel()
	_, heldOut := partitionGraphBatchEpisodes([]manifestEpisode{
		{EpisodeID: "te-1", Split: "test", TaskID: "t2", FamilyID: "fam"},
	})
	if len(heldOut) != 1 || heldOut[0].Episode == nil || heldOut[0].Episode.EpisodeID != "te-1" {
		t.Fatalf("held-out episode pointer = %#v", heldOut)
	}
}

func TestGraphBatchSkillProtocolRecordsServedAndDisposition(t *testing.T) {
	t.Parallel()
	offer := graphBatchOfferSpec{
		OfferID:        "offer-1",
		SkillReference: "skill://evaluation/pin0917-01@1",
		SHA256:         strings.Repeat("ab", 32),
		BodyDigest:     "sha256:" + strings.Repeat("ab", 32),
		Name:           "demo",
	}
	protocol := newGraphBatchSkillProtocol([]graphBatchOfferSpec{offer})
	protocol.ObserveToolStart(skillGetToolName, "call-get", `{"skill_reference":"skill://evaluation/pin0917-01@1"}`)
	protocol.ObserveToolStart(skillFeedbackToolName, "call-fb", `{"skill_reference":"skill://evaluation/pin0917-01@1","disposition":"accepted","reason":"use demo"}`)
	offers, served, disposition, reason := protocol.snapshot(false)
	if len(offers) != 1 || len(served) != 1 {
		t.Fatalf("offers=%#v served=%#v", offers, served)
	}
	if served[0].BodyDigest != offer.BodyDigest {
		t.Fatalf("served digest = %q, want %q", served[0].BodyDigest, offer.BodyDigest)
	}
	if disposition != "accepted" || !strings.Contains(reason, "demo") {
		t.Fatalf("disposition=%q reason=%q", disposition, reason)
	}
	messages := protocol.PublishFeedback(nil)
	if len(messages) != 1 || !strings.Contains(messages[0].Content, "SKILL_ACCEPTED") || !strings.Contains(messages[0].Content, "@"+graphBatchMemoryAgentID) {
		t.Fatalf("feedback messages = %#v", messages)
	}
}
