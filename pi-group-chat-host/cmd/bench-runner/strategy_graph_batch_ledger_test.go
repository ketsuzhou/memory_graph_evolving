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

func TestResolvePinnedNominationFourFormsAndRejects(t *testing.T) {
	t.Parallel()
	ledger, err := loadPinned0917Ledger()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	skill14 := ledger.Skills[13]
	skill4 := ledger.Skills[3]
	skill3 := ledger.Skills[2]

	exact, ok, leftover := ledger.resolveNomination(skill14.SkillReference)
	if !ok || exact.SkillReference != skill14.SkillReference || leftover != "" {
		t.Fatalf("exact ref = %#v ok=%v leftover=%q", exact, ok, leftover)
	}
	prefix, ok, leftover := ledger.resolveNomination(skill14.SHA256[:12])
	if !ok || prefix.SkillReference != skill14.SkillReference {
		t.Fatalf("sha256 prefix = %#v ok=%v leftover=%q want %s", prefix, ok, leftover, skill14.SkillReference)
	}
	entry, ok, leftover := ledger.resolveNomination("03")
	if !ok || entry.SkillReference != skill3.SkillReference {
		t.Fatalf("entry 03 = %#v ok=%v leftover=%q want %s", entry, ok, leftover, skill3.SkillReference)
	}
	entryPin, ok, leftover := ledger.resolveNomination("pin0917-03")
	if !ok || entryPin.SkillReference != skill3.SkillReference {
		t.Fatalf("entry pin0917-03 = %#v ok=%v leftover=%q want %s", entryPin, ok, leftover, skill3.SkillReference)
	}
	entryURI, ok, leftover := ledger.resolveNomination("skill://evaluation/4@pin0917-04", "83f3c1fc4f22")
	if !ok || entryURI.SkillReference != skill4.SkillReference {
		t.Fatalf("entry uri = %#v ok=%v leftover=%q want %s", entryURI, ok, leftover, skill4.SkillReference)
	}
	named, ok, leftover := ledger.resolveNomination("Verify-Expectations-Before-Modifying-Code")
	if !ok || named.SkillReference != skill3.SkillReference {
		t.Fatalf("name = %#v ok=%v leftover=%q want %s", named, ok, leftover, skill3.SkillReference)
	}

	if skill, ok, leftover := ledger.resolveNomination("skill://evaluation/missing@1", "ffffffff"); ok || leftover == "" {
		t.Fatalf("zero match must stay unresolved: %#v ok=%v leftover=%q", skill, ok, leftover)
	}

	dup := &pinnedLedger{
		Skills: []pinnedSkill{
			{SkillReference: "skill://evaluation/pin0917-01@1", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Name: "Foo-Bar", ID: "pin0917-01"},
			{SkillReference: "skill://evaluation/pin0917-02@1", SHA256: "aaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Name: "foo_bar", ID: "pin0917-02"},
		},
		ByRef:    map[string]pinnedSkill{},
		BySHA256: map[string]pinnedSkill{},
	}
	for _, skill := range dup.Skills {
		dup.ByRef[skill.SkillReference] = skill
		dup.BySHA256[skill.SHA256] = skill
	}
	if skill, ok, leftover := dup.resolveNomination("", "aaaaaaaa"); ok || leftover == "" {
		t.Fatalf("ambiguous sha256 prefix must reject: %#v ok=%v leftover=%q", skill, ok, leftover)
	}
	if skill, ok, leftover := dup.resolveNomination("FOO_BAR"); ok || leftover == "" {
		t.Fatalf("ambiguous name must reject: %#v ok=%v leftover=%q", skill, ok, leftover)
	}

	smoke14 := "SKILL_SELECTION\nskill_id: skill://evaluation/pin0917-14@9e9f60ee3752\nsha256: 9e9f60ee3752\ntrigger: After implementing the core algorithm\n"
	got, unresolved := parseGraphBatchNominations(smoke14, ledger)
	if len(unresolved) != 0 || len(got) != 1 || got[0].SkillReference != skill14.SkillReference {
		t.Fatalf("smoke abc320 parse = %#v unresolved=%v", got, unresolved)
	}
	smoke4 := "SKILL_SELECTION\nskill_id: skill://evaluation/4@pin0917-04\nsha256: 83f3c1fc4f22\ntrigger: Task allows modifying one element\n"
	got, unresolved = parseGraphBatchNominations(smoke4, ledger)
	if len(unresolved) != 0 || len(got) != 1 || got[0].SkillReference != skill4.SkillReference {
		t.Fatalf("smoke 3423 parse = %#v unresolved=%v", got, unresolved)
	}
	got, unresolved = parseGraphBatchNominations("SKILL_SELECTION\nskill_id: skill://evaluation/missing@1\nsha256: ffffffffdeadbeef\ntrigger: none\n", ledger)
	if len(got) != 0 || len(unresolved) != 1 {
		t.Fatalf("zero-match parse = %#v unresolved=%v", got, unresolved)
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
