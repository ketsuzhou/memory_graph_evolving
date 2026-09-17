package main

import (
	"context"
	"strings"
	"testing"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
)

func TestAssembleDirectedSkillOfferMentionsBothRoutesOnlyTask(t *testing.T) {
	t.Parallel()
	ledger, err := loadPinned0917Ledger()
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	assembly, err := assembleDirectedSkillOffer("abc320_a", ledger.Skills[:1])
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if assembly.RecipientAgentID != graphBatchTaskAgentID {
		t.Fatalf("recipient = %q, want %q", assembly.RecipientAgentID, graphBatchTaskAgentID)
	}
	if !strings.Contains(assembly.Content, "@"+graphBatchTaskAgentID) || !strings.Contains(assembly.Content, "@"+graphBatchMemoryAgentID) {
		t.Fatalf("content missing display mentions:\n%s", assembly.Content)
	}
	if !strings.Contains(assembly.Content, ledger.Skills[0].SkillReference) || !strings.Contains(assembly.Content, ledger.Skills[0].BodyDigest) {
		t.Fatalf("content missing exact ref or digest:\n%s", assembly.Content)
	}
	got, err := publishDirectedSkillOffer(context.Background(), directedoffer.NewCoordinator(), "room-test", "abc320_a", assembly)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(got.Deliveries) != 1 {
		t.Fatalf("deliveries = %#v, want exactly 1", got.Deliveries)
	}
	if got.Deliveries[0].AgentID != graphBatchTaskAgentID {
		t.Fatalf("delivery agent = %q, want task only", got.Deliveries[0].AgentID)
	}
}

func TestGraphBatchTaskUptakePromptHasNoDemotionHeader(t *testing.T) {
	t.Parallel()
	assembly := graphBatchOfferAssembly{
		Content: "@agent-primary @agent-memory DIRECTED SKILL OFFER\nskill_reference: skill://evaluation/pin0917-01@1\n",
		Offers:  []graphBatchOfferSpec{{SkillReference: "skill://evaluation/pin0917-01@1", Name: "demo"}},
	}
	prompt := graphBatchTaskUptakePrompt(assembly, "Solve A^B+B^A")
	if strings.Contains(prompt, "Memory from earlier sessions") || strings.Contains(prompt, "background material only") {
		t.Fatalf("uptake prompt leaked demotion copy:\n%s", prompt)
	}
	if !strings.Contains(prompt, "skill_get") || !strings.Contains(prompt, "skill_feedback") {
		t.Fatalf("uptake prompt missing protocol verbs:\n%s", prompt)
	}
}

func TestSkillOfferToolsJSRegistersBothTools(t *testing.T) {
	t.Parallel()
	js := skillOfferToolsJS([]graphBatchOfferSpec{{
		SkillReference: "skill://evaluation/pin0917-01@1",
		SHA256:         strings.Repeat("a", 64),
		BodyDigest:     "sha256:" + strings.Repeat("a", 64),
		Name:           "demo",
		Body:           "CONSOLIDATED SKILL\nname: demo",
	}})
	if !strings.Contains(js, `name: "skill_get"`) || !strings.Contains(js, `name: "skill_feedback"`) {
		t.Fatalf("extension missing tools:\n%s", js)
	}
	if !strings.Contains(js, "skill://evaluation/pin0917-01@1") {
		t.Fatalf("extension missing baked reference")
	}
}
