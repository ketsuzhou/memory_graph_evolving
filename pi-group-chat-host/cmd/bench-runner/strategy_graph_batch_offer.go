package main

import (
	"context"
	"fmt"
	"strings"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
)

const (
	graphBatchTaskAgentID   = "agent-primary"
	graphBatchMemoryAgentID = "agent-memory"
	graphBatchOfferAuthor   = "host-skill-offer"
)

type graphBatchOfferSpec struct {
	OfferID        string
	SkillReference string
	SHA256         string
	Trigger        string
	Name           string
	BodyDigest     string
	Body           string
}

type graphBatchOfferAssembly struct {
	Content          string
	RecipientAgentID string
	DisplayMentions  []string
	Offers           []graphBatchOfferSpec
}

// assembleDirectedSkillOffer builds the user-confirmed offer shape: one
// canonical room body that @-mentions both the task and memory agents for
// display, while the structured recipient is only the task agent.
func assembleDirectedSkillOffer(attemptID string, skills []pinnedSkill) (graphBatchOfferAssembly, error) {
	if len(skills) == 0 {
		return graphBatchOfferAssembly{}, fmt.Errorf("directed offer requires at least one selected skill")
	}
	if len(skills) > 3 {
		return graphBatchOfferAssembly{}, fmt.Errorf("directed offer refuses more than 3 skills (got %d)", len(skills))
	}
	var body strings.Builder
	fmt.Fprintf(&body, "@%s @%s DIRECTED SKILL OFFER\n", graphBatchTaskAgentID, graphBatchMemoryAgentID)
	body.WriteString("The Host is delivering the following exact skill references to the task agent. Memory is copied for display only and has no new turn.\n")
	offers := make([]graphBatchOfferSpec, 0, len(skills))
	for i, skill := range skills {
		offerID := fmt.Sprintf("offer-%s-%02d", sanitizeID(attemptID), i+1)
		offers = append(offers, graphBatchOfferSpec{
			OfferID:        offerID,
			SkillReference: skill.SkillReference,
			SHA256:         skill.SHA256,
			Trigger:        skill.Trigger,
			Name:           skill.Name,
			BodyDigest:     skill.BodyDigest,
			Body:           skill.Body,
		})
		fmt.Fprintf(&body, "\n--- offer %s ---\n", offerID)
		fmt.Fprintf(&body, "skill_reference: %s\n", skill.SkillReference)
		fmt.Fprintf(&body, "sha256: %s\n", skill.SHA256)
		fmt.Fprintf(&body, "content_digest: %s\n", skill.BodyDigest)
		fmt.Fprintf(&body, "name: %s\n", skill.Name)
		fmt.Fprintf(&body, "trigger: %s\n", skill.Trigger)
	}
	return graphBatchOfferAssembly{
		Content:          body.String(),
		RecipientAgentID: graphBatchTaskAgentID,
		DisplayMentions:  []string{graphBatchTaskAgentID, graphBatchMemoryAgentID},
		Offers:           offers,
	}, nil
}

func publishDirectedSkillOffer(ctx context.Context, coordinator *directedoffer.Coordinator, roomID, attemptID string, assembly graphBatchOfferAssembly) (directedoffer.Result, error) {
	if coordinator == nil {
		return directedoffer.Result{}, fmt.Errorf("directed offer coordinator is required")
	}
	if strings.TrimSpace(assembly.RecipientAgentID) != graphBatchTaskAgentID {
		return directedoffer.Result{}, fmt.Errorf("directed offer recipient %q must be the task agent", assembly.RecipientAgentID)
	}
	if !strings.Contains(assembly.Content, "@"+graphBatchTaskAgentID) || !strings.Contains(assembly.Content, "@"+graphBatchMemoryAgentID) {
		return directedoffer.Result{}, fmt.Errorf("directed offer content must display-mention both agents")
	}
	ref := ""
	if len(assembly.Offers) > 0 {
		ref = assembly.Offers[0].SkillReference
	}
	return coordinator.Offer(ctx, directedoffer.Request{
		RoomID:           roomID,
		AuthorID:         graphBatchOfferAuthor,
		Content:          assembly.Content,
		IdempotencyKey:   "skill-offer-" + sanitizeID(attemptID),
		RecipientAgentID: graphBatchTaskAgentID,
		SkillReference:   ref,
	})
}

func graphBatchTaskUptakePrompt(assembly graphBatchOfferAssembly, taskPrompt string) string {
	var builder strings.Builder
	builder.WriteString("Directed skill offers are in this room message. They are instructions for you, the task agent.\n")
	builder.WriteString("Before using bash, write, or any other problem-solving tool you must, for each offered skill:\n")
	builder.WriteString("1. Call skill_get with the exact skill_reference printed below.\n")
	builder.WriteString("2. Call skill_feedback with accepted or rejected and a short reason. Accepting a skill requires naming it.\n")
	builder.WriteString("Then solve the task. If you reject a skill, say why and proceed without it.\n\n")
	builder.WriteString(assembly.Content)
	builder.WriteString("\n--- task ---\n")
	builder.WriteString(taskPrompt)
	return builder.String()
}
