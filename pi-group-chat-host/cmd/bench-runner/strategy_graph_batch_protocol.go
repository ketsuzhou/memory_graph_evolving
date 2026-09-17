package main

import (
	"encoding/json"
	"strings"
	"sync"

	"river2.dev/pi-group-chat-host/internal/runtime"
	"river2.dev/pi-group-chat-host/internal/skilldisposition"
)

type skillOfferAttempt struct {
	OfferID        string `json:"offer_id"`
	SkillReference string `json:"skill_reference"`
	SHA256         string `json:"sha256"`
	Trigger        string `json:"trigger,omitempty"`
	Name           string `json:"name,omitempty"`
	BodyDigest     string `json:"body_digest"`
}

type skillServedAttempt struct {
	OfferID        string `json:"offer_id"`
	SkillReference string `json:"skill_reference"`
	BodyDigest     string `json:"body_digest"`
	ToolCallID     string `json:"tool_call_id,omitempty"`
}

type graphBatchSkillProtocol struct {
	mu            sync.Mutex
	offers        []graphBatchOfferSpec
	memoryAgentID string
	taskAgentID   string
	served        []skillServedAttempt
	disposition   string
	reason        string
	repairUsed    bool
	ordinaryEarly int
	feedbacks     []skilldispositionPublication
}

type skilldispositionPublication struct {
	Marker  string
	OfferID string
	Skill   string
	Reason  string
}

func newGraphBatchSkillProtocol(offers []graphBatchOfferSpec) *graphBatchSkillProtocol {
	return &graphBatchSkillProtocol{
		offers:        append([]graphBatchOfferSpec(nil), offers...),
		memoryAgentID: graphBatchMemoryAgentID,
		taskAgentID:   graphBatchTaskAgentID,
	}
}

func (p *graphBatchSkillProtocol) ObserveToolStart(tool, callID, args string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch tool {
	case skillGetToolName:
		ref := protocolArg(args, "skill_reference", "skill_id")
		skill, ok := p.lookupLocked(ref)
		if !ok {
			p.repairUsed = true
			return
		}
		p.served = append(p.served, skillServedAttempt{
			OfferID:        skill.OfferID,
			SkillReference: skill.SkillReference,
			BodyDigest:     skill.BodyDigest,
			ToolCallID:     callID,
		})
	case skillFeedbackToolName:
		ref := protocolArg(args, "skill_reference", "skill_id")
		disposition := strings.ToLower(protocolArg(args, "disposition"))
		reason := protocolArg(args, "reason")
		skill, ok := p.lookupLocked(ref)
		if !ok && len(p.offers) == 1 {
			skill, ok = p.offers[0], true
		}
		if !ok || (disposition != skilldisposition.DispositionAccepted && disposition != skilldisposition.DispositionRejected) {
			p.repairUsed = true
			return
		}
		if !p.hasServedLocked(skill.SkillReference) {
			p.repairUsed = true
			return
		}
		p.disposition = disposition
		p.reason = reason
		marker := skilldisposition.MarkerAccepted
		if disposition == skilldisposition.DispositionRejected {
			marker = skilldisposition.MarkerRejected
		}
		p.feedbacks = append(p.feedbacks, skilldispositionPublication{
			Marker:  marker,
			OfferID: skill.OfferID,
			Skill:   skill.SkillReference,
			Reason:  reason,
		})
	default:
		if p.disposition == "" && len(p.offers) > 0 && isProblemSolvingTool(tool) {
			p.ordinaryEarly++
			if p.ordinaryEarly > 1 {
				p.disposition = skilldisposition.DispositionProtocolError
				p.reason = "ordinary tool before skill protocol"
			} else {
				p.repairUsed = true
			}
		}
	}
}

func (p *graphBatchSkillProtocol) ObserveToolEnd(tool, callID string, isError bool, result string) {
}

func (p *graphBatchSkillProtocol) PublishFeedback(messages []runtime.VisibleMessage) []runtime.VisibleMessage {
	if p == nil {
		return messages
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, item := range p.feedbacks {
		content := formatGraphBatchDispositionMessage(p.memoryAgentID, item.Marker, item.OfferID, item.Skill)
		if item.Reason != "" {
			content += " reason=" + item.Reason
		}
		messages = append(messages, runtime.VisibleMessage{
			AuthorID: p.taskAgentID,
			Content:  content,
		})
	}
	return messages
}

func (p *graphBatchSkillProtocol) snapshot(named bool) (offers []skillOfferAttempt, served []skillServedAttempt, disposition, reason string) {
	if p == nil {
		return nil, nil, "", ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	offers = make([]skillOfferAttempt, 0, len(p.offers))
	for _, offer := range p.offers {
		offers = append(offers, skillOfferAttempt{
			OfferID:        offer.OfferID,
			SkillReference: offer.SkillReference,
			SHA256:         offer.SHA256,
			Trigger:        offer.Trigger,
			Name:           offer.Name,
			BodyDigest:     offer.BodyDigest,
		})
	}
	served = append([]skillServedAttempt(nil), p.served...)
	disposition = p.disposition
	reason = p.reason
	if disposition == "" && len(p.offers) > 0 {
		if p.repairUsed {
			disposition = skilldisposition.DispositionProtocolError
			if reason == "" {
				reason = "skill protocol incomplete after one repair"
			}
		}
	}
	_ = named
	return offers, served, disposition, reason
}

func (p *graphBatchSkillProtocol) lookupLocked(ref string) (graphBatchOfferSpec, bool) {
	ref = strings.TrimSpace(ref)
	for _, offer := range p.offers {
		if offer.SkillReference == ref || offer.SHA256 == ref || offer.OfferID == ref {
			return offer, true
		}
	}
	return graphBatchOfferSpec{}, false
}

func (p *graphBatchSkillProtocol) hasServedLocked(ref string) bool {
	for _, item := range p.served {
		if item.SkillReference == ref {
			return true
		}
	}
	return false
}

func protocolArg(raw string, keys ...string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(raw), &object); err != nil {
		return ""
	}
	for _, key := range keys {
		if value, ok := object[key]; ok {
			if text, ok := value.(string); ok {
				return strings.TrimSpace(text)
			}
		}
	}
	return ""
}

func isProblemSolvingTool(name string) bool {
	switch name {
	case skillGetToolName, skillFeedbackToolName, "room_send", "room_reply", "room_react":
		return false
	default:
		return true
	}
}

func formatGraphBatchDispositionMessage(memoryID, marker, offerID, skillRef string) string {
	return "@" + memoryID + " " + marker + " offer=" + offerID + " skill=" + skillRef
}

func namedInReasoning(text string, offers []graphBatchOfferSpec) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	lower := strings.ToLower(text)
	for _, offer := range offers {
		if offer.Name != "" && strings.Contains(lower, strings.ToLower(offer.Name)) {
			return true
		}
		if offer.SkillReference != "" && strings.Contains(text, offer.SkillReference) {
			return true
		}
		if len(offer.SHA256) >= 12 && strings.Contains(lower, offer.SHA256[:12]) {
			return true
		}
	}
	return false
}
