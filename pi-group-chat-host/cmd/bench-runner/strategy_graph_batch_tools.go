package main

import (
	"encoding/json"
	"strconv"
)

const (
	skillGetToolName      = "skill_get"
	skillFeedbackToolName = "skill_feedback"
)

// skillOfferToolsJS registers skill_get / skill_feedback on the ordinary
// task-agent extension. Bodies are baked from the directed offer so the
// model receives exact bytes in the same call; Host observes the tool
// events to record served digests and disposition.
func skillOfferToolsJS(offers []graphBatchOfferSpec) string {
	type baked struct {
		SkillReference string `json:"skill_reference"`
		SHA256         string `json:"sha256"`
		BodyDigest     string `json:"body_digest"`
		Name           string `json:"name"`
		Body           string `json:"body"`
	}
	items := make([]baked, 0, len(offers))
	for _, offer := range offers {
		items = append(items, baked{
			SkillReference: offer.SkillReference,
			SHA256:         offer.SHA256,
			BodyDigest:     offer.BodyDigest,
			Name:           offer.Name,
			Body:           offer.Body,
		})
	}
	raw, err := json.Marshal(items)
	if err != nil {
		raw = []byte("[]")
	}
	return `
  const OFFERED_SKILLS = ` + string(raw) + `;
  const offeredByRef = {};
  for (const skill of OFFERED_SKILLS) {
    offeredByRef[skill.skill_reference] = skill;
    offeredByRef[skill.sha256] = skill;
  }
  let skillGetRepairs = 0;
  const servedRefs = {};
  pi.registerTool({
    name: "` + skillGetToolName + `",
    label: "Skill get",
    description: "Fetch the exact body of one directed skill offer. Call this with the offer's exact skill_reference before skill_feedback.",
    parameters: {
      type: "object",
      properties: {
        skill_reference: { type: "string", description: "Exact skill://evaluation/<id>@<revision> from the directed offer." },
        skill_id: { type: "string", description: "Alias of skill_reference." },
      },
      additionalProperties: false,
    },
    async execute(_toolCallId, args) {
      const ref = String((args && (args.skill_reference || args.skill_id)) || "").trim();
      const skill = offeredByRef[ref];
      if (!skill) {
        skillGetRepairs += 1;
        const suffix = skillGetRepairs > 1
          ? " protocol_error: fence unlocked so you may proceed to solve."
          : " One repair remains: retry skill_get with an exact offered skill_reference.";
        return { content: [{ type: "text", text: "skill_get failed: " + ref + " is not an exact offered skill reference." + suffix }] };
      }
      servedRefs[skill.skill_reference] = skill.body_digest;
      return { content: [{ type: "text", text: "SKILL_BODY skill_reference=" + skill.skill_reference + " content_digest=" + skill.body_digest + "\n" + skill.body }] };
    },
  });
  pi.registerTool({
    name: "` + skillFeedbackToolName + `",
    label: "Skill feedback",
    description: "Declare accepted or rejected for one offered skill after skill_get. Accepting a skill requires naming it.",
    parameters: {
      type: "object",
      properties: {
        skill_reference: { type: "string", description: "Exact offered skill_reference." },
        skill_id: { type: "string", description: "Alias of skill_reference." },
        disposition: { type: "string", description: "accepted or rejected" },
        reason: { type: "string", description: "Why you accept or reject the skill." },
      },
      required: ["disposition"],
      additionalProperties: false,
    },
    async execute(_toolCallId, args) {
      const ref = String((args && (args.skill_reference || args.skill_id)) || "").trim();
      const disposition = String((args && args.disposition) || "").trim().toLowerCase();
      const reason = String((args && args.reason) || "").trim();
      const skill = offeredByRef[ref] || OFFERED_SKILLS[0];
      if (disposition !== "accepted" && disposition !== "rejected") {
        return { content: [{ type: "text", text: "skill_feedback failed: disposition must be accepted or rejected." }] };
      }
      if (!skill) {
        return { content: [{ type: "text", text: "skill_feedback failed: unknown skill_reference " + ref }] };
      }
      if (!servedRefs[skill.skill_reference]) {
        return { content: [{ type: "text", text: "skill_feedback failed: skill_get the exact reference first." }] };
      }
      if (disposition === "accepted" && reason === "") {
        return { content: [{ type: "text", text: "skill_feedback failed: accepted requires a reason that names the skill." }] };
      }
      return { content: [{ type: "text", text: "SKILL_FEEDBACK " + disposition + " skill_reference=" + skill.skill_reference + " reason=" + reason }] };
    },
  });
  // ` + strconv.Itoa(len(offers)) + ` offered skills baked into this extension.
`
}
