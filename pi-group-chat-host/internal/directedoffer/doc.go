// Package directedoffer atomically delivers a structured Skill Offer and
// continues the target Agent's exact Pi session.
//
// A Skill Offer MUST carry an authoritative recipient_agent_id. Free-text
// @agent in the body is display only: it never routes and never creates a
// Delivery. The coordinator writes one canonical Room message and at most
// one unique directed Delivery, then (if the target task is running)
// interrupts once and resumes the stored exact session with every pending
// directed message injected in Room sequence.
//
// Tests use ScriptedExactSession. Production Pi is not required here; later
// tickets wrap sessionctrl and the process-group abort policy.
package directedoffer
