// Package skillfence enforces the post-resume Skill Offer protocol fence
// (Warm Skill Graph Batch contract §4.4).
//
// After a directed mention resume, the target task session MUST resolve a
// manifest-pinned exact Skill Reference via skill_get before any ordinary
// tool. Host records a served fact only when those exact body bytes have
// entered the target Pi session. A resolution failure creates neither
// served nor rejected and notifies Memory with a structured
// resolution_error. A successful skill_get moves the fence to
// skill_feedback.
//
// GMS internals are never imported here. Tests bind SkillResolver to a
// testdata adapter that calls the real evaluation freeze/graph digest.
package skillfence
