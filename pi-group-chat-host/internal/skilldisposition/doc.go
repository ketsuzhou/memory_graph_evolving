// Package skilldisposition records the Host-owned Skill Offer disposition
// after a served fact (Warm Skill Graph Batch contract §2.2, §4.4).
//
// skill_feedback(accepted) atomically creates exactly one terminal
// disposition, an interaction signal, a room-visible
// `@memory-agent SKILL_ACCEPTED` message, and a unique Memory Delivery,
// then lifts the task tool fence. An optional skill_adoption binds the
// exact Skill, offer, checkpoint, behavior change, and affected action;
// accepted never implies adopted, verified, active, or
// marginal-gain-supported.
//
// An unserved offer cannot be accepted. The same offer/agent pair has one
// terminal disposition: identical replay is idempotent, a conflicting
// replay is refused. Invalid feedback is repaired once; a second failure
// records protocol_error and lifts the fence.
//
// GMS internals are never imported. Served facts are Host-authored
// skillfence.ServedFact values. Room publication reuses directedoffer.
package skilldisposition
