// Package agentrun is the Host composition of Agent single-flight,
// directed-offer enqueue/resume, and effects-unknown fail-closed interrupt.
//
// It does not reimplement those policies. concurrentepisode owns same-Agent
// single-flight, directedoffer owns enqueue / exact-session resume / Segment,
// and effectsinterrupt owns the effects_unknown fail-closed abort path.
package agentrun
