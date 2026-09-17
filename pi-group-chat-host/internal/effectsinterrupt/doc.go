// Package effectsinterrupt is the Host abort policy for directed mentions:
// it distinguishes model generation, read-only tools, and mutating tools,
// waits for a mutating completion before abort, escalates an uncooperative
// process group TERM→KILL, and fail-closes an attempt whose side effects
// cannot be proven.
//
// A quarantined effects_unknown attempt is never resumed. The runner mints
// a new attempt ID, keeps the original failure record, and on retry
// exhaustion leaves the preregistered task in the pass@1 denominator as 0.
//
// Tests use ScriptedSession and FakeProcessTree. Production Pi is not
// required here; session identity reuses sessionctrl.Session and
// directedoffer.Binding.
package effectsinterrupt
