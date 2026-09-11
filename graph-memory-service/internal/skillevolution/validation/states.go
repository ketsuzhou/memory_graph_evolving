package validation

import (
	"fmt"
	"os"

	"river2.dev/graph-memory-service/internal/contract"
)

// stateEventsSchemaVersion is the envelope of $FIX/schema-cases/state
// events.json documents (check_state_event_doc keys).
const stateEventsSchemaVersion = "rsih-skill-evolution.state-events.v1"

var stateEventDocKeys = map[string]bool{
	"schema_version": true, "case_id": true, "bundle": true,
	"machine": true, "initial_state": true, "events": true,
}

// Bundle is one loaded state-machine bundle of $FIX/schema/state/*.schema.json
// (Contract §9 state machines; the schema files are the single authority).
type Bundle struct {
	Path     string
	machines map[string]*Machine
}

// LoadStateBundle parses one state-machine bundle document fail-closed.
func LoadStateBundle(path string) (*Bundle, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("validation: read state bundle: %w", err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return nil, fmt.Errorf("validation: state bundle %s is not valid UTF-8 JSON: %w", path, err)
	}
	obj, ok := contract.AsObject(value)
	if !ok {
		return nil, fmt.Errorf("validation: state bundle %s is not an object", path)
	}
	rawMachines, _ := contract.AsArray(obj["machines"])
	bundle := &Bundle{Path: path, machines: map[string]*Machine{}}
	for _, rawMachine := range rawMachines {
		machineDoc, isObj := contract.AsObject(rawMachine)
		if !isObj {
			return nil, fmt.Errorf("validation: state bundle %s: machine entry is not an object", path)
		}
		machine, err := parseMachine(machineDoc)
		if err != nil {
			return nil, fmt.Errorf("validation: state bundle %s: %w", path, err)
		}
		bundle.machines[machine.ID] = machine
	}
	if len(bundle.machines) == 0 {
		return nil, fmt.Errorf("validation: state bundle %s declares no machines", path)
	}
	return bundle, nil
}

// MachineIDs returns the bundle's machine ids in stable order.
func (b *Bundle) MachineIDs() []string {
	out := make([]string, 0, len(b.machines))
	for id := range b.machines {
		out = append(out, id)
	}
	sortStrings(out)
	return out
}

// Machine returns one machine of the bundle.
func (b *Bundle) Machine(id string) (*Machine, error) {
	machine, ok := b.machines[id]
	if !ok {
		return nil, fmt.Errorf("validation: bundle %s has no machine %q (has %v)", b.Path, id, b.MachineIDs())
	}
	return machine, nil
}

// Machine is one state machine loaded from the schema authority: initial
// states, the terminal classification, the legal transition edges with
// their triggers.
type Machine struct {
	ID            string
	initialStates map[string]bool
	states        map[string]bool // defined states
	terminal      map[string]bool
	edges         map[string]map[string]bool
	triggers      map[string]string // from\x1fto -> trigger
}

func parseMachine(doc map[string]any) (*Machine, error) {
	id, _ := contract.AsString(doc["machine_id"])
	if id == "" {
		return nil, newError(CodeSchemaRequiredFieldMissing, "machine_id missing")
	}
	machine := &Machine{
		ID:            id,
		initialStates: map[string]bool{},
		states:        map[string]bool{},
		terminal:      map[string]bool{},
		edges:         map[string]map[string]bool{},
		triggers:      map[string]string{},
	}
	rawInitials, _ := contract.AsArray(doc["initial_states"])
	for _, raw := range rawInitials {
		if state, isStr := contract.AsString(raw); isStr {
			machine.initialStates[state] = true
		}
	}
	rawStates, _ := contract.AsArray(doc["states"])
	for _, raw := range rawStates {
		stateDoc, isObj := contract.AsObject(raw)
		if !isObj {
			return nil, newError(CodeSchemaKeywordUnknown, "machine %s: state entry is not an object", id)
		}
		name, _ := contract.AsString(stateDoc["name"])
		if name == "" {
			return nil, newError(CodeSchemaRequiredFieldMissing, "machine %s: state without a name", id)
		}
		machine.states[name] = true
		if terminal, isBool := stateDoc["terminal"].(bool); isBool && terminal {
			machine.terminal[name] = true
		}
	}
	rawTransitions, _ := contract.AsArray(doc["transitions"])
	for _, raw := range rawTransitions {
		transition, isObj := contract.AsObject(raw)
		if !isObj {
			return nil, newError(CodeSchemaKeywordUnknown, "machine %s: transition entry is not an object", id)
		}
		from, _ := contract.AsString(transition["from"])
		to, _ := contract.AsString(transition["to"])
		trigger, _ := contract.AsString(transition["trigger"])
		if from == "" || to == "" {
			return nil, newError(CodeSchemaRequiredFieldMissing, "machine %s: transition without from/to", id)
		}
		if machine.edges[from] == nil {
			machine.edges[from] = map[string]bool{}
		}
		machine.edges[from][to] = true
		machine.triggers[from+"\x1f"+to] = trigger
	}
	return machine, nil
}

// InitialStates returns the machine's initial states in stable order.
func (m *Machine) InitialStates() []string {
	out := make([]string, 0, len(m.initialStates))
	for state := range m.initialStates {
		out = append(out, state)
	}
	sortStrings(out)
	return out
}

// IsInitial reports whether state is an initial state.
func (m *Machine) IsInitial(state string) bool { return m.initialStates[state] }

// IsDefined reports whether state exists in the machine.
func (m *Machine) IsDefined(state string) bool { return m.states[state] }

// IsTerminal reports whether state is a terminal state (no legal exits,
// never reopened). Undefined states are not terminal.
func (m *Machine) IsTerminal(state string) bool { return m.terminal[state] }

// HasEdge reports whether from -> to is a legal transition edge.
func (m *Machine) HasEdge(from, to string) bool {
	targets, ok := m.edges[from]
	return ok && targets[to]
}

// Trigger returns the frozen trigger name of one edge (empty when absent).
func (m *Machine) Trigger(from, to string) (string, bool) {
	trigger, ok := m.triggers[from+"\x1f"+to]
	return trigger, ok
}

// StateEvent is one transition of a replay document.
type StateEvent struct {
	Sequence int64
	From     string
	To       string
}

// ReplayDocument validates and replays one schema-cases state events.json
// document against this machine (check_event_sequence semantics):
// defined states, from-state continuity, terminal states never reopen,
// legal edges only.
func (m *Machine) ReplayDocument(doc map[string]any) error {
	for key := range doc {
		if !stateEventDocKeys[key] {
			return newError(CodeFixtureExpectationMismatch, "state-events document key %q outside the closed set", key)
		}
	}
	for key := range stateEventDocKeys {
		if _, present := doc[key]; !present {
			return newError(CodeFixtureExpectationMismatch, "state-events document missing key %q", key)
		}
	}
	if sv, _ := contract.AsString(doc["schema_version"]); sv != stateEventsSchemaVersion {
		return newError(CodeFixtureExpectationMismatch, "state-events schema_version %q invalid", sv)
	}
	initialState, _ := contract.AsString(doc["initial_state"])
	if !m.initialStates[initialState] {
		return newError(CodeIllegalStateTransition, "initial_state %q is not an initial state of %s", initialState, m.ID)
	}
	rawEvents, ok := contract.AsArray(doc["events"])
	if !ok {
		return newError(CodeFixtureExpectationMismatch, "events must be an array")
	}
	events := make([]StateEvent, 0, len(rawEvents))
	for i, raw := range rawEvents {
		eventDoc, isObj := contract.AsObject(raw)
		if !isObj {
			return newError(CodeIllegalStateTransition, "event %d is not an object", i)
		}
		if len(eventDoc) != 3 {
			return newError(CodeFixtureExpectationMismatch, "event %d keys invalid", i)
		}
		sequence, seqOK := instanceInt(eventDoc["event_sequence"])
		from, fromOK := contract.AsString(eventDoc["from_state"])
		to, toOK := contract.AsString(eventDoc["to_state"])
		if !seqOK || !fromOK || !toOK {
			return newError(CodeFixtureExpectationMismatch, "event %d fields invalid", i)
		}
		events = append(events, StateEvent{Sequence: sequence, From: from, To: to})
	}
	if _, err := m.Replay(initialState, events); err != nil {
		return err
	}
	return nil
}

// Replay replays events from initial with the check_event_sequence order:
// undefined states (UNDEFINED_STATE), from-state continuity
// (ILLEGAL_STATE_TRANSITION), terminal reopen (TERMINAL_STATE_REOPEN),
// legal edge (ILLEGAL_STATE_TRANSITION). Events must already be sorted by
// sequence 1..n.
func (m *Machine) Replay(initial string, events []StateEvent) (string, error) {
	for i, event := range events {
		if event.Sequence != int64(i+1) {
			return "", newError(CodeIllegalStateTransition, "event_sequence must be 1..n unique")
		}
	}
	current := initial
	for i, event := range events {
		if !m.states[event.From] {
			return "", newError(CodeUndefinedState, "event %d from undefined state %q", i, event.From)
		}
		if !m.states[event.To] {
			return "", newError(CodeUndefinedState, "event %d to undefined state %q", i, event.To)
		}
		if event.From != current {
			return "", newError(CodeIllegalStateTransition, "event %d from %q but current state is %q", i, event.From, current)
		}
		if m.terminal[event.From] {
			return "", newError(CodeTerminalStateReopen, "event %d reopens terminal state %q", i, event.From)
		}
		if !m.HasEdge(event.From, event.To) {
			return "", newError(CodeIllegalStateTransition, "event %d %q -> %q is not a legal edge of %s", i, event.From, event.To, m.ID)
		}
		current = event.To
	}
	return current, nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
