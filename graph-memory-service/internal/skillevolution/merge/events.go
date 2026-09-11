// Package merge is the GMS-208 binary Step Guidance merge subsystem
// (GMS §7, §12.10–§12.13; Contract §5.5, §7.22–§7.23, §9.4, §10.3–§10.4).
//
// It drives the full MergeProposal lifecycle over the GMS-102 ledger:
//
//	proposed → admitted → synthesizing → candidate_bound → validating
//	        → replaying → decision_pending → activation_pending → released
//	(+ terminal duplicate|rejected|inconclusive|stale|withdrawn, never reopened)
//
// MT2 admission: graded band gates (below threshold never admits), M2
// compatibility gates, active-only sources and the single in-flight
// proposal-group winner (merge-group-winner head CAS; losers become exact
// duplicates pointing at the canonical winner). MT3 synthesis: the protected
// builder freezes both sources' bodies/evidence/claims/conflicts and derives
// the merged step_guidance body (branch union with disjoint provenance,
// conflict segments marked); blocking unresolved conflicts reject with no
// candidate. MT4: the bilateral replay handoff (A-only/B-only/overlap/
// conflict families over the frozen reference envelope) flows into the
// GMS-203 replay canonicalizer and the GMS-203 U1 evaluator. MT5: the M@1
// derived activation commits dual source-head CAS, the new derived lineage,
// the Candidate→Released mapping, the §7.13 ActivationEvent (two
// derived_from refs, no supersedes) and the outbox in ONE transaction —
// sources A/B stay retained active, and any source-head movement stale with
// zero partial activation.
package merge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Schema file names of the merge authority shapes ($FIX/schema/shared and
// $FIX/schema/state; CTR-005 authority).
const (
	SchemaMergeProposal      = "merge-proposal.schema.json"
	SchemaMergeProposalEvent = "merge-proposal-event.schema.json"
	SchemaReleaseDecision    = "release-decision.schema.json"
	SchemaActivationEvent    = "activation-event.schema.json"
	// BundleMergeLifecycle is the authority state-machine bundle file.
	BundleMergeLifecycle = "merge-lifecycle.schema.json"
	// MachineMergeProposal is the merge proposal state machine id.
	MachineMergeProposal = "merge-proposal"
	// StateNone is the genesis pseudo-state of a proposal stream.
	StateNone = "none"

	// SchemaMergeSourceExpectation is the internal record shape pinned by a
	// merge-source head at admission (the frozen source-head expectation,
	// Contract §5.5.5/M5). Content-addressed in the ledger content store.
	SchemaMergeSourceExpectation = "gms.merge-source-expectation.v1"
	// SchemaReleaseMapping aliases the GMS-204 release-mapping record shape
	// (the M@1 transaction reuses it byte-for-byte).
	SchemaReleaseMapping = "gms.release-mapping.v1"
)

// Lifecycle states (Contract §9.4; the frozen machine is the single source).
const (
	StateProposed          = "proposed"
	StateAdmitted          = "admitted"
	StateSynthesizing      = "synthesizing"
	StateCandidateBound    = "candidate_bound"
	StateValidating        = "validating"
	StateReplaying         = "replaying"
	StateDecisionPending   = "decision_pending"
	StateActivationPending = "activation_pending"
	StateReleased          = "released"
	StateDuplicate         = "duplicate"
	StateRejected          = "rejected"
	StateInconclusive      = "inconclusive"
	StateStale             = "stale"
	StateWithdrawn         = "withdrawn"
)

// EventInfo reports one appended merge proposal event.
type EventInfo struct {
	FromState       string
	ToState         string
	EventSequence   uint64
	EventID         string
	EventDigest     string // ledger payload digest of the event
	ExpectedPrevSeq uint64
	ReasonCodes     []string
}

// eventEngine loads the §9.4 machine from the schema authority and appends
// gms.merge-proposal-event.v1 payloads through the ledger transaction.
type eventEngine struct {
	gates        *validation.Gates
	machine      *validation.Machine
	bundleDigest string
	store        ledger.Store
}

func newEventEngine(gates *validation.Gates, stateDir string, store ledger.Store) (*eventEngine, error) {
	if gates == nil {
		return nil, fmt.Errorf("merge: nil validation gates")
	}
	bundlePath := filepath.Join(stateDir, BundleMergeLifecycle)
	bundle, err := validation.LoadStateBundle(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("merge: load %s: %w", BundleMergeLifecycle, err)
	}
	machine, err := bundle.Machine(MachineMergeProposal)
	if err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	raw, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("merge: read state bundle: %w", err)
	}
	return &eventEngine{
		gates:        gates,
		machine:      machine,
		bundleDigest: contract.DigestBytes(raw),
		store:        store,
	}, nil
}

// Machine exposes the loaded §9.4 authority machine (read-only).
func (e *eventEngine) Machine() *validation.Machine { return e.machine }

// committedState replays the merge event stream of one proposal and returns
// the derived state and last event sequence ("none", 0 when empty).
func (e *eventEngine) committedState(proposalID string) (string, uint64, error) {
	entries, err := e.store.Snapshot(ledger.LedgerMergeEvent, proposalID)
	if err != nil {
		return "", 0, err
	}
	state, lastSeq := StateNone, uint64(0)
	for _, entry := range entries {
		payload, ok, err := e.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return "", 0, fmt.Errorf("merge: event %s payload unresolved", entry.EventID)
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return "", 0, fmt.Errorf("merge: event %s payload invalid: %w", entry.EventID, err)
		}
		obj, _ := contract.AsObject(value)
		from, _ := contract.AsString(obj["from_state"])
		to, _ := contract.AsString(obj["to_state"])
		if from != state {
			return "", 0, fmt.Errorf("merge: event %s from %q but derived state is %q (torn stream)", entry.EventID, from, state)
		}
		state, lastSeq = to, entry.Sequence
	}
	return state, lastSeq, nil
}

// appendEventTx validates the from -> to transition against the frozen §9.4
// machine (continuity, defined states, no terminal reopen, legal edge),
// builds the gms.merge-proposal-event.v1 payload with the frozen trigger as
// its reason code (plus any extra closed-registry codes) and a recomputed
// event_digest over the schema x-digest preimage, and stages the ledger
// append inside the caller-owned transaction.
func (e *eventEngine) appendEventTx(tx *ledger.Tx, prop *Proposal, from, to string, extraReasons []string, recordRefs []contract.VersionedRef, observedHeads []contract.SkillArtifactRef) (EventInfo, error) {
	state, lastSeq, err := e.committedState(prop.ID())
	if err != nil {
		return EventInfo{}, err
	}
	if err := e.checkTransition(state, lastSeq, from, to); err != nil {
		return EventInfo{}, err
	}
	sequence := lastSeq + 1
	trigger, _ := e.machine.Trigger(from, to)
	if from == StateNone {
		trigger = "merge_proposal_created"
	}
	reasonCodes := []any{}
	if trigger != "" {
		reasonCodes = append(reasonCodes, trigger)
	}
	for _, extra := range extraReasons {
		if extra != "" {
			reasonCodes = append(reasonCodes, extra)
		}
	}
	if recordRefs == nil {
		recordRefs = []contract.VersionedRef{}
	}
	records := make([]any, 0, len(recordRefs))
	for _, ref := range recordRefs {
		records = append(records, versionedRefDoc(ref))
	}
	heads := make([]any, 0, len(observedHeads))
	for _, head := range observedHeads {
		heads = append(heads, similarity.SkillRefDoc(head))
	}
	payload := map[string]any{
		"schema_version":                   contract.SchemaMergeProposalEvent,
		"proposal_ref":                     versionedRefDoc(prop.Ref()),
		"event_sequence":                   json.Number(fmt.Sprintf("%d", sequence)),
		"event_id":                         fmt.Sprintf("mpe-%s-%d", prop.ID(), sequence),
		"from_state":                       from,
		"to_state":                         to,
		"reason_codes":                     reasonCodes,
		"authority_ref":                    e.authorityRef(),
		"record_refs":                      records,
		"observed_source_heads":            heads,
		"expected_previous_event_sequence": json.Number(fmt.Sprintf("%d", sequence-1)),
	}
	digest, err := e.gates.ComputeDigestPreimage(payload, SchemaMergeProposalEvent)
	if err != nil {
		return EventInfo{}, err
	}
	payload["event_digest"] = digest
	if err := e.gates.ValidateInstance(payload, SchemaMergeProposalEvent); err != nil {
		return EventInfo{}, err
	}
	eventID := stringOf(payload["event_id"])
	entry, err := tx.AppendEvent(ledger.LedgerMergeEvent, prop.ID(), eventID, marshalDoc(payload))
	if err != nil {
		return EventInfo{}, err
	}
	return EventInfo{
		FromState:       from,
		ToState:         to,
		EventSequence:   sequence,
		EventID:         eventID,
		EventDigest:     entry.PayloadDigest,
		ExpectedPrevSeq: sequence - 1,
		ReasonCodes:     reasonStrings(reasonCodes),
	}, nil
}

// checkTransition enforces the frozen §9.4 machine semantics for one staged
// transition (from must be the committed state).
func (e *eventEngine) checkTransition(state string, lastSeq uint64, from, to string) error {
	if from == StateNone {
		if lastSeq != 0 {
			return transitionError("merge proposal stream not empty (state %s); genesis none -> proposed needs a new stream", state)
		}
		if !e.machine.IsInitial(to) {
			return transitionError("none -> %s: %q is not an initial state of the frozen §9.4 machine", to, to)
		}
		return nil
	}
	if !e.machine.IsDefined(from) || !e.machine.IsDefined(to) {
		return transitionError("%s -> %s leaves the defined state set of §9.4", from, to)
	}
	if from != state {
		return transitionError("from_state %q but the current state is %q", from, state)
	}
	if e.machine.IsTerminal(from) {
		return transitionError("terminal state %q never reopens; a new attempt needs a new proposal", from)
	}
	if !e.machine.HasEdge(from, to) {
		return transitionError("%s -> %s is not an edge of the frozen §9.4 machine", from, to)
	}
	return nil
}

// authorityRef is the deterministic protected authority record of this engine.
func (e *eventEngine) authorityRef() map[string]any {
	return map[string]any{
		"id":      MachineMergeProposal,
		"version": json.Number("1"),
		"digest":  e.bundleDigest,
	}
}

func reasonStrings(values []any) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if s, ok := contract.AsString(v); ok {
			out = append(out, s)
		}
	}
	return out
}

func stringOf(v any) string {
	s, _ := contract.AsString(v)
	return s
}

func marshalDoc(doc map[string]any) []byte {
	data, err := json.Marshal(doc)
	if err != nil {
		panic(fmt.Sprintf("merge: payload marshal: %v", err))
	}
	return data
}

// pairSlice copies a canonical pair into a slice (map/event payload use).
func pairSlice(pair [2]contract.SkillArtifactRef) []contract.SkillArtifactRef {
	out := make([]contract.SkillArtifactRef, 2)
	out[0], out[1] = pair[0], pair[1]
	return out
}
