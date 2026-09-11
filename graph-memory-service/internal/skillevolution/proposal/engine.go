// Package proposal is the GMS-202 ordinary SkillProposal state engine
// (Contract §9.1, GMS §4.4): strict §7.9 proposal parsing with x-digest
// recomputation, and append-only §9.1 lifecycle events (gms.proposal-event.v1)
// written through the GMS-102 ledger transaction manager.
//
// The state machine is NOT hand-coded here: the engine loads
// $FIX/schema/state/proposal-lifecycle.schema.json (machine
// "skill-proposal") at construction — the schema file stays the single
// authority for states, terminal classification, legal edges and triggers.
package proposal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Schema file names of the ordinary proposal DTOs (CTR-005 authority).
const (
	SchemaProposal      = "skill-proposal.schema.json"
	SchemaProposalEvent = "proposal-event.schema.json"
	// BundleProposalLifecycle is the authority state-machine bundle file.
	BundleProposalLifecycle = "proposal-lifecycle.schema.json"
	// MachineSkillProposal is the ordinary proposal state machine id.
	MachineSkillProposal = "skill-proposal"
	// StateNone is the genesis pseudo-state of a proposal stream.
	StateNone = "none"

	ReasonProposalInvalid = "PROPOSAL_INVALID"
)

// Service is the ordinary proposal state engine.
type Service struct {
	gates        *validation.Gates
	machine      *validation.Machine
	bundleDigest string
	store        ledger.Store
	mgr          ledger.TxManager
	registry     ledger.ReasonRegistry
}

// NewService loads the §9.1 state machine from the schema authority and
// wires the engine to the ledger. It fails closed when the machine cannot
// be loaded or a mapped reason code is outside the registry.
func NewService(gates *validation.Gates, stateDir string, store ledger.Store, mgr ledger.TxManager, registry ledger.ReasonRegistry) (*Service, error) {
	if gates == nil {
		return nil, fmt.Errorf("proposal: nil validation gates")
	}
	bundlePath := filepath.Join(stateDir, BundleProposalLifecycle)
	bundle, err := validation.LoadStateBundle(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("proposal: load %s: %w", BundleProposalLifecycle, err)
	}
	machine, err := bundle.Machine(MachineSkillProposal)
	if err != nil {
		return nil, fmt.Errorf("proposal: %w", err)
	}
	rawBundle, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("proposal: read bundle: %w", err)
	}
	if store == nil || mgr == nil || registry == nil {
		return nil, fmt.Errorf("proposal: nil store, transaction manager or registry")
	}
	for _, code := range []string{
		ReasonProposalInvalid, ledger.ReasonProposalStateConflict,
		ledger.ReasonIllegalStateTransition, ledger.ReasonIdempotencyConflict,
	} {
		if err := registry.Verify(code); err != nil {
			return nil, fmt.Errorf("proposal: %w", err)
		}
	}
	return &Service{
		gates:        gates,
		machine:      machine,
		bundleDigest: contract.DigestBytes(rawBundle),
		store:        store,
		mgr:          mgr,
		registry:     registry,
	}, nil
}

// Machine exposes the loaded authority state machine (states, terminal
// classification, edges, triggers — read-only use).
func (s *Service) Machine() *validation.Machine { return s.machine }

// Gates exposes the shared validation gates.
func (s *Service) Gates() *validation.Gates { return s.gates }

// ComputeDigest computes the proposal_digest x-digest of one §7.9 document.
func (s *Service) ComputeDigest(doc map[string]any) (string, error) {
	return s.gates.ComputeDigestPreimage(doc, SchemaProposal)
}

// Proposal is the immutable parsed view of one Contract §7.9 SkillProposal.
type Proposal struct {
	id         string
	version    string
	digest     string
	kind       string
	operation  string
	evidence   []contract.EvidenceRef
	policyRefs []contract.VersionedRef
}

// ID returns the proposal id.
func (p *Proposal) ID() string { return p.id }

// Version returns the exact proposal version digits.
func (p *Proposal) Version() string { return p.version }

// Digest returns the recomputed proposal digest.
func (p *Proposal) Digest() string { return p.digest }

// Kind returns the proposed canonical kind.
func (p *Proposal) Kind() string { return p.kind }

// Operation returns the requested operation.
func (p *Proposal) Operation() string { return p.operation }

// EvidenceRefs returns copies of the proposal's evidence refs.
func (p *Proposal) EvidenceRefs() []contract.EvidenceRef {
	out := make([]contract.EvidenceRef, len(p.evidence))
	copy(out, p.evidence)
	return out
}

// PolicyRefs returns copies of the proposal's policy refs.
func (p *Proposal) PolicyRefs() []contract.VersionedRef {
	out := make([]contract.VersionedRef, len(p.policyRefs))
	copy(out, p.policyRefs)
	return out
}

// Ref renders the proposal identity as a VersionedRef.
func (p *Proposal) Ref() contract.VersionedRef {
	return contract.VersionedRef{ID: p.id, Version: p.version, Digest: p.digest}
}

// ParseProposal parses one §7.9 document through the full closed pipeline
// (shape against the authority schema, integer-only core, proposal_digest
// preimage recompute) and strict evidence-ref parsing.
func (s *Service) ParseProposal(doc any) (*Proposal, error) {
	obj, ok := contract.AsObject(doc)
	if !ok {
		return nil, validationError(ReasonProposalInvalid, "proposal document must be a JSON object")
	}
	if err := s.gates.ValidateInstance(obj, SchemaProposal); err != nil {
		return nil, err
	}
	proposal := &Proposal{
		id:        stringField(obj, "proposal_id"),
		version:   versionDigits(obj["proposal_version"]),
		digest:    stringField(obj, "proposal_digest"),
		kind:      stringField(obj, "proposed_kind"),
		operation: stringField(obj, "requested_operation"),
	}
	refsRaw, _ := contract.AsArray(obj["evidence_refs"])
	for _, raw := range refsRaw {
		refObj, isObj := contract.AsObject(raw)
		if !isObj {
			return nil, validationError(validation.CodeSchemaEnumInvalid, "evidence ref must be an object")
		}
		ref, err := contract.ParseEvidenceRef(refObj)
		if err != nil {
			return nil, validationError(validation.CodeOf(err), "%v", err)
		}
		proposal.evidence = append(proposal.evidence, ref)
	}
	policiesRaw, _ := contract.AsArray(obj["policy_refs"])
	for _, raw := range policiesRaw {
		refObj, _ := contract.AsObject(raw)
		ref, err := contract.ParseVersionedRef(refObj)
		if err != nil {
			return nil, validationError(validation.CodeOf(err), "policy ref: %v", err)
		}
		proposal.policyRefs = append(proposal.policyRefs, ref)
	}
	return proposal, nil
}

// EventInfo reports one appended proposal event.
type EventInfo struct {
	FromState       string
	ToState         string
	EventSequence   uint64
	EventID         string
	ExpectedPrevSeq uint64
}

// AppendTransition validates the from -> to transition against the loaded
// authority machine (continuity, defined states, no terminal reopen, legal
// edge; ILLEGAL_STATE_TRANSITION otherwise), builds the gms.proposal-event.v1
// payload with a recomputed event_digest and appends it to the proposal
// ledger in one atomic transaction. The creation event none -> proposed is
// legal exactly once, from an empty stream.
func (s *Service) AppendTransition(ctx context.Context, proposalDoc any, from, to string) (EventInfo, error) {
	proposal, err := s.ParseProposal(proposalDoc)
	if err != nil {
		return EventInfo{}, err
	}
	var info EventInfo
	err = s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		staged, err := s.stageTransition(tx, proposal, from, to, nil)
		if err != nil {
			return err
		}
		info = staged
		return nil
	})
	if err != nil {
		return EventInfo{}, err
	}
	return info, nil
}

// AppendTransitionTx stages the same transition inside a caller-owned
// transaction (used by the candidate binding to commit the candidate and
// its candidate_bound event atomically).
func (s *Service) AppendTransitionTx(tx *ledger.Tx, proposal *Proposal, from, to string, recordRefs []contract.VersionedRef) (EventInfo, error) {
	return s.stageTransition(tx, proposal, from, to, recordRefs)
}

// stageTransition is the shared engine: continuity checks against the
// committed stream, payload build with x-digest, ledger append.
func (s *Service) stageTransition(tx *ledger.Tx, proposal *Proposal, from, to string, recordRefs []contract.VersionedRef) (EventInfo, error) {
	current, lastSeq, err := s.committedState(proposal.ID())
	if err != nil {
		return EventInfo{}, err
	}
	if from == StateNone {
		if lastSeq != 0 {
			return EventInfo{}, transitionError("proposal %s already exists (state %s); genesis none -> proposed needs a new stream", proposal.ID(), current)
		}
		if !s.machine.IsInitial(to) {
			return EventInfo{}, transitionError("none -> %s: %q is not an initial state of the frozen machine", to, to)
		}
	} else {
		if !s.machine.IsDefined(from) || !s.machine.IsDefined(to) {
			return EventInfo{}, transitionError("%s -> %s leaves the defined state set", from, to)
		}
		if from != current {
			return EventInfo{}, transitionError("from_state %q but the current state of %s is %q", from, proposal.ID(), current)
		}
		if s.machine.IsTerminal(from) {
			return EventInfo{}, transitionError("terminal state %q never reopens; a new attempt needs a new proposal", from)
		}
		if !s.machine.HasEdge(from, to) {
			return EventInfo{}, transitionError("%s -> %s is not an edge of the frozen §9.1 machine", from, to)
		}
	}
	sequence := lastSeq + 1
	payload, err := s.buildEventPayload(proposal, from, to, sequence, recordRefs)
	if err != nil {
		return EventInfo{}, err
	}
	eventID := stringField(payload, "event_id")
	if _, err := tx.AppendEvent(ledger.LedgerProposal, proposal.ID(), eventID, marshalJSON(payload)); err != nil {
		return EventInfo{}, err
	}
	return EventInfo{
		FromState:       from,
		ToState:         to,
		EventSequence:   sequence,
		EventID:         eventID,
		ExpectedPrevSeq: sequence - 1,
	}, nil
}

// buildEventPayload assembles one gms.proposal-event.v1 payload with the
// frozen trigger of the edge as its reason code and a recomputed
// event_digest over the schema's x-digest preimage.
func (s *Service) buildEventPayload(proposal *Proposal, from, to string, sequence uint64, recordRefs []contract.VersionedRef) (map[string]any, error) {
	trigger, _ := s.machine.Trigger(from, to)
	if from == StateNone {
		trigger = "proposal_created"
	}
	if recordRefs == nil {
		recordRefs = []contract.VersionedRef{}
	}
	reasonCodes := []any{}
	if trigger != "" {
		reasonCodes = append(reasonCodes, trigger)
	}
	records := make([]any, 0, len(recordRefs))
	for _, ref := range recordRefs {
		records = append(records, map[string]any{
			"id": ref.ID, "version": json.Number(ref.Version), "digest": ref.Digest,
		})
	}
	payload := map[string]any{
		"schema_version":                   ledger.SchemaProposalEvent,
		"proposal_ref":                     versionedRefJSON(proposal.Ref()),
		"event_sequence":                   json.Number(fmt.Sprintf("%d", sequence)),
		"event_id":                         fmt.Sprintf("pe-%s-%d", proposal.ID(), sequence),
		"from_state":                       from,
		"to_state":                         to,
		"reason_codes":                     reasonCodes,
		"authority_ref":                    s.authorityRef(),
		"record_refs":                      records,
		"payload_digest":                   proposal.Digest(),
		"expected_previous_event_sequence": json.Number(fmt.Sprintf("%d", sequence-1)),
	}
	digest, err := s.gates.ComputeDigestPreimage(payload, SchemaProposalEvent)
	if err != nil {
		return nil, err
	}
	payload["event_digest"] = digest
	if err := s.gates.ValidateInstance(payload, SchemaProposalEvent); err != nil {
		return nil, err
	}
	return payload, nil
}

// authorityRef is the deterministic protected authority record of this
// engine (the loaded state-machine bundle file, digest-verified by load).
func (s *Service) authorityRef() map[string]any {
	return map[string]any{
		"id":      MachineSkillProposal,
		"version": json.Number("1"),
		"digest":  s.bundleDigest,
	}
}

// CurrentState replays the committed proposal events of one stream and
// returns the derived state and last event sequence ("none", 0 when the
// stream is empty).
func (s *Service) CurrentState(ctx context.Context, proposalID string) (string, uint64, error) {
	return s.committedState(proposalID)
}

func (s *Service) committedState(proposalID string) (string, uint64, error) {
	entries, err := s.store.Snapshot(ledger.LedgerProposal, proposalID)
	if err != nil {
		return "", 0, err
	}
	state, lastSeq := StateNone, uint64(0)
	for _, entry := range entries {
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return "", 0, fmt.Errorf("proposal: event %s payload unresolved", entry.EventID)
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return "", 0, fmt.Errorf("proposal: event %s payload invalid: %w", entry.EventID, err)
		}
		obj, _ := contract.AsObject(value)
		from, _ := contract.AsString(obj["from_state"])
		to, _ := contract.AsString(obj["to_state"])
		if from != state {
			return "", 0, fmt.Errorf("proposal: event %s from %q but derived state is %q (torn stream)", entry.EventID, from, state)
		}
		state, lastSeq = to, entry.Sequence
	}
	return state, lastSeq, nil
}

// Entries returns the committed proposal ledger entries of one stream.
func (s *Service) Entries(ctx context.Context, proposalID string) ([]ledger.Entry, error) {
	return s.store.Snapshot(ledger.LedgerProposal, proposalID)
}

// --- helpers ---------------------------------------------------------------

func stringField(obj map[string]any, key string) string {
	s, _ := contract.AsString(obj[key])
	return s
}

func versionDigits(v any) string {
	if n, ok := v.(json.Number); ok {
		return string(n)
	}
	return ""
}

func versionedRefJSON(ref contract.VersionedRef) map[string]any {
	return map[string]any{
		"id": ref.ID, "version": json.Number(ref.Version), "digest": ref.Digest,
	}
}

func marshalJSON(value map[string]any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		// The payload model is decoder values (json.Number); marshal cannot
		// fail for it. Treat failure as fatal wiring error.
		panic(fmt.Sprintf("proposal: payload marshal: %v", err))
	}
	return data
}

func validationError(code, format string, args ...any) error {
	return &validation.Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

func transitionError(format string, args ...any) error {
	return validationError(ledger.ReasonIllegalStateTransition, format, args...)
}
