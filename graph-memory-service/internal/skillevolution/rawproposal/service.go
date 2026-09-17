// Package rawproposal admits immutable, evidence-grounded Raw Skill
// Proposals from authorized diagnosis runs. It is deliberately separate from
// proposal: GMS-202 SkillProposal lifecycle records have different semantics.
package rawproposal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

const (
	// SchemaVersion is the closed body shape admitted by this package.
	SchemaVersion = "gms.raw-skill-proposal.v1"
	// NoveltyHypothesized is the only novelty state a one-trajectory proposal
	// may carry. Admission never implies validation or activation.
	NoveltyHypothesized = "hypothesized"
)

// Checkpoint is an exact checkpoint in a frozen trajectory snapshot.
type Checkpoint struct {
	ID         string
	SnapshotID string
}

// Evidence is a committed, exact evidence item available to diagnosis. Facts
// are the observable statements an insight is allowed to rely on.
type Evidence struct {
	ID              string
	TrajectoryID    string
	SnapshotID      string
	ObservableFacts []string
}

// FrozenTrajectory is the Host/GMS-approved diagnosis read scope. PublicOutcome
// intentionally excludes hidden tests, gold answers, and reference solutions.
type FrozenTrajectory struct {
	ID                        string
	SnapshotID                string
	CompleteTrajectory        []string
	PublicOutcome             string
	Checkpoints               map[string]Checkpoint
	Evidence                  map[string]Evidence
	AuthorizedDiagnosisRunIDs map[string]bool
}

// TrajectorySource is the protected Host/GMS read seam. Implementations must
// return only a complete, frozen trajectory authorized for the diagnosis run.
type TrajectorySource interface {
	ReadFrozenTrajectory(ctx context.Context, diagnosisRunID, trajectoryID string) (FrozenTrajectory, error)
}

// MemoryTrajectorySource is the in-memory fixture/reference adapter. It is
// useful for tests; production adapters belong behind TrajectorySource.
type MemoryTrajectorySource struct {
	mu           sync.RWMutex
	trajectories map[string]FrozenTrajectory
}

func NewMemoryTrajectorySource() *MemoryTrajectorySource {
	return &MemoryTrajectorySource{trajectories: make(map[string]FrozenTrajectory)}
}

// Put replaces a fixture before admission. Reads and returned values are deep
// copies, so callers cannot mutate a frozen scope after it is registered.
func (s *MemoryTrajectorySource) Put(trajectory FrozenTrajectory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trajectories[trajectory.ID] = cloneTrajectory(trajectory)
}

func (s *MemoryTrajectorySource) ReadFrozenTrajectory(_ context.Context, diagnosisRunID, trajectoryID string) (FrozenTrajectory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	trajectory, ok := s.trajectories[trajectoryID]
	if !ok {
		return FrozenTrajectory{}, fmt.Errorf("frozen trajectory %q does not exist", trajectoryID)
	}
	if !trajectory.AuthorizedDiagnosisRunIDs[diagnosisRunID] {
		return FrozenTrajectory{}, fmt.Errorf("diagnosis run %q is not authorized for frozen trajectory %q", diagnosisRunID, trajectoryID)
	}
	return cloneTrajectory(trajectory), nil
}

// RawSkillProposal is the immutable canonical record required by the Warm
// Skill Graph Batch contract §2.2.
type RawSkillProposal struct {
	ProposalID               string
	SchemaVersion            string
	SourceCheckpointID       string
	SourceEvidenceRefs       []string
	ContextTrigger           string
	FailureOrOpportunity     string
	BaselineBehavior         string
	NonObviousInsight        string
	DecisionPolicyOrSteps    []string
	ExpectedBehaviorChange   string
	Contraindications        []string
	Pitfalls                 []string
	OutcomeObserved          string
	NoveltyStatus            string
	CreatedByAgentRunID      string
	CreatedAt                string
	ContentDigest            string
}

// RawSkillProposalRef is a complete, immutable identifier. ProposalID is
// never truncated and ContentDigest is the canonical body identity.
type RawSkillProposalRef struct {
	ProposalID    string
	ContentDigest string
	URI           string
}

// RawProposalProvenance is the canonical, bidirectional provenance binding
// created atomically with a RawSkillProposal.
type RawProposalProvenance struct {
	ProposalRef         RawSkillProposalRef
	SourceTrajectoryID  string
	SourceSnapshotID    string
	SourceCheckpoint    Checkpoint
	SourceEvidence      []Evidence
}

// AdmissionRequest contains a diagnosis declaration. The service, not the
// diagnosis agent, verifies the read scope and mints the canonical record.
type AdmissionRequest struct {
	IdempotencyKey string
	DiagnosisRunID string
	TrajectoryID   string
	Body           map[string]any
}

// Service is the protected admission authority. Its in-process record store
// is append-only: it exposes no update or delete operation. The provided
// TxManager supplies the canonical idempotency transaction seam.
type Service struct {
	source TrajectorySource
	tx     ledger.TxManager

	mu          sync.RWMutex
	records     map[string]RawSkillProposal
	provenance  map[string]RawProposalProvenance
	byCheckpoint map[string][]string
	byEvidence  map[string][]string
}

func NewService(source TrajectorySource, tx ledger.TxManager) (*Service, error) {
	if source == nil || tx == nil {
		return nil, fmt.Errorf("rawproposal: nil trajectory source or transaction manager")
	}
	return &Service{
		source:       source,
		tx:           tx,
		records:      make(map[string]RawSkillProposal),
		provenance:   make(map[string]RawProposalProvenance),
		byCheckpoint: make(map[string][]string),
		byEvidence:   make(map[string][]string),
	}, nil
}

// ReadTrajectory lets diagnosis consume the complete authorized frozen
// trajectory and public outcome before it declares exact source references.
func (s *Service) ReadTrajectory(ctx context.Context, diagnosisRunID, trajectoryID string) (FrozenTrajectory, error) {
	return s.source.ReadFrozenTrajectory(ctx, diagnosisRunID, trajectoryID)
}

// Admit validates the closed proposal body and all source references before
// atomically recording the immutable proposal and its provenance indexes. Any
// failed validation returns before record mutation; a transaction failure also
// leaves the package indexes untouched.
func (s *Service) Admit(ctx context.Context, request AdmissionRequest) (RawSkillProposalRef, error) {
	proposal, err := parseBody(request.Body)
	if err != nil {
		return RawSkillProposalRef{}, err
	}
	if request.IdempotencyKey == "" || request.DiagnosisRunID == "" || request.TrajectoryID == "" {
		return RawSkillProposalRef{}, admissionError(validation.CodeSchemaRequiredFieldMissing, "idempotency key, diagnosis run id, and trajectory id are required")
	}
	trajectory, err := s.source.ReadFrozenTrajectory(ctx, request.DiagnosisRunID, request.TrajectoryID)
	if err != nil {
		return RawSkillProposalRef{}, admissionError(validation.CodeNonExactRef, "authorized complete frozen trajectory is unavailable: %v", err)
	}
	if trajectory.ID != request.TrajectoryID || trajectory.SnapshotID == "" || len(trajectory.CompleteTrajectory) == 0 || trajectory.PublicOutcome == "" {
		return RawSkillProposalRef{}, admissionError(validation.CodeNonExactRef, "diagnosis source did not return a complete exact frozen trajectory with a public outcome")
	}
	if err := validateProposal(proposal, request, trajectory); err != nil {
		return RawSkillProposalRef{}, err
	}

	requestDigest, err := contract.DigestOf(map[string]any{
		"diagnosis_run_id": request.DiagnosisRunID,
		"trajectory_id":    request.TrajectoryID,
		"body":             request.Body,
	})
	if err != nil {
		return RawSkillProposalRef{}, admissionError(validation.CodeNonIntegerNumber, "request cannot enter canonical hashed core: %v", err)
	}
	ref := proposalRef(proposal)
	outcome, err := json.Marshal(ref)
	if err != nil {
		return RawSkillProposalRef{}, fmt.Errorf("rawproposal: encode idempotency outcome: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.records[proposal.ProposalID]; exists && existing.ContentDigest != proposal.ContentDigest {
		return RawSkillProposalRef{}, admissionError(ledger.ReasonIdempotencyConflict, "proposal id %q already identifies a different canonical body", proposal.ProposalID)
	}
	var replay RawSkillProposalRef
	err = s.tx.WithinTx(ctx, func(tx *ledger.Tx) error {
		replayed, recorded, err := tx.RecordIdempotency("raw-skill-proposal:"+request.IdempotencyKey, requestDigest, outcome)
		if err != nil {
			return err
		}
		if replayed {
			if err := json.Unmarshal(recorded, &replay); err != nil {
				return fmt.Errorf("rawproposal: stored idempotency outcome is invalid: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return RawSkillProposalRef{}, err
	}
	if replay.ProposalID != "" {
		return replay, nil
	}
	if existing, exists := s.records[proposal.ProposalID]; exists {
		if existing.ContentDigest != proposal.ContentDigest {
			return RawSkillProposalRef{}, admissionError(ledger.ReasonIdempotencyConflict, "proposal id %q already identifies a different canonical body", proposal.ProposalID)
		}
		return proposalRef(existing), nil
	}

	provenance := RawProposalProvenance{
		ProposalRef:        ref,
		SourceTrajectoryID: trajectory.ID,
		SourceSnapshotID:   trajectory.SnapshotID,
		SourceCheckpoint:   trajectory.Checkpoints[proposal.SourceCheckpointID],
	}
	for _, evidenceID := range proposal.SourceEvidenceRefs {
		provenance.SourceEvidence = append(provenance.SourceEvidence, cloneEvidence(trajectory.Evidence[evidenceID]))
	}
	s.records[proposal.ProposalID] = cloneProposal(proposal)
	s.provenance[proposal.ProposalID] = cloneProvenance(provenance)
	s.byCheckpoint[proposal.SourceCheckpointID] = append(s.byCheckpoint[proposal.SourceCheckpointID], proposal.ProposalID)
	for _, evidenceID := range proposal.SourceEvidenceRefs {
		s.byEvidence[evidenceID] = append(s.byEvidence[evidenceID], proposal.ProposalID)
	}
	return ref, nil
}

// Get loads a proposal by its complete proposal ID.
func (s *Service) Get(proposalID string) (RawSkillProposal, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	proposal, ok := s.records[proposalID]
	return cloneProposal(proposal), ok
}

func (s *Service) ProvenanceForProposal(proposalID string) (RawProposalProvenance, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	provenance, ok := s.provenance[proposalID]
	return cloneProvenance(provenance), ok
}

func (s *Service) ProposalsForCheckpoint(checkpointID string) []RawSkillProposalRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.refsFor(s.byCheckpoint[checkpointID])
}

func (s *Service) ProposalsForEvidence(evidenceID string) []RawSkillProposalRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.refsFor(s.byEvidence[evidenceID])
}

func (s *Service) refsFor(ids []string) []RawSkillProposalRef {
	refs := make([]RawSkillProposalRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, proposalRef(s.records[id]))
	}
	return refs
}

// Count is primarily useful for asserting fail-closed, no-partial-append
// behavior in in-memory fixtures.
func (s *Service) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}

var bodyFields = map[string]bool{
	"proposal_id": true, "schema_version": true, "source_checkpoint_id": true,
	"source_evidence_refs": true, "context_trigger": true, "failure_or_opportunity": true,
	"baseline_behavior": true, "non_obvious_insight": true, "decision_policy_or_steps": true,
	"expected_behavior_change": true, "contraindications": true, "pitfalls": true,
	"outcome_observed": true, "novelty_status": true, "created_by_agent_run_id": true,
	"created_at": true, "content_digest": true,
}

func parseBody(body map[string]any) (RawSkillProposal, error) {
	if body == nil {
		return RawSkillProposal{}, admissionError(validation.CodeSchemaTypeInvalid, "proposal body must be an object")
	}
	for key := range body {
		if !bodyFields[key] {
			return RawSkillProposal{}, admissionError(validation.CodeSchemaFieldUnknown, "unknown or prohibited raw proposal field %q", key)
		}
	}
	for key := range bodyFields {
		if _, ok := body[key]; !ok {
			return RawSkillProposal{}, admissionError(validation.CodeSchemaRequiredFieldMissing, "missing required raw proposal field %q", key)
		}
	}
	stringField := func(key string) (string, error) {
		value, ok := body[key].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return "", admissionError(validation.CodeSchemaTypeInvalid, "%s must be a non-empty string", key)
		}
		return value, nil
	}
	stringsField := func(key string) ([]string, error) {
		raw, ok := body[key].([]any)
		if !ok || len(raw) == 0 && key == "source_evidence_refs" {
			return nil, admissionError(validation.CodeSchemaTypeInvalid, "%s must be an array%s", key, requiredSuffix(key == "source_evidence_refs"))
		}
		out := make([]string, 0, len(raw))
		seen := make(map[string]bool)
		for _, item := range raw {
			value, ok := item.(string)
			if !ok || strings.TrimSpace(value) == "" || seen[value] {
				return nil, admissionError(validation.CodeSchemaTypeInvalid, "%s must contain unique non-empty strings", key)
			}
			seen[value] = true
			out = append(out, value)
		}
		return out, nil
	}
	var proposal RawSkillProposal
	var err error
	if proposal.ProposalID, err = stringField("proposal_id"); err != nil { return proposal, err }
	if proposal.SchemaVersion, err = stringField("schema_version"); err != nil { return proposal, err }
	if proposal.SourceCheckpointID, err = stringField("source_checkpoint_id"); err != nil { return proposal, err }
	if proposal.SourceEvidenceRefs, err = stringsField("source_evidence_refs"); err != nil { return proposal, err }
	if proposal.ContextTrigger, err = stringField("context_trigger"); err != nil { return proposal, err }
	if proposal.FailureOrOpportunity, err = stringField("failure_or_opportunity"); err != nil { return proposal, err }
	if proposal.BaselineBehavior, err = stringField("baseline_behavior"); err != nil { return proposal, err }
	if proposal.NonObviousInsight, err = stringField("non_obvious_insight"); err != nil { return proposal, err }
	if proposal.DecisionPolicyOrSteps, err = stringsField("decision_policy_or_steps"); err != nil { return proposal, err }
	if proposal.ExpectedBehaviorChange, err = stringField("expected_behavior_change"); err != nil { return proposal, err }
	if proposal.Contraindications, err = stringsField("contraindications"); err != nil { return proposal, err }
	if proposal.Pitfalls, err = stringsField("pitfalls"); err != nil { return proposal, err }
	if proposal.OutcomeObserved, err = stringField("outcome_observed"); err != nil { return proposal, err }
	if proposal.NoveltyStatus, err = stringField("novelty_status"); err != nil { return proposal, err }
	if proposal.CreatedByAgentRunID, err = stringField("created_by_agent_run_id"); err != nil { return proposal, err }
	if proposal.CreatedAt, err = stringField("created_at"); err != nil { return proposal, err }
	if proposal.ContentDigest, err = stringField("content_digest"); err != nil { return proposal, err }
	return proposal, nil
}

func requiredSuffix(required bool) string { if required { return " with at least one item" }; return "" }

func validateProposal(proposal RawSkillProposal, request AdmissionRequest, trajectory FrozenTrajectory) error {
	if proposal.SchemaVersion != SchemaVersion {
		return admissionError(validation.CodeSchemaVersionUnsupported, "schema version %q is unsupported", proposal.SchemaVersion)
	}
	if proposal.NoveltyStatus != NoveltyHypothesized {
		return admissionError(validation.CodeSchemaConstMismatch, "novelty_status must be %q", NoveltyHypothesized)
	}
	if proposal.CreatedByAgentRunID != request.DiagnosisRunID {
		return admissionError(validation.CodeNonExactRef, "created_by_agent_run_id must exactly match the authorized diagnosis run")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, proposal.CreatedAt)
	if err != nil || createdAt.Format(time.RFC3339Nano) != proposal.CreatedAt {
		return admissionError(validation.CodeSchemaPatternInvalid, "created_at must be canonical RFC3339Nano UTC")
	}
	if !strings.HasSuffix(proposal.CreatedAt, "Z") {
		return admissionError(validation.CodeSchemaPatternInvalid, "created_at must be UTC")
	}
	checkpoint, ok := trajectory.Checkpoints[proposal.SourceCheckpointID]
	if !ok || checkpoint.ID != proposal.SourceCheckpointID || checkpoint.SnapshotID != trajectory.SnapshotID {
		return admissionError(validation.CodeNonExactRef, "source checkpoint %q is missing, ambiguous, or outside frozen snapshot %q", proposal.SourceCheckpointID, trajectory.SnapshotID)
	}
	facts := make([]string, 0)
	for _, evidenceID := range proposal.SourceEvidenceRefs {
		evidence, ok := trajectory.Evidence[evidenceID]
		if !ok || evidence.ID != evidenceID || evidence.TrajectoryID != trajectory.ID || evidence.SnapshotID != trajectory.SnapshotID {
			return admissionError(validation.CodeNonExactRef, "source evidence %q is missing, unauthorized, ambiguous, or cross-snapshot", evidenceID)
		}
		facts = append(facts, evidence.ObservableFacts...)
	}
	if len(facts) == 0 {
		return admissionError(validation.CodeNonExactRef, "exact evidence has no observable facts that can ground an insight")
	}
	if proposal.OutcomeObserved != trajectory.PublicOutcome {
		return admissionError(validation.CodeNonExactRef, "outcome_observed must exactly equal the trajectory public outcome")
	}
	if hasProhibitedLeak(proposal) {
		return admissionError(validation.CodeSchemaFieldUnknown, "hidden-test, gold, or reference-solution material is prohibited")
	}
	if !containsOne(proposal.ContextTrigger, facts) || isGlobalTrigger(proposal.ContextTrigger) {
		return admissionError(validation.CodeSchemaEnumInvalid, "context_trigger must be evidence-specific rather than global")
	}
	if !containsOne(proposal.NonObviousInsight, facts) {
		return admissionError(validation.CodeNonExactRef, "non_obvious_insight is not entailed by cited observable evidence")
	}
	if !stepsContainEvidence(proposal.DecisionPolicyOrSteps, facts) || isGenericAdvice(proposal.DecisionPolicyOrSteps) {
		return admissionError(validation.CodeSchemaEnumInvalid, "decision policy must be evidence-specific rather than pure generic advice")
	}
	if normalize(proposal.BaselineBehavior) == normalize(proposal.ExpectedBehaviorChange) {
		return admissionError(validation.CodeSchemaEnumInvalid, "expected_behavior_change must differ from baseline_behavior")
	}
	computed, err := contentDigest(proposal)
	if err != nil {
		return admissionError(validation.CodeNonIntegerNumber, "proposal cannot enter canonical hashed core: %v", err)
	}
	if proposal.ContentDigest != computed {
		return admissionError(validation.CodeDigestMismatch, "content_digest %s does not match canonical body %s", proposal.ContentDigest, computed)
	}
	return nil
}

func contentDigest(proposal RawSkillProposal) (string, error) {
	return contract.DigestOf(map[string]any{
		"proposal_id": proposal.ProposalID, "schema_version": proposal.SchemaVersion,
		"source_checkpoint_id": proposal.SourceCheckpointID, "source_evidence_refs": stringsAny(proposal.SourceEvidenceRefs),
		"context_trigger": proposal.ContextTrigger, "failure_or_opportunity": proposal.FailureOrOpportunity,
		"baseline_behavior": proposal.BaselineBehavior, "non_obvious_insight": proposal.NonObviousInsight,
		"decision_policy_or_steps": stringsAny(proposal.DecisionPolicyOrSteps), "expected_behavior_change": proposal.ExpectedBehaviorChange,
		"contraindications": stringsAny(proposal.Contraindications), "pitfalls": stringsAny(proposal.Pitfalls),
		"outcome_observed": proposal.OutcomeObserved, "novelty_status": proposal.NoveltyStatus,
		"created_by_agent_run_id": proposal.CreatedByAgentRunID, "created_at": proposal.CreatedAt,
	})
}

func stringsAny(values []string) []any { out := make([]any, len(values)); for i := range values { out[i] = values[i] }; return out }

func proposalRef(proposal RawSkillProposal) RawSkillProposalRef {
	return RawSkillProposalRef{ProposalID: proposal.ProposalID, ContentDigest: proposal.ContentDigest, URI: "raw-skill-proposal://gms/" + proposal.ProposalID + "@" + proposal.ContentDigest}
}

func containsOne(text string, facts []string) bool {
	text = normalize(text)
	for _, fact := range facts {
		if fact != "" && strings.Contains(text, normalize(fact)) {
			return true
		}
	}
	return false
}

func stepsContainEvidence(steps, facts []string) bool {
	for _, step := range steps {
		if containsOne(step, facts) {
			return true
		}
	}
	return false
}

func normalize(text string) string { return strings.Join(strings.Fields(strings.ToLower(text)), " ") }
func isGlobalTrigger(trigger string) bool {
	trigger = normalize(trigger)
	for _, forbidden := range []string{"all tasks", "every task", "any task", "always", "globally", "in general"} {
		if strings.Contains(trigger, forbidden) {
			return true
		}
	}
	return false
}
func isGenericAdvice(steps []string) bool {
	if len(steps) == 0 {
		return true
	}
	for _, step := range steps {
		step = normalize(step)
		for _, generic := range []string{"be careful", "try harder", "improve the code", "write better code", "use best practices"} {
			if strings.Contains(step, generic) {
				return true
			}
		}
	}
	return false
}
func hasProhibitedLeak(proposal RawSkillProposal) bool {
	text := normalize(strings.Join([]string{
		proposal.ContextTrigger, proposal.FailureOrOpportunity, proposal.BaselineBehavior,
		proposal.NonObviousInsight, proposal.ExpectedBehaviorChange, proposal.OutcomeObserved,
		strings.Join(proposal.DecisionPolicyOrSteps, " "), strings.Join(proposal.Contraindications, " "),
		strings.Join(proposal.Pitfalls, " "),
	}, " "))
	for _, forbidden := range []string{"hidden test", "hidden_test", "gold solution", "gold answer", "reference solution", "gold output"} {
		if strings.Contains(text, forbidden) {
			return true
		}
	}
	return false
}
func admissionError(code, format string, args ...any) error { return &validation.Error{Code: code, Detail: fmt.Sprintf(format, args...)} }

func cloneProposal(in RawSkillProposal) RawSkillProposal {
	in.SourceEvidenceRefs = append([]string(nil), in.SourceEvidenceRefs...)
	in.DecisionPolicyOrSteps = append([]string(nil), in.DecisionPolicyOrSteps...)
	in.Contraindications = append([]string(nil), in.Contraindications...)
	in.Pitfalls = append([]string(nil), in.Pitfalls...)
	return in
}

func cloneEvidence(in Evidence) Evidence {
	in.ObservableFacts = append([]string(nil), in.ObservableFacts...)
	return in
}

func cloneProvenance(in RawProposalProvenance) RawProposalProvenance {
	out := in
	out.SourceEvidence = make([]Evidence, len(in.SourceEvidence))
	for i := range in.SourceEvidence {
		out.SourceEvidence[i] = cloneEvidence(in.SourceEvidence[i])
	}
	return out
}

func cloneTrajectory(in FrozenTrajectory) FrozenTrajectory {
	out := in
	out.CompleteTrajectory = append([]string(nil), in.CompleteTrajectory...)
	out.Checkpoints = make(map[string]Checkpoint, len(in.Checkpoints))
	for k, v := range in.Checkpoints {
		out.Checkpoints[k] = v
	}
	out.Evidence = make(map[string]Evidence, len(in.Evidence))
	for k, v := range in.Evidence {
		out.Evidence[k] = cloneEvidence(v)
	}
	out.AuthorizedDiagnosisRunIDs = make(map[string]bool, len(in.AuthorizedDiagnosisRunIDs))
	for k, v := range in.AuthorizedDiagnosisRunIDs {
		out.AuthorizedDiagnosisRunIDs[k] = v
	}
	return out
}
