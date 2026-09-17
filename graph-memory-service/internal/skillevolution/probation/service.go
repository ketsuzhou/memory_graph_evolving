// Package probation governs advisory candidates that have enough
// independently evidenced continuation support for strictly context-bound
// exposure. It is intentionally not an activation authority: only Arm C may
// create an active Skill revision.
package probation

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

type ProbationState string

const (
	StateAdvisory  ProbationState = "advisory"
	StateProbation ProbationState = "probation"
	StateSuspended ProbationState = "suspended"
)

const ActivationAuthorityArmCOnly = "arm_c_policy_decision_only"
const (
	ReasonInsufficientIndependentContexts     = "INSUFFICIENT_INDEPENDENT_CONTEXTS"
	ReasonToolContractValidationRequired      = "TOOL_CONTRACT_VALIDATION_REQUIRED"
	ReasonRefutationThresholdReached          = "REFUTATION_THRESHOLD_REACHED"
	ReasonToolContractFailureThresholdReached = "TOOL_CONTRACT_FAILURE_THRESHOLD_REACHED"
	ReasonRecoveryEvidenceInsufficient        = "RECOVERY_EVIDENCE_INSUFFICIENT"
	ReasonRecoveryMustBeExplicit              = "RECOVERY_MUST_BE_EXPLICIT"
	ReasonUnknownCandidate                    = "UNKNOWN_PROBATION_CANDIDATE"
)

// Policy is versioned and fully explains the governed, non-activation path.
type Policy struct {
	Version                            string
	MinimumIndependentContexts         int
	RefutationSuspensionThreshold      int
	ContractFailureSuspensionThreshold int
	RecoveryAdditionalContexts         int
}

type ToolContractValidation struct {
	ResultRef           contract.VersionedRef
	ValidationID        string
	CandidateID         string
	CandidateBodyDigest string
	ContractRef         contract.VersionedRef
	Passed              bool
}
type ApplicabilityEnvelope struct{ ContextProfileDigests []string }
type Record struct {
	CandidateID           string
	Subject               usageprojection.UsageSubjectRef
	Kind                  string
	State                 ProbationState
	Envelope              ApplicabilityEnvelope
	PolicyVersion         string
	ActivationAuthority   string
	ToolContractValidated bool
	ContractFailureCount  int
	SuspendedContexts     []string
	Reason                string
	ChangedAt             time.Time
}
type AdmissionRequest struct {
	CandidateID              string
	Subject                  usageprojection.UsageSubjectRef
	Kind                     string
	ToolContractValidationID string
}
type RecoveryRequest struct {
	CandidateID string
	Explicit    bool
}
type Decision struct {
	Record Record
	Reason string
}

// Service uses only accepted usage observations; it has no dependency on an
// activation service, release decision, or runtime executor.
type Service struct {
	policy       Policy
	observations usageprojection.UsageProjectionStore
	now          func() time.Time
	mu           sync.Mutex
	records      map[string]Record
	validations  map[string]ToolContractValidation
}

func NewService(policy Policy, observations usageprojection.UsageProjectionStore) (*Service, error) {
	if policy.ContractFailureSuspensionThreshold == 0 {
		policy.ContractFailureSuspensionThreshold = policy.RefutationSuspensionThreshold
	}
	if policy.Version == "" || policy.MinimumIndependentContexts < 2 || policy.RefutationSuspensionThreshold < 1 || policy.ContractFailureSuspensionThreshold < 1 || policy.RecoveryAdditionalContexts < 1 || observations == nil {
		return nil, fmt.Errorf("probation: versioned policy, positive thresholds, and usage observations are required")
	}
	return &Service{policy: policy, observations: observations, now: time.Now, records: map[string]Record{}, validations: map[string]ToolContractValidation{}}, nil
}

func (s *Service) RegisterToolContractValidation(validation ToolContractValidation) bool {
	if validation.ValidationID == "" || validation.ResultRef.ID == "" || validation.ResultRef.Version == "" || validation.ResultRef.Digest == "" || validation.CandidateID == "" || validation.CandidateBodyDigest == "" || validation.ContractRef.ID == "" || validation.ContractRef.Version == "" || validation.ContractRef.Digest == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, found := s.validations[validation.ValidationID]; found {
		return existing == validation
	}
	s.validations[validation.ValidationID] = validation
	return true
}

func (s *Service) validation(id string) (ToolContractValidation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	validation, found := s.validations[id]
	return validation, found
}

func (s *Service) Admit(ctx context.Context, request AdmissionRequest) Decision {
	if request.CandidateID == "" || request.Subject.CanonicalKey() == "" {
		return Decision{Reason: ReasonInsufficientIndependentContexts}
	}
	contexts, err := s.continuationContexts(ctx, request.Subject)
	if err != nil {
		return Decision{Reason: ReasonInsufficientIndependentContexts}
	}
	record := Record{CandidateID: request.CandidateID, Subject: request.Subject, Kind: request.Kind, State: StateAdvisory, PolicyVersion: s.policy.Version, ActivationAuthority: ActivationAuthorityArmCOnly, ChangedAt: s.now()}
	validation, validationFound := s.validation(request.ToolContractValidationID)
	if validationFound {
		record.ToolContractValidated = validToolContractValidation(request.Subject, request.CandidateID, validation)
	}
	if len(contexts) < s.policy.MinimumIndependentContexts {
		record.Reason = ReasonInsufficientIndependentContexts
		return s.put(record)
	}
	if request.Kind == "tool" && !record.ToolContractValidated {
		record.Reason = ReasonToolContractValidationRequired
		return s.put(record)
	}
	record.State, record.Envelope, record.Reason = StateProbation, ApplicabilityEnvelope{ContextProfileDigests: contexts}, ""
	return s.put(record)
}

// AdvisoryVisible preserves ordinary advisory visibility for unknown records;
// a governed probation record becomes visible only inside its exact envelope.
func (s *Service) AdvisoryVisible(candidateID string, profile usageprojection.ContextProfile) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[candidateID]
	if !found || record.State == StateAdvisory {
		return true
	}
	if record.State != StateProbation {
		return false
	}
	for _, digest := range record.Envelope.ContextProfileDigests {
		if digest == profile.Digest() {
			return true
		}
	}
	return false
}

// Observe is the governance degradation executor for a persisted Arm B
// observation. Unknown or active skills are deliberately ignored: no usage
// observation can mint probation or change Arm C activation state.
func (s *Service) Observe(ctx context.Context, subject usageprojection.UsageSubjectRef) {
	if subject.CandidateRef == nil || subject.SkillRef != nil {
		return
	}
	_, _ = s.Record(subject.CandidateRef.CandidateID)
	_ = s.Reconcile(ctx, subject.CandidateRef.CandidateID)
}

func (s *Service) Reconcile(ctx context.Context, candidateID string) Decision {
	s.mu.Lock()
	record, found := s.records[candidateID]
	s.mu.Unlock()
	if !found {
		return Decision{Reason: ReasonUnknownCandidate}
	}
	if record.State != StateProbation {
		return Decision{Record: record, Reason: record.Reason}
	}
	count, err := s.refutationCount(ctx, record.Subject)
	if err == nil && count >= s.policy.RefutationSuspensionThreshold {
		contexts, _ := s.continuationContexts(ctx, record.Subject)
		record.State, record.Envelope, record.SuspendedContexts, record.Reason, record.ChangedAt = StateSuspended, ApplicabilityEnvelope{}, contexts, ReasonRefutationThresholdReached, s.now()
		return s.put(record)
	}
	return Decision{Record: record}
}

func (s *Service) ReportToolContractValidation(candidateID string, validation ToolContractValidation) Decision {
	if !s.RegisterToolContractValidation(validation) {
		return Decision{Reason: ReasonToolContractValidationRequired}
	}
	s.mu.Lock()
	record, found := s.records[candidateID]
	s.mu.Unlock()
	if !found {
		return Decision{Reason: ReasonUnknownCandidate}
	}
	if validation.Passed && validation.ValidationID != "" && validation.ResultRef.ID != "" && validation.ResultRef.Version != "" && validation.ResultRef.Digest != "" && validation.CandidateID == record.CandidateID && record.Subject.CandidateRef != nil && validation.CandidateBodyDigest == record.Subject.CandidateRef.BodyDigest && validation.ContractRef.ID != "" && validation.ContractRef.Version != "" && validation.ContractRef.Digest != "" {
		record.ToolContractValidated = true
		return s.put(record)
	}
	record.ContractFailureCount++
	record.ToolContractValidated = false
	if record.ContractFailureCount >= s.policy.ContractFailureSuspensionThreshold {
		contexts, _ := s.continuationContexts(context.Background(), record.Subject)
		record.State, record.Envelope, record.SuspendedContexts, record.Reason, record.ChangedAt = StateSuspended, ApplicabilityEnvelope{}, contexts, ReasonToolContractFailureThresholdReached, s.now()
	}
	return s.put(record)
}

func (s *Service) Recover(ctx context.Context, request RecoveryRequest) Decision {
	s.mu.Lock()
	record, found := s.records[request.CandidateID]
	s.mu.Unlock()
	if !found {
		return Decision{Reason: ReasonUnknownCandidate}
	}
	if !request.Explicit {
		return Decision{Record: record, Reason: ReasonRecoveryMustBeExplicit}
	}
	if record.State != StateSuspended {
		return Decision{Record: record, Reason: record.Reason}
	}
	contexts, err := s.continuationContexts(ctx, record.Subject)
	need := s.policy.RecoveryAdditionalContexts
	if err != nil || countNewContexts(contexts, record.SuspendedContexts) < need || (record.Kind == "tool" && !record.ToolContractValidated) {
		return Decision{Record: record, Reason: ReasonRecoveryEvidenceInsufficient}
	}
	record.State, record.Envelope, record.SuspendedContexts, record.Reason, record.ChangedAt = StateProbation, ApplicabilityEnvelope{ContextProfileDigests: contexts}, nil, "", s.now()
	return s.put(record)
}

func (s *Service) Record(candidateID string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[candidateID]
	return clone(record), found
}
func (s *Service) put(record Record) Decision {
	s.mu.Lock()
	s.records[record.CandidateID] = clone(record)
	s.mu.Unlock()
	return Decision{Record: record, Reason: record.Reason}
}

func (s *Service) continuationContexts(ctx context.Context, subject usageprojection.UsageSubjectRef) ([]string, error) {
	snapshot, err := s.observations.Read(ctx)
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, observation := range snapshot.Interactions {
		if observation.DiagnosticOnly || observation.Subject.CanonicalKey() != subject.CanonicalKey() || observation.Stage != usageprojection.InteractionVerified || !admitted(observation.EvidenceRefs) {
			continue
		}
		set[observation.ContextProfile.Digest()] = true
	}
	out := make([]string, 0, len(set))
	for digest := range set {
		out = append(out, digest)
	}
	sort.Strings(out)
	return out, nil
}
func (s *Service) refutationCount(ctx context.Context, subject usageprojection.UsageSubjectRef) (int, error) {
	snapshot, err := s.observations.Read(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, assessment := range snapshot.Assessments {
		if assessment.DiagnosticOnly || assessment.Subject.CanonicalKey() != subject.CanonicalKey() || len(assessment.CounterevidenceRefs) == 0 || !admitted(assessment.EvidenceRefs) || !admitted(assessment.CounterevidenceRefs) {
			continue
		}
		count++
	}
	return count, nil
}
func admitted(refs []contract.EvidenceRef) bool {
	if len(refs) == 0 {
		return false
	}
	for _, ref := range refs {
		if ref.CommitState != "committed" && ref.CommitState != "sealed" {
			return false
		}
	}
	return true
}
func validToolContractValidation(subject usageprojection.UsageSubjectRef, candidateID string, validation ToolContractValidation) bool {
	ref := subject.CandidateRef
	return ref != nil && subject.SkillRef == nil && validation.Passed && validation.ValidationID != "" && validation.ResultRef.ID != "" && validation.ResultRef.Version != "" && validation.ResultRef.Digest != "" && validation.CandidateID == candidateID && validation.CandidateID == ref.CandidateID && validation.CandidateBodyDigest == ref.BodyDigest && validation.ContractRef.ID != "" && validation.ContractRef.Version != "" && validation.ContractRef.Digest != ""
}

func countNewContexts(current, prior []string) int {
	seen := map[string]bool{}
	for _, digest := range prior {
		seen[digest] = true
	}
	count := 0
	for _, digest := range current {
		if !seen[digest] {
			count++
		}
	}
	return count
}

func clone(record Record) Record {
	record.Envelope.ContextProfileDigests = append([]string(nil), record.Envelope.ContextProfileDigests...)
	record.SuspendedContexts = append([]string(nil), record.SuspendedContexts...)
	return record
}

// UsageSummaryTier classifies an exact candidate for the aggregate read
// without exposing mutable governance records or changing activation state.
func (s *Service) UsageSummaryTier(ref contract.CandidateArtifactRef, profile usageprojection.ContextProfile) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[ref.CandidateID]
	if !found || record.Subject.CandidateRef == nil || *record.Subject.CandidateRef != ref || record.State == StateAdvisory {
		return "advisory"
	}
	if record.State == StateSuspended {
		return "suspended"
	}
	for _, digest := range record.Envelope.ContextProfileDigests {
		if digest == profile.Digest() {
			return "probation"
		}
	}
	return "suspended"
}
