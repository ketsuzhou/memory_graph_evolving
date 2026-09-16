// Package promotion implements the PG-23 coordination boundary between
// Diagnosis-owned proposals, isolated evaluation replay, and production
// activation (Q90=C, SC-6.x): stable ownership, sandbox-only replay, exact
// base-revision CAS promotion, and rollback through Promotion Records.
package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

var ErrPromotionNotImplemented = errors.New("promotion: not implemented")

var (
	ErrProposalIdentityRequired = errors.New("promotion: proposal requires a stable owner and id")
	ErrProposalUnknown          = errors.New("promotion: proposal was never registered")
	ErrProposalIdentityConflict = errors.New("promotion: proposal id already registered with different owner/visibility/target")
	ErrReplayNotIsolated        = errors.New("promotion: replay must run in a distinct clone Room")
	ErrMutationBranchRequired   = errors.New("promotion: replay branch must be replay_skill_mutation")
	ErrSandboxRequired          = errors.New("promotion: replay side effects are sandbox-only")
	ErrBaselineRequired         = errors.New("promotion: replay requires a matched baseline reference")
	ErrBudgetRequired           = errors.New("promotion: replay requires a positive execution budget")
	ErrPromotionIncomplete      = errors.New("promotion: promotion requires evaluation evidence and an exact base revision")
	ErrStaleBaseRevision        = errors.New("promotion: base revision CAS conflict; refresh against the active revision")
	ErrPromotionRecordUnknown   = errors.New("promotion: promotion record not found")
	// P0-4 evidence chain errors: replay results and evaluation activations
	// are immutable records, and production promotion only admits an
	// activation whose proposal/target/base/candidate bindings match exactly.
	ErrReplaySessionUnknown        = errors.New("promotion: replay session was never prepared")
	ErrReplaySessionMismatch       = errors.New("promotion: replay result does not match the session's immutable proposal binding")
	ErrCandidateRequired           = errors.New("promotion: replay requires the candidate revision under evaluation")
	ErrReplayResultIncomplete      = errors.New("promotion: replay result requires a closed outcome, policy digest, threshold, and evidence digest")
	ErrReplayResultConflict        = errors.New("promotion: replay result id already recorded with different facts")
	ErrReplayNotPassed             = errors.New("promotion: evaluation activation requires a passed replay result")
	ErrEvaluationActivationUnknown = errors.New("promotion: evaluation ref does not resolve to an activation record")
	ErrEvaluationBindingMismatch   = errors.New("promotion: evaluation activation does not bind this proposal/target/base/candidate")
)

type Namespace string

const (
	NamespaceEvaluation Namespace = "evaluation"
	NamespaceProduction Namespace = "production"
)

type Visibility string

const (
	VisibilityPrivate Visibility = "private"
	VisibilityShared  Visibility = "shared"
)

// Target is deliberately independent of Proposal owner and visibility.
type Target struct {
	Namespace Namespace
	TenantID  domain.TenantID
	Name      string
}

// Proposal preserves stable Diagnosis Agent ownership; a run identifier is
// intentionally not represented as its owner.
type Proposal struct {
	ID         string
	Owner      domain.PrincipalID
	Visibility Visibility
	Target     Target
}

type ReplayBudget struct {
	MaxCostUnits int64
}

type ReplayRequest struct {
	Proposal           Proposal
	SourceRoomID       string
	CloneRoomID        string
	AncestorRoomID     string
	MutationBranch     string
	SandboxOnlyEffects bool
	MatchedBaselineRef string
	CandidateRef       string
	Budget             ReplayBudget
}

// ReplaySession is the immutable canonical replay request (R2): every fact a
// later verdict or activation may bind against is frozen here at preparation
// time, and the session ID digests the whole binding so a divergent request
// can never reuse another session's identity.
type ReplaySession struct {
	ID                 string
	ProposalID         string
	SourceRoomID       string
	CloneRoomID        string
	AncestorRoomID     string
	MutationBranch     string
	SandboxOnlyEffects bool
	MatchedBaselineRef string
	CandidateRef       string
	Budget             ReplayBudget
}

// ReplayOutcome is the closed result vocabulary for an evaluation replay.
type ReplayOutcome string

const (
	ReplayPass            ReplayOutcome = "pass"
	ReplayFail            ReplayOutcome = "fail"
	ReplayInconclusive    ReplayOutcome = "inconclusive"
	ReplayBudgetExhausted ReplayOutcome = "budget_exhausted"
)

// ReplayResultRequest records the immutable outcome of one prepared replay
// session: a closed outcome plus the evaluation policy digest, the pass
// threshold, and the evidence digest the verdict was computed over.
type ReplayResultRequest struct {
	SessionID              string
	ProposalID             string
	Outcome                ReplayOutcome
	EvaluationPolicyDigest string
	PassThreshold          string
	EvidenceDigest         string
}

// ReplayResult is the immutable verdict record; only ReplayPass can feed an
// evaluation activation.
type ReplayResult struct {
	ID                     string
	SessionID              string
	ProposalID             string
	Outcome                ReplayOutcome
	EvaluationPolicyDigest string
	PassThreshold          string
	EvidenceDigest         string
	RecordedAt             time.Time
}

// EvaluationActivationRequest activates a passed replay result in the
// evaluation namespace, pinning the exact target/candidate/base bindings a
// later production promotion must match.
type EvaluationActivationRequest struct {
	ReplayResultRef   string
	ProposalID        string
	EvaluationTarget  Target
	CandidateRef      string
	ExactBaseRevision string
}

// EvaluationActivation is the immutable activation record; its Ref is the
// only EvaluationRef a production Promote admits.
type EvaluationActivation struct {
	Ref               string
	ReplayResultRef   string
	ProposalID        string
	EvaluationTarget  Target
	CandidateRef      string
	ExactBaseRevision string
	ActivatedAt       time.Time
}

type PromotionRequest struct {
	ProposalID string
	// EvaluationRef must resolve to an EvaluationActivation recorded through
	// ActivateEvaluation; free-form strings are rejected (P0-4).
	EvaluationRef string
	// CandidateRef must equal the activated candidate binding.
	CandidateRef      string
	ProductionTarget  Target
	ExactBaseRevision string
	// PromotedRevision is the new revision the target will carry after a
	// successful CAS; a promotion must actually move the revision.
	PromotedRevision string
}

// PromotionRecord is the durable evidence required before an evaluation
// result can activate production, and carries the rollback relationship.
type PromotionRecord struct {
	ID                string
	ProposalID        string
	EvaluationRef     string
	CandidateRef      string
	ProductionTarget  Target
	ExactBaseRevision string
	PromotedRevision  string
	RollbackRevision  string
}

type RollbackRequest struct {
	PromotionRecordID string
	ExpectedRevision  string
}

const mutationBranch = "replay_skill_mutation"

// Service is the in-memory PG-23 coordination authority. Registration is the
// only write path for proposals: ownership and visibility are immutable
// facts once registered, and activation state changes exclusively through
// CAS-checked Promote/Rollback.
type Service struct {
	mu             sync.Mutex
	proposals      map[string]*Proposal
	sessions       map[string]*ReplaySession
	replayResults  map[string]*ReplayResult
	activations    map[string]*EvaluationActivation
	records        map[string]*PromotionRecord
	activeRevision map[string]string
	now            func() time.Time
}

func NewService() *Service {
	return &Service{
		proposals:      map[string]*Proposal{},
		sessions:       map[string]*ReplaySession{},
		replayResults:  map[string]*ReplayResult{},
		activations:    map[string]*EvaluationActivation{},
		records:        map[string]*PromotionRecord{},
		activeRevision: map[string]string{},
		now:            time.Now,
	}
}

// RegisterProposal registers a proposal once: ownership, visibility, and
// target are immutable facts. An identical replay is idempotent; a same-ID
// registration that changes any of them is rejected (Q90=C).
func (s *Service) RegisterProposal(_ context.Context, proposal Proposal) (*Proposal, error) {
	if proposal.ID == "" || proposal.Owner == "" {
		return nil, ErrProposalIdentityRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.proposals[proposal.ID]; ok {
		if existing.Owner == proposal.Owner && existing.Visibility == proposal.Visibility && existing.Target == proposal.Target {
			return cloneProposal(existing), nil
		}
		return nil, ErrProposalIdentityConflict
	}
	stored := proposal
	s.proposals[proposal.ID] = &stored
	return cloneProposal(&stored), nil
}

// PrepareReplay validates the isolation contract before any replay session
// exists: distinct clone Room, replay_skill_mutation ancestry, sandbox-only
// effects, a matched baseline, a named candidate, and a positive budget. The
// prepared session is the immutable canonical binding (R2): its ID digests
// every binding fact, so an identical request replays the same session and a
// divergent one (different ancestor, baseline, or candidate) prepares a
// distinct session instead of rebinding the old one.
func (s *Service) PrepareReplay(_ context.Context, req ReplayRequest) (*ReplaySession, error) {
	if req.Proposal.ID == "" {
		return nil, ErrProposalUnknown
	}
	if req.CloneRoomID == "" || req.CloneRoomID == req.SourceRoomID {
		return nil, ErrReplayNotIsolated
	}
	if req.MutationBranch != mutationBranch {
		return nil, ErrMutationBranchRequired
	}
	if !req.SandboxOnlyEffects {
		return nil, ErrSandboxRequired
	}
	if req.MatchedBaselineRef == "" {
		return nil, ErrBaselineRequired
	}
	if req.CandidateRef == "" {
		return nil, ErrCandidateRequired
	}
	if req.Budget.MaxCostUnits <= 0 {
		return nil, ErrBudgetRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	registered, ok := s.proposals[req.Proposal.ID]
	if !ok {
		return nil, ErrProposalUnknown
	}
	// The replayed proposal body must agree with the write-once registration;
	// ownership, visibility, and target are never re-declared here (Q90=C).
	if registered.Owner != req.Proposal.Owner || registered.Visibility != req.Proposal.Visibility || registered.Target != req.Proposal.Target {
		return nil, ErrProposalIdentityConflict
	}
	id := "replay-" + shortDigest(req.Proposal.ID+"|"+req.SourceRoomID+"|"+req.CloneRoomID+"|"+req.AncestorRoomID+"|"+req.MutationBranch+"|"+req.MatchedBaselineRef+"|"+req.CandidateRef+"|"+strconv.FormatInt(req.Budget.MaxCostUnits, 10))
	if existing, ok := s.sessions[id]; ok {
		return cloneSession(existing), nil
	}
	session := &ReplaySession{
		ID: id, ProposalID: req.Proposal.ID,
		SourceRoomID: req.SourceRoomID, CloneRoomID: req.CloneRoomID, AncestorRoomID: req.AncestorRoomID,
		MutationBranch: req.MutationBranch, SandboxOnlyEffects: req.SandboxOnlyEffects,
		MatchedBaselineRef: req.MatchedBaselineRef, CandidateRef: req.CandidateRef, Budget: req.Budget,
	}
	s.sessions[id] = session
	return cloneSession(session), nil
}

var digestShape = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// RecordReplayResult writes the immutable verdict of a prepared replay
// session (P0-4): the session must exist, the proposal must be registered,
// the outcome must be in the closed vocabulary, and the policy/evidence
// digests plus threshold must be present and well-shaped. Re-recording the
// same facts is idempotent; any divergence on the derived ID is a conflict.
func (s *Service) RecordReplayResult(_ context.Context, req ReplayResultRequest) (*ReplayResult, error) {
	switch req.Outcome {
	case ReplayPass, ReplayFail, ReplayInconclusive, ReplayBudgetExhausted:
	default:
		return nil, ErrReplayResultIncomplete
	}
	if req.SessionID == "" || req.ProposalID == "" || req.PassThreshold == "" {
		return nil, ErrReplayResultIncomplete
	}
	if !digestShape.MatchString(req.EvaluationPolicyDigest) || !digestShape.MatchString(req.EvidenceDigest) {
		return nil, ErrReplayResultIncomplete
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[req.SessionID]
	if !ok || session.ID != req.SessionID {
		return nil, ErrReplaySessionUnknown
	}
	// (R2) the verdict's (session, proposal) pair must match the immutable
	// session binding: a session prepared for one proposal cannot carry a
	// verdict recorded against another.
	if session.ProposalID != req.ProposalID {
		return nil, ErrReplaySessionMismatch
	}
	if _, ok := s.proposals[req.ProposalID]; !ok {
		return nil, ErrProposalUnknown
	}
	id := "replay-result-" + shortDigest(req.SessionID+"|"+req.ProposalID+"|"+string(req.Outcome)+"|"+req.EvaluationPolicyDigest+"|"+req.PassThreshold+"|"+req.EvidenceDigest)
	if existing, ok := s.replayResults[id]; ok {
		return cloneReplayResult(existing), nil
	}
	// The same session may only carry one verdict per (proposal, outcome,
	// evidence) identity: a second result for the session that disagrees on
	// any fact is rejected rather than silently superseding the record.
	for _, result := range s.replayResults {
		if result.SessionID == req.SessionID && result.ProposalID == req.ProposalID && result.ID != id {
			return nil, ErrReplayResultConflict
		}
	}
	result := &ReplayResult{
		ID:                     id,
		SessionID:              req.SessionID,
		ProposalID:             req.ProposalID,
		Outcome:                req.Outcome,
		EvaluationPolicyDigest: req.EvaluationPolicyDigest,
		PassThreshold:          req.PassThreshold,
		EvidenceDigest:         req.EvidenceDigest,
		RecordedAt:             s.now(),
	}
	s.replayResults[id] = result
	return cloneReplayResult(result), nil
}

// ActivateEvaluation turns a passed replay result into an immutable
// evaluation-namespace activation record pinning proposal/target/candidate/
// base. Non-pass outcomes, unknown results, and non-evaluation targets are
// rejected; the activation Ref is the only evaluation evidence a production
// Promote admits.
func (s *Service) ActivateEvaluation(_ context.Context, req EvaluationActivationRequest) (*EvaluationActivation, error) {
	if req.ReplayResultRef == "" || req.ProposalID == "" || req.CandidateRef == "" || req.ExactBaseRevision == "" {
		return nil, ErrPromotionIncomplete
	}
	if req.EvaluationTarget.Namespace != NamespaceEvaluation {
		return nil, errors.New("promotion: evaluation activation requires an evaluation-namespace target")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.replayResults[req.ReplayResultRef]
	if !ok {
		return nil, ErrEvaluationActivationUnknown
	}
	if result.ProposalID != req.ProposalID {
		return nil, ErrEvaluationBindingMismatch
	}
	if result.Outcome != ReplayPass {
		return nil, ErrReplayNotPassed
	}
	// (R2) the activation binds to the immutable session snapshot and the
	// write-once proposal registration, not to caller-declared fields: the
	// candidate must be the one the session was prepared to evaluate, and the
	// evaluation target must be exactly the proposal's registered target.
	session, ok := s.sessions[result.SessionID]
	if !ok {
		return nil, ErrReplaySessionUnknown
	}
	if session.ProposalID != req.ProposalID {
		return nil, ErrReplaySessionMismatch
	}
	proposal, ok := s.proposals[req.ProposalID]
	if !ok {
		return nil, ErrProposalUnknown
	}
	if req.CandidateRef != session.CandidateRef || req.EvaluationTarget != proposal.Target {
		return nil, ErrEvaluationBindingMismatch
	}
	// The base revision is the production projection's active state at
	// activation time, not a free field: once a promotion has moved the
	// target's active revision, only that revision can back a fresh
	// activation — the same CAS Promote enforces on the record.
	prodKey := targetKey(Target{Namespace: NamespaceProduction, TenantID: proposal.Target.TenantID, Name: proposal.Target.Name})
	if active := s.activeRevision[prodKey]; active != "" && active != req.ExactBaseRevision {
		return nil, ErrStaleBaseRevision
	}
	ref := "evaluation-activation-" + shortDigest(req.ReplayResultRef+"|"+req.ProposalID+"|"+targetKey(req.EvaluationTarget)+"|"+req.CandidateRef+"|"+req.ExactBaseRevision)
	if existing, ok := s.activations[ref]; ok {
		return cloneActivation(existing), nil
	}
	activation := &EvaluationActivation{
		Ref:               ref,
		ReplayResultRef:   req.ReplayResultRef,
		ProposalID:        req.ProposalID,
		EvaluationTarget:  req.EvaluationTarget,
		CandidateRef:      req.CandidateRef,
		ExactBaseRevision: req.ExactBaseRevision,
		ActivatedAt:       s.now(),
	}
	s.activations[ref] = activation
	return cloneActivation(activation), nil
}

// Promote activates an evaluation result in production under exact
// base-revision CAS: the evaluation ref must resolve to an activation
// record whose proposal/target/candidate/base bindings match the request
// exactly, the target's active revision must still equal the evaluated
// base, and success moves the active revision to the promoted revision
// while the record retains the previous revision as its rollback target.
func (s *Service) Promote(_ context.Context, req PromotionRequest) (*PromotionRecord, error) {
	if req.ProposalID == "" {
		return nil, ErrProposalUnknown
	}
	if req.EvaluationRef == "" || req.ExactBaseRevision == "" || req.CandidateRef == "" {
		return nil, ErrPromotionIncomplete
	}
	if req.PromotedRevision == "" || req.PromotedRevision == req.ExactBaseRevision {
		return nil, ErrPromotionIncomplete
	}
	if req.ProductionTarget.Namespace != NamespaceProduction {
		return nil, errors.New("promotion: only production targets require promotion records")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.proposals[req.ProposalID]; !ok {
		return nil, ErrProposalUnknown
	}
	activation, ok := s.activations[req.EvaluationRef]
	if !ok {
		return nil, ErrEvaluationActivationUnknown
	}
	if activation.ProposalID != req.ProposalID ||
		activation.CandidateRef != req.CandidateRef ||
		activation.ExactBaseRevision != req.ExactBaseRevision ||
		activation.EvaluationTarget.TenantID != req.ProductionTarget.TenantID ||
		activation.EvaluationTarget.Name != req.ProductionTarget.Name {
		return nil, ErrEvaluationBindingMismatch
	}
	key := targetKey(req.ProductionTarget)
	active := s.activeRevision[key]
	// Exact base CAS: an unset active revision admits the declared base;
	// any other current revision makes the request stale.
	if active != "" && active != req.ExactBaseRevision {
		return nil, ErrStaleBaseRevision
	}
	rollback := active
	if rollback == "" {
		rollback = req.ExactBaseRevision
	}
	record := &PromotionRecord{
		ID:                "promotion-" + shortDigest(req.ProposalID+"|"+req.EvaluationRef+"|"+key+"|"+req.PromotedRevision),
		ProposalID:        req.ProposalID,
		EvaluationRef:     req.EvaluationRef,
		CandidateRef:      req.CandidateRef,
		ProductionTarget:  req.ProductionTarget,
		ExactBaseRevision: req.ExactBaseRevision,
		PromotedRevision:  req.PromotedRevision,
		RollbackRevision:  rollback,
	}
	s.records[record.ID] = record
	s.activeRevision[key] = req.PromotedRevision
	return cloneRecord(record), nil
}

// Rollback repoints the production target at the record's rollback revision.
// It is a double CAS: the caller must present the record's exact promoted
// revision, and the target's active revision must still be that promoted
// revision — a superseded record can never overwrite a newer activation.
func (s *Service) Rollback(_ context.Context, req RollbackRequest) (*PromotionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[req.PromotionRecordID]
	if !ok {
		return nil, ErrPromotionRecordUnknown
	}
	if req.ExpectedRevision != record.PromotedRevision {
		return nil, ErrStaleBaseRevision
	}
	key := targetKey(record.ProductionTarget)
	if s.activeRevision[key] != record.PromotedRevision {
		return nil, ErrStaleBaseRevision
	}
	s.activeRevision[key] = record.RollbackRevision
	return cloneRecord(record), nil
}

func targetKey(target Target) string {
	return string(target.TenantID) + "\x00" + string(target.Namespace) + "\x00" + target.Name
}

func cloneRecord(record *PromotionRecord) *PromotionRecord {
	cloned := *record
	return &cloned
}

func cloneReplayResult(result *ReplayResult) *ReplayResult {
	cloned := *result
	return &cloned
}

func cloneActivation(activation *EvaluationActivation) *EvaluationActivation {
	cloned := *activation
	return &cloned
}

func cloneProposal(proposal *Proposal) *Proposal {
	cloned := *proposal
	return &cloned
}

func cloneSession(session *ReplaySession) *ReplaySession {
	cloned := *session
	return &cloned
}

func shortDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])[:16]
}
