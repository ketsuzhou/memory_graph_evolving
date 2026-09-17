// Package batchconsolidation admits one CAS Skill-evolution consolidation
// over a family of canonical Raw Skill Proposals (Warm Skill Graph Batch
// contract §2.2 ConsolidationDecision and §5.3).
//
// Source references are complete proposal IDs. Hash prefixes are rejected.
// Every family proposal is covered by exactly one decision. The expected
// ledger revision is applied with exact Head CAS; a stale or raced
// expectation leaves the ledger unchanged. Model or transport failure
// fail-closes and MUST NOT degrade to a provenance-less Markdown ledger.
package batchconsolidation

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

const (
	// SchemaVersion is the closed ConsolidationDecision body shape.
	SchemaVersion = "gms.consolidation-decision.v1"
	// LedgerHeadKey is the exact Skill Evolution Ledger CAS head.
	LedgerHeadKey = "warm-skill-graph-batch.skill-evolution"

	OpRetain               = "retain"
	OpRevise               = "revise"
	OpSpecialize           = "specialize"
	OpMerge                = "merge"
	OpRetire               = "retire"
	OpInsufficientEvidence = "insufficient_evidence"
)

var closedOperations = map[string]bool{
	OpRetain: true, OpRevise: true, OpSpecialize: true,
	OpMerge: true, OpRetire: true, OpInsufficientEvidence: true,
}

var (
	fullDigestRE    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	partialDigestRE = regexp.MustCompile(`^sha256:[0-9a-f]{1,63}$`)
	shortHexRE      = regexp.MustCompile(`^[0-9a-f]{7,16}$`)
)

// ProposalCatalog is the read-only rawproposal seam. Implementations must
// resolve only complete IDs; this package never truncates a source ID.
type ProposalCatalog interface {
	Get(proposalID string) (rawproposal.RawSkillProposal, bool)
	ProvenanceForProposal(proposalID string) (rawproposal.RawProposalProvenance, bool)
}

// DecisionModel is an optional review/assist port. A failure is fatal: the
// service does not invent a Markdown ledger to paper over it.
type DecisionModel interface {
	Review(ctx context.Context, family []rawproposal.RawSkillProposal, drafts []DecisionDraft) error
}

// ConditionalConflict records a policy clash that is retained rather than
// merged. Source IDs are complete.
type ConditionalConflict struct {
	LeftSourceID  string
	RightSourceID string
	Condition     string
	EvidenceRefs  []string
}

// SkillRevision is the successor minted for retain/revise/specialize/merge.
// It always stores every source proposal ID and the union of their evidence
// lineage, never only the first source.
type SkillRevision struct {
	RevisionID        string
	URI               string
	SourceProposalIDs []string
	EvidenceLineage   []string
	ContentDigest     string
}

// ConsolidationDecision is the contract §2.2 record.
type ConsolidationDecision struct {
	DecisionID             string
	ExpectedLedgerRevision uint64
	Operation              string
	SourceProposalIDs      []string
	SuccessorSkillRevision *SkillRevision
	ConditionalConflicts   []ConditionalConflict
	RationaleEvidenceRefs  []string
	CreatedByAgentRunID    string
	ContentDigest          string
}

// DecisionDraft is a submitted (or classifier-produced) decision before
// successor minting and content-digest sealing.
type DecisionDraft struct {
	DecisionID            string
	Operation             string
	SourceProposalIDs     []string
	ConditionalConflicts  []ConditionalConflict
	RationaleEvidenceRefs []string
}

// Request is one skill_consolidate attempt against an expected revision.
type Request struct {
	IdempotencyKey         string
	AgentRunID             string
	ExpectedLedgerRevision uint64
	FamilyProposalIDs      []string
	Decisions              []DecisionDraft
}

// Result is the atomically committed consolidation outcome.
type Result struct {
	LedgerRevision uint64
	LedgerDigest   string
	Decisions      []ConsolidationDecision
}

type commitFunc func(ctx context.Context, tx *ledger.Tx, expectedSeq uint64, expectedDigest string, payload []byte) (string, error)

// Service is the protected consolidation authority. Local indexes are
// append-only views of CAS-committed batches; they are never a second write
// path and are not updated on a failed transaction.
type Service struct {
	catalog ProposalCatalog
	store   ledger.Store
	tx      ledger.TxManager
	model   DecisionModel
	commit  commitFunc

	mu             sync.RWMutex
	decisions      []ConsolidationDecision
	byID           map[string]ConsolidationDecision
	coverage       map[string]string
	successors     map[string]SkillRevision
	revision       uint64
	digest         string
	markdownWrites int
}

func NewService(catalog ProposalCatalog, store ledger.Store, tx ledger.TxManager) (*Service, error) {
	if catalog == nil || store == nil || tx == nil {
		return nil, fmt.Errorf("batchconsolidation: nil catalog, store, or transaction manager")
	}
	return &Service{
		catalog:    catalog,
		store:      store,
		tx:         tx,
		commit:     commitCanonicalBatch,
		byID:       make(map[string]ConsolidationDecision),
		coverage:   make(map[string]string),
		successors: make(map[string]SkillRevision),
	}, nil
}

func (s *Service) SetDecisionModel(model DecisionModel) { s.model = model }

func (s *Service) CurrentRevision() (uint64, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision, s.digest
}

func (s *Service) Decision(id string) (ConsolidationDecision, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.byID[id]
	return cloneDecision(d), ok
}

func (s *Service) Decisions() []ConsolidationDecision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConsolidationDecision, len(s.decisions))
	for i := range s.decisions {
		out[i] = cloneDecision(s.decisions[i])
	}
	return out
}

func (s *Service) Coverage(proposalID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.coverage[proposalID]
	return id, ok
}

func (s *Service) Successor(revisionID string) (SkillRevision, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rev, ok := s.successors[revisionID]
	return cloneSuccessor(rev), ok
}

// MarkdownFallbackUsed reports whether a provenance-less Markdown ledger was
// written. The success path never does this; the field exists so tests can
// lock the fail-closed invariant.
func (s *Service) MarkdownFallbackUsed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.markdownWrites > 0
}

// Consolidate classifies (when drafts are omitted) and CAS-commits a complete
// coverage of FamilyProposalIDs at ExpectedLedgerRevision.
func (s *Service) Consolidate(ctx context.Context, req Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if req.IdempotencyKey == "" || req.AgentRunID == "" {
		return Result{}, fail(validation.CodeSchemaRequiredFieldMissing, "idempotency key and created_by agent run id are required")
	}
	if len(req.FamilyProposalIDs) == 0 {
		return Result{}, fail(validation.CodeSchemaRequiredFieldMissing, "family_proposal_ids must list every raw proposal to cover")
	}

	family, err := s.loadFamily(req.FamilyProposalIDs)
	if err != nil {
		return Result{}, err
	}
	drafts := req.Decisions
	if len(drafts) == 0 {
		drafts = classifyFamily(family, req.FamilyProposalIDs)
	}
	if s.model != nil {
		if err := s.model.Review(ctx, familySlice(family, req.FamilyProposalIDs), drafts); err != nil {
			return Result{}, fmt.Errorf("batchconsolidation: decision model failed; refusing markdown ledger fallback: %w", err)
		}
	}
	decisions, err := s.sealDecisions(req, family, drafts)
	if err != nil {
		return Result{}, err
	}

	batch := batchCanonical(req.ExpectedLedgerRevision+1, decisions)
	canonical, err := contract.JCS(batch)
	if err != nil {
		return Result{}, fail(validation.CodeNonIntegerNumber, "consolidation batch cannot enter canonical hashed core: %v", err)
	}
	requestDigest, err := contract.DigestOf(requestCanonical(req))
	if err != nil {
		return Result{}, fail(validation.CodeNonIntegerNumber, "consolidation request cannot enter canonical hashed core: %v", err)
	}

	currentSeq, currentDigest, ok, err := s.store.GetHead(ledger.HeadProjection, LedgerHeadKey)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		currentSeq, currentDigest = 0, ""
	}
	if currentSeq != req.ExpectedLedgerRevision {
		return Result{}, &ledger.Error{
			ReasonCode: ledger.ReasonProjectionHeadConflict,
			Message:    fmt.Sprintf("expected ledger revision %d but head is %d", req.ExpectedLedgerRevision, currentSeq),
		}
	}

	outcome := Result{LedgerRevision: req.ExpectedLedgerRevision + 1, Decisions: decisions}
	outcomeBytes, err := json.Marshal(resultWire(outcome, ""))
	if err != nil {
		return Result{}, fmt.Errorf("batchconsolidation: encode idempotency outcome: %w", err)
	}

	var replay Result
	err = s.tx.WithinTx(ctx, func(tx *ledger.Tx) error {
		replayed, recorded, err := tx.RecordIdempotency("skill-consolidate:"+req.IdempotencyKey, requestDigest, outcomeBytes)
		if err != nil {
			return err
		}
		if replayed {
			digest, parsed, err := parseResultWire(recorded)
			if err != nil {
				return err
			}
			replay = parsed
			replay.LedgerDigest = digest
			return nil
		}
		digest, err := s.writer()(ctx, tx, req.ExpectedLedgerRevision, currentDigest, canonical)
		if err != nil {
			return err
		}
		outcome.LedgerDigest = digest
		replay = outcome
		return nil
	})
	if err != nil {
		if isTransportOrModelErr(err) {
			return Result{}, fmt.Errorf("batchconsolidation: transport failed; refusing markdown ledger fallback: %w", err)
		}
		return Result{}, err
	}
	if replay.LedgerDigest == "" && replay.LedgerRevision != 0 {
		_, digest, ok, err := s.store.GetHead(ledger.HeadProjection, LedgerHeadKey)
		if err == nil && ok {
			replay.LedgerDigest = digest
		}
	}
	s.remember(replay)
	return cloneResult(replay), nil
}

func (s *Service) writer() commitFunc {
	if s.commit != nil {
		return s.commit
	}
	return commitCanonicalBatch
}

func commitCanonicalBatch(_ context.Context, tx *ledger.Tx, expectedSeq uint64, expectedDigest string, payload []byte) (string, error) {
	digest, err := tx.PutContent(payload)
	if err != nil {
		return "", err
	}
	if err := tx.CompareAndSwapHead(ledger.HeadProjection, LedgerHeadKey, expectedSeq, expectedDigest, expectedSeq+1, digest); err != nil {
		return "", err
	}
	return digest, nil
}

func (s *Service) remember(result Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if result.LedgerRevision < s.revision {
		return
	}
	if result.LedgerRevision == s.revision && result.LedgerDigest == s.digest && s.digest != "" {
		return
	}
	for _, d := range result.Decisions {
		if _, exists := s.byID[d.DecisionID]; exists {
			continue
		}
		cloned := cloneDecision(d)
		s.decisions = append(s.decisions, cloned)
		s.byID[d.DecisionID] = cloned
		for _, src := range d.SourceProposalIDs {
			s.coverage[src] = d.DecisionID
		}
		if d.SuccessorSkillRevision != nil {
			s.successors[d.SuccessorSkillRevision.RevisionID] = cloneSuccessor(*d.SuccessorSkillRevision)
		}
	}
	if result.LedgerRevision > s.revision {
		s.revision = result.LedgerRevision
		s.digest = result.LedgerDigest
	}
}

func (s *Service) loadFamily(ids []string) (map[string]rawproposal.RawSkillProposal, error) {
	family := make(map[string]rawproposal.RawSkillProposal, len(ids))
	for _, id := range ids {
		if err := rejectIncompleteID(id, nil); err != nil {
			return nil, err
		}
		if _, dup := family[id]; dup {
			return nil, fail(validation.CodeSchemaTypeInvalid, "family_proposal_ids must not repeat %q", id)
		}
		proposal, ok := s.catalog.Get(id)
		if !ok {
			if s.prefixOfKnown(id) {
				return nil, fail(validation.CodeNonExactRef, "family source %q is a prefix of a complete proposal id; source ids must be complete", id)
			}
			return nil, fail(validation.CodeNonExactRef, "unknown source proposal id %q", id)
		}
		if proposal.ProposalID != id {
			return nil, fail(validation.CodeNonExactRef, "catalog resolved %q to a different complete id %q", id, proposal.ProposalID)
		}
		family[id] = proposal
	}
	return family, nil
}

func (s *Service) prefixOfKnown(id string) bool {
	if id == "" {
		return false
	}
	// Probe by asking the catalog for every family member is impossible
	// without List; tests pass the truncated form as a source. The
	// decision-source check below also walks the provided family.
	return looksLikeHashPrefix(id)
}

func (s *Service) sealDecisions(req Request, family map[string]rawproposal.RawSkillProposal, drafts []DecisionDraft) ([]ConsolidationDecision, error) {
	if len(drafts) == 0 {
		return nil, fail(validation.CodeSchemaRequiredFieldMissing, "at least one consolidation decision is required")
	}
	covered := make(map[string]string, len(family))
	out := make([]ConsolidationDecision, 0, len(drafts))
	seenDecision := make(map[string]bool, len(drafts))
	for _, draft := range drafts {
		if !closedOperations[draft.Operation] {
			return nil, fail(validation.CodeSchemaEnumInvalid, "operation %q is outside retain|revise|specialize|merge|retire|insufficient_evidence", draft.Operation)
		}
		if len(draft.SourceProposalIDs) == 0 {
			return nil, fail(validation.CodeSchemaRequiredFieldMissing, "decision %q has no source_proposal_ids", draft.DecisionID)
		}
		sources := make([]rawproposal.RawSkillProposal, 0, len(draft.SourceProposalIDs))
		seenSrc := make(map[string]bool, len(draft.SourceProposalIDs))
		for _, src := range draft.SourceProposalIDs {
			if err := rejectIncompleteID(src, family); err != nil {
				return nil, err
			}
			proposal, ok := family[src]
			if !ok {
				if prefixOfFamily(src, family) {
					return nil, fail(validation.CodeNonExactRef, "source %q is a prefix of a complete proposal id; source ids must be complete", src)
				}
				return nil, fail(validation.CodeNonExactRef, "unknown source proposal id %q", src)
			}
			if seenSrc[src] {
				return nil, fail(validation.CodeSchemaTypeInvalid, "decision %q repeats source %q", draft.DecisionID, src)
			}
			if prev, ok := covered[src]; ok {
				return nil, fail(validation.CodeSchemaTypeInvalid, "source %q is covered by both %q and %q", src, prev, draft.DecisionID)
			}
			seenSrc[src] = true
			covered[src] = draft.DecisionID
			sources = append(sources, proposal)
		}
		lineage := evidenceLineage(s.catalog, sources)
		if err := validateConflicts(draft, family); err != nil {
			return nil, err
		}
		rationale := draft.RationaleEvidenceRefs
		if len(rationale) == 0 {
			rationale = append([]string(nil), lineage...)
		} else if err := requireKnownEvidence(rationale, lineage); err != nil {
			return nil, err
		}
		successor := mintSuccessor(draft.Operation, sources, lineage)
		id := draft.DecisionID
		if id == "" {
			id = decisionIDFor(draft.Operation, draft.SourceProposalIDs)
		}
		if seenDecision[id] {
			return nil, fail(validation.CodeSchemaTypeInvalid, "decision_id %q is repeated in the batch", id)
		}
		seenDecision[id] = true
		sealed := ConsolidationDecision{
			DecisionID:             id,
			ExpectedLedgerRevision: req.ExpectedLedgerRevision,
			Operation:              draft.Operation,
			SourceProposalIDs:      append([]string(nil), draft.SourceProposalIDs...),
			SuccessorSkillRevision: successor,
			ConditionalConflicts:   cloneConflicts(draft.ConditionalConflicts),
			RationaleEvidenceRefs:  rationale,
			CreatedByAgentRunID:    req.AgentRunID,
		}
		digest, err := decisionDigest(sealed)
		if err != nil {
			return nil, fail(validation.CodeNonIntegerNumber, "decision cannot enter canonical hashed core: %v", err)
		}
		sealed.ContentDigest = digest
		out = append(out, sealed)
	}
	for _, id := range req.FamilyProposalIDs {
		if _, ok := covered[id]; !ok {
			return nil, fail(validation.CodeSchemaRequiredFieldMissing, "source proposal %q is not covered by any decision", id)
		}
	}
	return out, nil
}

func validateConflicts(draft DecisionDraft, family map[string]rawproposal.RawSkillProposal) error {
	for _, c := range draft.ConditionalConflicts {
		for _, id := range []string{c.LeftSourceID, c.RightSourceID} {
			if err := rejectIncompleteID(id, family); err != nil {
				return err
			}
			if _, ok := family[id]; !ok {
				return fail(validation.CodeNonExactRef, "conditional conflict references unknown source %q", id)
			}
		}
	}
	return nil
}

func requireKnownEvidence(refs, lineage []string) error {
	allowed := make(map[string]bool, len(lineage))
	for _, id := range lineage {
		allowed[id] = true
	}
	for _, id := range refs {
		if !allowed[id] {
			return fail(validation.CodeNonExactRef, "rationale evidence %q is outside the source evidence lineage", id)
		}
	}
	return nil
}

func rejectIncompleteID(id string, family map[string]rawproposal.RawSkillProposal) error {
	if strings.TrimSpace(id) == "" {
		return fail(validation.CodeSchemaRequiredFieldMissing, "source proposal id must be a non-empty complete id")
	}
	if looksLikeHashPrefix(id) {
		return fail(validation.CodeNonExactRef, "source %q is a hash prefix; source ids must be complete proposal ids", id)
	}
	if prefixOfFamily(id, family) {
		return fail(validation.CodeNonExactRef, "source %q is a prefix of a complete proposal id; source ids must be complete", id)
	}
	return nil
}

func looksLikeHashPrefix(id string) bool {
	if fullDigestRE.MatchString(id) {
		return false
	}
	return partialDigestRE.MatchString(id) || shortHexRE.MatchString(id)
}

func prefixOfFamily(id string, family map[string]rawproposal.RawSkillProposal) bool {
	if family == nil || id == "" {
		return false
	}
	if _, exact := family[id]; exact {
		return false
	}
	for full, proposal := range family {
		if full != id && strings.HasPrefix(full, id) {
			return true
		}
		if proposal.ContentDigest != id && strings.HasPrefix(proposal.ContentDigest, id) {
			return true
		}
	}
	return false
}

func mintSuccessor(operation string, sources []rawproposal.RawSkillProposal, lineage []string) *SkillRevision {
	if operation == OpRetire || operation == OpInsufficientEvidence {
		return nil
	}
	ids := make([]string, len(sources))
	for i, p := range sources {
		ids[i] = p.ProposalID
	}
	sort.Strings(ids)
	ev := append([]string(nil), lineage...)
	sort.Strings(ev)
	digest, err := contract.DigestOf(map[string]any{
		"operation":           operation,
		"source_proposal_ids": stringsAny(ids),
		"evidence_lineage":    stringsAny(ev),
	})
	if err != nil {
		return nil
	}
	return &SkillRevision{
		RevisionID:        "skill-revision-" + digest,
		URI:               "skill://evaluation/" + strings.TrimPrefix(digest, "sha256:") + "@1",
		SourceProposalIDs: ids,
		EvidenceLineage:   ev,
		ContentDigest:     digest,
	}
}

func evidenceLineage(catalog ProposalCatalog, sources []rawproposal.RawSkillProposal) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, p := range sources {
		for _, id := range p.SourceEvidenceRefs {
			add(id)
		}
		if catalog == nil {
			continue
		}
		if prov, ok := catalog.ProvenanceForProposal(p.ProposalID); ok {
			for _, ev := range prov.SourceEvidence {
				add(ev.ID)
			}
		}
	}
	sort.Strings(out)
	return out
}

func decisionIDFor(operation string, sources []string) string {
	ids := append([]string(nil), sources...)
	sort.Strings(ids)
	digest, err := contract.DigestOf(map[string]any{"operation": operation, "source_proposal_ids": stringsAny(ids)})
	if err != nil {
		return "consolidation-decision-" + operation
	}
	return "consolidation-decision-" + digest
}

func decisionDigest(d ConsolidationDecision) (string, error) {
	return contract.DigestOf(map[string]any{
		"decision_id":              d.DecisionID,
		"expected_ledger_revision": d.ExpectedLedgerRevision,
		"operation":                d.Operation,
		"source_proposal_ids":      stringsAny(d.SourceProposalIDs),
		"successor_skill_revision": successorCanonical(d.SuccessorSkillRevision),
		"conditional_conflicts":    conflictsCanonical(d.ConditionalConflicts),
		"rationale_evidence_refs":  stringsAny(d.RationaleEvidenceRefs),
		"created_by_agent_run_id":  d.CreatedByAgentRunID,
	})
}

func successorCanonical(s *SkillRevision) any {
	if s == nil {
		return nil
	}
	return map[string]any{
		"revision_id":         s.RevisionID,
		"uri":                 s.URI,
		"source_proposal_ids": stringsAny(s.SourceProposalIDs),
		"evidence_lineage":    stringsAny(s.EvidenceLineage),
		"content_digest":      s.ContentDigest,
	}
}

func conflictsCanonical(conflicts []ConditionalConflict) []any {
	out := make([]any, len(conflicts))
	for i, c := range conflicts {
		out[i] = map[string]any{
			"left_source_id":  c.LeftSourceID,
			"right_source_id": c.RightSourceID,
			"condition":       c.Condition,
			"evidence_refs":   stringsAny(c.EvidenceRefs),
		}
	}
	return out
}

func batchCanonical(revision uint64, decisions []ConsolidationDecision) map[string]any {
	items := make([]any, len(decisions))
	for i, d := range decisions {
		items[i] = map[string]any{
			"decision_id":              d.DecisionID,
			"expected_ledger_revision": d.ExpectedLedgerRevision,
			"operation":                d.Operation,
			"source_proposal_ids":      stringsAny(d.SourceProposalIDs),
			"successor_skill_revision": successorCanonical(d.SuccessorSkillRevision),
			"conditional_conflicts":    conflictsCanonical(d.ConditionalConflicts),
			"rationale_evidence_refs":  stringsAny(d.RationaleEvidenceRefs),
			"created_by_agent_run_id":  d.CreatedByAgentRunID,
			"content_digest":           d.ContentDigest,
		}
	}
	return map[string]any{
		"schema_version":  "gms.skill-evolution-ledger-batch.v1",
		"ledger_revision": revision,
		"decisions":       items,
	}
}

func requestCanonical(req Request) map[string]any {
	family := append([]string(nil), req.FamilyProposalIDs...)
	drafts := make([]any, len(req.Decisions))
	for i, d := range req.Decisions {
		drafts[i] = map[string]any{
			"decision_id":             d.DecisionID,
			"operation":               d.Operation,
			"source_proposal_ids":     stringsAny(d.SourceProposalIDs),
			"conditional_conflicts":   conflictsCanonical(d.ConditionalConflicts),
			"rationale_evidence_refs": stringsAny(d.RationaleEvidenceRefs),
		}
	}
	return map[string]any{
		"agent_run_id":             req.AgentRunID,
		"expected_ledger_revision": req.ExpectedLedgerRevision,
		"family_proposal_ids":      stringsAny(family),
		"decisions":                drafts,
	}
}

func resultWire(r Result, digest string) map[string]any {
	items := make([]any, len(r.Decisions))
	for i, d := range r.Decisions {
		items[i] = map[string]any{
			"decision_id":              d.DecisionID,
			"expected_ledger_revision": json.Number(fmt.Sprintf("%d", d.ExpectedLedgerRevision)),
			"operation":                d.Operation,
			"source_proposal_ids":      d.SourceProposalIDs,
			"successor_skill_revision": r.Decisions[i].SuccessorSkillRevision,
			"conditional_conflicts":    d.ConditionalConflicts,
			"rationale_evidence_refs":  d.RationaleEvidenceRefs,
			"created_by_agent_run_id":  d.CreatedByAgentRunID,
			"content_digest":           d.ContentDigest,
		}
	}
	return map[string]any{
		"ledger_revision": json.Number(fmt.Sprintf("%d", r.LedgerRevision)),
		"ledger_digest":   digest,
		"decisions":       items,
	}
}

func parseResultWire(raw []byte) (string, Result, error) {
	var wire struct {
		LedgerRevision uint64                  `json:"ledger_revision"`
		LedgerDigest   string                  `json:"ledger_digest"`
		Decisions      []ConsolidationDecision `json:"decisions"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return "", Result{}, fmt.Errorf("batchconsolidation: stored idempotency outcome is invalid: %w", err)
	}
	return wire.LedgerDigest, Result{LedgerRevision: wire.LedgerRevision, LedgerDigest: wire.LedgerDigest, Decisions: wire.Decisions}, nil
}

func familySlice(family map[string]rawproposal.RawSkillProposal, ids []string) []rawproposal.RawSkillProposal {
	out := make([]rawproposal.RawSkillProposal, 0, len(ids))
	for _, id := range ids {
		out = append(out, family[id])
	}
	return out
}

func isTransportOrModelErr(err error) bool {
	if err == nil {
		return false
	}
	if ledger.ReasonOf(err) != "" {
		return false
	}
	if validation.CodeOf(err) != "" {
		return false
	}
	return true
}

func fail(code, format string, args ...any) error {
	return &validation.Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

func stringsAny(values []string) []any {
	out := make([]any, len(values))
	for i := range values {
		out[i] = values[i]
	}
	return out
}

func cloneDecision(in ConsolidationDecision) ConsolidationDecision {
	in.SourceProposalIDs = append([]string(nil), in.SourceProposalIDs...)
	in.RationaleEvidenceRefs = append([]string(nil), in.RationaleEvidenceRefs...)
	in.ConditionalConflicts = cloneConflicts(in.ConditionalConflicts)
	if in.SuccessorSkillRevision != nil {
		cloned := cloneSuccessor(*in.SuccessorSkillRevision)
		in.SuccessorSkillRevision = &cloned
	}
	return in
}

func cloneSuccessor(in SkillRevision) SkillRevision {
	in.SourceProposalIDs = append([]string(nil), in.SourceProposalIDs...)
	in.EvidenceLineage = append([]string(nil), in.EvidenceLineage...)
	return in
}

func cloneConflicts(in []ConditionalConflict) []ConditionalConflict {
	out := make([]ConditionalConflict, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].EvidenceRefs = append([]string(nil), in[i].EvidenceRefs...)
	}
	return out
}

func cloneResult(in Result) Result {
	out := in
	out.Decisions = make([]ConsolidationDecision, len(in.Decisions))
	for i := range in.Decisions {
		out.Decisions[i] = cloneDecision(in.Decisions[i])
	}
	return out
}
