package merge

import (
	"context"
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluator"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Config wires the merge lifecycle service.
type Config struct {
	Gates    *validation.Gates
	Store    ledger.Store
	Tx       ledger.TxManager
	Registry ledger.ReasonRegistry
	// StateDir is the authority state-machine directory (contains
	// merge-lifecycle.schema.json).
	StateDir string
	// Similarity is the MT1 protected assessor (assessment resolution).
	Similarity *similarity.Service
	// Evidence resolves committed §7.7 evidence refs (the GMS-201 query
	// port) for proposal evidence closure.
	Evidence similarity.EvidenceResolver
	// Artifacts is the GMS-202 canonical artifact gate.
	Artifacts *artifact.Service
	// Candidates is the GMS-202 binding service (merge closure/provenance
	// gates only; the merge path mints its own candidates).
	Candidates *candidate.BindingService
	// Replay is the GMS-203 canonicalizer.
	Replay *replay.Service
	// Evaluator is the GMS-203 U1 evaluator.
	Evaluator *evaluator.Evaluator
	// Heads resolves the current active revision of a lineage (the GMS-204
	// read seam).
	Heads similarity.ActiveHeadResolver
}

// Service is the protected merge lifecycle engine.
type Service struct {
	gates      *validation.Gates
	store      ledger.Store
	mgr        ledger.TxManager
	registry   ledger.ReasonRegistry
	engine     *eventEngine
	similarity *similarity.Service
	evidence   similarity.EvidenceResolver
	artifacts  *artifact.Service
	candidates *candidate.BindingService
	replay     *replay.Service
	evaluator  *evaluator.Evaluator
	heads      similarity.ActiveHeadResolver
}

// NewService fails closed unless every dependency is wired, the §9.4
// authority machine loads, and every reason code this package can emit
// exists in the digest-verified registry (Contract §13.7.1 R5).
func NewService(cfg Config) (*Service, error) {
	if cfg.Gates == nil || cfg.Store == nil || cfg.Tx == nil || cfg.Registry == nil ||
		cfg.StateDir == "" || cfg.Similarity == nil || cfg.Evidence == nil || cfg.Artifacts == nil ||
		cfg.Candidates == nil || cfg.Replay == nil || cfg.Evaluator == nil || cfg.Heads == nil {
		return nil, errors.New("merge: nil dependency in config")
	}
	for _, code := range []string{
		ReasonMergeStateConflict, ReasonMergeGroupInFlight, ReasonMergeSourceHeadStale,
		ReasonActiveHeadConflict, ReasonSimilarityBelowThreshold, ReasonSimilarityAssessmentInvalid,
		ReasonMergeSourceInvalid, ReasonMergeKindUnsupported, ReasonMergeSourceNotActive,
		ReasonMergeProposalDuplicate, ReasonMergeBranchRegression, ReasonMergeEvidenceIncomplete,
		ReasonMergeBlockingConflict, ReasonEvidenceNotCommitted, ReasonSkillKindInvalid,
		ReasonSourceHeadStale, ReasonCriticalRegression, ReasonMissingFamily,
		ReasonCandidateNotBound, ReasonCandidateReleasedBodyMismatch, ReasonReleaseNotAccepted,
		ReasonReleaseDecisionInvalid, ReasonMergeWithdrawalForbidden,
		ReasonRefMismatch, ReasonDigestMismatch, ledger.ReasonIdempotencyConflict,
		ledger.ReasonLineageVersionConflict, ledger.ReasonActivationSequenceConflict,
	} {
		if err := cfg.Registry.Verify(code); err != nil {
			return nil, fmt.Errorf("merge: %w", err)
		}
	}
	engine, err := newEventEngine(cfg.Gates, cfg.StateDir, cfg.Store)
	if err != nil {
		return nil, err
	}
	return &Service{
		gates:      cfg.Gates,
		store:      cfg.Store,
		mgr:        cfg.Tx,
		registry:   cfg.Registry,
		engine:     engine,
		similarity: cfg.Similarity,
		evidence:   cfg.Evidence,
		artifacts:  cfg.Artifacts,
		candidates: cfg.Candidates,
		replay:     cfg.Replay,
		evaluator:  cfg.Evaluator,
		heads:      cfg.Heads,
	}, nil
}

// Gates exposes the shared validation gates (read-only use).
func (s *Service) Gates() *validation.Gates { return s.gates }

// activeHeads resolves the current active heads of both sources (canonical
// order preserved; a missing head yields nil for that slot).
func (s *Service) activeHeads(ctx context.Context, pair [2]contract.SkillArtifactRef) ([2]*contract.SkillArtifactRef, error) {
	var out [2]*contract.SkillArtifactRef
	for i, source := range pair {
		active, err := s.heads.ActiveRevision(ctx, source.LineageID)
		if err != nil {
			return out, err
		}
		out[i] = active
	}
	return out, nil
}

// requireFreshSources fails with MERGE_SOURCE_NOT_ACTIVE unless both pinned
// sources are STILL the current active heads of their lineages (the
// active-only invariant re-checked at every lifecycle step that freezes or
// consumes source heads, M2/M5).
func (s *Service) requireFreshSources(ctx context.Context, pair [2]contract.SkillArtifactRef) error {
	for _, source := range pair {
		active, err := s.heads.ActiveRevision(ctx, source.LineageID)
		if err != nil {
			return err
		}
		if active == nil {
			return newError(ReasonMergeSourceNotActive,
				"source %s has no active revision any more; the merge is stale", source.LineageID)
		}
		if *active != source {
			return newError(ReasonMergeSourceNotActive,
				"source %s moved to %s v%s; the frozen source head is stale (no partial effects are possible)",
				source.LineageID, active.LineageID, active.Version)
		}
	}
	return nil
}

// sourceExpectationRecord renders the gms.merge-source-expectation.v1
// record pinned by a HeadMergeSource CAS at admission.
func sourceExpectationRecord(prop *Proposal, source contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":     SchemaMergeSourceExpectation,
		"proposal_ref":       versionedRefDoc(prop.Ref()),
		"source_pair_key":    prop.PairKey(),
		"proposal_group_key": prop.GroupKey(),
		"expected_head":      similarity.SkillRefDoc(source),
	}
}

// mergeSourceHeadKey is the HeadMergeSource key of one source lineage under
// one proposal group: lineage and group both bind, so two different groups
// over the same lineage pin different keys while a retry of the same group
// addresses the same key.
func mergeSourceHeadKey(lineageID, groupKey string) string {
	return lineageID + "\x1f" + groupKey
}

// resolveSourceExpectation reads one pinned source expectation back.
func (s *Service) resolveSourceExpectation(lineageID, groupKey string) (map[string]any, []byte, bool, error) {
	_, digest, ok, err := s.store.GetHead(ledger.HeadMergeSource, mergeSourceHeadKey(lineageID, groupKey))
	if err != nil || !ok || digest == "" {
		return nil, nil, false, err
	}
	payload, found, err := s.store.Get(digest)
	if err != nil || !found {
		return nil, nil, false, err
	}
	value, err := contract.ParseJSONStrict(payload)
	if err != nil {
		return nil, nil, false, err
	}
	obj, _ := contract.AsObject(value)
	return obj, payload, true, nil
}
