// Package candidate implements the GMS-202 immutable candidate binding
// (GMS §4.3, Contract §9.1 candidate_bound): the one-shot transaction that
// mints a Contract §7.4 CandidateArtifactRef only when every evidence ref
// resolves committed and every static gate is green, writes the canonical
// body, the CandidateRef, the proposal candidate_bound event and the
// idempotency record atomically over the GMS-102 ledger, and hands out an
// immutable read-only view that can never enter Runtime execution.
//
// Model suggestions cannot bind directly: Bind accepts only a strict
// §7.9 SkillProposal (PROPOSAL_INVALID otherwise) — suggestions must go
// through the proposal state machine and these static gates.
package candidate

import (
	"context"
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/evidence"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Closed reason codes of the binding gate (frozen system registry).
const (
	ReasonProposalInvalid          = "PROPOSAL_INVALID"
	ReasonProposalStateConflict    = "PROPOSAL_STATE_CONFLICT"
	ReasonSkillKindInvalid         = "SKILL_KIND_INVALID"
	ReasonEvidenceNotCommitted     = "EVIDENCE_NOT_COMMITTED"
	ReasonCandidateImmutable       = "CANDIDATE_IMMUTABLE"
	ReasonCandidateNotExecutable   = "CANDIDATE_NOT_EXECUTABLE"
	ReasonMergeEvidenceIncomplete  = "MERGE_EVIDENCE_INCOMPLETE"
	ReasonMergeBlockingConflict    = "MERGE_BLOCKING_CONFLICT"
	ReasonSimilarityBelowThreshold = "SIMILARITY_BELOW_THRESHOLD"
)

// Error is the registry-code gate failure; Detail is diagnostic only.
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "candidate: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err ("" when nil); artifact,
// proposal and validation layer codes surface unchanged.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if ce, ok := err.(*Error); ok {
		return ce.ReasonCode
	}
	if code := artifact.CodeOf(err); code != "" {
		return code
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	if code := ledger.ReasonOf(err); code != "" {
		return code
	}
	return ""
}

// EvidenceResolver resolves one evidence id to its committed §7.7 ref. The
// GMS-201 AdmissionService satisfies the source interface directly
// (AdaptEvidence below); staged evidence is structurally absent — it can
// never resolve.
type EvidenceResolver interface {
	GetEvidence(evidenceID string) (contract.EvidenceRef, bool, error)
}

// admissionEvidence is the GMS-201 query interface (evidence.Service).
type admissionEvidence interface {
	GetEvidence(evidenceID string) (evidence.CommittedEvidence, bool, error)
}

// AdaptEvidence adapts the GMS-201 committed-evidence query interface to
// the resolver port.
func AdaptEvidence(svc admissionEvidence) EvidenceResolver {
	return &admissionAdapter{svc: svc}
}

type admissionAdapter struct{ svc admissionEvidence }

func (a *admissionAdapter) GetEvidence(id string) (contract.EvidenceRef, bool, error) {
	committed, ok, err := a.svc.GetEvidence(id)
	if err != nil || !ok {
		return contract.EvidenceRef{}, ok, err
	}
	return contract.EvidenceRef{
		SchemaVersion:  contract.SchemaEvidenceRef,
		EvidenceID:     committed.EvidenceID,
		Version:        committed.Version,
		EvidenceDigest: committed.EvidenceDigest,
		CommitState:    committed.CommitState,
		EvidenceKind:   committed.EvidenceKind,
	}, true, nil
}

// Config wires the binding service.
type Config struct {
	Store     ledger.Store
	Tx        ledger.TxManager
	Registry  ledger.ReasonRegistry
	Artifacts *artifact.Service
	Proposals *proposal.Service
	Evidence  EvidenceResolver
}

// BindingService is the protected candidate committer.
type BindingService struct {
	store     ledger.Store
	mgr       ledger.TxManager
	registry  ledger.ReasonRegistry
	artifacts *artifact.Service
	proposals *proposal.Service
	evidence  EvidenceResolver
}

// NewBindingService fails closed unless every mapped reason code exists in
// the digest-verified registry (Contract §13.7.1 R5).
func NewBindingService(cfg Config) (*BindingService, error) {
	if cfg.Store == nil || cfg.Tx == nil || cfg.Registry == nil ||
		cfg.Artifacts == nil || cfg.Proposals == nil || cfg.Evidence == nil {
		return nil, fmt.Errorf("candidate: nil dependency in binding config")
	}
	for _, code := range []string{
		ReasonProposalInvalid, ReasonProposalStateConflict, ReasonSkillKindInvalid,
		ReasonEvidenceNotCommitted, ReasonCandidateImmutable, ReasonCandidateNotExecutable,
		ReasonMergeEvidenceIncomplete, ReasonMergeBlockingConflict,
		ReasonSimilarityBelowThreshold, validation.CodePermissionCapExceeded,
		ledger.ReasonIdempotencyConflict, ledger.ReasonDigestMismatch,
	} {
		if err := cfg.Registry.Verify(code); err != nil {
			return nil, fmt.Errorf("candidate: %w", err)
		}
	}
	return &BindingService{
		store:     cfg.Store,
		mgr:       cfg.Tx,
		registry:  cfg.Registry,
		artifacts: cfg.Artifacts,
		proposals: cfg.Proposals,
		evidence:  cfg.Evidence,
	}, nil
}

// Proposals exposes the wired proposal state engine (read paths and
// transition staging shared with the binding transaction).
func (s *BindingService) Proposals() *proposal.Service { return s.proposals }

// BindRequest is the one-shot binding input: a strict §7.9 proposal
// document and the suggested canonical artifact envelope. CandidateID is
// optional (derived from origin+body when empty).
type BindRequest struct {
	ProposalDoc any
	ArtifactDoc any
	CandidateID string
}

// Bind runs the protected canonicalizer and candidate-binding transaction
// (GMS §4.3). Fail-closed order:
//
//  1. proposal parses strictly (PROPOSAL_INVALID — model suggestions and
//     any non-proposal envelope cannot bind directly);
//  2. the artifact passes every static gate of the three-kind canonical
//     gate (schema/extension/digest/permission/kind/composite codes);
//  3. proposal kind == artifact kind (SKILL_KIND_INVALID — kind drift);
//  4. every evidence ref (proposal + branch provenance) resolves committed
//     with the exact (id, version, digest) triple (EVIDENCE_NOT_COMMITTED);
//  5. the proposal sits in admitted (PROPOSAL_STATE_CONFLICT);
//  6. an already-committed candidate identity replays idempotently or
//     rejects CANDIDATE_IMMUTABLE on any field change;
//  7. ONE transaction commits canonical body + CandidateRef + proposal
//     candidate_bound event + idempotency record, or nothing.
func (s *BindingService) Bind(ctx context.Context, req BindRequest) (*CandidateView, error) {
	proposalDoc, err := s.proposals.ParseProposal(req.ProposalDoc)
	if err != nil {
		return nil, newError(ReasonProposalInvalid, "proposal does not parse as a strict gms.skill-proposal.v1 document (model suggestions cannot bind directly): %v", err)
	}
	canonical, err := s.artifacts.Canonicalize(req.ArtifactDoc)
	if err != nil {
		return nil, err
	}
	if canonical.Kind() != proposalDoc.Kind() {
		return nil, newError(ReasonSkillKindInvalid,
			"proposal kind %q != artifact kind %q (kind drift; a kind change needs a new lineage via derive_lineage)", proposalDoc.Kind(), canonical.Kind())
	}
	if err := s.checkEvidenceClosure(proposalDoc, canonical); err != nil {
		return nil, err
	}

	candidateID := req.CandidateID
	if candidateID == "" {
		candidateID = deriveCandidateID(proposalDoc.Ref(), canonical.BodyDigest())
	}

	// Existing identity: idempotent replay or immutable rejection.
	if existing, found, err := s.findCandidate(candidateID); err != nil {
		return nil, err
	} else if found {
		if existing.BodyDigest == canonical.BodyDigest() {
			return s.viewFromRef(existing)
		}
		return nil, newError(ReasonCandidateImmutable,
			"candidate %s is bound to body %s; any field change is rejected (a new body needs a new candidate attempt)", candidateID, existing.BodyDigest)
	}

	state, _, err := s.proposals.CurrentState(ctx, proposalDoc.ID())
	if err != nil {
		return nil, err
	}
	if state != "admitted" {
		return nil, newError(ReasonProposalStateConflict,
			"candidate binding requires the proposal in admitted, current state is %q", state)
	}

	ref := contract.CandidateArtifactRef{
		SchemaVersion: contract.SchemaCandidateArtifactRef,
		CandidateID:   candidateID,
		Kind:          canonical.Kind(),
		BodyDigest:    canonical.BodyDigest(),
		OriginType:    "skill_proposal",
		OriginRef:     proposalDoc.Ref(),
	}
	return s.commitCandidate(ctx, ref, canonical, proposalDoc, "admitted", "candidate_bound")
}

// commitCandidate runs the single atomic write set.
func (s *BindingService) commitCandidate(ctx context.Context, ref contract.CandidateArtifactRef, canonical *artifact.CanonicalArtifact, proposalDoc *proposal.Proposal, from, to string) (*CandidateView, error) {
	payload := candidateRefJSON(ref)
	requestDigest, err := contract.DigestOf(map[string]any{
		"candidate_id": ref.CandidateID,
		"body_digest":  ref.BodyDigest,
		"origin_ref": map[string]any{
			"id": ref.OriginRef.ID, "version": json.Number(ref.OriginRef.Version), "digest": ref.OriginRef.Digest,
		},
	})
	if err != nil {
		return nil, newError(ledger.ReasonDigestMismatch, "binding request digest: %v", err)
	}
	var view *CandidateView
	err = s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		replayed, recorded, err := tx.RecordIdempotency(idempotencyKey(ref.CandidateID), requestDigest, marshalJSON(payload))
		if err != nil {
			return err
		}
		if replayed {
			value, err := contract.ParseJSONStrict(recorded)
			if err != nil {
				return newError(ledger.ReasonDigestMismatch, "recorded binding outcome invalid: %v", err)
			}
			obj, _ := contract.AsObject(value)
			parsed, err := contract.ParseCandidateArtifactRef(obj)
			if err != nil {
				return newError(ledger.ReasonDigestMismatch, "recorded binding outcome invalid: %v", err)
			}
			view, err = s.viewFromRef(parsed)
			return err
		}
		if _, err := tx.PutContent(canonical.Canonical()); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ledger.LedgerCandidate, "", ref.CandidateID, marshalJSON(payload)); err != nil {
			return err
		}
		recordRefs := []contract.VersionedRef{{ID: ref.CandidateID, Version: "1", Digest: ref.BodyDigest}}
		if _, err := s.proposals.AppendTransitionTx(tx, proposalDoc, from, to, recordRefs); err != nil {
			return err
		}
		view = &CandidateView{ref: ref, body: canonical.Canonical()}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return view, nil
}

// checkEvidenceClosure resolves every evidence ref of the proposal and of
// the artifact's branch provenance against the committed-evidence query
// port; the exact (id, version, digest) triple must match and the state
// must be committed|sealed (EVIDENCE_NOT_COMMITTED otherwise — staged
// evidence can never support a candidate).
func (s *BindingService) checkEvidenceClosure(proposalDoc *proposal.Proposal, canonical *artifact.CanonicalArtifact) error {
	refs := proposalDoc.EvidenceRefs()
	if bodyRefs, err := canonical.EvidenceRefs(); err != nil {
		return newError(ReasonEvidenceNotCommitted, "artifact branch evidence refs invalid: %v", err)
	} else {
		refs = append(refs, bodyRefs...)
	}
	for _, ref := range refs {
		record, found, err := s.evidence.GetEvidence(ref.EvidenceID)
		if err != nil {
			return err
		}
		if !found || record.Version != ref.Version || record.EvidenceDigest != ref.EvidenceDigest ||
			(record.CommitState != "committed" && record.CommitState != "sealed") {
			return newError(ReasonEvidenceNotCommitted,
				"evidence %s v%s does not resolve committed with digest %s (staged/uncommitted evidence cannot support a candidate)",
				ref.EvidenceID, ref.Version, ref.EvidenceDigest)
		}
	}
	return nil
}

// findCandidate resolves one committed candidate identity.
func (s *BindingService) findCandidate(candidateID string) (contract.CandidateArtifactRef, bool, error) {
	entries, err := s.store.Snapshot(ledger.LedgerCandidate, "")
	if err != nil {
		return contract.CandidateArtifactRef{}, false, err
	}
	for _, entry := range entries {
		if entry.EventID != candidateID {
			continue
		}
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return contract.CandidateArtifactRef{}, false, newError(ledger.ReasonDigestMismatch, "candidate %s payload unresolved", candidateID)
		}
		value, _ := contract.ParseJSONStrict(payload)
		obj, _ := contract.AsObject(value)
		ref, err := contract.ParseCandidateArtifactRef(obj)
		if err != nil {
			return contract.CandidateArtifactRef{}, false, newError(ledger.ReasonDigestMismatch, "candidate %s payload invalid: %v", candidateID, err)
		}
		return ref, true, nil
	}
	return contract.CandidateArtifactRef{}, false, nil
}

// viewFromRef rebuilds the immutable view of one committed candidate.
func (s *BindingService) viewFromRef(ref contract.CandidateArtifactRef) (*CandidateView, error) {
	body, ok, err := s.store.Get(ref.BodyDigest)
	if err != nil || !ok {
		return nil, newError(ledger.ReasonDigestMismatch, "candidate %s canonical body %s unresolved", ref.CandidateID, ref.BodyDigest)
	}
	if contract.DigestBytes(body) != ref.BodyDigest {
		return nil, newError(ledger.ReasonDigestMismatch, "candidate %s canonical body digest mismatch", ref.CandidateID)
	}
	return &CandidateView{ref: ref, body: append([]byte(nil), body...)}, nil
}

// CandidateView is the immutable read-only view of one bound candidate:
// identity only — no released lineage version, no Runtime execution input.
// Every accessor returns copies; no mutator exists by construction.
type CandidateView struct {
	ref  contract.CandidateArtifactRef
	body []byte
}

// Ref returns a copy of the exact §7.4 CandidateArtifactRef.
func (v *CandidateView) Ref() contract.CandidateArtifactRef { return v.ref }

// BodyDigest returns the frozen canonical body digest (candidate and
// released bodies digest equal, GMS §3.7).
func (v *CandidateView) BodyDigest() string { return v.ref.BodyDigest }

// CanonicalBody returns a copy of the frozen canonical JCS bytes.
func (v *CandidateView) CanonicalBody() []byte {
	out := make([]byte, len(v.body))
	copy(out, v.body)
	return out
}

// OriginType returns skill_proposal or merge_proposal.
func (v *CandidateView) OriginType() string { return v.ref.OriginType }

// OriginRef returns a copy of the origin exact ref.
func (v *CandidateView) OriginRef() contract.VersionedRef { return v.ref.OriginRef }

// RuntimeInput NEVER yields a Runtime execution input: candidates are not
// executable (Contract §7.4/§9.3.6; GMS §3.1). The method exists so the
// guarantee is testable and every caller fails closed with
// CANDIDATE_NOT_EXECUTABLE.
func (v *CandidateView) RuntimeInput() error {
	return newError(ReasonCandidateNotExecutable,
		"candidate %s is not executable Runtime input; only a released, activated SkillArtifactRef may execute", v.ref.CandidateID)
}

// --- helpers -----------------------------------------------------------------

func idempotencyKey(candidateID string) string {
	return "candidate-bind:" + candidateID
}

// deriveCandidateID mints the deterministic candidate identity from the
// origin exact ref and the canonical body digest: the same proposal + body
// always addresses the same candidate, any change addresses a new one.
func deriveCandidateID(origin contract.VersionedRef, bodyDigest string) string {
	digest, err := contract.DigestOf(map[string]any{
		"origin_ref":  map[string]any{"id": origin.ID, "version": json.Number(origin.Version), "digest": origin.Digest},
		"body_digest": bodyDigest,
	})
	if err != nil {
		// Decoder-model values with exact digests always canonicalize.
		panic(fmt.Sprintf("candidate: derive id: %v", err))
	}
	return "cand-" + digest[len("sha256:"):len("sha256:")+16]
}

func candidateRefJSON(ref contract.CandidateArtifactRef) map[string]any {
	return map[string]any{
		"schema_version": contract.SchemaCandidateArtifactRef,
		"candidate_id":   ref.CandidateID,
		"kind":           ref.Kind,
		"body_digest":    ref.BodyDigest,
		"origin_type":    ref.OriginType,
		"origin_ref": map[string]any{
			"id":      ref.OriginRef.ID,
			"version": json.Number(ref.OriginRef.Version),
			"digest":  ref.OriginRef.Digest,
		},
	}
}

func marshalJSON(value map[string]any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("candidate: payload marshal: %v", err))
	}
	return data
}

// NewCandidateView constructs an immutable view from an already-verified
// exact ref and canonical body. It is used by trusted direct Arm C adapters
// and test fixtures; callers still cannot obtain Runtime input from a view.
func NewCandidateView(ref contract.CandidateArtifactRef, canonicalBody []byte) (*CandidateView, error) {
	if ref.CandidateID == "" || ref.BodyDigest == "" || contract.DigestBytes(canonicalBody) != ref.BodyDigest {
		return nil, newError(ReasonCandidateImmutable, "exact candidate ref/body binding is invalid")
	}
	return &CandidateView{ref: ref, body: append([]byte(nil), canonicalBody...)}, nil
}
