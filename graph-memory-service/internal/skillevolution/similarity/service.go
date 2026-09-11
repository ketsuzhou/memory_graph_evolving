package similarity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// SchemaSimilarityAssessment is the authority schema of the §7.21 DTO.
const SchemaSimilarityAssessment = "similarity-assessment.schema.json"

// Closed reason codes of the MT1 gates (frozen system registry,
// gms-merge-similarity group; aliases reuse the single frozen source).
const (
	ReasonSimilarityBelowThreshold = contract.ReasonSimilarityBelowThreshold
	ReasonSkillKindInvalid         = "SKILL_KIND_INVALID"
	ReasonActiveHeadConflict       = ledger.ReasonActiveHeadConflict
	ReasonEvidenceNotCommitted     = contract.ReasonEvidenceNotCommitted
	ReasonRefMismatch              = contract.ReasonRefMismatch
	ReasonDigestMismatch           = contract.ReasonDigestMismatch
)

// Error is the registry-code gate failure; Detail is diagnostic only.
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "similarity: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err ("" when nil); ledger and
// validation layer codes surface unchanged.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if se, ok := err.(*Error); ok {
		return se.ReasonCode
	}
	if code := ledger.ReasonOf(err); code != "" {
		return code
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	var sce *contract.SchemaError
	if errors.As(err, &sce) {
		return sce.ReasonCode
	}
	return ""
}

// EvidenceResolver resolves one evidence id to its committed §7.7 ref (the
// GMS-201 query port; staged evidence is structurally absent).
type EvidenceResolver interface {
	GetEvidence(evidenceID string) (contract.EvidenceRef, bool, error)
}

// ActiveHeadResolver reads the current active revision of one lineage. The
// GMS-204 activation service satisfies this directly (it replays the
// authoritative activation ledger) — the GMS-208 merge packages consume it
// as the single active-head read seam.
type ActiveHeadResolver interface {
	ActiveRevision(ctx context.Context, lineageID string) (*contract.SkillArtifactRef, error)
}

// Config wires the similarity service.
type Config struct {
	Gates    *validation.Gates
	Store    ledger.Store
	Tx       ledger.TxManager
	Registry ledger.ReasonRegistry
	Evidence EvidenceResolver
	Heads    ActiveHeadResolver
	// Policy defaults to DefaultPolicyV1 when nil.
	Policy *Policy
}

// Service is the protected MT1 assessor and similarity-ledger writer.
type Service struct {
	gates    *validation.Gates
	store    ledger.Store
	mgr      ledger.TxManager
	registry ledger.ReasonRegistry
	evidence EvidenceResolver
	heads    ActiveHeadResolver
	policy   Policy
}

// NewService fails closed unless every mapped reason code exists in the
// digest-verified registry (Contract §13.7.1 R5).
func NewService(cfg Config) (*Service, error) {
	if cfg.Gates == nil || cfg.Store == nil || cfg.Tx == nil || cfg.Registry == nil ||
		cfg.Evidence == nil || cfg.Heads == nil {
		return nil, errors.New("similarity: nil dependency in config")
	}
	for _, code := range []string{
		ReasonSimilarityBelowThreshold, ReasonSkillKindInvalid,
		ReasonActiveHeadConflict, ReasonEvidenceNotCommitted,
		ReasonRefMismatch, ReasonDigestMismatch, ledger.ReasonIdempotencyConflict,
	} {
		if err := cfg.Registry.Verify(code); err != nil {
			return nil, fmt.Errorf("similarity: %w", err)
		}
	}
	policy := DefaultPolicyV1()
	if cfg.Policy != nil {
		policy = *cfg.Policy
	}
	return &Service{
		gates:    cfg.Gates,
		store:    cfg.Store,
		mgr:      cfg.Tx,
		registry: cfg.Registry,
		evidence: cfg.Evidence,
		heads:    cfg.Heads,
		policy:   policy,
	}, nil
}

// Policy exposes the frozen band policy.
func (s *Service) Policy() Policy { return s.policy }

// Assessment is the immutable normalized view of one Contract §7.21
// SimilarityAssessment. A+B and B+A normalize to byte-identical canonical
// documents, the same digest and the same pair key.
type Assessment struct {
	doc         map[string]any
	id          string
	version     string
	digest      string
	band        string
	scoreMicros int64
	sources     [2]contract.SkillArtifactRef
	pairKey     string
	policyRef   contract.VersionedRef
	canonical   []byte
}

// ID returns the assessment id.
func (a *Assessment) ID() string { return a.id }

// Version returns the exact assessment version digits.
func (a *Assessment) Version() string { return a.version }

// Digest returns the whole-document canonical digest (ledger identity).
func (a *Assessment) Digest() string { return a.digest }

// Band returns the declared/derived §7.21 band.
func (a *Assessment) Band() string { return a.band }

// ScoreMicros returns the integer micro-scaled score.
func (a *Assessment) ScoreMicros() int64 { return a.scoreMicros }

// Sources returns the canonical §6.3-ordered pair.
func (a *Assessment) Sources() [2]contract.SkillArtifactRef { return a.sources }

// PairKey returns the canonical source-pair key (dedup.source_pair_key
// semantics, GMS §7.2).
func (a *Assessment) PairKey() string { return a.pairKey }

// PolicyRef returns the versioned policy ref of the assessment.
func (a *Assessment) PolicyRef() contract.VersionedRef { return a.policyRef }

// Ref renders the assessment identity as a VersionedRef.
func (a *Assessment) Ref() contract.VersionedRef {
	return contract.VersionedRef{ID: a.id, Version: a.version, Digest: a.digest}
}

// Doc returns a deep copy of the normalized document.
func (a *Assessment) Doc() map[string]any { return deepCopyDoc(a.doc) }

// CanonicalBytes returns a copy of the canonical JCS bytes.
func (a *Assessment) CanonicalBytes() []byte {
	out := make([]byte, len(a.canonical))
	copy(out, a.canonical)
	return out
}

// Normalize strict-parses one §7.21 document and normalizes the source pair
// to canonical §6.3 order: A+B and B+A produce byte-identical canonical
// bytes, the same whole-document digest and the same pair key (MT1
// canonical-pair invariant). The declared assessment_digest is corpus data
// (ValidateShape); the ledger identity is the whole-document canonical
// digest.
func (s *Service) Normalize(doc any) (*Assessment, error) {
	obj, ok := contract.AsObject(doc)
	if !ok {
		return nil, newError(ReasonRefMismatch, "similarity assessment must be a JSON object")
	}
	if err := s.gates.ValidateShape(obj, SchemaSimilarityAssessment); err != nil {
		return nil, err
	}
	rawSources, _ := contract.AsArray(obj["source_skill_refs"])
	if len(rawSources) != 2 {
		return nil, newError(ReasonRefMismatch, "source_skill_refs must hold exactly 2 refs")
	}
	firstRaw, _ := contract.AsObject(rawSources[0])
	secondRaw, _ := contract.AsObject(rawSources[1])
	first, err := contract.ParseSkillArtifactRef(firstRaw)
	if err != nil {
		return nil, newError(ReasonRefMismatch, "source ref 0: %v", err)
	}
	second, err := contract.ParseSkillArtifactRef(secondRaw)
	if err != nil {
		return nil, newError(ReasonRefMismatch, "source ref 1: %v", err)
	}
	if first.LineageID == second.LineageID {
		return nil, newError(ReasonRefMismatch,
			"a symmetric pair needs two distinct lineages, got %q twice (self-merge is not a merge)", first.LineageID)
	}
	pair := CanonicalPair(first, second)
	normalized := deepCopyDoc(obj)
	normalized["source_skill_refs"] = []any{SkillRefDoc(pair[0]), SkillRefDoc(pair[1])}
	canonical, err := contract.JCS(contract.NormalizeForHashing(deepCopyDoc(normalized)))
	if err != nil {
		return nil, newError(validation.CodeNonIntegerNumber, "assessment cannot enter the hashed core: %v", err)
	}
	pairKey, err := SourcePairKey(pair)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "%v", err)
	}
	score, _ := intOf(normalized["score_micros"])
	policyRaw, _ := contract.AsObject(normalized["policy_ref"])
	policyRef, _ := contract.ParseVersionedRef(policyRaw)
	return &Assessment{
		doc:         normalized,
		id:          stringOf(normalized["assessment_id"]),
		version:     digitsOf(normalized["assessment_version"]),
		digest:      contract.DigestBytes(canonical),
		band:        stringOf(normalized["band"]),
		scoreMicros: score,
		sources:     pair,
		pairKey:     pairKey,
		policyRef:   policyRef,
		canonical:   canonical,
	}, nil
}

// AssessInput is the closed MT1 assessment input. Compatibility observations
// (applicability overlap, permission compatibility, blocking conflict count)
// arrive from the protected feature comparison; the band is never an input —
// it is derived from score_micros under the frozen policy.
type AssessInput struct {
	AssessmentID string
	// Sources holds exactly 2 refs in any order (the pair is normalized).
	Sources                []contract.SkillArtifactRef
	ScoreMicros            int64
	ApplicabilityOverlap   string // none|partial|complete|unknown
	PermissionCompatible   bool
	BlockingConflictCount  int64
	FeatureRecordRefs      []contract.VersionedRef
	SupportingEvidenceRefs []contract.EvidenceRef
	RefutingEvidenceRefs   []contract.EvidenceRef
	AssessorRef            contract.VersionedRef
}

// Assess runs the protected MT1 mint (GMS §12.9). Fail-closed order:
//
//  1. exactly 2 distinct refs, both step_guidance (cross-kind pairs reject:
//     v1 merges step_guidance only, Contract §5.5.1);
//  2. active-only: each source MUST be the CURRENT active head of its
//     lineage (inactive or superseded sources reject — a merge over a stale
//     head is impossible by construction);
//  3. every supporting/refuting evidence ref resolves committed
//     (EVIDENCE_NOT_COMMITTED);
//  4. the band is DERIVED from the integer score under the versioned policy;
//  5. the document canonicalizes with its x-digest (assessment_digest) and
//     validates against the authority schema;
//  6. ONE transaction appends the immutable record to the similarity ledger,
//     idempotent per (assessment id, digest).
//
// Below-threshold pairs assess fine — the assessment itself is the record —
// but the band blocks any later proposal creation (admission re-checks).
func (s *Service) Assess(ctx context.Context, in AssessInput) (*Assessment, error) {
	if in.AssessmentID == "" {
		return nil, newError(ReasonRefMismatch, "assessment id required")
	}
	if len(in.Sources) != 2 {
		return nil, newError(ReasonRefMismatch,
			"binary similarity needs exactly 2 sources, got %d (n-way assessment is out of scope in v1)", len(in.Sources))
	}
	if in.Sources[0].LineageID == in.Sources[1].LineageID {
		return nil, newError(ReasonRefMismatch,
			"sources must be two distinct lineages, got %q twice", in.Sources[0].LineageID)
	}
	for _, source := range in.Sources {
		if source.Kind != KindStepGuidance {
			return nil, newError(ReasonSkillKindInvalid,
				"source %s is %q; v1 merges step_guidance only (cross-kind merge is out of scope)", source.LineageID, source.Kind)
		}
	}
	pair := CanonicalPair(in.Sources[0], in.Sources[1])
	// Active-only sources: the exact ref must be the current active head.
	for _, source := range pair {
		active, err := s.heads.ActiveRevision(ctx, source.LineageID)
		if err != nil {
			return nil, err
		}
		if active == nil {
			return nil, newError(ReasonActiveHeadConflict,
				"source %s has no active revision; merge sources must be currently active", source.LineageID)
		}
		if *active != source {
			return nil, newError(ReasonActiveHeadConflict,
				"source %s is not the current active head (%s v%s); only current active heads may assess for merge",
				source.LineageID, active.LineageID, active.Version)
		}
	}
	// Evidence closure: staged/uncommitted evidence can never support an
	// assessment.
	for _, ref := range append(append([]contract.EvidenceRef{}, in.SupportingEvidenceRefs...), in.RefutingEvidenceRefs...) {
		if err := s.checkEvidence(ref); err != nil {
			return nil, err
		}
	}
	if len(in.FeatureRecordRefs) == 0 {
		return nil, newError(ReasonRefMismatch, "at least one feature record ref is required")
	}

	band := s.policy.Band(in.ScoreMicros)
	doc := map[string]any{
		"schema_version":      contract.SchemaSimilarityAssessment,
		"assessment_id":       in.AssessmentID,
		"assessment_version":  json.Number("1"),
		"source_skill_refs":   []any{SkillRefDoc(pair[0]), SkillRefDoc(pair[1])},
		"policy_ref":          versionedRefDoc(s.policy.Ref),
		"feature_record_refs": versionedRefDocs(in.FeatureRecordRefs),
		"score_micros":        json.Number(fmt.Sprintf("%d", in.ScoreMicros)),
		"band":                band,
		"compatibility": map[string]any{
			"kind_compatible":         true, // both step_guidance, checked above
			"applicability_overlap":   in.ApplicabilityOverlap,
			"permission_compatible":   in.PermissionCompatible,
			"port_compatible":         "not_applicable", // step_guidance carries no ports
			"blocking_conflict_count": json.Number(fmt.Sprintf("%d", in.BlockingConflictCount)),
		},
		"supporting_evidence_refs": evidenceRefDocs(in.SupportingEvidenceRefs),
		"refuting_evidence_refs":   evidenceRefDocs(in.RefutingEvidenceRefs),
		"assessor_ref":             versionedRefDoc(in.AssessorRef),
	}
	digest, err := s.gates.ComputeDigestPreimage(doc, SchemaSimilarityAssessment)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "assessment digest preimage: %v", err)
	}
	doc["assessment_digest"] = digest
	if err := s.gates.ValidateInstance(doc, SchemaSimilarityAssessment); err != nil {
		return nil, newError(ReasonDigestMismatch, "minted assessment failed its authority schema: %v", err)
	}
	assessment, err := s.Normalize(doc)
	if err != nil {
		return nil, err
	}
	if assessment.band != band {
		return nil, newError(ReasonDigestMismatch, "derived band %q disagrees with normalized band %q", band, assessment.band)
	}
	if err := s.commit(ctx, assessment); err != nil {
		return nil, err
	}
	return assessment, nil
}

// commit appends the immutable assessment record idempotently.
func (s *Service) commit(ctx context.Context, assessment *Assessment) error {
	return s.mgr.WithinTx(ctx, func(tx *ledger.Tx) error {
		replayed, recorded, err := tx.RecordIdempotency(
			"similarity-assess:"+assessment.id, assessment.digest, assessment.canonical)
		if err != nil {
			return err
		}
		if replayed {
			if string(recorded) != string(assessment.canonical) {
				return newError(ledger.ReasonIdempotencyConflict,
					"assessment %s replays with different canonical bytes", assessment.id)
			}
			return nil
		}
		if _, err := tx.AppendEvent(ledger.LedgerSimilarity, assessment.id, assessment.id, assessment.canonical); err != nil {
			return err
		}
		return nil
	})
}

// Resolve reads one committed assessment from the similarity ledger. A
// non-empty wantDigest must match the committed canonical digest exactly.
func (s *Service) Resolve(ctx context.Context, assessmentID, wantDigest string) (*Assessment, error) {
	entries, err := s.store.Snapshot(ledger.LedgerSimilarity, assessmentID)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return nil, newError(ReasonDigestMismatch, "assessment %s payload unresolved", assessmentID)
		}
		if wantDigest != "" && entry.PayloadDigest != wantDigest {
			continue
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return nil, newError(ReasonDigestMismatch, "assessment %s payload invalid: %v", assessmentID, err)
		}
		return s.Normalize(value)
	}
	return nil, newError(ReasonRefMismatch,
		"similarity assessment %s does not resolve committed with digest %s", assessmentID, wantDigest)
}

// checkEvidence resolves one evidence ref against the committed-evidence
// query port (exact (id, version, digest) triple, committed|sealed state).
func (s *Service) checkEvidence(ref contract.EvidenceRef) error {
	record, found, err := s.evidence.GetEvidence(ref.EvidenceID)
	if err != nil {
		return err
	}
	if !found || record.Version != ref.Version || record.EvidenceDigest != ref.EvidenceDigest ||
		(record.CommitState != "committed" && record.CommitState != "sealed") {
		return newError(ReasonEvidenceNotCommitted,
			"evidence %s v%s does not resolve committed with digest %s (staged evidence cannot support an assessment)",
			ref.EvidenceID, ref.Version, ref.EvidenceDigest)
	}
	return nil
}

// --- decoder-model helpers ----------------------------------------------------

func versionedRefDoc(ref contract.VersionedRef) map[string]any {
	return map[string]any{"id": ref.ID, "version": json.Number(ref.Version), "digest": ref.Digest}
}

func versionedRefDocs(refs []contract.VersionedRef) []any {
	out := make([]any, 0, len(refs))
	for _, ref := range refs {
		out = append(out, versionedRefDoc(ref))
	}
	return out
}

func evidenceRefDocs(refs []contract.EvidenceRef) []any {
	out := make([]any, 0, len(refs))
	for _, ref := range refs {
		out = append(out, map[string]any{
			"schema_version":  contract.SchemaEvidenceRef,
			"evidence_id":     ref.EvidenceID,
			"version":         json.Number(ref.Version),
			"evidence_digest": ref.EvidenceDigest,
			"commit_state":    ref.CommitState,
			"evidence_kind":   ref.EvidenceKind,
		})
	}
	return out
}

func stringOf(v any) string {
	s, _ := contract.AsString(v)
	return s
}

func digitsOf(v any) string {
	if n, ok := v.(json.Number); ok {
		return string(n)
	}
	return ""
}

func intOf(v any) (int64, bool) {
	if !contract.IsIntegerNumber(v) {
		return 0, false
	}
	n, _ := v.(json.Number)
	var out int64
	if _, err := fmt.Sscanf(string(n), "%d", &out); err != nil {
		return 0, false
	}
	return out, true
}

func deepCopyDoc(obj map[string]any) map[string]any {
	data, err := contract.JCS(obj)
	if err != nil {
		return nil
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return nil
	}
	out, _ := contract.AsObject(value)
	return out
}
