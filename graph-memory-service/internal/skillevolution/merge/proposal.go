package merge

import (
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/similarity"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// SchemaMergeProposalDoc is the §7.22 payload schema version (the ledger
// whitelist string of the merge-proposal ledger).
const SchemaMergeProposalDoc = "gms.merge-proposal.v1"

// Merge intent constants frozen by v1 (Contract §5.5 M-decisions).
const (
	IntentTargetKind                  = similarity.KindStepGuidance
	IntentStrategy                    = "symmetric_new_lineage"
	IntentSourceDisposition           = "retain"
	IntentNormalizedApplicabilitySalt = "gms.merge-applicability.v1"
)

// PolicyRefs are the six versioned policy refs a §7.22 proposal must freeze
// (Contract §7.22 policies; every threshold and rule is versioned — M7).
type PolicyRefs struct {
	SimilarityPolicyRef  contract.VersionedRef
	MergePolicyRef       contract.VersionedRef
	ValidationProfileRef contract.VersionedRef
	ReplayProfileRef     contract.VersionedRef
	ReleaseRuleRef       contract.VersionedRef
	UtilityComparatorRef contract.VersionedRef
}

func (p PolicyRefs) doc() map[string]any {
	return map[string]any{
		"similarity_policy_ref":  versionedRefDoc(p.SimilarityPolicyRef),
		"merge_policy_ref":       versionedRefDoc(p.MergePolicyRef),
		"validation_profile_ref": versionedRefDoc(p.ValidationProfileRef),
		"replay_profile_ref":     versionedRefDoc(p.ReplayProfileRef),
		"release_rule_ref":       versionedRefDoc(p.ReleaseRuleRef),
		"utility_comparator_ref": versionedRefDoc(p.UtilityComparatorRef),
	}
}

// Origin is the Contract §7.9 origin block of the initiator.
type Origin struct {
	InitiatorType string // automatic | model | human
	InitiatorRef  string
	RequestRef    string
}

func (o Origin) doc() map[string]any {
	return map[string]any{
		"initiator_type": o.InitiatorType,
		"initiator_ref":  o.InitiatorRef,
		"request_ref":    o.RequestRef,
	}
}

// SourceEvidenceSet is one source's frozen evidence bundle (§7.22
// source_evidence_sets entry).
type SourceEvidenceSet struct {
	Source                 contract.SkillArtifactRef
	SupportingEvidenceRefs []contract.EvidenceRef
	RefutingEvidenceRefs   []contract.EvidenceRef
	SuccessPathRefs        []contract.VersionedRef
	FailurePathRefs        []contract.VersionedRef
	RecoveryPathRefs       []contract.VersionedRef
	BranchClaimRefs        []contract.VersionedRef
}

// JointEvidence is the bilateral joint evidence block (§7.22).
type JointEvidence struct {
	OverlapEvidenceRefs    []contract.EvidenceRef
	DivergenceEvidenceRefs []contract.EvidenceRef
	SharedAnchorRefs       []contract.VersionedRef
	PairedFixtureRefs      []contract.VersionedRef
}

// Conflict is one parsed §7.22 conflicts entry.
type Conflict struct {
	Doc map[string]any
}

// Proposal is the immutable parsed view of one committed §7.22 document.
type Proposal struct {
	doc        map[string]any
	id         string
	version    string
	digest     string
	canonical  []byte
	sources    [2]contract.SkillArtifactRef
	pairKey    string
	groupKey   string
	intentKey  string
	assessRefs []contract.VersionedRef
}

// ID returns the proposal id.
func (p *Proposal) ID() string { return p.id }

// Version returns the exact proposal version digits.
func (p *Proposal) Version() string { return p.version }

// Digest returns the whole-document canonical digest (ledger identity).
func (p *Proposal) Digest() string { return p.digest }

// Ref renders the proposal identity as a VersionedRef.
func (p *Proposal) Ref() contract.VersionedRef {
	return contract.VersionedRef{ID: p.id, Version: p.version, Digest: p.digest}
}

// Sources returns the canonical §6.3-ordered source pair.
func (p *Proposal) Sources() [2]contract.SkillArtifactRef { return p.sources }

// PairKey returns dedup.source_pair_key.
func (p *Proposal) PairKey() string { return p.pairKey }

// GroupKey returns dedup.proposal_group_key.
func (p *Proposal) GroupKey() string { return p.groupKey }

// IntentKey returns dedup.intent_key.
func (p *Proposal) IntentKey() string { return p.intentKey }

// AssessmentRefs returns the frozen similarity assessment refs.
func (p *Proposal) AssessmentRefs() []contract.VersionedRef {
	out := make([]contract.VersionedRef, len(p.assessRefs))
	copy(out, p.assessRefs)
	return out
}

// Conflicts returns deep copies of the conflict entries.
func (p *Proposal) Conflicts() []map[string]any {
	raw, _ := contract.AsArray(p.doc["conflicts"])
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if obj, ok := contract.AsObject(item); ok {
			out = append(out, deepCopyDoc(obj))
		}
	}
	return out
}

// Doc returns a deep copy of the committed document.
func (p *Proposal) Doc() map[string]any { return deepCopyDoc(p.doc) }

// CanonicalBytes returns a copy of the canonical JCS bytes.
func (p *Proposal) CanonicalBytes() []byte {
	out := make([]byte, len(p.canonical))
	copy(out, p.canonical)
	return out
}

// ParseProposal strict-parses one committed §7.22 document. The three dedup
// keys are RE-DERIVED from the canonical sources and intent and the derived
// values are authoritative: every group/winner decision uses the recomputed
// keys, never the declared ones (corpus documents carry frozen
// pre-implementation digests — declared keys are inert data, so a smuggled
// group identity cannot influence behavior).
func ParseProposal(gates *validation.Gates, doc any) (*Proposal, error) {
	obj, ok := contract.AsObject(doc)
	if !ok {
		return nil, newError(ReasonRefMismatch, "merge proposal must be a JSON object")
	}
	if err := gates.ValidateShape(obj, SchemaMergeProposal); err != nil {
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
		return nil, newError(ReasonMergeSourceInvalid, "source ref 0: %v", err)
	}
	second, err := contract.ParseSkillArtifactRef(secondRaw)
	if err != nil {
		return nil, newError(ReasonMergeSourceInvalid, "source ref 1: %v", err)
	}
	pair := similarity.CanonicalPair(first, second)
	if pair[0] != first || pair[1] != second {
		return nil, newError(ReasonRefMismatch,
			"source_skill_refs are not in canonical §6.3 order (the pair must be normalized before commitment)")
	}
	pairKey, err := similarity.SourcePairKey(pair)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "%v", err)
	}
	intent, _ := contract.AsObject(obj["merge_intent"])
	groupKey, intentKey, err := deriveDedupKeys(pair, intent)
	if err != nil {
		return nil, err
	}
	canonical, err := contract.JCS(contract.NormalizeForHashing(deepCopyDoc(obj)))
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "proposal does not canonicalize: %v", err)
	}
	var assessRefs []contract.VersionedRef
	rawAssess, _ := contract.AsArray(obj["similarity_assessment_refs"])
	for _, raw := range rawAssess {
		refObj, _ := contract.AsObject(raw)
		ref, err := contract.ParseVersionedRef(refObj)
		if err != nil {
			return nil, newError(ReasonRefMismatch, "similarity assessment ref: %v", err)
		}
		assessRefs = append(assessRefs, ref)
	}
	return &Proposal{
		doc:        deepCopyDoc(obj),
		id:         stringOf(obj["proposal_id"]),
		version:    digitsOf(obj["proposal_version"]),
		digest:     contract.DigestBytes(canonical),
		canonical:  canonical,
		sources:    pair,
		pairKey:    pairKey,
		groupKey:   groupKey,
		intentKey:  intentKey,
		assessRefs: assessRefs,
	}, nil
}

// deriveDedupKeys computes dedup.intent_key and dedup.proposal_group_key
// (GMS §7.2): the intent key digests the full merge intent; the group key
// digests the source lineages plus the normalized applicability key and the
// target kind — two proposals over the same pair and applicability with the
// same target kind share one group, whatever their intent details.
func deriveDedupKeys(pair [2]contract.SkillArtifactRef, intent map[string]any) (groupKey, intentKey string, err error) {
	intentDigest, derr := contract.DigestOf(map[string]any{
		"target_kind":                  stringOf(intent["target_kind"]),
		"strategy":                     stringOf(intent["strategy"]),
		"normalized_applicability_key": stringOf(intent["normalized_applicability_key"]),
		"source_disposition":           stringOf(intent["source_disposition"]),
		"required_branch_preservation": boolOf(intent["required_branch_preservation"]),
	})
	if derr != nil {
		return "", "", newError(ReasonDigestMismatch, "intent key: %v", derr)
	}
	groupDigest, derr := contract.DigestOf([]any{
		pair[0].LineageID,
		pair[1].LineageID,
		stringOf(intent["normalized_applicability_key"]),
		stringOf(intent["target_kind"]),
	})
	if derr != nil {
		return "", "", newError(ReasonDigestMismatch, "group key: %v", derr)
	}
	return groupDigest, intentDigest, nil
}

// LoadProposal resolves one proposal from the merge-proposal ledger by id
// (exact digest when non-empty).
func (s *Service) LoadProposal(proposalID, wantDigest string) (*Proposal, error) {
	entries, err := s.store.Snapshot(ledger.LedgerMergeProposal, proposalID)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if wantDigest != "" && entry.PayloadDigest != wantDigest {
			continue
		}
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			return nil, newError(ReasonDigestMismatch, "proposal %s payload unresolved", proposalID)
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return nil, newError(ReasonDigestMismatch, "proposal %s payload invalid: %v", proposalID, err)
		}
		return ParseProposal(s.gates, value)
	}
	return nil, newError(ReasonRefMismatch,
		"merge proposal %s does not resolve committed with digest %s", proposalID, wantDigest)
}

// CurrentState returns the derived lifecycle state of one proposal stream.
func (s *Service) CurrentState(proposalID string) (string, uint64, error) {
	return s.engine.committedState(proposalID)
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

func digitsOf(v any) string {
	if n, ok := v.(json.Number); ok {
		return string(n)
	}
	return ""
}

func boolOf(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

func num(v int64) json.Number { return json.Number(fmt.Sprintf("%d", v)) }

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
