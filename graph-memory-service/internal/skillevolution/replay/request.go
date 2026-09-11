package replay

import (
	"encoding/json"
	"strconv"

	"river2.dev/graph-memory-service/internal/contract"
)

// Request is the frozen Contract §7.10 ReplayRequest: a strictly parsed,
// schema-validated decoder-model document plus typed exact-ref views. The
// same packet always addresses the same request bytes (idempotency).
type Request struct {
	doc map[string]any

	id                  string
	candidate           contract.CandidateArtifactRef
	candidateDoc        map[string]any
	baselines           []contract.SkillArtifactRef
	baselineDocs        []map[string]any
	adapterRef          contract.VersionedRef
	profileRef          contract.VersionedRef
	idempotencyKey      string
	requiredSourceHeads []map[string]any
	requestDigest       string
}

// ParseRequest validates one decoder-model §7.10 document against the
// shared authority schema (closed fields, integer-only core, exact-ref
// shapes, causal mode, conditional required_source_heads for merge
// candidates) and returns the frozen request. Validation codes surface
// unchanged (SCHEMA_*/NON_INTEGER_NUMBER/...).
func (s *Service) ParseRequest(doc any) (*Request, error) {
	obj, ok := contract.AsObject(doc)
	if !ok {
		return nil, newError(ReasonReplayRequestInvalid, "replay request must be a JSON object")
	}
	if err := s.gates.ValidateInstance(obj, SchemaReplayRequest); err != nil {
		return nil, err
	}
	id, _ := contract.AsString(obj["replay_request_id"])
	if id == "" {
		return nil, newError(ReasonReplayRequestInvalid, "replay_request_id empty after schema validation")
	}

	candidateRaw, _ := contract.AsObject(obj["candidate_ref"])
	candidate, err := contract.ParseCandidateArtifactRef(candidateRaw)
	if err != nil {
		return nil, newError(ReasonReplayRequestInvalid, "candidate_ref is not an exact §7.4 ref: %v", err)
	}
	var baselines []contract.SkillArtifactRef
	var baselineDocs []map[string]any
	rawBaselines, _ := contract.AsArray(obj["baseline_skill_refs"])
	for _, raw := range rawBaselines {
		refDoc, isObj := contract.AsObject(raw)
		if !isObj {
			return nil, newError(ReasonReplayRequestInvalid, "baseline_skill_refs entry is not an object")
		}
		ref, err := contract.ParseSkillArtifactRef(refDoc)
		if err != nil {
			return nil, newError(ReasonReplayRequestInvalid, "baseline skill ref is not an exact §7.3 ref: %v", err)
		}
		baselines = append(baselines, ref)
		baselineDocs = append(baselineDocs, refDoc)
	}
	if len(baselines) == 0 {
		return nil, newError(ReasonReplayRequestInvalid, "at least one baseline skill ref is required")
	}

	adapter, err := versionedRefOf(obj, "runtime_adapter_ref")
	if err != nil {
		return nil, err
	}
	profile, err := versionedRefOf(obj, "replay_profile_ref")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _ := contract.AsString(obj["idempotency_key"])

	var heads []map[string]any
	if rawHeads, ok := contract.AsArray(obj["required_source_heads"]); ok {
		for _, raw := range rawHeads {
			if head, isObj := contract.AsObject(raw); isObj {
				heads = append(heads, head)
			}
		}
	}

	// The request digest freezes the whole document (JCS); the same packet
	// always yields the same ReplayRequestRef.
	digest, err := contract.DigestOf(obj)
	if err != nil {
		return nil, newError(ReasonReplayRequestInvalid, "request does not canonicalize: %v", err)
	}

	return &Request{
		doc:                 obj,
		id:                  id,
		candidate:           candidate,
		candidateDoc:        candidateRaw,
		baselines:           baselines,
		baselineDocs:        baselineDocs,
		adapterRef:          adapter,
		profileRef:          profile,
		idempotencyKey:      idempotencyKey,
		requiredSourceHeads: heads,
		requestDigest:       digest,
	}, nil
}

func versionedRefOf(obj map[string]any, field string) (contract.VersionedRef, error) {
	raw, ok := contract.AsObject(obj[field])
	if !ok {
		return contract.VersionedRef{}, newError(ReasonReplayRequestInvalid, "%s is not an object", field)
	}
	ref, err := contract.ParseVersionedRef(raw)
	if err != nil {
		return contract.VersionedRef{}, newError(ReasonReplayRequestInvalid, "%s is not an exact §7.2 ref: %v", field, err)
	}
	return ref, nil
}

// ID returns the replay request id.
func (r *Request) ID() string { return r.id }

// CandidateRef returns a copy of the exact §7.4 candidate ref.
func (r *Request) CandidateRef() contract.CandidateArtifactRef { return r.candidate }

// CandidateRefDoc returns the decoder-model candidate ref (shared, not
// copied; treat as read-only).
func (r *Request) CandidateRefDoc() map[string]any { return r.candidateDoc }

// BaselineRefs returns copies of the exact §7.3 baseline refs.
func (r *Request) BaselineRefs() []contract.SkillArtifactRef {
	out := make([]contract.SkillArtifactRef, len(r.baselines))
	copy(out, r.baselines)
	return out
}

// AdapterRef returns the frozen runtime adapter ref.
func (r *Request) AdapterRef() contract.VersionedRef { return r.adapterRef }

// ProfileRef returns the frozen replay profile ref.
func (r *Request) ProfileRef() contract.VersionedRef { return r.profileRef }

// IsMerge reports whether the candidate originates from a merge proposal
// (then §7.10 requires the frozen required_source_heads).
func (r *Request) IsMerge() bool { return r.candidate.OriginType == "merge_proposal" }

// RequiredSourceHeads returns deep copies of the frozen source-head
// expectations (empty for ordinary revisions).
func (r *Request) RequiredSourceHeads() []map[string]any {
	out := make([]map[string]any, 0, len(r.requiredSourceHeads))
	for _, head := range r.requiredSourceHeads {
		out = append(out, deepCopyObject(head))
	}
	return out
}

// RequestDigest returns the JCS digest of the whole frozen request.
func (r *Request) RequestDigest() string { return r.requestDigest }

// Ref returns the exact VersionedRef addressing this frozen request.
func (r *Request) Ref() contract.VersionedRef {
	return contract.VersionedRef{ID: r.id, Version: "1", Digest: r.requestDigest}
}

// Doc returns a deep copy of the frozen request document.
func (r *Request) Doc() map[string]any { return deepCopyObject(r.doc) }

// deepCopyObject round-trips a decoder-model value through JCS so callers
// cannot mutate frozen state through shared maps.
func deepCopyObject(obj map[string]any) map[string]any {
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

// num64 wraps an int64 as an exact integer JSON number (no float path).
func num64(v int64) json.Number { return json.Number(strconv.FormatInt(v, 10)) }
