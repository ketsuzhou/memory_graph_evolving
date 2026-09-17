// Package artifact is the GMS-202 canonical four-kind v2 artifact gate.
// (Contract §8, GMS §3): the canonical artifact envelope, the closed kind
// bodies (procedure / step_guidance / composite) validated against the
// CTR-005 authority schemas, digest recomputation, the extension whitelist,
// exact refs, the permission cap and — for composites — ports, DAG,
// bounded retry, failure-handler and whole-packet freeze checks.
//
// The service is a pure gate: it neither reads nor writes ledgers. Evidence
// commitment closure belongs to the candidate binding transaction
// (internal/skillevolution/candidate); release-time child activation
// belongs to GMS-204.
package artifact

import (
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// Closed reason codes of this gate (frozen system registry, group
// gms-artifact-proposal-candidate plus the schema/canonical/ref group
// reused from the contract constants).
const (
	ReasonArtifactBodyInvalid        = "ARTIFACT_BODY_INVALID"
	ReasonBranchProvenanceIncomplete = "BRANCH_PROVENANCE_INCOMPLETE"
	ReasonPortSchemaMissing          = "PORT_SCHEMA_MISSING"
	ReasonPortSchemaIncompatible     = "PORT_SCHEMA_INCOMPATIBLE"
	ReasonCompositeCycle             = "COMPOSITE_CYCLE"
	ReasonCompositeChildNotActive    = "COMPOSITE_CHILD_NOT_ACTIVE"
	ReasonCompositeRetryInvalid      = "COMPOSITE_RETRY_INVALID"
	ReasonCompositeFailureInvalid    = "COMPOSITE_FAILURE_ACTION_INVALID"
	ReasonPermissionCapExceeded      = validation.CodePermissionCapExceeded
	ReasonRefMismatch                = contract.ReasonRefMismatch
	ReasonNonExactRef                = validation.CodeNonExactRef
	ReasonSkillKindInvalid           = "SKILL_KIND_INVALID"
)

// Error is the registry-code gate failure; Detail is diagnostic only.
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "artifact: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err ("" when nil). Validation
// errors from the schema layer surface with their own closed codes.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if ae, ok := err.(*Error); ok {
		return ae.ReasonCode
	}
	return validation.CodeOf(err)
}

// Schema file names of the canonical artifact authority shapes.
const (
	SchemaEnvelope       = "skill-artifact-envelope.schema.json"
	SchemaHumanProcedure = "procedure-body.schema.json"
	SchemaStepGuidance   = "step-guidance-body.schema.json"
	SchemaComposite      = "composite-artifact-body.schema.json"
	SchemaTool           = "tool-artifact-body.schema.json"
)

// kindBodySchemas maps each canonical kind to its body authority schema.
var kindBodySchemas = map[string]string{
	"human_procedure": SchemaHumanProcedure,
	"step_guidance":   SchemaStepGuidance,
	"composite":       SchemaComposite,
	"tool":            SchemaTool,
}

// Service is the canonical artifact gate.
type Service struct {
	gates    *validation.Gates
	children ChildArtifactResolver // optional release-time child resolution seam
}

// NewService wires the gate to the shared validation gates.
func NewService(gates *validation.Gates) (*Service, error) {
	if gates == nil {
		return nil, fmt.Errorf("artifact: nil validation gates")
	}
	return &Service{gates: gates}, nil
}

// WithChildResolver returns a copy of the service that additionally
// resolves every composite child to a released, currently active revision
// and folds the children permission union under the Host cap (GMS §3.5;
// used at release binding, nil in the pure static gate).
func (s *Service) WithChildResolver(children ChildArtifactResolver) *Service {
	clone := *s
	clone.children = children
	return &clone
}

// CanonicalArtifact is the immutable result of a green canonicalization:
// the frozen canonical bytes, their digest and — for composites — the
// frozen whole-packet view. Views hand out copies only.
type CanonicalArtifact struct {
	kind        string
	digest      string
	canonical   []byte
	packet      *CompositePacket
	body        map[string]any
	permissions []string
}

// Kind returns the canonical kind.
func (a *CanonicalArtifact) Kind() string { return a.kind }

// BodyDigest returns the digest of the canonical artifact bytes (equal for
// candidate and released bodies, GMS §3.7).
func (a *CanonicalArtifact) BodyDigest() string { return a.digest }

// Canonical returns a copy of the canonical JCS bytes.
func (a *CanonicalArtifact) Canonical() []byte {
	out := make([]byte, len(a.canonical))
	copy(out, a.canonical)
	return out
}

// Packet returns the frozen composite packet (nil for non-composites).
func (a *CanonicalArtifact) Packet() *CompositePacket { return a.packet }

// Body returns a deep copy of the kind body.
func (a *CanonicalArtifact) Body() map[string]any { return deepCopyMap(a.body) }

// PermissionCapabilities returns the envelope permission capabilities
// (copies) — the composite union folds them under the Host cap.
func (a *CanonicalArtifact) PermissionCapabilities() []string {
	out := make([]string, len(a.permissions))
	copy(out, a.permissions)
	return out
}

// permissionCapabilities reads the envelope permissions array.
func permissionCapabilities(envelope map[string]any) []string {
	entries, _ := contract.AsArray(envelope["permissions"])
	out := make([]string, 0, len(entries))
	for _, raw := range entries {
		entry, _ := contract.AsObject(raw)
		if capability, ok := contract.AsString(entry["capability"]); ok {
			out = append(out, capability)
		}
	}
	return out
}

// EvidenceRefs returns every strict Contract §7.7 evidence ref carried by
// the artifact body (branch provenance), fail-closed on shape violations.
func (a *CanonicalArtifact) EvidenceRefs() ([]contract.EvidenceRef, error) {
	return collectEvidenceRefs(a.body)
}

// Canonicalize runs the full three-kind canonical gate over one decoder-
// model artifact document:
//
//  1. envelope shape (closed fields, kind enum, applicability, permissions)
//     against $FIX schema/shared/skill-artifact-envelope.schema.json;
//  2. the integer-only hashed core (JCS) and its digest;
//  3. the extension whitelist (unknown required fails, optional ignored);
//  4. the kind body shape against the kind's authority schema;
//  5. the semantic kind gates: kind drift over every exact skill ref,
//     permission cap, procedure step uniqueness, guidance branch uniqueness
//     and provenance coverage, composite ports/DAG/retry/fallback and the
//     whole-packet freeze.
func (s *Service) Canonicalize(doc any) (*CanonicalArtifact, error) {
	// 0. Exact-ref kind-drift integrity applies to any document shape (S1
	// kindDriftIssue): two refs sharing identity with different kinds
	// reject before anything else.
	if err := checkKindDrift(doc); err != nil {
		return nil, err
	}
	obj, ok := contract.AsObject(doc)
	if !ok {
		return nil, newError(ReasonArtifactBodyInvalid, "artifact document must be a JSON object")
	}
	// 1. Envelope shape (includes the extension and digest gates).
	if err := s.gates.ValidateInstance(obj, SchemaEnvelope); err != nil {
		return nil, err
	}
	// 2. Canonical bytes and digest of the whole envelope (lineage/version
	// never enter the body; candidate and released bodies digest equal).
	canonical, err := contract.JCS(contract.NormalizeForHashing(deepCopyValue(obj)))
	if err != nil {
		return nil, newError(validation.CodeNonIntegerNumber, "artifact cannot enter the hashed core: %v", err)
	}
	kind, _ := contract.AsString(obj["kind"])
	body, _ := contract.AsObject(obj["body"])

	// 3. Kind body gate.
	if err := s.canonicalizeKind(kind, obj, body); err != nil {
		return nil, err
	}

	result := &CanonicalArtifact{
		kind:        kind,
		digest:      contract.DigestBytes(canonical),
		canonical:   canonical,
		body:        deepCopyMap(body),
		permissions: permissionCapabilities(obj),
	}
	if kind == "composite" {
		packet, err := freezePacket(body)
		if err != nil {
			return nil, err
		}
		result.packet = packet
	}
	return result, nil
}

// canonicalizeKind dispatches the per-kind static gates.
func (s *Service) canonicalizeKind(kind string, envelope, body map[string]any) error {
	if err := s.gates.CheckPermissionsWalked(envelope); err != nil {
		return err
	}
	switch kind {
	case "human_procedure":
		// The closed body schema already forbids forged checkpoint-derived
		// causal_context / branch-local future (Contract §8.2); check the
		// remaining semantics before the shape gate so kind-specific codes
		// surface first.
		if err := checkUniqueIDs(body, "steps", "step_id", ReasonArtifactBodyInvalid, "step_id"); err != nil {
			return err
		}
		return s.gates.ValidateInstance(body, SchemaHumanProcedure)
	case "step_guidance":
		if err := checkBranchProvenance(body); err != nil {
			return err
		}
		if err := checkUniqueIDs(body, "branches", "branch_id", ReasonArtifactBodyInvalid, "branch_id"); err != nil {
			return err
		}
		return s.gates.ValidateInstance(body, SchemaStepGuidance)
	case "composite":
		return s.canonicalizeComposite(body)
	case "tool":
		return s.canonicalizeTool(envelope, body)
	default:
		return newError(ReasonSkillKindInvalid, "kind %q outside the closed canonical kind set", kind)
	}
}

// checkKindDrift rejects two exact skill refs sharing
// (lineage_id, version, artifact_digest) with different kinds (S1
// kindDriftIssue, Contract §6.2 kind/lineage integrity).
func checkKindDrift(value any) error {
	groups := map[string]map[string]bool{}
	collectSkillRefs(value, func(ref map[string]any) {
		key := contract.CanonicalKey([]any{
			contract.CanonicalKey(ref["lineage_id"]),
			contract.CanonicalKey(ref["version"]),
			contract.CanonicalKey(ref["artifact_digest"]),
		})
		if groups[key] == nil {
			groups[key] = map[string]bool{}
		}
		groups[key][contract.CanonicalKey(ref["kind"])] = true
	})
	for _, kinds := range groups {
		if len(kinds) > 1 {
			return newError(ReasonRefMismatch, "two exact skill refs share identity but carry different kinds (kind drift)")
		}
	}
	return nil
}

// collectSkillRefs walks the document for objects that declare the §7.3
// schema version.
func collectSkillRefs(value any, fn func(map[string]any)) {
	switch t := value.(type) {
	case map[string]any:
		if sv, _ := contract.AsString(t["schema_version"]); sv == contract.SchemaSkillArtifactRef {
			fn(t)
		}
		for _, v := range t {
			collectSkillRefs(v, fn)
		}
	case []any:
		for _, item := range t {
			collectSkillRefs(item, fn)
		}
	}
}

// checkUniqueIDs rejects duplicate ids in one id-bearing array.
func checkUniqueIDs(holder map[string]any, arrayField, idField string, reason, what string) error {
	if holder == nil {
		return nil
	}
	items, _ := contract.AsArray(holder[arrayField])
	seen := map[string]bool{}
	for _, raw := range items {
		item, _ := contract.AsObject(raw)
		id, ok := contract.AsString(item[idField])
		if ok && id != "" {
			if seen[id] {
				return newError(reason, "duplicate %s %q", what, id)
			}
			seen[id] = true
		}
	}
	return nil
}

// checkBranchProvenance: every guidance branch must trace to at least one
// evidence ref (committed/sealed evidence, checkpoint-derived anchors or
// protected assessments all arrive as evidence refs; model free text is
// never provenance — GMS §3.4).
func checkBranchProvenance(body map[string]any) error {
	branches, _ := contract.AsArray(body["branches"])
	for i, raw := range branches {
		branch, _ := contract.AsObject(raw)
		action, _ := contract.AsObject(branch["action"])
		refs, _ := contract.AsArray(action["evidence_refs"])
		if len(refs) == 0 {
			return newError(ReasonBranchProvenanceIncomplete,
				"branch %d carries no action evidence ref: branch provenance must trace to committed/sealed evidence, an anchor or a protected assessment", i)
		}
	}
	return nil
}

// collectEvidenceRefs walks a body for every evidence_refs array and parses
// each entry as a strict Contract §7.7 ref.
func collectEvidenceRefs(body map[string]any) ([]contract.EvidenceRef, error) {
	var out []contract.EvidenceRef
	var walk func(value any) error
	walk = func(value any) error {
		switch t := value.(type) {
		case map[string]any:
			if refsRaw, present := t["evidence_refs"]; present {
				refs, _ := contract.AsArray(refsRaw)
				for _, rawRef := range refs {
					refObj, isObj := contract.AsObject(rawRef)
					if !isObj {
						return newError(validation.CodeSchemaEnumInvalid, "evidence ref must be an object")
					}
					ref, err := contract.ParseEvidenceRef(refObj)
					if err != nil {
						return newError(validation.CodeOf(err) /* schema code */, "%v", err)
					}
					out = append(out, ref)
				}
			}
			for _, v := range t {
				if err := walk(v); err != nil {
					return err
				}
			}
		case []any:
			for _, item := range t {
				if err := walk(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(body); err != nil {
		return nil, err
	}
	return out, nil
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopyMap(t)
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}

func deepCopyMap(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = deepCopyValue(v)
	}
	return out
}

// canonicalizeTool validates the v2 executable contract and proves that the
// tool body's declared capabilities exactly match the envelope authority.
func (s *Service) canonicalizeTool(envelope, body map[string]any) error {
	if err := s.gates.ValidateInstance(body, SchemaTool); err != nil {
		return err
	}
	declaredRaw, _ := contract.AsArray(body["declared_capabilities"])
	declared := make(map[string]bool, len(declaredRaw))
	for _, raw := range declaredRaw {
		capability, _ := contract.AsString(raw)
		declared[capability] = true
	}
	permissions := permissionCapabilities(envelope)
	if len(declared) != len(permissions) {
		return newError(ReasonPermissionCapExceeded, "tool declared capabilities do not match envelope permissions")
	}
	for _, capability := range permissions {
		if !declared[capability] {
			return newError(ReasonPermissionCapExceeded, "tool capability %q is not declared by the executable contract", capability)
		}
	}
	return nil
}
