// Package evidence implements the GMS protected evidence admission pipeline
// (GMS-201; GMS Spec §2.2/§4.2/§12.2, Contract §7.5–§7.7 and §13.1).
//
// Host settled Segment records are staged GMS-privately, validated against
// the FND-002 sealed-segment protocol (segment digest = SHA-256(JCS(segment)),
// recomputed Evidence/Path Seal digests, DAG/causal completeness, terminal
// tools, scope and provenance) and — only when every gate passes — committed
// through one atomic ledger transaction that stores the canonical evidence
// body, appends the EvidenceRef to the evidence ledger and records the
// idempotency result. Failed/aborted Host terminals and tampered inputs never
// mint an EvidenceRef.
//
// State machine (GMS §2.2; "staged" is the spec's "received" intake state):
//
//	staged → validating → committed
//	                    ↘ rejected
//	                    ↘ inconclusive
//
// Terminal states never reopen; recovery requires a new submission.
//
// Layering (the GMS-201 refactor seam):
//   - records.go  data vocabulary: states, closed reason codes, admission
//     errors, intake/staging records, the scope policy, the exact-ref read
//     models and the Host seal derivations shared with
//     $FIX/validate_recorded.py;
//   - service.go  the AdmissionService: staging, the validation gates, the
//     atomic commit transaction over the GMS-102 ledger ports and the
//     committed-only readers.
//
// This package imports internal/contract (GMS-101) as the single JCS/digest/
// reason authority and internal/skillevolution/ledger (GMS-102) as the single
// transaction authority; it never re-implements either.
package evidence

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// ---------------------------------------------------------------------------
// Admission states (GMS §2.2)
// ---------------------------------------------------------------------------

// Status is the closed admission state enum of one staged submission.
type Status string

const (
	// StatusStaged is the private intake state (the spec's "received"):
	// recorded, invisible to every committed-evidence reader.
	StatusStaged Status = "staged"
	// StatusValidating means the gates have run and passed for this
	// submission; it is the only state Commit accepts.
	StatusValidating Status = "validating"
	// StatusCommitted is terminal: the EvidenceRef is minted and readable.
	StatusCommitted Status = "committed"
	// StatusRejected is terminal: a closed gate failed deterministically.
	StatusRejected Status = "rejected"
	// StatusInconclusive is terminal: valid Host data that cannot be trusted
	// as complete evidence (mark_inconclusive, Contract §13.3).
	StatusInconclusive Status = "inconclusive"
)

// admissionEdges is the legal transition table; terminal states have no exit.
var admissionEdges = map[Status]map[Status]bool{
	StatusStaged:     {StatusValidating: true},
	StatusValidating: {StatusCommitted: true, StatusRejected: true, StatusInconclusive: true},
}

func legalAdmissionTransition(from, to Status) bool {
	return admissionEdges[from][to]
}

// StateEvent is one append-only admission state transition of a staged record.
type StateEvent struct {
	From       Status
	To         Status
	ReasonCode string // "" on the happy path
	Note       string
}

// ---------------------------------------------------------------------------
// Closed-registry reason codes (Contract §13.7.1; every code this package can
// emit exists in the digest-verified system registry — see
// verifyAdmissionReasonCodes)
// ---------------------------------------------------------------------------

const (
	// ReasonSegmentNotSettled: failed/aborted Host terminal — no seals exist,
	// no EvidenceRef may be minted (GMS §12.2).
	ReasonSegmentNotSettled = "SEGMENT_NOT_SETTLED"
	// ReasonNonsettledSegmentHasRefs: refs/seal bodies on a non-settled record.
	ReasonNonsettledSegmentHasRefs = "NONSETTLED_SEGMENT_HAS_REFS"
	// ReasonTerminalMismatch: seal and segment disagree on terminal identity.
	ReasonTerminalMismatch = "TERMINAL_MISMATCH"
	// ReasonTerminalAuditMismatch: terminal audit missing or divergent.
	ReasonTerminalAuditMismatch = "TERMINAL_AUDIT_MISMATCH"
	// ReasonSealDigestMismatch: seal digest does not cover the canonical bytes.
	ReasonSealDigestMismatch = "SEAL_DIGEST_MISMATCH"
	// ReasonCanonicalizationFailed: canonical bytes are not the JCS form.
	ReasonCanonicalizationFailed = "CANONICALIZATION_FAILED"
	// ReasonAppendAfterSettled: member outside the frozen frontier.
	ReasonAppendAfterSettled = "APPEND_AFTER_SETTLED"
	// ReasonMemberOrderInvalid: member sequences/ticks not strictly monotone.
	ReasonMemberOrderInvalid = "MEMBER_ORDER_INVALID"
	// ReasonFrontierMismatch: frontier end does not equal the last member.
	ReasonFrontierMismatch = "FRONTIER_MISMATCH"
	// ReasonPayloadDigestMismatch: member payload digest recompute failed.
	ReasonPayloadDigestMismatch = "PAYLOAD_DIGEST_MISMATCH"
	// ReasonDAGCycle: the causal graph is cyclic.
	ReasonDAGCycle = "DAG_CYCLE"
	// ReasonMissingCausalLink: per-kind causal requirements unmet.
	ReasonMissingCausalLink = "MISSING_CAUSAL_LINK"
	// ReasonNonterminalTool: delivery/tool not terminal at close.
	ReasonNonterminalTool = "NONTERMINAL_TOOL"
	// ReasonToolProxyDigestMismatch: ToolProxyResult digest recompute failed.
	ReasonToolProxyDigestMismatch = "TOOL_PROXY_DIGEST_MISMATCH"
	// ReasonEvidenceSealBodyMismatch: Evidence Seal body diverges from the record.
	ReasonEvidenceSealBodyMismatch = "EVIDENCE_SEAL_BODY_MISMATCH"
	// ReasonEvidenceSealDigestMismatch: Evidence Seal digest recompute failed.
	ReasonEvidenceSealDigestMismatch = "EVIDENCE_SEAL_DIGEST_MISMATCH"
	// ReasonPathSealDigestMismatch: Path Seal digest recompute failed.
	ReasonPathSealDigestMismatch = "PATH_SEAL_DIGEST_MISMATCH"
	// ReasonCheckpointNotSealed: checkpoint digest preimage recompute failed.
	ReasonCheckpointNotSealed = "CHECKPOINT_NOT_SEALED"
	// ReasonRefMismatch: exact ref identity divergence.
	ReasonRefMismatch = "REF_MISMATCH"
	// ReasonEvidenceSealInvalid: the seal envelope declares impossible evidence.
	ReasonEvidenceSealInvalid = "EVIDENCE_SEAL_INVALID"
	// ReasonMissingSealedPath: no sealed path / unresolved path / incomplete path.
	ReasonMissingSealedPath = "MISSING_SEALED_PATH"
	// ReasonEvidenceScopeDenied: submitter scope not authorized (exact).
	ReasonEvidenceScopeDenied = "EVIDENCE_SCOPE_DENIED"
	// ReasonEvidenceProvenanceIncomplete: submitter/scope/policy incomplete.
	ReasonEvidenceProvenanceIncomplete = "EVIDENCE_PROVENANCE_INCOMPLETE"
	// ReasonEvidenceStagingInvalid: unknown or malformed staged submission.
	ReasonEvidenceStagingInvalid = "EVIDENCE_STAGING_INVALID"
	// ReasonIllegalStateTransition: admission state machine violation.
	ReasonIllegalStateTransition = "ILLEGAL_STATE_TRANSITION"
	// ReasonEvidenceNotCommitted: ref outside the committed|sealed states.
	ReasonEvidenceNotCommitted = "EVIDENCE_NOT_COMMITTED"
)

// Aliases of codes frozen in internal/contract and internal/skillevolution/
// ledger (single source of truth).
const (
	ReasonSchemaVersionUnsupported = contract.ReasonSchemaVersionUnsupported
	ReasonSchemaFieldUnknown       = contract.ReasonSchemaFieldUnknown
	ReasonSchemaEnumInvalid        = contract.ReasonSchemaEnumInvalid
	ReasonSchemaRequiredMissing    = contract.ReasonSchemaRequiredFieldMissin
	ReasonInvalidJSON              = contract.ReasonInvalidJSON
	ReasonNonIntegerNumber         = contract.ReasonNonIntegerNumber
	ReasonDigestMismatch           = contract.ReasonDigestMismatch
	ReasonIdempotencyConflict      = ledger.ReasonIdempotencyConflict
)

// allAdmissionReasonCodes lists every code the package can emit; construction
// verifies each one against the digest-verified registry (Contract §13.7.1 R5).
func allAdmissionReasonCodes() []string {
	return []string{
		ReasonSegmentNotSettled, ReasonNonsettledSegmentHasRefs,
		ReasonTerminalMismatch, ReasonTerminalAuditMismatch,
		ReasonSealDigestMismatch, ReasonCanonicalizationFailed,
		ReasonAppendAfterSettled, ReasonMemberOrderInvalid, ReasonFrontierMismatch,
		ReasonPayloadDigestMismatch, ReasonDAGCycle, ReasonMissingCausalLink,
		ReasonNonterminalTool, ReasonToolProxyDigestMismatch,
		ReasonEvidenceSealBodyMismatch, ReasonEvidenceSealDigestMismatch,
		ReasonPathSealDigestMismatch, ReasonCheckpointNotSealed, ReasonRefMismatch,
		ReasonEvidenceSealInvalid, ReasonMissingSealedPath, ReasonEvidenceScopeDenied,
		ReasonEvidenceProvenanceIncomplete, ReasonEvidenceStagingInvalid,
		ReasonIllegalStateTransition, ReasonEvidenceNotCommitted,
		ReasonSchemaVersionUnsupported, ReasonSchemaEnumInvalid,
		ReasonSchemaRequiredMissing, ReasonInvalidJSON, ReasonNonIntegerNumber,
		ReasonDigestMismatch, ReasonIdempotencyConflict,
	}
}

// ---------------------------------------------------------------------------
// Admission errors
// ---------------------------------------------------------------------------

// AdmissionError is the fail-closed error type of the admission pipeline.
// ReasonCode is a closed-registry code; Detail is diagnostic only and never
// drives behavior.
type AdmissionError struct {
	ReasonCode string
	Detail     string
}

func (e *AdmissionError) Error() string {
	return fmt.Sprintf("evidence: %s: %s", e.ReasonCode, e.Detail)
}

// ReasonOf returns the closed-registry reason code carried by err — either an
// AdmissionError of this package or a port error of the ledger layer — or ""
// when err carries no code.
func ReasonOf(err error) string {
	var admErr *AdmissionError
	if errors.As(err, &admErr) {
		return admErr.ReasonCode
	}
	return ledger.ReasonOf(err)
}

// newAdmissionError builds an AdmissionError after verifying the code against
// the digest-verified reason registry; an unregistered code fails closed as a
// wiring error instead of leaking an invented behavior code.
func newAdmissionError(registry ledger.ReasonRegistry, code, detail string, args ...any) error {
	if registry == nil {
		return errors.New("evidence: nil reason registry")
	}
	if err := registry.Verify(code); err != nil {
		return fmt.Errorf("evidence: reason code %q is outside the closed registry: %w", code, err)
	}
	return &AdmissionError{ReasonCode: code, Detail: fmt.Sprintf(detail, args...)}
}

func verifyAdmissionReasonCodes(registry ledger.ReasonRegistry) error {
	if registry == nil {
		return errors.New("evidence: nil reason registry")
	}
	for _, code := range allAdmissionReasonCodes() {
		if err := registry.Verify(code); err != nil {
			return fmt.Errorf("evidence: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Intake packet, scope policy and staging records (GMS §2.2)
// ---------------------------------------------------------------------------

// SubmittedSegment is the external intake packet of one Host sealed Segment
// submission: the raw segment record bytes, the seal envelope bytes, the
// claimed canonical bytes, the submitter identity, its scope declaration and
// the optional external mutation idempotency key (Contract §13.2).
type SubmittedSegment struct {
	SegmentJSON []byte
	SealJSON    []byte
	Canonical   []byte
	Submitter   string
	Scope       AdmissionScope
	// IdempotencyKey is optional; when empty the commit derives the stable
	// default key "evidence-commit/<evidence_id>".
	IdempotencyKey string
}

// AdmissionScope is the submitter's scope declaration: the room it submits
// for and the exact scope profile ref it claims.
type AdmissionScope struct {
	RoomID          string
	ScopeProfileRef contract.VersionedRef
}

// ScopePolicy is the deployment-pinned admission scope: the closed set of
// rooms whose segments are admissible and the exact (id, version, digest)
// scope profiles accepted. Everything else is denied exactly — never by name
// or version alone (GMS §2.1 exact-CAS principle applied to authorization).
type ScopePolicy struct {
	AllowedRooms    []string
	AllowedProfiles []contract.VersionedRef
}

func (p ScopePolicy) roomAllowed(roomID string) bool {
	for _, room := range p.AllowedRooms {
		if room == roomID {
			return true
		}
	}
	return false
}

func (p ScopePolicy) profileAllowed(ref contract.VersionedRef) bool {
	for _, allowed := range p.AllowedProfiles {
		if allowed == ref {
			return true
		}
	}
	return false
}

// CheckDeclared authorizes the submitter's declaration against the segment's
// room before any seal is inspected (authorization gates integrity detail).
func (p ScopePolicy) CheckDeclared(registry ledger.ReasonRegistry, declared AdmissionScope, segmentRoomID string) error {
	if declared.RoomID == "" || declared.ScopeProfileRef.ID == "" ||
		declared.ScopeProfileRef.Version == "" || declared.ScopeProfileRef.Digest == "" {
		return newAdmissionError(registry, ReasonEvidenceProvenanceIncomplete,
			"scope declaration incomplete: room and exact scope profile ref are required")
	}
	if !digestShape.MatchString(declared.ScopeProfileRef.Digest) {
		return newAdmissionError(registry, ReasonSchemaEnumInvalid,
			"declared scope profile digest %q is not sha256:<64 lowercase hex>", declared.ScopeProfileRef.Digest)
	}
	if declared.RoomID != segmentRoomID {
		return newAdmissionError(registry, ReasonEvidenceScopeDenied,
			"declared room %q does not match the segment room %q", declared.RoomID, segmentRoomID)
	}
	if !p.roomAllowed(declared.RoomID) {
		return newAdmissionError(registry, ReasonEvidenceScopeDenied,
			"room %q is outside the admitted scope", declared.RoomID)
	}
	if !p.profileAllowed(declared.ScopeProfileRef) {
		return newAdmissionError(registry, ReasonEvidenceScopeDenied,
			"scope profile %s@%s is outside the admitted exact profiles",
			declared.ScopeProfileRef.ID, declared.ScopeProfileRef.Version)
	}
	return nil
}

// CheckSealed binds the declaration to the Evidence Seal's own scope profile:
// the sealed ref must equal the declared exact ref, not merely share an id.
func (p ScopePolicy) CheckSealed(registry ledger.ReasonRegistry, declared AdmissionScope, sealedProfile map[string]any) error {
	if sealedProfile == nil {
		return nil // non-settled records seal no scope profile
	}
	sealed, err := contract.ParseVersionedRef(sealedProfile)
	if err != nil {
		return newAdmissionError(registry, ReasonSchemaEnumInvalid,
			"sealed scope_profile_ref is not an exact VersionedRef: %v", err)
	}
	if sealed != declared.ScopeProfileRef {
		return newAdmissionError(registry, ReasonEvidenceScopeDenied,
			"declared scope profile %s@%s differs from the sealed scope profile %s@%s",
			declared.ScopeProfileRef.ID, declared.ScopeProfileRef.Version, sealed.ID, sealed.Version)
	}
	return nil
}

// StagingRecordView is the private read model of one staged submission.
// Staged ids are GMS-private: they never surface through the committed
// evidence readers and MUST NOT be used by projection or proposal callers —
// only committed/sealed EvidenceRefs are referenceable (GMS §2.2).
type StagingRecordView struct {
	StagedID         string
	Status           Status
	RoomID           string
	SegmentID        string
	TerminalState    string
	SubmissionDigest string
	Submitter        string
	IdempotencyKey   string // resolved at commit; "" while unknown
	ValidationReason string
	Events           []StateEvent
}

// ValidationOutcome is the Validate verdict: Valid=true (status validating,
// ready to commit) or a terminal rejected/inconclusive record carrying the
// closed reason code. Failed/aborted segments also carry the Host terminal
// audit verbatim as the non-ref outcome trail.
type ValidationOutcome struct {
	StagedID      string
	Status        Status
	Valid         bool
	ReasonCode    string
	Detail        string
	TerminalAudit map[string]any
	Candidate     *EvidenceCandidate
}

// EvidenceCandidate is the fully validated sealed evidence of one settled
// segment, awaiting the commit transaction. Every digest is recomputed from
// the submitted record — none is copied from the envelope unverified.
type EvidenceCandidate struct {
	EvidenceID        string // "evseal-<segment_id>" per the Host seal protocol
	Version           string
	EvidenceKind      string
	EvidenceDigest    string // SHA-256(JCS(evidence_seal_body))
	EvidenceSealID    string
	PathSealID        string
	PathSealDigest    string // SHA-256(JCS(path_seal_body))
	SegmentDigest     string // SHA-256(canonical segment bytes)
	RoomID            string
	SegmentID         string
	SegmentVersion    string
	SegmentRefJSON    map[string]any // derived Contract §7.5 exact ref
	CheckpointRefJSON map[string]any // derived Contract §7.6 exact ref
	CheckpointDigest  string
	PathIDs           []string
	PathDomains       []string
	CanonicalSegment  []byte // the evidence body stored at commit
}

// ---------------------------------------------------------------------------
// Exact-ref read models (Contract §7.5–§7.7)
// ---------------------------------------------------------------------------

// Schema versions of the Host/GMS DTOs this package parses.
const (
	schemaSealedSegment = "host.sealed-segment.v1"
	schemaSegmentSeal   = "host.segment-seal.v1"
	schemaSegmentRef    = "host.segment-ref.v1"
	schemaCheckpointRef = "host.checkpoint-ref.v1"
	schemaEvidenceRef   = contract.SchemaEvidenceRef
)

// digestShape is the Contract §6.1.3 digest form.
var digestShape = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// SegmentRefView is the Contract §7.5 SegmentRef read model.
type SegmentRefView struct {
	SchemaVersion   string
	RoomID          string
	SegmentID       string
	SegmentVersion  string
	SegmentDigest   string
	EvidenceSealRef contract.VersionedRef
	PathSealRef     contract.VersionedRef
}

// ParseSegmentRef parses one closed §7.5 ref (fail-closed on unknown fields,
// non-integer versions and malformed digests).
func ParseSegmentRef(registry ledger.ReasonRegistry, obj map[string]any) (SegmentRefView, error) {
	closed := []string{"schema_version", "room_id", "segment_id", "segment_version",
		"segment_digest", "evidence_seal_ref", "path_seal_ref"}
	if err := rejectUnknownFields(registry, obj, "segment-ref", closed); err != nil {
		return SegmentRefView{}, err
	}
	if sv, _ := contract.AsString(obj["schema_version"]); sv != schemaSegmentRef {
		return SegmentRefView{}, newAdmissionError(registry, ReasonSchemaVersionUnsupported,
			"SegmentRef: schema_version %q is not %q", sv, schemaSegmentRef)
	}
	view := SegmentRefView{SchemaVersion: schemaSegmentRef}
	var ok bool
	if view.RoomID, ok = contract.AsString(obj["room_id"]); !ok || view.RoomID == "" {
		return SegmentRefView{}, newAdmissionError(registry, ReasonSchemaRequiredMissing, "SegmentRef: room_id")
	}
	if view.SegmentID, ok = contract.AsString(obj["segment_id"]); !ok || view.SegmentID == "" {
		return SegmentRefView{}, newAdmissionError(registry, ReasonSchemaRequiredMissing, "SegmentRef: segment_id")
	}
	version, err := versionString(registry, obj, "SegmentRef", "segment_version")
	if err != nil {
		return SegmentRefView{}, err
	}
	view.SegmentVersion = version
	if view.SegmentDigest, ok = contract.AsString(obj["segment_digest"]); !ok || !digestShape.MatchString(view.SegmentDigest) {
		return SegmentRefView{}, newAdmissionError(registry, ReasonSchemaEnumInvalid, "SegmentRef: segment_digest")
	}
	for _, field := range []struct {
		name string
		dest *contract.VersionedRef
	}{{"evidence_seal_ref", &view.EvidenceSealRef}, {"path_seal_ref", &view.PathSealRef}} {
		raw, isObj := contract.AsObject(obj[field.name])
		if !isObj {
			return SegmentRefView{}, newAdmissionError(registry, ReasonSchemaRequiredMissing, "SegmentRef: %s", field.name)
		}
		ref, err := contract.ParseVersionedRef(raw)
		if err != nil {
			return SegmentRefView{}, newAdmissionError(registry, ReasonSchemaEnumInvalid, "SegmentRef: %s: %v", field.name, err)
		}
		*field.dest = ref
	}
	return view, nil
}

// ToJSON renders the view back into the canonical decoder model (JCS-ready).
func (v SegmentRefView) ToJSON() map[string]any {
	return map[string]any{
		"schema_version":    schemaSegmentRef,
		"room_id":           v.RoomID,
		"segment_id":        v.SegmentID,
		"segment_version":   json.Number(v.SegmentVersion),
		"segment_digest":    v.SegmentDigest,
		"evidence_seal_ref": versionedRefJSON(v.EvidenceSealRef),
		"path_seal_ref":     versionedRefJSON(v.PathSealRef),
	}
}

// CheckpointRefView is the Contract §7.6 CheckpointRef read model.
type CheckpointRefView struct {
	SchemaVersion      string
	RoomID             string
	Segment            SegmentRefView
	CheckpointID       string
	CheckpointSequence string
	CheckpointDigest   string
}

// ParseCheckpointRef parses one closed §7.6 ref.
func ParseCheckpointRef(registry ledger.ReasonRegistry, obj map[string]any) (CheckpointRefView, error) {
	closed := []string{"schema_version", "room_id", "segment_ref", "checkpoint_id",
		"checkpoint_sequence", "checkpoint_digest"}
	if err := rejectUnknownFields(registry, obj, "checkpoint-ref", closed); err != nil {
		return CheckpointRefView{}, err
	}
	if sv, _ := contract.AsString(obj["schema_version"]); sv != schemaCheckpointRef {
		return CheckpointRefView{}, newAdmissionError(registry, ReasonSchemaVersionUnsupported,
			"CheckpointRef: schema_version %q is not %q", sv, schemaCheckpointRef)
	}
	view := CheckpointRefView{SchemaVersion: schemaCheckpointRef}
	var ok bool
	if view.RoomID, ok = contract.AsString(obj["room_id"]); !ok || view.RoomID == "" {
		return CheckpointRefView{}, newAdmissionError(registry, ReasonSchemaRequiredMissing, "CheckpointRef: room_id")
	}
	segRaw, isObj := contract.AsObject(obj["segment_ref"])
	if !isObj {
		return CheckpointRefView{}, newAdmissionError(registry, ReasonSchemaRequiredMissing, "CheckpointRef: segment_ref")
	}
	seg, err := ParseSegmentRef(registry, segRaw)
	if err != nil {
		return CheckpointRefView{}, err
	}
	view.Segment = seg
	if view.CheckpointID, ok = contract.AsString(obj["checkpoint_id"]); !ok || view.CheckpointID == "" {
		return CheckpointRefView{}, newAdmissionError(registry, ReasonSchemaRequiredMissing, "CheckpointRef: checkpoint_id")
	}
	if view.CheckpointSequence, err = versionString(registry, obj, "CheckpointRef", "checkpoint_sequence"); err != nil {
		return CheckpointRefView{}, err
	}
	if view.CheckpointDigest, ok = contract.AsString(obj["checkpoint_digest"]); !ok || !digestShape.MatchString(view.CheckpointDigest) {
		return CheckpointRefView{}, newAdmissionError(registry, ReasonSchemaEnumInvalid, "CheckpointRef: checkpoint_digest")
	}
	return view, nil
}

// ToJSON renders the view back into the canonical decoder model.
func (v CheckpointRefView) ToJSON() map[string]any {
	return map[string]any{
		"schema_version":      schemaCheckpointRef,
		"room_id":             v.RoomID,
		"segment_ref":         v.Segment.ToJSON(),
		"checkpoint_id":       v.CheckpointID,
		"checkpoint_sequence": json.Number(v.CheckpointSequence),
		"checkpoint_digest":   v.CheckpointDigest,
	}
}

// EvidenceRefView is the Contract §7.7 EvidenceRef read model of committed
// evidence. SourceSegmentRef is the full §7.5 SegmentRef (the structured ref
// the Host seals carry), not the generic VersionedRef projection that
// internal/contract.ParseEvidenceRef models.
type EvidenceRefView struct {
	SchemaVersion    string
	EvidenceID       string
	Version          string
	EvidenceDigest   string
	CommitState      string
	EvidenceKind     string
	SourceSegmentRef *SegmentRefView
}

var evidenceCommitStates = map[string]bool{"committed": true, "sealed": true}

var evidenceKindSet = map[string]bool{
	"success_path": true, "failure_path": true, "recovery_path": true,
	"observation": true, "replay_result": true, "human_attestation": true,
}

// ParseEvidenceRef parses one closed §7.7 ref. A commit_state outside
// {committed, sealed} fails with EVIDENCE_NOT_COMMITTED — staged or
// uncommitted evidence is unreadable by construction.
func ParseEvidenceRef(registry ledger.ReasonRegistry, obj map[string]any) (EvidenceRefView, error) {
	closed := []string{"schema_version", "evidence_id", "version", "evidence_digest",
		"commit_state", "evidence_kind", "source_segment_ref"}
	if err := rejectUnknownFields(registry, obj, "evidence-ref", closed); err != nil {
		return EvidenceRefView{}, err
	}
	if sv, _ := contract.AsString(obj["schema_version"]); sv != schemaEvidenceRef {
		return EvidenceRefView{}, newAdmissionError(registry, ReasonSchemaVersionUnsupported,
			"EvidenceRef: schema_version %q is not %q", sv, schemaEvidenceRef)
	}
	view := EvidenceRefView{SchemaVersion: schemaEvidenceRef}
	var ok bool
	if view.EvidenceID, ok = contract.AsString(obj["evidence_id"]); !ok || view.EvidenceID == "" {
		return EvidenceRefView{}, newAdmissionError(registry, ReasonSchemaRequiredMissing, "EvidenceRef: evidence_id")
	}
	version, err := versionString(registry, obj, "EvidenceRef", "version")
	if err != nil {
		return EvidenceRefView{}, err
	}
	view.Version = version
	if view.EvidenceDigest, ok = contract.AsString(obj["evidence_digest"]); !ok || !digestShape.MatchString(view.EvidenceDigest) {
		return EvidenceRefView{}, newAdmissionError(registry, ReasonSchemaEnumInvalid, "EvidenceRef: evidence_digest")
	}
	if view.CommitState, ok = contract.AsString(obj["commit_state"]); !ok || !evidenceCommitStates[view.CommitState] {
		return EvidenceRefView{}, newAdmissionError(registry, ReasonEvidenceNotCommitted,
			"EvidenceRef %s: commit_state %v outside committed|sealed", view.EvidenceID, obj["commit_state"])
	}
	if view.EvidenceKind, ok = contract.AsString(obj["evidence_kind"]); !ok || !evidenceKindSet[view.EvidenceKind] {
		return EvidenceRefView{}, newAdmissionError(registry, ReasonSchemaEnumInvalid, "EvidenceRef: evidence_kind")
	}
	if raw, present := obj["source_segment_ref"]; present {
		segRaw, isObj := contract.AsObject(raw)
		if !isObj {
			return EvidenceRefView{}, newAdmissionError(registry, ReasonSchemaEnumInvalid, "EvidenceRef: source_segment_ref must be an object")
		}
		seg, err := ParseSegmentRef(registry, segRaw)
		if err != nil {
			return EvidenceRefView{}, err
		}
		view.SourceSegmentRef = &seg
	}
	return view, nil
}

// CommittedEvidence is the committed-only read model: the exact EvidenceRef
// plus the auditable provenance (Host seals, checkpoint, sealed paths,
// submitter, commit sequence) required to trace an EvidenceRef back to its
// Segment (GMS §12.2 Observability).
type CommittedEvidence struct {
	EvidenceID         string
	Version            string
	EvidenceDigest     string
	CommitState        string
	EvidenceKind       string
	Segment            SegmentRefView
	Checkpoint         CheckpointRefView
	EvidenceSealID     string
	EvidenceSealDigest string
	PathSealID         string
	PathSealDigest     string
	PathIDs            []string
	Submitter          string
	StagedID           string
	CommitSequence     uint64
}

// ---------------------------------------------------------------------------
// Host seal derivations (mirroring $FIX/validate_recorded.py — the FND-002
// protocol is the single authority; these recompute, never trust, the envelope)
// ---------------------------------------------------------------------------

// primaryEvidenceKind derives the sealed evidence kind from the sealed path
// domains: recovery over failure over success (validate_recorded.py
// _primary_evidence_kind).
func primaryEvidenceKind(domains map[string]bool) string {
	switch {
	case domains["recovery"]:
		return "recovery_path"
	case domains["failure"]:
		return "failure_path"
	default:
		return "success_path"
	}
}

// deriveSegmentRefJSON recomputes the Contract §7.5 SegmentRef of a settled
// segment: the seal ids follow the Host protocol ("evseal-"/"pathseal-" +
// segment id, version 1) and every digest is recomputed from the seal bodies.
func deriveSegmentRefJSON(seg map[string]any, registry ledger.ReasonRegistry, segmentDigest string) (map[string]any, error) {
	evidenceDigest, err := contract.DigestOf(seg["evidence_seal_body"])
	if err != nil {
		return nil, newAdmissionError(registry, ReasonNonIntegerNumber, "evidence seal body cannot enter the hashed core: %v", err)
	}
	pathDigest, err := contract.DigestOf(seg["path_seal_body"])
	if err != nil {
		return nil, newAdmissionError(registry, ReasonNonIntegerNumber, "path seal body cannot enter the hashed core: %v", err)
	}
	segmentID, _ := contract.AsString(seg["segment_id"])
	return map[string]any{
		"schema_version":    schemaSegmentRef,
		"room_id":           seg["room_id"],
		"segment_id":        seg["segment_id"],
		"segment_version":   seg["segment_version"],
		"segment_digest":    segmentDigest,
		"evidence_seal_ref": versionedRefJSON(contract.VersionedRef{ID: "evseal-" + segmentID, Version: "1", Digest: evidenceDigest}),
		"path_seal_ref":     versionedRefJSON(contract.VersionedRef{ID: "pathseal-" + segmentID, Version: "1", Digest: pathDigest}),
	}, nil
}

// checkpointPreimageJSON builds the digest-free checkpoint preimage of
// validate_recorded.py: (room_id, segment_id, segment_version, segment_digest,
// checkpoint_id, checkpoint_sequence, close_intent). The segment digest slot
// is filled by the caller with the recomputed digest — a seal-declared digest
// never enters the preimage.
func checkpointPreimageJSON(seg map[string]any, segmentDigest string) (map[string]any, error) {
	identity, ok := contract.AsObject(seg["checkpoint_identity"])
	if !ok {
		return nil, errors.New("checkpoint_identity missing")
	}
	closeObj, ok := contract.AsObject(seg["close"])
	if !ok {
		return nil, errors.New("close missing")
	}
	return map[string]any{
		"room_id":             seg["room_id"],
		"segment_id":          seg["segment_id"],
		"segment_version":     seg["segment_version"],
		"segment_digest":      segmentDigest,
		"checkpoint_id":       identity["checkpoint_id"],
		"checkpoint_sequence": identity["checkpoint_sequence"],
		"close_intent":        closeObj["close_intent"],
	}, nil
}

// ---------------------------------------------------------------------------
// Small decoder-model helpers shared by the gates
// ---------------------------------------------------------------------------

func versionedRefJSON(ref contract.VersionedRef) map[string]any {
	return map[string]any{"id": ref.ID, "version": json.Number(ref.Version), "digest": ref.Digest}
}

func rejectUnknownFields(registry ledger.ReasonRegistry, obj map[string]any, where string, closed []string) error {
	allowed := make(map[string]bool, len(closed))
	for _, field := range closed {
		allowed[field] = true
	}
	for key := range obj {
		if !allowed[key] {
			return newAdmissionError(registry, ReasonSchemaFieldUnknown,
				"%s: unknown field %q outside the closed set", where, key)
		}
	}
	return nil
}

// versionString extracts a required integer >= 1 field, keeping its exact
// decimal digits (no float round-trip).
func versionString(registry ledger.ReasonRegistry, obj map[string]any, where, field string) (string, error) {
	raw, present := obj[field]
	if !present {
		return "", newAdmissionError(registry, ReasonSchemaRequiredMissing, "%s: missing required field %q", where, field)
	}
	number, ok := raw.(json.Number)
	if !ok || !contract.IsIntegerNumber(number) {
		return "", newAdmissionError(registry, ReasonSchemaEnumInvalid, "%s: field %q must be an integer >= 1", where, field)
	}
	digits := number.String()
	if len(digits) == 0 || digits[0] == '-' || (digits[0] == '0' && len(digits) > 1) {
		return "", newAdmissionError(registry, ReasonSchemaEnumInvalid, "%s: field %q must be an integer >= 1", where, field)
	}
	if digits == "0" {
		return "", newAdmissionError(registry, ReasonSchemaEnumInvalid, "%s: field %q must be an integer >= 1", where, field)
	}
	return digits, nil
}

// sortedStrings returns a sorted copy (deterministic set comparisons).
func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
