// Package activation is the GMS-204 release/activation service
// (GMS §2.6/§2.9/§6, Contract §7.12–§7.14, §9.2, §10.6, §13.1/§13.5).
//
// An accepted ReleaseDecision never equals a released skill (Contract
// §10.6). Activation is the single protected transaction that turns one
// accepted decision over one frozen candidate into a released, active
// revision: it atomically commits the version assignment, the
// Candidate→Released mapping with its byte-equality proof, the
// skill-active head CAS, the Contract §7.13 ActivationEvent in the single
// global activation stream, the transactional outbox record and the proposal
// terminal event. Any failing step leaves no partial version, head, event,
// outbox record or proposal terminal state behind (Contract §13.1, GMS
// §2.9). Graph projection is NOT part of the transaction: the outbox record
// is the only graph-facing write and is delivered after commit (GMS-205).
//
// U2=A: the v1 reachable operation set is exactly activate / deactivate.
// `probation` is a reserved, unreachable state — the event constructor
// rejects any event_type outside {activate, deactivate} with
// PROBATION_UNSUPPORTED and no public entry point can produce it (GMS §6.4).
//
// Deactivation and reactivation are protected compensating operations
// (Contract §13.5): deactivation requires an explicit protected
// authorization and the current head as the exact CAS expectation;
// reactivation of a historical released revision requires a NEW accepted
// release decision and writes a NEW activate event — history is append-only
// and is never deleted or rewritten.
package activation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// ---------------------------------------------------------------------------
// Frozen vocabulary (Contract §7.12–§7.13, §9.2; CTR-005 authority schemas)
// ---------------------------------------------------------------------------

// Schema file names of the activation authority shapes
// ($FIX/schema/shared).
const (
	SchemaReleaseDecision   = "release-decision.schema.json"
	SchemaActivationEvent   = "activation-event.schema.json"
	SchemaDeactivationEvent = "deactivation-event.schema.json"
)

// SchemaReleaseMapping is the internal record shape of the release mapping
// (version assignment + Candidate→Released byte-equality proof, GMS §2.9).
// It is content-addressed and bound by the lineage-version head; the release
// LEDGER stays unused until a payload schema is frozen for it (its GMS-102
// whitelist is deliberately empty — fail-closed schema authority).
const SchemaReleaseMapping = "gms.release-mapping.v1"

// Closed event-type set of the v1 activation ledger (Contract §7.13/§9.2.6).
const (
	EventTypeActivate   = "activate"
	EventTypeDeactivate = "deactivate"
)

// activationEventTypes is the closed reachable event-type set. probation is
// a reserved enum value that MUST NOT be produced by any v1 implementation
// (Contract §9.2.6) — buildActivationEvent enforces this at the only event
// construction site.
var activationEventTypes = map[string]bool{
	EventTypeActivate:   true,
	EventTypeDeactivate: true,
}

// Decision outcomes (Contract §7.12) and proposal states (Contract §9.1).
const (
	OutcomeAccepted         = "accepted"
	StateActivationPending  = "activation_pending"
	StateReleased           = "released"
	ProjectionTargetRuntime = "runtime"
)

// activationStreamKey is the HeadActivationSequence key of the single global
// activation stream (GMS §2.6).
const activationStreamKey = "activation"

// Closed reason codes of the activation gates (frozen system registry,
// GMS §11.4 activation/release group). Codes already frozen elsewhere are
// aliased, never redefined.
const (
	// ReasonReleaseDecisionInvalid: the decision document is not a valid
	// §7.12 instance.
	ReasonReleaseDecisionInvalid = "RELEASE_DECISION_INVALID"
	// ReasonReleaseNotAccepted: only an accepted decision authorizes
	// activation (Contract §10.6).
	ReasonReleaseNotAccepted = "RELEASE_NOT_ACCEPTED"
	// ReasonCandidateReleasedBodyMismatch: candidate and released bodies are
	// not byte-identical (Q25-B/§16.4 #7).
	ReasonCandidateReleasedBodyMismatch = "CANDIDATE_RELEASED_BODY_MISMATCH"
	// ReasonCandidateNotFound: the candidate ref resolves to no committed
	// candidate binding.
	ReasonCandidateNotFound = "CANDIDATE_NOT_FOUND"
	// ReasonSkillKindInvalid: decision/candidate kind disagreement.
	ReasonSkillKindInvalid = "SKILL_KIND_INVALID"
	// ReasonLineageKindMismatch: a revision of a different kind into an
	// existing lineage (kind changes need a new lineage).
	ReasonLineageKindMismatch = "LINEAGE_KIND_MISMATCH"
	// ReasonPermissionCapExceeded: permission beyond the Host v1 cap,
	// re-verified at release time (GMS §6.1).
	ReasonPermissionCapExceeded = "PERMISSION_CAP_EXCEEDED"
	// ReasonProbationUnsupported: probation is reserved and unreachable.
	ReasonProbationUnsupported = "PROBATION_UNSUPPORTED"
	// ReasonDeactivationAuthorizationInvalid: missing/denied protected
	// deactivation authorization (Contract §9.2.3).
	ReasonDeactivationAuthorizationInvalid = "DEACTIVATION_AUTHORIZATION_INVALID"
	// ReasonReactivationAuthorizationRequired: reactivating a historical
	// released ref needs a new protected decision (Contract §9.2.7).
	ReasonReactivationAuthorizationRequired = "REACTIVATION_AUTHORIZATION_REQUIRED"
	// ReasonNoActiveHead: the lineage has no active revision to deactivate.
	ReasonNoActiveHead = "NO_ACTIVE_HEAD"
	// ReasonProposalInvalid: the origin proposal is missing or not an
	// ordinary §7.9 proposal.
	ReasonProposalInvalid = "PROPOSAL_INVALID"
	// ReasonArtifactBodyInvalid: the committed canonical body is unusable.
	ReasonArtifactBodyInvalid = "ARTIFACT_BODY_INVALID"
)

// Aliases of codes frozen in the ledger/contract layers (single source).
const (
	ReasonProposalStateConflict      = ledger.ReasonProposalStateConflict
	ReasonRefMismatch                = contract.ReasonRefMismatch
	ReasonDigestMismatch             = contract.ReasonDigestMismatch
	ReasonActiveHeadConflict         = ledger.ReasonActiveHeadConflict
	ReasonLineageVersionConflict     = ledger.ReasonLineageVersionConflict
	ReasonActivationSequenceConflict = ledger.ReasonActivationSequenceConflict
	ReasonActivationEventInvalid     = ledger.ReasonActivationEventInvalid
	ReasonIdempotencyConflict        = ledger.ReasonIdempotencyConflict
)

// Error is the registry-code activation failure; Detail is diagnostic only
// and never drives behavior.
type Error struct {
	ReasonCode string
	Detail     string
}

func (e *Error) Error() string { return "activation: " + e.ReasonCode + ": " + e.Detail }

func newError(code, format string, args ...any) *Error {
	return &Error{ReasonCode: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf returns the closed reason code of err ("" when nil); ledger,
// validation and contract-schema codes surface unchanged.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if ae, ok := err.(*Error); ok {
		return ae.ReasonCode
	}
	if code := ledger.ReasonOf(err); code != "" {
		return code
	}
	if code := validation.CodeOf(err); code != "" {
		return code
	}
	var se *contract.SchemaError
	if errors.As(err, &se) {
		return se.ReasonCode
	}
	return ""
}

// ---------------------------------------------------------------------------
// Ports (the GMS-203 decision and the GMS-202 candidate view plug in here)
// ---------------------------------------------------------------------------

// ReleaseDecision is the protected decision input port, satisfied by the
// GMS-203 evaluator's authoritative §7.12 Decision. The service re-validates
// the whole document against the authority schema on every activation — it
// never trusts the caller's in-memory verdict.
type ReleaseDecision interface {
	Outcome() string
	DecisionDigest() string
	Doc() map[string]any
}

// CandidateView is the frozen candidate input port, satisfied by the GMS-202
// immutable CandidateView. Identity only — no released version, no Runtime
// execution input.
type CandidateView interface {
	Ref() contract.CandidateArtifactRef
	BodyDigest() string
	CanonicalBody() []byte
}

// DeactivationAuthorizer resolves one protected deactivation authorization
// (Contract §9.2.3): production wiring validates the protected authorization
// record the deactivate event must reference. Unwired means every
// deactivation is denied — deactivate is never reachable without explicit
// authorization.
type DeactivationAuthorizer interface {
	AuthorizeDeactivation(auth contract.VersionedRef, lineageID string, skill contract.SkillArtifactRef) error
}

type denyAllDeactivations struct{}

func (denyAllDeactivations) AuthorizeDeactivation(contract.VersionedRef, string, contract.SkillArtifactRef) error {
	return errors.New("no deactivation authorizer wired; deactivation is denied")
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

// Config wires the activation service.
type Config struct {
	Store    ledger.Store
	Tx       ledger.TxManager
	Registry ledger.ReasonRegistry
	Gates    *validation.Gates
	// Proposals stages the ordinary proposal terminal event inside the
	// activation transaction (activation_pending -> released).
	Proposals *proposal.Service
	// DeactivationAuthorizer guards the protected deactivate operation.
	// Nil denies every deactivation (fail closed).
	DeactivationAuthorizer DeactivationAuthorizer
}

// ActivationService is the protected release/deactivation transaction
// service.
type ActivationService struct {
	store      ledger.Store
	mgr        ledger.TxManager
	registry   ledger.ReasonRegistry
	gates      *validation.Gates
	proposals  *proposal.Service
	authorizer DeactivationAuthorizer
}

// NewActivationService fails closed at construction time unless every
// reason code the service can emit exists in the digest-verified registry
// (Contract §13.7.1 R5).
func NewActivationService(cfg Config) (*ActivationService, error) {
	if cfg.Store == nil || cfg.Tx == nil || cfg.Registry == nil || cfg.Gates == nil || cfg.Proposals == nil {
		return nil, errors.New("activation: nil dependency in activation config")
	}
	for _, code := range []string{
		ReasonReleaseDecisionInvalid, ReasonReleaseNotAccepted,
		ReasonCandidateReleasedBodyMismatch, ReasonCandidateNotFound,
		ReasonSkillKindInvalid, ReasonLineageKindMismatch,
		ReasonPermissionCapExceeded, ReasonProbationUnsupported,
		ReasonDeactivationAuthorizationInvalid, ReasonReactivationAuthorizationRequired,
		ReasonNoActiveHead, ReasonProposalInvalid, ReasonArtifactBodyInvalid,
		ReasonProposalStateConflict, ReasonRefMismatch, ReasonDigestMismatch,
		ReasonActiveHeadConflict, ReasonLineageVersionConflict,
		ReasonActivationSequenceConflict, ReasonActivationEventInvalid,
		ReasonIdempotencyConflict,
	} {
		if err := cfg.Registry.Verify(code); err != nil {
			return nil, fmt.Errorf("activation: %w", err)
		}
	}
	authorizer := cfg.DeactivationAuthorizer
	if authorizer == nil {
		authorizer = denyAllDeactivations{}
	}
	return &ActivationService{
		store:      cfg.Store,
		mgr:        cfg.Tx,
		registry:   cfg.Registry,
		gates:      cfg.Gates,
		proposals:  cfg.Proposals,
		authorizer: authorizer,
	}, nil
}

// ActivateRequest is the one-shot release input: an accepted §7.12 decision
// over one frozen candidate, the target lineage, the frozen expected active
// head (nil = expect no active revision: new lineage, or cleared after a
// deactivation) and the ordinary §7.9 proposal document that originated the
// candidate.
type ActivateRequest struct {
	Decision     ReleaseDecision
	Candidate    CandidateView
	LineageID    string
	ExpectedHead *contract.SkillArtifactRef
	ProposalDoc  any
}

// ReleaseResult is the committed outcome of one activation.
type ReleaseResult struct {
	// ReleasedRef is the exact §7.3 ref of the now-released, active
	// revision (Reactivate returns the historical ref unchanged).
	ReleasedRef contract.SkillArtifactRef
	// ActivationSequence is the global activation stream position of the
	// event (single stream, monotone, never reused).
	ActivationSequence uint64
	// EventID and EventDigest identify the committed ActivationEvent;
	// EventDigest is the ledger payload digest (whole canonical document).
	EventID     string
	EventDigest string
	// OutboxKey is the fixed key of the transactional outbox record.
	OutboxKey string
	// PreviousActiveRef is the superseded revision, if any.
	PreviousActiveRef *contract.SkillArtifactRef
	// Replayed reports an idempotent replay of a recorded outcome (no new
	// version, event or outbox record was written).
	Replayed bool
}

// Activate runs the protected release transaction (GMS §6.2). Fail-closed
// guard order before the transaction:
//
//  1. the decision parses and re-validates against the §7.12 authority
//     schema with its x-digest recomputation, and the port digest matches
//     the document (RELEASE_DECISION_INVALID / DIGEST_MISMATCH /
//     REF_MISMATCH);
//  2. the outcome is accepted (RELEASE_NOT_ACCEPTED);
//  3. the decision covers EXACTLY the presented candidate ref
//     (REF_MISMATCH; kind disagreement SKILL_KIND_INVALID);
//  4. the candidate resolves to a committed candidate binding
//     (CANDIDATE_NOT_FOUND / REF_MISMATCH);
//  5. candidate and released bodies are byte-identical — the canonical body
//     digests to the declared body digest and equals the committed bytes
//     (CANDIDATE_RELEASED_BODY_MISMATCH);
//  6. the canonical body permissions stay under the Host v1 cap
//     (PERMISSION_CAP_EXCEEDED);
//  7. the target lineage exists, the revision kind matches the lineage kind
//     (LINEAGE_KIND_MISMATCH) and the origin is an ordinary §7.9 proposal in
//     activation_pending (PROPOSAL_INVALID / REF_MISMATCH /
//     PROPOSAL_STATE_CONFLICT; a proposal already in released is admitted
//     only as an idempotent replay of the same decision key, enforced inside
//     the transaction after the idempotency probe).
//
// Inside the single transaction the expected active head is re-derived from
// the authoritative activation ledger and must match the frozen expectation
// exactly (ACTIVE_HEAD_CONFLICT); version allocation, mapping, head CAS,
// event, outbox and proposal terminal event then commit atomically or not
// at all. The request is idempotent per decision key (Contract §13.2).
func (s *ActivationService) Activate(ctx context.Context, req ActivateRequest) (*ReleaseResult, error) {
	dv, err := s.parseDecision(req.Decision)
	if err != nil {
		return nil, err
	}
	if dv.outcome != OutcomeAccepted {
		return nil, newError(ReasonReleaseNotAccepted,
			"decision %s outcome %q: only an accepted decision authorizes activation, and even accepted != released (Contract §10.6)", dv.id, dv.outcome)
	}
	if req.Candidate == nil || req.Candidate.Ref().CandidateID == "" {
		return nil, newError(ReasonCandidateNotFound, "no frozen candidate view presented")
	}
	candRef := req.Candidate.Ref()
	if candRef != dv.candidate {
		return nil, newError(ReasonRefMismatch,
			"decision %s covers candidate %+v but the presented candidate is %+v", dv.id, dv.candidate.CandidateID+"("+dv.candidate.BodyDigest+")", candRef.CandidateID+"("+candRef.BodyDigest+")")
	}
	if dv.candidate.Kind != candRef.Kind {
		return nil, newError(ReasonSkillKindInvalid, "decision candidate kind %s != candidate kind %s (kind drift)", dv.candidate.Kind, candRef.Kind)
	}
	committed, found, err := s.findCandidate(candRef.CandidateID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, newError(ReasonCandidateNotFound,
			"candidate %s resolves to no committed candidate binding; unbound candidates cannot release", candRef.CandidateID)
	}
	if committed != candRef {
		return nil, newError(ReasonRefMismatch,
			"committed candidate %s differs from the presented ref (immutable binding violated)", candRef.CandidateID)
	}
	if err := s.verifyBodyEquality(candRef, req.Candidate.CanonicalBody()); err != nil {
		return nil, err
	}
	if req.LineageID == "" {
		return nil, newError(ReasonReleaseDecisionInvalid, "target lineage is required")
	}
	if err := s.checkLineageKind(req.LineageID, candRef.Kind); err != nil {
		return nil, err
	}
	proposalDoc, err := s.parseOriginProposal(candRef, req.ProposalDoc)
	if err != nil {
		return nil, err
	}
	state, _, err := s.proposals.CurrentState(ctx, proposalDoc.ID())
	if err != nil {
		return nil, err
	}
	if state != StateActivationPending && state != StateReleased {
		return nil, newError(ReasonProposalStateConflict,
			"release requires proposal %s in %s, current state is %q", proposalDoc.ID(), StateActivationPending, state)
	}
	// state==released is legal ONLY as an idempotent replay of the same
	// decision key — commitActivate enforces this after the idempotency
	// probe (a first-time request over a released proposal fails closed).
	return s.commitActivate(ctx, activateInput{
		decision:      dv,
		candidate:     candRef,
		lineageID:     req.LineageID,
		expected:      req.ExpectedHead,
		proposalID:    proposalDoc.ID(),
		proposal:      proposalDoc,
		proposalState: state,
	})
}

// DeactivateRequest is the protected compensating deactivation input. The
// authorization ref must be resolvable by the wired DeactivationAuthorizer
// for exactly this lineage and skill ref; SkillRef MUST be the current
// active revision (the exact CAS expectation).
type DeactivateRequest struct {
	LineageID     string
	SkillRef      contract.SkillArtifactRef
	Authorization contract.VersionedRef
}

// DeactivationResult is the committed outcome of one deactivation.
type DeactivationResult struct {
	DeactivatedRef     contract.SkillArtifactRef
	ActivationSequence uint64
	EventID            string
	EventDigest        string
	OutboxKey          string
	Replayed           bool
}

// Deactivate runs the protected deactivation transaction (Contract §9.2.3,
// §13.5): explicit authorization, exact current-head expectation, then ONE
// transaction appending the deactivate event, CAS-clearing the active head
// and enqueueing the outbox record. Deactivation NEVER deletes or rewrites
// artifacts, mapping records, events or Graph nodes — history stays
// append-only and the mapping/lineage heads are untouched.
func (s *ActivationService) Deactivate(ctx context.Context, req DeactivateRequest) (*DeactivationResult, error) {
	if req.LineageID == "" {
		return nil, newError(ReasonDeactivationAuthorizationInvalid, "lineage is required")
	}
	if req.SkillRef.LineageID != req.LineageID || req.SkillRef.ArtifactDigest == "" || req.SkillRef.Version == "" {
		return nil, newError(ReasonRefMismatch,
			"deactivation target must be the exact §7.3 ref of the current active revision of %s", req.LineageID)
	}
	if _, ok := req.SkillRef.VersionInt(); !ok {
		return nil, newError(ReasonRefMismatch, "deactivation target version %q is not an integer", req.SkillRef.Version)
	}
	if err := s.authorizer.AuthorizeDeactivation(req.Authorization, req.LineageID, req.SkillRef); err != nil {
		return nil, newError(ReasonDeactivationAuthorizationInvalid,
			"protected deactivation authorization %s denied for lineage %s: %v (Contract §9.2.3)", req.Authorization.ID, req.LineageID, err)
	}
	return s.commitDeactivate(ctx, req)
}

// ReactivateRequest reactivates one historical released revision
// (Contract §9.2.7): a NEW accepted decision over the ORIGINAL candidate of
// the release mapping, the exact released ref, and the frozen expected
// active head (nil = expect none).
type ReactivateRequest struct {
	Decision     ReleaseDecision
	ReleasedRef  contract.SkillArtifactRef
	ExpectedHead *contract.SkillArtifactRef
}

// Reactivate writes a NEW activate event for an already-released revision:
// a fresh global activation sequence, the head CAS back onto the released
// ref, the outbox record — and NO new version, NO new mapping, NO rewrite
// of any prior event (append-only compensating activation).
func (s *ActivationService) Reactivate(ctx context.Context, req ReactivateRequest) (*ReleaseResult, error) {
	if req.Decision == nil {
		return nil, newError(ReasonReactivationAuthorizationRequired,
			"reactivating a released revision requires a new protected release decision (Contract §9.2.7); old events are never rewritten")
	}
	dv, err := s.parseDecision(req.Decision)
	if err != nil {
		return nil, err
	}
	if dv.outcome != OutcomeAccepted {
		return nil, newError(ReasonReleaseNotAccepted, "reactivation decision %s outcome %q", dv.id, dv.outcome)
	}
	if req.ReleasedRef.LineageID == "" || req.ReleasedRef.ArtifactDigest == "" {
		return nil, newError(ReasonRefMismatch, "reactivation target must be an exact released §7.3 ref")
	}
	if _, ok := req.ReleasedRef.VersionInt(); !ok {
		return nil, newError(ReasonRefMismatch, "reactivation target version %q is not an integer", req.ReleasedRef.Version)
	}
	mapping, found, err := s.findMappingForVersion(req.ReleasedRef.LineageID, req.ReleasedRef.Version)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, newError(ReasonRefMismatch,
			"no committed release mapping for %s v%s; only released revisions can reactivate", req.ReleasedRef.LineageID, req.ReleasedRef.Version)
	}
	releasedRaw, _ := contract.AsObject(mapping["released_ref"])
	released, err := contract.ParseSkillArtifactRef(releasedRaw)
	if err != nil || released != req.ReleasedRef {
		return nil, newError(ReasonRefMismatch, "release mapping disagrees with the reactivation target")
	}
	if equal, _ := mapping["body_digest_equal"].(bool); !equal {
		return nil, newError(ReasonCandidateReleasedBodyMismatch, "release mapping lost its byte-equality proof")
	}
	candRaw, _ := contract.AsObject(mapping["candidate_ref"])
	mappedCand, err := contract.ParseCandidateArtifactRef(candRaw)
	if err != nil {
		return nil, newError(ReasonDigestMismatch, "release mapping candidate ref invalid: %v", err)
	}
	if dv.candidate != mappedCand {
		return nil, newError(ReasonRefMismatch,
			"the new decision covers candidate %s but the released revision was minted from %s", dv.candidate.CandidateID, mappedCand.CandidateID)
	}
	if err := s.verifyBodyEquality(mappedCand, nil); err != nil {
		return nil, err
	}
	return s.commitReactivate(ctx, reactivateInput{
		decision:  dv,
		released:  req.ReleasedRef,
		body:      mappedCand.BodyDigest,
		expected:  req.ExpectedHead,
		candidate: mappedCand,
	})
}

// ActiveRevision returns the current active revision of one lineage by
// replaying the authoritative activation ledger (GMS §2.6: the ledger is the
// complete history source). nil means no active revision.
func (s *ActivationService) ActiveRevision(ctx context.Context, lineageID string) (*contract.SkillArtifactRef, error) {
	state, err := s.replayLineage(lineageID)
	if err != nil {
		return nil, err
	}
	if state.active == nil {
		return nil, nil
	}
	ref := *state.active
	return &ref, nil
}

// ---------------------------------------------------------------------------
// Decision guards (the GMS-204 refactor seam)
// ---------------------------------------------------------------------------

// decisionView is the strictly parsed §7.12 decision.
type decisionView struct {
	id        string
	version   string
	digest    string
	outcome   string
	candidate contract.CandidateArtifactRef
	doc       map[string]any
}

// parseDecision re-derives everything from the document; nothing trusts the
// in-memory verdict of the caller. The authority schema re-validates the
// whole shape AND recomputes decision_digest over the frozen x-digest
// preimage, so a forged decision fails closed before any guard runs.
func (s *ActivationService) parseDecision(dec ReleaseDecision) (*decisionView, error) {
	if dec == nil || dec.Doc() == nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "no release decision presented")
	}
	doc, ok := contract.AsObject(dec.Doc())
	if !ok {
		return nil, newError(ReasonReleaseDecisionInvalid, "decision document is not a JSON object")
	}
	if err := s.gates.ValidateInstance(doc, SchemaReleaseDecision); err != nil {
		if validation.CodeOf(err) == ReasonDigestMismatch {
			return nil, newError(ReasonDigestMismatch, "decision_digest does not recompute over the frozen preimage: %v", err)
		}
		return nil, newError(ReasonReleaseDecisionInvalid, "decision failed the §7.12 authority schema: %v", err)
	}
	id, _ := contract.AsString(doc["decision_id"])
	version := ""
	if n, isNum := doc["decision_version"]; isNum {
		version = jsonDigits(n)
	}
	digest, _ := contract.AsString(doc["decision_digest"])
	if dec.DecisionDigest() != digest {
		return nil, newError(ReasonRefMismatch,
			"decision port digest %s disagrees with the document decision_digest %s", dec.DecisionDigest(), digest)
	}
	outcome, _ := contract.AsString(doc["outcome"])
	candRaw, _ := contract.AsObject(doc["candidate_ref"])
	cand, err := contract.ParseCandidateArtifactRef(candRaw)
	if err != nil {
		return nil, newError(ReasonReleaseDecisionInvalid, "decision candidate_ref is not an exact §7.4 ref: %v", err)
	}
	return &decisionView{id: id, version: version, digest: digest, outcome: outcome, candidate: cand, doc: doc}, nil
}

// verifyBodyEquality proves the Candidate→Released byte-equality mapping
// (GMS §3.7, Contract §16.4 #7): the canonical body MUST digest to the
// declared body digest, the committed content-addressed bytes MUST exist and
// be byte-identical, and the permissions stay under the Host v1 cap (the
// release-time re-verification of GMS §6.1). body == nil verifies only the
// committed bytes under the declared digest (reactivation).
func (s *ActivationService) verifyBodyEquality(candRef contract.CandidateArtifactRef, body []byte) error {
	if body != nil {
		if digest := contract.DigestBytes(body); digest != candRef.BodyDigest {
			return newError(ReasonCandidateReleasedBodyMismatch,
				"candidate canonical body digests to %s but the ref declares %s (released and candidate bodies must be byte-identical)", digest, candRef.BodyDigest)
		}
	}
	stored, ok, err := s.store.Get(candRef.BodyDigest)
	if err != nil {
		return err
	}
	if !ok {
		return newError(ReasonCandidateReleasedBodyMismatch,
			"canonical body %s resolves to no committed content (candidate and released bodies must be byte-identical)", candRef.BodyDigest)
	}
	if digest := contract.DigestBytes(stored); digest != candRef.BodyDigest {
		return newError(ReasonCandidateReleasedBodyMismatch, "committed body under %s digests to %s", candRef.BodyDigest, digest)
	}
	if body != nil && string(stored) != string(body) {
		return newError(ReasonCandidateReleasedBodyMismatch,
			"committed body bytes differ from the candidate canonical body (byte equality, not digest aliasing)")
	}
	if body != nil {
		value, err := contract.ParseJSONStrict(stored)
		if err != nil {
			return newError(ReasonArtifactBodyInvalid, "committed canonical body is not valid JSON: %v", err)
		}
		if err := s.gates.CheckPermissionsWalked(value); err != nil {
			return err
		}
	}
	return nil
}

// findCandidate resolves one committed candidate binding (GMS §2.4).
func (s *ActivationService) findCandidate(candidateID string) (contract.CandidateArtifactRef, bool, error) {
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
			return contract.CandidateArtifactRef{}, false, newError(ReasonDigestMismatch, "candidate %s payload unresolved", candidateID)
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			return contract.CandidateArtifactRef{}, false, newError(ReasonDigestMismatch, "candidate %s payload invalid: %v", candidateID, err)
		}
		obj, _ := contract.AsObject(value)
		ref, err := contract.ParseCandidateArtifactRef(obj)
		if err != nil {
			return contract.CandidateArtifactRef{}, false, newError(ReasonDigestMismatch, "candidate %s payload invalid: %v", candidateID, err)
		}
		return ref, true, nil
	}
	return contract.CandidateArtifactRef{}, false, nil
}

// checkLineageKind rejects a revision of a different kind into an existing
// lineage: kind changes need a new lineage (derive_lineage), never a silent
// drift. When the lineage-version head's mapping record does not resolve
// (a racing writer consumed the slot without leaving content) the lineage
// kind cannot be derived; the kind guard is then carried by the candidate /
// decision agreement and the version allocator's CAS — the check is skipped
// rather than failing the whole lineage forever on a torn predecessor.
func (s *ActivationService) checkLineageKind(lineageID, kind string) error {
	_, digest, ok, err := s.store.GetHead(ledger.HeadLineageVersion, lineageID)
	if err != nil || !ok {
		return err
	}
	mapping, err := s.resolveMapping(digest)
	if err != nil {
		return nil // torn predecessor slot: kind enforced by the release itself
	}
	lineageKind, _ := contract.AsString(mapping["kind"])
	if lineageKind != kind {
		return newError(ReasonLineageKindMismatch,
			"lineage %s is %s; a %s revision needs a new lineage (derive_lineage), not a silent kind change", lineageID, lineageKind, kind)
	}
	return nil
}

// parseOriginProposal binds the candidate to its ordinary §7.9 origin: the
// document MUST parse strictly and its exact ref MUST equal the candidate's
// origin ref (provenance). Merge-origin candidates are not activatable in
// the ordinary release path (the derived activation belongs to the merge
// subsystem).
func (s *ActivationService) parseOriginProposal(candRef contract.CandidateArtifactRef, doc any) (*proposal.Proposal, error) {
	if candRef.OriginType != "skill_proposal" {
		return nil, newError(ReasonProposalInvalid,
			"candidate origin %q: the ordinary release transaction activates skill_proposal candidates only", candRef.OriginType)
	}
	if doc == nil {
		return nil, newError(ReasonProposalInvalid, "the ordinary proposal document is required to write the terminal released event")
	}
	parsed, err := s.proposals.ParseProposal(doc)
	if err != nil {
		return nil, newError(ReasonProposalInvalid, "origin proposal does not parse as a strict gms.skill-proposal.v1 document: %v", err)
	}
	if parsed.Ref() != candRef.OriginRef {
		return nil, newError(ReasonRefMismatch,
			"candidate origin ref %+v does not match the presented proposal %+v", candRef.OriginRef, parsed.Ref())
	}
	return parsed, nil
}

// jsonDigits returns the exact decimal digits of a decoder-model number.
func jsonDigits(v any) string {
	if n, ok := v.(json.Number); ok {
		return string(n)
	}
	return ""
}
