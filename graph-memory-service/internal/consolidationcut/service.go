// Package consolidationcut implements the Room-scoped Consolidation Cut job
// seam (SC-4.1..4.7): Room-serial freeze, immutable manifests, the frozen
// cut_job state machine, cooperative cancellation, and idempotent triggers.
package consolidationcut

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

// ErrCutNotImplemented is kept as the red-stage detector for the PG-20
// contract tests; no green-path code returns it.
var ErrCutNotImplemented = errors.New("consolidation cut not implemented")

var (
	ErrCutNotFound            = errors.New("consolidation cut not found")
	ErrIdempotencyKeyRequired = errors.New("cut trigger requires an idempotency key")
	// ErrIdempotencyConflict is CUT_IDEMPOTENCY_CONFLICT: same key, changed body.
	ErrIdempotencyConflict = errors.New("cut idempotency conflict: same key with changed body")
	// ErrIllegalTransition rejects moves absent from the frozen cut_job
	// state machine bundle.
	ErrIllegalTransition = errors.New("illegal cut job stage transition")
	// ErrGuardNotSatisfied rejects a legal edge whose frozen guard fact was
	// not provided (e.g. diagnosis_verified, receipts_complete).
	ErrGuardNotSatisfied = errors.New("cut job transition guard not satisfied")
	// ErrGuardAuthorityUnavailable classifies an authority READ failure
	// (ledger/transport/context errors) as transient infrastructure: the
	// transition still fails closed, but unlike ErrGuardNotSatisfied it is
	// not a deterministic verdict about the frozen fact and must not be
	// permanently recorded as one.
	ErrGuardAuthorityUnavailable = errors.New("cut guard authority is unavailable")
	// ErrRoomOccupied rejects re-entry seams (resume, rediagnose) that would
	// make a room hold two active cuts at once (SC-4.1 room-serial freeze).
	ErrRoomOccupied = errors.New("cut room is occupied by another in-flight cut")
	// ErrRediagnoseRequired rejects partially_failed→diagnosing via the raw
	// transition: governance re-entry must go through Rediagnose (SC-4.2).
	ErrRediagnoseRequired = errors.New("partially_failed re-diagnosis requires the rediagnose seam")
	// ErrJobVersionConflict is the optimistic CAS failure on JobVersion.
	ErrJobVersionConflict = errors.New("cut job version CAS conflict")
	// ErrNotCancelable rejects cancel requests whose source stage has no
	// cancel edge in the frozen bundle.
	ErrNotCancelable = errors.New("cut job stage is not cooperatively cancelable")
	// ErrNotResumable rejects resume against any stage other than failed.
	ErrNotResumable = errors.New("cut job is not resumable; only failed jobs resume")
	// ErrReceiptCoverageIncomplete rejects a freeze whose commit receipts do
	// not cover the sealed segments exactly 1:1 (SC-4.4).
	ErrReceiptCoverageIncomplete = errors.New("commit receipts do not cover the sealed segments exactly")
	// ErrInvalidMode rejects trigger modes outside threshold|force.
	ErrInvalidMode = errors.New("cut trigger mode must be threshold or force")
	// ErrManifestContractViolation rejects an injected Freezer manifest that
	// does not satisfy the frozen manifest contract.
	ErrManifestContractViolation = errors.New("freezer manifest violates the frozen contract")
)

type CutID string
type RoomID string
type SegmentID string
type ReceiptID string

type Mode string

const (
	ModeThreshold Mode = "threshold"
	ModeForce     Mode = "force"
)

type Stage string

const (
	StageQueued               Stage = "queued"
	StageFreezing             Stage = "freezing"
	StageFrozen               Stage = "frozen"
	StageDiagnosing           Stage = "diagnosing"
	StageConsolidating        Stage = "consolidating"
	StageConsolidatingPartial Stage = "consolidating_partial"
	StageReplaying            Stage = "replaying"
	StageActivating           Stage = "activating"
	StageCompleted            Stage = "completed"
	StagePartiallyFailed      Stage = "partially_failed"
	StageFailed               Stage = "failed"
	StageCancelling           Stage = "cancelling"
	StageCancelled            Stage = "cancelled"
	StagePartiallyCancelled   Stage = "partially_cancelled"
)

// transitionEdges is the frozen cut_job.state.json v2 transition table
// (contract_ref SC-4.7, SC-4.1, SC-4.3, SC-4.2). Implementations must not add
// edges; contract gaps go back to PG-00 for versioning.
var transitionEdges = map[Stage]map[Stage]bool{
	StageQueued: {StageFreezing: true, StageCancelled: true},
	StageFreezing: {
		StageFrozen: true, StageFailed: true, StageCancelled: true,
	},
	StageFrozen: {StageDiagnosing: true, StageCancelling: true},
	StageDiagnosing: {
		StageConsolidating: true, StagePartiallyFailed: true, StageFailed: true, StageCancelling: true,
	},
	StagePartiallyFailed: {
		// Governance re-entry only via explicit rediagnose (Rediagnose).
		StageConsolidatingPartial: true, StageDiagnosing: true,
	},
	StageConsolidatingPartial: {StagePartiallyFailed: true, StageFailed: true},
	StageConsolidating:        {StageReplaying: true, StageCompleted: true, StageFailed: true, StageCancelling: true},
	StageReplaying:            {StageActivating: true, StageCompleted: true, StageFailed: true, StageCancelling: true},
	StageActivating:           {StageCompleted: true, StageCancelling: true, StageFailed: true},
	StageFailed: {
		// Resume re-enters the recorded failed-from stage.
		StageDiagnosing: true, StageConsolidating: true, StageReplaying: true, StageActivating: true,
	},
	StageCancelling: {StageCancelled: true, StagePartiallyCancelled: true},
}

// terminalStages and the in-flight set follow the frozen bundle: terminal
// [completed, cancelled, partially_cancelled, partially_failed]; failed is
// resumable, not in-flight.
var terminalStages = map[Stage]bool{
	StageCompleted: true, StageCancelled: true, StagePartiallyCancelled: true, StagePartiallyFailed: true,
}

func inFlight(stage Stage) bool {
	switch stage {
	case StageQueued, StageFreezing, StageFrozen, StageDiagnosing,
		StageConsolidating, StageConsolidatingPartial, StageReplaying,
		StageActivating, StageCancelling:
		return true
	}
	return false
}

// resumableFailedFrom reports whether a failed job may re-enter the given
// stage: exactly the stages a job can actually FAIL from — the transition
// table's failed-edge sources plus freezing (the internal frozen-guard
// failure point recorded by Trigger's failLocked path). This is deliberately
// NARROWER than inFlight: a corrupted FailedFrom of queued/frozen/cancelling
// (in-flight but not a legal failure origin) must never be resumed or
// restored.
func resumableFailedFrom(stage Stage) bool {
	switch stage {
	case StageFreezing, StageDiagnosing, StageConsolidatingPartial,
		StageConsolidating, StageReplaying, StageActivating:
		return true
	}
	return false
}

// reasonDiagnosisFailed is the canonical reason for the SC-4.3 diagnosis
// failure path (diagnosing→partially_failed).
const reasonDiagnosisFailed = "DIAGNOSIS_FAILED_OR_INCONCLUSIVE"

type SpaceResultKind string

const (
	SpaceResultPending   SpaceResultKind = "pending"
	SpaceResultPublished SpaceResultKind = "published"
	SpaceResultDuplicate SpaceResultKind = "duplicate"
	SpaceResultNoChange  SpaceResultKind = "no_change"
	SpaceResultFailed    SpaceResultKind = "failed"
)

type SpaceScope struct {
	SpaceID               domain.SpaceID
	Scope                 domain.SpaceScope
	EvidenceBatchIDs      []domain.BatchID
	ProjectionHeadVersion *domain.ProjectionVersion
	QueryWatermark        *int64
}

type EvidenceCommitReceipt struct {
	ReceiptID   ReceiptID
	SegmentID   SegmentID
	BatchIDs    []domain.BatchID
	BatchDigest string
}

type PolicyRevisions struct {
	DiagnosisPolicyRevision string
	ReplayPolicyRevision    string
	AggregationPolicyDigest string
}

// Manifest is the immutable Cut input formed only by the server-side freeze.
type Manifest struct {
	CutID                  CutID
	TenantID               domain.TenantID
	RoomID                 RoomID
	Mode                   Mode
	TriggerSource          string
	IdempotencyKey         string
	PreviousCutID          *CutID
	RoomSequenceWatermark  int64
	SealedSegmentIDs       []SegmentID
	SpaceScopes            []SpaceScope
	CutDigest              string
	PolicyRevisions        PolicyRevisions
	EvidenceCommitReceipts []EvidenceCommitReceipt
}

type SpaceResult struct {
	SpaceID          domain.SpaceID
	Result           SpaceResultKind
	PublishedVersion *domain.ProjectionVersion
	Reason           string
}

type Job struct {
	CutID                 CutID
	TenantID              domain.TenantID
	RoomID                RoomID
	Stage                 Stage
	JobVersion            int64
	CutDigest             string
	Outcome               SpaceResultKind
	RoomSequenceWatermark *int64
	SpaceResults          []SpaceResult
	FailureReasons        []string
	// DiagnosisPolicyRevision records the active diagnosis revision; explicit
	// rediagnose advances it without touching the immutable manifest.
	DiagnosisPolicyRevision string
	// FailedFrom records the stage a failed job resumes into.
	FailedFrom Stage
	// LastTransitionEvidence records the authority-resolved evidence refs
	// that admitted the most recent guarded transition (audit trail).
	LastTransitionEvidence TransitionEvidence
	// NewDiagnosisRunRef binds the governance-issued re-diagnosis run that
	// re-entered diagnosing (SC-4.2).
	NewDiagnosisRunRef string
	UpdatedAt          time.Time
}

type TriggerRequest struct {
	TenantID       domain.TenantID
	PrincipalID    domain.PrincipalID
	RoomID         RoomID
	Mode           Mode
	TriggerSource  string
	IdempotencyKey string
}

type CancelRequest struct {
	TenantID           domain.TenantID
	PrincipalID        domain.PrincipalID
	CutID              CutID
	ExpectedJobVersion int64
}

type ResumeRequest = CancelRequest

type RediagnoseRequest struct {
	TenantID                domain.TenantID
	PrincipalID             domain.PrincipalID
	CutID                   CutID
	ExpectedJobVersion      int64
	DiagnosisPolicyRevision string
	// NewDiagnosisRunRef binds the governance-issued re-diagnosis run (and
	// its fresh immutable annotation revision) to this transition atomically;
	// SC-4.2 rediagnosis is a new run, never a resume of the old one.
	NewDiagnosisRunRef string
}

// TransitionEvidence carries references to facts owned by the respective
// authorities (diagnosis, projection, proposal/replay, promotion CAS). The
// references are meaningless without a configured GuardAuthority that
// resolves them against those authorities; callers can no longer self-report
// guard booleans (SC-4.1/4.3/4.8).
type TransitionEvidence struct {
	QuotaGrantRef           string
	ReceiptsDigest          string
	DiagnosisAnnotationRef  string
	DiagnosisPolicyRevision string
	ProjectionRecordDigest  string
	ProposalRef             string
	ReplayResultRef         string
	CASBaseRevision         string
	// AuthorityFactsDigest binds the guarded admission to the authority fact
	// snapshot the service resolved and re-verified (Phase B resolve → Phase C
	// re-resolve must confirm the identical digest). It is stamped by the
	// service and never read from the caller's request.
	AuthorityFactsDigest string
}

// GuardConfirmation is the authority's snapshot of the facts behind one guard
// resolution (R3): FactsDigest canonically covers the resolved fact set, so
// the service can re-resolve the guard at apply time and fail closed on any
// drift — a fact revoked or superseded between the two CAS-checked windows
// can no longer admit the transition.
type GuardConfirmation struct {
	FactsDigest string
	Facts       map[string]string
}

// ConfirmFacts renders an authority's resolved fact set into a GuardConfirmation
// with a canonical digest: the resolving authority method's name, then each
// sorted fact as length-prefixed key and value, so no key or value containing
// NUL, "=", or any other delimiter can make two different fact sets encode to
// the same input. Two calls confirm the same facts if and only if the facts
// match, and the service recomputes this digest over the reported facts at
// every resolution — an authority's reported digest must cover its own facts.
func ConfirmFacts(guardName string, facts map[string]string) GuardConfirmation {
	keys := make([]string, 0, len(facts))
	for key := range facts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var encoded strings.Builder
	encoded.WriteString(guardName)
	for _, key := range keys {
		value := facts[key]
		encoded.WriteString("\x00")
		encoded.WriteString(strconv.Itoa(len(key)))
		encoded.WriteString(":")
		encoded.WriteString(key)
		encoded.WriteString(strconv.Itoa(len(value)))
		encoded.WriteString(":")
		encoded.WriteString(value)
	}
	sum := sha256.Sum256([]byte(encoded.String()))
	return GuardConfirmation{FactsDigest: "sha256:" + hex.EncodeToString(sum[:]), Facts: facts}
}

type TransitionRequest struct {
	TenantID           domain.TenantID
	CutID              CutID
	ExpectedJobVersion int64
	To                 Stage
	Reason             string
	Evidence           TransitionEvidence
	SpaceResults       []SpaceResult
}

// GuardAuthority resolves transition-guard evidence against the authorities
// that own the facts. A nil authority fails every guarded edge closed; a
// reference the authority does not recognize fails closed with
// ErrGuardNotSatisfied. Implementations are only ever invoked without the cut
// service's lock held (the service resolves and re-resolves the guard between
// two CAS-checked lock windows and never consults an authority under the
// lock), so adapters may call back into the cut service itself — e.g. reading
// GetManifest — without self-deadlocking; every confirmation's digest must
// canonically cover its facts (ConfirmFacts), and the re-resolve must confirm
// the identical digest or the transition fails closed.
//
// Every guarded admission is applied under an authority-side fact
// RESERVATION: ReserveFacts is the apply-time serialization point — while the
// reservation is held, mutation of the facts behind any guard (revocation,
// supersession) must serialize behind ReleaseFacts, so a fact cannot be
// revoked between the final guard validation and the transition commit (the
// apply-time authority CAS; R3). The reservation is taken and released
// without the cut service's lock held.
type GuardAuthority interface {
	QuotaGranted(ctx context.Context, tenantID domain.TenantID, cutID CutID, grantRef string) (bool, GuardConfirmation, error)
	ReceiptsComplete(ctx context.Context, tenantID domain.TenantID, cutID CutID, receiptsDigest string) (bool, GuardConfirmation, error)
	DiagnosisVerified(ctx context.Context, tenantID domain.TenantID, cutID CutID, annotationRef, policyRevision string) (bool, GuardConfirmation, error)
	ProjectionComplete(ctx context.Context, tenantID domain.TenantID, cutID CutID, recordDigest string) (bool, GuardConfirmation, error)
	ProposalRefs(ctx context.Context, tenantID domain.TenantID, cutID CutID) ([]string, GuardConfirmation, error)
	ReplayPassed(ctx context.Context, tenantID domain.TenantID, cutID CutID, replayResultRef string) (bool, GuardConfirmation, error)
	CASBaseRevisionMatches(ctx context.Context, tenantID domain.TenantID, cutID CutID, baseRevision string) (bool, GuardConfirmation, error)
	SideEffectsCompleted(ctx context.Context, tenantID domain.TenantID, cutID CutID) (bool, GuardConfirmation, error)
	// ReserveFacts pins the authority's facts for one transition apply: the
	// final guard validation runs under the reservation, and fact mutations
	// must block until the matching ReleaseFacts, so the confirmed digest
	// cannot be revoked between that validation and the commit.
	ReserveFacts(ctx context.Context, tenantID domain.TenantID, cutID CutID) error
	// ReleaseFacts drops a reservation taken by ReserveFacts and lets
	// blocked fact mutations proceed.
	ReleaseFacts(ctx context.Context, tenantID domain.TenantID, cutID CutID)
}

// Repository is the consumer-owned persistence seam; the in-memory authority
// below drives the current increment and PG-50A wires durable persistence in
// through this interface.
type Repository interface {
	Create(context.Context, TriggerRequest) (Job, bool, error)
	Job(context.Context, domain.TenantID, CutID) (Job, error)
	Manifest(context.Context, domain.TenantID, CutID) (Manifest, error)
	RequestCancel(context.Context, CancelRequest) (Job, error)
	Resume(context.Context, ResumeRequest) (Job, error)
	RequestRediagnosis(context.Context, RediagnoseRequest) (Job, error)
}

// Freezer creates the server-side atomic Room/evidence snapshot.
type Freezer interface {
	Freeze(context.Context, TriggerRequest) (Manifest, error)
}

// AuditEntry is one append-only governance/fact record for a cut request or
// transition; trigger conflicts and idempotency conflicts are auditable here.
type AuditEntry struct {
	At       time.Time
	Kind     string
	TenantID domain.TenantID
	RoomID   RoomID
	CutID    CutID
	Actor    domain.PrincipalID
	Detail   string
}

// Service is the Room-scoped Cut job authority. With nil Repository/Freezer it
// runs on the built-in in-memory store and the deterministic fixture freezer
// (the tracer-bullet evidence source); injected adapters replace either.
type Service struct {
	repository Repository
	freezer    Freezer
	guard      GuardAuthority

	mu           sync.Mutex
	jobs         map[CutID]*Job
	manifests    map[CutID]*Manifest
	idempotency  map[string]CutID
	requests     map[string]TriggerRequest
	roomInFlight map[domain.TenantID]map[RoomID]CutID
	roomLastCut  map[domain.TenantID]map[RoomID]CutID
	// roomReservations pins (tenant, room) across the unlocked freeze phase,
	// so Room-serial survives the lock split: a second same-Room trigger is
	// rejected while the first is still freezing.
	roomReservations map[domain.TenantID]map[RoomID]bool
	audit            []AuditEntry
	now              func() time.Time
	// transitionHookAfterResolve fires right after the Phase B resolution
	// (before the reservation); transitionHookUnderReservation fires after
	// the final validation, while the fact reservation is held. They are
	// deterministic interleaving hooks for the reservation regression tests
	// (same-package only) and are nil in production.
	transitionHookAfterResolve     func()
	transitionHookUnderReservation func()
}

func NewService(repository Repository, freezer Freezer) *Service {
	return &Service{
		repository:       repository,
		freezer:          freezer,
		jobs:             map[CutID]*Job{},
		manifests:        map[CutID]*Manifest{},
		idempotency:      map[string]CutID{},
		requests:         map[string]TriggerRequest{},
		roomInFlight:     map[domain.TenantID]map[RoomID]CutID{},
		roomLastCut:      map[domain.TenantID]map[RoomID]CutID{},
		roomReservations: map[domain.TenantID]map[RoomID]bool{},
		now:              time.Now,
	}
}

// SetGuardAuthority wires the trusted guard-evidence resolver. Until it is
// set, every guarded transition edge fails closed: guard facts come from the
// owning authorities, never from the request.
func (s *Service) SetGuardAuthority(authority GuardAuthority) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.guard = authority
}

// AuditTrail returns a copy of the audit records for one tenant.
func (s *Service) AuditTrail(tenantID domain.TenantID) []AuditEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]AuditEntry, 0, len(s.audit))
	for _, entry := range s.audit {
		if entry.TenantID == tenantID {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (s *Service) auditLocked(kind string, tenantID domain.TenantID, roomID RoomID, cutID CutID, actor domain.PrincipalID, detail string) {
	s.audit = append(s.audit, AuditEntry{
		At: s.now(), Kind: kind, TenantID: tenantID, RoomID: roomID,
		CutID: cutID, Actor: actor, Detail: detail,
	})
}

func (s *Service) Trigger(ctx context.Context, req TriggerRequest) (Job, bool, error) {
	if req.IdempotencyKey == "" {
		return Job{}, false, ErrIdempotencyKeyRequired
	}
	if req.Mode != ModeThreshold && req.Mode != ModeForce {
		return Job{}, false, ErrInvalidMode
	}
	key := idempotencyKey(req.TenantID, req.RoomID, req.TriggerSource, req.IdempotencyKey)

	// Phase 1 (locked): idempotency replay/conflict, then the Room-serial
	// admission with a reservation spanning the unlocked freeze. No freeze
	// work happens under the lock, so one room's slow receipt wait never
	// blocks unrelated rooms — while a same-Room competitor is still
	// rejected (SC-4.1), because the reservation outlives the lock window.
	s.mu.Lock()
	if existing, ok := s.idempotency[key]; ok {
		stored := s.requests[key]
		if sameTriggerBody(stored, req) {
			s.auditLocked("trigger_replay", req.TenantID, req.RoomID, existing, req.PrincipalID, "idempotent replay")
			job := cloneJob(*s.jobs[existing])
			s.mu.Unlock()
			return job, true, nil
		}
		s.auditLocked("trigger_idempotency_conflict", req.TenantID, req.RoomID, existing, req.PrincipalID, "same key, changed body")
		s.mu.Unlock()
		return Job{}, false, ErrIdempotencyConflict
	}
	if inFlightCut, ok := s.roomInFlight[req.TenantID][req.RoomID]; ok {
		// Room-serial freeze (SC-4.1): the competing trigger is rejected
		// (no new cut minted) but auditable, and the response points at the
		// in-flight cut because GET is the source of truth.
		s.auditLocked("trigger_room_serial_conflict", req.TenantID, req.RoomID, inFlightCut, req.PrincipalID, "room freeze already in flight")
		job := cloneJob(*s.jobs[inFlightCut])
		s.mu.Unlock()
		return job, false, nil
	}
	if s.roomReservations[req.TenantID][req.RoomID] {
		s.auditLocked("trigger_room_serial_conflict", req.TenantID, req.RoomID, "", req.PrincipalID, "room freeze reservation held")
		s.mu.Unlock()
		return Job{}, false, nil
	}
	s.reserveRoomLocked(req.TenantID, req.RoomID)
	s.mu.Unlock()

	// Phase 2 (unlocked): produce the frozen manifest snapshot. Error paths
	// run without the mutex held, so the reservation release must re-lock.
	manifest, err := s.freezeLocked(ctx, req)
	if err != nil {
		s.releaseRoom(req.TenantID, req.RoomID)
		return Job{}, false, err
	}
	if err := validateManifest(manifest, req); err != nil {
		s.releaseRoom(req.TenantID, req.RoomID)
		return Job{}, false, err
	}

	// Phase 3 (locked): win the room slot, stamp identity/digest, and record
	// the job at freezing. The reservation is released on every exit; a
	// recorded in-flight job keeps guarding the room from here on. Quota is
	// NOT consulted on this internal hop: quota_budget_granted guards the
	// worker-driven Transition walk of queued→freezing (whose callers carry a
	// quota grant ref), while the create-driven initial freeze is admitted at
	// the HTTP boundary by the purpose-bound evidence.commit grant.
	s.mu.Lock()
	if inFlightCut, ok := s.roomInFlight[req.TenantID][req.RoomID]; ok {
		s.auditLocked("trigger_room_serial_conflict", req.TenantID, req.RoomID, inFlightCut, req.PrincipalID, "lost room-serial race")
		s.mu.Unlock()
		s.releaseRoom(req.TenantID, req.RoomID)
		return cloneJob(*s.jobs[inFlightCut]), false, nil
	}
	var previous *CutID
	if last, ok := s.roomLastCut[req.TenantID][req.RoomID]; ok {
		cutID := last
		previous = &cutID
	}
	manifest.PreviousCutID = previous
	manifest.CutID = MintCutID(req.TenantID, req.RoomID, req.TriggerSource, req.IdempotencyKey)
	manifest.CutDigest = "sha256:" + manifestDigest(manifest)

	// Threshold mode without accumulated new evidence ends auditable
	// completed/no_change; only force bypasses the threshold.
	if req.Mode == ModeThreshold && len(manifest.SealedSegmentIDs) == 0 {
		job := s.newJobLocked(req, manifest)
		job = s.applyLocked(job, StageCompleted, SpaceResultNoChange)
		s.recordLocked(job, manifest, key, req)
		s.auditLocked("trigger_threshold_no_change", req.TenantID, req.RoomID, job.CutID, req.PrincipalID, "below threshold")
		cutID := job.CutID
		s.mu.Unlock()
		s.releaseRoom(req.TenantID, req.RoomID)
		return cloneJob(*s.jobs[cutID]), false, nil
	}

	// queued→freezing is the internal admission hop (see Phase 3); freezing
	// is recorded so the manifest is authoritative-visible before the frozen
	// guard consults the authority (which reads it back — the durable
	// authority re-derives receipts from committed evidence via GetManifest).
	job := s.newJobLocked(req, manifest)
	job = s.applyLocked(job, StageFreezing, SpaceResultPending)
	cutID := job.CutID
	version := job.JobVersion
	s.mu.Unlock()

	// The frozen guard. The structural bidirectional receipt-coverage check
	// runs first; with a GuardAuthority configured, freezing→frozen then
	// walks the SAME protocol as Transition (authority resolve → fact
	// reservation → re-resolve → locked apply), so the production durable
	// authority's receipts proof — not the caller-adjacent freezer output —
	// admits the initial frozen edge. Without a configured authority
	// (in-memory service mode) the structural check stands in, mirroring
	// checkGuardAuthority's no-authority failure for Transition walks.
	if err := receiptsCoverExactly(manifest); err != nil {
		s.mu.Lock()
		failed := s.failLocked(s.jobs[cutID], "RECEIPT_MISSING")
		s.recordLocked(failed, manifest, key, req)
		s.auditLocked("trigger_frozen_rejected", req.TenantID, req.RoomID, cutID, req.PrincipalID, err.Error())
		s.mu.Unlock()
		s.releaseRoom(req.TenantID, req.RoomID)
		return cloneJob(*s.jobs[cutID]), false, err
	}
	presented := ManifestReceiptsDigest(manifest)
	if s.guard != nil {
		_, err := s.Transition(ctx, TransitionRequest{
			TenantID: req.TenantID, CutID: cutID,
			ExpectedJobVersion: version, To: StageFrozen,
			Evidence: TransitionEvidence{ReceiptsDigest: presented},
		})
		if err != nil {
			// A deterministic guard verdict (ErrGuardNotSatisfied) is
			// recorded as RECEIPT_MISSING; an authority read failure
			// (ErrGuardAuthorityUnavailable) is recorded as
			// INFRASTRUCTURE_FAILURE — resumable, never misclassified as a
			// missing receipt fact.
			reason := "INFRASTRUCTURE_FAILURE"
			if errors.Is(err, ErrGuardNotSatisfied) {
				reason = "RECEIPT_MISSING"
			}
			s.mu.Lock()
			stored := s.jobs[cutID]
			if stored.Stage == StageFreezing {
				failed := s.failLocked(stored, reason)
				s.recordLocked(failed, manifest, key, req)
			} else {
				// A concurrent Cancel won the version CAS: the job already
				// moved on; record the idempotency binding and surface the
				// race error instead of stomping the other writer's stage.
				s.recordLocked(stored, manifest, key, req)
			}
			s.auditLocked("trigger_frozen_rejected", req.TenantID, req.RoomID, cutID, req.PrincipalID, err.Error())
			s.mu.Unlock()
			s.releaseRoom(req.TenantID, req.RoomID)
			return cloneJob(*s.jobs[cutID]), false, err
		}
	} else {
		s.mu.Lock()
		s.applyLocked(s.jobs[cutID], StageFrozen, SpaceResultPending)
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.recordLocked(s.jobs[cutID], manifest, key, req)
	s.auditLocked("trigger_frozen", req.TenantID, req.RoomID, cutID, req.PrincipalID, fmt.Sprintf("mode=%s sealed=%d", req.Mode, len(manifest.SealedSegmentIDs)))
	s.mu.Unlock()
	s.releaseRoom(req.TenantID, req.RoomID)
	return cloneJob(*s.jobs[cutID]), false, nil
}

func (s *Service) Get(_ context.Context, tenantID domain.TenantID, cutID CutID) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[cutID]
	if !ok || job.TenantID != tenantID {
		return Job{}, ErrCutNotFound
	}
	return cloneJob(*job), nil
}

func (s *Service) GetManifest(_ context.Context, tenantID domain.TenantID, cutID CutID) (Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	manifest, ok := s.manifests[cutID]
	if !ok || manifest.TenantID != tenantID {
		return Manifest{}, ErrCutNotFound
	}
	return cloneManifest(*manifest), nil
}

func (s *Service) Cancel(_ context.Context, req CancelRequest) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[req.CutID]
	if !ok || job.TenantID != req.TenantID {
		return Job{}, ErrCutNotFound
	}
	// Cancellation travels only the bundle's cancel edges (SC-4.7):
	// queued/freezing land directly in cancelled, later stages enter
	// cancelling for the worker to observe at a safe point. failed,
	// consolidating_partial, cancelling itself, and terminals have no cancel
	// edge and are rejected. Ordering follows SC-4.8: the caller's
	// ExpectedJobVersion is a mandatory CAS, so out-of-order cancel events
	// are rejected by the state machine instead of silently landing.
	if job.JobVersion != req.ExpectedJobVersion {
		s.auditLocked("cancel_rejected_stale_version", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, string(job.Stage))
		return Job{}, ErrJobVersionConflict
	}
	cancelable := map[Stage]Stage{
		StageQueued:        StageCancelled,
		StageFreezing:      StageCancelled,
		StageFrozen:        StageCancelling,
		StageDiagnosing:    StageCancelling,
		StageConsolidating: StageCancelling,
		StageReplaying:     StageCancelling,
		StageActivating:    StageCancelling,
	}
	next, ok := cancelable[job.Stage]
	if !ok {
		s.auditLocked("cancel_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, string(job.Stage))
		return Job{}, ErrNotCancelable
	}
	updated := *job
	updated.Stage = next
	updated.JobVersion++
	updated.UpdatedAt = s.now()
	*job = updated
	if !inFlight(next) {
		s.clearInFlightLocked(job.TenantID, job.RoomID, job.CutID)
		s.lastCutLocked(job.TenantID, job.RoomID, job.CutID)
	}
	s.auditLocked("cancel_requested", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, string(next))
	return cloneJob(*job), nil
}

func (s *Service) Resume(_ context.Context, req ResumeRequest) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[req.CutID]
	if !ok || job.TenantID != req.TenantID {
		return Job{}, ErrCutNotFound
	}
	if job.JobVersion != req.ExpectedJobVersion {
		return Job{}, ErrJobVersionConflict
	}
	if job.Stage != StageFailed {
		s.auditLocked("resume_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, string(job.Stage))
		return Job{}, ErrNotResumable
	}
	// Resuming re-enters an in-flight stage: the room must not be occupied
	// by another cut (SC-4.1), and this cut's own room occupancy plus the
	// stale last_cut recorded at its failure are rebuilt here so the
	// room-serial ledgers keep holding for snapshots taken at any point.
	if occupant, ok := s.roomInFlight[job.TenantID][job.RoomID]; ok && occupant != job.CutID {
		s.auditLocked("resume_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, "room occupied by "+string(occupant))
		return Job{}, ErrRoomOccupied
	}
	// No default re-entry stage: every runtime failed job records its failure
	// origin in FailedFrom (failLocked stamps it), so an empty value here is
	// corruption, not a legitimate state — resume must fail closed instead of
	// guessing diagnosing.
	if !resumableFailedFrom(job.FailedFrom) {
		s.auditLocked("resume_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, "failed-from stage "+string(job.FailedFrom)+" is not resumable")
		return Job{}, ErrNotResumable
	}
	target := job.FailedFrom
	updated := *job
	updated.Stage = target
	updated.JobVersion++
	updated.UpdatedAt = s.now()
	*job = updated
	if inFlight(target) {
		if s.roomInFlight[job.TenantID] == nil {
			s.roomInFlight[job.TenantID] = map[RoomID]CutID{}
		}
		s.roomInFlight[job.TenantID][job.RoomID] = job.CutID
	} else {
		s.clearInFlightLocked(job.TenantID, job.RoomID, job.CutID)
	}
	if s.roomLastCut[job.TenantID][job.RoomID] == job.CutID {
		delete(s.roomLastCut[job.TenantID], job.RoomID)
	}
	s.auditLocked("resume", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, string(target))
	return cloneJob(*job), nil
}

// Rediagnose re-enters diagnosing against the same frozen Cut with a new
// diagnosis policy revision (SC-4.2): a revision bump, never a resume. The
// only legal source stage is partially_failed, the revision must actually
// advance, and the governance-issued run reference binds the new immutable
// diagnosis run to this job atomically.
func (s *Service) Rediagnose(_ context.Context, req RediagnoseRequest) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[req.CutID]
	if !ok || job.TenantID != req.TenantID {
		return Job{}, ErrCutNotFound
	}
	if job.JobVersion != req.ExpectedJobVersion {
		return Job{}, ErrJobVersionConflict
	}
	if job.Stage != StagePartiallyFailed {
		s.auditLocked("rediagnose_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, string(job.Stage))
		return Job{}, ErrIllegalTransition
	}
	if req.PrincipalID == "" {
		s.auditLocked("rediagnose_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, "missing governance principal")
		return Job{}, errors.New("rediagnose requires a governance principal")
	}
	if req.DiagnosisPolicyRevision == "" {
		s.auditLocked("rediagnose_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, "missing policy revision")
		return Job{}, errors.New("rediagnose requires a new diagnosis policy revision")
	}
	if req.DiagnosisPolicyRevision == job.DiagnosisPolicyRevision {
		s.auditLocked("rediagnose_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, "same policy revision "+req.DiagnosisPolicyRevision)
		return Job{}, errors.New("rediagnose requires a policy revision distinct from the active one")
	}
	if req.NewDiagnosisRunRef == "" {
		s.auditLocked("rediagnose_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, "missing diagnosis run ref")
		return Job{}, errors.New("rediagnose requires the new diagnosis run reference")
	}
	// Rediagnosis re-enters diagnosing: the room must not be occupied by
	// another cut (SC-4.1), and this cut's own room occupancy plus the
	// stale last_cut recorded at its partially_failed terminal are rebuilt
	// here so the room-serial ledgers keep holding for snapshots taken at
	// any point.
	if occupant, ok := s.roomInFlight[job.TenantID][job.RoomID]; ok && occupant != job.CutID {
		s.auditLocked("rediagnose_rejected", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, "room occupied by "+string(occupant))
		return Job{}, ErrRoomOccupied
	}
	updated := *job
	updated.Stage = StageDiagnosing
	updated.DiagnosisPolicyRevision = req.DiagnosisPolicyRevision
	updated.NewDiagnosisRunRef = req.NewDiagnosisRunRef
	updated.JobVersion++
	updated.UpdatedAt = s.now()
	*job = updated
	if s.roomInFlight[job.TenantID] == nil {
		s.roomInFlight[job.TenantID] = map[RoomID]CutID{}
	}
	s.roomInFlight[job.TenantID][job.RoomID] = job.CutID
	if s.roomLastCut[job.TenantID][job.RoomID] == job.CutID {
		delete(s.roomLastCut[job.TenantID], job.RoomID)
	}
	s.auditLocked("rediagnose", req.TenantID, job.RoomID, job.CutID, req.PrincipalID, req.DiagnosisPolicyRevision+" run="+req.NewDiagnosisRunRef)
	return cloneJob(*job), nil
}

// Transition enforces the frozen cut_job state machine with optimistic
// JobVersion CAS: the edge must exist AND the bundle's guard must be
// resolved by the configured GuardAuthority against the owning authorities
// (callers carry evidence references, never self-reported booleans). The
// authority is resolved unlocked and then re-resolved under an authority-side
// fact reservation (ReserveFacts → validation → locked apply → ReleaseFacts):
// fact mutations serialize behind the reservation, so the digest validated
// immediately before the commit cannot be revoked or superseded mid-apply —
// the apply-time authority CAS. The apply window itself performs no authority
// calls (adapters may call back into the cut service), and a concurrent
// mutation of the job or of the underlying facts invalidates the admission.
// partially_failed→diagnosing is not available here at all: governance
// re-entry goes through Rediagnose.
func (s *Service) Transition(ctx context.Context, req TransitionRequest) (Job, error) {
	// Phase A (locked): identity, CAS, edge legality, request-local guards.
	s.mu.Lock()
	job, ok := s.jobs[req.CutID]
	if !ok || job.TenantID != req.TenantID {
		s.mu.Unlock()
		return Job{}, ErrCutNotFound
	}
	if job.JobVersion != req.ExpectedJobVersion {
		s.mu.Unlock()
		return Job{}, ErrJobVersionConflict
	}
	from := job.Stage
	guard := s.guard
	if !transitionEdges[from][req.To] {
		s.auditLocked("transition_rejected", req.TenantID, job.RoomID, job.CutID, "", string(from)+"→"+string(req.To))
		s.mu.Unlock()
		return Job{}, ErrIllegalTransition
	}
	if err := checkLocalGuards(from, req.To, job.DiagnosisPolicyRevision, req); err != nil {
		s.auditLocked("transition_guard_rejected", req.TenantID, job.RoomID, job.CutID, "", err.Error())
		s.mu.Unlock()
		return Job{}, err
	}
	s.mu.Unlock()

	// Phase B (unlocked): resolve guard evidence against the authorities,
	// capturing the fact snapshot that admitted the guard.
	confirmation, err := checkGuardAuthority(ctx, guard, req.TenantID, req.CutID, from, req.To, req)
	if err != nil {
		s.mu.Lock()
		if job, ok := s.jobs[req.CutID]; ok {
			s.auditLocked("transition_guard_rejected", req.TenantID, job.RoomID, job.CutID, "", err.Error())
		}
		s.mu.Unlock()
		return Job{}, err
	}

	// Phase C-pre (unlocked, immediately before the commit): take the
	// authority-side fact reservation and re-resolve the guard UNDER it —
	// fact mutations serialize behind the reservation (ReserveFacts), so
	// the digest confirmed here cannot be revoked or superseded between
	// this validation and the commit: this is the apply-time authority CAS
	// (R3). Authority adapters are NEVER invoked while the cut service lock
	// is held: adapters are allowed to call back into the cut service
	// itself (the integration tracer's authority reads GetManifest), so a
	// re-resolve under the lock would self-deadlock. The reservation +
	// validation therefore runs directly before the locked apply window,
	// and that window performs no authority calls.
	if guard != nil && confirmation.FactsDigest != "" {
		if s.transitionHookAfterResolve != nil {
			s.transitionHookAfterResolve()
		}
		if err := guard.ReserveFacts(ctx, req.TenantID, req.CutID); err != nil {
			// A reservation failure is an availability fault of the authority
			// dependency surface, not a deterministic fact rebuttal — it must
			// classify as transient (HTTP 500), never as a 422 receipt-style
			// rejection that would permanently falsify the job's failure
			// reason.
			err := fmt.Errorf("%w: authority fact reservation: %v", ErrGuardAuthorityUnavailable, err)
			s.mu.Lock()
			if job, ok := s.jobs[req.CutID]; ok {
				s.auditLocked("transition_guard_rejected", req.TenantID, job.RoomID, job.CutID, "", "phase C re-resolve: "+err.Error())
			}
			s.mu.Unlock()
			return Job{}, err
		}
		defer guard.ReleaseFacts(ctx, req.TenantID, req.CutID)
		recheck, err := checkGuardAuthority(ctx, guard, req.TenantID, req.CutID, from, req.To, req)
		if err != nil {
			s.mu.Lock()
			if job, ok := s.jobs[req.CutID]; ok {
				s.auditLocked("transition_guard_rejected", req.TenantID, job.RoomID, job.CutID, "", "phase C re-resolve: "+err.Error())
			}
			s.mu.Unlock()
			return Job{}, err
		}
		if recheck.FactsDigest != confirmation.FactsDigest {
			err := fmt.Errorf("%w: authority facts changed between resolution and application (%s → %s)", ErrGuardNotSatisfied, confirmation.FactsDigest, recheck.FactsDigest)
			s.mu.Lock()
			if job, ok := s.jobs[req.CutID]; ok {
				s.auditLocked("transition_guard_rejected", req.TenantID, job.RoomID, job.CutID, "", err.Error())
			}
			s.mu.Unlock()
			return Job{}, err
		}
		if s.transitionHookUnderReservation != nil {
			s.transitionHookUnderReservation()
		}
	}

	// Phase C (locked): re-verify the CAS window and apply the transition
	// stamped with the twice-confirmed digest. No authority is consulted
	// under the lock; the fact reservation taken in Phase C-pre stays held
	// (via the deferred ReleaseFacts) until the apply has completed, so the
	// confirmed facts cannot be mutated mid-commit.
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok = s.jobs[req.CutID]
	if !ok || job.TenantID != req.TenantID {
		return Job{}, ErrCutNotFound
	}
	if job.JobVersion != req.ExpectedJobVersion || job.Stage != from {
		return Job{}, ErrJobVersionConflict
	}
	updated := *job
	updated.Stage = req.To
	updated.JobVersion++
	updated.UpdatedAt = s.now()
	stamped := req.Evidence
	stamped.AuthorityFactsDigest = confirmation.FactsDigest
	updated.LastTransitionEvidence = stamped
	switch req.To {
	case StagePartiallyFailed, StageFailed:
		updated.FailureReasons = append(append([]string(nil), job.FailureReasons...), req.Reason)
	}
	if req.To == StageFailed {
		updated.FailedFrom = job.Stage
	}
	if len(req.SpaceResults) > 0 {
		updated.SpaceResults = append([]SpaceResult(nil), req.SpaceResults...)
		outcome := SpaceResultPublished
		for _, result := range updated.SpaceResults {
			if result.Result == SpaceResultNoChange {
				outcome = SpaceResultNoChange
			}
		}
		updated.Outcome = outcome
	}
	// Room-serial ledger convergence on ANY exit from the in-flight set —
	// terminal AND failed alike. failed keeps neither occupancy nor
	// settled-lineage: a failed cut releases its room so the next trigger
	// for the room is not blocked by a dead cut, mirroring the
	// Trigger-path failure and the Resume/Rediagnose occupancy rebuild.
	if !inFlight(req.To) {
		s.clearInFlightLocked(req.TenantID, job.RoomID, job.CutID)
		s.lastCutLocked(req.TenantID, job.RoomID, job.CutID)
	}
	*job = updated
	s.auditLocked("transition", req.TenantID, job.RoomID, job.CutID, "", string(from)+"→"+string(req.To)+" "+evidenceDetail(req.Evidence))
	return cloneJob(*job), nil
}

// checkLocalGuards evaluates the guard components that depend only on the
// job and the request itself (active policy binding, reason codes, terminal
// per-space results); absence or a non-terminal result fails closed.
func checkLocalGuards(from, to Stage, activePolicyRevision string, req TransitionRequest) error {
	switch {
	case from == StageDiagnosing && to == StageConsolidating:
		// SC-4.2: the diagnosis must be verified under the job's ACTIVE
		// policy revision; evidence citing a superseded (pre-rediagnose)
		// revision fails closed before any authority is consulted.
		if req.Evidence.DiagnosisPolicyRevision != activePolicyRevision {
			return fmt.Errorf("%w: diagnosis_verified (evidence policy revision %q does not match active %q)", ErrGuardNotSatisfied, req.Evidence.DiagnosisPolicyRevision, activePolicyRevision)
		}
	case from == StageDiagnosing && to == StagePartiallyFailed:
		if req.Reason != reasonDiagnosisFailed {
			return fmt.Errorf("%w: reason %s required", ErrGuardNotSatisfied, reasonDiagnosisFailed)
		}
	case from == StagePartiallyFailed && to == StageDiagnosing:
		return ErrRediagnoseRequired
	case from == StageReplaying && to == StageCompleted:
		if req.Reason != "REPLAY_FAIL_OR_INCONCLUSIVE" {
			return fmt.Errorf("%w: reason REPLAY_FAIL_OR_INCONCLUSIVE required", ErrGuardNotSatisfied)
		}
	case from == StageConsolidating && to == StageCompleted:
		if len(req.SpaceResults) == 0 {
			return fmt.Errorf("%w: per-space terminal results required", ErrGuardNotSatisfied)
		}
		for _, result := range req.SpaceResults {
			switch result.Result {
			case SpaceResultPublished, SpaceResultDuplicate, SpaceResultNoChange:
			default:
				return fmt.Errorf("%w: space %s is not terminal published/duplicate/no_change", ErrGuardNotSatisfied, result.SpaceID)
			}
		}
	}
	return nil
}

// checkGuardAuthority resolves the frozen bundle's authority-side guard for
// one edge, returning the authority's confirmation snapshot alongside the
// verdict. A nil authority, a missing evidence reference, an unknown
// reference, an authority that resolves the guard without a confirmation
// digest, or a digest that does not canonically cover the reported facts all
// fail closed with ErrGuardNotSatisfied; authority errors are surfaced
// wrapped.
func checkGuardAuthority(ctx context.Context, guard GuardAuthority, tenantID domain.TenantID, cutID CutID, from, to Stage, req TransitionRequest) (GuardConfirmation, error) {
	needsAuthority := func(guardName string) error {
		if guard == nil {
			return fmt.Errorf("%w: %s (no guard authority configured)", ErrGuardNotSatisfied, guardName)
		}
		return nil
	}
	resolve := func(guardName string, ok bool, err error) error {
		if err != nil {
			// A deterministic guard verdict keeps the ErrGuardNotSatisfied
			// class; any other authority failure is a transient read error
			// (ErrGuardAuthorityUnavailable) — both fail the edge closed,
			// but only a verdict may be permanently recorded as the guard's
			// answer (a transient ledger/transport fault must not be
			// misclassified as a missing fact).
			if errors.Is(err, ErrGuardNotSatisfied) {
				return fmt.Errorf("%w: %s: %v", ErrGuardNotSatisfied, guardName, err)
			}
			return fmt.Errorf("%w: %s: %v", ErrGuardAuthorityUnavailable, guardName, err)
		}
		if !ok {
			return fmt.Errorf("%w: %s", ErrGuardNotSatisfied, guardName)
		}
		return nil
	}
	requireConfirmed := func(guardName string, confirmation GuardConfirmation) error {
		if confirmation.FactsDigest == "" {
			return fmt.Errorf("%w: %s (authority returned no confirmation digest)", ErrGuardNotSatisfied, guardName)
		}
		if len(confirmation.Facts) == 0 {
			return fmt.Errorf("%w: %s (authority confirmed a verdict over no facts)", ErrGuardNotSatisfied, guardName)
		}
		// The service recomputes the canonical digest over the authority's
		// reported facts instead of trusting the reported digest: a
		// confirmation whose digest does not cover its own facts (a constant
		// digest over drifting facts) fails closed at every resolution (R3).
		if digest := ConfirmFacts(guardName, confirmation.Facts).FactsDigest; digest != confirmation.FactsDigest {
			return fmt.Errorf("%w: %s (authority confirmation digest does not cover its facts)", ErrGuardNotSatisfied, guardName)
		}
		return nil
	}
	switch {
	case from == StageQueued && to == StageFreezing:
		if err := needsAuthority("quota_budget_granted"); err != nil {
			return GuardConfirmation{}, err
		}
		if req.Evidence.QuotaGrantRef == "" {
			return GuardConfirmation{}, fmt.Errorf("%w: quota_budget_granted (missing grant ref)", ErrGuardNotSatisfied)
		}
		ok, confirmation, err := guard.QuotaGranted(ctx, tenantID, cutID, req.Evidence.QuotaGrantRef)
		if err := resolve("quota_budget_granted", ok, err); err != nil {
			return GuardConfirmation{}, err
		}
		return confirmation, requireConfirmed("quota_budget_granted", confirmation)
	case from == StageFreezing && to == StageFrozen:
		if err := needsAuthority("receipts_complete_for_all_sealed_segments"); err != nil {
			return GuardConfirmation{}, err
		}
		if req.Evidence.ReceiptsDigest == "" {
			return GuardConfirmation{}, fmt.Errorf("%w: receipts_complete_for_all_sealed_segments (missing receipts digest)", ErrGuardNotSatisfied)
		}
		ok, confirmation, err := guard.ReceiptsComplete(ctx, tenantID, cutID, req.Evidence.ReceiptsDigest)
		if err := resolve("receipts_complete_for_all_sealed_segments", ok, err); err != nil {
			return GuardConfirmation{}, err
		}
		return confirmation, requireConfirmed("receipts_complete_for_all_sealed_segments", confirmation)
	case from == StageDiagnosing && to == StageConsolidating:
		if err := needsAuthority("diagnosis_verified"); err != nil {
			return GuardConfirmation{}, err
		}
		if req.Evidence.DiagnosisAnnotationRef == "" || req.Evidence.DiagnosisPolicyRevision == "" {
			return GuardConfirmation{}, fmt.Errorf("%w: diagnosis_verified (missing annotation ref or policy revision)", ErrGuardNotSatisfied)
		}
		ok, confirmation, err := guard.DiagnosisVerified(ctx, tenantID, cutID, req.Evidence.DiagnosisAnnotationRef, req.Evidence.DiagnosisPolicyRevision)
		if err := resolve("diagnosis_verified", ok, err); err != nil {
			return GuardConfirmation{}, err
		}
		return confirmation, requireConfirmed("diagnosis_verified", confirmation)
	case from == StageConsolidatingPartial && to == StagePartiallyFailed:
		if err := needsAuthority("projection_complete"); err != nil {
			return GuardConfirmation{}, err
		}
		if req.Evidence.ProjectionRecordDigest == "" {
			return GuardConfirmation{}, fmt.Errorf("%w: projection_complete (missing projection record digest)", ErrGuardNotSatisfied)
		}
		ok, confirmation, err := guard.ProjectionComplete(ctx, tenantID, cutID, req.Evidence.ProjectionRecordDigest)
		if err := resolve("projection_complete", ok, err); err != nil {
			return GuardConfirmation{}, err
		}
		return confirmation, requireConfirmed("projection_complete", confirmation)
	case from == StageConsolidating && to == StageReplaying:
		if err := needsAuthority("proposal_exists"); err != nil {
			return GuardConfirmation{}, err
		}
		if req.Evidence.ProposalRef == "" {
			return GuardConfirmation{}, fmt.Errorf("%w: proposal_exists (missing proposal ref)", ErrGuardNotSatisfied)
		}
		refs, confirmation, err := guard.ProposalRefs(ctx, tenantID, cutID)
		if err != nil {
			if errors.Is(err, ErrGuardNotSatisfied) {
				return GuardConfirmation{}, fmt.Errorf("%w: proposal_exists: %v", ErrGuardNotSatisfied, err)
			}
			return GuardConfirmation{}, fmt.Errorf("%w: proposal_exists: %v", ErrGuardAuthorityUnavailable, err)
		}
		for _, ref := range refs {
			if ref == req.Evidence.ProposalRef {
				return confirmation, requireConfirmed("proposal_refs", confirmation)
			}
		}
		return GuardConfirmation{}, fmt.Errorf("%w: proposal_exists (ref %s not registered for cut)", ErrGuardNotSatisfied, req.Evidence.ProposalRef)
	case from == StageConsolidating && to == StageCompleted:
		if err := needsAuthority("no_proposal"); err != nil {
			return GuardConfirmation{}, err
		}
		refs, confirmation, err := guard.ProposalRefs(ctx, tenantID, cutID)
		if err != nil {
			if errors.Is(err, ErrGuardNotSatisfied) {
				return GuardConfirmation{}, fmt.Errorf("%w: no_proposal: %v", ErrGuardNotSatisfied, err)
			}
			return GuardConfirmation{}, fmt.Errorf("%w: no_proposal: %v", ErrGuardAuthorityUnavailable, err)
		}
		if len(refs) > 0 {
			return GuardConfirmation{}, fmt.Errorf("%w: no_proposal (%d proposals registered)", ErrGuardNotSatisfied, len(refs))
		}
		return confirmation, requireConfirmed("proposal_refs", confirmation)
	case from == StageReplaying && to == StageActivating:
		if err := needsAuthority("replay_pass_and_target_binding_exact"); err != nil {
			return GuardConfirmation{}, err
		}
		if req.Evidence.ReplayResultRef == "" {
			return GuardConfirmation{}, fmt.Errorf("%w: replay_pass_and_target_binding_exact (missing replay result ref)", ErrGuardNotSatisfied)
		}
		ok, confirmation, err := guard.ReplayPassed(ctx, tenantID, cutID, req.Evidence.ReplayResultRef)
		if err := resolve("replay_pass_and_target_binding_exact", ok, err); err != nil {
			return GuardConfirmation{}, err
		}
		return confirmation, requireConfirmed("replay_pass_and_target_binding_exact", confirmation)
	case from == StageActivating && to == StageCompleted:
		if err := needsAuthority("cas_base_revision_matches"); err != nil {
			return GuardConfirmation{}, err
		}
		if req.Evidence.CASBaseRevision == "" {
			return GuardConfirmation{}, fmt.Errorf("%w: cas_base_revision_matches (missing base revision)", ErrGuardNotSatisfied)
		}
		ok, confirmation, err := guard.CASBaseRevisionMatches(ctx, tenantID, cutID, req.Evidence.CASBaseRevision)
		if err := resolve("cas_base_revision_matches", ok, err); err != nil {
			return GuardConfirmation{}, err
		}
		return confirmation, requireConfirmed("cas_base_revision_matches", confirmation)
	case from == StageCancelling && to == StageCancelled:
		if err := needsAuthority("no_completed_side_effects"); err != nil {
			return GuardConfirmation{}, err
		}
		completed, confirmation, err := guard.SideEffectsCompleted(ctx, tenantID, cutID)
		if err != nil {
			return GuardConfirmation{}, fmt.Errorf("%w: no_completed_side_effects: %v", ErrGuardNotSatisfied, err)
		}
		if completed {
			return GuardConfirmation{}, fmt.Errorf("%w: no_completed_side_effects", ErrGuardNotSatisfied)
		}
		return confirmation, requireConfirmed("side_effects_completed", confirmation)
	case from == StageCancelling && to == StagePartiallyCancelled:
		if err := needsAuthority("some_side_effects_completed"); err != nil {
			return GuardConfirmation{}, err
		}
		completed, confirmation, err := guard.SideEffectsCompleted(ctx, tenantID, cutID)
		if err != nil {
			return GuardConfirmation{}, fmt.Errorf("%w: some_side_effects_completed: %v", ErrGuardNotSatisfied, err)
		}
		if !completed {
			return GuardConfirmation{}, fmt.Errorf("%w: some_side_effects_completed", ErrGuardNotSatisfied)
		}
		return confirmation, requireConfirmed("side_effects_completed", confirmation)
	}
	return GuardConfirmation{}, nil
}

// evidenceDetail renders the non-empty evidence references for the audit
// trail so each admitted transition is traceable to its authority facts.
func evidenceDetail(ev TransitionEvidence) string {
	pairs := make([]string, 0, 8)
	add := func(k, v string) {
		if v != "" {
			pairs = append(pairs, k+"="+v)
		}
	}
	add("quota", ev.QuotaGrantRef)
	add("receipts", ev.ReceiptsDigest)
	add("annotation", ev.DiagnosisAnnotationRef)
	add("policy", ev.DiagnosisPolicyRevision)
	add("projection", ev.ProjectionRecordDigest)
	add("proposal", ev.ProposalRef)
	add("replay", ev.ReplayResultRef)
	add("cas_base", ev.CASBaseRevision)
	add("authority_facts", ev.AuthorityFactsDigest)
	if len(pairs) == 0 {
		return "evidence=none"
	}
	return "evidence=" + strings.Join(pairs, ",")
}

func (s *Service) newJobLocked(req TriggerRequest, manifest Manifest) *Job {
	cutID := MintCutID(req.TenantID, req.RoomID, req.TriggerSource, req.IdempotencyKey)
	watermark := manifest.RoomSequenceWatermark
	job := &Job{
		CutID: cutID, TenantID: req.TenantID, RoomID: req.RoomID,
		Stage: StageQueued, JobVersion: 1, CutDigest: manifest.CutDigest,
		Outcome:                 SpaceResultPending,
		RoomSequenceWatermark:   &watermark,
		DiagnosisPolicyRevision: manifest.PolicyRevisions.DiagnosisPolicyRevision,
		UpdatedAt:               s.now(),
	}
	s.jobs[cutID] = job
	// The freezer's output is untrusted caller-adjacent memory: store a deep
	// copy so later external slice/pointer mutations cannot bleed into the
	// immutable manifest record.
	stored := cloneManifest(manifest)
	s.manifests[cutID] = &stored
	if s.roomInFlight[req.TenantID] == nil {
		s.roomInFlight[req.TenantID] = map[RoomID]CutID{}
	}
	s.roomInFlight[req.TenantID][req.RoomID] = cutID
	return job
}

// applyLocked moves the job through a legal stage hop without CAS (internal
// freeze path) and stamps the outcome.
func (s *Service) applyLocked(job *Job, to Stage, outcome SpaceResultKind) *Job {
	updated := *job
	updated.Stage = to
	updated.Outcome = outcome
	updated.JobVersion++
	updated.UpdatedAt = s.now()
	*job = updated
	return job
}

func (s *Service) failLocked(job *Job, reason string) *Job {
	updated := *job
	updated.Stage = StageFailed
	updated.FailureReasons = append(append([]string(nil), job.FailureReasons...), reason)
	updated.FailedFrom = job.Stage
	updated.JobVersion++
	updated.UpdatedAt = s.now()
	*job = updated
	return job
}

func (s *Service) recordLocked(job *Job, manifest Manifest, key string, req TriggerRequest) {
	stored := cloneManifest(manifest)
	s.manifests[job.CutID] = &stored
	s.idempotency[key] = job.CutID
	s.requests[key] = req
	if !inFlight(job.Stage) {
		s.clearInFlightLocked(job.TenantID, job.RoomID, job.CutID)
		s.lastCutLocked(job.TenantID, job.RoomID, job.CutID)
	}
}

func (s *Service) clearInFlightLocked(tenantID domain.TenantID, roomID RoomID, cutID CutID) {
	if s.roomInFlight[tenantID][roomID] == cutID {
		delete(s.roomInFlight[tenantID], roomID)
	}
}

func (s *Service) reserveRoomLocked(tenantID domain.TenantID, roomID RoomID) {
	if s.roomReservations[tenantID] == nil {
		s.roomReservations[tenantID] = map[RoomID]bool{}
	}
	s.roomReservations[tenantID][roomID] = true
}

func (s *Service) releaseRoomLocked(tenantID domain.TenantID, roomID RoomID) {
	delete(s.roomReservations[tenantID], roomID)
}

// releaseRoom is the locking wrapper for phase-2 error paths, which run
// without the mutex held (P0-7: the bare *Locked call raced concurrent
// reserve/delete on the shared map).
func (s *Service) releaseRoom(tenantID domain.TenantID, roomID RoomID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseRoomLocked(tenantID, roomID)
}

func (s *Service) lastCutLocked(tenantID domain.TenantID, roomID RoomID, cutID CutID) {
	if s.roomLastCut[tenantID] == nil {
		s.roomLastCut[tenantID] = map[RoomID]CutID{}
	}
	s.roomLastCut[tenantID][roomID] = cutID
}

// freezeLocked produces the manifest content. An injected Freezer is
// authoritative for evidence; otherwise the deterministic fixture freezer
// mints the tracer-bullet room snapshot (one sealed segment, 1:1 receipts,
// one pinned private space scope). The name is historical: with the room
// slot already admitted, it runs outside the service mutex.
func (s *Service) freezeLocked(ctx context.Context, req TriggerRequest) (Manifest, error) {
	if s.freezer != nil {
		return s.freezer.Freeze(ctx, req)
	}
	sealed := []SegmentID{SegmentID("segment-" + string(req.RoomID) + "-001")}
	batches := []domain.BatchID{domain.BatchID("batch-" + string(req.RoomID) + "-001")}
	receipts := make([]EvidenceCommitReceipt, 0, len(sealed))
	for _, segment := range sealed {
		receipts = append(receipts, EvidenceCommitReceipt{
			ReceiptID:   ReceiptID("receipt-" + segment),
			SegmentID:   segment,
			BatchIDs:    batches,
			BatchDigest: "sha256:" + fullDigest(string(req.TenantID)+"|"+string(segment)),
		})
	}
	// Threshold mode consults the same snapshot: no accumulated sealed
	// delta means the auditable no_change outcome, never a silent drop.
	if req.Mode == ModeThreshold {
		sealed = nil
		receipts = nil
	}
	head := domain.ProjectionVersion(1)
	query := int64(0)
	return Manifest{
		CutID: CutID("cut-pending"), TenantID: req.TenantID, RoomID: req.RoomID,
		Mode: req.Mode, TriggerSource: req.TriggerSource, IdempotencyKey: req.IdempotencyKey,
		RoomSequenceWatermark: 1,
		SealedSegmentIDs:      sealed, SpaceScopes: []SpaceScope{{
			SpaceID:               domain.SpaceID("space-" + string(req.RoomID) + "-private"),
			Scope:                 domain.SpaceScope("private"),
			EvidenceBatchIDs:      batches,
			ProjectionHeadVersion: &head,
			QueryWatermark:        &query,
		}},
		PolicyRevisions: PolicyRevisions{
			DiagnosisPolicyRevision: "diagnosis-v1",
			ReplayPolicyRevision:    "replay-v1",
			AggregationPolicyDigest: "sha256:" + fullDigest("aggregation-policy-v1"),
		},
		EvidenceCommitReceipts: receipts,
	}, nil
}

// validateManifest enforces the frozen manifest contract on freezer output:
// identity must match the request, policy revisions must be pinned, and
// digest fields must carry the sha256 digest shape (SC-3.1/SC-8.3).
func validateManifest(manifest Manifest, req TriggerRequest) error {
	if manifest.TenantID != req.TenantID || manifest.RoomID != req.RoomID || manifest.Mode != req.Mode {
		return fmt.Errorf("%w: identity mismatch", ErrManifestContractViolation)
	}
	if manifest.PolicyRevisions.DiagnosisPolicyRevision == "" || manifest.PolicyRevisions.ReplayPolicyRevision == "" {
		return fmt.Errorf("%w: policy revisions unpinned", ErrManifestContractViolation)
	}
	if manifest.PolicyRevisions.AggregationPolicyDigest != "" && !sha256DigestShape(manifest.PolicyRevisions.AggregationPolicyDigest) {
		return fmt.Errorf("%w: aggregation policy digest shape", ErrManifestContractViolation)
	}
	for _, receipt := range manifest.EvidenceCommitReceipts {
		if !sha256DigestShape(receipt.BatchDigest) {
			return fmt.Errorf("%w: receipt batch digest shape", ErrManifestContractViolation)
		}
		if len(receipt.BatchIDs) == 0 {
			return fmt.Errorf("%w: receipt without batch provenance", ErrManifestContractViolation)
		}
	}
	return nil
}

func sha256DigestShape(digest string) bool {
	if len(digest) != len("sha256:")+64 || !strings.HasPrefix(digest, "sha256:") {
		return false
	}
	for _, r := range digest[len("sha256:"):] {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// receiptsCoverExactly enforces the frozen guard: one receipt per sealed
// segment, no duplicates, and the receipt set is exactly the sealed set — a
// strict bidirectional comparison. It rejects duplicate sealed segments,
// duplicate receipts, and any receipt whose segment lies outside the frozen
// set (untrusted Freezer output must not smuggle coverage beyond the sealed
// surface).
func receiptsCoverExactly(manifest Manifest) error {
	sealedSet := make(map[SegmentID]bool, len(manifest.SealedSegmentIDs))
	for _, segment := range manifest.SealedSegmentIDs {
		if sealedSet[segment] {
			return ErrReceiptCoverageIncomplete
		}
		sealedSet[segment] = true
	}
	if len(manifest.EvidenceCommitReceipts) != len(sealedSet) {
		return ErrReceiptCoverageIncomplete
	}
	covered := make(map[SegmentID]bool, len(manifest.EvidenceCommitReceipts))
	for _, receipt := range manifest.EvidenceCommitReceipts {
		if covered[receipt.SegmentID] || !sealedSet[receipt.SegmentID] {
			// duplicate receipt, or a receipt for a segment that is not sealed
			return ErrReceiptCoverageIncomplete
		}
		covered[receipt.SegmentID] = true
	}
	// Bidirectional: every sealed segment is covered by exactly one receipt,
	// and no receipt exists for an unsealed segment.
	for segment := range sealedSet {
		if !covered[segment] {
			return ErrReceiptCoverageIncomplete
		}
	}
	return nil
}

func sameTriggerBody(stored, req TriggerRequest) bool {
	return stored.TenantID == req.TenantID && stored.PrincipalID == req.PrincipalID &&
		stored.RoomID == req.RoomID && stored.Mode == req.Mode && stored.TriggerSource == req.TriggerSource
}

// idempotencyKey scopes a trigger's idempotency identity to the full
// SC-2.3 tuple: tenant, room, trigger source, then the caller key.
func idempotencyKey(tenantID domain.TenantID, roomID RoomID, triggerSource, key string) string {
	return string(tenantID) + "\x00" + string(roomID) + "\x00" + triggerSource + "\x00" + key
}

func manifestDigest(manifest Manifest) string {
	copied := manifest
	copied.CutDigest = ""
	encoded, err := json.Marshal(copied)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func shortDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])[:16]
}

// MintCutID returns the deterministic CutID the service assigns to a trigger
// keyed by (tenant, room, trigger_source, idempotency-key). It covers the same
// canonical identity as idempotencyKey — tenant, room, trigger source and
// header key — so distinct trigger sources under the same header key mint
// distinct Cuts (no jobs[CutID] collision) and the HTTP layer can detect a
// room-in-flight conflict by comparing a fresh trigger's returned CutID
// against this mint.
func MintCutID(tenantID domain.TenantID, roomID RoomID, triggerSource, idempotencyKey string) CutID {
	return CutID("cut-" + shortDigest(string(tenantID)+"|"+string(roomID)+"|"+triggerSource+"|"+idempotencyKey))
}

func fullDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// ReceiptsDigest aggregates evidence-commit receipts into the canonical
// receipts digest presented at the freezing→frozen guard: the sorted
// aggregate of every receipt's batch digest. Trigger presents it as the
// guard evidence, and guard authorities re-derive the same aggregate from
// their own durable facts — a presented digest that does not match the
// authority's independent recomputation fails closed.
func ReceiptsDigest(receipts []EvidenceCommitReceipt) string {
	digests := make([]string, 0, len(receipts))
	for _, receipt := range receipts {
		digests = append(digests, receipt.BatchDigest)
	}
	sort.Strings(digests)
	if len(digests) == 0 {
		return "sha256:" + fullDigest("empty-receipt")
	}
	return "sha256:" + fullDigest(strings.Join(digests, "\x00"))
}

// ManifestReceiptsDigest is ReceiptsDigest over a frozen manifest's evidence
// commit receipts.
func ManifestReceiptsDigest(manifest Manifest) string {
	return ReceiptsDigest(manifest.EvidenceCommitReceipts)
}

func cloneJob(job Job) Job {
	cloned := job
	cloned.FailureReasons = append([]string(nil), job.FailureReasons...)
	cloned.SpaceResults = append([]SpaceResult(nil), job.SpaceResults...)
	if job.RoomSequenceWatermark != nil {
		watermark := *job.RoomSequenceWatermark
		cloned.RoomSequenceWatermark = &watermark
	}
	return cloned
}

func cloneManifest(manifest Manifest) Manifest {
	cloned := manifest
	cloned.SealedSegmentIDs = append([]SegmentID(nil), manifest.SealedSegmentIDs...)
	cloned.SpaceScopes = make([]SpaceScope, len(manifest.SpaceScopes))
	for index, scope := range manifest.SpaceScopes {
		cloned.SpaceScopes[index] = scope
		cloned.SpaceScopes[index].EvidenceBatchIDs = append([]domain.BatchID(nil), scope.EvidenceBatchIDs...)
		if scope.ProjectionHeadVersion != nil {
			version := *scope.ProjectionHeadVersion
			cloned.SpaceScopes[index].ProjectionHeadVersion = &version
		}
		if scope.QueryWatermark != nil {
			watermark := *scope.QueryWatermark
			cloned.SpaceScopes[index].QueryWatermark = &watermark
		}
	}
	cloned.EvidenceCommitReceipts = make([]EvidenceCommitReceipt, len(manifest.EvidenceCommitReceipts))
	for index, receipt := range manifest.EvidenceCommitReceipts {
		cloned.EvidenceCommitReceipts[index] = receipt
		cloned.EvidenceCommitReceipts[index].BatchIDs = append([]domain.BatchID(nil), receipt.BatchIDs...)
	}
	if manifest.PreviousCutID != nil {
		cutID := *manifest.PreviousCutID
		cloned.PreviousCutID = &cutID
	}
	return cloned
}

// serviceSnapshot is the durable image of the cut service's job state for
// composition persistence: jobs, manifests, the idempotency index with its
// recorded request bodies, the room in-flight/last-cut ledgers, and the
// audit trail. Live freeze reservations are deliberately excluded — a
// restart releases them, so a trigger that was mid-freeze when the process
// died simply retries; the idempotency index keeps the retry replay-safe.
type serviceSnapshot struct {
	Jobs         map[string]Job               `json:"jobs"`
	Manifests    map[string]Manifest          `json:"manifests"`
	Idempotency  map[string]string            `json:"idempotency"`
	Requests     map[string]TriggerRequest    `json:"requests"`
	RoomInFlight map[string]map[string]string `json:"room_in_flight"`
	RoomLastCut  map[string]map[string]string `json:"room_last_cut"`
	Audit        []AuditEntry                 `json:"audit"`
}

// Snapshot returns the JSON image of the cut job state under one lock, for
// the composition's start/stop persistence (PG-50A): the returned image is
// a deep copy — mutating the service afterwards never tears it.
func (s *Service) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	image := serviceSnapshot{
		Jobs:         make(map[string]Job, len(s.jobs)),
		Manifests:    make(map[string]Manifest, len(s.manifests)),
		Idempotency:  make(map[string]string, len(s.idempotency)),
		Requests:     make(map[string]TriggerRequest, len(s.requests)),
		RoomInFlight: make(map[string]map[string]string, len(s.roomInFlight)),
		RoomLastCut:  make(map[string]map[string]string, len(s.roomLastCut)),
		Audit:        append([]AuditEntry(nil), s.audit...),
	}
	for cutID, job := range s.jobs {
		image.Jobs[string(cutID)] = cloneJob(*job)
	}
	for cutID, manifest := range s.manifests {
		image.Manifests[string(cutID)] = cloneManifest(*manifest)
	}
	for key, cutID := range s.idempotency {
		image.Idempotency[key] = string(cutID)
		image.Requests[key] = s.requests[key]
	}
	for tenantID, rooms := range s.roomInFlight {
		image.RoomInFlight[string(tenantID)] = make(map[string]string, len(rooms))
		for roomID, cutID := range rooms {
			image.RoomInFlight[string(tenantID)][string(roomID)] = string(cutID)
		}
	}
	for tenantID, rooms := range s.roomLastCut {
		image.RoomLastCut[string(tenantID)] = make(map[string]string, len(rooms))
		for roomID, cutID := range rooms {
			image.RoomLastCut[string(tenantID)][string(roomID)] = string(cutID)
		}
	}
	encoded, err := json.Marshal(image)
	if err != nil {
		return nil, fmt.Errorf("cut state snapshot: %w", err)
	}
	return encoded, nil
}

// Restore replaces the in-memory job state from a snapshot image. It is the
// explicit startup boundary of the composition (PG-50A): a restore never
// resurrects live freeze reservations (the process that held them is gone),
// and an invalid image fails closed leaving the receiver untouched. The new
// state is fully validated into local maps BEFORE any field of the receiver
// is mutated, then swapped in atomically under one lock — a failed or
// semantically-inconsistent restore leaves the receiver exactly as it was.
func (s *Service) Restore(image []byte) error {
	var data serviceSnapshot
	if err := json.Unmarshal(image, &data); err != nil {
		return fmt.Errorf("cut state snapshot is not valid JSON: %w", err)
	}
	if !validSnapshotShape(data) {
		return fmt.Errorf("cut state snapshot has an invalid or inconsistent shape")
	}

	// Validate-before-install: build every map into a local, and only commit
	// them (plus the audit) to the receiver once all cross-references check
	// out. A dangling CutID, a room ledger pointing at a job whose tenant/
	// room do not match its key, or an idempotency target that does not exist
	// as a job is a corrupt image and must fail closed.
	jobs := make(map[CutID]*Job, len(data.Jobs))
	for cutID, job := range data.Jobs {
		if CutID(cutID) != job.CutID {
			return fmt.Errorf("cut state snapshot: job key %q does not match job.CutID %q", cutID, job.CutID)
		}
		stored := cloneJob(job)
		jobs[CutID(cutID)] = &stored
	}

	manifests := make(map[CutID]*Manifest, len(data.Manifests))
	for cutID, manifest := range data.Manifests {
		if CutID(cutID) != manifest.CutID {
			return fmt.Errorf("cut state snapshot: manifest key %q does not match manifest.CutID %q", cutID, manifest.CutID)
		}
		stored := cloneManifest(manifest)
		manifests[CutID(cutID)] = &stored
	}

	// Full job↔manifest cross-reference before any reference resolution:
	// every restored job must carry a manifest of the same identity and
	// digest, and every manifest a job — an orphan on either side would
	// restore a split authoritative state (GET job succeeds while GET
	// manifest misses, or vice versa).
	for cutID, job := range jobs {
		manifest, ok := manifests[cutID]
		if !ok {
			return fmt.Errorf("cut state snapshot: job %q has no manifest", cutID)
		}
		if manifest.TenantID != job.TenantID || manifest.RoomID != job.RoomID {
			return fmt.Errorf("cut state snapshot: manifest %q identity does not match job (tenant=%s room=%s)", cutID, manifest.TenantID, manifest.RoomID)
		}
		if manifest.CutDigest != job.CutDigest {
			return fmt.Errorf("cut state snapshot: manifest %q cut digest does not match job", cutID)
		}
		// The stored digest must also cover the manifest content itself:
		// tampering any manifest field while copying the stale digest
		// strings is rejected by recomputing the canonical digest.
		if want := "sha256:" + manifestDigest(*manifest); manifest.CutDigest != want {
			return fmt.Errorf("cut state snapshot: manifest %q cut digest does not cover its content", cutID)
		}
		// FailedFrom is Resume's re-entry target: a corrupted value would
		// resume the job into an illegal stage. A non-empty value must be an
		// actual failure origin — not merely any in-flight stage — and a
		// FAILED job must record one at all: an empty FailedFrom would
		// otherwise paper over the corruption and default to diagnosing.
		if job.FailedFrom != "" && !resumableFailedFrom(job.FailedFrom) {
			return fmt.Errorf("cut state snapshot: job %q failed-from stage %q is not resumable", cutID, job.FailedFrom)
		}
		if job.Stage == StageFailed && !resumableFailedFrom(job.FailedFrom) {
			return fmt.Errorf("cut state snapshot: failed job %q does not record a resumable failed-from stage", cutID)
		}
	}
	for cutID := range manifests {
		if _, ok := jobs[cutID]; !ok {
			return fmt.Errorf("cut state snapshot: manifest %q has no job", cutID)
		}
	}

	idempotency := make(map[string]CutID, len(data.Idempotency))
	requests := make(map[string]TriggerRequest, len(data.Requests))
	bindingCount := make(map[CutID]int, len(jobs))
	for key, cutID := range data.Idempotency {
		target := CutID(cutID)
		job, ok := jobs[target]
		if !ok {
			return fmt.Errorf("cut state snapshot: idempotency key %q targets unknown cut %q", key, cutID)
		}
		request, ok := data.Requests[key]
		if !ok {
			// An idempotency entry without its recorded request body cannot be
			// replay-checked against a new body — fail closed rather than
			// replaying blindly.
			return fmt.Errorf("cut state snapshot: idempotency key %q has no recorded request body", key)
		}
		if key != idempotencyKey(request.TenantID, request.RoomID, request.TriggerSource, request.IdempotencyKey) {
			return fmt.Errorf("cut state snapshot: idempotency key %q does not match its recorded request", key)
		}
		if job.TenantID != request.TenantID || job.RoomID != request.RoomID {
			return fmt.Errorf("cut state snapshot: idempotency key %q request does not match target job %q", key, cutID)
		}
		if target != MintCutID(request.TenantID, request.RoomID, request.TriggerSource, request.IdempotencyKey) {
			return fmt.Errorf("cut state snapshot: idempotency key %q target cut %q does not match its recorded request", key, cutID)
		}
		manifest, ok := manifests[target]
		if !ok || manifest.TenantID != job.TenantID || manifest.RoomID != job.RoomID {
			return fmt.Errorf("cut state snapshot: idempotency key %q target manifest does not match job %q", key, cutID)
		}
		// The recorded request must also match the manifest's FROZEN identity
		// fields: a tampered mode/source/key would silently change what a
		// replay of that key re-runs against the frozen cut.
		if request.Mode != manifest.Mode || request.TriggerSource != manifest.TriggerSource || request.IdempotencyKey != manifest.IdempotencyKey {
			return fmt.Errorf("cut state snapshot: idempotency key %q recorded request does not match the frozen manifest %q", key, cutID)
		}
		idempotency[key] = target
		requests[key] = request
		bindingCount[target]++
	}
	// The recorded-request map is the idempotency map's exact shadow: an
	// extra request body without a binding is snapshot corruption, not
	// dead weight to silently drop.
	if len(requests) != len(data.Requests) {
		return fmt.Errorf("cut state snapshot: recorded requests exist without idempotency bindings")
	}
	// Exact reverse binding: every restored cut was minted by exactly one
	// idempotent trigger (the key is part of MintCutID), so every job must be
	// the target of exactly one binding. A binding stripped from the snapshot
	// would silently break replay semantics for that cut — fail closed.
	for cutID := range jobs {
		if bindingCount[cutID] != 1 {
			return fmt.Errorf("cut state snapshot: job %q has %d idempotency bindings, want exactly 1", cutID, bindingCount[cutID])
		}
	}

	validateRoomLedger := func(name string, source map[string]map[string]string) (map[domain.TenantID]map[RoomID]CutID, error) {
		ledger := make(map[domain.TenantID]map[RoomID]CutID, len(source))
		for tenantID, rooms := range source {
			byRoom := make(map[RoomID]CutID, len(rooms))
			for roomID, cutID := range rooms {
				job, ok := jobs[CutID(cutID)]
				if !ok {
					return nil, fmt.Errorf("cut state snapshot: %s entry %s/%s targets unknown cut %q", name, tenantID, roomID, cutID)
				}
				if job.TenantID != domain.TenantID(tenantID) || job.RoomID != RoomID(roomID) {
					return nil, fmt.Errorf("cut state snapshot: %s entry %s/%s does not match job %q (tenant=%s room=%s)", name, tenantID, roomID, cutID, job.TenantID, job.RoomID)
				}
				byRoom[RoomID(roomID)] = CutID(cutID)
			}
			ledger[domain.TenantID(tenantID)] = byRoom
		}
		return ledger, nil
	}

	roomInFlight, err := validateRoomLedger("room_in_flight", data.RoomInFlight)
	if err != nil {
		return err
	}
	roomLastCut, err := validateRoomLedger("room_last_cut", data.RoomLastCut)
	if err != nil {
		return err
	}

	// Room-serial ledgers must hold in both directions: a live (in-flight)
	// cut occupies its room, an occupancy entry never points at a terminal
	// cut, and a last_cut entry never presents a still-running cut as
	// settled lineage. These mirror the runtime invariants maintained by
	// Trigger/Transition/Cancel/Resume/Rediagnose, so a snapshot injected
	// with a forged ledger cannot resurrect a blocked or double-active room.
	for cutID, job := range jobs {
		if !inFlight(job.Stage) {
			continue
		}
		if occupant, occupied := roomInFlight[job.TenantID][job.RoomID]; !occupied || occupant != cutID {
			return fmt.Errorf("cut state snapshot: in-flight job %q does not occupy its room", cutID)
		}
	}
	for tenantID, rooms := range roomInFlight {
		for roomID, cutID := range rooms {
			// Occupancy entries must point at jobs that are ACTUALLY in
			// flight — not merely non-terminal. A failed (or otherwise
			// settled) target would restore a permanently blocked room.
			if !inFlight(jobs[cutID].Stage) {
				return fmt.Errorf("cut state snapshot: room_in_flight %s/%s points at non-in-flight job %q (stage %s)", tenantID, roomID, cutID, jobs[cutID].Stage)
			}
		}
	}
	for tenantID, rooms := range roomLastCut {
		for roomID, cutID := range rooms {
			if inFlight(jobs[cutID].Stage) {
				return fmt.Errorf("cut state snapshot: room_last_cut %s/%s points at in-flight job %q", tenantID, roomID, cutID)
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = jobs
	s.manifests = manifests
	s.idempotency = idempotency
	s.requests = requests
	s.roomInFlight = roomInFlight
	s.roomLastCut = roomLastCut
	s.roomReservations = map[domain.TenantID]map[RoomID]bool{}
	s.audit = append([]AuditEntry(nil), data.Audit...)
	return nil
}

// validSnapshotShape rejects structurally-invalid snapshot shapes that
// json.Unmarshal would otherwise silently accept: `null`, an empty `{}`
// object (which is not a usable composition and masks a truncated write),
// top-level values of the wrong JSON type, and any missing core map — every
// core map must be present (possibly empty), so a partial image cannot mask
// a truncated write as a legitimate fresh state. A freshly-created service
// has non-nil empty maps, so a legitimate fresh state round-trips as
// {"jobs":{},"manifests":{},...} — never as bare `null` or `{}`.
func validSnapshotShape(data serviceSnapshot) bool {
	return data.Jobs != nil && data.Manifests != nil && data.Idempotency != nil &&
		data.Requests != nil && data.RoomInFlight != nil && data.RoomLastCut != nil
}
