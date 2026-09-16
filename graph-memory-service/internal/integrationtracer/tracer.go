// Package integrationtracer hosts the PG-40..43 cross-repo integration
// scenarios. It is deliberately NOT a composition root: each scenario builds
// its own in-memory authorities and reports factual outcomes as JSON for the
// Python orchestrators under
// specs/benchmark-diagnosis-replay-trajectory-export/conformance/integration.
package integrationtracer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"river2.dev/graph-memory-service/internal/artifactsecurity"
	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/diagnosis"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/modellineage"
	"river2.dev/graph-memory-service/internal/revocation"
	"river2.dev/graph-memory-service/internal/skillevolution/promotion"
)

func shaHex(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func digestOf(input string) string {
	return "sha256:" + shaHex(input)
}

// approvingGate is the tracer's deterministic disclosure gate: exactly the
// partitioning policy the frozen contract allows (same scope, or the
// private→shared partition with labels); everything else is denied with a
// reason code.
type approvingGate struct{}

func (approvingGate) Decide(_ context.Context, req diagnosis.DisclosureRequest) (diagnosis.DisclosureDecision, error) {
	if req.SourceScope == req.TargetScope {
		return diagnosis.DisclosureDecision{Allowed: true, ReasonCode: "DISCLOSURE_SAME_SCOPE"}, nil
	}
	if req.SourceScope == domain.SpacePrivate && req.TargetScope == domain.SpaceShared {
		return diagnosis.DisclosureDecision{
			Allowed:    true,
			ReasonCode: "DISCLOSURE_PARTITIONED",
			Labels:     []diagnosis.DisclosureLabel{"derived_rationale", "citations_only"},
		}, nil
	}
	return diagnosis.DisclosureDecision{Allowed: false, ReasonCode: "DISCLOSURE_DENIED_SCOPE_NARROWING"}, nil
}

// annotationRefOf renders the evidence reference form the guard authority
// resolves: "<annotation-id>@<revision>". A transition naming a ref the
// diagnosis authority cannot resolve — or one that resolves to a non-verified
// annotation, a different cut, or a different policy revision — fails closed.
func annotationRefOf(id diagnosis.AnnotationID, revision int64) string {
	return string(id) + "@" + strconv.FormatInt(revision, 10)
}

// receiptsDigestOf recomputes the canonical receipts digest over a frozen
// manifest's evidence commit receipts. The guard authority derives the
// expected value from the manifest it loads itself, so no caller can mint a
// receipts_complete fact for a manifest that does not carry the receipts.
// The canonical aggregation is the consolidationcut package's own — the same
// digest Trigger presents at the freezing→frozen guard and the durable
// authority re-derives from committed evidence.
func receiptsDigestOf(manifest consolidationcut.Manifest) string {
	return consolidationcut.ManifestReceiptsDigest(manifest)
}

// tracerGuardAuthority implements consolidationcut.GuardAuthority over the
// authorities that actually own the facts: diagnosis annotations resolve
// through the diagnosis service's read API (status, cut binding, and policy
// revision are all checked), receipts through the frozen manifest stored by
// the cut service, proposals and side effects through append-only registries
// the scenarios populate only when the real flow registers them. Unknown or
// unresolvable evidence fails closed.
type tracerGuardAuthority struct {
	diagnosis  *diagnosis.Service
	manifestOf func(context.Context, domain.TenantID, consolidationcut.CutID) (consolidationcut.Manifest, error)

	proposals   map[consolidationcut.CutID][]string
	sideEffects map[consolidationcut.CutID]bool
}

func newTracerGuardAuthority(diagnosisService *diagnosis.Service) *tracerGuardAuthority {
	return &tracerGuardAuthority{
		diagnosis:   diagnosisService,
		proposals:   map[consolidationcut.CutID][]string{},
		sideEffects: map[consolidationcut.CutID]bool{},
	}
}

func (a *tracerGuardAuthority) QuotaGranted(_ context.Context, _ domain.TenantID, _ consolidationcut.CutID, _ string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, nil
}

func (a *tracerGuardAuthority) ReceiptsComplete(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, receiptsDigest string) (bool, consolidationcut.GuardConfirmation, error) {
	if a.manifestOf == nil || receiptsDigest == "" {
		return false, consolidationcut.GuardConfirmation{}, nil
	}
	manifest, err := a.manifestOf(ctx, tenantID, cutID)
	if err != nil {
		return false, consolidationcut.GuardConfirmation{}, err
	}
	ok := receiptsDigestOf(manifest) == receiptsDigest
	return ok, consolidationcut.ConfirmFacts("receipts_complete_for_all_sealed_segments", map[string]string{
		"cut": string(cutID), "manifest_digest": receiptsDigestOf(manifest), "matched": strconv.FormatBool(ok),
	}), nil
}

func (a *tracerGuardAuthority) DiagnosisVerified(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, annotationRef, policyRevision string) (bool, consolidationcut.GuardConfirmation, error) {
	id, revision, ok := strings.Cut(annotationRef, "@")
	if !ok || id == "" {
		return false, consolidationcut.GuardConfirmation{}, nil
	}
	parsed, err := strconv.ParseInt(revision, 10, 64)
	if err != nil {
		return false, consolidationcut.GuardConfirmation{}, nil
	}
	annotation, found := a.diagnosis.LookupAnnotation(ctx, tenantID, diagnosis.AnnotationID(id), parsed)
	if !found {
		return false, consolidationcut.GuardConfirmation{}, nil
	}
	verified := annotation.Status == diagnosis.StatusVerified &&
		annotation.CutID == cutID &&
		annotation.DiagnosisPolicyRevision == policyRevision
	return verified, consolidationcut.ConfirmFacts("diagnosis_verified", map[string]string{
		"annotation": id, "revision": revision, "status": string(annotation.Status),
		"cut": string(annotation.CutID), "policy": annotation.DiagnosisPolicyRevision,
		"verified": strconv.FormatBool(verified),
	}), nil
}

func (a *tracerGuardAuthority) ProjectionComplete(_ context.Context, _ domain.TenantID, _ consolidationcut.CutID, _ string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, nil
}

func (a *tracerGuardAuthority) ProposalRefs(_ context.Context, _ domain.TenantID, cutID consolidationcut.CutID) ([]string, consolidationcut.GuardConfirmation, error) {
	refs := append([]string(nil), a.proposals[cutID]...)
	return refs, consolidationcut.ConfirmFacts("proposal_refs", map[string]string{
		"cut": string(cutID), "refs": strings.Join(refs, ","), "count": strconv.Itoa(len(refs)),
	}), nil
}

func (a *tracerGuardAuthority) ReplayPassed(_ context.Context, _ domain.TenantID, _ consolidationcut.CutID, _ string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, nil
}

func (a *tracerGuardAuthority) CASBaseRevisionMatches(_ context.Context, _ domain.TenantID, _ consolidationcut.CutID, _ string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, nil
}

func (a *tracerGuardAuthority) SideEffectsCompleted(_ context.Context, _ domain.TenantID, cutID consolidationcut.CutID) (bool, consolidationcut.GuardConfirmation, error) {
	completed := a.sideEffects[cutID]
	return completed, consolidationcut.ConfirmFacts("side_effects_completed", map[string]string{
		"cut": string(cutID), "completed": strconv.FormatBool(completed),
	}), nil
}

// ReserveFacts/ReleaseFacts satisfy the R3 apply-time reservation seam. The
// tracer drives its authorities single-threaded (each scenario runs
// sequentially and every mutation — annotation registration, proposal
// registration, replay verdicts — happens between transitions, never during
// one), so the reservation is a pass-through here; the seam exists so the
// composition wiring (PG-50A) inherits the same apply-time CAS contract the
// stub authority regression tests pin down.
func (a *tracerGuardAuthority) ReserveFacts(context.Context, domain.TenantID, consolidationcut.CutID) error {
	return nil
}

func (a *tracerGuardAuthority) ReleaseFacts(context.Context, domain.TenantID, consolidationcut.CutID) {
}

// hostFreeze is the contract-shaped Host freeze output the tracer consumes
// (produced by the Host integrationtracer freeze scenario).
type hostFreeze struct {
	TenantID            string           `json:"tenant_id"`
	RoomID              string           `json:"room_id"`
	RoomEpoch           int64            `json:"room_epoch"`
	RoomSequence        int64            `json:"room_sequence"`
	SealedSegmentIDs    []string         `json:"sealed_segment_ids"`
	DeferredSegmentIDs  []string         `json:"deferred_segment_ids"`
	SubmissionPermitted bool             `json:"submission_permitted"`
	Receipts            []hostReceipt    `json:"receipts"`
	Spaces              []hostSpaceFacts `json:"spaces"`
}

// hostReceipt is one evidence commit receipt as the Host reports it.
type hostReceipt struct {
	ReceiptID   string   `json:"receipt_id"`
	SegmentID   string   `json:"segment_id"`
	BatchIDs    []string `json:"batch_ids"`
	BatchDigest string   `json:"batch_digest"`
}

// hostBarrierFacts is the contract-shaped Host benchmark-barrier output:
// the released-barrier state, the next-episode admission decision, and the
// per-logical-run terminal ledger the batch diagnosis consumes (PG-41).
type hostBarrierFacts struct {
	TenantID string `json:"tenant_id"`
	BatchID  string `json:"batch_id"`
	ArmID    string `json:"arm_id"`
	Seed     int64  `json:"seed"`
	Barrier  struct {
		BlockedWhilePending bool `json:"blocked_while_pending"`
		BlockedWhilePartial bool `json:"blocked_while_partial"`
		ReleasedWhenDurable bool `json:"released_when_durable"`
	} `json:"barrier"`
	Admission struct {
		NextEpisodeAdmitted bool     `json:"next_episode_admitted"`
		BlockingOutboxIDs   []string `json:"blocking_outbox_ids"`
		Reason              string   `json:"reason"`
	} `json:"admission"`
	Restart struct {
		RecoveredRows    int      `json:"recovered_rows"`
		StagedRowBlocked bool     `json:"staged_row_blocked_barrier"`
		RecoveredIDs     []string `json:"recovered_ids"`
	} `json:"restart"`
	LogicalRuns []hostLogicalRunFacts `json:"logical_runs"`
}

type hostLogicalRunFacts struct {
	LogicalRunID     string              `json:"logical_run_id"`
	TaskID           string              `json:"task_id"`
	EpisodeID        string              `json:"episode_id"`
	RoomID           string              `json:"room_id"`
	Attempts         int                 `json:"attempts"`
	TerminalState    string              `json:"terminal_state"`
	FailureReason    string              `json:"failure_reason"`
	RoomEpoch        int64               `json:"room_epoch"`
	RoomClosed       bool                `json:"room_closed"`
	SealedSegmentIDs []string            `json:"sealed_segment_ids"`
	Receipts         []hostReceipt       `json:"receipts"`
	Spaces           []hostSpaceFacts    `json:"spaces"`
	Children         []hostAgentRunFacts `json:"children"`
}

// hostAgentRunFacts is one registered child agent run of a logical run, as
// the Host terminal ledger reports it (mirrors the Host agentRunResult).
type hostAgentRunFacts struct {
	AgentRunID string `json:"agent_run_id"`
	Status     string `json:"status"`
	Error      string `json:"error"`
}

// hostSpaceFacts is one frozen space scope with its own evidence batch set
// and projection watermarks (mirrors the Host hostSpaceJSON).
type hostSpaceFacts struct {
	SpaceID               string   `json:"space_id"`
	Scope                 string   `json:"scope"`
	EvidenceBatchIDs      []string `json:"evidence_batch_ids"`
	ProjectionHeadVersion int64    `json:"projection_head_version"`
	QueryWatermark        int64    `json:"query_watermark"`
}

// frozenBatchManifest mirrors the PG-00B evaluation_batch_manifest envelope;
// GMS re-parses it locally (no cross-repo import) to cross-check the Host
// barrier facts against the frozen known set.
type frozenBatchManifest struct {
	EvaluationBatchID string `json:"evaluation_batch_id"`
	ArmID             string `json:"arm_id"`
	Seed              int64  `json:"seed"`
	LogicalRuns       []struct {
		LogicalRunID      string   `json:"logical_run_id"`
		TaskID            string   `json:"task_id"`
		RoomID            string   `json:"room_id"`
		EpisodeID         string   `json:"episode_id"`
		ExpectedAgentRuns []string `json:"expected_agent_runs"`
	} `json:"logical_runs"`
}

// manifestFreezer replays an externally produced freeze snapshot as the
// authoritative GMS-side manifest source; it is the adapter the composition
// root (PG-50A) will replace with the real repository-backed freezer. The
// entered/release channels gate one freeze at a time so the tracer can prove
// Room-serial admission deterministically.
type manifestFreezer struct {
	perRoom map[string]func(req consolidationcut.TriggerRequest) consolidationcut.Manifest
	entered chan string
	release chan string
}

func (f *manifestFreezer) Freeze(_ context.Context, req consolidationcut.TriggerRequest) (consolidationcut.Manifest, error) {
	if f.entered != nil {
		f.entered <- string(req.RoomID)
		<-f.release
	}
	builder, ok := f.perRoom[string(req.RoomID)]
	if !ok {
		return consolidationcut.Manifest{}, fmt.Errorf("no freeze builder for room %s", req.RoomID)
	}
	return builder(req), nil
}

func annotationTargetOf(segmentID, callID string) diagnosis.AnnotationTarget {
	call := callID
	return diagnosis.AnnotationTarget{SegmentID: segmentID, CallID: &call}
}

// ProductionCut runs the PG-40 GMS half: exact-batch trigger over the Host
// freeze, Room-serial concurrency, threshold no_change, the diagnosis chain,
// and the guarded stage walk with partial-failure retry. Every guarded hop
// carries evidence references the configured GuardAuthority resolves against
// the owning authorities; forged, unverified, stale-policy, and
// post-rediagnose-superseded references are all probed fail-closed.
func ProductionCut(hostFreezePath string) (map[string]any, error) {
	raw, err := os.ReadFile(hostFreezePath)
	if err != nil {
		return nil, err
	}
	var host hostFreeze
	if err := json.Unmarshal(raw, &host); err != nil {
		return nil, err
	}
	if host.TenantID == "" || host.RoomID == "" || len(host.SealedSegmentIDs) == 0 || len(host.Spaces) < 2 {
		return nil, fmt.Errorf("host freeze fixture is incomplete: %s", hostFreezePath)
	}
	ctx := context.Background()
	tenant := domain.TenantID(host.TenantID)

	policyRevisions := consolidationcut.PolicyRevisions{
		DiagnosisPolicyRevision: "diagnosis-v1",
		ReplayPolicyRevision:    "replay-v1",
		AggregationPolicyDigest: digestOf("aggregation-policy-v1"),
	}
	buildMain := func(req consolidationcut.TriggerRequest) consolidationcut.Manifest {
		manifest := consolidationcut.Manifest{
			TenantID:              req.TenantID,
			RoomID:                req.RoomID,
			Mode:                  req.Mode,
			TriggerSource:         req.TriggerSource,
			IdempotencyKey:        req.IdempotencyKey,
			RoomSequenceWatermark: host.RoomSequence,
			PolicyRevisions:       policyRevisions,
		}
		for _, segment := range host.SealedSegmentIDs {
			manifest.SealedSegmentIDs = append(manifest.SealedSegmentIDs, consolidationcut.SegmentID(segment))
		}
		for _, space := range host.Spaces {
			scope := consolidationcut.SpaceScope{SpaceID: domain.SpaceID(space.SpaceID), Scope: domain.SpaceScope(space.Scope)}
			for _, batch := range space.EvidenceBatchIDs {
				scope.EvidenceBatchIDs = append(scope.EvidenceBatchIDs, domain.BatchID(batch))
			}
			head := domain.ProjectionVersion(space.ProjectionHeadVersion)
			watermark := space.QueryWatermark
			scope.ProjectionHeadVersion = &head
			scope.QueryWatermark = &watermark
			manifest.SpaceScopes = append(manifest.SpaceScopes, scope)
		}
		for _, receipt := range host.Receipts {
			manifest.EvidenceCommitReceipts = append(manifest.EvidenceCommitReceipts, evidenceReceiptOf(receipt))
		}
		return manifest
	}
	buildEmpty := func(req consolidationcut.TriggerRequest) consolidationcut.Manifest {
		return consolidationcut.Manifest{
			TenantID:              req.TenantID,
			RoomID:                req.RoomID,
			Mode:                  req.Mode,
			TriggerSource:         req.TriggerSource,
			IdempotencyKey:        req.IdempotencyKey,
			RoomSequenceWatermark: host.RoomSequence,
			PolicyRevisions:       policyRevisions,
		}
	}

	freezer := &manifestFreezer{perRoom: map[string]func(req consolidationcut.TriggerRequest) consolidationcut.Manifest{
		host.RoomID:      buildMain,
		"room-threshold": buildEmpty,
	}}
	service := consolidationcut.NewService(nil, freezer)
	diagnosisService := diagnosis.NewService(nil, approvingGate{}, nil)
	authority := newTracerGuardAuthority(diagnosisService)
	authority.manifestOf = service.GetManifest
	service.SetGuardAuthority(authority)

	// Room-serial proof: trigger A wins the slot and blocks inside the gated
	// freeze; trigger B (same room, same body) must be rejected against the
	// reservation while A is outside the lock, auditable as a room-serial
	// conflict and pointing at A's in-flight cut.
	freezer.entered = make(chan string, 1)
	freezer.release = make(chan string, 1)
	trigger := consolidationcut.TriggerRequest{
		TenantID: tenant, RoomID: consolidationcut.RoomID(host.RoomID),
		Mode: consolidationcut.ModeForce, TriggerSource: "benchmark",
		PrincipalID: domain.PrincipalID("diagnosis-agent"), IdempotencyKey: "pg40-main",
	}
	type triggerResult struct {
		job       consolidationcut.Job
		duplicate bool
		err       error
	}
	resultA := make(chan triggerResult, 1)
	go func() {
		job, duplicate, err := service.Trigger(ctx, trigger)
		resultA <- triggerResult{job, duplicate, err}
	}()
	<-freezer.entered
	jobB, duplicateB, errB := service.Trigger(ctx, trigger)
	freezer.release <- host.RoomID
	resA := <-resultA
	if resA.err != nil {
		return nil, fmt.Errorf("trigger A: %w", resA.err)
	}
	// The reservation-held rejection mints no cut (empty job, no error, no
	// duplicate flag) and is auditable; A remains the only frozen cut.
	concurrentRejected := !duplicateB && errB == nil && jobB.CutID == ""
	// Disarm the gate (ordered after A's completion via resultA) so the
	// remaining triggers freeze without blocking.
	freezer.entered = nil
	freezer.release = nil

	// Threshold mode with no accumulated evidence lands auditable no_change.
	jobThreshold, _, err := service.Trigger(ctx, consolidationcut.TriggerRequest{
		TenantID: tenant, RoomID: "room-threshold",
		Mode: consolidationcut.ModeThreshold, TriggerSource: "benchmark",
		PrincipalID: domain.PrincipalID("diagnosis-agent"), IdempotencyKey: "pg40-threshold",
	})
	if err != nil {
		return nil, fmt.Errorf("threshold trigger: %w", err)
	}

	// Diagnosis chain over the main cut's exact private scope (Q53=A).
	privateSpaces := []hostSpaceFacts{}
	for _, space := range host.Spaces {
		if space.Scope == string(domain.SpacePrivate) {
			privateSpaces = append(privateSpaces, space)
		}
	}
	scopeBindings := []diagnosis.PrivateSpaceBinding{}
	for _, space := range privateSpaces {
		scopeBindings = append(scopeBindings, diagnosis.PrivateSpaceBinding{
			SpaceID: domain.SpaceID(space.SpaceID), RoomID: consolidationcut.RoomID(host.RoomID), RoomEpoch: host.RoomEpoch,
		})
	}
	if len(scopeBindings) == 0 {
		return nil, fmt.Errorf("host freeze fixture carries no private spaces")
	}
	if err := diagnosisService.RegisterCutScope(tenant, resA.job.CutID, scopeBindings); err != nil {
		return nil, fmt.Errorf("register cut scope: %w", err)
	}
	issuedAt := time.Now()
	capability, _, err := diagnosisService.IssueCapability(ctx, diagnosis.Capability{
		CapabilityID: "capability-pg40", TenantID: tenant, CutID: resA.job.CutID,
		PrincipalID:        domain.PrincipalID("diagnosis-agent"),
		BoundPrivateSpaces: scopeBindings,
		Operations:         []diagnosis.CapabilityOperation{diagnosis.CapabilityOperationRead},
		IssuedAt:           issuedAt,
		ExpiresAt:          issuedAt.Add(time.Hour),
	})
	if err != nil {
		return nil, fmt.Errorf("issue capability: %w", err)
	}
	// Capability-bounded reads over exactly the frozen evidence batches of
	// each private space's own projection (P1-8: no shared batch set bleeds
	// into the private scope).
	reads := []map[string]any{}
	for i, binding := range scopeBindings {
		for _, batch := range privateSpaces[i].EvidenceBatchIDs {
			entry, err := diagnosisService.ReadPrivate(ctx, diagnosis.PrivateReadRequest{
				Capability: capability, SpaceID: binding.SpaceID,
				BatchID:  domain.BatchID(batch),
				EventIDs: []string{"event-1", "event-2"},
				ReadAt:   issuedAt,
			})
			if err != nil {
				return nil, fmt.Errorf("read %s/%s: %w", binding.SpaceID, batch, err)
			}
			reads = append(reads, map[string]any{
				"space_id": string(entry.SpaceID), "batch_id": batch,
				"digest": entry.Digest, "previous": entry.PreviousDigest,
			})
		}
	}

	cutID := resA.job.CutID
	seed := func(id string, status diagnosis.AnnotationStatus, policyRevision string, citations []diagnosis.EvidenceCitation) (diagnosis.AnnotationRef, error) {
		annotation := diagnosis.Annotation{
			AnnotationID: diagnosis.AnnotationID(id), Revision: 1, CutID: cutID, TenantID: tenant,
			Target:                  annotationTargetOf(host.SealedSegmentIDs[0], id+"-call"),
			Status:                  status,
			EvidenceCitations:       citations,
			DiagnosisPolicyRevision: policyRevision,
			ExecutionRecipeDigest:   digestOf("recipe-v1"),
		}
		stored, _, err := diagnosisService.AppendAnnotation(ctx, annotation)
		if err != nil {
			return diagnosis.AnnotationRef{}, err
		}
		return diagnosis.AnnotationRef{AnnotationID: stored.AnnotationID, Revision: stored.Revision}, nil
	}
	verifiedRef, err := seed("annotation-verified", diagnosis.StatusVerified, "diagnosis-v1", []diagnosis.EvidenceCitation{
		{BatchID: domain.BatchID(privateSpaces[0].EvidenceBatchIDs[0]), EventIDs: []string{"event-1"}},
	})
	if err != nil {
		return nil, fmt.Errorf("seed verified annotation: %w", err)
	}
	missingRef, err := seed("annotation-missing", diagnosis.StatusMissing, "diagnosis-v1", nil)
	if err != nil {
		return nil, fmt.Errorf("seed missing annotation: %w", err)
	}
	aggregate, err := diagnosisService.Aggregate(ctx, diagnosis.AggregationRequest{
		TenantID: tenant, CutID: cutID,
		AnnotationRefs: []diagnosis.AnnotationRef{verifiedRef, missingRef},
		PolicyDigest:   digestOf("aggregation-policy-v1"),
	})
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}
	disclosure, err := diagnosisService.Disclose(ctx, diagnosis.DisclosureRequest{
		Annotation: diagnosis.Annotation{
			AnnotationID: verifiedRef.AnnotationID, Revision: verifiedRef.Revision, CutID: cutID, TenantID: tenant,
			DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: digestOf("recipe-v1"),
		},
		SourceScope: domain.SpacePrivate, TargetScope: domain.SpaceShared,
		Rationale: "space publish rationale",
	})
	if err != nil {
		return nil, fmt.Errorf("disclose: %w", err)
	}

	// Guarded stage walk with a partial-failure retry (SC-4.2/4.3). The
	// first pass under diagnosis-v1 probes every evidence failure mode
	// fail-closed, then fails with the canonical reason; governance
	// re-diagnoses under diagnosis-v2 bound to a new run; the superseded v1
	// evidence is probed against the v2 job; and only the fresh v2
	// annotation admits the second pass, which completes with per-space
	// terminal results.
	transition := func(req consolidationcut.TransitionRequest) (consolidationcut.Job, error) {
		return service.Transition(ctx, req)
	}
	probeConsolidating := func(job consolidationcut.Job, evidence consolidationcut.TransitionEvidence) bool {
		_, err := transition(consolidationcut.TransitionRequest{
			TenantID: tenant, CutID: job.CutID, ExpectedJobVersion: job.JobVersion,
			To: consolidationcut.StageConsolidating, Evidence: evidence,
		})
		return errors.Is(err, consolidationcut.ErrGuardNotSatisfied)
	}
	diagnosing, err := transition(consolidationcut.TransitionRequest{
		TenantID: tenant, CutID: cutID, ExpectedJobVersion: resA.job.JobVersion,
		To: consolidationcut.StageDiagnosing,
	})
	if err != nil {
		return nil, fmt.Errorf("frozen->diagnosing: %w", err)
	}
	guardProbes := map[string]bool{
		"no_evidence": probeConsolidating(diagnosing, consolidationcut.TransitionEvidence{}),
		"forged_ref": probeConsolidating(diagnosing, consolidationcut.TransitionEvidence{
			DiagnosisAnnotationRef: "annotation-forged@1", DiagnosisPolicyRevision: "diagnosis-v1",
		}),
		"unverified_annotation": probeConsolidating(diagnosing, consolidationcut.TransitionEvidence{
			DiagnosisAnnotationRef:  annotationRefOf(missingRef.AnnotationID, missingRef.Revision),
			DiagnosisPolicyRevision: "diagnosis-v1",
		}),
		"wrong_policy_binding": probeConsolidating(diagnosing, consolidationcut.TransitionEvidence{
			DiagnosisAnnotationRef:  annotationRefOf(verifiedRef.AnnotationID, verifiedRef.Revision),
			DiagnosisPolicyRevision: "diagnosis-v0",
		}),
	}
	partiallyFailed, err := transition(consolidationcut.TransitionRequest{
		TenantID: tenant, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
		To: consolidationcut.StagePartiallyFailed, Reason: "DIAGNOSIS_FAILED_OR_INCONCLUSIVE",
	})
	if err != nil {
		return nil, fmt.Errorf("diagnosing->partially_failed: %w", err)
	}
	rediagnosed, err := service.Rediagnose(ctx, consolidationcut.RediagnoseRequest{
		TenantID: tenant, CutID: partiallyFailed.CutID, ExpectedJobVersion: partiallyFailed.JobVersion,
		PrincipalID:             domain.PrincipalID("governance-pg40"),
		DiagnosisPolicyRevision: "diagnosis-v2",
		NewDiagnosisRunRef:      "diagnosis-run-pg40-2",
	})
	if err != nil {
		return nil, fmt.Errorf("rediagnose: %w", err)
	}
	// The superseded v1 evidence fails closed against the v2 job in both
	// mismatch shapes: v1 ref under the v1 policy (rejected by the active
	// policy binding) and v1 ref claimed under the v2 policy (rejected by
	// the diagnosis authority: the annotation itself is bound to v1).
	staleProbes := map[string]bool{
		"v1_evidence_after_rediagnose": probeConsolidating(rediagnosed, consolidationcut.TransitionEvidence{
			DiagnosisAnnotationRef:  annotationRefOf(verifiedRef.AnnotationID, verifiedRef.Revision),
			DiagnosisPolicyRevision: "diagnosis-v1",
		}),
		"v1_annotation_claimed_as_v2": probeConsolidating(rediagnosed, consolidationcut.TransitionEvidence{
			DiagnosisAnnotationRef:  annotationRefOf(verifiedRef.AnnotationID, verifiedRef.Revision),
			DiagnosisPolicyRevision: "diagnosis-v2",
		}),
	}
	// The re-diagnosis run produces a fresh immutable verified annotation
	// under diagnosis-v2; it is the only evidence that admits the hop.
	verifiedV2Ref, err := seed("annotation-verified-v2", diagnosis.StatusVerified, "diagnosis-v2", []diagnosis.EvidenceCitation{
		{BatchID: domain.BatchID(privateSpaces[0].EvidenceBatchIDs[0]), EventIDs: []string{"event-1", "event-2"}},
	})
	if err != nil {
		return nil, fmt.Errorf("seed v2 annotation: %w", err)
	}
	v2Evidence := consolidationcut.TransitionEvidence{
		DiagnosisAnnotationRef:  annotationRefOf(verifiedV2Ref.AnnotationID, verifiedV2Ref.Revision),
		DiagnosisPolicyRevision: "diagnosis-v2",
	}
	consolidating, err := transition(consolidationcut.TransitionRequest{
		TenantID: tenant, CutID: rediagnosed.CutID, ExpectedJobVersion: rediagnosed.JobVersion,
		To: consolidationcut.StageConsolidating, Evidence: v2Evidence,
	})
	if err != nil {
		return nil, fmt.Errorf("diagnosing->consolidating: %w", err)
	}
	stampedEvidence := consolidating.LastTransitionEvidence
	stampedEvidence.AuthorityFactsDigest = ""
	if stampedEvidence != v2Evidence {
		return nil, fmt.Errorf("admitted transition did not stamp its evidence: %#v", consolidating.LastTransitionEvidence)
	}
	// R3: the admission is also bound to the authority fact snapshot — the
	// digest is service-resolved (Phase B resolve → Phase C re-resolve), so
	// it must be present and non-forgable.
	authorityFactsBound := consolidating.LastTransitionEvidence.AuthorityFactsDigest != ""
	if !authorityFactsBound {
		return nil, fmt.Errorf("admitted transition did not stamp the authority facts digest")
	}
	spaceResults := make([]consolidationcut.SpaceResult, 0, len(host.Spaces))
	publishedVersion := domain.ProjectionVersion(11)
	for i, space := range host.Spaces {
		result := consolidationcut.SpaceResult{SpaceID: domain.SpaceID(space.SpaceID), Result: consolidationcut.SpaceResultNoChange}
		if i == 0 {
			result = consolidationcut.SpaceResult{
				SpaceID: domain.SpaceID(space.SpaceID), Result: consolidationcut.SpaceResultPublished,
				PublishedVersion: &publishedVersion,
			}
		}
		spaceResults = append(spaceResults, result)
	}
	completed, err := transition(consolidationcut.TransitionRequest{
		TenantID: tenant, CutID: consolidating.CutID, ExpectedJobVersion: consolidating.JobVersion,
		To: consolidationcut.StageCompleted, SpaceResults: spaceResults,
	})
	if err != nil {
		return nil, fmt.Errorf("consolidating->completed: %w", err)
	}

	audits := service.AuditTrail(tenant)
	kinds := map[string]int{}
	for _, entry := range audits {
		kinds[entry.Kind]++
	}
	diagnosisAudits := diagnosisService.AuditChainDigest(tenant)

	return map[string]any{
		"trigger": map[string]any{
			"cut_id": string(cutID), "stage": string(resA.job.Stage), "duplicate": resA.duplicate,
			"sealed_segments": len(host.SealedSegmentIDs), "deferred_segments": len(host.DeferredSegmentIDs),
			"receipts": len(host.Receipts),
		},
		"concurrent": map[string]any{
			"rejected_same_body": concurrentRejected,
			"no_new_cut_minted":  jobB.CutID == "",
			"audit_kind":         kinds["trigger_room_serial_conflict"] > 0,
		},
		"threshold": map[string]any{
			"stage": string(jobThreshold.Stage), "outcome": string(jobThreshold.Outcome),
			"audit_kind": kinds["trigger_threshold_no_change"] > 0,
		},
		"diagnosis": map[string]any{
			"private_spaces":     len(scopeBindings),
			"reads":              reads,
			"masked_count":       len(aggregate.MaskedAnnotations),
			"source_count":       len(aggregate.SourceAnnotations),
			"confidence":         aggregate.Confidence,
			"disclosure":         disclosure.Decision.Allowed,
			"reason_code":        disclosure.Decision.ReasonCode,
			"public_rationale":   disclosure.PublicRationale,
			"private_rationale":  disclosure.PrivateRationale,
			"audit_chain_digest": diagnosisAudits,
		},
		"walk": map[string]any{
			"stages": []string{
				string(diagnosing.Stage), string(partiallyFailed.Stage), string(rediagnosed.Stage),
				string(consolidating.Stage), string(completed.Stage),
			},
			"failure_reasons":       partiallyFailed.FailureReasons,
			"policy_revision":       completed.DiagnosisPolicyRevision,
			"policy_walked":         completed.DiagnosisPolicyRevision == "diagnosis-v2",
			"rediagnose_run_ref":    completed.NewDiagnosisRunRef,
			"space_results":         len(completed.SpaceResults),
			"outcome":               string(completed.Outcome),
			"guard_rejected":        kinds["transition_guard_rejected"] > 0,
			"rediagnose_audited":    kinds["rediagnose"] > 0,
			"evidence_stamped":      completed.LastTransitionEvidence.DiagnosisAnnotationRef == "" && stampedEvidence == v2Evidence,
			"authority_facts_bound": authorityFactsBound,
			"guard_probes":          guardProbes,
			"stale_policy_probes":   staleProbes,
		},
		"audit_kinds": kinds,
	}, nil
}

func evidenceReceiptOf(receipt hostReceipt) consolidationcut.EvidenceCommitReceipt {
	converted := consolidationcut.EvidenceCommitReceipt{
		ReceiptID:   consolidationcut.ReceiptID(receipt.ReceiptID),
		SegmentID:   consolidationcut.SegmentID(receipt.SegmentID),
		BatchDigest: receipt.BatchDigest,
	}
	for _, batch := range receipt.BatchIDs {
		converted.BatchIDs = append(converted.BatchIDs, domain.BatchID(batch))
	}
	return converted
}

var sha256DigestShape = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// strictDecodeFile parses exactly one JSON document into out. It fails
// closed on unknown fields (a future or forged contract cannot smuggle data
// past the boundary), duplicate keys within any object, trailing data after
// the first document, and non-JSON content (R1).
func strictDecodeFile(path string, out any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := checkSingleJSONObject(raw); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s: trailing data after the single JSON document", path)
	}
	return nil
}

// checkSingleJSONObject walks the raw token stream and rejects duplicate
// keys inside any object (encoding/json itself silently keeps the last
// duplicate, which would let a forged fact override a validated one).
func checkSingleJSONObject(raw []byte) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	type frame struct {
		object    bool
		expectKey bool
		keys      map[string]bool
	}
	var stack []frame
	for {
		token, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				stack = append(stack, frame{object: true, expectKey: true, keys: map[string]bool{}})
				continue
			case '[':
				stack = append(stack, frame{})
				continue
			case '}', ']':
				if len(stack) == 0 {
					return errors.New("unbalanced JSON containers")
				}
				stack = stack[:len(stack)-1]
				if len(stack) > 0 && stack[len(stack)-1].object {
					// The closed container was the value slot of a key.
					stack[len(stack)-1].expectKey = true
				}
				continue
			}
		}
		if len(stack) == 0 {
			continue // scalar top-level document: no keys
		}
		top := &stack[len(stack)-1]
		if !top.object {
			continue // array element: a value slot
		}
		if top.expectKey {
			key, ok := token.(string)
			if !ok {
				return errors.New("non-string object key in JSON document")
			}
			if top.keys[key] {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			top.keys[key] = true
			top.expectKey = false
			continue
		}
		// A scalar consumed the value slot; the next token is a key.
		top.expectKey = true
	}
	if len(stack) != 0 {
		return errors.New("unbalanced JSON containers")
	}
	return nil
}

// validateBarrierFacts enforces the closed PG-41 facts contract: required
// identities, closed enums, digest shapes, and internal consistency between
// the barrier, admission, restart, and per-run ledger facts (R1). Every
// check runs before any cut is minted.
func validateBarrierFacts(facts *hostBarrierFacts) error {
	if facts.TenantID == "" || facts.BatchID == "" || facts.ArmID == "" {
		return errors.New("barrier facts: tenant_id/batch_id/arm_id are required")
	}
	if facts.Seed <= 0 {
		return errors.New("barrier facts: seed must be positive")
	}
	if !facts.Barrier.ReleasedWhenDurable {
		return errors.New("batch barrier not released: refusing to diagnose before all evidence projections are durable")
	}
	if !facts.Barrier.BlockedWhilePending || !facts.Barrier.BlockedWhilePartial {
		return errors.New("barrier facts: a released barrier must still evidence fail-closed blocking while pending and while partial")
	}
	if !facts.Admission.NextEpisodeAdmitted || facts.Admission.Reason != "BARRIER_RELEASED" || len(facts.Admission.BlockingOutboxIDs) != 0 {
		return errors.New("barrier facts: a released barrier must admit the next episode with no blocking outbox rows")
	}
	if facts.Restart.RecoveredRows != len(facts.Restart.RecoveredIDs) {
		return errors.New("barrier facts: restart recovered_rows does not match recovered_ids")
	}
	if !facts.Restart.StagedRowBlocked {
		return errors.New("barrier facts: restart recovery must evidence that a staged (uncommitted) row still blocks the barrier")
	}
	if len(facts.LogicalRuns) == 0 {
		return errors.New("barrier facts carry no logical runs")
	}

	// Outbox accounting: the Host settles one shared and one private
	// projection row per commit receipt; recovery must have seen them all.
	outboxRows := 0
	for _, run := range facts.LogicalRuns {
		if err := validateLogicalRunFacts(run); err != nil {
			return fmt.Errorf("logical run %s: %w", run.LogicalRunID, err)
		}
		outboxRows += len(run.Receipts) * 2
	}
	if facts.Restart.RecoveredRows != outboxRows {
		return fmt.Errorf("barrier facts: restart recovered_rows=%d does not match the ledger's outbox rows=%d", facts.Restart.RecoveredRows, outboxRows)
	}
	recoveredSet := map[string]bool{}
	for _, id := range facts.Restart.RecoveredIDs {
		if recoveredSet[id] {
			return fmt.Errorf("barrier facts: duplicate recovered outbox id %s", id)
		}
		recoveredSet[id] = true
	}
	return nil
}

func validateLogicalRunFacts(run hostLogicalRunFacts) error {
	if run.LogicalRunID == "" || run.TaskID == "" || run.EpisodeID == "" || run.RoomID == "" {
		return errors.New("identity fields are required")
	}
	if run.Attempts < 1 {
		return errors.New("attempts must be >= 1")
	}
	if run.RoomEpoch <= 0 || !run.RoomClosed {
		return errors.New("room must be closed at a positive epoch")
	}
	switch run.TerminalState {
	case "completed":
		if run.FailureReason != "" {
			return errors.New("a completed run must not carry a failure reason")
		}
	case "failed":
		if run.FailureReason == "" {
			return errors.New("a failed run must preserve its failure reason")
		}
	default:
		return fmt.Errorf("terminal_state %q is not in the closed enum {completed, failed}", run.TerminalState)
	}
	if len(run.Children) == 0 {
		return errors.New("the registered child agent runs are required")
	}
	childIDs := map[string]bool{}
	failedChild := false
	for _, child := range run.Children {
		if child.AgentRunID == "" || childIDs[child.AgentRunID] {
			return fmt.Errorf("child agent run %q is empty or duplicated", child.AgentRunID)
		}
		childIDs[child.AgentRunID] = true
		switch child.Status {
		case "succeeded":
			if child.Error != "" {
				return fmt.Errorf("child %s: a succeeded child must not carry an error", child.AgentRunID)
			}
		case "failed":
			failedChild = true
			if child.Error == "" {
				return fmt.Errorf("child %s: a failed child must preserve its error", child.AgentRunID)
			}
		default:
			return fmt.Errorf("child %s: status %q is not in the closed enum {succeeded, failed}", child.AgentRunID, child.Status)
		}
	}
	if (run.TerminalState == "failed") != failedChild {
		return errors.New("run terminal state does not agree with its child ledger")
	}
	if len(run.SealedSegmentIDs) == 0 {
		return errors.New("sealed_segment_ids are required")
	}
	sealed := map[string]bool{}
	for _, segment := range run.SealedSegmentIDs {
		if segment == "" || sealed[segment] {
			return fmt.Errorf("sealed segment %q is empty or duplicated", segment)
		}
		sealed[segment] = true
	}
	if len(run.Receipts) == 0 {
		return errors.New("evidence commit receipts are required")
	}
	receiptIDs := map[string]bool{}
	covered := map[string]int{}
	receiptBatches := map[string]bool{}
	for _, receipt := range run.Receipts {
		if receipt.ReceiptID == "" || receiptIDs[receipt.ReceiptID] {
			return fmt.Errorf("receipt %q is empty or duplicated", receipt.ReceiptID)
		}
		receiptIDs[receipt.ReceiptID] = true
		if !sealed[receipt.SegmentID] {
			return fmt.Errorf("receipt %s cites segment %s outside the sealed set", receipt.ReceiptID, receipt.SegmentID)
		}
		covered[receipt.SegmentID]++
		if len(receipt.BatchIDs) == 0 {
			return fmt.Errorf("receipt %s carries no projection batches", receipt.ReceiptID)
		}
		for _, batch := range receipt.BatchIDs {
			if batch == "" || receiptBatches[batch] {
				return fmt.Errorf("receipt %s: batch %q is empty or duplicated across receipts", receipt.ReceiptID, batch)
			}
			receiptBatches[batch] = true
		}
		if !sha256DigestShape.MatchString(receipt.BatchDigest) {
			return fmt.Errorf("receipt %s: batch_digest %q is not a sha256 digest", receipt.ReceiptID, receipt.BatchDigest)
		}
	}
	for segment := range sealed {
		if covered[segment] != 1 {
			return fmt.Errorf("sealed segment %s is covered by %d receipts, want exactly 1", segment, covered[segment])
		}
	}
	if len(run.Spaces) == 0 {
		return errors.New("space scopes are required")
	}
	spaceIDs := map[string]bool{}
	sharedBatches := map[string]bool{}
	privateBatches := map[string]bool{}
	hasPrivate, hasShared := false, false
	for _, space := range run.Spaces {
		if space.SpaceID == "" || spaceIDs[space.SpaceID] {
			return fmt.Errorf("space %q is empty or duplicated", space.SpaceID)
		}
		spaceIDs[space.SpaceID] = true
		if len(space.EvidenceBatchIDs) == 0 {
			return fmt.Errorf("space %s carries no evidence batches", space.SpaceID)
		}
		if space.ProjectionHeadVersion <= 0 || space.QueryWatermark <= 0 {
			return fmt.Errorf("space %s: projection head and query watermark must be positive", space.SpaceID)
		}
		switch domain.SpaceScope(space.Scope) {
		case domain.SpacePrivate:
			hasPrivate = true
		case domain.SpaceShared:
			hasShared = true
		default:
			return fmt.Errorf("space %s: scope %q is not in the closed enum", space.SpaceID, space.Scope)
		}
		for _, batch := range space.EvidenceBatchIDs {
			if !receiptBatches[batch] {
				return fmt.Errorf("space %s cites batch %s that no commit receipt produced", space.SpaceID, batch)
			}
			switch domain.SpaceScope(space.Scope) {
			case domain.SpacePrivate:
				privateBatches[batch] = true
			case domain.SpaceShared:
				sharedBatches[batch] = true
			}
		}
	}
	if !hasPrivate || !hasShared {
		return errors.New("space scopes must include both a private and a shared space")
	}
	for batch := range sharedBatches {
		if privateBatches[batch] {
			return fmt.Errorf("batch %s appears in both shared and private space scopes", batch)
		}
	}
	return nil
}

// validateBatchManifest enforces the frozen PG-00B manifest envelope shape
// before it is trusted as the known-set authority.
func validateBatchManifest(manifest *frozenBatchManifest) error {
	if manifest.EvaluationBatchID == "" || manifest.ArmID == "" {
		return errors.New("batch manifest: evaluation_batch_id/arm_id are required")
	}
	if manifest.Seed <= 0 {
		return errors.New("batch manifest: seed must be positive")
	}
	if len(manifest.LogicalRuns) == 0 {
		return errors.New("batch manifest: logical_runs are required")
	}
	seen := map[string]bool{}
	for _, run := range manifest.LogicalRuns {
		if run.LogicalRunID == "" || run.TaskID == "" || run.EpisodeID == "" || run.RoomID == "" {
			return fmt.Errorf("batch manifest: logical run %q has empty identity fields", run.LogicalRunID)
		}
		if seen[run.LogicalRunID] {
			return fmt.Errorf("batch manifest: duplicate logical_run_id %s", run.LogicalRunID)
		}
		seen[run.LogicalRunID] = true
		if len(run.ExpectedAgentRuns) == 0 {
			return fmt.Errorf("batch manifest: logical run %s declares no expected agent runs", run.LogicalRunID)
		}
		children := map[string]bool{}
		for _, child := range run.ExpectedAgentRuns {
			if child == "" || children[child] {
				return fmt.Errorf("batch manifest: logical run %s has an empty or duplicated expected agent run", run.LogicalRunID)
			}
			children[child] = true
		}
	}
	return nil
}

// crossCheckKnownSet proves the barrier facts ledger and the frozen manifest
// describe exactly the same known set: duplicate IDs are rejected on both
// sides, the key sets must be equal in both directions, and every canonical
// tuple (task, episode, room) plus the expected agent-run set must match the
// terminal child ledger exactly (R1).
func crossCheckKnownSet(facts *hostBarrierFacts, manifest *frozenBatchManifest) error {
	if facts.BatchID != manifest.EvaluationBatchID || facts.ArmID != manifest.ArmID || facts.Seed != manifest.Seed {
		return errors.New("barrier facts do not match the frozen batch identity (batch/arm/seed)")
	}
	factsByID := map[string]hostLogicalRunFacts{}
	for _, run := range facts.LogicalRuns {
		if _, exists := factsByID[run.LogicalRunID]; exists {
			return fmt.Errorf("barrier facts: duplicate logical_run_id %s", run.LogicalRunID)
		}
		factsByID[run.LogicalRunID] = run
	}
	manifestByID := map[string]struct {
		taskID, episodeID, roomID string
		expected                  []string
	}{}
	for _, run := range manifest.LogicalRuns {
		if _, exists := manifestByID[run.LogicalRunID]; exists {
			return fmt.Errorf("batch manifest: duplicate logical_run_id %s", run.LogicalRunID)
		}
		manifestByID[run.LogicalRunID] = struct {
			taskID, episodeID, roomID string
			expected                  []string
		}{run.TaskID, run.EpisodeID, run.RoomID, run.ExpectedAgentRuns}
	}
	if len(factsByID) != len(manifestByID) {
		return fmt.Errorf("known-set size mismatch: facts carry %d logical runs, manifest freezes %d", len(factsByID), len(manifestByID))
	}
	for id, frozen := range manifestByID {
		run, ok := factsByID[id]
		if !ok {
			return fmt.Errorf("frozen logical run %s is missing from the barrier facts", id)
		}
		if run.TaskID != frozen.taskID || run.EpisodeID != frozen.episodeID || run.RoomID != frozen.roomID {
			return fmt.Errorf("logical run %s: identity tuple (task/episode/room) does not match the frozen manifest", id)
		}
		expected := append([]string(nil), frozen.expected...)
		actual := make([]string, 0, len(run.Children))
		for _, child := range run.Children {
			actual = append(actual, child.AgentRunID)
		}
		sort.Strings(expected)
		sort.Strings(actual)
		if strings.Join(expected, ",") != strings.Join(actual, ",") {
			return fmt.Errorf("logical run %s: registered children %v do not match the frozen expected agent runs %v", id, actual, expected)
		}
	}
	for id := range factsByID {
		if _, ok := manifestByID[id]; !ok {
			return fmt.Errorf("barrier facts carry logical run %s that the frozen manifest does not declare", id)
		}
	}
	return nil
}

// BatchDiagnosis runs the PG-41 GMS half: it consumes the Host
// benchmark-barrier facts and the frozen batch manifest, refuses to diagnose
// unless the barrier actually released over a fully terminal known set,
// cross-checks the barrier facts against the frozen manifest identity, and
// then runs one Room-scoped cut plus capability-bounded diagnosis per
// logical run (failed runs are diagnosed on their preserved evidence, never
// dropped). The whole input boundary fails closed — strict single-document
// parses, closed enums, digest shapes, internal consistency, and an exact
// bijection between the facts ledger and the frozen known set — and every
// validation completes before the first cut is minted (R1).
func BatchDiagnosis(barrierFactsPath, batchManifestPath string) (map[string]any, error) {
	var facts hostBarrierFacts
	if err := strictDecodeFile(barrierFactsPath, &facts); err != nil {
		return nil, fmt.Errorf("parse barrier facts: %w", err)
	}
	var manifest frozenBatchManifest
	if err := strictDecodeFile(batchManifestPath, &manifest); err != nil {
		return nil, fmt.Errorf("parse batch manifest: %w", err)
	}
	if err := validateBatchManifest(&manifest); err != nil {
		return nil, err
	}
	// Barrier gate + closed-contract validation of every fact consumed.
	if err := validateBarrierFacts(&facts); err != nil {
		return nil, err
	}
	// Exact known-set bijection against the frozen manifest identity.
	if err := crossCheckKnownSet(&facts, &manifest); err != nil {
		return nil, err
	}

	// Terminal ledger gates already enforced by validation; recompute the
	// reporting counters from the validated facts.
	allTerminal := true
	failedPreserved := 0
	for _, run := range facts.LogicalRuns {
		if run.TerminalState == "failed" {
			failedPreserved++
		}
	}

	ctx := context.Background()
	tenant := domain.TenantID(facts.TenantID)
	policyRevisions := consolidationcut.PolicyRevisions{
		DiagnosisPolicyRevision: "diagnosis-v1",
		ReplayPolicyRevision:    "replay-v1",
		AggregationPolicyDigest: digestOf("aggregation-policy-v1"),
	}
	perRoom := map[string]func(req consolidationcut.TriggerRequest) consolidationcut.Manifest{}
	for _, run := range facts.LogicalRuns {
		run := run
		perRoom[run.RoomID] = func(req consolidationcut.TriggerRequest) consolidationcut.Manifest {
			manifest := consolidationcut.Manifest{
				TenantID: req.TenantID, RoomID: req.RoomID, Mode: req.Mode,
				TriggerSource: req.TriggerSource, IdempotencyKey: req.IdempotencyKey,
				RoomSequenceWatermark: run.RoomEpoch, PolicyRevisions: policyRevisions,
			}
			for _, segment := range run.SealedSegmentIDs {
				manifest.SealedSegmentIDs = append(manifest.SealedSegmentIDs, consolidationcut.SegmentID(segment))
			}
			for _, receipt := range run.Receipts {
				manifest.EvidenceCommitReceipts = append(manifest.EvidenceCommitReceipts, evidenceReceiptOf(receipt))
			}
			for _, space := range run.Spaces {
				scope := consolidationcut.SpaceScope{
					SpaceID: domain.SpaceID(space.SpaceID), Scope: domain.SpaceScope(space.Scope),
				}
				for _, batch := range space.EvidenceBatchIDs {
					scope.EvidenceBatchIDs = append(scope.EvidenceBatchIDs, domain.BatchID(batch))
				}
				head := domain.ProjectionVersion(space.ProjectionHeadVersion)
				watermark := space.QueryWatermark
				scope.ProjectionHeadVersion = &head
				scope.QueryWatermark = &watermark
				manifest.SpaceScopes = append(manifest.SpaceScopes, scope)
			}
			return manifest
		}
	}
	service := consolidationcut.NewService(nil, &manifestFreezer{perRoom: perRoom})
	diagnosisService := diagnosis.NewService(nil, approvingGate{}, nil)
	authority := newTracerGuardAuthority(diagnosisService)
	authority.manifestOf = service.GetManifest
	service.SetGuardAuthority(authority)

	runs := []map[string]any{}
	for index, run := range facts.LogicalRuns {
		job, _, err := service.Trigger(ctx, consolidationcut.TriggerRequest{
			TenantID: tenant, RoomID: consolidationcut.RoomID(run.RoomID),
			Mode: consolidationcut.ModeForce, TriggerSource: "benchmark",
			PrincipalID:    domain.PrincipalID("diagnosis-agent"),
			IdempotencyKey: fmt.Sprintf("pg41-%s-%s", facts.BatchID, run.LogicalRunID),
		})
		if err != nil {
			return nil, fmt.Errorf("trigger cut for logical run %s: %w", run.LogicalRunID, err)
		}
		if job.Stage != consolidationcut.StageFrozen {
			return nil, fmt.Errorf("logical run %s: cut did not freeze: stage=%s", run.LogicalRunID, job.Stage)
		}
		// Exact Room-bound private scope (Q53=A) with the facts' room epoch.
		privateSpaces := []hostSpaceFacts{}
		for _, space := range run.Spaces {
			if space.Scope == string(domain.SpacePrivate) {
				privateSpaces = append(privateSpaces, space)
			}
		}
		bindings := make([]diagnosis.PrivateSpaceBinding, 0, len(privateSpaces))
		for _, space := range privateSpaces {
			bindings = append(bindings, diagnosis.PrivateSpaceBinding{
				SpaceID: domain.SpaceID(space.SpaceID), RoomID: consolidationcut.RoomID(run.RoomID), RoomEpoch: run.RoomEpoch,
			})
		}
		if err := diagnosisService.RegisterCutScope(tenant, job.CutID, bindings); err != nil {
			return nil, fmt.Errorf("logical run %s: register cut scope: %w", run.LogicalRunID, err)
		}
		issuedAt := time.Now()
		capability, _, err := diagnosisService.IssueCapability(ctx, diagnosis.Capability{
			CapabilityID:       diagnosis.CapabilityID("capability-pg41-" + run.LogicalRunID),
			TenantID:           tenant,
			CutID:              job.CutID,
			PrincipalID:        domain.PrincipalID("diagnosis-agent"),
			BoundPrivateSpaces: bindings,
			Operations:         []diagnosis.CapabilityOperation{diagnosis.CapabilityOperationRead},
			IssuedAt:           issuedAt,
			ExpiresAt:          issuedAt.Add(time.Hour),
		})
		if err != nil {
			return nil, fmt.Errorf("logical run %s: issue capability: %w", run.LogicalRunID, err)
		}
		// Capability-bounded reads over exactly the frozen evidence batches
		// of each private space's own projection.
		readCount := 0
		for i, binding := range bindings {
			for _, batch := range privateSpaces[i].EvidenceBatchIDs {
				if _, err := diagnosisService.ReadPrivate(ctx, diagnosis.PrivateReadRequest{
					Capability: capability, SpaceID: binding.SpaceID,
					BatchID:  domain.BatchID(batch),
					EventIDs: []string{"event-" + run.LogicalRunID},
					ReadAt:   issuedAt,
				}); err != nil {
					return nil, fmt.Errorf("logical run %s: read %s/%s: %w", run.LogicalRunID, binding.SpaceID, batch, err)
				}
				readCount++
			}
		}
		// One verified annotation per run under the frozen policy revision.
		callID := "call-" + run.LogicalRunID
		stored, _, err := diagnosisService.AppendAnnotation(ctx, diagnosis.Annotation{
			AnnotationID:            diagnosis.AnnotationID("annotation-" + run.LogicalRunID),
			Revision:                1,
			CutID:                   job.CutID,
			TenantID:                tenant,
			Target:                  diagnosis.AnnotationTarget{SegmentID: run.SealedSegmentIDs[0], CallID: &callID},
			Status:                  diagnosis.StatusVerified,
			EvidenceCitations:       []diagnosis.EvidenceCitation{{BatchID: domain.BatchID(privateSpaces[0].EvidenceBatchIDs[0]), EventIDs: []string{"event-" + run.LogicalRunID}}},
			DiagnosisPolicyRevision: "diagnosis-v1",
			ExecutionRecipeDigest:   digestOf("recipe-v1"),
		})
		if err != nil {
			return nil, fmt.Errorf("logical run %s: append annotation: %w", run.LogicalRunID, err)
		}
		ref := diagnosis.AnnotationRef{AnnotationID: stored.AnnotationID, Revision: stored.Revision}
		aggregate, err := diagnosisService.Aggregate(ctx, diagnosis.AggregationRequest{
			TenantID: tenant, CutID: job.CutID, AnnotationRefs: []diagnosis.AnnotationRef{ref},
			PolicyDigest: digestOf("aggregation-policy-v1"),
		})
		if err != nil {
			return nil, fmt.Errorf("logical run %s: aggregate: %w", run.LogicalRunID, err)
		}
		// Guarded walk: frozen→diagnosing→consolidating (v1 evidence the
		// authority resolves)→completed with terminal per-space results.
		diagnosing, err := service.Transition(ctx, consolidationcut.TransitionRequest{
			TenantID: tenant, CutID: job.CutID, ExpectedJobVersion: job.JobVersion,
			To: consolidationcut.StageDiagnosing,
		})
		if err != nil {
			return nil, fmt.Errorf("logical run %s: frozen->diagnosing: %w", run.LogicalRunID, err)
		}
		evidence := consolidationcut.TransitionEvidence{
			DiagnosisAnnotationRef:  annotationRefOf(ref.AnnotationID, ref.Revision),
			DiagnosisPolicyRevision: "diagnosis-v1",
		}
		consolidating, err := service.Transition(ctx, consolidationcut.TransitionRequest{
			TenantID: tenant, CutID: diagnosing.CutID, ExpectedJobVersion: diagnosing.JobVersion,
			To: consolidationcut.StageConsolidating, Evidence: evidence,
		})
		if err != nil {
			return nil, fmt.Errorf("logical run %s: diagnosing->consolidating: %w", run.LogicalRunID, err)
		}
		spaceResults := make([]consolidationcut.SpaceResult, 0, len(run.Spaces))
		published := domain.ProjectionVersion(int64(21 + index))
		publishedOne := false
		for _, space := range run.Spaces {
			result := consolidationcut.SpaceResult{SpaceID: domain.SpaceID(space.SpaceID), Result: consolidationcut.SpaceResultNoChange}
			if !publishedOne && space.Scope == string(domain.SpacePrivate) {
				version := published
				result = consolidationcut.SpaceResult{SpaceID: domain.SpaceID(space.SpaceID), Result: consolidationcut.SpaceResultPublished, PublishedVersion: &version}
				publishedOne = true
			}
			spaceResults = append(spaceResults, result)
		}
		completed, err := service.Transition(ctx, consolidationcut.TransitionRequest{
			TenantID: tenant, CutID: consolidating.CutID, ExpectedJobVersion: consolidating.JobVersion,
			To: consolidationcut.StageCompleted, SpaceResults: spaceResults,
		})
		if err != nil {
			return nil, fmt.Errorf("logical run %s: consolidating->completed: %w", run.LogicalRunID, err)
		}
		runs = append(runs, map[string]any{
			"logical_run_id": run.LogicalRunID, "task_id": run.TaskID, "episode_id": run.EpisodeID,
			"room_id": run.RoomID, "attempts": run.Attempts,
			"terminal_state": run.TerminalState, "failure_reason": run.FailureReason,
			"cut_id": string(completed.CutID), "outcome": string(completed.Outcome),
			"private_spaces": len(bindings), "reads": readCount,
			"annotations": 1, "masked": len(aggregate.MaskedAnnotations),
			"confidence": aggregate.Confidence,
			"stages":     []string{string(job.Stage), string(diagnosing.Stage), string(consolidating.Stage), string(completed.Stage)},
		})
	}

	return map[string]any{
		"batch": map[string]any{
			"evaluation_batch_id": facts.BatchID, "arm_id": facts.ArmID, "seed": facts.Seed,
			"logical_runs": len(facts.LogicalRuns), "manifest_match": true,
		},
		"barrier": map[string]any{
			"blocked_while_pending": facts.Barrier.BlockedWhilePending,
			"blocked_while_partial": facts.Barrier.BlockedWhilePartial,
			"released_when_durable": facts.Barrier.ReleasedWhenDurable,
		},
		"admission": map[string]any{
			"next_episode_admitted": facts.Admission.NextEpisodeAdmitted,
			"blocking_outbox_ids":   facts.Admission.BlockingOutboxIDs,
			"reason":                facts.Admission.Reason,
		},
		"restart": map[string]any{
			"recovered_rows":     facts.Restart.RecoveredRows,
			"staged_row_blocked": facts.Restart.StagedRowBlocked,
			"recovered_ids":      facts.Restart.RecoveredIDs,
		},
		"ledger": map[string]any{
			"known_set_size":   len(facts.LogicalRuns),
			"all_terminal":     allTerminal,
			"failed_preserved": failedPreserved,
		},
		"runs": runs,
	}, nil
}

// ReplayActivation runs the PG-42 GMS half: Diagnosis-owned stable proposal,
// gated private→shared disclosure, isolated replay, the immutable replay
// verdict, evaluation-scope activation, and exact base-revision production
// CAS. Every production hop must present an activation ref recorded through
// ActivateEvaluation; free-form evaluation strings, forged refs, fail
// verdicts, and binding mismatches are all probed (P0-4).
func ReplayActivation() (map[string]any, error) {
	ctx := context.Background()
	tenant := domain.TenantID("tenant-pg42")
	diagnosisService := diagnosis.NewService(nil, approvingGate{}, nil)
	promotionService := promotion.NewService()

	cutID := consolidationcut.CutID("cut-pg42")
	owner := domain.PrincipalID("diagnosis-agent")
	proposal := promotion.Proposal{
		ID:         "proposal-pg42",
		Owner:      owner,
		Visibility: promotion.VisibilityPrivate,
		Target: promotion.Target{
			Namespace: promotion.NamespaceEvaluation, TenantID: tenant, Name: "skill-pg42",
		},
	}
	registered, err := promotionService.RegisterProposal(ctx, proposal)
	if err != nil {
		return nil, fmt.Errorf("register proposal: %w", err)
	}
	// Stable ownership (Q90=C): a same-ID registration under a different
	// owner is an identity conflict; an identical replay is idempotent.
	_, errConflict := promotionService.RegisterProposal(ctx, promotion.Proposal{
		ID: proposal.ID, Owner: domain.PrincipalID("attacker-run"), Visibility: proposal.Visibility, Target: proposal.Target,
	})
	identityConflict := errors.Is(errConflict, promotion.ErrProposalIdentityConflict)
	replayRegistered, errReplay := promotionService.RegisterProposal(ctx, proposal)
	ownerStable := errReplay == nil && replayRegistered.Owner == owner && identityConflict

	// Private-derived disclosure through the partitioning gate.
	verified := diagnosis.Annotation{
		AnnotationID: "annotation-pg42", Revision: 1, CutID: cutID, TenantID: tenant,
		Target:                  annotationTargetOf("segment-pg42-001", "call-pg42"),
		Status:                  diagnosis.StatusVerified,
		EvidenceCitations:       []diagnosis.EvidenceCitation{{BatchID: "batch-pg42", EventIDs: []string{"event-1"}}},
		DiagnosisPolicyRevision: "diagnosis-v1",
		ExecutionRecipeDigest:   digestOf("recipe-v1"),
	}
	if _, _, err := diagnosisService.AppendAnnotation(ctx, verified); err != nil {
		return nil, fmt.Errorf("append annotation: %w", err)
	}
	disclosure, err := diagnosisService.Disclose(ctx, diagnosis.DisclosureRequest{
		Annotation:  verified,
		SourceScope: domain.SpacePrivate, TargetScope: domain.SpaceShared,
		Rationale: "private-derived skill proposal rationale",
	})
	if err != nil {
		return nil, fmt.Errorf("disclose: %w", err)
	}
	// Scope narrowing (shared→private) must be denied fail-closed.
	narrowed := verified
	narrowed.AnnotationID = "annotation-pg42-narrow"
	if _, _, err := diagnosisService.AppendAnnotation(ctx, narrowed); err != nil {
		return nil, fmt.Errorf("append narrowed annotation: %w", err)
	}
	_, errNarrow := diagnosisService.Disclose(ctx, diagnosis.DisclosureRequest{
		Annotation:  narrowed,
		SourceScope: domain.SpaceShared, TargetScope: domain.SpacePrivate,
		Rationale: "narrowing attempt",
	})
	disclosureGated := disclosure.Decision.Allowed &&
		disclosure.Decision.ReasonCode == "DISCLOSURE_PARTITIONED" &&
		disclosure.PublicRationale != "" && disclosure.PrivateRationale == "" &&
		errors.Is(errNarrow, diagnosis.ErrDisclosureDenied)

	// Isolated replay in a distinct clone room, sandbox-only, matched
	// baseline, positive budget.
	replayRequest := promotion.ReplayRequest{
		Proposal:           *registered,
		SourceRoomID:       "room-pg42-source",
		CloneRoomID:        "room-pg42-clone",
		AncestorRoomID:     "room-pg42-source",
		MutationBranch:     "replay_skill_mutation",
		SandboxOnlyEffects: true,
		MatchedBaselineRef: "baseline-pg42",
		CandidateRef:       "candidate-pg42@1",
		Budget:             promotion.ReplayBudget{MaxCostUnits: 100},
	}
	session, err := promotionService.PrepareReplay(ctx, replayRequest)
	if err != nil {
		return nil, fmt.Errorf("prepare replay: %w", err)
	}
	sameRoom := replayRequest
	sameRoom.CloneRoomID = replayRequest.SourceRoomID
	_, errSameRoom := promotionService.PrepareReplay(ctx, sameRoom)
	replayIsolated := session.ID != "" && errors.Is(errSameRoom, promotion.ErrReplayNotIsolated)

	// The immutable pass verdict over the replay evidence (P0-4): closed
	// outcome vocabulary, well-shaped policy/evidence digests, idempotent
	// re-record, conflicting second verdict rejected.
	passRequest := promotion.ReplayResultRequest{
		SessionID: session.ID, ProposalID: proposal.ID, Outcome: promotion.ReplayPass,
		EvaluationPolicyDigest: digestOf("evaluation-policy-pg42-v1"),
		PassThreshold:          "pass@0.8",
		EvidenceDigest:         digestOf("replay-evidence-pg42-session1"),
	}
	passResult, err := promotionService.RecordReplayResult(ctx, passRequest)
	if err != nil {
		return nil, fmt.Errorf("record pass verdict: %w", err)
	}
	passReplay, err := promotionService.RecordReplayResult(ctx, passRequest)
	passIdempotent := err == nil && passReplay.ID == passResult.ID
	conflict := passRequest
	conflict.Outcome = promotion.ReplayFail
	_, errConflictVerdict := promotionService.RecordReplayResult(ctx, conflict)
	conflictRejected := errors.Is(errConflictVerdict, promotion.ErrReplayResultConflict)
	// Closed vocabulary: an out-of-enum outcome string is rejected.
	offVocabulary := passRequest
	offVocabulary.Outcome = promotion.ReplayOutcome("passed-ish")
	_, errVocabulary := promotionService.RecordReplayResult(ctx, offVocabulary)
	vocabularyClosed := errVocabulary != nil && !errors.Is(errVocabulary, promotion.ErrReplayResultConflict)
	// (R2) the session binding is immutable: a verdict recorded against a
	// different registered proposal cannot ride this session.
	otherProposal := promotion.Proposal{
		ID: "proposal-pg42-other", Owner: owner, Visibility: promotion.VisibilityPrivate,
		Target: promotion.Target{Namespace: promotion.NamespaceEvaluation, TenantID: tenant, Name: "skill-pg42-other"},
	}
	if _, err := promotionService.RegisterProposal(ctx, otherProposal); err != nil {
		return nil, fmt.Errorf("register other proposal: %w", err)
	}
	_, errRebind := promotionService.RecordReplayResult(ctx, promotion.ReplayResultRequest{
		SessionID: session.ID, ProposalID: otherProposal.ID, Outcome: promotion.ReplayPass,
		EvaluationPolicyDigest: digestOf("evaluation-policy-pg42-v1"),
		PassThreshold:          "pass@0.8",
		EvidenceDigest:         digestOf("replay-evidence-pg42-rebind"),
	})
	sessionRebindingRejected := errors.Is(errRebind, promotion.ErrReplaySessionMismatch)

	evaluationTarget := promotion.Target{Namespace: promotion.NamespaceEvaluation, TenantID: tenant, Name: "skill-pg42"}
	productionTarget := promotion.Target{Namespace: promotion.NamespaceProduction, TenantID: tenant, Name: "skill-pg42"}

	// A failing replay can never activate evaluation: second isolated
	// session, fail verdict, activation refused.
	failRequest := replayRequest
	failRequest.CloneRoomID = "room-pg42-clone-fail"
	failRequest.MatchedBaselineRef = "baseline-pg42-fail"
	failSession, err := promotionService.PrepareReplay(ctx, failRequest)
	if err != nil {
		return nil, fmt.Errorf("prepare fail replay: %w", err)
	}
	failResult, err := promotionService.RecordReplayResult(ctx, promotion.ReplayResultRequest{
		SessionID: failSession.ID, ProposalID: proposal.ID, Outcome: promotion.ReplayFail,
		EvaluationPolicyDigest: digestOf("evaluation-policy-pg42-v1"),
		PassThreshold:          "pass@0.8",
		EvidenceDigest:         digestOf("replay-evidence-pg42-session-fail"),
	})
	if err != nil {
		return nil, fmt.Errorf("record fail verdict: %w", err)
	}
	_, errFailActivation := promotionService.ActivateEvaluation(ctx, promotion.EvaluationActivationRequest{
		ReplayResultRef: failResult.ID, ProposalID: proposal.ID, EvaluationTarget: evaluationTarget,
		CandidateRef: "candidate-pg42@1", ExactBaseRevision: "skill-pg42@1",
	})
	failActivationRejected := errors.Is(errFailActivation, promotion.ErrReplayNotPassed)

	// Activations live in the evaluation namespace only.
	_, errProductionActivation := promotionService.ActivateEvaluation(ctx, promotion.EvaluationActivationRequest{
		ReplayResultRef: passResult.ID, ProposalID: proposal.ID, EvaluationTarget: productionTarget,
		CandidateRef: "candidate-pg42@1", ExactBaseRevision: "skill-pg42@1",
	})
	productionActivationRejected := errProductionActivation != nil

	// (R2) activation derives its bindings from the immutable session
	// snapshot: a candidate the session never evaluated, or a target other
	// than the proposal's registered target, is rejected.
	_, errWrongCandidateActivation := promotionService.ActivateEvaluation(ctx, promotion.EvaluationActivationRequest{
		ReplayResultRef: passResult.ID, ProposalID: proposal.ID, EvaluationTarget: evaluationTarget,
		CandidateRef: "candidate-pg42@9", ExactBaseRevision: "skill-pg42@1",
	})
	candidateBindingRejected := errors.Is(errWrongCandidateActivation, promotion.ErrEvaluationBindingMismatch)
	_, errWrongTargetActivation := promotionService.ActivateEvaluation(ctx, promotion.EvaluationActivationRequest{
		ReplayResultRef: passResult.ID, ProposalID: proposal.ID,
		EvaluationTarget: promotion.Target{Namespace: promotion.NamespaceEvaluation, TenantID: tenant, Name: "other-skill"},
		CandidateRef:     "candidate-pg42@1", ExactBaseRevision: "skill-pg42@1",
	})
	targetBindingRejected := errors.Is(errWrongTargetActivation, promotion.ErrEvaluationBindingMismatch)

	// The pass verdict activates evaluation with the exact bindings a later
	// production promotion must match.
	activation, err := promotionService.ActivateEvaluation(ctx, promotion.EvaluationActivationRequest{
		ReplayResultRef: passResult.ID, ProposalID: proposal.ID, EvaluationTarget: evaluationTarget,
		CandidateRef: "candidate-pg42@1", ExactBaseRevision: "skill-pg42@1",
	})
	if err != nil {
		return nil, fmt.Errorf("activate evaluation: %w", err)
	}

	// Production promotion probes: free-form evaluation strings and forged
	// activation refs are unknown; candidate/base mismatches violate the
	// activation binding.
	_, errFreeForm := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: "evaluation-pass-pg42", CandidateRef: "candidate-pg42@1",
		ProductionTarget: productionTarget, ExactBaseRevision: "skill-pg42@1", PromotedRevision: "skill-pg42@2",
	})
	freeFormRejected := errors.Is(errFreeForm, promotion.ErrEvaluationActivationUnknown)
	_, errForged := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: activation.Ref + "-forged", CandidateRef: "candidate-pg42@1",
		ProductionTarget: productionTarget, ExactBaseRevision: "skill-pg42@1", PromotedRevision: "skill-pg42@2",
	})
	forgedRejected := errors.Is(errForged, promotion.ErrEvaluationActivationUnknown)
	_, errCandidate := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: activation.Ref, CandidateRef: "candidate-pg42@9",
		ProductionTarget: productionTarget, ExactBaseRevision: "skill-pg42@1", PromotedRevision: "skill-pg42@2",
	})
	candidateRejected := errors.Is(errCandidate, promotion.ErrEvaluationBindingMismatch)
	_, errBase := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: activation.Ref, CandidateRef: "candidate-pg42@1",
		ProductionTarget: productionTarget, ExactBaseRevision: "skill-pg42@0", PromotedRevision: "skill-pg42@2",
	})
	baseRejected := errors.Is(errBase, promotion.ErrEvaluationBindingMismatch)
	wrongTarget := promotion.Target{Namespace: promotion.NamespaceProduction, TenantID: tenant, Name: "other-skill"}
	_, errTarget := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: activation.Ref, CandidateRef: "candidate-pg42@1",
		ProductionTarget: wrongTarget, ExactBaseRevision: "skill-pg42@1", PromotedRevision: "skill-pg42@2",
	})
	targetRejected := errors.Is(errTarget, promotion.ErrEvaluationBindingMismatch)

	// The bound production hop: activation ref + candidate + exact base.
	record, err := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: activation.Ref, CandidateRef: "candidate-pg42@1",
		ProductionTarget: productionTarget, ExactBaseRevision: "skill-pg42@1", PromotedRevision: "skill-pg42@2",
	})
	if err != nil {
		return nil, fmt.Errorf("promote: %w", err)
	}
	// Stale base CAS: replaying the consumed activation against base @1 is
	// rejected because the active revision moved to @2.
	_, errStale := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: activation.Ref, CandidateRef: "candidate-pg42@1",
		ProductionTarget: productionTarget, ExactBaseRevision: "skill-pg42@1", PromotedRevision: "skill-pg42@3",
	})
	staleRejected := errors.Is(errStale, promotion.ErrStaleBaseRevision)
	// (R2) the base revision at activation time is derived from the target's
	// active state: a fresh pass session cannot activate against base @1
	// once the production target has moved to @2.
	staleBaseRequest := replayRequest
	staleBaseRequest.CloneRoomID = "room-pg42-clone-stale"
	staleBaseRequest.MatchedBaselineRef = "baseline-pg42-stale"
	staleBaseSession, err := promotionService.PrepareReplay(ctx, staleBaseRequest)
	if err != nil {
		return nil, fmt.Errorf("prepare stale-base replay: %w", err)
	}
	staleBaseResult, err := promotionService.RecordReplayResult(ctx, promotion.ReplayResultRequest{
		SessionID: staleBaseSession.ID, ProposalID: proposal.ID, Outcome: promotion.ReplayPass,
		EvaluationPolicyDigest: digestOf("evaluation-policy-pg42-v1"),
		PassThreshold:          "pass@0.8",
		EvidenceDigest:         digestOf("replay-evidence-pg42-session-stale"),
	})
	if err != nil {
		return nil, fmt.Errorf("record stale-base verdict: %w", err)
	}
	_, errStaleBaseActivation := promotionService.ActivateEvaluation(ctx, promotion.EvaluationActivationRequest{
		ReplayResultRef: staleBaseResult.ID, ProposalID: proposal.ID, EvaluationTarget: evaluationTarget,
		CandidateRef: "candidate-pg42@1", ExactBaseRevision: "skill-pg42@1",
	})
	staleBaseActivationRejected := errors.Is(errStaleBaseActivation, promotion.ErrStaleBaseRevision)
	// A refresh is a whole new evidence chain: second isolated session, new
	// candidate, new pass verdict, new activation against base @2.
	refreshRequest := replayRequest
	refreshRequest.CloneRoomID = "room-pg42-clone-refresh"
	refreshRequest.MatchedBaselineRef = "baseline-pg42-refresh"
	refreshRequest.CandidateRef = "candidate-pg42@2"
	refreshSession, err := promotionService.PrepareReplay(ctx, refreshRequest)
	if err != nil {
		return nil, fmt.Errorf("prepare refresh replay: %w", err)
	}
	refreshResult, err := promotionService.RecordReplayResult(ctx, promotion.ReplayResultRequest{
		SessionID: refreshSession.ID, ProposalID: proposal.ID, Outcome: promotion.ReplayPass,
		EvaluationPolicyDigest: digestOf("evaluation-policy-pg42-v1"),
		PassThreshold:          "pass@0.8",
		EvidenceDigest:         digestOf("replay-evidence-pg42-session-refresh"),
	})
	if err != nil {
		return nil, fmt.Errorf("record refresh verdict: %w", err)
	}
	refreshActivation, err := promotionService.ActivateEvaluation(ctx, promotion.EvaluationActivationRequest{
		ReplayResultRef: refreshResult.ID, ProposalID: proposal.ID, EvaluationTarget: evaluationTarget,
		CandidateRef: "candidate-pg42@2", ExactBaseRevision: "skill-pg42@2",
	})
	if err != nil {
		return nil, fmt.Errorf("activate refresh: %w", err)
	}
	record2, err := promotionService.Promote(ctx, promotion.PromotionRequest{
		ProposalID: proposal.ID, EvaluationRef: refreshActivation.Ref, CandidateRef: "candidate-pg42@2",
		ProductionTarget: productionTarget, ExactBaseRevision: "skill-pg42@2", PromotedRevision: "skill-pg42@3",
	})
	if err != nil {
		return nil, fmt.Errorf("refresh promote: %w", err)
	}

	return map[string]any{
		"proposal": map[string]any{
			"id": registered.ID, "owner": string(registered.Owner),
			"visibility": string(registered.Visibility), "namespace": string(registered.Target.Namespace),
			"owner_stable": ownerStable,
		},
		"disclosure": map[string]any{
			"allowed": disclosure.Decision.Allowed, "reason_code": disclosure.Decision.ReasonCode,
			"public_rationale": disclosure.PublicRationale, "citations": len(disclosure.Citations),
			"audit_digest": disclosure.AuditDigest, "gated": disclosureGated,
		},
		"replay": map[string]any{
			"session_id": session.ID, "clone_room": replayRequest.CloneRoomID,
			"isolated": replayIsolated, "sandbox_only": replayRequest.SandboxOnlyEffects,
			"baseline":  replayRequest.MatchedBaselineRef,
			"result_id": passResult.ID, "outcome": string(passResult.Outcome),
			"pass_threshold":             passResult.PassThreshold,
			"evidence_digest":            passResult.EvidenceDigest,
			"policy_digest":              passResult.EvaluationPolicyDigest,
			"idempotent":                 passIdempotent,
			"conflict_rejected":          conflictRejected,
			"vocabulary_closed":          vocabularyClosed,
			"session_rebinding_rejected": sessionRebindingRejected,
			"fail_activation_rejected":   failActivationRejected,
		},
		"activation": map[string]any{
			"evaluation_only":                productionActivationRejected,
			"activation_ref":                 activation.Ref,
			"candidate_ref":                  activation.CandidateRef,
			"base_revision":                  activation.ExactBaseRevision,
			"free_form_ref_rejected":         freeFormRejected,
			"forged_ref_rejected":            forgedRejected,
			"candidate_mismatch":             candidateRejected,
			"base_mismatch":                  baseRejected,
			"target_mismatch":                targetRejected,
			"candidate_binding_rejected":     candidateBindingRejected,
			"target_binding_rejected":        targetBindingRejected,
			"stale_base_activation_rejected": staleBaseActivationRejected,
			"record_id":                      record.ID,
			"record_base_revision":           record.ExactBaseRevision,
			"promoted":                       record.PromotedRevision,
			"rollback":                       record.RollbackRevision,
			"stale_rejected":                 staleRejected,
			"refresh_session":                refreshSession.ID,
			"refresh_activation_ref":         refreshActivation.Ref,
			"refresh_promoted":               record2.PromotedRevision,
			"refresh_rollback":               record2.RollbackRevision,
		},
	}, nil
}

// Revocation runs the PG-43 GMS half: cryptographic payload erasure with
// closed non-sensitive reason codes, authorization-epoch cache invalidation,
// cooperative training stop, and complete root-inclusive lineage tombstone
// cascades that cannot regrow from any erased node.
func Revocation() (map[string]any, error) {
	ctx := context.Background()
	tenant := domain.TenantID("tenant-pg43")
	security := artifactsecurity.NewService()
	revocations := revocation.NewService()
	lineage := modellineage.NewService()

	// Payload erase: seal → open → erase → open fails closed; the tombstone
	// retains only the closed reason code and non-sensitive facts, and the
	// tenant identity survives.
	aad := artifactsecurity.AAD{TenantID: tenant, ArtifactID: "artifact-export-1", Purpose: "trajectory_export"}
	payload := []byte("reconstructed training payload pg43")
	envelope, err := security.Seal(ctx, aad, payload)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	opened, err := security.Open(ctx, envelope)
	if err != nil {
		return nil, fmt.Errorf("open before erase: %w", err)
	}
	identityBefore, err := security.TenantIdentity(ctx, tenant, []byte("principal-pg43"))
	if err != nil {
		return nil, fmt.Errorf("identity before erase: %w", err)
	}
	// Free-text, PII-bearing, or overlong reasons are rejected before any
	// erasure happens (P1-10): the tombstone vocabulary is closed.
	_, errPIIReason := security.Erase(ctx, tenant, aad.ArtifactID,
		artifactsecurity.EraseReason("user zhou@example.com asked to delete payload "+strings.Repeat("A", 64)))
	piiReasonRejected := errors.Is(errPIIReason, artifactsecurity.ErrEraseReasonInvalid)
	_, errFreeText := security.Erase(ctx, tenant, aad.ArtifactID, artifactsecurity.EraseReason("revoked"))
	freeTextRejected := errors.Is(errFreeText, artifactsecurity.ErrEraseReasonInvalid)
	tombstone, err := security.Erase(ctx, tenant, aad.ArtifactID, artifactsecurity.EraseUserDeletion)
	if err != nil {
		return nil, fmt.Errorf("erase: %w", err)
	}
	_, errErased := security.Open(ctx, envelope)
	payloadUnreadable := errors.Is(errErased, artifactsecurity.ErrArtifactErased)
	identityAfter, err := security.TenantIdentity(ctx, tenant, []byte("principal-pg43"))
	if err != nil {
		return nil, fmt.Errorf("identity after erase: %w", err)
	}
	tombstoneClean := tombstone.Reason == artifactsecurity.EraseUserDeletion &&
		tombstone.TenantID == tenant && tombstone.ArtifactID == aad.ArtifactID && tombstone.KeyID != ""

	// Epoch invalidation: a cached authorization computed under the pre-
	// revocation epoch is stale; the exact current epoch passes; equal or
	// older revocations are regressions.
	subject := revocation.Subject{TenantID: tenant, PrincipalID: domain.PrincipalID("agent-pg43")}
	if err := revocations.Authorize(ctx, subject, revocation.Epoch(0)); err != nil {
		return nil, fmt.Errorf("authorize epoch 0: %w", err)
	}
	revoked, err := revocations.Revoke(ctx, revocation.Entry{Subject: subject, Epoch: revocation.Epoch(1), Reason: "PAYLOAD_ERASED"})
	if err != nil {
		return nil, fmt.Errorf("revoke epoch 1: %w", err)
	}
	errCached := revocations.Authorize(ctx, subject, revocation.Epoch(0))
	cacheInvalidated := errors.Is(errCached, revocation.ErrStaleAuthorizationEpoch)
	if err := revocations.Authorize(ctx, subject, revocation.Epoch(1)); err != nil {
		return nil, fmt.Errorf("authorize epoch 1: %w", err)
	}
	_, errRegression := revocations.Revoke(ctx, revocation.Entry{Subject: subject, Epoch: revocation.Epoch(1), Reason: "REPLAY"})
	epochMonotonic := errors.Is(errRegression, revocation.ErrEpochRegression)

	// Cooperative training stop: the in-flight poller continues at the exact
	// current epoch, then voluntarily stops once a revocation advances it.
	polls := []string{}
	if err := revocations.MayContinueTraining(ctx, "training-pg43", subject, revocation.Epoch(1)); err == nil {
		polls = append(polls, "continue@1")
	}
	if _, err := revocations.Revoke(ctx, revocation.Entry{Subject: subject, Epoch: revocation.Epoch(2), Reason: "TRAINING_WITHDRAWAL"}); err != nil {
		return nil, fmt.Errorf("revoke epoch 2: %w", err)
	}
	errStop := revocations.MayContinueTraining(ctx, "training-pg43", subject, revocation.Epoch(1))
	if errors.Is(errStop, revocation.ErrStaleAuthorizationEpoch) {
		polls = append(polls, "stop@1(stale)")
	}
	trainingStopped := len(polls) == 2 && polls[1] == "stop@1(stale)"

	// Lineage tombstone cascade over a diamond: the erased root itself and
	// every transitive descendant are tombstoned exactly once with their
	// cascade source, and erased lineage cannot regrow from the root OR
	// from any descendant.
	ref := func(id string) modellineage.CheckpointRef {
		return modellineage.CheckpointRef{ID: modellineage.CheckpointID(id), TenantID: tenant}
	}
	nodes := []modellineage.Node{
		{Ref: ref("ckpt-root")},
		{Ref: ref("ckpt-a"), Parents: []modellineage.CheckpointRef{ref("ckpt-root")}},
		{Ref: ref("ckpt-b"), Parents: []modellineage.CheckpointRef{ref("ckpt-root")}},
		{Ref: ref("ckpt-c"), Parents: []modellineage.CheckpointRef{ref("ckpt-a"), ref("ckpt-b")}},
	}
	for _, node := range nodes {
		if err := lineage.Put(ctx, node); err != nil {
			return nil, fmt.Errorf("lineage put %s: %w", node.Ref.ID, err)
		}
	}
	descendants, err := lineage.AffectedDescendants(ctx, ref("ckpt-root"))
	if err != nil {
		return nil, fmt.Errorf("affected descendants: %w", err)
	}
	tombstones, err := lineage.TombstoneCascade(ctx, ref("ckpt-root"), artifactsecurity.EraseTrainingWithdrawal)
	if err != nil {
		return nil, fmt.Errorf("tombstone cascade: %w", err)
	}
	tombstoned := map[string]bool{}
	sourceCorrect := true
	for _, entry := range tombstones {
		tombstoned[string(entry.Ref.ID)] = true
		if entry.SourceRef.ID != "ckpt-root" {
			sourceCorrect = false
		}
	}
	rootRecorded := tombstones[0].Ref.ID == "ckpt-root" && tombstones[0].SourceRef.ID == "ckpt-root"
	affectedComplete := len(descendants) == 3 &&
		tombstoned["ckpt-root"] && tombstoned["ckpt-a"] && tombstoned["ckpt-b"] && tombstoned["ckpt-c"] &&
		sourceCorrect && rootRecorded
	errRegrow := lineage.Put(ctx, modellineage.Node{
		Ref: ref("ckpt-d"), Parents: []modellineage.CheckpointRef{ref("ckpt-c")},
	})
	regrowBlocked := errors.Is(errRegrow, modellineage.ErrTombstonedAncestor)
	// Root-parent bypass probe: a live child directly under the erased root
	// must also be rejected, not just children of tombstoned descendants.
	errRootRegrow := lineage.Put(ctx, modellineage.Node{
		Ref: ref("ckpt-root-child"), Parents: []modellineage.CheckpointRef{ref("ckpt-root")},
	})
	rootRegrowBlocked := errors.Is(errRootRegrow, modellineage.ErrTombstonedAncestor)
	// R6: the cascade reason vocabulary is closed and shared with artifact
	// erase — free text, PII, and unknown codes are rejected before any
	// tombstone is written.
	_, errCascadeFreeText := lineage.TombstoneCascade(ctx, ref("ckpt-a"), artifactsecurity.EraseReason("revoked"))
	cascadeFreeTextRejected := errors.Is(errCascadeFreeText, modellineage.ErrCascadeReasonInvalid)
	_, errCascadePII := lineage.TombstoneCascade(ctx, ref("ckpt-a"), artifactsecurity.EraseReason("user zhou@example.com asked to delete payload "+strings.Repeat("A", 64)))
	cascadePIIRejected := errors.Is(errCascadePII, modellineage.ErrCascadeReasonInvalid)
	cascadeReasonClosed := cascadeFreeTextRejected && cascadePIIRejected && len(lineage.Tombstones()) == len(tombstones)

	return map[string]any{
		"erase": map[string]any{
			"opened_before":       string(opened) == string(payload),
			"payload_unreadable":  payloadUnreadable,
			"tombstone_key_id":    string(tombstone.KeyID),
			"tombstone_reason":    string(tombstone.Reason),
			"tombstone_clean":     tombstoneClean,
			"pii_reason_rejected": piiReasonRejected,
			"free_text_rejected":  freeTextRejected,
			"identity_stable":     identityBefore == identityAfter,
		},
		"epoch": map[string]any{
			"revoked_cursor":    string(revoked.Cursor),
			"cache_invalidated": cacheInvalidated,
			"monotonic":         epochMonotonic,
		},
		"training": map[string]any{
			"polls":   polls,
			"stopped": trainingStopped,
		},
		"lineage": map[string]any{
			"descendants":           len(descendants),
			"tombstones":            len(tombstones),
			"root_recorded":         rootRecorded,
			"affected_complete":     affectedComplete,
			"regrow_blocked":        regrowBlocked,
			"root_regrow_blocked":   rootRegrowBlocked,
			"cascade_reason_closed": cascadeReasonClosed,
			"tombstone_reason":      string(tombstones[0].Reason),
		},
	}, nil
}
