package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

// ---------------------------------------------------------------------------
// Flat cut-error writer
//
// The cut routes deliberately do NOT use the wrapped wireError envelope
// ({"error":{code,message,request_id,details}}) that the rest of the
// transport uses. The frozen consolidation-cuts OpenAPI declares a flat
// Error object {"code","message"[,"details"]} with additionalProperties
// false, and only reason codes from the frozen reasons snapshot are valid.
// request_id is intentionally absent from that schema.
// ---------------------------------------------------------------------------

type flatCutError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func writeCutError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, flatCutError{Code: code, Message: message})
}

// ---------------------------------------------------------------------------
// CutJob DTO
//
// The frozen CutJob schema is additionalProperties:false with an exact
// required set. The internal Job carries fields that must never be exposed
// (DiagnosisPolicyRevision, FailedFrom, LastTransitionEvidence,
// NewDiagnosisRunRef), so the DTO is built field-by-field from the service
// Job rather than serialized wholesale.
// ---------------------------------------------------------------------------

func cutJobDTO(job consolidationcut.Job) map[string]any {
	dto := map[string]any{
		"cut_id":      string(job.CutID),
		"tenant_id":   string(job.TenantID),
		"room_id":     string(job.RoomID),
		"stage":       string(job.Stage),
		"job_version": job.JobVersion,
	}
	if job.CutDigest != "" {
		dto["cut_digest"] = job.CutDigest
	}
	if job.Outcome != "" && job.Outcome != consolidationcut.SpaceResultPending {
		dto["outcome"] = string(job.Outcome)
	}
	if job.RoomSequenceWatermark != nil {
		dto["room_sequence_watermark"] = *job.RoomSequenceWatermark
	}
	dto["space_results"] = cutSpaceResultsDTO(job.SpaceResults)
	if len(job.FailureReasons) > 0 {
		dto["failure_reasons"] = job.FailureReasons
	}
	dto["updated_at"] = job.UpdatedAt.UTC().Format(time.RFC3339Nano)
	return dto
}

func cutSpaceResultsDTO(results []consolidationcut.SpaceResult) []any {
	items := make([]any, 0, len(results))
	for _, result := range results {
		item := map[string]any{
			"space_id": string(result.SpaceID),
			"result":   string(result.Result),
		}
		if result.Reason != "" {
			item["reason"] = result.Reason
		}
		items = append(items, item)
	}
	return items
}

// ---------------------------------------------------------------------------
// Committed evidence source for the production freezer / durable authority
//
// The production freezer and the durable guard authority need two facts: the
// Registry (space identity/version/grants) and the committed evidence ledger.
// (0, math.MaxInt64) enumerates a space's complete committed set.
// ---------------------------------------------------------------------------

type committedBatchesSource interface {
	CommittedEvidenceBatches(context.Context, domain.TenantID, domain.SpaceID, int64, int64) ([]domain.EvidenceBatch, error)
}

// RoomFreezer is the production consolidation-cut Freezer: it turns the
// room→space deployment binding plus the committed evidence ledger into the
// server-side atomic freeze manifest (SC-4.4). It holds NO authorizer — the
// purpose-bound authorization runs in the handler, so the freezer stays a
// pure facts resolver. The LastSealed ledger is durable state restored on
// boot so threshold diffs survive a restart (PG-50A start/stop
// restorability).
type RoomFreezer struct {
	registry   ports.RegistryStore
	committed  committedBatchesSource
	roomSpaces map[string][]domain.SpaceID

	compositionMu sync.Mutex
	mu            sync.Mutex
	lastSealed    map[consolidationcut.RoomID]map[consolidationcut.SegmentID]bool
}

func NewRoomFreezer(registry ports.RegistryStore, committed committedBatchesSource, roomSpaces map[string][]domain.SpaceID) *RoomFreezer {
	normalized := make(map[string][]domain.SpaceID, len(roomSpaces))
	for room, spaces := range roomSpaces {
		normalized[room] = append([]domain.SpaceID(nil), spaces...)
	}
	return &RoomFreezer{
		registry:   registry,
		committed:  committed,
		roomSpaces: normalized,
		lastSealed: map[consolidationcut.RoomID]map[consolidationcut.SegmentID]bool{},
	}
}

// LockComposition serializes cut service mutations that update lastSealed with
// paired service/freezer snapshots. It is intentionally separate from the
// freezer fact mutex: callers may hold it while Trigger invokes Freeze, and
// persistence uses it to capture one linearizable composition image.
func (f *RoomFreezer) LockComposition() {
	f.compositionMu.Lock()
}

// UnlockComposition releases the composition-level cut mutation/snapshot lock.
func (f *RoomFreezer) UnlockComposition() {
	f.compositionMu.Unlock()
}

// ResolveSpaces returns the legally-bound spaces for a room. The handler uses
// it for purpose-bound authorization BEFORE freeze (the freezer performs no
// authorization itself); an empty result means the room has no legal
// bindings and freezes fail closed. Room identity is a tenant-agnostic
// deployment binding: the same room id maps to the same space set for every
// tenant.
func (f *RoomFreezer) ResolveSpaces(roomID string) []domain.SpaceID {
	return append([]domain.SpaceID(nil), f.roomSpaces[roomID]...)
}

func (f *RoomFreezer) hasBinding(roomID string) bool {
	spaces, ok := f.roomSpaces[roomID]
	return ok && len(spaces) > 0
}

type spaceSnapshot struct {
	spaceID domain.SpaceID
	scope   domain.SpaceScope
	head    *domain.ProjectionVersion
	batches []domain.EvidenceBatch
	query   int64 // this space's own committed-batch count (per-space query watermark)
}

// Freeze implements consolidationcut.Freezer. It computes the sealed segment
// set from the bound spaces' committed batches, pins per-space scope facts,
// and returns the manifest the service stamps and makes immutable.
func (f *RoomFreezer) Freeze(ctx context.Context, req consolidationcut.TriggerRequest) (consolidationcut.Manifest, error) {
	room := string(req.RoomID)
	spaceIDs := f.ResolveSpaces(room)
	if len(spaceIDs) == 0 {
		return consolidationcut.Manifest{}, fmt.Errorf("%w: room %q has no legally bound spaces", consolidationcut.ErrManifestContractViolation, room)
	}

	snapshot := make([]spaceSnapshot, 0, len(spaceIDs))
	maxSequence := int64(0)
	for _, spaceID := range spaceIDs {
		space, err := f.registry.Space(ctx, req.TenantID, spaceID)
		if err != nil {
			return consolidationcut.Manifest{}, fmt.Errorf("freeze %q space %s: %w", room, spaceID, err)
		}
		batches, err := f.committed.CommittedEvidenceBatches(ctx, req.TenantID, spaceID, 0, math.MaxInt64)
		if err != nil {
			return consolidationcut.Manifest{}, fmt.Errorf("freeze %q space %s: enumerate committed evidence: %w", room, spaceID, err)
		}
		head := domain.ProjectionVersion(space.Version)
		snapshot = append(snapshot, spaceSnapshot{spaceID: spaceID, scope: space.Scope, head: &head, batches: batches, query: int64(len(batches))})
		for _, batch := range batches {
			for _, event := range batch.Events {
				if event.Sequence > maxSequence {
					maxSequence = event.Sequence
				}
			}
		}
	}

	// Sealed segments = distinct SourceSegmentIDs across all bound spaces,
	// sorted for determinism.
	allSegments := map[consolidationcut.SegmentID]bool{}
	for _, entry := range snapshot {
		for _, batch := range entry.batches {
			if batch.SourceSegmentID != "" {
				allSegments[consolidationcut.SegmentID(batch.SourceSegmentID)] = true
			}
		}
	}
	orderEd := make([]consolidationcut.SegmentID, 0, len(allSegments))
	for segment := range allSegments {
		orderEd = append(orderEd, segment)
	}
	sort.Slice(orderEd, func(i, j int) bool { return orderEd[i] < orderEd[j] })
	sealed := orderEd

	if req.Mode == consolidationcut.ModeThreshold {
		f.mu.Lock()
		last := f.lastSealed[req.RoomID]
		f.mu.Unlock()
		delta := make([]consolidationcut.SegmentID, 0, len(sealed))
		for _, segment := range sealed {
			if last == nil || !last[segment] {
				delta = append(delta, segment)
			}
		}
		sealed = delta
	}

	return f.buildManifest(req, sealed, snapshot, maxSequence)
}

func (f *RoomFreezer) buildManifest(req consolidationcut.TriggerRequest, sealed []consolidationcut.SegmentID, snapshot []spaceSnapshot, roomSequenceWatermark int64) (consolidationcut.Manifest, error) {
	manifest := consolidationcut.Manifest{
		TenantID:              req.TenantID,
		RoomID:                req.RoomID,
		Mode:                  req.Mode,
		TriggerSource:         req.TriggerSource,
		IdempotencyKey:        req.IdempotencyKey,
		RoomSequenceWatermark: roomSequenceWatermark,
		SealedSegmentIDs:      append([]consolidationcut.SegmentID(nil), sealed...),
		PolicyRevisions: consolidationcut.PolicyRevisions{
			DiagnosisPolicyRevision: "diagnosis-v1",
			ReplayPolicyRevision:    "replay-v1",
			AggregationPolicyDigest: "sha256:" + stableDigest("aggregation-policy-v1"),
		},
	}
	for _, entry := range snapshot {
		batchIDs := make([]domain.BatchID, 0, len(entry.batches))
		for _, batch := range entry.batches {
			batchIDs = append(batchIDs, batch.ID)
		}
		scope := consolidationcut.SpaceScope{
			SpaceID:               entry.spaceID,
			Scope:                 entry.scope,
			EvidenceBatchIDs:      batchIDs,
			ProjectionHeadVersion: entry.head,
			QueryWatermark:        &entry.query,
		}
		manifest.SpaceScopes = append(manifest.SpaceScopes, scope)
	}
	// One receipt per sealed segment: the committed batches under that segment
	// across all bound spaces, with a deterministic aggregate batch digest.
	segmentToBatches := map[consolidationcut.SegmentID][]domain.EvidenceBatch{}
	for _, entry := range snapshot {
		for _, batch := range entry.batches {
			if batch.SourceSegmentID == "" {
				continue
			}
			segment := consolidationcut.SegmentID(batch.SourceSegmentID)
			segmentToBatches[segment] = append(segmentToBatches[segment], batch)
		}
	}
	for _, segment := range sealed {
		batches := segmentToBatches[segment]
		batchIDs := make([]domain.BatchID, 0, len(batches))
		digests := make([]string, 0, len(batches))
		for _, batch := range batches {
			batchIDs = append(batchIDs, batch.ID)
			if batch.Provenance.ContentSHA256 == "" {
				return consolidationcut.Manifest{}, fmt.Errorf("%w: segment %s batch %s has no content digest", consolidationcut.ErrManifestContractViolation, segment, batch.ID)
			}
			digests = append(digests, "sha256:"+batch.Provenance.ContentSHA256)
		}
		sort.Strings(digests)
		manifest.EvidenceCommitReceipts = append(manifest.EvidenceCommitReceipts, consolidationcut.EvidenceCommitReceipt{
			ReceiptID:   consolidationcut.ReceiptID("receipt-" + string(segment)),
			SegmentID:   segment,
			BatchIDs:    batchIDs,
			BatchDigest: aggregateDigest(digests),
		})
	}
	return manifest, nil
}

func aggregateDigest(componentDigests []string) string {
	if len(componentDigests) == 0 {
		return "sha256:" + stableDigest("empty-receipt")
	}
	return "sha256:" + stableDigest(strings.Join(componentDigests, "\x00"))
}

// equalBatchIDSets reports whether two batch-ID lists name the same set
// (order-insensitive, no duplicates on either side).
func equalBatchIDSets(a, b []domain.BatchID) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[domain.BatchID]bool, len(a))
	for _, id := range a {
		seen[id] = true
	}
	for _, id := range b {
		if !seen[id] {
			return false
		}
	}
	return true
}

func stableDigest(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// Snapshot returns a durable image of the Freezer's threshold last-sealed
// ledger so a restarted composition can resume threshold diffs (PG-50A).
func (f *RoomFreezer) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	image := map[string][]string{}
	for room, segments := range f.lastSealed {
		ordered := make([]string, 0, len(segments))
		for segment := range segments {
			ordered = append(ordered, string(segment))
		}
		sort.Strings(ordered)
		image[string(room)] = ordered
	}
	return json.Marshal(image)
}

// ValidateRestore checks a freezer threshold-ledger image without mutating
// the receiver. Server composition validates it before swapping the paired
// cut-service image, preventing a failed second restore from splitting the
// recovered composition.
func (f *RoomFreezer) ValidateRestore(image []byte) error {
	_, err := decodeRoomFreezerState(image)
	return err
}

// Restore installs the Freezer's last-sealed ledger from a snapshot image.
// An invalid image fails closed leaving the receiver untouched.
func (f *RoomFreezer) Restore(image []byte) error {
	state, err := decodeRoomFreezerState(image)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSealed = state
	return nil
}

func decodeRoomFreezerState(image []byte) (map[consolidationcut.RoomID]map[consolidationcut.SegmentID]bool, error) {
	var data map[string][]string
	if err := json.Unmarshal(image, &data); err != nil {
		return nil, fmt.Errorf("freezer state snapshot is not valid JSON: %w", err)
	}
	if data == nil {
		return nil, fmt.Errorf("freezer state snapshot must be an object")
	}
	state := make(map[consolidationcut.RoomID]map[consolidationcut.SegmentID]bool, len(data))
	for room, segments := range data {
		if room == "" {
			return nil, fmt.Errorf("freezer state snapshot has an empty room id")
		}
		set := map[consolidationcut.SegmentID]bool{}
		for _, segment := range segments {
			if segment == "" || set[consolidationcut.SegmentID(segment)] {
				return nil, fmt.Errorf("freezer state snapshot has an empty or duplicate segment for room %q", room)
			}
			set[consolidationcut.SegmentID(segment)] = true
		}
		state[consolidationcut.RoomID(room)] = set
	}
	return state, nil
}

// recordSealed updates the last-sealed ledger after a successful trigger. It
// is called with the fully-stamped manifest after the service records the
// job, so the threshold diff for the NEXT trigger excludes what this freeze
// just sealed.
func (f *RoomFreezer) recordSealed(roomID consolidationcut.RoomID, sealed []consolidationcut.SegmentID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastSealed[roomID] == nil {
		f.lastSealed[roomID] = map[consolidationcut.SegmentID]bool{}
	}
	for _, segment := range sealed {
		f.lastSealed[roomID][segment] = true
	}
}

// ---------------------------------------------------------------------------
// Signed event notifier
//
// Signed webhooks carry ONLY cut_id/stage/job_version (plus the event_id/ts/
// signature envelope). They are notifications, never the source of truth:
// GET remains authoritative, and a missed event never affects GET truth.
// The stream is an append-only JSONL file. Each event is HMAC-SHA256 signed
// over "cut_id|stage|job_version|ts" with the deployment secret. event_id is
// cut_id@stage#version so at-least-once redelivery of the same logical event
// is idempotent for downstream dedup.
// ---------------------------------------------------------------------------

type cutEventLine struct {
	EventID    string `json:"event_id"`
	CutID      string `json:"cut_id"`
	Stage      string `json:"stage"`
	JobVersion int64  `json:"job_version"`
	TS         string `json:"ts"`
	Signature  string `json:"signature"`
}

// CutEventSink is the append-only destination for signed cut events.
type CutEventSink interface {
	AppendLine(line []byte) error
}

type fileCutEventSink struct {
	mu   sync.Mutex
	path string
}

func NewFileCutEventSink(path string) *fileCutEventSink {
	return &fileCutEventSink{path: path}
}

func (f *fileCutEventSink) AppendLine(line []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out strings.Builder
	out.Write(line)
	if !strings.HasSuffix(string(line), "\n") {
		out.WriteString("\n")
	}
	return appendFile(f.path, []byte(out.String()))
}

// appendFile appends bytes to a file, creating it if needed.
func appendFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return err
	}
	return file.Sync()
}

type nopCutEventSink struct{}

func (nopCutEventSink) AppendLine([]byte) error { return nil }

// CutEventNotifier signs and appends cut stage-transition notifications. A
// nil secret disables it (nil notifier), and sink failures are logged but
// never fail the request — webhooks are notifications, GET is authoritative.
type CutEventNotifier struct {
	secret []byte
	sink   CutEventSink
	logger *JSONLogger
	mu     sync.Mutex
}

// NewCutEventNotifier returns a notifier without an operational logger. A
// nil sink yields a disabled (no-op) notifier; a nil secret likewise disables
// signing/emission. Server composition should prefer
// NewCutEventNotifierWithLogger so sink failures remain observable while
// notifications stay non-authoritative.
func NewCutEventNotifier(secret string, sink CutEventSink) *CutEventNotifier {
	return NewCutEventNotifierWithLogger(secret, sink, nil)
}

// NewCutEventNotifierWithLogger returns a notifier that reports append
// failures through the normal structured operational log. Event delivery is
// intentionally at-least-once and never changes the authoritative GET state.
func NewCutEventNotifierWithLogger(secret string, sink CutEventSink, logger *JSONLogger) *CutEventNotifier {
	if secret == "" || sink == nil {
		return &CutEventNotifier{sink: nopCutEventSink{}, logger: logger}
	}
	return &CutEventNotifier{secret: []byte(secret), sink: sink, logger: logger}
}

func (n *CutEventNotifier) Notify(cutID consolidationcut.CutID, stage consolidationcut.Stage, jobVersion int64, at time.Time) {
	if n == nil || len(n.secret) == 0 {
		return
	}
	eventID := string(cutID) + "@" + string(stage) + "#" + fmt.Sprintf("%d", jobVersion)
	ts := at.UTC().Format(time.RFC3339Nano)
	sig := signCutEvent(n.secret, string(cutID), string(stage), jobVersion, ts)
	line, err := json.Marshal(cutEventLine{
		EventID: eventID, CutID: string(cutID), Stage: string(stage),
		JobVersion: jobVersion, TS: ts, Signature: sig,
	})
	if err != nil {
		return
	}
	if err := n.sink.AppendLine(line); err != nil {
		if n.logger != nil {
			n.logger.Log("cut_event_sink_error", map[string]any{
				"cut_id":      string(cutID),
				"stage":       string(stage),
				"job_version": jobVersion,
				"event_id":    eventID,
				"error":       err.Error(),
			})
		}
	}
}

func signCutEvent(secret []byte, cutID, stage string, jobVersion int64, ts string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(cutID))
	mac.Write([]byte("|"))
	mac.Write([]byte(stage))
	mac.Write([]byte("|"))
	mac.Write([]byte(fmt.Sprintf("%d", jobVersion)))
	mac.Write([]byte("|"))
	mac.Write([]byte(ts))
	return hex.EncodeToString(mac.Sum(nil))
}

// ---------------------------------------------------------------------------
// Durable GuardAuthority adapter (PG-50A line-232 ①)
//
// This adapter resolves the guarded edges the composition can already back
// with trusted server-side facts (committed evidence + the registry). Edges
// whose facts are owned by not-yet-wired workers (quota, projection,
// proposal/replay, promotion CAS, side-effects) fail closed with
// ErrGuardNotSatisfied — those remain on the PG-50B/C ledger. ReserveFacts
// serializes all fact mutation behind a per-cut counter exactly as the R3
// contract requires, so an apply-time re-resolve cannot observe a revoked
// or superseded fact.
// ---------------------------------------------------------------------------

type durableGuardAuthority struct {
	committed     committedBatchesSource
	manifests     func(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID) (consolidationcut.Manifest, error)
	roomBindings  map[string][]domain.SpaceID
	reservationMu sync.Mutex
	reservations  map[string]int
	freezer       *RoomFreezer
}

// manifestProviderType is the constructor argument shape for a manifest
// resolver. The adapter is only ever invoked WITHOUT the cut service's lock
// held (the service documents this for adapters), so calling back into
// service.GetManifest is safe and free of self-deadlock.
type manifestProvider func(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID) (consolidationcut.Manifest, error)

func NewDurableGuardAuthority(committed committedBatchesSource, manifests manifestProvider, roomBindings map[string][]domain.SpaceID) *durableGuardAuthority {
	if manifests == nil {
		manifests = func(context.Context, domain.TenantID, consolidationcut.CutID) (consolidationcut.Manifest, error) {
			return consolidationcut.Manifest{}, consolidationcut.ErrCutNotFound
		}
	}
	return &durableGuardAuthority{
		committed:    committed,
		manifests:    manifests,
		roomBindings: roomBindings,
		reservations: map[string]int{},
	}
}

func (a *durableGuardAuthority) factReservationKey(tenantID domain.TenantID, cutID consolidationcut.CutID) string {
	return string(tenantID) + "\x00" + string(cutID)
}

func (a *durableGuardAuthority) ReserveFacts(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID) error {
	a.reservationMu.Lock()
	defer a.reservationMu.Unlock()
	a.reservations[a.factReservationKey(tenantID, cutID)]++
	return nil
}

func (a *durableGuardAuthority) ReleaseFacts(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID) {
	a.reservationMu.Lock()
	defer a.reservationMu.Unlock()
	key := a.factReservationKey(tenantID, cutID)
	if n := a.reservations[key]; n <= 1 {
		delete(a.reservations, key)
		return
	}
	a.reservations[key]--
}

// receiptsCompleteForCut re-derives the sealed segment receipts for the cut's
// frozen manifest from the committed evidence ledger and compares the
// aggregation against the presented digest. The committed ledger — not the
// manifest's own claims — is the deciding authority, and the comparison is
// EXACT in both directions: per bound space, the manifest's declared batch
// set must equal the space's full committed set (staged, superseded,
// cross-space, duplicated, or OMITTED committed batches all fail closed — a
// freezer cannot lie by omission either), every declared batch must land in a
// sealed segment, and the receipts re-aggregated from the ledger's content
// digests must match the presented digest exactly (SC-4.4). The scope set
// itself must equal the room's deployment binding. Availability faults
// (committed-ledger or manifest-provider read failures) surface as transient
// errors — the service classifies them ErrGuardAuthorityUnavailable (HTTP
// 500); only deterministic fact rebuttals are ErrGuardNotSatisfied (HTTP 422
// RECEIPT_MISSING).
func (a *durableGuardAuthority) receiptsCompleteForCut(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, presented string) (bool, consolidationcut.GuardConfirmation, error) {
	manifest, err := a.manifests(ctx, tenantID, cutID)
	if err != nil {
		if errors.Is(err, consolidationcut.ErrCutNotFound) {
			// Deterministic absence: the job's manifest is recorded BEFORE the
			// frozen guard consults it, so a missing manifest is a fact
			// rebuttal, not an availability fault.
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: manifest unavailable: %v", consolidationcut.ErrGuardNotSatisfied, err)
		}
		// Any other provider failure is transient: surface it unwrapped so the
		// service classifies it ErrGuardAuthorityUnavailable instead of
		// permanently falsifying the failure reason as RECEIPT_MISSING.
		return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("receipts_complete: manifest provider: %w", err)
	}
	// NOTE: there is deliberately NO zero-sealed early success here. An empty
	// sealed set must still prove scope==binding and declared==committed for
	// every bound space — otherwise a freezer holding a room with committed
	// evidence could present an empty manifest and slip into frozen. A
	// zero-sealed manifest is only admissible when every bound space's
	// committed set is genuinely empty (no-change); the shared validation
	// below admits exactly that case.
	// The frozen scope set must equal the room's deployment binding exactly.
	bound := map[domain.SpaceID]bool{}
	for _, spaceID := range a.roomBindings[string(manifest.RoomID)] {
		bound[spaceID] = true
	}
	if len(bound) == 0 {
		return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: room %s has no deployment binding", consolidationcut.ErrGuardNotSatisfied, manifest.RoomID)
	}
	scoped := map[domain.SpaceID]bool{}
	for _, scope := range manifest.SpaceScopes {
		if scoped[scope.SpaceID] {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: space %s declared by multiple scopes", consolidationcut.ErrGuardNotSatisfied, scope.SpaceID)
		}
		scoped[scope.SpaceID] = true
		if !bound[scope.SpaceID] {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: space %s is not bound to room %s", consolidationcut.ErrGuardNotSatisfied, scope.SpaceID, manifest.RoomID)
		}
	}
	for spaceID := range bound {
		if !scoped[spaceID] {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: bound space %s of room %s is missing from the manifest scopes", consolidationcut.ErrGuardNotSatisfied, spaceID, manifest.RoomID)
		}
	}
	segmentToBatches := map[consolidationcut.SegmentID][]domain.EvidenceBatch{}
	// Global declared index: a batch may be declared exactly once across ALL
	// scopes — per-scope dedup alone would let a duplicate scope double-count
	// it into the receipts aggregation.
	declaredGlobal := map[domain.BatchID]domain.SpaceID{}
	for _, scope := range manifest.SpaceScopes {
		committed, err := a.committed.CommittedEvidenceBatches(ctx, tenantID, scope.SpaceID, 0, math.MaxInt64)
		if err != nil {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("receipts_complete: committed ledger for space %s: %w", scope.SpaceID, err)
		}
		committedByID := make(map[domain.BatchID]domain.EvidenceBatch, len(committed))
		for _, batch := range committed {
			committedByID[batch.ID] = batch
		}
		for _, batchID := range scope.EvidenceBatchIDs {
			if previous, dup := declaredGlobal[batchID]; dup {
				return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: batch %s declared by multiple scopes (%s and %s)", consolidationcut.ErrGuardNotSatisfied, batchID, previous, scope.SpaceID)
			}
			declaredGlobal[batchID] = scope.SpaceID
			batch, ok := committedByID[batchID]
			if !ok {
				return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: batch %s is not a committed batch of space %s", consolidationcut.ErrGuardNotSatisfied, batchID, scope.SpaceID)
			}
			if batch.SourceSegmentID != "" {
				segment := consolidationcut.SegmentID(batch.SourceSegmentID)
				segmentToBatches[segment] = append(segmentToBatches[segment], batch)
			}
		}
		// Exact-set direction two: the manifest must cover the space's FULL
		// committed set. A freezer that omits a committed batch (and its
		// segment) would otherwise re-derive self-consistently — the
		// independent authority exists precisely to catch that lie.
		for _, batch := range committed {
			if _, ok := declaredGlobal[batch.ID]; !ok {
				return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: committed batch %s of space %s is omitted by the manifest", consolidationcut.ErrGuardNotSatisfied, batch.ID, scope.SpaceID)
			}
		}
	}
	sealed := make(map[consolidationcut.SegmentID]bool, len(manifest.SealedSegmentIDs))
	for _, segment := range manifest.SealedSegmentIDs {
		if sealed[segment] {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: segment %s sealed twice", consolidationcut.ErrGuardNotSatisfied, segment)
		}
		sealed[segment] = true
	}
	// Every declared committed batch must land inside a sealed segment: a
	// batch parked in an unsealed segment would silently escape the receipts
	// aggregation below.
	for segment := range segmentToBatches {
		if !sealed[segment] {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: declared batch targets unsealed segment %s", consolidationcut.ErrGuardNotSatisfied, segment)
		}
	}
	receipts := make([]consolidationcut.EvidenceCommitReceipt, 0, len(manifest.SealedSegmentIDs))
	for _, segment := range manifest.SealedSegmentIDs {
		batches := segmentToBatches[segment]
		if len(batches) == 0 {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: segment %s has no committed batches", consolidationcut.ErrGuardNotSatisfied, segment)
		}
		digests := make([]string, 0, len(batches))
		batchIDs := make([]domain.BatchID, 0, len(batches))
		for _, batch := range batches {
			if batch.Provenance.ContentSHA256 == "" {
				return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: segment %s batch %s has no content digest", consolidationcut.ErrGuardNotSatisfied, segment, batch.ID)
			}
			digests = append(digests, "sha256:"+batch.Provenance.ContentSHA256)
			batchIDs = append(batchIDs, batch.ID)
		}
		sort.Strings(digests)
		receipts = append(receipts, consolidationcut.EvidenceCommitReceipt{
			ReceiptID:   consolidationcut.ReceiptID("receipt-" + string(segment)),
			SegmentID:   segment,
			BatchIDs:    batchIDs,
			BatchDigest: aggregateDigest(digests),
		})
	}
	// Per-segment provenance binding. The canonical receipts digest covers
	// only each receipt's BatchDigest, so an aggregate match alone cannot tie
	// a receipt's BatchIDs to its segment: a freezer could swap two receipts'
	// batch lists or substitute phantom batch IDs while keeping the digests.
	// The manifest receipt for EVERY sealed segment must therefore equal the
	// authority's re-derivation exactly — same batch set and same aggregate
	// digest — before the presented aggregate is even compared.
	manifestReceipts := make(map[consolidationcut.SegmentID]consolidationcut.EvidenceCommitReceipt, len(manifest.EvidenceCommitReceipts))
	for _, receipt := range manifest.EvidenceCommitReceipts {
		if _, dup := manifestReceipts[receipt.SegmentID]; dup {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: duplicate manifest receipt for segment %s", consolidationcut.ErrGuardNotSatisfied, receipt.SegmentID)
		}
		manifestReceipts[receipt.SegmentID] = receipt
	}
	if len(manifestReceipts) != len(receipts) {
		return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: manifest carries %d receipts for %d sealed segments", consolidationcut.ErrGuardNotSatisfied, len(manifestReceipts), len(receipts))
	}
	for _, derived := range receipts {
		presented, ok := manifestReceipts[derived.SegmentID]
		if !ok {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: manifest carries no receipt for sealed segment %s", consolidationcut.ErrGuardNotSatisfied, derived.SegmentID)
		}
		if presented.BatchDigest != derived.BatchDigest {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: receipt digest for segment %s does not match committed ledger", consolidationcut.ErrGuardNotSatisfied, derived.SegmentID)
		}
		if !equalBatchIDSets(presented.BatchIDs, derived.BatchIDs) {
			return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: receipt batch set for segment %s does not match committed ledger", consolidationcut.ErrGuardNotSatisfied, derived.SegmentID)
		}
	}
	facts := make(map[string]string, len(receipts)+1)
	recomputed := consolidationcut.ReceiptsDigest(receipts)
	facts["receipts_digest"] = recomputed
	facts["sealed"] = fmt.Sprintf("%d", len(receipts))
	if recomputed != presented {
		return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: receipts_complete: digest mismatch (presented %s, recomputed %s)", consolidationcut.ErrGuardNotSatisfied, presented, recomputed)
	}
	return true, consolidationcut.ConfirmFacts("receipts_complete_for_all_sealed_segments", facts), nil
}

func (a *durableGuardAuthority) QuotaGranted(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, grantRef string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: quota_budget_granted (worker-owned fact, not wired in PG-50A)", consolidationcut.ErrGuardNotSatisfied)
}
func (a *durableGuardAuthority) ReceiptsComplete(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, receiptsDigest string) (bool, consolidationcut.GuardConfirmation, error) {
	return a.receiptsCompleteForCut(ctx, tenantID, cutID, receiptsDigest)
}
func (a *durableGuardAuthority) DiagnosisVerified(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, annotationRef, policyRevision string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: diagnosis_verified (diagnosis service not wired in PG-50A)", consolidationcut.ErrGuardNotSatisfied)
}
func (a *durableGuardAuthority) ProjectionComplete(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, recordDigest string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: projection_complete (worker-owned fact, not wired in PG-50A)", consolidationcut.ErrGuardNotSatisfied)
}
func (a *durableGuardAuthority) ProposalRefs(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID) ([]string, consolidationcut.GuardConfirmation, error) {
	return nil, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: proposal_refs (proposal authority not wired in PG-50A)", consolidationcut.ErrGuardNotSatisfied)
}
func (a *durableGuardAuthority) ReplayPassed(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, replayResultRef string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: replay_pass (replay authority not wired in PG-50A)", consolidationcut.ErrGuardNotSatisfied)
}
func (a *durableGuardAuthority) CASBaseRevisionMatches(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID, baseRevision string) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: cas_base_revision (promotion authority not wired in PG-50A)", consolidationcut.ErrGuardNotSatisfied)
}
func (a *durableGuardAuthority) SideEffectsCompleted(ctx context.Context, tenantID domain.TenantID, cutID consolidationcut.CutID) (bool, consolidationcut.GuardConfirmation, error) {
	return false, consolidationcut.GuardConfirmation{}, fmt.Errorf("%w: side_effects_completed (side-effect authority not wired in PG-50A)", consolidationcut.ErrGuardNotSatisfied)
}

// ---------------------------------------------------------------------------
// Cut HTTP handlers
// ---------------------------------------------------------------------------

// purpose-bound authorization: the room's evidence-writer principal must hold
// exactly the bound spaces for the lifecycle purpose and evidence.commit
// operation. Returns true when the request should be aborted (an error was
// already written). The freezer performs no authorization itself — this runs
// in the handler, matching the existing handler authorization style.
func (h *Handler) authorizeCutFreeze(w http.ResponseWriter, r *http.Request, identity authz.Identity, freezer *RoomFreezer, roomID string) bool {
	bound := freezer.ResolveSpaces(roomID)
	if len(bound) == 0 {
		writeCutError(w, http.StatusUnprocessableEntity, "MISSING_MANIFEST_FIELD", "room has no legally bound spaces")
		return true
	}
	if _, err := h.authorizer.AuthorizeExact(r.Context(), identity, bound, domain.GrantPurposeLifecycle, domain.GrantOperationEvidenceCommit); err != nil {
		var protocol *domain.ProtocolError
		if errors.As(err, &protocol) {
			writeCutError(w, http.StatusForbidden, protocol.Code(), protocol.Message)
			return true
		}
		writeCutError(w, http.StatusForbidden, "SPACE_FORBIDDEN", "freeze authorization failed")
		return true
	}
	return false
}

// createConsolidationCut implements POST /v1/rooms/{room_id}/consolidation-cuts.
// The Idempotency-Key header is REQUIRED; the body is exactly
// {mode, trigger_source} with additionalProperties false. Purpose-bound
// authorization (evidence.commit on the bound spaces) precedes the freeze.
func (h *Handler) createConsolidationCut(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, roomID string) {
	service := h.deps.ConsolidationCuts
	freezer := h.deps.CutFreezer
	if service == nil || freezer == nil {
		writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation-cut composition is not configured")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeCutError(w, http.StatusUnprocessableEntity, "MISSING_MANIFEST_FIELD", "Idempotency-Key header is required")
		return
	}
	if err := object.rejectUnknownFields(map[string]bool{"mode": true, "trigger_source": true}); err != nil {
		writeCutError(w, http.StatusUnprocessableEntity, "UNKNOWN_CORE_FIELD", "unknown body field in consolidation-cut create")
		return
	}
	var mode, triggerSource string
	var errs []fieldDetail
	object.requireString("mode", &mode, &errs)
	object.requireString("trigger_source", &triggerSource, &errs)
	if len(errs) > 0 {
		writeCutError(w, http.StatusUnprocessableEntity, "MISSING_MANIFEST_FIELD", "mode and trigger_source are required")
		return
	}
	if mode != string(consolidationcut.ModeThreshold) && mode != string(consolidationcut.ModeForce) {
		writeCutError(w, http.StatusUnprocessableEntity, "PAYLOAD_VALIDATION_FAILED", "mode must be threshold or force")
		return
	}
	if byteLen(triggerSource) < 1 {
		writeCutError(w, http.StatusUnprocessableEntity, "PAYLOAD_VALIDATION_FAILED", "trigger_source must be non-empty")
		return
	}
	if h.authorizeCutFreeze(w, r, identity, freezer, roomID) {
		return
	}
	// Keep Trigger's authoritative job mutation, lastSealed update, and a
	// concurrent composition snapshot in one linearizable cut-composition
	// window. The freezer's fact mutex stays separate, so Trigger can safely
	// invoke Freeze while this lock is held.
	freezer.LockComposition()
	defer freezer.UnlockComposition()

	job, replay, err := service.Trigger(r.Context(), consolidationcut.TriggerRequest{
		TenantID:       identity.TenantID,
		PrincipalID:    identity.PrincipalID,
		RoomID:         consolidationcut.RoomID(roomID),
		Mode:           consolidationcut.Mode(mode),
		TriggerSource:  triggerSource,
		IdempotencyKey: key,
	})
	if err != nil {
		if errors.Is(err, consolidationcut.ErrIdempotencyConflict) {
			writeCutError(w, http.StatusConflict, "CUT_IDEMPOTENCY_CONFLICT", "same Idempotency-Key with a changed frozen body")
			return
		}
		if errors.Is(err, consolidationcut.ErrInvalidMode) {
			writeCutError(w, http.StatusUnprocessableEntity, "PAYLOAD_VALIDATION_FAILED", "mode must be threshold or force")
			return
		}
		if errors.Is(err, consolidationcut.ErrManifestContractViolation) {
			writeCutError(w, http.StatusUnprocessableEntity, "MISSING_MANIFEST_FIELD", "freeze could not legally bind the room evidence")
			return
		}
		if errors.Is(err, consolidationcut.ErrReceiptCoverageIncomplete) {
			writeCutError(w, http.StatusUnprocessableEntity, "RECEIPT_MISSING", "freeze receipts do not cover the sealed segments")
			return
		}
		if errors.Is(err, consolidationcut.ErrGuardNotSatisfied) {
			// The durable authority rejected the initial freezing→frozen
			// receipts proof (the service has already persisted the failed
			// job with RECEIPT_MISSING): a deterministic receipt rejection is
			// a contract 422, not an infrastructure failure.
			writeCutError(w, http.StatusUnprocessableEntity, "RECEIPT_MISSING", "freeze receipts were not verified by the durable guard authority")
			return
		}
		writeCutError(w, http.StatusInternalServerError, "INFRASTRUCTURE_FAILURE", "freeze failed")
		return
	}
	// A non-replay trigger whose returned CutID is NOT the cut this key mints
	// is the room's already in-flight cut (SC-4.1 Room-serial freeze): the
	// caller's view of the room is stale and GET is authoritative — surface a
	// retryable 409 ROOM_EPOCH_STALE rather than a misleading 202.
	if !replay && job.CutID != consolidationcut.MintCutID(identity.TenantID, consolidationcut.RoomID(roomID), triggerSource, key) {
		writeCutError(w, http.StatusConflict, "ROOM_EPOCH_STALE", "a freeze is already in flight for this room; GET is authoritative, retry after it settles")
		return
	}
	// The freeze succeeded and sealed a manifest. Record the sealed set in
	// the freezer's threshold ledger; then, for the instance-scoped notifier,
	// emit the frozen-stage notification. Replays carry the same facts.
	if manifest, mErr := service.GetManifest(r.Context(), identity.TenantID, job.CutID); mErr == nil {
		freezer.recordSealed(consolidationcut.RoomID(roomID), manifest.SealedSegmentIDs)
	}
	ctxNotifier := cutNotifierFrom(h)
	ctxNotifier.Notify(job.CutID, job.Stage, job.JobVersion, job.UpdatedAt)
	if !replay {
		writeJSON(w, http.StatusAccepted, cutJobDTO(job))
		return
	}
	// Replay also returns 202 with the original job (frozen contract: 202 is
	// the only create success the OpenAPI exposes).
	writeJSON(w, http.StatusAccepted, cutJobDTO(job))
}

// getConsolidationCut implements GET /v1/consolidation-cuts/{cut_id} — the
// authoritative status source (SC-8.1). Webhooks only notify.
func (h *Handler) getConsolidationCut(w http.ResponseWriter, r *http.Request, identity authz.Identity, cutID string) {
	service := h.deps.ConsolidationCuts
	if service == nil {
		writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation-cut composition is not configured")
		return
	}
	job, err := service.Get(r.Context(), identity.TenantID, consolidationcut.CutID(cutID))
	if err != nil {
		if errors.Is(err, consolidationcut.ErrCutNotFound) {
			writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation cut not found")
			return
		}
		writeCutError(w, http.StatusInternalServerError, "INFRASTRUCTURE_FAILURE", "read cut failed")
		return
	}
	writeJSON(w, http.StatusOK, cutJobDTO(job))
}

// cancelConsolidationCut implements POST /v1/consolidation-cuts/{cut_id}:cancel
// with an EMPTY body. Cooperative cancellation uses a mandatory JobVersion CAS:
// the handler reads the current version, then retries on a conflict (bounded).
// An already-terminal or uncancelable stage is a cooperative no-op returning
// 202 with the current job.
func (h *Handler) cancelConsolidationCut(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, cutID string) {
	service := h.deps.ConsolidationCuts
	if service == nil {
		writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation-cut composition is not configured")
		return
	}
	// cancel is an empty-body POST per the frozen OpenAPI: no requestBody.
	// The ServeHTTP body decode produces an empty object for an absent body;
	// any non-empty JSON body is a contract violation.
	if len(object) > 0 {
		writeCutError(w, http.StatusUnprocessableEntity, "PAYLOAD_VALIDATION_FAILED", "cancel accepts no request body")
		return
	}
	for attempt := 0; attempt < 3; attempt++ {
		current, err := service.Get(r.Context(), identity.TenantID, consolidationcut.CutID(cutID))
		if err != nil {
			if errors.Is(err, consolidationcut.ErrCutNotFound) {
				writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation cut not found")
				return
			}
			writeCutError(w, http.StatusInternalServerError, "INFRASTRUCTURE_FAILURE", "read cut failed")
			return
		}
		job, err := service.Cancel(r.Context(), consolidationcut.CancelRequest{
			TenantID:           identity.TenantID,
			PrincipalID:        identity.PrincipalID,
			CutID:              consolidationcut.CutID(cutID),
			ExpectedJobVersion: current.JobVersion,
		})
		if err != nil {
			if errors.Is(err, consolidationcut.ErrCutNotFound) {
				writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation cut not found")
				return
			}
			if errors.Is(err, consolidationcut.ErrJobVersionConflict) {
				continue // bounded CAS retry
			}
			if errors.Is(err, consolidationcut.ErrNotCancelable) {
				// Cooperative no-op: the target terminal/cancelling state is
				// already reached, or the stage has no cancel edge. Return 202
				// with the current job.
				writeJSON(w, http.StatusAccepted, cutJobDTO(current))
				return
			}
			writeCutError(w, http.StatusInternalServerError, "INFRASTRUCTURE_FAILURE", "cancel failed")
			return
		}
		h.cutNotifier().Notify(job.CutID, job.Stage, job.JobVersion, job.UpdatedAt)
		writeJSON(w, http.StatusAccepted, cutJobDTO(job))
		return
	}
	writeCutError(w, http.StatusConflict, "ROOM_EPOCH_STALE", "cancel could not acquire a stable job version; retry")
}

// rediagnoseConsolidationCut implements POST /v1/consolidation-cuts/{cut_id}:rediagnose.
// Body carries only diagnosis_policy_revision; the composition mints the
// NewDiagnosisRunRef server-side (deterministic per cut+revision). Only a
// partially_failed job is legal; the service enforces that domain invariant
// as a 422.
func (h *Handler) rediagnoseConsolidationCut(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, cutID string) {
	service := h.deps.ConsolidationCuts
	if service == nil {
		writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation-cut composition is not configured")
		return
	}
	if err := object.rejectUnknownFields(map[string]bool{"diagnosis_policy_revision": true}); err != nil {
		writeCutError(w, http.StatusUnprocessableEntity, "UNKNOWN_CORE_FIELD", "unknown body field in rediagnose")
		return
	}
	var revision string
	var errs []fieldDetail
	object.requireString("diagnosis_policy_revision", &revision, &errs)
	if len(errs) > 0 || revision == "" {
		writeCutError(w, http.StatusUnprocessableEntity, "MISSING_MANIFEST_FIELD", "diagnosis_policy_revision is required")
		return
	}
	runRef := diagnosisRunRef(cutID, revision)
	for attempt := 0; attempt < 3; attempt++ {
		current, err := service.Get(r.Context(), identity.TenantID, consolidationcut.CutID(cutID))
		if err != nil {
			if errors.Is(err, consolidationcut.ErrCutNotFound) {
				writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation cut not found")
				return
			}
			writeCutError(w, http.StatusInternalServerError, "INFRASTRUCTURE_FAILURE", "read cut failed")
			return
		}
		job, err := service.Rediagnose(r.Context(), consolidationcut.RediagnoseRequest{
			TenantID:                identity.TenantID,
			PrincipalID:             identity.PrincipalID,
			CutID:                   consolidationcut.CutID(cutID),
			ExpectedJobVersion:      current.JobVersion,
			DiagnosisPolicyRevision: revision,
			NewDiagnosisRunRef:      runRef,
		})
		if err != nil {
			if errors.Is(err, consolidationcut.ErrCutNotFound) {
				writeCutError(w, http.StatusNotFound, "NOT_FOUND", "consolidation cut not found")
				return
			}
			if errors.Is(err, consolidationcut.ErrJobVersionConflict) {
				continue // bounded CAS retry
			}
			if errors.Is(err, consolidationcut.ErrIllegalTransition) {
				writeCutError(w, http.StatusUnprocessableEntity, "PAYLOAD_VALIDATION_FAILED", "rediagnose is only legal from a partially_failed cut")
				return
			}
			writeCutError(w, http.StatusUnprocessableEntity, "PAYLOAD_VALIDATION_FAILED", "rediagnose failed")
			return
		}
		h.cutNotifier().Notify(job.CutID, job.Stage, job.JobVersion, job.UpdatedAt)
		writeJSON(w, http.StatusAccepted, cutJobDTO(job))
		return
	}
	writeCutError(w, http.StatusConflict, "ROOM_EPOCH_STALE", "rediagnose could not acquire a stable job version; retry")
}

func diagnosisRunRef(cutID, revision string) string {
	return "diagnosis-run-" + stableDigest(cutID + "|" + revision)[:16]
}

func (h *Handler) cutNotifier() *CutEventNotifier {
	if h.deps.CutEvents == nil {
		return NewCutEventNotifier("", nil)
	}
	return h.deps.CutEvents
}

// cutNotifierFrom is retained for call sites that already hold the Handler.
func cutNotifierFrom(h *Handler) *CutEventNotifier { return h.cutNotifier() }
