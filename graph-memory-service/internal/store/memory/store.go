package memory

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

var (
	errConflict = domain.NewProtocolError(409, "IDEMPOTENCY_CONFLICT", "idempotency key was already used with different content")
	errNotFound = domain.NewProtocolError(404, "BATCH_NOT_FOUND", "evidence batch not found")
	errSession  = domain.NewProtocolError(404, "EXPLORATION_NOT_FOUND", "exploration session not found")
)

// Store is the in-memory adapter for every GMS port. Mutations serialize under
// one mutex and each mutation is a single critical section, emulating the
// transaction atomicity the production Postgres adapter must provide.
type Store struct {
	mu sync.Mutex

	tenants    map[domain.TenantID]domain.Tenant
	principals map[domain.TenantID]map[domain.PrincipalID]domain.Principal
	spaces     map[domain.TenantID]map[domain.SpaceID]domain.Space
	grants     map[domain.TenantID]map[domain.GrantID]domain.Grant

	batches        map[domain.TenantID]map[domain.BatchID]domain.EvidenceBatch
	stageKeys      map[domain.TenantID]map[stageKey]domain.BatchID
	publishedOrder map[evidenceSpaceKey][]domain.BatchID
	batchVersion   map[domain.BatchID]int64

	sessions       map[domain.ExplorationSessionID]domain.ExplorationSession
	sessionByKey   map[sessionKey]domain.ExplorationSessionID
	operations     map[domain.ExplorationSessionID]map[string]storedOperation
	servedItems    map[domain.ExplorationSessionID]map[string]domain.RecallItem
	navigationRuns map[domain.ExplorationSessionID]map[string]ports.NavigationRunJournal

	// Curation durable state (M1–M4). Map shapes are frozen by the snapshot
	// contract: the top-level keys below serialize as the snake_case names in
	// curation snapshots and always render as JSON objects, never null.

	causalTrials         map[domain.TenantID]map[domain.SpaceID]map[domain.CausalTrialEventID]domain.CausalTrialEvent
	causalTrialOrder     map[domain.TenantID]map[domain.SpaceID][]domain.CausalTrialEventID
	causalTrialHeads     map[domain.TenantID]map[domain.SpaceID]string
	causalEstimates      map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]map[int64]domain.CausalEstimateRevision
	causalEstimateLatest map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]int64
	causalEstimateHeads  map[domain.TenantID]map[domain.SpaceID]map[domain.CausalEstimateID]string
	causalRewards        map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]map[int64]domain.CausalRewardRevision
	causalRewardLatest   map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]int64
	causalRewardHeads    map[domain.TenantID]map[domain.SpaceID]map[domain.CausalRewardID]string

	diveTrajectories      map[domain.TenantID]map[domain.ExplorationSessionID]domain.DiveTrajectory
	diveServedOrder       map[domain.TenantID]map[domain.ExplorationSessionID][]string
	diveResults           map[domain.TenantID]map[domain.ExplorationSessionID]domain.DiveResult
	diveTrajectoryDigests map[domain.TenantID]map[domain.ExplorationSessionID]string

	consolidationActivity  map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationActivity
	consolidationCursors   map[domain.TenantID]map[domain.SpaceID]domain.ConsolidationCursor
	projectionHeads        map[domain.TenantID]map[domain.SpaceID]domain.ProjectionHead
	projectionVersions     map[domain.TenantID]map[domain.SpaceID]map[domain.ProjectionVersion]domain.DerivedProjection
	consolidationRounds    map[domain.TenantID]map[domain.SpaceID]map[domain.ConsolidationRoundID]domain.RoundResult
	retrievalReplayPlans   map[string]domain.RetrievalReplayPlan
	retrievalReplayResults map[domain.ConsolidationRoundID]domain.CandidateStats

	patternRevisions         map[domain.TenantID]map[domain.SpaceID]map[string]map[int64]domain.PatternRevision
	patternLatest            map[domain.TenantID]map[domain.SpaceID]map[string]int64
	proposalRounds           map[domain.TenantID]map[domain.SpaceID]map[string]domain.ProposalRoundOutcome
	proposals                map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillProposal
	proposalFingerprintIndex map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]string
	rejectionMemory          map[domain.TenantID]map[domain.SpaceID]map[domain.ProposalFingerprint]domain.RejectionMemory
	reviewedDiffs            map[string]domain.ReviewedDiff
	candidates               map[domain.TenantID]map[domain.SpaceID]map[string]domain.SkillCandidate
	candidateByProposal      map[string]string
	pairedReplayPlans        map[string]domain.PairedReplayPlan
	pairedReplayTrials       map[string]map[domain.ReplayArm]map[int]domain.PairedReplayTrial
	pairedReplayResults      map[string]domain.PairedReplayResult
	mutationBacktestResults  map[string]any
	candidateDecisions       map[string]domain.CandidateDecision
	skillActivations         map[string]map[string]domain.SkillActivation
	activeSkillVersions      map[string]int64
}

type stageKey struct {
	spaceID domain.SpaceID
	key     string
}

type evidenceSpaceKey struct {
	tenantID domain.TenantID
	spaceID  domain.SpaceID
}

type sessionKey struct {
	tenant      domain.TenantID
	principal   domain.PrincipalID
	idempotency string
}

type storedOperation struct {
	RequestJSON []byte `json:"request_json"`
	Response    []byte `json:"response"`
}

func New() *Store {
	store := &Store{
		tenants:        make(map[domain.TenantID]domain.Tenant),
		principals:     make(map[domain.TenantID]map[domain.PrincipalID]domain.Principal),
		spaces:         make(map[domain.TenantID]map[domain.SpaceID]domain.Space),
		grants:         make(map[domain.TenantID]map[domain.GrantID]domain.Grant),
		batches:        make(map[domain.TenantID]map[domain.BatchID]domain.EvidenceBatch),
		stageKeys:      make(map[domain.TenantID]map[stageKey]domain.BatchID),
		publishedOrder: make(map[evidenceSpaceKey][]domain.BatchID),
		batchVersion:   make(map[domain.BatchID]int64),
		sessions:       make(map[domain.ExplorationSessionID]domain.ExplorationSession),
		sessionByKey:   make(map[sessionKey]domain.ExplorationSessionID),
		operations:     make(map[domain.ExplorationSessionID]map[string]storedOperation),
		servedItems:    make(map[domain.ExplorationSessionID]map[string]domain.RecallItem),
		navigationRuns: make(map[domain.ExplorationSessionID]map[string]ports.NavigationRunJournal),
	}
	store.initCuration()
	return store
}

func (s *Store) InitializeTenant(ctx context.Context, tenant domain.Tenant, bootstrap domain.Principal) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.tenants[tenant.ID]; ok {
		if existing.DisplayName == tenant.DisplayName && existing.BootstrapPrincipalID == tenant.BootstrapPrincipalID {
			return false, nil
		}
		return false, errConflict
	}
	if len(s.tenants) > 0 {
		return false, errConflict
	}
	s.tenants[tenant.ID] = tenant
	s.principals[tenant.ID] = map[domain.PrincipalID]domain.Principal{bootstrap.ID: bootstrap}
	s.spaces[tenant.ID] = make(map[domain.SpaceID]domain.Space)
	s.grants[tenant.ID] = make(map[domain.GrantID]domain.Grant)
	s.batches[tenant.ID] = make(map[domain.BatchID]domain.EvidenceBatch)
	s.stageKeys[tenant.ID] = make(map[stageKey]domain.BatchID)
	return true, nil
}

func (s *Store) PutPrincipal(ctx context.Context, tenantID domain.TenantID, principal domain.Principal) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := s.principals[tenantID]
	if byID == nil {
		return false, errTenantMissing()
	}
	if existing, ok := byID[principal.ID]; ok {
		if existing == principal {
			return false, nil
		}
		return false, errConflict
	}
	byID[principal.ID] = principal
	return true, nil
}

func (s *Store) PutSpace(ctx context.Context, tenantID domain.TenantID, space domain.Space) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := s.spaces[tenantID]
	if byID == nil {
		return false, errTenantMissing()
	}
	if existing, ok := byID[space.ID]; ok {
		// Version advances with later evidence commits; registration replay
		// must stay idempotent, so only request-derived fields compare.
		sameScope := existing.Scope == space.Scope && existing.TenantID == space.TenantID && existing.DisplayName == space.DisplayName
		sameOwner := (existing.OwnerPrincipalID == nil && space.OwnerPrincipalID == nil) ||
			(existing.OwnerPrincipalID != nil && space.OwnerPrincipalID != nil && *existing.OwnerPrincipalID == *space.OwnerPrincipalID)
		if sameScope && sameOwner {
			return false, nil
		}
		return false, errConflict
	}
	byID[space.ID] = space
	return true, nil
}

func (s *Store) PutGrant(ctx context.Context, tenantID domain.TenantID, grant domain.Grant) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := s.grants[tenantID]
	if byID == nil {
		return false, errTenantMissing()
	}
	if existing, ok := byID[grant.ID]; ok {
		if reflect.DeepEqual(existing, grant) {
			return false, nil
		}
		return false, errConflict
	}
	byID[grant.ID] = grant
	return true, nil
}

func errTenantMissing() error {
	return domain.NewProtocolError(409, "TENANT_NOT_INITIALIZED", "tenant is not initialized")
}

func (s *Store) Space(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID) (domain.Space, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := s.spaces[tenantID]
	if byID == nil {
		return domain.Space{}, domain.NewProtocolError(404, "SPACE_NOT_FOUND", "space not found")
	}
	space, ok := byID[spaceID]
	if !ok {
		return domain.Space{}, domain.NewProtocolError(404, "SPACE_NOT_FOUND", "space not found")
	}
	return space, nil
}

func (s *Store) Principal(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID) (domain.Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	principal, ok := s.principals[tenantID][principalID]
	if !ok {
		return domain.Principal{}, domain.NewProtocolError(404, "PRINCIPAL_NOT_FOUND", "principal not found")
	}
	return principal, nil
}

func (s *Store) Grants(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID) ([]domain.Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var all []domain.Grant
	for _, grant := range s.grants[tenantID] {
		if grant.PrincipalID == principalID {
			all = append(all, grant)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	return all, nil
}

func (s *Store) ActiveGrants(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, now time.Time) ([]domain.Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var active []domain.Grant
	for _, grant := range s.grants[tenantID] {
		if grant.PrincipalID == principalID && grant.ActiveAt(now) {
			active = append(active, grant)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	return active, nil
}

func (s *Store) Stage(ctx context.Context, batch domain.EvidenceBatch) (domain.EvidenceBatch, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := s.batches[batch.TenantID]
	if byID == nil {
		byID = make(map[domain.BatchID]domain.EvidenceBatch)
		s.batches[batch.TenantID] = byID
	}
	keys := s.stageKeys[batch.TenantID]
	if keys == nil {
		keys = make(map[stageKey]domain.BatchID)
		s.stageKeys[batch.TenantID] = keys
	}

	key := stageKey{spaceID: batch.SpaceID, key: batch.IdempotencyKey}
	if existingID, ok := keys[key]; ok {
		existing := byID[existingID]
		if existing.ID != batch.ID || !sameBatch(existing, batch) {
			return domain.EvidenceBatch{}, false, errConflict
		}
		return existing, true, nil
	}
	if existing, ok := byID[batch.ID]; ok {
		if !sameBatch(existing, batch) {
			return domain.EvidenceBatch{}, false, errConflict
		}
		return existing, true, nil
	}

	stored := cloneBatch(batch)
	stored.State = domain.EvidenceBatchStaged
	stored.MemoryVersion = nil
	stored.CommittedAt = nil
	byID[stored.ID] = stored
	keys[key] = stored.ID
	return cloneBatch(stored), false, nil
}

func (s *Store) Commit(ctx context.Context, tenantID domain.TenantID, batchID domain.BatchID, commitID string, committedAt time.Time) (domain.EvidenceBatch, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := s.batches[tenantID]
	batch, ok := byID[batchID]
	if !ok {
		return domain.EvidenceBatch{}, false, errNotFound
	}
	if batch.State == domain.EvidenceBatchCommitted {
		return cloneBatch(batch), true, nil
	}

	// One critical section: manifest write, version advance, and publication
	// happen together or not at all. The batch version is the space's commit
	// watermark; the space version never decreases and never drops below a
	// provisioned floor.
	spaces := s.spaces[tenantID]
	space, ok := spaces[batch.SpaceID]
	if !ok {
		return domain.EvidenceBatch{}, false, domain.NewProtocolError(404, "SPACE_NOT_FOUND", "space not found")
	}
	orderKey := evidenceSpaceKey{tenantID: tenantID, spaceID: batch.SpaceID}
	version := int64(len(s.publishedOrder[orderKey])) + 1
	if space.Version < version {
		space.Version = version
		spaces[batch.SpaceID] = space
	}

	batch.State = domain.EvidenceBatchCommitted
	versionCopy := version
	batch.MemoryVersion = &versionCopy
	at := committedAt
	batch.CommittedAt = &at
	byID[batchID] = batch
	s.batchVersion[batchID] = version
	s.publishedOrder[orderKey] = append(s.publishedOrder[orderKey], batchID)
	s.bumpCommittedBatch(tenantID, batch.SpaceID)
	return cloneBatch(batch), false, nil
}

func (s *Store) Batch(ctx context.Context, tenantID domain.TenantID, batchID domain.BatchID) (domain.EvidenceBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch, ok := s.batches[tenantID][batchID]
	if !ok {
		return domain.EvidenceBatch{}, errNotFound
	}
	return cloneBatch(batch), nil
}

// CommittedEvidenceBatches returns immutable committed batches in ascending
// memory-version order for the half-open build window (after, through].
func (s *Store) CommittedEvidenceBatches(ctx context.Context, tenantID domain.TenantID, spaceID domain.SpaceID, afterMemoryVersion, throughMemoryVersion int64) ([]domain.EvidenceBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if afterMemoryVersion < 0 || throughMemoryVersion < afterMemoryVersion {
		return nil, domain.NewProtocolError(422, "INVALID_MEMORY_WINDOW", "evidence memory window is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.spaces[tenantID][spaceID]; !ok {
		return nil, domain.NewProtocolError(404, "SPACE_NOT_FOUND", "space not found")
	}
	batches := make([]domain.EvidenceBatch, 0)
	for _, batchID := range s.publishedOrder[evidenceSpaceKey{tenantID: tenantID, spaceID: spaceID}] {
		batch, ok := s.batches[tenantID][batchID]
		if !ok || batch.TenantID != tenantID || batch.SpaceID != spaceID || batch.State != domain.EvidenceBatchCommitted || batch.MemoryVersion == nil {
			continue
		}
		if *batch.MemoryVersion > afterMemoryVersion && *batch.MemoryVersion <= throughMemoryVersion {
			batches = append(batches, cloneBatch(batch))
		}
	}
	return batches, nil
}

// RecordSuccessfulRecall increments query activity for exactly the authorized
// pinned spaces after retrieval has completed without degradation.
func (s *Store) RecordSuccessfulRecall(ctx context.Context, tenantID domain.TenantID, pinned []domain.PinnedSpace) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[domain.SpaceID]struct{}, len(pinned))
	for _, pin := range pinned {
		if _, duplicate := seen[pin.SpaceID]; duplicate {
			return domain.NewProtocolError(422, "DUPLICATE_SPACE", "pinned spaces must be unique")
		}
		seen[pin.SpaceID] = struct{}{}
		if _, ok := s.spaces[tenantID][pin.SpaceID]; !ok {
			return domain.NewProtocolError(404, "SPACE_NOT_FOUND", "space not found")
		}
	}
	for _, pin := range pinned {
		bySpace := ensure2(s.consolidationActivity, tenantID, pin.SpaceID)
		activity := bySpace[pin.SpaceID]
		activity.SpaceID = pin.SpaceID
		activity.QueryOrdinal++
		bySpace[pin.SpaceID] = activity
	}
	return nil
}

// CommittedEvidenceDocuments copies the complete exact pinned view in one
// critical section. The returned values share no maps or slices with Store, so
// retrieval can tokenize and call providers after the authoritative lock is
// released.
func (s *Store) CommittedEvidenceDocuments(ctx context.Context, tenantID domain.TenantID, pinned []domain.PinnedSpace) ([]ports.EvidenceDocument, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	spaces := s.spaces[tenantID]
	if spaces == nil {
		return nil, errTenantMissing()
	}
	seenSpaces := make(map[domain.SpaceID]struct{}, len(pinned))
	for _, pin := range pinned {
		if _, duplicate := seenSpaces[pin.SpaceID]; duplicate {
			return nil, domain.NewProtocolError(422, "DUPLICATE_SPACE", "pinned spaces must be unique")
		}
		seenSpaces[pin.SpaceID] = struct{}{}
		if _, ok := spaces[pin.SpaceID]; !ok {
			return nil, domain.NewProtocolError(404, "SPACE_NOT_FOUND", "space not found")
		}
		if pin.MemoryVersion < 0 {
			return nil, domain.NewProtocolError(422, "INVALID_MEMORY_VERSION", "pinned memory version must not be negative")
		}
	}

	documents := make([]ports.EvidenceDocument, 0)
	for _, pin := range pinned {
		for _, batchID := range s.publishedOrder[evidenceSpaceKey{tenantID: tenantID, spaceID: pin.SpaceID}] {
			batch, ok := s.batches[tenantID][batchID]
			if !ok || batch.TenantID != tenantID || batch.SpaceID != pin.SpaceID ||
				batch.State != domain.EvidenceBatchCommitted || batch.MemoryVersion == nil ||
				*batch.MemoryVersion > pin.MemoryVersion {
				continue
			}
			for _, event := range batch.Events {
				documents = append(documents, ports.EvidenceDocument{
					SpaceID: pin.SpaceID, MemoryVersion: *batch.MemoryVersion,
					BatchID: batch.ID, EventID: event.ID, Content: event.Content,
				})
			}
		}
	}
	return documents, nil
}

// Recall returns ranking over committed events in the pinned spaces whose
// commit version is at most the pinned version. Identical content is served
// once — the first committed occurrence wins — so a host that restages
// earlier history inside later batches cannot crowd the result list with
// duplicate citations. Ranking uses case-insensitive word overlap between
// query and content, normalized by query length and by content length (sqrt):
// a compact entry that answers the query outranks a long transcript that
// merely happens to contain the query words. Items without overlap are not
// served.
func (s *Store) Recall(ctx context.Context, tenantID domain.TenantID, pinned []domain.PinnedSpace, query string, maxResults int) ([]domain.RecallItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queryWords := wordSet(query)
	type scored struct {
		item  domain.RecallItem
		score float64
	}
	var matches []scored
	seen := make(map[[sha256.Size]byte]struct{})
	for _, pin := range pinned {
		for _, batchID := range s.publishedOrder[evidenceSpaceKey{tenantID: tenantID, spaceID: pin.SpaceID}] {
			batch := s.batches[tenantID][batchID]
			if batch.TenantID != tenantID || batch.State != domain.EvidenceBatchCommitted || batch.MemoryVersion == nil {
				continue
			}
			if *batch.MemoryVersion > pin.MemoryVersion {
				continue
			}
			for _, event := range batch.Events {
				digest := sha256.Sum256([]byte(event.Content))
				if _, duplicate := seen[digest]; duplicate {
					continue
				}
				seen[digest] = struct{}{}
				contentWords := wordSet(event.Content)
				overlap := 0
				for word := range queryWords {
					if contentWords[word] {
						overlap++
					}
				}
				if overlap == 0 {
					continue
				}
				score := (float64(overlap) / float64(len(queryWords))) / math.Sqrt(float64(len(contentWords)))
				matches = append(matches, scored{
					item: domain.RecallItem{
						Content:       event.Content,
						SourceSpaceID: pin.SpaceID,
						MemoryVersion: *batch.MemoryVersion,
						Citation: domain.Citation{
							ID:              evidenceCitationID(batch.ID, event.ID),
							EvidenceBatchID: batch.ID,
							EventIDs:        []string{event.ID},
						},
						Score: score,
					},
					score: score,
				})
			}
		}
	}
	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].score != matches[j].score {
			return matches[i].score > matches[j].score
		}
		return matches[i].item.Citation.ID < matches[j].item.Citation.ID
	})
	if len(matches) > maxResults {
		matches = matches[:maxResults]
	}
	items := make([]domain.RecallItem, 0, len(matches))
	for _, match := range matches {
		items = append(items, match.item)
	}
	return items, nil
}

// Retrieve adapts the deterministic lexical baseline to the outcome-bearing
// RecallStore port. Production composition uses retrieval.Engine instead.
func (s *Store) Retrieve(ctx context.Context, tenantID domain.TenantID, pinned []domain.PinnedSpace, query string, maxResults int) (ports.RecallOutcome, error) {
	items, err := s.Recall(ctx, tenantID, pinned, query, maxResults)
	if err != nil {
		return ports.RecallOutcome{}, err
	}
	return ports.RecallOutcome{Items: items, Applied: []ports.RetrievalChannel{ports.RetrievalBM25}}, nil
}

func (s *Store) Start(ctx context.Context, session domain.ExplorationSession, idempotencyKey string) (domain.ExplorationSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := sessionKey{tenant: session.TenantID, principal: session.PrincipalID, idempotency: idempotencyKey}
	if existingID, ok := s.sessionByKey[key]; ok {
		existing := s.sessions[existingID]
		// Replays must be semantically identical to the original start.
		// Pinned versions, progress counters, and state are excluded: they
		// legitimately advance after the original call.
		if !sameStart(existing, session) {
			return domain.ExplorationSession{}, false, errConflict
		}
		return cloneSession(existing), true, nil
	}
	stored := cloneSession(session)
	stored.State = "active"
	s.sessions[stored.ID] = stored
	s.sessionByKey[key] = stored.ID
	if s.servedItems[stored.ID] == nil {
		s.servedItems[stored.ID] = make(map[string]domain.RecallItem)
	}
	return cloneSession(stored), false, nil
}

// sameStart compares the semantic content of an exploration start: the
// request_id, exact Space set, query, and budget. Anything else on the session
// is derived state that may differ between the original call and a replay.
func sameStart(a, b domain.ExplorationSession) bool {
	return a.RequestID == b.RequestID &&
		a.Query == b.Query &&
		a.Budget == b.Budget &&
		reflect.DeepEqual(a.SpaceIDs, b.SpaceIDs)
}

func (s *Store) Session(ctx context.Context, tenantID domain.TenantID, sessionID domain.ExplorationSessionID) (domain.ExplorationSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.sessions[sessionID]
	if !ok || session.TenantID != tenantID {
		return domain.ExplorationSession{}, errSession
	}
	return cloneSession(session), nil
}

// ServedItem resolves only citations previously exposed by this exploration.
// Graph traversal uses it as the anchor fence, so callers cannot probe the
// projection with arbitrary evidence or node identifiers.
func (s *Store) ServedItem(ctx context.Context, sessionID domain.ExplorationSessionID, citationID string) (domain.RecallItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[sessionID]; !ok {
		return domain.RecallItem{}, errSession
	}
	item, ok := s.servedItems[sessionID][citationID]
	if !ok {
		return domain.RecallItem{}, domain.NewProtocolError(422, "ANCHOR_NOT_SERVED", "anchor citation was not served by this exploration session")
	}
	cloned := item
	cloned.Citation.EventIDs = append([]string(nil), item.Citation.EventIDs...)
	if item.Traversal != nil {
		metadata := *item.Traversal
		cloned.Traversal = &metadata
	}
	return cloned, nil
}

// ServedCitations is implemented by exploration responses so Apply can record
// exactly which citations the session served.
type ServedCitations = domain.ServedCitations

// Replay returns an existing operation response before callers perform fresh
// graph reads or budget checks. A changed request under the same key fails
// closed; a missing key leaves the caller free to prepare and apply the step.
func (s *Store) Replay(ctx context.Context, sessionID domain.ExplorationSessionID, operationID string, operation domain.ExplorationOperation) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[sessionID]; !ok {
		return nil, false, errSession
	}
	stored, found := s.operations[sessionID][operationID]
	if !found {
		return nil, false, nil
	}
	semantic, err := operation.SemanticJSON()
	if err != nil {
		return nil, false, err
	}
	if string(stored.RequestJSON) != string(semantic) {
		return nil, false, errConflict
	}
	return append([]byte(nil), stored.Response...), true, nil
}

// Apply executes mutate and stores the JSON-encoded response keyed by the
// operation ID. Replaying the same operation returns the stored response; a
// changed request under the same key fails closed. Everything runs under the
// store mutex so step counting, session mutation, citation recording, and
// response storage are one atomic section.
func (s *Store) Apply(ctx context.Context, sessionID domain.ExplorationSessionID, operationID string, operation domain.ExplorationOperation) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.sessions[sessionID]
	if !ok {
		return nil, false, errSession
	}
	ops := s.operations[sessionID]
	if ops == nil {
		ops = make(map[string]storedOperation)
		s.operations[sessionID] = ops
	}
	semantic, err := operation.SemanticJSON()
	if err != nil {
		return nil, false, err
	}
	if stored, ok := ops[operationID]; ok {
		if string(stored.RequestJSON) != string(semantic) {
			return nil, false, errConflict
		}
		return append([]byte(nil), stored.Response...), true, nil
	}

	served := s.servedItems[sessionID]
	lookup := func(citationID string) (domain.RecallItem, bool) {
		item, ok := served[citationID]
		return item, ok
	}
	next, response, err := operation.Mutate(cloneSession(session), lookup)
	if err != nil {
		return nil, false, err
	}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return nil, false, err
	}
	s.sessions[sessionID] = cloneSession(next)
	if citationSource, ok := response.(ServedCitations); ok {
		if served == nil {
			served = make(map[string]domain.RecallItem)
			s.servedItems[sessionID] = served
		}
		for _, item := range citationSource.ServedCitations() {
			served[item.Citation.ID] = item
			s.noteServedCitation(session.TenantID, sessionID, item.Citation.ID)
		}
	}
	ops[operationID] = storedOperation{RequestJSON: semantic, Response: responseJSON}
	return responseJSON, false, nil
}

// RecordServed remembers recall items served outside an Apply step (the
// exploration start query) so submit can accept only session-served citations.
// Distinct citations also count once against the session result budget;
// replaying a start adds nothing new and therefore does not move the counter.
func (s *Store) RecordServed(ctx context.Context, sessionID domain.ExplorationSessionID, items []domain.RecallItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.sessions[sessionID]
	if !ok {
		return errSession
	}
	served := s.servedItems[sessionID]
	if served == nil {
		served = make(map[string]domain.RecallItem)
		s.servedItems[sessionID] = served
	}
	for _, item := range items {
		if _, seen := served[item.Citation.ID]; !seen {
			session.ResultsServed++
			s.noteServedCitation(session.TenantID, sessionID, item.Citation.ID)
		}
		served[item.Citation.ID] = item
	}
	s.sessions[sessionID] = session
	return nil
}

func cloneBatch(batch domain.EvidenceBatch) domain.EvidenceBatch {
	cloned := batch
	cloned.Events = append([]domain.EvidenceEvent(nil), batch.Events...)
	cloned.Links = append([]domain.EvidenceLink(nil), batch.Links...)
	if batch.MemoryVersion != nil {
		version := *batch.MemoryVersion
		cloned.MemoryVersion = &version
	}
	if batch.CommittedAt != nil {
		at := *batch.CommittedAt
		cloned.CommittedAt = &at
	}
	return cloned
}

func cloneSession(session domain.ExplorationSession) domain.ExplorationSession {
	cloned := session
	cloned.SpaceIDs = append([]domain.SpaceID(nil), session.SpaceIDs...)
	cloned.PinnedSpaces = append([]domain.PinnedSpace(nil), session.PinnedSpaces...)
	return cloned
}

func sameBatch(a, b domain.EvidenceBatch) bool {
	a.State = ""
	b.State = ""
	a.MemoryVersion = nil
	b.MemoryVersion = nil
	a.CommittedAt = nil
	b.CommittedAt = nil
	return reflect.DeepEqual(a, b)
}

func evidenceCitationID(batchID domain.BatchID, eventID string) string {
	return domain.StableCitationID("cit-", string(batchID), eventID)
}

func wordSet(text string) map[string]bool {
	words := make(map[string]bool)
	for _, word := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !('a' <= r && r <= 'z' || '0' <= r && r <= '9' || r >= 0x80)
	}) {
		words[word] = true
	}
	return words
}
