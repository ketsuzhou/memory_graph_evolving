package exploration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

type StartRequest struct {
	RequestID      string
	IdempotencyKey string
	SpaceIDs       []domain.SpaceID
	Query          string
	MaxSteps       int
	MaxResults     int
}

type StartResult struct {
	Session        domain.ExplorationSession
	Items          []domain.RecallItem
	RemainingSteps int
	Duplicate      bool
}

type ExploreRequest struct {
	OperationID      string
	AnchorCitationID string
	Relation         string
	Limit            int
}

type RedirectRequest struct {
	OperationID       string
	Query             string
	AnchorCitationIDs []string
	Reason            string
}

type StepResult struct {
	SessionID      domain.ExplorationSessionID
	OperationID    string
	State          string
	Items          []domain.RecallItem
	RemainingSteps int
	Duplicate      bool
}

// ServedCitations exposes the recall items this step served so the store
// records them under the session for submit-time fencing.
func (r StepResult) ServedCitations() []domain.RecallItem {
	return append([]domain.RecallItem(nil), r.Items...)
}

type SubmitRequest struct {
	OperationID string
	Found       bool
	Summary     string
	CitationIDs []string
}

type SubmittedCitation struct {
	CitationID      string
	SourceSpaceID   domain.SpaceID
	MemoryVersion   int64
	EvidenceBatchID domain.BatchID
}

type SubmitResult struct {
	SessionID   domain.ExplorationSessionID
	OperationID string
	State       string
	Found       bool
	Summary     string
	Citations   []SubmittedCitation
	Duplicate   bool
}

type Service struct {
	authorizer     *authz.Authorizer
	registry       ports.RegistryStore
	recallStore    ports.RecallStore
	neighborFinder ports.EmbeddingNeighborFinder
	store          explorationRepository
	clock          ports.Clock
}

func New(registry ports.RegistryStore, recallStore ports.RecallStore, store explorationRepository, clock ports.Clock) *Service {
	service := &Service{
		authorizer:  authz.NewAuthorizer(registry, clock),
		registry:    registry,
		recallStore: recallStore,
		store:       store,
		clock:       clock,
	}
	service.neighborFinder, _ = recallStore.(ports.EmbeddingNeighborFinder)
	return service
}

// AuthorizeNavigation checks the current session binding and grants without
// exposing served content. Automatic navigation calls it before reserving a
// run ID; each later observation and suboperation is authorized again.
func (s *Service) AuthorizeNavigation(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID) (domain.ExplorationSession, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return domain.ExplorationSession{}, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationExplore); err != nil {
		return domain.ExplorationSession{}, err
	}
	return session, nil
}

// ObserveNavigation returns only the session query/budgets and already served
// evidence needed by the navigation orchestrator. It re-checks current access
// before any content can be sent to an external selector.
func (s *Service) ObserveNavigation(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, limit int) (domain.ExplorationSession, []domain.RecallItem, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return domain.ExplorationSession{}, nil, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationExplore); err != nil {
		return domain.ExplorationSession{}, nil, err
	}
	store, ok := s.store.(interface {
		ServedNavigationItems(context.Context, domain.ExplorationSessionID, int) ([]domain.RecallItem, error)
	})
	if !ok {
		return domain.ExplorationSession{}, nil, domain.NewProtocolError(503, "PATH_SELECTION_UNAVAILABLE", "navigation observation store is unavailable")
	}
	items, err := store.ServedNavigationItems(ctx, sessionID, limit)
	if err != nil {
		return domain.ExplorationSession{}, nil, err
	}
	return session, items, nil
}

// Start opens a fenced exploration over an exact Space set. Any private Space
// is rejected 403 even when a Grant covers it: Memory Agent exploration is
// Room-shared-only in this tracer. Authorization failure never reserves the
// idempotency key. The latest projection whose evidence watermark is visible
// at each pinned MemoryVersion is fixed for the whole session.
func (s *Service) Start(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, request StartRequest) (StartResult, error) {
	for _, spaceID := range request.SpaceIDs {
		space, err := s.registry.Space(ctx, tenantID, spaceID)
		if err != nil {
			return StartResult{}, err
		}
		if space.Scope == domain.SpacePrivate {
			return StartResult{}, domain.NewProtocolError(403, "SPACE_FORBIDDEN", "private spaces are not explorable in this tracer")
		}
	}
	pinned, err := s.authorizer.AuthorizeExact(ctx, authz.Identity{TenantID: tenantID, PrincipalID: principalID}, request.SpaceIDs, domain.GrantPurposeToolPlane, domain.GrantOperationExplorationStart)
	if err != nil {
		return StartResult{}, err
	}
	for index := range pinned {
		pinned[index], err = s.pinProjection(ctx, tenantID, pinned[index])
		if err != nil {
			return StartResult{}, err
		}
	}

	session := domain.ExplorationSession{
		ID:           domain.ExplorationSessionID(newSessionID()),
		TenantID:     tenantID,
		PrincipalID:  principalID,
		SpaceIDs:     append([]domain.SpaceID(nil), request.SpaceIDs...),
		PinnedSpaces: pinned,
		RequestID:    request.RequestID,
		Query:        request.Query,
		Budget:       domain.ExplorationBudget{MaxSteps: request.MaxSteps, MaxResults: request.MaxResults},
		StepsUsed:    0,
		State:        "active",
		ExpiresAt:    s.clock.Now().Add(sessionTTL),
	}
	stored, duplicate, err := s.store.Start(ctx, session, request.IdempotencyKey)
	if err != nil {
		return StartResult{}, err
	}

	// Recall at the pinned versions is deterministic, so replayed starts
	// reproduce the original served items.
	outcome, err := s.recallStore.Retrieve(ctx, tenantID, stored.PinnedSpaces, stored.Query, stored.Budget.MaxResults)
	if err != nil {
		return StartResult{}, err
	}
	items := outcome.Items
	if err := s.store.RecordServed(ctx, stored.ID, items); err != nil {
		return StartResult{}, err
	}
	responseSession := stored
	if duplicate {
		// Start idempotency returns the original start view, not mutable
		// trajectory state that may have advanced or become terminal.
		responseSession.State = "active"
		responseSession.StepsUsed = 0
		responseSession.ResultsServed = distinctCitationCount(items)
	}
	return StartResult{
		Session:        responseSession,
		Items:          items,
		RemainingSteps: responseSession.Budget.MaxSteps,
		Duplicate:      duplicate,
	}, nil
}

// Explore performs one real, projection-backed graph step. The anchor must be
// a citation already served by this session. Raw recall citations are resolved
// through node EvidenceRefs; projection citations identify the exact node for
// the next hop. No lexical fallback is used when the projection has no match.
func (s *Service) Explore(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, request ExploreRequest) (StepResult, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return StepResult{}, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationExplore); err != nil {
		return StepResult{}, err
	}
	operation := exploreOperation{request: request, sessionID: sessionID}
	if result, found, err := s.replayStep(ctx, sessionID, request.OperationID, operation); err != nil || found {
		return result, err
	}
	limit := request.Limit
	if remaining := remainingResults(session); remaining < limit {
		limit = remaining
	}
	if limit <= 0 {
		return StepResult{}, domain.NewProtocolError(429, "EXPLORATION_BUDGET_EXHAUSTED", "exploration result budget exhausted")
	}
	anchor, err := s.store.ServedItem(ctx, sessionID, request.AnchorCitationID)
	if err != nil {
		return StepResult{}, err
	}
	items, err := s.expandProjection(ctx, tenantID, session, anchor, request.Relation, limit)
	if err != nil {
		return StepResult{}, err
	}
	operation.items = items
	return s.applyStep(ctx, sessionID, request.OperationID, operation)
}

// Redirect re-queries inside the pinned scope after a steering instruction.
// The re-query is capped by the session's remaining result budget, not just
// the per-session MaxResults ceiling.
func (s *Service) Redirect(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, request RedirectRequest) (StepResult, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return StepResult{}, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationRedirect); err != nil {
		return StepResult{}, err
	}
	operation := redirectOperation{request: request, sessionID: sessionID}
	if result, found, err := s.replayStep(ctx, sessionID, request.OperationID, operation); err != nil || found {
		return result, err
	}
	limit := session.Budget.MaxResults
	if remaining := remainingResults(session); remaining < limit {
		limit = remaining
	}
	if limit <= 0 {
		return StepResult{}, domain.NewProtocolError(429, "EXPLORATION_BUDGET_EXHAUSTED", "exploration result budget exhausted")
	}
	outcome, err := s.recallStore.Retrieve(ctx, tenantID, session.PinnedSpaces, request.Query, limit)
	if err != nil {
		return StepResult{}, err
	}
	items := outcome.Items
	operation.items = items
	return s.applyStep(ctx, sessionID, request.OperationID, operation)
}

// Submit terminates the session; every citation must have been served by this
// session. The submit operation itself is re-authorized before it applies.
func (s *Service) Submit(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, request SubmitRequest) (SubmitResult, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationSubmit); err != nil {
		return SubmitResult{}, err
	}
	operation := submitOperation{request: request, sessionID: sessionID}
	responseJSON, duplicate, err := s.store.Apply(ctx, sessionID, request.OperationID, operation)
	if err != nil {
		return SubmitResult{}, err
	}
	var result SubmitResult
	if err := json.Unmarshal(responseJSON, &result); err != nil {
		return SubmitResult{}, err
	}
	result.Duplicate = duplicate
	return result, nil
}

func (s *Service) ReplayExplore(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, request ExploreRequest) (StepResult, bool, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return StepResult{}, false, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationExplore); err != nil {
		return StepResult{}, false, err
	}
	return s.replayStep(ctx, sessionID, request.OperationID, exploreOperation{request: request, sessionID: sessionID})
}

func (s *Service) ReplayRedirect(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, request RedirectRequest) (StepResult, bool, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return StepResult{}, false, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationRedirect); err != nil {
		return StepResult{}, false, err
	}
	return s.replayStep(ctx, sessionID, request.OperationID, redirectOperation{request: request, sessionID: sessionID})
}

func (s *Service) ReplaySubmit(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, request SubmitRequest) (SubmitResult, bool, error) {
	session, err := s.loadSession(ctx, tenantID, principalID, sessionID)
	if err != nil {
		return SubmitResult{}, false, err
	}
	if err := s.authorizeStep(ctx, tenantID, principalID, session, domain.GrantOperationExplorationSubmit); err != nil {
		return SubmitResult{}, false, err
	}
	responseJSON, found, err := s.store.Replay(ctx, sessionID, request.OperationID, submitOperation{request: request, sessionID: sessionID})
	if err != nil || !found {
		return SubmitResult{}, found, err
	}
	var result SubmitResult
	if err := json.Unmarshal(responseJSON, &result); err != nil {
		return SubmitResult{}, false, err
	}
	result.Duplicate = true
	return result, true, nil
}

func (s *Service) applyStep(ctx context.Context, sessionID domain.ExplorationSessionID, operationID string, operation domain.ExplorationOperation) (StepResult, error) {
	responseJSON, duplicate, err := s.store.Apply(ctx, sessionID, operationID, operation)
	if err != nil {
		return StepResult{}, err
	}
	var result StepResult
	if err := json.Unmarshal(responseJSON, &result); err != nil {
		return StepResult{}, err
	}
	result.Duplicate = duplicate
	return result, nil
}

func (s *Service) replayStep(ctx context.Context, sessionID domain.ExplorationSessionID, operationID string, operation domain.ExplorationOperation) (StepResult, bool, error) {
	responseJSON, found, err := s.store.Replay(ctx, sessionID, operationID, operation)
	if err != nil || !found {
		return StepResult{}, found, err
	}
	var result StepResult
	if err := json.Unmarshal(responseJSON, &result); err != nil {
		return StepResult{}, false, err
	}
	result.Duplicate = true
	return result, true, nil
}

func (s *Service) loadSession(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID) (domain.ExplorationSession, error) {
	session, err := s.store.Session(ctx, tenantID, sessionID)
	if err != nil {
		return domain.ExplorationSession{}, err
	}
	if session.TenantID != tenantID || session.PrincipalID != principalID {
		return domain.ExplorationSession{}, domain.NewProtocolError(404, "EXPLORATION_NOT_FOUND", "exploration session not found")
	}
	if !s.clock.Now().Before(session.ExpiresAt) {
		return domain.ExplorationSession{}, domain.NewProtocolError(409, "EXPLORATION_EXPIRED", "exploration session has expired")
	}
	return session, nil
}

// authorizeStep re-runs the exact Space-set authorization for one step
// operation at the current time. Sessions never outlive their grants: expiry
// or a revoked operation mid-session fails closed before any read or state
// change consumes budget.
func (s *Service) authorizeStep(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, session domain.ExplorationSession, operation domain.GrantOperation) error {
	_, err := s.authorizer.AuthorizeExact(ctx, authz.Identity{TenantID: tenantID, PrincipalID: principalID}, session.SpaceIDs, domain.GrantPurposeToolPlane, operation)
	return err
}

// remainingResults is the session-wide result budget left for new items.
func remainingResults(session domain.ExplorationSession) int {
	remaining := session.Budget.MaxResults - session.ResultsServed
	if remaining < 0 {
		return 0
	}
	return remaining
}

func distinctCitationCount(items []domain.RecallItem) int {
	citations := make(map[string]struct{}, len(items))
	for _, item := range items {
		citations[item.Citation.ID] = struct{}{}
	}
	return len(citations)
}

const sessionTTL = time.Hour

func newSessionID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return "ex-" + hex.EncodeToString(raw[:])
}

type exploreOperation struct {
	request   ExploreRequest
	items     []domain.RecallItem
	sessionID domain.ExplorationSessionID
}

func (o exploreOperation) SemanticJSON() ([]byte, error) {
	return json.Marshal(struct {
		OperationID      string `json:"operation_id"`
		AnchorCitationID string `json:"anchor_citation_id"`
		Relation         string `json:"relation"`
		Limit            int    `json:"limit"`
	}{
		OperationID:      o.request.OperationID,
		AnchorCitationID: o.request.AnchorCitationID,
		Relation:         o.request.Relation,
		Limit:            o.request.Limit,
	})
}

func (o exploreOperation) Mutate(session domain.ExplorationSession, servedItem func(string) (domain.RecallItem, bool)) (domain.ExplorationSession, any, error) {
	if session.State != "active" {
		return session, nil, domain.NewProtocolError(409, "INVALID_STATE", "exploration session is terminal")
	}
	if session.StepsUsed+1 > session.Budget.MaxSteps {
		return session, nil, domain.NewProtocolError(429, "EXPLORATION_BUDGET_EXHAUSTED", "exploration step budget exhausted")
	}
	if session.ResultsServed+len(o.items) > session.Budget.MaxResults {
		return session, nil, domain.NewProtocolError(429, "EXPLORATION_BUDGET_EXHAUSTED", "exploration result budget exhausted")
	}
	session.StepsUsed++
	session.ResultsServed += len(o.items)
	return session, StepResult{
		SessionID:      o.sessionID,
		OperationID:    o.request.OperationID,
		State:          "active",
		Items:          o.items,
		RemainingSteps: session.Budget.MaxSteps - session.StepsUsed,
	}, nil
}

type redirectOperation struct {
	request   RedirectRequest
	items     []domain.RecallItem
	sessionID domain.ExplorationSessionID
}

func (o redirectOperation) SemanticJSON() ([]byte, error) {
	return json.Marshal(struct {
		OperationID       string   `json:"operation_id"`
		Query             string   `json:"query"`
		AnchorCitationIDs []string `json:"anchor_citation_ids"`
		Reason            string   `json:"reason"`
	}{
		OperationID:       o.request.OperationID,
		Query:             o.request.Query,
		AnchorCitationIDs: o.request.AnchorCitationIDs,
		Reason:            o.request.Reason,
	})
}

func (o redirectOperation) Mutate(session domain.ExplorationSession, servedItem func(string) (domain.RecallItem, bool)) (domain.ExplorationSession, any, error) {
	if session.State != "active" {
		return session, nil, domain.NewProtocolError(409, "INVALID_STATE", "exploration session is terminal")
	}
	if session.StepsUsed+1 > session.Budget.MaxSteps {
		return session, nil, domain.NewProtocolError(429, "EXPLORATION_BUDGET_EXHAUSTED", "exploration step budget exhausted")
	}
	if session.ResultsServed+len(o.items) > session.Budget.MaxResults {
		return session, nil, domain.NewProtocolError(429, "EXPLORATION_BUDGET_EXHAUSTED", "exploration result budget exhausted")
	}
	session.StepsUsed++
	session.ResultsServed += len(o.items)
	return session, StepResult{
		SessionID:      o.sessionID,
		OperationID:    o.request.OperationID,
		State:          "active",
		Items:          o.items,
		RemainingSteps: session.Budget.MaxSteps - session.StepsUsed,
	}, nil
}

type submitOperation struct {
	request   SubmitRequest
	sessionID domain.ExplorationSessionID
}

func (o submitOperation) SemanticJSON() ([]byte, error) {
	return json.Marshal(struct {
		OperationID string   `json:"operation_id"`
		Found       bool     `json:"found"`
		Summary     string   `json:"summary"`
		CitationIDs []string `json:"citation_ids"`
	}{
		OperationID: o.request.OperationID,
		Found:       o.request.Found,
		Summary:     o.request.Summary,
		CitationIDs: o.request.CitationIDs,
	})
}

func (o submitOperation) Mutate(session domain.ExplorationSession, servedItem func(string) (domain.RecallItem, bool)) (domain.ExplorationSession, any, error) {
	if session.State != "active" {
		return session, nil, domain.NewProtocolError(409, "INVALID_STATE", "exploration session is terminal")
	}
	if o.request.Found && len(o.request.CitationIDs) == 0 {
		return session, nil, domain.NewProtocolError(422, "CITATION_REQUIRED", "found submissions must cite session-served evidence")
	}
	if o.request.Found && o.request.Summary == "" {
		return session, nil, domain.NewProtocolError(422, "SUMMARY_REQUIRED", "found submissions must include a non-empty summary")
	}
	if !o.request.Found && o.request.Summary != "" {
		return session, nil, domain.NewProtocolError(422, "SUMMARY_NOT_ALLOWED", "unfound submissions must not carry a summary")
	}
	citations := make([]SubmittedCitation, 0, len(o.request.CitationIDs))
	for _, citationID := range o.request.CitationIDs {
		item, ok := servedItem(citationID)
		if !ok {
			return session, nil, domain.NewProtocolError(422, "CITATION_NOT_SERVED", "citation was not served by this exploration session")
		}
		citations = append(citations, SubmittedCitation{
			CitationID:      item.Citation.ID,
			SourceSpaceID:   item.SourceSpaceID,
			MemoryVersion:   item.MemoryVersion,
			EvidenceBatchID: item.Citation.EvidenceBatchID,
		})
	}
	session.State = "submitted"
	return session, SubmitResult{
		SessionID:   o.sessionID,
		OperationID: o.request.OperationID,
		State:       "submitted",
		Found:       o.request.Found,
		Summary:     o.request.Summary,
		Citations:   citations,
	}, nil
}
