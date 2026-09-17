// Package rejectredirect turns a rejected Skill disposition into a
// structured reason, a Memory Delivery, and a reason-driven next
// exploration that does not repeat the same revision, excluded lineage,
// or source set (Warm Skill Graph Batch contract §3.3).
//
// Resolution errors are notified separately and MUST NOT enter rejection
// reason statistics. Exhausting every remaining candidate terminates as
// all_candidates_rejected.
package rejectredirect

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"river2.dev/graph-memory-service/internal/skillevolution/evaluationexplore"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
)

const (
	ReasonNotApplicable         = "not_applicable"
	ReasonAlreadyKnown          = "already_known"
	ReasonAlreadyResolved       = "already_resolved"
	ReasonTooGeneric            = "too_generic"
	ReasonInsufficientContext   = "insufficient_context"
	ReasonIncorrectAssumption   = "incorrect_assumption"
	ReasonConflictsWithEvidence = "conflicts_with_evidence"
	ReasonDuplicateOffer        = "duplicate_offer"
	ReasonUnsafeOrHarmful       = "unsafe_or_harmful"

	ReasonResolutionError = "resolution_error"

	TaskContextOpeningOnly = "opening_only"

	TerminalAllCandidatesRejected = "all_candidates_rejected"
	TerminalNeedStopped           = "need_stopped"
	TerminalResolutionError       = "resolution_error"

	KindSkillRejected = "SKILL_REJECTED"
	RecipientMemory   = "memory-agent"

	ActionExploreSpecialization    = "explore_specialization"
	ActionExcludeInsightLineage    = "exclude_insight_lineage"
	ActionStopCurrentNeed          = "stop_current_need"
	ActionExploreGuardedDescendant = "explore_guarded_descendant"
	ActionExcludeAssumptionBranch  = "exclude_assumption_branches"
	ActionFindConflictAlternative  = "find_conflict_alternative"
	ActionExcludeLineageSourceSet  = "exclude_lineage_and_source_set"
	ActionBlacklistExactRevision   = "blacklist_exact_revision"

	GuardRequireSpecialization    = "require_specialization"
	GuardRequireGuardedDescendant = "require_guarded_descendant"
)

// CoreReasonCodes is the closed §3.3 set. Table-driven tests MUST cover all nine.
var CoreReasonCodes = []string{
	ReasonNotApplicable,
	ReasonAlreadyKnown,
	ReasonAlreadyResolved,
	ReasonTooGeneric,
	ReasonInsufficientContext,
	ReasonIncorrectAssumption,
	ReasonConflictsWithEvidence,
	ReasonDuplicateOffer,
	ReasonUnsafeOrHarmful,
}

var coreReasonSet = func() map[string]bool {
	out := make(map[string]bool, len(CoreReasonCodes))
	for _, code := range CoreReasonCodes {
		out[code] = true
	}
	return out
}()

var (
	ErrInvalidRejection = errors.New("reject redirect: invalid rejected disposition")
	ErrUnknownReason    = errors.New("reject redirect: unknown rejection reason_code")
	ErrInvalidOpening   = errors.New("reject redirect: invalid opening context")
	ErrViewRequired     = errors.New("reject redirect: evaluation view is required")
	ErrMemoryRequired   = errors.New("reject redirect: memory is required")
)

// RejectedDisposition is the Host-authored rejected feedback. Both
// reason_code and free-text reason are required.
type RejectedDisposition struct {
	OfferID    string
	AgentID    string
	Ref        string
	ReasonCode string
	Reason     string
	SourceSet  []string
}

// ResolutionNotice is a structured Memory notification for a
// resolution/serve failure. It is not a rejected disposition.
type ResolutionNotice struct {
	OfferID        string
	AgentID        string
	SkillReference string
	ReasonCode     string
	Failure        string
	Detail         string
}

// MemoryDelivery is the directed Memory payload for one rejection.
type MemoryDelivery struct {
	DeliveryID string
	Recipient  string
	Kind       string
	OfferID    string
	AgentID    string
	Ref        string
	ReasonCode string
	Reason     string
}

// Guard records a reason-driven applicability constraint on a rejected ref.
type Guard struct {
	Ref        string
	ReasonCode string
	Constraint string
}

// Policy is the episode-local Memory redirect/stop state.
type Policy struct {
	Guards               []Guard
	ExcludedLineages     []string
	ExcludedRevisions    []string
	ExcludedSourceSets   []string
	BlacklistedRevisions []string
	StopCurrentNeed      bool
	StopReason           string
}

// AuditHop is one offer → reason → next query link.
type AuditHop struct {
	OfferID    string
	Ref        string
	ReasonCode string
	Reason     string
	NextQuery  string
	NextSeeds  []string
	Action     string
}

// Memory owns episode-local rejection policy, deliveries, and the audit chain.
type Memory struct {
	NeedID                string
	TaskContextMonitoring string
	Policy                Policy
	AuditTrail            []AuditHop
	ReasonCounts          map[string]int
	Deliveries            []MemoryDelivery
	OfferedRefs           []string
	OfferedSourceSets     []string
	ResolutionNotices     []ResolutionNotice
}

func NewMemory(needID string) *Memory {
	return &Memory{
		NeedID:                needID,
		TaskContextMonitoring: TaskContextOpeningOnly,
		ReasonCounts:          map[string]int{},
	}
}

// Result is one Memory step: either a first offer, a rejection-driven
// redirect offer, or a terminal stop.
type Result struct {
	Delivery   *MemoryDelivery
	Offer      *evaluationexplore.PendingOffer
	NextQuery  string
	NextSeeds  []string
	Action     string
	Terminal   string
	AuditTrail []AuditHop
	Explore    evaluationexplore.Result
}

type exploreRequest struct {
	View    *evaluationgraph.View
	Opening evaluationexplore.OpeningContext
	Budget  evaluationexplore.Budget
}

// Offer explores from the opening context and records the first pending offer.
func Offer(mem *Memory, view *evaluationgraph.View, opening evaluationexplore.OpeningContext, budget evaluationexplore.Budget) (Result, error) {
	if mem == nil {
		return Result{}, ErrMemoryRequired
	}
	if view == nil {
		return Result{}, ErrViewRequired
	}
	if opening.CheckpointID == "" || opening.TargetAgentID == "" {
		return Result{}, fmt.Errorf("%w: checkpoint and target are required", ErrInvalidOpening)
	}
	got, err := exploreUnblocked(mem, exploreRequest{View: view, Opening: opening, Budget: budget}, opening.SeedRefs, opening.Query)
	if err != nil {
		return Result{}, err
	}
	out := Result{Explore: got, NextQuery: opening.Query, NextSeeds: append([]string{}, opening.SeedRefs...)}
	if got.Offer != nil {
		mem.recordOffer(view, got.Offer.SkillReference, nil)
		out.Offer = cloneOffer(got.Offer)
		return out, nil
	}
	out.Terminal = got.Terminal
	if out.Terminal == "" {
		out.Terminal = evaluationexplore.TerminalNoApplicableCandidate
	}
	return out, nil
}

// ApplyRejection records the structured reason, emits a Memory Delivery,
// updates guard/exclusion/blacklist/stop policy, appends the audit hop,
// and explores the next non-duplicate candidate when the need continues.
func ApplyRejection(mem *Memory, rejection RejectedDisposition, view *evaluationgraph.View, opening evaluationexplore.OpeningContext, budget evaluationexplore.Budget) (Result, error) {
	if mem == nil {
		return Result{}, ErrMemoryRequired
	}
	if view == nil {
		return Result{}, ErrViewRequired
	}
	if err := validateRejection(rejection); err != nil {
		return Result{}, err
	}
	if opening.CheckpointID == "" || opening.TargetAgentID == "" {
		return Result{}, fmt.Errorf("%w: checkpoint and target are required", ErrInvalidOpening)
	}

	if existing := mem.deliveryForOffer(rejection.OfferID); existing != nil {
		return Result{
			Delivery:   cloneDelivery(existing),
			AuditTrail: cloneHops(mem.AuditTrail),
			Action:     lastAction(mem.AuditTrail),
			Terminal:   terminalIfStopped(mem),
		}, nil
	}

	sourceSet := mergeSourceSet(view, rejection.Ref, rejection.SourceSet)
	mem.recordOffer(view, rejection.Ref, sourceSet)
	delivery := MemoryDelivery{
		DeliveryID: "delivery-reject-" + rejection.OfferID,
		Recipient:  RecipientMemory,
		Kind:       KindSkillRejected,
		OfferID:    rejection.OfferID,
		AgentID:    rejection.AgentID,
		Ref:        rejection.Ref,
		ReasonCode: rejection.ReasonCode,
		Reason:     rejection.Reason,
	}
	mem.Deliveries = append(mem.Deliveries, delivery)
	mem.ReasonCounts[rejection.ReasonCode]++

	plan, err := planRedirect(mem, rejection, view, opening)
	if err != nil {
		return Result{}, err
	}
	applyPlan(&mem.Policy, plan)
	hop := AuditHop{
		OfferID:    rejection.OfferID,
		Ref:        rejection.Ref,
		ReasonCode: rejection.ReasonCode,
		Reason:     rejection.Reason,
		NextQuery:  plan.NextQuery,
		NextSeeds:  append([]string{}, plan.NextSeeds...),
		Action:     plan.Action,
	}
	mem.AuditTrail = append(mem.AuditTrail, hop)

	out := Result{
		Delivery:   cloneDelivery(&delivery),
		NextQuery:  plan.NextQuery,
		NextSeeds:  append([]string{}, plan.NextSeeds...),
		Action:     plan.Action,
		AuditTrail: cloneHops(mem.AuditTrail),
	}
	if plan.Stop {
		mem.Policy.StopCurrentNeed = true
		mem.Policy.StopReason = rejection.ReasonCode
		out.Terminal = TerminalNeedStopped
		return out, nil
	}
	if len(plan.NextSeeds) == 0 {
		out.Terminal = TerminalAllCandidatesRejected
		return out, nil
	}

	got, err := exploreUnblocked(mem, exploreRequest{View: view, Opening: opening, Budget: budget}, plan.NextSeeds, exploreQuery(plan))
	if err != nil {
		return Result{}, err
	}
	out.Explore = got
	if got.Offer != nil {
		mem.recordOffer(view, got.Offer.SkillReference, nil)
		out.Offer = cloneOffer(got.Offer)
		return out, nil
	}
	if isBudgetTerminal(got.Terminal) {
		out.Terminal = got.Terminal
		return out, nil
	}
	out.Terminal = TerminalAllCandidatesRejected
	return out, nil
}

// NotifyResolutionError records a resolution/serve failure. It does not
// create a rejected disposition, Memory Delivery, or reason-count entry.
func NotifyResolutionError(mem *Memory, notice ResolutionNotice) error {
	if mem == nil {
		return ErrMemoryRequired
	}
	if notice.ReasonCode == "" {
		notice.ReasonCode = ReasonResolutionError
	}
	mem.ResolutionNotices = append(mem.ResolutionNotices, notice)
	return nil
}

func validateRejection(rejection RejectedDisposition) error {
	if rejection.OfferID == "" || rejection.Ref == "" || rejection.Reason == "" {
		return fmt.Errorf("%w: offer_id, ref, and reason are required", ErrInvalidRejection)
	}
	if !coreReasonSet[rejection.ReasonCode] {
		return fmt.Errorf("%w: %s", ErrUnknownReason, rejection.ReasonCode)
	}
	return nil
}

type redirectPlan struct {
	Action    string
	Stop      bool
	NextQuery string
	NextSeeds []string
	Guards    []Guard
	Lineages  []string
	Revisions []string
	Sources   []string
	Blacklist []string
}

func planRedirect(mem *Memory, rejection RejectedDisposition, view *evaluationgraph.View, opening evaluationexplore.OpeningContext) (redirectPlan, error) {
	ref, err := parseRef(rejection.Ref)
	if err != nil {
		return redirectPlan{}, err
	}
	sourceKey := sourceSetKey(mergeSourceSet(view, rejection.Ref, rejection.SourceSet))
	plan := redirectPlan{}
	switch rejection.ReasonCode {
	case ReasonNotApplicable:
		plan.Action = ActionExploreSpecialization
		plan.Guards = []Guard{{Ref: rejection.Ref, ReasonCode: rejection.ReasonCode, Constraint: GuardRequireSpecialization}}
		plan.Revisions = []string{rejection.Ref}
		plan.NextSeeds = specializedDescendants(view, ref)
		plan.NextQuery = "specializes:" + rejection.Ref
	case ReasonAlreadyKnown:
		plan.Action = ActionExcludeInsightLineage
		plan.Lineages = []string{ref.LineageID}
		plan.Revisions = []string{rejection.Ref}
		plan.NextSeeds = otherLineageNeighbors(view, ref, mem)
		plan.NextQuery = opening.Query
	case ReasonAlreadyResolved:
		plan.Action = ActionStopCurrentNeed
		plan.Stop = true
		plan.Revisions = []string{rejection.Ref}
	case ReasonTooGeneric:
		plan.Action = ActionExploreGuardedDescendant
		plan.Guards = []Guard{{Ref: rejection.Ref, ReasonCode: rejection.ReasonCode, Constraint: GuardRequireGuardedDescendant}}
		plan.Revisions = []string{rejection.Ref}
		plan.NextSeeds = specializedDescendants(view, ref)
		plan.NextQuery = "specialized_descendant:" + rejection.Ref
	case ReasonInsufficientContext:
		plan.Action = ActionStopCurrentNeed
		plan.Stop = mem.TaskContextMonitoring == "" || mem.TaskContextMonitoring == TaskContextOpeningOnly
		plan.Revisions = []string{rejection.Ref}
		if !plan.Stop {
			plan.NextSeeds = otherLineageNeighbors(view, ref, mem)
			plan.NextQuery = opening.Query
		}
	case ReasonIncorrectAssumption:
		plan.Action = ActionExcludeAssumptionBranch
		plan.Lineages = []string{ref.LineageID}
		plan.Revisions = []string{rejection.Ref}
		plan.NextSeeds = otherLineageNeighbors(view, ref, mem)
		plan.NextQuery = "assumption_alternative:" + rejection.Ref
	case ReasonConflictsWithEvidence:
		plan.Action = ActionFindConflictAlternative
		plan.Revisions = []string{rejection.Ref}
		plan.NextSeeds = conflictAlternatives(view, ref, mem)
		plan.NextQuery = "conflict_alternative:" + rejection.Ref
	case ReasonDuplicateOffer:
		plan.Action = ActionExcludeLineageSourceSet
		plan.Lineages = []string{ref.LineageID}
		plan.Revisions = []string{rejection.Ref}
		if sourceKey != "" {
			plan.Sources = []string{sourceKey}
		}
		plan.NextSeeds = otherLineageNeighbors(view, ref, mem)
		plan.NextQuery = "dedupe_alternative:" + rejection.Ref
	case ReasonUnsafeOrHarmful:
		plan.Action = ActionBlacklistExactRevision
		plan.Blacklist = []string{rejection.Ref}
		plan.Revisions = []string{rejection.Ref}
		plan.NextSeeds = otherLineageNeighbors(view, ref, mem)
		plan.NextQuery = "safe_alternative:" + rejection.Ref
	default:
		return redirectPlan{}, fmt.Errorf("%w: %s", ErrUnknownReason, rejection.ReasonCode)
	}
	plan.NextSeeds = filterSeeds(mem, view, plan)
	return plan, nil
}

func applyPlan(policy *Policy, plan redirectPlan) {
	for _, guard := range plan.Guards {
		if !hasGuard(policy.Guards, guard) {
			policy.Guards = append(policy.Guards, guard)
		}
	}
	policy.ExcludedLineages = unionSorted(policy.ExcludedLineages, plan.Lineages)
	policy.ExcludedRevisions = unionSorted(policy.ExcludedRevisions, plan.Revisions)
	policy.ExcludedSourceSets = unionSorted(policy.ExcludedSourceSets, plan.Sources)
	policy.BlacklistedRevisions = unionSorted(policy.BlacklistedRevisions, plan.Blacklist)
}

func exploreQuery(plan redirectPlan) string {
	// Redirect seeds already name the next candidates. An empty Explore
	// query lets those nodes remain offerable even when their bodies do
	// not contain the original opening token.
	if len(plan.NextSeeds) > 0 {
		return ""
	}
	return plan.NextQuery
}

func exploreUnblocked(mem *Memory, req exploreRequest, seeds []string, query string) (evaluationexplore.Result, error) {
	try := func(seedList []string, q string) (evaluationexplore.Result, error) {
		opening := req.Opening
		if len(seedList) > 0 {
			opening.SeedRefs = append([]string{}, seedList...)
		}
		opening.Query = q
		return evaluationexplore.Explore(evaluationexplore.Request{
			View:                 req.View,
			Opening:              opening,
			Budget:               req.Budget,
			OffersAlreadyEmitted: len(mem.OfferedRefs),
		})
	}

	candidates := uniqueStrings(seeds)
	got, err := try(candidates, query)
	if err != nil {
		return evaluationexplore.Result{}, err
	}
	if got.Offer != nil && !mem.blocked(req.View, got.Offer.SkillReference) {
		return got, nil
	}
	if got.Offer == nil && isBudgetTerminal(got.Terminal) {
		return got, nil
	}

	ranked := uniqueStrings(append(append([]string{}, got.Ranking...), candidates...))
	seen := map[string]bool{}
	for _, ref := range ranked {
		if seen[ref] || mem.blocked(req.View, ref) {
			continue
		}
		seen[ref] = true
		got, err = try([]string{ref}, "")
		if err != nil {
			return evaluationexplore.Result{}, err
		}
		if got.Offer != nil && !mem.blocked(req.View, got.Offer.SkillReference) {
			return got, nil
		}
		if got.Offer == nil && isBudgetTerminal(got.Terminal) {
			return got, nil
		}
	}
	got.Offer = nil
	got.SelectedRef = ""
	got.Choice = ""
	if got.Terminal == "" || !isBudgetTerminal(got.Terminal) {
		got.Terminal = TerminalAllCandidatesRejected
	}
	return got, nil
}

func (m *Memory) blocked(view *evaluationgraph.View, ref string) bool {
	if contains(m.OfferedRefs, ref) || contains(m.Policy.ExcludedRevisions, ref) || contains(m.Policy.BlacklistedRevisions, ref) {
		return true
	}
	parsed, err := parseRef(ref)
	if err == nil && contains(m.Policy.ExcludedLineages, parsed.LineageID) {
		return true
	}
	src := sourceSetKey(sourceSetOf(view, ref))
	if src != "" && (contains(m.Policy.ExcludedSourceSets, src) || contains(m.OfferedSourceSets, src)) {
		return true
	}
	return false
}

func (m *Memory) recordOffer(view *evaluationgraph.View, ref string, explicit []string) {
	if ref == "" {
		return
	}
	if !contains(m.OfferedRefs, ref) {
		m.OfferedRefs = append(m.OfferedRefs, ref)
	}
	src := mergeSourceSet(view, ref, explicit)
	key := sourceSetKey(src)
	if key != "" && !contains(m.OfferedSourceSets, key) {
		m.OfferedSourceSets = append(m.OfferedSourceSets, key)
	}
}

func (m *Memory) deliveryForOffer(offerID string) *MemoryDelivery {
	for i := range m.Deliveries {
		if m.Deliveries[i].OfferID == offerID {
			return &m.Deliveries[i]
		}
	}
	return nil
}

func specializedDescendants(view *evaluationgraph.View, ref evaluationgraph.RevisionRef) []string {
	seen := map[string]bool{}
	var out []string
	for _, edge := range view.Edges {
		var next evaluationgraph.RevisionRef
		switch {
		case edge.Kind == evaluationexplore.RelSpecializes && edge.From == ref:
			next = edge.To
		case edge.Kind == evaluationexplore.RelGeneralizes && edge.To == ref:
			next = edge.From
		default:
			continue
		}
		uri := next.String()
		if seen[uri] {
			continue
		}
		seen[uri] = true
		out = append(out, uri)
	}
	sort.Strings(out)
	return out
}

func otherLineageNeighbors(view *evaluationgraph.View, ref evaluationgraph.RevisionRef, mem *Memory) []string {
	seen := map[string]bool{}
	var out []string
	add := func(next evaluationgraph.RevisionRef) {
		uri := next.String()
		if next == ref || seen[uri] || mem.blocked(view, uri) {
			return
		}
		seen[uri] = true
		out = append(out, uri)
	}
	for _, edge := range view.Edges {
		if !redirectRelation(edge.Kind) {
			continue
		}
		if edge.From == ref {
			add(edge.To)
		}
		if edge.To == ref {
			add(edge.From)
		}
	}
	sort.Strings(out)
	return out
}

func conflictAlternatives(view *evaluationgraph.View, ref evaluationgraph.RevisionRef, mem *Memory) []string {
	conflicts := map[evaluationgraph.RevisionRef]bool{}
	for _, edge := range view.Edges {
		if edge.Kind != evaluationexplore.RelConditionallyConflictsWith {
			continue
		}
		if edge.From == ref {
			conflicts[edge.To] = true
		}
		if edge.To == ref {
			conflicts[edge.From] = true
		}
	}
	supported := map[evaluationgraph.RevisionRef]bool{}
	for _, edge := range view.Edges {
		if edge.Kind != evaluationexplore.RelSupports && edge.Kind != evaluationexplore.RelDerivedFromStep {
			continue
		}
		supported[edge.From] = true
		supported[edge.To] = true
	}
	seen := map[string]bool{}
	var preferred, fallback []string
	add := func(next evaluationgraph.RevisionRef) {
		uri := next.String()
		if next == ref || conflicts[next] || seen[uri] || mem.blocked(view, uri) {
			return
		}
		seen[uri] = true
		if supported[next] {
			preferred = append(preferred, uri)
			return
		}
		fallback = append(fallback, uri)
	}
	for _, edge := range view.Edges {
		if !redirectRelation(edge.Kind) {
			continue
		}
		if edge.From == ref {
			add(edge.To)
		}
		if edge.To == ref {
			add(edge.From)
		}
	}
	sort.Strings(preferred)
	sort.Strings(fallback)
	return append(preferred, fallback...)
}

func redirectRelation(kind string) bool {
	switch kind {
	case evaluationexplore.RelSpecializes, evaluationexplore.RelGeneralizes,
		evaluationexplore.RelRelatedTo, evaluationexplore.RelAdaptedFrom,
		evaluationexplore.RelCoUsedWith, evaluationexplore.RelSupersedes,
		evaluationexplore.RelRetiredBy:
		return true
	}
	return false
}

func filterSeeds(mem *Memory, view *evaluationgraph.View, plan redirectPlan) []string {
	excluded := map[string]bool{}
	for _, lineage := range append(append([]string{}, mem.Policy.ExcludedLineages...), plan.Lineages...) {
		excluded[lineage] = true
	}
	blockedRef := map[string]bool{}
	for _, ref := range append(append(append([]string{}, mem.OfferedRefs...), plan.Revisions...), plan.Blacklist...) {
		blockedRef[ref] = true
	}
	for _, ref := range mem.Policy.ExcludedRevisions {
		blockedRef[ref] = true
	}
	for _, ref := range mem.Policy.BlacklistedRevisions {
		blockedRef[ref] = true
	}
	var out []string
	for _, seed := range plan.NextSeeds {
		if blockedRef[seed] {
			continue
		}
		parsed, err := parseRef(seed)
		if err != nil || excluded[parsed.LineageID] {
			continue
		}
		if mem.blocked(view, seed) {
			continue
		}
		src := sourceSetKey(sourceSetOf(view, seed))
		if src != "" && (contains(mem.Policy.ExcludedSourceSets, src) || contains(plan.Sources, src) || contains(mem.OfferedSourceSets, src)) {
			continue
		}
		out = append(out, seed)
	}
	return uniqueStrings(out)
}

func sourceSetOf(view *evaluationgraph.View, ref string) []string {
	parsed, err := parseRef(ref)
	if err != nil || view == nil {
		return nil
	}
	seen := map[string]bool{}
	var ids []string
	for _, edge := range view.Edges {
		if edge.Kind != evaluationexplore.RelDerivedFromStep && edge.Kind != evaluationexplore.RelSupports {
			continue
		}
		var peer evaluationgraph.RevisionRef
		switch {
		case edge.From == parsed:
			peer = edge.To
		case edge.To == parsed:
			peer = edge.From
		default:
			continue
		}
		id := peer.String()
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func mergeSourceSet(view *evaluationgraph.View, ref string, explicit []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range append(sourceSetOf(view, ref), explicit...) {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func sourceSetKey(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	return strings.Join(ids, ",")
}

func parseRef(raw string) (evaluationgraph.RevisionRef, error) {
	return evaluationexplore.ParseSkillReference(raw, evaluationgraph.ScopeEvaluation)
}

func isBudgetTerminal(terminal string) bool {
	switch terminal {
	case evaluationexplore.TerminalGraphBudgetExhausted,
		evaluationexplore.TerminalOfferBudgetExhausted,
		evaluationexplore.TerminalMemoryTimeout:
		return true
	}
	return false
}

func terminalIfStopped(mem *Memory) string {
	if mem.Policy.StopCurrentNeed {
		return TerminalNeedStopped
	}
	return ""
}

func lastAction(hops []AuditHop) string {
	if len(hops) == 0 {
		return ""
	}
	return hops[len(hops)-1].Action
}

func hasGuard(guards []Guard, want Guard) bool {
	for _, guard := range guards {
		if guard == want {
			return true
		}
	}
	return false
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func unionSorted(base, extra []string) []string {
	return uniqueStrings(append(append([]string{}, base...), extra...))
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range in {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func cloneOffer(offer *evaluationexplore.PendingOffer) *evaluationexplore.PendingOffer {
	if offer == nil {
		return nil
	}
	copy := *offer
	return &copy
}

func cloneDelivery(delivery *MemoryDelivery) *MemoryDelivery {
	if delivery == nil {
		return nil
	}
	copy := *delivery
	return &copy
}

func cloneHops(hops []AuditHop) []AuditHop {
	if hops == nil {
		return nil
	}
	out := make([]AuditHop, len(hops))
	copy(out, hops)
	for i := range out {
		out[i].NextSeeds = append([]string{}, hops[i].NextSeeds...)
	}
	return out
}
