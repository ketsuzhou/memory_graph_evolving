// Package evaluationexplore is the Memory Explore Agent read path over a
// pinned, non-authoritative evaluation graph. It starts from opening seeds,
// walks only contract-allowed relations inside fixed graph/turn/offer
// budgets, and may emit a pending original-skill offer. It never writes a
// ledger, never reads held-out feedback, and never treats the graph as
// activation authority.
package evaluationexplore

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
)

const (
	ChoiceServeOriginal = "serve_original"

	DefaultMaxGraphStepsPerEpisode        = 12
	DefaultMaxOffersPerAgentPerCheckpoint = 3
	DefaultMaxMemoryTurnsPerEpisode       = 6

	TerminalGraphBudgetExhausted      = "graph_budget_exhausted"
	TerminalOfferBudgetExhausted      = "offer_budget_exhausted"
	TerminalMemoryTimeout             = "memory_timeout"
	TerminalNoApplicableCandidate     = "no_applicable_candidate"
	TerminalNoCandidateFromEmptyGraph = "no_candidate_from_empty_graph"
)

const (
	RelSpecializes                = "specializes"
	RelGeneralizes                = "generalizes"
	RelRelatedTo                  = "related_to"
	RelDerivedFromStep            = "derived_from_step"
	RelSupports                   = "supports"
	RelAdaptedFrom                = "adapted_from"
	RelCoUsedWith                 = "co_used_with"
	RelConditionallyConflictsWith = "conditionally_conflicts_with"
	RelAcceptedIn                 = "accepted_in"
	RelRejectedIn                 = "rejected_in"
	RelSupersedes                 = "supersedes"
	RelRetiredBy                  = "retired_by"
)

var (
	ErrDisallowedExpansion = errors.New("evaluation explore: disallowed room or evidence expansion")
	ErrLatestAlias         = errors.New("evaluation explore: latest alias is not an exact skill reference")
	ErrLocalPathRef        = errors.New("evaluation explore: local path is not an exact skill reference")
	ErrCrossScopeRef       = errors.New("evaluation explore: cross-scope skill reference")
	ErrInvalidBudget       = errors.New("evaluation explore: invalid budget")
	ErrInvalidOpening      = errors.New("evaluation explore: invalid opening context")
)

var allowedRelations = map[string]bool{
	RelSpecializes:                true,
	RelGeneralizes:                true,
	RelRelatedTo:                  true,
	RelDerivedFromStep:            true,
	RelSupports:                   true,
	RelAdaptedFrom:                true,
	RelCoUsedWith:                 true,
	RelConditionallyConflictsWith: true,
	RelAcceptedIn:                 true,
	RelRejectedIn:                 true,
	RelSupersedes:                 true,
	RelRetiredBy:                  true,
}

var inverseRelation = map[string]string{
	RelSpecializes:                RelGeneralizes,
	RelGeneralizes:                RelSpecializes,
	RelSupersedes:                 RelRetiredBy,
	RelRetiredBy:                  RelSupersedes,
	RelRelatedTo:                  RelRelatedTo,
	RelCoUsedWith:                 RelCoUsedWith,
	RelConditionallyConflictsWith: RelConditionallyConflictsWith,
	RelAcceptedIn:                 RelAcceptedIn,
	RelRejectedIn:                 RelRejectedIn,
}

const (
	baseScore            = 1000
	distancePenalty      = 10
	specializesBonus     = 40
	generalizesBonus     = 10
	relatedBonus         = 15
	derivedSupportsBonus = 20
	adaptedFromBonus     = 20
	coUsedBonus          = 5
	acceptedInBonus      = 50
	rejectedInPenalty    = 50
	conflictPenalty      = 80
	successorBonus       = 30
)

// Budget is the per-episode Memory Explore cap frozen by the contract.
type Budget struct {
	MaxGraphStepsPerEpisode        int
	MaxOffersPerAgentPerCheckpoint int
	MaxMemoryTurnsPerEpisode       int
}

func DefaultBudget() Budget {
	return Budget{
		MaxGraphStepsPerEpisode:        DefaultMaxGraphStepsPerEpisode,
		MaxOffersPerAgentPerCheckpoint: DefaultMaxOffersPerAgentPerCheckpoint,
		MaxMemoryTurnsPerEpisode:       DefaultMaxMemoryTurnsPerEpisode,
	}
}

// OpeningContext is the only task input Memory Explore may read: opening
// seeds/query plus the directed offer target. Intermediate tool state is out
// of scope.
type OpeningContext struct {
	SeedRefs      []string
	Query         string
	CheckpointID  string
	TargetAgentID string
}

// HeldOutFeedback is accepted so callers can pass a test-time trace. Explore
// must not read it; changing it cannot change seeds, ranking, or the result.
type HeldOutFeedback struct {
	OfferID     string
	Ref         string
	Disposition string
	Reason      string
}

type Request struct {
	View                 *evaluationgraph.View
	Scope                evaluationgraph.Scope
	Opening              OpeningContext
	Budget               Budget
	OffersAlreadyEmitted int
	HeldOut              []HeldOutFeedback
}

type PendingOffer struct {
	SkillReference string
	CheckpointID   string
	TargetAgentID  string
	Choice         string
}

type AuditEvent struct {
	Kind   string
	Turn   int
	Step   int
	From   string
	To     string
	Edge   string
	Ref    string
	Choice string
	Detail string
}

type Result struct {
	Seeds         []string
	NeighborOrder []string
	SelectedRef   string
	Choice        string
	Offer         *PendingOffer
	AuditTrace    []AuditEvent
	Ranking       []string
	Terminal      string
	GraphSteps    int
	MemoryTurns   int
}

type hop struct {
	to     evaluationgraph.RevisionRef
	kind   string
	source string
}

type ranked struct {
	ref   evaluationgraph.RevisionRef
	score int
}

// Explore walks the pinned evaluation view from opening seeds, ranks visited
// candidates by the frozen policy, and either emits one serve_original pending
// offer or a budget/no-skill terminal. Held-out feedback is ignored.
func Explore(req Request) (Result, error) {
	if req.View == nil {
		return Result{}, fmt.Errorf("%w: view is required", ErrInvalidOpening)
	}
	scope := req.Scope
	if scope == "" {
		scope = evaluationgraph.ScopeEvaluation
	}
	if scope != evaluationgraph.ScopeEvaluation {
		return Result{}, fmt.Errorf("%w: %s", evaluationgraph.ErrScopeDenied, scope)
	}
	if req.Opening.CheckpointID == "" || req.Opening.TargetAgentID == "" {
		return Result{}, fmt.Errorf("%w: checkpoint and target are required", ErrInvalidOpening)
	}
	budget, err := normalizeBudget(req.Budget)
	if err != nil {
		return Result{}, err
	}

	seeds, err := resolveSeeds(req.View, scope, req.Opening)
	if err != nil {
		return Result{}, err
	}
	if len(req.View.Nodes) == 0 {
		return Result{
			Seeds:      seedStrings(seeds),
			AuditTrace: seedEvents(seeds),
			Terminal:   TerminalNoCandidateFromEmptyGraph,
		}, nil
	}
	if len(seeds) == 0 {
		return Result{
			AuditTrace: []AuditEvent{{Kind: "terminal", Detail: TerminalNoApplicableCandidate}},
			Terminal:   TerminalNoApplicableCandidate,
		}, nil
	}

	adjacent, err := buildAdjacency(req.View)
	if err != nil {
		return Result{}, err
	}

	result := Result{Seeds: seedStrings(seeds), AuditTrace: seedEvents(seeds)}
	seen := make(map[evaluationgraph.RevisionRef]bool, len(seeds))
	dist := make(map[evaluationgraph.RevisionRef]int, len(seeds))
	queue := make([]evaluationgraph.RevisionRef, 0, len(seeds))
	for _, seed := range seeds {
		seen[seed] = true
		dist[seed] = 0
		queue = append(queue, seed)
	}

	stoppedGraph := false
	stoppedTurns := false
	for len(queue) > 0 {
		if result.MemoryTurns >= budget.MaxMemoryTurnsPerEpisode {
			stoppedTurns = true
			break
		}
		result.MemoryTurns++
		current := queue[0]
		queue = queue[1:]
		for _, next := range adjacent[current] {
			if isDisallowedExpansion(next.kind) {
				return Result{}, fmt.Errorf("%w: %s", ErrDisallowedExpansion, next.kind)
			}
			if !allowedRelations[next.kind] {
				continue
			}
			if result.GraphSteps >= budget.MaxGraphStepsPerEpisode {
				stoppedGraph = true
				break
			}
			result.GraphSteps++
			result.AuditTrace = append(result.AuditTrace, AuditEvent{
				Kind: "expand",
				Turn: result.MemoryTurns,
				Step: result.GraphSteps,
				From: current.String(),
				To:   next.to.String(),
				Edge: next.kind,
			})
			if seen[next.to] {
				continue
			}
			seen[next.to] = true
			dist[next.to] = dist[current] + 1
			queue = append(queue, next.to)
			result.NeighborOrder = append(result.NeighborOrder, next.to.String())
		}
		if stoppedGraph {
			break
		}
	}

	ranking := rankVisited(req.View, seen, dist, seeds)
	result.Ranking = make([]string, 0, len(ranking))
	for _, item := range ranking {
		result.Ranking = append(result.Ranking, item.ref.String())
	}
	result.AuditTrace = append(result.AuditTrace, AuditEvent{
		Kind:   "rank",
		Detail: strings.Join(result.Ranking, ","),
	})

	selected, ok := firstEligible(req.View, ranking, req.Opening.Query)
	if !ok {
		result.Terminal = noCandidateTerminal(len(req.View.Nodes) == 0, stoppedGraph, stoppedTurns)
		result.AuditTrace = append(result.AuditTrace, AuditEvent{Kind: "terminal", Detail: result.Terminal})
		return result, nil
	}
	result.SelectedRef = selected.String()
	result.Choice = ChoiceServeOriginal
	result.AuditTrace = append(result.AuditTrace, AuditEvent{
		Kind:   "select",
		Ref:    result.SelectedRef,
		Choice: ChoiceServeOriginal,
	})
	if req.OffersAlreadyEmitted >= budget.MaxOffersPerAgentPerCheckpoint {
		result.Terminal = TerminalOfferBudgetExhausted
		result.AuditTrace = append(result.AuditTrace, AuditEvent{Kind: "terminal", Detail: result.Terminal, Ref: result.SelectedRef})
		return result, nil
	}
	result.Offer = &PendingOffer{
		SkillReference: result.SelectedRef,
		CheckpointID:   req.Opening.CheckpointID,
		TargetAgentID:  req.Opening.TargetAgentID,
		Choice:         ChoiceServeOriginal,
	}
	result.AuditTrace = append(result.AuditTrace, AuditEvent{
		Kind:   "offer",
		Ref:    result.SelectedRef,
		Choice: ChoiceServeOriginal,
		Detail: fmt.Sprintf("checkpoint=%s target=%s", req.Opening.CheckpointID, req.Opening.TargetAgentID),
	})
	return result, nil
}

func normalizeBudget(budget Budget) (Budget, error) {
	out := budget
	if out.MaxGraphStepsPerEpisode == 0 {
		out.MaxGraphStepsPerEpisode = DefaultMaxGraphStepsPerEpisode
	}
	if out.MaxOffersPerAgentPerCheckpoint == 0 {
		out.MaxOffersPerAgentPerCheckpoint = DefaultMaxOffersPerAgentPerCheckpoint
	}
	if out.MaxMemoryTurnsPerEpisode == 0 {
		out.MaxMemoryTurnsPerEpisode = DefaultMaxMemoryTurnsPerEpisode
	}
	if out.MaxGraphStepsPerEpisode < 0 || out.MaxOffersPerAgentPerCheckpoint < 0 || out.MaxMemoryTurnsPerEpisode < 0 {
		return Budget{}, ErrInvalidBudget
	}
	return out, nil
}

func resolveSeeds(view *evaluationgraph.View, scope evaluationgraph.Scope, opening OpeningContext) ([]evaluationgraph.RevisionRef, error) {
	if len(opening.SeedRefs) > 0 {
		seeds := make([]evaluationgraph.RevisionRef, 0, len(opening.SeedRefs))
		seen := map[evaluationgraph.RevisionRef]bool{}
		for _, raw := range opening.SeedRefs {
			ref, err := ParseSkillReference(raw, scope)
			if err != nil {
				return nil, err
			}
			if _, err := view.Get(scope, ref); err != nil {
				if errors.Is(err, evaluationgraph.ErrRevisionNotFound) && len(view.Nodes) == 0 {
					continue
				}
				return nil, err
			}
			if seen[ref] {
				continue
			}
			seen[ref] = true
			seeds = append(seeds, ref)
		}
		return seeds, nil
	}
	if opening.Query == "" {
		return nil, nil
	}
	var seeds []evaluationgraph.RevisionRef
	for _, node := range view.Nodes {
		if strings.Contains(node.Body, opening.Query) || strings.Contains(node.Ref.LineageID, opening.Query) {
			seeds = append(seeds, node.Ref)
		}
	}
	sort.Slice(seeds, func(i, j int) bool { return seeds[i].String() < seeds[j].String() })
	return seeds, nil
}

// ParseSkillReference accepts only a manifest-pinned exact URI. Latest aliases,
// local paths, and cross-scope refs fail closed.
func ParseSkillReference(raw string, scope evaluationgraph.Scope) (evaluationgraph.RevisionRef, error) {
	trimmed := strings.TrimSpace(raw)
	if looksLikeLocalPath(trimmed) {
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrLocalPathRef, raw)
	}
	if scope != "" && scope != evaluationgraph.ScopeEvaluation {
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", evaluationgraph.ErrScopeDenied, scope)
	}
	if strings.Contains(trimmed, "@latest") || isUnpinnedSkillURI(trimmed) {
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrLatestAlias, raw)
	}
	namespace, ref, err := evaluationgraph.ParseRevisionURI(trimmed)
	if err != nil {
		if looksLikeLocalPath(trimmed) {
			return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrLocalPathRef, raw)
		}
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrLatestAlias, raw)
	}
	if namespace != string(evaluationgraph.ScopeEvaluation) && namespace != evaluationgraph.StreamEvaluation {
		return evaluationgraph.RevisionRef{}, fmt.Errorf("%w: %s", ErrCrossScopeRef, raw)
	}
	return ref, nil
}

func looksLikeLocalPath(raw string) bool {
	if strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "./") || strings.HasPrefix(raw, "../") {
		return true
	}
	if strings.HasPrefix(raw, "file:") || strings.HasPrefix(raw, "file://") {
		return true
	}
	return strings.Contains(raw, "\\")
}

func isUnpinnedSkillURI(raw string) bool {
	if !strings.HasPrefix(raw, "skill://") {
		return false
	}
	return !strings.Contains(raw, "@")
}

func buildAdjacency(view *evaluationgraph.View) (map[evaluationgraph.RevisionRef][]hop, error) {
	adjacent := make(map[evaluationgraph.RevisionRef][]hop)
	seen := map[string]bool{}
	add := func(from, to evaluationgraph.RevisionRef, kind, source string) {
		key := from.String() + "\x00" + to.String() + "\x00" + kind + "\x00" + source
		if seen[key] {
			return
		}
		seen[key] = true
		adjacent[from] = append(adjacent[from], hop{to: to, kind: kind, source: source})
	}
	for _, edge := range view.Edges {
		if isDisallowedExpansion(edge.Kind) {
			add(edge.From, edge.To, edge.Kind, edge.SourceID)
			continue
		}
		if !allowedRelations[edge.Kind] {
			continue
		}
		add(edge.From, edge.To, edge.Kind, edge.SourceID)
		if inverse, ok := inverseRelation[edge.Kind]; ok {
			add(edge.To, edge.From, inverse, edge.SourceID)
		}
	}
	for ref := range adjacent {
		sort.Slice(adjacent[ref], func(i, j int) bool {
			left, right := adjacent[ref][i], adjacent[ref][j]
			if left.to.String() != right.to.String() {
				return left.to.String() < right.to.String()
			}
			if left.kind != right.kind {
				return left.kind < right.kind
			}
			return left.source < right.source
		})
	}
	return adjacent, nil
}

func isDisallowedExpansion(kind string) bool {
	switch kind {
	case "in_room", "room_contains", "mentioned_in", "offered_at", "room_message",
		"has_evidence", "evidence_of", "evidence_in", "located_in_room", "attached_evidence":
		return true
	}
	if strings.HasPrefix(kind, "room_") || strings.HasPrefix(kind, "evidence_") {
		return true
	}
	return false
}

func rankVisited(
	view *evaluationgraph.View,
	seen map[evaluationgraph.RevisionRef]bool,
	dist map[evaluationgraph.RevisionRef]int,
	seeds []evaluationgraph.RevisionRef,
) []ranked {
	seedSet := map[evaluationgraph.RevisionRef]bool{}
	for _, seed := range seeds {
		seedSet[seed] = true
	}
	out := make([]ranked, 0, len(seen))
	for _, node := range view.Nodes {
		if !seen[node.Ref] {
			continue
		}
		out = append(out, ranked{ref: node.Ref, score: scoreNode(view, node.Ref, dist[node.Ref], seedSet)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		return out[i].ref.String() < out[j].ref.String()
	})
	return out
}

func scoreNode(view *evaluationgraph.View, ref evaluationgraph.RevisionRef, distance int, seeds map[evaluationgraph.RevisionRef]bool) int {
	score := baseScore - distance*distancePenalty
	flags := map[string]bool{}
	for _, edge := range view.Edges {
		switch edge.Kind {
		case RelSpecializes:
			if edge.To == ref {
				flags["specializes"] = true
			}
			if edge.From == ref {
				flags["generalizes"] = true
			}
		case RelGeneralizes:
			if edge.To == ref {
				flags["generalizes"] = true
			}
			if edge.From == ref {
				flags["specializes"] = true
			}
		case RelRelatedTo:
			if edge.From == ref || edge.To == ref {
				flags["related"] = true
			}
		case RelDerivedFromStep, RelSupports:
			if edge.From == ref || edge.To == ref {
				flags["derived"] = true
			}
		case RelAdaptedFrom:
			if edge.From == ref || edge.To == ref {
				flags["adapted"] = true
			}
		case RelCoUsedWith:
			if edge.From == ref || edge.To == ref {
				flags["coused"] = true
			}
		case RelAcceptedIn:
			if edge.From == ref {
				flags["accepted"] = true
			}
		case RelRejectedIn:
			if edge.From == ref {
				flags["rejected"] = true
			}
		case RelSupersedes:
			if edge.From == ref {
				flags["successor"] = true
			}
		case RelRetiredBy:
			if edge.To == ref {
				flags["successor"] = true
			}
		}
	}
	if flags["specializes"] {
		score += specializesBonus
	}
	if flags["generalizes"] {
		score += generalizesBonus
	}
	if flags["related"] {
		score += relatedBonus
	}
	if flags["derived"] {
		score += derivedSupportsBonus
	}
	if flags["adapted"] {
		score += adaptedFromBonus
	}
	if flags["coused"] {
		score += coUsedBonus
	}
	if flags["accepted"] {
		score += acceptedInBonus
	}
	if flags["rejected"] {
		score -= rejectedInPenalty
	}
	if flags["successor"] {
		score += successorBonus
	}
	if conflictsWithSeed(view, ref, seeds) {
		score -= conflictPenalty
	}
	return score
}

func conflictsWithSeed(view *evaluationgraph.View, ref evaluationgraph.RevisionRef, seeds map[evaluationgraph.RevisionRef]bool) bool {
	for _, edge := range view.Edges {
		if edge.Kind != RelConditionallyConflictsWith {
			continue
		}
		if edge.From == ref && seeds[edge.To] || edge.To == ref && seeds[edge.From] {
			return true
		}
	}
	return false
}

func retired(view *evaluationgraph.View, ref evaluationgraph.RevisionRef) bool {
	for _, edge := range view.Edges {
		if edge.Kind == RelRetiredBy && edge.From == ref {
			return true
		}
		if edge.Kind == RelSupersedes && edge.To == ref {
			return true
		}
	}
	return false
}

func firstEligible(view *evaluationgraph.View, ranking []ranked, query string) (evaluationgraph.RevisionRef, bool) {
	bodies := map[evaluationgraph.RevisionRef]string{}
	for _, node := range view.Nodes {
		bodies[node.Ref] = node.Body
	}
	for _, item := range ranking {
		if !offerable(view, item.ref, bodies[item.ref], query) {
			continue
		}
		return item.ref, true
	}
	return evaluationgraph.RevisionRef{}, false
}

func offerable(view *evaluationgraph.View, ref evaluationgraph.RevisionRef, body, query string) bool {
	if retired(view, ref) {
		return false
	}
	if query != "" && !strings.Contains(body, query) {
		return false
	}
	return true
}

func noCandidateTerminal(emptyGraph, stoppedGraph, stoppedTurns bool) string {
	if emptyGraph {
		return TerminalNoCandidateFromEmptyGraph
	}
	if stoppedGraph {
		return TerminalGraphBudgetExhausted
	}
	if stoppedTurns {
		return TerminalMemoryTimeout
	}
	return TerminalNoApplicableCandidate
}

func seedStrings(seeds []evaluationgraph.RevisionRef) []string {
	out := make([]string, len(seeds))
	for i, seed := range seeds {
		out[i] = seed.String()
	}
	return out
}

func seedEvents(seeds []evaluationgraph.RevisionRef) []AuditEvent {
	out := make([]AuditEvent, 0, len(seeds))
	for _, seed := range seeds {
		out = append(out, AuditEvent{Kind: "seed", Ref: seed.String()})
	}
	return out
}
