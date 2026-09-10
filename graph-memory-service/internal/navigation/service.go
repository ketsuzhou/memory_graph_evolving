// Package navigation orchestrates bounded automatic traversal over an existing
// Exploration session. Selectors only choose among server-generated actions;
// every action still runs through Exploration's authorization and fences.
package navigation

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/ports"
)

type Mode string

const (
	ModeOff      Mode = "off"
	ModeOptional Mode = "optional"
	ModeRequired Mode = "required"
)

func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case ModeOff, ModeOptional, ModeRequired:
		return Mode(value), nil
	default:
		return "", fmt.Errorf("path selector mode must be off, optional, or required")
	}
}

type Config struct {
	Mode                Mode
	MaxModelCalls       int
	MaxDuration         time.Duration
	MaxTotalTokens      int
	MaxCandidates       int
	MaxCandidateBytes   int
	ExploreLimit        int
	MaxRecoveryDuration time.Duration
	Checkpoint          func() error
}

type Request struct {
	RunID         string
	MaxModelCalls int
}

type Step struct {
	Index         int               `json:"index"`
	Intent        ports.IntentKind  `json:"intent"`
	ActionID      string            `json:"action_id"`
	OperationID   string            `json:"operation_id"`
	AppliedPolicy string            `json:"applied_policy"`
	Degradation   string            `json:"degradation,omitempty"`
	ModelPolicy   ports.ModelPolicy `json:"model_policy"`
	Usage         ports.ModelUsage  `json:"usage"`
}

type Result struct {
	State         string                          `json:"state"`
	Found         bool                            `json:"found"`
	Summary       string                          `json:"summary"`
	Citations     []exploration.SubmittedCitation `json:"citations"`
	Steps         []Step                          `json:"steps"`
	AppliedPolicy string                          `json:"applied_policy"`
	Degradation   []string                        `json:"degradation"`
	Duplicate     bool                            `json:"duplicate"`
}

type Service struct {
	selector    ports.PathSelector
	store       ports.NavigationStore
	exploration *exploration.Service
	config      Config
}

func New(selector ports.PathSelector, store ports.NavigationStore, explorationService *exploration.Service, config Config) (*Service, error) {
	if store == nil || explorationService == nil {
		return nil, fmt.Errorf("navigation: store and exploration service are required")
	}
	if config.Mode != ModeOff && config.Mode != ModeOptional && config.Mode != ModeRequired {
		return nil, fmt.Errorf("navigation: invalid selector mode")
	}
	if config.MaxModelCalls < 1 || config.MaxModelCalls > 16 {
		return nil, fmt.Errorf("navigation: max model calls must be 1-16")
	}
	if config.MaxDuration <= 0 || config.MaxDuration > time.Minute {
		return nil, fmt.Errorf("navigation: max duration must be within one minute")
	}
	if config.MaxTotalTokens < 1 || config.MaxTotalTokens > 100000 {
		return nil, fmt.Errorf("navigation: max total tokens must be 1-100000")
	}
	if config.MaxCandidates < 1 || config.MaxCandidates > 32 || config.MaxCandidateBytes < 1 || config.MaxCandidateBytes > 4096 {
		return nil, fmt.Errorf("navigation: invalid candidate bounds")
	}
	if config.ExploreLimit < 1 || config.ExploreLimit > 100 {
		return nil, fmt.Errorf("navigation: explore limit must be 1-100")
	}
	if config.MaxRecoveryDuration <= 0 || config.MaxRecoveryDuration > 10*time.Second {
		return nil, fmt.Errorf("navigation: recovery duration must be within 10 seconds")
	}
	return &Service{selector: selector, store: store, exploration: explorationService, config: config}, nil
}

func (s *Service) Navigate(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, request Request) (Result, error) {
	if s.config.Mode == ModeOff {
		return Result{}, unavailable("path selection is disabled")
	}
	// Authorize before reserving run_id so a caller cannot poison another
	// principal's idempotency key with an inaccessible session.
	if _, err := s.exploration.AuthorizeNavigation(ctx, tenantID, principalID, sessionID); err != nil {
		return Result{}, err
	}
	maxModelCalls := request.MaxModelCalls
	if maxModelCalls > s.config.MaxModelCalls {
		maxModelCalls = s.config.MaxModelCalls
	}
	startedAt := time.Now().UTC()
	run, duplicate, err := s.store.StartNavigationRun(ctx, sessionID, request.RunID, maxModelCalls, startedAt, startedAt.Add(s.config.MaxDuration))
	if err != nil {
		return Result{}, err
	}
	if err := s.checkpoint(); err != nil {
		return Result{}, err
	}
	if run.State == "completed" {
		result, err := decodeResult(run.ResultJSON)
		if err != nil {
			return Result{}, err
		}
		result.Duplicate = true
		return result, nil
	}
	deadline := run.Deadline
	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		run, _, err = s.store.NavigationRun(ctx, sessionID, request.RunID)
		if err != nil {
			return Result{}, err
		}
		if result, found, err := terminalResult(run); err != nil {
			return Result{}, err
		} else if found {
			result.Duplicate = duplicate
			return s.finish(ctx, sessionID, request.RunID, result)
		}

		step, hasIncomplete := incompleteStep(run)
		if !hasIncomplete && !time.Now().Before(deadline) {
			if s.config.Mode == ModeRequired {
				return Result{}, unavailable("required path selector exceeded the run deadline")
			}
			step = deadlineStopStep(run)
			stored, _, err := s.store.RecordNavigationIntent(ctx, sessionID, request.RunID, step)
			if err != nil {
				return Result{}, err
			}
			step, hasIncomplete = stored, true
			if err := s.checkpoint(); err != nil {
				return Result{}, err
			}
		}
		if !hasIncomplete {
			observation, actions, err := s.observe(ctx, tenantID, principalID, sessionID)
			if err != nil {
				return Result{}, err
			}
			step, err = s.selectStep(ctx, deadline, run, observation, actions)
			if err != nil {
				return Result{}, err
			}
			if step.AppliedPolicy == "required_unavailable" && !step.ModelCall {
				return Result{}, unavailable("required path selector is unavailable")
			}
			stored, _, err := s.store.RecordNavigationIntent(ctx, sessionID, request.RunID, step)
			if err != nil {
				return Result{}, err
			}
			step = stored
			if err := s.checkpoint(); err != nil {
				return Result{}, err
			}
		}
		if step.AppliedPolicy == "required_unavailable" {
			responseJSON, err := json.Marshal(stepOutcome{State: "active"})
			if err != nil {
				return Result{}, err
			}
			if err := s.store.CompleteNavigationStep(ctx, sessionID, request.RunID, step.Index, step.OperationID, responseJSON); err != nil {
				return Result{}, err
			}
			if err := s.checkpoint(); err != nil {
				return Result{}, err
			}
			return Result{}, unavailable("required path selector is unavailable")
		}

		if isDeadlineStopStep(step) {
			responseJSON, err := json.Marshal(stepOutcome{State: "stopped", Terminal: true})
			if err != nil {
				return Result{}, err
			}
			if err := s.store.CompleteNavigationStep(ctx, sessionID, request.RunID, step.Index, step.OperationID, responseJSON); err != nil {
				return Result{}, err
			}
			if err := s.checkpoint(); err != nil {
				return Result{}, err
			}
			continue
		}

		var outcome stepOutcome
		if time.Now().Before(deadline) {
			executionContext, cancelExecution := context.WithDeadline(ctx, deadline)
			outcome, err = s.execute(executionContext, tenantID, principalID, sessionID, step)
			cancelExecution()
			if err != nil && ctx.Err() == nil && !time.Now().Before(deadline) {
				outcome, err = s.recoverBounded(ctx, tenantID, principalID, sessionID, step)
			}
		} else {
			outcome, err = s.recoverBounded(ctx, tenantID, principalID, sessionID, step)
		}
		if err != nil {
			return Result{}, err
		}
		responseJSON, err := json.Marshal(outcome)
		if err != nil {
			return Result{}, err
		}
		if err := s.store.CompleteNavigationStep(ctx, sessionID, request.RunID, step.Index, step.OperationID, responseJSON); err != nil {
			return Result{}, err
		}
		if err := s.checkpoint(); err != nil {
			return Result{}, err
		}
	}
}

func (s *Service) observe(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID) (ports.NavigationObservation, map[string]ports.NavigationAction, error) {
	session, items, err := s.exploration.ObserveNavigation(ctx, tenantID, principalID, sessionID, s.config.MaxCandidates)
	if err != nil {
		return ports.NavigationObservation{}, nil, err
	}
	remainingSteps := session.Budget.MaxSteps - session.StepsUsed
	if remainingSteps < 0 {
		remainingSteps = 0
	}
	remainingResults := session.Budget.MaxResults - session.ResultsServed
	if remainingResults < 0 {
		remainingResults = 0
	}
	observation := ports.NavigationObservation{
		Query: session.Query, RemainingSteps: remainingSteps, RemainingResults: remainingResults,
		Candidates: make([]ports.NavigationCandidate, 0, len(items)),
	}
	for _, item := range items {
		candidate := ports.NavigationCandidate{CitationID: item.Citation.ID, Content: truncateUTF8(item.Content, s.config.MaxCandidateBytes)}
		if item.Traversal != nil {
			candidate.Traversal = &ports.NavigationTraversalCandidate{
				RouteKind: truncateUTF8(item.Traversal.RouteKind, 64), Relation: truncateUTF8(item.Traversal.Relation, 128),
				Direction: truncateUTF8(item.Traversal.Direction, 32),
			}
		}
		observation.Candidates = append(observation.Candidates, candidate)
	}
	actions := make(map[string]ports.NavigationAction)
	addAction := func(action ports.NavigationAction) {
		actions[action.ActionID] = action
		observation.Actions = append(observation.Actions, action)
	}
	if remainingSteps > 0 && remainingResults > 0 {
		relations := []string{"mentions", "responds_to", "continues", "delegates_to", "related"}
		for _, candidate := range observation.Candidates {
			for _, relation := range relations {
				addAction(ports.NavigationAction{
					ActionID: actionID("explore", candidate.CitationID, relation), Intent: ports.IntentExplore,
					CitationID: candidate.CitationID, Relation: relation, Limit: min(s.config.ExploreLimit, remainingResults),
				})
			}
			addAction(ports.NavigationAction{
				ActionID: actionID("redirect", candidate.CitationID), Intent: ports.IntentRedirect, CitationID: candidate.CitationID,
			})
		}
	}
	if len(observation.Candidates) > 0 {
		addAction(ports.NavigationAction{ActionID: actionID("submit", "found"), Intent: ports.IntentSubmit, Found: true})
	}
	addAction(ports.NavigationAction{ActionID: actionID("submit", "not-found"), Intent: ports.IntentSubmit})
	sort.Slice(observation.Actions, func(i, j int) bool { return observation.Actions[i].ActionID < observation.Actions[j].ActionID })
	return observation, actions, nil
}

func (s *Service) selectStep(ctx context.Context, deadline time.Time, run ports.NavigationRunJournal, observation ports.NavigationObservation, actions map[string]ports.NavigationAction) (ports.NavigationStepJournal, error) {
	index := len(run.Steps)
	modelCalls, totalTokens := runUsage(run)
	serverStop := func(reason string, usage ports.ModelUsage, policy ports.ModelPolicy, modelCall bool) ports.NavigationStepJournal {
		intent := fallbackIntent(observation)
		return newStep(run.RunID, index, intent, actions[intent.ActionID], observation, modelCall, usage, policy, "server_clamp", reason)
	}
	if modelCalls >= run.MaxModelCalls {
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "model_call_limit", false, ports.ModelUsage{}, ports.ModelPolicy{}), nil
		}
		return serverStop("model_call_limit", ports.ModelUsage{}, ports.ModelPolicy{}, false), nil
	}
	if totalTokens >= s.config.MaxTotalTokens {
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "token_limit", false, ports.ModelUsage{}, ports.ModelPolicy{}), nil
		}
		return serverStop("token_limit", ports.ModelUsage{}, ports.ModelPolicy{}, false), nil
	}
	if !time.Now().Before(deadline) {
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "time_limit", false, ports.ModelUsage{}, ports.ModelPolicy{}), nil
		}
		return serverStop("time_limit", ports.ModelUsage{}, ports.ModelPolicy{}, false), nil
	}
	if s.selector == nil {
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "selector_unconfigured", false, ports.ModelUsage{}, ports.ModelPolicy{}), nil
		}
		intent := fallbackIntent(observation)
		return newStep(run.RunID, index, intent, actions[intent.ActionID], observation, false, ports.ModelUsage{}, ports.ModelPolicy{}, "deterministic_fallback", "selector_unconfigured"), nil
	}
	selectContext, cancel := context.WithDeadline(ctx, deadline)
	selection, err := s.selector.Select(selectContext, observation)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return ports.NavigationStepJournal{}, ctx.Err()
		}
		if !time.Now().Before(deadline) {
			if s.config.Mode == ModeRequired {
				return requiredUnavailableStep(run.RunID, index, "time_limit", true, ports.ModelUsage{}, ports.ModelPolicy{}), nil
			}
			return serverStop("time_limit", ports.ModelUsage{}, ports.ModelPolicy{}, true), nil
		}
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "selector_error", true, ports.ModelUsage{}, ports.ModelPolicy{}), nil
		}
		intent := fallbackIntent(observation)
		return newStep(run.RunID, index, intent, actions[intent.ActionID], observation, true, ports.ModelUsage{}, ports.ModelPolicy{}, "deterministic_fallback", "selector_error"), nil
	}
	if !validUsage(selection.Usage) || !validModelPolicy(selection.Policy) {
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "selector_invalid", true, ports.ModelUsage{}, ports.ModelPolicy{}), nil
		}
		intent := fallbackIntent(observation)
		return newStep(run.RunID, index, intent, actions[intent.ActionID], observation, true, ports.ModelUsage{}, ports.ModelPolicy{}, "deterministic_fallback", "selector_invalid"), nil
	}
	if totalTokens+selection.Usage.TotalTokens > s.config.MaxTotalTokens {
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "token_limit", true, selection.Usage, selection.Policy), nil
		}
		return serverStop("token_limit", selection.Usage, selection.Policy, true), nil
	}
	action, valid := actions[selection.Intent.ActionID]
	if !valid || action.Intent != selection.Intent.Kind || !validCitationReferences(observation, selection.Intent) ||
		(selection.Intent.Kind == ports.IntentSubmit && action.Found != (len(selection.Intent.CitationIDs) > 0)) {
		if s.config.Mode == ModeRequired {
			return requiredUnavailableStep(run.RunID, index, "selector_invalid", true, selection.Usage, selection.Policy), nil
		}
		intent := fallbackIntent(observation)
		return newStep(run.RunID, index, intent, actions[intent.ActionID], observation, true, selection.Usage, selection.Policy, "deterministic_fallback", "selector_invalid"), nil
	}
	return newStep(run.RunID, index, selection.Intent, action, observation, true, selection.Usage, selection.Policy, "llm", ""), nil
}

func (s *Service) execute(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, step ports.NavigationStepJournal) (stepOutcome, error) {
	// Re-observe before every persisted action so expired or revoked access is
	// rejected before served content is read or session state changes.
	_, _, err := s.exploration.ObserveNavigation(ctx, tenantID, principalID, sessionID, s.config.MaxCandidates)
	if err != nil {
		return stepOutcome{}, err
	}
	switch step.Intent.Kind {
	case ports.IntentExplore:
		result, err := s.exploration.Explore(ctx, tenantID, principalID, sessionID, exploration.ExploreRequest{
			OperationID: step.OperationID, AnchorCitationID: step.Action.CitationID,
			Relation: step.Action.Relation, Limit: step.Action.Limit,
		})
		if err != nil {
			return stepOutcome{}, err
		}
		return stepOutcome{State: result.State}, nil
	case ports.IntentRedirect:
		result, err := s.exploration.Redirect(ctx, tenantID, principalID, sessionID, exploration.RedirectRequest{
			OperationID: step.OperationID, Query: step.RedirectQuery, AnchorCitationIDs: []string{step.Action.CitationID},
			Reason: step.RedirectReason,
		})
		if err != nil {
			return stepOutcome{}, err
		}
		return stepOutcome{State: result.State}, nil
	case ports.IntentSubmit:
		found := step.Action.Found && len(step.Intent.CitationIDs) > 0
		summary := ""
		if found {
			summary = step.SubmitSummary
			if summary == "" {
				return stepOutcome{}, domain.NewProtocolError(422, "SUMMARY_REQUIRED", "persisted found submission has no evidence-derived summary")
			}
		}
		result, err := s.exploration.Submit(ctx, tenantID, principalID, sessionID, exploration.SubmitRequest{
			OperationID: step.OperationID, Found: found, Summary: summary, CitationIDs: append([]string(nil), step.Intent.CitationIDs...),
		})
		if err != nil {
			return stepOutcome{}, err
		}
		return stepOutcome{
			State: result.State, Terminal: true, Found: result.Found, Summary: result.Summary,
			Citations: result.Citations,
		}, nil
	default:
		return stepOutcome{}, unavailable("persisted path selection intent is invalid")
	}
}

func (s *Service) recoverBounded(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, step ports.NavigationStepJournal) (stepOutcome, error) {
	recoveryContext, cancel := context.WithTimeout(ctx, s.config.MaxRecoveryDuration)
	defer cancel()
	return s.recoverExpiredStep(recoveryContext, tenantID, principalID, sessionID, step)
}

func (s *Service) recoverExpiredStep(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, sessionID domain.ExplorationSessionID, step ports.NavigationStepJournal) (stepOutcome, error) {
	switch step.Intent.Kind {
	case ports.IntentExplore:
		result, found, err := s.exploration.ReplayExplore(ctx, tenantID, principalID, sessionID, exploration.ExploreRequest{
			OperationID: step.OperationID, AnchorCitationID: step.Action.CitationID,
			Relation: step.Action.Relation, Limit: step.Action.Limit,
		})
		if err != nil {
			return stepOutcome{}, err
		}
		if found {
			return stepOutcome{State: result.State}, nil
		}
	case ports.IntentRedirect:
		result, found, err := s.exploration.ReplayRedirect(ctx, tenantID, principalID, sessionID, exploration.RedirectRequest{
			OperationID: step.OperationID, Query: step.RedirectQuery, AnchorCitationIDs: []string{step.Action.CitationID},
			Reason: step.RedirectReason,
		})
		if err != nil {
			return stepOutcome{}, err
		}
		if found {
			return stepOutcome{State: result.State}, nil
		}
	case ports.IntentSubmit:
		foundSubmission := step.Action.Found && len(step.Intent.CitationIDs) > 0
		result, found, err := s.exploration.ReplaySubmit(ctx, tenantID, principalID, sessionID, exploration.SubmitRequest{
			OperationID: step.OperationID, Found: foundSubmission, Summary: step.SubmitSummary,
			CitationIDs: append([]string(nil), step.Intent.CitationIDs...),
		})
		if err != nil {
			return stepOutcome{}, err
		}
		if found {
			return stepOutcome{
				State: result.State, Terminal: true, Found: result.Found,
				Summary: result.Summary, Citations: result.Citations,
			}, nil
		}
	default:
		return stepOutcome{}, unavailable("persisted path selection intent is invalid")
	}
	// The intent was durable but no suboperation was. Mark it complete without
	// applying fresh work after the deadline; the next loop enforces the
	// optional server-stop or required-unavailable policy.
	return stepOutcome{State: "active"}, nil
}

func (s *Service) finish(ctx context.Context, sessionID domain.ExplorationSessionID, runID string, result Result) (Result, error) {
	duplicate := result.Duplicate
	result.Duplicate = false
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return Result{}, err
	}
	if err := s.store.CompleteNavigationRun(ctx, sessionID, runID, resultJSON); err != nil {
		return Result{}, err
	}
	if err := s.checkpoint(); err != nil {
		return Result{}, err
	}
	result.Duplicate = duplicate
	return result, nil
}

type stepOutcome struct {
	State     string                          `json:"state"`
	Terminal  bool                            `json:"terminal"`
	Found     bool                            `json:"found"`
	Summary   string                          `json:"summary"`
	Citations []exploration.SubmittedCitation `json:"citations"`
}

func terminalResult(run ports.NavigationRunJournal) (Result, bool, error) {
	if run.State == "completed" {
		result, err := decodeResult(run.ResultJSON)
		return result, true, err
	}
	if len(run.Steps) == 0 {
		return Result{}, false, nil
	}
	last := run.Steps[len(run.Steps)-1]
	if len(last.ResponseJSON) == 0 {
		return Result{}, false, nil
	}
	var outcome stepOutcome
	if err := json.Unmarshal(last.ResponseJSON, &outcome); err != nil {
		return Result{}, false, err
	}
	if !outcome.Terminal {
		return Result{}, false, nil
	}
	steps := make([]Step, 0, len(run.Steps))
	degradation := make([]string, 0)
	seenDegradation := make(map[string]bool)
	appliedPolicy := "llm"
	for _, journal := range run.Steps {
		steps = append(steps, Step{
			Index: journal.Index, Intent: journal.Intent.Kind, ActionID: journal.Intent.ActionID,
			OperationID: journal.OperationID, AppliedPolicy: journal.AppliedPolicy,
			Degradation: journal.Degradation, ModelPolicy: journal.ModelPolicy, Usage: journal.Usage,
		})
		if journal.Degradation != "" && !seenDegradation[journal.Degradation] {
			seenDegradation[journal.Degradation] = true
			degradation = append(degradation, journal.Degradation)
		}
		if journal.AppliedPolicy == "deterministic_fallback" {
			appliedPolicy = "deterministic_fallback"
		} else if journal.AppliedPolicy == "server_clamp" && appliedPolicy == "llm" {
			appliedPolicy = "server_clamp"
		}
	}
	return Result{
		State: outcome.State, Found: outcome.Found, Summary: outcome.Summary,
		Citations: outcome.Citations, Steps: steps, AppliedPolicy: appliedPolicy,
		Degradation: degradation,
	}, true, nil
}

func incompleteStep(run ports.NavigationRunJournal) (ports.NavigationStepJournal, bool) {
	for _, step := range run.Steps {
		if len(step.ResponseJSON) == 0 {
			return step, true
		}
	}
	return ports.NavigationStepJournal{}, false
}

func runUsage(run ports.NavigationRunJournal) (int, int) {
	modelCalls, tokens := 0, 0
	for _, step := range run.Steps {
		if step.ModelCall {
			modelCalls++
		}
		tokens += step.Usage.TotalTokens
	}
	return modelCalls, tokens
}

func deadlineStopStep(run ports.NavigationRunJournal) ports.NavigationStepJournal {
	action := ports.NavigationAction{
		ActionID: domain.StableCitationID("navact-", "server-deadline-stop", run.RunID),
		Intent:   ports.IntentSubmit,
	}
	return ports.NavigationStepJournal{
		Index: len(run.Steps), Intent: ports.Intent{Kind: ports.IntentSubmit, ActionID: action.ActionID, CitationIDs: []string{}},
		Action: action, OperationID: domain.StableCitationID("navstop-", run.RunID, fmt.Sprintf("%d", len(run.Steps))),
		AppliedPolicy: "server_clamp", Degradation: "time_limit",
	}
}

func isDeadlineStopStep(step ports.NavigationStepJournal) bool {
	return step.AppliedPolicy == "server_clamp" && step.Degradation == "time_limit" && !step.ModelCall &&
		strings.HasPrefix(step.OperationID, "navstop-")
}

func requiredUnavailableStep(runID string, index int, reason string, modelCall bool, usage ports.ModelUsage, policy ports.ModelPolicy) ports.NavigationStepJournal {
	return ports.NavigationStepJournal{
		Index: index, OperationID: domain.StableCitationID("navfail-", runID, fmt.Sprintf("%d", index)),
		ModelCall: modelCall, Usage: usage, ModelPolicy: policy,
		AppliedPolicy: "required_unavailable", Degradation: reason,
	}
}

func newStep(runID string, index int, intent ports.Intent, action ports.NavigationAction, observation ports.NavigationObservation, modelCall bool, usage ports.ModelUsage, modelPolicy ports.ModelPolicy, appliedPolicy, degradation string) ports.NavigationStepJournal {
	step := ports.NavigationStepJournal{
		Index: index, Intent: intent, Action: action,
		OperationID: domain.StableCitationID("navop-", runID, fmt.Sprintf("%d", index), intent.ActionID),
		ModelCall:   modelCall, Usage: usage, ModelPolicy: modelPolicy,
		AppliedPolicy: appliedPolicy, Degradation: degradation,
	}
	if intent.Kind == ports.IntentRedirect {
		step.RedirectQuery = truncateUTF8(strings.TrimSpace(observation.Query+"\nFocus on served evidence:\n"+candidateContent(observation.Candidates, action.CitationID)), 16000)
		step.RedirectReason = "automatic navigation selected a served citation"
	}
	if intent.Kind == ports.IntentSubmit && action.Found {
		step.SubmitSummary = deterministicCandidateSummary(observation.Candidates, intent.CitationIDs)
	}
	return step
}

func fallbackIntent(observation ports.NavigationObservation) ports.Intent {
	for _, action := range observation.Actions {
		if action.Intent == ports.IntentSubmit && !action.Found {
			return ports.Intent{Kind: ports.IntentSubmit, ActionID: action.ActionID, CitationIDs: []string{}}
		}
	}
	return ports.Intent{}
}

func validCitationReferences(observation ports.NavigationObservation, intent ports.Intent) bool {
	known := make(map[string]bool, len(observation.Candidates))
	for _, candidate := range observation.Candidates {
		known[candidate.CitationID] = true
	}
	seen := make(map[string]bool, len(intent.CitationIDs))
	for _, citationID := range intent.CitationIDs {
		if !known[citationID] || seen[citationID] {
			return false
		}
		seen[citationID] = true
	}
	if intent.Kind != ports.IntentSubmit {
		return len(intent.CitationIDs) == 0
	}
	return true
}

func validModelPolicy(policy ports.ModelPolicy) bool {
	return policy.Provider != "" && len(policy.Provider) <= 128 && policy.Model != "" && len(policy.Model) <= 256 &&
		policy.PromptPolicy != "" && len(policy.PromptPolicy) <= 128
}

func validUsage(usage ports.ModelUsage) bool {
	return usage.PromptTokens >= 0 && usage.CompletionTokens >= 0 && usage.TotalTokens >= 0 && usage.TotalTokens <= 1_000_000 &&
		usage.PromptTokens <= usage.TotalTokens && usage.CompletionTokens <= usage.TotalTokens &&
		usage.TotalTokens == usage.PromptTokens+usage.CompletionTokens
}

func actionID(parts ...string) string { return domain.StableCitationID("navact-", parts...) }

func deterministicCandidateSummary(candidates []ports.NavigationCandidate, citationIDs []string) string {
	byCitation := make(map[string]string, len(candidates))
	for _, candidate := range candidates {
		byCitation[candidate.CitationID] = candidate.Content
	}
	parts := make([]string, 0, len(citationIDs))
	for _, citationID := range citationIDs {
		content, ok := byCitation[citationID]
		if !ok {
			return ""
		}
		content = strings.TrimSpace(content)
		if content == "" {
			content = "Served evidence " + citationID
		}
		parts = append(parts, "["+citationID+"] "+content)
	}
	return truncateUTF8(strings.Join(parts, "\n"), 16000)
}

func candidateContent(candidates []ports.NavigationCandidate, citationID string) string {
	for _, candidate := range candidates {
		if candidate.CitationID == citationID {
			return candidate.Content
		}
	}
	return ""
}

func truncateUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func decodeResult(data []byte) (Result, error) {
	var result Result
	if len(data) == 0 {
		return Result{}, fmt.Errorf("navigation: completed run has no result")
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Service) checkpoint() error {
	if s.config.Checkpoint == nil {
		return nil
	}
	if err := s.config.Checkpoint(); err != nil {
		return fmt.Errorf("navigation: persist journal checkpoint: %w", err)
	}
	return nil
}

func unavailable(message string) error {
	return domain.NewProtocolError(503, "PATH_SELECTION_UNAVAILABLE", message)
}
