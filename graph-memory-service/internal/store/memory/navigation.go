package memory

import (
	"bytes"
	"context"
	"sort"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

func (s *Store) StartNavigationRun(_ context.Context, sessionID domain.ExplorationSessionID, runID string, maxModelCalls int, startedAt, deadline time.Time) (ports.NavigationRunJournal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[sessionID]; !ok {
		return ports.NavigationRunJournal{}, false, errSession
	}
	byRun := s.navigationRuns[sessionID]
	if byRun == nil {
		byRun = make(map[string]ports.NavigationRunJournal)
		s.navigationRuns[sessionID] = byRun
	}
	if existing, ok := byRun[runID]; ok {
		if existing.MaxModelCalls != maxModelCalls {
			return ports.NavigationRunJournal{}, false, errConflict
		}
		return cloneNavigationRun(existing), true, nil
	}
	run := ports.NavigationRunJournal{
		SessionID: sessionID, RunID: runID, MaxModelCalls: maxModelCalls,
		StartedAt: startedAt, Deadline: deadline, State: "running", Steps: []ports.NavigationStepJournal{},
	}
	byRun[runID] = run
	return cloneNavigationRun(run), false, nil
}

func (s *Store) NavigationRun(_ context.Context, sessionID domain.ExplorationSessionID, runID string) (ports.NavigationRunJournal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[sessionID]; !ok {
		return ports.NavigationRunJournal{}, false, errSession
	}
	run, ok := s.navigationRuns[sessionID][runID]
	if !ok {
		return ports.NavigationRunJournal{}, false, nil
	}
	return cloneNavigationRun(run), true, nil
}

// RecordNavigationIntent uses first-writer-wins for one logical step. A
// concurrent retry that sampled a different provider response receives the
// already durable decision and must execute that instead.
func (s *Store) RecordNavigationIntent(_ context.Context, sessionID domain.ExplorationSessionID, runID string, step ports.NavigationStepJournal) (ports.NavigationStepJournal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.navigationRuns[sessionID][runID]
	if !ok {
		return ports.NavigationStepJournal{}, false, errSession
	}
	if run.State != "running" || step.Index < 0 || step.Index > len(run.Steps) {
		return ports.NavigationStepJournal{}, false, errConflict
	}
	if step.Index < len(run.Steps) {
		return cloneNavigationStep(run.Steps[step.Index]), true, nil
	}
	stored := cloneNavigationStep(step)
	stored.ResponseJSON = nil
	run.Steps = append(run.Steps, stored)
	s.navigationRuns[sessionID][runID] = run
	return cloneNavigationStep(stored), false, nil
}

func (s *Store) CompleteNavigationStep(_ context.Context, sessionID domain.ExplorationSessionID, runID string, stepIndex int, operationID string, responseJSON []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.navigationRuns[sessionID][runID]
	if !ok || stepIndex < 0 || stepIndex >= len(run.Steps) {
		return errSession
	}
	step := run.Steps[stepIndex]
	if step.OperationID != operationID || len(responseJSON) == 0 {
		return errConflict
	}
	if len(step.ResponseJSON) > 0 {
		if !bytes.Equal(step.ResponseJSON, responseJSON) {
			return errConflict
		}
		return nil
	}
	step.ResponseJSON = append([]byte(nil), responseJSON...)
	run.Steps[stepIndex] = step
	s.navigationRuns[sessionID][runID] = run
	return nil
}

func (s *Store) CompleteNavigationRun(_ context.Context, sessionID domain.ExplorationSessionID, runID string, resultJSON []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.navigationRuns[sessionID][runID]
	if !ok || len(resultJSON) == 0 {
		return errSession
	}
	if run.State == "completed" {
		if !bytes.Equal(run.ResultJSON, resultJSON) {
			return errConflict
		}
		return nil
	}
	run.State = "completed"
	run.ResultJSON = append([]byte(nil), resultJSON...)
	s.navigationRuns[sessionID][runID] = run
	return nil
}

func (s *Store) ServedNavigationItems(_ context.Context, sessionID domain.ExplorationSessionID, limit int) ([]domain.RecallItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[sessionID]; !ok {
		return nil, errSession
	}
	citationIDs := make([]string, 0, len(s.servedItems[sessionID]))
	for citationID := range s.servedItems[sessionID] {
		citationIDs = append(citationIDs, citationID)
	}
	sort.Strings(citationIDs)
	if limit >= 0 && len(citationIDs) > limit {
		citationIDs = citationIDs[:limit]
	}
	items := make([]domain.RecallItem, 0, len(citationIDs))
	for _, citationID := range citationIDs {
		item := s.servedItems[sessionID][citationID]
		item.Citation.EventIDs = append([]string(nil), item.Citation.EventIDs...)
		if item.Traversal != nil {
			traversal := *item.Traversal
			item.Traversal = &traversal
		}
		items = append(items, item)
	}
	return items, nil
}

func cloneNavigationRun(run ports.NavigationRunJournal) ports.NavigationRunJournal {
	cloned := run
	cloned.ResultJSON = append([]byte(nil), run.ResultJSON...)
	cloned.Steps = make([]ports.NavigationStepJournal, len(run.Steps))
	for index, step := range run.Steps {
		cloned.Steps[index] = cloneNavigationStep(step)
	}
	return cloned
}

func cloneNavigationStep(step ports.NavigationStepJournal) ports.NavigationStepJournal {
	cloned := step
	cloned.Intent.CitationIDs = append([]string(nil), step.Intent.CitationIDs...)
	cloned.ResponseJSON = append([]byte(nil), step.ResponseJSON...)
	return cloned
}

var _ ports.NavigationStore = (*Store)(nil)
