package armc

import (
	"context"
	"fmt"
	"sync"

	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

// UsageSummaryReader supplies the observational, non-diagnostic priority
// input. It is deliberately narrower than any Arm C evaluation or activation
// authority.
type UsageSummaryReader interface {
	UsageSummary(context.Context, usageprojection.UsageSummaryRequest) (usageprojection.UsageSummary, error)
}

// PendingItem is an already-admitted Arm C work item. Queue priority does not
// make an item eligible and carries no activation authority.
type PendingItem struct {
	CandidateID   string
	Subject       usageprojection.UsageSubjectRef
	AuthorityTier usageprojection.AuthorityTier
}

// PendingDequeue pairs an item with the exact observational explanation used
// to select it, so operators can audit scheduling without inferring authority.
type PendingDequeue struct {
	Item        PendingItem
	Explanation usageprojection.PriorityExplanation
}

// PendingQueue is the scheduling seam for immutable Arm C work. A production
// scheduler may persist/drive it later; this queue intentionally changes only
// the order in which already pending work is selected.
type PendingQueue struct {
	usage UsageSummaryReader
	mu    sync.Mutex
	items map[string]PendingItem
}

func NewPendingQueue(usage UsageSummaryReader) *PendingQueue {
	return &PendingQueue{usage: usage, items: map[string]PendingItem{}}
}

func (q *PendingQueue) Enqueue(_ context.Context, item PendingItem) error {
	if item.CandidateID == "" {
		return fmt.Errorf("armc pending queue: candidate id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if existing, ok := q.items[item.CandidateID]; ok && existing != item {
		return fmt.Errorf("armc pending queue: candidate %s conflicts", item.CandidateID)
	}
	q.items[item.CandidateID] = item
	return nil
}

func (q *PendingQueue) Dequeue(ctx context.Context) (PendingDequeue, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return PendingDequeue{}, false, nil
	}
	summary := usageprojection.UsageSummary{PolicyVersion: "usage-unavailable"}
	if q.usage != nil {
		var err error
		summary, err = q.usage.UsageSummary(ctx, usageprojection.UsageSummaryRequest{IncludeDiagnostic: false})
		if err != nil {
			return PendingDequeue{}, false, err
		}
	}
	inputs := make([]usageprojection.PriorityInput, 0, len(q.items))
	for _, item := range q.items {
		inputs = append(inputs, usageprojection.PriorityInput{Key: item.CandidateID, AuthorityTier: item.AuthorityTier, Subject: item.Subject})
	}
	ordered := usageprojection.Prioritize(summary, inputs)
	selected := ordered[0]
	item := q.items[selected.Key]
	delete(q.items, selected.Key)
	return PendingDequeue{Item: item, Explanation: selected.Explanation}, true, nil
}
