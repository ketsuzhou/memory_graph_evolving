package recall_test

import (
	"context"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
	"river2.dev/graph-memory-service/internal/recall"
)

// delayedRecallStore returns its items only after the delay, honoring
// context cancellation so a deadline-bound caller observes expiry.
type delayedRecallStore struct {
	items []domain.RecallItem
	delay time.Duration
}

func (s *delayedRecallStore) Retrieve(ctx context.Context, _ domain.TenantID, _ []domain.PinnedSpace, _ string, _ int) (ports.RecallOutcome, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
	}
	return ports.RecallOutcome{Items: s.items, Applied: []ports.RetrievalChannel{ports.RetrievalBM25}}, nil
}

// TestReviewDeadlineBoundsRetrieval proves deadline_ms is executed: when the
// retrieval outlives the caller's budget, Recall degrades to timed_out with
// the items already fetched instead of failing or over-waiting.
func TestReviewDeadlineBoundsRetrieval(t *testing.T) {
	fx := newFixture(t)
	const space = domain.SpaceID("space-deadline")
	fx.putSpace(t, space, domain.SpaceShared, 1)
	fx.putGrant(t, "grant-deadline", []domain.SpaceID{space}, []domain.GrantOperation{domain.GrantOperationRecall}, fx.clock.Now().Add(time.Hour))

	service := recall.New(fx.store, &delayedRecallStore{
		items: []domain.RecallItem{{Content: "already fetched", SourceSpaceID: space, MemoryVersion: 1}},
		delay: 200 * time.Millisecond,
	}, fx.clock)

	result, err := service.Recall(context.Background(), testTenantID, testPrincipalID, domain.RecallRequest{
		RequestID: "request-deadline", Query: "launch", SpaceIDs: []domain.SpaceID{space},
		MaxResults: 5, DeadlineMS: 10,
	})
	if err != nil {
		t.Fatalf("deadline exceeded returned an error instead of a typed degradation: %v", err)
	}
	if result.Degradation.State != "timed_out" {
		t.Fatalf("degradation state = %q, want timed_out", result.Degradation.State)
	}
	if len(result.Degradation.Reasons) == 0 {
		t.Fatal("timed_out degradation carries no reasons")
	}
	if len(result.Items) != 1 || result.Items[0].Content != "already fetched" {
		t.Fatalf("items = %#v, want the one item fetched before expiry", result.Items)
	}
	if result.RequestID != "request-deadline" {
		t.Fatalf("request_id = %q, want request-deadline", result.RequestID)
	}
}

// TestReviewDeadlineWithinBudgetCompletes proves a deadline that is not hit
// does not degrade the result.
func TestReviewDeadlineWithinBudgetCompletes(t *testing.T) {
	fx := newFixture(t)
	const space = domain.SpaceID("space-deadline-ok")
	fx.putSpace(t, space, domain.SpaceShared, 1)
	fx.putGrant(t, "grant-deadline-ok", []domain.SpaceID{space}, []domain.GrantOperation{domain.GrantOperationRecall}, fx.clock.Now().Add(time.Hour))

	service := recall.New(fx.store, &delayedRecallStore{
		items: []domain.RecallItem{{Content: "fast item", SourceSpaceID: space, MemoryVersion: 1}},
		delay: 0,
	}, fx.clock)

	result, err := service.Recall(context.Background(), testTenantID, testPrincipalID, domain.RecallRequest{
		RequestID: "request-deadline-ok", Query: "launch", SpaceIDs: []domain.SpaceID{space},
		MaxResults: 5, DeadlineMS: 5000,
	})
	if err != nil {
		t.Fatalf("recall within deadline: %v", err)
	}
	if result.Degradation.State != "complete" {
		t.Fatalf("degradation state = %q, want complete", result.Degradation.State)
	}
}
