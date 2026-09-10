package memory

import (
	"context"
	"strings"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

var recallNow = time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)

// recallBatch builds one settled evidence batch carrying the given contents as
// room_message events, the shape the Host's whole-room projection emits.
func recallBatch(id domain.BatchID, tenant domain.TenantID, space domain.SpaceID, contents []string) domain.EvidenceBatch {
	var events []domain.EvidenceEvent
	for index, content := range contents {
		events = append(events, domain.EvidenceEvent{
			ID: "event-" + string(id) + "-" + string(rune('a'+index)), Sequence: int64(index + 1),
			Kind: "room_message", Content: content, OccurredAt: recallNow.Add(-time.Minute),
		})
	}
	return domain.EvidenceBatch{
		ID: id, TenantID: tenant, IdempotencyKey: "key-" + string(id), SpaceID: space,
		StreamID: "stream-" + string(id), SourceSegmentID: "segment-" + string(id),
		Provenance: domain.Provenance{
			HostType: "pi-group-chat-host", HostInstanceID: "host", SourceKind: "room_shared",
			CapturedAt: recallNow.Add(-time.Minute), ContentSHA256: strings.Repeat("a", 64),
		},
		Events:          events,
		Links:           []domain.EvidenceLink{},
		TerminalOutcome: "settled",
	}
}

func commitRecallBatch(t *testing.T, store *Store, batch domain.EvidenceBatch) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.InitializeTenant(ctx, domain.Tenant{
		ID: batch.TenantID, DisplayName: "Recall fixture", BootstrapPrincipalID: "bootstrap",
	}, domain.Principal{ID: "bootstrap", TenantID: batch.TenantID, Kind: domain.PrincipalService, DisplayName: "Bootstrap"}); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	if _, err := store.PutSpace(ctx, batch.TenantID, domain.Space{ID: batch.SpaceID, TenantID: batch.TenantID, Scope: domain.SpaceShared, DisplayName: "Recall fixture"}); err != nil {
		t.Fatalf("put space: %v", err)
	}
	if _, _, err := store.Stage(ctx, batch); err != nil {
		t.Fatalf("stage %s: %v", batch.ID, err)
	}
	if _, _, err := store.Commit(ctx, batch.TenantID, batch.ID, "commit-"+string(batch.ID), recallNow); err != nil {
		t.Fatalf("commit %s: %v", batch.ID, err)
	}
}

// The Host's whole-room projection restages earlier messages inside every new
// episode's batch; recall must serve identical content once, not once per
// batch, or duplicates crowd out everything else in the top-k.
func TestRecallDeduplicatesRestagedContent(t *testing.T) {
	store := New()
	const tenant = domain.TenantID("tenant-recall")
	const space = domain.SpaceID("space-recall")
	preference := "User prefers compact TSV reports with High/Medium/Low priorities"

	commitRecallBatch(t, store, recallBatch("batch-1", tenant, space, []string{preference}))
	commitRecallBatch(t, store, recallBatch("batch-2", tenant, space, []string{
		preference, // restaged episode-1 message
		"Solve the array divisibility problem and send the code",
	}))

	items, err := store.Recall(context.Background(), tenant, []domain.PinnedSpace{{SpaceID: space, MemoryVersion: 2}}, "TSV report priorities", 5)
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	served := 0
	for _, item := range items {
		if strings.Contains(item.Content, "TSV") {
			served++
		}
	}
	if served != 1 {
		t.Fatalf("restaged content served %d times, want exactly 1", served)
	}
}

// Word-overlap alone lets a long transcript that merely contains the query
// words outrank the compact entry that answers the query; the sqrt content
// length normalization flips that ordering.
func TestRecallLengthNormalizationFavorsCompactEntries(t *testing.T) {
	store := New()
	const tenant = domain.TenantID("tenant-recall")
	const space = domain.SpaceID("space-recall")
	long := strings.Repeat("filler word ", 400) + "the launch is friday and the budget risk is real"
	compact := "The team agreed that launch is Friday."

	commitRecallBatch(t, store, recallBatch("batch-long", tenant, space, []string{long}))
	commitRecallBatch(t, store, recallBatch("batch-compact", tenant, space, []string{compact}))

	items, err := store.Recall(context.Background(), tenant, []domain.PinnedSpace{{SpaceID: space, MemoryVersion: 2}}, "when is the launch friday", 5)
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("expected at least one recalled item")
	}
	if !strings.Contains(items[0].Content, "launch is Friday") {
		t.Fatalf("compact entry should outrank the long transcript; top item starts %q", items[0].Content[:min(80, len(items[0].Content))])
	}
}
