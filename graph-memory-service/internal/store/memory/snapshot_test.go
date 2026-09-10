package memory

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

var snapNow = time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)

// seedSnapshotStore fills a store with one of every durable shape: tenant,
// principals, spaces, grants, a committed batch, and an exploration session
// with a recorded start idempotency key, replayable operation, and served
// citations.
func seedSnapshotStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	store := New()
	const tenant = domain.TenantID("tenant-snap")
	const agent = domain.PrincipalID("agent-snap")
	const space = domain.SpaceID("space-snap")

	if _, err := store.InitializeTenant(ctx, domain.Tenant{
		ID: tenant, DisplayName: "Snap", BootstrapPrincipalID: "bootstrap",
	}, domain.Principal{ID: "bootstrap", TenantID: tenant, Kind: domain.PrincipalService, DisplayName: "Bootstrap"}); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	if _, err := store.PutPrincipal(ctx, tenant, domain.Principal{ID: agent, TenantID: tenant, Kind: domain.PrincipalAgent, DisplayName: "Agent"}); err != nil {
		t.Fatalf("put principal: %v", err)
	}
	if _, err := store.PutSpace(ctx, tenant, domain.Space{ID: space, TenantID: tenant, Scope: domain.SpaceShared, DisplayName: "Shared"}); err != nil {
		t.Fatalf("put space: %v", err)
	}
	if _, err := store.PutGrant(ctx, tenant, domain.Grant{
		ID: "grant-snap", TenantID: tenant, PrincipalID: agent, SpaceIDs: []domain.SpaceID{space},
		Purpose: domain.GrantPurposeToolPlane, ExpiresAt: snapNow.Add(time.Hour),
		Operations: []domain.GrantOperation{domain.GrantOperationExplorationStart},
	}); err != nil {
		t.Fatalf("put grant: %v", err)
	}

	batch := domain.EvidenceBatch{
		ID: "batch-snap", TenantID: tenant, IdempotencyKey: "outbox-snap", SpaceID: space,
		StreamID: "stream-snap", SourceSegmentID: "segment-snap",
		Provenance: domain.Provenance{
			HostType: "pi-group-chat-host", HostInstanceID: "host-snap", SourceKind: "room_shared",
			CapturedAt: snapNow.Add(-time.Minute), ContentSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Events:          []domain.EvidenceEvent{{ID: "event-snap", Sequence: 1, Kind: "room_message", Content: "Durable fact.", OccurredAt: snapNow.Add(-time.Minute)}},
		Links:           []domain.EvidenceLink{},
		TerminalOutcome: "settled",
	}
	if _, _, err := store.Stage(ctx, batch); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if _, _, err := store.Commit(ctx, tenant, batch.ID, "commit-snap", snapNow); err != nil {
		t.Fatalf("commit: %v", err)
	}

	session := domain.ExplorationSession{
		ID: "session-snap", TenantID: tenant, PrincipalID: agent,
		SpaceIDs: []domain.SpaceID{space}, PinnedSpaces: []domain.PinnedSpace{{SpaceID: space, MemoryVersion: 1}},
		RequestID: "request-snap", Query: "durable", Budget: domain.ExplorationBudget{MaxSteps: 3, MaxResults: 10},
		State: "active", ExpiresAt: snapNow.Add(time.Hour),
	}
	if _, _, err := store.Start(ctx, session, "start-snap"); err != nil {
		t.Fatalf("start session: %v", err)
	}
	items, err := store.Recall(ctx, tenant, session.PinnedSpaces, "durable", 10)
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if err := store.RecordServed(ctx, session.ID, items); err != nil {
		t.Fatalf("record served: %v", err)
	}
	return store
}

func TestSnapshotRoundTripsThroughFile(t *testing.T) {
	ctx := context.Background()
	store := seedSnapshotStore(t)
	path := filepath.Join(t.TempDir(), "state.json")

	if err := store.PersistToFile(path); err != nil {
		t.Fatalf("persist: %v", err)
	}

	restored := New()
	if err := restored.LoadFromFile(path); err != nil {
		t.Fatalf("load: %v", err)
	}

	// Binding survives.
	tenant, principal, ok := restored.Binding()
	if !ok || tenant != "tenant-snap" || principal != "bootstrap" {
		t.Fatalf("binding = (%q, %q, %v), want (tenant-snap, bootstrap, true)", tenant, principal, ok)
	}
	// Committed evidence stays visible to recall with source identity.
	items, err := restored.Recall(ctx, tenant, []domain.PinnedSpace{{SpaceID: "space-snap", MemoryVersion: 1}}, "durable", 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("recall after restore = (%d items, %v), want 1 item", len(items), err)
	}
	if items[0].Citation.EvidenceBatchID != "batch-snap" || items[0].MemoryVersion != 1 {
		t.Fatalf("recalled item lost identity: %#v", items[0])
	}
	// Batch status keeps the committed state and version.
	batch, err := restored.Batch(ctx, tenant, "batch-snap")
	if err != nil || batch.State != domain.EvidenceBatchCommitted || batch.MemoryVersion == nil || *batch.MemoryVersion != 1 {
		t.Fatalf("batch after restore = (%#v, %v)", batch, err)
	}
	// Stage idempotency survives: replaying the identical stage request is a
	// duplicate; a different batch under the same key stays a conflict.
	_, duplicate, err := restored.Stage(ctx, domain.EvidenceBatch{
		ID: "batch-snap", TenantID: tenant, IdempotencyKey: "outbox-snap", SpaceID: "space-snap",
		StreamID: "stream-snap", SourceSegmentID: "segment-snap",
		Provenance: domain.Provenance{
			HostType: "pi-group-chat-host", HostInstanceID: "host-snap", SourceKind: "room_shared",
			CapturedAt: snapNow.Add(-time.Minute), ContentSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Events:          []domain.EvidenceEvent{{ID: "event-snap", Sequence: 1, Kind: "room_message", Content: "Durable fact.", OccurredAt: snapNow.Add(-time.Minute)}},
		TerminalOutcome: "settled",
	})
	if err != nil || !duplicate {
		t.Fatalf("stage replay after restore = (duplicate=%v, %v), want duplicate", duplicate, err)
	}
	// Exploration session, served citations, and start idempotency survive.
	session, err := restored.Session(ctx, tenant, "session-snap")
	if err != nil || session.State != "active" || session.RequestID != "request-snap" || session.ResultsServed != 1 {
		t.Fatalf("session after restore = (%#v, %v)", session, err)
	}
	if _, err := restored.Session(ctx, tenant, "session-other"); err == nil {
		t.Fatal("unknown session resolved after restore")
	}
	replayed, duplicate, err := restored.Start(ctx, session, "start-snap")
	if err != nil || !duplicate || replayed.ID != session.ID {
		t.Fatalf("start replay after restore = (%q, duplicate=%v, %v)", replayed.ID, duplicate, err)
	}
}

func TestLoadFromFileMissingFileStartsEmpty(t *testing.T) {
	store := New()
	if err := store.LoadFromFile(filepath.Join(t.TempDir(), "absent.json")); err != nil {
		t.Fatalf("missing snapshot file must be a clean first boot: %v", err)
	}
	if _, _, ok := store.Binding(); ok {
		t.Fatal("empty store must report no binding")
	}
}

func TestRestoreRejectsCorruptImage(t *testing.T) {
	store := New()
	if err := store.Restore([]byte("{not json")); err == nil {
		t.Fatal("corrupt image must fail closed")
	}
}
