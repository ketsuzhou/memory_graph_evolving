package revocation

import (
	"context"
	"errors"
	"testing"

	"river2.dev/graph-memory-service/internal/domain"
)

func testSubject() Subject { return Subject{TenantID: "tenant-a", PrincipalID: "principal-a"} }

func TestAuthorizationEpochInvalidatesStaleCacheDecision(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	// The subject starts at epoch 0; only the exact current epoch is valid.
	if err := svc.Authorize(ctx, testSubject(), Epoch(0)); err != nil {
		t.Fatalf("authorize before any revocation: %v", err)
	}
	if err := svc.Authorize(ctx, testSubject(), Epoch(1)); !errors.Is(err, ErrStaleAuthorizationEpoch) {
		t.Fatalf("future epoch was authorized without an issuance: err=%v", err)
	}
	// The revocation advances the subject epoch; the cached epoch-2 decision
	// must now be invalid while the fresh epoch-3 decision stays valid.
	if _, err := svc.Revoke(ctx, Entry{Subject: testSubject(), Epoch: 3, Reason: "consent-withdrawn"}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := svc.Authorize(ctx, testSubject(), Epoch(2)); !errors.Is(err, ErrStaleAuthorizationEpoch) {
		t.Fatalf("stale epoch-2 authorization was not invalidated: err=%v", err)
	}
	if err := svc.Authorize(ctx, testSubject(), Epoch(3)); err != nil {
		t.Fatalf("authorize at current epoch: %v", err)
	}
	if err := svc.Authorize(ctx, testSubject(), Epoch(4)); !errors.Is(err, ErrStaleAuthorizationEpoch) {
		t.Fatalf("unissued future epoch-4 authorization was accepted: err=%v", err)
	}
}

func TestRevocationRequiresStrictEpochBumpAndAppendsUniqueCursors(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	entry, err := svc.Revoke(ctx, Entry{Subject: testSubject(), Epoch: 3, Reason: "consent-withdrawn"})
	if errors.Is(err, ErrRevocationNotImplemented) {
		t.Fatal("RED: append-only revocation ledger is not implemented")
	}
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if entry.Cursor == "" || entry.Epoch != 3 {
		t.Fatalf("revocation entry is not durable/ordered: %#v", entry)
	}
	// SC-7.4: a revocation bumps the epoch. Re-appending the same epoch is a
	// regression and must be rejected, not produce a second valid event.
	if _, err := svc.Revoke(ctx, Entry{Subject: testSubject(), Epoch: 3, Reason: "consent-withdrawn"}); !errors.Is(err, ErrEpochRegression) {
		t.Fatalf("same-epoch revocation append was accepted: err=%v", err)
	}
	if _, err := svc.Revoke(ctx, Entry{Subject: testSubject(), Epoch: 2, Reason: "consent-withdrawn"}); !errors.Is(err, ErrEpochRegression) {
		t.Fatalf("older-epoch revocation append was accepted: err=%v", err)
	}
	// The only legal follow-up append is the strictly next epoch.
	replayed, err := svc.Revoke(ctx, Entry{Subject: testSubject(), Epoch: 4, Reason: "consent-withdrawn-again"})
	if err != nil {
		t.Fatalf("next-epoch append: %v", err)
	}
	if replayed.Cursor == entry.Cursor {
		t.Fatal("ledger reused a cursor; appends are not unique")
	}
	if replayed.Cursor <= entry.Cursor {
		t.Fatal("ledger cursors are not ordered")
	}
}

func TestRevocationStopsInFlightTrainingAtCooperativePoll(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	subject := Subject{TenantID: domain.TenantID("tenant-a"), PrincipalID: domain.PrincipalID("principal-a")}
	// Before revocation the cooperative poll lets training continue at the
	// current epoch 0; an unissued future epoch stops it fail closed.
	if err := svc.MayContinueTraining(ctx, TrainingID("training-7"), subject, Epoch(0)); err != nil {
		t.Fatalf("training continuation gate: %v", err)
	}
	if err := svc.MayContinueTraining(ctx, TrainingID("training-7"), subject, Epoch(3)); !errors.Is(err, ErrStaleAuthorizationEpoch) {
		t.Fatalf("future-epoch training poll was accepted: err=%v", err)
	}
	// After a revocation the same poll at the stale epoch stops training;
	// a poll observing the new epoch continues.
	if _, err := svc.Revoke(ctx, Entry{Subject: subject, Epoch: 4, Reason: "consent-withdrawn"}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := svc.MayContinueTraining(ctx, TrainingID("training-7"), subject, Epoch(3)); !errors.Is(err, ErrStaleAuthorizationEpoch) {
		t.Fatalf("stale training poll was not stopped: err=%v", err)
	}
	if err := svc.MayContinueTraining(ctx, TrainingID("training-7"), subject, Epoch(4)); err != nil {
		t.Fatalf("post-revocation epoch poll should continue: %v", err)
	}
	if err := svc.MayContinueTraining(ctx, TrainingID("training-7"), subject, Epoch(5)); !errors.Is(err, ErrStaleAuthorizationEpoch) {
		t.Fatalf("future-epoch training poll was accepted post-revocation: err=%v", err)
	}
}
