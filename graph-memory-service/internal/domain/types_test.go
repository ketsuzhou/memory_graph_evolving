package domain_test

import (
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

func TestGrantActiveAtUsesStrictNanosecondExpiryBoundary(t *testing.T) {
	expiresAt := time.Date(2030, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	grant := domain.Grant{ExpiresAt: expiresAt}

	if !grant.ActiveAt(expiresAt.Add(-time.Nanosecond)) {
		t.Error("grant must be active one nanosecond before expiry")
	}
	if grant.ActiveAt(expiresAt) {
		t.Error("grant must be expired at exact equality")
	}
	if grant.ActiveAt(expiresAt.Add(time.Nanosecond)) {
		t.Error("grant must remain expired after expiry")
	}
}
