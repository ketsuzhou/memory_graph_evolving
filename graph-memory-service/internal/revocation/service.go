// Package revocation implements PG-24 authorization epochs, the append-only
// revocation ledger, and cooperative training-stop polling (SC-7.x): a
// cached authorization computed under an epoch older than the subject's
// current epoch is invalid, and in-flight training observes revocation only
// at cooperative poll points.
package revocation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

var ErrRevocationNotImplemented = errors.New("revocation: not implemented")

var (
	// ErrStaleAuthorizationEpoch is the cache invalidation signal: the
	// presented epoch predates a recorded revocation.
	ErrStaleAuthorizationEpoch = errors.New("revocation: authorization epoch is stale")
	// ErrEpochRegression rejects ledger appends that would move a subject's
	// epoch backwards.
	ErrEpochRegression = errors.New("revocation: epoch must not regress")
)

type Subject struct {
	TenantID    domain.TenantID
	PrincipalID domain.PrincipalID
}

type Epoch int64
type TrainingID string
type Cursor string

type Entry struct {
	Cursor    Cursor
	Subject   Subject
	Epoch     Epoch
	Reason    string
	CreatedAt time.Time
}

// Ledger is append-only: its API intentionally has no update or delete.
type Ledger interface {
	Append(context.Context, Entry) (Entry, error)
	Entries(context.Context, Subject, Cursor) ([]Entry, error)
}

// memoryLedger is the built-in append-only ledger used when no external
// Ledger is injected.
type memoryLedger struct {
	mu      sync.Mutex
	seq     int
	entries []Entry
}

func (l *memoryLedger) Append(_ context.Context, entry Entry) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	entry.Cursor = Cursor(fmt.Sprintf("rev-%06d", l.seq))
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	l.entries = append(l.entries, entry)
	return entry, nil
}

func (l *memoryLedger) Entries(_ context.Context, subject Subject, _ Cursor) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, 0)
	for _, entry := range l.entries {
		if entry.Subject == subject {
			out = append(out, entry)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cursor < out[j].Cursor })
	return out, nil
}

// Service is the epoch authority. Subject epochs advance monotonically with
// each revocation; authorization and training polls fail closed when the
// caller presents an epoch older than the subject's current epoch.
type Service struct {
	mu     sync.Mutex
	ledger Ledger
	now    func() time.Time
}

func NewService() *Service {
	return &Service{ledger: &memoryLedger{}, now: time.Now}
}

// NewServiceWithLedger wires an external append-only ledger.
func NewServiceWithLedger(ledger Ledger) *Service {
	return &Service{ledger: ledger, now: time.Now}
}

// Authorize accepts only a decision computed at the subject's exact current
// epoch: anything older is a stale cache decision, and anything newer was
// never issued by this authority (epochs only advance through Revocation).
func (s *Service) Authorize(ctx context.Context, subject Subject, epoch Epoch) error {
	current, err := s.currentEpoch(ctx, subject)
	if err != nil {
		return err
	}
	if epoch != current {
		return ErrStaleAuthorizationEpoch
	}
	return nil
}

// Revoke appends one immutable revocation fact and advances the subject
// epoch. The presented epoch must strictly exceed the current one (SC-7.4:
// every deletion/withdrawal bumps the epoch); equal or older epochs are
// regressions. The ledger API has no update or delete by contract.
func (s *Service) Revoke(ctx context.Context, entry Entry) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.currentEpoch(ctx, entry.Subject)
	if err != nil {
		return Entry{}, err
	}
	if entry.Epoch <= current {
		return Entry{}, ErrEpochRegression
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = s.now()
	}
	stored, err := s.ledger.Append(ctx, entry)
	if err != nil {
		return Entry{}, err
	}
	return stored, nil
}

// MayContinueTraining is the cooperative poll point (Q88): in-flight
// training keeps polling; a stale epoch answer stops it voluntarily instead
// of a hard preempt, and a future epoch (never issued) also stops it fail
// closed.
func (s *Service) MayContinueTraining(ctx context.Context, _ TrainingID, subject Subject, epoch Epoch) error {
	current, err := s.currentEpoch(ctx, subject)
	if err != nil {
		return err
	}
	if epoch != current {
		return ErrStaleAuthorizationEpoch
	}
	return nil
}

// currentEpoch is the subject's latest revoked epoch (0 when never revoked).
func (s *Service) currentEpoch(ctx context.Context, subject Subject) (Epoch, error) {
	entries, err := s.ledger.Entries(ctx, subject, "")
	if err != nil {
		return 0, err
	}
	var current Epoch
	for _, entry := range entries {
		if entry.Epoch > current {
			current = entry.Epoch
		}
	}
	return current, nil
}
