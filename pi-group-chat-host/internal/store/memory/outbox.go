package memory

import (
	"context"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

type outboxRow struct {
	entry          domain.EvidenceOutboxEntry
	stageAttempts  int
	commitAttempts int
	inFlight       bool
}

// Outbox is the durable evidence projection queue. Entries stage and commit
// at-least-once against Graph Memory; "committed" is the only terminal state.
type Outbox struct {
	mu   sync.Mutex
	rows []*outboxRow
}

func NewOutbox() *Outbox {
	return &Outbox{}
}

// Adopt inserts settled outbox entries into the queue, replacing any row with
// the same ID so crash replays stay unique.
func (o *Outbox) Adopt(entries []domain.EvidenceOutboxEntry) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, entry := range entries {
		replaced := false
		for index, row := range o.rows {
			if row.entry.ID == entry.ID {
				o.rows[index] = &outboxRow{entry: entry, stageAttempts: entry.Attempts}
				replaced = true
				break
			}
		}
		if !replaced {
			o.rows = append(o.rows, &outboxRow{entry: entry, stageAttempts: entry.Attempts})
		}
	}
	return nil
}

func (o *Outbox) ClaimPending(_ context.Context, now time.Time) (domain.EvidenceOutboxEntry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, row := range o.rows {
		if row.entry.State == "committed" || row.inFlight || row.entry.NextAttemptAt.After(now) {
			continue
		}
		row.inFlight = true
		row.entry.Attempts++
		return row.entry, nil
	}
	return domain.EvidenceOutboxEntry{}, ErrNotFound
}

func (o *Outbox) MarkStaged(_ context.Context, entryID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, row := range o.rows {
		if row.entry.ID != entryID {
			continue
		}
		if row.entry.State == "pending" {
			row.stageAttempts++
		}
		row.entry.State = "staged"
		row.inFlight = false
		return nil
	}
	return ErrNotFound
}

func (o *Outbox) MarkCommitted(_ context.Context, entryID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, row := range o.rows {
		if row.entry.ID != entryID {
			continue
		}
		if row.entry.State != "committed" {
			row.commitAttempts++
		}
		row.entry.State = "committed"
		row.inFlight = false
		return nil
	}
	return ErrNotFound
}

func (o *Outbox) Reschedule(_ context.Context, entryID string, nextAttempt time.Time, lastError string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, row := range o.rows {
		if row.entry.ID != entryID {
			continue
		}
		row.entry.State = "pending"
		row.entry.NextAttemptAt = nextAttempt
		row.entry.LastError = lastError
		row.inFlight = false
		return nil
	}
	return ErrNotFound
}

func (o *Outbox) RecoverPending(_ context.Context, now time.Time) ([]domain.EvidenceOutboxEntry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var pending []domain.EvidenceOutboxEntry
	for _, row := range o.rows {
		if row.entry.State != "committed" {
			pending = append(pending, row.entry)
		}
	}
	return pending, nil
}

// Entries returns every outbox row in creation order with its attempt
// counters, for observation and restart reporting.
func (o *Outbox) Entries() []OutboxRowSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	snapshots := make([]OutboxRowSnapshot, 0, len(o.rows))
	for _, row := range o.rows {
		snapshots = append(snapshots, OutboxRowSnapshot{
			Entry:          row.entry,
			StageAttempts:  row.stageAttempts,
			CommitAttempts: row.commitAttempts,
		})
	}
	return snapshots
}

type OutboxRowSnapshot struct {
	Entry          domain.EvidenceOutboxEntry
	StageAttempts  int
	CommitAttempts int
}

var _ ports.OutboxStore = (*Outbox)(nil)
