package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	leaseToken     string
}

// Outbox is the durable evidence projection queue. Entries stage and commit
// at-least-once against Graph Memory; "committed" is the only terminal state.
// Every stage/commit/reschedule must present the lease token ClaimPending
// minted, so only the claim owner advances a row; tokens carry the queue's
// epoch, which a restore strictly bumps, so pre-restart tokens can never
// collide with post-restart leases (R5).
type Outbox struct {
	mu     sync.Mutex
	rows   []*outboxRow
	epoch  int64
	claims int64
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

// Reconcile is the mandatory evidence-handoff contract (R5 round 6): every
// settled row the durable store recorded must be enqueued here before the
// call returns. Rows already present keep their workflow state — a staged
// or committed row is never downgraded, and its attempt counters and lease
// are untouched; rows the queue is missing are inserted. The handoff is a
// required capability: refusing it must happen through the returned error,
// never through a silent no-op, or settled evidence would go missing while
// later drains read an empty queue as success.
func (o *Outbox) Reconcile(_ context.Context, entries []domain.EvidenceOutboxEntry) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	known := make(map[string]bool, len(o.rows))
	for _, row := range o.rows {
		known[row.entry.ID] = true
	}
	for _, entry := range entries {
		if known[entry.ID] {
			continue
		}
		o.rows = append(o.rows, &outboxRow{entry: entry, stageAttempts: entry.Attempts})
		known[entry.ID] = true
	}
	return nil
}

func (o *Outbox) ClaimPending(_ context.Context, now time.Time) (ports.OutboxClaim, domain.EvidenceOutboxEntry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, row := range o.rows {
		if row.entry.State == "committed" || row.inFlight || row.entry.NextAttemptAt.After(now) {
			continue
		}
		row.inFlight = true
		row.entry.Attempts++
		o.claims++
		row.leaseToken = fmt.Sprintf("%d:%s#%d", o.epoch, row.entry.ID, o.claims)
		return ports.OutboxClaim{EntryID: row.entry.ID, Token: row.leaseToken}, row.entry, nil
	}
	return ports.OutboxClaim{}, domain.EvidenceOutboxEntry{}, ports.ErrNoOutboxClaim
}

func (o *Outbox) rowFor(entryID string, claim ports.OutboxClaim) (*outboxRow, error) {
	for _, row := range o.rows {
		if row.entry.ID != entryID {
			continue
		}
		if row.leaseToken == "" || row.leaseToken != claim.Token {
			return nil, fmt.Errorf("%w: entry %s (claim token stale, foreign, or expired)", ErrOutboxNotLeased, entryID)
		}
		return row, nil
	}
	return nil, ErrNotFound
}

func (o *Outbox) MarkStaged(_ context.Context, claim ports.OutboxClaim) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	row, err := o.rowFor(claim.EntryID, claim)
	if err != nil {
		return err
	}
	if row.entry.State == "committed" {
		return fmt.Errorf("outbox entry %s is already committed; refusing staged downgrade", claim.EntryID)
	}
	if row.entry.State == "staged" {
		return fmt.Errorf("%w: entry %s", ErrOutboxAlreadyStaged, claim.EntryID)
	}
	row.stageAttempts++
	row.entry.State = "staged"
	// The stage lease is RETAINED through the commit (R5 round 4): the
	// worker that staged the row stays its only owner, so a concurrent
	// drain can never commit the wrong row. The lease dies with an explicit
	// release path only: Reschedule (retry), RecoverPending (restart), or
	// MarkCommitted (terminal).
	return nil
}

func (o *Outbox) MarkCommitted(_ context.Context, claim ports.OutboxClaim) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	row, err := o.rowFor(claim.EntryID, claim)
	if err != nil {
		return err
	}
	// Commit requires a durable stage receipt (R5): the staging worker
	// commits under its retained lease, and only a post-restart or
	// post-reschedule worker retries the idempotent commit under a fresh
	// claim on the staged row.
	if row.entry.State != "staged" {
		return fmt.Errorf("%w: entry %s is %s", ErrOutboxStageReceiptRequired, claim.EntryID, row.entry.State)
	}
	row.commitAttempts++
	row.entry.State = "committed"
	row.inFlight = false
	row.leaseToken = ""
	return nil
}

// Reschedule records a failed stage/commit attempt. A staged entry keeps its
// stage receipt: recovery must retry only the idempotent commit, not re-stage
// the batch. A committed entry stays terminal.
func (o *Outbox) Reschedule(_ context.Context, claim ports.OutboxClaim, nextAttempt time.Time, lastError string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	row, err := o.rowFor(claim.EntryID, claim)
	if err != nil {
		return err
	}
	switch row.entry.State {
	case "staged", "committed":
	default:
		row.entry.State = "pending"
	}
	row.entry.NextAttemptAt = nextAttempt
	row.entry.LastError = lastError
	row.inFlight = false
	row.leaseToken = ""
	return nil
}

// ListPending is the non-destructive pending read: every non-committed row
// without clearing leases (R5). Normal recounts and barrier checks use it;
// RecoverPending is the startup boundary that kills leases.
func (o *Outbox) ListPending(_ context.Context, now time.Time) ([]domain.EvidenceOutboxEntry, error) {
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

// RecoverPending is the startup-recovery boundary: it lists every
// non-committed row and clears every live lease, so pre-crash claim tokens
// die with the process that minted them (R5). It must only run at process
// startup — normal draining and recounts use ListPending, which never clears
// leases.
func (o *Outbox) RecoverPending(_ context.Context, now time.Time) ([]domain.EvidenceOutboxEntry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var pending []domain.EvidenceOutboxEntry
	for _, row := range o.rows {
		if row.entry.State != "committed" {
			pending = append(pending, row.entry)
		}
		row.inFlight = false
		row.leaseToken = ""
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

// outboxSnapshot is the durable image of the standalone queue: rows with
// their attempt counters and the lease epoch. Live leases and the claim
// counter are excluded — a restart releases them — and the epoch is bumped
// on restore so a token minted in any earlier process lifetime can never
// validate against a row re-claimed afterwards (R5).
type outboxSnapshot struct {
	Epoch int64               `json:"epoch"`
	Rows  []OutboxRowSnapshot `json:"rows"`
}

// Snapshot returns the JSON image of the queue under one lock.
func (o *Outbox) Snapshot() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rows := make([]OutboxRowSnapshot, 0, len(o.rows))
	for _, row := range o.rows {
		rows = append(rows, OutboxRowSnapshot{
			Entry:          row.entry,
			StageAttempts:  row.stageAttempts,
			CommitAttempts: row.commitAttempts,
		})
	}
	return json.Marshal(outboxSnapshot{Epoch: o.epoch, Rows: rows})
}

// Restore replaces the queue contents with a snapshot image and bumps the
// lease epoch: pre-restart claim tokens die with the process that minted
// them, and the bumped epoch keeps every future token distinct from every
// past one (R5). An exhausted epoch fails closed instead of wrapping.
func (o *Outbox) Restore(image []byte) error {
	var data outboxSnapshot
	if err := json.Unmarshal(image, &data); err != nil {
		return fmt.Errorf("outbox snapshot is not valid JSON: %w", err)
	}
	if data.Epoch == math.MaxInt64 {
		return fmt.Errorf("outbox snapshot epoch is exhausted: lease tokens can no longer be fenced across restarts")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rows = make([]*outboxRow, 0, len(data.Rows))
	for _, row := range data.Rows {
		o.rows = append(o.rows, &outboxRow{
			entry:          row.Entry,
			stageAttempts:  row.StageAttempts,
			commitAttempts: row.CommitAttempts,
		})
	}
	o.epoch = data.Epoch + 1
	o.claims = 0
	return nil
}

var _ ports.OutboxStore = (*Outbox)(nil)
