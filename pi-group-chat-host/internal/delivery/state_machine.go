package delivery

import (
	"errors"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

var (
	ErrNotImplemented    = ports.ErrNotImplemented
	ErrInvalidTransition = errors.New("invalid delivery state transition")
)

// Transition applies the Delivery state machine. Legal transitions are
// pending->accepted->settled and pending|accepted->failed|aborted. Nothing
// else moves, so agent_end can never settle and settled is terminal.
func Transition(current domain.Delivery, target domain.DeliveryState, at time.Time) (domain.Delivery, error) {
	legal := false
	switch target {
	case domain.DeliveryAccepted:
		legal = current.State == domain.DeliveryPending
	case domain.DeliverySettled:
		legal = current.State == domain.DeliveryAccepted
	case domain.DeliveryFailed, domain.DeliveryAborted:
		legal = current.State == domain.DeliveryPending || current.State == domain.DeliveryAccepted
	case domain.DeliveryPending:
		legal = false
	}
	if !legal {
		return current, ErrInvalidTransition
	}
	updated := current
	updated.State = target
	switch target {
	case domain.DeliveryAccepted:
		updated.AcceptedAt = &at
	case domain.DeliverySettled:
		updated.SettledAt = &at
	case domain.DeliveryFailed, domain.DeliveryAborted:
		updated.TerminalAt = &at
	}
	return updated, nil
}
