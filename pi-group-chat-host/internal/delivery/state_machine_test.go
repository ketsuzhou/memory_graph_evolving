package delivery

import (
	"errors"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/domain"
)

func TestTransitionRejectsIllegalDeliveryTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from domain.DeliveryState
		to   domain.DeliveryState
	}{
		{name: "pending directly to settled", from: domain.DeliveryPending, to: domain.DeliverySettled},
		{name: "settled back to accepted", from: domain.DeliverySettled, to: domain.DeliveryAccepted},
		{name: "failed back to pending", from: domain.DeliveryFailed, to: domain.DeliveryPending},
		{name: "aborted to failed", from: domain.DeliveryAborted, to: domain.DeliveryFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := domain.Delivery{ID: "delivery-1", State: tt.from}
			_, err := Transition(current, tt.to, time.Date(2026, 9, 8, 11, 0, 0, 0, time.UTC))
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("Transition(%q -> %q) error = %v, want %v", tt.from, tt.to, err, ErrInvalidTransition)
			}
		})
	}
}
