package agentrun

import (
	"context"

	"river2.dev/pi-group-chat-host/internal/concurrentepisode"
	"river2.dev/pi-group-chat-host/internal/directedoffer"
	"river2.dev/pi-group-chat-host/internal/effectsinterrupt"
)

// Coordinator composes the three Host run authorities behind one seam.
type Coordinator struct {
	flights *concurrentepisode.AgentRunCoordinator
	offers  *directedoffer.Coordinator
	effects *effectsinterrupt.Coordinator
}

// New builds an empty composition. Policy is forwarded to the effects
// interrupt coordinator; a zero Policy uses that package's defaults.
func New(policy effectsinterrupt.Policy) *Coordinator {
	return &Coordinator{
		flights: concurrentepisode.NewAgentRunCoordinator(),
		offers:  directedoffer.NewCoordinator(),
		effects: effectsinterrupt.NewCoordinator(policy, nil),
	}
}

func (c *Coordinator) Flights() *concurrentepisode.AgentRunCoordinator {
	return c.flights
}

func (c *Coordinator) Offers() *directedoffer.Coordinator {
	return c.offers
}

func (c *Coordinator) Effects() *effectsinterrupt.Coordinator {
	return c.effects
}

// AttachOffer binds one exact task session through the directed-offer
// coordinator. Missing §4.2 pins fail closed.
func (c *Coordinator) AttachOffer(binding directedoffer.Binding, session directedoffer.ExactSession) error {
	return c.offers.Attach(binding, session)
}

// Run single-flights one Agent's complete Pi turn until settled.
func (c *Coordinator) Run(ctx context.Context, agentID string, fn func() error) error {
	return c.flights.Run(ctx, agentID, fn)
}

// Offer publishes one Room message and, when the target session is
// running, joins the directed-offer abort/resume cycle.
func (c *Coordinator) Offer(ctx context.Context, req directedoffer.Request) (directedoffer.Result, error) {
	return c.offers.Offer(ctx, req)
}
