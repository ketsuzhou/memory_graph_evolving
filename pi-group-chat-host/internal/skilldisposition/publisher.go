package skilldisposition

import (
	"context"
	"sync"

	"river2.dev/pi-group-chat-host/internal/directedoffer"
)

// Publisher writes the room-visible accepted/rejected mention and the
// unique Memory Delivery. Implementations MUST be all-or-none: a returned
// error means neither a durable message nor a Memory Delivery was kept.
type Publisher interface {
	Publish(ctx context.Context, req directedoffer.Request) (directedoffer.Result, error)
}

// OfferPublisher is the production Publisher. It delegates to the Host
// directed-offer coordinator so the mention and unique Delivery stay
// atomic with existing Room semantics.
type OfferPublisher struct {
	offers *directedoffer.Coordinator
}

func NewOfferPublisher(offers *directedoffer.Coordinator) *OfferPublisher {
	if offers == nil {
		offers = directedoffer.NewCoordinator()
	}
	return &OfferPublisher{offers: offers}
}

func (p *OfferPublisher) Publish(ctx context.Context, req directedoffer.Request) (directedoffer.Result, error) {
	return p.offers.Offer(ctx, req)
}

func (p *OfferPublisher) Coordinator() *directedoffer.Coordinator {
	return p.offers
}

// ScriptedPublisher is a fixture Publisher. Fail, when set, is returned
// without writing. Tests use it to prove Signal / Room message / Memory
// Delivery stay all-or-none.
type ScriptedPublisher struct {
	Inner *OfferPublisher
	Fail  error

	mu      sync.Mutex
	calls   int
	results []directedoffer.Result
}

func NewScriptedPublisher(inner *OfferPublisher) *ScriptedPublisher {
	if inner == nil {
		inner = NewOfferPublisher(nil)
	}
	return &ScriptedPublisher{Inner: inner}
}

func (p *ScriptedPublisher) Publish(ctx context.Context, req directedoffer.Request) (directedoffer.Result, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.Fail != nil {
		return directedoffer.Result{}, p.Fail
	}
	result, err := p.Inner.Publish(ctx, req)
	if err != nil {
		return directedoffer.Result{}, err
	}
	p.mu.Lock()
	p.results = append(p.results, result)
	p.mu.Unlock()
	return result, nil
}

func (p *ScriptedPublisher) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *ScriptedPublisher) Results() []directedoffer.Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]directedoffer.Result, len(p.results))
	copy(out, p.results)
	return out
}
