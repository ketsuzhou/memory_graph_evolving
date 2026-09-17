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

// NewOfferPublisher returns the Host directed-offer coordinator as the
// production Publisher. The coordinator already owns atomic Room message
// plus unique Delivery; this helper only supplies a default instance.
func NewOfferPublisher(offers *directedoffer.Coordinator) *directedoffer.Coordinator {
	if offers == nil {
		offers = directedoffer.NewCoordinator()
	}
	return offers
}

// ScriptedPublisher is a fixture Publisher. Fail, when set, is returned
// without writing. Tests use it to prove Signal / Room message / Memory
// Delivery stay all-or-none.
type ScriptedPublisher struct {
	Inner Publisher
	Fail  error

	mu      sync.Mutex
	calls   int
	results []directedoffer.Result
}

func NewScriptedPublisher(inner Publisher) *ScriptedPublisher {
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
