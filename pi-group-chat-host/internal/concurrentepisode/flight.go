package concurrentepisode

import (
	"context"
	"sync"
)

// AgentRunCoordinator single-flights one Agent's complete Pi turn until
// settled. Distinct agents may run in parallel. This is the Host coordinator
// named by the system contract; TB-06 uses it without wiring interrupt.
type AgentRunCoordinator struct {
	mu     sync.Mutex
	active map[string]*flight
}

type flight struct {
	done chan struct{}
}

// NewAgentRunCoordinator builds an empty single-flight registry.
func NewAgentRunCoordinator() *AgentRunCoordinator {
	return &AgentRunCoordinator{active: make(map[string]*flight)}
}

// Run executes fn as the sole in-flight Pi turn for agentID. A second call
// for the same agent waits for the first to settle, then runs. It never lets
// two turns of one agent overlap.
func (c *AgentRunCoordinator) Run(ctx context.Context, agentID string, fn func() error) error {
	for {
		acquired, wait, err := c.tryAcquire(agentID)
		if err != nil {
			return err
		}
		if !acquired {
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		defer c.release(agentID)
		return fn()
	}
}

func (c *AgentRunCoordinator) tryAcquire(agentID string) (bool, <-chan struct{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if agentID == "" {
		return false, nil, ErrDistinctAgentsRequired
	}
	if existing, ok := c.active[agentID]; ok {
		return false, existing.done, nil
	}
	c.active[agentID] = &flight{done: make(chan struct{})}
	return true, nil, nil
}

func (c *AgentRunCoordinator) release(agentID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.active[agentID]; ok {
		delete(c.active, agentID)
		close(current.done)
	}
}

// InFlight reports whether agentID currently holds a Pi-run lease.
func (c *AgentRunCoordinator) InFlight(agentID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.active[agentID]
	return ok
}
