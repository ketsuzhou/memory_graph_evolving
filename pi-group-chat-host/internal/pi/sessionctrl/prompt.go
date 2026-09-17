package sessionctrl

import (
	"context"
	"encoding/json"
	"fmt"
)

// Prompt writes one Host-authored prompt onto the still-open exact session.
// It returns after the prompt acknowledgement; callers wait for WaitSettled
// (or Abort) for the turn to end. Stdin stays open so a later abort/resume
// generation can reuse the controller until Close.
func (c *Controller) Prompt(ctx context.Context, message string) error {
	id := c.nextRequestID()
	waiter := make(chan response, 1)

	c.pendingMu.Lock()
	select {
	case <-c.done:
		c.pendingMu.Unlock()
		return c.readerErr
	default:
		c.pending[id] = waiter
		c.pendingMu.Unlock()
	}

	payload, err := json.Marshal(struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Message string `json:"message"`
	}{ID: id, Type: "prompt", Message: message})
	if err != nil {
		c.removePending(id)
		return fmt.Errorf("%w: encode prompt: %v", ErrProtocol, err)
	}
	c.writeMu.Lock()
	_, writeErr := c.stdin.Write(append(payload, '\n'))
	c.writeMu.Unlock()
	if writeErr != nil {
		c.removePending(id)
		return fmt.Errorf("%w: write prompt: %v", ErrProcessExited, writeErr)
	}

	select {
	case received := <-waiter:
		if received.command != "prompt" {
			return fmt.Errorf("%w: response command %q is not a prompt acknowledgement", ErrProtocol, received.command)
		}
		if received.success == nil {
			return fmt.Errorf("%w: prompt response has no success flag", ErrProtocol)
		}
		if !*received.success {
			return fmt.Errorf("%w: prompt: %s", ErrRejected, received.error)
		}
		return nil
	case <-ctx.Done():
		c.removePending(id)
		return ctx.Err()
	case <-c.done:
		c.removePending(id)
		return c.readerErr
	}
}
