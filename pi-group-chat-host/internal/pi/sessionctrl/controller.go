// Package sessionctrl controls one exact, persistent Pi RPC session.
package sessionctrl

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrProcessExited = errors.New("pi session controller: process exited")
	ErrProtocol      = errors.New("pi session controller: protocol error")
	ErrRejected      = errors.New("pi session controller: command rejected")
)

// Session pins a Host binding to one persisted Pi session. File is passed
// verbatim to Pi's --session flag; ID is the corresponding exact Pi session ID
// recorded by the Host, never a prefix selected by Pi interactively.
type Session struct {
	File string
	ID   string
}

func (s Session) Validate() error {
	if s.File == "" {
		return errors.New("pi session controller: exact session file is required")
	}
	if s.ID == "" {
		return errors.New("pi session controller: exact session ID is required")
	}
	return nil
}

// Event is an informative Pi stdout frame. Raw preserves the complete JSON
// frame so later coordinators can consume newly introduced Pi event fields
// without adding another stdout reader.
type Event struct {
	Type string
	Raw  json.RawMessage
}

// State is Pi's get_state response. Raw preserves fields not yet needed by the
// Host while the named fields support safe interrupt decisions.
type State struct {
	SessionFile         string          `json:"sessionFile"`
	SessionID           string          `json:"sessionId"`
	IsStreaming         bool            `json:"isStreaming"`
	IsCompacting        bool            `json:"isCompacting"`
	PendingMessageCount int             `json:"pendingMessageCount"`
	Raw                 json.RawMessage `json:"-"`
}

// Config binds the controller to already-created process pipes. Exactly one
// controller must be constructed for stdout: it owns all stdout reads and
// demultiplexes responses and events for its callers.
type Config struct {
	Session    Session
	Generation uint64
	Stdin      io.WriteCloser
	Stdout     io.Reader
	Close      func() error
	RequestID  func() string
}

type Controller struct {
	session    Session
	generation uint64
	stdin      io.WriteCloser
	close      func() error
	requestID  func() string

	requestSequence atomic.Uint64
	settled         atomic.Bool
	settledOnce     sync.Once
	settledCh       chan struct{}
	done            chan struct{}

	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]chan response

	// readerErr is written immediately before done is closed. Every reader of
	// it observes done first, which supplies the required happens-before edge.
	readerErr error

	events     chan Event
	eventMu    sync.Mutex
	eventCV    *sync.Cond
	eventQ     []Event
	readerDone bool

	closeOnce sync.Once
	closeErr  error
}

type response struct {
	command string
	success *bool
	error   string
	data    json.RawMessage
}

type frame struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Command string          `json:"command"`
	Success *bool           `json:"success"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

type command struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// New starts the controller's sole stdout reader and an event publisher.
func New(config Config) (*Controller, error) {
	if err := config.Session.Validate(); err != nil {
		return nil, err
	}
	if config.Stdin == nil {
		return nil, errors.New("pi session controller: stdin is required")
	}
	if config.Stdout == nil {
		return nil, errors.New("pi session controller: stdout is required")
	}
	if config.Close == nil {
		config.Close = config.Stdin.Close
	}
	if config.RequestID == nil {
		config.RequestID = func() string { return "rpc" }
	}
	controller := &Controller{
		session: config.Session, generation: config.Generation, stdin: config.Stdin,
		close: config.Close, requestID: config.RequestID,
		settledCh: make(chan struct{}), done: make(chan struct{}),
		pending: make(map[string]chan response), events: make(chan Event),
	}
	controller.eventCV = sync.NewCond(&controller.eventMu)
	go controller.readLoop(config.Stdout)
	go controller.publishEvents()
	return controller, nil
}

func (c *Controller) SessionFile() string       { return c.session.File }
func (c *Controller) SessionID() string         { return c.session.ID }
func (c *Controller) ProcessGeneration() uint64 { return c.generation }
func (c *Controller) Settled() bool             { return c.settled.Load() }
func (c *Controller) Events() <-chan Event      { return c.events }

// GetState obtains Pi's current state while the agent may still be running.
func (c *Controller) GetState(ctx context.Context) (State, error) {
	response, err := c.send(ctx, "get_state")
	if err != nil {
		return State{}, err
	}
	var state State
	if err := json.Unmarshal(response.data, &state); err != nil {
		return State{}, fmt.Errorf("%w: decode get_state data: %v", ErrProtocol, err)
	}
	state.Raw = append(state.Raw[:0], response.data...)
	return state, nil
}

// ClearQueue discards Pi's pending steering and follow-up work while the
// current agent operation continues. It is intentionally explicit: callers
// must choose whether queue removal is safe before sending it.
func (c *Controller) ClearQueue(ctx context.Context) error {
	_, err := c.send(ctx, "clear_queue")
	return err
}

// Abort asks Pi to stop its current agent operation. It does not itself mean
// that the session settled; callers must wait for WaitSettled.
func (c *Controller) Abort(ctx context.Context) error {
	_, err := c.send(ctx, "abort")
	return err
}

// WaitSettled waits for Pi's agent_settled event, not merely a successful
// abort response or agent_end event.
func (c *Controller) WaitSettled(ctx context.Context) error {
	if c.Settled() {
		return nil
	}
	select {
	case <-c.settledCh:
		return nil
	case <-c.done:
		if c.Settled() {
			return nil
		}
		return c.readerErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close releases the process supplied in Config. It is idempotent and does
// not alter legacy Process.Prompt's one-shot stdin-close behavior.
func (c *Controller) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.close() })
	return c.closeErr
}

func (c *Controller) send(ctx context.Context, name string) (response, error) {
	id := c.nextRequestID()
	waiter := make(chan response, 1)

	c.pendingMu.Lock()
	select {
	case <-c.done:
		c.pendingMu.Unlock()
		return response{}, c.readerErr
	default:
		c.pending[id] = waiter
		c.pendingMu.Unlock()
	}

	payload, err := json.Marshal(command{ID: id, Type: name})
	if err != nil {
		c.removePending(id)
		return response{}, fmt.Errorf("%w: encode %s: %v", ErrProtocol, name, err)
	}
	c.writeMu.Lock()
	_, writeErr := c.stdin.Write(append(payload, '\n'))
	c.writeMu.Unlock()
	if writeErr != nil {
		c.removePending(id)
		return response{}, fmt.Errorf("%w: write %s: %v", ErrProcessExited, name, writeErr)
	}

	select {
	case received := <-waiter:
		if received.command != name {
			return response{}, fmt.Errorf("%w: response command %q does not match %q", ErrProtocol, received.command, name)
		}
		if received.success == nil {
			return response{}, fmt.Errorf("%w: %s response has no success flag", ErrProtocol, name)
		}
		if !*received.success {
			return response{}, fmt.Errorf("%w: %s: %s", ErrRejected, name, received.error)
		}
		return received, nil
	case <-ctx.Done():
		c.removePending(id)
		return response{}, ctx.Err()
	case <-c.done:
		c.removePending(id)
		return response{}, c.readerErr
	}
}

func (c *Controller) nextRequestID() string {
	base := strings.TrimSpace(c.requestID())
	if base == "" {
		base = "rpc"
	}
	return fmt.Sprintf("%s-%d", base, c.requestSequence.Add(1))
}

func (c *Controller) removePending(id string) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}

func (c *Controller) readLoop(stdout io.Reader) {
	var terminal error
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytesTrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var decoded frame
		if err := json.Unmarshal(line, &decoded); err != nil {
			terminal = fmt.Errorf("%w: malformed stdout frame: %v", ErrProtocol, err)
			break
		}
		if decoded.Type == "response" {
			c.deliverResponse(decoded)
			continue
		}
		raw := append(json.RawMessage(nil), line...)
		if decoded.Type == "agent_settled" {
			c.settledOnce.Do(func() {
				c.settled.Store(true)
				close(c.settledCh)
			})
		}
		c.enqueueEvent(Event{Type: decoded.Type, Raw: raw})
	}
	if terminal == nil {
		if err := scanner.Err(); err != nil {
			terminal = fmt.Errorf("%w: stdout: %v", ErrProcessExited, err)
		} else {
			terminal = ErrProcessExited
		}
	}
	c.pendingMu.Lock()
	for id := range c.pending {
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
	c.readerErr = terminal
	close(c.done)

	c.eventMu.Lock()
	c.readerDone = true
	c.eventCV.Broadcast()
	c.eventMu.Unlock()
}

func (c *Controller) deliverResponse(frame frame) {
	c.pendingMu.Lock()
	waiter, found := c.pending[frame.ID]
	if found {
		delete(c.pending, frame.ID)
	}
	c.pendingMu.Unlock()
	if found {
		waiter <- response{command: frame.Command, success: frame.Success, error: frame.Error, data: append(json.RawMessage(nil), frame.Data...)}
	}
}

func (c *Controller) enqueueEvent(event Event) {
	c.eventMu.Lock()
	c.eventQ = append(c.eventQ, event)
	c.eventCV.Signal()
	c.eventMu.Unlock()
}

func (c *Controller) publishEvents() {
	for {
		c.eventMu.Lock()
		for len(c.eventQ) == 0 && !c.readerDone {
			c.eventCV.Wait()
		}
		if len(c.eventQ) == 0 && c.readerDone {
			c.eventMu.Unlock()
			close(c.events)
			return
		}
		event := c.eventQ[0]
		c.eventQ = c.eventQ[1:]
		c.eventMu.Unlock()
		c.events <- event
	}
}

func bytesTrimSpace(value []byte) []byte {
	return []byte(strings.TrimSpace(string(value)))
}
