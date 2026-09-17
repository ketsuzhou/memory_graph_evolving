package sessionctrl

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type guardedReader struct {
	reader io.Reader
	active atomic.Int32
	max    atomic.Int32
}

func (r *guardedReader) Read(buffer []byte) (int, error) {
	active := r.active.Add(1)
	for {
		current := r.max.Load()
		if active <= current || r.max.CompareAndSwap(current, active) {
			break
		}
	}
	defer r.active.Add(-1)
	return r.reader.Read(buffer)
}

type testTransport struct {
	stdinReader  *io.PipeReader
	stdinWriter  *io.PipeWriter
	stdoutReader *io.PipeReader
	stdoutWriter *io.PipeWriter
}

func newTestTransport() testTransport {
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	return testTransport{stdinReader, stdinWriter, stdoutReader, stdoutWriter}
}

func newController(t *testing.T, transport testTransport, stdout io.Reader) *Controller {
	t.Helper()
	controller, err := New(Config{
		Session:    Session{File: "/sessions/exact.jsonl", ID: "exact-session-id"},
		Generation: 7,
		Stdin:      transport.stdinWriter,
		Stdout:     stdout,
		Close: func() error {
			return transport.stdinWriter.Close()
		},
		RequestID: func() string { return "control" },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return controller
}

func TestControllerControlsExactRunningSessionWithOneStdoutReader(t *testing.T) {
	transport := newTestTransport()
	guarded := &guardedReader{reader: transport.stdoutReader}
	controller := newController(t, transport, guarded)

	requests := make(chan command, 3)
	go func() {
		scanner := bufio.NewScanner(transport.stdinReader)
		for scanner.Scan() {
			var request command
			if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
				t.Errorf("decode command: %v", err)
				return
			}
			requests <- request
		}
	}()

	events := make(chan Event, 2)
	go func() {
		for event := range controller.Events() {
			events <- event
		}
		close(events)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stateResult := make(chan State, 1)
	errorResult := make(chan error, 3)
	go func() { state, err := controller.GetState(ctx); stateResult <- state; errorResult <- err }()
	go func() { errorResult <- controller.ClearQueue(ctx) }()
	go func() { errorResult <- controller.Abort(ctx) }()

	seen := map[string]command{}
	for len(seen) < 3 {
		request := <-requests
		seen[request.Type] = request
	}
	if _, err := io.WriteString(transport.stdoutWriter, "{\"type\":\"agent_start\"}\n"); err != nil {
		t.Fatalf("write event: %v", err)
	}
	for _, name := range []string{"abort", "clear_queue", "get_state"} {
		request := seen[name]
		data := ""
		if name == "get_state" {
			data = `,"data":{"sessionFile":"/sessions/exact.jsonl","sessionId":"exact-session-id","isStreaming":true,"pendingMessageCount":2}`
		}
		frame := `{"id":"` + request.ID + `","type":"response","command":"` + name + `","success":true` + data + "}\n"
		if _, err := io.WriteString(transport.stdoutWriter, frame); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}
	if _, err := io.WriteString(transport.stdoutWriter, "{\"type\":\"agent_settled\"}\n"); err != nil {
		t.Fatalf("write settled event: %v", err)
	}

	for range 3 {
		if err := <-errorResult; err != nil {
			t.Fatalf("control command error = %v", err)
		}
	}
	state := <-stateResult
	if state.SessionID != "exact-session-id" || !state.IsStreaming || state.PendingMessageCount != 2 {
		t.Fatalf("GetState() = %+v", state)
	}
	if err := controller.WaitSettled(ctx); err != nil {
		t.Fatalf("WaitSettled() error = %v", err)
	}
	if !controller.Settled() {
		t.Fatal("Settled() = false after agent_settled")
	}
	if controller.SessionFile() != "/sessions/exact.jsonl" || controller.SessionID() != "exact-session-id" || controller.ProcessGeneration() != 7 {
		t.Fatalf("exact session metadata = file=%q id=%q generation=%d", controller.SessionFile(), controller.SessionID(), controller.ProcessGeneration())
	}
	if guarded.max.Load() != 1 {
		t.Fatalf("stdout had %d simultaneous readers, want exactly one", guarded.max.Load())
	}
	if event := <-events; event.Type != "agent_start" {
		t.Fatalf("first event = %q, want agent_start", event.Type)
	}
	if event := <-events; event.Type != "agent_settled" {
		t.Fatalf("second event = %q, want agent_settled", event.Type)
	}
	if err := transport.stdoutWriter.Close(); err != nil {
		t.Fatalf("close fake stdout: %v", err)
	}
	if err := controller.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestControllerConcurrentRPCResponseCorrelationAndEventStreamRaceFree(t *testing.T) {
	transport := newTestTransport()
	controller := newController(t, transport, transport.stdoutReader)

	const commands = 96
	go func() {
		scanner := bufio.NewScanner(transport.stdinReader)
		encoder := json.NewEncoder(transport.stdoutWriter)
		for scanner.Scan() {
			var request command
			if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
				t.Errorf("decode command: %v", err)
				return
			}
			if err := encoder.Encode(map[string]any{"type": "queue_update", "request": request.ID}); err != nil {
				t.Errorf("write event: %v", err)
				return
			}
			data := any(nil)
			if request.Type == "get_state" {
				data = map[string]any{"sessionId": request.ID, "isStreaming": true}
			}
			if err := encoder.Encode(map[string]any{"id": request.ID, "type": "response", "command": request.Type, "success": true, "data": data}); err != nil {
				t.Errorf("write response: %v", err)
				return
			}
		}
	}()

	var received atomic.Int32
	eventsDone := make(chan struct{})
	go func() {
		for range controller.Events() {
			received.Add(1)
		}
		close(eventsDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls sync.WaitGroup
	errs := make(chan error, commands)
	for i := 0; i < commands; i++ {
		calls.Add(1)
		go func(index int) {
			defer calls.Done()
			switch index % 3 {
			case 0:
				state, err := controller.GetState(ctx)
				if err == nil && state.SessionID == "" {
					err = ErrProtocol
				}
				errs <- err
			case 1:
				errs <- controller.ClearQueue(ctx)
			default:
				errs <- controller.Abort(ctx)
			}
		}(i)
	}
	calls.Wait()
	for range commands {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent RPC error = %v", err)
		}
	}
	if err := transport.stdinWriter.Close(); err != nil {
		t.Fatalf("close fake stdin: %v", err)
	}
	if err := transport.stdoutWriter.Close(); err != nil {
		t.Fatalf("close fake stdout: %v", err)
	}
	select {
	case <-eventsDone:
	case <-ctx.Done():
		t.Fatal("event stream did not finish")
	}
	if got := received.Load(); got != commands {
		t.Fatalf("events received = %d, want %d", got, commands)
	}
}

func TestNewRejectsIncompleteExactSession(t *testing.T) {
	transport := newTestTransport()
	_, err := New(Config{Session: Session{File: "/sessions/exact.jsonl"}, Stdin: transport.stdinWriter, Stdout: transport.stdoutReader})
	if err == nil {
		t.Fatal("New() succeeded without exact session ID")
	}
	_, err = New(Config{Session: Session{ID: "exact-session-id"}, Stdin: transport.stdinWriter, Stdout: transport.stdoutReader})
	if err == nil {
		t.Fatal("New() succeeded without exact session file")
	}
}

func TestWaitSettledRequiresAgentSettledNotAbortResponse(t *testing.T) {
	transport := newTestTransport()
	controller := newController(t, transport, transport.stdoutReader)
	t.Cleanup(func() {
		_ = transport.stdoutWriter.Close()
		_ = controller.Close()
	})

	go func() {
		for range controller.Events() {
		}
	}()

	requests := make(chan command, 1)
	go func() {
		scanner := bufio.NewScanner(transport.stdinReader)
		if !scanner.Scan() {
			t.Errorf("expected abort command: %v", scanner.Err())
			return
		}
		var request command
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			t.Errorf("decode command: %v", err)
			return
		}
		requests <- request
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errorResult := make(chan error, 1)
	go func() { errorResult <- controller.Abort(ctx) }()

	request := <-requests
	if request.Type != "abort" {
		t.Fatalf("command = %q, want abort", request.Type)
	}
	if _, err := io.WriteString(transport.stdoutWriter, `{"type":"agent_end","messages":[],"willRetry":false}`+"\n"); err != nil {
		t.Fatalf("write agent_end: %v", err)
	}
	frame := `{"id":"` + request.ID + `","type":"response","command":"abort","success":true}` + "\n"
	if _, err := io.WriteString(transport.stdoutWriter, frame); err != nil {
		t.Fatalf("write abort response: %v", err)
	}
	if err := <-errorResult; err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if controller.Settled() {
		t.Fatal("Settled() = true after abort response and agent_end")
	}

	select {
	case err := <-func() chan error {
		done := make(chan error, 1)
		go func() { done <- controller.WaitSettled(ctx) }()
		return done
	}():
		t.Fatalf("WaitSettled() returned before agent_settled: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	if _, err := io.WriteString(transport.stdoutWriter, `{"type":"agent_settled"}`+"\n"); err != nil {
		t.Fatalf("write settled event: %v", err)
	}
	if err := controller.WaitSettled(ctx); err != nil {
		t.Fatalf("WaitSettled() error = %v", err)
	}
	if !controller.Settled() {
		t.Fatal("Settled() = false after agent_settled")
	}
}
