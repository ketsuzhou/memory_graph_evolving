package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"river2.dev/pi-group-chat-host/internal/ports"
)

type PromptAcceptance = ports.PromptAcceptance
type PiEvent = ports.PiEvent
type PiProcess = ports.PiProcess

// Event types emitted on the Pi stdout stream. The prompt acknowledgement is a
// response frame; only agent_settled terminates a turn.
const (
	eventResponse       = "response"
	eventPromptAccepted = "prompt_accepted"
	eventAgentStart     = "agent_start"
	eventAgentEnd       = "agent_end"
	eventAgentSettled   = "agent_settled"
	eventToolStart      = "tool_execution_start"
	eventToolEnd        = "tool_execution_end"
	eventError          = "error"
)

type rpcFrame struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Command    string          `json:"command"`
	Success    *bool           `json:"success"`
	Error      string          `json:"error"`
	Message    flexibleString  `json:"message"`
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Args       json.RawMessage `json:"args"`
	Result     json.RawMessage `json:"result"`
	IsError    *bool           `json:"isError"`
	WillRetry  *bool           `json:"willRetry"`
}

// flexibleString accepts a string value and tolerates the object-valued
// `message` field real Pi emits on message_start/message_end/message_update
// frames. Only error frames carry a Host-readable string message; the object
// form carries turn content the Host does not consume, so it decodes to "".
type flexibleString string

func (f *flexibleString) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return json.Unmarshal(data, (*string)(f))
	}
	*f = ""
	return nil
}

type Process struct {
	command   *exec.Cmd
	stdin     io.WriteCloser
	scanner   *bufio.Scanner
	requestID func() string
	closed    bool
}

func newLineScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	return scanner
}

// Prompt writes one strict LF-delimited JSONL prompt request and reads frames
// until settlement. A negative acknowledgement returns ErrPromptRejected
// without emitting turn events; EOF before agent_settled returns
// ErrProcessExited; malformed frames return ErrProtocol.
func (p *Process) Prompt(ctx context.Context, message string, handle func(PiEvent) error) (PromptAcceptance, error) {
	requestID := p.requestID()
	frame, err := json.Marshal(promptRequest{ID: requestID, Type: "prompt", Message: message})
	if err != nil {
		return PromptAcceptance{}, fmt.Errorf("encode prompt: %w", err)
	}
	if _, err := p.stdin.Write(append(frame, '\n')); err != nil {
		return PromptAcceptance{}, fmt.Errorf("%w: write prompt: %v", ErrProcessExited, err)
	}
	// Keep stdin open until settlement: a real Pi RPC process treats stdin EOF
	// as an immediate shutdown and would exit after echoing the prompt without
	// running the turn. Closing happens once the read loop ends, so a Pi that
	// dies before agent_settled still surfaces through stdout EOF below.
	defer func() { _ = p.stdin.Close() }()


	accepted := false
	acceptedAt := time.Time{}
	for p.scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return PromptAcceptance{}, fmt.Errorf("%w: %v", ErrCancelled, err)
		}
		line := p.scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var decoded rpcFrame
		if err := json.Unmarshal(line, &decoded); err != nil {
			return PromptAcceptance{}, fmt.Errorf("%w: malformed frame %q: %v", ErrProtocol, string(line), err)
		}
		switch decoded.Type {
		case eventResponse:
			if decoded.ID != requestID {
				return PromptAcceptance{}, fmt.Errorf("%w: response id %q does not match prompt %q", ErrProtocol, decoded.ID, requestID)
			}
			if decoded.Command != "prompt" {
				return PromptAcceptance{}, fmt.Errorf("%w: response command %q is not a prompt acknowledgement", ErrProtocol, decoded.Command)
			}
			if decoded.Success == nil {
				return PromptAcceptance{}, fmt.Errorf("%w: response without success flag", ErrProtocol)
			}
			if accepted {
				return PromptAcceptance{}, fmt.Errorf("%w: duplicate prompt acknowledgement", ErrProtocol)
			}
			if !*decoded.Success {
				return PromptAcceptance{ID: requestID, Success: false, AcceptedAt: time.Now()}, fmt.Errorf("%w: %s", ErrPromptRejected, decoded.Error)
			}
			accepted = true
			acceptedAt = time.Now()
			// The acknowledgement is a durable boundary for the caller: it
			// must be able to persist acceptance before any turn events flow.
			if err := handle(PiEvent{Type: eventPromptAccepted}); err != nil {
				return PromptAcceptance{}, err
			}
		case eventAgentSettled:
			if !accepted {
				return PromptAcceptance{}, fmt.Errorf("%w: agent_settled without prompt acknowledgement", ErrProtocol)
			}
			if err := handle(PiEvent{Type: eventAgentSettled}); err != nil {
				return PromptAcceptance{}, err
			}
			return PromptAcceptance{ID: requestID, Success: true, AcceptedAt: acceptedAt}, nil
		case eventAgentStart:
			if err := handle(PiEvent{Type: eventAgentStart}); err != nil {
				return PromptAcceptance{}, err
			}
		case eventAgentEnd:
			if err := handle(PiEvent{Type: eventAgentEnd, WillRetry: decoded.WillRetry}); err != nil {
				return PromptAcceptance{}, err
			}
		case eventToolStart:
			if err := handle(PiEvent{Type: eventToolStart, ToolCallID: decoded.ToolCallID, ToolName: decoded.ToolName, Content: string(decoded.Args)}); err != nil {
				return PromptAcceptance{}, err
			}
		case eventToolEnd:
			if err := handle(PiEvent{Type: eventToolEnd, ToolCallID: decoded.ToolCallID, ToolName: decoded.ToolName, Result: string(decoded.Result), IsError: decoded.IsError}); err != nil {
				return PromptAcceptance{}, err
			}
		case eventError:
			return PromptAcceptance{}, fmt.Errorf("%w: %s", ErrProtocol, decoded.Message)
		default:
			// Unknown informative frames are ignored; they can never settle a turn.
		}
	}
	if err := p.scanner.Err(); err != nil {
		return PromptAcceptance{}, fmt.Errorf("%w: %v", ErrProcessExited, err)
	}
	// A context expiry kills the child (CommandContext), which surfaces as
	// stdout EOF here. Reporting that as a process exit would misclassify the
	// Host's own timeout kill as a Pi crash; the context check restores the
	// distinction the failure classification and retry policy rely on.
	if err := ctx.Err(); err != nil {
		return PromptAcceptance{}, fmt.Errorf("%w: %v", ErrCancelled, err)
	}
	return PromptAcceptance{}, fmt.Errorf("%w: stdout closed before agent_settled", ErrProcessExited)
}

func (p *Process) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	if err := p.stdin.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return p.command.Wait()
}

var _ ports.PiProcess = (*Process)(nil)
