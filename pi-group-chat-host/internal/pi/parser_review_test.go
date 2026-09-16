package pi

import (
	"context"
	"errors"
	"testing"
)

// TestResponseFramesAreStrictPromptAcks proves the decoder rejects a response
// whose command is not the prompt acknowledgement and any second
// acknowledgement after the first — wrong command and duplicate acks are
// protocol errors, not silent accepts.
func TestResponseFramesAreStrictPromptAcks(t *testing.T) {
	t.Run("wrong command is a protocol error", func(t *testing.T) {
		process := startSettlementFixture(t, []string{
			`{"id":"settlement-request","type":"response","command":"other","success":true}`,
		})
		_, err := process.Prompt(context.Background(), "message", func(PiEvent) error { return nil })
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("Prompt() error = %v, want %v", err, ErrProtocol)
		}
	})
	t.Run("duplicate acknowledgement is a protocol error", func(t *testing.T) {
		process := startSettlementFixture(t, []string{
			`{"id":"settlement-request","type":"response","command":"prompt","success":true}`,
			`{"id":"settlement-request","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_settled"}`,
		})
		_, err := process.Prompt(context.Background(), "message", func(PiEvent) error { return nil })
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("Prompt() error = %v, want %v", err, ErrProtocol)
		}
	})
	t.Run("acknowledgement is surfaced as a prompt_accepted event", func(t *testing.T) {
		process := startSettlementFixture(t, []string{
			`{"id":"settlement-request","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"agent_settled"}`,
		})
		acceptedSeen := false
		settledSeen := false
		_, err := process.Prompt(context.Background(), "message", func(event PiEvent) error {
			switch event.Type {
			case "prompt_accepted":
				acceptedSeen = true
			case "agent_settled":
				settledSeen = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Prompt() error = %v", err)
		}
		if !acceptedSeen || !settledSeen {
			t.Fatalf("events: prompt_accepted=%v agent_settled=%v, want both", acceptedSeen, settledSeen)
		}
	})
}

// TestToolEndCarriesResultEnvelope proves the completion frame's result and
// isError fields reach the callback so callers can gate side effects.
func TestToolEndCarriesResultEnvelope(t *testing.T) {
	process := startSettlementFixture(t, []string{
		`{"id":"settlement-request","type":"response","command":"prompt","success":true}`,
		`{"type":"tool_execution_start","toolCallId":"call-1","toolName":"room_reply","args":{"content":"hi"}}`,
		`{"type":"tool_execution_end","toolCallId":"call-1","toolName":"room_reply","result":{"ok":true},"isError":false}`,
		`{"type":"agent_settled"}`,
	})
	var endEvent PiEvent
	_, err := process.Prompt(context.Background(), "message", func(event PiEvent) error {
		if event.Type == "tool_execution_end" {
			endEvent = event
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if endEvent.IsError == nil || *endEvent.IsError {
		t.Fatalf("end event isError = %v, want false", endEvent.IsError)
	}
	if endEvent.Result == "" {
		t.Fatal("end event lost the result envelope")
	}
	if endEvent.ToolCallID != "call-1" || endEvent.ToolName != "room_reply" {
		t.Fatalf("end event identity = %q/%q", endEvent.ToolCallID, endEvent.ToolName)
	}
}

// TestVersionParsingRejectsPaddedOutput proves version gate no longer accepts
// arbitrary text that merely ends with the locked version.
func TestVersionParsingRejectsPaddedOutput(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantErr bool
	}{
		{name: "bare version passes", version: RequiredVersion},
		{name: "prefixed binary name passes", version: "pi " + RequiredVersion},
		{name: "evil prefix is rejected", version: "evil prefix " + RequiredVersion, wantErr: true},
		{name: "trailing build metadata is rejected", version: RequiredVersion + "+build", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeFakePi(t, fakePiOptions{version: test.version})
			launcher := NewLauncher(LauncherConfig{Executable: path})
			err := launcher.VerifyExactVersion(context.Background(), path)
			if test.wantErr && !errors.Is(err, ErrVersionMismatch) {
				t.Fatalf("VerifyExactVersion() error = %v, want %v", err, ErrVersionMismatch)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("VerifyExactVersion() error = %v, want nil", err)
			}
		})
	}
}
