package pi

import (
	"context"
	"errors"
	"testing"
)

func TestPromptAckAcceptsAndOnlyAgentSettledSettles(t *testing.T) {
	t.Run("negative acknowledgement does not accept or settle", func(t *testing.T) {
		process := startSettlementFixture(t, []string{
			`{"id":"settlement-request","type":"response","command":"prompt","success":false,"error":"retry later"}`,
		})
		acceptance, err := process.Prompt(context.Background(), "message", func(PiEvent) error {
			t.Fatal("rejected prompt must not emit turn events")
			return nil
		})
		if !errors.Is(err, ErrPromptRejected) {
			t.Fatalf("Prompt() error = %v, want %v", err, ErrPromptRejected)
		}
		if acceptance.Success {
			t.Fatalf("rejected prompt was accepted: %+v", acceptance)
		}
	})

	t.Run("acknowledgement accepts and agent settled closes", func(t *testing.T) {
		process := startSettlementFixture(t, []string{
			`{"id":"settlement-request","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"agent_end","messages":[],"willRetry":false}`,
			`{"type":"agent_settled"}`,
		})
		settled := false
		acceptance, err := process.Prompt(context.Background(), "message", func(event PiEvent) error {
			if event.Type == "agent_settled" {
				settled = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Prompt() error = %v", err)
		}
		if !acceptance.Success || acceptance.ID != "settlement-request" {
			t.Fatalf("prompt was not accepted at acknowledgement: %+v", acceptance)
		}
		if !settled {
			t.Fatal("agent_settled did not settle the turn")
		}
	})
}

func TestAgentEndDoesNotCloseSegment(t *testing.T) {
	t.Run("negative fixture stops after willRetry false agent_end", func(t *testing.T) {
		process := startSettlementFixtureWithOptions(t, fakePiOptions{
			frames: []string{
				`{"id":"settlement-request","type":"response","command":"prompt","success":true}`,
				`{"type":"agent_end","messages":[],"willRetry":false}`,
			},
			// The Host keeps stdin open until settlement, so a Pi that stops
			// mid-turn only surfaces through process exit; the fixture must
			// die after its frames rather than serve another prompt.
			exitAfterFrames: true,
		})
		seenEnd := false
		seenSettled := false
		_, err := process.Prompt(context.Background(), "message", func(event PiEvent) error {
			seenEnd = seenEnd || event.Type == "agent_end"
			seenSettled = seenSettled || event.Type == "agent_settled"
			return nil
		})
		if !errors.Is(err, ErrProcessExited) && !errors.Is(err, ErrProtocol) {
			t.Fatalf("EOF before settlement error = %v, want process/protocol failure", err)
		}
		if !seenEnd || seenSettled {
			t.Fatalf("negative fixture events: seenEnd=%v seenSettled=%v", seenEnd, seenSettled)
		}
	})

	t.Run("retry fixture continues after willRetry true until settled", func(t *testing.T) {
		process := startSettlementFixture(t, []string{
			`{"id":"settlement-request","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"agent_end","messages":[],"willRetry":true}`,
			`{"type":"agent_start"}`,
			`{"type":"agent_end","messages":[],"willRetry":false}`,
			`{"type":"agent_settled"}`,
		})
		var retries []bool
		settled := false
		_, err := process.Prompt(context.Background(), "message", func(event PiEvent) error {
			if event.Type == "agent_end" && event.WillRetry != nil {
				retries = append(retries, *event.WillRetry)
				if settled {
					t.Fatal("agent_end observed after settlement")
				}
			}
			if event.Type == "agent_settled" {
				settled = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Prompt() error = %v", err)
		}
		if len(retries) != 2 || !retries[0] || retries[1] || !settled {
			t.Fatalf("retry sequence = %v, settled=%v", retries, settled)
		}
	})
}

func startSettlementFixture(t *testing.T, frames []string) PiProcess {
	t.Helper()
	return startSettlementFixtureWithOptions(t, fakePiOptions{frames: frames})
}

func startSettlementFixtureWithOptions(t *testing.T, options fakePiOptions) PiProcess {
	t.Helper()
	path := writeFakePi(t, options)
	launcher := NewLauncher(LauncherConfig{
		Executable: path,
		RequestID:  func() string { return "settlement-request" },
	})
	process, err := launcher.Start(context.Background(), AgentProfile{Kind: ProfileOrdinary, BuiltinToolsEnabled: true})
	if err != nil {
		t.Fatalf("start settlement fixture: %v", err)
	}
	t.Cleanup(func() { _ = process.Close() })
	return process
}
