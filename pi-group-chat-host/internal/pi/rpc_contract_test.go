package pi

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRPCContract0851(t *testing.T) {
	t.Run("version gate accepts required version", func(t *testing.T) {
		path := writeFakePi(t, fakePiOptions{version: RequiredVersion})
		output, err := probeFakePiVersion(t, path)
		if err != nil || strings.TrimSpace(output) != RequiredVersion {
			t.Fatalf("invalid fake version fixture: output=%q err=%v", output, err)
		}
		launcher := NewLauncher(LauncherConfig{Executable: path})
		if err := launcher.VerifyExactVersion(context.Background(), path); err != nil {
			t.Fatalf("exact required Pi version must pass: %v", err)
		}
	})

	t.Run("prompt uses strict LF-delimited JSONL", func(t *testing.T) {
		const requestID = "request:id/opaque?yes"
		frames := []string{
			`{"id":"request:id/opaque?yes","type":"response","command":"prompt","success":true}`,
			`{"type":"agent_start"}`,
			`{"type":"agent_end","messages":[],"willRetry":false}`,
			`{"type":"agent_settled"}`,
		}
		path := writeFakePi(t, fakePiOptions{frames: frames})
		request := `{"id":"request:id/opaque?yes","type":"prompt","message":"hello"}`
		if got := exchangeFakePiJSONL(t, path, []string{"--mode", "rpc"}, nil, request); !reflect.DeepEqual(got, frames) {
			t.Fatalf("invalid fake JSONL fixture: got %#v want %#v", got, frames)
		}

		launcher := NewLauncher(LauncherConfig{
			Executable: path,
			RequestID:  func() string { return requestID },
		})
		process, err := launcher.Start(context.Background(), AgentProfile{Kind: ProfileOrdinary, BuiltinToolsEnabled: true})
		if err != nil {
			t.Fatalf("start exact-version RPC process: %v", err)
		}
		defer process.Close()
		acceptance, err := process.Prompt(context.Background(), "hello", func(PiEvent) error { return nil })
		if err != nil {
			t.Fatalf("prompt: %v", err)
		}
		if !acceptance.Success || acceptance.ID != requestID {
			t.Fatalf("unexpected acceptance: %+v", acceptance)
		}
	})
}

func TestRPCRejectsEveryOtherVersion(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		versionExit int
	}{
		{name: "0.84.2 is rejected", version: "0.84.2"},
		{name: "malformed output is rejected", version: "not-a-semver"},
		{name: "non-zero version exit is rejected", version: RequiredVersion, versionExit: 7},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeFakePi(t, fakePiOptions{version: test.version, versionExit: test.versionExit})
			launcher := NewLauncher(LauncherConfig{Executable: path})
			err := launcher.VerifyExactVersion(context.Background(), path)
			if !errors.Is(err, ErrVersionMismatch) {
				t.Fatalf("VerifyExactVersion() error = %v, want %v", err, ErrVersionMismatch)
			}
		})
	}
}

func TestRPCUsesOnlyDocumentedFlagsAndOpaqueIDs(t *testing.T) {
	tests := []struct {
		name    string
		profile AgentProfile
		want    []string
	}{
		{
			name:    "ordinary",
			profile: AgentProfile{Kind: ProfileOrdinary, BuiltinToolsEnabled: true},
			want:    []string{"--mode", "rpc", "--session-dir", "SESSION", "--provider", "PROVIDER", "--model", "MODEL", "--no-extensions", "--extension", "EXTENSION", "--no-approve"},
		},
		{
			name: "memory",
			profile: AgentProfile{Kind: ProfileMemory, AllowedToolNames: []string{
				ToolRoomSend, ToolRoomReply, ToolRoomReact, ToolMemoryStart,
				ToolMemoryExplore, ToolMemoryRedirect, ToolMemorySubmit,
			}},
			want: []string{"--mode", "rpc", "--session-dir", "SESSION", "--provider", "PROVIDER", "--model", "MODEL", "--no-builtin-tools", "--no-extensions", "--extension", "EXTENSION", "--tools", "room_send,room_reply,room_react,memory_start,memory_explore,memory_redirect,memory_submit", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-approve"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			argvLog := filepath.Join(directory, "argv.log")
			stdinLog := filepath.Join(directory, "stdin.log")
			const requestID = "opaque/host?id=not-a-sequence"
			path := writeFakePi(t, fakePiOptions{frames: []string{
				`{"id":"opaque/host?id=not-a-sequence","type":"response","command":"prompt","success":true}`,
				`{"type":"agent_end","messages":[],"willRetry":false}`,
				`{"type":"agent_settled"}`,
			}})
			launcher := NewLauncher(LauncherConfig{
				Executable: path, SessionDir: "SESSION", Provider: "PROVIDER", Model: "MODEL", ExtensionPath: "EXTENSION",
				Environment: []string{"FAKE_PI_ARGV_LOG=" + argvLog, "FAKE_PI_STDIN_LOG=" + stdinLog},
				RequestID:   func() string { return requestID },
			})
			process, err := launcher.Start(context.Background(), test.profile)
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			defer process.Close()
			if _, err := process.Prompt(context.Background(), "opaque IDs stay opaque", func(PiEvent) error { return nil }); err != nil {
				t.Fatalf("Prompt() error = %v", err)
			}

			argvBytes, err := os.ReadFile(argvLog)
			if err != nil {
				t.Fatalf("read argv log: %v", err)
			}
			gotArgs := strings.Split(strings.TrimSuffix(string(argvBytes), "\n"), "\n")
			if !reflect.DeepEqual(gotArgs, test.want) {
				t.Fatalf("argv = %#v, want documented flags %#v", gotArgs, test.want)
			}
			stdinBytes, err := os.ReadFile(stdinLog)
			if err != nil {
				t.Fatalf("read stdin log: %v", err)
			}
			var sent struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(string(stdinBytes))), &sent); err != nil {
				t.Fatalf("decode prompt request: %v", err)
			}
			if sent.ID != requestID || sent.Type != "prompt" || sent.Message != "opaque IDs stay opaque" {
				t.Fatalf("prompt request changed opaque fields: %+v", sent)
			}
		})
	}
}
