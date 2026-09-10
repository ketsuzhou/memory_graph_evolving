package pi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"river2.dev/pi-group-chat-host/internal/domain"
	"river2.dev/pi-group-chat-host/internal/ports"
)

const RequiredVersion = "0.85.1"

type LauncherConfig struct {
	Executable    string
	SessionDir    string
	Provider      string
	Model         string
	ExtensionPath string
	Environment   []string
	RequestID     func() string
	// Stderr, when non-nil, receives everything the Pi process writes to
	// stderr. A Pi that dies mid-turn is otherwise undiagnosable: its exit
	// reason only ever appears on this stream.
	Stderr io.Writer
}

type Launcher struct {
	config LauncherConfig
}

func NewLauncher(config LauncherConfig) *Launcher {
	if config.RequestID == nil {
		config.RequestID = randomRequestID
	}
	return &Launcher{config: config}
}

func randomRequestID() string {
	var buffer [12]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return fmt.Sprintf("prompt-%d", os.Getpid())
	}
	return "prompt-" + hex.EncodeToString(buffer[:])
}

// VerifyExactVersion runs the documented `pi --version` preflight and accepts
// exactly the locked version, nothing else. Any other version, malformed
// output, or failed exit is ErrVersionMismatch.
func (l *Launcher) VerifyExactVersion(ctx context.Context, executable string) error {
	command := exec.CommandContext(ctx, executable, "--version")
	command.Env = append([]string(nil), l.config.Environment...)
	var stdout bytes.Buffer
	command.Stdout = &stdout
	if err := command.Run(); err != nil {
		return fmt.Errorf("%w: %s --version failed: %v", ErrVersionMismatch, executable, err)
	}
	reported := parseVersionOutput(stdout.String())
	if reported != RequiredVersion {
		return fmt.Errorf("%w: %s reports %q, want exactly %q", ErrVersionMismatch, executable, reported, RequiredVersion)
	}
	return nil
}

// parseVersionOutput accepts only the two well-formed spellings of the
// locked version — bare "0.85.1" or "pi 0.85.1". Any other prefix, suffix, or
// embedded text stays a version mismatch instead of being masked by taking
// the last whitespace field.
func parseVersionOutput(output string) string {
	reported := strings.TrimSpace(output)
	if reported == "" {
		return ""
	}
	fields := strings.Fields(reported)
	switch len(fields) {
	case 1:
		return fields[0]
	case 2:
		if fields[0] != "pi" {
			return ""
		}
		return fields[1]
	default:
		return ""
	}
}

// Start verifies the version gate and then launches one Pi RPC child process
// for the profile, using only documented flags and a server-owned environment.
func (l *Launcher) Start(ctx context.Context, profile domain.AgentProfile) (ports.PiProcess, error) {
	if err := l.VerifyExactVersion(ctx, l.config.Executable); err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, l.config.Executable, l.argv(profile)...)
	command.Env = append([]string(nil), l.config.Environment...)
	if profile.WorkingDirectory != "" {
		if err := os.MkdirAll(profile.WorkingDirectory, 0o755); err == nil {
			command.Dir = profile.WorkingDirectory
		}
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdin pipe: %w", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdout pipe: %w", err)
	}
	command.Stderr = l.config.Stderr
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("pi start: %w", err)
	}
	return &Process{
		command:   command,
		stdin:     stdin,
		requestID: l.config.RequestID,
		scanner:   newLineScanner(stdout),
	}, nil
}

// argv builds the documented flag sequence. Ordinary keeps Pi's full coding
// tool surface; the Memory Agent additionally disables built-in tools, skills,
// prompt templates, and context files, and restricts --tools to the fixed
// Room + Memory surface.
func (l *Launcher) argv(profile domain.AgentProfile) []string {
	memoryProfile := profile.Kind == domain.ProfileMemory || !profile.BuiltinToolsEnabled
	args := []string{"--mode", "rpc", "--session-dir", l.config.SessionDir, "--provider", l.config.Provider, "--model", l.config.Model}
	if memoryProfile {
		args = append(args, "--no-builtin-tools")
	}
	args = append(args, "--no-extensions")
	if l.config.ExtensionPath != "" {
		args = append(args, "--extension", l.config.ExtensionPath)
	}
	if memoryProfile {
		tools := profile.AllowedToolNames
		if len(tools) == 0 {
			tools = MemoryAgentToolSurface()
		}
		args = append(args, "--tools", strings.Join(tools, ","), "--no-skills", "--no-prompt-templates", "--no-context-files")
	}
	args = append(args, "--no-approve")
	return args
}

var _ ports.PiLauncher = (*Launcher)(nil)

// promptRequest is the exact stdin frame shape; JSON field order follows the
// struct so the wire form is stable.
type promptRequest struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Message string `json:"message"`
}
