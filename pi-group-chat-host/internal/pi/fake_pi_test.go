package pi

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakePiOptions struct {
	version     string
	versionExit int
	frames      []string
	// exitAfterFrames makes the script die right after emitting its frames —
	// a Pi process that exits before agent_settled, surfacing as stdout EOF.
	// Without it the script keeps serving prompts until stdin EOF.
	exitAfterFrames bool
}

func writeFakePi(t testing.TB, options fakePiOptions) string {
	t.Helper()
	version := options.version
	if version == "" {
		version = RequiredVersion
	}

	var script strings.Builder
	script.WriteString("#!/bin/sh\n")
	script.WriteString("if [ \"${1:-}\" = \"--version\" ]; then\n")
	script.WriteString("  printf '%s\\n' \"${FAKE_PI_VERSION:-")
	script.WriteString(shellSingleQuotedValue(version))
	script.WriteString("}\"\n")
	script.WriteString("  exit \"${FAKE_PI_VERSION_EXIT:-")
	script.WriteString(intString(options.versionExit))
	script.WriteString("}\"\n")
	script.WriteString("fi\n")
	script.WriteString("if [ -n \"${FAKE_PI_ARGV_LOG:-}\" ]; then\n")
	script.WriteString("  printf '%s\\n' \"$@\" > \"$FAKE_PI_ARGV_LOG\"\n")
	script.WriteString("fi\n")
	script.WriteString("while IFS= read -r line; do\n")
	script.WriteString("  if [ -n \"${FAKE_PI_STDIN_LOG:-}\" ]; then\n")
	script.WriteString("    printf '%s\\n' \"$line\" >> \"$FAKE_PI_STDIN_LOG\"\n")
	script.WriteString("  fi\n")
	for _, frame := range options.frames {
		script.WriteString("  printf '%s\\n' '")
		script.WriteString(shellSingleQuotedValue(frame))
		script.WriteString("'\n")
	}
	if options.exitAfterFrames {
		script.WriteString("  exit 0\n")
	}
	script.WriteString("done\n")

	path := filepath.Join(t.TempDir(), "fake-pi")
	if err := os.WriteFile(path, []byte(script.String()), 0o700); err != nil {
		t.Fatalf("write fake Pi: %v", err)
	}
	return path
}

func shellSingleQuotedValue(value string) string {
	return strings.ReplaceAll(value, "'", "'\\''")
}

func intString(value int) string {
	if value == 0 {
		return "0"
	}
	if value < 0 {
		return "1"
	}
	var digits [20]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[i:])
}

func probeFakePiVersion(t testing.TB, path string, environment ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "--version")
	command.Env = append([]string(nil), environment...)
	output, err := command.Output()
	return string(output), err
}

func exchangeFakePiJSONL(t testing.TB, path string, args, environment []string, request string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, args...)
	command.Env = append([]string(nil), environment...)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("fake Pi stdin: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("fake Pi stdout: %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start fake Pi: %v", err)
	}
	if _, err := io.WriteString(stdin, request+"\n"); err != nil {
		t.Fatalf("write fake Pi JSONL: %v", err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatalf("close fake Pi stdin: %v", err)
	}
	var lines []string
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read fake Pi JSONL: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait for fake Pi: %v", err)
	}
	return lines
}
