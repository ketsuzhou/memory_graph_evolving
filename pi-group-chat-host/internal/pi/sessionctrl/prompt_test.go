package sessionctrl

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func TestControllerPromptAcknowledgesWithoutClosingStdin(t *testing.T) {
	transport := newTestTransport()
	controller := newController(t, transport, transport.stdoutReader)
	requests := make(chan map[string]any, 1)
	go func() {
		scanner := bufio.NewScanner(transport.stdinReader)
		if !scanner.Scan() {
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &payload); err != nil {
			t.Errorf("decode prompt: %v", err)
			return
		}
		requests <- payload
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- controller.Prompt(ctx, "solve the task") }()

	payload := <-requests
	if payload["type"] != "prompt" || payload["message"] != "solve the task" {
		t.Fatalf("prompt frame = %#v", payload)
	}
	id, _ := payload["id"].(string)
	if _, err := io.WriteString(transport.stdoutWriter, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`+"\n"); err != nil {
		t.Fatalf("write prompt ack: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if controller.Settled() {
		t.Fatal("Prompt acknowledgement must not settle the session")
	}
	if err := transport.stdoutWriter.Close(); err != nil {
		t.Fatalf("close stdout: %v", err)
	}
	if err := controller.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
