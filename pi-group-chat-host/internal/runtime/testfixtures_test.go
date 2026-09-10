package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFakePi(t *testing.T, promptCases map[string][]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-pi")
	var script strings.Builder
	script.WriteString("#!/bin/sh\n")
	script.WriteString("if [ \"${1:-}\" = \"--version\" ]; then\n")
	script.WriteString("  printf '%s\\n' \"${FAKE_PI_VERSION:-0.85.1}\"\n")
	script.WriteString("  exit \"${FAKE_PI_VERSION_EXIT:-0}\"\n")
	script.WriteString("fi\n")
	script.WriteString("while IFS= read -r line; do\n")
	script.WriteString("  case \"$line\" in\n")
	for requestID, frames := range promptCases {
		for index, frame := range frames {
			if !json.Valid([]byte(frame)) {
				t.Fatalf("fake Pi fixture %q frame %d is invalid JSON: %s", requestID, index, frame)
			}
		}
		script.WriteString("    *'\"id\":\"")
		script.WriteString(shellSingleQuotedFragment(requestID))
		script.WriteString("\"'* )\n")
		for _, frame := range frames {
			script.WriteString("      printf '%s\\n' '")
			script.WriteString(shellSingleQuotedFragment(frame))
			script.WriteString("'\n")
		}
		script.WriteString("      ;;\n")
	}
	script.WriteString("    *) printf '%s\\n' '{\"type\":\"error\",\"message\":\"unknown fixture request\"}' ;;\n")
	script.WriteString("  esac\n")
	script.WriteString("done\n")
	if err := os.WriteFile(path, []byte(script.String()), 0o700); err != nil {
		t.Fatalf("write fake Pi: %v", err)
	}
	return path
}

func shellSingleQuotedFragment(value string) string {
	return strings.ReplaceAll(value, "'", "'\\\"'\\\"'")
}

func ordinaryPiFrames(requestID, replyOp, replyTo, content string) []string {
	return []string{
		`{"id":"` + requestID + `","type":"response","command":"prompt","success":true}`,
		`{"type":"agent_start"}`,
		`{"type":"tool_execution_start","toolCallId":"tool-` + replyOp + `","toolName":"room_reply","args":{"client_operation_id":"` + replyOp + `","in_reply_to_message_id":"` + replyTo + `","content":"` + content + `"}}`,
		`{"type":"tool_execution_end","toolCallId":"tool-` + replyOp + `","toolName":"room_reply","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"message_id\":\"reply-` + replyOp + `\",\"sequence\":2}}"}],"details":{}},"isError":false}`,
		`{"type":"agent_end","messages":[],"willRetry":false}`,
		`{"type":"agent_settled"}`,
	}
}
