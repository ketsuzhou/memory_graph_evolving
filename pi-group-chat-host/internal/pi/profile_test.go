package pi

import (
	"errors"
	"reflect"
	"testing"
)

func TestMemoryAgentProfileExposesOnlyRoomAndMemoryTools(t *testing.T) {
	profile, err := NewMemoryAgentProfile("/server/room-memory", []string{"PROVIDER_TOKEN"})
	if err != nil {
		t.Fatalf("NewMemoryAgentProfile() error = %v", err)
	}
	wantTools := []string{
		ToolRoomSend,
		ToolRoomReply,
		ToolRoomReact,
		ToolMemoryStart,
		ToolMemoryExplore,
		ToolMemoryRedirect,
		ToolMemorySubmit,
	}
	if profile.Kind != ProfileMemory {
		t.Fatalf("profile kind = %q, want %q", profile.Kind, ProfileMemory)
	}
	if profile.BuiltinToolsEnabled {
		t.Fatal("memory profile enabled Pi built-in tools")
	}
	if !reflect.DeepEqual(profile.AllowedToolNames, wantTools) {
		t.Fatalf("allowed tools = %#v, want exactly %#v", profile.AllowedToolNames, wantTools)
	}
	if profile.WorkingDirectory != "/server/room-memory" || !reflect.DeepEqual(profile.EnvironmentAllowlist, []string{"PROVIDER_TOKEN"}) {
		t.Fatalf("profile changed server authority: %+v", profile)
	}
}

func TestMemoryAgentRejectsCodingToolInvocation(t *testing.T) {
	profile, err := NewMemoryAgentProfile("/server/room-memory", nil)
	if err != nil {
		t.Fatalf("NewMemoryAgentProfile() error = %v", err)
	}
	for _, tool := range []string{"bash", "shell", "read", "write", "edit", "process", "filesystem", "http"} {
		t.Run(tool, func(t *testing.T) {
			if err := ValidateToolInvocation(profile, tool); !errors.Is(err, ErrToolNotAllowed) {
				t.Fatalf("ValidateToolInvocation(%q) error = %v, want %v", tool, err, ErrToolNotAllowed)
			}
		})
	}
}
