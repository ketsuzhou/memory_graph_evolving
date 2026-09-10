package pi

import (
	"errors"
	"fmt"

	"river2.dev/pi-group-chat-host/internal/domain"
)

type AgentProfile = domain.AgentProfile

const (
	ProfileOrdinary = domain.ProfileOrdinary
	ProfileMemory   = domain.ProfileMemory
)

const (
	ToolRoomSend       = "room_send"
	ToolRoomReply      = "room_reply"
	ToolRoomReact      = "room_react"
	ToolMemoryStart    = "memory_start"
	ToolMemoryExplore  = "memory_explore"
	ToolMemoryRedirect = "memory_redirect"
	ToolMemorySubmit   = "memory_submit"
)

// NewOrdinaryAgentProfile builds the operator-trusted ordinary profile: Pi's
// complete documented coding tools stay enabled and the server owns the
// working directory and environment allowlist. It is not a sandbox.
func NewOrdinaryAgentProfile(workingDirectory string, environmentAllowlist []string) (AgentProfile, error) {
	if workingDirectory == "" {
		return AgentProfile{}, errors.New("ordinary profile requires a server-owned working directory")
	}
	return AgentProfile{
		Kind:                 domain.ProfileOrdinary,
		BuiltinToolsEnabled:  true,
		AllowedToolNames:     nil,
		WorkingDirectory:     workingDirectory,
		EnvironmentAllowlist: append([]string(nil), environmentAllowlist...),
	}, nil
}

// NewMemoryAgentProfile builds the least-privileged persistent Memory Agent
// profile: no built-in tools, exactly the Room and Memory tool surface.
func NewMemoryAgentProfile(workingDirectory string, environmentAllowlist []string) (AgentProfile, error) {
	if workingDirectory == "" {
		return AgentProfile{}, errors.New("memory profile requires a server-owned working directory")
	}
	return AgentProfile{
		Kind:                 domain.ProfileMemory,
		BuiltinToolsEnabled:  false,
		AllowedToolNames:     MemoryAgentToolSurface(),
		WorkingDirectory:     workingDirectory,
		EnvironmentAllowlist: append([]string(nil), environmentAllowlist...),
	}, nil
}

// MemoryAgentToolSurface returns the Memory Agent's fixed tool list in the
// canonical --tools order.
func MemoryAgentToolSurface() []string {
	return []string{
		ToolRoomSend,
		ToolRoomReply,
		ToolRoomReact,
		ToolMemoryStart,
		ToolMemoryExplore,
		ToolMemoryRedirect,
		ToolMemorySubmit,
	}
}

// ValidateToolInvocation enforces the profile's tool surface. The Memory Agent
// can neither discover nor invoke any coding, shell, process, filesystem, or
// network tool.
func ValidateToolInvocation(profile AgentProfile, tool string) error {
	if profile.Kind == domain.ProfileMemory || !profile.BuiltinToolsEnabled || len(profile.AllowedToolNames) > 0 {
		for _, allowed := range profile.AllowedToolNames {
			if allowed == tool {
				return nil
			}
		}
		return fmt.Errorf("%w: %q is outside the agent tool surface", ErrToolNotAllowed, tool)
	}
	return nil
}
