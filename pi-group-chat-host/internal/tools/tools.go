package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

var (
	ErrToolNotAllowed = errors.New("tools: tool not allowed")
	ErrAuthorityField = errors.New("tools: model cannot select authority")
)

// authorityFields are Host-owned and can never appear as model-selectable
// parameters: tenants, principals, grants, and spaces are resolved
// server-side from the ExecutionAuthority, not from model arguments.
var authorityFields = map[string]bool{
	"tenant": true, "tenant_id": true,
	"principal": true, "principal_id": true, "acting_principal_id": true,
	"grant": true, "grant_id": true, "grants": true,
	"space": true, "space_id": true, "space_ids": true, "private_space_id": true,
}

type ToolSchema struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

type Result struct {
	Value any
}

type Dispatcher struct{}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{}
}

func stringProperty(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func integerProperty(description string) map[string]any {
	return map[string]any{"type": "integer", "description": description}
}

func booleanProperty(description string) map[string]any {
	return map[string]any{"type": "boolean", "description": description}
}

func stringArrayProperty(description string) map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": description}
}

func objectSchema(properties map[string]any, required ...string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             append([]string(nil), required...),
		"additionalProperties": false,
	}
}

// MemoryAgentSchemas returns exactly the Memory Agent's seven tool schemas.
// None of them exposes a tenant, principal, grant, or space parameter.
func MemoryAgentSchemas() ([]ToolSchema, error) {
	return []ToolSchema{
		{
			Name:        "room_send",
			Description: "Publish a visible message to the room.",
			Parameters: objectSchema(map[string]any{
				"client_operation_id": stringProperty("Idempotency key chosen by the agent for this publish."),
				"content":             stringProperty("Message content visible to the room."),
			}, "client_operation_id", "content"),
		},
		{
			Name:        "room_reply",
			Description: "Publish a visible reply to an existing room message.",
			Parameters: objectSchema(map[string]any{
				"client_operation_id":    stringProperty("Idempotency key chosen by the agent for this reply."),
				"in_reply_to_message_id": stringProperty("ID of the message being answered."),
				"content":                stringProperty("Reply content visible to the room."),
			}, "client_operation_id", "in_reply_to_message_id", "content"),
		},
		{
			Name:        "room_react",
			Description: "Attach a reaction to an existing room message.",
			Parameters: objectSchema(map[string]any{
				"client_operation_id": stringProperty("Idempotency key chosen by the agent for this reaction."),
				"message_id":          stringProperty("ID of the message to react to."),
				"emoji":               stringProperty("Reaction emoji."),
			}, "client_operation_id", "message_id", "emoji"),
		},
		{
			Name:        "memory_start",
			Description: "Open a bounded graph-memory exploration for the room's shared memory.",
			Parameters: objectSchema(map[string]any{
				"client_operation_id": stringProperty("Idempotency key chosen by the agent for this operation."),
				"query":               stringProperty("What to look for in memory."),
				"max_steps":           integerProperty("Maximum exploration steps."),
				"max_results":         integerProperty("Maximum recalled results."),
			}, "client_operation_id", "query"),
		},
		{
			Name:        "memory_explore",
			Description: "Take one exploration step from an anchor citation.",
			Parameters: objectSchema(map[string]any{
				"client_operation_id": stringProperty("Idempotency key chosen by the agent for this operation."),
				"session_id":          stringProperty("Exploration session returned by memory_start."),
				"anchor_citation_id":  stringProperty("Citation to explore around."),
				"relation":            stringProperty("Relation direction to follow."),
				"limit":               integerProperty("Maximum items for this step."),
			}, "client_operation_id", "session_id"),
		},
		{
			Name:        "memory_redirect",
			Description: "Refine the exploration query within the open session.",
			Parameters: objectSchema(map[string]any{
				"client_operation_id": stringProperty("Idempotency key chosen by the agent for this operation."),
				"session_id":          stringProperty("Exploration session returned by memory_start."),
				"query":               stringProperty("Refined query."),
				"anchor_citation_ids": stringArrayProperty("Citations to anchor the refined query."),
				"reason":              stringProperty("Why the query is being refined."),
			}, "client_operation_id", "session_id", "query", "anchor_citation_ids", "reason"),
		},
		{
			Name:        "memory_submit",
			Description: "Close the exploration with an answer grounded in served citations.",
			Parameters: objectSchema(map[string]any{
				"client_operation_id": stringProperty("Idempotency key chosen by the agent for this operation."),
				"session_id":          stringProperty("Exploration session returned by memory_start."),
				"found":               booleanProperty("Whether the query was answered."),
				"summary":             stringProperty("Answer summary."),
				"citation_ids":        stringArrayProperty("Citations that ground the answer."),
			}, "client_operation_id", "session_id", "found", "summary", "citation_ids"),
		},
	}, nil
}

func memoryAgentToolSet() map[string]ToolSchema {
	schemas, _ := MemoryAgentSchemas()
	set := make(map[string]ToolSchema, len(schemas))
	for _, schema := range schemas {
		set[schema.Name] = schema
	}
	return set
}

// Dispatch enforces the Memory Agent allowlist: any coding, shell, process,
// filesystem, or network tool is ErrToolNotAllowed.
func (d *Dispatcher) Dispatch(ctx context.Context, name string, arguments json.RawMessage) (Result, error) {
	schema, ok := memoryAgentToolSet()[name]
	if !ok {
		return Result{}, fmt.Errorf("%w: %q", ErrToolNotAllowed, name)
	}
	if err := ValidateModelArguments(schema, arguments); err != nil {
		return Result{}, err
	}
	var value any
	if err := json.Unmarshal(arguments, &value); err != nil {
		return Result{}, fmt.Errorf("tools: decode %s arguments: %w", name, err)
	}
	return Result{Value: value}, nil
}

// ValidateModelArguments rejects any attempt by the model to select
// Host-authority fields; unknown parameters are rejected as well because
// every schema sets additionalProperties to false.
func ValidateModelArguments(schema ToolSchema, arguments json.RawMessage) error {
	var object map[string]any
	if err := json.Unmarshal(arguments, &object); err != nil {
		return fmt.Errorf("tools: decode %s arguments: %w", schema.Name, err)
	}
	if object == nil {
		return nil
	}
	var names []string
	for name := range object {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if authorityFields[name] {
			return fmt.Errorf("%w: %q in %s arguments", ErrAuthorityField, name, schema.Name)
		}
	}
	properties, _ := schema.Parameters["properties"].(map[string]any)
	for _, name := range names {
		if _, known := properties[name]; !known {
			return fmt.Errorf("%w: unknown parameter %q in %s arguments", ErrToolNotAllowed, name, schema.Name)
		}
	}
	if required, ok := schema.Parameters["required"].([]string); ok {
		for _, name := range required {
			if _, present := object[name]; !present {
				return fmt.Errorf("tools: %s is missing required parameter %q", schema.Name, name)
			}
		}
	}
	return nil
}
