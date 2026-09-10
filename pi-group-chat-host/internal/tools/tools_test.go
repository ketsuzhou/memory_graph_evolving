package tools

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestMemoryAgentToolSchemasAreExactlySeven(t *testing.T) {
	schemas, err := MemoryAgentSchemas()
	if err != nil {
		t.Fatalf("MemoryAgentSchemas() error = %v", err)
	}
	if len(schemas) != 7 {
		t.Fatalf("Memory Agent tool count = %d, want exactly 7", len(schemas))
	}
	got := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		got = append(got, schema.Name)
		if additional, ok := schema.Parameters["additionalProperties"].(bool); !ok || additional {
			t.Errorf("%s must reject unknown model parameters", schema.Name)
		}
	}
	sort.Strings(got)
	want := []string{"memory_explore", "memory_redirect", "memory_start", "memory_submit", "room_react", "room_reply", "room_send"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Memory Agent tools = %v, want %v", got, want)
	}
}

func TestMemoryAgentRejectsCodingToolInvocation(t *testing.T) {
	dispatcher := NewDispatcher()
	for _, name := range []string{"bash", "shell", "exec", "read", "write", "edit", "grep", "find", "browser", "http"} {
		t.Run(name, func(t *testing.T) {
			_, err := dispatcher.Dispatch(context.Background(), name, json.RawMessage(`{}`))
			if !errors.Is(err, ErrToolNotAllowed) {
				t.Errorf("Dispatch(%q) error = %v, want ErrToolNotAllowed", name, err)
			}
		})
	}
}

func TestMemoryAgentModelParametersRejectAuthorityFields(t *testing.T) {
	schemas, err := MemoryAgentSchemas()
	if err != nil {
		t.Fatalf("MemoryAgentSchemas() error = %v", err)
	}
	for _, schema := range schemas {
		assertNoAuthoritySchemaFields(t, schema.Name, schema.Parameters)
		for _, field := range []string{
			"tenant", "tenant_id", "principal", "principal_id", "acting_principal_id",
			"grant", "grant_id", "grants", "space", "space_id", "space_ids", "private_space_id",
		} {
			t.Run(schema.Name+"/"+field, func(t *testing.T) {
				args, err := json.Marshal(map[string]any{field: "attacker-selected"})
				if err != nil {
					t.Fatal(err)
				}
				if err := ValidateModelArguments(schema, args); !errors.Is(err, ErrAuthorityField) {
					t.Errorf("ValidateModelArguments(%s) error = %v, want ErrAuthorityField", field, err)
				}
			})
		}
	}
}

func assertNoAuthoritySchemaFields(t *testing.T, toolName string, value any) {
	t.Helper()
	forbidden := map[string]bool{
		"tenant": true, "tenant_id": true, "principal": true, "principal_id": true,
		"grant": true, "grant_id": true, "grants": true, "space": true, "space_id": true,
		"space_ids": true, "private_space_id": true,
	}
	var walk func(any)
	walk = func(current any) {
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				if forbidden[key] {
					t.Errorf("%s schema exposes forbidden authority field %q", toolName, key)
				}
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
}
