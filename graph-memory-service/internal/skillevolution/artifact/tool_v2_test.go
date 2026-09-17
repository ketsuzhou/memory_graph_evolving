package artifact_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

func TestCanonicalizeAcceptsV2ToolArtifactAndRejectsV1(t *testing.T) {
	t.Parallel()

	service := newArtifactService(t)
	tool := v2ToolArtifact()
	canonical, err := service.Canonicalize(tool)
	if err != nil {
		t.Fatalf("canonicalize v2 tool artifact: %v", err)
	}
	if got, want := canonical.Kind(), "tool"; got != want {
		t.Fatalf("canonical kind = %q, want %q", got, want)
	}
	if canonical.BodyDigest() == "" {
		t.Fatal("canonical v2 tool artifact has no digest")
	}

	legacy := cloneMap(tool)
	legacy["schema_version"] = "gms.skill-artifact.v1"
	if _, err := service.Canonicalize(legacy); validation.CodeOf(err) != "SCHEMA_CONST_MISMATCH" {
		t.Fatalf("v1 artifact error code = %q, want %q; err=%v", validation.CodeOf(err), "SCHEMA_CONST_MISMATCH", err)
	}
}

func newArtifactService(t *testing.T) *artifact.Service {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	schemas, err := validation.LoadSchemaSet(filepath.Join(dir, "schema", "shared"))
	if err != nil {
		t.Fatalf("load shared schema set: %v", err)
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		t.Fatalf("new gates: %v", err)
	}
	service, err := artifact.NewService(gates)
	if err != nil {
		t.Fatalf("new artifact service: %v", err)
	}
	return service
}

func v2ToolArtifact() map[string]any {
	return map[string]any{
		"schema_version": "gms.skill-artifact.v2",
		"kind":           "tool",
		"title":          "Apply a controlled workspace patch",
		"description":    "Runs one attested OCI package with a typed contract.",
		"applicability":  map[string]any{"predicates": []any{}, "exclusions": []any{}},
		"permissions": []any{
			map[string]any{"capability": "tool_execute", "scope": "task-private"},
		},
		"body": map[string]any{
			"tool_package": map[string]any{
				"schema_version":        "gms.tool-package-ref.v2",
				"oci_image_digest":      digest("oci-image"),
				"entrypoint":            []any{"/app/tool"},
				"build_attestation_ref": versionedRef("attestation"),
			},
			"interface": map[string]any{
				"input_schema_ref":  versionedRef("input-schema"),
				"output_schema_ref": versionedRef("output-schema"),
				"error_schema_ref":  versionedRef("error-schema"),
			},
			"execution_policy": map[string]any{
				"network":        "disabled",
				"cpu_millis":     json.Number("100"),
				"memory_bytes":   json.Number("1048576"),
				"timeout_millis": json.Number("1000"),
				"workspace_mode": "controlled_patch",
				"patch_policy": map[string]any{
					"allowed_paths":                 []any{"src/**"},
					"require_base_workspace_digest": true,
					"require_structured_manifest":   true,
				},
			},
			"declared_capabilities":   []any{"tool_execute"},
			"validation_contract_ref": versionedRef("validation-contract"),
			"candidate_provenance_ref": map[string]any{
				"schema_version": "gms.candidate-artifact-ref.v2",
				"candidate_id":   "candidate-tool-1",
				"kind":           "tool",
				"body_digest":    digest("candidate-body"),
				"origin_type":    "skill_proposal",
				"origin_ref":     versionedRef("proposal-tool-1"),
			},
		},
	}
}

func versionedRef(id string) map[string]any {
	return map[string]any{"id": id, "version": json.Number("1"), "digest": digest(id)}
}

func digest(_ string) string { return "sha256:" + strings.Repeat("a", 64) }

func cloneMap(value map[string]any) map[string]any {
	data, _ := json.Marshal(value)
	decoded, _ := contract.ParseJSONStrict(data)
	clone, _ := contract.AsObject(decoded)
	return clone
}
