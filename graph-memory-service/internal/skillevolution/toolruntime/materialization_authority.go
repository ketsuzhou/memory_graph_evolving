package toolruntime

import (
	"context"
	"encoding/json"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/materializationread"
)

// MaterializationAuthority adapts the authoritative exact-active closure read
// to ToolRuntime admission. Candidate, stale, deactivated, and non-exact refs
// are rejected by materializationread before any container adapter is reached.
type MaterializationAuthority struct{ reader *materializationread.Service }

func NewMaterializationAuthority(reader *materializationread.Service) *MaterializationAuthority {
	return &MaterializationAuthority{reader: reader}
}
func (a *MaterializationAuthority) ResolveActivatedTool(ctx context.Context, ref contract.SkillArtifactRef) (ResolvedTool, *Failure) {
	if a == nil || a.reader == nil {
		return ResolvedTool{}, NewFailure(FailureAdapterUnavailable, "exact-active authority is not configured")
	}
	if ref.Kind != "tool" {
		return ResolvedTool{}, NewFailure(FailureToolKindInvalid, "only exact Tool Skill refs may be invoked")
	}
	closure, err := a.reader.Read(ctx, materializationread.ReadRequest{Roots: []contract.SkillArtifactRef{ref}})
	if err != nil {
		return ResolvedTool{}, NewFailure(FailureToolNotActivated, err.Error())
	}
	if len(closure.Nodes) != 1 || closure.Nodes[0].Ref != ref || closure.Nodes[0].Kind != "tool" {
		return ResolvedTool{}, NewFailure(FailureToolNotActivated, "authority closure does not contain the exact activated Tool Skill")
	}
	var envelope map[string]any
	if err := json.Unmarshal(closure.Nodes[0].CanonicalBytes, &envelope); err != nil {
		return ResolvedTool{}, NewFailure(FailureToolNotActivated, "activated Tool Skill canonical body is invalid")
	}
	body, _ := envelope["body"].(map[string]any)
	packageDoc, _ := body["tool_package"].(map[string]any)
	image, _ := packageDoc["oci_image_digest"].(string)
	entryRaw, _ := packageDoc["entrypoint"].([]any)
	entrypoint := make([]string, 0, len(entryRaw))
	for _, raw := range entryRaw {
		if value, ok := raw.(string); ok {
			entrypoint = append(entrypoint, value)
		}
	}
	policy, _ := body["execution_policy"].(map[string]any)
	return ResolvedTool{Ref: ref, Package: OCIPackage{ImageDigest: image, Entrypoint: entrypoint, CPUUnits: integer(policy["cpu_millis"]), MemoryBytes: integer(policy["memory_bytes"]), TimeoutMillis: integer(policy["timeout_millis"])}, ValidationContractID: validationContractID(body)}, nil
}
func integer(value any) int64 {
	switch n := value.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}
func validationContractID(body map[string]any) string {
	ref, _ := body["validation_contract_ref"].(map[string]any)
	id, _ := ref["id"].(string)
	return id
}
