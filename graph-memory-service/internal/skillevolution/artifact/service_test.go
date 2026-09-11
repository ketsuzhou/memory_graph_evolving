package artifact

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

func loadGates(t *testing.T) *validation.Gates {
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
		t.Fatalf("NewGates: %v", err)
	}
	return gates
}

func newService(t *testing.T) *Service {
	t.Helper()
	svc, err := NewService(loadGates(t))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func fixtureSource(t *testing.T, category, caseName string) (any, map[string]any) {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, category, caseName, "source.json"))
	if err != nil {
		t.Fatalf("read %s/%s: %v", category, caseName, err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		t.Fatalf("parse %s/%s: %v", category, caseName, err)
	}
	obj, _ := contract.AsObject(value)
	return value, obj
}

func fixtureExpected(t *testing.T, category, caseName string) (bool, string) {
	t.Helper()
	dir, _ := contract.DefaultConformanceDir()
	data, err := os.ReadFile(filepath.Join(dir, category, caseName, "expected.json"))
	if err != nil {
		t.Fatalf("read expected %s/%s: %v", category, caseName, err)
	}
	value, _ := contract.ParseJSONStrict(data)
	obj, _ := contract.AsObject(value)
	accept, _ := obj["expected_accept"].(bool)
	reason, _ := contract.AsString(obj["expected_reason_code"])
	return accept, reason
}

func childRef(lineage string, version int, kind, digest string) map[string]any {
	return map[string]any{
		"schema_version":  contract.SchemaSkillArtifactRef,
		"lineage_id":      lineage,
		"version":         jsonInt(version),
		"kind":            kind,
		"artifact_digest": digest,
	}
}

func jsonInt(v int) any { return json.Number(strconv.Itoa(v)) }

// compositeFixture loads artifact-003-composite and returns its envelope.
func compositeFixture(t *testing.T) map[string]any {
	t.Helper()
	_, obj := fixtureSource(t, "artifacts", "artifact-003-composite")
	return deepCopy(obj).(map[string]any) // package deepCopyMap also available
}

// ---------------------------------------------------------------------------
// GMS-202 TDD Red #2
// ---------------------------------------------------------------------------

// TestCompositeRejectsCycleMissingPortAndPermissionOverflow pins the
// Composite static gates (Contract §8.4/§9.5.1, GMS §3.5): cycles, missing
// named child ports and permissions beyond the Host authority cap are all
// fail-closed, as are unbounded retries and invalid fallback declarations.
func TestCompositeRejectsCycleMissingPortAndPermissionOverflow(t *testing.T) {
	svc := newService(t)
	base := compositeFixture(t)

	// Sanity: the golden composite passes every static gate.
	golden, err := svc.Canonicalize(deepCopy(base))
	if err != nil {
		t.Fatalf("golden composite rejected: %v", err)
	}
	if golden.Kind() != "composite" || golden.Packet() == nil {
		t.Fatalf("composite gate did not produce a frozen packet")
	}

	// Cycle: c2 -> c1 closes the loop around the c1 -> c2 edge.
	cyclic := deepCopyMap(base)
	body, _ := contract.AsObject(cyclic["body"])
	edges, _ := contract.AsArray(body["edges"])
	edges = append(edges, map[string]any{
		"from_child_id": "c2-report", "from_port": "report",
		"to_child_id": "c1-plan", "to_port": "request",
		"mapping": map[string]any{},
	})
	body["edges"] = edges
	if _, err := svc.Canonicalize(cyclic); CodeOf(err) != "COMPOSITE_CYCLE" {
		t.Fatalf("cycle: want COMPOSITE_CYCLE, got %v", err)
	}

	// Missing named child port.
	missingPort := deepCopyMap(base)
	missingBody, _ := contract.AsObject(missingPort["body"])
	children, _ := contract.AsArray(missingBody["children"])
	second, _ := contract.AsObject(children[1])
	delete(second, "input_port")
	if _, err := svc.Canonicalize(missingPort); CodeOf(err) != "PORT_SCHEMA_MISSING" {
		t.Fatalf("missing port: want PORT_SCHEMA_MISSING, got %v", err)
	}

	// Incompatible port: edge references a port the child never declared.
	badPort := deepCopyMap(base)
	badBody, _ := contract.AsObject(badPort["body"])
	badEdges, _ := contract.AsArray(badBody["edges"])
	badEdge, _ := contract.AsObject(badEdges[0])
	badEdge["to_port"] = "nowhere"
	if _, err := svc.Canonicalize(badPort); CodeOf(err) != "PORT_SCHEMA_INCOMPATIBLE" {
		t.Fatalf("incompatible port: want PORT_SCHEMA_INCOMPATIBLE, got %v", err)
	}

	// Permission beyond the Host cap (orchestration_permissions).
	overCap := deepCopyMap(base)
	overBody, _ := contract.AsObject(overCap["body"])
	overBody["orchestration_permissions"] = []any{
		map[string]any{"capability": "filesystem_write", "scope": "/"},
	}
	if _, err := svc.Canonicalize(overCap); CodeOf(err) != "PERMISSION_CAP_EXCEEDED" {
		t.Fatalf("permission overflow: want PERMISSION_CAP_EXCEEDED, got %v", err)
	}

	// Unbounded retry: max_attempts below 1 rejects fail-closed.
	unbounded := deepCopyMap(base)
	unBody, _ := contract.AsObject(unbounded["body"])
	unBody["retry_policies"] = []any{
		map[string]any{
			"child_id": "c2-report", "max_attempts": jsonInt(0),
			"retryable_reason_codes": []any{"EVIDENCE_NOT_COMMITTED"},
			"backoff_units":          jsonInt(1),
		},
	}
	if _, err := svc.Canonicalize(unbounded); CodeOf(err) != "COMPOSITE_RETRY_INVALID" {
		t.Fatalf("unbounded retry: want COMPOSITE_RETRY_INVALID, got %v", err)
	}

	// Retry policy for an undeclared child.
	ghostRetry := deepCopyMap(base)
	ghostBody, _ := contract.AsObject(ghostRetry["body"])
	ghostBody["retry_policies"] = []any{
		map[string]any{
			"child_id": "c3-ghost", "max_attempts": jsonInt(2),
			"retryable_reason_codes": []any{"EVIDENCE_NOT_COMMITTED"},
			"backoff_units":          jsonInt(1),
		},
	}
	if _, err := svc.Canonicalize(ghostRetry); CodeOf(err) != "COMPOSITE_RETRY_INVALID" {
		t.Fatalf("ghost retry child: want COMPOSITE_RETRY_INVALID, got %v", err)
	}

	// Fallback action without a declared fallback child.
	noFallback := deepCopyMap(base)
	nfBody, _ := contract.AsObject(noFallback["body"])
	nfBody["failure_handlers"] = []any{
		map[string]any{
			"child_id":     "c2-report",
			"reason_codes": []any{"PORT_SCHEMA_MISSING"},
			"action":       "fallback",
		},
	}
	if _, err := svc.Canonicalize(noFallback); CodeOf(err) != "COMPOSITE_FAILURE_ACTION_INVALID" {
		t.Fatalf("fallback without child: want COMPOSITE_FAILURE_ACTION_INVALID, got %v", err)
	}

	// Fallback child that depends on the failing child (unsatisfiable).
	depFail := deepCopyMap(base)
	dfBody, _ := contract.AsObject(depFail["body"])
	dfBody["failure_handlers"] = []any{
		map[string]any{
			"child_id":          "c1-plan",
			"reason_codes":      []any{"PORT_SCHEMA_MISSING"},
			"action":            "fallback",
			"fallback_child_id": "c2-report", // c2 depends on c1
		},
	}
	if _, err := svc.Canonicalize(depFail); CodeOf(err) != "COMPOSITE_FAILURE_ACTION_INVALID" {
		t.Fatalf("unsatisfiable fallback: want COMPOSITE_FAILURE_ACTION_INVALID, got %v", err)
	}

	// Whole-packet freeze: any child ref change forces a new packet digest.
	changedChild := deepCopyMap(base)
	ccBody, _ := contract.AsObject(changedChild["body"])
	ccChildren, _ := contract.AsArray(ccBody["children"])
	firstChild, _ := contract.AsObject(ccChildren[0])
	firstChild["skill_ref"] = childRef("sg-alpha", 3, "step_guidance",
		"sha256:1111111111111111111111111111111111111111111111111111111111111111")
	changed, err := svc.Canonicalize(changedChild)
	if err != nil {
		t.Fatalf("changed child ref rejected: %v", err)
	}
	if changed.Packet().Digest() == golden.Packet().Digest() {
		t.Fatal("child ref change did not change the frozen composite packet digest")
	}
}

// ---------------------------------------------------------------------------
// Kind gates
// ---------------------------------------------------------------------------

func TestProcedureAndGuidanceKindGates(t *testing.T) {
	svc := newService(t)

	// procedure: duplicate step_id rejects.
	_, proc := fixtureSource(t, "artifacts", "artifact-002-procedure")
	dup := deepCopyMap(proc)
	body, _ := contract.AsObject(dup["body"])
	steps, _ := contract.AsArray(body["steps"])
	second, _ := contract.AsObject(steps[1])
	second["step_id"] = "s1-freeze"
	if _, err := svc.Canonicalize(dup); CodeOf(err) != "ARTIFACT_BODY_INVALID" {
		t.Fatalf("duplicate step_id: want ARTIFACT_BODY_INVALID, got %v", err)
	}

	// procedure: forged detached future field rejects
	// (closed body schema, Contract §8.2).
	forged := deepCopyMap(proc)
	forgedBody, _ := contract.AsObject(forged["body"])
	forgedBody["detached_future"] = map[string]any{"final_task_impact": "forged"}
	if _, err := svc.Canonicalize(forged); CodeOf(err) != "SCHEMA_FIELD_UNKNOWN" {
		t.Fatalf("forged detached future: want SCHEMA_FIELD_UNKNOWN, got %v", err)
	}

	// step_guidance: duplicate branch_id rejects.
	_, sg := fixtureSource(t, "artifacts", "artifact-001-step-guidance")
	dupBranch := deepCopyMap(sg)
	sgBody, _ := contract.AsObject(dupBranch["body"])
	branches, _ := contract.AsArray(sgBody["branches"])
	dupSecond, _ := contract.AsObject(branches[1])
	dupSecond["branch_id"] = "b-success"
	if _, err := svc.Canonicalize(dupBranch); CodeOf(err) != "ARTIFACT_BODY_INVALID" {
		t.Fatalf("duplicate branch_id: want ARTIFACT_BODY_INVALID, got %v", err)
	}

	// step_guidance: a branch without any provenance (no evidence refs, no
	// claim refs) rejects (GMS §3.4).
	orphan := deepCopyMap(sg)
	orphanBody, _ := contract.AsObject(orphan["body"])
	orphanBranches, _ := contract.AsArray(orphanBody["branches"])
	orphanSecond, _ := contract.AsObject(orphanBranches[1])
	orphanAction, _ := contract.AsObject(orphanSecond["action"])
	orphanAction["evidence_refs"] = []any{}
	if _, err := svc.Canonicalize(orphan); CodeOf(err) != "BRANCH_PROVENANCE_INCOMPLETE" {
		t.Fatalf("branch without provenance: want BRANCH_PROVENANCE_INCOMPLETE, got %v", err)
	}
}

func TestKindDriftAndNonExactRefsReject(t *testing.T) {
	svc := newService(t)

	// artifact-neg-005: same (lineage, version, digest) with two kinds (a
	// root array of two exact refs — the drift check applies to any shape).
	driftValue, _ := fixtureSource(t, "artifacts", "artifact-neg-005-kind-drift")
	if _, err := svc.Canonicalize(deepCopy(driftValue)); CodeOf(err) != "REF_MISMATCH" {
		t.Fatalf("kind drift: want REF_MISMATCH, got %v", err)
	}

	// Non-exact child ref: latest version / graph node id.
	latest := compositeFixture(t)
	latestBody, _ := contract.AsObject(latest["body"])
	latestChildren, _ := contract.AsArray(latestBody["children"])
	latestFirst, _ := contract.AsObject(latestChildren[0])
	latestRef, _ := contract.AsObject(latestFirst["skill_ref"])
	latestRef["version"] = "latest"
	if _, err := svc.Canonicalize(latest); CodeOf(err) != "NON_EXACT_REF" {
		t.Fatalf("latest version child: want NON_EXACT_REF, got %v", err)
	}

	graphNode := compositeFixture(t)
	gnBody, _ := contract.AsObject(graphNode["body"])
	gnChildren, _ := contract.AsArray(gnBody["children"])
	gnFirst, _ := contract.AsObject(gnChildren[0])
	gnRef, _ := contract.AsObject(gnFirst["skill_ref"])
	delete(gnRef, "lineage_id")
	delete(gnRef, "version")
	gnRef["graph_node_id"] = "node-1"
	if _, err := svc.Canonicalize(graphNode); CodeOf(err) != "NON_EXACT_REF" {
		t.Fatalf("graph-node child: want NON_EXACT_REF, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Fixture-driven conformance matrix ($FIX artifacts/merge/negative)
// ---------------------------------------------------------------------------

// TestArtifactFixtureMatrix drives the canonical artifact gate over the
// $FIX artifact/merge/negative corpus: for every case the S1 semantics
// (GMS-101 DeriveOutcome) must agree with expected.json, and for every
// skill-artifact envelope the three-kind gate must agree too.
func TestArtifactFixtureMatrix(t *testing.T) {
	svc := newService(t)

	artifacts := []string{
		"artifact-001-step-guidance", "artifact-002-procedure",
		"artifact-003-composite", "artifact-004-optional-extension",
		"artifact-005-guidance-view", "artifact-006-explore-result",
		"artifact-007-skill-lock",
		"artifact-neg-001-required-extension", "artifact-neg-002-composite-cycle",
		"artifact-neg-003-port-missing", "artifact-neg-004-permission-cap",
		"artifact-neg-005-kind-drift",
	}
	merges := []string{
		"merge-001-similarity-ab", "merge-002-similarity-ba",
		"merge-003-merge-proposal", "merge-neg-001-below-threshold",
		"merge-neg-002-blocking-conflict",
	}
	negatives := []string{"negative-001-digest-tamper"}

	categoryDirs := map[string]string{"artifacts": "artifacts", "merges": "merge", "negatives": "negative"}
	for group, cases := range map[string][]string{"artifacts": artifacts, "merges": merges, "negatives": negatives} {
		for _, caseName := range cases {
			value, _ := fixtureSource(t, categoryDirs[group], caseName)
			wantAccept, wantReason := fixtureExpected(t, categoryDirs[group], caseName)

			// S1 semantics parity via the GMS-101 runner.
			if outcome := contract.DeriveOutcome(rawSource(t, categoryDirs[group], caseName), categoryOf(group), sidecar(t, categoryDirs[group], caseName)); outcome.Accept != wantAccept || outcome.ReasonCode != wantReason {
				t.Fatalf("%s/%s: S1 semantics want (%v, %s) got (%v, %s)", group, caseName, wantAccept, wantReason, outcome.Accept, outcome.ReasonCode)
			}

			// Three-kind gate applies only to skill-artifact envelopes.
			obj, _ := contract.AsObject(value)
			if obj == nil {
				continue
			}
			if sv, _ := contract.AsString(obj["schema_version"]); sv != "gms.skill-artifact.v1" {
				continue
			}
			_, err := svc.Canonicalize(deepCopy(obj))
			gotAccept, gotReason := err == nil, ""
			if err != nil {
				gotReason = CodeOf(err)
			}
			if gotAccept != wantAccept || gotReason != wantReason {
				t.Fatalf("%s/%s: artifact gate want (%v, %s) got (%v, %s)", group, caseName, wantAccept, wantReason, gotAccept, gotReason)
			}
		}
	}
}

func categoryOf(group string) string {
	switch group {
	case "artifacts":
		return "artifact"
	case "merges":
		return "merge"
	default:
		return "negative"
	}
}

func rawSource(t *testing.T, category, caseName string) []byte {
	t.Helper()
	dir, _ := contract.DefaultConformanceDir()
	data, err := os.ReadFile(filepath.Join(dir, category, caseName, "source.json"))
	if err != nil {
		t.Fatalf("read source %s/%s: %v", category, caseName, err)
	}
	return data
}

func sidecar(t *testing.T, category, caseName string) []byte {
	t.Helper()
	dir, _ := contract.DefaultConformanceDir()
	data, err := os.ReadFile(filepath.Join(dir, category, caseName, "canonical.utf8"))
	if err != nil {
		t.Fatalf("read canonical %s/%s: %v", category, caseName, err)
	}
	return data
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[k] = deepCopy(item)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = deepCopy(item)
		}
		return out
	default:
		return v
	}
}
