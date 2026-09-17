package toolruntime_test

import (
	"context"
	"errors"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/toolruntime"
)

func TestRuntimeRejectsUnactivatedToolBeforeAdapter(t *testing.T) {
	authority := &fakeAuthority{err: toolruntime.NewFailure(toolruntime.FailureToolNotActivated, "not active")}
	adapter := &fakeContainer{}
	runtime := newRuntime(t, authority, adapter, &fakeWorkspace{}, &fakeValidator{})

	result := runtime.Invoke(context.Background(), invocation())
	if result.Failure == nil || result.Failure.Code != toolruntime.FailureToolNotActivated {
		t.Fatalf("unactivated invocation = %#v", result)
	}
	if adapter.calls != 0 {
		t.Fatalf("adapter calls = %d, want 0", adapter.calls)
	}
}

func TestPatchManifestRejectsEscapingPathAndBaseDigestMismatch(t *testing.T) {
	workspace := toolruntime.WorkspaceSnapshot{ID: "task-1", BaseDigest: digest("base")}
	valid := toolruntime.PatchManifest{BaseWorkspaceDigest: workspace.BaseDigest, AllowedPaths: []string{"output/"}, Changes: []toolruntime.PatchChange{{Path: "output/result.txt", FileType: toolruntime.FileTypeRegular, BlobRef: digest("blob"), SizeBytes: 3}}, Rollback: toolruntime.RollbackMetadata{RollbackID: "rollback-1"}}
	if failure := valid.ValidateForApply(workspace, 10); failure != nil {
		t.Fatalf("valid patch rejected: %v", failure)
	}

	escape := valid
	escape.Changes[0].Path = "../outside.txt"
	if failure := escape.ValidateForApply(workspace, 10); failure == nil || failure.Code != toolruntime.FailurePatchPathNotAllowed {
		t.Fatalf("escape failure = %#v", failure)
	}

	stale := valid
	stale.BaseWorkspaceDigest = digest("other-base")
	if failure := stale.ValidateForApply(workspace, 10); failure == nil || failure.Code != toolruntime.FailureWorkspaceBaseMismatch {
		t.Fatalf("base failure = %#v", failure)
	}
}

func TestRuntimeAppliesValidatedFakeContainerPatchAndReturnsTypedResult(t *testing.T) {
	tool := testTool()
	authority := &fakeAuthority{tool: tool}
	adapter := &fakeContainer{result: toolruntime.ContainerResult{Output: map[string]any{"changed": true}, Patch: &toolruntime.PatchManifest{BaseWorkspaceDigest: digest("base"), AllowedPaths: []string{"output/"}, Changes: []toolruntime.PatchChange{{Path: "output/result.txt", FileType: toolruntime.FileTypeRegular, BlobRef: digest("patch-blob"), Diff: "+++ result", SizeBytes: 12}}, Rollback: toolruntime.RollbackMetadata{RollbackID: "rollback-1", Reversible: true}}}}
	workspace := &fakeWorkspace{}
	validator := &fakeValidator{result: toolruntime.ValidationResult{Passed: true, Checks: []toolruntime.ValidationCheck{{CheckID: "typed-output", Passed: true}}}}
	runtime := newRuntime(t, authority, adapter, workspace, validator)

	result := runtime.Invoke(context.Background(), invocation())
	if result.Failure != nil {
		t.Fatalf("runtime failure = %v", result.Failure)
	}
	if result.Output["changed"] != true || result.Patch == nil || !result.Validation.Passed {
		t.Fatalf("result = %#v", result)
	}
	if adapter.calls != 1 || workspace.calls != 1 || validator.calls != 1 {
		t.Fatalf("calls adapter/workspace/validator = %d/%d/%d", adapter.calls, workspace.calls, validator.calls)
	}
	if adapter.last.NetworkEnabled || !adapter.last.TaskPrivateCOW || !adapter.last.ReadOnlyInputs {
		t.Fatalf("sandbox policy not enforced: %#v", adapter.last)
	}
}

func TestRuntimeCircuitOpensAfterTypedFailuresAndExplicitResetRecovers(t *testing.T) {
	failure := toolruntime.NewFailure(toolruntime.FailureContainerExecution, "boom")
	authority := &fakeAuthority{tool: testTool()}
	adapter := &fakeContainer{err: failure}
	validator := &fakeValidator{result: toolruntime.ValidationResult{Passed: true}}
	runtime := newRuntime(t, authority, adapter, &fakeWorkspace{}, validator)

	for attempt := 0; attempt < 2; attempt++ {
		result := runtime.Invoke(context.Background(), invocation())
		if result.Failure == nil || result.Failure.Code != toolruntime.FailureContainerExecution {
			t.Fatalf("attempt %d = %#v", attempt, result)
		}
	}
	state := runtime.CircuitState(invocation().ToolRef)
	if state.State != toolruntime.CircuitOpen || state.ConsecutiveFailures != 2 {
		t.Fatalf("state = %#v", state)
	}
	blocked := runtime.Invoke(context.Background(), invocation())
	if blocked.Failure == nil || blocked.Failure.Code != toolruntime.FailureCircuitOpen || adapter.calls != 2 {
		t.Fatalf("blocked = %#v calls=%d", blocked, adapter.calls)
	}
	if failure := runtime.ResetCircuit(invocation().ToolRef); failure != nil {
		t.Fatalf("reset: %v", failure)
	}
	adapter.err = nil
	adapter.result = toolruntime.ContainerResult{Output: map[string]any{"recovered": true}}
	result := runtime.Invoke(context.Background(), invocation())
	if result.Failure != nil || runtime.CircuitState(invocation().ToolRef).State != toolruntime.CircuitClosed {
		t.Fatalf("recovered = %#v state=%#v", result, runtime.CircuitState(invocation().ToolRef))
	}
}

type fakeAuthority struct {
	tool toolruntime.ResolvedTool
	err  *toolruntime.Failure
}

func (f *fakeAuthority) ResolveActivatedTool(context.Context, contract.SkillArtifactRef) (toolruntime.ResolvedTool, *toolruntime.Failure) {
	return f.tool, f.err
}

type fakeContainer struct {
	calls  int
	last   toolruntime.ContainerRequest
	result toolruntime.ContainerResult
	err    *toolruntime.Failure
}

func (f *fakeContainer) Run(_ context.Context, request toolruntime.ContainerRequest) (toolruntime.ContainerResult, *toolruntime.Failure) {
	f.calls++
	f.last = request
	return f.result, f.err
}

type fakeWorkspace struct{ calls int }

func (f *fakeWorkspace) ApplyValidatedPatch(context.Context, toolruntime.WorkspaceSnapshot, toolruntime.PatchManifest) (toolruntime.PatchReceipt, *toolruntime.Failure) {
	f.calls++
	return toolruntime.PatchReceipt{Applied: true, RollbackID: "rollback-1"}, nil
}

type fakeValidator struct {
	calls  int
	result toolruntime.ValidationResult
	err    *toolruntime.Failure
}

func (f *fakeValidator) ValidateInvocation(context.Context, toolruntime.ValidationRequest) (toolruntime.ValidationResult, *toolruntime.Failure) {
	f.calls++
	return f.result, f.err
}

func newRuntime(t *testing.T, authority toolruntime.ActivatedToolAuthority, adapter toolruntime.ContainerAdapter, workspace toolruntime.WorkspaceAdapter, validator toolruntime.InvocationValidator) *toolruntime.Runtime {
	t.Helper()
	runtime, err := toolruntime.New(toolruntime.Config{Authority: authority, Container: adapter, Workspace: workspace, Validator: validator, ConsecutiveFailureThreshold: 2, MaxPatchBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}
func invocation() toolruntime.Invocation {
	return toolruntime.Invocation{ToolRef: testTool().Ref, Input: map[string]any{"query": "fix"}, Workspace: toolruntime.WorkspaceSnapshot{ID: "task-1", BaseDigest: digest("base")}}
}
func testTool() toolruntime.ResolvedTool {
	return toolruntime.ResolvedTool{Ref: contract.SkillArtifactRef{SchemaVersion: contract.SchemaSkillArtifactRef, LineageID: "tool-lineage", Version: "1", Kind: "tool", ArtifactDigest: digest("tool")}, Package: toolruntime.OCIPackage{ImageDigest: digest("image"), Entrypoint: []string{"/tool/run"}, CPUUnits: 1, MemoryBytes: 1024, TimeoutMillis: 1000}, ValidationContractID: "contract-1"}
}
func digest(seed string) string { return contract.DigestBytes([]byte(seed)) }

var _ = errors.New
