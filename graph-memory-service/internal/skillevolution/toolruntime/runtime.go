// Package toolruntime invokes only exact activated Tool Skills through a
// task-private copy-on-write workspace. OCI/container, validation, patch
// application, logs, and circuit accounting stay behind this deep module's
// public Runtime seam.
package toolruntime

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
)

type FailureCode string

const (
	FailureToolNotActivated      FailureCode = "TOOL_NOT_ACTIVATED"
	FailureToolKindInvalid       FailureCode = "TOOL_KIND_INVALID"
	FailureInvalidInput          FailureCode = "TOOL_INPUT_INVALID"
	FailureContainerExecution    FailureCode = "TOOL_CONTAINER_EXECUTION_FAILED"
	FailureValidation            FailureCode = "TOOL_VALIDATION_FAILED"
	FailurePatchPathNotAllowed   FailureCode = "PATCH_PATH_NOT_ALLOWED"
	FailureWorkspaceBaseMismatch FailureCode = "WORKSPACE_BASE_DIGEST_MISMATCH"
	FailurePatchTooLarge         FailureCode = "PATCH_TOO_LARGE"
	FailurePatchFileType         FailureCode = "PATCH_FILE_TYPE_NOT_ALLOWED"
	FailurePatchMalformed        FailureCode = "PATCH_MANIFEST_INVALID"
	FailurePatchApply            FailureCode = "PATCH_APPLY_FAILED"
	FailureCircuitOpen           FailureCode = "TOOL_CIRCUIT_OPEN"
	FailureAdapterUnavailable    FailureCode = "TOOL_ADAPTER_UNAVAILABLE"
)

// Failure is a typed, caller-safe failure. Detail is diagnostic-only; callers
// branch on Code rather than container output or runtime internals.
type Failure struct {
	Code   FailureCode
	Detail string
}

func (f *Failure) Error() string { return string(f.Code) + ": " + f.Detail }
func NewFailure(code FailureCode, detail string) *Failure {
	return &Failure{Code: code, Detail: detail}
}

// WorkspaceSnapshot identifies only a task-private workspace base. It is not
// a host path and the public runtime API never accepts arbitrary mount paths.
type WorkspaceSnapshot struct {
	ID         string
	BaseDigest string
}

type FileType string

const (
	FileTypeRegular FileType = "regular"
	FileTypeDelete  FileType = "delete"
)

// PatchChange describes one structured workspace mutation. BlobRef and Diff
// are audit carriers; apply implementations choose their private storage form.
type PatchChange struct {
	Path      string
	FileType  FileType
	BlobRef   string
	Diff      string
	SizeBytes int64
}

type RollbackMetadata struct {
	RollbackID string
	Reversible bool
}

// PatchManifest is the only write effect ToolRuntime accepts from a sandbox.
type PatchManifest struct {
	BaseWorkspaceDigest string
	AllowedPaths        []string
	Changes             []PatchChange
	Rollback            RollbackMetadata
}

// ValidateForApply verifies the patch before a workspace adapter can mutate
// anything. It is independent of the adapter so every implementation shares
// path/base/size/type protection.
func (p PatchManifest) ValidateForApply(workspace WorkspaceSnapshot, maxBytes int64) *Failure {
	if workspace.ID == "" || !isDigest(workspace.BaseDigest) || p.BaseWorkspaceDigest != workspace.BaseDigest {
		return NewFailure(FailureWorkspaceBaseMismatch, "patch base digest does not equal the task-private workspace snapshot")
	}
	if len(p.AllowedPaths) == 0 || p.Rollback.RollbackID == "" {
		return NewFailure(FailurePatchMalformed, "patch requires allowed paths and rollback metadata")
	}
	allowed := append([]string(nil), p.AllowedPaths...)
	sort.Strings(allowed)
	var total int64
	for _, change := range p.Changes {
		if change.Path == "" || !safeRelativePath(change.Path) || !pathAllowed(change.Path, allowed) {
			return NewFailure(FailurePatchPathNotAllowed, "patch change path is outside the declared allowlist")
		}
		if change.FileType != FileTypeRegular && change.FileType != FileTypeDelete {
			return NewFailure(FailurePatchFileType, "patch change type is not regular or delete")
		}
		if change.SizeBytes < 0 {
			return NewFailure(FailurePatchMalformed, "patch change size is negative")
		}
		total += change.SizeBytes
		if maxBytes > 0 && total > maxBytes {
			return NewFailure(FailurePatchTooLarge, "patch exceeds configured maximum size")
		}
		if change.FileType == FileTypeRegular && change.BlobRef == "" && change.Diff == "" {
			return NewFailure(FailurePatchMalformed, "regular patch change requires blob ref or diff")
		}
	}
	return nil
}

func safeRelativePath(value string) bool {
	clean := path.Clean(value)
	return clean != "." && !strings.HasPrefix(clean, "../") && !strings.HasPrefix(clean, "/") && clean == value
}
func pathAllowed(value string, prefixes []string) bool {
	for _, prefix := range prefixes {
		clean := strings.TrimPrefix(path.Clean(prefix), "./")
		if clean == "." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
			continue
		}
		if strings.HasSuffix(prefix, "/") {
			if strings.HasPrefix(value, prefix) {
				return true
			}
		} else if value == clean {
			return true
		}
	}
	return false
}
func isDigest(value string) bool {
	return len(value) == len("sha256:")+64 && strings.HasPrefix(value, "sha256:")
}

// OCIPackage is the resolved, content-addressed execution contract. The
// runtime owns how this becomes a container process.
type OCIPackage struct {
	ImageDigest   string
	Entrypoint    []string
	CPUUnits      int64
	MemoryBytes   int64
	TimeoutMillis int64
}

type ResolvedTool struct {
	Ref                  contract.SkillArtifactRef
	Package              OCIPackage
	ValidationContractID string
}

// Invocation accepts no image ref, command, mount path, capability list, or
// patch path from callers. Those derive from the exact activated Tool Skill.
type Invocation struct {
	ToolRef   contract.SkillArtifactRef
	Input     map[string]any
	Workspace WorkspaceSnapshot
}

type ValidationCheck struct {
	CheckID string
	Passed  bool
	Reason  string
}
type ValidationResult struct {
	Passed bool
	Checks []ValidationCheck
}
type PatchReceipt struct {
	Applied    bool
	RollbackID string
}
type InvocationResult struct {
	ToolRef    contract.SkillArtifactRef
	Output     map[string]any
	Patch      *PatchReceipt
	Validation ValidationResult
	Failure    *Failure
}

// ActivatedToolAuthority must resolve only the current active exact tool ref.
type ActivatedToolAuthority interface {
	ResolveActivatedTool(context.Context, contract.SkillArtifactRef) (ResolvedTool, *Failure)
}
type ContainerRequest struct {
	Tool           ResolvedTool
	Input          map[string]any
	Workspace      WorkspaceSnapshot
	NetworkEnabled bool
	ReadOnlyInputs bool
	TaskPrivateCOW bool
}
type ContainerResult struct {
	Output map[string]any
	Patch  *PatchManifest
}
type ContainerAdapter interface {
	Run(context.Context, ContainerRequest) (ContainerResult, *Failure)
}
type WorkspaceAdapter interface {
	ApplyValidatedPatch(context.Context, WorkspaceSnapshot, PatchManifest) (PatchReceipt, *Failure)
}
type ValidationRequest struct {
	Tool      ResolvedTool
	Input     map[string]any
	Output    map[string]any
	Patch     *PatchManifest
	Workspace WorkspaceSnapshot
}
type InvocationValidator interface {
	ValidateInvocation(context.Context, ValidationRequest) (ValidationResult, *Failure)
}

type CircuitStatus string

const (
	CircuitClosed CircuitStatus = "closed"
	CircuitOpen   CircuitStatus = "open"
)

type CircuitSnapshot struct {
	State               CircuitStatus
	ConsecutiveFailures int
}

type Config struct {
	Authority                   ActivatedToolAuthority
	Container                   ContainerAdapter
	Workspace                   WorkspaceAdapter
	Validator                   InvocationValidator
	ConsecutiveFailureThreshold int
	MaxPatchBytes               int64
}

// Runtime is the public ToolRuntime seam. Its adapters are deliberately
// private implementation dependencies selected at construction.
type Runtime struct {
	authority ActivatedToolAuthority
	container ContainerAdapter
	workspace WorkspaceAdapter
	validator InvocationValidator
	threshold int
	maxPatch  int64
	mu        sync.Mutex
	circuits  map[string]CircuitSnapshot
}

func New(cfg Config) (*Runtime, error) {
	if cfg.Authority == nil || cfg.Container == nil || cfg.Workspace == nil || cfg.Validator == nil {
		return nil, fmt.Errorf("toolruntime: authority, container, workspace, and validator are required")
	}
	if cfg.ConsecutiveFailureThreshold < 1 || cfg.MaxPatchBytes < 1 {
		return nil, fmt.Errorf("toolruntime: positive failure threshold and patch limit are required")
	}
	return &Runtime{authority: cfg.Authority, container: cfg.Container, workspace: cfg.Workspace, validator: cfg.Validator, threshold: cfg.ConsecutiveFailureThreshold, maxPatch: cfg.MaxPatchBytes, circuits: map[string]CircuitSnapshot{}}, nil
}

func (r *Runtime) Invoke(ctx context.Context, invocation Invocation) InvocationResult {
	key := toolKey(invocation.ToolRef)
	if state := r.CircuitState(invocation.ToolRef); state.State == CircuitOpen {
		return InvocationResult{ToolRef: invocation.ToolRef, Failure: NewFailure(FailureCircuitOpen, "explicit reset is required before this tool may be invoked again")}
	}
	if invocation.ToolRef.Kind != "tool" {
		return r.failed(key, invocation.ToolRef, NewFailure(FailureToolKindInvalid, "invocation ref is not a Tool Skill"))
	}
	if len(invocation.Input) == 0 {
		return r.failed(key, invocation.ToolRef, NewFailure(FailureInvalidInput, "typed input object is required"))
	}
	tool, failure := r.authority.ResolveActivatedTool(ctx, invocation.ToolRef)
	if failure != nil {
		return r.failed(key, invocation.ToolRef, failure)
	}
	if tool.Ref != invocation.ToolRef || tool.Ref.Kind != "tool" {
		return r.failed(key, invocation.ToolRef, NewFailure(FailureToolNotActivated, "authority did not resolve the exact activated Tool Skill"))
	}
	containerResult, failure := r.container.Run(ctx, ContainerRequest{Tool: tool, Input: invocation.Input, Workspace: invocation.Workspace, NetworkEnabled: false, ReadOnlyInputs: true, TaskPrivateCOW: true})
	if failure != nil {
		return r.failed(key, invocation.ToolRef, failure)
	}
	validation, failure := r.validator.ValidateInvocation(ctx, ValidationRequest{Tool: tool, Input: invocation.Input, Output: containerResult.Output, Patch: containerResult.Patch, Workspace: invocation.Workspace})
	if failure != nil {
		return r.failed(key, invocation.ToolRef, failure)
	}
	if !validation.Passed {
		return r.failed(key, invocation.ToolRef, NewFailure(FailureValidation, "Validation Contract rejected invocation output"))
	}
	result := InvocationResult{ToolRef: tool.Ref, Output: containerResult.Output, Validation: validation}
	if containerResult.Patch != nil {
		if failure := containerResult.Patch.ValidateForApply(invocation.Workspace, r.maxPatch); failure != nil {
			return r.failed(key, invocation.ToolRef, failure)
		}
		receipt, failure := r.workspace.ApplyValidatedPatch(ctx, invocation.Workspace, *containerResult.Patch)
		if failure != nil {
			return r.failed(key, invocation.ToolRef, failure)
		}
		result.Patch = &receipt
	}
	r.succeeded(key)
	return result
}

func (r *Runtime) CircuitState(ref contract.SkillArtifactRef) CircuitSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.circuits[toolKey(ref)]
	if state.State == "" {
		state.State = CircuitClosed
	}
	return state
}
func (r *Runtime) ResetCircuit(ref contract.SkillArtifactRef) *Failure {
	if ref.Kind != "tool" {
		return NewFailure(FailureToolKindInvalid, "only Tool Skill circuits can be reset")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.circuits[toolKey(ref)] = CircuitSnapshot{State: CircuitClosed}
	return nil
}
func (r *Runtime) failed(key string, ref contract.SkillArtifactRef, failure *Failure) InvocationResult {
	r.mu.Lock()
	state := r.circuits[key]
	state.State = CircuitClosed
	state.ConsecutiveFailures++
	if state.ConsecutiveFailures >= r.threshold {
		state.State = CircuitOpen
	}
	r.circuits[key] = state
	r.mu.Unlock()
	return InvocationResult{ToolRef: ref, Failure: failure}
}
func (r *Runtime) succeeded(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.circuits[key] = CircuitSnapshot{State: CircuitClosed}
}
func toolKey(ref contract.SkillArtifactRef) string {
	return ref.LineageID + ":" + ref.Version + ":" + ref.ArtifactDigest
}
