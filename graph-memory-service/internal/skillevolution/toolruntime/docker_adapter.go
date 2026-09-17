package toolruntime

import (
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"strings"
)

// DockerAdapter is the first OCI/container adapter. It uses only the Docker
// CLI from the standard library boundary so no Docker Go dependency is added.
// Production wiring must supply task-private COW mounts through the adapter's
// deployment configuration; tests use a fake ContainerAdapter instead.
type DockerAdapter struct{ binary string }

func NewDockerAdapter(binary string) *DockerAdapter {
	if binary == "" {
		binary = "docker"
	}
	return &DockerAdapter{binary: binary}
}

func (a *DockerAdapter) Run(ctx context.Context, request ContainerRequest) (ContainerResult, *Failure) {
	if !isDigest(request.Tool.Package.ImageDigest) || len(request.Tool.Package.Entrypoint) == 0 || request.NetworkEnabled || !request.ReadOnlyInputs || !request.TaskPrivateCOW || request.Workspace.ID == "" {
		return ContainerResult{}, NewFailure(FailureAdapterUnavailable, "Docker request violates OCI digest or sandbox policy")
	}
	input, err := json.Marshal(request.Input)
	if err != nil {
		return ContainerResult{}, NewFailure(FailureInvalidInput, "typed input cannot encode as JSON")
	}
	args := []string{"run", "--rm", "--network=none", "--read-only", "--cpus=" + strconv.FormatInt(request.Tool.Package.CPUUnits, 10), "--memory=" + strconv.FormatInt(request.Tool.Package.MemoryBytes, 10), request.Tool.Package.ImageDigest}
	args = append(args, request.Tool.Package.Entrypoint...)
	command := exec.CommandContext(ctx, a.binary, args...)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.Output()
	if err != nil {
		return ContainerResult{}, NewFailure(FailureContainerExecution, "Docker OCI command failed")
	}
	var typed map[string]any
	if err := json.Unmarshal(output, &typed); err != nil {
		return ContainerResult{}, NewFailure(FailureContainerExecution, "Docker tool output is not typed JSON")
	}
	return ContainerResult{Output: typed}, nil
}
