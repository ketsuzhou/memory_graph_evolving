package toolruntime_test

import (
	"context"
	"os/exec"
	"testing"

	"river2.dev/graph-memory-service/internal/skillevolution/toolruntime"
)

func TestDockerAdapterIntegrationRequiresLocalDocker(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker integration requires a local docker binary")
	}
	adapter := toolruntime.NewDockerAdapter("docker")
	if _, failure := adapter.Run(context.Background(), toolruntime.ContainerRequest{}); failure == nil {
		t.Fatal("empty Docker request must fail typed validation")
	}
}
