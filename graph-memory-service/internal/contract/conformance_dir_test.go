package contract

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocateConformanceDirFromOutsideRepoTree(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "memory_graph_evolving", defaultSpecRelToParent)
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "manifest.json"), []byte(`{"cases":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	start := filepath.Join(root, "evol_bench", "EvoAgentBench")
	if err := os.MkdirAll(start, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := locateConformanceDir(start)
	if err != nil {
		t.Fatalf("locate: %v", err)
	}
	if got != target {
		t.Fatalf("locate = %q, want %q", got, target)
	}
}

func TestDefaultConformanceDirHonorsEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(ConformanceDirEnvVar, dir)
	got, err := DefaultConformanceDir()
	if err != nil || got != dir {
		t.Fatalf("DefaultConformanceDir = %q %v", got, err)
	}
}
