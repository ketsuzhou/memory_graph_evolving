package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveGMSConformanceDirUsesEnv(t *testing.T) {
	t.Setenv("RSIH_CONFORMANCE_DIR", "/explicit/rsih")
	t.Setenv("GRAPH_MEMORY_CONFORMANCE_DIR", "/explicit/gms")
	got, err := resolveGMSConformanceDir("/tmp/missing-gms")
	if err != nil || got != "/explicit/rsih" {
		t.Fatalf("resolve = %q %v", got, err)
	}
}

func TestLocateGMSConformanceDirFromOutsideRepoTree(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "memory_graph_evolving", gmsConformanceRel)
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
	got, err := locateGMSConformanceDir(start)
	if err != nil {
		t.Fatalf("locate: %v", err)
	}
	if got != target {
		t.Fatalf("locate = %q, want %q", got, target)
	}
}

func TestWithGMSConformanceEnvReplacesOrAppends(t *testing.T) {
	got := withGMSConformanceEnv([]string{"PATH=/bin", "RSIH_CONFORMANCE_DIR=old"}, "/new")
	if len(got) != 2 || got[1] != "RSIH_CONFORMANCE_DIR=/new" {
		t.Fatalf("replace = %#v", got)
	}
	got = withGMSConformanceEnv([]string{"PATH=/bin"}, "/new")
	if len(got) != 2 || got[1] != "RSIH_CONFORMANCE_DIR=/new" {
		t.Fatalf("append = %#v", got)
	}
}
