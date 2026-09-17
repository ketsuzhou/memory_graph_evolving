package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const gmsConformanceRel = "specs/rsi-harness-skill-evolution/conformance"

func resolveGMSConformanceDir(gmsBinary string) (string, error) {
	if dir := strings.TrimSpace(os.Getenv("RSIH_CONFORMANCE_DIR")); dir != "" {
		return dir, nil
	}
	if dir := strings.TrimSpace(os.Getenv("GRAPH_MEMORY_CONFORMANCE_DIR")); dir != "" {
		return dir, nil
	}
	var starts []string
	if wd, err := os.Getwd(); err == nil && wd != "" {
		starts = append(starts, wd)
	}
	if gmsBinary != "" {
		if abs, err := filepath.Abs(gmsBinary); err == nil {
			starts = append(starts, filepath.Dir(abs))
		}
	}
	if exe, err := os.Executable(); err == nil && exe != "" {
		starts = append(starts, filepath.Dir(exe))
	}
	for _, start := range starts {
		if dir, err := locateGMSConformanceDir(start); err == nil {
			return dir, nil
		}
	}
	return "", fmt.Errorf("cannot locate %s; set RSIH_CONFORMANCE_DIR", gmsConformanceRel)
}

func locateGMSConformanceDir(start string) (string, error) {
	dir := start
	for {
		for _, candidate := range []string{
			filepath.Join(dir, gmsConformanceRel),
			filepath.Join(dir, "memory_graph_evolving", gmsConformanceRel),
			filepath.Join(filepath.Dir(dir), gmsConformanceRel),
		} {
			if isGMSConformanceDir(candidate) {
				return candidate, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("not found upward from %s", start)
		}
		dir = parent
	}
}

func isGMSConformanceDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return false
	}
	_, err = os.Stat(filepath.Join(path, "manifest.json"))
	return err == nil
}

func withGMSConformanceEnv(environ []string, dir string) []string {
	out := make([]string, 0, len(environ)+1)
	replaced := false
	for _, item := range environ {
		if strings.HasPrefix(item, "RSIH_CONFORMANCE_DIR=") {
			out = append(out, "RSIH_CONFORMANCE_DIR="+dir)
			replaced = true
			continue
		}
		out = append(out, item)
	}
	if !replaced {
		out = append(out, "RSIH_CONFORMANCE_DIR="+dir)
	}
	return out
}
