package main

import (
	"strings"
	"testing"
)

func TestParseServerConfigAcceptsConformanceDirFlag(t *testing.T) {
	config, err := parseServerConfig([]string{
		"-token", "bench-token",
		"-conformance-dir", "/tmp/rsih-conformance",
	}, func(string) string { return "" })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if config.conformanceDir != "/tmp/rsih-conformance" {
		t.Fatalf("conformanceDir = %q", config.conformanceDir)
	}
}

func TestParseServerConfigConformanceDirFallsBackToEnv(t *testing.T) {
	config, err := parseServerConfig([]string{"-token", "bench-token"}, func(name string) string {
		if name == "RSIH_CONFORMANCE_DIR" {
			return "/env/rsih"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if config.conformanceDir != "/env/rsih" {
		t.Fatalf("conformanceDir = %q", config.conformanceDir)
	}
}

func TestParseServerConfigGraphMemoryConformanceDirWinsOverRSIH(t *testing.T) {
	config, err := parseServerConfig([]string{"-token", "bench-token"}, func(name string) string {
		switch name {
		case "GRAPH_MEMORY_CONFORMANCE_DIR":
			return "/env/gms"
		case "RSIH_CONFORMANCE_DIR":
			return "/env/rsih"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if config.conformanceDir != "/env/gms" {
		t.Fatalf("conformanceDir = %q", config.conformanceDir)
	}
}

func TestParseServerConfigRejectsUnexpectedPositional(t *testing.T) {
	_, err := parseServerConfig([]string{"-token", "bench-token", "extra"}, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "unexpected positional") {
		t.Fatalf("error = %v", err)
	}
}
