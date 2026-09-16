package contractconformance

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func corpusRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate parity test source")
	}
	return filepath.Clean(filepath.Join(
		filepath.Dir(file),
		"..", "..", "..",
		"specs", "benchmark-diagnosis-replay-trajectory-export", "conformance",
	))
}

func TestGoRunnerMatchesManifestVerdicts(t *testing.T) {
	root := corpusRoot(t)
	got, err := CorpusVerdicts(root)
	if err != nil {
		t.Fatalf("run Go corpus validator: %v", err)
	}
	want, err := DeclaredVerdicts(root)
	if err != nil {
		t.Fatalf("read declared corpus verdicts: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Go corpus verdicts = %#v, want %#v", got, want)
	}
}

func TestGoRunnerMatchesPythonRunner(t *testing.T) {
	root := corpusRoot(t)
	pythonRunner := filepath.Join(root, "tests", "test_corpus.py")
	output, err := exec.Command("python3", pythonRunner, "--report").Output()
	if err != nil {
		t.Fatalf("run Python corpus validator: %v", err)
	}
	var pythonVerdicts map[string]bool
	if err := json.Unmarshal(output, &pythonVerdicts); err != nil {
		t.Fatalf("decode Python verdict report %q: %v", output, err)
	}
	goVerdicts, err := CorpusVerdicts(root)
	if err != nil {
		t.Fatalf("run Go corpus validator: %v", err)
	}
	if !reflect.DeepEqual(goVerdicts, pythonVerdicts) {
		t.Fatalf("Go verdicts = %#v, Python verdicts = %#v", goVerdicts, pythonVerdicts)
	}
}
