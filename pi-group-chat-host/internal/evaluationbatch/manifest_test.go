package evaluationbatch

import (
	"bytes"
	"testing"
)

func TestParseManifestRejectsDuplicateLogicalRuns(t *testing.T) {
	t.Parallel()

	manifest := `{"benchmark":"evoagentbench","domain":"code","task_id":"task-1","episode_id":"episode-1","family_id":"family-1","split":"test","order":0,"turns":[{"prompt":"first"}]}
{"benchmark":"evoagentbench","domain":"code","task_id":"task-1","episode_id":"episode-1","family_id":"family-1","split":"test","order":1,"turns":[{"prompt":"duplicate"}]}
`

	if _, err := ParseManifest(bytes.NewBufferString(manifest)); err == nil {
		t.Fatal("duplicate logical run was accepted; EvaluationBatch manifests must freeze a unique known set")
	}
}

func TestParseBatchManifestRejectsIncompleteOrAmbiguousKnownSet(t *testing.T) {
	t.Parallel()

	valid := `{"evaluation_batch_id":"batch-1","arm_id":"arm-a","seed":7,"logical_runs":[{"logical_run_id":"run-1","task_id":"task-1","episode_id":"episode-1","expected_agent_runs":["agent-a"]}]}`
	tests := []struct {
		name     string
		manifest string
	}{
		{name: "missing seed", manifest: `{"evaluation_batch_id":"batch-1","arm_id":"arm-a","logical_runs":[{"logical_run_id":"run-1","task_id":"task-1","episode_id":"episode-1","expected_agent_runs":["agent-a"]}]}`},
		{name: "null seed", manifest: `{"evaluation_batch_id":"batch-1","arm_id":"arm-a","seed":null,"logical_runs":[{"logical_run_id":"run-1","task_id":"task-1","episode_id":"episode-1","expected_agent_runs":["agent-a"]}]}`},
		{name: "unknown field", manifest: `{"evaluation_batch_id":"batch-1","arm_id":"arm-a","seed":7,"unknown":true,"logical_runs":[{"logical_run_id":"run-1","task_id":"task-1","episode_id":"episode-1","expected_agent_runs":["agent-a"]}]}`},
		{name: "trailing JSON", manifest: valid + ` {"not":"part of the manifest"}`},
		{name: "same task episode under different run ID", manifest: `{"evaluation_batch_id":"batch-1","arm_id":"arm-a","seed":7,"logical_runs":[{"logical_run_id":"run-1","task_id":"task-1","episode_id":"episode-1","expected_agent_runs":["agent-a"]},{"logical_run_id":"run-2","task_id":"task-1","episode_id":"episode-1","expected_agent_runs":["agent-b"]}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseBatchManifest(bytes.NewBufferString(test.manifest)); err == nil {
				t.Fatal("malformed frozen batch manifest was accepted")
			}
		})
	}
}

func TestParseManifestRejectsUnknownAndTrailingJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		manifest string
	}{
		{name: "unknown field", manifest: `{"task_id":"task-1","episode_id":"episode-1","turns":[{"prompt":"hello"}],"typo":true}`},
		{name: "trailing JSON", manifest: `{"task_id":"task-1","episode_id":"episode-1","turns":[{"prompt":"hello"}]} {"task_id":"task-2"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseManifest(bytes.NewBufferString(test.manifest)); err == nil {
				t.Fatal("non-canonical JSONL entry was accepted")
			}
		})
	}
}
