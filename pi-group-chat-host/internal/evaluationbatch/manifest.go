// Package evaluationbatch freezes the benchmark known set: one manifest is
// the authoritative set of logical runs an EvaluationBatch barrier waits on.
// The manifest's logical identity is the frozen six-field tuple
// (evaluation_batch_id, task_id, episode_id, arm_id, seed, logical_run_id)
// (SC-2.1); retries become attempts of the same logical run and never new
// entries, so a manifest that repeats a logical run is malformed and fails
// closed at parse time.
package evaluationbatch

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Episode is one manifest line. Field semantics mirror the bench-runner
// manifest contract: order defaults to manifest line order and family_id
// falls back to domain, then benchmark. Runner-side payloads (stage_files,
// user_simulator) stay the runner's concern and are not carried here.
type Episode struct {
	Benchmark string `json:"benchmark"`
	Domain    string `json:"domain"`
	TaskID    string `json:"task_id"`
	EpisodeID string `json:"episode_id"`
	FamilyID  string `json:"family_id"`
	Role      string `json:"role"`
	Split     string `json:"split"`
	Order     *int   `json:"order"`
	Turns     []struct {
		Prompt string `json:"prompt"`
	} `json:"turns"`

	// Runner-side extras are declared so the strict decoder can tell a known
	// runner payload from a typo'd core field: only genuinely unknown keys
	// are rejected.
	StageFiles    json.RawMessage `json:"stage_files,omitempty"`
	UserSimulator json.RawMessage `json:"user_simulator,omitempty"`
}

// LogicalRunKey is the canonical logical-run identity: a struct, so no
// delimiter concatenation can collide two distinct id pairs into one key.
type LogicalRunKey struct {
	TaskID    string
	EpisodeID string
}

// BatchManifest is the contract-envelope form of the frozen known set
// (PG-00B evaluation_batch_manifest.schema.json): batch-level identity plus
// the logical runs the barrier closes over.
type BatchManifest struct {
	EvaluationBatchID string            `json:"evaluation_batch_id"`
	ArmID             string            `json:"arm_id"`
	Seed              int64             `json:"seed"`
	LogicalRuns       []BatchLogicalRun `json:"logical_runs"`
}

// BatchLogicalRun is one frozen logical run of a BatchManifest.
type BatchLogicalRun struct {
	LogicalRunID      string   `json:"logical_run_id"`
	TaskID            string   `json:"task_id"`
	RoomID            string   `json:"room_id,omitempty"`
	EpisodeID         string   `json:"episode_id"`
	ExpectedAgentRuns []string `json:"expected_agent_runs"`
}

// ParseBatchManifest parses the contract-envelope manifest and fails closed:
// every identity field of the SC-2.1 tuple must be present (the batch-level
// evaluation_batch_id/arm_id/seed are shared by all logical runs), unknown
// fields are rejected, and both the full identity tuple and the logical_run_id
// string must be unique across the known set.
func ParseBatchManifest(r io.Reader) (BatchManifest, error) {
	decoder := json.NewDecoder(r)
	var raw map[string]json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return BatchManifest{}, fmt.Errorf("batch manifest: %w", err)
	}
	if err := requireDecoderEOF(decoder); err != nil {
		return BatchManifest{}, fmt.Errorf("batch manifest: %w", err)
	}
	seed, present := raw["seed"]
	if !present || bytes.Equal(bytes.TrimSpace(seed), []byte("null")) {
		return BatchManifest{}, fmt.Errorf("batch manifest: missing seed")
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return BatchManifest{}, fmt.Errorf("batch manifest: re-encode strict input: %w", err)
	}
	strict := json.NewDecoder(bytes.NewReader(encoded))
	strict.DisallowUnknownFields()
	var manifest BatchManifest
	if err := strict.Decode(&manifest); err != nil {
		return BatchManifest{}, fmt.Errorf("batch manifest: %w", err)
	}
	if err := requireDecoderEOF(strict); err != nil {
		return BatchManifest{}, fmt.Errorf("batch manifest: %w", err)
	}
	if manifest.EvaluationBatchID == "" || manifest.ArmID == "" {
		return BatchManifest{}, fmt.Errorf("batch manifest: missing evaluation_batch_id/arm_id")
	}
	if len(manifest.LogicalRuns) == 0 {
		return BatchManifest{}, fmt.Errorf("batch manifest: logical_runs is empty")
	}
	seenTuple := make(map[LogicalRunKey]int)
	seenRunID := make(map[string]int)
	for index, run := range manifest.LogicalRuns {
		position := index + 1
		if run.LogicalRunID == "" || run.TaskID == "" || run.EpisodeID == "" {
			return BatchManifest{}, fmt.Errorf("batch manifest: logical run %d: missing logical_run_id/task_id/episode_id", position)
		}
		if len(run.ExpectedAgentRuns) == 0 {
			return BatchManifest{}, fmt.Errorf("batch manifest: logical run %d (%s): expected_agent_runs is empty", position, run.LogicalRunID)
		}
		key := LogicalRunKey{TaskID: run.TaskID, EpisodeID: run.EpisodeID}
		if previous, duplicate := seenTuple[key]; duplicate {
			return BatchManifest{}, fmt.Errorf("batch manifest: logical run %d repeats task %q episode %q (first seen at %d); manifests must freeze a unique known set", position, run.TaskID, run.EpisodeID, previous)
		}
		seenTuple[key] = position
		if previous, duplicate := seenRunID[run.LogicalRunID]; duplicate {
			return BatchManifest{}, fmt.Errorf("batch manifest: logical run %d repeats logical_run_id %q (first seen at %d)", position, run.LogicalRunID, previous)
		}
		seenRunID[run.LogicalRunID] = position
	}
	return manifest, nil
}

func requireDecoderEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

// ParseManifest reads one JSONL manifest and returns its episodes in
// manifest order. Every line must carry task_id, episode_id and a non-empty
// first turn; a repeated (task_id, episode_id) pair is a parse error because
// the batch barrier can only close over a unique known set.
func ParseManifest(r io.Reader) ([]Episode, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var episodes []Episode
	seen := make(map[LogicalRunKey]int)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var episode Episode
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&episode); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		if err := requireDecoderEOF(decoder); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		if episode.TaskID == "" || episode.EpisodeID == "" || len(episode.Turns) == 0 || episode.Turns[0].Prompt == "" {
			return nil, fmt.Errorf("line %d: missing task_id/episode_id/turns", lineNumber)
		}
		if episode.FamilyID == "" {
			episode.FamilyID = episode.Domain
		}
		if episode.FamilyID == "" {
			episode.FamilyID = episode.Benchmark
		}
		if episode.FamilyID == "" {
			episode.FamilyID = "default"
		}
		if episode.Order == nil {
			order := lineNumber - 1
			episode.Order = &order
		}
		key := LogicalRunKey{TaskID: episode.TaskID, EpisodeID: episode.EpisodeID}
		if previous, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("line %d: duplicate logical run task %q episode %q (first seen on line %d); manifests must freeze a unique known set", lineNumber, episode.TaskID, episode.EpisodeID, previous)
		}
		seen[key] = lineNumber
		episodes = append(episodes, episode)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(episodes) == 0 {
		return nil, fmt.Errorf("manifest has no episodes")
	}
	return episodes, nil
}
