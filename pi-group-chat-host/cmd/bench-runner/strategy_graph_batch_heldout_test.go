package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProductionHeldOutSkipRecallOfferAndDisposition(t *testing.T) {
	ledger, err := loadPinned0917Ledger()
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	skill := ledger.Skills[0]
	selection := "SKILL_SELECTION\\nskill_id: " + skill.SkillReference + "\\nsha256: " + skill.SHA256 + "\\ntrigger: " + strings.ReplaceAll(skill.Trigger, "\"", "'")
	recallHits := 0
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/recalls") {
			recallHits++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"request_id":"r","items":[{"content":"should not leak","source_space_id":"space-x","memory_version":1,"citation":{"citation_id":"c1","evidence_batch_id":"b1","event_ids":["e1"]},"score":0.9}],"degradation":{"state":"complete","reasons":[]}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer memory.Close()

	outDir := t.TempDir()
	pi := writeGraphBatchFakePi(t, skill.SkillReference, selection)
	episode := manifestEpisode{
		EpisodeID: "abc320_a", TaskID: "abc320_a", FamilyID: "code_implementation",
		Benchmark: "evoagentbench", Domain: "code_implementation", Split: "test",
		Turns: []struct {
			Prompt string `json:"prompt"`
		}{{Prompt: "Print A^B+B^A"}},
	}
	exec, err := newProductionGraphBatchHeldOut(armConfig{
		arm: graphBatchStrategyID, evaluationID: "eval-a0", seed: 1,
		gmsURL: memory.URL, gmsToken: "token", piBinary: pi,
		provider: "openrouter", model: "qwen/qwen3.5-27b",
		outDir: outDir, episodeTimeout: 30 * time.Second, piEnv: []string{"HOME", "PATH"},
	})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	got, err := exec.Execute(context.Background(), "sha256:"+strings.Repeat("0", 64), graphBatchRetrieval{ManifestDigest: "sha256:" + strings.Repeat("0", 64)}, graphBatchHeldOutSpec{
		Sequence: 1, AttemptID: episode.EpisodeID, Episode: &episode,
	}, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got.Record == nil {
		t.Fatal("missing attempt record")
	}
	record := got.Record
	if record.SkillRetrievalStatus != "published" && record.DeliveryStatus != deliveryStatusTaskFinishedFirst {
		t.Fatalf("retrieval status=%q delivery=%q text=%v err=%q", record.SkillRetrievalStatus, record.DeliveryStatus, record.SkillRetrievalText, record.Error)
	}
	if !record.StartOverlap {
		t.Fatal("task and memory start times must overlap")
	}
	if record.RecallCitations != 0 || record.RecallState == "complete" {
		t.Fatalf("task recall leaked: state=%q citations=%d", record.RecallState, record.RecallCitations)
	}
	if record.UsedResumeFlag || record.UsedContinueFlag {
		t.Fatal("exact-session path used --resume or --continue")
	}
	if recallHits == 0 {
		t.Fatal("retrieval turn must still be allowed to recall; only the task turn skips it")
	}
	if len(record.Offers) == 0 || record.Offers[0].SkillReference != skill.SkillReference {
		t.Fatalf("offers = %#v", record.Offers)
	}
	if record.DeliveryStatus == deliveryStatusDelivered {
		if record.SegmentReason != "DIRECTED_MENTION" || record.ContinuationOf == "" {
			t.Fatalf("resume segment = reason=%q continuation_of=%q", record.SegmentReason, record.ContinuationOf)
		}
		if record.AbortCount < 1 || record.ResumeCount < 1 {
			t.Fatalf("interrupt/resume counts = %d/%d", record.AbortCount, record.ResumeCount)
		}
		if len(record.Served) == 0 || record.Served[0].BodyDigest != skill.BodyDigest {
			t.Fatalf("served = %#v, want digest %s", record.Served, skill.BodyDigest)
		}
		if record.Disposition != "accepted" {
			t.Fatalf("disposition = %q reason=%q", record.Disposition, record.DispositionReason)
		}
	}
	output := ""
	if record.FinalCodeOutput != nil {
		output += *record.FinalCodeOutput
	}
	if record.FinalOutput != nil {
		output += *record.FinalOutput
	}
	for _, entry := range record.Transcript {
		output += "\n" + entry.Content
	}
	if !strings.Contains(output, "TASK_COMPLETE") && !strings.Contains(output, "print(320)") {
		t.Fatalf("missing task solution: final=%v graded=%v", record.FinalOutput, record.FinalCodeOutput)
	}
	if !strings.Contains(output, "SKILL_ACCEPTED") || !strings.Contains(output, "@"+graphBatchMemoryAgentID) {
		t.Fatalf("missing §4.4 SKILL_ACCEPTED publication: %q", output)
	}
	if !record.NamedInReasoning {
		t.Fatalf("accepted skill was not named in reasoning")
	}
}

func TestProductionHeldOutDeclinedHasNoOfferOrFence(t *testing.T) {
	memory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/recalls") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"request_id":"r","items":[],"degradation":{"state":"empty","reasons":[]}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer memory.Close()
	outDir := t.TempDir()
	pi := writeGraphBatchFakePi(t, "skill://evaluation/unused@1", "NO_SKILL")
	episode := manifestEpisode{
		EpisodeID: "3423", TaskID: "3423", FamilyID: "code_implementation",
		Benchmark: "evoagentbench", Domain: "code_implementation", Split: "test",
		Turns: []struct {
			Prompt string `json:"prompt"`
		}{{Prompt: "Solve 3423"}},
	}
	exec, err := newProductionGraphBatchHeldOut(armConfig{
		arm: graphBatchStrategyID, evaluationID: "eval-a0", seed: 1,
		gmsURL: memory.URL, gmsToken: "token", piBinary: pi,
		provider: "openrouter", model: "qwen/qwen3.5-27b",
		outDir: outDir, episodeTimeout: 30 * time.Second, piEnv: []string{"HOME", "PATH"},
	})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	got, err := exec.Execute(context.Background(), "sha256:"+strings.Repeat("0", 64), graphBatchRetrieval{}, graphBatchHeldOutSpec{
		Sequence: 1, AttemptID: episode.EpisodeID, Episode: &episode,
	}, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	record := got.Record
	if record.SkillRetrievalStatus != "declined" {
		t.Fatalf("retrieval status=%q text=%v", record.SkillRetrievalStatus, record.SkillRetrievalText)
	}
	if record.DeliveryStatus != deliveryStatusDeclined {
		t.Fatalf("delivery = %q, want declined", record.DeliveryStatus)
	}
	if record.AbortCount != 0 {
		t.Fatalf("declined path must not interrupt, abort=%d", record.AbortCount)
	}
	if len(record.Offers) != 0 || len(record.Served) != 0 || record.Disposition != "" {
		t.Fatalf("declined path must skip offer/fence: offers=%#v served=%#v disposition=%q", record.Offers, record.Served, record.Disposition)
	}
}

func writeGraphBatchFakePi(t *testing.T, skillRef, selection string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "fake-pi")
	frames := map[string][]map[string]any{
		"retr": {
			{"id": "ignored", "type": "response", "command": "prompt", "success": true},
			{"type": "agent_start"},
			{"type": "tool_execution_start", "toolCallId": "retr-send", "toolName": "room_send", "args": map[string]string{"client_operation_id": "retr", "content": strings.ReplaceAll(selection, "\\n", "\n")}},
			{"type": "tool_execution_end", "toolCallId": "retr-send", "toolName": "room_send", "result": map[string]any{"content": []map[string]string{{"type": "text", "text": "acknowledged"}}}, "isError": false},
			{"type": "agent_end", "messages": []any{}, "willRetry": false},
			{"type": "agent_settled"},
		},
		"task": {
			{"id": "ignored", "type": "response", "command": "prompt", "success": true},
			{"type": "agent_start"},
			{"type": "tool_execution_start", "toolCallId": "get-1", "toolName": "skill_get", "args": map[string]string{"skill_reference": skillRef}},
			{"type": "tool_execution_end", "toolCallId": "get-1", "toolName": "skill_get", "result": map[string]any{"content": []map[string]string{{"type": "text", "text": "SKILL_BODY"}}}, "isError": false},
			{"type": "tool_execution_start", "toolCallId": "fb-1", "toolName": "skill_feedback", "args": map[string]string{"skill_reference": skillRef, "disposition": "accepted", "reason": "use " + skillRef}},
			{"type": "tool_execution_end", "toolCallId": "fb-1", "toolName": "skill_feedback", "result": map[string]any{"content": []map[string]string{{"type": "text", "text": "SKILL_FEEDBACK accepted"}}}, "isError": false},
			{"type": "tool_execution_start", "toolCallId": "send-1", "toolName": "room_send", "args": map[string]string{"client_operation_id": "done", "content": "```python\nprint(320)\n```\nTASK_COMPLETE"}},
			{"type": "tool_execution_end", "toolCallId": "send-1", "toolName": "room_send", "result": map[string]any{"content": []map[string]string{{"type": "text", "text": "acknowledged"}}}, "isError": false},
			{"type": "agent_end", "messages": []any{}, "willRetry": false},
			{"type": "agent_settled"},
		},
	}
	raw, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal frames: %v", err)
	}
	framePath := filepath.Join(dir, "frames.json")
	if err := os.WriteFile(framePath, raw, 0o644); err != nil {
		t.Fatalf("write frames: %v", err)
	}
	script := `#!/usr/bin/env python3
import json, sys, threading, time
if len(sys.argv) > 1 and sys.argv[1] == "--version":
    print("0.85.1")
    raise SystemExit(0)
if "--resume" in sys.argv or "--continue" in sys.argv:
    raise SystemExit("forbidden interactive resume flag")
frames = json.load(open("` + framePath + `"))
aborted = False

def emit(key, req_id):
    for frame in frames[key]:
        frame = dict(frame)
        if frame.get("type") == "response":
            frame["id"] = req_id
        print(json.dumps(frame), flush=True)

def idle_settle():
    time.sleep(0.5)
    if not aborted:
        print(json.dumps({"type": "agent_end", "messages": [], "willRetry": False}), flush=True)
        print(json.dumps({"type": "agent_settled"}), flush=True)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    kind = req.get("type")
    req_id = req.get("id", "ignored")
    if kind == "abort":
        aborted = True
        print(json.dumps({"id": req_id, "type": "response", "command": "abort", "success": True}), flush=True)
        print(json.dumps({"type": "agent_settled"}), flush=True)
        continue
    if kind in ("get_state", "clear_queue"):
        print(json.dumps({"id": req_id, "type": "response", "command": kind, "success": True, "data": {}}), flush=True)
        continue
    if kind != "prompt":
        continue
    message = req.get("message") or ""
    if "Skill retrieval turn" in message or str(req_id).endswith("-retr"):
        emit("retr", req_id)
        continue
    if "Directed skill offers" in message:
        emit("task", req_id)
        continue
    print(json.dumps({"id": req_id, "type": "response", "command": "prompt", "success": True}), flush=True)
    print(json.dumps({"type": "agent_start"}), flush=True)
    threading.Thread(target=idle_settle, daemon=True).start()
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}
	return path
}
