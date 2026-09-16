package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"river2.dev/pi-group-chat-host/internal/runtime"
)

func TestLoadManifestResolvesOptionalFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")
	// An evo-style record (no family_id/order/role) followed by a past-style
	// record (every field explicit). The evo record must inherit its family
	// from the domain field and its order from the manifest line position.
	content := `{"benchmark": "evoagentbench", "domain": "code_implementation", "task_id": "1883_C", "episode_id": "1883_C", "split": "train", "turns": [{"prompt": "write code"}]}
{"benchmark": "past_bench", "episode_id": "learn_a", "family_id": "SM01", "order": 3, "role": "learn", "split": "learn", "task_id": "SM01-a", "turns": [{"prompt": "take note"}]}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	episodes, err := loadManifest(path)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if len(episodes) != 2 {
		t.Fatalf("expected 2 episodes, got %d", len(episodes))
	}
	evo := episodes[0]
	if evo.FamilyID != "code_implementation" {
		t.Errorf("evo family fallback: got %q, want code_implementation", evo.FamilyID)
	}
	if evo.Order == nil || *evo.Order != 0 {
		t.Errorf("evo order fallback: got %v, want 0", evo.Order)
	}
	if evo.Benchmark != "evoagentbench" || evo.Domain != "code_implementation" {
		t.Errorf("evo benchmark/domain not preserved: %q/%q", evo.Benchmark, evo.Domain)
	}
	past := episodes[1]
	if past.FamilyID != "SM01" || past.Role != "learn" {
		t.Errorf("past explicit fields overwritten: family=%q role=%q", past.FamilyID, past.Role)
	}
	if past.Order == nil || *past.Order != 3 {
		t.Errorf("past explicit order overwritten: got %v, want 3", past.Order)
	}
}

func TestLoadManifestRejectsMissingRequiredFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")
	if err := os.WriteFile(path, []byte(`{"benchmark": "x", "turns": [{"prompt": "p"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadManifest(path); err == nil {
		t.Fatal("expected error for missing task_id/episode_id, got nil")
	}
}

func TestWarmArmRunsTrainBeforeTestInLineOrder(t *testing.T) {
	episodes := []manifestEpisode{
		{TaskID: "t2", EpisodeID: "test-2", FamilyID: "d", Split: "test", Order: intPtr(3)},
		{TaskID: "t1", EpisodeID: "train-1", FamilyID: "d", Split: "train", Order: intPtr(0)},
		{TaskID: "t3", EpisodeID: "test-1", FamilyID: "d", Split: "test", Order: intPtr(2)},
	}
	ordered := episodesOfFamily(episodes, "d")
	if ordered[0].EpisodeID != "train-1" || ordered[1].EpisodeID != "test-1" || ordered[2].EpisodeID != "test-2" {
		t.Errorf("episodes not in manifest order: %v %v %v", ordered[0].EpisodeID, ordered[1].EpisodeID, ordered[2].EpisodeID)
	}
}

func intPtr(value int) *int { return &value }

func TestLoadManifestParsesStageFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")
	content := `{"benchmark": "finevo-gdpval", "task_id": "t1", "episode_id": "t1", "family_id": "Accountants", "split": "test", "order": 0, "turns": [{"prompt": "audit"}], "stage_files": [{"src": "t1/Population v2.xlsx", "dst": "reference/Population v2.xlsx"}]}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	episodes, err := loadManifest(path)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if !needsRefsRoot(episodes) {
		t.Error("needsRefsRoot must detect stage_files")
	}
	if len(episodes[0].StageFiles) != 1 {
		t.Fatalf("expected 1 stage file, got %d", len(episodes[0].StageFiles))
	}
	if episodes[0].StageFiles[0].Src != "t1/Population v2.xlsx" || episodes[0].StageFiles[0].Dst != "reference/Population v2.xlsx" {
		t.Errorf("stage file not preserved: %+v", episodes[0].StageFiles[0])
	}
}

func TestStageReferenceFiles(t *testing.T) {
	refsRoot := t.TempDir()
	workDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(refsRoot, "task-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(refsRoot, "task-a", "data v2.xlsx"), []byte("xlsx-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	files := []stageFile{
		{Src: "task-a/data v2.xlsx", Dst: "reference/data v2.xlsx"},
		{Src: "task-a/data v2.xlsx", Dst: "reference/nested/more.xlsx"},
	}
	staged, err := stageReferenceFiles(refsRoot, workDir, files)
	if err != nil {
		t.Fatalf("stageReferenceFiles: %v", err)
	}
	if staged != 2 {
		t.Fatalf("expected 2 staged files, got %d", staged)
	}
	for _, dst := range []string{"reference/data v2.xlsx", "reference/nested/more.xlsx"} {
		data, err := os.ReadFile(filepath.Join(workDir, dst))
		if err != nil || string(data) != "xlsx-bytes" {
			t.Errorf("staged file %s missing or wrong: %v %q", dst, err, string(data))
		}
	}
}

func TestStageReferenceFilesRejectsEscapeAndMissingSource(t *testing.T) {
	refsRoot := t.TempDir()
	workDir := t.TempDir()
	if _, err := stageReferenceFiles(refsRoot, workDir, []stageFile{{Src: "a", Dst: "../escape.txt"}}); err == nil {
		t.Error("expected error for dst escaping the workdir")
	}
	if _, err := stageReferenceFiles(refsRoot, workDir, []stageFile{{Src: "a", Dst: "/abs.txt"}}); err == nil {
		t.Error("expected error for absolute dst")
	}
	if _, err := stageReferenceFiles("", workDir, []stageFile{{Src: "a", Dst: "b"}}); err == nil {
		t.Error("expected error for empty refs root")
	}
	staged, err := stageReferenceFiles(refsRoot, workDir, []stageFile{{Src: "missing.bin", Dst: "reference/missing.bin"}})
	if err == nil || staged != 0 {
		t.Errorf("expected missing source failure, got staged=%d err=%v", staged, err)
	}
}

func TestSkillFingerprintNormalizesWhitespace(t *testing.T) {
	first := skillFingerprint("SKILL PROPOSAL\nname:  retry\nsteps:\n1. wait\n2. retry")
	// Formatting drift alone must not mint a second skill.
	second := skillFingerprint("SKILL PROPOSAL  name: retry steps: 1. wait 2. retry")
	if first != second {
		t.Errorf("whitespace normalization broken: %q != %q", first, second)
	}
	if skillFingerprint("different content") == first {
		t.Error("distinct proposals must not collide")
	}
}

func TestRenderSkillBlockReplacedByRetrievalTurn(t *testing.T) {
	// The injection renderer is gone by design: test episodes now get skills
	// through the diagnosis agent's retrieval turn (runRetrievalTurn). The
	// ledger chunker that feeds skills_list is what carries the guarantee.
	ledger := []skillProposal{
		{Sequence: 1, EpisodeID: "ep-a", SHA256: "aaaa11112222", Text: "skill one"},
		{Sequence: 2, EpisodeID: "ep-b", SHA256: "bbbb33334444", Text: "skill two"},
		{Sequence: 3, EpisodeID: "ep-c", SHA256: "cccc55556666", Text: "skill three"},
		{Sequence: 4, EpisodeID: "ep-d", SHA256: "dddd77778888", Text: "skill four"},
	}
	chunks := buildLedgerChunks(ledger)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 ledger chunks (3 entries each), got %d", len(chunks))
	}
	if !strings.Contains(chunks[0], "aaaa11112222") || !strings.Contains(chunks[0], "skill three") {
		t.Errorf("first chunk missing early entries: %q", chunks[0])
	}
	if !strings.Contains(chunks[1], "dddd77778888") {
		t.Errorf("second chunk missing late entry: %q", chunks[1])
	}
}

func TestMemoryTurnToolJSRegistersAllowedNames(t *testing.T) {
	// The JS templates and the turn authorities must agree on tool names:
	// pi's --tools filter drops any extension tool the authority does not
	// carry, which silently blinds the diagnosis/retrieval turns.
	if !strings.Contains(trajectoryToolJS(nil), `name: "`+trajectoryReadToolName+`"`) {
		t.Errorf("trajectoryToolJS must register %q", trajectoryReadToolName)
	}
	if !strings.Contains(skillsToolJS(nil), `name: "`+skillsListToolName+`"`) {
		t.Errorf("skillsToolJS must register %q", skillsListToolName)
	}
}

func TestRoomTranscriptMapsRolesAndSkipsEmpty(t *testing.T) {
	transcript := roomTranscript([]runtime.VisibleMessage{
		{AuthorID: "human", Content: "solve the task"},
		{AuthorID: "agent-primary", Content: "   "},
		{AuthorID: "agent-primary", Content: "```python\nprint(1)\n```"},
	})
	if len(transcript) != 2 {
		t.Fatalf("expected prompt + one publish, got %d entries", len(transcript))
	}
	if transcript[0].Role != "user" || transcript[0].Content != "solve the task" {
		t.Errorf("prompt entry unexpected: %+v", transcript[0])
	}
	if transcript[1].Role != "agent" || !strings.Contains(transcript[1].Content, "python") {
		t.Errorf("publish entry unexpected: %+v", transcript[1])
	}
}

func TestEpisodeRoomIsPerTask(t *testing.T) {
	trainA := episodeRoom("code_implementation", 3, "train")
	trainB := episodeRoom("code_implementation", 4, "train")
	testA := episodeRoom("code_implementation", 5, "test")
	if trainA == trainB || trainB == testA || trainA == testA {
		t.Fatal("every task episode must get its own room")
	}
	if trainA != "room-code-implementation-train-3" || testA != "room-code-implementation-test-5" {
		t.Errorf("room ids unexpected: %q %q", trainA, testA)
	}
	// The warm-skill test-room derivation and the generic one must agree,
	// or registration and the episode loop would drift apart.
	if room, _, _, _ := warmSkillTestRoom("code_implementation", 5); room != testA {
		t.Errorf("warmSkillTestRoom room %q must equal episodeRoom test id %q", room, testA)
	}
}

func TestWarmSkillTestRoomIsolation(t *testing.T) {
	room1, shared1, private1, memoryPrivate1 := warmSkillTestRoom("code_implementation", 4)
	room2, shared2, private2, memoryPrivate2 := warmSkillTestRoom("code_implementation", 5)
	if room1 == room2 || shared1 == shared2 || private1 == private2 || memoryPrivate1 == memoryPrivate2 {
		t.Fatal("each test episode must get its own room and spaces")
	}
	if room1 != "room-code-implementation-test-4" {
		t.Errorf("room id unexpected: %q", room1)
	}
	if shared1 != "space-code-implementation-test-4-shared" || private1 != "space-code-implementation-test-4-private" {
		t.Errorf("space ids unexpected: %q %q", shared1, private1)
	}
	if memoryPrivate1 != "space-code-implementation-test-4-memory-private" {
		t.Errorf("memory-agent space id unexpected: %q", memoryPrivate1)
	}
	if strings.Contains(memoryPrivate1, MemoryAgentPrivateSpaceID("code_implementation")) {
		t.Error("test-room memory-agent space must not alias the train memory-agent space")
	}
}

func TestBuildTrajectoryChunksPacksAndHardSplits(t *testing.T) {
	transcript := []transcriptEntry{
		{Role: "user", Content: "task"},
		{Role: "agent", Content: strings.Repeat("x", maxTrajectoryChunkChars+10)},
		{Role: "agent", Content: "done"},
	}
	chunks := buildTrajectoryChunks(transcript)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks (oversized entry hard-split), got %d", len(chunks))
	}
	if !strings.HasPrefix(chunks[0], "[user] task") {
		t.Errorf("first chunk wrong: %q", chunks[0])
	}
	if len([]rune(chunks[1])) != maxTrajectoryChunkChars {
		t.Errorf("hard-split chunk not capped: %d runes", len([]rune(chunks[1])))
	}
	if !strings.HasSuffix(chunks[2], "[agent] done") {
		t.Errorf("last chunk wrong: %q", chunks[2])
	}
	if buildTrajectoryChunks(nil) != nil {
		t.Error("empty transcript must produce no chunks")
	}
}

func TestSplitSkillProposals(t *testing.T) {
	reply := "Diagnosis: two problems found.\n\nSKILL PROPOSAL\nname: retry-on-timeout\ntrigger: long waits\nsteps: 1. wait 2. retry\npitfalls: none\n\nSKILL PROPOSAL\nname: digit-dp\ntrigger: digit sums\nsteps: 1. dp\npitfalls: leading zeros"
	proposals := splitSkillProposals(reply)
	if len(proposals) != 2 {
		t.Fatalf("expected 2 proposals, got %d", len(proposals))
	}
	if !strings.HasPrefix(proposals[0], "name: retry-on-timeout") {
		t.Errorf("first proposal mangled: %q", proposals[0])
	}
	if !strings.Contains(proposals[1], "leading zeros") {
		t.Errorf("second proposal truncated: %q", proposals[1])
	}
	if got := splitSkillProposals("NO_SKILL"); got != nil {
		t.Errorf("NO_SKILL must yield no proposals, got %v", got)
	}
	if got := splitSkillProposals("SKILL PROPOSAL: NO_SKILL"); got != nil {
		t.Errorf("NO_SKILL after header must be dropped, got %v", got)
	}
}

func TestExtractRetrievedFingerprints(t *testing.T) {
	reply := "@task-agent @memory-agent\n[skill aaaa11112222 from episode 1899_A]\nskill text\n[skill bbbb33334444 from episode 2757]\nmore text"
	got := extractRetrievedFingerprints(reply)
	if len(got) != 2 || got[0] != "aaaa11112222" || got[1] != "bbbb33334444" {
		t.Fatalf("fingerprints wrong: %v", got)
	}
	if got := extractRetrievedFingerprints("NO_SKILL_APPLICABLE"); len(got) != 0 {
		t.Errorf("declined note must yield no fingerprints, got %v", got)
	}
}

func TestTruncateRunesKeepsValidUTF8(t *testing.T) {
	value := "技能一：失败后重试"
	cut := truncateRunes(value, 3)
	if cut != "技能一" {
		t.Errorf("rune cut wrong: %q", cut)
	}
	if truncateRunes(value, 100) != value {
		t.Error("short values must pass through unchanged")
	}
}

func TestSplitConsolidatedSkillsAndStripSources(t *testing.T) {
	reply := "analysis\n\nCONSOLIDATED SKILL\nname: retry\nsteps: wait then retry\nsources: aaaa1111, bbbb2222\n\nCONSOLIDATED SKILL\nname: dp\nsources: cccc3333"
	blocks := splitConsolidatedSkills(reply)
	if len(blocks) != 2 {
		t.Fatalf("expected two consolidated blocks, got %d", len(blocks))
	}
	text, sources, ok := consolidatedSources(blocks[0])
	if !ok || len(sources) != 2 || sources[0] != "aaaa1111" || sources[1] != "bbbb2222" {
		t.Fatalf("sources parse = %q %#v %t", text, sources, ok)
	}
	if strings.Contains(text, "sources:") || !strings.Contains(text, "name: retry") {
		t.Errorf("stored text must retain skill and strip sources: %q", text)
	}
}

func TestResolveSourcePrefixesRejectsMissingAndAmbiguous(t *testing.T) {
	raw := []skillProposal{
		{EpisodeID: "one", SHA256: "aaaa1111ccccdddd", Text: "one"},
		{EpisodeID: "two", SHA256: "bbbb2222eeeeffff", Text: "two"},
	}
	resolved, ok := resolveSourcePrefixes(raw, []string{"aaaa1111", "bbbb2222"})
	if !ok || len(resolved) != 2 || resolved[0].EpisodeID != "one" || resolved[1].EpisodeID != "two" {
		t.Fatalf("resolved sources = %#v, ok=%t", resolved, ok)
	}
	if _, ok := resolveSourcePrefixes(raw, []string{"missing"}); ok {
		t.Fatal("unknown source prefix must fail")
	}
	ambiguous := append(raw, skillProposal{SHA256: "aaaa1111eeeeffff", Text: "three"})
	if _, ok := resolveSourcePrefixes(ambiguous, []string{"aaaa1111"}); ok {
		t.Fatal("ambiguous source prefix must fail")
	}
}

func TestParseConsolidatedSkillsRequiresEveryRawSourceOnce(t *testing.T) {
	raw := []skillProposal{
		{Sequence: 1, EpisodeID: "one", SHA256: "aaaa1111ccccdddd", Text: "one"},
		{Sequence: 2, EpisodeID: "two", SHA256: "bbbb2222eeeeffff", Text: "two"},
	}
	reply := "CONSOLIDATED SKILL\nname: merged\nsources: aaaa1111, bbbb2222"
	consolidated, provenance, ok := parseConsolidatedSkills(reply, raw)
	if !ok || len(consolidated) != 1 || len(provenance[consolidated[0].SHA256]) != 2 {
		t.Fatalf("valid consolidation rejected: %#v %#v ok=%t", consolidated, provenance, ok)
	}
	if strings.Contains(consolidated[0].Text, "sources:") {
		t.Errorf("sources line leaked into stored skill text: %q", consolidated[0].Text)
	}
	if _, _, ok := parseConsolidatedSkills("CONSOLIDATED SKILL\nname: incomplete\nsources: aaaa1111", raw); ok {
		t.Fatal("consolidation that omits a raw source must fail")
	}
}

func TestSkillStrategyForThreeArms(t *testing.T) {
	cases := map[string]skillStrategy{
		"warm-skill":           skillStrategyBatch,
		"warm-skill-batch":     skillStrategyBatch,
		"warm-skill-replay":    skillStrategyReplay,
		"warm-skill-online":    skillStrategyOnline,
		"warm-skill-continual": skillStrategyContinual,
	}
	for arm, want := range cases {
		got, ok := skillStrategyFor(arm)
		if !ok || got != want || !isSkillArm(arm) {
			t.Errorf("strategy for %q = %q, %t; want %q, true", arm, got, ok, want)
		}
	}
	if _, ok := skillStrategyFor("warm"); ok || isSkillArm("warm") {
		t.Error("ordinary warm arm must not be classified as a skill arm")
	}
	if _, ok := skillStrategyFor("reset"); ok || isSkillArm("reset") {
		t.Error("reset arm is not a skill arm; it must not seat a memory agent or run any skill turn")
	}
	if firstTrialCacheScope(skillStrategyBatch) != "baseline" || firstTrialCacheScope(skillStrategyReplay) != "baseline" || firstTrialCacheScope(skillStrategyOnline) != "online" || firstTrialCacheScope(skillStrategyContinual) != "continual" {
		t.Error("A/B must share baseline cache while C and continual own separate caches")
	}
}

func TestUsesIsolatedTaskRooms(t *testing.T) {
	// Cold uses isolated rooms since the greenfield within-test-accumulation
	// convention was removed: it is now the official Vanilla baseline shape.
	for _, arm := range []string{"warm-skill", "warm-skill-batch", "warm-skill-replay", "warm-skill-online", "warm-skill-continual", "reset", "cold"} {
		if !usesIsolatedTaskRooms(arm) {
			t.Errorf("arm %q must use per-task isolated throwaway rooms", arm)
		}
	}
	for _, arm := range []string{"warm", "warm-ma"} {
		if usesIsolatedTaskRooms(arm) {
			t.Errorf("arm %q must keep family shared-space semantics", arm)
		}
	}
}

func TestStreamPromptsDropSplitClaims(t *testing.T) {
	diag := streamDiagnosisPromptFor("ep-9")
	if strings.Contains(diag, "every train task has already finished") || strings.Contains(diag, "train task") {
		t.Error("stream diagnosis prompt must not claim train is over or call the task a train task")
	}
	if !strings.Contains(diag, "ongoing stream") {
		t.Error("stream diagnosis prompt must describe the stream semantics")
	}
	retr := streamRetrievalPromptHead
	if strings.Contains(retr, "A test task is about to start") {
		t.Error("stream retrieval prompt must not claim the upcoming task is a test task")
	}
	if !strings.Contains(retr, "The next task of the ongoing stream") || !strings.Contains(retr, "@task-agent @memory-agent") || !strings.Contains(retr, "NO_SKILL_APPLICABLE") {
		t.Error("stream retrieval prompt must keep the room conventions")
	}
	if strings.Contains(retr, "upcoming test task") {
		t.Error("stream retrieval prompt must not label the upcoming task section as test")
	}
}

func TestWarmSkillIsolatedRoomSeparatesTrainAndTest(t *testing.T) {
	trainRoom, trainShared, trainPrivate, trainMemory := warmSkillIsolatedRoom("code", 7, "train")
	testRoom, testShared, testPrivate, testMemory := warmSkillIsolatedRoom("code", 7, "test")
	if trainRoom == testRoom || trainShared == testShared || trainPrivate == testPrivate || trainMemory == testMemory {
		t.Fatal("skill strategies must isolate train and test task spaces")
	}
	if trainRoom != "room-code-train-7" || trainShared != "space-code-train-7-shared" {
		t.Errorf("unexpected train isolation ids: %q %q", trainRoom, trainShared)
	}
	if room, shared, private, memory := warmSkillTestRoom("code", 7); room != testRoom || shared != testShared || private != testPrivate || memory != testMemory {
		t.Error("warmSkillTestRoom must remain the test specialization of the generic isolated room")
	}
}

func TestSaveFirstTrialTrajectoryIsImmutablePerTask(t *testing.T) {
	outDir := t.TempDir()
	first := savedTrajectory{Sequence: 1, EpisodeID: "ep-1", TaskID: "task-1", RoomID: "room-1", Transcript: []transcriptEntry{{Role: "user", Content: "first"}}}
	if err := saveFirstTrialTrajectory(outDir, "baseline", "family", first); err != nil {
		t.Fatalf("save first trial: %v", err)
	}
	// A later arm may execute the same task differently, but cannot overwrite
	// the shared A/B first-trial artifact.
	later := first
	later.Transcript[0].Content = "different later run"
	if err := saveFirstTrialTrajectory(outDir, "baseline", "family", later); err != nil {
		t.Fatalf("repeat save: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(outDir, "train-first-trials", "baseline", "family", "ep-1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got savedTrajectory
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Transcript[0].Content != "first" {
		t.Errorf("immutable cache was overwritten: %#v", got)
	}
	if err := saveFirstTrialTrajectory(outDir, "online", "family", later); err != nil {
		t.Fatalf("online cache must be independently writable: %v", err)
	}
}

func TestProposalsFromEpisodeStayTaskLocal(t *testing.T) {
	ledger := []skillProposal{
		{EpisodeID: "task-1", SHA256: "aaaa", Text: "first"},
		{EpisodeID: "task-2", SHA256: "bbbb", Text: "second"},
		{EpisodeID: "task-1", SHA256: "cccc", Text: "third"},
	}
	got := proposalsFromEpisode(ledger, "task-1")
	if len(got) != 2 || got[0].SHA256 != "aaaa" || got[1].SHA256 != "cccc" {
		t.Fatalf("task-local guidance leaked or lost entries: %#v", got)
	}
}

func TestMemoryPaginationToolsUsePi085ParamsSignature(t *testing.T) {
	want := "async execute(_toolCallId, args, _signal, _onUpdate, _ctx)"
	if !strings.Contains(trajectoryToolJS(nil), want) {
		t.Fatal("trajectory_read must receive Pi 0.85 params as its second callback argument")
	}
	if !strings.Contains(skillsToolJS(nil), want) {
		t.Fatal("skills_list must receive Pi 0.85 params as its second callback argument")
	}
}

func TestCompactTrajectoryFallbackUsesLastAgentOutput(t *testing.T) {
	fallback := compactTrajectoryFallback([]transcriptEntry{
		{Role: "user", Content: "task"},
		{Role: "agent", Content: "early answer"},
		{Role: "agent", Content: "final answer"},
	})
	if !strings.Contains(fallback, "final answer") || strings.Contains(fallback, "early answer") {
		t.Errorf("fallback must contain only terminal agent output: %q", fallback)
	}
	if got := compactTrajectoryFallback([]transcriptEntry{{Role: "user", Content: "task"}}); got != "" {
		t.Errorf("prompt-only trajectory must have no fallback, got %q", got)
	}
}

func TestFinalPiSessionOutputRecoversLastAssistantText(t *testing.T) {
	dir := t.TempDir()
	content := `{"type":"message","message":{"role":"assistant","content":[{"type":"thinking","thinking":"private"},{"type":"text","text":"first"}]}}
{"type":"message","message":{"role":"user","content":[{"type":"text","text":"ignore"}]}}
{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"final answer"}]}}
`
	if err := os.WriteFile(filepath.Join(dir, "session.jsonl"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "host-events.jsonl"), []byte(`{"message":{"role":"assistant","content":[{"type":"text","text":"must ignore"}]}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got := finalPiSessionOutput(dir)
	if got == nil || *got != "final answer" {
		t.Fatalf("final session output = %v, want final answer", got)
	}
}
