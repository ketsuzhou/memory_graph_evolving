// Command bench-runner is the benchmark-general form of past-runner: it
// executes manifest-driven episodes (EvoAgentBench domain manifests today,
// PAST-Bench family manifests, later AgentStream/FinEvo-style streams) against
// this Host plus a real graph-memory-service. Rooms are one-shot execution
// contexts in the official per-task session shape: every task episode —
// train or test, every arm — gets its own room, and no room ever hosts two
// tasks, so no conversation history crosses tasks. What crosses tasks is
// the memory layer only. Warm arm: every episode of a group (family/domain)
// in manifest order against one shared/private space pair per group,
// evidence drained after each episode so later episodes recall earlier ones.
// Cold arm: only the manifest's test-split episodes in a
// fresh tenant, so no learning episode ever lands in memory. Each episode is
// one fresh Pi process in its own working directory — per-episode context is
// exactly the recalled evidence plus the episode prompt, and episodes cannot
// leak answers to later episodes through the filesystem.
//
// Unlike past-runner, group/order/role fields are optional: family_id falls
// back to the record's domain, then benchmark; order falls back to manifest
// line order (converters emit train before test, which is the warm-arm
// learning order). The manifest's benchmark and domain fields are carried
// into attempts.jsonl so the memory_bench grading bridges can select the
// right grader. Each attempt additionally records the per-space recall
// citation histogram — the mechanism evidence needed for cross-domain
// contamination analysis in interleaved/streaming protocols.
//
// Episodes may declare stage_files — reference inputs copied from a
// --refs-root directory into the episode workdir before the Pi process
// starts (GDPVal-style deliverable tasks: the agent reads reference/ and
// writes deliverable_files/ inside its own workdir). Staging failures are
// per-episode infrastructure failures; a manifest that declares stage_files
// without --refs-root fails closed at load time.
//
// A third arm, warm-ma, evaluates the multi-agent room form (H4): identical
// to warm, plus a persistent Memory Agent seated in every room whose
// curation turn runs after each episode's evidence drain — it reviews the
// recalled shared state and publishes a distillation via room_send, which
// drains as shared evidence and becomes the next episode's recallable team
// note. Attempts record the curation outcome in memory_agent_* fields.
//
// A fourth arm, warm-skill, evaluates the proposal-skill layer (H3) in the
// official batch shape: while train tasks run, no skill exists anywhere.
// Every train task's trajectory is saved to a reusable per-family
// train-trajectories file; once ALL train tasks of the family have finished,
// ONE consolidation phase runs in a dedicated consolidation room: a
// diagnosis turn per train task (the diagnosis agent — agent-memory, same
// model as the task agent — pages through that task's saved trajectory with
// the chunked trajectory_read tool, diagnoses the problematic steps, and
// publishes 1-3 structured SKILL PROPOSALs via room_send, or NO_SKILL),
// followed by one consolidation-agent turn that reads the accumulated raw
// proposals with skills_list and merges near-duplicates into the final
// ledger — fingerprint dedup, similarity merge, and source provenance, the
// semantics of the graph-memory-service skillevolution merge path without
// its curator gates. Proposals stay proposal-stage by design: no replay
// validation, no curator, no activation — the bench measures raw
// distillation value. The consolidated ledger is the frozen evolution
// state. Every test episode then runs in
// its own fresh room with throwaway spaces — the freeze is write-only: test
// evidence lands in the throwaway spaces, so the train room's memory is never
// modified and test episodes cannot contaminate each other, while recall in a
// test room still spans the train shared space (train memory stays readable
// across rooms, matching official memory-based baselines). Before the task
// starts, the diagnosis agent runs a
// retrieval turn in the test room: it reads the ledger with the skills_list
// tool, selects the proposals that apply to the upcoming task, and publishes
// them as an unaddressed REFERENCE NOTES message — the task agent
// meets the skills through normal recall, never through harness injection.
// Transfer generalization = warm-skill − cold; the skill layer's marginal
// value over plain warm = warm-skill − warm. This is the official two-phase
// shape: evolution state is built on the train side only, frozen, then
// evaluated on held-out tasks.
//
// A fifth arm, warm-skill-continual, is the FinEvo-Bench-aligned continual
// stream: the online strategy's per-task loop — retrieval turn before the
// task, diagnosis turn after it — extended from the train phase to the
// WHOLE manifest. There is no evolve-freeze: no consolidation batch runs
// before the first test episode, no record carries a frozen version, and
// the per-family ledger keeps growing across test episodes, exactly as
// FinEvo's per-task reflect loop intends. One consolidation turn still
// runs per family after the stream ends, purely as a reporting artifact.
// Records emit in stream order, giving the per-position paired curves
// that diff against the reset arm: evolving(pos) − reset(pos).
//
// The reset arm is that paired state-reset control (FinEvo's reset
// baseline, stricter than cold): it runs the FULL manifest — train and
// test — every task in its own throwaway isolated room, with no memory
// agent, no ledger, and no retrieval or diagnosis turns. Drain lands in
// the task's own disposable spaces, so nothing a task writes is ever
// recallable by another task and reset recall citations stay zero,
// making per-position Δ against the continual arms cleanly attributable
// to accumulated experience alone.
//
// Output is one attempts.jsonl whose records are field-compatible with the
// memory_bench grading bridges (benchmark/task_id/episode_id/arm/status/
// final_output), plus per-episode mechanism evidence (recall state, citation
// count and source-space histogram) and the persisted per-episode working
// directories under <out-dir>/work-<arm>/episodes/.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
	"river2.dev/pi-group-chat-host/internal/pi/roombridge"
	"river2.dev/pi-group-chat-host/internal/ports"
	"river2.dev/pi-group-chat-host/internal/runtime"
)

type manifestEpisode struct {
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
	StageFiles []stageFile `json:"stage_files"`
	// UserSimulator marks a dual-control episode: the runner drives an LLM
	// user between agent turns. Its system prompt is runner-side material
	// (official user-sim protocol + persona) and never enters the agent
	// prompt.
	UserSimulator *userSimConfig `json:"user_simulator,omitempty"`
}

type userSimConfig struct {
	SystemPrompt string   `json:"system_prompt"`
	StopMarkers  []string `json:"stop_markers"`
	MaxUserTurns int      `json:"max_user_turns"`
}

type transcriptEntry struct {
	Role    string `json:"role"` // "user" | "agent"
	Content string `json:"content"`
}

// stageFile copies refsRoot/Src into the episode workdir at Dst before the
// episode starts. Dst must stay inside the workdir (relative, no ..).
type stageFile struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

type attemptRecord struct {
	RunID           string            `json:"run_id"`
	EvaluationID    string            `json:"evaluation_id"`
	Benchmark       string            `json:"benchmark"`
	Domain          string            `json:"domain,omitempty"`
	TaskID          string            `json:"task_id"`
	EpisodeID       string            `json:"episode_id"`
	FamilyID        string            `json:"family_id"`
	Role            string            `json:"role,omitempty"`
	Split           string            `json:"split,omitempty"`
	Arm             string            `json:"arm"`
	DeliveryClass   string            `json:"delivery_class,omitempty"`
	RoomID          string            `json:"room_id,omitempty"` // warm-skill test episodes: frozen-state room differs per episode
	MemoryPolicy    string            `json:"memory_policy"`
	Seed            int               `json:"seed"`
	Attempt         int               `json:"attempt"`
	Status          string            `json:"status"`
	FailureKind     string            `json:"failure_kind,omitempty"`
	Error           string            `json:"error,omitempty"`
	DurationSeconds float64           `json:"duration_seconds"`
	WorkDir         string            `json:"work_dir,omitempty"`
	StagedFiles     int               `json:"staged_files,omitempty"`
	PublishReminder bool              `json:"publish_reminder,omitempty"`
	RecallState     string            `json:"recall_state"`
	RecallCitations int               `json:"recall_citations"`
	RecallSpaces    map[string]int    `json:"recall_spaces,omitempty"`
	FinalOutput     *string           `json:"final_output"`
	// The last task-agent publish carrying a fenced code block — what grading
	// reads. final_output keeps its historical last-non-empty-publish
	// semantics for comparability; the two differ exactly when a trailing
	// bare "TASK_COMPLETE" publish buries the code message.
	FinalCodeOutput *string           `json:"final_code_output,omitempty"`
	Transcript      []transcriptEntry `json:"transcript,omitempty"`
	UserTurns       int               `json:"user_turns,omitempty"`
	Termination     string            `json:"termination,omitempty"`         // user_stop | agent_error | max_turns
	TextFallbacks   int               `json:"text_fallback_turns,omitempty"` // turns answered from unpublished plain text (channel artifact)
	// Multi-agent arm (warm-ma): the Memory Agent curation turn that ran
	// after this episode's evidence drain. Its room_send publish is the
	// distilled shared note the next episode recalls.
	MemoryAgentTurn        bool   `json:"memory_agent_turn,omitempty"`
	MemoryAgentStatus      string `json:"memory_agent_status,omitempty"` // published | no_reply
	MemoryAgentReplyBytes  int    `json:"memory_agent_reply_bytes,omitempty"`
	MemoryAgentRecallState string `json:"memory_agent_recall_state,omitempty"`
	MemoryAgentRecallCites int    `json:"memory_agent_recall_citations,omitempty"`
	// Proposal-skill arm (warm-skill): the diagnosis agent turn that ran
	// after this episode's evidence drain, and the proposal block injected
	// into this episode's prompt before it ran.
	SkillProposalStatus string `json:"skill_proposal_status,omitempty"` // published | declined | duplicate | no_reply
	SkillProposalCount  int    `json:"skill_proposal_count,omitempty"`  // new proposals published this turn
	SkillProposalBytes  int    `json:"skill_proposal_bytes,omitempty"`
	SkillProposalSHA256 string `json:"skill_proposal_sha256,omitempty"`
	SkillProposalsTotal int    `json:"skill_proposals_total,omitempty"` // family ledger size after this episode
	// The retrieval turn that ran before a test episode: the diagnosis agent
	// selected ledger skills for the upcoming task and published them to the
	// room (the task agent meets them via recall, never harness injection).
	SkillRetrievalStatus string  `json:"skill_retrieval_status,omitempty"` // published | declined | no_reply
	SkillRetrievalCount  int     `json:"skill_retrieval_count,omitempty"`  // fingerprints the agent quoted
	SkillRetrievalSHA256 string  `json:"skill_retrieval_sha256,omitempty"` // comma-joined quoted fingerprints
	SkillRetrievalText   *string `json:"skill_retrieval_text,omitempty"`   // the verbatim published note
	SkillRetrievalRecall string  `json:"skill_retrieval_recall,omitempty"` // retrieval turn's own recall state
	// Arm B telemetry is deliberately outside the grading bridge. Reporting
	// failures are observable but never change status, output, or local-ledger authority.
	SkillEvolutionInteractionReports  int `json:"skill_evolution_interaction_reports,omitempty"`
	SkillEvolutionInteractionFailures int `json:"skill_evolution_interaction_failures,omitempty"`
	SkillEvolutionDiagnosisReports    int `json:"skill_evolution_diagnosis_reports,omitempty"`
	SkillEvolutionDiagnosisFailures   int `json:"skill_evolution_diagnosis_failures,omitempty"`
	SkillEvolutionAdvisoryCandidates  int `json:"skill_evolution_advisory_candidates,omitempty"`
	SkillEvolutionAdvisoryFailures    int `json:"skill_evolution_advisory_failures,omitempty"`
	// C3 provenance fields are non-grading audit telemetry. GMS advisory is
	// primary; local ledger material is recorded only as comparison context.
	SkillAdvisoryReadStatus            string          `json:"skill_advisory_read_status,omitempty"`
	SkillRetrievalSource               string          `json:"skill_retrieval_source,omitempty"`
	SkillRetrievalAdvisoryCount        int             `json:"skill_retrieval_advisory_count,omitempty"`
	SkillRetrievalLocalComparisonCount int             `json:"skill_retrieval_local_comparison_count,omitempty"`
	SkillExposure                      []skillExposure `json:"skill_exposure,omitempty"`
	// FrozenSkillVersion identifies the immutable consolidated skill ledger used by every held-out episode.
	FrozenSkillVersion  string `json:"frozen_skill_version,omitempty"`
	ConsolidationRoomID string `json:"consolidation_room_id,omitempty"`
	// GraphSnapshot is the family's frozen GMS version vector recorded at
	// consolidation: every train-scope cut plus, per space, the frozen
	// projection head version. Held-out episodes pin their recall reads to it.
	GraphSnapshot *graphSnapshot `json:"graph_snapshot,omitempty"`
	// TestWritePolicy states precisely where a held-out episode's evidence
	// goes: its own throwaway spaces — never the frozen train graph. The
	// episode's recall may still READ the frozen graph through the pinned
	// snapshot versions above.
	TestWritePolicy string `json:"test_write_policy,omitempty"`
	// Task-local replay is exclusive to warm-skill-replay and reads only the
	// first trial's proposals for this same task.
	SkillReplayAttempted bool    `json:"skill_replay_attempted,omitempty"`
	SkillReplayStatus    string  `json:"skill_replay_status,omitempty"`
	SkillReplayRecall    string  `json:"skill_replay_recall,omitempty"`
	SkillReplayCitations int     `json:"skill_replay_citations,omitempty"`
	SkillReplayOutput    *string `json:"skill_replay_output,omitempty"`
	// Directed skill-offer telemetry for warm-skill-graph-batch production
	// held-out. Legacy arms leave these empty. LCB grading ignores them.
	Offers            []skillOfferAttempt  `json:"offers,omitempty"`
	Served            []skillServedAttempt `json:"served,omitempty"`
	Disposition       string               `json:"disposition,omitempty"`
	DispositionReason string               `json:"reason,omitempty"`
	NamedInReasoning  bool                 `json:"named_in_reasoning,omitempty"`
}

type skillStrategy string

const (
	skillStrategyBatch      skillStrategy = "batch" // legacy/broken-delivery
	skillStrategyGraphBatch skillStrategy = "graph_batch"
	legacyBrokenDelivery    = "legacy/broken-delivery"
	skillStrategyReplay     skillStrategy = "task_replay"
	skillStrategyOnline     skillStrategy = "online"
	skillStrategyContinual  skillStrategy = "continual"
)

// skillStrategyFor maps runner arms to their experiment protocol. warm-skill
// remains the backward-compatible spelling of the official-style batch arm.
func skillStrategyFor(arm string) (skillStrategy, bool) {
	switch arm {
	case "warm-skill", "warm-skill-batch":
		// Retained to reproduce batch-6. Delivery is legacy/broken-delivery.
		return skillStrategyBatch, true
	case graphBatchStrategyID:
		return skillStrategyGraphBatch, true
	case "warm-skill-replay":
		return skillStrategyReplay, true
	case "warm-skill-online":
		return skillStrategyOnline, true
	case "warm-skill-continual":
		return skillStrategyContinual, true
	default:
		return "", false
	}
}

func isSkillArm(arm string) bool { _, ok := skillStrategyFor(arm); return ok }

func skillDeliveryClass(arm string) string {
	strategy, ok := skillStrategyFor(arm)
	if !ok {
		return ""
	}
	if strategy == skillStrategyBatch {
		return legacyBrokenDelivery
	}
	if strategy == skillStrategyGraphBatch {
		return graphBatchAuthorityGMS
	}
	return string(strategy)
}

// usesIsolatedTaskRooms reports whether the arm gives every task its own
// throwaway GMS spaces: the skill strategies, whose only cross-task channel
// is the runner ledger, the reset baseline, which has no cross-task
// channel at all, and the cold arm, which mirrors the official
// EvoAgentBench Vanilla baseline — test tasks only, no train phase, no
// memory accumulation between tasks, so recall always comes back empty.
func usesIsolatedTaskRooms(arm string) bool {
	return isSkillArm(arm) || arm == "reset" || arm == "cold"
}

// firstTrialCacheScope keeps A/B in one baseline cache (their first trials
// see no skills); online and continual own separate caches because their
// later first trials intentionally observe their own stream's skills.
func firstTrialCacheScope(strategy skillStrategy) string {
	switch strategy {
	case skillStrategyOnline:
		return "online"
	case skillStrategyContinual:
		return "continual"
	default:
		return "baseline"
	}
}

func main() {
	manifestPath := flag.String("manifest", "", "episode manifest JSONL (required; past_official/evo_official converter output)")
	gmsBinary := flag.String("gms-binary", "", "graph-memory-server binary; the runner spawns one single-tenant instance per arm (required for multi-arm runs)")
	gmsURL := flag.String("gms-url", "", "pre-existing graph-memory-service base URL (single-arm runs only)")
	gmsToken := flag.String("gms-token", "", "token for the pre-existing graph-memory-service")
	piBinary := flag.String("pi-binary", "", "Pi RPC binary path, version 0.85.1 (required)")
	provider := flag.String("provider", "", "Pi provider name (required)")
	model := flag.String("model", "", "Pi model id (required)")
	arms := flag.String("arms", "warm,cold", "comma-separated arms to run")
	families := flag.String("families", "", "comma-separated family/domain filter (default: every group in the manifest)")
	evaluationID := flag.String("evaluation-id", "", "evaluation id used in run ids and tenant ids (required)")
	seed := flag.Int("seed", 0, "schedule seed recorded in attempts")
	outDir := flag.String("out-dir", "", "output directory for attempts.jsonl and per-episode workdirs (required)")
	episodeTimeout := flag.Duration("episode-timeout", 30*time.Minute, "per-episode timeout")
	batchParallelism := flag.Int("batch-parallelism", 4, "maximum concurrent train, diagnosis, or test jobs for warm-skill-batch and cold")
	piEnv := flag.String("pi-env", "HOME,PATH", "comma-separated environment allowlist passed to the Pi process")
	refsRoot := flag.String("refs-root", "", "base directory for episode stage_files sources (required when the manifest declares stage_files)")
	publishReminder := flag.String("publish-reminder", "", "channel-adaptation text appended to every episode prompt asking the agent to publish its final answer through the room tools (empty = off); visible in the room like any user message")
	extensionFile := flag.String("extension-file", "", "use this Pi extension JS instead of the built-in room bridge (e.g. a domain tool plane); required for tool-plane benchmarks")
	userSimURL := flag.String("user-sim-url", "", "OpenAI-compatible chat endpoint for user-simulator turns (required when the manifest declares user_simulator)")
	userSimModel := flag.String("user-sim-model", "", "model id for the user simulator")
	sidecarURL := flag.String("sidecar-url", "", "tool-plane sidecar base URL; after each dual-control episode the runner POSTs /close so the lifecycle log records the final DB (empty = off)")
	flag.Parse()
	if *manifestPath == "" || *piBinary == "" || *provider == "" || *model == "" || *evaluationID == "" || *outDir == "" {
		flag.Usage()
		os.Exit(2)
	}
	armList := splitList(*arms)
	if *batchParallelism < 1 {
		fatal("--batch-parallelism must be at least 1")
	}
	if *gmsBinary == "" {
		if *gmsURL == "" || *gmsToken == "" || len(armList) > 1 {
			fmt.Fprintln(os.Stderr, "bench-runner: multi-arm runs require --gms-binary (graph-memory-service is single-tenant); --gms-url/--gms-token serve single-arm runs against a pre-existing instance")
			os.Exit(2)
		}
	}

	episodes, err := loadManifest(*manifestPath)
	if err != nil {
		fatal("load manifest: %v", err)
	}
	if needsRefsRoot(episodes) && *refsRoot == "" {
		fatal("manifest declares stage_files but --refs-root is empty; pass the reference-file base directory")
	}
	refsRootAbs := ""
	if *refsRoot != "" {
		if refsRootAbs, err = filepath.Abs(*refsRoot); err != nil {
			fatal("resolve refs root: %v", err)
		}
		if info, err := os.Stat(refsRootAbs); err != nil || !info.IsDir() {
			fatal("--refs-root %q is not a directory", *refsRoot)
		}
	}
	if filter := splitList(*families); len(filter) > 0 {
		allowed := map[string]bool{}
		for _, family := range filter {
			allowed[family] = true
		}
		var kept []manifestEpisode
		for _, episode := range episodes {
			if allowed[episode.FamilyID] {
				kept = append(kept, episode)
			}
		}
		if len(kept) == 0 {
			fatal("family filter matched no manifest episodes")
		}
		episodes = kept
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal("create out dir: %v", err)
	}
	// The Pi process resolves --session-dir and --extension against its own
	// working directory (the per-episode dir), so every path handed downstream
	// must be absolute even when the caller passed a relative out dir.
	absOutDir, err := filepath.Abs(*outDir)
	if err != nil {
		fatal("resolve out dir: %v", err)
	}
	*outDir = absOutDir
	attemptsPath := filepath.Join(*outDir, "attempts.jsonl")
	attemptsFile, err := os.Create(attemptsPath)
	if err != nil {
		fatal("create attempts file: %v", err)
	}
	defer attemptsFile.Close()
	writer := bufio.NewWriter(attemptsFile)
	defer writer.Flush()
	var emitMu sync.Mutex

	// emit is called from parallel train, diagnosis, and test workers; the
	// mutex keeps the attempts writer and the console line interleaving sane.
	emit := func(record attemptRecord) {
		emitMu.Lock()
		defer emitMu.Unlock()
		line, _ := json.Marshal(record)
		fmt.Fprintln(writer, string(line))
		writer.Flush()
		status := record.Status
		if record.Error != "" {
			status += " (" + record.Error + ")"
		}
		output := "output=nil"
		if record.FinalOutput != nil {
			output = "output=yes"
		}
		fmt.Printf("[%s] %s %s/%s recall=%s(%d) %s %s\n", record.Arm, record.Benchmark, record.FamilyID, record.EpisodeID, record.RecallState, record.RecallCitations, output, status)
	}
	for _, arm := range armList {
		if arm != "warm" && arm != "cold" && arm != "warm-ma" && arm != "reset" && !isSkillArm(arm) {
			fatal("unknown arm %q: only warm, cold, warm-ma, reset, warm-skill, warm-skill-batch, warm-skill-graph-batch, warm-skill-replay, warm-skill-online, and warm-skill-continual exist", arm)
		}
		policy := "read_write"
		if arm == "cold" {
			policy = "no_learn_memory_present"
		}
		if arm == "reset" {
			policy = "state_reset_per_task"
		}
		config := armConfig{
			arm: arm, episodes: episodes, evaluationID: *evaluationID, seed: *seed,
			gmsURL: *gmsURL, gmsToken: *gmsToken, piBinary: *piBinary,
			provider: *provider, model: *model, policy: policy,
			episodeTimeout: *episodeTimeout, batchParallelism: *batchParallelism, piEnv: splitList(*piEnv),
			outDir: *outDir, publishReminder: *publishReminder,
			refsRoot:      refsRootAbs,
			extensionFile: *extensionFile, userSimURL: *userSimURL, userSimModel: *userSimModel,
			sidecarURL: *sidecarURL,
		}
		if *gmsBinary != "" {
			instance, err := startGMS(*gmsBinary, filepath.Join(*outDir, "gms-"+arm), armCutBindings(episodes, arm))
			if err != nil {
				fatal("arm %s: %v", arm, err)
			}
			config.gmsURL, config.gmsToken = instance.url, instance.token
			fmt.Printf("[%s] graph-memory-service on %s (snapshot %s)\n", arm, instance.url, instance.statePath)
			runErr := runArm(context.Background(), config, emit)
			instance.stop()
			if runErr != nil {
				fatal("arm %s: %v", arm, runErr)
			}
			continue
		}
		if bindings := armCutBindings(episodes, arm); len(bindings) > 0 {
			// Remote GMS is pre-existing: the runner cannot pass server flags,
			// so the deployment must carry the identical room→space bindings
			// or every consolidation-cut freeze fails closed.
			fmt.Printf("[%s] remote graph-memory-service must be started with these freeze bindings:\n", arm)
			for _, binding := range bindings {
				fmt.Printf("  -cut-room-space %s\n", binding)
			}
		}
		if err := runArm(context.Background(), config, emit); err != nil {
			fatal("arm %s: %v", arm, err)
		}
	}
	fmt.Printf("attempts written to %s\n", attemptsPath)
}

type armConfig struct {
	arm              string
	episodes         []manifestEpisode
	evaluationID     string
	seed             int
	gmsURL           string
	gmsToken         string
	piBinary         string
	provider         string
	model            string
	policy           string
	episodeTimeout   time.Duration
	batchParallelism int
	piEnv            []string
	outDir           string
	publishReminder  string
	refsRoot         string
	extensionFile    string
	userSimURL       string
	userSimModel     string
	sidecarURL       string
	armBReporter     *armBReporter
	// graphBatch is the TB-14 composition runtime. It is required when the
	// arm has episodes: the new strategy must not fall through to the
	// legacy runner-local []skillProposal pipeline.
	graphBatch *graphBatchRuntime
}

// gmsInstance is one arm-private graph-memory-service process. The service is
// single-tenant by design, so arm isolation means one instance per arm with
// its own snapshot file, which also preserves the full post-run memory state
// for inspection.
type gmsInstance struct {
	url       string
	token     string
	statePath string
	process   *os.Process
	logFile   *os.File
}

func startGMS(binary, stateDir string, cutBindings []string) (*gmsInstance, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create gms state dir: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("reserve gms port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return nil, fmt.Errorf("release reserved port: %w", err)
	}
	token := "bench-eval-" + sanitizeID(filepath.Base(stateDir)) + "-token"
	statePath := filepath.Join(stateDir, "state.json")
	logFile, err := os.Create(filepath.Join(stateDir, "gms.log"))
	if err != nil {
		return nil, fmt.Errorf("create gms log: %w", err)
	}
	conformanceDir, err := resolveGMSConformanceDir(binary)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("graph-memory-service conformance dir: %w", err)
	}
	args := []string{binary,
		"-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"-token", token,
		"-state", statePath,
		"-conformance-dir", conformanceDir,
	}
	for _, binding := range cutBindings {
		// Room→space deployment binding for room-scoped consolidation cuts:
		// without it the freezer fails closed and no freeze can be admitted.
		args = append(args, "-cut-room-space", binding)
	}
	process, err := os.StartProcess(binary, args, &os.ProcAttr{
		Env:   withGMSConformanceEnv(os.Environ(), conformanceDir),
		Files: []*os.File{os.Stdin, logFile, logFile},
	})
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start graph-memory-service: %w", err)
	}
	instance := &gmsInstance{
		url:       fmt.Sprintf("http://127.0.0.1:%d", port),
		token:     token,
		statePath: statePath,
		process:   process,
		logFile:   logFile,
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		probe, err := http.Get(instance.url + "/healthz")
		if err == nil {
			probe.Body.Close()
			return instance, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	instance.stop()
	return nil, fmt.Errorf("graph-memory-service did not become reachable on %s (see %s)", instance.url, logFile.Name())
}

func (instance *gmsInstance) stop() {
	if instance.process != nil {
		_ = instance.process.Kill()
		_, _ = instance.process.Wait()
		instance.process = nil
	}
	if instance.logFile != nil {
		instance.logFile.Close()
		instance.logFile = nil
	}
}

func runArm(ctx context.Context, config armConfig, emit func(attemptRecord)) error {
	strategy, skillArm := skillStrategyFor(config.arm)
	if skillArm && strategy == skillStrategyGraphBatch {
		return runGraphBatchArm(ctx, config, emit)
	}
	tenantID := sanitizeID(config.evaluationID) + "-" + config.arm + "-t"
	client := memoryclient.NewClient(config.gmsURL, config.gmsToken, &http.Client{Timeout: 10 * time.Second}, 1<<20)
	var summary *c3ArmSummary
	originalEmit := emit
	emit = func(record attemptRecord) {
		if summary != nil {
			summary.observe(record)
		}
		originalEmit(record)
	}
	if isSkillArm(config.arm) {
		// Arm B uses the existing GMS endpoint and is fail-open by construction:
		// telemetry outages must not change task execution or grading.
		config.armBReporter = newArmBReporter(client, func(format string, args ...any) { fmt.Printf(format, args...) })
		summary = &c3ArmSummary{SchemaVersion: "pi-group-chat-host.c3-arm-summary.v1", Arm: config.arm, ExposureSources: map[string]int{}}
		defer writeC3ArmSummary(config, config.armBReporter, summary)
	}

	if _, err := client.InitializeTenant(ctx, ports.InitializeTenantRequest{
		TenantID: tenantID, DisplayName: "Bench evaluation tenant (" + config.arm + ")", BootstrapPrincipalID: "host-service",
	}); err != nil {
		return fmt.Errorf("InitializeTenant: %w", err)
	}
	// The bootstrap principal comes into existence with the tenant; registering
	// it again with different content is a correct 409. Only the task agent is
	// a separate registration.
	if _, err := client.RegisterPrincipal(ctx, ports.RegisterPrincipalRequest{
		PrincipalID: "agent-primary", Kind: "agent", DisplayName: "Bench task agent",
	}); err != nil {
		return fmt.Errorf("RegisterPrincipal agent-primary: %w", err)
	}
	// The multi-agent arms (warm-ma, warm-skill) additionally seat a
	// persistent Memory Agent in every room: warm-ma uses it for curation
	// turns, warm-skill for diagnosis turns (see runDiagnosisTurn).
	seatMemoryAgent := config.arm == "warm-ma" || isSkillArm(config.arm)
	if seatMemoryAgent {
		if _, err := client.RegisterPrincipal(ctx, ports.RegisterPrincipalRequest{
			PrincipalID: "agent-memory", Kind: "agent", DisplayName: "Persistent memory agent",
		}); err != nil {
			return fmt.Errorf("RegisterPrincipal agent-memory: %w", err)
		}
	}

	familyOrder := familySequence(config.episodes)
	var grants []string
	// Warm-skill freeze semantics: every test episode gets its own fresh room
	// with throwaway spaces, so test evidence never touches the train spaces
	// and no test episode can contaminate another; recall in those rooms spans
	// the train shared space besides the throwaway ones, keeping train memory
	// readable across rooms. The
	// spaces are registered up front here; their ids derive from the same
	// global sequence the episode loop assigns below (see warmSkillTestRoom).
	testSequence := 0
	for _, family := range familyOrder {
		shared := "space-" + sanitizeID(family) + "-shared"
		private := "space-" + sanitizeID(family) + "-private"
		owner := "agent-primary"
		spaceRequests := []ports.RegisterSpaceRequest{
			{SpaceID: shared, Scope: "shared", DisplayName: family + " shared memory"},
			{SpaceID: private, Scope: "private", OwnerPrincipalID: &owner, DisplayName: family + " agent private memory"},
		}
		if seatMemoryAgent {
			memoryOwner := "agent-memory"
			spaceRequests = append(spaceRequests, ports.RegisterSpaceRequest{
				SpaceID: MemoryAgentPrivateSpaceID(family), Scope: "private", OwnerPrincipalID: &memoryOwner,
				DisplayName: family + " memory agent private memory",
			})
		}
		for _, request := range spaceRequests {
			if _, err := client.RegisterSpace(ctx, request); err != nil {
				return fmt.Errorf("RegisterSpace %s: %w", request.SpaceID, err)
			}
		}
		grants = append(grants, shared, private)
		if seatMemoryAgent {
			grants = append(grants, MemoryAgentPrivateSpaceID(family))
		}
		if usesIsolatedTaskRooms(config.arm) {
			owner := "agent-primary"
			for _, episode := range episodesOfFamily(config.episodes, family) {
				if config.arm == "cold" && episode.Split != "test" {
					continue
				}
				testSequence++
				_, taskShared, taskPrivate, taskMemoryPrivate := warmSkillIsolatedRoom(family, testSequence, episode.Split)
				memoryOwner := "agent-memory"
				requests := []ports.RegisterSpaceRequest{
					{SpaceID: taskShared, Scope: "shared", DisplayName: family + " " + episode.Split + " task shared memory"},
					{SpaceID: taskPrivate, Scope: "private", OwnerPrincipalID: &owner, DisplayName: family + " " + episode.Split + " task private memory"},
				}
				if skillArm {
					// Only skill arms seat agent-memory; the reset baseline
					// registers no memory-agent space at all.
					requests = append(requests, ports.RegisterSpaceRequest{
						SpaceID: taskMemoryPrivate, Scope: "private", OwnerPrincipalID: &memoryOwner,
						DisplayName: family + " " + episode.Split + " task memory-agent private memory",
					})
				}
				for _, request := range requests {
					if _, err := client.RegisterSpace(ctx, request); err != nil {
						return fmt.Errorf("RegisterSpace %s: %w", request.SpaceID, err)
					}
				}
				grants = append(grants, taskShared, taskPrivate)
				if skillArm {
					grants = append(grants, taskMemoryPrivate)
				}
				if skillArm {
					// Retrieval turns run in throwaway rooms so their prompts
					// never become recallable evidence next to the task; see
					// warmSkillRetrievalRoom.
					_, retrShared, retrPrivate, retrMemoryPrivate := warmSkillRetrievalRoom(family, episode.EpisodeID)
					memoryOwner := "agent-memory"
					for _, request := range []ports.RegisterSpaceRequest{
						{SpaceID: retrShared, Scope: "shared", DisplayName: family + " retrieval scratch shared memory"},
						{SpaceID: retrPrivate, Scope: "private", OwnerPrincipalID: &owner, DisplayName: family + " retrieval scratch private memory"},
						{SpaceID: retrMemoryPrivate, Scope: "private", OwnerPrincipalID: &memoryOwner, DisplayName: family + " retrieval scratch memory-agent private memory"},
					} {
						if _, err := client.RegisterSpace(ctx, request); err != nil {
							return fmt.Errorf("RegisterSpace %s: %w", request.SpaceID, err)
						}
					}
					grants = append(grants, retrShared, retrPrivate, retrMemoryPrivate)
				}
				if strategy == skillStrategyBatch && episode.Split != "test" {
					_, diagnosisShared, diagnosisPrivate, diagnosisMemoryPrivate := warmSkillDiagnosisRoom(family, testSequence)
					for _, request := range []ports.RegisterSpaceRequest{
						{SpaceID: diagnosisShared, Scope: "shared", DisplayName: family + " diagnosis shared memory"},
						{SpaceID: diagnosisPrivate, Scope: "private", OwnerPrincipalID: &owner, DisplayName: family + " diagnosis private memory"},
						{SpaceID: diagnosisMemoryPrivate, Scope: "private", OwnerPrincipalID: &memoryOwner, DisplayName: family + " diagnosis memory-agent private memory"},
					} {
						if _, err := client.RegisterSpace(ctx, request); err != nil {
							return fmt.Errorf("RegisterSpace %s: %w", request.SpaceID, err)
						}
					}
					grants = append(grants, diagnosisShared, diagnosisPrivate, diagnosisMemoryPrivate)
				}
			}
			if skillArm {
				// The consolidation room hosts the post-train batch: per-task
				// diagnosis turns plus the merge turn. Its spaces are separate
				// from the train shared space, so proposals never pollute the
				// task evidence later test episodes recall.
				_, consShared, consPrivate, consMemoryPrivate := warmSkillConsolidationRoom(family)
				consMemoryOwner := "agent-memory"
				for _, request := range []ports.RegisterSpaceRequest{
					{SpaceID: consShared, Scope: "shared", DisplayName: family + " consolidation room shared memory"},
					{SpaceID: consPrivate, Scope: "private", OwnerPrincipalID: &owner, DisplayName: family + " consolidation room private memory"},
					{SpaceID: consMemoryPrivate, Scope: "private", OwnerPrincipalID: &consMemoryOwner, DisplayName: family + " consolidation room memory-agent private memory"},
				} {
					if _, err := client.RegisterSpace(ctx, request); err != nil {
						return fmt.Errorf("RegisterSpace %s: %w", request.SpaceID, err)
					}
				}
				grants = append(grants, consShared, consPrivate, consMemoryPrivate)
			}
		}
	}
	for _, grant := range []ports.RegisterGrantRequest{
		{
			GrantID: "grant-" + config.arm + "-lifecycle", PrincipalID: "host-service",
			SpaceIDs: grants, Purpose: "lifecycle", Operations: []string{"evidence.stage", "evidence.commit", "recall"},
			ExpiresAt: "2027-12-31T00:00:00Z",
		},
		{
			GrantID: "grant-" + config.arm + "-tool-plane", PrincipalID: "host-service",
			SpaceIDs: grants, Purpose: "tool_plane", Operations: []string{"recall", "exploration.start", "exploration.explore", "exploration.redirect", "exploration.submit"},
			ExpiresAt: "2027-12-31T00:00:00Z",
		},
	} {
		if _, err := client.RegisterGrant(ctx, grant); err != nil {
			return fmt.Errorf("RegisterGrant %s: %w", grant.GrantID, err)
		}
	}

	// Workdirs persist under the out dir: each episode gets its own fresh
	// directory so episodes cannot pass answers through the filesystem, and
	// the produced files stay available for grading and inspection.
	workDir := filepath.Join(config.outDir, "work-"+config.arm)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return fmt.Errorf("work dir: %w", err)
	}
	extensionPath, err := roombridge.WriteTo(filepath.Join(workDir, "extension"))
	if err != nil {
		return fmt.Errorf("write room bridge extension: %w", err)
	}
	if config.extensionFile != "" {
		// Tool-plane benchmarks ship a combined extension (room bridge +
		// domain tools); the runner only points Pi at it.
		extensionPath = config.extensionFile
	}
	// Cold rides the same parallel coordinator as warm-skill-batch with an
	// empty train phase: it is the official Vanilla baseline shape, so its
	// held-out tasks run concurrently in isolated rooms (see
	// runParallelSkillStrategyBatch).
	if (skillArm && strategy == skillStrategyBatch) || config.arm == "cold" {
		return runParallelSkillStrategyBatch(ctx, config, tenantID, workDir, extensionPath, familyOrder, emit)
	}
	sequence := 0
	// Proposal-skill arm (warm-skill): per-family ledger of the skill
	// proposals distilled so far. Before each test episode the diagnosis
	// agent reads the ledger through the skills_list tool and publishes the
	// applicable proposals to the room itself (see runRetrievalTurn); the
	// ledger lives only in the runner's memory, while the published proposals
	// also reach GMS as ordinary shared evidence through the drain.
	skillLedger := map[string][]skillProposal{}
	// One stable content-derived ledger version is stamped on every held-out record.
	frozenSkillVersions := map[string]string{}
	// The frozen GMS version vector per family, recorded when the family's
	// consolidation phase triggers its train-scope cuts. Held-out episodes
	// pin their recall reads to it.
	frozenGraphSnapshots := map[string]*graphSnapshot{}

	for _, family := range familyOrder {
		// The family's episodes consume the global sequence counter from this
		// value; the train-scope cut derivation must see the same base.
		sequenceAtFamilyStart := sequence
		// Warm-skill batch state (official two-phase shape): train
		// trajectories are collected while train tasks run — no skill exists
		// during training — and their records stay buffered until the
		// post-train consolidation phase stamps the diagnosis outcome, so
		// attempts.jsonl keeps one complete record per train task.
		var trainTrajectories []savedTrajectory
		var bufferedTrainRecords []*attemptRecord
		consolidationDone := false
		runConsolidation := func() {
			if consolidationDone {
				return
			}
			consolidationDone = true
			version, snapshot, err := runConsolidationPhase(ctx, runtime.NewSession(), consolidationPhaseInput{
				config:        config,
				diagnose:      strategy == skillStrategyBatch,
				tenantID:      tenantID,
				family:        family,
				extensionPath: extensionPath,
				trajectories:  trainTrajectories,
				records:       bufferedTrainRecords,
				ledger:        &skillLedger,
				episodes:      config.episodes,
				strategy:      strategy,
				sequenceBase:  sequenceAtFamilyStart,
			})
			if err != nil {
				fmt.Printf("[%s] consolidation %s warning: %v\n", config.arm, family, err)
			}
			frozenSkillVersions[family] = version
			if snapshot != nil {
				frozenGraphSnapshots[family] = snapshot
			}
			for _, buffered := range bufferedTrainRecords {
				emit(*buffered)
			}
		}
		for _, episode := range episodesOfFamily(config.episodes, family) {
			if config.arm == "cold" && episode.Split != "test" {
				continue
			}
			sequence++
			session := runtime.NewSession()
			// Official per-task session shape: every episode — train or
			// test, every arm — runs in its own one-shot room; rooms never
			// carry state across tasks. Warm arms still accumulate: evidence
			// drains into the family shared space, which is what later
			// episodes recall, not room history. Warm-skill freeze semantics
			// additionally give test episodes throwaway spaces (registered
			// and granted up front), so test writes never reach the train
			// spaces and test episodes cannot contaminate each other. The
			// freeze is write-only: recall still spans the train shared
			// space, so train memory stays retrievable across rooms.
			episodeRoomID := episodeRoom(family, sequence, episode.Split)
			episodeShared := "space-" + sanitizeID(family) + "-shared"
			episodePrivate := "space-" + sanitizeID(family) + "-private"
			episodeMemoryPrivate := MemoryAgentPrivateSpaceID(family)
			var recallSpaceIDs []string
			var recallSpaceVersions map[string]int64
			if usesIsolatedTaskRooms(config.arm) {
				_, episodeShared, episodePrivate, episodeMemoryPrivate = warmSkillIsolatedRoom(family, sequence, episode.Split)
				// A/B never read another train task's evidence. C receives cross-task
				// skills only through its explicit retrieval turn into this room.
				// The reset baseline never reads anything but its own empty
				// throwaway spaces — its recall citations must stay zero.
				recallSpaceIDs = []string{episodeShared, episodePrivate}
			}
			episodeDir := filepath.Join(workDir, "episodes", fmt.Sprintf("%04d-%s", sequence, sanitizeID(episode.EpisodeID)))
			if err := os.MkdirAll(episodeDir, 0o755); err != nil {
				return fmt.Errorf("episode work dir: %w", err)
			}
			record := attemptRecord{
				RunID:           fmt.Sprintf("%s:%s:%d:%s", config.evaluationID, episode.TaskID, config.seed, config.arm),
				EvaluationID:    config.evaluationID,
				Benchmark:       episode.Benchmark,
				Domain:          episode.Domain,
				TaskID:          episode.TaskID,
				EpisodeID:       episode.EpisodeID,
				FamilyID:        episode.FamilyID,
				Role:            episode.Role,
				Split:           episode.Split,
				Arm:             config.arm,
				DeliveryClass:   skillDeliveryClass(config.arm),
				RoomID:          episodeRoomID,
				MemoryPolicy:    config.policy,
				Seed:            config.seed,
				Attempt:         1,
				WorkDir:         episodeDir,
				PublishReminder: config.publishReminder != "",
			}
			startTiming := time.Now()
			// Reference inputs land in the workdir before the episode starts;
			// the agent only ever sees workdir-relative paths, and a missing
			// source is a per-episode infrastructure failure, not a task
			// failure.
			staged, stageErr := stageReferenceFiles(config.refsRoot, episodeDir, episode.StageFiles)
			if stageErr != nil {
				record.Status = "failed"
				record.FailureKind = "infrastructure"
				record.Error = truncate(stageErr.Error(), 4000)
				record.DurationSeconds = time.Since(startTiming).Seconds()
				record.StagedFiles = staged
				emit(record)
				continue
			}
			record.StagedFiles = staged
			if skillArm && strategy == skillStrategyOnline && episode.Split != "test" && len(skillLedger[family]) > 0 {
				if retErr := runRetrievalTurn(ctx, session, retrievalInput{config: config, tenantID: tenantID, roomID: episodeRoomID, sharedSpaceID: episodeShared, privateSpaceID: episodeMemoryPrivate, family: family, sequence: sequence, episodeID: episode.EpisodeID, episodeDir: episodeDir, extensionPath: extensionPath, taskPrompt: episode.Turns[0].Prompt, ledger: &skillLedger, record: &record}); retErr != nil {
					record.Status, record.FailureKind = "failed", "infrastructure"
					record.Error = truncate("online skill retrieval turn: "+retErr.Error(), 4000)
					emit(record)
					continue
				}
			}
			// Continual (FinEvo-style stream): one retrieval turn before EVERY
			// episode — train or test — whenever the growing ledger holds
			// anything. No freeze point exists, so the gates below skip the
			// consolidation batch and the frozen-version stamp for this arm.
			if skillArm && strategy == skillStrategyContinual && len(skillLedger[family]) > 0 {
				if retErr := runRetrievalTurn(ctx, session, retrievalInput{config: config, tenantID: tenantID, roomID: episodeRoomID, sharedSpaceID: episodeShared, privateSpaceID: episodeMemoryPrivate, family: family, sequence: sequence, episodeID: episode.EpisodeID, episodeDir: episodeDir, extensionPath: extensionPath, taskPrompt: episode.Turns[0].Prompt, ledger: &skillLedger, record: &record, stream: true}); retErr != nil {
					record.Status, record.FailureKind = "failed", "infrastructure"
					record.Error = truncate("continual skill retrieval turn: "+retErr.Error(), 4000)
					emit(record)
					continue
				}
			}
			// Retrieval turn (warm-skill, test episodes only): before the task
			// starts, the diagnosis agent reads the family skill ledger and
			// publishes the applicable proposals to the room. Runs outside the
			// episode timeout on its own budget; its duration stays out of the
			// episode's duration metric. The first test episode is also the
			// freeze point: the consolidation batch (per-task diagnosis plus
			// the merge turn) runs exactly once, before any test task.
			if skillArm && episode.Split == "test" && strategy != skillStrategyContinual {
				runConsolidation()
			}
			if skillArm && episode.Split == "test" && strategy != skillStrategyContinual {
				record.FrozenSkillVersion = frozenSkillVersions[family]
				record.ConsolidationRoomID, _, _, _ = warmSkillConsolidationRoom(family)
				// Held-out episodes read the frozen train graph through the
				// consolidation cuts' pinned projection versions, and never
				// write it: their evidence drains into this room's throwaway
				// spaces only.
				record.TestWritePolicy = "disposable_spaces_only"
				if snapshot := frozenGraphSnapshots[family]; snapshot != nil {
					record.GraphSnapshot = snapshot
					recallSpaceIDs = append(append([]string(nil), recallSpaceIDs...), snapshot.sortedSpaces()...)
					recallSpaceVersions = snapshot.Spaces
				}
			}
			if skillArm && episode.Split == "test" && strategy != skillStrategyContinual && len(skillLedger[family]) > 0 {
				if retErr := runRetrievalTurn(ctx, session, retrievalInput{
					config:         config,
					tenantID:       tenantID,
					roomID:         episodeRoomID,
					sharedSpaceID:  episodeShared,
					privateSpaceID: episodeMemoryPrivate,
					family:         family,
					sequence:       sequence,
					episodeID:      episode.EpisodeID,
					episodeDir:     episodeDir,
					extensionPath:  extensionPath,
					taskPrompt:     episode.Turns[0].Prompt,
					ledger:         &skillLedger,
					record:         &record,
				}); retErr != nil {
					record.Status = "failed"
					record.FailureKind = "infrastructure"
					record.Error = truncate("skill retrieval turn: "+retErr.Error(), 4000)
					emit(record)
					continue
				}
			}
			started := time.Now()
			episodeCtx, cancel := context.WithTimeout(ctx, config.episodeTimeout)
			turnRequestBase := func(turnIndex int) runtime.TurnRequest {
				return runtime.TurnRequest{
					Authority: runtime.ExecutionAuthority{
						TenantID: tenantID, RoomID: episodeRoomID, AgentID: "agent-primary", ProfileKind: "ordinary",
						WorkingDirectory: episodeDir, EnvironmentAllowlist: config.piEnv,
						Provider: config.provider, Model: config.model,
						SharedSpaceID: episodeShared, PrivateSpaceID: episodePrivate,
						RecallSpaceIDs: recallSpaceIDs, RecallSpaceVersions: recallSpaceVersions,
					},
					MemoryBaseURL:   config.gmsURL,
					MemoryAuthToken: config.gmsToken,
					PiBinary:        config.piBinary,
					PiExtensionPath: extensionPath,
					PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-t%d", sanitizeID(config.evaluationID), config.arm, sequence, sanitizeID(episode.EpisodeID), turnIndex),
					EventLogPath:    filepath.Join(episodeDir, "host-events.jsonl"),
					PiStderrPath:    filepath.Join(episodeDir, "pi-stderr.log"),
				}
			}
			var turn runtime.TurnResult
			var transcript []transcriptEntry
			var termination string
			var textFallbacks int
			if episode.UserSimulator != nil {
				if config.userSimURL == "" || config.userSimModel == "" {
					cancel()
					record.Status = "failed"
					record.FailureKind = "infrastructure"
					record.Error = "episode declares user_simulator; pass --user-sim-url and --user-sim-model"
					record.DurationSeconds = time.Since(started).Seconds()
					emit(record)
					continue
				}
				turn, transcript, termination, textFallbacks, err = runDualControlEpisode(episodeCtx, session, dualControlInput{
					episode:    episode,
					config:     config,
					base:       turnRequestBase,
					episodeDir: episodeDir,
					sequence:   sequence,
				})
			} else {
				request := turnRequestBase(0)
				request.RoomInput = episode.Turns[0].Prompt
				if config.publishReminder != "" {
					request.RoomInput += "\n\n" + config.publishReminder
				}
				request.HumanMessageID = fmt.Sprintf("human-%04d-%s", sequence, sanitizeID(episode.EpisodeID))
				turn, err = session.Turn(episodeCtx, request)
				if err == nil {
					transcript = roomTranscript(turn.Messages)
				}
			}
			if err != nil {
				cancel()
				record.Status = "failed"
				record.FailureKind = "task"
				record.Error = truncate(err.Error(), 4000)
				record.DurationSeconds = time.Since(started).Seconds()
				emit(record)
				continue
			}
			record.Status = "success"
			record.DurationSeconds = time.Since(started).Seconds()
			record.RecallState = turn.Recall.State
			record.RecallCitations = len(turn.Recall.Citations)
			record.Transcript = transcript
			record.Termination = termination
			record.TextFallbacks = textFallbacks
			for _, entry := range transcript {
				if entry.Role == "user" {
					record.UserTurns++
				}
			}
			if len(turn.Recall.Items) > 0 {
				spaces := map[string]int{}
				for _, item := range turn.Recall.Items {
					spaces[item.SourceSpaceID]++
				}
				record.RecallSpaces = spaces
			}
			capture := captureTaskOutput(episodeDir, turn.Messages)
			record.FinalCodeOutput = capture.graded
			if capture.final != nil {
				record.FinalOutput = capture.final
				if capture.fromSession {
					// A model may finish with a valid final answer without room_send.
					// Preserve it for grading and diagnosis rather than treating the
					// trajectory as prompt-only.
					transcript = append(transcript, transcriptEntry{Role: "agent", Content: *capture.final})
					record.Transcript = transcript
				}
			}
			// The recall-visibility boundary: evidence of this episode becomes
			// recallable for the next episode only after the drain commits.
			drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
			_, drainErr := session.DrainEvidence(drainCtx, runtime.DrainRequest{
				RoomID: episodeRoomID, MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken,
				EventLogPath: filepath.Join(episodeDir, "host-events.jsonl"),
			})
			drainCancel()
			if drainErr != nil {
				cancel()
				record.Status = "failed"
				record.FailureKind = "infrastructure"
				record.Error = truncate("drain evidence: "+drainErr.Error(), 4000)
				emit(record)
				continue
			}
			if config.arm == "warm-ma" {
				if maErr := runMemoryAgentTurn(ctx, session, memoryAgentInput{
					config:        config,
					tenantID:      tenantID,
					roomID:        episodeRoomID,
					family:        family,
					sequence:      sequence,
					episodeID:     episode.EpisodeID,
					episodeDir:    episodeDir,
					extensionPath: extensionPath,
					record:        &record,
				}); maErr != nil {
					cancel()
					record.Status = "failed"
					record.FailureKind = "infrastructure"
					record.Error = truncate("memory agent turn: "+maErr.Error(), 4000)
					emit(record)
					continue
				}
			}
			// Official batch shape: no skill work happens during training.
			// A finished train task contributes its saved trajectory to the
			// family's consolidation batch; its record waits buffered until
			// the consolidation phase stamps the diagnosis outcome. Failed
			// train episodes already emitted above carry no diagnosis fields.
			// The continual strategy runs this block for every episode, so
			// test trajectories join the per-arm stream log too, and its
			// records fall through to the shared emit tail in stream order
			// instead of buffering.
			if skillArm && (episode.Split != "test" || strategy == skillStrategyContinual) {
				if len(transcript) > 0 {
					saved := savedTrajectory{
						Sequence: sequence, EpisodeID: episode.EpisodeID, TaskID: episode.TaskID,
						RoomID: episodeRoomID, Transcript: transcript,
					}
					if episode.Split != "test" {
						trainTrajectories = append(trainTrajectories, saved)
					}
					if saveErr := appendSavedTrajectory(config.outDir, config.arm, family, saved); saveErr != nil {
						cancel()
						return fmt.Errorf("save train trajectory %s: %w", saved.EpisodeID, saveErr)
					}
					if episode.Split != "test" {
						if cacheErr := saveFirstTrialTrajectory(config.outDir, firstTrialCacheScope(strategy), family, saved); cacheErr != nil {
							cancel()
							return fmt.Errorf("save first train trial %s: %w", saved.EpisodeID, cacheErr)
						}
					}
				}
				if strategy != skillStrategyBatch && len(transcript) > 0 {
					consRoom, consShared, _, consMemoryPrivate := warmSkillConsolidationRoom(family)
					if diagErr := runDiagnosisTurn(ctx, runtime.NewSession(), diagnosisInput{config: config, tenantID: tenantID, roomID: consRoom, sharedSpaceID: consShared, memoryPrivateSpace: consMemoryPrivate, family: family, sequence: sequence, episodeID: episode.EpisodeID, extensionPath: extensionPath, transcript: transcript, ledger: &skillLedger, record: &record, stream: strategy == skillStrategyContinual}); diagErr != nil {
						record.SkillProposalStatus = "error"
					}
					if strategy == skillStrategyReplay {
						runTaskLocalReplay(ctx, session, taskLocalReplayInput{config: config, tenantID: tenantID, roomID: episodeRoomID, sharedSpaceID: episodeShared, privateSpaceID: episodePrivate, memoryPrivateSpace: episodeMemoryPrivate, family: family, sequence: sequence, episodeID: episode.EpisodeID, episodeDir: episodeDir, extensionPath: extensionPath, taskPrompt: episode.Turns[0].Prompt, ledger: &skillLedger, record: &record})
					}
				}
				if episode.Split != "test" && strategy != skillStrategyContinual {
					bufferedTrainRecords = append(bufferedTrainRecords, &record)
					cancel()
					continue
				}
				// Continual episodes — train included — fall through to the
				// shared emit tail so attempts.jsonl lands in stream order.
			}
			if episode.UserSimulator != nil && config.sidecarURL != "" {
				// Best-effort lifecycle closure: the sidecar snapshots the
				// final DB into its log and frees the episode environment.
				closeSidecarEnv(ctx, config.sidecarURL, episode.EpisodeID, episodeDir)
			}
			cancel()
			emit(record)
		}
		// Train-only families still get their consolidation batch and their
		// buffered records emitted.
		if isSkillArm(config.arm) {
			runConsolidation()
		}
	}
	return nil
}

// MemoryAgentPrivateSpaceID returns the Memory Agent's private space id for
// one family. The space is registered up front like every other space the
// arm projects evidence into.
func MemoryAgentPrivateSpaceID(family string) string {
	return "space-ma-" + sanitizeID(family) + "-private"
}

// episodeRoom derives the one-shot room for one task episode. Rooms are
// single-task execution contexts in the official per-task session shape —
// no room ever hosts two tasks, so conversation history never crosses
// tasks; the global sequence number keeps ids unique within a run.
func episodeRoom(family string, sequence int, split string) string {
	return "room-" + sanitizeID(family) + "-" + split + "-" + strconv.Itoa(sequence)
}

// warmSkillTestRoom derives the throwaway spaces for one warm-skill test
// episode, including the retrieval turn's memory-
// agent private space (the retrieval note's private projection must not land
// in any train space). The room id comes from episodeRoom; the id embeds the
// episode's global
// sequence number: the registration phase and the episode loop compute it
// identically, so spaces are always pre-registered and pre-granted.
func warmSkillIsolatedRoom(family string, sequence int, split string) (roomID, shared, private, memoryPrivate string) {
	id := sanitizeID(family) + "-" + split + "-" + strconv.Itoa(sequence)
	return episodeRoom(family, sequence, split), "space-" + id + "-shared", "space-" + id + "-private", "space-" + id + "-memory-private"
}

func warmSkillTestRoom(family string, sequence int) (roomID, shared, private, memoryPrivate string) {
	return warmSkillIsolatedRoom(family, sequence, "test")
}

// warmSkillConsolidationRoom derives the room and spaces hosting the
// post-train consolidation batch: one diagnosis turn per train task plus the
// merge turn. Its spaces stay disjoint from the family's train spaces so
// the skill layer never lands in the task evidence test episodes recall.
func warmSkillConsolidationRoom(family string) (roomID, shared, private, memoryPrivate string) {
	id := sanitizeID(family) + "-consolidation"
	return "room-" + id, "space-" + id + "-shared", "space-" + id + "-private", "space-" + id + "-memory-private"
}

// memoryAgentPrompt is the curation brief for warm-ma Memory Agent turns.
// The agent never sees the task; it works from the recalled shared state
// plus this brief, and its room_send publish becomes the next episode's
// recallable shared memory.
const memoryAgentPrompt = "Room memory curation turn. A task episode in this room just finished and its evidence is committed to the room's shared memory; the context snapshot above recalls what the team has distilled so far. Publish exactly one concise team note via room_send: which approach worked in recent episodes, what failed and why, and which pattern, requirement, or caveat is worth carrying into the next task of this scenario. Write it as durable shared memory for your teammates, not as a reply to any person. Do not attempt the tasks yourself."

type memoryAgentInput struct {
	config        armConfig
	tenantID      string
	roomID        string
	family        string
	sequence      int
	episodeID     string
	episodeDir    string
	extensionPath string
	record        *attemptRecord
}

// runMemoryAgentTurn executes one Memory Agent curation turn after an
// episode's evidence drain: the memory-profile agent (room tools only, no
// builtin tools) receives the room's recalled shared state and publishes a
// distillation through room_send; the follow-up drain makes that note
// recallable for the next episode. This is the H4 multi-agent arm's
// protocol deviation from the ordinary single-agent arms: the Memory
// Protocol exploration tools (memory_start/…) are not exposed to real Pi
// processes by the room bridge, so curation rides the recall snapshot plus
// the publish channel.
func runMemoryAgentTurn(ctx context.Context, session *runtime.Session, input memoryAgentInput) error {
	config := input.config
	maDir := filepath.Join(config.outDir, "work-"+config.arm, "memory-agent",
		fmt.Sprintf("%04d-%s", input.sequence, sanitizeID(input.episodeID)))
	if err := os.MkdirAll(maDir, 0o755); err != nil {
		return fmt.Errorf("memory agent work dir: %w", err)
	}
	maCtx, maCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer maCancel()
	turn, err := session.Turn(maCtx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{
			TenantID: input.tenantID, RoomID: input.roomID, AgentID: "agent-memory", ProfileKind: "memory",
			WorkingDirectory: maDir, EnvironmentAllowlist: config.piEnv,
			Provider: config.provider, Model: config.model,
			SharedSpaceID:  "space-" + sanitizeID(input.family) + "-shared",
			PrivateSpaceID: MemoryAgentPrivateSpaceID(input.family),
		},
		RoomInput:       memoryAgentPrompt,
		HumanMessageID:  fmt.Sprintf("human-%04d-%s-ma", input.sequence, sanitizeID(input.episodeID)),
		MemoryBaseURL:   config.gmsURL,
		MemoryAuthToken: config.gmsToken,
		PiBinary:        config.piBinary,
		PiExtensionPath: input.extensionPath,
		PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-ma", sanitizeID(config.evaluationID), config.arm, input.sequence, sanitizeID(input.episodeID)),
		EventLogPath:    filepath.Join(input.episodeDir, "host-events.jsonl"),
		PiStderrPath:    filepath.Join(maDir, "pi-stderr.log"),
	})
	if err != nil {
		return err
	}
	input.record.MemoryAgentTurn = true
	input.record.MemoryAgentRecallState = turn.Recall.State
	input.record.MemoryAgentRecallCites = len(turn.Recall.Citations)
	if reply := finalAgentOutput(turn.Messages, "agent-memory"); reply != nil {
		input.record.MemoryAgentStatus = "published"
		input.record.MemoryAgentReplyBytes = len(*reply)
	} else {
		input.record.MemoryAgentStatus = "no_reply"
	}
	fmt.Printf("[%s] memory agent turn %04d recall=%s(%d) %s\n",
		config.arm, input.sequence, turn.Recall.State, len(turn.Recall.Citations), input.record.MemoryAgentStatus)
	// The curation note must be recall-visible before the next episode
	// starts, so the Memory Agent's own evidence drains immediately.
	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	_, drainErr := session.DrainEvidence(drainCtx, runtime.DrainRequest{
		RoomID: input.roomID, MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken,
		EventLogPath: filepath.Join(input.episodeDir, "host-events.jsonl"),
	})
	drainCancel()
	return drainErr
}

const (
	// Trajectory chunks cap how many characters one trajectory_read response
	// carries; ledger chunks pack a fixed number of proposals each. Both keep
	// every single tool response comfortably inside one model context.
	maxTrajectoryChunkChars = 2400
	ledgerEntriesPerChunk   = 12
	skillTextMaxChars       = 1200
	// The ledger's first chunks are a compact index — one line per skill with
	// its name, trigger, sha256 prefix, and the detail chunk holding the full
	// entry — so a retrieval agent can shortlist from the index and page only
	// into plausible detail chunks. Index pages pack to their own char cap.
	ledgerIndexChunkChars = 4000
	ledgerNameMaxChars    = 40
	ledgerTriggerMaxChars = 90
	// skills_list hard call budget: past it the tool answers with a
	// publish-now nudge instead of another page, so a model stuck paging
	// cannot stall the retrieval turn until its deadline. 18 covers fully
	// paging a 14-chunk ledger plus a few re-reads before the nudge fires.
	skillsListCallBudget = 18
	// Governance cap on a family's consolidated ledger: past it the tail is
	// dropped after consolidation, keeping test-time retrieval affordable.
	maxConsolidatedSkills = 60
	// Extension tools the memory-profile turns register; the same names must
	// ride on the turn authority or pi's --tools filter drops them.
	trajectoryReadToolName = "trajectory_read"
	skillsListToolName     = "skills_list"
)

// diagnosisPrompt is the brief for the warm-skill diagnosis turn. The
// diagnosis agent (agent-memory, same model as the task agent) pages through
// the finished episode's trajectory with trajectory_read, diagnoses the
// problematic steps, and publishes 1-3 structured proposals (or NO_SKILL).
// Proposals are proposal-stage by design — no replay validation, no curator
// gate, per the H3 protocol.
// diagnosisPromptFor briefs the diagnosis turn inside the consolidation
// room: train is over, this turn reviews one saved train trajectory.
func diagnosisPromptFor(episodeID string) string {
	return "Diagnosis turn. You are this family's diagnosis agent in a dedicated diagnosis room; every train task has already finished. Your job now is to diagnose the trajectory of train task " + episodeID + ". The full trajectory (the task, the agent's every step, the published result) is available through the trajectory_read tool: call it with chunk 0 first — every response header states total_chunks — and read as many chunks as you need.\n\nDiagnose the problematic steps: where the attempt stalled, chose a wrong approach, wasted effort, or nearly failed. Then publish via room_send one message containing between 1 and 3 skill proposals, one per diagnosed problem whose lesson generalizes, each in exactly this shape:\n\nSKILL PROPOSAL\nname: <short imperative name>\ntrigger: <situations where it applies>\nsteps: <concrete numbered steps>\npitfalls: <what goes wrong when skipped>\n\nEach proposal must be grounded in a specific step you diagnosed. If the trajectory exposes nothing reusable, publish exactly: NO_SKILL"
}

// streamDiagnosisPromptFor is the continual arm's variant: the stream
// never freezes, so the task under review may be a test task and the
// distilled proposal feeds the very next episode of the stream.
func streamDiagnosisPromptFor(episodeID string) string {
	return "Diagnosis turn. You are this family's diagnosis agent in its consolidation room; one task of the ongoing stream just finished and the next task is about to start. Your job now is to diagnose the trajectory of task " + episodeID + ". The full trajectory (the task, the agent's every step, the published result) is available through the trajectory_read tool: call it with chunk 0 first — every response header states total_chunks — and read as many chunks as you need.\n\nDiagnose the problematic steps: where the attempt stalled, chose a wrong approach, wasted effort, or nearly failed. Then publish via room_send one message containing between 1 and 3 skill proposals, one per diagnosed problem whose lesson generalizes, each in exactly this shape:\n\nSKILL PROPOSAL\nname: <short imperative name>\ntrigger: <situations where it applies>\nsteps: <concrete numbered steps>\npitfalls: <what goes wrong when skipped>\n\nEach proposal must be grounded in a specific step you diagnosed. If the trajectory exposes nothing reusable, publish exactly: NO_SKILL"
}

// consolidationPrompt briefs the consolidation-agent turn: merge and dedupe
// the raw proposals the diagnosis turns accumulated, GMS-skillevolution
// semantics (fingerprint dedup, similarity merge, source provenance) minus
// the curator gates.
const consolidationPrompt = "Skill consolidation turn. You are this family's consolidation agent. The diagnosis agent has distilled raw skill proposals from every train task; they are available through the skills_list tool: chunk 0 is a compact index — one line per raw proposal with its entry number, name, trigger, sha256 prefix, and the chunk holding its full text. Start from the index, then read all detail chunks (every response header states total_chunks) before deciding.\n\nMerge and deduplicate aggressively: combine near-duplicates into one stronger skill (union the concrete steps and pitfalls, keep the sharpest name and trigger), drop strictly weaker variants, and keep every skill that stands alone unchanged. Prefer the smallest set that covers every raw proposal — when two skills could serve the same trigger, merge them into one. The consolidated set must stay under 60 skills; if merging alone cannot get there, fold the most overlapping skills until it does. Then publish via room_send one message with the final consolidated set, one block per skill in exactly this shape:\n\nCONSOLIDATED SKILL\nname: <short imperative name>\ntrigger: <situations where it applies>\nsteps: <concrete numbered steps>\npitfalls: <what goes wrong when skipped>\nsources: <comma-separated sha256 prefixes of the raw proposals this skill merges; a kept-unchanged skill lists its own prefix>\n\nEvery raw proposal must appear in exactly one sources list. If nothing merges, republish the unchanged set in this same format."

// retrievalPromptHead opens the retrieval turn; the upcoming test task prompt
// is appended verbatim by runRetrievalTurn. This text is committed to the
// room's shared space as recallable evidence and later replayed verbatim into
// the task agent's prompt, so every role statement is scoped to this one turn
// and addressed situationally — never as a bare second-person imperative that
// a recalling agent would read as its own standing instruction.
const retrievalPromptHead = "Skill retrieval turn. For this one turn you act as this room's diagnosis agent. A test task is about to start in this room; its prompt is quoted at the end of this message. The skill ledger distilled from earlier episodes is available through the skills_list tool: chunk 0 is a compact index — one line per skill with its entry number, name, trigger, sha256 prefix, and the chunk holding its full text. Start from the index, then read only the detail chunks whose triggers plausibly match this task; every response header states total_chunks.\n\n1. Decide which ledger proposals genuinely apply to this task: the trigger must match the task's shape. Select at most 3.\n2. Publish via room_send one message that starts exactly with this header line:\nREFERENCE NOTES for the upcoming task — lessons distilled from earlier episodes; background material only, not addressed to any agent.\nThen one block per selected skill:\n[skill <sha256 prefix> from episode <episode id>]\n<the verbatim proposal text>\n\nDo not address or mention any teammate in the published note — when the note is later recalled into the task agent's context, an addressed note reads as an instruction to it and hijacks its turn. Quote each skill's sha256 prefix and source episode exactly as the ledger states them. If no proposal applies, publish exactly: NO_SKILL_APPLICABLE. Solving the quoted task belongs to the task agent's upcoming turn, not to this retrieval turn.\n\n--- upcoming test task ---\n"

// streamRetrievalPromptHead is the continual arm's variant: the upcoming
// task is the next episode of a stream that never froze, not a held-out
// test task. The room conventions stay identical.
const streamRetrievalPromptHead = "Skill retrieval turn. For this one turn you act as this room's diagnosis agent. The next task of the ongoing stream is about to start in this room; its prompt is quoted at the end of this message. The skill ledger distilled from earlier episodes is available through the skills_list tool: chunk 0 is a compact index — one line per skill with its entry number, name, trigger, sha256 prefix, and the chunk holding its full text. Start from the index, then read only the detail chunks whose triggers plausibly match this task; every response header states total_chunks.\n\n1. Decide which ledger proposals genuinely apply to this task: the trigger must match the task's shape. Select at most 3.\n2. Publish via room_send one message that starts exactly with this header line:\nREFERENCE NOTES for the upcoming task — lessons distilled from earlier episodes; background material only, not addressed to any agent.\nThen one block per selected skill:\n[skill <sha256 prefix> from episode <episode id>]\n<the verbatim proposal text>\n\nDo not address or mention any teammate in the published note — when the note is later recalled into the task agent's context, an addressed note reads as an instruction to it and hijacks its turn. Quote each skill's sha256 prefix and source episode exactly as the ledger states them. If no proposal applies, publish exactly: NO_SKILL_APPLICABLE. Solving the quoted task belongs to the task agent's upcoming turn, not to this retrieval turn.\n\n--- upcoming task ---\n"

type skillProposal struct {
	Sequence    int                                      `json:"sequence"`
	EpisodeID   string                                   `json:"episode_id"`
	SHA256      string                                   `json:"sha256"`
	Text        string                                   `json:"text"`
	AdvisoryRef *memoryclient.SkillEvolutionCandidateRef `json:"advisory_ref,omitempty"`
}

// savedTrajectory is one train task's finished trajectory, persisted to the
// out-dir so the consolidation batch (and any later analysis) can replay it.
type savedTrajectory struct {
	Sequence   int               `json:"sequence"`
	EpisodeID  string            `json:"episode_id"`
	TaskID     string            `json:"task_id"`
	RoomID     string            `json:"room_id"`
	Transcript []transcriptEntry `json:"transcript"`
}

// appendSavedTrajectory appends one train trajectory to the family's reusable train-trajectories file.
func appendSavedTrajectory(outDir, arm, family string, saved savedTrajectory) error {
	dir := filepath.Join(outDir, "work-"+arm, "train-trajectories")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	line, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	handle, err := os.OpenFile(filepath.Join(dir, sanitizeID(family)+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer handle.Close()
	_, err = handle.Write(append(line, '\n'))
	return err
}

// saveFirstTrialTrajectory writes one immutable first-trial artifact per task.
// Batch and task-replay share the baseline scope; online is separate because
// its later first trials intentionally observe prior online skills.
func saveFirstTrialTrajectory(outDir, scope, family string, saved savedTrajectory) error {
	dir := filepath.Join(outDir, "train-first-trials", scope, sanitizeID(family))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, sanitizeID(saved.EpisodeID)+".json")
	encoded, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		return err
	}
	if existing, err := os.ReadFile(path); err == nil {
		var prior savedTrajectory
		if err := json.Unmarshal(existing, &prior); err != nil {
			return fmt.Errorf("decode existing first trial: %w", err)
		}
		if prior.EpisodeID != saved.EpisodeID || prior.TaskID != saved.TaskID {
			return fmt.Errorf("first-trial cache identity conflict at %s", path)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o644)
}

type diagnosisInput struct {
	config                                                      armConfig
	tenantID, roomID, sharedSpaceID, memoryPrivateSpace, family string
	sequence                                                    int
	episodeID, extensionPath                                    string
	transcript                                                  []transcriptEntry
	ledger                                                      *map[string][]skillProposal
	record                                                      *attemptRecord
	// stream selects the continual-arm prompt variant: the stream never
	// freezes, so the brief must not claim training is already over.
	stream bool
}

type consolidationPhaseInput struct {
	config               armConfig
	diagnose, forceMerge bool
	tenantID, family     string
	extensionPath        string
	trajectories         []savedTrajectory
	records              []*attemptRecord
	ledger               *map[string][]skillProposal
	// episodes/strategy/sequenceBase reproduce the arm's registration
	// derivation for the family's train-scope rooms, so the graph-snapshot
	// cuts freeze exactly the rooms the arm drained evidence into.
	episodes     []manifestEpisode
	strategy     skillStrategy
	sequenceBase int
}

type consolidationSummary struct {
	Family                 string              `json:"family"`
	ConsolidationRoomID    string              `json:"consolidation_room_id"`
	TrainRoomIDs           []string            `json:"train_room_ids"`
	RawSkillCount          int                 `json:"raw_skill_count"`
	ConsolidatedSkillCount int                 `json:"consolidated_skill_count"`
	Status                 string              `json:"status"`
	FrozenSkillVersion     string              `json:"frozen_skill_version"`
	ConsolidatedSourceMap  map[string][]string `json:"consolidated_source_map,omitempty"`
	// GraphSnapshot records the GMS room-scoped consolidation cuts and their
	// frozen per-space projection head versions — real ProjectionVersion
	// data, never the ledger content hash.
	GraphSnapshot *graphSnapshot `json:"graph_snapshot,omitempty"`
}

type retrievalInput struct {
	config                                                  armConfig
	tenantID, roomID, sharedSpaceID, privateSpaceID, family string
	sequence                                                int
	episodeID, episodeDir, extensionPath, taskPrompt        string
	ledger                                                  *map[string][]skillProposal
	record                                                  *attemptRecord
	// stream selects the continual-arm prompt variant: the upcoming task is
	// the next episode of an ongoing stream, not a held-out test task.
	stream bool
}

// runDiagnosisTurn runs after train completion in its assigned diagnosis room. A failed
// diagnosis never changes the already-completed task outcome: it is recorded as skill status=error.
func runDiagnosisTurn(ctx context.Context, session *runtime.Session, input diagnosisInput) error {
	config := input.config
	diagDir := filepath.Join(config.outDir, "work-"+config.arm, "diagnosis", fmt.Sprintf("%04d-%s", input.sequence, sanitizeID(input.episodeID)))
	if err := os.MkdirAll(diagDir, 0o755); err != nil {
		return fmt.Errorf("diagnosis work dir: %w", err)
	}
	chunks := buildTrajectoryChunks(input.transcript)
	chunksJSON, err := json.Marshal(chunks)
	if err != nil {
		return fmt.Errorf("marshal trajectory chunks: %w", err)
	}
	if err := os.WriteFile(filepath.Join(diagDir, "trajectory-chunks.json"), chunksJSON, 0o644); err != nil {
		return fmt.Errorf("record trajectory chunks: %w", err)
	}
	extensionPath := filepath.Join(diagDir, "diagnosis-extension.mjs")
	if err := writeMemoryTurnExtension(input.extensionPath, extensionPath, trajectoryToolJS(chunks)); err != nil {
		return err
	}
	diagCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	prompt := diagnosisPromptFor(input.episodeID)
	if input.stream {
		prompt = streamDiagnosisPromptFor(input.episodeID)
	}
	turn, err := session.Turn(diagCtx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{TenantID: input.tenantID, RoomID: input.roomID, AgentID: "agent-memory", ProfileKind: "memory", WorkingDirectory: diagDir, EnvironmentAllowlist: config.piEnv, Provider: config.provider, Model: config.model, SharedSpaceID: input.sharedSpaceID, PrivateSpaceID: input.memoryPrivateSpace, ExtraMemoryTools: []string{trajectoryReadToolName}},
		RoomInput: prompt + compactTrajectoryFallback(input.transcript), HumanMessageID: fmt.Sprintf("human-%04d-%s-diag", input.sequence, sanitizeID(input.episodeID)), MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken, PiBinary: config.piBinary, PiExtensionPath: extensionPath,
		PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-diag", sanitizeID(config.evaluationID), config.arm, input.sequence, sanitizeID(input.episodeID)), EventLogPath: filepath.Join(diagDir, "host-events.jsonl"), PiStderrPath: filepath.Join(diagDir, "pi-stderr.log"),
	})
	if err != nil {
		input.record.SkillProposalStatus = "error"
		input.record.SkillProposalsTotal = len((*input.ledger)[input.family])
		return nil
	}
	var accepted []skillProposal
	if reply := finalAgentOutput(turn.Messages, "agent-memory"); reply == nil {
		input.record.SkillProposalStatus = "no_reply"
	} else {
		proposals := splitSkillProposals(*reply)
		var added []skillProposal
		acceptedFingerprints := map[string]bool{}
		var fingerprints []string
		for _, text := range proposals {
			fingerprint := skillFingerprint(text)
			fingerprints = append(fingerprints, fingerprint)
			if !acceptedFingerprints[fingerprint] && !skillLedgerHas(*input.ledger, input.family, fingerprint) {
				acceptedFingerprints[fingerprint] = true
				added = append(added, skillProposal{Sequence: input.sequence, EpisodeID: input.episodeID, SHA256: fingerprint, Text: text})
			}
		}
		input.record.SkillProposalSHA256 = strings.Join(fingerprints, ",")
		switch {
		case len(proposals) == 0:
			input.record.SkillProposalStatus = "declined"
		case len(added) == 0:
			input.record.SkillProposalStatus = "duplicate"
		default:
			input.record.SkillProposalStatus, input.record.SkillProposalCount = "published", len(added)
			accepted = append(accepted, added...)
			skillsDir := filepath.Join(config.outDir, "work-"+config.arm, "skills")
			if err := os.MkdirAll(skillsDir, 0o755); err != nil {
				return fmt.Errorf("skill proposal dir: %w", err)
			}
			for _, proposal := range added {
				input.record.SkillProposalBytes += len(proposal.Text)
				(*input.ledger)[input.family] = append((*input.ledger)[input.family], proposal)
				if err := os.WriteFile(filepath.Join(skillsDir, fmt.Sprintf("%04d-%s.md", input.sequence, sanitizeID(input.episodeID))), []byte(proposal.Text+"\n"), 0o644); err != nil {
					return fmt.Errorf("record skill proposal: %w", err)
				}
			}
		}
	}
	input.record.SkillProposalsTotal = len((*input.ledger)[input.family])
	fmt.Printf("[%s] diagnosis %04d recall=%s(%d) %s total=%d\n", config.arm, input.sequence, turn.Recall.State, len(turn.Recall.Citations), input.record.SkillProposalStatus, input.record.SkillProposalsTotal)
	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	_, drainErr := session.DrainEvidence(drainCtx, runtime.DrainRequest{RoomID: input.roomID, MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken, EventLogPath: filepath.Join(diagDir, "host-events.jsonl")})
	drainCancel()
	if drainErr != nil {
		input.record.SkillProposalStatus = "error"
	}
	for _, proposal := range accepted {
		if config.armBReporter != nil {
			config.armBReporter.reportDiagnosis(ctx, input.record, proposal, armBEvaluationBatchID(input.record))
		}
	}
	return nil
}

// runConsolidationPhase diagnoses every saved train trajectory once, merges the raw
// ledger once, then freezes the family's train-scope GMS rooms into one
// GraphSnapshot version vector. Its summary records all exact train room IDs,
// the frozen output version, and the real cut-backed snapshot.
func runConsolidationPhase(ctx context.Context, session *runtime.Session, input consolidationPhaseInput) (string, *graphSnapshot, error) {
	roomID, sharedSpaceID, _, memoryPrivateSpace := warmSkillConsolidationRoom(input.family)
	records := make(map[string]*attemptRecord, len(input.records))
	for _, record := range input.records {
		records[record.EpisodeID] = record
	}
	trainRoomIDs, seen := make([]string, 0, len(input.trajectories)), map[string]bool{}
	for _, saved := range input.trajectories {
		if saved.RoomID != "" && !seen[saved.RoomID] {
			trainRoomIDs, seen[saved.RoomID] = append(trainRoomIDs, saved.RoomID), true
		}
		if record := records[saved.EpisodeID]; record != nil {
			record.ConsolidationRoomID = roomID
			if input.diagnose {
				if err := runDiagnosisTurn(ctx, session, diagnosisInput{config: input.config, tenantID: input.tenantID, roomID: roomID, sharedSpaceID: sharedSpaceID, memoryPrivateSpace: memoryPrivateSpace, family: input.family, sequence: saved.Sequence, episodeID: saved.EpisodeID, extensionPath: input.extensionPath, transcript: saved.Transcript, ledger: input.ledger, record: record}); err != nil {
					record.SkillProposalStatus = "error"
				}
			}
		}
	}
	for _, record := range input.records {
		if !hasTrajectory(input.trajectories, record.EpisodeID) {
			record.SkillProposalStatus, record.SkillProposalsTotal = "no_trajectory", len((*input.ledger)[input.family])
		}
	}
	rawSkillCount := len((*input.ledger)[input.family])
	status, sourceMap := runConsolidationTurn(ctx, session, input, roomID, sharedSpaceID, memoryPrivateSpace)
	version := frozenSkillVersion((*input.ledger)[input.family])
	// Freeze the family's train-scope GMS state: one force cut per room the
	// train phase drained into, merged into one version vector. A freeze
	// failure downgrades to a warning + snapshot-less summary — the ledger is
	// already frozen and the run's benchmark value must not be lost — but the
	// returned error surfaces it in the runner log.
	var snapshot *graphSnapshot
	var snapshotErr error
	if armFreezesGraph(input.config.arm) {
		next := input.sequenceBase
		rooms := trainScopeRoomsForFamily(input.episodes, input.family, &next, input.strategy)
		freeze, err := freezeGraphSnapshot(ctx, snapshotCutClient(input.config.gmsURL, input.config.gmsToken), input.tenantID, snapshotKeyPrefix(input.config, input.family), rooms)
		if err != nil {
			snapshotErr = fmt.Errorf("graph snapshot %s: %w", input.family, err)
		} else {
			snapshot = &freeze
		}
	}
	for _, record := range input.records {
		record.FrozenSkillVersion, record.ConsolidationRoomID = version, roomID
	}
	summary := consolidationSummary{Family: input.family, ConsolidationRoomID: roomID, TrainRoomIDs: trainRoomIDs, RawSkillCount: rawSkillCount, ConsolidatedSkillCount: len((*input.ledger)[input.family]), Status: status, FrozenSkillVersion: version, ConsolidatedSourceMap: sourceMap, GraphSnapshot: snapshot}
	dir := filepath.Join(input.config.outDir, "work-"+input.config.arm, "consolidation")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return version, snapshot, err
	}
	body, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return version, snapshot, err
	}
	if err := os.WriteFile(filepath.Join(dir, sanitizeID(input.family)+"-summary.json"), append(body, '\n'), 0o644); err != nil {
		return version, snapshot, err
	}
	return version, snapshot, snapshotErr
}

// snapshotKeyPrefix is the deterministic idempotency-key prefix for one
// family's consolidation cuts: a retried consolidation replays the original
// frozen jobs instead of re-freezing at a moved head.
func snapshotKeyPrefix(config armConfig, family string) string {
	return fmt.Sprintf("%s-%s-%s", sanitizeID(config.evaluationID), config.arm, sanitizeID(family))
}

func hasTrajectory(trajectories []savedTrajectory, episodeID string) bool {
	for _, trajectory := range trajectories {
		if trajectory.EpisodeID == episodeID {
			return true
		}
	}
	return false
}

func runConsolidationTurn(ctx context.Context, session *runtime.Session, input consolidationPhaseInput, roomID, sharedSpaceID, memoryPrivateSpace string) (string, map[string][]string) {
	raw := append([]skillProposal(nil), (*input.ledger)[input.family]...)
	sourceMap := make(map[string][]string)
	if len(raw) == 0 && !input.forceMerge {
		return "kept_raw", sourceMap
	}
	consDir := filepath.Join(input.config.outDir, "work-"+input.config.arm, "consolidation", sanitizeID(input.family))
	if err := os.MkdirAll(consDir, 0o755); err != nil {
		return "kept_raw", sourceMap
	}
	extensionPath := filepath.Join(consDir, "consolidation-extension.mjs")
	if err := writeMemoryTurnExtension(input.extensionPath, extensionPath, skillsToolJS(raw)); err != nil {
		return "kept_raw", sourceMap
	}
	turnCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	turn, err := session.Turn(turnCtx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{TenantID: input.tenantID, RoomID: roomID, AgentID: "agent-memory", ProfileKind: "memory", WorkingDirectory: consDir, EnvironmentAllowlist: input.config.piEnv, Provider: input.config.provider, Model: input.config.model, SharedSpaceID: sharedSpaceID, PrivateSpaceID: memoryPrivateSpace, ExtraMemoryTools: []string{skillsListToolName}},
		RoomInput: consolidationPrompt, HumanMessageID: "human-" + sanitizeID(input.family) + "-consolidate", MemoryBaseURL: input.config.gmsURL, MemoryAuthToken: input.config.gmsToken, PiBinary: input.config.piBinary, PiExtensionPath: extensionPath,
		PromptRequestID: fmt.Sprintf("prompt-%s-%s-%s-consolidate", sanitizeID(input.config.evaluationID), input.config.arm, sanitizeID(input.family)), EventLogPath: filepath.Join(consDir, "host-events.jsonl"), PiStderrPath: filepath.Join(consDir, "pi-stderr.log"),
	})
	if err != nil {
		return "kept_raw", sourceMap
	}
	status := "kept_raw"
	if reply := finalAgentOutput(turn.Messages, "agent-memory"); reply != nil {
		if consolidated, provenance, valid := parseConsolidatedSkills(*reply, raw); valid {
			capped, dropped := capConsolidatedSkills(consolidated, maxConsolidatedSkills)
			if dropped > 0 {
				fmt.Printf("[%s] consolidation %s: governance cap dropped the last %d consolidated skills (had %d, keeping %d)\n",
					input.config.arm, input.family, dropped, len(consolidated), len(capped))
			}
			(*input.ledger)[input.family], sourceMap, status = capped, provenance, "consolidated"
		}
	}
	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	_, drainErr := session.DrainEvidence(drainCtx, runtime.DrainRequest{RoomID: roomID, MemoryBaseURL: input.config.gmsURL, MemoryAuthToken: input.config.gmsToken, EventLogPath: filepath.Join(consDir, "host-events.jsonl")})
	drainCancel()
	if drainErr != nil {
		(*input.ledger)[input.family] = raw
		status, sourceMap = "kept_raw", map[string][]string{}
	}
	// Persist only after the final fail-open decision, so this artifact is
	// always identical to the frozen ledger later test tasks consume.
	skillsDir := filepath.Join(input.config.outDir, "work-"+input.config.arm, "skills")
	if err := os.MkdirAll(skillsDir, 0o755); err == nil {
		var out strings.Builder
		for _, skill := range (*input.ledger)[input.family] {
			fmt.Fprintf(&out, "CONSOLIDATED SKILL\n%s\n\n", skill.Text)
		}
		_ = os.WriteFile(filepath.Join(skillsDir, "consolidated-"+sanitizeID(input.family)+".md"), []byte(out.String()), 0o644)
	}
	return status, sourceMap
}

// capConsolidatedSkills enforces the ledger governance cap after a valid
// consolidation: the kept head preserves consolidation order and the tail is
// dropped so test-time retrieval stays pageable. Provenance bookkeeping is
// left untouched — the summary still records where every raw proposal went.
func capConsolidatedSkills(consolidated []skillProposal, limit int) ([]skillProposal, int) {
	if limit <= 0 || len(consolidated) <= limit {
		return consolidated, 0
	}
	return consolidated[:limit:limit], len(consolidated) - limit
}

// parseConsolidatedSkills enforces exact-one-source coverage and strips the sources line.
func parseConsolidatedSkills(reply string, raw []skillProposal) ([]skillProposal, map[string][]string, bool) {
	blocks := splitConsolidatedSkills(reply)
	if len(blocks) == 0 {
		return nil, nil, false
	}
	used, provenance := make(map[string]bool, len(raw)), make(map[string][]string)
	consolidated := make([]skillProposal, 0, len(blocks))
	for _, block := range blocks {
		text, prefixes, ok := consolidatedSources(block)
		if !ok {
			return nil, nil, false
		}
		resolved, ok := resolveSourcePrefixes(raw, prefixes)
		if !ok {
			return nil, nil, false
		}
		for _, source := range resolved {
			if used[source.SHA256] {
				return nil, nil, false
			}
			used[source.SHA256] = true
		}
		fingerprint := skillFingerprint(text)
		provenance[fingerprint] = append([]string(nil), prefixes...)
		consolidated = append(consolidated, skillProposal{Sequence: resolved[0].Sequence, EpisodeID: resolved[0].EpisodeID, SHA256: fingerprint, Text: text})
	}
	if len(used) != len(raw) {
		return nil, nil, false
	}
	return consolidated, provenance, true
}

var consolidatedSourcesPattern = regexp.MustCompile(`(?mi)^sources:\s*([0-9a-f]{6,64}(?:\s*,\s*[0-9a-f]{6,64})*)\s*$`)

func consolidatedSources(block string) (string, []string, bool) {
	match := consolidatedSourcesPattern.FindStringSubmatchIndex(block)
	if match == nil {
		return "", nil, false
	}
	text := strings.TrimSpace(block[:match[0]] + block[match[1]:])
	if text == "" {
		return "", nil, false
	}
	prefixes := strings.Split(block[match[2]:match[3]], ",")
	for index := range prefixes {
		prefixes[index] = strings.TrimSpace(prefixes[index])
	}
	return text, prefixes, true
}

func resolveSourcePrefixes(raw []skillProposal, prefixes []string) ([]skillProposal, bool) {
	resolved := make([]skillProposal, 0, len(prefixes))
	for _, prefix := range prefixes {
		prefix = strings.TrimSpace(strings.ToLower(prefix))
		var match *skillProposal
		for i := range raw {
			if strings.HasPrefix(raw[i].SHA256, prefix) {
				if match != nil {
					return nil, false
				}
				match = &raw[i]
			}
		}
		if match == nil {
			return nil, false
		}
		resolved = append(resolved, *match)
	}
	return resolved, true
}

func frozenSkillVersion(ledger []skillProposal) string {
	encoded, _ := json.Marshal(ledger)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

type taskLocalReplayInput struct {
	config                                                                      armConfig
	tenantID, roomID, sharedSpaceID, privateSpaceID, memoryPrivateSpace, family string
	sequence                                                                    int
	episodeID, episodeDir, extensionPath, taskPrompt                            string
	ledger                                                                      *map[string][]skillProposal
	record                                                                      *attemptRecord
}

func proposalsFromEpisode(ledger []skillProposal, episodeID string) []skillProposal {
	var out []skillProposal
	for _, proposal := range ledger {
		if proposal.EpisodeID == episodeID {
			out = append(out, proposal)
		}
	}
	return out
}

// runTaskLocalReplay gives one train task exactly one second trial. It feeds
// skills_list only the proposals generated from that task's first trajectory.
func runTaskLocalReplay(ctx context.Context, session *runtime.Session, input taskLocalReplayInput) {
	guidance := proposalsFromEpisode((*input.ledger)[input.family], input.episodeID)
	if len(guidance) == 0 {
		input.record.SkillReplayStatus = "no_guidance"
		return
	}
	input.record.SkillReplayAttempted = true
	local := map[string][]skillProposal{input.family: guidance}
	retrievalRecord := attemptRecord{}
	if err := runRetrievalTurn(ctx, session, retrievalInput{config: input.config, tenantID: input.tenantID, roomID: input.roomID, sharedSpaceID: input.sharedSpaceID, privateSpaceID: input.memoryPrivateSpace, family: input.family, sequence: input.sequence, episodeID: input.episodeID, episodeDir: input.episodeDir, extensionPath: input.extensionPath, taskPrompt: input.taskPrompt, ledger: &local, record: &retrievalRecord}); err != nil {
		input.record.SkillReplayStatus = "retrieval_error"
		return
	}
	replayDir := filepath.Join(input.episodeDir, "task-replay")
	if err := os.MkdirAll(replayDir, 0o755); err != nil {
		input.record.SkillReplayStatus = "workdir_error"
		return
	}
	replayCtx, cancel := context.WithTimeout(ctx, input.config.episodeTimeout)
	defer cancel()
	turn, err := session.Turn(replayCtx, runtime.TurnRequest{
		Authority:      runtime.ExecutionAuthority{TenantID: input.tenantID, RoomID: input.roomID, AgentID: "agent-primary", ProfileKind: "ordinary", WorkingDirectory: replayDir, EnvironmentAllowlist: input.config.piEnv, Provider: input.config.provider, Model: input.config.model, SharedSpaceID: input.sharedSpaceID, PrivateSpaceID: input.privateSpaceID},
		RoomInput:      input.taskPrompt + "\n\nThis is a second task-local trial. Use recalled guidance, then solve independently.",
		HumanMessageID: fmt.Sprintf("human-%04d-%s-replay", input.sequence, sanitizeID(input.episodeID)), MemoryBaseURL: input.config.gmsURL, MemoryAuthToken: input.config.gmsToken, PiBinary: input.config.piBinary, PiExtensionPath: input.extensionPath, PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-replay", sanitizeID(input.config.evaluationID), input.config.arm, input.sequence, sanitizeID(input.episodeID)), EventLogPath: filepath.Join(replayDir, "host-events.jsonl"), PiStderrPath: filepath.Join(replayDir, "pi-stderr.log"),
	})
	if err != nil {
		input.record.SkillReplayStatus = "task_error"
		return
	}
	input.record.SkillReplayStatus, input.record.SkillReplayRecall, input.record.SkillReplayCitations = "completed", turn.Recall.State, len(turn.Recall.Citations)
	input.record.SkillReplayOutput = finalAgentOutput(turn.Messages, "agent-primary")
	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	_, drainErr := session.DrainEvidence(drainCtx, runtime.DrainRequest{RoomID: input.roomID, MemoryBaseURL: input.config.gmsURL, MemoryAuthToken: input.config.gmsToken, EventLogPath: filepath.Join(replayDir, "host-events.jsonl")})
	drainCancel()
	if drainErr != nil {
		input.record.SkillReplayStatus = "drain_error"
	}
}

// runRetrievalTurn executes one skill retrieval turn before a test episode
// (warm-skill arm, test split only): the diagnosis agent reads the family
// ledger via skills_list in a THROWAWAY room, selects the proposals that
// apply to the upcoming task, and publishes them as an unaddressed REFERENCE
// NOTES message. The turn deliberately does not run in the task room: its
// prompt must never become recallable evidence the task agent could execute —
// the smoke runs caught the task agent replaying the recalled retrieval
// instructions instead of solving the task. Only the published note crosses
// over, written as one evidence batch into the task room's shared space, so
// the task agent still meets the skills through normal pre-turn recall —
// never through harness prompt injection. The quoted fingerprints are
// recorded for attribution.
func runRetrievalTurn(ctx context.Context, session *runtime.Session, input retrievalInput) error {
	config := input.config
	retDir := filepath.Join(config.outDir, "work-"+config.arm, "retrieval",
		fmt.Sprintf("%04d-%s", input.sequence, sanitizeID(input.episodeID)))
	if err := os.MkdirAll(retDir, 0o755); err != nil {
		return fmt.Errorf("retrieval work dir: %w", err)
	}
	// C3 formal source: GMS advisory is the only model-visible skill input.
	// The runner-local ledger stays available solely for comparison telemetry.
	localComparison := append([]skillProposal(nil), (*input.ledger)[input.family]...)
	advisory := []skillProposal(nil)
	if config.armBReporter != nil {
		advisory, _ = config.armBReporter.advisoryLedger(ctx, input.record)
	} else {
		input.record.SkillAdvisoryReadStatus = "not_configured"
	}
	guidance, source := c3RetrievalGuidance(advisory, localComparison, input.record.SkillAdvisoryReadStatus)
	// The warm-skill arm keeps the frozen runner-local ledger as the
	// model-visible authority; C3 advisory sourcing stays telemetry plus a
	// supplement, never a replacement (attemptRecord: local-ledger authority).
	if len(guidance) == 0 {
		guidance, source = localComparison, "local_ledger"
	} else {
		guidance, source = supplementLedger(localComparison, advisory), "local_ledger+advisory"
	}
	input.record.SkillRetrievalSource = source
	input.record.SkillRetrievalAdvisoryCount = len(advisory)
	input.record.SkillRetrievalLocalComparisonCount = len(localComparison)
	extensionPath := filepath.Join(retDir, "retrieval-extension.mjs")
	ledger := guidance
	if err := writeMemoryTurnExtension(input.extensionPath, extensionPath, skillsToolJS(ledger)); err != nil {
		return err
	}
	scratchRoom, scratchShared, scratchPrivate, _ := warmSkillRetrievalRoom(input.family, input.episodeID)
	retCtx, retCancel := context.WithTimeout(ctx, retrievalTurnDeadline(len(buildLedgerChunks(ledger))))
	defer retCancel()
	promptHead := retrievalPromptHead
	if input.stream {
		promptHead = streamRetrievalPromptHead
	}
	turn, err := session.Turn(retCtx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{
			TenantID: input.tenantID, RoomID: scratchRoom, AgentID: "agent-memory", ProfileKind: "memory",
			WorkingDirectory: retDir, EnvironmentAllowlist: config.piEnv,
			Provider: config.provider, Model: config.model,
			SharedSpaceID:    scratchShared,
			PrivateSpaceID:   scratchPrivate,
			ExtraMemoryTools: []string{skillsListToolName},
		},
		RoomInput:       promptHead + "\n" + input.taskPrompt,
		HumanMessageID:  fmt.Sprintf("human-%04d-%s-retr", input.sequence, sanitizeID(input.episodeID)),
		MemoryBaseURL:   config.gmsURL,
		MemoryAuthToken: config.gmsToken,
		PiBinary:        config.piBinary,
		PiExtensionPath: extensionPath,
		PromptRequestID: fmt.Sprintf("prompt-%s-%s-%04d-%s-retr", sanitizeID(config.evaluationID), config.arm, input.sequence, sanitizeID(input.episodeID)),
		EventLogPath:    filepath.Join(input.episodeDir, "host-events.jsonl"),
		PiStderrPath:    filepath.Join(retDir, "pi-stderr.log"),
	})
	if err != nil {
		return err
	}
	input.record.SkillRetrievalRecall = turn.Recall.State
	var reply *string
	if reply = finalAgentOutput(turn.Messages, "agent-memory"); reply == nil {
		input.record.SkillRetrievalStatus = "no_reply"
		input.record.SkillRetrievalText = reply
		return nil
	} else {
		input.record.SkillRetrievalText = reply
		fingerprints := extractRetrievedFingerprints(*reply)
		input.record.SkillRetrievalSHA256 = strings.Join(fingerprints, ",")
		switch {
		case len(fingerprints) == 0:
			input.record.SkillRetrievalStatus = "declined"
			// A declined turn publishes no note: NO_SKILL_APPLICABLE carries no
			// skill, and an instruction-shaped recall item only invites the
			// task agent to mimic the retrieval role.
			fmt.Printf("[%s] retrieval %04d recall=%s(%d) declined\n",
				config.arm, input.sequence, turn.Recall.State, len(turn.Recall.Citations))
			return nil
			default:
				input.record.SkillRetrievalStatus = "published"
				input.record.SkillRetrievalCount = len(fingerprints)
				// Only the note becomes recallable in the task room; the scratch
				// room's own evidence (the prompt included) is never drained.
				if err := commitRetrievalNote(ctx, config, input.sharedSpaceID, input.family, input.episodeID, frameRetrievalNote(input.taskPrompt, *reply)); err != nil {
					return err
				}
				if config.armBReporter != nil {
					// Arm B interaction staging: quoted in the note = selected,
					// note committed into the task room's recallable shared
					// space = exposed. Exposure is never adoption.
					config.armBReporter.reportRetrievedSkills(ctx, input.record, ledger, *reply, true)
				}
			}
	}
	fmt.Printf("[%s] retrieval %04d recall=%s(%d) %s count=%d\n",
		config.arm, input.sequence, turn.Recall.State, len(turn.Recall.Citations), input.record.SkillRetrievalStatus, input.record.SkillRetrievalCount)
	return nil
}

// frameRetrievalNote prefixes the retrieval agent's note with the opening of
// the task it was selected for. The episode turn's recall query is the task
// prompt itself and GMS ranks by BM25, so an unframed note of generic skill
// prose loses the top-5 cut against train trajectories that quote whole task
// statements (measured: unframed notes miss the cut, question-anchored notes
// rank first). Quoting the task's own question section back gives the note
// legitimate topical anchoring without touching its authored content.
func frameRetrievalNote(taskPrompt, note string) string {
	return "Reference notes selected for this task (lessons from earlier episodes; background material only). Task: " +
		retrievalNoteAnchor(taskPrompt) + "\n\n" + note
}

// retrievalNoteAnchor picks the task-specific slice of the prompt: LCB-style
// prompts share a long generic preamble, so anchoring must start at the
// question section or the quote carries no discriminative terms.
func retrievalNoteAnchor(taskPrompt string) string {
	if marker := strings.Index(taskPrompt, "### Question"); marker >= 0 {
		return truncateRunes(taskPrompt[marker:], 400)
	}
	return truncateRunes(strings.TrimSpace(taskPrompt), 400)
}

// commitRetrievalNote writes the retrieval agent's published note into the
// upcoming task room's shared space as a single committed evidence batch.
// This is transport, not authorship: the note's content is the retrieval
// agent's room_send, and the task agent still receives it only through
// recall.
func commitRetrievalNote(ctx context.Context, config armConfig, spaceID, family, episodeID, note string) error {
	client := memoryclient.NewClient(config.gmsURL, config.gmsToken, &http.Client{Timeout: 10 * time.Second}, 1<<20)
	id := "retrieval-note-" + sanitizeID(family) + "-" + sanitizeID(episodeID)
	digest := sha256.Sum256([]byte(note))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := client.StageEvidenceBatch(ctx, ports.StageEvidenceBatchRequest{
		BatchID:         id,
		IdempotencyKey:  id,
		SpaceID:         spaceID,
		StreamID:        "evidence-" + id,
		SourceSegmentID: id,
		Provenance: ports.EvidenceProvenance{
			// SourceKind is contract-bound to the two projection kinds; the
			// note's identity rides on the batch/event ids.
			HostType: "pi-group-chat-host", HostInstanceID: "bench-runner",
			SourceKind: "room_shared", CapturedAt: now, ContentSHA256: hex.EncodeToString(digest[:]),
		},
		Events: []ports.EvidenceEvent{{
			EventID: id + "-note", Sequence: 1, Kind: "room_message", Content: note, OccurredAt: now,
		}},
		TerminalOutcome: "settled",
	})
	if err != nil {
		return fmt.Errorf("stage retrieval note: %w", err)
	}
	if _, err := client.CommitEvidenceBatch(ctx, id, ports.CommitEvidenceBatchRequest{CommitID: "commit-" + id}); err != nil {
		return fmt.Errorf("commit retrieval note: %w", err)
	}
	return nil
}

// retrievalTurnDeadline scales the retrieval turn's wall-clock budget with
// the ledger it must page through: a floor covering the index read and the
// drafted publish, plus a per-chunk allowance for the serial skills_list
// round-trips, hard-capped so one slow retrieval turn cannot eat the
// episode's own budget. batch-5 died on the old flat 5-minute deadline when
// the ledger grew to 38 chunks at evening LLM latency; batch-6's first
// launch died at 120s+8s/chunk because a 14-chunk ledger under 32-way
// parallel load needs ~30s per skills_list round-trip (observed 12-41s in
// the aborted run's retrieval transcripts), so the per-chunk allowance must
// cover a real model round-trip, not a fast tool call.
func retrievalTurnDeadline(chunks int) time.Duration {
	budget := 240*time.Second + time.Duration(chunks)*40*time.Second
	if budget > 1080*time.Second {
		budget = 1080 * time.Second
	}
	return budget
}

func skillLedgerHas(ledger map[string][]skillProposal, family, fingerprint string) bool {
	for _, proposal := range ledger[family] {
		if proposal.SHA256 == fingerprint {
			return true
		}
	}
	return false
}

// buildTrajectoryChunks packs the transcript into chunk-sized pages: one
// "[role] content" line per entry, entries never split across chunks unless
// a single entry exceeds the cap (then it is rune-hard-split).
func buildTrajectoryChunks(transcript []transcriptEntry) []string {
	var chunks []string
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			chunks = append(chunks, current.String())
			current.Reset()
		}
	}
	for _, entry := range transcript {
		line := "[" + entry.Role + "] " + entry.Content
		for {
			runes := []rune(line)
			if len(runes) <= maxTrajectoryChunkChars {
				break
			}
			flush()
			chunks = append(chunks, string(runes[:maxTrajectoryChunkChars]))
			line = string(runes[maxTrajectoryChunkChars:])
		}
		if current.Len() > 0 && current.Len()+len(line)+1 > maxTrajectoryChunkChars {
			flush()
		}
		if current.Len() > 0 {
			current.WriteString("\n")
		}
		current.WriteString(line)
	}
	flush()
	return chunks
}

// buildLedgerChunks renders the family ledger as skills_list pages: the
// leading chunk(s) are a compact index — one line per proposal with its entry
// number, name, trigger, sha256 prefix, and the detail chunk holding its full
// text — followed by detail chunks with ledgerEntriesPerChunk full proposals
// each. The index keeps a retrieval turn cheap: the agent shortlists from
// names and triggers, then pages only into plausible detail chunks.
func buildLedgerChunks(ledger []skillProposal) []string {
	if len(ledger) == 0 {
		return nil
	}
	var details []string
	for start := 0; start < len(ledger); start += ledgerEntriesPerChunk {
		end := start + ledgerEntriesPerChunk
		if end > len(ledger) {
			end = len(ledger)
		}
		var entries []string
		for index := start; index < end; index++ {
			proposal := ledger[index]
			entries = append(entries, fmt.Sprintf("entry %d | from episode %s | sha256 %s\n%s",
				index+1, proposal.EpisodeID, shaPrefix(proposal.SHA256), truncateRunes(proposal.Text, skillTextMaxChars)))
		}
		details = append(details, strings.Join(entries, "\n\n"))
	}
	// Index pages precede the details and each index line names the detail
	// chunk holding that entry, so the per-line chunk numbers must agree with
	// the page count. Chunk numbering is 0-based, so with P index pages the
	// first detail chunk is chunk P. Pack to a fixpoint: the detail-chunk
	// reference adds at most a couple of digits, so this converges
	// immediately in practice.
	detailBase := 1
	var index []string
	for iteration := 0; iteration < 8; iteration++ {
		index = packLedgerIndex(ledger, detailBase)
		if len(index) == detailBase {
			return append(index, details...)
		}
		detailBase = len(index)
	}
	return append(index, details...)
}

// packLedgerIndex renders one index line per proposal (name, trigger, sha256
// prefix, and the detail chunk holding the full entry at the given base) and
// packs the lines into pages of at most ledgerIndexChunkChars runes.
func packLedgerIndex(ledger []skillProposal, detailBase int) []string {
	lines := make([]string, len(ledger))
	for index, proposal := range ledger {
		name, trigger := skillMetaLine(proposal.Text)
		lines[index] = fmt.Sprintf("entry %d | name: %s | trigger: %s | sha256 %s | full text in chunk %d",
			index+1, name, trigger, shaPrefix(proposal.SHA256), detailBase+index/ledgerEntriesPerChunk)
	}
	var pages []string
	var current strings.Builder
	for _, line := range lines {
		if current.Len() > 0 && current.Len()+len(line)+1 > ledgerIndexChunkChars {
			pages = append(pages, current.String())
			current.Reset()
		}
		if current.Len() > 0 {
			current.WriteString("\n")
		}
		current.WriteString(line)
	}
	if current.Len() > 0 {
		pages = append(pages, current.String())
	}
	return pages
}

var (
	skillNameLinePattern    = regexp.MustCompile(`(?im)^name:\s*(.+)$`)
	skillTriggerLinePattern = regexp.MustCompile(`(?im)^trigger:\s*(.+)$`)
)

// skillMetaLine pulls the declared name and trigger out of a proposal's
// structured text for the ledger index; an unstructured proposal falls back
// to its first non-empty line so the index line never goes blank.
func skillMetaLine(text string) (name, trigger string) {
	if match := skillNameLinePattern.FindStringSubmatch(text); match != nil {
		name = strings.TrimSpace(match[1])
	}
	if match := skillTriggerLinePattern.FindStringSubmatch(text); match != nil {
		trigger = strings.TrimSpace(match[1])
	}
	if name == "" {
		for _, line := range strings.Split(text, "\n") {
			if trimmed := strings.TrimSpace(line); trimmed != "" {
				name = trimmed
				break
			}
		}
	}
	return truncateRunes(name, ledgerNameMaxChars), truncateRunes(trigger, ledgerTriggerMaxChars)
}

func shaPrefix(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// splitBlocks extracts non-empty bodies following an exact block header.
func splitBlocks(reply, header string) []string {
	var blocks []string
	for {
		start := strings.Index(reply, header)
		if start < 0 {
			break
		}
		rest := reply[start+len(header):]
		end := strings.Index(rest, header)
		block := rest
		if end >= 0 {
			block = rest[:end]
		}
		block = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(block), ":-"))
		if block != "" {
			blocks = append(blocks, block)
		}
		reply = rest
		if end < 0 {
			break
		}
	}
	return blocks
}

func splitSkillProposals(reply string) []string {
	blocks := splitBlocks(reply, "SKILL PROPOSAL")
	var proposals []string
	for _, block := range blocks {
		if !strings.EqualFold(block, "NO_SKILL") {
			proposals = append(proposals, block)
		}
	}
	return proposals
}

func splitConsolidatedSkills(reply string) []string { return splitBlocks(reply, "CONSOLIDATED SKILL") }

// extractRetrievedSelections preserves the quoted fingerprint and episode
// discriminator. The pair identifies distinct GMS candidates with identical
// rendered guidance.
type retrievedSelection struct{ Fingerprint, EpisodeID string }

var retrievalSelectionPattern = regexp.MustCompile(`\[skill ([0-9a-f]{6,64}) from episode ([^\]]+)\]`)

func extractRetrievedSelections(reply string) []retrievedSelection {
	matches := retrievalSelectionPattern.FindAllStringSubmatch(reply, -1)
	out := make([]retrievedSelection, 0, len(matches))
	for _, match := range matches {
		out = append(out, retrievedSelection{Fingerprint: match[1], EpisodeID: match[2]})
	}
	return out
}

func extractRetrievedFingerprints(reply string) []string {
	selections := extractRetrievedSelections(reply)
	fingerprints := make([]string, 0, len(selections))
	for _, selection := range selections {
		fingerprints = append(fingerprints, selection.Fingerprint)
	}
	return fingerprints
}

// writeMemoryTurnExtension splices an extra tool surface into the episode's
// Pi extension: the base extension's module prologue (env consts, helpers)
// and its registerTool body are kept verbatim, the tool JS is inserted just
// before the body closes. The result is one self-contained .mjs the memory
// turn loads instead of the base extension.
func writeMemoryTurnExtension(basePath, outPath, toolJS string) error {
	source, err := os.ReadFile(basePath)
	if err != nil {
		return fmt.Errorf("read base extension: %w", err)
	}
	const marker = "export default function (pi) {"
	index := strings.Index(string(source), marker)
	if index < 0 {
		return fmt.Errorf("base extension %s: unexpected shape (no %q)", basePath, marker)
	}
	head := strings.TrimSpace(string(source)[:index])
	body := string(source)[index+len(marker):]
	if last := strings.LastIndex(body, "}"); last >= 0 {
		body = body[:last]
	}
	var out strings.Builder
	out.WriteString(head + "\n")
	out.WriteString("export default function (pi) {\n")
	out.WriteString(body)
	out.WriteString(toolJS)
	out.WriteString("}\n")
	return os.WriteFile(outPath, []byte(out.String()), 0o644)
}

// trajectoryToolJS registers the trajectory_read tool with the episode's
// trajectory chunks baked in as a JSON literal.
func trajectoryToolJS(chunks []string) string {
	chunksJSON, err := json.Marshal(chunks)
	if err != nil {
		chunksJSON = []byte("[]")
	}
	return `
  const TRAJECTORY_CHUNKS = ` + string(chunksJSON) + `;
  pi.registerTool({
    name: "trajectory_read",
    label: "Trajectory read",
    description: "Read one chunk of the finished episode's full trajectory (the task, the agent's every step, the published result). Chunk indices are 0-based; every response header states total_chunks.",
    parameters: { type: "object", properties: { chunk: { type: "integer", description: "0-based chunk index" } }, additionalProperties: false },
    async execute(_toolCallId, args, _signal, _onUpdate, _ctx) {
      // Pi 0.85 passes the opaque tool-call ID first and params second. A
      // one-argument callback receives only the ID, silently making every
      // page request fall back to chunk 0.
      // Models do call pagination tools bare despite the prompt; a missing
      // chunk serves the first page instead of an error loop.
      let index = Math.trunc(Number(args && args.chunk));
      if (!Number.isInteger(index)) {
        index = 0;
      }
      if (index < 0 || index >= TRAJECTORY_CHUNKS.length) {
        return { content: [{ type: "text", text: "invalid chunk " + String(args && args.chunk) + "; total_chunks=" + TRAJECTORY_CHUNKS.length }] };
      }
      return { content: [{ type: "text", text: "chunk " + index + "/" + (TRAJECTORY_CHUNKS.length - 1) + "\n" + TRAJECTORY_CHUNKS[index] }] };
    },
  });
`
}

// skillsToolJS registers the skills_list tool with the family ledger pages
// baked in as a JSON literal. Past skillsListCallBudget calls the tool stops
// serving pages and nudges the agent to publish, so a model stuck paging
// cannot stall the turn until its deadline.
func skillsToolJS(ledger []skillProposal) string {
	chunks := buildLedgerChunks(ledger)
	chunksJSON, err := json.Marshal(chunks)
	if err != nil {
		chunksJSON = []byte("[]")
	}
	return `
  const SKILL_LEDGER_CHUNKS = ` + string(chunksJSON) + `;
  let SKILL_LIST_CALLS = 0;
  pi.registerTool({
    name: "skills_list",
    label: "Skills list",
    description: "Read this room's skill ledger. The first chunks are a compact index (one line per skill: entry number, name, trigger, sha256 prefix, and the chunk holding its full text); the later chunks hold the full entries, ` + strconv.Itoa(ledgerEntriesPerChunk) + ` per chunk. Chunk indices are 0-based; every response header states total_chunks.",
    parameters: { type: "object", properties: { chunk: { type: "integer", description: "0-based chunk index" } }, additionalProperties: false },
    async execute(_toolCallId, args, _signal, _onUpdate, _ctx) {
      // See trajectory_read: params are the second callback argument in Pi 0.85.
      SKILL_LIST_CALLS++;
      if (SKILL_LIST_CALLS > ` + strconv.Itoa(skillsListCallBudget) + `) {
        return { content: [{ type: "text", text: "skills_list budget exhausted after " + SKILL_LIST_CALLS + " calls: you have seen the index and enough entries. Stop paging and publish your reply now via room_send, or publish NO_SKILL_APPLICABLE if nothing applies." }] };
      }
      let index = Math.trunc(Number(args && args.chunk));
      if (!Number.isInteger(index)) {
        index = 0;
      }
      if (index < 0 || index >= SKILL_LEDGER_CHUNKS.length) {
        return { content: [{ type: "text", text: "invalid chunk " + String(args && args.chunk) + "; total_chunks=" + SKILL_LEDGER_CHUNKS.length }] };
      }
      return { content: [{ type: "text", text: "chunk " + index + "/" + (SKILL_LEDGER_CHUNKS.length - 1) + "\n" + SKILL_LEDGER_CHUNKS[index] }] };
    },
  });
`
}

// skillFingerprint dedupes proposals the way the graph-memory skill-proposal
// service does: sha256 over whitespace-normalized text, so formatting drift
// alone cannot mint a second skill.
func skillFingerprint(text string) string {
	normalized := strings.Join(strings.Fields(text), " ")
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
}

// truncateRunes cuts at limit runes, never mid-rune: published proposals may
// be non-ASCII and the block goes back into a model prompt.
func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

// stageReferenceFiles copies each declared source out of the refs root into
// the episode workdir. Dst paths must be relative and stay inside the
// workdir; the copy stops at the first failure and reports how many files
// were staged so partial state stays inspectable in the workdir.
func stageReferenceFiles(refsRoot, workDir string, files []stageFile) (int, error) {
	if len(files) == 0 {
		return 0, nil
	}
	if refsRoot == "" {
		return 0, fmt.Errorf("stage reference files: --refs-root is empty")
	}
	staged := 0
	for index, file := range files {
		if file.Src == "" || file.Dst == "" {
			return staged, fmt.Errorf("stage file %d: src and dst must be non-empty", index)
		}
		if filepath.IsAbs(file.Dst) {
			return staged, fmt.Errorf("stage file %d: dst %q must be relative to the episode workdir", index, file.Dst)
		}
		cleanDst := filepath.Clean(file.Dst)
		if cleanDst == ".." || strings.HasPrefix(cleanDst, ".."+string(filepath.Separator)) {
			return staged, fmt.Errorf("stage file %d: dst %q escapes the episode workdir", index, file.Dst)
		}
		source := filepath.Join(refsRoot, file.Src)
		target := filepath.Join(workDir, cleanDst)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return staged, fmt.Errorf("stage file %d: create dir: %w", index, err)
		}
		if err := copyFile(source, target); err != nil {
			return staged, fmt.Errorf("stage file %d (%s): %w", index, file.Src, err)
		}
		staged++
	}
	return staged, nil
}

func copyFile(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(target)
	if err != nil {
		return err
	}
	if _, err := out.ReadFrom(in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func needsRefsRoot(episodes []manifestEpisode) bool {
	for _, episode := range episodes {
		if len(episode.StageFiles) > 0 {
			return true
		}
	}
	return false
}

// compactTrajectoryFallback makes the last agent outcome available even if a
// model mishandles tool pagination. It is bounded so diagnosis prompts remain
// comfortably inside context while trajectory_read still exposes the full log.
func compactTrajectoryFallback(transcript []transcriptEntry) string {
	for index := len(transcript) - 1; index >= 0; index-- {
		if transcript[index].Role == "agent" && strings.TrimSpace(transcript[index].Content) != "" {
			return "\n\n--- compact terminal trajectory fallback ---\n" + truncateRunes(transcript[index].Content, skillTextMaxChars) + "\n--- end compact fallback ---\n"
		}
	}
	return ""
}

// finalPiSessionOutput recovers the final plain-text assistant response from
// Pi's append-only session JSONL when it was not published through room_send.
// The runner owns the episode working directory, so only its direct .jsonl
// children are considered; host-events.jsonl is deliberately excluded.
func finalPiSessionOutput(episodeDir string) *string {
	entries, err := os.ReadDir(episodeDir)
	if err != nil {
		return nil
	}
	var latest *string
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "host-events.jsonl" || filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(episodeDir, entry.Name()))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			var event struct {
				Message struct {
					Role    string `json:"role"`
					Content []struct {
						Type string `json:"type"`
						Text string `json:"text"`
					} `json:"content"`
				} `json:"message"`
			}
			if json.Unmarshal([]byte(line), &event) != nil || event.Message.Role != "assistant" {
				continue
			}
			var text []string
			for _, item := range event.Message.Content {
				if item.Type == "text" && strings.TrimSpace(item.Text) != "" {
					text = append(text, item.Text)
				}
			}
			if value := strings.TrimSpace(strings.Join(text, "\n")); value != "" {
				copy := value
				latest = &copy
			}
		}
	}
	return latest
}

// finalAgentOutput returns the content of the last message the agent published
// in the turn; nil when the agent produced no room message.
func finalAgentOutput(messages []runtime.VisibleMessage, agentID string) *string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].AuthorID == agentID && strings.TrimSpace(messages[index].Content) != "" {
			content := messages[index].Content
			return &content
		}
	}
	return nil
}

// finalTaskOutput returns the last room message the task agent published that
// carries a fenced code block, falling back to its last non-empty publish.
// Models that close with a bare "TASK_COMPLETE" room_send after the code
// message would otherwise bury their solution from grading.
func finalTaskOutput(messages []runtime.VisibleMessage) *string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].AuthorID != "agent-primary" || strings.TrimSpace(messages[index].Content) == "" {
			continue
		}
		content := messages[index].Content
		if strings.Contains(content, "```") {
			return &content
		}
	}
	return finalAgentOutput(messages, "agent-primary")
}

// taskOutputCapture is what a finished task turn leaves for the record: the
// graded view prefers the last code-bearing publish, while final_output keeps
// its historical last-non-empty-publish semantics for run-to-run
// comparability.
type taskOutputCapture struct {
	final       *string // last non-empty publish; Pi session text when none was published
	graded      *string // last publish carrying a code fence, when any
	fromSession bool    // final came from the session log, not a room publish
}

// captureTaskOutput collects both outputs from one finished task turn. When
// the agent never published through room_send (or published only codeless
// text), Pi's session log recovers the final answer exactly as the old
// single-field capture did.
func captureTaskOutput(episodeDir string, messages []runtime.VisibleMessage) taskOutputCapture {
	capture := taskOutputCapture{final: finalAgentOutput(messages, "agent-primary"), graded: finalTaskOutput(messages)}
	if capture.final == nil {
		if session := finalPiSessionOutput(episodeDir); session != nil {
			capture.final, capture.fromSession = session, true
		}
	}
	return capture
}

// roomTranscript turns a plain episode's room messages into the trajectory
// the diagnosis agent pages through: the human task prompt plus every agent
// publish, in room order. Without it the plain (no-simulator) path would
// hand diagnosis turns an empty trajectory.
func roomTranscript(messages []runtime.VisibleMessage) []transcriptEntry {
	var transcript []transcriptEntry
	for _, message := range messages {
		if strings.TrimSpace(message.Content) == "" {
			continue
		}
		role := "user"
		if message.AuthorID == "agent-primary" {
			role = "agent"
		}
		transcript = append(transcript, transcriptEntry{Role: role, Content: message.Content})
	}
	return transcript
}

func familySequence(episodes []manifestEpisode) []string {
	seen := map[string]bool{}
	var families []string
	for _, episode := range episodes {
		if !seen[episode.FamilyID] {
			seen[episode.FamilyID] = true
			families = append(families, episode.FamilyID)
		}
	}
	sort.Strings(families)
	return families
}

func episodesOfFamily(episodes []manifestEpisode, family string) []manifestEpisode {
	var matched []manifestEpisode
	for _, episode := range episodes {
		if episode.FamilyID == family {
			matched = append(matched, episode)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return *matched[i].Order < *matched[j].Order })
	return matched
}

// loadManifest parses one JSONL manifest and resolves the optional fields:
// order defaults to the manifest line order (converters emit train before
// test, which is the warm-arm learning order), and family_id falls back to
// the record domain, then benchmark, then a shared default group.
func loadManifest(path string) ([]manifestEpisode, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var episodes []manifestEpisode
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var episode manifestEpisode
		if err := json.Unmarshal(line, &episode); err != nil {
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
		episodes = append(episodes, episode)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(episodes) == 0 {
		return nil, fmt.Errorf("manifest %s has no episodes", path)
	}
	return episodes, nil
}

func splitList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

func sanitizeID(value string) string {
	var builder strings.Builder
	for _, runeValue := range strings.ToLower(value) {
		switch {
		case runeValue >= 'a' && runeValue <= 'z', runeValue >= '0' && runeValue <= '9':
			builder.WriteRune(runeValue)
		default:
			builder.WriteRune('-')
		}
	}
	return strings.Trim(builder.String(), "-")
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "bench-runner: "+format+"\n", args...)
	os.Exit(1)
}

// ---------------------------------------------------------------------------
// Dual-control episodes (tau2-style): an LLM user simulator talks to the
// agent between turns. The simulator's system prompt is manifest material and
// never enters the agent prompt; the agent only ever sees the conversation
// transcript. Each agent turn is one fresh Pi process whose prompt carries
// the transcript so far (the runtime's per-turn context is roomInput +
// recalled evidence) — a documented deviation from an in-process multi-turn
// session, recorded per-episode as user_turns and termination.
// ---------------------------------------------------------------------------

const dualControlOpening = "Hi! How can I help you today?"

type dualControlInput struct {
	episode    manifestEpisode
	config     armConfig
	base       func(turnIndex int) runtime.TurnRequest
	episodeDir string
	sequence   int
}

func runDualControlEpisode(ctx context.Context, session *runtime.Session, input dualControlInput) (runtime.TurnResult, []transcriptEntry, string, int, error) {
	sim := input.episode.UserSimulator
	var transcript []transcriptEntry
	textFallbacks := 0
	// The simulator opens by answering the agent's default greeting (the
	// official orchestrator seeds the trajectory the same way).
	userMessage, err := userSimulatorReply(ctx, input.config, sim, []transcriptEntry{{Role: "agent", Content: dualControlOpening}})
	if err != nil {
		return runtime.TurnResult{}, transcript, "", textFallbacks, fmt.Errorf("user simulator opening: %w", err)
	}
	if hasStopMarker(userMessage, sim.StopMarkers) {
		// The official orchestrator appends the user's stop-marker message to
		// the trajectory before ending; keep the transcript identical.
		transcript = append(transcript, transcriptEntry{Role: "user", Content: userMessage})
		return runtime.TurnResult{}, transcript, "user_stop", textFallbacks, nil
	}
	maxTurns := sim.MaxUserTurns
	if maxTurns <= 0 {
		maxTurns = 20
	}
	var lastTurn runtime.TurnResult
	termination := "max_turns"
	for turnIndex := 1; turnIndex <= maxTurns; turnIndex++ {
		transcript = append(transcript, transcriptEntry{Role: "user", Content: userMessage})
		request := input.base(turnIndex)
		request.HumanMessageID = fmt.Sprintf("human-%04d-%s-t%d", input.sequence, sanitizeID(input.episode.EpisodeID), turnIndex)
		request.RoomInput = renderDualControlPrompt(input.episode.Turns[0].Prompt, transcript, toolActionLog(input.episodeDir))
		turn, turnErr := session.Turn(ctx, request)
		if turnErr != nil {
			return lastTurn, transcript, "", textFallbacks, turnErr
		}
		lastTurn = turn
		reply := finalAgentOutput(turn.Messages, "agent-primary")
		if reply == nil {
			// Official tau2 delivers every agent message to the user; the
			// room channel only delivers room_send publishes. When the agent
			// ends its turn with plain text, fall back to that text so the
			// trajectory keeps official message semantics (a channel
			// artifact, counted in text_fallback_turns for disclosure).
			if text := lastAssistantText(input.episodeDir); text != "" {
				reply = &text
				textFallbacks++
			}
		}
		if reply == nil {
			// The agent produced nothing user-directed; the official protocol
			// ends the episode as an agent communication error (reward 0).
			return lastTurn, transcript, "agent_error", textFallbacks, nil
		}
		transcript = append(transcript, transcriptEntry{Role: "agent", Content: *reply})
		next, err := userSimulatorReply(ctx, input.config, sim, transcript)
		if err != nil {
			return lastTurn, transcript, "", textFallbacks, fmt.Errorf("user simulator turn %d: %w", turnIndex, err)
		}
		if hasStopMarker(next, sim.StopMarkers) {
			transcript = append(transcript, transcriptEntry{Role: "user", Content: next})
			return lastTurn, transcript, "user_stop", textFallbacks, nil
		}
		userMessage = next
	}
	return lastTurn, transcript, termination, textFallbacks, nil
}

// lastAssistantText reads the turn's Pi session log (newest JSONL in the
// episode dir besides the host and tau2 logs) and returns the last assistant
// message's concatenated text — the output the official protocol would have
// delivered to the user.
func lastAssistantText(episodeDir string) string {
	entries, err := os.ReadDir(episodeDir)
	if err != nil {
		return ""
	}
	var newest string
	var newestTime time.Time
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		if entry.Name() == "host-events.jsonl" || entry.Name() == "tau2-actions.jsonl" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil || newest != "" && !info.ModTime().After(newestTime) {
			continue
		}
		newest, newestTime = entry.Name(), info.ModTime()
	}
	if newest == "" {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(episodeDir, newest))
	if err != nil {
		return ""
	}
	type sessionEntry struct {
		Type    string `json:"type"`
		Message struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	last := ""
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var entry sessionEntry
		if json.Unmarshal([]byte(line), &entry) != nil || entry.Type != "message" || entry.Message.Role != "assistant" {
			continue
		}
		var text strings.Builder
		for _, part := range entry.Message.Content {
			if part.Type == "text" {
				text.WriteString(part.Text)
			}
		}
		if strings.TrimSpace(text.String()) != "" {
			last = text.String()
		}
	}
	return strings.TrimSpace(last)
}

// closeSidecarEnv asks the tool-plane sidecar to close the episode's
// environment. The env key mirrors the extension's derivation: arms run the
// same task ids, so the workdir's last three path segments qualify the key.
func closeSidecarEnv(ctx context.Context, sidecarURL, episodeID, episodeDir string) {
	segments := make([]string, 0, 4)
	for _, part := range strings.Split(filepath.ToSlash(episodeDir), "/") {
		if part != "" {
			segments = append(segments, part)
		}
	}
	if len(segments) > 3 {
		segments = segments[len(segments)-3:]
	}
	body, _ := json.Marshal(map[string]string{
		"episode_id": episodeID + "@" + strings.Join(segments, "/"),
	})
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(callCtx, http.MethodPost,
		strings.TrimSuffix(sidecarURL, "/")+"/close", bytes.NewReader(body))
	if err != nil {
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		fmt.Printf("[sidecar] close %s: %v\n", episodeID, err)
		return
	}
	defer response.Body.Close()
	io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
}

func renderDualControlPrompt(briefing string, transcript []transcriptEntry, actionLog string) string {
	var builder strings.Builder
	builder.WriteString(briefing)
	builder.WriteString("\n\n=== Conversation with the user so far ===\n")
	for _, entry := range transcript {
		if entry.Role == "user" {
			builder.WriteString("\nUser: " + entry.Content + "\n")
		} else {
			builder.WriteString("\nYou: " + entry.Content + "\n")
		}
	}
	if actionLog != "" {
		builder.WriteString("\n=== Your tool calls so far in this session ===\n")
		builder.WriteString(actionLog)
	}
	builder.WriteString("\nRespond to the user's latest message. Use your domain tools when the policy requires them; earlier tool results are listed above, so do not repeat a call whose information cannot have changed. Your turn is only complete once you publish your reply to the user with the room_send tool — plain assistant text is not delivered to the user.")
	return builder.String()
}

// toolActionLog renders the episode's tau2-actions.jsonl (written by the
// domain extension on every tool call) for re-injection into the next turn's
// prompt. Official agents keep their own tool results in context across
// turns; fresh-process turns lose them, so this restores the same
// information — the agent's own calls, nothing from the grader or the user
// simulator. Results are truncated: variant dumps run to kilobytes and the
// prompt re-renders every turn.
func toolActionLog(episodeDir string) string {
	path := filepath.Join(episodeDir, "tau2-actions.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	const (
		maxActions   = 40
		maxResult    = 400
		maxArguments = 200
	)
	var lines []string
	for _, raw := range strings.Split(string(data), "\n") {
		if raw == "" || len(lines) >= maxActions {
			continue
		}
		var call struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
			Result    string                 `json:"result"`
		}
		if json.Unmarshal([]byte(raw), &call) != nil || call.Name == "" {
			continue
		}
		args := fmt.Sprintf("%v", call.Arguments)
		if len(args) > maxArguments {
			args = args[:maxArguments] + "..."
		}
		result := call.Result
		if len(result) > maxResult {
			result = result[:maxResult] + "..."
		}
		lines = append(lines, fmt.Sprintf("- %s(%s) -> %s\n", call.Name, args, result))
	}
	if len(lines) == maxActions {
		lines = append([]string{"... (earlier calls truncated) ...\n"}, lines...)
	}
	return strings.Join(lines, "")
}

func hasStopMarker(message string, markers []string) bool {
	for _, marker := range markers {
		if marker != "" && strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

type userSimMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type userSimChatRequest struct {
	Model       string           `json:"model"`
	Messages    []userSimMessage `json:"messages"`
	Temperature float64          `json:"temperature"`
}

type userSimChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// userSimulatorReply calls the OpenAI-compatible endpoint with the official
// role convention: the simulator plays assistant, so agent messages arrive
// as user-role and the simulator's own turns as assistant-role. Temperature
// is pinned to 0: the simulator must stick to its scenario's known_info
// (identity flips corrupt authentication flows) rather than improvise.
func userSimulatorReply(ctx context.Context, config armConfig, sim *userSimConfig, transcript []transcriptEntry) (string, error) {
	messages := []userSimMessage{{Role: "system", Content: sim.SystemPrompt}}
	for _, entry := range transcript {
		if entry.Role == "agent" {
			messages = append(messages, userSimMessage{Role: "user", Content: entry.Content})
		} else {
			messages = append(messages, userSimMessage{Role: "assistant", Content: entry.Content})
		}
	}
	payload, err := json.Marshal(userSimChatRequest{Model: config.userSimModel, Messages: messages, Temperature: 0})
	if err != nil {
		return "", err
	}
	callCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(callCtx, http.MethodPost, strings.TrimSuffix(config.userSimURL, "/")+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var parsed userSimChatResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("user simulator response: %w: %.200s", err, string(body))
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("user simulator error: %s", parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("user simulator returned no choices: %.200s", string(body))
	}
	return parsed.Choices[0].Message.Content, nil
}

type skillExposure struct {
	Source       string                                   `json:"source"`
	Stage        string                                   `json:"stage"`
	CandidateRef *memoryclient.SkillEvolutionCandidateRef `json:"candidate_ref,omitempty"`
	LocalSHA256  string                                   `json:"local_sha256,omitempty"`
}

type c3ArmSummary struct {
	SchemaVersion      string                                        `json:"schema_version"`
	Arm                string                                        `json:"arm"`
	AdvisoryReadOK     int                                           `json:"advisory_read_ok"`
	AdvisoryReadFailed int                                           `json:"advisory_read_failed"`
	AdvisoryCandidates int                                           `json:"advisory_candidates"`
	ExposureSources    map[string]int                                `json:"exposure_sources"`
	CandidateRefs      []memoryclient.SkillEvolutionCandidateRef     `json:"candidate_refs"`
	LifecycleStatus    string                                        `json:"lifecycle_evidence_status"`
	LifecycleOutcomes  []memoryclient.SkillEvolutionCandidateOutcome `json:"lifecycle_outcomes,omitempty"`
}

func (s *c3ArmSummary) observe(record attemptRecord) {
	if record.SkillAdvisoryReadStatus == "ok" {
		s.AdvisoryReadOK++
	}
	if record.SkillAdvisoryReadStatus == "unavailable" {
		s.AdvisoryReadFailed++
	}
	s.AdvisoryCandidates += record.SkillRetrievalAdvisoryCount
	for _, exposure := range record.SkillExposure {
		s.ExposureSources[exposure.Source]++
		if exposure.CandidateRef != nil {
			s.addCandidateRef(*exposure.CandidateRef)
		}
	}
}
func (s *c3ArmSummary) addCandidateRef(ref memoryclient.SkillEvolutionCandidateRef) {
	for _, existing := range s.CandidateRefs {
		if existing.CandidateID == ref.CandidateID && existing.BodyDigest == ref.BodyDigest {
			return
		}
	}
	s.CandidateRefs = append(s.CandidateRefs, ref)
}
func writeC3ArmSummary(config armConfig, reporter *armBReporter, summary *c3ArmSummary) {
	if reporter != nil {
		lifecycle := reporter.c3LifecycleSummary(context.Background(), summary.CandidateRefs)
		summary.LifecycleStatus, summary.LifecycleOutcomes = lifecycle.Status, lifecycle.Outcomes
	} else {
		summary.LifecycleStatus = "not_configured"
	}
	body, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		fmt.Printf("[%s] C3 arm summary warning: %v\n", config.arm, err)
		return
	}
	path := filepath.Join(config.outDir, "work-"+config.arm, "arm-summary.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fmt.Printf("[%s] C3 arm summary warning: %v\n", config.arm, err)
		return
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		fmt.Printf("[%s] C3 arm summary warning: %v\n", config.arm, err)
	}
}
