// Command past-runner executes PAST-Bench episodes (channel-adapted manifest
// from memory_bench's past_official converter) against this Host plus a real
// graph-memory-service. Warm arm: every episode of a family in official order,
// one room and one shared/private space pair per family, evidence drained
// after each episode so later episodes recall earlier ones. Cold arm: only the
// manifest's test-split episodes in a fresh tenant, so no learning episode
// ever lands in memory. Each episode is one fresh Pi process — per-episode
// context is exactly the recalled evidence plus the episode prompt.
//
// Output is one attempts.jsonl whose records are field-compatible with the
// memory_bench grading bridge (benchmark/task_id/episode_id/arm/status/
// final_output), plus per-episode mechanism evidence (recall state and
// citation count).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
	"river2.dev/pi-group-chat-host/internal/pi/roombridge"
	"river2.dev/pi-group-chat-host/internal/ports"
	"river2.dev/pi-group-chat-host/internal/runtime"
)

type manifestEpisode struct {
	TaskID    string `json:"task_id"`
	EpisodeID string `json:"episode_id"`
	FamilyID  string `json:"family_id"`
	Role      string `json:"role"`
	Split     string `json:"split"`
	Order     int    `json:"order"`
	Turns     []struct {
		Prompt string `json:"prompt"`
	} `json:"turns"`
}

type attemptRecord struct {
	RunID           string  `json:"run_id"`
	EvaluationID    string  `json:"evaluation_id"`
	Benchmark       string  `json:"benchmark"`
	TaskID          string  `json:"task_id"`
	EpisodeID       string  `json:"episode_id"`
	FamilyID        string  `json:"family_id"`
	Role            string  `json:"role"`
	Arm             string  `json:"arm"`
	MemoryPolicy    string  `json:"memory_policy"`
	Seed            int     `json:"seed"`
	Attempt         int     `json:"attempt"`
	Status          string  `json:"status"`
	FailureKind     string  `json:"failure_kind,omitempty"`
	Error           string  `json:"error,omitempty"`
	DurationSeconds float64 `json:"duration_seconds"`
	RecallState     string  `json:"recall_state"`
	RecallCitations int     `json:"recall_citations"`
	FinalOutput     *string `json:"final_output"`
	FormatReminder  bool    `json:"format_reminder,omitempty"`
}

func main() {
	manifestPath := flag.String("manifest", "", "PAST manifest JSONL from memory_bench past_official (required)")
	gmsBinary := flag.String("gms-binary", "", "graph-memory-server binary; the runner spawns one single-tenant instance per arm (required for multi-arm runs)")
	gmsURL := flag.String("gms-url", "", "pre-existing graph-memory-service base URL (single-arm runs only)")
	gmsToken := flag.String("gms-token", "", "token for the pre-existing graph-memory-service")
	piBinary := flag.String("pi-binary", "", "Pi RPC binary path, version 0.84.3 (required)")
	provider := flag.String("provider", "", "Pi provider name (required)")
	model := flag.String("model", "", "Pi model id (required)")
	arms := flag.String("arms", "warm,cold", "comma-separated arms to run")
	families := flag.String("families", "", "comma-separated family filter (default: every family in the manifest)")
	evaluationID := flag.String("evaluation-id", "", "evaluation id used in run ids and tenant ids (required)")
	seed := flag.Int("seed", 0, "schedule seed recorded in attempts")
	outDir := flag.String("out-dir", "", "output directory for attempts.jsonl (required)")
	episodeTimeout := flag.Duration("episode-timeout", 6*time.Minute, "per-episode timeout")
	piEnv := flag.String("pi-env", "HOME,PATH", "comma-separated environment allowlist passed to the Pi process")
	formatReminder := flag.String("format-reminder", "", "channel-adaptation note appended to every episode's room input, identical for both arms (e.g. prefer tab-separated rows over markdown tables); empty = off")
	flag.Parse()
	if *manifestPath == "" || *piBinary == "" || *provider == "" || *model == "" || *evaluationID == "" || *outDir == "" {
		flag.Usage()
		os.Exit(2)
	}
	armList := splitList(*arms)
	if *gmsBinary == "" {
		if *gmsURL == "" || *gmsToken == "" || len(armList) > 1 {
			fmt.Fprintln(os.Stderr, "past-runner: multi-arm runs require --gms-binary (graph-memory-service is single-tenant); --gms-url/--gms-token serve single-arm runs against a pre-existing instance")
			os.Exit(2)
		}
	}

	episodes, err := loadManifest(*manifestPath)
	if err != nil {
		fatal("load manifest: %v", err)
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
	attemptsPath := filepath.Join(*outDir, "attempts.jsonl")
	attemptsFile, err := os.Create(attemptsPath)
	if err != nil {
		fatal("create attempts file: %v", err)
	}
	defer attemptsFile.Close()
	writer := bufio.NewWriter(attemptsFile)
	defer writer.Flush()

	emit := func(record attemptRecord) {
		line, _ := json.Marshal(record)
		fmt.Fprintln(writer, string(line))
		writer.Flush()
		status := record.Status
		if record.Error != "" {
			status += " (" + record.Error + ")"
		}
		fmt.Printf("[%s] %s/%s recall=%s(%d) %s\n", record.Arm, record.FamilyID, record.EpisodeID, record.RecallState, record.RecallCitations, status)
	}
	for _, arm := range armList {
		if arm != "warm" && arm != "cold" {
			fatal("unknown arm %q: only warm and cold exist", arm)
		}
		policy := "read_write"
		if arm == "cold" {
			policy = "no_learn_memory_present"
		}
		config := armConfig{
			arm: arm, episodes: episodes, evaluationID: *evaluationID, seed: *seed,
			gmsURL: *gmsURL, gmsToken: *gmsToken, piBinary: *piBinary,
			provider: *provider, model: *model, policy: policy,
			episodeTimeout: *episodeTimeout, piEnv: splitList(*piEnv),
			formatReminder: *formatReminder,
		}
		if *gmsBinary != "" {
			instance, err := startGMS(*gmsBinary, filepath.Join(*outDir, "gms-"+arm))
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
		if err := runArm(context.Background(), config, emit); err != nil {
			fatal("arm %s: %v", arm, err)
		}
	}
	fmt.Printf("attempts written to %s\n", attemptsPath)
}

type armConfig struct {
	arm            string
	episodes       []manifestEpisode
	evaluationID   string
	seed           int
	gmsURL         string
	gmsToken       string
	piBinary       string
	provider       string
	model          string
	policy         string
	formatReminder string
	episodeTimeout time.Duration
	piEnv          []string
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

func startGMS(binary, stateDir string) (*gmsInstance, error) {
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
	token := "past-eval-" + sanitizeID(filepath.Base(stateDir)) + "-token"
	statePath := filepath.Join(stateDir, "state.json")
	logFile, err := os.Create(filepath.Join(stateDir, "gms.log"))
	if err != nil {
		return nil, fmt.Errorf("create gms log: %w", err)
	}
	process, err := os.StartProcess(binary, []string{binary,
		"-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"-token", token,
		"-state", statePath,
	}, &os.ProcAttr{Files: []*os.File{os.Stdin, logFile, logFile}})
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
	tenantID := sanitizeID(config.evaluationID) + "-" + config.arm + "-t"
	client := memoryclient.NewClient(config.gmsURL, config.gmsToken, &http.Client{Timeout: 10 * time.Second}, 1<<20)

	if _, err := client.InitializeTenant(ctx, ports.InitializeTenantRequest{
		TenantID: tenantID, DisplayName: "PAST evaluation tenant (" + config.arm + ")", BootstrapPrincipalID: "host-service",
	}); err != nil {
		return fmt.Errorf("InitializeTenant: %w", err)
	}
	// The bootstrap principal comes into existence with the tenant; registering
	// it again with different content is a correct 409. Only the task agent is
	// a separate registration.
	if _, err := client.RegisterPrincipal(ctx, ports.RegisterPrincipalRequest{
		PrincipalID: "agent-primary", Kind: "agent", DisplayName: "PAST task agent",
	}); err != nil {
		return fmt.Errorf("RegisterPrincipal agent-primary: %w", err)
	}

	familyOrder := familySequence(config.episodes)
	var grants []string
	for _, family := range familyOrder {
		shared := "space-" + sanitizeID(family) + "-shared"
		private := "space-" + sanitizeID(family) + "-private"
		owner := "agent-primary"
		for _, request := range []ports.RegisterSpaceRequest{
			{SpaceID: shared, Scope: "shared", DisplayName: family + " shared memory"},
			{SpaceID: private, Scope: "private", OwnerPrincipalID: &owner, DisplayName: family + " agent private memory"},
		} {
			if _, err := client.RegisterSpace(ctx, request); err != nil {
				return fmt.Errorf("RegisterSpace %s: %w", request.SpaceID, err)
			}
		}
		grants = append(grants, shared, private)
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

	workDir, err := os.MkdirTemp("", "past-runner-"+config.arm+"-")
	if err != nil {
		return fmt.Errorf("work dir: %w", err)
	}
	defer os.RemoveAll(workDir)
	extensionPath, err := roombridge.WriteTo(filepath.Join(workDir, "extension"))
	if err != nil {
		return fmt.Errorf("write room bridge extension: %w", err)
	}
	session := runtime.NewSession()

	for _, family := range familyOrder {
		roomID := "room-" + sanitizeID(family)
		for _, episode := range episodesOfFamily(config.episodes, family) {
			if config.arm == "cold" && episode.Split != "test" {
				continue
			}
			record := attemptRecord{
				RunID:        fmt.Sprintf("%s:%s:%d:%s", config.evaluationID, episode.TaskID, config.seed, config.arm),
				EvaluationID: config.evaluationID,
				Benchmark:    "past_bench",
				TaskID:       episode.TaskID,
				EpisodeID:    episode.EpisodeID,
				FamilyID:     episode.FamilyID,
				Role:         episode.Role,
				Arm:          config.arm,
				MemoryPolicy: config.policy,
				Seed:         config.seed,
				Attempt:      1,
			}
			started := time.Now()
			episodeCtx, cancel := context.WithTimeout(ctx, config.episodeTimeout)
			shared := "space-" + sanitizeID(family) + "-shared"
			private := "space-" + sanitizeID(family) + "-private"
			roomInput := episode.Turns[0].Prompt
			if config.formatReminder != "" {
				roomInput += "\n\n" + config.formatReminder
			}
			record.FormatReminder = config.formatReminder != ""
			turn, err := session.Turn(episodeCtx, runtime.TurnRequest{
				Authority: runtime.ExecutionAuthority{
					TenantID: tenantID, RoomID: roomID, AgentID: "agent-primary", ProfileKind: "ordinary",
					WorkingDirectory: workDir, EnvironmentAllowlist: config.piEnv,
					Provider: config.provider, Model: config.model,
					SharedSpaceID: shared, PrivateSpaceID: private,
				},
				RoomInput:       roomInput,
				HumanMessageID:  "human-" + sanitizeID(episode.EpisodeID),
				MemoryBaseURL:   config.gmsURL,
				MemoryAuthToken: config.gmsToken,
				PiBinary:        config.piBinary,
				PiExtensionPath: extensionPath,
				PromptRequestID: "prompt-" + sanitizeID(config.evaluationID) + "-" + config.arm + "-" + sanitizeID(episode.EpisodeID),
			})
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
			if output := finalAgentOutput(turn.Messages, "agent-primary"); output != nil {
				record.FinalOutput = output
			}
			// The recall-visibility boundary: evidence of this episode becomes
			// recallable for the next episode only after the drain commits.
			drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
			_, drainErr := session.DrainEvidence(drainCtx, runtime.DrainRequest{
				RoomID: roomID, MemoryBaseURL: config.gmsURL, MemoryAuthToken: config.gmsToken,
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
			cancel()
			emit(record)
		}
	}
	return nil
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
	sort.Slice(matched, func(i, j int) bool { return matched[i].Order < matched[j].Order })
	return matched
}

func loadManifest(path string) ([]manifestEpisode, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var episodes []manifestEpisode
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
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
		if episode.TaskID == "" || episode.EpisodeID == "" || episode.FamilyID == "" || len(episode.Turns) == 0 || episode.Turns[0].Prompt == "" {
			return nil, fmt.Errorf("line %d: missing task_id/episode_id/family_id/turns", lineNumber)
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
	fmt.Fprintf(os.Stderr, "past-runner: "+format+"\n", args...)
	os.Exit(1)
}
