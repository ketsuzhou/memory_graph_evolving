package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"river2.dev/graph-memory-service/internal/embedding"
	"river2.dev/graph-memory-service/internal/navigation"
	"river2.dev/graph-memory-service/internal/pathselector"
	"river2.dev/graph-memory-service/internal/retrieval"
)

type serverConfig struct {
	addr      string
	token     string
	state     string
	accessLog string

	retrievalMode       retrieval.EmbeddingMode
	embeddingBaseURL    string
	embeddingModel      string
	embeddingAPIKeyEnv  string
	embeddingTimeout    time.Duration
	embeddingPolicy     string
	bm25Weight          float64
	navigationMode      navigation.Mode
	navigationBaseURL   string
	navigationModel     string
	navigationAPIKeyEnv string
	navigationTimeout   time.Duration
	navigationPolicy    string
	navigationTokenCap  int

	builderEnabled        bool
	builderPoll           time.Duration
	builderBatchThreshold int64
	builderQueryThreshold int64

	// PG-50A consolidation-cut composition wiring.
	cutRoomSpaces  []string
	cutEventSecret string

	// conformanceDir pins the shared FND-001 corpus so graph-batch can start
	// when the process cwd is outside the repository tree.
	conformanceDir string
}

type roomSpaceBindingsValue struct {
	values []string
}

func (v *roomSpaceBindingsValue) String() string { return strings.Join(v.values, ",") }

func (v *roomSpaceBindingsValue) Set(raw string) error {
	v.values = append(v.values, raw)
	return nil
}

func parseServerConfig(args []string, getenv func(string) string) (serverConfig, error) {
	var config serverConfig
	var retrievalMode string
	var navigationMode string
	var roomSpaceBindings roomSpaceBindingsValue

	embeddingTimeout, err := envDuration(getenv, "GRAPH_MEMORY_EMBEDDING_TIMEOUT", 30*time.Second)
	if err != nil {
		return config, err
	}
	navigationTimeout, err := envDuration(getenv, "GRAPH_MEMORY_NAVIGATION_TIMEOUT", 10*time.Second)
	if err != nil {
		return config, err
	}
	bm25Weight, err := envFloat(getenv, "GRAPH_MEMORY_BM25_WEIGHT", 0.5)
	if err != nil {
		return config, err
	}
	navigationTokenCap, err := envInt(getenv, "GRAPH_MEMORY_NAVIGATION_TOKEN_CAP", 512)
	if err != nil {
		return config, err
	}
	builderEnabled, err := envBool(getenv, "GRAPH_MEMORY_BUILDER_ENABLED", false)
	if err != nil {
		return config, err
	}
	builderPoll, err := envDuration(getenv, "GRAPH_MEMORY_BUILDER_POLL", time.Minute)
	if err != nil {
		return config, err
	}
	builderBatchThreshold, err := envInt64(getenv, "GRAPH_MEMORY_BUILDER_BATCH_THRESHOLD", 50)
	if err != nil {
		return config, err
	}
	builderQueryThreshold, err := envInt64(getenv, "GRAPH_MEMORY_BUILDER_QUERY_THRESHOLD", 200)
	if err != nil {
		return config, err
	}

	flags := flag.NewFlagSet("graph-memory-service", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&config.addr, "addr", envOr(getenv, "GRAPH_MEMORY_ADDR", "127.0.0.1:18080"), "listen address")
	flags.StringVar(&config.token, "token", getenv("GRAPH_MEMORY_AUTH_TOKEN"), "deployment bearer token (required; defaults from GRAPH_MEMORY_AUTH_TOKEN)")
	flags.StringVar(&config.state, "state", getenv("GRAPH_MEMORY_STATE"), "JSON snapshot file (empty = memory only)")
	flags.StringVar(&config.accessLog, "access-log", getenv("GRAPH_MEMORY_ACCESS_LOG"), "JSONL log destination (empty = stderr)")

	flags.StringVar(&retrievalMode, "retrieval-mode", envOr(getenv, "GRAPH_MEMORY_RETRIEVAL_MODE", "off"), "retrieval embedding mode: off, optional, or required")
	flags.StringVar(&config.embeddingBaseURL, "embedding-base-url", getenv("GRAPH_MEMORY_EMBEDDING_BASE_URL"), "OpenAI-compatible embedding base URL")
	flags.StringVar(&config.embeddingModel, "embedding-model", getenv("GRAPH_MEMORY_EMBEDDING_MODEL"), "OpenAI-compatible embedding model")
	flags.StringVar(&config.embeddingAPIKeyEnv, "embedding-api-key-env", envOr(getenv, "GRAPH_MEMORY_EMBEDDING_API_KEY_ENV", "GRAPH_MEMORY_EMBEDDING_API_KEY"), "environment variable containing the embedding API key")
	flags.DurationVar(&config.embeddingTimeout, "embedding-timeout", embeddingTimeout, "embedding HTTP timeout")
	flags.StringVar(&config.embeddingPolicy, "embedding-policy", envOr(getenv, "GRAPH_MEMORY_EMBEDDING_POLICY", embedding.PolicyOpenAICompatibleV1), "embedding request policy")
	flags.Float64Var(&config.bm25Weight, "bm25-weight", bm25Weight, "BM25 share of a successfully applied hybrid score, from 0 to 1")

	flags.StringVar(&navigationMode, "navigation-mode", envOr(getenv, "GRAPH_MEMORY_NAVIGATION_MODE", "off"), "navigation mode: off, optional, or required")
	flags.StringVar(&config.navigationBaseURL, "navigation-base-url", getenv("GRAPH_MEMORY_NAVIGATION_BASE_URL"), "OpenAI-compatible navigation base URL")
	flags.StringVar(&config.navigationModel, "navigation-model", getenv("GRAPH_MEMORY_NAVIGATION_MODEL"), "OpenAI-compatible navigation model")
	flags.StringVar(&config.navigationAPIKeyEnv, "navigation-api-key-env", envOr(getenv, "GRAPH_MEMORY_NAVIGATION_API_KEY_ENV", "GRAPH_MEMORY_PATH_SELECTOR_API_KEY"), "environment variable containing the navigation API key")
	flags.DurationVar(&config.navigationTimeout, "navigation-timeout", navigationTimeout, "per-call navigation provider timeout")
	flags.StringVar(&config.navigationPolicy, "navigation-prompt-policy", envOr(getenv, "GRAPH_MEMORY_NAVIGATION_PROMPT_POLICY", pathselector.PromptPolicyV1), "navigation prompt policy")
	flags.IntVar(&config.navigationTokenCap, "navigation-token-cap", navigationTokenCap, "maximum provider completion tokens per navigation call")

	flags.BoolVar(&config.builderEnabled, "builder-enabled", builderEnabled, "enable automatic structural projection building")
	flags.DurationVar(&config.builderPoll, "builder-poll", builderPoll, "projection builder poll interval")
	flags.Int64Var(&config.builderBatchThreshold, "builder-batch-threshold", builderBatchThreshold, "committed batches required to trigger a projection build")
	flags.Int64Var(&config.builderQueryThreshold, "builder-query-threshold", builderQueryThreshold, "successful recalls required to trigger a projection build")
	// PG-50A consolidation-cut composition: repeatable room→space binding
	// (e.g. -cut-room-space room-a=space1,space2) and the signed-event secret.
	flags.Var(&roomSpaceBindings, "cut-room-space", "room=space1,space2 deployment binding (repeatable)")
	flags.StringVar(&config.cutEventSecret, "cut-event-secret", getenv("GRAPH_MEMORY_CUT_EVENT_SECRET"), "HMAC secret for signed cut webhook notifications")
	flags.StringVar(&config.conformanceDir, "conformance-dir", firstNonEmptyEnv(getenv, "GRAPH_MEMORY_CONFORMANCE_DIR", "RSIH_CONFORMANCE_DIR"), "shared FND-001 conformance corpus (graph-batch policy/schema)")
	config.cutEventSecret = strings.TrimSpace(config.cutEventSecret)
	if err := flags.Parse(args); err != nil {
		return config, fmt.Errorf("server configuration: %w", err)
	}
	if flags.NArg() != 0 {
		return config, fmt.Errorf("server configuration: unexpected positional arguments")
	}
	config.cutRoomSpaces = roomSpaceBindings.values
	config.conformanceDir = strings.TrimSpace(config.conformanceDir)

	config.token = strings.TrimSpace(config.token)
	config.embeddingBaseURL = strings.TrimSpace(config.embeddingBaseURL)
	config.embeddingModel = strings.TrimSpace(config.embeddingModel)
	config.embeddingAPIKeyEnv = strings.TrimSpace(config.embeddingAPIKeyEnv)
	config.embeddingPolicy = strings.TrimSpace(config.embeddingPolicy)
	config.navigationBaseURL = strings.TrimSpace(config.navigationBaseURL)
	config.navigationModel = strings.TrimSpace(config.navigationModel)
	config.navigationAPIKeyEnv = strings.TrimSpace(config.navigationAPIKeyEnv)
	config.navigationPolicy = strings.TrimSpace(config.navigationPolicy)
	if config.token == "" {
		return config, fmt.Errorf("server configuration: -token or GRAPH_MEMORY_AUTH_TOKEN is required")
	}
	config.retrievalMode, err = retrieval.ParseEmbeddingMode(retrievalMode)
	if err != nil {
		return config, err
	}
	config.navigationMode, err = navigation.ParseMode(strings.ToLower(strings.TrimSpace(navigationMode)))
	if err != nil {
		return config, err
	}
	if math.IsNaN(config.bm25Weight) || math.IsInf(config.bm25Weight, 0) || config.bm25Weight < 0 || config.bm25Weight > 1 {
		return config, fmt.Errorf("server configuration: BM25 weight must be finite and between 0 and 1")
	}
	if config.embeddingTimeout <= 0 || config.embeddingTimeout > 2*time.Minute {
		return config, fmt.Errorf("server configuration: embedding timeout must be within two minutes")
	}
	if config.navigationTimeout <= 0 || config.navigationTimeout > 30*time.Second {
		return config, fmt.Errorf("server configuration: navigation timeout must be within 30 seconds")
	}
	if config.embeddingPolicy != embedding.PolicyOpenAICompatibleV1 {
		return config, fmt.Errorf("server configuration: unsupported embedding policy %q", config.embeddingPolicy)
	}
	if config.navigationPolicy != pathselector.PromptPolicyV1 {
		return config, fmt.Errorf("server configuration: unsupported navigation prompt policy %q", config.navigationPolicy)
	}
	if config.navigationTokenCap < 1 || config.navigationTokenCap > 4096 {
		return config, fmt.Errorf("server configuration: navigation token cap must be 1-4096")
	}
	if !validEnvironmentName(config.embeddingAPIKeyEnv) || !validEnvironmentName(config.navigationAPIKeyEnv) {
		return config, fmt.Errorf("server configuration: provider API key environment names must be valid")
	}
	if config.builderPoll <= 0 || config.builderBatchThreshold <= 0 || config.builderQueryThreshold <= 0 {
		return config, fmt.Errorf("server configuration: builder poll interval and thresholds must be positive")
	}
	if err := validateProviderConfig("embedding", config.retrievalMode != retrieval.EmbeddingOff, config.retrievalMode == retrieval.EmbeddingRequired, config.embeddingBaseURL, config.embeddingModel, getenv(config.embeddingAPIKeyEnv)); err != nil {
		return config, err
	}
	if err := validateProviderConfig("navigation", config.navigationMode != navigation.ModeOff, config.navigationMode == navigation.ModeRequired, config.navigationBaseURL, config.navigationModel, getenv(config.navigationAPIKeyEnv)); err != nil {
		return config, err
	}
	return config, nil
}

func validateProviderConfig(name string, enabled, required bool, baseURL, model, apiKey string) error {
	configured := baseURL != "" || model != ""
	if enabled && configured && (baseURL == "" || model == "") {
		return fmt.Errorf("server configuration: %s base URL and model must be configured together", name)
	}
	if required && (baseURL == "" || model == "" || apiKey == "") {
		return fmt.Errorf("server configuration: required %s provider needs base URL, model, and a non-empty API key environment value", name)
	}
	return nil
}

func validEnvironmentName(value string) bool {
	if value == "" {
		return false
	}
	for index, r := range value {
		if (index == 0 && r != '_') || (index > 0 && r != '_') {
			if !unicode.IsLetter(r) && (index == 0 || !unicode.IsDigit(r)) {
				return false
			}
		}
	}
	return true
}

func envOr(getenv func(string) string, name, fallback string) string {
	if value := strings.TrimSpace(getenv(name)); value != "" {
		return value
	}
	return fallback
}

func firstNonEmptyEnv(getenv func(string) string, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func envDuration(getenv func(string) string, name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("server configuration: %s must be a duration: %w", name, err)
	}
	return parsed, nil
}

func envFloat(getenv func(string) string, name string, fallback float64) (float64, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("server configuration: %s must be a number: %w", name, err)
	}
	return parsed, nil
}

func envInt(getenv func(string) string, name string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("server configuration: %s must be an integer: %w", name, err)
	}
	return parsed, nil
}

func envInt64(getenv func(string) string, name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("server configuration: %s must be an integer: %w", name, err)
	}
	return parsed, nil
}

func envBool(getenv func(string) string, name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("server configuration: %s must be a boolean: %w", name, err)
	}
	return parsed, nil
}
