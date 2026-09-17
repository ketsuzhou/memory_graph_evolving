// Command server runs the graph-memory-service HTTP transport as a real
// process. Tenant bootstrap binds the deployment token; see ADR 0040 for the
// single-instance, static-token tracer-bullet deployment posture. With -state
// set, the whole store is reloaded from that JSON snapshot at boot and
// persisted atomically after every mutating request. Every request and every
// state persistence emits one JSON line on the -access-log sink (stderr by
// default), giving post-run analysis request timing, recall shapes, evidence
// batch shapes, and error codes without any content payloads.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"river2.dev/graph-memory-service/internal/consolidation"
	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/dive"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/embedding"
	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/httpapi"
	"river2.dev/graph-memory-service/internal/navigation"
	"river2.dev/graph-memory-service/internal/pathselector"
	"river2.dev/graph-memory-service/internal/ports"
	"river2.dev/graph-memory-service/internal/projectionbuilder"
	"river2.dev/graph-memory-service/internal/recall"
	"river2.dev/graph-memory-service/internal/retrieval"
	"river2.dev/graph-memory-service/internal/skillevolution/activation"
	"river2.dev/graph-memory-service/internal/skillevolution/armc"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/compositehandoff"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/materializationread"
	"river2.dev/graph-memory-service/internal/skillevolution/policyactivation"
	"river2.dev/graph-memory-service/internal/skillevolution/probation"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	skillretrieval "river2.dev/graph-memory-service/internal/skillevolution/retrieval"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
	"river2.dev/graph-memory-service/internal/skillproposal"
	"river2.dev/graph-memory-service/internal/store/memory"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

func main() {
	config, err := parseServerConfig(os.Args[1:], os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	var logSink io.Writer = os.Stderr
	if config.accessLog != "" {
		file, err := os.OpenFile(config.accessLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Fatalf("server: open access log %s: %v", config.accessLog, err)
		}
		defer file.Close()
		logSink = file
	}
	eventLog := httpapi.NewJSONLogger(logSink)

	clock := systemClock{}
	store := memory.New()
	var statePersistMu sync.Mutex
	var provider ports.EmbeddingProvider
	if config.retrievalMode != retrieval.EmbeddingOff && config.embeddingBaseURL != "" {
		provider, err = embedding.NewOpenAICompatible(
			&http.Client{Timeout: config.embeddingTimeout},
			config.embeddingBaseURL,
			os.Getenv(config.embeddingAPIKeyEnv),
			config.embeddingModel,
			config.embeddingPolicy,
		)
		if err != nil {
			log.Fatal(err)
		}
	}
	retrievalEngine, err := retrieval.New(store, provider, retrieval.Config{
		EmbeddingMode: config.retrievalMode,
		BM25Weight:    config.bm25Weight,
	}, store)
	if err != nil {
		log.Fatal(err)
	}
	var selector ports.PathSelector
	if config.navigationMode != navigation.ModeOff && config.navigationBaseURL != "" {
		selector, err = pathselector.NewOpenAICompatible(
			&http.Client{Timeout: config.navigationTimeout},
			config.navigationBaseURL,
			os.Getenv(config.navigationAPIKeyEnv),
			config.navigationModel,
			config.navigationPolicy,
			config.navigationTokenCap,
			config.navigationTimeout,
		)
		if err != nil {
			log.Fatal(err)
		}
	}
	explorationService := exploration.New(store, retrievalEngine, store, clock)
	navigationService, err := navigation.New(selector, store, explorationService, navigation.Config{
		Mode: config.navigationMode, MaxModelCalls: 8, MaxDuration: 30 * time.Second,
		MaxTotalTokens: 8192, MaxCandidates: 24, MaxCandidateBytes: 2048, ExploreLimit: 5,
		MaxRecoveryDuration: 2 * time.Second,
		Checkpoint: func() error {
			if config.state == "" {
				return nil
			}
			return persistState(config.state, store, &statePersistMu)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	// PG-50A consolidation-cut composition: the production freezer (deployment
	// room→space bindings), the Room-scoped cut service, the durable guard
	// authority, and the signed event notifier. When no room→space bindings
	// are configured the cut service is still wired (so GET/404 routing stays
	// consistent) but freezes fail closed on the empty binding set.
	var cutFreezer *httpapi.RoomFreezer
	var cutService *consolidationcut.Service
	var cutEvents *httpapi.CutEventNotifier
	cutStateFile := ""
	cutRoomSpaces, roomErr := parseCutRoomSpaces(config.cutRoomSpaces)
	if roomErr != nil {
		log.Fatal(roomErr)
	}
	cutFreezer = httpapi.NewRoomFreezer(store, store, cutRoomSpaces)
	cutService = consolidationcut.NewService(nil, cutFreezer)
	cutService.SetGuardAuthority(httpapi.NewDurableGuardAuthority(store, cutService.GetManifest, cutRoomSpaces))
	if config.state != "" {
		cutStateFile = config.state + ".cuts.json"
		cutEvents = httpapi.NewCutEventNotifierWithLogger(config.cutEventSecret, httpapi.NewFileCutEventSink(config.state+".cut-events.jsonl"), eventLog)
	} else {
		cutEvents = httpapi.NewCutEventNotifierWithLogger("", nil, eventLog)
	}

	candidateStore := memory.CandidateStore{Store: store}
	curationService := skillproposal.New(store, store, store, candidateStore, clock, candidateStore)
	handler := httpapi.NewHandler(httpapi.Dependencies{
		Token:             config.token,
		Registry:          store,
		Evidence:          evidence.New(store, store, clock),
		Recall:            recall.New(store, retrievalEngine, clock),
		Exploration:       explorationService,
		Navigation:        navigationService,
		DiveStore:         store,
		DiveJudge:         dive.New(store, dive.NewDeterministicScorer(), clock),
		Patterns:          store,
		Proposals:         store,
		Candidates:        candidateStore,
		Curation:          curationService,
		Clock:             clock,
		ConsolidationCuts: cutService,
		CutFreezer:        cutFreezer,
		CutEvents:         cutEvents,
	})

	bootFields := map[string]any{
		"addr": config.addr, "state_path": config.state,
		"retrieval_mode": string(config.retrievalMode), "navigation_mode": string(config.navigationMode),
		"builder_enabled": config.builderEnabled,
	}
	if config.state != "" {
		loadStarted := time.Now()
		if err := store.LoadFromFile(config.state); err != nil {
			log.Fatalf("server: load state %s: %v", config.state, err)
		}
		// PG-50A: restore the cut composition (jobs + freezer threshold
		// ledger) from <state>.cuts.json on boot so a restart resumes both
		// the authoritative GET state and threshold diffs.
		if err := loadCutState(cutStateFile, cutService, cutFreezer); err != nil {
			log.Fatal(err)
		}
		if info, statErr := os.Stat(cutStateFile); statErr == nil {
			bootFields["cut_state_bytes"] = info.Size()
		}
		if tenant, principal, ok := store.Binding(); ok {
			if bound, ok := handler.(*httpapi.Handler); ok {
				bound.RestoreBinding(tenant, principal)
			}
			log.Printf("server: restored tenant %s from %s", tenant, config.state)
			bootFields["restored_tenant"] = string(tenant)
			bootFields["restored_principal"] = string(principal)
		}
		if info, err := os.Stat(config.state); err == nil {
			bootFields["state_bytes"] = info.Size()
		}
		bootFields["load_ms"] = float64(time.Since(loadStarted).Microseconds()) / 1000
		// The request logger wraps the persistence wrapper so a line's
		// duration_ms is the full server-side cost the caller observed,
		// while the separate persist_state line breaks out the snapshot write.
		handler = persistAfterMutation(config.state, store, handler, eventLog, &statePersistMu)
		// PG-50A: after a cut mutation persist the cut composition too, so a
		// restart resumes the authoritative job state (acceptance item 5).
		handler = persistCutAfterMutation(cutStateFile, cutService, cutFreezer, handler, eventLog, &statePersistMu)
	}
	// GMS-205 Runtime projector (env-gated; the skill-evolution ledger
	// stack is its own authority, separate from the legacy wire-model
	// store above).
	var skillEvolution *projectorWorker
	skillEvolutionEnabled, skillEvolutionPoll, err := skillEvolutionConfig()
	if err != nil {
		log.Fatal(err)
	}
	if skillEvolutionEnabled {
		skillEvolution, err = newProjectorWorker(eventLog, skillEvolutionPoll, config.token, config.state, candidateStore)
		if err != nil {
			log.Fatal(err)
		}
		if config.state != "" {
			usageStatePath := config.state + ".usage-projection.json"
			if err := skillEvolution.usageStore.LoadFromFile(usageStatePath); err != nil {
				log.Fatalf("server: load usage projection state %s: %v", usageStatePath, err)
			}
			skillEvolution.usageStore.SetCheckpoint(func([]byte) error {
				statePersistMu.Lock()
				defer statePersistMu.Unlock()
				return skillEvolution.usageStore.PersistToFile(usageStatePath)
			})
			bootFields["usage_projection_state_path"] = usageStatePath
		}
		bootFields["skill_evolution_enabled"] = true
		bootFields["skill_evolution_poll_seconds"] = skillEvolutionPoll.Seconds()
	}
	var projectionWorker *projectionbuilder.Worker
	if config.builderEnabled {
		consolidationConfig := consolidation.DefaultConfig()
		consolidationConfig.TriggerCommittedBatches = config.builderBatchThreshold
		consolidationConfig.TriggerQueries = config.builderQueryThreshold
		replayRunner, err := projectionbuilder.NewReplayRunner(store, store)
		if err != nil {
			log.Fatal(err)
		}
		consolidationService := consolidation.New(store, replayRunner, clock, consolidationConfig)
		builder, err := projectionbuilder.New(store, consolidationService, projectionbuilder.Config{
			OnBuild: func(_ context.Context, space domain.Space, result domain.RoundResult, buildErr error) {
				fields := map[string]any{
					"tenant_id": string(space.TenantID), "space_id": string(space.ID),
					"round_id": string(result.RoundID), "outcome": string(result.Outcome),
				}
				if buildErr != nil {
					fields["error"] = buildErr.Error()
				}
				if config.state != "" {
					if persistErr := persistState(config.state, store, &statePersistMu); persistErr != nil {
						fields["persist_error"] = persistErr.Error()
					}
				}
				eventLog.Log("projection_build", fields)
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		projectionWorker, err = projectionbuilder.NewWorker(store, builder, projectionbuilder.WorkerConfig{
			PollInterval: config.builderPoll, Consolidation: consolidationConfig,
			OnError: func(err error) { eventLog.Log("projection_builder_error", map[string]any{"error": err.Error()}) },
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// GMS-207 closure read route and the GMS-206 tool routes
	// (memory_explore/memory_expand/skill_get): mounted above the legacy
	// persistence wrapper (they never mutate the legacy wire-model store)
	// and below the request logger so every skill-evolution request is
	// logged.
	if skillEvolution != nil {
		mux := http.NewServeMux()
		mux.Handle("/", handler)
		mux.Handle(httpapi.SkillEvolutionClosureReadPath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionToolExplorePath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionToolExpandPath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionToolSkillGetPath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionInteractionRecordPath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionDiagnosisRecordPath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionAdvisoryReadPath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionUsageSummaryReadPath, skillEvolution.skillEvolutionIO)
		mux.Handle(httpapi.SkillEvolutionCandidateOutcomesReadPath, skillEvolution.skillEvolutionIO)
		handler = mux
	}
	handler = httpapi.RequestLogger(eventLog)(handler)
	eventLog.Log("server_start", bootFields)

	serverContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	if projectionWorker != nil {
		if err := projectionWorker.Start(serverContext); err != nil {
			log.Fatal(err)
		}
		defer projectionWorker.Stop()
	}
	if skillEvolution != nil {
		skillEvolution.start(serverContext)
		defer skillEvolution.stop()
	}
	server := &http.Server{Addr: config.addr, Handler: handler}
	go func() {
		<-serverContext.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			log.Printf("server: graceful shutdown: %v", err)
		}
	}()

	log.Printf("graph-memory-service listening on http://%s", config.addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("server: listen: %v", err)
	}
}

// persistAfterMutation snapshots the store after every non-GET request so the
// state file tracks the last accepted mutation. A failed persist is logged and
// served: the in-memory state stays authoritative for this process.
func persistAfterMutation(path string, store *memory.Store, next http.Handler, eventLog *httpapi.JSONLogger, persistMu *sync.Mutex) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if r.Method != http.MethodGet {
			persistStarted := time.Now()
			err := persistState(path, store, persistMu)
			fields := map[string]any{
				"path":        path,
				"duration_ms": float64(time.Since(persistStarted).Microseconds()) / 1000,
			}
			if info, statErr := os.Stat(path); statErr == nil {
				fields["state_bytes"] = info.Size()
			}
			if err != nil {
				fields["error"] = err.Error()
			}
			eventLog.Log("persist_state", fields)
		}
	})
}

func persistState(path string, store *memory.Store, persistMu *sync.Mutex) error {
	persistMu.Lock()
	defer persistMu.Unlock()
	return store.PersistToFile(path)
}

// parseCutRoomSpaces converts the repeatable -cut-room-space room=space1,space2
// flag values into the deployment room→space binding map the production
// freezer consumes. A malformed binding is a fatal configuration error.
func parseCutRoomSpaces(bindings []string) (map[string][]domain.SpaceID, error) {
	result := map[string][]domain.SpaceID{}
	for _, binding := range bindings {
		parts := strings.SplitN(binding, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("server configuration: -cut-room-space must be room=space1,space2, got %q", binding)
		}
		room := strings.TrimSpace(parts[0])
		var spaces []domain.SpaceID
		for _, raw := range strings.Split(parts[1], ",") {
			space := strings.TrimSpace(raw)
			if space == "" {
				return nil, fmt.Errorf("server configuration: -cut-room-space %q has an empty space id", binding)
			}
			spaces = append(spaces, domain.SpaceID(space))
		}
		result[room] = spaces
	}
	return result, nil
}

// cutStateImage is the durable composition snapshot written to
// <state>.cuts.json: the cut service job image plus the freezer's threshold
// last-sealed ledger, so a restart resumes both jobs and threshold diffs
// (PG-50A start/stop restorability).
type cutStateImage struct {
	Cuts              []byte `json:"cuts"`
	FreezerLastSealed []byte `json:"freezer_last_sealed"`
}

// persistCutState snapshots the cut composition to <state>.cuts.json. The
// write is atomic: the image is written to a temp file in the same directory,
// fsynced, then renamed over the target. A crash mid-write can therefore never
// leave a truncated <state>.cuts.json — either the old image or the new one
// is on disk, never a half-written one.
func persistCutState(path string, service *consolidationcut.Service, freezer *httpapi.RoomFreezer, persistMu *sync.Mutex) error {
	// The freezer composition lock serializes this paired snapshot with create
	// handlers through recordSealed. It prevents a durable image containing a
	// new threshold exclusion but an older service job/idempotency set.
	freezer.LockComposition()
	defer freezer.UnlockComposition()
	cuts, err := service.Snapshot()
	if err != nil {
		return err
	}
	sealed, err := freezer.Snapshot()
	if err != nil {
		return err
	}
	image := cutStateImage{Cuts: cuts, FreezerLastSealed: sealed}
	encoded, err := json.Marshal(image)
	if err != nil {
		return err
	}
	persistMu.Lock()
	defer persistMu.Unlock()
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(encoded); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// loadCutState restores the cut composition from <state>.cuts.json on boot.
// A missing file is a fresh start; an invalid file is fatal (fail closed).
func loadCutState(path string, service *consolidationcut.Service, freezer *httpapi.RoomFreezer) error {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var image cutStateImage
	if err := json.Unmarshal(raw, &image); err != nil {
		return fmt.Errorf("server: load cut state %s: %w", path, err)
	}
	// Validate the freezer side before installing the service side. Both are
	// then restored under the same composition lock, so a malformed freezer
	// image cannot leave the paired service snapshot partially installed.
	if err := freezer.ValidateRestore(image.FreezerLastSealed); err != nil {
		return fmt.Errorf("server: validate freezer state from %s: %w", path, err)
	}
	freezer.LockComposition()
	defer freezer.UnlockComposition()
	if err := service.Restore(image.Cuts); err != nil {
		return fmt.Errorf("server: restore cut jobs from %s: %w", path, err)
	}
	if err := freezer.Restore(image.FreezerLastSealed); err != nil {
		return fmt.Errorf("server: restore freezer state from %s: %w", path, err)
	}
	return nil
}

// persistCutAfterMutation deliberately follows the server's established
// crash-recovery convention used by persistAfterMutation: the in-memory
// authority serves the completed response, then an atomic snapshot records
// the latest recoverable state. A persistence failure is logged as an
// operational error but does not rewrite an already-served response; a
// stronger write-before-response guarantee requires one server-wide commit
// protocol for the main store and the cut composition, not a cut-only local
// exception.
func persistCutAfterMutation(path string, service *consolidationcut.Service, freezer *httpapi.RoomFreezer, next http.Handler, eventLog *httpapi.JSONLogger, persistMu *sync.Mutex) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if !isCutMutation(r) {
			return
		}
		persistStarted := time.Now()
		err := persistCutState(path, service, freezer, persistMu)
		fields := map[string]any{
			"path":        path,
			"duration_ms": float64(time.Since(persistStarted).Microseconds()) / 1000,
		}
		if info, statErr := os.Stat(path); statErr == nil {
			fields["cut_state_bytes"] = info.Size()
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		eventLog.Log("persist_cut_state", fields)
	})
}

// isCutMutation reports whether a request mutates the consolidation-cut
// composition (POST create/cancel/rediagnose). Both the frozen OpenAPI slash
// action paths (/cancel, /rediagnose) and the codebase colon convention
// (:cancel, :rediagnose) mutate, so both must persist. GET is read-only and
// never triggers persistence.
func isCutMutation(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	return strings.HasSuffix(r.URL.Path, "/consolidation-cuts") ||
		strings.HasSuffix(r.URL.Path, ":cancel") ||
		strings.HasSuffix(r.URL.Path, "/cancel") ||
		strings.HasSuffix(r.URL.Path, ":rediagnose") ||
		strings.HasSuffix(r.URL.Path, "/rediagnose")
}

// ---------------------------------------------------------------------------
// GMS-205 Runtime projector wiring
// ---------------------------------------------------------------------------

// skillEvolutionConfig reads the projector wiring switches from the
// environment (parseServerConfig owns the flag surface; the skill-evolution
// stack is env-driven only):
//
//	GRAPH_MEMORY_SKILL_EVOLUTION_ENABLED  bool     (default false)
//	GRAPH_MEMORY_SKILL_EVOLUTION_POLL     duration (default 30s)
func skillEvolutionConfig() (bool, time.Duration, error) {
	enabled := strings.ToLower(strings.TrimSpace(os.Getenv("GRAPH_MEMORY_SKILL_EVOLUTION_ENABLED")))
	if enabled == "" {
		enabled = "false"
	}
	on, err := strconv.ParseBool(enabled)
	if err != nil {
		return false, 0, fmt.Errorf("server configuration: GRAPH_MEMORY_SKILL_EVOLUTION_ENABLED must be a boolean: %w", err)
	}
	poll := 30 * time.Second
	if raw := strings.TrimSpace(os.Getenv("GRAPH_MEMORY_SKILL_EVOLUTION_POLL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return false, 0, fmt.Errorf("server configuration: GRAPH_MEMORY_SKILL_EVOLUTION_POLL must be a duration: %w", err)
		}
		if parsed <= 0 || parsed > 10*time.Minute {
			return false, 0, fmt.Errorf("server configuration: GRAPH_MEMORY_SKILL_EVOLUTION_POLL must be positive and within ten minutes")
		}
		poll = parsed
	}
	return on, poll, nil
}

// projectorWorker owns the skill-evolution projection stack of this
// process: the reason-code registry over the frozen conformance policy, the
// authority schema gates, the ledger store/transaction manager, the GMS-205
// Runtime projector, the GMS-207 exact materialization closure read, and
// the GMS-206 retrieval tools (memory_explore/memory_expand/skill_get)
// served over the same in-process authorities. It projects once at boot and
// then on every poll tick; every projection (success or blocked) emits one
// JSON line on the access log. Relation sources (similarity assessments,
// claim assessments) and the evidence producer stay unwired in this tracer
// bullet: the memory ledger cannot enumerate per-assessment streams and no
// in-process evidence producer exists yet, so the projector consumes the
// activation ledger, the retrieval tools resolve evidence through a
// fail-closed (never-committed) resolver, and both block fail closed on any
// unwired source — exactly the fail-closed contract. A durable adapter
// replaces the memory store.
type projectorWorker struct {
	svc              *projector.Service
	skillEvolutionIO http.Handler
	usageStore       *usageprojection.MemoryStore
	usageProjection  *usageprojection.UsageProjectionService
	probation        *probation.Service
	armcStore        *armc.MemoryStore
	armcService      *armc.Service
	armcRegistrar    *compositehandoff.ArmCPlanRegistrar
	policyWorker     *policyactivation.DirectWorker
	eventLog         *httpapi.JSONLogger
	poll             time.Duration
	done             chan struct{}
}

// unwiredEvidence is the fail-closed EvidenceResolver of the tracer-bullet
// deployment: with no in-process evidence producer nothing resolves as
// committed, so no evidence citation can ever be served unverified.
type unwiredEvidence struct{}

func (unwiredEvidence) GetEvidence(string) (contract.EvidenceRef, bool, error) {
	return contract.EvidenceRef{}, false, nil
}

// projectionWatermark adapts the GMS-205 projector to the closure read's
// optional §7.14 watermark port (information only; never gates a read).
type projectionWatermark struct {
	svc *projector.Service
}

func (p projectionWatermark) ProjectionWatermark(ctx context.Context) (map[string]any, bool) {
	if p.svc == nil {
		return nil, false
	}
	doc, _, _ := p.svc.Watermark()
	return doc, doc != nil
}

// newProjectorWorker wires the stack; any wiring failure is fatal
// (fail-closed construction, Contract §13.7.1 R5).
func newProjectorWorker(eventLog *httpapi.JSONLogger, poll time.Duration, token, statePath string, candidates memory.CandidateStore) (*projectorWorker, error) {
	confDir, err := contract.DefaultConformanceDir()
	if err != nil {
		return nil, fmt.Errorf("skill evolution: %w", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(confDir, "policy"))
	if err != nil {
		return nil, fmt.Errorf("skill evolution: load reason policy: %w", err)
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	schemas, err := validation.LoadSchemaSet(filepath.Join(confDir, "schema", "shared"))
	if err != nil {
		return nil, fmt.Errorf("skill evolution: load authority schemas: %w", err)
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: new gates: %w", err)
	}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: new ledger store: %w", err)
	}
	manager, err := ledger.NewManager(store, registry)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: new transaction manager: %w", err)
	}
	proposals, err := proposal.NewService(gates, filepath.Join(confDir, "schema", "state"), store, manager, registry)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: proposal service: %w", err)
	}
	activations, err := activation.NewActivationService(activation.Config{
		Store: store, Tx: manager, Registry: registry, Gates: gates, Proposals: proposals,
	})
	if err != nil {
		return nil, fmt.Errorf("skill evolution: activation service: %w", err)
	}
	artifacts, err := artifact.NewService(gates)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: artifact service: %w", err)
	}
	svc, err := projector.NewService(projector.Config{
		Store: store, Tx: manager, Registry: registry, Gates: gates,
	})
	if err != nil {
		return nil, fmt.Errorf("skill evolution: new projector: %w", err)
	}
	closure, err := materializationread.NewService(materializationread.Config{
		Heads: activations, Artifacts: store, Gate: artifacts, Activations: store,
		Registry: registry, Watermark: projectionWatermark{svc: svc},
	})
	if err != nil {
		return nil, fmt.Errorf("skill evolution: closure read: %w", err)
	}
	// GMS-206 retrieval tools over the same authorities: the frozen §12.7.1
	// tool-binding matrix gates every result, and the M6 readiness gate keeps
	// skill_get disabled until its recorded conformance cases are green.
	toolPolicy, err := skillretrieval.LoadToolPolicy(
		filepath.Join(confDir, "policy", "tool-success-validation.v1.json"))
	if err != nil {
		return nil, fmt.Errorf("skill evolution: load tool policy: %w", err)
	}
	skillGetGreen, err := skillretrieval.VerifySkillGetReadiness(confDir)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: skill_get readiness gate: %w", err)
	}
	usageStore := usageprojection.NewMemoryStore()
	usageService, err := usageprojection.NewUsageProjectionService(usageprojection.UsageRankingPolicy{Version: "usage-ranking-v1"}, usageStore)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: usage projection: %w", err)
	}
	retrievalTools, err := skillretrieval.NewService(skillretrieval.Config{
		Projection: svc, Heads: activations, Artifacts: store, Gate: artifacts,
		Evidence: unwiredEvidence{}, Gates: gates, Registry: registry,
		Policy: toolPolicy, Usage: usageService, SkillGetEnabled: skillGetGreen,
	})
	if err != nil {
		return nil, fmt.Errorf("skill evolution: retrieval tools: %w", err)
	}
	probationService, err := probation.NewService(probation.Policy{Version: "probation-v1", MinimumIndependentContexts: 2, RefutationSuspensionThreshold: 2, RecoveryAdditionalContexts: 1}, usageStore)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: probation policy: %w", err)
	}
	advisoryReader, err := candidate.NewAdvisoryReader(store)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: advisory reader: %w", err)
	}
	handler := httpapi.NewSkillEvolutionHandler(httpapi.SkillEvolutionDependencies{
		Token: token, Reader: closure, Retrieval: retrievalTools, Usage: usageService, Advisory: advisoryReader, Probation: probationService, Outcomes: candidates, WriterIdentity: "server-arm-b",
	})
	armcStore := armc.NewMemoryStore()
	if statePath != "" {
		armcStatePath := statePath + ".armc.json"
		if err := armcStore.LoadFromFile(armcStatePath); err != nil {
			return nil, fmt.Errorf("skill evolution: load Arm C state: %w", err)
		}
		armcStore.SetCheckpoint(func([]byte) error { return armcStore.PersistToFile(armcStatePath) })
	}
	armcService, err := armc.NewService(armcStore, gates)
	if err != nil {
		return nil, fmt.Errorf("skill evolution: Arm C evaluator: %w", err)
	}
	policyWorker, err := policyactivation.NewDirectWorker(candidates, candidates, armcService.DirectEvaluator(), policyactivation.NewDirectActivator(candidates, systemClock{}))
	if err != nil {
		return nil, fmt.Errorf("skill evolution: policy activation worker: %w", err)
	}
	armcRegistrar, err := compositehandoff.NewArmCPlanRegistrar(armcStore, candidates, func(ctx context.Context, job policyactivation.Job) (policyactivation.Result, error) {
		return policyWorker.Process(ctx, job)
	})
	if err != nil {
		return nil, fmt.Errorf("skill evolution: direct Arm C registrar: %w", err)
	}
	return &projectorWorker{
		svc: svc, skillEvolutionIO: handler,
		usageStore: usageStore, usageProjection: usageService, probation: probationService,
		armcStore: armcStore, armcService: armcService, armcRegistrar: armcRegistrar, policyWorker: policyWorker,
		eventLog: eventLog, poll: poll, done: make(chan struct{}),
	}, nil
}

// start runs the boot projection and the poll loop until ctx ends.
func (w *projectorWorker) start(ctx context.Context) {
	w.projectOnce(ctx)
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(w.poll)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.projectOnce(ctx)
			}
		}
	}()
}

// stop waits for the poll loop to drain.
func (w *projectorWorker) stop() {
	<-w.done
}

// projectOnce drains the authoritative sources by cursor and logs the
// committed head (watermark digest, cursors, state; the blocking reason
// code when blocked). Errors are logged, never fatal: the projector is
// idempotent and the next tick reprojects from the same cursor.
func (w *projectorWorker) projectOnce(ctx context.Context) {
	started := time.Now()
	result, err := w.svc.ProjectRuntime(ctx)
	fields := map[string]any{
		"duration_ms": float64(time.Since(started).Microseconds()) / 1000,
	}
	if result != nil {
		fields["state"] = result.State
		fields["projected"] = result.Projected
		fields["cursors"] = map[string]any{
			"activation":          result.Cursors.Activation,
			"similarity":          result.Cursors.Similarity,
			"evidence_assessment": result.Cursors.EvidenceAssessment,
		}
		fields["watermark_digest"] = result.WatermarkDigest
		if result.Blocked != nil {
			fields["blocked_code"] = result.Blocked.Code
		}
	}
	if err != nil {
		fields["error"] = err.Error()
		if code := projector.CodeOf(err); code != "" {
			fields["reason_code"] = code
		}
	}
	w.eventLog.Log("skill_evolution_projection", fields)
}

// CompleteArmC is the explicit server-owned synchronous completion entry.
// It is intentionally not mounted as HTTP and is not scheduled by the
// projector ticker: an Arm C evaluator producer calls it immediately after
// it has registered the exact manifest, contract, plan and paired fixture.
func (w *projectorWorker) CompleteArmC(ctx context.Context, job policyactivation.Job) (policyactivation.Result, error) {
	result, err := w.policyWorker.Process(ctx, job)
	fields := map[string]any{"candidate_id": job.CandidateID, "activated": result.Activated, "rejected": result.Rejected}
	if result.RejectionReason != "" {
		fields["rejection_reason"] = result.RejectionReason
	}
	if result.DecisionRef.DecisionID != "" {
		fields["decision_id"] = result.DecisionRef.DecisionID
		fields["decision_digest"] = result.DecisionRef.Digest
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	w.eventLog.Log("arm_c_completion", fields)
	return result, err
}

// AdmitProbation is a server-owned, non-HTTP governance entry for producers
// that have already persisted immutable usage evidence and (for tool skills)
// an exact contract-validation result. It cannot activate a Skill.
func (w *projectorWorker) AdmitProbation(ctx context.Context, request probation.AdmissionRequest) probation.Decision {
	if w.probation == nil {
		return probation.Decision{Reason: probation.ReasonUnknownCandidate}
	}
	return w.probation.Admit(ctx, request)
}

// ReportProbationToolContractValidation is the server-owned path by which a
// trusted tool-validation producer reports an exact immutable result. It is
// intentionally not mounted as HTTP.
func (w *projectorWorker) ReportProbationToolContractValidation(candidateID string, validation probation.ToolContractValidation) probation.Decision {
	if w.probation == nil {
		return probation.Decision{Reason: probation.ReasonUnknownCandidate}
	}
	return w.probation.ReportToolContractValidation(candidateID, validation)
}

// RecoverProbation requires an explicit producer action; fresh qualifying
// evidence remains checked by the probation policy before visibility returns.
func (w *projectorWorker) RecoverProbation(ctx context.Context, request probation.RecoveryRequest) probation.Decision {
	if w.probation == nil {
		return probation.Decision{Reason: probation.ReasonUnknownCandidate}
	}
	return w.probation.Recover(ctx, request)
}

// ObserveProbation is a server-owned governance tick/producer entry. Public
// Arm B HTTP writes only append observations; an internal producer calls this
// after durable admission when it wants suspension policy reconciled.
func (w *projectorWorker) ObserveProbation(ctx context.Context, subject usageprojection.UsageSubjectRef) {
	if w.probation != nil {
		w.probation.Observe(ctx, subject)
	}
}
