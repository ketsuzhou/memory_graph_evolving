package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type tracerMemoryFixture struct {
	mu            sync.Mutex
	routes        []string
	recallSpaces  [][]string
	exploreSpaces [][]string
	staged        map[string]string
	committed     map[string]bool
}

func newTracerMemoryServer(t *testing.T) (*httptest.Server, *tracerMemoryFixture) {
	t.Helper()
	fixture := &tracerMemoryFixture{staged: map[string]string{}, committed: map[string]bool{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tracer-memory-token" {
			t.Errorf("%s %s Authorization = %q", r.Method, r.URL.Path, got)
			writeTracerJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"code": "UNAUTHORIZED"}})
			return
		}
		fixture.mu.Lock()
		fixture.routes = append(fixture.routes, r.Method+" "+r.URL.Path)
		fixture.mu.Unlock()
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/tenant":
			writeTracerJSON(w, http.StatusCreated, map[string]any{"tenant_id": "tenant-tracer", "bootstrap_principal_id": "host-service", "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/principals":
			var body map[string]any
			decodeTracerBody(t, r, &body)
			writeTracerJSON(w, http.StatusCreated, map[string]any{"principal_id": body["principal_id"], "kind": body["kind"], "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces":
			var body map[string]any
			decodeTracerBody(t, r, &body)
			writeTracerJSON(w, http.StatusCreated, map[string]any{"space_id": body["space_id"], "scope": body["scope"], "owner_principal_id": body["owner_principal_id"], "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/grants":
			var body map[string]any
			decodeTracerBody(t, r, &body)
			writeTracerJSON(w, http.StatusCreated, map[string]any{"grant_id": body["grant_id"], "principal_id": body["principal_id"], "space_ids": body["space_ids"], "purpose": body["purpose"], "operations": body["operations"], "expires_at": body["expires_at"], "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/recalls":
			var body struct {
				RequestID string   `json:"request_id"`
				SpaceIDs  []string `json:"space_ids"`
			}
			decodeTracerBody(t, r, &body)
			fixture.mu.Lock()
			fixture.recallSpaces = append(fixture.recallSpaces, append([]string(nil), body.SpaceIDs...))
			firstVisible := fixture.committed["batch-first-shared"]
			fixture.mu.Unlock()
			items := []any{}
			state := "empty"
			if firstVisible {
				state = "complete"
				items = append(items, tracerRecallItem("The first answer established Decision Alpha.", "space-room-shared", "citation-first-shared", "batch-first-shared", "event-first-reply"))
			}
			writeTracerJSON(w, http.StatusOK, map[string]any{"request_id": body.RequestID, "items": items, "degradation": map[string]any{"state": state, "reasons": []string{}}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/evidence-batches:stage":
			var body struct {
				BatchID        string `json:"batch_id"`
				IdempotencyKey string `json:"idempotency_key"`
				SpaceID        string `json:"space_id"`
			}
			decodeTracerBody(t, r, &body)
			fixture.mu.Lock()
			old, duplicate := fixture.staged[body.IdempotencyKey]
			if !duplicate {
				fixture.staged[body.IdempotencyKey] = body.BatchID
			}
			fixture.mu.Unlock()
			if duplicate && old != body.BatchID {
				writeTracerJSON(w, http.StatusConflict, map[string]any{"error": map[string]any{"code": "IDEMPOTENCY_CONFLICT"}})
				return
			}
			status := http.StatusCreated
			if duplicate {
				status = http.StatusOK
			}
			writeTracerJSON(w, status, map[string]any{"batch_id": body.BatchID, "idempotency_key": body.IdempotencyKey, "space_id": body.SpaceID, "state": "staged", "duplicate": duplicate})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/evidence-batches/") && strings.HasSuffix(r.URL.Path, ":commit"):
			batchID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/evidence-batches/"), ":commit")
			var body struct {
				CommitID string `json:"commit_id"`
			}
			decodeTracerBody(t, r, &body)
			fixture.mu.Lock()
			duplicate := fixture.committed[batchID]
			fixture.committed[batchID] = true
			fixture.mu.Unlock()
			writeTracerJSON(w, http.StatusOK, map[string]any{"batch_id": batchID, "commit_id": body.CommitID, "state": "committed", "memory_version": 1, "committed_at": "2026-09-08T11:00:00Z", "duplicate": duplicate})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/explorations":
			var body struct {
				RequestID string   `json:"request_id"`
				SpaceIDs  []string `json:"space_ids"`
			}
			decodeTracerBody(t, r, &body)
			fixture.mu.Lock()
			fixture.exploreSpaces = append(fixture.exploreSpaces, append([]string(nil), body.SpaceIDs...))
			fixture.mu.Unlock()
			writeTracerJSON(w, http.StatusCreated, map[string]any{"session_id": "exploration-tracer", "request_id": body.RequestID, "state": "active", "pinned_spaces": []any{map[string]any{"space_id": "space-room-shared", "memory_version": 1}}, "items": []any{tracerRecallItem("Decision Alpha evidence.", "space-room-shared", "citation-first-shared", "batch-first-shared", "event-first-reply")}, "remaining_steps": 3, "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/explorations/exploration-tracer:explore":
			writeTracerJSON(w, http.StatusOK, map[string]any{"session_id": "exploration-tracer", "operation_id": "memory-explore-001", "state": "active", "items": []any{tracerRecallItem("Decision Alpha supporting edge.", "space-room-shared", "citation-first-edge", "batch-first-shared", "event-first-edge")}, "remaining_steps": 2, "duplicate": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/explorations/exploration-tracer:submit":
			writeTracerJSON(w, http.StatusOK, map[string]any{"session_id": "exploration-tracer", "operation_id": "memory-submit-001", "state": "submitted", "found": true, "summary": "Decision Alpha is supported by first-turn evidence.", "citations": []any{map[string]any{"citation_id": "citation-first-shared", "source_space_id": "space-room-shared", "memory_version": 1, "evidence_batch_id": "batch-first-shared"}}, "duplicate": false})
		default:
			http.NotFound(w, r)
		}
	}))
	return server, fixture
}

func TestTracerRunsNineStepsAndSecondTurnAndMemoryAgentCiteFirstTurnEvidence(t *testing.T) {
	memory, memoryFixture := newTracerMemoryServer(t)
	defer memory.Close()

	firstFrames := ordinaryPiFrames("prompt-first", "first-reply-op", "human-first", "Decision Alpha is the durable choice.")
	secondFrames := ordinaryPiFrames("prompt-second", "second-reply-op", "human-second", "As cited by citation-first-shared, Decision Alpha was chosen.")
	memoryFrames := []string{
		`{"id":"prompt-memory","type":"response","command":"prompt","success":true}`,
		`{"type":"agent_start"}`,
		`{"type":"tool_execution_start","toolCallId":"memory-start-call","toolName":"memory_start","args":{"client_operation_id":"memory-start-001","query":"Find Decision Alpha","max_steps":4,"max_results":5}}`,
		`{"type":"tool_execution_end","toolCallId":"memory-start-call","toolName":"memory_start","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"session_id\":\"exploration-tracer\",\"items\":[],\"remaining_steps\":3}}"}],"details":{}},"isError":false}`,
		`{"type":"tool_execution_start","toolCallId":"memory-explore-call","toolName":"memory_explore","args":{"client_operation_id":"memory-explore-001","session_id":"exploration-tracer","anchor_citation_id":"citation-first-shared","relation":"related","limit":5}}`,
		`{"type":"tool_execution_end","toolCallId":"memory-explore-call","toolName":"memory_explore","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"items\":[],\"remaining_steps\":2}}"}],"details":{}},"isError":false}`,
		`{"type":"tool_execution_start","toolCallId":"memory-submit-call","toolName":"memory_submit","args":{"client_operation_id":"memory-submit-001","session_id":"exploration-tracer","found":true,"summary":"Decision Alpha is supported.","citation_ids":["citation-first-shared"]}}`,
		`{"type":"tool_execution_end","toolCallId":"memory-submit-call","toolName":"memory_submit","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"found\":true,\"summary\":\"Decision Alpha is supported.\",\"citations\":[{\"citation_id\":\"citation-first-shared\"}]}}"}],"details":{}},"isError":false}`,
		`{"type":"tool_execution_start","toolCallId":"memory-reply-call","toolName":"room_reply","args":{"client_operation_id":"memory-reply-op","in_reply_to_message_id":"human-memory","content":"Decision Alpha is supported [citation-first-shared]."}}`,
		`{"type":"tool_execution_end","toolCallId":"memory-reply-call","toolName":"room_reply","result":{"content":[{"type":"text","text":"{\"ok\":true,\"result\":{\"message_id\":\"memory-reply\",\"sequence\":6}}"}],"details":{}},"isError":false}`,
		`{"type":"agent_end","messages":[],"willRetry":false}`,
		`{"type":"agent_settled"}`,
	}
	pi := writeFakePi(t, map[string][]string{"prompt-first": firstFrames, "prompt-second": secondFrames, "prompt-memory": memoryFrames})

	ids := TracerIDs{
		TenantID: "tenant-tracer", RoomID: "room-tracer", OrdinaryAgentID: "agent-builder", MemoryAgentID: "agent-memory",
		SharedSpaceID: "space-room-shared", PrivateSpaceID: "space-builder-private",
		FirstPromptRequestID: "prompt-first", SecondPromptRequestID: "prompt-second", MemoryPromptRequestID: "prompt-memory",
	}
	got, err := RunTracer(t.Context(), TracerScenario{
		IDs:           ids,
		Authority:     ExecutionAuthority{TenantID: ids.TenantID, RoomID: ids.RoomID, AgentID: ids.OrdinaryAgentID, ProfileKind: "ordinary", WorkingDirectory: "/srv/tracer", EnvironmentAllowlist: []string{"PATH"}, Provider: "fixture-provider", Model: "fixture-model", SharedSpaceID: ids.SharedSpaceID, PrivateSpaceID: ids.PrivateSpaceID},
		MemoryBaseURL: memory.URL, MemoryAuthToken: "tracer-memory-token", PiBinary: pi,
		FirstQuestion: "@builder What is our durable choice?", SecondQuestion: "@builder What did you decide previously?", MemoryQuestion: "@memory Find the evidence for Decision Alpha.",
	})
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("nine-step tracer contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("run tracer: %v", err)
	}

	if !reflect.DeepEqual(got.CompletedSteps, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("completed PLAN section 2 steps = %v", got.CompletedSteps)
	}
	if len(got.Messages) != 6 {
		t.Fatalf("visible canonical messages = %d, want three human mentions and three explicit replies: %#v", len(got.Messages), got.Messages)
	}
	for i, message := range got.Messages {
		if message.Sequence != int64(i+1) {
			t.Fatalf("message %q sequence = %d, want %d", message.ID, message.Sequence, i+1)
		}
	}
	if len(got.Deliveries) != 3 || len(got.Segments) != 3 {
		t.Fatalf("deliveries/segments = %d/%d, want one of each per mentioned turn", len(got.Deliveries), len(got.Segments))
	}
	if len(got.Outbox) != 6 {
		t.Fatalf("outboxes = %d, want shared+private for each closed segment", len(got.Outbox))
	}
	for _, row := range got.Outbox {
		if row.State != "committed" {
			t.Fatalf("evidence not recall-visible: %#v", row)
		}
	}
	if len(got.Recalls) != 2 || !containsString(got.Recalls[1].Citations, "citation-first-shared") {
		t.Fatalf("second-turn Recall does not cite first-turn evidence: %#v", got.Recalls)
	}
	if len(got.PiPrompts) < 2 || !strings.Contains(got.PiPrompts[1], "citation-first-shared") {
		t.Fatalf("second Pi prompt lacks durable first-turn citation: %#v", got.PiPrompts)
	}
	if got.MemoryAgent.ProfileKind != "memory" || !reflect.DeepEqual(got.MemoryAgent.SpaceIDs, []string{"space-room-shared"}) {
		t.Fatalf("Memory Agent authority = %#v", got.MemoryAgent)
	}
	if !got.MemoryAgent.Started || !got.MemoryAgent.Explored || !got.MemoryAgent.Submitted {
		t.Fatalf("Memory Agent did not complete start/explore/submit: %#v", got.MemoryAgent)
	}
	if !containsString(got.MemoryAgent.SubmittedCitations, "citation-first-shared") || !strings.Contains(got.MemoryAgent.Reply.Content, "citation-first-shared") {
		t.Fatalf("Memory Agent reply is not tied to submitted citation: %#v", got.MemoryAgent)
	}

	memoryFixture.mu.Lock()
	defer memoryFixture.mu.Unlock()
	if !reflect.DeepEqual(memoryFixture.recallSpaces, [][]string{{"space-room-shared", "space-builder-private"}, {"space-room-shared", "space-builder-private"}}) {
		t.Fatalf("ordinary Recall scopes = %v", memoryFixture.recallSpaces)
	}
	if !reflect.DeepEqual(memoryFixture.exploreSpaces, [][]string{{"space-room-shared"}}) {
		t.Fatalf("Memory Agent exploration scopes = %v", memoryFixture.exploreSpaces)
	}
	for _, route := range []string{"PUT /v1/tenant", "POST /v1/principals", "POST /v1/spaces", "POST /v1/grants", "POST /v1/recalls", "POST /v1/evidence-batches:stage", "POST /v1/explorations", "POST /v1/explorations/exploration-tracer:explore", "POST /v1/explorations/exploration-tracer:submit"} {
		if !containsString(memoryFixture.routes, route) {
			t.Errorf("Memory Protocol route %q not exercised; got %v", route, memoryFixture.routes)
		}
	}
}

func tracerRecallItem(content, spaceID, citationID, batchID, eventID string) map[string]any {
	return map[string]any{"content": content, "source_space_id": spaceID, "memory_version": 1, "citation": map[string]any{"citation_id": citationID, "evidence_batch_id": batchID, "event_ids": []string{eventID}}, "score": 0.95}
}

func decodeTracerBody(t *testing.T, r *http.Request, target any) {
	t.Helper()
	if err := json.NewDecoder(r.Body).Decode(target); err != nil {
		t.Errorf("decode %s %s: %v", r.Method, r.URL.Path, err)
	}
}

func writeTracerJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		panic(fmt.Sprintf("encode tracer fixture: %v", err))
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
