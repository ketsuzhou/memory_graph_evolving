package memoryclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientUsesHTTPJSONAgainstIndependentServer(t *testing.T) {
	owner := "principal-owner"
	capturedAt := "2026-09-08T10:00:00.123456789Z"
	stage := StageEvidenceBatchRequest{
		BatchID: "batch-1", IdempotencyKey: "outbox-1", SpaceID: "space-shared", StreamID: "stream-1", SourceSegmentID: "segment-1",
		Provenance: EvidenceProvenance{HostType: "pi-group-chat-host", HostInstanceID: "host-1", SourceKind: "room_shared", CapturedAt: capturedAt, ContentSHA256: strings.Repeat("a", 64)},
		Events:     []EvidenceEvent{{EventID: "event-1", Sequence: 1, Kind: "room_message", Content: "hello", OccurredAt: capturedAt}},
		Links:      []EvidenceLink{}, TerminalOutcome: "settled",
	}

	tests := []struct {
		name, method, path string
		body               any
		idempotencyField   string
		idempotencyValue   string
		response           string
		invoke             func(*Client) error
	}{
		{
			name: "initialize tenant", method: http.MethodPut, path: "/v1/tenant",
			body: InitializeTenantRequest{TenantID: "tenant-1", DisplayName: "Tenant", BootstrapPrincipalID: "service-1"}, idempotencyField: "tenant_id", idempotencyValue: "tenant-1",
			response: `{"tenant_id":"tenant-1","bootstrap_principal_id":"service-1","duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.InitializeTenant(context.Background(), InitializeTenantRequest{TenantID: "tenant-1", DisplayName: "Tenant", BootstrapPrincipalID: "service-1"})
				return err
			},
		},
		{
			name: "register principal", method: http.MethodPost, path: "/v1/principals",
			body: RegisterPrincipalRequest{PrincipalID: "principal-1", Kind: "agent", DisplayName: "Agent"}, idempotencyField: "principal_id", idempotencyValue: "principal-1",
			response: `{"principal_id":"principal-1","kind":"agent","duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.RegisterPrincipal(context.Background(), RegisterPrincipalRequest{PrincipalID: "principal-1", Kind: "agent", DisplayName: "Agent"})
				return err
			},
		},
		{
			name: "register space", method: http.MethodPost, path: "/v1/spaces",
			body: RegisterSpaceRequest{SpaceID: "space-private", Scope: "private", OwnerPrincipalID: &owner, DisplayName: "Private"}, idempotencyField: "space_id", idempotencyValue: "space-private",
			response: `{"space_id":"space-private","scope":"private","owner_principal_id":"principal-owner","duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.RegisterSpace(context.Background(), RegisterSpaceRequest{SpaceID: "space-private", Scope: "private", OwnerPrincipalID: &owner, DisplayName: "Private"})
				return err
			},
		},
		{
			name: "register grant", method: http.MethodPost, path: "/v1/grants",
			body: RegisterGrantRequest{GrantID: "grant-1", PrincipalID: "principal-1", SpaceIDs: []string{"space-shared"}, Purpose: "tool_plane", Operations: []string{"recall"}, ExpiresAt: "2027-09-08T10:00:00Z"}, idempotencyField: "grant_id", idempotencyValue: "grant-1",
			response: `{"grant_id":"grant-1","principal_id":"principal-1","space_ids":["space-shared"],"purpose":"tool_plane","operations":["recall"],"expires_at":"2027-09-08T10:00:00Z","duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.RegisterGrant(context.Background(), RegisterGrantRequest{GrantID: "grant-1", PrincipalID: "principal-1", SpaceIDs: []string{"space-shared"}, Purpose: "tool_plane", Operations: []string{"recall"}, ExpiresAt: "2027-09-08T10:00:00Z"})
				return err
			},
		},
		{
			name: "stage evidence batch", method: http.MethodPost, path: "/v1/evidence-batches:stage",
			body: stage, idempotencyField: "idempotency_key", idempotencyValue: "outbox-1",
			response: `{"batch_id":"batch-1","idempotency_key":"outbox-1","space_id":"space-shared","state":"staged","duplicate":false}`,
			invoke:   func(c *Client) error { _, err := c.StageEvidenceBatch(context.Background(), stage); return err },
		},
		{
			name: "commit evidence batch", method: http.MethodPost, path: "/v1/evidence-batches/batch-1:commit",
			body: CommitEvidenceBatchRequest{CommitID: "commit-1"}, idempotencyField: "commit_id", idempotencyValue: "commit-1",
			response: `{"batch_id":"batch-1","commit_id":"commit-1","state":"committed","memory_version":1,"committed_at":"2026-09-08T10:01:00Z","duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.CommitEvidenceBatch(context.Background(), "batch-1", CommitEvidenceBatchRequest{CommitID: "commit-1"})
				return err
			},
		},
		{
			name: "get evidence batch", method: http.MethodGet, path: "/v1/evidence-batches/batch-1",
			body: nil, idempotencyField: "", idempotencyValue: "",
			response: `{"batch_id":"batch-1","idempotency_key":"outbox-1","space_id":"space-shared","source_segment_id":"segment-1","state":"staged","memory_version":null,"committed_at":null}`,
			invoke:   func(c *Client) error { _, err := c.EvidenceBatch(context.Background(), "batch-1"); return err },
		},
		{
			name: "recall", method: http.MethodPost, path: "/v1/recalls",
			body: RecallRequest{RequestID: "request-recall", Query: "hello", SpaceIDs: []string{"space-shared", "space-private"}, MaxResults: 5, DeadlineMS: 500}, idempotencyField: "", idempotencyValue: "",
			response: `{"request_id":"request-recall","items":[],"degradation":{"state":"empty","reasons":[]}}`,
			invoke: func(c *Client) error {
				_, err := c.Recall(context.Background(), RecallRequest{RequestID: "request-recall", Query: "hello", SpaceIDs: []string{"space-shared", "space-private"}, MaxResults: 5, DeadlineMS: 500})
				return err
			},
		},
		{
			name: "start exploration", method: http.MethodPost, path: "/v1/explorations",
			body: StartExplorationRequest{RequestID: "request-start", IdempotencyKey: "start-1", SpaceIDs: []string{"space-shared"}, Query: "hello", MaxSteps: 3, MaxResults: 5}, idempotencyField: "idempotency_key", idempotencyValue: "start-1",
			response: `{"session_id":"session-1","request_id":"request-start","state":"active","pinned_spaces":[{"space_id":"space-shared","memory_version":1}],"items":[],"remaining_steps":3,"duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.StartExploration(context.Background(), StartExplorationRequest{RequestID: "request-start", IdempotencyKey: "start-1", SpaceIDs: []string{"space-shared"}, Query: "hello", MaxSteps: 3, MaxResults: 5})
				return err
			},
		},
		{
			name: "explore", method: http.MethodPost, path: "/v1/explorations/session-1:explore",
			body: ExploreRequest{OperationID: "operation-explore", AnchorCitationID: "citation-1", Relation: "related", Limit: 5}, idempotencyField: "operation_id", idempotencyValue: "operation-explore",
			response: `{"session_id":"session-1","operation_id":"operation-explore","state":"active","items":[],"remaining_steps":2,"duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.Explore(context.Background(), "session-1", ExploreRequest{OperationID: "operation-explore", AnchorCitationID: "citation-1", Relation: "related", Limit: 5})
				return err
			},
		},
		{
			name: "redirect", method: http.MethodPost, path: "/v1/explorations/session-1:redirect",
			body: RedirectRequest{OperationID: "operation-redirect", Query: "other", AnchorCitationIDs: []string{"citation-1"}, Reason: "refine"}, idempotencyField: "operation_id", idempotencyValue: "operation-redirect",
			response: `{"session_id":"session-1","operation_id":"operation-redirect","state":"active","items":[],"remaining_steps":1,"duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.Redirect(context.Background(), "session-1", RedirectRequest{OperationID: "operation-redirect", Query: "other", AnchorCitationIDs: []string{"citation-1"}, Reason: "refine"})
				return err
			},
		},
		{
			name: "submit", method: http.MethodPost, path: "/v1/explorations/session-1:submit",
			body: SubmitRequest{OperationID: "operation-submit", Found: true, Summary: "found", CitationIDs: []string{"citation-1"}}, idempotencyField: "operation_id", idempotencyValue: "operation-submit",
			response: `{"session_id":"session-1","operation_id":"operation-submit","state":"submitted","found":true,"summary":"found","citations":[{"citation_id":"citation-1","source_space_id":"space-shared","memory_version":1,"evidence_batch_id":"batch-1"}],"duplicate":false}`,
			invoke: func(c *Client) error {
				_, err := c.Submit(context.Background(), "session-1", SubmitRequest{OperationID: "operation-submit", Found: true, Summary: "found", CitationIDs: []string{"citation-1"}})
				return err
			},
		},
	}

	if len(tests) != 12 {
		t.Fatalf("contract table has %d methods, want 12", len(tests))
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var called atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called.Store(true)
				if r.Method != tt.method {
					t.Errorf("method = %q, want %q", r.Method, tt.method)
				}
				if r.URL.RequestURI() != tt.path {
					t.Errorf("path = %q, want %q", r.URL.RequestURI(), tt.path)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer graph-token" {
					t.Errorf("Authorization = %q", got)
				}
				if got := r.Header.Get("Content-Type"); got != "application/json" {
					t.Errorf("Content-Type = %q", got)
				}
				if got := r.Header.Get("Accept"); got != "application/json" {
					t.Errorf("Accept = %q", got)
				}

				gotBody, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				assertJSONBody(t, gotBody, tt.body)
				assertIdempotencyKey(t, gotBody, tt.idempotencyField, tt.idempotencyValue)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tt.response)
			}))
			defer server.Close()

			client := NewClient(server.URL, "graph-token", server.Client(), 1<<20)
			if err := tt.invoke(client); err != nil {
				t.Errorf("call returned error: %v", err)
			}
			if !called.Load() {
				t.Error("independent HTTP server was not called")
			}
		})
	}
}

func TestRecallSynthesizesUnavailableDegradationForTimeout5xxAndOutage(t *testing.T) {
	recall := RecallRequest{RequestID: "recall-degraded", Query: "q", SpaceIDs: []string{"shared", "private"}, MaxResults: 5, DeadlineMS: 20}

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(250 * time.Millisecond):
			}
		}))
		defer server.Close()
		httpClient := server.Client()
		httpClient.Timeout = 20 * time.Millisecond
		assertRecallUnavailable(t, NewClient(server.URL, "token", httpClient, 1<<20), recall)
	})

	t.Run("5xx", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"UNAVAILABLE","message":"down","request_id":"server-request","details":[]}}`)
		}))
		defer server.Close()
		assertRecallUnavailable(t, NewClient(server.URL, "token", server.Client(), 1<<20), recall)
	})

	t.Run("outage", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		client := server.Client()
		url := server.URL
		server.Close()
		assertRecallUnavailable(t, NewClient(url, "token", client, 1<<20), recall)
	})
}

func assertRecallUnavailable(t *testing.T, client *Client, request RecallRequest) {
	t.Helper()
	response, err := client.Recall(context.Background(), request)
	if err != nil {
		t.Errorf("Recall returned error instead of degradation: %v", err)
	}
	if response.RequestID != request.RequestID {
		t.Errorf("request_id = %q, want %q", response.RequestID, request.RequestID)
	}
	if response.Degradation.State != "unavailable" {
		t.Errorf("degradation state = %q, want unavailable", response.Degradation.State)
	}
	if len(response.Degradation.Reasons) == 0 {
		t.Error("unavailable degradation must include a reason")
	}
	if len(response.Items) != 0 {
		t.Errorf("outage items = %v, want none and no scope fallback", response.Items)
	}
}

func assertJSONBody(t *testing.T, got []byte, want any) {
	t.Helper()
	if want == nil {
		if len(got) != 0 {
			t.Errorf("body = %q, want empty", got)
		}
		return
	}
	wantBytes, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal expected body: %v", err)
	}
	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Errorf("body is not JSON: %v", err)
		return
	}
	if err := json.Unmarshal(wantBytes, &wantValue); err != nil {
		t.Fatalf("decode expected body: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Errorf("body = %s, want %s", got, wantBytes)
	}
}

func assertIdempotencyKey(t *testing.T, body []byte, field, want string) {
	t.Helper()
	if len(body) == 0 {
		if field != "" {
			t.Errorf("missing idempotency field %q", field)
		}
		return
	}
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		return
	}
	if field == "" {
		for _, forbidden := range []string{"idempotency_key", "operation_id", "commit_id"} {
			if _, exists := object[forbidden]; exists {
				t.Errorf("unexpected idempotency field %q", forbidden)
			}
		}
		return
	}
	if got, ok := object[field].(string); !ok || got != want {
		t.Errorf("idempotency field %q = %#v, want %q", field, object[field], want)
	}
}
