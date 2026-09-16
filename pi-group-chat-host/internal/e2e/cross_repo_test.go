// Package e2e verifies the cross-repository integration contract: the Host's
// Memory Protocol adapter and turn runtime against a REAL graph-memory-service
// process. The test is gated on GMS_E2E_BASE_URL and GMS_E2E_TOKEN so the
// normal unit suite never needs the other repository running.
package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
	"river2.dev/pi-group-chat-host/internal/ports"
	"river2.dev/pi-group-chat-host/internal/runtime"
)

const (
	e2eTenant        = "tenant-e2e"
	e2eHostPrincipal = "host-service"
	e2eSharedSpace   = "space-e2e-shared"
	e2ePrivateSpace  = "space-e2e-private"
	sharedFact       = "The team agreed that launch is Friday."
	privateFact      = "The owner privately noted a budget risk."
	e2eQuestion      = "What did we learn in the first turn?"
)

func TestHostRuntimeAgainstRealGraphMemoryService(t *testing.T) {
	baseURL := os.Getenv("GMS_E2E_BASE_URL")
	token := os.Getenv("GMS_E2E_TOKEN")
	if baseURL == "" || token == "" {
		t.Skip("cross-repo E2E disabled: set GMS_E2E_BASE_URL and GMS_E2E_TOKEN to a running graph-memory-service process")
	}
	ctx := context.Background()
	client := memoryclient.NewClient(baseURL, token, &http.Client{Timeout: 5 * time.Second}, 1<<20)
	now := time.Now().UTC()

	// Lifecycle: tenant bootstrap binds this deployment token to the acting
	// host principal; spaces and grants follow.
	if _, err := client.InitializeTenant(ctx, ports.InitializeTenantRequest{
		TenantID: e2eTenant, DisplayName: "E2E tenant", BootstrapPrincipalID: e2eHostPrincipal,
	}); err != nil {
		t.Fatalf("InitializeTenant: %v", err)
	}
	if _, err := client.RegisterPrincipal(ctx, ports.RegisterPrincipalRequest{
		PrincipalID: "agent-owner", Kind: "agent", DisplayName: "Owner agent",
	}); err != nil {
		t.Fatalf("RegisterPrincipal: %v", err)
	}
	for _, space := range []ports.RegisterSpaceRequest{
		{SpaceID: e2eSharedSpace, Scope: "shared", DisplayName: "Room shared"},
		{SpaceID: e2ePrivateSpace, Scope: "private", OwnerPrincipalID: ptr("agent-owner"), DisplayName: "Owner private"},
	} {
		if _, err := client.RegisterSpace(ctx, space); err != nil {
			t.Fatalf("RegisterSpace %s: %v", space.SpaceID, err)
		}
	}
	// Two grants mirror the purpose fence the service enforces since the
	// review fixes: lifecycle covers evidence and recall, tool_plane covers
	// the exploration tool surface. One mixed grant would be rejected at
	// authorization time with GRANT_MISSING.
	if _, err := client.RegisterGrant(ctx, ports.RegisterGrantRequest{
		GrantID:     "grant-e2e-lifecycle",
		PrincipalID: e2eHostPrincipal,
		SpaceIDs:    []string{e2eSharedSpace, e2ePrivateSpace},
		Purpose:     "lifecycle",
		Operations:  []string{"evidence.stage", "evidence.commit", "recall"},
		// A fixed expiry keeps registration replays byte-identical; a
		// per-run timestamp would be a real content change and the service
		// correctly answers 409 IDEMPOTENCY_CONFLICT for those.
		ExpiresAt: "2027-12-31T00:00:00Z",
	}); err != nil {
		t.Fatalf("RegisterGrant lifecycle: %v", err)
	}
	if _, err := client.RegisterGrant(ctx, ports.RegisterGrantRequest{
		GrantID:     "grant-e2e-tool-plane",
		PrincipalID: e2eHostPrincipal,
		SpaceIDs:    []string{e2eSharedSpace},
		Purpose:     "tool_plane",
		Operations:  []string{"exploration.start", "exploration.explore", "exploration.submit"},
		ExpiresAt:   "2027-12-31T00:00:00Z",
	}); err != nil {
		t.Fatalf("RegisterGrant tool-plane: %v", err)
	}

	authority := runtime.ExecutionAuthority{
		TenantID: e2eTenant, RoomID: "room-e2e", AgentID: "agent-owner", ProfileKind: "ordinary",
		WorkingDirectory: t.TempDir(), SharedSpaceID: e2eSharedSpace, PrivateSpaceID: e2ePrivateSpace,
	}
	piBinary := writeE2EFakePi(t, "prompt-e2e-first", "prompt-e2e-second")

	// Turn one: no evidence exists yet, so the scoped recall degrades to
	// empty but the turn still settles with two pending outbox projections.
	first, err := runtime.ExecuteOrdinaryTurn(ctx, runtime.TurnRequest{
		Authority: authority, RoomInput: e2eQuestion, HumanMessageID: "human-e2e-first",
		MemoryBaseURL: baseURL, MemoryAuthToken: token, PiBinary: piBinary, PromptRequestID: "prompt-e2e-first",
	})
	if err != nil {
		t.Fatalf("first ExecuteOrdinaryTurn against real GMS: %v", err)
	}
	if first.Delivery.State != "settled" || first.Segment.State != "settled" {
		t.Fatalf("first turn did not settle: %#v", first)
	}
	// On a pristine service this is "empty"; on replays against the same
	// process the previous run's committed batches are already visible, so
	// both states are legal here. The strict "invisible before commit"
	// guarantee is asserted by the in-repo unit suite.
	if first.Recall.State != "empty" && first.Recall.State != "complete" {
		t.Fatalf("first recall state = %q, want empty or replay-visible complete", first.Recall.State)
	}
	if len(first.Outbox) != 2 || first.Outbox[0].State != "pending" || first.Outbox[1].State != "pending" {
		t.Fatalf("first turn outbox = %#v, want two pending projections", first.Outbox)
	}

	// Commit the first turn's shared and private evidence through the same
	// adapter, exactly like the durable outbox drain would.
	commitEvidence(t, ctx, client, "batch-e2e-shared", e2eSharedSpace, "e2e-outbox-shared", "room_shared", sharedFact, now)
	commitEvidence(t, ctx, client, "batch-e2e-private", e2ePrivateSpace, "e2e-outbox-private", "agent_private", privateFact, now)

	// Turn two: the same scoped recall must now cite the committed evidence
	// and the composed Pi prompt must carry the durable citation.
	second, err := runtime.ExecuteOrdinaryTurn(ctx, runtime.TurnRequest{
		Authority: authority, RoomInput: e2eQuestion, HumanMessageID: "human-e2e-second",
		MemoryBaseURL: baseURL, MemoryAuthToken: token, PiBinary: piBinary, PromptRequestID: "prompt-e2e-second",
	})
	if err != nil {
		t.Fatalf("second ExecuteOrdinaryTurn against real GMS: %v", err)
	}
	if second.Recall.State != "complete" || len(second.Recall.Citations) != 2 {
		t.Fatalf("second recall = %#v, want complete with both citations", second.Recall)
	}
	if !strings.Contains(second.Prompt, second.Recall.Citations[0]) {
		t.Fatalf("second prompt lacks durable citation %q: %q", second.Recall.Citations[0], second.Prompt)
	}

	// Exploration over the shared space only, through the Memory tool plane
	// surface used by the persistent Memory Agent.
	start, err := client.StartExploration(ctx, ports.StartExplorationRequest{
		RequestID: "e2e-exploration", IdempotencyKey: "e2e-exploration-op",
		SpaceIDs: []string{e2eSharedSpace}, Query: e2eQuestion, MaxSteps: 3, MaxResults: 5,
	})
	if err != nil {
		t.Fatalf("StartExploration: %v", err)
	}
	if len(start.Items) == 0 {
		t.Fatalf("exploration start served no shared items: %#v", start)
	}
	submitted, err := client.Submit(ctx, start.SessionID, ports.SubmitRequest{
		OperationID: "e2e-submit-op", Found: true, Summary: "Launch is Friday.",
		CitationIDs: []string{start.Items[0].Citation.CitationID},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(submitted.Citations) == 0 {
		t.Fatalf("submit returned no grounded citations: %#v", submitted)
	}
}

func commitEvidence(t *testing.T, ctx context.Context, client *memoryclient.Client, batchID, spaceID, key, sourceKind, content string, now time.Time) {
	t.Helper()
	digest := sha256.Sum256([]byte(content))
	response, err := client.StageEvidenceBatch(ctx, ports.StageEvidenceBatchRequest{
		BatchID:         batchID,
		IdempotencyKey:  key,
		SpaceID:         spaceID,
		StreamID:        "stream-" + spaceID,
		SourceSegmentID: "segment-e2e-first",
		Provenance: ports.EvidenceProvenance{
			HostType:       "pi-group-chat-host",
			HostInstanceID: "host-e2e",
			SourceKind:     sourceKind,
			// Fixed timestamps keep at-least-once replays byte-identical;
			// drift here would be a real content change (409).
			CapturedAt:    "2026-09-08T11:00:00Z",
			ContentSHA256: hex.EncodeToString(digest[:]),
		},
		Events: []ports.EvidenceEvent{{
			EventID: "event-" + batchID, Sequence: 1, Kind: "room_message",
			Content: content, OccurredAt: "2026-09-08T11:00:00Z",
		}},
		Links:           nil,
		TerminalOutcome: "settled",
	})
	if err != nil {
		t.Fatalf("StageEvidenceBatch %s: %v", batchID, err)
	}
	if response.BatchID != batchID || response.SpaceID != spaceID {
		t.Fatalf("stage %s response = %#v, want matching batch and space", batchID, response)
	}
	if response.State != "staged" && response.State != "committed" {
		t.Fatalf("stage %s response = %#v, want staged (or replayed committed)", batchID, response)
	}
	committed, err := client.CommitEvidenceBatch(ctx, batchID, ports.CommitEvidenceBatchRequest{CommitID: "commit-" + batchID})
	if err != nil {
		t.Fatalf("CommitEvidenceBatch %s: %v", batchID, err)
	}
	if committed.State != "committed" || committed.MemoryVersion < 1 {
		t.Fatalf("commit %s response = %#v, want committed with memory_version", batchID, committed)
	}
}

// writeE2EFakePi writes the same shape of fixture Pi used by the runtime
// contract tests: per-request-id frame replay over strict LF-delimited JSONL.
func writeE2EFakePi(t *testing.T, requestIDs ...string) string {
	t.Helper()
	var script strings.Builder
	script.WriteString("#!/bin/sh\n")
	script.WriteString("if [ \"${1:-}\" = \"--version\" ]; then printf '%s\\n' '0.84.3'; exit 0; fi\n")
	script.WriteString("while IFS= read -r line; do\n")
	script.WriteString("  case \"$line\" in\n")
	for index, requestID := range requestIDs {
		frames := e2eFrames(requestID, fmt.Sprintf("e2e-reply-op-%d", index+1))
		script.WriteString("    *'\"id\":\"" + requestID + "\"'*)\n")
		for _, frame := range frames {
			script.WriteString("      printf '%s\\n' '" + strings.ReplaceAll(frame, "'", "'\\''") + "'\n")
		}
		script.WriteString("      ;;\n")
	}
	script.WriteString("    *) printf '%s\\n' '{\"type\":\"error\",\"message\":\"unknown fixture request\"}' ;;\n")
	script.WriteString("  esac\ndone\n")
	path := filepath.Join(t.TempDir(), "fake-pi-e2e")
	if err := os.WriteFile(path, []byte(script.String()), 0o700); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}
	return path
}

func e2eFrames(requestID, replyOp string) []string {
	return []string{
		`{"id":"` + requestID + `","type":"response","command":"prompt","success":true}`,
		`{"type":"agent_start"}`,
		`{"type":"tool_execution_start","toolCallId":"tool-` + replyOp + `","toolName":"room_reply","args":{"client_operation_id":"` + replyOp + `","in_reply_to_message_id":"human-e2e","content":"E2E reply with citation."}}`,
		`{"type":"tool_execution_end","toolCallId":"tool-` + replyOp + `","toolName":"room_reply","isError":false}`,
		`{"type":"agent_end","messages":[],"willRetry":false}`,
		`{"type":"agent_settled"}`,
	}
}

func ptr(value string) *string { return &value }
