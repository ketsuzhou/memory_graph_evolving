// gmtest-host scripts the pi-group-chat-host runtime for the external Python
// test driver: JSON-lines requests on stdin, one JSON response per line on
// stdout. One process owns one durable in-memory store, so turns, drains,
// and restart recovery inside a case share room state, while cases stay
// isolated from each other by process boundaries.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
	"river2.dev/pi-group-chat-host/internal/pi/roombridge"
	"river2.dev/pi-group-chat-host/internal/ports"
	"river2.dev/pi-group-chat-host/internal/runtime"
)

type spaceSpec struct {
	ID    string  `json:"id"`
	Scope string  `json:"scope"`
	Owner *string `json:"owner"`
}

type grantSpec struct {
	ID         string   `json:"id"`
	Principal  string   `json:"principal"`
	SpaceIDs   []string `json:"spaces"`
	Purpose    string   `json:"purpose"`
	Operations []string `json:"operations"`
	ExpiresAt  string   `json:"expires_at"`
}

type principalSpec struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	DisplayName string `json:"display_name"`
}

type request struct {
	Op string `json:"op"`

	MemoryURL string `json:"memory_url,omitempty"`
	Token     string `json:"token,omitempty"`

	// setup
	Tenant     string          `json:"tenant,omitempty"`
	Principal  string          `json:"principal,omitempty"`
	Principals []principalSpec `json:"principals,omitempty"`
	Spaces     []spaceSpec     `json:"spaces,omitempty"`
	Grants     []grantSpec     `json:"grants,omitempty"`

	// turn
	TenantID      string   `json:"tenant_id,omitempty"`
	RoomID        string   `json:"room_id,omitempty"`
	AgentID       string   `json:"agent_id,omitempty"`
	Profile       string   `json:"profile,omitempty"`
	WorkDir       string   `json:"work_dir,omitempty"`
	Allowlist     []string `json:"allowlist,omitempty"`
	Provider      string   `json:"provider,omitempty"`
	Model         string   `json:"model,omitempty"`
	SharedSpace   string   `json:"shared_space,omitempty"`
	PrivateSpace  string   `json:"private_space,omitempty"`
	RoomInput     string   `json:"room_input,omitempty"`
	HumanID       string   `json:"human_id,omitempty"`
	PromptID      string   `json:"prompt_id,omitempty"`
	PiBinary      string   `json:"pi_binary,omitempty"`
	ExtensionPath string   `json:"extension_path,omitempty"`
	TimeoutSecs   int      `json:"timeout_secs,omitempty"`

	// drain
	Room string `json:"room,omitempty"`

	// explore (memory-agent tool plane)
	RequestID  string   `json:"request_id,omitempty"`
	IdemKey    string   `json:"idem_key,omitempty"`
	SpaceIDs   []string `json:"space_ids,omitempty"`
	Query      string   `json:"query,omitempty"`
	MaxSteps   int      `json:"max_steps,omitempty"`
	MaxResults int      `json:"max_results,omitempty"`
	Submit     bool     `json:"submit,omitempty"`
	Summary    string   `json:"summary,omitempty"`

	// recover
	Scenario json.RawMessage `json:"scenario,omitempty"`

	// tracer
	OrdinaryAgentID string `json:"ordinary_agent_id,omitempty"`
	MemoryAgentID   string `json:"memory_agent_id,omitempty"`
	FirstQuestion   string `json:"first_question,omitempty"`
	SecondQuestion  string `json:"second_question,omitempty"`
	MemQuestion     string `json:"memory_question,omitempty"`
}

type response struct {
	OK      bool          `json:"ok"`
	Error   string        `json:"error,omitempty"`
	Result  resultPayload `json:"result"`
}

type resultPayload struct {
	Turn    json.RawMessage `json:"turn,omitempty"`
	Batches []string        `json:"batches,omitempty"`
	Explore json.RawMessage `json:"explore,omitempty"`
	Recover json.RawMessage `json:"recover,omitempty"`
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

func main() {
	extDir, err := os.MkdirTemp("", "gmtest-host-ext-")
	if err != nil {
		fatal("temp dir: %v", err)
	}
	extensionPath, err := roombridge.WriteTo(filepath.Join(extDir, "extension"))
	if err != nil {
		fatal("room bridge extension: %v", err)
	}
	session := runtime.NewSession()

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	out := bufio.NewWriter(os.Stdout)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			writeLine(out, response{OK: false, Error: fmt.Sprintf("bad request line: %v", err)})
			continue
		}
		res, err := dispatch(session, extensionPath, &req)
		if err != nil {
			writeLine(out, response{OK: false, Error: err.Error()})
			continue
		}
		writeLine(out, response{OK: true, Result: res})
	}
	if err := scanner.Err(); err != nil {
		fatal("stdin: %v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gmtest-host: "+format+"\n", args...)
	os.Exit(1)
}

func writeLine(out *bufio.Writer, resp response) {
	data, err := json.Marshal(resp)
	if err != nil {
		data = []byte(`{"ok":false,"error":"response marshal failed"}`)
	}
	out.Write(data)
	out.WriteByte('\n')
	out.Flush()
}

func dispatch(session *runtime.Session, extensionPath string, req *request) (resultPayload, error) {
	ctx := context.Background()
	switch req.Op {
	case "setup":
		return opSetup(ctx, req)
	case "turn":
		return opTurn(ctx, session, extensionPath, req)
	case "drain":
		batches, err := session.DrainEvidence(ctx, runtime.DrainRequest{
			RoomID: req.Room, MemoryBaseURL: req.MemoryURL, MemoryAuthToken: req.Token,
		})
		if err != nil {
			return resultPayload{}, fmt.Errorf("drain: %w", err)
		}
		return resultPayload{Batches: batches}, nil
	case "explore":
		return opExplore(ctx, req)
	case "recover":
		return opRecover(ctx, session, req)
	case "tracer":
		return opTracer(ctx, req)
	default:
		return resultPayload{}, fmt.Errorf("unknown op %q", req.Op)
	}
}

func opSetup(ctx context.Context, req *request) (resultPayload, error) {
	client := memoryclient.NewClient(req.MemoryURL, req.Token, &http.Client{Timeout: 5 * time.Second}, 1<<20)
	if _, err := client.InitializeTenant(ctx, ports.InitializeTenantRequest{
		TenantID: req.Tenant, DisplayName: "gmtest tenant " + req.Tenant, BootstrapPrincipalID: req.Principal,
	}); err != nil {
		return resultPayload{}, fmt.Errorf("InitializeTenant: %w", err)
	}
	for _, p := range req.Principals {
		if _, err := client.RegisterPrincipal(ctx, ports.RegisterPrincipalRequest{
			PrincipalID: p.ID, Kind: p.Kind, DisplayName: p.DisplayName,
		}); err != nil {
			return resultPayload{}, fmt.Errorf("RegisterPrincipal %s: %w", p.ID, err)
		}
	}
	for _, s := range req.Spaces {
		if _, err := client.RegisterSpace(ctx, ports.RegisterSpaceRequest{
			SpaceID: s.ID, Scope: s.Scope, OwnerPrincipalID: s.Owner, DisplayName: "gmtest " + s.ID,
		}); err != nil {
			return resultPayload{}, fmt.Errorf("RegisterSpace %s: %w", s.ID, err)
		}
	}
	for _, g := range req.Grants {
		if _, err := client.RegisterGrant(ctx, ports.RegisterGrantRequest{
			GrantID: g.ID, PrincipalID: g.Principal, SpaceIDs: g.SpaceIDs,
			Purpose: g.Purpose, Operations: g.Operations, ExpiresAt: g.ExpiresAt,
		}); err != nil {
			return resultPayload{}, fmt.Errorf("RegisterGrant %s: %w", g.ID, err)
		}
	}
	return resultPayload{}, nil
}

func opTurn(ctx context.Context, session *runtime.Session, extensionPath string, req *request) (resultPayload, error) {
	if req.ExtensionPath != "" {
		extensionPath = req.ExtensionPath
	}
	timeout := time.Duration(req.TimeoutSecs) * time.Second
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	turn, err := session.Turn(ctx, runtime.TurnRequest{
		Authority: runtime.ExecutionAuthority{
			TenantID: req.TenantID, RoomID: req.RoomID, AgentID: req.AgentID,
			ProfileKind: req.Profile, WorkingDirectory: req.WorkDir,
			EnvironmentAllowlist: req.Allowlist, Provider: req.Provider, Model: req.Model,
			SharedSpaceID: req.SharedSpace, PrivateSpaceID: req.PrivateSpace,
		},
		RoomInput: req.RoomInput, HumanMessageID: req.HumanID,
		MemoryBaseURL: req.MemoryURL, MemoryAuthToken: req.Token,
		PiBinary: req.PiBinary, PiExtensionPath: extensionPath, PromptRequestID: req.PromptID,
	})
	if err != nil {
		return resultPayload{}, fmt.Errorf("turn: %w", err)
	}
	data, err := json.Marshal(turn)
	if err != nil {
		return resultPayload{}, fmt.Errorf("marshal turn: %w", err)
	}
	return resultPayload{Turn: data}, nil
}

func opExplore(ctx context.Context, req *request) (resultPayload, error) {
	client := memoryclient.NewClient(req.MemoryURL, req.Token, &http.Client{Timeout: 5 * time.Second}, 1<<20)
	start, err := client.StartExploration(ctx, ports.StartExplorationRequest{
		RequestID: req.RequestID, IdempotencyKey: req.IdemKey,
		SpaceIDs: req.SpaceIDs, Query: req.Query, MaxSteps: req.MaxSteps, MaxResults: req.MaxResults,
	})
	if err != nil {
		return resultPayload{}, fmt.Errorf("StartExploration: %w", err)
	}
	payload := map[string]any{"start": start}
	if req.Submit && len(start.Items) > 0 {
		submitted, err := client.Submit(ctx, start.SessionID, ports.SubmitRequest{
			OperationID: req.IdemKey + "-submit", Found: req.Submit,
			Summary: req.Summary, CitationIDs: []string{start.Items[0].Citation.CitationID},
		})
		if err != nil {
			return resultPayload{}, fmt.Errorf("Submit: %w", err)
		}
		payload["submit"] = submitted
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return resultPayload{}, fmt.Errorf("marshal explore: %w", err)
	}
	return resultPayload{Explore: data}, nil
}

func opTracer(ctx context.Context, req *request) (resultPayload, error) {
	scenario := runtime.TracerScenario{
		IDs: runtime.TracerIDs{
			TenantID: req.TenantID, RoomID: req.RoomID,
			OrdinaryAgentID: req.OrdinaryAgentID, MemoryAgentID: req.MemoryAgentID,
			SharedSpaceID: req.SharedSpace, PrivateSpaceID: req.PrivateSpace,
			FirstPromptRequestID: req.PromptID + "-1", SecondPromptRequestID: req.PromptID + "-2",
			MemoryPromptRequestID: req.PromptID + "-mem",
		},
		Authority: runtime.ExecutionAuthority{
			TenantID: req.TenantID, RoomID: req.RoomID, AgentID: req.OrdinaryAgentID,
			ProfileKind: "ordinary", WorkingDirectory: req.WorkDir,
			EnvironmentAllowlist: req.Allowlist, Provider: req.Provider, Model: req.Model,
			SharedSpaceID: req.SharedSpace, PrivateSpaceID: req.PrivateSpace,
		},
		MemoryBaseURL: req.MemoryURL, MemoryAuthToken: req.Token,
		PiBinary: req.PiBinary, FirstQuestion: req.FirstQuestion,
		SecondQuestion: req.SecondQuestion, MemoryQuestion: req.MemQuestion,
	}
	result, err := runtime.RunTracer(ctx, scenario)
	if err != nil {
		return resultPayload{}, fmt.Errorf("tracer: %w", err)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return resultPayload{}, fmt.Errorf("marshal tracer: %w", err)
	}
	return resultPayload{Explore: data}, nil
}

func opRecover(ctx context.Context, session *runtime.Session, req *request) (resultPayload, error) {
	var scenario runtime.RecoveryScenario
	if err := json.Unmarshal(req.Scenario, &scenario); err != nil {
		return resultPayload{}, fmt.Errorf("scenario decode: %w", err)
	}
	result, err := session.RecoverAfterRestart(ctx, scenario)
	if err != nil {
		return resultPayload{}, fmt.Errorf("recover: %w", err)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return resultPayload{}, fmt.Errorf("marshal recover: %w", err)
	}
	return resultPayload{Recover: data}, nil
}
