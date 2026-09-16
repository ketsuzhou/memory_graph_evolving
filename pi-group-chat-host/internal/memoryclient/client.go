package memoryclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"river2.dev/pi-group-chat-host/internal/ports"
)

// ProtocolError is a non-2xx Memory Protocol response decoded from the
// {error:{code,message}} envelope.
type ProtocolError struct {
	StatusCode int
	Code       string
	Message    string
	RequestID  string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("memory protocol error: HTTP %d %s: %s", e.StatusCode, e.Code, e.Message)
}

type Client struct {
	baseURL          string
	token            string
	httpClient       *http.Client
	maxResponseBytes int64
}

func NewClient(baseURL, token string, httpClient *http.Client, maxResponseBytes int64) *Client {
	return &Client{baseURL: strings.TrimSuffix(baseURL, "/"), token: token, httpClient: httpClient, maxResponseBytes: maxResponseBytes}
}

func New(baseURL, token string, httpClient *http.Client, maxResponseBytes int64) *Client {
	return NewClient(baseURL, token, httpClient, maxResponseBytes)
}

// errorEnvelopeDecoder decodes one surface's non-2xx body into an error. The
// memory protocol wraps its envelope as {error:{code,message,request_id}};
// the consolidation-cut surface uses a flat {code,message}.
type errorEnvelopeDecoder func(status int, raw []byte) error

func decodeProtocolEnvelope(status int, raw []byte) error {
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	_ = json.Unmarshal(raw, &envelope)
	return &ProtocolError{StatusCode: status, Code: envelope.Error.Code, Message: envelope.Error.Message, RequestID: envelope.Error.RequestID}
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	return c.send(ctx, method, path, "", decodeProtocolEnvelope, body, out)
}

// send is the shared HTTP exchange behind every client surface: marshal the
// body, send it bearer-authenticated, cap the response read, decode non-2xx
// bodies through the surface's error envelope, and decode 2xx bodies into out.
func (c *Client) send(ctx context.Context, method, path, idempotencyKey string, decodeError errorEnvelopeDecoder, body any, out any) error {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode %s %s: %w", method, path, err)
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, payload)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return decodeError(response.StatusCode, raw)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}

func (c *Client) InitializeTenant(ctx context.Context, request InitializeTenantRequest) (InitializeTenantResponse, error) {
	var response InitializeTenantResponse
	err := c.do(ctx, http.MethodPut, "/v1/tenant", request, &response)
	return response, err
}

func (c *Client) RegisterPrincipal(ctx context.Context, request RegisterPrincipalRequest) (RegisterPrincipalResponse, error) {
	var response RegisterPrincipalResponse
	err := c.do(ctx, http.MethodPost, "/v1/principals", request, &response)
	return response, err
}

func (c *Client) RegisterSpace(ctx context.Context, request RegisterSpaceRequest) (RegisterSpaceResponse, error) {
	var response RegisterSpaceResponse
	err := c.do(ctx, http.MethodPost, "/v1/spaces", request, &response)
	return response, err
}

func (c *Client) RegisterGrant(ctx context.Context, request RegisterGrantRequest) (RegisterGrantResponse, error) {
	var response RegisterGrantResponse
	err := c.do(ctx, http.MethodPost, "/v1/grants", request, &response)
	return response, err
}

func (c *Client) StageEvidenceBatch(ctx context.Context, request StageEvidenceBatchRequest) (StageEvidenceBatchResponse, error) {
	var response StageEvidenceBatchResponse
	err := c.do(ctx, http.MethodPost, "/v1/evidence-batches:stage", request, &response)
	return response, err
}

func (c *Client) CommitEvidenceBatch(ctx context.Context, batchID string, request CommitEvidenceBatchRequest) (CommitEvidenceBatchResponse, error) {
	var response CommitEvidenceBatchResponse
	err := c.do(ctx, http.MethodPost, "/v1/evidence-batches/"+batchID+":commit", request, &response)
	return response, err
}

func (c *Client) EvidenceBatch(ctx context.Context, batchID string) (EvidenceBatchStatusResponse, error) {
	var response EvidenceBatchStatusResponse
	err := c.do(ctx, http.MethodGet, "/v1/evidence-batches/"+batchID, nil, &response)
	return response, err
}

// Recall never fails the turn on transport errors, timeouts, or Graph Memory
// outages: the adapter synthesizes a Host-side "unavailable" degradation with
// no items and never widens or substitutes the requested Spaces.
func (c *Client) Recall(ctx context.Context, request RecallRequest) (RecallResponse, error) {
	var response RecallResponse
	err := c.do(ctx, http.MethodPost, "/v1/recalls", request, &response)
	if err == nil {
		if response.Degradation.State == "" {
			response.Degradation.State = "empty"
		}
		return response, nil
	}
	if protocolErr, ok := err.(*ProtocolError); ok && protocolErr.StatusCode < 500 {
		return RecallResponse{}, err
	}
	return RecallResponse{
		RequestID:   request.RequestID,
		Degradation: RecallDegradation{State: "unavailable", Reasons: []string{fmt.Sprintf("memory service unavailable: %v", err)}},
	}, nil
}

func (c *Client) StartExploration(ctx context.Context, request StartExplorationRequest) (StartExplorationResponse, error) {
	var response StartExplorationResponse
	err := c.do(ctx, http.MethodPost, "/v1/explorations", request, &response)
	return response, err
}

func (c *Client) Explore(ctx context.Context, sessionID string, request ExploreRequest) (ExploreResponse, error) {
	var response ExploreResponse
	err := c.do(ctx, http.MethodPost, "/v1/explorations/"+sessionID+":explore", request, &response)
	return response, err
}

func (c *Client) Redirect(ctx context.Context, sessionID string, request RedirectRequest) (RedirectResponse, error) {
	var response RedirectResponse
	err := c.do(ctx, http.MethodPost, "/v1/explorations/"+sessionID+":redirect", request, &response)
	return response, err
}

func (c *Client) Submit(ctx context.Context, sessionID string, request SubmitRequest) (SubmitResponse, error) {
	var response SubmitResponse
	err := c.do(ctx, http.MethodPost, "/v1/explorations/"+sessionID+":submit", request, &response)
	return response, err
}

var _ ports.GraphMemoryClient = (*Client)(nil)
