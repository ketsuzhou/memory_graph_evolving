package memoryclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// CutJob is the client view of the consolidation-cut job DTO. SpaceScopes is
// the immutable manifest's frozen per-space facts — the projection head
// version each bound space was frozen at — and is the data a GraphSnapshot
// vector is built from. SpaceResults carries a published_version only for
// spaces a terminal transition published.
type CutJob struct {
	CutID      string `json:"cut_id"`
	TenantID   string `json:"tenant_id"`
	RoomID     string `json:"room_id"`
	Stage      string `json:"stage"`
	JobVersion int64  `json:"job_version"`
	CutDigest  string `json:"cut_digest,omitempty"`
	Outcome    string `json:"outcome,omitempty"`

	SpaceResults []CutSpaceResult `json:"space_results"`
	SpaceScopes  []CutSpaceScope  `json:"space_scopes,omitempty"`

	FailureReasons []string `json:"failure_reasons,omitempty"`
	UpdatedAt      string   `json:"updated_at"`
}

type CutSpaceResult struct {
	SpaceID          string `json:"space_id"`
	Result           string `json:"result"`
	PublishedVersion *int64 `json:"published_version,omitempty"`
	Reason           string `json:"reason,omitempty"`
}

type CutSpaceScope struct {
	SpaceID               string `json:"space_id"`
	ProjectionHeadVersion *int64 `json:"projection_head_version,omitempty"`
	QueryWatermark        *int64 `json:"query_watermark,omitempty"`
}

// cutFlatError decodes the consolidation-cut surface's flat Error envelope
// ({"code","message"} — no request_id wrapper).
type cutFlatError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// cutRequest performs one bearer-authenticated consolidation-cut call. The cut
// surface rejects the shared transport's wrapped error envelope, so errors are
// decoded from the flat shape and surfaced as ProtocolError with the flat code.
func (c *Client) cutRequest(ctx context.Context, method, path, idempotencyKey string, body any, out any) error {
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
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
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
		var flat cutFlatError
		_ = json.Unmarshal(raw, &flat)
		return &ProtocolError{StatusCode: response.StatusCode, Code: flat.Code, Message: flat.Message}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, path, err)
	}
	return nil
}

// TriggerConsolidationCut freezes one room (SC-8.1): the Idempotency-Key is
// required and scoped to (tenant, room, trigger_source, key) — the same key
// with the same body replays the original frozen job. The returned job is the
// authoritative status; the freeze walks queued→freezing→frozen before the
// 202 lands, so a frozen job body already carries the space_scopes snapshot.
func (c *Client) TriggerConsolidationCut(ctx context.Context, roomID, idempotencyKey, mode, triggerSource string) (CutJob, error) {
	var job CutJob
	err := c.cutRequest(ctx, http.MethodPost, "/v1/rooms/"+roomID+"/consolidation-cuts", idempotencyKey,
		map[string]string{"mode": mode, "trigger_source": triggerSource}, &job)
	return job, err
}

// ConsolidationCut reads the authoritative job status (webhooks only notify).
func (c *Client) ConsolidationCut(ctx context.Context, cutID string) (CutJob, error) {
	var job CutJob
	err := c.cutRequest(ctx, http.MethodGet, "/v1/consolidation-cuts/"+cutID, "", nil, &job)
	return job, err
}
