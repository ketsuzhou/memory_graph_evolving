package memoryclient

import (
	"context"
	"encoding/json"
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

// TriggerConsolidationCutRequest is the trigger body. The Idempotency-Key is
// a header, not a field, and is scoped to (tenant, room, trigger_source, key).
type TriggerConsolidationCutRequest struct {
	Mode          string `json:"mode"`
	TriggerSource string `json:"trigger_source"`
}

// decodeCutFlatError decodes the consolidation-cut surface's flat Error
// envelope ({"code","message"} — no request_id wrapper) that the shared
// transport's wrapped decoder would miss.
func decodeCutFlatError(status int, raw []byte) error {
	var flat struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &flat)
	return &ProtocolError{StatusCode: status, Code: flat.Code, Message: flat.Message}
}

// cutRequest performs one consolidation-cut call: the shared transport with
// the cut surface's flat error envelope.
func (c *Client) cutRequest(ctx context.Context, method, path, idempotencyKey string, body any, out any) error {
	return c.send(ctx, method, path, idempotencyKey, decodeCutFlatError, body, out)
}

// TriggerConsolidationCut freezes one room (SC-8.1): the same key with the
// same body replays the original job instead of freezing again. The local
// freezer resolves the freeze synchronously, so its create response already
// carries the frozen job's space_scopes; an async deployment may answer
// queued or freezing — poll ConsolidationCut for the authoritative status.
func (c *Client) TriggerConsolidationCut(ctx context.Context, roomID, idempotencyKey string, request TriggerConsolidationCutRequest) (CutJob, error) {
	var job CutJob
	err := c.cutRequest(ctx, http.MethodPost, "/v1/rooms/"+roomID+"/consolidation-cuts", idempotencyKey, request, &job)
	return job, err
}

// ConsolidationCut reads the authoritative job status (webhooks only notify).
func (c *Client) ConsolidationCut(ctx context.Context, cutID string) (CutJob, error) {
	var job CutJob
	err := c.cutRequest(ctx, http.MethodGet, "/v1/consolidation-cuts/"+cutID, "", nil, &job)
	return job, err
}
