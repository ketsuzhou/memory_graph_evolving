// skill_evolution.go is the GMS-207/GMS-206 transport adapter: the private
// skill evolution surface exposing the exact materialization closure read
// (POST /v1/skill-evolution/closure:read, consumed by the RSIH materializer,
// Contract §6.2.1/§13.7, GMS §12.7, RSIH §4.2) and the three v1 Memory tool
// routes (POST /v1/skill-evolution/tools/{memory_explore,memory_expand,
// skill_get}) carrying the exact Contract §7.17 ToolProxyRequest and the
// closed upstream success payload + read audit (Contract §7.18, GMS §10.5).
//
// It is deliberately a standalone http.Handler (NewSkillEvolutionHandler) so
// the composition root can mount it beside the Memory Protocol handler
// without either surface growing knowledge of the other; the wire envelope,
// strict decode and bearer auth mirror the Memory Protocol rules exactly.
package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/materializationread"
	"river2.dev/graph-memory-service/internal/skillevolution/retrieval"
)

// SkillEvolutionClosureReadPath is the GMS-207 closure route.
const SkillEvolutionClosureReadPath = "/v1/skill-evolution/closure:read"

// The GMS-206 Memory tool routes (Contract §5.6/§7.17; closed tool set v1).
const (
	SkillEvolutionToolExplorePath  = "/v1/skill-evolution/tools/memory_explore"
	SkillEvolutionToolExpandPath   = "/v1/skill-evolution/tools/memory_expand"
	SkillEvolutionToolSkillGetPath = "/v1/skill-evolution/tools/skill_get"

	// ToolProxyRequestSchemaVersion is the frozen Host wire envelope
	// (Contract §7.17).
	ToolProxyRequestSchemaVersion = "host.tool-proxy-request.v1"
	// ToolUpstreamResponseSchemaVersion is the GMS success envelope carrying
	// the exact upstream typed payload + read audit for the Host adapter
	// (HST-201 Upstream port; Contract §7.18).
	ToolUpstreamResponseSchemaVersion = "gms.tool-upstream-response.v1"
)

// SkillEvolutionDependencies wires the skill evolution transport. Token is
// the same deployment bearer-token model as the Memory Protocol handler;
// Reader is the closure read service (materializationread.NewService);
// Retrieval is the GMS-206 tools service (retrieval.NewService; optional —
// the tool routes fail closed with 500 INTERNAL when it is not wired).
type SkillEvolutionDependencies struct {
	Token     string
	Reader    *materializationread.Service
	Retrieval *retrieval.Service
}

// SkillEvolutionRoutes lists the routes served by NewSkillEvolutionHandler
// (kept in the Route shape of Routes() so a composition root can merge the
// two route tables).
func SkillEvolutionRoutes() []Route {
	return []Route{
		{Method: http.MethodPost, Path: SkillEvolutionClosureReadPath},
		{Method: http.MethodPost, Path: SkillEvolutionToolExplorePath},
		{Method: http.MethodPost, Path: SkillEvolutionToolExpandPath},
		{Method: http.MethodPost, Path: SkillEvolutionToolSkillGetPath},
	}
}

// NewSkillEvolutionHandler builds the standalone skill evolution transport.
// The composition root mounts it (e.g. mux.Handle over SkillEvolutionRoutes())
// next to the Memory Protocol handler. A nil Reader is a wiring bug and fails
// fast; a nil Retrieval only disables the tool routes (the GMS-206 surface
// can be deployed after GMS-207 without touching the closure contract).
func NewSkillEvolutionHandler(deps SkillEvolutionDependencies) http.Handler {
	if deps.Reader == nil {
		panic("httpapi: NewSkillEvolutionHandler requires a materializationread.Service")
	}
	return &skillEvolutionHandler{
		auth:   authz.NewAuthenticator(deps.Token),
		reader: deps.Reader,
		tools:  deps.Retrieval,
	}
}

type skillEvolutionHandler struct {
	auth   *authz.Authenticator
	reader *materializationread.Service
	tools  *retrieval.Service
}

func (h *skillEvolutionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, err := h.auth.Authenticate(r.Header.Get("Authorization")); err != nil {
		writeError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid bearer token", nil)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "unknown skill evolution route", nil)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Content-Type must be application/json", nil)
		return
	}
	body, _ := readBody(r)
	switch r.URL.Path {
	case SkillEvolutionClosureReadPath:
		object, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			writeClosureInvalidRequest(w, body, decodeErr)
			return
		}
		h.handleClosureRead(w, r, object)
	case SkillEvolutionToolExplorePath, SkillEvolutionToolExpandPath, SkillEvolutionToolSkillGetPath:
		object, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			writeClosureInvalidRequest(w, body, decodeErr)
			return
		}
		h.handleTool(w, r, object)
	default:
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "unknown skill evolution route", nil)
	}
}

// handleClosureRead decodes one closure read request strictly, hands the exact
// roots to the closure service and renders the deterministic response. Every
// service failure surfaces its closed reason code; the HTTP status is derived
// from the registry semantics of that code (Contract §13.7.1):
// ACTIVATION_SEQUENCE_CONFLICT carries retry_scope=new_attempt → 409;
// NO_ACTIVE_HEAD is resource absence → 404; every other closed semantic code
// → 422; a codeless failure is never shown to the caller (500 INTERNAL).
func (h *skillEvolutionHandler) handleClosureRead(w http.ResponseWriter, r *http.Request, object strictObject) {
	if details := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "root_skill_refs": true, "max_nodes": true,
	}); details != nil {
		writeClosureInvalidRequest(w, nil, details)
		return
	}
	var errs []fieldDetail
	var requestID string
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)

	var rootRaws []json.RawMessage
	if raw, ok := object["root_skill_refs"]; ok {
		if err := json.Unmarshal(raw, &rootRaws); err != nil {
			markJSONError(&errs)
		} else if len(rootRaws) == 0 {
			addFieldError(&errs, "root_skill_refs", "root_skill_refs must contain at least one exact ref")
		}
	} else {
		addFieldError(&errs, "root_skill_refs", "required field is missing")
	}

	maxNodes := 0
	if raw, ok := object["max_nodes"]; ok {
		if err := json.Unmarshal(raw, &maxNodes); err != nil || maxNodes < 1 {
			addFieldError(&errs, "max_nodes", "max_nodes must be a positive integer when present")
		}
	}
	if len(errs) > 0 {
		writeClosureInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	request := materializationread.ReadRequest{MaxNodes: maxNodes}
	for i, raw := range rootRaws {
		// Integer fields must survive as exact json.Number (Contract §6.3:
		// integer-only hashed core) — a float64 decode would corrupt version
		// comparisons beyond 2^53 and drop the integer shape entirely.
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var obj map[string]any
		if err := decoder.Decode(&obj); err != nil || obj == nil {
			writeClosureInvalidRequest(w, nil, invalidRequestFrom([]fieldDetail{
				{Field: "root_skill_refs", Reason: "each root_skill_refs entry must be a JSON object"},
			}))
			return
		}
		ref, err := materializationread.ParseRootRef(obj)
		if err != nil {
			code := materializationread.CodeOf(err)
			if code == "" {
				code = "INVALID_REQUEST"
			}
			writeClosureError(w, requestID, closureReadStatus(code), code, err.Error(), []fieldDetail{{
				Field:  fmt.Sprintf("root_skill_refs[%d]", i),
				Reason: code,
			}})
			return
		}
		request.Roots = append(request.Roots, ref)
	}

	closure, err := h.reader.Read(r.Context(), request)
	if err != nil {
		code := materializationread.CodeOf(err)
		status := closureReadStatus(code)
		if status == http.StatusInternalServerError {
			writeError(w, r, status, "INTERNAL", "internal service failure", nil)
			return
		}
		writeClosureError(w, requestID, status, code, err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, closureReadResponse(requestID, closure))
}

// closureReadStatus maps a closed reason code to its HTTP status from the
// frozen registry semantics (meaning is taken verbatim from the registry; the
// transport only chooses the status class).
func closureReadStatus(code string) int {
	switch code {
	case materializationread.ReasonActivationSequenceConflict:
		// registry: retry_scope=new_attempt, retryable=false (Contract §13.4)
		return http.StatusConflict
	case materializationread.ReasonNoActiveHead:
		return http.StatusNotFound
	case "":
		return http.StatusInternalServerError
	default:
		return http.StatusUnprocessableEntity
	}
}

// closureReadResponse renders the deterministic closure document.
func closureReadResponse(requestID string, closure *materializationread.Closure) map[string]any {
	nodes := make([]any, 0, len(closure.Nodes))
	for _, node := range closure.Nodes {
		deps := make([]any, 0, len(node.Dependencies))
		for _, dep := range node.Dependencies {
			deps = append(deps, materializationread.SkillRefDoc(dep))
		}
		permissions := make([]any, 0, len(node.Permissions))
		for _, permission := range node.Permissions {
			permissions = append(permissions, map[string]any{
				"capability": permission.Capability,
				"scope":      permission.Scope,
			})
		}
		nodes = append(nodes, map[string]any{
			"skill_ref":              materializationread.SkillRefDoc(node.Ref),
			"kind":                   node.Kind,
			"artifact_digest":        node.ArtifactDigest,
			"canonical_bytes_base64": base64.StdEncoding.EncodeToString(node.CanonicalBytes),
			"input_port_schema":      materializationread.PortSchemaDoc(node.InputPort),
			"output_port_schema":     materializationread.PortSchemaDoc(node.OutputPort),
			"permissions":            permissions,
			"dependency_refs":        deps,
		})
	}
	response := map[string]any{
		"schema_version":         closure.SchemaVersion,
		"request_id":             requestID,
		"root_skill_refs":        closure.RootRefDocs(),
		"closure_nodes":          nodes,
		"activation_sequence":    json.Number(strconv.FormatUint(closure.ActivationSequence, 10)),
		"activation_head_digest": closure.ActivationHeadDigest,
		"torn_read_token":        closure.TornReadToken,
	}
	if closure.Watermark != nil {
		response["projection_watermark"] = closure.Watermark
	}
	return response
}

// writeClosureError emits the standard wire error envelope, echoing a valid
// request_id so the caller can correlate the failure.
func writeClosureError(w http.ResponseWriter, requestID string, status int, code, message string, details []fieldDetail) {
	if requestID == "" || byteLen(requestID) > 128 {
		requestID = newRequestID()
	}
	if details == nil {
		details = []fieldDetail{}
	}
	writeJSON(w, status, wireError{Error: wireErrorBody{
		Code: code, Message: message, RequestID: requestID, Details: details,
	}})
}

// writeClosureInvalidRequest mirrors Handler.writeInvalidRequest for the
// standalone skill evolution surface (400 INVALID_REQUEST / INVALID_JSON).
func writeClosureInvalidRequest(w http.ResponseWriter, body []byte, request *invalidRequest) {
	var requestID string
	if body != nil {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(body, &object); err == nil {
			if raw, ok := object["request_id"]; ok {
				_ = json.Unmarshal(raw, &requestID)
			}
		}
	}
	if requestID == "" || byteLen(requestID) > 128 {
		requestID = newRequestID()
	}
	code := "INVALID_REQUEST"
	if request.jsonError {
		code = "INVALID_JSON"
	}
	details := request.fields
	if details == nil {
		details = []fieldDetail{}
	}
	writeJSON(w, http.StatusBadRequest, wireError{Error: wireErrorBody{
		Code: code, Message: "request violates the skill evolution closure read contract",
		RequestID: requestID, Details: details,
	}})
}

// ---------------------------------------------------------------------------
// GMS-206 Memory tool routes (Contract §7.17/§7.18, GMS §10.5)
// ---------------------------------------------------------------------------

// handleTool decodes one Contract §7.17 ToolProxyRequest strictly (closed
// field set, integer-only numbers), dispatches to the bound tool of the
// closed v1 set and renders the exact upstream payload + read audit. Every
// retrieval failure surfaces its closed reason code; the HTTP status is
// derived from the frozen registry semantics of that code:
// PROJECTION_BEHIND_REQUIRED_SEQUENCE is retryable same-request → 503;
// EXPLORE_FENCE_CONFLICT is a state conflict → 409; NO_ACTIVE_HEAD is
// resource absence → 404; every other closed semantic code → 422; a codeless
// failure is never shown to the caller (500 INTERNAL).
func (h *skillEvolutionHandler) handleTool(w http.ResponseWriter, r *http.Request, object strictObject) {
	if h.tools == nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL", "tool surface is not wired", nil)
		return
	}
	request, proxyRequestID, errs := decodeToolProxyRequest(object)
	if len(errs) > 0 {
		writeClosureInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	var response *retrieval.Response
	var err error
	switch request.ToolName {
	case retrieval.ToolExplore:
		response, err = h.tools.Explore(r.Context(), *request)
	case retrieval.ToolExpand:
		response, err = h.tools.Expand(r.Context(), *request)
	case retrieval.ToolSkillGet:
		response, err = h.tools.SkillGet(r.Context(), *request)
	default:
		writeClosureError(w, proxyRequestID, http.StatusUnprocessableEntity,
			retrieval.ReasonToolUnsupported, "tool_name is not bound in the closed v1 set", nil)
		return
	}
	if err != nil {
		code := retrieval.CodeOf(err)
		status := toolStatus(code)
		if status == http.StatusInternalServerError {
			writeError(w, r, status, "INTERNAL", "internal service failure", nil)
			return
		}
		writeClosureError(w, proxyRequestID, status, code, err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version":         ToolUpstreamResponseSchemaVersion,
		"proxy_request_id":       proxyRequestID,
		"tool_name":              request.ToolName,
		"status":                 response.Status,
		"result":                 response.Result,
		"read_audit":             response.ReadAudit,
		"upstream_result_digest": response.UpstreamResultDigest,
	})
}

// decodeToolProxyRequest decodes the closed §7.17 envelope. Integer fields
// survive as exact json.Number (Contract §6.3); unknown fields, wrong
// schema_version and non-object arguments are 400 INVALID_REQUEST.
func decodeToolProxyRequest(object strictObject) (*retrieval.ToolRequest, string, []fieldDetail) {
	if details := object.rejectUnknownFields(map[string]bool{
		"schema_version": true, "proxy_request_id": true, "room_id": true,
		"agent_id": true, "delivery_id": true, "tool_name": true,
		"arguments": true, "scope_profile_ref": true, "idempotency_key": true,
		"requested_min_activation_sequence": true, "timeout_millis": true,
	}); details != nil {
		return nil, "", details.fields
	}
	var errs []fieldDetail
	if raw, present := object["schema_version"]; present {
		var schemaVersion string
		if err := json.Unmarshal(raw, &schemaVersion); err != nil || schemaVersion != ToolProxyRequestSchemaVersion {
			addFieldError(&errs, "schema_version", "schema_version must be "+ToolProxyRequestSchemaVersion)
		}
	} else {
		addFieldError(&errs, "schema_version", "required field is missing")
	}

	request := &retrieval.ToolRequest{}
	object.requireString("proxy_request_id", &request.ProxyRequestID, &errs)
	validateID(&errs, "proxy_request_id", request.ProxyRequestID)
	object.requireString("room_id", &request.RoomID, &errs)
	validateID(&errs, "room_id", request.RoomID)
	object.requireString("agent_id", &request.AgentID, &errs)
	validateID(&errs, "agent_id", request.AgentID)
	object.requireString("delivery_id", &request.DeliveryID, &errs)
	validateID(&errs, "delivery_id", request.DeliveryID)
	object.requireString("tool_name", &request.ToolName, &errs)
	object.requireString("idempotency_key", &request.IdempotencyKey, &errs)

	if raw, present := object["arguments"]; present {
		// Arguments must stay an integer-preserving JSON object: the query
		// identity digest and every closed DTO inside are integer-only.
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var arguments map[string]any
		if err := decoder.Decode(&arguments); err != nil || arguments == nil {
			addFieldError(&errs, "arguments", "arguments must be a JSON object")
		} else {
			request.Arguments = arguments
		}
	} else {
		addFieldError(&errs, "arguments", "required field is missing")
	}

	if raw, present := object["scope_profile_ref"]; present {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var scopeDoc map[string]any
		if err := decoder.Decode(&scopeDoc); err != nil || scopeDoc == nil {
			addFieldError(&errs, "scope_profile_ref", "scope_profile_ref must be a JSON object")
		} else if ref, err := contract.ParseVersionedRef(scopeDoc); err != nil {
			addFieldError(&errs, "scope_profile_ref", "scope_profile_ref must be an exact versioned ref: "+err.Error())
		} else {
			request.ScopeProfile = ref
		}
	} else {
		addFieldError(&errs, "scope_profile_ref", "required field is missing")
	}

	if raw, present := object["requested_min_activation_sequence"]; present {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var number json.Number
		if err := decoder.Decode(&number); err != nil || !contract.IsIntegerNumber(number) {
			addFieldError(&errs, "requested_min_activation_sequence", "requested_min_activation_sequence must be an integer when present")
		} else if value, err := strconv.ParseUint(number.String(), 10, 64); err != nil {
			addFieldError(&errs, "requested_min_activation_sequence", "requested_min_activation_sequence out of range")
		} else {
			request.RequestedMinActivationSequence = value
			request.HasMinActivationSequence = true
		}
	}

	if raw, present := object["timeout_millis"]; present {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var number json.Number
		if err := decoder.Decode(&number); err != nil || !contract.IsIntegerNumber(number) {
			addFieldError(&errs, "timeout_millis", "timeout_millis must be an integer when present")
		} else if value, err := strconv.ParseUint(number.String(), 10, 64); err != nil {
			addFieldError(&errs, "timeout_millis", "timeout_millis out of range")
		} else {
			request.TimeoutMillis = value
		}
	}
	return request, request.ProxyRequestID, errs
}

// toolStatus maps a closed retrieval reason code to its HTTP status from the
// frozen registry semantics (the transport only chooses the status class).
func toolStatus(code string) int {
	switch code {
	case retrieval.ReasonProjectionBehindRequiredSequence:
		// registry: retryable, retry_scope=same_request — the projection is
		// catching up; the same request will succeed later (GMS §10.10).
		return http.StatusServiceUnavailable
	case retrieval.ReasonExploreFenceConflict:
		// A session/state conflict: the caller continues from the current
		// fences with a new attempt (Contract §12.1/§12.7.2 C3/C6).
		return http.StatusConflict
	case retrieval.ReasonNoActiveHead:
		return http.StatusNotFound
	case "":
		return http.StatusInternalServerError
	default:
		return http.StatusUnprocessableEntity
	}
}
