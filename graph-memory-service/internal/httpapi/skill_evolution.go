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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/materializationread"
	"river2.dev/graph-memory-service/internal/skillevolution/retrieval"
	"river2.dev/graph-memory-service/internal/skillevolution/usageprojection"
)

// SkillEvolutionClosureReadPath is the GMS-207 closure route.
const SkillEvolutionClosureReadPath = "/v1/skill-evolution/closure:read"

const (
	SkillEvolutionInteractionRecordPath     = "/v1/skill-evolution/interactions:record"
	SkillEvolutionDiagnosisRecordPath       = "/v1/skill-evolution/diagnoses:record"
	SkillEvolutionAdvisoryReadPath          = "/v1/skill-evolution/advisory-candidates:read"
	SkillEvolutionUsageSummaryReadPath      = "/v1/skill-evolution/usage-summary:read"
	SkillEvolutionCandidateOutcomesReadPath = "/v1/skill-evolution/candidate-outcomes:read"
)

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

type AdvisoryReader interface {
	ListAdvisory(context.Context) ([]candidate.AdvisoryCandidate, error)
}

type CandidateOutcomesReader interface {
	CandidateOutcomes(context.Context, []contract.CandidateArtifactRef) ([]domain.CandidateLifecycleOutcome, error)
}

// AdvisoryVisibility applies governed probation envelopes to otherwise
// non-authoritative advisory candidates. Active runtime retrieval remains a
// separate Arm C authority.
// ProbationObserver consumes already-persisted observational records only to
// reconcile an existing governed candidate; it cannot admit or activate one.
type ProbationObserver interface {
	Observe(context.Context, usageprojection.UsageSubjectRef)
}

type AdvisoryVisibility interface {
	AdvisoryVisible(string, usageprojection.ContextProfile) bool
}

// SkillEvolutionDependencies wires the skill evolution transport. Token is
// the same deployment bearer-token model as the Memory Protocol handler;
// Reader is the closure read service (materializationread.NewService);
// Retrieval is the GMS-206 tools service (retrieval.NewService; optional —
// the tool routes fail closed with 500 INTERNAL when it is not wired).
type SkillEvolutionDependencies struct {
	Token          string
	Reader         *materializationread.Service
	Retrieval      *retrieval.Service
	Usage          *usageprojection.UsageProjectionService
	Advisory       AdvisoryReader
	Probation      AdvisoryVisibility
	Outcomes       CandidateOutcomesReader
	WriterIdentity string
}

// SkillEvolutionRoutes lists the routes served by NewSkillEvolutionHandler
// (kept in the Route shape of Routes() so a composition root can merge the
// two route tables).
func SkillEvolutionRoutes() []Route {
	return []Route{
		{Method: http.MethodPost, Path: SkillEvolutionClosureReadPath},
		{Method: http.MethodPost, Path: SkillEvolutionInteractionRecordPath},
		{Method: http.MethodPost, Path: SkillEvolutionDiagnosisRecordPath},
		{Method: http.MethodPost, Path: SkillEvolutionAdvisoryReadPath},
		{Method: http.MethodPost, Path: SkillEvolutionUsageSummaryReadPath},
		{Method: http.MethodPost, Path: SkillEvolutionCandidateOutcomesReadPath},
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
		auth:           authz.NewAuthenticator(deps.Token),
		reader:         deps.Reader,
		tools:          deps.Retrieval,
		usage:          deps.Usage,
		advisory:       deps.Advisory,
		probation:      deps.Probation,
		outcomes:       deps.Outcomes,
		writerIdentity: deps.WriterIdentity,
	}
}

type skillEvolutionHandler struct {
	auth           *authz.Authenticator
	reader         *materializationread.Service
	tools          *retrieval.Service
	usage          *usageprojection.UsageProjectionService
	advisory       AdvisoryReader
	probation      AdvisoryVisibility
	outcomes       CandidateOutcomesReader
	writerIdentity string
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
	case SkillEvolutionInteractionRecordPath:
		object, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			writeClosureInvalidRequest(w, body, decodeErr)
			return
		}
		h.handleInteractionRecord(w, r, object)
	case SkillEvolutionDiagnosisRecordPath:
		object, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			writeClosureInvalidRequest(w, body, decodeErr)
			return
		}
		h.handleDiagnosisRecord(w, r, object)
	case SkillEvolutionAdvisoryReadPath:
		object, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			writeClosureInvalidRequest(w, body, decodeErr)
			return
		}
		h.handleAdvisoryRead(w, r, object)
	case SkillEvolutionUsageSummaryReadPath:
		object, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			writeClosureInvalidRequest(w, body, decodeErr)
			return
		}
		h.handleUsageSummaryRead(w, r, object)
	case SkillEvolutionCandidateOutcomesReadPath:
		object, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			writeClosureInvalidRequest(w, body, decodeErr)
			return
		}
		h.handleCandidateOutcomesRead(w, r, object)
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

// ---------------------------------------------------------------------------
// Arm B producer and advisory routes
// ---------------------------------------------------------------------------

func (h *skillEvolutionHandler) armBReady(w http.ResponseWriter, r *http.Request) bool {
	if h.usage == nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL", "Arm B usage projection is not wired", nil)
		return false
	}
	if h.writerIdentity == "" {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL", "Arm B writer identity is not configured", nil)
		return false
	}
	return true
}

func (h *skillEvolutionHandler) handleInteractionRecord(w http.ResponseWriter, r *http.Request, object strictObject) {
	if !h.armBReady(w, r) {
		return
	}
	if details := object.rejectUnknownFields(map[string]bool{"request_id": true, "subject": true, "context_profile": true, "stage": true, "evidence_refs": true, "policy_ref": true, "model_ref": true, "environment_ref": true, "evaluation_batch_id": true, "diagnostic_only": true}); details != nil {
		writeClosureInvalidRequest(w, nil, details)
		return
	}
	requestID, interaction, errs := decodeInteraction(object, h.writerIdentity)
	if len(errs) > 0 {
		writeClosureInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	if err := h.usage.RecordInteraction(r.Context(), interaction); err != nil {
		writeClosureError(w, requestID, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"schema_version": "gms.arm-b-write.v1", "request_id": requestID, "recorded": true, "non_authoritative": true, "writer_identity": h.writerIdentity})
}

func (h *skillEvolutionHandler) handleDiagnosisRecord(w http.ResponseWriter, r *http.Request, object strictObject) {
	if !h.armBReady(w, r) {
		return
	}
	if details := object.rejectUnknownFields(map[string]bool{"request_id": true, "assessment_id": true, "subject": true, "context_profile": true, "returned_path_id": true, "addressed_agent_id": true, "adoption_evidence_refs": true, "contribution_score_micros": true, "confidence_micros": true, "counterevidence_refs": true, "rationale": true, "evidence_refs": true, "rubric_ref": true, "evaluation_batch_id": true, "diagnostic_only": true}); details != nil {
		writeClosureInvalidRequest(w, nil, details)
		return
	}
	requestID, assessment, errs := decodeDiagnosis(object, h.writerIdentity)
	if len(errs) > 0 {
		writeClosureInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	if err := h.usage.RecordDiagnosis(r.Context(), assessment); err != nil {
		writeClosureError(w, requestID, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"schema_version": "gms.arm-b-write.v1", "request_id": requestID, "recorded": true, "non_authoritative": true, "writer_identity": h.writerIdentity})
}

func (h *skillEvolutionHandler) handleAdvisoryRead(w http.ResponseWriter, r *http.Request, object strictObject) {
	if !h.armBReady(w, r) {
		return
	}
	if h.advisory == nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL", "advisory reader is not wired", nil)
		return
	}
	if details := object.rejectUnknownFields(map[string]bool{"request_id": true, "context_profile": true}); details != nil {
		writeClosureInvalidRequest(w, nil, details)
		return
	}
	var errs []fieldDetail
	var requestID string
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)
	profile, err := parseContextProfile(object["context_profile"])
	if err != nil {
		addFieldError(&errs, "context_profile", err.Error())
	}
	if len(errs) > 0 {
		writeClosureInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	summary, err := h.usage.UsageSummary(r.Context(), usageprojection.UsageSummaryRequest{ContextProfile: &profile, IncludeDiagnostic: false})
	if err != nil {
		writeClosureError(w, requestID, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	candidates, err := h.advisory.ListAdvisory(r.Context())
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL", "advisory reader failed", nil)
		return
	}
	out := []any{}
	for _, candidate := range candidates {
		if candidate.Kind != "human_procedure" && candidate.Kind != "step_guidance" {
			continue
		}
		if h.probation != nil && !h.probation.AdvisoryVisible(candidate.CandidateID, profile) {
			continue
		}
		out = append(out, map[string]any{"candidate_id": candidate.CandidateID, "candidate_ref": candidateRefDoc(candidate.Ref), "kind": candidate.Kind, "guidance": candidate.Guidance, "non_authoritative": true})
	}
	writeJSON(w, http.StatusOK, map[string]any{"schema_version": "gms.advisory-candidates.v1", "request_id": requestID, "non_authoritative": true, "usage_summary": usageSummaryDoc(summary), "candidates": out})
}

func decodeInteraction(object strictObject, writer string) (string, usageprojection.Interaction, []fieldDetail) {
	var errs []fieldDetail
	var requestID, stage, batch string
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)
	object.requireString("stage", &stage, &errs)
	subject, err := parseSubject(object["subject"])
	if err != nil {
		addFieldError(&errs, "subject", err.Error())
	}
	profile, err := parseContextProfile(object["context_profile"])
	if err != nil {
		addFieldError(&errs, "context_profile", err.Error())
	}
	evidence, err := parseEvidenceRefs(object["evidence_refs"])
	if err != nil {
		addFieldError(&errs, "evidence_refs", err.Error())
	}
	if raw, ok := object["evaluation_batch_id"]; ok {
		if err := json.Unmarshal(raw, &batch); err != nil || batch == "" {
			addFieldError(&errs, "evaluation_batch_id", "evaluation_batch_id must be a non-empty string when present")
		}
	}
	diagnostic := batch != ""
	if raw, ok := object["diagnostic_only"]; ok {
		requested := false
		if err := json.Unmarshal(raw, &requested); err != nil {
			addFieldError(&errs, "diagnostic_only", "diagnostic_only must be a boolean")
		} else if batch != "" && !requested {
			addFieldError(&errs, "diagnostic_only", "diagnostic_only cannot be false when evaluation_batch_id is present")
		} else if batch == "" {
			diagnostic = requested
		}
	}
	var policy, model, environment contract.VersionedRef
	if raw, ok := object["policy_ref"]; ok {
		policy, err = parseVersionedRef(raw)
		if err != nil {
			addFieldError(&errs, "policy_ref", err.Error())
		}
	}
	if raw, ok := object["model_ref"]; ok {
		model, err = parseVersionedRef(raw)
		if err != nil {
			addFieldError(&errs, "model_ref", err.Error())
		}
	}
	if raw, ok := object["environment_ref"]; ok {
		environment, err = parseVersionedRef(raw)
		if err != nil {
			addFieldError(&errs, "environment_ref", err.Error())
		}
	}
	return requestID, usageprojection.Interaction{Subject: subject, ContextProfile: profile, SourceLineageID: writer, Stage: usageprojection.InteractionStage(stage), EvidenceRefs: evidence, PolicyRef: policy, ModelRef: model, EnvironmentRef: environment, EvaluationBatchID: batch, DiagnosticOnly: diagnostic}, errs
}

func decodeDiagnosis(object strictObject, writer string) (string, usageprojection.DiagnosisUtilityAssessment, []fieldDetail) {
	var errs []fieldDetail
	var requestID, assessmentID, path, agent, rationale, batch string
	var score, confidence int64
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)
	object.requireString("assessment_id", &assessmentID, &errs)
	object.requireString("returned_path_id", &path, &errs)
	object.requireString("addressed_agent_id", &agent, &errs)
	object.requireString("rationale", &rationale, &errs)
	object.requireString("evaluation_batch_id", &batch, &errs)
	object.requireInt64("contribution_score_micros", &score, &errs)
	object.requireInt64("confidence_micros", &confidence, &errs)
	subject, err := parseSubject(object["subject"])
	if err != nil {
		addFieldError(&errs, "subject", err.Error())
	}
	profile, err := parseContextProfile(object["context_profile"])
	if err != nil {
		addFieldError(&errs, "context_profile", err.Error())
	}
	evidence, err := parseEvidenceRefs(object["evidence_refs"])
	if err != nil {
		addFieldError(&errs, "evidence_refs", err.Error())
	}
	adoption, err := parseEvidenceRefs(object["adoption_evidence_refs"])
	if err != nil {
		addFieldError(&errs, "adoption_evidence_refs", err.Error())
	}
	counter, err := parseEvidenceRefs(object["counterevidence_refs"])
	if err != nil {
		addFieldError(&errs, "counterevidence_refs", err.Error())
	}
	rubric, err := parseVersionedRef(object["rubric_ref"])
	if err != nil {
		addFieldError(&errs, "rubric_ref", err.Error())
	}
	diagnostic := batch != ""
	if raw, ok := object["diagnostic_only"]; ok {
		requested := false
		if err := json.Unmarshal(raw, &requested); err != nil {
			addFieldError(&errs, "diagnostic_only", "diagnostic_only must be a boolean")
		} else if batch != "" && !requested {
			addFieldError(&errs, "diagnostic_only", "diagnostic_only cannot be false when evaluation_batch_id is present")
		} else if batch == "" {
			diagnostic = requested
		}
	}
	return requestID, usageprojection.DiagnosisUtilityAssessment{AssessmentID: assessmentID, Subject: subject, ContextProfile: profile, SourceLineageID: writer, ReturnedPathID: path, AddressedAgentID: agent, AdoptionEvidenceRefs: adoption, ContributionScoreMicros: score, ConfidenceMicros: confidence, CounterevidenceRefs: counter, Rationale: rationale, EvidenceRefs: evidence, RubricRef: rubric, EvaluationBatchID: batch, DiagnosticOnly: diagnostic}, errs
}

func parseSubject(raw json.RawMessage) (usageprojection.UsageSubjectRef, error) {
	obj, err := decodeRawObject(raw)
	if err != nil {
		return usageprojection.UsageSubjectRef{}, err
	}
	if candidateRaw, ok := obj["candidate_ref"]; ok {
		ref, err := parseCandidateRef(candidateRaw)
		if err != nil {
			return usageprojection.UsageSubjectRef{}, err
		}
		return usageprojection.UsageSubjectRef{CandidateRef: &ref}, nil
	}
	if skillRaw, ok := obj["skill_ref"]; ok {
		ref, err := parseSkillRef(skillRaw)
		if err != nil {
			return usageprojection.UsageSubjectRef{}, err
		}
		return usageprojection.UsageSubjectRef{SkillRef: &ref}, nil
	}
	return usageprojection.UsageSubjectRef{}, fmt.Errorf("requires candidate_ref or skill_ref")
}
func parseContextProfile(raw any) (usageprojection.ContextProfile, error) {
	obj, err := objectValue(raw)
	if err != nil {
		return usageprojection.ContextProfile{}, err
	}
	var p usageprojection.ContextProfile
	p.SchemaVersion, _ = contract.AsString(obj["schema_version"])
	p.TaskFamily, _ = contract.AsString(obj["task_family"])
	p.RuntimeClass, _ = contract.AsString(obj["runtime_class"])
	p.EnvironmentClass, _ = contract.AsString(obj["environment_class"])
	p.WorkspaceFeatureTags = stringsFrom(obj["workspace_feature_tags"])
	p.ObservableGuardFacts = stringsFrom(obj["observable_guard_facts"])
	if rawRef, ok := obj["tool_policy_ref"]; ok {
		p.ToolPolicyRef, err = parseVersionedRef(rawRef)
		if err != nil {
			return p, err
		}
	}
	return p, nil
}
func parseEvidenceRefs(raw json.RawMessage) ([]contract.EvidenceRef, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	out := make([]contract.EvidenceRef, 0, len(values))
	for _, v := range values {
		obj, err := decodeRawObject(v)
		if err != nil {
			return nil, err
		}
		ref, err := contract.ParseEvidenceRef(obj)
		if err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, nil
}
func parseVersionedRef(raw any) (contract.VersionedRef, error) {
	obj, err := objectValue(raw)
	if err != nil {
		return contract.VersionedRef{}, err
	}
	return contract.ParseVersionedRef(obj)
}
func parseCandidateRef(raw any) (contract.CandidateArtifactRef, error) {
	obj, err := objectValue(raw)
	if err != nil {
		return contract.CandidateArtifactRef{}, err
	}
	return contract.ParseCandidateArtifactRef(obj)
}
func parseSkillRef(raw any) (contract.SkillArtifactRef, error) {
	obj, err := objectValue(raw)
	if err != nil {
		return contract.SkillArtifactRef{}, err
	}
	return contract.ParseSkillArtifactRef(obj)
}
func objectValue(value any) (map[string]any, error) {
	if obj, ok := value.(map[string]any); ok {
		return obj, nil
	}
	if raw, ok := value.(json.RawMessage); ok {
		return decodeRawObject(raw)
	}
	return nil, fmt.Errorf("must be an object")
}
func decodeRawObject(raw json.RawMessage) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var obj map[string]any
	if err := decoder.Decode(&obj); err != nil || obj == nil {
		return nil, fmt.Errorf("must be an object")
	}
	return obj, nil
}
func stringsFrom(raw any) []string {
	values, _ := contract.AsArray(raw)
	out := make([]string, 0, len(values))
	for _, v := range values {
		if s, ok := contract.AsString(v); ok {
			out = append(out, s)
		}
	}
	return out
}
func usageSummaryDoc(summary usageprojection.UsageSummary) map[string]any {
	entries := []any{}
	for _, entry := range summary.Entries {
		entries = append(entries, map[string]any{"subject": entry.Subject.CanonicalKey(), "contribution_score_micros": entry.AverageContributionScoreMicros, "confidence_micros": entry.AverageConfidenceMicros, "adopted_count": entry.AdoptedCount, "verified_count": entry.VerifiedCount, "outcome_correlated_count": entry.OutcomeCorrelatedCount, "counterevidence_count": entry.CounterevidenceCount})
	}
	return map[string]any{"policy_version": summary.PolicyVersion, "entries": entries}
}

const usageSummaryReadSchemaV1 = "gms.usage-summary-read.v1"

type usageSummaryRequest struct {
	RequestID string
	Profile   usageprojection.ContextProfile
	Subjects  []usageprojection.UsageSubjectRef
}

// handleUsageSummaryRead is the frozen C2 aggregate-only read. It calls the
// interaction-only projection method; diagnoses, raw evidence, rationale, and
// individual observations are structurally unavailable to this response.
func (h *skillEvolutionHandler) handleUsageSummaryRead(w http.ResponseWriter, r *http.Request, object strictObject) {
	if !h.armBReady(w, r) {
		return
	}
	request, errs := decodeUsageSummaryRequest(object)
	if len(errs) > 0 {
		writeClosureInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	summary, err := h.usage.InteractionOnlySummary(r.Context(), request.Profile, request.Subjects)
	if err != nil {
		writeClosureError(w, request.RequestID, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	tiers := map[string][]usageprojection.InteractionOnlyEntry{"active": {}, "probation": {}, "advisory": {}}
	for _, entry := range summary.Entries {
		tier := h.usageSummaryTier(r.Context(), entry.Subject, request.Profile)
		if tier == "suspended" {
			continue
		}
		tiers[tier] = append(tiers[tier], entry)
	}
	entries := []any{}
	aggregates := []any{}
	for _, tier := range []string{"active", "probation", "advisory"} {
		group := tiers[tier]
		adopted, verified, outcomes, reused := 0, 0, 0, 0
		for _, entry := range group {
			adopted += entry.AdoptedCount
			verified += entry.VerifiedCount
			outcomes += entry.OutcomeCorrelatedCount
			reused += entry.DeduplicatedReuseCount
			entries = append(entries, map[string]any{"authority_tier": tier, "subject_ref": usageSubjectDoc(entry.Subject), "independent_context_count": json.Number(strconv.Itoa(entry.IndependentContextCount)), "independent_lineage_count": json.Number(strconv.Itoa(entry.IndependentLineageCount)), "adopted_count": json.Number(strconv.Itoa(entry.AdoptedCount)), "verified_count": json.Number(strconv.Itoa(entry.VerifiedCount)), "outcome_correlated_count": json.Number(strconv.Itoa(entry.OutcomeCorrelatedCount)), "deduplicated_reuse_count": json.Number(strconv.Itoa(entry.DeduplicatedReuseCount))})
		}
		aggregates = append(aggregates, map[string]any{"authority_tier": tier, "subject_count": json.Number(strconv.Itoa(len(group))), "adopted_count": json.Number(strconv.Itoa(adopted)), "verified_count": json.Number(strconv.Itoa(verified)), "outcome_correlated_count": json.Number(strconv.Itoa(outcomes)), "deduplicated_reuse_count": json.Number(strconv.Itoa(reused))})
	}
	writeJSON(w, http.StatusOK, map[string]any{"schema_version": usageSummaryReadSchemaV1, "request_id": request.RequestID, "context_profile_digest": request.Profile.Digest(), "usage_policy_version": summary.PolicyVersion, "diagnostic_policy": "interaction_only", "authority_tier_aggregates": aggregates, "entries": entries})
}

func (h *skillEvolutionHandler) usageSummaryTier(ctx context.Context, subject usageprojection.UsageSubjectRef, profile usageprojection.ContextProfile) string {
	if subject.SkillRef != nil {
		if _, err := h.reader.Read(ctx, materializationread.ReadRequest{Roots: []contract.SkillArtifactRef{*subject.SkillRef}, MaxNodes: 1}); err == nil {
			return "active"
		}
	}
	// C2 is interaction-only. Probation transitions may be diagnosis-derived,
	// so candidates remain advisory rather than importing that influence.
	_ = profile
	return "advisory"
}

func decodeUsageSummaryRequest(object strictObject) (usageSummaryRequest, []fieldDetail) {
	if details := object.rejectUnknownFields(map[string]bool{"schema_version": true, "request_id": true, "context_profile": true, "subject_refs": true}); details != nil {
		return usageSummaryRequest{}, details.fields
	}
	var errs []fieldDetail
	var schema string
	object.requireString("schema_version", &schema, &errs)
	if schema != usageSummaryReadSchemaV1 {
		addFieldError(&errs, "schema_version", "schema_version must be "+usageSummaryReadSchemaV1)
	}
	request := usageSummaryRequest{}
	object.requireString("request_id", &request.RequestID, &errs)
	validateID(&errs, "request_id", request.RequestID)
	profileRaw, present := object["context_profile"]
	if !present {
		addFieldError(&errs, "context_profile", "required field is missing")
	} else if profile, err := decodeUsageSummaryProfile(profileRaw); err != nil {
		addFieldError(&errs, "context_profile", err.Error())
	} else {
		request.Profile = profile
	}
	subjectsRaw, present := object["subject_refs"]
	if !present {
		addFieldError(&errs, "subject_refs", "required field is missing")
	} else {
		var values []json.RawMessage
		if err := json.Unmarshal(subjectsRaw, &values); err != nil || len(values) == 0 {
			addFieldError(&errs, "subject_refs", "subject_refs must be a non-empty array of exact refs")
		} else {
			for index, raw := range values {
				subject, err := decodeUsageSummarySubject(raw)
				if err != nil {
					addFieldError(&errs, fmt.Sprintf("subject_refs[%d]", index), err.Error())
				} else {
					request.Subjects = append(request.Subjects, subject)
				}
			}
		}
	}
	return request, errs
}
func decodeUsageSummaryProfile(raw json.RawMessage) (usageprojection.ContextProfile, error) {
	obj, err := decodeRawObject(raw)
	if err != nil {
		return usageprojection.ContextProfile{}, err
	}
	allowed := map[string]bool{"schema_version": true, "task_family": true, "runtime_class": true, "workspace_feature_tags": true, "observable_guard_facts": true, "tool_policy_ref": true, "environment_class": true}
	for key := range obj {
		if !allowed[key] {
			return usageprojection.ContextProfile{}, fmt.Errorf("unknown field %q", key)
		}
	}
	for _, key := range []string{"schema_version", "task_family", "runtime_class", "workspace_feature_tags", "observable_guard_facts", "tool_policy_ref", "environment_class"} {
		if _, ok := obj[key]; !ok {
			return usageprojection.ContextProfile{}, fmt.Errorf("required field %q is missing", key)
		}
	}
	var profile usageprojection.ContextProfile
	if profile.SchemaVersion, _ = contract.AsString(obj["schema_version"]); profile.SchemaVersion == "" {
		return profile, fmt.Errorf("schema_version must be a string")
	}
	if profile.TaskFamily, _ = contract.AsString(obj["task_family"]); profile.TaskFamily == "" {
		return profile, fmt.Errorf("task_family must be a string")
	}
	if profile.RuntimeClass, _ = contract.AsString(obj["runtime_class"]); profile.RuntimeClass == "" {
		return profile, fmt.Errorf("runtime_class must be a string")
	}
	if profile.EnvironmentClass, _ = contract.AsString(obj["environment_class"]); profile.EnvironmentClass == "" {
		return profile, fmt.Errorf("environment_class must be a string")
	}
	if profile.WorkspaceFeatureTags, err = strictUsageStringArray(obj["workspace_feature_tags"]); err != nil {
		return profile, fmt.Errorf("workspace_feature_tags: %w", err)
	}
	if profile.ObservableGuardFacts, err = strictUsageStringArray(obj["observable_guard_facts"]); err != nil {
		return profile, fmt.Errorf("observable_guard_facts: %w", err)
	}
	if profile.ToolPolicyRef, err = parseVersionedRef(obj["tool_policy_ref"]); err != nil {
		return profile, fmt.Errorf("tool_policy_ref: %w", err)
	}
	return profile, nil
}
func strictUsageStringArray(value any) ([]string, error) {
	values, ok := contract.AsArray(value)
	if !ok {
		return nil, fmt.Errorf("must be an array")
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := contract.AsString(value)
		if !ok {
			return nil, fmt.Errorf("items must be strings")
		}
		out = append(out, text)
	}
	return out, nil
}
func decodeUsageSummarySubject(raw json.RawMessage) (usageprojection.UsageSubjectRef, error) {
	obj, err := decodeRawObject(raw)
	if err != nil {
		return usageprojection.UsageSubjectRef{}, err
	}
	if len(obj) != 1 {
		return usageprojection.UsageSubjectRef{}, fmt.Errorf("requires exactly one of skill_ref or candidate_ref")
	}
	if nested, ok := obj["skill_ref"]; ok {
		ref, err := parseSkillRef(nested)
		if err != nil {
			return usageprojection.UsageSubjectRef{}, err
		}
		return usageprojection.UsageSubjectRef{SkillRef: &ref}, nil
	}
	if nested, ok := obj["candidate_ref"]; ok {
		ref, err := parseCandidateRef(nested)
		if err != nil {
			return usageprojection.UsageSubjectRef{}, err
		}
		return usageprojection.UsageSubjectRef{CandidateRef: &ref}, nil
	}
	return usageprojection.UsageSubjectRef{}, fmt.Errorf("requires exactly one of skill_ref or candidate_ref")
}
func usageSubjectDoc(subject usageprojection.UsageSubjectRef) map[string]any {
	if subject.SkillRef != nil {
		ref := subject.SkillRef
		return map[string]any{"skill_ref": map[string]any{"schema_version": ref.SchemaVersion, "lineage_id": ref.LineageID, "version": json.Number(ref.Version), "kind": ref.Kind, "artifact_digest": ref.ArtifactDigest}}
	}
	ref := subject.CandidateRef
	return map[string]any{"candidate_ref": map[string]any{"schema_version": ref.SchemaVersion, "candidate_id": ref.CandidateID, "kind": ref.Kind, "body_digest": ref.BodyDigest, "origin_type": ref.OriginType, "origin_ref": map[string]any{"id": ref.OriginRef.ID, "version": json.Number(ref.OriginRef.Version), "digest": ref.OriginRef.Digest}}}
}

func candidateRefDoc(ref contract.CandidateArtifactRef) map[string]any {
	return map[string]any{"schema_version": ref.SchemaVersion, "candidate_id": ref.CandidateID, "kind": ref.Kind, "body_digest": ref.BodyDigest, "origin_type": ref.OriginType, "origin_ref": map[string]any{"id": ref.OriginRef.ID, "version": json.Number(ref.OriginRef.Version), "digest": ref.OriginRef.Digest}}
}

func (h *skillEvolutionHandler) handleCandidateOutcomesRead(w http.ResponseWriter, r *http.Request, object strictObject) {
	if h.outcomes == nil {
		writeError(w, r, http.StatusServiceUnavailable, "UNAVAILABLE", "candidate lifecycle outcome reader is not wired", nil)
		return
	}
	if details := object.rejectUnknownFields(map[string]bool{"request_id": true, "candidate_refs": true}); details != nil {
		writeClosureInvalidRequest(w, nil, details)
		return
	}
	var errs []fieldDetail
	var requestID string
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)
	var raws []json.RawMessage
	if raw, ok := object["candidate_refs"]; !ok || json.Unmarshal(raw, &raws) != nil || len(raws) == 0 {
		addFieldError(&errs, "candidate_refs", "candidate_refs must be a non-empty exact-ref array")
	}
	refs := make([]contract.CandidateArtifactRef, 0, len(raws))
	for index, raw := range raws {
		ref, err := parseCandidateRef(raw)
		if err != nil {
			addFieldError(&errs, fmt.Sprintf("candidate_refs[%d]", index), err.Error())
		} else {
			refs = append(refs, ref)
		}
	}
	if len(errs) > 0 {
		writeClosureInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	outcomes, err := h.outcomes.CandidateOutcomes(r.Context(), refs)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "INTERNAL", "candidate lifecycle outcome reader failed", nil)
		return
	}
	docs := make([]any, 0, len(outcomes))
	for _, outcome := range outcomes {
		doc := map[string]any{"candidate_ref": candidateRefDoc(outcome.CandidateRef), "status": outcome.Status}
		if outcome.DecisionRef != nil {
			doc["decision_ref"] = map[string]any{"id": outcome.DecisionRef.ID, "version": json.Number(strconv.FormatInt(outcome.DecisionRef.Version, 10)), "digest": outcome.DecisionRef.Digest}
		}
		if outcome.Reason != "" {
			doc["reason"] = outcome.Reason
		}
		docs = append(docs, doc)
	}
	writeJSON(w, http.StatusOK, map[string]any{"schema_version": "gms.candidate-outcomes-read.v1", "request_id": requestID, "outcomes": docs})
}
