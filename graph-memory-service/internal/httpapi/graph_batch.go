// graph_batch.go is the Warm Skill Graph Batch transport adapter. It exposes
// skill_propose, skill_consolidate, evaluation-freeze, and
// trajectories:register as a standalone http.Handler so the composition root
// can mount them beside Memory Protocol and the closed v1 skill-evolution
// tool set without growing either surface.
//
// GraphBatchRoutes is intentionally NOT part of Routes() (Memory Protocol
// OpenAPI) or SkillEvolutionRoutes() (closed memory_explore/expand/skill_get
// set). The dispatch lives here, mirroring ConsolidationCutRoutes.
package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strconv"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/batchconsolidation"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationfreeze"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

const (
	GraphBatchSkillProposePath         = "/v1/skill-evolution/tools/skill_propose"
	GraphBatchSkillConsolidatePath     = "/v1/skill-evolution/tools/skill_consolidate"
	GraphBatchEvaluationFreezePath     = "/v1/skill-evolution/evaluation-freeze"
	GraphBatchTrajectoriesRegisterPath = "/v1/skill-evolution/trajectories:register"
)

// GraphBatchDependencies wires the Warm Skill Graph Batch transport.
type GraphBatchDependencies struct {
	Token       string
	Proposals   *rawproposal.Service
	Consolidate *batchconsolidation.Service
	Freeze      *evaluationfreeze.Service
	Source      *rawproposal.MemoryTrajectorySource
}

// GraphBatchRoutes lists the routes served by NewGraphBatchHandler. The
// composition root mounts them with mux.Handle; they are never gated behind
// skillEvolutionEnabled and never folded into SkillEvolutionRoutes().
func GraphBatchRoutes() []Route {
	return []Route{
		{Method: http.MethodPost, Path: GraphBatchSkillProposePath},
		{Method: http.MethodPost, Path: GraphBatchSkillConsolidatePath},
		{Method: http.MethodPost, Path: GraphBatchEvaluationFreezePath},
		{Method: http.MethodPost, Path: GraphBatchTrajectoriesRegisterPath},
	}
}

// NewGraphBatchHandler builds the standalone Warm Skill Graph Batch transport.
func NewGraphBatchHandler(deps GraphBatchDependencies) http.Handler {
	if deps.Proposals == nil || deps.Consolidate == nil || deps.Freeze == nil || deps.Source == nil {
		panic("httpapi: NewGraphBatchHandler requires proposals, consolidate, freeze, and trajectory source")
	}
	return &graphBatchHandler{
		auth:        authz.NewAuthenticator(deps.Token),
		proposals:   deps.Proposals,
		consolidate: deps.Consolidate,
		freeze:      deps.Freeze,
		source:      deps.Source,
	}
}

type graphBatchHandler struct {
	auth        *authz.Authenticator
	proposals   *rawproposal.Service
	consolidate *batchconsolidation.Service
	freeze      *evaluationfreeze.Service
	source      *rawproposal.MemoryTrajectorySource
}

func (h *graphBatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, err := h.auth.Authenticate(r.Header.Get("Authorization")); err != nil {
		writeError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid bearer token", nil)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "unknown graph batch route", nil)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Content-Type must be application/json", nil)
		return
	}
	body, _ := readBody(r)
	object, decodeErr := decodeStrictObject(body)
	if decodeErr != nil {
		writeGraphBatchInvalidRequest(w, body, decodeErr)
		return
	}
	switch r.URL.Path {
	case GraphBatchSkillProposePath:
		h.handlePropose(w, r, object)
	case GraphBatchSkillConsolidatePath:
		h.handleConsolidate(w, r, object)
	case GraphBatchEvaluationFreezePath:
		h.handleFreeze(w, r, object)
	case GraphBatchTrajectoriesRegisterPath:
		h.handleRegister(w, r, object)
	default:
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "unknown graph batch route", nil)
	}
}

func (h *graphBatchHandler) handlePropose(w http.ResponseWriter, r *http.Request, object strictObject) {
	if details := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "idempotency_key": true, "diagnosis_run_id": true,
		"trajectory_id": true, "body": true,
	}); details != nil {
		writeGraphBatchInvalidRequest(w, nil, details)
		return
	}
	var errs []fieldDetail
	var requestID, idempotencyKey, diagnosisRunID, trajectoryID string
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)
	object.requireString("idempotency_key", &idempotencyKey, &errs)
	object.requireString("diagnosis_run_id", &diagnosisRunID, &errs)
	object.requireString("trajectory_id", &trajectoryID, &errs)
	body, err := decodeJSONObject(object["body"], "body")
	if err != nil {
		addFieldError(&errs, "body", err.Error())
	}
	if len(errs) > 0 {
		writeGraphBatchInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	ref, admitErr := h.proposals.Admit(r.Context(), rawproposal.AdmissionRequest{
		IdempotencyKey: idempotencyKey,
		DiagnosisRunID: diagnosisRunID,
		TrajectoryID:   trajectoryID,
		Body:           body,
	})
	if admitErr != nil {
		writeGraphBatchServiceError(w, requestID, admitErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"proposal_id":    ref.ProposalID,
		"content_digest": ref.ContentDigest,
		"uri":            ref.URI,
	})
}

func (h *graphBatchHandler) handleConsolidate(w http.ResponseWriter, r *http.Request, object strictObject) {
	if details := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "idempotency_key": true, "agent_run_id": true,
		"expected_ledger_revision": true, "family_proposal_ids": true,
	}); details != nil {
		writeGraphBatchInvalidRequest(w, nil, details)
		return
	}
	var errs []fieldDetail
	var requestID, idempotencyKey, agentRunID string
	var expected uint64
	var family []string
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)
	object.requireString("idempotency_key", &idempotencyKey, &errs)
	object.requireString("agent_run_id", &agentRunID, &errs)
	requireUint64Field(object, "expected_ledger_revision", &expected, &errs)
	object.requireStringArray("family_proposal_ids", &family, &errs)
	if len(errs) > 0 {
		writeGraphBatchInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	// Decisions omitted: batchconsolidation.Consolidate classifies the family.
	result, err := h.consolidate.Consolidate(r.Context(), batchconsolidation.Request{
		IdempotencyKey:         idempotencyKey,
		AgentRunID:             agentRunID,
		ExpectedLedgerRevision: expected,
		FamilyProposalIDs:      family,
	})
	if err != nil {
		writeGraphBatchServiceError(w, requestID, err)
		return
	}
	decisions := make([]any, 0, len(result.Decisions))
	for _, d := range result.Decisions {
		decisions = append(decisions, map[string]any{
			"decision_id":         d.DecisionID,
			"operation":           d.Operation,
			"source_proposal_ids": d.SourceProposalIDs,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ledger_revision": json.Number(strconv.FormatUint(result.LedgerRevision, 10)),
		"ledger_digest":   result.LedgerDigest,
		"decisions":       decisions,
	})
}

func (h *graphBatchHandler) handleFreeze(w http.ResponseWriter, r *http.Request, object strictObject) {
	if details := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "expected_ledger_revision": true, "expected_ledger_digest": true,
		"proposal_ids": true, "evidence_cut": true, "policy": true, "scope": true,
		"graph_fixture": true,
	}); details != nil {
		writeGraphBatchInvalidRequest(w, nil, details)
		return
	}
	var errs []fieldDetail
	var requestID, expectedDigest string
	var expected uint64
	var proposalIDs []string
	object.requireString("request_id", &requestID, &errs)
	validateID(&errs, "request_id", requestID)
	requireUint64Field(object, "expected_ledger_revision", &expected, &errs)
	object.requireString("expected_ledger_digest", &expectedDigest, &errs)
	object.requireStringArray("proposal_ids", &proposalIDs, &errs)
	cut, cutErr := decodeEvidenceCut(object["evidence_cut"])
	if cutErr != nil {
		addFieldError(&errs, "evidence_cut", cutErr.Error())
	}
	policy, policyErr := decodePolicyRevisions(object["policy"])
	if policyErr != nil {
		addFieldError(&errs, "policy", policyErr.Error())
	}
	scope, scopeErr := decodeFreezeScope(object["scope"])
	if scopeErr != nil {
		addFieldError(&errs, "scope", scopeErr.Error())
	}
	fixture, fixtureErr := decodeGraphFixture(object["graph_fixture"])
	if fixtureErr != nil {
		addFieldError(&errs, "graph_fixture", fixtureErr.Error())
	}
	if len(errs) > 0 {
		writeGraphBatchInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	manifest, err := h.freeze.Freeze(r.Context(), evaluationfreeze.Request{
		ExpectedLedgerRevision: expected,
		ExpectedLedgerDigest:   expectedDigest,
		EvidenceCut:            cut,
		ProposalIDs:            proposalIDs,
		GraphFixture:           fixture,
		Policy:                 policy,
		Scope:                  scope,
	})
	if err != nil {
		writeGraphBatchServiceError(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"manifest_id":     manifest.ID,
		"digest":          manifest.Digest,
		"ledger_revision": json.Number(strconv.FormatUint(manifest.LedgerRevision, 10)),
		"ledger_digest":   manifest.LedgerDigest,
	})
}

func (h *graphBatchHandler) handleRegister(w http.ResponseWriter, r *http.Request, object strictObject) {
	if details := object.rejectUnknownFields(map[string]bool{
		"diagnosis_run_id": true, "trajectory": true,
	}); details != nil {
		writeGraphBatchInvalidRequest(w, nil, details)
		return
	}
	var errs []fieldDetail
	var diagnosisRunID string
	object.requireString("diagnosis_run_id", &diagnosisRunID, &errs)
	traj, trajErrs := decodeFrozenTrajectory(object["trajectory"], diagnosisRunID)
	errs = append(errs, trajErrs...)
	if len(errs) > 0 {
		writeGraphBatchInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	h.source.Put(traj)
	writeJSON(w, http.StatusOK, map[string]any{
		"registered":       true,
		"trajectory_id":    traj.ID,
		"diagnosis_run_id": diagnosisRunID,
	})
}

func decodeJSONObject(raw json.RawMessage, field string) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, errors.New(field + " must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New(field + " must be a JSON object")
	}
	return object, nil
}

func requireUint64Field(object strictObject, key string, dst *uint64, errs *[]fieldDetail) {
	raw, ok := object[key]
	if !ok {
		addFieldError(errs, key, "required field is missing")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var number json.Number
	if err := decoder.Decode(&number); err != nil || !contract.IsIntegerNumber(number) {
		addFieldError(errs, key, key+" must be an integer")
		return
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		addFieldError(errs, key, key+" out of range")
		return
	}
	*dst = value
}

func decodeEvidenceCut(raw json.RawMessage) (evaluationfreeze.EvidenceCut, error) {
	object, err := decodeStrictNested(raw, "evidence_cut")
	if err != nil {
		return evaluationfreeze.EvidenceCut{}, err
	}
	if details := object.rejectUnknownFields(map[string]bool{
		"batches": true, "watermark": true, "watermark_digest": true,
	}); details != nil {
		return evaluationfreeze.EvidenceCut{}, errors.New("evidence_cut has unknown fields")
	}
	var cut evaluationfreeze.EvidenceCut
	if raw, ok := object["watermark"]; ok {
		if err := json.Unmarshal(raw, &cut.Watermark); err != nil {
			return evaluationfreeze.EvidenceCut{}, errors.New("watermark must be a string")
		}
	}
	if raw, ok := object["watermark_digest"]; ok {
		if err := json.Unmarshal(raw, &cut.WatermarkDigest); err != nil {
			return evaluationfreeze.EvidenceCut{}, errors.New("watermark_digest must be a string")
		}
	}
	batchesRaw, ok := object["batches"]
	if !ok {
		return evaluationfreeze.EvidenceCut{}, errors.New("evidence_cut.batches is required")
	}
	var items []json.RawMessage
	if err := json.Unmarshal(batchesRaw, &items); err != nil {
		return evaluationfreeze.EvidenceCut{}, errors.New("evidence_cut.batches must be an array")
	}
	for _, item := range items {
		batchObj, err := decodeStrictNested(item, "evidence_cut.batches")
		if err != nil {
			return evaluationfreeze.EvidenceCut{}, err
		}
		if details := batchObj.rejectUnknownFields(map[string]bool{"id": true, "kind": true}); details != nil {
			return evaluationfreeze.EvidenceCut{}, errors.New("evidence batch has unknown fields")
		}
		var batch evaluationfreeze.EvidenceBatch
		var errs []fieldDetail
		batchObj.requireString("id", &batch.ID, &errs)
		if raw, ok := batchObj["kind"]; ok {
			if err := json.Unmarshal(raw, &batch.Kind); err != nil {
				return evaluationfreeze.EvidenceCut{}, errors.New("evidence batch kind must be a string")
			}
		}
		if len(errs) > 0 {
			return evaluationfreeze.EvidenceCut{}, errors.New("evidence batch id is required")
		}
		cut.Batches = append(cut.Batches, batch)
	}
	return cut, nil
}

func decodePolicyRevisions(raw json.RawMessage) (evaluationfreeze.PolicyRevisions, error) {
	object, err := decodeStrictNested(raw, "policy")
	if err != nil {
		return evaluationfreeze.PolicyRevisions{}, err
	}
	if details := object.rejectUnknownFields(map[string]bool{
		"prompt_digest": true, "schema_digest": true, "model_digest": true,
		"tool_digest": true, "config_digest": true, "grading_policy_digest": true,
	}); details != nil {
		return evaluationfreeze.PolicyRevisions{}, errors.New("policy has unknown fields")
	}
	var policy evaluationfreeze.PolicyRevisions
	var errs []fieldDetail
	object.requireString("prompt_digest", &policy.PromptDigest, &errs)
	object.requireString("schema_digest", &policy.SchemaDigest, &errs)
	object.requireString("model_digest", &policy.ModelDigest, &errs)
	object.requireString("tool_digest", &policy.ToolDigest, &errs)
	object.requireString("config_digest", &policy.ConfigDigest, &errs)
	object.requireString("grading_policy_digest", &policy.GradingPolicyDigest, &errs)
	if len(errs) > 0 {
		return evaluationfreeze.PolicyRevisions{}, errors.New("policy is missing required digest pins")
	}
	return policy, nil
}

func decodeFreezeScope(raw json.RawMessage) ([]evaluationfreeze.ScopeItem, error) {
	if len(raw) == 0 {
		return nil, errors.New("scope must be an array")
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, errors.New("scope must be an array")
	}
	out := make([]evaluationfreeze.ScopeItem, 0, len(items))
	for _, item := range items {
		object, err := decodeStrictNested(item, "scope")
		if err != nil {
			return nil, err
		}
		if details := object.rejectUnknownFields(map[string]bool{"kind": true, "id": true}); details != nil {
			return nil, errors.New("scope item has unknown fields")
		}
		var scope evaluationfreeze.ScopeItem
		var errs []fieldDetail
		object.requireString("kind", &scope.Kind, &errs)
		object.requireString("id", &scope.ID, &errs)
		if len(errs) > 0 {
			return nil, errors.New("scope item requires kind and id")
		}
		out = append(out, scope)
	}
	return out, nil
}

func decodeGraphFixture(raw json.RawMessage) (evaluationgraph.CanonicalFixture, error) {
	if len(raw) == 0 {
		fixture, err := evaluationgraph.NewFixture(nil)
		if err != nil {
			return evaluationgraph.CanonicalFixture{}, err
		}
		return fixture, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var fixture evaluationgraph.CanonicalFixture
	if err := decoder.Decode(&fixture); err != nil {
		return evaluationgraph.CanonicalFixture{}, errors.New("graph_fixture must be a CanonicalFixture object")
	}
	return fixture, nil
}

func decodeFrozenTrajectory(raw json.RawMessage, diagnosisRunID string) (rawproposal.FrozenTrajectory, []fieldDetail) {
	var errs []fieldDetail
	if len(raw) == 0 {
		addFieldError(&errs, "trajectory", "required field is missing")
		return rawproposal.FrozenTrajectory{}, errs
	}
	object, decodeErr := decodeStrictObject(raw)
	if decodeErr != nil {
		addFieldError(&errs, "trajectory", "trajectory must be a JSON object")
		return rawproposal.FrozenTrajectory{}, errs
	}
	if details := object.rejectUnknownFields(map[string]bool{
		"id": true, "snapshot_id": true, "complete_trajectory": true, "public_outcome": true,
		"checkpoints": true, "evidence": true, "authorized_diagnosis_run_ids": true,
	}); details != nil {
		return rawproposal.FrozenTrajectory{}, details.fields
	}
	var traj rawproposal.FrozenTrajectory
	object.requireString("id", &traj.ID, &errs)
	object.requireString("snapshot_id", &traj.SnapshotID, &errs)
	object.requireString("public_outcome", &traj.PublicOutcome, &errs)
	object.requireStringArray("complete_trajectory", &traj.CompleteTrajectory, &errs)
	traj.Checkpoints = map[string]rawproposal.Checkpoint{}
	if raw, ok := object["checkpoints"]; ok {
		var wires map[string]struct {
			ID         string `json:"id"`
			SnapshotID string `json:"snapshot_id"`
		}
		if err := json.Unmarshal(raw, &wires); err != nil {
			addFieldError(&errs, "trajectory.checkpoints", "checkpoints must be an object")
		} else {
			for key, wire := range wires {
				id := wire.ID
				if id == "" {
					id = key
				}
				traj.Checkpoints[key] = rawproposal.Checkpoint{ID: id, SnapshotID: wire.SnapshotID}
			}
		}
	}
	traj.Evidence = map[string]rawproposal.Evidence{}
	if raw, ok := object["evidence"]; ok {
		var wires map[string]struct {
			ID              string   `json:"id"`
			TrajectoryID    string   `json:"trajectory_id"`
			SnapshotID      string   `json:"snapshot_id"`
			ObservableFacts []string `json:"observable_facts"`
		}
		if err := json.Unmarshal(raw, &wires); err != nil {
			addFieldError(&errs, "trajectory.evidence", "evidence must be an object")
		} else {
			for key, wire := range wires {
				id := wire.ID
				if id == "" {
					id = key
				}
				traj.Evidence[key] = rawproposal.Evidence{
					ID: id, TrajectoryID: wire.TrajectoryID, SnapshotID: wire.SnapshotID,
					ObservableFacts: append([]string(nil), wire.ObservableFacts...),
				}
			}
		}
	}
	traj.AuthorizedDiagnosisRunIDs = map[string]bool{}
	if raw, ok := object["authorized_diagnosis_run_ids"]; ok {
		ids, ok := decodeStringSet(raw)
		if !ok {
			addFieldError(&errs, "trajectory.authorized_diagnosis_run_ids", "authorized_diagnosis_run_ids must be an array of strings")
		} else {
			for _, id := range ids {
				traj.AuthorizedDiagnosisRunIDs[id] = true
			}
		}
	}
	if diagnosisRunID != "" {
		traj.AuthorizedDiagnosisRunIDs[diagnosisRunID] = true
	}
	return traj, errs
}

func decodeStringSet(raw json.RawMessage) ([]string, bool) {
	var asArray []string
	if err := json.Unmarshal(raw, &asArray); err == nil {
		return asArray, true
	}
	var asObject map[string]bool
	if err := json.Unmarshal(raw, &asObject); err == nil {
		out := make([]string, 0, len(asObject))
		for id, ok := range asObject {
			if ok {
				out = append(out, id)
			}
		}
		return out, true
	}
	return nil, false
}

func decodeStrictNested(raw json.RawMessage, field string) (strictObject, error) {
	if len(raw) == 0 {
		return nil, errors.New(field + " is required")
	}
	object, err := decodeStrictObject(raw)
	if err != nil {
		return nil, errors.New(field + " must be a JSON object")
	}
	return object, nil
}

func writeGraphBatchInvalidRequest(w http.ResponseWriter, body []byte, request *invalidRequest) {
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
		Code: code, Message: "request violates the graph batch contract",
		RequestID: requestID, Details: details,
	}})
}

func writeGraphBatchServiceError(w http.ResponseWriter, requestID string, err error) {
	if requestID == "" || byteLen(requestID) > 128 {
		requestID = newRequestID()
	}
	status, code := graphBatchErrorStatus(err)
	if status == http.StatusInternalServerError {
		writeError(w, nil, status, "INTERNAL", "internal service failure", nil)
		return
	}
	writeJSON(w, status, wireError{Error: wireErrorBody{
		Code: code, Message: err.Error(), RequestID: requestID, Details: []fieldDetail{},
	}})
}

func graphBatchErrorStatus(err error) (int, string) {
	if code := validation.CodeOf(err); code != "" {
		return http.StatusUnprocessableEntity, code
	}
	if reason := ledger.ReasonOf(err); reason != "" {
		switch reason {
		case ledger.ReasonProjectionHeadConflict, ledger.ReasonIdempotencyConflict:
			return http.StatusConflict, reason
		default:
			return http.StatusUnprocessableEntity, reason
		}
	}
	switch {
	case errors.Is(err, evaluationfreeze.ErrMovedHead), errors.Is(err, evaluationfreeze.ErrManifestConflict):
		return http.StatusConflict, "PROJECTION_HEAD_CONFLICT"
	case errors.Is(err, evaluationfreeze.ErrMissingProposal),
		errors.Is(err, evaluationfreeze.ErrProvenanceHole),
		errors.Is(err, evaluationfreeze.ErrGraphDigestMismatch),
		errors.Is(err, evaluationfreeze.ErrScopeContamination),
		errors.Is(err, evaluationfreeze.ErrPartialGraphFreeze),
		errors.Is(err, evaluationfreeze.ErrNotFrozen):
		return http.StatusUnprocessableEntity, "EVALUATION_FREEZE_REJECTED"
	default:
		return http.StatusInternalServerError, "INTERNAL"
	}
}
