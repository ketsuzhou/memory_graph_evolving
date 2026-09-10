package httpapi

import (
	"context"
	"net/http"
	"time"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/pattern"
	"river2.dev/graph-memory-service/internal/ports"
)

// Curation read/governance routes (M2 dive read, M4 pattern/proposal/
// candidate surface). All additions inherit bearer auth, server-resolved
// identity, strict deep JSON, opaque 1–128-byte IDs, and the 201/200/409
// idempotency discipline of the existing protocol.

// DiveStore is the dive slice of the exploration store: trajectory
// derivation, terminal-trajectory recording, and the one idempotent
// judgment per session.
type DiveStore interface {
	ports.DiveStore
	RecordDiveTrajectory(context.Context, domain.DiveTrajectory) error
	Session(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.ExplorationSession, error)
}

// DiveJudge is the server-owned judge wired by deployment code.
type DiveJudge interface {
	JudgeSubmitted(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.DiveResult, bool, error)
}

// ProposalStore is the proposal slice the governed read route resolves IDs
// against, on top of the persisted proposal port.
type ProposalStore interface {
	ports.ProposalStore
	Proposal(context.Context, domain.TenantID, domain.SpaceID, string) (domain.SkillProposal, error)
}

// requiredQueryValues enforces the exact query-parameter contract: only the
// allowed keys, each present exactly once, no unknown parameters.
func requiredQueryValues(r *http.Request, allowed map[string]bool) (map[string]string, *invalidRequest) {
	query := r.URL.Query()
	values := make(map[string]string, len(allowed))
	var errs []fieldDetail
	for key, list := range query {
		if !allowed[key] {
			addFieldError(&errs, key, "unknown query parameter")
			continue
		}
		if len(list) > 1 {
			addFieldError(&errs, key, "query parameter must appear exactly once")
			continue
		}
		values[key] = list[0]
	}
	for key := range allowed {
		if _, ok := values[key]; !ok {
			addFieldError(&errs, key, "required query parameter is missing")
		}
	}
	if len(errs) > 0 {
		return nil, invalidRequestFrom(errs)
	}
	return values, nil
}

func queryRequestID(w http.ResponseWriter, r *http.Request, allowed map[string]bool) (string, bool) {
	values, invalid := requiredQueryValues(r, allowed)
	if invalid != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", invalid.Error(), invalid.fields)
		return "", false
	}
	var errs []fieldDetail
	validateID(&errs, "request_id", values["request_id"])
	if allowed["space_id"] {
		validateID(&errs, "space_id", values["space_id"])
	}
	if len(errs) > 0 {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "request violates the protocol request shape", errs)
		return "", false
	}
	return values["request_id"], true
}

func (h *Handler) authorizeCuration(ctx context.Context, identity authz.Identity, spaceID domain.SpaceID, operation domain.GrantOperation) error {
	_, err := h.authorizer.AuthorizeExact(ctx, identity, []domain.SpaceID{spaceID}, domain.GrantPurposeCuration, operation)
	return err
}

// judgeDiveSubmission freezes the terminal trajectory after a successful
// submit and hands it to the judge. Dive failures never fail the submit: the
// read route reports DIVE_NOT_FOUND until a judgment exists.
func (h *Handler) judgeDiveSubmission(ctx context.Context, identity authz.Identity, sessionID domain.ExplorationSessionID, result exploration.SubmitResult) {
	if h.deps.DiveStore == nil || h.deps.DiveJudge == nil {
		return
	}
	trajectory, err := h.deps.DiveStore.DiveTrajectory(ctx, identity.TenantID, sessionID)
	if err != nil {
		return
	}
	trajectory.State = result.State
	trajectory.Found = result.Found
	trajectory.Summary = result.Summary
	submitted := make([]string, 0, len(result.Citations))
	for _, citation := range result.Citations {
		submitted = append(submitted, citation.CitationID)
	}
	trajectory.SubmittedCitationIDs = submitted
	if err := h.deps.DiveStore.RecordDiveTrajectory(ctx, trajectory); err != nil {
		return
	}
	_, _, _ = h.deps.DiveJudge.JudgeSubmitted(ctx, identity.TenantID, sessionID)
}

func isTerminalExplorationState(state string) bool {
	switch state {
	case "submitted", "error", "budget_exhausted", "timeout":
		return true
	}
	return false
}

func (h *Handler) handleDiveRead(w http.ResponseWriter, r *http.Request, identity authz.Identity, sessionID string) {
	requestID, ok := queryRequestID(w, r, map[string]bool{"request_id": true})
	if !ok {
		return
	}
	ctx := r.Context()
	// The session is loaded first only to recover the server-owned exact
	// Space set; authorization is then checked against that set.
	session, err := h.deps.DiveStore.Session(ctx, identity.TenantID, domain.ExplorationSessionID(sessionID))
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	pinned := make([]domain.SpaceID, 0, len(session.PinnedSpaces))
	for _, pin := range session.PinnedSpaces {
		pinned = append(pinned, pin.SpaceID)
	}
	if _, err := h.authorizer.AuthorizeExact(ctx, identity, pinned, domain.GrantPurposeLifecycle, domain.GrantOperationDiveRead); err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	if !isTerminalExplorationState(session.State) {
		h.writeServiceError(w, requestID, domain.NewProtocolError(409, "EXPLORATION_NOT_TERMINAL", "dive judgment exists only for terminal explorations"))
		return
	}
	result, err := h.deps.DiveStore.DiveResult(ctx, identity.TenantID, domain.ExplorationSessionID(sessionID))
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusOK, diveResultDTO(result, requestID))
}

func diveResultDTO(result domain.DiveResult, requestID string) map[string]any {
	var score any
	switch result.Score.Classification {
	case domain.DiveFound, domain.DiveMiss:
		score = map[string]any{
			"relevance": result.Score.Relevance, "groundedness": result.Score.Groundedness,
			"completeness": result.Score.Completeness, "overall": result.Score.Overall,
			"rounds": result.Score.Rounds,
		}
	}
	items := make([]map[string]any, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, map[string]any{
			"citation_id": item.CitationID, "statement": item.Statement,
			"source_space_id": string(item.SourceSpaceID), "memory_version": item.MemoryVersion,
			"evidence_batch_id": string(item.EvidenceBatchID), "event_ids": item.EventIDs,
			"retrieval_score": item.RetrievalScore, "submitted": item.Submitted,
			"authoritative": item.Authoritative,
		})
	}
	return map[string]any{
		"request_id": requestID, "session_id": string(result.SessionID),
		"classification": string(result.Score.Classification), "score": score, "items": items,
		"incomplete": result.Incomplete, "evaluated_at": result.EvaluatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (h *Handler) handlePatternRead(w http.ResponseWriter, r *http.Request, identity authz.Identity, patternID string) {
	requestID, ok := queryRequestID(w, r, map[string]bool{"space_id": true, "request_id": true})
	if !ok {
		return
	}
	spaceID := domain.SpaceID(r.URL.Query().Get("space_id"))
	if err := h.authorizeCuration(r.Context(), identity, spaceID, domain.GrantOperationPatternRead); err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	revision, err := h.deps.Patterns.LatestPatternRevision(r.Context(), identity.TenantID, spaceID, patternID)
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusOK, patternRevisionDTO(revision))
}

func (h *Handler) handlePatternReject(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, patternID string) {
	if err := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "space_id": true, "operation_id": true, "expected_revision": true, "reason": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var requestID, spaceID, operationID, reason string
	var expectedRevision int64
	object.requireString("request_id", &requestID, &errs)
	object.requireString("space_id", &spaceID, &errs)
	object.requireString("operation_id", &operationID, &errs)
	object.requireInt64("expected_revision", &expectedRevision, &errs)
	object.requireString("reason", &reason, &errs)
	validateID(&errs, "request_id", requestID)
	validateID(&errs, "space_id", spaceID)
	validateID(&errs, "operation_id", operationID)
	if expectedRevision < 1 {
		addFieldError(&errs, "expected_revision", "expected_revision must be at least 1")
	}
	if byteLen(reason) < 1 || byteLen(reason) > 1000 {
		addFieldError(&errs, "reason", "reason must be 1-1000 UTF-8 bytes")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	ctx := r.Context()
	space := domain.SpaceID(spaceID)
	if err := h.authorizeCuration(ctx, identity, space, domain.GrantOperationPatternReject); err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	latest, err := h.deps.Patterns.LatestPatternRevision(ctx, identity.TenantID, space, patternID)
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	// Identical replays resolve against the already-rejected head.
	if latest.Status == domain.PatternRejected && latest.Revision == expectedRevision+1 &&
		latest.RejectionReason == reason && latest.CreatedBy == identity.PrincipalID {
		writeJSON(w, http.StatusOK, map[string]any{"pattern": patternRevisionDTO(latest), "duplicate": true})
		return
	}
	if latest.Revision != expectedRevision || latest.Status == domain.PatternRejected {
		h.writeServiceError(w, requestID, domain.NewProtocolError(409, "PATTERN_REVISION_CONFLICT",
			"expected_revision does not match the current revisable pattern revision"))
		return
	}
	rejected, err := pattern.RejectPattern(latest, reason, identity.PrincipalID, h.deps.Clock.Now())
	if err != nil {
		h.writeServiceError(w, requestID, domain.NewProtocolError(422, "INVALID_REQUEST", err.Error()))
		return
	}
	stored, duplicate, err := h.deps.Patterns.AppendPatternRevision(ctx, rejected, patternID)
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{"pattern": patternRevisionDTO(stored), "duplicate": duplicate})
}

func patternRevisionDTO(revision domain.PatternRevision) map[string]any {
	evidence := make([]map[string]any, 0, len(revision.Evidence))
	for _, item := range revision.Evidence {
		var causal any
		if item.CausalEstimate != nil {
			causal = map[string]any{"estimate_id": string(item.CausalEstimate.EstimateID), "revision": item.CausalEstimate.Revision}
		}
		evidence = append(evidence, map[string]any{
			"evidence_batch_id": string(item.EvidenceRef.BatchID), "event_ids": item.EvidenceRef.EventIDs,
			"source_space_id": string(item.SourceSpaceID), "lineage_id": item.LineageID,
			"strength": string(item.Strength), "causal_estimate": causal,
		})
	}
	return map[string]any{
		"pattern_id": revision.PatternID, "space_id": string(revision.SpaceID),
		"revision": revision.Revision, "status": string(revision.Status),
		"problem": revision.Problem, "applicability": revision.Applicability,
		"recommended_action": revision.RecommendedAction, "evidence": evidence,
		"policy_version": revision.PolicyVersion, "content_hash": revision.ContentHash,
		"rejection_reason": revision.RejectionReason, "created_by": string(revision.CreatedBy),
		"created_at": revision.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func patternRefsDTO(refs []domain.PatternRef) []map[string]any {
	dto := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		dto = append(dto, map[string]any{"pattern_id": ref.PatternID, "revision": ref.Revision})
	}
	return dto
}

func (h *Handler) handleProposalRead(w http.ResponseWriter, r *http.Request, identity authz.Identity, proposalID string) {
	requestID, ok := queryRequestID(w, r, map[string]bool{"space_id": true, "request_id": true})
	if !ok {
		return
	}
	spaceID := domain.SpaceID(r.URL.Query().Get("space_id"))
	if err := h.authorizeCuration(r.Context(), identity, spaceID, domain.GrantOperationProposalRead); err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	proposal, err := h.deps.Proposals.Proposal(r.Context(), identity.TenantID, spaceID, proposalID)
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"proposal_id": proposal.ProposalID, "round_id": proposal.RoundID,
		"fingerprint": string(proposal.Fingerprint), "pattern_refs": patternRefsDTO(proposal.PatternRefs),
		"candidate_id": proposal.CandidateID, "created_at": proposal.CreatedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (h *Handler) handleCandidateRead(w http.ResponseWriter, r *http.Request, identity authz.Identity, candidateID string) {
	requestID, ok := queryRequestID(w, r, map[string]bool{"space_id": true, "request_id": true})
	if !ok {
		return
	}
	spaceID := domain.SpaceID(r.URL.Query().Get("space_id"))
	if err := h.authorizeCuration(r.Context(), identity, spaceID, domain.GrantOperationCandidateRead); err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	candidate, err := h.deps.Candidates.Candidate(r.Context(), identity.TenantID, spaceID, candidateID)
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"candidate_id": candidate.CandidateID, "space_id": string(candidate.SpaceID),
		"proposal_id": candidate.ProposalID, "fingerprint": string(candidate.Fingerprint),
		"target_skill_id": candidate.TargetSkillID, "new_skill_name": candidate.NewSkillName,
		"base_artifact_version": candidate.BaseArtifactVersion,
		"diff": map[string]any{
			"diff_id": candidate.Diff.DiffID, "base_artifact_version": candidate.Diff.BaseArtifactVersion,
			"base_artifact_hash": candidate.Diff.BaseArtifactHash, "candidate_artifact_hash": candidate.Diff.CandidateArtifactHash,
			"diff_hash": candidate.Diff.DiffHash, "reviewer": string(candidate.Diff.Reviewer),
			"review_policy_version": candidate.Diff.ReviewPolicyVersion,
			"reviewed_at":           candidate.Diff.ReviewedAt.UTC().Format(time.RFC3339Nano),
		},
		"pattern_refs": patternRefsDTO(candidate.PatternRefs), "status": string(candidate.Status),
		"replay_result_id": candidate.ReplayResultID, "created_at": candidate.CreatedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (h *Handler) handleCandidateDecide(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, candidateID string) {
	if err := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "space_id": true, "decision_id": true, "decision": true,
		"candidate_digest": true, "replay_result_id": true, "policy_version": true, "reason": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var requestID, spaceID, decisionID, decision, digest, replayResultID, policyVersion, reason string
	object.requireString("request_id", &requestID, &errs)
	object.requireString("space_id", &spaceID, &errs)
	object.requireString("decision_id", &decisionID, &errs)
	object.requireString("decision", &decision, &errs)
	object.requireString("candidate_digest", &digest, &errs)
	object.requireString("replay_result_id", &replayResultID, &errs)
	object.requireString("policy_version", &policyVersion, &errs)
	object.requireString("reason", &reason, &errs)
	validateID(&errs, "request_id", requestID)
	validateID(&errs, "space_id", spaceID)
	validateID(&errs, "decision_id", decisionID)
	validateID(&errs, "candidate_digest", digest)
	validateID(&errs, "replay_result_id", replayResultID)
	validateID(&errs, "policy_version", policyVersion)
	if decision != "accepted" && decision != "rejected" {
		addFieldError(&errs, "decision", "decision must be accepted or rejected")
	}
	if byteLen(reason) < 1 || byteLen(reason) > 1000 {
		addFieldError(&errs, "reason", "reason must be 1-1000 UTF-8 bytes")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	ctx := r.Context()
	space := domain.SpaceID(spaceID)
	if _, err := h.deps.Candidates.Candidate(ctx, identity.TenantID, space, candidateID); err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	stored, created, err := h.deps.Curation.Decide(ctx, identity.TenantID, identity.PrincipalID, space, domain.CandidateDecision{
		DecisionID: decisionID, CandidateID: candidateID, Decision: decision,
		CandidateDigest: digest, ReplayResultID: replayResultID,
		PolicyVersion: policyVersion, Reason: reason,
	})
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	duplicate := !created
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"decision": map[string]any{
			"decision_id": stored.DecisionID, "candidate_id": stored.CandidateID,
			"decision": stored.Decision, "candidate_digest": stored.CandidateDigest,
			"replay_result_id": stored.ReplayResultID, "policy_version": stored.PolicyVersion,
			"reason": stored.Reason, "decided_by": string(stored.DecidedBy),
			"decided_at": stored.DecidedAt.UTC().Format(time.RFC3339Nano),
		},
		"duplicate": duplicate,
	})
}

func (h *Handler) handleCandidateActivate(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, candidateID string) {
	if err := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "space_id": true, "activation_id": true, "decision_id": true,
		"replay_result_id": true, "candidate_digest": true, "expected_base_version": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var requestID, spaceID, activationID, decisionID, replayResultID, digest string
	var expectedBaseVersion int64
	object.requireString("request_id", &requestID, &errs)
	object.requireString("space_id", &spaceID, &errs)
	object.requireString("activation_id", &activationID, &errs)
	object.requireString("decision_id", &decisionID, &errs)
	object.requireString("replay_result_id", &replayResultID, &errs)
	object.requireString("candidate_digest", &digest, &errs)
	object.requireInt64("expected_base_version", &expectedBaseVersion, &errs)
	validateID(&errs, "request_id", requestID)
	validateID(&errs, "space_id", spaceID)
	validateID(&errs, "activation_id", activationID)
	validateID(&errs, "decision_id", decisionID)
	validateID(&errs, "replay_result_id", replayResultID)
	validateID(&errs, "candidate_digest", digest)
	if expectedBaseVersion < 1 {
		addFieldError(&errs, "expected_base_version", "expected_base_version must be at least 1")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	stored, created, err := h.deps.Curation.Activate(r.Context(), identity.TenantID, identity.PrincipalID, domain.SpaceID(spaceID), domain.SkillActivation{
		ActivationID: activationID, CandidateID: candidateID, DecisionID: decisionID,
		ReplayResultID: replayResultID, CandidateDigest: digest, ExpectedBaseVersion: expectedBaseVersion,
	})
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	duplicate := !created
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"activation": map[string]any{
			"activation_id": stored.ActivationID, "candidate_id": stored.CandidateID,
			"decision_id": stored.DecisionID, "replay_result_id": stored.ReplayResultID,
			"candidate_digest": stored.CandidateDigest, "expected_base_version": stored.ExpectedBaseVersion,
			"new_artifact_version": stored.NewArtifactVersion, "activated_by": string(stored.ActivatedBy),
			"activated_at": stored.ActivatedAt.UTC().Format(time.RFC3339Nano),
		},
		"duplicate": duplicate,
	})
}
