package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/consolidationcut"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/evidence"
	"river2.dev/graph-memory-service/internal/exploration"
	"river2.dev/graph-memory-service/internal/navigation"
	"river2.dev/graph-memory-service/internal/ports"
	"river2.dev/graph-memory-service/internal/recall"
	"river2.dev/graph-memory-service/internal/skillproposal"
)

type Dependencies struct {
	Token       string
	Registry    ports.RegistryStore
	Evidence    *evidence.Service
	Recall      *recall.Service
	Exploration *exploration.Service
	Navigation  *navigation.Service
	DiveStore   DiveStore
	DiveJudge   DiveJudge
	Patterns    ports.PatternStore
	Proposals   ProposalStore
	Candidates  ports.CandidateStore
	Curation    *skillproposal.Service
	Clock       ports.Clock

	// PG-50A consolidation-cut composition: the Room-scoped cut job service,
	// the production freezer (deployment room→space bindings), and the signed
	// event notifier. All three are optional — when nil, the cut routes fail
	// closed with 404 NOT_FOUND.
	ConsolidationCuts *consolidationcut.Service
	CutFreezer        *RoomFreezer
	CutEvents         *CutEventNotifier
}

type Route struct {
	Method string
	Path   string
}

func Routes() []Route {
	return []Route{
		{http.MethodPut, "/v1/tenant"},
		{http.MethodPost, "/v1/principals"},
		{http.MethodPost, "/v1/spaces"},
		{http.MethodPost, "/v1/grants"},
		{http.MethodPost, "/v1/evidence-batches:stage"},
		{http.MethodPost, "/v1/evidence-batches/{batch_id}:commit"},
		{http.MethodGet, "/v1/evidence-batches/{batch_id}"},
		{http.MethodPost, "/v1/recalls"},
		{http.MethodPost, "/v1/explorations"},
		{http.MethodPost, "/v1/explorations/{session_id}:explore"},
		{http.MethodPost, "/v1/explorations/{session_id}:redirect"},
		{http.MethodPost, "/v1/explorations/{session_id}:submit"},
		{http.MethodPost, "/v1/explorations/{session_id}:navigate"},
		{http.MethodGet, "/v1/explorations/{session_id}/dive"},
		{http.MethodGet, "/v1/patterns/{pattern_id}"},
		{http.MethodPost, "/v1/patterns/{pattern_id}:reject"},
		{http.MethodGet, "/v1/proposals/{proposal_id}"},
		{http.MethodGet, "/v1/candidates/{candidate_id}"},
		{http.MethodPost, "/v1/candidates/{candidate_id}:decide"},
		{http.MethodPost, "/v1/candidates/{candidate_id}:activate"},
	}
}

// ConsolidationCutRoutes documents the consolidation-cut surface, which is a
// SEPARATE contract (openapi/consolidation-cuts.yaml) from the Memory
// Protocol (memory-protocol.yaml). It is intentionally NOT part of Routes() —
// a conformance test pins Routes() to the Memory Protocol spec — and is
// instead conformance-checked against consolidation-cuts.yaml on its own.
// The dispatch lives in ServeHTTP; cancel/rediagnose accept both the frozen
// slash spelling (/cancel, /rediagnose) and the codebase colon convention
// (:cancel, :rediagnose).
func ConsolidationCutRoutes() []Route {
	return []Route{
		{http.MethodPost, "/v1/rooms/{room_id}/consolidation-cuts"},
		{http.MethodGet, "/v1/consolidation-cuts/{cut_id}"},
		{http.MethodPost, "/v1/consolidation-cuts/{cut_id}/cancel"},
		{http.MethodPost, "/v1/consolidation-cuts/{cut_id}/rediagnose"},
	}
}

// Handler is the only public transport adapter. Tenant and acting Principal
// are resolved from the deployment token binding captured at tenant
// initialization; operational payloads never carry or select them.
type Handler struct {
	deps       Dependencies
	auth       *authz.Authenticator
	authorizer *authz.Authorizer

	mu          sync.Mutex
	initialized bool
	tenant      domain.TenantID
	principal   domain.PrincipalID
}

func NewHandler(dependencies Dependencies) http.Handler {
	return &Handler{
		deps:       dependencies,
		auth:       authz.NewAuthenticator(dependencies.Token),
		authorizer: authz.NewAuthorizer(dependencies.Registry, dependencies.Clock),
	}
}

type wireError struct {
	Error wireErrorBody `json:"error"`
}

type wireErrorBody struct {
	Code      string        `json:"code"`
	Message   string        `json:"message"`
	RequestID string        `json:"request_id"`
	Details   []fieldDetail `json:"details"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, err := h.auth.Authenticate(r.Header.Get("Authorization")); err != nil {
		writeTransportError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid bearer token", nil)
		return
	}

	// Every protocol route with a body speaks exactly application/json; a
	// wrong or missing media type is rejected before the body is trusted.
	// A POST/PUT with no body AND no Content-Type header is permitted (the
	// PG-50A cancel route is an empty-body POST); a body without
	// application/json is still rejected.
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		// A body is present unless Content-Length is explicitly 0 (the
		// PG-50A cancel route is an empty-body POST). Zero length → no body →
		// no Content-Type required. A body without application/json is still
		// rejected. -1 (chunked/unknown) counts as having a body.
		hasBody := r.ContentLength != 0
		if hasBody && r.Header.Get("Content-Type") == "" {
			writeTransportError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Content-Type must be application/json", nil)
			return
		}
		if header := r.Header.Get("Content-Type"); header != "" {
			mediaType, _, err := mime.ParseMediaType(header)
			if err != nil || mediaType != "application/json" {
				writeTransportError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "Content-Type must be application/json", nil)
				return
			}
		}
	}

	body, _ := readBody(r)
	var object strictObject
	if len(body) > 0 {
		decoded, decodeErr := decodeStrictObject(body)
		if decodeErr != nil {
			// Consolidation-cut routes use the flat error envelope, not the
			// wrapped Memory Protocol transport error.
			if isCutPath(r.URL.Path) {
				code := "INVALID_REQUEST"
				if decodeErr.jsonError {
					code = "INVALID_JSON"
				}
				writeCutError(w, http.StatusBadRequest, code, "request violates the consolidation-cut contract")
				return
			}
			h.writeInvalidRequest(w, body, decodeErr)
			return
		}
		object = decoded
	}

	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/v1/tenant":
		h.handleInitializeTenant(w, r, object)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/principals":
		h.withBinding(w, r, object, h.handleRegisterPrincipal)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/spaces":
		h.withBinding(w, r, object, h.handleRegisterSpace)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/grants":
		h.withBinding(w, r, object, h.handleRegisterGrant)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/evidence-batches:stage":
		h.withBinding(w, r, object, h.handleStageEvidence)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/evidence-batches/") && strings.HasSuffix(r.URL.Path, ":commit"):
		batchID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/evidence-batches/"), ":commit")
		if !validPathID(batchID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "batch_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handleCommitEvidence(w, r, o, identity, batchID)
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/evidence-batches/"):
		batchID := strings.TrimPrefix(r.URL.Path, "/v1/evidence-batches/")
		if !validPathID(batchID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "batch_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBindingRaw(w, r, func(w http.ResponseWriter, r *http.Request, identity authz.Identity) {
			h.handleEvidenceStatus(w, r, identity, batchID)
		})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/recalls":
		h.withBinding(w, r, object, h.handleRecall)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/explorations":
		h.withBinding(w, r, object, h.handleExplorationStart)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/explorations/") && strings.HasSuffix(r.URL.Path, ":explore"):
		sessionID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/explorations/"), ":explore")
		if !validPathID(sessionID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "session_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handleExplorationStep(w, r, o, identity, sessionID, "explore")
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/explorations/") && strings.HasSuffix(r.URL.Path, ":redirect"):
		sessionID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/explorations/"), ":redirect")
		if !validPathID(sessionID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "session_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handleExplorationStep(w, r, o, identity, sessionID, "redirect")
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/explorations/") && strings.HasSuffix(r.URL.Path, ":submit"):
		sessionID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/explorations/"), ":submit")
		if !validPathID(sessionID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "session_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handleExplorationStep(w, r, o, identity, sessionID, "submit")
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/explorations/") && strings.HasSuffix(r.URL.Path, ":navigate"):
		sessionID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/explorations/"), ":navigate")
		if !validPathID(sessionID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "session_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handleExplorationNavigate(w, r, o, identity, sessionID)
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/explorations/") && strings.HasSuffix(r.URL.Path, "/dive"):
		sessionID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/explorations/"), "/dive")
		if !validPathID(sessionID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "session_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBindingRaw(w, r, func(w http.ResponseWriter, r *http.Request, identity authz.Identity) {
			h.handleDiveRead(w, r, identity, sessionID)
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/patterns/"):
		patternID := strings.TrimPrefix(r.URL.Path, "/v1/patterns/")
		if !validPathID(patternID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "pattern_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBindingRaw(w, r, func(w http.ResponseWriter, r *http.Request, identity authz.Identity) {
			h.handlePatternRead(w, r, identity, patternID)
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/patterns/") && strings.HasSuffix(r.URL.Path, ":reject"):
		patternID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/patterns/"), ":reject")
		if !validPathID(patternID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "pattern_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handlePatternReject(w, r, o, identity, patternID)
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/proposals/"):
		proposalID := strings.TrimPrefix(r.URL.Path, "/v1/proposals/")
		if !validPathID(proposalID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "proposal_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBindingRaw(w, r, func(w http.ResponseWriter, r *http.Request, identity authz.Identity) {
			h.handleProposalRead(w, r, identity, proposalID)
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/candidates/"):
		candidateID := strings.TrimPrefix(r.URL.Path, "/v1/candidates/")
		if !validPathID(candidateID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "candidate_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBindingRaw(w, r, func(w http.ResponseWriter, r *http.Request, identity authz.Identity) {
			h.handleCandidateRead(w, r, identity, candidateID)
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/candidates/") && strings.HasSuffix(r.URL.Path, ":decide"):
		candidateID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/candidates/"), ":decide")
		if !validPathID(candidateID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "candidate_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handleCandidateDecide(w, r, o, identity, candidateID)
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/candidates/") && strings.HasSuffix(r.URL.Path, ":activate"):
		candidateID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/candidates/"), ":activate")
		if !validPathID(candidateID) {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "candidate_id must be 1-128 UTF-8 bytes", nil)
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.handleCandidateActivate(w, r, o, identity, candidateID)
		})
	// PG-50A consolidation-cut composition.
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/rooms/") && strings.HasSuffix(r.URL.Path, "/consolidation-cuts"):
		roomID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/rooms/"), "/consolidation-cuts")
		if !validPathID(roomID) {
			writeCutError(w, http.StatusBadRequest, "PAYLOAD_VALIDATION_FAILED", "room_id must be 1-128 UTF-8 bytes")
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.createConsolidationCut(w, r, o, identity, roomID)
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/consolidation-cuts/"):
		cutID := strings.TrimPrefix(r.URL.Path, "/v1/consolidation-cuts/")
		if !validPathID(cutID) {
			writeCutError(w, http.StatusBadRequest, "PAYLOAD_VALIDATION_FAILED", "cut_id must be 1-128 UTF-8 bytes")
			return
		}
		h.withBindingRaw(w, r, func(w http.ResponseWriter, r *http.Request, identity authz.Identity) {
			h.getConsolidationCut(w, r, identity, cutID)
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/consolidation-cuts/") && (strings.HasSuffix(r.URL.Path, ":cancel") || strings.HasSuffix(r.URL.Path, "/cancel")):
		cutID := trimCutAction(r.URL.Path, "/cancel", ":cancel")
		if !validPathID(cutID) {
			writeCutError(w, http.StatusBadRequest, "PAYLOAD_VALIDATION_FAILED", "cut_id must be 1-128 UTF-8 bytes")
			return
		}
		h.withBindingRaw(w, r, func(w http.ResponseWriter, r *http.Request, identity authz.Identity) {
			h.cancelConsolidationCut(w, r, object, identity, cutID)
		})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/consolidation-cuts/") && (strings.HasSuffix(r.URL.Path, ":rediagnose") || strings.HasSuffix(r.URL.Path, "/rediagnose")):
		cutID := trimCutAction(r.URL.Path, "/rediagnose", ":rediagnose")
		if !validPathID(cutID) {
			writeCutError(w, http.StatusBadRequest, "PAYLOAD_VALIDATION_FAILED", "cut_id must be 1-128 UTF-8 bytes")
			return
		}
		h.withBinding(w, r, object, func(w http.ResponseWriter, r *http.Request, o strictObject, identity authz.Identity) {
			h.rediagnoseConsolidationCut(w, r, o, identity, cutID)
		})
	default:
		writeTransportError(w, r, http.StatusNotFound, "NOT_FOUND", "unknown protocol route", nil)
	}
}

// trimCutAction strips either the slash or the colon action suffix from the
// trailing cut route, returning the cut_id. Both spellings are accepted so
// the frozen consolidation-cuts.yaml (/cancel, /rediagnose) and the codebase
// colon convention (:cancel, :rediagnose) both resolve.
func trimCutAction(path, slashSuffix, colonSuffix string) string {
	prefix := "/v1/consolidation-cuts/"
	if strings.HasSuffix(path, colonSuffix) {
		return strings.TrimSuffix(strings.TrimPrefix(path, prefix), colonSuffix)
	}
	return strings.TrimSuffix(strings.TrimPrefix(path, prefix), slashSuffix)
}

func validPathID(id string) bool {
	return byteLen(id) >= 1 && byteLen(id) <= 128
}

func (h *Handler) identity() (authz.Identity, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.initialized {
		return authz.Identity{}, false
	}
	return authz.Identity{TenantID: h.tenant, PrincipalID: h.principal}, true
}

func (h *Handler) setBinding(tenant domain.TenantID, principal domain.PrincipalID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.initialized = true
	h.tenant = tenant
	h.principal = principal
}

func (h *Handler) withBinding(w http.ResponseWriter, r *http.Request, object strictObject, handle func(http.ResponseWriter, *http.Request, strictObject, authz.Identity)) {
	identity, ok := h.identity()
	if !ok {
		writeTransportError(w, r, http.StatusConflict, "TENANT_NOT_INITIALIZED", "tenant must be initialized before operational requests", nil)
		return
	}
	handle(w, r, object, identity)
}

func (h *Handler) withBindingRaw(w http.ResponseWriter, r *http.Request, handle func(http.ResponseWriter, *http.Request, authz.Identity)) {
	identity, ok := h.identity()
	if !ok {
		writeTransportError(w, r, http.StatusConflict, "TENANT_NOT_INITIALIZED", "tenant must be initialized before operational requests", nil)
		return
	}
	handle(w, r, identity)
}

// RestoreBinding re-establishes the token→tenant binding when a server
// restarts from a persisted store. Server wiring only: the pair must come from
// the restored server state, not from any request payload.
func (h *Handler) RestoreBinding(tenantID domain.TenantID, principalID domain.PrincipalID) {
	h.setBinding(tenantID, principalID)
}

func (h *Handler) handleInitializeTenant(w http.ResponseWriter, r *http.Request, object strictObject) {
	if err := object.rejectUnknownFields(map[string]bool{
		"tenant_id": true, "display_name": true, "bootstrap_principal_id": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var tenantID, displayName, bootstrapID string
	object.requireString("tenant_id", &tenantID, &errs)
	object.requireString("display_name", &displayName, &errs)
	object.requireString("bootstrap_principal_id", &bootstrapID, &errs)
	if !validateID(&errs, "tenant_id", tenantID) || !validateID(&errs, "bootstrap_principal_id", bootstrapID) {
		// length violations recorded inside validateID
	}
	if byteLen(displayName) < 1 || byteLen(displayName) > 200 {
		addFieldError(&errs, "display_name", "display_name must be 1-200 UTF-8 bytes")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	tenant := domain.Tenant{ID: domain.TenantID(tenantID), DisplayName: displayName, BootstrapPrincipalID: domain.PrincipalID(bootstrapID)}
	bootstrap := domain.Principal{ID: domain.PrincipalID(bootstrapID), TenantID: tenant.ID, Kind: domain.PrincipalService, DisplayName: "Bootstrap service principal"}
	created, err := h.deps.Registry.InitializeTenant(r.Context(), tenant, bootstrap)
	if err != nil {
		h.writeServiceError(w, "", err)
		return
	}
	if created {
		h.setBinding(tenant.ID, bootstrap.ID)
		writeJSON(w, http.StatusCreated, map[string]any{
			"tenant_id": tenantID, "bootstrap_principal_id": bootstrapID, "duplicate": false,
		})
		return
	}
	h.setBinding(tenant.ID, bootstrap.ID)
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id": tenantID, "bootstrap_principal_id": bootstrapID, "duplicate": true,
	})
}

func (h *Handler) handleRegisterPrincipal(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity) {
	if err := object.rejectUnknownFields(map[string]bool{
		"principal_id": true, "kind": true, "display_name": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var principalID, kind, displayName string
	object.requireString("principal_id", &principalID, &errs)
	object.requireString("kind", &kind, &errs)
	object.requireString("display_name", &displayName, &errs)
	validateID(&errs, "principal_id", principalID)
	if kind != "human" && kind != "agent" && kind != "service" {
		addFieldError(&errs, "kind", "kind must be human, agent, or service")
	}
	if byteLen(displayName) < 1 || byteLen(displayName) > 200 {
		addFieldError(&errs, "display_name", "display_name must be 1-200 UTF-8 bytes")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	principal := domain.Principal{ID: domain.PrincipalID(principalID), TenantID: identity.TenantID, Kind: domain.PrincipalKind(kind), DisplayName: displayName}
	created, err := h.deps.Registry.PutPrincipal(r.Context(), identity.TenantID, principal)
	if err != nil {
		h.writeServiceError(w, "", err)
		return
	}
	writeCreated(w, created, map[string]any{
		"principal_id": principalID, "kind": kind, "duplicate": !created,
	})
}

func (h *Handler) handleRegisterSpace(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity) {
	if err := object.rejectUnknownFields(map[string]bool{
		"space_id": true, "scope": true, "owner_principal_id": true, "display_name": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var spaceID, scope, displayName string
	var owner *string
	object.requireString("space_id", &spaceID, &errs)
	object.requireString("scope", &scope, &errs)
	object.requireNullableString("owner_principal_id", &owner, &errs)
	object.requireString("display_name", &displayName, &errs)
	validateID(&errs, "space_id", spaceID)
	if scope != "shared" && scope != "private" {
		addFieldError(&errs, "scope", "scope must be shared or private")
	}
	if byteLen(displayName) < 1 || byteLen(displayName) > 200 {
		addFieldError(&errs, "display_name", "display_name must be 1-200 UTF-8 bytes")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	var ownerID *domain.PrincipalID
	if scope == "shared" {
		if owner != nil {
			writeError(w, r, http.StatusUnprocessableEntity, "SHARED_SPACE_OWNER_FORBIDDEN", "shared spaces have no owner principal", nil)
			return
		}
	} else {
		if owner == nil {
			writeError(w, r, http.StatusUnprocessableEntity, "PRIVATE_SPACE_OWNER_REQUIRED", "private spaces require an owner principal", nil)
			return
		}
		if !validateID(&errs, "owner_principal_id", *owner) {
			h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
			return
		}
		if _, err := h.deps.Registry.Principal(r.Context(), identity.TenantID, domain.PrincipalID(*owner)); err != nil {
			writeError(w, r, http.StatusUnprocessableEntity, "PRINCIPAL_NOT_FOUND", "owner principal does not exist", nil)
			return
		}
		id := domain.PrincipalID(*owner)
		ownerID = &id
	}

	space := domain.Space{ID: domain.SpaceID(spaceID), TenantID: identity.TenantID, Scope: domain.SpaceScope(scope), OwnerPrincipalID: ownerID, DisplayName: displayName, Version: 0}
	created, err := h.deps.Registry.PutSpace(r.Context(), identity.TenantID, space)
	if err != nil {
		h.writeServiceError(w, "", err)
		return
	}
	writeCreated(w, created, map[string]any{
		"space_id": spaceID, "scope": scope, "owner_principal_id": owner, "duplicate": !created,
	})
}

func (h *Handler) handleRegisterGrant(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity) {
	if err := object.rejectUnknownFields(map[string]bool{
		"grant_id": true, "principal_id": true, "space_ids": true, "purpose": true, "operations": true, "expires_at": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var grantID, principalID, purpose, expiresAt string
	var spaceIDs, operations []string
	object.requireString("grant_id", &grantID, &errs)
	object.requireString("principal_id", &principalID, &errs)
	object.requireStringArray("space_ids", &spaceIDs, &errs)
	object.requireString("purpose", &purpose, &errs)
	object.requireStringArray("operations", &operations, &errs)
	object.requireString("expires_at", &expiresAt, &errs)
	validateID(&errs, "grant_id", grantID)
	validateID(&errs, "principal_id", principalID)
	if purpose != "lifecycle" && purpose != "tool_plane" && purpose != "curation" {
		addFieldError(&errs, "purpose", "purpose must be lifecycle, tool_plane, or curation")
	}
	if len(spaceIDs) == 0 {
		addFieldError(&errs, "space_ids", "space_ids must be a non-empty exact set")
	} else if hasDuplicates(spaceIDs) {
		addFieldError(&errs, "space_ids", "space_ids must not contain duplicates")
	}
	validOperations := map[string]bool{
		"evidence.stage": true, "evidence.commit": true, "recall": true,
		"exploration.start": true, "exploration.explore": true, "exploration.redirect": true, "exploration.submit": true,
		"dive.read":           true,
		"causal.trial.append": true, "causal.estimate.append": true, "causal.reward.append": true, "causal.read": true,
		"pattern.read": true, "pattern.reject": true, "proposal.read": true,
		"candidate.read": true, "candidate.decide": true, "candidate.activate": true,
	}
	if len(operations) == 0 {
		addFieldError(&errs, "operations", "operations must be a non-empty exact set")
	} else {
		for _, operation := range operations {
			if !validOperations[operation] {
				addFieldError(&errs, "operations", "unknown grant operation "+operation)
			}
		}
		if hasDuplicates(operations) {
			addFieldError(&errs, "operations", "operations must not contain duplicates")
		}
	}
	if errs == nil || len(errs) == 0 {
		if _, ok := parseTimestamp(expiresAt); !ok {
			addFieldError(&errs, "expires_at", "expires_at must be canonical UTC RFC3339Nano")
		}
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	expiry, _ := parseTimestamp(expiresAt)
	if !h.deps.Clock.Now().Before(expiry) {
		writeError(w, r, http.StatusUnprocessableEntity, "EXPIRY_NOT_FUTURE", "expires_at must be strictly after server time", nil)
		return
	}
	if _, err := h.deps.Registry.Principal(r.Context(), identity.TenantID, domain.PrincipalID(principalID)); err != nil {
		writeError(w, r, http.StatusUnprocessableEntity, "PRINCIPAL_NOT_FOUND", "grantee principal does not exist", nil)
		return
	}
	for _, spaceID := range spaceIDs {
		if byteLen(spaceID) < 1 || byteLen(spaceID) > 128 {
			writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "space id is malformed", nil)
			return
		}
		if _, err := h.deps.Registry.Space(r.Context(), identity.TenantID, domain.SpaceID(spaceID)); err != nil {
			h.writeServiceError(w, "", err)
			return
		}
	}

	spaceIDValues := make([]domain.SpaceID, 0, len(spaceIDs))
	for _, spaceID := range spaceIDs {
		spaceIDValues = append(spaceIDValues, domain.SpaceID(spaceID))
	}
	operationValues := make([]domain.GrantOperation, 0, len(operations))
	for _, operation := range operations {
		operationValues = append(operationValues, domain.GrantOperation(operation))
	}
	grant := domain.Grant{
		ID: domain.GrantID(grantID), TenantID: identity.TenantID, PrincipalID: domain.PrincipalID(principalID),
		SpaceIDs: spaceIDValues, Purpose: domain.GrantPurpose(purpose), Operations: operationValues, ExpiresAt: expiry,
	}
	created, err := h.deps.Registry.PutGrant(r.Context(), identity.TenantID, grant)
	if err != nil {
		h.writeServiceError(w, "", err)
		return
	}
	writeCreated(w, created, map[string]any{
		"grant_id": grantID, "principal_id": principalID, "space_ids": spaceIDs,
		"purpose": purpose, "operations": operations, "expires_at": expiresAt, "duplicate": !created,
	})
}

func (h *Handler) handleStageEvidence(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity) {
	request, decodeErr := decodeStageRequest(object)
	if decodeErr != nil {
		h.writeInvalidRequest(w, nil, decodeErr)
		return
	}
	if _, spaceErr := h.deps.Registry.Space(r.Context(), identity.TenantID, domain.SpaceID(request.spaceID)); spaceErr != nil {
		h.writeServiceError(w, "", spaceErr)
		return
	}

	batch := domain.EvidenceBatch{
		ID: domain.BatchID(request.batchID), TenantID: identity.TenantID, IdempotencyKey: request.idempotencyKey,
		SpaceID: domain.SpaceID(request.spaceID), StreamID: request.streamID, SourceSegmentID: request.sourceSegmentID,
		Provenance: request.provenance, Events: request.events, Links: request.links, TerminalOutcome: request.terminalOutcome,
	}
	stored, duplicate, stageErr := h.deps.Evidence.Stage(r.Context(), identity.TenantID, identity.PrincipalID, batch)
	if stageErr != nil {
		h.writeServiceError(w, "", stageErr)
		return
	}
	status := http.StatusOK
	if !duplicate {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{
		"batch_id": string(stored.ID), "idempotency_key": stored.IdempotencyKey,
		"space_id": string(stored.SpaceID), "state": string(stored.State), "duplicate": duplicate,
	})
}

func (h *Handler) handleCommitEvidence(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, batchID string) {
	if err := object.rejectUnknownFields(map[string]bool{"commit_id": true}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var commitID string
	object.requireString("commit_id", &commitID, &errs)
	validateID(&errs, "commit_id", commitID)
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	batch, duplicate, err := h.deps.Evidence.Commit(r.Context(), identity.TenantID, identity.PrincipalID, domain.BatchID(batchID), commitID)
	if err != nil {
		h.writeServiceError(w, "", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"batch_id": string(batch.ID), "commit_id": commitID, "state": string(batch.State),
		"memory_version": *batch.MemoryVersion, "committed_at": batch.CommittedAt.UTC().Format(time.RFC3339Nano),
		"duplicate": duplicate,
	})
}

func (h *Handler) handleEvidenceStatus(w http.ResponseWriter, r *http.Request, identity authz.Identity, batchID string) {
	batch, err := h.deps.Evidence.Batch(r.Context(), identity.TenantID, identity.PrincipalID, domain.BatchID(batchID))
	if err != nil {
		h.writeServiceError(w, "", err)
		return
	}
	var version any
	if batch.MemoryVersion != nil {
		version = *batch.MemoryVersion
	}
	var committedAt any
	if batch.CommittedAt != nil {
		committedAt = batch.CommittedAt.UTC().Format(time.RFC3339Nano)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"batch_id": string(batch.ID), "idempotency_key": batch.IdempotencyKey, "space_id": string(batch.SpaceID),
		"source_segment_id": batch.SourceSegmentID, "state": string(batch.State),
		"memory_version": version, "committed_at": committedAt,
	})
}

func (h *Handler) handleRecall(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity) {
	if err := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "query": true, "space_ids": true, "space_versions": true, "max_results": true, "deadline_ms": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var requestID, query string
	var spaceIDs []string
	var spaceVersions map[string]int64
	var maxResults, deadlineMS int
	object.requireString("request_id", &requestID, &errs)
	object.requireString("query", &query, &errs)
	object.requireStringArray("space_ids", &spaceIDs, &errs)
	if raw, ok := object["space_versions"]; ok {
		if err := json.Unmarshal(raw, &spaceVersions); err != nil {
			markJSONError(&errs)
		}
	}
	object.requireInt("max_results", &maxResults, &errs)
	object.requireInt("deadline_ms", &deadlineMS, &errs)
	validateID(&errs, "request_id", requestID)
	if byteLen(query) < 1 || byteLen(query) > 16000 {
		addFieldError(&errs, "query", "query must be 1-16000 UTF-8 bytes")
	}
	if len(spaceIDs) == 0 || hasDuplicates(spaceIDs) {
		addFieldError(&errs, "space_ids", "space_ids must be a non-empty list without duplicates")
	}
	for index, spaceID := range spaceIDs {
		validateID(&errs, "space_ids["+strconv.Itoa(index)+"]", spaceID)
	}
	if maxResults < 1 || maxResults > 100 {
		addFieldError(&errs, "max_results", "max_results must be 1-100")
	}
	if deadlineMS < 1 || deadlineMS > 30000 {
		addFieldError(&errs, "deadline_ms", "deadline_ms must be 1-30000")
	}
	knownSpaces := make(map[string]bool, len(spaceIDs))
	for _, spaceID := range spaceIDs {
		knownSpaces[spaceID] = true
	}
	pinnedVersions := make(map[domain.SpaceID]int64, len(spaceVersions))
	for spaceID, version := range spaceVersions {
		if !knownSpaces[spaceID] {
			addFieldError(&errs, "space_versions", "space_versions keys must be a subset of space_ids")
			break
		}
		if version < 0 {
			addFieldError(&errs, "space_versions", "space_versions values must not be negative")
			break
		}
		pinnedVersions[domain.SpaceID(spaceID)] = version
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	spaceIDValues := make([]domain.SpaceID, 0, len(spaceIDs))
	for _, spaceID := range spaceIDs {
		spaceIDValues = append(spaceIDValues, domain.SpaceID(spaceID))
	}
	result, err := h.deps.Recall.Recall(r.Context(), identity.TenantID, identity.PrincipalID, domain.RecallRequest{
		RequestID: requestID, Query: query, SpaceIDs: spaceIDValues, SpaceVersions: pinnedVersions, MaxResults: maxResults, DeadlineMS: deadlineMS,
	})
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	writeJSON(w, http.StatusOK, recallResultDTO(result))
}

func (h *Handler) handleExplorationStart(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity) {
	if err := object.rejectUnknownFields(map[string]bool{
		"request_id": true, "idempotency_key": true, "space_ids": true, "query": true, "max_steps": true, "max_results": true,
	}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var requestID, idempotencyKey, query string
	var spaceIDs []string
	var maxSteps, maxResults int
	object.requireString("request_id", &requestID, &errs)
	object.requireString("idempotency_key", &idempotencyKey, &errs)
	object.requireStringArray("space_ids", &spaceIDs, &errs)
	object.requireString("query", &query, &errs)
	object.requireInt("max_steps", &maxSteps, &errs)
	object.requireInt("max_results", &maxResults, &errs)
	validateID(&errs, "request_id", requestID)
	validateID(&errs, "idempotency_key", idempotencyKey)
	if byteLen(query) < 1 || byteLen(query) > 16000 {
		addFieldError(&errs, "query", "query must be 1-16000 UTF-8 bytes")
	}
	if len(spaceIDs) == 0 || hasDuplicates(spaceIDs) {
		addFieldError(&errs, "space_ids", "space_ids must be a non-empty list without duplicates")
	}
	for index, spaceID := range spaceIDs {
		validateID(&errs, "space_ids["+strconv.Itoa(index)+"]", spaceID)
	}
	if maxSteps < 1 || maxSteps > 20 {
		addFieldError(&errs, "max_steps", "max_steps must be 1-20")
	}
	if maxResults < 1 || maxResults > 100 {
		addFieldError(&errs, "max_results", "max_results must be 1-100")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}

	spaceIDValues := make([]domain.SpaceID, 0, len(spaceIDs))
	for _, spaceID := range spaceIDs {
		spaceIDValues = append(spaceIDValues, domain.SpaceID(spaceID))
	}
	result, err := h.deps.Exploration.Start(r.Context(), identity.TenantID, identity.PrincipalID, exploration.StartRequest{
		RequestID: requestID, IdempotencyKey: idempotencyKey, SpaceIDs: spaceIDValues,
		Query: query, MaxSteps: maxSteps, MaxResults: maxResults,
	})
	if err != nil {
		h.writeServiceError(w, requestID, err)
		return
	}
	pinned := make([]map[string]any, 0, len(result.Session.PinnedSpaces))
	for _, pin := range result.Session.PinnedSpaces {
		pinned = append(pinned, map[string]any{"space_id": string(pin.SpaceID), "memory_version": pin.MemoryVersion})
	}
	status := http.StatusOK
	if !result.Duplicate {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{
		"session_id": string(result.Session.ID), "request_id": requestID, "state": result.Session.State,
		"pinned_spaces": pinned, "items": recallItemsDTO(result.Items), "remaining_steps": result.RemainingSteps,
		"duplicate": result.Duplicate,
	})
}

func (h *Handler) handleExplorationStep(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, sessionID, kind string) {
	var errs []fieldDetail
	var operationID string
	var items []domain.RecallItem
	var remaining int
	var duplicate bool
	var state string
	var found bool
	var summary string
	var citations []exploration.SubmittedCitation

	switch kind {
	case "explore":
		if err := object.rejectUnknownFields(map[string]bool{
			"operation_id": true, "anchor_citation_id": true, "relation": true, "limit": true,
		}); err != nil {
			h.writeInvalidRequest(w, nil, err)
			return
		}
		var anchor, relation string
		var limit int
		object.requireString("operation_id", &operationID, &errs)
		object.requireString("anchor_citation_id", &anchor, &errs)
		object.requireString("relation", &relation, &errs)
		object.requireInt("limit", &limit, &errs)
		validateID(&errs, "operation_id", operationID)
		validateID(&errs, "anchor_citation_id", anchor)
		switch relation {
		case "mentions", "responds_to", "continues", "delegates_to", "related":
		default:
			addFieldError(&errs, "relation", "relation must be an explicit exploration relation")
		}
		if limit < 1 || limit > 100 {
			addFieldError(&errs, "limit", "limit must be 1-100")
		}
		if len(errs) > 0 {
			h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
			return
		}
		result, err := h.deps.Exploration.Explore(r.Context(), identity.TenantID, identity.PrincipalID, domain.ExplorationSessionID(sessionID), exploration.ExploreRequest{
			OperationID: operationID, AnchorCitationID: anchor, Relation: relation, Limit: limit,
		})
		if err != nil {
			h.writeServiceError(w, "", err)
			return
		}
		items, remaining, duplicate, state = result.Items, result.RemainingSteps, result.Duplicate, result.State
	case "redirect":
		if err := object.rejectUnknownFields(map[string]bool{
			"operation_id": true, "query": true, "anchor_citation_ids": true, "reason": true,
		}); err != nil {
			h.writeInvalidRequest(w, nil, err)
			return
		}
		var query, reason string
		var anchors []string
		object.requireString("operation_id", &operationID, &errs)
		object.requireString("query", &query, &errs)
		object.requireStringArray("anchor_citation_ids", &anchors, &errs)
		object.requireString("reason", &reason, &errs)
		validateID(&errs, "operation_id", operationID)
		if byteLen(query) < 1 || byteLen(query) > 16000 {
			addFieldError(&errs, "query", "query must be 1-16000 UTF-8 bytes")
		}
		if byteLen(reason) < 1 || byteLen(reason) > 1000 {
			addFieldError(&errs, "reason", "reason must be 1-1000 UTF-8 bytes")
		}
		for index, anchor := range anchors {
			validateID(&errs, "anchor_citation_ids["+strconv.Itoa(index)+"]", anchor)
		}
		if len(errs) > 0 {
			h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
			return
		}
		result, err := h.deps.Exploration.Redirect(r.Context(), identity.TenantID, identity.PrincipalID, domain.ExplorationSessionID(sessionID), exploration.RedirectRequest{
			OperationID: operationID, Query: query, AnchorCitationIDs: anchors, Reason: reason,
		})
		if err != nil {
			h.writeServiceError(w, "", err)
			return
		}
		items, remaining, duplicate, state = result.Items, result.RemainingSteps, result.Duplicate, result.State
	case "submit":
		if err := object.rejectUnknownFields(map[string]bool{
			"operation_id": true, "found": true, "summary": true, "citation_ids": true,
		}); err != nil {
			h.writeInvalidRequest(w, nil, err)
			return
		}
		var citationIDs []string
		object.requireString("operation_id", &operationID, &errs)
		object.requireBool("found", &found, &errs)
		object.requireString("summary", &summary, &errs)
		object.requireStringArray("citation_ids", &citationIDs, &errs)
		validateID(&errs, "operation_id", operationID)
		if found && len(citationIDs) == 0 {
			addFieldError(&errs, "citation_ids", "found submissions must cite session-served evidence")
		}
		if found && byteLen(summary) < 1 {
			addFieldError(&errs, "summary", "found submissions must include a non-empty summary")
		}
		if !found && byteLen(summary) > 0 {
			addFieldError(&errs, "summary", "summary must be empty when found is false")
		}
		for index, citationID := range citationIDs {
			validateID(&errs, "citation_ids["+strconv.Itoa(index)+"]", citationID)
		}
		if len(errs) > 0 {
			h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
			return
		}
		result, err := h.deps.Exploration.Submit(r.Context(), identity.TenantID, identity.PrincipalID, domain.ExplorationSessionID(sessionID), exploration.SubmitRequest{
			OperationID: operationID, Found: found, Summary: summary, CitationIDs: citationIDs,
		})
		if err != nil {
			h.writeServiceError(w, "", err)
			return
		}
		// The server judges every terminal submission; the trajectory is
		// frozen from server-owned state before scoring.
		h.judgeDiveSubmission(r.Context(), identity, domain.ExplorationSessionID(sessionID), result)
		duplicate = result.Duplicate
		state = result.State
		citations = result.Citations
	}

	switch kind {
	case "submit":
		citationDTOs := make([]map[string]any, 0, len(citations))
		for _, citation := range citations {
			citationDTOs = append(citationDTOs, map[string]any{
				"citation_id": citation.CitationID, "source_space_id": string(citation.SourceSpaceID),
				"memory_version": citation.MemoryVersion, "evidence_batch_id": string(citation.EvidenceBatchID),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"session_id": sessionID, "operation_id": operationID, "state": state,
			"found": found, "summary": summary, "citations": citationDTOs, "duplicate": duplicate,
		})
	default:
		writeJSON(w, http.StatusOK, map[string]any{
			"session_id": sessionID, "operation_id": operationID, "state": state,
			"items": recallItemsDTO(items), "remaining_steps": remaining, "duplicate": duplicate,
		})
	}
}

func (h *Handler) handleExplorationNavigate(w http.ResponseWriter, r *http.Request, object strictObject, identity authz.Identity, sessionID string) {
	if err := object.rejectUnknownFields(map[string]bool{"run_id": true, "max_model_calls": true}); err != nil {
		h.writeInvalidRequest(w, nil, err)
		return
	}
	var errs []fieldDetail
	var runID string
	var maxModelCalls int
	object.requireString("run_id", &runID, &errs)
	object.requireInt("max_model_calls", &maxModelCalls, &errs)
	validateID(&errs, "run_id", runID)
	if maxModelCalls < 1 || maxModelCalls > 16 {
		addFieldError(&errs, "max_model_calls", "max_model_calls must be 1-16")
	}
	if len(errs) > 0 {
		h.writeInvalidRequest(w, nil, invalidRequestFrom(errs))
		return
	}
	if h.deps.Navigation == nil {
		h.writeServiceError(w, "", domain.NewProtocolError(503, "PATH_SELECTION_UNAVAILABLE", "path selection is unavailable"))
		return
	}
	result, err := h.deps.Navigation.Navigate(r.Context(), identity.TenantID, identity.PrincipalID, domain.ExplorationSessionID(sessionID), navigation.Request{
		RunID: runID, MaxModelCalls: maxModelCalls,
	})
	if err != nil {
		h.writeServiceError(w, "", err)
		return
	}
	citationDTOs := make([]map[string]any, 0, len(result.Citations))
	for _, citation := range result.Citations {
		citationDTOs = append(citationDTOs, map[string]any{
			"citation_id": citation.CitationID, "source_space_id": string(citation.SourceSpaceID),
			"memory_version": citation.MemoryVersion, "evidence_batch_id": string(citation.EvidenceBatchID),
		})
	}
	if result.State == "submitted" {
		h.judgeDiveSubmission(r.Context(), identity, domain.ExplorationSessionID(sessionID), exploration.SubmitResult{
			SessionID: domain.ExplorationSessionID(sessionID), State: result.State, Found: result.Found,
			Summary: result.Summary, Citations: result.Citations, Duplicate: result.Duplicate,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"state": result.State, "found": result.Found, "summary": result.Summary,
		"citations": citationDTOs, "steps": result.Steps, "applied_policy": result.AppliedPolicy,
		"degradation": result.Degradation, "duplicate": result.Duplicate,
	})
}

type stageRequestValues struct {
	batchID, idempotencyKey, spaceID, streamID, sourceSegmentID string
	provenance                                                  domain.Provenance
	events                                                      []domain.EvidenceEvent
	links                                                       []domain.EvidenceLink
	terminalOutcome                                             string
}

func decodeStageRequest(object strictObject) (*stageRequestValues, *invalidRequest) {
	if err := object.rejectUnknownFields(map[string]bool{
		"batch_id": true, "idempotency_key": true, "space_id": true, "stream_id": true, "source_segment_id": true,
		"provenance": true, "events": true, "links": true, "terminal_outcome": true,
	}); err != nil {
		return nil, err
	}
	var errs []fieldDetail
	var values stageRequestValues
	object.requireString("batch_id", &values.batchID, &errs)
	object.requireString("idempotency_key", &values.idempotencyKey, &errs)
	object.requireString("space_id", &values.spaceID, &errs)
	object.requireString("stream_id", &values.streamID, &errs)
	object.requireString("source_segment_id", &values.sourceSegmentID, &errs)
	object.requireString("terminal_outcome", &values.terminalOutcome, &errs)
	validateID(&errs, "batch_id", values.batchID)
	validateID(&errs, "idempotency_key", values.idempotencyKey)
	validateID(&errs, "space_id", values.spaceID)
	validateID(&errs, "stream_id", values.streamID)
	validateID(&errs, "source_segment_id", values.sourceSegmentID)
	switch values.terminalOutcome {
	case "settled", "failed", "aborted":
	default:
		addFieldError(&errs, "terminal_outcome", "terminal_outcome must be settled, failed, or aborted")
	}

	provenanceRaw, ok := object["provenance"]
	if !ok {
		addFieldError(&errs, "provenance", "required field is missing")
	} else {
		var provenanceObject map[string]json.RawMessage
		if err := json.Unmarshal(provenanceRaw, &provenanceObject); err != nil {
			markJSONError(&errs)
		} else {
			allowed := map[string]bool{"host_type": true, "host_instance_id": true, "source_kind": true, "captured_at": true, "content_sha256": true}
			var pErrs []fieldDetail
			for key := range provenanceObject {
				if !allowed[key] {
					addFieldError(&pErrs, "provenance."+key, "unknown field")
				}
			}
			var hostType, instanceID, sourceKind, capturedAt, sha string
			requireProvenanceString(provenanceObject, "host_type", &hostType, &pErrs)
			requireProvenanceString(provenanceObject, "host_instance_id", &instanceID, &pErrs)
			requireProvenanceString(provenanceObject, "source_kind", &sourceKind, &pErrs)
			requireProvenanceString(provenanceObject, "captured_at", &capturedAt, &pErrs)
			requireProvenanceString(provenanceObject, "content_sha256", &sha, &pErrs)
			if len(pErrs) == 0 {
				if hostType != "pi-group-chat-host" {
					addFieldError(&pErrs, "provenance.host_type", "host_type must be pi-group-chat-host for this tracer")
				}
				if byteLen(instanceID) < 1 || byteLen(instanceID) > 128 {
					addFieldError(&pErrs, "provenance.host_instance_id", "host_instance_id must be 1-128 UTF-8 bytes")
				}
				if sourceKind != "room_shared" && sourceKind != "agent_private" {
					addFieldError(&pErrs, "provenance.source_kind", "source_kind must be room_shared or agent_private")
				}
				capturedAtTime, ok := parseTimestamp(capturedAt)
				if !ok {
					addFieldError(&pErrs, "provenance.captured_at", "captured_at must be canonical UTC RFC3339Nano")
				} else {
					values.provenance.CapturedAt = capturedAtTime
				}
				if !isLowercaseHex64(sha) {
					addFieldError(&pErrs, "provenance.content_sha256", "content_sha256 must be 64 lowercase hex characters")
				}
				values.provenance.HostType = hostType
				values.provenance.HostInstanceID = instanceID
				values.provenance.SourceKind = sourceKind
				values.provenance.ContentSHA256 = sha
			}
			errs = append(errs, pErrs...)
		}
	}

	eventsRaw, ok := object["events"]
	if !ok {
		addFieldError(&errs, "events", "required field is missing")
	} else {
		var events []map[string]json.RawMessage
		if err := json.Unmarshal(eventsRaw, &events); err != nil {
			markJSONError(&errs)
		} else {
			eventIDs := make(map[string]bool, len(events))
			previousSequence := int64(0)
			allowedEventKeys := map[string]bool{"event_id": true, "sequence": true, "kind": true, "content": true, "occurred_at": true}
			for index, eventObject := range events {
				field := func(name string) string { return "events[" + itoa(index) + "]." + name }
				var eErrs []fieldDetail
				var id string
				var sequence int64
				var kind, content, occurredAt string
				for key := range eventObject {
					if !allowedEventKeys[key] {
						addFieldError(&eErrs, field(key), "unknown field")
					}
				}
				requireProvenanceString(eventObject, "event_id", &id, &eErrs)
				requireInt64Field(eventObject, "sequence", &sequence, &eErrs)
				requireProvenanceString(eventObject, "kind", &kind, &eErrs)
				requireProvenanceString(eventObject, "content", &content, &eErrs)
				requireProvenanceString(eventObject, "occurred_at", &occurredAt, &eErrs)
				if len(eErrs) == 0 {
					if byteLen(id) < 1 || byteLen(id) > 128 || eventIDs[id] {
						addFieldError(&eErrs, field("event_id"), "event_id must be unique and 1-128 bytes")
					}
					eventIDs[id] = true
					switch kind {
					case "room_message", "room_tool_call", "room_tool_result", "pi_internal", "segment_terminal":
					default:
						addFieldError(&eErrs, field("kind"), "unknown evidence event kind")
					}
					if content == "" && kind != "segment_terminal" {
						addFieldError(&eErrs, field("content"), "content may be empty only for segment_terminal")
					}
					if sequence <= previousSequence {
						addFieldError(&eErrs, field("sequence"), "sequence must be positive and strictly increasing")
					}
					previousSequence = sequence
					occurredAtTime, ok := parseTimestamp(occurredAt)
					if !ok {
						addFieldError(&eErrs, field("occurred_at"), "occurred_at must be canonical UTC RFC3339Nano")
					} else {
						values.events = append(values.events, domain.EvidenceEvent{ID: id, Sequence: sequence, Kind: kind, Content: content, OccurredAt: occurredAtTime})
					}
				}
				errs = append(errs, eErrs...)
			}
			if len(events) == 0 && len(errs) == 0 {
				addFieldError(&errs, "events", "events must be a non-empty ordered array")
			}
		}
	}

	linksRaw, ok := object["links"]
	if !ok {
		addFieldError(&errs, "links", "required field is missing")
	} else {
		var links []map[string]json.RawMessage
		if err := json.Unmarshal(linksRaw, &links); err != nil {
			markJSONError(&errs)
		} else {
			eventIDs := make(map[string]bool, len(values.events))
			for _, event := range values.events {
				eventIDs[event.ID] = true
			}
			linkIDs := make(map[string]bool, len(links))
			allowedLinkKeys := map[string]bool{"link_id": true, "from_event_id": true, "to_event_id": true, "relation": true}
			for index, linkObject := range links {
				field := func(name string) string { return "links[" + itoa(index) + "]." + name }
				var lErrs []fieldDetail
				var id, from, to, relation string
				for key := range linkObject {
					if !allowedLinkKeys[key] {
						addFieldError(&lErrs, field(key), "unknown field")
					}
				}
				requireProvenanceString(linkObject, "link_id", &id, &lErrs)
				requireProvenanceString(linkObject, "from_event_id", &from, &lErrs)
				requireProvenanceString(linkObject, "to_event_id", &to, &lErrs)
				requireProvenanceString(linkObject, "relation", &relation, &lErrs)
				if len(lErrs) == 0 {
					if byteLen(id) < 1 || byteLen(id) > 128 || linkIDs[id] {
						addFieldError(&lErrs, field("link_id"), "link_id must be unique and 1-128 bytes")
					}
					linkIDs[id] = true
					switch relation {
					case "mentions", "responds_to", "continues", "delegates_to":
					default:
						addFieldError(&lErrs, field("relation"), "unknown link relation")
					}
					if !eventIDs[from] || !eventIDs[to] {
						addFieldError(&lErrs, field("from_event_id"), "links must reference events in this batch")
					}
					values.links = append(values.links, domain.EvidenceLink{ID: id, FromEventID: from, ToEventID: to, Relation: relation})
				}
				errs = append(errs, lErrs...)
			}
		}
	}

	if len(errs) > 0 {
		return nil, invalidRequestFrom(errs)
	}
	return &values, nil
}

func requireProvenanceString(object map[string]json.RawMessage, key string, dst *string, errs *[]fieldDetail) {
	raw, ok := object[key]
	if !ok {
		addFieldError(errs, key, "required field is missing")
		return
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		markJSONError(errs)
	}
}

func requireInt64Field(object map[string]json.RawMessage, key string, dst *int64, errs *[]fieldDetail) {
	raw, ok := object[key]
	if !ok {
		addFieldError(errs, key, "required field is missing")
		return
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		markJSONError(errs)
	}
}

func recallResultDTO(result domain.RecallResult) map[string]any {
	reasons := result.Degradation.Reasons
	if reasons == nil {
		reasons = []string{}
	}
	return map[string]any{
		"request_id": result.RequestID,
		"items":      recallItemsDTO(result.Items),
		"degradation": map[string]any{
			"state":   result.Degradation.State,
			"reasons": reasons,
		},
	}
}

func recallItemsDTO(items []domain.RecallItem) []map[string]any {
	dtos := make([]map[string]any, 0, len(items))
	for _, item := range items {
		eventIDs := item.Citation.EventIDs
		if eventIDs == nil {
			eventIDs = []string{}
		}
		dto := map[string]any{
			"content":         item.Content,
			"source_space_id": string(item.SourceSpaceID),
			"memory_version":  item.MemoryVersion,
			"citation": map[string]any{
				"citation_id":       item.Citation.ID,
				"evidence_batch_id": string(item.Citation.EvidenceBatchID),
				"event_ids":         eventIDs,
			},
			"score": item.Score,
		}
		if item.Traversal != nil {
			dto["traversal"] = map[string]any{
				"projection_node_id": string(item.Traversal.ProjectionNodeID),
				"projection_version": int64(item.Traversal.ProjectionVersion),
				"projection_digest":  item.Traversal.ProjectionDigest,
				"route_kind":         item.Traversal.RouteKind,
				"edge_id":            string(item.Traversal.EdgeID),
				"edge_kind":          string(item.Traversal.EdgeKind),
				"relation":           item.Traversal.Relation,
				"direction":          item.Traversal.Direction,
			}
		}
		dtos = append(dtos, dto)
	}
	return dtos
}

func writeCreated(w http.ResponseWriter, created bool, body map[string]any) {
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// isCutPath reports whether a request targets a consolidation-cut route,
// which uses a FLAT error envelope ({"code","message"}) — distinct from the
// wrapped wireError used by the Memory Protocol transport. Ingress-level
// errors (auth, media-type, tenant binding) are routed through the transport
// writer so a single cut operation never mixes the two wire shapes.
func isCutPath(path string) bool {
	if strings.HasSuffix(path, "/consolidation-cuts") { // room create
		return true
	}
	if strings.HasSuffix(path, ":cancel") || strings.HasSuffix(path, ":rediagnose") {
		return true
	}
	return strings.Contains(path, "/consolidation-cuts/") // get / sub-action
}

// writeTransportError writes a protocol error using the wire shape required
// by the target route: flat for consolidation-cut routes, wrapped otherwise.
func writeTransportError(w http.ResponseWriter, r *http.Request, status int, code, message string, details []fieldDetail) {
	if isCutPath(r.URL.Path) {
		writeCutError(w, status, code, message)
		return
	}
	writeError(w, r, status, code, message, details)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details []fieldDetail) {
	if details == nil {
		details = []fieldDetail{}
	}
	writeJSON(w, status, wireError{Error: wireErrorBody{
		Code: code, Message: message, RequestID: newRequestID(), Details: details,
	}})
}

func (h *Handler) writeInvalidRequest(w http.ResponseWriter, body []byte, request *invalidRequest) {
	var requestID string
	if body != nil {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(body, &object); err == nil {
			if raw, ok := object["request_id"]; ok {
				_ = json.Unmarshal(raw, &requestID)
			}
		}
	}
	if requestID == "" {
		requestID = newRequestID()
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
		Code: code, Message: "request violates the Memory Protocol request contract",
		RequestID: requestID, Details: details,
	}})
}

func (h *Handler) writeServiceError(w http.ResponseWriter, requestID string, err error) {
	// A request_id already decoded from a valid body is echoed back so the
	// caller can correlate the failure; anything else gets a fresh ID.
	if requestID == "" || byteLen(requestID) > 128 {
		requestID = newRequestID()
	}
	var protocol *domain.ProtocolError
	if errors.As(err, &protocol) {
		details := []fieldDetail{}
		writeJSON(w, protocol.Status, wireError{Error: wireErrorBody{
			Code: protocol.Code(), Message: protocol.Message, RequestID: requestID, Details: details,
		}})
		return
	}
	writeError(w, nil, http.StatusInternalServerError, "INTERNAL", "internal service failure", nil)
}

func newRequestID() string {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return "req-" + hex.EncodeToString(raw[:])
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return []byte("{}"), nil
	}
	defer r.Body.Close()
	body := make([]byte, 0, 4096)
	buffer := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buffer)
		body = append(body, buffer[:n]...)
		if err != nil {
			return body, nil
		}
		if len(body) > 4<<20 {
			return body, nil
		}
	}
}

func validateID(errs *[]fieldDetail, field, value string) bool {
	if byteLen(value) < 1 || byteLen(value) > 128 {
		addFieldError(errs, field, field+" must be 1-128 UTF-8 bytes")
		return false
	}
	return true
}

func byteLen(value string) int { return len([]byte(value)) }

func hasDuplicates(values []string) bool {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func parseTimestamp(raw string) (time.Time, bool) {
	if raw == "" || !strings.HasSuffix(raw, "Z") {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || parsed.Location() != time.UTC {
		return time.Time{}, false
	}
	if parsed.Format(time.RFC3339Nano) != raw {
		return time.Time{}, false
	}
	return parsed, true
}

func isLowercaseHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if !('0' <= r && r <= '9' || 'a' <= r && r <= 'f') {
			return false
		}
	}
	return true
}

func itoa(value int) string { return strconv.Itoa(value) }
