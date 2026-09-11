// Package explore implements the Host-side ExploreSession for Memory
// Explore retrieval (HST-204, Host Spec §2.3–§2.4, §7, §9.4; Contract §5.6,
// §7.15–§7.18, §12).
//
// Layering and division of labor with internal/toolproxy (HST-201):
//
//   - toolproxy owns the same-call proxy: the §7.17 request envelope, the
//     idempotency lease, the §7.18 exact ToolProxyResult and §13.7.1
//     reason precedence (CTR-002 matrix as the upstream payload gate).
//   - explore owns everything session-shaped: the ExploreSession binding
//     to (room, agent, scope profile), the request-side clamping of
//     budgets/freshness against the Host-exact profile, the response-side
//     exact validation (full CTR-003 §12.7.2 carrier semantics layered
//     over the CTR-002 binding rules), and the independent
//     fence/citation metering under CAS.
//
// The Host validates but never reranks, filters or truncates result
// content (Host Spec §7.7): a violating response fails whole, it is never
// partially returned.
package explore

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/toolproxy"
)

// Agent roles (Host Spec §2.3–§2.4).
type Role string

// The two v1 roles.
const (
	// RoleMemoryAgent may only use the current Room's shared Space.
	RoleMemoryAgent Role = "memory"
	// RoleOrdinaryAgent visibility comes from the Host exact scope profile.
	RoleOrdinaryAgent Role = "ordinary"
)

// ProfileRef is a versioned ref (Contract §7.2 VersionedRef view).
type ProfileRef struct {
	ID      string
	Version int64
	Digest  string
}

// BudgetCaps are the Q35 budget ceilings of one Host-exact profile
// (Host Spec §7.4).
type BudgetCaps struct {
	TotalCap            int64
	EvidenceSubcap      int64
	SkillSubcap         int64
	GuidanceTokenBudget int64
	TimeoutMillis       int64
}

// CitationCaps are the independent citation meter ceilings of one profile.
type CitationCaps struct {
	MaxIdentityCitations int64
	MaxEvidenceCitations int64
}

// AuthorizationSet is the derived authorization view of one scope profile
// (Host Spec §2.4): two ordinary profiles must produce different sets.
type AuthorizationSet struct {
	Tools               []string
	Budgets             BudgetCaps
	Citations           CitationCaps
	RoomSharedSpaceOnly bool
}

// ScopeProfile is the Host-exact versioned scope profile binding an agent
// role to tool authorization, budget ceilings, freshness floor and the
// CTR-005 frozen profiles it references.
type ScopeProfile struct {
	ID      string
	Version int64
	Role    Role
	Tools   []string

	Budgets               BudgetCaps
	Citations             CitationCaps
	MinActivationSequence int64

	RenderProfile     ProfileRef
	ResourceProfile   ProfileRef
	PermissionProfile ProfileRef
}

// Authorizations derives the comparable authorization view. Tools are
// sorted canonically; two profiles with different content never derive
// equal sets (the digest of the set also feeds query identity).
func (p *ScopeProfile) Authorizations() AuthorizationSet {
	tools := append([]string(nil), p.Tools...)
	sortStrings(tools)
	return AuthorizationSet{
		Tools:               tools,
		Budgets:             p.Budgets,
		Citations:           p.Citations,
		RoomSharedSpaceOnly: p.Role == RoleMemoryAgent,
	}
}

// ---------------------------------------------------------------------------
// CTR-005 frozen profiles: digest-verified loading
// ---------------------------------------------------------------------------

// FrozenProfiles holds the CTR-005 render/resource/permission profiles
// loaded with digest verification (profile_digest = SHA-256 of the JCS of
// the digest_preimage fields). A drifted profile file fails the load.
type FrozenProfiles struct {
	docs map[string]*contract.Object
}

var frozenProfileFiles = map[string]string{
	"render":     "render-profile.v1.json",
	"resource":   "resource-profile.v1.json",
	"permission": "permission-profile.v1.json",
}

// LoadFrozenProfiles loads and digest-verifies the three CTR-005 profiles
// from dir. Every profile_digest is recomputed over its declared
// digest_preimage fields; any drift fails closed.
func LoadFrozenProfiles(dir string) (*FrozenProfiles, error) {
	frozen := &FrozenProfiles{docs: map[string]*contract.Object{}}
	for kind, filename := range frozenProfileFiles {
		data, err := os.ReadFile(filepath.Join(dir, filename))
		if err != nil {
			return nil, fmt.Errorf("frozen profile %s: %w", kind, err)
		}
		v, err := contract.ParseJSON(data)
		if err != nil {
			return nil, fmt.Errorf("frozen profile %s: %w", kind, err)
		}
		doc, ok := v.(*contract.Object)
		if !ok {
			return nil, fmt.Errorf("frozen profile %s: document is not an object", kind)
		}
		declared, _ := contract.StringOf(doc, "profile_digest")
		preimageFields := stringListOf(doc, "digest_preimage")
		if len(preimageFields) == 0 {
			return nil, fmt.Errorf("frozen profile %s: digest_preimage missing", kind)
		}
		preimage := contract.NewObject()
		for _, field := range preimageFields {
			val, present := doc.Get(field)
			if !present {
				return nil, fmt.Errorf("frozen profile %s: digest_preimage field %s missing", kind, field)
			}
			preimage.Set(field, val)
		}
		recomputed, err := contract.DigestOf(preimage)
		if err != nil {
			return nil, fmt.Errorf("frozen profile %s: preimage not canonicalizable: %w", kind, err)
		}
		if recomputed != declared {
			return nil, fmt.Errorf("frozen profile %s: profile digest mismatch: declared %s recomputed %s (fail closed)", kind, declared, recomputed)
		}
		frozen.docs[kind] = doc
	}
	return frozen, nil
}

// Ref returns the verified versioned ref of one frozen profile.
func (fp *FrozenProfiles) Ref(kind string) ProfileRef {
	doc, ok := fp.docs[kind]
	if !ok {
		return ProfileRef{}
	}
	id, _ := contract.StringOf(doc, "profile_id")
	version := int64(0)
	if v, ok := intFieldOf(doc, "profile_version"); ok {
		version = v
	}
	digest, _ := contract.StringOf(doc, "profile_digest")
	return ProfileRef{ID: id, Version: version, Digest: digest}
}

// verify checks one scope profile's CTR-005 references against the frozen
// profiles: the exact (id, version, digest) triple must match. A stale
// version claim or a foreign digest fails closed (SCOPE_PROFILE_INVALID).
func (fp *FrozenProfiles) verify(p *ScopeProfile) error {
	for kind, ref := range map[string]ProfileRef{
		"render":     p.RenderProfile,
		"resource":   p.ResourceProfile,
		"permission": p.PermissionProfile,
	} {
		frozen := fp.Ref(kind)
		if ref.ID != frozen.ID || ref.Version != frozen.Version || ref.Digest != frozen.Digest {
			return &Failure{
				Code:    "SCOPE_PROFILE_INVALID",
				Message: fmt.Sprintf("scope profile %s references a stale or foreign %s profile (want %s v%d)", p.ID, kind, frozen.ID, frozen.Version),
			}
		}
	}
	return nil
}

func stringListOf(obj *contract.Object, key string) []string {
	arr, ok := arrayOf(obj, key)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(contract.String); ok {
			out = append(out, string(s))
		}
	}
	return out
}

func sortStrings(list []string) {
	for i := 1; i < len(list); i++ {
		for j := i; j > 0 && list[j] < list[j-1]; j-- {
			list[j], list[j-1] = list[j-1], list[j]
		}
	}
}

// ---------------------------------------------------------------------------
// Failure
// ---------------------------------------------------------------------------

// Failure is a closed-code session failure (Host Spec §5.9 conventions:
// the code names behavior, the message is diagnostic only).
type Failure struct {
	Code    string
	Message string
}

func (f *Failure) Error() string {
	return f.Code + ": " + f.Message
}

// ---------------------------------------------------------------------------
// Manager: Room membership, profile verification, session registry and the
// Room-scoped fence registry
// ---------------------------------------------------------------------------

// Manager opens ExploreSessions and owns the cross-session fence registry
// (a fence digest is bound to exactly one (Room, session) owner).
type Manager struct {
	frozen *FrozenProfiles
	policy *toolproxy.Policy

	mu         sync.Mutex
	members    map[string]map[string]struct{} // room -> agents
	sessions   map[string]*Session
	fenceOwner map[string]string // fence key -> session id
}

// NewManager builds a session manager over the verified frozen profiles
// and the digest-verified tool policy.
func NewManager(frozen *FrozenProfiles, policy *toolproxy.Policy) *Manager {
	return &Manager{
		frozen:     frozen,
		policy:     policy,
		members:    map[string]map[string]struct{}{},
		sessions:   map[string]*Session{},
		fenceOwner: map[string]string{},
	}
}

// JoinRoom records Room membership (Host Spec §2.2).
func (m *Manager) JoinRoom(roomID string, agentIDs ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	agents, ok := m.members[roomID]
	if !ok {
		agents = map[string]struct{}{}
		m.members[roomID] = agents
	}
	for _, agent := range agentIDs {
		agents[agent] = struct{}{}
	}
}

// claimFence registers a fence digest for one session: first claim wins;
// any later claim (same session included) is a conflict (Host Spec §7.3:
// fences never cross Rooms, scope profiles or agents' private spaces, and
// one fence is consumed exactly once).
func (m *Manager) claimFence(kind, digest, sessionID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := kind + "\x1f" + digest
	if owner, claimed := m.fenceOwner[key]; claimed {
		return owner, false
	}
	m.fenceOwner[key] = sessionID
	return "", true
}

// OpenSession binds one ExploreSession to (room, agent, profile) after
// membership and profile verification (Host Spec §7.3).
func (m *Manager) OpenSession(sessionID, roomID, agentID string, profile *ScopeProfile) (*Session, error) {
	if profile == nil {
		return nil, &Failure{Code: "SCOPE_PROFILE_INVALID", Message: "scope profile is required"}
	}
	if err := m.frozen.verify(profile); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	agents, known := m.members[roomID]
	if !known {
		return nil, &Failure{Code: "HOST_AGENT_NOT_IN_ROOM", Message: fmt.Sprintf("room %s is unknown", roomID)}
	}
	if _, member := agents[agentID]; !member {
		return nil, &Failure{Code: "HOST_AGENT_NOT_IN_ROOM", Message: fmt.Sprintf("agent %s is not a member of room %s", agentID, roomID)}
	}
	if _, dup := m.sessions[sessionID]; dup {
		return nil, &Failure{Code: "EXPLORE_SESSION_INVALID", Message: fmt.Sprintf("session %s already exists", sessionID)}
	}
	session := &Session{
		id:      sessionID,
		roomID:  roomID,
		agentID: agentID,
		profile: profile,
		manager: m,
		ledger:  newFenceLedger(sessionID, m),
	}
	m.sessions[sessionID] = session
	return session, nil
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

// Session is one ExploreSession: the room/agent/profile binding, the
// prepared-query cache and the fence ledger (Host Spec §7.3).
type Session struct {
	id      string
	roomID  string
	agentID string
	profile *ScopeProfile
	manager *Manager
	ledger  *fenceLedger
}

// ID returns the session identity.
func (s *Session) ID() string { return s.id }

// Room returns the bound Room.
func (s *Session) Room() string { return s.roomID }

// Agent returns the bound agent.
func (s *Session) Agent() string { return s.agentID }

// Role returns the bound agent role.
func (s *Session) Role() Role { return s.profile.Role }

// Profile returns the bound scope profile.
func (s *Session) Profile() *ScopeProfile { return s.profile }

// FenceAccount snapshots the independent fence/citation meters.
func (s *Session) FenceAccount() FenceAccount { return s.ledger.account() }

// Audit snapshots the session audit chain.
func (s *Session) Audit() []AuditEntry { return s.ledger.auditEntries() }

// LastConsumedPayload returns the last served payload exactly as delivered.
func (s *Session) LastConsumedPayload() *contract.Object { return s.ledger.lastConsumedPayload() }

// ---------------------------------------------------------------------------
// Request side: query preparation, clamping and exact replay
// ---------------------------------------------------------------------------

// QueryRequest is the agent-visible query request (before clamping).
type QueryRequest struct {
	ToolName                  string
	QueryText                 string
	RuntimeContextHash        string
	RankerPolicyRef           ProfileRef
	Budgets                   BudgetCaps // requested; clamped by the profile caps
	MinActivationSequence     int64      // requested freshness floor
	ContinuationOfQueryDigest string
}

// ClampNote records one budget/freshness field clamped by the profile cap.
type ClampNote struct {
	Field     string
	Requested int64
	Effective int64
}

// PreparedQuery is the effective (clamped) query plus its canonical digest.
type PreparedQuery struct {
	QueryDigest                    string
	EffectiveBudgets               BudgetCaps
	EffectiveMinActivationSequence int64
	Clamps                         []ClampNote
	// ReplayOf is non-nil when the identical canonical request was already
	// served: the cached exact result is referenced and nothing is
	// re-consumed (Contract §12.7.2 C6, Host Spec §7.3).
	ReplayOf *ConsumedResult
}

// clampQuery is the pure request-side clamping step (HST-204 refactor
// seam): agent-requested values may shrink below the profile ceilings,
// never exceed them, and the freshness floor is raised to the profile
// policy minimum. Every adjustment is recorded as a clamp note. This is
// the ONLY place request values are rewritten; the response side
// (validateResponse/consumeResponse) never re-clamps anything.
func clampQuery(requested BudgetCaps, requestedFloor int64, profile *ScopeProfile) (BudgetCaps, int64, []ClampNote) {
	var notes []ClampNote
	clampDown := func(field string, requested, ceiling int64) int64 {
		if requested > ceiling {
			notes = append(notes, ClampNote{Field: field, Requested: requested, Effective: ceiling})
			return ceiling
		}
		return requested
	}
	effective := BudgetCaps{
		TotalCap:            clampDown("total_cap", requested.TotalCap, profile.Budgets.TotalCap),
		EvidenceSubcap:      clampDown("evidence_subcap", requested.EvidenceSubcap, profile.Budgets.EvidenceSubcap),
		SkillSubcap:         clampDown("skill_subcap", requested.SkillSubcap, profile.Budgets.SkillSubcap),
		GuidanceTokenBudget: clampDown("guidance_token_budget", requested.GuidanceTokenBudget, profile.Budgets.GuidanceTokenBudget),
		TimeoutMillis:       clampDown("timeout_millis", requested.TimeoutMillis, profile.Budgets.TimeoutMillis),
	}
	floor := requestedFloor
	if floor < profile.MinActivationSequence {
		notes = append(notes, ClampNote{
			Field:     "min_activation_sequence",
			Requested: requestedFloor,
			Effective: profile.MinActivationSequence,
		})
		floor = profile.MinActivationSequence
	}
	return effective, floor, notes
}

// PrepareQuery clamps the requested budgets/freshness against the profile
// ceilings and derives the canonical query digest. An identical repeat
// preparation resolves to the cached exact result (ReplayOf) without
// re-consuming any fence.
func (s *Session) PrepareQuery(req QueryRequest) (*PreparedQuery, error) {
	if !s.manager.policy.IsProxyTool(req.ToolName) {
		return nil, &Failure{Code: reasonHostToolNotAllowed, Message: fmt.Sprintf("tool %q is not in the closed tool set v1", req.ToolName)}
	}
	if !s.profileAllows(req.ToolName) {
		return nil, &Failure{Code: reasonHostToolNotAllowed, Message: fmt.Sprintf("scope profile %s does not allow tool %q", s.profile.ID, req.ToolName)}
	}
	if req.QueryText == "" {
		return nil, &Failure{Code: reasonArgumentsInvalid, Message: "query_text must be non-empty"}
	}
	if !sha256Pattern.MatchString(req.RuntimeContextHash) {
		return nil, &Failure{Code: reasonArgumentsInvalid, Message: "runtime_context_hash must be a Digest"}
	}
	if req.RankerPolicyRef.ID == "" || !sha256Pattern.MatchString(req.RankerPolicyRef.Digest) {
		return nil, &Failure{Code: reasonArgumentsInvalid, Message: "ranker_policy_ref must be an exact versioned ref"}
	}

	requested := req.Budgets
	if requested.TotalCap < 0 || requested.EvidenceSubcap < 0 || requested.SkillSubcap < 0 ||
		requested.GuidanceTokenBudget < 0 || requested.TimeoutMillis < 1 {
		return nil, &Failure{Code: reasonArgumentsInvalid, Message: "requested budgets must be non-negative integers (timeout >= 1)"}
	}
	if req.MinActivationSequence < 0 {
		return nil, &Failure{Code: reasonArgumentsInvalid, Message: "requested_min_activation_sequence must be >= 0"}
	}

	prepared := &PreparedQuery{}
	prepared.EffectiveBudgets, prepared.EffectiveMinActivationSequence, prepared.Clamps =
		clampQuery(requested, req.MinActivationSequence, s.profile)

	digest, err := s.queryDigest(req, prepared)
	if err != nil {
		return nil, &Failure{Code: reasonArgumentsInvalid, Message: "canonical query digest failed: " + err.Error()}
	}
	prepared.QueryDigest = digest
	s.ledger.recordPrepared(digest, prepared.EffectiveBudgets)

	// Exact replay: the same canonical request digest returns the cached
	// result reference without re-consuming any fence.
	if terminal, served := s.ledger.terminalFor(digest); served {
		prepared.ReplayOf = terminal
		s.ledger.appendAudit(AuditEntry{
			Kind:         AuditQueryReplay,
			QueryDigest:  digest,
			ToolName:     req.ToolName,
			CASOutcome:   CASReplayedSaved,
			ResultDigest: terminal.ResultDigest,
		})
	}
	return prepared, nil
}

func (s *Session) profileAllows(tool string) bool {
	for _, allowed := range s.profile.Tools {
		if allowed == tool {
			return true
		}
	}
	return false
}

// queryDigest builds the canonical query identity: the effective
// (post-clamp) MemoryExploreArgumentsV1 core (GMS §10.2) — the same
// canonical bytes whose SHA-256 the upstream ExploreResult.query_digest
// MUST echo. filters.active_only materializes to its default true.
func (s *Session) queryDigest(req QueryRequest, prepared *PreparedQuery) (string, error) {
	ranker := contract.NewObject()
	ranker.Set("id", contract.String(req.RankerPolicyRef.ID))
	ranker.Set("version", contract.Number(fmt.Sprintf("%d", req.RankerPolicyRef.Version)))
	ranker.Set("digest", contract.String(req.RankerPolicyRef.Digest))
	budgets := contract.NewObject()
	budgets.Set("total_cap", contract.Number(fmt.Sprintf("%d", prepared.EffectiveBudgets.TotalCap)))
	budgets.Set("evidence_subcap", contract.Number(fmt.Sprintf("%d", prepared.EffectiveBudgets.EvidenceSubcap)))
	budgets.Set("skill_subcap", contract.Number(fmt.Sprintf("%d", prepared.EffectiveBudgets.SkillSubcap)))
	budgets.Set("guidance_token_budget", contract.Number(fmt.Sprintf("%d", prepared.EffectiveBudgets.GuidanceTokenBudget)))
	filters := contract.NewObject()
	filters.Set("active_only", contract.Bool(true))
	obj := contract.NewObject()
	obj.Set("schema_version", contract.String("gms.memory-explore-arguments.v1"))
	obj.Set("explore_session_id", contract.String(s.id))
	obj.Set("query_text", contract.String(req.QueryText))
	obj.Set("runtime_context_hash", contract.String(req.RuntimeContextHash))
	obj.Set("ranker_policy_ref", ranker)
	obj.Set("budgets", budgets)
	obj.Set("filters", filters)
	if req.ContinuationOfQueryDigest != "" {
		obj.Set("continuation_of_query_digest", contract.String(req.ContinuationOfQueryDigest))
	}
	return contract.DigestOf(obj)
}

// ---------------------------------------------------------------------------
// Response side: exact validation, fence consumption, terminal CAS
// ---------------------------------------------------------------------------

// ConsumeRequest carries one upstream success payload plus the request and
// read-audit context needed for the session-layer obligations.
type ConsumeRequest struct {
	QueryDigest string
	ToolName    string
	Request     contract.Value // Contract §7.17 ToolProxyRequest
	ReadAudit   contract.Value // authoritative read audit record
	Payload     contract.Value // upstream success result payload
}

// ConsumeResponse validates one upstream success payload against the
// frozen derivations and the session obligations, then consumes the fences
// and citation meters under CAS and fixes the terminal result. The Host
// never rewrites the payload: the returned digest is over the bytes as
// delivered.
func (s *Session) ConsumeResponse(req ConsumeRequest) *Outcome {
	request, _ := req.Request.(*contract.Object)
	audit, _ := req.ReadAudit.(*contract.Object)
	payload, _ := req.Payload.(*contract.Object)

	// Terminal CAS first: a response arriving after its query reached a
	// terminal result is late. Byte-identical replays return the saved
	// exact result; anything else is audit-only and fails closed
	// (§13.7.1 R3-4 late semantics — never a rewrite, never a re-consume).
	if terminal, fixed := s.ledger.terminalFor(req.QueryDigest); fixed {
		if payload != nil {
			if digest, err := contract.DigestOf(payload); err == nil && digest == terminal.ResultDigest {
				s.ledger.appendAudit(AuditEntry{
					Kind:         AuditLateUpstream,
					QueryDigest:  req.QueryDigest,
					ToolName:     req.ToolName,
					CASOutcome:   CASReplayedSaved,
					ResultDigest: terminal.ResultDigest,
				})
				return &Outcome{Accept: true, ResultDigest: terminal.ResultDigest, Canonical: terminal.Canonical, Replay: true}
			}
		}
		s.ledger.appendAudit(AuditEntry{
			Kind:        AuditLateUpstream,
			QueryDigest: req.QueryDigest,
			ToolName:    req.ToolName,
			ReasonCode:  reasonIdempotency,
			CASOutcome:  CASLateOnly,
			Detail:      map[string]string{"mode": "audit_only"},
		})
		return &Outcome{Accept: false, ReasonCode: reasonIdempotency}
	}

	outcome := s.validateResponse(req, payload, request, audit)
	if !outcome.Accept {
		s.ledger.appendAudit(AuditEntry{
			Kind:        AuditValidationFailure,
			QueryDigest: req.QueryDigest,
			ToolName:    req.ToolName,
			ReasonCode:  outcome.ReasonCode,
		})
		return &outcome
	}

	// Fence/citation consumption (CAS): a conflicting consume leaves the
	// ledger untouched and fails the whole result.
	if reason := s.ledger.consumeResponse(payload, s.profile.Citations); reason != "" {
		outcome = Outcome{Accept: false, ReasonCode: reason}
		s.ledger.appendAudit(AuditEntry{
			Kind:        AuditFenceConsumed,
			QueryDigest: req.QueryDigest,
			ToolName:    req.ToolName,
			ReasonCode:  reason,
			CASOutcome:  CASFenceConflict,
		})
		return &outcome
	}

	terminal := s.ledger.commitTerminal(req.QueryDigest, &ConsumedResult{
		QueryDigest:  req.QueryDigest,
		ResultDigest: outcome.ResultDigest,
		Canonical:    outcome.Canonical,
	})
	s.ledger.appendAudit(AuditEntry{
		Kind:         AuditFenceConsumed,
		QueryDigest:  req.QueryDigest,
		ToolName:     req.ToolName,
		CASOutcome:   CASFirstConsume,
		ResultDigest: terminal.ResultDigest,
	})
	return &outcome
}

// validateResponse runs the full validation pipeline for one payload.
// Request clamping already happened at PrepareQuery; this side is pure
// response validation (the HST-204 refactor seam).
func (s *Session) validateResponse(req ConsumeRequest, payload *contract.Object, request, audit *contract.Object) Outcome {
	// Tool binding: closed tool set, then the session profile.
	if !s.manager.policy.IsProxyTool(req.ToolName) {
		return rejectedOutcome(reasonHostToolNotAllowed)
	}
	if !s.profileAllows(req.ToolName) {
		return rejectedOutcome(reasonHostToolNotAllowed)
	}
	if payload == nil {
		return rejectedOutcome(reasonArtifactBody)
	}

	// Room/scope binding: the request names this session's Room and agent;
	// a Memory Agent may only ever touch the current Room's shared Space,
	// and the read audit must agree with the request (whole-result scope
	// violation; the Host never filters cross-scope items out).
	if request != nil {
		roomID, _ := contract.StringOf(request, "room_id")
		agentID, _ := contract.StringOf(request, "agent_id")
		if roomID != s.roomID || agentID != s.agentID {
			return rejectedOutcome(reasonScopeViolation)
		}
	}
	if reason := scopeRuleReason(request, audit); reason != "" {
		return rejectedOutcome(reason)
	}

	binding, _ := s.manager.policy.Tools[req.ToolName]

	// skill_get: closed GuidanceView rules only (CTR-002 M4). The closed
	// GuidanceView DTO carries no query digest; freshness and authorization
	// come from the request fields plus the read audit above.
	if binding.Ruleset == "guidance-view-closed" {
		return deriveGuidanceViewClosedMatrix(payload, request, audit, binding)
	}

	// Explore family: the response binds to the prepared query identity.
	payloadQueryDigest, _ := contract.StringOf(payload, "query_digest")
	if req.QueryDigest == "" || payloadQueryDigest != req.QueryDigest {
		return rejectedOutcome(reasonQueryInvalid)
	}

	// The CTR-002 binding layer (extensions, closed body, schema binding,
	// field set, required fields) ...
	if reason := commonBindingReason(payload, binding); reason != "" {
		return rejectedOutcome(reason)
	}
	// ... then the full CTR-003 carrier semantics with the LIVE session
	// context (prior served refs, prior watermark — C3/C6 pagination).
	sessionCtx, _ := s.ledger.sessionContext().(*contract.Object)
	if outcome := deriveCarrierOutcome(payload, sessionCtx); !outcome.Accept {
		return outcome
	}
	// Matrix-layer typed citations (Contract §12.6).
	evidenceResults, _ := arrayOf(payload, "evidence_results")
	skillResults, _ := arrayOf(payload, "skill_results")
	if reason := citationRuleReason(evidenceResults, skillResults); reason != "" {
		return rejectedOutcome(reason)
	}
	// Candidate-sourced results are never executable.
	if reason := candidateRefReason(payload); reason != "" {
		return rejectedOutcome(reason)
	}
	// Embedded GuidanceView digests and the frozen profile floors.
	if reason := embeddedGuidanceViewReason(payload, 2, 1); reason != "" {
		return rejectedOutcome(reason)
	}
	// The returned budgets must respect the effective (clamped) ceilings
	// for this query (the Host never trims an over-budget response).
	if reason := budgetsWithinCaps(payload, s.ledger.effectiveBudgetsFor(req.QueryDigest, s.profile.Budgets)); reason != "" {
		return rejectedOutcome(reason)
	}
	// Freshness: the effective (clamped) floor vs the result watermark.
	watermark, _ := objectOf(payload, "watermark")
	if watermark != nil {
		floor := s.profile.MinActivationSequence
		if requested, ok := intFieldOf(request, "requested_min_activation_sequence"); ok && requested > floor {
			floor = requested
		}
		if floor > 0 {
			projectedRaw, _ := watermark.Get("projected_through_activation_sequence")
			projected, isInt := isNonNegativeInt(projectedRaw)
			if !isInt || projected.Cmp(big.NewInt(floor)) < 0 {
				return rejectedOutcome(reasonBehindSequence)
			}
		}
	}
	return acceptedOutcome(payload)
}

// commonBindingReason applies the CTR-002 common binding layer to one
// payload (extensions, closed body, schema binding, closed field set,
// required fields).
func commonBindingReason(payload *contract.Object, binding toolproxy.ToolBinding) string {
	if hasUnknownRequiredExtension(payload) {
		return reasonExtensionUnknown
	}
	schemaVersion, _ := contract.StringOf(payload, "schema_version")
	if schemaVersion != binding.ResultSchemaVersion {
		if schemaVersion == "" {
			return reasonArtifactBody
		}
		return reasonBindingInvalid
	}
	closed := map[string]struct{}{}
	for _, field := range binding.ClosedFields {
		closed[field] = struct{}{}
	}
	for _, key := range payload.Keys() {
		if key == "extensions" {
			continue
		}
		if _, allowed := closed[key]; !allowed {
			return reasonFieldUnknown
		}
	}
	for _, field := range binding.RequiredFields {
		if _, present := payload.Get(field); !present {
			return reasonFieldMissing
		}
	}
	return ""
}

// effectiveBudgetsFor returns the clamped budgets recorded for one prepared
// query (the session profile ceilings for an unregistered digest).
func (f *fenceLedger) effectiveBudgetsFor(queryDigest string, fallback BudgetCaps) BudgetCaps {
	f.mu.Lock()
	defer f.mu.Unlock()
	if caps, ok := f.preparedBudgets[queryDigest]; ok {
		return caps
	}
	return fallback
}
