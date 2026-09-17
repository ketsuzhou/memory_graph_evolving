package explore

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"river2.dev/pi-group-chat-host/internal/contract"
	"river2.dev/pi-group-chat-host/internal/toolproxy"
)

// ---------------------------------------------------------------------------
// Corpus helpers: drive the frozen CTR-002/CTR-003 fixture corpora under
// $FIX/tools/explore through the explore validator derivations and require
// exact accept/reason/digest parity with expected.json (the hard evidence
// that this implementation conforms to the frozen protocol).
// ---------------------------------------------------------------------------

type corpusCase struct {
	caseID         string
	doc            *contract.Object
	expectedAccept bool
	expectedReason string // "" when accepted
	expectedDigest string
}

func mustParseFile(t *testing.T, path string) contract.Value {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	v, err := contract.ParseJSON(data)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return v
}

func mustObject(t *testing.T, v contract.Value, what string) *contract.Object {
	t.Helper()
	obj, ok := v.(*contract.Object)
	if !ok {
		t.Fatalf("%s is not an object", what)
	}
	return obj
}

func exploreCorpusDir(t *testing.T) string {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	return filepath.Join(dir, "tools")
}

// loadManifestCases returns the manifest-declared cases for one array key
// ("cases" filtered to category explore, or "omission_cases").
func loadManifestCases(t *testing.T, arrayKey string) []corpusCase {
	t.Helper()
	toolsDir := exploreCorpusDir(t)
	manifest := mustObject(t, mustParseFile(t, filepath.Join(toolsDir, "manifest.json")), "tools manifest")
	raw, present := manifest.Get(arrayKey)
	if !present {
		t.Fatalf("manifest has no %s array", arrayKey)
	}
	arr, ok := raw.(contract.Array)
	if !ok {
		t.Fatalf("manifest %s is not an array", arrayKey)
	}
	var cases []corpusCase
	for _, item := range arr {
		entry := mustObject(t, item, "manifest entry")
		category, _ := contract.StringOf(entry, "category")
		if arrayKey == "cases" && category != "explore" {
			continue // expand/skill-get corpora belong to their owning slices
		}
		caseID, _ := contract.StringOf(entry, "case_id")
		inputPath, _ := contract.StringOf(entry, "input_path")
		expectedPath, _ := contract.StringOf(entry, "expected_path")
		doc := mustObject(t, mustParseFile(t, filepath.Join(toolsDir, inputPath)), "case input "+caseID)
		expected := mustObject(t, mustParseFile(t, filepath.Join(toolsDir, expectedPath)), "expected "+caseID)
		acceptVal, _ := expected.Get("expected_accept")
		accept, ok := acceptVal.(contract.Bool)
		if !ok {
			t.Fatalf("case %s: expected_accept is not a boolean", caseID)
		}
		cc := corpusCase{caseID: caseID, doc: doc, expectedAccept: bool(accept)}
		if bool(accept) {
			cc.expectedDigest, _ = contract.StringOf(expected, "expected_result_digest")
		} else {
			cc.expectedReason, _ = contract.StringOf(expected, "expected_reason_code")
		}
		cases = append(cases, cc)
	}
	return cases
}

// ---------------------------------------------------------------------------
// Session test helpers
// ---------------------------------------------------------------------------

func testPolicy(t *testing.T) *toolproxy.Policy {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	policy, err := toolproxy.LoadToolPolicy(filepath.Join(dir, "policy", "tool-success-validation.v1.json"))
	if err != nil {
		t.Fatalf("load tool policy: %v", err)
	}
	return policy
}

func testFrozenProfiles(t *testing.T) *FrozenProfiles {
	t.Helper()
	dir, err := contract.ConformanceDir()
	if err != nil {
		t.Fatalf("resolve conformance dir: %v", err)
	}
	frozen, err := LoadFrozenProfiles(filepath.Join(dir, "policy", "profiles"))
	if err != nil {
		t.Fatalf("load frozen CTR-005 profiles: %v", err)
	}
	return frozen
}

func fullScopeProfile(frozen *FrozenProfiles) *ScopeProfile {
	return &ScopeProfile{
		ID:                    "scope-full-ordinary",
		Version:               1,
		Role:                  RoleOrdinaryAgent,
		Tools:                 []string{"memory_explore", "memory_expand", "skill_get"},
		Budgets:               BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		Citations:             CitationCaps{MaxIdentityCitations: 32, MaxEvidenceCitations: 32},
		MinActivationSequence: 10,
		RenderProfile:         frozen.Ref("render"),
		ResourceProfile:       frozen.Ref("resource"),
		PermissionProfile:     frozen.Ref("permission"),
	}
}

func restrictedScopeProfile(frozen *FrozenProfiles) *ScopeProfile {
	return &ScopeProfile{
		ID:                    "scope-restricted-ordinary",
		Version:               1,
		Role:                  RoleOrdinaryAgent,
		Tools:                 []string{"memory_explore"},
		Budgets:               BudgetCaps{TotalCap: 4, EvidenceSubcap: 2, SkillSubcap: 2, GuidanceTokenBudget: 100, TimeoutMillis: 1000},
		Citations:             CitationCaps{MaxIdentityCitations: 2, MaxEvidenceCitations: 2},
		MinActivationSequence: 0,
		RenderProfile:         frozen.Ref("render"),
		ResourceProfile:       frozen.Ref("resource"),
		PermissionProfile:     frozen.Ref("permission"),
	}
}

// fixtureSession opens a session bound to the room/agent carried by a
// fixture input document (room-ctr003-r1 / agent-ctr003-a1).
func fixtureSession(t *testing.T, profile *ScopeProfile) (*Manager, *Session) {
	t.Helper()
	m := NewManager(testFrozenProfiles(t), testPolicy(t))
	m.JoinRoom("room-ctr003-r1", "agent-ctr003-a1")
	session, err := m.OpenSession("exp-session-test-0001", "room-ctr003-r1", "agent-ctr003-a1", profile)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	return m, session
}

func fixturePayload(t *testing.T, rel string) *contract.Object {
	t.Helper()
	dir := exploreCorpusDir(t)
	doc := mustObject(t, mustParseFile(t, filepath.Join(dir, rel)), rel)
	payloadVal, _ := doc.Get("result_payload")
	return mustObject(t, payloadVal, "result_payload")
}

// fixtureCaseDoc loads a whole fixture input document.
func fixtureCaseDoc(t *testing.T, rel string) *contract.Object {
	t.Helper()
	dir := exploreCorpusDir(t)
	return mustObject(t, mustParseFile(t, filepath.Join(dir, rel)), rel)
}

// clone deep-copies a contract value tree.
func clone(v contract.Value) contract.Value {
	switch typed := v.(type) {
	case *contract.Object:
		out := contract.NewObject()
		for _, key := range typed.Keys() {
			val, _ := typed.Get(key)
			out.Set(key, clone(val))
		}
		return out
	case contract.Array:
		out := make(contract.Array, 0, len(typed))
		for _, item := range typed {
			out = append(out, clone(item))
		}
		return out
	default:
		return v
	}
}

func cloneObject(t *testing.T, v contract.Value, what string) *contract.Object {
	t.Helper()
	return mustObject(t, clone(v), what)
}

func setString(t *testing.T, obj *contract.Object, key, value string) {
	t.Helper()
	obj.Set(key, contract.String(value))
}

func num(v int64) contract.Number {
	return contract.Number(int64String(v))
}

func int64String(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [24]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// fixtureRequest builds a §7.17-shaped request bound to the fixture room and
// agent with the requested freshness floor.
func fixtureRequest(minSequence int64) *contract.Object {
	scope := contract.NewObject()
	scope.Set("id", contract.String("scope-memory-room-ctr003"))
	scope.Set("version", num(1))
	scope.Set("digest", contract.String("sha256:e94b96b79521f30b974dcaa27b35f7026011604224b9e7f332e357cc750b9be7"))
	req := contract.NewObject()
	req.Set("schema_version", contract.String("host.tool-proxy-request.v1"))
	req.Set("proxy_request_id", contract.String("pr-test-0001"))
	req.Set("room_id", contract.String("room-ctr003-r1"))
	req.Set("agent_id", contract.String("agent-ctr003-a1"))
	req.Set("delivery_id", contract.String("delivery-test-0001"))
	req.Set("tool_name", contract.String("memory_explore"))
	req.Set("arguments", contract.NewObject())
	req.Set("scope_profile_ref", scope)
	req.Set("idempotency_key", contract.String("sha256:1111111111111111111111111111111111111111111111111111111111111111"))
	req.Set("requested_min_activation_sequence", num(minSequence))
	req.Set("timeout_millis", num(5000))
	return req
}

// consumePage runs one full query/consume cycle over a payload and requires
// acceptance. The payload's query_digest is set to the prepared query
// identity — exactly what a well-behaved upstream echoes for the arguments
// the Host sent (GMS §10.2).
func consumePage(t *testing.T, session *Session, queryText string, payload *contract.Object, minSequence int64) *Outcome {
	t.Helper()
	prepared, err := session.PrepareQuery(QueryRequest{
		ToolName:              "memory_explore",
		QueryText:             queryText,
		RuntimeContextHash:    "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
		RankerPolicyRef:       ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
		Budgets:               BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		MinActivationSequence: minSequence,
	})
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	payload.Set("query_digest", contract.String(prepared.QueryDigest))
	outcome := session.ConsumeResponse(ConsumeRequest{
		QueryDigest: prepared.QueryDigest,
		ToolName:    "memory_explore",
		Request:     fixtureRequest(minSequence),
		Payload:     clone(payload),
	})
	if !outcome.Accept {
		t.Fatalf("expected accepted consume, got reason=%s", outcome.ReasonCode)
	}
	return outcome
}

func fenceCounts(s *Session) FenceAccount {
	return s.FenceAccount()
}

// ---------------------------------------------------------------------------
// The Red test (HST-204 plan): one entry point, subtests per obligation.
// ---------------------------------------------------------------------------

func TestExploreSessionEnforcesScopeBudgetsFencesAndOmissions(t *testing.T) {
	// --- Hard evidence: exact accept/reason parity over the frozen corpora.

	t.Run("OmissionCorpusExactParity24", func(t *testing.T) {
		cases := loadManifestCases(t, "omission_cases")
		if len(cases) != 24 {
			t.Fatalf("expected 24 registered omission cases, got %d", len(cases))
		}
		for _, cc := range cases {
			outcome := DeriveOmissionCaseOutcome(cc.doc)
			if outcome.Accept != cc.expectedAccept {
				t.Errorf("case %s: derived accept=%v want %v (reason=%s)", cc.caseID, outcome.Accept, cc.expectedAccept, outcome.ReasonCode)
				continue
			}
			if cc.expectedAccept {
				if outcome.ResultDigest != cc.expectedDigest {
					t.Errorf("case %s: derived digest %s want %s", cc.caseID, outcome.ResultDigest, cc.expectedDigest)
				}
			} else if outcome.ReasonCode != cc.expectedReason {
				t.Errorf("case %s: derived reason %q want %q", cc.caseID, outcome.ReasonCode, cc.expectedReason)
			}
		}
	})

	t.Run("BasicCorpusExactParity10", func(t *testing.T) {
		policy := testPolicy(t)
		cases := loadManifestCases(t, "cases")
		if len(cases) != 10 {
			t.Fatalf("expected 10 registered explore basic cases, got %d", len(cases))
		}
		for _, cc := range cases {
			outcome := DeriveMatrixCaseOutcome(policy, cc.doc)
			if outcome.Accept != cc.expectedAccept {
				t.Errorf("case %s: derived accept=%v want %v (reason=%s)", cc.caseID, outcome.Accept, cc.expectedAccept, outcome.ReasonCode)
				continue
			}
			if cc.expectedAccept {
				if outcome.ResultDigest != cc.expectedDigest {
					t.Errorf("case %s: derived digest %s want %s", cc.caseID, outcome.ResultDigest, cc.expectedDigest)
				}
			} else if outcome.ReasonCode != cc.expectedReason {
				t.Errorf("case %s: derived reason %q want %q", cc.caseID, outcome.ReasonCode, cc.expectedReason)
			}
		}
	})

	// --- Frozen profile loading is digest-verified (CTR-005).

	t.Run("FrozenProfilesDigestVerified", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		for _, kind := range []string{"render", "resource", "permission"} {
			ref := frozen.Ref(kind)
			if ref.ID == "" || ref.Version < 1 || !digestPattern(ref.Digest) {
				t.Errorf("frozen profile %s did not load with a verified ref: %+v", kind, ref)
			}
		}
		// Tampering any profile body must fail the digest check.
		source := filepath.Join(exploreCorpusDir(t), "..", "policy", "profiles")
		for _, kind := range []string{"render-profile.v1.json", "resource-profile.v1.json", "permission-profile.v1.json"} {
			dir := t.TempDir()
			data, err := os.ReadFile(filepath.Join(source, kind))
			if err != nil {
				t.Fatalf("read profile source: %v", err)
			}
			tampered := []byte(replaceFirst(string(data), `"profile_version": 1`, `"profile_version": 2`))
			if err := os.WriteFile(filepath.Join(dir, kind), tampered, 0o644); err != nil {
				t.Fatalf("write tampered profile: %v", err)
			}
			if _, err := LoadFrozenProfiles(dir); err == nil {
				t.Errorf("tampered %s must fail digest verification", kind)
			}
		}
	})

	t.Run("StaleOrForeignProfileRefRejected", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		m := NewManager(frozen, testPolicy(t))
		m.JoinRoom("room-ctr003-r1", "agent-ctr003-a1")
		// Wrong digest.
		badDigest := fullScopeProfile(frozen)
		badDigest.ID = "scope-bad-digest"
		badDigest.PermissionProfile.Digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
		if _, err := m.OpenSession("s-bad-digest", "room-ctr003-r1", "agent-ctr003-a1", badDigest); err == nil {
			t.Error("profile with a foreign permission-profile digest must be rejected")
		}
		// Stale version claim.
		stale := fullScopeProfile(frozen)
		stale.ID = "scope-stale"
		stale.RenderProfile.Version = frozen.Ref("render").Version + 1
		if _, err := m.OpenSession("s-stale", "room-ctr003-r1", "agent-ctr003-a1", stale); err == nil {
			t.Error("profile claiming a render version newer than the frozen profile must be rejected (stale CAS)")
		}
		// Agent not in room.
		if _, err := m.OpenSession("s-foreign-agent", "room-ctr003-r1", "agent-not-member", fullScopeProfile(frozen)); err == nil {
			t.Error("agent outside the Room membership must not open a session")
		}
	})

	// --- Memory Agent scope: current Room shared Space only (Host §2.3).

	t.Run("MemoryAgentScopedToCurrentRoom", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		m := NewManager(frozen, testPolicy(t))
		m.JoinRoom("room-ctr003-r1", "agent-ctr003-a1")
		m.JoinRoom("room-other-r9", "agent-ctr003-a1")
		memoryProfile := fullScopeProfile(frozen)
		memoryProfile.ID = "scope-memory-agent"
		memoryProfile.Role = RoleMemoryAgent
		session, err := m.OpenSession("exp-session-memory", "room-ctr003-r1", "agent-ctr003-a1", memoryProfile)
		if err != nil {
			t.Fatalf("open memory-agent session: %v", err)
		}
		if session.Role() != RoleMemoryAgent || !session.Profile().Authorizations().RoomSharedSpaceOnly {
			t.Fatal("memory-agent session must be room-shared-space scoped")
		}
		payload := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "memory-agent room-scoped query",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare query: %v", err)
		}
		// Same agent, but the request names another Room: whole-result scope
		// violation, never a filtered partial response.
		crossRoom := fixtureRequest(10)
		setString(t, crossRoom, "room_id", "room-other-r9")
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     crossRoom,
			Payload:     clone(payload),
		})
		if outcome.Accept || outcome.ReasonCode != "EXPLORE_SCOPE_VIOLATION" {
			t.Fatalf("cross-Room request must fail closed with EXPLORE_SCOPE_VIOLATION, got accept=%v reason=%s", outcome.Accept, outcome.ReasonCode)
		}
		// A read audit from another Room is a scope violation too.
		request := fixtureRequest(10)
		outcome = session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     request,
			ReadAudit:   readAuditFor("room-other-r9", "agent-ctr003-a1"),
			Payload:     clone(payload),
		})
		if outcome.Accept || outcome.ReasonCode != "EXPLORE_SCOPE_VIOLATION" {
			t.Fatalf("foreign read audit must fail closed with EXPLORE_SCOPE_VIOLATION, got accept=%v reason=%s", outcome.Accept, outcome.ReasonCode)
		}
	})

	// --- Two ordinary profiles produce different authorization sets.

	t.Run("OrdinaryProfilesDifferInAuthorizations", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		full := fullScopeProfile(frozen)
		restricted := restrictedScopeProfile(frozen)
		if reflect.DeepEqual(full.Authorizations(), restricted.Authorizations()) {
			t.Fatal("two ordinary profiles must produce different authorization sets")
		}
		fullAuth := full.Authorizations()
		restrictedAuth := restricted.Authorizations()
		if len(restrictedAuth.Tools) != 1 || restrictedAuth.Tools[0] != "memory_explore" {
			t.Fatalf("restricted profile tool set: %v", restrictedAuth.Tools)
		}
		if fullAuth.Budgets.TotalCap <= restrictedAuth.Budgets.TotalCap {
			t.Fatalf("full profile total cap %d must exceed restricted %d", fullAuth.Budgets.TotalCap, restrictedAuth.Budgets.TotalCap)
		}
		// The restricted profile denies skill_get; the full profile allows it.
		_, rSession := fixtureSession(t, restricted)
		_, err := rSession.PrepareQuery(QueryRequest{
			ToolName:           "skill_get",
			QueryText:          "skill retrieval",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 1, EvidenceSubcap: 1, SkillSubcap: 1, GuidanceTokenBudget: 100, TimeoutMillis: 1000},
		})
		var failure *Failure
		if !errors.As(err, &failure) || failure.Code != "HOST_TOOL_NOT_ALLOWED" {
			t.Fatalf("restricted profile must deny skill_get with HOST_TOOL_NOT_ALLOWED, got %v", err)
		}
		_, fSession := fixtureSession(t, full)
		if _, err := fSession.PrepareQuery(QueryRequest{
			ToolName:           "skill_get",
			QueryText:          "skill retrieval",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 1, EvidenceSubcap: 1, SkillSubcap: 1, GuidanceTokenBudget: 100, TimeoutMillis: 1000},
		}); err != nil {
			t.Fatalf("full profile must allow skill_get: %v", err)
		}
	})

	// --- Budget clamping: agent-requested values may shrink, never grow.

	t.Run("BudgetsClampedToProfileCaps", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		_, session := fixtureSession(t, fullScopeProfile(frozen))
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "clamp test",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets: BudgetCaps{
				TotalCap:            1000,
				EvidenceSubcap:      999,
				SkillSubcap:         998,
				GuidanceTokenBudget: 99999,
				TimeoutMillis:       600000,
			},
		})
		if err != nil {
			t.Fatalf("prepare query: %v", err)
		}
		want := BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000}
		if prepared.EffectiveBudgets != want {
			t.Fatalf("clamped budgets %+v want %+v", prepared.EffectiveBudgets, want)
		}
		// Five budget ceilings clamped plus the freshness floor raised from
		// the requested 0 to the profile floor 10: every clamp is recorded.
		if len(prepared.Clamps) != 6 {
			t.Fatalf("every clamped field must be recorded, got %d clamp notes: %+v", len(prepared.Clamps), prepared.Clamps)
		}
		clampedFields := map[string]bool{}
		for _, note := range prepared.Clamps {
			if note.Field == "min_activation_sequence" {
				// The freshness floor clamps upward (0 -> profile floor 10).
				if note.Requested >= note.Effective {
					t.Fatalf("freshness clamp must raise the floor: %+v", note)
				}
			} else if note.Requested <= note.Effective {
				// Budget ceilings clamp downward.
				t.Fatalf("clamp note for %s must record requested>effective: %+v", note.Field, note)
			}
			clampedFields[note.Field] = true
		}
		for _, field := range []string{"total_cap", "evidence_subcap", "skill_subcap", "guidance_token_budget", "timeout_millis", "min_activation_sequence"} {
			if !clampedFields[field] {
				t.Fatalf("clamp for %s must be recorded: %+v", field, prepared.Clamps)
			}
		}
		// Smaller-than-cap requests pass through untouched.
		prepared, err = session.PrepareQuery(QueryRequest{
			ToolName:              "memory_explore",
			QueryText:             "clamp test small",
			RuntimeContextHash:    "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:       ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:               BudgetCaps{TotalCap: 3, EvidenceSubcap: 2, SkillSubcap: 1, GuidanceTokenBudget: 500, TimeoutMillis: 800},
			MinActivationSequence: 10,
		})
		if err != nil {
			t.Fatalf("prepare small query: %v", err)
		}
		if prepared.EffectiveBudgets != (BudgetCaps{TotalCap: 3, EvidenceSubcap: 2, SkillSubcap: 1, GuidanceTokenBudget: 500, TimeoutMillis: 800}) {
			t.Fatalf("smaller-than-cap budgets must pass through, got %+v", prepared.EffectiveBudgets)
		}
		if len(prepared.Clamps) != 0 {
			t.Fatalf("no clamp expected for under-cap request, got %+v", prepared.Clamps)
		}
		// Negative requests are malformed, not clamped.
		_, err = session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "negative",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: -1},
		})
		var failure *Failure
		if !errors.As(err, &failure) || failure.Code != "TOOL_ARGUMENTS_INVALID" {
			t.Fatalf("negative budget must fail with TOOL_ARGUMENTS_INVALID, got %v", err)
		}
	})

	// --- Freshness: min sequence behind fails the whole result.

	t.Run("MinSequenceBehindFails", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		_, session := fixtureSession(t, fullScopeProfile(frozen))
		payload := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:              "memory_explore",
			QueryText:             "freshness behind",
			RuntimeContextHash:    "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:       ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:               BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
			MinActivationSequence: 50, // payload watermark projects through 12
		})
		if err != nil {
			t.Fatalf("prepare query: %v", err)
		}
		payload.Set("query_digest", contract.String(prepared.QueryDigest))
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(50),
			Payload:     clone(payload),
		})
		if outcome.Accept || outcome.ReasonCode != "PROJECTION_BEHIND_REQUIRED_SEQUENCE" {
			t.Fatalf("watermark behind the requested sequence must fail with PROJECTION_BEHIND_REQUIRED_SEQUENCE, got accept=%v reason=%s", outcome.Accept, outcome.ReasonCode)
		}
		if got := fenceCounts(session); got.EvidenceFencesConsumed != 0 || got.SkillFencesConsumed != 0 {
			t.Fatalf("a rejected response must not consume fences: %+v", got)
		}
	})

	// --- Same query: exact replay, fences consumed exactly once.

	t.Run("SameQueryExactReplayDoesNotReconsumeFences", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		_, session := fixtureSession(t, fullScopeProfile(frozen))
		payload := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")
		query := QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "replayable query",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		}
		first, err := session.PrepareQuery(query)
		if err != nil {
			t.Fatalf("first prepare: %v", err)
		}
		if first.ReplayOf != nil {
			t.Fatal("first preparation must not be a replay")
		}
		payload.Set("query_digest", contract.String(first.QueryDigest))
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: first.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     clone(payload),
		})
		if !outcome.Accept {
			t.Fatalf("first consume must accept, got %s", outcome.ReasonCode)
		}
		afterFirst := fenceCounts(session)
		if afterFirst.EvidenceFencesConsumed != 1 || afterFirst.SkillFencesConsumed != 1 {
			t.Fatalf("first consume must consume each fence once: %+v", afterFirst)
		}
		if afterFirst.IdentityCitations != 1 {
			t.Fatalf("one skill result carries exactly one artifact identity citation: %+v", afterFirst)
		}
		// Retry of the same query returns the cached exact result reference
		// and consumes nothing.
		retry, err := session.PrepareQuery(query)
		if err != nil {
			t.Fatalf("retry prepare: %v", err)
		}
		if retry.QueryDigest != first.QueryDigest {
			t.Fatalf("same canonical request must yield the same digest: %s vs %s", retry.QueryDigest, first.QueryDigest)
		}
		if retry.ReplayOf == nil || retry.ReplayOf.ResultDigest != outcome.ResultDigest {
			t.Fatalf("retry must reference the cached exact result: %+v vs %s", retry.ReplayOf, outcome.ResultDigest)
		}
		if got := fenceCounts(session); got != afterFirst {
			t.Fatalf("replay must not re-consume fences: before %+v after %+v", afterFirst, got)
		}
		// A byte-identical response replaying after the terminal result also
		// consumes nothing and returns the saved digest.
		replayOutcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: first.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     clone(payload),
		})
		if !replayOutcome.Accept || !replayOutcome.Replay || replayOutcome.ResultDigest != outcome.ResultDigest {
			t.Fatalf("terminal replay must return the saved exact result: %+v", replayOutcome)
		}
		if got := fenceCounts(session); got != afterFirst {
			t.Fatalf("terminal replay must not re-consume fences: %+v", got)
		}
	})

	// --- Late (post-terminal, different-bytes) response: audit-only, no
	// fence consumption, whole-result failure (§13.7.1 R3-4 late semantics).

	t.Run("LateResponseDoesNotConsumeFence", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		_, session := fixtureSession(t, fullScopeProfile(frozen))
		payload := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "late response scenario",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		payload.Set("query_digest", contract.String(prepared.QueryDigest))
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     clone(payload),
		})
		if !outcome.Accept {
			t.Fatalf("first consume must accept: %s", outcome.ReasonCode)
		}
		afterFirst := fenceCounts(session)

		// A different payload for the same query digest arrives after the
		// terminal result was fixed.
		late := cloneObject(t, payload, "late payload")
		ev, _ := late.Get("evidence_results")
		evArr := ev.(contract.Array)
		evFirst := evArr[0].(*contract.Object)
		evFirst.Set("rank_score_micros", num(1))
		lateOutcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     late,
		})
		if lateOutcome.Accept {
			t.Fatal("a post-terminal different-bytes response must fail closed")
		}
		if got := fenceCounts(session); got != afterFirst {
			t.Fatalf("late response must not consume fences: %+v", got)
		}
		audits := session.Audit()
		foundLate := false
		for _, entry := range audits {
			if entry.Kind == AuditLateUpstream {
				foundLate = true
			}
		}
		if !foundLate {
			t.Fatalf("late response must be recorded as %s audit: %+v", AuditLateUpstream, audits)
		}
	})

	// --- Host never reranks or filters served content.

	t.Run("HostDoesNotRerankOrFilterServedResults", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		base := fixturePayload(t, "explore/top-level-omission/pos-omission-total-cap/input.json")

		_, sessionA := fixtureSession(t, fullScopeProfile(frozen))
		outcomeA := consumePage(t, sessionA, "rerank probe base", base, 10)

		// Same payload with served arrays reversed: still accepted (the Host
		// validates, it does not reorder), and the result digest is computed
		// over the bytes as delivered, proving no silent normalization.
		permuted := cloneObject(t, base, "permuted payload")
		for _, field := range []string{"evidence_results", "skill_results"} {
			raw, _ := permuted.Get(field)
			arr := raw.(contract.Array)
			reversed := contract.Array{arr[1], arr[0]}
			permuted.Set(field, reversed)
		}
		_, sessionB := fixtureSession(t, fullScopeProfile(frozen))
		outcomeB := consumePage(t, sessionB, "rerank probe permuted", permuted, 10)
		if outcomeB.ResultDigest == outcomeA.ResultDigest {
			t.Fatal("delivering a different served order must yield a different result digest; equal digests imply the Host reordered content")
		}
		want, err := contract.DigestOf(permuted)
		if err != nil {
			t.Fatalf("digest permuted: %v", err)
		}
		if outcomeB.ResultDigest != want {
			t.Fatalf("result digest must be over the payload as delivered: got %s want %s", outcomeB.ResultDigest, want)
		}
		// The served arrays reach the consumer byte-identical to delivery.
		servedA := canonicalOf(t, sessionA, "evidence_results")
		servedB := canonicalOf(t, sessionB, "evidence_results")
		if servedA == servedB {
			t.Fatal("delivered order must be preserved, not normalized to one canonical order")
		}
		// The frozen omission carrier, by contrast, IS order-validated
		// (misordered carrier entries are rejected, never re-sorted).
		misordered := cloneObject(t, base, "misordered carrier")
		omRaw, _ := misordered.Get("omissions")
		omArr := omRaw.(contract.Array)
		misordered.Set("omissions", contract.Array{omArr[1], omArr[0]})
		_, sessionC := fixtureSession(t, fullScopeProfile(frozen))
		prepared, err := sessionC.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "misordered carrier probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare misordered: %v", err)
		}
		misordered.Set("query_digest", contract.String(prepared.QueryDigest))
		outcomeC := sessionC.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     misordered,
		})
		if outcomeC.Accept || outcomeC.ReasonCode != "TOOL_RESULT_BINDING_INVALID" {
			t.Fatalf("misordered omission carrier must be rejected, not re-sorted: %+v", outcomeC)
		}
	})

	// --- Candidate-sourced results are never executable.

	t.Run("CandidateSourcedResultRejected", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		base := fixturePayload(t, "explore/top-level-omission/pos-omission-total-cap/input.json")
		candidate := cloneObject(t, base, "candidate payload")
		skRaw, _ := candidate.Get("skill_results")
		sk := skRaw.(contract.Array)
		for _, item := range sk {
			entry := item.(*contract.Object)
			// Mark both the served skill_ref and the identity citation ref.
			markCandidate(t, entry, "skill_ref")
			if idRaw, ok := entry.Get("artifact_identity_citation"); ok {
				markCandidate(t, idRaw.(*contract.Object), "skill_ref")
			}
		}
		_, session := fixtureSession(t, fullScopeProfile(frozen))
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "candidate probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		candidate.Set("query_digest", contract.String(prepared.QueryDigest))
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     candidate,
		})
		if outcome.Accept || outcome.ReasonCode != "CANDIDATE_NOT_EXECUTABLE" {
			t.Fatalf("candidate-sourced result must fail whole with CANDIDATE_NOT_EXECUTABLE, got %+v", outcome)
		}
	})

	// --- Fence CAS: an already-consumed fence digest cannot be consumed
	// again (mixed/replayed fences), and fences never cross Rooms.

	t.Run("FenceConsumedOncePerSessionAndRoom", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		m := NewManager(frozen, testPolicy(t))
		m.JoinRoom("room-ctr003-r1", "agent-ctr003-a1")
		m.JoinRoom("room-other-r9", "agent-b9")
		sessionA, err := m.OpenSession("exp-session-fence-a", "room-ctr003-r1", "agent-ctr003-a1", fullScopeProfile(frozen))
		if err != nil {
			t.Fatalf("open session A: %v", err)
		}
		sessionB, err := m.OpenSession("exp-session-fence-b", "room-other-r9", "agent-b9", fullScopeProfile(frozen))
		if err != nil {
			t.Fatalf("open session B: %v", err)
		}
		payload := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")
		consumePage(t, sessionA, "fence page one", payload, 10)

		// Same session, new query, reusing the already-consumed fence
		// digests: CAS conflict.
		reused := cloneObject(t, payload, "reused fence payload")
		wm, _ := reused.Get("watermark")
		wm.(*contract.Object).Set("projected_through_activation_sequence", num(13))
		prepared, err := sessionA.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "fence reuse probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare reuse probe: %v", err)
		}
		reused.Set("query_digest", contract.String(prepared.QueryDigest))
		outcome := sessionA.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     reused,
		})
		if outcome.Accept || outcome.ReasonCode != "EXPLORE_FENCE_CONFLICT" {
			t.Fatalf("re-consuming a consumed fence must fail with EXPLORE_FENCE_CONFLICT, got %+v", outcome)
		}
		if got := fenceCounts(sessionA); got.EvidenceFencesConsumed != 1 || got.SkillFencesConsumed != 1 {
			t.Fatalf("conflicting consume must not change fence accounting: %+v", got)
		}

		// Another Room must not reuse the fence either.
		bRequest := fixtureRequest(10)
		setString(t, bRequest, "room_id", "room-other-r9")
		setString(t, bRequest, "agent_id", "agent-b9")
		bPrepared, err := sessionB.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "cross room fence probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare cross-room probe: %v", err)
		}
		payload.Set("query_digest", contract.String(bPrepared.QueryDigest))
		crossOutcome := sessionB.ConsumeResponse(ConsumeRequest{
			QueryDigest: bPrepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     bRequest,
			Payload:     clone(payload),
		})
		if crossOutcome.Accept || crossOutcome.ReasonCode != "EXPLORE_FENCE_CONFLICT" {
			t.Fatalf("a fence served to another Room must not be consumable there, got %+v", crossOutcome)
		}
	})

	// --- Citation meters are independent and capped.

	t.Run("IndependentCitationMeters", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		_, session := fixtureSession(t, restrictedScopeProfile(frozen)) // caps: 2 identity / 2 evidence citations
		payload := fixturePayload(t, "explore/top-level-omission/pos-omission-total-cap/input.json")
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "citation meter probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 4, EvidenceSubcap: 2, SkillSubcap: 2, GuidanceTokenBudget: 100, TimeoutMillis: 1000},
		})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		payload.Set("query_digest", contract.String(prepared.QueryDigest))
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     clone(payload),
		})
		// payload carries 2 skill results (2 identity citations, exactly at
		// the identity cap) and 4 evidence citations (2 evidence-result
		// citations + 2 skill-listed) which exceeds the evidence-citation
		// cap: the meters are independent, only the overrun fails.
		if outcome.Accept || outcome.ReasonCode != "BUDGET_EXCEEDED" {
			t.Fatalf("evidence-citation meter overrun must fail with BUDGET_EXCEEDED, got %+v", outcome)
		}
		account := fenceCounts(session)
		if account.EvidenceFencesConsumed != 0 || account.SkillFencesConsumed != 0 {
			t.Fatalf("rejected consume must not touch fences: %+v", account)
		}
	})

	// --- Guidance view digest and stale embedded profiles fail closed.

	t.Run("ViewHashAndStaleProfileFailClosed", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		base := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")

		tamperedHash := cloneObject(t, base, "tampered view hash")
		skRaw, _ := tamperedHash.Get("skill_results")
		sk := skRaw.(contract.Array)
		view := sk[0].(*contract.Object)
		gvRaw, _ := view.Get("guidance_view")
		gv := gvRaw.(*contract.Object)
		setString(t, gv, "view_hash", "sha256:0000000000000000000000000000000000000000000000000000000000000000")
		_, session := fixtureSession(t, fullScopeProfile(frozen))
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "view hash probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		tamperedHash.Set("query_digest", contract.String(prepared.QueryDigest))
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     tamperedHash,
		})
		if outcome.Accept || outcome.ReasonCode != "GUIDANCE_VIEW_HASH_MISMATCH" {
			t.Fatalf("tampered view_hash must fail with GUIDANCE_VIEW_HASH_MISMATCH, got %+v", outcome)
		}

		staleRender := cloneObject(t, base, "stale render profile")
		skRaw, _ = staleRender.Get("skill_results")
		sk = skRaw.(contract.Array)
		view = sk[0].(*contract.Object)
		gvRaw, _ = view.Get("guidance_view")
		gv = gvRaw.(*contract.Object)
		rpRaw, _ := gv.Get("render_profile_ref")
		rp := rpRaw.(*contract.Object)
		rp.Set("version", num(1)) // below the policy-frozen floor of 2
		_, session2 := fixtureSession(t, fullScopeProfile(frozen))
		prepared2, err := session2.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "stale render probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare stale: %v", err)
		}
		staleRender.Set("query_digest", contract.String(prepared2.QueryDigest))
		outcome2 := session2.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared2.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     staleRender,
		})
		if outcome2.Accept || outcome2.ReasonCode != "SCHEMA_VERSION_UNSUPPORTED" {
			t.Fatalf("stale embedded render profile must fail with SCHEMA_VERSION_UNSUPPORTED, got %+v", outcome2)
		}
	})

	// --- Response must bind to the prepared query identity.

	t.Run("ResponseBoundToQueryDigest", func(t *testing.T) {
		frozen := testFrozenProfiles(t)
		_, session := fixtureSession(t, fullScopeProfile(frozen))
		payload := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")
		prepared, err := session.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "digest binding probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		payload.Set("query_digest", contract.String(prepared.QueryDigest))
		outcome := session.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     clone(payload),
		})
		if !outcome.Accept {
			t.Fatalf("aligned consume must accept: %s", outcome.ReasonCode)
		}
		// A response whose ExploreResult.query_digest belongs to another
		// query cannot be bound into this one.
		foreign := cloneObject(t, payload, "foreign digest payload")
		setString(t, foreign, "query_digest", "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		_, session2 := fixtureSession(t, fullScopeProfile(frozen))
		prepared2, err := session2.PrepareQuery(QueryRequest{
			ToolName:           "memory_explore",
			QueryText:          "foreign digest probe",
			RuntimeContextHash: "sha256:f3787883f34f39526cc5e5bfa3d10c3f6158e6cf5d61c6fce911e1531bad9d7e",
			RankerPolicyRef:    ProfileRef{ID: "gms.ranker.lexical-graph.v1", Version: 1, Digest: "sha256:2195be3e916f34dc1cfcd9219271b8e789de974045d7c9e6ece643805a4c79bb"},
			Budgets:            BudgetCaps{TotalCap: 10, EvidenceSubcap: 4, SkillSubcap: 4, GuidanceTokenBudget: 2000, TimeoutMillis: 5000},
		})
		if err != nil {
			t.Fatalf("prepare foreign: %v", err)
		}
		outcome2 := session2.ConsumeResponse(ConsumeRequest{
			QueryDigest: prepared2.QueryDigest,
			ToolName:    "memory_explore",
			Request:     fixtureRequest(10),
			Payload:     foreign,
		})
		if outcome2.Accept || outcome2.ReasonCode != "EXPLORE_QUERY_INVALID" {
			t.Fatalf("foreign query digest must fail with EXPLORE_QUERY_INVALID, got %+v", outcome2)
		}
	})
}

// ---------------------------------------------------------------------------
// Small test-only utilities
// ---------------------------------------------------------------------------

func digestPattern(s string) bool {
	if len(s) != 71 {
		return false
	}
	if s[:7] != "sha256:" {
		return false
	}
	for i := 7; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func replaceFirst(s, old, new string) string {
	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}

func readAuditFor(roomID, agentID string) *contract.Object {
	scope := contract.NewObject()
	scope.Set("id", contract.String("scope-memory-room-ctr003"))
	scope.Set("version", num(1))
	scope.Set("digest", contract.String("sha256:e94b96b79521f30b974dcaa27b35f7026011604224b9e7f332e357cc750b9be7"))
	audit := contract.NewObject()
	audit.Set("schema_version", contract.String("host.read-audit.v1"))
	audit.Set("proxy_request_id", contract.String("pr-test-0001"))
	audit.Set("room_id", contract.String(roomID))
	audit.Set("agent_id", contract.String(agentID))
	audit.Set("scope_profile_ref", scope)
	audit.Set("watermark_projection_sequence", num(12))
	audit.Set("active_head_activation_sequence", num(12))
	return audit
}

// markCandidate rewrites a SkillArtifactRef slot into the candidate form.
func markCandidate(t *testing.T, holder *contract.Object, field string) {
	t.Helper()
	raw, present := holder.Get(field)
	if !present {
		t.Fatalf("field %s missing", field)
	}
	ref := raw.(*contract.Object)
	ref.Set("schema_version", contract.String("gms.candidate-artifact-ref.v1"))
}

// canonicalOf returns canonical bytes of one top-level array of the last
// consumed payload of the session.
func canonicalOf(t *testing.T, s *Session, field string) string {
	t.Helper()
	results := s.LastConsumedPayload()
	if results == nil {
		t.Fatal("session has no consumed payload")
	}
	raw, _ := results.Get(field)
	bytes, err := contract.JCS(raw)
	if err != nil {
		t.Fatalf("canonicalize %s: %v", field, err)
	}
	return string(bytes)
}

var _ = sort.Strings

func TestDeriveCarrierOutcomeRejectsTamperedEmbeddedGuidanceView(t *testing.T) {
	payload := fixturePayload(t, "explore/top-level-omission/pos-omission-pagination-page1/input.json")
	skills, _ := arrayOf(payload, "skill_results")
	entry := skills[0].(*contract.Object)
	view, _ := objectOf(entry, "guidance_view")
	setString(t, view, "view_hash", "sha256:0000000000000000000000000000000000000000000000000000000000000000")

	outcome := deriveCarrierOutcome(payload, nil)
	if outcome.Accept || outcome.ReasonCode != reasonViewHashMismatch {
		t.Fatalf("tampered embedded GuidanceView = %+v, want GUIDANCE_VIEW_HASH_MISMATCH", outcome)
	}
}
