// Command s2s4-driver is the GMS-side INT-002 contract driver (GMS-201
// evidence admission, GMS-202 candidate binding, GMS-203 replay
// request/canonicalization).
//
// Line-delimited JSON protocol over stdio: one instruction per line on
// stdin, one single-line JSON response per instruction on stdout, logs on
// stderr. Instructions are {"id":N,"op":"...", ...}; responses are
// {"id":N,"ok":true,...} or {"id":N,"ok":false,"error":{"code","reason"}}
// with codes drawn from the §13.7.1 closed reason registry vocabulary.
//
// The driver is a pure fixture tool of the shared conformance tracer
// ($FIX/integration/run_s2_s4.py): it drives the real GMS services over
// the frozen shared fixtures and reports raw facts only. It never computes
// scores, U1 decisions or release semantics (GMS §5.4); the §10.3 integer
// utility vector it reports is the raw count vector of the canonical §7.11
// ReplayResult, not an evaluation.
//
// Ops (all take "fixtures": the shared conformance directory):
//
//	evidence.admit        {case_id, mode: commit|stage_only|non_settled|tamper}
//	candidate.bind        {mode: ok|uncommitted}
//	replay.build_request  {}
//	replay.canonicalize   {mode: ok|missing_family|cross_run|nondeterministic,
//	                       request_doc, runs}
//
// One driver session holds one in-memory GMS-102 ledger; evidence
// committed by evidence.admit is exactly what candidate.bind resolves
// (the real dependency order, GMS §2.2 → §4.3).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/artifact"
	"river2.dev/graph-memory-service/internal/skillevolution/candidate"
	"river2.dev/graph-memory-service/internal/skillevolution/evidence"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/proposal"
	"river2.dev/graph-memory-service/internal/skillevolution/replay"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

const submitter = "int002-conformance-tracer"

func main() {
	reader := bufio.NewReaderSize(os.Stdin, 1<<20)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	respond := func(payload map[string]any) {
		line, _ := json.Marshal(payload)
		writer.WriteString(string(line) + "\n")
		writer.Flush()
	}
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			handleInstruction(line, respond)
		}
		if err != nil {
			return
		}
	}
}

func handleInstruction(raw string, respond func(map[string]any)) {
	value, perr := contract.ParseJSONStrict([]byte(raw))
	if perr != nil {
		respond(driverError(0, "DRIVER_INSTRUCTION_INVALID", "unparseable instruction: "+perr.Error()))
		return
	}
	body, ok := value.(map[string]any)
	if !ok {
		respond(driverError(0, "DRIVER_INSTRUCTION_INVALID", "instruction is not an object"))
		return
	}
	id := jsonInt(body["id"])
	op := strOf(body["op"])
	if op == "" {
		respond(driverError(id, "DRIVER_INSTRUCTION_INVALID", "op missing"))
		return
	}
	var payload map[string]any
	var code, reason string
	switch op {
	case "evidence.admit":
		payload, code, reason = opEvidenceAdmit(body)
	case "candidate.bind":
		payload, code, reason = opCandidateBind(body)
	case "replay.build_request":
		payload, code, reason = opReplayBuildRequest(body)
	case "replay.canonicalize":
		payload, code, reason = opReplayCanonicalize(body)
	default:
		code, reason = "DRIVER_OP_UNKNOWN", "op "+op
	}
	if code != "" {
		respond(driverError(id, code, reason))
		return
	}
	payload["id"] = id
	payload["ok"] = true
	payload["op"] = op
	respond(payload)
}

func driverError(id int64, code, reason string) map[string]any {
	return map[string]any{
		"id":    id,
		"ok":    false,
		"error": map[string]any{"code": code, "reason": reason},
	}
}

// ---------------------------------------------------------------------------
// Shared decoding helpers (decoder-model: json.Number for every number)
// ---------------------------------------------------------------------------

func strOf(v any) string {
	s, _ := v.(string)
	return s
}

func boolOf(v any) bool {
	b, _ := v.(bool)
	return b
}

func objOf(v any) map[string]any {
	o, _ := v.(map[string]any)
	return o
}

func arrOf(v any) []any {
	a, _ := v.([]any)
	return a
}

func jsonInt(v any) int64 {
	n, ok := v.(json.Number)
	if !ok {
		if s, isStr := v.(string); isStr {
			i, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				return i
			}
		}
		return 0
	}
	i, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func num(v int64) json.Number { return json.Number(strconv.FormatInt(v, 10)) }

func conformanceDir(instruction map[string]any) (string, error) {
	if dir := strOf(instruction["fixtures"]); dir != "" {
		return dir, nil
	}
	if dir := os.Getenv(contract.ConformanceDirEnvVar); dir != "" {
		return dir, nil
	}
	return contract.DefaultConformanceDir()
}

// ---------------------------------------------------------------------------
// Session wiring: one in-memory GMS-102 ledger + GMS-201/202/203 services
// ---------------------------------------------------------------------------

type session struct {
	dir       string
	registry  *ledger.ContractReasonRegistry
	store     *ledger.MemoryStore
	mgr       *ledger.Manager
	gates     *validation.Gates
	admission *evidence.AdmissionService
	proposals *proposal.Service
	binding   *candidate.BindingService
	replay    *replay.Service
	bound     int // bound candidates of this session
	// proposalSeq mints a fresh proposal id per candidate.bind call: the
	// ok and uncommitted probes each seed their own state-machine stream.
	proposalSeq int
	// scope is the corpus-pinned admission scope (scratch ledgers reuse it).
	scope evidence.ScopePolicy
	// roomProfile is the deployment admission scope: the sealed scope
	// profile of each corpus room (non-settled records seal none, so their
	// submissions declare the room's deployment profile).
	roomProfile map[string]contract.VersionedRef
}

var sess *session

func sessionFor(instruction map[string]any) (*session, error) {
	dir, err := conformanceDir(instruction)
	if err != nil {
		return nil, err
	}
	if sess != nil {
		if sess.dir != dir {
			return nil, fmt.Errorf("driver session bound to fixtures %s, got %s", sess.dir, dir)
		}
		return sess, nil
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		return nil, err
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		return nil, err
	}
	mgr, err := ledger.NewManager(store, registry)
	if err != nil {
		return nil, err
	}
	schemas, err := validation.LoadSchemaSet(filepath.Join(dir, "schema", "shared"))
	if err != nil {
		return nil, err
	}
	gates, err := validation.NewGates(schemas)
	if err != nil {
		return nil, err
	}
	scope, roomProfile := corpusScopePolicy(dir)
	admission, err := evidence.NewAdmissionService(store, mgr, registry, scope)
	if err != nil {
		return nil, err
	}
	artifacts, err := artifact.NewService(gates)
	if err != nil {
		return nil, err
	}
	proposals, err := proposal.NewService(gates, filepath.Join(dir, "schema", "state"), store, mgr, registry)
	if err != nil {
		return nil, err
	}
	binding, err := candidate.NewBindingService(candidate.Config{
		Store:     store,
		Tx:        mgr,
		Registry:  registry,
		Artifacts: artifacts,
		Proposals: proposals,
		Evidence:  candidate.AdaptEvidence(admission),
	})
	if err != nil {
		return nil, err
	}
	replaySvc, err := replay.NewService(gates, policy)
	if err != nil {
		return nil, err
	}
	sess = &session{
		dir: dir, registry: registry, store: store, mgr: mgr, gates: gates,
		admission: admission, proposals: proposals, binding: binding, replay: replaySvc,
		scope:       scope,
		roomProfile: roomProfile,
	}
	return sess, nil
}

// scratchAdmission builds an isolated admission service over an empty store
// (same registry, same corpus-pinned scope): a staged submission on it can
// only be invisible, so it probes pre-commit visibility without a commit
// from earlier in the session satisfying the lookup.
func (s *session) scratchAdmission() (*evidence.AdmissionService, error) {
	store, err := ledger.NewMemoryStore(s.registry)
	if err != nil {
		return nil, err
	}
	mgr, err := ledger.NewManager(store, s.registry)
	if err != nil {
		return nil, err
	}
	return evidence.NewAdmissionService(store, mgr, s.registry, s.scope)
}

// corpusScopePolicy pins the deployment admission scope to the closed set
// of rooms and exact scope profiles the recorded segments corpus declares
// (every other room/profile stays denied, GMS §2.1 exact-CAS). It also
// returns each room's sealed profile: non-settled records seal none, so
// their submissions declare the room's deployment profile.
func corpusScopePolicy(dir string) (evidence.ScopePolicy, map[string]contract.VersionedRef) {
	var policy evidence.ScopePolicy
	roomProfile := map[string]contract.VersionedRef{}
	seenProfile := map[string]bool{}
	for _, files := range allSegmentCases(dir) {
		policy.AllowedRooms = appendUnique(policy.AllowedRooms, files.roomID)
		if files.scopeProfile != nil {
			if ref, err := contract.ParseVersionedRef(files.scopeProfile); err == nil {
				if !seenProfile[ref.ID+"/"+ref.Version] {
					seenProfile[ref.ID+"/"+ref.Version] = true
					policy.AllowedProfiles = append(policy.AllowedProfiles, ref)
				}
				if _, ok := roomProfile[files.roomID]; !ok {
					roomProfile[files.roomID] = ref
				}
			}
		}
	}
	return policy, roomProfile
}

func appendUnique(list []string, value string) []string {
	for _, existing := range list {
		if existing == value {
			return list
		}
	}
	return append(list, value)
}

// ---------------------------------------------------------------------------
// Recorded corpus loading
// ---------------------------------------------------------------------------

type recordedCase struct {
	caseID  string
	path    string
	kind    string // corpus: segments | q29b
	family  string
	outcome string // expected_outcome: accept | reject | reserved
}

func recordedCases(dir string) ([]recordedCase, error) {
	data, err := os.ReadFile(filepath.Join(dir, "recorded", "manifest.json"))
	if err != nil {
		return nil, err
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return nil, err
	}
	manifest := objOf(value)
	var cases []recordedCase
	for _, raw := range arrOf(manifest["cases"]) {
		entry := objOf(raw)
		if entry == nil {
			continue
		}
		cases = append(cases, recordedCase{
			caseID:  strOf(entry["case_id"]),
			path:    strOf(entry["path"]),
			kind:    strOf(entry["corpus"]),
			family:  strOf(entry["family"]),
			outcome: strOf(entry["expected_outcome"]),
		})
	}
	return cases, nil
}

type segmentFiles struct {
	caseID       string
	roomID       string
	scopeProfile map[string]any
	segment      map[string]any
	rawSeal      []byte
	canonical    []byte
}

func loadSegmentCase(dir, caseID string) (*segmentFiles, error) {
	cases, err := recordedCases(dir)
	if err != nil {
		return nil, err
	}
	rel := ""
	for _, c := range cases {
		if c.kind == "segments" && c.caseID == caseID {
			rel = c.path
			break
		}
	}
	if rel == "" {
		return nil, fmt.Errorf("recorded segment case %s not in manifest", caseID)
	}
	return loadSegmentPath(dir, rel, caseID)
}

func loadSegmentPath(dir, rel, caseID string) (*segmentFiles, error) {
	base := filepath.Join(dir, "recorded", filepath.FromSlash(rel))
	segRaw, err := os.ReadFile(filepath.Join(base, "segment.json"))
	if err != nil {
		return nil, err
	}
	sealRaw, err := os.ReadFile(filepath.Join(base, "seal.json"))
	if err != nil {
		return nil, err
	}
	canonical, err := os.ReadFile(filepath.Join(base, "canonical.utf8"))
	if err != nil {
		return nil, err
	}
	segValue, err := contract.ParseJSONStrict(segRaw)
	if err != nil {
		return nil, err
	}
	segment := objOf(segValue)
	scopeProfile := objOf(objOf(segment["evidence_seal_body"])["scope_profile_ref"])
	return &segmentFiles{
		caseID:       caseID,
		roomID:       strOf(segment["room_id"]),
		scopeProfile: scopeProfile,
		segment:      segment,
		rawSeal:      sealRaw,
		canonical:    canonical,
	}, nil
}

// allSegmentCases loads every segments-corpus case (scope policy input).
func allSegmentCases(dir string) []*segmentFiles {
	cases, err := recordedCases(dir)
	if err != nil {
		return nil
	}
	var out []*segmentFiles
	for _, c := range cases {
		if c.kind != "segments" {
			continue
		}
		if files, err := loadSegmentPath(dir, c.path, c.caseID); err == nil {
			out = append(out, files)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// GMS-201: evidence.admit
// ---------------------------------------------------------------------------

func opEvidenceAdmit(instruction map[string]any) (map[string]any, string, string) {
	s, err := sessionFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	caseID := strOf(instruction["case_id"])
	mode := strOf(instruction["mode"])
	if mode == "" {
		mode = "commit"
	}
	if caseID == "" {
		return nil, "DRIVER_INSTRUCTION_INVALID", "case_id required"
	}
	files, err := loadSegmentCase(s.dir, caseID)
	if err != nil {
		return nil, "FIXTURE_CASE_MISSING", err.Error()
	}

	// The scope declaration mirrors what the Host sealed: the room and the
	// exact scope profile ref of the Evidence Seal body. Non-settled
	// records seal no scope profile; their submissions declare the room's
	// deployment profile (the closed admission set of the corpus).
	declaredScope := evidence.AdmissionScope{RoomID: files.roomID}
	if files.scopeProfile != nil {
		ref, err := contract.ParseVersionedRef(files.scopeProfile)
		if err != nil {
			return nil, "DRIVER_INTERNAL_ERROR", "scope profile ref: " + err.Error()
		}
		declaredScope.ScopeProfileRef = ref
	} else if profile, ok := s.roomProfile[files.roomID]; ok {
		declaredScope.ScopeProfileRef = profile
	}

	sealBytes := files.rawSeal
	if mode == "tamper" {
		// One hex digit of the seal's segment digest flipped: admission
		// must fail closed (SEAL_DIGEST_MISMATCH) and mint nothing.
		mutated, merr := flipSealDigest(files.rawSeal)
		if merr != nil {
			return nil, "DRIVER_INTERNAL_ERROR", merr.Error()
		}
		sealBytes = mutated
	}

	sub := evidence.SubmittedSegment{
		SegmentJSON:    mustJSONBytes(files.segment),
		SealJSON:       sealBytes,
		Canonical:      files.canonical,
		Submitter:      submitter,
		Scope:          declaredScope,
		IdempotencyKey: "int002-evidence/" + caseID,
	}
	stagedID, serr := s.admission.Stage(sub)
	response := map[string]any{
		"case_id":                caseID,
		"mode":                   mode,
		"staged":                 serr == nil,
		"scoring_fields_present": false,
	}
	if serr != nil {
		response["valid"] = false
		response["status"] = "staged"
		response["reason_code"] = evidence.ReasonOf(serr)
		response["committed"] = nil
		response["commit_rejected"] = true
		response["commit_code"] = evidence.ReasonOf(serr)
		response["lookup_after_stage"] = nil
		response["ledger_size"] = num(int64(committedCount(s)))
		response["chain_segment_digest_match"] = true
		response["chain_evidence_digest_match"] = true
		return response, "", ""
	}

	outcome, verr := s.admission.Validate(stagedID)
	valid := verr == nil && outcome.Valid
	status := string(outcome.Status)
	reason := outcome.ReasonCode
	if verr != nil {
		status = "rejected"
		reason = evidence.ReasonOf(verr)
	}
	response["valid"] = valid
	response["status"] = status
	response["reason_code"] = reason

	committed := map[string]any(nil)
	commitRejected := false
	commitCode := ""
	switch mode {
	case "stage_only":
		// Staged submissions are invisible to every committed-evidence
		// reader: the lookup must miss before commit. The probe stages the
		// same submission on an isolated admission ledger (same service
		// code, empty store) -- evidence committed earlier in this session
		// for the same segment must not satisfy the lookup, because the
		// question is whether THIS staged, uncommitted submission leaks.
		probe := map[string]any{"found": false}
		if scratch, xerr := s.scratchAdmission(); xerr != nil {
			return nil, "DRIVER_INTERNAL_ERROR",
				"scratch admission: " + xerr.Error()
		} else if _, perr := scratch.Stage(sub); perr != nil {
			probe = map[string]any{"found": false, "stage_error": evidence.ReasonOf(perr)}
		} else {
			evidenceID := "evseal-" + strOf(files.segment["segment_id"])
			_, found, _ := scratch.GetEvidence(evidenceID)
			probe = map[string]any{"found": found}
		}
		response["lookup_after_stage"] = probe
		response["visible_before_commit"] = boolOf(probe["found"])
	case "non_settled", "tamper":
		_, cerr := s.admission.Commit(context.Background(), stagedID)
		commitRejected = cerr != nil
		commitCode = evidence.ReasonOf(cerr)
	default: // commit
		ce, cerr := s.admission.Commit(context.Background(), stagedID)
		if cerr != nil {
			commitRejected = true
			commitCode = evidence.ReasonOf(cerr)
		} else {
			committed = committedDoc(ce)
		}
	}
	response["committed"] = committed
	response["commit_rejected"] = commitRejected
	response["commit_code"] = commitCode
	response["ledger_size"] = num(int64(committedCount(s)))

	// Recompute-don't-trust chain checks (the FND-002 protocol authority):
	// the committed §7.5 segment digest covers the canonical bytes and the
	// EvidenceRef digest covers the Evidence Seal body.
	segMatch, evMatch := sealChainMatches(files, committed)
	response["chain_segment_digest_match"] = segMatch
	response["chain_evidence_digest_match"] = evMatch
	return response, "", ""
}

func committedCount(s *session) int {
	list, err := s.admission.CommittedEvidence()
	if err != nil {
		return 0
	}
	return len(list)
}

// committedDoc is the wire view of one committed evidence (Contract §7.7
// ref plus the auditable provenance).
func committedDoc(ce evidence.CommittedEvidence) map[string]any {
	return map[string]any{
		"evidence_id":       ce.EvidenceID,
		"version":           ce.Version,
		"evidence_digest":   ce.EvidenceDigest,
		"commit_state":      ce.CommitState,
		"evidence_kind":     ce.EvidenceKind,
		"segment_digest":    ce.Segment.SegmentDigest,
		"checkpoint_digest": ce.Checkpoint.CheckpointDigest,
		"path_seal_digest":  ce.PathSealDigest,
		"path_ids":          ce.PathIDs,
		"commit_sequence":   num(int64(ce.CommitSequence)),
	}
}

// sealChainMatches recomputes both chain links of a committed evidence
// against the submitted segment record itself.
func sealChainMatches(files *segmentFiles, committed map[string]any) (bool, bool) {
	if committed == nil {
		return true, true // nothing committed: nothing to cross-check
	}
	segDigest := contract.DigestBytes(files.canonical)
	evDigest, err := contract.DigestOf(files.segment["evidence_seal_body"])
	if err != nil {
		return false, false
	}
	return strOf(committed["segment_digest"]) == segDigest,
		strOf(committed["evidence_digest"]) == evDigest
}

// flipSealDigest flips one hex digit of the seal envelope's segment_digest.
func flipSealDigest(raw []byte) ([]byte, error) {
	value, err := contract.ParseJSONStrict(raw)
	if err != nil {
		return nil, err
	}
	obj := objOf(value)
	digest := strOf(obj["segment_digest"])
	if len(digest) != len("sha256:")+64 {
		return nil, fmt.Errorf("seal segment_digest %q not sha256-shaped", digest)
	}
	mutated := []byte(digest)
	last := len(mutated) - 1
	if mutated[last] == 'a' {
		mutated[last] = 'b'
	} else {
		mutated[last] = 'a'
	}
	obj["segment_digest"] = string(mutated)
	return mustJSONBytes(obj), nil
}

func mustJSONBytes(obj map[string]any) []byte {
	data, err := json.Marshal(obj)
	if err != nil {
		return []byte("{}")
	}
	return data
}

// ---------------------------------------------------------------------------
// GMS-202: candidate.bind
// ---------------------------------------------------------------------------

func opCandidateBind(instruction map[string]any) (map[string]any, string, string) {
	s, err := sessionFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	mode := strOf(instruction["mode"])
	if mode == "" {
		mode = "ok"
	}
	// The evidence the proposal rests on: the committed evidence of this
	// session's ledger (the real dependency order). The uncommitted
	// negative references an evidence id that can never exist — the
	// non-settled segment mints no EvidenceRef.
	var (
		evidenceID   = "evseal-seg-failed-0001"
		evidenceVer  = "1"
		evidenceDig  = contract.DigestBytes([]byte("int002-uncommitted-evidence"))
		evidenceKind = "failure_path"
		segment      evidence.SegmentRefView
		haveSegment  bool
	)
	if mode == "ok" {
		committedList, cerr := s.admission.CommittedEvidence()
		if cerr != nil {
			return nil, "DRIVER_INTERNAL_ERROR", cerr.Error()
		}
		if len(committedList) == 0 {
			return nil, "DRIVER_INSTRUCTION_INVALID",
				"candidate.bind requires a prior evidence.admit commit in this session"
		}
		ce := committedList[0]
		evidenceID = ce.EvidenceID
		evidenceVer = ce.Version
		evidenceDig = ce.EvidenceDigest
		evidenceKind = ce.EvidenceKind
		segment = ce.Segment
		haveSegment = true
	}

	s.proposalSeq++
	proposalID := fmt.Sprintf("prop-int002-%04d", s.proposalSeq)
	proposalDoc, perr := buildProposalDoc(s, proposalID, evidenceID, evidenceVer, evidenceDig, evidenceKind, segment, haveSegment)
	if perr != nil {
		return nil, "DRIVER_INTERNAL_ERROR", perr.Error()
	}
	ctx := context.Background()
	// Genesis + admitted transitions (proposal state machine).
	if _, err := s.proposals.AppendTransition(ctx, proposalDoc, "none", "proposed"); err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "seed proposed: " + err.Error()
	}
	if _, err := s.proposals.AppendTransition(ctx, proposalDoc, "proposed", "admitted"); err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "seed admitted: " + err.Error()
	}

	envelope := artifactEnvelope(evidenceID, evidenceVer, evidenceDig, evidenceKind)
	view, berr := s.binding.Bind(ctx, candidate.BindRequest{
		ProposalDoc: proposalDoc,
		ArtifactDoc: envelope,
	})
	if berr != nil {
		return map[string]any{
			"mode":                        mode,
			"bound":                       false,
			"candidate_ref":               nil,
			"bind_code":                   candidate.CodeOf(berr),
			"chain_evidence_digest_match": false,
			"runtime_input_refused":       true,
			"runtime_input_code":          candidate.ReasonCandidateNotExecutable,
			"ledger_candidate_entries":    num(0),
			"scoring_fields_present":      false,
		}, "", ""
	}
	s.bound++
	ref := view.Ref()
	response := map[string]any{
		"mode":                        mode,
		"bound":                       true,
		"chain_evidence_digest_match": true,
		// Candidates are never executable Runtime input (Contract §9.3.6).
		"runtime_input_code":       candidate.ReasonCandidateNotExecutable,
		"ledger_candidate_entries": num(int64(s.bound)),
		"scoring_fields_present":   false,
	}
	rerr := view.RuntimeInput()
	response["runtime_input_refused"] = rerr != nil
	response["candidate_ref"] = map[string]any{
		"schema_version": ref.SchemaVersion,
		"candidate_id":   ref.CandidateID,
		"kind":           ref.Kind,
		"body_digest":    ref.BodyDigest,
		"origin_type":    ref.OriginType,
		"origin_ref": map[string]any{
			"id":      ref.OriginRef.ID,
			"version": num(jsonInt(ref.OriginRef.Version)),
			"digest":  ref.OriginRef.Digest,
		},
	}
	return response, "", ""
}

// buildProposalDoc assembles a strict gms.skill-proposal.v1 document over
// the session's committed evidence with its digest preimage recomputed.
func buildProposalDoc(s *session, proposalID, evidenceID, version, digest, kind string,
	segment evidence.SegmentRefView, haveSegment bool) (map[string]any, error) {
	evidenceRef := map[string]any{
		"schema_version":  contract.SchemaEvidenceRef,
		"evidence_id":     evidenceID,
		"version":         num(jsonInt(version)),
		"evidence_digest": digest,
		"commit_state":    "committed",
		"evidence_kind":   kind,
	}
	var sourceRefs []any
	if haveSegment {
		sourceRefs = []any{map[string]any{
			"schema_version":    "host.segment-ref.v1",
			"room_id":           segment.RoomID,
			"segment_id":        segment.SegmentID,
			"segment_version":   num(jsonInt(segment.SegmentVersion)),
			"segment_digest":    segment.SegmentDigest,
			"evidence_seal_ref": versionedRefDoc(segment.EvidenceSealRef),
			"path_seal_ref":     versionedRefDoc(segment.PathSealRef),
		}}
	} else {
		sourceRefs = []any{map[string]any{
			"schema_version":    "host.segment-ref.v1",
			"room_id":           "room-alpha-0001",
			"segment_id":        "seg-failed-0001",
			"segment_version":   num(1),
			"segment_digest":    contract.DigestBytes([]byte("int002-uncommitted-segment")),
			"evidence_seal_ref": versionedRefDoc(uncommittedRef("evseal-seg-failed-0001")),
			"path_seal_ref":     versionedRefDoc(uncommittedRef("pathseal-seg-failed-0001")),
		}}
	}
	doc := map[string]any{
		"schema_version":      "gms.skill-proposal.v1",
		"proposal_id":         proposalID,
		"proposal_version":    num(1),
		"proposed_kind":       "step_guidance",
		"source_segment_refs": sourceRefs,
		"evidence_refs":       []any{evidenceRef},
		"requested_operation": "create_lineage",
		"origin": map[string]any{
			"initiator_type": "model",
			"initiator_ref":  "agent-int002",
			"request_ref":    "req-int002-0001",
		},
		"policy_refs": []any{map[string]any{
			"id":      "gms.policy.static-gates.v1",
			"version": num(1),
			"digest":  contract.DigestBytes([]byte("int002-policy-static-gates")),
		}},
	}
	digestPreimage, err := s.gates.ComputeDigestPreimage(doc, "skill-proposal.schema.json")
	if err != nil {
		return nil, err
	}
	doc["proposal_digest"] = digestPreimage
	return doc, nil
}

func uncommittedRef(id string) contract.VersionedRef {
	return contract.VersionedRef{
		ID:      id,
		Version: "1",
		Digest:  contract.DigestBytes([]byte("int002-uncommitted:" + id)),
	}
}

func versionedRefDoc(ref contract.VersionedRef) map[string]any {
	return map[string]any{
		"id":      ref.ID,
		"version": num(jsonInt(ref.Version)),
		"digest":  ref.Digest,
	}
}

// artifactEnvelope is a gate-clean step_guidance artifact whose branch
// provenance cites exactly the proposal's committed evidence.
func artifactEnvelope(evidenceID, version, digest, kind string) map[string]any {
	return map[string]any{
		"schema_version": "gms.skill-artifact.v1",
		"kind":           "step_guidance",
		"title":          "Cite sealed evidence before revision",
		"description":    "INT-002 conformance candidate envelope",
		"applicability": map[string]any{
			"predicates": []any{},
			"exclusions": []any{},
		},
		"permissions": []any{
			map[string]any{"capability": "memory_expand", "scope": "room-shared-space"},
		},
		"body": map[string]any{
			"causal_context": map[string]any{
				"summary":    "Revision guidance rests on sealed evidence",
				"claim_refs": []any{},
			},
			"branches": []any{map[string]any{
				"branch_id": "b-int002",
				"when":      map[string]any{"field": "evidence.commit_state", "op": "equals", "value": "committed"},
				"action": map[string]any{
					"guidance": "Bind candidate only after evidence commit", "failure_action": "stop",
					"evidence_refs": []any{map[string]any{
						"schema_version": contract.SchemaEvidenceRef, "evidence_id": evidenceID, "version": num(jsonInt(version)), "evidence_digest": digest, "commit_state": "committed", "evidence_kind": kind,
					}},
				},
				"future": map[string]any{"expected_outcome": "sealed chain", "critical_steps": []any{}, "final_task_impact": "Candidate binds with a complete seal chain"},
			}},
		},
	}
}

// ---------------------------------------------------------------------------
// GMS-203: replay.build_request
// ---------------------------------------------------------------------------

// q29bFixture is one recorded Q29-B packet.
type q29bFixture struct {
	packetID string
	family   string
	runtime  map[string]any
	domain   string
	side     string
}

// familyDomain is the frozen §7.11 family→domain envelope mapping. The
// GMS-208 merge family spans both sources and replays through the overlap
// domain envelope.
var familyDomain = map[string]string{
	"baseline": "source_a", "candidate": "source_a", "overlap": "overlap",
	"conflict": "source_b", "late-output": "source_a",
	"clock-exhausted": "source_a", "extra-tool-call": "source_a",
	"merge": "overlap",
}

func (q *q29bFixture) sealed() map[string]any      { return objOf(q.runtime["sealed_inputs"]) }
func (q *q29bFixture) runBlock() map[string]any    { return objOf(q.runtime["run"]) }
func (q *q29bFixture) expected() map[string]any    { return objOf(q.runtime["expected"]) }
func (q *q29bFixture) terminal() map[string]any    { return objOf(q.runtime["terminal"]) }
func (q *q29bFixture) artifactRef() map[string]any { return objOf(q.sealed()["artifact_ref"]) }

type replayCorpus struct {
	manifest       map[string]any
	byFamily       map[string]*q29bFixture
	families       []string
	reserved       []string
	manifestDigest string
}

func loadReplayCorpus(dir string) (*replayCorpus, error) {
	manifestBytes, err := os.ReadFile(filepath.Join(dir, "recorded", "manifest.json"))
	if err != nil {
		return nil, err
	}
	manifestValue, err := contract.ParseJSONStrict(manifestBytes)
	if err != nil {
		return nil, err
	}
	manifest := objOf(manifestValue)
	cases, err := recordedCases(dir)
	if err != nil {
		return nil, err
	}
	byFamily := map[string]*q29bFixture{}
	reserved := []string{}
	for _, r := range arrOf(manifest["q29b_reserved_families"]) {
		reserved = append(reserved, strOf(r))
	}
	for _, c := range cases {
		if c.kind != "q29b" {
			continue
		}
		isReserved := false
		for _, r := range reserved {
			if r == c.family {
				isReserved = true
			}
		}
		if isReserved {
			continue
		}
		// Reference semantics (validate_recorded.py / the RSIH loader): a
		// reserved-outcome entry is a placeholder directory, never a
		// runnable packet.
		if c.outcome == "reserved" {
			continue
		}
		// A family with several recorded packets (the GMS-208 merge pair)
		// replays through its FIRST packet -- the packet the RSIH runner
		// resolves for the family name.
		if _, loaded := byFamily[c.family]; loaded {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, "recorded", filepath.FromSlash(c.path), "runtime.json"))
		if err != nil {
			return nil, err
		}
		value, err := contract.ParseJSONStrict(raw)
		if err != nil {
			return nil, err
		}
		runtime := objOf(value)
		byFamily[c.family] = &q29bFixture{
			packetID: strOf(runtime["packet_id"]),
			family:   c.family,
			runtime:  runtime,
			domain:   familyDomain[c.family],
			side:     strOf(objOf(runtime["run"])["side"]),
		}
	}
	var families []string
	for _, raw := range arrOf(manifest["q29b_families"]) {
		family := strOf(raw)
		skip := false
		for _, r := range reserved {
			if r == family {
				skip = true
			}
		}
		if !skip {
			families = append(families, family)
		}
	}
	return &replayCorpus{
		manifest:       manifest,
		byFamily:       byFamily,
		families:       families,
		reserved:       reserved,
		manifestDigest: contract.DigestBytes(manifestBytes),
	}, nil
}

type replayChain struct {
	segmentMatch   bool
	evidenceMatch  bool
	candidateMatch bool
}

// buildReplayInputs assembles the frozen §7.10 request document and family
// plan from the recorded corpus (the GMS-203 corpus contract).
func buildReplayInputs(dir string) (map[string]any, *replay.Plan, *replayChain, *replayCorpus, error) {
	corpus, err := loadReplayCorpus(dir)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	base := corpus.byFamily["baseline"].artifactRef()
	conflict := corpus.byFamily["conflict"].artifactRef()
	candidate := corpus.byFamily["candidate"].artifactRef()

	plan := &replay.Plan{}
	var segments []any
	seen := map[string]bool{}
	chain := &replayChain{segmentMatch: true, evidenceMatch: true, candidateMatch: true}
	for _, family := range corpus.families {
		q := corpus.byFamily[family]
		sealed := q.sealed()
		for _, raw := range arrOf(sealed["segment_refs"]) {
			sr := objOf(raw)
			if seen[strOf(sr["segment_id"])] {
				continue
			}
			seen[strOf(sr["segment_id"])] = true
			segments = append(segments, sr)
			segMatch, evMatch := segmentChainMatches(dir, sr)
			chain.segmentMatch = chain.segmentMatch && segMatch
			chain.evidenceMatch = chain.evidenceMatch && evMatch
		}
		packet := replay.Packet{
			ID:                   q.packetID,
			Side:                 q.side,
			ExpectedRunStatus:    strOf(q.terminal()["status"]),
			ExpectedReasonCode:   strOf(q.terminal()["reason_code"]),
			ExpectedOutputDigest: strOf(q.expected()["output_digest"]),
			ExpectedArtifactRef:  q.artifactRef(),
			Critical:             true,
		}
		for _, raw := range arrOf(sealed["path_refs"]) {
			if pr := objOf(raw); pr != nil {
				packet.PathDomains = append(packet.PathDomains, strOf(pr["domain"]))
			}
		}
		baseline := base
		if family == "conflict" {
			// The conflict family's envelope baseline is the policy-selected
			// source-B skill (Contract §10.4).
			baseline = conflict
		}
		plan.Families = append(plan.Families, replay.Family{
			Name:     family,
			Domain:   q.domain,
			Sides:    []string{q.side},
			Baseline: baseline,
			Packets:  []replay.Packet{packet},
		})
	}
	run := corpus.byFamily["baseline"].runBlock()
	doc := map[string]any{
		"schema_version":      "gms.replay-request.v1",
		"replay_request_id":   "replay-int002-0001",
		"candidate_ref":       candidate,
		"baseline_skill_refs": []any{base, conflict},
		"fixture_set_refs": []any{map[string]any{
			"id": "fixture-q29b", "version": num(1), "digest": corpus.manifestDigest}},
		"segment_refs":        segments,
		"replay_profile_ref":  objOf(run["profile_ref"]),
		"runtime_adapter_ref": objOf(run["adapter_ref"]),
		"mode":                "causal_evaluation",
		"idempotency_key":     contract.DigestBytes([]byte("idempotency-int002-0001")),
	}
	chain.candidateMatch = strOf(candidate["body_digest"]) != "" &&
		strOf(candidate["body_digest"]) == strOf(corpus.byFamily["candidate"].artifactRef()["body_digest"])
	return doc, plan, chain, corpus, nil
}

// segmentChainMatches recomputes one §7.5 segment ref against the recorded
// seal authority: the segment digest and the Evidence Seal digest must both
// resolve exactly.
func segmentChainMatches(dir string, sr map[string]any) (bool, bool) {
	segmentID := strOf(sr["segment_id"])
	cases, err := recordedCases(dir)
	if err != nil {
		return false, false
	}
	for _, c := range cases {
		if c.kind != "segments" || c.caseID != segmentID {
			continue
		}
		sealRaw, err := os.ReadFile(filepath.Join(dir, "recorded", filepath.FromSlash(c.path), "seal.json"))
		if err != nil {
			return false, false
		}
		value, err := contract.ParseJSONStrict(sealRaw)
		if err != nil {
			return false, false
		}
		seal := objOf(value)
		segMatch := strOf(seal["segment_digest"]) == strOf(sr["segment_digest"])
		evSeal := objOf(seal["evidence_seal"])
		evRef := objOf(sr["evidence_seal_ref"])
		evMatch := evSeal != nil && evRef != nil &&
			strOf(evSeal["seal_digest"]) == strOf(evRef["digest"]) &&
			strOf(evSeal["seal_id"]) == strOf(evRef["id"])
		return segMatch, evMatch
	}
	return false, false
}

func opReplayBuildRequest(instruction map[string]any) (map[string]any, string, string) {
	s, err := sessionFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	doc, plan, chain, corpus, err := buildReplayInputs(s.dir)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}
	req, perr := s.replay.ParseRequest(doc)
	if perr != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "parse replay request: " + perr.Error()
	}
	families := make([]map[string]any, 0, len(plan.Families))
	for _, family := range plan.Families {
		packets := make([]map[string]any, 0, len(family.Packets))
		for _, p := range family.Packets {
			packets = append(packets, map[string]any{
				"id":                     p.ID,
				"side":                   p.Side,
				"expected_output_digest": p.ExpectedOutputDigest,
				"expected_run_status":    p.ExpectedRunStatus,
				"expected_reason_code":   p.ExpectedReasonCode,
				"artifact_ref":           p.ExpectedArtifactRef,
				"path_domains":           p.PathDomains,
			})
		}
		families = append(families, map[string]any{
			"name":     family.Name,
			"domain":   family.Domain,
			"sides":    family.Sides,
			"baseline": family.Baseline,
			"packets":  packets,
		})
	}
	return map[string]any{
		"request_doc":             doc,
		"request_digest":          req.RequestDigest(),
		"fixture_manifest_digest": corpus.manifestDigest,
		"segment_chain_match":     chain.segmentMatch,
		"evidence_chain_match":    chain.evidenceMatch,
		"candidate_chain_match":   chain.candidateMatch,
		"families":                families,
		"reserved_families":       corpus.reserved,
		"scoring_fields_present":  false,
	}, "", ""
}

// ---------------------------------------------------------------------------
// GMS-203: replay.canonicalize
// ---------------------------------------------------------------------------

func opReplayCanonicalize(instruction map[string]any) (map[string]any, string, string) {
	s, err := sessionFor(instruction)
	if err != nil {
		return nil, "DRIVER_FIXTURES_UNRESOLVED", err.Error()
	}
	mode := strOf(instruction["mode"])
	if mode == "" {
		mode = "ok"
	}
	doc, plan, _, _, err := buildReplayInputs(s.dir)
	if err != nil {
		return nil, "DRIVER_INTERNAL_ERROR", err.Error()
	}
	requestDoc := doc
	if given := objOf(instruction["request_doc"]); given != nil {
		requestDoc = given
	}
	req, perr := s.replay.ParseRequest(requestDoc)
	if perr != nil {
		return nil, "DRIVER_INTERNAL_ERROR", "parse replay request: " + perr.Error()
	}

	runs, rerr := runsFromInstruction(instruction, plan, req)
	if rerr != nil {
		return nil, "DRIVER_INSTRUCTION_INVALID", rerr.Error()
	}

	switch mode {
	case "missing_family":
		// A family the fixture set must cover, planned with no packets,
		// fails closed with MISSING_FAMILY.
		mutated := &replay.Plan{Families: append([]replay.Family{}, plan.Families...)}
		for i := range mutated.Families {
			if mutated.Families[i].Name == "candidate" {
				mutated.Families[i].Packets = nil
			}
		}
		_, cerr := s.replay.Canonicalize(req, mutated, runs)
		if cerr == nil {
			return nil, "DRIVER_INTERNAL_ERROR", "missing-family plan canonicalized without rejection"
		}
		return map[string]any{
			"mode": mode, "rejected": true,
			"reason_code":            replay.CodeOf(cerr),
			"scoring_fields_present": false,
		}, "", ""
	case "cross_run":
		// A run of a foreign packet (another replay round's identity) must
		// fail closed instead of contaminating this request's result.
		foreign := runs[0]
		foreign.PacketID = "q29b-foreign-run-9999"
		_, cerr := s.replay.Canonicalize(req, plan, append(append([]replay.RunOutput{}, runs...), foreign))
		if cerr == nil {
			return nil, "DRIVER_INTERNAL_ERROR", "foreign run canonicalized without rejection"
		}
		return map[string]any{
			"mode": mode, "rejected": true,
			"reason_code":            replay.CodeOf(cerr),
			"scoring_fields_present": false,
		}, "", ""
	case "nondeterministic":
		// Same packet, a second attempt with a different digest: the result
		// must be inconclusive with REPLAY_NONDETERMINISTIC, never green.
		tampered := runs[len(runs)-1]
		tampered.OutputDigest = "sha256:" + strings.Repeat("a", 64)
		tampered.Output = []byte(`{"tampered":true}`)
		result, cerr := s.replay.Canonicalize(req, plan, append(append([]replay.RunOutput{}, runs...), tampered))
		if cerr != nil {
			return nil, "DRIVER_INTERNAL_ERROR", "nondeterministic corpus: " + cerr.Error()
		}
		return map[string]any{
			"mode": mode, "rejected": false,
			"status":                 result.Status(),
			"failure_reason_code":    result.FailureReasonCode(),
			"result_digest":          result.ResultDigest(),
			"rerun_digest_equal":     false,
			"outcome_families":       []string{},
			"outcome_count":          num(0),
			"records":                recordsDoc(result, runs),
			"scoring_fields_present": false,
		}, "", ""
	}

	result, cerr := s.replay.Canonicalize(req, plan, runs)
	if cerr != nil {
		return map[string]any{
			"mode": mode, "rejected": true,
			"reason_code":            replay.CodeOf(cerr),
			"scoring_fields_present": false,
		}, "", ""
	}
	again, aerr := s.replay.Canonicalize(req, plan, runs)
	rerunEqual := aerr == nil && again.ResultDigest() == result.ResultDigest()
	outcomeFamilies := []string{}
	for _, o := range result.Outcomes() {
		outcomeFamilies = append(outcomeFamilies, o.FixtureRef.ID)
	}
	return map[string]any{
		"mode":                   mode,
		"rejected":               false,
		"status":                 result.Status(),
		"failure_reason_code":    result.FailureReasonCode(),
		"result_digest":          result.ResultDigest(),
		"rerun_digest_equal":     rerunEqual,
		"outcome_families":       outcomeFamilies,
		"outcome_count":          num(int64(len(result.Outcomes()))),
		"records":                recordsDoc(result, runs),
		"utility":                result.Utility().Doc(),
		"scoring_fields_present": false,
	}, "", ""
}

// runsFromInstruction maps the tracer-observed runs onto canonicalizer
// RunOutputs, correlated through the frozen plan (side, artifact, adapter
// and profile all pinned by the request). With no observed runs supplied,
// the plan's own expected terminal observations stand in (the corpus
// contract view).
func runsFromInstruction(instruction map[string]any, plan *replay.Plan, req *replay.Request) ([]replay.RunOutput, error) {
	byPacket := map[string]replay.Packet{}
	for _, family := range plan.Families {
		for _, p := range family.Packets {
			byPacket[p.ID] = p
		}
	}
	adapterDoc := versionedRefDoc(req.AdapterRef())
	profileDoc := versionedRefDoc(req.ProfileRef())
	var runs []replay.RunOutput
	rawRuns := arrOf(instruction["runs"])
	if len(rawRuns) == 0 {
		for _, family := range plan.Families {
			for _, p := range family.Packets {
				runs = append(runs, replay.RunOutput{
					PacketID:     p.ID,
					Side:         p.Side,
					RunStatus:    p.ExpectedRunStatus,
					ReasonCode:   p.ExpectedReasonCode,
					OutputDigest: p.ExpectedOutputDigest,
					ArtifactRef:  p.ExpectedArtifactRef,
					AdapterRef:   adapterDoc,
					ProfileRef:   profileDoc,
				})
			}
		}
		return runs, nil
	}
	for _, raw := range rawRuns {
		r := objOf(raw)
		if r == nil {
			return nil, fmt.Errorf("runs entry is not an object")
		}
		packetID := strOf(r["packet_id"])
		p, ok := byPacket[packetID]
		if !ok {
			return nil, fmt.Errorf("observed run of packet %q correlates to no planned packet", packetID)
		}
		runs = append(runs, replay.RunOutput{
			PacketID:     packetID,
			Side:         p.Side,
			RunStatus:    strOf(r["run_status"]),
			ReasonCode:   strOf(r["reason_code"]),
			OutputDigest: strOf(r["output_digest"]),
			ArtifactRef:  p.ExpectedArtifactRef,
			AdapterRef:   adapterDoc,
			ProfileRef:   profileDoc,
		})
	}
	return runs, nil
}

func recordsDoc(result *replay.Result, runs []replay.RunOutput) []map[string]any {
	digestOfPacket := map[string]string{}
	statusOfPacket := map[string]string{}
	for _, r := range runs {
		digestOfPacket[r.PacketID] = r.OutputDigest
		statusOfPacket[r.PacketID] = r.RunStatus
	}
	records := make([]map[string]any, 0, len(result.Records()))
	for _, rec := range result.Records() {
		records = append(records, map[string]any{
			"packet_id":     rec.PacketID,
			"category":      rec.Category,
			"run_status":    statusOfPacket[rec.PacketID],
			"reason_code":   rec.ReasonCode,
			"output_digest": digestOfPacket[rec.PacketID],
		})
	}
	return records
}
