package contract

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Corpus metadata schema versions (Contract §16.2/§16.3).
const (
	ManifestSchemaVersion  = "rsih-skill-evolution.conformance-manifest.v1"
	ContractSchemaVersion  = "rsih-skill-evolution.system-contract.v1"
	ExpectedSchemaVersion  = "rsih-skill-evolution.conformance-expected.v1"
	ConformanceDirEnvVar   = "RSIH_CONFORMANCE_DIR"
	manifestFilename       = "manifest.json"
	defaultSpecRelToParent = "specs/rsi-harness-skill-evolution/conformance"
)

// Categories is the closed category enum of the S1 corpus (Contract §16.2).
var Categories = []string{"canonicalization", "ref", "artifact", "event", "merge", "negative"}

// categoryDirs maps each category to its on-disk directory (Contract §16.1).
var categoryDirs = map[string]string{
	"canonicalization": "canonicalization",
	"ref":              "refs",
	"artifact":         "artifacts",
	"event":            "events",
	"merge":            "merge",
	"negative":         "negative",
}

// Closed S1 reason-code registry: the Contract codes (§6.2, §9.3, §13.7,
// §16.4) plus the FND-001 extension codes adopted by CTR-004 (§13.7.1).
// An expected reason outside this set is a corpus error.
const (
	ReasonUnknownRequiredExtension = "UNKNOWN_REQUIRED_EXTENSION"
	ReasonEvidenceNotCommitted     = "EVIDENCE_NOT_COMMITTED"
	ReasonNonExactRef              = "NON_EXACT_REF"
	ReasonDigestMismatch           = "DIGEST_MISMATCH"
	ReasonRefMismatch              = "REF_MISMATCH"
	ReasonPortSchemaMissing        = "PORT_SCHEMA_MISSING"
	ReasonCompositeCycle           = "COMPOSITE_CYCLE"
	ReasonPermissionCapExceeded    = "PERMISSION_CAP_EXCEEDED"
	ReasonProjectionEventConflict  = "PROJECTION_EVENT_CONFLICT"
	ReasonProjectionSequenceGap    = "PROJECTION_SEQUENCE_GAP"
	ReasonIllegalStateTransition   = "ILLEGAL_STATE_TRANSITION"
	ReasonSimilarityBelowThreshold = "SIMILARITY_BELOW_THRESHOLD"
	ReasonMergeBlockingConflict    = "MERGE_BLOCKING_CONFLICT"
)

var s1ReasonCodes = map[string]bool{
	ReasonNonIntegerNumber:         true,
	ReasonUnknownRequiredExtension: true,
	ReasonDigestMismatch:           true,
	ReasonRefMismatch:              true,
	ReasonNonExactRef:              true,
	ReasonCompositeCycle:           true,
	ReasonPortSchemaMissing:        true,
	ReasonMergeBlockingConflict:    true,
	ReasonEvidenceNotCommitted:     true,
	ReasonProjectionSequenceGap:    true,
	ReasonPermissionCapExceeded:    true,
	ReasonProjectionEventConflict:  true,
	ReasonBOMNotAllowed:            true,
	ReasonInvalidJSON:              true,
	ReasonIllegalStateTransition:   true,
	ReasonSimilarityBelowThreshold: true,
}

// v1 conformance policy constants (versioned with the reference validator).
var (
	// No required extension is registered in v1; the mechanism exists so that
	// required:true + unknown key always fails closed (Contract §1.3.3/§6.4).
	knownExtensionsV1 = map[string]bool{}

	// v1 Host tool surface (Contract §7.17) doubles as the Host authority
	// capability cap for permission checking (Contract §14.2).
	hostCapabilitiesV1 = map[string]bool{
		"memory_explore": true,
		"memory_expand":  true,
		"skill_get":      true,
	}

	evidenceCommitStates = map[string]bool{"committed": true, "sealed": true}
)

// legalProposalTransitions is the Contract §9.4 MergeProposal state machine;
// terminal states have no exits.
var legalProposalTransitions = func() map[string]bool {
	pairs := [][2]string{
		{"none", "proposed"},
		{"proposed", "admitted"}, {"proposed", "duplicate"}, {"proposed", "rejected"},
		{"proposed", "stale"}, {"proposed", "withdrawn"},
		{"admitted", "synthesizing"}, {"admitted", "rejected"}, {"admitted", "stale"},
		{"admitted", "withdrawn"},
		{"synthesizing", "candidate_bound"}, {"synthesizing", "inconclusive"},
		{"synthesizing", "rejected"}, {"synthesizing", "stale"},
		{"candidate_bound", "validating"}, {"candidate_bound", "stale"},
		{"validating", "replaying"}, {"validating", "rejected"}, {"validating", "stale"},
		{"replaying", "decision_pending"}, {"replaying", "rejected"},
		{"replaying", "inconclusive"}, {"replaying", "stale"},
		{"decision_pending", "activation_pending"}, {"decision_pending", "rejected"},
		{"decision_pending", "inconclusive"},
		{"activation_pending", "released"}, {"activation_pending", "rejected"},
		{"activation_pending", "stale"},
	}
	out := make(map[string]bool, len(pairs))
	for _, p := range pairs {
		out[p[0]+"\x1f"+p[1]] = true
	}
	return out
}()

// ---------------------------------------------------------------------------
// Derived evaluation (ported from the FND-001 reference validator)
// ---------------------------------------------------------------------------

// Outcome is the derived evaluation result, computed independently of
// expected.json.
type Outcome struct {
	Accept     bool
	ReasonCode string // "" when accepted
	Canonical  []byte // nil when the case is not canonicalizable
}

// DeriveOutcome derives (accept, reason, canonical bytes) from the case input
// alone, in the pinned fail-closed order (Contract §16.3):
//
//  1. source bytes start with a UTF-8 BOM -> BOM_NOT_ALLOWED;
//  2. source is not valid UTF-8/JSON     -> INVALID_JSON;
//  3. canonicalization hits a non-integer-> NON_INTEGER_NUMBER;
//  4. semantic checks (per category)     -> the matching closed reason code;
//  5. canonical.utf8 file bytes differ from the canonicalization of the
//     source                              -> DIGEST_MISMATCH;
//  6. otherwise accepted.
func DeriveOutcome(sourceBytes []byte, category string, canonicalFileBytes []byte) Outcome {
	if bytes.HasPrefix(sourceBytes, bomUTF8) {
		return Outcome{Accept: false, ReasonCode: ReasonBOMNotAllowed}
	}
	parsed, err := ParseJSONStrict(sourceBytes)
	if err != nil {
		return Outcome{Accept: false, ReasonCode: ReasonInvalidJSON}
	}
	canonical, err := JCS(NormalizeForHashing(parsed))
	if err != nil {
		var ce *CanonicalizationError
		if errors.As(err, &ce) {
			return Outcome{Accept: false, ReasonCode: ce.ReasonCode}
		}
		return Outcome{Accept: false, ReasonCode: ReasonInvalidJSON}
	}
	if issue := semanticIssue(parsed, category); issue != "" {
		return Outcome{Accept: false, ReasonCode: issue, Canonical: canonical}
	}
	if !bytes.Equal(canonicalFileBytes, canonical) {
		return Outcome{Accept: false, ReasonCode: ReasonDigestMismatch, Canonical: canonical}
	}
	return Outcome{Accept: true, Canonical: canonical}
}

type checkFunc func(value any) string

var genericChecks = []checkFunc{extensionsIssue, commitStateIssue}

var categoryChecks = map[string][]checkFunc{
	"canonicalization": {},
	"ref":              {refShapeIssue, candidateReleasedEqualityIssue},
	"artifact":         {kindDriftIssue, compositePortsIssue, compositeCycleIssue, permissionCapIssue},
	"event":            {projectionConflictIssue, projectionGapIssue, proposalTransitionIssue},
	"merge":            {belowThresholdIssue, blockingConflictIssue},
	"negative":         {},
}

func semanticIssue(value any, category string) string {
	checks := make([]checkFunc, 0, len(genericChecks)+3)
	checks = append(checks, genericChecks...)
	checks = append(checks, categoryChecks[category]...)
	for _, check := range checks {
		if code := check(value); code != "" {
			return code
		}
	}
	return ""
}

// extensionsIssue: an extension entry with required:true whose key is not in
// KNOWN_EXTENSIONS_V1 (Contract §6.4/§1.3.3). Unknown optional extensions are
// ignored (they MAY be) and the case stays accepted.
func extensionsIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		extensions, ok := AsObject(holder["extensions"])
		if !ok {
			return
		}
		for key, entry := range extensions {
			obj, isObj := AsObject(entry)
			required, isBool := obj["required"].(bool)
			if isObj && isBool && required && !knownExtensionsV1[key] {
				found = ReasonUnknownRequiredExtension
				return
			}
		}
	})
	return found
}

// commitStateIssue: any object carrying commit_state outside
// {committed, sealed} (Contract §7.7/§13.7).
func commitStateIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		if raw, present := holder["commit_state"]; present {
			s, isStr := AsString(raw)
			if !isStr || !evidenceCommitStates[s] {
				found = ReasonEvidenceNotCommitted
			}
		}
	})
	return found
}

// refShapeIssue: exact-ref shape enforcement (Contract §6.2, §5.1.5).
// Graph node forms, `latest`/non-integer versions and naked ids are not
// exact refs. CandidateArtifactRef is exact via (candidate_id, body_digest)
// and intentionally carries no version (§7.4).
func refShapeIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		if _, ok := holder["graph_node_id"]; ok {
			found = ReasonNonExactRef
			return
		}
		if _, ok := holder["node_id"]; ok {
			found = ReasonNonExactRef
			return
		}
		if _, ok := holder["candidate_id"]; ok {
			if _, hasBody := holder["body_digest"]; !hasBody {
				found = ReasonNonExactRef // naked candidate id
			}
			return
		}
		if _, ok := holder["lineage_id"]; ok {
			if !IsIntegerNumber(holder["version"]) {
				found = ReasonNonExactRef // missing version or `latest`
				return
			}
			if _, hasDigest := holder["artifact_digest"]; !hasDigest {
				found = ReasonNonExactRef // naked lineage name
			}
			return
		}
		if _, ok := holder["evidence_id"]; ok {
			if !IsIntegerNumber(holder["version"]) {
				found = ReasonNonExactRef
				return
			}
			if _, hasDigest := holder["evidence_digest"]; !hasDigest {
				found = ReasonNonExactRef
			}
			return
		}
		if _, ok := holder["id"]; ok {
			if !IsIntegerNumber(holder["version"]) {
				found = ReasonNonExactRef // naked generic id / latest version
				return
			}
			if _, hasDigest := holder["digest"]; !hasDigest {
				found = ReasonNonExactRef
			}
		}
	})
	return found
}

// candidateReleasedEqualityIssue: top-level candidate_ref/released_ref body
// equality (§16.4 #7): candidate body_digest must equal released
// artifact_digest.
func candidateReleasedEqualityIssue(value any) string {
	obj, ok := AsObject(value)
	if !ok {
		return ""
	}
	candidate, cOk := AsObject(obj["candidate_ref"])
	released, rOk := AsObject(obj["released_ref"])
	if cOk && rOk {
		if !EqualJSON(candidate["body_digest"], released["artifact_digest"]) {
			return ReasonDigestMismatch
		}
	}
	return ""
}

// kindDriftIssue: two SkillArtifactRefs sharing (lineage_id, version,
// artifact_digest) with different kind (Contract §6.2).
func kindDriftIssue(value any) string {
	groups := map[string]map[string]bool{}
	IterDicts(value, func(holder map[string]any) {
		if sv, _ := AsString(holder["schema_version"]); sv != SchemaSkillArtifactRef {
			return
		}
		key := CanonicalKey([]any{CanonicalKey(holder["lineage_id"]), CanonicalKey(holder["version"]), CanonicalKey(holder["artifact_digest"])})
		if groups[key] == nil {
			groups[key] = map[string]bool{}
		}
		groups[key][CanonicalKey(holder["kind"])] = true
	})
	for _, kinds := range groups {
		if len(kinds) > 1 {
			return ReasonRefMismatch
		}
	}
	return ""
}

// compositePortsIssue: a composite child missing input_port or output_port
// (Contract §5.2.5/§8.4).
func compositePortsIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		children, hasChildren := AsArray(holder["children"])
		_, hasEdges := AsArray(holder["edges"])
		if !hasChildren || !hasEdges {
			return
		}
		for _, childRaw := range children {
			child, isObj := AsObject(childRaw)
			if !isObj {
				continue
			}
			_, hasInput := child["input_port"]
			_, hasOutput := child["output_port"]
			if !hasInput || !hasOutput {
				found = ReasonPortSchemaMissing
				return
			}
		}
	})
	return found
}

// compositeCycleIssue: a cycle over composite child edges
// (Contract §5.2.3/§8.4).
func compositeCycleIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		children, hasChildren := AsArray(holder["children"])
		edges, hasEdges := AsArray(holder["edges"])
		if !hasChildren || !hasEdges {
			return
		}
		childIDs := map[string]bool{}
		for _, childRaw := range children {
			if child, isObj := AsObject(childRaw); isObj {
				childIDs[CanonicalKey(child["child_id"])] = true
			}
		}
		adjacency := map[string][]string{}
		for _, edgeRaw := range edges {
			edge, isObj := AsObject(edgeRaw)
			if !isObj {
				continue
			}
			from := CanonicalKey(edge["from_child_id"])
			adjacency[from] = append(adjacency[from], CanonicalKey(edge["to_child_id"]))
		}
		if hasCycle(childIDs, adjacency) {
			found = ReasonCompositeCycle
		}
	})
	return found
}

// hasCycle is a white/gray/black DFS over the child-id graph; cycle existence
// is independent of the start order, which is kept sorted for determinism.
func hasCycle(nodes map[string]bool, adjacency map[string][]string) bool {
	const ( // 0 white, 1 gray, 2 black
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(nodes))
	starts := make([]string, 0, len(nodes))
	for n := range nodes {
		starts = append(starts, n)
	}
	sort.Strings(starts)
	var visit func(string) bool
	visit = func(node string) bool {
		color[node] = gray
		for _, next := range adjacency[node] {
			if !nodes[next] {
				continue
			}
			if color[next] == gray {
				return true
			}
			if color[next] == white && visit(next) {
				return true
			}
		}
		color[node] = black
		return false
	}
	for _, n := range starts {
		if color[n] == white && visit(n) {
			return true
		}
	}
	return false
}

// permissionCapIssue: any permission/orchestration_permissions capability
// outside the v1 Host tool surface (Contract §7.17/§14.2).
func permissionCapIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		for _, key := range []string{"permissions", "orchestration_permissions"} {
			entries, ok := AsArray(holder[key])
			if !ok {
				continue
			}
			for _, entryRaw := range entries {
				entry, isObj := AsObject(entryRaw)
				if !isObj {
					continue
				}
				capability, isStr := AsString(entry["capability"])
				if !isStr || !hostCapabilitiesV1[capability] {
					found = ReasonPermissionCapExceeded
					return
				}
			}
		}
	})
	return found
}

// projectionBatches yields the event batches of a projection input: either
// the document is a JSON array of events, or an object carrying "events".
func projectionBatches(value any) [][]any {
	var batches [][]any
	if arr, ok := AsArray(value); ok {
		batches = append(batches, arr)
	} else if obj, ok := AsObject(value); ok {
		if events, ok := AsArray(obj["events"]); ok {
			batches = append(batches, events)
		}
	}
	return batches
}

// projectionEvents filters a batch to dict events with an integer
// activation_sequence; a batch with any non-qualifying entry is skipped.
func projectionEvents(batch []any) []map[string]any {
	events := make([]map[string]any, 0, len(batch))
	for _, raw := range batch {
		event, isObj := AsObject(raw)
		if !isObj || !IsIntegerNumber(event["activation_sequence"]) {
			return nil
		}
		events = append(events, event)
	}
	return events
}

// projectionConflictIssue: same activation_sequence with different
// event_digest (Contract §9.3). Duplicated identical events are idempotent
// and accepted (§13.2).
func projectionConflictIssue(value any) string {
	for _, batch := range projectionBatches(value) {
		events := projectionEvents(batch)
		if len(events) < 2 || len(events) != len(batch) {
			continue
		}
		bySequence := map[string]map[string]bool{}
		for _, event := range events {
			seq := CanonicalKey(event["activation_sequence"])
			if bySequence[seq] == nil {
				bySequence[seq] = map[string]bool{}
			}
			bySequence[seq][CanonicalKey(event["event_digest"])] = true
		}
		for _, digests := range bySequence {
			if len(digests) > 1 {
				return ReasonProjectionEventConflict
			}
		}
	}
	return ""
}

// projectionGapIssue: non-contiguous unique activation sequences
// (Contract §9.3: consume contiguously, stop on gap).
func projectionGapIssue(value any) string {
	for _, batch := range projectionBatches(value) {
		events := projectionEvents(batch)
		if len(events) < 2 || len(events) != len(batch) {
			continue
		}
		unique := map[string]*bigInt{}
		for _, event := range events {
			seq := CanonicalKey(event["activation_sequence"])
			if _, seen := unique[seq]; !seen {
				n, _ := parseBigInt(event["activation_sequence"])
				unique[seq] = n
			}
		}
		sequences := make([]*bigInt, 0, len(unique))
		for _, n := range unique {
			sequences = append(sequences, n)
		}
		sort.Slice(sequences, func(i, j int) bool { return sequences[i].Cmp(sequences[j]) < 0 })
		one := big.NewInt(1)
		for i := 1; i < len(sequences); i++ {
			diff := new(big.Int).Sub(sequences[i], sequences[i-1])
			if diff.Cmp(one) != 0 {
				return ReasonProjectionSequenceGap
			}
		}
	}
	return ""
}

// proposalTransitionIssue: MergeProposalEvent transition outside the §9.4
// state machine (terminal states never reopen).
func proposalTransitionIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		if sv, _ := AsString(holder["schema_version"]); sv != SchemaMergeProposalEvent {
			return
		}
		from, _ := AsString(holder["from_state"])
		to, _ := AsString(holder["to_state"])
		if !legalProposalTransitions[from+"\x1f"+to] {
			found = ReasonIllegalStateTransition
		}
	})
	return found
}

// belowThresholdIssue: SimilarityAssessment with band below_suggestion must
// not auto-admit (Contract §15.2 MT1 negative path).
func belowThresholdIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		if sv, _ := AsString(holder["schema_version"]); sv != SchemaSimilarityAssessment {
			return
		}
		if band, _ := AsString(holder["band"]); band == "below_suggestion" {
			found = ReasonSimilarityBelowThreshold
		}
	})
	return found
}

// blockingConflictIssue: a blocking conflict with unresolved resolution
// (Contract §9.4.4/§13.7).
func blockingConflictIssue(value any) string {
	found := ""
	IterDicts(value, func(holder map[string]any) {
		if found != "" {
			return
		}
		conflicts, ok := AsArray(holder["conflicts"])
		if !ok {
			return
		}
		for _, conflictRaw := range conflicts {
			conflict, isObj := AsObject(conflictRaw)
			if !isObj {
				continue
			}
			if blocking, isBool := conflict["blocking"].(bool); !isBool || !blocking {
				continue
			}
			resolution, isObj := AsObject(conflict["resolution"])
			if isObj {
				if action, _ := AsString(resolution["action"]); action == "unresolved" {
					found = ReasonMergeBlockingConflict
					return
				}
			}
		}
	})
	return found
}

// ---------------------------------------------------------------------------
// Corpus / manifest validation
// ---------------------------------------------------------------------------

var caseFiles = []string{"source.json", "canonical.utf8", "canonical.base64", "expected.json"}

var manifestPathKeys = []string{"source_path", "canonical_utf8_path", "canonical_base64_path", "expected_path"}

var manifestMirrorKeys = []string{"expected_accept", "expected_digest", "expected_canonical_byte_length", "expected_reason_code"}

type manifestEntry struct {
	CaseID            string
	Category          string
	SourcePath        string
	CanonicalUTF8Path string
	CanonicalBase64   string
	ExpectedPath      string
	Mirrors           map[string]any
}

// CaseResult is the derived-vs-golden result of one corpus case.
type CaseResult struct {
	CaseID             string
	Category           string
	SourcePath         string
	OK                 bool
	Problems           []string
	DerivedAccept      bool
	DerivedReason      *string
	DerivedDigest      *string
	DerivedLength      int
	DerivedBase64      string
	Canonical          []byte
	CanonicalFileBytes []byte
}

// CorpusResult is a full corpus run: corpus-level integrity errors plus the
// per-case results in manifest order.
type CorpusResult struct {
	Root           string
	CorpusErrors   []string
	Cases          []CaseResult
	ManifestDigest string
}

// DefaultConformanceDir locates the shared FND-001 conformance corpus:
// RSIH_CONFORMANCE_DIR overrides, otherwise the directory is resolved by
// walking from the working directory and the executable, looking for
// specs/rsi-harness-skill-evolution/conformance (including the
// memory_graph_evolving/ and GMS-module-sibling layouts). Repository-local
// expectation copies are forbidden (§16.5).
func DefaultConformanceDir() (string, error) {
	if dir := strings.TrimSpace(os.Getenv(ConformanceDirEnvVar)); dir != "" {
		return dir, nil
	}
	var starts []string
	if wd, err := os.Getwd(); err == nil && wd != "" {
		starts = append(starts, wd)
	}
	if exe, err := os.Executable(); err == nil && exe != "" {
		starts = append(starts, filepath.Dir(exe))
	}
	var last error
	for _, start := range starts {
		dir, err := locateConformanceDir(start)
		if err == nil {
			return dir, nil
		}
		last = err
	}
	if last == nil {
		last = fmt.Errorf("no search root")
	}
	return "", fmt.Errorf("contract: cannot locate the GMS repository root (go.mod); set %s: %w", ConformanceDirEnvVar, last)
}

func locateConformanceDir(start string) (string, error) {
	dir := start
	for {
		for _, candidate := range conformanceDirCandidates(dir) {
			if isConformanceDir(candidate) {
				return candidate, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("conformance corpus not found upward from %s", start)
		}
		dir = parent
	}
}

func conformanceDirCandidates(dir string) []string {
	out := []string{
		filepath.Join(dir, defaultSpecRelToParent),
		filepath.Join(dir, "memory_graph_evolving", defaultSpecRelToParent),
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
		out = append(out, filepath.Join(filepath.Dir(dir), defaultSpecRelToParent))
	}
	return out
}

func isConformanceDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return false
	}
	_, err = os.Stat(filepath.Join(path, "manifest.json"))
	return err == nil
}

// RunCorpus walks every manifest case under root, derives each outcome
// independently of the golden expectations and compares against expected.json
// and the manifest mirrors. Corpus-level integrity problems land in
// CorpusErrors; per-case mismatches in CaseResult.Problems. The manifest
// digest is over the raw manifest.json file bytes.
func RunCorpus(root string) *CorpusResult {
	res := &CorpusResult{Root: root}

	manifestPath := filepath.Join(root, manifestFilename)
	rawManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		res.CorpusErrors = append(res.CorpusErrors, fmt.Sprintf("manifest.json missing under %s", root))
		return res
	}
	res.ManifestDigest = DigestBytes(rawManifest)

	manifest, err := ParseJSONStrict(rawManifest)
	if err != nil {
		res.CorpusErrors = append(res.CorpusErrors, fmt.Sprintf("manifest.json is not valid UTF-8 JSON: %v", err))
		return res
	}
	obj, ok := AsObject(manifest)
	if !ok {
		res.CorpusErrors = append(res.CorpusErrors, "manifest.json must be a JSON object")
		return res
	}
	if sv, _ := AsString(obj["schema_version"]); sv != ManifestSchemaVersion {
		res.CorpusErrors = append(res.CorpusErrors, fmt.Sprintf("manifest schema_version must be %q, got %q", ManifestSchemaVersion, sv))
	}
	if sv, _ := AsString(obj["contract_schema_version"]); sv != ContractSchemaVersion {
		res.CorpusErrors = append(res.CorpusErrors, fmt.Sprintf("manifest contract_schema_version must be %q, got %q", ContractSchemaVersion, sv))
	}
	rawCases, ok := AsArray(obj["cases"])
	if !ok || len(rawCases) == 0 {
		res.CorpusErrors = append(res.CorpusErrors, "manifest cases must be a non-empty array")
		return res
	}

	entries := validateManifestEntries(root, rawCases, &res.CorpusErrors)
	for _, entry := range entries {
		res.Cases = append(res.Cases, evaluateEntry(root, entry))
	}
	return res
}

func validateManifestEntries(root string, rawCases []any, errorsOut *[]string) []manifestEntry {
	validCategories := map[string]bool{}
	for _, c := range Categories {
		validCategories[c] = true
	}

	var entries []manifestEntry
	seenIDs := map[string]bool{}
	for index, raw := range rawCases {
		entry, isObj := AsObject(raw)
		if !isObj {
			*errorsOut = append(*errorsOut, fmt.Sprintf("manifest cases[%d] must be an object", index))
			continue
		}
		caseID, ok := AsString(entry["case_id"])
		if !ok || caseID == "" {
			*errorsOut = append(*errorsOut, fmt.Sprintf("manifest cases[%d] has invalid case_id", index))
			continue
		}
		if seenIDs[caseID] {
			*errorsOut = append(*errorsOut, fmt.Sprintf("duplicate case id in manifest: %s", caseID))
			continue
		}
		seenIDs[caseID] = true
		category, _ := AsString(entry["category"])
		if !validCategories[category] {
			*errorsOut = append(*errorsOut, fmt.Sprintf("case %s has unknown category %q (expected one of %s)", caseID, category, strings.Join(Categories, "|")))
			continue
		}
		pathsOK := true
		for _, key := range manifestPathKeys {
			value, isStr := AsString(entry[key])
			if !isStr || value == "" {
				*errorsOut = append(*errorsOut, fmt.Sprintf("case %s missing %s", caseID, key))
				pathsOK = false
			}
		}
		if !pathsOK {
			continue
		}
		resolved := map[string]string{}
		for _, key := range manifestPathKeys {
			value, _ := AsString(entry[key])
			path, ok := safeRelative(root, value)
			if !ok {
				*errorsOut = append(*errorsOut, fmt.Sprintf("case %s %s is not a safe relative path: %q", caseID, key, value))
				pathsOK = false
				continue
			}
			if info, err := os.Stat(path); err != nil || info.IsDir() {
				*errorsOut = append(*errorsOut, fmt.Sprintf("case %s %s does not exist: %s", caseID, key, value))
				pathsOK = false
			}
			resolved[key] = value
		}
		if !pathsOK {
			continue
		}
		expectedDir := categoryDirs[category]
		if !strings.HasPrefix(resolved["source_path"], expectedDir+"/") {
			*errorsOut = append(*errorsOut, fmt.Sprintf("case %s category %q must live under %s/", caseID, category, expectedDir))
			continue
		}
		mirrors := map[string]any{}
		for _, key := range manifestMirrorKeys {
			if v, present := entry[key]; present {
				mirrors[key] = v
			}
		}
		entries = append(entries, manifestEntry{
			CaseID:            caseID,
			Category:          category,
			SourcePath:        resolved["source_path"],
			CanonicalUTF8Path: resolved["canonical_utf8_path"],
			CanonicalBase64:   resolved["canonical_base64_path"],
			ExpectedPath:      resolved["expected_path"],
			Mirrors:           mirrors,
		})
	}

	// Corpus completeness: every case directory must be declared exactly once
	// and contain all four files (Contract §16.1/§16.2).
	declaredDirs := map[string]bool{}
	for _, entry := range entries {
		parts := strings.Split(entry.SourcePath, "/")
		declaredDirs[strings.Join(parts[:len(parts)-1], "/")] = true
	}
	sortedDirs := make([]string, 0, len(categoryDirs))
	for _, category := range Categories {
		sortedDirs = append(sortedDirs, category)
	}
	sort.Strings(sortedDirs)
	for _, category := range sortedDirs {
		dirname := categoryDirs[category]
		base := filepath.Join(root, dirname)
		children, err := os.ReadDir(base)
		if err != nil {
			if hasCategory(entries, category) {
				*errorsOut = append(*errorsOut, fmt.Sprintf("category directory missing: %s/", dirname))
			}
			continue // no declared cases and no directory: partial corpus is fine
		}
		for _, child := range children {
			if !child.IsDir() {
				continue
			}
			rel := dirname + "/" + child.Name()
			if !declaredDirs[rel] {
				*errorsOut = append(*errorsOut, fmt.Sprintf("undeclared case directory not in manifest: %s", rel))
				continue
			}
			for _, name := range caseFiles {
				if _, err := os.Stat(filepath.Join(base, child.Name(), name)); err != nil {
					*errorsOut = append(*errorsOut, fmt.Sprintf("case directory %s missing %s", rel, name))
				}
			}
		}
	}
	return entries
}

func hasCategory(entries []manifestEntry, category string) bool {
	for _, entry := range entries {
		if entry.Category == category {
			return true
		}
	}
	return false
}

// safeRelative resolves rel under root after rejecting absolute paths,
// ".." components and backslashes, and verifies the resolved path stays
// inside root even through symlinks.
func safeRelative(root, rel string) (string, bool) {
	if filepath.IsAbs(rel) || strings.Contains(rel, "\\") || strings.HasPrefix(rel, "/") {
		return "", false
	}
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if part == ".." {
			return "", false
		}
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", false
	}
	resolved := filepath.Join(root, rel)
	evaluated, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		return "", false
	}
	relative, err := filepath.Rel(resolvedRoot, evaluated)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	return resolved, true
}

func evaluateEntry(root string, entry manifestEntry) CaseResult {
	result := CaseResult{
		CaseID:     entry.CaseID,
		Category:   entry.Category,
		SourcePath: entry.SourcePath,
		OK:         true,
	}
	var problems []string

	sourceBytes, err := os.ReadFile(filepath.Join(root, entry.SourcePath))
	if err != nil {
		result.OK = false
		result.Problems = []string{fmt.Sprintf("source.json unreadable: %v", err)}
		return result
	}
	canonicalFileBytes, err := os.ReadFile(filepath.Join(root, entry.CanonicalUTF8Path))
	if err != nil {
		result.OK = false
		result.Problems = []string{fmt.Sprintf("canonical.utf8 unreadable: %v", err)}
		return result
	}
	base64Text, err := os.ReadFile(filepath.Join(root, entry.CanonicalBase64))
	if err != nil {
		result.OK = false
		result.Problems = []string{fmt.Sprintf("canonical.base64 unreadable: %v", err)}
		return result
	}
	result.CanonicalFileBytes = canonicalFileBytes

	// Base64 sidecar must decode to exactly the canonical.utf8 bytes (§16.1).
	decoded, err := decodeBase64Sidecar(base64Text)
	if err != nil {
		problems = append(problems, fmt.Sprintf("canonical.base64 is not valid Base64: %v", err))
	} else if !bytes.Equal(decoded, canonicalFileBytes) {
		problems = append(problems, "canonical.base64 does not decode to canonical.utf8 bytes")
	}
	if bytes.HasPrefix(canonicalFileBytes, bomUTF8) {
		problems = append(problems, "canonical.utf8 must not start with a UTF-8 BOM")
	}

	// Derived evaluation is independent of the golden expectations.
	outcome := DeriveOutcome(sourceBytes, entry.Category, canonicalFileBytes)
	result.DerivedAccept = outcome.Accept
	if outcome.ReasonCode != "" {
		reason := outcome.ReasonCode
		result.DerivedReason = &reason
	}
	result.Canonical = outcome.Canonical
	if outcome.Canonical != nil {
		digest := DigestBytes(outcome.Canonical)
		result.DerivedDigest = &digest
		result.DerivedLength = len(outcome.Canonical)
	}
	result.DerivedBase64 = base64.StdEncoding.EncodeToString(canonicalFileBytes)

	expectedRaw, err := os.ReadFile(filepath.Join(root, entry.ExpectedPath))
	if err != nil {
		result.OK = false
		result.Problems = []string{fmt.Sprintf("expected.json unreadable: %v", err)}
		return result
	}
	expectedValue, err := ParseJSONStrict(expectedRaw)
	if err != nil {
		result.OK = false
		result.Problems = append(problems, fmt.Sprintf("%s expected.json is not valid UTF-8 JSON: %v", entry.CaseID, err))
		return result
	}
	expected, ok := AsObject(expectedValue)
	if !ok {
		result.OK = false
		result.Problems = append(problems, fmt.Sprintf("%s: expected.json must be a JSON object", entry.CaseID))
		return result
	}

	problems = append(problems, checkExpectedShape(entry, expected)...)
	checkManifestMirrors(entry, expected, &problems)
	checkDigestExpectations(expected, outcome, canonicalFileBytes, &problems)
	checkOutcomeParity(expected, outcome, canonicalFileBytes, &problems)

	if len(problems) > 0 {
		result.OK = false
		result.Problems = problems
	}
	return result
}

// decodeBase64Sidecar mirrors base64.b64decode(text.strip(), validate=True):
// surrounding whitespace is trimmed, everything else must be standard
// alphabet with correct padding.
func decodeBase64Sidecar(text []byte) ([]byte, error) {
	trimmed := strings.TrimSpace(string(text))
	return base64.StdEncoding.Strict().DecodeString(trimmed)
}

func checkExpectedShape(entry manifestEntry, expected map[string]any) []string {
	var problems []string
	if sv, ok := AsString(expected["schema_version"]); !ok || sv != ExpectedSchemaVersion {
		problems = append(problems, fmt.Sprintf("%s: expected schema_version must be %q", entry.CaseID, ExpectedSchemaVersion))
	}
	if cid, ok := AsString(expected["case_id"]); !ok || cid != entry.CaseID {
		problems = append(problems, fmt.Sprintf("%s: expected case_id does not match manifest", entry.CaseID))
	}
	accept, acceptIsBool := expected["expected_accept"].(bool)
	if !acceptIsBool {
		problems = append(problems, fmt.Sprintf("%s: expected_accept must be a boolean", entry.CaseID))
	}
	reasonRaw, hasReason := expected["expected_reason_code"]
	if hasReason && reasonRaw != nil {
		reason, isStr := AsString(reasonRaw)
		if acceptIsBool && !accept {
			if !isStr || !s1ReasonCodes[reason] {
				problems = append(problems, fmt.Sprintf("%s: rejected case must carry expected_reason_code from the closed registry", entry.CaseID))
			}
		} else {
			problems = append(problems, fmt.Sprintf("%s: accepted case must not carry expected_reason_code", entry.CaseID))
		}
	}
	return problems
}

func checkManifestMirrors(entry manifestEntry, expected map[string]any, problems *[]string) {
	for key, value := range entry.Mirrors {
		if got, present := expected[key]; present && !EqualJSON(got, value) {
			*problems = append(*problems, fmt.Sprintf("case %s: manifest mirror %s=%s disagrees with expected.json %s", entry.CaseID, key, CanonicalKey(value), CanonicalKey(got)))
		}
	}
}

func checkDigestExpectations(expected map[string]any, outcome Outcome, canonicalFileBytes []byte, problems *[]string) {
	if outcome.Canonical != nil {
		expectedDigest, isStr := AsString(expected["expected_digest"])
		if !isStr {
			*problems = append(*problems, "canonicalizable case missing expected_digest")
		} else if expectedDigest != DigestBytes(outcome.Canonical) {
			*problems = append(*problems, fmt.Sprintf("expected_digest %s != derived %s", expectedDigest, DigestBytes(outcome.Canonical)))
		}
		lengthRaw, hasLength := expected["expected_canonical_byte_length"]
		if !hasLength || !IsIntegerNumber(lengthRaw) {
			*problems = append(*problems, "canonicalizable case missing expected_canonical_byte_length")
		} else {
			want, _ := parseBigInt(lengthRaw)
			got := new(big.Int).SetInt64(int64(len(outcome.Canonical)))
			if want.Cmp(got) != 0 {
				*problems = append(*problems, fmt.Sprintf("expected_canonical_byte_length %s != derived %d", want.String(), len(outcome.Canonical)))
			}
		}
		return
	}
	if _, has := expected["expected_digest"]; has {
		*problems = append(*problems, "non-canonicalizable case must not declare digest/length")
	}
	if _, has := expected["expected_canonical_byte_length"]; has {
		*problems = append(*problems, "non-canonicalizable case must not declare digest/length")
	}
	if len(canonicalFileBytes) != 0 {
		*problems = append(*problems, "non-canonicalizable case must have empty canonical files")
	}
}

func checkOutcomeParity(expected map[string]any, outcome Outcome, canonicalFileBytes []byte, problems *[]string) {
	expectedAccept, _ := expected["expected_accept"].(bool)
	if outcome.Accept != expectedAccept {
		*problems = append(*problems, fmt.Sprintf("derived accept=%v (reason %s) but expected accept=%v", outcome.Accept, outcome.ReasonCode, expectedAccept))
		return
	}
	if outcome.Accept {
		return
	}
	expectedReason, _ := AsString(expected["expected_reason_code"])
	if outcome.ReasonCode != expectedReason {
		*problems = append(*problems, fmt.Sprintf("derived reason %s != expected reason %s", outcome.ReasonCode, expectedReason))
	}
	// Except for the digest-tamper fixture itself, the canonical file must
	// still faithfully hold the canonicalization of the source.
	if outcome.ReasonCode != ReasonDigestMismatch && outcome.Canonical != nil && !bytes.Equal(canonicalFileBytes, outcome.Canonical) {
		*problems = append(*problems, "canonical.utf8 bytes do not match canonicalized source")
	}
}
