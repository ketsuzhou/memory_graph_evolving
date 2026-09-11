package contract

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Conformance runner over the FND-001 shared JCS/SHA-256 golden corpus
// (Contract §16). This file is a semantic port of the reference validator
// ($FIX/validate.py): every accept/reject decision and reason code is
// derived independently from the case source, and only then compared with
// expected.json. The corpus is read-only; no expected value is ever copied
// into this implementation.

const (
	manifestSchemaVersion = "rsih-skill-evolution.conformance-manifest.v1"
	expectedSchemaVersion = "rsih-skill-evolution.conformance-expected.v1"
	contractSchemaVersion = "rsih-skill-evolution.system-contract.v1"
)

var categories = []string{"canonicalization", "ref", "artifact", "event", "merge", "negative"}

var categoryDirs = map[string]string{
	"canonicalization": "canonicalization",
	"ref":              "refs",
	"artifact":         "artifacts",
	"event":            "events",
	"merge":            "merge",
	"negative":         "negative",
}

var caseFiles = []string{"source.json", "canonical.utf8", "canonical.base64", "expected.json"}

// Closed reason-code registry (Contract §16.4 codes plus the fixture-level
// extensions adopted by CTR-004, Contract §13.7.1). An expected reason
// outside it is a corpus error.
var contractReasonCodes = map[string]struct{}{
	"NON_INTEGER_NUMBER":         {},
	"UNKNOWN_REQUIRED_EXTENSION": {},
	"DIGEST_MISMATCH":            {},
	"REF_MISMATCH":               {},
	"NON_EXACT_REF":              {},
	"COMPOSITE_CYCLE":            {},
	"PORT_SCHEMA_MISSING":        {},
	"MERGE_BLOCKING_CONFLICT":    {},
	"EVIDENCE_NOT_COMMITTED":     {},
	"PROJECTION_SEQUENCE_GAP":    {},
	"PERMISSION_CAP_EXCEEDED":    {},
	"PROJECTION_EVENT_CONFLICT":  {},
}

var extensionReasonCodes = map[string]struct{}{
	"BOM_NOT_ALLOWED":            {},
	"INVALID_JSON":               {},
	"ILLEGAL_STATE_TRANSITION":   {},
	"SIMILARITY_BELOW_THRESHOLD": {},
}

func allReasonCodes() map[string]struct{} {
	out := make(map[string]struct{}, len(contractReasonCodes)+len(extensionReasonCodes))
	for k := range contractReasonCodes {
		out[k] = struct{}{}
	}
	for k := range extensionReasonCodes {
		out[k] = struct{}{}
	}
	return out
}

// v1 conformance policy constants (Contract §1.3.3, §7.17, §7.7, §9.4).
var knownExtensionsV1 = map[string]struct{}{} // none registered in v1: required:true always fails closed

var hostCapabilitiesV1 = map[string]struct{}{
	"memory_explore": {},
	"memory_expand":  {},
	"skill_get":      {},
}

var evidenceCommitStates = map[string]struct{}{"committed": {}, "sealed": {}}

var legalProposalTransitions = map[[2]string]struct{}{
	{"none", "proposed"}:                       {},
	{"proposed", "admitted"}:                   {},
	{"proposed", "duplicate"}:                  {},
	{"proposed", "rejected"}:                   {},
	{"proposed", "stale"}:                      {},
	{"proposed", "withdrawn"}:                  {},
	{"admitted", "synthesizing"}:               {},
	{"admitted", "rejected"}:                   {},
	{"admitted", "stale"}:                      {},
	{"admitted", "withdrawn"}:                  {},
	{"synthesizing", "candidate_bound"}:        {},
	{"synthesizing", "inconclusive"}:           {},
	{"synthesizing", "rejected"}:               {},
	{"synthesizing", "stale"}:                  {},
	{"candidate_bound", "validating"}:          {},
	{"candidate_bound", "stale"}:               {},
	{"validating", "replaying"}:                {},
	{"validating", "rejected"}:                 {},
	{"validating", "stale"}:                    {},
	{"replaying", "decision_pending"}:          {},
	{"replaying", "rejected"}:                  {},
	{"replaying", "inconclusive"}:              {},
	{"replaying", "stale"}:                     {},
	{"decision_pending", "activation_pending"}: {},
	{"decision_pending", "rejected"}:           {},
	{"decision_pending", "inconclusive"}:       {},
	{"activation_pending", "released"}:         {},
	{"activation_pending", "rejected"}:         {},
	{"activation_pending", "stale"}:            {},
}

var utf8BOM = []byte("\xef\xbb\xbf")

// Outcome is the derived evaluation result, computed independently of
// expected.json.
type Outcome struct {
	Accept     bool
	ReasonCode string // empty when accepted
	Canonical  []byte // canonical bytes when the case is canonicalizable, else nil
	Digest     string // digest of Canonical when canonicalizable, else empty
}

// CaseResult is the per-case verdict.
type CaseResult struct {
	CaseID              string
	Category            string
	SourcePath          string // relative to the conformance dir, as declared
	CanonicalUTF8Path   string
	CanonicalBase64Path string
	ExpectedPath        string
	OK                  bool
	Problems            []string
	Derived             Outcome
	CanonicalFileBytes  []byte // raw canonical.utf8 file content
}

// CorpusResult aggregates corpus integrity errors and per-case verdicts.
type CorpusResult struct {
	CorpusErrors   []string
	Cases          []CaseResult // manifest order
	ManifestDigest string       // sha256 of manifest.json file bytes
}

// ---------------------------------------------------------------------------
// Derived evaluation (fail-closed at the first violation)
// ---------------------------------------------------------------------------

// DeriveOutcome derives (accept, reason, canonical bytes) from the case
// input alone (Contract §16.3 evaluation order).
func DeriveOutcome(source []byte, category string, canonicalFileBytes []byte) Outcome {
	// 1. BOM never enters the hashed core (§6.1.1).
	if bytes.HasPrefix(source, utf8BOM) {
		return Outcome{Accept: false, ReasonCode: "BOM_NOT_ALLOWED"}
	}
	// 2. Must be valid UTF-8 JSON.
	parsed, err := ParseJSON(source)
	if err != nil {
		return Outcome{Accept: false, ReasonCode: "INVALID_JSON"}
	}
	// 3. Integer-only canonicalization (§6.1.4) after pair normalization.
	canonical, err := JCS(NormalizeForHashing(parsed))
	if err != nil {
		if ce, ok := err.(*CanonicalizationError); ok {
			return Outcome{Accept: false, ReasonCode: ce.ReasonCode}
		}
		return Outcome{Accept: false, ReasonCode: "INVALID_JSON"}
	}
	// 4. Closed semantic checks (§13.7).
	if issue := semanticIssue(parsed, category); issue != "" {
		return Outcome{Accept: false, ReasonCode: issue, Canonical: canonical, Digest: DigestBytes(canonical)}
	}
	// 5. The golden sidecar must hold exactly the canonicalization.
	if !bytes.Equal(canonicalFileBytes, canonical) {
		return Outcome{Accept: false, ReasonCode: "DIGEST_MISMATCH", Canonical: canonical, Digest: DigestBytes(canonical)}
	}
	// 6. Accepted.
	return Outcome{Accept: true, Canonical: canonical, Digest: DigestBytes(canonical)}
}

type semanticCheck func(Value) string

var genericChecks = []semanticCheck{extensionsIssue, commitStateIssue}

var categoryChecks = map[string][]semanticCheck{
	"canonicalization": {},
	"ref":              {refShapeIssue, candidateReleasedEqualityIssue},
	"artifact":         {kindDriftIssue, compositePortsIssue, compositeCycleIssue, permissionCapIssue},
	"event":            {projectionConflictIssue, projectionGapIssue, proposalTransitionIssue},
	"merge":            {belowThresholdIssue, blockingConflictIssue},
	"negative":         {},
}

func semanticIssue(v Value, category string) string {
	for _, check := range append(append([]semanticCheck{}, genericChecks...), categoryChecks[category]...) {
		if code := check(v); code != "" {
			return code
		}
	}
	return ""
}

// iterObjects visits every object in the tree (preorder).
func iterObjects(v Value, fn func(*Object)) {
	switch t := v.(type) {
	case *Object:
		fn(t)
		for _, key := range t.Keys() {
			child, _ := t.Get(key)
			iterObjects(child, fn)
		}
	case Array:
		for _, item := range t {
			iterObjects(item, fn)
		}
	}
}

// isJSONInt mirrors the reference _is_int: a JSON integer, never a bool.
func isJSONInt(v Value, ok bool) bool {
	if !ok {
		return false
	}
	n, isNum := v.(Number)
	return isNum && n.IsInteger()
}

// jsonValueEqual compares two JSON values by canonical bytes (deep equality).
func jsonValueEqual(a, b Value) bool {
	ab, errA := JCS(a)
	bb, errB := JCS(b)
	return errA == nil && errB == nil && bytes.Equal(ab, bb)
}

// canonicalKey returns the canonical bytes of v, or "" when v cannot be
// canonicalized (cannot happen at semantic-check time: canonicalization of
// the whole document already succeeded).
func canonicalKey(v Value) string {
	b, err := JCS(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// Generic checks ------------------------------------------------------------

// extensionsIssue: an extension entry with required:true whose key is not in
// KNOWN_EXTENSIONS_V1 (§1.3.3/§6.4). Unknown optional extensions are ignored.
func extensionsIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		extVal, present := o.Get("extensions")
		if !present {
			return
		}
		ext, isObj := extVal.(*Object)
		if !isObj {
			return
		}
		for _, key := range ext.Keys() {
			entryVal, _ := ext.Get(key)
			entry, isEntry := entryVal.(*Object)
			if !isEntry {
				continue
			}
			reqVal, hasReq := entry.Get("required")
			req, isBool := reqVal.(Bool)
			if hasReq && isBool && bool(req) {
				if _, known := knownExtensionsV1[key]; !known {
					found = "UNKNOWN_REQUIRED_EXTENSION"
					return
				}
			}
		}
	})
	return found
}

// commitStateIssue: any object carrying commit_state outside
// {committed, sealed} (§7.7/§13.7). Non-string values are outside the set.
func commitStateIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		val, present := o.Get("commit_state")
		if !present {
			return
		}
		s, isStr := val.(String)
		if _, ok := evidenceCommitStates[string(s)]; !isStr || !ok {
			found = "EVIDENCE_NOT_COMMITTED"
		}
	})
	return found
}

// Category `ref` checks ------------------------------------------------------

// refShapeIssue: exact-ref shape (§5.1.5/§6.2). Graph node forms, `latest`
// or non-integer versions, and naked ids (identifier without version and/or
// digest) are NON_EXACT_REF. CandidateArtifactRef is exact via
// (candidate_id, body_digest) and carries no version (§7.4).
func refShapeIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		if _, has := o.Get("graph_node_id"); has {
			found = "NON_EXACT_REF"
			return
		}
		if _, has := o.Get("node_id"); has {
			found = "NON_EXACT_REF"
			return
		}
		if _, has := o.Get("candidate_id"); has {
			if _, hasBody := o.Get("body_digest"); !hasBody {
				found = "NON_EXACT_REF"
			}
			return
		}
		if _, has := o.Get("lineage_id"); has {
			version, hasVersion := o.Get("version")
			if !isJSONInt(version, hasVersion) {
				found = "NON_EXACT_REF"
				return
			}
			if _, hasDigest := o.Get("artifact_digest"); !hasDigest {
				found = "NON_EXACT_REF"
			}
			return
		}
		if _, has := o.Get("evidence_id"); has {
			version, hasVersion := o.Get("version")
			if !isJSONInt(version, hasVersion) {
				found = "NON_EXACT_REF"
				return
			}
			if _, hasDigest := o.Get("evidence_digest"); !hasDigest {
				found = "NON_EXACT_REF"
			}
			return
		}
		if _, has := o.Get("id"); has {
			version, hasVersion := o.Get("version")
			if !isJSONInt(version, hasVersion) {
				found = "NON_EXACT_REF"
				return
			}
			if _, hasDigest := o.Get("digest"); !hasDigest {
				found = "NON_EXACT_REF"
			}
		}
	})
	return found
}

// candidateReleasedEqualityIssue (§16.4 #7): when a value carries both
// candidate_ref and released_ref, candidate body_digest must equal released
// artifact_digest.
func candidateReleasedEqualityIssue(root Value) string {
	obj, ok := root.(*Object)
	if !ok {
		return ""
	}
	candVal, hasCand := obj.Get("candidate_ref")
	relVal, hasRel := obj.Get("released_ref")
	if !hasCand || !hasRel {
		return ""
	}
	cand, candOK := candVal.(*Object)
	rel, relOK := relVal.(*Object)
	if !candOK || !relOK {
		return ""
	}
	candDigest, _ := cand.Get("body_digest")
	relDigest, _ := rel.Get("artifact_digest")
	if !jsonValueEqual(candDigest, relDigest) {
		return "DIGEST_MISMATCH"
	}
	return ""
}

// Category `artifact` checks --------------------------------------------------

// kindDriftIssue: two SkillArtifactRefs sharing the exact identity
// (lineage_id, version, artifact_digest) with different kind (§6.2).
func kindDriftIssue(root Value) string {
	groups := map[string]map[string]struct{}{}
	iterObjects(root, func(o *Object) {
		if _, ok := AsSkillArtifactRef(o); !ok {
			return
		}
		lineage, _ := o.Get("lineage_id")
		version, _ := o.Get("version")
		digest, _ := o.Get("artifact_digest")
		kind, _ := o.Get("kind")
		idKey := canonicalKey(Array{lineage, version, digest})
		kindKey := canonicalKey(kind)
		if groups[idKey] == nil {
			groups[idKey] = map[string]struct{}{}
		}
		groups[idKey][kindKey] = struct{}{}
	})
	for _, kinds := range groups {
		if len(kinds) > 1 {
			return "REF_MISMATCH"
		}
	}
	return ""
}

// compositePortsIssue: composite children must carry input_port and
// output_port (§5.2.5/§8.4).
func compositePortsIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		childrenVal, hasChildren := o.Get("children")
		edgesVal, hasEdges := o.Get("edges")
		if !hasChildren || !hasEdges {
			return
		}
		children, childrenOK := childrenVal.(Array)
		_, edgesOK := edgesVal.(Array)
		if !childrenOK || !edgesOK {
			return
		}
		for _, childVal := range children {
			child, isObj := childVal.(*Object)
			if !isObj {
				continue
			}
			_, hasIn := child.Get("input_port")
			_, hasOut := child.Get("output_port")
			if !hasIn || !hasOut {
				found = "PORT_SCHEMA_MISSING"
				return
			}
		}
	})
	return found
}

// compositeCycleIssue: child edges must form a DAG (§5.2.3/§8.4).
func compositeCycleIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		childrenVal, hasChildren := o.Get("children")
		edgesVal, hasEdges := o.Get("edges")
		if !hasChildren || !hasEdges {
			return
		}
		children, childrenOK := childrenVal.(Array)
		edges, edgesOK := edgesVal.(Array)
		if !childrenOK || !edgesOK {
			return
		}
		nodes := map[string]struct{}{}
		for _, childVal := range children {
			child, isObj := childVal.(*Object)
			if !isObj {
				continue
			}
			idVal, _ := child.Get("child_id")
			nodes[canonicalKey(idVal)] = struct{}{}
		}
		adjacency := map[string][]string{}
		for _, edgeVal := range edges {
			edge, isObj := edgeVal.(*Object)
			if !isObj {
				continue
			}
			fromVal, _ := edge.Get("from_child_id")
			toVal, _ := edge.Get("to_child_id")
			adjacency[canonicalKey(fromVal)] = append(adjacency[canonicalKey(fromVal)], canonicalKey(toVal))
		}
		if hasCompositeCycle(nodes, adjacency) {
			found = "COMPOSITE_CYCLE"
		}
	})
	return found
}

// hasCompositeCycle: Kahn topological sort over the nodes; a cycle exists
// iff the sort cannot consume every node. Edges touching non-child nodes are
// ignored, matching the reference DFS behavior.
func hasCompositeCycle(nodes map[string]struct{}, adjacency map[string][]string) bool {
	indegree := make(map[string]int, len(nodes))
	for node := range nodes {
		indegree[node] = 0
	}
	for from, tos := range adjacency {
		if _, isNode := nodes[from]; !isNode {
			continue
		}
		for _, to := range tos {
			if _, isNode := nodes[to]; !isNode {
				continue
			}
			indegree[to]++
		}
	}
	queue := make([]string, 0, len(nodes))
	for node, deg := range indegree {
		if deg == 0 {
			queue = append(queue, node)
		}
	}
	processed := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		processed++
		for _, next := range adjacency[node] {
			if _, isNode := nodes[next]; !isNode {
				continue
			}
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	return processed != len(nodes)
}

// permissionCapIssue: every permission/orchestration_permissions capability
// must be inside the v1 Host tool surface (§7.17/§14.2).
func permissionCapIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		for _, key := range []string{"permissions", "orchestration_permissions"} {
			entriesVal, present := o.Get(key)
			if !present {
				continue
			}
			entries, isArr := entriesVal.(Array)
			if !isArr {
				continue
			}
			for _, entryVal := range entries {
				entry, isObj := entryVal.(*Object)
				if !isObj {
					continue
				}
				capability, hasCap := StringOf(entry, "capability")
				if _, allowed := hostCapabilitiesV1[capability]; !hasCap || !allowed {
					found = "PERMISSION_CAP_EXCEEDED"
					return
				}
			}
		}
	})
	return found
}

// Category `event` checks -----------------------------------------------------

// projectionBatch returns the root event batch: the root array itself, or
// the events array of a root object.
func projectionBatch(root Value) (Array, bool) {
	switch t := root.(type) {
	case Array:
		return t, true
	case *Object:
		eventsVal, present := t.Get("events")
		if !present {
			return nil, false
		}
		events, isArr := eventsVal.(Array)
		return events, isArr
	}
	return nil, false
}

// qualifyingEvents filters the batch to objects with an integer
// activation_sequence; a batch qualifies only when every item qualifies and
// at least two events are present (§9.3).
func qualifyingEvents(batch Array) ([]*Object, bool) {
	var events []*Object
	for _, item := range batch {
		obj, isObj := item.(*Object)
		if !isObj {
			return nil, false
		}
		seq, hasSeq := obj.Get("activation_sequence")
		if !isJSONInt(seq, hasSeq) {
			return nil, false
		}
		events = append(events, obj)
	}
	if len(events) < 2 {
		return nil, false
	}
	return events, true
}

// projectionConflictIssue: same activation_sequence with different
// event_digest (§9.3). Identical duplicates are idempotent (§13.2).
func projectionConflictIssue(root Value) string {
	batch, ok := projectionBatch(root)
	if !ok {
		return ""
	}
	events, ok := qualifyingEvents(batch)
	if !ok {
		return ""
	}
	bySequence := map[string]map[string]struct{}{}
	for _, event := range events {
		seqVal, _ := event.Get("activation_sequence")
		digestVal, _ := event.Get("event_digest")
		seqKey := canonicalKey(seqVal)
		digestKey := canonicalKey(digestVal)
		if bySequence[seqKey] == nil {
			bySequence[seqKey] = map[string]struct{}{}
		}
		bySequence[seqKey][digestKey] = struct{}{}
	}
	for _, digests := range bySequence {
		if len(digests) > 1 {
			return "PROJECTION_EVENT_CONFLICT"
		}
	}
	return ""
}

// projectionGapIssue: activation sequences must be consumed contiguously;
// stop on the first gap (§9.3).
func projectionGapIssue(root Value) string {
	batch, ok := projectionBatch(root)
	if !ok {
		return ""
	}
	events, ok := qualifyingEvents(batch)
	if !ok {
		return ""
	}
	seen := map[string]*big.Int{}
	var sequences []*big.Int
	for _, event := range events {
		seqVal, _ := event.Get("activation_sequence")
		seq := canonicalKey(seqVal)
		if _, dup := seen[seq]; dup {
			continue
		}
		n := seqVal.(Number)
		bi, _ := n.Int()
		seen[seq] = bi
		sequences = append(sequences, bi)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i].Cmp(sequences[j]) < 0 })
	one := big.NewInt(1)
	diff := new(big.Int)
	for i := 1; i < len(sequences); i++ {
		diff.Sub(sequences[i], sequences[i-1])
		if diff.Cmp(one) != 0 {
			return "PROJECTION_SEQUENCE_GAP"
		}
	}
	return ""
}

// proposalTransitionIssue: MergeProposalEvent transitions must be legal and
// terminal states never reopen (§9.4).
func proposalTransitionIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		event, ok := AsMergeProposalEvent(o)
		if !ok {
			return
		}
		if !event.HasFromState || !event.HasToState {
			found = "ILLEGAL_STATE_TRANSITION"
			return
		}
		pair := [2]string{event.FromState, event.ToState}
		if _, legal := legalProposalTransitions[pair]; !legal {
			found = "ILLEGAL_STATE_TRANSITION"
		}
	})
	return found
}

// Category `merge` checks -----------------------------------------------------

// belowThresholdIssue: below_suggestion must not auto-admit (§15.2 MT1).
func belowThresholdIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		if sa, ok := AsSimilarityAssessment(o); ok && sa.HasBand && sa.Band == "below_suggestion" {
			found = "SIMILARITY_BELOW_THRESHOLD"
		}
	})
	return found
}

// blockingConflictIssue: a blocking conflict resolved as unresolved
// (§9.4.4/§13.7).
func blockingConflictIssue(root Value) string {
	found := ""
	iterObjects(root, func(o *Object) {
		if found != "" {
			return
		}
		conflictsVal, present := o.Get("conflicts")
		if !present {
			return
		}
		conflicts, isArr := conflictsVal.(Array)
		if !isArr {
			return
		}
		for _, conflictVal := range conflicts {
			conflict, isObj := conflictVal.(*Object)
			if !isObj {
				continue
			}
			blockingVal, hasBlocking := conflict.Get("blocking")
			blocking, isBool := blockingVal.(Bool)
			if !hasBlocking || !isBool || !bool(blocking) {
				continue
			}
			resolutionVal, hasResolution := conflict.Get("resolution")
			if !hasResolution {
				continue
			}
			resolution, isObj := resolutionVal.(*Object)
			if isObj {
				if action, has := StringOf(resolution, "action"); has && action == "unresolved" {
					found = "MERGE_BLOCKING_CONFLICT"
					return
				}
			}
		}
	})
	return found
}

// ---------------------------------------------------------------------------
// Corpus / manifest validation (Contract §16.1/§16.2)
// ---------------------------------------------------------------------------

type manifestEntry struct {
	caseID              string
	category            string
	sourcePath          string
	canonicalUTF8Path   string
	canonicalBase64Path string
	expectedPath        string
	mirrors             map[string]Value
}

// ConformanceDir resolves the golden corpus directory. RSIH_CONFORMANCE_DIR
// overrides; the default is the frozen relative location computed from this
// repository's module root.
func ConformanceDir() (string, error) {
	if env := os.Getenv("RSIH_CONFORMANCE_DIR"); env != "" {
		return env, nil
	}
	root, err := moduleRoot()
	if err != nil {
		return "", fmt.Errorf("locate host repo root for conformance dir: %w", err)
	}
	return filepath.Clean(filepath.Join(root, "..", "specs", "rsi-harness-skill-evolution", "conformance")), nil
}

func moduleRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found upward from %s", wd)
		}
		dir = parent
	}
}

// RunCorpus validates the corpus under dir. Corpus integrity problems are
// reported in CorpusErrors (exit-2 class); case mismatches in CaseResult
// (exit-1 class). A non-directory input is a hard error.
func RunCorpus(dir string) (*CorpusResult, error) {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("conformance dir %s is not a directory", dir)
	}
	result := &CorpusResult{}
	manifest, manifestBytes, ok := loadManifest(dir, &result.CorpusErrors)
	if !ok {
		return result, nil
	}
	result.ManifestDigest = DigestBytes(manifestBytes)
	entries := validateManifestEntries(dir, manifest, &result.CorpusErrors)
	for i := range entries {
		result.Cases = append(result.Cases, evaluateEntry(dir, &entries[i]))
	}
	return result, nil
}

func loadManifest(root string, errors *[]string) (*Object, []byte, bool) {
	data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		*errors = append(*errors, fmt.Sprintf("manifest.json missing under %s", root))
		return nil, nil, false
	}
	v, err := ParseJSON(data)
	if err != nil {
		*errors = append(*errors, fmt.Sprintf("manifest.json is not valid UTF-8 JSON: %v", err))
		return nil, nil, false
	}
	manifest, ok := v.(*Object)
	if !ok {
		*errors = append(*errors, "manifest.json must be a JSON object")
		return nil, nil, false
	}
	if sv, _ := StringOf(manifest, "schema_version"); sv != manifestSchemaVersion {
		*errors = append(*errors, fmt.Sprintf("manifest schema_version must be %q, got %q", manifestSchemaVersion, sv))
	}
	if cv, _ := StringOf(manifest, "contract_schema_version"); cv != contractSchemaVersion {
		*errors = append(*errors, fmt.Sprintf("manifest contract_schema_version must be %q, got %q", contractSchemaVersion, cv))
	}
	casesVal, present := manifest.Get("cases")
	if !present {
		*errors = append(*errors, "manifest cases must be a non-empty array")
		return nil, nil, false
	}
	cases, isArr := casesVal.(Array)
	if !isArr || len(cases) == 0 {
		*errors = append(*errors, "manifest cases must be a non-empty array")
		return nil, nil, false
	}
	return manifest, data, true
}

func validateManifestEntries(root string, manifest *Object, errors *[]string) []manifestEntry {
	requiredPaths := []string{"source_path", "canonical_utf8_path", "canonical_base64_path", "expected_path"}
	mirrorKeys := []string{"expected_accept", "expected_digest", "expected_canonical_byte_length", "expected_reason_code"}
	casesVal, _ := manifest.Get("cases")
	cases := casesVal.(Array)

	entries := []manifestEntry{}
	seenIDs := map[string]struct{}{}
	for index, raw := range cases {
		entry, ok := raw.(*Object)
		if !ok {
			*errors = append(*errors, fmt.Sprintf("manifest cases[%d] must be an object", index))
			continue
		}
		caseID, hasID := StringOf(entry, "case_id")
		if !hasID || caseID == "" {
			*errors = append(*errors, fmt.Sprintf("manifest cases[%d] has invalid case_id", index))
			continue
		}
		if _, dup := seenIDs[caseID]; dup {
			*errors = append(*errors, fmt.Sprintf("duplicate case id in manifest: %s", caseID))
			continue
		}
		seenIDs[caseID] = struct{}{}
		category, _ := StringOf(entry, "category")
		if !containsString(categories, category) {
			*errors = append(*errors, fmt.Sprintf("case %s has unknown category %q (expected one of %s)", caseID, category, strings.Join(categories, "|")))
			continue
		}
		pathsOK := true
		for _, key := range requiredPaths {
			value, isStr := StringOf(entry, key)
			if !isStr || value == "" {
				*errors = append(*errors, fmt.Sprintf("case %s missing %s", caseID, key))
				pathsOK = false
			}
		}
		if !pathsOK {
			continue
		}
		next := manifestEntry{caseID: caseID, category: category, mirrors: map[string]Value{}}
		next.sourcePath, _ = StringOf(entry, "source_path")
		next.canonicalUTF8Path, _ = StringOf(entry, "canonical_utf8_path")
		next.canonicalBase64Path, _ = StringOf(entry, "canonical_base64_path")
		next.expectedPath, _ = StringOf(entry, "expected_path")
		for _, key := range requiredPaths {
			value, _ := StringOf(entry, key)
			resolved, safe := safeRelative(root, value)
			if !safe {
				*errors = append(*errors, fmt.Sprintf("case %s %s is not a safe relative path: %q", caseID, key, value))
				pathsOK = false
				continue
			}
			if info, err := os.Stat(resolved); err != nil || info.IsDir() {
				*errors = append(*errors, fmt.Sprintf("case %s %s does not exist: %s", caseID, key, value))
				pathsOK = false
			}
		}
		if !pathsOK {
			continue
		}
		expectedDir := categoryDirs[category]
		if !strings.HasPrefix(next.sourcePath, expectedDir+"/") {
			*errors = append(*errors, fmt.Sprintf("case %s category %q must live under %s/", caseID, category, expectedDir))
			continue
		}
		for _, key := range mirrorKeys {
			if value, present := entry.Get(key); present {
				next.mirrors[key] = value
			}
		}
		entries = append(entries, next)
	}

	// Corpus completeness: every case directory must be declared exactly once
	// and contain all four files (Contract §16.1/§16.2).
	declaredDirs := map[string]struct{}{}
	for i := range entries {
		parts := strings.Split(entries[i].sourcePath, "/")
		declaredDirs[strings.Join(parts[:len(parts)-1], "/")] = struct{}{}
	}
	for _, category := range categories {
		dirname := categoryDirs[category]
		base := filepath.Join(root, dirname)
		children, err := os.ReadDir(base)
		if err != nil {
			if entryOfCategory(entries, category) {
				*errors = append(*errors, fmt.Sprintf("category directory missing: %s/", dirname))
			}
			continue // no declared cases and no directory: partial corpus is fine
		}
		names := make([]string, 0, len(children))
		for _, child := range children {
			names = append(names, child.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			if !isDirNoSymlink(filepath.Join(base, name)) {
				continue
			}
			rel := dirname + "/" + name
			if _, declared := declaredDirs[rel]; !declared {
				*errors = append(*errors, fmt.Sprintf("undeclared case directory not in manifest: %s", rel))
				continue
			}
			for _, file := range caseFiles {
				if !fileExists(filepath.Join(base, name, file)) {
					*errors = append(*errors, fmt.Sprintf("case directory %s missing %s", rel, file))
				}
			}
		}
	}
	return entries
}

func entryOfCategory(entries []manifestEntry, category string) bool {
	for i := range entries {
		if entries[i].category == category {
			return true
		}
	}
	return false
}

func isDirNoSymlink(path string) bool {
	info, err := os.Stat(path) // follows symlinks; Lstat would treat symlinked dirs as files
	return err == nil && info.IsDir()
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func containsString(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// safeRelative resolves a manifest path under root, rejecting absolute
// paths, ".." components, backslashes and symlink escapes.
func safeRelative(root, relative string) (string, bool) {
	if filepath.IsAbs(relative) || strings.Contains(relative, "\\") {
		return "", false
	}
	for _, part := range strings.Split(relative, "/") {
		if part == ".." || part == "" {
			return "", false
		}
	}
	resolved := filepath.Join(root, relative)
	rootReal, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", false
	}
	resolvedReal, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(rootReal, resolvedReal)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return resolved, true
}

func evaluateEntry(root string, entry *manifestEntry) CaseResult {
	result := CaseResult{
		CaseID:              entry.caseID,
		Category:            entry.category,
		SourcePath:          entry.sourcePath,
		CanonicalUTF8Path:   entry.canonicalUTF8Path,
		CanonicalBase64Path: entry.canonicalBase64Path,
		ExpectedPath:        entry.expectedPath,
	}
	var problems []string

	sourceBytes, err := os.ReadFile(filepath.Join(root, entry.sourcePath))
	if err != nil {
		problems = append(problems, fmt.Sprintf("source.json unreadable: %v", err))
		result.Problems = problems
		return result
	}
	canonicalFileBytes, err := os.ReadFile(filepath.Join(root, entry.canonicalUTF8Path))
	if err != nil {
		problems = append(problems, fmt.Sprintf("canonical.utf8 unreadable: %v", err))
		result.Problems = problems
		return result
	}
	result.CanonicalFileBytes = canonicalFileBytes
	base64Text, err := os.ReadFile(filepath.Join(root, entry.canonicalBase64Path))
	if err != nil {
		problems = append(problems, fmt.Sprintf("canonical.base64 unreadable: %v", err))
		result.Problems = problems
		return result
	}

	// Base64 sidecar must decode to exactly the canonical.utf8 bytes (§16.1).
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(base64Text)))
	if err != nil {
		problems = append(problems, fmt.Sprintf("canonical.base64 is not valid Base64: %v", err))
	} else if !bytes.Equal(decoded, canonicalFileBytes) {
		problems = append(problems, "canonical.base64 does not decode to canonical.utf8 bytes")
	}
	if bytes.HasPrefix(canonicalFileBytes, utf8BOM) {
		problems = append(problems, "canonical.utf8 must not start with a UTF-8 BOM")
	}

	expectedBytes, err := os.ReadFile(filepath.Join(root, entry.expectedPath))
	if err != nil {
		problems = append(problems, fmt.Sprintf("expected.json unreadable: %v", err))
		result.Problems = problems
		return result
	}
	expectedValue, err := ParseJSON(expectedBytes)
	if err != nil {
		problems = append(problems, fmt.Sprintf("%s expected.json is not valid UTF-8 JSON: %v", entry.caseID, err))
		result.Problems = problems
		return result
	}
	expected, ok := expectedValue.(*Object)
	if !ok {
		problems = append(problems, fmt.Sprintf("%s: expected.json must be a JSON object", entry.caseID))
		result.Problems = problems
		return result
	}

	problems = append(problems, checkExpectedShape(entry, expected)...)
	for key, value := range entry.mirrors {
		if expectedValue2, present := expected.Get(key); present {
			if !jsonValueEqual(value, expectedValue2) {
				problems = append(problems, fmt.Sprintf("case %s: manifest mirror %s disagrees with expected.json", entry.caseID, key))
			}
		}
	}

	result.Derived = DeriveOutcome(sourceBytes, entry.category, canonicalFileBytes)

	problems = append(problems, checkDigestExpectations(expected, result.Derived, canonicalFileBytes, entry.caseID)...)
	problems = append(problems, checkOutcomeParity(expected, result.Derived, canonicalFileBytes, entry.caseID)...)

	result.Problems = problems
	result.OK = len(problems) == 0
	return result
}

// checkExpectedShape validates expected.json itself (corpus errors).
func checkExpectedShape(entry *manifestEntry, expected *Object) []string {
	var problems []string
	if sv, _ := StringOf(expected, "schema_version"); sv != expectedSchemaVersion {
		problems = append(problems, fmt.Sprintf("%s: expected schema_version must be %q", entry.caseID, expectedSchemaVersion))
	}
	if cid, _ := StringOf(expected, "case_id"); cid != entry.caseID {
		problems = append(problems, fmt.Sprintf("%s: expected case_id %q does not match manifest", entry.caseID, cid))
	}
	acceptVal, hasAccept := expected.Get("expected_accept")
	accept, acceptIsBool := acceptVal.(Bool)
	if !hasAccept || !acceptIsBool {
		problems = append(problems, fmt.Sprintf("%s: expected_accept must be a boolean", entry.caseID))
		return problems
	}
	reasonVal, hasReason := expected.Get("expected_reason_code")
	if !bool(accept) {
		reason, isStr := reasonVal.(String)
		if !hasReason || !isStr {
			problems = append(problems, fmt.Sprintf("%s: rejected case must carry expected_reason_code from the closed registry", entry.caseID))
			return problems
		}
		if _, known := allReasonCodes()[string(reason)]; !known {
			problems = append(problems, fmt.Sprintf("%s: rejected case must carry expected_reason_code from the closed registry", entry.caseID))
		}
	} else if hasReason {
		if _, isNull := reasonVal.(Null); !isNull {
			problems = append(problems, fmt.Sprintf("%s: accepted case must not carry expected_reason_code", entry.caseID))
		}
	}
	return problems
}

// checkDigestExpectations: digest/length expectations are present exactly
// when canonicalizable; for the digest-tamper fixture they describe the
// untampered canonicalization of the source.
func checkDigestExpectations(expected *Object, outcome Outcome, canonicalFileBytes []byte, caseID string) []string {
	var problems []string
	if outcome.Canonical != nil {
		expectedDigestVal, hasDigest := expected.Get("expected_digest")
		expectedDigest, isStr := expectedDigestVal.(String)
		if !hasDigest || !isStr {
			problems = append(problems, fmt.Sprintf("%s: canonicalizable case missing expected_digest", caseID))
		} else if string(expectedDigest) != outcome.Digest {
			problems = append(problems, fmt.Sprintf("%s: expected_digest %s != derived %s", caseID, string(expectedDigest), outcome.Digest))
		}
		lengthVal, hasLength := expected.Get("expected_canonical_byte_length")
		lengthNum, isNum := lengthVal.(Number)
		if !hasLength || !isNum || !lengthNum.IsInteger() {
			problems = append(problems, fmt.Sprintf("%s: canonicalizable case missing expected_canonical_byte_length", caseID))
		} else if bi, ok := lengthNum.Int(); !ok || bi.Int64() != int64(len(outcome.Canonical)) {
			problems = append(problems, fmt.Sprintf("%s: expected_canonical_byte_length %s != derived %d", caseID, string(lengthNum), len(outcome.Canonical)))
		}
	} else {
		if _, present := expected.Get("expected_digest"); present {
			problems = append(problems, fmt.Sprintf("%s: non-canonicalizable case must not declare digest/length", caseID))
		}
		if _, present := expected.Get("expected_canonical_byte_length"); present {
			problems = append(problems, fmt.Sprintf("%s: non-canonicalizable case must not declare digest/length", caseID))
		}
		if len(canonicalFileBytes) != 0 {
			problems = append(problems, fmt.Sprintf("%s: non-canonicalizable case must have empty canonical files", caseID))
		}
	}
	return problems
}

// checkOutcomeParity: derived accept/reject and reason must match the
// golden expectations.
func checkOutcomeParity(expected *Object, outcome Outcome, canonicalFileBytes []byte, caseID string) []string {
	var problems []string
	expectedAcceptVal, hasAccept := expected.Get("expected_accept")
	expectedAcceptBool, isBool := expectedAcceptVal.(Bool)
	if !hasAccept || !isBool {
		return problems // already reported by the shape check
	}
	expectedAccept := bool(expectedAcceptBool)
	if outcome.Accept != expectedAccept {
		problems = append(problems, fmt.Sprintf("%s: derived accept=%v (reason %s) but expected accept=%v", caseID, outcome.Accept, reasonOrDash(outcome), expectedAccept))
		return problems
	}
	if !outcome.Accept {
		expectedReason, _ := StringOf(expected, "expected_reason_code")
		if outcome.ReasonCode != expectedReason {
			problems = append(problems, fmt.Sprintf("%s: derived reason %s != expected reason %s", caseID, reasonOrDash(outcome), expectedReason))
		}
		// Except for the digest-tamper fixture itself, the canonical file
		// must still faithfully hold the canonicalization of the source.
		if outcome.ReasonCode != "DIGEST_MISMATCH" && outcome.Canonical != nil && !bytes.Equal(canonicalFileBytes, outcome.Canonical) {
			problems = append(problems, fmt.Sprintf("%s: canonical.utf8 bytes do not match canonicalized source", caseID))
		}
	}
	return problems
}

func reasonOrDash(outcome Outcome) string {
	if outcome.ReasonCode == "" {
		return "-"
	}
	return outcome.ReasonCode
}
