// rank.go is the deterministic lexical+graph integer ranker (GMS §10.7,
// policy gms.ranker.lexical-graph.v1). Scores are pure integer micros; the
// order is one frozen comparator chain (total desc, graph distance asc,
// exact-ref canonical order asc) — no map iteration order ever reaches a
// response, so the same input, projection head, policy and fences always
// produce the same order (completion criterion 同序).
package retrieval

import (
	"fmt"
	"sort"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
)

// Frozen integer micros of the lexical-graph ranker v1.
const (
	titleTermMicros       = int64(600000)
	descriptionTermMicros = int64(300000)
	bodyTermMicros        = int64(50000)

	graphDistanceCap = 2
	graphStepMicros  = int64(100000) // (cap+1-distance) * step, distance <= cap
	unreachableDist  = graphDistanceCap + 1
)

// skillCandidate is one ranked active skill revision.
type skillCandidate struct {
	ref         contract.SkillArtifactRef
	node        projector.RevisionNode
	title       string
	description string
	bodyText    string
	envelope    map[string]any // parsed canonical envelope (renderer input)
	refs        []contract.EvidenceRef
	lexical     int64
	graph       int64
	total       int64
	distance    int
}

// evidenceCandidate is one ranked committed evidence ref.
type evidenceCandidate struct {
	ref      contract.EvidenceRef
	vertex   projector.RefVertex
	claim    string
	lexical  int64
	graph    int64
	total    int64
	distance int
}

// rankScores scores the skill/evidence candidates of one snapshot for one
// query deterministically.
//
// Lexical micros: one hit per unique query term and field class (title,
// description, body). Graph micros: (cap+1-distance) * step over the
// undirected Runtime Graph, evidence vertices anchoring skill revisions and
// lexically matched revisions anchoring evidence (the §10.7 graph boost).
func rankScores(skills []*skillCandidate, evidence []*evidenceCandidate, terms []string, snapshot projector.GraphSnapshot) {
	termSet := map[string]bool{}
	for _, term := range terms {
		termSet[term] = true
	}
	for _, skill := range skills {
		skill.lexical = lexicalMicros(termSet, skill.title, skill.description, skill.bodyText)
	}
	for _, ev := range evidence {
		ev.lexical = evidenceLexicalMicros(termSet, ev.ref)
	}

	// Undirected adjacency over the snapshot (BFS handles direction).
	adjacency := map[string]map[string]bool{}
	addEdge := func(from, to projector.NodeRef) {
		if adjacency[from.String()] == nil {
			adjacency[from.String()] = map[string]bool{}
		}
		if adjacency[to.String()] == nil {
			adjacency[to.String()] = map[string]bool{}
		}
		adjacency[from.String()][to.String()] = true
		adjacency[to.String()][from.String()] = true
	}
	// Deterministic node registration first (revisions, branches, vertices),
	// then edges; BFS over map neighbors is order-independent for distance.
	for key, node := range snapshot.Revisions {
		_ = key
		if adjacency[projector.RevisionRef(node.Ref).String()] == nil {
			adjacency[projector.RevisionRef(node.Ref).String()] = map[string]bool{}
		}
	}
	for _, node := range snapshot.Branches {
		if adjacency[projector.BranchRef(node.Revision, node.BranchID).String()] == nil {
			adjacency[projector.BranchRef(node.Revision, node.BranchID).String()] = map[string]bool{}
		}
	}
	for _, vertex := range snapshot.Vertices {
		ref := vertexNodeRef(vertex)
		if adjacency[ref.String()] == nil {
			adjacency[ref.String()] = map[string]bool{}
		}
	}
	for _, edge := range snapshot.Edges {
		addEdge(edge.From, edge.To)
	}

	// Anchors: evidence vertices anchor skill candidates; lexically matched
	// revisions anchor evidence candidates.
	evidenceAnchors := make([]string, 0, len(snapshot.Vertices))
	for _, vertex := range snapshot.Vertices {
		if vertex.Type == "evidence" {
			evidenceAnchors = append(evidenceAnchors, vertexNodeRef(vertex).String())
		}
	}
	sort.Strings(evidenceAnchors)
	matchedRevisions := make([]string, 0, len(skills))
	for _, skill := range skills {
		if skill.lexical > 0 {
			matchedRevisions = append(matchedRevisions, projector.RevisionRef(skill.ref).String())
		}
	}
	sort.Strings(matchedRevisions)

	distances := func(anchorSet []string, target string) int {
		best := unreachableDist
		for _, anchor := range anchorSet {
			if anchor == target {
				return 0
			}
			d := bfsDistance(adjacency, anchor, target, graphDistanceCap)
			if d < best {
				best = d
			}
		}
		return best
	}
	for _, skill := range skills {
		skill.distance = distances(evidenceAnchors, projector.RevisionRef(skill.ref).String())
		if skill.distance <= graphDistanceCap {
			skill.graph = int64(graphDistanceCap+1-skill.distance) * graphStepMicros
		}
		skill.total = skill.lexical + skill.graph
	}
	for _, ev := range evidence {
		ev.distance = distances(matchedRevisions, vertexNodeRef(ev.vertex).String())
		if ev.distance <= graphDistanceCap {
			ev.graph = int64(graphDistanceCap+1-ev.distance) * graphStepMicros
		}
		ev.total = ev.lexical + ev.graph
	}
}

// lexicalMicros counts one hit per unique term and field class.
func lexicalMicros(termSet map[string]bool, title, description, body string) int64 {
	var score int64
	for term := range termSet {
		if containsTerm(title, term) {
			score += titleTermMicros
		}
		if containsTerm(description, term) {
			score += descriptionTermMicros
		}
		if containsTerm(body, term) {
			score += bodyTermMicros
		}
	}
	return score
}

func evidenceLexicalMicros(termSet map[string]bool, ref contract.EvidenceRef) int64 {
	var score int64
	text := ref.EvidenceID + " " + ref.EvidenceKind
	for term := range termSet {
		if containsTerm(text, term) {
			score += descriptionTermMicros
		}
	}
	return score
}

// containsTerm reports whether the term appears as a whole word.
func containsTerm(text, term string) bool {
	if text == "" || term == "" {
		return false
	}
	runes := []rune(text)
	n := len([]rune(term))
	m := len(runes)
	for i := 0; i+n <= m; i++ {
		if string(runes[i:i+n]) != term {
			continue
		}
		before := i == 0 || isTermSeparator(runes[i-1])
		after := i+n == m || isTermSeparator(runes[i+n])
		if before && after {
			return true
		}
	}
	return false
}

func isTermSeparator(r rune) bool {
	return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9')
}

// bfsDistance returns the hop distance between two nodes (cap-bounded,
// unreachableDist when beyond the cap).
func bfsDistance(adjacency map[string]map[string]bool, from, to string, cap int) int {
	if from == to {
		return 0
	}
	type queued struct {
		node string
		dist int
	}
	visited := map[string]bool{from: true}
	queue := []queued{{node: from, dist: 0}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.dist >= cap {
			continue
		}
		// Deterministic neighbor order (sorted) — BFS distance is
		// order-independent, but every traversal here stays reproducible.
		neighbors := make([]string, 0, len(adjacency[current.node]))
		for neighbor := range adjacency[current.node] {
			neighbors = append(neighbors, neighbor)
		}
		sort.Strings(neighbors)
		for _, neighbor := range neighbors {
			if neighbor == to {
				return current.dist + 1
			}
			if visited[neighbor] {
				continue
			}
			visited[neighbor] = true
			queue = append(queue, queued{node: neighbor, dist: current.dist + 1})
		}
	}
	return unreachableDist
}

func vertexNodeRef(vertex projector.RefVertex) projector.NodeRef {
	if vertex.Type == "checkpoint" {
		return projector.NodeRef{Kind: projector.NodeCheckpointVertex, RefType: "checkpoint", RefID: vertex.RefID, Digest: vertex.Digest}
	}
	return projector.NodeRef{Kind: projector.NodeEvidenceVertex, RefType: "evidence", RefID: vertex.RefID, Digest: vertex.Digest}
}

// rankOrderSkills sorts skill candidates by the frozen comparator chain.
func rankOrderSkills(skills []*skillCandidate) {
	sort.SliceStable(skills, func(i, j int) bool {
		a, b := skills[i], skills[j]
		if a.total != b.total {
			return a.total > b.total
		}
		if a.distance != b.distance {
			return a.distance < b.distance
		}
		if a.ref.LineageID != b.ref.LineageID {
			return a.ref.LineageID < b.ref.LineageID
		}
		if a.ref.Version != b.ref.Version {
			return versionLess(a.ref.Version, b.ref.Version)
		}
		return a.ref.ArtifactDigest < b.ref.ArtifactDigest
	})
}

// rankOrderEvidence sorts evidence candidates by total desc then the exact
// ref canonical order.
func rankOrderEvidence(evidence []*evidenceCandidate) {
	sort.SliceStable(evidence, func(i, j int) bool {
		a, b := evidence[i], evidence[j]
		if a.total != b.total {
			return a.total > b.total
		}
		if a.distance != b.distance {
			return a.distance < b.distance
		}
		return contract.CanonicalKey(evidenceRefDoc(a.ref)) < contract.CanonicalKey(evidenceRefDoc(b.ref))
	})
}

// versionLess compares decimal version strings numerically.
func versionLess(a, b string) bool {
	ai, aok := parseVersionInt(a)
	bi, bok := parseVersionInt(b)
	if aok == nil && bok == nil {
		return ai < bi
	}
	return a < b
}

// allocation is the budget walk state of one result page.
type allocation struct {
	totalCap       int64
	evidenceSubcap int64
	skillSubcap    int64
	tokenBudget    int64

	usedTotal    int64
	usedEvidence int64
	usedSkills   int64
	usedTokens   int64

	servedEvidence []*evidenceCandidate
	servedSkills   []*skillCandidate
	omissions      []omission
	codes          []string
}

// omission is one typed omitted-ref carrier entry (Contract §12.7.2 C1).
type omission struct {
	kind       string // "evidence" | "skill"
	ref        map[string]any
	reasonCode string
}

func (a *allocation) omit(kind string, ref map[string]any, reason string) {
	a.omissions = append(a.omissions, omission{kind: kind, ref: ref, reasonCode: reason})
}

// take allocates one candidate; served is false when a cap forced a typed
// omission (never a silent drop).
func (a *allocation) takeEvidence(candidate *evidenceCandidate) bool {
	if a.usedTotal >= a.totalCap {
		a.omit("evidence", evidenceRefDoc(candidate.ref), CodeTotalCapReached)
		return false
	}
	if a.usedEvidence >= a.evidenceSubcap {
		a.omit("evidence", evidenceRefDoc(candidate.ref), CodeEvidenceSubcapReached)
		return false
	}
	a.usedTotal++
	a.usedEvidence++
	a.servedEvidence = append(a.servedEvidence, candidate)
	return true
}

func (a *allocation) takeSkill(candidate *skillCandidate, tokens int64) bool {
	if a.usedTotal >= a.totalCap {
		a.omit("skill", skillRefDoc(candidate.ref), CodeTotalCapReached)
		return false
	}
	if a.usedSkills >= a.skillSubcap {
		a.omit("skill", skillRefDoc(candidate.ref), CodeSkillSubcapReached)
		return false
	}
	if a.usedTokens+tokens > a.tokenBudget {
		a.omit("skill", skillRefDoc(candidate.ref), CodeGuidanceTokenBudgetReached)
		return false
	}
	a.usedTotal++
	a.usedSkills++
	a.usedTokens += tokens
	a.servedSkills = append(a.servedSkills, candidate)
	return true
}

// finalizeOmissions sorts, dedupes and derives the truncation codes
// (order-preserving dedupe of the entry reason codes).
func (a *allocation) finalizeOmissions() {
	sort.SliceStable(a.omissions, func(i, j int) bool {
		if a.omissions[i].kind != a.omissions[j].kind {
			return a.omissions[i].kind < a.omissions[j].kind
		}
		return contract.CanonicalKey(a.omissions[i].ref) < contract.CanonicalKey(a.omissions[j].ref)
	})
	deduped := a.omissions[:0]
	seenRef := map[string]bool{}
	for _, entry := range a.omissions {
		key := entry.kind + "\x1f" + contract.CanonicalKey(entry.ref)
		if seenRef[key] {
			continue
		}
		seenRef[key] = true
		deduped = append(deduped, entry)
	}
	a.omissions = deduped
	seenCode := map[string]bool{}
	a.codes = a.codes[:0]
	for _, entry := range a.omissions {
		if !seenCode[entry.reasonCode] {
			seenCode[entry.reasonCode] = true
			a.codes = append(a.codes, entry.reasonCode)
		}
	}
}

// omissionDocs renders the carrier entries in canonical order.
func (a *allocation) omissionDocs() []any {
	docs := make([]any, 0, len(a.omissions))
	for _, entry := range a.omissions {
		docs = append(docs, map[string]any{
			"kind":        entry.kind,
			"ref":         entry.ref,
			"reason_code": entry.reasonCode,
		})
	}
	return docs
}

// truncationCodes renders the order-preserving deduped codes.
func (a *allocation) truncationCodes() []any {
	return stringsToAny(a.codes)
}

// servedFenceDigest freezes one cumulative served-set fence: SHA-256 over
// the JCS of the closed fence document (refs canonically sorted).
func servedFenceDigest(sessionID, kind string, refDocs []map[string]any) string {
	keys := make([]string, 0, len(refDocs))
	for _, doc := range refDocs {
		keys = append(keys, contract.CanonicalKey(doc))
	}
	sort.Strings(keys)
	sorted := make([]any, 0, len(keys))
	for _, key := range keys {
		sorted = append(sorted, key)
	}
	digest, err := contract.DigestOf(map[string]any{
		"schema_version":     "gms.served-fence.v1",
		"explore_session_id": sessionID,
		"kind":               kind,
		"refs":               sorted,
	})
	if err != nil {
		return contract.DigestBytes([]byte(fmt.Sprintf("fence-uncanonicalizable:%s:%s", sessionID, kind)))
	}
	return digest
}
