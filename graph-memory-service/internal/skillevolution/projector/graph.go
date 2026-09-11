// graph.go is the projection storage of the Runtime Graph: the derived
// read model, its private head/cursor/watermark state and the atomic
// projection-storage transaction port (GMS §8.5, §9.1; Contract §11.2).
//
// The Graph is a DERIVED store. The activation ledger and the active-head
// table remain the only runtime authority (Contract §5.3.1): every node and
// edge here is rebuildable from the authoritative sources, and nothing in
// this file may create, release, activate or deactivate a Skill.
package projector

import (
	"encoding/json"
	"sort"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// NodeKind is the closed Skill-domain node set plus the typed reference
// vertices checkpoints/evidence require when the underlying graph needs an
// endpoint (Contract §11.2, C2: a reference vertex keeps only the exact DTO
// ref, digest and type — never a body copy).
type NodeKind string

const (
	NodeRevision         NodeKind = "skill_revision"
	NodeBranch           NodeKind = "branch"
	NodeEvidenceVertex   NodeKind = "evidence_ref_vertex"
	NodeCheckpointVertex NodeKind = "checkpoint_ref_vertex"
)

// Lifecycle is the runtime lifecycle metadata of one revision node (C1,
// Contract §5.4/§11.5: nodes are retained after supersede/deactivate;
// deactivation only updates metadata).
type Lifecycle string

const (
	LifecycleActive      Lifecycle = "active"
	LifecycleSuperseded  Lifecycle = "superseded"
	LifecycleDeactivated Lifecycle = "deactivated"
)

// RevisionNode is one exact released Skill revision node (Contract §11.2:
// exact ref, kind/lineage/version, runtime lifecycle metadata, source
// projection sequence/schema).
type RevisionNode struct {
	Ref                     contract.SkillArtifactRef
	Lifecycle               Lifecycle
	FirstProjectedSequence  uint64
	ProjectionSchemaVersion string
}

// BranchNode is one revision-scoped branch node derived from the released
// canonical body (has_branch; GMS §9.2).
type BranchNode struct {
	Revision     contract.SkillArtifactRef
	BranchID     string
	BranchDigest string
}

// RefVertex is one typed reference vertex (evidence or checkpoint): exact
// DTO ref identity and digest only (C2).
type RefVertex struct {
	Type   string // "evidence" | "checkpoint"
	RefID  string
	Digest string
}

// NodeRef is the typed address of one Graph vertex; its canonical String is
// the map key and the endpoint identity inside edge documents.
type NodeRef struct {
	Kind     NodeKind
	Revision contract.SkillArtifactRef // revision / branch owner
	BranchID string                    // branch nodes
	RefType  string                    // reference vertices: "evidence"|"checkpoint"
	RefID    string                    // reference vertices: DTO id
	Digest   string                    // reference vertices: DTO digest
}

// RevisionRef returns the NodeRef of one exact revision node.
func RevisionRef(ref contract.SkillArtifactRef) NodeRef {
	return NodeRef{Kind: NodeRevision, Revision: ref}
}

// BranchRef returns the NodeRef of one revision-scoped branch node.
func BranchRef(revision contract.SkillArtifactRef, branchID string) NodeRef {
	return NodeRef{Kind: NodeBranch, Revision: revision, BranchID: branchID}
}

// EvidenceVertexRef returns the NodeRef of one typed evidence vertex.
func EvidenceVertexRef(evidenceID, digest string) NodeRef {
	return NodeRef{Kind: NodeEvidenceVertex, RefType: "evidence", RefID: evidenceID, Digest: digest}
}

// String renders the canonical endpoint identity.
func (n NodeRef) String() string {
	switch n.Kind {
	case NodeRevision:
		return "skill:" + n.Revision.LineageID + ":" + n.Revision.Version + ":" + n.Revision.ArtifactDigest
	case NodeBranch:
		return "branch:" + n.Revision.LineageID + ":" + n.Revision.Version + ":" + n.Revision.ArtifactDigest + ":" + n.BranchID
	case NodeEvidenceVertex:
		return "evidence-ref:" + n.RefID + ":" + n.Digest
	case NodeCheckpointVertex:
		return "checkpoint-ref:" + n.RefID + ":" + n.Digest
	}
	return "unknown:"
}

// SourceRef is the exact authority record a projection mutation came from.
// Every non-activation edge keeps it for audit reconstruction (GMS §8.4).
type SourceRef struct {
	Source   string // SourceActivation | SourceSimilarity | SourceEvidenceAssessment
	Stream   string // ledger stream ("" for the single activation stream)
	Sequence uint64
	RecordID string
	Digest   string
}

// Edge is one typed relation of the closed nine-relation set (Contract
// §11.3; rules.go freezes the mapping). Edge identity is
// (relation, endpoints, source record), so a re-projected identical record
// is an idempotent no-op while a new assessment record appends a new edge
// version instead of rewriting the old judgment (GMS §9.5).
type Edge struct {
	Relation string
	From     NodeRef
	To       NodeRef
	Source   SourceRef
}

// edgeKey is the deterministic edge map key.
func edgeKey(e Edge) string {
	return e.Relation + "\x1f" + e.From.String() + "\x1f" + e.To.String() + "\x1f" +
		e.Source.Source + "\x1f" + e.Source.Stream + "\x1f" + ulong(e.Source.Sequence)
}

// Mutation is one staged Graph write. The closed set lives below; the
// projector rules are the only constructors.
type Mutation interface{ mutation() }

type upsertRevisionNode struct{ Node RevisionNode }

type setLifecycle struct {
	Revision  contract.SkillArtifactRef
	Lifecycle Lifecycle
}

type upsertBranchNode struct{ Node BranchNode }

type upsertRefVertex struct{ Vertex RefVertex }

type upsertEdge struct{ Edge Edge }

type setLineageActive struct {
	LineageID string
	Ref       contract.SkillArtifactRef
}

type clearLineageActive struct {
	LineageID string
	Ref       contract.SkillArtifactRef
}

type recordSourceDigest struct {
	Source   string
	Sequence uint64
	Digest   string
}

func (upsertRevisionNode) mutation() {}
func (setLifecycle) mutation()       {}
func (upsertBranchNode) mutation()   {}
func (upsertRefVertex) mutation()    {}
func (upsertEdge) mutation()         {}
func (setLineageActive) mutation()   {}
func (clearLineageActive) mutation() {}
func (recordSourceDigest) mutation() {}

// graphState is the full mutable state of one projection head: the derived
// Graph itself plus the private head/cursor/watermark bookkeeping. It is
// always handled as a value and swapped atomically on commit.
type graphState struct {
	headSeq    uint64 // private monotone CAS counter (not public watermark data)
	headDigest string

	state    string
	cursors  CursorVector
	prefixes PrefixState

	watermark       map[string]any
	watermarkDigest string
	priorWatermark  map[string]any // read-only old head after a rebuild (GMS §8.6)

	blocked *BlockedInfo

	revisions     map[string]RevisionNode
	branches      map[string]BranchNode
	vertices      map[string]RefVertex
	edges         map[string]Edge
	lineageActive map[string]contract.SkillArtifactRef

	consumed map[string]map[uint64]string // source → sequence → record digest
}

func newGraphState() *graphState {
	return &graphState{
		state:         StateUninitialized,
		revisions:     map[string]RevisionNode{},
		branches:      map[string]BranchNode{},
		vertices:      map[string]RefVertex{},
		edges:         map[string]Edge{},
		lineageActive: map[string]contract.SkillArtifactRef{},
		consumed:      map[string]map[uint64]string{},
	}
}

func (g *graphState) clone() *graphState {
	out := newGraphState()
	out.headSeq, out.headDigest = g.headSeq, g.headDigest
	out.state, out.cursors, out.prefixes = g.state, g.cursors, g.prefixes
	out.watermark = deepCopyDoc(g.watermark)
	out.watermarkDigest = g.watermarkDigest
	out.priorWatermark = deepCopyDoc(g.priorWatermark)
	if g.blocked != nil {
		blocked := *g.blocked
		out.blocked = &blocked
	}
	for k, v := range g.revisions {
		out.revisions[k] = v
	}
	for k, v := range g.branches {
		out.branches[k] = v
	}
	for k, v := range g.vertices {
		out.vertices[k] = v
	}
	for k, v := range g.edges {
		out.edges[k] = v
	}
	for k, v := range g.lineageActive {
		out.lineageActive[k] = v
	}
	for source, index := range g.consumed {
		copied := make(map[uint64]string, len(index))
		for seq, digest := range index {
			copied[seq] = digest
		}
		out.consumed[source] = copied
	}
	return out
}

// apply executes one staged mutation against the in-flight state. Apply
// happens on a private clone inside the projection-storage transaction, so
// a mid-apply failure leaves the committed head untouched.
func (g *graphState) apply(m Mutation) {
	switch mut := m.(type) {
	case upsertRevisionNode:
		g.revisions[RevisionRef(mut.Node.Ref).String()] = mut.Node
	case setLifecycle:
		key := RevisionRef(mut.Revision).String()
		if node, ok := g.revisions[key]; ok {
			node.Lifecycle = mut.Lifecycle
			g.revisions[key] = node
		}
	case upsertBranchNode:
		g.branches[BranchRef(mut.Node.Revision, mut.Node.BranchID).String()] = mut.Node
	case upsertRefVertex:
		g.vertices[NodeRef{Kind: vertexKindOf(mut.Vertex.Type), RefType: mut.Vertex.Type, RefID: mut.Vertex.RefID, Digest: mut.Vertex.Digest}.String()] = mut.Vertex
	case upsertEdge:
		g.edges[edgeKey(mut.Edge)] = mut.Edge
	case setLineageActive:
		g.lineageActive[mut.LineageID] = mut.Ref
	case clearLineageActive:
		if current, ok := g.lineageActive[mut.LineageID]; ok && current == mut.Ref {
			delete(g.lineageActive, mut.LineageID)
		}
	case recordSourceDigest:
		if g.consumed[mut.Source] == nil {
			g.consumed[mut.Source] = map[uint64]string{}
		}
		g.consumed[mut.Source][mut.Sequence] = mut.Digest
	}
}

func vertexKindOf(refType string) NodeKind {
	if refType == "checkpoint" {
		return NodeCheckpointVertex
	}
	return NodeEvidenceVertex
}

// ---------------------------------------------------------------------------
// The projection-storage transaction port (GMS §8.5)
// ---------------------------------------------------------------------------

// CommitUnit is ONE projection-storage transaction: the Graph mutations,
// the complete new cursor vector and prefix commitments, the projection
// head CAS (expected → new watermark digest) and the new watermark
// document. The unit commits atomically or not at all; Graph mutation
// failure means the watermark does not advance, and an advanced watermark
// certifies the corresponding mutation is visible.
type CommitUnit struct {
	ExpectedHeadSeq    uint64
	ExpectedHeadDigest string
	Mutations          []Mutation
	Cursors            CursorVector
	Prefixes           PrefixState
	State              string
	Watermark          map[string]any
	WatermarkDigest    string
	// RebuildSwap, when non-nil, replaces the whole derived state (Rebuild,
	// GMS §8.6): the unit still CASes the same head, so a concurrent writer
	// that moved the head discards the rebuild.
	RebuildSwap *graphState
	// PriorWatermark keeps the old head's watermark readable after a
	// rebuild swap (GMS §8.6: the old head may serve read-only traffic and
	// MUST expose its old watermark).
	PriorWatermark map[string]any
	// Blocked freezes the blocking reason of a state-only blocked commit
	// (nil on every advancing commit; the blocked info clears on the next
	// successful projection).
	Blocked *BlockedInfo
}

// Storage is the projection-storage transaction port. The default adapter
// (GraphStorage) coordinates the ledger HeadProjection CAS with the Graph
// swap under one lock; a durable adapter replaces it with one ACID
// transaction over the same vocabulary.
type Storage interface {
	CommitProjection(unit *CommitUnit) error
}

// Graph is the handle to the Runtime projection storage.
type Graph struct {
	mu    sync.Mutex
	state *graphState
}

// NewGraph returns an uninitialized Runtime projection storage (watermark
// absent, state uninitialized, cursor vector zero — Contract §9.3).
func NewGraph() *Graph {
	return &Graph{state: newGraphState()}
}

// GraphStorage is the default in-memory Storage adapter: it serializes the
// head CAS (ledger HeadProjection head, keyed by stream) with the Graph
// state swap under the Graph mutex. NOT PRODUCTION DURABILITY (same caveat
// as the ledger memory adapter); a durable adapter MUST provide one ACID
// transaction for the whole unit.
type GraphStorage struct {
	graph *Graph
	store ledger.Store // HeadProjection CAS target
	// testApplyHook, when set, runs after the unit is staged on the private
	// clone but BEFORE the head CAS: a non-nil error simulates a
	// mid-transaction failure and MUST leave neither the head nor the Graph
	// advanced (the head CAS is the irreversible commit point). In-package
	// test seam only.
	testApplyHook func() error
}

// NewGraphStorage wires the default storage adapter.
func NewGraphStorage(graph *Graph, store ledger.Store) (*GraphStorage, error) {
	if graph == nil || store == nil {
		return nil, errNilDependency
	}
	return &GraphStorage{graph: graph, store: store}, nil
}

// CommitProjection applies one unit atomically:
//
//  1. under the Graph lock, verify the expected head matches the committed
//     head (PROJECTION_HEAD_CONFLICT discards the unit otherwise — the
//     caller recomputes from the new head, never overwriting concurrent
//     progress, GMS §8.5);
//  2. stage the whole unit on a private clone (mutations, or the rebuild
//     swap state) — every failure-prone step happens BEFORE any
//     irreversible write, because the ledger HeadProjection CAS is
//     forward-only by contract (GMS §2.1) and cannot be compensated;
//  3. CAS the ledger HeadProjection head to the new watermark digest —
//     the commit point;
//  4. swap the staged state in (a pointer assignment that cannot fail).
//
// A failure at any step therefore leaves no head advance, no mutation, no
// cursor and no watermark.
func (g *GraphStorage) CommitProjection(unit *CommitUnit) error {
	if unit == nil {
		return errNilDependency
	}
	g.graph.mu.Lock()
	defer g.graph.mu.Unlock()

	current := g.graph.state
	if current.headSeq != unit.ExpectedHeadSeq || current.headDigest != unit.ExpectedHeadDigest {
		return newError(nil, ReasonProjectionHeadConflict,
			"projection head is (%d, %s) but the unit expects (%d, %s); discarding this result — recompute from the new head (GMS §8.5)",
			current.headSeq, current.headDigest, unit.ExpectedHeadSeq, unit.ExpectedHeadDigest)
	}
	newHeadSeq := current.headSeq + 1

	// Stage on a private clone; nothing below may touch the committed state.
	var staged *graphState
	if unit.RebuildSwap != nil {
		staged = unit.RebuildSwap.clone()
		staged.priorWatermark = deepCopyDoc(unit.PriorWatermark)
	} else {
		staged = current.clone()
		for _, mutation := range unit.Mutations {
			staged.apply(mutation)
		}
	}
	staged.headSeq, staged.headDigest = newHeadSeq, unit.WatermarkDigest
	staged.state = unit.State
	staged.cursors = unit.Cursors
	staged.prefixes = unit.Prefixes
	staged.watermark = deepCopyDoc(unit.Watermark)
	staged.watermarkDigest = unit.WatermarkDigest
	if unit.State == StateBlocked && unit.Blocked != nil {
		blocked := *unit.Blocked
		staged.blocked = &blocked
	} else {
		staged.blocked = nil
	}
	// In-package test seam: a failure here simulates a mid-transaction
	// crash AFTER the unit was staged but BEFORE the head CAS — nothing is
	// committed anywhere.
	if g.testApplyHook != nil {
		if err := g.testApplyHook(); err != nil {
			return err
		}
	}
	if err := g.store.CompareAndSwap(ledger.HeadProjection, StreamRuntime,
		unit.ExpectedHeadSeq, unit.ExpectedHeadDigest, newHeadSeq, unit.WatermarkDigest); err != nil {
		return err
	}
	// Commit point passed: publish the staged state.
	g.graph.state = staged
	return nil
}

// ---------------------------------------------------------------------------
// Read side (deep copies only; the Graph is never mutated through reads)
// ---------------------------------------------------------------------------

// GraphSnapshot is a deep copy of the derived Runtime Graph.
type GraphSnapshot struct {
	State         string
	Cursors       CursorVector
	Revisions     map[string]RevisionNode
	Branches      map[string]BranchNode
	Vertices      map[string]RefVertex
	Edges         map[string]Edge
	LineageActive map[string]contract.SkillArtifactRef
}

// Snapshot returns a deep copy of the current derived state.
func (g *Graph) Snapshot() GraphSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.state
	out := GraphSnapshot{
		State:         s.state,
		Cursors:       s.cursors,
		Revisions:     map[string]RevisionNode{},
		Branches:      map[string]BranchNode{},
		Vertices:      map[string]RefVertex{},
		Edges:         map[string]Edge{},
		LineageActive: map[string]contract.SkillArtifactRef{},
	}
	for k, v := range s.revisions {
		out.Revisions[k] = v
	}
	for k, v := range s.branches {
		out.Branches[k] = v
	}
	for k, v := range s.vertices {
		out.Vertices[k] = v
	}
	for k, v := range s.edges {
		out.Edges[k] = v
	}
	for k, v := range s.lineageActive {
		out.LineageActive[k] = v
	}
	return out
}

// head returns the committed private head (sequence, digest).
func (g *Graph) head() (uint64, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.headSeq, g.state.headDigest
}

// stateOf returns a deep copy of the whole internal state (rebuild seeding
// and tests).
func (g *Graph) stateOf() *graphState {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.clone()
}

// GraphDigest renders the canonical digest of the derived content: sorted
// canonical node/edge/lineage documents. A from-zero rebuild MUST produce
// the identical digest (GMS §8.6, completion criterion "从零重建相同").
func (g *Graph) GraphDigest() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.contentDigest()
}

func (g *graphState) contentDigest() string {
	nodes := make([]any, 0, len(g.revisions)+len(g.branches)+len(g.vertices))
	for _, node := range g.revisions {
		nodes = append(nodes, map[string]any{
			"node":                     RevisionRef(node.Ref).String(),
			"kind":                     node.Ref.Kind,
			"lifecycle":                string(node.Lifecycle),
			"first_projected_sequence": json.Number(ulong(node.FirstProjectedSequence)),
			"projection_schema":        node.ProjectionSchemaVersion,
		})
	}
	for _, branch := range g.branches {
		nodes = append(nodes, map[string]any{
			"node":          BranchRef(branch.Revision, branch.BranchID).String(),
			"branch_digest": branch.BranchDigest,
		})
	}
	for _, vertex := range g.vertices {
		nodes = append(nodes, map[string]any{
			"node": NodeRef{Kind: vertexKindOf(vertex.Type), RefType: vertex.Type, RefID: vertex.RefID, Digest: vertex.Digest}.String(),
		})
	}
	edgeDocs := make([]any, 0, len(g.edges))
	for _, edge := range g.edges {
		edgeDocs = append(edgeDocs, map[string]any{
			"relation": edge.Relation,
			"from":     edge.From.String(),
			"to":       edge.To.String(),
			"source": map[string]any{
				"source":    edge.Source.Source,
				"stream":    edge.Source.Stream,
				"sequence":  json.Number(ulong(edge.Source.Sequence)),
				"record_id": edge.Source.RecordID,
				"digest":    edge.Source.Digest,
			},
		})
	}
	lineages := make([]any, 0, len(g.lineageActive))
	for lineage, ref := range g.lineageActive {
		lineages = append(lineages, map[string]any{
			"lineage_id":     lineage,
			"active_version": json.Number(ref.Version),
			"active_digest":  ref.ArtifactDigest,
		})
	}
	doc := map[string]any{"nodes": sortedCanonical(nodes), "edges": sortedCanonical(edgeDocs), "lineage_active": sortedCanonical(lineages)}
	digest, err := contract.DigestOf(doc)
	if err != nil {
		return ""
	}
	return digest
}

// sortedCanonical renders each element to its canonical key and sorts by
// it, making the digest independent of map iteration order.
func sortedCanonical(items []any) []any {
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = contract.CanonicalKey(item)
	}
	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return keys[order[a]] < keys[order[b]] })
	out := make([]any, len(items))
	for i, idx := range order {
		out[i] = items[idx]
	}
	return out
}

func deepCopyDoc(doc map[string]any) map[string]any {
	if doc == nil {
		return nil
	}
	out := make(map[string]any, len(doc))
	for k, v := range doc {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopyDoc(t)
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}
