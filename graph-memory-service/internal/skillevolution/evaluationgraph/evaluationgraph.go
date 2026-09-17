// Package evaluationgraph builds a non-authoritative, non-active read view for
// an evaluation's consolidated advisory proposals. It never writes a ledger or
// exposes an executable closure: canonical records remain the only authority.
package evaluationgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

const StreamEvaluation = "evaluation"

var (
	ErrInvalidCanonicalInput = errors.New("evaluation graph: invalid canonical input")
	ErrPinMismatch           = errors.New("evaluation graph: projection pin mismatch")
	ErrHeldOutFeedbackInput  = errors.New("evaluation graph: held-out feedback is not projection input")
	ErrScopeDenied           = errors.New("evaluation graph: scope is not permitted")
	ErrRevisionNotFound      = errors.New("evaluation graph: exact revision not found")
)

type Scope string

const (
	ScopeEvaluation Scope = StreamEvaluation
	ScopeRuntime    Scope = "runtime"
)

type RecordKind string

const (
	RecordProposal       RecordKind = "advisory_proposal"
	RecordConsolidation  RecordKind = "consolidation"
	RecordRelation       RecordKind = "relation"
	RecordHeldOutFeedback RecordKind = "held_out_feedback"
)

type RevisionRef struct {
	LineageID string `json:"lineage_id"`
	Revision  uint64 `json:"revision"`
}

func (r RevisionRef) String() string {
	return fmt.Sprintf("skill://%s/%s@%d", StreamEvaluation, r.LineageID, r.Revision)
}

type Proposal struct {
	Ref      RevisionRef `json:"ref"`
	Body     string      `json:"body"`
	Advisory bool        `json:"advisory"`
}

type Consolidation struct {
	Target      RevisionRef `json:"target"`
	Disposition string      `json:"disposition"`
}

type Relation struct {
	From RevisionRef `json:"from"`
	To   RevisionRef `json:"to"`
	Kind string      `json:"kind"`
}

// CanonicalRecord is an immutable input copied from the authoritative skill
// evolution records. Digest covers all fields other than itself.
type CanonicalRecord struct {
	Sequence      uint64         `json:"sequence"`
	ID            string         `json:"id"`
	Kind          RecordKind     `json:"kind"`
	Proposal      *Proposal      `json:"proposal,omitempty"`
	Consolidation *Consolidation `json:"consolidation,omitempty"`
	Relation      *Relation      `json:"relation,omitempty"`
	Digest        string         `json:"digest"`
}

type Pin struct {
	Stream          string `json:"stream"`
	LedgerRevision  uint64 `json:"ledger_revision"`
	LedgerDigest    string `json:"ledger_digest"`
	Watermark       string `json:"watermark"`
	WatermarkDigest string `json:"watermark_digest"`
}

type CanonicalFixture struct {
	Pin     Pin               `json:"pin"`
	Records []CanonicalRecord `json:"records"`
}

// NewFixture is a fixture-only convenience for sealing canonical records.
// Production callers must pass their independently committed records to Build.
func NewFixture(records []CanonicalRecord) (CanonicalFixture, error) {
	copied := cloneRecords(records)
	for i := range copied {
		if err := validateShape(copied[i]); err != nil {
			return CanonicalFixture{}, err
		}
		copied[i].Digest = recordDigest(copied[i])
	}
	ledgerDigest := digestJSON(copied)
	pin := Pin{
		Stream:         StreamEvaluation,
		LedgerRevision: uint64(len(copied)),
		LedgerDigest:   ledgerDigest,
	}
	pin.Watermark = watermark(pin.Stream, pin.LedgerRevision, pin.LedgerDigest)
	pin.WatermarkDigest = digestJSON(pin.Watermark)
	return CanonicalFixture{Pin: pin, Records: copied}, nil
}

type Node struct {
	Ref  RevisionRef `json:"ref"`
	Body string      `json:"body"`
}

type Edge struct {
	From     RevisionRef `json:"from"`
	To       RevisionRef `json:"to"`
	Kind     string      `json:"kind"`
	SourceID string      `json:"source_id"`
}

type View struct {
	Pin              Pin    `json:"pin"`
	ProjectionDigest string `json:"projection_digest"`
	Nodes            []Node `json:"nodes"`
	Edges            []Edge `json:"edges"`
}

// Build derives a view solely from pin-verified canonical records. It does not
// consult, mutate, or stand in for the Runtime projection or activation ledger.
func Build(fixture CanonicalFixture) (*View, error) {
	if err := verifyFixture(fixture); err != nil {
		return nil, err
	}

	proposals := make(map[RevisionRef]Proposal)
	consolidated := make(map[RevisionRef]bool)
	for _, record := range fixture.Records {
		switch record.Kind {
		case RecordProposal:
			proposals[record.Proposal.Ref] = *record.Proposal
		case RecordConsolidation:
			consolidated[record.Consolidation.Target] = true
		}
	}

	nodes := make([]Node, 0, len(proposals))
	for ref, proposal := range proposals {
		if consolidated[ref] {
			nodes = append(nodes, Node{Ref: ref, Body: proposal.Body})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Ref.String() < nodes[j].Ref.String() })

	included := make(map[RevisionRef]bool, len(nodes))
	for _, node := range nodes {
		included[node.Ref] = true
	}
	edges := make([]Edge, 0)
	for _, record := range fixture.Records {
		if record.Kind != RecordRelation {
			continue
		}
		relation := record.Relation
		if !included[relation.From] || !included[relation.To] {
			return nil, fmt.Errorf("%w: relation %q references an unconsolidated revision", ErrInvalidCanonicalInput, record.ID)
		}
		edges = append(edges, Edge{From: relation.From, To: relation.To, Kind: relation.Kind, SourceID: record.ID})
	}
	sort.Slice(edges, func(i, j int) bool {
		left, right := edges[i], edges[j]
		if left.From.String() != right.From.String() { return left.From.String() < right.From.String() }
		if left.To.String() != right.To.String() { return left.To.String() < right.To.String() }
		if left.Kind != right.Kind { return left.Kind < right.Kind }
		return left.SourceID < right.SourceID
	})

	view := &View{Pin: fixture.Pin, Nodes: nodes, Edges: edges}
	view.ProjectionDigest = digestJSON(struct {
		Pin   Pin    `json:"pin"`
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	}{Pin: view.Pin, Nodes: view.Nodes, Edges: view.Edges})
	return view, nil
}

func (v *View) Get(scope Scope, ref RevisionRef) (Node, error) {
	if scope != ScopeEvaluation {
		return Node{}, fmt.Errorf("%w: %s", ErrScopeDenied, scope)
	}
	for _, node := range v.Nodes {
		if node.Ref == ref {
			return node, nil
		}
	}
	return Node{}, fmt.Errorf("%w: %s", ErrRevisionNotFound, ref.String())
}

// Explore performs deterministic breadth-first traversal of the exact pinned
// revisions. maxSteps bounds expanded edges, not returned nodes.
func (v *View) Explore(scope Scope, seed RevisionRef, maxSteps int) ([]Node, error) {
	if scope != ScopeEvaluation {
		return nil, fmt.Errorf("%w: %s", ErrScopeDenied, scope)
	}
	if maxSteps < 0 {
		return nil, fmt.Errorf("%w: negative graph budget", ErrInvalidCanonicalInput)
	}
	seedNode, err := v.Get(scope, seed)
	if err != nil { return nil, err }
	byRef := make(map[RevisionRef]Node, len(v.Nodes))
	for _, node := range v.Nodes { byRef[node.Ref] = node }
	adjacent := make(map[RevisionRef][]RevisionRef)
	for _, edge := range v.Edges { adjacent[edge.From] = append(adjacent[edge.From], edge.To) }
	for ref := range adjacent {
		sort.Slice(adjacent[ref], func(i, j int) bool { return adjacent[ref][i].String() < adjacent[ref][j].String() })
	}
	seen := map[RevisionRef]bool{seed: true}
	queue := []RevisionRef{seed}
	out := []Node{seedNode}
	steps := 0
	for len(queue) > 0 && steps < maxSteps {
		current := queue[0]
		queue = queue[1:]
		for _, next := range adjacent[current] {
			if steps >= maxSteps { break }
			steps++
			if seen[next] { continue }
			seen[next] = true
			queue = append(queue, next)
			out = append(out, byRef[next])
		}
	}
	return out, nil
}

// Bytes is a stable, byte-comparable serialization of the derived view.
func (v *View) Bytes() ([]byte, error) { return json.Marshal(v) }

// Cache is an explicitly disposable derived cache. Build never reads from it,
// so deleting it cannot lose canonical history or alter a future reconstruction.
type Cache struct{ view *View }
func (c *Cache) Replace(view *View) { c.view = cloneView(view) }
func (c *Cache) Delete() { c.view = nil }
func (c *Cache) Snapshot() (*View, bool) {
	if c.view == nil { return nil, false }
	return cloneView(c.view), true
}

func verifyFixture(fixture CanonicalFixture) error {
	pin := fixture.Pin
	if pin.Stream != StreamEvaluation || pin.LedgerRevision != uint64(len(fixture.Records)) {
		return fmt.Errorf("%w: evaluation stream or revision", ErrPinMismatch)
	}
	for index, record := range fixture.Records {
		if record.Sequence != uint64(index+1) { return fmt.Errorf("%w: non-contiguous sequence", ErrInvalidCanonicalInput) }
		if record.Kind == RecordHeldOutFeedback { return ErrHeldOutFeedbackInput }
		if err := validateShape(record); err != nil { return err }
		if record.Digest != recordDigest(record) { return fmt.Errorf("%w: record %q digest", ErrPinMismatch, record.ID) }
	}
	ledgerDigest := digestJSON(fixture.Records)
	if pin.LedgerDigest != ledgerDigest { return fmt.Errorf("%w: ledger digest", ErrPinMismatch) }
	wantWatermark := watermark(pin.Stream, pin.LedgerRevision, pin.LedgerDigest)
	if pin.Watermark != wantWatermark || pin.WatermarkDigest != digestJSON(pin.Watermark) {
		return fmt.Errorf("%w: watermark", ErrPinMismatch)
	}
	return nil
}

func validateShape(record CanonicalRecord) error {
	if record.Sequence == 0 || record.ID == "" { return fmt.Errorf("%w: record sequence and id required", ErrInvalidCanonicalInput) }
	switch record.Kind {
	case RecordProposal:
		if record.Proposal == nil || record.Consolidation != nil || record.Relation != nil || !record.Proposal.Advisory || !validRef(record.Proposal.Ref) || record.Proposal.Body == "" { return fmt.Errorf("%w: advisory proposal", ErrInvalidCanonicalInput) }
	case RecordConsolidation:
		if record.Consolidation == nil || record.Proposal != nil || record.Relation != nil || !validRef(record.Consolidation.Target) || !validDisposition(record.Consolidation.Disposition) { return fmt.Errorf("%w: consolidation", ErrInvalidCanonicalInput) }
	case RecordRelation:
		if record.Relation == nil || record.Proposal != nil || record.Consolidation != nil || !validRef(record.Relation.From) || !validRef(record.Relation.To) || record.Relation.Kind == "" { return fmt.Errorf("%w: relation", ErrInvalidCanonicalInput) }
	case RecordHeldOutFeedback:
		return nil
	default:
		return fmt.Errorf("%w: unknown record kind %q", ErrInvalidCanonicalInput, record.Kind)
	}
	return nil
}

func validRef(ref RevisionRef) bool { return ref.LineageID != "" && ref.Revision > 0 }
func validDisposition(value string) bool {
	switch value { case "retain", "revise", "specialize", "merge": return true }
	return false
}
func watermark(stream string, revision uint64, ledgerDigest string) string { return fmt.Sprintf("%s@%d:%s", stream, revision, ledgerDigest) }
func recordDigest(record CanonicalRecord) string { record.Digest = ""; return digestJSON(record) }
func digestJSON(value any) string { data, _ := json.Marshal(value); sum := sha256.Sum256(data); return "sha256:" + hex.EncodeToString(sum[:]) }
func cloneRecords(records []CanonicalRecord) []CanonicalRecord { data, _ := json.Marshal(records); var cloned []CanonicalRecord; _ = json.Unmarshal(data, &cloned); return cloned }
func cloneView(view *View) *View { if view == nil { return nil }; data, _ := view.Bytes(); var cloned View; _ = json.Unmarshal(data, &cloned); return &cloned }
