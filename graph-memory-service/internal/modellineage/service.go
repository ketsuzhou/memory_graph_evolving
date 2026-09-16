// Package modellineage implements PG-24 checkpoint/model lineage: complete,
// transitive, deduplicated descendant tracing and tombstone propagation to
// every affected descendant (SC-7.x governance seam).
package modellineage

import (
	"context"
	"errors"
	"sort"
	"sync"

	"river2.dev/graph-memory-service/internal/artifactsecurity"
	"river2.dev/graph-memory-service/internal/domain"
)

var ErrModelLineageNotImplemented = errors.New("modellineage: not implemented")

var (
	ErrNodeUnknown        = errors.New("modellineage: checkpoint node was never registered")
	ErrDuplicateParentRef = errors.New("modellineage: node lists a duplicate parent reference")
	ErrNodeExists         = errors.New("modellineage: checkpoint node already registered; lineage edges are immutable")
	ErrCrossTenantParent  = errors.New("modellineage: parent belongs to a different tenant")
	ErrTombstonedAncestor = errors.New("modellineage: node descends from an erased (tombstoned) checkpoint")
	// ErrCascadeReasonInvalid rejects free-text cascade reasons: lineage
	// tombstones are non-sensitive audit records sharing artifact erase's
	// closed reason vocabulary, so PII or payload fragments can never enter
	// lineage records through this seam (R6).
	ErrCascadeReasonInvalid = errors.New("modellineage: cascade reason must be a closed non-sensitive reason code")
)

type CheckpointID string

type CheckpointRef struct {
	ID       CheckpointID
	TenantID domain.TenantID
}

// Node records all immediate parents; a descendant is affected transitively.
type Node struct {
	Ref     CheckpointRef
	Parents []CheckpointRef
}

type Tombstone struct {
	Ref       CheckpointRef
	Reason    string
	SourceRef CheckpointRef
}

type Store interface {
	Put(context.Context, Node) error
	AffectedDescendants(context.Context, CheckpointRef) ([]CheckpointRef, error)
	TombstoneCascade(context.Context, CheckpointRef, artifactsecurity.EraseReason) ([]Tombstone, error)
}

// Service is the in-memory lineage authority: registration via Put (edges
// immutable once written), forward BFS for descendants, and tombstone
// cascades that reach every transitive descendant exactly once and stay
// recorded for later inspection.
type Service struct {
	mu         sync.Mutex
	nodes      map[CheckpointRef]*Node
	children   map[CheckpointRef][]CheckpointRef
	tombstones []Tombstone
}

func NewService() *Service {
	return &Service{
		nodes:      map[CheckpointRef]*Node{},
		children:   map[CheckpointRef][]CheckpointRef{},
		tombstones: []Tombstone{},
	}
}

// Put registers a node and its immediate parent edges. Parent references
// must exist in the same tenant so the graph stays closed under tracing and
// tenant isolation; an already-registered ref is rejected because rewriting
// edges would strand stale adjacency and break complete tracing, and a node
// under an already-tombstoned ancestor is rejected so erased lineage cannot
// silently regrow live descendants.
func (s *Service) Put(_ context.Context, node Node) error {
	if node.Ref.ID == "" {
		return errors.New("modellineage: node requires an id")
	}
	seen := make(map[CheckpointRef]bool, len(node.Parents))
	for _, parent := range node.Parents {
		if parent == node.Ref {
			return errors.New("modellineage: node cannot be its own parent")
		}
		if seen[parent] {
			return ErrDuplicateParentRef
		}
		if parent.TenantID != node.Ref.TenantID {
			return ErrCrossTenantParent
		}
		seen[parent] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[node.Ref]; ok {
		return ErrNodeExists
	}
	if s.tombstonedAncestorLocked(node.Parents) {
		return ErrTombstonedAncestor
	}
	for _, parent := range node.Parents {
		if _, ok := s.nodes[parent]; !ok {
			return ErrNodeUnknown
		}
	}
	stored := Node{Ref: node.Ref, Parents: append([]CheckpointRef(nil), node.Parents...)}
	s.nodes[node.Ref] = &stored
	for _, parent := range node.Parents {
		s.children[parent] = append(s.children[parent], node.Ref)
	}
	return nil
}

// AffectedDescendants returns every transitive descendant of the ref,
// deduplicated (diamond merges appear once), deterministically ordered, and
// never including the ref itself.
func (s *Service) AffectedDescendants(_ context.Context, ref CheckpointRef) ([]CheckpointRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[ref]; !ok {
		return nil, ErrNodeUnknown
	}
	return s.descendantsLocked(ref), nil
}

// TombstoneCascade propagates one erasure tombstone to the source root AND
// every affected descendant, each carrying the original source ref and
// reason. The reason must come from the closed vocabulary artifact erase
// shares (artifactsecurity.EraseReason): free text, PII, or unknown codes
// are rejected before any tombstone is written. Recording the root closes
// the regrow bypass: a Put naming the erased root as a direct parent is
// rejected exactly like a put under a tombstoned descendant. The descendant
// snapshot and the tombstone writes share one lock span, so a concurrent Put
// can never land between them and be missed by the cascade; cascades are
// idempotent, and the accumulated tombstones stay queryable until PG-50A
// wires durable persistence.
func (s *Service) TombstoneCascade(_ context.Context, ref CheckpointRef, reason artifactsecurity.EraseReason) ([]Tombstone, error) {
	if !artifactsecurity.ValidEraseReason(reason) {
		return nil, ErrCascadeReasonInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[ref]; !ok {
		return nil, ErrNodeUnknown
	}
	descendants := s.descendantsLocked(ref)
	tombstones := make([]Tombstone, 0, len(descendants)+1)
	root := Tombstone{Ref: ref, Reason: string(reason), SourceRef: ref}
	if !s.tombstoneRecordedLocked(root) {
		s.tombstones = append(s.tombstones, root)
	}
	tombstones = append(tombstones, root)
	for _, descendant := range descendants {
		tombstone := Tombstone{Ref: descendant, Reason: string(reason), SourceRef: ref}
		if !s.tombstoneRecordedLocked(tombstone) {
			s.tombstones = append(s.tombstones, tombstone)
		}
		tombstones = append(tombstones, tombstone)
	}
	return tombstones, nil
}

// Tombstones returns every recorded cascade tombstone.
func (s *Service) Tombstones() []Tombstone {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Tombstone(nil), s.tombstones...)
}

// descendantsLocked is the forward BFS under the caller's lock; callers must
// hold s.mu.
func (s *Service) descendantsLocked(ref CheckpointRef) []CheckpointRef {
	visited := map[CheckpointRef]bool{}
	frontier := []CheckpointRef{ref}
	var descendants []CheckpointRef
	for len(frontier) > 0 {
		current := frontier[0]
		frontier = frontier[1:]
		children := append([]CheckpointRef(nil), s.children[current]...)
		sort.Slice(children, func(i, j int) bool { return children[i].ID < children[j].ID })
		for _, child := range children {
			if visited[child] || child == ref {
				continue
			}
			visited[child] = true
			descendants = append(descendants, child)
			frontier = append(frontier, child)
		}
	}
	return descendants
}

// tombstonedAncestorLocked reports whether any listed parent sits in a
// recorded tombstone's affected set; recorded tombstones cover exactly the
// currently-erased nodes, so a Ref match is sufficient. Callers must hold
// s.mu.
func (s *Service) tombstonedAncestorLocked(parents []CheckpointRef) bool {
	tombstoned := make(map[CheckpointRef]bool, len(s.tombstones))
	for _, tombstone := range s.tombstones {
		tombstoned[tombstone.Ref] = true
	}
	for _, parent := range parents {
		if tombstoned[parent] {
			return true
		}
	}
	return false
}

func (s *Service) tombstoneRecordedLocked(tombstone Tombstone) bool {
	for _, recorded := range s.tombstones {
		if recorded.Ref == tombstone.Ref && recorded.SourceRef == tombstone.SourceRef && recorded.Reason == tombstone.Reason {
			return true
		}
	}
	return false
}
