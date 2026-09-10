package dag

import (
	"context"
	"errors"
	"sort"
)

// ErrNotImplemented is kept as the red sentinel for this package's contract
// tests; the implementation no longer returns it.
var ErrNotImplemented = errors.New("dag authority not implemented")

// Graph Memory is recall/evidence only: it can never append events, close
// segments, or rewrite links in the canonical Interaction DAG. Every
// mutation-like field in its response is recorded as ignored and the snapshot
// is returned unchanged.
type Segment struct {
	ID         string
	RoomID     string
	DeliveryID string
	State      string
}

type Event struct {
	ID        string
	SegmentID string
	Kind      string
	Content   string
}

type Link struct {
	ID            string
	FromSegmentID string
	ToSegmentID   string
	Kind          string
}

type Snapshot struct {
	Segments []Segment
	Events   []Event
	Links    []Link
}

type GraphMemoryResponse struct {
	RecallContent      string
	CitationID         string
	MutationLikeFields map[string]any
}

type AuthorityResult struct {
	Before                    Snapshot
	After                     Snapshot
	IgnoredMutationFieldNames []string
}

func ApplyGraphMemoryResponse(_ context.Context, snapshot Snapshot, response GraphMemoryResponse) (AuthorityResult, error) {
	ignored := make([]string, 0, len(response.MutationLikeFields))
	for name := range response.MutationLikeFields {
		ignored = append(ignored, name)
	}
	sort.Strings(ignored)
	return AuthorityResult{
		Before:                    snapshot,
		After:                     snapshot,
		IgnoredMutationFieldNames: ignored,
	}, nil
}
