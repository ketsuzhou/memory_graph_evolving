package dag

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestGraphMemoryResponsesCannotMutateCanonicalDAG(t *testing.T) {
	t.Parallel()
	canonical := Snapshot{
		Segments: []Segment{{ID: "segment-001", RoomID: "room-alpha", DeliveryID: "delivery-001", State: "open"}},
		Events:   []Event{{ID: "event-001", SegmentID: "segment-001", Kind: "room_message", Content: "trusted host event"}},
		Links:    []Link{{ID: "link-001", FromSegmentID: "segment-root", ToSegmentID: "segment-001", Kind: "mentions"}},
	}
	untrusted := GraphMemoryResponse{
		RecallContent: "Useful recalled fact.",
		CitationID:    "citation-001",
		MutationLikeFields: map[string]any{
			"append_event":  map[string]any{"segment_id": "segment-001", "kind": "forged"},
			"close_segment": map[string]any{"segment_id": "segment-001", "state": "settled"},
			"delete_link":   "link-001",
			"segments":      []any{},
		},
	}

	result, err := ApplyGraphMemoryResponse(context.Background(), canonical, untrusted)
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("DAG authority contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("apply Graph Memory response: %v", err)
	}
	if !reflect.DeepEqual(result.Before, canonical) || !reflect.DeepEqual(result.After, canonical) {
		t.Fatalf("canonical DAG changed from %#v to %#v; Graph Memory is recall/evidence only", canonical, result.After)
	}

	gotIgnored := append([]string(nil), result.IgnoredMutationFieldNames...)
	sort.Strings(gotIgnored)
	wantIgnored := []string{"append_event", "close_segment", "delete_link", "segments"}
	if !reflect.DeepEqual(gotIgnored, wantIgnored) {
		t.Fatalf("ignored mutation-like fields = %v, want %v", gotIgnored, wantIgnored)
	}
}
