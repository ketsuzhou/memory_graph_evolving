package segment

// Protected conversation path extraction (Host 4.3-4.4, branch preservation).
//
// ExtractPaths is a pure function over the frozen close input. It implements
// the deterministic policy frozen as host.path-extraction-policy v1 in the
// FND-002 corpus:
//
//   - With no failed tool execution the segment carries exactly one success
//     path: every member from the first through the terminal
//     decision_checkpoint, excluding replay_dispatch/replay_completion
//     events (replay correlation events are not conversation evidence). The
//     success path keeps the full room-sequence chain including the delivery
//     transition and the checkpoint.
//
//   - Every failed tool execution freezes a failure path: the member prefix
//     through the failed tool result. The failed tool result is the path's
//     terminal outcome event and attaches via its tool_call_to_result link,
//     so the room-sequence edge into it is not part of the path's causal
//     links (the failure prefix's chain ends at the tool call).
//
//   - When a bounded-infra retry of a failed call succeeded, a recovery path
//     covers the full conversation through the checkpoint (replay events
//     excluded), preserving both the failure prefix and the recovery
//     actions (Host 4.4). Its causal spine ends at the recovered
//     model_response: the delivery transition and checkpoint attach through
//     their delivery_of/response_to links rather than room-sequence edges.
//
// Tool outcomes mirror the exact ToolProxyResult statuses: plain successes,
// "failed", and "succeeded_via_retry" with attempts=2 for bounded-infra
// retries. Delivery outcomes cover the deliveries whose subject event lies
// on the path. Path labels come from this protected policy only; model
// suggestions never enter the seal.

import (
	"sort"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// Path is one sealed conversation path (Host 4.3).
type Path struct {
	PathID            string
	Domain            string // success|failure|recovery
	OrderedEventRefs  []string
	CausalLinkRefs    []string
	CausalLinksDigest string
	StartRoomSequence int64
	EndRoomSequence   int64
	TerminalOutcome   string
	ToolOutcomes      []ToolOutcome
	DeliveryOutcomes  []DeliveryOutcome
	CheckpointRefID   string
	Completeness      string
}

// ToolOutcome is one tool call's terminal outcome on a path.
type ToolOutcome struct {
	ToolCallEvent string
	Outcome       string
	Attempts      int64 // >0 only on succeeded_via_retry
}

// DeliveryOutcome is one delivery's terminal outcome on a path.
type DeliveryOutcome struct {
	DeliveryID string
	Outcome    string
}

// ExtractPaths discovers and classifies the decision-relevant paths of the
// frozen close input. It returns them ordered by (end room sequence, path
// id); an empty result fails the settled close with MISSING_FAMILY.
func ExtractPaths(snap Snapshot) []Path {
	members := snap.Members
	executions := snap.Executions

	var failurePrefixes [][]string
	hasRecovered := false
	for _, e := range executions {
		status, retry := executionStatus(e)
		if status == "succeeded" {
			if retry {
				hasRecovered = true
			}
			continue
		}
		prefix := make([]string, 0, len(members))
		for _, m := range members {
			prefix = append(prefix, m.EventID)
			if m.EventID == e.ToolResultEvent {
				break
			}
		}
		if len(prefix) == 0 || prefix[len(prefix)-1] != e.ToolResultEvent {
			continue // result event outside membership; validation rejects separately
		}
		failurePrefixes = append(failurePrefixes, prefix)
	}

	paths := make([]Path, 0, 2)
	if len(failurePrefixes) == 0 {
		paths = append(paths, buildPath(snap, "path-success-0001", "success",
			conversationEvents(members), nil, "success"))
	} else {
		for i, prefix := range failurePrefixes {
			terminal := prefix[len(prefix)-1]
			paths = append(paths, buildPath(snap, pathID("failure", i+1), "failure",
				prefix, map[string]bool{terminal: true}, "failed"))
		}
		if hasRecovered {
			// The recovery spine ends at the recovered model_response; the
			// correlated delivery/checkpoint tail attaches semantically.
			noRoomSequenceInto := map[string]bool{}
			for _, m := range members {
				if m.EventKind == "delivery_transition" || m.EventKind == "decision_checkpoint" {
					noRoomSequenceInto[m.EventID] = true
				}
			}
			paths = append(paths, buildPath(snap, "path-recovery-0001", "recovery",
				conversationEvents(members), noRoomSequenceInto, "recovery_succeeded"))
		}
	}

	sort.SliceStable(paths, func(i, j int) bool {
		if paths[i].EndRoomSequence != paths[j].EndRoomSequence {
			return paths[i].EndRoomSequence < paths[j].EndRoomSequence
		}
		return paths[i].PathID < paths[j].PathID
	})
	return paths
}

// executionStatus reads the exact ToolProxyResult status and the bounded
// retry flag of one execution.
func executionStatus(e ToolExecution) (status string, retry bool) {
	status = "failed"
	if e.ToolProxyResult != nil {
		if s, ok := contract.StringOf(e.ToolProxyResult, "status"); ok {
			status = s
		}
	}
	if e.RetryCorrelation != nil {
		if flag, ok := e.RetryCorrelation.Get("is_retry"); ok {
			if b, isBool := flag.(contract.Bool); isBool && bool(b) {
				retry = true
			}
		}
	}
	return status, retry
}

// conversationEvents lists every member except replay dispatch/completion
// correlation events.
func conversationEvents(members []Member) []string {
	events := make([]string, 0, len(members))
	for _, m := range members {
		if m.EventKind == "replay_dispatch" || m.EventKind == "replay_completion" {
			continue
		}
		events = append(events, m.EventID)
	}
	return events
}

func pathID(domain string, index int) string {
	digits := ""
	switch {
	case index >= 1000:
		digits = itoa(index)
	case index >= 100:
		digits = "0" + itoa(index)
	case index >= 10:
		digits = "00" + itoa(index)
	default:
		digits = "000" + itoa(index)
	}
	return "path-" + domain + "-" + digits
}

func buildPath(snap Snapshot, pathIDValue, domain string, events []string,
	noRoomSequenceInto map[string]bool, outcome string) Path {
	members, links, deliveries, executions := snap.Members, snap.Links, snap.Deliveries, snap.Executions

	inPath := make(map[string]bool, len(events))
	for _, id := range events {
		inPath[id] = true
	}

	linkRefs := make([]string, 0, len(links))
	linkObjects := make(contract.Array, 0, len(links))
	for _, l := range links {
		if !inPath[l.FromEvent] || !inPath[l.ToEvent] {
			continue
		}
		if l.LinkKind == "room_sequence" && noRoomSequenceInto[l.ToEvent] {
			continue
		}
		linkRefs = append(linkRefs, l.LinkID)
		linkObjects = append(linkObjects, linkObject(l))
	}
	linksDigest, _ := contract.DigestOf(linkObjects) // string-only core: cannot fail

	var toolOutcomes []ToolOutcome
	for _, e := range executions {
		if !inPath[e.ToolCallEvent] {
			continue
		}
		status, retry := executionStatus(e)
		switch {
		case status != "succeeded":
			toolOutcomes = append(toolOutcomes, ToolOutcome{ToolCallEvent: e.ToolCallEvent, Outcome: "failed"})
		case retry:
			toolOutcomes = append(toolOutcomes, ToolOutcome{ToolCallEvent: e.ToolCallEvent, Outcome: "succeeded_via_retry", Attempts: 2})
		default:
			toolOutcomes = append(toolOutcomes, ToolOutcome{ToolCallEvent: e.ToolCallEvent, Outcome: "succeeded"})
		}
	}

	var deliveryOutcomes []DeliveryOutcome
	for _, d := range deliveries {
		if !inPath[d.SubjectEvent] {
			continue
		}
		deliveryOutcomes = append(deliveryOutcomes, DeliveryOutcome{DeliveryID: d.DeliveryID, Outcome: d.TerminalState})
	}

	checkpointID := ""
	for i := len(members) - 1; i >= 0; i-- {
		if members[i].EventKind == "decision_checkpoint" {
			checkpointID, _ = contract.StringOf(members[i].Payload, "checkpoint_id")
			break
		}
	}

	return Path{
		PathID:            pathIDValue,
		Domain:            domain,
		OrderedEventRefs:  events,
		CausalLinkRefs:    linkRefs,
		CausalLinksDigest: linksDigest,
		StartRoomSequence: seqOf(members, events[0]),
		EndRoomSequence:   seqOf(members, events[len(events)-1]),
		TerminalOutcome:   outcome,
		ToolOutcomes:      toolOutcomes,
		DeliveryOutcomes:  deliveryOutcomes,
		CheckpointRefID:   checkpointID,
		Completeness:      "complete",
	}
}

func seqOf(members []Member, eventID string) int64 {
	for _, m := range members {
		if m.EventID == eventID {
			return m.RoomSequence
		}
	}
	return 0
}

// primaryEvidenceKind classifies the EvidenceRef kind by the sealed path
// families (recovery > failure > success).
func primaryEvidenceKind(paths []Path) string {
	domains := map[string]bool{}
	for _, p := range paths {
		domains[p.Domain] = true
	}
	switch {
	case domains["recovery"]:
		return "recovery_path"
	case domains["failure"]:
		return "failure_path"
	default:
		return "success_path"
	}
}

// primaryPath returns the path whose family classifies the evidence seal.
func primaryPath(paths []Path) Path {
	want := map[string]string{
		"recovery_path": "recovery",
		"failure_path":  "failure",
		"success_path":  "success",
	}[primaryEvidenceKind(paths)]
	for _, p := range paths {
		if p.Domain == want {
			return p
		}
	}
	return paths[0]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
