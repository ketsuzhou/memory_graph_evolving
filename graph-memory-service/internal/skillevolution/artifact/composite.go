package artifact

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

// integerFormPattern matches the plain decimal integer JSON productions.
var integerFormPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

// SchemaCompositePacket is the private frozen whole-Composite packet
// envelope (GMS-202): the packet digest freezes children, ports, edges,
// retry policies, failure handlers, orchestration permissions and the
// composite port schemas. Any child ref change produces a new digest, i.e.
// a new Composite revision (Contract §8.4/§9.5.6).
const SchemaCompositePacket = "gms.composite-packet.v1"

// ChildArtifactResolver optionally resolves one exact child ref to its
// released artifact envelope (used to check release-time activation and to
// compute the children permission union; nil skips both — release-time
// binding belongs to GMS-204).
type ChildArtifactResolver interface {
	// ResolveChild returns the child's canonical artifact for the exact ref,
	// or an error carrying COMPOSITE_CHILD_NOT_ACTIVE when the ref does not
	// resolve to a released, currently active revision.
	ResolveChild(ref contract.SkillArtifactRef) (*CanonicalArtifact, error)
}

// canonicalizeComposite runs the Composite static gates (GMS §3.5,
// Contract §8.4/§9.5.1). Kind-specific codes fire before the generic body
// shape gate so each violation surfaces with its closed reason.
func (s *Service) canonicalizeComposite(body map[string]any) error {
	children, _ := contract.AsArray(body["children"])
	declared := map[string]map[string]any{}
	for _, raw := range children {
		child, _ := contract.AsObject(raw)
		id, _ := contract.AsString(child["child_id"])
		if id != "" {
			declared[id] = child
		}
	}

	// 1. Named ports: every child declares input_port and output_port
	// (PORT_SCHEMA_MISSING — checked before the body shape so the closed
	// code, not SCHEMA_REQUIRED_FIELD_MISSING, surfaces).
	for _, raw := range children {
		child, _ := contract.AsObject(raw)
		id, _ := contract.AsString(child["child_id"])
		_, hasInput := contract.AsString(child["input_port"])
		_, hasOutput := contract.AsString(child["output_port"])
		if !hasInput || !hasOutput {
			return newError(ReasonPortSchemaMissing, "child %q must declare named input_port and output_port", id)
		}
		// Exact §7.3 identity form first: latest versions, naked ids and
		// Graph-node forms are NON_EXACT_REF, not shape errors.
		refRaw, _ := contract.AsObject(child["skill_ref"])
		if !isExactRefShape(refRaw) {
			return newError(ReasonNonExactRef, "child %q skill_ref is not an exact ref (lineage+integer version+digest)", id)
		}
	}
	// 2. Bounded retry: every policy references a declared child and fixes
	// a finite max_attempts >= 1 (COMPOSITE_RETRY_INVALID; cycles may only
	// exist as bounded retries, Contract §8.4).
	if retries, ok := contract.AsArray(body["retry_policies"]); ok {
		for _, raw := range retries {
			policy, _ := contract.AsObject(raw)
			childID, _ := contract.AsString(policy["child_id"])
			if _, isDeclared := declared[childID]; !isDeclared {
				return newError(ReasonCompositeRetryInvalid, "retry policy references undeclared child %q", childID)
			}
			attempts, isInt := IntegerOfAttempts(policy["max_attempts"])
			if !isInt || attempts < 1 {
				return newError(ReasonCompositeRetryInvalid, "retry policy of child %q must fix a bounded max_attempts >= 1", childID)
			}
		}
	}
	// 3. Fallback declarations: action=fallback declares its fallback child
	// (COMPOSITE_FAILURE_ACTION_INVALID; the conditional requirement also
	// exists in the schema, this surfaces the closed code first).
	if handlers, ok := contract.AsArray(body["failure_handlers"]); ok {
		for _, raw := range handlers {
			handler, _ := contract.AsObject(raw)
			if action, _ := contract.AsString(handler["action"]); action == "fallback" {
				fallback, isStr := contract.AsString(handler["fallback_child_id"])
				if !isStr || fallback == "" {
					childID, _ := contract.AsString(handler["child_id"])
					return newError(ReasonCompositeFailureInvalid, "fallback handler of child %q declares no fallback_child_id", childID)
				}
			}
		}
	}
	// 4. Body shape against the authority schema (closed fields, port
	// schema refs, permission entries, enums, conditionals).
	if err := s.gates.ValidateInstance(body, SchemaComposite); err != nil {
		return err
	}
	// 5. Child identity: unique child ids and strict §7.3 refs (the exact
	// form was pre-checked; the strict parse closes the remaining shape
	// surface: schema version, kind enum, digest pattern).
	seen := map[string]bool{}
	refs := make([]contract.SkillArtifactRef, 0, len(children))
	for _, raw := range children {
		child, _ := contract.AsObject(raw)
		id, _ := contract.AsString(child["child_id"])
		if seen[id] {
			return newError(ReasonArtifactBodyInvalid, "duplicate child_id %q", id)
		}
		seen[id] = true
		refRaw, _ := contract.AsObject(child["skill_ref"])
		ref, err := contract.ParseSkillArtifactRef(refRaw)
		if err != nil {
			return newError(validation.CodeOf(err), "child %q skill_ref: %v", id, err)
		}
		refs = append(refs, ref)
	}
	// 6. Edges: endpoints declared, ports compatible with the children's
	// declared ports (edge from_port is the source child's output_port,
	// to_port the target child's input_port — PORT_SCHEMA_INCOMPATIBLE).
	edges, _ := contract.AsArray(body["edges"])
	adjacency := map[string][]string{}
	for _, raw := range edges {
		edge, _ := contract.AsObject(raw)
		fromID, _ := contract.AsString(edge["from_child_id"])
		toID, _ := contract.AsString(edge["to_child_id"])
		fromChild, fromOK := declared[fromID]
		toChild, toOK := declared[toID]
		if !fromOK || !toOK {
			return newError(ReasonArtifactBodyInvalid, "edge %q -> %q references an undeclared child", fromID, toID)
		}
		fromPort, _ := contract.AsString(edge["from_port"])
		toPort, _ := contract.AsString(edge["to_port"])
		expectedFrom, _ := contract.AsString(fromChild["output_port"])
		expectedTo, _ := contract.AsString(toChild["input_port"])
		if fromPort != expectedFrom || toPort != expectedTo {
			return newError(ReasonPortSchemaIncompatible,
				"edge %q[%s] -> %q[%s] does not match the declared ports (%q output %q, %q input %q)",
				fromID, fromPort, toID, toPort, fromID, expectedFrom, toID, expectedTo)
		}
		adjacency[fromID] = append(adjacency[fromID], toID)
	}
	// 7. DAG: children/edges must be acyclic (COMPOSITE_CYCLE).
	if hasCycle(seen, adjacency) {
		return newError(ReasonCompositeCycle, "composite children/edges form a cycle; loops may only exist as bounded retries")
	}
	// 8. Fallback resolution: the fallback child is declared and its
	// dependencies are satisfiable without the failing child.
	if handlers, ok := contract.AsArray(body["failure_handlers"]); ok {
		for _, raw := range handlers {
			handler, _ := contract.AsObject(raw)
			if action, _ := contract.AsString(handler["action"]); action != "fallback" {
				continue
			}
			childID, _ := contract.AsString(handler["child_id"])
			fallback, _ := contract.AsString(handler["fallback_child_id"])
			if _, isDeclared := declared[fallback]; !isDeclared {
				return newError(ReasonCompositeFailureInvalid, "fallback child %q of %q is not declared", fallback, childID)
			}
			// Edges point prerequisite -> dependent: the fallback is
			// unsatisfiable when the failing child is upstream of it (the
			// fallback would need the failed child's output).
			if reachable(adjacency, childID, fallback) {
				return newError(ReasonCompositeFailureInvalid,
					"fallback child %q (transitively) depends on the failing child %q, so its dependencies cannot be satisfied", fallback, childID)
			}
		}
	}
	// 9. Optional release-time child resolution: every child resolves to a
	// released, currently active revision and the children permission union
	// joins the orchestration permissions under the Host cap.
	if s.children != nil {
		union := map[string]bool{}
		for _, ref := range refs {
			child, err := s.children.ResolveChild(ref)
			if err != nil {
				return err
			}
			for _, capability := range child.PermissionCapabilities() {
				union[capability] = true
			}
		}
		for _, raw := range orchestrationPermissions(body) {
			entry, _ := contract.AsObject(raw)
			capability, _ := contract.AsString(entry["capability"])
			union[capability] = true
		}
		capabilities := make([]string, 0, len(union))
		for capability := range union {
			capabilities = append(capabilities, capability)
		}
		sort.Strings(capabilities)
		for _, capability := range capabilities {
			if !validation.HostCapabilityCapV2[capability] {
				return newError(ReasonPermissionCapExceeded, "composite permission union (children + orchestration) exceeds the Host cap at %q", capability)
			}
		}
	}
	return nil
}

// CompositePacket is the frozen whole-Composite definition.
type CompositePacket struct {
	digest string
	refs   []contract.SkillArtifactRef
}

// Digest returns the packet digest: any change to children, ports, edges,
// retry policies, failure handlers or permissions forces a new revision.
func (p *CompositePacket) Digest() string { return p.digest }

// ChildRefs returns copies of the exact child refs (frozen at gate time).
func (p *CompositePacket) ChildRefs() []contract.SkillArtifactRef {
	out := make([]contract.SkillArtifactRef, len(p.refs))
	copy(out, p.refs)
	return out
}

// freezePacket builds the frozen packet over the already-validated body.
func freezePacket(body map[string]any) (*CompositePacket, error) {
	packet := map[string]any{
		"schema_version": SchemaCompositePacket,
	}
	for _, key := range []string{
		"children", "edges", "retry_policies", "failure_handlers",
		"orchestration_permissions", "input_port_schema", "output_port_schema",
	} {
		if value, present := body[key]; present {
			packet[key] = deepCopyValue(value)
		}
	}
	canonical, err := contract.JCS(packet)
	if err != nil {
		return nil, newError(validation.CodeNonIntegerNumber, "composite packet cannot enter the hashed core: %v", err)
	}
	var refs []contract.SkillArtifactRef
	children, _ := contract.AsArray(body["children"])
	for _, raw := range children {
		child, _ := contract.AsObject(raw)
		refRaw, _ := contract.AsObject(child["skill_ref"])
		ref, err := contract.ParseSkillArtifactRef(refRaw)
		if err != nil {
			return nil, newError(validation.CodeOf(err), "child ref: %v", err)
		}
		refs = append(refs, ref)
	}
	return &CompositePacket{digest: contract.DigestBytes(canonical), refs: refs}, nil
}

// --- helpers -----------------------------------------------------------------

// isExactRefShape reports whether ref carries the exact §7.3 identity form:
// lineage_id + plain-integer version + digest and no Graph-node aliasing
// (NON_EXACT_REF otherwise — latest, naked ids and graph node ids are not
// exact refs).
func isExactRefShape(ref map[string]any) bool {
	if ref == nil {
		return false
	}
	if _, has := ref["graph_node_id"]; has {
		return false
	}
	if _, has := ref["node_id"]; has {
		return false
	}
	if _, ok := contract.AsString(ref["lineage_id"]); !ok {
		return false
	}
	if !contract.IsIntegerNumber(ref["version"]) {
		return false
	}
	_, hasDigest := contract.AsString(ref["artifact_digest"])
	return hasDigest
}

// hasCycle is the white/gray/black DFS of the S1 semantics (deterministic
// start order).
func hasCycle(nodes map[string]bool, adjacency map[string][]string) bool {
	const (
		white, gray, black = 0, 1, 2
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

// reachable reports whether target is transitively reachable from start.
func reachable(adjacency map[string][]string, start, target string) bool {
	if start == target {
		return true
	}
	seen := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		for _, next := range adjacency[node] {
			if next == target {
				return true
			}
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}

func orchestrationPermissions(body map[string]any) []any {
	list, _ := contract.AsArray(body["orchestration_permissions"])
	return list
}

// IntegerOfAttempts reads max_attempts in plain integer form.
func IntegerOfAttempts(v any) (int64, bool) {
	number, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	text := string(number)
	if !integerFormPattern.MatchString(text) {
		return 0, false
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		if strings.HasPrefix(text, "-") {
			return -1, true // below the minimum either way
		}
		return 1 << 62, true
	}
	return value, true
}

// declaredPermissions lists the envelope permission capabilities (used for
// the composite permission union when children resolve).
func (a *CanonicalArtifact) declaredPermissions() []string {
	return nil // envelope permissions are checked by CheckPermissionsWalked;
	// the per-child union joins through the resolver seam at release time.
}
