// Package contractconformance validates the frozen JSON conformance corpus
// without importing production services. It intentionally implements only the
// JSON Schema vocabulary exercised by this corpus.
package contractconformance

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type fixtureEntry struct {
	File       string   `json:"file"`
	Kind       string   `json:"kind"`
	Target     string   `json:"target"`
	Verdict    string   `json:"verdict"`
	ExtraRules []string `json:"extra_rules"`
}

type manifest struct {
	Fixtures []fixtureEntry `json:"fixtures"`
}

var reasonCodeLine = regexp.MustCompile(`^\s*-\s+code:\s*([A-Z][A-Z0-9_]*)\s*$`)

// CorpusVerdicts returns one acceptance verdict per manifest-indexed fixture.
func CorpusVerdicts(root string) (map[string]bool, error) {
	var index manifest
	if err := readJSON(filepath.Join(root, "manifest.json"), &index); err != nil {
		return nil, err
	}
	reasonCodes, err := readReasonCodes(filepath.Join(root, "reasons.yaml"))
	if err != nil {
		return nil, err
	}

	verdicts := make(map[string]bool, len(index.Fixtures))
	for _, entry := range index.Fixtures {
		var fixture any
		if err := readJSON(filepath.Join(root, "fixtures", entry.File), &fixture); err != nil {
			return nil, fmt.Errorf("read fixture %s: %w", entry.File, err)
		}

		var accepted bool
		var rules []string
		switch entry.Kind {
		case "schema":
			var schema map[string]any
			if err := readJSON(filepath.Join(root, "schema", entry.Target+".json"), &schema); err != nil {
				return nil, fmt.Errorf("read schema %s: %w", entry.Target, err)
			}
			accepted = validateSchema(fixture, schema, schema) == nil
			rules = entry.ExtraRules
		case "cross":
			accepted = true
			rules = []string{entry.Target}
		case "state":
			var machine map[string]any
			if err := readJSON(filepath.Join(root, "state", entry.Target+".json"), &machine); err != nil {
				return nil, fmt.Errorf("read state machine %s: %w", entry.Target, err)
			}
			accepted = validateState(fixture, machine, reasonCodes) == nil
		default:
			return nil, fmt.Errorf("fixture %s has unsupported kind %q", entry.File, entry.Kind)
		}
		for _, name := range rules {
			if err := validateExtraRule(name, fixture); err != nil {
				accepted = false
				break
			}
		}
		verdicts[entry.File] = accepted
	}
	return verdicts, nil
}

// DeclaredVerdicts returns the expected acceptance verdict encoded in manifest.json.
func DeclaredVerdicts(root string) (map[string]bool, error) {
	var index manifest
	if err := readJSON(filepath.Join(root, "manifest.json"), &index); err != nil {
		return nil, err
	}

	verdicts := make(map[string]bool, len(index.Fixtures))
	for _, entry := range index.Fixtures {
		switch entry.Verdict {
		case "accept":
			verdicts[entry.File] = true
		case "reject":
			verdicts[entry.File] = false
		default:
			return nil, fmt.Errorf("fixture %s has unsupported verdict %q", entry.File, entry.Verdict)
		}
	}
	return verdicts, nil
}

func readJSON(path string, target any) error {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, target)
}

func readReasonCodes(path string) (map[string]struct{}, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	codes := map[string]struct{}{}
	for _, line := range strings.Split(string(contents), "\n") {
		if matches := reasonCodeLine.FindStringSubmatch(line); matches != nil {
			codes[matches[1]] = struct{}{}
		}
	}
	if len(codes) == 0 {
		return nil, fmt.Errorf("reason registry %s contains no codes", path)
	}
	return codes, nil
}

func validateExtraRule(name string, instance any) error {
	fixture, ok := instance.(map[string]any)
	if !ok {
		return fmt.Errorf("fixture is not an object")
	}
	switch name {
	case "logical_run_episode_unique":
		runs, ok := fixture["logical_runs"].([]any)
		if !ok {
			return fmt.Errorf("logical_runs is not an array")
		}
		seen := map[string]struct{}{}
		for _, raw := range runs {
			run, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("logical run is not an object")
			}
			key := canonicalPair(run["task_id"], run["episode_id"])
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate logical run task/episode identity")
			}
			seen[key] = struct{}{}
		}
		return nil
	case "logical_run_id_unique":
		runs, ok := fixture["logical_runs"].([]any)
		if !ok {
			return fmt.Errorf("logical_runs is not an array")
		}
		seen := map[string]struct{}{}
		for _, raw := range runs {
			run, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("logical run is not an object")
			}
			key, ok := run["logical_run_id"].(string)
			if !ok {
				return fmt.Errorf("logical run id is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate logical run id")
			}
			seen[key] = struct{}{}
		}
		return nil
	case "cut_receipts_cover_sealed_segments":
		return validateCutReceiptCoverage(fixture)
	case "sink_receipt_matches":
		return validateSinkReceipt(fixture)
	case "capability_scope_within_cut":
		return validateCapabilityScope(fixture)
	default:
		return fmt.Errorf("unsupported extra rule %q", name)
	}
}

func validateCutReceiptCoverage(cut map[string]any) error {
	sealed, ok := cut["sealed_segment_ids"].([]any)
	if !ok {
		return fmt.Errorf("sealed_segment_ids is not an array")
	}
	receipts, ok := cut["evidence_commit_receipts"].([]any)
	if !ok {
		return fmt.Errorf("evidence_commit_receipts is not an array")
	}
	sealedIDs := map[string]struct{}{}
	for _, raw := range sealed {
		id, ok := raw.(string)
		if !ok {
			return fmt.Errorf("sealed segment id is not a string")
		}
		if _, duplicate := sealedIDs[id]; duplicate {
			return fmt.Errorf("duplicate sealed segment %q", id)
		}
		sealedIDs[id] = struct{}{}
	}
	receiptIDs := map[string]struct{}{}
	for _, raw := range receipts {
		receipt, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("receipt is not an object")
		}
		id, ok := receipt["segment_id"].(string)
		if !ok {
			return fmt.Errorf("receipt segment id is not a string")
		}
		if _, duplicate := receiptIDs[id]; duplicate {
			return fmt.Errorf("duplicate receipt coverage for segment %q", id)
		}
		receiptIDs[id] = struct{}{}
	}
	if !reflect.DeepEqual(sealedIDs, receiptIDs) {
		return fmt.Errorf("receipt coverage does not exactly match sealed segments")
	}
	return nil
}

func validateSinkReceipt(export map[string]any) error {
	rawReceipt, present := export["sink_receipt"]
	if !present || rawReceipt == nil {
		return nil
	}
	receipt, ok := rawReceipt.(map[string]any)
	if !ok {
		return fmt.Errorf("sink_receipt is not an object")
	}
	if receipt["sink_id"] != export["required_sink_id"] || receipt["verified_digest"] != export["content_digest"] {
		return fmt.Errorf("sink receipt does not match export")
	}
	return nil
}

func validateCapabilityScope(fixture map[string]any) error {
	cut, ok := fixture["cut"].(map[string]any)
	if !ok {
		return fmt.Errorf("cross fixture cut is not an object")
	}
	capability, ok := fixture["capability"].(map[string]any)
	if !ok {
		return fmt.Errorf("cross fixture capability is not an object")
	}
	if capability["tenant_id"] != cut["tenant_id"] || capability["cut_id"] != cut["cut_id"] {
		return fmt.Errorf("capability tenant or cut does not match frozen cut")
	}
	spaceScopes, ok := cut["space_scopes"].([]any)
	if !ok {
		return fmt.Errorf("cut space scopes are not an array")
	}
	frozenPrivate := map[string]struct{}{}
	for _, raw := range spaceScopes {
		scope, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("space scope is not an object")
		}
		if scope["scope"] != "private" {
			continue
		}
		id, ok := scope["space_id"].(string)
		if !ok {
			return fmt.Errorf("private space id is not a string")
		}
		frozenPrivate[id] = struct{}{}
	}
	boundSpaces, ok := capability["bound_private_spaces"].([]any)
	if !ok {
		return fmt.Errorf("capability bound private spaces are not an array")
	}
	bound := map[string]struct{}{}
	for _, raw := range boundSpaces {
		space, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("bound private space is not an object")
		}
		if space["room_id"] != cut["room_id"] {
			return fmt.Errorf("bound private space has another room")
		}
		id, ok := space["space_id"].(string)
		if !ok {
			return fmt.Errorf("bound private space id is not a string")
		}
		if _, duplicate := bound[id]; duplicate {
			return fmt.Errorf("duplicate bound private space %q", id)
		}
		bound[id] = struct{}{}
	}
	if !reflect.DeepEqual(frozenPrivate, bound) {
		return fmt.Errorf("capability does not exactly cover frozen private spaces")
	}
	return nil
}

func canonicalPair(left, right any) string {
	encoded, _ := json.Marshal([]any{left, right})
	return string(encoded)
}

func validateState(instance any, machine map[string]any, reasonCodes map[string]struct{}) error {
	fixture, ok := instance.(map[string]any)
	if !ok {
		return fmt.Errorf("state fixture is not an object")
	}
	machineID, _ := machine["machine_id"].(string)
	if fixtureMachine, _ := fixture["machine"].(string); fixtureMachine != machineID {
		return fmt.Errorf("fixture machine %q does not match %q", fixtureMachine, machineID)
	}

	transitions, ok := machine["transitions"].([]any)
	if !ok {
		return fmt.Errorf("state machine transitions are not an array")
	}
	for index, rawTransition := range transitions {
		transition, ok := rawTransition.(map[string]any)
		if !ok {
			return fmt.Errorf("transition %d is not an object", index)
		}
		if reason, present := transition["reason"]; present {
			code, ok := reason.(string)
			if !ok {
				return fmt.Errorf("transition %d reason is not a string", index)
			}
			if _, registered := reasonCodes[code]; !registered {
				return fmt.Errorf("transition %d references unknown reason %q", index, code)
			}
		}
	}
	sequence, ok := fixture["sequence"].([]any)
	if !ok {
		return fmt.Errorf("state fixture sequence is not an array")
	}
	var current string
	for index, rawStep := range sequence {
		step, ok := rawStep.(map[string]any)
		if !ok {
			return fmt.Errorf("sequence step %d is not an object", index)
		}
		from, _ := step["from"].(string)
		to, _ := step["to"].(string)
		if index > 0 && from != current {
			return fmt.Errorf("step %d starts at %q, want %q", index, from, current)
		}
		if !hasTransition(transitions, from, to) {
			return fmt.Errorf("step %d transition %q -> %q is illegal", index, from, to)
		}
		current = to
	}
	return nil
}

func hasTransition(transitions []any, from, to string) bool {
	for _, raw := range transitions {
		transition, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if stringOrListContains(transition["from"], from) && stringOrListContains(transition["to"], to) {
			return true
		}
	}
	return false
}

func stringOrListContains(value any, want string) bool {
	switch typed := value.(type) {
	case string:
		return typed == want
	case []any:
		for _, item := range typed {
			if item, ok := item.(string); ok && item == want {
				return true
			}
		}
	}
	return false
}

func validateSchema(instance any, schema, root map[string]any) error {
	if ref, ok := schema["$ref"].(string); ok {
		resolved, err := resolveLocalReference(root, ref)
		if err != nil {
			return err
		}
		return validateSchema(instance, resolved, root)
	}
	if typeValue, ok := schema["type"]; ok && !matchesType(instance, typeValue) {
		return fmt.Errorf("type mismatch: got %T, want %v", instance, typeValue)
	}
	if constant, ok := schema["const"]; ok && !reflect.DeepEqual(instance, constant) {
		return fmt.Errorf("const mismatch")
	}
	if values, ok := schema["enum"].([]any); ok && !containsJSONValue(values, instance) {
		return fmt.Errorf("enum mismatch")
	}
	if pattern, ok := schema["pattern"].(string); ok {
		value, ok := instance.(string)
		if !ok {
			return fmt.Errorf("pattern requires a string")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("compile pattern %q: %w", pattern, err)
		}
		if !re.MatchString(value) {
			return fmt.Errorf("pattern mismatch")
		}
	}
	if minLength, ok := number(schema["minLength"]); ok {
		value, ok := instance.(string)
		if !ok || float64(len(value)) < minLength {
			return fmt.Errorf("minLength mismatch")
		}
	}
	if minimum, ok := number(schema["minimum"]); ok {
		value, ok := number(instance)
		if !ok || value < minimum {
			return fmt.Errorf("minimum mismatch")
		}
	}
	if format, ok := schema["format"].(string); ok && format == "date-time" {
		value, ok := instance.(string)
		if !ok {
			return fmt.Errorf("date-time requires a string")
		}
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			return fmt.Errorf("invalid date-time: %w", err)
		}
	}
	if allOf, ok := schema["allOf"].([]any); ok {
		for _, raw := range allOf {
			child, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("allOf child is not an object")
			}
			if err := validateSchema(instance, child, root); err != nil {
				return err
			}
		}
	}
	if oneOf, ok := schema["oneOf"].([]any); ok {
		matches := 0
		for _, raw := range oneOf {
			child, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("oneOf child is not an object")
			}
			if validateSchema(instance, child, root) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("oneOf mismatch: matched %d alternatives", matches)
		}
	}
	if condition, ok := schema["if"].(map[string]any); ok && validateSchema(instance, condition, root) == nil {
		if consequent, ok := schema["then"].(map[string]any); ok {
			if err := validateSchema(instance, consequent, root); err != nil {
				return err
			}
		}
	}
	if object, ok := instance.(map[string]any); ok {
		if required, ok := schema["required"].([]any); ok {
			for _, raw := range required {
				key, ok := raw.(string)
				if !ok {
					return fmt.Errorf("required key is not a string")
				}
				if _, found := object[key]; !found {
					return fmt.Errorf("missing required property %q", key)
				}
			}
		}
		properties, _ := schema["properties"].(map[string]any)
		if additional, ok := schema["additionalProperties"].(bool); ok && !additional {
			for key := range object {
				if _, known := properties[key]; !known {
					return fmt.Errorf("unknown property %q", key)
				}
			}
		}
		for key, rawChild := range properties {
			value, present := object[key]
			if !present {
				continue
			}
			child, ok := rawChild.(map[string]any)
			if !ok {
				return fmt.Errorf("property schema %q is not an object", key)
			}
			if err := validateSchema(value, child, root); err != nil {
				return fmt.Errorf("property %q: %w", key, err)
			}
		}
	}
	if array, ok := instance.([]any); ok {
		if minItems, ok := number(schema["minItems"]); ok && float64(len(array)) < minItems {
			return fmt.Errorf("minItems mismatch")
		}
		if rawItems, exists := schema["items"]; exists {
			items, ok := rawItems.(map[string]any)
			if !ok {
				return fmt.Errorf("items schema is not an object")
			}
			for index, value := range array {
				if err := validateSchema(value, items, root); err != nil {
					return fmt.Errorf("item %d: %w", index, err)
				}
			}
		}
	}
	return nil
}

func resolveLocalReference(root map[string]any, reference string) (map[string]any, error) {
	if !strings.HasPrefix(reference, "#/") {
		return nil, fmt.Errorf("unsupported reference %q", reference)
	}
	var current any = root
	for _, segment := range strings.Split(strings.TrimPrefix(reference, "#/"), "/") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("reference %q traverses non-object", reference)
		}
		current, ok = object[strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")]
		if !ok {
			return nil, fmt.Errorf("reference %q not found", reference)
		}
	}
	resolved, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("reference %q does not resolve to an object", reference)
	}
	return resolved, nil
}

func matchesType(instance any, rawType any) bool {
	if types, ok := rawType.([]any); ok {
		for _, raw := range types {
			if name, ok := raw.(string); ok && matchesType(instance, name) {
				return true
			}
		}
		return false
	}
	name, ok := rawType.(string)
	if !ok {
		return false
	}
	switch name {
	case "object":
		_, ok := instance.(map[string]any)
		return ok
	case "array":
		_, ok := instance.([]any)
		return ok
	case "string":
		_, ok := instance.(string)
		return ok
	case "boolean":
		_, ok := instance.(bool)
		return ok
	case "null":
		return instance == nil
	case "number":
		_, ok := number(instance)
		return ok
	case "integer":
		value, ok := number(instance)
		return ok && math.Trunc(value) == value
	default:
		return false
	}
}

func number(value any) (float64, bool) {
	number, ok := value.(float64)
	return number, ok
}

func containsJSONValue(values []any, want any) bool {
	for _, value := range values {
		if reflect.DeepEqual(value, want) {
			return true
		}
	}
	return false
}
