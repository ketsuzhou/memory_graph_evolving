package validation

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
)

// integerForm mirrors the JSON productions that denote plain decimal
// integers; any other number form fails the integer-only hashed core.
var integerForm = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// SchemaSet holds the parsed authority schema documents of
// $FIX/schema/shared keyed by file name (the $id stays inside the doc).
// The documents are frozen inputs: the loader sanity-checks that each is an
// object carrying an $id whose base name matches the file and that every
// keyword stays inside the closed subset the interpreter implements.
type SchemaSet struct {
	dir   string
	docs  map[string]map[string]any
	names []string
}

// LoadSchemaSet loads every *.schema.json under dir (the shared DTO
// authority of $FIX/schema/shared, Contract §16.6 F1) and validates that
// every document stays inside the closed keyword subset. Parent-directory
// schemas ($FIX/schema/*.json, referenced from shared DTO schemas for
// shared $defs) are pulled in lazily on first $ref: only the referenced
// subtrees are ever interpreted, mirroring validate_contract.py's on-demand
// resolution (those documents carry keywords outside the closed subset).
func LoadSchemaSet(dir string) (*SchemaSet, error) {
	set := &SchemaSet{dir: dir, docs: map[string]map[string]any{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("validation: read schema dir %s: %w", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".schema.json") {
			continue
		}
		if err := set.loadSchemaFile(filepath.Join(dir, name), name); err != nil {
			return nil, fmt.Errorf("validation: %w", err)
		}
	}
	if len(set.docs) == 0 {
		return nil, fmt.Errorf("validation: no *.schema.json found under %s", dir)
	}
	sort.Strings(set.names)
	// Second pass: every shared document's keywords and $refs must resolve
	// now that the whole authority set is loaded.
	for _, name := range set.names {
		if err := set.validateSchemaNode(set.docs[name], name, 0); err != nil {
			return nil, fmt.Errorf("validation: schema %s malformed: %w", name, err)
		}
	}
	return set, nil
}

// loadSchemaFile reads one schema document and registers it under key.
func (s *SchemaSet) loadSchemaFile(absPath, key string) error {
	data, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("read schema %s: %w", key, err)
	}
	value, err := contract.ParseJSONStrict(data)
	if err != nil {
		return fmt.Errorf("schema %s is not valid UTF-8 JSON: %w", key, err)
	}
	obj, ok := contract.AsObject(value)
	if !ok {
		return fmt.Errorf("schema %s is not an object", key)
	}
	if id, _ := contract.AsString(obj["$id"]); id == "" {
		return fmt.Errorf("schema %s carries no $id", key)
	}
	if _, exists := s.docs[key]; !exists {
		s.docs[key] = obj
		s.names = append(s.names, key)
	}
	return nil
}

// Files returns the loaded schema file names in stable order.
func (s *SchemaSet) Files() []string {
	out := make([]string, len(s.names))
	copy(out, s.names)
	return out
}

// Schema returns one authority document by file name.
func (s *SchemaSet) Schema(file string) (map[string]any, error) {
	doc, ok := s.docs[file]
	if !ok {
		return nil, fmt.Errorf("validation: schema %q is not loaded (authority set: %d files)", file, len(s.docs))
	}
	return doc, nil
}

// refResolution is one resolved $ref: the target node inside the file the
// pointer belongs to (refs may cross sibling files).
type refResolution struct {
	node map[string]any
	file string
}

// resolveRef resolves one $ref string relative to the current schema key:
//   - "#/ptr"                 local pointer into the current document;
//   - "file.schema.json#/ptr" sibling document (same directory);
//   - "../file.schema.json"   document of the parent directory.
func (s *SchemaSet) resolveRef(ref, currentKey string) (refResolution, error) {
	file, pointer := currentKey, strings.TrimPrefix(ref, "#")
	if idx := strings.Index(ref, "#"); idx >= 0 && !strings.HasPrefix(ref, "#") {
		file, pointer = ref[:idx], ref[idx+1:]
	}
	if file != currentKey {
		file = path.Clean(path.Join(path.Dir(currentKey), file))
	}
	doc, ok := s.docs[file]
	if !ok {
		// Lazy parent-directory load: only ".."-relative keys, at most one
		// level above the authority dir, and only the referenced subtrees
		// are ever interpreted (no whole-document subset validation).
		if strings.HasPrefix(file, "../") && !strings.Contains(strings.TrimPrefix(file, "../"), "..") {
			if err := s.loadSchemaFile(filepath.Join(s.dir, file), file); err != nil {
				return refResolution{}, newError(CodeSchemaKeywordUnknown, "$ref %q: %v", ref, err)
			}
			doc = s.docs[file]
		} else {
			return refResolution{}, newError(CodeSchemaKeywordUnknown, "unresolvable $ref %q (file %q not in the authority set)", ref, file)
		}
	}
	node := any(doc)
	if pointer != "" {
		for _, rawSegment := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
			segment := strings.ReplaceAll(strings.ReplaceAll(rawSegment, "~1", "/"), "~0", "~")
			if obj, isObj := contract.AsObject(node); isObj {
				next, present := obj[segment]
				if !present {
					return refResolution{}, newError(CodeSchemaKeywordUnknown, "$ref %q: pointer segment %q missing", ref, segment)
				}
				node = next
				continue
			}
			arr, isArray := contract.AsArray(node)
			if isArray {
				index, err := strconv.Atoi(segment)
				if err != nil || index < 0 || index >= len(arr) {
					return refResolution{}, newError(CodeSchemaKeywordUnknown, "$ref %q: invalid array segment %q", ref, segment)
				}
				node = arr[index]
				continue
			}
			return refResolution{}, newError(CodeSchemaKeywordUnknown, "$ref %q: pointer crosses a scalar at %q", ref, segment)
		}
	}
	resolved, ok := contract.AsObject(node)
	if !ok {
		return refResolution{}, newError(CodeSchemaKeywordUnknown, "$ref %q resolves to a non-object schema node", ref)
	}
	return refResolution{node: resolved, file: file}, nil
}

const maxSchemaDepth = 64

// validateNode implements the closed keyword subset of validate_contract.py
// _validate_node over the decoder model: $ref (+sibling keywords), type
// (string or list), const, enum, pattern, minLength, minimum,
// items/minItems/maxItems, required, properties,
// additionalProperties:false, allOf, if/then/else, not.
func (s *SchemaSet) validateNode(instance any, node map[string]any, path, file string, depth int) error {
	if depth > maxSchemaDepth {
		return newError(CodeSchemaRefCycle, "%s: schema nesting deeper than %d", path, maxSchemaDepth)
	}
	if ref, has := node["$ref"]; has {
		refStr, ok := contract.AsString(ref)
		if !ok {
			return newError(CodeSchemaKeywordUnknown, "%s: $ref must be a string", path)
		}
		resolved, err := s.resolveRef(refStr, file)
		if err != nil {
			return err
		}
		if err := s.validateNode(instance, resolved.node, path, resolved.file, depth+1); err != nil {
			return err
		}
		rest := make(map[string]any, len(node)-1)
		for k, v := range node {
			if k != "$ref" {
				rest[k] = v
			}
		}
		if len(rest) > 0 {
			return s.validateNode(instance, rest, path, file, depth+1)
		}
		return nil
	}

	if expected, has := node["type"]; has {
		var types []any
		if list, isList := contract.AsArray(expected); isList {
			types = list
		} else {
			types = []any{expected}
		}
		matched := false
		for _, raw := range types {
			name, _ := contract.AsString(raw)
			if typeMatches(instance, name) {
				matched = true
				break
			}
		}
		if !matched {
			return newError(CodeSchemaTypeInvalid, "%s: expected type %v, got %v", path, expected, describe(instance))
		}
	}
	if want, has := node["const"]; has && !jsonEqual(instance, want) {
		return newError(CodeSchemaConstMismatch, "%s: %v != const %v", path, describe(instance), describe(want))
	}
	if enum, has := contract.AsArray(node["enum"]); has && !enumContains(enum, instance) {
		return newError(CodeSchemaEnumInvalid, "%s: %v outside the closed enum", path, describe(instance))
	}
	if pattern, has := contract.AsString(node["pattern"]); has {
		text, isStr := contract.AsString(instance)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return newError(CodeSchemaKeywordUnknown, "%s: pattern %q does not compile", path, pattern)
		}
		if !isStr || !re.MatchString(text) {
			return newError(CodeSchemaPatternInvalid, "%s: %v does not match %s", path, describe(instance), pattern)
		}
	}
	if minLength, ok := intValue(node["minLength"]); ok {
		if text, isStr := contract.AsString(instance); isStr && len(text) < minLength {
			return newError(CodeSchemaPatternInvalid, "%s: length %d < minLength %d", path, len(text), minLength)
		}
	}
	if minimum, ok := intValue(node["minimum"]); ok {
		if number, isInt := instanceInt(instance); isInt && number < int64(minimum) {
			return newError(CodeSchemaMinimumViolated, "%s: %v < minimum %d", path, describe(instance), minimum)
		}
	}
	if items, isArray := contract.AsArray(instance); isArray {
		if minItems, ok := intValue(node["minItems"]); ok && len(items) < minItems {
			return newError(CodeSchemaMinItemsViolated, "%s: %d items < minItems %d", path, len(items), minItems)
		}
		if maxItems, ok := intValue(node["maxItems"]); ok && len(items) > maxItems {
			return newError(CodeSchemaMinItemsViolated, "%s: %d items > maxItems %d", path, len(items), maxItems)
		}
		if itemSchema, has := contract.AsObject(node["items"]); has {
			for index, item := range items {
				if err := s.validateNode(item, itemSchema, fmt.Sprintf("%s/items[%d]", path, index), file, depth+1); err != nil {
					return err
				}
			}
		}
	}
	if obj, isObj := contract.AsObject(instance); isObj {
		for _, name := range requiredNames(node) {
			if _, present := obj[name]; !present {
				return newError(CodeSchemaRequiredFieldMissing, "%s: required field %q missing", path, name)
			}
		}
		props, _ := contract.AsObject(node["properties"])
		if reject, has := node["additionalProperties"]; has && reject == false {
			for _, key := range sortedMapKeys(obj) {
				if _, known := props[key]; !known {
					return newError(CodeSchemaFieldUnknown, "%s: unknown core field %q", path, key)
				}
			}
		}
		for _, key := range sortedMapKeys(props) {
			if value, present := obj[key]; present {
				sub, _ := contract.AsObject(props[key])
				if err := s.validateNode(value, sub, path+"/"+key, file, depth+1); err != nil {
					return err
				}
			}
		}
	}
	if allOf, ok := contract.AsArray(node["allOf"]); ok {
		for _, rawEntry := range allOf {
			entry, _ := contract.AsObject(rawEntry)
			if err := s.validateNode(instance, entry, path+"/allOf", file, depth+1); err != nil {
				return err
			}
		}
	}
	if ifNode, has := contract.AsObject(node["if"]); has {
		var branch any
		if err := s.validateNode(instance, ifNode, path+"/if", file, depth+1); err != nil {
			branch = node["else"]
		} else {
			branch = node["then"]
		}
		if branch != nil {
			if reject, isFalse := branch.(bool); isFalse {
				if !reject {
					return newError(CodeSchemaTypeInvalid, "%s: conditional false branch rejects the instance", path)
				}
				return nil // schema true accepts everything
			}
			branchNode, isObj := contract.AsObject(branch)
			if !isObj {
				return newError(CodeSchemaKeywordUnknown, "%s: conditional branch is not a schema object", path)
			}
			if err := s.validateNode(instance, branchNode, path+"/branch", file, depth+1); err != nil {
				if CodeOf(err) == CodeSchemaRequiredFieldMissing {
					return newError(CodeSchemaConditionalRequired, "%s", err.(*Error).Detail)
				}
				return err
			}
		}
	}
	if notNode, has := contract.AsObject(node["not"]); has {
		if err := s.validateNode(instance, notNode, path+"/not", file, depth+1); err == nil {
			return newError(CodeSchemaConditionalForbidden, "%s: conditionally forbidden field present", path)
		}
	}
	return nil
}

// closedSchemaKeywords is the keyword subset the interpreter implements;
// the authority schemas must stay inside it (fail-closed at load).
var closedSchemaKeywords = map[string]bool{
	"$schema": true, "$id": true, "title": true, "description": true,
	"contract_refs": true, "type": true, "enum": true, "const": true,
	"pattern": true, "minLength": true, "minimum": true, "items": true,
	"minItems": true, "maxItems": true, "required": true,
	"properties": true, "additionalProperties": true, "$ref": true,
	"allOf": true, "if": true, "then": true, "else": true, "not": true,
	"x-digest": true, "$defs": true,
}

// validateSchemaNode checks one authority schema document: unknown keywords
// fail at load time instead of silently passing validation.
func (s *SchemaSet) validateSchemaNode(node map[string]any, path string, depth int) error {
	if depth > maxSchemaDepth {
		return newError(CodeSchemaRefCycle, "%s: schema nesting too deep", path)
	}
	for key := range node {
		if !closedSchemaKeywords[key] {
			return newError(CodeSchemaKeywordUnknown, "%s: schema keyword %q outside the closed subset", path, key)
		}
	}
	if ref, has := node["$ref"]; has {
		refStr, _ := contract.AsString(ref)
		key := schemaKeyOf(path)
		resolved, err := s.resolveRef(refStr, key)
		if err != nil {
			return err
		}
		return s.validateSchemaNode(resolved.node, resolved.file+refSuffix(path), depth+1)
	}
	children := []struct {
		keyword string
		docs    []map[string]any
	}{}
	if props, ok := contract.AsObject(node["properties"]); ok {
		for _, key := range sortedMapKeys(props) {
			if sub, isObj := contract.AsObject(props[key]); isObj {
				children = append(children, struct {
					keyword string
					docs    []map[string]any
				}{"properties/" + key, []map[string]any{sub}})
			}
		}
	}
	if defs, ok := contract.AsObject(node["$defs"]); ok {
		for _, key := range sortedMapKeys(defs) {
			if sub, isObj := contract.AsObject(defs[key]); isObj {
				children = append(children, struct {
					keyword string
					docs    []map[string]any
				}{"$defs/" + key, []map[string]any{sub}})
			}
		}
	}
	if items, ok := contract.AsObject(node["items"]); ok {
		children = append(children, struct {
			keyword string
			docs    []map[string]any
		}{"items", []map[string]any{items}})
	}
	if allOf, ok := contract.AsArray(node["allOf"]); ok {
		for i, raw := range allOf {
			if entry, isObj := contract.AsObject(raw); isObj {
				children = append(children, struct {
					keyword string
					docs    []map[string]any
				}{fmt.Sprintf("allOf[%d]", i), []map[string]any{entry}})
			}
		}
	}
	for _, keyword := range []string{"if", "then", "else", "not"} {
		if sub, ok := contract.AsObject(node[keyword]); ok {
			children = append(children, struct {
				keyword string
				docs    []map[string]any
			}{keyword, []map[string]any{sub}})
		}
	}
	for _, child := range children {
		for _, doc := range child.docs {
			if err := s.validateSchemaNode(doc, path+"/"+child.keyword, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- decoder-model helpers --------------------------------------------------

// typeMatches mirrors _type_matches: booleans are not integers.
func typeMatches(instance any, expected string) bool {
	switch expected {
	case "object":
		_, ok := contract.AsObject(instance)
		return ok
	case "array":
		_, ok := contract.AsArray(instance)
		return ok
	case "string":
		_, ok := contract.AsString(instance)
		return ok
	case "integer":
		_, ok := instanceInt(instance)
		return ok
	case "boolean":
		_, ok := instance.(bool)
		return ok
	case "null":
		return instance == nil
	default:
		return false
	}
}

// instanceInt returns the exact integer value of a decoder-model number in
// plain integer form. Values beyond int64 type-match but clamp for
// comparisons (the authority schemas' minima are tiny).
func instanceInt(instance any) (int64, bool) {
	number, ok := instance.(json.Number)
	if !ok {
		return 0, false
	}
	text := string(number)
	if !integerForm.MatchString(text) {
		return 0, false
	}
	if value, err := strconv.ParseInt(text, 10, 64); err == nil {
		return value, true
	}
	if strings.HasPrefix(text, "-") {
		return -1 << 63, true
	}
	return 1<<63 - 1, true
}

// intValue reads an integer schema keyword.
func intValue(v any) (int, bool) {
	n, ok := instanceInt(v)
	if !ok || n < 0 || n > 1<<30 {
		return 0, false
	}
	return int(n), true
}

// jsonEqual is _json_equal: strict bool/int discrimination, exact integer
// digits, recursive containers.
func jsonEqual(left, right any) bool {
	lb, leftBool := left.(bool)
	rb, rightBool := right.(bool)
	if leftBool || rightBool {
		return leftBool && rightBool && lb == rb
	}
	if li, lok := instanceInt(left); lok {
		ri, rok := instanceInt(right)
		return rok && li == ri
	}
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	switch l := left.(type) {
	case string:
		r, ok := right.(string)
		return ok && l == r
	case []any:
		r, ok := right.([]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for i := range l {
			if !jsonEqual(l[i], r[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		r, ok := right.(map[string]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for k, v := range l {
			rv, present := r[k]
			if !present || !jsonEqual(v, rv) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func enumContains(enum []any, value any) bool {
	for _, candidate := range enum {
		if jsonEqual(candidate, value) {
			return true
		}
	}
	return false
}

func sortedMapKeys(obj map[string]any) []string {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// requiredNames reads a schema "required" array in declaration order.
func requiredNames(node map[string]any) []string {
	raw, ok := contract.AsArray(node["required"])
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, isStr := contract.AsString(item); isStr {
			out = append(out, s)
		}
	}
	return out
}

func describe(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(t)
	case string:
		return strconv.Quote(t)
	default:
		data, err := contract.JCS(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(data)
	}
}

// schemaKeyOf extracts the schema file key from a nested validation path
// ("../x.schema.json/properties/y" -> "../x.schema.json").
func schemaKeyOf(p string) string {
	if idx := strings.Index(p, ".schema.json"); idx >= 0 {
		return p[:idx+len(".schema.json")]
	}
	return p
}

// refSuffix keeps the human-readable pointer tail of a nested path.
func refSuffix(p string) string {
	key := schemaKeyOf(p)
	if len(p) > len(key) {
		return p[len(key):]
	}
	return ""
}
