package validation

import (
	"encoding/json"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
)

// HostCapabilityCapV1 is the v1 Host authority capability cap (Contract
// §7.17 tool surface / §14.2): any permission capability outside it fails
// with PERMISSION_CAP_EXCEEDED.
var HostCapabilityCapV1 = map[string]bool{
	"memory_explore": true,
	"memory_expand":  true,
	"skill_get":      true,
}

// Gates is the shared static-gate engine: schema shape from the authority
// set, the integer-only hashed core, x-digest preimage recomputation, the
// extension whitelist policy and the permission cap. It mirrors
// validate_contract.py validate_instance for every DTO case.
type Gates struct {
	schemas *SchemaSet
}

// NewGates wires the gate engine to a loaded authority schema set.
func NewGates(schemas *SchemaSet) (*Gates, error) {
	if schemas == nil || len(schemas.docs) == 0 {
		return nil, fmt.Errorf("validation: nil or empty schema authority set")
	}
	return &Gates{schemas: schemas}, nil
}

// SchemaSet exposes the authority set (read-only use).
func (g *Gates) SchemaSet() *SchemaSet { return g.schemas }

// ValidateInstance runs the full closed pipeline over one instance against
// the named authority schema (validate_contract.py validate_instance):
//
//  1. every JSON number in the instance must be a plain integer
//     (NON_INTEGER_NUMBER — the hashed core is integer-only);
//  2. shape validation via the closed keyword subset (SCHEMA_* codes),
//     honoring a root $ref;
//  3. the root schema's x-digest preimage recomputation (DIGEST_MISMATCH);
//  4. the extensions gate (UNKNOWN_REQUIRED_EXTENSION for unknown
//     required:true extensions; unknown optional extensions are ignored).
func (g *Gates) ValidateInstance(instance any, schemaFile string) error {
	if err := checkFloats(instance, "instance"); err != nil {
		return err
	}
	doc, err := g.schemas.Schema(schemaFile)
	if err != nil {
		return err
	}
	node, file := doc, schemaFile
	if ref, has := doc["$ref"]; has {
		refStr, _ := contract.AsString(ref)
		resolved, err := g.schemas.resolveRef(refStr, schemaFile)
		if err != nil {
			return err
		}
		node, file = resolved.node, resolved.file
	}
	if err := g.schemas.validateNode(instance, node, schemaFile, file, 0); err != nil {
		return err
	}
	if obj, isObj := contract.AsObject(instance); isObj {
		if meta, ok := contract.AsObject(doc["x-digest"]); ok {
			if err := g.checkDigestMeta(obj, meta); err != nil {
				return err
			}
		}
		if raw, present := obj["extensions"]; present {
			if err := g.CheckExtensions(raw); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateShape runs the closed pipeline WITHOUT the root x-digest
// recomputation: integer-only core, shape (closed keyword subset, root
// $ref honored) and the extensions gate. This mirrors the merge-category
// semantics of validate.py, where the corpus verdict is derived from the
// semantic overlays plus whole-document canonicalization and the DTO's
// internal x-digest fields are corpus data rather than recomputed
// invariants (the merge fixtures' declared assessment/proposal digests do
// not recompute under the plain preimage rule).
func (g *Gates) ValidateShape(instance any, schemaFile string) error {
	if err := checkFloats(instance, "instance"); err != nil {
		return err
	}
	doc, err := g.schemas.Schema(schemaFile)
	if err != nil {
		return err
	}
	node, file := doc, schemaFile
	if ref, has := doc["$ref"]; has {
		refStr, _ := contract.AsString(ref)
		resolved, err := g.schemas.resolveRef(refStr, schemaFile)
		if err != nil {
			return err
		}
		node, file = resolved.node, resolved.file
	}
	if err := g.schemas.validateNode(instance, node, schemaFile, file, 0); err != nil {
		return err
	}
	if obj, isObj := contract.AsObject(instance); isObj {
		if raw, present := obj["extensions"]; present {
			if err := g.CheckExtensions(raw); err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckExtensions applies Contract §6.4: an extension entry with
// required:true whose key is not registered in v1 fails closed; unknown
// optional extensions are ignored.
func (g *Gates) CheckExtensions(extensions any) error {
	obj, ok := contract.AsObject(extensions)
	if !ok {
		return newError(CodeUnknownRequiredExtension, "extensions must be an object")
	}
	for _, key := range sortedMapKeys(obj) {
		entry, isObj := contract.AsObject(obj[key])
		if !isObj {
			continue
		}
		if required, isBool := entry["required"].(bool); isBool && required {
			return newError(CodeUnknownRequiredExtension, "required extension %q is not registered in v1", key)
		}
	}
	return nil
}

// CheckDigestPreimage recomputes the x-digest declared by schemaFile for
// instance and compares it (DIGEST_MISMATCH fail-closed).
func (g *Gates) CheckDigestPreimage(instance map[string]any, schemaFile string) error {
	doc, err := g.schemas.Schema(schemaFile)
	if err != nil {
		return err
	}
	meta, ok := contract.AsObject(doc["x-digest"])
	if !ok {
		return newError(CodeSchemaKeywordUnknown, "schema %s declares no x-digest", schemaFile)
	}
	return g.checkDigestMeta(instance, meta)
}

// ComputeDigestPreimage computes the x-digest of instance per the named
// schema: SHA-256 over the JCS of the declared preimage fields (present
// fields only), never including the digest field or extensions.
func (g *Gates) ComputeDigestPreimage(instance map[string]any, schemaFile string) (string, error) {
	doc, err := g.schemas.Schema(schemaFile)
	if err != nil {
		return "", err
	}
	meta, ok := contract.AsObject(doc["x-digest"])
	if !ok {
		return "", newError(CodeSchemaKeywordUnknown, "schema %s declares no x-digest", schemaFile)
	}
	core := g.preimageCore(instance, meta)
	digest, err := contract.DigestOf(core)
	if err != nil {
		return "", newError(CodeNonIntegerNumber, "digest preimage cannot enter the hashed core: %v", err)
	}
	return digest, nil
}

func (g *Gates) checkDigestMeta(instance, meta map[string]any) error {
	digestField, _ := contract.AsString(meta["digest_field"])
	if digestField == "" {
		return nil
	}
	declaredRaw, present := instance[digestField]
	if !present {
		return nil
	}
	declared, isStr := contract.AsString(declaredRaw)
	if !isStr || !digestShape(declared) {
		return newError(CodeDigestMismatch, "%s malformed", digestField)
	}
	core := g.preimageCore(instance, meta)
	computed, err := contract.DigestOf(core)
	if err != nil {
		return newError(CodeNonIntegerNumber, "digest preimage cannot enter the hashed core: %v", err)
	}
	if computed != declared {
		return newError(CodeDigestMismatch, "%s %s != recomputed %s", digestField, declared, computed)
	}
	return nil
}

func (g *Gates) preimageCore(instance, meta map[string]any) map[string]any {
	fields, _ := contract.AsArray(meta["preimage_fields"])
	core := make(map[string]any, len(fields))
	for _, raw := range fields {
		name, _ := contract.AsString(raw)
		if value, present := instance[name]; present {
			core[name] = value
		}
	}
	return core
}

func digestShape(digest string) bool {
	if len(digest) != 7+64 {
		return false
	}
	if digest[:7] != "sha256:" {
		return false
	}
	for i := 7; i < len(digest); i++ {
		c := digest[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// checkFloats walks the whole value tree first: any non-integer JSON number
// anywhere in the instance fails before shape checks (the pinned order of
// validate_instance, mirrored from _floats_in).
func checkFloats(value any, where string) error {
	switch t := value.(type) {
	case json.Number:
		if !integerForm.MatchString(string(t)) {
			return newError(CodeNonIntegerNumber, "non-integer number %s in %s", string(t), where)
		}
	case map[string]any:
		for _, key := range sortedMapKeys(t) {
			if err := checkFloats(t[key], where+"."+key); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range t {
			if err := checkFloats(item, fmt.Sprintf("%s[%d]", where, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckPermissionEntry validates one Contract §8.1 permission entry against
// the v1 Host authority capability cap: capability and scope must be
// non-empty strings and the capability must be inside the cap
// (PERMISSION_CAP_EXCEEDED otherwise).
func (g *Gates) CheckPermissionEntry(entry any) error {
	obj, ok := contract.AsObject(entry)
	if !ok {
		return newError(CodeSchemaEnumInvalid, "permission entry must be an object")
	}
	capability, okCap := contract.AsString(obj["capability"])
	scope, okScope := contract.AsString(obj["scope"])
	if !okCap || capability == "" {
		return newError(CodeSchemaRequiredFieldMissing, "permission capability must be a non-empty string")
	}
	if !okScope || scope == "" {
		return newError(CodeSchemaRequiredFieldMissing, "permission %q carries no explicit scope (CTR-005 permission profile)", capability)
	}
	if !HostCapabilityCapV1[capability] {
		return newError(CodePermissionCapExceeded, "capability %q is outside the v1 Host authority cap", capability)
	}
	return nil
}

// CheckPermissionList validates an array of permission entries.
func (g *Gates) CheckPermissionList(entries any) error {
	list, ok := contract.AsArray(entries)
	if !ok {
		return newError(CodeSchemaEnumInvalid, "permissions must be an array")
	}
	for i, entry := range list {
		if err := g.CheckPermissionEntry(entry); err != nil {
			if ve, isVE := err.(*Error); isVE {
				ve.Detail = fmt.Sprintf("permissions[%d]: %s", i, ve.Detail)
			}
			return err
		}
	}
	return nil
}

// CheckPermissionsWalked walks a document and validates every
// permissions / orchestration_permissions array under the cap (the S1
// permissionCapIssue semantics, applied by the artifact gate).
func (g *Gates) CheckPermissionsWalked(value any) error {
	switch t := value.(type) {
	case map[string]any:
		for _, key := range []string{"permissions", "orchestration_permissions"} {
			if raw, present := t[key]; present {
				if err := g.CheckPermissionList(raw); err != nil {
					return err
				}
			}
		}
		for _, key := range sortedMapKeys(t) {
			if key == "permissions" || key == "orchestration_permissions" {
				continue
			}
			if err := g.CheckPermissionsWalked(t[key]); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range t {
			if err := g.CheckPermissionsWalked(item); err != nil {
				return err
			}
		}
	}
	return nil
}
