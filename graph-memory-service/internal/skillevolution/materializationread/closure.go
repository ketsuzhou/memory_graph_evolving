// closure.go is the closure value model of the GMS-207 exact materialization
// read (Contract §8.4/§13.7, GMS §12.7, RSIH §4.2): the canonically ordered
// transitive closure of exact skill refs, each node carrying the frozen
// canonical bytes, ports, permissions and dependency edges RSIH needs to
// render and freeze a bundle. GMS supplies these inputs only — it never
// mints manifest/lock identity (Contract §6.2.1: RSIH's materializer owns
// that).
package materializationread

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
)

// SchemaClosureRead is the private response envelope schema version of the
// closure read (GMS §11.1 logical capability; the DTO surface is private to
// GMS — shared DTOs stay untouched).
const SchemaClosureRead = "gms.materialization-closure-read.v1"

// SchemaTornReadToken is the preimage schema of the torn-read token.
const SchemaTornReadToken = "gms.closure-torn-read-token.v1"

// Permission is one envelope permission entry (Contract §8.1).
type Permission struct {
	Capability string
	Scope      string
}

// PortSchemaRef is the Contract §7.2 JsonSchemaRef shape of one named port
// schema ({schema_id, version, digest}).
type PortSchemaRef struct {
	SchemaID string
	Version  string
	Digest   string
}

// SkillRefDoc renders one exact §7.3 ref back to its wire document.
func SkillRefDoc(ref contract.SkillArtifactRef) map[string]any {
	return map[string]any{
		"schema_version":  ref.SchemaVersion,
		"lineage_id":      ref.LineageID,
		"version":         json.Number(ref.Version),
		"kind":            ref.Kind,
		"artifact_digest": ref.ArtifactDigest,
	}
}

// PortSchemaDoc renders one port schema ref to its wire document.
func PortSchemaDoc(port *PortSchemaRef) any {
	if port == nil {
		return nil
	}
	return map[string]any{
		"schema_id": port.SchemaID,
		"version":   json.Number(port.Version),
		"digest":    port.Digest,
	}
}

// ClosureNode is one node of the ordered closure: the exact ref, its frozen
// canonical envelope bytes (digesting to the ref's artifact_digest), the
// port schemas, the envelope permissions and the explicit dependency edges
// (composite children in canonical order; leaves carry none).
type ClosureNode struct {
	Ref            contract.SkillArtifactRef
	Kind           string
	ArtifactDigest string
	CanonicalBytes []byte
	InputPort      *PortSchemaRef
	OutputPort     *PortSchemaRef
	Permissions    []Permission
	Dependencies   []contract.SkillArtifactRef
}

// Closure is the deterministic result of one closure read.
type Closure struct {
	SchemaVersion string
	// Roots are the requested roots in canonical order (each verified to be
	// the current authoritative active head at read time).
	Roots []contract.SkillArtifactRef
	// Nodes is the closure in DFS preorder from the canonically ordered
	// roots, children visited in canonical order, shared subtrees emitted
	// exactly once. The order depends only on the closure content — never on
	// response arrival, map iteration or filesystem order (RSIH §4.2).
	Nodes []ClosureNode
	// ActivationSequence is the global activation stream position observed
	// for the read: the freeze sequence RSIH binds into the manifest
	// (created_from_activation_sequence) and the SkillLock
	// (activation_sequence_at_freeze).
	ActivationSequence uint64
	// ActivationHeadDigest is the ledger payload digest of the activation
	// stream head the read double-checked.
	ActivationHeadDigest string
	// TornReadToken binds (activation sequence, head digest, roots, ordered
	// closure refs) into one digest; the same sequence yields the same token.
	TornReadToken string
	// Watermark is the then-visible Contract §7.14 ProjectionWatermark,
	// carried as INFORMATION only (GMS §11.1): the read never waits for the
	// Graph to catch up and never lets the watermark substitute the
	// authoritative active heads. nil when no projection source is wired.
	Watermark map[string]any
}

// ClosureRefDocs renders the ordered closure refs as wire documents (used by
// the HTTP adapter and the torn-read token preimage).
func (c *Closure) ClosureRefDocs() []any {
	out := make([]any, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		out = append(out, SkillRefDoc(node.Ref))
	}
	return out
}

// RootRefDocs renders the canonically ordered roots as wire documents.
func (c *Closure) RootRefDocs() []any {
	out := make([]any, 0, len(c.Roots))
	for _, ref := range c.Roots {
		out = append(out, SkillRefDoc(ref))
	}
	return out
}

// TornReadToken computes the double-check token preimage digest:
// SHA-256(JCS({schema_version, activation_sequence, activation_head_digest,
// root_refs, closure_refs})) — every field in canonical (lineage, version,
// digest) order so the token is a pure function of the closure content.
func TornReadToken(sequence uint64, headDigest string, roots []contract.SkillArtifactRef, nodes []ClosureNode) (string, error) {
	rootDocs := make([]any, 0, len(roots))
	for _, ref := range roots {
		rootDocs = append(rootDocs, SkillRefDoc(ref))
	}
	closureDocs := make([]any, 0, len(nodes))
	for _, node := range nodes {
		closureDocs = append(closureDocs, SkillRefDoc(node.Ref))
	}
	return contract.DigestOf(map[string]any{
		"schema_version":         SchemaTornReadToken,
		"activation_sequence":    json.Number(strconv.FormatUint(sequence, 10)),
		"activation_head_digest": headDigest,
		"root_refs":              rootDocs,
		"closure_refs":           closureDocs,
	})
}

// CompareRefs is the canonical §6.3 exact-ref order: lineage_id UTF-8 bytes,
// then version as an exact integer (beyond 2^53), then artifact_digest. It is
// the single ordering authority of the closure walk.
func CompareRefs(a, b contract.SkillArtifactRef) int {
	if c := strings.Compare(a.LineageID, b.LineageID); c != 0 {
		return c
	}
	av, aOK := a.VersionInt()
	bv, bOK := b.VersionInt()
	if aOK && bOK && av.Cmp(bv) != 0 {
		return av.Cmp(bv)
	}
	if c := strings.Compare(a.Version, b.Version); c != 0 {
		return c
	}
	return strings.Compare(a.ArtifactDigest, b.ArtifactDigest)
}

// SortRefs orders refs canonically in place.
func SortRefs(refs []contract.SkillArtifactRef) {
	sort.Slice(refs, func(i, j int) bool { return CompareRefs(refs[i], refs[j]) < 0 })
}

// refKey is the exact identity of one ref for map keys.
func refKey(ref contract.SkillArtifactRef) string {
	return ref.LineageID + "\x1f" + ref.Version + "\x1f" + ref.ArtifactDigest
}

// versionOfExact reads an integer field in exact decimal form.
func versionOfExact(v any) (string, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return "", false
	}
	text := string(n)
	z, ok := new(big.Int).SetString(text, 10)
	if !ok || z.Sign() < 1 || !strings.HasPrefix(text, z.String()) {
		return "", false
	}
	return text, true
}

// portRefOf reads one {schema_id, version, digest} port schema ref from a
// kind body (nil when absent).
func portRefOf(raw any) (*PortSchemaRef, error) {
	if raw == nil {
		return nil, nil
	}
	obj, ok := contract.AsObject(raw)
	if !ok {
		return nil, fmt.Errorf("port schema must be an object")
	}
	schemaID, _ := contract.AsString(obj["schema_id"])
	version, vOK := versionOfExact(obj["version"])
	digest, _ := contract.AsString(obj["digest"])
	if schemaID == "" || !vOK || !strings.HasPrefix(digest, "sha256:") {
		return nil, fmt.Errorf("port schema ref is not a well-formed JsonSchemaRef")
	}
	return &PortSchemaRef{SchemaID: schemaID, Version: version, Digest: digest}, nil
}

// permissionsOf reads the envelope permission entries in declaration order.
func permissionsOf(envelope map[string]any) []Permission {
	entries, _ := contract.AsArray(envelope["permissions"])
	out := make([]Permission, 0, len(entries))
	for _, raw := range entries {
		entry, _ := contract.AsObject(raw)
		capability, _ := contract.AsString(entry["capability"])
		scope, _ := contract.AsString(entry["scope"])
		out = append(out, Permission{Capability: capability, Scope: scope})
	}
	return out
}
