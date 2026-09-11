package contract

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
)

// Closed reason-code registry loader (Contract §13.7/§13.7.1).
//
// Two frozen policy documents govern transport behavior: the system registry
// (owner gms-system) and the Host-local proxy registry (owner host-proxy).
// Loading is digest-verified: the registry_digest declared inside a document
// must equal the JCS digest of the document minus its registry_digest field.
// Any drift - tampered file, reordered keys, changed retry semantics - fails
// closed with an error; a receiver that has not verified the digest MUST NOT
// trust passthrough codes (transport_mapping.passthrough.receiver_precondition).

const (
	systemReasonPolicyFile    = "system-reason-codes.v1.json"
	hostProxyReasonPolicyFile = "host-proxy-reason-codes.v1.json"

	systemReasonSchemaVersion    = "rsih-skill-evolution.system-reason-policy.v1"
	hostProxyReasonSchemaVersion = "rsih-skill-evolution.host-proxy-reason-policy.v1"

	systemPolicyID    = "system-reason-codes"
	hostProxyPolicyID = "host-proxy-reason-codes"

	systemRegistryOwner    = "gms-system"
	hostProxyRegistryOwner = "host-proxy"
)

var registryDigestRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ReasonCode is one closed reason-code entry: the behavioral meaning source
// (status, retryable, retry_scope, terminal) that receivers MUST take
// verbatim and MUST NOT infer from message text.
type ReasonCode struct {
	Name       string
	Group      string
	Owner      string
	Status     string // failure | inconclusive | success
	Retryable  bool
	RetryScope string // none | same_request | new_attempt
	Terminal   bool
	SpecRef    string
	Notes      string
}

// ReasonPolicy is one digest-verified registry document.
type ReasonPolicy struct {
	SourceName     string
	PolicyID       string
	SchemaVersion  string
	RegistryOwner  string
	RegistryDigest string // declared (== recomputed) digest
	Transport      *Object
	codes          map[string]ReasonCode
	order          []string
}

// Lookup returns the entry for a code. Unknown codes are absent; callers
// fail closed on them (Contract §13.7.1 unknown_code rule).
func (p *ReasonPolicy) Lookup(code string) (ReasonCode, bool) {
	e, ok := p.codes[code]
	return e, ok
}

// Codes returns all code names in registry order.
func (p *ReasonPolicy) Codes() []string {
	return append([]string(nil), p.order...)
}

// ReasonBundle combines the system registry with the Host proxy registry.
type ReasonBundle struct {
	System    *ReasonPolicy
	HostProxy *ReasonPolicy
}

// Lookup resolves a code across the bundle: the system registry is the
// meaning authority for shared/GMS codes, the Host registry for Host-local
// proxy codes. Unknown codes are absent (fail closed).
func (b *ReasonBundle) Lookup(code string) (ReasonCode, bool) {
	if e, ok := b.System.Lookup(code); ok {
		return e, true
	}
	if e, ok := b.HostProxy.Lookup(code); ok {
		return e, true
	}
	return ReasonCode{}, false
}

// LoadReasonBundle loads and digest-verifies both registry documents from
// policyDir.
func LoadReasonBundle(policyDir string) (*ReasonBundle, error) {
	system, err := LoadReasonPolicy(filepath.Join(policyDir, systemReasonPolicyFile))
	if err != nil {
		return nil, err
	}
	host, err := LoadReasonPolicy(filepath.Join(policyDir, hostProxyReasonPolicyFile))
	if err != nil {
		return nil, err
	}
	if dup := intersect(system.order, host.order); len(dup) != 0 {
		return nil, fmt.Errorf("reason registries share code names across owners: %v", dup)
	}
	return &ReasonBundle{System: system, HostProxy: host}, nil
}

// LoadReasonPolicy loads one registry document and verifies its declared
// registry_digest against a freshly computed JCS digest of the body
// (document minus registry_digest). Schema closure, code closure and
// ownership are enforced; any violation is an error (fail closed).
func LoadReasonPolicy(path string) (*ReasonPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read reason policy %s: %w", filepath.Base(path), err)
	}
	v, err := ParseJSON(data)
	if err != nil {
		return nil, fmt.Errorf("reason policy %s is not valid JSON: %w", filepath.Base(path), err)
	}
	doc, ok := v.(*Object)
	if !ok {
		return nil, fmt.Errorf("reason policy %s: document is not an object", filepath.Base(path))
	}

	name := filepath.Base(path)
	isSystem := name == systemReasonPolicyFile
	wantSchema := hostProxyReasonSchemaVersion
	wantPolicyID := hostProxyPolicyID
	wantOwner := hostProxyRegistryOwner
	if isSystem {
		wantSchema = systemReasonSchemaVersion
		wantPolicyID = systemPolicyID
		wantOwner = systemRegistryOwner
	}

	// Top-level key closure.
	for _, key := range []string{"schema_version", "policy_id", "version", "registry_owner", "codes", "registry_digest", "transport_mapping"} {
		if _, present := doc.Get(key); !present {
			return nil, fmt.Errorf("reason policy %s: missing top-level key %q", name, key)
		}
	}
	if len(doc.Keys()) != 7 {
		return nil, fmt.Errorf("reason policy %s: unexpected top-level keys %v", name, doc.Keys())
	}
	if sv, _ := StringOf(doc, "schema_version"); sv != wantSchema {
		return nil, fmt.Errorf("reason policy %s: schema_version %q != %q", name, sv, wantSchema)
	}
	if pid, _ := StringOf(doc, "policy_id"); pid != wantPolicyID {
		return nil, fmt.Errorf("reason policy %s: policy_id %q != %q", name, pid, wantPolicyID)
	}
	if owner, _ := StringOf(doc, "registry_owner"); owner != wantOwner {
		return nil, fmt.Errorf("reason policy %s: registry_owner %q != %q", name, owner, wantOwner)
	}
	ver, present := doc.Get("version")
	verNum, isNum := ver.(Number)
	if !present || !isNum || !verNum.IsInteger() || verNum != Number("1") {
		return nil, fmt.Errorf("reason policy %s: version must be integer 1", name)
	}
	declaredDigest, _ := StringOf(doc, "registry_digest")
	if !registryDigestRe.MatchString(declaredDigest) {
		return nil, fmt.Errorf("reason policy %s: registry_digest malformed", name)
	}
	transportVal, _ := doc.Get("transport_mapping")
	transport, transportOK := transportVal.(*Object)
	if !transportOK {
		return nil, fmt.Errorf("reason policy %s: transport_mapping must be an object", name)
	}

	// Digest verification: JCS over the document minus registry_digest.
	body := NewObject()
	for _, key := range doc.Keys() {
		if key == "registry_digest" {
			continue
		}
		val, _ := doc.Get(key)
		body.Set(key, val)
	}
	recomputed, err := DigestOf(body)
	if err != nil {
		return nil, fmt.Errorf("reason policy %s: digest preimage not canonicalizable: %w", name, err)
	}
	if recomputed != declaredDigest {
		return nil, fmt.Errorf("reason policy %s: registry digest mismatch: declared %s but recomputed %s (fail closed)", name, declaredDigest, recomputed)
	}

	// Codes array closure.
	codesVal, _ := doc.Get("codes")
	codesArr, ok := codesVal.(Array)
	if !ok || len(codesArr) == 0 {
		return nil, fmt.Errorf("reason policy %s: codes must be a non-empty array", name)
	}
	policy := &ReasonPolicy{
		SourceName:     name,
		PolicyID:       wantPolicyID,
		SchemaVersion:  wantSchema,
		RegistryOwner:  wantOwner,
		RegistryDigest: declaredDigest,
		Transport:      transport,
		codes:          make(map[string]ReasonCode),
	}
	for i, raw := range codesArr {
		entry, err := parseReasonCode(raw, wantOwner, name, i)
		if err != nil {
			return nil, err
		}
		if _, dup := policy.codes[entry.Name]; dup {
			return nil, fmt.Errorf("reason policy %s: duplicate code %q", name, entry.Name)
		}
		policy.codes[entry.Name] = entry
		policy.order = append(policy.order, entry.Name)
	}
	return policy, nil
}

func parseReasonCode(raw Value, wantOwner, policyName string, index int) (ReasonCode, error) {
	obj, ok := raw.(*Object)
	if !ok {
		return ReasonCode{}, fmt.Errorf("reason policy %s: codes[%d] is not an object", policyName, index)
	}
	required := []string{"name", "group", "owner", "status", "retryable", "retry_scope", "terminal", "spec_ref"}
	for _, key := range required {
		if _, present := obj.Get(key); !present {
			return ReasonCode{}, fmt.Errorf("reason policy %s: codes[%d] missing %q", policyName, index, key)
		}
	}
	entry := ReasonCode{}
	entry.Name, _ = StringOf(obj, "name")
	entry.Group, _ = StringOf(obj, "group")
	entry.Owner, _ = StringOf(obj, "owner")
	entry.Status, _ = StringOf(obj, "status")
	entry.RetryScope, _ = StringOf(obj, "retry_scope")
	entry.SpecRef, _ = StringOf(obj, "spec_ref")
	entry.Notes, _ = StringOf(obj, "notes")
	if entry.Name == "" {
		return ReasonCode{}, fmt.Errorf("reason policy %s: codes[%d] has empty name", policyName, index)
	}
	if entry.Owner != wantOwner {
		return ReasonCode{}, fmt.Errorf("reason policy %s: code %q owner %q violates registry ownership %q", policyName, entry.Name, entry.Owner, wantOwner)
	}
	switch entry.Status {
	case "failure", "inconclusive", "success":
	default:
		return ReasonCode{}, fmt.Errorf("reason policy %s: code %q has unknown status %q", policyName, entry.Name, entry.Status)
	}
	switch entry.RetryScope {
	case "none", "same_request", "new_attempt":
	default:
		return ReasonCode{}, fmt.Errorf("reason policy %s: code %q has unknown retry_scope %q", policyName, entry.Name, entry.RetryScope)
	}
	retryableVal, _ := obj.Get("retryable")
	retryable, ok := retryableVal.(Bool)
	if !ok {
		return ReasonCode{}, fmt.Errorf("reason policy %s: code %q retryable must be a boolean", policyName, entry.Name)
	}
	entry.Retryable = bool(retryable)
	terminalVal, _ := obj.Get("terminal")
	terminal, ok := terminalVal.(Bool)
	if !ok {
		return ReasonCode{}, fmt.Errorf("reason policy %s: code %q terminal must be a boolean", policyName, entry.Name)
	}
	entry.Terminal = bool(terminal)
	return entry, nil
}

func intersect(a, b []string) []string {
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	var out []string
	for _, s := range b {
		if _, ok := set[s]; ok {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
