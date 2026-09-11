package contract

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

// Frozen reason-code registry policy files and digests (Contract §13.7.1
// revision v1.1, CTR-004). Loading MUST verify the digest by recomputation;
// a mismatch fails closed. The two digests below are additionally pinned to
// the values frozen in the contract text (R2) so a re-signed-but-unfrozen
// registry is also rejected.
const (
	SystemReasonPolicyFile        = "system-reason-codes.v1.json"
	HostProxyReasonPolicyFile     = "host-proxy-reason-codes.v1.json"
	SystemReasonPolicyID          = "system-reason-codes"
	HostProxyReasonPolicyID       = "host-proxy-reason-codes"
	SystemReasonSchemaVersion     = "rsih-skill-evolution.system-reason-policy.v1"
	HostProxyReasonSchemaVersion  = "rsih-skill-evolution.host-proxy-reason-policy.v1"
	SystemRegistryOwner           = "gms-system"
	HostProxyRegistryOwner        = "host-proxy"
	SystemRegistryDigestFrozen    = "sha256:16410afa27498bb425885d4629c65309389f15adeb975f97f98674170ad00a2c"
	HostProxyRegistryDigestFrozen = "sha256:e48f27252bff3fd34868ef4bc5b56a678cf2a35d59f4cd7f2c71f78290485f5e"
)

var (
	reasonPolicyTopLevelKeys = map[string]bool{
		"schema_version": true, "policy_id": true, "version": true,
		"registry_owner": true, "transport_mapping": true, "codes": true,
		"registry_digest": true,
	}
	reasonCodeRequiredKeys = map[string]bool{
		"name": true, "group": true, "status": true, "retryable": true,
		"retry_scope": true, "terminal": true, "owner": true, "spec_ref": true,
	}
	reasonCodeNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`)
	reasonDigestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

	reasonStatuses    = map[string]bool{"success": true, "failure": true, "inconclusive": true}
	reasonRetryScopes = map[string]bool{"same_request": true, "new_attempt": true, "none": true}
)

// ReasonCodeInfo is one closed-registry entry: the total
// status/retryable/retry_scope/terminal mapping that receivers MUST take
// verbatim (Contract §13.7.1 R1).
type ReasonCodeInfo struct {
	Name       string
	Group      string
	Status     string
	Retryable  bool
	RetryScope string
	Terminal   bool
	Owner      string
	SpecRef    string
	Notes      string
	HasNotes   bool
}

// ReasonPolicy is one digest-verified reason-code registry document.
type ReasonPolicy struct {
	Path           string
	SchemaVersion  string
	PolicyID       string
	RegistryOwner  string
	RegistryDigest string

	codes map[string]ReasonCodeInfo
	names []string // document order
}

// LoadSystemReasonPolicy loads policyDir/system-reason-codes.v1.json,
// verifies its recomputed JCS digest against the declared value and the
// contract-frozen digest, and returns the registry.
func LoadSystemReasonPolicy(policyDir string) (*ReasonPolicy, error) {
	p, err := LoadReasonPolicy(filepath.Join(policyDir, SystemReasonPolicyFile))
	if err != nil {
		return nil, err
	}
	if p.SchemaVersion != SystemReasonSchemaVersion {
		return nil, fmt.Errorf("contract: %s: schema_version %q is not %q", p.Path, p.SchemaVersion, SystemReasonSchemaVersion)
	}
	if p.PolicyID != SystemReasonPolicyID {
		return nil, fmt.Errorf("contract: %s: policy_id %q is not %q", p.Path, p.PolicyID, SystemReasonPolicyID)
	}
	if p.RegistryOwner != SystemRegistryOwner {
		return nil, fmt.Errorf("contract: %s: registry_owner %q is not %q", p.Path, p.RegistryOwner, SystemRegistryOwner)
	}
	if p.RegistryDigest != SystemRegistryDigestFrozen {
		return nil, fmt.Errorf("contract: %s: registry_digest %s does not match the contract-frozen system digest %s", p.Path, p.RegistryDigest, SystemRegistryDigestFrozen)
	}
	return p, nil
}

// LoadHostProxyReasonPolicy loads policyDir/host-proxy-reason-codes.v1.json
// with the same digest-verified fail-closed load path.
func LoadHostProxyReasonPolicy(policyDir string) (*ReasonPolicy, error) {
	p, err := LoadReasonPolicy(filepath.Join(policyDir, HostProxyReasonPolicyFile))
	if err != nil {
		return nil, err
	}
	if p.SchemaVersion != HostProxyReasonSchemaVersion {
		return nil, fmt.Errorf("contract: %s: schema_version %q is not %q", p.Path, p.SchemaVersion, HostProxyReasonSchemaVersion)
	}
	if p.PolicyID != HostProxyReasonPolicyID {
		return nil, fmt.Errorf("contract: %s: policy_id %q is not %q", p.Path, p.PolicyID, HostProxyReasonPolicyID)
	}
	if p.RegistryOwner != HostProxyRegistryOwner {
		return nil, fmt.Errorf("contract: %s: registry_owner %q is not %q", p.Path, p.RegistryOwner, HostProxyRegistryOwner)
	}
	if p.RegistryDigest != HostProxyRegistryDigestFrozen {
		return nil, fmt.Errorf("contract: %s: registry_digest %s does not match the contract-frozen host-proxy digest %s", p.Path, p.RegistryDigest, HostProxyRegistryDigestFrozen)
	}
	return p, nil
}

// LoadReasonPolicy loads one registry document and verifies it fail-closed:
// closed top-level shape, non-empty closed-shape codes array without
// duplicates, and registry_digest == SHA-256(JCS(document minus
// registry_digest)). Any violation is an error; there is no degraded
// no-schema mode.
func LoadReasonPolicy(path string) (*ReasonPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("contract: read reason policy: %w", err)
	}
	doc, err := ParseJSONStrict(data)
	if err != nil {
		return nil, fmt.Errorf("contract: %s is not valid UTF-8 JSON: %w", path, err)
	}
	obj, ok := AsObject(doc)
	if !ok {
		return nil, fmt.Errorf("contract: %s: document is not an object", path)
	}
	for key := range obj {
		if !reasonPolicyTopLevelKeys[key] {
			return nil, fmt.Errorf("contract: %s: unknown top-level key %q", path, key)
		}
	}
	for key := range reasonPolicyTopLevelKeys {
		if _, present := obj[key]; !present {
			return nil, fmt.Errorf("contract: %s: missing top-level key %q", path, key)
		}
	}
	declared, _ := AsString(obj["registry_digest"])
	if !reasonDigestPattern.MatchString(declared) {
		return nil, fmt.Errorf("contract: %s: registry_digest malformed", path)
	}

	// Digest preimage is the whole document minus registry_digest (R2).
	body := make(map[string]any, len(obj))
	for k, v := range obj {
		if k != "registry_digest" {
			body[k] = v
		}
	}
	recomputed, err := DigestOf(body)
	if err != nil {
		return nil, fmt.Errorf("contract: %s: policy body failed integer-only JCS: %w", path, err)
	}
	if recomputed != declared {
		return nil, fmt.Errorf("contract: POLICY_DIGEST_MISMATCH: %s: declared %s but recomputed %s", path, declared, recomputed)
	}

	policyID, _ := AsString(obj["policy_id"])
	owner, _ := AsString(obj["registry_owner"])
	schemaVersion, _ := AsString(obj["schema_version"])
	if !IsIntegerNumber(obj["version"]) {
		return nil, fmt.Errorf("contract: %s: version must be an integer", path)
	}
	if tm, ok := AsObject(obj["transport_mapping"]); !ok || len(tm) == 0 {
		return nil, fmt.Errorf("contract: %s: transport_mapping must be a non-empty object", path)
	}

	rawCodes, ok := AsArray(obj["codes"])
	if !ok || len(rawCodes) == 0 {
		return nil, fmt.Errorf("contract: %s: codes must be a non-empty array", path)
	}
	codes := make(map[string]ReasonCodeInfo, len(rawCodes))
	names := make([]string, 0, len(rawCodes))
	for i, raw := range rawCodes {
		entry, ok := AsObject(raw)
		if !ok {
			return nil, fmt.Errorf("contract: %s: codes[%d] is not an object", path, i)
		}
		for key := range entry {
			if !reasonCodeRequiredKeys[key] && key != "notes" {
				return nil, fmt.Errorf("contract: %s: codes[%d] unknown key %q", path, i, key)
			}
		}
		for key := range reasonCodeRequiredKeys {
			if _, present := entry[key]; !present {
				return nil, fmt.Errorf("contract: %s: codes[%d] missing key %q", path, i, key)
			}
		}
		name, _ := AsString(entry["name"])
		if !reasonCodeNamePattern.MatchString(name) {
			return nil, fmt.Errorf("contract: %s: codes[%d] malformed code name %q", path, i, name)
		}
		if _, dup := codes[name]; dup {
			return nil, fmt.Errorf("contract: POLICY_CODE_DUPLICATED: %s: %s appears more than once", path, name)
		}
		info := ReasonCodeInfo{Name: name}
		info.Group, _ = AsString(entry["group"])
		info.Owner, _ = AsString(entry["owner"])
		info.SpecRef, _ = AsString(entry["spec_ref"])
		if info.Group == "" || info.SpecRef == "" {
			return nil, fmt.Errorf("contract: %s: codes[%d] group/spec_ref must be non-empty strings", path, i)
		}
		var okStatus, okScope bool
		info.Status, okStatus = AsString(entry["status"])
		info.RetryScope, okScope = AsString(entry["retry_scope"])
		if !okStatus || !reasonStatuses[info.Status] {
			return nil, fmt.Errorf("contract: %s: codes[%d] invalid status", path, i)
		}
		if !okScope || !reasonRetryScopes[info.RetryScope] {
			return nil, fmt.Errorf("contract: %s: codes[%d] invalid retry_scope", path, i)
		}
		var isBool bool
		info.Retryable, isBool = entry["retryable"].(bool)
		info.Terminal, isBool = entry["terminal"].(bool)
		if !isBool {
			return nil, fmt.Errorf("contract: %s: codes[%d] retryable/terminal must be booleans", path, i)
		}
		if notes, present := entry["notes"]; present {
			info.Notes, _ = AsString(notes)
			info.HasNotes = true
		}
		codes[name] = info
		names = append(names, name)
	}

	return &ReasonPolicy{
		Path:           path,
		SchemaVersion:  schemaVersion,
		PolicyID:       policyID,
		RegistryOwner:  owner,
		RegistryDigest: declared,
		codes:          codes,
		names:          names,
	}, nil
}

// Lookup returns the total mapping of a registered code. Codes outside the
// closed registry have no defined behavior and fail closed (Contract
// §13.7.1: consumers MUST NOT infer meaning from message text).
func (p *ReasonPolicy) Lookup(code string) (ReasonCodeInfo, error) {
	info, ok := p.codes[code]
	if !ok {
		return ReasonCodeInfo{}, fmt.Errorf("contract: unknown reason code %q outside the closed registry %q (%s)", code, p.PolicyID, p.Path)
	}
	return info, nil
}

// Has reports whether the code exists in this registry.
func (p *ReasonPolicy) Has(code string) bool {
	_, ok := p.codes[code]
	return ok
}

// CodeCount returns the number of registered codes.
func (p *ReasonPolicy) CodeCount() int { return len(p.names) }

// Names returns the code names in document order.
func (p *ReasonPolicy) Names() []string {
	out := make([]string, len(p.names))
	copy(out, p.names)
	return out
}
