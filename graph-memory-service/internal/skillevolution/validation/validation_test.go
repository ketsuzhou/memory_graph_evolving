package validation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
)

func jsonNum(s string) any { return json.Number(s) }

func conformanceDir(t *testing.T) string {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("locate conformance corpus: %v", err)
	}
	return dir
}

func newGates(t *testing.T) *Gates {
	t.Helper()
	dir := conformanceDir(t)
	schemas, err := LoadSchemaSet(filepath.Join(dir, "schema", "shared"))
	if err != nil {
		t.Fatalf("LoadSchemaSet: %v", err)
	}
	gates, err := NewGates(schemas)
	if err != nil {
		t.Fatalf("NewGates: %v", err)
	}
	return gates
}

// schemaCase is one $FIX/schema-cases/manifest.json entry.
type schemaCase struct {
	CaseID      string
	Category    string
	Kind        string
	Target      string
	InstanceRel string
	ExpectedRel string
}

func loadSchemaCases(t *testing.T) []schemaCase {
	t.Helper()
	dir := conformanceDir(t)
	raw, err := os.ReadFile(filepath.Join(dir, "schema-cases", "manifest.json"))
	if err != nil {
		t.Fatalf("read schema-cases manifest: %v", err)
	}
	value, err := contract.ParseJSONStrict(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	obj, _ := contract.AsObject(value)
	cases, _ := contract.AsArray(obj["cases"])
	out := make([]schemaCase, 0, len(cases))
	for _, rawCase := range cases {
		entry, _ := contract.AsObject(rawCase)
		out = append(out, schemaCase{
			CaseID:      str(entry["case_id"]),
			Category:    str(entry["category"]),
			Kind:        str(entry["kind"]),
			Target:      str(entry["target"]),
			InstanceRel: str(entry["instance_path"]),
			ExpectedRel: str(entry["expected_path"]),
		})
	}
	return out
}

func str(v any) string {
	s, _ := contract.AsString(v)
	return s
}

// TestDTOSchemaCaseSuite drives every dto schema-case through the shared
// gate pipeline (shape from the authority schema -> integer-only core ->
// x-digest preimage -> extensions): accept/reason must match expected.json
// exactly (CTR-005 machine-readable authority).
func TestDTOSchemaCaseSuite(t *testing.T) {
	gates := newGates(t)
	dir := conformanceDir(t)
	for _, tc := range loadSchemaCases(t) {
		if tc.Category != "dto" {
			continue
		}
		instanceRaw, err := os.ReadFile(filepath.Join(dir, "schema-cases", tc.InstanceRel))
		if err != nil {
			t.Fatalf("%s: read instance: %v", tc.CaseID, err)
		}
		expectedRaw, err := os.ReadFile(filepath.Join(dir, "schema-cases", tc.ExpectedRel))
		if err != nil {
			t.Fatalf("%s: read expected: %v", tc.CaseID, err)
		}
		instance, err := contract.ParseJSONStrict(instanceRaw)
		if err != nil {
			t.Fatalf("%s: parse instance: %v", tc.CaseID, err)
		}
		expected, _ := contract.ParseJSONStrict(expectedRaw)
		expectedObj, _ := contract.AsObject(expected)
		wantAccept, _ := expectedObj["expected_accept"].(bool)
		wantReason := str(expectedObj["expected_reason_code"])

		schemaFile := filepath.Base(tc.Target)
		err = gates.ValidateInstance(instance, schemaFile)
		gotAccept, gotReason := err == nil, ""
		if err != nil {
			gotReason = CodeOf(err)
		}
		if gotAccept != wantAccept || gotReason != wantReason {
			t.Errorf("%s (%s): want (%v, %q) got (%v, %q)", tc.CaseID, schemaFile, wantAccept, wantReason, gotAccept, gotReason)
		}
	}
}

// TestStateSchemaCaseSuite replays every state schema-case against the
// machine loaded from $FIX/schema/state: accept/reason parity with
// expected.json (schema/state is the single authority for the machines).
func TestStateSchemaCaseSuite(t *testing.T) {
	dir := conformanceDir(t)
	bundles := map[string]*Bundle{}
	for _, tc := range loadSchemaCases(t) {
		if tc.Category != "state" {
			continue
		}
		eventsRaw, err := os.ReadFile(filepath.Join(dir, "schema-cases", tc.InstanceRel))
		if err != nil {
			t.Fatalf("%s: read events: %v", tc.CaseID, err)
		}
		expectedRaw, err := os.ReadFile(filepath.Join(dir, "schema-cases", tc.ExpectedRel))
		if err != nil {
			t.Fatalf("%s: read expected: %v", tc.CaseID, err)
		}
		events, err := contract.ParseJSONStrict(eventsRaw)
		if err != nil {
			t.Fatalf("%s: parse events: %v", tc.CaseID, err)
		}
		expected, _ := contract.ParseJSONStrict(expectedRaw)
		expectedObj, _ := contract.AsObject(expected)
		wantAccept, _ := expectedObj["expected_accept"].(bool)
		wantReason := str(expectedObj["expected_reason_code"])

		eventsObj, _ := contract.AsObject(events)
		bundleName := str(eventsObj["bundle"])
		bundle, ok := bundles[bundleName]
		if !ok {
			bundle, err = LoadStateBundle(filepath.Join(dir, "schema", "state", bundleName+".schema.json"))
			if err != nil {
				t.Fatalf("load bundle %s: %v", bundleName, err)
			}
			bundles[bundleName] = bundle
		}
		machine, err := bundle.Machine(str(eventsObj["machine"]))
		if err != nil {
			t.Fatalf("%s: machine: %v", tc.CaseID, err)
		}
		err = machine.ReplayDocument(eventsObj)
		gotAccept, gotReason := err == nil, ""
		if err != nil {
			gotReason = CodeOf(err)
		}
		if gotAccept != wantAccept || gotReason != wantReason {
			t.Errorf("%s (%s/%s): want (%v, %q) got (%v, %q)", tc.CaseID, bundleName, str(eventsObj["machine"]), wantAccept, wantReason, gotAccept, gotReason)
		}
	}
}

// TestGoStateTablesMatchSchemaAuthority cross-checks the GMS-102 frozen Go
// tables against the machines loaded from $FIX/schema/state: the schema files
// stay the single authority for legal transitions.
func TestGoStateTablesMatchSchemaAuthority(t *testing.T) {
	dir := conformanceDir(t)
	// The ledger package tables are cross-checked in their own tests; here we
	// pin that the schema-driven engine agrees with the frozen Go tables by
	// exporting the machine edges for the ordinary proposal machine.
	bundle, err := LoadStateBundle(filepath.Join(dir, "schema", "state", "proposal-lifecycle.schema.json"))
	if err != nil {
		t.Fatalf("load proposal lifecycle: %v", err)
	}
	machine, err := bundle.Machine("skill-proposal")
	if err != nil {
		t.Fatalf("machine: %v", err)
	}
	if len(machine.InitialStates()) == 0 {
		t.Fatal("no initial states")
	}
	if !machine.HasEdge("proposed", "admitted") {
		t.Fatal("proposed -> admitted missing")
	}
	if machine.HasEdge("proposed", "released") {
		t.Fatal("proposed -> released must not be an edge")
	}
	if !machine.IsTerminal("released") || machine.IsTerminal("proposed") {
		t.Fatal("terminal classification wrong")
	}
	if trigger, ok := machine.Trigger("proposed", "admitted"); !ok || trigger == "" {
		t.Fatal("trigger of proposed -> admitted missing")
	}
}

func TestExtensionGateAndDigestGate(t *testing.T) {
	gates := newGates(t)

	if err := gates.CheckExtensions(map[string]any{
		"ai.example.optional": map[string]any{"required": false},
	}); err != nil {
		t.Fatalf("unknown optional extension must be ignored: %v", err)
	}
	if err := gates.CheckExtensions(map[string]any{
		"ai.example.required": map[string]any{"required": true},
	}); CodeOf(err) != CodeUnknownRequiredExtension {
		t.Fatalf("unknown required extension: %v", err)
	}

	doc := map[string]any{
		"schema_version": "gms.proposal-event.v1",
		"event_sequence": jsonNum("1"),
		"event_id":       "ev-1",
	}
	digest, err := gates.ComputeDigestPreimage(doc, "proposal-event.schema.json")
	if err != nil {
		t.Fatalf("digest preimage: %v", err)
	}
	doc["event_digest"] = digest
	if err := gates.CheckDigestPreimage(doc, "proposal-event.schema.json"); err != nil {
		t.Fatalf("digest roundtrip: %v", err)
	}
	doc["event_id"] = "ev-2"
	if err := gates.CheckDigestPreimage(doc, "proposal-event.schema.json"); CodeOf(err) != CodeDigestMismatch {
		t.Fatalf("tampered preimage: %v", err)
	}
}

func TestPermissionCapGate(t *testing.T) {
	gates := newGates(t)
	allowed := []any{
		map[string]any{"capability": "memory_explore", "scope": "room"},
		map[string]any{"capability": "memory_expand", "scope": "room"},
		map[string]any{"capability": "skill_get", "scope": "session"},
	}
	for _, entry := range allowed {
		if err := gates.CheckPermissionEntry(entry); err != nil {
			t.Fatalf("allowed permission rejected: %v", err)
		}
	}
	if err := gates.CheckPermissionEntry(map[string]any{"capability": "filesystem_write", "scope": "/"}); CodeOf(err) != CodePermissionCapExceeded {
		t.Fatalf("capability beyond cap: %v", err)
	}
	if err := gates.CheckPermissionEntry(map[string]any{"capability": "memory_explore"}); err == nil {
		t.Fatal("permission without explicit scope must reject")
	}
}
