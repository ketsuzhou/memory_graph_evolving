// Package projector is the GMS-205 Runtime projector (GMS §8–§9, §12.5/
// §12.9/§12.13–§12.14; Contract §5.3–§5.4, §9.3, §11, §13.6, §16.6).
//
// cursors.go freezes the private cursor vector, the per-source ledger
// prefix commitments and the Contract §7.14 ProjectionWatermark construction
// (GMS §2.8, §8.4).
package projector

import (
	"encoding/json"
	"errors"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// Projection streams (Contract §11.1: logically independent runtime and
// curation streams/heads; v1 materializes Runtime only).
const (
	StreamRuntime  = "runtime"
	StreamCuration = "curation"
)

// ProjectionSchemaVersion is the v1 Runtime projection schema identity (as
// frozen by the $FIX/events/event-004 watermark fixture).
const ProjectionSchemaVersion = "gms.runtime-projection.v1"

// Projector states (Contract §9.3 — the shared state set; this package does
// not add or reopen states).
const (
	StateUninitialized = "uninitialized"
	StateCatchingUp    = "catching_up"
	StateCurrent       = "current"
	StateBlocked       = "blocked"
	StateRebuilding    = "rebuilding"
)

// Source names of the private cursor vector (GMS §2.8): the activation
// cursor plus the two relation-source cursors. The activation ledger doubles
// as the artifact-release source; no anchor source exists in v1.
const (
	SourceActivation         = "activation"
	SourceSimilarity         = "similarity_assessment"
	SourceEvidenceAssessment = "evidence_assessment"
)

// CursorVector is the complete private cursor vector of one projection head
// (GMS §2.8/§8.4): each entry is the count of contiguously consumed records
// of that source (0 = none). Every successful batch recomputes the whole
// vector; the public §7.14 watermark commits it through
// source_ledger_digest.
type CursorVector struct {
	Activation         uint64
	Similarity         uint64
	EvidenceAssessment uint64
}

// PrefixState carries the rolling per-source ledger-prefix commitments: for
// every consumed source record the accumulator folds the record identity
// (sequence, record id, canonical record digest) into a chained digest, so
// source_ledger_digest commits the exact consumed prefix of every source
// (GMS §8.4). An empty accumulator denotes "nothing consumed".
type PrefixState struct {
	Activation         string
	Similarity         string
	EvidenceAssessment string
}

// foldPrefix chains one consumed record into a source prefix accumulator.
// The fold is deterministic and replay-stable: the same records in the same
// order always produce the same accumulator, so a from-zero rebuild
// recomputes the identical source_ledger_digest.
func foldPrefix(previous string, sequence uint64, recordID, recordDigest string) (string, error) {
	var prev any
	if previous != "" {
		prev = previous
	}
	return contract.DigestOf(map[string]any{
		"previous":      prev,
		"sequence":      json.Number(ulong(sequence)),
		"record_id":     recordID,
		"record_digest": recordDigest,
	})
}

// sourceLedgerDigest is the canonical commitment over the complete,
// canonically ordered private cursor vector, the projection schema and the
// per-source ledger prefix digests (GMS §8.4). Source order is fixed
// (activation, similarity assessment, evidence/claim assessment) so the
// commitment is replay-stable.
func sourceLedgerDigest(cursors CursorVector, prefixes PrefixState) (string, error) {
	return contract.DigestOf(map[string]any{
		"projection_schema_version": ProjectionSchemaVersion,
		"sources": []any{
			map[string]any{
				"source":        SourceActivation,
				"cursor":        json.Number(ulong(cursors.Activation)),
				"prefix_digest": nullableDigest(prefixes.Activation),
			},
			map[string]any{
				"source":        SourceSimilarity,
				"cursor":        json.Number(ulong(cursors.Similarity)),
				"prefix_digest": nullableDigest(prefixes.Similarity),
			},
			map[string]any{
				"source":        SourceEvidenceAssessment,
				"cursor":        json.Number(ulong(cursors.EvidenceAssessment)),
				"prefix_digest": nullableDigest(prefixes.EvidenceAssessment),
			},
		},
	})
}

// buildWatermark renders the Contract §7.14 ProjectionWatermark document of
// one projection head state and returns it with its watermark_digest. The
// projection head string is content-derived (stream + source ledger digest)
// so the watermark is independent of batch boundaries: an incremental catch-
// up and a from-zero rebuild over the same consumed sources produce the
// identical head and digest. watermark_digest is computed through the
// authority x-digest rule (sha256 over JCS of the seven core fields minus
// the digest itself) and the whole document is validated against the frozen
// authority schema before use.
func buildWatermark(registry ledger.ReasonRegistry, state string, cursors CursorVector, prefixes PrefixState) (map[string]any, string, error) {
	if !projectorStates[state] {
		return nil, "", newError(registry, ReasonSchemaEnumInvalid, "state %q is outside the Contract §9.3 projector state set", state)
	}
	ledgerDigest, err := sourceLedgerDigest(cursors, prefixes)
	if err != nil {
		return nil, "", newError(registry, ReasonDigestMismatch, "source ledger digest: %v", err)
	}
	doc := map[string]any{
		"schema_version":                        "gms.projection-watermark.v1",
		"projection_stream":                     StreamRuntime,
		"projection_schema_version":             ProjectionSchemaVersion,
		"projection_head":                       StreamRuntime + "@" + ledgerDigest,
		"projected_through_activation_sequence": json.Number(ulong(cursors.Activation)),
		"source_ledger_digest":                  ledgerDigest,
		"state":                                 state,
	}
	digest, err := digestOfDoc(doc)
	if err != nil {
		return nil, "", newError(registry, ReasonDigestMismatch, "watermark digest: %v", err)
	}
	doc["watermark_digest"] = digest
	return doc, digest, nil
}

// digestOfDoc computes the authority x-digest of a watermark document
// before its digest field exists: sha256 over the JCS of the seven preimage
// fields (schema_version, projection_stream, projection_schema_version,
// projection_head, projected_through_activation_sequence,
// source_ledger_digest, state).
func digestOfDoc(doc map[string]any) (string, error) {
	preimage := map[string]any{}
	for _, field := range []string{
		"schema_version", "projection_stream", "projection_schema_version",
		"projection_head", "projected_through_activation_sequence",
		"source_ledger_digest", "state",
	} {
		value, present := doc[field]
		if !present {
			return "", errors.New("watermark preimage field missing: " + field)
		}
		preimage[field] = value
	}
	return contract.DigestOf(preimage)
}

var projectorStates = map[string]bool{
	StateUninitialized: true,
	StateCatchingUp:    true,
	StateCurrent:       true,
	StateBlocked:       true,
	StateRebuilding:    true,
}

func nullableDigest(digest string) any {
	if digest == "" {
		return nil
	}
	return digest
}

func ulong(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
