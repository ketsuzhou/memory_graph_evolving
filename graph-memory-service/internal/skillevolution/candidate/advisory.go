package candidate

import (
	"context"
	"fmt"
	"strings"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// AdvisoryCandidate is a text-only, non-authoritative rendering of an
// immutable candidate. It intentionally has no runtime/package/patch field.
type AdvisoryCandidate struct {
	Ref         contract.CandidateArtifactRef
	CandidateID string
	Kind        string
	Guidance    string
}

type AdvisoryReader struct{ store ledger.Store }

func NewAdvisoryReader(store ledger.Store) (*AdvisoryReader, error) {
	if store == nil {
		return nil, fmt.Errorf("candidate: advisory store required")
	}
	return &AdvisoryReader{store: store}, nil
}
func (r *AdvisoryReader) ListAdvisory(_ context.Context) ([]AdvisoryCandidate, error) {
	entries, err := r.store.Snapshot(ledger.LedgerCandidate, "")
	if err != nil {
		return nil, err
	}
	out := []AdvisoryCandidate{}
	for _, entry := range entries {
		payload, ok, err := r.store.Get(entry.PayloadDigest)
		if err != nil || !ok {
			continue
		}
		value, err := contract.ParseJSONStrict(payload)
		if err != nil {
			continue
		}
		obj, _ := contract.AsObject(value)
		ref, err := contract.ParseCandidateArtifactRef(obj)
		if err != nil || (ref.Kind != "human_procedure" && ref.Kind != "step_guidance") {
			continue
		}
		body, ok, err := r.store.Get(ref.BodyDigest)
		if err != nil || !ok || contract.DigestBytes(body) != ref.BodyDigest {
			continue
		}
		bodyValue, err := contract.ParseJSONStrict(body)
		if err != nil {
			continue
		}
		envelope, _ := contract.AsObject(bodyValue)
		guidance := advisoryGuidance(ref.Kind, envelope)
		if guidance == "" {
			continue
		}
		out = append(out, AdvisoryCandidate{Ref: ref, CandidateID: ref.CandidateID, Kind: ref.Kind, Guidance: guidance})
	}
	return out, nil
}
func advisoryGuidance(kind string, envelope map[string]any) string {
	body, _ := contract.AsObject(envelope["body"])
	parts := []string{}
	if kind == "human_procedure" {
		steps, _ := contract.AsArray(body["steps"])
		for _, raw := range steps {
			step, _ := contract.AsObject(raw)
			if text, _ := contract.AsString(step["instruction"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	if kind == "step_guidance" {
		branches, _ := contract.AsArray(body["branches"])
		for _, raw := range branches {
			branch, _ := contract.AsObject(raw)
			action, _ := contract.AsObject(branch["action"])
			if text, _ := contract.AsString(action["guidance"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n")
}
