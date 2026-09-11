// rebuild.go carries the projection-stream states: the Curation stream
// stays disabled/uninitialized (Contract §11.1) and the Runtime Rebuild
// swap (GMS §8.6).
package projector

import (
	"context"

	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
)

// StreamStatus reports the materialization state of one projection stream.
type StreamStatus struct {
	Stream       string
	Materialized bool
	State        string
	Note         string
}

// CurationStatus reports the Curation projection stream status: v1
// materializes the Runtime stream ONLY — the Curation stream exists as a
// logical stream/head identity but stays disabled and uninitialized
// (Contract §11.1; U7), so no Curation node, edge or watermark is ever
// written.
func (s *Service) CurationStatus() StreamStatus {
	return StreamStatus{
		Stream:       StreamCuration,
		Materialized: false,
		State:        "disabled",
		Note:         "Contract §11.1: v1 materializes the Runtime stream only; the Curation stream stays disabled and uninitialized",
	}
}

// Rebuild rebuilds the Runtime Graph from zero over the authoritative
// sources (GMS §8.6; Contract §9.3 any→rebuilding→catching_up/current):
//
//   - the whole activation ledger from sequence zero plus every relation
//     record from zero folds through the SAME rules into a shadow state —
//     the rebuild is rule-identical to the incremental path, so the
//     rebuilt content digest, cursor vector and watermark reproduce the
//     incremental ones exactly (batch-boundary independence);
//   - the unit CASes the SAME projection head: a concurrent writer that
//     moved the head discards the rebuild (PROJECTION_HEAD_CONFLICT);
//   - the swap replaces the whole derived state atomically, and the
//     pre-rebuild head's watermark stays readable through PriorWatermark;
//   - a rule violation during the fold blocks the head state-only, exactly
//     like an incremental batch (the ledger replay cannot silently skip).
func (s *Service) Rebuild(ctx context.Context) (*ProjectionResult, error) {
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	current := s.graph.stateOf()

	entries, err := s.store.Snapshot(ledger.LedgerActivation, "")
	if err != nil {
		return nil, err
	}
	var batch []pendingActivation
	for _, entry := range entries {
		payload, ok, err := s.store.Get(entry.PayloadDigest)
		if err != nil {
			return nil, err
		}
		if !ok {
			return s.commitBlocked(current, SourceActivation, entry.Sequence, ReasonDigestMismatch,
				"activation event %d payload %s resolves to no committed content during rebuild", entry.Sequence, entry.PayloadDigest)
		}
		batch = append(batch, pendingActivation{sequence: entry.Sequence, eventID: entry.EventID, digest: entry.PayloadDigest, payload: payload})
	}

	// Fold the whole history from zero into a shadow state.
	shadow := newGraphState()
	shadow.state = StateRebuilding
	var mutations []Mutation
	prefixes := PrefixState{}
	cursors := CursorVector{}
	expected := uint64(1)
	for _, item := range batch {
		if item.sequence != expected {
			return s.commitBlocked(current, SourceActivation, item.sequence, ReasonProjectionSequenceGap,
				"the activation ledger itself is non-contiguous at %d (expected %d); the authority is torn", item.sequence, expected)
		}
		view, err := s.parseActivationEvent(item.sequence, item.eventID, item.payload, item.digest)
		if err != nil {
			return s.commitBlockedErr(current, SourceActivation, item.sequence, err)
		}
		eventMutations, err := s.activationMutations(shadow, view)
		if err != nil {
			return s.commitBlockedErr(current, SourceActivation, item.sequence, err)
		}
		for _, mutation := range eventMutations {
			shadow.apply(mutation)
			mutations = append(mutations, mutation)
		}
		if prefixes.Activation, err = foldPrefix(prefixes.Activation, item.sequence, item.eventID, item.digest); err != nil {
			return nil, newError(s.registry, ReasonDigestMismatch, "activation prefix fold: %v", err)
		}
		cursors.Activation = item.sequence
		expected++
	}
	simCursor, simPrefix, err := s.foldRelationSource(shadow, s.similarity, SourceSimilarity, 0, "", &mutations)
	if err != nil {
		return s.commitBlockedErr(current, "", 0, err)
	}
	assessCursor, assessPrefix, err := s.foldRelationSource(shadow, s.assessments, SourceEvidenceAssessment, 0, "", &mutations)
	if err != nil {
		return s.commitBlockedErr(current, "", 0, err)
	}
	cursors.Similarity, prefixes.Similarity = simCursor, simPrefix
	cursors.EvidenceAssessment, prefixes.EvidenceAssessment = assessCursor, assessPrefix

	state := StateCurrent
	if head, ok, err := s.store.Head(ledger.LedgerActivation, ""); err != nil {
		return nil, err
	} else if ok && head.Sequence > cursors.Activation {
		state = StateCatchingUp
	}
	return s.commitUnit(&CommitUnit{
		ExpectedHeadSeq:    current.headSeq,
		ExpectedHeadDigest: current.headDigest,
		RebuildSwap:        shadow,
		PriorWatermark:     deepCopyDoc(current.watermark),
		Cursors:            cursors,
		Prefixes:           prefixes,
		State:              state,
	}, len(batch))
}
