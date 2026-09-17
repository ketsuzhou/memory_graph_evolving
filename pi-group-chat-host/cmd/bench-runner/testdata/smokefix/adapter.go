// Package smokefix is a test-only adapter that builds the TB-16 fixture
// Skill graph and exposes GMS reject-redirect plus held-out isolation
// without Host production code importing GMS internals.
package smokefix

import (
	"errors"
	"fmt"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationexplore"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
	"river2.dev/graph-memory-service/internal/skillevolution/rejectredirect"
)

const (
	GenericRef       = "skill://evaluation/generic@1"
	SpecializedRef   = "skill://evaluation/specialized@1"
	GenericBody      = "generic-advice"
	SpecializedBody  = "specialized-timeout"
	ReasonTooGeneric = rejectredirect.ReasonTooGeneric
	ActionRedirect   = rejectredirect.ActionExploreGuardedDescendant
)

// RedirectHop is the Host-visible offer → reason → next query audit.
type RedirectHop struct {
	OfferID    string
	Ref        string
	ReasonCode string
	Reason     string
	NextQuery  string
	NextOffer  string
	Action     string
	NextSeeds  []string
}

// Fixture is one specialize-graph episode used by the contract smoke.
type Fixture struct {
	GenericRef         string
	SpecializedRef     string
	GenericBody        []byte
	SpecializedBody    []byte
	GenericDigest      string
	SpecializedDigest  string
	ProjectionDigest   string
	LedgerDigest       string
	view               *evaluationgraph.View
	mem                *rejectredirect.Memory
	opening            evaluationexplore.OpeningContext
}

// New seals a small generic→specialized evaluation graph.
func New() (*Fixture, error) {
	records := []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-generic", "generic", 1, GenericBody),
		proposal(2, "proposal-specialized", "specialized", 1, SpecializedBody),
		consolidate(3, "consolidate-generic", "generic", 1),
		consolidate(4, "consolidate-specialized", "specialized", 1),
		relation(5, "generic-specialized", "generic", 1, "specialized", 1, evaluationexplore.RelSpecializes),
	}
	canonical, err := evaluationgraph.NewFixture(records)
	if err != nil {
		return nil, err
	}
	view, err := evaluationgraph.Build(canonical)
	if err != nil {
		return nil, err
	}
	return &Fixture{
		GenericRef:        GenericRef,
		SpecializedRef:    SpecializedRef,
		GenericBody:       []byte(GenericBody),
		SpecializedBody:   []byte(SpecializedBody),
		GenericDigest:     contract.DigestBytes([]byte(GenericBody)),
		SpecializedDigest: contract.DigestBytes([]byte(SpecializedBody)),
		ProjectionDigest:  view.ProjectionDigest,
		LedgerDigest:      view.Pin.LedgerDigest,
		view:              view,
		mem:               rejectredirect.NewMemory("need-smoke-too-generic"),
		opening: evaluationexplore.OpeningContext{
			SeedRefs:      []string{GenericRef},
			Query:         GenericBody,
			CheckpointID:  "cp-opening-smoke",
			TargetAgentID: "task-agent",
		},
	}, nil
}

// FirstOffer explores from the opening seed and returns the generic Skill.
func (f *Fixture) FirstOffer() (string, error) {
	if f == nil || f.view == nil || f.mem == nil {
		return "", errors.New("smokefix: fixture is required")
	}
	got, err := rejectredirect.Offer(f.mem, f.view, f.opening, evaluationexplore.DefaultBudget())
	if err != nil {
		return "", err
	}
	if got.Offer == nil {
		return "", fmt.Errorf("smokefix: first offer missing; terminal=%s", got.Terminal)
	}
	return got.Offer.SkillReference, nil
}

// RejectTooGeneric records a too_generic rejection and explores the
// specialized descendant. The hop is the contract §3.3 audit link.
func (f *Fixture) RejectTooGeneric(offerID, reason string) (RedirectHop, error) {
	if f == nil || f.view == nil || f.mem == nil {
		return RedirectHop{}, errors.New("smokefix: fixture is required")
	}
	got, err := rejectredirect.ApplyRejection(f.mem, rejectredirect.RejectedDisposition{
		OfferID:    offerID,
		AgentID:    "task-agent",
		Ref:        GenericRef,
		ReasonCode: ReasonTooGeneric,
		Reason:     reason,
	}, f.view, f.opening, evaluationexplore.DefaultBudget())
	if err != nil {
		return RedirectHop{}, err
	}
	hop := RedirectHop{
		OfferID:    offerID,
		Ref:        GenericRef,
		ReasonCode: ReasonTooGeneric,
		Reason:     reason,
		NextQuery:  got.NextQuery,
		Action:     got.Action,
		NextSeeds:  append([]string(nil), got.NextSeeds...),
	}
	if got.Offer != nil {
		hop.NextOffer = got.Offer.SkillReference
	}
	return hop, nil
}

// Resolve returns the exact frozen body and digest for a Skill Reference.
func (f *Fixture) Resolve(ref string) (body []byte, digest string, err error) {
	if f == nil || f.view == nil {
		return nil, "", errors.New("smokefix: fixture is required")
	}
	parsed, err := evaluationexplore.ParseSkillReference(ref, evaluationgraph.ScopeEvaluation)
	if err != nil {
		return nil, "", err
	}
	node, err := f.view.Get(evaluationgraph.ScopeEvaluation, parsed)
	if err != nil {
		return nil, "", err
	}
	body = []byte(node.Body)
	return body, contract.DigestBytes(body), nil
}

// HeldOutFeedbackRejected proves held-out feedback cannot enter the
// frozen evaluation graph. The error is evaluationgraph.ErrHeldOutFeedbackInput.
func HeldOutFeedbackRejected() error {
	records := []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-generic", "generic", 1, GenericBody),
		proposal(2, "proposal-specialized", "specialized", 1, SpecializedBody),
		consolidate(3, "consolidate-generic", "generic", 1),
		consolidate(4, "consolidate-specialized", "specialized", 1),
		relation(5, "generic-specialized", "generic", 1, "specialized", 1, evaluationexplore.RelSpecializes),
		{
			Sequence: 6,
			ID:       "held-out-feedback-1",
			Kind:     evaluationgraph.RecordHeldOutFeedback,
		},
	}
	fixture, err := evaluationgraph.NewFixture(records)
	if err != nil {
		return err
	}
	_, err = evaluationgraph.Build(fixture)
	if err == nil {
		return errors.New("smokefix: held-out feedback was accepted as graph input")
	}
	if !errors.Is(err, evaluationgraph.ErrHeldOutFeedbackInput) {
		return fmt.Errorf("smokefix: held-out feedback error = %w, want %v", err, evaluationgraph.ErrHeldOutFeedbackInput)
	}
	return err
}

func proposal(seq uint64, id, lineage string, revision uint64, body string) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq,
		ID:       id,
		Kind:     evaluationgraph.RecordProposal,
		Proposal: &evaluationgraph.Proposal{
			Ref:      evaluationgraph.RevisionRef{LineageID: lineage, Revision: revision},
			Body:     body,
			Advisory: true,
		},
	}
}

func consolidate(seq uint64, id, lineage string, revision uint64) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq,
		ID:       id,
		Kind:     evaluationgraph.RecordConsolidation,
		Consolidation: &evaluationgraph.Consolidation{
			Target:      evaluationgraph.RevisionRef{LineageID: lineage, Revision: revision},
			Disposition: "retain",
		},
	}
}

func relation(seq uint64, id, fromLineage string, fromRev uint64, toLineage string, toRev uint64, kind string) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq,
		ID:       id,
		Kind:     evaluationgraph.RecordRelation,
		Relation: &evaluationgraph.Relation{
			From: evaluationgraph.RevisionRef{LineageID: fromLineage, Revision: fromRev},
			To:   evaluationgraph.RevisionRef{LineageID: toLineage, Revision: toRev},
			Kind: kind,
		},
	}
}
