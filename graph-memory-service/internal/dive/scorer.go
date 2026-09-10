package dive

import (
	"context"

	"river2.dev/graph-memory-service/internal/domain"
)

// DeterministicScorer grades found/miss trajectories from server-owned facts
// only: every submitted citation must have been served by the session
// (relevance), every counted citation must carry event-level evidence
// (groundedness), and completeness is the found/miss outcome itself. No
// model, no prose, no caller input contributes.
type DeterministicScorer struct{}

func NewDeterministicScorer() *DeterministicScorer {
	return &DeterministicScorer{}
}

func (DeterministicScorer) Score(_ context.Context, trajectory domain.DiveTrajectory) (domain.DiveDimensions, error) {
	served := make(map[string]domain.RecallItem, len(trajectory.ServedItems))
	for _, item := range trajectory.ServedItems {
		served[item.Citation.ID] = item
	}
	submitted := len(trajectory.SubmittedCitationIDs)
	if submitted == 0 {
		return domain.DiveDimensions{}, nil
	}
	known, grounded := 0, 0
	for _, citationID := range trajectory.SubmittedCitationIDs {
		item, ok := served[citationID]
		if !ok {
			continue
		}
		known++
		if len(item.Citation.EventIDs) > 0 {
			grounded++
		}
	}
	completeness := 0.0
	if trajectory.Found {
		completeness = 1.0
	}
	return domain.DiveDimensions{
		Relevance:    float64(known) / float64(submitted),
		Groundedness: float64(grounded) / float64(submitted),
		Completeness: completeness,
	}, nil
}
