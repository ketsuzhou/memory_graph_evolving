package recall

import (
	"context"
	"fmt"
	"time"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

type Service struct {
	authorizer *authz.Authorizer
	store      ports.RecallStore
	clock      ports.Clock
}

func New(registry ports.RegistryStore, store ports.RecallStore, clock ports.Clock) *Service {
	return &Service{
		authorizer: authz.NewAuthorizer(registry, clock),
		store:      store,
		clock:      clock,
	}
}

// Recall validates the complete requested Space set before any read. A
// missing Space is 404 and an unauthorized or exactly expired one is 403;
// neither produces partial items. After authorization, a retrieval failure is
// a typed in-band degradation over the exact requested scope, never a widened
// fallback. The caller's deadline_ms bounds the whole retrieval: hitting it
// degrades to timed_out with whatever authorized items were already fetched.
//
// A request that pins a space to an earlier version (SpaceVersions — the
// frozen-head versions a consolidation-cut manifest publishes) is resolved to
// that snapshot here: the pin replaces the current-head authorization pin, so
// retrieval filters to exactly the evidence at or below the pinned version
// instead of annotating a live-head read. A pin ahead of the space's head is
// rejected with STALE_PROJECTION_HEAD (422) — it can never silently widen to
// the live head.
func (s *Service) Recall(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, request domain.RecallRequest) (domain.RecallResult, error) {
	pinned, err := s.authorizer.AuthorizeExact(ctx, authz.Identity{TenantID: tenantID, PrincipalID: principalID}, request.SpaceIDs, domain.GrantPurposeLifecycle, domain.GrantOperationRecall)
	if err != nil {
		return domain.RecallResult{}, err
	}
	if len(request.SpaceVersions) > 0 {
		for index, pin := range pinned {
			requested, ok := request.SpaceVersions[pin.SpaceID]
			if !ok {
				continue
			}
			if requested < 0 || requested > pin.MemoryVersion {
				return domain.RecallResult{}, domain.NewProtocolError(422, "STALE_PROJECTION_HEAD",
					fmt.Sprintf("pinned version %d for space %s is outside [0, current head %d]", requested, pin.SpaceID, pin.MemoryVersion))
			}
			pin.MemoryVersion = requested
			pinned[index] = pin
		}
	}

	if request.DeadlineMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(request.DeadlineMS)*time.Millisecond)
		defer cancel()
	}
	if ctx.Err() != nil {
		return timedOut(request, nil), nil
	}
	outcome, err := s.store.Retrieve(ctx, tenantID, pinned, request.Query, request.MaxResults)
	if ctx.Err() != nil {
		return timedOut(request, outcome.Items), nil
	}
	if err != nil {
		return domain.RecallResult{
			RequestID: request.RequestID,
			Items:     outcome.Items,
			Degradation: domain.RecallDegradation{
				State:   "partial",
				Reasons: []string{"retrieval failure over the exact requested scope: " + err.Error()},
			},
		}, nil
	}
	if outcome.Degradation != nil {
		return domain.RecallResult{RequestID: request.RequestID, Items: outcome.Items, Degradation: *outcome.Degradation}, nil
	}
	state := "complete"
	if len(outcome.Items) == 0 {
		state = "empty"
	}
	return domain.RecallResult{
		RequestID:   request.RequestID,
		Items:       outcome.Items,
		Degradation: domain.RecallDegradation{State: state},
	}, nil
}

func timedOut(request domain.RecallRequest, items []domain.RecallItem) domain.RecallResult {
	if items == nil {
		items = []domain.RecallItem{}
	}
	return domain.RecallResult{
		RequestID: request.RequestID,
		Items:     items,
		Degradation: domain.RecallDegradation{
			State:   "timed_out",
			Reasons: []string{"deadline_ms budget exceeded before retrieval completed"},
		},
	}
}
