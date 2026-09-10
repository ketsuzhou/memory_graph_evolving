package evidence

import (
	"context"
	"encoding/hex"
	"strings"

	"river2.dev/graph-memory-service/internal/authz"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

type Service struct {
	authorizer *authz.Authorizer
	store      ports.EvidenceStore
	clock      ports.Clock
}

func New(registry ports.RegistryStore, store ports.EvidenceStore, clock ports.Clock) *Service {
	return &Service{
		authorizer: authz.NewAuthorizer(registry, clock),
		store:      store,
		clock:      clock,
	}
}

// Stage authorizes the exact target Space for evidence.stage, validates the
// complete manifest as a domain invariant (not just at the HTTP edge), and
// persists the invisible immutable candidate.
func (s *Service) Stage(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, batch domain.EvidenceBatch) (domain.EvidenceBatch, bool, error) {
	if _, err := s.authorizer.AuthorizeExact(ctx, authz.Identity{TenantID: tenantID, PrincipalID: principalID}, []domain.SpaceID{batch.SpaceID}, domain.GrantPurposeLifecycle, domain.GrantOperationEvidenceStage); err != nil {
		return domain.EvidenceBatch{}, false, err
	}
	if err := validateManifest(batch); err != nil {
		return domain.EvidenceBatch{}, false, err
	}
	stored, duplicate, err := s.store.Stage(ctx, batch)
	if err != nil {
		return domain.EvidenceBatch{}, false, err
	}
	return stored, duplicate, nil
}

// Commit authorizes evidence.commit, re-validates the staged manifest so no
// adapter path can publish an invalid candidate, and atomically publishes it.
func (s *Service) Commit(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, batchID domain.BatchID, commitID string) (domain.EvidenceBatch, bool, error) {
	batch, err := s.store.Batch(ctx, tenantID, batchID)
	if err != nil {
		return domain.EvidenceBatch{}, false, err
	}
	if _, err := s.authorizer.AuthorizeExact(ctx, authz.Identity{TenantID: tenantID, PrincipalID: principalID}, []domain.SpaceID{batch.SpaceID}, domain.GrantPurposeLifecycle, domain.GrantOperationEvidenceCommit); err != nil {
		return domain.EvidenceBatch{}, false, err
	}
	if err := validateManifest(batch); err != nil {
		return domain.EvidenceBatch{}, false, err
	}
	return s.store.Commit(ctx, tenantID, batchID, commitID, s.clock.Now())
}

func (s *Service) Batch(ctx context.Context, tenantID domain.TenantID, principalID domain.PrincipalID, batchID domain.BatchID) (domain.EvidenceBatch, error) {
	return s.store.Batch(ctx, tenantID, batchID)
}

// validateManifest enforces the frozen evidence invariants in the use case so
// every caller of the port — HTTP today, other adapters later — stages and
// commits only complete manifests. It mirrors the wire-level rules.
func validateManifest(batch domain.EvidenceBatch) error {
	invalid := func(detail string) error {
		return domain.NewProtocolError(400, "INVALID_REQUEST", "evidence manifest violates the protocol: "+detail)
	}
	if batch.SpaceID == "" || batch.IdempotencyKey == "" || batch.StreamID == "" || batch.SourceSegmentID == "" {
		return invalid("batch identity fields must be non-empty")
	}
	if batch.Provenance.HostType != "pi-group-chat-host" {
		return invalid("provenance.host_type must be pi-group-chat-host")
	}
	if batch.Provenance.SourceKind != "room_shared" && batch.Provenance.SourceKind != "agent_private" {
		return invalid("provenance.source_kind must be room_shared or agent_private")
	}
	if batch.Provenance.HostInstanceID == "" || batch.Provenance.CapturedAt.IsZero() {
		return invalid("provenance host_instance_id and captured_at are required")
	}
	if len(batch.Provenance.ContentSHA256) != 64 {
		return invalid("provenance.content_sha256 must be 64 lowercase hex characters")
	}
	if _, err := hex.DecodeString(batch.Provenance.ContentSHA256); err != nil || strings.ToLower(batch.Provenance.ContentSHA256) != batch.Provenance.ContentSHA256 {
		return invalid("provenance.content_sha256 must be 64 lowercase hex characters")
	}
	switch batch.TerminalOutcome {
	case "settled", "failed", "aborted":
	default:
		return invalid("terminal_outcome must be settled, failed, or aborted")
	}
	if len(batch.Events) == 0 {
		return invalid("events must be a non-empty ordered array")
	}
	eventIDs := make(map[string]bool, len(batch.Events))
	previous := int64(0)
	for _, event := range batch.Events {
		if event.ID == "" || len(event.ID) > 128 || eventIDs[event.ID] {
			return invalid("event_id must be unique and 1-128 bytes")
		}
		eventIDs[event.ID] = true
		switch event.Kind {
		case "room_message", "room_tool_call", "room_tool_result", "pi_internal", "segment_terminal":
		default:
			return invalid("unknown evidence event kind " + event.Kind)
		}
		if event.Content == "" && event.Kind != "segment_terminal" {
			return invalid("content may be empty only for segment_terminal")
		}
		if event.Sequence <= previous {
			return invalid("sequence must be positive and strictly increasing")
		}
		previous = event.Sequence
		if event.OccurredAt.IsZero() {
			return invalid("occurred_at is required")
		}
	}
	linkIDs := make(map[string]bool, len(batch.Links))
	for _, link := range batch.Links {
		if link.ID == "" || len(link.ID) > 128 || linkIDs[link.ID] {
			return invalid("link_id must be unique and 1-128 bytes")
		}
		linkIDs[link.ID] = true
		switch link.Relation {
		case "mentions", "responds_to", "continues", "delegates_to":
		default:
			return invalid("unknown link relation " + link.Relation)
		}
		if !eventIDs[link.FromEventID] || !eventIDs[link.ToEventID] {
			return invalid("links must reference events in this batch")
		}
	}
	return nil
}
