package authz

import (
	"context"
	"crypto/subtle"
	"strings"

	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/ports"
)

var ErrUnauthorized = domain.NewProtocolError(401, "UNAUTHORIZED", "missing or invalid bearer token")

// Identity is the server-resolved execution identity. It never comes from a
// request payload.
type Identity struct {
	TenantID    domain.TenantID
	PrincipalID domain.PrincipalID
}

// Authenticator validates the static deployment bearer token. Comparison is
// constant time so token probing cannot measure partial matches.
type Authenticator struct {
	token []byte
}

func NewAuthenticator(token string) *Authenticator {
	return &Authenticator{token: []byte(token)}
}

func (a *Authenticator) Authenticate(authorization string) (Identity, error) {
	const scheme = "Bearer "
	if !strings.HasPrefix(authorization, scheme) {
		return Identity{}, ErrUnauthorized
	}
	credential := authorization[len(scheme):]
	if subtle.ConstantTimeCompare([]byte(credential), a.token) != 1 {
		return Identity{}, ErrUnauthorized
	}
	// Tenant and Principal binding is resolved by the caller from server state;
	// a valid token alone proves nothing about scope.
	return Identity{}, nil
}

// Authorizer makes exact Space-set authorization decisions from server-stored
// Grants. The complete requested set is validated before any read.
type Authorizer struct {
	registry ports.RegistryStore
	clock    ports.Clock
}

func NewAuthorizer(registry ports.RegistryStore, clock ports.Clock) *Authorizer {
	return &Authorizer{registry: registry, clock: clock}
}

// AuthorizeExact validates every requested Space exists and is covered by one
// or more active Grants held by the principal for the operation. It returns
// the pinned Space versions in request order. Missing Spaces are 404; spaces
// with no active covering Grant are 403 (SPACE_FORBIDDEN, or GRANT_EXPIRED
// when a covering grant exists but is exactly expired); an active grant that
// never contained the operation is GRANT_MISSING.
func (a *Authorizer) AuthorizeExact(ctx context.Context, identity Identity, spaceIDs []domain.SpaceID, purpose domain.GrantPurpose, operation domain.GrantOperation) ([]domain.PinnedSpace, error) {
	now := a.clock.Now()
	grants, err := a.registry.Grants(ctx, identity.TenantID, identity.PrincipalID)
	if err != nil {
		return nil, err
	}

	pinned := make([]domain.PinnedSpace, 0, len(spaceIDs))
	for _, spaceID := range spaceIDs {
		space, err := a.registry.Space(ctx, identity.TenantID, spaceID)
		if err != nil {
			return nil, err
		}
		covered := false
		expiredCover := false
		holdsOperation := false
		for _, grant := range grants {
			if !containsSpace(grant.SpaceIDs, spaceID) {
				continue
			}
			// The purpose fence: only grants issued for the requested plane
			// count as coverage. A lifecycle grant never authorizes tool-plane
			// operations and vice versa, even when the operations overlap.
			if grant.Purpose != purpose {
				continue
			}
			if grant.ActiveAt(now) {
				holdsOperation = holdsOperation || containsOperation(grant.Operations, operation)
				covered = covered || holdsOperation
				continue
			}
			if containsOperation(grant.Operations, operation) {
				expiredCover = true
			}
		}
		switch {
		case covered:
			pinned = append(pinned, domain.PinnedSpace{SpaceID: space.ID, MemoryVersion: space.Version})
		case expiredCover:
			return nil, domain.NewProtocolError(403, "GRANT_EXPIRED", "covering grant is expired")
		case holdsOperation:
			return nil, domain.NewProtocolError(403, "SPACE_FORBIDDEN", "no active grant covers this space for the operation")
		default:
			return nil, domain.NewProtocolError(403, "GRANT_MISSING", "principal holds no grant with this operation for this space")
		}
	}
	return pinned, nil
}

func containsSpace(spaces []domain.SpaceID, target domain.SpaceID) bool {
	for _, spaceID := range spaces {
		if spaceID == target {
			return true
		}
	}
	return false
}

func containsOperation(operations []domain.GrantOperation, target domain.GrantOperation) bool {
	for _, operation := range operations {
		if operation == target {
			return true
		}
	}
	return false
}
