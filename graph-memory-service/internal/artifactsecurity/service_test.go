package artifactsecurity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/domain"
)

func TestTenantIdentityIsTenantScopedHMACAndCannotCrossTenant(t *testing.T) {
	svc := NewService()
	identity, err := svc.TenantIdentity(context.Background(), domain.TenantID("tenant-a"), []byte("principal-private-value"))
	if errors.Is(err, ErrArtifactSecurityNotImplemented) {
		t.Fatal("RED: tenant-scoped HMAC identity is not implemented")
	}
	if err != nil {
		t.Fatalf("tenant identity: %v", err)
	}
	if identity == "" {
		t.Fatal("tenant identity must not be empty")
	}
	other, err := svc.TenantIdentity(context.Background(), domain.TenantID("tenant-b"), []byte("principal-private-value"))
	if err != nil {
		t.Fatalf("cross-tenant identity: %v", err)
	}
	if other == identity {
		t.Fatal("tenant-scoped HMAC identity collided across tenants for the same value")
	}
	same, err := svc.TenantIdentity(context.Background(), domain.TenantID("tenant-a"), []byte("principal-private-value"))
	if err != nil {
		t.Fatalf("repeat identity: %v", err)
	}
	if same != identity {
		t.Fatal("tenant-scoped HMAC identity was not deterministic within a tenant")
	}
}

func TestSealUsesTenantEnvelopeEncryptionAndBoundAAD(t *testing.T) {
	svc := NewService()
	envelope, err := svc.Seal(context.Background(), AAD{TenantID: "tenant-a", ArtifactID: "artifact-1", Purpose: "trajectory-export"}, []byte("sensitive payload"))
	if errors.Is(err, ErrArtifactSecurityNotImplemented) {
		t.Fatal("RED: tenant envelope encryption with artifact-bound AAD is not implemented")
	}
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if len(envelope.Ciphertext) == 0 || len(envelope.WrappedDEK) == 0 {
		t.Fatalf("envelope lacks encrypted payload or wrapped DEK: %#v", envelope)
	}
}

func TestCryptoErasureLeavesOnlyNonSensitiveTombstone(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	aad := AAD{TenantID: "tenant-a", ArtifactID: "artifact-1", Purpose: "trajectory-export"}
	envelope, err := svc.Seal(ctx, aad, []byte("sensitive payload"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if plaintext, err := svc.Open(ctx, envelope); err != nil || string(plaintext) != "sensitive payload" {
		t.Fatalf("seal/open roundtrip failed: plaintext=%q err=%v", plaintext, err)
	}
	identityBefore, err := svc.TenantIdentity(ctx, domain.TenantID("tenant-a"), []byte("principal-private-value"))
	if err != nil {
		t.Fatalf("identity before erase: %v", err)
	}
	tombstone, err := svc.Erase(ctx, domain.TenantID("tenant-a"), ArtifactID("artifact-1"), EraseUserDeletion)
	if errors.Is(err, ErrArtifactSecurityNotImplemented) {
		t.Fatal("RED: cryptographic erasure with a non-sensitive tombstone is not implemented")
	}
	if err != nil {
		t.Fatalf("erase: %v", err)
	}
	if tombstone.ArtifactID != "artifact-1" || tombstone.KeyID == "" {
		t.Fatalf("invalid erasure tombstone: %#v", tombstone)
	}
	if string(tombstone.Reason) == "sensitive payload" || len(tombstone.Reason) > 64 {
		t.Fatalf("tombstone retained sensitive content: %#v", tombstone)
	}
	// KEK rotation is the erasure: the old envelope is unrecoverable.
	if _, err := svc.Open(ctx, envelope); !errors.Is(err, ErrArtifactErased) {
		t.Fatalf("post-erasure open did not fail closed: err=%v", err)
	}
	// Erasure is scoped to the artifact: a sibling artifact of the same
	// tenant keeps its own KEK and stays readable.
	sibling, err := svc.Seal(ctx, AAD{TenantID: "tenant-a", ArtifactID: "artifact-2", Purpose: "trajectory-export"}, []byte("sibling payload"))
	if err != nil {
		t.Fatalf("seal sibling: %v", err)
	}
	if plaintext, err := svc.Open(ctx, sibling); err != nil || string(plaintext) != "sibling payload" {
		t.Fatalf("erase of artifact-1 damaged sibling artifact-2: err=%v", err)
	}
	// The tenant identity key is independent of artifact KEKs: identities
	// survive erasure unchanged.
	identityAfter, err := svc.TenantIdentity(ctx, domain.TenantID("tenant-a"), []byte("principal-private-value"))
	if err != nil {
		t.Fatalf("identity after erase: %v", err)
	}
	if identityAfter != identityBefore {
		t.Fatalf("tenant identity was not stable across erasure: before=%s after=%s", identityBefore, identityAfter)
	}
}

func TestEnvelopeSurvivesServiceRestartAndErasedStateRestores(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	aad := AAD{TenantID: "tenant-a", ArtifactID: "artifact-1", Purpose: "trajectory-export"}
	live, err := svc.Seal(ctx, aad, []byte("durable payload"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	// A restarted service re-derives the initial KEK, so an un-erased
	// envelope stays readable.
	restarted := NewService()
	plaintext, err := restarted.Open(ctx, live)
	if err != nil || string(plaintext) != "durable payload" {
		t.Fatalf("restart broke a live envelope: plaintext=%q err=%v", plaintext, err)
	}
	// Erasure survives restart too: the tombstone (persisted by PG-50A,
	// restored here through the constructor) pins the destroyed KeyID, so
	// the pre-erasure envelope fails closed on the restarted service.
	if _, err := svc.Erase(ctx, domain.TenantID("tenant-a"), ArtifactID("artifact-1"), EraseUserDeletion); err != nil {
		t.Fatalf("erase: %v", err)
	}
	restored := NewServiceWithTombstones(svc.Tombstones())
	if _, err := restored.Open(ctx, live); !errors.Is(err, ErrArtifactErased) {
		t.Fatalf("restored tombstone did not fail closed on the erased envelope: err=%v", err)
	}
	// The restored service also mints fresh entropy for new envelopes of
	// the erased artifact instead of resurrecting the destroyed key, and
	// those new envelopes open normally.
	fresh, err := restored.Seal(ctx, aad, []byte("post-restart payload"))
	if err != nil {
		t.Fatalf("post-restart seal: %v", err)
	}
	if fresh.KeyID == tombstonedKeyID(t, svc, live) {
		t.Fatal("post-restart seal reused the destroyed key")
	}
	if plaintext, err := restored.Open(ctx, fresh); err != nil || string(plaintext) != "post-restart payload" {
		t.Fatalf("post-restart seal/open roundtrip failed: plaintext=%q err=%v", plaintext, err)
	}
	if _, err := restored.Open(ctx, live); !errors.Is(err, ErrArtifactErased) {
		t.Fatalf("post-restart seal resurrected the erased envelope: err=%v", err)
	}
}

func tombstonedKeyID(t *testing.T, svc *Service, envelope *Envelope) KeyID {
	t.Helper()
	for _, tombstone := range svc.Tombstones() {
		if tombstone.KeyID == envelope.KeyID {
			return tombstone.KeyID
		}
	}
	t.Fatalf("envelope KeyID %s was not covered by a tombstone", envelope.KeyID)
	return ""
}

func TestAADBindingIsUnambiguousAcrossFieldBoundaries(t *testing.T) {
	// Length-prefixing all three fields means no two distinct triples can
	// produce the same binding bytes, even with adversarial splits.
	boundaryA := aadBytes(AAD{TenantID: "tenant-a", ArtifactID: "artifact", Purpose: "bc"})
	boundaryB := aadBytes(AAD{TenantID: "tenant-a", ArtifactID: "artifact", Purpose: "c"})
	if string(boundaryA) == string(boundaryB) {
		t.Fatal("distinct (artifact, purpose) splits produced identical AAD bytes")
	}
	crossField := aadBytes(AAD{TenantID: "tenant-a", ArtifactID: "artifact", Purpose: "bc"})
	shifted := aadBytes(AAD{TenantID: "tenant-a", ArtifactID: "artifactb", Purpose: "c"})
	if string(crossField) == string(shifted) {
		t.Fatal("AAD bytes were ambiguous across the artifact/purpose field boundary")
	}

	// The binding is enforced by the AEAD: any envelope field tampering is
	// detected as an erasure/failure, never decoded as a different artifact.
	svc := NewService()
	ctx := context.Background()
	envelope, err := svc.Seal(ctx, AAD{TenantID: "tenant-a", ArtifactID: "artifact-1", Purpose: "trajectory-export"}, []byte("sensitive payload"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	tampered := *envelope
	tampered.AAD.Purpose = "trajectory-export-x"
	if _, err := svc.Open(ctx, &tampered); !errors.Is(err, ErrArtifactErased) {
		t.Fatalf("tampered AAD purpose was not rejected: err=%v", err)
	}
	tamperedAgain := *envelope
	tamperedAgain.AAD.ArtifactID = "artifact-2"
	if _, err := svc.Open(ctx, &tamperedAgain); !errors.Is(err, ErrArtifactErased) {
		t.Fatalf("tampered AAD artifact was not rejected: err=%v", err)
	}
}

// P1-10: tombstone reasons are a closed non-sensitive vocabulary; free text
// (PII, payload fragments, over-long strings) never reaches the ledger.
func TestEraseRejectsFreeTextAndPIIReasons(t *testing.T) {
	svc := NewService()
	ctx := context.Background()
	aad := AAD{TenantID: "tenant-a", ArtifactID: "artifact-pii", Purpose: "trajectory-export"}
	if _, err := svc.Seal(ctx, aad, []byte("sensitive payload")); err != nil {
		t.Fatalf("seal: %v", err)
	}
	for _, reason := range []EraseReason{
		"",
		"revoked",
		"user alice@example.com asked for deletion of file /home/alice/secret.txt",
		"sensitive payload",
		EraseReason(strings.Repeat("A", 65)),
		EraseReason("USER_DELETION_REQUEST\nextra"),
	} {
		if _, err := svc.Erase(ctx, domain.TenantID("tenant-a"), ArtifactID("artifact-pii"), reason); !errors.Is(err, ErrEraseReasonInvalid) {
			t.Fatalf("free-text/PII erase reason %q was accepted: err=%v", reason, err)
		}
	}
	if len(svc.Tombstones()) != 0 {
		t.Fatalf("rejected erasures left tombstones behind: %#v", svc.Tombstones())
	}
	// Every closed code is admissible and yields a non-sensitive tombstone.
	for _, reason := range []EraseReason{EraseUserDeletion, EraseTrainingWithdrawal, EraseRetentionExpired, EraseSecurityIncident} {
		tombstone, err := svc.Erase(ctx, domain.TenantID("tenant-a"), ArtifactID("artifact-pii"), reason)
		if err != nil {
			t.Fatalf("closed reason %q rejected: %v", reason, err)
		}
		if tombstone.Reason != reason || len(tombstone.Reason) > 64 {
			t.Fatalf("tombstone reason not the closed code: %#v", tombstone)
		}
	}
}
