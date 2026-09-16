// Package artifactsecurity implements PG-24 tenant artifact confidentiality:
// tenant-scoped HMAC identities, AES-GCM envelope encryption bound to
// artifact AAD, and cryptographic erasure via per-artifact KEK rotation that
// leaves only non-sensitive tombstones.
package artifactsecurity

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"

	"river2.dev/graph-memory-service/internal/domain"
)

var ErrArtifactSecurityNotImplemented = errors.New("artifactsecurity: not implemented")

var (
	// ErrArtifactErased is returned when an envelope was sealed under a KEK
	// that has since been rotated out of existence: the ciphertext is
	// unrecoverable by construction.
	ErrArtifactErased = errors.New("artifactsecurity: artifact was cryptographically erased")
	ErrSealInputEmpty = errors.New("artifactsecurity: seal requires a payload")
	// ErrEraseReasonInvalid rejects free-text erasure reasons: tombstones
	// are non-sensitive audit records (SC-7.3/7.6), so only the closed
	// reason-code vocabulary below is admissible.
	ErrEraseReasonInvalid = errors.New("artifactsecurity: erase reason must be a closed non-sensitive reason code")
)

// EraseReason is the closed vocabulary of non-sensitive erasure reasons.
// Free text (potentially PII or payload fragments) never reaches a
// tombstone; additions are append-only like the contract reason codes.
type EraseReason string

const (
	EraseUserDeletion       EraseReason = "USER_DELETION_REQUEST"
	EraseTrainingWithdrawal EraseReason = "TRAINING_WITHDRAWAL"
	EraseRetentionExpired   EraseReason = "RETENTION_EXPIRED"
	EraseSecurityIncident   EraseReason = "SECURITY_INCIDENT"
)

// validEraseReason is the closed membership check: empty, unknown, or
// over-long values are all invalid.
func validEraseReason(reason EraseReason) bool {
	switch reason {
	case EraseUserDeletion, EraseTrainingWithdrawal, EraseRetentionExpired, EraseSecurityIncident:
		return true
	}
	return false
}

// ValidEraseReason exposes the closed membership check so sibling authorities
// (lineage tombstone cascades) admit exactly the reason vocabulary artifact
// erase does.
func ValidEraseReason(reason EraseReason) bool {
	return validEraseReason(reason)
}

type ArtifactID string
type KeyID string
type TenantIdentity string

type AAD struct {
	TenantID   domain.TenantID
	ArtifactID ArtifactID
	Purpose    string
}

type Envelope struct {
	ArtifactID     ArtifactID
	TenantIdentity TenantIdentity
	WrappedDEK     []byte
	Ciphertext     []byte
	KeyID          KeyID
	AAD            AAD
}

// KeyTombstone deliberately retains non-sensitive audit facts only. It is
// also the erasure state consumed by a restarted service: a tombstoned KeyID
// can never unwrap again even though initial KEKs are derivable.
type KeyTombstone struct {
	TenantID   domain.TenantID
	ArtifactID ArtifactID
	KeyID      KeyID
	Reason     EraseReason
}

// devMasterSecret is the tracer-bullet master key. Production composition
// (PG-50A) must source tenant KEKs from a KMS; the rotation-based erasure
// below is unchanged by that substitution.
var devMasterSecret = []byte("pg24-artifactsecurity-dev-master")

// kekKey scopes one KEK to a single (tenant, artifact) pair: erasing one
// artifact rotates only its own key and never touches sibling artifacts.
type kekKey struct {
	tenant   domain.TenantID
	artifact ArtifactID
}

// Service keeps one mutable KEK per artifact. Initial KEKs are derived
// deterministically from the master secret, so envelopes survive service
// restarts; erasure replaces the artifact's KEK with fresh entropy (the
// rotation) and records the destroyed KeyID as a tombstone, so every prior
// envelope of that artifact becomes undecryptable while tombstones retain
// only non-sensitive facts.
type Service struct {
	mu         sync.Mutex
	keks       map[kekKey][]byte
	tombstones []KeyTombstone
}

func NewService() *Service {
	return &Service{keks: map[kekKey][]byte{}}
}

// NewServiceWithTombstones restores persisted erasure state; PG-50A wires
// the durable tombstone ledger through this constructor.
func NewServiceWithTombstones(tombstones []KeyTombstone) *Service {
	service := NewService()
	service.tombstones = append([]KeyTombstone(nil), tombstones...)
	return service
}

// identityKey derives the stable tenant identity key. It is independent of
// every artifact KEK, so identities are stable across erasures and
// rotations, and different tenants yield unrelated keys.
func identityKey(tenantID domain.TenantID) []byte {
	mac := hmac.New(sha256.New, devMasterSecret)
	mac.Write([]byte("identity|"))
	mac.Write([]byte(tenantID))
	return mac.Sum(nil)
}

// TenantIdentity derives a keyed, tenant-scoped identity digest: the same
// principal value under different tenants yields unrelated digests, the
// input is not recoverable, and the digest survives erasure and rotation.
func (s *Service) TenantIdentity(_ context.Context, tenantID domain.TenantID, principalValue []byte) (TenantIdentity, error) {
	return tenantIdentity(identityKey(tenantID), principalValue), nil
}

func tenantIdentity(key, principalValue []byte) TenantIdentity {
	mac := hmac.New(sha256.New, key)
	mac.Write(principalValue)
	return TenantIdentity("tid-" + hex.EncodeToString(mac.Sum(nil))[:24])
}

// Seal encrypts the payload with a fresh DEK (AES-256-GCM, AAD-bound to
// tenant/artifact/purpose) and wraps the DEK under the artifact KEK.
func (s *Service) Seal(_ context.Context, aad AAD, payload []byte) (*Envelope, error) {
	if len(payload) == 0 {
		return nil, ErrSealInputEmpty
	}
	if aad.TenantID == "" || aad.ArtifactID == "" || aad.Purpose == "" {
		return nil, errors.New("artifactsecurity: seal requires tenant, artifact, and purpose")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := kekKey{aad.TenantID, aad.ArtifactID}
	if s.keks[key] == nil && s.tombstonedLocked(key, "") {
		// Restored erasure state: the derivable initial key was destroyed
		// before the restart, so this artifact's next envelope needs fresh
		// entropy instead of resurrecting the destroyed key.
		s.keks[key] = randomBytes(32)
	}
	kek := s.kekLocked(key)
	dek := randomBytes(32)
	ciphertext, err := gcmSeal(dek, aadBytes(aad), payload)
	if err != nil {
		return nil, err
	}
	wrappedDEK, err := gcmSeal(kek, aadBytes(aad), dek)
	if err != nil {
		return nil, err
	}
	identity := tenantIdentity(identityKey(aad.TenantID), dek)
	return &Envelope{
		ArtifactID:     aad.ArtifactID,
		TenantIdentity: identity,
		WrappedDEK:     wrappedDEK,
		Ciphertext:     ciphertext,
		KeyID:          keyID(kek),
		AAD:            aad,
	}, nil
}

// Open unwraps the DEK under the artifact's CURRENT KEK and decrypts. The
// whole operation holds the service lock, so a concurrent erase can never
// land between key lookup and decrypt. An erased (rotated) KEK, a tombstoned
// KeyID, or tampered AAD fails closed with ErrArtifactErased.
func (s *Service) Open(_ context.Context, envelope *Envelope) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("artifactsecurity: open requires an envelope")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := kekKey{envelope.AAD.TenantID, envelope.AAD.ArtifactID}
	if s.tombstonedLocked(key, envelope.KeyID) {
		return nil, ErrArtifactErased
	}
	kek := s.kekLocked(key)
	dek, err := gcmOpen(kek, aadBytes(envelope.AAD), envelope.WrappedDEK)
	if err != nil {
		return nil, ErrArtifactErased
	}
	plaintext, err := gcmOpen(dek, aadBytes(envelope.AAD), envelope.Ciphertext)
	if err != nil {
		return nil, ErrArtifactErased
	}
	return plaintext, nil
}

// Erase rotates the ARTIFACT's KEK: the previous key is dropped without
// export, so every envelope sealed under it is cryptographically
// unrecoverable. Sibling artifacts of the same tenant keep their own keys.
// Only the non-sensitive tombstone survives.
func (s *Service) Erase(_ context.Context, tenantID domain.TenantID, artifactID ArtifactID, reason EraseReason) (*KeyTombstone, error) {
	if !validEraseReason(reason) {
		return nil, ErrEraseReasonInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := kekKey{tenantID, artifactID}
	destroyed := keyID(s.kekLocked(key))
	// Rotation is the erasure: replace the KEK with fresh entropy so the
	// destroyed key exists nowhere.
	s.keks[key] = randomBytes(32)
	tombstone := &KeyTombstone{
		TenantID:   tenantID,
		ArtifactID: artifactID,
		KeyID:      destroyed,
		Reason:     reason,
	}
	s.tombstones = append(s.tombstones, *tombstone)
	return tombstone, nil
}

// Tombstones returns the retained non-sensitive erasure records.
func (s *Service) Tombstones() []KeyTombstone {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]KeyTombstone(nil), s.tombstones...)
}

// tombstonedLocked reports whether this artifact has any erasure record;
// with a non-empty kid it checks that exact destroyed KeyID, so a restarted
// service (which can re-derive the initial KEK) still refuses every
// envelope sealed under the destroyed key.
func (s *Service) tombstonedLocked(key kekKey, kid KeyID) bool {
	for _, tombstone := range s.tombstones {
		if tombstone.TenantID != key.tenant || tombstone.ArtifactID != key.artifact {
			continue
		}
		if kid == "" || tombstone.KeyID == kid {
			return true
		}
	}
	return false
}

// kekLocked returns the artifact KEK: the rotated one after an erasure, or
// the deterministically derived initial key on first use (which is what
// makes sealed envelopes readable after a service restart).
func (s *Service) kekLocked(key kekKey) []byte {
	if kek, ok := s.keks[key]; ok {
		return kek
	}
	h := sha256.New()
	h.Write(devMasterSecret)
	h.Write([]byte("\x00artifact-kek\x00"))
	h.Write([]byte(key.tenant))
	h.Write([]byte("\x00"))
	h.Write([]byte(key.artifact))
	kek := h.Sum(nil)
	s.keks[key] = kek
	return kek
}

func keyID(kek []byte) KeyID {
	sum := sha256.Sum256(kek)
	return KeyID("key-" + hex.EncodeToString(sum[:])[:12])
}

// aadBytes encodes the AAD unambiguously: every field is length-prefixed,
// so no two distinct (tenant, artifact, purpose) triples can produce the
// same binding bytes.
func aadBytes(aad AAD) []byte {
	fields := [][]byte{[]byte(aad.TenantID), []byte(aad.ArtifactID), []byte(aad.Purpose)}
	out := make([]byte, 0, 24+len(aad.TenantID)+len(aad.ArtifactID)+len(aad.Purpose))
	var length [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		out = append(out, length[:]...)
		out = append(out, field...)
	}
	return out
}

func gcmSeal(key, aad, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := randomBytes(gcm.NonceSize())
	return gcm.Seal(nonce, nonce, plaintext, aad), nil
}

func gcmOpen(key, aad, sealed []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(sealed) < gcm.NonceSize() {
		return nil, errors.New("artifactsecurity: sealed input too short")
	}
	nonce, ciphertext := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	return gcm.Open(nil, nonce, ciphertext, aad)
}

func randomBytes(n int) []byte {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic("artifactsecurity: crypto/rand unavailable: " + err.Error())
	}
	return buf
}
