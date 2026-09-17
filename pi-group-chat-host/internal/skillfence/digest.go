package skillfence

import (
	"crypto/sha256"
	"encoding/hex"
)

// BodyDigest is the Host-authored exact-body identity: sha256 of the
// raw bytes that must match the GMS view digest and the Pi capture.
func BodyDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}
