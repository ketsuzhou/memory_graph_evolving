package replay

import (
	"river2.dev/graph-memory-service/internal/contract"
)

// Plan is the frozen fixture-family plan of one paired replay (GMS §5.2,
// Contract §10.4): which families the fixture set must cover, which sides
// each family observes, the envelope baseline of every family and the
// per-side packet expectations (expected terminal behavior + expected
// output digest). The plan is frozen with the request; evaluation time
// cannot alter the baseline mapping.
type Plan struct {
	Families []Family
}

// Family is one required fixture family: its Contract §7.11 domain, the
// sides it observes and the reference-envelope baseline (§10.4: A fixtures
// baseline=A, B fixtures baseline=B, overlap the policy-selected best
// applicable released source, ordinary revisions the active revision).
type Family struct {
	Name     string
	Domain   string
	Sides    []string
	Baseline map[string]any // decoder-model §7.3 SkillArtifactRef
	Packets  []Packet
}

// Packet is one planned fixture case on one side: the expected terminal
// behavior (run status + registry reason code) and the expected output
// digest of that side. Only a succeeded/empty-code expectation can ever be
// a semantic pass; a failed expectation with an infra code is an
// inconclusive case; a failed expectation with a semantic code is a
// semantic failure of the executing skill.
type Packet struct {
	ID                   string
	Side                 string
	PathDomains          []string
	Critical             bool
	ExpectedRunStatus    string
	ExpectedReasonCode   string
	ExpectedOutputDigest string
	ExpectedArtifactRef  map[string]any // decoder-model ref executed on this side
}

// RunOutput is one observed RSIH run of one packet: the Host/RSIH
// correlation (packet id + side + adapter/profile refs), the executed
// artifact ref, the observed terminal status and the output digest (and
// canonical output bytes when the adapter returns them). The same packet
// re-observed must yield the same digest (GMS §5.2 determinism).
type RunOutput struct {
	PacketID     string
	Side         string
	RunStatus    string
	ReasonCode   string // "" when succeeded
	OutputDigest string
	Output       []byte // optional canonical bytes; re-hashed when present
	CostUnits    int64
	ArtifactRef  map[string]any
	AdapterRef   map[string]any // optional; must equal the request's when present
	ProfileRef   map[string]any // optional; must equal the request's when present
}

// validate checks the closed plan shape fail-closed.
func (p *Plan) validate() error {
	if len(p.Families) == 0 {
		return newError(ReasonReplayRequestInvalid, "plan declares no families")
	}
	seen := map[string]bool{}
	for _, family := range p.Families {
		if family.Name == "" || seen[family.Name] {
			return newError(ReasonReplayRequestInvalid, "family name %q empty or duplicated", family.Name)
		}
		seen[family.Name] = true
		if !closedDomains[family.Domain] {
			return newError(ReasonReplayRequestInvalid, "family %s domain %q outside the closed enum", family.Name, family.Domain)
		}
		if len(family.Sides) == 0 {
			return newError(ReasonReplayRequestInvalid, "family %s observes no sides", family.Name)
		}
		sideSet := map[string]bool{}
		for _, side := range family.Sides {
			if !closedSides[side] {
				return newError(ReasonReplayRequestInvalid, "family %s side %q outside {baseline,candidate}", family.Name, side)
			}
			sideSet[side] = true
		}
		if family.Baseline == nil {
			return newError(ReasonReplayRequestInvalid, "family %s declares no reference-envelope baseline", family.Name)
		}
		if _, err := contract.ParseSkillArtifactRef(family.Baseline); err != nil {
			return newError(ReasonReplayRequestInvalid, "family %s baseline is not an exact §7.3 ref: %v", family.Name, err)
		}
		if len(family.Packets) == 0 {
			return newError(ReasonMissingFamily, "family %s plans no packets", family.Name)
		}
		for _, packet := range family.Packets {
			if !sideSet[packet.Side] {
				return newError(ReasonReplayRequestInvalid, "packet %s side %q not observed by family %s", packet.ID, packet.Side, family.Name)
			}
			if packet.ExpectedRunStatus != "succeeded" && packet.ExpectedRunStatus != "failed" {
				return newError(ReasonReplayRequestInvalid, "packet %s expected run status %q invalid", packet.ID, packet.ExpectedRunStatus)
			}
			if packet.ExpectedRunStatus == "failed" && packet.ExpectedReasonCode == "" {
				return newError(ReasonReplayRequestInvalid, "packet %s expects failure without a reason code", packet.ID)
			}
			if packet.ExpectedRunStatus == "succeeded" && packet.ExpectedReasonCode != "" {
				return newError(ReasonReplayRequestInvalid, "packet %s expects success with a reason code", packet.ID)
			}
			if !digestShaped(packet.ExpectedOutputDigest) {
				return newError(ReasonReplayRequestInvalid, "packet %s expected output digest %q malformed", packet.ID, packet.ExpectedOutputDigest)
			}
			if packet.ExpectedArtifactRef == nil {
				return newError(ReasonReplayRequestInvalid, "packet %s declares no artifact ref", packet.ID)
			}
		}
	}
	return nil
}

func digestShaped(digest string) bool {
	if len(digest) != len("sha256:")+64 {
		return false
	}
	if digest[:7] != "sha256:" {
		return false
	}
	for i := 7; i < len(digest); i++ {
		c := digest[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
