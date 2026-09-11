package replay

import (
	"sort"

	"river2.dev/graph-memory-service/internal/contract"
)

// Canonicalize verifies the Host/RSIH correlation of every observed run
// against the frozen request and family plan and mints the authoritative
// §7.11 ReplayResult (GMS §5.3).
//
// Determinism contract: the same request + plan + multiset of runs always
// yields byte-identical canonical bytes and the same result digest, input
// order notwithstanding.
func (s *Service) Canonicalize(req *Request, plan *Plan, runs []RunOutput) (*Result, error) {
	if req == nil {
		return nil, newError(ReasonReplayRequestInvalid, "nil replay request")
	}
	if plan == nil {
		return nil, newError(ReasonReplayRequestInvalid, "nil family plan")
	}
	if err := plan.validate(); err != nil {
		return nil, err
	}
	if err := s.checkPlanAgainstRequest(req, plan); err != nil {
		return nil, err
	}

	// Index the planned packets and validate the declared sides.
	type packetKey struct{ family, id, side string }
	packets := map[packetKey]Packet{}
	familyOfPacket := map[string]string{}
	for _, family := range plan.Families {
		for _, packet := range family.Packets {
			key := packetKey{family.Name, packet.ID, packet.Side}
			if _, dup := packets[key]; dup {
				return nil, newError(ReasonReplayRequestInvalid, "packet %s/%s planned twice in family %s", packet.ID, packet.Side, family.Name)
			}
			packets[key] = packet
			familyOfPacket[packet.ID+"\x1f"+packet.Side] = family.Name
		}
	}

	// Correlate every observed run (fail-closed before any counting).
	observed := map[packetKey][]RunOutput{}
	for i, run := range runs {
		if !closedSides[run.Side] {
			return nil, newError(ReasonReplayRequestInvalid, "run %d side %q outside {baseline,candidate}", i, run.Side)
		}
		family, ok := familyOfPacket[run.PacketID+"\x1f"+run.Side]
		if !ok {
			return nil, newError(ReasonReplayRequestInvalid,
				"run of packet %q on side %q correlates to no planned packet (Host/RSIH correlation failure)", run.PacketID, run.Side)
		}
		packet := packets[packetKey{family, run.PacketID, run.Side}]
		if err := s.checkRunCorrelation(req, packet, run); err != nil {
			return nil, err
		}
		key := packetKey{family, run.PacketID, run.Side}
		observed[key] = append(observed[key], run)
	}

	// Family completeness: every planned side of every family observed.
	for _, family := range plan.Families {
		for _, side := range family.Sides {
			seen := false
			for _, packet := range family.Packets {
				if packet.Side == side && len(observed[packetKey{family.Name, packet.ID, side}]) > 0 {
					seen = true
				}
			}
			if !seen {
				return nil, newError(ReasonMissingFamily,
					"family %s observes no %s-side runs (fixture family incomplete)", family.Name, side)
			}
		}
	}

	// Nondeterminism: same packet+side, different digests (checked before
	// digest verification — a diverging rerun is never attributed to
	// either output).
	nondeterministic := map[packetKey]bool{}
	for key, group := range observed {
		first := group[0].OutputDigest
		for _, run := range group[1:] {
			if run.OutputDigest != first {
				nondeterministic[key] = true
			}
		}
	}

	// Classify and aggregate.
	agg := newAggregator()
	for _, family := range plan.Families {
		fam := familyAggregation{name: family.Name, domain: family.Domain, baseline: family.Baseline}
		for _, packet := range family.Packets {
			key := packetKey{family.Name, packet.ID, packet.Side}
			group := observed[key]
			var category string
			reason := packet.ExpectedReasonCode
			switch {
			case nondeterministic[key]:
				category = CategoryNondeterministic
			default:
				// Deterministic group: every run must reproduce the
				// expected output digest (and its own bytes).
				for i, run := range group {
					if run.OutputDigest != packet.ExpectedOutputDigest {
						return nil, newError(ReasonDigestMismatch,
							"packet %s/%s run %d output digest %s != expected %s", packet.ID, packet.Side, i, run.OutputDigest, packet.ExpectedOutputDigest)
					}
					if len(run.Output) > 0 && contract.DigestBytes(run.Output) != run.OutputDigest {
						return nil, newError(ReasonDigestMismatch,
							"packet %s/%s run %d output bytes do not hash to the declared digest", packet.ID, packet.Side, i)
					}
				}
				switch packet.ExpectedRunStatus {
				case "succeeded":
					category = CategorySemanticPass
				default:
					if s.isInfraCode(packet.ExpectedReasonCode) {
						category = CategoryInfra
					} else {
						category = CategorySemanticFail
					}
				}
			}
			recovery := false
			for _, domain := range packet.PathDomains {
				if domain == "recovery" {
					recovery = true
				}
			}
			record := RunRecord{
				PacketID: packet.ID, Family: family.Name, Domain: family.Domain,
				Side: packet.Side, Critical: packet.Critical, Recovery: recovery,
				Category: category, ReasonCode: reason,
			}
			agg.add(packet, record, group)
			fam.addPacket(packet, record, group)
		}
		agg.addFamily(fam)
	}
	if agg.overflow() {
		return s.inconclusiveResult(req, plan, agg, ReasonUtilityArithmeticOverflow)
	}
	if len(nondeterministic) > 0 {
		return s.inconclusiveResult(req, plan, agg, ReasonReplayNondeterministic)
	}
	return s.mintResult(req, plan, agg, StatusSucceeded, "")
}

// checkPlanAgainstRequest freezes the envelope mapping: every family
// baseline must be one of the request's exact baselines.
func (s *Service) checkPlanAgainstRequest(req *Request, plan *Plan) error {
	allowed := map[string]bool{}
	for _, doc := range req.baselineDocs {
		allowed[contract.CanonicalKey(doc)] = true
	}
	for _, family := range plan.Families {
		if !allowed[contract.CanonicalKey(family.Baseline)] {
			return newError(ReasonRefMismatch,
				"family %s envelope baseline is not one of the request's baseline skill refs (reference envelope frozen by §10.4)", family.Name)
		}
	}
	if req.IsMerge() && len(req.requiredSourceHeads) == 0 {
		return newError(ReasonReplayRequestInvalid, "merge candidate request carries no frozen required_source_heads")
	}
	return nil
}

// checkRunCorrelation verifies one observed run against the frozen request
// and its planned packet: adapter/profile exactness, the executed artifact
// ref, and the side-vs-ref-type consistency (a baseline/candidate swap
// rejects REPLAY_REQUEST_INVALID).
func (s *Service) checkRunCorrelation(req *Request, packet Packet, run RunOutput) error {
	if run.AdapterRef != nil {
		if contract.CanonicalKey(run.AdapterRef) != contract.CanonicalKey(versionedRefDoc(req.AdapterRef())) {
			return newError(ReasonRefMismatch,
				"packet %s/%s run correlates to a different runtime adapter than the frozen request", packet.ID, packet.Side)
		}
	}
	if run.ProfileRef != nil {
		if contract.CanonicalKey(run.ProfileRef) != contract.CanonicalKey(versionedRefDoc(req.ProfileRef())) {
			return newError(ReasonRefMismatch,
				"packet %s/%s run correlates to a different replay profile than the frozen request", packet.ID, packet.Side)
		}
	}
	if run.ArtifactRef == nil {
		return newError(ReasonReplayRequestInvalid, "packet %s/%s run carries no executed artifact ref", packet.ID, packet.Side)
	}
	if contract.CanonicalKey(run.ArtifactRef) != contract.CanonicalKey(packet.ExpectedArtifactRef) {
		return newError(ReasonRefMismatch,
			"packet %s/%s run executed a different artifact than planned", packet.ID, packet.Side)
	}
	switch run.Side {
	case SideBaseline:
		// Baseline side must execute an exact §7.3 skill ref from the
		// request baselines; a candidate ref here is a side swap.
		if _, isCandidate := run.ArtifactRef["candidate_id"]; isCandidate {
			return newError(ReasonReplayRequestInvalid,
				"packet %s executes the candidate artifact ref on the baseline side (baseline/candidate must not be swapped)", packet.ID)
		}
		ref, err := contract.ParseSkillArtifactRef(run.ArtifactRef)
		if err != nil {
			return newError(ReasonRefMismatch, "packet %s baseline artifact ref invalid: %v", packet.ID, err)
		}
		if !refInBaselines(req, ref) {
			return newError(ReasonRefMismatch,
				"packet %s executes baseline %s v%s which is not among the request baselines", packet.ID, ref.LineageID, ref.Version)
		}
	case SideCandidate:
		// Candidate side must execute exactly the request's candidate ref.
		if _, isCandidate := run.ArtifactRef["candidate_id"]; !isCandidate {
			return newError(ReasonReplayRequestInvalid,
				"packet %s executes a released skill artifact ref on the candidate side (baseline/candidate must not be swapped)", packet.ID)
		}
		ref, err := contract.ParseCandidateArtifactRef(run.ArtifactRef)
		if err != nil {
			return newError(ReasonRefMismatch, "packet %s candidate artifact ref invalid: %v", packet.ID, err)
		}
		if contract.CanonicalKey(run.ArtifactRef) != contract.CanonicalKey(req.CandidateRefDoc()) {
			return newError(ReasonRefMismatch,
				"packet %s executes candidate %s which is not the request candidate", packet.ID, ref.CandidateID)
		}
	}
	return nil
}

// refInBaselines resolves the exact §6.2 identity triple (plus kind, so a
// kind drift never resolves) against the request baselines.
func refInBaselines(req *Request, ref contract.SkillArtifactRef) bool {
	for _, baseline := range req.baselines {
		if baseline.LineageID == ref.LineageID && baseline.Version == ref.Version &&
			baseline.ArtifactDigest == ref.ArtifactDigest && baseline.Kind == ref.Kind {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Result minting
// ---------------------------------------------------------------------------

func (s *Service) mintResult(req *Request, plan *Plan, agg *aggregator, status, failureCode string) (*Result, error) {
	outcomes := make([]FixtureOutcome, 0, len(agg.families))
	for _, fam := range agg.families {
		outcome, err := agg.outcomeFor(fam)
		if err != nil {
			return nil, newError(ReasonReplayResultInvalid, "family %s outcome: %v", fam.name, err)
		}
		outcomes = append(outcomes, outcome)
	}
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].FixtureRef.ID != outcomes[j].FixtureRef.ID {
			return outcomes[i].FixtureRef.ID < outcomes[j].FixtureRef.ID
		}
		return outcomes[i].OutputDigest < outcomes[j].OutputDigest
	})
	sort.Slice(agg.records, func(i, j int) bool {
		a, b := agg.records[i], agg.records[j]
		if a.Family != b.Family {
			return a.Family < b.Family
		}
		if a.PacketID != b.PacketID {
			return a.PacketID < b.PacketID
		}
		return a.Side < b.Side
	})
	resultID, err := deriveResultID(req, plan)
	if err != nil {
		return nil, newError(ReasonReplayResultInvalid, "derive result id: %v", err)
	}
	result := &Result{
		replayResultID:    resultID,
		replayRequestRef:  req.Ref(),
		candidateDoc:      req.CandidateRefDoc(),
		status:            status,
		outcomes:          outcomes,
		utility:           agg.utility,
		envelope:          agg.envelope,
		candidateCases:    agg.candCases,
		envelopeCases:     agg.envCases,
		failureReasonCode: failureCode,
		records:           agg.records,
	}
	digest, err := contract.DigestOf(resultPreimage(result.Doc()))
	if err != nil {
		return nil, newError(ReasonReplayResultInvalid, "result digest: %v", err)
	}
	result.resultDigest = digest
	// Self-check: the minted document must satisfy the authority schema
	// (shape, closed fields, conditional failure code and the recomputed
	// digest) before it leaves the canonicalizer.
	if err := s.gates.ValidateInstance(result.Doc(), SchemaReplayResult); err != nil {
		return nil, newError(ReasonReplayResultInvalid, "minted result failed its own authority schema: %v", err)
	}
	return result, nil
}

// inconclusiveResult mints the fail-closed inconclusive variants
// (nondeterminism, arithmetic overflow): status=inconclusive plus the
// specific closed failure code (Contract §13.7.1 R4).
func (s *Service) inconclusiveResult(req *Request, plan *Plan, agg *aggregator, code string) (*Result, error) {
	return s.mintResult(req, plan, agg, StatusInconclusive, code)
}

// resultPreimage extracts the §7.11 x-digest preimage fields.
func resultPreimage(doc map[string]any) map[string]any {
	fields := []string{"schema_version", "replay_result_id", "replay_request_ref", "candidate_ref",
		"status", "fixture_outcomes", "utility_vector"}
	core := make(map[string]any, len(fields))
	for _, field := range fields {
		core[field] = doc[field]
	}
	return core
}

// deriveResultID mints the deterministic result identity from the frozen
// request digest and family plan.
func deriveResultID(req *Request, plan *Plan) (string, error) {
	families := make([]any, 0, len(plan.Families))
	for _, family := range plan.Families {
		families = append(families, family.Name)
	}
	sort.Slice(families, func(i, j int) bool {
		return contract.CanonicalKey(families[i]) < contract.CanonicalKey(families[j])
	})
	digest, err := contract.DigestOf(map[string]any{
		"request_digest": req.RequestDigest(),
		"families":       families,
	})
	if err != nil {
		return "", err
	}
	return "rr-" + digest[len("sha256:"):len("sha256:")+16], nil
}
