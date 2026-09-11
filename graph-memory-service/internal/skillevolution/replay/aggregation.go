package replay

import (
	"sort"

	"river2.dev/graph-memory-service/internal/contract"
)

// ---------------------------------------------------------------------------
// Aggregation (checked arithmetic; the Refactor seam of GMS-203)
// ---------------------------------------------------------------------------

// aggregator accumulates per-family outcomes and both utility vectors with
// checked int64 arithmetic only.
type aggregator struct {
	families   []familyAggregation
	utility    UtilityVector
	envelope   UtilityVector
	candCases  int64
	envCases   int64
	records    []RunRecord
	overflowed bool
}

type familyAggregation struct {
	name     string
	domain   string
	baseline map[string]any
	packets  []aggregatedPacket
}

type aggregatedPacket struct {
	packet  Packet
	record  RunRecord
	digests []string
}

func (f *familyAggregation) addPacket(packet Packet, record RunRecord, group []RunOutput) {
	digests := make([]string, 0, len(group))
	for _, run := range group {
		digests = append(digests, run.OutputDigest)
	}
	sort.Strings(digests)
	f.packets = append(f.packets, aggregatedPacket{packet: packet, record: record, digests: digests})
}

func newAggregator() *aggregator {
	return &aggregator{records: []RunRecord{}}
}

func (a *aggregator) overflow() bool { return a.overflowed }

// checked add: on overflow the aggregator flips to overflowed and keeps 0.
func (a *aggregator) addChecked(dst *int64, delta int64) {
	if a.overflowed {
		return
	}
	sum, err := checkedAdd(*dst, delta)
	if err != nil {
		a.overflowed = true
		return
	}
	*dst = sum
}

// add folds one packet's classified runs into both vectors.
func (a *aggregator) add(packet Packet, record RunRecord, group []RunOutput) {
	a.records = append(a.records, record)
	if record.Side == SideCandidate {
		a.addChecked(&a.candCases, int64(len(group)))
	} else {
		a.addChecked(&a.envCases, int64(len(group)))
	}
	if record.Category == CategoryNondeterministic {
		// The case is unresolved: never a pass, an inconclusive case on
		// the candidate side.
		if record.Side == SideCandidate {
			a.addChecked(&a.utility.InconclusiveCaseCount, int64(len(group)))
		} else {
			a.addChecked(&a.envelope.InconclusiveCaseCount, int64(len(group)))
		}
	}
	for _, run := range group {
		if record.Side == SideCandidate {
			a.addChecked(&a.utility.ExecutionCostUnits, run.CostUnits)
		} else {
			a.addChecked(&a.envelope.ExecutionCostUnits, run.CostUnits)
		}
	}
	if record.Passed() {
		if record.Side == SideCandidate {
			a.addChecked(&a.utility.TaskSuccessCount, 1)
			if packet.Critical {
				a.addChecked(&a.utility.CriticalBranchPassCount, 1)
			}
			if record.Recovery {
				a.addChecked(&a.utility.RecoverySuccessCount, 1)
			}
		} else {
			a.addChecked(&a.envelope.TaskSuccessCount, 1)
			if packet.Critical {
				a.addChecked(&a.envelope.CriticalBranchPassCount, 1)
			}
			if record.Recovery {
				a.addChecked(&a.envelope.RecoverySuccessCount, 1)
			}
		}
	}
	if record.Side == SideCandidate && record.Category == CategoryInfra {
		a.addChecked(&a.utility.InconclusiveCaseCount, int64(len(group)))
	}
	if record.Side == SideBaseline && record.Category == CategoryInfra {
		a.addChecked(&a.envelope.InconclusiveCaseCount, int64(len(group)))
	}
}

// addFamily freezes one family aggregation.
func (a *aggregator) addFamily(fam familyAggregation) {
	a.families = append(a.families, fam)
}

// outcomeFor builds the §7.11 fixture outcome of one family.
func (a *aggregator) outcomeFor(fam familyAggregation) (FixtureOutcome, error) {
	var baselinePassed, candidatePassed, total, regressions int64
	byPacket := map[string]map[string]*aggregatedPacket{}
	for i := range fam.packets {
		ap := &fam.packets[i]
		if byPacket[ap.packet.ID] == nil {
			byPacket[ap.packet.ID] = map[string]*aggregatedPacket{}
		}
		byPacket[ap.packet.ID][ap.packet.Side] = ap
	}
	for _, ap := range fam.packets {
		// total_cases counts observed runs, not planned packets.
		total += int64(len(ap.digests))
		if ap.record.Passed() {
			switch ap.packet.Side {
			case SideBaseline:
				baselinePassed++
			case SideCandidate:
				candidatePassed++
			}
		}
	}
	// Critical regressions: critical packets where the baseline side
	// semantically passed and the candidate side did not.
	for _, sides := range byPacket {
		baseline, hasBaseline := sides[SideBaseline]
		candidate, hasCandidate := sides[SideCandidate]
		if hasBaseline && baseline.packet.Critical &&
			baseline.record.Passed() && (!hasCandidate || !candidate.record.Passed()) {
			regressions++
		}
	}
	outputDigest, err := familyOutputDigest(fam)
	if err != nil {
		return FixtureOutcome{}, err
	}
	planDigest, err := familyPlanDigest(fam)
	if err != nil {
		return FixtureOutcome{}, err
	}
	return FixtureOutcome{
		FixtureRef:              contract.VersionedRef{ID: fam.name, Version: "1", Digest: planDigest},
		Domain:                  fam.domain,
		BaselineRef:             fam.baseline,
		BaselinePassed:          baselinePassed,
		CandidatePassed:         candidatePassed,
		TotalCases:              total,
		CriticalRegressionCount: regressions,
		OutputDigest:            outputDigest,
	}, nil
}

// familyOutputDigest digests the deterministic per-side observed digests.
func familyOutputDigest(fam familyAggregation) (string, error) {
	pairs := make([]any, 0, len(fam.packets))
	for _, ap := range fam.packets {
		digests := make([]any, 0, len(ap.digests))
		for _, d := range ap.digests {
			digests = append(digests, d)
		}
		pairs = append(pairs, map[string]any{
			"packet_id": ap.packet.ID,
			"side":      ap.packet.Side,
			"digests":   digests,
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return contract.CanonicalKey(pairs[i]) < contract.CanonicalKey(pairs[j])
	})
	return contract.DigestOf(pairs)
}

// familyPlanDigest freezes the family plan identity (packet ids, sides and
// expected digests).
func familyPlanDigest(fam familyAggregation) (string, error) {
	pairs := make([]any, 0, len(fam.packets))
	for _, ap := range fam.packets {
		pairs = append(pairs, map[string]any{
			"packet_id":              ap.packet.ID,
			"side":                   ap.packet.Side,
			"expected_output_digest": ap.packet.ExpectedOutputDigest,
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return contract.CanonicalKey(pairs[i]) < contract.CanonicalKey(pairs[j])
	})
	return contract.DigestOf(pairs)
}

// checkedAdd adds two int64s fail-closed on overflow.
func checkedAdd(a, b int64) (int64, error) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, errOverflow
	}
	return c, nil
}

type overflowError struct{}

func (overflowError) Error() string { return "replay: checked arithmetic overflow" }

var errOverflow = overflowError{}
