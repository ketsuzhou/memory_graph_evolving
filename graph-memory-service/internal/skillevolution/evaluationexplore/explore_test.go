package evaluationexplore

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
)

func TestFixedFixtureSeedNeighborOrderSelectedRefAndAuditTraceAreReproducible(t *testing.T) {
	view := mustView(t, reproducibleRecords())
	req := Request{
		View:  view,
		Scope: evaluationgraph.ScopeEvaluation,
		Opening: OpeningContext{
			SeedRefs:      []string{"skill://evaluation/alpha@1"},
			Query:         "retry-backoff",
			CheckpointID:  "cp-opening-1",
			TargetAgentID: "task-agent-1",
		},
	}

	first, err := Explore(req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Explore(req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same fixture produced a different explore result\nfirst: %#v\nsecond: %#v", first, second)
	}

	wantSeeds := []string{"skill://evaluation/alpha@1"}
	wantNeighbors := []string{
		"skill://evaluation/beta@1",
		"skill://evaluation/gamma@1",
		"skill://evaluation/delta@1",
	}
	if !reflect.DeepEqual(first.Seeds, wantSeeds) {
		t.Fatalf("seeds = %#v, want %#v", first.Seeds, wantSeeds)
	}
	if !reflect.DeepEqual(first.NeighborOrder, wantNeighbors) {
		t.Fatalf("neighbor order = %#v, want %#v", first.NeighborOrder, wantNeighbors)
	}
	if first.SelectedRef != "skill://evaluation/beta@1" || first.Choice != ChoiceServeOriginal {
		t.Fatalf("selected = %q choice = %q", first.SelectedRef, first.Choice)
	}
	if first.Offer == nil {
		t.Fatal("expected a pending original offer")
	}
	wantOffer := &PendingOffer{
		SkillReference: "skill://evaluation/beta@1",
		CheckpointID:   "cp-opening-1",
		TargetAgentID:  "task-agent-1",
		Choice:         ChoiceServeOriginal,
	}
	if !reflect.DeepEqual(first.Offer, wantOffer) {
		t.Fatalf("offer = %#v, want %#v", first.Offer, wantOffer)
	}

	wantAudit := []AuditEvent{
		{Kind: "seed", Ref: "skill://evaluation/alpha@1"},
		{Kind: "expand", Turn: 1, Step: 1, From: "skill://evaluation/alpha@1", To: "skill://evaluation/beta@1", Edge: RelSpecializes},
		{Kind: "expand", Turn: 1, Step: 2, From: "skill://evaluation/alpha@1", To: "skill://evaluation/gamma@1", Edge: RelRelatedTo},
		{Kind: "expand", Turn: 2, Step: 3, From: "skill://evaluation/beta@1", To: "skill://evaluation/alpha@1", Edge: RelGeneralizes},
		{Kind: "expand", Turn: 2, Step: 4, From: "skill://evaluation/beta@1", To: "skill://evaluation/delta@1", Edge: RelRelatedTo},
		{Kind: "expand", Turn: 3, Step: 5, From: "skill://evaluation/gamma@1", To: "skill://evaluation/alpha@1", Edge: RelRelatedTo},
		{Kind: "expand", Turn: 4, Step: 6, From: "skill://evaluation/delta@1", To: "skill://evaluation/beta@1", Edge: RelRelatedTo},
		{Kind: "rank", Detail: "skill://evaluation/beta@1,skill://evaluation/alpha@1,skill://evaluation/gamma@1,skill://evaluation/delta@1"},
		{Kind: "select", Ref: "skill://evaluation/beta@1", Choice: ChoiceServeOriginal},
		{Kind: "offer", Ref: "skill://evaluation/beta@1", Choice: ChoiceServeOriginal, Detail: "checkpoint=cp-opening-1 target=task-agent-1"},
	}
	if !reflect.DeepEqual(first.AuditTrace, wantAudit) {
		t.Fatalf("audit trace = %#v\nwant %#v", first.AuditTrace, wantAudit)
	}
}

func TestDisallowedRoomEvidenceLatestAliasLocalPathAndCrossScopeRefsAreRejected(t *testing.T) {
	view := mustView(t, []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-alpha", "alpha", 1, "retry-backoff seed"),
		proposal(2, "proposal-beta", "beta", 1, "retry-backoff related"),
		consolidate(3, "consolidate-alpha", "alpha", 1),
		consolidate(4, "consolidate-beta", "beta", 1),
		relation(5, "alpha-room", "alpha", 1, "beta", 1, "in_room"),
	})
	opening := OpeningContext{CheckpointID: "cp-1", TargetAgentID: "task-agent-1"}

	t.Run("room expansion", func(t *testing.T) {
		_, err := Explore(Request{
			View:    view,
			Opening: withSeeds(opening, "skill://evaluation/alpha@1"),
		})
		if !errors.Is(err, ErrDisallowedExpansion) {
			t.Fatalf("room expansion error = %v, want disallowed expansion", err)
		}
	})

	evidenceView := mustView(t, []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-alpha", "alpha", 1, "retry-backoff seed"),
		proposal(2, "proposal-beta", "beta", 1, "retry-backoff related"),
		consolidate(3, "consolidate-alpha", "alpha", 1),
		consolidate(4, "consolidate-beta", "beta", 1),
		relation(5, "alpha-evidence", "alpha", 1, "beta", 1, "has_evidence"),
	})
	t.Run("evidence expansion", func(t *testing.T) {
		_, err := Explore(Request{
			View:    evidenceView,
			Opening: withSeeds(opening, "skill://evaluation/alpha@1"),
		})
		if !errors.Is(err, ErrDisallowedExpansion) {
			t.Fatalf("evidence expansion error = %v, want disallowed expansion", err)
		}
	})

	safeView := mustView(t, reproducibleRecords())
	for name, tc := range map[string]struct {
		seed  string
		scope evaluationgraph.Scope
		want  error
	}{
		"latest alias":    {seed: "skill://evaluation/alpha@latest", want: ErrLatestAlias},
		"unpinned alias":  {seed: "skill://evaluation/alpha", want: ErrLatestAlias},
		"local abs path":  {seed: "/var/skills/alpha.md", want: ErrLocalPathRef},
		"local file uri":  {seed: "file:///var/skills/alpha.md", want: ErrLocalPathRef},
		"local rel path":  {seed: "./skills/alpha", want: ErrLocalPathRef},
		"cross-scope ref": {seed: "skill://runtime/alpha@1", want: ErrCrossScopeRef},
		"runtime scope":   {seed: "skill://evaluation/alpha@1", scope: evaluationgraph.ScopeRuntime, want: evaluationgraph.ErrScopeDenied},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Explore(Request{
				View:    safeView,
				Scope:   tc.scope,
				Opening: withSeeds(opening, tc.seed),
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestGraphTurnAndOfferBudgetsProduceDistinctTerminalStates(t *testing.T) {
	t.Run("graph_steps", func(t *testing.T) {
		view := mustView(t, fanoutRecords())
		got, err := Explore(Request{
			View: view,
			Opening: OpeningContext{
				SeedRefs:      []string{"skill://evaluation/hub@1"},
				Query:         "keep-going",
				CheckpointID:  "cp-graph",
				TargetAgentID: "task-agent-1",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Terminal != TerminalGraphBudgetExhausted {
			t.Fatalf("terminal = %q, want %s", got.Terminal, TerminalGraphBudgetExhausted)
		}
		if got.GraphSteps != DefaultMaxGraphStepsPerEpisode {
			t.Fatalf("graph steps = %d, want %d", got.GraphSteps, DefaultMaxGraphStepsPerEpisode)
		}
		if got.Offer != nil {
			t.Fatalf("graph budget must not emit an offer: %#v", got.Offer)
		}
		if containsRef(got.NeighborOrder, "skill://evaluation/nbr-12@1") {
			t.Fatalf("graph budget leaked the 13th neighbor: %#v", got.NeighborOrder)
		}
	})

	t.Run("memory_turns", func(t *testing.T) {
		view := mustView(t, chainRecords())
		got, err := Explore(Request{
			View: view,
			Opening: OpeningContext{
				SeedRefs:      []string{"skill://evaluation/chain-0@1"},
				Query:         "deep-target",
				CheckpointID:  "cp-turns",
				TargetAgentID: "task-agent-1",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Terminal != TerminalMemoryTimeout {
			t.Fatalf("terminal = %q, want %s", got.Terminal, TerminalMemoryTimeout)
		}
		if got.MemoryTurns != DefaultMaxMemoryTurnsPerEpisode {
			t.Fatalf("memory turns = %d, want %d", got.MemoryTurns, DefaultMaxMemoryTurnsPerEpisode)
		}
		if got.GraphSteps >= DefaultMaxGraphStepsPerEpisode {
			t.Fatalf("turn budget must stop before the graph cap: steps=%d", got.GraphSteps)
		}
		if got.Offer != nil || got.SelectedRef != "" {
			t.Fatalf("turn budget must not select: %#v", got)
		}
		if containsRef(got.NeighborOrder, "skill://evaluation/chain-7@1") {
			t.Fatalf("turn budget leaked the 7th hop: %#v", got.NeighborOrder)
		}
	})

	t.Run("offers", func(t *testing.T) {
		view := mustView(t, reproducibleRecords())
		got, err := Explore(Request{
			View: view,
			Opening: OpeningContext{
				SeedRefs:      []string{"skill://evaluation/alpha@1"},
				Query:         "retry-backoff",
				CheckpointID:  "cp-offers",
				TargetAgentID: "task-agent-1",
			},
			OffersAlreadyEmitted: DefaultMaxOffersPerAgentPerCheckpoint,
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Terminal != TerminalOfferBudgetExhausted {
			t.Fatalf("terminal = %q, want %s", got.Terminal, TerminalOfferBudgetExhausted)
		}
		if got.Offer != nil {
			t.Fatalf("offer budget must not emit a 4th offer: %#v", got.Offer)
		}
		if got.SelectedRef != "skill://evaluation/beta@1" {
			t.Fatalf("would-be selection = %q, want beta", got.SelectedRef)
		}
	})
}

func TestConditionalConflictRetiredSupersededAndTrainFeedbackEdgesAffectCandidates(t *testing.T) {
	view := mustView(t, policyRecords())
	got, err := Explore(Request{
		View: view,
		Opening: OpeningContext{
			SeedRefs:      []string{"skill://evaluation/topic@1"},
			Query:         "policy-topic",
			CheckpointID:  "cp-policy",
			TargetAgentID: "task-agent-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.SelectedRef != "skill://evaluation/guarded@1" || got.Choice != ChoiceServeOriginal {
		t.Fatalf("policy selected = %q choice = %q", got.SelectedRef, got.Choice)
	}
	if got.Offer == nil || got.Offer.SkillReference != "skill://evaluation/guarded@1" {
		t.Fatalf("policy offer = %#v", got.Offer)
	}
	if offerableInResult(view, "outdated") {
		t.Fatal("retired/superseded skill remained offerable")
	}
	if got.SelectedRef == "skill://evaluation/outdated@1" || got.SelectedRef == "skill://evaluation/rival@1" {
		t.Fatalf("policy selected a retired or conflicting candidate: %q", got.SelectedRef)
	}
	if indexOf(got.Ranking, "skill://evaluation/guarded@1") != 0 {
		t.Fatalf("accepted specialist must rank first: %#v", got.Ranking)
	}
	replacement := indexOf(got.Ranking, "skill://evaluation/replacement@1")
	rival := indexOf(got.Ranking, "skill://evaluation/rival@1")
	weakly := indexOf(got.Ranking, "skill://evaluation/weakly@1")
	if replacement < 0 || rival < 0 || weakly < 0 {
		t.Fatalf("expected successor, weak co-use, and rival in ranking: %#v", got.Ranking)
	}
	if replacement > rival {
		t.Fatalf("successor must outrank conditional-conflict+rejected rival: %#v", got.Ranking)
	}
	if weakly > rival {
		t.Fatalf("weak co-use must still outrank rejected conflict: %#v", got.Ranking)
	}
}

func TestHeldOutFeedbackChangesDoNotAffectSeedRankingOrExploreResult(t *testing.T) {
	view := mustView(t, reproducibleRecords())
	base := Request{
		View: view,
		Opening: OpeningContext{
			SeedRefs:      []string{"skill://evaluation/alpha@1"},
			Query:         "retry-backoff",
			CheckpointID:  "cp-heldout",
			TargetAgentID: "task-agent-1",
		},
	}
	without, err := Explore(base)
	if err != nil {
		t.Fatal(err)
	}
	withAccept := base
	withAccept.HeldOut = []HeldOutFeedback{{
		OfferID:     "held-out-1",
		Ref:         "skill://evaluation/gamma@1",
		Disposition: "accepted",
		Reason:      "would have polluted ranking",
	}}
	withReject := base
	withReject.HeldOut = []HeldOutFeedback{{
		OfferID:     "held-out-2",
		Ref:         "skill://evaluation/beta@1",
		Disposition: "rejected",
		Reason:      "must not hide the selected original",
	}}
	gotAccept, err := Explore(withAccept)
	if err != nil {
		t.Fatal(err)
	}
	gotReject, err := Explore(withReject)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(without.Seeds, gotAccept.Seeds) || !reflect.DeepEqual(without.Seeds, gotReject.Seeds) {
		t.Fatalf("held-out feedback changed seeds: %#v %#v %#v", without.Seeds, gotAccept.Seeds, gotReject.Seeds)
	}
	if !reflect.DeepEqual(without.Ranking, gotAccept.Ranking) || !reflect.DeepEqual(without.Ranking, gotReject.Ranking) {
		t.Fatalf("held-out feedback changed ranking: %#v %#v %#v", without.Ranking, gotAccept.Ranking, gotReject.Ranking)
	}
	if !reflect.DeepEqual(without, gotAccept) || !reflect.DeepEqual(without, gotReject) {
		t.Fatalf("held-out feedback changed explore result\nbase: %#v\naccept: %#v\nreject: %#v", without, gotAccept, gotReject)
	}
}

func reproducibleRecords() []evaluationgraph.CanonicalRecord {
	return []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-alpha", "alpha", 1, "retry-backoff seed"),
		proposal(2, "proposal-beta", "beta", 1, "retry-backoff specialized"),
		proposal(3, "proposal-gamma", "gamma", 1, "retry-backoff related"),
		proposal(4, "proposal-delta", "delta", 1, "retry-backoff derived"),
		consolidate(5, "consolidate-alpha", "alpha", 1),
		consolidate(6, "consolidate-beta", "beta", 1),
		consolidate(7, "consolidate-gamma", "gamma", 1),
		consolidate(8, "consolidate-delta", "delta", 1),
		relation(9, "alpha-beta", "alpha", 1, "beta", 1, RelSpecializes),
		relation(10, "alpha-gamma", "alpha", 1, "gamma", 1, RelRelatedTo),
		relation(11, "beta-delta", "beta", 1, "delta", 1, RelRelatedTo),
	}
}

func fanoutRecords() []evaluationgraph.CanonicalRecord {
	records := make([]evaluationgraph.CanonicalRecord, 0, 42)
	seq := uint64(1)
	records = append(records, proposal(seq, "proposal-hub", "hub", 1, "fanout seed"))
	seq++
	for i := 0; i < 13; i++ {
		body := fmt.Sprintf("neighbor-%02d", i)
		if i == 12 {
			body = "keep-going applicable neighbor"
		}
		records = append(records, proposal(seq, fmt.Sprintf("proposal-nbr-%02d", i), fmt.Sprintf("nbr-%02d", i), 1, body))
		seq++
	}
	records = append(records, consolidate(seq, "consolidate-hub", "hub", 1))
	seq++
	for i := 0; i < 13; i++ {
		records = append(records, consolidate(seq, fmt.Sprintf("consolidate-nbr-%02d", i), fmt.Sprintf("nbr-%02d", i), 1))
		seq++
	}
	for i := 0; i < 13; i++ {
		records = append(records, relation(seq, fmt.Sprintf("hub-nbr-%02d", i), "hub", 1, fmt.Sprintf("nbr-%02d", i), 1, RelRelatedTo))
		seq++
	}
	return records
}

func chainRecords() []evaluationgraph.CanonicalRecord {
	records := make([]evaluationgraph.CanonicalRecord, 0, 24)
	seq := uint64(1)
	for i := 0; i < 8; i++ {
		body := fmt.Sprintf("chain-node-%d", i)
		if i == 7 {
			body = "deep-target applicable"
		}
		records = append(records, proposal(seq, fmt.Sprintf("proposal-chain-%d", i), fmt.Sprintf("chain-%d", i), 1, body))
		seq++
	}
	for i := 0; i < 8; i++ {
		records = append(records, consolidate(seq, fmt.Sprintf("consolidate-chain-%d", i), fmt.Sprintf("chain-%d", i), 1))
		seq++
	}
	for i := 0; i < 7; i++ {
		records = append(records, relation(seq, fmt.Sprintf("chain-%d-%d", i, i+1), fmt.Sprintf("chain-%d", i), 1, fmt.Sprintf("chain-%d", i+1), 1, RelRelatedTo))
		seq++
	}
	return records
}

func policyRecords() []evaluationgraph.CanonicalRecord {
	return []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-topic", "topic", 1, "policy-topic seed"),
		proposal(2, "proposal-guarded", "guarded", 1, "policy-topic specialist"),
		proposal(3, "proposal-outdated", "outdated", 1, "policy-topic retired"),
		proposal(4, "proposal-replacement", "replacement", 1, "policy-topic successor"),
		proposal(5, "proposal-rival", "rival", 1, "policy-topic conflict"),
		proposal(6, "proposal-weakly", "weakly", 1, "policy-topic weak"),
		proposal(7, "proposal-train", "train-case", 1, "policy-topic train-anchor"),
		consolidate(8, "consolidate-topic", "topic", 1),
		consolidate(9, "consolidate-guarded", "guarded", 1),
		consolidate(10, "consolidate-outdated", "outdated", 1),
		consolidate(11, "consolidate-replacement", "replacement", 1),
		consolidate(12, "consolidate-rival", "rival", 1),
		consolidate(13, "consolidate-weakly", "weakly", 1),
		consolidate(14, "consolidate-train", "train-case", 1),
		relation(15, "topic-guarded", "topic", 1, "guarded", 1, RelSpecializes),
		relation(16, "topic-outdated", "topic", 1, "outdated", 1, RelRelatedTo),
		relation(17, "topic-rival", "topic", 1, "rival", 1, RelConditionallyConflictsWith),
		relation(18, "topic-weakly", "topic", 1, "weakly", 1, RelCoUsedWith),
		relation(19, "outdated-retired", "outdated", 1, "replacement", 1, RelRetiredBy),
		relation(20, "replacement-supersedes", "replacement", 1, "outdated", 1, RelSupersedes),
		relation(21, "guarded-accepted", "guarded", 1, "train-case", 1, RelAcceptedIn),
		relation(22, "rival-rejected", "rival", 1, "train-case", 1, RelRejectedIn),
	}
}

func proposal(seq uint64, id, lineage string, revision uint64, body string) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq,
		ID:       id,
		Kind:     evaluationgraph.RecordProposal,
		Proposal: &evaluationgraph.Proposal{
			Ref:      evaluationgraph.RevisionRef{LineageID: lineage, Revision: revision},
			Body:     body,
			Advisory: true,
		},
	}
}

func consolidate(seq uint64, id, lineage string, revision uint64) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq,
		ID:       id,
		Kind:     evaluationgraph.RecordConsolidation,
		Consolidation: &evaluationgraph.Consolidation{
			Target:      evaluationgraph.RevisionRef{LineageID: lineage, Revision: revision},
			Disposition: "retain",
		},
	}
}

func relation(seq uint64, id, fromLineage string, fromRev uint64, toLineage string, toRev uint64, kind string) evaluationgraph.CanonicalRecord {
	return evaluationgraph.CanonicalRecord{
		Sequence: seq,
		ID:       id,
		Kind:     evaluationgraph.RecordRelation,
		Relation: &evaluationgraph.Relation{
			From: evaluationgraph.RevisionRef{LineageID: fromLineage, Revision: fromRev},
			To:   evaluationgraph.RevisionRef{LineageID: toLineage, Revision: toRev},
			Kind: kind,
		},
	}
}

func mustView(t *testing.T, records []evaluationgraph.CanonicalRecord) *evaluationgraph.View {
	t.Helper()
	fixture, err := evaluationgraph.NewFixture(records)
	if err != nil {
		t.Fatal(err)
	}
	view, err := evaluationgraph.Build(fixture)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func withSeeds(opening OpeningContext, seeds ...string) OpeningContext {
	opening.SeedRefs = seeds
	return opening
}

func containsRef(refs []string, want string) bool {
	return indexOf(refs, want) >= 0
}

func indexOf(refs []string, want string) int {
	for i, ref := range refs {
		if ref == want {
			return i
		}
	}
	return -1
}

func offerableInResult(view *evaluationgraph.View, lineage string) bool {
	ref := evaluationgraph.RevisionRef{LineageID: lineage, Revision: 1}
	for _, node := range view.Nodes {
		if node.Ref == ref {
			return offerable(view, ref, node.Body, "policy-topic")
		}
	}
	return false
}
