package rejectredirect

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/skillevolution/evaluationexplore"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
)

func TestCoreReasonsHaveTableDrivenRedirectOrStop(t *testing.T) {
	type want struct {
		action      string
		stop        bool
		nextOffer   string
		terminal    string
		guard       string
		lineage     string
		blacklist   string
		sourceSet   string
		nextQueryIn string
	}
	cases := []struct {
		code     string
		view     []evaluationgraph.CanonicalRecord
		seed     string
		query    string
		reject   string
		reason   string
		want     want
		wantNext bool
	}{
		{
			code:   ReasonNotApplicable,
			view:   specializeRecords("generic-advice", "specialized-timeout"),
			seed:   "skill://evaluation/generic@1",
			query:  "generic-advice",
			reject: "skill://evaluation/generic@1",
			reason: "does not apply to this checkpoint",
			want: want{
				action:      ActionExploreSpecialization,
				nextOffer:   "skill://evaluation/specialized@1",
				guard:       GuardRequireSpecialization,
				nextQueryIn: "specializes:",
			},
			wantNext: true,
		},
		{
			code:   ReasonAlreadyKnown,
			view:   relatedRecords("known", "other", "insight-known", "insight-other"),
			seed:   "skill://evaluation/known@1",
			query:  "insight-known",
			reject: "skill://evaluation/known@1",
			reason: "this insight is already known",
			want: want{
				action:    ActionExcludeInsightLineage,
				nextOffer: "skill://evaluation/other@1",
				lineage:   "known",
			},
			wantNext: true,
		},
		{
			code:   ReasonAlreadyResolved,
			view:   singleRecords("resolved", "already-handled"),
			seed:   "skill://evaluation/resolved@1",
			query:  "already-handled",
			reject: "skill://evaluation/resolved@1",
			reason: "the need is already resolved",
			want: want{
				action:   ActionStopCurrentNeed,
				stop:     true,
				terminal: TerminalNeedStopped,
			},
		},
		{
			code:   ReasonTooGeneric,
			view:   specializeRecords("generic-advice", "specialized-timeout"),
			seed:   "skill://evaluation/generic@1",
			query:  "generic-advice",
			reject: "skill://evaluation/generic@1",
			reason: "too generic for this failure",
			want: want{
				action:      ActionExploreGuardedDescendant,
				nextOffer:   "skill://evaluation/specialized@1",
				guard:       GuardRequireGuardedDescendant,
				nextQueryIn: "specialized_descendant:",
			},
			wantNext: true,
		},
		{
			code:   ReasonInsufficientContext,
			view:   singleRecords("thin", "needs-more-context"),
			seed:   "skill://evaluation/thin@1",
			query:  "needs-more-context",
			reject: "skill://evaluation/thin@1",
			reason: "opening context is not enough",
			want: want{
				action:   ActionStopCurrentNeed,
				stop:     true,
				terminal: TerminalNeedStopped,
			},
		},
		{
			code:   ReasonIncorrectAssumption,
			view:   relatedRecords("assumed", "alternate", "assumption-branch", "other-assumption"),
			seed:   "skill://evaluation/assumed@1",
			query:  "assumption-branch",
			reject: "skill://evaluation/assumed@1",
			reason: "assumes a retryable network error",
			want: want{
				action:    ActionExcludeAssumptionBranch,
				nextOffer: "skill://evaluation/alternate@1",
				lineage:   "assumed",
			},
			wantNext: true,
		},
		{
			code:   ReasonConflictsWithEvidence,
			view:   conflictRecords(),
			seed:   "skill://evaluation/subject@1",
			query:  "conflict-subject",
			reject: "skill://evaluation/subject@1",
			reason: "conflicts with the observed compile error",
			want: want{
				action:    ActionFindConflictAlternative,
				nextOffer: "skill://evaluation/alternative@1",
			},
			wantNext: true,
		},
		{
			code:   ReasonDuplicateOffer,
			view:   sourceSetRecords(),
			seed:   "skill://evaluation/twin-a@1",
			query:  "source-twin-a",
			reject: "skill://evaluation/twin-a@1",
			reason: "duplicate of a prior offer",
			want: want{
				action:    ActionExcludeLineageSourceSet,
				nextOffer: "skill://evaluation/unique-c@1",
				lineage:   "twin-a",
				sourceSet: "skill://evaluation/shared-anchor@1",
			},
			wantNext: true,
		},
		{
			code:   ReasonUnsafeOrHarmful,
			view:   relatedRecords("harmful", "safe", "unsafe-body", "safe-body"),
			seed:   "skill://evaluation/harmful@1",
			query:  "unsafe-body",
			reject: "skill://evaluation/harmful@1",
			reason: "would leak credentials",
			want: want{
				action:    ActionBlacklistExactRevision,
				nextOffer: "skill://evaluation/safe@1",
				blacklist: "skill://evaluation/harmful@1",
			},
			wantNext: true,
		},
	}

	if len(cases) != len(CoreReasonCodes) {
		t.Fatalf("table has %d cases, want %d core reasons", len(cases), len(CoreReasonCodes))
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		seen[tc.code] = true
		t.Run(tc.code, func(t *testing.T) {
			mem := NewMemory("need-" + tc.code)
			view := mustView(t, tc.view)
			opening := openingFor(tc.seed, tc.query)
			first, err := Offer(mem, view, opening, evaluationexplore.Budget{})
			if err != nil {
				t.Fatal(err)
			}
			if first.Offer == nil || first.Offer.SkillReference != tc.reject {
				t.Fatalf("first offer = %#v, want %s", first.Offer, tc.reject)
			}

			got, err := ApplyRejection(mem, RejectedDisposition{
				OfferID:    "offer-" + tc.code,
				AgentID:    "task-agent-1",
				Ref:        tc.reject,
				ReasonCode: tc.code,
				Reason:     tc.reason,
			}, view, opening, evaluationexplore.Budget{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Delivery == nil || got.Delivery.Kind != KindSkillRejected || got.Delivery.ReasonCode != tc.code || got.Delivery.Reason != tc.reason {
				t.Fatalf("delivery = %#v", got.Delivery)
			}
			if got.Action != tc.want.action {
				t.Fatalf("action = %q, want %q", got.Action, tc.want.action)
			}
			if mem.Policy.StopCurrentNeed != tc.want.stop {
				t.Fatalf("stop = %v, want %v", mem.Policy.StopCurrentNeed, tc.want.stop)
			}
			if got.Terminal != tc.want.terminal {
				t.Fatalf("terminal = %q, want %q", got.Terminal, tc.want.terminal)
			}
			if tc.want.guard != "" && !hasGuard(mem.Policy.Guards, Guard{Ref: tc.reject, ReasonCode: tc.code, Constraint: tc.want.guard}) {
				t.Fatalf("missing guard %q: %#v", tc.want.guard, mem.Policy.Guards)
			}
			if tc.want.lineage != "" && !contains(mem.Policy.ExcludedLineages, tc.want.lineage) {
				t.Fatalf("excluded lineages = %#v, want %q", mem.Policy.ExcludedLineages, tc.want.lineage)
			}
			if tc.want.blacklist != "" && !contains(mem.Policy.BlacklistedRevisions, tc.want.blacklist) {
				t.Fatalf("blacklist = %#v, want %q", mem.Policy.BlacklistedRevisions, tc.want.blacklist)
			}
			if tc.want.sourceSet != "" && !contains(mem.Policy.ExcludedSourceSets, tc.want.sourceSet) {
				t.Fatalf("excluded source sets = %#v, want %q", mem.Policy.ExcludedSourceSets, tc.want.sourceSet)
			}
			if tc.want.nextQueryIn != "" && !containsString(got.NextQuery, tc.want.nextQueryIn) {
				t.Fatalf("next query = %q, want substring %q", got.NextQuery, tc.want.nextQueryIn)
			}
			if len(mem.AuditTrail) != 1 || mem.AuditTrail[0].OfferID != "offer-"+tc.code || mem.AuditTrail[0].ReasonCode != tc.code || mem.AuditTrail[0].NextQuery != got.NextQuery {
				t.Fatalf("audit hop = %#v", mem.AuditTrail)
			}
			if mem.ReasonCounts[tc.code] != 1 {
				t.Fatalf("reason counts = %#v", mem.ReasonCounts)
			}
			if tc.wantNext {
				if got.Offer == nil || got.Offer.SkillReference != tc.want.nextOffer {
					t.Fatalf("next offer = %#v, want %s", got.Offer, tc.want.nextOffer)
				}
				if got.Offer.SkillReference == tc.reject {
					t.Fatal("redirect re-offered the rejected revision")
				}
				return
			}
			if got.Offer != nil {
				t.Fatalf("stop/terminal must not emit an offer: %#v", got.Offer)
			}
		})
	}
	for _, code := range CoreReasonCodes {
		if !seen[code] {
			t.Fatalf("table-driven cases omitted core reason %s", code)
		}
	}
}

func TestGenericSkillRejectedExploresSpecializedDescendantAndOffersAgain(t *testing.T) {
	view := mustView(t, []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-generic", "generic", 1, "generic retry advice"),
		proposal(2, "proposal-specialized", "postgres-timeout", 1, "postgres timeout retry with jitter"),
		proposal(3, "proposal-unrelated", "unrelated", 1, "unrelated cache warming"),
		consolidate(4, "consolidate-generic", "generic", 1),
		consolidate(5, "consolidate-specialized", "postgres-timeout", 1),
		consolidate(6, "consolidate-unrelated", "unrelated", 1),
		relation(7, "generic-specialized", "generic", 1, "postgres-timeout", 1, evaluationexplore.RelSpecializes),
		relation(8, "generic-unrelated", "generic", 1, "unrelated", 1, evaluationexplore.RelRelatedTo),
	})
	mem := NewMemory("need-generic")
	opening := openingFor("skill://evaluation/generic@1", "generic retry")

	first, err := Offer(mem, view, opening, evaluationexplore.Budget{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Offer == nil || first.Offer.SkillReference != "skill://evaluation/generic@1" {
		t.Fatalf("first offer = %#v, want generic", first.Offer)
	}

	got, err := ApplyRejection(mem, RejectedDisposition{
		OfferID:    "offer-generic-1",
		AgentID:    "task-agent-1",
		Ref:        "skill://evaluation/generic@1",
		ReasonCode: ReasonTooGeneric,
		Reason:     "need a specialized timeout recipe",
	}, view, opening, evaluationexplore.Budget{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Action != ActionExploreGuardedDescendant {
		t.Fatalf("action = %q", got.Action)
	}
	if got.Offer == nil || got.Offer.SkillReference != "skill://evaluation/postgres-timeout@1" {
		t.Fatalf("specialized re-offer = %#v", got.Offer)
	}
	if got.Offer.Choice != evaluationexplore.ChoiceServeOriginal {
		t.Fatalf("choice = %q, want serve_original", got.Offer.Choice)
	}
	if !contains(got.NextSeeds, "skill://evaluation/postgres-timeout@1") {
		t.Fatalf("next seeds = %#v, want specialized descendant", got.NextSeeds)
	}
	if len(mem.AuditTrail) != 1 {
		t.Fatalf("audit trail = %#v", mem.AuditTrail)
	}
	hop := mem.AuditTrail[0]
	if hop.OfferID != "offer-generic-1" || hop.Ref != "skill://evaluation/generic@1" || hop.ReasonCode != ReasonTooGeneric || hop.NextQuery == "" {
		t.Fatalf("offer → reason → next query hop = %#v", hop)
	}
	if !reflect.DeepEqual(hop.NextSeeds, got.NextSeeds) {
		t.Fatalf("audit next seeds = %#v, want %#v", hop.NextSeeds, got.NextSeeds)
	}
	if got.Explore.SelectedRef != "skill://evaluation/postgres-timeout@1" {
		t.Fatalf("explore selected = %q", got.Explore.SelectedRef)
	}
}

func TestSameRevisionExcludedLineageAndSourceSetAreNotReoffered(t *testing.T) {
	t.Run("same revision", func(t *testing.T) {
		view := mustView(t, singleRecords("lonely", "only-candidate"))
		mem := NewMemory("need-same-rev")
		opening := openingFor("skill://evaluation/lonely@1", "only-candidate")
		first, err := Offer(mem, view, opening, evaluationexplore.Budget{})
		if err != nil {
			t.Fatal(err)
		}
		if first.Offer == nil {
			t.Fatal("expected an initial offer")
		}
		got, err := ApplyRejection(mem, RejectedDisposition{
			OfferID:    "offer-lonely",
			AgentID:    "task-agent-1",
			Ref:        "skill://evaluation/lonely@1",
			ReasonCode: ReasonNotApplicable,
			Reason:     "not applicable and no descendant exists",
		}, view, opening, evaluationexplore.Budget{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Offer != nil {
			t.Fatalf("same revision was re-offered: %#v", got.Offer)
		}
		if got.Terminal != TerminalAllCandidatesRejected {
			t.Fatalf("terminal = %q, want %s", got.Terminal, TerminalAllCandidatesRejected)
		}
		if !reflect.DeepEqual(mem.OfferedRefs, []string{"skill://evaluation/lonely@1"}) {
			t.Fatalf("offered refs = %#v", mem.OfferedRefs)
		}
	})

	t.Run("excluded lineage", func(t *testing.T) {
		view := mustView(t, []evaluationgraph.CanonicalRecord{
			proposal(1, "proposal-alpha-1", "alpha", 1, "known-insight v1"),
			proposal(2, "proposal-alpha-2", "alpha", 2, "known-insight v2"),
			proposal(3, "proposal-beta", "beta", 1, "other-insight"),
			consolidate(4, "consolidate-alpha-1", "alpha", 1),
			consolidate(5, "consolidate-alpha-2", "alpha", 2),
			consolidate(6, "consolidate-beta", "beta", 1),
			relation(7, "alpha1-alpha2", "alpha", 1, "alpha", 2, evaluationexplore.RelRelatedTo),
			relation(8, "alpha1-beta", "alpha", 1, "beta", 1, evaluationexplore.RelRelatedTo),
		})
		mem := NewMemory("need-lineage")
		opening := openingFor("skill://evaluation/alpha@1", "known-insight v1")
		if _, err := Offer(mem, view, opening, evaluationexplore.Budget{}); err != nil {
			t.Fatal(err)
		}
		got, err := ApplyRejection(mem, RejectedDisposition{
			OfferID:    "offer-alpha-1",
			AgentID:    "task-agent-1",
			Ref:        "skill://evaluation/alpha@1",
			ReasonCode: ReasonAlreadyKnown,
			Reason:     "same insight lineage",
		}, view, opening, evaluationexplore.Budget{})
		if err != nil {
			t.Fatal(err)
		}
		if !contains(mem.Policy.ExcludedLineages, "alpha") {
			t.Fatalf("excluded lineages = %#v", mem.Policy.ExcludedLineages)
		}
		if got.Offer == nil || got.Offer.SkillReference != "skill://evaluation/beta@1" {
			t.Fatalf("next offer = %#v, want beta", got.Offer)
		}
		if got.Offer.SkillReference == "skill://evaluation/alpha@2" {
			t.Fatal("excluded lineage revision was offered")
		}
		if contains(got.NextSeeds, "skill://evaluation/alpha@2") {
			t.Fatalf("next seeds leaked excluded lineage: %#v", got.NextSeeds)
		}
	})

	t.Run("same source set", func(t *testing.T) {
		view := mustView(t, sourceSetRecords())
		mem := NewMemory("need-source")
		opening := openingFor("skill://evaluation/twin-a@1", "source-twin-a")
		if _, err := Offer(mem, view, opening, evaluationexplore.Budget{}); err != nil {
			t.Fatal(err)
		}
		got, err := ApplyRejection(mem, RejectedDisposition{
			OfferID:    "offer-twin-a",
			AgentID:    "task-agent-1",
			Ref:        "skill://evaluation/twin-a@1",
			ReasonCode: ReasonDuplicateOffer,
			Reason:     "same source set as a prior offer",
		}, view, opening, evaluationexplore.Budget{})
		if err != nil {
			t.Fatal(err)
		}
		if !contains(mem.Policy.ExcludedSourceSets, "skill://evaluation/shared-anchor@1") {
			t.Fatalf("excluded source sets = %#v", mem.Policy.ExcludedSourceSets)
		}
		if got.Offer == nil || got.Offer.SkillReference != "skill://evaluation/unique-c@1" {
			t.Fatalf("next offer = %#v, want unique-c", got.Offer)
		}
		if got.Offer.SkillReference == "skill://evaluation/twin-b@1" {
			t.Fatal("same source set was re-offered")
		}
	})
}

func TestInsufficientContextAndAlreadyResolvedStopCurrentNeedInOpeningOnly(t *testing.T) {
	for _, code := range []string{ReasonInsufficientContext, ReasonAlreadyResolved} {
		t.Run(code, func(t *testing.T) {
			view := mustView(t, specializeRecords("opening-only-body", "would-be-specialized"))
			mem := NewMemory("need-stop-" + code)
			if mem.TaskContextMonitoring != TaskContextOpeningOnly {
				t.Fatalf("default monitoring = %q, want opening_only", mem.TaskContextMonitoring)
			}
			opening := openingFor("skill://evaluation/generic@1", "opening-only-body")
			if _, err := Offer(mem, view, opening, evaluationexplore.Budget{}); err != nil {
				t.Fatal(err)
			}
			got, err := ApplyRejection(mem, RejectedDisposition{
				OfferID:    "offer-stop-" + code,
				AgentID:    "task-agent-1",
				Ref:        "skill://evaluation/generic@1",
				ReasonCode: code,
				Reason:     "stop the current need in opening-only mode",
			}, view, opening, evaluationexplore.Budget{})
			if err != nil {
				t.Fatal(err)
			}
			if !mem.Policy.StopCurrentNeed || mem.Policy.StopReason != code {
				t.Fatalf("policy stop = %#v", mem.Policy)
			}
			if got.Terminal != TerminalNeedStopped || got.Action != ActionStopCurrentNeed {
				t.Fatalf("result = %#v", got)
			}
			if got.Offer != nil {
				t.Fatalf("opening-only stop must not explore a descendant: %#v", got.Offer)
			}
			if got.NextQuery != "" || len(got.NextSeeds) != 0 {
				t.Fatalf("stopped need leaked a next query: query=%q seeds=%#v", got.NextQuery, got.NextSeeds)
			}
			if hop := mem.AuditTrail[0]; hop.NextQuery != "" || hop.ReasonCode != code {
				t.Fatalf("audit hop = %#v", hop)
			}
		})
	}
}

func TestResolutionErrorIsOmittedFromReasonCountsAndExhaustedRejectionsTerminateAllCandidatesRejected(t *testing.T) {
	view := mustView(t, specializeRecords("generic-advice", "specialized-timeout"))
	mem := NewMemory("need-resolution")
	opening := openingFor("skill://evaluation/generic@1", "generic-advice")

	if err := NotifyResolutionError(mem, ResolutionNotice{
		OfferID:        "offer-resolution",
		AgentID:        "task-agent-1",
		SkillReference: "skill://evaluation/generic@1",
		ReasonCode:     ReasonResolutionError,
		Failure:        "digest_mismatch",
		Detail:         "served bytes were never injected",
	}); err != nil {
		t.Fatal(err)
	}
	if len(mem.Deliveries) != 0 || len(mem.AuditTrail) != 0 {
		t.Fatalf("resolution error minted a rejection: deliveries=%#v audit=%#v", mem.Deliveries, mem.AuditTrail)
	}
	if mem.ReasonCounts[ReasonResolutionError] != 0 || mem.ReasonCounts[ReasonTooGeneric] != 0 {
		t.Fatalf("resolution error entered reason counts: %#v", mem.ReasonCounts)
	}
	if len(mem.ResolutionNotices) != 1 || mem.ResolutionNotices[0].ReasonCode != ReasonResolutionError {
		t.Fatalf("resolution notices = %#v", mem.ResolutionNotices)
	}

	if _, err := Offer(mem, view, opening, evaluationexplore.Budget{}); err != nil {
		t.Fatal(err)
	}
	firstReject, err := ApplyRejection(mem, RejectedDisposition{
		OfferID:    "offer-generic",
		AgentID:    "task-agent-1",
		Ref:        "skill://evaluation/generic@1",
		ReasonCode: ReasonTooGeneric,
		Reason:     "need the specialized descendant",
	}, view, opening, evaluationexplore.Budget{})
	if err != nil {
		t.Fatal(err)
	}
	if firstReject.Offer == nil || firstReject.Offer.SkillReference != "skill://evaluation/specialized@1" {
		t.Fatalf("first redirect = %#v", firstReject.Offer)
	}

	exhausted, err := ApplyRejection(mem, RejectedDisposition{
		OfferID:    "offer-specialized",
		AgentID:    "task-agent-1",
		Ref:        "skill://evaluation/specialized@1",
		ReasonCode: ReasonNotApplicable,
		Reason:     "specialized descendant is also inapplicable",
	}, view, opening, evaluationexplore.Budget{})
	if err != nil {
		t.Fatal(err)
	}
	if exhausted.Offer != nil {
		t.Fatalf("exhausted rejection still offered %#v", exhausted.Offer)
	}
	if exhausted.Terminal != TerminalAllCandidatesRejected {
		t.Fatalf("terminal = %q, want %s", exhausted.Terminal, TerminalAllCandidatesRejected)
	}
	if mem.ReasonCounts[ReasonResolutionError] != 0 {
		t.Fatalf("resolution error leaked into counts: %#v", mem.ReasonCounts)
	}
	if mem.ReasonCounts[ReasonTooGeneric] != 1 || mem.ReasonCounts[ReasonNotApplicable] != 1 {
		t.Fatalf("rejection counts = %#v", mem.ReasonCounts)
	}
	if _, ok := mem.ReasonCounts[ReasonResolutionError]; ok {
		t.Fatal("resolution_error key must not appear in rejection reason statistics")
	}

	replay, err := ApplyRejection(mem, RejectedDisposition{
		OfferID:    "offer-generic",
		AgentID:    "task-agent-1",
		Ref:        "skill://evaluation/generic@1",
		ReasonCode: ReasonTooGeneric,
		Reason:     "need the specialized descendant",
	}, view, opening, evaluationexplore.Budget{})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Delivery == nil || replay.Delivery.DeliveryID != firstReject.Delivery.DeliveryID {
		t.Fatalf("idempotent replay delivery = %#v", replay.Delivery)
	}
	if mem.ReasonCounts[ReasonTooGeneric] != 1 {
		t.Fatalf("idempotent replay double-counted: %#v", mem.ReasonCounts)
	}
}

func TestUnknownReasonCodeIsRejected(t *testing.T) {
	view := mustView(t, singleRecords("alpha", "body-alpha"))
	mem := NewMemory("need-unknown")
	opening := openingFor("skill://evaluation/alpha@1", "body-alpha")
	if _, err := Offer(mem, view, opening, evaluationexplore.Budget{}); err != nil {
		t.Fatal(err)
	}
	_, err := ApplyRejection(mem, RejectedDisposition{
		OfferID:    "offer-unknown",
		AgentID:    "task-agent-1",
		Ref:        "skill://evaluation/alpha@1",
		ReasonCode: "vibes",
		Reason:     "not a core reason",
	}, view, opening, evaluationexplore.Budget{})
	if !errors.Is(err, ErrUnknownReason) {
		t.Fatalf("error = %v, want unknown reason", err)
	}
}

func specializeRecords(genericBody, specializedBody string) []evaluationgraph.CanonicalRecord {
	return []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-generic", "generic", 1, genericBody),
		proposal(2, "proposal-specialized", "specialized", 1, specializedBody),
		consolidate(3, "consolidate-generic", "generic", 1),
		consolidate(4, "consolidate-specialized", "specialized", 1),
		relation(5, "generic-specialized", "generic", 1, "specialized", 1, evaluationexplore.RelSpecializes),
	}
}

func relatedRecords(left, right, leftBody, rightBody string) []evaluationgraph.CanonicalRecord {
	return []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-"+left, left, 1, leftBody),
		proposal(2, "proposal-"+right, right, 1, rightBody),
		consolidate(3, "consolidate-"+left, left, 1),
		consolidate(4, "consolidate-"+right, right, 1),
		relation(5, left+"-"+right, left, 1, right, 1, evaluationexplore.RelRelatedTo),
	}
}

func singleRecords(lineage, body string) []evaluationgraph.CanonicalRecord {
	return []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-"+lineage, lineage, 1, body),
		consolidate(2, "consolidate-"+lineage, lineage, 1),
	}
}

func conflictRecords() []evaluationgraph.CanonicalRecord {
	return []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-subject", "subject", 1, "conflict-subject"),
		proposal(2, "proposal-rival", "rival", 1, "conflict-rival"),
		proposal(3, "proposal-alternative", "alternative", 1, "conflict-alternative"),
		proposal(4, "proposal-support", "support-anchor", 1, "support-evidence"),
		consolidate(5, "consolidate-subject", "subject", 1),
		consolidate(6, "consolidate-rival", "rival", 1),
		consolidate(7, "consolidate-alternative", "alternative", 1),
		consolidate(8, "consolidate-support", "support-anchor", 1),
		relation(9, "subject-rival", "subject", 1, "rival", 1, evaluationexplore.RelConditionallyConflictsWith),
		relation(10, "subject-alternative", "subject", 1, "alternative", 1, evaluationexplore.RelRelatedTo),
		relation(11, "alternative-support", "alternative", 1, "support-anchor", 1, evaluationexplore.RelSupports),
	}
}

func sourceSetRecords() []evaluationgraph.CanonicalRecord {
	return []evaluationgraph.CanonicalRecord{
		proposal(1, "proposal-twin-a", "twin-a", 1, "source-twin-a"),
		proposal(2, "proposal-twin-b", "twin-b", 1, "source-twin-b"),
		proposal(3, "proposal-unique-c", "unique-c", 1, "source-unique-c"),
		proposal(4, "proposal-shared-anchor", "shared-anchor", 1, "shared-anchor-body"),
		proposal(5, "proposal-other-anchor", "other-anchor", 1, "other-anchor-body"),
		consolidate(6, "consolidate-twin-a", "twin-a", 1),
		consolidate(7, "consolidate-twin-b", "twin-b", 1),
		consolidate(8, "consolidate-unique-c", "unique-c", 1),
		consolidate(9, "consolidate-shared-anchor", "shared-anchor", 1),
		consolidate(10, "consolidate-other-anchor", "other-anchor", 1),
		relation(11, "twin-a-shared", "twin-a", 1, "shared-anchor", 1, evaluationexplore.RelSupports),
		relation(12, "twin-b-shared", "twin-b", 1, "shared-anchor", 1, evaluationexplore.RelSupports),
		relation(13, "unique-c-other", "unique-c", 1, "other-anchor", 1, evaluationexplore.RelSupports),
		relation(14, "twin-a-twin-b", "twin-a", 1, "twin-b", 1, evaluationexplore.RelRelatedTo),
		relation(15, "twin-a-unique-c", "twin-a", 1, "unique-c", 1, evaluationexplore.RelRelatedTo),
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

func openingFor(seed, query string) evaluationexplore.OpeningContext {
	return evaluationexplore.OpeningContext{
		SeedRefs:      []string{seed},
		Query:         query,
		CheckpointID:  "cp-opening-1",
		TargetAgentID: "task-agent-1",
	}
}

func containsString(value, substr string) bool {
	return strings.Contains(value, substr)
}
