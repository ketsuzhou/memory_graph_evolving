package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGraphBatchPairedReportKeepsMissingTimeoutNoOutputInfraFailureInDenominatorAsZero(t *testing.T) {
	policy := testGraphBatchReportPolicy()
	req := graphBatchPairedReportRequest{
		Manifest: graphBatchPreregisteredManifest{
			graphBatchReportPolicy: policy,
			Tasks: []graphBatchPreregisteredTask{
				{TaskID: "task-pass"},
				{TaskID: "task-missing"},
				{TaskID: "task-timeout"},
				{TaskID: "task-no-output"},
				{TaskID: "task-infra"},
			},
		},
		Warm: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{
				{TaskID: "task-pass", GraderOutcome: graphBatchOutcomePass, MemoryTerminal: "accepted"},
				{TaskID: "task-missing", GraderOutcome: graphBatchOutcomeMissing, MemoryTerminal: "protocol_error"},
				{TaskID: "task-timeout", GraderOutcome: graphBatchOutcomeTimeout, MemoryTerminal: "memory_timeout"},
				{TaskID: "task-no-output", GraderOutcome: graphBatchOutcomeNoOutput, MemoryTerminal: "no_applicable_candidate"},
				{TaskID: "task-infra", GraderOutcome: graphBatchOutcomeInfraFailure, MemoryTerminal: "memory_error"},
			},
		},
		Cold: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{
				{TaskID: "task-pass", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-missing", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-timeout", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-no-output", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-infra", GraderOutcome: graphBatchOutcomeFail},
			},
		},
	}

	report, err := generateGraphBatchPairedReport(req)
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	if report.PassAt1.Warm.Denominator != 5 || report.PassAt1.Cold.Denominator != 5 {
		t.Fatalf("denominator warm=%d cold=%d; want 5/5 preregistered tasks", report.PassAt1.Warm.Denominator, report.PassAt1.Cold.Denominator)
	}
	if report.PassAt1.Warm.Passed != 1 || report.PassAt1.Warm.Rate != "1/5" {
		t.Fatalf("warm pass@1 = %d %q; want 1/5 with missing/timeout/no-output/infra scored 0", report.PassAt1.Warm.Passed, report.PassAt1.Warm.Rate)
	}
	if report.Paired.Total != 5 || len(report.Pairings) != 5 {
		t.Fatalf("pairings = %d total=%d; want every preregistered task retained", len(report.Pairings), report.Paired.Total)
	}
	byID := map[string]graphBatchPairing{}
	for _, pairing := range report.Pairings {
		byID[pairing.TaskID] = pairing
	}
	for _, taskID := range []string{"task-missing", "task-timeout", "task-no-output", "task-infra"} {
		pairing, ok := byID[taskID]
		if !ok {
			t.Fatalf("preregistered %s dropped from the denominator", taskID)
		}
		if pairing.WarmPassed {
			t.Fatalf("%s counted as a pass; want grader non-pass kept as 0", taskID)
		}
	}
}

func TestGraphBatchPairedReportRefusesDuplicateMissingTaskAndPolicyMismatch(t *testing.T) {
	valid := func() graphBatchPairedReportRequest {
		return testGraphBatchPairedRequest()
	}
	cases := []struct {
		name   string
		mutate func(*graphBatchPairedReportRequest)
		want   string
	}{
		{
			name: "duplicate manifest task",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Manifest.Tasks = append(req.Manifest.Tasks, graphBatchPreregisteredTask{TaskID: "task-a"})
			},
			want: "duplicate task",
		},
		{
			name: "duplicate warm task",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Warm.Observations = append(req.Warm.Observations, req.Warm.Observations[0])
			},
			want: "duplicate task",
		},
		{
			name: "missing warm task",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Warm.Observations = req.Warm.Observations[:3]
			},
			want: "missing task",
		},
		{
			name: "missing cold task",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Cold.Observations = req.Cold.Observations[1:]
			},
			want: "missing task",
		},
		{
			name: "extra task not in manifest",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Warm.Observations = append(req.Warm.Observations, graphBatchTaskObservation{TaskID: "task-extra", GraderOutcome: graphBatchOutcomeFail})
			},
			want: "missing task",
		},
		{
			name: "manifest digest mismatch",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Warm.ManifestDigest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			},
			want: "manifest mismatch",
		},
		{
			name: "model mismatch",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Cold.Model = "other-model"
			},
			want: "model mismatch",
		},
		{
			name: "seed mismatch",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Warm.Seed = "99"
			},
			want: "seed mismatch",
		},
		{
			name: "grading policy mismatch",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Cold.GradingPolicy = "other-grading"
			},
			want: "grading policy mismatch",
		},
		{
			name: "salvage policy mismatch",
			mutate: func(req *graphBatchPairedReportRequest) {
				req.Warm.SalvagePolicy = "other-salvage"
			},
			want: "salvage policy mismatch",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			req := valid()
			testCase.mutate(&req)
			_, err := generateGraphBatchPairedReport(req)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v; want %q", err, testCase.want)
			}
		})
	}
}

func TestGraphBatchPairedReportCountsOnlyExactBodyDigestDeliveryAsServed(t *testing.T) {
	policy := testGraphBatchReportPolicy()
	exact := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cited := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	partial := "sha256:cccc"
	req := graphBatchPairedReportRequest{
		Manifest: graphBatchPreregisteredManifest{
			graphBatchReportPolicy: policy,
			Tasks:                  []graphBatchPreregisteredTask{{TaskID: "task-a"}},
		},
		Warm: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{{
				TaskID:         "task-a",
				GraderOutcome:  graphBatchOutcomePass,
				MemoryTerminal: "accepted",
				Mechanism: graphBatchTaskMechanism{
					Offers: []graphBatchOfferRecord{
						{
							OfferID:      "offer-recall",
							DeliveryKind: graphBatchDeliveryRecallCitation,
							BodyDigest:   cited,
							Disposition:  graphBatchDispositionAccepted,
							Kind:         graphBatchKindOriginal,
							Verdict:      "supported",
							Adopted:      true,
						},
						{
							OfferID:      "offer-partial",
							DeliveryKind: graphBatchDeliveryExactDigest,
							BodyDigest:   partial,
							Disposition:  graphBatchDispositionAccepted,
							Kind:         graphBatchKindOriginal,
						},
						{
							OfferID:      "offer-exact",
							DeliveryKind: graphBatchDeliveryExactDigest,
							BodyDigest:   exact,
							Disposition:  graphBatchDispositionAccepted,
							Kind:         graphBatchKindOriginal,
							Verdict:      "supported",
						},
					},
				},
			}},
		},
		Cold: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations:           []graphBatchTaskObservation{{TaskID: "task-a", GraderOutcome: graphBatchOutcomeFail}},
		},
	}

	report, err := generateGraphBatchPairedReport(req)
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	if report.MechanismFunnel.Offers != 3 || report.MechanismFunnel.ExactDigestServed != 1 || report.MechanismFunnel.RecallCitationsIgnored != 1 {
		t.Fatalf("funnel = %#v; want 1 exact served and 1 ignored recall citation", report.MechanismFunnel)
	}
	if report.MechanismFunnel.Accepted != 1 || report.MechanismFunnel.Adopted != 0 {
		t.Fatalf("recall citation leaked into served accepted/adopted: %#v", report.MechanismFunnel)
	}
	if len(report.ServedDigests) != 1 || report.ServedDigests[0].OfferID != "offer-exact" || report.ServedDigests[0].BodyDigest != exact {
		t.Fatalf("served digests = %#v; only exact body digest delivery may count", report.ServedDigests)
	}
	if len(report.Verdicts) != 1 || report.Verdicts[0].OfferID != "offer-exact" {
		t.Fatalf("verdicts = %#v; recall citation must not receive a served verdict", report.Verdicts)
	}
}

func TestGraphBatchPairedReportWinsLossesTiesEqualCompletePairedManifest(t *testing.T) {
	report, err := generateGraphBatchPairedReport(testGraphBatchPairedRequest())
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	if report.Paired.Wins+report.Paired.Losses+report.Paired.Ties != report.Paired.Total {
		t.Fatalf("wins(%d)+losses(%d)+ties(%d) != total(%d)", report.Paired.Wins, report.Paired.Losses, report.Paired.Ties, report.Paired.Total)
	}
	if report.Paired.Total != 4 || report.Paired.Wins != 1 || report.Paired.Losses != 1 || report.Paired.Ties != 2 {
		t.Fatalf("paired = %#v; want 1 win, 1 loss, 2 ties over the full manifest", report.Paired)
	}
	if len(report.Pairings) != 4 {
		t.Fatalf("pairings = %d; want the complete paired manifest", len(report.Pairings))
	}
	want := map[string]string{"task-a": graphBatchPairWin, "task-b": graphBatchPairLoss, "task-c": graphBatchPairTie, "task-d": graphBatchPairTie}
	for _, pairing := range report.Pairings {
		if pairing.Result != want[pairing.TaskID] {
			t.Fatalf("%s result = %q; want %q", pairing.TaskID, pairing.Result, want[pairing.TaskID])
		}
		delete(want, pairing.TaskID)
	}
	if len(want) != 0 {
		t.Fatalf("pairings omitted tasks %v", want)
	}
}

func TestGraphBatchPairedReportGoldenJSONIsStableAndDisclosesUnmatchedProtocolCost(t *testing.T) {
	report, err := generateGraphBatchPairedReport(testGraphBatchPairedRequest())
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	first, err := marshalGraphBatchPairedReport(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	second, err := marshalGraphBatchPairedReport(report)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("golden JSON is not stable across remarsals")
	}
	if string(first) != graphBatchPairedReportGoldenJSON {
		t.Fatalf("golden JSON drifted\n got:\n%s\nwant:\n%s", first, graphBatchPairedReportGoldenJSON)
	}
	if !strings.Contains(string(first), "protocol token/time unmatched") {
		t.Fatalf("report does not disclose unmatched protocol token/time costs:\n%s", first)
	}
	var decoded graphBatchPairedReport
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatalf("golden JSON is not decodable: %v", err)
	}
	if decoded.PassAt1.Warm.Rate != "2/4" || decoded.PassAt1.Cold.Rate != "2/4" {
		t.Fatalf("decoded pass@1 = %#v", decoded.PassAt1)
	}
}

func TestGraphBatchPairedReportUsesGraderOutcomeNotMechanismAsPassAuthority(t *testing.T) {
	policy := testGraphBatchReportPolicy()
	req := graphBatchPairedReportRequest{
		Manifest: graphBatchPreregisteredManifest{
			graphBatchReportPolicy: policy,
			Tasks:                  []graphBatchPreregisteredTask{{TaskID: "task-a"}},
		},
		Warm: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{{
				TaskID:         "task-a",
				GraderOutcome:  graphBatchOutcomeFail,
				MemoryTerminal: "accepted",
				Mechanism: graphBatchTaskMechanism{Offers: []graphBatchOfferRecord{{
					OfferID:      "offer-a",
					DeliveryKind: graphBatchDeliveryExactDigest,
					BodyDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Disposition:  graphBatchDispositionAccepted,
					Adopted:      true,
					Verdict:      "supported",
				}}},
			}},
		},
		Cold: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations:           []graphBatchTaskObservation{{TaskID: "task-a", GraderOutcome: graphBatchOutcomeFail}},
		},
	}
	report, err := generateGraphBatchPairedReport(req)
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	if report.PassAt1.Warm.Passed != 0 || report.Pairings[0].WarmPassed {
		t.Fatalf("accepted/adopted mechanism overrode grader fail: %#v", report.PassAt1)
	}
}

func testGraphBatchReportPolicy() graphBatchReportPolicy {
	return graphBatchReportPolicy{
		ManifestDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		Model:          "warm-skill-graph-batch.model.v1",
		Seed:           "7",
		GradingPolicy:  "warm-skill-graph-batch.grading.v1",
		SalvagePolicy:  "pre-fixed.v1",
	}
}

func testGraphBatchPairedRequest() graphBatchPairedReportRequest {
	policy := testGraphBatchReportPolicy()
	return graphBatchPairedReportRequest{
		Manifest: graphBatchPreregisteredManifest{
			graphBatchReportPolicy: policy,
			Tasks: []graphBatchPreregisteredTask{
				{TaskID: "task-a"},
				{TaskID: "task-b"},
				{TaskID: "task-c"},
				{TaskID: "task-d"},
			},
		},
		Warm: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{
				{
					TaskID:         "task-a",
					GraderOutcome:  graphBatchOutcomePass,
					MemoryTerminal: "accepted",
					Mechanism: graphBatchTaskMechanism{
						GraphSeeds: 1, GraphSteps: 3,
						Offers: []graphBatchOfferRecord{{
							OfferID:      "offer-a",
							DeliveryKind: graphBatchDeliveryExactDigest,
							BodyDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							Disposition:  graphBatchDispositionAccepted,
							Kind:         graphBatchKindOriginal,
							Verdict:      "supported",
						}},
					},
				},
				{
					TaskID:         "task-b",
					GraderOutcome:  graphBatchOutcomeFail,
					MemoryTerminal: "all_candidates_rejected",
					Mechanism: graphBatchTaskMechanism{
						GraphSeeds: 2, GraphSteps: 4, Redirects: 1,
						Offers: []graphBatchOfferRecord{
							{
								OfferID:      "offer-b-recall",
								DeliveryKind: graphBatchDeliveryRecallCitation,
								BodyDigest:   "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
								Disposition:  graphBatchDispositionAccepted,
								Kind:         graphBatchKindOriginal,
								Verdict:      "supported",
							},
							{
								OfferID:      "offer-b",
								DeliveryKind: graphBatchDeliveryExactDigest,
								BodyDigest:   "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
								Disposition:  graphBatchDispositionRejected,
								ReasonCode:   "not_applicable",
								Reason:       "guard mismatch",
								Kind:         graphBatchKindOriginal,
								Verdict:      "refuted",
							},
						},
					},
				},
				{
					TaskID:         "task-c",
					GraderOutcome:  graphBatchOutcomeTimeout,
					MemoryTerminal: "memory_timeout",
				},
				{
					TaskID:         "task-d",
					GraderOutcome:  graphBatchOutcomePass,
					MemoryTerminal: "accepted",
					Mechanism: graphBatchTaskMechanism{
						GraphSeeds: 1, GraphSteps: 2, Redirects: 1,
						Offers: []graphBatchOfferRecord{{
							OfferID:      "offer-d",
							DeliveryKind: graphBatchDeliveryExactDigest,
							BodyDigest:   "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
							Disposition:  graphBatchDispositionAccepted,
							Kind:         graphBatchKindAdaptation,
							Adopted:      true,
							Verdict:      "supported",
						}},
					},
				},
			},
		},
		Cold: graphBatchArmResult{
			graphBatchReportPolicy: policy,
			Observations: []graphBatchTaskObservation{
				{TaskID: "task-a", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-b", GraderOutcome: graphBatchOutcomePass},
				{TaskID: "task-c", GraderOutcome: graphBatchOutcomeFail},
				{TaskID: "task-d", GraderOutcome: graphBatchOutcomePass},
			},
		},
	}
}

const graphBatchPairedReportGoldenJSON = `{
  "schema_version": "warm-skill-graph-batch.paired-report.v1",
  "strategy": "warm-skill-graph-batch",
  "policy": {
    "manifest_digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
    "model": "warm-skill-graph-batch.model.v1",
    "seed": "7",
    "grading_policy": "warm-skill-graph-batch.grading.v1",
    "salvage_policy": "pre-fixed.v1"
  },
  "pass_at_1": {
    "warm": {
      "passed": 2,
      "denominator": 4,
      "rate": "2/4"
    },
    "cold": {
      "passed": 2,
      "denominator": 4,
      "rate": "2/4"
    }
  },
  "paired": {
    "wins": 1,
    "losses": 1,
    "ties": 2,
    "total": 4
  },
  "pairings": [
    {
      "task_id": "task-a",
      "warm_outcome": "pass",
      "cold_outcome": "fail",
      "warm_passed": true,
      "cold_passed": false,
      "result": "win"
    },
    {
      "task_id": "task-b",
      "warm_outcome": "fail",
      "cold_outcome": "pass",
      "warm_passed": false,
      "cold_passed": true,
      "result": "loss"
    },
    {
      "task_id": "task-c",
      "warm_outcome": "timeout",
      "cold_outcome": "fail",
      "warm_passed": false,
      "cold_passed": false,
      "result": "tie"
    },
    {
      "task_id": "task-d",
      "warm_outcome": "pass",
      "cold_outcome": "pass",
      "warm_passed": true,
      "cold_passed": true,
      "result": "tie"
    }
  ],
  "mechanism_funnel": {
    "graph_seeds": 4,
    "graph_steps": 9,
    "redirects": 2,
    "offers": 4,
    "exact_digest_served": 3,
    "exact_digest_served_rate": "3/4",
    "accepted": 2,
    "rejected": 1,
    "no_response": 0,
    "adopted": 1,
    "recall_citations_ignored": 1,
    "normal_no_skill": 0,
    "budget_failures": 0,
    "infra_failures": 1,
    "disposition_outcomes": [
      {
        "disposition": "accepted",
        "outcome": "pass",
        "count": 2
      },
      {
        "disposition": "rejected",
        "outcome": "fail",
        "count": 1
      }
    ]
  },
  "reason_distribution": [
    {
      "reason_code": "not_applicable",
      "count": 1
    }
  ],
  "served_digests": [
    {
      "task_id": "task-a",
      "offer_id": "offer-a",
      "body_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "kind": "original"
    },
    {
      "task_id": "task-b",
      "offer_id": "offer-b",
      "body_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "kind": "original"
    },
    {
      "task_id": "task-d",
      "offer_id": "offer-d",
      "body_digest": "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      "kind": "adaptation"
    }
  ],
  "adaptations": {
    "original_served": 2,
    "adaptation_served": 1
  },
  "verdicts": [
    {
      "task_id": "task-a",
      "offer_id": "offer-a",
      "body_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "verdict": "supported",
      "disposition": "accepted",
      "warm_outcome": "pass"
    },
    {
      "task_id": "task-b",
      "offer_id": "offer-b",
      "body_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "verdict": "refuted",
      "disposition": "rejected",
      "warm_outcome": "fail"
    },
    {
      "task_id": "task-d",
      "offer_id": "offer-d",
      "body_digest": "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      "verdict": "supported",
      "disposition": "accepted",
      "warm_outcome": "pass"
    }
  ],
  "terminal_taxonomy": [
    {
      "name": "accepted",
      "count": 2
    },
    {
      "name": "all_candidates_rejected",
      "count": 1
    },
    {
      "name": "memory_timeout",
      "count": 1
    }
  ],
  "known_limitations": [
    "generation-0 empty graph: train episodes inherit no Skill feedback",
    "task_context_monitoring=opening_only: Memory Explore Agent does not re-judge mid-task context",
    "protocol token/time unmatched: memory, interrupt, and Skill-tool token and wall-clock costs are not matched across arms",
    "test feedback is trace_only and never writes back to the trainable graph",
    "crash uncertainty fail closed: inconsistent process state is not auto-recovered",
    "contract smoke Host session seams stay scripted until real Pi 0.85.1 abort/resume is proven; diagnosis/consolidation/freeze production path is GMS HTTP"
  ]
}
`
