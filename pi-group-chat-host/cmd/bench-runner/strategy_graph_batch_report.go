package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	graphBatchPairedReportSchema = "warm-skill-graph-batch.paired-report.v1"

	graphBatchOutcomePass         = "pass"
	graphBatchOutcomeFail         = "fail"
	graphBatchOutcomeTimeout      = "timeout"
	graphBatchOutcomeNoOutput     = "no-output"
	graphBatchOutcomeMissing      = "missing"
	graphBatchOutcomeInfraFailure = "infra_failure"

	graphBatchPairWin  = "win"
	graphBatchPairLoss = "loss"
	graphBatchPairTie  = "tie"

	graphBatchDeliveryExactDigest    = "exact_body_digest"
	graphBatchDeliveryRecallCitation = "recall_citation"
	graphBatchDispositionAccepted    = "accepted"
	graphBatchDispositionRejected    = "rejected"
	graphBatchDispositionNoResponse  = "no-response"
	graphBatchKindOriginal           = "original"
	graphBatchKindAdaptation         = "adaptation"
)

var graphBatchExactDigestRE = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

var graphBatchKnownLimitations = []string{
	"generation-0 empty graph: train episodes inherit no Skill feedback",
	"task_context_monitoring=opening_only: Memory Explore Agent does not re-judge mid-task context",
	"protocol token/time unmatched: memory, interrupt, and Skill-tool token and wall-clock costs are not matched across arms",
	"test feedback is trace_only and never writes back to the trainable graph",
	"crash uncertainty fail closed: inconsistent process state is not auto-recovered",
	"contract smoke Host session seams stay scripted until real Pi 0.85.1 abort/resume is proven; diagnosis/consolidation/freeze production path is GMS HTTP",
}

// graphBatchReportPolicy is the frozen pairing identity. Warm and cold must
// share these pins with the preregistered manifest or report generation fails.
type graphBatchReportPolicy struct {
	ManifestDigest string `json:"manifest_digest"`
	Model          string `json:"model"`
	Seed           string `json:"seed"`
	GradingPolicy  string `json:"grading_policy"`
	SalvagePolicy  string `json:"salvage_policy"`
}

type graphBatchPreregisteredTask struct {
	TaskID string `json:"task_id"`
}

type graphBatchPreregisteredManifest struct {
	graphBatchReportPolicy
	Tasks []graphBatchPreregisteredTask `json:"tasks"`
}

type graphBatchOfferRecord struct {
	OfferID        string `json:"offer_id"`
	SkillReference string `json:"skill_reference,omitempty"`
	DeliveryKind   string `json:"delivery_kind"`
	BodyDigest     string `json:"body_digest,omitempty"`
	Disposition    string `json:"disposition,omitempty"`
	ReasonCode     string `json:"reason_code,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Kind           string `json:"kind,omitempty"`
	Adopted        bool   `json:"adopted,omitempty"`
	Verdict        string `json:"verdict,omitempty"`
}

type graphBatchTaskMechanism struct {
	GraphSeeds int                     `json:"graph_seeds"`
	GraphSteps int                     `json:"graph_steps"`
	Redirects  int                     `json:"redirects"`
	Offers     []graphBatchOfferRecord `json:"offers,omitempty"`
}

type graphBatchTaskObservation struct {
	TaskID         string                  `json:"task_id"`
	GraderOutcome  string                  `json:"grader_outcome"`
	MemoryTerminal string                  `json:"memory_terminal,omitempty"`
	Mechanism      graphBatchTaskMechanism `json:"mechanism"`
}

type graphBatchArmResult struct {
	graphBatchReportPolicy
	Observations []graphBatchTaskObservation `json:"observations"`
}

type graphBatchPairedReportRequest struct {
	Manifest graphBatchPreregisteredManifest
	Warm     graphBatchArmResult
	Cold     graphBatchArmResult
}

type graphBatchPassAt1 struct {
	Passed      int    `json:"passed"`
	Denominator int    `json:"denominator"`
	Rate        string `json:"rate"`
}

type graphBatchPassAt1Pair struct {
	Warm graphBatchPassAt1 `json:"warm"`
	Cold graphBatchPassAt1 `json:"cold"`
}

type graphBatchPairedCounts struct {
	Wins   int `json:"wins"`
	Losses int `json:"losses"`
	Ties   int `json:"ties"`
	Total  int `json:"total"`
}

type graphBatchPairing struct {
	TaskID      string `json:"task_id"`
	WarmOutcome string `json:"warm_outcome"`
	ColdOutcome string `json:"cold_outcome"`
	WarmPassed  bool   `json:"warm_passed"`
	ColdPassed  bool   `json:"cold_passed"`
	Result      string `json:"result"`
}

type graphBatchNamedCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type graphBatchReasonCount struct {
	ReasonCode string `json:"reason_code"`
	Count      int    `json:"count"`
}

type graphBatchServedDigest struct {
	TaskID     string `json:"task_id"`
	OfferID    string `json:"offer_id"`
	BodyDigest string `json:"body_digest"`
	Kind       string `json:"kind"`
}

type graphBatchVerdictRow struct {
	TaskID      string `json:"task_id"`
	OfferID     string `json:"offer_id"`
	BodyDigest  string `json:"body_digest"`
	Verdict     string `json:"verdict"`
	Disposition string `json:"disposition"`
	WarmOutcome string `json:"warm_outcome"`
}

type graphBatchDispositionOutcome struct {
	Disposition string `json:"disposition"`
	Outcome     string `json:"outcome"`
	Count       int    `json:"count"`
}

type graphBatchAdaptationCounts struct {
	OriginalServed   int `json:"original_served"`
	AdaptationServed int `json:"adaptation_served"`
}

type graphBatchMechanismFunnel struct {
	GraphSeeds             int                            `json:"graph_seeds"`
	GraphSteps             int                            `json:"graph_steps"`
	Redirects              int                            `json:"redirects"`
	Offers                 int                            `json:"offers"`
	ExactDigestServed      int                            `json:"exact_digest_served"`
	ExactDigestServedRate  string                         `json:"exact_digest_served_rate"`
	Accepted               int                            `json:"accepted"`
	Rejected               int                            `json:"rejected"`
	NoResponse             int                            `json:"no_response"`
	Adopted                int                            `json:"adopted"`
	RecallCitationsIgnored int                            `json:"recall_citations_ignored"`
	NormalNoSkill          int                            `json:"normal_no_skill"`
	BudgetFailures         int                            `json:"budget_failures"`
	InfraFailures          int                            `json:"infra_failures"`
	DispositionOutcomes    []graphBatchDispositionOutcome `json:"disposition_outcomes"`
}

type graphBatchPairedReport struct {
	SchemaVersion      string                     `json:"schema_version"`
	Strategy           string                     `json:"strategy"`
	Policy             graphBatchReportPolicy     `json:"policy"`
	PassAt1            graphBatchPassAt1Pair      `json:"pass_at_1"`
	Paired             graphBatchPairedCounts     `json:"paired"`
	Pairings           []graphBatchPairing        `json:"pairings"`
	MechanismFunnel    graphBatchMechanismFunnel  `json:"mechanism_funnel"`
	ReasonDistribution []graphBatchReasonCount    `json:"reason_distribution"`
	ServedDigests      []graphBatchServedDigest   `json:"served_digests"`
	Adaptations        graphBatchAdaptationCounts `json:"adaptations"`
	Verdicts           []graphBatchVerdictRow     `json:"verdicts"`
	TerminalTaxonomy   []graphBatchNamedCount     `json:"terminal_taxonomy"`
	KnownLimitations   []string                   `json:"known_limitations"`
}

// generateGraphBatchPairedReport builds the full-denominator paired report.
// The preregistered held-out manifest is the only pass@1 denominator; grader
// outcome is the only pass authority. Mechanism facts never change scoring.
func generateGraphBatchPairedReport(req graphBatchPairedReportRequest) (graphBatchPairedReport, error) {
	if err := validateGraphBatchReportPolicy(req.Manifest.graphBatchReportPolicy); err != nil {
		return graphBatchPairedReport{}, err
	}
	if len(req.Manifest.Tasks) == 0 {
		return graphBatchPairedReport{}, fmt.Errorf("graph batch paired report: preregistered manifest has no tasks")
	}
	if err := matchGraphBatchReportPolicy("warm", req.Manifest.graphBatchReportPolicy, req.Warm.graphBatchReportPolicy); err != nil {
		return graphBatchPairedReport{}, err
	}
	if err := matchGraphBatchReportPolicy("cold", req.Manifest.graphBatchReportPolicy, req.Cold.graphBatchReportPolicy); err != nil {
		return graphBatchPairedReport{}, err
	}
	taskIDs, err := uniqueManifestTaskIDs(req.Manifest.Tasks)
	if err != nil {
		return graphBatchPairedReport{}, err
	}
	warm, err := indexGraphBatchObservations("warm", taskIDs, req.Warm.Observations)
	if err != nil {
		return graphBatchPairedReport{}, err
	}
	cold, err := indexGraphBatchObservations("cold", taskIDs, req.Cold.Observations)
	if err != nil {
		return graphBatchPairedReport{}, err
	}

	pairings := make([]graphBatchPairing, 0, len(taskIDs))
	warmPassed, coldPassed := 0, 0
	wins, losses, ties := 0, 0, 0
	for _, taskID := range taskIDs {
		warmObs := warm[taskID]
		coldObs := cold[taskID]
		if err := validateGraphBatchGraderOutcome("warm", taskID, warmObs.GraderOutcome); err != nil {
			return graphBatchPairedReport{}, err
		}
		if err := validateGraphBatchGraderOutcome("cold", taskID, coldObs.GraderOutcome); err != nil {
			return graphBatchPairedReport{}, err
		}
		wPass := warmObs.GraderOutcome == graphBatchOutcomePass
		cPass := coldObs.GraderOutcome == graphBatchOutcomePass
		if wPass {
			warmPassed++
		}
		if cPass {
			coldPassed++
		}
		result := graphBatchPairTie
		switch {
		case wPass && !cPass:
			result = graphBatchPairWin
			wins++
		case cPass && !wPass:
			result = graphBatchPairLoss
			losses++
		default:
			ties++
		}
		pairings = append(pairings, graphBatchPairing{
			TaskID:      taskID,
			WarmOutcome: warmObs.GraderOutcome,
			ColdOutcome: coldObs.GraderOutcome,
			WarmPassed:  wPass,
			ColdPassed:  cPass,
			Result:      result,
		})
	}

	funnel, reasons, served, verdicts, terminals, adaptations := aggregateGraphBatchMechanism(taskIDs, warm)
	denom := len(taskIDs)
	return graphBatchPairedReport{
		SchemaVersion: graphBatchPairedReportSchema,
		Strategy:      graphBatchStrategyID,
		Policy:        req.Manifest.graphBatchReportPolicy,
		PassAt1: graphBatchPassAt1Pair{
			Warm: graphBatchPassAt1{Passed: warmPassed, Denominator: denom, Rate: graphBatchRate(warmPassed, denom)},
			Cold: graphBatchPassAt1{Passed: coldPassed, Denominator: denom, Rate: graphBatchRate(coldPassed, denom)},
		},
		Paired:             graphBatchPairedCounts{Wins: wins, Losses: losses, Ties: ties, Total: denom},
		Pairings:           pairings,
		MechanismFunnel:    funnel,
		ReasonDistribution: reasons,
		ServedDigests:      served,
		Adaptations:        adaptations,
		Verdicts:           verdicts,
		TerminalTaxonomy:   terminals,
		KnownLimitations:   append([]string(nil), graphBatchKnownLimitations...),
	}, nil
}

func marshalGraphBatchPairedReport(report graphBatchPairedReport) ([]byte, error) {
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

func validateGraphBatchReportPolicy(policy graphBatchReportPolicy) error {
	switch {
	case strings.TrimSpace(policy.ManifestDigest) == "":
		return fmt.Errorf("graph batch paired report: manifest digest is required")
	case strings.TrimSpace(policy.Model) == "":
		return fmt.Errorf("graph batch paired report: model is required")
	case strings.TrimSpace(policy.Seed) == "":
		return fmt.Errorf("graph batch paired report: seed is required")
	case strings.TrimSpace(policy.GradingPolicy) == "":
		return fmt.Errorf("graph batch paired report: grading policy is required")
	case strings.TrimSpace(policy.SalvagePolicy) == "":
		return fmt.Errorf("graph batch paired report: salvage policy is required")
	default:
		return nil
	}
}

func matchGraphBatchReportPolicy(arm string, want, got graphBatchReportPolicy) error {
	switch {
	case got.ManifestDigest != want.ManifestDigest:
		return fmt.Errorf("graph batch paired report: %s manifest mismatch", arm)
	case got.Model != want.Model:
		return fmt.Errorf("graph batch paired report: %s model mismatch", arm)
	case got.Seed != want.Seed:
		return fmt.Errorf("graph batch paired report: %s seed mismatch", arm)
	case got.GradingPolicy != want.GradingPolicy:
		return fmt.Errorf("graph batch paired report: %s grading policy mismatch", arm)
	case got.SalvagePolicy != want.SalvagePolicy:
		return fmt.Errorf("graph batch paired report: %s salvage policy mismatch", arm)
	default:
		return nil
	}
}

func uniqueManifestTaskIDs(tasks []graphBatchPreregisteredTask) ([]string, error) {
	ids := make([]string, 0, len(tasks))
	seen := map[string]bool{}
	for _, task := range tasks {
		id := strings.TrimSpace(task.TaskID)
		if id == "" {
			return nil, fmt.Errorf("graph batch paired report: missing task in manifest")
		}
		if seen[id] {
			return nil, fmt.Errorf("graph batch paired report: duplicate task %q in manifest", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, nil
}

func indexGraphBatchObservations(arm string, taskIDs []string, observations []graphBatchTaskObservation) (map[string]graphBatchTaskObservation, error) {
	required := map[string]bool{}
	for _, id := range taskIDs {
		required[id] = true
	}
	byID := map[string]graphBatchTaskObservation{}
	for _, observation := range observations {
		id := strings.TrimSpace(observation.TaskID)
		if id == "" {
			return nil, fmt.Errorf("graph batch paired report: missing task in %s", arm)
		}
		if _, ok := byID[id]; ok {
			return nil, fmt.Errorf("graph batch paired report: duplicate task %q in %s", id, arm)
		}
		if !required[id] {
			return nil, fmt.Errorf("graph batch paired report: missing task %q in manifest", id)
		}
		byID[id] = observation
	}
	for _, id := range taskIDs {
		if _, ok := byID[id]; !ok {
			return nil, fmt.Errorf("graph batch paired report: missing task %q in %s", id, arm)
		}
	}
	return byID, nil
}

func validateGraphBatchGraderOutcome(arm, taskID, outcome string) error {
	switch outcome {
	case graphBatchOutcomePass, graphBatchOutcomeFail, graphBatchOutcomeTimeout, graphBatchOutcomeNoOutput, graphBatchOutcomeMissing, graphBatchOutcomeInfraFailure:
		return nil
	default:
		return fmt.Errorf("graph batch paired report: %s task %q has unknown grader outcome %q", arm, taskID, outcome)
	}
}

func graphBatchRate(passed, denom int) string {
	return fmt.Sprintf("%d/%d", passed, denom)
}

func graphBatchExactBodyDelivered(offer graphBatchOfferRecord) bool {
	return offer.DeliveryKind == graphBatchDeliveryExactDigest && graphBatchExactDigestRE.MatchString(offer.BodyDigest)
}

func aggregateGraphBatchMechanism(taskIDs []string, warm map[string]graphBatchTaskObservation) (graphBatchMechanismFunnel, []graphBatchReasonCount, []graphBatchServedDigest, []graphBatchVerdictRow, []graphBatchNamedCount, graphBatchAdaptationCounts) {
	funnel := graphBatchMechanismFunnel{}
	reasonCounts := map[string]int{}
	terminalCounts := map[string]int{}
	dispositionOutcomes := map[string]int{}
	served := make([]graphBatchServedDigest, 0)
	verdicts := make([]graphBatchVerdictRow, 0)
	adaptations := graphBatchAdaptationCounts{}

	for _, taskID := range taskIDs {
		obs := warm[taskID]
		funnel.GraphSeeds += obs.Mechanism.GraphSeeds
		funnel.GraphSteps += obs.Mechanism.GraphSteps
		funnel.Redirects += obs.Mechanism.Redirects
		terminal := obs.MemoryTerminal
		if terminal == "" {
			terminal = "unspecified"
		}
		terminalCounts[terminal]++
		switch terminal {
		case "no_candidate_from_empty_graph", "no_applicable_candidate":
			funnel.NormalNoSkill++
		case "graph_budget_exhausted", "offer_budget_exhausted":
			funnel.BudgetFailures++
		case "memory_timeout", "memory_error", "protocol_error", "resolution_error", "task_finished_before_delivery":
			funnel.InfraFailures++
		}
		for _, offer := range obs.Mechanism.Offers {
			funnel.Offers++
			if offer.DeliveryKind == graphBatchDeliveryRecallCitation {
				funnel.RecallCitationsIgnored++
			}
			if !graphBatchExactBodyDelivered(offer) {
				continue
			}
			funnel.ExactDigestServed++
			kind := offer.Kind
			if kind == "" {
				kind = graphBatchKindOriginal
			}
			switch kind {
			case graphBatchKindAdaptation:
				adaptations.AdaptationServed++
			default:
				kind = graphBatchKindOriginal
				adaptations.OriginalServed++
			}
			disposition := offer.Disposition
			if disposition == "" {
				disposition = graphBatchDispositionNoResponse
			}
			switch disposition {
			case graphBatchDispositionAccepted:
				funnel.Accepted++
			case graphBatchDispositionRejected:
				funnel.Rejected++
				if offer.ReasonCode != "" {
					reasonCounts[offer.ReasonCode]++
				}
			default:
				funnel.NoResponse++
				disposition = graphBatchDispositionNoResponse
			}
			if offer.Adopted {
				funnel.Adopted++
			}
			served = append(served, graphBatchServedDigest{
				TaskID:     taskID,
				OfferID:    offer.OfferID,
				BodyDigest: offer.BodyDigest,
				Kind:       kind,
			})
			verdicts = append(verdicts, graphBatchVerdictRow{
				TaskID:      taskID,
				OfferID:     offer.OfferID,
				BodyDigest:  offer.BodyDigest,
				Verdict:     offer.Verdict,
				Disposition: disposition,
				WarmOutcome: obs.GraderOutcome,
			})
			dispositionOutcomes[disposition+"\x1f"+obs.GraderOutcome]++
		}
	}

	sort.SliceStable(served, func(i, j int) bool {
		if served[i].TaskID != served[j].TaskID {
			return served[i].TaskID < served[j].TaskID
		}
		if served[i].OfferID != served[j].OfferID {
			return served[i].OfferID < served[j].OfferID
		}
		return served[i].BodyDigest < served[j].BodyDigest
	})
	sort.SliceStable(verdicts, func(i, j int) bool {
		if verdicts[i].TaskID != verdicts[j].TaskID {
			return verdicts[i].TaskID < verdicts[j].TaskID
		}
		if verdicts[i].OfferID != verdicts[j].OfferID {
			return verdicts[i].OfferID < verdicts[j].OfferID
		}
		return verdicts[i].BodyDigest < verdicts[j].BodyDigest
	})
	funnel.ExactDigestServedRate = graphBatchRate(funnel.ExactDigestServed, funnel.Offers)
	funnel.DispositionOutcomes = sortedDispositionOutcomes(dispositionOutcomes)
	return funnel, sortedReasonCounts(reasonCounts), served, verdicts, sortedNamedCounts(terminalCounts), adaptations
}

func sortedReasonCounts(counts map[string]int) []graphBatchReasonCount {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]graphBatchReasonCount, 0, len(keys))
	for _, key := range keys {
		out = append(out, graphBatchReasonCount{ReasonCode: key, Count: counts[key]})
	}
	return out
}

func sortedNamedCounts(counts map[string]int) []graphBatchNamedCount {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]graphBatchNamedCount, 0, len(keys))
	for _, key := range keys {
		out = append(out, graphBatchNamedCount{Name: key, Count: counts[key]})
	}
	return out
}

func sortedDispositionOutcomes(counts map[string]int) []graphBatchDispositionOutcome {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]graphBatchDispositionOutcome, 0, len(keys))
	for _, key := range keys {
		parts := strings.SplitN(key, "\x1f", 2)
		out = append(out, graphBatchDispositionOutcome{Disposition: parts[0], Outcome: parts[1], Count: counts[key]})
	}
	return out
}
