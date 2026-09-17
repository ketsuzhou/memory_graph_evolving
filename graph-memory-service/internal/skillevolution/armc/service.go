// Package armc implements server-owned Arm C evaluation. It resolves an exact
// versioned Validation Contract through a task-family manifest and evaluates
// pre-recorded paired fixture observations without executing candidate code.
package armc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/domain"
	"river2.dev/graph-memory-service/internal/skillevolution/policyactivation"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

const (
	SchemaValidationContract = "validation-contract.schema.json"
	SchemaTaskFamilyManifest = "task-family-manifest.schema.json"
)

type ValidationContractRef struct {
	ContractID string
	Version    int64
	Digest     string
}

type TaskFamilyManifestRef struct {
	ManifestID string
	Version    int64
	Digest     string
}

type ResourceLimits struct {
	MaxCommands       int
	MaxCPUUnits       int64
	MaxMemoryBytes    int64
	MaxDurationMillis int64
}

type ValidationContract struct {
	ContractID                 string
	Version                    int64
	Digest                     string
	TaskFamily                 string
	AllowedCommands            []string
	RequiredArtifactInvariants []string
	Limits                     ResourceLimits
	SuccessSignals             []string
	FailureSignals             []string
}

func (c ValidationContract) Ref() ValidationContractRef {
	return ValidationContractRef{ContractID: c.ContractID, Version: c.Version, Digest: c.Digest}
}

func ValidationContractDigest(c ValidationContract) string {
	doc := contractDocument(c)
	delete(doc, "contract_digest")
	digest, _ := contract.DigestOf(doc)
	return digest
}

type TaskFamilyManifest struct {
	ManifestID  string
	Version     int64
	Digest      string
	TaskFamily  string
	ContractRef ValidationContractRef
}

func (m TaskFamilyManifest) Ref() TaskFamilyManifestRef {
	return TaskFamilyManifestRef{ManifestID: m.ManifestID, Version: m.Version, Digest: m.Digest}
}

func TaskFamilyManifestDigest(m TaskFamilyManifest) string {
	payload := struct {
		SchemaVersion, ManifestID string
		Version                   int64
		TaskFamily                string
		ContractRef               ValidationContractRef
	}{"gms.task-family-manifest.v1", m.ManifestID, m.Version, m.TaskFamily, m.ContractRef}
	return digest(payload)
}

type CommandResult struct {
	Name     string
	ExitCode int
}

type FixtureRun struct {
	Commands           []CommandResult
	ArtifactInvariants map[string]bool
	SuccessSignals     []string
	FailureSignals     []string
	CPUUnits           int64
	MemoryBytes        int64
	DurationMillis     int64
}

type PairedFixture struct {
	FixtureID  string
	TaskFamily string
	Candidate  FixtureRun
	Baseline   FixtureRun
}

type EvaluationPlan struct {
	CandidateID           string
	DecisionID            string
	DecisionVersion       int64
	ManifestRef           TaskFamilyManifestRef
	FixtureIDs            []string
	PolicyRef             domain.PolicyArtifactRef
	ExpectedActiveVersion int64
	Coverage              domain.CoverageProof
}

type Store interface {
	PutValidationContract(context.Context, ValidationContract) (bool, error)
	ValidationContract(context.Context, ValidationContractRef) (ValidationContract, error)
	PutTaskFamilyManifest(context.Context, TaskFamilyManifest) (bool, error)
	TaskFamilyManifest(context.Context, TaskFamilyManifestRef) (TaskFamilyManifest, error)
	PutFixture(context.Context, PairedFixture) (bool, error)
	Fixture(context.Context, string) (PairedFixture, error)
	PutPlan(context.Context, EvaluationPlan) (bool, error)
	Plan(context.Context, string) (EvaluationPlan, error)
}

type CheckResult struct {
	CheckID         string
	CandidatePassed bool
	BaselinePassed  bool
	Reason          string
}

type Service struct {
	store Store
	gates *validation.Gates
}

func NewService(store Store, gates *validation.Gates) (*Service, error) {
	if store == nil || gates == nil {
		return nil, fmt.Errorf("armc: store and gates are required")
	}
	return &Service{store: store, gates: gates}, nil
}

func (s *Service) validateContractSchema(c ValidationContract) error {
	return s.gates.ValidateInstance(contractDocument(c), SchemaValidationContract)
}

func contractDocument(c ValidationContract) map[string]any {
	return map[string]any{"schema_version": "gms.validation-contract.v1", "contract_id": c.ContractID, "version": json.Number(strconv.FormatInt(c.Version, 10)), "contract_digest": c.Digest, "task_family": c.TaskFamily, "allowed_commands": stringsAny(c.AllowedCommands), "required_artifact_invariants": stringsAny(c.RequiredArtifactInvariants), "resource_limits": map[string]any{"max_commands": json.Number(strconv.Itoa(c.Limits.MaxCommands)), "max_cpu_units": json.Number(strconv.FormatInt(c.Limits.MaxCPUUnits, 10)), "max_memory_bytes": json.Number(strconv.FormatInt(c.Limits.MaxMemoryBytes, 10)), "max_duration_millis": json.Number(strconv.FormatInt(c.Limits.MaxDurationMillis, 10))}, "success_signals": stringsAny(c.SuccessSignals), "failure_signals": stringsAny(c.FailureSignals)}
}

func execute(contractArtifact ValidationContract, run FixtureRun) ([]CheckResult, bool) {
	checks := []CheckResult{}
	allowed := map[string]bool{}
	for _, command := range contractArtifact.AllowedCommands {
		allowed[command] = true
	}
	commandsPass := len(run.Commands) <= contractArtifact.Limits.MaxCommands
	for _, command := range run.Commands {
		if !allowed[command.Name] || command.ExitCode != 0 {
			commandsPass = false
		}
	}
	checks = append(checks, CheckResult{CheckID: "commands", CandidatePassed: commandsPass})
	invariantsPass := true
	for _, invariant := range contractArtifact.RequiredArtifactInvariants {
		if !run.ArtifactInvariants[invariant] {
			invariantsPass = false
		}
	}
	checks = append(checks, CheckResult{CheckID: "artifact_invariants", CandidatePassed: invariantsPass})
	resourcePass := run.CPUUnits <= contractArtifact.Limits.MaxCPUUnits && run.MemoryBytes <= contractArtifact.Limits.MaxMemoryBytes && run.DurationMillis <= contractArtifact.Limits.MaxDurationMillis
	checks = append(checks, CheckResult{CheckID: "resource_limits", CandidatePassed: resourcePass})
	signalsPass := containsAll(run.SuccessSignals, contractArtifact.SuccessSignals) && !containsAny(run.FailureSignals, contractArtifact.FailureSignals)
	checks = append(checks, CheckResult{CheckID: "signals", CandidatePassed: signalsPass})
	return checks, commandsPass && invariantsPass && resourcePass && signalsPass
}

func mergeChecks(fixture string, candidate, baseline []CheckResult) []CheckResult {
	out := make([]CheckResult, len(candidate))
	for i := range candidate {
		out[i] = CheckResult{CheckID: fixture + ":" + candidate[i].CheckID, CandidatePassed: candidate[i].CandidatePassed, BaselinePassed: baseline[i].CandidatePassed}
	}
	return out
}
func toDomainChecks(checks []CheckResult) []domain.ArmCCheckResult {
	out := make([]domain.ArmCCheckResult, len(checks))
	for i, c := range checks {
		out[i] = domain.ArmCCheckResult{CheckID: c.CheckID, CandidatePassed: c.CandidatePassed, BaselinePassed: c.BaselinePassed, Reason: c.Reason}
	}
	return out
}
func stringsAny(values []string) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}
func containsAll(values, required []string) bool {
	set := map[string]bool{}
	for _, v := range values {
		set[v] = true
	}
	for _, r := range required {
		if !set[r] {
			return false
		}
	}
	return true
}
func containsAny(values, unwanted []string) bool {
	set := map[string]bool{}
	for _, v := range values {
		set[v] = true
	}
	for _, u := range unwanted {
		if set[u] {
			return true
		}
	}
	return false
}
func sorted(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
func digest(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// EvaluateDirect is the CandidateView-native Arm C evaluator. It resolves the
// same immutable plan/manifest/contract/fixtures as Evaluate, but consumes the
// direct GMS-202 registration instead of domain.SkillCandidate.
func (s *Service) EvaluateDirect(ctx context.Context, request policyactivation.DirectEvaluationRequest) (policyactivation.EvaluationOutput, error) {
	candidate := request.Candidate
	plan, err := s.store.Plan(ctx, candidate.CandidateRef.CandidateID)
	if err != nil {
		return policyactivation.EvaluationOutput{}, err
	}
	manifest, err := s.store.TaskFamilyManifest(ctx, plan.ManifestRef)
	if err != nil {
		return s.rejectDirectOutput(candidate, plan, "task_family_manifest_ref_mismatch"), nil
	}
	if manifest.TaskFamily == "" || manifest.Digest != TaskFamilyManifestDigest(manifest) {
		return s.rejectDirectOutput(candidate, plan, "task_family_manifest_digest_mismatch"), nil
	}
	contractArtifact, err := s.store.ValidationContract(ctx, manifest.ContractRef)
	if err != nil || contractArtifact.Ref() != manifest.ContractRef {
		return s.rejectDirectOutput(candidate, plan, "validation_contract_ref_mismatch"), nil
	}
	if contractArtifact.Digest != ValidationContractDigest(contractArtifact) {
		return s.rejectDirectOutput(candidate, plan, "validation_contract_digest_mismatch"), nil
	}
	if err := s.validateContractSchema(contractArtifact); err != nil {
		return s.rejectDirectOutput(candidate, plan, "validation_contract_schema_invalid"), nil
	}
	if manifest.TaskFamily != contractArtifact.TaskFamily {
		return s.rejectDirectOutput(candidate, plan, "validation_contract_task_family_mismatch"), nil
	}
	checks := []CheckResult{}
	candidatePasses, baselinePasses := 0, 0
	for _, fixtureID := range plan.FixtureIDs {
		fixture, err := s.store.Fixture(ctx, fixtureID)
		if err != nil || fixture.TaskFamily != manifest.TaskFamily {
			return s.rejectDirectOutput(candidate, plan, "paired_fixture_missing"), nil
		}
		candidateChecks, candidatePass := execute(contractArtifact, fixture.Candidate)
		baselineChecks, baselinePass := execute(contractArtifact, fixture.Baseline)
		checks = append(checks, mergeChecks(fixtureID, candidateChecks, baselineChecks)...)
		if candidatePass {
			candidatePasses++
		}
		if baselinePass {
			baselinePasses++
		}
	}
	passed := len(plan.FixtureIDs) > 0 && candidatePasses > baselinePasses
	reason := ""
	if !passed {
		reason = "paired_candidate_not_better"
	}
	evaluation := domain.ArmCEvaluation{EvaluationID: "armc:" + plan.DecisionID, Version: 1, CandidateID: candidate.CandidateRef.CandidateID, CandidateDigest: candidate.CandidateRef.BodyDigest, Passed: passed, TaskFamily: manifest.TaskFamily, ContractRef: domain.VersionedArtifactRef{ID: contractArtifact.ContractID, Version: contractArtifact.Version, Digest: contractArtifact.Digest}, Checks: toDomainChecks(checks), Reason: reason}
	evaluation.Digest = domain.ArmCEvaluationDigest(evaluation)
	return policyactivation.EvaluationOutput{DecisionID: plan.DecisionID, DecisionVersion: plan.DecisionVersion, Evaluation: evaluation, Coverage: plan.Coverage, PolicyRef: plan.PolicyRef, ExpectedActiveVersion: candidate.ExpectedActiveVersion}, nil
}
func (s *Service) rejectDirectOutput(candidate domain.ArmCCandidateRegistration, plan EvaluationPlan, reason string) policyactivation.EvaluationOutput {
	evaluation := domain.ArmCEvaluation{EvaluationID: "armc:" + plan.DecisionID, Version: 1, CandidateID: candidate.CandidateRef.CandidateID, CandidateDigest: candidate.CandidateRef.BodyDigest, Passed: false, Reason: reason}
	evaluation.Digest = domain.ArmCEvaluationDigest(evaluation)
	return policyactivation.EvaluationOutput{DecisionID: plan.DecisionID, DecisionVersion: plan.DecisionVersion, Evaluation: evaluation, Coverage: plan.Coverage, PolicyRef: plan.PolicyRef, ExpectedActiveVersion: candidate.ExpectedActiveVersion}
}

// DirectEvaluator adapts this immutable-plan service to the CandidateView
// Arm C worker without exposing legacy domain.SkillCandidate.
func (s *Service) DirectEvaluator() policyactivation.DirectEvaluator {
	return directEvaluator{service: s}
}

type directEvaluator struct{ service *Service }

func (d directEvaluator) Evaluate(ctx context.Context, request policyactivation.DirectEvaluationRequest) (policyactivation.EvaluationOutput, error) {
	return d.service.EvaluateDirect(ctx, request)
}
