package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const SkillArtifactSchemaV2 = "skill-artifact/2.0"

type SkillArtifactKind string

const (
	SkillArtifactHumanProcedure SkillArtifactKind = "human_procedure"
	SkillArtifactStepGuidance   SkillArtifactKind = "step_guidance"
	SkillArtifactComposite      SkillArtifactKind = "composite"
	SkillArtifactTool           SkillArtifactKind = "tool"
)

var skillArtifactDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// SkillArtifactRef identifies one immutable canonical Skill artifact. Version
// is a monotonic lineage revision; Digest proves the exact canonical body.
type SkillArtifactRef struct {
	Kind          SkillArtifactKind `json:"kind"`
	SkillID       string            `json:"skill_id"`
	LineageID     string            `json:"lineage_id"`
	Version       int64             `json:"version"`
	SchemaVersion string            `json:"schema_version"`
	Digest        string            `json:"digest"`
}

// ImmutableArtifactRef identifies non-Skill evidence and profile artifacts.
type ImmutableArtifactRef struct {
	Kind          string `json:"kind"`
	ArtifactID    string `json:"artifact_id"`
	Version       int64  `json:"version"`
	SchemaVersion string `json:"schema_version"`
	Digest        string `json:"digest"`
}

type SkillArtifact struct {
	SchemaVersion        string             `json:"schema_version"`
	Kind                 SkillArtifactKind  `json:"kind"`
	SkillID              string             `json:"skill_id"`
	LineageID            string             `json:"lineage_id"`
	Version              int64              `json:"version"`
	Parents              []SkillArtifactRef `json:"parents,omitempty"`
	Name                 string             `json:"name"`
	Anchors              []SkillAnchorRef   `json:"anchors,omitempty"`
	Body                 SkillArtifactBody  `json:"body"`
	Dependencies         []SkillArtifactRef `json:"dependencies,omitempty"`
	RequiredCapabilities []string           `json:"required_capabilities,omitempty"`
}

type SkillAnchorRef struct {
	ConversationPathRef ImmutableArtifactRef `json:"conversation_path_ref"`
	CheckpointID        string               `json:"checkpoint_id"`
}

// SkillArtifactBody is a tagged union selected by SkillArtifact.Kind. Exactly
// one member must be present.
type SkillArtifactBody struct {
	HumanProcedure *HumanProcedureSkillBody `json:"human_procedure,omitempty"`
	StepGuidance   *StepGuidanceSkillBody   `json:"step_guidance,omitempty"`
	Composite      *CompositeSkillBody      `json:"composite,omitempty"`
	Tool           *ToolSkillBody           `json:"tool,omitempty"`
}

type ToolSkillBody struct {
	PackageRef            ToolPackageRef       `json:"package_ref"`
	InputSchemaRef        ImmutableArtifactRef `json:"input_schema_ref"`
	OutputSchemaRef       ImmutableArtifactRef `json:"output_schema_ref"`
	ErrorSchemaRef        ImmutableArtifactRef `json:"error_schema_ref"`
	ValidationContractRef ImmutableArtifactRef `json:"validation_contract_ref"`
	DeclaredCapabilities  []string             `json:"declared_capabilities"`
	NetworkDisabled       bool                 `json:"network_disabled"`
}

type ToolPackageRef struct {
	OCIImageDigest      string               `json:"oci_image_digest"`
	Entrypoint          []string             `json:"entrypoint"`
	BuildAttestationRef ImmutableArtifactRef `json:"build_attestation_ref"`
}

type HumanProcedureSkillBody struct {
	Instructions []string `json:"instructions"`
}

type StepGuidanceSkillBody struct {
	CausalContext CausalContext        `json:"causal_context"`
	Branches      []StepGuidanceBranch `json:"branches"`
}

type CausalContext struct {
	Facts []CausalFact `json:"facts"`
}

type CausalFact struct {
	FactID       string                 `json:"fact_id"`
	Statement    string                 `json:"statement"`
	EvidenceRefs []ImmutableArtifactRef `json:"evidence_refs,omitempty"`
}

// StepGuidanceBranch is one atomic decision path: an observable condition,
// the immediate action, and the future path caused by selecting that action.
type StepGuidanceBranch struct {
	BranchID string             `json:"branch_id"`
	When     ObservableGuard    `json:"when"`
	Action   StepGuidanceAction `json:"action"`
	Future   StepGuidanceFuture `json:"future"`
}

type ObservableGuard struct {
	Predicate   json.RawMessage `json:"predicate"`
	Explanation string          `json:"explanation"`
}

type StepGuidanceAction struct {
	Instructions []GuidanceAction `json:"instructions"`
	Constraints  []string         `json:"constraints,omitempty"`
	Rationale    string           `json:"rationale"`
}

type GuidanceAction struct {
	Instruction string `json:"instruction"`
}

type StepGuidanceFuture struct {
	Disposition         FuturePathDisposition   `json:"disposition"`
	CriticalSteps       []FutureCriticalStep    `json:"critical_steps"`
	OutcomeDistribution []OutcomeObservation    `json:"outcome_distribution,omitempty"`
	EvidenceSupport     []BranchEvidenceSupport `json:"evidence_support,omitempty"`
}

type FuturePathDisposition string

const (
	FuturePathSuccess     FuturePathDisposition = "success"
	FuturePathFailureRisk FuturePathDisposition = "failure_risk"
)

type FutureCriticalStep struct {
	StepID      string `json:"step_id"`
	Instruction string `json:"instruction"`
	Horizon     string `json:"horizon,omitempty"`
}

type OutcomeObservation struct {
	Outcome      string `json:"outcome"`
	Observations int    `json:"observations"`
}

type BranchEvidenceRelation string

const (
	BranchEvidenceSupports BranchEvidenceRelation = "supports"
	BranchEvidenceRefutes  BranchEvidenceRelation = "refutes"
)

type BranchCoverage string

const (
	BranchCoverageExercised    BranchCoverage = "exercised"
	BranchCoverageAvoided      BranchCoverage = "avoided"
	BranchCoverageRecovered    BranchCoverage = "recovered"
	BranchCoverageInapplicable BranchCoverage = "inapplicable"
)

type BranchEvidenceSupport struct {
	EvidenceRef ImmutableArtifactRef   `json:"evidence_ref"`
	Relation    BranchEvidenceRelation `json:"relation"`
	Coverage    BranchCoverage         `json:"coverage"`
	Claims      []string               `json:"claims"`
}

// CompositeSkillBody references exact child artifacts and adds orchestration
// only. Child guidance is never copied into the composite.
type CompositeSkillBody struct {
	Children               []SkillArtifactRef         `json:"children"`
	ControlFlow            []CompositeSkillStep       `json:"control_flow"`
	DataFlow               []CompositeDataFlow        `json:"data_flow,omitempty"`
	PermissionRequirements []string                   `json:"permission_requirements,omitempty"`
	FailureHandling        []CompositeFailureHandling `json:"failure_handling,omitempty"`
}

type CompositeSkillStep struct {
	StepID    string           `json:"step_id"`
	SkillRef  SkillArtifactRef `json:"skill_ref"`
	DependsOn []string         `json:"depends_on,omitempty"`
}

type CompositeDataFlow struct {
	FromStep string            `json:"from_step"`
	ToStep   string            `json:"to_step"`
	Mapping  map[string]string `json:"mapping,omitempty"`
}

type CompositeFailureHandling struct {
	StepID    string `json:"step_id"`
	OnFailure string `json:"on_failure"`
	Action    string `json:"action"`
}

func ValidateSkillArtifactRef(ref SkillArtifactRef) error {
	if ref.Kind != SkillArtifactHumanProcedure && ref.Kind != SkillArtifactStepGuidance && ref.Kind != SkillArtifactComposite && ref.Kind != SkillArtifactTool {
		return fmt.Errorf("skill artifact ref: unsupported kind %q", ref.Kind)
	}
	if strings.TrimSpace(ref.SkillID) == "" || strings.TrimSpace(ref.LineageID) == "" {
		return fmt.Errorf("skill artifact ref: skill_id and lineage_id are required")
	}
	if ref.Version < 1 {
		return fmt.Errorf("skill artifact ref: version must be at least 1")
	}
	if ref.SchemaVersion != SkillArtifactSchemaV2 {
		return fmt.Errorf("skill artifact ref: unsupported schema_version %q", ref.SchemaVersion)
	}
	if !skillArtifactDigestPattern.MatchString(ref.Digest) {
		return fmt.Errorf("skill artifact ref: digest must be sha256:<64 lowercase hex>")
	}
	return nil
}

func ValidateSkillArtifact(artifact SkillArtifact) error {
	if artifact.SchemaVersion != SkillArtifactSchemaV2 {
		return fmt.Errorf("skill artifact: unsupported schema_version %q", artifact.SchemaVersion)
	}
	if strings.TrimSpace(artifact.SkillID) == "" || strings.TrimSpace(artifact.LineageID) == "" || strings.TrimSpace(artifact.Name) == "" {
		return fmt.Errorf("skill artifact: skill_id, lineage_id, and name are required")
	}
	if artifact.Version < 1 {
		return fmt.Errorf("skill artifact: version must be at least 1")
	}
	for i, ref := range artifact.Parents {
		if err := ValidateSkillArtifactRef(ref); err != nil {
			return fmt.Errorf("skill artifact parent %d: %w", i, err)
		}
	}
	for i, ref := range artifact.Dependencies {
		if err := ValidateSkillArtifactRef(ref); err != nil {
			return fmt.Errorf("skill artifact dependency %d: %w", i, err)
		}
	}
	if err := validateUniqueNonempty(artifact.RequiredCapabilities, "required capability"); err != nil {
		return err
	}
	for i, anchor := range artifact.Anchors {
		if strings.TrimSpace(anchor.CheckpointID) == "" {
			return fmt.Errorf("skill artifact anchor %d: checkpoint_id is required", i)
		}
		if err := validateImmutableArtifactRef(anchor.ConversationPathRef); err != nil {
			return fmt.Errorf("skill artifact anchor %d: %w", i, err)
		}
	}

	present := 0
	if artifact.Body.HumanProcedure != nil {
		present++
	}
	if artifact.Body.StepGuidance != nil {
		present++
	}
	if artifact.Body.Composite != nil {
		present++
	}
	if artifact.Body.Tool != nil {
		present++
	}
	if present != 1 {
		return fmt.Errorf("skill artifact: body must contain exactly one kind")
	}

	switch artifact.Kind {
	case SkillArtifactHumanProcedure:
		if artifact.Body.HumanProcedure == nil {
			return fmt.Errorf("skill artifact: procedure kind requires procedure body")
		}
		return validateHumanProcedure(*artifact.Body.HumanProcedure)
	case SkillArtifactStepGuidance:
		if artifact.Body.StepGuidance == nil {
			return fmt.Errorf("skill artifact: step_guidance kind requires step_guidance body")
		}
		return validateStepGuidance(*artifact.Body.StepGuidance)
	case SkillArtifactComposite:
		if artifact.Body.Composite == nil {
			return fmt.Errorf("skill artifact: composite kind requires composite body")
		}
		return validateComposite(artifact, *artifact.Body.Composite)
	case SkillArtifactTool:
		if artifact.Body.Tool == nil {
			return fmt.Errorf("skill artifact: tool kind requires tool body")
		}
		return validateTool(*artifact.Body.Tool)
	default:
		return fmt.Errorf("skill artifact: unsupported kind %q", artifact.Kind)
	}
}

// SkillArtifactDigest computes the content address of a validated canonical
// artifact. Digest is deliberately outside SkillArtifact to avoid self-hash.
func SkillArtifactDigest(artifact SkillArtifact) (string, error) {
	if err := ValidateSkillArtifact(artifact); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		return "", fmt.Errorf("skill artifact: encode canonical body: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func SkillArtifactExactRef(artifact SkillArtifact) (SkillArtifactRef, error) {
	digest, err := SkillArtifactDigest(artifact)
	if err != nil {
		return SkillArtifactRef{}, err
	}
	return SkillArtifactRef{
		Kind: artifact.Kind, SkillID: artifact.SkillID, LineageID: artifact.LineageID,
		Version: artifact.Version, SchemaVersion: artifact.SchemaVersion, Digest: digest,
	}, nil
}

func validateHumanProcedure(body HumanProcedureSkillBody) error {
	if len(body.Instructions) == 0 {
		return fmt.Errorf("procedure skill: at least one instruction is required")
	}
	return validateUniqueNonempty(body.Instructions, "procedure instruction")
}

func validateStepGuidance(body StepGuidanceSkillBody) error {
	factIDs := map[string]bool{}
	for i, fact := range body.CausalContext.Facts {
		if err := uniqueID(factIDs, fact.FactID, "causal fact"); err != nil {
			return err
		}
		if strings.TrimSpace(fact.Statement) == "" {
			return fmt.Errorf("causal fact %d: statement is required", i)
		}
		for j, ref := range fact.EvidenceRefs {
			if err := validateImmutableArtifactRef(ref); err != nil {
				return fmt.Errorf("causal fact %s evidence %d: %w", fact.FactID, j, err)
			}
		}
	}
	if len(body.Branches) == 0 {
		return fmt.Errorf("step guidance: at least one condition-action-future branch is required")
	}
	branchIDs := map[string]bool{}
	for _, branch := range body.Branches {
		if err := uniqueID(branchIDs, branch.BranchID, "step guidance branch"); err != nil {
			return err
		}
		if err := validateGuard(branch.When, "step guidance branch "+branch.BranchID); err != nil {
			return err
		}
		if len(branch.Action.Instructions) == 0 || strings.TrimSpace(branch.Action.Rationale) == "" {
			return fmt.Errorf("step guidance branch %s: action instructions and rationale are required", branch.BranchID)
		}
		for _, action := range branch.Action.Instructions {
			if strings.TrimSpace(action.Instruction) == "" {
				return fmt.Errorf("step guidance branch %s: action instruction is required", branch.BranchID)
			}
		}
		if branch.Future.Disposition != FuturePathSuccess && branch.Future.Disposition != FuturePathFailureRisk {
			return fmt.Errorf("step guidance branch %s: unsupported future disposition %q", branch.BranchID, branch.Future.Disposition)
		}
		if len(branch.Future.CriticalSteps) == 0 {
			return fmt.Errorf("step guidance branch %s: future requires at least one critical step", branch.BranchID)
		}
		stepIDs := map[string]bool{}
		for _, step := range branch.Future.CriticalSteps {
			if err := uniqueID(stepIDs, step.StepID, "future critical step"); err != nil {
				return err
			}
			if strings.TrimSpace(step.Instruction) == "" {
				return fmt.Errorf("future critical step %s: instruction is required", step.StepID)
			}
		}
		for _, evidence := range branch.Future.EvidenceSupport {
			if err := validateImmutableArtifactRef(evidence.EvidenceRef); err != nil {
				return fmt.Errorf("step guidance branch %s evidence: %w", branch.BranchID, err)
			}
			if evidence.Relation != BranchEvidenceSupports && evidence.Relation != BranchEvidenceRefutes {
				return fmt.Errorf("step guidance branch %s: unsupported evidence relation %q", branch.BranchID, evidence.Relation)
			}
			switch evidence.Coverage {
			case BranchCoverageExercised, BranchCoverageAvoided, BranchCoverageRecovered, BranchCoverageInapplicable:
			default:
				return fmt.Errorf("step guidance branch %s: unsupported coverage %q", branch.BranchID, evidence.Coverage)
			}
			if len(evidence.Claims) == 0 {
				return fmt.Errorf("step guidance branch %s: evidence claims are required", branch.BranchID)
			}
		}
	}
	return nil
}

func validateComposite(artifact SkillArtifact, body CompositeSkillBody) error {
	if len(body.Children) == 0 || len(body.ControlFlow) == 0 {
		return fmt.Errorf("composite skill: children and control_flow are required")
	}
	children := map[string]SkillArtifactRef{}
	for i, child := range body.Children {
		if err := ValidateSkillArtifactRef(child); err != nil {
			return fmt.Errorf("composite skill child %d: %w", i, err)
		}
		if child.SkillID == artifact.SkillID && child.LineageID == artifact.LineageID {
			return fmt.Errorf("composite skill: direct self-reference is forbidden")
		}
		key := skillRefKey(child)
		if _, duplicate := children[key]; duplicate {
			return fmt.Errorf("composite skill: duplicate child ref %s", key)
		}
		children[key] = child
	}
	steps := map[string]CompositeSkillStep{}
	for _, step := range body.ControlFlow {
		if strings.TrimSpace(step.StepID) == "" {
			return fmt.Errorf("composite skill: control-flow step_id is required")
		}
		if _, duplicate := steps[step.StepID]; duplicate {
			return fmt.Errorf("composite skill: duplicate control-flow step %s", step.StepID)
		}
		if err := ValidateSkillArtifactRef(step.SkillRef); err != nil {
			return fmt.Errorf("composite skill step %s: %w", step.StepID, err)
		}
		if _, ok := children[skillRefKey(step.SkillRef)]; !ok {
			return fmt.Errorf("composite skill step %s: skill_ref is not declared in children", step.StepID)
		}
		steps[step.StepID] = step
	}
	for _, step := range body.ControlFlow {
		for _, dependency := range step.DependsOn {
			if _, ok := steps[dependency]; !ok {
				return fmt.Errorf("composite skill step %s: unknown dependency %s", step.StepID, dependency)
			}
			if dependency == step.StepID {
				return fmt.Errorf("composite skill step %s: self dependency is forbidden", step.StepID)
			}
		}
	}
	if err := validateCompositeAcyclic(steps); err != nil {
		return err
	}
	for _, flow := range body.DataFlow {
		if _, ok := steps[flow.FromStep]; !ok {
			return fmt.Errorf("composite skill data flow: unknown from_step %s", flow.FromStep)
		}
		if _, ok := steps[flow.ToStep]; !ok {
			return fmt.Errorf("composite skill data flow: unknown to_step %s", flow.ToStep)
		}
	}
	for _, handling := range body.FailureHandling {
		if _, ok := steps[handling.StepID]; !ok {
			return fmt.Errorf("composite skill failure handling: unknown step %s", handling.StepID)
		}
		if strings.TrimSpace(handling.OnFailure) == "" || strings.TrimSpace(handling.Action) == "" {
			return fmt.Errorf("composite skill failure handling for %s: on_failure and action are required", handling.StepID)
		}
	}
	return validateUniqueNonempty(body.PermissionRequirements, "composite permission requirement")
}

func validateCompositeAcyclic(steps map[string]CompositeSkillStep) error {
	state := map[string]uint8{}
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return fmt.Errorf("composite skill: control_flow contains a cycle at %s", id)
		case 2:
			return nil
		}
		state[id] = 1
		dependencies := append([]string(nil), steps[id].DependsOn...)
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	ids := make([]string, 0, len(steps))
	for id := range steps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func validateGuard(guard ObservableGuard, label string) error {
	if len(guard.Predicate) == 0 || !json.Valid(guard.Predicate) {
		return fmt.Errorf("%s: predicate must be valid structured JSON", label)
	}
	if strings.TrimSpace(guard.Explanation) == "" {
		return fmt.Errorf("%s: explanation is required", label)
	}
	return nil
}

func validateImmutableArtifactRef(ref ImmutableArtifactRef) error {
	if strings.TrimSpace(ref.Kind) == "" || strings.TrimSpace(ref.ArtifactID) == "" || strings.TrimSpace(ref.SchemaVersion) == "" {
		return fmt.Errorf("artifact ref: kind, artifact_id, and schema_version are required")
	}
	if ref.Version < 1 {
		return fmt.Errorf("artifact ref: version must be at least 1")
	}
	if !skillArtifactDigestPattern.MatchString(ref.Digest) {
		return fmt.Errorf("artifact ref: digest must be sha256:<64 lowercase hex>")
	}
	return nil
}

func validateUniqueNonempty(values []string, label string) error {
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("%s must be non-empty", label)
		}
		if seen[value] {
			return fmt.Errorf("duplicate %s %q", label, value)
		}
		seen[value] = true
	}
	return nil
}

func uniqueID(seen map[string]bool, id, label string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("%s id is required", label)
	}
	if seen[id] {
		return fmt.Errorf("duplicate %s id %q", label, id)
	}
	seen[id] = true
	return nil
}

func skillRefKey(ref SkillArtifactRef) string {
	return fmt.Sprintf("%s@%d#%s", ref.LineageID, ref.Version, ref.Digest)
}

// SkillArtifactDigestHex accepts the canonical sha256: form and returns the
// legacy hex field representation used by ReviewedDiff.
func SkillArtifactDigestHex(ref SkillArtifactRef) string {
	return strings.TrimPrefix(ref.Digest, "sha256:")
}

// ValidateSkillCandidateArtifactRefs preserves legacy hash-only candidates but
// makes exact refs all-or-nothing for new candidates. A new Skill may omit a
// base ref only when its base version is zero.
func ValidateSkillCandidateArtifactRefs(candidate SkillCandidate) error {
	base := candidate.BaseArtifactRef
	next := candidate.CandidateArtifactRef
	if base == nil && next == nil {
		return nil
	}
	if next == nil {
		return fmt.Errorf("skill candidate: candidate_artifact_ref is required when exact refs are used")
	}
	if err := ValidateSkillArtifactRef(*next); err != nil {
		return fmt.Errorf("skill candidate: %w", err)
	}
	if next.SkillID != candidate.TargetSkillID {
		return fmt.Errorf("skill candidate: candidate artifact skill_id does not match target_skill_id")
	}
	if next.Version != candidate.BaseArtifactVersion+1 {
		return fmt.Errorf("skill candidate: candidate artifact version must equal base_artifact_version + 1")
	}
	if candidate.Diff.CandidateArtifactHash != "" && candidate.Diff.CandidateArtifactHash != next.Digest && candidate.Diff.CandidateArtifactHash != SkillArtifactDigestHex(*next) {
		return fmt.Errorf("skill candidate: candidate artifact digest does not match reviewed diff")
	}
	if base == nil {
		if candidate.BaseArtifactVersion != 0 {
			return fmt.Errorf("skill candidate: base_artifact_ref is required for a nonzero base version")
		}
		return nil
	}
	if err := ValidateSkillArtifactRef(*base); err != nil {
		return fmt.Errorf("skill candidate base: %w", err)
	}
	if base.SkillID != candidate.TargetSkillID || base.LineageID != next.LineageID {
		return fmt.Errorf("skill candidate: base and candidate refs must identify the target Skill lineage")
	}
	if base.Version != candidate.BaseArtifactVersion {
		return fmt.Errorf("skill candidate: base artifact ref version does not match base_artifact_version")
	}
	if candidate.Diff.BaseArtifactHash != "" && candidate.Diff.BaseArtifactHash != base.Digest && candidate.Diff.BaseArtifactHash != SkillArtifactDigestHex(*base) {
		return fmt.Errorf("skill candidate: base artifact digest does not match reviewed diff")
	}
	return nil
}

func validateTool(body ToolSkillBody) error {
	if !skillArtifactDigestPattern.MatchString(body.PackageRef.OCIImageDigest) || len(body.PackageRef.Entrypoint) == 0 {
		return fmt.Errorf("tool skill: OCI package digest and entrypoint are required")
	}
	for _, entry := range body.PackageRef.Entrypoint {
		if strings.TrimSpace(entry) == "" {
			return fmt.Errorf("tool skill: entrypoint contains an empty argument")
		}
	}
	for _, ref := range []ImmutableArtifactRef{body.PackageRef.BuildAttestationRef, body.InputSchemaRef, body.OutputSchemaRef, body.ErrorSchemaRef, body.ValidationContractRef} {
		if err := validateImmutableArtifactRef(ref); err != nil {
			return fmt.Errorf("tool skill: immutable contract reference: %w", err)
		}
	}
	if !body.NetworkDisabled {
		return fmt.Errorf("tool skill: network must be disabled")
	}
	return validateUniqueNonempty(body.DeclaredCapabilities, "tool declared capability")
}
