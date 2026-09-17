// Package policyactivation creates immutable Arm C policy decisions and the
// final activation result from direct GMS-202 candidate registrations.
package policyactivation

import "river2.dev/graph-memory-service/internal/domain"

// EvaluationOutput is the server-owned Arm C evaluator output. It carries no
// caller-supplied activation value and is independently revalidated by the
// direct worker before any activation CAS.
type EvaluationOutput struct {
	DecisionID            string
	DecisionVersion       int64
	Evaluation            domain.ArmCEvaluation
	Coverage              domain.CoverageProof
	PolicyRef             domain.PolicyArtifactRef
	ExpectedActiveVersion int64
}

// Job identifies one server-side Arm C completion flow by its immutable
// direct candidate registration key.
type Job struct {
	TenantID    domain.TenantID
	SpaceID     domain.SpaceID
	CandidateID string
}

// Result is the queryable outcome of one Arm C flow. Rejection remains an
// immutable policy decision and never implies activation or runtime execution.
type Result struct {
	DecisionRef     domain.ActivationPolicyDecisionRef
	Activated       bool
	Rejected        bool
	RejectionReason string
	Activation      *domain.SkillActivation
}
