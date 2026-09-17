package usageprojection

import "context"

const UsageProjectionSnapshotSchemaV1 = "usage-projection-store/1.0"

// UsageProjectionSnapshot is the durable observation boundary. It contains
// only the privacy-safe records already accepted by the UsageProjectionService
// and never carries normative graph or activation state.
type UsageProjectionSnapshot struct {
	SchemaVersion string                       `json:"schema_version"`
	Interactions  []Interaction                `json:"interactions"`
	Assessments   []DiagnosisUtilityAssessment `json:"assessments"`
}

// UsageProjectionStore is the durable read/write seam behind the observational
// projection. Implementations must preserve append order and provide one
// internally consistent read snapshot.
type UsageProjectionStore interface {
	AppendInteraction(context.Context, Interaction) error
	AppendDiagnosis(context.Context, DiagnosisUtilityAssessment) error
	Read(context.Context) (UsageProjectionSnapshot, error)
}
