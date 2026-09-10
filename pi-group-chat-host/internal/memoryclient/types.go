package memoryclient

import "river2.dev/pi-group-chat-host/internal/ports"

type InitializeTenantRequest = ports.InitializeTenantRequest
type InitializeTenantResponse = ports.InitializeTenantResponse
type RegisterPrincipalRequest = ports.RegisterPrincipalRequest
type RegisterPrincipalResponse = ports.RegisterPrincipalResponse
type RegisterSpaceRequest = ports.RegisterSpaceRequest
type RegisterSpaceResponse = ports.RegisterSpaceResponse
type RegisterGrantRequest = ports.RegisterGrantRequest
type RegisterGrantResponse = ports.RegisterGrantResponse
type EvidenceProvenance = ports.EvidenceProvenance
type EvidenceEvent = ports.EvidenceEvent
type EvidenceLink = ports.EvidenceLink
type StageEvidenceBatchRequest = ports.StageEvidenceBatchRequest
type StageEvidenceBatchResponse = ports.StageEvidenceBatchResponse
type CommitEvidenceBatchRequest = ports.CommitEvidenceBatchRequest
type CommitEvidenceBatchResponse = ports.CommitEvidenceBatchResponse
type EvidenceBatchStatusResponse = ports.EvidenceBatchStatusResponse
type RecallRequest = ports.RecallRequest
type Citation = ports.Citation
type RecallItem = ports.RecallItem
type RecallDegradation = ports.RecallDegradation
type RecallResponse = ports.RecallResponse
type StartExplorationRequest = ports.StartExplorationRequest
type PinnedSpace = ports.PinnedSpace
type StartExplorationResponse = ports.StartExplorationResponse
type ExploreRequest = ports.ExploreRequest
type ExploreResponse = ports.ExploreResponse
type RedirectRequest = ports.RedirectRequest
type RedirectResponse = ports.RedirectResponse
type SubmitRequest = ports.SubmitRequest
type SubmittedCitation = ports.SubmittedCitation
type SubmitResponse = ports.SubmitResponse
