package diagnosis

import (
	"context"
	"errors"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/domain"
)

// gateFunc adapts a function to the DisclosureGate interface; the fixtures
// below are the deterministic disclosure gates under test.
type gateFunc func(context.Context, DisclosureRequest) (DisclosureDecision, error)

func (f gateFunc) Decide(ctx context.Context, req DisclosureRequest) (DisclosureDecision, error) {
	return f(ctx, req)
}

// partitionGate is the deterministic contract gate: it permits same-scope
// disclosure and partitioned private→shared disclosure, and denies any
// narrowing that would move shared content into a private domain.
func partitionGate() DisclosureGate {
	return gateFunc(func(_ context.Context, req DisclosureRequest) (DisclosureDecision, error) {
		switch {
		case req.TargetScope == req.SourceScope,
			req.SourceScope == domain.SpacePrivate && req.TargetScope == domain.SpaceShared:
			return DisclosureDecision{Allowed: true, ReasonCode: "DISCLOSURE_PARTITIONED", Labels: append([]DisclosureLabel(nil), req.Annotation.DisclosureLabels...)}, nil
		default:
			return DisclosureDecision{Allowed: false, ReasonCode: "DISCLOSURE_DENIED_SCOPE_NARROWING"}, nil
		}
	})
}

// seedDisclosureAnnotation stores the immutable annotation revision that
// disclosure requests resolve against: disclosure never trusts caller-carried
// annotation content.
func seedDisclosureAnnotation(t *testing.T, service *Service) Annotation {
	t.Helper()
	callID := "call-1"
	annotation := Annotation{
		AnnotationID: "annotation-private", Revision: 1, CutID: "cut-1", TenantID: "tenant-1",
		Target: AnnotationTarget{SegmentID: "segment-1", CallID: &callID}, Status: StatusVerified,
		DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest,
		EvidenceCitations: []EvidenceCitation{{BatchID: "batch-private", EventIDs: []string{"event-private"}}},
		DisclosureLabels:  []DisclosureLabel{"private"},
	}
	if _, _, err := service.AppendAnnotation(context.Background(), annotation); err != nil {
		t.Fatalf("seeding disclosure annotation failed: %v", err)
	}
	return annotation
}

func disclosureRequest(source, target domain.SpaceScope) DisclosureRequest {
	return DisclosureRequest{
		Annotation: Annotation{
			AnnotationID: "annotation-private", Revision: 1, CutID: "cut-1", TenantID: "tenant-1",
			Status: StatusVerified, DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest,
		},
		SourceScope: source, TargetScope: target,
		Rationale: "private plaintext must not cross the disclosure gate",
	}
}

func TestDisclosureFailsClosedWithoutGate(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil, nil)
	seedDisclosureAnnotation(t, service)
	if _, err := service.Disclose(context.Background(), disclosureRequest(domain.SpacePrivate, domain.SpaceShared)); !errors.Is(err, ErrDisclosureDenied) {
		t.Fatalf("disclosure without a configured gate did not fail closed: %v", err)
	}
}

func TestDisclosurePartitionsRationaleAndCitationsFailClosed(t *testing.T) {
	t.Parallel()
	service := NewService(nil, partitionGate(), nil)
	stored := seedDisclosureAnnotation(t, service)
	output, err := service.Disclose(context.Background(), disclosureRequest(domain.SpacePrivate, domain.SpaceShared))
	requireDiagnosisImplemented(t, err)
	if !output.Decision.Allowed || output.PrivateRationale != "" || output.PublicRationale == "private plaintext must not cross the disclosure gate" {
		t.Fatalf("disclosure did not partition private rationale or fail closed: %#v", output)
	}
	// Citations come from the stored revision, not the caller's request.
	if len(output.Citations) != 1 || output.Citations[0].BatchID != stored.EvidenceCitations[0].BatchID || len(output.Citations[0].EventIDs) == 0 {
		t.Fatalf("disclosure dropped stored ID/digest citation provenance: %#v", output.Citations)
	}

	// An annotation reference that was never stored cannot be disclosed.
	unknown := disclosureRequest(domain.SpacePrivate, domain.SpaceShared)
	unknown.Annotation.AnnotationID = "annotation-never-stored"
	if _, err := service.Disclose(context.Background(), unknown); !errors.Is(err, ErrAnnotationUnknown) {
		t.Fatalf("disclosure of an unstored annotation was accepted: %v", err)
	}

	denied := NewService(nil, gateFunc(func(_ context.Context, _ DisclosureRequest) (DisclosureDecision, error) {
		return DisclosureDecision{Allowed: false, ReasonCode: "DISCLOSURE_DENIED_BY_POLICY"}, nil
	}), nil)
	seedDisclosureAnnotation(t, denied)
	_, err = denied.Disclose(context.Background(), disclosureRequest(domain.SpacePrivate, domain.SpaceShared))
	if !errors.Is(err, ErrDisclosureDenied) || !strings.Contains(err.Error(), "DISCLOSURE_DENIED_BY_POLICY") {
		t.Fatalf("deny-gate decision was not surfaced fail-closed: %v", err)
	}
}

func TestPrivateReadsAndDisclosureAppendVerifiableAuditChain(t *testing.T) {
	t.Parallel()
	service, _ := newAuthorityService(t, partitionGate())
	capability, _, err := service.IssueCapability(context.Background(), diagnosisCapability())
	requireDiagnosisImplemented(t, err)
	read, err := service.ReadPrivate(context.Background(), PrivateReadRequest{
		Capability: capability, SpaceID: "private-a", BatchID: "batch-private", EventIDs: []string{"event-private"}, ReadAt: capability.IssuedAt,
	})
	requireDiagnosisImplemented(t, err)
	if read.PreviousDigest == "" || read.Digest == "" || capability.ReadAuditChainDigest == "" {
		t.Fatalf("private read did not append an auditable digest chain link: read=%#v capability=%#v", read, capability)
	}
	seedDisclosureAnnotation(t, service)
	output, err := service.Disclose(context.Background(), DisclosureRequest{
		Annotation:  Annotation{AnnotationID: "annotation-private", Revision: 1, CutID: capability.CutID, TenantID: capability.TenantID, Status: StatusVerified, DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest},
		SourceScope: domain.SpacePrivate, TargetScope: domain.SpacePrivate, Rationale: "authorized private rationale",
	})
	requireDiagnosisImplemented(t, err)
	if output.AuditDigest == "" {
		t.Fatalf("disclosure did not append an audit-chain record: %#v", output)
	}
}
