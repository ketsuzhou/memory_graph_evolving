package diagnosis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"river2.dev/graph-memory-service/internal/domain"
)

var recipeDigest = func() string {
	sum := sha256.Sum256([]byte("execution-recipe-v1"))
	return "sha256:" + hex.EncodeToString(sum[:])
}()

func diagnosisCapability() Capability {
	agentRun := AgentRunID("diagnosis-run-1")
	return Capability{
		CapabilityID: "capability-1", TenantID: "tenant-1", CutID: "cut-1", PrincipalID: "diagnosis-agent",
		AgentRunID: &agentRun,
		BoundPrivateSpaces: []PrivateSpaceBinding{
			{SpaceID: "private-a", RoomID: "room-1", RoomEpoch: 7},
			{SpaceID: "private-b", RoomID: "room-1", RoomEpoch: 7},
		},
		Operations:           []CapabilityOperation{CapabilityOperationRead},
		IssuedAt:             time.Date(2026, time.September, 14, 10, 0, 0, 0, time.UTC),
		ExpiresAt:            time.Date(2026, time.September, 14, 11, 0, 0, 0, time.UTC),
		ReadAuditChainDigest: "sha256:audit-chain",
	}
}

// newAuthorityService builds a service with a caller-controlled authority
// clock and the frozen cut scope the fixture capability is bound to.
func newAuthorityService(t *testing.T, gate DisclosureGate) (*Service, func(time.Time)) {
	t.Helper()
	service := NewService(nil, gate, nil)
	current := diagnosisCapability().IssuedAt
	service.now = func() time.Time { return current }
	if err := service.RegisterCutScope("tenant-1", "cut-1", diagnosisCapability().BoundPrivateSpaces); err != nil {
		t.Fatalf("registering frozen cut scope failed: %v", err)
	}
	return service, func(at time.Time) { current = at }
}

func requireDiagnosisImplemented(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ErrDiagnosisNotImplemented) {
		t.Fatalf("PG-22 Diagnosis contract is red: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected diagnosis error: %v", err)
	}
}

func TestCapabilityReadsExactlyFrozenPrivateSpacesReadOnlyBeforeExpiry(t *testing.T) {
	t.Parallel()
	service, setClock := newAuthorityService(t, nil)
	capability, _, err := service.IssueCapability(context.Background(), diagnosisCapability())
	requireDiagnosisImplemented(t, err)
	if !reflect.DeepEqual(capability.BoundPrivateSpaces, diagnosisCapability().BoundPrivateSpaces) || !reflect.DeepEqual(capability.Operations, []CapabilityOperation{CapabilityOperationRead}) {
		t.Fatalf("capability did not preserve exact frozen private scope/read-only operation: %#v", capability)
	}
	for _, spaceID := range []domain.SpaceID{"private-a", "private-b"} {
		_, err := service.ReadPrivate(context.Background(), PrivateReadRequest{Capability: capability, SpaceID: spaceID, BatchID: "batch-1", EventIDs: []string{"event-1"}, ReadAt: capability.IssuedAt.Add(time.Minute)})
		requireDiagnosisImplemented(t, err)
	}

	// The scope must be exact on issue: extra, missing, or altered bindings
	// violate capability_scope_within_cut even when self-consistent.
	altered := diagnosisCapability()
	altered.BoundPrivateSpaces = append(append([]PrivateSpaceBinding(nil), altered.BoundPrivateSpaces...), PrivateSpaceBinding{SpaceID: "private-c", RoomID: "room-1", RoomEpoch: 7})
	if _, _, err := service.IssueCapability(context.Background(), altered); !errors.Is(err, ErrCapabilityScopeViolation) {
		t.Fatalf("capability declaring an extra private space was accepted: %v", err)
	}
	narrowed := diagnosisCapability()
	narrowed.BoundPrivateSpaces = narrowed.BoundPrivateSpaces[:1]
	if _, _, err := service.IssueCapability(context.Background(), narrowed); !errors.Is(err, ErrCapabilityScopeViolation) {
		t.Fatalf("capability dropping a frozen private space was accepted: %v", err)
	}
	unchallenged := diagnosisCapability()
	unchallenged.BoundPrivateSpaces[0].RoomEpoch = 8
	if _, _, err := service.IssueCapability(context.Background(), unchallenged); !errors.Is(err, ErrCapabilityScopeViolation) {
		t.Fatalf("capability with a stale room epoch was accepted: %v", err)
	}

	// Expiry is judged by the authority clock: after it passes, a caller
	// replaying a stale pre-expiry event time must still be rejected.
	setClock(capability.ExpiresAt)
	forged := PrivateReadRequest{Capability: capability, SpaceID: "private-a", BatchID: "batch-1", EventIDs: []string{"event-1"}, ReadAt: capability.IssuedAt.Add(time.Minute)}
	if _, err := service.ReadPrivate(context.Background(), forged); !errors.Is(err, ErrCapabilityExpired) {
		t.Fatalf("forged pre-expiry event time resurrected an expired capability: %v", err)
	}
	outOfScope := PrivateReadRequest{Capability: capability, SpaceID: "private-outside-manifest", BatchID: "batch-2", ReadAt: capability.IssuedAt.Add(time.Minute)}
	if _, err := service.ReadPrivate(context.Background(), outOfScope); err == nil {
		t.Fatalf("out-of-scope private read was accepted: %#v", outOfScope)
	}
}

func TestAnnotationBindsExactlyOneCallOrAssistantTurn(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil, nil)
	callID := "call-1"
	annotation := Annotation{
		AnnotationID: "annotation-call", Revision: 1, CutID: "cut-1", TenantID: "tenant-1",
		Target: AnnotationTarget{SegmentID: "segment-1", CallID: &callID}, Status: StatusVerified,
		DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest,
	}
	stored, _, err := service.AppendAnnotation(context.Background(), annotation)
	requireDiagnosisImplemented(t, err)
	if stored.Target.CallID == nil || stored.Target.AssistantTurnSeq != nil {
		t.Fatalf("call-bound annotation target was not preserved exclusively: %#v", stored.Target)
	}
	turn := int64(3)
	for _, target := range []AnnotationTarget{
		{SegmentID: "segment-1"},
		{SegmentID: "segment-1", CallID: &callID, AssistantTurnSeq: &turn},
	} {
		annotation.Target = target
		if _, _, err := service.AppendAnnotation(context.Background(), annotation); err == nil {
			t.Fatalf("annotation target violating oneOf was accepted: %#v", target)
		}
	}
}

func TestAnnotationRevisionsAreImmutableHistory(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil, nil)
	callID := "call-1"
	base := Annotation{
		AnnotationID: "annotation-1", CutID: "cut-1", TenantID: "tenant-1",
		Target: AnnotationTarget{SegmentID: "segment-1", CallID: &callID}, Status: StatusInconclusive,
		DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest,
	}
	first := base
	first.Revision = 1
	if _, created, err := service.AppendAnnotation(context.Background(), first); err != nil || created {
		t.Fatalf("initial revision append failed: created=%v err=%v", created, err)
	}
	// A conflicting rewrite of the same revision is rejected; an identical
	// replay stays idempotent.
	if _, _, err := service.AppendAnnotation(context.Background(), func() Annotation { a := first; a.Status = StatusVerified; return a }()); !errors.Is(err, ErrAnnotationRevisionConflict) {
		t.Fatalf("conflicting same-revision append was accepted: %v", err)
	}
	if _, created, err := service.AppendAnnotation(context.Background(), first); err != nil || !created {
		t.Fatalf("identical revision replay was not idempotent: created=%v err=%v", created, err)
	}
	// A higher revision supersedes without destroying the old revision.
	second := base
	second.Revision = 2
	second.Status = StatusVerified
	second.SupersedesRevision = func() *int64 { r := int64(1); return &r }()
	if _, created, err := service.AppendAnnotation(context.Background(), second); err != nil || created {
		t.Fatalf("revision 2 append failed: created=%v err=%v", created, err)
	}
	// Both revisions remain independently resolvable for aggregation.
	resolved, err := service.Aggregate(context.Background(), AggregationRequest{
		TenantID: "tenant-1", CutID: "cut-1", PolicyDigest: "sha256:aggregation-v1",
		AnnotationRefs: []AnnotationRef{{AnnotationID: "annotation-1", Revision: 1}},
	})
	requireDiagnosisImplemented(t, err)
	if len(resolved.MaskedAnnotations) != 1 {
		t.Fatalf("historical revision 1 (inconclusive) was not preserved as masked: %#v", resolved)
	}
	resolved, err = service.Aggregate(context.Background(), AggregationRequest{
		TenantID: "tenant-1", CutID: "cut-1", PolicyDigest: "sha256:aggregation-v1",
		AnnotationRefs: []AnnotationRef{{AnnotationID: "annotation-1", Revision: 2}},
	})
	requireDiagnosisImplemented(t, err)
	if len(resolved.SourceAnnotations) != 1 || resolved.Confidence != "high" {
		t.Fatalf("revision 2 (verified) did not aggregate as high confidence: %#v", resolved)
	}
}

func TestAnnotationSchemaValidationRejectsMalformedRevisions(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil, nil)
	callID := "call-1"
	base := Annotation{
		AnnotationID: "annotation-1", Revision: 1, CutID: "cut-1", TenantID: "tenant-1",
		Target:                  AnnotationTarget{SegmentID: "segment-1", CallID: &callID},
		DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest,
	}
	for name, mutate := range map[string]func(*Annotation){
		"unknown-status":         func(a *Annotation) { a.Status = AnnotationStatus("speculative") },
		"zero-revision":          func(a *Annotation) { a.Status = StatusVerified; a.Revision = 0 },
		"unshaped-recipe-digest": func(a *Annotation) { a.Status = StatusVerified; a.ExecutionRecipeDigest = "sha256:recipe" },
	} {
		annotation := base
		mutate(&annotation)
		if _, _, err := service.AppendAnnotation(context.Background(), annotation); err == nil {
			t.Fatalf("malformed annotation %q was accepted", name)
		}
	}
}

func TestMissingAndInconclusiveAnnotationsAreMaskedFromAggregation(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil, nil)
	// Aggregate resolves refs against stored annotations, so seed the three
	// statuses first; IDs mirror the statuses under test.
	statuses := map[string]AnnotationStatus{
		"verified":     StatusVerified,
		"missing":      StatusMissing,
		"inconclusive": StatusInconclusive,
	}
	for annotationID, status := range statuses {
		id := annotationID
		_, _, err := service.AppendAnnotation(context.Background(), Annotation{
			AnnotationID: AnnotationID(annotationID), Revision: 1, CutID: "cut-1", TenantID: "tenant-1",
			Target: AnnotationTarget{SegmentID: "segment-1", CallID: &id}, Status: status,
			DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest,
		})
		if err != nil {
			t.Fatalf("seeding annotation %s failed: %v", annotationID, err)
		}
	}
	result, err := service.Aggregate(context.Background(), AggregationRequest{
		TenantID: "tenant-1", CutID: "cut-1", PolicyDigest: "sha256:aggregation-v1",
		AnnotationRefs: []AnnotationRef{{AnnotationID: "verified", Revision: 1}, {AnnotationID: "missing", Revision: 1}, {AnnotationID: "inconclusive", Revision: 1}},
	})
	requireDiagnosisImplemented(t, err)
	if result.Confidence == "high" || len(result.MaskedAnnotations) != 2 {
		t.Fatalf("missing/inconclusive annotations entered an unmasked high-confidence aggregate: %#v", result)
	}
}

func TestAggregationIsPolicyDigestBoundAndDeterministic(t *testing.T) {
	t.Parallel()
	service := NewService(nil, nil, nil)
	callID := "call-1"
	if _, _, err := service.AppendAnnotation(context.Background(), Annotation{
		AnnotationID: "annotation-1", Revision: 1, CutID: "cut-1", TenantID: "tenant-1",
		Target: AnnotationTarget{SegmentID: "segment-1", CallID: &callID}, Status: StatusVerified,
		DiagnosisPolicyRevision: "diagnosis-v1", ExecutionRecipeDigest: recipeDigest,
	}); err != nil {
		t.Fatalf("seeding annotation failed: %v", err)
	}
	request := AggregationRequest{TenantID: "tenant-1", CutID: "cut-1", PolicyDigest: "sha256:policy-a", AnnotationRefs: []AnnotationRef{{AnnotationID: "annotation-1", Revision: 1}}}
	first, err := service.Aggregate(context.Background(), request)
	requireDiagnosisImplemented(t, err)
	second, err := service.Aggregate(context.Background(), request)
	requireDiagnosisImplemented(t, err)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same aggregation policy/input was non-deterministic: first=%#v second=%#v", first, second)
	}
	request.PolicyDigest = "sha256:policy-b"
	changed, err := service.Aggregate(context.Background(), request)
	requireDiagnosisImplemented(t, err)
	if reflect.DeepEqual(changed, first) || changed.PolicyDigest == first.PolicyDigest {
		t.Fatalf("different policy digest did not produce distinguishable aggregate: first=%#v changed=%#v", first, changed)
	}
}
