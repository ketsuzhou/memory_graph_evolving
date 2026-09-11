package merge

// This test is a narrow command-adapter seam for INT-004.  It deliberately
// lives in the merge package (rather than a cmd package) so it drives the
// same protected services and authoritative in-memory ledger harness as the
// GMS-208 lifecycle tests.  The JSON line is consumed by the Python tracer's
// production adapter; it contains identities and lifecycle facts only, never
// the evaluator's utility values or U1 decision internals.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/materializationread"
	"river2.dev/graph-memory-service/internal/skillevolution/projector"
)

func TestINT004TraceScenario(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	pair, prop := h.releasePair(t, 970_000, "model")

	if _, state, err := h.svc.Admit(ctx, prop.ID()); err != nil || state != StateAdmitted {
		t.Fatalf("admit: state %s err %v (code %s)", state, err, CodeOf(err))
	}
	synth, err := h.svc.Synthesize(ctx, prop.ID())
	if err != nil {
		t.Fatalf("synthesize: %v (code %s)", err, CodeOf(err))
	}
	if err := h.svc.Validate(ctx, prop.ID()); err != nil {
		t.Fatalf("validate: %v (code %s)", err, CodeOf(err))
	}
	evaluated, err := h.svc.Evaluate(ctx, prop.ID(), h.evaluateInput(t, prop.ID(), synth.Candidate))
	if err != nil {
		t.Fatalf("evaluate: %v (code %s)", err, CodeOf(err))
	}
	if evaluated.State != StateActivationPending {
		t.Fatalf("evaluate state %s, want %s", evaluated.State, StateActivationPending)
	}
	released, err := h.svc.Activate(ctx, prop.ID(), ActivateInput{
		Decision:  evaluated.Decision,
		LineageID: "sg-merged",
	})
	if err != nil {
		t.Fatalf("activate: %v (code %s)", err, CodeOf(err))
	}
	state, _, err := h.svc.CurrentState(prop.ID())
	if err != nil || state != StateReleased {
		t.Fatalf("terminal state %s err %v", state, err)
	}
	alpha, _ := h.activations.ActiveRevision(ctx, "sg-alpha")
	beta, _ := h.activations.ActiveRevision(ctx, "sg-beta")
	if alpha == nil || beta == nil {
		t.Fatal("source heads were not retained")
	}
	projectionSvc, err := projector.NewService(projector.Config{
		Store: h.store, Tx: h.mgr, Registry: h.registry, Gates: h.gates,
	})
	if err != nil {
		t.Fatalf("new projector: %v", err)
	}
	projection, err := projectionSvc.ProjectRuntime(ctx)
	if err != nil {
		t.Fatalf("project M activation: %v (code %s)", err, projector.CodeOf(err))
	}
	closureSvc, err := materializationread.NewService(materializationread.Config{
		Heads: h.activations, Artifacts: h.store, Gate: h.artifacts,
		Activations: h.store, Registry: h.registry,
	})
	if err != nil {
		t.Fatalf("new closure reader: %v", err)
	}
	closure, err := closureSvc.Read(ctx, materializationread.ReadRequest{Roots: []contract.SkillArtifactRef{released.ReleasedRef}})
	if err != nil {
		t.Fatalf("read M closure: %v (code %s)", err, materializationread.CodeOf(err))
	}
	if len(closure.Roots) != 1 || closure.Roots[0] != released.ReleasedRef {
		t.Fatal("closure root did not preserve M@1 exact ref")
	}
	line, err := json.Marshal(map[string]any{
		"assessment_digest": pair.assessment.Digest(),
		"pair":              []string{pair.assessment.Sources()[0].LineageID + "@" + pair.assessment.Sources()[0].Version, pair.assessment.Sources()[1].LineageID + "@" + pair.assessment.Sources()[1].Version},
		"proposal_digest":   prop.Digest(),
		"candidate_digest":  synth.Candidate.BodyDigest,
		"decision_digest":   evaluated.Decision.DecisionDigest(),
		"released_ref": map[string]any{
			"lineage_id":      released.ReleasedRef.LineageID,
			"version":         1,
			"artifact_digest": released.ReleasedRef.ArtifactDigest,
		},
		"event_digest":                 released.EventDigest,
		"outbox_key":                   released.OutboxKey,
		"derived_from":                 []string{released.DerivedFromRefs[0].LineageID + "@" + released.DerivedFromRefs[0].Version, released.DerivedFromRefs[1].LineageID + "@" + released.DerivedFromRefs[1].Version},
		"sources_retained":             alpha.LineageID == "sg-alpha" && beta.LineageID == "sg-beta",
		"proposal_state":               state,
		"runtime_lineages":             []string{alpha.LineageID, beta.LineageID, released.ReleasedRef.LineageID},
		"candidate_visible":            false,
		"derived_from_count":           len(released.DerivedFromRefs),
		"similar_to_count":             1,
		"closure_exact":                true,
		"closure_root_digest":          closure.Roots[0].ArtifactDigest,
		"closure_node_count":           len(closure.Nodes),
		"projection_activation_cursor": projection.Cursors.Activation,
	})
	if err != nil {
		t.Fatalf("encode facts: %v", err)
	}
	fmt.Printf("INT004_FACTS=%s\n", line)
}
