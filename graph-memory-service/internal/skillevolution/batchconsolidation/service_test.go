package batchconsolidation

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
	"river2.dev/graph-memory-service/internal/skillevolution/validation"
)

func TestFixtureFamiliesProduceExpectedDecisions(t *testing.T) {
	cases := []struct {
		name          string
		build         func(*harness) []string
		wantOp        string
		wantSources   int
		wantConflicts bool
		wantSuccessor bool
	}{
		{"exact_duplicate", (*harness).admitExactDuplicates, OpRetain, 2, false, true},
		{"guard_specialization", (*harness).admitGuardSpecialization, OpSpecialize, 2, false, true},
		{"compatible_merge", (*harness).admitCompatibleMerge, OpMerge, 2, false, true},
		{"conditional_conflict", (*harness).admitConditionalConflict, OpRetain, 2, true, true},
		{"insufficient_evidence", (*harness).admitInsufficientEvidence, OpInsufficientEvidence, 1, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			family := tc.build(h)
			result, err := h.svc.Consolidate(context.Background(), Request{
				IdempotencyKey:         "idem-" + tc.name,
				AgentRunID:             "consolidator-01",
				ExpectedLedgerRevision: 0,
				FamilyProposalIDs:      family,
			})
			if err != nil {
				t.Fatalf("consolidate fixture: %v", err)
			}
			if result.LedgerRevision != 1 || result.LedgerDigest == "" || len(result.Decisions) != 1 {
				t.Fatalf("result = %#v", result)
			}
			d := result.Decisions[0]
			if d.Operation != tc.wantOp {
				t.Fatalf("operation = %q, want %q", d.Operation, tc.wantOp)
			}
			if d.DecisionID == "" || d.CreatedByAgentRunID != "consolidator-01" || d.ContentDigest == "" || d.ExpectedLedgerRevision != 0 {
				t.Fatalf("decision missing contract fields: %#v", d)
			}
			if len(d.SourceProposalIDs) != tc.wantSources {
				t.Fatalf("sources = %v, want %d complete ids", d.SourceProposalIDs, tc.wantSources)
			}
			if got, want := sortedCopy(d.SourceProposalIDs), sortedCopy(family); !equalStrings(got, want) {
				t.Fatalf("sources %v do not exactly cover family %v", d.SourceProposalIDs, family)
			}
			for _, src := range d.SourceProposalIDs {
				if src != familyFullID(family, src) || looksLikeHashPrefix(src) {
					t.Fatalf("source %q is not a complete family id", src)
				}
				if _, ok := h.svc.Coverage(src); !ok {
					t.Fatalf("source %q is not covered", src)
				}
			}
			if tc.wantConflicts && len(d.ConditionalConflicts) == 0 {
				t.Fatal("expected conditional_conflicts on the conflict fixture")
			}
			if !tc.wantConflicts && len(d.ConditionalConflicts) != 0 {
				t.Fatalf("unexpected conflicts %#v", d.ConditionalConflicts)
			}
			if tc.wantSuccessor {
				if d.SuccessorSkillRevision == nil {
					t.Fatal("expected successor skill revision")
				}
				if !equalStrings(sortedCopy(d.SuccessorSkillRevision.SourceProposalIDs), sortedCopy(family)) {
					t.Fatalf("successor sources = %v, want all family ids %v", d.SuccessorSkillRevision.SourceProposalIDs, family)
				}
			} else if d.SuccessorSkillRevision != nil {
				t.Fatalf("insufficient-evidence must not mint a successor: %#v", d.SuccessorSkillRevision)
			}
		})
	}
}

func TestPrefixUnknownMissingAndDuplicateCoverageFailClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*harness, []string) Request
	}{
		{"prefix_id", func(h *harness, family []string) Request {
			return h.coverRequest("idem-prefix", family, DecisionDraft{
				DecisionID: "decision-prefix", Operation: OpRetain,
				SourceProposalIDs: []string{family[0][:len(family[0])-2]},
			})
		}},
		{"hash_prefix", func(h *harness, family []string) Request {
			return h.coverRequest("idem-hash-prefix", family, DecisionDraft{
				DecisionID: "decision-hash-prefix", Operation: OpRetain,
				SourceProposalIDs: []string{"sha256:abcd"},
			})
		}},
		{"unknown_source", func(h *harness, family []string) Request {
			return h.coverRequest("idem-unknown", family, DecisionDraft{
				DecisionID: "decision-unknown", Operation: OpRetain,
				SourceProposalIDs: []string{"raw-proposal-does-not-exist-00000001"},
			})
		}},
		{"missing_source", func(h *harness, family []string) Request {
			return h.coverRequest("idem-missing", family, DecisionDraft{
				DecisionID: "decision-missing", Operation: OpRetain,
				SourceProposalIDs: []string{family[0]},
			})
		}},
		{"duplicate_coverage", func(h *harness, family []string) Request {
			return h.coverRequest("idem-dup", family,
				DecisionDraft{DecisionID: "decision-a", Operation: OpRetain, SourceProposalIDs: []string{family[0]}},
				DecisionDraft{DecisionID: "decision-b", Operation: OpRetain, SourceProposalIDs: []string{family[0], family[1]}},
			)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			family := h.admitExactDuplicates()
			beforeSeq, beforeDigest, beforeOK := h.head()
			beforeCount := len(h.svc.Decisions())
			_, err := h.svc.Consolidate(context.Background(), tc.mutate(h, family))
			if err == nil {
				t.Fatal("invalid coverage/source succeeded, want fail-closed")
			}
			if validation.CodeOf(err) == "" && ledger.ReasonOf(err) == "" {
				t.Fatalf("error %v carries no closed code", err)
			}
			afterSeq, afterDigest, afterOK := h.head()
			if afterSeq != beforeSeq || afterDigest != beforeDigest || afterOK != beforeOK {
				t.Fatalf("ledger changed from (%d,%s,%v) to (%d,%s,%v)", beforeSeq, beforeDigest, beforeOK, afterSeq, afterDigest, afterOK)
			}
			if got := len(h.svc.Decisions()); got != beforeCount {
				t.Fatalf("local decision count %d, want %d", got, beforeCount)
			}
		})
	}
}

func TestConcurrentWritersSameExpectedRevisionExactlyOneSucceeds(t *testing.T) {
	h := newHarness(t)
	family := h.admitExactDuplicates()
	var wg sync.WaitGroup
	var successes, failures int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.svc.Consolidate(context.Background(), Request{
				IdempotencyKey:         "idem-race-" + string(rune('a'+i)),
				AgentRunID:             "consolidator-01",
				ExpectedLedgerRevision: 0,
				FamilyProposalIDs:      family,
			})
			if err == nil {
				atomic.AddInt32(&successes, 1)
				return
			}
			atomic.AddInt32(&failures, 1)
			if ledger.ReasonOf(err) != ledger.ReasonProjectionHeadConflict && !strings.Contains(err.Error(), "expected ledger revision") {
				t.Errorf("loser error = %v, want projection head conflict", err)
			}
		}(i)
	}
	wg.Wait()
	if successes != 1 || failures != 1 {
		t.Fatalf("race outcome successes=%d failures=%d, want 1/1", successes, failures)
	}
	seq, digest, ok := h.head()
	if !ok || seq != 1 || digest == "" {
		t.Fatalf("ledger head = (%d,%s,%v), want revision 1", seq, digest, ok)
	}
	if got := len(h.svc.Decisions()); got != 1 {
		t.Fatalf("committed %d decisions, want exactly one winner batch", got)
	}
}

func TestSuccessorPreservesAllSourceIDsAndEvidenceLineage(t *testing.T) {
	h := newHarness(t)
	family := h.admitThreeSourceMerge()
	result, err := h.svc.Consolidate(context.Background(), h.coverRequest("idem-successor", family, DecisionDraft{
		DecisionID: "decision-merge-all", Operation: OpMerge, SourceProposalIDs: family,
	}))
	if err != nil {
		t.Fatalf("merge three sources: %v", err)
	}
	if len(result.Decisions) != 1 || result.Decisions[0].SuccessorSkillRevision == nil {
		t.Fatalf("result = %#v", result)
	}
	succ := *result.Decisions[0].SuccessorSkillRevision
	if !equalStrings(sortedCopy(succ.SourceProposalIDs), sortedCopy(family)) {
		t.Fatalf("successor kept %v, want all source ids %v (not only the first)", succ.SourceProposalIDs, family)
	}
	wantEvidence := []string{"evidence-01", "evidence-02", "evidence-03"}
	if !equalStrings(sortedCopy(succ.EvidenceLineage), wantEvidence) {
		t.Fatalf("evidence lineage = %v, want union %v", succ.EvidenceLineage, wantEvidence)
	}
	if succ.RevisionID == "" || succ.ContentDigest == "" {
		t.Fatalf("successor missing identity: %#v", succ)
	}
	stored, ok := h.svc.Successor(succ.RevisionID)
	if !ok || !equalStrings(stored.SourceProposalIDs, succ.SourceProposalIDs) || !equalStrings(stored.EvidenceLineage, succ.EvidenceLineage) {
		t.Fatalf("stored successor = %#v, ok=%v", stored, ok)
	}
}

func TestModelTransportFailureDoesNotDegradeToMarkdownLedger(t *testing.T) {
	t.Run("model", func(t *testing.T) {
		h := newHarness(t)
		h.svc.SetDecisionModel(failingModel{err: errors.New("model unavailable")})
		family := h.admitExactDuplicates()
		dir := t.TempDir()
		_, err := h.svc.Consolidate(context.Background(), Request{
			IdempotencyKey: "idem-model-fail", AgentRunID: "consolidator-01",
			FamilyProposalIDs: family,
		})
		if err == nil {
			t.Fatal("model failure succeeded")
		}
		if !strings.Contains(err.Error(), "refusing markdown ledger fallback") {
			t.Fatalf("error %v does not refuse markdown fallback", err)
		}
		assertUnchangedEmptyLedger(t, h, dir)
	})
	t.Run("transport", func(t *testing.T) {
		h := newHarness(t)
		h.svc.commit = func(context.Context, *ledger.Tx, uint64, string, []byte) (string, error) {
			return "", errors.New("content store transport down")
		}
		family := h.admitExactDuplicates()
		dir := t.TempDir()
		_, err := h.svc.Consolidate(context.Background(), Request{
			IdempotencyKey: "idem-transport-fail", AgentRunID: "consolidator-01",
			FamilyProposalIDs: family,
		})
		if err == nil {
			t.Fatal("transport failure succeeded")
		}
		if !strings.Contains(err.Error(), "refusing markdown ledger fallback") {
			t.Fatalf("error %v does not refuse markdown fallback", err)
		}
		assertUnchangedEmptyLedger(t, h, dir)
	})
}

func assertUnchangedEmptyLedger(t *testing.T, h *harness, dir string) {
	t.Helper()
	if h.svc.MarkdownFallbackUsed() {
		t.Fatal("service recorded a markdown ledger fallback")
	}
	if seq, digest, ok := h.head(); ok || seq != 0 || digest != "" {
		t.Fatalf("canonical ledger moved to (%d,%s,%v)", seq, digest, ok)
	}
	if rev, _ := h.svc.CurrentRevision(); rev != 0 || len(h.svc.Decisions()) != 0 {
		t.Fatalf("in-memory ledger revision=%d decisions=%d", rev, len(h.svc.Decisions()))
	}
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		name := strings.ToLower(info.Name())
		if strings.HasSuffix(name, ".md") && (strings.Contains(name, "ledger") || strings.Contains(name, "skill")) {
			t.Fatalf("wrote provenance-less markdown ledger %s", path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk temp dir: %v", err)
	}
}

type failingModel struct{ err error }

func (m failingModel) Review(context.Context, []rawproposal.RawSkillProposal, []DecisionDraft) error {
	return m.err
}

type harness struct {
	t         *testing.T
	proposals *rawproposal.Service
	store     *ledger.MemoryStore
	svc       *Service
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir, err := contract.DefaultConformanceDir()
	if err != nil {
		t.Fatalf("conformance dir: %v", err)
	}
	policy, err := contract.LoadSystemReasonPolicy(filepath.Join(dir, "policy"))
	if err != nil {
		t.Fatalf("reason policy: %v", err)
	}
	registry := &ledger.ContractReasonRegistry{Policy: policy}
	store, err := ledger.NewMemoryStore(registry)
	if err != nil {
		t.Fatalf("memory ledger: %v", err)
	}
	tx, err := ledger.NewManager(store, registry)
	if err != nil {
		t.Fatalf("transaction manager: %v", err)
	}
	source := rawproposal.NewMemoryTrajectorySource()
	source.Put(rawproposal.FrozenTrajectory{
		ID: "trajectory-01", SnapshotID: "snapshot-01",
		CompleteTrajectory: []string{"opening", "checkpoint", "outcome"},
		PublicOutcome:      "failed: file-not-found",
		Checkpoints:        map[string]rawproposal.Checkpoint{"checkpoint-01": {ID: "checkpoint-01", SnapshotID: "snapshot-01"}},
		Evidence: map[string]rawproposal.Evidence{
			"evidence-01": {ID: "evidence-01", TrajectoryID: "trajectory-01", SnapshotID: "snapshot-01", ObservableFacts: []string{"the relative path resolved outside the workspace"}},
			"evidence-02": {ID: "evidence-02", TrajectoryID: "trajectory-01", SnapshotID: "snapshot-01", ObservableFacts: []string{"the tool timed out after 30s on the large input file"}},
			"evidence-03": {ID: "evidence-03", TrajectoryID: "trajectory-01", SnapshotID: "snapshot-01", ObservableFacts: []string{"the compiler emitted unused-variable on helper.go"}},
		},
		AuthorizedDiagnosisRunIDs: map[string]bool{"diagnosis-run-01": true},
	})
	proposals, err := rawproposal.NewService(source, tx)
	if err != nil {
		t.Fatalf("raw proposal service: %v", err)
	}
	svc, err := NewService(proposals, store, tx)
	if err != nil {
		t.Fatalf("consolidation service: %v", err)
	}
	return &harness{t: t, proposals: proposals, store: store, svc: svc}
}

func (h *harness) head() (uint64, string, bool) {
	h.t.Helper()
	seq, digest, ok, err := h.store.GetHead(ledger.HeadProjection, LedgerHeadKey)
	if err != nil {
		h.t.Fatalf("get ledger head: %v", err)
	}
	return seq, digest, ok
}

func (h *harness) coverRequest(key string, family []string, drafts ...DecisionDraft) Request {
	return Request{
		IdempotencyKey: key, AgentRunID: "consolidator-01",
		FamilyProposalIDs: family, Decisions: drafts,
	}
}

func (h *harness) admitExactDuplicates() []string {
	a := h.admit(proposalSpec{id: "raw-proposal-dup-aaaaaaaaaaaaaa01"})
	b := h.admit(proposalSpec{id: "raw-proposal-dup-aaaaaaaaaaaaaa02"})
	return []string{a, b}
}

func (h *harness) admitGuardSpecialization() []string {
	a := h.admit(proposalSpec{
		id:                "raw-proposal-spec-aaaaaaaaaaaa01",
		contraindications: []string{"Do not rewrite an already absolute path."},
	})
	b := h.admit(proposalSpec{
		id:                "raw-proposal-spec-aaaaaaaaaaaa02",
		contraindications: []string{"Do not rewrite paths that already have a scheme."},
	})
	return []string{a, b}
}

func (h *harness) admitCompatibleMerge() []string {
	a := h.admit(proposalSpec{
		id:      "raw-proposal-merge-aaaaaaaaaaa01",
		insight: "The relative path resolved outside the workspace, so tool invocation needs an explicit workspace-root path.",
		steps:   []string{"When the relative path resolved outside the workspace, resolve it against the workspace root before invoking the tool."},
		change:  "Resolve the path against the workspace root before invoking the tool.",
	})
	b := h.admit(proposalSpec{
		id:      "raw-proposal-merge-aaaaaaaaaaa02",
		insight: "The relative path resolved outside the workspace, so escaped paths must be rejected after resolution.",
		steps:   []string{"When the relative path resolved outside the workspace, reject any path that still escapes after resolution."},
		change:  "Reject paths that still escape the workspace after resolution.",
	})
	return []string{a, b}
}

func (h *harness) admitConditionalConflict() []string {
	a := h.admit(proposalSpec{
		id:     "raw-proposal-conflict-aaaaaaaaa01",
		steps:  []string{"When the relative path resolved outside the workspace, resolve it against the workspace root before invoking the tool."},
		change: "Resolve the path against the workspace root before invoking the tool.",
	})
	b := h.admit(proposalSpec{
		id:     "raw-proposal-conflict-aaaaaaaaa02",
		steps:  []string{"When the relative path resolved outside the workspace, never rewrite the path; fail closed instead."},
		change: "Never rewrite the escaped path; fail closed.",
	})
	return []string{a, b}
}

func (h *harness) admitInsufficientEvidence() []string {
	id := h.admit(proposalSpec{
		id:      "raw-proposal-insufficient-aaaaaa01",
		failure: "The relative path resolved outside the workspace; insufficient evidence to generalize beyond this trajectory.",
	})
	return []string{id}
}

func (h *harness) admitThreeSourceMerge() []string {
	a := h.admit(proposalSpec{id: "raw-proposal-lineage-aaaaaaaaaa01", evidence: []string{"evidence-01"}})
	b := h.admit(proposalSpec{
		id:       "raw-proposal-lineage-aaaaaaaaaa02",
		evidence: []string{"evidence-02"},
		trigger:  "When the tool timed out after 30s on the large input file at checkpoint-01",
		failure:  "The tool timed out after 30s on the large input file.",
		insight:  "The tool timed out after 30s on the large input file, so the call needs a bounded chunk.",
		steps:    []string{"When the tool timed out after 30s on the large input file, split the input before invoking the tool."},
		change:   "Split the large input before invoking the tool.",
	})
	c := h.admit(proposalSpec{
		id:       "raw-proposal-lineage-aaaaaaaaaa03",
		evidence: []string{"evidence-03"},
		trigger:  "When the compiler emitted unused-variable on helper.go at checkpoint-01",
		failure:  "The compiler emitted unused-variable on helper.go.",
		insight:  "The compiler emitted unused-variable on helper.go, so drop the unused helper before compile.",
		steps:    []string{"When the compiler emitted unused-variable on helper.go, remove the unused helper before compile."},
		change:   "Remove the unused helper before compile.",
	})
	return []string{a, b, c}
}

type proposalSpec struct {
	id, trigger, failure, baseline, insight, change, outcome string
	steps, evidence, contraindications, pitfalls             []string
}

func (h *harness) admit(spec proposalSpec) string {
	h.t.Helper()
	if spec.trigger == "" {
		spec.trigger = "When the relative path resolved outside the workspace at checkpoint-01"
	}
	if spec.failure == "" {
		spec.failure = "The relative path resolved outside the workspace and caused file-not-found."
	}
	if spec.baseline == "" {
		spec.baseline = "Invoke the tool with the relative path directly."
	}
	if spec.insight == "" {
		spec.insight = "The relative path resolved outside the workspace, so tool invocation needs an explicit workspace-root path."
	}
	if len(spec.steps) == 0 {
		spec.steps = []string{"When the relative path resolved outside the workspace, resolve it against the workspace root before invoking the tool."}
	}
	if spec.change == "" {
		spec.change = "Resolve the path against the workspace root before invoking the tool."
	}
	if spec.outcome == "" {
		spec.outcome = "failed: file-not-found"
	}
	if len(spec.evidence) == 0 {
		spec.evidence = []string{"evidence-01"}
	}
	if spec.pitfalls == nil {
		spec.pitfalls = []string{"Do not rewrite an already absolute path."}
	}
	if spec.contraindications == nil {
		spec.contraindications = []string{"Do not rewrite an already absolute path."}
	}
	body := map[string]any{
		"proposal_id": spec.id, "schema_version": rawproposal.SchemaVersion, "source_checkpoint_id": "checkpoint-01",
		"source_evidence_refs": anyStrings(spec.evidence), "context_trigger": spec.trigger,
		"failure_or_opportunity": spec.failure, "baseline_behavior": spec.baseline,
		"non_obvious_insight": spec.insight, "decision_policy_or_steps": anyStrings(spec.steps),
		"expected_behavior_change": spec.change, "contraindications": anyStrings(spec.contraindications),
		"pitfalls": anyStrings(spec.pitfalls), "outcome_observed": spec.outcome,
		"novelty_status": rawproposal.NoveltyHypothesized, "created_by_agent_run_id": "diagnosis-run-01",
		"created_at": "2026-09-17T09:53:15Z",
	}
	digest, err := contract.DigestOf(map[string]any{
		"proposal_id": body["proposal_id"], "schema_version": body["schema_version"],
		"source_checkpoint_id": body["source_checkpoint_id"], "source_evidence_refs": body["source_evidence_refs"],
		"context_trigger": body["context_trigger"], "failure_or_opportunity": body["failure_or_opportunity"],
		"baseline_behavior": body["baseline_behavior"], "non_obvious_insight": body["non_obvious_insight"],
		"decision_policy_or_steps": body["decision_policy_or_steps"], "expected_behavior_change": body["expected_behavior_change"],
		"contraindications": body["contraindications"], "pitfalls": body["pitfalls"],
		"outcome_observed": body["outcome_observed"], "novelty_status": body["novelty_status"],
		"created_by_agent_run_id": body["created_by_agent_run_id"], "created_at": body["created_at"],
	})
	if err != nil {
		h.t.Fatalf("proposal digest: %v", err)
	}
	body["content_digest"] = digest
	ref, err := h.proposals.Admit(context.Background(), rawproposal.AdmissionRequest{
		IdempotencyKey: "idem-" + spec.id, DiagnosisRunID: "diagnosis-run-01",
		TrajectoryID: "trajectory-01", Body: body,
	})
	if err != nil {
		h.t.Fatalf("admit %s: %v", spec.id, err)
	}
	return ref.ProposalID
}

func anyStrings(values []string) []any {
	out := make([]any, len(values))
	for i := range values {
		out[i] = values[i]
	}
	return out
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func familyFullID(family []string, src string) string {
	for _, id := range family {
		if id == src {
			return id
		}
	}
	return ""
}
