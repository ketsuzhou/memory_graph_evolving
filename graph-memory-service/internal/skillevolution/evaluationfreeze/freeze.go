// Package evaluationfreeze seals one immutable EvaluationFreezeManifest
// before the first held-out test (Warm Skill Graph Batch contract §5.4).
//
// The manifest simultaneously binds the evidence cut, Skill Evolution
// Ledger revision, complete proposal/provenance set, Evaluation Graph
// digest/watermark, and prompt/schema/model/tool/config/grading-policy
// revisions. Test attempts receive only that digest. Freeze is
// fail-closed: a moved head, missing proposal, provenance hole, graph
// digest mismatch, contaminated test scope, or partial graph produces no
// manifest and cannot start tests. There is no warning-then-continue path.
package evaluationfreeze

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/batchconsolidation"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
	"river2.dev/graph-memory-service/internal/skillevolution/ledger"
	"river2.dev/graph-memory-service/internal/skillevolution/rawproposal"
)

const (
	// SchemaVersion is the closed EvaluationFreezeManifest body shape.
	SchemaVersion = "gms.evaluation-freeze-manifest.v1"
	// LedgerHeadKey is the Skill Evolution Ledger CAS head this freeze pins.
	LedgerHeadKey = batchconsolidation.LedgerHeadKey
)

var digestRE = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

var (
	// ErrMovedHead is a ledger head that moved after the freeze pin was taken.
	ErrMovedHead = errors.New("evaluation freeze: ledger head moved")
	// ErrMissingProposal is a freeze-set proposal ID that cannot be loaded.
	ErrMissingProposal = errors.New("evaluation freeze: missing proposal")
	// ErrProvenanceHole is a proposal whose provenance or evidence cut is incomplete.
	ErrProvenanceHole = errors.New("evaluation freeze: provenance hole")
	// ErrGraphDigestMismatch is a rebuilt Evaluation Graph that does not match the pin.
	ErrGraphDigestMismatch = errors.New("evaluation freeze: graph digest mismatch")
	// ErrScopeContamination is Test Room, overlay, or held-out feedback in freeze scope.
	ErrScopeContamination = errors.New("evaluation freeze: test room, overlay, or held-out feedback in freeze scope")
	// ErrPartialGraphFreeze rejects any incomplete graph freeze, including a
	// caller asking to continue after a warning.
	ErrPartialGraphFreeze = errors.New("evaluation freeze: partial graph freeze is not allowed")
	// ErrNotFrozen is a test-start attempt before a successful freeze.
	ErrNotFrozen = errors.New("evaluation freeze: no sealed manifest; tests cannot start")
	// ErrManifestConflict is a second freeze with a different digest.
	ErrManifestConflict = errors.New("evaluation freeze: evaluation already sealed to a different manifest digest")
)

// ProposalCatalog is the read-only rawproposal seam. Implementations must
// resolve complete IDs and the provenance minted with each proposal.
type ProposalCatalog interface {
	Get(proposalID string) (rawproposal.RawSkillProposal, bool)
	ProvenanceForProposal(proposalID string) (rawproposal.RawProposalProvenance, bool)
}

// EvidenceBatch is one committed evidence batch in the freeze cut.
type EvidenceBatch struct {
	ID   string
	Kind string
}

// EvidenceCut is the frozen evidence watermark and batch set.
type EvidenceCut struct {
	Batches         []EvidenceBatch
	Watermark       string
	WatermarkDigest string
}

// PolicyRevisions pins the run configuration that every test must share.
type PolicyRevisions struct {
	PromptDigest        string
	SchemaDigest        string
	ModelDigest         string
	ToolDigest          string
	ConfigDigest        string
	GradingPolicyDigest string
}

// ScopeItem names one input included in the freeze. Contaminating kinds
// (test_room, overlay, held_out_feedback) fail closed.
type ScopeItem struct {
	Kind string
	ID   string
}

// ProposalBinding is one complete proposal plus its provenance digest.
type ProposalBinding struct {
	ProposalID       string
	ContentDigest    string
	ProvenanceDigest string
}

// GraphPin is the rebuilt Evaluation Graph identity bound into the manifest.
type GraphPin struct {
	Stream           string
	LedgerRevision   uint64
	LedgerDigest     string
	ProjectionDigest string
	Watermark        string
	WatermarkDigest  string
}

// Manifest is the immutable freeze document. Callers receive a snapshot;
// the package exposes no mutation of a sealed manifest.
type Manifest struct {
	ID             string
	Digest         string
	EvidenceCut    EvidenceCut
	LedgerRevision uint64
	LedgerDigest   string
	Proposals      []ProposalBinding
	Graph          GraphPin
	Policy         PolicyRevisions
}

// TestBinding is the only freeze identity a test attempt may receive.
type TestBinding struct {
	ManifestDigest string
}

// TestAttempt records one held-out attempt against the sealed digest.
type TestAttempt struct {
	AttemptID      string
	ManifestDigest string
}

// Request is one freeze attempt. Expected ledger and graph pins are
// compared against live heads and a rebuilt Evaluation Graph.
type Request struct {
	ExpectedLedgerRevision       uint64
	ExpectedLedgerDigest         string
	EvidenceCut                  EvidenceCut
	ProposalIDs                  []string
	GraphFixture                 evaluationgraph.CanonicalFixture
	ExpectedGraphDigest          string
	ExpectedGraphWatermark       string
	ExpectedGraphWatermarkDigest string
	Policy                       PolicyRevisions
	Scope                        []ScopeItem
	// PartialGraph and ContinueOnGraphWarning are rejected: a partial
	// graph freeze must not warn and continue.
	PartialGraph           bool
	ContinueOnGraphWarning bool
}

// Service seals at most one EvaluationFreezeManifest and records test
// attempts against that single digest.
type Service struct {
	catalog ProposalCatalog
	heads   ledger.HeadStore

	mu       sync.Mutex
	sealed   *Manifest
	attempts []TestAttempt
}

func NewService(catalog ProposalCatalog, heads ledger.HeadStore) (*Service, error) {
	if catalog == nil || heads == nil {
		return nil, fmt.Errorf("evaluationfreeze: nil proposal catalog or ledger head store")
	}
	return &Service{catalog: catalog, heads: heads}, nil
}

// Freeze validates the complete cut and seals one content-addressed
// manifest. The same input is idempotent: it yields the same ID and digest.
// A changed head or a different cut after a first seal fail closed.
func (s *Service) Freeze(_ context.Context, req Request) (Manifest, error) {
	if s == nil || s.catalog == nil || s.heads == nil {
		return Manifest{}, fmt.Errorf("evaluationfreeze: service is not wired")
	}
	manifest, err := seal(s.heads, s.catalog, req)
	if err != nil {
		return Manifest{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed != nil && s.sealed.Digest != manifest.Digest {
		return Manifest{}, fmt.Errorf("%w: sealed %s, got %s", ErrManifestConflict, s.sealed.Digest, manifest.Digest)
	}
	if s.sealed == nil {
		cloned := cloneManifest(manifest)
		s.sealed = &cloned
	}
	return cloneManifest(*s.sealed), nil
}

// SealedManifest returns the unique freeze, if one exists.
func (s *Service) SealedManifest() (Manifest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed == nil {
		return Manifest{}, false
	}
	return cloneManifest(*s.sealed), true
}

// TestBinding returns the single digest tests may pin. It fails if freeze
// has not succeeded, so tests cannot start on a missing or partial manifest.
func (s *Service) TestBinding() (TestBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed == nil {
		return TestBinding{}, ErrNotFrozen
	}
	return TestBinding{ManifestDigest: s.sealed.Digest}, nil
}

// RecordTestAttempt binds one attempt to the sealed digest only. N attempts
// therefore record exactly the same digest.
func (s *Service) RecordTestAttempt(attemptID string) (TestAttempt, error) {
	if strings.TrimSpace(attemptID) == "" {
		return TestAttempt{}, fmt.Errorf("evaluationfreeze: test attempt id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed == nil {
		return TestAttempt{}, ErrNotFrozen
	}
	attempt := TestAttempt{AttemptID: attemptID, ManifestDigest: s.sealed.Digest}
	s.attempts = append(s.attempts, attempt)
	return attempt, nil
}

func (s *Service) TestAttempts() []TestAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TestAttempt, len(s.attempts))
	copy(out, s.attempts)
	return out
}

func seal(heads ledger.HeadStore, catalog ProposalCatalog, req Request) (Manifest, error) {
	if req.PartialGraph || req.ContinueOnGraphWarning {
		return Manifest{}, fmt.Errorf("%w: refuse warning-then-continue", ErrPartialGraphFreeze)
	}
	if err := rejectContaminatedScope(req.Scope); err != nil {
		return Manifest{}, err
	}
	if err := verifyLedgerHead(heads, req.ExpectedLedgerRevision, req.ExpectedLedgerDigest); err != nil {
		return Manifest{}, err
	}
	cut, err := sealEvidenceCut(req.EvidenceCut)
	if err != nil {
		return Manifest{}, err
	}
	if err := validatePolicy(req.Policy); err != nil {
		return Manifest{}, err
	}
	bindings, err := bindProposals(catalog, req.ProposalIDs, cut)
	if err != nil {
		return Manifest{}, err
	}
	graph, err := bindGraph(req)
	if err != nil {
		return Manifest{}, err
	}

	canonical := map[string]any{
		"schema_version": SchemaVersion,
		"evidence_cut": map[string]any{
			"batch_ids":        stringsAny(batchIDs(cut)),
			"watermark":        cut.Watermark,
			"watermark_digest": cut.WatermarkDigest,
		},
		"ledger_revision": req.ExpectedLedgerRevision,
		"ledger_digest":   req.ExpectedLedgerDigest,
		"proposals":       proposalCanonical(bindings),
		"evaluation_graph": map[string]any{
			"stream":            graph.Stream,
			"ledger_revision":   graph.LedgerRevision,
			"ledger_digest":     graph.LedgerDigest,
			"projection_digest": graph.ProjectionDigest,
			"watermark":         graph.Watermark,
			"watermark_digest":  graph.WatermarkDigest,
		},
		"policy": map[string]any{
			"prompt_digest":         req.Policy.PromptDigest,
			"schema_digest":         req.Policy.SchemaDigest,
			"model_digest":          req.Policy.ModelDigest,
			"tool_digest":           req.Policy.ToolDigest,
			"config_digest":         req.Policy.ConfigDigest,
			"grading_policy_digest": req.Policy.GradingPolicyDigest,
		},
	}
	digest, err := contract.DigestOf(canonical)
	if err != nil {
		return Manifest{}, fmt.Errorf("evaluationfreeze: manifest cannot enter canonical hashed core: %w", err)
	}
	return Manifest{
		ID:             "evaluation-freeze/" + strings.TrimPrefix(digest, "sha256:"),
		Digest:         digest,
		EvidenceCut:    cut,
		LedgerRevision: req.ExpectedLedgerRevision,
		LedgerDigest:   req.ExpectedLedgerDigest,
		Proposals:      bindings,
		Graph:          graph,
		Policy:         req.Policy,
	}, nil
}

func verifyLedgerHead(heads ledger.HeadStore, expectedSeq uint64, expectedDigest string) error {
	seq, digest, ok, err := heads.GetHead(ledger.HeadProjection, LedgerHeadKey)
	if err != nil {
		return err
	}
	if !ok {
		seq, digest = 0, ""
	}
	if seq != expectedSeq || digest != expectedDigest {
		return fmt.Errorf("%w: expected revision %d digest %q, head is %d %q", ErrMovedHead, expectedSeq, expectedDigest, seq, digest)
	}
	if expectedSeq > 0 && !digestRE.MatchString(expectedDigest) {
		return fmt.Errorf("%w: ledger digest %q is not a sha256 pin", ErrMovedHead, expectedDigest)
	}
	return nil
}

func sealEvidenceCut(cut EvidenceCut) (EvidenceCut, error) {
	if len(cut.Batches) == 0 {
		return EvidenceCut{}, fmt.Errorf("evaluationfreeze: evidence cut must list at least one batch")
	}
	seen := make(map[string]bool, len(cut.Batches))
	cloned := make([]EvidenceBatch, 0, len(cut.Batches))
	ids := make([]string, 0, len(cut.Batches))
	for _, batch := range cut.Batches {
		if strings.TrimSpace(batch.ID) == "" {
			return EvidenceCut{}, fmt.Errorf("evaluationfreeze: evidence batch id is required")
		}
		if contaminatedKind(batch.Kind) {
			return EvidenceCut{}, fmt.Errorf("%w: evidence batch %q kind %q", ErrScopeContamination, batch.ID, batch.Kind)
		}
		if seen[batch.ID] {
			return EvidenceCut{}, fmt.Errorf("evaluationfreeze: evidence batch %q is repeated", batch.ID)
		}
		seen[batch.ID] = true
		cloned = append(cloned, EvidenceBatch{ID: batch.ID, Kind: normalizeKind(batch.Kind)})
		ids = append(ids, batch.ID)
	}
	sort.Strings(ids)
	batchDigest, err := contract.DigestOf(map[string]any{"batch_ids": stringsAny(ids)})
	if err != nil {
		return EvidenceCut{}, fmt.Errorf("evaluationfreeze: evidence cut cannot enter canonical hashed core: %w", err)
	}
	watermark := fmt.Sprintf("evidence@%d:%s", len(ids), batchDigest)
	watermarkDigest, err := contract.DigestOf(watermark)
	if err != nil {
		return EvidenceCut{}, fmt.Errorf("evaluationfreeze: evidence watermark cannot enter canonical hashed core: %w", err)
	}
	if cut.Watermark != "" && cut.Watermark != watermark {
		return EvidenceCut{}, fmt.Errorf("evaluationfreeze: evidence watermark %q does not match sealed cut %q", cut.Watermark, watermark)
	}
	if cut.WatermarkDigest != "" && cut.WatermarkDigest != watermarkDigest {
		return EvidenceCut{}, fmt.Errorf("evaluationfreeze: evidence watermark digest %q does not match sealed cut %q", cut.WatermarkDigest, watermarkDigest)
	}
	sort.Slice(cloned, func(i, j int) bool { return cloned[i].ID < cloned[j].ID })
	return EvidenceCut{Batches: cloned, Watermark: watermark, WatermarkDigest: watermarkDigest}, nil
}

func bindProposals(catalog ProposalCatalog, ids []string, cut EvidenceCut) ([]ProposalBinding, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: freeze set is empty", ErrMissingProposal)
	}
	cutIDs := make(map[string]bool, len(cut.Batches))
	for _, batch := range cut.Batches {
		cutIDs[batch.ID] = true
	}
	seen := make(map[string]bool, len(ids))
	out := make([]ProposalBinding, 0, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) == "" {
			return nil, fmt.Errorf("%w: proposal id is empty", ErrMissingProposal)
		}
		if seen[id] {
			return nil, fmt.Errorf("%w: proposal %q is repeated", ErrMissingProposal, id)
		}
		seen[id] = true
		proposal, ok := catalog.Get(id)
		if !ok || proposal.ProposalID != id {
			return nil, fmt.Errorf("%w: %q", ErrMissingProposal, id)
		}
		provenance, ok := catalog.ProvenanceForProposal(id)
		if !ok {
			return nil, fmt.Errorf("%w: proposal %q has no provenance record", ErrProvenanceHole, id)
		}
		if err := verifyProvenance(proposal, provenance, cutIDs); err != nil {
			return nil, err
		}
		provDigest, err := provenanceDigest(provenance)
		if err != nil {
			return nil, fmt.Errorf("%w: proposal %q provenance is not canonical: %v", ErrProvenanceHole, id, err)
		}
		out = append(out, ProposalBinding{
			ProposalID:       proposal.ProposalID,
			ContentDigest:    proposal.ContentDigest,
			ProvenanceDigest: provDigest,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProposalID < out[j].ProposalID })
	return out, nil
}

func verifyProvenance(proposal rawproposal.RawSkillProposal, provenance rawproposal.RawProposalProvenance, cutIDs map[string]bool) error {
	if provenance.ProposalRef.ProposalID != proposal.ProposalID || provenance.ProposalRef.ContentDigest != proposal.ContentDigest {
		return fmt.Errorf("%w: proposal %q provenance ref does not match the canonical body", ErrProvenanceHole, proposal.ProposalID)
	}
	if provenance.SourceTrajectoryID == "" || provenance.SourceSnapshotID == "" || provenance.SourceCheckpoint.ID == "" || provenance.SourceCheckpoint.ID != proposal.SourceCheckpointID {
		return fmt.Errorf("%w: proposal %q is missing trajectory, snapshot, or checkpoint provenance", ErrProvenanceHole, proposal.ProposalID)
	}
	if len(provenance.SourceEvidence) == 0 || len(proposal.SourceEvidenceRefs) == 0 {
		return fmt.Errorf("%w: proposal %q has an empty evidence lineage", ErrProvenanceHole, proposal.ProposalID)
	}
	cited := make(map[string]bool, len(provenance.SourceEvidence))
	for _, ev := range provenance.SourceEvidence {
		if ev.ID == "" || ev.SnapshotID != provenance.SourceSnapshotID || ev.TrajectoryID != provenance.SourceTrajectoryID {
			return fmt.Errorf("%w: proposal %q evidence %q is incomplete or cross-snapshot", ErrProvenanceHole, proposal.ProposalID, ev.ID)
		}
		if !cutIDs[ev.ID] {
			return fmt.Errorf("%w: proposal %q evidence %q is outside the evidence cut", ErrProvenanceHole, proposal.ProposalID, ev.ID)
		}
		cited[ev.ID] = true
	}
	for _, id := range proposal.SourceEvidenceRefs {
		if !cited[id] {
			return fmt.Errorf("%w: proposal %q cites evidence %q with no provenance record", ErrProvenanceHole, proposal.ProposalID, id)
		}
	}
	return nil
}

func bindGraph(req Request) (GraphPin, error) {
	if containsHeldOutGraphRecord(req.GraphFixture) {
		return GraphPin{}, fmt.Errorf("%w: held-out feedback record in evaluation graph fixture", ErrScopeContamination)
	}
	view, err := evaluationgraph.Build(req.GraphFixture)
	if err != nil {
		if errors.Is(err, evaluationgraph.ErrHeldOutFeedbackInput) {
			return GraphPin{}, fmt.Errorf("%w: %v", ErrScopeContamination, err)
		}
		if errors.Is(err, evaluationgraph.ErrPinMismatch) {
			return GraphPin{}, fmt.Errorf("%w: %v", ErrGraphDigestMismatch, err)
		}
		return GraphPin{}, fmt.Errorf("%w: %v", ErrPartialGraphFreeze, err)
	}
	if view == nil || view.ProjectionDigest == "" || view.Pin.Watermark == "" || view.Pin.WatermarkDigest == "" {
		return GraphPin{}, fmt.Errorf("%w: rebuilt graph is missing digest or watermark", ErrPartialGraphFreeze)
	}
	if unconsolidated := unconsolidatedProposals(req.GraphFixture, view); len(unconsolidated) > 0 {
		return GraphPin{}, fmt.Errorf("%w: unconsolidated proposals %v are not in the frozen graph", ErrPartialGraphFreeze, unconsolidated)
	}
	if req.ExpectedGraphDigest != "" && req.ExpectedGraphDigest != view.ProjectionDigest {
		return GraphPin{}, fmt.Errorf("%w: expected projection digest %q, rebuilt %q", ErrGraphDigestMismatch, req.ExpectedGraphDigest, view.ProjectionDigest)
	}
	if req.ExpectedGraphWatermark != "" && req.ExpectedGraphWatermark != view.Pin.Watermark {
		return GraphPin{}, fmt.Errorf("%w: expected watermark %q, rebuilt %q", ErrGraphDigestMismatch, req.ExpectedGraphWatermark, view.Pin.Watermark)
	}
	if req.ExpectedGraphWatermarkDigest != "" && req.ExpectedGraphWatermarkDigest != view.Pin.WatermarkDigest {
		return GraphPin{}, fmt.Errorf("%w: expected watermark digest %q, rebuilt %q", ErrGraphDigestMismatch, req.ExpectedGraphWatermarkDigest, view.Pin.WatermarkDigest)
	}
	return GraphPin{
		Stream:           view.Pin.Stream,
		LedgerRevision:   view.Pin.LedgerRevision,
		LedgerDigest:     view.Pin.LedgerDigest,
		ProjectionDigest: view.ProjectionDigest,
		Watermark:        view.Pin.Watermark,
		WatermarkDigest:  view.Pin.WatermarkDigest,
	}, nil
}

func unconsolidatedProposals(fixture evaluationgraph.CanonicalFixture, view *evaluationgraph.View) []string {
	included := make(map[evaluationgraph.RevisionRef]bool, len(view.Nodes))
	for _, node := range view.Nodes {
		included[node.Ref] = true
	}
	var missing []string
	for _, record := range fixture.Records {
		if record.Kind != evaluationgraph.RecordProposal || record.Proposal == nil {
			continue
		}
		if !included[record.Proposal.Ref] {
			missing = append(missing, record.ID)
		}
	}
	sort.Strings(missing)
	return missing
}

func containsHeldOutGraphRecord(fixture evaluationgraph.CanonicalFixture) bool {
	for _, record := range fixture.Records {
		if record.Kind == evaluationgraph.RecordHeldOutFeedback {
			return true
		}
	}
	return false
}

func rejectContaminatedScope(scope []ScopeItem) error {
	for _, item := range scope {
		if contaminatedKind(item.Kind) {
			return fmt.Errorf("%w: scope %q %q", ErrScopeContamination, item.Kind, item.ID)
		}
	}
	return nil
}

func contaminatedKind(kind string) bool {
	switch normalizeKind(kind) {
	case "test_room", "overlay", "held_out_feedback", "held_out", "testroom":
		return true
	default:
		return false
	}
}

func normalizeKind(kind string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	kind = strings.ReplaceAll(kind, "-", "_")
	kind = strings.ReplaceAll(kind, " ", "_")
	return kind
}

func validatePolicy(policy PolicyRevisions) error {
	fields := []struct {
		name, value string
	}{
		{"prompt_digest", policy.PromptDigest},
		{"schema_digest", policy.SchemaDigest},
		{"model_digest", policy.ModelDigest},
		{"tool_digest", policy.ToolDigest},
		{"config_digest", policy.ConfigDigest},
		{"grading_policy_digest", policy.GradingPolicyDigest},
	}
	for _, field := range fields {
		if !digestRE.MatchString(field.value) {
			return fmt.Errorf("evaluationfreeze: policy %s must be a sha256 pin", field.name)
		}
	}
	return nil
}

func provenanceDigest(p rawproposal.RawProposalProvenance) (string, error) {
	evidence := make([]any, len(p.SourceEvidence))
	for i, ev := range p.SourceEvidence {
		evidence[i] = map[string]any{
			"id":               ev.ID,
			"trajectory_id":    ev.TrajectoryID,
			"snapshot_id":      ev.SnapshotID,
			"observable_facts": stringsAny(ev.ObservableFacts),
		}
	}
	return contract.DigestOf(map[string]any{
		"proposal_id":                   p.ProposalRef.ProposalID,
		"content_digest":                p.ProposalRef.ContentDigest,
		"uri":                           p.ProposalRef.URI,
		"source_trajectory_id":          p.SourceTrajectoryID,
		"source_snapshot_id":            p.SourceSnapshotID,
		"source_checkpoint_id":          p.SourceCheckpoint.ID,
		"source_checkpoint_snapshot_id": p.SourceCheckpoint.SnapshotID,
		"source_evidence":               evidence,
	})
}

func proposalCanonical(bindings []ProposalBinding) []any {
	out := make([]any, len(bindings))
	for i, b := range bindings {
		out[i] = map[string]any{
			"proposal_id":       b.ProposalID,
			"content_digest":    b.ContentDigest,
			"provenance_digest": b.ProvenanceDigest,
		}
	}
	return out
}

func batchIDs(cut EvidenceCut) []string {
	ids := make([]string, len(cut.Batches))
	for i, batch := range cut.Batches {
		ids[i] = batch.ID
	}
	sort.Strings(ids)
	return ids
}

func stringsAny(values []string) []any {
	out := make([]any, len(values))
	for i := range values {
		out[i] = values[i]
	}
	return out
}

func cloneManifest(in Manifest) Manifest {
	out := in
	out.EvidenceCut.Batches = append([]EvidenceBatch(nil), in.EvidenceCut.Batches...)
	out.Proposals = append([]ProposalBinding(nil), in.Proposals...)
	return out
}
