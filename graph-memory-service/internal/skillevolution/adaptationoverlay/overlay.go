// Package adaptationoverlay is the episode-local ContextualSkillAdaptation
// overlay for Warm Skill Graph Batch (contract §2.2).
//
// Memory must explicitly choose serve_original or create_adaptation.
// A test adaptation binds an exact source revision, a replayable opening
// snapshot, an explicit delta, and a generated body. It is resolvable by
// same-attempt skill_get and is destroyed when the episode closes. The
// overlay never writes the Skill Evolution Ledger, never mutates a
// canonical Skill, and never changes the frozen Evaluation Graph.
package adaptationoverlay

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"river2.dev/graph-memory-service/internal/contract"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationexplore"
	"river2.dev/graph-memory-service/internal/skillevolution/evaluationgraph"
)

const (
	ChoiceServeOriginal    = evaluationexplore.ChoiceServeOriginal
	ChoiceCreateAdaptation = "create_adaptation"

	KindOriginal   = "original"
	KindAdaptation = "adaptation"

	OverlayNamespace = "evaluation-overlay"
)

var (
	ErrDuplicate      = errors.New("adaptation overlay: adaptation without a substantial delta is a duplicate")
	ErrUnknownChoice  = errors.New("adaptation overlay: memory must explicitly choose serve_original or create_adaptation")
	ErrClosed         = errors.New("adaptation overlay: episode is closed")
	ErrScopeMismatch  = errors.New("adaptation overlay: adaptation is resolvable only in the same attempt, manifest, and target scope")
	ErrNotFound       = errors.New("adaptation overlay: skill reference is not resolvable")
	ErrInvalidRequest = errors.New("adaptation overlay: invalid request")
	ErrSourceMissing  = errors.New("adaptation overlay: exact source revision is not in the pinned evaluation view")
)

// Pin is the episode identity an overlay may serve. Adaptations never
// escape this triple.
type Pin struct {
	AttemptID      string
	ManifestDigest string
	TargetAgentID  string
}

// OpeningSnapshot is the replayable, content-addressed capture of the
// opening-only task context bound into an adaptation.
type OpeningSnapshot struct {
	AttemptID      string
	ManifestDigest string
	CheckpointID   string
	TargetAgentID  string
	Query          string
	SeedRefs       []string
	Digest         string
}

// Delta is the explicit adaptation change. GeneratedBody is the full
// adapted text Memory wants to serve; it must differ from the source body.
type Delta struct {
	Guard         string
	Steps         []string
	Rationale     string
	GeneratedBody string
}

// Binding is one immutable episode-local adaptation. It references the
// exact source revision and does not replace it.
type Binding struct {
	AdaptationID    string
	SkillReference  string
	SourceRef       string
	SourceBody      string
	SourceDigest    string
	OpeningSnapshot OpeningSnapshot
	Delta           Delta
	Body            string
	BodyDigest      string
}

// PendingOffer is the directed Skill offer Memory emits after an explicit
// original-or-adaptation choice.
type PendingOffer struct {
	SkillReference string
	CheckpointID   string
	TargetAgentID  string
	Choice         string
	Kind           string
}

// ChooseRequest is one Memory decision. Choice is mandatory.
type ChooseRequest struct {
	Choice    string
	Opening   evaluationexplore.OpeningContext
	SourceRef string
	Delta     Delta
}

// ChooseResult is the offer (and, for create_adaptation, the overlay binding)
// produced by an explicit choice.
type ChooseResult struct {
	Choice     string
	Offer      *PendingOffer
	Adaptation *Binding
}

// SkillGetRequest is the same-attempt skill_get resolution fence.
type SkillGetRequest struct {
	SkillReference string
	AttemptID      string
	ManifestDigest string
	TargetAgentID  string
}

// Resolved is the exact body skill_get may serve. Kind distinguishes
// canonical original Skills from episode-local adaptations.
type Resolved struct {
	SkillReference string
	Kind           string
	Body           string
	BodyDigest     string
	SourceRef      string
}

// ReportEntry lets a reporter count original versus adaptation exposures
// without treating an overlay body as a canonical Skill.
type ReportEntry struct {
	Stage          string
	Kind           string
	SkillReference string
	SourceRef      string
	BodyDigest     string
	Choice         string
}

// Overlay is one test episode's disposable adaptation store. It reads a
// pinned evaluation view and never appends to it.
type Overlay struct {
	mu           sync.Mutex
	view         *evaluationgraph.View
	pin          Pin
	closed       bool
	adaptations  map[string]Binding
	sourceBodies map[string]string
	sourceDigest map[string]string
	report       []ReportEntry
}

// Open pins one attempt/manifest/target onto a frozen evaluation view.
func Open(view *evaluationgraph.View, pin Pin) (*Overlay, error) {
	if view == nil {
		return nil, fmt.Errorf("%w: evaluation view is required", ErrInvalidRequest)
	}
	if strings.TrimSpace(pin.AttemptID) == "" || strings.TrimSpace(pin.ManifestDigest) == "" || strings.TrimSpace(pin.TargetAgentID) == "" {
		return nil, fmt.Errorf("%w: attempt, manifest digest, and target are required", ErrInvalidRequest)
	}
	bodies := make(map[string]string, len(view.Nodes))
	digests := make(map[string]string, len(view.Nodes))
	for _, node := range view.Nodes {
		ref := node.Ref.String()
		bodies[ref] = node.Body
		digests[ref] = bodyDigest(node.Body)
	}
	return &Overlay{
		view:         view,
		pin:          pin,
		adaptations:  make(map[string]Binding),
		sourceBodies: bodies,
		sourceDigest: digests,
	}, nil
}

// Choose records Memory's explicit serve_original or create_adaptation
// decision. create_adaptation fails closed when the delta is not substantial.
func (o *Overlay) Choose(req ChooseRequest) (ChooseResult, error) {
	if o == nil {
		return ChooseResult{}, fmt.Errorf("%w: overlay is not open", ErrInvalidRequest)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ChooseResult{}, ErrClosed
	}
	if err := o.validateOpening(req.Opening); err != nil {
		return ChooseResult{}, err
	}
	switch req.Choice {
	case ChoiceServeOriginal:
		return o.chooseOriginal(req)
	case ChoiceCreateAdaptation:
		return o.chooseAdaptation(req)
	case "":
		return ChooseResult{}, ErrUnknownChoice
	default:
		return ChooseResult{}, fmt.Errorf("%w: %s", ErrUnknownChoice, req.Choice)
	}
}

func (o *Overlay) chooseOriginal(req ChooseRequest) (ChooseResult, error) {
	ref, body, err := o.resolveOriginalSource(req)
	if err != nil {
		return ChooseResult{}, err
	}
	offer := &PendingOffer{
		SkillReference: ref,
		CheckpointID:   req.Opening.CheckpointID,
		TargetAgentID:  o.pin.TargetAgentID,
		Choice:         ChoiceServeOriginal,
		Kind:           KindOriginal,
	}
	digest := bodyDigest(body)
	o.report = append(o.report, ReportEntry{
		Stage: "offered", Kind: KindOriginal, SkillReference: ref,
		BodyDigest: digest, Choice: ChoiceServeOriginal,
	})
	return ChooseResult{Choice: ChoiceServeOriginal, Offer: offer}, nil
}

func (o *Overlay) chooseAdaptation(req ChooseRequest) (ChooseResult, error) {
	sourceRef, sourceBody, err := o.resolveOriginalSource(req)
	if err != nil {
		return ChooseResult{}, err
	}
	if !substantialDelta(sourceBody, req.Delta) {
		return ChooseResult{}, ErrDuplicate
	}
	snapshot, err := sealOpening(o.pin, req.Opening)
	if err != nil {
		return ChooseResult{}, err
	}
	body := req.Delta.GeneratedBody
	binding, err := bindAdaptation(sourceRef, sourceBody, snapshot, req.Delta, body)
	if err != nil {
		return ChooseResult{}, err
	}
	o.adaptations[binding.SkillReference] = binding
	cloned := cloneBinding(binding)
	offer := &PendingOffer{
		SkillReference: binding.SkillReference,
		CheckpointID:   req.Opening.CheckpointID,
		TargetAgentID:  o.pin.TargetAgentID,
		Choice:         ChoiceCreateAdaptation,
		Kind:           KindAdaptation,
	}
	o.report = append(o.report, ReportEntry{
		Stage: "offered", Kind: KindAdaptation, SkillReference: binding.SkillReference,
		SourceRef: sourceRef, BodyDigest: binding.BodyDigest, Choice: ChoiceCreateAdaptation,
	})
	return ChooseResult{Choice: ChoiceCreateAdaptation, Offer: offer, Adaptation: &cloned}, nil
}

func (o *Overlay) resolveOriginalSource(req ChooseRequest) (string, string, error) {
	if strings.TrimSpace(req.SourceRef) != "" {
		ref, err := evaluationexplore.ParseSkillReference(req.SourceRef, evaluationgraph.ScopeEvaluation)
		if err != nil {
			return "", "", err
		}
		node, err := o.view.Get(evaluationgraph.ScopeEvaluation, ref)
		if err != nil {
			if errors.Is(err, evaluationgraph.ErrRevisionNotFound) {
				return "", "", fmt.Errorf("%w: %s", ErrSourceMissing, req.SourceRef)
			}
			return "", "", err
		}
		return node.Ref.String(), node.Body, nil
	}
	explored, err := evaluationexplore.Explore(evaluationexplore.Request{
		View:    o.view,
		Scope:   evaluationgraph.ScopeEvaluation,
		Opening: req.Opening,
	})
	if err != nil {
		return "", "", err
	}
	if explored.SelectedRef == "" {
		return "", "", fmt.Errorf("%w: explore did not select an exact source revision", ErrSourceMissing)
	}
	ref, err := evaluationexplore.ParseSkillReference(explored.SelectedRef, evaluationgraph.ScopeEvaluation)
	if err != nil {
		return "", "", err
	}
	node, err := o.view.Get(evaluationgraph.ScopeEvaluation, ref)
	if err != nil {
		return "", "", err
	}
	return node.Ref.String(), node.Body, nil
}

func (o *Overlay) validateOpening(opening evaluationexplore.OpeningContext) error {
	if strings.TrimSpace(opening.CheckpointID) == "" {
		return fmt.Errorf("%w: opening checkpoint is required", ErrInvalidRequest)
	}
	if opening.TargetAgentID != "" && opening.TargetAgentID != o.pin.TargetAgentID {
		return fmt.Errorf("%w: opening target %q", ErrScopeMismatch, opening.TargetAgentID)
	}
	return nil
}

// SkillGet resolves a manifest-pinned original or an episode-local
// adaptation. Adaptations require the exact opening pin and a live episode.
func (o *Overlay) SkillGet(req SkillGetRequest) (Resolved, error) {
	if o == nil {
		return Resolved{}, fmt.Errorf("%w: overlay is not open", ErrInvalidRequest)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if strings.TrimSpace(req.SkillReference) == "" {
		return Resolved{}, fmt.Errorf("%w: skill reference is required", ErrInvalidRequest)
	}
	if isOverlayReference(req.SkillReference) {
		return o.resolveAdaptation(req)
	}
	return o.resolveCanonical(req)
}

func (o *Overlay) resolveAdaptation(req SkillGetRequest) (Resolved, error) {
	if o.closed {
		return Resolved{}, ErrClosed
	}
	if req.AttemptID != o.pin.AttemptID || req.ManifestDigest != o.pin.ManifestDigest || req.TargetAgentID != o.pin.TargetAgentID {
		return Resolved{}, ErrScopeMismatch
	}
	binding, ok := o.adaptations[req.SkillReference]
	if !ok {
		return Resolved{}, fmt.Errorf("%w: %s", ErrNotFound, req.SkillReference)
	}
	resolved := Resolved{
		SkillReference: binding.SkillReference,
		Kind:           KindAdaptation,
		Body:           binding.Body,
		BodyDigest:     binding.BodyDigest,
		SourceRef:      binding.SourceRef,
	}
	o.report = append(o.report, ReportEntry{
		Stage: "resolved", Kind: KindAdaptation, SkillReference: binding.SkillReference,
		SourceRef: binding.SourceRef, BodyDigest: binding.BodyDigest, Choice: ChoiceCreateAdaptation,
	})
	return resolved, nil
}

func (o *Overlay) resolveCanonical(req SkillGetRequest) (Resolved, error) {
	if req.ManifestDigest != o.pin.ManifestDigest {
		return Resolved{}, ErrScopeMismatch
	}
	ref, err := evaluationexplore.ParseSkillReference(req.SkillReference, evaluationgraph.ScopeEvaluation)
	if err != nil {
		return Resolved{}, err
	}
	node, err := o.view.Get(evaluationgraph.ScopeEvaluation, ref)
	if err != nil {
		if errors.Is(err, evaluationgraph.ErrRevisionNotFound) {
			return Resolved{}, fmt.Errorf("%w: %s", ErrNotFound, req.SkillReference)
		}
		return Resolved{}, err
	}
	digest := bodyDigest(node.Body)
	resolved := Resolved{
		SkillReference: node.Ref.String(),
		Kind:           KindOriginal,
		Body:           node.Body,
		BodyDigest:     digest,
	}
	o.report = append(o.report, ReportEntry{
		Stage: "resolved", Kind: KindOriginal, SkillReference: node.Ref.String(),
		BodyDigest: digest, Choice: ChoiceServeOriginal,
	})
	return resolved, nil
}

// CloseEpisode destroys every local adaptation. Later skill_get of an
// adaptation fails closed; canonical Skills remain in the frozen view.
func (o *Overlay) CloseEpisode() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.adaptations = make(map[string]Binding)
}

// Report is the original-versus-adaptation exposure list for this episode.
func (o *Overlay) Report() []ReportEntry {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]ReportEntry, len(o.report))
	copy(out, o.report)
	return out
}

// SourceRevision returns the canonical Skill still stored in the pinned
// view. Overlay creation must not change its body or digest.
func (o *Overlay) SourceRevision(ref string) (evaluationgraph.Node, string, error) {
	if o == nil {
		return evaluationgraph.Node{}, "", fmt.Errorf("%w: overlay is not open", ErrInvalidRequest)
	}
	parsed, err := evaluationexplore.ParseSkillReference(ref, evaluationgraph.ScopeEvaluation)
	if err != nil {
		return evaluationgraph.Node{}, "", err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	node, err := o.view.Get(evaluationgraph.ScopeEvaluation, parsed)
	if err != nil {
		return evaluationgraph.Node{}, "", err
	}
	return node, bodyDigest(node.Body), nil
}

// SourceSnapshot is the body/digest captured when the overlay opened. It
// is compared against the live view to prove the source was not rewritten.
func (o *Overlay) SourceSnapshot(ref string) (body, digest string, ok bool) {
	if o == nil {
		return "", "", false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	body, ok = o.sourceBodies[ref]
	if !ok {
		return "", "", false
	}
	return body, o.sourceDigest[ref], true
}

// ViewDigest is the pinned Evaluation Graph projection digest. The overlay
// never rebuilds or replaces the view, so this cannot change.
func (o *Overlay) ViewDigest() string {
	if o == nil || o.view == nil {
		return ""
	}
	return o.view.ProjectionDigest
}

func sealOpening(pin Pin, opening evaluationexplore.OpeningContext) (OpeningSnapshot, error) {
	seeds := append([]string(nil), opening.SeedRefs...)
	sort.Strings(seeds)
	target := opening.TargetAgentID
	if target == "" {
		target = pin.TargetAgentID
	}
	canonical := map[string]any{
		"attempt_id":      pin.AttemptID,
		"manifest_digest": pin.ManifestDigest,
		"checkpoint_id":   opening.CheckpointID,
		"target_agent_id": target,
		"query":           opening.Query,
		"seed_refs":       stringsAny(seeds),
	}
	digest, err := contract.DigestOf(canonical)
	if err != nil {
		return OpeningSnapshot{}, fmt.Errorf("adaptation overlay: opening snapshot cannot enter canonical hashed core: %w", err)
	}
	return OpeningSnapshot{
		AttemptID:      pin.AttemptID,
		ManifestDigest: pin.ManifestDigest,
		CheckpointID:   opening.CheckpointID,
		TargetAgentID:  target,
		Query:          opening.Query,
		SeedRefs:       seeds,
		Digest:         digest,
	}, nil
}

func bindAdaptation(sourceRef, sourceBody string, snapshot OpeningSnapshot, delta Delta, body string) (Binding, error) {
	srcDigest := bodyDigest(sourceBody)
	canonical := map[string]any{
		"source_ref":              sourceRef,
		"source_digest":           srcDigest,
		"opening_snapshot_digest": snapshot.Digest,
		"delta": map[string]any{
			"guard":     delta.Guard,
			"steps":     stringsAny(delta.Steps),
			"rationale": delta.Rationale,
		},
		"body": body,
	}
	idDigest, err := contract.DigestOf(canonical)
	if err != nil {
		return Binding{}, fmt.Errorf("adaptation overlay: adaptation cannot enter canonical hashed core: %w", err)
	}
	id := "adapt-" + strings.TrimPrefix(idDigest, "sha256:")[:32]
	return Binding{
		AdaptationID:    id,
		SkillReference:  fmt.Sprintf("skill://%s/%s@1", OverlayNamespace, id),
		SourceRef:       sourceRef,
		SourceBody:      sourceBody,
		SourceDigest:    srcDigest,
		OpeningSnapshot: snapshot,
		Delta:           cloneDelta(delta),
		Body:            body,
		BodyDigest:      bodyDigest(body),
	}, nil
}

func substantialDelta(sourceBody string, delta Delta) bool {
	body := strings.TrimSpace(delta.GeneratedBody)
	if body == "" || body == strings.TrimSpace(sourceBody) {
		return false
	}
	if strings.TrimSpace(delta.Guard) == "" && strings.TrimSpace(delta.Rationale) == "" && !hasNonEmptyStep(delta.Steps) {
		return false
	}
	return true
}

func hasNonEmptyStep(steps []string) bool {
	for _, step := range steps {
		if strings.TrimSpace(step) != "" {
			return true
		}
	}
	return false
}

func isOverlayReference(raw string) bool {
	return strings.HasPrefix(strings.TrimSpace(raw), "skill://"+OverlayNamespace+"/")
}

func bodyDigest(body string) string {
	return contract.DigestBytes([]byte(body))
}

func stringsAny(values []string) []any {
	out := make([]any, len(values))
	for i := range values {
		out[i] = values[i]
	}
	return out
}

func cloneDelta(in Delta) Delta {
	out := in
	if in.Steps != nil {
		out.Steps = append([]string(nil), in.Steps...)
	}
	return out
}

func cloneBinding(in Binding) Binding {
	out := in
	out.Delta = cloneDelta(in.Delta)
	out.OpeningSnapshot.SeedRefs = append([]string(nil), in.OpeningSnapshot.SeedRefs...)
	return out
}
