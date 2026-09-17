package skillfence

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	ToolSkillGet      = "skill_get"
	ToolSkillFeedback = "skill_feedback"

	FenceGet      = "skill_get"
	FenceFeedback = "skill_feedback"

	StageServed   = "served"
	StageRejected = "rejected"

	ReasonResolutionError = "resolution_error"
	FailureLatestAlias    = "latest_alias"
	FailureLocalPath      = "local_path"
	FailureCrossManifest  = "cross_manifest_revision"
	FailurePermission     = "permission_mismatch"
	FailureDigestMismatch = "digest_mismatch"
	FailureOrdinaryTool   = "ordinary_tool_fenced"

	EvaluationNamespace = "evaluation"
)

var (
	ErrResolverRequired          = errors.New("skill fence: skill resolver is required")
	ErrSessionRequired           = errors.New("skill fence: target pi session is required")
	ErrMemoryRequired            = errors.New("skill fence: memory notifier is required")
	ErrOfferRequired             = errors.New("skill fence: offer binding is required")
	ErrOfferNotBound             = errors.New("skill fence: no offer is bound")
	ErrLatestAlias               = errors.New("skill fence: latest alias is not an exact skill reference")
	ErrLocalPath                 = errors.New("skill fence: local path is not an exact skill reference")
	ErrCrossManifestRevision     = errors.New("skill fence: skill revision is not in the pinned manifest")
	ErrPermissionMismatch        = errors.New("skill fence: skill get permission mismatch")
	ErrDigestMismatch            = errors.New("skill fence: captured body digest does not match the pinned view")
	ErrOrdinaryToolFenced        = errors.New("skill fence: ordinary tools are fenced until protocol disposition")
	ErrFeedbackRequired          = errors.New("skill fence: skill_get succeeded; feedback fence is required")
	ErrSkillGetRequired          = errors.New("skill fence: skill_get is required before other tools")
	ErrAgentMismatch             = errors.New("skill fence: tool call agent does not match the offer recipient")
	ErrPinnedReferenceRequired   = errors.New("skill fence: offer must pin an exact skill reference")
	ErrManifestDigestRequired    = errors.New("skill fence: offer must pin a freeze manifest digest")
	ErrSkillGetReferenceMismatch = errors.New("skill fence: skill_get must request the offer-pinned exact reference")
)

var exactSkillRef = regexp.MustCompile(`^skill://([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+)@([1-9][0-9]*)$`)

// Offer is the Host-owned directed offer the resume fence is bound to.
type Offer struct {
	OfferID        string
	AgentID        string
	SkillReference string
	ManifestDigest string
}

// ToolCall is one Pi-issued tool request observed by the fence.
type ToolCall struct {
	CallID         string
	Name           string
	AgentID        string
	SkillReference string
}

// Result is the fence decision for one tool call.
type Result struct {
	Allowed    bool
	Duplicate  bool
	Fence      string
	ReasonCode string
	Failure    string
	Served     *ServedFact
}

// ServedFact is the Host-authored proof that exact body bytes entered
// the target Pi session. accepted/rejected/adopted are never implied.
type ServedFact struct {
	OfferID        string
	AgentID        string
	SkillReference string
	ToolCallID     string
	BodyDigest     string
	ManifestDigest string
	Stage          string
}

// ResolvedView is the GMS-owned exact body the resolver returns for a
// manifest-pinned Skill Reference.
type ResolvedView struct {
	Reference      string
	Body           []byte
	ViewDigest     string
	ManifestDigest string
}

// ResolveRequest is the Host seam for a manifest-pinned exact get.
type ResolveRequest struct {
	ManifestDigest string
	SkillReference string
	AgentID        string
}

// SkillResolver is the Host seam for GMS exact Skill resolution.
// Production binds a GMS client; tests bind the testdata adapter.
type SkillResolver interface {
	Resolve(ctx context.Context, req ResolveRequest) (ResolvedView, error)
}

// PiSession is the exact target session that must receive body bytes
// before a served fact may be recorded.
type PiSession interface {
	AgentID() string
	InjectExactBody(ctx context.Context, body []byte) error
	CapturedBody() []byte
}

// ResolutionNotice is the structured Memory notification for a
// resolution/serve failure. It is not a rejected disposition.
type ResolutionNotice struct {
	OfferID        string
	AgentID        string
	SkillReference string
	ReasonCode     string
	Failure        string
	Detail         string
}

// MemoryNotifier is the Host seam that tells Memory a resolution_error
// occurred. Implementations must not mint served or rejected facts.
type MemoryNotifier interface {
	NotifyResolutionError(ctx context.Context, notice ResolutionNotice) error
}

type InteractionRecord struct {
	Stage      string
	OfferID    string
	ToolCallID string
	BodyDigest string
}

// Coordinator owns the post-resume tool fence, served facts, and
// resolution_error notices for one bound offer.
type Coordinator struct {
	resolver SkillResolver
	session  PiSession
	memory   MemoryNotifier

	mu       sync.Mutex
	offer    Offer
	bound    bool
	fence    string
	served   []ServedFact
	rejected []InteractionRecord
	notices  []ResolutionNotice
	byCall   map[string]Result
}

func NewCoordinator(resolver SkillResolver, session PiSession, memory MemoryNotifier) (*Coordinator, error) {
	if resolver == nil {
		return nil, ErrResolverRequired
	}
	if session == nil {
		return nil, ErrSessionRequired
	}
	if memory == nil {
		return nil, ErrMemoryRequired
	}
	return &Coordinator{
		resolver: resolver,
		session:  session,
		memory:   memory,
		byCall:   map[string]Result{},
	}, nil
}

// BindOffer starts the skill_get fence for one resumed task offer.
func (c *Coordinator) BindOffer(offer Offer) error {
	if strings.TrimSpace(offer.OfferID) == "" || strings.TrimSpace(offer.AgentID) == "" {
		return ErrOfferRequired
	}
	if strings.TrimSpace(offer.ManifestDigest) == "" {
		return ErrManifestDigestRequired
	}
	if _, err := ParseExactSkillReference(offer.SkillReference); err != nil {
		return fmt.Errorf("%w: %v", ErrPinnedReferenceRequired, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offer = offer
	c.bound = true
	c.fence = FenceGet
	return nil
}

func (c *Coordinator) Fence() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fence
}

func (c *Coordinator) Served() []ServedFact {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ServedFact(nil), c.served...)
}

func (c *Coordinator) Rejected() []InteractionRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]InteractionRecord(nil), c.rejected...)
}

func (c *Coordinator) Notices() []ResolutionNotice {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ResolutionNotice(nil), c.notices...)
}

// HandleToolCall is the exclusive post-resume tool gate. Ordinary tools
// are refused until skill_get succeeds and the fence moves to feedback.
func (c *Coordinator) HandleToolCall(ctx context.Context, call ToolCall) (Result, error) {
	c.mu.Lock()
	if !c.bound {
		c.mu.Unlock()
		return Result{}, ErrOfferNotBound
	}
	if call.CallID != "" {
		if existing, ok := c.byCall[call.CallID]; ok {
			c.mu.Unlock()
			existing.Duplicate = true
			return existing, existing.resultError()
		}
	}
	offer := c.offer
	fence := c.fence
	c.mu.Unlock()

	result, err := c.dispatch(ctx, offer, fence, call)

	c.mu.Lock()
	defer c.mu.Unlock()
	if call.CallID != "" {
		stored := result
		stored.Duplicate = false
		c.byCall[call.CallID] = stored
	}
	return result, err
}

func (c *Coordinator) dispatch(ctx context.Context, offer Offer, fence string, call ToolCall) (Result, error) {
	if call.AgentID != "" && call.AgentID != offer.AgentID {
		return c.failResolution(ctx, offer, call, ErrPermissionMismatch, FailurePermission)
	}
	if c.session.AgentID() != "" && c.session.AgentID() != offer.AgentID {
		return c.failResolution(ctx, offer, call, ErrPermissionMismatch, FailurePermission)
	}

	switch fence {
	case FenceGet:
		if call.Name != ToolSkillGet {
			return Result{Fence: FenceGet, ReasonCode: FailureOrdinaryTool}, ErrSkillGetRequired
		}
		return c.handleSkillGet(ctx, offer, call)
	case FenceFeedback:
		if call.Name == ToolSkillGet {
			return Result{Fence: FenceFeedback, ReasonCode: FailureOrdinaryTool}, ErrFeedbackRequired
		}
		if call.Name != ToolSkillFeedback {
			return Result{Fence: FenceFeedback, ReasonCode: FailureOrdinaryTool}, ErrOrdinaryToolFenced
		}
		return Result{Allowed: true, Fence: FenceFeedback}, nil
	default:
		return Result{}, ErrOfferNotBound
	}
}

func (c *Coordinator) handleSkillGet(ctx context.Context, offer Offer, call ToolCall) (Result, error) {
	ref := strings.TrimSpace(call.SkillReference)
	if ref == "" {
		ref = offer.SkillReference
	}
	if _, err := ParseExactSkillReference(ref); err != nil {
		return c.failResolution(ctx, offer, call, err, failureOf(err))
	}
	if ref != offer.SkillReference {
		return c.failResolution(ctx, offer, call, fmt.Errorf("%w: got %s, pinned %s", ErrSkillGetReferenceMismatch, ref, offer.SkillReference), FailureCrossManifest)
	}

	resolved, err := c.resolver.Resolve(ctx, ResolveRequest{
		ManifestDigest: offer.ManifestDigest,
		SkillReference: ref,
		AgentID:        offer.AgentID,
	})
	if err != nil {
		return c.failResolution(ctx, offer, call, classifyResolve(err), failureOf(classifyResolve(err)))
	}
	if resolved.ManifestDigest != "" && resolved.ManifestDigest != offer.ManifestDigest {
		return c.failResolution(ctx, offer, call, ErrCrossManifestRevision, FailureCrossManifest)
	}
	if resolved.ViewDigest == "" || BodyDigest(resolved.Body) != resolved.ViewDigest {
		return c.failResolution(ctx, offer, call, ErrDigestMismatch, FailureDigestMismatch)
	}

	if err := c.session.InjectExactBody(ctx, append([]byte(nil), resolved.Body...)); err != nil {
		return c.failResolution(ctx, offer, call, err, FailureDigestMismatch)
	}
	captured := c.session.CapturedBody()
	capturedDigest := BodyDigest(captured)
	if capturedDigest != resolved.ViewDigest {
		return c.failResolution(ctx, offer, call, ErrDigestMismatch, FailureDigestMismatch)
	}

	served := ServedFact{
		OfferID:        offer.OfferID,
		AgentID:        offer.AgentID,
		SkillReference: ref,
		ToolCallID:     call.CallID,
		BodyDigest:     capturedDigest,
		ManifestDigest: offer.ManifestDigest,
		Stage:          StageServed,
	}

	c.mu.Lock()
	c.served = append(c.served, served)
	c.fence = FenceFeedback
	c.mu.Unlock()

	copyServed := served
	return Result{Allowed: true, Fence: FenceFeedback, Served: &copyServed}, nil
}

func (c *Coordinator) failResolution(ctx context.Context, offer Offer, call ToolCall, err error, failure string) (Result, error) {
	notice := ResolutionNotice{
		OfferID:        offer.OfferID,
		AgentID:        offer.AgentID,
		SkillReference: firstNonEmpty(call.SkillReference, offer.SkillReference),
		ReasonCode:     ReasonResolutionError,
		Failure:        failure,
		Detail:         err.Error(),
	}
	_ = c.memory.NotifyResolutionError(ctx, notice)

	c.mu.Lock()
	c.notices = append(c.notices, notice)
	c.mu.Unlock()

	return Result{Fence: FenceGet, ReasonCode: ReasonResolutionError, Failure: failure}, err
}

func (r Result) resultError() error {
	if r.Allowed {
		return nil
	}
	switch r.Failure {
	case FailureLatestAlias:
		return ErrLatestAlias
	case FailureLocalPath:
		return ErrLocalPath
	case FailureCrossManifest:
		return ErrCrossManifestRevision
	case FailurePermission:
		return ErrPermissionMismatch
	case FailureDigestMismatch:
		return ErrDigestMismatch
	case FailureOrdinaryTool:
		if r.Fence == FenceFeedback {
			return ErrOrdinaryToolFenced
		}
		return ErrSkillGetRequired
	default:
		if r.ReasonCode == FailureOrdinaryTool {
			if r.Fence == FenceFeedback {
				return ErrFeedbackRequired
			}
			return ErrSkillGetRequired
		}
		return nil
	}
}

// ParseExactSkillReference accepts only skill://<namespace>/<lineage>@<revision>
// with a numeric revision. Latest aliases and local paths fail closed.
func ParseExactSkillReference(raw string) (ref struct {
	Namespace string
	Lineage   string
	Revision  uint64
}, err error) {
	trimmed := strings.TrimSpace(raw)
	if looksLikeLocalPath(trimmed) {
		return ref, fmt.Errorf("%w: %s", ErrLocalPath, raw)
	}
	if strings.Contains(strings.ToLower(trimmed), "@latest") || isUnpinnedSkillURI(trimmed) {
		return ref, fmt.Errorf("%w: %s", ErrLatestAlias, raw)
	}
	match := exactSkillRef.FindStringSubmatch(trimmed)
	if match == nil {
		if looksLikeLocalPath(trimmed) {
			return ref, fmt.Errorf("%w: %s", ErrLocalPath, raw)
		}
		return ref, fmt.Errorf("%w: %s", ErrLatestAlias, raw)
	}
	if match[1] != EvaluationNamespace {
		return ref, fmt.Errorf("%w: %s", ErrPermissionMismatch, raw)
	}
	revision, perr := strconv.ParseUint(match[3], 10, 64)
	if perr != nil {
		return ref, fmt.Errorf("%w: %s", ErrLatestAlias, raw)
	}
	ref.Namespace = match[1]
	ref.Lineage = match[2]
	ref.Revision = revision
	return ref, nil
}

func looksLikeLocalPath(raw string) bool {
	if strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "./") || strings.HasPrefix(raw, "../") {
		return true
	}
	if strings.HasPrefix(raw, "file:") || strings.HasPrefix(raw, "file://") {
		return true
	}
	return strings.Contains(raw, "\\")
}

func isUnpinnedSkillURI(raw string) bool {
	if !strings.HasPrefix(raw, "skill://") {
		return false
	}
	return !strings.Contains(raw, "@")
}

func classifyResolve(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrLatestAlias), errors.Is(err, ErrLocalPath),
		errors.Is(err, ErrCrossManifestRevision), errors.Is(err, ErrPermissionMismatch),
		errors.Is(err, ErrDigestMismatch):
		return err
	default:
		msg := strings.ToLower(err.Error())
		switch {
		case strings.Contains(msg, "latest"):
			return fmt.Errorf("%w: %v", ErrLatestAlias, err)
		case strings.Contains(msg, "local") || strings.Contains(msg, "file:"):
			return fmt.Errorf("%w: %v", ErrLocalPath, err)
		case strings.Contains(msg, "permission") || strings.Contains(msg, "scope") || strings.Contains(msg, "denied"):
			return fmt.Errorf("%w: %v", ErrPermissionMismatch, err)
		case strings.Contains(msg, "digest"):
			return fmt.Errorf("%w: %v", ErrDigestMismatch, err)
		default:
			return fmt.Errorf("%w: %v", ErrCrossManifestRevision, err)
		}
	}
}

func failureOf(err error) string {
	switch {
	case errors.Is(err, ErrLatestAlias):
		return FailureLatestAlias
	case errors.Is(err, ErrLocalPath):
		return FailureLocalPath
	case errors.Is(err, ErrPermissionMismatch):
		return FailurePermission
	case errors.Is(err, ErrDigestMismatch):
		return FailureDigestMismatch
	default:
		return FailureCrossManifest
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
