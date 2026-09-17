package skillfence

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gmsview "river2.dev/graph-memory-service/testdata/gmsview"
)

func TestLatestLocalPathCrossManifestPermissionAndDigestMismatchFail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cases := []struct {
		name    string
		setup   func(*testing.T, *harness)
		call    ToolCall
		wantErr error
	}{
		{
			name:    "latest_alias",
			call:    ToolCall{CallID: "get-latest", Name: ToolSkillGet, AgentID: "task-agent", SkillReference: "skill://evaluation/alpha@latest"},
			wantErr: ErrLatestAlias,
		},
		{
			name:    "unpinned_latest",
			call:    ToolCall{CallID: "get-unpinned", Name: ToolSkillGet, AgentID: "task-agent", SkillReference: "skill://evaluation/alpha"},
			wantErr: ErrLatestAlias,
		},
		{
			name:    "local_absolute_path",
			call:    ToolCall{CallID: "get-abs", Name: ToolSkillGet, AgentID: "task-agent", SkillReference: "/tmp/skills/alpha.md"},
			wantErr: ErrLocalPath,
		},
		{
			name:    "local_relative_path",
			call:    ToolCall{CallID: "get-rel", Name: ToolSkillGet, AgentID: "task-agent", SkillReference: "./skills/alpha.md"},
			wantErr: ErrLocalPath,
		},
		{
			name:    "local_file_uri",
			call:    ToolCall{CallID: "get-file", Name: ToolSkillGet, AgentID: "task-agent", SkillReference: "file:///tmp/skills/alpha.md"},
			wantErr: ErrLocalPath,
		},
		{
			name: "cross_manifest_revision",
			setup: func(t *testing.T, h *harness) {
				t.Helper()
				if err := h.coord.BindOffer(Offer{
					OfferID:        "offer-cross",
					AgentID:        "task-agent",
					SkillReference: h.adapter.CrossManifestRef(),
					ManifestDigest: h.adapter.ManifestDigest,
				}); err != nil {
					t.Fatalf("bind cross-manifest offer: %v", err)
				}
			},
			call:    ToolCall{CallID: "get-cross", Name: ToolSkillGet, AgentID: "task-agent"},
			wantErr: ErrCrossManifestRevision,
		},
		{
			name: "cross_manifest_digest",
			setup: func(t *testing.T, h *harness) {
				t.Helper()
				if err := h.coord.BindOffer(Offer{
					OfferID:        "offer-wrong-manifest",
					AgentID:        "task-agent",
					SkillReference: h.adapter.PinnedRef,
					ManifestDigest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
				}); err != nil {
					t.Fatalf("bind wrong-manifest offer: %v", err)
				}
			},
			call:    ToolCall{CallID: "get-wrong-manifest", Name: ToolSkillGet, AgentID: "task-agent"},
			wantErr: ErrCrossManifestRevision,
		},
		{
			name:    "permission_wrong_agent",
			call:    ToolCall{CallID: "get-other", Name: ToolSkillGet, AgentID: "other-agent", SkillReference: ""},
			wantErr: ErrPermissionMismatch,
		},
		{
			name:    "permission_runtime_namespace",
			call:    ToolCall{CallID: "get-runtime", Name: ToolSkillGet, AgentID: "task-agent", SkillReference: "skill://runtime/alpha@1"},
			wantErr: ErrPermissionMismatch,
		},
		{
			name: "permission_gms_scope",
			setup: func(t *testing.T, h *harness) {
				t.Helper()
				h.adapter.Authorize("task-agent", false)
			},
			call:    ToolCall{CallID: "get-denied", Name: ToolSkillGet, AgentID: "task-agent"},
			wantErr: ErrPermissionMismatch,
		},
		{
			name: "digest_mismatch",
			setup: func(_ *testing.T, h *harness) {
				h.session.CorruptCapture = func(body []byte) []byte {
					out := append([]byte(nil), body...)
					if len(out) == 0 {
						return []byte("corrupted")
					}
					out[0] ^= 0xff
					return out
				}
			},
			call:    ToolCall{CallID: "get-digest", Name: ToolSkillGet, AgentID: "task-agent"},
			wantErr: ErrDigestMismatch,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHarness(t)
			if tc.setup != nil {
				tc.setup(t, h)
			}
			_, err := h.coord.HandleToolCall(ctx, tc.call)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("HandleToolCall() error = %v, want %v", err, tc.wantErr)
			}
			assertNoServedOrRejected(t, h.coord)
			if h.coord.Fence() != FenceGet {
				t.Fatalf("fence = %q, want %q after a closed failure", h.coord.Fence(), FenceGet)
			}
		})
	}
}

func TestResolutionFailureDoesNotCreateServedOrRejectedAndNotifiesMemory(t *testing.T) {
	t.Parallel()
	h := newTestHarness(t)
	ctx := context.Background()

	result, err := h.coord.HandleToolCall(ctx, ToolCall{
		CallID:         "get-latest",
		Name:           ToolSkillGet,
		AgentID:        "task-agent",
		SkillReference: "skill://evaluation/alpha@latest",
	})
	if !errors.Is(err, ErrLatestAlias) {
		t.Fatalf("HandleToolCall() error = %v, want latest alias", err)
	}
	if result.Allowed || result.Served != nil || result.ReasonCode != ReasonResolutionError {
		t.Fatalf("result = %#v, want a resolution_error without a served fact", result)
	}
	assertNoServedOrRejected(t, h.coord)

	notices := h.memory.Notices()
	if len(notices) != 1 {
		t.Fatalf("memory notices = %#v, want exactly one structured resolution_error", notices)
	}
	notice := notices[0]
	if notice.ReasonCode != ReasonResolutionError || notice.Failure != FailureLatestAlias {
		t.Fatalf("notice = %#v, want reason_code=%s failure=%s", notice, ReasonResolutionError, FailureLatestAlias)
	}
	if notice.OfferID != "offer-1" || notice.AgentID != "task-agent" {
		t.Fatalf("notice routing = %#v, want offer-1 / task-agent", notice)
	}
	if notice.SkillReference == "" || notice.Detail == "" {
		t.Fatalf("notice missing exact reference or detail: %#v", notice)
	}

	hostNotices := h.coord.Notices()
	if len(hostNotices) != 1 || hostNotices[0].ReasonCode != ReasonResolutionError {
		t.Fatalf("host notices = %#v, want the same structured resolution_error", hostNotices)
	}
}

func TestSuccessPiCapturedDigestEqualsGMSViewDigestEqualsHostServedDigest(t *testing.T) {
	t.Parallel()
	h := newTestHarness(t)
	ctx := context.Background()

	result, err := h.coord.HandleToolCall(ctx, ToolCall{
		CallID:  "get-1",
		Name:    ToolSkillGet,
		AgentID: "task-agent",
	})
	if err != nil {
		t.Fatalf("skill_get: %v", err)
	}
	if !result.Allowed || result.Served == nil || result.Fence != FenceFeedback {
		t.Fatalf("result = %#v, want allowed served fact and feedback fence", result)
	}

	captured := h.session.CapturedBody()
	if string(captured) != string(h.adapter.Body) {
		t.Fatalf("pi captured %q, want exact GMS body %q", captured, h.adapter.Body)
	}
	piDigest := BodyDigest(captured)
	gmsDigest := h.adapter.ViewDigest
	hostDigest := result.Served.BodyDigest
	if piDigest == "" || piDigest != gmsDigest || gmsDigest != hostDigest {
		t.Fatalf("digests diverge: pi=%s gms=%s host=%s", piDigest, gmsDigest, hostDigest)
	}
	if hostDigest != BodyDigest(h.adapter.Body) {
		t.Fatalf("host served digest %s, want sha256 of exact GMS body bytes", hostDigest)
	}

	served := h.coord.Served()
	if len(served) != 1 || served[0].Stage != StageServed || served[0].BodyDigest != hostDigest {
		t.Fatalf("served = %#v, want one Host-authored served fact", served)
	}
	if served[0].SkillReference != h.adapter.PinnedRef || served[0].ManifestDigest != h.adapter.ManifestDigest {
		t.Fatalf("served identity = %#v, want pinned %s on %s", served[0], h.adapter.PinnedRef, h.adapter.ManifestDigest)
	}
	if len(h.coord.Rejected()) != 0 {
		t.Fatalf("rejected = %#v, want none on a successful serve", h.coord.Rejected())
	}
}

func TestOrdinaryToolsFencedBeforeSkillGetAndFeedbackFenceAfterSuccess(t *testing.T) {
	t.Parallel()
	h := newTestHarness(t)
	ctx := context.Background()

	if h.coord.Fence() != FenceGet {
		t.Fatalf("initial fence = %q, want %q", h.coord.Fence(), FenceGet)
	}
	_, err := h.coord.HandleToolCall(ctx, ToolCall{CallID: "bash-1", Name: "bash", AgentID: "task-agent"})
	if !errors.Is(err, ErrSkillGetRequired) {
		t.Fatalf("ordinary tool before skill_get error = %v, want skill_get required", err)
	}
	_, err = h.coord.HandleToolCall(ctx, ToolCall{CallID: "feedback-early", Name: ToolSkillFeedback, AgentID: "task-agent"})
	if !errors.Is(err, ErrSkillGetRequired) {
		t.Fatalf("skill_feedback before skill_get error = %v, want skill_get required", err)
	}
	assertNoServedOrRejected(t, h.coord)
	if len(h.memory.Notices()) != 0 {
		t.Fatalf("ordinary-tool fence notified memory: %#v", h.memory.Notices())
	}

	got, err := h.coord.HandleToolCall(ctx, ToolCall{CallID: "get-1", Name: ToolSkillGet, AgentID: "task-agent"})
	if err != nil || !got.Allowed || got.Fence != FenceFeedback {
		t.Fatalf("skill_get = %#v err=%v, want success and feedback fence", got, err)
	}
	if h.coord.Fence() != FenceFeedback {
		t.Fatalf("fence after skill_get = %q, want %q", h.coord.Fence(), FenceFeedback)
	}

	_, err = h.coord.HandleToolCall(ctx, ToolCall{CallID: "bash-2", Name: "bash", AgentID: "task-agent"})
	if !errors.Is(err, ErrOrdinaryToolFenced) {
		t.Fatalf("ordinary tool after skill_get error = %v, want feedback fence", err)
	}
	feedback, err := h.coord.HandleToolCall(ctx, ToolCall{CallID: "feedback-1", Name: ToolSkillFeedback, AgentID: "task-agent"})
	if err != nil || !feedback.Allowed || feedback.Fence != FenceFeedback {
		t.Fatalf("skill_feedback = %#v err=%v, want allowed inside the feedback fence", feedback, err)
	}
	if len(h.coord.Served()) != 1 {
		t.Fatalf("served after feedback fence = %#v, want the original served fact only", h.coord.Served())
	}
}

func TestSameToolCallReplayDoesNotDuplicateServedRecord(t *testing.T) {
	t.Parallel()
	h := newTestHarness(t)
	ctx := context.Background()
	call := ToolCall{CallID: "get-replay", Name: ToolSkillGet, AgentID: "task-agent"}

	first, err := h.coord.HandleToolCall(ctx, call)
	if err != nil || first.Served == nil || first.Duplicate {
		t.Fatalf("first skill_get = %#v err=%v, want a new served fact", first, err)
	}
	second, err := h.coord.HandleToolCall(ctx, call)
	if err != nil || !second.Duplicate || second.Served == nil {
		t.Fatalf("replay = %#v err=%v, want the original served fact", second, err)
	}
	if first.Served.BodyDigest != second.Served.BodyDigest || first.Served.ToolCallID != second.Served.ToolCallID {
		t.Fatalf("replay served %#v, want the same fact as %#v", second.Served, first.Served)
	}
	if got := h.coord.Served(); len(got) != 1 {
		t.Fatalf("served records = %#v, want exactly one Host-authored fact", got)
	}
}

type harness struct {
	adapter *gmsview.Adapter
	session *ScriptedSession
	memory  *RecordingNotifier
	coord   *Coordinator
}

func newTestHarness(t *testing.T) *harness {
	t.Helper()
	adapter, err := gmsview.New()
	if err != nil {
		t.Fatalf("gms freeze/graph adapter: %v", err)
	}
	session := NewScriptedSession("task-agent")
	memory := &RecordingNotifier{}
	coord, err := NewCoordinator(&liveResolver{adapter: adapter}, session, memory)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	if err := coord.BindOffer(Offer{
		OfferID:        "offer-1",
		AgentID:        "task-agent",
		SkillReference: adapter.PinnedRef,
		ManifestDigest: adapter.ManifestDigest,
	}); err != nil {
		t.Fatalf("bind offer: %v", err)
	}
	return &harness{adapter: adapter, session: session, memory: memory, coord: coord}
}

func assertNoServedOrRejected(t *testing.T, coord *Coordinator) {
	t.Helper()
	if got := coord.Served(); len(got) != 0 {
		t.Fatalf("served = %#v, want none", got)
	}
	if got := coord.Rejected(); len(got) != 0 {
		t.Fatalf("rejected = %#v, want none", got)
	}
}

type liveResolver struct {
	adapter *gmsview.Adapter
}

func (l *liveResolver) Resolve(ctx context.Context, req ResolveRequest) (ResolvedView, error) {
	view, err := l.adapter.Resolve(ctx, gmsview.Request{
		ManifestDigest: req.ManifestDigest,
		SkillReference: req.SkillReference,
		AgentID:        req.AgentID,
	})
	if err != nil {
		switch {
		case errors.Is(err, gmsview.ErrPermission):
			return ResolvedView{}, fmt.Errorf("%w: %v", ErrPermissionMismatch, err)
		case errors.Is(err, gmsview.ErrDigest):
			return ResolvedView{}, fmt.Errorf("%w: %v", ErrDigestMismatch, err)
		default:
			return ResolvedView{}, fmt.Errorf("%w: %v", ErrCrossManifestRevision, err)
		}
	}
	return ResolvedView{
		Reference:      view.Reference,
		Body:           view.Body,
		ViewDigest:     view.ViewDigest,
		ManifestDigest: view.ManifestDigest,
	}, nil
}
