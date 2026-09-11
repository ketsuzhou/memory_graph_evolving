package replay

// HST-203 attempt ledger and dispatch transport port (Host 6.4/6.6; RSIH 6).
//
// One attempt is one family×variant dispatch to the runner. Infra
// failures the closed reason registry marks retryable=true with
// retry_scope=same_request append a new attempt (bounded by the frozen
// retry policy); semantic failures never retry — a semantic failure must
// not be retried into a pass (Host 6.6, S4). Attempt records are
// append-only: a retry never overwrites a prior attempt.
//
// RunnerPort is the only seam to the RSIH HarnessRunner. The Host sends
// the frozen attempt inputs and receives a private immutable raw output
// reference; the Host imports nothing from RSIH and the runner carries no
// Host scoring semantics (no U1, no release).

import (
	"context"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// RunKey identifies one dispatch lane: fixture family × run side.
type RunKey struct {
	Family string
	Side   string
}

// FrozenInputs is the immutable slice of the plan one attempt dispatches.
// It is identical for every retry of the same run key.
type FrozenInputs struct {
	PlanDigest      string
	ReplayRequestID string
	CorrelationID   string
	RunKey          RunKey
	Artifact        SkillRef     // baseline side
	Candidate       CandidateRef // candidate side
	Fixtures        []FixturePlan
	Seeds           Seeds
	Permissions     map[string]int64
	Profile         Ref
	Adapter         Ref
	CapturePolicy   *contract.Object
}

// Attempt is one dispatch: replay identity reused across retries, unique
// per-attempt correlation (RSIH 6.6).
type Attempt struct {
	ReplayRequestID string
	IdempotencyKey  string
	PlanDigest      string
	RunKey          RunKey
	RunID           string
	AttemptNo       int
	CorrelationID   string
	Inputs          FrozenInputs
}

// LateEvent is a late output observation. Late outputs are audit-only:
// they arrive after the run terminal and never rewrite it (Host 6.4;
// recorded-late-determinism expectation recorded_as=late-audit-only).
type LateEvent struct {
	ArrivalStep int64
	Capability  string
	Origin      string
	Detail      *contract.Object
	RecordedAs  string
}

// RawOutput is the runner's private immutable raw execution output
// reference (RSIH 6.5). It is not a Contract 7.11 ReplayResult and carries
// no winner or release semantics.
type RawOutput struct {
	Ref        Ref
	Digest     string
	Status     string // succeeded | failed | inconclusive
	ReasonCode string
	LateAudit  []LateEvent
}

// RunnerPort dispatches one frozen attempt to the RSIH HarnessRunner.
// Implementations MUST be idempotent per attempt correlation identity:
// dispatching the same Attempt again returns the same raw output digest
// (the Host uses this for its determinism probe).
type RunnerPort interface {
	Run(ctx context.Context, att Attempt) (RawOutput, error)
}

// transport owns the runner wire. The scheduler never touches RunnerPort
// directly: scheduling/correlation policy stays independent of the
// adapter transport, and the transport adds no policy of its own —
// classification and retry budgets live in the scheduler.
type transport struct {
	runner RunnerPort
}

func newTransport(runner RunnerPort) transport { return transport{runner: runner} }

// dispatch sends exactly one attempt identity.
func (t transport) dispatch(ctx context.Context, att Attempt) (RawOutput, error) {
	return t.runner.Run(ctx, att)
}

// probe re-dispatches the same attempt identity; a compliant runner
// returns the identical raw output digest.
func (t transport) probe(ctx context.Context, att Attempt) (string, bool) {
	out, err := t.runner.Run(ctx, att)
	if err != nil {
		return "", false
	}
	return out.Digest, true
}

// AttemptRecord is one append-only ledger entry.
type AttemptRecord struct {
	Attempt
	Outcome   string // succeeded | failed | inconclusive | transport-error
	Retryable bool   // registry-classified infra retryability
	Exhausted bool   // retryable but the frozen retry budget was exhausted
	Output    *RawOutput
	Detail    string
}

// Attempt outcome vocabulary.
const (
	OutcomeSucceeded    = "succeeded"
	OutcomeFailed       = "failed"
	OutcomeInconclusive = "inconclusive"
	OutcomeTransport    = "transport-error"
)

// classifyFailure consults the closed reason registry: a failure is infra
// retryable only when the registry marks the code retryable=true AND
// retry_scope=same_request (Host 6.6 whitelist). Unknown codes are never
// retryable — fail closed.
func classifyFailure(bundle *contract.ReasonBundle, code string) (retryable, known bool) {
	if bundle == nil || code == "" {
		return false, false
	}
	entry, ok := bundle.Lookup(code)
	if !ok {
		return false, false
	}
	return entry.Retryable && entry.RetryScope == "same_request", true
}

// correlationID derives the unique per-attempt correlation identity from
// frozen inputs only (deterministic across schedulers).
func correlationID(planDigest string, key RunKey, attemptNo int) string {
	o := contract.NewObject()
	o.Set("plan_digest", contract.String(planDigest))
	o.Set("family", contract.String(key.Family))
	o.Set("side", contract.String(key.Side))
	o.Set("attempt_no", num(int64(attemptNo)))
	digest, err := contract.DigestOf(o)
	if err != nil {
		return ""
	}
	return "corr-" + digest[7:23]
}

// newAttempt freezes the dispatch inputs for one attempt of a run key.
func newAttempt(p *Plan, fam FamilyPlan, variant VariantPlan, attemptNo int) Attempt {
	key := RunKey{Family: fam.Family, Side: variant.Side}
	att := Attempt{
		ReplayRequestID: p.ReplayRequestID,
		IdempotencyKey:  p.IdempotencyKey,
		PlanDigest:      p.PlanDigest,
		RunKey:          key,
		RunID:           variant.RunID,
		AttemptNo:       attemptNo,
		CorrelationID:   correlationID(p.PlanDigest, key, attemptNo),
		Inputs: FrozenInputs{
			PlanDigest:      p.PlanDigest,
			ReplayRequestID: p.ReplayRequestID,
			CorrelationID:   p.CorrelationID,
			RunKey:          key,
			Fixtures:        fam.Fixtures,
			Seeds:           fam.Seeds,
			Permissions:     p.Permissions,
			Profile:         p.Profile,
			Adapter:         p.Adapter,
			CapturePolicy:   capturePolicyObject(),
		},
	}
	if variant.Side == SideCandidate {
		att.Inputs.Candidate = p.Candidate
	} else {
		att.Inputs.Artifact = p.Baseline
	}
	return att
}
