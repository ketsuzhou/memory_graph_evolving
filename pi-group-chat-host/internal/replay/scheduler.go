package replay

// HST-203 replay scheduler (Host 6; S4).
//
// Schedule flow (Host 6.1): validate the ReplayRequest and sealed inputs,
// freeze the plan, dispatch paired baseline/candidate runs over the
// RunnerPort, correlate raw outputs. The Host owns scheduling, frozen
// plan and correlation only: it never scores fixtures, never computes a
// U1 winner, never mints a Contract 7.11 ReplayResult and never emits
// release/activation semantics — those stay GMS authority (Host 6.7).

import (
	"context"
	"sync"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// ---------------------------------------------------------------------------
// Plan-time ports
// ---------------------------------------------------------------------------

// SealedSegment is the settled-segment view the scheduler resolves from
// the seal record (HST-202 store). Non-settled segments carry only the
// terminal state.
type SealedSegment struct {
	TerminalState string
	SegmentDigest string
	SegmentRef    SegmentRefInput
	PathSealRef   Ref
	PathDomains   map[string]string // path_id -> domain
}

// SealSource resolves settled segments and their seals (read-only).
type SealSource interface {
	SealedSegment(ctx context.Context, segmentID string) (SealedSegment, error)
}

// FamilyDeclaration is one fixture family declared by the frozen fixture
// manifest. Reserved families (merge until GMS-208) are declared but
// carry no packets: the plan may freeze them as empty placeholders, but
// the declaration itself must exist.
type FamilyDeclaration struct {
	Name       string
	Reserved   bool
	PacketRefs []Ref
}

// FixtureManifest is the frozen fixture-set declaration.
type FixtureManifest struct {
	Families []FamilyDeclaration
}

// PathRefView is one sealed conversation path a fixture pins.
type PathRefView struct {
	PathID      string
	Domain      string
	SegmentID   string
	PathSealRef Ref
}

// FixturePacket is one Q29-B recorded runtime packet (FND-002 protocol,
// pure data — it carries no runner instructions).
type FixturePacket struct {
	Ref                 Ref
	Family              string
	Side                string
	RunID               string
	RunAttempt          int
	Body                *contract.Object
	Segments            []SegmentRefInput
	PathRefs            []PathRefView
	RequiredPathDomains []string
	SealedInputDigest   string
	EnvironmentDigest   string
	CapabilityKinds     map[string]string
	Seeds               Seeds
	ExpectedDigest      string
	ExpectedStatus      string
}

// FixtureSource loads the frozen fixture corpus (read-only).
type FixtureSource interface {
	Manifest(ctx context.Context) (FixtureManifest, error)
	Packet(ctx context.Context, ref Ref) (FixturePacket, error)
}

// Profile is the resolved replay profile availability view.
type Profile struct {
	Ref              Ref
	Available        bool
	PermissionCaps   map[string]int64
	Adapter          Ref
	AdapterAvailable bool
}

// ProfileSource resolves replay profiles / runtime adapters (read-only).
type ProfileSource interface {
	Profile(ctx context.Context, ref Ref) (Profile, error)
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

// Options configures the scheduler. Reasons is the digest-verified closed
// reason registry that owns retry semantics; MaxAttempts bounds infra
// retries (default 2); DisableDeterminismProbe turns off the
// same-identity re-dispatch probe.
type Options struct {
	Reasons                 *contract.ReasonBundle
	MaxAttempts             int
	DisableDeterminismProbe bool
}

func (o Options) maxAttempts() int {
	if o.MaxAttempts < 1 {
		return 2
	}
	return o.MaxAttempts
}

// OutputRecord is the correlated per-run outcome. It references raw
// outputs only — no scoring, no utility, no release semantics.
type OutputRecord struct {
	Family         string
	Side           string
	RunID          string
	Attempts       int
	TerminalStatus string
	ReasonCode     string
	OutputRef      Ref
	OutputDigest   string
	LateAudit      []LateEvent
}

// ProbeRecord is one determinism probe: the same attempt identity
// re-dispatched must reproduce the recorded digest.
type ProbeRecord struct {
	RunKey         RunKey
	CorrelationID  string
	RecordedDigest string
	ProbedDigest   string
	Matched        bool
}

// ScheduleResult is the module-private scheduling outcome handed back to
// the caller (GMS canonicalizes authoritative results from these
// records; Host 6.7).
type ScheduleResult struct {
	Accepted               bool
	ReasonCode             string
	Detail                 string
	Plan                   *Plan
	Attempts               []AttemptRecord
	Outputs                []OutputRecord
	DeterminismProbes      []ProbeRecord
	SourceHeadExpectations []SkillRef
	IdempotentReplay       bool
}

// scheduledReplay is the stored state of one idempotency-keyed replay.
type scheduledReplay struct {
	requestDigest string
	plan          *Plan
	attempts      []AttemptRecord
	outputs       []OutputRecord
	probes        []ProbeRecord
	hardError     *Error
}

// Scheduler owns replay scheduling: frozen plans, the idempotency lease
// (same key reuses the same frozen plan; execution dispatch is
// single-writer per key) and attempt/output correlation.
type Scheduler struct {
	deps Deps
	wire transport // the only runner seam; scheduling code never touches it
	opts Options
	mu   sync.Mutex
	// replays is the idempotency-keyed store of frozen plans, append-only
	// attempt ledgers and correlated outputs (execution lease CAS).
	replays map[string]*scheduledReplay
}

// NewScheduler wires the scheduler to its ports.
func NewScheduler(deps Deps, runner RunnerPort, opts Options) (*Scheduler, error) {
	if deps.Seals == nil || deps.Fixtures == nil || deps.Profiles == nil {
		return nil, reject("REPLAY_REQUEST_INVALID", "scheduler deps incomplete")
	}
	if runner == nil {
		return nil, reject("REPLAY_REQUEST_INVALID", "runner port missing")
	}
	if opts.Reasons == nil {
		return nil, reject("REPLAY_REQUEST_INVALID", "closed reason registry required")
	}
	return &Scheduler{
		deps:    deps,
		wire:    newTransport(runner),
		opts:    opts,
		replays: make(map[string]*scheduledReplay),
	}, nil
}

// Schedule validates, freezes and dispatches one ReplayRequest.
//
// The idempotency key is the replay identity: the same key with the same
// frozen input reuses the stored plan and attempt ledger (nothing is
// re-dispatched); the same key with a different input is a conflict;
// rejected requests never claim the key.
func (s *Scheduler) Schedule(ctx context.Context, req *Request) ScheduleResult {
	if err := validateRequestShape(req); err != nil {
		return rejectResult(err)
	}
	reqDigest := digestOfValue(requestObject(req))

	s.mu.Lock()
	if existing, ok := s.replays[req.IdempotencyKey]; ok {
		s.mu.Unlock()
		if existing.requestDigest != reqDigest {
			return rejectResult(reject("IDEMPOTENCY_CONFLICT",
				"idempotency key reused with a different frozen input"))
		}
		return s.snapshot(existing, true)
	}
	s.mu.Unlock()

	plan, err := BuildPlan(ctx, req, s.deps, RetryPolicy{MaxAttempts: s.opts.maxAttempts()})
	if err != nil {
		return rejectResult(err)
	}

	// Execution lease: claim the idempotency key before dispatch so a
	// duplicate request cannot double-dispatch (Host 9.3 CAS).
	rec := &scheduledReplay{requestDigest: reqDigest, plan: plan}
	s.mu.Lock()
	if _, clash := s.replays[req.IdempotencyKey]; clash {
		s.mu.Unlock()
		return rejectResult(reject("IDEMPOTENCY_CONFLICT", "replay lease lost"))
	}
	s.replays[req.IdempotencyKey] = rec
	s.mu.Unlock()

	if herr := s.dispatch(ctx, plan, rec); herr != nil {
		rec.hardError = herr
	}
	return s.snapshot(rec, false)
}

// rejectResult converts a fail-closed error into a result view.
func rejectResult(err error) ScheduleResult {
	res := ScheduleResult{}
	if re, ok := err.(*Error); ok {
		res.ReasonCode = re.ReasonCode
		res.Detail = re.Detail
		return res
	}
	res.ReasonCode = "REPLAY_REQUEST_INVALID"
	res.Detail = err.Error()
	return res
}

// snapshot renders the stored replay state; the stored records are never
// mutated by rendering.
func (s *Scheduler) snapshot(rec *scheduledReplay, idempotent bool) ScheduleResult {
	res := ScheduleResult{
		Accepted:          rec.hardError == nil,
		Plan:              rec.plan,
		Attempts:          append([]AttemptRecord(nil), rec.attempts...),
		Outputs:           append([]OutputRecord(nil), rec.outputs...),
		DeterminismProbes: append([]ProbeRecord(nil), rec.probes...),
		IdempotentReplay:  idempotent,
	}
	if rec.plan != nil {
		res.SourceHeadExpectations = append([]SkillRef(nil), rec.plan.SourceHeads...)
	}
	if rec.hardError != nil {
		res.ReasonCode = rec.hardError.ReasonCode
		res.Detail = rec.hardError.Detail
	}
	return res
}

// ---------------------------------------------------------------------------
// Dispatch and correlation
// ---------------------------------------------------------------------------

// dispatch runs every family×variant lane of the frozen plan. Correlation
// is keyed by run key, not completion order; reserved families are frozen
// placeholders and never dispatch (Host 6.4-6.5).
func (s *Scheduler) dispatch(ctx context.Context, plan *Plan, rec *scheduledReplay) *Error {
	for _, fam := range plan.Families {
		if fam.Reserved {
			continue
		}
		for _, variant := range fam.Variants {
			if herr := s.dispatchRun(ctx, plan, fam, variant, rec); herr != nil {
				return herr
			}
		}
	}
	if s.opts.DisableDeterminismProbe {
		return nil
	}
	return s.probeDeterminism(ctx, rec)
}

// dispatchRun drives one lane's bounded attempt loop. Infra failures the
// closed registry whitelists (retryable + same_request) append a new
// attempt; semantic failures, unknown codes and transport errors
// terminate the lane fail-closed (Host 6.6).
func (s *Scheduler) dispatchRun(ctx context.Context, plan *Plan, fam FamilyPlan, variant VariantPlan, rec *scheduledReplay) *Error {
	att := newAttempt(plan, fam, variant, 1)
	for {
		out, err := s.wire.dispatch(ctx, att)
		ar := AttemptRecord{Attempt: att}
		if err != nil {
			// Unclassifiable transport failure: terminal, never retried
			// into a pass, recorded verbatim for diagnosis.
			ar.Outcome = OutcomeTransport
			ar.Detail = err.Error()
			s.record(rec, &ar, nil)
			return nil
		}
		// Late outputs are audit-only; anything trying to rewrite the
		// terminal fails the whole schedule closed.
		for _, le := range out.LateAudit {
			if le.RecordedAs != "late-audit-only" {
				ar.Outcome = outcomeOf(out.Status)
				ar.Output = &out
				s.record(rec, &ar, &out)
				return &Error{ReasonCode: "LATE_OUTPUT_REWRITE",
					Detail: "late event recorded_as=" + le.RecordedAs + " in " + fam.Family + "/" + variant.Side}
			}
		}
		ar.Outcome = outcomeOf(out.Status)
		ar.Output = &out
		if out.Status == "failed" {
			retryable, known := classifyFailure(s.opts.Reasons, out.ReasonCode)
			ar.Retryable = retryable && known
			if !ar.Retryable || att.AttemptNo >= plan.RetryPolicy.MaxAttempts {
				ar.Exhausted = ar.Retryable
				s.record(rec, &ar, &out)
				return nil
			}
			s.record(rec, &ar, &out)
			att = newAttempt(plan, fam, variant, att.AttemptNo+1)
			continue
		}
		s.record(rec, &ar, &out)
		return nil
	}
}

// record appends one attempt to the ledger (append-only) and correlates
// the lane's output record. Late audits attach to the terminal attempt's
// view; they never influence the terminal.
func (s *Scheduler) record(rec *scheduledReplay, ar *AttemptRecord, out *RawOutput) {
	rec.attempts = append(rec.attempts, *ar)
	idx := -1
	for i := range rec.outputs {
		if rec.outputs[i].Family == ar.RunKey.Family && rec.outputs[i].Side == ar.RunKey.Side {
			idx = i
			break
		}
	}
	if idx < 0 {
		rec.outputs = append(rec.outputs, OutputRecord{
			Family: ar.RunKey.Family,
			Side:   ar.RunKey.Side,
			RunID:  ar.RunID,
		})
		idx = len(rec.outputs) - 1
	}
	o := &rec.outputs[idx]
	o.Attempts++
	if out != nil {
		o.TerminalStatus = out.Status
		o.ReasonCode = out.ReasonCode
		o.OutputRef = out.Ref
		o.OutputDigest = out.Digest
		o.LateAudit = append([]LateEvent(nil), out.LateAudit...)
	} else {
		o.TerminalStatus = "failed"
	}
}

// probeDeterminism re-dispatches the terminal attempt identity of every
// succeeded lane: the runner must return the identical raw output digest
// (idempotent dispatch identity, RSIH 6.6). Any drift fails the schedule
// closed as nondeterminism.
func (s *Scheduler) probeDeterminism(ctx context.Context, rec *scheduledReplay) *Error {
	for i := range rec.outputs {
		o := &rec.outputs[i]
		if o.TerminalStatus != "succeeded" || o.OutputDigest == "" {
			continue
		}
		var att *AttemptRecord
		for j := len(rec.attempts) - 1; j >= 0; j-- {
			a := &rec.attempts[j]
			if a.RunKey.Family == o.Family && a.RunKey.Side == o.Side && a.Output != nil {
				att = a
				break
			}
		}
		if att == nil {
			continue
		}
		probe := ProbeRecord{
			RunKey:         att.RunKey,
			CorrelationID:  att.CorrelationID,
			RecordedDigest: o.OutputDigest,
		}
		if probed, ok := s.wire.probe(ctx, att.Attempt); ok {
			probe.ProbedDigest = probed
			probe.Matched = probed == o.OutputDigest
		}
		rec.probes = append(rec.probes, probe)
		if !probe.Matched {
			return &Error{ReasonCode: "REPLAY_NONDETERMINISTIC",
				Detail: "run " + o.Family + "/" + o.Side + " produced a different digest under the same attempt identity"}
		}
	}
	return nil
}

// outcomeOf maps a runner status onto the attempt outcome vocabulary;
// unknown statuses are unclassifiable and therefore terminal.
func outcomeOf(status string) string {
	switch status {
	case "succeeded":
		return OutcomeSucceeded
	case "failed":
		return OutcomeFailed
	case "inconclusive":
		return OutcomeInconclusive
	default:
		return OutcomeTransport
	}
}
