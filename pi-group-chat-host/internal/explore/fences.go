package explore

// Fence accounting for one ExploreSession (HST-204, Host Spec §7.3,
// Contract §5.6/§12.6/§12.7.2 C6): the evidence fence, the skill fence and
// the two citation meters are counted independently, every consumption goes
// through a compare-and-swap, and a response that arrives after its query
// already reached a terminal result is audit-only and consumes nothing
// (Contract §13.7.1 R3-4 late semantics).

import (
	"sync"

	"river2.dev/pi-group-chat-host/internal/contract"
)

// Audit entry kinds for the explore session audit chain.
const (
	// AuditQueryReplay records a PrepareQuery that resolved to the cached
	// exact result of an earlier identical canonical request digest.
	AuditQueryReplay = "query_replay_audit"
	// AuditFenceConsumed records one validated response whose fences were
	// consumed and whose terminal result was fixed.
	AuditFenceConsumed = "fence_consumed_audit"
	// AuditValidationFailure records a response rejected before any fence
	// or citation was touched.
	AuditValidationFailure = "validation_failure_audit"
	// AuditLateUpstream records a post-terminal upstream response:
	// audit-only, never behavioral.
	AuditLateUpstream = "late_upstream_audit"
)

// CAS outcomes on the fence/terminal ledger.
const (
	CASFirstConsume  = "first_consume"
	CASFenceConflict = "fence_conflict_observed"
	CASLateOnly      = "late_only"
	CASReplayedSaved = "replayed_saved_terminal"
)

// AuditEntry is one immutable session-audit record. Free-text detail is
// diagnostic only; every behavioral field is a digest or a closed enum.
type AuditEntry struct {
	Kind         string
	SessionID    string
	QueryDigest  string
	ToolName     string
	ReasonCode   string
	CASOutcome   string
	ResultDigest string
	Detail       map[string]string
}

// ConsumedResult is the exact terminal result of one served query page.
type ConsumedResult struct {
	QueryDigest  string
	ResultDigest string
	Canonical    []byte
}

// FenceAccount is an introspection snapshot of the independent meters.
type FenceAccount struct {
	EvidenceFencesConsumed int
	SkillFencesConsumed    int
	IdentityCitations      int64
	EvidenceCitations      int64
	// PriorWatermark is projected_through_activation_sequence of the last
	// served page, or -1 before the first page.
	PriorWatermark int64
}

// fenceClaimer is the Room-scoped fence registry owned by the Manager: a
// fence digest may only ever be claimed by one (Room, session) owner, so a
// fence can never cross a Room, a scope profile or another agent's private
// space (Host Spec §7.3).
type fenceClaimer interface {
	// claimFence registers digest for sessionID. It returns
	// ("", true) when newly claimed, (owner, false) when the fence already
	// belongs to owner (including sessionID itself: same fence consumed
	// twice is a conflict, not an idempotent replay).
	claimFence(kind, digest, sessionID string) (owner string, ok bool)
}

// fenceLedger meters one session. All mutations happen under mu and only
// after the full response validation passed, so a rejected response can
// never leave partial consumption behind.
type fenceLedger struct {
	mu           sync.Mutex
	sessionID    string
	claimer      fenceClaimer
	evidenceSeen map[string]struct{}
	skillSeen    map[string]struct{}

	identityCitations    int64
	evidenceCitations    int64
	priorWatermark       *contract.Object
	priorServedDocuments []*contract.Object // every served page, delivery order
	lastPayload          *contract.Object
	terminals            map[string]*ConsumedResult
	preparedBudgets      map[string]BudgetCaps
	audit                []AuditEntry
}

func newFenceLedger(sessionID string, claimer fenceClaimer) *fenceLedger {
	return &fenceLedger{
		sessionID:       sessionID,
		claimer:         claimer,
		evidenceSeen:    map[string]struct{}{},
		skillSeen:       map[string]struct{}{},
		terminals:       map[string]*ConsumedResult{},
		preparedBudgets: map[string]BudgetCaps{},
	}
}

// recordPrepared remembers the effective (clamped) budgets of one prepared
// query so the response side can check the returned budgets against the
// ceilings the upstream was actually told about.
func (f *fenceLedger) recordPrepared(queryDigest string, caps BudgetCaps) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.preparedBudgets[queryDigest] = caps
}

// account snapshots the independent meters.
func (f *fenceLedger) account() FenceAccount {
	f.mu.Lock()
	defer f.mu.Unlock()
	prior := int64(-1)
	if f.priorWatermark != nil {
		if seq, ok := intFieldOf(f.priorWatermark, "projected_through_activation_sequence"); ok {
			prior = seq
		}
	}
	return FenceAccount{
		EvidenceFencesConsumed: len(f.evidenceSeen),
		SkillFencesConsumed:    len(f.skillSeen),
		IdentityCitations:      f.identityCitations,
		EvidenceCitations:      f.evidenceCitations,
		PriorWatermark:         prior,
	}
}

// sessionContext builds the carrier-validation session block
// ({prior_watermark, prior_served}) from live fence state; nil before the
// first served page (the frozen validator treats a nil session as "no prior
// page": no fence-advancement or prior-served obligations).
func (f *fenceLedger) sessionContext() contract.Value {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.priorWatermark == nil {
		return nil
	}
	priorServed := contract.NewObject()
	evidenceRefs := contract.Array{}
	skillRefs := contract.Array{}
	// priorServedRefs is a set of JCS keys; rebuild typed refs from the
	// last payload plus every earlier page kept in priorServedDocuments.
	for _, doc := range f.priorServedDocuments {
		if arr, ok := arrayOf(doc, "evidence_results"); ok {
			for _, item := range arr {
				if entry, ok := item.(*contract.Object); ok {
					if ref, present := entry.Get("evidence_ref"); present {
						evidenceRefs = append(evidenceRefs, ref)
					}
				}
			}
		}
		if arr, ok := arrayOf(doc, "skill_results"); ok {
			for _, item := range arr {
				if entry, ok := item.(*contract.Object); ok {
					if ref, present := entry.Get("skill_ref"); present {
						skillRefs = append(skillRefs, ref)
					}
				}
			}
		}
	}
	priorServed.Set("evidence_refs", evidenceRefs)
	priorServed.Set("skill_refs", skillRefs)
	ctx := contract.NewObject()
	ctx.Set("prior_watermark", f.priorWatermark)
	ctx.Set("prior_served", priorServed)
	return ctx
}

// terminalFor returns the fixed terminal result of one query digest.
func (f *fenceLedger) terminalFor(queryDigest string) (*ConsumedResult, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.terminals[queryDigest]
	return t, ok
}

// commitTerminal fixes the terminal result of a query digest: first wins,
// later commits return the already-fixed terminal unchanged.
func (f *fenceLedger) commitTerminal(queryDigest string, result *ConsumedResult) *ConsumedResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.terminals[queryDigest]; ok {
		return existing
	}
	f.terminals[queryDigest] = result
	return result
}

// consumeResponse consumes fences and citation meters for one fully
// validated ExploreResult payload. Any conflict (fence reuse inside the
// session, fence claimed by another Room/session, citation meter overrun)
// leaves the ledger untouched and returns the closed failure reason.
func (f *fenceLedger) consumeResponse(payload *contract.Object, caps CitationCaps) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	fencesRaw, _ := payload.Get("served_fences")
	fences, _ := fencesRaw.(*contract.Object)
	if fences == nil {
		return reasonFieldMissing
	}
	evidenceFence, _ := contract.StringOf(fences, "evidence_fence_digest")
	skillFence, _ := contract.StringOf(fences, "skill_fence_digest")

	// Same-fence-twice inside this session: the CAS body. Checked before
	// touching the manager registry so the conflicting consume is fully
	// rolled back (nothing has mutated yet).
	if evidenceFence != "" {
		if _, seen := f.evidenceSeen[evidenceFence]; seen {
			return reasonFenceConflict
		}
	}
	if skillFence != "" {
		if _, seen := f.skillSeen[skillFence]; seen {
			return reasonFenceConflict
		}
	}
	// Room/scope isolation of fences is enforced by the manager registry.
	if f.claimer != nil {
		if evidenceFence != "" {
			if _, ok := f.claimer.claimFence("evidence", evidenceFence, f.sessionID); !ok {
				return reasonFenceConflict
			}
		}
		if skillFence != "" {
			if _, ok := f.claimer.claimFence("skill", skillFence, f.sessionID); !ok {
				return reasonFenceConflict
			}
		}
	}

	// Citation meters run independently: artifact identity citations and
	// evidence citations are separate budgets (Contract §5.6.2/§12.6).
	identityDelta := int64(0)
	evidenceDelta := int64(0)
	if arr, ok := arrayOf(payload, "evidence_results"); ok {
		for _, item := range arr {
			if _, isObj := item.(*contract.Object); isObj {
				evidenceDelta++
			}
		}
	}
	if arr, ok := arrayOf(payload, "skill_results"); ok {
		for _, item := range arr {
			entry, isObj := item.(*contract.Object)
			if !isObj {
				continue
			}
			if _, hasIdentity := entry.Get("artifact_identity_citation"); hasIdentity {
				identityDelta++
			}
			if citationsRaw, present := entry.Get("evidence_citations"); present {
				if citations, isArr := citationsRaw.(contract.Array); isArr {
					for _, citation := range citations {
						if _, isObj := citation.(*contract.Object); isObj {
							evidenceDelta++
						}
					}
				}
			}
		}
	}
	if caps.MaxIdentityCitations >= 0 && f.identityCitations+identityDelta > caps.MaxIdentityCitations {
		return reasonBudgetExceeded
	}
	if caps.MaxEvidenceCitations >= 0 && f.evidenceCitations+evidenceDelta > caps.MaxEvidenceCitations {
		return reasonBudgetExceeded
	}

	// Commit: everything from here on cannot fail.
	if evidenceFence != "" {
		f.evidenceSeen[evidenceFence] = struct{}{}
	}
	if skillFence != "" {
		f.skillSeen[skillFence] = struct{}{}
	}
	f.identityCitations += identityDelta
	f.evidenceCitations += evidenceDelta
	if wmRaw, present := payload.Get("watermark"); present {
		if wm, isObj := wmRaw.(*contract.Object); isObj {
			f.priorWatermark = wm
		}
	}
	f.priorServedDocuments = append(f.priorServedDocuments, payload)
	f.lastPayload = payload
	return ""
}

// appendAudit records one audit entry verbatim.
func (f *fenceLedger) appendAudit(entry AuditEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry.SessionID == "" {
		entry.SessionID = f.sessionID
	}
	f.audit = append(f.audit, entry)
}

// auditEntries snapshots the audit chain in record order.
func (f *fenceLedger) auditEntries() []AuditEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]AuditEntry(nil), f.audit...)
}

// lastConsumedPayload returns the most recently served payload (the exact
// bytes as delivered — never reordered or filtered by the Host).
func (f *fenceLedger) lastConsumedPayload() *contract.Object {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastPayload
}
