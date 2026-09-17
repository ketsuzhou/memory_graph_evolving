# 04 — TB-02 — 一条 frozen trajectory 端到端产出 canonical Raw Skill Proposal

**What to build:** Diagnosis 读取完整 trajectory、公开 outcome 和 exact checkpoint/evidence refs，通过受保护 admission 创建 immutable Raw Skill Proposal 与 provenance，并返回完整 ID/ref。

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] Valid fixture 可按完整 ID 读回，proposal 与 source checkpoint/evidence 双向可追踪。
- [x] Missing/cross-snapshot/unauthorized evidence 整单失败且不产生 partial append。
- [x] Global trigger、纯通用建议、无 baseline delta、无法由证据推出的 insight 被拒绝。
- [x] Hidden-test/gold 字段或未知 core 字段被拒绝。
- [x] 同 idempotency key + 同 body 返回同 ref；不同 body 冲突。

## Comments
Implemented in `graph-memory-service/internal/skillevolution/rawproposal/` only. Diagnosis reads a complete frozen trajectory plus public outcome through `Service.ReadTrajectory`; protected `Admit` validates exact checkpoint/evidence refs, mints an immutable Raw Skill Proposal with `novelty_status=hypothesized` and bidirectional provenance, and returns a full ID/ref (never a hash prefix).

Acceptance coverage:
- `TestAdmissionValidFixtureReadsByFullIDAndTracesBothWays`
- `TestAdmissionRejectsInvalidEvidenceWithoutPartialAppend`
- `TestAdmissionRejectsNonSpecificOrUnevidencedAdvice`
- `TestAdmissionRejectsHiddenGoldAndUnknownFields`
- `TestAdmissionIdempotencySameBodyReplaysAndDifferentBodyConflicts`

Required commands from `graph-memory-service` (GOCACHE=/tmp/wsgb-tb02-go-cache, `$HOME/go/bin/go`):
- `$HOME/go/bin/go test ./internal/skillevolution/rawproposal/ -count=1` — pass
- `$HOME/go/bin/go test ./internal/skillevolution/proposal/ ./internal/skillevolution/ledger/ -count=1` — pass

Did not change `cmd/server/main.go`, `httpapi/**`, `projector/**`, `proposal/**`, `skillproposal/**`, or `pi-group-chat-host/**`. Ordinary SkillProposal lifecycle is untouched.
