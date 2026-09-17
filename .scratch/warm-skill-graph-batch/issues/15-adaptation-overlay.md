# 15 — TB-13 — Context adaptation 只存在于 episode-local overlay

**What to build:** Memory 可显式选择 original 或创建绑定 exact source、opening snapshot、delta 的 adaptation；test adaptation 可被同 attempt 的 `skill_get` 解析，但不会修改 canonical Skill 或 frozen graph。

**Blocked by:** 07 — TB-07 — Bounded Evaluation Graph explore 选择 original Skill offer; 12 — TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**Status:** resolved

- [x] 无实质 delta 的 adaptation 被拒为 duplicate。
- [x] Source revision body/digest 保持不变。
- [x] Adaptation 只能在同 attempt、同 manifest、同 target scope解析。
- [x] Episode close 后 adaptation 不可解析。
- [x] Ledger/graph/freeze digest 在 overlay 生命周期前后不变，report可区分 original/adaptation。

## Comments
Implemented in `graph-memory-service/internal/skillevolution/adaptationoverlay/` only (package `adaptationoverlay`). Memory must explicitly choose `serve_original` or `create_adaptation`. A test adaptation binds an exact source revision, a replayable opening snapshot, an explicit delta, and a generated body; same-attempt `skill_get` can resolve it. The overlay never writes the Skill Evolution Ledger, never mutates a canonical Skill, and never changes the frozen Evaluation Graph. Episode close destroys local adaptations.

Acceptance coverage:
- `TestAdaptationWithoutSubstantialDeltaIsRejectedAsDuplicate`
- `TestSourceRevisionBodyAndDigestRemainUnchanged`
- `TestAdaptationResolvesOnlyInSameAttemptManifestAndTargetScope`
- `TestAdaptationIsUnresolvableAfterEpisodeClose`
- `TestLedgerGraphFreezeDigestsUnchangedAndReportDistinguishesOriginalFromAdaptation`

Required command from `graph-memory-service` (GOCACHE=/tmp/wsgb-tb13-go-cache, `$HOME/go/bin/go`):
- `$HOME/go/bin/go test ./internal/skillevolution/adaptationoverlay/ ./internal/skillevolution/evaluationfreeze/ -count=1` — pass

Did not change `evaluationexplore/**`, `evaluationgraph/**`, `evaluationfreeze/**`, `rejectredirect/**`, `cmd/server`, `httpapi`, or `pi-group-chat-host/**`.
