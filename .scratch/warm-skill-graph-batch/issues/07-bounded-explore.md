# 07 — TB-07 — Bounded Evaluation Graph explore 选择 original Skill offer

**What to build:** Memory Agent 从 opening seeds 开始，在固定 graph/turn/offer 预算内沿允许关系多跳探索，显式选择 `serve_original` 并生成包含 exact Skill Reference、checkpoint 和 target 的 pending offer。

**Blocked by:** 01 — PF-01 — 增加 non-active Evaluation Skill Graph read view

**Status:** resolved

- [x] 固定 fixture 的 seed、neighbor order、selected ref 和 audit trace 可复现。
- [x] Disallowed Room/evidence expansion、latest alias、本地 path、跨 scope ref 被拒绝。
- [x] 12 graph steps、6 memory turns、3 offers 边界分别产生正确 terminal state。
- [x] Conditional conflict、retired/superseded 和 train feedback edge按 policy影响候选。
- [x] Held-out feedback变化不影响 seed、ranking 或 explore result。

## Comments
Implemented in `graph-memory-service/internal/skillevolution/evaluationexplore/` only (package `evaluationexplore`). Memory Explore starts from opening seeds on a pinned `evaluationgraph.View`, walks contract-allowed relations inside the frozen 12/6/3 budgets, explicitly chooses `serve_original`, and emits a pending offer with exact Skill Reference, checkpoint, and target. Held-out feedback is accepted on the request but never read.

Acceptance coverage:
- `TestFixedFixtureSeedNeighborOrderSelectedRefAndAuditTraceAreReproducible`
- `TestDisallowedRoomEvidenceLatestAliasLocalPathAndCrossScopeRefsAreRejected`
- `TestGraphTurnAndOfferBudgetsProduceDistinctTerminalStates`
- `TestConditionalConflictRetiredSupersededAndTrainFeedbackEdgesAffectCandidates`
- `TestHeldOutFeedbackChangesDoNotAffectSeedRankingOrExploreResult`

Required command from `graph-memory-service` (GOCACHE=/tmp/wsgb-tb07-go-cache, `$HOME/go/bin/go`):
- `$HOME/go/bin/go test ./internal/skillevolution/evaluationexplore/ ./internal/skillevolution/evaluationgraph/ -count=1` — pass

Did not change `evaluationgraph` implementation, `cmd/server/main.go`, `httpapi/**`, `projector/**`, `rawproposal/**`, `proposal/**`, or `pi-group-chat-host/**`.
