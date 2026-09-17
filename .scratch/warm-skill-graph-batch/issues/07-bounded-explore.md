# 07 — TB-07 — Bounded Evaluation Graph explore 选择 original Skill offer

**What to build:** Memory Agent 从 opening seeds 开始，在固定 graph/turn/offer 预算内沿允许关系多跳探索，显式选择 `serve_original` 并生成包含 exact Skill Reference、checkpoint 和 target 的 pending offer。

**Blocked by:** 01 — PF-01 — 增加 non-active Evaluation Skill Graph read view

**Status:** ready-for-agent

- [ ] 固定 fixture 的 seed、neighbor order、selected ref 和 audit trace 可复现。
- [ ] Disallowed Room/evidence expansion、latest alias、本地 path、跨 scope ref 被拒绝。
- [ ] 12 graph steps、6 memory turns、3 offers 边界分别产生正确 terminal state。
- [ ] Conditional conflict、retired/superseded 和 train feedback edge按 policy影响候选。
- [ ] Held-out feedback变化不影响 seed、ranking 或 explore result。

## Comments
