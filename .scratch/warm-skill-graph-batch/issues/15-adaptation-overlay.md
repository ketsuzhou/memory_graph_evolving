# 15 — TB-13 — Context adaptation 只存在于 episode-local overlay

**What to build:** Memory 可显式选择 original 或创建绑定 exact source、opening snapshot、delta 的 adaptation；test adaptation 可被同 attempt 的 `skill_get` 解析，但不会修改 canonical Skill 或 frozen graph。

**Blocked by:** 07 — TB-07 — Bounded Evaluation Graph explore 选择 original Skill offer; 12 — TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**Status:** ready-for-agent

- [ ] 无实质 delta 的 adaptation 被拒为 duplicate。
- [ ] Source revision body/digest 保持不变。
- [ ] Adaptation 只能在同 attempt、同 manifest、同 target scope解析。
- [ ] Episode close 后 adaptation 不可解析。
- [ ] Ledger/graph/freeze digest 在 overlay 生命周期前后不变，report可区分 original/adaptation。

## Comments
