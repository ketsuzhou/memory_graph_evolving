# 14 — TB-12 — Rejected reason 驱动下一次非重复 exploration

**What to build:** Rejected disposition 产生结构化 reason 和 Memory Delivery；Memory 按九类 core reason 更新 guard/exclusion/blacklist/stop policy，并留下 `offer → reason → next query` 审计链。

**Blocked by:** 07 — TB-07 — Bounded Evaluation Graph explore 选择 original Skill offer; 12 — TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**Status:** resolved

- [x] 九个 reason codes 均有 table-driven redirect/stop 行为测试。
- [x] Fixture 演示 generic Skill rejected 后探索 specialized descendant 并再次 offer。
- [x] Same revision、excluded lineage 和相同 source set 不重复 offer。
- [x] `insufficient_context/already_resolved` 在 opening-only 模式停止当前 need。
- [x] Resolution error 不进入 rejection reason 统计；全部拒绝终止为 all-candidates-rejected。

## Comments
