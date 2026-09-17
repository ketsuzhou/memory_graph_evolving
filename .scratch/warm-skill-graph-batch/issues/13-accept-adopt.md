# 13 — TB-11 — Accepted disposition 解除围栏并可选记录 adoption

**What to build:** `skill_feedback(accepted)` 原子创建唯一 disposition、interaction signal、群聊可见 accepted 消息和 Memory Delivery，并解除 task 工具围栏；可选 adoption 独立记录实际行为改变。

**Blocked by:** 12 — TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**Status:** ready-for-agent

- [ ] 未 served 的 offer 不能 accepted。
- [ ] 同 offer/agent 只有一个 terminal disposition；相同重放幂等、冲突重放拒绝。
- [ ] Signal、Room message、Memory Delivery 三项 all-or-none。
- [ ] Accepted 不自动产生 adopted、verified、active 或 marginal-gain-supported。
- [ ] 无效 feedback 仅 repair 一次；第二次记录 protocol-error 并解除围栏。

## Comments
