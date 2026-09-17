# 04 — TB-02 — 一条 frozen trajectory 端到端产出 canonical Raw Skill Proposal

**What to build:** Diagnosis 读取完整 trajectory、公开 outcome 和 exact checkpoint/evidence refs，通过受保护 admission 创建 immutable Raw Skill Proposal 与 provenance，并返回完整 ID/ref。

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] Valid fixture 可按完整 ID 读回，proposal 与 source checkpoint/evidence 双向可追踪。
- [ ] Missing/cross-snapshot/unauthorized evidence 整单失败且不产生 partial append。
- [ ] Global trigger、纯通用建议、无 baseline delta、无法由证据推出的 insight 被拒绝。
- [ ] Hidden-test/gold 字段或未知 core 字段被拒绝。
- [ ] 同 idempotency key + 同 body 返回同 ref；不同 body 冲突。

## Comments
