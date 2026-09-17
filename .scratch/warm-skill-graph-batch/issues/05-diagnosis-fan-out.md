# 05 — TB-03 — Diagnosis fan-out 保留 outcome 并覆盖每个 served exposure

**What to build:** 每条 train trajectory 独立并行 diagnosis；结果按预分配 sequence 归并；每个 served exposure 恰有一个 supported/refuted/inconclusive verdict，公开失败信息可见而 hidden/gold 不可见。

**Blocked by:** 04 — TB-02 — 一条 frozen trajectory 端到端产出 canonical Raw Skill Proposal

**Status:** ready-for-agent

- [ ] N 条 trajectory 产生 N 个 terminal diagnosis jobs，并有并发 barrier 证明。
- [ ] Pass/fail、timeout/no-output/protocol-error、公开 compile/runtime error 进入输入。
- [ ] Binary-only failure 使用 `cause=unknown`，不得生成虚构根因字段。
- [ ] 每个 served exposure verdict exactly once；未 served offer 不产生 exposure verdict。
- [ ] 单 job 失败被记录且不产生伪 proposal；consolidation 不在所有 jobs terminal 前启动。

## Comments
