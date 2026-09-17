# 16 — TB-14 — 新策略贯通 train→diagnosis→consolidation→freeze→held-out

**What to build:** `warm-skill-graph-batch` 使用 canonical pipeline：parallel train barrier、parallel diagnosis、single consolidation、unified freeze、parallel held-out task+memory；test feedback仅写 trace，normal no-skill继续，mechanism failures分类保留。

**Blocked by:** 05 — TB-03 — Diagnosis fan-out 保留 outcome 并覆盖每个 served exposure; 06 — TB-04 — 一次 CAS consolidation 完整覆盖 family proposals; 09 — TB-05 — 统一 EvaluationFreezeManifest 在首个 test 前 fail closed; 08 — TB-06 — Task 与 opening-only Memory Agent 在空图上真实并发; 11 — TB-09 — Mutating tool 安全中断与 effects-unknown retry; 13 — TB-11 — Accepted disposition 解除围栏并可选记录 adoption; 14 — TB-12 — Rejected reason 驱动下一次非重复 exploration; 15 — TB-13 — Context adaptation 只存在于 episode-local overlay

**Status:** ready-for-agent

- [ ] 端到端 fixture 证明阶段 barrier、single consolidation 和 held-out parallelism。
- [ ] 新策略不以 runner-local `[]skillProposal` 或 Markdown/hash prefix 为 authority。
- [ ] Freeze失败时 0 个 test started。
- [ ] Test A feedback 不改变 Test B retrieval；所有 test 同 manifest digest。
- [ ] Legacy `warm-skill-batch` dispatch、artifact schema 和 golden行为不变。

## Comments
