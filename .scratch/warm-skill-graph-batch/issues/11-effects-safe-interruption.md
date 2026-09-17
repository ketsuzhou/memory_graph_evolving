# 11 — TB-09 — Mutating tool 安全中断与 effects-unknown retry

**What to build:** Coordinator 区分 model/read-only/mutating execution；mutating tool 完成后再 abort，超时按 process-group TERM→KILL；无法证明 side effect 时 quarantine attempt，并由 runner 重试而非盲目 resume。

**Blocked by:** 10 — TB-08 — Structured offer 原子投递并中断/续跑 exact task session

**Status:** ready-for-agent

- [ ] Read-only/model generation 可立即 cooperative abort。
- [ ] Mutating tool 的 completion 在 abort 前被观察，已完成操作不重复执行。
- [ ] Uncooperative child 经过 grace、TERM、KILL 后整个 process tree 被回收。
- [ ] Effects-unknown attempt 不 resume，产生新 attempt ID 且保留原失败记录。
- [ ] Retry exhaustion 保留 preregistered task 并在主分母记 0。

## Comments
