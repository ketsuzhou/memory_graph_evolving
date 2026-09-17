# 01 — PF-01 — 增加 non-active Evaluation Skill Graph read view

**What to build:** 一个未激活 advisory proposal 经 fixture consolidation 后，可以在 evaluation scope 中按 exact revision 多跳探索和读取；它不会进入 active Runtime graph、activation ledger 或 executable closure。该 view 只投影 canonical records，不成为第二权威源。

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] 相同 canonical input 重建得到相同 projection digest、节点、边和顺序。
- [ ] Non-active proposal 在 evaluation scope 可 explore/get，在 Runtime active scope 仍被拒绝。
- [ ] Projection pin、watermark 和 digest 不匹配时 fail closed。
- [ ] Held-out feedback record 作为 projection input 时被拒绝。
- [ ] 删除 projection 后可从 canonical fixture 重建为 byte-equivalent view。

## Comments
