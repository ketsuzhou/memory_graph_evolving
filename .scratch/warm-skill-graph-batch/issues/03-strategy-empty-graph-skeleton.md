# 03 — TB-01 — 新策略以空 Skill Graph 完成一个正常 no-skill episode

**What to build:** CLI 可显式选择 `warm-skill-graph-batch`，冻结 contract 默认配置；generation 0 在空 evaluation graph 上得到 `no_candidate_from_empty_graph` 并完成任务，legacy dispatch 不变。

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] 新旧 strategy dispatch table 均有测试，旧 strategy 输出不变。
- [ ] 非法 graph/offer/turn budget 和未知策略 fail closed。
- [ ] Attempt artifact包含 strategy、empty initial snapshot、frozen config 和正常 no-skill terminal。
- [ ] Empty graph 不被统计为 timeout、memory error 或 protocol error。

## Comments
