# 03 — TB-01 — 新策略以空 Skill Graph 完成一个正常 no-skill episode

**What to build:** CLI 可显式选择 `warm-skill-graph-batch`，冻结 contract 默认配置；generation 0 在空 evaluation graph 上得到 `no_candidate_from_empty_graph` 并完成任务，legacy dispatch 不变。

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] 新旧 strategy dispatch table 均有测试，旧 strategy 输出不变。
- [x] 非法 graph/offer/turn budget 和未知策略 fail closed。
- [x] Attempt artifact包含 strategy、empty initial snapshot、frozen config 和正常 no-skill terminal。
- [x] Empty graph 不被统计为 timeout、memory error 或 protocol error。

## Comments
模块：`pi-group-chat-host/cmd/bench-runner/`（`strategy_graph_batch.go` 空图 adapter；`main.go` 仅增加 `skillStrategyGraphBatch` dispatch 与 `runArm` fail-closed）。

验证：`GOCACHE=/tmp/wsgb-tb01-go-cache $HOME/go/bin/go test ./cmd/bench-runner/ -count=1`
