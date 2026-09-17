# 08 — TB-06 — Task 与 opening-only Memory Agent 在空图上真实并发

**What to build:** 新 episode 同时启动 task session 与 Memory Agent；memory 只见 opening context，并在空图上正常结束；task 不等待 memory 且不被无消息中断。

**Blocked by:** 02 — PF-02 — 把 one-shot Pi process 扩展为可控 exact-session controller; 03 — TB-01 — 新策略以空 Skill Graph 完成一个正常 no-skill episode

**Status:** resolved

- [x] Start-barrier 证明 task 与 memory 都在另一方 settled 前进入 running。
- [x] Memory input 不含 task 中间 tool state 或后续消息。
- [x] 同一 Agent 无重叠 Pi run，不同 Agent 可并行。
- [x] Empty/no-applicable 与 timeout/error terminal 可区分。
- [x] Memory 无 offer 时 task session 不被 abort/resume。

## Comments
模块：`pi-group-chat-host/internal/concurrentepisode/`（`Run` + start-barrier + `AgentRunCoordinator` single-flight；`ScriptedSession` 假会话，不依赖真实 Pi）。未改 `internal/runtime/**`、`internal/pi/process.go`、`internal/pi/launcher.go`、`internal/pi/sessionctrl/**`、`cmd/bench-runner/**`、`graph-memory-service/**`。

验收覆盖：
- `TestStartBarrierBothEnterRunningBeforeEitherSettles`
- `TestMemoryInputExcludesTaskToolStateAndFollowUpMessages`
- `TestSameAgentHasNoOverlappingPiRunAndDifferentAgentsOverlap`
- `TestEmptyAndNoApplicableTerminalsAreDistinctFromTimeoutAndError`
- `TestNoOfferDoesNotAbortOrResumeTaskSession`

空图正常终端：`no_candidate_from_empty_graph`。Memory 无 offer 时不 abort/resume task；task 可在 memory 仍 running 时 settled。

验证：`GOCACHE=/tmp/wsgb-tb06-go-cache $HOME/go/bin/go test ./internal/concurrentepisode/ ./internal/pi/sessionctrl/ -count=1`
