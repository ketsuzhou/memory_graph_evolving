# 11 — TB-09 — Mutating tool 安全中断与 effects-unknown retry

**What to build:** Coordinator 区分 model/read-only/mutating execution；mutating tool 完成后再 abort，超时按 process-group TERM→KILL；无法证明 side effect 时 quarantine attempt，并由 runner 重试而非盲目 resume。

**Blocked by:** 10 — TB-08 — Structured offer 原子投递并中断/续跑 exact task session

**Status:** resolved

- [x] Read-only/model generation 可立即 cooperative abort。
- [x] Mutating tool 的 completion 在 abort 前被观察，已完成操作不重复执行。
- [x] Uncooperative child 经过 grace、TERM、KILL 后整个 process tree 被回收。
- [x] Effects-unknown attempt 不 resume，产生新 attempt ID 且保留原失败记录。
- [x] Retry exhaustion 保留 preregistered task 并在主分母记 0。

## Comments
模块：`pi-group-chat-host/internal/effectsinterrupt/`（`Coordinator.Interrupt` 按 model/read-only/mutating 选择 abort 策略；mutating 先观察 completion 再 cooperative abort；grace 超时对 fake process-group 发 SIGTERM，再 SIGKILL；`effects_unknown` 由 `Runner` quarantine 后 mint 新 attempt ID，不 resume；retry 耗尽保留 preregistered task，pass@1 分母记 0。`ScriptedSession` + `FakeProcessTree`，不依赖真实 Pi）。未改 `internal/runtime/**`、`internal/directedoffer/**`、`internal/pi/**`、`cmd/bench-runner/**`、`graph-memory-service/**`。只读复用 `directedoffer.Binding` / `ResumeRequest` 与 `sessionctrl.Session`。

验收覆盖：
- `TestReadOnlyAndModelGenerationAbortCooperativelyImmediately`
- `TestMutatingToolCompletionIsObservedBeforeAbortAndCompletedOpsAreNotReplayed`
- `TestUncooperativeChildIsReclaimedAfterGraceTermAndKill`
- `TestEffectsUnknownDoesNotResumeAndRetriesWithNewAttemptID`
- `TestRetryExhaustionKeepsPreregisteredTaskAndScoresZeroInDenominator`

验证：`GOCACHE=/tmp/wsgb-tb09-go-cache $HOME/go/bin/go test ./internal/effectsinterrupt/ ./internal/directedoffer/ -count=1`
