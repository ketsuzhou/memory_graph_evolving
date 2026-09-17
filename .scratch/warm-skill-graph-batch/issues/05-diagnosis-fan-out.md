# 05 — TB-03 — Diagnosis fan-out 保留 outcome 并覆盖每个 served exposure

**What to build:** 每条 train trajectory 独立并行 diagnosis；结果按预分配 sequence 归并；每个 served exposure 恰有一个 supported/refuted/inconclusive verdict，公开失败信息可见而 hidden/gold 不可见。

**Blocked by:** 04 — TB-02 — 一条 frozen trajectory 端到端产出 canonical Raw Skill Proposal

**Status:** resolved

- [x] N 条 trajectory 产生 N 个 terminal diagnosis jobs，并有并发 barrier 证明。
- [x] Pass/fail、timeout/no-output/protocol-error、公开 compile/runtime error 进入输入。
- [x] Binary-only failure 使用 `cause=unknown`，不得生成虚构根因字段。
- [x] 每个 served exposure verdict exactly once；未 served offer 不产生 exposure verdict。
- [x] 单 job 失败被记录且不产生伪 proposal；consolidation 不在所有 jobs terminal 前启动。

## Comments
Implemented in `pi-group-chat-host/internal/diagnosisfanout/` only. Each train trajectory becomes one independent diagnosis job; results merge by pre-assigned sequence; `CanStartConsolidation()` stays false until every job is terminal. Successful jobs mint a real canonical proposal through `rawproposal.Admit` (in-memory trajectory source); failed jobs are recorded and never call Admit. Served exposures receive exactly one supported/refuted/inconclusive verdict; unserved offers do not. Public pass/fail/timeout/no-output/protocol-error and compile/runtime errors enter the worker input; hidden/gold material is stripped; binary-only failures use `cause=unknown` with no invented root-cause fields. Worker is a fixture/stub — no Pi or model.

Acceptance coverage:
- `TestFanOutNTrajectoriesProduceNTerminalJobsWithConcurrentBarrier`
- `TestDiagnosisInputIncludesPublicOutcomesAndErrors`
- `TestBinaryOnlyFailureUsesCauseUnknownWithoutInventedRootCause`
- `TestServedExposureVerdictExactlyOnceAndUnservedOffersHaveNone`
- `TestFailedJobIsRecordedWithoutFakeProposalAndBlocksConsolidation`

Required command from `pi-group-chat-host` (GOCACHE=/tmp/wsgb-tb03-go-cache, `$HOME/go/bin/go`):
- `$HOME/go/bin/go test ./internal/diagnosisfanout/ -count=1` — pass

Did not change `rawproposal/`, `evaluationgraph/`, `cmd/server`, `httpapi`, `bench-runner/main.go`, `parallel_batch.go`, `internal/runtime/**`, or `internal/pi/**`.
