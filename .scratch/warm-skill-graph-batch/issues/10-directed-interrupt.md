# 10 — TB-08 — Structured offer 原子投递并中断/续跑 exact task session

**What to build:** Skill Offer 用结构化 target 原子写 Room message + unique Delivery，安全停止 running task，并用同一 exact session 恢复；中断期间到达的 messages 一次有序注入，创建 continuation Segment。

**Blocked by:** 02 — PF-02 — 把 one-shot Pi process 扩展为可控 exact-session controller; 08 — TB-06 — Task 与 opening-only Memory Agent 在空图上真实并发

**Status:** resolved

- [x] 自由文本 `@agent` 不路由；structured target 才创建 Delivery。
- [x] 同 key/body 重放返回原 message/delivery；同 key不同 body 冲突。
- [x] 两个并发 mentions 只触发一次 abort，按 Room sequence 一次 resume 注入。
- [x] Resume 后 session ID/file 与中断前一致，已完成 tool results 不重放。
- [x] 新 Segment 正确记录 `continuation_of` 和 directed-mention reason。

## Comments
模块：`pi-group-chat-host/internal/directedoffer/`（`Coordinator.Offer` 原子写 Room message + unique Delivery；同 Agent 单次 abort + 按 Room sequence 一次 resume；`ScriptedExactSession` 假会话，不依赖真实 Pi）。未改 `internal/runtime/**`、`internal/pi/process.go`、`internal/pi/launcher.go`、`internal/pi/sessionctrl/**`、`internal/concurrentepisode/**`、`cmd/bench-runner/**`、`graph-memory-service/**`。未改 `delivery/room/segment` 现有语义。

验收覆盖：
- `TestFreeTextAtAgentDoesNotRouteStructuredTargetCreatesDelivery`
- `TestSameKeyBodyReplayReturnsOriginalAndDifferentBodyConflicts`
- `TestConcurrentMentionsAbortOnceAndResumeInjectsInRoomSequence`
- `TestResumeKeepsExactSessionAndDoesNotReplayCompletedToolResults`
- `TestContinuationSegmentRecordsContinuationOfAndDirectedMentionReason`

自由文本 `@agent` 只落 Room message，不创建 Delivery。Structured `recipient_agent_id` 才路由。Resume 使用绑定的 exact session file/id；已完成 tool results 不重放。新 Segment 记录 `continuation_of` 与 `DIRECTED_MENTION`。

验证：`GOCACHE=/tmp/wsgb-tb08-go-cache $HOME/go/bin/go test ./internal/directedoffer/ ./internal/delivery/ ./internal/pi/sessionctrl/ -count=1`
