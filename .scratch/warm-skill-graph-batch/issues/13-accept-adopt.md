# 13 — TB-11 — Accepted disposition 解除围栏并可选记录 adoption

**What to build:** `skill_feedback(accepted)` 原子创建唯一 disposition、interaction signal、群聊可见 accepted 消息和 Memory Delivery，并解除 task 工具围栏；可选 adoption 独立记录实际行为改变。

**Blocked by:** 12 — TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**Status:** resolved

- [x] 未 served 的 offer 不能 accepted。
- [x] 同 offer/agent 只有一个 terminal disposition；相同重放幂等、冲突重放拒绝。
- [x] Signal、Room message、Memory Delivery 三项 all-or-none。
- [x] Accepted 不自动产生 adopted、verified、active 或 marginal-gain-supported。
- [x] 无效 feedback 仅 repair 一次；第二次记录 protocol-error 并解除围栏。

## Comments
模块：`pi-group-chat-host/internal/skilldisposition/`（`Coordinator.NoteServed` 只接受 Host-authored `skillfence.ServedFact`；`HandleToolCall(skill_feedback(accepted))` 原子写唯一 terminal disposition、interaction signal、群聊可见 `@memory-agent SKILL_ACCEPTED` 与 Memory directed Delivery，并解除 task 工具围栏。可选 `skill_adoption` 独立绑定 exact Skill/offer/checkpoint/behavior change/affected action。无效 feedback 允许一次 repair；第二次记录 `protocol_error` 后解除围栏。同 `CallID` 或同 offer/agent/disposition 重放幂等，冲突 disposition 拒绝）。Room 发布复用 `directedoffer.Coordinator.Offer`。未改 `internal/skillfence/**`、`internal/directedoffer/**`、`internal/effectsinterrupt/**`、`internal/runtime/**`、`cmd/bench-runner/**`、`graph-memory-service/**`。

验收覆盖：
- `TestUnservedOfferCannotBeAccepted`
- `TestSameOfferAgentHasOneTerminalDispositionSameReplayIdempotentConflictRejected`
- `TestSignalRoomMessageMemoryDeliveryAreAllOrNone`
- `TestAcceptedDoesNotAutoProduceAdoptedVerifiedActiveOrMarginalGainSupported`
- `TestInvalidFeedbackRepairsOnceThenProtocolErrorLiftsFence`

验证：`GOCACHE=/tmp/wsgb-tb11-go-cache $HOME/go/bin/go test ./internal/skilldisposition/ ./internal/skillfence/ -count=1`
