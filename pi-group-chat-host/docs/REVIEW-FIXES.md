# pi-group-chat-host review 修复回执(对应 docs/REVIEW-tracer.md)

状态:B1–B4、M1–M5、M7–M9、M11、M13、m1 全部修复;M4 在修复过程中二次收紧(见下)。
冻结的 kiro 测试文件未改动;全部测试含新增回归测试全绿(`go test -count=1 ./...`),
并新增轻量磁盘持久化与重启语义。B5/B6/M6/M10/M12/M14 记录为 tracer 后债务。

## 逐项修复

| ID | 修复方式 | 实现位置 | 回归测试 |
|---|---|---|---|
| B1 ack/settlement 耐久边界 | Pi 解码器将 prompt ack 转为 `prompt_accepted` 回调事件;runtime 在该事件里 `AcceptAndOpenSegment`(持久 accept 边界先于任何 turn 事件被信任)。负 ack 不开段、不动 delivery(保持 pending 可重领);settle 前防御:无 acknowledged 段直接报错 | `internal/pi/process.go`、`internal/runtime/orchestration.go` | `internal/pi/parser_review_test.go`(ack 事件)、`internal/runtime/review_fixes_test.go`(负 ack 后 delivery pending、无 segment、可重领) |
| B2 副作用延迟到成功 completion | `tool_execution_start` 仅记录 pendingCalls;`tool_execution_end` 必须配对 ToolCallID+ToolName 且 `isError` 显式为 false 才执行;失败/缺 envelope/不配对的 completion 一律不发布 | `internal/runtime/orchestration.go` | runtime/review_fixes_test.go(失败、孤儿、缺 envelope、改名四种 completion 均不发布) |
| B3 settlement 与 outbox 同事务 | Store 增加 outbox 工作面(ClaimPending/MarkStaged/MarkCommitted/Reschedule/RecoverPendingOutbox/Adopt/Entries 含计数);`StoreOutbox` 单对象视图使 settlement 写与 worker 读共享同一互斥锁和行集 | `internal/store/memory/store.go`、`internal/store/memory/outbox.go` | recovery/tracer 冻结测试 + snapshot round-trip |
| B4 grants 对 conformant GMS 无效 | RunTracer 注册第三个 Space(memory agent 私有空间)并把两个 grant 授给 `host-service`:lifecycle/[evidence.stage, evidence.commit, recall] 与 tool_plane/[exploration.*],均覆盖三 Space、显式 ExpiresAt | `internal/runtime/orchestration.go`(RunTracer) | tracer 冻结测试 |
| M1 版本门禁过宽 | `parseVersionOutput` 只接受 1 字段(`0.85.1`)或 2 字段(`pi 0.85.1`),其余(如 `evil prefix 0.85.1`、`0.85.1+build`)拒绝 | `internal/pi/launcher.go` | pi/parser_review_test.go |
| M2 parser 非严格 | response 帧 `command != "prompt"` → ErrProtocol;首次 ack 后重复 ack → ErrProtocol;`rpcFrame` 增 Result/IsError 并透传到回调 | `internal/pi/process.go` | pi/parser_review_test.go |
| M3 schema/校验不精确 | memory_start 补 max_steps/max_results、memory_explore 补 anchor_citation_id/relation/limit、memory_redirect 补 anchor_citation_ids/reason、memory_submit 补 citation_ids、room_react 参数改 message_id;runtime 经 `toolSchemaFor`+`ValidateModelArguments` 与域校验(range/enum/required)双查 | `internal/tools/tools.go`、`internal/runtime/orchestration.go`(handleToolInvocation) | tools/runtime 冻结测试 |
| M4 幂等作用域错误(两轮修复) | 第一轮:reply key 记录发布 agent。第二轮(本轮回归测试发现残余缺陷):跨 agent 复用同 key 仍报冲突,改为 `replyKey{Agent, Op}` 复合键——同 agent 同 key 内容冻结(不同内容 409),不同 agent 同 key 完全独立;room_react 幂等作用域同步修正 | `internal/store/memory/store.go`(PublishToolMessage)、`snapshot.go`(复合键序列化为 reply_keys 切片) | `internal/store/memory/review_fixes_test.go`(作用域、冲突、重放、快照 round-trip 保 agent 维度) |
| M5 session ownership fail-closed | `authoritativeSession(hostSession, modelSession)`:host 未开段或 model 给出不同 session → 拒绝调用(explore/redirect/submit 均查) | `internal/runtime/orchestration.go` | runtime/review_fixes_test.go(伪造 session_id 的 explore/submit 均不触达远端) |
| M7 single-flight 与 transition invariant | ClaimNext 按 (room, agent) 单飞(在飞时重复 claim 返回同一 delivery);Fail/Abort 对未开段的 delivery 只终结 delivery,不创建幽灵段 | `internal/store/memory/store.go` | store/review_fixes_test.go |
| M8 outbox retry/commit key 确定化 | CommitID = `commit-<entry.ID>`;drain 对已 staged 行直接 commit 不重复 stage;Entries 暴露 stage/commit 尝试计数 | `internal/runtime/orchestration.go`、store | recovery 冻结测试 |
| M9 recall snapshot 只含 ID | `RecallObservation.Items` 含 CitationID/Content/SourceSpaceID/MemoryVersion;prompt 渲染 `- [citation] "content" (source=space version=N)` | `internal/runtime/orchestration.go` | tracer/recall_scope 冻结测试 |
| M11 失败分类与稳定错误码 | `classifyPromptFailure` → PI_PROMPT_REJECTED(leave pending)/PI_CANCELLED(abort)/PI_PROCESS_EXITED、PI_PROTOCOL_ERROR、PI_ERROR(fail);原始 provider 文本不落入耐久状态 | `internal/runtime/orchestration.go`、`internal/pi/errors.go` | runtime/review_fixes_test.go |
| M13 伪造私有 Space | Memory Agent 私有 Space 由 RunTracer 注册(owner=memory agent),private outbox 指向注册空间 | `internal/runtime/orchestration.go` | tracer 冻结测试 |
| m1 fixture 不匹配 | runtime 侧 fake Pi fixture 帧序列按 ack → start → end 配对 | `internal/runtime/testfixtures_test.go` | 全部 runtime 测试 |

## 轻量磁盘持久化(新增)

- `internal/store/memory/snapshot.go`(Host 版):房间消息/投递/段/DAG 事件/链接/outbox 行+
  worker 尝试计数的全量 JSON 快照;`PersistToFile`(临时文件 → fsync → rename)/`LoadFromFile`
  (缺文件 = 首次启动)。
- **重启语义**:claims 与 (room, agent) 单飞 lease 不落盘——重启即释放,pending delivery
  重新可领(at-least-once 契约);reply 幂等键随快照保留 agent 维度,重启后重放语义不变。
- 验证:`internal/store/memory/snapshot_test.go` round-trip(消息顺序、幂等重放 duplicate、
  delivery settled、segment settled、outbox staged+计数、空 store)。

## 跨仓库 E2E 适配(spec 演进)

GMS 修复 B1(purpose fence)后,冻结 E2E bootstrap 的单一混合 grant(lifecycle +
exploration.*)不再合法。`internal/e2e/cross_repo_test.go` 的 **bootstrap 数据**改为两个
purpose 合法的 grant(lifecycle / tool_plane),**断言未动**。三连跑通过;另做了 GMS
`kill -9` 重启后免 bootstrap 的 recall 验证(直接召回重启前提交的证据)。

## 遗留债务(未在本轮修复,原因:结构性大改超出 tracer 范围)

- B5:真实 Pi trusted extension 注册与 Memory 工具注册表验收(需真实 pi 0.85.1 运行时)。
- B6:canonical Interaction DAG 全量投影(evidence 当前按 segment 投影,DAG 链接不完整)。
- M6:Recall/Room 身份从持久配置解析(现为内存 authority)。
- M10:memoryclient 全面 typed protocol/严格响应校验(现按端点最小校验)。
- M12:真实 crash 注入的恢复测试(现为 snapshot round-trip + 重启语义测试)。
- M14:用例包(room/delivery/dag)与 runtime 共享 invariant 的整合。
