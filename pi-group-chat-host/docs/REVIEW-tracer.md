# pi-group-chat-host tracer 实现评审(对照 docs/PLAN.md)

## 总体结论(3-5 句)

总体结论为 **不符合冻结 spec，当前不应视为 tracer 完成**。虽然现有测试在宽松 fake 上覆盖了六条可见消息、三条 Delivery、三段 Segment、六条 outbox 的表面计数，但关键耐久边界、真实工具完成语义、Graph Memory 授权、DAG/evidence 投影及恢复语义并未成立；本评审共识别 **6 个 Blocker、14 个 Major、1 个 Minor**。最严重的问题是 ack 直到 `agent_settled` 后才持久化、`tool_execution_start` 即产生副作用，以及 settlement 后另行向独立 outbox 队列 `Adopt`，三者分别破坏结算状态机、可见消息权威和原子性。ADR 0040 指定的六项有意偏离均未作为问题报告，并在相关章节标注“ADR 0040 已接受”。

## 逐节对照表(PLAN 章节 | 状态 ✅/⚠️/❌ | 证据 file:line | 备注)

| PLAN 章节 | 状态 | 证据 file:line | 备注 |
|---|---:|---|---|
| §1 Scope, authority, and frozen rulings | ❌ | `internal/runtime/orchestration.go:286-293,601-605,707-745` | token-bound Principal 的 Grant、唯一 Memory Agent、ack/settlement 均偏离。Host 直接管理 Pi 子进程为 **ADR 0040 已接受**。 |
| §2 Tracer sequence and completion order | ❌ | `internal/runtime/orchestration.go:238-396`; `internal/runtime/tracer_test.go:159-182` | 宽松 fake 下计数为 6 message/3 delivery/3 segment/6 outbox，但 Grant 与 Memory 私有 Space 使其无法对 conformant GMS 完成；步骤 5–7 在 drain 失败时仍会标完成。 |
| §3 Package structure and dependency direction | ⚠️ | `internal/runtime/orchestration.go:17-21`; `go.mod:1-3` | 目录齐全、Go 1.26、仅标准库、无 sibling/Multica import；但 runtime 直接依赖 concrete memory store，且绕过 `room/delivery/dag/evidence/tools` 用例包。`ErrNotImplemented` 保留为 **ADR 0040 已接受**。 |
| §4 Memory Protocol v1 HTTP contract | ⚠️ | `internal/ports/graph_memory.go:5-219`; `internal/memoryclient/client.go:89-174` | 12 方法和主要 JSON 字段存在，但 typed error、opaque path ID、严格响应校验及实际授权/投影不合规。 |
| §4.1 Common wire rules | ⚠️ | `internal/memoryclient/client.go:45-83,120-171` | Header/JSON 方法正确；路径 ID 直接拼接，成功响应接受任意 2xx，decoder 接受未知/重复/缺失字段，响应上限也未可靠检测。 |
| §4.2 Error envelope and status codes | ⚠️ | `internal/memoryclient/client.go:17-22,68-78` | `ProtocolError` 保留 status/code/message/request_id，但丢失 required `details`， malformed envelope 也会被构造成空 typed error。 |
| §4.3 Tenant initialization | ✅ | `internal/ports/graph_memory.go:20-31`; `internal/memoryclient/client.go:89-93` | `PUT /v1/tenant` 请求/响应字段匹配。 |
| §4.4 Principal registration | ✅ | `internal/ports/graph_memory.go:33-44`; `internal/memoryclient/client.go:95-99` | `POST /v1/principals` 请求/响应字段匹配。 |
| §4.5 Space registration | ✅ | `internal/ports/graph_memory.go:46-60`; `internal/memoryclient/client.go:101-105` | `POST /v1/spaces` 及 nullable owner 字段匹配。 |
| §4.6 Exact Grant registration | ❌ | `internal/runtime/orchestration.go:282-299`; `internal/e2e/cross_repo_test.go:65-79` | tracer 把 Grant 给 agent 而不是 token-bound `host-service`，并发送非法 purpose `memory`、遗漏 redirect；E2E 的 service grant 反而说明正确 caller 模型。 |
| §4.7 Evidence Batch stage, commit, and status | ❌ | `internal/runtime/orchestration.go:966-1055`; `internal/store/memory/outbox.go:99-123` | HTTP shape 基本正确；commit key 每次重建、失败后重置 pending，且 batch 从全 Room 消息而非 source Segment 投影。`captured_at` 从 `ClosedAt` 派生为 **ADR 0040 已接受**。 |
| §4.8 Recall | ⚠️ | `internal/memoryclient/client.go:133-151`; `internal/runtime/orchestration.go:638-669,1174-1195` | unavailable 合成且不扩 scope；ordinary 请求顺序为 `[shared, private]`，但 Space 来自请求内 authority 而非持久配置，prompt 仅列 citation ID、不含 recalled content/source。 |
| §4.9 Exploration | ❌ | `internal/runtime/orchestration.go:815-929,1206-1211`; `internal/tools/tools.go:98-138` | start 使用 shared-only，但 session ownership 可被模型兜底，required args 不完整，cited reply 未被服务端约束，tracer Grant 也不能授权这些调用。 |
| §5 Core Host domain signatures and state machines | ❌ | `internal/domain/types.go:3-112`; `internal/store/memory/store.go:145-172,178-247` | 类型大体齐全；存储不执行合法 transition、Room tool 无 scoped idempotency/conflict、single-flight 锁错粒度、reaction 被建模成 message。 |
| §6 Exact Pi 0.85.1 JSONL RPC contract | ❌ | `internal/pi/launcher.go:51-129`; `internal/pi/process.go:74-139` | 版本、argv 的 isolated happy path 存在，但 runtime 漏 extension、无 registry inspection，frame 校验和两阶段持久化不合规。 |
| §6.1 Version negotiation and process launch | ❌ | `internal/pi/launcher.go:51-73,77-129`; `internal/runtime/orchestration.go:688-704` | `parseVersionOutput` 接受任意前缀最后字段；无 path+file identity cache；runtime 未设置 trusted extension；Memory 启动不执行 `get_commands`。MkdirAll 行为为 **ADR 0040 已接受**。 |
| §6.2 Prompt acceptance and settlement | ❌ | `internal/pi/process.go:95-115`; `internal/runtime/orchestration.go:707-745` | `Prompt` 到 settled 才返回，之后 runtime 才 accept/open 并立即 settle；negative ack 被 fail/close，不是 pending/no Segment。单 prompt 半关闭 stdin 为 **ADR 0040 已接受**。 |
| §6.3 Tool event envelope and Room tool calls | ❌ | `internal/pi/process.go:123-129`; `internal/runtime/orchestration.go:707-720,775-812`; `internal/tools/tools.go:72-95` | completion result/isError 未解析或匹配；副作用发生在 start；react 参数名/结果模型错误；幂等 conflict 未实现。 |
| §6.4 Memory Agent tool calls | ❌ | `internal/tools/tools.go:98-138`; `internal/runtime/orchestration.go:815-929,1206-1211` | 工具名齐全，但 required/enum/range 不精确；unknown authority 字段在 runtime 被忽略而非 fail closed；session/citation 约束缺失。 |
| §6.5 Pi command/protocol errors | ❌ | `internal/pi/process.go:34-43,91-135`; `internal/runtime/orchestration.go:723-726`; `internal/store/memory/store.go:252-266` | 不校验 command/error/额外字段/重复 response/agent_end messages/tool result；所有 Prompt 错误一律 failed，取消不 aborted，并持久化原始错误字符串。 |
| §7 Fake Pi test subprocess format | ✅ | `internal/pi/fake_pi_test.go:22-55`; `internal/runtime/testfixtures_test.go:13-46` | POSIX sh、`os.WriteFile(...,0o700)`、literal JSONL、无真实网络/provider 均符合。stdin 半关闭及 EOF 表现为 **ADR 0040 已接受**；仅有一个 Minor：fixture 未验证 request `type`。 |
| §8 GraphMemoryClient interface | ⚠️ | `internal/ports/graph_memory.go:5-18`; `internal/memoryclient/client.go:17-22,45-83,89-174` | 12 方法齐全并用 HTTP/JSON；unavailable 合成正确，但 typed error/严格响应/max size/path escaping 不完整。 |
| §9 Host storage port signatures | ⚠️ | `internal/ports/host.go:11-74`; `internal/store/memory/store.go:200-299`; `internal/runtime/orchestration.go:741-745` | 接口签名匹配；实现未守 transition/single-flight，且 settlement 写 Store 内 rows 后再向另一 Outbox `Adopt`，没有 spec 所需原子边界。 |
| §10 Required behavioral invariants | ❌ | `internal/runtime/orchestration.go:601-669,707-745,934-1055`; `internal/store/memory/store.go:145-194` | 仅基本 message/delivery 唯一性和表面计数成立；持久 authority、ack facts、成功工具可见性、DAG、outbox 原子性/稳定 retry 均失败。 |
| §11 Test strategy | ❌ | `internal/pi/rpc_contract_test.go:14-145`; `internal/runtime/recovery_test.go:66-95`; `internal/memoryclient/client_test.go:185-237` | 仅标准库且 fake 形式正确；缺 duplicate/out-of-order JSONL、malformed response、每个 durable boundary crash、accepted recovery 等规定失败测试。跨仓库 env-gated E2E 为 **ADR 0040 已接受**。 |
| §12 Acceptance-criteria test map | ⚠️ | `internal/runtime/tracer_test.go:122-213`; `internal/runtime/recovery_test.go:16-125`; `internal/pi/settlement_test.go:9-91` | 命名测试基本存在且当前背景称通过，但多个测试只断言表面结果：settlement 测试不检查 store 持久边界，recovery 测试从 `RecoveryScenario` 重灌空 store，DAG 测试不经过 runtime。 |
| §13 Done criteria | ❌ | `internal/runtime/tracer_test.go:159-182`; `internal/e2e/cross_repo_test.go:107-112,153-199` | 第二轮 citation 与 shared-only exploration 在 fake/manual E2E 中展示；真实 runtime outbox 未用于 E2E，Memory Agent runtime reply/Grant/私有 projection 仍不合规，build/vet 本次按约束未执行。 |

## 问题清单(按 Blocker/Major/Minor/Nit 分级,每条:标题、PLAN 引用、证据 file:line、影响、建议;没有就写『无』)

### Blocker

#### B1. ack、settlement 与 rejection 的耐久边界被折叠
- **PLAN 引用**：§5 InteractionSegment；§6.2；§10（`docs/PLAN.md:383-392,536`）。
- **证据**：`Process.Prompt` 只在读到 `agent_settled` 时返回（`internal/pi/process.go:107-115`）；runtime 返回后才 `AcceptAndOpenSegment`，随后立即 `SettleAndCloseSegment`（`internal/runtime/orchestration.go:707-745`）；任何 Prompt 错误都调用 `FailAndCloseSegment`（`internal/runtime/orchestration.go:723-726`），该 store 会凭空创建 failed Segment 并终结 Delivery（`internal/store/memory/store.go:252-266`）。
- **影响**：ack 后、settled 前崩溃不会留下 accepted/open；negative ack 不再 retryable pending，且错误地产生 Segment。`agent_end` 与 `agent_settled` 虽在 parser 中区分，却没有成为独立持久事实。
- **建议**：把 acknowledgement 暴露为独立事件/回调并在继续读事件前原子 accept+open；negative ack 只释放 claim、保持 pending 且不创建 Segment；仅 `agent_settled` 原子 settle+close。

#### B2. 工具在 `tool_execution_start` 即执行，失败 completion 也可能产生可见消息
- **PLAN 引用**：§6.3；§10（`docs/PLAN.md:394-421,537`）。
- **证据**：runtime callback 丢弃除 start 外的所有事件（`internal/runtime/orchestration.go:707-720`），并在 start 中直接 `PublishToolMessage`（`internal/runtime/orchestration.go:775-812`）；parser 对 end 只转发 ID/name，不解析 `result`/`isError`（`internal/pi/process.go:123-129`）。
- **影响**：`isError:true`、mismatched ID/name、malformed result、甚至永不出现 end 的调用都可发布 canonical speech 或执行 Memory 操作，违反“仅成功 Room 工具可见”。
- **建议**：记录 start 但不产生副作用；按 `toolCallId+toolName` 配对严格解析 canonical result，在可信 Host tool dispatch 成功事务提交后才发布/记录 completion，失败时返回规范错误且不产生可见实体。

#### B3. settlement 与 worker 可见 outbox 不在同一事务
- **PLAN 引用**：§2 step 7；§9；§10（`docs/PLAN.md:28,529,538`）。
- **证据**：runtime 使用两个对象分别承担 DeliveryStore 与 OutboxStore（`internal/runtime/orchestration.go:198-205,240-248`）；settlement 先写 `Store` 自己的 `s.outbox`（`internal/store/memory/store.go:217-247`），返回后再向独立 `Outbox` 调 `Adopt`（`internal/runtime/orchestration.go:741-745`）；drain 只读取后者（`internal/runtime/orchestration.go:966-990`）。
- **影响**：进程在 settle 返回与 Adopt 之间崩溃时，Segment/Delivery 已 settled，但 worker 永远看不到两条 projection，直接破坏 at-least-once 与恢复保证。
- **建议**：同一 concrete durable adapter 同时实现 DeliveryStore/OutboxStore，并让 settlement transaction 直接插入 worker 查询的 rows；删除事后 `Adopt`，恢复只扫描同一 durable table。

#### B4. tracer 创建的 Grants 对 conformant GMS 无效
- **PLAN 引用**：§4.6；§2 step 3（`docs/PLAN.md:134-149,24`）。
- **证据**：tenant token 绑定 `host-service`，但 tracer 把 grants 授给 ordinary/memory agent（`internal/runtime/orchestration.go:251-293`）；Memory grant 使用协议枚举之外的 `Purpose: "memory"`，且 operations 缺 `exploration.redirect`（`internal/runtime/orchestration.go:290-293`）。对比 E2E 正确地授给 token-bound `host-service`（`internal/e2e/cross_repo_test.go:65-79`）。
- **影响**：真实服务会在注册时以 422/400 拒绝非法 purpose，或在后续 recall/stage/exploration 因当前 caller 无 Grant 返回 403；九步 tracer 无法端到端运行。
- **建议**：按 token-bound service Principal 建立合法 `lifecycle`/`tool_plane` exact grants，包含实际调用的完整 operation set；测试 fake 必须验证 purpose、principal 与 operations，而非原样回显任意 body。

#### B5. runtime 启动的真实 Pi 没有 trusted extension，Memory 工具注册表也未验收
- **PLAN 引用**：§6.1（`docs/PLAN.md:364-373`）。
- **证据**：runtime 构造 `LauncherConfig` 时未设置 `ExtensionPath`（`internal/runtime/orchestration.go:688-695`）；launcher 仅在非空时追加 `--extension`（`internal/pi/launcher.go:118-120`）；Memory argv 构造后直接启动，没有 `get_commands`/registry inspection（`internal/pi/launcher.go:77-101,109-129`）。
- **影响**：真实 ordinary/Memory 进程无法获得 Host Room/Memory extension；即使配置异常暴露 coding/network 工具，启动也不会 fail closed。argv isolated test 因显式注入 `EXTENSION` 而掩盖 runtime 缺陷。
- **建议**：把 trusted extension path 设为必填 server config，空值拒绝启动；Memory 启动后执行冻结的 command/tool registry 检查并逐项比较 exact allowlist，发现任何额外敏感工具立即终止。

#### B6. canonical Interaction DAG 未接入 runtime，evidence 不是 source Segment 投影
- **PLAN 引用**：§1、§5、§10、§13（`docs/PLAN.md:3-16,320-341,536-539,566`）。
- **证据**：runtime 的 Pi callback 只处理 tool start，完全不 `AppendEvent`/`PutLink`（`internal/runtime/orchestration.go:707-720`）；这两个方法只在 store 定义（`internal/store/memory/store.go:302-318`）。stage 时重新枚举整个 Room 的 messages，并全部编码为 `room_message`（`internal/runtime/orchestration.go:994-1012`），projection 只改变 Space/`source_kind`（`internal/runtime/orchestration.go:1040-1053`）。
- **影响**：ack、agent_end、tool start/end、terminal、mentions/responds_to 等 canonical facts 不存在；后续 Segment 会重复导出历史 Room，shared/private 内容相同，final assistant private evidence 消失，Graph Memory citation 无法可靠对应源 Segment。
- **建议**：在 ack 后建立 Segment event sequence，严格记录 Pi/tool/terminal 事件和 Host links；结算时从该 Segment 的 immutable DAG 生成 visibility-specific sanitized projections，shared/private 各自 hash、events、links，禁止扫描整间 Room 替代 Segment。

### Major

#### M1. 版本门禁规范化过宽且没有 file-identity cache
- **PLAN 引用**：§6.1（`docs/PLAN.md:364`）。
- **证据**：`parseVersionOutput` 取任意 whitespace fields 的最后一个值（`internal/pi/launcher.go:66-73`），因此 `evil prefix 0.85.1` 也通过；每次 `Start` 都再次执行 `--version`（`internal/pi/launcher.go:51-61,77-80`），没有 path+file identity 状态。
- **影响**：接受 spec 明确禁止的 malformed output；每次 turn 重验但可在身份变化边界之外出现竞态，也未实现冻结 cache 语义。
- **建议**：仅接受 trim 后 `0.85.1` 或精确 `pi 0.85.1`；用 executable canonical path + stat identity 缓存成功结果，identity 变化时失效，并新增 prefix/suffix/build/prerelease/cache invalidation 测试。

#### M2. JSONL parser 不是冻结的严格 RPC decoder
- **PLAN 引用**：§6.1、§6.2、§6.5（`docs/PLAN.md:366,383-389,438`）。
- **证据**：单一宽松 struct + `json.Unmarshal`（`internal/pi/process.go:34-43,91-94`）；response 不校验 `command` 或 success/error 互斥，也不拒绝重复 response（`internal/pi/process.go:95-106`）；`agent_end` 不含/不验证 messages，`agent_settled` 接受额外字段，unknown event 被忽略（`internal/pi/process.go:107-135`）。
- **影响**：wrong command、duplicate/conflicting ack、missing required fields、duplicate JSON keys、额外 settled fields 与 out-of-order events可能被接受，协议错误不能可靠关闭 turn。
- **建议**：按 frame type 使用 strict decoder（duplicate-key detection、unknown-field rejection、单值/size 检查），维护 ack/active/settled 状态机并验证每种 exact shape；oversize 应映射 `PI_PROTOCOL_ERROR`。

#### M3. Tool schema 与 runtime 参数校验均不精确
- **PLAN 引用**：§6.3、§6.4（`docs/PLAN.md:415-432`）。
- **证据**：`room_react` 暴露 `target_message_id` 而非 `message_id`（`internal/tools/tools.go:89-95`）；memory_start 漏 required max_steps/max_results，explore/redirect/submit 也各漏 required 字段（`internal/tools/tools.go:98-138`）；runtime 直接 `json.Unmarshal` 到 struct，未知/authority 字段被静默忽略（`internal/runtime/orchestration.go:781-788,819-826,848-855,875-882,901-908`）。
- **影响**：无效零值请求会被送到 GMS，攻击性 Space-like 字段没有按 spec fail closed，relation/range/domain invariant 依赖远端偶然拒绝。
- **建议**：让 runtime 统一经过 exact schema/typed validator，required、enum、range、unknown fields 全部本地拒绝；修正 react 参数名并对 authority-like 字段返回稳定安全错误。

#### M4. Room tool 幂等作用域、冲突检测与 reaction 实体均错误
- **PLAN 引用**：§5、§6.3、§10（`docs/PLAN.md:357-358,415-421,533`）。
- **证据**：store 每 Room 仅用裸 `clientOperationID` 查 `replyKeys`（`internal/store/memory/store.go:145-172`），不包含 Agent/tool，也不比较原始 args；runtime 将 `room_react` 的 emoji 发布为 `RoomMessage`（`internal/runtime/orchestration.go:779-805`）。
- **影响**：不同 Agent/tool 的同 key 相互碰撞；同 key 改 content 会错误返回旧结果而非 `TOOL_IDEMPOTENCY_CONFLICT`；reaction 没有 `reaction_id`，并污染 canonical message sequence。
- **建议**：以 `(Room,Agent,tool,client_operation_id)` 保存 canonical args hash+result，changed args 原子 conflict；增加独立 Reaction 类型/store 方法，reply 才创建 message/responds_to。

#### M5. Memory session ownership与 cited reply 约束没有 fail closed
- **PLAN 引用**：§4.9、§6.4（`docs/PLAN.md:259,423-434`）。
- **证据**：若 Host 尚无 session，`authoritativeSession` 直接采用模型提供的任意 session；若已有则静默替换 mismatched session，而非拒绝（`internal/runtime/orchestration.go:1206-1211`，调用点 `858,885,911`）。任意 Memory Room tool 都可先成为 `memoryReply`（`internal/runtime/orchestration.go:805-812`），submitted citations 只作 trace 收集（`internal/runtime/orchestration.go:918-927`），二者无验证关系。
- **影响**：可引用未知/跨 Room session；Memory Agent 可在 submit 前或无有效 citation 时发布“cited”回复，测试中的字符串包含关系不是服务端权威。
- **建议**：维护 Room+Agent 绑定的 session registry；模型 session 必须等于 registry 值，否则拒绝；仅在成功 submit 后允许 reply，并从结构化 citation IDs 验证引用属于该 submit 响应。

#### M6. Recall/Room 身份不是从持久配置解析，Room 的 MemoryAgentID 还会写成 ordinary Agent
- **PLAN 引用**：§1、§5、§10（`docs/PLAN.md:14,261-358,535,540`）。
- **证据**：每 turn `CreateRoom` 都把当前 `agentID` 作为 `MemoryAgentID`（`internal/runtime/orchestration.go:601-605`）；首 turn 是 ordinary Agent，后续 existing Room 不更新（`internal/store/memory/store.go:65-82`）。ordinary Recall 直接使用 request 的 `ExecutionAuthority.SharedSpaceID/PrivateSpaceID`（`internal/runtime/orchestration.go:638-645`），store 也直接忽略 ordinary membership 参数（`internal/store/memory/store.go:65-82`）。
- **影响**：唯一持久 Memory Agent 身份错误，private Space/Agent profile 并无 durable authority source；“敌意文本不能改 authority”测试只证明字符串未被解析，不证明调用者不能注入 authority。
- **建议**：先持久创建 Room、members、ordinary profile、Memory profile 与 private-space mapping；turn 输入只携带 Room/message/target identity，orchestrator 从 stores 解析所有 authority，禁止 request 覆盖。

#### M7. Delivery single-flight 和存储 transition invariant 未实现
- **PLAN 引用**：§5、§9、§10（`docs/PLAN.md:296-320,492-503,534`）。
- **证据**：`ClaimNext` 只标记单个 Delivery ID；同一 Room-Agent 的第二次并发 claim 会跳过第一条并领取下一条（`internal/store/memory/store.go:178-194`）。accept/settle/fail store 方法没有调用 `delivery.Transition`，settle/fail 可从 pending 直接发生，segment 不存在时还会创建（`internal/store/memory/store.go:200-247,252-284`）。
- **影响**：同一 Agent 可并发两 turn，非法 state transition 被持久化；独立 `delivery.Transition` 单元测试不能保护真实 adapter 路径。
- **建议**：按 `(Tenant,Room,Agent)` 建 durable lease/claim；所有 adapter mutation 使用同一 transition validator并检查 expected current state、segment identity、outbox count。

#### M8. outbox retry key 不稳定，drain 失败却报告步骤成功
- **PLAN 引用**：§2、§4.7、§10（`docs/PLAN.md:28,32,177-197,539`）。
- **证据**：每次 commit 都生成新的 `deps.IDs.NewID("commit")`（`internal/runtime/orchestration.go:982`）；stage/commit 错误只 reschedule+continue，`drainOutbox` 最终返回 nil（`internal/runtime/orchestration.go:966-990`）；RunTracer 随即把 steps 5/6/7 标完成（`internal/runtime/orchestration.go:321-325`）。
- **影响**：重启后 commit idempotency key 改变；全部 Memory 写入失败时 tracer 仍声称 stage/commit/recovery 完成。
- **建议**：将稳定 `CommitID` 持久化在 outbox row（或确定性派生于 entry ID）；drain 返回逐 row outcome，tracer 仅在两条 projection 都 committed 且状态查询确认后标步骤完成。

#### M9. cited Context Snapshot 只含 citation ID，不含 recalled evidence
- **PLAN 引用**：§2 step 5/8、§6.2、§10（`docs/PLAN.md:26,29,380,535`）。
- **证据**：runtime 从 Recall items 只提取 `CitationID`（`internal/runtime/orchestration.go:657-665`）；prompt 输出仅 `- [citation]`（`internal/runtime/orchestration.go:1174-1195`），丢弃 item content、source_space_id、memory_version、batch/event IDs。
- **影响**：Pi 看不到“第一轮决定了什么”，只能回显 opaque citation；测试证明 citation 字符串被携带，而非第二轮真正利用第一轮 evidence。
- **建议**：构造 bounded、escaped、source-preserving snapshot，至少包含 sanitized content、Space/version 和 durable citation；继续把 Room 输入明确标成 untrusted content。

#### M10. GraphMemoryClient 未完整保持 typed protocol/opaque ID/严格响应契约
- **PLAN 引用**：§4.1、§4.2、§8（`docs/PLAN.md:56-81,481`）。
- **证据**：`ProtocolError` 没有 `details`（`internal/memoryclient/client.go:17-22`）；error body decode 失败被忽略（`internal/memoryclient/client.go:68-78`）；resource/session IDs 直接拼 URL path（`internal/memoryclient/client.go:120-126,159-171`）；`LimitReader(N)` 不检测第 N+1 字节，成功响应用宽松 `json.Unmarshal`（`internal/memoryclient/client.go:64-83`）。
- **影响**：包含 `/`, `?`, `%` 的合法 opaque ID 改变路由；调用方丢失 field/reason details；204/缺字段/未知字段/duplicate-key/超限响应不能按 contract 可靠拒绝。
- **建议**：对 path segment 做正确 escaping；完整建模 error details；读取 `max+1` 并显式报超限；使用 strict response decoder和每方法 required/constant/domain validation，验证 `Content-Type: application/json`。

#### M11. cancellation/失败分类与持久化安全错误码不符合契约
- **PLAN 引用**：§6.2、§6.5（`docs/PLAN.md:390,438`）。
- **证据**：runtime 对所有 `Prompt` error 一律 `FailAndCloseSegment(..., promptErr.Error())`（`internal/runtime/orchestration.go:723-726`）；store 把该字符串写入 `CloseReason`，未设置 `Delivery.FailureCode`（`internal/store/memory/store.go:252-266,405-429`）。
- **影响**：`context` cancellation 不会变成 aborted/`PI_CANCELLED`；任意 Pi/provider error message被持久化，可能包含不安全文本，且消费者拿不到冻结 stable code。
- **建议**：集中 errors.Is 分类到五个 stable code；取消走 Abort；只持久化安全 code/固定摘要，原始 stderr/provider 文本不进入 domain/evidence/loggable state。

#### M12. 恢复测试是从外部 snapshot 重灌空 store，不是 durable restart
- **PLAN 引用**：§2 step 7、§10、§11、§12 criterion 3（`docs/PLAN.md:28,534,547,555`）。
- **证据**：测试创建空 `durableStore/durableOutbox`，`beforeCrash` 从未执行任何写入，随后把 `RecoveryScenario` 直接交给 restarted runtime（`internal/runtime/recovery_test.go:66-95`）；`RecoverAfterRestart` 自己重新 append messages、把 scenario segments 强制 settle、再 Adopt rows（`internal/runtime/orchestration.go:402-485`）。此外 `RecoverPending` 返回 accepted，但 `ClaimNext` 只接受 pending（`internal/store/memory/store.go:178-194,288-299`）。
- **影响**：未证明任何 durable boundary 的进程重建；accepted/open 恢复会被列出后无法 claim；exactly-once visible 仅是手工 fixture 重放结果。
- **建议**：先用一个 orchestrator 写入共享 adapters，在 ack、tool publish、settle、stage、commit 各边界注入 crash，再只重建 orchestrator；恢复逻辑直接扫描 stores，不接受外部 domain snapshot，并为 accepted/open 明确定义 resume/fail 流程。

#### M13. Memory Agent private outbox 指向未注册的伪造 Space
- **PLAN 引用**：§2 step 3/7、§4.7、§10（`docs/PLAN.md:24,28,155-181,538`）。
- **证据**：tracer 只注册 shared 和 ordinary owner private 两个 Spaces（`internal/runtime/orchestration.go:273-280`），Memory turn 却构造 `space-<memory-agent>-private`（`internal/runtime/orchestration.go:347-360`），并为该 Segment 创建/最终 drain private batch（`internal/runtime/orchestration.go:934-961,367-370`）。
- **影响**：conformant GMS 对该 stage 返回 `SPACE_NOT_FOUND`/403，六条 outbox 不会全部 committed；fake server 没验证 Space 存在而掩盖问题。
- **建议**：先由 spec 裁定 Memory Segment private projection 的目标（见“spec 疑问”）；实现只能使用持久注册并 exact-granted 的 Space，禁止字符串拼接 authority ID。

#### M14. 规定的用例包是孤岛，runtime 绕过其 invariant
- **PLAN 引用**：§3（`docs/PLAN.md:34-50`）。
- **证据**：runtime 只 import domain/memoryclient/pi/ports/concrete memory store（`internal/runtime/orchestration.go:17-21`），不组合 `room/delivery/dag/evidence/tools`；真实路径直接调用 store 和本地 switch（`internal/runtime/orchestration.go:601-745,775-929`）。
- **影响**：`delivery.Transition`、tool schema validator、evidence service、DAG authority 等单测通过但对生产路径没有约束，形成“测试绿色、集成语义错误”的结构性原因。
- **建议**：runtime 只编排深模块接口；把 Room publication、Delivery transition、DAG event、tool dispatch、evidence transaction 分别交给对应用例模块，composition root 才选择 concrete adapters。

### Minor

#### m1. Fake Pi fixture 未匹配 request `type`
- **PLAN 引用**：§7（`docs/PLAN.md:440-458`）。
- **证据**：package Pi fixture 对收到的任意一行都输出预录 frames（`internal/pi/fake_pi_test.go:35-43`）；runtime fixture case 只匹配 request ID substring（`internal/runtime/testfixtures_test.go:27-43`）。
- **影响**：即使 Host 发出错误 command/type，只要 ID 碰巧匹配，fixture 仍返回成功，削弱 request-shape 回归检测。
- **建议**：在 POSIX case 中同时匹配 exact opaque id 与 `"type":"prompt"`，不匹配则发 error/退出；保留 literal frame 与无 shell eval 特性。

### Nit

无。

## spec 疑问(没有就写『无』)

1. **九步还是十步？** §2 明确编号 1–9（`docs/PLAN.md:20-30`），§13 却要求 “all ten included architecture scenario steps”（`docs/PLAN.md:566`）。建议冻结为九步，或补出缺失的第十步及对应验收断言。
2. **failed/aborted Segment 是否也必须原子创建两条 outbox？** domain 注释写“Every closed source Segment creates exactly two entries”（`docs/PLAN.md:341-342`），但冻结 storage ports 的 `FailAndCloseSegment`/`AbortAndCloseSegment` 没有 entries 参数（`docs/PLAN.md:500-501`）。若“closed”包含 failed/aborted，需要明确 adapter 如何获得两个 Space/ID 并保持同事务；若仅 settled 产生 evidence，应把 invariant 改成 “Every settled Segment”。
3. **Memory Agent Segment 的 private projection 应写到哪个 Space？** tracer setup 只要求一个 shared 和一个 ordinary-owner private Space（`docs/PLAN.md:24`），但每个 closed Segment 又要求 shared+private 两条（`docs/PLAN.md:28,341-342`）。实现当前拼出未注册 private Space（`internal/runtime/orchestration.go:347-350`）；spec 应明确是新增 Memory-Agent Private、复用某个合法 Private，还是 Memory Segment 例外只投 shared。
4. **冻结 `PiProcess.Prompt` seam 如何表达 ack 后继续等待 settled？** §6.2 要求 ack 立即 durable accept/open、随后继续等待 `agent_settled`（`docs/PLAN.md:383-392`），但 §9 的 `Prompt(...) (PromptAcceptance,error)` 只有最终返回值，`PiEvent` 也没有 acknowledgement frame（`docs/PLAN.md:515-521`）。建议明确 Prompt 是 ack 时返回并由独立 stream 继续事件，还是 callback 增加 response/acceptance 事件；同时 §9 也缺 negative-ack 后释放 claim 的 port 操作。

## 无问题章节清单

- §4.3 Tenant initialization
- §4.4 Principal registration
- §4.5 Space registration
- §7 Fake Pi test subprocess format（除 m1 的 request-type 校验缺口外，脚本生成与 frame 顺序符合；stdin 半关闭为 ADR 0040 已接受）
- 另行确认：`go.mod:1-3` 为 Go 1.26 且没有第三方依赖；源码未发现 Multica/sibling implementation import。环境变量门控的 `internal/e2e` 为 ADR 0040 已接受补充，不计 scope creep。

## 验收自查

1. ✅ `docs/REVIEW-tracer.md` 已创建，包含要求的总体结论、逐节表、分级问题、spec 疑问和无问题章节。
2. ✅ 逐节表覆盖 PLAN 全部 13 个顶层章节，并展开覆盖全部 4.x 与 6.x 子章节。
3. ✅ 每条问题与 spec 疑问均给出 `file:line` 证据及 PLAN 章节/行引用。
4. ✅ 本次仅调用一次文件写入，目标为 `docs/REVIEW-tracer.md`；未调用任何其他写操作，未修改源码、测试或 `docs/PLAN.md`。受当前非交互策略限制，未执行 Git/Go 命令；因此未重复运行用户已声明通过的 `go test ./...`，也未运行 build/vet。
