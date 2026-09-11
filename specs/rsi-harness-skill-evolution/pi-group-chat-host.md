# pi-group-chat-host Module Specification

```yaml
document_status: normative
schema_version: rsih-skill-evolution.pi-group-chat-host.v1
system_contract: ./system-contract.md
system_contract_schema_version: rsih-skill-evolution.system-contract.v1
language: zh-CN
```

> 本文是 `pi-group-chat-host`（以下简称 Host）在 RSI-Harness Skill Evolution 系统中的模块规范。本文服从 [`system-contract.md`](./system-contract.md)（以下简称 Contract）。共享 DTO、跨模块状态与全局不变量只由 Contract 定义；本文只规定 Host 如何生产、校验、保存和传递这些对象。若本文与 Contract 冲突，以 Contract 为准。

## 1. Host authority and exclusions

### 1.1 规范范围

Host 实现声称符合本规范时，MUST 同时符合 Contract §1 的规范词与版本规则、Contract §3.1 的 Host 权威、Contract §5 的全局不变量、Contract §13 的一致性与失败语义，以及 Contract §14 的权限边界。

本文不复制共享 schema。本文引用的共享对象及唯一 schema 来源如下：

| 共享对象 | 唯一定义 |
|---|---|
| `SkillArtifactRef` | Contract §7.3 |
| `CandidateArtifactRef` | Contract §7.4 |
| `SegmentRef` | Contract §7.5 |
| `CheckpointRef` | Contract §7.6 |
| `EvidenceRef` | Contract §7.7 |
| `SkillAnchor` | Contract §7.8 |
| `SkillProposal` | Contract §7.9 |
| `ReplayRequest` | Contract §7.10 |
| `ReplayResult` | Contract §7.11 |
| `ReleaseDecision` | Contract §7.12 |
| `ActivationEvent` | Contract §7.13 |
| `ProjectionWatermark` | Contract §7.14 |
| `GuidanceView` | Contract §7.15 |
| `ExploreResult` | Contract §7.16 |
| `ToolProxyRequest` | Contract §7.17 |
| `ToolProxyResult` | Contract §7.18 |
| `MaterializationManifest` | Contract §7.19 |
| `SkillLock` | Contract §7.20 |
| `VersionedRef`、digest 与 exact-ref 规则 | Contract §6、§7.2 |

Host 内部的 Room、Agent、Delivery、DAG event/link、seal body、调度 receipt 和审计记录是模块私有持久化模型。它们 MAY 采用实现特定 schema，但 MUST NOT 作为新的跨模块共享 DTO，也不得与上表对象同名重定义。

### 1.2 Host 的唯一权威

依 Contract §3.1，Host MUST 唯一拥有：

1. Room identity、Room 生命周期与 Room 内消息序列；
2. Agent identity、Agent membership、Agent role 与 Host scope profile 绑定；
3. Delivery identity、状态、顺序、去重和终态；
4. Interaction DAG 的 event、causal link 与有序 frontier；
5. Segment 边界与 `open|settled|failed|aborted` 状态；
6. Segment close 时的 Decision Checkpoint；
7. Segment Evidence Seal 与 Conversation Path Seal；
8. checkpoint、live execution 与 replay 的调度；
9. 本地 Tool Proxy；
10. 返回给 Pi 的唯一可见 tool result；
11. Memory Agent 的 Room shared Space 与普通 Agent 的 Host profile scope。

### 1.3 Host 明确不拥有

Host MUST NOT：

- 创建、修改、release、activate 或 deactivate canonical Skill；
- 分配 Skill lineage/version；
- 生成或修改 `CandidateArtifactRef`、`ReleaseDecision`、`ActivationEvent`；
- 评价 replay utility、执行 U1 Pareto 比较或选择 release winner；
- 修改 GMS evidence、candidate、evaluation、activation 或 merge ledger；
- 写入 Runtime/Curation Skill Graph；
- 将 Graph node ID、裸名称或 `latest` 当作可执行 Skill ref；
- materialize RSIH bundle 或 `skill.lock.json`；
- 允许模型、Agent 或 Genome 绕过 protected GMS pipeline。

Host MAY 收集模型或 Agent 的 Diagnose/Propose 内容，但 MUST 将其视为非权威数据，并服从 Contract §3.4、§14.3。

### 1.4 前置修复门：Tool Proxy 回传断点

Q15/Q34 是 Host 暴露 Skill retrieval 的硬前置条件：

> 在 S2 验收通过前，Host MUST NOT 向 Pi/Agent 宣称 `memory_explore`、`memory_expand` 或 `skill_get` 已接入 GMS。

仅在 Pi tool call 的同步执行路径中完成 `Pi → Host local Tool Proxy → GMS → exact ToolProxyResult → Pi` 才算接入。Pi `tool_execution_end` 之后的旁路 GMS 调用、日志写入或 UI 展示不构成 Agent 可见结果，MUST NOT 被用于 S2/S7 验收。

### 1.5 权威写与派生写

Host authoritative writes 包括：Room/Agent/Delivery 状态、Interaction DAG、Segment 状态、Decision Checkpoint、seal、Tool Proxy audit 与 replay scheduling record。

以下是 Host 派生读模型，MUST 可从 Host authoritative writes 重建：

- Room transcript view；
- Segment event list view；
- DAG path index；
- Delivery status index；
- replay queue projection；
- Tool Proxy observability dashboard。

派生视图不得成为 seal、message order 或 Pi tool result 的权威来源。

## 2. Room/Agent/Delivery model

### 2.1 Room

Room 是多 Agent 交互的 Host 权威边界，定义遵循 Contract §4。每个 Room MUST 有不可复用的 `room_id` 和单调递增的 `room_event_sequence`。

Room 内任何会影响 replay、seal 或 Agent 观察结果的事件 MUST 在提交时获得唯一 sequence。两个已提交事件不得共享 sequence；已提交 sequence 不得重排或复用。

Room MAY 有实现私有状态，例如 `active|closing|closed`，但 Room 状态不得替代 Segment 状态。Room 关闭 MUST 阻止新 Segment 和新 Delivery；已提交的 settled Segment 与 seal 保持可读取。

### 2.2 Agent identity 与 membership

Host MUST 为每个参与者绑定：

- 不可复用 `agent_id`；
- 所属 `room_id`；
- role；
- membership 有效区间；
- exact scope profile ref；
- Host authority cap；
- Pi session/runtime correlation（适用时）。

Display name、模型名称或 prompt 名称不是 identity。身份变更 MUST 产生新的 Host 审计事件，不得改写历史 event 的 actor。

### 2.3 Memory Agent scope

依 Q13 与 Contract §12.1：

1. role 为 Memory Agent 的 Agent MUST 只能使用当前 Room 的 shared Space；
2. Memory Agent MUST NOT 跨 Room 查询、扩展或复用 served-item fence；
3. Memory Agent 的 `ToolProxyRequest.scope_profile_ref` MUST 解析为 Room shared profile；
4. Host MUST 校验请求 `room_id` 与 Agent membership 一致；
5. GMS 即使返回跨 Room 数据，Host 也 MUST 将整个响应视为 scope violation 并 fail closed，不得只删掉违规条目后继续返回。

### 2.4 Ordinary Agent scope

普通 Agent 的可见范围 MUST 由 Host exact versioned scope profile 决定，而不是 Agent 自报。Profile 至少决定：

- 当前 Room/Segment 可见性；
- shared/private evidence 范围；
- Skill retrieval 是否允许；
- `memory_explore|memory_expand|skill_get` 工具集合；
- Q35 budgets 上限；
- 最低 projection freshness 策略；
- Host permission cap。

Profile 更新只影响更新后的请求，不得改变已返回 `ToolProxyResult` 或已 settled Segment。

### 2.5 Delivery identity 与状态

Delivery 是 Host 对一次定向 Agent 输入或工具结果交付的权威记录。v1 内部状态 MUST 至少表达：

```text
queued → in_flight → delivered
                  ↘ failed
                  ↘ cancelled
```

规则：

1. `queued` 之前 MUST 分配不可复用 `delivery_id` 与 idempotency identity；
2. 只有一个 worker 可通过 CAS 将同一 Delivery 从 `queued` 推进到 `in_flight`；
3. `delivered|failed|cancelled` 是终态，不得重开；
4. Retry MUST 复用原 Delivery/idempotency identity，或创建明确关联的新 attempt；不得伪装成首次交付；
5. Tool result Delivery 只有在 exact `ToolProxyResult` 已进入 Pi tool return path 后才可 `delivered`；
6. 超时后到达的 upstream 响应不得把 `failed|cancelled` 改成 `delivered`。

### 2.6 消息顺序

Host MUST 以提交 sequence，而非到达时钟、模型输出时间或展示顺序，确定 Room/Segment 消息顺序。相同 Room 中：

- append 必须 CAS 当前 sequence；
- 重复 idempotency key + 相同 payload digest MUST 返回原 event；
- 相同 key + 不同 digest MUST `IDEMPOTENCY_CONFLICT`；
- clock timestamp 仅供审计，不参与因果排序。

### 2.7 Transcript view 不得成为 replay 输入权威

当前 Room 的累计 transcript MAY 作为 UI/Agent 上下文派生视图，但 Host MUST NOT 在 Segment settled 后通过“重新拼接当前累计 transcript”生成 replay 输入。Replay 必须使用 seal 固定的 event refs、payload digests、causal links 与边界。后续 Room 事件不得渗入历史 Segment fixture。

## 3. Interaction DAG and Segment lifecycle

### 3.1 Interaction DAG

Host MUST 在生产路径实际提交 Interaction DAG event 与 causal link；只在内存中拼接 transcript 不满足本规范。

DAG event 至少应区分以下模块私有类别：

- Agent/user message；
- model/Pi response；
- tool call；
- tool result；
- Delivery transition；
- Segment open/terminal transition；
- Decision Checkpoint；
- replay dispatch/completion correlation。

DAG link 至少应表达：

- Room sequence precedence；
- response-to；
- caused-by；
- tool-call-to-result；
- Delivery-of；
- Segment membership；
- replay-of sealed path。

这些 link 是 Host Interaction DAG 的内部关系，不是 Contract §11.3 的九种 Skill Graph relation，MUST NOT 被投影为自由 Skill relation。

### 3.2 DAG append 不变量

1. Event body 与 payload digest 一经提交 MUST 不可变；
2. 新 link 只能引用已存在 event 或与同一原子事务创建的 event；
3. DAG MUST 无因果环；Room sequence edge 必须单调；
4. Tool result 必须唯一关联其 tool call 与 Delivery；
5. Segment event membership 必须在 settled 前冻结；
6. settled/failed/aborted 后不得向该 Segment 追加 event/link；
7. 纠错、补充或恢复必须进入新 event 和新 Segment。

### 3.3 Segment 状态机

v1 Segment 状态封闭为：

```text
open ──settle──> settled
  ├──fail──────> failed
  └──abort─────> aborted
```

`settled|failed|aborted` 均为终态，不得重开或互转。

| 状态 | 进入条件 | 允许写入 | 合法退出 |
|---|---|---|---|
| `open` | Host 分配 segment identity、起始 frontier 与 owner Room | event、link、Delivery、tool correlation | settled/failed/aborted |
| `settled` | protected close 全部条件满足，checkpoint/seals/refs 原子提交 | 只读与派生索引 | 无 |
| `failed` | Host 内部错误或完整性错误导致无法形成合法 close | 只追加独立诊断审计，不写入原 Segment | 无 |
| `aborted` | 合法取消、Room 关闭或 operator policy 在 close 前终止 | 只追加独立取消审计，不写入原 Segment | 无 |

v1 settled Segment 的 `segment_version` SHOULD 为 `1`。已 settled Segment 不得 reseal；如需新的解释或补充，MUST 创建新 `segment_id`，而非增加旧 Segment version。

### 3.4 Segment open

Host 创建 `open` Segment 时 MUST 固定：

- Room identity；
- 起始 Room sequence/frontier；
- Segment owner/scheduler correlation；
- applicable Host profile refs；
- allowed Agent membership snapshot；
- idempotency identity；
- open reason。

模型/Agent MAY 请求开始或结束 Segment，但只有 Host protected lifecycle service 可以改变 Segment 状态。

### 3.5 Settled close 的前置条件

Host 仅在以下条件全部满足时 MAY settle：

1. Segment 仍为 `open`，且 close CAS 的 expected state/head 匹配；
2. event membership 与 Room sequence 连续、可解析；
3. 所有纳入 Segment 的 Delivery 和 tool call 均到达终态；
4. 每个 tool call 有唯一 tool result 或明确 terminal failure；
5. DAG 无环且所有 causal refs 可解析；
6. success/failure/recovery 路径已由 protected path extraction 发现并分类；
7. Evidence Seal body 可 canonicalize/digest；
8. Conversation Path Seal body可 canonicalize/digest；
9. Segment digest、Contract §7.5 `SegmentRef` 可产生；
10. terminal Decision Checkpoint 与 Contract §7.6 `CheckpointRef` 可产生；
11. permission/scope 审计没有未解决违规；
12. 原子 close transaction 可一次提交。

任一条件不满足时不得产生部分 settled Segment。可恢复基础设施错误 MAY 保持 `open` 并 bounded retry；确定性完整性错误 MUST 进入 `failed`。

### 3.6 Decision Checkpoint 是 Host 权威 Segment close

v1 中，每个 settled Segment MUST 恰有一个 terminal Decision Checkpoint；该 checkpoint 就是 Host 权威的 Segment close。

规范顺序：

```text
freeze open Segment frontier
→ validate DAG and terminal Deliveries
→ extract paths
→ compute immutable seals
→ compute SegmentRef
→ compute terminal CheckpointRef referencing SegmentRef
→ atomically commit settled + refs + checkpoint audit event
```

为避免摘要循环：

- Segment/seal 摘要 preimage MAY 包含 checkpoint identity/sequence 和 close intent；
- MUST NOT 包含尚未计算的 `checkpoint_digest`；
- `CheckpointRef` 按 Contract §7.6 引用已确定的 `SegmentRef`；
- checkpoint audit event MAY 引用 `CheckpointRef`，但不得反向改变 Segment/seal 摘要。

只有 settled close 的 `CheckpointRef` 可供 GMS `SkillAnchor` 使用（Contract §7.8、§11.4 `anchored_at`）。Failed/aborted Segment MUST NOT 产生可锚定 Skill 的 Decision Checkpoint。

### 3.7 Close transaction

Close transaction MUST 原子持久化：

- Segment terminal state=`settled`；
- frozen start/end frontier；
- Evidence Seal exact ref；
- Conversation Path Seal exact ref；
- Contract §7.5 `SegmentRef`；
- terminal checkpoint identity/sequence/digest；
- Contract §7.6 `CheckpointRef`；
- close audit event 与必要 DAG membership/link；
- close idempotency result。

相同 close idempotency key + 相同 frozen input MUST 返回原 refs；不同 frozen input MUST `IDEMPOTENCY_CONFLICT`。

### 3.8 Failed 与 aborted

`failed` 用于系统无法证明 close 完整性，例如 DAG cycle、payload digest mismatch、缺失 tool result 或 seal canonicalization failure。`aborted` 用于明确取消，而不是把错误伪装成成功结束。

Failed/aborted Segment：

- MUST 保留已有 Host audit records；
- MUST NOT 产生 Contract §7.5 `SegmentRef`；
- MUST NOT 产生可用于 `SkillAnchor` 的 Contract §7.6 `CheckpointRef`；
- MUST NOT 被用于 proposal/replay fixture；
- MUST NOT 被原地修复为 settled；
- 恢复必须创建新 Segment，并以模块私有审计 correlation 指向旧 Segment。

### 3.9 Segment 与 Skill proposal 的边界

Host MAY 在 settled close 后调度 Diagnose/Propose，并提供 exact `SegmentRef`、`CheckpointRef` 与 seal refs。Host MUST NOT 自行构造 canonical `SkillProposal` 内容或 Candidate；Contract §7.9 的权威提交和 canonical validation由 GMS 负责。

## 4. Evidence Seal and Conversation Path Seal

### 4.1 Seal 通用不变量

Evidence Seal 与 Conversation Path Seal 是 Host authoritative immutable records，遵循 Contract §4、§5.1、§6。Seal MUST：

- 只引用 frozen Segment membership 中的 exact event/payload refs；
- 使用 JCS + SHA-256 与 integer-only hashed core；
- 具有不可复用 identity、version 与 digest；
- 在 settled close transaction 中固定；
- 不含 mutable URL、`latest`、裸名称或当前 transcript pointer；
- 不因 GMS、Graph、模型或后续 Room 事件变化而改变；
- 可由授权 reader 按 exact ref 重建 canonical bytes。

Seal 是 Host 对来源、顺序和路径的证明；它不等同于 GMS 已 commit `EvidenceRef`。GMS 是否接受/commit evidence 由 GMS 权威决定（Contract §3.2、§7.7）。

### 4.2 Evidence Seal 必须覆盖的语义

Evidence Seal 的模块私有 canonical body MUST 固定：

1. Room、Segment identity 与 frozen sequence interval；
2. ordered event refs 与 payload digests；
3. actor/Agent identities；
4. Delivery identities 与终态；
5. tool call/result correlation 与 exact ToolProxyResult digest（适用时）；
6. causal link set digest；
7. included evidence candidates；
8. excluded/omitted event refs 及 machine-readable reason；
9. applicable Host scope/profile refs；
10. close policy/version；
11. canonicalization/schema version。

Seal 不得复制可从 exact payload ref 获取的大正文，除非保存正文是验证 canonical bytes 所必需；任何复制内容必须有 digest 相等校验。

### 4.3 Conversation Path Seal 必须覆盖的语义

Path Seal MUST 固定所有 protected path extraction 已发现且与决策相关的路径：

- `success`；
- `failure`；
- `recovery`。

每条路径至少 MUST 固定：

- path identity；
- ordered event refs；
- causal link refs/digest；
- start/end frontier；
- terminal outcome classification；
- tool/Delivery outcomes；
- branch/checkpoint correlation；
- completeness status；
- extraction policy/version。

Path label 必须由 protected deterministic policy 或受保护人工 attestation 决定。模型给出的 label 只可作为 suggestion。无法可靠分类的路径 MUST 标记为未用于 fixture，并记录 reason；不得强行归为 success。

### 4.4 Branch preservation

依 Contract §10.5、§15 S3/S4，Host MUST 保留所有已发现 success/failure/recovery branches，不得只封存最终成功 transcript。Recovery path MUST 同时保留触发 failure 的前缀与恢复动作，以便 paired replay 能重现因果上下文。

### 4.5 Seal 读取与披露

Host MUST 按请求者 scope 返回 seal 或其授权子集。若授权子集会破坏 digest 可验证性，Host MUST 返回 exact seal ref + denied fields metadata，而不是生成冒充原 seal 的新内容。任何 redacted derivative 必须有独立 identity/digest，且不得替代权威 seal。

### 4.6 Seal retention

Settled Segment 的 seals MUST 至少保留到所有引用它们的 proposal、candidate、replay、release 与 SkillAnchor 的保留期结束。若系统支持删除底层 payload，必须先满足外部引用与审计 retention policy；不可解析的 seal 不再符合本规范。

## 5. Tool Proxy

### 5.1 目的与硬门

Tool Proxy 修复当前“GMS 在 Pi tool lifecycle 之后被调用、真实结果未返回 Pi”的断点。依 Contract §7.17、§7.18、§12.2、§12.7、§15 S2，唯一合法主路径是：

```text
Pi tool call
→ Host intercepts before tool completion
→ Host validates and persists ToolProxyRequest audit
→ Host local Tool Proxy calls GMS
→ GMS returns typed upstream result
→ Host validates scope/schema/digest/budget/watermark
→ Host creates exact ToolProxyResult
→ Host returns that exact result through the same Pi tool call
→ Pi observes tool result
→ Host records delivered/tool_execution_end audit
```

Host MUST 在向 Pi 暴露 Skill retrieval 前完成并通过 S2。Feature flag、tool registration 和 advertised capability MUST 以 S2 readiness 为前置条件。

### 5.2 共享 DTO 引用

Tool Proxy request MUST 使用 Contract §7.17 `ToolProxyRequest`，result MUST 使用 Contract §7.18 `ToolProxyResult`。本文不增加或重新解释其字段。Canonicalization、digest 与 extensions 使用 Contract §6、§7.1、§16 的 shared conformance fixtures。

按 tool name，成功 `result` 必须承载 Contract 定义的对象：

- `memory_explore`：Contract §7.16 `ExploreResult`；
- `memory_expand`：由 GMS 接口声明并受 Contract §7.16/§7.15 约束的扩展结果；若没有冻结 shared result schema，v1 MUST fail closed，不得返回自由 JSON；
- `skill_get`：Contract §7.15 `GuidanceView` 或后续由 Contract 冻结的 exact artifact-read DTO；在 schema 冻结前不得返回自由 artifact body。

【修订 v1.1，CTR-002】Host 对 success `result` 的校验是 tool-specific 的，以 Contract §12.7.1 冻结的 validation matrix 为准（权威文件 conformance `policy/tool-success-validation.v1.json`，JCS digest 冻结于该条款）：`memory_explore`/`memory_expand` 要求 watermark、budgets、citations（explore-family 规则集）；`skill_get` 只验证 closed `GuidanceView` 的适用字段，MUST NOT 要求 watermark/budgets/citations，其 freshness/authorization 由 ToolProxyRequest 字段与权威 read audit 记录承载。`skill_get` 在该条款 readiness gate（fixture+policy 全绿）通过前保持 disabled（fail closed，Host 侧 `HOST_TOOL_NOT_ALLOWED`）。

### 5.3 Request construction

Host MUST 从自身权威状态填充并验证：

- `proxy_request_id`；
- `room_id`；
- `agent_id`；
- `delivery_id`；
- allowed `tool_name`；
- exact `scope_profile_ref`；
- `idempotency_key`；
- profile-capped `timeout_millis`；
- 适用的 `requested_min_activation_sequence`。

Agent/model 提供的 `room_id`、`agent_id`、scope、budget、timeout 或 freshness 只可视为请求建议。Host MUST 使用权威 membership/profile 覆盖或拒绝，不得信任自报值。

### 5.4 Synchronous return-path requirement

Host MUST 使 Pi 的 tool execution future/promise 在以下之一发生前保持未完成：

- exact success `ToolProxyResult` 已构造并进入 Pi return channel；
- exact failed/inconclusive `ToolProxyResult` 已构造并进入同一 channel；
- Pi/session 已由权威 cancellation 终止。

Host MUST NOT：

- 先返回 placeholder/empty result 再异步查询 GMS；
- 在 `tool_execution_end` 后把 GMS 结果只写日志；
- 把结果注入下一条 user/system message；
- 用 UI、side channel 或 Memory Agent 私有状态代替当前 tool result；
- 在 Pi 已收到 terminal result 后用 late response 改写它。

### 5.5 Exact response 语义

“Exact”至少意味着：

1. Host 构造的 Contract §7.18 对象按 Contract §6/§16 canonical profile 序列化；
2. Pi tool channel 接收相同 canonical payload，不得删字段、重排语义数组、摘要化或二次自然语言改写；
3. GMS 有响应时，`upstream_result_digest` 必须验证 exact upstream payload；
4. 请求尚未发出、无 upstream response、Host timeout 或 cancellation 时，`upstream_result_digest` MUST 是 Host 模块私有 immutable upstream-attempt record 的 canonical digest；该 record 必须明确区分 `not_started|no_response|cancelled`，不得伪造 GMS payload；
5. `proxy_result_digest` 必须通过共享 golden fixture 所冻结的 digest procedure；Host 模块不得自创 digest preimage；
6. Host 持久化的 delivered audit digest 与 Pi 实际接收 payload digest 必须相同。

Host conformance suite MUST 在 Contract §16 的 fixture suite 上增加 ToolProxyRequest/ToolProxyResult success、no-response、timeout、cancel 与 late-response cases。若 `proxy_result_digest` 的共享 digest procedure 尚未由 fixture 冻结，S2 MUST 视为未通过。

如果 Pi runtime 必须使用 content blocks，Host MUST 使用可逆、确定性 wrapper，并在 S2 fixture 中证明解包后的 canonical bytes 与 ToolProxyResult 相同。

### 5.6 Upstream validation

成功返回前 Host MUST 校验：

- schema/version 已知；
- required extensions 可理解；
- digest 正确；
- Room/Agent scope 不越权；
- result type 与 tool name 匹配；
- Contract §12.4 budgets 未超限；
- Contract §12.6 identity/evidence citations 分离；
- watermark 存在；
- 强新鲜度请求满足 `min_activation_sequence`。

任一失败 MUST 返回失败 ToolProxyResult，不能删掉违规结果后返回其余内容，因为那会改变 upstream exact result 与 ranking/budget语义。

【修订 v1.1，CTR-002】上表为 v1 基线清单；按 Contract §12.7.1 matrix，其各项按 tool 适用性执行：watermark/budgets/citations 检查仅适用于 `memory_explore`/`memory_expand`（explore-family）；`skill_get` 只验证 closed `GuidanceView` 完整性、`view_hash` 一致性与 policy 冻结的 profile 版本下限，scope/freshness 由 request 字段与权威 read audit 记录承载。向 `GuidanceView` 私添 watermark 等字段的 payload MUST 以 `SCHEMA_FIELD_UNKNOWN`（Host 侧 `UPSTREAM_SCHEMA_INVALID`）fail closed；wrapper/free JSON、未知 tool、tool/result type mismatch 一律 fail closed。

### 5.7 Idempotency 与并发

Host MUST 以 `(room_id, agent_id, delivery_id, idempotency_key)` 绑定请求。行为：

- 相同 key + 相同 canonical request digest：返回已保存 exact terminal ToolProxyResult，禁止二次产生不同 Agent observation；
- 相同 key + 不同 digest：`IDEMPOTENCY_CONFLICT`；
- 同一 request 的并发 worker：只有一个获得 execution lease，其余等待或读取终态；
- lease 过期可由新 worker接管，但不得产生两个 Pi-visible result；
- GMS retry 必须复用 upstream idempotency identity。

### 5.8 Timeout、cancel 与 late result

Timeout/cancel MUST 产生 exact failed/inconclusive ToolProxyResult 并通过同一 Pi return path 返回（除非整个 Pi session 已不可逆取消）。Late GMS response：

- MUST 记录为 late audit；
- MUST NOT 修改 terminal Delivery；
- MUST NOT 注入 Pi；
- MUST NOT 作为后续 tool call 的结果；
- MAY 供运维诊断，但不得成为 Segment evidence，除非后续新 Segment 以新 observation 明确纳入。

### 5.9 Host-local v1 proxy reason codes

Host 自身生成 Contract §7.18 `error.reason_code` 时，MUST 从以下 Host-local 封闭集合选择：

- `HOST_PROXY_INVALID_REQUEST`
- `HOST_AGENT_NOT_IN_ROOM`
- `HOST_SCOPE_DENIED`
- `HOST_TOOL_NOT_ALLOWED`
- `HOST_PROXY_TIMEOUT`
- `HOST_PROXY_CANCELLED`
- `GMS_UNAVAILABLE`
- `UPSTREAM_SCHEMA_INVALID`
- `UPSTREAM_DIGEST_MISMATCH`
- `UPSTREAM_SCOPE_VIOLATION`
- `UPSTREAM_BUDGET_VIOLATION`
- `PROJECTION_BEHIND_REQUIRED_SEQUENCE`
- `IDEMPOTENCY_CONFLICT`
- `PI_RETURN_CHANNEL_FAILED`

上游 reason code 只有在其所属 exact GMS/system policy 中已被枚举、Host 能识别且保持原义时 MAY 透传；未知上游 code MUST 映射为 `UPSTREAM_SCHEMA_INVALID` 并把原 code 仅作为非行为型诊断 metadata 保存。Host-local 集合不重新定义共享字段的全系统值域；全系统 reason-code registry 仍由 Contract/GMS 的版本化 policy 权威拥有。

自由文本 message 仅供人类诊断，不得驱动 retry 或权限行为。Retryability 必须由 exact Host policy 决定。

系统 reason-code registry、ownership、precedence（含未知 code 替换与 late result audit-only）与两份 policy 的 JCS digest，以 Contract §13.7.1（修订 v1.1，CTR-004）冻结的 policy 为准。

### 5.10 Tool audit 与 DAG

每次 Tool Proxy 执行 MUST 在 Interaction DAG 中形成：

```text
tool_call_event
  → proxy_request_audit
  → upstream_completion_or_failure
  → tool_result_event
  → Delivery terminal event
```

只有 Pi 实际收到的 ToolProxyResult 可作为 `tool_result_event` payload。Upstream late/side-channel result 必须使用不同审计类别，不得连接为 tool result。

## 6. Replay scheduling

### 6.1 Host 职责

依 Contract §3.1、§10.1、§10.5，Host 拥有 replay scheduling，但不拥有 evaluator 或 `ReplayResult` 的权威判定。职责边界：

```text
GMS freezes ReplayRequest
→ Host validates schedulability and sealed inputs
→ Host dispatches baseline/candidate runs to RSIH HarnessRunner
→ RSIH executes deterministic adapter
→ Host correlates execution outputs
→ GMS canonicalizes authoritative ReplayResult and evaluates release
```

Host MUST NOT 计算 U1 utility winner、修改 fixture outcomes 或把基础设施成功等同于 candidate 通过。

### 6.2 ReplayRequest 接受条件

Host 只接受 Contract §7.10 `ReplayRequest`。调度前 MUST 验证：

1. schema/digest/exact refs；
2. `mode=causal_evaluation`；
3. 所有 `segment_refs` 可解析且对应 settled Segment；
4. Evidence/Path seals 可解析且 digest 匹配；
5. fixture set refs 与 sealed path coverage 一致；
6. baseline/candidate refs 不含裸名称、`latest` 或 Graph node ID；
7. runtime adapter/profile exact refs 可用；
8. merge 请求有两个 `required_source_heads`；
9. permission requirements 不超过 Host cap；
10. idempotency key 未冲突。

Host 不负责判断 source head 是否仍为 active；该 hard gate 属 GMS。Host MUST 保留并回传该 expectation，不得删除。

### 6.3 Frozen replay plan

调度时 Host MUST 形成模块私有 frozen plan，固定：

- ReplayRequest exact digest；
- baseline/candidate run identities；
- fixture execution order；
- sealed event/path inputs；
- runtime adapter/profile versions；
- environment allowlist；
- permissions；
- timeout/retry policy；
- output capture/digest policy。

Frozen plan 不得在 retry 时改变。任何实质输入变化必须形成新的 ReplayRequest。

### 6.4 Paired execution

Baseline 与 candidate MUST：

- 使用同一 fixture family 与顺序；
- 使用同一 deterministic fake runtime adapter version；
- 使用相同初始状态、权限、environment 与 deterministic seeds；
- 隔离可变工作目录、cache 和 side effects；
- 捕获 exact inputs/outputs/reason codes；
- 不读取 replay 开始后的 Room transcript 或 active Graph 变化。

Host MAY 顺序或并行调度 paired runs，但结果 correlation MUST 不依赖完成顺序。

### 6.5 Success/failure/recovery coverage

依 Contract §10.5，Host MUST 从 Conversation Path Seal 调度所有 fixture 声明的 success/failure/recovery paths。不得因 candidate 在 success path 通过而跳过 failure/recovery。Merge replay 还 MUST 调度 A-only、B-only、overlap/conflict families（Contract §9.4.6、§10.4）。

### 6.6 Retry

Host 只可对明确标记 retryable 的基础设施失败进行 bounded retry，例如 worker unavailable。语义执行失败、permission denial、digest mismatch、fixture assertion failure 不得自动 retry 成“成功”。Retry：

- 复用 ReplayRequest/idempotency identity；
- 创建明确 attempt correlation；
- 不改变 frozen plan；
- 不覆盖先前 attempt；
- 超限后上报 failed/inconclusive execution outcome。

### 6.7 Result authority

Host MAY 保存模块私有 dispatch/attempt/output records，但 MUST NOT 自行铸造 Contract §7.11 的权威 `ReplayResult`，除非 GMS 模块规范明确将 canonicalization adapter 部署在 Host 进程且逻辑 authority 仍属于 GMS。无论部署位置，GMS 必须拥有最终 record identity、schema validation 与 ledger write。

### 6.8 Cancellation

Replay cancellation MUST 记录授权来源与终止 attempt。取消不得产生 accepted evaluation。部分输出可作为诊断数据，但只有 GMS 明确 canonicalize 为 `inconclusive` 后才能进入评估 ledger。

## 7. Memory Explore integration

### 7.1 集成前置条件

S7 必须依赖 S2 已通过。Host MUST NOT 在 Tool Proxy exact return path 未通过验收时注册或宣传 Memory Explore Skill retrieval。

### 7.2 Scope enforcement

依 Contract §12.1：

- Memory Agent → 当前 Room shared Space；
- ordinary Agent → Host exact scope profile；
- 跨 Room request → fail closed；
- profile 不允许的 tool/result type → fail closed；
- Agent 自报 scope 不具权威性。

Host MUST 将 exact `scope_profile_ref` 放入 Contract §7.17 `ToolProxyRequest`。GMS MUST 再校验 scope；双重校验不改变 Host 的责任。

### 7.3 ExploreSession 与 fences

Host MUST 将 ExploreSession 绑定到 `(room_id, agent_id or memory-agent-role, session identity, scope_profile_ref)`。同一 ExploreSession：

- Evidence 与 Skill 结果共享 session identity；
- 使用 Contract §7.16 的独立 evidence/Skill served fences；
- fence 不得跨 Room、scope profile 或不同 Agent 私有空间复用；
- retry 返回相同 exact result 时不得重复消费 fence；
- 新查询必须有新的 query digest 或明确 continuation identity。

### 7.4 Q35 budgets

Host exact profile MUST 给出最大：

- total result cap；
- evidence subcap；
- Skill subcap；
- Guidance View token budget；
- timeout；
- expansion depth（若启用）。

Agent 请求值 MAY 更小，不得更大。Host MUST 在发往 GMS 前计算 effective budget。Effective budget 必须通过 Contract §7.17 `arguments` 中由后续 GMS 模块规范绑定的 tool-specific exact request schema 传递，并与 `scope_profile_ref` 可复核；Host MUST NOT 自创 ad-hoc budget object。在该 tool-specific schema 尚未冻结前，S7 MUST 视为不具备发布条件。

GMS 返回的 Contract §7.16 `ExploreResult` MUST 满足：

```text
total_used <= total_cap
evidence_count <= evidence_subcap
skill_count <= skill_subcap
sum(GuidanceView.content_token_count) <= guidance_token_budget
```

Host 不得通过本地裁剪超额 upstream result 来“修复”违规；必须返回 `UPSTREAM_BUDGET_VIOLATION`，以保持 exact response、ranking 与 served fences 一致。

【修订 v1.1，CTR-003】截断 success 的 top-level omitted evidence/Skill refs 由 Contract §12.7.2 冻结为 `ExploreResult.omissions` carrier（typed exact refs + 4 个 truncation code + 排序/去重/accounting/fence 前进义务）；Host MUST 原样透传该 carrier，不得本地补写、删改或以裁剪替代。

### 7.5 Freshness 与 watermark

每个 ExploreResult MUST 包含 Contract §7.14 `ProjectionWatermark`。若 Host profile 或 request 要求 `min_activation_sequence`：

- Host MUST 将其放入 ToolProxyRequest；
- GMS watermark 未追平时 Host MUST 返回 `PROJECTION_BEHIND_REQUIRED_SEQUENCE`；
- Host MAY 根据 exact retry policy 等待/重试；
- Host MUST NOT 静默降低 sequence、动态拼图或返回 candidate；
- Skill deployment/materialization 不走此 Graph fallback，而应读取 GMS 权威 active exact refs（Contract §5.3.6）。

### 7.6 Typed results 与 citations

Host MUST 验证 Contract §7.16：

- evidence/Skill result types 不混用；
- artifact identity citation 与 evidence citation 分离；
- 每个 Skill result 有 exact `SkillArtifactRef` 与 Contract §7.15 `GuidanceView`；
- Guidance View 有 source ref、profile/policy、branches、context hash、view hash；
- omitted/truncated 内容有 reason 与 expandable refs。

Host 不得把 evidence citation 改写成 Skill identity，也不得因显示方便删除 exact refs。

### 7.7 Ranking 与结果透明性

GMS 负责 Contract §12.3 deterministic lexical + graph ranking。Host MUST：

- 原样保留 result order；
- 不本地 re-rank；
- 不让模型在 Pi 观察前过滤；
- 将 watermark、budget、truncation 与 citations 一并返回；
- 将 A/B/M retained active 产生的重叠结果作为合法结果，不自动隐藏 source Skills。

### 7.8 Expansion

`memory_expand` 仅可扩展已在同一 ExploreSession 中合法服务且 fence 允许的 exact ref。若 shared expansion result schema 尚未由 Contract 冻结，v1 MUST 禁用该 tool，而不是使用自由 JSON。Expansion 不得扩大 Room/scope/profile 权限或刷新 session closure。

### 7.9 Skill retrieval 与执行分离

Memory Explore 返回 Guidance View 是只读检索，不代表：

- Skill 已绑定到 RSIH S-slot；
- 当前 session 已 materialize；
- Agent 获得新增权限；
- Graph node 可执行；
- source evidence 已被当前 Agent验证。

执行仍需 GMS active exact ref 与 RSIH materialization 流程（Contract §3.2-3.3、§7.19-7.20）。

## 8. Failure and recovery

### 8.1 总原则

Host MUST fail closed，保留 append-only 审计，并以新 attempt/event/Segment 恢复。不得通过改写 settled Segment、seal、Pi-visible result 或 replay plan 恢复。

### 8.2 失败矩阵

| 失败 | Host 终态/响应 | 可否 retry | 禁止行为 |
|---|---|---|---|
| Segment DAG cycle/ref missing | Segment `failed` | 新 Segment | 原地删 edge 后 settle |
| Seal canonicalization/digest failure | Segment `failed` | 新 Segment | 产生部分 SegmentRef |
| 合法取消 open Segment | Segment `aborted` | 新 Segment | 标记 settled |
| Delivery worker unavailable | 保持 queued 或 attempt failed | bounded | 重复 Agent observation |
| GMS unavailable | failed ToolProxyResult | policy允许 | placeholder success |
| GMS timeout | failed/inconclusive ToolProxyResult | bounded | late result 注入 Pi |
| Upstream schema/digest invalid | failed ToolProxyResult | 通常否 | 本地修补 payload |
| Scope/budget violation | failed ToolProxyResult | profile变更后新请求 | 过滤后部分返回 |
| Projection behind required sequence | failed/inconclusive ToolProxyResult | 等待/重试 | 降低 freshness |
| Pi return channel failure | Delivery failed | session policy | 声称 Agent 已观察 |
| Replay infrastructure failure | attempt failed/inconclusive | bounded | 修改 frozen plan |
| Replay semantic failure | 上报 GMS | 否 | 自动重跑到成功 |
| Replay cancel | cancelled/inconclusive evidence | 新请求 | accepted decision |

### 8.3 Segment recovery

Failed/aborted Segment 不得重开。恢复流程：

```text
old Segment terminal audit
→ create new Segment with new identity
→ optionally correlate recovery_of internally
→ append/replay required events explicitly
→ run full close/seal checks
```

新 Segment 不得继承旧 Segment 尚未验证的 seal/ref。

### 8.4 Tool Proxy recovery

相同 idempotency request 的 retry 必须返回同一 terminal result，或由 lease owner继续原请求。若业务上需要重新查询，必须创建新 `proxy_request_id`、Delivery 与 idempotency key，并让 Agent 明确观察这是新 tool call。

### 8.5 Replay recovery

基础设施 retry 必须保持 ReplayRequest 与 frozen plan。输入、fixture、adapter、permission 或 timeout policy 改变时必须由 GMS 创建新 ReplayRequest。Host 不得自行 rebase stale candidate/source heads。

### 8.6 Observability

Host MUST 为以下关联提供结构化、可查询审计：

- Room sequence → DAG event；
- Agent → membership/profile；
- Delivery → payload/tool result；
- Segment → seals/checkpoint/terminal state；
- ToolProxyRequest → upstream attempt → ToolProxyResult → Pi delivery；
- ReplayRequest → dispatch attempts → captured outputs；
- error reason code → retry/cancel outcome。

日志不得作为权威记录的替代。Observability 输出必须避免泄漏跨 Room/private scope 内容。

### 8.7 Security 与不受信输入

Pi/model arguments、GMS payload、artifact text、replay output 与外部 tool output 均是不受信数据。Host MUST 进行 schema、digest、scope、permission、size、timeout 校验，不得执行其中试图修改 authority、profile、tool routing 或 seal policy 的指令。

## 9. Slice obligations

本章采用统一模板，并细化 Contract §15 中 Host 参与的 S2、S3、S4、S7。除本章明确 Host-owned 的写入外，其他模块权威不转移给 Host。

### 9.1 S2 — Host Tool Proxy exact result return

#### Inputs

- Pi 发起的 `memory_explore|memory_expand|skill_get` tool call；
- Host Room/Agent/Delivery authoritative state；
- Host exact scope/budget/timeout profile；
- Contract §7.17 `ToolProxyRequest`；
- GMS typed upstream result。

#### Preconditions

- Agent membership 有效；
- tool 在 profile allowlist；
- Delivery 已分配且未 terminal；
- Pi return channel 尚未完成；
- GMS endpoint 可按 shared contract响应；
- shared conformance fixture 已冻结 ToolProxyResult canonical/digest procedure。

#### Authoritative writes

Host MUST 写入：

- tool call DAG event；
- ToolProxyRequest audit 与 idempotency binding；
- upstream attempt/status audit；
- exact ToolProxyResult audit；
- Pi return/delivery terminal event；
- tool-call-to-result causal link。

#### Derived writes

- Tool latency/availability metrics；
- proxy dashboard/index；
- transcript/tool result view。

Derived writes 不得替代 authoritative result。

#### Outputs

- Pi 通过原 tool call 收到 exactly one Contract §7.18 `ToolProxyResult`；
- success result type 与 tool name 匹配；
- failure/inconclusive 也通过同一 return path 可见。

#### Failure modes

- invalid request/scope/tool；
- GMS unavailable/timeout；
- upstream schema/digest/budget/watermark violation；
- idempotency conflict；
- Pi return channel failure；
- late upstream response。

所有失败按 §5.8-5.9、§8 处理。

#### Idempotency/CAS

- Request idempotency 按 §5.7；
- Delivery `queued→in_flight→terminal` 使用 CAS；
- 同一 request 最多一个 Pi-visible terminal result；
- late result 不得 CAS 覆盖终态。

#### Observability

必须能以 `proxy_request_id`/`delivery_id` 追踪 tool call、upstream attempt、exact result digest 和 Pi delivery；必须区分 Pi-visible result 与 late/side-channel data。

#### Acceptance fixture

最小 fixture MUST 证明：

1. Pi 发起 tool call；
2. Host 在 tool completion 前调用 fake GMS；
3. fake GMS 返回固定 typed result；
4. Host 返回 exact ToolProxyResult；
5. Pi 观察到的 canonical payload/digest 与 Host audit 相同；
6. 不存在 `tool_execution_end` 后旁路替代；
7. retry 返回相同 exact result；
8. timeout late response 不进入 Pi。

#### Non-goals

- Skill ranking；
- Graph projection；
- artifact activation；
- RSIH materialization；
- 自由 JSON `memory_expand`。

### 9.2 S3 — Sealed Segment fixture

#### Inputs

- recorded Room interaction；
- Agent/message/Delivery/tool events；
- Room sequence 与 Interaction DAG；
- Host close policy/profile；
- protected path extraction policy。

#### Preconditions

- Segment=`open`；
- expected Segment/Room frontier CAS 匹配；
- 所有纳入 Delivery/tool calls terminal；
- event payloads/digests 可解析；
- scope/permission audit完整。

#### Authoritative writes

Host MUST 原子写入：

- Segment=`settled`；
- frozen event frontier/membership；
- immutable Evidence Seal；
- immutable Conversation Path Seal；
- Contract §7.5 `SegmentRef`；
- terminal Decision Checkpoint；
- Contract §7.6 `CheckpointRef`；
- close audit event/DAG links/idempotency result。

#### Derived writes

- Segment event-list view；
- DAG path index；
- fixture discovery index；
- transcript projection。

#### Outputs

- 可 exact 解析的 SegmentRef 与 CheckpointRef；
- success/failure/recovery path refs；
- immutable canonical seal bytes/digests；
- GMS 可消费的 sealed source bundle。

#### Failure modes

- DAG cycle/missing ref；
- non-terminal Delivery/tool；
- digest/canonicalization failure；
- path extraction incomplete；
- close CAS/idempotency conflict；
- Room cancel。

确定性完整性失败→`failed`；合法取消→`aborted`；不得部分 settle。

#### Idempotency/CAS

- Segment close 以 expected `open` state/frontier CAS；
- 相同 close key/input 返回同 refs；
- 不同 input 同 key 冲突；
- settled 后所有 append 失败。

#### Observability

必须能从 Segment terminal audit 追踪 frozen frontier、seal refs、checkpoint、close policy、path counts、omissions 和 failure reason。

#### Acceptance fixture

Recorded fixture MUST 至少包含：

- 一个 success path；
- 一个 failure path；
- 一个 recovery path；
- 一次 ToolProxyResult；
- 明确 causal links；
- settle 后追加被拒；
- 重新读取 canonical seals 得到同 digest；
- failed/aborted Segment 不产生 SegmentRef/CheckpointRef。

#### Non-goals

- GMS evidence commit；
- Skill proposal/candidate；
- replay scoring；
- 对历史 transcript 动态重建 seal。

### 9.3 S4 — Validate and paired replay scheduling

#### Inputs

- GMS Contract §7.10 `ReplayRequest`；
- settled SegmentRef 与 seals；
- baseline/candidate exact refs；
- success/failure/recovery fixture refs；
- deterministic fake runtime adapter/profile；
- RSIH HarnessRunner capacity。

#### Preconditions

- ReplayRequest schema/digest/idempotency有效；
- Segment settled 且 seals 可验证；
- exact refs 无 `latest`/Graph ID；
- Host permissions允许；
- frozen plan可构造；
- merge 时 A/B/overlap/conflict families 与 source-head expectations存在。

#### Authoritative writes

Host MUST 写入：

- scheduling acceptance/rejection；
- frozen replay plan；
- baseline/candidate dispatch attempts；
- cancellation/retry correlation；
- captured exact execution output refs/digests。

GMS 而非 Host 写权威 ReplayResult/evaluation ledger。

#### Derived writes

- replay queue projection；
- worker utilization/latency metrics；
- attempt status dashboard。

#### Outputs

- correlated baseline/candidate execution outputs；
- 完整 fixture coverage；
- infrastructure/semantic outcome distinction；
- 可供 GMS canonicalize Contract §7.11 ReplayResult 的记录。

#### Failure modes

- invalid/unsettled Segment；
- seal/digest mismatch；
- adapter/profile unavailable；
- permission denied；
- worker infrastructure failure；
- semantic execution failure；
- timeout/cancel；
- missing fixture family。

Host 不得将任何失败转换为 accepted decision。

#### Idempotency/CAS

- 相同 ReplayRequest/idempotency key 使用同 frozen plan；
- execution lease CAS 防重复；
- infrastructure retry 创建新 attempt 但不改输入；
- semantic failure 不自动 retry；
- output completion 只能一次 terminal CAS。

#### Observability

必须能按 ReplayRequest 跟踪 fixture→baseline/candidate run→attempt→output digest；明确标记 retry 原因、adapter version、permissions 与 sealed source refs。

#### Acceptance fixture

Fake runtime fixture MUST 证明：

1. baseline/candidate 接收相同 frozen inputs；
2. success/failure/recovery 全执行；
3. merge fixture 执行 A-only、B-only、overlap 与 conflict families；
4. rerun 得到相同 output digests；
5. transcript 后续变化不影响 replay；
6. infrastructure retry 不改变 plan；
7. Host 不计算 release winner。

#### Non-goals

- Candidate static validation的权威实现；
- U1 comparator；
- ReleaseDecision；
- activation/projector；
- production live traffic evaluation。

### 9.4 S7 — Memory Explore retrieval

#### Inputs

- Pi Memory Explore tool call；
- Room/Agent/Delivery state；
- exact Host scope/budget/freshness profile；
- ExploreSession/fence context；
- GMS Contract §7.16 `ExploreResult` 与 §7.14 watermark。

#### Preconditions

- S2 已通过且 feature enabled；
- Agent membership/scope有效；
- Memory Agent 仅 Room shared Space；
- ordinary Agent profile允许；
- budgets与 timeout已 clamp；
- requested sequence合法。

#### Authoritative writes

Host MUST 写入：

- exact ToolProxyRequest/Result audits；
- ExploreSession-to-Room/Agent/profile binding；
- Tool result Delivery 与 DAG links；
- Host-side fence correlation（不重定义 GMS fence）。

#### Derived writes

- retrieval latency/usage metrics；
- budget consumption dashboard；
- watermark lag observation；
- Agent transcript view。

#### Outputs

Pi MUST 收到 exact ToolProxyResult，其中 success result 保持：

- typed Evidence/Skill arrays；
- separate fences；
- exact Skill refs；
- Guidance Views；
- separate identity/evidence citations；
- budgets/truncation；
- projection watermark。

【修订 v1.1，CTR-003】上述 truncation 输出的 top-level 形态为 Contract §12.7.2 `omissions` carrier（本 slice fixture 增补 `tools/explore/{top-level-omission,no-omission,negative}`）。

#### Failure modes

- scope/profile mismatch；
- cross-Room request/result；
- budget violation；
- missing/behind watermark；
- invalid citations/view hash；
- unsupported expansion schema；
- GMS timeout/unavailable；
- Pi return failure。

Host 必须返回失败 result，不得本地过滤/重排后伪造成功。

#### Idempotency/CAS

- 遵循 S2 proxy idempotency；
- retry 不重复消费 served fence；
- session binding 使用 expected profile/session CAS；
- profile 变化后的请求必须使用新 identity；
- late result 不更新 fence 或 Pi observation。

#### Observability

必须能按 ExploreSession/query digest 追踪 scope profile、effective budgets、watermark、result counts、truncation、view hashes、served fences 与 Pi delivery digest，且不得泄漏未授权内容。

#### Acceptance fixture

最小 fixture MUST 证明：

1. Memory Agent 只能检索当前 Room shared data；
2. ordinary Agent 受两个不同 profile 得到不同授权结果；
3. Evidence/Skill result/fence 分离；
4. identity/evidence citations 分离；
5. total/subcaps/Guidance budget 均满足；
6. result order未被 Host 修改；
7. watermark随结果返回；
8. `min_activation_sequence` 未满足时 fail closed；
9. exact ToolProxyResult 到达 Pi；
10. retry 不重复消费 fence。

#### Non-goals

- GMS ranking实现；
- Dynamic Graph construction；
- candidate/curation retrieval；
- S-slot binding/materialization；
- 跨 Room Memory Agent；
- Host 本地生成 Guidance View。
