# Warm Skill Graph Batch — System Contract

```yaml
contract_id: warm-skill-graph-batch.system-contract
version: 0.1.0
status: documentation-only
language: zh-CN
decision_source: ../../../handoff-gms-snapshot-bench.md
strategy_id: warm-skill-graph-batch
legacy_strategy_id: warm-skill-batch
```

> 本契约冻结 `pi-group-chat-host`（Host）、Graph Memory Service（GMS）和
> bench-runner 之间的新 batch-offline Skill 评测协议。实现与自动化验收完成前，
> 状态保持 `documentation-only`。MUST、MUST NOT、SHOULD、MAY 按 RFC 2119 解释。

## 1. 目标与非目标

### 1.1 目标

系统 MUST 支持以下闭环：

```text
train task trajectories and outcomes
→ per-trajectory parallel diagnosis
→ provenance-complete raw Skill proposals
→ single batch consolidation
→ frozen Skill/evidence/projection manifest
→ held-out task and Memory Explore Agent start concurrently
→ bounded multi-hop Skill Graph exploration
→ optional context adaptation
→ directed Skill Reference offer
→ safe task-session interrupt and exact-session continuation
→ skill_get / served / accepted-or-rejected
→ rejection-directed continued exploration
→ held-out task outcome and trace-only feedback
```

主实验衡量 `warm-skill-graph-batch` 相对 cold 的全量 held-out pass@1 差值。

### 1.2 当前 arm 的明确边界

1. 当前 arm 是 `batch-offline`，报告 MAY 显示为 Arm A。
2. 输入 Skill graph 为空；因此 generation 0 train 没有 inherited Skill feedback。
3. 本代新 Skill 在 train diagnosis/consolidation 后生成，首次在线反馈来自 test。
4. Test feedback 仅保存到 held-out trace，MUST NOT 写回任何 trainable graph。
5. Memory Explore Agent 只读取 task opening context；不读取 task agent 的中间工具状态。
6. Task 与 memory 并行运行；Skill offer 可中断 task session，但 memory 不持续重判 task context。
7. 本 arm 不运行 replay，不得把 proposal 标记 validated、active 或 marginal-gain-supported。
8. 本实验暂不匹配 memory、interrupt、Skill 工具带来的额外 token/时间成本；报告 MUST 披露此限制。
9. `batch-replay-feedback` 与 `online-continual` 是未来独立策略，不属于本契约。
10. 大规模 182/86 正式评测不是代码验收；先通过小规模 smoke。

## 2. Authority 与数据模型

### 2.1 Authority

- Host MUST 唯一拥有 Room、Agent、directed Delivery、message order、Agent session binding、Segment、Decision Checkpoint、interrupt/resume 和 served 事实。
- GMS MUST 唯一拥有 committed evidence、immutable Skill Evolution Ledger、Skill revision identity、provenance、consolidation decisions、Runtime Skill Graph Projection 和 frozen manifest。
- Agent MAY 检索、adapt、propose、accept/reject 或声明 adopted；Agent MUST NOT 自行创建可信 provenance、验证 Skill、修改 canonical ledger 或激活 Skill。
- Runtime Skill Graph 是可重建 projection，不是 Skill authority。
- runner `[]skillProposal` 仅是 frozen ledger snapshot 的 compatibility view；新策略 MUST NOT 将其作为独立写权威。

### 2.2 核心记录

#### RawSkillProposal

每条 proposal MUST 至少包含：

```text
proposal_id
schema_version
source_checkpoint_id
source_evidence_refs[]
context_trigger
failure_or_opportunity
baseline_behavior
non_obvious_insight
decision_policy_or_steps[]
expected_behavior_change
contraindications[]
pitfalls[]
outcome_observed
novelty_status = hypothesized
created_by_agent_run_id
created_at
content_digest
```

Admission MUST 拒绝：无 exact evidence/checkpoint、全局 trigger、纯通用建议、无法由证据推出的 insight、无 baseline delta、hidden-test/gold 泄漏或未知 core 字段。

#### ConsolidationDecision

```text
decision_id
expected_ledger_revision
operation = retain | revise | specialize | merge | retire | insufficient_evidence
source_proposal_ids[]
successor_skill_revision?
conditional_conflicts[]
rationale_evidence_refs[]
created_by_agent_run_id
content_digest
```

每个 raw proposal MUST 被恰好一个 decision 覆盖。Source 使用完整 ID，不得使用 hash prefix。

#### SkillReference

Skill Reference MUST 是 immutable URI，概念格式：

```text
skill://<namespace>/<lineage>@<revision>
```

解析 MUST pin 当前 evaluation manifest；禁止 latest alias、本地文件路径和跨 snapshot revision。

#### ContextualSkillAdaptation

Adaptation MUST 引用 exact source revision、Replayable Context Snapshot、显式 delta 与生成正文。它不得覆盖源 Skill。Memory agent MUST 在 `serve_original` 与 `create_adaptation` 间显式选择；无实质 delta 的 adaptation 拒绝为 duplicate。

- Train adaptation MAY 进入 train ledger，但 generation 0 空图时不会产生。
- Test adaptation 只存在于 episode-local overlay，episode 结束销毁。

#### Skill interaction records

系统 MUST 区分：

```text
matched → offered → served → accepted/rejected → adopted? → verified? → outcome-correlated?
```

任何较早阶段都不得暗示较晚阶段。

- `served`：Host 证明 exact body digest 已进入 target exact Pi session。
- `accepted`：Agent 阅读完整正文后认为对当前 checkpoint 有用；仅表示 selected relevance。
- `rejected`：Agent 阅读完整正文后认为当前 checkpoint 不适用或无用，并给出原因。
- `adopted`：Agent 可选声明 Skill 实际改变了其决策。
- `verified`：Diagnosis 对 exact exposure 给出 supported/refuted/inconclusive。

因反馈发生在完整正文之后，所有 served Skills（包括 rejected）都属于实际 treatment，不能从上下文撤回。

## 3. Provenance 与 Graph

### 3.1 Proposal-time provenance

Diagnosis agent 只能声明 source references。Host/GMS MUST 验证引用属于允许读取的 frozen trajectory，并原子创建 Raw Skill 与 provenance records。无效、越权、跨 snapshot 或歧义引用 MUST 使整个 proposal 失败。

Projection SHOULD 派生以下关系：

```text
Evidence --derived_from_step--> Raw Skill
Evidence --supports--> Raw Skill
Raw Skill --consolidated_into--> Consolidated Skill
Source Skill --adapted_for--> Contextual Adaptation
Skill --offered_at--> Agent + Checkpoint
Offer --accepted|rejected--> Feedback Signal
Exposure --supported|refuted|inconclusive--> Diagnosis Verdict
```

Canonical records 是边的权威来源；Graph edge 本身不得反向充当历史事实。

### 3.2 Bounded exploration

Memory Explore Agent 从 task opening 匹配到的 seed nodes 开始。每 episode 默认预算：

```text
max_graph_steps_per_episode = 12
max_offers_per_agent_per_checkpoint = 3
max_memory_turns_per_episode = 6
```

允许探索：`specializes/generalizes`、`related_to`、`derived_from_step/supports`、`adapted_from`、弱 `co_used_with`、`conditionally_conflicts_with`、aggregated train `accepted_in/rejected_in`、`supersedes/retired_by`。

禁止沿任意 Room/evidence 边无界扩散；禁止读取 held-out feedback。

### 3.3 Reason-driven redirect

Rejected feedback MUST 包含 `reason_code` 与自由文本 `reason`。Core codes：

```text
not_applicable
already_known
already_resolved
too_generic
insufficient_context
incorrect_assumption
conflicts_with_evidence
duplicate_offer
unsafe_or_harmful
```

Redirect policy：

- `not_applicable`：更新 guard，探索 specialization；
- `already_known`：排除相同 insight lineage；
- `already_resolved`：停止当前 need；
- `too_generic`：寻找 guarded/specialized descendant；
- `insufficient_context`：opening-only 模式下停止当前 need；
- `incorrect_assumption`：排除相同 assumption branches；
- `conflicts_with_evidence`：检查 conflict/support evidence 后找替代项；
- `duplicate_offer`：排除同 lineage/source set；
- `unsafe_or_harmful`：episode 内 blacklist exact revision。

每次 `offer → reason → next query` MUST 可审计。

## 4. Directed delivery 与 Agent session

### 4.1 Structured routing

Skill Offer MUST 携带权威 `recipient_agent_id`。正文中的 `@agent` 只是展示，不承担路由或权限。Host MUST 原子写 canonical Room message 与 unique directed Delivery。

Memory 与 task Agent MAY 并行运行；同一 Agent 的完整 Pi turn MUST single-flight 至 `agent_settled`。

### 4.2 AgentSessionBinding

Binding key MUST 包含：

```text
evaluation_id
logical_run_id
attempt_id
room_id
agent_id
```

Binding MUST pin exact session file/id、session directory、working directory、profile/provider/model/tool-policy digests 和 run generation。不同 episode、attempt、arm MUST 隔离。

### 4.3 Directed mention interruption

收到 structured directed mention 时，Host 通用 `AgentRunCoordinator` MUST：

1. 幂等 enqueue message/Delivery，并推进 interrupt generation；
2. 对同 Agent 只触发一次中断；后续 messages 按 Room sequence 排队；
3. 对模型生成/只读工具请求 RPC abort；
4. 对可能产生副作用的工具先等待 completion，再 abort；
5. 超过 grace 才 process-group SIGTERM，最终 SIGKILL；
6. 无法确认副作用时标记 `effects_unknown`，终止 attempt 并 fail closed；
7. 保留已完成 messages/tool results/abort results/workspace 状态；
8. 使用 exact `--session <stored-session-file>` 恢复；禁止交互 `--resume`、模糊 `--continue`；
9. 一次 resume 有序注入所有 pending directed messages；
10. 新 Segment 记录 `continuation_of` 与 `DIRECTED_MENTION`；
11. 不重放已完成工具或重复注入 message。

首版只要求单进程 benchmark durability。进程崩溃后若无法证明状态一致，MUST fail closed，不自动猜测恢复。

### 4.4 Offer protocol fence

Task agent 在 mention-resume 后 MUST 先：

```text
skill_get(exact Skill Reference)
skill_feedback(accepted | rejected, ...)
```

完成 disposition 后才能恢复普通工具。

- `skill_get` 成功时 Host 记录 exact digest served。
- Resolution error 通知 memory，不计 rejected。
- 无效 feedback 允许一次 protocol repair；仍失败记录 protocol_error 后解除围栏。
- 同一 offer/agent 只允许一个幂等最终 disposition。

`skill_feedback` MUST 原子创建 interaction signal、群聊可见 `@memory-agent SKILL_ACCEPTED|SKILL_REJECTED` 消息和 memory directed Delivery。

可选 `skill_adoption` 绑定 exact Skill、offer、checkpoint、behavior change 和 affected action；未调用不得推断 rejected。

## 5. Batch pipeline

### 5.1 Train

Generation 0 输入空 Skill graph。Train tasks MAY 并行；memory path会正常结束为 `no_candidate_from_empty_graph`。每条 train trajectory MUST 保存结构化 outcome：pass/fail、timeout/no-output/protocol-error，以及可公开的 compile/runtime error。禁止 hidden test inputs 与 gold/reference solution；无可观察原因时标 `cause=unknown`。

### 5.2 Diagnosis

每条 trajectory 启动一个独立 diagnosis job，可并行。Diagnosis MUST 读取完整 frozen trajectory、结构化 outcome 和该 trajectory 的 Skill interaction signals（generation 0 通常为空），并使用 `skill_propose` 创建 canonical records。

Diagnosis 对每个 served exposure输出 supported/refuted/inconclusive；accepted/rejected 不能替代正确性判断。

### 5.3 Consolidation

所有 diagnosis terminal 后运行一次 consolidation。使用 `skill_consolidate` 与 expected ledger revision CAS。决策顺序：

1. exact duplicate → deduplicate；
2. applicability overlap 且 policy 兼容 → merge；
3. insight 相同但 guards 不同 → guarded branches/specializations；
4. policy 冲突 → conditional conflict；
5. 纯通识无新增信息 → retire/reject；
6. provenance/feedback 不足 → insufficient_evidence。

单轨迹 Skill 只能 `novelty=hypothesized`。Baseline 已正确执行的建议不得由该轨迹重复 proposal。只有 replay arm 能升级为 `marginal_gain_supported`。

### 5.4 Freeze 与 Test

GMS MUST 生成一个 manifest，绑定：

- evidence batches/watermarks；
- Skill Evolution Ledger revision/digest；
- complete proposal IDs and provenance；
- Runtime Skill Graph projection version/digest；
- prompt/schema/model/tool/config/grading policy revisions。

任一 freeze 失败、digest 不一致、provenance 不完整或 test scope污染 MUST 使 arm 在 test 前 fail closed。

所有 test episodes MUST 使用同一 manifest digest。每个 test episode 只有独立 local overlay。Test feedback 保存到 Held-Out Skill Feedback Trace，MUST NOT 写入 graph、ledger 或后续 consolidation。

## 6. 状态与失败分类

Memory terminal states：

```text
accepted
all_candidates_rejected
no_candidate_from_empty_graph
no_applicable_candidate
graph_budget_exhausted
offer_budget_exhausted
memory_timeout
memory_error
task_finished_before_delivery
resolution_error
protocol_error
```

只有 empty/no-applicable 是正常 no-skill。其他状态不得掩盖为检索无结果。

- Memory timeout/error：task MAY 继续，但 trace标 mechanism failure。
- `effects_unknown`：当前 attempt终止；按既有 retry budget重试。
- Retry耗尽：预注册 task 在主 pass@1 中记 0。
- No applicable Skill：正常继续任务。

## 7. 配置

Frozen run config MUST 至少包含：

```text
strategy = warm-skill-graph-batch
initial_skill_snapshot = empty
skill_feedback_persistence = train_only
max_graph_steps_per_episode = 12
max_offers_per_agent_per_checkpoint = 3
max_memory_turns_per_episode = 6
mention_interrupt_grace
mention_kill_grace
skill_adaptation = optional
task_context_monitoring = opening_only
test_feedback_sink = trace_only
```

## 8. 指标与评测口径

主指标：

```text
pass@1 = passed / all preregistered held-out tasks
```

Timeout、failed、no-output、infrastructure failure 均保留在分母。必须同时报告相同 manifest/model/seed/budget/grading policy 下的 paired warm-vs-cold wins/losses。Salvage policy 必须在所有 arms 统一且预先固定。

机制指标 MUST 包含：graph seeds/steps/redirects、offer count、exact-digest served rate、accepted/rejected/no-response、reason distribution、adoption、per-exposure verdict、normal no-skill vs budget/infra failures、original vs adaptation、以及各 exposure disposition 对应 outcome（仅描述性）。

内容级验收不变量：

1. served 证明 exact body digest 实际进入目标 Pi session；
2. held-out feedback 不出现在任何 test retrieval scope；
3. 所有 test episode 使用相同 manifest digest。

## 9. Compatibility

- 旧 `warm-skill-batch` MUST 保留，标记 legacy/broken-delivery，用于复现 batch-6。
- 新语义 MUST 使用 `warm-skill-graph-batch`，不得复用旧名称。
- 新策略直接使用 GMS canonical ledger；legacy 策略 MAY 继续使用 runner-local ledger。
- 新策略不得把旧 Markdown/hash-prefix ledger 视为完整 provenance；导入必须显式标 `legacy_unverified`。

## 10. Smoke acceptance

小规模 smoke MUST 证明：

1. task 与 memory Agent 确实并行；
2. structured mention 会安全停止并恢复 exact task session；
3. resolved Skill body digest 与 served record 一致；
4. task agent 产生 accepted/rejected 与 reason；
5. rejected 会根据 reason 触发下一次 graph exploration；
6. same Agent 无重叠 Pi run；
7. effects-unknown fail closed；
8. diagnosis proposal保留 exact evidence lineage；
9. consolidation完整覆盖 source IDs；
10. held-out feedback不进入 frozen graph；
11.所有 test使用同一 manifest；
12.主指标使用全量预注册分母。
