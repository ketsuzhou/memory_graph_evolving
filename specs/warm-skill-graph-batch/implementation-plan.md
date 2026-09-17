# Warm Skill Graph Batch — Parallel Ticket Plan

```yaml
document_status: approved-ticket-plan
schema_version: warm-skill-graph-batch.ticket-plan.v2
contract: system-contract.md
strategy: warm-skill-graph-batch
legacy_strategy: warm-skill-batch
baseline: local-main@1cc8db4
approval_gate: approved-by-user
```

## 1. 拆分原则

1. 除两个必要 prefactor 外，每个 ticket 都是可独立演示的纵向 tracer bullet，而不是 schema/API/store/test 横向分层。
2. 每个 ticket 适合一个 fresh agent context 完成，并自带机器可验证验收标准。
3. 新策略与 legacy 策略隔离；不得改变 `warm-skill-batch` 的历史语义。
4. GMS 已有 immutable evolution ledger、proposal/candidate lifecycle、active Runtime projection、Usage Projection 和 Memory read plane；新任务只补 batch-specific records 与 non-active Evaluation read view。
5. Host 已有 Room/Delivery/Segment/outbox/tool-proxy 原语；新任务只补 structured recipient、exact-session controller 与 Skill interaction protocol。
6. 各 ticket 使用独立 deep module/adapter；共享 composition root 只由最终集成 ticket 修改，以保持最大并行度。
7. Canonical record、projection、Host delivery 和 report 中的未知 core 字段、版本错配与 authority 缺失均 fail closed。

## 2. Blocking graph

```text
PF-01 Evaluation Skill Graph prefactor ─┬─ TB-07 bounded explore ───────────────┬─ TB-12 reject redirect ─┐
                                        │                                      └─ TB-13 adaptation ──────┤
TB-02 canonical raw proposal ─┬─ TB-03 diagnosis fan-out/verdict ────────────────────────────────────────┤
                              └─ TB-04 CAS consolidation ──┬─ TB-05 unified freeze ── TB-10 served ─┬─ TB-11 accept/adopt ─┤
                                                          │                                         ├─ TB-12 reject ──────┤
PF-02 controllable Pi session ─┬─ TB-06 concurrent empty episode ── TB-08 directed interrupt ─ TB-09 safe-effects ───────┤
                               └─────────────────────────────┘                                         │                  │
TB-01 strategy skeleton ───────── TB-06 ────────────────────────────────────────────────────────────────┘                  │
                                                                                                                           ▼
                                                                                                                   TB-14 full pipeline
                                                                                                                           │
                                                                                                                   TB-15 reporting
                                                                                                                           │
                                                                                                                   TB-16 smoke
```

## 3. Prefactor tickets

### PF-01 — 增加 non-active Evaluation Skill Graph read view

**Blocked by:** None（可立即开始）。

**What it delivers:** 一个未激活 advisory proposal 经 fixture consolidation 后，可以在 evaluation scope 中按 exact revision 多跳探索和读取；它不会进入 active Runtime graph、activation ledger 或 executable closure。该 view 只投影 canonical records，不成为第二权威源。

**Acceptance criteria:**

- [ ] 相同 canonical input 重建得到相同 projection digest、节点、边和顺序。
- [ ] Non-active proposal 在 evaluation scope 可 explore/get，在 Runtime active scope 仍被拒绝。
- [ ] Projection pin、watermark 和 digest 不匹配时 fail closed。
- [ ] Held-out feedback record 作为 projection input 时被拒绝。
- [ ] 删除 projection 后可从 canonical fixture 重建为 byte-equivalent view。

### PF-02 — 把 one-shot Pi process 扩展为可控 exact-session controller

**Blocked by:** None（可立即开始）。

**What it delivers:** Host 可以启动并识别 exact Pi session，保持 RPC 全双工控制，查询 state、abort、等待 settled，并在不改变 legacy one-shot turn 行为的情况下为后续 exact-session resume 提供 seam。

**Acceptance criteria:**

- [ ] Controller 暴露 exact session file/id、process generation 和 settled state。
- [ ] Agent running 时可发送 `get_state/clear_queue/abort`，stdout 只有一个 reader。
- [ ] Launcher 能使用 exact `--session`，禁止用交互 `--resume` 或模糊 `--continue`。
- [ ] Legacy launcher flags、one-shot runtime 行为与现有测试保持不变。
- [ ] Race test 证明 RPC writer、response correlation 和 event stream 无并发读写竞争。

## 4. Vertical tracer-bullet tickets

### TB-01 — 新策略以空 Skill Graph 完成一个正常 no-skill episode

**Blocked by:** None（可立即开始）。

**What it delivers:** CLI 可显式选择 `warm-skill-graph-batch`，冻结 contract 默认配置；generation 0 在空 evaluation graph 上得到 `no_candidate_from_empty_graph` 并完成任务，legacy dispatch 不变。

**Acceptance criteria:**

- [ ] 新旧 strategy dispatch table 均有测试，旧 strategy 输出不变。
- [ ] 非法 graph/offer/turn budget 和未知策略 fail closed。
- [ ] Attempt artifact包含 strategy、empty initial snapshot、frozen config 和正常 no-skill terminal。
- [ ] Empty graph 不被统计为 timeout、memory error 或 protocol error。

### TB-02 — 一条 frozen trajectory 端到端产出 canonical Raw Skill Proposal

**Blocked by:** None（可立即开始）。

**What it delivers:** Diagnosis 读取完整 trajectory、公开 outcome 和 exact checkpoint/evidence refs，通过受保护 admission 创建 immutable Raw Skill Proposal 与 provenance，并返回完整 ID/ref。

**Acceptance criteria:**

- [ ] Valid fixture 可按完整 ID 读回，proposal 与 source checkpoint/evidence 双向可追踪。
- [ ] Missing/cross-snapshot/unauthorized evidence 整单失败且不产生 partial append。
- [ ] Global trigger、纯通用建议、无 baseline delta、无法由证据推出的 insight 被拒绝。
- [ ] Hidden-test/gold 字段或未知 core 字段被拒绝。
- [ ] 同 idempotency key + 同 body 返回同 ref；不同 body 冲突。

### TB-03 — Diagnosis fan-out 保留 outcome 并覆盖每个 served exposure

**Blocked by:** TB-02。

**What it delivers:** 每条 train trajectory 独立并行 diagnosis；结果按预分配 sequence 归并；每个 served exposure 恰有一个 supported/refuted/inconclusive verdict，公开失败信息可见而 hidden/gold 不可见。

**Acceptance criteria:**

- [ ] N 条 trajectory 产生 N 个 terminal diagnosis jobs，并有并发 barrier 证明。
- [ ] Pass/fail、timeout/no-output/protocol-error、公开 compile/runtime error 进入输入。
- [ ] Binary-only failure 使用 `cause=unknown`，不得生成虚构根因字段。
- [ ] 每个 served exposure verdict exactly once；未 served offer 不产生 exposure verdict。
- [ ] 单 job 失败被记录且不产生伪 proposal；consolidation 不在所有 jobs terminal 前启动。

### TB-04 — 一次 CAS consolidation 完整覆盖 family proposals

**Blocked by:** TB-02。

**What it delivers:** Consolidation 用完整 proposal IDs 提交 retain/revise/specialize/merge/retire/insufficient-evidence decisions；expected ledger revision CAS 原子生效，每条 raw proposal 恰好被一个 decision 覆盖。

**Acceptance criteria:**

- [ ] Exact duplicate、guard specialization、compatible merge、conditional conflict 和 insufficient-evidence fixtures得到预期 decision。
- [ ] Prefix ID、unknown source、漏 source、重复覆盖全部失败且 ledger 不变。
- [ ] 两个并发 writer 使用同 expected revision 时恰好一个成功。
- [ ] Successor 保存所有 source proposal IDs 和 evidence lineage，不只保存首个 source。
- [ ] Model/transport 失败不得退化成无 provenance Markdown ledger。

### TB-05 — 统一 EvaluationFreezeManifest 在首个 test 前 fail closed

**Blocked by:** PF-01、TB-04。

**What it delivers:** 一个 immutable manifest 同时绑定 evidence cut、Skill ledger revision、proposal/provenance set、Evaluation Graph digest/watermark，以及 prompt/schema/model/tool/config/grading policy；所有 test 只接收一个 digest。

**Acceptance criteria:**

- [ ] 相同输入幂等产生同 manifest ID/digest。
- [ ] 任一 moved head、missing proposal、provenance hole 或 graph digest mismatch 在 test 前失败。
- [ ] Test Room、overlay 或 held-out feedback 混入 scope 时失败。
- [ ] N 个 test attempt 记录完全相同 manifest digest。
- [ ] Partial graph freeze 不得 warning 后继续运行。

### TB-06 — Task 与 opening-only Memory Agent 在空图上真实并发

**Blocked by:** PF-02、TB-01。

**What it delivers:** 新 episode 同时启动 task session 与 Memory Agent；memory 只见 opening context，并在空图上正常结束；task 不等待 memory 且不被无消息中断。

**Acceptance criteria:**

- [ ] Start-barrier 证明 task 与 memory 都在另一方 settled 前进入 running。
- [ ] Memory input 不含 task 中间 tool state 或后续消息。
- [ ] 同一 Agent 无重叠 Pi run，不同 Agent 可并行。
- [ ] Empty/no-applicable 与 timeout/error terminal 可区分。
- [ ] Memory 无 offer 时 task session 不被 abort/resume。

### TB-07 — Bounded Evaluation Graph explore 选择 original Skill offer

**Blocked by:** PF-01。

**What it delivers:** Memory Agent 从 opening seeds 开始，在固定 graph/turn/offer 预算内沿允许关系多跳探索，显式选择 `serve_original` 并生成包含 exact Skill Reference、checkpoint 和 target 的 pending offer。

**Acceptance criteria:**

- [ ] 固定 fixture 的 seed、neighbor order、selected ref 和 audit trace 可复现。
- [ ] Disallowed Room/evidence expansion、latest alias、本地 path、跨 scope ref 被拒绝。
- [ ] 12 graph steps、6 memory turns、3 offers 边界分别产生正确 terminal state。
- [ ] Conditional conflict、retired/superseded 和 train feedback edge按 policy影响候选。
- [ ] Held-out feedback变化不影响 seed、ranking 或 explore result。

### TB-08 — Structured offer 原子投递并中断/续跑 exact task session

**Blocked by:** PF-02、TB-06。

**What it delivers:** Skill Offer 用结构化 target 原子写 Room message + unique Delivery，安全停止 running task，并用同一 exact session 恢复；中断期间到达的 messages 一次有序注入，创建 continuation Segment。

**Acceptance criteria:**

- [ ] 自由文本 `@agent` 不路由；structured target 才创建 Delivery。
- [ ] 同 key/body 重放返回原 message/delivery；同 key不同 body 冲突。
- [ ] 两个并发 mentions 只触发一次 abort，按 Room sequence 一次 resume 注入。
- [ ] Resume 后 session ID/file 与中断前一致，已完成 tool results 不重放。
- [ ] 新 Segment 正确记录 `continuation_of` 和 directed-mention reason。

### TB-09 — Mutating tool 安全中断与 effects-unknown retry

**Blocked by:** TB-08。

**What it delivers:** Coordinator 区分 model/read-only/mutating execution；mutating tool 完成后再 abort，超时按 process-group TERM→KILL；无法证明 side effect 时 quarantine attempt，并由 runner 重试而非盲目 resume。

**Acceptance criteria:**

- [ ] Read-only/model generation 可立即 cooperative abort。
- [ ] Mutating tool 的 completion 在 abort 前被观察，已完成操作不重复执行。
- [ ] Uncooperative child 经过 grace、TERM、KILL 后整个 process tree 被回收。
- [ ] Effects-unknown attempt 不 resume，产生新 attempt ID 且保留原失败记录。
- [ ] Retry exhaustion 保留 preregistered task 并在主分母记 0。

### TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**Blocked by:** TB-05、TB-08。

**What it delivers:** Resumed task 在普通工具前必须解析 manifest-pinned exact Skill Reference；只有 exact body bytes 成功进入目标 Pi session 才记录 Host-authored served fact。

**Acceptance criteria:**

- [ ] Latest/local path、cross-manifest revision、权限或 digest mismatch 均失败。
- [ ] Resolution failure 不创建 served/rejected，并结构化通知 Memory Agent。
- [ ] 成功时 Pi captured bytes digest = GMS view digest = Host served digest。
- [ ] `skill_get` 前普通工具被 fence；成功后必须进入 feedback fence。
- [ ] 同 tool call 重放不重复创建 served record。

### TB-11 — Accepted disposition 解除围栏并可选记录 adoption

**Blocked by:** TB-10。

**What it delivers:** `skill_feedback(accepted)` 原子创建唯一 disposition、interaction signal、群聊可见 accepted 消息和 Memory Delivery，并解除 task 工具围栏；可选 adoption 独立记录实际行为改变。

**Acceptance criteria:**

- [ ] 未 served 的 offer 不能 accepted。
- [ ] 同 offer/agent 只有一个 terminal disposition；相同重放幂等、冲突重放拒绝。
- [ ] Signal、Room message、Memory Delivery 三项 all-or-none。
- [ ] Accepted 不自动产生 adopted、verified、active 或 marginal-gain-supported。
- [ ] 无效 feedback 仅 repair 一次；第二次记录 protocol-error 并解除围栏。

### TB-12 — Rejected reason 驱动下一次非重复 exploration

**Blocked by:** TB-07、TB-10。

**What it delivers:** Rejected disposition 产生结构化 reason 和 Memory Delivery；Memory 按九类 core reason 更新 guard/exclusion/blacklist/stop policy，并留下 `offer → reason → next query` 审计链。

**Acceptance criteria:**

- [ ] 九个 reason codes 均有 table-driven redirect/stop 行为测试。
- [ ] Fixture 演示 generic Skill rejected 后探索 specialized descendant 并再次 offer。
- [ ] Same revision、excluded lineage 和相同 source set 不重复 offer。
- [ ] `insufficient_context/already_resolved` 在 opening-only 模式停止当前 need。
- [ ] Resolution error 不进入 rejection reason 统计；全部拒绝终止为 all-candidates-rejected。

### TB-13 — Context adaptation 只存在于 episode-local overlay

**Blocked by:** TB-07、TB-10。

**What it delivers:** Memory 可显式选择 original 或创建绑定 exact source、opening snapshot、delta 的 adaptation；test adaptation 可被同 attempt 的 `skill_get` 解析，但不会修改 canonical Skill 或 frozen graph。

**Acceptance criteria:**

- [ ] 无实质 delta 的 adaptation 被拒为 duplicate。
- [ ] Source revision body/digest 保持不变。
- [ ] Adaptation 只能在同 attempt、同 manifest、同 target scope解析。
- [ ] Episode close 后 adaptation 不可解析。
- [ ] Ledger/graph/freeze digest 在 overlay 生命周期前后不变，report可区分 original/adaptation。

### TB-14 — 新策略贯通 train→diagnosis→consolidation→freeze→held-out

**Blocked by:** TB-03、TB-04、TB-05、TB-06、TB-09、TB-11、TB-12、TB-13。

**What it delivers:** `warm-skill-graph-batch` 使用 canonical pipeline：parallel train barrier、parallel diagnosis、single consolidation、unified freeze、parallel held-out task+memory；test feedback仅写 trace，normal no-skill继续，mechanism failures分类保留。

**Acceptance criteria:**

- [ ] 端到端 fixture 证明阶段 barrier、single consolidation 和 held-out parallelism。
- [ ] 新策略不以 runner-local `[]skillProposal` 或 Markdown/hash prefix 为 authority。
- [ ] Freeze失败时 0 个 test started。
- [ ] Test A feedback 不改变 Test B retrieval；所有 test 同 manifest digest。
- [ ] Legacy `warm-skill-batch` dispatch、artifact schema 和 golden行为不变。

### TB-15 — 从 preregistered manifest 生成 full-denominator paired report

**Blocked by:** TB-14。

**What it delivers:** Reporter 以 preregistered test manifest 为分母和 grader outcome 为通过权威，输出 warm/cold pass@1、paired wins/losses、机制 funnel、reason、served digest、adaptation、verdict、terminal taxonomy及已知实验限制。

**Acceptance criteria:**

- [ ] Missing、timeout、no-output、infra failure 均保留分母并计 0。
- [ ] Duplicate/missing task、manifest/model/seed/grading/salvage policy mismatch 拒绝生成。
- [ ] 任意 recall citation 不能计 served；只有 exact body digest delivery 可计。
- [ ] Paired wins+losses+ties 等于完整 paired manifest。
- [ ] Golden JSON 排序稳定并明确披露 protocol token/time 未匹配。

### TB-16 — Deterministic smoke 证明 system-contract 十二项不变量

**Blocked by:** TB-09、TB-14、TB-15。

**What it delivers:** 小规模 fixture graph 和 scripted/real Pi 运行完整 reject→redirect→accept、effects-unknown retry、canonical diagnosis/consolidation、frozen test isolation，并生成自描述 artifact bundle。

**Acceptance criteria:**

- [x] 机器逐项断言 `system-contract.md §10` 的十二项 smoke 条件。
- [x] Real Pi 0.85.1 smoke 证明 exact-session abort/resume；binary不可用时不得把 contract 标成 green。
- [x] Bundle记录命令、版本、config、manifest、body digests和每项 assertion evidence。
- [x] Host/GMS targeted tests、repository-wide tests及 race suites通过。
- [x] Known limitations 固化：generation-0无train feedback、opening-only、无protocol-cost control、crash uncertainty fail closed。

## 5. 最大并行前沿

### Frontier A — 立即可开 4 路

- PF-01 Evaluation Skill Graph
- PF-02 Pi exact-session controller
- TB-01 strategy empty-graph skeleton
- TB-02 canonical Raw Skill Proposal

排他约束：两个 GMS worker 不修改 server composition root；两个 Host/runner worker不修改共享 runtime composition root。

### Frontier B — 第一批完成后最多 4 路

- TB-03 diagnosis fan-out（TB-02 后）
- TB-04 CAS consolidation（TB-02 后）
- TB-06 concurrent empty episode（PF-02 + TB-01 后）
- TB-07 bounded explore（PF-01 后）

### Frontier C — Delivery 主链与 Freeze 可并行

- TB-05 unified freeze（PF-01 + TB-04 后）
- TB-08 directed interrupt（PF-02 + TB-06 后）

TB-05 与 TB-08修改不同 authority domains，可并行。

### Frontier D — 最多 4 路

- TB-09 effects-safe interruption（TB-08 后）
- TB-10 served digest（TB-05 + TB-08 后）
- TB-12 rejected redirect（TB-07 + TB-10 后）
- TB-13 adaptation overlay（TB-07 + TB-10 后）

严格依赖下，TB-12/TB-13 要等 TB-10；可先由 TB-09 与 TB-10 两路启动，随后扩到 TB-11/TB-12/TB-13 三路。

### Integration tail

- TB-14 是唯一 composition 汇合点。
- TB-15 报告依赖完整 pipeline。
- TB-16 只做 public-seam smoke/conformance，不补前序 ticket 遗漏的单元测试。

## 6. 旧 SG 计划的替换关系

| 旧横向任务 | 新纵向替代 |
|---|---|
| SG-00 schema/state freeze | DTO 与状态随 TB-02/04/05/08/10/11/12/13 各自落地 |
| SG-10 entire ledger | 复用现有 ledger；TB-02 + TB-04 补 batch records |
| SG-11 Runtime graph | PF-01 + TB-07，避免污染 active Runtime graph |
| SG-20 directed delivery | TB-08 |
| SG-21 AgentRunCoordinator | PF-02 + TB-08 + TB-09 |
| SG-22 all Skill tools | TB-10 + TB-11 + TB-12 + TB-13 |
| SG-30 diagnosis+consolidation | TB-02 + TB-03 + TB-04 |
| SG-31 entire online loop | TB-06 + TB-07 + TB-08 + TB-11 + TB-12 |
| SG-32 freeze | TB-05 |
| SG-40 strategy | TB-01 + TB-14 |
| SG-41 reporting | TB-15 |
| SG-50 smoke | 每票自测 + TB-16 public-seam smoke |
| SG-60 review | TB-16 后的 release gate，不作为实现 ticket |

## 7. 发布约束

本文件已由用户确认，任务粒度与 blocking edges 进入 approved 状态。发布为一票一文件仍需先配置 issue tracker；若采用本地 tracker，则输出到 feature-scoped `.scratch/.../issues/`，每票单独记录 blockers 与 acceptance criteria。
