```yaml
document_status: implementation_plan
schema_version: benchmark-diagnosis-replay-export.implementation-plan.v1
language: zh-CN
decision_baseline: Q27-Q112
contract_gate: PG-00D
repositories:
  host: ../../pi-group-chat-host
  gms: ../../graph-memory-service
  areal: ../../../areal
  multica: ../../../multica
```

# Benchmark Diagnosis、Production Cut、Replay 与多 Agent 轨迹实施计划

## 1. 目标、裁决基线与非目标

本计划把已确认的 Q27–Q112 转成可执行工作图。目标是同时交付：已知 `EvaluationBatch` 的全 Agent completion barrier；长生命周期生产 Room 的外部 `ConsolidationCut`；冻结输入上的 Diagnosis、Diagnosis Agent-owned Proposal、隔离 Replay、CAS Activation/Promotion；共享 Interaction DAG 加逐 Agent reconstructed trajectory 的可靠导出与 Training View；权限、撤销、审计及跨服务可靠性。

三项不得被实现“修正”的用户裁决：

1. **Q53=A**：Diagnosis 可读取当前 Cut manifest 绑定的全部 private Spaces；权限仍限 tenant/Room/Cut，使用短期只读 capability，并执行分区输出、disclosure gate 和审计。
2. **Q60=B**：durable training payload 是从 canonical structured transcript 重新 tokenized 的数据，不保存原始 rollout tensors；必须标记 `trajectory_fidelity=reconstructed`，仅允许 SFT、偏好学习或显式支持重建数据的离线训练，禁止伪造 behavior logprobs、weight versions 或 on-policy fidelity。
3. **Q90=C**：Proposal 默认归属稳定 Diagnosis Agent identity；ownership、visibility、evidence permissions、approval 与 activation target 是独立字段。

非目标：不把 benchmark Room closure 套到 production Room；不把 legacy Multica consolidation/export 当作 greenfield 权威；不重写 `event_codec.py`、`SuperNodeAssembler` 或 `dag_merge.py` 的 DAG parser/assembler；不以 latest view、wall-clock quiet window、模型自评或部分成功冒充冻结完成。

统一路径：

- `ROOT=/home/zhoujie22/river2_0`
- `SPEC=$ROOT/memory_graph_evolving/specs/benchmark-diagnosis-replay-trajectory-export`
- `HOST=$ROOT/memory_graph_evolving/pi-group-chat-host`
- `GMS=$ROOT/memory_graph_evolving/graph-memory-service`
- `AREAL=$ROOT/areal`
- `MULTICA=$ROOT/multica`

## 2. 已确认的外部 seam

测试和调用方只允许跨以下 seam；PG-00D 之前只能写 contract/conformance red tests，不得自行发明 transport 字段。

1. **Evaluation seam**：冻结 `EvaluationBatchManifest`，其 logical identity 为 `evaluation_batch_id, task_id, episode_id, arm_id, seed, logical_run_id`；retry 另有 `attempt_id`，动态 delegate 原子登记 `agent_run_id`。`succeeded|failed|aborted` 为 Agent terminal；耗尽 retry 后 logical run terminal。所有 Rooms 关闭、receipts 齐全后形成 Batch barrier。
2. **Room lifecycle seam**：benchmark Room 使用 epoch/CAS 完成 `active → closing → closed`；closing 拒绝新 root turn、delegate、Delivery 和 Segment，只允许已登记工作终止。Production Room 不因 Cut 关闭。
3. **Cut HTTP seam**：`POST /rooms/{room_id}/consolidation-cuts` 返回 `202` 与 immutable `cut_id`；`GET /consolidation-cuts/{cut_id}` 是状态事实来源；支持 `threshold|force`、idempotency key、cooperative cancellation、显式 rediagnose，以及可选 signed webhook/event stream。
4. **Cut consistency seam**：Host 先冻结 Room sequence 与 sealed Segment IDs；receipts 齐全后，GMS 冻结 exact evidence batch IDs、每个 Space head/query state及 previous cut。Room 级 Cut、Space 级 CAS publication；进行中 Segment 延后。
5. **Diagnosis seam**：原始 annotation 绑定 `call_id` 或 `(segment_id, assistant_turn_seq)`，状态为 `verified|failed|inconclusive|missing`；高层分数由固定 policy digest 聚合。Diagnosis recipe、模型、prompt、tool policy、sampling 和 seed 全冻结。
6. **Proposal/Replay/Activation seam**：Proposal 归稳定 Diagnosis Agent；visibility 与 target 独立。Replay 在隔离 Room clone 中执行，branch 标记 `replay_skill_mutation` 及 ancestry；新工具副作用只允许 sandbox。通过固定 Replay Policy 后按 exact base revision CAS 激活；benchmark 只激活 evaluation namespace，production 需要 Promotion Record。
7. **Export seam**：immutable export 保存共享 DAG topology、逐 `agent_run_id` canonical structured transcript，以及 business、Memory Explore、Consolidation、replay Agent payload；Diagnosis 单独保存。归档含全部分支，普通 Training View 默认排除 replay root 及跨 Agent 污染后代。
8. **Retokenization seam**：冻结 tokenizer、chat template、special-token、truncation 和 loss-mask policy digests。默认只训练目标 Agent 自己的 assistant/tool-call argument tokens；其他消息和 tool result 只作上下文。
9. **Durability seam**：`freeze → write → acknowledge`；required durable sink 校验 digest 并写 receipt 后才能清理源 Session。Schema/version/digest 可组合；private identity 使用 tenant-scoped keyed digest。
10. **Governance seam**：当前权限与冻结最大披露边界取交集；payload 可 cryptographic erase，保留非敏感 tombstone；authorization epoch/revocation ledger 必须使缓存、运行中训练和 affected model lineage 可追踪。

## 3. 并行规则与文件所有权

- **PG-00D 是唯一 contract freeze gate**。所有实现组只读取 `$SPEC/system-contract.md`、`$SPEC/conformance/schema/**`、`$GMS/openapi/consolidation-cuts.yaml` 和状态机 bundle；发现契约缺口必须回到 PG-00 串行版本化，禁止在实现组间口头约定字段。
- 同一时刻，不同并行组不得修改同一文件。下表列出的“拥有文件/模块”是排他写集合；未列出的 composition root 只能由 PG-50 修改。
- `event_codec.py`、`SuperNodeAssembler`、`dag_merge.py` 只允许复用或最小扩展；`AssembledDag.from_dict` 不改。DAG 子图选择发生在 assembly 后。
- 每组执行一个纵向 TDD slice：一个可观察失败 → 最小实现 → 局部回归。不得先批量实现再补测试。
- 旧 working-tree 修改不是本计划产物；实施时不得覆盖。所有组开始前记录各仓 `git status --short`。

## 4. 工作图总览

```text
PG-00A normative contract
  → PG-00B schemas/state/reasons
    → PG-00C OpenAPI
      → PG-00D golden conformance freeze
        ├─ PG-10 Host Room lifecycle
        ├─ PG-11 Host evidence receipts
        ├─ PG-12 EvaluationBatch engine
        ├─ PG-13 Host Cut coordinator adapter
        ├─ PG-20 GMS Cut job
        ├─ PG-21 GMS per-source projection cursor
        ├─ PG-22 Diagnosis/private disclosure
        ├─ PG-23 Proposal/Replay/Activation governance
        ├─ PG-24 artifact security/revocation
        ├─ PG-30 AReaL DAG projection/export
        ├─ PG-31 reconstructed retokenization
        ├─ PG-32 durable export lifecycle
        └─ PG-33 Training View

PG-10 + PG-11 + PG-12 + PG-30 + PG-31 + PG-32 + PG-22 → PG-41 benchmark tracer
PG-11 + PG-13 + PG-20 + PG-21 + PG-22                     → PG-40 production Cut tracer
PG-22 + PG-23 + PG-30 + PG-33                             → PG-42 replay/activation tracer
PG-24 + PG-32 + PG-33                                     → PG-43 revoke/training tracer
PG-40..43                                                  → PG-50 composition wiring
PG-50                                                     → PG-60 migration/full acceptance
```

`PG-10..33` 在 PG-00D 后按排他文件集合最大并行；它们只用 shared fixtures/fakes 互测，不直接导入其他仓实现。

## 5. Contract-first 任务

| 并行组 | Blocking edges | 排他文件/模块 | Red tests / 契约产物 | 验收标准 |
|---|---|---|---|---|
| **PG-00A** | 无 | `$SPEC/system-contract.md`; `$MULTICA/docs/adr/0042-benchmark-diagnosis-replay-export.md`; `$MULTICA/docs/engineering-principles.md`; GMS `CONTEXT.md` | 逐条追踪 Q27–Q112；定义 authority、身份、watermark、fidelity、privacy | 三项特殊裁决逐字成立；无 latest/quiet-window/on-policy 伪声明；新裁决登记为“仅文档”直至 conformance 绿色 |
| **PG-00B** | PG-00A | `$SPEC/conformance/schema/**`; `$SPEC/conformance/state/**`; `$SPEC/conformance/reasons.yaml` | closed JSON schemas：EvaluationBatch、RoomClose、CommitReceipt、Cut/Job、DiagnosisAnnotation、Proposal ownership、Replay lineage、Export/TrainingView/Revocation；状态机 bundles | unknown core field fail closed；所有 terminal/transition authority、digest preimage、idempotency scope可机器验证 |
| **PG-00C** | PG-00B | `$GMS/openapi/consolidation-cuts.yaml`; `$SPEC/openapi/export.yaml` | POST/GET/cancel/rediagnose/webhook；freeze/write/ack/read/revoke | OpenAPI 3.1 lint 绿色；status/error/retry/strict-partial 与 schema refs一致；POST retry same key same body稳定、不同 body冲突 |
| **PG-00D** | PG-00B, PG-00C | `$SPEC/conformance/fixtures/**`; `$SPEC/conformance/tests/**`; `$SPEC/conformance/manifest.json` | 三语言共同 golden corpus 与 machine-readable adjacency | Go/Python conformance runner逐字节、digest、accept/reject/reason parity；之后 fixture只可版本化追加 |

## 6. 第一实施波：可并行模块组

| 并行组 | Blocking edges | 排他文件/模块 | 登记测试路径 | 验收标准 |
|---|---|---|---|---|
| **PG-10 Host Room lifecycle** | PG-00D | Host `internal/room/lifecycle*.go`, `internal/ports/room_lifecycle.go`,专用 adapter文件（不改 `store.go`） | `internal/room/lifecycle_test.go` | epoch CAS 原子阻断新 root/delegate/Delivery/Segment；已登记工作可终止；receipts齐后 closed；production Cut不关 Room |
| **PG-11 Host evidence receipt** | PG-00D | Host `internal/evidence/receipt*.go`, `internal/store/memory/store.go`, outbox snapshot adapter | `internal/store/memory/evidence_receipt_test.go`; `internal/runtime/session_barrier_test.go` | stage receipt在 commit retry/restart后保留；commit receipt含 batch/version/digest；DrainEvidence 任一未 committed即 fail closed；绝不跨 Room 投影 |
| **PG-12 EvaluationBatch** | PG-00D | Host `internal/evaluationbatch/**`；`cmd/bench-runner` 只由 PG-50B 接线 | `internal/evaluationbatch/manifest_test.go` | manifest known-set唯一；canonical logical ID无分隔符碰撞；retry attempt单调；动态 child登记；所有 logical runs/Rooms terminal 后 barrier；失败样本不丢 |
| **PG-13 Host Cut adapter** | PG-00D | Host `internal/consolidationcut/**` | `internal/consolidationcut/coordinator_test.go` | 两阶段冻结 Host half：固定 room epoch/sequence/sealed segments，等待 exact receipts；进行中 Segment延后；相同 request稳定 |
| **PG-20 GMS Cut job** | PG-00D | GMS `internal/consolidationcut/**` | `internal/consolidationcut/service_test.go`, `job_test.go` | Room串行 freeze；mode threshold/force/no_change；不可变 manifest；阶段状态、partial failure、resume/cancel、idempotency成立 |
| **PG-21 GMS projection/cursor** | PG-00D | GMS `internal/consolidation/service.go`, `internal/projectionbuilder/**`, `internal/store/memory/curation*.go` | `internal/consolidation/invariants_test.go`, `internal/projectionbuilder/per_source_cursor_test.go` | post-submit operation整轮拒绝；hierarchy strict DAG；exact batch set/per-source cursor不越过其他 Room；CAS/idempotency和checkpoint compaction成立 |
| **PG-22 Diagnosis/private access** | PG-00D | GMS `internal/diagnosis/**` | `internal/diagnosis/service_test.go`, `disclosure_test.go` | Cut-scoped Diagnosis可读 manifest内全部 private Spaces但不能跨 Room/tenant；call/turn annotation；missing/inconclusive mask；分区 rationale、citation/disclosure审计 |
| **PG-23 Proposal/Replay/Activation** | PG-00D | GMS `internal/skillevolution/proposal/**`, `replay/**`, `activation/**`, `promotion/**`（不改 composition root） | 各包 `*_test.go` | stable Diagnosis Agent owner；visibility/target分离；isolated replay、budget、matched baseline；base revision CAS；rollback及 evaluation→production promotion |
| **PG-24 Security/revocation** | PG-00D | GMS `internal/artifactsecurity/**`, `internal/revocation/**`, `internal/modellineage/**` | 各包 `*_test.go` | tenant envelope encryption/HMAC identity；cryptographic erasure+tombstone；authorization epoch使缓存失效；affected checkpoints/models可追踪 |
| **PG-30 AReaL DAG export** | PG-00D | AReaL `agents/dag/event_codec.py`, `agents/__init__.py`,新 `agents/dag/export_view.py`；不改 parser/assembler/merge | `tests/test_multi_agent_export_contract.py` | assembled `SuperNode.nodes` 不丢；共享 DAG 自包含投影；逐 agent_run payload；edge identity sidecar；replay ancestry跨 Agent剪枝且源 DAG不变 |
| **PG-31 reconstructed retokenization** | PG-00D | AReaL `core/batch_convert.py`,新 `core/structured_retokenization.py` | `tests/test_structured_retokenization_contract.py` | canonical records按 sequence；role/tool结构不字符串拼接；冻结 recipe；只 mask目标 Agent output/tool args；不生成 logprobs/versions；incomplete context fail closed |
| **PG-32 durable export** | PG-00D | AReaL新 `areal/v2/trajectory_export/**`（暂不改 `data_proxy/app.py`/`session.py`） | `areal/v2/trajectory_export/tests/test_lifecycle.py` | freeze/write/ack非破坏；中途失败零 pop；required sink receipt后清理；same export ID same digest，不同 frozen input冲突 |
| **PG-33 Training View** | PG-00D | AReaL新 `customized_areal/tree_search/training_view/**` | `tests/test_training_view_policy.py` | reconstructed用途限制；固定 diagnosis/aggregation/mask/role policy；默认排除 replay；显式 counterfactual view；test split污染与 revoke gate成立 |

## 7. 集成与 composition 组

| 并行组 | Blocking edges | 排他文件/模块 | 测试路径 | 验收标准 |
|---|---|---|---|---|
| **PG-40 Production Cut tracer** | PG-11, PG-13, PG-20, PG-21, PG-22 | `$SPEC/conformance/integration/run_production_cut.py`及对应 fixtures | `conformance/integration/tests/test_production_cut.py` | Host freeze→receipts→GMS exact batches→Diagnosis→Space publishes；open Segment延后；并发 trigger/no_change/partial retry可审计 |
| **PG-41 Benchmark tracer** | PG-10, PG-11, PG-12, PG-22, PG-30, PG-31, PG-32 | `$SPEC/conformance/integration/run_benchmark_barrier.py` | `conformance/integration/tests/test_benchmark_barrier.py` | known-set全 Agent terminal→Room closed→batch barrier→Diagnosis→逐 Agent reconstructed export；失败/aborted保留；下一 episode不越未完成 evidence barrier |
| **PG-42 Replay/Activation tracer** | PG-22, PG-23, PG-30, PG-33 | `$SPEC/conformance/integration/run_replay_activation.py` | `conformance/integration/tests/test_replay_activation.py` | private-derived Diagnosis-owned Proposal；disclosure/target approval；隔离 replay；全后代 taint；pass自动激活 evaluation scope；stale CAS不发布 |
| **PG-43 Revocation/training tracer** | PG-24, PG-32, PG-33 | `$SPEC/conformance/integration/run_revocation.py` | `conformance/integration/tests/test_revocation.py` | durable sink ack、payload erase、cache epoch失效、运行中训练停止、affected model lineage与tombstone完整 |
| **PG-50A GMS wiring** | PG-40, PG-42, PG-43 | GMS `internal/httpapi/consolidation_cuts.go`, `internal/httpapi/handler.go`, `cmd/server/main.go` | `internal/httpapi/consolidation_cut_contract_test.go` | HTTP实现逐响应匹配冻结 OpenAPI；GET事实源；signed event仅通知；purpose-bound auth；composition启停可恢复 |
| **PG-50B Host wiring** | PG-40, PG-41 | Host `internal/runtime/session.go`, `cmd/bench-runner/main.go`，以及 integration root `.gitignore` 的精确 build-artifact anchoring | runtime/runner contract tests | runner使用 EvaluationBatch engine；Room close、strict receipt barrier和 Cut adapter接入；旧拼接 run_id不再是事实源；源码目录不再被裸 `bench-runner` ignore pattern吞掉 |
| **PG-50C AReaL wiring** | PG-41, PG-42, PG-43 | AReaL `areal/v2/inference_service/data_proxy/app.py`, `session.py`及 route tests | `areal/v2/inference_service/data_proxy/tests/test_trajectory_export.py` | 新 export非破坏；旧 destructive endpoint不作为新路径；结构化 transcript→reconstructed tokens；ack/revoke正确 |
| **PG-60 Migration/full acceptance** | PG-50A, PG-50B, PG-50C | `$SPEC/migration/**`, CI/conformance scripts（独占） | `conformance/integration/tests/test_legacy_migration.py`及全套 | legacy缺失字段标 `unknown|legacy_unverified`；strict view/promotion fail closed；全部局部、cross-repo、OpenAPI/schema、race/pytest gates绿色 |

## 8. Blocking edges 与合入纪律

1. PG-00A→00D 严格串行；它们冻结所有跨组通信对象。
2. PG-10..33 不以彼此源码为依赖；需要对端时只使用 PG-00D fixtures 和 fake adapter。
3. PG-40..43 的 integration 文件互不重叠，可并行；它们不修改生产 composition root。
4. PG-50A/B/C 位于不同仓库，可并行，但各自是该仓唯一 composition-root writer。
5. 任何 schema/OpenAPI/state 修改都使 PG-00D digest变化，并要求所有直接消费者重跑；禁止实现组附带改 schema。
6. 每个组只 stage 自己拥有的文件；不得使用 `git add .`。旧 working-tree 修改必须原样保留。

## 9. TDD 和验收命令

Red 阶段要求：测试应成功收集/编译，并因缺失行为或明确断言失败；若 seam 尚不存在，可出现定向 `AttributeError`/compile failure，但计划优先使用可编译的行为红测。Red 不应由路径错误、fixture拼写或环境缺依赖造成。

首批命令：

```bash
cd "$HOST" && /home/zhoujie22/river2_0/.tools/go-1.26.8/bin/go test ./internal/store/memory -run '^TestCommitRetryPreservesStagedReceipt$' -count=1
cd "$HOST" && /home/zhoujie22/river2_0/.tools/go-1.26.8/bin/go test ./internal/evaluationbatch -run '^TestParseManifestRejectsDuplicateLogicalRuns$' -count=1
cd "$GMS" && /home/zhoujie22/river2_0/.tools/go-1.26.8/bin/go test ./internal/consolidation -run '^TestConsolidationRejects(PostSubmitOperations|HierarchyCycles)$' -count=1
cd "$AREAL" && uv run pytest customized_areal/tree_search/tests/test_multi_agent_export_contract.py -q
cd "$AREAL" && uv run pytest customized_areal/tree_search/tests/test_structured_retokenization_contract.py -q
```

Green 后局部回归：

```bash
cd "$HOST" && /home/zhoujie22/river2_0/.tools/go-1.26.8/bin/go test ./internal/store/memory ./internal/runtime ./internal/evaluationbatch ./cmd/bench-runner -count=1
cd "$GMS" && /home/zhoujie22/river2_0/.tools/go-1.26.8/bin/go test ./internal/consolidation ./internal/projectionbuilder -count=1 -race
cd "$AREAL" && uv run pytest customized_areal/tree_search/tests/test_event_codec.py customized_areal/tree_search/tests/test_multi_agent_export_contract.py customized_areal/tree_search/tests/test_structured_retokenization_contract.py -q
```

最终验收还必须包含 Host/GMS `go test ./... -race`、AReaL相关 pytest、schema/OpenAPI lint、PG-40..43 cross-repo tracers。GPU/multi-node测试如不可用必须明确记录 skipped reason，不能冒充已验证。

## 10. 当前进度（2026-09-14）

- [x] Q27–Q112 设计树收敛；三项特殊用户裁决已固定。
- [x] 调查现有 Host/GMS/AReaL seam、测试惯例与跨仓 plan 格式。
- [x] 确认 `evol_bench/.../code/bench-runner` 是快照，可信 Go 源为 Host `cmd/bench-runner`。
- [x] 冻结本计划的并行组、blocking edges、排他文件集合和验收标准。
- [x] PG-00A–D 合同冻结完成（v1.1.1）：system-contract v1.1.0 + SC-11.4 修订表、12 个 closed schema、2 个状态机 bundle、33 reason codes、OpenAPI v1.1.0 双文件、58 fixture 金标准语料（manifest v1.1.1 + extra_rules）；Go/Python runner 与独立校验器三方 parity 绿。
- [x] 第一批 PG-11/12/21/30/31 红测→实现全绿；配套 9 项实现缺陷（P0-2/P0-3/P1-4..8/P2-9）修复并带回归测试。
- [x] 第二批并行组红测→实现全绿（本轮，TDD：kiro 写红测、实现补绿、kiro review）：
  - PG-10 Host Room lifecycle（epoch CAS、closing 写栅栏、close guard、production Cut 不关房；-race）
  - PG-13 Host Cut coordinator（幂等快照、open segment fail-closed 延后、exact receipts、模式/身份/收据校验；-race）
  - PG-20 GMS Cut job（Room 串行 freeze、threshold/force、不可变 manifest、冻结状态机全边表、幂等/取消/恢复/rediagnose、审计链；-race）
  - PG-22 GMS Diagnosis（Q53=A capability 精确范围+过期、annotation oneOf、missing/inconclusive mask、policy-digest 聚合、disclosure 分区+审计链；-race）
  - PG-23 promotion 协调边界（Q90=C ownership/visibility/target 分离、隔离 replay 校验、exact base CAS、rollback；-race）
  - PG-24 artifactsecurity/revocation/modellineage（AES-GCM 信封+KEK 轮转擦除、HMAC 租户身份、epoch 失效缓存、append-only ledger、协作停止、谱系去重级联+tombstone 传播；-race）
  - PG-32 AReaL durable export（freeze→write→acknowledge 强顺序、ack 前置 write 门、required sink receipt 校验、同 ID 幂等/异输入冲突、线程安全）
  - PG-33 AReaL Training View（reconstructed 用途枚举、四 policy digest 钉扎比对、branch/role 枚举 fail-closed、默认排除 replay/taint、显式 counterfactual、test split 拒绝、revoke gate）
- [x] kiro review 第 1 轮（Host/AReaL）完成并修复：P0 两项（ROOM_EPOCH_STALE 误作转移 authority → 仅 BARRIER_REACHED 可入 closing；acknowledge 绕过 write → write 门前置）+ P1（幂等域 tenant/room/mode、receipt 身份/内容校验、锁范围、freeze 线程安全、内容 digest preimage 去 sink）。
- [x] kiro review 第 2 轮（GMS 六组）完成，P0/P1 修复全绿并经 terra 复核裁决：
  - PG-20：Transition 增 guardFor 逐边验证冻结 guard（diagnosis_verified、SpaceResult 终态、rediagnose 专属边）；Cancel 限定 bundle 可取消 source；外部 freezer 产物过 manifest 校验（identity/pinned revision/sha256 digest/receipt）；freeze 移出全局锁（三段式 Trigger）。
  - PG-22：RegisterCutScope + IssueCapability 与冻结 cut private 集合精确 multiset 相等；过期判定改 authority clock（ReadAt 只作事件时间）；nil disclosure gate fail-closed；annotation 改 (tenant,id,revision) 不可变历史（同 revision 幂等/冲突区分）；status/revision/digest-shape schema 校验。
  - PG-23：PromotionRequest/Record 增 PromotedRevision；Promote CAS active==base 后置 active=promoted；Rollback 双重 CAS（ExpectedRevision==record.PromotedRevision 且当前 active==promoted），可证明 base@42→promoted@43→rollback@42 且被新 activation 压制的旧 record 不能回滚。
  - PG-24a：初始 KEK 确定性派生（重启后未擦除信封可读）+ per-(tenant,artifact) KEK（Erase 只毁本 artifact）；tombstone KeyID 门 + NewServiceWithTombstones 恢复使擦除态跨重启 fail-closed；AAD 三字段全长度前缀（无歧义绑定）；Open 全程持锁；TenantIdentity 用独立 identity key（跨擦除稳定）。
  - PG-24b：Revoke 严格 epoch bump（同 epoch 拒绝）；Authorize/MayContinueTraining 精确匹配 current epoch（未来 epoch fail-closed）。
  - PG-24c：重复 Put 拒绝（ErrNodeExists，杜绝 stale 邻接边/间接环）；跨 tenant parent 拒绝；cascade tombstone 落数据 ledger 幂等可查。
- [x] terra 对第 2 轮修复的验证裁决完成（只读 + 全仓 -race 复跑）：authority clock 分离、nil gate fail-closed、AAD 全字段长度前缀、Open 全程持锁、epoch 严格 bump/精确匹配、immutable graph、rollback 双 CAS 等核心方向全部 accept；据此又修复其指出的包内可修 P1：PG-20 Room reservation（锁拆分后 SC-4.1 仍成立）、Cancel 恢复 SC-4.8 version CAS、幂等键扩为 tenant/room/source/key 四元组、freezer 产物 manifest 深拷贝；PG-22 issue 时钟有效性 + 审计链 digest 64hex + disclosure 只认已存 annotation revision（gate 调用不持锁）；PG-23 proposal write-once（同 ID 改 owner/visibility/target 拒绝）；PG-24c cascade 单锁 span（消灭 TOCTOU）+ tombstone 子树禁止再生。
- [x] PG-40..43 集成 tracers 第 1 轮实现经 terra review 判 **reject**（7 P0 + 3 P1），本轮全部修复并重新全绿：
  - P0-4 promotion 真实证据链：`RecordReplayResult`（封闭 verdict 词表 pass|fail|inconclusive|budget_exhausted、sha256 digest 形状校验、单一 (session,proposal) verdict、幂等重录与冲突区分）→ `ActivateEvaluation`（仅 pass、仅 evaluation namespace、不可变）→ `Promote`（activation 的 proposal/candidate/base/target 精确绑定匹配 + CAS）；伪造 ref、free-form ref、candidate/base/target 不匹配、fail 激活、stale base 均有 tracer 拒绝探针。
  - P0-5 consolidationcut 移除自报 guard 布尔：`TransitionRequest` 改携带 `TransitionEvidence` 引用（annotation ref、receipts digest、policy revision、proposal/replay/CAS ref）；8 方法 `GuardAuthority` 在双 CAS 锁窗口之间无锁查询，nil authority/空 ref fail-closed；准入证据固化进 `Job.LastTransitionEvidence` 与 audit `evidence=...`；伪造 ref、未验证 annotation、错误 policy 绑定、重诊后 stale v1 证据全部拒绝。
  - P0-6 `Rediagnose` 仅允许 partially_failed→diagnosing、要求不同 policy revision、原子绑定新 diagnosis run ref；SC-4.2 本地 guard 将 diagnosing→consolidating 的证据 policy revision 绑定到 Job 的 active revision（重诊后旧证据即 stale）。
  - P0-7 `Trigger` freezer 错误路径的 reservation 清理统一走 `releaseRoom`（Lock→releaseRoomLocked→Unlock），-race 并发回归测试覆盖。
  - P1-10 `Erase` reason 改封闭非敏感枚举 `EraseReason`（PII/自由文本/过长值拒绝）；`TombstoneCascade` 先记 root（diamond 得 4 条 tombstone，root Ref==SourceRef），`Put` 拒绝 root 或任意 tombstoned descendant 之下再生（ErrTombstonedAncestor）。
  - P1-8/9 Host tracer：shared/private Space 各自独立 evidence batch 集合 + per-Space projection head/query watermark；logical-run ledger 以 identity 为键（attempts、terminal state、failure reason 保留，绝不按 RoomID 覆盖）；真实 next-episode admission seam（pending/partial 拒绝并输出阻塞 outbox IDs + reason，全部 durable 后放行）与 crash/restart outbox 恢复（staged 行仍阻塞）；barrier facts 以 JSON 文件输出作为跨仓合同。
  - P0-1/2/3 编排器：step `detail` 改结构化 dict、assertion `detail` 保持 string rationale（kiro 建议的 shape 合同），`build_trace` 对重复 step name/assertion ID fail-closed；PG-41 改为 Host `benchmark-barrier <manifest> <facts.json>` → GMS `batch-diagnosis <facts.json> <manifest>`（barrier 未放行、manifest 不匹配、ledger 非全 terminal 即拒绝；每个 logical run 走真实 Trigger→RegisterCutScope→IssueCapability→ReadPrivate→AppendAnnotation→Aggregate→guarded walk）；PG-42 taint 改为 `ExecutionDAG.descendants` 真实 outgoing-edge 遍历 + 独立声明的 expected 集合 + replay prefix 作为 exclusion authority（含 sibling/ancestor 排除、legacy branch fail-closed）。
  - 本轮全绿：GMS/Host `go test ./... -count=1 -race`；AReaL 合同 pytest 37 项（另 test_execution_dag 19 项同绿）；spec conformance 1 项 + integration 8 项冻结 tracer 测试。
- [x] PG-40..43 集成 tracers 第 2 轮 terra review 仍判 **reject**（R1–R6，1 P0 + 5 P1），本轮全部修复并重新全绿：
  - R1（P0）GMS `BatchDiagnosis` 改严格单文档解码（DisallowUnknownFields + token-walk 重复 JSON key 检测 + 尾随数据拒绝）+ facts/manifest 封闭枚举与 sha256 digest 形状校验 + receipt/space/children 内部一致性（receipt 恰好覆盖每个 sealed segment 一次、space batch 必须来自 commit receipts、shared/private 批次集不相交、child 状态封闭且与 run terminal state 一致、outbox 行数对账）+ facts↔manifest 精确双射（含 `expected_agent_runs` 与 children 比对）；所有校验先于首个 cut 铸造。Host facts 补 `children` 数组。新增 GMS integrationtracer 驱动级测试 18 项（伪造 facts 的未知字段/重复 run ID/缺集/多集/错 agent 集/identity 伪造/barrier 未放行/restart 行不符/坏 digest/receipt 越界/space 批次无 receipt/child 非终态/尾随 JSON/重复 JSON key 全部拒绝）。
  - R2 `ReplaySession` 改为不可变 canonical 快照（proposal/source/clone/ancestor/mutation branch/sandbox/baseline/candidate/budget 全量持久，session ID digest 覆盖全部绑定字段，identical request 幂等重放、divergent ancestor 得到不同 session）；`RecordReplayResult` 验证 (session, proposal) 关系（ErrReplaySessionMismatch）；`ActivateEvaluation` 从快照派生绑定——candidate 必须等于 session 冻结值、evaluation target 必须精确等于注册 proposal 的 target、base 必须等于 production 投影当前 active revision（已移动即 ErrStaleBaseRevision）；caller 自由字段全部失效。新增快照绑定拒绝测试 + tracer 探针（错 proposal verdict、错 candidate/target activation、stale-base activation）。
  - R3 consolidationcut `GuardAuthority` 8 方法全部返回 `GuardConfirmation{FactsDigest, Facts}`（包级 `ConfirmFacts` 规范化 digest）；`Transition` Phase C 持锁重解析同一 guard 并要求 digest 精确一致——Phase B→C 之间事实被撤销或漂移（JobVersion 未动）即 fail-closed（审计含 "phase C re-resolve"），`ErrGuardNotSatisfied` 包装；admission stamp 新增服务端解析的 `AuthorityFactsDigest`（覆盖 caller 自报值，caller 声明一律忽略）。新增撤销竞态与漂移竞态回归测试（job 不变量保持 + 审计可查 + digest 非 caller 可伪造）。
  - R4 Host PG-40 freeze 改走真实 evidence settlement：每个 sealed segment 经 `SettleAndCreateOutbox` 落 room_shared/agent_private outbox 行，space 批次集合从 store 读回行派生（与 receipts 集合比对，`batches_match_receipts`/`outbox_rows_persisted=receipts*2` facts），projection head/watermark 显式标注 `fixture_authority`（PG-50B 换 typed projection rows）。
  - R5 outbox 改 claim-lease 所有权语义：`ClaimPending` 返回 `ports.OutboxClaim{EntryID, Token}`（`ports.ErrNoOutboxClaim` 哨兵），`MarkStaged/MarkCommitted/Reschedule` 必须持有有效 token（未领取/stale/异 worker token → `ErrOutboxNotLeased`），staged 行拒绝 re-stage（`ErrOutboxAlreadyStaged`，恢复只允许 commit retry）、commit 必须有 durable stage receipt（`ErrOutboxStageReceiptRequired`）；`RecoverPendingOutbox` 是 restart 边界（清除全部 lease，crash 前token 全部失效）；admission 改为 store 侧 `AdmitNextEpisode` batch-gate authority（与 settlement 同锁序列化：check 与 admission 之间新落 outbox 行会阻塞 admission；一个 batch 恰好 admitted 一次，`ErrOutboxDoubleAdmission` + admission log）。runtime `drainOutbox` 重写为 lease-holding worker（claim→stage→re-claim→commit，staged 行 commit-only retry，末尾 fail-closed recount）。新增 lease 所有权与 single-flight/admission authority 测试；PG-41 tracer 探针：unleased/stale/foreign stage、restage refused、double worker distinct、check-vs-new-outbox blocked、double admission rejected、crashed worker rejected。
  - R6 `TombstoneCascade` 改收 `artifactsecurity.EraseReason`（共享封闭枚举，`ValidEraseReason` 导出成员检查）：空/自由文本/PII/未知 code 在写任何 tombstone 前拒绝（`ErrCascadeReasonInvalid`）；新增枚举拒绝测试 + tracer `cascade_reason_closed` 探针（free text/PII 拒绝且 ledger 不增长）。
  - 新增 Host integrationtracer 驱动级测试（PG-40 freeze 合同 facts + PG-41 barrier/admission/lease 探针 + facts 文件不变量）。
  - 本轮全绿：GMS/Host `go test ./... -count=1 -race`；AReaL 合同 pytest 37 项；spec conformance 1 项 + integration 8 项（含全部新探针）。
- [x] 第 3 轮 terra 复审裁决：R1/R2/R4 accept-with-notes（非阻断 notes：R1 recovered_ids 精确集合比对挂 PG-50B 合同扩展；R2 未初始化 active 指针接受 caller-declared initial base 为 PG-50 残留）、R6 accepted，**R3/R5 仍判 reject（合同级阻断）**，本轮全部修复并重新全绿：
  - R3'（死锁 + digest 自报）：`Transition` 重构为 Phase B（无锁 resolve）→ Phase C-pre（无锁 re-resolve + digest 精确一致）→ Phase C（持锁 CAS 重验 + stamp + apply，**零 authority 调用**）——authority adapter 允许回调 cut service（如 tracer 读 `GetManifest`），持锁 re-resolve 会自死锁；新增 reentrant authority 死锁回归测试（DiagnosisVerified 内调 GetManifest，10s watchdog）。服务端在**每次** resolve 重算 canonical digest 覆盖 authority 上报 facts（constant digest 漂移事实 "does not cover its facts" fail-closed，空 facts fail-closed）；canonical 名固定为 resolving authority method 名（`proposal_refs`/`side_effects_completed`，一方法服务两 edge），新增 lying-digest 拒绝测试。
  - R5'（restart token collision + admission 持久化 + 破坏性 drain recovery）：lease token 加持久化 outbox epoch 前缀（snapshot 持久化 epoch，restore 严格 +1——上一进程 lifetime 的 token 永不能在 restore 后验证为行 owner）；snapshot 持久化 `admittedBatch`/admission log（restore 后重复 admission 仍是 double admission）；新增非破坏性 `ports.OutboxStore.ListPending`（Store/StoreOutbox/独立 Outbox 三处实现），runtime `drainOutbox` 起止 recount 与 tracer `barrierReady` 全部改用 ListPending，`RecoverPending` 降级为显式 startup-recovery 边界（仅 tracer crash/restart 序列保留）；新增 4 项测试：snapshot stale-token 拒绝、admission-after-restore double-admission、ListPending 不清 live lease（与 RecoverPending startup 边界对照）、两个并发 drain 恰好一次全 commit 且零 lease 失败。
  - 本轮全绿：GMS/Host `go test ./... -count=1 -race`；AReaL 合同 pytest 37 项；spec conformance 1 项 + integration 8 项。
- [x] 第 4 轮 terra 复审仍判 **reject**（R3'/R5' 合同级阻断），本轮全部修复并重新全绿：
  - R3'（apply-time authority TOCTOU）：C-pre 无锁 re-resolve 与持锁 apply 之间 authority facts 仍可变——Job CAS 只保护 version/stage，不保护 authority facts。R3'' 修复：`GuardAuthority` 接口新增 `ReserveFacts`/`ReleaseFacts` reservation seam（doc 钉死语义：reservation 期间 fact 变更必须阻塞到 Release）；`Transition` 改为 Phase A（持锁 local）→ Phase B（无锁 resolve）→ **ReserveFacts → 无锁 re-resolve（digest 必须一致）→ Phase C（持锁 CAS+apply，零 authority 调用）→ defer ReleaseFacts**（defer 注册顺序保证先 Unlock 再 Release）；`ConfirmFacts` 改长度前缀编码（`name` + 每 pair `\0 len(key):key len(val):value`，消除 NUL/=` 碘撞歧义）。新增 deterministic hook 测试：C-pre 成功后撤销 fact → apply 必须 `ErrGuardNotSatisfied` 且 job 不变（`TestGuardFactsRevokedAfterResolveFailClosedAtApply`）；reservation 期间并发 revocation 被阻塞在 apply 之后（`TestGuardReservationSerializesRevocationBehindApply`，阻塞证明 + release 后 withdraw 完成 + 新 service 验证已撤 fact 拒绝）；GMS/Host integrationtracer authority adapter pass-through Reserve/Release（seam 留给 PG-50A composition）。
  - R5'-1（wrong-row commit）：`MarkStaged` 曾释放 lease、drain 曾用不带 row ID 的 `ClaimPending()` 盲拿 row 并对任意 row 发远端 commit、`_ = MarkCommitted(...)` 忽略错误照样 append committed。R5'' 修复：`MarkStaged` **保留 stage lease**（GMS Host 两处 Store/Outbox；释放路径只剩 Reschedule/RecoverPending/MarkCommitted）；runtime `drainOutbox` 重写为 claim→(staged? commit-only)→remote stage→MarkStaged→**同 claim commit**——不存在 stage 后 re-claim 盲拿错行的窗口；`MarkCommitted` 失败与远端 commit 失败**双双 fail-closed**（Reschedule + drainErr + 绝不 append committed）；PG-41 tracer restage 探针改两次 MarkStaged（第二次 `ErrOutboxAlreadyStaged`）→ 同 claim MarkCommitted，crash/restart 序列（RecoverPending 杀 lease → crashed token 拒绝 → 重拿 staged 行 commit-only）仍成立。
  - R5'-2（独立 Outbox 无跨恢复 epoch fence）：独立 `Outbox` 曾把 epoch 写进 token 但没有 snapshot/restore。R5'' 修复：新增 `Snapshot()/Restore()`（`outboxSnapshot{Epoch, Rows}`，JSON；restore 时 epoch 严格 +1、claims=0、活 lease 不持久化；`MaxInt64` fail closed）；`Store.Restore` 同加 epoch 溢出 guard；`RecoverAfterRestart` 在 DeliveryStore.RecoverPending 之后显式调 `OutboxStore.RecoverPending`（startup 边界只在 restart，error 传播）。
  - 新增 deterministic 测试：`TestOutboxStageLeaseBarrierDeterministic`（store-backed 与 standalone 双队列各跑同一 barrier 脚本：A claim+MarkStaged 后 B 只能拿到别的 row、B token 对 A 行 MarkStaged/MarkCommitted 均 `ErrOutboxNotLeased`、A 原 lease 恰好 commit 一次、repeat commit 死 lease 拒绝、Reschedule 释放路径后旧 token 失效 + commit-only 重试、ListPending 空 + ClaimPending 返回 `ErrNoOutboxClaim` 即无遗留 live lease）与 `TestStandaloneOutboxRestoreEpochFencesStaleTokens`（独立 Outbox 镜像 Store 的 restart stale-token fence）。
  - Notes 处置：epoch `MaxInt64` 溢出 guard 已做（Outbox.Restore + Store.Restore 双处）；`RecoverAfterRestart` 已显式接线 outbox RecoverPending；admission scan 按 batch 限定属 scope 遗留，挂账 PG-50。
  - 本轮全绿：GMS/Host `go test ./... -count=1 -race`（GMS 需 PATH=/usr/bin 前缀使 python3 解析到带 jsonschema 的解释器）；AReaL 合同 pytest 37 项；spec conformance 1 项 + integration 8 项。
- [x] 第 5 轮 terra 复审：R3'' 判 **accept-with-notes**（notes：PG-50A 生产 adapter 必须把 ReserveFacts 与全部 fact mutation 纳入同一 authority-side 序列化协议；ReserveFacts 无可取消 lease 语义，生产 adapter 应使用 ctx；建议补 reservation-acquisition 失败测试），R5'' 仍判 **reject**（2 阻断），本轮全部修复并重新全绿：
  - R5'-1（Reschedule 失败被吞）：remote stage/commit 或 MarkCommitted 失败后 `_ = Reschedule(...)` 忽略释放错误——若释放失败，row 继续持 live lease，同进程后续 drain 无法 re-claim，且调用方看不到“lease release/retry recording 失败”根因。R5''' 修复：`drainOutbox` 新增 `failEntry` helper（orchestration.go），三个失败路径统一走它：Reschedule 失败时记 `outbox_release_failed` 事件，drainErr 合并双根因（原 remote error 以 %w 保留为 cause + release 失败附注“row stays leased until a restart boundary”）；MarkStaged 失败路径同补 lease 释放（旧行为直接 continue 会 pin 住 live lease），释放失败双根因可见。新增 `TestDrainFailsClosedWhenLeaseReleaseFails`（runtime）：stage 远端 500 + 注入 Reschedule 失败 → 错误同时含 stage 根因与 release 根因；row 仍 pending 且 ClaimPending 返回 `ErrNoOutboxClaim`（live lease single-flight 钉死，无假绿）。
  - R5'-2（Adopt 失败被吞）：独立 durable Outbox 接线下 `SettleAndCloseSegment` 已把 segment 标 settled 而 `_ = adopter.Adopt(entries)` 忽略 handoff 错误——worker queue 永远收不到 rows，后续 drain 空队列返回成功，已产生 evidence 被静默遗漏，违反 receipt/evidence barrier fail-closed 合同。R5''' 修复：两处 Adopt 全部 fail-closed 向上传播——`RecoverAfterRestart` segment settlement（`recover segment %s: hand evidence outbox entries to the worker queue: %w`）与 `executeTurnUnchecked` turn settlement（`turn settlement %s: ...`），turn/recovery 均返回错误而非假成功。新增 `TestRecoveryFailsClosedWhenOutboxHandoffFails` + `TestTurnSettlementFailsClosedWhenOutboxHandoffFails`（runtime，注入 failing adopter：错误含 handoff 根因、queue 零 rows、recovery 不得当作 drained/complete）。
  - r5 note 处置：`Store.Restore` 的 `MaxInt64` epoch 校验移至 json.Unmarshal 之后任何 state 变更之前（validate-then-install，restore 失败不再留下半更新 receiver）；R3 note 的 reservation-acquisition 失败测试已补（`TestGuardReservationAcquisitionFailsClosed`：ReserveFacts 拒绝 → `ErrGuardNotSatisfied` 含根因 + job 不变 + 审计可查）；recovery capability 拆分、admission batch-scope、outbox 行 RoomID 隔离均挂账 PG-50。
  - 本轮全绿：GMS/Host `go test ./... -count=1 -race`（GMS 需 PATH=/usr/bin 前缀）；AReaL 合同 pytest 37 项；spec conformance 1 项 + integration 8 项。
- [x] 第 6 轮 terra 复审：R5'''-1（Reschedule/lease-release fail-closed + MarkStaged 失败路径 lease hygiene）判 **accept**（notes：drainErr 首错保留、后续 release 失败靠 outbox_release_failed 事件可见；建议后续 errors.Join）；Restore validate-before-install 判 **accept-with-notes**（建议补 MaxInt64 直接测试）；R5'''-2（独立 Outbox handoff）仍判 **reject**（2 项架构级阻断：① handoff 是可选私有 type assertion——未实现 outboxAdopter 的独立 queue 被静默跳过，settlement 成功、queue 永远为空、后续 drain 对空队列假绿；② 已失败 handoff 无 durable 补偿/reconciliation——store 已 settled 而 queue 缺 rows，上层吞错继续运行或重启后 queue 仍空则 ListPending/ClaimPending 假绿；新增测试未证明后续 drain/barrier 拒绝），本轮全部修复并重新全绿：
  - R5''''（handoff 升级为强制 port 合同）：`ports.OutboxStore` 新增强制 `Reconcile(ctx, entries) error`（doc 钉死：缺行插入、既有行不降级（staged/committed 行不被 ledger 的 stale 记录降级）、store-backed 验证 settlement 同锁事务写入的行存在、error=至少一行未入队必须 fail-closed）——“未实现 Adopt 时 settlement 必须失败”被结构性满足：port 方法缺失=编译失败，runtime 不再存在任何静默跳过分支（原 outboxAdopter 可选断言删除）。独立 Outbox 实现 add-missing/no-downgrade 的 `Reconcile`；Store 实现 `ReconcileOutbox`（验证存在，未持有=contract breach 拒绝）+ `StoreOutbox.Reconcile`。
  - R5''''（durable ledger + reconciliation）：`Store.OutboxEntries` 枚举全部 settled rows（durable evidence ledger）；runtime 新增 `outboxLedger` capability（无法枚举→拒绝 drain/restart，fail-closed）；`RecoverAfterRestart` 在 startup 边界 sweep 全部 ledger rows 入 queue（半交接 re-hand，queue 拒绝→restart 失败）；`drainOutbox` 开头 reconciliation guard（每次 drain 前对账 ledger vs queue：缺行自愈重 hand，queue 仍拒绝→drain 失败，杜绝空队列假绿）。
  - 新增/更新测试：`TestLaterRunsRefuseWhileOutboxHandoffIsBroken`（半交接后后续 run 拒绝且 ledger 完好）；`TestRestartReconcilesPendingHandoff`（健康 queue 重启自愈：startup sweep→drain 把此前半交接的 settled evidence 全部 commit，不丢行）；`TestOutboxReconcileHandoffContract`（双实现：committed 行不被降级、missing 行插入、store-backed foreign row 拒绝）；`TestHostRestoreRejectsExhaustedEpochLeavingStoreUnchanged` + `TestStandaloneOutboxRestoreRejectsExhaustedEpoch`（r6 note：MaxInt64 被拒且 receiver 未动、后续合法 image 仍可安装）；failclosed 三项改注入 Reconcile 失败。
  - r6 notes 处置：MaxInt64 直接测试已补（双处）；errors.Join 建议挂账 PG-50（首错保留语义不变）；recovery capability 拆分、admission batch-scope、outbox 行 RoomID 隔离继续挂账 PG-50。
  - 本轮全绿：GMS/Host `go test ./... -count=1 -race`（GMS 需 PATH=/usr/bin 前缀）；AReaL 合同 pytest 37 项；spec conformance 1 项 + integration 8 项。
- [x] 第 7 轮 terra 复审（PG-40..43 集成 tracers 终裁）：**accept**。逐项裁决：R5⁗-1（强制 Reconcile port、handoff 完整性）accept；R5⁗-2（Store/standalone Reconcile 语义）accept-with-notes（note：Store-backed Reconcile 按 entry.ID 确认存在，未逐项比较 SourceSegmentID/Projection/SpaceID/BatchID 等 metadata——当前无可信输入路径，建议下批补 same-ID/different-payload 负测并验证 immutable identity 字段）；R5⁗-3（startup sweep、drain guard、半交接恢复）accept；R5⁗-4（RecoverPending 与 reconciliation 顺序）accept；Restore max epoch accept。独立复跑全绿（Host/GMS -race、SPEC 8+1、AReaL 37、六条跨仓 driver -race）。Non-blocking notes 挂账：① outboxLedger 仍是 runtime 私有 capability 断言（缺失时 fail-closed，非漏洞），PG-50 可提升为显式 dependency/port；② Reconcile 多失败聚合建议 errors.Join（诊断改善）；③ admission 全 Store scan/outbox 行 room+batch identity（PG-50 scope 扩展已登记）。
- [ ] PG-50A/B/C composition wiring、PG-60 migration/full acceptance 未开始（下一批）：含 terra 累积挂账项——PG-50A 生产 authority adapter（ReserveFacts 与全部 fact mutation 同 authority-side 序列化协议、ctx 取消）、outboxLedger 显式 dependency、admission batch-scope、outbox 行 RoomID/batch identity、drainErr errors.Join、Store Reconcile identity 字段校验 + same-ID/different-payload 负测。
- [ ] terra 裁决的下一批合同级阻断项（全部是 durable/authority 接线，归 PG-50A/B/PG-43）：① PG-20 状态迁移 guard 必须取自持久 job/cut/receipt/replay/proposal 事实或可信服务端 adapter，不能由 TransitionRequest 布尔自报（本轮已删除自报布尔并落地 TransitionEvidence refs + GuardAuthority 无锁查询/fail-closed 语义；持久服务端 adapter 接线仍归 PG-50A）；② PG-22 capability scope 必须由不可变 Cut manifest 投影并与 issuance 原子绑定，删除任意调用者 RegisterCutScope 授权面；③ PG-24a 需认证、原子、durable 的 artifact key-state store（active key version + tombstone 集合，rotate/tombstone 原子提交，启动 fail-closed），并补 erase→restart→seal fresh→restart→open 回归。
- [ ] 遗留债务（挂账）：PG-10 持久化 authority 与生产写路径栅栏接线（PG-50B）、PG-13 冻结原子性靠 adapter 保证（PG-50B）、PG-32 真实 sink/source adapter（PG-50C）、PG-33 authorization epoch 重验 seam（PG-43）、PG-22 disclosure gate 语义库与 Repository 持久化（PG-50A）、PG-23 evaluation proof/privacy clearance/target authorization 校验与 durable promotion 事件、PG-24b ledger conditional append/CAS + event 词表/cursor 分页、PG-24c durable tombstone 持久化（均 PG-50A/PG-43 归口）、AReaL `test_public_exports_available_from_package` 需完整 venv（1 项环境限制）、batch_convert 旧 tensor 路径重接（需 torch 环境）。

本轮（第 4→7 轮 review 循环）完成 PG-40..43 集成 tracers 全部 terra 阻断项修复并经第 7 轮复审终裁 **accept**：R3 事实 reservation（ReserveFacts/ReleaseFacts seam + 长度前缀 digest 编码 + revocation-behind-reservation 确定性测试）、R5 同 claim commit/MarkStaged 保留 lease/MarkCommitted fail-closed/epoch fence 双实现 snapshot-restore/Reschedule-失败可见/强制 Reconcile handoff port 合同/startup+drain 双重 ledger reconciliation（半交接拒绝与自愈）/MaxInt64 validate-before-install；全部验证套件绿（GMS/Host -race、AReaL 合同 pytest 37、spec conformance 1 + integration 8）；此前各轮 Red→Green 与 review 修复均已合入；除上述白名单文件外未动其他文件。PG-50A/B/C 与 PG-60 为下一批（含累积挂账项）。

- [ ] **PG-50A GMS wiring 第 4 批：consolidation-cut composition 五要素全部落地（2026-09-16）**——本批完成手稿对接的 PG-50A 全部 5 项冻结验收的实现，代码已全绿，**待第 1 轮 terra review / 裁决**：
  - ① **HTTP 表面 + DTO + 幂等 + 目的绑定授权**（`internal/httpapi/consolidation_cuts.go` + `handler.go`）：create(get 走 withBinding + Idempotency-Key 必填)/get/`:cancel`/`:rediagnose` 四路由经 ServeHTTP switch 分发；flatCutError 扁平 `{"code","message"}`（非 transport 包裹式 wireError，匹配冻结 OpenAPI Error schema）；CutJobDTO 字段白名单省略内部 Job 字段（DiagnosisPolicyRevision/FailedFrom/LastTransitionEvidence/NewDiagnosisRunRef/PrincipalID/PreviousCutID 等一律不输出）；幂等复合键 `TenantID|RoomID|trigger_source|key`（同 key 同 body 重放 202 原 job；同 key 改 mode → 409 CUT_IDEMPOTENCY_CONFLICT）；服务端 mint NewDiagnosisRunRef（确定性 `diagnosis-run-`+shortDigest）；**新增 409 ROOM_EPOCH_STALE**（同 room 已有 in-flight 冻结时新 key 触发返回 409 而非 202，GET 权威重试——本轮补齐，服务端新增导出 `MintCutID` 单一来源 + handler 冲突检测）。
  - ② **生产 freezer**（httpapi `RoomFreezer`）：room→space 由 `-cut-room-space room=space1,space2` 可重复 flag 解析（package-level `roomSpaceBindingsValue`）；未绑定 room fail closed（422 MISSING_MANIFEST_FIELD）；sealed segments = 绑定 space 内 committed batches 的 distinct SourceSegmentIDs（排序确定）；threshold 模式取与 `lastSealed` 差集（空差集→completed/no_change），force 全量；BatchDigest=`sha256:`+ContentSHA256；SpaceScope（EvidenceBatchIDs/ProjectionHeadVersion=registry space.Version/QueryWatermark=committed batch 计数）、RoomSequenceWatermark=max batch event Sequence；**授权放 handler（freezer 纯事实、无 authorizer 依赖）**——create handler 内 `AuthorizeExact(..., "evidence.commit")`，未覆盖 grant→403 ProtocolError 转扁平。
  - ③ **签名事件 notifier**（httpapi `CutEventNotifier` + fileCutEventSink）：append-only JSONL，每行 `{"event_id","cut_id","stage","job_version","ts","signature"}`，event_id=`cut_id@stage#version`（at-least-once 幂等重放）；signature=HMAC-SHA256(secret, `cut_id|stage|job_version|ts`) hex；**字段严格限白名单（仅通知，永不含 watermarks/outcome/receipts）**；trigger/cancel/rediagnose 各发一条；GET 保持权威；secret=="" 或 sink==nil → notifier 禁用（nop），secret 取 `GRAPH_MEMORY_CUT_EVENT_SECRET` env/flag，sink 追加 `<state>.cut-events.jsonl`。
  - ④ **组合启停可恢复**：Service + RoomFreezer 各自 Snapshot/Restore（§4.4 service 层先前已落地）；main.go 在 `<state>.cuts.json` 存 `{"cuts":<service snapshot bytes>,"freezer_last_sealed":{...}}`，重启 boot 时 loadCutState→Restore 两者；新增 `persistCutAfterMutation` wrapper 在 `persistAfterMutation` 之后外层包（detect POST 且路径含 `/consolidation-cuts`、`:cancel`、`:rediagnose` 后缀即走 isCutMutation）；`cut_state_bytes` 计入 boot 字段。
  - ⑤ **durable GuardAuthority**（httpapi `durableGuardAuthority`）：经 `manifestProvider` 构造回调（**只在无 service 锁下被调**，可 reentrant 读 GetManifest）；`ReceiptsComplete` 从 GetManifest→sealed segments→store committed batches 解析真实 receipts digest；`ReserveFacts/ReleaseFacts` 走 per-cut authority 侧 mutex+counter 序列化 fact mutation（响应 PG-40 R5'' note：ReserveFacts 与事实变更同一 authority 侧序列化协议）；`QuotaGranted/ProjectionComplete/ProposalRefs/ReplayPassed/CASBaseRevisionMatches/SideEffectsCompleted/DiagnosisVerified`（无 diagnosis service 时）→ **fail closed ErrGuardNotSatisfied**（worker 驱动残留诚实登记）；`NewDurableGuardAuthority` + `service.SetGuardAuthority(...)` 接入 main.go。
  - 测试：contract test 8 组 + 新增 `TestConsolidationCutCompositionSurvivesRestart`（service+freezer 快照→重建组合同 store→恢复→GET/GetManifest/幂等重放/阈值再冻结/Cancel 均复现）+ `TestConsolidationCutRoomInFlightConflict`（409 ROOM_EPOCH_STALE）。
  - **错误码登记**（手稿 §3.4/§4.5）：404 用 transport 惯例 `NOT_FOUND`（reason 枚举无 transport 404）；409 CUT_IDEMPOTENCY_CONFLICT（同 key 异 body）、409 ROOM_EPOCH_STALE（room 在航）；422 UNKNOWN_CORE_FIELD/MISSING_MANIFEST_FIELD/PAYLOAD_VALIDATION_FAILED；403 原 ProtocolError code 转扁平；room 无绑定→422 MISSING_MANIFEST_FIELD。
  - **已登记偏离/wrinkle**：① cut 路由**不进 `Routes()`**（`TestMemoryProtocolV1RoutesMatchOpenAPI` 对比冻结 `memory-protocol.yaml`；cut 面在独立 `consolidation-cuts.yaml`）——手稿 §5 写“append 4 routes”与 conformance 冲突，本批选择以 conformance 绿为准，分发全在 ServeHTTP switch；② OpenAPI `consolidation-cuts.yaml` 写 `/cancel`、`/rediagnose` 斜杠子路径，手稿 §3.1 明确规定 `:cancel`、`:rediagnose` 冒号（与全仓 `:stage`/`:commit` 惯例一致），实现按手稿冒号约定；③ 4 个历史 gofmt-dirty 文件未动（git 确认），`internal/consolidationcut/service.go` 本批 gofmt 干净。
  - **验证（本批）**：GMS `go test ./... -count=1` 全绿；cut 契约 10 测全过；GMS `-race` 通过（唯 `TestGoRunnerMatchesPythonRunner` 在全量并行 race 下偶发排序性失败、单独 `-race` 通过＝非本批回归）；GMS gofmt 仅余 4 个受保护既有脏文件；Host 集成 8 + spec conformance 1 全过。**AReaL 37 项 gate 本批环境受限**（aiohttp 仅在缺 pytest 的 venv、`/usr/bin/python3` 有 pytest 无 aiohttp；本批未改任何 AReaL 文件，非回归）。`cmd/server/` 为 git-ignored（改动在盘可编译但不入 git）。
  - **下一步**：写 kiro 第 1 轮 review prompt（复用会话 `sess_6d51df44-41cc-4db5-bfc7-28551baf0ae6` --model gpt-5.6-terra）→ 派发 → 转述 verdict → 修复 reject 直到 accept。


- [x] **PG-50A GMS consolidation-cut wiring round-1 REJECT fixes（2026-09-16）**：已修复并待 terra round-2 裁决。
  - OpenAPI `Error.code` 枚举扩展为 cut-domain reason codes 加实际 flat transport/authorization codes（`UNAUTHORIZED`、`INVALID_REQUEST`、`INVALID_JSON`、`TENANT_NOT_INITIALIZED`、`NOT_FOUND`、`GRANT_MISSING`、`GRANT_EXPIRED`、`SPACE_FORBIDDEN`），与 cut dispatcher 的扁平错误 surface 对齐；slash 与 colon action 路径同时可寻址，独立 `ConsolidationCutRoutes()` 持续对独立 OpenAPI conformance。
  - `Service.Restore` 采用 validate-before-install：拒绝空/截断形状、job/map-key 不一致、manifest/map-key 不一致、dangling idempotency target/request、room ledger 与 job tenant/room 不一致；新增 Snapshot→Restore round-trip 及三类坏镜像 fail-closed 回归。
  - notifier 补 HMAC 重算、字段白名单、稳定 `event_id` 的 at-least-once replay 语义与 sink failure structured-log 回归；sink append failure 发 `cut_event_sink_error`，但不改变 GET 权威性。
  - `persistCutAfterMutation` 明确登记并沿用既有 `persistAfterMutation` crash-recovery 模型：内存态完成响应后以原子 snapshot 更新恢复点，失败记录操作日志而不改写已发 response；更强 write-before-response 需主 store 与 cut composition 的全局提交协议，挂账后续 composition durability 工作。
  - 收尾补全（round-2 派发前）：完成上轮中断的 `snapshot_restore_test.go` 两类子测试（idempotency recorded-request 不匹配、job map-key≠CutID 拒绝），并修复 `Service.Restore` idempotency 校验循环中 `target` 未定义的编译残留；`isCutMutation` 补 `/cancel`、`/rediagnose` 斜杠后缀（双派发下经冻结 slash 路径的 cancel/rediagnose 同样触发 cut 状态持久化）。
  - 本轮未触碰受保护历史 gofmt-dirty 文件；完成 GMS 定向/全量、跨仓 conformance 及 format gate 后更新此项验证记录。
  - **round-2 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go test ./... -count=1` 全过；`go test -race -count=1 ./...` 全过（0 FAIL，含此前排序性 flaky 的 `TestGoRunnerMatchesPythonRunner`）；gofmt 仅余 4 个受保护既有脏文件（`skill_artifact.go` + GMS 3 个 skillevolution 副本；Host 侧 `cmd/bench-runner/main.go` 亦为预存未动）；spec conformance 1 项 + integration 8 项 pytest 桩全过；AReaL gates 环境受限不变（本批零 AReaL 文件改动）。

- [x] **PG-50A round-2 terra 复审判 reject（阻断 5→2），两项阻断已修复并重新全绿（2026-09-16）**：round-2 确认 ①②③⑤ 修复真实落地（双派发/独立 route conformance/MintCutID 补 source/扁平 ingress 错误/枚举闭合/per-space watermark/receipts 双向/原子写/严格 Restore 基础/notifier 回归），HTTP 组与 Error contract 组升 accept-with-notes、RoomFreezer 组升 accept；残余阻断两项及修复：
  - **阻断 A（Restore 孤立引用）**：`Service.Restore` 此前只对幂等可达 target 强校验，孤立 job/manifest 可恢复导致 GET job 成功而 GET manifest 缺失的权威态分裂。修复：job↔manifest 全量双向交叉验证**前置于**全部引用解析——每个 job 必须有同 CutID manifest 且 tenant/room/CutDigest 逐项一致、每个 manifest 必须有 job；`validSnapshotShape` 要求全部核心 map 非 nil（部分截断镜像不得伪装 fresh state）。新增 `TestServiceRestoreRejectsOrphanJobsAndManifests`（孤立 job/孤立 manifest/manifest 身份错配/digest 错配/缺核心 map，receiver 不变）。
  - **阻断 B（初始 freeze 绕开 durable authority）**：`Trigger` 曾以内部 `applyLocked` 直走 queued→freezing→frozen，生产 durable authority 的 quota/receipts fail-closed 对初始边无效。修复：Phase 3 在 freezing 态落账（job+manifest 对 authority 可见、room 保护不变）→ 解锁后 freezing→frozen **走与 `Transition` 完全相同的协议**（authority 解析 → ReserveFacts 保留 → 重解析 digest 一致 → 持锁 apply，零 authority 调用）；结构化 `receiptsCoverExactly` 先行 fail-closed；authority 拒绝 → job failed RECEIPT_MISSING + `trigger_frozen_rejected` 审计 + 幂等重放落在 failed job。**合同澄清（登记）**：`quota_budget_granted` 只 guard worker 驱动的 `Transition` 走 queued→freezing（调用方携带 QuotaGrantRef 证据）；create 驱动的初始 freeze 在 HTTP 边界经目的绑定 evidence.commit 授权准入，内部 hop 不查 quota。无 authority 的纯内存 service 模式保持本地 walk（与 Transition 无 authority fail-closed 同类，生产 main.go 恒注入 durable authority）。receipts digest 收敛单一来源：新增导出 `consolidationcut.ReceiptsDigest/ManifestReceiptsDigest`（Trigger 呈递 = durable adapter 从 committed evidence 重算 = tracer `receiptsDigestOf`，三方同一公式）。新增 `TestInitialFreezeConsultsGuardAuthorityAtFrozenGuard`（拒绝 → fail closed + 审计 + 重放语义；放行 → StageFrozen + AuthorityFactsDigest/ReceiptsDigest 证据已 stamp 且与 manifest 一致）。
  - 适配面：stub authority 测试经 `triggerForcedJob` 统一注册本次 freeze 的 receipts fact（stub=白名单 authority 语义不变）；integrationtracer authority 改用包级 canonical digest。round-2 的 non-blocking notes（slash 路径直测回归、validPathID 拒绝内嵌 `/`、cancel/rediagnose 目的绑定授权、TenantNotInitialized response 声明、多 space watermark 回归）挂账下一轮处置。
  - **round-3 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go test ./... -count=1` 全过；`-race -count=1 ./...` 全过；改动目录 gofmt 干净；spec conformance 1 + integration 8 pytest 桩全过。

- [x] **PG-50A round-3 terra 复审：阻断 1 判过时误报（代码已解决）、阻断 2（HTTP 错误映射）属实并修复（2026-09-16）**：
  - **阻断 1（Restore 双向验证）经核实为过时视图**：round-3 判决所述"仅幂等可达校验、shape 只查 Jobs"与本轮代码不符——job↔manifest 全量双向验证已在 idempotency/room-ledger 解析**之前**（service.go：孤立 job "has no manifest"、身份/digest 逐项比对、孤立 manifest "has no job"），`validSnapshotShape` 已要求全部核心 map 非 nil，`TestServiceRestoreRejectsOrphanJobsAndManifests` 五个子测试全绿。round-4 派发时以行号证据请 terra 复核当前文件。
  - **阻断 2（initial frozen-edge authority 拒绝被 HTTP 映射为 500）属实并修复**：`createConsolidationCut` 的 Trigger 错误映射新增 `errors.Is(err, ErrGuardNotSatisfied)` → **422 RECEIPT_MISSING**（"freeze receipts were not verified by the durable guard authority"）——确定性 receipt 拒绝非 infrastructure failure；GET 对已 failed job 保持权威。
  - **新增生产 composition HTTP 测试** `TestConsolidationCutDurableAuthorityRejectsUnverifiableReceipts`：真实 `NewDurableGuardAuthority` 注入 composition，说谎 freezer（`phantomSegmentFreezer`）附加结构自洽但 store 无 committed batches 的 phantom segment——本地结构校验通过、仅 durable authority 独立重算可拒；断言 422 RECEIPT_MISSING → `trigger_frozen_rejected` 审计 → GET failed job（stage=failed、failure_reasons=[RECEIPT_MISSING]）→ 同 key 重放 202 落在同一 failed job。
  - round-2/3 各组 non-blocking notes 继续挂账（slash 直测回归、validPathID 内嵌 `/`、cancel/rediagnose 授权、TenantNotInitialized response 声明、多 space watermark 回归、跨文件 crash-atomic commit、notifier consumer）。
  - **round-4 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go test ./... -count=1` 全过；`-race -count=1 ./...` 全过（0 FAIL）；gofmt 仅余受保护既有脏文件；spec conformance 1 + integration 8 pytest 桩全过。

- [x] **PG-50A round-4 terra 复审判 reject（两项实质语义缺口），已修复并重新全绿（2026-09-16）**：round-4 确认 HTTP 422 映射链、Trigger 走 Transition 协议、composition 注入 durable authority、审计/GET/幂等重放全部真实落地且无 round-3 non-blocking 项回归；残余两项及修复：
  - **阻断 1（Restore 完整语义校验，逐项补齐）**：在既有 job↔manifest 双向验证之上补 ①**manifest digest 内容重算**——`manifest.CutDigest != "sha256:"+manifestDigest(*manifest)` 即拒（篡改内容而同步伪造两处 digest 字符串无法通过）；②**Requests 反向校验**——`Requests` 必须是 `Idempotency` 的精确影子（`len(requests) != len(data.Requests)` → "recorded requests exist without idempotency bindings"，孤立 request 不再被静默丢弃）；③**room ledger 双向不变量**——in-flight job 必须恰占据其 room（"does not occupy its room"）、`RoomInFlight` 条目不得指向 terminal job（"points at terminal job"）、`RoomLastCut` 条目不得指向 in-flight job（"points at in-flight job"）；④**运行时失配修复**——`Resume`/`Rediagnose` 重入 in-flight stage 时重建自身 room 占据并清除失效 last_cut，且房间被**另一** cut 占据时以 `ErrRoomOccupied` 拒绝（SC-4.1）；新增 `TestServiceRestoreRejectsBrokenRoomSerialLedgers`（3 子测试）与 `TestResumeAndRediagnoseRejectRoomOccupiedByAnotherCut`（拒绝 + 清场后重建双链路）。
  - **阻断 2（durable authority 从 committed ledger 独立裁决 + 暂态/确定性分类）**：`receiptsCompleteForCut` 重写为**逐 SpaceScope 枚举 `CommittedEvidenceBatches(ctx, tenant, space, 0, MaxInt64)`**（过滤 `EvidenceBatchCommitted` + space 归属的账本查询）并以其为裁决权威——重复申报 batch、staged/未 committed/跨 space batch 均确定性拒绝（`ErrGuardNotSatisfied`）；每申报 batch 必须落在 sealed segment（未封口 segment 的 batch 不得逃逸聚合）；receipts 从**账本内容 digest** 重聚合并与呈递 digest 全等比对；ledger 读取错误**原样上抛**（不经 ErrGuardNotSatisfied 包装）。移除未参与裁决的 `batchLookupSource`/`Batch` 依赖，`NewDurableGuardAuthority(committed, manifests)` 收敛为 2 参。**分类协议**：service 新增 `ErrGuardAuthorityUnavailable`——`checkGuardAuthority.resolve` 将非 `ErrGuardNotSatisfied` 的 authority 错误包为暂态类；Trigger 初始 freeze 据此分类 failed 理由：`ErrGuardNotSatisfied` → RECEIPT_MISSING、暂态 → INFRASTRUCTURE_FAILURE（HTTP 422 vs 500 分流不变：暂态落入既有 500 INFRASTRUCTURE_FAILURE 兜底，不再被永久化为确定性 receipt 缺失）。新增 `TestInitialFreezeClassifiesTransientAuthorityFailure`（stub setErr → 暂态分类；未注册 digest → RECEIPT_MISSING）、`TestConsolidationCutTransientAuthorityFailureIs500AndRecovers`（flaky committed ledger → 500 INFRASTRUCTURE_FAILURE + failed job 理由正确 + ledger 恢复后新 key 冻结成功）。
  - **round-5 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go vet`（4 包）+ `go test ./... -count=1` 全过；`-race -count=1 ./...` 全过（0 FAIL，exit 0）；改动目录 gofmt 干净（仅余受保护既有脏文件）；spec conformance 1 + integration 8 pytest 桩全过。

- [x] **PG-50A round-5 terra 复审判 reject（两项各收敛为具体子缺口），已逐项修复并重新全绿（2026-09-16）**：round-5 确认生产 wiring（main.go durable authority）、committed-ledger 子集校验、暂态 ledger 分类、Restore 内容重算/孤立 request 拒绝/Resume+Rediagnose 重建全部真实落地；残余子缺口及修复：
  - **阻断 1a（`Transition → StageFailed` 遗留 `RoomInFlight`）**：Transition 的 ledger 收敛条件由 `terminalStages[req.To]` 改为 **`!inFlight(req.To)`**（service.go Phase C）——failed 与 terminal 一样释放 room 占据并落 last_cut，同 room 新 key Trigger 不再被死 cut 阻塞（与 Trigger 失败路径、Resume/Rediagnose 重建语义三方一致）；Restore 的占据校验由"不得指向 terminal"严格化为"**必须指向 in-flight job**"（"points at non-in-flight job (stage %s)"，failed 同样被拒）。回归：`TestFailedTransitionReleasesRoomForNextTrigger`（failed 后同 room 新 key 成功 + 快照恢复后无 stale occupancy、新触发成功）+ Restore 子测试 "room_in_flight points at failed job"。
  - **阻断 1b（job↔幂等/request 绑定缺反向全量）**：Restore 增加**反向精确绑定**——每个 job 必须恰为一个 idempotency binding 的 target（`bindingCount[cutID] != 1` → "want exactly 1"，成对剥离 binding 即拒）；recorded request 与 manifest **冻结字段交叉验证**（`Mode`/`TriggerSource`/`IdempotencyKey` 与 manifest 逐项全等，篡改 mode 即拒；tenant/room/source/key 经既有 MintCutID 校验钉死）。负测：`TestServiceRestoreRejectsBrokenIdempotencyBindings`（binding 对剥离 / mode 篡改两子测试，receiver 不变）。
  - **阻断 2（committed ledger 独立裁决收敛为全集一致 + 全局去重 + scope=binding + 暂态分类残余）**：`receiptsCompleteForCut` 再加固——①**集合双向相等**：每 scope 的 declared 集合除"⊂ committed"外，新增"committed ledger 全集必须被 manifest 完整申报"（"committed batch %s of space %s is omitted by the manifest"——说谎 freezer 遗漏 batch+segment 的自我一致伪造被独立权威识破）；②**全局去重**：`declaredGlobal map[BatchID]SpaceID` 跨 scope 索引 + scope 级 `scoped` 集合，重复 scope、同 batch 多 scope 申报均确定性拒绝；③**scope 集合 = room 部署绑定**：`NewDurableGuardAuthority(committed, manifests, roomBindings)` 三参（main.go/contract 测试同步），scope ⊄ binding 或 binding ⊄ scope 均拒；④**暂态分类闭合**：manifest provider 错误区分 `ErrCutNotFound`（确定性事实反驳 → ErrGuardNotSatisfied）与其他读取失败（**原样上抛** → service ErrGuardAuthorityUnavailable → HTTP 500）；service 侧 `ReserveFacts` 失败由 ErrGuardNotSatisfied 改为 **ErrGuardAuthorityUnavailable**（reservation 是可用性故障而非证据反驳，不再被永久化为 RECEIPT_MISSING）。
  - 新测试：contract `TestConsolidationCutDurableAuthorityRejectsIncompleteManifests` 六子测试（诚实两 space 冻结通过 / 遗漏 committed batch / 重复 scope / 未绑定 scope / 缺 scope / 跨 scope 重复 batch，经 `lyingFreezer` 注入自洽伪造）；in-package `TestDurableGuardAuthorityClassifiesManifestProviderFailures`（provider 读取失败 = 暂态、ErrCutNotFound = 确定性）；`TestGuardReservationAcquisitionFailsClosed` 断言更新为暂态类。**非阻断建议同步落地**：共享 `cutContractComposition` 显式接线生产 durable authority（常规 contract 触发一律经 committed-ledger receipts 证明准入），全量 suite 保持绿。
  - **round-6 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go vet`（4 包）+ `go test ./... -count=1` 全过；`-race -count=1 ./...` 全过；改动目录 gofmt 干净（仅余受保护既有脏文件）；spec conformance 1 + integration 8 pytest 桩全过。

- [x] **PG-50A round-6 terra 复审：阻断 1a/1b 双双 accept；唯一残余阻断为 zero-sealed 提前成功绕过，已修复（2026-09-16）**：round-6 确认 failed 释放 room、Restore 双向占据/绑定校验、committed 全集相等、全局去重、scope=binding、暂态分类全部落地；残余与修复：
  - **阻断（zero-sealed 提前成功绕过）**：`receiptsCompleteForCut` 曾在 `len(SealedSegmentIDs)==0` 时提前 `return true`——先于 binding/scope 全集、committed 全集、去重、receipt 重推导等全部校验，持有已 committed 证据的 room 可被不可信 freezer 以空 manifest（sealed/receipts/scopes 全空）骗入 frozen。修复：**整体移除提前成功分支**，空 sealed 集合同样走共享校验链（scope=binding → declared==committed 双向 → 未封口 segment 检查 → receipt 重推导与 digest 全等）——空 manifest 只有在**每个绑定 space 的 committed 集合确实为空**（真实 no-change）时才会通过全部检查并被空 receipts digest 全等放行；threshold 模式 zero-delta 不经此路径（Trigger 的 completed/no_change 分支先行分流）。附带说明注释钉死"无提前成功"约束。
  - 回归：contract `TestConsolidationCutDurableAuthorityValidatesZeroSealedManifests` 两子测试——"empty manifest cannot hide committed evidence"（`lyingFreezer` 清空 sealed/receipts/scopes → 422 RECEIPT_MISSING → `trigger_frozen_rejected` 审计 → GET failed → 同 key 幂等重放原 failed job）；"honest no-evidence freeze freezes through"（`cutBootstrapShell` 仅建 tenant/space/grant、零证据 → force 冻结 202 frozen，正例防误伤）。
  - **额外加固项（kiro 非阻断建议）同步落地**：Restore 校验 `FailedFrom` 必须为空或 in-flight stage（"failed-from stage %q is not resumable"，防篡改后 Resume 写入非法 stage）；`Resume` 运行时对非 in-flight target 增加防御性 `ErrNotResumable`（审计后拒绝，深度防御）。Restore 子测试 "failed-from stage not resumable"。
  - **round-7 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go vet`（4 包）+ `go test ./... -count=1` 全过；`-race -count=1 ./...` 全过；gofmt 干净（仅余受保护既有脏文件）；spec conformance 1 + integration 8 pytest 桩全过。

- [x] **PG-50A round-7 terra 复审：round-6 zero-sealed 阻断判解除；新发现两项阻断（FailedFrom 谓词过宽、receipt provenance 未绑定），已修复（2026-09-16）**：
  - **round-6 阻断解除确认**：zero-sealed 提前成功分支移除生效——空 manifest 走完整 authority 链（provider 分类 → scope=binding → declared==committed 双向 → 未封口检查 → receipt 重推导与 digest 全等）；清空伪造被确定性 422、诚实零证据正例 202 frozen、threshold zero-delta 不经 guard——`TestConsolidationCutDurableAuthorityValidatesZeroSealedManifests` 判真实覆盖。
  - **新阻断 A（FailedFrom 谓词宽于合法失败来源）**：`inFlight` 含 queued/frozen/cancelling，它们 in-flight 但**不可达为失败来源**——篡改合法 failed 快照的 `FailedFrom=frozen` 可通过 Restore+Resume 直达 frozen。修复：新增**领域精确谓词 `resumableFailedFrom(stage)`**（= 转移表 failed 出边源 ∪ {freezing 内部 failLocked}：freezing/diagnosing/consolidating_partial/consolidating/replaying/activating），Restore 与 Resume 共同使用（替换 `inFlight`）。回归：`TestServiceRestoreRejectsIllegalFailedFromStages`（FailedFrom=frozen/queued/cancelling 三例拒绝、receiver 不变）+ `TestFailedSnapshotRestoreResumeRestoresExactFailureOrigin`（合法 diagnosing→failed → 快照 → 恢复 → Resume **精确回到 diagnosing**）。
  - **新阻断 B（receipt batch provenance 未绑定）**：canonical `ReceiptsDigest` 只聚合 `BatchDigest`——交换两个 receipt 的 `BatchIDs` 或以伪 batch ID 替换（保留 digest）后 aggregate 仍匹配，持久化 provenance 失证。修复：durable authority 在 aggregate 比较前新增**逐段精确绑定**——manifest receipts 按 SegmentID 索引（重复段收据拒、数量≠sealed 数拒），对每条 authority 重推导收据要求 manifest 收据 SegmentID 存在、`BatchDigest` 全等、`BatchIDs` 集合精确相等（新 helper `equalBatchIDSets`，顺序不敏感）。回归：contract `TestConsolidationCutDurableAuthorityBindsReceiptProvenance` 两子测试——"swapped receipt batch lists"（双 segment 交换 BatchIDs → 422 → 审计 → GET failed → 幂等重放原 failed job）/"phantom batch IDs with real digests"（伪 ID 保 digest → 422 RECEIPT_MISSING）。
  - **round-8 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go vet`（4 包）+ `go test ./... -count=1` 全过；`-race -count=1 ./...` 全过；gofmt 干净（仅余受保护既有脏文件）；spec conformance 1 + integration 8 pytest 桩全过。

- [x] **PG-50A round-8 terra 复审：阻断 B（receipt provenance）判 accept；阻断 A 收敛为唯一空值绕过（FailedFrom=""），已修复（2026-09-16）**：
  - **阻断 B 解除确认**：逐段精确绑定（SegmentID 索引 + 重复段收据拒 + 数量一致 + BatchDigest 全等 + `equalBatchIDSets` 集合全等）判真实落地；`TestConsolidationCutDurableAuthorityBindsReceiptProvenance` 判真实使用生产 RoomFreezer/memory ledger/durable authority（非 stub）。**non-blocking 精度建议同步补齐**：phantom-batch-IDs 子测试补 GET failed + 幂等重放断言。
  - **阻断 A 残余（空值绕过）**：`Restore` 只查非空 FailedFrom、`Resume` 对空 FailedFrom 回退 diagnosing——篡改 failed 快照清空 FailedFrom 可恢复并默认重入 diagnosing（如 freezing→failed 的 job 被洗成 diagnosing 来源）。修复：①Restore 对 `Stage == StageFailed` 的 job 一律要求 `resumableFailedFrom(FailedFrom)`（空值同拒，"failed job %q does not record a resumable failed-from stage"）；②Resume **删除空值回退**——`!resumableFailedFrom(job.FailedFrom)` 直接审计 + ErrNotResumable（运行时 failed job 恒有 failLocked/Transition stamp 的 FailedFrom，回退只会掩盖损坏）。
  - 回归 `TestServiceRestoreRejectsFailedJobWithoutFailedFrom` 三子测试：internal frozen-guard 失败链（FailedFrom=freezing）清空即拒 / diagnosing 转移失败链清空即拒（均断言 receiver 不变）/ 运行时防线（Restore 已拒该状态故经包内直改 stored job 的 FailedFrom 注入，Resume → ErrNotResumable 而非默认 diagnosing）。既有 "room_in_flight points at failed job" 子测试同步为 FailedFrom 给合理来源（该子测试专测占据不变量，不被新检查抢先拒绝）。
  - **round-9 派发前全量验证（2026-09-16，全绿）**：GMS `go build ./...` + `go vet`（4 包）+ `go test ./... -count=1` 全过；`-race -count=1 ./...` 全过（exit 0）；gofmt 干净（仅余受保护既有脏文件）；spec conformance 1 + integration 8 pytest 桩全过。

- [x] **PG-50A round-9 terra 复审：Verdict accept（kiro 复核循环收敛，2026-09-16）**：
  - **空 FailedFrom 绕过解除确认**：`resumableFailedFrom` 允许集与转移表全部 failed 写入路径一致（Transition 失败出边 + failLocked 内部 freezing，无其他生产写入点）；Restore 非空必须属于允许集 + failed job 空值同拒；Resume 无回退、空/非法来源审计后 ErrNotResumable。五条回归（frozen-guard 内部失败链 / diagnosing 转移失败链 / 运行时直注空 FailedFrom 拒绝 / failed 占据子测试的 FailedFrom 合理化 / 合法链精确恢复 diagnosing）判真实覆盖。
  - **receipt provenance 绑定与 phantom 生命周期**：逐段绑定（SegmentID 索引/重复拒/数量一致/digest 全等/batch 集合全等）判保持有效；phantom 子测试以生产 composition 覆盖 422 RECEIPT_MISSING → trigger_frozen_rejected 审计 → GET failed → 同 key 幂等重放完整生命周期，与 swapped 子测试同精度。
  - **遗留 non-blocking 建议（可选，不影响验收）**：phantom 子测试可显式断言 audit 找到非空 failedID、GET 的 failure_reasons 含 RECEIPT_MISSING（当前经 GET/replay 间接覆盖）。既有已登记债务不变：response 后独立持久化（非跨文件 crash-atomic）；slash action 缺独立 direct-route contract test。
  - **round-9 复审自跑验证**：定向三包 `go test -count=1` + `-race` + `go build ./...` 全过（kiro 自行复跑确认）。派发前全量 gate 同为全绿（build/vet/全量/-race/gofmt/spec conformance 1 + integration 8）。
  - **PG-50A 批次收尾**：GMS consolidation-cut composition wiring（batch 4）经 9 轮 review 循环（round-1~8 reject、round-9 accept），实现 + 回归测试 + §10 记录全部落地。
