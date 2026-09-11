# graph-memory-service Module Specification

```yaml
document_status: normative
schema_version: rsih-skill-evolution.graph-memory-service.v1
system_contract: ./system-contract.md
system_contract_schema_version: rsih-skill-evolution.system-contract.v1
host_spec: ./pi-group-chat-host.md
host_spec_schema_version: rsih-skill-evolution.pi-group-chat-host.v1
language: zh-CN
```

> 本文是 `graph-memory-service`（以下简称 GMS）在 RSI-Harness Skill Evolution 系统中的规范性模块规范。本文服从 [`system-contract.md`](./system-contract.md)（以下简称 Contract）和 [`pi-group-chat-host.md`](./pi-group-chat-host.md)（以下简称 Host Spec）。共享 DTO、跨模块状态机和全局不变量只由 Contract 定义；若本文与 Contract 冲突，以 Contract 为准。Host 行为由 Host Spec 约束；本文只冻结 GMS 对 Host 开放 slot 的绑定及 GMS 自身权威行为。

## 1. GMS authority boundary

### 1.1 规范范围与共享 DTO 注册表

实现声称符合本文时，MUST 同时符合 Contract 中所有适用于 GMS 的 MUST/MUST NOT，尤其是 §3.2、§3.4、§5–§16；Contract §15、§16 的适用验收条件不可省略。本文不得复制或重新定义 Contract §7 的共享 JSON schema；下表是共享 DTO 的唯一来源表，字段、值域、必填性、exact-ref 语义与扩展规则均以所引章节为准。

| 共享 DTO | 唯一规范来源 |
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
| `SimilarityAssessment` | Contract §7.21 |
| `MergeProposal` | Contract §7.22 |
| `MergeProposalEvent` | Contract §7.23 |

`VersionedRef`、`Digest`、`Rational`、`JsonSchemaRef` 的唯一来源是 Contract §7.2；canonicalization、exact identity 和 extension 规则的唯一来源是 Contract §6。本文定义的 GMS 私有 record 和 tool-specific `arguments` payload 不是新的共享 DTO，不得被其他模块冒充为 Contract §7 对象。

### 1.2 GMS 唯一权威

GMS MUST 唯一拥有：

1. evidence staging、commit、assessment 与 immutable evidence records；
2. canonical Skill body、candidate、released artifact、lineage、version assignment 与 active head；
3. proposal admission、protected canonicalization、validation、replay-result canonicalization、hard gates、utility comparison与 `ReleaseDecision` ledger；
4. activation/deactivation ledger、outbox、source-head/active-head CAS；
5. `SimilarityAssessment`、`MergeProposal`、proposal-group winner 与 merge event ledger；
6. Runtime/Curation projection stream/head/cursors；v1 只物化 Runtime Graph；
7. deterministic lexical + graph retrieval、Guidance View 与 read-only exact artifact retrieval；
8. 本文 §10 的 tool-specific payload binding，以及 §11 的逻辑 API 与 GMS reason-code policy。

### 1.3 明确排除与信任边界

GMS MUST NOT 拥有 Room、Agent、Delivery、Interaction DAG、Segment、Decision Checkpoint、Host seal 或 Pi tool return channel；不得执行 Pi、HarnessRunner 或 Composite child。GMS MUST NOT 让 Graph 成为 artifact、evidence、assessment、activation 或 active-head 权威。模型、Agent、Genome、artifact text、tool arguments、replay output 与外部资源均是不受信数据；它们 MAY Diagnose/Propose，但 MUST NOT Validate/Execute/Select/Export、写 ledger、推进 protected state、分配版本或激活。

Host seal 证明来源和顺序，不自动等同于 committed `EvidenceRef`。RSIH bundle、manifest、`SKILL.md` 和 `skill.lock.json` 由 RSIH 拥有；GMS 只提供 active exact artifact 与 closure 输入。Graph node ID、裸名称和 `latest` MUST NOT 用于执行、release、materialization 或 exact lookup。

### 1.4 适用的不变量与 v1 非目标

本文把与 GMS 相关的 Q1–Q37、M1–M9、U1/U2、C1–C8 作为实现约束，而不是说明性追踪：三种 kind 与 lineage immutability、Composite exact orchestration、exact revision/branch 节点、九关系封闭集、Runtime/Curation 分离、Candidate/Released 分离、JCS/SHA-256、typed ports、closed failures、九 slices、recorded causal replay、hard gates、Q32 projector、budgets、deterministic ranking、content-addressed closure 输入、binary Step Guidance merge、constrained Pareto、历史节点与 typed reference vertex 均为 MUST。

v1 MUST NOT 实现或伪装支持：可达 probation 生命周期、自动 retirement/deprefer、Curation Graph 物化、OPD/reverse-KL、Data-RSI/Model-RSI/RSI²、production embedding、learned similarity/ranker、Procedure/Composite/cross-kind/n-way merge、merge tournament、stale 自动 rebase、在线 canary、任意循环 Composite。保留枚举或接口空间不构成能力声明。

## 2. Storage and immutable ledgers

### 2.1 存储总则

所有权威写 MUST append-only；更正通过新 record、state event 或 compensating event 表达。每个 record MUST 有不可复用 identity、schema version、canonical body digest、创建 authority、exact policy refs 和必要的 source refs。历史 schema、policy、artifact、assessment、event MUST 可按 exact ref 解析，禁止原地改写或解析为“最新”。

外部 mutation MUST 带 idempotency key：相同 key 与相同 canonical request digest返回原结果；相同 key 与不同 digest返回 `IDEMPOTENCY_CONFLICT`。所有 CAS MUST 比较 exact expected head/ref/sequence，不得只比较 lineage 名称或时间戳。

### 2.2 Evidence staging 与 commit

`staged evidence` 是 GMS 私有、不可被 Runtime Graph 引用的 intake 状态。私有 `EvidenceStagingRecord` MUST 固定 intake identity、Host seal/Segment exact refs、payload digest、proposed evidence kind、scope、submitter、schema/policy refs、idempotency key 与状态事件。状态至少表达：

```text
received → validating → committed
                    ↘ rejected
                    ↘ inconclusive
```

只有 protected evidence committer 验证 Host `SegmentRef`、seal digest、scope、provenance、payload canonicalization 与 kind 后，MAY 铸造 Contract §7.7 `EvidenceRef`，其 `commit_state` 只能是 `committed|sealed`。staged、rejected、inconclusive 或缺失 seal 的输入 MUST NOT 铸造 `EvidenceRef`，也不得进入 `supported_by/refuted_by`。Commit transaction MUST 原子写 evidence body、exact ref、source binding、commit event 与 idempotency result。后续 assessment 引用 immutable `EvidenceRef`，不能引用 staging ID。

Evidence retention MUST 至少覆盖所有 artifact branch、assessment、proposal、replay、release、merge、Graph edge 与 audit 的引用期。若 retention policy 允许 tombstone，大正文可受控归档，但 exact ref、digest、provenance 和可验证读取 MUST 保留；不可解析的被引用 evidence 不符合本文。

### 2.3 Artifact store、lineage 与 branch records

Canonical artifact store MUST content-address body canonical bytes，并将 envelope audit metadata 与 hashed core 分离。Released lineage registry MUST append version-assignment records；同 lineage version 是正整数且单调，不可复用。私有 lineage head index是由 version ledger重建的 CAS 加速索引，不得覆盖历史。

Branch identity MUST 绑定 exact artifact revision与 artifact body内 `branch_id`；branch record保存可重建元数据、body JSON Pointer、branch digest、provenance refs，不复制独立可变 guidance。Procedure、Step Guidance、Composite body均按 Contract §8验证。Candidate store和released store可共享content-addressed bytes，但身份空间与可见性 MUST 分离。

### 2.4 Proposal、candidate 与 validation ledgers

普通 proposal ledger MUST 保存 Contract §7.9 exact proposal、admission events、dedup key、expected parent/source heads、policy packet与终态。Candidate ledger MUST 保存 Contract §7.4 exact ref、canonical body digest、origin exact ref、immutable provenance coverage和 candidate binding event。Candidate body不可原地修改；新正文必须创建新 candidate/proposal attempt。

Validation ledger MUST 保存 exact validator/profile/schema refs、input packet digest、每个 hard gate的closed result、record refs、lease/CAS sequence与validation digest。Lease仅协调执行，不授权修改输入；同 input/profile重复验证必须确定地产生相同语义结果。

### 2.5 Replay、evaluation 与 release ledgers

Replay request/result ledger MUST 保存 Contract §7.10/§7.11 对象、Host dispatch correlation、RSIH adapter output refs、fixture/baseline mapping与idempotency result。GMS只接受 Host按 Host Spec §6调度、RSIH deterministic adapter执行的输出；GMS protected canonicalizer拥有权威 `ReplayResult` identity和ledger write。

Evaluation ledger MUST 保存 exact release-rule与utility-comparator refs、hard-gate packet、U1 comparison trace、overflow checks、source-head expectations和 Contract §7.12 `ReleaseDecision`。Accepted decision本身不等于release；只有 §6原子事务成功才形成 released/active revision。

### 2.6 Activation ledger、active heads 与 outbox

Activation ledger只追加 Contract §7.13 `ActivationEvent`；event type 值域与 lifecycle 的唯一来源分别是 Contract §7.13、§9.2，本文不增加或重列共享值。全局 activation sequence在单一运行 stream中正整数单调且无复用。Active-head table是每 lineage 的权威当前指针，必须由activation transaction以exact expected head CAS更新；ledger仍是完整历史来源。

Outbox record MUST 与成功 activation/deactivation同事务写入，固定 `outbox_key`、activation sequence/event digest、projection target和delivery state。Outbox delivery可重试但不得生成新 ActivationEvent。相同 outbox key不同 digest必须冲突。

### 2.7 Similarity、merge 与 assessment ledgers

Similarity ledger保存 Contract §7.21 immutable assessment及 exact feature/assessor/policy/evidence refs。Merge ledger保存 Contract §7.22 proposal和Contract §7.23逐状态事件。私有 group-winner index以 `proposal_group_key` 做CAS，只允许一个 admitted/in-flight winner；它必须可从append-only admission events重建。

额外 evidence在 `candidate_bound` 前仅可通过append-only attachment event加入frozen packet的下一版本；之后必须新建proposal attempt。终态不可重开。Source演进不改写旧 `derived_from`、assessment或proposal。

### 2.8 Projection heads、cursor vector 与索引

Runtime与Curation MUST 有独立 projection stream、schema version和可变head；v1 Curation保持未物化。每个projection head的私有 cursor vector至少包含：

- `activation_cursor`：连续activation sequence；
- `evidence_assessment_cursor`：连续evidence/claim assessment sequence；
- `similarity_assessment_cursor`：连续similarity assessment sequence；
- 适用时 `anchor_cursor` 与 artifact-release source cursor。

公共 Contract §7.14 `ProjectionWatermark.projected_through_activation_sequence` 表示运行可见性的activation下界；不得新增共享字段。`source_ledger_digest`与`watermark_digest` MUST 对完整、规范排序的私有cursor vector、projection schema/head和相应ledger prefix digests作承诺。Graph索引、lexical index、lineage traversal、active visibility cache均为派生数据，可从权威ledgers重建。

### 2.9 原子边界、保留与恢复

以下 MUST 原子：evidence commit；candidate binding + proposal event；release version assignment + Candidate→Released mapping + active-head CAS + ActivationEvent + outbox；deactivation head-clear + event + outbox；merge source-head CAS + new lineage/version + mapping + activation/outbox + proposal event；Graph mutation + projection cursor/head/watermark CAS。

Graph projection MUST NOT 加入release事务。任何部分失败不得暴露部分version、head、event或proposal终态。恢复只可重放相同idempotency输入或追加compensating event；不得删除历史。权威记录的retention MUST 不短于所有exact引用与合规审计期。

## 3. Canonical Skill artifact service

### 3.1 Canonical bytes、identity 与 exact resolution

GMS protected canonicalizer MUST 对Contract §8 artifact body使用UTF-8无BOM、RFC 8785 JCS和SHA-256 lowercase digest。Hashed core中只允许整数JSON number；比例使用Rational或定标整数。语义数组保持顺序；schema声明为集合的输入在进入core前按规范键排序。未知core字段拒绝；扩展只允许在`extensions`中，未知required extension fail closed，extension不得改变身份、权限、权威、release或core语义。

`SkillArtifactRef` exact identity为 `(lineage_id, version, artifact_digest)`；`CandidateArtifactRef`为 `(candidate_id, body_digest)`。所有字段同时校验；Graph node ID、裸名称、`latest`或字段不一致均fail closed。CandidateRef不得携带released lineage version，也不得作为Runtime执行输入。

### 3.2 Kind 与 lineage

Canonical kind值域的唯一来源是Contract §5.2与§8；GMS MUST 按该值域验证，不得增加、删除或重解释kind。创建lineage时固定kind；同lineage所有revision MUST 同kind。跨kind转换只能以Contract定义的`derive_lineage`语义创建新lineage，并记录protected derivation provenance。任何试图原地改kind的proposal必须拒绝。

### 3.3 Procedure validation

Procedure MUST 满足Contract §8.2：至少一个唯一`step_id`；instruction非空；pre/postconditions合法；failure action来自closed set。Procedure MUST NOT 注入伪造的checkpoint-derived `causal_context`或`future_path_summary`。若作为Composite child，named input/output JSON-Schema ports均为必填exact `JsonSchemaRef`且可解析。

### 3.4 Step Guidance validation 与 branch provenance

Step Guidance MUST 同时具有因果上下文、决策branches和未来路径摘要。Branch ID在revision内唯一；predicate、guidance、expected outcome、closed failure action与evidence refs按Contract §8.3验证。每个branch的provenance MUST 能追溯到committed/sealed evidence、Host checkpoint/anchor或protected assessment；模型自由文本不能成为provenance。

已发现success、failure、recovery branches MUST 可共存，不得因成功路径覆盖失败/恢复。Future critical steps保存在artifact body，不建立独立Graph node。Merge candidate还必须保留双端branch identity映射、冲突resolution和evidence coverage。

### 3.5 Composite validation

Composite只保存exact children、DAG control/data flow、ports、bounded retries、failure handlers和orchestration permissions，不复制child guidance，不伪造顶层Step Guidance。Validator MUST：

1. 解析每个exact child为released且当前active revision；
2. 验证named input/output ports及exact JSON schemas；
3. 验证mapping type compatibility；
4. 检测children/edges DAG无环；循环只能由`max_attempts`有界retry表达；
5. 验证failure action closed，fallback child已声明且依赖可满足；
6. 计算children permissions并集 + orchestration permissions，再受Host authority cap限制；
7. 固定whole-Composite replay packet。

任何child ref变化都必须创建新Composite revision，重新static validation与whole-Composite paired replay。Ablation可附加但不得替代whole replay。

### 3.6 Permission、dependency 与 extension gates

Artifact permission不得超过提案授权、evidence/policy允许范围和Host cap。Merge candidate权限不得超过sources合法并集与Host cap。`depends_on`必须由artifact显式声明且transitive closure无环；不得由模型或Graph相似性推断。Required extension schema/ref必须exact可解析；未知、验证失败或尝试覆盖core字段均拒绝。

### 3.7 Candidate 与 Released body equality

Canonicalizer一旦绑定Candidate，body canonical bytes和`body_digest`不可变。Release transaction分配lineage/version时，released artifact body MUST 与candidate body逐字节相同，`body_digest_equal=true`，并再次计算/比较digest。Envelope audit metadata可不同但不得进入body digest。任何差异返回`CANDIDATE_RELEASED_BODY_MISMATCH`且不得分配version、写activation或更新head。

## 4. Proposal and candidate pipeline

### 4.1 Diagnose/Propose 输入与私有模型输出 envelope

GMS MAY 接收Host settled `SegmentRef`/`CheckpointRef`、committed `EvidenceRef`、parent exact refs、operation、policy refs及model/human suggestion。GMS私有 `ModelSuggestionEnvelope` 只固定suggestion identity、initiator/request、input exact refs、proposed kind/body、model output digest、generation policy和安全审计；它不是`SkillProposal`、Candidate、validation或decision，不得投影Runtime。

模型输出中的schema、digest、authority、permission、policy、evidence claim均不可信。Protected admission/canonicalizer MUST 独立解析并决定是否接纳；禁止模型调用validator、evaluator、activation或ledger mutation。

### 4.2 SkillProposal admission

权威proposal使用Contract §7.9。Admission MUST 检查：source Segments均settled且seals可解析；evidence committed/sealed；checkpoint属于对应segment；parent refs exact；operation与kind/lineage一致；policy refs可解析；initiator有权；dedup/idempotency无冲突；permission未越界。失败写append-only rejection event，不创建Candidate。

### 4.3 Protected canonicalizer 与 Candidate binding

Protected canonicalizer将admitted proposal的suggested body按§3规范化、验证closed core/extension和provenance coverage，随后唯一地铸造Contract §7.4 `CandidateArtifactRef`。Candidate binding transaction MUST 原子写canonical bytes、CandidateRef、origin exact ref、proposal `candidate_bound` event、dedup result。Candidate不可执行、不可Explore、不可materialize。

### 4.4 普通 proposal 状态机绑定

普通 proposal 的状态集合、终态、合法 transition、withdraw 时点与每个 transition 的 required records，唯一规范来源是 Contract §9.1；本文不重列或扩展该状态机。GMS MUST 在 append state event 前校验 current exact state、Contract transition、§4.2 admission packet、§4.3 candidate binding、§5 validation/replay/evaluation records 与 §6 activation transaction 条件。任一 guard 不满足必须使用 Contract 状态机允许的 closed terminal/continuation path，不得跳过状态、重开终态或把 lease 当作 transition authority。任何需要新正文、fresh expectation 或 protected validation 后撤回再尝试的情形，MUST 创建新的 proposal/candidate attempt。

### 4.5 Dedup、idempotency 与 stale

私有proposal dedup key MUST 对规范排序的source refs、operation、kind、normalized applicability、body-intent digest和policy refs计算。Exact重复返回canonical proposal结果；相同idempotency key不同payload拒绝。Dedup不得把不同exact parent revision折叠。

Frozen expected head、source seal或policy不再满足时记录`stale`；stale不否定历史exact refs，也不得自动rebase。恢复需要新proposal、assessment或candidate attempt。

## 5. Replay and evaluator adapters

### 5.1 组件边界

GMS冻结Contract §7.10 `ReplayRequest`并拥有evaluation；Host按Host Spec §6拥有调度与sealed-input关联；RSIH HarnessRunner使用exact adapter/profile执行deterministic fake runtime。模型不得参与fixture选择、执行评分、hard gate或winner选择。

### 5.2 Sealed fixtures 与 paired replay

ReplayRequest MUST 固定CandidateRef、baseline exact refs、fixture sets、settled SegmentRefs、profile/adapter、causal mode和idempotency key；merge还必须固定两个`required_source_heads`。Fixtures来自Host Evidence/Path Seals，覆盖已发现success/failure/recovery。普通revision按同一fixture执行baseline/candidate；merge执行A-only、B-only、overlap/conflict families。

Paired runs MUST 具有相同initial state、fixture order、permissions、environment、deterministic seeds和adapter version，隔离workspace/cache/side effects。生产live结果不得替代sealed replay。重跑相同packet必须得到相同output digests；否则结果`inconclusive`或拒绝。

### 5.3 ReplayResult canonicalization

GMS验证Host correlation与RSIH outputs，按Contract §7.11铸造权威`ReplayResult`。每个fixture outcome必须有exact fixture/baseline、domain、counts、critical regressions和output digest；utility vector五维均为整数。缺family、digest mismatch、不确定性或adapter nondeterminism不得伪装成功。

### 5.4 Hard gates

Release前 MUST 全部通过：schema/canonicalization/digest、exact refs、required extensions、authority/provenance、committed evidence、permission/Host cap、kind/lineage、ports/DAG/retry/failure、fixture completeness、source-head freshness、zero safety violations、zero critical regressions，以及merge conflict closure。Hard gate失败不能被utility覆盖。

### 5.5 U1 constrained Pareto 可实现算法

Evaluator MUST 使用exact `release_rule_ref`和`utility_comparator_ref`冻结fixture mapping、主要维度方向、cost budget和ratio definitions。参考算法：

```text
input: candidate C, frozen fixtures F, baseline map B(f), comparator P
assert all_hard_gates_passed
assert every required fixture family is present
for each critical slice s in source A (and source B for merge):
    require critical_regression_count(C, s, B(s)) == 0

E = reference_envelope(F, B)
// A fixtures use A; B fixtures use B; overlap uses policy-selected best applicable source.
Cvec = aggregate_integer_vector(C, F)
Evec = aggregate_integer_vector(E, F)

primary = [task_success_count max,
           critical_branch_pass_count max,
           recovery_success_count max,
           inconclusive_case_count min]
for d in primary:
    require no_worse(Cvec[d], Evec[d], direction(d), P)
strict = exists d in primary where strictly_better(Cvec[d], Evec[d], direction(d), P)
require strict
require Cvec.execution_cost_units <= P.cost_budget_units
// cost下降不计入strict，且不能单独触发release。
accept
```

若policy比较比例 `(a_num/a_den)` 与 `(b_num/b_den)`，MUST 使用 `a_num*b_den` 和 `b_num*a_den` 交叉乘法。实现 MUST 使用足够宽的整数或checked multiplication；任何overflow、denominator非法、聚合溢出或无法证明比较正确均fail closed为`UTILITY_ARITHMETIC_OVERFLOW`/`inconclusive`，禁止浮点、饱和或截断。

“全部不差”只覆盖四个主要维度；cost仅受versioned budget约束。Reference envelope对每个fixture按Contract §10.4固定，不允许evaluation时改baseline。Merge必须A/B critical slices均零回归且相对envelope至少一个主要维度严格改善。

### 5.6 ReleaseDecision

Evaluator输出Contract §7.12 `ReleaseDecision`，记录validation/replay refs、exact rules、每个hard gate、outcome/reasons和source-head expectations。`accepted`只授权进入activation pending，不等于active。输入不足→`inconclusive`；明确不满足→`rejected`。Candidate或生成模型不得自评。

## 6. Activation and active heads

### 6.1 Version assignment 与 release transaction

Version只在accepted decision进入release transaction后分配。普通revision对目标lineage读取expected active head并CAS；新lineage expected head为空。事务 MUST 再验证Candidate/Released body digest equality、decision exactness、source/parent freshness、kind不变与permission。

### 6.2 原子 activate

成功activate transaction MUST 原子完成：

1. 分配不可复用lineage/version并写released artifact mapping；
2. 写Candidate→Released mapping和`body_digest_equal=true`证明；
3. CAS active head；
4. 追加Contract §7.13 `ActivationEvent`和全局sequence；
5. 写outbox；
6. 推进proposal/merge到`released`。

任一步失败不得留下部分version、mapping、head或event。Graph不在该事务中。

### 6.3 Supersede、deactivate 与 reactivate

同lineage激活新revision时event包含previous active ref，projector产生`new --supersedes--> old`；旧artifact保留。Deactivate必须有protected authorization、当前head CAS并追加deactivate event/outbox，随后clear head；不得删除artifact、event、edge或历史Graph node。重新激活历史released ref需要新的protected authorization/release decision和新activate event，禁止改旧event。

### 6.4 U2 与 probation

v1 runtime reachable transitions与event type只取Contract §9.2、§7.13定义的集合；本文不复列或增加值。`probation`是保留、不可达状态；实现不得生成probation event、将candidate作为probation运行，或用probation绕过release。默认运行读取只返回current active；历史exact读取按授权返回superseded/deactivated并明确状态。

### 6.5 Source-head freshness

普通revision必须验证expected target head；merge在activation前必须对A/B两个exact source heads同时CAS确认仍为current active、released、非probation Step Guidance。任一变化使proposal `stale`，不得部分激活或自动rebase。Merge新lineage激活不修改A/B heads。

## 7. MergeProposal subsystem

### 7.1 范围、发起与 similarity bands

v1只允许两个当前active、非probation、released `step_guidance` exact revisions合并。Contract §7.21 `SimilarityAssessment`是band值域与字段语义的唯一来源，并使用exact versioned policy、整数`score_micros`和规范排序source pair。下列不是新enum定义，而是GMS对Contract-defined band的admission effect mapping：

- `below_suggestion`：不得创建自动merge admission；
- `suggestion_only`：只可展示建议或积累evidence；
- `merge_review`：可创建proposal但等待protected human/additional-evidence gate；
- `auto_merge_eligible`：仅在无blocking conflict且所有compatibility gates通过时可自动admit。

Automatic/model/human均可请求（M1），均不得绕过protected gate。`similar_to`只表示assessment suggestion，不传递identity、evidence、permission或activation。

### 7.2 MergeProposal 内容与 keys

权威proposal使用Contract §7.22，必须保留两个source的supporting/refuting evidence、success/failure/recovery paths、branch claims，以及joint overlap/divergence/anchors/fixtures。Sources按Contract §6.3 canonical order。

- `source_pair_key`：规范排序两个exact refs的JCS/SHA-256；
- `intent_key`：target kind、symmetric strategy、normalized applicability、branch-preservation、source disposition与exact policies的JCS/SHA-256；
- `proposal_group_key`：source lineages + normalized applicability + target kind + merge-policy identity的JCS/SHA-256。

相同exact pair+intent是duplicate；相同lineages不同revision不是exact duplicate，旧attempt按freshness变stale。

### 7.3 Conflicts 与 resolution

Conflict category、resolution action、字段必填性与值域的唯一来源是 Contract §7.22；merge lifecycle 的合法使用由 Contract §9.4 约束。本文不重列或扩展这些共享 enum。

每个resolution MUST 有双端claim refs、applicability overlap、evidence refs和rationale；`prefer_source`必须给selected exact source并有policy/evidence授权。任何blocking + unresolved/request_human未完成、permission/port冲突或未知required语义均不得进入candidate validation。

### 7.4 Single in-flight winner 与 synthesis 分工

Admission以`proposal_group_key` CAS获得唯一in-flight winner（M9）；loser进入`duplicate`或按policy拒绝。Protected builder冻结A/B artifacts、双端evidence、assessment、conflicts、anchors、fixtures和policies。模型 MAY 提议merged正文；protected canonicalizer决定接纳、branch preservation、permission和provenance，禁止模型绑定Candidate或解决protected conflict。

### 7.5 Lifecycle 绑定与 GMS transition effects

Merge 状态集合、终态、合法 transition、withdraw 时点和 Contract §7.23 `MergeProposalEvent` 值域的唯一来源是 Contract §9.4、§7.23；本文不得新增、删除或重开共享状态。GMS 对合法 transition 仅实施以下权威 effects：admission 写入 M2/band/compatibility 检查与 group-winner CAS；synthesis 冻结 exact packet 与 bounded attempts；candidate binding 原子写唯一 CandidateRef 和 event；validation/replay/evaluation 固定 §5、§7.6 所需 records；activation 联合执行双 source-head CAS、新 derived lineage transaction 与 terminal event。任何 blocking conflict、资料不足、head 变化、hard-gate/U1 失败或 transaction conflict，MUST 通过 Contract §9.4 允许的 event 表达，禁止跳过状态、自动 rebase、部分 release 或将终态重开。

### 7.6 Bilateral replay 与 release

Replay必须分别覆盖A-only、B-only、overlap、conflict fixture families，baseline按reference envelope冻结。A与B critical slice任一回归非零即拒绝；四个主要utility维度相对envelope全部不差且至少一项严格改善；cost仅受预算。Assessment score不能替代replay或U1。

### 7.7 Derived activation、source retain 与关系

Accepted merge创建新symmetric derived lineage `M@1`（M3），Candidate body与released body相同；release provenance写两个exact `derived_from` source refs。A/B MUST retained active（M4），M不得`supersedes` A/B；M后续同lineagerevision可supersede旧M。禁止新增`merged_from`关系；九关系封闭集中的`derived_from`已经表达权威来源。

### 7.8 失败与后续演进

Activation前A/B任一head改变→`stale`（M5）；不得自动rebase。Source后续revision不改写旧M的`derived_from`。M被deactivate不改变proposal released历史，也不自动retire/fallback sources。Candidate bound后新evidence必须新proposal attempt。Merge不授权evidence、permission或applicability继承；双端记录始终保留。

## 8. Runtime/Curation projector architecture

### 8.1 Q32-B 权威流

运行架构 MUST 是：

```text
activation ledger + outbox + assessment/release sources
→ asynchronous idempotent projector
→ Runtime projection head + cursor vector + Graph mutation
→ Contract ProjectionWatermark
```

Activation ledger与active head是运行状态唯一权威；Graph只是派生读模型。Deployment、RSIH materialization与strong exact read直接读取权威active head/artifact，不等待Graph。

### 8.2 Runtime/Curation stream

Runtime和Curation逻辑stream/head必须独立，不共享可变head。v1只物化Runtime；candidate/rejected/inconclusive/proposal可由ledger API读取但不得进入Runtime，Curation projector必须保持disabled/uninitialized，不能伪装已支持。

### 8.3 顺序、幂等、gap 与冲突

Activation source严格按连续`activation_sequence`消费。投影幂等键为Contract §9.3规定的 `(projection_stream, projection_schema_version, activation_sequence, event_digest)`。重复相同event结果相同；同sequence不同digest→`PROJECTION_EVENT_CONFLICT`；缺sequence→`PROJECTION_SEQUENCE_GAP`并blocked，watermark不得越过。

Evidence/claim assessment、similarity assessment、anchor/release来源分别使用私有单调cursor和record-digest幂等键。每类source发现gap或同sequence异digest同样blocked；不能为了推进activation watermark跳过relation source错误。

### 8.4 Cursor vector 与公共 watermark

每次成功batch生成完整私有cursor vector。公共Contract §7.14不新增字段：

- `projected_through_activation_sequence` = 已连续投影的activation cursor；
- `source_ledger_digest` = 所有source ledger prefix identity/digest和cursor的规范承诺；
- `watermark_digest` = projection stream/schema/head/state + 完整cursor vector + source ledger digest的JCS/SHA-256。

因此activation sequence是运行可见性下界，而digest承诺relation sources的完整进度。每条非activation edge MUST 保存exact source record ref与digest，供审计重建。

### 8.5 原子 projection transaction 与 CAS

Graph node/edge mutation、source cursor update、projection head CAS和watermark写入 MUST 在同一projection-storage transaction中原子提交。Expected head/cursor不匹配时丢弃本次结果并从新head重算，不得覆盖并发进展。Graph mutation失败时watermark不推进；watermark推进即意味着对应mutation可见。

### 8.6 状态、blocked 与 rebuild

Projector 的共享状态集合和合法 transition 只由 Contract §9.3 定义，本文不重列。GMS 对 Contract `blocked` 语义的实现 MUST 停在首个失败 record 且不得推进相关 cursor/watermark；对 Contract `rebuilding` 语义的实现 MUST 从权威 ledgers/artifacts 创建新 projection head、从零按序重放，并在完成后以 CAS 切换 head。旧 head 可只读服务但必须暴露其旧 watermark；禁止在原 head 原地清洗历史。

### 8.7 Q32 八项运行不变量

Contract §5.3 第 1–8 项逐项、原文适用，是 Q32 的唯一规范定义；本文不复刻或缩窄它们。GMS implementation mapping 固定为：§8.1落实第1、3、6项；§8.2与§9.1落实第2、8项；§8.3–§8.6落实第4项；§8.4、§10.10落实第5、7项。任何 mapping 缺失均是不符合 Contract，而不是允许模块自行解释。

## 9. Skill Graph projection rules

### 9.1 节点、typed reference vertex 与 visibility

Skill-domain node封闭为exact Skill revision与branch node。Checkpoint/evidence若底层图要求vertex，MUST 使用typed reference vertex，仅保存exact DTO ref、digest、type和projection metadata，不复制正文（C2）。Runtime保留所有曾激活revision，含superseded/deactivated（C1）；默认检索仅current active，historical exact lookup可按授权返回旧节点。Candidate从不进入Runtime。

### 9.2 九关系规则表

Relation closed set及其structural/assessed分类只由Contract §11.3定义。下表逐项引用该集合，冻结GMS projector的source、direction、visibility、retention与validation mapping；它不创建第二套relation值域。

| Relation / 分类 | 权威 source record | Direction 与 endpoint | Visibility | Retention | Evidence/约束 |
|---|---|---|---|---|---|
| `composes` / Artifact structural | released Composite body | Composite revision → exact child revision | Composite曾激活后可见；默认受root active过滤 | 永久随历史node保留 | child release时active、exact ports/DAG/permissions已验 |
| `depends_on` / Artifact structural | released artifact显式dependency | dependent revision → exact prerequisite revision | source revision曾激活后可见 | 永久 | 必须显式声明、closure无环；不得推断 |
| `has_branch` / Artifact structural | released Step Guidance body | revision → revision-scoped branch node | revision曾激活后可见 | 永久 | branch id唯一，正文可由artifact重建 |
| `supersedes` / Activation structural | successful activate transition | newer same-lineage revision → previous active revision | activation投影后可见 | 永久 | 同lineage、同kind、event previous ref exact |
| `derived_from` / Release provenance | immutable derivation/release record | derived revision → exact source revision(s) | derived revision激活后可见 | 永久 | protected release provenance；模型不得推断（C3） |
| `anchored_at` / Host provenance | immutable `SkillAnchor` | Step Guidance revision → typed checkpoint ref vertex | revision与anchor均授权后可见 | 至少引用期，Runtime历史永久保edge identity | checkpoint来自Host settled Segment且digest可验 |
| `supported_by` / Evidence-assessed | versioned claim assessment | branch → typed committed/sealed evidence vertex | branch曾Runtime可见且assessment适用 | assessment/evidence引用期；历史edge保留 | exact claim/branch、committed evidence、assessor/policy ref |
| `refuted_by` / Evidence-assessed | versioned claim assessment | branch → typed committed/sealed evidence vertex | 同上，不因refute删除branch | 同上 | 不得引用staged evidence；保留反证 |
| `similar_to` / Similarity-assessed | Contract §7.21 assessment | canonical symmetric revision/branch pair | 两端均为Runtime历史node；默认查询可双向 | assessment引用期；edge identity历史保留 | exact policy/features/evidence；不触发merge/identity/permission继承 |

### 9.3 Artifact structural relations

`composes`、`depends_on`、`has_branch`只从released canonical body确定性派生。Projector MUST 对body digest复核；发现非法free relation、floating child或cycle进入blocked。Guidance正文不得存Graph（C4）。

### 9.4 Activation、release 与 Host provenance

`supersedes`只由成功activation产生，deactivate不删除edge。`derived_from`只由immutable release decision/provenance产生，可多source，merge M@1必须恰有A/B两条；不得建立`merged_from`。`anchored_at`只引用Contract §7.8 `SkillAnchor`与Host-sealed checkpoint。

### 9.5 Assessed relations

`supported_by/refuted_by`必须带exact assessment source record；新assessment追加新edge version/metadata，不原地改旧判断。`similar_to`使用canonical pair和assessment ref；低band仍可投影为assessment事实，但不得自动admit merge。Edge查询必须能返回source record ref。

### 9.6 Deactivation、历史与非法输入

Deactivate更新revision lifecycle metadata并从default active retrieval排除，不物理删除node/edge。历史exact traversal明确lifecycle与watermark。未知relation、uncommitted evidence、candidate endpoint、Graph自行创建的provenance或缺source record均`PROJECTION_RELATION_INVALID`并blocked。

## 10. Explore and Guidance View

### 10.1 Tool-specific arguments 的共同规范

本章在Contract §7.17 `ToolProxyRequest.arguments`既有开放slot内冻结三个GMS-owned normative payload binding；不改变外层共享DTO。三者core均closed，除列出字段外禁止额外core字段；可选`extensions`遵循Contract §6.4 namespaced规则。Canonical bytes为JCS UTF-8，digest为SHA-256，JSON number只允许整数；未知required extension fail closed。所有`*_ref`按Contract exact规则解析。

仅在本章tool payload内部使用的私有 `ExactBranchRefV1` 是closed value，逐字段固定为：`schema_version`（必填string，恰为`gms.exact-branch-ref.v1`）、`source_skill_ref`（必填Contract §7.3 `SkillArtifactRef`）、`branch_id`（必填non-empty string，且存在于该exact revision）、`branch_digest`（必填Digest，对该branch canonical body承诺）。其identity是 `(source_skill_ref exact identity, branch_id, branch_digest)`；它不是Contract共享DTO，不得脱离本章arguments冒充跨模块通用ref。

### 10.2 `MemoryExploreArgumentsV1`

`schema_version`固定为`gms.memory-explore-arguments.v1`。

| 字段 | 类型/必填性 | 约束 |
|---|---|---|
| `schema_version` | string，必填 | 必须等于上述值 |
| `explore_session_id` | non-empty string，必填 | 绑定Host Room/Agent/profile，不可跨scope复用 |
| `query_text` | string，必填 | UTF-8；大小上限由exact ranker/profile policy给出；空串仅在policy明确允许时可用 |
| `runtime_context_hash` | Digest，必填 | 当前运行上下文的exact hash |
| `ranker_policy_ref` | VersionedRef，必填 | exact、可解析、v1 deterministic lexical+graph policy |
| `budgets` | object，必填 | closed object，含下列四个非负整数 |
| `budgets.total_cap` | integer>=0，必填 | 总结果使用上限 |
| `budgets.evidence_subcap` | integer>=0，必填 | evidence数量上限，且不得导致total超限 |
| `budgets.skill_subcap` | integer>=0，必填 | Skill数量上限，且不得导致total超限 |
| `budgets.guidance_token_budget` | integer>=0，必填 | 所有GuidanceView token总预算 |
| `served_fences` | object，可选 | closed object；用于同session延续 |
| `served_fences.evidence_fence_digest` | Digest，可选 | 已服务evidence fence |
| `served_fences.skill_fence_digest` | Digest，可选 | 已服务Skill fence |
| `filters` | object，必填 | closed object；未指定`active_only`时语义默认true |
| `filters.active_only` | boolean，可选 | v1必须为true；false拒绝，历史读取走SkillGet |
| `filters.kinds` | array，可选 | 去重、规范排序；元素限三种canonical kind |
| `filters.lineage_ids` | array，可选 | non-empty strings，去重规范排序 |
| `filters.branch_refs` | array<`ExactBranchRefV1`>，可选 | 去重并按 `(source_skill_ref exact identity, branch_id, branch_digest)` 规范排序；禁止裸 branch string |
| `continuation_of_query_digest` | Digest，可选 | 必须引用同session/scope/ranker/context的前序query |
| `extensions` | object，可选 | Contract §6.4 |

Filters MUST NOT 请求candidate、proposal、rejected、inconclusive或Curation内容。

`MemoryExploreArgumentsV1` 的 effective payload MUST 在计算 identity 前执行且只执行 schema 声明的规范化：校验并规范排序 set-like arrays，并在省略时物化 `filters.active_only=true`；不得改写 query text、budgets、fences 或 extensions。`ExploreResult.query_digest` MUST 等于该 effective payload 的 JCS canonical bytes 的 SHA-256。省略 `active_only` 与显式 `true` 因而具有同一 query identity；`continuation_of_query_digest` 和后续 `source_query_digest` 只能引用按本规则生成且成功服务的 digest。

### 10.3 `MemoryExpandArgumentsV1`

`schema_version`固定为`gms.memory-expand-arguments.v1`。

| 字段 | 类型/必填性 | 约束 |
|---|---|---|
| `schema_version` | string，必填 | 必须等于上述值 |
| `explore_session_id` | non-empty string，必填 | 与source query和fences同session/scope |
| `source_query_digest` | Digest，必填 | 必须解析到本session已成功服务query |
| `target` | object，必填 | closed tagged union |
| `target.ref_type` | enum，必填 | `branch|evidence|checkpoint|child_skill` |
| `target.ref` | exact object，必填 | branch exact identity或Contract `EvidenceRef`/`CheckpointRef`/`SkillArtifactRef`，必须与tag匹配且已由source result合法暴露 |
| `source_view_hash` | Digest，可选/条件必填 | 扩展GuidanceView内容、branch或child_skill时必填；必须匹配source view |
| `runtime_context_hash` | Digest，必填 | 必须与source query兼容，变化需新explore |
| `ranker_policy_ref` | VersionedRef，必填 | 与source query exact policy一致 |
| `budgets` | object，必填 | 与§10.2同shape/约束，按本次expand计费且受profile上限 |
| `served_fences` | object，必填 | closed object；必须延续source query返回的fences |
| `served_fences.evidence_fence_digest` | Digest，必填 | 必须等于source result的evidence fence |
| `served_fences.skill_fence_digest` | Digest，必填 | 必须等于source result的Skill fence |
| `max_graph_depth` | integer，必填 | v1必须恰为`1` |
| `extensions` | object，可选 | Contract §6.4 |

Target未在source结果的expandable refs中、任一fence缺失/不相等、跨scope或depth非1均fail closed。Expand不得刷新session权限或active closure。`MemoryExpandArgumentsV1` 不定义默认字段；其 query identity 是完整 arguments core（含两个 required fence digests）JCS canonical bytes的SHA-256，成功 `ExploreResult.query_digest` MUST 使用该值。

### 10.4 `SkillGetArgumentsV1`

`schema_version`固定为`gms.skill-get-arguments.v1`。

| 字段 | 类型/必填性 | 约束 |
|---|---|---|
| `schema_version` | string，必填 | 必须等于上述值 |
| `skill_ref` | SkillArtifactRef，必填 | exact；禁止Graph node ID/裸名称/latest |
| `runtime_context_hash` | Digest，必填 | Guidance rendering上下文 |
| `render_profile_ref` | VersionedRef，必填 | exact rendering profile |
| `policy_ref` | VersionedRef，必填 | exact Guidance policy |
| `included_branch_refs` | array<non-empty branch_id string>，可选 | 每项由同一请求的`skill_ref`定域为revision-scoped branch identity；去重，按UTF-8 bytes排序，且必须存在于该revision；此字段不承载`ExactBranchRefV1` object |
| `guidance_token_budget` | integer>=0，必填 | 单view上限，受Host profile cap |
| `visibility` | enum，必填 | `current_active|historical_exact`；后者必须有exact Host profile授权 |
| `extensions` | object，可选 | Contract §6.4 |

`current_active`要求skill_ref等于权威active head；`historical_exact`可读取superseded/deactivated released ref，但不得读取candidate或赋予执行权限。

### 10.5 返回绑定与 Host readiness

外层始终是Contract §7.18 `ToolProxyResult`，禁止自由JSON：

- `memory_explore` success `result` = Contract §7.16 `ExploreResult`；
- `memory_expand` success `result` = Contract §7.16 `ExploreResult`；其`query_digest` MUST 是§10.3冻结的effective expansion arguments canonical bytes的SHA-256，并受同session、separate fences、budgets与watermark约束；
- `skill_get` success `result` = Contract §7.15 `GuidanceView`。

§10.2–§10.4关闭Host Spec §7.4的request-slot门槛。对`memory_expand`，shared result schema已经由Contract §7.16冻结，本文只完成Host Spec §5.2允许由GMS声明的tool-name binding；因此满足Host Spec §7.8的“由Contract冻结shared schema”条件，并未创建GMS私有替代结果。S2仍必须证明exact Pi return path。

`skill_get`的返回类型已经冻结，但当前Host Spec §5.6对所有success result无条件要求budgets、citations与watermark，而closed Contract §7.15 `GuidanceView`没有这些字段。GMS MUST NOT向GuidanceView添加字段或伪造ExploreResult wrapper；在Host Spec以新版本把这些检查限定为适用tool、或Contract以新版本提供兼容shared DTO之前，集成profile MUST 禁用`skill_get`并fail closed（GMS侧为`TOOL_UNSUPPORTED`，Host侧按其closed policy映射）。这不改变未来解除门槛后的success binding仍为Contract §7.15。

GMS返回的是Host构造外层ToolProxyResult所使用的exact upstream typed payload；Host不得重排或裁剪。

【修订 v1.1，CTR-002】Contract §12.7.1 已冻结 tool-specific success validation matrix（权威 `conformance/policy/tool-success-validation.v1.json`）：GMS 在返回 success payload 前 MUST 按同一 matrix 自验——`memory_explore`/`memory_expand` 依 explore-family 规则集（watermark/budgets/citations），`skill_get` 只验证 closed `GuidanceView` 适用字段（完整性、`view_hash`、profile 版本下限）。上段“Host Spec §5.6 无条件要求 budgets/citations/watermark 与 closed GuidanceView 不兼容”的缺口由该 matrix 解除：本节 disable 条件收窄为“Contract §12.7.1 readiness gate（`tools/` fixture + policy digest 全绿）未通过前 `skill_get` 保持 disabled（GMS 侧 `TOOL_UNSUPPORTED`）”；gate green 后 MAY enable，成功绑定仍且仅是 Contract §7.15 `GuidanceView`，禁止向其私添字段或伪造 `ExploreResult` wrapper。

### 10.6 Scope二次验证与 query flow

GMS MUST 二次验证Host提供的Room/Agent/profile binding、tool allowlist、budgets、freshness和session/fence ownership。跨Room、profile不匹配、自报scope或GMS结果越权均整体失败，不做局部过滤。

Query flow为：验证payload/digest/scope→检查watermark要求→deterministic lexical candidate retrieval→Graph expansion→integer ranking→budget allocation→canonical artifact read→Guidance render→typed result与watermark。

### 10.7 Deterministic ranking 与 visibility

Ranker policy必须exact versioned，所有权重用整数定标，禁止未版本化模型判断。排序至少固定：policy total score降序、active status、graph distance升序、exact ref lexical order（最终以lineage UTF-8 bytes、version、digest稳定打破平局）。同输入、projection head、policy和fences必须同序。

默认只返回current active。Historical只由SkillGet exact授权读取，不混入普通Explore。Merge后A/B/M均retained active，重叠结果是合法结果，不得自动隐藏sources；ranker只按versioned policy排序。

### 10.8 Budgets、fences 与 citations

必须保证`total_used<=total_cap`、两类count分别不超subcap、所有view token总和不超guidance budget。Explore/Expand成功截断只能在Contract §7.16 `truncation_reason_codes`使用§11.4冻结的四个success-metadata code；branch omission只能通过Contract §7.15既有`omitted_branch_refs`与`expandable_refs`表达，禁止私添共享字段或无标记截断。

Contract §12.4同时要求top-level omitted evidence/Skill refs，但Contract §7.16 v1没有承载它们的字段。若一次查询会因total/evidence/Skill cap省略top-level结果，GMS MUST fail closed为`TOOL_RESULT_BINDING_INVALID`，不得返回无法满足Contract的success payload；S7不得宣称该路径通过。只有后续Contract版本增加typed omitted refs或明确收窄§12.4后，才可启用这类成功截断。Guidance token budget造成的branch/content截断仅在现有GuidanceView字段可完整表达时可成功；standalone `skill_get`除§10.5门槛外还禁止成功返回`truncated=true`而无可承载reason的位置。

【修订 v1.1，CTR-003】Contract §7.16 已由 §12.7.2 冻结 top-level `omissions` carrier：cap-induced top-level omission 现可由 typed exact refs 完整表达并成功返回；上段“缺少承载位 MUST fail closed”仅对未满足 §12.7.2 一致性义务的 payload（如 silent truncation）继续适用，conformance 校验由 `validate_explore_omission.py` 承担。

Evidence与Skill共享ExploreSession但result type和served fence分离。Identity citation只证明exact Skill；evidence citation只证明claim支持/反驳，二者不得替代。Retry相同query返回相同exact result且不重复消费fence。

### 10.9 Guidance View 与 expansion

Guidance View从canonical artifact按exact render/policy/context派生，不写Graph。Renderer MUST 固定source ref、included/omitted branches、expandable exact refs、token count、truncated和content。`view_hash`是除自身hash字段外按冻结render schema形成的canonical view preimage digest；具体preimage必须由conformance fixture固定，不能由实现临时改变。

Expansion只能针对同session先前合法服务且fence允许的exact target，depth=1；扩展Guidance内容必须校验source_view_hash。返回仍为ExploreResult并携带最新projection watermark，但不得静默改变source query语义。

### 10.10 Freshness 与 exact upstream result

每个Explore/Expand响应携带Contract §7.14 watermark。若ToolProxyRequest提供`requested_min_activation_sequence`且projection未追平，GMS返回`PROJECTION_BEHIND_REQUIRED_SEQUENCE`，可按policy等待但不得降级。SkillGet `current_active`直接校验权威head，不依赖Graph追平。

GMS upstream payload必须canonicalizable且digest可验证；Host通过原Pi tool call返回exact Contract §7.18 result。Side-channel、late result或日志不得被宣称为Agent观察。

## 11. API and error contract

### 11.1 逻辑 API

本文定义逻辑能力，不绑定HTTP、RPC或进程边界：

| 能力 | 核心输入/输出 | 权威行为 |
|---|---|---|
| `IngestEvidence` | Host exact seals/payload → committed EvidenceRef或失败 | staging validation与commit ledger |
| `SubmitSkillProposal` | Contract SkillProposal/suggestion refs → proposal state | admission/dedup |
| `BindCandidate` | admitted proposal + suggested body → CandidateRef | protected canonicalization |
| `ValidateCandidate` | CandidateRef + exact profile/policies → validation record | hard schema/provenance gates |
| `CreateReplayRequest` | validated candidate + fixtures/baselines → ReplayRequest | freeze packet |
| `CommitReplayResult` | Host/RSIH outputs → ReplayResult | canonical authoritative result |
| `EvaluateRelease` | validation/replay/policies → ReleaseDecision | hard gates + U1 |
| `Activate` | accepted decision + expected head(s) → ReleasedRef/Event | atomic release/CAS/outbox |
| `Deactivate` | authorization + expected head → Event | atomic clear/outbox |
| `GetArtifact` | exact released ref | canonical bytes/metadata |
| `GetActiveHead` | lineage | authoritative exact head/null |
| `AssessSimilarity` | two exact revisions + policy/evidence | SimilarityAssessment |
| `Submit/AdvanceMerge` | MergeProposal/event transition | lifecycle/group CAS |
| `ProjectRuntime` | ledger source records | Graph + watermark CAS |
| `Explore` | MemoryExploreArgumentsV1 | ExploreResult |
| `Expand` | MemoryExpandArgumentsV1 | ExploreResult |
| `SkillGet` | SkillGetArgumentsV1 | GuidanceView |

所有mutation要求idempotency key和canonical request digest；read要求scope与exact refs。Graph/Explore/Expand读取必须按Contract §5.3.5、§11.6返回watermark。GMS私有`GetArtifact`、`GetActiveHead`和materialization closure读取若使用私有response envelope，也 MUST 附带当时可见的ProjectionWatermark作为信息字段，但不得等待它追平或以它替代权威active-head/activation sequence。Contract §7.15没有watermark slot，因此`SkillGet`遵循§10.5的fail-closed readiness规则，不得私添字段。

### 11.2 私有 error envelope

跨模块失败若由外层Contract §7.18承载，使用其`error`字段；其他GMS逻辑API可使用私有closed `GmsErrorEnvelope`，字段固定为：`schema_version=gms.error-envelope.v1`、`reason_code`、`message`、`retryable`、`reason_policy_ref`、`record_refs`、`error_digest`，可选namespaced `extensions`。Message不驱动行为；未知字段/required extension失败。

### 11.3 Reason-code policy identity

Reason/truncation code registry由本文§11.4冻结为`gms.reason-codes.v1`。其canonical policy body是GMS私有closed object，字段固定为：`schema_version="gms.reason-policy.v1"`、`policy_id="gms.reason-codes"`、`version=1`、`failure_codes`（按§11.4分类与出现顺序完整列出）、`truncation_codes`（按§11.4顺序完整列出）、`retry_same_request_codes`、`new_attempt_required_codes`、`inconclusive_status_codes`；禁止其他core字段。各数组无重复且顺序具有规范意义。

v1映射固定为：`retry_same_request_codes=[PROJECTION_BEHIND_REQUIRED_SEQUENCE]`；`new_attempt_required_codes=[IDEMPOTENCY_CONFLICT, PROPOSAL_STATE_CONFLICT, PROPOSAL_STALE, VALIDATION_INPUT_STALE, RELEASE_TRANSACTION_CONFLICT, ACTIVE_HEAD_CONFLICT, SOURCE_HEAD_STALE, ACTIVATION_SEQUENCE_CONFLICT, MERGE_STATE_CONFLICT, MERGE_SOURCE_HEAD_STALE, PROJECTION_HEAD_CONFLICT]`；`inconclusive_status_codes=[REPLAY_INCONCLUSIVE, MERGE_SYNTHESIS_INCONCLUSIVE]`。前一集合的`retryable=true`；后两集合及其余failure code的`retryable=false`，但`new_attempt_required_codes`只能通过新attempt/fresh expectation恢复。`inconclusive_status_codes`映射Contract §7.18 `status=inconclusive`，其他failure code映射`status=failed`。未出现在这些特殊集合中的failure code是terminal for same request。

部署 MUST 以 `VersionedRef{id:"gms.reason-codes",version:1,digest:<computed>}` 固定该body；digest是上述closed body的JCS/SHA-256。GMS conformance mapping MUST 在Contract §16逻辑目录下提供此policy的source/canonical/expected fixture并生成digest，不得手写占位值或依赖文档排版。调用方遇到未列code、重复code或policy digest不匹配 MUST fail closed，不能猜测status或retryability。Host-local codes（包括`UPSTREAM_*`、`HOST_*`、`PI_RETURN_CHANNEL_FAILED`）只由Host Spec §5.9定义，GMS MUST NOT生成。

### 11.4 封闭 reason-code 值域

**Schema/canonical/ref**：`SCHEMA_VERSION_UNSUPPORTED`、`SCHEMA_FIELD_UNKNOWN`、`SCHEMA_REQUIRED_FIELD_MISSING`、`SCHEMA_ENUM_INVALID`、`CANONICALIZATION_FAILED`、`NON_INTEGER_NUMBER`、`UNKNOWN_REQUIRED_EXTENSION`、`EXTENSION_SCHEMA_INVALID`、`DIGEST_MISMATCH`、`REF_MISMATCH`、`NON_EXACT_REF`、`IDEMPOTENCY_CONFLICT`。

**Evidence/Host provenance**：`EVIDENCE_STAGING_INVALID`、`EVIDENCE_NOT_COMMITTED`、`EVIDENCE_SEAL_INVALID`、`SEGMENT_NOT_SETTLED`、`CHECKPOINT_NOT_SEALED`、`EVIDENCE_SCOPE_DENIED`、`EVIDENCE_PROVENANCE_INCOMPLETE`。

**Artifact/proposal/candidate**：`SKILL_KIND_INVALID`、`LINEAGE_KIND_MISMATCH`、`LINEAGE_VERSION_CONFLICT`、`ARTIFACT_BODY_INVALID`、`BRANCH_PROVENANCE_INCOMPLETE`、`PORT_SCHEMA_MISSING`、`PORT_SCHEMA_INCOMPATIBLE`、`COMPOSITE_CYCLE`、`COMPOSITE_CHILD_NOT_ACTIVE`、`COMPOSITE_RETRY_INVALID`、`COMPOSITE_FAILURE_ACTION_INVALID`、`DEPENDENCY_CYCLE`、`PROPOSAL_INVALID`、`PROPOSAL_DUPLICATE`、`PROPOSAL_STATE_CONFLICT`、`PROPOSAL_STALE`、`PROPOSAL_WITHDRAWAL_FORBIDDEN`、`CANDIDATE_NOT_FOUND`、`CANDIDATE_IMMUTABLE`、`CANDIDATE_NOT_EXECUTABLE`、`CANDIDATE_RELEASED_BODY_MISMATCH`。

**Replay/evaluation/release**：`VALIDATION_FAILED`、`VALIDATION_INPUT_STALE`、`FIXTURE_SET_INCOMPLETE`、`REPLAY_REQUEST_INVALID`、`REPLAY_NONDETERMINISTIC`、`REPLAY_RESULT_INVALID`、`REPLAY_INCONCLUSIVE`、`CRITICAL_REGRESSION`、`SAFETY_GATE_FAILED`、`HARD_GATE_FAILED`、`REFERENCE_ENVELOPE_INVALID`、`UTILITY_NOT_PARETO_IMPROVED`、`UTILITY_COST_BUDGET_EXCEEDED`、`UTILITY_ARITHMETIC_OVERFLOW`、`RELEASE_DECISION_INVALID`、`RELEASE_NOT_ACCEPTED`、`RELEASE_TRANSACTION_CONFLICT`。

**Activation**：`ACTIVE_HEAD_CONFLICT`、`SOURCE_HEAD_STALE`、`ACTIVATION_AUTHORIZATION_INVALID`、`ACTIVATION_EVENT_INVALID`、`ACTIVATION_SEQUENCE_CONFLICT`、`DEACTIVATION_AUTHORIZATION_INVALID`、`NO_ACTIVE_HEAD`、`PROBATION_UNSUPPORTED`、`REACTIVATION_AUTHORIZATION_REQUIRED`。

**Merge/similarity**：`SIMILARITY_ASSESSMENT_INVALID`、`SIMILARITY_BELOW_THRESHOLD`、`MERGE_SOURCE_INVALID`、`MERGE_KIND_UNSUPPORTED`、`MERGE_SOURCE_NOT_ACTIVE`、`MERGE_SOURCE_PROBATIONARY`、`MERGE_PROPOSAL_DUPLICATE`、`MERGE_GROUP_IN_FLIGHT`、`MERGE_STATE_CONFLICT`、`MERGE_BLOCKING_CONFLICT`、`MERGE_EVIDENCE_INCOMPLETE`、`MERGE_BRANCH_REGRESSION`、`MERGE_SOURCE_HEAD_STALE`、`MERGE_SYNTHESIS_INCONCLUSIVE`、`MERGE_WITHDRAWAL_FORBIDDEN`。

**Projector/Graph**：`PROJECTION_SEQUENCE_GAP`、`PROJECTION_EVENT_CONFLICT`、`PROJECTION_SOURCE_GAP`、`PROJECTION_SOURCE_CONFLICT`、`PROJECTION_SCHEMA_UNSUPPORTED`、`PROJECTION_RELATION_INVALID`、`PROJECTION_HEAD_CONFLICT`、`PROJECTION_BLOCKED`、`PROJECTION_REBUILD_REQUIRED`、`PROJECTION_BEHIND_REQUIRED_SEQUENCE`。

**Explore/tool/scope/budget**：`TOOL_ARGUMENTS_INVALID`、`TOOL_RESULT_BINDING_INVALID`、`TOOL_UNSUPPORTED`、`EXPLORE_SESSION_INVALID`、`EXPLORE_SCOPE_VIOLATION`、`EXPLORE_QUERY_INVALID`、`EXPLORE_CONTINUATION_INVALID`、`EXPLORE_FILTER_INVALID`、`EXPLORE_FENCE_CONFLICT`、`EXPAND_TARGET_NOT_SERVED`、`EXPAND_SOURCE_VIEW_MISMATCH`、`EXPAND_DEPTH_UNSUPPORTED`、`SKILL_NOT_CURRENT_ACTIVE`、`HISTORICAL_READ_NOT_AUTHORIZED`、`GUIDANCE_RENDER_FAILED`、`GUIDANCE_VIEW_HASH_MISMATCH`、`BUDGET_INVALID`、`BUDGET_EXCEEDED`、`CITATION_INVALID`、`PERMISSION_CAP_EXCEEDED`、`SCOPE_PROFILE_INVALID`。

**成功响应的truncation metadata（不得放入error envelope）**：`TOTAL_CAP_REACHED`、`EVIDENCE_SUBCAP_REACHED`、`SKILL_SUBCAP_REACHED`、`GUIDANCE_TOKEN_BUDGET_REACHED`。

其中Contract negative fixtures要求的`NON_INTEGER_NUMBER`、`UNKNOWN_REQUIRED_EXTENSION`、`DIGEST_MISMATCH`、`NON_EXACT_REF`、`COMPOSITE_CYCLE`、`PORT_SCHEMA_MISSING`、`MERGE_BLOCKING_CONFLICT`、`EVIDENCE_NOT_COMMITTED`、`PROJECTION_SEQUENCE_GAP`、`PERMISSION_CAP_EXCEEDED`均保持原义。Host MAY 按Host Spec §5.9透传`PROJECTION_BEHIND_REQUIRED_SEQUENCE`和`IDEMPOTENCY_CONFLICT`；`UPSTREAM_SCHEMA_INVALID`、`UPSTREAM_DIGEST_MISMATCH`、`UPSTREAM_SCOPE_VIOLATION`、`UPSTREAM_BUDGET_VIOLATION`是Host校验GMS payload后生成的Host-local code，GMS不得自报。

### 11.5 Retry 与未知错误

Status、retryability与“必须新attempt”完全由§11.3 canonical policy body决定，不允许实现按前缀、HTTP状态或自由message推断。只有`retry_same_request_codes`中的code可对同一canonical request与idempotency identity等待/重试；`new_attempt_required_codes`必须重新冻结expectation并使用新attempt identity；其他failure code对原请求终止。未知code、未知required extension、policy body不闭合或reason policy digest不匹配 MUST fail closed；Host应映射为其`UPSTREAM_SCHEMA_INVALID`，不得透传未知行为码。

Reason-code registry 的权威冻结形式（system/host-proxy 两份 policy、ownership 与 precedence、§11.3 canonical body 的推导 digest）以 Contract §13.7.1（修订 v1.1，CTR-004）policy 为准。

## 12. Slice obligations

以下14个小节统一使用相同十项四级标题。每项均为GMS normative责任；其他模块权威不因slice而转移。

### 12.1 S1 — Exact refs and canonical digest

#### Inputs
Contract §16 golden source/expected files、artifact/tool payload samples、Go/TypeScript canonical bytes。

#### Preconditions
Schema/policy exact可解析；runner无网络、时钟与未固定随机输入。

#### Authoritative writes
无生产ledger写；conformance运行记录MAY append审计。

#### Derived writes
Canonical bytes、digest与reason-code comparison report。

#### Outputs
GMS canonical bytes/digest/accept-reject与RSIH TypeScript完全一致。

#### Failure modes
JCS差异、Unicode/key order差异、非整数、unknown required extension、digest/ref mismatch。

#### Idempotency/CAS
同fixture重复运行bit-identical；无CAS。

#### Observability
记录case ID、schema、computed/expected digest与closed reason code。

#### Acceptance fixture
Contract §16全部适用fixtures，尤其minimal ref、Unicode、三kind、candidate/released equality与negative cases。

#### Non-goals
修改golden以适配实现；网络依赖；生产artifact release。

### 12.2 S3 — Consume Host seals and commit evidence

#### Inputs
Host settled SegmentRef、CheckpointRef、Evidence/Path seals和scope/provenance packet。

#### Preconditions
Host close原子完成；exact refs/digests可解析；success/failure/recovery paths存在。

#### Authoritative writes
Evidence staging events、committed evidence bodies、Contract EvidenceRefs、commit/idempotency ledger。

#### Derived writes
Evidence lookup/provenance index；不得写Runtime edge直到assessment/source可投影。

#### Outputs
仅committed/sealed EvidenceRefs及可审计Host source bindings。

#### Failure modes
Segment未settled、seal/digest/scope错误、path不完整、重复冲突；失败不铸造EvidenceRef。

#### Idempotency/CAS
相同sealed input/key返回原EvidenceRef；不同digest冲突；commit单次CAS。

#### Observability
从EvidenceRef追踪Segment/seals/checkpoint、kind、commit gate与omission。

#### Acceptance fixture
消费Host S3 success/failure/recovery和ToolProxyResult路径；staged/uncommitted negative不得进入Graph。

#### Non-goals
创建或修改Host seal；重建当前transcript；Skill proposal/release。

### 12.3 S4 — Candidate validation and paired replay

#### Inputs
CandidateRef、validation profiles、ReplayRequest inputs、Host seals、baseline refs、RSIH outputs。

#### Preconditions
Candidate bound且immutable；fixtures覆盖required families；adapter/profile exact；Host可调度。

#### Authoritative writes
Validation records、ReplayRequest、canonical ReplayResult及proposal state events。

#### Derived writes
Replay status/index和comparison trace；Host queue仍由Host拥有。

#### Outputs
Deterministic paired outcomes、五维integer utility、hard-gate records供evaluation。

#### Failure modes
Schema/provenance/permission失败、missing family、nondeterminism、infrastructure/semantic failure、stale head。

#### Idempotency/CAS
相同packet重用request/result；lease防重复；不同input新request；state event CAS。

#### Observability
fixture→baseline/candidate→Host attempt→RSIH output→ReplayResult完整链。

#### Acceptance fixture
普通success/failure/recovery及merge A/B/overlap/conflict重放；后续transcript变化不影响digest。

#### Non-goals
Host执行调度；模型评分；activation；production live评价。

### 12.4 S5 — Decision and activation

#### Inputs
Validation records、ReplayResults、exact release/comparator policies、CandidateRef、expected heads。

#### Preconditions
Hard gates完整；U1可计算；decision input未stale；permission和body equality可证。

#### Authoritative writes
ReleaseDecision、version/mapping、active-head、ActivationEvent、outbox和proposal terminal event的原子写。

#### Derived writes
Active lookup index；Graph由后续异步projector写。

#### Outputs
Accepted且CAS成功的ReleasedRef/activation，或无部分发布的closed failure。

#### Failure modes
critical regression、非Pareto改善、cost超预算、overflow、head冲突、body mismatch。

#### Idempotency/CAS
Decision与activation keys幂等；expected active/source heads exact CAS；version不复用。

#### Observability
Decision gates、U1 trace、transaction result、sequence/outbox与head前后值。

#### Acceptance fixture
Hard gate与U1正负例、CAS失败无部分version、candidate/released相同body digest。

#### Non-goals
同步Graph写；RSIH bundle；probation或自动rollback。

### 12.5 S6 — Runtime projection

#### Inputs
Activation/outbox、released artifacts、anchors、evidence/claim与similarity assessments。

#### Preconditions
Projection schema/head已知；source ledgers连续；exact source records可解析。

#### Authoritative writes
无artifact ledger写；原子写Runtime Graph mutation、cursor vector、projection head/watermark。

#### Derived writes
Revision/branch/reference nodes、九关系、lexical/graph indexes。

#### Outputs
可重建Runtime Graph、历史nodes、source-record edges与Contract watermark。

#### Failure modes
Gap、duplicate conflict、illegal relation、candidate endpoint、unknown schema/digest；进入blocked。

#### Idempotency/CAS
每source幂等键；重复不变；same sequence不同digest冲突；head/cursor CAS。

#### Observability
每edge source ref、每batchcursor vector、watermark digest、blocked sequence和rebuild head。

#### Acceptance fixture
Duplicate idempotent、gap blocked、same-sequence conflict、deactivation保历史、九关系适用tracers。

#### Non-goals
Graph成为权威；Curation物化；candidate projection；release事务内投影。

### 12.6 S7 — Explore and Guidance retrieval

#### Inputs
三个tool-specific arguments payload、Host scope profile、Runtime projection、canonical artifacts。

#### Preconditions
Host S2 exact return readiness；payload schema冻结；scope/budgets/fences/freshness有效；`skill_get`必须满足§10.5跨文档门槛，否则保持disabled；会省略top-level结果的请求必须满足§10.8 schema可表达性，否则fail closed。

【修订 v1.1，CTR-002】Preconditions/Outputs 中 `skill_get` 的门槛引用更新为 Contract §12.7.1 readiness gate（见 §10.5 修订 v1.1）；Explore/Expand 的 success 验证按 §12.7.1 explore-family matrix（watermark/budgets/citations）执行，GMS 侧与 Host 侧推导一致。

#### Authoritative writes
ExploreSession/query/fence consumption与idempotency audit；不修改artifact/Graph权威。

#### Derived writes
Retrieval/ranking trace、Guidance Views、typed ExploreResults。

#### Outputs
Explore/Expand固定返回binding、separate results/fences/citations、budgets/truncation和watermark；`skill_get`仅在§10.5门槛解除后可成功返回GuidanceView。

#### Failure modes
Scope、budget、fence、view hash、unsupported filter/depth、projection behind、invalid citation。

#### Idempotency/CAS
相同query返回same result且fence不重复消费；session/profile与fence CAS。

#### Observability
Query digest、policy、scores/tie-break、counts、view hashes、fences、watermark和upstream digest。

#### Acceptance fixture
Explore/Expand正负例，active default、depth=1、required fences、normalized query digest、min sequence fail closed；`skill_get`与top-level omission在现有跨文档缺口下必须验证disabled/fail-closed，门槛解除后再启用historical auth与GuidanceView成功例。

#### Non-goals
Host本地rerank；free JSON；candidate/curation retrieval；materialization或新增权限。

### 12.7 S8 — Exact materialization inputs

#### Inputs
RSIH请求的active root exact refs、permission/render profiles和closure traversal需求。

#### Preconditions
Roots等于权威active heads；all exact children/dependencies可解析；permission允许；activation sequence固定。
Contract §6.2.1（修订 v1.1，CTR-001）已冻结 materialization exact identity；本模块按该决议提供 closure 输入，GMS 不铸造 manifest identity。

#### Authoritative writes
只读服务及materialization request audit；GMS不写Manifest/SkillLock。

#### Derived writes
Deterministic exact closure response与source artifact byte stream。

#### Outputs
Active exact root/transitive artifacts、digests、ports、permissions和freeze activation sequence供RSIH。

#### Failure modes
Head stale、missing child、digest mismatch、deactivated root、permission denial、cycle。

#### Idempotency/CAS
同roots/profile/sequence返回相同closure；读取head前后可用sequence/CAS token防撕裂。

#### Observability
Root→closure exact refs、bytes digests、sequence、permission decision与RSIH correlation。

#### Acceptance fixture
RSIH据输入生成content-addressed bundle/lock并验证session freeze；head后变不改变已冻结输入。

#### Non-goals
GMS生成bundle、SKILL.md、Manifest或SkillLock；依赖Graph追平。

### 12.8 S9 — Composite binding and replay

#### Inputs
Activated exact children、ports/schemas、DAG、retries/failures、permissions、Composite proposal/fixtures。

#### Preconditions
Children released/current active；ports匹配；DAG无环；Host cap允许；whole replay可调度。

#### Authoritative writes
Composite Candidate/artifact、validation/replay/decision、release/activation ledgers。

#### Derived writes
Projector产生`composes|depends_on`和revision node；closure index。

#### Outputs
通过static+whole replay的active exact Composite供RSIH materialize/execute。

#### Failure modes
Missing/inactive child、port mismatch、cycle、unbounded retry、invalid fallback、permission excess、whole replay regression。

#### Idempotency/CAS
Child refs冻结；任何变化新revision；candidate/release/head按通用CAS。

#### Observability
Composite→children/ports/edges/permissions、whole replay outcomes、release decision和activation。

#### Acceptance fixture
Exact children、typed mappings、bounded failure paths、whole replay；child变化强制新revision。

#### Non-goals
GMS执行Composite；复制child guidance；用ablation替代whole replay；动态child。

### 12.9 MT1 — Similarity assessment

#### Inputs
两个canonical ordered active Step Guidance refs、feature records、evidence、exact similarity policy/assessor。

#### Preconditions
Sources released/current active/nonprobation；policy整数定标；evidence committed。

#### Authoritative writes
Contract SimilarityAssessment immutable ledger record。

#### Derived writes
Projector可产生带source record ref的canonical `similar_to` edge。

#### Outputs
score_micros、band、compatibility、support/refute evidence和exact policy。

#### Failure modes
Kind/source invalid、uncommitted evidence、policy/digest mismatch、blocking incompatibility。

#### Idempotency/CAS
Canonical pair使A+B/B+A同key；相同input返回同assessment。

#### Observability
Feature refs、integer score、band thresholds、compatibility与edge projection cursor。

#### Acceptance fixture
Canonical pair/key一致；below-threshold不自动admit；unknown required extension拒绝。

#### Non-goals
自动identity合并；learned similarity；activation或evidence继承。

### 12.10 MT2 — Proposal admission and dedup

#### Inputs
SimilarityAssessment、双source refs/evidence、conflicts、policies、dedup keys和origin。

#### Preconditions
Band允许对应发起路径；M2 gates通过；group无winner或可CAS。

#### Authoritative writes
MergeProposal、initial/admission/duplicate events和group-winner record。

#### Derived writes
Merge queue/index；不进入Runtime。

#### Outputs
唯一admitted winner或指向canonical winner的duplicate终态。

#### Failure modes
Exact duplicate、group in-flight、stale head、permission/port/applicability不兼容。

#### Idempotency/CAS
source_pair/intent/group keys规范计算；single winner CAS；重复请求返回原event。

#### Observability
Assessment→proposal→group winner、observed heads、reason codes和event sequence。

#### Acceptance fixture
Exact duplicate→duplicate；同lineage不同revision不误dedup；并发只有一winner。

#### Non-goals
Candidate synthesis；自动rebase；n-way/cross-kind merge。

### 12.11 MT3 — Merge candidate synthesis

#### Inputs
Admitted proposal、frozen A/B bodies/evidence/claims/conflicts/policies及模型suggestion。

#### Preconditions
Group lease有效；source heads仍匹配；blocking conflicts已有合法resolution。

#### Authoritative writes
Synthesis attempts、frozen packet、CandidateRef binding与state events。

#### Derived writes
Provenance/branch-preservation validation trace。

#### Outputs
新derived-lineage intent的immutable merge CandidateRef，或closed reject/inconclusive/stale。

#### Failure modes
Blocking unresolved、evidence/provenance缺失、applicability/permission扩大、bounded attempts耗尽、head变化。

#### Idempotency/CAS
Frozen packet digest绑定attempt；唯一Candidate；state/lease CAS；新正文新attempt。

#### Observability
双端branch mapping、每conflict action/evidence、model suggestion与protected acceptance差异。

#### Acceptance fixture
双端branches保留；blocking conflict拒绝/不确定；模型不能直接bind或validate。

#### Non-goals
模型自评；修改A/B；新增`merged_from`；release。

### 12.12 MT4 — Bilateral replay and release decision

#### Inputs
Merge Candidate、A/B/overlap/conflict fixtures、baseline map、validation、rules/comparator。

#### Preconditions
Candidate valid；source expectations冻结；all families完整；Host/RSIH deterministic path可用。

#### Authoritative writes
ReplayRequest/Result、hard-gate records、Contract ReleaseDecision和state events。

#### Derived writes
Reference-envelope与U1 comparison trace。

#### Outputs
双端zero-critical-regression且Pareto严格改善的accepted decision，或reject/inconclusive。

#### Failure modes
任一source critical regression、missing family、nondeterminism、无主要维度改善、cost/overflow、stale。

#### Idempotency/CAS
相同fixtures/packet结果确定；decision input digest幂等；state CAS。

#### Observability
每domain baseline/outcome、五维aggregate、交叉乘法/overflow检查和reason。

#### Acceptance fixture
A/B均不回归+envelope严格改善正例；任一critical regression负例；仅cost改善拒绝。

#### Non-goals
Similarity score替代replay；production traffic；activation前隐藏A/B。

### 12.13 MT5 — Derived activation

#### Inputs
Accepted merge decision、CandidateRef、A/B expected active heads和new-lineage intent。

#### Preconditions
A/B仍current active/nonprobation/released Step Guidance；body equality和permission通过。

#### Authoritative writes
原子source-head CAS、M@1 lineage/version、mapping、ActivationEvent/outbox和proposal released event。

#### Derived writes
Projector后续产生M node、两条`derived_from`；A/B保持active visibility。

#### Outputs
Active M@1新lineage，A/B retained；无`supersedes`到A/B。

#### Failure modes
任一source head变化→stale；transaction/head/version conflict；body mismatch；不得部分激活。

#### Idempotency/CAS
两个source heads和new lineage head联合CAS；相同key返回原M@1/event。

#### Observability
A/B observed/expected heads、transaction commit、M ref、event sequence/outbox和proposal event。

#### Acceptance fixture
成功产生两条derived provenance且A/B active；source更新时stale且无M activation。

#### Non-goals
Retire/deprefer A/B；自动fallback；改写旧source lineage。

### 12.14 MT6 — Projection, retrieval and materialization

#### Inputs
M activation/outbox、release provenance、A/B/M artifacts/assessments及Explore/materialization requests。

#### Preconditions
MT5 committed；projector source连续；scope/policies/budgets有效。
Contract §6.2.1（修订 v1.1，CTR-001）已冻结 materialization exact identity；本模块按该决议提供 M closure 输入与 frozen sequence，不铸造 manifest/lock identity。

#### Authoritative writes
仅Explore/fence审计；artifact/activation历史已由MT5写，projection为派生事务。

#### Derived writes
M revision/branches、两条`derived_from`、similarity/evidence edges、watermark、indexes/Guidance Views。

#### Outputs
Runtime可检索A/B/M且不隐藏sources；RSIH获得M exact closure输入与frozen sequence。

#### Failure modes
Candidate泄漏、missing provenance edge、projection gap/behind、budget/scope/fence失败、closure digest错误。

#### Idempotency/CAS
Projection幂等/cursor CAS；Explore fence CAS；materialization closure按sequence稳定。

#### Observability
MT proposal→M activation→edges/source records→watermark→Explore order→RSIH closure全链。

#### Acceptance fixture
Candidate不在Runtime；M有两条derived_from；A/B/M检索可见；watermark/bundle-lock输入可验证。

#### Non-goals
Curation Graph；source retirement；GMS生成RSIH bundle；learned reranking。
