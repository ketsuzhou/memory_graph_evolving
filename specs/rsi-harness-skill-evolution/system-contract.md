# RSI-Harness Skill Evolution System Contract

```yaml
document_status: normative
schema_version: rsih-skill-evolution.system-contract.v1
decision_baseline: Q1-Q37,M1-M9,U1-U2
language: zh-CN
```

> 本文是 `pi-group-chat-host`（Host）、`graph-memory-service`（GMS）与 `RSI-Harness`（RSIH）之间的规范性系统契约。三个模块规范必须引用本文的 DTO、状态机和不变量，不得重新定义共享协议。若模块文档、实现或注释与本文冲突，以本文为准，除非后续由显式版本化决策替代。

## 1. Status and normative language

### 1.1 规范状态

本文状态为 **Normative**，协议版本为 `rsih-skill-evolution.system-contract.v1`。实现声称符合 v1 时，必须满足本文所有标记为 MUST/MUST NOT 的条款及第 15、16 章的适用验收条件。

### 1.2 规范词

- **MUST / 必须**：符合性要求；违反即不符合本契约。
- **MUST NOT / 禁止**：绝对禁止。
- **SHOULD / 应当**：除非记录明确、可审计的例外理由，否则必须遵守。
- **SHOULD NOT / 不应**：仅在记录明确例外理由时允许。
- **MAY / 可以**：兼容性允许，但不得改变权威边界。
- **exact ref**：包含足以唯一定位不可变对象的身份、版本和摘要，禁止解析为“最新”。
- **protected**：位于 Agent、模型和 Genome 可写权限之外的确定性代码或受保护服务。

### 1.3 版本与兼容

1. Core schema 是封闭集合。未知 core 字段 MUST 被拒绝，除非对应 schema 明确允许。
2. 扩展只允许出现在 `extensions` 中，键 MUST 为反向域名或组织命名空间，例如 `ai.cosmosmind.foo`。
3. 未知 optional extension MAY 被保留或忽略；未知 required extension MUST fail closed。
4. 扩展 MUST NOT 改变身份、摘要、权限、权威归属、release 判定或 core 字段语义。
5. 历史 policy、schema、assessment、artifact 和事件版本 MUST 可按 exact ref 解析；禁止原地改写。

## 2. Goals and non-goals

### 2.1 目标

v1 的目标是形成最小、可审计、可重放的端到端闭环：

```text
Host sealed Segment
→ Skill proposal
→ canonical candidate validation
→ paired fixture replay
→ protected policy decision
→ exact activation
→ Runtime Skill Graph projection
→ Memory Explore retrieval
→ RSIH exact materialization
```

系统还 MUST 支持二元 `step_guidance` merge 扩展：

```text
SimilarityAssessment
→ MergeProposal
→ merge candidate
→ shared validate/replay/release pipeline
→ new derived lineage
```

### 2.2 成功性质

系统 MUST 提供：

- 可验证的 exact identity 与跨语言一致摘要；
- Host 封存的轨迹、checkpoint 与 evidence provenance；
- 模型提议和 protected 决策的严格分离；
- Candidate 与 Released artifact 的严格分离；
- deterministic paired replay 与版本化 release policy；
- append-only activation ledger 与可重建 Graph；
- 有 watermark 的 Skill 检索；
- content-addressed RSIH bundle 与 session closure freeze；
- merge 的双端 evidence、冲突、回归与 lineage 审计。

### 2.3 v1 非目标

以下不属于 v1：

- Data-RSI、Model-RSI、RSI² scheduler 或训练系统；
- OPD 训练及 reverse-KL 执行；
- 生产 embedding、learned similarity 或 learned ranker；
- 完整 probation 生命周期；
- 自动 retirement；
- Curation Graph 的物化；
- `procedure`、`composite` 或跨 kind merge；
- n-way merge、并行 merge tournament、stale proposal 自动 rebase；
- 任意循环 Composite；
- 让 Graph、Agent、Genome、模型或 RSIH 自行激活或评价 Skill。

## 3. System boundaries

### 3.1 Host 权威

Host MUST 唯一拥有：

- Room、Agent identity、Delivery 与消息顺序；
- Interaction DAG、Segment、Decision Checkpoint；
- Segment Evidence Seal、Conversation Path Seal；
- checkpoint 与 replay 调度；
- 本地 Tool Proxy 及返回给 Pi 的唯一 tool result；
- Memory Agent 的 Room shared Space 与普通 Agent 的 Host profile scope。

Host MUST NOT：

- 创建 canonical Skill version；
- 评价、release 或激活 Skill；
- 修改 GMS ledger；
- 用 Graph node ID 替代 exact artifact ref。

### 3.2 GMS 权威

GMS MUST 唯一拥有：

- committed evidence 与 assessment；
- canonical Skill artifact、candidate、lineage 与 active head；
- validation、replay result、utility、release rule 与 decision ledger；
- activation/deactivation ledger 与 outbox；
- SimilarityAssessment 与 MergeProposal ledger；
- Runtime/Curation projection stream/head；
- Guidance View 与 read-only Skill retrieval。

Graph 是 GMS 的派生读模型，不是 artifact 或 activation 权威。

### 3.3 RSIH 权威

RSIH MUST 唯一拥有：

- Genome loading 与 static validation；
- Pi live/replay execution；
- exact Skill materialization；
- content-addressed bundle、`SKILL.md` rendering 与 `skill.lock.json`；
- Composite 的 deterministic orchestration execution。

RSIH MUST NOT 自评、自激活、选择 release winner 或修改 GMS 权威记录。

### 3.4 模型与 Agent 权限

模型和 Agent MAY Diagnose/Propose。它们 MUST NOT 执行受保护的 Validate/Execute/Select/Export、evaluator、release rule、active-head CAS 或 ledger mutation。模型输出在 protected canonicalizer 接受前不是权威记录。

## 4. Domain terminology

| 术语 | 规范定义 |
|---|---|
| Room | Host 管理的多 Agent 对话边界。 |
| Segment | Room 内由 Host 划定并最终封存的一段有序交互。 |
| Decision Checkpoint | Segment 中可锚定 Skill guidance 的 Host-sealed 决策位置。 |
| Evidence Seal | Host 对证据集合、顺序和摘要的不可变封存。 |
| Conversation Path Seal | Host 对 success/failure/recovery 路径的不可变封存。 |
| Skill lineage | kind 不变、按 revision 演进的逻辑身份。 |
| Skill revision | lineage 中由正整数 version 与 body digest 唯一确定的不可变 artifact。 |
| Candidate | 尚未 release、不可作为 Runtime Skill 执行的不可变候选正文。 |
| Released artifact | 通过 protected release decision 后分配 `SkillArtifactRef` 的 artifact。 |
| Active head | activation ledger 对某 lineage 当前可执行 exact revision 的权威指针；可为空。 |
| Guidance View | 按 profile、context 和 budget 从 canonical artifact 派生的可验证读取视图。 |
| Runtime Graph | 由 activation/evidence/assessment 等权威记录异步投影的运行检索视图。 |
| Curation Graph | 面向候选、评审和治理的独立未来投影视图；v1 不物化。 |
| Projection watermark | 某 projection 已连续处理到的权威事件序号。 |
| SimilarityAssessment | 对两个 exact revisions 的版本化、不可变相似性判断。 |
| MergeProposal | 对两个 exact active stable `step_guidance` revisions 的不可变合并意图。 |
| Reference envelope | 按 fixture domain 选择 source A、source B 或 best-applicable source 构成的比较基线。 |
| Fixture family | 固定、封存且有 exact ref 的 replay case 集合。 |
| Served-item fence | ExploreSession 内防止重复或越权重供同一对象的确定性约束。 |

## 5. Global invariants

### 5.1 权威与不可变性

1. 模型只做 Diagnose/Propose；protected code MUST 完成 Validate/Execute/Select/Export。
2. Sealed tests、evaluator、`release_rule`、utility comparator、ledger 与 active-head CAS MUST 位于 Agent/Genome 写面之外。
3. 所有权威记录 MUST append-only；更正通过新记录或 compensating event 表达。
4. Candidate、Released artifact、evidence、assessment、seal、policy 与 event 均 MUST exact-addressed。
5. Execute/Select/Export MUST 拒绝 `latest`、裸名称和 Graph node ID。

### 5.2 Skill 不变量

1. Canonical kind 封闭为 `procedure|step_guidance|composite`。
2. lineage kind MUST 永不改变；跨 kind 转化 MUST 创建新 derived lineage。
3. Composite MUST 仅保存 exact children、DAG control flow、typed data flow、permissions、bounded retries 与 failure handling；MUST NOT 复制 child guidance 或伪造顶层 Step Guidance。
4. Composite child 更新 MUST 创建新 Composite revision 并重验，禁止 floating child update。
5. 作为 Composite child 的 Skill MUST 声明 named JSON-Schema ports。

### 5.3 Q32 八条 activation/projection 不变量

1. Activation ledger 与 active head 是运行状态的唯一权威。
2. Runtime Graph MUST 只来源于曾被 activation ledger 激活、或未来由显式 probation event 授权的 exact released revisions；v1 不实现 probation transition。
3. Graph MUST NOT 创建、替换、release、激活或停用 Skill。
4. Projector MUST 幂等、按单调 activation sequence 推进、可从 ledger 从零重建，并以 CAS 更新 projection head/watermark。
5. 每次 Graph/Explore 响应 MUST 返回 projection watermark。
6. 部署与 RSIH materialization MUST 从权威 active head/exact artifact 读取，不依赖 Graph 追平。
7. 要求强新鲜度的调用 MUST 提供 `min_activation_sequence`；未追平 MUST fail closed 或明确等待/重试，禁止静默降级。
8. Candidate、rejected、inconclusive 或未激活 artifact MUST NOT 出现在 Runtime Graph。

### 5.4 Runtime 历史节点解释

Runtime Graph MUST 保留所有曾进入 Runtime stream 的历史 exact revision node，包括 superseded/deactivated revision，以支持 lineage、Composite exact refs 与审计。默认运行检索 MUST 排除 deactivated revision；历史 exact lookup MAY 返回它们。节点不得因停用而物理删除。

### 5.5 Merge 不变量

1. v1 只允许两个当前 active、非 probation、released `step_guidance` 合并。
2. 高置信且无 blocking conflict MAY 自动进入 pipeline；中置信只创建 proposal 等待人工或额外 evidence；模型/人工均可请求，但均不得绕过 Gate。
3. 对称 merge MUST 创建新 derived lineage；不得原地修改 source。
4. Source A/B 在 merge release 后 MUST retained active；retirement 是独立 protected decision。
5. Activation 前任一 source active head 改变 MUST 使 proposal `stale`。
6. Candidate MUST 在 A/B critical slices 分别不回归，并相对 reference envelope 严格改善。
7. Similarity thresholds MUST 使用 exact versioned policy ref 与整数定标，禁止浮点数。
8. 同一 proposal group 同时只允许一个 admitted/in-flight winner。
9. `similar_to` 仅为 suggestion，不传递 identity、evidence、permission 或 activation。

### 5.6 Runtime 结果与预算

1. Evidence 与 Skill 结果 MUST 使用不同 result type 与独立 served-item fence，但共享 ExploreSession。
2. Artifact identity citation 与 evidence citation MUST 分离。
3. Explore MUST 使用 total cap、evidence subcap、Skill subcap 与 Guidance View token budget。
4. v1 ranking MUST deterministic lexical + graph；权重和 tie-break MUST versioned。

## 6. Identity, canonicalization and exact refs

### 6.1 Canonical bytes

1. Hashed core body MUST 是 UTF-8 JSON，无 BOM。
2. Canonicalization MUST 使用 RFC 8785 JSON Canonicalization Scheme（JCS）。
3. Digest MUST 为 canonical bytes 的 SHA-256，以 `sha256:` 加 64 位 lowercase hex 表示。
4. Hashed core MUST NOT 含非整数 JSON number。比例 MUST 以 `{numerator,denominator}` 或定标整数表达。
5. 数组顺序具有语义；无序集合在进入 core body 前 MUST 按各 schema 指定规则排序。
6. 时间戳、日志位置、展示名等非语义 audit metadata MUST 位于 envelope，不得改变 artifact body digest。

### 6.2 Exact ref 规则

`SkillArtifactRef` 的 exact identity 是 `(lineage_id, version, artifact_digest)`；`CandidateArtifactRef` 的 exact identity 是 `(candidate_id, body_digest)`。引用解析 MUST 同时校验所有字段，任一不一致 MUST 返回 `DIGEST_MISMATCH` 或 `REF_MISMATCH`。

#### §6.2.1 Materialization exact identity（修订 v1.1，CTR-001）

本小节为版本化修订条款：不重写 §7.19/§7.20 既有正文，而是在其上冻结 `SkillLock.materialization_ref` 的唯一 exact identity 映射。S8/MT6 的解禁以本小节决议为准。

**1. Manifest exact identity。** `SkillLock.materialization_ref`（§7.2 `VersionedRef` 形状）的 target 定义为 **manifest exact identity** 三元组 `(manifest_id, manifest_version, manifest_digest)`：

- `materialization_ref.id` = `materialization_id`；
- `materialization_ref.version` = `manifest_version`（由 materializer 按 `materialization_id` 单调分配的 manifest 级版本，独立于 skill version）；
- `materialization_ref.digest` = `manifest_digest = SHA-256(JCS(manifest_document))`。

**2. manifest_document 闭合 preimage。** `manifest_document` 是闭合的 manifest JSON（§6.1 JCS、integer-only），v1.1 必填字段全集：

```jsonc
{
  "schema_version": "rsih.materialization-manifest.v1",
  "materialization_id": "string<non-empty>",
  "manifest_version": "integer>=1",                      // v1.1 新增
  "skill": { "lineage_id": "...", "version": "integer>=1", "kind": "procedure|step_guidance|composite" }, // v1.1 新增：root skill 摘要
  "root_skill_refs": [{ /* SkillArtifactRef，v1 恰好 1 个，与 skill 一致 */ }],
  "transitive_skill_refs": [{ /* SkillArtifactRef，含 root，按 §6.3 排序 */ }],
  "render_profile_ref": { /* VersionedRef */ },
  "permission_profile_ref": { /* VersionedRef */ },
  "producer": { "producer_id": "...", "producer_version": "integer>=1" }, // v1.1 新增：生成方 identity
  "files": [{ "path": "string<relative normalized>", "size_bytes": "integer>=0", "content_digest": "sha256:..." }], // 按 path UTF-16 升序；v1.1 新增 size_bytes
  "file_count": "integer>=1",                             // v1.1 新增，MUST == len(files)
  "total_bytes": "integer>=0",                            // v1.1 新增，MUST == sum(files[].size_bytes)
  "bundle_digest": "sha256:...",                          // §7.19 既有：bundle 层 rollup，见下
  "created_from_activation_sequence": "integer>=1"
}
```

`files[].source_skill_ref`（§7.19 OPTIONAL）仍 OPTIONAL；`extensions` 按 §6.4。manifest 任一字段（含 optional 字段的出现/取值）变化 → `manifest_digest` 必变。

**3. 三层 identity 分工，互不替代。**

- **bundle bytes 层**：content-addressed 存储的实际文件。每文件 `content_digest = SHA-256(file bytes)`；`bundle_digest = SHA-256(JCS([{content_digest, path}] 按 path UTF-16 升序))`，即由文件清单可独立推导的 bundle rollup/CAS 地址。
- **manifest 层**：对整个 bundle 的闭合描述，`manifest_digest = SHA-256(JCS(manifest_document))`。`bundle_digest` 是 manifest_document 的字段之一，但两者角色不同。
- **lock 层**：`SkillLock` 引用 manifest exact identity（第 1 条三元组）+ freeze 时刻 active head sequence + session id；lock 自身有独立 `lock_digest = SHA-256(JCS(lock_document minus lock_digest))`，preimage 字段即 §7.20 全部字段去掉 `lock_digest` 自身。

`bundle_digest`（或任何 per-file `content_digest`）MUST NOT 作为 `materialization_ref.digest`，违反返回 `BUNDLE_DIGEST_NOT_MANIFEST_REF`。三层 digest 互不相等、互不替换；交叉验证顺序见第 6 条。

**4. 唯一性。** 同 `(manifest_id, manifest_version)` MUST 映射唯一 `manifest_digest`；观测到同 ref 不同 bytes（不同 `manifest_document`/`bundle_digest`）MUST 以 `SAME_REF_DIFFERENT_BYTES` 拒绝 publish/freeze。bundle 层同 address 不同 bytes 依 §7.20/RSIH 规范为 integrity conflict，同样禁 publish。

**5. Session freeze 与 cache lookup。** 同一 session 只能成功冻结一次；freeze 后不可重开，任何更新需新 session。lock 的 `activation_sequence_at_freeze` MUST equal manifest 的 `created_from_activation_sequence`（head 在 closure read 与 freeze 之间变化即撕裂，禁止 freeze），违反返回 `MANIFEST_SEQUENCE_STALE`。lock closure MUST 覆盖 manifest 声明的全部文件（逐文件以 `materialized_path`/`file_digest` 可寻址且 digest 相符），且不得含 manifest 未声明节点；违反返回 `LOCK_CLOSURE_INCOMPLETE`。cache lookup 命中 MUST 全量复核 `manifest_digest` 与每文件 `content_digest`，MUST NOT 只比对 lock 或只信 CAS 目录名。

**6. 交叉验证与 fail-closed。** 校验按确定性顺序 fail-closed：BOM/JSON → `UNKNOWN_REQUIRED_EXTENSION`（§6.4）→ `MANIFEST_VERSION_MISSING` → `MANIFEST_PARTIAL`（file_count/total_bytes/文件清单不一致、路径重复或未排序、root/transitive 不一致）→ `SAME_REF_DIFFERENT_BYTES` → bundle bytes 层 `DIGEST_MISMATCH`（per-file/bundle rollup）→ lock ref id/version `REF_MISMATCH`（§6.2）→ `BUNDLE_DIGEST_NOT_MANIFEST_REF` → `MANIFEST_DIGEST_MISMATCH`（`materialization_ref.digest != SHA-256(JCS(manifest_document))`，含 manifest 字段变化后的 stale ref）→ `MANIFEST_SEQUENCE_STALE` → `LOCK_CLOSURE_INCOMPLETE` → lock `DIGEST_MISMATCH`。缺 `manifest_version` 或 manifest digest、bundle/manifest digest 混用、partial manifest、stale sequence、closure 少节点、same ref different bytes、unknown required extension 均 MUST 禁 publish/freeze。

**7. 兼容结论（旧/新 schema）。** 无 `manifest_version` 的 §7.19 v1 形状 manifest 及引用它的历史 lock 一律分类 `legacy_unlocked`：可作历史审计读取，但 MUST fail-closed（`MANIFEST_VERSION_MISSING`），不得 publish/freeze/cache-serve，MUST NOT 自动升级或补写新字段；升级只能通过显式重新 materialization 产生新 manifest exact identity。

**8. Fixture。** 本条款由 `$FIX/materialization/`（子 manifest `materialization/manifest.json`、`schema/materialization-identity.schema.json`、`validate_materialization_identity.py`、`tests/test_materialization_identity.py`）固定；fixture 不编码 RSIH 私有 bundle 目录布局。

本条款新增 reason codes（`MANIFEST_VERSION_MISSING`、`BUNDLE_DIGEST_NOT_MANIFEST_REF`、`MANIFEST_DIGEST_MISMATCH`、`MANIFEST_PARTIAL`、`MANIFEST_SEQUENCE_STALE`、`LOCK_CLOSURE_INCOMPLETE`、`SAME_REF_DIFFERENT_BYTES`）在本修订内冻结，待 §13 系统 reason-code registry policy（CTR-004）统一收编。

### 6.3 ID 与排序

- ID MUST 为非空、大小写敏感、不可复用字符串。
- Version 与 sequence MUST 为正整数；`0` 只可用于明确声明的 initial watermark。
- 对称 source pair MUST 按 `(lineage_id UTF-8 bytes, version, artifact_digest)` 升序规范化。
- `source_pair_key` MUST 由规范化 exact refs 计算。

### 6.4 Extensions

```jsonc
{
  "extensions": {
    "ai.example.optional": { "required": false, "value": {} },
    "ai.example.required": { "required": true, "schema_ref": "...", "value": {} }
  }
}
```

Required extension 未识别、schema 不可解析或验证失败时 MUST fail closed。Extension MUST NOT 覆盖 core 字段。

## 7. Shared DTO registry

### 7.1 Schema 记法

本章 JSONC 是规范性 shape：

- `// REQUIRED` 表示字段必须存在；`// OPTIONAL` 表示可省略。
- `string<...>`、`integer>=...` 表示约束类型，不是字面值。
- 未列出的 core 字段禁止出现。
- 所有 `*_ref` 必须按 exact ref 解析。
- `extensions` 如出现必须符合 6.4。

### 7.2 Common value types

```jsonc
// Digest
"sha256:<64 lowercase hex>"

// VersionedRef
{
  "id": "string<non-empty>",                 // REQUIRED
  "version": "integer>=1",                  // REQUIRED
  "digest": "sha256:..."                    // REQUIRED
}

// Rational
{
  "numerator": "integer>=0",                // REQUIRED
  "denominator": "integer>=1"               // REQUIRED
}

// JsonSchemaRef
{
  "schema_id": "string<non-empty>",          // REQUIRED
  "version": "integer>=1",                  // REQUIRED
  "digest": "sha256:..."                    // REQUIRED
}
```

### 7.3 `SkillArtifactRef`

```jsonc
{
  "schema_version": "gms.skill-artifact-ref.v1", // REQUIRED
  "lineage_id": "string<non-empty>",             // REQUIRED
  "version": "integer>=1",                       // REQUIRED
  "kind": "procedure|step_guidance|composite",   // REQUIRED
  "artifact_digest": "sha256:..."                // REQUIRED
}
```

### 7.4 `CandidateArtifactRef`

```jsonc
{
  "schema_version": "gms.candidate-artifact-ref.v1", // REQUIRED
  "candidate_id": "string<non-empty>",                // REQUIRED
  "kind": "procedure|step_guidance|composite",        // REQUIRED
  "body_digest": "sha256:...",                        // REQUIRED
  "origin_type": "skill_proposal|merge_proposal",     // REQUIRED
  "origin_ref": { "id": "string", "version": "integer>=1", "digest": "sha256:..." } // REQUIRED
}
```

CandidateRef MUST NOT 包含 released lineage version，也不得被 Runtime 执行。

### 7.5 `SegmentRef`

```jsonc
{
  "schema_version": "host.segment-ref.v1", // REQUIRED
  "room_id": "string<non-empty>",          // REQUIRED
  "segment_id": "string<non-empty>",       // REQUIRED
  "segment_version": "integer>=1",         // REQUIRED
  "segment_digest": "sha256:...",          // REQUIRED
  "evidence_seal_ref": { "id": "string", "version": "integer>=1", "digest": "sha256:..." }, // REQUIRED
  "path_seal_ref": { "id": "string", "version": "integer>=1", "digest": "sha256:..." }      // REQUIRED
}
```

### 7.6 `CheckpointRef`

```jsonc
{
  "schema_version": "host.checkpoint-ref.v1", // REQUIRED
  "room_id": "string<non-empty>",             // REQUIRED
  "segment_ref": { /* SegmentRef */ },         // REQUIRED
  "checkpoint_id": "string<non-empty>",       // REQUIRED
  "checkpoint_sequence": "integer>=1",        // REQUIRED
  "checkpoint_digest": "sha256:..."           // REQUIRED
}
```

### 7.7 `EvidenceRef`

```jsonc
{
  "schema_version": "gms.evidence-ref.v1", // REQUIRED
  "evidence_id": "string<non-empty>",      // REQUIRED
  "version": "integer>=1",                 // REQUIRED
  "evidence_digest": "sha256:...",         // REQUIRED
  "commit_state": "committed|sealed",      // REQUIRED
  "evidence_kind": "success_path|failure_path|recovery_path|observation|replay_result|human_attestation", // REQUIRED
  "source_segment_ref": { /* SegmentRef */ } // OPTIONAL
}
```

Runtime Graph relations MUST NOT 引用 staged/uncommitted evidence。

### 7.8 `SkillAnchor`

```jsonc
{
  "schema_version": "gms.skill-anchor.v1", // REQUIRED
  "anchor_id": "string<non-empty>",        // REQUIRED
  "skill_ref": { /* SkillArtifactRef */ },  // REQUIRED
  "checkpoint_ref": { /* CheckpointRef */ },// REQUIRED
  "anchor_digest": "sha256:...",           // REQUIRED
  "created_from_ref": { "id": "string", "version": "integer>=1", "digest": "sha256:..." } // REQUIRED
}
```

### 7.9 `SkillProposal`

```jsonc
{
  "schema_version": "gms.skill-proposal.v1", // REQUIRED
  "proposal_id": "string<non-empty>",        // REQUIRED
  "proposal_version": "integer>=1",          // REQUIRED
  "proposal_digest": "sha256:...",           // REQUIRED
  "proposed_kind": "procedure|step_guidance|composite", // REQUIRED
  "source_segment_refs": [{ /* SegmentRef */ }],          // REQUIRED, minItems=1
  "checkpoint_refs": [{ /* CheckpointRef */ }],           // OPTIONAL
  "evidence_refs": [{ /* EvidenceRef */ }],               // REQUIRED, minItems=1
  "parent_skill_refs": [{ /* SkillArtifactRef */ }],       // OPTIONAL
  "requested_operation": "create_lineage|revise_lineage|derive_lineage|bind_composite", // REQUIRED
  "origin": {
    "initiator_type": "automatic|model|human",            // REQUIRED
    "initiator_ref": "string<non-empty>",                 // REQUIRED
    "request_ref": "string<non-empty>"                    // REQUIRED
  },
  "policy_refs": [{ /* VersionedRef */ }],                 // REQUIRED
  "extensions": {}                                        // OPTIONAL
}
```

### 7.10 `ReplayRequest`

```jsonc
{
  "schema_version": "gms.replay-request.v1", // REQUIRED
  "replay_request_id": "string<non-empty>",  // REQUIRED
  "candidate_ref": { /* CandidateArtifactRef */ }, // REQUIRED
  "baseline_skill_refs": [{ /* SkillArtifactRef */ }], // REQUIRED, minItems=1
  "fixture_set_refs": [{ /* VersionedRef */ }],        // REQUIRED, minItems=1
  "segment_refs": [{ /* SegmentRef */ }],              // REQUIRED, minItems=1
  "replay_profile_ref": { /* VersionedRef */ },        // REQUIRED
  "runtime_adapter_ref": { /* VersionedRef */ },       // REQUIRED
  "mode": "causal_evaluation",                        // REQUIRED in v1
  "idempotency_key": "sha256:...",                    // REQUIRED
  "required_source_heads": [{ /* SkillArtifactRef */ }], // OPTIONAL, merge REQUIRED
  "extensions": {}                                     // OPTIONAL
}
```

### 7.11 `ReplayResult`

```jsonc
{
  "schema_version": "gms.replay-result.v1", // REQUIRED
  "replay_result_id": "string<non-empty>",  // REQUIRED
  "replay_request_ref": { /* VersionedRef */ }, // REQUIRED
  "candidate_ref": { /* CandidateArtifactRef */ }, // REQUIRED
  "status": "succeeded|failed|inconclusive",      // REQUIRED
  "fixture_outcomes": [{
    "fixture_ref": { /* VersionedRef */ },          // REQUIRED
    "domain": "source_a|source_b|overlap|general", // REQUIRED
    "baseline_ref": { /* SkillArtifactRef */ },     // REQUIRED
    "baseline_passed": "integer>=0",               // REQUIRED
    "candidate_passed": "integer>=0",              // REQUIRED
    "total_cases": "integer>=1",                   // REQUIRED
    "critical_regression_count": "integer>=0",     // REQUIRED
    "output_digest": "sha256:..."                  // REQUIRED
  }],                                                // REQUIRED, minItems=1
  "utility_vector": {
    "task_success_count": "integer>=0",            // REQUIRED
    "critical_branch_pass_count": "integer>=0",    // REQUIRED
    "recovery_success_count": "integer>=0",        // REQUIRED
    "inconclusive_case_count": "integer>=0",       // REQUIRED
    "execution_cost_units": "integer>=0"           // REQUIRED
  },
  "failure_reason_code": "string<closed-enum>",     // OPTIONAL
  "result_digest": "sha256:...",                    // REQUIRED
  "extensions": {}                                  // OPTIONAL
}
```

### 7.12 `ReleaseDecision`

```jsonc
{
  "schema_version": "gms.release-decision.v1", // REQUIRED
  "decision_id": "string<non-empty>",          // REQUIRED
  "decision_version": "integer>=1",            // REQUIRED
  "decision_digest": "sha256:...",             // REQUIRED
  "candidate_ref": { /* CandidateArtifactRef */ }, // REQUIRED
  "validation_record_refs": [{ /* VersionedRef */ }], // REQUIRED
  "replay_result_refs": [{ /* VersionedRef */ }],      // REQUIRED
  "release_rule_ref": { /* VersionedRef */ },          // REQUIRED
  "utility_comparator_ref": { /* VersionedRef */ },    // REQUIRED
  "hard_gate_results": [{
    "gate_code": "string<closed-enum>",               // REQUIRED
    "passed": "boolean",                              // REQUIRED
    "record_refs": [{ /* VersionedRef */ }]            // OPTIONAL
  }],                                                   // REQUIRED
  "outcome": "accepted|rejected|inconclusive",        // REQUIRED
  "reason_codes": ["string<closed-enum>"],            // REQUIRED
  "source_head_expectations": [{ /* SkillArtifactRef */ }], // OPTIONAL
  "extensions": {}                                    // OPTIONAL
}
```

### 7.13 `ActivationEvent`

```jsonc
{
  "schema_version": "gms.activation-event.v1", // REQUIRED
  "activation_sequence": "integer>=1",         // REQUIRED, globally monotonic per stream
  "event_id": "string<non-empty>",             // REQUIRED
  "event_digest": "sha256:...",                // REQUIRED
  "event_type": "activate|deactivate",         // REQUIRED
  "lineage_id": "string<non-empty>",           // REQUIRED
  "skill_ref": { /* SkillArtifactRef */ },       // REQUIRED
  "previous_active_ref": { /* SkillArtifactRef */ }, // OPTIONAL
  "release_decision_ref": { /* VersionedRef */ },    // REQUIRED for activate
  "deactivation_authorization_ref": { /* VersionedRef */ }, // REQUIRED for deactivate
  "candidate_ref": { /* CandidateArtifactRef */ },    // REQUIRED for first activation of released revision
  "body_digest_equal": "boolean",                    // REQUIRED for activate, MUST be true
  "derived_from_refs": [{ /* SkillArtifactRef */ }],  // OPTIONAL
  "expected_active_head": { /* SkillArtifactRef */ }, // OPTIONAL; absence means expected null
  "outbox_key": "sha256:...",                        // REQUIRED
  "extensions": {}                                    // OPTIONAL
}
```

Exactly one of `release_decision_ref` and `deactivation_authorization_ref` MUST be present according to `event_type`。

### 7.14 `ProjectionWatermark`

```jsonc
{
  "schema_version": "gms.projection-watermark.v1", // REQUIRED
  "projection_stream": "runtime|curation",         // REQUIRED
  "projection_schema_version": "string<non-empty>",// REQUIRED
  "projection_head": "string<non-empty>",          // REQUIRED
  "projected_through_activation_sequence": "integer>=0", // REQUIRED
  "source_ledger_digest": "sha256:...",             // REQUIRED
  "state": "uninitialized|catching_up|current|blocked|rebuilding", // REQUIRED
  "watermark_digest": "sha256:..."                  // REQUIRED
}
```

### 7.15 `GuidanceView`

```jsonc
{
  "schema_version": "gms.guidance-view.v1", // REQUIRED
  "source_skill_ref": { /* SkillArtifactRef */ }, // REQUIRED
  "render_profile_ref": { /* VersionedRef */ },   // REQUIRED
  "policy_ref": { /* VersionedRef */ },           // REQUIRED
  "runtime_context_hash": "sha256:...",           // REQUIRED
  "included_branch_refs": ["string<exact branch id>"], // REQUIRED
  "omitted_branch_refs": ["string<exact branch id>"],  // REQUIRED
  "expandable_refs": [{
    "ref_type": "branch|evidence|checkpoint|child_skill", // REQUIRED
    "ref": {}                                             // REQUIRED, matching exact DTO
  }],
  "content": "string<rendered guidance>",            // REQUIRED
  "content_token_count": "integer>=0",               // REQUIRED
  "truncated": "boolean",                             // REQUIRED
  "view_hash": "sha256:..."                           // REQUIRED
}
```

【修订 v1.1，CTR-002】`GuidanceView` 作为 `skill_get` success result 的验证范围以 §12.7.1 matrix 为准：只验证 closed schema 适用字段；禁止为 freshness/authorization 向本 DTO 私添 watermark/budgets/citations 等字段。

### 7.16 `ExploreResult`

```jsonc
{
  "schema_version": "gms.explore-result.v1", // REQUIRED
  "explore_session_id": "string<non-empty>", // REQUIRED
  "query_digest": "sha256:...",              // REQUIRED
  "ranker_policy_ref": { /* VersionedRef */ }, // REQUIRED
  "watermark": { /* ProjectionWatermark */ },  // REQUIRED
  "min_activation_sequence": "integer>=0",    // OPTIONAL
  "evidence_results": [{
    "result_type": "evidence",                // REQUIRED
    "evidence_ref": { /* EvidenceRef */ },      // REQUIRED
    "rank_score_micros": "integer",            // REQUIRED
    "citation": { "evidence_ref": { /* EvidenceRef */ }, "claim": "string" } // REQUIRED
  }],                                           // REQUIRED
  "skill_results": [{
    "result_type": "skill",                   // REQUIRED
    "skill_ref": { /* SkillArtifactRef */ },    // REQUIRED
    "rank_score_micros": "integer",            // REQUIRED
    "guidance_view": { /* GuidanceView */ },    // REQUIRED
    "artifact_identity_citation": { "skill_ref": { /* SkillArtifactRef */ } }, // REQUIRED
    "evidence_citations": [{ "evidence_ref": { /* EvidenceRef */ }, "claim": "string" }] // REQUIRED
  }],                                           // REQUIRED
  "served_fences": {
    "evidence_fence_digest": "sha256:...",     // REQUIRED
    "skill_fence_digest": "sha256:..."         // REQUIRED
  },
  "budgets": {
    "total_cap": "integer>=0",                 // REQUIRED
    "evidence_subcap": "integer>=0",           // REQUIRED
    "skill_subcap": "integer>=0",              // REQUIRED
    "guidance_token_budget": "integer>=0",      // REQUIRED
    "total_used": "integer>=0"                  // REQUIRED
  },
  "truncation_reason_codes": ["string<closed-enum>"] // REQUIRED
}
```

【修订 v1.1，CTR-002】`ExploreResult` 作为 `memory_explore`/`memory_expand` success result 的 watermark/budgets/citations 验证义务以 §12.7.1 matrix（explore-family 规则集）为准；top-level omitted refs 的字段形态由 CTR-003 后续冻结。

### 7.17 `ToolProxyRequest`

```jsonc
{
  "schema_version": "host.tool-proxy-request.v1", // REQUIRED
  "proxy_request_id": "string<non-empty>",        // REQUIRED
  "room_id": "string<non-empty>",                 // REQUIRED
  "agent_id": "string<non-empty>",                // REQUIRED
  "delivery_id": "string<non-empty>",             // REQUIRED
  "tool_name": "memory_explore|memory_expand|skill_get", // REQUIRED in v1
  "arguments": {},                                 // REQUIRED
  "scope_profile_ref": { /* VersionedRef */ },     // REQUIRED
  "idempotency_key": "sha256:...",                // REQUIRED
  "requested_min_activation_sequence": "integer>=0", // OPTIONAL
  "timeout_millis": "integer>=1"                  // REQUIRED
}
```

【修订 v1.1，CTR-002】freshness 与 authorization 由 request 字段（`requested_min_activation_sequence`、`room_id`、`agent_id`、`scope_profile_ref`）与权威 read audit 记录承载（§12.7.1 M4/M5），不进入 closed result DTO。

### 7.18 `ToolProxyResult`

```jsonc
{
  "schema_version": "host.tool-proxy-result.v1", // REQUIRED
  "proxy_request_id": "string<non-empty>",       // REQUIRED
  "status": "succeeded|failed|inconclusive",     // REQUIRED
  "result": {},                                   // REQUIRED when succeeded
  "error": {
    "reason_code": "string<closed-enum>",         // REQUIRED when failed/inconclusive
    "message": "string",                          // REQUIRED
    "retryable": "boolean",                       // REQUIRED
    "record_refs": [{ /* VersionedRef */ }]        // OPTIONAL
  },
  "upstream_result_digest": "sha256:...",         // REQUIRED
  "proxy_result_digest": "sha256:..."             // REQUIRED
}
```

Pi MUST 接收此 exact proxy result 作为唯一 tool result；后置旁路调用不得替代或补写 Pi 所见结果。

【修订 v1.1，CTR-002】success `result` 的 tool-specific validation matrix 见 §12.7.1（权威 policy：conformance `policy/tool-success-validation.v1.json`）；wrapper/free JSON 一律 fail closed。

### 7.19 `MaterializationManifest`

```jsonc
{
  "schema_version": "rsih.materialization-manifest.v1", // REQUIRED
  "materialization_id": "string<non-empty>",            // REQUIRED
  "root_skill_refs": [{ /* SkillArtifactRef */ }],        // REQUIRED, minItems=1
  "transitive_skill_refs": [{ /* SkillArtifactRef */ }],  // REQUIRED
  "render_profile_ref": { /* VersionedRef */ },           // REQUIRED
  "permission_profile_ref": { /* VersionedRef */ },       // REQUIRED
  "files": [{
    "path": "string<relative normalized path>",           // REQUIRED
    "content_digest": "sha256:...",                      // REQUIRED
    "source_skill_ref": { /* SkillArtifactRef */ }        // OPTIONAL
  }],                                                      // REQUIRED
  "bundle_digest": "sha256:...",                         // REQUIRED
  "created_from_activation_sequence": "integer>=1"       // REQUIRED
}
```

### 7.20 `SkillLock`

```jsonc
{
  "schema_version": "rsih.skill-lock.v1", // REQUIRED
  "session_id": "string<non-empty>",      // REQUIRED
  "materialization_ref": { /* VersionedRef */ }, // REQUIRED
  "root_skill_refs": [{ /* SkillArtifactRef */ }], // REQUIRED
  "locked_closure": [{
    "skill_ref": { /* SkillArtifactRef */ }, // REQUIRED
    "artifact_digest": "sha256:...",        // REQUIRED
    "materialized_path": "string<relative normalized path>", // REQUIRED
    "file_digest": "sha256:..."             // REQUIRED
  }],                                         // REQUIRED
  "activation_sequence_at_freeze": "integer>=1", // REQUIRED
  "lock_digest": "sha256:..."               // REQUIRED
}
```

Session 开始后 closure MUST 冻结；Graph 或 active head 更新不得静默改变该 session。

#### §7.20.1 materialization_ref 与 lock_digest 绑定（修订 v1.1，CTR-001）

本 DTO 的 `materialization_ref` 按 §6.2.1 冻结为 manifest exact identity `(materialization_id, manifest_version, manifest_digest)`，`digest` 严禁取 `bundle_digest`；`lock_digest` preimage 为本 DTO 全部字段去掉 `lock_digest` 自身；`activation_sequence_at_freeze` 与 `locked_closure` 约束以 §6.2.1 第 5 条为准。§7.19 manifest document 的 v1.1 闭合字段集与兼容结论（`legacy_unlocked`）同样以 §6.2.1 为准。

### 7.21 `SimilarityAssessment`

```jsonc
{
  "schema_version": "gms.similarity-assessment.v1", // REQUIRED
  "assessment_id": "string<non-empty>",             // REQUIRED
  "assessment_version": "integer>=1",               // REQUIRED
  "assessment_digest": "sha256:...",                // REQUIRED
  "source_skill_refs": [{ /* SkillArtifactRef */ }, { /* SkillArtifactRef */ }], // REQUIRED, exactly 2, canonical order
  "policy_ref": { /* VersionedRef */ },              // REQUIRED
  "feature_record_refs": [{ /* VersionedRef */ }],   // REQUIRED
  "score_micros": "integer>=0",                     // REQUIRED
  "band": "below_suggestion|suggestion_only|merge_review|auto_merge_eligible", // REQUIRED
  "compatibility": {
    "kind_compatible": "boolean",                   // REQUIRED
    "applicability_overlap": "none|partial|complete|unknown", // REQUIRED
    "permission_compatible": "boolean",             // REQUIRED
    "port_compatible": "boolean|not_applicable",    // REQUIRED
    "blocking_conflict_count": "integer>=0"          // REQUIRED
  },
  "supporting_evidence_refs": [{ /* EvidenceRef */ }],// REQUIRED
  "refuting_evidence_refs": [{ /* EvidenceRef */ }], // REQUIRED
  "assessor_ref": { /* VersionedRef */ },            // REQUIRED
  "extensions": {}                                   // OPTIONAL
}
```

### 7.22 `MergeProposal`

```jsonc
{
  "schema_version": "gms.merge-proposal.v1", // REQUIRED
  "proposal_id": "string<non-empty>",        // REQUIRED
  "proposal_version": "integer>=1",          // REQUIRED
  "proposal_digest": "sha256:...",           // REQUIRED
  "source_skill_refs": [{ /* SkillArtifactRef */ }, { /* SkillArtifactRef */ }], // REQUIRED, exactly 2 canonical order
  "similarity_assessment_refs": [{ /* VersionedRef */ }], // REQUIRED, minItems=1
  "source_evidence_sets": [{
    "source_skill_ref": { /* SkillArtifactRef */ }, // REQUIRED
    "supporting_evidence_refs": [{ /* EvidenceRef */ }], // REQUIRED
    "refuting_evidence_refs": [{ /* EvidenceRef */ }],  // REQUIRED
    "success_path_refs": [{ /* VersionedRef */ }],      // REQUIRED
    "failure_path_refs": [{ /* VersionedRef */ }],      // REQUIRED
    "recovery_path_refs": [{ /* VersionedRef */ }],     // REQUIRED
    "branch_claim_refs": [{ /* VersionedRef */ }]       // REQUIRED
  }],                                                    // REQUIRED, exactly 2
  "joint_evidence": {
    "overlap_evidence_refs": [{ /* EvidenceRef */ }],   // REQUIRED
    "divergence_evidence_refs": [{ /* EvidenceRef */ }],// REQUIRED
    "shared_anchor_refs": [{ /* SkillAnchor */ }],      // REQUIRED
    "paired_fixture_refs": [{ /* VersionedRef */ }]     // REQUIRED
  },                                                     // REQUIRED
  "merge_intent": {
    "target_kind": "step_guidance",                   // REQUIRED in v1
    "strategy": "symmetric_new_lineage",              // REQUIRED in v1
    "normalized_applicability_key": "sha256:...",      // REQUIRED
    "source_disposition": "retain",                    // REQUIRED in v1
    "required_branch_preservation": true                // REQUIRED in v1
  },
  "conflicts": [{
    "conflict_id": "string<non-empty>",                // REQUIRED
    "category": "causal_context_divergence|applicability_mismatch|contradictory_action|outcome_divergence|future_path_divergence|permission_mismatch|port_schema_mismatch|branch_identity_collision|evidence_contradiction|unknown", // REQUIRED
    "semantic_location": "string<JSON Pointer or semantic path>", // REQUIRED
    "source_a_claim_ref": { /* VersionedRef */ },       // REQUIRED
    "source_b_claim_ref": { /* VersionedRef */ },       // REQUIRED
    "applicability_overlap": "disjoint|partial|complete|unknown", // REQUIRED
    "resolution": {
      "action": "preserve_as_branches|narrow_applicability|prefer_source|request_human|unresolved", // REQUIRED
      "selected_source_ref": { /* SkillArtifactRef */ }, // OPTIONAL, required for prefer_source
      "predicate": {},                                  // OPTIONAL
      "evidence_refs": [{ /* EvidenceRef */ }],         // REQUIRED
      "rationale": "string"                            // REQUIRED
    },
    "blocking": "boolean"                              // REQUIRED
  }],                                                    // REQUIRED
  "policies": {
    "similarity_policy_ref": { /* VersionedRef */ },    // REQUIRED
    "merge_policy_ref": { /* VersionedRef */ },         // REQUIRED
    "validation_profile_ref": { /* VersionedRef */ },  // REQUIRED
    "replay_profile_ref": { /* VersionedRef */ },      // REQUIRED
    "release_rule_ref": { /* VersionedRef */ },        // REQUIRED
    "utility_comparator_ref": { /* VersionedRef */ }   // REQUIRED
  },
  "dedup": {
    "source_pair_key": "sha256:...",                  // REQUIRED
    "proposal_group_key": "sha256:...",               // REQUIRED
    "intent_key": "sha256:..."                        // REQUIRED
  },
  "origin": {
    "initiator_type": "automatic|model|human",        // REQUIRED
    "initiator_ref": "string<non-empty>",             // REQUIRED
    "request_ref": "string<non-empty>"                // REQUIRED
  },
  "prior_proposal_refs": [{ /* VersionedRef */ }],     // REQUIRED
  "extensions": {}                                    // OPTIONAL
}
```

### 7.23 `MergeProposalEvent`

```jsonc
{
  "schema_version": "gms.merge-proposal-event.v1", // REQUIRED
  "proposal_ref": { /* VersionedRef */ },           // REQUIRED
  "event_sequence": "integer>=1",                  // REQUIRED, monotonic per proposal
  "event_id": "string<non-empty>",                 // REQUIRED
  "event_digest": "sha256:...",                    // REQUIRED
  "from_state": "none|proposed|admitted|synthesizing|candidate_bound|validating|replaying|decision_pending|activation_pending", // REQUIRED
  "to_state": "proposed|admitted|synthesizing|candidate_bound|validating|replaying|decision_pending|activation_pending|released|duplicate|rejected|inconclusive|stale|withdrawn", // REQUIRED
  "reason_codes": ["string<closed-enum>"],          // REQUIRED
  "authority_ref": { /* VersionedRef */ },          // REQUIRED
  "record_refs": [{ /* VersionedRef or exact DTO ref */ }], // REQUIRED
  "observed_source_heads": [{ /* SkillArtifactRef */ }],    // REQUIRED
  "expected_previous_event_sequence": "integer>=0"         // REQUIRED
}
```

状态只能通过受保护 CAS 追加此事件推进；终态不得重新打开。

## 8. Canonical Skill artifact model

### 8.1 通用 envelope

```jsonc
{
  "schema_version": "gms.skill-artifact.v1", // REQUIRED
  "kind": "procedure|step_guidance|composite", // REQUIRED
  "title": "string<non-empty>",              // REQUIRED
  "description": "string",                   // REQUIRED
  "applicability": {
    "predicates": [{}],                       // REQUIRED
    "exclusions": [{}]                        // REQUIRED
  },
  "permissions": [{ "capability": "string", "scope": "string" }], // REQUIRED
  "body": {},                                 // REQUIRED, kind-specific
  "extensions": {}                           // OPTIONAL
}
```

Lineage/version 不进入 canonical body；它们由 released ref/ledger 分配。Body digest 在 candidate 与 released artifact 间 MUST 相等。

### 8.2 `procedure`

Procedure 描述稳定操作流程，不得伪造 checkpoint-derived Causal Context 或 Future Path Summary。

```jsonc
{
  "steps": [{
    "step_id": "string<unique>",              // REQUIRED
    "instruction": "string<non-empty>",       // REQUIRED
    "preconditions": [{}],                     // REQUIRED
    "postconditions": [{}],                    // REQUIRED
    "failure_action": "stop|skip|fallback|request_human|mark_inconclusive" // REQUIRED
  }],                                           // REQUIRED, minItems=1
  "input_port_schema": { /* JsonSchemaRef */ },// OPTIONAL, REQUIRED if used as Composite child
  "output_port_schema": { /* JsonSchemaRef */ }// OPTIONAL, REQUIRED if used as Composite child
}
```

### 8.3 `step_guidance`

Step Guidance MUST 提供因果相关先验，以及一个或多个原子的条件—行动—未来路径 branches。每个 branch 的 `when`、`action`、`future` 必须同存；禁止用独立的 decision collection 与 future summary 隐式关联。

```jsonc
{
  "causal_context": {
    "summary": "string<non-empty>",
    "claim_refs": [{ /* VersionedRef */ }]
  },
  "branches": [{
    "branch_id": "string<unique>",
    "when": {},
    "action": {
      "guidance": "string<non-empty>",
      "failure_action": "stop|skip|fallback|request_human|mark_inconclusive",
      "evidence_refs": [{ /* EvidenceRef */ }]
    },
    "future": {
      "expected_outcome": "string",
      "critical_steps": [{
        "step_id": "string<unique>",
        "condition": {},
        "task_impact": "string<non-empty>"
      }],
      "final_task_impact": "string<non-empty>"
    }
  }],
  "input_port_schema": { /* JsonSchemaRef */ },
  "output_port_schema": { /* JsonSchemaRef */ }
}
```

成功、失败和恢复 branches MUST 可同时保留。每个 branch 的 future critical steps 保存在该 branch 内，而不是独立 Graph node 或共享 future summary。

### 8.4 `composite`

```jsonc
{
  "children": [{
    "child_id": "string<unique>",              // REQUIRED
    "skill_ref": { /* SkillArtifactRef */ },    // REQUIRED, active at Composite release
    "input_port": "string<name>",              // REQUIRED
    "output_port": "string<name>"              // REQUIRED
  }],                                            // REQUIRED, minItems=1
  "edges": [{
    "from_child_id": "string",                 // REQUIRED
    "from_port": "string",                     // REQUIRED
    "to_child_id": "string",                   // REQUIRED
    "to_port": "string",                       // REQUIRED
    "mapping": {}                               // REQUIRED
  }],                                            // REQUIRED, acyclic
  "retry_policies": [{
    "child_id": "string",                     // REQUIRED
    "max_attempts": "integer>=1",              // REQUIRED
    "retryable_reason_codes": ["string<closed-enum>"], // REQUIRED
    "backoff_units": "integer>=0"              // REQUIRED
  }],
  "failure_handlers": [{
    "child_id": "string",                     // REQUIRED
    "reason_codes": ["string<closed-enum>"],   // REQUIRED
    "action": "stop|skip|fallback|request_human|mark_inconclusive", // REQUIRED
    "fallback_child_id": "string"              // OPTIONAL, required for fallback
  }],
  "orchestration_permissions": [{ "capability": "string", "scope": "string" }], // REQUIRED
  "input_port_schema": { /* JsonSchemaRef */ }, // REQUIRED
  "output_port_schema": { /* JsonSchemaRef */ } // REQUIRED
}
```

Composite permissions = union(children permissions) + orchestration permissions，并 MUST 被 Host authority cap 限制。Composite MUST 无环；循环只允许由 bounded retry 表达。每个 child 在 Composite release 前 MUST 已激活。Composite 更新任何 child ref 都 MUST 产生新 revision、static validation 与 whole-Composite paired replay。

## 9. End-to-end state machines

所有状态是 append-only records 的派生视图，不允许原地覆盖历史。

### 9.1 Skill proposal → candidate → release

```text
proposed
  → admitted
  → candidate_bound
  → validating
  → replay_pending
  → replaying
  → decision_pending
  → activation_pending
  → released
```

终态还包括 `rejected|inconclusive|stale|withdrawn`。

| 状态 | 进入条件 | 合法退出 |
|---|---|---|
| proposed | exact seals/evidence、kind、operation 与 policy refs 已提交 | admitted/rejected/stale/withdrawn |
| admitted | protected admission 验证权限、source refs、kind 与 dedup | candidate_bound/rejected/stale/withdrawn |
| candidate_bound | canonicalizer 写入唯一 CandidateRef | validating/stale |
| validating | validator lease 与 exact input packet 固定 | replay_pending/rejected/stale |
| replay_pending | static/hard schema gates 全过 | replaying/stale |
| replaying | ReplayRequest 已登记且 runtime adapter 执行 | decision_pending/rejected/inconclusive/stale |
| decision_pending | 完整 ReplayResult 可供 protected evaluator | activation_pending/rejected/inconclusive |
| activation_pending | decision accepted，等待 active-head CAS | released/rejected/stale |
| released | CandidateRef→ReleasedRef mapping 与 activation event 已原子提交 | 终态；后续运行变化由 activation event 表达 |

`withdrawn` 只允许在 candidate 进入 protected validation 前。任何新尝试 MUST 创建新 proposal/candidate，不得重开终态。

### 9.2 Activation/deactivation

Artifact lifecycle：

```text
candidate → released
```

Runtime state：

```text
(no active head) --activate--> active
active(old) --activate(new same lineage)--> old:superseded, new:active
active --deactivate--> deactivated + active head cleared
```

规范条件：

1. `activate` MUST 引用 accepted ReleaseDecision、CandidateRef、ReleasedRef，并满足 `body_digest_equal=true`。
2. 新 revision activation MUST 以 expected active head 做 CAS；成功后产生同 lineage `new --supersedes--> old`。
3. `deactivate` MUST 引用 protected deactivation authorization，并以当前 head 做 CAS。
4. Deactivation MUST NOT 删除 artifact、event 或 Graph node。
5. 默认 Runtime retrieval MUST 排除 deactivated revision。
6. v1 ledger event type 仅 `activate|deactivate`。`probation` 可作为保留枚举，但 MUST NOT 被 v1 实现伪造为可达状态。
7. 如需重新激活历史 released ref，MUST 有新的 protected activation authorization/release decision，并写新的 `activate` event；禁止直接修改旧 event。

### 9.3 Projector

```text
uninitialized → catching_up → current
                         ↘ blocked
current → catching_up (new events)
blocked → catching_up (operator/protected retry)
any → rebuilding → catching_up → current
```

- `uninitialized`：无 projection head，watermark=0。
- `catching_up`：按 `activation_sequence` 连续消费；有 gap 时 MUST 停止推进。
- `current`：已处理到消费时已知 ledger head。
- `blocked`：schema、digest、gap 或不可恢复 projection error；MUST 不越过失败 sequence。
- `rebuilding`：从权威 ledger 创建新 projection head；旧 head MAY 继续只读服务并显式暴露旧 watermark。

每个事件投影 MUST 使用幂等键 `(projection_stream, projection_schema_version, activation_sequence, event_digest)`。重复事件结果 MUST 相同；同 sequence 不同 digest MUST `PROJECTION_EVENT_CONFLICT`。Graph mutation 与 watermark CAS MUST 原子提交到同一 projection head。

### 9.4 MergeProposal 生命周期

```text
proposed
 → admitted
 → synthesizing
 → candidate_bound
 → validating
 → replaying
 → decision_pending
 → activation_pending
 → released
```

任意适用状态可终止为 `duplicate|rejected|inconclusive|stale|withdrawn`；终态不可重开。

#### 9.4.1 `proposed`

进入必须有 exact SimilarityAssessment、两个规范排序的 source refs、双端 evidence 与 versioned policies。退出：admission 通过→`admitted`；exact duplicate→`duplicate`；不兼容→`rejected`；head/policy 失效→`stale`；admission 前合法撤回→`withdrawn`。

#### 9.4.2 `admitted`

两个 sources MUST 为当前 active、非 probation、released `step_guidance`。Assessment MUST 达到 policy band；kind/applicability/permission/ports MUST 兼容；同 group MUST 通过 CAS 获得唯一 in-flight winner。退出：获得 synthesis lease→`synthesizing`；否则 rejected/stale/withdrawn。

#### 9.4.3 `synthesizing`

Protected builder MUST 冻结双端 artifacts、evidence、assessment、conflicts 与 policy packet。模型 MAY 提议正文，但 protected canonicalizer 决定是否接纳。Bounded attempts 全失败或 blocking conflict 无法解决→`inconclusive|rejected`；head 改变→`stale`；成功→`candidate_bound`。

#### 9.4.4 `candidate_bound`

唯一 CandidateRef 已写入；provenance coverage 完整；未知 required extension、permission 扩大、未解决 blocking conflict 均禁止进入。退出→`validating|stale`。

#### 9.4.5 `validating`

除通用验证外 MUST 验证：不原地覆盖 source、双端 branch preservation、conflict closure、双端 provenance、无静默 applicability 扩大、无 evidence/permission 继承。通过→`replaying`；hard failure→`rejected`；head 改变→`stale`。

#### 9.4.6 `replaying`

MUST 执行 A-only、B-only、overlap/conflict fixture families。双端 critical regression 任一非零→`rejected`；结果不足→`inconclusive`；完整→`decision_pending`；head 改变→`stale`。

#### 9.4.7 `decision_pending`

Protected evaluator 应用 U1 constrained Pareto comparator。Accepted→`activation_pending`；明确不满足→`rejected`；数据不足→`inconclusive`。

#### 9.4.8 `activation_pending`

事务 MUST 同时完成 source-head CAS、新 derived lineage/version、Candidate→Released mapping、activation ledger/outbox 与 proposal state event。CAS 失败→`stale`；成功→`released`。

#### 9.4.9 `released`

新 `M@1` MUST：

```text
M@1 derived_from A@x
M@1 derived_from B@y
```

A/B MUST retained active。M MUST NOT `supersedes` A/B；M 后续 revision MAY 以同 lineage `supersedes` 前版。即使 M 后续 deactivated，proposal 仍保持 `released`。

#### 9.4.10 去重与后续演进

- 相同 exact pair + intent → 后者 `duplicate` 并指向 canonical winner。
- Winner 在 candidate_bound 前收到额外 evidence MAY 追加 evidence attachment event；之后的新 evidence MUST 创建新 proposal attempt。
- 相同 lineages、不同 exact revisions 不是 exact duplicate；旧 attempt 按 M5 进入 stale。
- Source 后续演进不得改写既有 `derived_from`。

### 9.5 Composite execution

Composite artifact 状态：

```text
materialized → static_validated → replay_validated → executable
```

Run 状态：

```text
created → running → succeeded
                  ↘ failed
                  ↘ inconclusive
                  ↘ human_requested
```

Child attempt 状态：

```text
ready → running → succeeded
              ↘ retry_wait → running  (bounded)
              ↘ skipped
              ↘ fallback_selected
              ↘ failed
              ↘ inconclusive
              ↘ human_requested
```

规则：

1. 只有所有 exact children 可解析、已激活、ports 匹配、DAG 无环且权限受 Host cap 约束时，才可 `static_validated`。
2. Whole-Composite paired replay 通过后才可 `replay_validated/executable`；child/branch ablation MAY 作为附加记录，不替代 whole replay。
3. Scheduler 只可运行依赖已满足的 `ready` child。
4. Retry 不得超过 `max_attempts`；超出后执行封闭 action：`stop|skip|fallback|request_human|mark_inconclusive`。
5. `fallback` 必须指向声明且依赖可满足的 child；不得动态生成 child ref。
6. 任何 child ref 变化 MUST 创建新 Composite revision。

## 10. Evaluation and release policy

### 10.1 Protected pipeline

```text
Diagnose/Propose (model allowed)
→ Validate (protected)
→ Execute replay (protected scheduling + deterministic adapter)
→ Select (protected evaluator)
→ Export/Release (protected CAS + ledger)
```

Candidate 或其生成组件 MUST NOT 评价自身输出。

### 10.2 Hard gates

Release 前至少 MUST 通过：

- schema/canonicalization/digest；
- exact ref resolution；
- required extension；
- authority/provenance；
- evidence committed/sealed；
- permission/Host cap；
- kind/lineage；
- ports/DAG/retry/failure；
- fixture completeness；
- source-head freshness；
- zero safety violations；
- zero critical regressions；
- merge conflict closure（适用时）。

任一 hard gate 失败，utility 分数不得覆盖。

### 10.3 U1 constrained Pareto comparator

固定 fixture set 上使用整数 utility vector：

| 维度 | 方向 | 主要语义维度 |
|---|---|---|
| `task_success_count` | maximize | 是 |
| `critical_branch_pass_count` | maximize | 是 |
| `recovery_success_count` | maximize | 是 |
| `inconclusive_case_count` | minimize | 是 |
| `execution_cost_units` | minimize且受 budget 约束 | 否，不能单独触发 release |

Release MUST 满足：

1. hard gates 全过；
2. source A critical slices 不回归；
3. source B critical slices 不回归（merge 适用）；
4. 相对 reference envelope，所有主要维度不差；
5. 至少一个主要维度严格改善；
6. cost 不超过 versioned budget；
7. 仅 cost 下降不得构成严格改善。

比例比较 MUST 使用整数交叉乘法：`a_num * b_den` 与 `b_num * a_den`；禁止浮点。Comparator 与 budget MUST 由 exact `UtilityComparatorRef` 固定。

### 10.4 Reference envelope

- A fixtures baseline=A；
- B fixtures baseline=B；
- overlap fixtures baseline=versioned policy 选择的 best applicable released source；
- 普通单 source revision 使用当前 active exact revision 作为 baseline；
- fixture set 与 baseline mapping MUST 在 replay request/decision 中冻结。

### 10.5 Replay

v1 MUST 使用 recorded sealed path + deterministic fake runtime adapter，且只执行 Causal Evaluation Replay。Fixture MUST 包含已发现 success/failure/recovery branches。Live production result 不得替代 sealed paired replay。

### 10.6 Release 与 CAS

Accepted decision 不等于已激活。只有 active-head/source-head CAS、version assignment、Candidate→Released mapping、activation event 与 outbox 原子成功后才是 released/active。失败 MUST 记录 reason，不得部分发布。

## 11. Skill Graph contract

### 11.1 Streams

逻辑上 MUST 有独立 `runtime` 与 `curation` stream/head。v1 只物化 Runtime Graph；Curation records 通过 ledger/API 读取。两个 stream 不得共享可变 head。

### 11.2 Node 类型

Skill-domain node 封闭为：

- exact Skill revision node；
- branch node。

Q6 不禁止引用已有 checkpoint/evidence domain endpoint。若底层图要求边端点为 vertex，MUST 使用 typed reference vertex，仅保存 exact ref 与摘要，不复制证据、checkpoint、artifact 或 Markdown。

Revision node MUST 保存：

- exact SkillArtifactRef；
- kind、lineage、version；
- runtime lifecycle metadata；
- reconstructible retrieval metadata；
- source projection sequence/schema。

### 11.3 九关系封闭集

除以下关系外，v1 MUST 拒绝自由 relation string：

| Relation | Direction | 分类 | 权威来源 | v1 tracer |
|---|---|---|---|---|
| `supersedes` | newer revision → prior active same-lineage revision | Activation structural | activation transition | 必须产生 |
| `composes` | Composite revision → exact child revision | Artifact structural | released Composite body | Slice 9 必须产生 |
| `depends_on` | dependent revision → exact prerequisite | Artifact structural | explicit artifact dependency | 有声明时产生 |
| `has_branch` | Step Guidance revision → branch node | Artifact structural | released artifact body | 必须产生 |
| `derived_from` | derived revision → source revision | Release provenance | immutable derivation/release record | Merge tracer 必须产生 |
| `anchored_at` | Step Guidance revision → checkpoint ref vertex | Host provenance | immutable SkillAnchor | 有 anchor 时产生 |
| `supported_by` | branch → committed/sealed evidence ref vertex | Evidence-assessed | versioned claim assessment | 有证据评估时产生 |
| `refuted_by` | branch → committed/sealed evidence ref vertex | Evidence-assessed | versioned claim assessment | 有反证评估时产生 |
| `similar_to` | canonical symmetric pair of revisions/branches | Similarity-assessed | immutable SimilarityAssessment | Merge tracer 必须产生 |

`derived_from` 是权威 release provenance，不是模型自由推断。`similar_to` MUST canonicalize 为单一对称 edge identity，但查询 MAY 双向遍历。

### 11.4 Relation 规则

- `supersedes` MUST 同 lineage、同 kind，并由成功 activation transition 产生。
- `composes` child MUST 是 exact released/activated ref；更新 child 需新 Composite revision。
- `depends_on` MUST 显式声明且 transitive closure 无 cycle。
- `has_branch` 只由 exact artifact body 派生。
- `derived_from` MUST 有 immutable derivation decision；可多源。
- `anchored_at` MUST 引用 Host-sealed checkpoint。
- `supported_by/refuted_by` MUST 绑定 exact claim/branch 与 committed evidence。
- `similar_to` MUST 引用 versioned assessment；不得触发 identity/activation/evidence继承。

### 11.5 Visibility 与历史

- Candidate/proposal/rejected/inconclusive artifact MUST NOT 投影到 Runtime。
- Activated revision node MUST 保留，即使后续 superseded/deactivated。
- 默认 Explore 只返回 current active；历史 exact lookup/lineage traversal MAY 返回旧节点并明确状态。
- Deactivation event MUST 更新 lifecycle metadata，不删除 edges/history。

### 11.6 Projection freshness

每个读响应 MUST 携带 watermark。若提供 `min_activation_sequence` 且 watermark 较小，服务 MUST 返回 `PROJECTION_BEHIND_REQUIRED_SEQUENCE`，不得将旧 Graph 冒充最新。Artifact deployment MUST 直接读取权威 exact ref。

## 12. Memory Explore contract

### 12.1 Session 与结果类型

同一 ExploreSession 内 MUST 同时支持 evidence 与 Skill 两种结果，但结果结构、subcap 与 served fence MUST 分离。Memory Agent 只能在 Room shared Space 使用；普通 Agent scope 由 Host exact profile 决定。

### 12.2 Query flow

```text
Host Tool Proxy request
→ GMS scope/sequence validation
→ deterministic lexical retrieval
→ graph expansion/ranking
→ budget allocation
→ canonical artifact read
→ Guidance View rendering
→ typed ExploreResult + watermark
→ exact ToolProxyResult returned to Pi
```

### 12.3 Ranking

v1 MUST 使用 versioned deterministic lexical + graph ranker。不得使用未版本化模型判断。Tie-break MUST 最终落到稳定 exact-ref lexical order。建议顺序：policy score 降序、active status、graph distance 升序、artifact digest 升序。

### 12.4 Budgets

`total_used <= total_cap`，evidence/Skill 结果分别不得超过 subcap，所有 Guidance View 总 token count 不得超过 Guidance budget。截断 MUST 返回 reason code 与 omitted refs；禁止无标记截断。

### 12.5 Guidance View

Guidance View MUST 含 source exact ref、render/profile policy、included/omitted branches、expandable refs、runtime context hash 与 view hash。它是查询时派生视图，不得写入 Graph 作为 artifact 副本。

### 12.6 Citation fences

Artifact identity citation 只证明“这是哪个 Skill revision”；evidence citation 只证明“哪些 evidence 支持/反驳哪个 claim”。二者 MUST 分开，不能用 Skill identity 代替 evidence，也不能用 evidence ref 代替 artifact identity。

### 12.7 Tool result

Host Tool Proxy 的 exact response MUST 是 Pi 收到的唯一结果。GMS 在 Pi `tool_execution_end` 之后的旁路调用不得被宣称为 Agent 已观察到的结果。

#### §12.7.1 Tool-specific success validation matrix（修订 v1.1，CTR-002）

本小节为版本化修订条款：不重写 §12.4–§12.7 既有正文，而是冻结 tool success payload 的 **tool-specific validation matrix**，解决"Host 对所有 success result 无条件要求 budgets/citations/watermark"与 closed `GuidanceView`（§7.15，无这些字段）之间的跨文档冲突。本条款与 §13.7.1（CTR-004）reason registry 完全兼容：**不新增任何 reason code**，全部复用两份已冻结 registry 中的既有 code。

**M1 binding 声明（权威文件）。** 每个 tool name 绑定唯一 result schema 与唯一适用规则集；Host（Host §5.6）与 GMS（自验，GMS §10.5）MUST 按同一 matrix 校验 success payload。权威文件为 §16 conformance 目录下 `policy/tool-success-validation.v1.json`，其 policy digest（RFC 8785 JCS、SHA-256、除 `policy_digest` 自身外全文）冻结为：

`sha256:dde91eff2aaac31057512beba1c667e0b07b7cb4c36037db4e07eaced6f702f5`【修订 v1.1，CTR-003：本 policy 的 explore 条目已追加闭合字段 `omissions`（§12.7.2 carrier），既有字段语义不变，digest 随之重冻结】

加载方 MUST 重算 digest，不匹配 MUST fail closed。policy 声明的 closed tool 集合 v1 为 `memory_explore`、`memory_expand`、`skill_get`；未列 tool name 一律 closed failure（`TOOL_UNSUPPORTED`；Host 侧 `HOST_TOOL_NOT_ALLOWED`）。

**M2 Explore。** `memory_explore` success `result` MUST 是 §7.16 `ExploreResult` 且满足 explore-family 规则集：

- **watermark**：§7.14 `ProjectionWatermark` REQUIRED（含其 closed 子字段）；缺 watermark 或缺子字段 → `SCHEMA_REQUIRED_FIELD_MISSING`；
- **budgets**：§12.4 budgets 对象 REQUIRED 且 `total_used <= total_cap`、evidence/Skill 结果计数不超 subcap、Guidance token 总和不超 budget；缺对象/子字段 → `SCHEMA_REQUIRED_FIELD_MISSING`，一致性违反 → `BUDGET_INVALID`；
- **citations**：§12.6 typed citations REQUIRED——evidence result 的 `citation.evidence_ref+claim`、skill result 的 `artifact_identity_citation.skill_ref` 与 `evidence_citations`；缺任一 → `CITATION_INVALID`；
- **top-level omitted refs 存在性义务**：因 cap 截断省略 top-level 结果时 MUST 以 typed omitted evidence/Skill refs 完整表达（§12.4）；其字段形态已由 §12.7.2（修订 v1.1，CTR-003）冻结为闭合 `omissions` carrier。【修订 v1.1，CTR-003】下句"形态冻结前 `truncation_reason_codes` 非空的 success payload MUST fail closed（`TOOL_RESULT_BINDING_INVALID`）"的临时保留条款解除：carrier 齐备且满足 §12.7.2 全部一致性义务的截断 success payload 可被接受；silent truncation（有 code 无对应 exact refs）仍按原 code fail closed。

**M3 Expand。** `memory_expand` success `result` 绑定同一 §7.16 `ExploreResult` schema 与同一 explore-family 规则集（watermark/budgets/citations 同 M2），无 Expand 专属豁免。

**M4 skill_get。** `skill_get` success `result` MUST 是 §7.15 closed `GuidanceView`，且**只**按 closed schema 的适用字段验证：

- 验证内容限于：closed schema 完整性（§7.15 REQUIRED 字段全在、无未知字段）、digest 一致性（`view_hash` == 对去掉自身的 view document 的 JCS SHA-256，违反 → `GUIDANCE_VIEW_HASH_MISMATCH`）与 policy 冻结的 profile 版本下限（低于下限 → `SCHEMA_VERSION_UNSUPPORTED`，旧 profile 不得启用）。
- **禁止**要求 watermark/budgets/citations：这三项不是 skill_get 的适用规则（policy 中显式列为 non-applicable）。freshness（projection sequence/watermark 比对）与 authorization（scope/room）由 **request 字段（§7.17 `requested_min_activation_sequence`、`room_id`、`agent_id`、`scope_profile_ref`）+ 权威 read audit 记录**承载，MUST NOT 向 closed GuidanceView DTO 私添字段。任何向 GuidanceView 添加 `watermark`/`budgets`/`citations` 等字段的 payload → closed failure（DTO_FIELD_NOT_IN_CLOSED_SCHEMA 语义，复用 registry 既有 code `SCHEMA_FIELD_UNKNOWN`）。

**M5 通用规则（共有层，全部 tool 适用，按 policy 冻结顺序）。** 以下一律 closed failure：tool/result type mismatch（result `schema_version` ≠ 该 tool 绑定 schema → `TOOL_RESULT_BINDING_INVALID`）；free-form JSON body / 自由 artifact body（非 closed DTO → `ARTIFACT_BODY_INVALID`）；未知 tool name（→ `TOOL_UNSUPPORTED`；Host 侧 `HOST_TOOL_NOT_ALLOWED`）；unknown required extension（→ `UNKNOWN_REQUIRED_EXTENSION`）；scope 越权（read audit 与 request 的 room/agent/scope profile 不一致 → `EXPLORE_SCOPE_VIOLATION`；Host 侧 `UPSTREAM_SCOPE_VIOLATION`）；freshness 请求未满足（→ `PROJECTION_BEHIND_REQUIRED_SEQUENCE`，shared code，Host MAY 生成）；**禁止 wrapper**——把真实 payload 包在额外键（`payload`/`data`/`result` 等）下再宣称 success 属于 closed-schema field-set violation（→ `SCHEMA_FIELD_UNKNOWN`）。

当 Host 按 §13.7.1 R3 precedence 1 在 upstream payload 校验中发现同类缺陷时，MUST 使用该 registry 中对应 Host-owned code（`UPSTREAM_SCHEMA_INVALID`/`UPSTREAM_SCOPE_VIOLATION`/`UPSTREAM_BUDGET_VIOLATION`/`UPSTREAM_DIGEST_MISMATCH`）；本条款 matrix 内冻结的 code 为 GMS/system 侧 canonical 值。两套 mapping 已随 policy JSON 一并冻结，实现 MUST NOT 按 message 文本自由推断。

**M6 readiness gate。** `skill_get` 在本条款 fixture（`tools/` corpus，`tools/manifest.json` 登记，v1 覆盖 `tools/{explore/basic,expand,skill-get}`）与 policy 全绿前 disabled（fail closed：GMS 侧 `TOOL_UNSUPPORTED`，Host 侧 `HOST_TOOL_NOT_ALLOWED`）。conformance validator（`validate_tool_binding.py`，stdlib only）重算 policy digest、交叉校验两份 reason registry 的 §13.7.1 R2 冻结 digest、并对每 fixture case 按 matrix 推导 accept/reason 与 expected 精确比对；全部一致即 gate green，此后 Host/GMS MAY enable `skill_get`（仍按 M4 只验证适用字段）。旧 profile 版本（低于 policy 冻结下限）在任何时候 MUST NOT enable。

**M7 fixture。** 本条款由 `$FIX/tools/`（`tools/manifest.json`、`tools/{explore/basic,expand,skill-get}/**` 的 `input.json`+`expected.json`）、`$FIX/policy/tool-success-validation.v1.json`、`$FIX/validate_tool_binding.py` 与 `$FIX/tests/test_tool_binding.py` 固定（自带子 manifest，不改 §16.2 根 manifest）。Explore 的 top-level-omission 子树由 CTR-003 追加，本条款不预建。

#### §12.7.2 ExploreResult top-level omission carrier（修订 v1.1，CTR-003）

本小节为版本化修订条款：不重写 §7.15–§7.16 与 §12.1–§12.6 既有正文，只冻结 `ExploreResult` 的 **top-level omitted refs carrier**（§7.16 闭合字段组的扩展），完成 §12.7.1 M2 声明"字段形态由 CTR-003 后续冻结"的未尽事项。branch omission 仍只属于 §7.15 `GuidanceView`（`omitted_branch_refs`/`expandable_refs`，closed DTO 内部表达），禁止混入 top-level carrier。本条款与 §13.7.1（CTR-004）reason registry 完全兼容：**不新增任何 reason code**，全部复用既有 code。完整闭合形态由 `$FIX/schema/explore-result.schema.json` 冻结。

**C1 carrier 字段（§7.16 闭合字段组扩展）。** `ExploreResult` 顶层新增闭合字段 `omissions`（REQUIRED；无省略时 MUST 为空列表——语义确定，不是缺省；缺省 → `SCHEMA_REQUIRED_FIELD_MISSING`）。元素为闭合对象 `{kind, ref, reason_code}`，不得携带其他字段（→ `SCHEMA_FIELD_UNKNOWN`）：

- `kind`：封闭枚举 `evidence|skill`（其他值，包括 branch/checkpoint 等 → `SCHEMA_ENUM_INVALID`）；
- `ref`：与 kind 匹配的 **exact ref**——`evidence` → §7.7 `EvidenceRef`（`commit_state` 仅 `committed|sealed`，否则 → `EVIDENCE_NOT_COMMITTED`）；`skill` → §7.3 `SkillArtifactRef`。kind 与 ref 的 `schema_version` 错配（evidence/Skill 混类）→ `REF_MISMATCH`；latest/naked-id/Graph-node 等非 exact 形态 → `NON_EXACT_REF`；
- `reason_code`：只能取 §11.4（GMS）/§13.7.1 registry 中 `gms-truncation-success` 组已冻结的 4 个 truncation success-metadata code：`TOTAL_CAP_REACHED`、`EVIDENCE_SUBCAP_REACHED`、`SKILL_SUBCAP_REACHED`、`GUIDANCE_TOKEN_BUDGET_REACHED`；unknown code → `SCHEMA_ENUM_INVALID`。

**C2 排序与去重。** `omissions` 列表 MUST 按（kind 升序，exact ref 的 RFC 8785 JCS canonical bytes 升序）稳定排序——排序可由任意实现重放，乱序 → `TOOL_RESULT_BINDING_INVALID`。同一 exact ref（full closed ref 的 JCS bytes 相等）至多出现一次，重复 → `TOOL_RESULT_BINDING_INVALID`。

**C3 与 served 集合分离。** 已服务 ref MUST NOT 出现在 `omissions`：包括本 payload 的 `evidence_results[].evidence_ref`/`skill_results[].skill_ref`，以及同一 ExploreSession 先前页面已服务的 exact refs（分页场景）；违反 → `EXPLORE_FENCE_CONFLICT`。

**C4 双向一致性（解禁 §12.7.1 临时 fail closed）。** `truncation_reason_codes` 非空 ⇔ `omissions` 非空，且 `truncation_reason_codes` MUST 恰为按列表顺序对 omission 条目 `reason_code` 做保序去重后的序列（孤儿 code、私有 code 均禁止）。任一方向违反 → `TOOL_RESULT_BINDING_INVALID`：有 code 无 ref（silent truncation，含 carrier 整体缺省）与有 ref 无 code 均拒绝。kind/code 相容性：`evidence` 条目只能携带 `TOTAL_CAP_REACHED|EVIDENCE_SUBCAP_REACHED`；`skill` 条目只能携带 `TOTAL_CAP_REACHED|SKILL_SUBCAP_REACHED|GUIDANCE_TOKEN_BUDGET_REACHED`；违反 → `SCHEMA_ENUM_INVALID`。

**C5 budget accounting 可重放。** `budgets.total_used` MUST 等于 `len(evidence_results)+len(skill_results)`——只有 served 项消耗预算，omitted 项永不消耗；违反 → `BUDGET_INVALID`。截断声明必须真实：声明 `TOTAL_CAP_REACHED` ⇒ `total_used == total_cap` 且存在携带该 code 的条目；声明 `EVIDENCE_SUBCAP_REACHED` ⇒ evidence served 数 == `evidence_subcap` 且存在携带该 code 的 `evidence` 条目；声明 `SKILL_SUBCAP_REACHED` ⇒ skill served 数 == `skill_subcap` 且存在携带该 code 的 `skill` 条目；声明 `GUIDANCE_TOKEN_BUDGET_REACHED` ⇒ 存在携带该 code 的 `skill` 条目且 served Guidance token 总和仍满足 §12.4。虚假声明 → `BUDGET_INVALID`。

**C6 fence/watermark 前进。** 同一 ExploreSession 内相继 served 的新页面 MUST 使 watermark 前进：后一页 `watermark.projected_through_activation_sequence` > 前一页（会话先前 watermark）；不前进（含相等）→ `EXPLORE_FENCE_CONFLICT`。相同 query 的 retry 返回相同 exact result 且不重复消费 fence（§10.8/GMS）。freshness 下限（`requested_min_activation_sequence` ≤ `projected_through_activation_sequence`）仍按 §12.7.1 explore-family 规则执行。

**C7 result type 与 closed 形态。** success `result` MUST 是 §7.16 `gms.explore-result.v1`（wrong result type → `TOOL_RESULT_BINDING_INVALID`）；free JSON body → `ARTIFACT_BODY_INVALID`；`evidence_results[]/skill_results[]` 的 `result_type` 枚举违反 → `SCHEMA_ENUM_INVALID`。

**C8 fixture 与 validator。** 本条款由 `$FIX/tools/explore/{top-level-omission,no-omission,negative}/**`（每 case `input.json`+`expected.json`，登记于 `tools/manifest.json` 追加的 `omission_cases` 数组，不改 §12.7.1 M7 既有 `cases` 条目）、`$FIX/schema/explore-result.schema.json`、`$FIX/validate_explore_omission.py`（stdlib only，CLI `--fixtures <tools/explore>`）与 `$FIX/tests/test_explore_omission.py` 固定。本条款 fixture 全绿后，§12.7.1 M2 的临时保留条款即告解除（见该处修订标记）；§12.7.1 policy 的 explore 条目已追加 `omissions` 闭合字段并重冻结 digest，其 matrix 层 `truncation_without_omission_carrier` 规则保留为 silent-truncation 下界守卫（本条款 validator 对 carrier 语义做完整校验）。

## 13. Consistency and failure semantics

### 13.1 Transaction boundaries

下列写入 MUST 原子：

- candidate binding + proposal state event；
- release version assignment + Candidate→Released mapping + activation event + outbox + active-head CAS；
- projector Graph mutation + watermark CAS；
- merge activation + source-head CAS + new lineage head + outbox + proposal released event。

Graph projection不得加入 release 事务。

### 13.2 Idempotency

所有外部 mutation request MUST 带 idempotency key。相同 key + 相同 digest MUST 返回原结果；相同 key + 不同 digest MUST `IDEMPOTENCY_CONFLICT`。Retry 不得产生新 version、重复 activation 或重复 proposal winner。

### 13.3 Closed failure actions

运行 failure action 封闭为：

```text
stop | skip | fallback | request_human | mark_inconclusive
```

失败 reason code 必须是 schema-versioned closed enum。自由文本只作说明，不能驱动 machine behavior。

### 13.4 Stale

Stale 不否定历史 exact ref。它表示 frozen expectation 不再满足。Stale proposal/candidate不得自动 rebase；必须新建 assessment/proposal/candidate attempt。

### 13.5 Rollback

历史 release/activation不得删除或改写。回滚通过新的 protected `deactivate` 或后续 `activate` compensating event。Merge source 因 M4 retained，可继续运行；这不授权自动 retirement 或自动 fallback。

### 13.6 Projector failure

Projector 遇到 gap、digest conflict、未知 required schema 或非法 relation MUST 进入 `blocked`，watermark 不越过失败事件。Deployment 继续依赖权威 active head；Explore 强新鲜度请求 fail closed。

### 13.7 Fail-closed cases

至少以下情况 MUST fail closed：

- digest/ref mismatch；
- unknown required extension；
- naked name/`latest`/Graph node execution；
- uncommitted evidence；
- permission exceeds Host cap；
- source-head CAS failure；
- projection behind required sequence；
- missing Composite child/port；
- unresolved blocking merge conflict；
- non-integer hashed core number；
- model attempts protected mutation。

#### §13.7.1 System reason-code registry policy（修订 v1.1，CTR-004）

本小节由 CTR-004 冻结 system reason-code registry policy，作为全系统 `reason_code`/`status`/retry 语义的唯一权威；它细化 §13.2–§13.7 与 §16.4 的既有 reason 语义，不改变任何既有 code 的含义。

**R1 Registry 与 ownership。** v1 冻结两份 closed registry，存放于 §16 conformance 目录：

- `policy/system-reason-codes.v1.json`（owner=gms/system，157 个 code）：完整收编 GMS Spec §11.4 canonical 值域（112 个 failure code，按 §11.4 分组与出现顺序，另含 4 个 truncation success metadata code）；收编 FND-001 的 16 个 code（其中 12 个 Contract code 与 `SIMILARITY_BELOW_THRESHOLD` 已在 §11.4 值域内，扩展 code `BOM_NOT_ALLOWED`/`INVALID_JSON`/`ILLEGAL_STATE_TRANSITION` 归入 contract-canonicalization 组）；收编 FND-002 recorded corpus 的 38 个语义 code（segment-integrity、seal-digest、registry-integrity、environment-declaration、fake-runtime、late-determinism-expectation 六组）。每个 code 携带闭合字段 `name/group/status(success|failure|inconclusive)/retryable/retry_scope(same_request|new_attempt|none)/terminal/owner/spec_ref`（可选 `notes`）。
- `policy/host-proxy-reason-codes.v1.json`（owner=host-local，12 个 code）：Host Spec §5.9 的 Host-owned 集合（request-validation、infra、upstream-validation、delivery 四组），不与 system registry 重号。`IDEMPOTENCY_CONFLICT` 与 `PROJECTION_BEHIND_REQUIRED_SEQUENCE` 仍为 GMS-owned shared code：Host MAY 按 Host §5.7/§5.9 生成或透传，但其 status/retry 语义 MUST 逐字取自 system registry；GMS MUST NOT 生成 Host-local code（GMS §11.3）。

**R2 digest 冻结。** 各 policy 的 `registry_digest` 是除该字段外全文 RFC 8785 JCS SHA-256，冻结为：

- system registry：`sha256:16410afa27498bb425885d4629c65309389f15adeb975f97f98674170ad00a2c`
- host-proxy registry：`sha256:e48f27252bff3fd34868ef4bc5b56a678cf2a35d59f4cd7f2c71f78290485f5e`

三端（Host/GMS/RSIH）MUST 以重算 digest 校验方式加载，不匹配 MUST fail closed。GMS Spec §11.3 的 `gms.reason-codes.v1` canonical policy body MUST 可由 system registry 机械推导（failure/truncation 数组按 §11.4 顺序，加三个分类数组），其 JCS digest 冻结为 `sha256:998d42eba6b4b9e338497d4ed161e57eb0d804aba1e2f81c9317804cf70a98dd`（同值冻结于 conformance validator 常量，机器推导生成，非手写）。

**R3 precedence（全序，可执行）。** 对一次 failure observation，决策顺序唯一：

1. Host 本地 validation 失败（request/scope/tool/upstream payload 校验）→ Host-owned code 优先；同时观察到的 upstream code 降级为非行为型 metadata；
2. upstream code ∈ 已 digest 校验的 system registry 且 status ∈ {failure, inconclusive} → 原文透传；status/retryable/retry_scope/terminal 逐字取自 registry entry，接收端 MUST NOT 覆盖；
3. upstream code ∉ registry → 一律替换为 Host-local `UPSTREAM_SCHEMA_INVALID`（本修订的 closed unknown-upstream failure，别名 `UNKNOWN_UPSTREAM_CODE`）；原 code 仅作非行为型诊断 metadata，禁止按 message 文本推断；未知 code 永不透传；
4. late upstream result → 只入 audit，不产生、不替换、不改写任何 code 或 terminal 状态（违反即 `LATE_OUTPUT_REWRITE`）。

success/truncation code（`TOTAL_CAP_REACHED` 等 4 个）MUST NOT 出现在任何 `error.reason_code` slot。

**R4 status/retry/new-attempt 全映射。** 每个 code 的映射 total 且 MUST 满足：`retryable == (status==failure && retry_scope==same_request)`；`terminal == (retry_scope!=same_request)`；success/inconclusive → `retry_scope=none`。语义上：

- semantic failure（digest/ref/schema/permission/断言/integrity 类，即白名单与 stale CAS 集之外的其余 code）：`retry_scope=none`——same-request retry MUST NOT 发出，new attempt 亦不得改判原请求结果（semantic failure 不得重试成 pass）；
- infra failure 白名单 `{PROJECTION_BEHIND_REQUIRED_SEQUENCE, FAKE_TOOL_NO_RESPONSE, GMS_UNAVAILABLE, HOST_PROXY_TIMEOUT}`：`retryable=true`、`retry_scope=same_request`（bounded retry，复用 idempotency identity，Host §5.7/§6.6）；
- stale CAS 集（GMS §11.3 的 11 个 new-attempt code）：`retry_scope=new_attempt`——same-request retry 禁止（含误重试 stale CAS），必须重新冻结 expectation 并使用新 attempt identity（§13.4）；
- late result → 只入 audit（precedence 4），不得改写已交付 terminal 结果；
- inconclusive 判定条件：当失败无法被确定性归因到具体 failure code（如仅观察到 nondeterministic raw output、evaluation arithmetic overflow、capability family 缺失）时，由 GMS canonicalize 为 `REPLAY_INCONCLUSIVE`/`MERGE_SYNTHESIS_INCONCLUSIVE`（status=inconclusive）；一旦可确定性归因，MUST 使用具体 failure code（如 `REPLAY_NONDETERMINISTIC`/`UTILITY_ARITHMETIC_OVERFLOW`/`MISSING_FAMILY`，status=failure）。

**R5 唯一权威与验证。** 两份 policy JSON 是唯一权威；conformance validator `validate_reason_policy.py`（stdlib only）独立校验：schema 闭合、registry 内无重号、跨 registry ownership 不重叠、映射 total、digest 自洽、`reasons/` fixture 全部按 policy 推导通过、未知 code fail-closed（fixture corpus 自带 `reasons/manifest.json`，不修改 §16.2 根 manifest）。未知 code、digest mismatch、same-request 误重试 stale CAS、semantic failure 重试成 pass、late result 改写 terminal、自由 message 驱动 fallback 全部 MUST fail closed。

## 14. Permissions and trust boundaries

### 14.1 Capability matrix

| 能力 | Host | GMS | RSIH | 模型/Agent |
|---|---:|---:|---:|---:|
| Seal Segment/path/evidence order | MUST | read | read | propose only |
| Commit canonical evidence | submit | MUST | no | no |
| Create candidate | submit content | MUST canonicalize | no | propose only |
| Validate/replay decision | schedule | MUST own decision | execute adapter | no |
| Activate/deactivate | no | MUST | no | no |
| Mutate Runtime Graph | no | projector only | no | no |
| Explore | proxy | serve | consume | request |
| Materialize exact Skill | no | supply artifact | MUST | no |
| Execute Pi/Composite | schedule context | no | MUST | participate within permissions |

### 14.2 Permission computation

Composite permission requirement MUST 是 children union + orchestration requirements，并受 Host authority cap 限制。Merge candidate permission MUST 不超过 sources 合法 union 与 Host cap；未经 evidence/policy 授权不得扩大。

### 14.3 Untrusted inputs

模型输出、artifact正文、tool arguments、external resources 和 replay output 均视为数据。Protected components MUST schema-validate、digest-check、scope-check，不得执行其中指令来改变本文权威边界。

## 15. Vertical-slice acceptance matrix

### 15.1 九个 slices

| Slice | 端到端目标 | 主要输入 | 必须产生 | 验收条件 |
|---|---|---|---|---|
| S1 | Exact refs + digest | Golden JSON | Go/TS exact refs/digests | canonical bytes/digest 一致；负例同 reason code |
| S2 | Host result return | ToolProxyRequest | exact ToolProxyResult to Pi | Pi 所见 bytes/digest 与 proxy result 一致，无旁路替代 |
| S3 | Sealed Segment fixture | recorded Room path | SegmentRef、seals、branch refs | success/failure/recovery 可 exact 解析且不可变 |
| S4 | Validate + paired replay | Candidate + baseline + fixtures | validation/replay records | fake runtime 重放确定；模型不参与评分 |
| S5 | Decision + activation | ReplayResult、policies | ReleaseDecision、ReleasedRef、ActivationEvent | hard gates、U1、CAS 全过；失败无部分发布 |
| S6 | Runtime projection | activation/outbox | revision/branch nodes、Q9 edges、watermark | duplicate 幂等、gap blocked、历史节点保留 |
| S7 | Explore | scoped query + watermark | typed ExploreResult/GuidanceView | caps/fences/citations/tie-break 可验证 |
| S8 | RSIH materialization | active exact refs | bundle、manifest、skill.lock | closure frozen、所有 digest 可验证（manifest/lock identity 依 §6.2.1） |
| S9 | Composite binding/replay | activated children | Composite release/materialization | exact children、DAG、ports、permissions、whole replay 全过 |

### 15.2 Merge extension tracer（MT，不重编号 Q28 slices）

| Tracer | 目标 | 必须产生 | 负路径 |
|---|---|---|---|
| MT1 | Similarity assessment | exact Assessment + `similar_to` | below-threshold 不自动 admission |
| MT2 | Proposal/admission/dedup | MergeProposal + unique group winner | exact duplicate→duplicate |
| MT3 | Candidate synthesis | frozen packet + CandidateRef | blocking conflict→rejected/inconclusive |
| MT4 | Bilateral replay/release | A/B/overlap results + U1 decision | critical regression→rejected |
| MT5 | Derived activation | M@1 + two `derived_from` + A/B retained | source head changed→stale |
| MT6 | Projection/retrieval/materialization | Runtime node/edges/watermark/bundle/lock | candidate 不得出现在 Runtime |

#### §15.2.1 S8/MT6 readiness gates（修订 v1.1，CTR-001）

S8/MT6 此前因 `SkillLock.materialization_ref` 缺少唯一 exact identity 映射而 fail-closed。该未决点已由 §6.2.1（修订 v1.1）冻结：manifest exact identity、三层 digest 分工、唯一性、freeze/cache 规则与 `legacy_unlocked` 兼容结论均以 §6.2.1 为准。S8/MT6 的成功 publish/freeze 路径在 `$FIX/materialization` fixture 全绿后解禁；在此之前各模块继续 fail-closed，禁止以 `bundle_digest` 或私有 record 猜填共享字段。

### 15.3 决策 traceability

| 决策 | 规范落点 |
|---|---|
| Q1 三种 canonical kind | 5.2、8 |
| Q2 lineage kind 不变；跨 kind derived | 5.2、8.1 |
| Q3 Composite 只含 exact orchestration | 5.2、8.4 |
| Q4 DAG；循环仅 bounded retry | 8.4、9.5 |
| Q5 exact revision nodes + supersedes | 5.4、11.2-11.5 |
| Q6 Skill revision + branch node | 11.2 |
| Q7 Runtime/Curation 分离；v1 Runtime | 11.1 |
| Q8 node exact ref + reconstructible metadata | 11.2、12.5 |
| Q9 九关系封闭集 | 11.3-11.4 |
| Q10 structural 派生；inferred需 assessment | 11.3-11.4 |
| Q11 同 session、分 result/fence | 12.1、12.6 |
| Q12 metadata + exact ref + Guidance View | 7.15、12.5 |
| Q13 Memory Agent shared；普通 Agent profile | 3.1、12.1 |
| Q14 identity/evidence citation 分离 | 7.16、12.6 |
| Q15 先修 Host tool return path | 7.17-7.18、12.7、S2 |
| Q16 S-slot patch 绑定 exact Composite | 3.3、8.4、S9 |
| Q17 Composite children 已激活 | 8.4、9.5 |
| Q18 permission union + Host cap | 8.4、14.2 |
| Q19 static + whole Composite replay | 9.5、S9 |
| Q20 child ref 更新需新 revision | 5.2、8.4、11.4 |
| Q21 minimal vertical tracer | 2.1、15 |
| Q22 一系统契约 + 三模块 spec | 1、3、7.1 |
| Q23 JCS/SHA-256/integer/golden | 6、16 |
| Q24 closed core + namespaced extensions | 1.3、6.4 |
| Q25 CandidateRef/ReleasedRef 分离 | 6.2、7.3-7.4、9.1、10.6 |
| Q26 Composite child named JSON-Schema ports | 8.2-8.4、9.5 |
| Q27 closed retry/failure state machine | 8.4、9.5、13.3 |
| Q28 九 slices | 15.1 |
| Q29 recorded path + fake runtime | 10.5、S3-S4 |
| Q30 fixture含成功/失败/恢复；v1 causal replay | 10.5、7.10 |
| Q31 hard gates + utility + CAS | 10.2-10.6 |
| Q32 ledger + async idempotent projector + watermark | 5.3、9.2-9.3、11.6、13 |
| Q33 独立 stream/head；v1 Runtime only | 11.1 |
| Q34 Host local Tool Proxy，exact result | 7.17-7.18、12.7 |
| Q35 total/subcaps/Guidance budget | 7.16、12.4 |
| Q36 deterministic lexical + graph | 12.3 |
| Q37 content-addressed bundle + lock + freeze | 7.19-7.20、S8 |
| M1 分级混合发起 | 5.5、9.4.1-9.4.2 |
| M2 仅 active 非 probation released source | 5.5、9.4.2 |
| M3 新 symmetric derived lineage | 5.5、9.4.8-9.4.9 |
| M4 sources retained | 5.5、9.4.9、13.5 |
| M5 strict source-head CAS | 5.5、9.4.8、13.4 |
| M6 双端不回归 + envelope 改善 | 5.5、10.3-10.4 |
| M7 exact versioned integer similarity policy | 5.5、7.21、9.4.1 |
| M8 v1 仅二元 Step Guidance merge | 5.5、7.22、9.4 |
| M9 同 group 单 in-flight winner | 5.5、9.4.2、9.4.10 |
| U1 constrained Pareto | 10.3 |
| U2 minimal active/deactivated | 9.2、11.5、13.5 |
| C1 Runtime 保留历史 revision | 5.4、11.5 |
| C2 typed checkpoint/evidence reference vertex | 11.2 |
| C3 `derived_from` 属 release provenance | 11.3-11.4 |
| C4 Graph 不存 Guidance 正文 | 11.2、12.5 |
| C5 similar_to 不隐式 merge | 5.5、11.4 |
| C6 source retain 的检索重叠 | 5.6、12.3 |
| C7 stale 与 historical exact ref 分离 | 13.4 |
| C8 Candidate/Released identity 分离且 body digest equal | 7.13、9.2、10.6 |

## 16. Golden conformance fixtures

### 16.1 目录布局

规范 fixture 应位于未来实现仓库可共同消费的目录；模块规范必须指定实际映射。逻辑布局：

```text
conformance/
├── manifest.json
├── canonicalization/
│   └── <case-id>/
├── refs/
│   └── <case-id>/
├── artifacts/
│   └── <case-id>/
├── events/
│   └── <case-id>/
├── merge/
│   └── <case-id>/
└── negative/
    └── <case-id>/
```

每个 case：

```text
<case-id>/
├── source.json
├── canonical.utf8
├── canonical.base64
└── expected.json
```

`canonical.utf8` MUST 是无 BOM 的 exact canonical bytes；若 bytes 不便文本展示，`canonical.base64` MUST 提供标准 Base64。两者同时存在时解码后 MUST 完全相等。

### 16.2 `manifest.json`

```jsonc
{
  "schema_version": "rsih-skill-evolution.conformance-manifest.v1", // REQUIRED
  "contract_schema_version": "rsih-skill-evolution.system-contract.v1", // REQUIRED
  "cases": [{
    "case_id": "string<unique>",               // REQUIRED
    "category": "canonicalization|ref|artifact|event|merge|negative", // REQUIRED
    "source_path": "string",                  // REQUIRED
    "canonical_utf8_path": "string",          // REQUIRED
    "canonical_base64_path": "string",        // REQUIRED
    "expected_path": "string"                 // REQUIRED
  }]
}
```

### 16.3 `expected.json`

```jsonc
{
  "schema_version": "rsih-skill-evolution.conformance-expected.v1", // REQUIRED
  "case_id": "string",                           // REQUIRED
  "expected_accept": "boolean",                  // REQUIRED
  "expected_digest": "sha256:...",               // REQUIRED when accepted/canonicalizable
  "expected_canonical_byte_length": "integer>=0",// REQUIRED when canonicalizable
  "expected_reason_code": "string<closed-enum>",  // REQUIRED when rejected
  "expected_normalized_fields": {},               // OPTIONAL
  "notes": "string"                              // OPTIONAL, non-normative
}
```

### 16.4 最低 fixture 清单

v1 MUST 至少提供以下 15 类：

1. **Minimal SkillArtifactRef**：最小 exact ref 与 digest。
2. **Unicode/JCS key order**：不同输入 key 顺序、Unicode escaping 得到同 canonical bytes。
3. **Step Guidance artifact**：三段式、success/failure/recovery branches。
4. **Procedure artifact**：无伪造 causal/future字段。
5. **Composite artifact**：exact children、ports、DAG、retry/failure。
6. **CandidateArtifactRef**：无 released version，origin exact。
7. **Candidate→Released equality**：不同 ref identity、相同 body digest。
8. **Activation event**：activate、previous head、outbox、sequence。
9. **Deactivation event**：compensating event，历史 ref 保留。
10. **Projection watermark/idempotency**：重复 event 不变；同 sequence 异 digest 拒绝。
11. **SimilarityAssessment canonical pair**：A+B 与 B+A 规范成同 pair/key。
12. **MergeProposal**：双端 evidence、conflict、policies、dedup keys。
13. **Merge exact duplicate**：第二 proposal 指向 canonical winner。
14. **Merge stale CAS**：source head 更新导致 stale、无 activation。
15. **Guidance/Explore/lock closure**：view hash、separate citations、budget、frozen exact refs。

此外 MUST 提供以下 negative cases：

- hashed core 中出现非整数 number → `NON_INTEGER_NUMBER`；
- unknown required extension → `UNKNOWN_REQUIRED_EXTENSION`；
- digest mismatch → `DIGEST_MISMATCH`；
- naked name 或 `latest` → `NON_EXACT_REF`；
- Composite cycle → `COMPOSITE_CYCLE`；
- missing child port → `PORT_SCHEMA_MISSING`；
- unresolved blocking conflict → `MERGE_BLOCKING_CONFLICT`；
- uncommitted evidence → `EVIDENCE_NOT_COMMITTED`；
- projection gap → `PROJECTION_SEQUENCE_GAP`；
- permission 超过 Host cap → `PERMISSION_CAP_EXCEEDED`。

### 16.5 跨语言要求

1. Go（GMS/Host）与 TypeScript（RSIH）MUST 消费同一 source/expected files。
2. 两端 MUST 比较 canonical bytes，不仅比较 parsed object。
3. Digest、accept/reject 与 reason code MUST 全部一致。
4. Conformance runner MUST 单次运行、无网络、无时钟依赖、无随机未固定输入。
5. Golden 更新 MUST 伴随 contract/schema version 或显式兼容说明，禁止为通过实现而静默改 expected digest。

#### §16.6 Machine-readable contract freeze（修订 v1.1，CTR-005）

本小节由 CTR-005 冻结 Contract 的 machine-readable 权威层；它把 §7–§10、§13.3 与 §6.4/§6.2.1/§12.7.1/§12.7.2/§13.7.1 的既有语义固化为可直接校验的 schema/policy 文件，不引入任何新语义。冲突时以本 Contract 正文为准，机器层与正文不一致视为缺陷。

**F1 机器可读权威范围。** 以下目录是本 Contract 对应条款的 machine-readable 权威（`$FIX` 指 §16.1 conformance 根目录）：

- `$FIX/schema/shared/*.schema.json`：§7 全部 shared DTO（含 CTR-005 补足的普通 `SkillProposalEvent`/`MergeProposalEvent` 与 GMS §10.2–§10.4 tool arguments）+ §7.2 common value types（`common-types.schema.json`）；closed JSON Schema 子集（`additionalProperties:false`、显式 enum、integer-only、`if/then` 条件必填），`x-digest` 声明每个 DTO 的 digest field 与 preimage。
- `$FIX/schema/state/*.schema.json`：§9.1–§9.5 与 Host §3.3 状态机 bundle（state totality：每个状态唯一声明、每条 transition 引用已定义状态、terminal 无出边、非 terminal 必有 legal exit、全部可达）。
- `$FIX/policy/profiles/{render,resource,permission,runtime}-profile.v1.json`：四类 profile 的单位（closed unit registry：tokens/bytes/count/ms/units/micros/depth）、整数 min/max/default 上下限、mandatory permission scope、runtime determinism capability 与完整可重算的 `digest_preimage`→`profile_digest`。
- 既有冻结文件保持不变并被引用而非复制：`$FIX/schema/explore-result.schema.json`（CTR-003）、`$FIX/schema/materialization-identity.schema.json`（CTR-001）、`$FIX/policy/{system,host-proxy}-reason-codes.v1.json` 与 `$FIX/policy/tool-success-validation.v1.json`（CTR-002/CTR-004）。

**F2 Source of truth 与 provenance。** 手写 schema/policy 文件是 source of truth；一切派生物不得反向成为权威。`validate_contract.py --emit-provenance` 输出 `$FIX` 内全部 schema/policy 文件的 SHA-256 清单（落地为 `schema-cases/provenance.json`，携带 `generated_by.artifact_type=generated` 生成物标记）；该文件仅用于加载端核对，任何检查 MUST NOT 以它替代手写文件。drift 检查（§12.7.1/§13.7.1 冻结 digest 重算 + schema-cases corpus 的 accept/reason 派生）由 `validate_contract.py` 每次运行时执行。

**F3 三端加载义务（HST-101/GMS-101/RSI-101）。** Go Host、Go GMS 与 TypeScript RSIH adapter MUST 以 digest 校验方式加载本小节权威文件：加载后逐文件核对 provenance SHA-256（或按 F2 重算文件 digest），不一致 MUST fail closed 且不得降级为无 schema 运行；三端禁止 repository-local expected 副本（§16.5）。

**F4 schema_version 对应关系。** 每个 machine-readable 文件的 `schema_version` 与 Contract 一一对应：shared DTO 取 §7 各条款声明的 const（如 `gms.skill-artifact-ref.v1`、`host.tool-proxy-result.v1`、`rsih.skill-lock.v1`；activation/deactivation 事件共用 `gms.activation-event.v1`）；state bundle 统一 `rsih-skill-evolution.state-machine-bundle.v1`；profile 取 `rsih-skill-evolution.<kind>-profile.v1`；corpus/expected/provenance 元文档取 `rsih-skill-evolution.schema-cases.v1`/`rsih-skill-evolution.schema-case-expected.v1`/`rsih-skill-evolution.contract-provenance.v1`，其 `contract_schema_version` MUST 等于 `rsih-skill-evolution.system-contract.v1`。schema_version 变更 MUST 遵循 §16.5 第 5 条的 golden 更新纪律。

## 17. Reserved future work

以下接口空间被保留，但 v1 实现不得假装已经支持：

1. 完整 `probation` admission、exposure budget、promotion 和 rollback 状态机；
2. Source retirement/deprefer 的 protected policy 与 relation/visibility语义；
3. OPD Student `[in,out]`、Teacher `[in,skill,out]` 与 reverse KL `KL(Student || Teacher)` 数据/训练协议；
4. Curation Graph node/schema 与独立 projector；
5. Production embedding、semantic similarity calibration 与 learned hybrid ranker；
6. Procedure、Composite、跨 kind、n-way merge；
7. Competing merge candidate tournament；
8. stale proposal 自动 rebase；
9. Data-RSI、Model-RSI、RSI² scheduler；
10. child/branch ablation 作为 mandatory release gate；v1 仅 optional；
11. production traffic canary 或在线 evaluator；
12. retirement 后自动 fallback 到 source 的策略。

未来扩展 MUST 通过新 schema/policy/contract version，并保持历史 exact refs、ledger 与 conformance fixtures 可解析。
