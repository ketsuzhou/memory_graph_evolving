```yaml
document_status: normative
schema_version: rsih-skill-evolution.rsi-harness.v1
system_contract: ./system-contract.md
system_contract_schema_version: rsih-skill-evolution.system-contract.v1
host_spec: ./pi-group-chat-host.md
host_spec_schema_version: rsih-skill-evolution.pi-group-chat-host.v1
gms_spec: ./graph-memory-service.md
gms_spec_schema_version: rsih-skill-evolution.graph-memory-service.v1
language: zh-CN
```

> 本文是 `RSI-Harness`（以下简称 RSIH）在 RSIH Skill Evolution 系统中的 normative 模块规范。本文服从 [`system-contract.md`](./system-contract.md)（以下简称 Contract）、[`pi-group-chat-host.md`](./pi-group-chat-host.md)（以下简称 Host Spec）与 [`graph-memory-service.md`](./graph-memory-service.md)（以下简称 GMS Spec）。共享 DTO、跨模块状态机、全局不变量与共享 reason registry 不在本文重定义；冲突时以 Contract 为准，GMS 与 Host 的权威行为分别以各自模块规范为准。

## 1. RSIH authority boundary

### 1.1 规范范围

实现声称符合本规范时，MUST 同时符合 Contract §1 的规范词与版本规则、Contract §3.3 的 RSIH 权威、Contract §5 的全局不变量、Contract §6 的 canonicalization/exact identity、Contract §13 的一致性与失败语义及 Contract §14 的权限边界。本文只规定 RSIH 如何消费共享对象并执行其受保护职责。

Contract §7 是共享 DTO 唯一注册表；本文引用而不复制其 JSON schema。Skill artifact 三种 kind 的唯一 body/envelope 定义是 Contract §8；共享端到端与 Composite 状态机的唯一来源是 Contract §9；failure-action enum 的唯一来源是 Contract §13.3。共享 reason-code registry 与行为 policy 由 GMS Spec §11.3–§11.5 权威定义，Host-local proxy code 由 Host Spec §5.9 定义。本文不得增加、删减、别名化或按自由文本推断这些值域。

### 1.2 RSIH 唯一权威

RSIH MUST 只拥有以下运行与校验职责：

1. `HarnessGenome`/Pi runtime 的加载、兼容解析和一次 run 内的冻结视图；
2. released exact Skill 与私有 MetaRSI `S-slot` 的绑定；
3. released Skill closure 的 static validation；
4. 从 GMS 权威 closure 输入生成 exact、content-addressed materialization；
5. Host 调度下的 live/replay execution 与 deterministic replay adapter；
6. Contract §9.5 约束下的 deterministic Composite execution；
7. Pi integration、资源解析、隔离和 exact materialized execution source 的安装；
8. RSIH 私有 execution/materialization/diagnostic records。

RSIH 的 `Validate` 仅表示它执行 static/runtime-adapter validation；GMS 仍按 GMS Spec §2.4、§5.2–§5.6、§11.1 拥有权威 validation ledger、`ReplayResult` canonicalization、protected evaluation、`ReleaseDecision` 与 release state。

### 1.3 明确排除

RSIH MUST NOT：

- activate/deactivate Skill，写 active head、activation ledger、release ledger 或 Runtime Graph；
- self-score、计算 U1 winner、铸造权威 `ReplayResult` 或 `ReleaseDecision`；
- 接受 model/Genome 自报的权限、release 状态或 validation 结果作为权威事实；
- 执行 `CandidateArtifactRef`、未 released artifact、Graph node ID、naked name、别名、range 或 `latest`；
- 以 Graph projection、`GuidanceView`、session 文本或本地 cache 替代 GMS authoritative artifact/active-head read；
- 把 S-slot、materialization 私有 layout、raw execution output 或 `RsihDiagnostic` 冒充 Contract §7 共享 DTO；
- 修改 Host Room/Agent/Delivery/Segment/seal/Tool Proxy 权威状态，或绕过 Host Spec §5 的 same-call return path；
- 让 artifact 指令、Pi model、extension 或 Composite child 改写 protected validator、scheduler、permission cap、fixture、lock 或 digest policy。

### 1.4 Trust boundary 与 protected components

Skill body、Genome、Pi extension、fixture payload、recorded output、environment、filesystem entry 与 model output均为不受信输入。Exact-ref resolver、digest verifier、path normalizer、materializer publisher、static validator、fake runtime、HarnessRunner correlator、Composite scheduler 与 permission enforcer MUST 是 protected deterministic code。模型 MAY 参与 artifact 所允许的步骤，但不得拥有 `Validate/Execute/Select/Export` 的控制权。

### 1.5 发布现状

本规范描述的 actual exact-Skill adapter、materializer、`HarnessRunner`、Composite scheduler 与对应 conformance/acceptance fixtures **尚待实现**。在第 2 章所述现状兼容层之上，不得把文档中的新增接口宣称为当前实现。S2 gate、Pi `0.84.3` behavior fixtures、file-backed snapshot 迁移、`skill_get` 门槛与 top-level omission 门槛未全部关闭前，v1 Skill Evolution integration MUST 保持 disabled/fail closed。

## 2. Existing Genome compatibility

### 2.1 现状兼容层：v3 authoring、resolved v2 runtime

本节是对现有源码行为的兼容性冻结，不代表第 2.6 节 adapter 已实现。

- `RSI-Harness/src/harness/genome-bundle.ts:180-265` 的 `loadGenomeDocument` 把 `genome_schema_version="3"` 作为 authoring/assembly envelope：递归解析 base、按 manifest 顺序合并 components 与 overrides，最后强制生成 `genome_schema_version="2"` 并调用 v2 validator。`componentConfig` 与 `componentDescriptor` 位于同文件 `103-178`，限制 component 字段、contract/source 与 allowed operations；base cycle 和 manifest 内重复 component id 被拒绝。
- `RSI-Harness/src/harness/genome-bundle.ts:267-273` 的 `isHarnessGenomeBundle`/`loadHarnessGenomeFile` 不使 v3 成为运行时 schema。运行入口最终只获得 validated resolved v2。
- `RSI-Harness/src/harness/genome.ts:347-692` 的 `validateHarnessGenome` 要求 v2；`272-345` 的 `validateSkill`/`validatePromptTemplate` 保留 inline 与 file-backed 两种 authoring 形式；`1072-1154` 的 `renderHarnessSystemPrompt` 只广告 inline resources，避免把 Pi 自行发现的 file-backed Skill 假装成 RSIH inline `load_skill` target。

因此，v1 adapter MUST 接受“v3 authoring/assembly → canonical resolved v2”边界，不得让 Pi 或 replay scheduler直接解释 v3 component manifest。任何 v3 source、component 或 base 的变化只有经过重新 resolve、validate 与新 freeze 才可影响新 run。

### 2.2 现状兼容层：per-run `resolvedGenome`

`RSI-Harness/src/harness/genome-loader.ts:253-339` 的 `resolveHarnessGenome`/loaded path 按 direct path、project `.rsih/genomes`、user `~/.rsih/genomes`、builtin seed 顺序解析。`RSI-Harness/src/pi-cli-runtime.ts:1005-1174` 的 `runPiCli` 在每次 process/run 开始时计算一个 `resolvedGenome`，后续 settings、prompt、extension、resource hook 与 policy 闭包使用该对象；当前没有 run 中途重装 v3 manifest/components。

Builtin seed 的 stale/modified/shadowed 检测保护用户副本不被静默覆盖，但这只是 seed compatibility behavior，不是 Contract §7.19/§7.20 的 content lock。v1 exact adapter MUST 在 run/session freeze 后以 materialization/lock identity 为准，而不是以 seed 状态或可变路径为准。

### 2.3 现状兼容层：session `rsih.genome` snapshot

`RSI-Harness/src/pi-cli-runtime.ts:287-303` 的 `storedGenomeReference` 从 session JSONL 最后一个 custom `rsih.genome` entry恢复 `genome`、`reference`与`baseDirectory`。同文件 `864-920` 的 `session_start` 仅在 prior `genome_id` 或 `reference` 不同时 append `{reference, baseDirectory, genome_id, genome}`。无显式 `--genome` 的 resume优先使用该snapshot；显式 `--genome`会重新解析当前文件。

当前 restore 路径 **不会** 重新调用 `validateHarnessGenome`，也不验证 snapshot digest、`baseDirectory` policy或 entry完整性；`runPiCli`会直接把恢复对象用于 settings/resource/extension projection。因此被修改或损坏的 session JSONL可在 legacy路径改变恢复对象，不能把“上次曾是validated v2”当作本次已验证。

该 snapshot只冻结保存的 resolved v2 JSON值，并 **不冻结** file-backed Skill/template/extension/theme内容：snapshot中仍是source path，Pi后续 discovery/argv可重新读取当前bytes。因此文件可在session恢复时漂移、消失、被替换或经symlink指向别处；同id/reference内容变化还可能不产生新snapshot。当前`rsih.genome`既不是Contract §7.19 `MaterializationManifest`，也不是Contract §7.20 `SkillLock`。

### 2.4 现状兼容层：inline、file-backed 与 ambient 双轨

现状实际存在三路资源来源：

1. **inline**：`RSI-Harness/src/pi-cli-runtime.ts:528-590` 的 `registerInlineResources` 注册 `load_skill` tool、`/skill:<name>` slash commands 与 inline template commands；
2. **file-backed**：`RSI-Harness/src/harness/pi-projection.ts:137-143` 的 `projectGenomeResources` 经 Pi `resources_discover` 提供 Skill/template/theme paths；extension 因无同类 hook 而经显式 argv 注入；
3. **ambient**：Pi 默认 discovery 与 Genome source 并存。`RSI-Harness/src/harness/pi-projection.ts:150-162` 的 `genomeResourceIsolationArgs` 只在 `resources.isolate=true` 时加入禁止 ambient Skill/template/theme/extension 的 flags，并保留 repository instructions。`RSI-Harness/src/pi-cli-runtime.ts:750-773` 的 runtime labels 也会合并 Pi 实际 command/tool source。

这是一条兼容双轨/三来源路径，不具备 exact closure、无 collision 保证，也不能作为 deterministic replay 的 released Skill source。

### 2.5 Compatibility requirements

在迁移完成前，RSIH MAY 继续运行不启用 Skill Evolution 的 legacy Genome；但 MUST 明确标记为 `legacy_unlocked`，不得把其输出提交为满足本规范的 replay/materialization evidence。启用 v1 integration 时：

- v3 只用于 authoring/assembly，resolved v2 只作为 run configuration；
- legacy inline/file-backed/ambient resource 不得被隐式视为 released Skill；
- Genome-owned released Skill 必须全部来自第 4 章 locked bundle；
- ambient resources 若 profile 明确允许，MUST 与 Genome-owned registry 分域且不得进入 reproducible replay；replay profile MUST 隔离 ambient discovery；
- 同 session 的 `rsih.genome` 必须迁移为能关联 exact `SkillLock` 的 snapshot；只保存 source path 不满足迁移条件；
- 恢复任何 v1 snapshot时，MUST 在投影settings/resources/extensions之前重新验证v2 schema、snapshot/lock digest、session binding与`baseDirectory` policy；失败必须标记legacy且禁止v1 execution/replay，或直接fail closed；
- name collision、相同 resource 被 inline 与 file-backed 双重曝光、或 legacy path 与 locked bundle 重合时 MUST fail closed，而不是按加载顺序选 winner。

### 2.6 v1 新增 normative adapter

新增 adapter MUST 建立以下边界，且目前尚待实现：

```text
v3 authoring inputs
→ existing deterministic assembly
→ validated resolved Genome v2
→ resolve Genome Skill declarations to exact active SkillArtifactRefs
→ GMS authoritative closure read
→ RSIH static validation + exact materialization
→ Contract SkillLock session freeze
→ resolved resource registry
→ Pi/HarnessRunner/Composite execution
```

Adapter MUST 把“配置 identity”和“Skill content identity”分开：Genome id/reference 不证明 artifact bytes；只有 exact ref、artifact digest、manifest/lock digest 与 frozen activation sequence 可证明 execution source。Adapter 不得从 Graph node、projection display name、filesystem basename 或 `latest` 推导 exact ref。

## 3. Exact Skill binding into S

### 3.1 MetaRSI S-slot boundary

MetaRSI `S-slot` 是 RSIH 私有运行绑定点。`HarnessPatch` 对 S-slot 的唯一合法 Skill binding 是一个 released exact `SkillArtifactRef`（Contract §7.3）；不得绑定 `CandidateArtifactRef`（Contract §7.4）、Graph node ID、naked name、alias、semver range 或 `latest`。Resolver MUST 通过 GMS Spec §11.1 `GetArtifact`/`GetActiveHead` 的权威路径校验 ref 与 active expectation，不得以 Graph projection 替代。

S-slot binding MAY 使用 RSIH 私有 immutable record，例如记录 patch identity、exact Skill ref、materialization/lock ref、session、binding digest 与创建时 activation sequence；该 record 不得命名为共享 DTO，不得被序列化成伪造的 Contract §7 shape。私有 record 的 machine identity MUST 承诺 exact ref 与 lock digest，而非 display name。

### 3.2 `HarnessPatch` content rule

`HarnessPatch` 只携带/引用 exact Skill identity 和 RSIH 自身 patch metadata。对于 `composite`，patch MUST NOT 携带、复制、改写或局部覆盖 children、edges、retry policies、failure handlers、ports 或 orchestration permissions。Composite control flow 只来自 GMS canonical artifact（Contract §8.4）及其 locked exact children；任何 control-flow 变化都必须形成新 artifact revision，不能成为 patch-local override。

Candidate 处于 validation/replay 阶段时可由 Host/GMS 通过第 6 章专用 replay packet执行，但它 **不能绑定到 live S-slot**、不能写入 session `SkillLock`，也不能被普通 Genome/Pi resource registry发现。

### 3.3 Three-kind exact rendering

三种 kind 的 canonical artifact/body 唯一来源分别是 Contract §8.1–§8.4；本文不复写 shape。S-slot renderer MUST 根据 exact ref 中 kind 选择固定 renderer，kind/ref/body 不一致时 fail closed：

- `procedure`：按 canonical step order 生成 deterministic `SKILL.md` 指令视图；保留每步 pre/postcondition 与 failure-action 语义，不重排、不由模型摘要；
- `step_guidance`：固定呈现 causal context、decision guidance/branches、future path summary；included/omitted 只能来自明确 render profile，不能把 omission 伪装为完整 artifact；
- `composite`：呈现 Composite 自身说明、exact child identity、named ports 与由 protected scheduler消费的 control metadata；不得把 child正文拼接成新的未版本化 super-prompt，也不得由 S-slot 自行解释 DAG。

Rendering MUST 承诺 source exact ref、canonical body digest、render profile digest、renderer/runtime adapter exact ref、permission profile digest 与输出 bytes digest。Human-readable heading、换行、Unicode normalization、path 与 ordering 规则必须由 golden fixture 冻结。

### 3.4 Freeze semantics

S-slot binding 在 session start 成功发布 `SkillLock` 后冻结。随后 active head、Graph、Genome source file、ambient resource 或 display name变化均不得改变该 session 的 bound ref/bytes。需要新 revision 时 MUST 启动新 session 并重新 materialize；不得在旧 session 里“refresh latest”。若 exact historical artifact仍可解析且 lock 校验通过，旧 session MAY 按原 bytes继续；若不可解析或 lock stale/digest mismatch，则 fail closed，不能降级到当前 head。

## 4. Materialization

### 4.1 Q37-B authority flow

本文将 Contract traceability Q37 的 content-addressed bundle + lock + freeze 具体 RSIH 方案称为 **Q37-B**。但当前 Contract §7.19 `MaterializationManifest`没有version/manifest digest或对应exact-ref规则，而Contract §7.20 `SkillLock.materialization_ref`要求`VersionedRef`。本文不得私自决定该ref的`id/version/digest`映射。Contract在新版本中冻结Manifest exact identity或新增共享`MaterializationRef`、并由Contract §16 fixture固定之前，RSIH MUST 禁用S8/MT6的成功publish/freeze路径并fail closed；禁止用`bundle_digest`或任意私有record猜填共享字段。以下流程均以该版本化门槛已解除为前提：

【修订 v1.1，CTR-001】Contract §6.2.1 已冻结 materialization exact identity（`(materialization_id, manifest_version, manifest_digest)` 三元组、三层 digest 分工、freeze/cache 规则、`legacy_unlocked` 兼容结论），并以 `$FIX/materialization` fixture 固定；本模块按该决议实现，`bundle_digest` 严禁作为 `materialization_ref.digest`（`BUNDLE_DIGEST_NOT_MANIFEST_REF`）。fixture 全绿前 S8/MT6 成功路径仍按 disabled 报告。

```text
RSIH active-root request
→ GMS authoritative active-head/artifact closure read
→ closure preflight validation
→ deterministic rendering into private staging
→ compute candidate Manifest/Lock under frozen Contract identity rule
→ full static validation of staged files/Manifest/Lock
→ atomic content-addressed publish
→ session-start freeze
```

GMS input authority与读取语义以 GMS Spec §8.1（ledger/active head 而非 Graph）、§11.1（`GetArtifact`/`GetActiveHead` exact read）和 §12.7（S8 exact materialization inputs）为准。GMS 提供 active roots、transitive exact artifacts/bytes、digests、ports、permissions 与 freeze activation sequence；GMS 不生成 bundle、`SKILL.md`、Manifest 或 SkillLock。

### 4.2 Exact closure

Materializer MUST 从 exact active roots 开始遍历所有 explicit dependencies 与 Composite exact children，得到 transitive exact closure。每个节点必须验证 ref、kind、artifact digest、canonical body bytes、dependency/child exactness、ports、permissions 与 required extensions。Closure ordering MUST 独立于 response arrival、map iteration 和 filesystem order；同一输入/profile/sequence必须得到相同 ordered closure。

Root 在 freeze 时 MUST 等于权威 active head；transitive child/dependency 必须 exact resolvable，并满足 Contract §8/§9.5 与 GMS Spec §12.7 的 release/active条件。Traversal 遇到 cycle、missing ref、deactivated/stale root、digest mismatch 或 torn activation sequence MUST fail closed且不 publish。

### 4.3 Content-addressed bundle and deterministic files

每个 artifact 必须生成 deterministic `SKILL.md` 与必要的 protected scheduler metadata；所有 bytes 在写盘前完成 canonical rendering和 digest。不得把 source path 下原文件直接视为 rendered output。Materialization preimage 至少承诺：

- root 与 transitive exact refs及 canonical body digests；
- source ref/body/view/render profile exact identity与 digest；
- renderer/runtime adapter exact identity；
- permission profile exact identity与 digest；
- normalized relative paths、每文件 content digest、排序规则；
- freeze activation sequence与 required extension interpretation。

Contract §7.19 `MaterializationManifest` 与 §7.20 `SkillLock` 的 shape、必填性和 digest语义只引用 Contract，不在本文复制。RSIH MAY 定义私有 bundle directory layout、private metadata files和 bundle hash preimage，但这些定义 MUST 由 versioned profile/golden fixture冻结，MUST 能无歧义投影到共享 DTO，且 MUST NOT 增删共享 DTO 字段或改变其 identity。

### 4.4 Path, resource and filesystem safety

Exact render/materialization profile MUST 冻结并由digest承诺资源上限，至少包括closure node count/depth、file count、per-file bytes、total output bytes、staging quota、render time/work units与private metadata size。Materializer MUST 在遍历/分配/写盘前检查可预见上限，并在流式处理期间持续enforce；超过任一上限必须终止、清理或隔离staging且不得publish。不得依赖实现默认、OS剩余空间或OOM作为规范限制。

所有 bundle path 必须是 UTF-8、相对、normalized、无空 segment/`.`/`..`、无绝对路径、drive/UNC prefix、NUL、platform separator ambiguity或 normalization collision。Publisher MUST 在已验证的 staging root 内以 descriptor-relative/no-follow 操作创建文件；禁止跟随 symlink、hardlink escape、mount/path substitution或 TOCTOU 重解析。最终逐文件 real containment、mode、size与 digest必须复核。

Bundle SHOULD 只读；可执行 bit、device、socket、FIFO、symlink和未声明文件一律拒绝。Cache lookup命中后仍须验证 manifest/lock/profile与所有 file digests，不能只信 directory name。

### 4.5 Atomic publish and session freeze

Materializer MUST 在独立 staging directory 完成所有写入、fsync/等价 durability、全量复核与 digest计算，然后以单次 atomic rename/CAS发布到 bundle-digest address。目标已存在时，只有 manifest、lock-compatible metadata 与所有 bytes bit-identical 才可复用；同 address 不同 bytes是 integrity conflict。失败或取消时删除/隔离 staging；禁止 partial publish、逐文件暴露或把 incomplete bundle 标为 current。

`SkillLock` 在 session start 绑定 session、Contract版本化修复后可构造的materialization exact ref、roots、完整 locked closure与 activation sequence（shape见 Contract §7.20）。同一 session只能成功冻结一次；相同 request digest可幂等返回原 lock，不同 digest必须冲突。Head 在 freeze 后变化不影响旧 session，任何更新都必须新session。Head在closure read与freeze之间变化时，本次staging/materialization失败且不产生lock；因为尚未成功freeze，调用方MAY对同一目标session使用新的materialization request/idempotency identity从头重试，也MAY废弃目标session，但不得自动rebase或复用旧staging。

### 4.6 Cache and garbage collection

Content-addressed cache是派生存储，不是 artifact authority。Cache eviction MAY 删除无引用 bundle，但不得删除活跃 session lock所需 bytes；恢复时只能按 exact materialization/lock identity读取。GC、cache warming或镜像不得改变 manifest/lock digest。Remote cache若启用，必须先在本地对 exact bytes做完整校验，且 replay profile默认不允许网络取回。

## 5. Static validation

### 5.1 Validator input and authority

RSIH static validation分为两个protected phase。`closure preflight`在render前只验证exact refs、closure、extensions、permissions、ports、DAG与资源上限；通过后才可创建private staging。Deterministic rendering完成、candidate Manifest/Lock按已修复Contract规则可计算后，`full static validation`复核所有staged files、digests、profiles、Manifest/Lock、path安全与二次render一致性。Contract §9.5的`materialized`对应完整staging bytes及candidate materialization record已形成但尚未对session发布；`static_validated`对应full phase通过。只有`static_validated`才可执行§4.5 atomic publish/freeze并进入后续replay；preflight pass不得冒充完整static pass。

两个phase输入均来自 exact artifact/closure bytes、Contract refs、exact render/permission/runtime profiles、Host authority cap与 GMS freeze sequence。Validator MUST 是 pure/protected、无网络、无 wall-clock/random/model dependency，并输出 RSIH 私有 validation report。它不创建 GMS authoritative validation ledger、`ReplayResult`、`ReleaseDecision`，不推进 GMS proposal/release state；GMS 根据 GMS Spec §2.4、§5.2–§5.6、§11.1决定是否接受该 report并写权威记录。

### 5.2 Required checks

Validator MUST 至少检查：

1. envelope/body schema与 kind/ref一致，canonical body digest和 artifact ref digest正确；
2. exact refs完整，无 candidate、naked name、`latest`、Graph ID；
3. required extensions全部已知且版本/digest可验证；unknown optional extension不得改变 core语义；
4. artifact permissions、children union、orchestration permissions与 effective permission profile不超过 Host cap（Contract §14.2）；
5. Procedure/Step Guidance作为 child时具备 Contract §8.2–§8.3要求的 exact `JsonSchemaRef` ports；
6. Composite input/output/edge mappings类型兼容、DAG无环、retry有界、fallback child已声明；
7. Composite active child exact refs与 freeze sequence一致；child revision不得按 lineage head动态替换；
8. root/closure完整、无 duplicate identity/digest conflict、dependency cycle或 closure order ambiguity；
9. source/rendered file path、normalization、case-fold/Unicode collision、path traversal、symlink/hardlink/path escape；
10. manifest/lock/files/body/view/render/permission-profile之间 digest与 exact identity一致；
11. deterministic rendering二次运行 bit-identical；
12. runtime adapter/profile与 fixture要求的 capability/permission可满足。

### 5.3 Composite validation detail

Composite shared state和 execution transition只引用 Contract §9.5。Static validation MUST 使用 canonical artifact中 exact active children，不能在本地补 child、按 name discover、选择当前最新 revision或把缺失 child降级为 skip。DAG cycle不得用 runtime retry掩盖；retry只对单 child bounded attempts生效。Named ports与 `JsonSchemaRef`要求来自 Contract §8.2–§8.4、§9.5（Q26）。Failure action值域只引用 Contract §13.3（Q27），本文不复列。

### 5.4 Validation outcome

每个phase的Report MUST 区分 pass、semantic failure、infrastructure inconclusive，并包含phase、input digest、validator/profile exact ref、检查项、相关exact refs、private diagnostic refs与report digest。Preflight通过仅允许创建private staging；只有完整staging bytes与candidate materialization record形成后，才可进入Contract `materialized`；只有full report完整通过才可进入Contract `static_validated`并atomic publish。任何未知、读取撕裂、adapter不可用或无法证明条件均fail closed。RSIH不得把“process exited 0”当作artifact semantic pass，也不得把report中的自由message用作machine action。

## 6. HarnessRunner and replay adapter

### 6.1 Live versus replay

`HarnessRunner` MUST 有显式模式隔离：

- **live**：执行已 released、locked、materialized的 S-slot/Composite；仍受 Host authority cap、session lock与 deterministic scheduler约束，但可使用 profile明确允许的 live services；
- **replay**：只消费 Host frozen replay plan与 Contract §7.10 `ReplayRequest`（shape只引用，不复制），在 sealed recorded fixture和 deterministic fake runtime中执行 baseline/candidate paired runs。

Candidate只允许出现在 GMS 创建、Host 校验并调度的 replay packet内；不得进入 live registry/session lock。Mode必须进入 private run identity与 output digest，禁止 live output冒充 replay。

### 6.2 Q29-B sealed recorded fixture

本文将 Contract traceability Q29 的 recorded path + fake runtime方案称为 **Q29-B**。Replay必须遵循 Host Spec §6.1–§6.8：Host拥有 scheduling/frozen plan/correlation；RSIH只执行。Fixture输入必须来自 settled Segment及 Evidence/Path seals，且在执行前封闭 exact refs、ordered events、initial state、assertions、adapter/profile、seeds、permissions和 capture policy。

Deterministic fake runtime MUST 替代网络、真实外部服务、wall clock、OS randomness与未固定模型响应。它按 fixture提供 versioned clock/random/tool/provider/filesystem responses，并在未声明调用、调用次序/参数不符、fixture耗尽或 nondeterministic access时 fail closed。Recorded output是数据，不得携带改变 runner policy的指令。

### 6.3 Paired baseline/candidate execution

Baseline与 candidate MUST 使用：同一 fixtures及顺序、同一 adapter/profile/fake runtime version、同一 initial state、permissions、environment allowlist、clock/random seeds与 capture policy。二者 MUST 使用独立 workdir、cache namespace、session、process state和 side-effect sink；不得让 baseline warming、candidate输出或完成顺序影响另一侧。

Required coverage包括 success、failure、recovery。Merge还必须覆盖 source A、source B、overlap与 conflict families（Host Spec §6.5；Contract §9.4.6、§10.4）。任何 family缺失、被跳过或因一侧早成功而短路，raw output必须标记 incomplete/inconclusive，不能伪装成功。

### 6.4 Isolation

Replay worker MUST 默认：空白临时 workdir、只读 exact bundle、独立 writable scratch、独立 cache、最小 env allowlist、network disabled、fixed locale/timezone、virtual monotonic/wall clock、seeded random、stable process limits与 deterministic ordering。不得继承用户 HOME、ambient Pi resources、credential、agent cache、session history、repository未声明文件或 daemon state。

Host授权的 permission cap是上限；fixture/profile可进一步收窄。任何 network、filesystem、subprocess、tool或 model access都必须经 adapter代理并被录制。Late async task、background process与 side channel在 run terminal前必须收敛/终止；terminal后输出只能作为 late diagnostic，不得改写结果。

### 6.5 Raw execution output and result authority

RSIH只产私有 immutable raw execution output，至少可追踪 replay request/plan、run side、fixture、exact artifact/materialization、adapter/profile、attempt、captured events、assertion observations、resource usage、private outcome与 output digest。该 record不是 Contract §7.11 `ReplayResult`，不得携带 U1 winner或 release建议。

Host按 Host Spec §6 correlation；GMS按 GMS Spec §5.3与§11.1校验 correlation/RSIH output并 canonicalize权威 `ReplayResult`。只有 GMS evaluator可按 GMS Spec §5.4–§5.6计算 hard gates与 U1。RSIH不得更改 counts、critical regression、utility vector或以 adapter success代替 semantic success。

### 6.6 Retry and attempt identity

Host Spec §6.6控制 replay retry。RSIH只执行 frozen plan中明确授权的 bounded infrastructure retry；每次 attempt append新私有 record，复用 replay identity但使用唯一 attempt correlation。Semantic assertion failure、permission denial、digest mismatch、fixture mismatch与 nondeterminism不可 retry成 pass。改变 fixture、seed、adapter、permission、environment或 artifact必须新 ReplayRequest，而非同 request retry。

### 6.7 Replay adapter publication gate

实际 `HarnessRunner`、sealed fixture loader、fake runtime、paired isolation与 raw-output→GMS adapter尚待实现。发布前 MUST 具备 bit-identical repeat fixture、baseline/candidate cross-contamination negative fixture、success/failure/recovery、merge A/B/overlap/conflict、clock/random/network escape、late output与 adapter crash fixtures。

## 7. Composite execution

### 7.1 Canonical control and state

Composite artifact/body唯一来源是 Contract §8.4；共享 execution状态、child attempt与 terminal语义唯一来源是 Contract §9.5。RSIH不得重列或扩张共享状态机。Scheduler只消费 locked exact Composite及其 locked exact children，按 DAG dependency readiness执行；不得从 Graph、name registry或 current head替换 child。

### 7.2 DAG and named ports

Q26要求每个 child使用 named ports并绑定 exact `JsonSchemaRef`（Contract §8.2–§8.4、§9.5）。Scheduler MUST 在 child启动前验证所有依赖成功产生符合 schema的输出，执行 canonical edge mapping，并在映射前后校验 schema/digest。未满足依赖的 child不可 ready；同一 ready set的调度顺序必须由 versioned deterministic policy与 lexical child identity稳定决定，而非 map iteration或并发完成顺序。

Cycle在 static validation阶段拒绝。并行执行若被 profile允许，visible merge顺序、event ordering与 output digest仍必须 deterministic；存在共享 side effect且不能证明 commutative/isolation时必须串行或 fail closed。

### 7.3 Retry, failure and fallback

Q27 failure action值域只引用 Contract §13.3，禁止本文复列或私增。Retry必须使用 artifact声明的 bounded `max_attempts`及 exact reason policy；不得无限循环、指数时间依赖或按自由 message推断 retryability。Attempts耗尽后只能按 artifact中对该 child/reason声明的 closed handler处理。

Fallback只能跳转到 canonical Composite中显式声明的 exact child id，并且其 dependencies可满足、ports兼容、permissions已包含。Scheduler不得按 name加载外部 Skill、动态生成 fallback或把 current active revision替换 locked child。Child failure、skip、fallback、human request与 inconclusive必须保留完整 causal event，不能被最终成功覆盖。

### 7.4 Permissions

Q18 permission requirement按 Contract §8.4、§14.2计算：all exact children permission union + orchestration permissions，再由 Host authority cap限制。RSIH static validator计算并证明 effective set；runtime在每次 capability use再次enforce。Child不能继承兄弟未使用的 credential/scope；orchestrator不能借 child扩权；fallback/retry不扩大 cap。无法证明 scope containment或 runtime adapter不支持精确enforcement时 fail closed。

### 7.5 Exact revisions and replay

任何 child ref变化都要求新的 Composite revision、重新 static validation、重新 materialization及 whole-Composite paired replay（Contract §8.4、§9.5）。Ablation MAY 作为诊断，但 MUST NOT 替代 whole-Composite replay、不得用于满足 release gate。Live执行只能使用已经通过 GMS release并在 session lock内的 whole Composite。

### 7.6 Scheduler output

Scheduler产出 RSIH 私有 execution trace：Composite/lock/run identity、deterministic schedule decisions、child exact refs/attempts、port value digests、failure-handler decisions、permission decisions与 final raw outcome。该 trace不是共享状态机 record或权威 replay/evaluation record；GMS只在 GMS Spec §5.3 canonicalization后生成共享结果。

## 8. Pi integration

### 8.1 Current Pi behavior and exact dependency lock

现状 Pi integration如第 2.4 节：inline Skill通过 `load_skill`与 slash commands，file-backed Skill通过 `resources_discover`，ambient discovery默认并存且可由 isolation flags部分关闭。该行为是兼容层，不满足 exact materialization。

当前依赖锁定事实为 `RSI-Harness/package.json:27-39` 与 `package-lock.json:11-26`：`@earendil-works/pi-agent-core`、`pi-ai`、`pi-coding-agent`、`pi-tui`均 exact `0.84.3`，Node engine为 `>=22.19.0`。v1 conformance只针对该组合。任何 Pi或Node最低版本升级都必须重新运行并冻结 resource discovery、command/tool registration、same-call result、session resume与 isolation fixtures，不能按 semver假设行为等价。

### 8.2 Resolved resource registry

启用 v1时，adapter MUST 从 `SkillLock`和 published bundle构造一个私有 immutable `resolved resource registry`。Registry key必须含 exact Skill identity/kind/materialized path/file digest；display name只作 UI。每个 Genome-owned Skill只能映射到一个 locked source，所有加载、slash command、S-slot与 Composite child resolution必须回到同一 entry并复核 digest。

Exact materialized bundle MUST 成为 **唯一 Genome-owned execution source**。Legacy inline content和原 file-backed source path不可同时作为可执行副本；它们只能在迁移时解析为 exact active ref，随后由 bundle替代。无法映射、映射到多个 ref、name collision、inline/file-backed双重曝光或 bundle digest不一致均 fail closed。

Ambient resource若 live profile显式允许，必须标记为 non-Genome/non-replay namespace，不能遮蔽 registry name、不能被 S-slot/Composite引用，也不能进入 released replay。Replay必须关闭 ambient Skill/template/extension discovery，并固定 repository instruction policy。

### 8.3 Pi loading and session behavior

Pi extension在 `resources_discover`时只暴露 registry中 locked normalized paths；`load_skill`和 slash commands若保留兼容 UI，必须通过 exact registry lookup读取同一 rendered bytes，而不是闭包中的 mutable inline content。Session resume必须在任何settings/resource/extension projection之前重新验证resolved v2 schema、snapshot/lock/session digest binding、`baseDirectory` policy、Manifest、bundle与全部file digests；不得重新读取original authoring source。

`rsih.genome` snapshot迁移必须保存/关联 lock exact identity与 digest，并以 lock变化而非仅 `genome_id/reference`决定是否新 snapshot。旧 file-backed snapshot若无可验证 lock，MUST 标记 legacy并禁止进入 v1 replay；可由显式新 session重新 materialize，不能静默升级旧 session。

### 8.4 Host Tool Proxy same-call return

Pi中的 Memory tools必须满足 Host Spec §5.1–§5.10：Host构造的 Contract §7.18 exact `ToolProxyResult`通过 **同一次** Pi tool call返回；禁止 placeholder、`tool_execution_end`后补写、下一条 message注入、UI/side channel替代或 terminal后 late rewrite。若 Pi需要 content-block wrapper，必须是可逆 deterministic wrapper并由 S2 fixture证明 canonical bytes一致（Host Spec §5.5）。

S2是硬发布门槛（Host Spec §5.1；Contract §15 S2）。在 same-call exact success/failure/timeout/cancel/late fixtures及 digest procedure通过前，RSIH MUST 不注册、不广告 Skill retrieval capability。

### 8.5 GMS retrieval readiness gates

按 GMS Spec §10.5，`skill_get`当前虽绑定 Contract §7.15 `GuidanceView`，但 Host Spec §5.6对 success结果的无条件 budgets/citations/watermark要求与 closed `GuidanceView`不兼容。因此 integration profile MUST 禁用 `skill_get`并 fail closed；RSIH不得私添字段、伪造 `ExploreResult` wrapper或旁路返回 artifact body。只有 Host/Contract后续版本化修复且 fixtures通过后才能启用。

按 GMS Spec §10.8，Contract §7.16 v1缺少 top-level omitted evidence/Skill refs承载位。会因 total/evidence/Skill cap产生 top-level omission的查询 MUST fail closed，不得作为成功截断；RSIH不得用 UI、省略日志或自由文本补足。Guidance branch omission只可在既有 typed字段与 exact policy允许时返回。

【修订 v1.1，CTR-002】Contract §12.7.1（§8.4–§8.6 覆盖范围）已冻结 tool-specific success validation matrix 并解除上段 Host §5.6 与 closed `GuidanceView` 的冲突；`skill_get` 的 enable 以该条款 readiness gate（`tools/` fixture + policy digest 全绿）为准，RSIH 仍不得私添字段或旁路返回 artifact body。

【修订 v1.1，CTR-003】Contract §12.7.2 已冻结 `ExploreResult.omissions` carrier 并解除上段“top-level omission MUST fail closed”限制（改为按 §12.7.2 一致性义务校验）；RSIH 仍不得用 UI、省略日志或自由文本替代 exact carrier。

### 8.6 Pi publication gate

Pi `0.84.3` fixtures、S2 gate、exact registry、file-backed snapshot迁移、ambient collision/isolation与 lock-resume验证尚待实现。门槛未关闭时，legacy Pi运行可继续作为非规范模式，但不得宣称符合 v1 Skill Evolution，不得提交其输出为 release evidence。

## 9. Failure behavior

### 9.1 Fail-closed principle

任何 exact identity、digest、closure、permission、path、adapter、fixture、lock、port或 state无法证明时，RSIH MUST fail closed。失败不得降级到 naked name/current/latest、Graph、ambient resource、original source file、cached unverified bytes或部分 Composite。Model-provided recovery建议不具有 machine authority。

共享 reason code只来自 GMS Spec §11.3–§11.5及适用 Host Spec §5.9，failure action只来自 Contract §13.3。RSIH不得创建跨模块 reason enum。

### 9.2 Private `RsihDiagnostic`

RSIH MAY 定义私有 `RsihDiagnostic` 供日志/trace，字段可包括：private schema version、diagnostic id、phase、category、human message、informational retry projection、source policy exact ref、run/session/attempt correlation、exact input refs、manifest/lock/profile refs、path（脱敏 normalized）、expected/observed digest、cause diagnostic refs与 diagnostic digest。它 MUST 保持模块私有，不得放入共享 `reason_code` slot、不得驱动 Contract状态或冒充 GMS/Host policy。`informational retry projection`只能机械复制frozen Host/GMS policy已决定的结果，MUST NOT决定status、retryability、same-request retry、new attempt、failure action或fallback；缺source policy exact ref时必须省略。跨模块上报必须由接收方按其权威 closed registry映射；无法映射则 fail closed。

### 9.3 Required cases

- **missing exact ref**：在resolve/closure/static/runtime任一阶段停止；禁止按lineage/name查latest。若freeze前authoritative head变化，废弃staging；尚未产生lock时可对同一目标session使用新的materialization request/idempotency identity从头重试，已成功freeze则任何更新必须新session。若历史lock恢复时exact ref缺失，旧session不可执行。
- **digest mismatch**：隔离可疑 cache/bundle/output，停止 publish/execution；不得重算后覆盖 expected digest。只有从 GMS authoritative bytes发起全新 materialization才能恢复。
- **stale lock**：若 session已冻结且 bytes/ref仍完整，head后来变化本身不使锁漂移；但 lock与session/manifest/closure不一致、freeze前 sequence撕裂或 source无法验证时必须停止。更新 Skill必须新 session/rematerialize，禁止原 session refresh。
- **permission overflow**：static或runtime capability use立即失败；不得删掉 capability后继续执行声称等价的 artifact，也不得由 fallback扩大权限。
- **adapter failure**：分类为 infrastructure failure/inconclusive raw outcome，保留 attempt；只接受 Host frozen plan授权的 bounded retry。未知/非确定性 adapter不得产生 semantic pass。
- **Composite child failure**：严格按 Contract §9.5及 artifact handler处理；未声明 reason、attempt超限、invalid fallback、port mismatch或 permission denial时停止/形成相应 terminal raw outcome，不得动态选 child。

### 9.4 Infrastructure versus semantic failure

确定性 profile limit exceeded属于同一request的terminal semantic/policy rejection，不可retry。Infrastructure failure只包括 worker/adapter启动失败、瞬态 capture/publish I/O失败、受保护 runtime不可用等“未能可靠观察语义”的情况，通常只能上报 failed/inconclusive raw execution；是否同 request retry由 Host Spec §6.6与 exact policy决定。Semantic failure还包括 fixture assertion、artifact-declared failure path、permission denial、port/schema violation与 deterministic child failure；不得通过 infrastructure retry重跑成 pass。

Digest/ref/lock/permission/path安全失败属于 integrity/hard failure，不得标为普通 flaky infrastructure。RSIH不得自行将任何类别映射为 GMS release outcome；GMS按 GMS Spec §5.3–§5.6处理。

共享 reason-code registry、transport precedence 与 status/retry/new-attempt 映射以 Contract §13.7.1（修订 v1.1，CTR-004）冻结的 policy 为准；RSIH 加载时 MUST 校验 policy digest。

### 9.5 Atomicity, cancellation and late data

Materialization失败/取消 MUST 不产生 partial publish、Manifest current marker或 session lock。Execution terminal前必须停止/收敛 child、subprocess与 async tasks；terminal后到达的数据标记 late diagnostic，禁止修改 raw terminal record、Host Delivery/Pi result、fixture outcome或 GMS ledger。Side channel、日志、UI、next message与cache metadata不得替代正式 output channel。

同 request相同 canonical digest可幂等返回已保存终态；同 key不同 digest冲突。恢复必须从完整 immutable checkpoint/record开始，不能继续使用未知状态的 staging directory或 half-executed child。

### 9.6 Release blockers

以下均为 v1 发布 blockers：actual exact adapter/materializer/`HarnessRunner`/Composite scheduler/fixtures未实现；Contract §7.19/§7.20尚未冻结`SkillLock.materialization_ref`可引用的shared exact identity及golden fixture；render/resource/permission profile的schema、单位、数值、digest preimage与golden fixture尚未冻结；S2未通过；`skill_get`尚按 GMS Spec §10.5禁用；top-level omission按 GMS Spec §10.8仍需 fail closed；Pi `0.84.3`/Node behavior fixtures未冻结；legacy file-backed `rsih.genome` snapshot尚未迁移到 lock-backed freeze。任何 blocker存在时必须清楚报告 capability disabled，不得写“已实现”或以人工演示代替 fixture。

【修订 v1.1，CTR-001】上述“§7.19/§7.20 未冻结 shared exact identity 及 golden fixture”blocker 已由 Contract §6.2.1 冻结并以 `$FIX/materialization` fixture 固定；本模块按该决议实现，其余 blocker 照旧，fixture 全绿前 S8/MT6 成功路径保持 disabled。

## 10. Slice obligations

本章细化 RSIH 在 S1、S4、S8、S9、MT1–MT6 的职责。每个 obligation严格使用同一十项模板；其他模块权威不因 slice 转移。MT1–MT3与MT5中 RSIH通常是 no-op boundary/conformance guard；MT4负责 bilateral merge execution；MT6负责 bundle/lock。

### 10.1 S1 — Exact refs and canonical digest

#### Inputs

Contract §16 golden fixtures、Contract §6 canonicalization规则、三 kind canonical artifact样本、GMS exact refs与 RSIH TypeScript bytes。

#### Preconditions

Schema/profile exact可解析；runner无网络、时钟与未固定随机；共享 DTO只按 Contract §7解释。

#### Authoritative writes

无生产共享/ledger写。RSIH MAY 写私有 conformance run record，不得修改 golden expected以适配实现。

#### Derived writes

Canonical bytes、computed digest、cross-language comparison与 private diagnostic report。

#### Outputs

RSIH与 GMS对同一 fixture产生 bit-identical canonical bytes/digest及一致 accept/reject；未知 required extension、non-exact ref一律拒绝。

#### Failure modes

Unicode/key ordering差异、非整数 core number、schema drift、digest/ref mismatch、required extension未知、fixture缺失。

#### Idempotency/CAS

同 fixture/profile重复运行 bit-identical；无生产 CAS。Fixture identity相同但 bytes不同视为 conflict。

#### Observability

记录 case id、schema/profile exact ref、computed/expected digest、validator版本与 private diagnostic；不发明共享 reason code。

#### Acceptance fixture

覆盖 Contract §16 minimal refs、Unicode、三 kind、candidate/released body equality、negative ref/digest/extension/path cases及 TypeScript/GMS parity。

#### Non-goals

Release/activation、修改 Contract schema、按 Graph/name解析、用文档示例替代 executable golden fixture。

### 10.2 S4 — Candidate validation and paired replay execution

#### Inputs

Contract §7.10 `ReplayRequest`、Host Spec §6 frozen plan、Candidate replay packet、baseline exact refs、sealed success/failure/recovery fixtures、adapter/profile exact refs。

#### Preconditions

GMS已绑定 immutable candidate并创建 request；Host已验证 settled seals、schedulability、permissions与 idempotency；candidate未进入 live S-slot。

#### Authoritative writes

RSIH不写 GMS validation/replay ledger。只 append私有 static-validation、run-attempt与 raw execution records。

#### Derived writes

Deterministic traces、captured output digests、resource usage与 Host correlation material；不计算 U1。

#### Outputs

隔离的 baseline/candidate raw outputs、完整 fixture-family coverage、明确 infrastructure/semantic classification，交 Host correlation与 GMS Spec §5.3 canonicalization。

#### Failure modes

Fixture/seal/digest不匹配、adapter nondeterminism、workdir/cache泄漏、permission overflow、missing family、timeout/crash、late output。

#### Idempotency/CAS

Frozen plan不变时重复 attempt保持输入一致；attempt append-only。只有 Host Spec §6.6授权的 infrastructure retry可复用 request identity。

#### Observability

按 replay request、plan、side、fixture、attempt、artifact、adapter/profile与 output digest端到端追踪；标出 skipped/incomplete family。

#### Acceptance fixture

同 inputs下 bit-identical；success/failure/recovery全跑；baseline/candidate顺序互换不变；cache/env/network/clock/random隔离；semantic failure不被 retry成 pass。

#### Non-goals

S-slot live binding、activation、权威 `ReplayResult`、release scoring/U1、修改 fixture outcome。

### 10.3 S8 — Exact materialization

#### Inputs

GMS Spec §12.7 active root exact refs、authoritative closure bytes/digests/ports/permissions/freeze sequence，以及 exact render/permission profiles。

#### Preconditions

Contract已以新版本冻结Manifest→materialization exact ref规则及Contract §16 fixture；否则本slice只能验证fail-closed、不得成功publish/freeze。门槛解除后，roots等于权威active heads；closure exact可解析且无cycle；Host cap允许；staging root安全；renderer/resource-limit/profile fixture已冻结。
（修订 v1.1，CTR-001：该门槛已由 Contract §6.2.1 冻结并以 `$FIX/materialization` fixture 固定，本模块按该决议实现，fixture 全绿即解禁本 precondition。）

#### Authoritative writes

RSIH原子发布 content-addressed bundle，铸造 Contract §7.19 `MaterializationManifest`与§7.20 `SkillLock`；不写 GMS artifact/head/ledger。

#### Derived writes

只读 cache index、bundle lookup、registry projection与 materialization metrics；不得成为 artifact authority。

#### Outputs

Deterministic `SKILL.md`/metadata bytes、完整 transitive closure、manifest、session lock及 immutable resolved registry。

#### Failure modes

Stale/torn head、missing child/dependency、digest mismatch、cycle、permission denial、path/symlink escape、render nondeterminism、partial I/O。

#### Idempotency/CAS

同 roots/profiles/sequence产生同 bundle/lock preimage；publish按 bundle digest CAS；同 session只冻结一次；冲突不覆盖。

#### Observability

Root→closure→files→bundle→manifest→lock correlation，记录 activation sequence、profile refs、computed digests与 publish phase。

#### Acceptance fixture

GMS Spec §12.7 fixture生成 content-addressed bundle/lock；head随后变化不影响 frozen session；path traversal/symlink/partial publish negative全部拒绝。

#### Non-goals

GMS生成 bundle、从 Graph materialize、按 latest更新 session、原地改 lock、让 source path成为 execution bytes。

### 10.4 S9 — Composite binding and execution

#### Inputs

Released exact Composite、locked exact children、Contract §8.4 control data、named `JsonSchemaRef` ports、Host cap、whole-Composite fixtures。

#### Preconditions

Static validation通过；children在 freeze时满足 active要求；DAG无环；ports/mappings兼容；retry bounded；bundle/lock完整。

#### Authoritative writes

RSIH只 append私有 Composite schedule/child-attempt/port-digest/raw-output records；不写共享 Composite lifecycle或 release ledger。

#### Derived writes

Schedule visualization、ablation diagnostics与 performance metrics；ablation不得成为 release evidence替代物。

#### Outputs

Deterministic whole-Composite raw execution、完整 child causal trace、permission enforcement与 terminal raw outcome供 Host/GMS。

#### Failure modes

Inactive/missing child、port mismatch、cycle、unbounded retry、unknown handler、invalid fallback、permission overflow、child nondeterminism、whole replay regression。

#### Idempotency/CAS

Locked child refs不变；同 inputs/profile/seeds产生同 schedule/output digest。Child ref变化必须新 Composite revision与新 lock。

#### Observability

Composite→DAG ready decisions→child attempts→port digests→handler/fallback→terminal trace；保留所有 failed/retried attempts。

#### Acceptance fixture

Q18权限 union+Host cap、Q26 typed named ports、Q27 bounded handler、exact fallback、deterministic ready ordering及 whole-Composite paired replay；ablation不能替代。

#### Non-goals

动态 child discovery、patch-local control flow、按 latest替换 child、GMS执行 child、RSIH release decision。

### 10.5 MT1 — Similarity assessment boundary

#### Inputs

GMS产生的 exact similarity workflow correlation；RSIH MAY 接收只读 refs用于确认没有执行请求。

#### Preconditions

GMS Spec §5.7、§11.1拥有 assessment authority；RSIH没有 similarity policy、feature ledger或 Graph mutation权限。

#### Authoritative writes

无。RSIH MUST NOT 写 `SimilarityAssessment`、score、band、policy或 `similar_to` projection source。

#### Derived writes

MAY 写 private no-op/conformance guard日志，证明请求未被路由到 materializer/runner。

#### Outputs

默认无执行输出；若误路由则返回模块私有拒绝诊断供 GMS/Host按其 closed policy处理。

#### Failure modes

RSIH被要求打分、读取 Graph推断 similarity、模型自报 score、assessment ref被误当 Skill ref。

#### Idempotency/CAS

No-op guard重复调用结果一致；无共享 CAS、无 ledger副作用。

#### Observability

记录 correlation、调用方、拒绝 phase与“authority belongs to GMS”；不得复制 feature/evidence正文到 RSIH状态。

#### Acceptance fixture

向 RSIH提交 similarity-assessment mutation/score请求，证明零 shared writes、零 runner execution并 fail closed；合法 GMS路径不受影响。

#### Non-goals

Feature extraction、score/band计算、merge trigger、Graph edge、proposal admission或 release。

### 10.6 MT2 — Proposal admission and dedup boundary

#### Inputs

GMS proposal/admission/dedup workflow correlation、SimilarityAssessment和双source exact refs的只读identity；不得包含RSIH可执行candidate packet。

#### Preconditions

Contract §15.2与GMS Spec §12.10定义MT2；GMS独占MergeProposal、admission、dedup keys、group-winner CAS与event authority，RSIH无proposal mutation能力。

#### Authoritative writes

无。RSIH MUST NOT创建/修改`MergeProposal`或event、执行admission/dedup、抢占group winner、写proposal queue或将proposal materialize。

#### Derived writes

MAY写private no-op/conformance guard与unsupported-routing metric；不得形成candidate body、bundle或可执行resource。

#### Outputs

默认无执行输出；误路由必须返回模块私有拒绝诊断供Host/GMS按其closed policy处理。只有后续MT4合法ReplayRequest才进入runner。

#### Failure modes

RSIH被要求admit/dedup、模型自报winner、proposal exact duplicate被当成新run、两个released refs被直接拼成live Skill。

#### Idempotency/CAS

No-op guard重复无副作用；source-pair/intent/group key和winner CAS完全由GMS执行。

#### Observability

记录correlation、调用方、请求类别与“authority belongs to GMS”；区分proposal请求、candidate synthesis与replay dispatch。

#### Acceptance fixture

把proposal/admission/dedup mutation误发到RSIH，证明零artifact/proposal/group/bundle写、零runner execution；合法GMS MT2路径不受影响。

#### Non-goals

Similarity scoring、proposal admission/dedup、group winner、candidate synthesis、conflict adjudication、source retirement。

### 10.7 MT3 — Merge candidate synthesis boundary

#### Inputs

GMS candidate-synthesis workflow correlation、admitted proposal及frozen A/B/evidence/conflict packet的只读identity；在合法ReplayRequest形成前不得作为RSIH executable input。

#### Preconditions

Contract §15.2与GMS Spec §12.11定义MT3；GMS独占synthesis attempts、protected body acceptance、conflict closure、CandidateRef binding与proposal event authority。

#### Authoritative writes

无。RSIH MUST NOT synthesize canonical body、解决blocking conflict、绑定CandidateRef、修改proposal state或把suggested body写入bundle/registry。

#### Derived writes

MAY写private no-op/conformance guard，验证candidate-synthesis请求没有进入materializer/live runner；不得产生canonical candidate bytes或authoritative static report。

#### Outputs

默认无execution/validation输出；误路由fail closed。GMS完成MT3并在S4/MT4创建合法ReplayRequest后，RSIH才可对immutable candidate执行pure static/replay checks。

#### Failure modes

模型要求RSIH合并文本、绕过conflict closure、动态选择source、把suggestion当CandidateRef、candidate未绑定即进入live/replay、RSIH自定accepted/rejected。

#### Idempotency/CAS

No-op拒绝可重复且无副作用；synthesis lease、唯一Candidate binding与proposal-event CAS由GMS执行。

#### Observability

记录proposal/candidate correlation、拒绝phase与误路由类型；不得复制evidence正文或生成共享reason/state。

#### Acceptance fixture

误发merge synthesis/body binding到RSIH时零candidate/artifact/ledger/bundle写；即使suggested body静态看似合法也不生成report/state；合法GMS CandidateRef只能经S4/MT4 packet进入sandbox。

#### Non-goals

Body synthesis、conflict resolution、CandidateRef binding、authoritative validation、ReplayResult、ReleaseDecision或activation。

### 10.8 MT4 — Bilateral merge replay execution

#### Inputs

GMS创建的 merge `ReplayRequest`、Host frozen plan、两 source baselines、merge candidate、A/B/overlap/conflict sealed fixture families。

#### Preconditions

Contract §7.10 merge request含 required source heads；Host Spec §6接受调度；adapter/profile exact；candidate仅在 replay sandbox可见。

#### Authoritative writes

RSIH append私有 bilateral attempt/raw-output records；不写权威 `ReplayResult`、U1、merge event或 release ledger。

#### Derived writes

Per-family correlation、output digests、isolation/resource metrics与 diagnostic ablation；不改变 baseline envelope。

#### Outputs

A侧相对A baseline、B侧相对B baseline、overlap/conflict按 frozen policy运行的完整 raw outputs，交 Host correlation与 GMS Spec §5.3–§5.6。

#### Failure modes

缺任一 family、baseline swap、cross-run contamination、source expectation/digest不符、adapter nondeterminism、critical branch未执行、infra crash。

#### Idempotency/CAS

Frozen plan与 family order不变；attempt append-only；相同 request不改变 source mapping。Fresh source expectation需要新 GMS attempt/request。

#### Observability

按 source A/B、family、baseline/candidate side、fixture、attempt、output digest追踪；明确 overlap/conflict policy exact ref。

#### Acceptance fixture

A/B均有 success/failure/recovery，另含 overlap/conflict；交换执行先后不改变结果；缺 family必为 incomplete；RSIH不产生 U1 winner。

#### Non-goals

选择 merge winner、计算 constrained Pareto、修改 conflict resolution、retire sources、activation。

### 10.9 MT5 — Merge decision and activation boundary

#### Inputs

GMS evaluation/release correlation MAY 被 RSIH只读观察；不接受 decision/activation mutation command。

#### Preconditions

GMS Spec §5.4–§6与§11.1独占 hard gates、U1、`ReleaseDecision`、version assignment、active-head CAS与 activation ledger。

#### Authoritative writes

无。RSIH MUST NOT 写 decision、assign version、activate/deactivate、更新 Graph或 release ledger。

#### Derived writes

MAY 写 private no-op audit和 capability-denied metric；不得把 raw execution success标为 accepted。

#### Outputs

无 release输出。只有 GMS后续提供 active exact ref，才可进入新 session的 S-slot/materialization。

#### Failure modes

RSIH/model self-score、把 exit 0当 accepted、直接 materialize candidate、绕过 source-head CAS、自动替换现有 session Skill。

#### Idempotency/CAS

No-op guard重复无副作用；所有 decision/head/version CAS由 GMS执行。

#### Observability

记录非法 mutation企图与 correlation；可观察 GMS released ref但不得重解释 outcome。

#### Acceptance fixture

向 RSIH提交 accept/activate命令，证明零 shared writes；candidate replay成功后仍不可 live绑定；仅 GMS active exact ref可触发 MT6。

#### Non-goals

Hard-gate/U1计算、ReleaseDecision、versioning、activation/deactivation、Graph projection、source retirement。

### 10.10 MT6 — Released merge bundle and lock

#### Inputs

GMS active merged exact root、authoritative transitive closure、render/permission profiles、freeze activation sequence与目标新 session。

#### Preconditions

Contract的materialization exact-ref门槛已解除；否则只验证disabled/fail-closed且零publish。门槛解除后，merge revision已由GMS released/active；sources retention不改变root exactness；closure/children exact、permissions合法；旧session不被原地升级。
（修订 v1.1，CTR-001：该门槛已由 Contract §6.2.1 冻结并以 `$FIX/materialization` fixture 固定，本模块按该决议实现，fixture 全绿即解禁本 precondition。）

#### Authoritative writes

RSIH按第 4 章原子发布 merged Skill content-addressed bundle，铸造 Contract §7.19 Manifest与§7.20 session SkillLock。

#### Derived writes

Resolved resource registry、cache index、bundle provenance view与 metrics；Graph remains GMS-derived且不由 RSIH写。

#### Outputs

Merged released exact Skill的 deterministic `SKILL.md`/Composite metadata、完整 closure、manifest、lock与唯一 Genome-owned Pi source。

#### Failure modes

Root不再 active、source/child missing、digest/lock stale、permission overflow、render/path collision、partial publish、legacy source双重曝光。

#### Idempotency/CAS

同 root/closure/profiles/sequence产生同 bundle；publish content-addressed CAS；session lock单次 freeze；新 active head要求新 session/rematerialize。

#### Observability

追踪 merged root、retained source refs（仅 provenance）、closure、files、bundle/manifest/lock digests、sequence与 Pi registry installation。

#### Acceptance fixture

Released merged Step Guidance与 Composite各一例；session freeze后 head变化不漂移；source retained不造成 name collision；candidate/latest/Graph/path fallback全部拒绝。

#### Non-goals

Merge synthesis/evaluation/activation、修改 source disposition、旧 session hot swap、从 Graph或 naked name生成 bundle。
