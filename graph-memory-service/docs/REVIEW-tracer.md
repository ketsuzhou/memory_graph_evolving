# graph-memory-service tracer 实现评审(对照 docs/PLAN.md)

## 总体结论(3-5 句)

实现已具备 12 条冻结路由、标准库/内存适配器、Evidence 两阶段可见性、Recall 精确 Space 预授权与 `complete/empty/partial` 降级、Exploration 终态及 `CITATION_NOT_SERVED` 围栏，且基础响应形状与包依赖方向总体符合 PLAN。评审仍发现 2 个 Blocker：Grant `purpose` 完全未参与授权，以及 Exploration 的 explore/redirect/submit 未逐操作重验 operation/purpose/expiry，均可绕过冻结的权限围栏。另有 9 个 Major，集中在 Exploration start 幂等、结果预算、空 Space 的零版本响应、严格 JSON 深层未知字段、Recall deadline、错误 request_id、Opaque ID 全面校验、Evidence 用例层不变量及 Submit summary 规则。用户提供的 `go test ./...` 全绿事实不改变这些结论，因为现有测试在若干关键断言上缺失或放宽；本次环境策略禁止非交互 shell，故未独立执行 `go test/go vet/go build/git status`。

## 逐节对照表(PLAN 章节 | 状态 ✅/⚠️/❌ | 证据 file:line | 备注)

| PLAN 章节 | 状态 | 证据 file:line | 备注 |
|---|---:|---|---|
| §1 Scope, authority, and frozen rulings | ✅ | `go.mod:1-3`; `cmd/server/main.go:1-43`; `internal/store/memory/store.go:24-41` | 仅标准库、单实例内存 store、静态 token 与 tracer 入口均符合已知裁决；**ADR 0040 已接受**。 |
| §2 Tracer sequence and completion order | ⚠️ | `internal/evidence/service_test.go:205-457`; `internal/recall/service_test.go:212-338`; `internal/exploration/service_test.go:164-399` | 主链路按依赖顺序存在，但权限、协议和预算缺口意味着冻结步骤尚不能视为全部完成。 |
| §3 Package structure and dependency direction | ✅ | `internal/ports/ports.go:1-37`; `internal/evidence/service.go:1-9`; `internal/recall/service.go:1-9`; `internal/exploration/service.go:1-12`; `internal/store/memory/store.go:1-13` | domain 无仓库内依赖；ports 仅依赖 domain；用例不依赖 httpapi/具体 store；memory 不依赖 httpapi。 |
| §4 Memory Protocol v1 HTTP contract（总） | ⚠️ | `internal/httpapi/handler.go:27-45`; `internal/httpapi/conformance_test.go:16-31` | 12 条 method/path 与 OpenAPI 一致；字段级和行为级仍有下列偏离。 |
| §4.1 Common wire rules | ❌ | `internal/httpapi/decode.go:27-89`; `internal/httpapi/handler.go:824-910`; `internal/httpapi/handler.go:85-96` | 重复键/尾随值正确映射 `INVALID_JSON`，顶层未知字段正确映射 `INVALID_REQUEST`；但 event/link 深层未知字段、部分 Opaque ID 及请求 Content-Type 未严格执行。 |
| §4.2 Error envelope and status codes | ⚠️ | `internal/httpapi/handler.go:73-81`; `internal/httpapi/handler.go:987-1039`; `internal/authz/service.go:75-100` | 信封键形状正确；但可用 request_id 未回显；`SPACE_FORBIDDEN` 分支不可达，而其与 `GRANT_MISSING` 的精确分类需按“spec 疑问”裁决。 |
| §4.3 Tenant initialization | ✅ | `internal/httpapi/handler.go:185-226`; `internal/store/memory/store.go:76-99` | 首次 201、相同重放 200、冲突 409；token→Tenant+bootstrap Principal 绑定为 **ADR 0040 已接受**。 |
| §4.4 Principal registration | ✅ | `internal/httpapi/handler.go:228-261`; `internal/store/memory/store.go:101-117` | 必填、枚举、display_name 与 201/200/409 语义符合。 |
| §4.5 Space registration | ✅ | `internal/httpapi/handler.go:263-321`; `internal/store/memory/store.go:119-143` | shared/private owner 不变量与幂等符合；重放忽略 commit 推进的 Version 为 **ADR 0040 已接受**。 |
| §4.6 Exact Grant registration | ⚠️ | `internal/httpapi/handler.go:323-416`; `internal/store/memory/store.go:145-161`; `internal/authz/service.go:62-100` | 注册字段、过期边界、精确数组检查基本符合；但运行时未执行 purpose fence，错误类别 truth table 待 spec 裁决。端口 `Grants/Principal` 增强为 **ADR 0040 已接受**。 |
| §4.7 Evidence Batch stage/commit/status | ⚠️ | `internal/store/memory/store.go:215-304`; `internal/evidence/service.go:27-51`; `internal/evidence/service_test.go:205-457` | stage/commit 重放、冲突、不可见→原子发布、状态响应基本符合；版本序义为 **ADR 0040 已接受**。但完整 Evidence 不变量只在 HTTP 解码层验证，服务/存储 commit 未重新完整验证。 |
| §4.8 Recall | ⚠️ | `internal/recall/service.go:25-56`; `internal/authz/service.go:62-101`; `internal/recall/service_test.go:212-338` | 完整 Space 集先授权、missing/expired 拒绝不读、`complete/empty/partial` 与来源信息保留正确；`deadline_ms` 完全未参与执行。 |
| §4.9 Exploration | ❌ | `internal/exploration/service.go:105-225`; `internal/exploration/service.go:241-364`; `internal/store/memory/store.go:370-477` | 私有 Space 403、step 次数、终态、操作幂等和 `CITATION_NOT_SERVED` 已实现；但逐操作授权、start 语义幂等、结果预算、正版本及 found-summary 规则存在实质偏离。`RecordServed` 端口增强为 **ADR 0040 已接受**。 |
| §5 Core Go domain signatures | ⚠️ | `internal/domain/types.go:5-211` | 冻结类型/字段基本齐全；新增未导出实现契约可接受，但除 `Grant.ActiveAt` 外领域不变量没有下沉到 domain。 |
| §6 Storage port signatures | ⚠️ | `internal/ports/ports.go:11-37`; `internal/store/memory/store.go:215-290`; `internal/store/memory/store.go:402-477` | 端口签名及 **ADR 0040 已接受** 的 `Grants/Principal/RecordServed` 增强符合，mutex 临界区存在；但 Commit 本身不做完整 manifest 验证。 |
| §7 Required behavioral invariants | ❌ | `internal/authz/service.go:62-100`; `internal/store/memory/store.go:370-385`; `internal/exploration/service.go:155-225` | Evidence 可见性和 source identity 基本满足；purpose/逐操作授权、start 幂等、结果预算等核心不变量未满足。 |
| §8 Test strategy | ⚠️ | `internal/httpapi/contract_test.go:36-80`; `internal/recall/service_test.go:212-338`; `internal/exploration/service_test.go:268-365` | 仅标准库、fake clock、httptest 符合；但合同测试只覆盖顶层未知字段，且缺少 purpose、start 冲突、deadline、空 Space pin、结果预算、request_id 回显等关键负例。 |
| §9 Acceptance-criteria test map（all 10） | ⚠️ | `internal/evidence/service_test.go:205-314`; `internal/recall/service_test.go:212-338`; `internal/httpapi/authority_test.go:11-59`; `internal/httpapi/conformance_test.go:16-31` | PLAN 指定的 GMS 测试文件/函数均存在；Host 侧条目在本仓库不可核验，且 GMS 用例存在上述断言缺口。 |
| §10 Done criteria | ⚠️ | `internal/httpapi/tracer_e2e_test.go:120-265`; `cmd/server/main.go:20-43`; `docs/PLAN.md:412-414` | GMS tracer E2E 与独立入口存在，入口为 **ADR 0040 已接受**；跨仓库十步场景及两仓 `build/vet` 本次不可核验，且 Blocker/Major 尚未消除。 |

## 问题清单(按 Blocker/Major/Minor/Nit 分级,每条:标题、PLAN 引用、证据 file:line、影响、建议;没有就写「无」)

### Blocker

#### B1. Grant `purpose` 参数被接收但完全未参与授权
- **PLAN 引用**：§4.6 Exact Grant registration（purpose fence），§4.9（每次操作按 purpose 授权），§7；`docs/PLAN.md:128-143,253,386`。
- **证据**：`internal/authz/service.go:62-90` 的 `AuthorizeExact(... purpose, operation)` 只检查 Space、operation 和 `ActiveAt`，循环内没有读取 `grant.Purpose`；调用方明确分别传 lifecycle/tool_plane：`internal/evidence/service.go:28,44`、`internal/recall/service.go:31`、`internal/exploration/service.go:115`。
- **影响**：只要 Grant 的 operations 中写入目标操作，`tool_plane` Grant 可授权 evidence/recall，`lifecycle` Grant 也可授权 exploration.start，purpose fence 形同虚设，属于权限边界绕过。
- **建议**：授权覆盖条件必须同时满足 tenant、principal、`grant.Purpose == purpose`、exact Space、operation、`ActiveAt(now)`；增加双向 wrong-purpose 拒绝测试及 HTTP 403 错误码断言。

#### B2. Exploration 后续操作不校验对应 operation、purpose 或 Grant expiry
- **PLAN 引用**：§4.9 “Every operation is authorized against … purpose, and expiry”，§7；`docs/PLAN.md:253,388`。
- **证据**：仅 Start 调用 `AuthorizeExact(... exploration.start)`（`internal/exploration/service.go:105-119`）；Explore/Redirect/Submit 只调用 `loadSession`（`internal/exploration/service.go:155-188`），而 `loadSession` 仅校验 tenant/principal（`internal/exploration/service.go:214-224`），没有调用 Authorizer、检查对应 operation 或比较当前时间/Grant expiry。
- **影响**：仅持有 `exploration.start` 的 Principal 可继续 explore/redirect/submit；Grant 在会话期间到期后仍可操作，冻结的 operation fence 与严格过期语义被绕过。
- **建议**：每个操作在任何 Recall 读取/状态变更前，针对 session 固定的 exact Space 集分别执行 `exploration.explore/redirect/submit` + `tool_plane` + 当前时刻授权；增加缺操作、错误 purpose、恰好到期、到期后一纳秒测试，并确认失败不读取、不耗预算。

### Major

#### M1. Exploration Start 的 idempotency key 不比较语义内容
- **PLAN 引用**：§4.1、§4.9、§7；`docs/PLAN.md:58,231,253,385`。
- **证据**：`internal/store/memory/store.go:370-381` 发现 `(tenant, principal, key)` 已存在便直接返回旧 session，未比较 request_id、space_ids、query、max_steps、max_results；与 step 操作在 `internal/store/memory/store.go:430-440` 的 `SemanticJSON` 比较形成直接反差。现有测试只覆盖 step operation 冲突：`internal/exploration/service_test.go:323-365`。
- **影响**：同 key 改 query/Space/预算会错误得到 `200 duplicate:true`，而非 `409 IDEMPOTENCY_CONFLICT`；调用方无法发现语义冲突。
- **建议**：为 Start 存储规范化的完整 decoded semantic request（含 request_id），冲突时 fail-closed 且不改变原 session；补齐 201/200/409 三态 HTTP 测试。

#### M2. Evidence event/link 的未知字段被接受，违反严格 JSON
- **PLAN 引用**：§4.1、§8；`docs/PLAN.md:56,395`。
- **证据**：provenance 显式遍历未知键（`internal/httpapi/handler.go:782-788`），但 event 解码 `internal/httpapi/handler.go:824-870` 和 link 解码 `internal/httpapi/handler.go:877-910` 只读取已知键，没有 `rejectUnknownFields`/allowed-key 遍历。合同测试 `internal/httpapi/contract_test.go:36-80` 只测 tenant 顶层未知字段。
- **影响**：如 `events[0].tenant_id`、`links[0].unexpected` 会被静默忽略并成功 stage，违背冻结协议“unknown fields → 400 INVALID_REQUEST”，也削弱调用方拼写错误检测。
- **建议**：对每个嵌套对象执行与 provenance 相同的 exact-key 检查，返回精确 JSON path；补 event/link/provenance 嵌套未知字段测试。

#### M3. `deadline_ms` 仅被解析，Recall 执行完全不受相对预算约束
- **PLAN 引用**：§4.8、§7；`docs/PLAN.md:205,222-225,387`。
- **证据**：HTTP 将值写入 `RecallRequest.DeadlineMS`（`internal/httpapi/handler.go:506-534`），但 `internal/recall/service.go:25-56` 调用 store 时只传 query/maxResults，未创建 deadline context、计时或产生 `timed_out`；现有测试只填固定值（`internal/recall/service_test.go:278,319`）。
- **影响**：合法请求可无限超过 1–30000ms 的调用方预算，`timed_out` 降级不可达；“bounded Recall”不成立。
- **建议**：以服务时钟/真实 timer 建立相对 deadline（保留父 context 更早截止），超时返回 200 typed degradation，并确保只保留已授权 Space 的已取得 items；增加超时前/恰好超时/父 context 更早取消测试。

#### M4. 错误信封不回显请求体中可用的 `request_id`
- **PLAN 引用**：§4.2；`docs/PLAN.md:61-75`，尤其 `:70`。
- **证据**：端点校验和服务错误普遍以 `nil` 调用 writer（例如 `internal/httpapi/handler.go:496-539`）；`writeInvalidRequest` 只有 `body != nil` 才提取 request_id（`internal/httpapi/handler.go:1002-1028`），`writeServiceError` 始终生成新 ID（`internal/httpapi/handler.go:1032-1039`）。
- **影响**：Recall/Exploration Start 已成功解码 request_id 后发生 400/403/404/409 时，客户端关联 ID 被替换，违反冻结 error envelope 的相关性保证。
- **建议**：将已解码 request_id 显式传给统一错误 writer；无法可信解码时才生成；增加 INVALID_REQUEST、SPACE_NOT_FOUND、GRANT_EXPIRED、IDEMPOTENCY_CONFLICT 的 echo 测试。

#### M5. Common Opaque ID 规则未覆盖嵌套数组和 path IDs
- **PLAN 引用**：§4.1；`docs/PLAN.md:54-59`。
- **证据**：动态 path 直接截取后使用，未调用 `validateID`（`internal/httpapi/handler.go:112-142`）；Recall/Start 的 `space_ids` 直接转换（`internal/httpapi/handler.go:530-532,580-582`）；anchor/citation IDs 只解码不校验（`internal/httpapi/handler.go:625-645,660-677,691-708`）；link_id 只查重、不查 1–128 bytes（`internal/httpapi/handler.go:890-899`）。
- **影响**：空或超长 ID 可能被接受，或错误映射为 404/422 而非 400 INVALID_REQUEST；实现与 OpenAPI `OpaqueID` 不一致。
- **建议**：建立统一递归 Opaque ID validator，覆盖 path、所有 ID 数组、provenance.host_instance_id、event/link/citation anchor；为 0/129 字节分别补 HTTP 负例。

#### M6. Exploration 只执行 step 数预算，未执行 server-side result budget
- **PLAN 引用**：§4.9、§7；`docs/PLAN.md:231,237,253,388`。
- **证据**：session 保存 `Budget.MaxResults`（`internal/exploration/service.go:123-130`），Start/Redirect 使用该单次上限（`:139,174`），但 Explore 直接使用请求 `limit`（`:160`）；状态只跟踪 `StepsUsed`（`internal/domain/types.go:190-196`、`internal/exploration/service.go:259-305`），没有结果消耗计数。
- **影响**：`max_results=1` 的 session 可通过 Explore `limit=100` 返回超过 session 预算的结果，且可跨多个 step 累积无限结果（受 steps 数量限制但不受 MaxResults 限制）。
- **建议**：冻结明确的计数规则后，在 store 原子操作中跟踪/扣减结果预算，Explore 上限至少取 session 剩余预算与 request limit 的最小值；补单步越界及多步累计越界测试。

#### M7. 空 Space 启动 Exploration 会返回 schema 禁止的 `memory_version: 0`
- **PLAN 引用**：§4.9；`docs/PLAN.md:233`（positive integer）。
- **证据**：新 Space 版本初始化为 0（`internal/httpapi/handler.go:312`），授权原样 pin 当前版本（`internal/authz/service.go:93`），Start 响应直接输出该值（`internal/httpapi/handler.go:590-600`）；只有 commit 后才从 1 起推进（`internal/store/memory/store.go:276-286`）。
- **影响**：对尚无 commit 的合法 shared Space 启动 Exploration 会产生不符合 PLAN/OpenAPI 的 201 响应，客户端 schema 校验失败。
- **建议**：根据“spec 疑问”裁决初始版本语义；在裁决前至少不得发出 schema-invalid 成功响应，并增加空 Space Start 合同测试。

#### M8. Evidence 完整领域不变量只在 HTTP 层执行，Service/Store 可提交无效 manifest
- **PLAN 引用**：§3（domain owns invariants）、§4.7、§6、§7；`docs/PLAN.md:30-44,187,379,383`。
- **证据**：`domain/types.go:96-141` 只有数据结构；`evidence.Service.Stage` 授权后直接 `store.Stage`（`internal/evidence/service.go:27-35`）；Store Stage 只做幂等并强制 state（`internal/store/memory/store.go:215-249`），Commit 直接发布（`:254-290`），没有重验 events 顺序、links、provenance、terminal outcome 等。
- **影响**：任何非 HTTP 调用或未来适配器可 stage/commit 空 events、悬空 link、非法 kind/hash 等，违反“commit validates staged batch completely”和框架中立权威边界。
- **建议**：把完整 manifest validation 放入 domain/evidence 用例并在 Stage 与 Commit 调用；HTTP 只负责 wire 类型/错误 path 映射；增加直接 Service 调用的无效候选与 commit 前失败不可见测试。

#### M9. `found=true` 时空 `summary` 被接受并提交
- **PLAN 引用**：§4.9；`docs/PLAN.md:249`（summary 仅可在 `found=false` 时为空）。
- **证据**：HTTP 仅检查 found 时 citations 非空及 !found 时 summary 为空（`internal/httpapi/handler.go:697-703`）；领域操作同样只拒绝 !found+非空 summary（`internal/exploration/service.go:332-337`），没有拒绝 found+空 summary。
- **影响**：可产生违反冻结 SubmitRequest 不变量的 terminal session，之后无法修正。
- **建议**：在 HTTP 和领域层拒绝 `found=true && summary==""`（按领域不变量映射 422，或按最终裁决），增加“失败不终结 session、可随后有效提交”测试。

### Minor

#### m1. 请求 `Content-Type` 未校验为 `application/json`
- **PLAN 引用**：§4.1；`docs/PLAN.md:52`。
- **证据**：`ServeHTTP` 只校验 Authorization 后直接读/解 JSON（`internal/httpapi/handler.go:84-96`）；唯一 Content-Type 设置是响应端（`internal/httpapi/handler.go:987-990`）。
- **影响**：`text/plain`、缺失 Content-Type 的有 body 请求也会被接受，与冻结媒体类型契约不一致。
- **建议**：对有 JSON body 的协议路由校验规范化 media type；错误状态码需先按“spec 疑问”裁决，并补缺失/错误/带 charset 三类测试。

### Nit

无。

## spec 疑问（没有就写「无」）

1. **403 Grant 错误码缺少规范化 truth table。** PLAN 同时列出 `SPACE_FORBIDDEN`、`GRANT_MISSING`、`GRANT_EXPIRED`（`docs/PLAN.md:75`），但只明确 missing Space=404、unauthorized/expired=403（`:225`），没有定义“无任何 Grant”“有正确 operation 但不含 Space”“含 Space 但 purpose/operation 不符”“仅存在过期覆盖 Grant”各自应使用哪个 code。现有测试因此接受两个 code（`internal/recall/service_test.go:236`）；建议冻结逐情形映射。
2. **空 Space 的初始/pinned version 语义矛盾。** Space 由实现初始化为 0（`internal/httpapi/handler.go:312`），ADR 0040 的 commit 序号从 1 开始（`internal/store/memory/store.go:276`），但 Exploration 响应要求 pin positive integer（`docs/PLAN.md:233`）。PLAN 未说明空 Space 应拒绝 Start、以 1 表示空快照，还是允许 0；需裁决后同步 PLAN/OpenAPI/实现。
3. **Exploration `max_results` 的计费口径不够明确。** §7 称其为 server-side result budget（`docs/PLAN.md:388`），但协议同时给 Explore 独立 `limit`（`:237`），响应又没有 `remaining_results`。需冻结它是“每次检索上限”“全 session 累计 served item 数”还是“去重 citation 数”，以及 start items 是否扣减。
4. **错误 Content-Type 的状态码未列入 mapping。** §4.1 要求 JSON media type（`docs/PLAN.md:52`），§4.2 状态表（`:75`）未列 415；需明确返回 400 INVALID_REQUEST 还是 415（并补充稳定 code）。
5. **Recall 六种 degradation 的判定表不完整。** PLAN 给出 enum（`docs/PLAN.md:222-225`），但未定义“零 items + backend error”应为 `unavailable` 还是 `partial`、何时为 `stale`。当前实现所有 post-auth store error 都是 partial（`internal/recall/service.go:36-45`）；建议冻结各状态的进入条件，避免不同 adapter 漂移。

## 无问题章节清单

- §1 Scope, authority, and frozen rulings（所列静态 token、单实例 memory store、端口增强、版本语义、PutSpace 重放及 tracer main 均按 **ADR 0040 已接受**）。
- §3 Package structure and dependency direction。
- §4.3 Tenant initialization。
- §4.4 Principal registration。
- §4.5 Space registration。
- §4.7 中“stage 后不可见、成功 commit 全量可见、同/异 commit_id 资源幂等、batch_id Tenant 内唯一”的子项。
- §4.8 中“完整 Space 集先授权、失败不读、不扩大 scope、complete/empty/partial、source Space/version/citation 保留”的子项。
- §4.9 中“private Space 403、step 次数耗尽 429、submitted 终态、step operation semantic idempotency、`CITATION_NOT_SERVED`”的子项。

---

### 验收标准自查

1. ✅ `docs/REVIEW-tracer.md` 已创建，且包含指定六个主体章节与分级问题格式。
2. ✅ 逐节对照表覆盖 PLAN §1-§10，并逐项展开 §4.1-§4.9。
3. ✅ 每条问题与 spec 疑问均给出实现/测试 `file:line` 证据和 PLAN 章节/行号引用。
4. ⚠️ 本会话唯一被写入的文件是 `docs/REVIEW-tracer.md`；源码、测试、PLAN、OpenAPI 均未写入。环境策略拒绝非交互 shell，因此无法执行 `git status`；以评审前 docs 文件清单仅有 `docs/PLAN.md`、写入后仅新增本报告，且全部写工具调用均仅指向本报告，作为替代自查。用户提供的 `go test ./...` 全通过未在本会话重跑。
