# graph-memory-service 测试报告（2026-09-09）

## 结论

**全绿，可交付。** 全量测试套件（含竞态检测）零失败，`go vet` 无告警，`go build ./...` 成功。

## 执行环境

- 机器：本机（ubuntu，zhoujie22）
- 工具链：go1.26.1 linux/amd64（`/home/zhoujie22/go/bin/go`）
- 依赖面：仅 Go 标准库（`go.mod` 无第三方依赖，符合 PLAN.md 冻结裁决）
- 测试形态：内存适配器 + httptest HTTP 服务器；**不涉及真实 Postgres / Pi**（PLAN.md §1 冻结裁决："Tests use in-memory adapters and HTTP test servers, not real Postgres or Pi"）

## 总量

| 指标 | 数值 |
|---|---|
| 测试包 | 12（另有 cmd/server、ports 两个包无测试文件） |
| 顶层测试函数 | 79 |
| 实际执行用例（含 t.Run 子测试） | 188（79 顶层 + 109 子测试） |
| 失败 / 跳过 | 0 / 0 |
| `-race` 竞态检测 | 12 包全部 ok |
| `go vet ./...` | 干净 |
| 构建 | `go build ./...` 成功（server 二进制 ≈ 9.3 MB） |

## 分包明细

| 包 | 顶层用例 | 覆盖率 | 说明 |
|---|---|---|---|
| internal/httpapi | 24 | 78.0% | OpenAPI 路由对拍、401 信封、严格 JSON 校验、请求日志、协议字段完备性、跨仓库 tracer e2e |
| internal/store/memory | 12 | 64.5% | ports 的内存适配器（Registry/Grant/Evidence 等） |
| internal/exploration | 10 | 82.5% | Room 共享围栏内的 start/explore/redirect/submit 生命周期、预算与版本钉扎 |
| internal/evidence | 6 | 72.1% | 证据 staging、语义幂等冲突、原子提交后可见 |
| internal/skillproposal | 6 | 80.0% | 提案/候选治理路由、未知字段拒绝 |
| internal/consolidation | 5 | 82.9% | 整合流程 |
| internal/recall | 4 | 86.4% | 精确 Space 列表的有界 Recall、类型化降级、引用 |
| internal/authz | 3 | 80.4% | Bearer 认证、Grant/过期精确判定、purpose fence 复核 |
| internal/causal | 3 | 60.7% | 因果证据链 |
| internal/pattern | 2 | 87.7% | Pattern 路由在 curation purpose 下的读/拒绝语义 |
| internal/dive | 3 | 45.8% | 潜读（dive）服务器裁决的终态探索 |
| internal/domain | 1 | 20.0% | 值类型与不变量（大量表驱动子测试；部分类型尚未被上層引用故覆盖率偏低） |

覆盖率为 `go test -cover` 的语句覆盖率；domain 偏低主要因 tracer 阶段部分值类型尚未接入用例路径。

## 测试覆盖的关键裁决点（对应 PLAN.md 冻结裁决）

- 认证占位：常量时间 Bearer 比较，缺失/无效 → 精确 401 信封（`TestMissingAndInvalidAuthenticationReturnExactUnauthorizedEnvelope`）
- 首个租户初始化绑定部署 token；后续请求从绑定派生身份，不接受请求方自报租户（`TestValidBearerTokenAllowsTenantInitialization`、`TestOperationalPayloadCannotSelectTenantPrincipalOrGrant`）
- 协议面无 Host 数据库 / DAG 变更路由（`TestProtocolHasNoHostDatabaseOrDAGMutationRoute`）
- 共享/私有 Space 证据首回合 Recall 无跨 Space 泄漏（`TestGMSTracerRecallsFirstTurnSharedAndPrivateEvidenceWithoutCrossSpaceLeakage`，跨仓库 tracer 验收项）
- 严格 JSON：未知查询参数 / 未知事件与链接字段一律拒绝（`TestCurationRoutesRejectUnknownQueryParameters` 等）
- 时间戳必须规范 UTC RFC3339Nano（`TestGrantTimestampsRequireCanonicalUTCRFC3339Nano`）
- OpenAPI 3.1（openapi/memory-protocol.yaml）与实际路由一致（`TestMemoryProtocolV1RoutesMatchOpenAPI`）

## 环境注意事项（不影响测试结论）

1. 本机 Go 安装于 `~/go`，GOROOT 与 GOPATH 同目录（每次执行有告警，建议解包到独立目录如 `~/go-toolchain`）。
2. 默认构建缓存 `~/.cache/go-build` 存在 root 属主残留文件，普通用户执行会 permission denied；本次使用独立 `GOCACHE=/home/zhoujie22/.cache/go-build-gms` 规避。修复：`sudo chown -R zhoujie22:zhoujie22 ~/.cache/go-build`。
3. `go` 不在默认 PATH（位于 `/home/zhoujie22/go/bin`）；httpapi 的 tracer e2e 用例会自行拉起 `go` 子进程，需保证 PATH 含 go 目录，否则该用例报 exec 失败。

## 复现命令

```bash
cd /home/zhoujie22/river2_0/graph-memory-service
export PATH=/home/zhoujie22/go/bin:$PATH
export GOCACHE=/home/zhoujie22/.cache/go-build-gms GOPATH=/home/zhoujie22/.gopath-gms
go build ./... && go vet ./... && go test ./... -count=1 -race -cover
```

## 运行时集成验证（同日补充：multica Gen1 大迁移收尾）

上一节基线之后，multica Gen1 的 BM25/vector hybrid seed、embedding 邻居、LLM 路径选择与 automatic structural projection builder 已完成运行时集成。收尾验证使用仓库捆绑工具链（`/home/zhoujie22/river2_0/.tools/go-1.26.8/bin/go`，独立 GOCACHE/GOMODCACHE，全程无网络调用外部 provider）：

- `gofmt -l .` 无输出；`go build ./...`、`go vet ./...`、`go test ./...` 全部通过。
- 定向 `-race` 通过：`internal/exploration`、`internal/store/memory`、`internal/httpapi`、`internal/consolidation`、`internal/retrieval`、`internal/navigation`、`internal/projectionbuilder`、`internal/recall`（新增模块当前无测试文件，遵守“未索取不添加测试”约束）。
- 启动配置核验：`retrieval-mode`、`navigation-mode`、`builder-enabled` 默认分别为 `off`/`off`/`false`；`required` 模式在 base URL/model/API key 环境值缺失时启动即失败；密钥仅从命名环境变量读取，不出现在启动日志、access log 或快照。
- HTTP/OpenAPI 对拍：`POST /v1/explorations/{session_id}:navigate` 路由、`NavigateRequest`/`NavigateResponse`/`NavigationStep` 封闭 schema、explore 的 `route_kind=embedding` 枚举与 handler 实际序列化一致；请求解码拒绝未知字段。
- 快照核验：`navigation_runs` 恢复后经 `orEmptyMap` 保证非 nil；`published_order` 接受 legacy 空间键对象形式并转换/推断租户；BM25 索引与 embedding 缓存为进程本地派生数据，不进入快照；快照内 `stage_keys`/`session_by_key` 为协议幂等键，无任何密钥材料。
- 离线冒烟（真实进程 + curl，provider 全关、builder 开启小阈值）：stage/commit 后 builder 经 replay/CAS 发布投影并随 `-state` 持久化（`projection_build` 事件）；BM25 Recall 对两条内容给出有区分度的排序（1.0 vs 0.249）；Exploration 以 BM25 seed 启动并沿 builder 生成的 `mentions` 关系边完成一跳遍历（`route_kind=relation`、`projection_version=2`）；submit 引用 fence 通过；`navigation-mode=off` 下 `:navigate` 返回 `503 PATH_SELECTION_UNAVAILABLE`；kill 并以同一 `-state` 重启后租户绑定恢复、BM25 懒重建、Recall 正常。
