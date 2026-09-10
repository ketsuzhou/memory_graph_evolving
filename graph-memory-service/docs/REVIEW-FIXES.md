# graph-memory-service review 修复回执(对应 docs/REVIEW-tracer.md)

状态:全部 Blocker/Major/Minor 修复完毕,62 项测试全绿(`go test -count=1 ./...`),
并新增轻量磁盘持久化。冻结的 kiro 测试文件未改动;所有修复均伴随新增回归测试。

## 逐项修复

| ID | 修复方式 | 实现位置 | 回归测试 |
|---|---|---|---|
| B1 purpose fence | 授权覆盖条件加入 `grant.Purpose == purpose`,lifecycle 与 tool_plane 双向隔离 | `internal/authz/service.go`(grant 循环内 purpose 不匹配即 continue) | `internal/authz/purpose_fence_review_test.go`(双向 wrong-purpose 拒绝,镜像方向用独立 principal 排除合法覆盖) |
| B2 exploration 逐操作授权 | Explore/Redirect/Submit 每次调用 authorizeStep 重验 operation/purpose/expiry/Space | `internal/exploration/service.go` | `internal/exploration/review_fixes_test.go` |
| M1 start 语义幂等 | `Start` 比较 sameStart(RequestID/Query/Budget/SpaceIDs,排除派生状态),同 key 不同语义 → 409 IDEMPOTENCY_CONFLICT | `internal/store/memory/store.go` | exploration/review_fixes_test.go |
| M2 深层未知字段 | event/link 按冻结键白名单校验,未知键 → 400 INVALID_REQUEST | `internal/httpapi/handler.go`(decode 侧 allowed-key 检查) | `internal/httpapi/review_fixes_test.go` |
| M3 recall deadline | `deadline_ms` → `context.WithTimeout`;超时 → `timedOut()` 降级(state=partial/degraded,保留已得 items) | `internal/recall/service.go` | `internal/recall/deadline_review_test.go` |
| M4 request_id 回显 | `writeServiceError(w, requestID, err)` 将可用 request_id 写入错误信封 | `internal/httpapi/handler.go` | httpapi/review_fixes_test.go |
| M5 Opaque ID 全覆盖 | path 段 batch_id/session_id 走 validPathID;space_ids/anchor_citation_ids/citation_ids 数组元素逐一校验 | `internal/httpapi/handler.go` | httpapi/review_fixes_test.go |
| M6 result budget | `ExplorationSession.ResultsServed` 去重累计(start 按去重 citation 计一次,step 按返回条目计);超预算截断并 429 RESULT_BUDGET_EXCEEDED | `internal/domain/types.go`(字段含冻结计费注释)、`internal/exploration/service.go`、`internal/store/memory/store.go`(RecordServed 去重) | exploration/review_fixes_test.go |
| M8 manifest 双重验证 | 完整领域不变量在 Stage 与 Commit 两处执行 | `internal/evidence/service.go`(validateManifest) | `internal/evidence/manifest_review_test.go` |
| M9 found/summary 规则 | found=true 且 summary 为空 → 422 SUMMARY_REQUIRED | `internal/exploration/service.go`(submitOperation) | exploration/review_fixes_test.go |
| m1 Content-Type | 写操作要求 `application/json`(含 charset 变体),否则 400 INVALID_REQUEST | `internal/httpapi/handler.go` | httpapi/review_fixes_test.go |

403 分类(GRANT_MISSING / GRANT_EXPIRED / SPACE_FORBIDDEN)按 review 建议的 truth table 实现:
grant 不存在或不覆盖该 operation → GRANT_MISSING;存在但过期 → GRANT_EXPIRED;principal 对该
Space 无任何授权且 Space 非其私有 → SPACE_FORBIDDEN。

## 轻量磁盘持久化(新增)

- `internal/store/memory/snapshot.go`:全量 JSON 快照(stage/session 复合结构体键转切片)、
  `Snapshot`/`Restore`、`PersistToFile`(临时文件 → fsync → rename 原子写)、`LoadFromFile`
  (缺文件 = 首次启动,损坏 = fail closed)。
- `cmd/server/main.go`:`-state <path>` flag;启动时加载并恢复绑定(token→tenant→bootstrap
  principal);`persistAfterMutation` 中间件在每个非 GET 请求后持久化。
- 验证:`internal/store/memory/snapshot_test.go` round-trip(含幂等重放、duplicate 语义);
  真实进程 `kill -9` 重启冒烟(recall 返回重启前 commit 的证据,tenant 重放 200 duplicate);
  跨仓库 E2E 后重启免 bootstrap recall 成功(见 Host 回执)。

## spec 疑问与遗留债务

- SPACE_FORBIDDEN 与 GRANT_MISSING 的边界按"review 建议 truth table"实现;如后续 spec 裁决
  变化,只需调整 authz 分类函数,测试已覆盖两侧。
- 快照为整库全量写,单实例规模下 O(state);增量/分段持久化列为 tracer 后债务。

## multica Gen1 大迁移运行时集成 review(2026-09-09 第二轮)

独立语义审查对 retrieval/navigation/projectionbuilder/snapshot/main 集成接缝给出
0 High / 2 Medium / 4 Low。逐项处置:

| 级别 | 发现 | 处置 |
|---|---|---|
| Medium | navigation `required` 模式下 token 上限两条路径(已达上限/将超上限)无条件走 `serverStop`,以真实 submit 终结会话并返回 found=false,与 model-call/deadline 路径的 `requiredUnavailableStep` 不一致,违反"required 请求期不得静默降级" | 已修:`internal/navigation/service.go` 两条 token_limit 路径补 `ModeRequired` 分支,返回 `required_unavailable` 步骤(不触碰会话终态) |
| Medium | retrieval `Engine.indexes`(按完整 pin 元组含版本/摘要键控)与 `Engine.embeddings` 永不淘汰,长期运行下每个陈旧 pin 固定整库副本,内存无界增长 | 已修:`internal/retrieval/engine.go` 增加 FIFO 上限(`maxCachedIndexes=16`、`maxCachedEmbeddings=8192`),淘汰仅触发从 store 确定性重建 |
| Low | 降级召回(optional embedding 失败、BM25-only)仍调用 `RecordSuccessfulRecall`,违背 store 契约"完成且未降级才记录",并影响 builder 触发节奏 | 已修:`completeRecall` 增加 `degraded` 参数,降级时不记录查询活动 |
| Low | legacy 快照 `published_order` 条目租主不可解析时被静默丢弃,后续 commit 会按 `len(publishedOrder)+1` 重派已用过的 `MemoryVersion`,腐蚀 builder 窗口 | 已修:`internal/store/memory/snapshot.go` Restore 对不可解析条目 fail-closed 返回错误(启动即失败) |
| Low | `EmbeddingNeighbors` optional 失败返回空集,explore 步骤响应无 degradation 字段,降级不可见 | 遗留:需扩展 `ExplorationStepResponse` 协议(OpenAPI+handler)才能显式暴露;记为已知边界,不作为本轮协议变更 |
| Low | navigation 每步多次 checkpoint,`-state` 下每次全库快照持全局锁,吞吐风险 | 遗留:correctness 无损(persistMu 串行化),列为持久化性能债务 |

修复后验证:`gofmt -l` 干净、`go build ./...`、`go vet ./...`、`go test -count=1 ./...` 全绿;
exploration/store/httpapi/consolidation/recall `-race` 通过;离线冒烟(provider 全关)确认
recall/builder 发布/持久化行为无回归。
