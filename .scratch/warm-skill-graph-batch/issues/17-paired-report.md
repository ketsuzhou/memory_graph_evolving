# 17 — TB-15 — 从 preregistered manifest 生成 full-denominator paired report

**What to build:** Reporter 以 preregistered test manifest 为分母和 grader outcome 为通过权威，输出 warm/cold pass@1、paired wins/losses、机制 funnel、reason、served digest、adaptation、verdict、terminal taxonomy及已知实验限制。

**Blocked by:** 16 — TB-14 — 新策略贯通 train→diagnosis→consolidation→freeze→held-out

**Status:** resolved

- [x] Missing、timeout、no-output、infra failure 均保留分母并计 0。
- [x] Duplicate/missing task、manifest/model/seed/grading/salvage policy mismatch 拒绝生成。
- [x] 任意 recall citation 不能计 served；只有 exact body digest delivery 可计。
- [x] Paired wins+losses+ties 等于完整 paired manifest。
- [x] Golden JSON 排序稳定并明确披露 protocol token/time 未匹配。

## Comments
模块：`pi-group-chat-host/cmd/bench-runner/`（新增 `strategy_graph_batch_report.go` + `_test.go`。`generateGraphBatchPairedReport` 以 preregistered held-out manifest 为唯一 pass@1 分母，grader outcome 为通过权威；warm/cold 必须共享 manifest/model/seed/grading/salvage policy。Recall citation 永不计 served，只有 `exact_body_digest` + 完整 sha256 body digest 可计。已知限制固定披露 protocol token/time unmatched。未改 TB-14 `strategy_graph_batch_pipeline.go` 行为，未改 legacy `arm_b.go` / C3 / warm-skill-batch 报告语义或 golden）。

验收覆盖：
- `TestGraphBatchPairedReportKeepsMissingTimeoutNoOutputInfraFailureInDenominatorAsZero`
- `TestGraphBatchPairedReportRefusesDuplicateMissingTaskAndPolicyMismatch`
- `TestGraphBatchPairedReportCountsOnlyExactBodyDigestDeliveryAsServed`
- `TestGraphBatchPairedReportWinsLossesTiesEqualCompletePairedManifest`
- `TestGraphBatchPairedReportGoldenJSONIsStableAndDisclosesUnmatchedProtocolCost`
- `TestGraphBatchPairedReportUsesGraderOutcomeNotMechanismAsPassAuthority`

验证：`cd pi-group-chat-host && GOCACHE=/tmp/wsgb-tb15-go-cache $HOME/go/bin/go test ./cmd/bench-runner/ -count=1`

Legacy `warm-skill-batch` reporter / C3 / golden 测试仍绿。
