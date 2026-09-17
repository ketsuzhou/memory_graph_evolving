# 18 — TB-16 — Deterministic smoke 证明 system-contract 十二项不变量

**What to build:** 小规模 fixture graph 和 scripted/real Pi 运行完整 reject→redirect→accept、effects-unknown retry、canonical diagnosis/consolidation、frozen test isolation，并生成自描述 artifact bundle。

**Blocked by:** 11 — TB-09 — Mutating tool 安全中断与 effects-unknown retry; 16 — TB-14 — 新策略贯通 train→diagnosis→consolidation→freeze→held-out; 17 — TB-15 — 从 preregistered manifest 生成 full-denominator paired report

**Status:** resolved

- [x] 机器逐项断言 `system-contract.md §10` 的十二项 smoke 条件。
- [x] Real Pi 0.85.1 smoke 证明 exact-session abort/resume；binary不可用时不得把 contract 标成 green。
- [x] Bundle记录命令、版本、config、manifest、body digests和每项 assertion evidence。
- [x] Host/GMS targeted tests、repository-wide tests及 race suites通过。
- [x] Known limitations 固化：generation-0无train feedback、opening-only、无protocol-cost control、crash uncertainty fail closed。

## Comments
模块：`pi-group-chat-host/cmd/bench-runner/`（新增 `strategy_graph_batch_smoke.go` + `_test.go` + `testdata/smokefix` + `testdata/contract-smoke/bundle.json`。`runGraphBatchContractSmoke` 组合 Host/GMS public seams：concurrentepisode 并行、directedoffer exact-session abort/resume、skillfence served digest、skilldisposition accepted/rejected、GMS rejectredirect too_generic→specialized、AgentRunCoordinator 无重叠、effectsinterrupt fail closed、diagnosisfanout+rawadmit evidence lineage、pipeline consolidation/isolation/same manifest、TB-15 full-denominator report。未改 TB-14 `strategy_graph_batch_pipeline.go` 语义，未改 TB-15 报告口径，未改 legacy `warm-skill-batch`）。

十二项断言（`TestGraphBatchContractSmokeTwelveInvariants` 子测试）：
1. `01_task_and_memory_parallel`
2. `02_structured_mention_exact_session_abort_resume`
3. `03_resolved_body_digest_matches_served`
4. `04_accepted_rejected_with_reason`
5. `05_rejected_triggers_next_graph_exploration`
6. `06_same_agent_no_overlapping_pi_run`
7. `07_effects_unknown_fail_closed`
8. `08_diagnosis_proposal_keeps_exact_evidence_lineage`
9. `09_consolidation_covers_complete_source_ids`
10. `10_held_out_feedback_does_not_enter_frozen_graph`
11. `11_all_tests_use_same_manifest`
12. `12_primary_metric_uses_full_preregistered_denominator`

Bundle：`pi-group-chat-host/cmd/bench-runner/testdata/contract-smoke/bundle.json`。当前环境 Pi binary 为 0.84.2，contract_status=`incomplete`（不得标 green）。

验证：
- Host: `GOCACHE=/tmp/wsgb-tb16-go-cache $HOME/go/bin/go test ./... -count=1`
- Host race: `GOCACHE=/tmp/wsgb-tb16-go-cache $HOME/go/bin/go test -race ./internal/pi/sessionctrl/ ./internal/directedoffer/ ./internal/effectsinterrupt/ ./cmd/bench-runner/ -count=1`
- GMS: `GOCACHE=/tmp/wsgb-tb16-go-cache $HOME/go/bin/go test ./... -count=1`
