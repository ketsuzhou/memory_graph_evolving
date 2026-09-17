# 09 — TB-05 — 统一 EvaluationFreezeManifest 在首个 test 前 fail closed

**What to build:** 一个 immutable manifest 同时绑定 evidence cut、Skill ledger revision、proposal/provenance set、Evaluation Graph digest/watermark，以及 prompt/schema/model/tool/config/grading policy；所有 test 只接收一个 digest。

**Blocked by:** 01 — PF-01 — 增加 non-active Evaluation Skill Graph read view; 06 — TB-04 — 一次 CAS consolidation 完整覆盖 family proposals

**Status:** resolved

- [x] 相同输入幂等产生同 manifest ID/digest。
- [x] 任一 moved head、missing proposal、provenance hole 或 graph digest mismatch 在 test 前失败。
- [x] Test Room、overlay 或 held-out feedback 混入 scope 时失败。
- [x] N 个 test attempt 记录完全相同 manifest digest。
- [x] Partial graph freeze 不得 warning 后继续运行。

## Comments
Implemented in `graph-memory-service/internal/skillevolution/evaluationfreeze/` only. `Service.Freeze` seals one content-addressed `EvaluationFreezeManifest` that binds the evidence cut, Skill Evolution Ledger head, complete proposal/provenance set, rebuilt Evaluation Graph digest/watermark, and prompt/schema/model/tool/config/grading-policy revisions. Test attempts receive only that digest via `TestBinding` / `RecordTestAttempt`. Moved head, missing proposal, provenance hole, graph digest mismatch, Test Room/overlay/held-out scope contamination, and partial graph freeze fail closed before any test can start; there is no warning-then-continue path.

Acceptance coverage:
- `TestSameInputIdempotentlyProducesSameManifestIDAndDigest`
- `TestMovedHeadMissingProposalProvenanceHoleAndGraphDigestMismatchFailClosedBeforeTest`
- `TestTestRoomOverlayOrHeldOutFeedbackInScopeFailsClosed`
- `TestNTestAttemptsRecordIdenticalManifestDigest`
- `TestPartialGraphFreezeDoesNotContinueAfterWarning`

Required commands from `graph-memory-service` (GOCACHE=/tmp/wsgb-tb05-go-cache, `$HOME/go/bin/go`):
- `$HOME/go/bin/go test ./internal/skillevolution/evaluationfreeze/ ./internal/skillevolution/evaluationgraph/ ./internal/skillevolution/batchconsolidation/ -count=1` — pass

Did not change `cmd/server/main.go`, `httpapi/**`, `projector/**`, `evaluationgraph/**`, `batchconsolidation/**`, `rawproposal/**`, or `pi-group-chat-host/**`. Reused `evaluationgraph`, `batchconsolidation`, `rawproposal`, and `ledger` APIs without changing their semantics.
