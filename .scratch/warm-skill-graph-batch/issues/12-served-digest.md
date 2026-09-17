# 12 — TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**What to build:** Resumed task 在普通工具前必须解析 manifest-pinned exact Skill Reference；只有 exact body bytes 成功进入目标 Pi session 才记录 Host-authored served fact。

**Blocked by:** 09 — TB-05 — 统一 EvaluationFreezeManifest 在首个 test 前 fail closed; 10 — TB-08 — Structured offer 原子投递并中断/续跑 exact task session

**Status:** resolved

- [x] Latest/local path、cross-manifest revision、权限或 digest mismatch 均失败。
- [x] Resolution failure 不创建 served/rejected，并结构化通知 Memory Agent。
- [x] 成功时 Pi captured bytes digest = GMS view digest = Host served digest。
- [x] `skill_get` 前普通工具被 fence；成功后必须进入 feedback fence。
- [x] 同 tool call 重放不重复创建 served record。

## Comments
模块：`pi-group-chat-host/internal/skillfence/`（`Coordinator.BindOffer` 在 resume 后进入 `skill_get` fence；只有 exact body bytes 进入目标 Pi session 才记录 Host-authored served fact。Resolution failure 不创建 served/rejected，并向 Memory 发送结构化 `resolution_error`。成功后进入 `skill_feedback` fence。同 `CallID` 重放不重复 served）。测试通过 testdata adapter `internal/skillfence/testdata/gmsview` 调用真实 `evaluationfreeze` + `evaluationgraph` digest。未改 `internal/runtime/**`、`internal/directedoffer/**`、`internal/pi/**`、`cmd/bench-runner/**`、`evaluationfreeze/**`、`evaluationgraph/**`、`cmd/server`、`httpapi`。Host 产包不 import GMS internal。

验收覆盖：
- `TestLatestLocalPathCrossManifestPermissionAndDigestMismatchFail`
- `TestResolutionFailureDoesNotCreateServedOrRejectedAndNotifiesMemory`
- `TestSuccessPiCapturedDigestEqualsGMSViewDigestEqualsHostServedDigest`
- `TestOrdinaryToolsFencedBeforeSkillGetAndFeedbackFenceAfterSuccess`
- `TestSameToolCallReplayDoesNotDuplicateServedRecord`

验证：`GOCACHE=/tmp/wsgb-tb10-go-cache $HOME/go/bin/go test ./internal/skillfence/ ./internal/directedoffer/ -count=1`
