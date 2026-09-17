# 09 — TB-05 — 统一 EvaluationFreezeManifest 在首个 test 前 fail closed

**What to build:** 一个 immutable manifest 同时绑定 evidence cut、Skill ledger revision、proposal/provenance set、Evaluation Graph digest/watermark，以及 prompt/schema/model/tool/config/grading policy；所有 test 只接收一个 digest。

**Blocked by:** 01 — PF-01 — 增加 non-active Evaluation Skill Graph read view; 06 — TB-04 — 一次 CAS consolidation 完整覆盖 family proposals

**Status:** ready-for-agent

- [ ] 相同输入幂等产生同 manifest ID/digest。
- [ ] 任一 moved head、missing proposal、provenance hole 或 graph digest mismatch 在 test 前失败。
- [ ] Test Room、overlay 或 held-out feedback 混入 scope 时失败。
- [ ] N 个 test attempt 记录完全相同 manifest digest。
- [ ] Partial graph freeze 不得 warning 后继续运行。

## Comments
