# 12 — TB-10 — Offer fence 强制 exact skill_get 并记录 served digest

**What to build:** Resumed task 在普通工具前必须解析 manifest-pinned exact Skill Reference；只有 exact body bytes 成功进入目标 Pi session 才记录 Host-authored served fact。

**Blocked by:** 09 — TB-05 — 统一 EvaluationFreezeManifest 在首个 test 前 fail closed; 10 — TB-08 — Structured offer 原子投递并中断/续跑 exact task session

**Status:** ready-for-agent

- [ ] Latest/local path、cross-manifest revision、权限或 digest mismatch 均失败。
- [ ] Resolution failure 不创建 served/rejected，并结构化通知 Memory Agent。
- [ ] 成功时 Pi captured bytes digest = GMS view digest = Host served digest。
- [ ] `skill_get` 前普通工具被 fence；成功后必须进入 feedback fence。
- [ ] 同 tool call 重放不重复创建 served record。

## Comments
