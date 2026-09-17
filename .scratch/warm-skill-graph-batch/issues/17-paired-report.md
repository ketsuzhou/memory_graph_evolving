# 17 — TB-15 — 从 preregistered manifest 生成 full-denominator paired report

**What to build:** Reporter 以 preregistered test manifest 为分母和 grader outcome 为通过权威，输出 warm/cold pass@1、paired wins/losses、机制 funnel、reason、served digest、adaptation、verdict、terminal taxonomy及已知实验限制。

**Blocked by:** 16 — TB-14 — 新策略贯通 train→diagnosis→consolidation→freeze→held-out

**Status:** ready-for-agent

- [ ] Missing、timeout、no-output、infra failure 均保留分母并计 0。
- [ ] Duplicate/missing task、manifest/model/seed/grading/salvage policy mismatch 拒绝生成。
- [ ] 任意 recall citation 不能计 served；只有 exact body digest delivery 可计。
- [ ] Paired wins+losses+ties 等于完整 paired manifest。
- [ ] Golden JSON 排序稳定并明确披露 protocol token/time 未匹配。

## Comments
