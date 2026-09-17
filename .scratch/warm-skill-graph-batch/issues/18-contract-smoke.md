# 18 — TB-16 — Deterministic smoke 证明 system-contract 十二项不变量

**What to build:** 小规模 fixture graph 和 scripted/real Pi 运行完整 reject→redirect→accept、effects-unknown retry、canonical diagnosis/consolidation、frozen test isolation，并生成自描述 artifact bundle。

**Blocked by:** 11 — TB-09 — Mutating tool 安全中断与 effects-unknown retry; 16 — TB-14 — 新策略贯通 train→diagnosis→consolidation→freeze→held-out; 17 — TB-15 — 从 preregistered manifest 生成 full-denominator paired report

**Status:** ready-for-agent

- [ ] 机器逐项断言 `system-contract.md §10` 的十二项 smoke 条件。
- [ ] Real Pi 0.85.1 smoke 证明 exact-session abort/resume；binary不可用时不得把 contract 标成 green。
- [ ] Bundle记录命令、版本、config、manifest、body digests和每项 assertion evidence。
- [ ] Host/GMS targeted tests、repository-wide tests及 race suites通过。
- [ ] Known limitations 固化：generation-0无train feedback、opening-only、无protocol-cost control、crash uncertainty fail closed。

## Comments
