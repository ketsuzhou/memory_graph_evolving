# 06 — TB-04 — 一次 CAS consolidation 完整覆盖 family proposals

**What to build:** Consolidation 用完整 proposal IDs 提交 retain/revise/specialize/merge/retire/insufficient-evidence decisions；expected ledger revision CAS 原子生效，每条 raw proposal 恰好被一个 decision 覆盖。

**Blocked by:** 04 — TB-02 — 一条 frozen trajectory 端到端产出 canonical Raw Skill Proposal

**Status:** ready-for-agent

- [ ] Exact duplicate、guard specialization、compatible merge、conditional conflict 和 insufficient-evidence fixtures得到预期 decision。
- [ ] Prefix ID、unknown source、漏 source、重复覆盖全部失败且 ledger 不变。
- [ ] 两个并发 writer 使用同 expected revision 时恰好一个成功。
- [ ] Successor 保存所有 source proposal IDs 和 evidence lineage，不只保存首个 source。
- [ ] Model/transport 失败不得退化成无 provenance Markdown ledger。

## Comments
