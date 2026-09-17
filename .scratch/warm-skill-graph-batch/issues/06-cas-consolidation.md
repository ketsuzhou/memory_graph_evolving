# 06 — TB-04 — 一次 CAS consolidation 完整覆盖 family proposals

**What to build:** Consolidation 用完整 proposal IDs 提交 retain/revise/specialize/merge/retire/insufficient-evidence decisions；expected ledger revision CAS 原子生效，每条 raw proposal 恰好被一个 decision 覆盖。

**Blocked by:** 04 — TB-02 — 一条 frozen trajectory 端到端产出 canonical Raw Skill Proposal

**Status:** resolved

- [x] Exact duplicate、guard specialization、compatible merge、conditional conflict 和 insufficient-evidence fixtures得到预期 decision。
- [x] Prefix ID、unknown source、漏 source、重复覆盖全部失败且 ledger 不变。
- [x] 两个并发 writer 使用同 expected revision 时恰好一个成功。
- [x] Successor 保存所有 source proposal IDs 和 evidence lineage，不只保存首个 source。
- [x] Model/transport 失败不得退化成无 provenance Markdown ledger。

## Comments
Implemented in `graph-memory-service/internal/skillevolution/batchconsolidation/` only. Consolidation classifies a family of admitted Raw Skill Proposals (or accepts explicit drafts) and CAS-commits contract §2.2 `ConsolidationDecision` records against `expected_ledger_revision` on the Skill Evolution Ledger head. Sources are complete proposal IDs; hash prefixes are rejected. Every family proposal is covered by exactly one decision. Successors keep the full source-ID set and evidence lineage. Model/transport failures fail closed and never write a Markdown ledger.

Acceptance coverage:
- `TestFixtureFamiliesProduceExpectedDecisions`
- `TestPrefixUnknownMissingAndDuplicateCoverageFailClosed`
- `TestConcurrentWritersSameExpectedRevisionExactlyOneSucceeds`
- `TestSuccessorPreservesAllSourceIDsAndEvidenceLineage`
- `TestModelTransportFailureDoesNotDegradeToMarkdownLedger`

Required commands from `graph-memory-service` (GOCACHE=/tmp/wsgb-tb04-go-cache, `$HOME/go/bin/go`):
- `$HOME/go/bin/go test ./internal/skillevolution/batchconsolidation/ ./internal/skillevolution/rawproposal/ ./internal/skillevolution/ledger/ -count=1` — pass

Did not change `cmd/server/main.go`, `httpapi/**`, `projector/**`, `proposal/**`, `rawproposal/**`, `evaluationgraph/**`, or `pi-group-chat-host/**`. Reused `rawproposal` and `ledger` APIs without changing their semantics.
