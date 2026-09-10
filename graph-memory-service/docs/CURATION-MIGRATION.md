# Curation migration delivery receipt

This documents the delivery of the four-module curation migration planned in
`docs/PLAN-curation.md` (authored by kiro `gpt-5.6-sol` from the migration
handoff). Division of labor: kiro wrote the plan and the frozen red tests plus
minimal signature stubs; this delivery implemented everything to green without
editing any frozen test.

## What landed

| Area | Deliverable |
|---|---|
| M1 causal ledger | `internal/causal` — canonical payload hashes, append-only chains, witness state machine, applicability gates, curation-purpose fence |
| M2 dive judge | `internal/dive` — five-class terminal classification, first-served citation items, pure reward formulas, `DeterministicScorer` (server-facts-only), server-side auto-judgment wired into `:submit` |
| M3 consolidation | `internal/consolidation` — eight closed operations on a shadow copy, frozen thresholds (50 batches / 200 queries / 0.02 recall tolerance), retrieval-replay gate, head CAS publication, round idempotency |
| M4 pattern + skill proposal | `internal/pattern` (tentative→supported→proposal_eligible, terminal rejected, exact-revision causal verification) and `internal/skillproposal` (fingerprint canonicalization, atomic rounds, paired replay, decision/activation split) |
| Ports | `internal/ports/curation.go` — seven additive interfaces; existing ports untouched |
| Store | `internal/store/memory/curation.go` — all seven port implementations (chain continuity, revision gaps, fingerprint freeze, decision immutability, activation version CAS) + 36 durable snapshot fields; tracer-era snapshots restore into initialized empty maps |
| HTTP | Seven additive routes: `GET /v1/explorations/{session_id}/dive`, `GET /v1/patterns/{pattern_id}`, `POST /v1/patterns/{pattern_id}:reject`, `GET /v1/proposals/{proposal_id}`, `GET /v1/candidates/{candidate_id}`, `POST /v1/candidates/{candidate_id}:decide`, `POST /v1/candidates/{candidate_id}:activate` |
| Protocol registry | Grant registration now accepts `purpose: curation` and the ten new curation operations; `openapi/memory-protocol.yaml` extended in lockstep (route conformance test stays green) |
| Server | `cmd/server` wires dive judge, pattern/proposal/candidate stores, and the curation service |

## Done criteria (PLAN §11)

1. **Frozen signatures/packages exist without cycles** — yes; `internal/store/memory` satisfies the seven curation ports directly, except `CandidateStore`, which is satisfied by the `memory.CandidateStore` adapter (see rulings below).
2. **Existing twelve routes and pre-migration tests unchanged and green** — yes; route conformance test passes with the extended OpenAPI; no existing test file was edited.
3. **All M1–M4 frozen and implementation tests green; no frozen test edited** — yes; frozen suites: `causal/service_test.go`, `dive/judge_test.go`, `consolidation/service_test.go`, `pattern/pattern_test.go`, `skillproposal/service_test.go`, `store/memory/curation_snapshot_test.go`. Implementation-added suites: `store/memory/curation_store_test.go`, `httpapi/curation_contract_test.go`, `dive/scorer.go` exercised through the HTTP judgment path.
4. **Curation is additive; fencing stays bidirectional** — yes; the contract tests prove a `tool_plane`-only principal cannot read patterns (`403 GRANT_MISSING`) and the bound service principal cannot activate (`403`), while exact-Space `curation` grants pass.

## Implementation rulings worth remembering

- **The store is the only chain validator.** Services compute payload hashes; the memory store rejects an append whose `PreviousHash` does not extend the stored head (`409 CAUSAL_CHAIN_CONFLICT`) and revision numbers that skip (`409 CAUSAL_REVISION_CONFLICT`). No read port exposes the head, so the store is the only place continuity can be enforced.
- **`PutReplayPlan`/`PutReplayResult` collide across ports.** `RetrievalReplayStore` and `CandidateStore` declare same-named methods with different signatures; one Go type cannot implement both. The store names the paired-replay pair `PutCandidateReplayPlan`/`PutCandidateReplayResult` and `memory.CandidateStore` restores the port shape.
- **`Service.Decide`/`Service.Activate` return `created`, not `duplicate`.** The frozen skillproposal tests lock this (fake stores return `created=false`, tests assert the service echoes it). The HTTP layer computes `duplicate = !created` for its `201/200` discipline.
- **Dive trajectories are frozen before judging.** On `:submit`, the handler derives the trajectory from server-owned session state, fills in the submit facts, records it via `RecordDiveTrajectory` (content-idempotent), then judges. A judge failure never fails the submit; `GET .../dive` reports `404 DIVE_NOT_FOUND` until a judgment exists, `409 EXPLORATION_NOT_TERMINAL` for active sessions.
- **Genesis projection head is version 0 with an empty digest.** The first consolidation round bases on `Projection(0)`, the empty projection, and CASes against `ProjectionHead{Version: 0, Digest: ""}`.
- **First candidate seeds the activation baseline.** `PutCandidate` seeds `active_skill_versions[target_skill]` with the candidate's `BaseArtifactVersion` when absent; `ActivateCandidate` then CASes on `expected_base_version`.
- **Snapshots never render curation state as null.** All 36 fields are initialized in `New()` and re-initialized on `Restore`, so a tracer-era snapshot file (pre-curation) boots into usable empty maps.

## Verification

- `go test ./...` green in `graph-memory-service` and `pi-group-chat-host`.
- `go vet ./...` clean in both repositories.
- Cross-repo E2E (`pi-group-chat-host/internal/e2e`) run three consecutive times: green.
