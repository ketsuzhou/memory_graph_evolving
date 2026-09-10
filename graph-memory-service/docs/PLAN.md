# First tracer bullet implementation plan

## 1. Scope, authority, and frozen rulings

This plan is the self-contained implementation authority for `graph-memory-service`. The original tracer contract remains the authority boundary, while the implementation-status notes below describe the retrieval, projection, navigation, and curation capabilities added after that tracer.

The service is a greenfield, framework-neutral authority for Tenant, Space, Principal, Grant, evidence admission, Recall, Exploration, and governed derived projections. It owns the OpenAPI 3.1 Memory Protocol. It does not import `multica`, read a Host database, reconstruct or mutate the Host Interaction DAG, ingest legacy Multica memory, or share a database/transaction with the Host.

### 1.1 Current implementation status

- **Implemented and on by default:** exact-Space authorization, committed evidence and citation/version fences, BM25 Recall, deterministic Exploration lifecycle, durable in-memory journals, and optional atomic JSON snapshots. BM25 remains active even when all provider-backed capabilities are off.
- **Implemented but off by default:** OpenAI-compatible embeddings (`retrieval-mode=optional|required`), bounded OpenAI-compatible navigation (`navigation-mode=optional|required`), and the polling structural projection builder (`builder-enabled=true`). Provider calls receive detached evidence/projection copies and never execute while the authoritative memory-store mutex is held.
- **Provider policy:** `optional` uses the provider when fully configured and returns explicit degradation when Recall falls back to BM25; `required` fails server startup if URL, model, or the configured API-key environment value is missing, and request-time provider failure does not silently degrade. Secrets are read only from the named environment variable and are never included in logs or cache keys.
- **Projection/builder boundary:** Exploration pins and traverses published hierarchy/relation/entity projections, with optional embedding neighbors for `related`. The automatic builder is a deterministic **structural** batch-summary/event-leaf builder with structural/BM25 replay and CAS publication. It is not an LLM semantic summarizer, LATTICE top-down partitioner, or public write API.
- **Navigation boundary:** `:navigate` is a bounded selector over server-generated actions and served citations. The model cannot create evidence, node IDs, grants, Spaces, or free-form final results. It is not LATTICE slate calibration or path-score learning.
- **Still not implemented:** production Postgres/object storage, correction/retraction semantics, HA/multi-region coordination, a semantic LLM projection builder, projection/tree candidates in Recall, and a populated evaluation/holdout/safety replay corpus.

Frozen authority and deployment rulings:

- Authentication placeholder: every request uses `Authorization: Bearer <GRAPH_MEMORY_AUTH_TOKEN>`. Comparison is constant-time. Missing/invalid credentials are `401`. The first successful tenant initialization binds this single deployment token to that Tenant and to its bootstrap `service` Principal; later lifecycle/tool-plane requests derive Tenant and authenticated Principal from that binding, never from request-supplied Tenant identity. This is a tracer-only mechanism, not the final authentication design.
- Deployment assumption: one service instance and one initialized Tenant. The in-process worker and JSON snapshot are development/single-node facilities, not production coordination or storage.
- Authority, visibility, settlement, and trust decisions remain unchanged: the Host owns Room/Delivery/Pi/Interaction DAG; Graph Memory owns admitted evidence and retrieval. Raw Host sources remain with the Host.
- Tests and implementation use only the Go standard library. Tests use in-memory adapters and HTTP test servers, not real Postgres or Pi.

## 2. Tracer sequence and completion order

Implement in this dependency order; each step is complete only when its mapped tests pass:

1. Freeze and conformance-test Memory Protocol v1 from section 4 and `openapi/memory-protocol.yaml`.
2. Implement domain invariants and in-memory Registry/Grant adapters; initialize one Tenant, Host service Principal, ordinary Agent Principal, Memory Agent Principal, one shared Space, one private Space, and exact expiring Grants.
3. Implement evidence staging and atomic commit, including semantic idempotency conflict detection and visibility only after commit.
4. Implement bounded Recall over an exact, pre-authorized Space list, retaining source Space, version, citation, and degradation.
5. Implement Room-shared-only Exploration start/explore/redirect/submit with pinned versions and budgets.
6. Run the cross-repository tracer: accept the Host's separate shared/private batches for one opaque source Segment, then return first-turn evidence in second-turn Recall and cited Exploration output.

No later step may weaken an earlier scope or authorization check.

## 3. Package structure and dependency direction

```text
internal/domain/              Authoritative value types and invariants; standard library only.
internal/ports/               Narrow seams in domain types; depends only on domain.
internal/authz/               Authentication and exact Grant/expiry decisions.
internal/evidence/, recall/   Evidence and Recall use cases.
internal/retrieval/           BM25 plus optional embedding retrieval engine; implements RecallStore.
internal/exploration/         Pinned lifecycle and projection traversal.
internal/navigation/          Bounded automatic navigation use case.
internal/consolidation/       Shadow mutation, replay gate, and CAS publication.
internal/projectionbuilder/   Deterministic structural builder, replay runner, and polling worker.
internal/embedding/           OpenAI-compatible embedding adapter.
internal/pathselector/        OpenAI-compatible closed-set selector adapter.
internal/httpapi/             Sole public HTTP transport adapter.
internal/store/memory/        Development adapters and authoritative snapshot state.
openapi/                      Authoritative public contract; no Go implementation.
```

Allowed dependency direction is `domain → ports → use case → adapter`, with composition only in `cmd/server`. Domain imports no repository package; ports own cross-layer seams; use cases never import HTTP or a concrete store. Retrieval's BM25 indexes/embedding cache are derived, process-local, and intentionally excluded from snapshots. The memory adapter copies exact pinned evidence/projections under its lock and releases the lock before tokenization or provider calls. A future transactional production adapter may implement the same ports but is not present.

## 4. Memory Protocol v1 HTTP contract

This section and `openapi/memory-protocol.yaml` are normative. The Host plan section 4 is a field-for-field copy and explicitly cross-references this section. This HTTP/JSON contract is the only integration point between repositories.

### 4.1 Common wire rules

- Base paths start with `/v1`; request and response media type is `application/json`.
- Every request requires the static bearer token. Tenant and authenticated Principal are resolved from it after initialization. Operational requests MUST NOT contain `tenant_id` or an acting Principal field.
- IDs are required non-empty opaque strings, 1–128 bytes, compared byte-for-byte. No caller infers their format.
- Timestamps are required UTC RFC3339Nano strings. A Grant is active only when `now.Before(expires_at)`; equality is expired. No grace interval or rounded comparison is allowed.
- Unknown JSON fields, duplicate JSON object keys, trailing JSON values, wrong types, missing required fields, and non-finite numbers are rejected with `400 INVALID_JSON` or `400 INVALID_REQUEST`.
- Arrays marked required must be present; they may be empty only where explicitly stated. Space arrays are deduplicated only by rejection, never silently.
- Successful creation returns `201`; an identical idempotent replay returns `200` with `duplicate: true`. A replay of the same idempotency key with different semantic content returns `409 IDEMPOTENCY_CONFLICT` and changes nothing.
- `request_id`, `idempotency_key`, `operation_id`, `commit_id`, and all resource IDs are opaque; none encode scope or authority.

### 4.2 Error envelope and status codes

Every non-2xx response has exactly this shape:

| Field | JSON type | Required | Meaning |
|---|---|---:|---|
| `error` | object | yes | Error body. |
| `error.code` | string | yes | Stable machine code. |
| `error.message` | string | yes | Safe human-readable summary; no secret or raw evidence. |
| `error.request_id` | string | yes | Echoed request ID when available, otherwise a server-generated opaque ID. |
| `error.details` | array<object> | yes | May be empty. |
| `error.details[].field` | string | yes | JSON field/path, or empty string for a resource-level error. |
| `error.details[].reason` | string | yes | Stable reason text. |

Status mapping: `400` malformed/invalid request; `401` missing or invalid bearer token; `403 SPACE_FORBIDDEN`, `GRANT_MISSING`, or `GRANT_EXPIRED`; `404 SPACE_NOT_FOUND`, `BATCH_NOT_FOUND`, or `EXPLORATION_NOT_FOUND`; `409 IDEMPOTENCY_CONFLICT` or invalid state transition; `422` valid JSON violating a domain invariant; `429 EXPLORATION_BUDGET_EXHAUSTED`; `500 INTERNAL`; `503 UNAVAILABLE`. Authorization validates the complete requested Space set before reading; no partial result is returned for a missing or forbidden Space.

### 4.3 Tenant initialization

`PUT /v1/tenant`

Request:

| Field | Type | Required | Rules |
|---|---|---:|---|
| `tenant_id` | string | yes | Resource ID and idempotency key for initialization. This administrative creation field is not accepted by operational endpoints. |
| `display_name` | string | yes | 1–200 UTF-8 bytes. |
| `bootstrap_principal_id` | string | yes | Creates the token-bound `service` Principal. |

Response (`201` or idempotent `200`):

| Field | Type | Required |
|---|---|---:|
| `tenant_id` | string | yes |
| `bootstrap_principal_id` | string | yes |
| `duplicate` | boolean | yes |

The first success atomically creates/binds all three identities. The same `tenant_id` and semantic body replay safely; a different body or second Tenant returns `409`.

### 4.4 Principal registration

`POST /v1/principals`

Request:

| Field | Type | Required | Rules |
|---|---|---:|---|
| `principal_id` | string | yes | Idempotency key. |
| `kind` | string enum `human`, `agent`, `service` | yes | Memory identity class. |
| `display_name` | string | yes | 1–200 UTF-8 bytes. |

Response (`201`/`200`): `principal_id` string required, `kind` string required, `duplicate` boolean required.

### 4.5 Space registration

`POST /v1/spaces`

Request:

| Field | Type | Required | Rules |
|---|---|---:|---|
| `space_id` | string | yes | Idempotency key. |
| `scope` | string enum `shared`, `private` | yes | Durable visibility class. |
| `owner_principal_id` | string or null | yes | Must be `null` for `shared`; must name an existing Principal for `private`. |
| `display_name` | string | yes | 1–200 UTF-8 bytes; carries no authority. |

Response (`201`/`200`): `space_id` string, `scope` string, `owner_principal_id` string-or-null, and `duplicate` boolean; all fields required.

### 4.6 Exact Grant registration

`POST /v1/grants`

Request:

| Field | Type | Required | Rules |
|---|---|---:|---|
| `grant_id` | string | yes | Idempotency key. |
| `principal_id` | string | yes | Existing exact grantee. |
| `space_ids` | array<string> | yes | Non-empty exact set; every Space must exist; no wildcard or duplicate. |
| `purpose` | string enum `lifecycle`, `tool_plane` | yes | Purpose fence. |
| `operations` | array<string enum `evidence.stage`, `evidence.commit`, `recall`, `exploration.start`, `exploration.explore`, `exploration.redirect`, `exploration.submit`> | yes | Non-empty exact set, no duplicate. |
| `expires_at` | string(date-time) | yes | Exact UTC RFC3339Nano expiry; must be strictly after server time at creation. |

Response (`201`/`200`): `grant_id` string, `principal_id` string, `space_ids` array<string>, `purpose` string, `operations` array<string>, `expires_at` string, `duplicate` boolean; all required. Registration never makes the request body's Principal the current caller. Runtime authorization uses the token-bound Principal and server-stored Grants.

### 4.7 Evidence Batch stage, commit, and status

`POST /v1/evidence-batches:stage`

Request:

| Field | Type | Required | Rules |
|---|---|---:|---|
| `batch_id` | string | yes | Resource ID. |
| `idempotency_key` | string | yes | Deduplication key scoped to authenticated Tenant + `space_id`; Host uses its outbox entry ID. |
| `space_id` | string | yes | One exact authorized target Space. |
| `stream_id` | string | yes | Append-only Space-local lineage. |
| `source_segment_id` | string | yes | Opaque Host Segment correlation; never grants access to another Space. |
| `provenance` | object | yes | Sanitized Host provenance. |
| `provenance.host_type` | string | yes | For this tracer, `pi-group-chat-host`. |
| `provenance.host_instance_id` | string | yes | Opaque Host deployment ID. |
| `provenance.source_kind` | string enum `room_shared`, `agent_private` | yes | Projection kind. |
| `provenance.captured_at` | string(date-time) | yes | Host observation time. |
| `provenance.content_sha256` | string | yes | Lowercase 64-hex SHA-256 of the Host's sanitized source projection. |
| `events` | array<object> | yes | Non-empty, ordered evidence. |
| `events[].event_id` | string | yes | Unique within batch. |
| `events[].sequence` | integer(int64) | yes | Positive and strictly increasing. |
| `events[].kind` | string enum `room_message`, `room_tool_call`, `room_tool_result`, `pi_internal`, `segment_terminal` | yes | Sanitized evidence kind. |
| `events[].content` | string | yes | May be empty only for `segment_terminal`. |
| `events[].occurred_at` | string(date-time) | yes | Source event time. |
| `links` | array<object> | yes | May be empty. |
| `links[].link_id` | string | yes | Unique within batch. |
| `links[].from_event_id` | string | yes | Event in this batch. |
| `links[].to_event_id` | string | yes | Event in this batch. |
| `links[].relation` | string enum `mentions`, `responds_to`, `continues`, `delegates_to` | yes | Explicit Host relation. |
| `terminal_outcome` | string enum `settled`, `failed`, `aborted` | yes | Host-owned Segment result, accepted as provenance rather than reconstructed. |

Response (`201`/`200`): `batch_id` string, `idempotency_key` string, `space_id` string, `state` enum `staged`,`committed`, `duplicate` boolean; all required.

Idempotency is `(authenticated_tenant, space_id, idempotency_key)`. An identical decoded request returns the original batch. Same key with any changed field is `409`. `batch_id` is globally unique within the Tenant and conflicting reuse is `409`. Staging persists no recall-visible evidence.

`POST /v1/evidence-batches/{batch_id}:commit`

Request: `commit_id` string required; it is the commit idempotency key scoped to `batch_id`.

Response (`200`): `batch_id` string, `commit_id` string, `state` constant `committed`, `memory_version` integer(int64, minimum 1), `committed_at` string(date-time), `duplicate` boolean; all required.

Commit validates the staged batch completely, then atomically records its immutable manifest, advances exactly that Space's version, and exposes all or none of its events. Repeating the same `commit_id`, or committing an already committed identical batch, returns the original version with `duplicate: true`. A different `commit_id` after commit also returns the immutable committed result (commit is resource-idempotent); a conflicting staged payload can never replace it.

`GET /v1/evidence-batches/{batch_id}`

Response (`200`): `batch_id` string, `idempotency_key` string, `space_id` string, `source_segment_id` string, `state` enum `staged`,`committed`, `memory_version` integer-or-null, `committed_at` string(date-time)-or-null; all required. `null` values mean not committed.

### 4.8 Recall

`POST /v1/recalls`

Request:

| Field | Type | Required | Rules |
|---|---|---:|---|
| `request_id` | string | yes | Correlation only, not an authority selector. |
| `query` | string | yes | 1–16,000 UTF-8 bytes. |
| `space_ids` | array<string> | yes | Non-empty exact list, no duplicate. Ordinary Host turns send exactly Room Shared then owner Private. |
| `max_results` | integer | yes | 1–100. |
| `deadline_ms` | integer | yes | 1–30,000 relative budget. |

Response (`200`):

| Field | Type | Required | Rules |
|---|---|---:|---|
| `request_id` | string | yes | Echo. |
| `items` | array<object> | yes | May be empty. |
| `items[].content` | string | yes | Sanitized recall text. |
| `items[].source_space_id` | string | yes | Original Space, never erased by composition. |
| `items[].memory_version` | integer(int64) | yes | Positive source Space version. |
| `items[].citation` | object | yes | Durable citation. |
| `items[].citation.citation_id` | string | yes | Opaque citation ID. |
| `items[].citation.evidence_batch_id` | string | yes | Committed source batch. |
| `items[].citation.event_ids` | array<string> | yes | Non-empty source events. |
| `items[].score` | number | yes | Finite 0..1 ranking value, not authority. |
| `degradation` | object | yes | Scope-preserving outcome. |
| `degradation.state` | string enum `complete`, `empty`, `partial`, `stale`, `timed_out`, `unavailable` | yes | Typed state. |
| `degradation.reasons` | array<string> | yes | May be empty only for `complete` or `empty`. |

All Spaces are existence/Grant-validated before retrieval. A missing Space is `404`; an existing unauthorized or expired-Grant Space is `403`; neither returns items. Retrieval failure after authorization returns `200` with typed degradation and only items from requested/authorized Spaces. It never widens scope, searches the Tenant, another private Space, or legacy data.

The runtime always applies BM25. With `retrieval-mode=off` (the default), BM25 is the complete applied policy. In `optional`, successful embeddings produce normalized weighted BM25+cosine scores using the configured BM25 weight; an absent/failing/invalid provider returns the already-computed BM25 items with `degradation.state=partial` and a safe BM25-only reason. In `required`, missing provider configuration fails startup and a request-time provider failure returns typed degradation without a silent lexical fallback claim. Provider mode is deployment configuration, not caller-controlled input. The response exposes only the final bounded score; it does not claim per-channel attribution.

Embedding calls operate on immutable copies returned after the memory-store lock is released. Provider secrets are read from the configured environment variable, are not serialized, and are not included in request/event logs or embedding cache keys.

### 4.9 Exploration

`POST /v1/explorations`

Request: `request_id` string required; `idempotency_key` string required and scoped to authenticated Tenant+Principal; `space_ids` array<string> required non-empty/no duplicates (Memory Agent tracer sends exactly the Room Shared Space); `query` string required 1–16,000 bytes; `max_steps` integer required 1–20; `max_results` integer required 1–100.

Response (`201`/`200`): `session_id` string required; `request_id` string required; `state` constant `active` required; `pinned_spaces` array<object> required/non-empty, each with required `space_id` string and `memory_version` non-negative integer (`0` is the empty-Space watermark); `items` array of Recall item objects from section 4.8 required/may be empty; `remaining_steps` integer required; `duplicate` boolean required.

`POST /v1/explorations/{session_id}:explore`

Request: `operation_id` string required (idempotency key within session); `anchor_citation_id` string required; `relation` string enum `mentions`,`responds_to`,`continues`,`delegates_to`,`related` required; `limit` integer required 1–100.

Response (`200`): `session_id` string, `operation_id` string, `state` constant `active`, `items` array of Recall item objects, `remaining_steps` integer, `duplicate` boolean; all required.

Explore is a real one-hop traversal over the immutable derived projection pinned when the session starts. The anchor citation must already have been served by the same session. A seed Recall citation resolves to every projection node whose `EvidenceRefs` contain the cited batch/event; a citation returned by Explore carries the exact projection node identity for the next hop. `related` expands hierarchy edges in both directions, entity-reference co-occurrence, and typed relation edges; the other relation values expand only exact matching typed relation edges. Hierarchy edges are oriented `From=parent`, `To=child`. No projection, no projected anchor, or no matching neighbor returns an empty item list and consumes the successful step; Explore never falls back to lexical re-query.

Projection-backed Recall items add optional `traversal` metadata: `projection_node_id`, `projection_version`, `projection_digest`, `route_kind` (`hierarchy_parent`, `hierarchy_child`, `entity`, `relation`, or `embedding`), `edge_id`, `edge_kind`, `relation`, and `direction`. The `embedding` route is considered only for `relation=related`, ranks otherwise-unreached nodes in the exact pinned projection, and remains subordinate to explicit hierarchy/entity/relation routes. Off or optional-provider failure simply omits this route; required-provider failure fails the step. Each item remains backed by committed evidence and carries a stable citation; nodes without evidence visible at the session's pinned Memory Version are not served. The session's result budget counts returned neighbor items, and all returned citations enter the existing submit-time served-citation fence.

`POST /v1/explorations/{session_id}:redirect`

Request: `operation_id` string required (idempotency key within session); `query` string required 1–16,000 bytes; `anchor_citation_ids` array<string> required and may be empty; `reason` string required 1–1,000 bytes.

Response (`200`): `session_id` string, `operation_id` string, `state` constant `active`, `items` array of Recall item objects, `remaining_steps` integer, `duplicate` boolean; all required.

`POST /v1/explorations/{session_id}:navigate`

Request: `run_id` string required and idempotent within the session; `max_model_calls` integer required 1–16 and clamped by the server deployment cap (currently 8).

Response (`200`): terminal `state` enum `submitted`,`stopped`; `found`, `summary`, `citations`, non-empty `steps`, aggregate `applied_policy` enum `llm`,`deterministic_fallback`,`server_clamp`, `degradation`, and `duplicate`; all required. Each step records the closed-set intent, server action/operation IDs, applied policy, bounded model policy (`openai-compatible`, model, `gms-navigation-v1`) and provider-reported usage. Optional selector absence/failure uses a deterministic server action and records degradation; required absence/failure returns `503 PATH_SELECTION_UNAVAILABLE`. Off is the default and the endpoint returns unavailable. Every selected suboperation is journaled before execution and reuses Exploration authorization, budget, served-anchor, citation, and idempotency fences. Navigation never exposes a builder or projection write operation.

`POST /v1/explorations/{session_id}:submit`

Request: `operation_id` string required (idempotency key within session); `found` boolean required; `summary` string required (empty only when `found=false`); `citation_ids` array<string> required (non-empty when `found=true`, empty allowed otherwise). Every citation must have been served by this session.

Response (`200`): `session_id` string, `operation_id` string, `state` constant `submitted`, `found` boolean, `summary` string, `citations` array of objects each containing required `citation_id`, `source_space_id`, `memory_version`, and `evidence_batch_id`, plus `duplicate` boolean; all required.

Every operation is authorized against the session's server-pinned Principal, exact Space set, versions, purpose, and expiry. Start validates all Spaces before creating a session. An operation key replay with the same body returns the stored response; changed content is `409`. Budget exhaustion is `429` and does not widen scope. Submitted sessions are terminal. For this tracer, Memory Agent Exploration containing any private Space is rejected `403`, even if untrusted text asks for it.

## 5. Core Go domain signatures

These signatures are frozen design targets; implementation may add unexported fields/helpers but must not weaken them.

```go
type TenantID string
type SpaceID string
type PrincipalID string
type GrantID string
type BatchID string
type ExplorationSessionID string

type Tenant struct {
    ID TenantID
    DisplayName string
    BootstrapPrincipalID PrincipalID
}

type SpaceScope string
const (
    SpaceShared SpaceScope = "shared"
    SpacePrivate SpaceScope = "private"
)
type Space struct {
    ID SpaceID
    TenantID TenantID
    Scope SpaceScope
    OwnerPrincipalID *PrincipalID // nil iff shared
    DisplayName string
    Version int64
}

type PrincipalKind string
const (PrincipalHuman PrincipalKind = "human"; PrincipalAgent PrincipalKind = "agent"; PrincipalService PrincipalKind = "service")
type Principal struct { ID PrincipalID; TenantID TenantID; Kind PrincipalKind; DisplayName string }

type GrantPurpose string
type GrantOperation string
type Grant struct {
    ID GrantID
    TenantID TenantID
    PrincipalID PrincipalID
    SpaceIDs []SpaceID
    Purpose GrantPurpose
    Operations []GrantOperation
    ExpiresAt time.Time
}
func (g Grant) ActiveAt(now time.Time) bool // exactly now.Before(g.ExpiresAt)

type Provenance struct { HostType, HostInstanceID, SourceKind string; CapturedAt time.Time; ContentSHA256 string }
type EvidenceEvent struct { ID string; Sequence int64; Kind, Content string; OccurredAt time.Time }
type EvidenceLink struct { ID, FromEventID, ToEventID, Relation string }
type EvidenceBatchState string
type EvidenceBatch struct {
    ID BatchID
    TenantID TenantID
    IdempotencyKey string
    SpaceID SpaceID
    StreamID string
    SourceSegmentID string // opaque correlation only
    Provenance Provenance
    Events []EvidenceEvent
    Links []EvidenceLink
    TerminalOutcome string
    State EvidenceBatchState
    MemoryVersion *int64
    CommittedAt *time.Time
}

type Citation struct { ID string; EvidenceBatchID BatchID; EventIDs []string }
type RecallRequest struct { RequestID, Query string; SpaceIDs []SpaceID; MaxResults, DeadlineMS int }
type RecallItem struct { Content string; SourceSpaceID SpaceID; MemoryVersion int64; Citation Citation; Score float64 }
type RecallDegradation struct { State string; Reasons []string }
type RecallResult struct { RequestID string; Items []RecallItem; Degradation RecallDegradation }

type ExplorationBudget struct { MaxSteps, MaxResults int }
type PinnedSpace struct { SpaceID SpaceID; MemoryVersion int64 }
type ExplorationSession struct {
    ID ExplorationSessionID
    TenantID TenantID
    PrincipalID PrincipalID
    SpaceIDs []SpaceID
    PinnedSpaces []PinnedSpace
    Query string
    Budget ExplorationBudget
    StepsUsed int
    State string // active|submitted
    ExpiresAt time.Time
}
```

Tenant/Principal are always supplied to use cases from authenticated server context, not decoded operational payload fields.

## 6. Storage port signatures

```go
type Clock interface { Now() time.Time }

type RegistryStore interface {
    InitializeTenant(context.Context, domain.Tenant, domain.Principal) (created bool, err error)
    PutPrincipal(context.Context, domain.TenantID, domain.Principal) (created bool, err error)
    PutSpace(context.Context, domain.TenantID, domain.Space) (created bool, err error)
    PutGrant(context.Context, domain.TenantID, domain.Grant) (created bool, err error)
    Space(context.Context, domain.TenantID, domain.SpaceID) (domain.Space, error)
    ActiveGrants(context.Context, domain.TenantID, domain.PrincipalID, time.Time) ([]domain.Grant, error)
}

type EvidenceStore interface {
    Stage(context.Context, domain.EvidenceBatch) (batch domain.EvidenceBatch, duplicate bool, err error)
    Commit(context.Context, domain.TenantID, domain.BatchID, string, time.Time) (batch domain.EvidenceBatch, duplicate bool, err error)
    Batch(context.Context, domain.TenantID, domain.BatchID) (domain.EvidenceBatch, error)
}

type RecallStore interface {
    Recall(context.Context, domain.TenantID, []domain.PinnedSpace, string, int) ([]domain.RecallItem, error)
}

type ExplorationStore interface {
    Start(context.Context, domain.ExplorationSession, string) (session domain.ExplorationSession, duplicate bool, err error)
    Session(context.Context, domain.TenantID, domain.ExplorationSessionID) (domain.ExplorationSession, error)
    Apply(context.Context, domain.ExplorationSessionID, string, any) (storedResponse []byte, duplicate bool, err error)
}
```

The in-memory adapter must serialize mutations under a mutex and emulate transaction atomicity. `Commit` performs validation, manifest write, version increment, and publication as one critical section; injected failure before the final swap leaves the batch staged and invisible. Production Postgres implementation is explicitly deferred.

## 7. Required behavioral invariants

- Evidence uses two phases: stage validates/persists an invisible immutable candidate; commit atomically makes the complete batch visible and advances one Space version. Partial visibility is impossible.
- Shared and private batches may carry the same opaque `source_segment_id`; they have different Space IDs and idempotency keys. Correlation never enables a cross-Space read.
- Idempotency compares decoded semantic content. Identical retries return the original resource/result; key reuse with changed content is fail-closed `409`.
- Recall validates the complete requested Space list and exact active Grants before touching recall data. Nonexistent is `404`; unauthorized or exactly expired is `403`; there is no partial or Tenant-wide fallback.
- Every result retains source Space, source version, citation, and typed degradation. Composition never strips source identity.
- Exploration pins exact Space versions at start, enforces server-side step/result budgets, and accepts submission citations only from material served in that session.
- Graph Memory treats Host Segment IDs and terminal outcomes as opaque provenance and never attempts to mutate the canonical DAG.

## 8. Test strategy

Use only `testing`, `net/http`, `net/http/httptest`, `encoding/json`, `context`, `time`, and other Go standard-library packages. No third-party assertions, routers, mocks, database drivers, or code generators. Use deterministic fake clocks and the in-memory adapters. HTTP conformance tests exercise real handlers through `httptest.Server`. No test requires Postgres, Multica, a network service, or Pi.

Contract tests must reject unknown fields, verify every required field, cover every status/error envelope, and compare routes/methods against `openapi/memory-protocol.yaml`. Cross-repository Host tests use an `httptest` fake implementing precisely section 4.

## 9. Acceptance-criteria test map (all 10)

| # | Architecture acceptance criterion | Test file and function(s) |
|---:|---|---|
| 1 | Pi pinned/contract-tested at 0.85.1; opaque IDs; no undocumented flags | Host `internal/pi/rpc_contract_test.go`: `TestRPCContract0851`, `TestRPCRejectsEveryOtherVersion`, `TestRPCUsesOnlyDocumentedFlagsAndOpaqueIDs`. |
| 2 | Duplicate message, Delivery, Room-tool, Evidence Batch attempts are idempotent | GMS `internal/evidence/service_test.go`: `TestStageAndCommitAreSemanticallyIdempotent`, `TestIdempotencyConflictFailsClosed`; Host `internal/room/idempotency_test.go`: `TestMessageDeliveryAndRoomReplyRetriesPublishOnce`. |
| 3 | Host restart recovers pending Deliveries/outbox without duplicate visible messages | Host `internal/runtime/recovery_test.go`: `TestRestartRecoversPendingDeliveryAndEvidenceOutboxExactlyOnceVisible`. |
| 4 | Prompt ack and `agent_settled` distinct; `agent_end` never settles | Host `internal/pi/settlement_test.go`: `TestPromptAckAcceptsAndOnlyAgentSettledSettles`, `TestAgentEndDoesNotCloseSegment`. |
| 5 | Recall contains only server-resolved Room Shared + owner Private IDs | GMS `internal/recall/service_test.go`: `TestRecallRejectsMissingForbiddenAndExpiredSpacesAtomically`; Host `internal/runtime/recall_scope_test.go`: `TestPreTurnRecallUsesOnlySharedAndOwnerPrivate`. |
| 6 | Recall outage degrades turn; evidence stays pending; no scope fallback | GMS `internal/recall/service_test.go`: `TestRecallFailureReturnsScopedDegradation`; Host `internal/runtime/degradation_test.go`: `TestRecallOutageContinuesTurnAndKeepsEvidencePending`. |
| 7 | Memory Agent cannot discover/invoke coding tools | Host `internal/pi/profile_test.go`: `TestMemoryAgentProfileExposesOnlyRoomAndMemoryTools`, `TestMemoryAgentRejectsCodingToolInvocation`. |
| 8 | Untrusted input cannot alter identity/workdir/env/credentials/Grants/Spaces | GMS `internal/httpapi/authority_test.go`: `TestOperationalPayloadCannotSelectTenantPrincipalOrGrant`; Host `internal/runtime/authority_test.go`: `TestRoomInputCannotOverrideServerResolvedExecutionAuthority`. |
| 9 | Graph Memory cannot query Host Room DB or mutate Interaction DAG | GMS `internal/httpapi/authority_test.go`: `TestProtocolHasNoHostDatabaseOrDAGMutationRoute`; Host `internal/dag/authority_test.go`: `TestGraphMemoryResponsesCannotMutateCanonicalDAG`. |
| 10 | Separate databases; only versioned HTTP/JSON communication | GMS `internal/httpapi/conformance_test.go`: `TestMemoryProtocolV1RoutesMatchOpenAPI`; Host `internal/memoryclient/client_test.go`: `TestClientUsesHTTPJSONAgainstIndependentServer`. |

## 10. Done criteria for the current single-node implementation

The implementation is integrated when the OpenAPI paths and strict schemas match the HTTP adapter; BM25 works with provider-backed retrieval/navigation/builder disabled; optional and required provider modes have the documented fallback/fail-fast behavior; Exploration pins and traverses only published projections and citations; the structural builder publishes only through replay/CAS and persists successful background publication when `-state` is configured; legacy snapshots restore fail-closed with every authoritative map initialized; derived retrieval indexes remain outside snapshots; and the built-in Go toolchain passes `gofmt`, `go test ./...`, and `go vet ./...`. This does not declare production storage, semantic LLM tree building, populated quality replay corpora, correction/retraction, or HA complete.
