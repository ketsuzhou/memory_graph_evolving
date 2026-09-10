# First tracer bullet implementation plan

## 1. Scope, authority, and frozen rulings

This plan is the self-contained implementation authority for the first tracer bullet of `pi-group-chat-host`. Later implementation and test tasks MUST use this plan rather than reconstructing decisions from chat.

The Host is the greenfield authority for Rooms, membership, canonical Room messages, routing, durable Deliveries, Pi sessions, visible Room-tool output, and the canonical Interaction DAG. It talks to `graph-memory-service` only through Memory Protocol v1 HTTP/JSON; it never imports Graph Memory implementation code, shares its database, or reads legacy Multica data.

Frozen slice rulings:

- Graph Memory authentication placeholder: send `Authorization: Bearer <GRAPH_MEMORY_AUTH_TOKEN>`; never place the token in prompts, transcripts, DAG events, evidence, or logs.
- Deployment assumption: one Host instance. Final worker concurrency, Host UI/API, provider credential transport, repository hosting, filesystem retention, Postgres sizing, and deployment hardening remain open.
- Ordinary Agents are operator-trusted and run Pi's complete coding tools directly on the Host OS. Working directories and environment allowlists are operational controls, not a sandbox or containment claim.
- One persistent Memory Agent exists per Room. It receives only Room tools and memory `start`, `explore`, `redirect`, `submit`; coding/shell/process/generic filesystem/arbitrary network tools are absent.
- Production Postgres adapters/migrations are outside this slice. Tests use in-memory adapters, fake Pi child processes, and `httptest` Memory Protocol servers.
- Only `agent_settled` normally settles a Delivery and closes its Segment. `agent_end` is recorded but never treated as settlement.

## 2. Tracer sequence and completion order

Implement in this dependency order:

1. Implement domain invariants, in-memory stores, and deterministic IDs/idempotency.
2. Implement the conformant Memory Protocol client from section 4 and test it against `httptest.Server`.
3. Create one Tenant binding, Room, ordinary Agent, unique Memory Agent, shared Space, private Space, and exact Grants through the client.
4. Persist a human mention message and unique `(agent_id,message_id)` pending Delivery transactionally.
5. Claim one Delivery, Recall exactly shared+owner-private, compose degraded-or-cited context, version-check Pi, and send the prompt.
6. Persist prompt acknowledgement as Delivery acceptance and Segment opening. Route explicit `room_reply` once despite retries. Record `agent_end` without closing; on `agent_settled`, atomically settle and close.
7. Persist one shared and one private evidence outbox entry from the same Segment; drain each through stage then commit and recover after restart.
8. On the second human question, prove Recall cites first-turn evidence.
9. Mention the unique Memory Agent; use Room-shared-only memory start/explore/submit and publish one cited Room reply.

A step is complete only when its mapped tests pass. No retry may create a second canonical visible message, Delivery, Segment, or outbox projection.

## 3. Package structure and dependency direction

```text
internal/domain/         Room/message/Delivery/Segment/link/outbox/profile value types and invariants.
internal/ports/          Host persistence, clock, ID, Pi, and Graph Memory client seams in domain types.
internal/room/           Canonical message append, bounded mention routing, Room-tool publication.
internal/delivery/       Durable claim/accept/settle/fail/abort state machine and recovery.
internal/dag/            Host-authoritative Segment/event/link opening and closure.
internal/evidence/       Shared/private projection and durable stage/commit outbox worker.
internal/memoryclient/   Memory Protocol v1 HTTP/JSON conformant client adapter.
internal/pi/             Exact 0.85.1 version gate, process lifecycle, strict JSONL RPC, sessions.
internal/tools/          Server-authorized Room and Memory tool schemas/dispatch.
internal/runtime/        Single-flight vertical orchestration across delivery/recall/Pi/DAG/outbox.
internal/store/memory/   Test/development adapters for ports.
```

Dependency direction: `domain` imports only standard library; `ports` depends on `domain`; `room`, `delivery`, and `dag` depend on `domain+ports`; `memoryclient` implements a `ports` seam; `pi` implements a `ports` seam; `tools` depends on `room` and the Graph Memory seam; `evidence` depends on `dag+ports`; `runtime` composes interfaces and use cases; `store/memory` depends on `domain+ports`. Use cases never import concrete stores. No package imports `multica` or the sibling repository.

## 4. Memory Protocol v1 HTTP contract

**This section is field-for-field identical to `graph-memory-service/docs/PLAN.md` section 4 (“Memory Protocol v1 HTTP contract”).** If either copy changes, both copies and `graph-memory-service/openapi/memory-protocol.yaml` must change in the same protocol revision. This HTTP/JSON contract is the only integration point between repositories.

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

### 4.9 Exploration

`POST /v1/explorations`

Request: `request_id` string required; `idempotency_key` string required and scoped to authenticated Tenant+Principal; `space_ids` array<string> required non-empty/no duplicates (Memory Agent tracer sends exactly the Room Shared Space); `query` string required 1–16,000 bytes; `max_steps` integer required 1–20; `max_results` integer required 1–100.

Response (`201`/`200`): `session_id` string required; `request_id` string required; `state` constant `active` required; `pinned_spaces` array<object> required/non-empty, each with required `space_id` string and `memory_version` positive integer; `items` array of Recall item objects from section 4.8 required/may be empty; `remaining_steps` integer required; `duplicate` boolean required.

`POST /v1/explorations/{session_id}:explore`

Request: `operation_id` string required (idempotency key within session); `anchor_citation_id` string required; `relation` string enum `mentions`,`responds_to`,`continues`,`delegates_to`,`related` required; `limit` integer required 1–100.

Response (`200`): `session_id` string, `operation_id` string, `state` constant `active`, `items` array of Recall item objects, `remaining_steps` integer, `duplicate` boolean; all required.

`POST /v1/explorations/{session_id}:redirect`

Request: `operation_id` string required (idempotency key within session); `query` string required 1–16,000 bytes; `anchor_citation_ids` array<string> required and may be empty; `reason` string required 1–1,000 bytes.

Response (`200`): `session_id` string, `operation_id` string, `state` constant `active`, `items` array of Recall item objects, `remaining_steps` integer, `duplicate` boolean; all required.

`POST /v1/explorations/{session_id}:submit`

Request: `operation_id` string required (idempotency key within session); `found` boolean required; `summary` string required (empty only when `found=false`); `citation_ids` array<string> required (non-empty when `found=true`, empty allowed otherwise). Every citation must have been served by this session.

Response (`200`): `session_id` string, `operation_id` string, `state` constant `submitted`, `found` boolean, `summary` string, `citations` array of objects each containing required `citation_id`, `source_space_id`, `memory_version`, and `evidence_batch_id`, plus `duplicate` boolean; all required.

Every operation is authorized against the session's server-pinned Principal, exact Space set, versions, purpose, and expiry. Start validates all Spaces before creating a session. An operation key replay with the same body returns the stored response; changed content is `409`. Budget exhaustion is `429` and does not widen scope. Submitted sessions are terminal. For this tracer, Memory Agent Exploration containing any private Space is rejected `403`, even if untrusted text asks for it.

## 5. Core Host domain signatures and state machines

```go
type TenantID string
type RoomID string
type AgentID string
type MessageID string
type DeliveryID string
type SegmentID string
type SpaceID string

type Room struct {
    ID RoomID
    TenantID TenantID
    SharedSpaceID SpaceID
    NextSequence int64
    MemoryAgentID AgentID // exactly one, persistent
}

type RoomMessage struct {
    ID MessageID
    RoomID RoomID
    Sequence int64 // positive, strictly Room-local
    AuthorID string
    Content string
    ReplyToMessageID *MessageID
    CreatedAt time.Time
    IdempotencyKey string
}

type DeliveryState string
const (DeliveryPending DeliveryState = "pending"; DeliveryAccepted DeliveryState = "accepted"; DeliverySettled DeliveryState = "settled"; DeliveryFailed DeliveryState = "failed"; DeliveryAborted DeliveryState = "aborted")
type Delivery struct {
    ID DeliveryID
    RoomID RoomID
    AgentID AgentID
    MessageID MessageID
    State DeliveryState
    Attempts int
    AcceptedAt, SettledAt, TerminalAt *time.Time
    FailureCode string
}
// Unique constraint: (AgentID, MessageID). Legal transitions:
// pending->accepted->settled; pending|accepted->failed|aborted. No other transition.

type SegmentState string
const (SegmentOpen SegmentState = "open"; SegmentSettled SegmentState = "settled"; SegmentFailed SegmentState = "failed"; SegmentAborted SegmentState = "aborted")
type InteractionSegment struct {
    ID SegmentID
    TenantID TenantID
    RoomID RoomID
    AgentID AgentID
    DeliveryID DeliveryID
    State SegmentState
    OpenedAt time.Time
    ClosedAt *time.Time
    CloseReason string
}
// Open exactly when the matching prompt response success=true is durably accepted.
// Close normally only on agent_settled; agent_end is only an event.
// Process terminal error closes failed; cancellation/shutdown closes aborted.

type SegmentLinkKind string
const (LinkMentions SegmentLinkKind = "mentions"; LinkRespondsTo SegmentLinkKind = "responds_to"; LinkContinues SegmentLinkKind = "continues"; LinkDelegatesTo SegmentLinkKind = "delegates_to")
type SegmentLink struct { ID string; RoomID RoomID; FromSegmentID, ToSegmentID SegmentID; Kind SegmentLinkKind; CreatedAt time.Time }
// Mention/reply/continuation are explicit Host facts; delegates_to only follows a successful structured room_delegate operation (not included as a tracer tool).

type EvidenceProjection string
const (ProjectionRoomShared EvidenceProjection = "room_shared"; ProjectionAgentPrivate EvidenceProjection = "agent_private")
type EvidenceOutboxEntry struct {
    ID string // Memory Protocol idempotency_key
    SourceSegmentID SegmentID
    Projection EvidenceProjection
    SpaceID SpaceID
    BatchID string
    State string // pending|staged|committed
    Attempts int
    NextAttemptAt time.Time
    LastError string
}
// Every closed source Segment creates exactly two entries in one Host transaction:
// shared and private, with different IDs/Spaces but the same SourceSegmentID.

type AgentProfileKind string
const (ProfileOrdinary AgentProfileKind = "ordinary"; ProfileMemory AgentProfileKind = "memory")
type AgentProfile struct {
    Kind AgentProfileKind
    BuiltinToolsEnabled bool
    AllowedToolNames []string
    WorkingDirectory string
    EnvironmentAllowlist []string
}
// Ordinary: all documented Pi coding tools + Room tools, direct Host execution.
// Memory: no built-ins; exactly room_send, room_reply, room_react,
// memory_start, memory_explore, memory_redirect, memory_submit.
```

Message append and mention routing occur in one store transaction. A Room-tool idempotency key maps to exactly one canonical message. Pi final assistant text never creates a Room message.

## 6. Exact Pi 0.85.1 JSONL RPC contract

### 6.1 Version negotiation and process launch

Before any RPC process starts, execute the configured Pi binary with documented `--version`. Trim surrounding ASCII whitespace, normalize only an optional leading `pi ` label, and require semantic version text exactly `0.85.1`; any other value (including installed `0.84.2`, prerelease/build suffix, malformed/empty output, timeout, or non-zero exit) is a hard startup/Agent-unavailable error. Do not send a prompt and do not accept a Delivery. Cache the successful result by executable path plus file identity for the process lifetime; re-check after executable identity changes. This preflight is the version negotiation because Pi RPC has no protocol-version command.

Use LF-delimited JSON only; strip one trailing CR on input. One complete JSON object per line; reject invalid JSON, oversized frames, unknown response IDs, or terminal events before acknowledgement. IDs are opaque Host-generated strings.

Documented launch flags only:

- Ordinary: `--mode rpc --session-dir <server-owned-dir> --provider <server-value> --model <server-value> --no-extensions --extension <trusted-host-extension> --no-approve`. Built-in coding tools stay enabled. The model cannot change cwd, provider, model, extension path, or environment.
- Memory: same RPC/session/provider/model flags plus `--no-builtin-tools --no-extensions --extension <trusted-host-extension> --tools room_send,room_reply,room_react,memory_start,memory_explore,memory_redirect,memory_submit --no-skills --no-prompt-templates --no-context-files --no-approve`. Startup calls `get_commands`/tool registry inspection and fails if any coding/shell/process/filesystem/arbitrary-network tool is discoverable.

No Multica-private or undocumented capture/output flags are permitted. Environment is built from an empty base plus the server allowlist and short-lived provider/bridge credentials; values are never evidence.

### 6.2 Prompt acceptance and settlement

Host request (all fields required):

```json
{"id":"<opaque-request-id>","type":"prompt","message":"<composed Room input and cited Context Snapshot>"}
```

Pi acceptance response: `id` string required and equal to request; `type` constant `response`; `command` constant `prompt`; `success` boolean required; `error` string required only when `success=false` and absent when true. `success=true` is the durable Delivery acceptance/Segment-open boundary. Rejection leaves Delivery retryable pending and opens no Segment.

Relevant events:

- `{"type":"agent_start"}`: execution started; not acceptance or settlement.
- `agent_end`: fields `type` constant `agent_end`, `messages` array required, `willRetry` boolean required. It ends one low-level run and may precede retry/compaction/continuation. Record it inside the open Segment; never settle or close from it, regardless of `willRetry`.
- `{"type":"agent_settled"}`: no additional fields accepted for the frozen fixture. It means no automatic retry, compaction retry, or queued continuation remains. Persist Delivery `accepted->settled` and Segment `open->settled` atomically before evidence outbox creation.
- Pi `error`, process exit, malformed protocol, or terminal failed assistant state closes `failed`; Host cancellation/shutdown closes `aborted`. Neither is normal settlement.

The Host waits for `agent_settled`, not `agent_end`, to release the Room-Agent single-flight turn.

### 6.3 Tool event envelope and Room tool calls

Invocation arrives as:

```json
{"type":"tool_execution_start","toolCallId":"<opaque>","toolName":"<name>","args":{}}
```

Completion arrives as:

```json
{"type":"tool_execution_end","toolCallId":"<same>","toolName":"<name>","result":{"content":[{"type":"text","text":"<JSON tool result>"}],"details":{}},"isError":false}
```

`toolCallId`, `toolName`, and exact decoded args are recorded as events. A completion with mismatched ID/name or malformed result fails the turn. The trusted extension returns canonical JSON text:

- success: `{"ok":true,"result":{...}}`
- failure: `{"ok":false,"error":{"code":"<stable>","message":"<safe>","retryable":false}}`, with `isError:true`.

Room tool args (unknown fields rejected; identity/Room/Agent/workdir/env/Spaces are never model arguments):

| Tool | Required args | Successful `result` |
|---|---|---|
| `room_send` | `client_operation_id` string, `content` string | `message_id` string, `sequence` integer |
| `room_reply` | `client_operation_id` string, `in_reply_to_message_id` string, `content` string | `message_id` string, `sequence` integer |
| `room_react` | `client_operation_id` string, `message_id` string, `emoji` string | `reaction_id` string, `message_id` string |

`client_operation_id` is unique per `(Room,Agent,tool)` and is the idempotency key. Identical replay returns the original result; changed args return `TOOL_IDEMPOTENCY_CONFLICT`. Only these tools create visible speech/reaction. `room_reply` creates `responds_to`; mention routing creates `mentions`; natural language never creates `delegates_to`.

### 6.4 Memory Agent tool calls

The model never supplies Tenant, Principal, Grant, or Space IDs. Host injects the authenticated service context and exactly the current Room Shared Space. Calls map to Memory Protocol section 4.9:

| Tool | Required args | Host mapping / successful result |
|---|---|---|
| `memory_start` | `client_operation_id` string, `query` string, `max_steps` integer, `max_results` integer | Host derives request/idempotency IDs and shared `space_ids`; returns `session_id`, `items`, `remaining_steps`. |
| `memory_explore` | `client_operation_id` string, `session_id` string, `anchor_citation_id` string, `relation` enum from protocol, `limit` integer | Maps operation ID; returns `items`, `remaining_steps`. |
| `memory_redirect` | `client_operation_id` string, `session_id` string, `query` string, `anchor_citation_ids` array<string>, `reason` string | Maps operation ID; returns `items`, `remaining_steps`. |
| `memory_submit` | `client_operation_id` string, `session_id` string, `found` boolean, `summary` string, `citation_ids` array<string> | Returns terminal `found`, `summary`, and source-preserving citations. |

Session ownership and Room mapping are server-side. Unknown/cross-Room session IDs, any attempt to pass a Space-like authority field, private-space access in this tracer, citations not served by the session, and changed idempotent replays fail closed. A cited Memory Agent Room reply must use citations returned by `memory_submit`.

### 6.5 Pi command/protocol errors

Pi command failure shape is `{"id":"<echo>","type":"response","command":"<echoed-command>","success":false,"error":"<message>"}`. Treat parse errors, unknown response IDs, wrong command, duplicate conflicting responses, out-of-order settlement, unsupported events required for correctness, child stderr-only failure, and EOF before settlement as protocol/turn failure. Safe error codes persisted by Host are `PI_VERSION_MISMATCH`, `PI_PROMPT_REJECTED`, `PI_PROTOCOL_ERROR`, `PI_PROCESS_EXITED`, `PI_CANCELLED`; raw provider secrets and arbitrary stderr are not persisted in evidence.

## 7. Fake Pi test subprocess format

Tests create an executable POSIX `sh` file in `t.TempDir()` using only `os.WriteFile(..., 0o700)` and run it with `os/exec`. Required behavior:

```sh
#!/bin/sh
if [ "${1:-}" = "--version" ]; then
  printf '%s\n' "${FAKE_PI_VERSION:-0.85.1}"
  exit "${FAKE_PI_VERSION_EXIT:-0}"
fi
# Tests may assert the remaining documented argv by writing it to FAKE_PI_ARGV_LOG.
# In RPC mode, consume one request JSON object per LF-delimited line.
while IFS= read -r line; do
  # Match only the opaque request id/type needed by the fixture.
  # Emit each pre-recorded JSON object with: printf '%s\n' '<single-line JSON>'
done
```

Each scenario script embeds literal one-line JSONL frames in exact output order. A normal prompt fixture emits: prompt response success, `agent_start`, optional Room/Memory `tool_execution_start` and matching `tool_execution_end`, `agent_end` with `willRetry`, then `agent_settled`. The negative settlement fixture stops after `agent_end`; retry fixture emits `agent_end {willRetry:true}` followed by more events and one final `agent_settled`; version fixtures set `FAKE_PI_VERSION` to `0.84.2`, malformed, and `0.85.1`. Scripts flush naturally per `printf`, write protocol only to stdout, optional diagnostics to stderr, and never invoke real Pi/network/provider. Fixture lines are data, not shell-evaluated untrusted input.

## 8. GraphMemoryClient interface

The Host owns this seam; `internal/memoryclient` is the conformant HTTP adapter. Request/response types mirror section 4 exactly and do not import sibling code.

```go
type GraphMemoryClient interface {
    InitializeTenant(context.Context, InitializeTenantRequest) (InitializeTenantResponse, error)
    RegisterPrincipal(context.Context, RegisterPrincipalRequest) (RegisterPrincipalResponse, error)
    RegisterSpace(context.Context, RegisterSpaceRequest) (RegisterSpaceResponse, error)
    RegisterGrant(context.Context, RegisterGrantRequest) (RegisterGrantResponse, error)
    StageEvidenceBatch(context.Context, StageEvidenceBatchRequest) (StageEvidenceBatchResponse, error)
    CommitEvidenceBatch(context.Context, string, CommitEvidenceBatchRequest) (CommitEvidenceBatchResponse, error)
    EvidenceBatch(context.Context, string) (EvidenceBatchStatusResponse, error)
    Recall(context.Context, RecallRequest) (RecallResponse, error)
    StartExploration(context.Context, StartExplorationRequest) (StartExplorationResponse, error)
    Explore(context.Context, string, ExploreRequest) (ExploreResponse, error)
    Redirect(context.Context, string, RedirectRequest) (RedirectResponse, error)
    Submit(context.Context, string, SubmitRequest) (SubmitResponse, error)
}
```

The adapter accepts base URL, static token, `*http.Client`, and maximum response size by constructor. It sets JSON headers, never retries non-idempotent requests without their frozen key, preserves typed protocol errors, honors context deadlines, and synthesizes Host-side Recall degradation `unavailable` on transport/outage without adding Spaces. All Memory Agent operations remain Room-shared-only.

## 9. Host storage port signatures

```go
type Clock interface { Now() time.Time }
type IDSource interface { NewID(kind string) string }

type RoomStore interface {
    CreateRoom(context.Context, domain.Room, domain.AgentID, domain.AgentID) (created bool, err error)
    Room(context.Context, domain.RoomID) (domain.Room, error)
    AppendMessageAndDeliveries(context.Context, domain.RoomID, string, domain.RoomMessage, []domain.Delivery) (domain.RoomMessage, []domain.Delivery, bool, error)
    PublishToolMessage(context.Context, domain.RoomID, domain.AgentID, string, domain.RoomMessage) (domain.RoomMessage, bool, error)
}

type DeliveryStore interface {
    ClaimNext(context.Context, domain.RoomID, domain.AgentID, time.Time) (domain.Delivery, error)
    AcceptAndOpenSegment(context.Context, domain.DeliveryID, domain.InteractionSegment, time.Time) error
    SettleAndCloseSegment(context.Context, domain.DeliveryID, domain.SegmentID, time.Time, []domain.EvidenceOutboxEntry) error
    FailAndCloseSegment(context.Context, domain.DeliveryID, domain.SegmentID, string, time.Time) error
    AbortAndCloseSegment(context.Context, domain.DeliveryID, domain.SegmentID, string, time.Time) error
    RecoverPending(context.Context, time.Time) ([]domain.Delivery, error)
}

type DAGStore interface {
    AppendEvent(context.Context, domain.SegmentID, SegmentEvent) error
    PutLink(context.Context, domain.SegmentLink) (created bool, err error)
    Segment(context.Context, domain.SegmentID) (domain.InteractionSegment, error)
}

type OutboxStore interface {
    ClaimPending(context.Context, time.Time) (domain.EvidenceOutboxEntry, error)
    MarkStaged(context.Context, string) error
    MarkCommitted(context.Context, string) error
    Reschedule(context.Context, string, time.Time, string) error
    RecoverPending(context.Context, time.Time) ([]domain.EvidenceOutboxEntry, error)
}

type PiProcess interface {
    Prompt(context.Context, string, func(PiEvent) error) (PromptAcceptance, error)
    Close() error
}
type PiLauncher interface {
    VerifyExactVersion(context.Context, string) error
    Start(context.Context, domain.AgentProfile) (PiProcess, error)
}
```

The in-memory adapter serializes transactions and persists state across reconstructed runtime instances in tests. Production Postgres is deferred, but ports preserve the transaction boundaries required for message+Delivery, acceptance+Segment open, settlement+Segment close+two outbox rows, and idempotent Room-tool publication.

## 10. Required behavioral invariants

- A canonical message has one Room-local sequence. Duplicate human publication returns the original message; `(agent_id,message_id)` creates at most one Delivery.
- One `(Tenant,Room,Agent)` Pi session is single-flight. Restart reclaims pending work without inventing acceptance or settlement.
- Pre-turn Recall Space IDs come only from persisted Room/Agent configuration: exactly Room Shared then receiving ordinary Agent Private. Failure creates an explicit degraded snapshot and still permits the Pi prompt.
- Prompt acknowledgement, `agent_end`, and `agent_settled` are separate persisted facts. Only acknowledgement opens; only settled closes normally.
- Visible Agent speech is only successful Room tools. Final assistant text remains internal private evidence.
- Closing one Segment creates exactly two durable outbox entries in the same transaction. They share source Segment, not visibility.
- Outbox is at-least-once: stage and commit retries use stable IDs/keys; rows remain pending until Graph Memory accepts/commits; no fallback Space exists.
- Untrusted content can influence prompt/tool arguments but cannot set Agent identity, profile, working directory, env allowlist, credentials, Grant, Tenant, or Space mapping.
- Memory Agent starts with the restricted profile and can explore only persisted Room Shared Space. It cannot discover coding tools.

## 11. Test strategy

Use only Go standard library: `testing`, `net/http`, `net/http/httptest`, `encoding/json`, `os`, `os/exec`, `context`, `time`, and peers. No third-party assertions, routers, mocks, process helpers, or database drivers. Do not use real Postgres or a real provider. Fake Pi is the executable script in section 7. The locally installed Pi 0.84.2 may be used only by a version-gate test to prove rejection; no prompt is sent to it. Host integration tests use `httptest.Server` as the fake Memory Protocol server and assert every method/path/body/header.

Failure tests cover crash/restart after each durable boundary, duplicate/out-of-order JSONL events, HTTP timeout/status/malformed body, exactly expired Grants, and hostile Room text containing fake identity/workdir/env/Space/tool instructions.

## 12. Acceptance-criteria test map (all 10)

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

## 13. Done criteria for this tracer

The slice is done when all ten included architecture scenario steps execute end-to-end in tests, every row in section 12 passes, the second turn cites first-turn evidence, Memory Agent produces a cited shared-only reply, both repositories pass `go build ./...` and `go vet ./...`, and dependency inspection shows no Multica/sibling implementation import or shared database configuration.
