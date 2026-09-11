// Host-owned room bridge for real Pi RPC processes.
//
// Registers exactly the Room tool surface of the Memory Protocol. The Host
// executes the actual side effects when it observes a successful paired
// completion frame (tool_execution_start + tool_execution_end with the same
// id/name); these handlers only acknowledge completion so the model sees a
// finished call. Plain assistant text is not delivered to the room, so every
// guideline points the model at room_send.
//
// When HOST_TOOL_PROXY_URL is set, the same-call Tool Proxy tool surface
// (memory_explore / memory_expand / skill_get, Contract §7.17 v1) is also
// registered: execute() forwards the call to the Host proxy endpoint and
// returns its exact canonical ToolProxyResult bytes verbatim — the tool
// future stays open until the Host answers, so the result the model sees is
// the same-call terminal result, never a placeholder (Host Spec §5.4). Any
// transport failure throws: the call fails closed in Pi, it is never
// backfilled from a later GMS response.
const toolProxyURL = process.env.HOST_TOOL_PROXY_URL;
const toolProxyToken = process.env.HOST_TOOL_PROXY_TOKEN;
const toolProxyTimeoutMillis = Number(process.env.HOST_TOOL_PROXY_TIMEOUT_MS || 30000);

function toolCallIdOf(ctx) {
  return (
    ctx?.toolCallId ??
    ctx?.toolCall?.id ??
    ctx?.callId ??
    ctx?.id ??
    null
  );
}

async function executeViaToolProxy(args, ctx, toolName) {
  const toolCallID = toolCallIdOf(ctx);
  if (!toolCallID) {
    throw new Error(
      `tool proxy: no tool call id in the execution context for ${toolName}; failing closed`,
    );
  }
  let response;
  try {
    response = await fetch(toolProxyURL, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(toolProxyToken ? { authorization: `Bearer ${toolProxyToken}` } : {}),
      },
      body: JSON.stringify({ tool_call_id: toolCallID, tool_name: toolName, arguments: args ?? {} }),
      signal: AbortSignal.timeout(toolProxyTimeoutMillis),
    });
  } catch (error) {
    throw new Error(`tool proxy: return channel request failed for ${toolName}: ${error}`);
  }
  if (!response.ok) {
    throw new Error(
      `tool proxy: return channel rejected ${toolName} with HTTP ${response.status}`,
    );
  }
  // Verbatim passthrough: the Host already serialized the canonical
  // Contract §7.18 ToolProxyResult; re-encoding or rewriting it here would
  // break the exactness guarantee (Host Spec §5.5).
  const text = await response.text();
  return { content: [{ type: "text", text }] };
}

export default function (pi) {
  for (const definition of [
    {
      name: "room_send",
      label: "Room send",
      description:
        "Publish a visible message to the room. This is the only way your reply reaches the room; plain assistant text without this tool is not delivered.",
      promptGuidelines: [
        "Use room_send to deliver every reply; plain text answers are not visible to the room.",
      ],
      parameters: {
        type: "object",
        properties: {
          client_operation_id: { type: "string", description: "Idempotency key chosen by the agent for this publish." },
          content: { type: "string", description: "Message content visible to the room." },
        },
        required: ["client_operation_id", "content"],
        additionalProperties: false,
      },
    },
    {
      name: "room_reply",
      label: "Room reply",
      description:
        "Publish a visible reply to an existing room message, for example when answering a specific message.",
      promptGuidelines: [
        "Use room_reply when answering a specific room message so the thread stays connected.",
      ],
      parameters: {
        type: "object",
        properties: {
          client_operation_id: { type: "string", description: "Idempotency key chosen by the agent for this reply." },
          in_reply_to_message_id: { type: "string", description: "ID of the message being answered." },
          content: { type: "string", description: "Reply content visible to the room." },
        },
        required: ["client_operation_id", "in_reply_to_message_id", "content"],
        additionalProperties: false,
      },
    },
    {
      name: "room_react",
      label: "Room react",
      description: "Attach a reaction emoji to an existing room message.",
      parameters: {
        type: "object",
        properties: {
          client_operation_id: { type: "string", description: "Idempotency key chosen by the agent for this reaction." },
          message_id: { type: "string", description: "ID of the message to react to." },
          emoji: { type: "string", description: "Reaction emoji." },
        },
        required: ["client_operation_id", "message_id", "emoji"],
        additionalProperties: false,
      },
    },
  ]) {
    pi.registerTool({
      ...definition,
      async execute() {
        return { content: [{ type: "text", text: "acknowledged" }] };
      },
    });
  }

  // Same-call Tool Proxy surface (Host Spec §5.4): registered only when the
  // Host proxy endpoint is configured. Room tools above never take this
  // path — their side-effect semantics stay bound to the paired completion
  // frame the Host observes.
  if (toolProxyURL) {
    for (const definition of [
      {
        name: "memory_explore",
        label: "Memory explore",
        description:
          "Explore graph memory for evidence and skills within the room's scope. Resolved by the Host tool proxy in this very call.",
        parameters: {
          type: "object",
          properties: {
            query_text: { type: "string", description: "What to look for in graph memory." },
            anchor_citation_id: { type: "string", description: "Optional citation to expand from." },
          },
          additionalProperties: true,
        },
      },
      {
        name: "memory_expand",
        label: "Memory expand",
        description:
          "Expand one cited result into its related evidence and skills within the room's scope. Resolved by the Host tool proxy in this very call.",
        parameters: {
          type: "object",
          properties: {
            citation_id: { type: "string", description: "Citation to expand." },
          },
          required: ["citation_id"],
          additionalProperties: true,
        },
      },
      {
        name: "skill_get",
        label: "Skill get",
        description:
          "Fetch one closed guidance view for a skill within the room's scope. Resolved by the Host tool proxy in this very call.",
        parameters: {
          type: "object",
          properties: {
            skill_id: { type: "string", description: "Skill whose guidance view is requested." },
          },
          required: ["skill_id"],
          additionalProperties: true,
        },
      },
    ]) {
      pi.registerTool({
        ...definition,
        async execute(args, ctx) {
          return executeViaToolProxy(args, ctx, definition.name);
        },
      });
    }
  }
}
