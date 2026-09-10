// Host-owned room bridge for real Pi RPC processes.
//
// Registers exactly the Room tool surface of the Memory Protocol. The Host
// executes the actual side effects when it observes a successful paired
// completion frame (tool_execution_start + tool_execution_end with the same
// id/name); these handlers only acknowledge completion so the model sees a
// finished call. Plain assistant text is not delivered to the room, so every
// guideline points the model at room_send.
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
}
