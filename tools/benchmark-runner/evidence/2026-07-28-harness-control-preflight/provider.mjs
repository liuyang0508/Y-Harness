import { appendFileSync, existsSync } from "node:fs";
import { createServer } from "node:http";

const logPath = process.argv[2];
if (!logPath) throw new Error("missing log path");
if (existsSync(logPath)) throw new Error("log path already exists");

const model = "claude-haiku-4-5-20251001";
const systemPrompt = "Return only the exact text requested by the user.";
const userPrompt = "Return exactly YH-HARNESS-CONTROL-OK";
const outputText = "YH-HARNESS-CONTROL-OK";
let requestCount = 0;

function textBlocks(value) {
  if (typeof value === "string") return [value];
  if (!Array.isArray(value)) return [];
  return value
    .filter((block) => block?.type === "text" && typeof block.text === "string")
    .map((block) => block.text);
}

function responseInputItems(input) {
  if (!Array.isArray(input)) return [];
  return input.map((item) => {
    const texts = Array.isArray(item.content)
      ? item.content
          .filter((content) => typeof content?.text === "string")
          .map((content) => content.text)
      : [];
    return {
      type: item.type,
      role: item.role,
      content_types: Array.isArray(item.content)
        ? item.content.map((content) => content.type)
        : [],
      text_count: texts.length,
      has_requested_system: texts.includes(systemPrompt),
      has_requested_prompt: texts.includes(userPrompt),
    };
  });
}

function messageItems(messages) {
  if (!Array.isArray(messages)) return [];
  return messages.map((message) => {
    const texts = textBlocks(message.content);
    return {
      role: message.role,
      content_types: Array.isArray(message.content)
        ? message.content.map((block) => block.type)
        : [typeof message.content],
      text_count: texts.length,
      has_requested_prompt: texts.includes(userPrompt),
    };
  });
}

function writeAnthropicEvent(response, event, data) {
  response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function writeResponsesEvent(response, value) {
  response.write(`event: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`);
}

function writeLog(value) {
  appendFileSync(logPath, `${JSON.stringify(value)}\n`);
}

const server = createServer((request, response) => {
  const chunks = [];
  let bytes = 0;
  request.on("data", (chunk) => {
    bytes += chunk.length;
    if (bytes > 2_097_152) request.destroy();
    else chunks.push(chunk);
  });
  request.on("end", () => {
    const ordinal = ++requestCount;
    const rawBody = Buffer.concat(chunks).toString("utf8");
    const body = rawBody ? JSON.parse(rawBody) : null;

    if (request.method === "HEAD" && request.url === "/") {
      writeLog({
        ordinal,
        protocol: "claude_probe",
        method: request.method,
        path: request.url,
        authorization: "not_sent",
      });
      response.writeHead(ordinal === 1 ? 200 : 422).end();
      return;
    }

    if (request.method === "POST" && request.url === "/v1/messages?beta=true") {
      const system = textBlocks(body?.system);
      const messages = messageItems(body?.messages);
      const authorized =
        request.headers["x-api-key"] === "yh-harness-control-anthropic";
      writeLog({
        ordinal,
        protocol: "anthropic_messages",
        method: request.method,
        path: request.url,
        authorization: authorized ? "x-api-key-valid" : "invalid",
        body: {
          model: body?.model,
          stream: body?.stream,
          max_tokens: body?.max_tokens,
          system: {
            block_count: system.length,
            has_requested_system: system.includes(systemPrompt),
          },
          messages,
          tool_names: body?.tools?.map((tool) => tool.name) ?? [],
          thinking: body?.thinking,
        },
      });
      const valid =
        ordinal === 2 &&
        authorized &&
        body?.model === model &&
        body?.stream === true &&
        system.includes(systemPrompt) &&
        messages.some((message) => message.has_requested_prompt) &&
        body?.thinking?.type === "enabled";
      if (!valid) {
        response.writeHead(422).end();
        return;
      }

      response.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "close",
      });
      writeAnthropicEvent(response, "message_start", {
        type: "message_start",
        message: {
          id: "msg_yh_harness_control_claude",
          type: "message",
          role: "assistant",
          model,
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: {
            input_tokens: 10,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            output_tokens: 0,
          },
        },
      });
      writeAnthropicEvent(response, "content_block_start", {
        type: "content_block_start",
        index: 0,
        content_block: { type: "text", text: "" },
      });
      writeAnthropicEvent(response, "content_block_delta", {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: outputText },
      });
      writeAnthropicEvent(response, "content_block_stop", {
        type: "content_block_stop",
        index: 0,
      });
      writeAnthropicEvent(response, "message_delta", {
        type: "message_delta",
        delta: { stop_reason: "end_turn", stop_sequence: null },
        usage: { output_tokens: 5 },
      });
      writeAnthropicEvent(response, "message_stop", { type: "message_stop" });
      response.end();
      return;
    }

    if (request.method === "POST" && request.url === "/v1/responses") {
      const input = responseInputItems(body?.input);
      const authorized =
        request.headers.authorization === "Bearer yh-harness-control-openai";
      writeLog({
        ordinal,
        protocol: "openai_responses",
        method: request.method,
        path: request.url,
        authorization: authorized ? "bearer-valid" : "invalid",
        body: {
          model: body?.model,
          stream: body?.stream,
          store: body?.store,
          input,
          reasoning: body?.reasoning,
          tool_choice: body?.tool_choice,
          tool_names:
            body?.tools?.map((tool) => tool.name ?? `type:${tool.type}`) ?? [],
        },
      });
      const valid =
        ordinal === 3 &&
        authorized &&
        body?.model === model &&
        body?.stream === true &&
        input.some((item) => item.has_requested_system) &&
        input.some((item) => item.has_requested_prompt) &&
        body?.reasoning?.effort === "medium";
      if (!valid) {
        response.writeHead(422).end();
        return;
      }

      response.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "close",
      });
      writeResponsesEvent(response, {
        type: "response.created",
        response: { id: "resp_yh_harness_control_codex" },
      });
      writeResponsesEvent(response, {
        type: "response.output_item.done",
        item: {
          type: "message",
          role: "assistant",
          id: "msg_yh_harness_control_codex",
          content: [{ type: "output_text", text: outputText }],
        },
      });
      writeResponsesEvent(response, {
        type: "response.completed",
        response: {
          id: "resp_yh_harness_control_codex",
          usage: {
            input_tokens: 10,
            input_tokens_details: { cached_tokens: 0 },
            output_tokens: 5,
            output_tokens_details: { reasoning_tokens: 0 },
            total_tokens: 15,
          },
        },
      });
      response.end();
      return;
    }

    writeLog({
      ordinal,
      protocol: "unknown",
      method: request.method,
      path: request.url,
      authorization: "not_inspected",
    });
    response.writeHead(404).end();
  });
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (typeof address === "object" && address) {
    process.stdout.write(`${address.port}\n`);
  }
});
