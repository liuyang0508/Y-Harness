import { createHash } from "node:crypto";
import { appendFileSync, existsSync } from "node:fs";
import { createServer } from "node:http";

const logPath = process.argv[2];
if (!logPath) throw new Error("missing log path");
if (existsSync(logPath)) throw new Error("log path already exists");

const model = "claude-haiku-4-5-20251001";
const text = "YH-CLAUDE-ADAPTER-OK";
const requestedSystem = "Return only the exact text requested by the user.";
let requestCount = 0;
let modelRequestCount = 0;

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function textBlocks(value) {
  if (typeof value === "string") return [value];
  if (!Array.isArray(value)) return [];
  return value
    .filter((block) => block?.type === "text" && typeof block.text === "string")
    .map((block) => block.text);
}

function writeEvent(response, event, data) {
  response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
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
    const system = textBlocks(body?.system);
    const messages =
      body?.messages?.map((message) => ({
        role: message.role,
        content_types: Array.isArray(message.content)
          ? message.content.map((block) => block.type)
          : [typeof message.content],
        text_count: textBlocks(message.content).length,
        text_sha256: sha256(textBlocks(message.content).join("\n")),
        last_text: textBlocks(message.content).at(-1),
      })) ?? [];
    appendFileSync(
      logPath,
      `${JSON.stringify({
        ordinal,
        method: request.method,
        path: request.url,
        authorization: request.headers["x-api-key"]
          ? "x-api-key-present"
          : "missing",
        anthropic_version: request.headers["anthropic-version"],
        body: {
          model: body?.model,
          stream: body?.stream,
          max_tokens: body?.max_tokens,
          system: {
            block_count: system.length,
            has_requested_system: system.includes(requestedSystem),
          },
          messages,
          tool_names: body?.tools?.map((tool) => tool.name) ?? [],
          thinking: body?.thinking,
          output_config: body?.output_config,
        },
      })}\n`,
    );

    if (request.method === "HEAD" && request.url === "/") {
      response.writeHead(200).end();
      return;
    }
    const modelOrdinal = ++modelRequestCount;
    if (
      modelOrdinal !== 1 ||
      !request.headers["x-api-key"] ||
      request.method !== "POST" ||
      request.url !== "/v1/messages?beta=true" ||
      body?.model !== model ||
      body?.stream !== true
    ) {
      response.writeHead(422).end();
      return;
    }

    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "close",
    });
    writeEvent(response, "message_start", {
      type: "message_start",
      message: {
        id: "msg_yh_claude_1",
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
    writeEvent(response, "content_block_start", {
      type: "content_block_start",
      index: 0,
      content_block: { type: "text", text: "" },
    });
    writeEvent(response, "content_block_delta", {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text },
    });
    writeEvent(response, "content_block_stop", {
      type: "content_block_stop",
      index: 0,
    });
    writeEvent(response, "message_delta", {
      type: "message_delta",
      delta: { stop_reason: "end_turn", stop_sequence: null },
      usage: { output_tokens: 5 },
    });
    writeEvent(response, "message_stop", { type: "message_stop" });
    response.end();
  });
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (typeof address === "object" && address) {
    process.stdout.write(`${address.port}\n`);
  }
});
