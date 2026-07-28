import { appendFileSync, existsSync } from "node:fs";
import { createServer } from "node:http";

const logPath = process.argv[2];
if (!logPath) throw new Error("missing log path");
if (existsSync(logPath)) throw new Error("log path already exists");

const model = "grok-4.5";
let responseIndex = 0;

function writeEvent(response, value) {
  response.write(`data: ${JSON.stringify(value)}\n\n`);
}

function complete(response) {
  const index = responseIndex++;
  const id = `resp_yh_grok_${index}`;
  const text = "YH-GROK-BUILD-ADAPTER-OK";
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "close",
  });
  writeEvent(response, {
    type: "response.created",
    sequence_number: 0,
    response: {
      id,
      object: "response",
      created_at: 1785200000,
      model,
      status: "in_progress",
      output: [],
    },
  });
  writeEvent(response, {
    type: "response.output_text.delta",
    sequence_number: 1,
    item_id: `item_yh_grok_${index}`,
    output_index: 0,
    content_index: 0,
    delta: text,
  });
  writeEvent(response, {
    type: "response.completed",
    sequence_number: 2,
    response: {
      id,
      object: "response",
      created_at: 1785200000,
      model,
      status: "completed",
      output: [{
        type: "message",
        id: `msg_yh_grok_${index}`,
        role: "assistant",
        status: "completed",
        content: [{ type: "output_text", text, annotations: [] }],
      }],
      usage: {
        input_tokens: 10,
        output_tokens: 5,
        total_tokens: 15,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
    },
  });
  response.end("data: [DONE]\n\n");
}

function summarize(body) {
  if (!body) return null;
  return {
    model: body.model,
    stream: body.stream,
    store: body.store,
    input: body.input?.map(({ role, content }) => ({ role, content })),
    reasoning: body.reasoning,
    max_output_tokens: body.max_output_tokens ?? null,
    tool_choice: body.tool_choice ?? null,
    tool_names: body.tools?.map(({ name }) => name) ?? [],
  };
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
    const authorized =
      request.headers.authorization === "Bearer yh-grok-loopback-fixture";
    const rawBody = Buffer.concat(chunks).toString("utf8");
    const body = rawBody ? JSON.parse(rawBody) : null;
    appendFileSync(
      logPath,
      `${JSON.stringify({
        method: request.method,
        path: request.url,
        authorization: authorized ? "bearer-present" : "invalid",
        body: summarize(body),
      })}\n`,
    );

    if (!authorized) {
      response.writeHead(401).end();
    } else if (request.method === "GET" && request.url === "/v1/models") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        object: "list",
        data: [{
          id: model,
          object: "model",
          created: 1785200000,
          owned_by: "yh-loopback",
          apiBackend: "responses",
          supportsReasoningEffort: true,
          reasoningEffort: "low",
          reasoningEfforts: [{ value: "low", label: "Low", default: true }],
        }],
      }));
    } else if (
      request.method === "POST" &&
      request.url === "/v1/responses" &&
      responseIndex < 2
    ) {
      complete(response);
    } else {
      response.writeHead(400).end();
    }
  });
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (typeof address === "object" && address) {
    process.stdout.write(`${address.port}\n`);
  }
});
