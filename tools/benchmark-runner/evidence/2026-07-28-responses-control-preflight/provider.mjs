import { appendFileSync, existsSync } from "node:fs";
import { createServer } from "node:http";

const logPath = process.argv[2];
if (!logPath) throw new Error("missing log path");
if (existsSync(logPath)) throw new Error("log path already exists");

const model = "gpt-5.4";
const systemPrompt = "Return only the exact text requested by the user.";
const userPrompt = "Return exactly YH-RESPONSES-CONTROL-OK";
const outputText = "YH-RESPONSES-CONTROL-OK";
let requestCount = 0;
let responseCount = 0;

function contentTexts(content) {
  if (typeof content === "string") return [content];
  if (!Array.isArray(content)) return [];
  return content
    .filter((part) => typeof part?.text === "string")
    .map((part) => part.text);
}

function summarizeInput(input) {
  if (!Array.isArray(input)) return [];
  return input.map((item) => {
    const texts = contentTexts(item.content);
    return {
      type: item.type,
      role: item.role,
      content_types: Array.isArray(item.content)
        ? item.content.map((part) => part.type)
        : [typeof item.content],
      text_count: texts.length,
      has_requested_system: texts.includes(systemPrompt),
      has_requested_prompt: texts.some((text) => text.includes(userPrompt)),
    };
  });
}

function writeEvent(response, value) {
  response.write(`event: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`);
}

function complete(response) {
  const index = ++responseCount;
  const responseId = `resp_yh_responses_control_${index}`;
  const itemId = `msg_yh_responses_control_${index}`;
  const message = {
    type: "message",
    role: "assistant",
    id: itemId,
    status: "completed",
    content: [{ type: "output_text", text: outputText, annotations: [] }],
  };
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "close",
  });
  writeEvent(response, {
    type: "response.created",
    sequence_number: 0,
    response: {
      id: responseId,
      object: "response",
      created_at: 1785200000,
      model,
      status: "in_progress",
      output: [],
    },
  });
  writeEvent(response, {
    type: "response.output_item.done",
    sequence_number: 1,
    output_index: 0,
    item: message,
  });
  writeEvent(response, {
    type: "response.completed",
    sequence_number: 2,
    response: {
      id: responseId,
      object: "response",
      created_at: 1785200000,
      model,
      status: "completed",
      output: [message],
      usage: {
        input_tokens: 10,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens: 5,
        output_tokens_details: { reasoning_tokens: 0 },
        total_tokens: 15,
      },
    },
  });
  response.end("data: [DONE]\n\n");
}

function logRequest(ordinal, product, request, authorized, responseStatus, body) {
  appendFileSync(
    logPath,
    `${JSON.stringify({
      ordinal,
      product,
      method: request.method,
      path: request.url,
      authorization: authorized ? "bearer-valid" : "invalid",
      response_status: responseStatus,
      body: body
        ? {
            model: body.model,
            stream: body.stream,
            store: body.store,
            input: summarizeInput(body.input),
            reasoning: body.reasoning,
            max_output_tokens: body.max_output_tokens ?? null,
            tool_choice: body.tool_choice ?? null,
            tool_names:
              body.tools?.map((tool) => tool.name ?? `type:${tool.type}`) ?? [],
          }
        : null,
    })}\n`,
  );
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
    const codexAuthorized =
      request.headers.authorization === "Bearer yh-responses-control-openai";
    const grokAuthorized =
      request.headers.authorization === "Bearer yh-responses-control-grok";

    if (ordinal === 1) {
      const input = summarizeInput(body?.input);
      const valid =
        codexAuthorized &&
        request.method === "POST" &&
        request.url === "/v1/responses" &&
        body?.model === model &&
        body?.stream === true &&
        body?.reasoning?.effort === "medium" &&
        input.some((item) => item.has_requested_system) &&
        input.some((item) => item.has_requested_prompt);
      logRequest(
        ordinal,
        "codex_main",
        request,
        codexAuthorized,
        valid ? 200 : 422,
        body,
      );
      if (!valid) {
        response.writeHead(422).end();
        return;
      }
      complete(response);
      return;
    }

    if (ordinal === 2) {
      const valid =
        grokAuthorized &&
        request.method === "GET" &&
        request.url === "/v1/models";
      logRequest(
        ordinal,
        "grok_catalog",
        request,
        grokAuthorized,
        valid ? 200 : 422,
        body,
      );
      if (!valid) {
        response.writeHead(422).end();
        return;
      }
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          object: "list",
          data: [
            {
              id: model,
              object: "model",
              created: 1785200000,
              owned_by: "yh-shared-responses-control",
              apiBackend: "responses",
              supportsReasoningEffort: true,
              reasoningEffort: "medium",
              reasoningEfforts: [
                { value: "medium", label: "Medium", default: true },
              ],
            },
          ],
        }),
      );
      return;
    }

    if (ordinal === 3) {
      const input = summarizeInput(body?.input);
      const valid =
        grokAuthorized &&
        request.method === "POST" &&
        request.url === "/v1/responses" &&
        body?.model === model &&
        body?.stream === true &&
        input.some((item) => item.has_requested_prompt) &&
        body?.tool_choice?.name === "session_title";
      logRequest(
        ordinal,
        "grok_title",
        request,
        grokAuthorized,
        valid ? 200 : 422,
        body,
      );
      if (!valid) {
        response.writeHead(422).end();
        return;
      }
      complete(response);
      return;
    }

    if (ordinal === 4) {
      const input = summarizeInput(body?.input);
      const valid =
        grokAuthorized &&
        request.method === "POST" &&
        request.url === "/v1/responses" &&
        body?.model === model &&
        body?.stream === true &&
        body?.reasoning?.effort === "medium" &&
        input.some((item) => item.has_requested_system) &&
        input.some((item) => item.has_requested_prompt);
      logRequest(
        ordinal,
        "grok_main",
        request,
        grokAuthorized,
        valid ? 200 : 422,
        body,
      );
      if (!valid) {
        response.writeHead(422).end();
        return;
      }
      complete(response);
      return;
    }

    logRequest(ordinal, "unexpected", request, false, 422, body);
    response.writeHead(422).end();
  });
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (typeof address === "object" && address) {
    process.stdout.write(`${address.port}\n`);
  }
});
