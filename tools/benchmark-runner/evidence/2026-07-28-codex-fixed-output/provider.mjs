import { appendFileSync, existsSync } from "node:fs";
import { createServer } from "node:http";
import { createHash } from "node:crypto";

const logPath = process.argv[2];
if (!logPath) throw new Error("missing log path");
if (existsSync(logPath)) throw new Error("log path already exists");

const model = "gpt-5.4";
const text = "YH-CODEX-ADAPTER-OK";
let requestCount = 0;

function summarizeInput(input) {
  return input?.map((item) => ({
    type: item.type,
    role: item.role,
    content: item.content?.map((content) => ({
      type: content.type,
      text: content.text,
    })),
  }));
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function writeEvent(response, value) {
  response.write(`event: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`);
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
    const authorized =
      request.headers.authorization === "Bearer yh-codex-loopback-fixture";
    const rawBody = Buffer.concat(chunks).toString("utf8");
    const body = rawBody ? JSON.parse(rawBody) : null;
    appendFileSync(
      logPath,
      `${JSON.stringify({
        ordinal,
        method: request.method,
        path: request.url,
        authorization: authorized ? "bearer-present" : "invalid",
        body: {
          model: body?.model,
          stream: body?.stream,
          store: body?.store,
          instructions: {
            sha256: sha256(body?.instructions ?? ""),
            has_skills: body?.instructions?.includes("<skills_instructions>"),
            has_apps: body?.instructions?.includes("<apps_instructions>"),
          },
          input: summarizeInput(body?.input),
          reasoning: body?.reasoning,
          tool_choice: body?.tool_choice,
          tool_names:
            body?.tools?.map((tool) => tool.name ?? `type:${tool.type}`) ?? [],
        },
      })}\n`,
    );

    if (
      ordinal !== 1 ||
      !authorized ||
      request.method !== "POST" ||
      request.url !== "/v1/responses" ||
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
    writeEvent(response, {
      type: "response.created",
      response: { id: "resp_yh_codex_1" },
    });
    writeEvent(response, {
      type: "response.output_item.done",
      item: {
        type: "message",
        role: "assistant",
        id: "msg_yh_codex_1",
        content: [{ type: "output_text", text }],
      },
    });
    writeEvent(response, {
      type: "response.completed",
      response: {
        id: "resp_yh_codex_1",
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
  });
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (typeof address === "object" && address) {
    process.stdout.write(`${address.port}\n`);
  }
});
