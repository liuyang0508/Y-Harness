import { createServer } from "node:http";
import { writeFileSync } from "node:fs";

const logPath = process.argv[2];
if (!logPath) throw new Error("missing log path");

const server = createServer((request, response) => {
  const chunks = [];
  let bytes = 0;
  request.on("data", (chunk) => {
    bytes += chunk.length;
    if (bytes > 2_097_152) request.destroy();
    else chunks.push(chunk);
  });
  request.on("end", () => {
    if (
      request.method !== "POST" ||
      request.url !== "/v1/chat/completions" ||
      request.headers.authorization !== "Bearer fixture-token"
    ) {
      response.writeHead(400).end();
      return;
    }
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    writeFileSync(
      logPath,
      `${JSON.stringify({
        method: request.method,
        path: request.url,
        authorization: request.headers.authorization ? "bearer-present" : "absent",
        body,
      })}\n`,
      { flag: "wx" },
    );
    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "close",
    });
    const base = {
      id: "chatcmpl-yh-opencode",
      object: "chat.completion.chunk",
      created: 1785200000,
      model: "local-deterministic",
    };
    response.write(`data: ${JSON.stringify({
      ...base,
      choices: [{
        index: 0,
        delta: { role: "assistant", content: "YH-OPENCODE-ADAPTER-OK" },
        finish_reason: null,
      }],
    })}\n\n`);
    response.write(`data: ${JSON.stringify({
      ...base,
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    })}\n\n`);
    response.end("data: [DONE]\n\n");
  });
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (typeof address === "object" && address) {
    process.stdout.write(`${address.port}\n`);
  }
});
