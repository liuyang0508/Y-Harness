#!/usr/bin/env python3
"""Deterministic, side-effect-free stdio MCP fixture for Tool Trace evidence."""

import json
import sys


def reply(request_id, result):
    sys.stdout.write(
        json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "result": result},
            separators=(",", ":"),
        )
        + "\n"
    )
    sys.stdout.flush()


for raw_line in sys.stdin:
    try:
        message = json.loads(raw_line)
    except (TypeError, ValueError):
        continue
    method = message.get("method")
    request_id = message.get("id")
    if method == "initialize":
        reply(
            request_id,
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "y-harness-tool-trace", "version": "1"},
                "instructions": "Diagnostic fixture exposing one uppercase Tool.",
            },
        )
    elif method == "tools/list":
        reply(
            request_id,
            {
                "tools": [
                    {
                        "name": "uppercase",
                        "description": "Convert one text value to uppercase.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    }
                ]
            },
        )
    elif method == "tools/call":
        params = message.get("params") or {}
        arguments = params.get("arguments") or {}
        if params.get("name") != "uppercase" or not isinstance(arguments.get("text"), str):
            sys.stdout.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32602, "message": "Invalid Tool arguments"},
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )
            sys.stdout.flush()
            continue
        output = {"text": arguments["text"].upper()}
        reply(
            request_id,
            {
                "content": [{"type": "text", "text": json.dumps(output)}],
                "structuredContent": output,
                "isError": False,
            },
        )
    elif request_id is not None:
        reply(request_id, {})
