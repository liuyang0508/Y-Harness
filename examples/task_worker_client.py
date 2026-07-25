#!/usr/bin/env python3
"""Dependency-free Protocol v10 Task worker lifecycle example."""

import json
import os
import subprocess
import sys
import uuid


class Client:
    def __init__(self, process):
        self.process = process
        self.sequence = 0

    def call(self, method, **fields):
        self.sequence += 1
        request = {
            "id": f"worker-{self.sequence}",
            "protocol_version": "10",
            "command": {"method": method, **fields},
        }
        self.process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("Y-Harness service stopped before responding")
        response = json.loads(line)
        body = response["body"]
        if body["status"] != "success":
            raise RuntimeError(f"{body['error']['code']}: {body['error']['message']}")
        return body["result"]


def task(task_id, dependencies):
    return {
        "id": task_id,
        "description": f"execute {task_id}",
        "dependencies": dependencies,
        "priority": 0,
        "workspace": "none",
    }


def main():
    config = sys.argv[1] if len(sys.argv) > 1 else "y-harness.json"
    binary = os.environ.get("YH_BIN", "yh")
    process = subprocess.Popen(
        [binary, "serve", config],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    client = Client(process)
    graph_id = f"worker-example-{uuid.uuid4().hex}"
    try:
        initialized = client.call("initialize")
        if "task.worker.claim" not in initialized["capabilities"]:
            raise RuntimeError("service did not advertise Task worker capabilities")

        client.call(
            "create_task_graph",
            graph_id=graph_id,
            definitions=[task("collect", []), task("synthesize", ["collect"])],
        )
        root = client.call(
            "claim_tasks",
            graph_id=graph_id,
            lease_duration_ms=60_000,
            maximum=1,
        )["claims"][0]
        client.call(
            "send_task_message",
            graph_id=graph_id,
            task_id="collect",
            lease_id=root["lease"]["id"],
            to="synthesize",
            body="collection ready",
        )
        client.call(
            "complete_task",
            graph_id=graph_id,
            task_id="collect",
            lease_id=root["lease"]["id"],
            completion={"summary": "collection complete", "artifacts": []},
        )

        dependent = client.call(
            "claim_tasks",
            graph_id=graph_id,
            lease_duration_ms=60_000,
            maximum=1,
        )["claims"][0]
        inbox = client.call(
            "get_task_messages",
            graph_id=graph_id,
            task_id="synthesize",
            lease_id=dependent["lease"]["id"],
            after_sequence=0,
            limit=32,
        )["page"]
        if [message["body"] for message in inbox["messages"]] != ["collection ready"]:
            raise RuntimeError("Task mailbox result did not match the expected message")
        client.call(
            "complete_task",
            graph_id=graph_id,
            task_id="synthesize",
            lease_id=dependent["lease"]["id"],
            completion={"summary": "synthesis complete", "artifacts": []},
        )
        graph = client.call("get_task_graph", graph_id=graph_id)["graph"]
        if not graph["terminal"]:
            raise RuntimeError("Task Graph did not reach terminal state")
        print(
            f"graph: {graph_id} revision: {graph['revision']} "
            f"tasks: {graph['task_count']} terminal: true"
        )
    finally:
        process.stdin.close()
        status = process.wait(timeout=30)
        if status != 0 and sys.exc_info()[0] is None:
            raise RuntimeError(f"Y-Harness service exited with status {status}")


if __name__ == "__main__":
    main()
