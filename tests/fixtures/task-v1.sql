CREATE TABLE task_graphs (
    graph_id       TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    revision       INTEGER NOT NULL CHECK(revision > 0),
    graph_json     TEXT NOT NULL
);

INSERT INTO task_graphs (graph_id, schema_version, revision, graph_json)
VALUES (
    'task-graph-v1',
    1,
    4,
    '{"tasks":{},"messages":[],"next_message_sequence":1}'
);
