PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;

CREATE TABLE approval_records (
    approval_id     TEXT PRIMARY KEY,
    thread_id       TEXT NOT NULL,
    turn_id         TEXT NOT NULL,
    status          TEXT NOT NULL
                    CHECK(status IN ('pending', 'settled', 'orphaned')),
    revision        INTEGER NOT NULL CHECK(revision > 0),
    requested_at_ms INTEGER NOT NULL,
    record_json     TEXT NOT NULL
);

CREATE INDEX approval_pending_order
    ON approval_records(status, requested_at_ms, approval_id);
CREATE INDEX approval_turn
    ON approval_records(thread_id, turn_id, status);

CREATE TABLE approval_inbox_metadata (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL CHECK(value > 0)
);
INSERT INTO approval_inbox_metadata (key, value) VALUES ('record_schema', 2);

INSERT INTO approval_records
    (approval_id, thread_id, turn_id, status, revision, requested_at_ms, record_json)
VALUES
    (
        'approval-v2',
        'thread-v2',
        'turn-v2',
        'pending',
        1,
        1,
        '{"schema_version":2,"request":{"id":"approval-v2","requested_by":{"kind":"local_process"},"authorization":{"thread_id":"thread-v2","turn_id":"turn-v2","call_id":"call-v2","descriptor":{"name":"deploy","description":"deploy one bounded artifact","input_schema":{"type":"object"}},"origin":{"kind":"built_in"},"input":{"artifact":"a-2"}},"reason":"deployment changes external state","risk":"high"},"status":{"status":"pending"},"revision":1,"requested_at_ms":1,"settled_at_ms":null}'
    );
