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

INSERT INTO approval_records
    (approval_id, thread_id, turn_id, status, revision, requested_at_ms, record_json)
VALUES
    (
        'approval-v1',
        'thread-v1',
        'turn-v1',
        'pending',
        1,
        1,
        '{"schema_version":1,"request":{"id":"approval-v1","authorization":{"thread_id":"thread-v1","turn_id":"turn-v1","call_id":"call-v1","descriptor":{"name":"deploy","description":"deploy one bounded artifact","input_schema":{"type":"object"}},"origin":{"kind":"built_in"},"input":{"artifact":"a-1"}},"reason":"deployment changes external state","risk":"high"},"status":{"status":"pending"},"revision":1,"requested_at_ms":1,"settled_at_ms":null}'
    );
