PRAGMA foreign_keys = ON;

CREATE TABLE events (
    sequence       INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    thread_id      TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    event_json     TEXT NOT NULL
);
CREATE INDEX events_thread_sequence ON events(thread_id, sequence);
CREATE TABLE streams (
    thread_id TEXT PRIMARY KEY,
    version   INTEGER NOT NULL CHECK(version >= 0)
);
CREATE TABLE stream_recovery (
    thread_id      TEXT PRIMARY KEY,
    recovery_bytes INTEGER NOT NULL CHECK(recovery_bytes >= 0),
    FOREIGN KEY(thread_id) REFERENCES streams(thread_id)
);
CREATE TABLE state_snapshots (
    thread_id       TEXT PRIMARY KEY,
    stream_version  INTEGER NOT NULL CHECK(stream_version > 0),
    snapshot_json   TEXT NOT NULL
);

INSERT INTO events
    (event_id, thread_id, recorded_at_ms, schema_version, event_json)
VALUES
    ('event-v1-created', 'thread-v1', 1, 1,
     '{"type":"thread_created","created_at_ms":1}');
INSERT INTO streams (thread_id, version) VALUES ('thread-v1', 1);
INSERT INTO stream_recovery (thread_id, recovery_bytes)
SELECT 'thread-v1', length(CAST(event_json AS BLOB)) + 512
FROM events
WHERE event_id = 'event-v1-created';
