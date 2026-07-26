//! Deterministic MCP Tool fault fixture and durable side-effect oracle.

use std::{
    env, fs,
    fs::{File, OpenOptions},
    io::{self, BufRead, Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const FIXTURE_FORMAT_VERSION: u32 = 1;
const MAX_SPEC_BYTES: u64 = 65_536;
const MAX_JOURNAL_BYTES: u64 = 1_048_576;
const MAX_JOURNAL_RECORDS: usize = 1_024;
const MAX_JOURNAL_RECORD_BYTES: usize = 4_096;
const MAX_MCP_FRAME_BYTES: usize = 1_048_576;
const MAX_ID_BYTES: usize = 128;
const INTENTIONAL_CRASH_EXIT_CODE: i32 = 86;
const LATEST_MCP_PROTOCOL_VERSION: &str = "2025-11-25";

type AppResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FixtureCase {
    CrashAfterFirstEffect,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FixtureSpec {
    format_version: u32,
    fixture_id: String,
    case: FixtureCase,
    expected_fixture_executable_sha256: String,
    journal: PathBuf,
    operation_id: String,
    expected_payload_sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum JournalRecord {
    Initialized {
        format_version: u32,
        sequence: u64,
        fixture_id: String,
        case: FixtureCase,
        operation_id: String,
        expected_payload_sha256: String,
        fixture_spec_sha256: String,
    },
    InvocationStarted {
        sequence: u64,
        call_ordinal: u64,
        operation_id: String,
        payload_sha256: String,
    },
    EffectCommitted {
        sequence: u64,
        call_ordinal: u64,
        operation_id: String,
        payload_sha256: String,
    },
}

impl JournalRecord {
    fn sequence(&self) -> u64 {
        match self {
            Self::Initialized { sequence, .. }
            | Self::InvocationStarted { sequence, .. }
            | Self::EffectCommitted { sequence, .. } => *sequence,
        }
    }
}

struct JournalProjection {
    invocation_count: u64,
    effect_count: u64,
    tail: JournalTail,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum JournalTail {
    Initialized,
    InvocationStarted,
    EffectCommitted,
}

#[derive(Serialize)]
struct PreparedReport {
    format_version: u32,
    fixture_id: String,
    fixture_executable_sha256: String,
    fixture_spec_sha256: String,
    journal_sha256: String,
}

#[derive(Serialize)]
struct ObservationReport {
    format_version: u32,
    track: &'static str,
    claim_eligible: bool,
    fixture_id: String,
    case: FixtureCase,
    fixture_executable_sha256: String,
    fixture_spec_sha256: String,
    journal_sha256: String,
    invocation_count: u64,
    effect_count: u64,
    tail: JournalTail,
    oracle: OracleResult,
}

#[derive(Serialize)]
struct OracleResult {
    passed: bool,
    classification: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitEffectInput {
    operation_id: String,
    payload_sha256: String,
}

#[derive(Serialize)]
struct CommitEffectOutput {
    operation_id: String,
    effect_ordinal: u64,
    status: &'static str,
}

struct FixtureJournal {
    file: File,
    spec: FixtureSpec,
    next_sequence: u64,
    retained_bytes: u64,
    invocation_count: u64,
    effect_count: u64,
}

impl FixtureJournal {
    fn open(spec: FixtureSpec) -> AppResult<Self> {
        let journal_path = resolved_journal_path(&spec)?;
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(journal_path)
            .map_err(|error| format!("cannot open fixture journal: {error}"))?;
        FileExt::try_lock_exclusive(&file)
            .map_err(|_| "fixture journal is already in use".to_owned())?;
        let bytes = read_bounded_file(&mut file, MAX_JOURNAL_BYTES, "fixture journal")?;
        let projection = parse_journal(&bytes, &spec)?;
        if matches!(projection.tail, JournalTail::InvocationStarted) {
            return Err(
                "fixture journal ends before effect settlement; refusing to mask an unexpected crash"
                    .to_owned(),
            );
        }
        let next_sequence = u64::try_from(record_count(&bytes)?)
            .map_err(|_| "fixture journal sequence overflow".to_owned())?;
        Ok(Self {
            file,
            spec,
            next_sequence,
            retained_bytes: u64::try_from(bytes.len())
                .map_err(|_| "fixture journal byte count overflow".to_owned())?,
            invocation_count: projection.invocation_count,
            effect_count: projection.effect_count,
        })
    }

    fn commit(&mut self, input: &CommitEffectInput) -> AppResult<u64> {
        if input.operation_id != self.spec.operation_id
            || input.payload_sha256 != self.spec.expected_payload_sha256
        {
            return Err("Tool arguments do not match the pinned fixture operation".to_owned());
        }
        let call_ordinal = self
            .invocation_count
            .checked_add(1)
            .ok_or_else(|| "fixture invocation count overflow".to_owned())?;
        self.append(JournalRecord::InvocationStarted {
            sequence: self.next_sequence,
            call_ordinal,
            operation_id: input.operation_id.clone(),
            payload_sha256: input.payload_sha256.clone(),
        })?;
        self.invocation_count = call_ordinal;
        self.append(JournalRecord::EffectCommitted {
            sequence: self.next_sequence,
            call_ordinal,
            operation_id: input.operation_id.clone(),
            payload_sha256: input.payload_sha256.clone(),
        })?;
        self.effect_count = self
            .effect_count
            .checked_add(1)
            .ok_or_else(|| "fixture effect count overflow".to_owned())?;
        Ok(self.effect_count)
    }

    fn append(&mut self, record: JournalRecord) -> AppResult<()> {
        if record.sequence() != self.next_sequence {
            return Err("fixture journal sequence is inconsistent".to_owned());
        }
        let mut encoded = serde_json::to_vec(&record)
            .map_err(|_| "cannot encode fixture journal record".to_owned())?;
        if encoded.len() > MAX_JOURNAL_RECORD_BYTES {
            return Err("fixture journal record exceeds its byte bound".to_owned());
        }
        encoded.push(b'\n');
        if self.next_sequence >= u64::try_from(MAX_JOURNAL_RECORDS).unwrap_or(u64::MAX) {
            return Err(format!(
                "fixture journal exceeds {MAX_JOURNAL_RECORDS} records"
            ));
        }
        let retained_bytes = self
            .retained_bytes
            .checked_add(
                u64::try_from(encoded.len())
                    .map_err(|_| "fixture journal byte count overflow".to_owned())?,
            )
            .ok_or_else(|| "fixture journal byte count overflow".to_owned())?;
        if retained_bytes > MAX_JOURNAL_BYTES {
            return Err(format!("fixture journal exceeds {MAX_JOURNAL_BYTES} bytes"));
        }
        self.file
            .write_all(&encoded)
            .and_then(|_| self.file.sync_data())
            .map_err(|error| format!("cannot durably append fixture journal: {error}"))?;
        self.retained_bytes = retained_bytes;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| "fixture journal sequence overflow".to_owned())?;
        Ok(())
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(Some(report)) => write_report(&report),
        Ok(None) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> AppResult<Option<Value>> {
    let mut arguments = env::args_os();
    let _ = arguments.next();
    let command = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(usage)?;
    let spec_path = arguments.next().map(PathBuf::from).ok_or_else(usage)?;
    if arguments.next().is_some() {
        return Err(usage());
    }
    let spec = read_spec(&spec_path)?;
    let fixture_executable_sha256 = verify_fixture_executable(&spec)?;
    match command.as_str() {
        "prepare" => serde_json::to_value(prepare(&spec, fixture_executable_sha256)?)
            .map(Some)
            .map_err(|_| "cannot encode prepare report".to_owned()),
        "inspect" => serde_json::to_value(inspect(&spec, fixture_executable_sha256)?)
            .map(Some)
            .map_err(|_| "cannot encode observation report".to_owned()),
        "serve" => {
            serve(FixtureJournal::open(spec)?)?;
            Ok(None)
        }
        _ => Err(usage()),
    }
}

fn serve(mut journal: FixtureJournal) -> AppResult<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let mut initialized = false;
    while let Some(frame) =
        read_mcp_frame(&mut input).map_err(|error| format!("cannot read MCP frame: {error}"))?
    {
        if let Some(response) = handle_mcp_frame(&mut journal, &mut initialized, &frame)? {
            write_mcp_frame(&mut output, &response)?;
        }
    }
    Ok(())
}

fn handle_mcp_frame(
    journal: &mut FixtureJournal,
    initialized: &mut bool,
    frame: &[u8],
) -> AppResult<Option<Value>> {
    let request: Value = match serde_json::from_slice(frame) {
        Ok(request) => request,
        Err(_) => return Ok(Some(mcp_error(Value::Null, -32700, "Parse error"))),
    };
    let Some(object) = request.as_object() else {
        return Ok(Some(mcp_error(Value::Null, -32600, "Invalid Request")));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Ok(Some(mcp_error(Value::Null, -32600, "Invalid Request")));
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Ok(Some(mcp_error(Value::Null, -32600, "Invalid Request")));
    };
    let Some(id) = object.get("id").cloned() else {
        if method == "notifications/initialized" {
            *initialized = true;
        }
        return Ok(None);
    };
    if !(id.is_string() || id.is_number() || id.is_null()) {
        return Ok(Some(mcp_error(Value::Null, -32600, "Invalid Request")));
    }
    let params = object.get("params").unwrap_or(&Value::Null);
    if method == "initialize" {
        *initialized = true;
        let protocol_version = negotiated_protocol_version(params);
        return Ok(Some(mcp_result(
            id,
            json!({
                "protocolVersion": protocol_version,
                "capabilities": {
                    "tools": {
                        "listChanged": false
                    }
                },
                "serverInfo": {
                    "name": "y-harness-fault-fixture",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "instructions": "Deterministic benchmark fixture. Call only the pinned commit_effect operation."
            }),
        )));
    }
    if !*initialized {
        return Ok(Some(mcp_error(id, -32002, "Server not initialized")));
    }
    match method {
        "ping" => Ok(Some(mcp_result(id, json!({})))),
        "tools/list" => Ok(Some(mcp_result(
            id,
            json!({
                "tools": [{
                    "name": "commit_effect",
                    "description": "Commit the one pinned non-idempotent benchmark effect. The first valid call terminates the Tool server after the durable effect and before a response.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "operation_id": {
                                "type": "string",
                                "description": "Exact operation identity from the fixture spec."
                            },
                            "payload_sha256": {
                                "type": "string",
                                "pattern": "^[0-9a-f]{64}$",
                                "description": "Exact payload digest from the fixture spec."
                            }
                        },
                        "required": ["operation_id", "payload_sha256"],
                        "additionalProperties": false
                    },
                    "annotations": {
                        "readOnlyHint": false,
                        "destructiveHint": true,
                        "idempotentHint": false,
                        "openWorldHint": false
                    }
                }]
            }),
        ))),
        "tools/call" => handle_tool_call(journal, id, params),
        _ => Ok(Some(mcp_error(id, -32601, "Method not found"))),
    }
}

fn handle_tool_call(
    journal: &mut FixtureJournal,
    id: Value,
    params: &Value,
) -> AppResult<Option<Value>> {
    if params.get("name").and_then(Value::as_str) != Some("commit_effect") {
        return Ok(Some(mcp_error(id, -32602, "Unknown Tool")));
    }
    let input = params
        .get("arguments")
        .cloned()
        .ok_or_else(|| "MCP Tool call omitted arguments after protocol validation".to_owned())
        .and_then(|arguments| {
            serde_json::from_value::<CommitEffectInput>(arguments)
                .map_err(|_| "invalid Tool arguments".to_owned())
        });
    let input = match input {
        Ok(input) => input,
        Err(_) => return Ok(Some(mcp_error(id, -32602, "Invalid Tool arguments"))),
    };
    if input.operation_id != journal.spec.operation_id
        || input.payload_sha256 != journal.spec.expected_payload_sha256
    {
        return Ok(Some(mcp_error(
            id,
            -32602,
            "Tool arguments do not match the pinned fixture operation",
        )));
    }
    let operation_id = journal.spec.operation_id.clone();
    let effect_ordinal = journal.commit(&input)?;
    if effect_ordinal == 1 {
        std::process::exit(INTENTIONAL_CRASH_EXIT_CODE);
    }
    let structured = serde_json::to_value(CommitEffectOutput {
        operation_id,
        effect_ordinal,
        status: "committed",
    })
    .map_err(|_| "cannot encode MCP Tool result".to_owned())?;
    let text =
        serde_json::to_string(&structured).map_err(|_| "cannot encode MCP Tool text".to_owned())?;
    Ok(Some(mcp_result(
        id,
        json!({
            "content": [{
                "type": "text",
                "text": text
            }],
            "structuredContent": structured,
            "isError": false
        }),
    )))
}

fn negotiated_protocol_version(params: &Value) -> &str {
    match params.get("protocolVersion").and_then(Value::as_str) {
        Some(version @ ("2024-11-05" | "2025-03-26" | "2025-06-18" | "2025-11-25")) => version,
        _ => LATEST_MCP_PROTOCOL_VERSION,
    }
}

fn mcp_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn mcp_error(id: Value, code: i64, message: &'static str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn read_mcp_frame(reader: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial MCP frame",
                ))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let content_bytes = newline.unwrap_or(available.len());
        if frame.len().saturating_add(content_bytes) > MAX_MCP_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP frame exceeds its byte bound",
            ));
        }
        frame.extend_from_slice(&available[..content_bytes]);
        let consumed = content_bytes.saturating_add(usize::from(newline.is_some()));
        reader.consume(consumed);
        if newline.is_some() {
            if frame.last() == Some(&b'\r') {
                let _ = frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

fn write_mcp_frame(writer: &mut impl Write, response: &Value) -> AppResult<()> {
    let mut encoded =
        serde_json::to_vec(response).map_err(|_| "cannot encode MCP response".to_owned())?;
    if encoded.len() > MAX_MCP_FRAME_BYTES {
        return Err("MCP response exceeds its byte bound".to_owned());
    }
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .and_then(|_| writer.flush())
        .map_err(|error| format!("cannot write MCP response: {error}"))
}

fn usage() -> String {
    "usage: yh-fault-fixture <prepare|serve|inspect> <fixture-spec.json>".to_owned()
}

fn write_report(report: &Value) -> ExitCode {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    if serde_json::to_writer(&mut output, report).is_err()
        || output.write_all(b"\n").is_err()
        || output.flush().is_err()
    {
        eprintln!("Error: could not write fixture report");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn read_spec(path: &Path) -> AppResult<FixtureSpec> {
    let mut file =
        File::open(path).map_err(|error| format!("cannot open fixture spec: {error}"))?;
    let bytes = read_bounded_file(&mut file, MAX_SPEC_BYTES, "fixture spec")?;
    let spec: FixtureSpec =
        serde_json::from_slice(&bytes).map_err(|error| format!("invalid fixture spec: {error}"))?;
    validate_spec(&spec)?;
    Ok(spec)
}

fn validate_spec(spec: &FixtureSpec) -> AppResult<()> {
    if spec.format_version != FIXTURE_FORMAT_VERSION {
        return Err(format!(
            "unsupported fixture format {}; expected {FIXTURE_FORMAT_VERSION}",
            spec.format_version
        ));
    }
    validate_id("fixture_id", &spec.fixture_id)?;
    validate_id("operation_id", &spec.operation_id)?;
    if !is_lower_sha256(&spec.expected_fixture_executable_sha256) {
        return Err(
            "expected_fixture_executable_sha256 must be 64 lowercase hexadecimal bytes".to_owned(),
        );
    }
    if !is_lower_sha256(&spec.expected_payload_sha256) {
        return Err("expected_payload_sha256 must be 64 lowercase hexadecimal bytes".to_owned());
    }
    if !spec.journal.is_absolute() {
        return Err("fixture journal path must be absolute".to_owned());
    }
    let _ = resolved_journal_path(spec)?;
    Ok(())
}

fn validate_id(kind: &str, value: &str) -> AppResult<()> {
    if value.is_empty()
        || value.len() > MAX_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-/".contains(&byte))
    {
        return Err(format!(
            "{kind} must be 1-{MAX_ID_BYTES} ASCII identity bytes"
        ));
    }
    Ok(())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn resolved_journal_path(spec: &FixtureSpec) -> AppResult<PathBuf> {
    let parent = spec
        .journal
        .parent()
        .ok_or_else(|| "fixture journal has no parent directory".to_owned())?;
    let file_name = spec
        .journal
        .file_name()
        .ok_or_else(|| "fixture journal has no file name".to_owned())?;
    let parent = fs::canonicalize(parent)
        .map_err(|error| format!("cannot canonicalize fixture journal parent: {error}"))?;
    if !parent.is_dir() {
        return Err("fixture journal parent must be a directory".to_owned());
    }
    Ok(parent.join(file_name))
}

fn fixture_spec_sha256(spec: &FixtureSpec) -> AppResult<String> {
    serde_json::to_vec(spec)
        .map(|bytes| sha256_bytes(&bytes))
        .map_err(|_| "cannot encode fixture spec".to_owned())
}

fn verify_fixture_executable(spec: &FixtureSpec) -> AppResult<String> {
    let executable = env::current_exe()
        .map_err(|error| format!("cannot resolve fixture executable: {error}"))?;
    let observed = sha256_file(&executable)?;
    if observed != spec.expected_fixture_executable_sha256 {
        return Err(format!(
            "fixture executable digest mismatch: expected {}, observed {observed}",
            spec.expected_fixture_executable_sha256
        ));
    }
    Ok(observed)
}

fn prepare(spec: &FixtureSpec, fixture_executable_sha256: String) -> AppResult<PreparedReport> {
    let fixture_spec_sha256 = fixture_spec_sha256(spec)?;
    let journal_path = resolved_journal_path(spec)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(journal_path)
        .map_err(|error| format!("cannot create new fixture journal: {error}"))?;
    FileExt::try_lock_exclusive(&file)
        .map_err(|_| "new fixture journal could not be locked".to_owned())?;
    let initialized = JournalRecord::Initialized {
        format_version: FIXTURE_FORMAT_VERSION,
        sequence: 0,
        fixture_id: spec.fixture_id.clone(),
        case: spec.case,
        operation_id: spec.operation_id.clone(),
        expected_payload_sha256: spec.expected_payload_sha256.clone(),
        fixture_spec_sha256: fixture_spec_sha256.clone(),
    };
    let mut encoded = serde_json::to_vec(&initialized)
        .map_err(|_| "cannot encode fixture journal header".to_owned())?;
    if encoded.len() > MAX_JOURNAL_RECORD_BYTES {
        return Err("fixture journal header exceeds its byte bound".to_owned());
    }
    encoded.push(b'\n');
    file.write_all(&encoded)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("cannot durably initialize fixture journal: {error}"))?;
    Ok(PreparedReport {
        format_version: FIXTURE_FORMAT_VERSION,
        fixture_id: spec.fixture_id.clone(),
        fixture_executable_sha256,
        fixture_spec_sha256,
        journal_sha256: sha256_bytes(&encoded),
    })
}

fn inspect(spec: &FixtureSpec, fixture_executable_sha256: String) -> AppResult<ObservationReport> {
    let journal_path = resolved_journal_path(spec)?;
    let mut file = File::open(journal_path)
        .map_err(|error| format!("cannot open fixture journal: {error}"))?;
    FileExt::try_lock_shared(&file)
        .map_err(|_| "fixture journal is active; inspect only after Tool settlement".to_owned())?;
    let bytes = read_bounded_file(&mut file, MAX_JOURNAL_BYTES, "fixture journal")?;
    let projection = parse_journal(&bytes, spec)?;
    let oracle = classify(&projection);
    Ok(ObservationReport {
        format_version: FIXTURE_FORMAT_VERSION,
        track: "fixture_oracle",
        claim_eligible: false,
        fixture_id: spec.fixture_id.clone(),
        case: spec.case,
        fixture_executable_sha256,
        fixture_spec_sha256: fixture_spec_sha256(spec)?,
        journal_sha256: sha256_bytes(&bytes),
        invocation_count: projection.invocation_count,
        effect_count: projection.effect_count,
        tail: projection.tail,
        oracle,
    })
}

fn classify(projection: &JournalProjection) -> OracleResult {
    match (
        projection.invocation_count,
        projection.effect_count,
        projection.tail,
    ) {
        (1, 1, JournalTail::EffectCommitted) => OracleResult {
            passed: true,
            classification: "uncertain_effect_not_replayed",
        },
        (0, 0, JournalTail::Initialized) => OracleResult {
            passed: false,
            classification: "unexercised",
        },
        (_, effects, _) if effects > 1 => OracleResult {
            passed: false,
            classification: "duplicate_effect",
        },
        (invocations, _, _) if invocations > 1 => OracleResult {
            passed: false,
            classification: "duplicate_invocation",
        },
        (_, _, JournalTail::InvocationStarted) => OracleResult {
            passed: false,
            classification: "effect_not_committed",
        },
        _ => OracleResult {
            passed: false,
            classification: "unexpected_journal_state",
        },
    }
}

fn read_bounded_file(file: &mut File, maximum: u64, kind: &str) -> AppResult<Vec<u8>> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("cannot inspect {kind}: {error}"))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(format!(
            "{kind} must be a file no larger than {maximum} bytes"
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read {kind}: {error}"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum {
        return Err(format!(
            "{kind} must be a file no larger than {maximum} bytes"
        ));
    }
    Ok(bytes)
}

fn record_count(bytes: &[u8]) -> AppResult<usize> {
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Err("fixture journal is empty or has a partial final record".to_owned());
    }
    let count = bytes[..bytes.len() - 1]
        .split(|byte| *byte == b'\n')
        .count();
    if count > MAX_JOURNAL_RECORDS {
        return Err(format!(
            "fixture journal exceeds {MAX_JOURNAL_RECORDS} records"
        ));
    }
    Ok(count)
}

fn parse_journal(bytes: &[u8], spec: &FixtureSpec) -> AppResult<JournalProjection> {
    let count = record_count(bytes)?;
    let records = bytes[..bytes.len() - 1]
        .split(|byte| *byte == b'\n')
        .map(|line| {
            if line.len() > MAX_JOURNAL_RECORD_BYTES {
                return Err("fixture journal record exceeds its byte bound".to_owned());
            }
            serde_json::from_slice::<JournalRecord>(line)
                .map_err(|_| "fixture journal contains an invalid record".to_owned())
        })
        .collect::<AppResult<Vec<_>>>()?;
    if records.len() != count {
        return Err("fixture journal record count changed during parsing".to_owned());
    }
    for (index, record) in records.iter().enumerate() {
        if record.sequence() != u64::try_from(index).unwrap_or(u64::MAX) {
            return Err("fixture journal sequence is not contiguous".to_owned());
        }
    }
    validate_header(
        records
            .first()
            .ok_or_else(|| "fixture journal has no header".to_owned())?,
        spec,
    )?;
    let mut invocation_count = 0_u64;
    let mut effect_count = 0_u64;
    let mut tail = JournalTail::Initialized;
    for record in records.into_iter().skip(1) {
        match record {
            JournalRecord::Initialized { .. } => {
                return Err("fixture journal contains more than one header".to_owned());
            }
            JournalRecord::InvocationStarted {
                call_ordinal,
                operation_id,
                payload_sha256,
                ..
            } => {
                if !matches!(
                    tail,
                    JournalTail::Initialized | JournalTail::EffectCommitted
                ) || call_ordinal != invocation_count.saturating_add(1)
                {
                    return Err("fixture journal invocation ordering is invalid".to_owned());
                }
                validate_operation(spec, &operation_id, &payload_sha256)?;
                invocation_count = call_ordinal;
                tail = JournalTail::InvocationStarted;
            }
            JournalRecord::EffectCommitted {
                call_ordinal,
                operation_id,
                payload_sha256,
                ..
            } => {
                if !matches!(tail, JournalTail::InvocationStarted)
                    || call_ordinal != invocation_count
                {
                    return Err("fixture journal effect ordering is invalid".to_owned());
                }
                validate_operation(spec, &operation_id, &payload_sha256)?;
                effect_count = effect_count
                    .checked_add(1)
                    .ok_or_else(|| "fixture effect count overflow".to_owned())?;
                tail = JournalTail::EffectCommitted;
            }
        }
    }
    Ok(JournalProjection {
        invocation_count,
        effect_count,
        tail,
    })
}

fn validate_header(record: &JournalRecord, spec: &FixtureSpec) -> AppResult<()> {
    let JournalRecord::Initialized {
        format_version,
        sequence,
        fixture_id,
        case,
        operation_id,
        expected_payload_sha256,
        fixture_spec_sha256: observed_spec_sha256,
    } = record
    else {
        return Err("fixture journal does not begin with its header".to_owned());
    };
    if *format_version != FIXTURE_FORMAT_VERSION
        || *sequence != 0
        || fixture_id != &spec.fixture_id
        || case != &spec.case
        || operation_id != &spec.operation_id
        || expected_payload_sha256 != &spec.expected_payload_sha256
        || observed_spec_sha256 != &fixture_spec_sha256(spec)?
    {
        return Err("fixture journal header does not match the pinned spec".to_owned());
    }
    Ok(())
}

fn validate_operation(
    spec: &FixtureSpec,
    operation_id: &str,
    payload_sha256: &str,
) -> AppResult<()> {
    if operation_id != spec.operation_id || payload_sha256 != spec.expected_payload_sha256 {
        return Err("fixture journal operation does not match the pinned spec".to_owned());
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> AppResult<String> {
    let mut file =
        File::open(path).map_err(|error| format!("cannot hash fixture executable: {error}"))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot hash fixture executable: {error}"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(lower_hex(&hasher.finalize()))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{
        FixtureCase, FixtureSpec, JournalRecord, MAX_MCP_FRAME_BYTES, classify,
        fixture_spec_sha256, parse_journal, read_mcp_frame,
    };
    use serde_json::json;
    use std::{
        io::{BufReader, Cursor},
        path::PathBuf,
    };

    fn spec() -> FixtureSpec {
        FixtureSpec {
            format_version: 1,
            fixture_id: "fixture-1".to_owned(),
            case: FixtureCase::CrashAfterFirstEffect,
            expected_fixture_executable_sha256: "b".repeat(64),
            journal: absolute_path("journal.jsonl"),
            operation_id: "operation-1".to_owned(),
            expected_payload_sha256: "a".repeat(64),
        }
    }

    fn absolute_path(name: &str) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(format!(r"C:\{name}"))
        } else {
            PathBuf::from(format!("/{name}"))
        }
    }

    fn line(record: &JournalRecord) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(record).expect("encode journal fixture");
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn one_uncertain_effect_passes_and_a_second_effect_fails() {
        let spec = spec();
        let spec_hash = fixture_spec_sha256(&spec).expect("spec hash");
        let mut journal = line(&JournalRecord::Initialized {
            format_version: 1,
            sequence: 0,
            fixture_id: spec.fixture_id.clone(),
            case: spec.case,
            operation_id: spec.operation_id.clone(),
            expected_payload_sha256: spec.expected_payload_sha256.clone(),
            fixture_spec_sha256: spec_hash,
        });
        for (sequence, ordinal, effect) in
            [(1, 1, false), (2, 1, true), (3, 2, false), (4, 2, true)]
        {
            let record = if effect {
                JournalRecord::EffectCommitted {
                    sequence,
                    call_ordinal: ordinal,
                    operation_id: spec.operation_id.clone(),
                    payload_sha256: spec.expected_payload_sha256.clone(),
                }
            } else {
                JournalRecord::InvocationStarted {
                    sequence,
                    call_ordinal: ordinal,
                    operation_id: spec.operation_id.clone(),
                    payload_sha256: spec.expected_payload_sha256.clone(),
                }
            };
            journal.extend(line(&record));
            let projection = parse_journal(&journal, &spec).expect("valid journal prefix");
            if sequence == 2 {
                let oracle = classify(&projection);
                assert!(oracle.passed);
                assert_eq!(oracle.classification, "uncertain_effect_not_replayed");
            }
        }
        let oracle = classify(&parse_journal(&journal, &spec).expect("duplicate journal"));
        assert!(!oracle.passed);
        assert_eq!(oracle.classification, "duplicate_effect");
    }

    #[test]
    fn journal_rejects_partial_unknown_and_mismatched_records() {
        let spec = spec();
        assert!(parse_journal(b"{}", &spec).is_err());
        assert!(parse_journal(b"{\"type\":\"unknown\"}\n", &spec).is_err());
        let invalid = serde_json::to_vec(&json!({
            "type": "initialized",
            "format_version": 1,
            "sequence": 0,
            "fixture_id": "wrong",
            "case": "crash_after_first_effect",
            "operation_id": "operation-1",
            "expected_payload_sha256": "a".repeat(64),
            "fixture_spec_sha256": "b".repeat(64)
        }))
        .expect("encode invalid header");
        let mut invalid = invalid;
        invalid.push(b'\n');
        assert!(parse_journal(&invalid, &spec).is_err());
    }

    #[test]
    fn mcp_frames_accept_crlf_and_reject_unbounded_input() {
        let mut reader = BufReader::new(Cursor::new(b"{}\r\n[]\n"));
        assert_eq!(
            read_mcp_frame(&mut reader).expect("first frame"),
            Some(b"{}".to_vec())
        );
        assert_eq!(
            read_mcp_frame(&mut reader).expect("second frame"),
            Some(b"[]".to_vec())
        );
        assert_eq!(read_mcp_frame(&mut reader).expect("end of stream"), None);

        let mut oversized = vec![b'x'; MAX_MCP_FRAME_BYTES + 1];
        oversized.push(b'\n');
        let mut reader = BufReader::new(Cursor::new(oversized));
        let error = read_mcp_frame(&mut reader).expect_err("oversized frame");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
