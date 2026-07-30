//! Bounded Protocol v28 client over a supervised `yh` child process.

use std::{
    error::Error,
    ffi::OsStr,
    io,
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    time::{sleep, timeout},
};
use y_harness::{
    PROTOCOL_VERSION, ProtocolCommand, ProtocolRequest, ProtocolResponse, ProtocolResponseBody,
    ProtocolResult,
};

const MAX_RESPONSE_BYTES: usize = 16_777_216;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const DIAGNOSTIC_SETTLE: Duration = Duration::from_millis(25);
const MAX_ENGINE_DIAGNOSTIC_BYTES: usize = 16_384;

pub(crate) type ClientResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Engine process startup mode selected by the operator.
pub(crate) enum EngineMode<'a> {
    Demo,
    Config(&'a Path),
}

/// One request-at-a-time JSONL client.
///
/// The child Engine remains the sole execution and durable-state authority.
pub(crate) struct ProtocolClient {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    diagnostics: Arc<Mutex<EngineDiagnostics>>,
    next_request_id: u64,
}

impl ProtocolClient {
    pub(crate) fn spawn(engine: &OsStr, mode: EngineMode<'_>) -> ClientResult<Self> {
        let mut command = Command::new(engine);
        match mode {
            EngineMode::Demo => {
                command.arg("serve-demo");
            }
            EngineMode::Config(config) => {
                if !config.is_file() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "Engine config does not exist: {}. Run `yh init <directory>` or use `yh-tui --demo`",
                            config.display()
                        ),
                    )
                    .into());
                }
                command.arg("serve").arg(config);
            }
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("Engine stdin was not created"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("Engine stdout was not created"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("Engine stderr was not created"))?;
        let diagnostics = Arc::new(Mutex::new(EngineDiagnostics::default()));
        tokio::spawn(drain_engine_diagnostics(stderr, diagnostics.clone()));
        Ok(Self {
            child,
            input: Some(input),
            output: BufReader::new(output),
            diagnostics,
            next_request_id: 1,
        })
    }

    pub(crate) async fn call(&mut self, command: ProtocolCommand) -> ClientResult<ProtocolResult> {
        let request_id = format!("tui-{}", self.next_request_id);
        self.next_request_id = self.next_request_id.saturating_add(1);
        let request = ProtocolRequest {
            id: request_id.clone(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            command,
        };
        let mut encoded = serde_json::to_vec(&request)?;
        encoded.push(b'\n');

        let operation = async {
            let input = self
                .input
                .as_mut()
                .ok_or_else(|| io::Error::other("Engine input is closed"))?;
            input.write_all(&encoded).await?;
            input.flush().await?;
            read_bounded_line(&mut self.output).await
        };
        let response = match timeout(REQUEST_TIMEOUT, operation).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                sleep(DIAGNOSTIC_SETTLE).await;
                return Err(self.with_diagnostics(error));
            }
            Err(_) => {
                return Err(
                    io::Error::new(io::ErrorKind::TimedOut, "Engine response timed out").into(),
                );
            }
        };
        let response: ProtocolResponse = serde_json::from_slice(&response)?;
        if response.id.as_deref() != Some(request_id.as_str()) {
            return Err(io::Error::other(format!(
                "Engine response id {:?} did not match {request_id:?}",
                response.id
            ))
            .into());
        }
        if response.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_mismatch(&response.protocol_version).into());
        }
        match response.body {
            ProtocolResponseBody::Success { result } => Ok(result),
            ProtocolResponseBody::Error { error } => Err(io::Error::other(format!(
                "{}: {}{}",
                error.code,
                error.message,
                if error.retryable { " (retryable)" } else { "" }
            ))
            .into()),
        }
    }

    fn with_diagnostics(
        &self,
        source: Box<dyn Error + Send + Sync>,
    ) -> Box<dyn Error + Send + Sync> {
        let diagnostic = self
            .diagnostics
            .lock()
            .ok()
            .map(|diagnostics| diagnostics.render())
            .unwrap_or_default();
        if diagnostic.is_empty() {
            source
        } else {
            io::Error::other(format!("{source}; {diagnostic}")).into()
        }
    }

    pub(crate) async fn shutdown(mut self) -> ClientResult<()> {
        self.input.take();
        match timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(status) => {
                let status = status?;
                if status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!("Engine exited with status {status}")).into())
                }
            }
            Err(_) => {
                self.child.kill().await?;
                let _ = self.child.wait().await;
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Engine did not stop after its protocol input closed",
                )
                .into())
            }
        }
    }
}

fn protocol_mismatch(engine_protocol: &str) -> io::Error {
    io::Error::other(format!(
        "Engine protocol {engine_protocol:?} did not match TUI protocol \
         {PROTOCOL_VERSION}. Install matching `yh` and `yh-tui` builds; from \
         one Y-Harness checkout run `./scripts/install.sh` and \
         `./scripts/install-tui.sh`"
    ))
}

#[derive(Default)]
struct EngineDiagnostics {
    bytes: Vec<u8>,
    truncated: bool,
}

impl EngineDiagnostics {
    fn record(&mut self, chunk: &[u8]) {
        let remaining = MAX_ENGINE_DIAGNOSTIC_BYTES.saturating_sub(self.bytes.len());
        let accepted = remaining.min(chunk.len());
        self.bytes.extend_from_slice(&chunk[..accepted]);
        self.truncated |= accepted < chunk.len();
    }

    fn render(&self) -> String {
        let mut rendered = String::from_utf8_lossy(&self.bytes)
            .trim()
            .chars()
            .map(|character| {
                if character.is_control() && !matches!(character, '\n' | '\t') {
                    '�'
                } else {
                    character
                }
            })
            .collect::<String>();
        if self.truncated {
            rendered.push('…');
        }
        if rendered.is_empty() {
            String::new()
        } else {
            format!("Engine stderr: {rendered}")
        }
    }
}

async fn drain_engine_diagnostics(
    mut stderr: ChildStderr,
    diagnostics: Arc<Mutex<EngineDiagnostics>>,
) {
    let mut chunk = [0_u8; 4096];
    loop {
        let read = match stderr.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        let Ok(mut diagnostics) = diagnostics.lock() else {
            return;
        };
        diagnostics.record(&chunk[..read]);
    }
}

async fn read_bounded_line(reader: &mut BufReader<ChildStdout>) -> ClientResult<Vec<u8>> {
    read_bounded_line_with_limit(reader, MAX_RESPONSE_BYTES).await
}

async fn read_bounded_line_with_limit<R>(
    reader: &mut R,
    maximum_bytes: usize,
) -> ClientResult<Vec<u8>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Engine stopped before sending a complete response",
            )
            .into());
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        let complete = available[..consumed].last() == Some(&b'\n');
        let content_bytes = consumed.saturating_sub(usize::from(complete));
        if line.len().saturating_add(content_bytes) > maximum_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Engine response exceeds {maximum_bytes} bytes"),
            )
            .into());
        }
        line.extend_from_slice(&available[..content_bytes]);
        reader.consume(consumed);
        if complete {
            return Ok(line);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, path::Path};

    use tokio::io::BufReader;

    use super::{
        EngineDiagnostics, EngineMode, MAX_ENGINE_DIAGNOSTIC_BYTES, ProtocolClient,
        protocol_mismatch, read_bounded_line_with_limit,
    };

    #[tokio::test]
    async fn response_limit_excludes_the_jsonl_delimiter() -> super::ClientResult<()> {
        let mut exact = BufReader::new(&b"1234\n"[..]);
        assert_eq!(read_bounded_line_with_limit(&mut exact, 4).await?, b"1234");

        let mut oversized = BufReader::new(&b"12345\n"[..]);
        let error = match read_bounded_line_with_limit(&mut oversized, 4).await {
            Ok(_) => return Err("oversized frame was accepted".into()),
            Err(error) => error,
        };
        assert!(error.to_string().contains("exceeds 4 bytes"));
        Ok(())
    }

    #[test]
    fn missing_config_fails_before_engine_spawn() -> super::ClientResult<()> {
        let error = match ProtocolClient::spawn(
            OsStr::new("engine-that-must-not-run"),
            EngineMode::Config(Path::new("missing-y-harness.json")),
        ) {
            Ok(_) => return Err("missing config was accepted".into()),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Engine config does not exist"));
        assert!(error.to_string().contains("yh-tui --demo"));
        Ok(())
    }

    #[test]
    fn engine_diagnostics_are_bounded_and_control_safe() {
        let mut diagnostics = EngineDiagnostics::default();
        diagnostics.record(b"\x1bsecret");
        diagnostics.record(&vec![b'x'; MAX_ENGINE_DIAGNOSTIC_BYTES]);
        let rendered = diagnostics.render();
        assert!(!rendered.contains('\u{1b}'));
        assert!(rendered.contains("�secret"));
        assert!(rendered.ends_with('…'));
        assert!(rendered.len() <= MAX_ENGINE_DIAGNOSTIC_BYTES + 32);
    }

    #[test]
    fn protocol_mismatch_names_both_coordinates_and_the_reinstall_path() {
        let diagnostic = protocol_mismatch("23").to_string();
        assert!(diagnostic.contains("Engine protocol \"23\""));
        assert!(diagnostic.contains("TUI protocol 28"));
        assert!(diagnostic.contains("./scripts/install.sh"));
        assert!(diagnostic.contains("./scripts/install-tui.sh"));
    }
}
