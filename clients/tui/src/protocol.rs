//! Bounded Protocol v10 client over a supervised `yh` child process.

use std::{error::Error, ffi::OsStr, io, path::Path, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};
use y_harness::{
    PROTOCOL_VERSION, ProtocolCommand, ProtocolRequest, ProtocolResponse, ProtocolResponseBody,
    ProtocolResult,
};

const MAX_RESPONSE_BYTES: usize = 16_777_216;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

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
                command.arg("serve").arg(config);
            }
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
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
        Ok(Self {
            child,
            input: Some(input),
            output: BufReader::new(output),
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
        let response = timeout(REQUEST_TIMEOUT, operation)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Engine response timed out"))??;
        let response: ProtocolResponse = serde_json::from_slice(&response)?;
        if response.id.as_deref() != Some(request_id.as_str()) {
            return Err(io::Error::other(format!(
                "Engine response id {:?} did not match {request_id:?}",
                response.id
            ))
            .into());
        }
        if response.protocol_version != PROTOCOL_VERSION {
            return Err(io::Error::other(format!(
                "Engine protocol {:?} did not match {PROTOCOL_VERSION}",
                response.protocol_version
            ))
            .into());
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
    use tokio::io::BufReader;

    use super::read_bounded_line_with_limit;

    #[tokio::test]
    async fn response_limit_excludes_the_jsonl_delimiter() {
        let mut exact = BufReader::new(&b"1234\n"[..]);
        assert_eq!(
            read_bounded_line_with_limit(&mut exact, 4)
                .await
                .expect("accept exact frame"),
            b"1234"
        );

        let mut oversized = BufReader::new(&b"12345\n"[..]);
        let error = read_bounded_line_with_limit(&mut oversized, 4)
            .await
            .expect_err("reject oversized frame");
        assert!(error.to_string().contains("exceeds 4 bytes"));
    }
}
