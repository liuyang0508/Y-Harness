//! Dispatch-time executable drift measurement for one-shot Process Brokers.

use std::{
    fs::{self, File},
    io::Read as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use sha2::{Digest, Sha256};
use tokio::{
    fs::File as TokioFile,
    io::AsyncReadExt,
    time::{Instant, timeout_at},
};

use super::{
    ProcessBroker, ProcessBrokerDescriptor, ProcessExecutableIntegrity, ProcessOutput,
    ProcessRequest, validate_broker_descriptor, validate_request,
};
use crate::{CancellationToken, HarnessError, HarnessFuture, kernel::capture_capability_metadata};

/// Maximum command-file size accepted by a dispatch-time digest lock.
pub const MAX_DIGEST_LOCKED_PROGRAM_BYTES: u64 = 268_435_456;

/// Process Broker wrapper that remeasures one exact executable before dispatch.
///
/// Construction performs an initial bounded measurement. Every later request
/// must select the same path and is remeasured within that request's existing
/// cancellation and total timeout budget before the delegate can launch it.
/// This is drift detection, not an atomic OS exec measurement.
pub struct DigestLockedProcessBroker {
    delegate: Arc<dyn ProcessBroker>,
    descriptor: ProcessBrokerDescriptor,
    program: PathBuf,
    expected_sha256: String,
}

impl DigestLockedProcessBroker {
    /// Creates a dispatch-time digest lock around one existing broker.
    pub fn new(
        delegate: Arc<dyn ProcessBroker>,
        program: PathBuf,
        expected_sha256: impl Into<String>,
    ) -> Result<Self, HarnessError> {
        let expected_sha256 = expected_sha256.into();
        validate_program_sha256(&expected_sha256)?;
        if !program.is_absolute() {
            return Err(HarnessError::InvalidConfiguration(
                "digest-locked process executable path must be absolute".to_owned(),
            ));
        }
        verify_program_sha256_sync(&program, &expected_sha256)?;
        let mut descriptor =
            capture_capability_metadata("digest-locked process broker descriptor", || {
                delegate.descriptor()
            })?;
        validate_broker_descriptor(&descriptor)?;
        if !descriptor.executable_integrity.is_unmeasured() {
            return Err(HarnessError::InvalidConfiguration(
                "process broker already declares executable integrity".to_owned(),
            ));
        }
        descriptor.executable_integrity = ProcessExecutableIntegrity::DispatchSha256 {
            sha256: expected_sha256.clone(),
        };
        Ok(Self {
            delegate,
            descriptor,
            program,
            expected_sha256,
        })
    }
}

impl ProcessBroker for DigestLockedProcessBroker {
    fn descriptor(&self) -> ProcessBrokerDescriptor {
        self.descriptor.clone()
    }

    fn execute<'a>(
        &'a self,
        mut request: ProcessRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ProcessOutput> {
        Box::pin(async move {
            validate_request(&request)?;
            if request.program != self.program {
                return Err(HarnessError::Execution(
                    "digest-locked process request selected a different executable".to_owned(),
                ));
            }
            let deadline = Instant::now().checked_add(request.timeout).ok_or_else(|| {
                HarnessError::Execution("process timeout exceeds runtime clock".to_owned())
            })?;
            verify_program_sha256_before(
                &self.program,
                &self.expected_sha256,
                &cancellation,
                request.cancellation_phase,
                deadline,
            )
            .await?;
            request.timeout = deadline.saturating_duration_since(Instant::now());
            if request.timeout.is_zero() {
                return Err(HarnessError::Execution(
                    "configured executable digest exhausted process timeout".to_owned(),
                ));
            }
            self.delegate.execute(request, cancellation).await
        })
    }
}

pub(super) async fn verify_program_sha256_before(
    path: &Path,
    expected: &str,
    cancellation: &CancellationToken,
    phase: crate::ExecutionPhase,
    deadline: Instant,
) -> Result<(), HarnessError> {
    let measurement = verify_program_sha256(path, expected);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(HarnessError::Cancelled { phase })
        }
        measured = timeout_at(deadline, measurement) => {
            measured
                .map_err(|_| {
                    HarnessError::Execution(
                        "configured executable digest measurement timed out".to_owned(),
                    )
                })?
        }
    }
}

pub(super) fn validate_program_sha256(expected: &str) -> Result<(), HarnessError> {
    if expected.len() != 64
        || !expected
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HarnessError::InvalidConfiguration(
            "process executable SHA-256 must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn verify_program_sha256_sync(path: &Path, expected: &str) -> Result<(), HarnessError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        HarnessError::InvalidConfiguration(format!(
            "cannot inspect digest-locked process executable: {error}"
        ))
    })?;
    validate_digest_locked_metadata(&metadata, true)?;
    let mut file = File::open(path).map_err(|error| {
        HarnessError::InvalidConfiguration(format!(
            "cannot open digest-locked process executable: {error}"
        ))
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 65_536];
    let mut bytes = 0_u64;
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            HarnessError::InvalidConfiguration(format!(
                "cannot measure digest-locked process executable: {error}"
            ))
        })?;
        if read == 0 {
            break;
        }
        bytes = checked_byte_count(bytes, read, true)?;
        hasher.update(&buffer[..read]);
    }
    if bytes != metadata.len() {
        return Err(HarnessError::InvalidConfiguration(
            "digest-locked process executable changed during measurement".to_owned(),
        ));
    }
    let digest = hasher.finalize();
    if sha256_hex(&digest) != expected {
        return Err(HarnessError::InvalidConfiguration(
            "digest-locked process executable SHA-256 mismatch".to_owned(),
        ));
    }
    Ok(())
}

async fn verify_program_sha256(path: &Path, expected: &str) -> Result<(), HarnessError> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|error| {
        HarnessError::Execution(format!(
            "cannot inspect digest-locked process executable: {error}"
        ))
    })?;
    validate_digest_locked_metadata(&metadata, false)?;
    let mut file = TokioFile::open(path).await.map_err(|error| {
        HarnessError::Execution(format!(
            "cannot open digest-locked process executable: {error}"
        ))
    })?;
    let opened = file.metadata().await.map_err(|error| {
        HarnessError::Execution(format!(
            "cannot inspect opened digest-locked process executable: {error}"
        ))
    })?;
    validate_digest_locked_metadata(&opened, false)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 65_536];
    let mut bytes = 0_u64;
    loop {
        let read = file.read(&mut buffer).await.map_err(|error| {
            HarnessError::Execution(format!(
                "cannot measure digest-locked process executable: {error}"
            ))
        })?;
        if read == 0 {
            break;
        }
        bytes = checked_byte_count(bytes, read, false)?;
        hasher.update(&buffer[..read]);
    }
    if bytes != opened.len() {
        return Err(HarnessError::Execution(
            "digest-locked process executable changed during measurement".to_owned(),
        ));
    }
    let current = tokio::fs::symlink_metadata(path).await.map_err(|error| {
        HarnessError::Execution(format!(
            "cannot recheck digest-locked process executable: {error}"
        ))
    })?;
    validate_digest_locked_metadata(&current, false)?;
    if current.len() != bytes {
        return Err(HarnessError::Execution(
            "digest-locked process executable changed during measurement".to_owned(),
        ));
    }
    let digest = hasher.finalize();
    if sha256_hex(&digest) != expected {
        return Err(HarnessError::Execution(
            "digest-locked process executable SHA-256 mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn checked_byte_count(current: u64, read: usize, configuration: bool) -> Result<u64, HarnessError> {
    let message = || "digest-locked process executable byte count overflow".to_owned();
    let read = u64::try_from(read).map_err(|_| match configuration {
        true => HarnessError::InvalidConfiguration(message()),
        false => HarnessError::Execution(message()),
    })?;
    let total = current
        .checked_add(read)
        .ok_or_else(|| match configuration {
            true => HarnessError::InvalidConfiguration(message()),
            false => HarnessError::Execution(message()),
        })?;
    if total > MAX_DIGEST_LOCKED_PROGRAM_BYTES {
        let message = format!(
            "digest-locked process executable exceeds {MAX_DIGEST_LOCKED_PROGRAM_BYTES} bytes"
        );
        return Err(match configuration {
            true => HarnessError::InvalidConfiguration(message),
            false => HarnessError::Execution(message),
        });
    }
    Ok(total)
}

fn validate_digest_locked_metadata(
    metadata: &fs::Metadata,
    configuration: bool,
) -> Result<(), HarnessError> {
    let message = if !metadata.file_type().is_file() {
        Some("digest-locked process executable must be a regular file without symlinks".to_owned())
    } else if metadata.len() > MAX_DIGEST_LOCKED_PROGRAM_BYTES {
        Some(format!(
            "digest-locked process executable exceeds {MAX_DIGEST_LOCKED_PROGRAM_BYTES} bytes"
        ))
    } else {
        None
    };
    match (message, configuration) {
        (Some(message), true) => Err(HarnessError::InvalidConfiguration(message)),
        (Some(message), false) => Err(HarnessError::Execution(message)),
        (None, _) => Ok(()),
    }
}

fn sha256_hex(digest: &[u8]) -> String {
    let mut encoded = String::with_capacity(digest.len().saturating_mul(2));
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::{DigestLockedProcessBroker, sha256_hex};
    use crate::{
        CancellationToken, DenyProcessBroker, ExecutionPhase, HarnessError, HarnessFuture,
        ProcessBroker, ProcessBrokerDescriptor, ProcessExecutableIntegrity, ProcessIsolation,
        ProcessOutput, ProcessRequest,
    };

    struct RecordingBroker {
        timeouts: Mutex<Vec<Duration>>,
    }

    impl ProcessBroker for RecordingBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            ProcessBrokerDescriptor {
                name: "digest-recording".to_owned(),
                isolation: ProcessIsolation::Sandboxed {
                    mechanism: "test".to_owned(),
                },
                executable_integrity: ProcessExecutableIntegrity::Unmeasured,
            }
        }

        fn execute<'a>(
            &'a self,
            request: ProcessRequest,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async move {
                self.timeouts
                    .lock()
                    .expect("timeout lock")
                    .push(request.timeout);
                Ok(ProcessOutput {
                    success: true,
                    code: Some(0),
                    stdout: b"ok".to_vec(),
                    stderr: Vec::new(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                })
            })
        }
    }

    #[test]
    fn process_broker_descriptor_defaults_old_integrity_evidence_to_unmeasured() {
        let descriptor: ProcessBrokerDescriptor = serde_json::from_value(json!({
            "name": "legacy-broker",
            "isolation": {"kind": "denied"}
        }))
        .expect("legacy descriptor");
        assert_eq!(
            descriptor.executable_integrity,
            ProcessExecutableIntegrity::Unmeasured
        );
        assert!(
            serde_json::to_value(descriptor)
                .expect("encode descriptor")
                .get("executable_integrity")
                .is_none()
        );
    }

    #[tokio::test]
    async fn digest_locked_broker_remeasures_each_dispatch_and_recovers_after_restore() {
        let path = std::env::temp_dir().join(format!(
            "y-harness-digest-lock-{}",
            crate::EventId::generate()
        ));
        let original = b"stable executable fixture";
        fs::write(&path, original).expect("write digest fixture");
        let expected = sha256_hex(&Sha256::digest(original));
        let delegate = Arc::new(RecordingBroker {
            timeouts: Mutex::new(Vec::new()),
        });
        let broker =
            DigestLockedProcessBroker::new(delegate.clone(), path.clone(), expected.clone())
                .expect("digest lock");
        assert_eq!(
            broker.descriptor().executable_integrity,
            ProcessExecutableIntegrity::DispatchSha256 {
                sha256: expected.clone()
            }
        );
        let request = || ProcessRequest {
            program: path.clone(),
            args: Vec::new(),
            current_dir: std::env::temp_dir(),
            environment: BTreeMap::new(),
            secret_environment: BTreeMap::new(),
            stdin: b"request".to_vec(),
            timeout: Duration::from_secs(2),
            max_output_bytes: 1_024,
            cancellation_phase: ExecutionPhase::Effect,
        };

        broker
            .execute(request(), CancellationToken::new())
            .await
            .expect("matching dispatch");
        {
            let delegated = delegate.timeouts.lock().expect("timeouts");
            assert_eq!(delegated.len(), 1);
            assert!(delegated[0] < Duration::from_secs(2));
        }

        fs::write(&path, b"drifted credential-secret executable").expect("drift fixture");
        let error = broker
            .execute(request(), CancellationToken::new())
            .await
            .err()
            .expect("drifted command must not dispatch");
        assert!(error.to_string().contains("SHA-256 mismatch"));
        assert!(!error.to_string().contains("credential-secret"));
        assert_eq!(delegate.timeouts.lock().expect("timeouts").len(), 1);

        fs::write(&path, original).expect("restore digest fixture");
        broker
            .execute(request(), CancellationToken::new())
            .await
            .expect("restored dispatch");
        assert_eq!(delegate.timeouts.lock().expect("timeouts").len(), 2);

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert_eq!(
            broker
                .execute(request(), cancellation)
                .await
                .err()
                .expect("pre-cancelled measurement"),
            HarnessError::Cancelled {
                phase: ExecutionPhase::Effect
            }
        );
        assert_eq!(delegate.timeouts.lock().expect("timeouts").len(), 2);

        let mut wrong_path = request();
        wrong_path.program = std::env::temp_dir().join("different-executable");
        let error = broker
            .execute(wrong_path, CancellationToken::new())
            .await
            .err()
            .expect("different path");
        assert!(error.to_string().contains("different executable"));
        assert_eq!(delegate.timeouts.lock().expect("timeouts").len(), 2);
        fs::remove_file(path).expect("remove digest fixture");
    }

    #[test]
    fn digest_locked_broker_rejects_invalid_digest_mismatch_and_symlink() {
        let path = std::env::temp_dir().join(format!(
            "y-harness-digest-lock-invalid-{}",
            crate::EventId::generate()
        ));
        fs::write(&path, b"stable").expect("write digest fixture");
        let delegate: Arc<dyn ProcessBroker> = Arc::new(DenyProcessBroker);
        let invalid =
            DigestLockedProcessBroker::new(delegate.clone(), path.clone(), "A".repeat(64))
                .err()
                .expect("invalid lowercase digest");
        assert!(invalid.to_string().contains("lowercase hexadecimal"));
        let mismatch =
            DigestLockedProcessBroker::new(delegate.clone(), path.clone(), "0".repeat(64))
                .err()
                .expect("digest mismatch");
        assert!(mismatch.to_string().contains("SHA-256 mismatch"));

        #[cfg(unix)]
        {
            let link = std::env::temp_dir().join(format!(
                "y-harness-digest-lock-link-{}",
                crate::EventId::generate()
            ));
            std::os::unix::fs::symlink(&path, &link).expect("create digest symlink");
            let error = DigestLockedProcessBroker::new(
                delegate,
                link.clone(),
                sha256_hex(&Sha256::digest(b"stable")),
            )
            .err()
            .expect("symlink must fail closed");
            assert!(error.to_string().contains("without symlinks"));
            fs::remove_file(link).expect("remove digest symlink");
        }
        fs::remove_file(path).expect("remove digest fixture");
    }
}
