//! Optional lifecycle owner for host-driven Temporal maintenance.
//!
//! This module belongs to the reference service, not the embedded Engine. It
//! supplies wall-clock time, polling cadence, diagnostics, and shutdown while
//! [`y_harness::TemporalDriver`] remains a bounded task-free Core primitive.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::{
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout},
};
use y_harness::{
    AuthorityContext, CancellationToken, HarnessError, MAX_TEMPORAL_SCAN_LIMIT,
    TemporalAttemptOutcome, TemporalDriver, TemporalTickCursor, TemporalTickRequest,
};

const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const MIN_POLL_INTERVAL_MS: u64 = 100;
const MAX_POLL_INTERVAL_MS: u64 = 86_400_000;
const DEFAULT_SCAN_LIMIT: usize = 64;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DIAGNOSTIC_CHARS: usize = 1_024;

/// Explicit opt-in reference-service polling policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServiceTemporalConfig {
    /// Polling cadence. Missed ticks are skipped rather than replayed.
    #[serde(default = "default_poll_interval_ms")]
    pub(super) poll_interval_ms: u64,
    /// Maximum authoritative identities visited per source and tick.
    #[serde(default = "default_scan_limit")]
    pub(super) scan_limit: usize,
}

impl ServiceTemporalConfig {
    pub(super) fn validate(&self) -> Result<(), HarnessError> {
        if !(MIN_POLL_INTERVAL_MS..=MAX_POLL_INTERVAL_MS).contains(&self.poll_interval_ms) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "temporal.poll_interval_ms must be {MIN_POLL_INTERVAL_MS}-{MAX_POLL_INTERVAL_MS}"
            )));
        }
        if !(1..=MAX_TEMPORAL_SCAN_LIMIT).contains(&self.scan_limit) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "temporal.scan_limit must be 1-{MAX_TEMPORAL_SCAN_LIMIT}"
            )));
        }
        Ok(())
    }
}

/// Joinable ownership of one process-local maintenance loop.
pub(super) struct TemporalServiceHandle {
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl TemporalServiceHandle {
    /// Stops cadence admission and waits a bounded time for an in-flight tick.
    pub(super) async fn shutdown(self) -> Result<(), HarnessError> {
        self.cancellation.cancel();
        let mut task = self.task;
        match timeout(SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(HarnessError::Temporal(
                "reference-service maintenance task terminated unexpectedly".to_owned(),
            )),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(HarnessError::Temporal(
                    "reference-service maintenance shutdown timed out".to_owned(),
                ))
            }
        }
    }
}

/// Starts the explicitly configured reference-service lifecycle.
pub(super) fn start(
    driver: TemporalDriver,
    authority: AuthorityContext,
    config: ServiceTemporalConfig,
) -> Result<TemporalServiceHandle, HarnessError> {
    config.validate()?;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        run(driver, authority, config, task_cancellation).await;
    });
    Ok(TemporalServiceHandle { cancellation, task })
}

async fn run(
    driver: TemporalDriver,
    authority: AuthorityContext,
    config: ServiceTemporalConfig,
    cancellation: CancellationToken,
) {
    let mut ticker = interval(Duration::from_millis(config.poll_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut cursor = TemporalTickCursor::default();
    let mut degraded = false;

    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            _ = ticker.tick() => {
                let result = match trusted_unix_ms() {
                    Ok(at_ms) => driver
                        .tick_as(
                            TemporalTickRequest {
                                at_ms,
                                scan_limit: config.scan_limit,
                                cursor: cursor.clone(),
                            },
                            &authority,
                        )
                        .await,
                    Err(error) => Err(error),
                };
                match result {
                    Ok(report) => {
                        cursor = report.next_cursor;
                        let failures = report
                            .attempts
                            .iter()
                            .filter(|attempt| {
                                matches!(attempt.outcome, TemporalAttemptOutcome::Failed)
                            })
                            .count();
                        if failures == 0 {
                            if degraded {
                                eprintln!("Y-Harness temporal maintenance recovered");
                                degraded = false;
                            }
                        } else if !degraded {
                            eprintln!(
                                "Y-Harness temporal maintenance degraded: \
                                 {failures} advancement attempt(s) failed"
                            );
                            degraded = true;
                        }
                    }
                    Err(error) => {
                        if !degraded {
                            eprintln!(
                                "Y-Harness temporal maintenance degraded: {}",
                                bounded_diagnostic(&error.to_string())
                            );
                            degraded = true;
                        }
                    }
                }
            }
        }
    }
}

fn trusted_unix_ms() -> Result<u64, HarnessError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| HarnessError::Temporal("service clock precedes the Unix epoch".to_owned()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| HarnessError::Temporal("service clock exceeds Unix milliseconds".to_owned()))
}

fn bounded_diagnostic(message: &str) -> String {
    let mut chars = message.chars().map(|character| {
        if character.is_control() {
            ' '
        } else {
            character
        }
    });
    let mut bounded = chars
        .by_ref()
        .take(MAX_DIAGNOSTIC_CHARS)
        .collect::<String>();
    if chars.next().is_some() {
        bounded.pop();
        bounded.push('…');
    }
    bounded
}

const fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

const fn default_scan_limit() -> usize {
    DEFAULT_SCAN_LIMIT
}

#[cfg(test)]
mod tests {
    use super::{MAX_DIAGNOSTIC_CHARS, ServiceTemporalConfig, bounded_diagnostic, trusted_unix_ms};
    use y_harness::MAX_TEMPORAL_SCAN_LIMIT;

    #[test]
    fn configuration_is_strict_bounded_and_defaulted() {
        let defaulted: ServiceTemporalConfig =
            serde_json::from_str("{}").expect("default Temporal config");
        assert_eq!(defaulted.poll_interval_ms, 1_000);
        assert_eq!(defaulted.scan_limit, 64);
        defaulted.validate().expect("valid defaults");

        for invalid in [
            ServiceTemporalConfig {
                poll_interval_ms: 99,
                scan_limit: 1,
            },
            ServiceTemporalConfig {
                poll_interval_ms: 100,
                scan_limit: 0,
            },
            ServiceTemporalConfig {
                poll_interval_ms: 100,
                scan_limit: MAX_TEMPORAL_SCAN_LIMIT + 1,
            },
        ] {
            assert!(invalid.validate().is_err());
        }
        assert!(serde_json::from_str::<ServiceTemporalConfig>(r#"{"extra":true}"#).is_err());
    }

    #[test]
    fn diagnostics_are_single_line_and_bounded() {
        let diagnostic = bounded_diagnostic(&format!("{}\nsecret", "a".repeat(2_048)));
        assert!(!diagnostic.contains('\n'));
        assert_eq!(diagnostic.chars().count(), MAX_DIAGNOSTIC_CHARS);
        assert!(diagnostic.ends_with('…'));
        assert!(trusted_unix_ms().expect("current Unix time") > 0);
    }
}
