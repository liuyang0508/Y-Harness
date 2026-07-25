//! Cooperative cancellation shared by runtime and executable capabilities.

use tokio::sync::watch;

/// Cloneable, race-free cancellation signal for one bounded execution.
///
/// Cancellation is monotonic: once requested, every current and future waiter
/// observes it. Dropping a caller's runtime future is not equivalent to calling
/// [`cancel`](Self::cancel); abandoned turns are recovered as interrupted.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    sender: watch::Sender<bool>,
}

impl CancellationToken {
    /// Creates an active token.
    #[must_use]
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self { sender }
    }

    /// Requests cancellation and wakes every registered capability waiter.
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    /// Returns whether cancellation has already been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    /// Waits until this token is cancelled.
    ///
    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::CancellationToken;

    #[tokio::test]
    async fn cancellation_is_monotonic_for_current_and_future_waiters() {
        let token = CancellationToken::new();
        let waiter = token.clone();
        let task = tokio::spawn(async move {
            waiter.cancelled().await;
        });

        token.cancel();
        task.await.expect("waiter");
        token.cancelled().await;
        assert!(token.is_cancelled());
    }
}
