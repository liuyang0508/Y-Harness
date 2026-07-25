//! Shared panic and settlement isolation for executable capability Futures.

use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};

use crate::CancellationToken;

pub(crate) fn isolate_future<F>(
    operation: impl FnOnce() -> F,
    settlement_cancellation: Option<CancellationToken>,
) -> Result<IsolatedFuture<F>, ()> {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(future) => Ok(IsolatedFuture {
            future: Some(Box::pin(future)),
            settlement_cancellation,
        }),
        Err(_) => {
            if let Some(cancellation) = settlement_cancellation {
                cancellation.cancel();
            }
            Err(())
        }
    }
}

pub(crate) struct IsolatedFuture<F> {
    future: Option<Pin<Box<F>>>,
    settlement_cancellation: Option<CancellationToken>,
}

impl<F> IsolatedFuture<F> {
    fn settle(&mut self) -> bool {
        if let Some(cancellation) = self.settlement_cancellation.take() {
            cancellation.cancel();
        }
        catch_unwind(AssertUnwindSafe(|| drop(self.future.take()))).is_err()
    }
}

impl<F: Future> Future for IsolatedFuture<F> {
    type Output = Result<F::Output, ()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(future) = this.future.as_mut() else {
            return Poll::Ready(Err(()));
        };
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(Poll::Ready(output)) => {
                if this.settle() {
                    Poll::Ready(Err(()))
                } else {
                    Poll::Ready(Ok(output))
                }
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => {
                this.settle();
                Poll::Ready(Err(()))
            }
        }
    }
}

impl<F> Drop for IsolatedFuture<F> {
    fn drop(&mut self) {
        self.settle();
    }
}
