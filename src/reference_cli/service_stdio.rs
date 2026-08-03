//! Cancellation-safe stdin bridge for the reference service host.
//!
//! Tokio's portable stdin wrapper delegates reads to its blocking pool. A read
//! waiting on an open pipe cannot be cancelled, so Runtime shutdown can hang
//! after SIGTERM. This bridge confines that unavoidable wait to one detached OS
//! thread and exposes only a bounded asynchronous channel to the Runtime.

use std::{
    io::{self, Read},
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::mpsc,
};

const READ_CHUNK_BYTES: usize = 8 * 1_024;
const BUFFERED_CHUNKS: usize = 4;

/// Bounded asynchronous view of a detached blocking stdin reader.
pub(super) struct ServiceStdin {
    receiver: mpsc::Receiver<io::Result<Vec<u8>>>,
    current: Vec<u8>,
    offset: usize,
}

impl ServiceStdin {
    /// Starts the sole reference-host stdin reader.
    pub(super) fn spawn() -> io::Result<Self> {
        let (sender, receiver) = mpsc::channel(BUFFERED_CHUNKS);
        thread::Builder::new()
            .name("yh-service-stdin".to_owned())
            .spawn(move || {
                let stdin = io::stdin();
                pump(&mut stdin.lock(), &sender);
            })?;
        Ok(Self {
            receiver,
            current: Vec::new(),
            offset: 0,
        })
    }
}

impl AsyncRead for ServiceStdin {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if this.offset < this.current.len() {
                let available = &this.current[this.offset..];
                let copied = available.len().min(output.remaining());
                output.put_slice(&available[..copied]);
                this.offset += copied;
                return Poll::Ready(Ok(()));
            }
            match this.receiver.poll_recv(context) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.current = chunk;
                    this.offset = 0;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn pump(input: &mut impl Read, sender: &mpsc::Sender<io::Result<Vec<u8>>>) {
    loop {
        let mut chunk = vec![0_u8; READ_CHUNK_BYTES];
        match input.read(&mut chunk) {
            Ok(0) => return,
            Ok(read) => {
                chunk.truncate(read);
                if sender.blocking_send(Ok(chunk)).is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = sender.blocking_send(Err(error));
                return;
            }
        }
    }
}
