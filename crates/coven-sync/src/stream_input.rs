//! Bounded channels between a committed database reader and a journal writer.

use crate::SyncError;
use std::io::{self, Read};
use tokio::sync::mpsc;

pub(crate) struct StreamInput {
    receiver: mpsc::Receiver<Result<Vec<u8>, SyncError>>,
    chunk: io::Cursor<Vec<u8>>,
}
impl StreamInput {
    /// A stream and where its reader sends the bytes, one chunk at a time.
    pub(crate) fn channel() -> (StreamSender, Self) {
        let (sender, receiver) = mpsc::channel(1);
        (StreamSender(sender), Self { receiver, chunk: io::Cursor::new(Vec::new()) })
    }
}

/// Where the reader of a stream sends its bytes, or the error that ends it.
#[derive(Clone)]
pub(crate) struct StreamSender(mpsc::Sender<Result<Vec<u8>, SyncError>>);

type Sent = Result<(), mpsc::error::SendError<Result<Vec<u8>, SyncError>>>;

impl StreamSender {
    /// Send the next chunk, or the error, from a blocking task; refused once
    /// the stream is dropped.
    pub(crate) fn blocking_send(&self, next: Result<Vec<u8>, SyncError>) -> Sent {
        self.0.blocking_send(next)
    }

    /// Send the next chunk, or the error; refused once the stream is dropped.
    pub(crate) async fn send(&self, next: Result<Vec<u8>, SyncError>) -> Sent {
        self.0.send(next).await
    }
}
impl Read for StreamInput {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let length = self.chunk.read(output)?;
            if length > 0 {
                return Ok(length);
            }
            match self.receiver.blocking_recv() {
                Some(Ok(bytes)) => self.chunk = io::Cursor::new(bytes),
                Some(Err(error)) => return Err(io::Error::other(error)),
                None => return Ok(0),
            }
        }
    }
}
