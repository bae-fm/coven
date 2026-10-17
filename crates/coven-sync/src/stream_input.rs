//! Bounded channels between a committed database reader and a journal writer.

use crate::SyncError;
use std::io::{self, Read};
use tokio::sync::mpsc;

pub(crate) struct StreamInput {
    receiver: mpsc::Receiver<Result<Vec<u8>, SyncError>>,
    chunk: io::Cursor<Vec<u8>>,
}
impl StreamInput {
    pub(crate) fn channel() -> (mpsc::Sender<Result<Vec<u8>, SyncError>>, Self) {
        let (sender, receiver) = mpsc::channel(1);
        (
            sender,
            Self {
                receiver,
                chunk: io::Cursor::new(Vec::new()),
            },
        )
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
