//! Bounded plaintext channels from asynchronous storage to database readers.

use crate::{write_object::PartSink, SyncError};
use coven_database::{DownloadedPartStream, DownloadedWriteStream};
use coven_format::write_stream::WriteHeaderFrame;
use std::io::{self, Read};
use tokio::sync::mpsc;

pub(crate) struct StreamInput {
    receiver: mpsc::Receiver<Vec<u8>>,
    chunk: io::Cursor<Vec<u8>>,
}

impl StreamInput {
    pub(crate) fn channel() -> (mpsc::Sender<Vec<u8>>, Self) {
        let (send, receiver) = mpsc::channel(1);
        (
            send,
            Self {
                receiver,
                chunk: io::Cursor::new(Vec::new()),
            },
        )
    }
}

impl Read for StreamInput {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.chunk.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            match self.receiver.blocking_recv() {
                Some(bytes) => self.chunk = io::Cursor::new(bytes),
                None => return Ok(0),
            }
        }
    }
}

pub(crate) struct ChannelParts(Vec<Option<mpsc::Sender<Vec<u8>>>>);

impl ChannelParts {
    pub(crate) fn new(
        header: WriteHeaderFrame,
        opens: &[bool],
    ) -> (Self, DownloadedWriteStream<StreamInput>) {
        let (senders, parts) = opens
            .iter()
            .map(|opened| {
                if *opened {
                    let (send, input) = StreamInput::channel();
                    (Some(send), DownloadedPartStream::Opened(input))
                } else {
                    (None, DownloadedPartStream::Skipped)
                }
            })
            .unzip();
        (Self(senders), DownloadedWriteStream { header, parts })
    }
}

impl PartSink for ChannelParts {
    fn opens(&self, part: usize) -> bool {
        self.0[part].is_some()
    }
    async fn chunk(&mut self, part: usize, bytes: Vec<u8>) -> Result<(), SyncError> {
        // An early database refusal closes its receiver. The caller awaits
        // that result and the transfer's authentication independently.
        let _ = self.0[part]
            .as_ref()
            .expect("opened part")
            .send(bytes)
            .await;
        Ok(())
    }
    async fn end(&mut self, part: usize) -> Result<(), SyncError> {
        self.0[part] = None;
        Ok(())
    }
}

/// Feed the same opened bytes to application/staging and file-reference checks.
pub(crate) struct WithReferences<S> {
    pub(crate) primary: S,
    pub(crate) references: ChannelParts,
}

impl<S: PartSink> PartSink for WithReferences<S> {
    fn opens(&self, part: usize) -> bool {
        self.primary.opens(part) || self.references.opens(part)
    }
    async fn chunk(&mut self, part: usize, bytes: Vec<u8>) -> Result<(), SyncError> {
        if self.references.opens(part) {
            self.references.chunk(part, bytes.clone()).await?;
        }
        if self.primary.opens(part) {
            self.primary.chunk(part, bytes).await?;
        }
        Ok(())
    }
    async fn end(&mut self, part: usize) -> Result<(), SyncError> {
        self.references.end(part).await?;
        self.primary.end(part).await
    }
}
