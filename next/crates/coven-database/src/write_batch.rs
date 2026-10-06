//! App-provided bytes handed over for one write.

use std::pin::Pin;
use tokio::io::AsyncRead;

/// Files to attach in one transaction; errors discard all its supplied bytes.
pub struct WriteBatch {
    pub(crate) files: Vec<(String, String, FileSource)>,
}

impl WriteBatch {
    pub(crate) fn new() -> Self {
        Self { files: Vec::new() }
    }

    /// Hand over bytes under the declared namespace and file id. Streams are
    /// consumed once and never buffered as a whole file. Coven fills each
    /// attached row's size from those bytes, together with its hash and location.
    pub fn put_file(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<FileSource>,
    ) {
        self.files.push((namespace.into(), id.into(), bytes.into()));
    }
}

/// An owned buffer or a reader whose stream coven consumes exactly once.
pub enum FileSource {
    /// Bytes already held by the app.
    Bytes(Vec<u8>),
    /// A stream read in bounded chunks.
    Stream(Pin<Box<dyn AsyncRead + Send>>),
}

impl From<Vec<u8>> for FileSource {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bytes(bytes)
    }
}
