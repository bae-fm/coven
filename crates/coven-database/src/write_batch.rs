//! The files one write stages, each given its new id (§17.2).

use crate::PreparedUserFile;
use coven_foundation::id_source::{FileId, IdSource};
use std::pin::Pin;
use tokio::io::AsyncRead;

/// Files to stage in one write; errors discard all its supplied bytes. Each
/// staged file gets a new id from the store's id source, which the batch
/// borrows while the app stages; the write's SQL puts the id in the rows that
/// reference the file.
pub struct WriteBatch<'a> {
    ids: &'a dyn IdSource,
    pub(crate) files: Vec<BatchFile>,
}

/// One staged file: its id, its namespace, and its bytes.
pub(crate) struct BatchFile {
    pub(crate) id: FileId,
    pub(crate) namespace: String,
    pub(crate) source: BatchSource,
}

/// The bytes of a staged file: the app's, or the user's original.
pub(crate) enum BatchSource {
    App(FileSource),
    User(PreparedUserFile),
}

impl<'a> WriteBatch<'a> {
    pub(crate) fn new(ids: &'a dyn IdSource) -> Self {
        Self {
            ids,
            files: Vec::new(),
        }
    }

    /// Stage app bytes as a new file in an app namespace, for the cloud. A
    /// stream is consumed once and never buffered whole. Returns the file's
    /// id for a row to reference.
    pub fn put_file(&mut self, namespace: &str, bytes: impl Into<FileSource>) -> FileId {
        self.push(namespace, BatchSource::App(bytes.into()))
    }

    /// Stage a user's original, read once already, as a new file in a user
    /// namespace, for the cloud. Returns the file's id for a row to reference.
    pub fn put_user_file(&mut self, namespace: &str, file: PreparedUserFile) -> FileId {
        self.push(namespace, BatchSource::User(file))
    }

    fn push(&mut self, namespace: &str, source: BatchSource) -> FileId {
        let id = FileId(self.ids.new_id());
        self.files.push(BatchFile {
            id,
            namespace: namespace.to_owned(),
            source,
        });
        id
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
