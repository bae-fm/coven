//! The files one write stages, each given its new id (§17.2).

use crate::UserSource;
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

/// The bytes of a staged file: the app's, with where its file is to be, or
/// the user's original.
pub(crate) enum BatchSource {
    App(FileSource, Place),
    User(UserSource),
}

/// Where a staged file is to be: on this device only, or uploaded to the
/// cloud (§17.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    /// On this device only, with no upload.
    DeviceOnly,
    /// Uploaded; on this device only until its upload is stored.
    Cloud,
    /// Uploaded, as `Cloud`, and kept whole here too once stored: the write
    /// switching it to the cloud pins it on this device (§17.7).
    CloudKeptHere,
}

impl<'a> WriteBatch<'a> {
    pub(crate) fn new(ids: &'a dyn IdSource) -> Self {
        Self {
            ids,
            files: Vec::new(),
        }
    }

    /// Stage app bytes as a new file in an app namespace, kept in coven's
    /// own copy: on this device only, or for the cloud. A stream is consumed
    /// once and never buffered whole. Returns the file's id for a row to
    /// reference.
    pub fn put_file(
        &mut self,
        namespace: &str,
        bytes: impl Into<FileSource>,
        place: Place,
    ) -> FileId {
        self.push(namespace, BatchSource::App(bytes.into(), place))
    }

    /// Stage a user's original as a new file in a user namespace: observed,
    /// on this device only, or read once already, for the cloud. Returns the
    /// file's id for a row to reference.
    pub fn put_user_file(&mut self, namespace: &str, source: UserSource) -> FileId {
        self.push(namespace, BatchSource::User(source))
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
