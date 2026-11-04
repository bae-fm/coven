//! The app bytes one staging writes to coven's copies of them, each file
//! given its new id (§17.2).

use coven_foundation::id_source::{FileId, IdSource};
use std::pin::Pin;
use tokio::io::AsyncRead;

/// App bytes to stage, before the write that puts them in rows; a failure
/// discards all of them. Each staged file gets a new id from the store's id
/// source, which the batch borrows while the app stages; the write's SQL
/// puts the id in the rows that reference the file.
pub struct WriteBatch<'a> {
    ids: &'a dyn IdSource,
    files: Vec<BatchFile>,
}

/// A new file over an existing file's bytes: its new id, and the file whose
/// bytes, namespace, place and key it takes (§17.2).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExistingFile {
    pub(crate) id: FileId,
    pub(crate) from: FileId,
}

/// One staged file: its id, its namespace, its bytes, and where it is to be.
pub(crate) struct BatchFile {
    pub(crate) id: FileId,
    pub(crate) namespace: String,
    pub(crate) source: FileSource,
    pub(crate) place: Place,
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
    /// switching it to the cloud pins it on this device, which makes its
    /// cache copy from coven's copy of app bytes, and downloads a user's
    /// original's from storage, never reading the original (§17.8).
    CloudKeptHere,
}

impl<'a> WriteBatch<'a> {
    pub(crate) fn new(ids: &'a dyn IdSource) -> Self {
        Self { ids, files: Vec::new() }
    }

    /// The files staged, for the staging that copies them in.
    pub(crate) fn into_files(self) -> Vec<BatchFile> {
        self.files
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
        let id = FileId(self.ids.new_id());
        self.files.push(BatchFile {
            id,
            namespace: namespace.to_owned(),
            source: bytes.into(),
            place,
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
