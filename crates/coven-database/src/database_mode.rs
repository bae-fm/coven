//! Which handle opened the store: its one writer, or a read-only handle
//! (§4, E1). Both read the store through the same connections; only the
//! writer writes, transfers files and keeps their records.

use super::file_database::FileDatabase;

mod sealed {
    use crate::database::{file_database::FileDatabase, DatabaseAccess};

    /// What each mode holds. Sealed: a store opens only as one of these two.
    pub trait Sealed: Sized {
        /// The open store, behind the handle's lock until it closes.
        type Open: Send + Sync + 'static;
        /// The lock the open store holds on its directory.
        type Lock: Send + Sync + 'static;
        /// What only a writer handle holds beside the open store.
        type WriterParts: Clone + Send + Sync + 'static;

        /// The connections every mode reads through.
        fn access(open: &Self::Open) -> &DatabaseAccess<Self::Lock>;

        /// The file owner that keeps file records: the writer's own, and
        /// none for a read-only handle.
        fn records(files: &FileDatabase<Self>) -> Option<&FileDatabase<super::Writer>>
        where
            Self: super::Mode;
    }
}

/// The mode a store's handle opened in: [`Writer`] or [`ReadOnly`].
pub trait Mode: sealed::Sealed + Send + Sync + 'static {}

/// The store's one writer: it writes, migrates, transfers files and keeps
/// their records (§4).
pub enum Writer {}

/// A read-only handle: it reads synced rows and keeps its own file cache,
/// and nothing else (E1).
pub enum ReadOnly {}

impl Mode for Writer {}
impl Mode for ReadOnly {}

impl sealed::Sealed for Writer {
    type Open = super::DatabaseInner;
    type Lock = coven_foundation::files::StoreLock;
    type WriterParts = super::WriterParts;

    fn access(open: &super::DatabaseInner) -> &super::DatabaseAccess<Self::Lock> {
        &open.access
    }

    fn records(files: &FileDatabase<Self>) -> Option<&FileDatabase<Writer>> {
        Some(files)
    }
}

impl sealed::Sealed for ReadOnly {
    type Open = super::DatabaseAccess<Self::Lock>;
    type Lock = coven_foundation::files::StoreReadLock;
    type WriterParts = ();

    fn access(open: &Self::Open) -> &Self::Open {
        open
    }

    fn records(_: &FileDatabase<Self>) -> Option<&FileDatabase<Writer>> {
        None
    }
}
