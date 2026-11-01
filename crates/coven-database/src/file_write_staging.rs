//! The files a write's steps stage (§17.2): app bytes `stage_files` already
//! wrote, users' originals, and new files over existing files' bytes. Each
//! gets its record once the write's rows name it.

use super::{FileWrite, StagedFile, StagedSource};
use crate::{write_batch::ExistingFile, DbError, Place, PreparedUserFile, Provenance};
use coven_foundation::{files::FileName, id_source::FileId};

impl FileWrite<'_> {
    /// Bring files `stage_files` staged in this store into this write, and
    /// return what its batch returned. Each name must still be reserved
    /// here: files staged by another open store, or by this one before it
    /// closed, aren't (`NotStagedHere`), and keep their names for their own
    /// store to release.
    pub(crate) fn put_staged<T>(&self, staged: crate::StagedFiles<T>) -> Result<T, DbError> {
        {
            let reserved = self.reservations.lock().expect("file staging lock poisoned");
            if staged.names().iter().any(|name| !reserved.contains(name)) {
                return Err(DbError::NotStagedHere);
            }
        }
        let (files, names, value) = staged.take();
        self.brought.borrow_mut().extend(names);
        self.staged.borrow_mut().extend(files);
        Ok(value)
    }

    /// The reserved names this write brought in, which go free once its
    /// transaction has decided.
    pub(crate) fn take_brought(&self) -> Vec<FileName> {
        std::mem::take(&mut self.brought.borrow_mut())
    }

    /// Stage a user's original, read whole once already, as a new file in a
    /// user namespace, and return its new id.
    pub(crate) fn put_user_file(
        &self,
        namespace: &str,
        original: PreparedUserFile,
        place: Place,
    ) -> Result<FileId, DbError> {
        let declared = self
            .schema
            .namespace(namespace)
            .ok_or_else(|| DbError::UndeclaredNamespace { namespace: namespace.to_owned() })?;
        if declared.kind != Provenance::UserProvided {
            return Err(DbError::NamespaceKind {
                kind: declared.kind.clone(),
                namespace: namespace.to_owned(),
            });
        }
        let id = FileId(self.ids.new_id());
        self.staged.borrow_mut().push(StagedFile {
            id,
            namespace: namespace.to_owned(),
            size: original.original.size,
            source: StagedSource::Original(original.original),
            hashes: original.hashes,
            place,
        });
        Ok(id)
    }

    /// Stage a new file over an existing file's bytes, and return its new id.
    pub(crate) fn existing_file(&self, from: FileId) -> FileId {
        let id = FileId(self.ids.new_id());
        self.existing.borrow_mut().push(ExistingFile { id, from });
        id
    }
}
