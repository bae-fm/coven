//! The moves of this device's files to the cloud that a write records
//! (§17.3): which files each moves, checked when the app names them, and
//! the uploads queued for them once the write applied.

use super::{FileWrite, StagedSource};
use crate::{
    file_hashes::FileHashes,
    file_location::{FileLocation, Place},
    file_moves::{self, MoveState},
    file_table::Record,
    DbError, MoveId, PreparedUserFile, Provenance,
};
use coven_crypto::ContentHash;
use coven_foundation::{files::FileName, id_source::FileId};
use std::collections::BTreeSet;

/// A file of this device, on it only, to move to the cloud (§17.3, E3).
#[derive(Debug)]
pub enum MoveFile {
    /// A user's original, read whole once again before the write: its
    /// upload checks each chunk against what this read found.
    User(FileId, PreparedUserFile),
    /// As `User`, and kept whole here too once stored: the write switching
    /// it to the cloud pins it on this device, which downloads it from
    /// storage. The original is never read for the cache (§17.8).
    UserKeptHere(FileId, PreparedUserFile),
    /// An app file, uploaded from coven's copy.
    App(FileId),
    /// As `App`, and kept whole here too once stored: the cache copy is
    /// made from coven's copy (§17.8).
    AppKeptHere(FileId),
}

/// A move this write recorded, with the uploads it queues once applied.
pub(super) struct Moving {
    id: MoveId,
    uploads: Vec<MoveUpload>,
}

/// One upload of a move: the files over the same bytes it stores, each with
/// whether this device keeps it whole once stored.
struct MoveUpload {
    files: Vec<(FileId, bool)>,
    size: u64,
    source: StagedSource,
    hashes: FileHashes,
}

impl FileWrite<'_> {
    /// Record a move of these files to the cloud (§17.3). Each must be on
    /// this device only, with no upload queued. A user's original must
    /// still have its recorded path, size and modification time. The files
    /// switch to the cloud together, in one write once every one is stored.
    pub(crate) fn move_to_cloud(&self, files: Vec<MoveFile>) -> Result<MoveId, DbError> {
        let db = self.database;
        let mut uploads: Vec<MoveUpload> = Vec::new();
        let mut named = BTreeSet::new();
        for file in files {
            let (id, keep) = match &file {
                MoveFile::User(id, _) | MoveFile::App(id) => (*id, false),
                MoveFile::UserKeptHere(id, _) | MoveFile::AppKeptHere(id) => (*id, true),
            };
            if !named.insert(id) {
                continue;
            }
            let record = Record::read(db, id)?.ok_or(DbError::FileAbsent { file: id })?;
            match record.place {
                Place::DeviceOnly(device) if device == self.device => {}
                Place::DeviceOnly(_) => return Err(DbError::NotThisDevice { file: id }),
                Place::Cloud(_) => return Err(DbError::NotDeviceOnlyHere { file: id }),
            }
            let moving = self
                .moving
                .borrow()
                .iter()
                .flat_map(|moving| &moving.uploads)
                .any(|upload| upload.files.iter().any(|(file, _)| *file == id));
            if moving || crate::file_queue::upload_of(db, id)?.is_some() {
                return Err(DbError::NotDeviceOnlyHere { file: id });
            }
            let upload = match file {
                MoveFile::User(_, prepared) | MoveFile::UserKeptHere(_, prepared) => {
                    let recorded =
                        crate::user_file::read(db, id)?.ok_or_else(|| DbError::NamespaceKind {
                            namespace: record.namespace.clone(),
                            kind: Provenance::AppProvided,
                        })?;
                    let observed = &prepared.observed;
                    if recorded.path != observed.path()
                        || recorded.size != observed.size()
                        || recorded.modified_at != observed.modified_at()
                    {
                        return Err(DbError::UserFileChanged {
                            path: recorded.path,
                        });
                    }
                    MoveUpload {
                        files: vec![(id, keep)],
                        size: observed.size(),
                        source: StagedSource::Original(prepared.observed),
                        hashes: prepared.hashes,
                    }
                }
                MoveFile::App(_) | MoveFile::AppKeptHere(_) => {
                    let (name, hashes) = copy(db, id)?.ok_or_else(|| DbError::NamespaceKind {
                        namespace: record.namespace.clone(),
                        kind: Provenance::UserProvided,
                    })?;
                    MoveUpload {
                        files: vec![(id, keep)],
                        size: record.size,
                        source: StagedSource::Copy(name),
                        hashes,
                    }
                }
            };
            // Files over the same bytes here share one upload.
            match uploads
                .iter_mut()
                .find(|queued| same_source(&queued.source, &upload.source))
            {
                Some(queued) => queued.files.push((id, keep)),
                None => uploads.push(upload),
            }
        }
        let id = file_moves::create(db)?;
        self.moving.borrow_mut().push(Moving { id, uploads });
        Ok(id)
    }

    /// Cancel a move: every file stays on this device only, and its record
    /// ends (§17.3). Cancelling a cancelled move does nothing; a finished
    /// move, whose files are in the cloud, can't be cancelled, nor can a
    /// bound file's, whose held write would go out naming a file no other
    /// device can read (§17.4).
    pub(crate) fn cancel_move(&self, id: MoveId) -> Result<(), DbError> {
        let db = self.database;
        match file_moves::state(db, id)? {
            None => Err(DbError::MoveUnknown { id }),
            Some(MoveState::Finished) => Err(DbError::MoveFinished { id }),
            Some(MoveState::Cancelled) => Ok(()),
            Some(MoveState::Moving | MoveState::Failed) if file_moves::bound(db, id)? => {
                Err(DbError::MoveBound { id })
            }
            Some(MoveState::Moving | MoveState::Failed) => {
                self.moving.borrow_mut().retain(|moving| moving.id != id);
                file_moves::end_unswitched(db, id, MoveState::Cancelled)?;
                file_moves::clear_pending(db, id)
            }
        }
    }

    /// Queue the uploads of each move this write recorded, for the files
    /// still on this device only once it applied. A move left with none
    /// has nothing to switch, and ends.
    pub(super) fn queue_moves(&self, now: std::time::SystemTime) -> Result<(), DbError> {
        let db = self.database;
        for moving in self.moving.take() {
            let mut queued = false;
            for upload in moving.uploads {
                let mut files = Vec::new();
                for (file, keep) in upload.files {
                    if Record::read(db, file)?
                        .is_some_and(|record| record.place == Place::DeviceOnly(self.device))
                    {
                        files.push((file, keep));
                    }
                }
                if files.is_empty() {
                    continue;
                }
                let location = FileLocation::new(self.device, self.ids)?;
                super::uploads::enqueue(
                    db,
                    super::uploads::Queued {
                        files: &files,
                        size: upload.size,
                        source: &upload.source,
                        hashes: &upload.hashes,
                        of: moving.id,
                        holds: None,
                    },
                    &location,
                    now,
                )?;
                queued = true;
            }
            if !queued {
                file_moves::finish(db, moving.id)?;
            }
        }
        Ok(())
    }
}

/// Whether two uploads would send the same bytes here.
fn same_source(a: &StagedSource, b: &StagedSource) -> bool {
    match (a, b) {
        (StagedSource::Original(a), StagedSource::Original(b)) => a.path() == b.path(),
        (StagedSource::Copy(a), StagedSource::Copy(b)) => a == b,
        _ => false,
    }
}

/// Coven's copy of an app file, with the hashes its first read recorded.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn copy(
    db: &crate::sqlite::DatabaseConnection,
    file: FileId,
) -> Result<Option<(FileName, FileHashes)>, DbError> {
    let Some((path, content)) = db
        .query(
            "SELECT path,hash FROM _coven_device_files WHERE file=?1",
            [file],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )?
        .pop()
    else {
        return Ok(None);
    };
    let decode = |bytes: Vec<u8>| -> Result<ContentHash, DbError> {
        Ok(ContentHash::from_bytes(
            bytes.try_into().map_err(|_| DbError::DamagedDatabase)?,
        ))
    };
    let chunks = db
        .query(
            "SELECT hash FROM _coven_copy_chunks WHERE path=?1 ORDER BY chunk",
            [&path],
            |r| r.get::<_, Vec<u8>>(0),
        )?
        .into_iter()
        .map(decode)
        .collect::<Result<Vec<_>, _>>()?;
    let content = decode(content)?;
    Ok(Some((
        FileName::new(path).map_err(|_| DbError::DamagedDatabase)?,
        FileHashes::from_parts(content, chunks),
    )))
}
