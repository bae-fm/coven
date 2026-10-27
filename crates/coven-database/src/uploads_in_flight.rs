//! The files this process is sending now (§4, §17.7). Nothing about them is
//! stored: an attempt ends when its task does, a crash included, so a
//! reopened store starts with none. App SQL reads them through coven's SQL
//! functions, as `coven_uploads.sending` and the `coven_sending` view, and a
//! live query that reads either runs again as an upload starts or ends.

use crate::observation::{CommitObserver, RowChange};
use coven_foundation::id_source::FileId;
use rusqlite::functions::FunctionFlags;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

/// What a start or end changes, as live queries read it: the `sending`
/// column of `coven_uploads` and the files `coven_sending` lists.
const READ_AS: [(&str, &str); 2] = [("coven_uploads", "sending"), ("coven_sending", "")];

/// Coven's functions that read the files being sent, which app SQL reaches
/// only through coven's views.
pub(crate) const FUNCTIONS: [&str; 2] = ["_coven_sending", "_coven_sending_files"];

/// The files this process is sending now. The upload worker adds each
/// attempt as it starts and removes it as it ends, cancelled included. One
/// shared owner per open store: clones share it.
#[derive(Clone)]
pub struct UploadsInFlight {
    shared: Arc<InFlight>,
}

struct InFlight {
    files: Mutex<BTreeSet<FileId>>,
    /// Told of each start and end, as of a commit that changed what the
    /// live queries reading `READ_AS` read.
    observer: CommitObserver,
}

impl UploadsInFlight {
    pub(crate) fn new(observer: CommitObserver) -> Self {
        Self {
            shared: Arc::new(InFlight {
                files: Mutex::new(BTreeSet::new()),
                observer,
            }),
        }
    }

    fn files(&self) -> std::sync::MutexGuard<'_, BTreeSet<FileId>> {
        self.shared
            .files
            .lock()
            .expect("uploads in flight poisoned")
    }

    /// The files being sent now.
    pub fn current(&self) -> BTreeSet<FileId> {
        self.files().clone()
    }

    /// Mark the file as being sent until the returned guard drops.
    pub fn start(&self, file: FileId) -> Sending {
        let started = self.files().insert(file);
        if started {
            self.changed();
        }
        Sending {
            owner: self.clone(),
            file,
        }
    }

    /// Tell live queries after the set changed, so a query that ran before
    /// the change runs again, and one that runs after it reads the new set.
    fn changed(&self) {
        self.shared.observer.commit(
            READ_AS
                .iter()
                .map(|(table, column)| RowChange {
                    table: (*table).into(),
                    column: (*column).into(),
                    keys: None,
                })
                .collect(),
            None,
        );
    }

    /// Register `_coven_sending(file)`, whether this process is sending the
    /// file with these 16 id bytes, and `_coven_sending_files()`, the files
    /// it is sending as a JSON array of their ids' hex, on a connection
    /// to the store's database. Neither is deterministic; both are safe in
    /// coven's views.
    pub(crate) fn register(&self, connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;
        let owner = self.clone();
        connection.create_scalar_function("_coven_sending", 1, flags, move |ctx| {
            // Anything but a file id's 16 bytes fails the statement.
            let file = uuid::Uuid::from_slice(&ctx.get::<Vec<u8>>(0)?)
                .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
            Ok(owner.files().contains(&FileId(file)))
        })?;
        let owner = self.clone();
        connection.create_scalar_function("_coven_sending_files", 0, flags, move |_| {
            let files: Vec<String> = owner
                .files()
                .iter()
                .map(|file| file.0.simple().to_string())
                .collect();
            serde_json::to_string(&files)
                .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))
        })
    }
}

/// One file being sent; dropping it, also when its attempt is cancelled,
/// marks the file as no longer sent.
pub struct Sending {
    owner: UploadsInFlight,
    file: FileId,
}

impl Drop for Sending {
    fn drop(&mut self) {
        let ended = self.owner.files().remove(&self.file);
        if ended {
            self.owner.changed();
        }
    }
}

#[cfg(test)]
#[path = "uploads_in_flight_tests.rs"]
mod tests;
