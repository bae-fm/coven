//! The files this process is transferring now (§4, §17.6, §17.7): the ones
//! it is sending, and the ones it is downloading to keep whole. Nothing about
//! them is stored: an attempt ends when its task does, a crash included, so a
//! reopened store starts with none. App SQL reads them through coven's SQL
//! functions, as `coven_uploads.sending` and the `coven_sending` and
//! `coven_downloading` views, and a live query that reads one runs again as a
//! transfer it shows starts or ends.

use crate::observation::{CommitObserver, RowChange};
use coven_foundation::id_source::{DeviceId, FileId};
use rusqlite::functions::FunctionFlags;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// One way files go, and the SQL that reads the files going that way.
struct Direction {
    /// What a start or end changes, as live queries read it: a view's
    /// column, or a view whose rows it changes.
    read_as: &'static [(&'static str, &'static str)],
    /// `name(file)`, whether the file with these 16 id bytes is in flight,
    /// for a view that asks of each of its rows.
    contains: Option<&'static str>,
    /// `name()`, the files in flight as a JSON array of hex text, each one's
    /// uploader's eight id bytes and then its own sixteen: what a view joins
    /// by index.
    files: &'static str,
}

const SENDING: Direction = Direction {
    read_as: &[("coven_uploads", "sending"), ("coven_sending", "")],
    contains: Some("_coven_sending"),
    files: "_coven_sending_files",
};

const DOWNLOADING: Direction = Direction {
    read_as: &[("coven_downloading", "")],
    contains: None,
    files: "_coven_downloading_files",
};

/// Coven's functions that read the files in flight, which app SQL reaches
/// only through coven's views.
pub(crate) const FUNCTIONS: [&str; 3] = [
    "_coven_sending",
    "_coven_sending_files",
    "_coven_downloading_files",
];

/// Both ways files go: one owner per open store, whose clones share it.
#[derive(Clone)]
pub(crate) struct TransfersInFlight {
    pub(crate) uploads: InFlight,
    pub(crate) downloads: InFlight,
}

impl TransfersInFlight {
    pub(crate) fn new(observer: CommitObserver) -> Self {
        Self {
            uploads: InFlight::new(&SENDING, observer.clone()),
            downloads: InFlight::new(&DOWNLOADING, observer),
        }
    }

    /// Register both ways' functions on a connection to the store's
    /// database. None is deterministic; all are safe in coven's views.
    pub(crate) fn register(&self, connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        self.uploads.register(connection)?;
        self.downloads.register(connection)
    }
}

/// The files this process is transferring one way. The worker transferring
/// them adds each attempt as it starts and removes it as it ends, cancelled
/// included. Clones share it.
#[derive(Clone)]
pub struct InFlight {
    shared: Arc<Shared>,
}

struct Shared {
    direction: &'static Direction,
    state: Mutex<State>,
    /// Told of each start and end, as of a commit that changed what the
    /// live queries reading the direction's views read.
    observer: CommitObserver,
}

#[derive(Default)]
struct State {
    /// Each file in flight, with its uploader and its attempts.
    files: BTreeMap<FileId, Entry>,
    /// The next attempt's number, so an ended attempt is never taken for a
    /// later one of the same file.
    next: u64,
}

struct Entry {
    device: DeviceId,
    /// Its attempts in flight, oldest first. One pin can keep the same file
    /// twice at once, and the file stays in flight until both end.
    attempts: Vec<u64>,
}

impl InFlight {
    fn new(direction: &'static Direction, observer: CommitObserver) -> Self {
        Self {
            shared: Arc::new(Shared {
                direction,
                state: Mutex::new(State::default()),
                observer,
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.shared
            .state
            .lock()
            .expect("transfers in flight poisoned")
    }

    /// The files in flight now.
    pub fn current(&self) -> std::collections::BTreeSet<FileId> {
        self.state().files.keys().copied().collect()
    }

    /// Mark the file, which `device` uploaded, as in flight until the
    /// returned guard drops.
    pub fn start(&self, device: DeviceId, file: FileId) -> Transferring {
        let (attempt, started) = {
            let mut state = self.state();
            let attempt = state.next;
            state.next += 1;
            let entry = state.files.entry(file).or_insert_with(|| Entry {
                device,
                attempts: Vec::new(),
            });
            entry.attempts.push(attempt);
            (attempt, entry.attempts.len() == 1)
        };
        if started {
            self.changed();
        }
        Transferring {
            owner: self.clone(),
            file,
            attempt,
        }
    }

    /// Tell live queries after the files changed, so a query that ran
    /// before the change runs again, and one that runs after it reads them.
    fn changed(&self) {
        self.shared.observer.commit(
            self.shared
                .direction
                .read_as
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

    fn register(&self, connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;
        if let Some(name) = self.shared.direction.contains {
            let owner = self.clone();
            connection.create_scalar_function(name, 1, flags, move |ctx| {
                // Anything but a file id's 16 bytes fails the statement.
                let file = uuid::Uuid::from_slice(&ctx.get::<Vec<u8>>(0)?)
                    .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
                Ok(owner.state().files.contains_key(&FileId(file)))
            })?;
        }
        let owner = self.clone();
        connection.create_scalar_function(self.shared.direction.files, 0, flags, move |_| {
            let files: Vec<String> = owner
                .state()
                .files
                .iter()
                .map(|(file, entry)| format!("{:016x}{}", entry.device.0, file.0.simple()))
                .collect();
            serde_json::to_string(&files)
                .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))
        })
    }
}

/// One attempt to transfer a file; dropping it, also when its attempt is
/// cancelled, ends the attempt.
pub struct Transferring {
    owner: InFlight,
    file: FileId,
    attempt: u64,
}

impl Drop for Transferring {
    fn drop(&mut self) {
        let ended = {
            let mut state = self.owner.state();
            let entry = state
                .files
                .get_mut(&self.file)
                .expect("an attempt in flight keeps its file's entry");
            entry.attempts.retain(|attempt| *attempt != self.attempt);
            let ended = entry.attempts.is_empty();
            if ended {
                state.files.remove(&self.file);
            }
            ended
        };
        if ended {
            self.owner.changed();
        }
    }
}

#[cfg(test)]
#[path = "transfers_in_flight_tests.rs"]
mod tests;
