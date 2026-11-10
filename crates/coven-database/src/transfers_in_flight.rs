//! The stored objects this process is transferring now (§4, §17.8, §17.9):
//! the ones it is sending, and the ones it is downloading to keep whole.
//! Nothing about them is stored: an attempt ends when its task does, a crash
//! included, so a reopened store starts with none. App SQL reads them through
//! coven's SQL functions, as `coven_uploads.sending` and the `coven_sending`
//! and `coven_downloading` views, which name each file over an object, and a
//! live query that reads one runs again as a transfer it shows starts or
//! ends. How far each has got is progress no table holds: a `TransferWatch`
//! hears it, by file, and how each ended (E7, E8).

use crate::observation::{CommitObserver, RowChange};
use crate::transfer_watch::{changes_since, Heard, TransferChange, TransferEnd};
use coven_foundation::id_source::{DeviceId, FileId, ObjectId};
use coven_foundation::locks::lock_unpoisoned;
use rusqlite::functions::FunctionFlags;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// How far one transfer has got: the bytes of its encrypted object
/// handed to the connection or received, in whatever order, out of the
/// object's whole size. Known from the transfer's start; never stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransferProgress {
    /// The object's bytes moved so far.
    pub bytes_transferred: u64,
    /// The object's whole encrypted size.
    pub bytes_total: u64,
}

/// One way files go, and the SQL that reads the files going that way.
struct Direction {
    /// What a start or end changes, as live queries read it: a view's
    /// column, or a view whose rows it changes.
    read_as: &'static [(&'static str, &'static str)],
    /// `name(object)`, whether the stored object with this id is in flight,
    /// for a view that asks of each of its rows.
    contains: Option<&'static str>,
    /// `name()`, the objects in flight as a JSON array of hex text, each
    /// one's uploader's eight id bytes and then its own sixteen: what a view
    /// joins by index.
    objects: &'static str,
}

const SENDING: Direction = Direction {
    read_as: &[("coven_uploads", "sending"), ("coven_sending", "")],
    contains: Some("_coven_sending"),
    objects: "_coven_sending_objects",
};

const DOWNLOADING: Direction = Direction {
    read_as: &[("coven_downloading", "")],
    contains: None,
    objects: "_coven_downloading_objects",
};

/// Coven's functions that read the files in flight, which app SQL reaches
/// only through coven's views.
pub(crate) const FUNCTIONS: [&str; 3] =
    ["_coven_sending", "_coven_sending_objects", "_coven_downloading_objects"];

/// Both ways files go: one owner per open store, whose clones share it.
#[derive(Clone)]
pub(crate) struct TransfersInFlight {
    uploads: InFlight,
    downloads: InFlight,
}

impl TransfersInFlight {
    pub(crate) fn new(observer: CommitObserver) -> Self {
        Self {
            uploads: InFlight::new(&SENDING, observer.clone()),
            downloads: InFlight::new(&DOWNLOADING, observer),
        }
    }

    /// The files this process is sending, as the upload worker marks them.
    pub(crate) fn uploads(&self) -> InFlight {
        self.uploads.clone()
    }

    /// The files this process is downloading to keep whole, as each pin's
    /// download marks them.
    pub(crate) fn downloads(&self) -> InFlight {
        self.downloads.clone()
    }

    /// Register both ways' functions on a connection to the store's
    /// database. None is deterministic; all are safe in coven's views.
    pub(crate) fn register(&self, connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        self.uploads.register(connection)?;
        self.downloads.register(connection)
    }
}

/// The stored objects this process is transferring one way. The worker
/// transferring them adds each attempt as it starts and removes it as it
/// ends, saying how, cancelled included. Clones share it.
#[derive(Clone)]
pub struct InFlight {
    shared: Arc<InFlightShared>,
}

struct InFlightShared {
    direction: &'static Direction,
    state: Mutex<InFlightState>,
    /// Told of each start and end, as of a commit that changed what the
    /// live queries reading the direction's views read.
    observer: CommitObserver,
    /// Tells each watch that a file started, moved or ended, after the state
    /// changed; a watch reads the change from the state.
    changed: watch::Sender<()>,
}

#[derive(Default)]
struct InFlightState {
    /// Each object in flight, with its uploader and its attempts.
    objects: BTreeMap<ObjectId, Entry>,
    /// The next attempt's number, so an ended attempt is never taken for a
    /// later one of the same object.
    next: u64,
    /// Each open watch, by number, with what its reader last heard of each
    /// file: how far it had got, or how it ended since. `None` before the
    /// watch's first read. An attempt that ends marks its files ended here
    /// in the same step that takes them out of flight, so no reader can
    /// hear a file gone before it hears how it ended.
    watches: BTreeMap<u64, Option<BTreeMap<FileId, Heard>>>,
    /// The next watch's number.
    next_watch: u64,
}

struct Entry {
    device: DeviceId,
    /// Its attempts in flight, oldest first, each with the files it moves
    /// the object's bytes for and how far it has got. One pin can keep the
    /// same object twice at once: it stays in flight until both end, and
    /// shows the newer one's progress.
    attempts: Vec<(u64, Vec<FileId>, TransferProgress)>,
}

impl InFlightState {
    /// An attempt for `files` ended as `end` says: each of them no attempt
    /// still moves is ended for every watch that heard it.
    fn ended(&mut self, files: &[FileId], end: TransferEnd) {
        let now = self.progress();
        for file in files.iter().filter(|file| !now.contains_key(*file)) {
            for heard in self.watches.values_mut().flatten() {
                if let Some(heard) = heard.get_mut(file) {
                    *heard = Heard::Ended(end);
                }
            }
        }
    }

    /// Each file in flight with the progress its object's newest attempt
    /// shows.
    fn progress(&self) -> BTreeMap<FileId, TransferProgress> {
        let mut progress = BTreeMap::new();
        for entry in self.objects.values() {
            for (_, files, moved) in &entry.attempts {
                for file in files {
                    progress.insert(*file, *moved);
                }
            }
        }
        progress
    }
}

impl InFlight {
    fn new(direction: &'static Direction, observer: CommitObserver) -> Self {
        Self {
            shared: Arc::new(InFlightShared {
                direction,
                state: Mutex::new(InFlightState::default()),
                observer,
                changed: watch::channel(()).0,
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, InFlightState> {
        lock_unpoisoned(&self.shared.state)
    }

    /// The stored objects in flight now.
    pub fn current(&self) -> std::collections::BTreeSet<ObjectId> {
        self.state().objects.keys().copied().collect()
    }

    /// Mark the stored object, which `device` uploaded, as in flight for
    /// `files` until the returned guard drops, as far as `progress` says.
    pub fn start(
        &self,
        device: DeviceId,
        object: ObjectId,
        files: Vec<FileId>,
        progress: TransferProgress,
    ) -> Transferring {
        let (attempt, started) = {
            let mut state = self.state();
            let attempt = state.next;
            state.next += 1;
            let entry = state
                .objects
                .entry(object)
                .or_insert_with(|| Entry { device, attempts: Vec::new() });
            entry.attempts.push((attempt, files, progress));
            (attempt, entry.attempts.len() == 1)
        };
        self.shared.changed.send_replace(());
        if started {
            self.changed();
        }
        Transferring { owner: self.clone(), object, attempt, end: TransferEnd::Dropped }
    }

    /// Hear how far each file in flight has got: all of them first, then
    /// each change, and how each file ended, until the store closes.
    pub fn subscribe(&self) -> crate::TransferWatch {
        let number = {
            let mut state = self.state();
            let number = state.next_watch;
            state.next_watch += 1;
            state.watches.insert(number, None);
            number
        };
        crate::TransferWatch::new(
            self.clone(),
            number,
            self.shared.changed.subscribe(),
            self.shared.observer.subscribe(),
        )
    }

    /// What watch `number` has yet to hear: every file in flight on its
    /// first read, and after that each change since its last.
    pub(crate) fn read(&self, number: u64) -> Vec<TransferChange> {
        let mut state = self.state();
        let now = state.progress();
        let heard = state.watches.get_mut(&number).expect("a watch is registered until it drops");
        match heard {
            Some(heard) => changes_since(heard, now),
            None => {
                *heard = Some(
                    now.iter().map(|(file, progress)| (*file, Heard::Moved(*progress))).collect(),
                );
                vec![TransferChange::Reset(now)]
            }
        }
    }

    /// Watch `number` dropped: nothing hears for it any more.
    pub(crate) fn forget(&self, number: u64) {
        self.state().watches.remove(&number);
    }

    /// The attempt has got `bytes` of the way; an attempt that ended moves
    /// nothing.
    fn reached(&self, object: ObjectId, attempt: u64, bytes: u64) {
        let mut state = self.state();
        let Some(progress) = state.objects.get_mut(&object).and_then(|entry| {
            entry
                .attempts
                .iter_mut()
                .find(|(number, _, _)| *number == attempt)
                .map(|(_, _, progress)| progress)
        }) else {
            return;
        };
        progress.bytes_transferred = bytes;
        drop(state);
        self.shared.changed.send_replace(());
    }

    /// Tell live queries after the files changed, so a query that ran
    /// before the change runs again, and one that runs after it reads them.
    fn changed(&self) {
        self.shared.observer.commit(
            crate::observation::Judged::unjudged(
                self.shared
                    .direction
                    .read_as
                    .iter()
                    .map(|(table, column)| RowChange {
                        table: (*table).into(),
                        column: (*column).into(),
                        rows: None,
                    })
                    .collect(),
            ),
            None,
        );
    }

    fn register(&self, connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;
        if let Some(name) = self.shared.direction.contains {
            let owner = self.clone();
            connection.create_scalar_function(name, 1, flags, move |ctx| {
                // Anything but an object id fails the statement.
                Ok(owner.state().objects.contains_key(&ctx.get::<ObjectId>(0)?))
            })?;
        }
        let owner = self.clone();
        connection.create_scalar_function(self.shared.direction.objects, 0, flags, move |_| {
            let objects: Vec<String> = owner
                .state()
                .objects
                .iter()
                .map(|(object, entry)| format!("{:016x}{}", entry.device.0, object.0.simple()))
                .collect();
            serde_json::to_string(&objects)
                .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))
        })
    }
}

/// One attempt to transfer a stored object. Its worker ends it as its
/// transfer ended; dropping it otherwise, as a cancelled attempt does, ends
/// it as dropped.
pub struct Transferring {
    owner: InFlight,
    object: ObjectId,
    attempt: u64,
    /// How the attempt ends when this drops.
    end: TransferEnd,
}

impl Transferring {
    /// What tells this attempt's progress, for whatever moves its bytes.
    pub fn meter(&self) -> TransferMeter {
        TransferMeter { owner: self.owner.clone(), object: self.object, attempt: self.attempt }
    }

    /// End the attempt as `end` says.
    pub fn end(mut self, end: TransferEnd) {
        self.end = end;
    }
}

/// Tells one attempt how far it has got. Once the attempt ends, it tells
/// nothing: a request still finishing can't move a later attempt.
#[derive(Clone)]
pub struct TransferMeter {
    owner: InFlight,
    object: ObjectId,
    attempt: u64,
}

impl TransferMeter {
    /// The attempt has moved `bytes` of the file's object.
    pub fn reached(&self, bytes: u64) {
        self.owner.reached(self.object, self.attempt, bytes);
    }
}

impl Drop for Transferring {
    fn drop(&mut self) {
        let ended = {
            let mut state = self.owner.state();
            let attempt = state.objects.get_mut(&self.object).and_then(|entry| {
                let at =
                    entry.attempts.iter().position(|(attempt, _, _)| *attempt == self.attempt)?;
                let (_, files, _) = entry.attempts.remove(at);
                Some((files, entry.attempts.is_empty()))
            });
            attempt.map(|(files, ended)| {
                if ended {
                    state.objects.remove(&self.object);
                }
                state.ended(&files, self.end);
                ended
            })
        };
        match ended {
            Some(ended) => {
                self.owner.shared.changed.send_replace(());
                if ended {
                    self.owner.changed();
                }
            }
            // An attempt in flight keeps its place in its object's entry;
            // without it the progress watches read is wrong. This drop may
            // run during an unwind, so it fails the store's handle instead of
            // panicking, and the bug goes to the call it runs in or the bug
            // sink (E15).
            None => self.owner.shared.observer.fail_with_bug(coven_foundation::task::Bug::new(
                "end a transfer in flight",
                "an attempt in flight lost its place in its object's entry",
            )),
        }
    }
}

#[cfg(test)]
#[path = "transfers_in_flight_tests.rs"]
mod tests;
