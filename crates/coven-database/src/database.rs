//! Opening is the composition root; a shared handle owns all connections and
//! the writer lock. Blocking calls retain these resources after its last owner
//! drops, while another open waits for their release.

use crate::store_call::StoreCall;
#[cfg(test)]
use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::locks::{read_unpoisoned, write_unpoisoned};
use coven_foundation::task::{blocking_completing, CarriesBug};
#[cfg(test)]
use std::sync::Mutex;
use std::sync::{Arc, RwLock};

#[path = "file_database.rs"]
pub(crate) mod file_database;

#[path = "database_local_bytes.rs"]
pub(crate) mod local_bytes;

#[path = "library_bytes.rs"]
pub(crate) mod library_bytes;

#[path = "archive_bytes.rs"]
pub(crate) mod archive_bytes;

#[path = "database_mode.rs"]
mod mode;
pub use mode::{Mode, ReadOnly, Writer};

#[path = "file_staging.rs"]
mod file_staging;
pub use file_staging::StagedFiles;

#[path = "input_database.rs"]
pub(crate) mod input_database;

#[path = "database_operations.rs"]
mod operations;
#[path = "database_provider_access.rs"]
mod provider_access;

#[path = "database_changes.rs"]
mod changes;
#[path = "circle_rebuild.rs"]
mod circle_rebuild;
#[path = "database_file_retention.rs"]
pub(crate) mod file_retention;
#[path = "database_live_queries.rs"]
mod live_queries;
#[path = "database_snapshots.rs"]
mod snapshots;
pub use circle_rebuild::{ExportStopped, RebuildDatabase, RebuildError};

#[cfg(any(test, feature = "test-utils"))]
#[path = "database_operations_tests.rs"]
pub(crate) mod operation_tests;

use coven_foundation::clock::{ClockRef, SystemClock};
use coven_foundation::files::{StoreDir, StoreLock};
use coven_foundation::id_source::FileId;
use coven_foundation::id_source::{DeviceId, IdSourceRef, UuidIds};

use crate::app_policy::CovenPolicy;
use crate::read::RunningRead;
use crate::sqlite::StoreSql;
use crate::store_failure::StoreFailure;
use crate::store_failure::StoreHandle;
use crate::write_failure::Hosted;
use crate::{DbError, Migration, MigrationOutcome, SyncedTable};
use crate::{LiveQuery, LostValue, Read, SqlReadContext};
use sqlive::Setup;

#[path = "database_builder.rs"]
mod builder;
#[cfg(test)]
pub(crate) use builder::coven_only;
#[path = "database_store.rs"]
mod store;

#[path = "database_call.rs"]
mod call;
#[path = "database_connection_calls.rs"]
mod connection_calls;
#[path = "database_retry.rs"]
mod retry;
#[path = "database_sync.rs"]
mod sync;
pub use builder::{
    closed_sync_facts, discarded_operations, discarded_store_name, read_device_id_in_use,
    read_store_name, DatabaseBuilder,
};
pub(crate) use call::{finish_blocking, process, start_call};

/// Shared ownership of the database, as its writer or a read-only handle
/// (`StoreDatabase<ReadOnly>`), which reads the store through the same calls and
/// cannot write synced rows or migrate. A read-only handle's connections can
/// change only local cache metadata, and its shared store lock prevents
/// deletion until all of them close. Closing any clone closes them all.
///
/// ```compile_fail
/// async fn cannot_write(handle: &coven_database::Database<coven_database::ReadOnly>) {
///     handle.write(|_| Ok(())).await;
/// }
/// ```
/// ```compile_fail
/// fn cannot_subscribe(handle: &coven_database::Database<coven_database::ReadOnly>) {
///     handle.subscribe(|_| Ok::<_, coven_database::DbError>(()));
/// }
/// ```
/// ```compile_fail
/// fn cannot_reconfigure(handle: &coven_database::Database<coven_database::ReadOnly>) {
///     handle.subscribe_reconfigurable((), |_, _| Ok::<_, coven_database::DbError>(()));
/// }
/// ```
/// ```compile_fail
/// fn cannot_subscribe_losses(handle: &coven_database::Database<coven_database::ReadOnly>) {
///     handle.subscribe_lost_values();
/// }
/// ```
/// ```compile_fail
/// fn sends_nothing(handle: &coven_database::Database<coven_database::ReadOnly>) {
///     handle.uploads_in_flight();
/// }
/// ```
pub struct StoreDatabase<M: Mode = Writer> {
    /// Where every connection reports its commits (§4); it outlives them, and
    /// once they close makes only closed subscriptions.
    observer: StoreHandle,
    /// Where a bug in the store's work that nobody waits for goes (E15).
    bugs: coven_foundation::task::BugSinkRef,
    /// The open store, `None` once closed. Closing takes it before anything
    /// that can panic, so a panic while it is written leaves it `None`, a
    /// whole state: the lock is taken as it is (E15).
    inner: Arc<RwLock<Option<M::Open>>>,
    /// Marks the store's lock closing, for a wait in this process on its
    /// release, once closing begins or the last clone drops.
    closing: Arc<coven_foundation::files::StoreClosingGuard>,
    /// What only the writer holds beside it.
    writer: M::WriterParts,
}

// Written out: `derive(Clone)` would ask the open store to be `Clone`.
impl<M: Mode> Clone for StoreDatabase<M> {
    fn clone(&self) -> Self {
        Self {
            observer: self.observer.clone(),
            bugs: self.bugs.clone(),
            inner: self.inner.clone(),
            closing: self.closing.clone(),
            writer: self.writer.clone(),
        }
    }
}

/// What the store's writer holds beside its open store.
#[derive(Clone)]
pub struct WriterParts {
    /// The files this process is sending and downloading now, and the
    /// device's network as this process last heard of it, which coven's
    /// views read (§4).
    facts: crate::memory_facts::ProcessFacts,
    file_tasks: Arc<tokio::sync::RwLock<()>>,
    /// Told when a close finds a call holding the open store and waits for
    /// it to end.
    #[cfg(test)]
    close_waits: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    /// The removals of snapshot stagings their callers abandoned, for a test
    /// to wait on.
    #[cfg(test)]
    abandoned_stagings: Arc<Mutex<Vec<tests::hooks::StagingRemoval>>>,
}

/// The writer's open store.
pub struct DatabaseInner {
    inputs: input_database::InputDatabase,
    access: DatabaseAccess<StoreLock>,
    migrations: Vec<MigrationOutcome>,
    /// The journal through which passes publish raise captures, when the
    /// composition root supplied one (`DatabaseBuilder::migration_operation`):
    /// a circle captured after the migration's journal finished opens it
    /// again (§18.3).
    raise_operation: Option<Box<crate::migration::MigrationOperation>>,
    /// The app's write for a member's device as its registration applies:
    /// the transaction that first applies this device's registration runs it
    /// (§13.2).
    joining_write: Option<Box<crate::joining_write::JoiningHook>>,
    /// The app's before-commit function, which each transaction that changes
    /// the app's rows runs last (§9.4).
    before_commit: Option<Box<crate::before_commit::BeforeCommit>>,
    /// What a rebuild opens its database with (§18.3).
    open_inputs: builder::OpenInputs,
    clock: ClockRef,
    ids: IdSourceRef,
    /// The names of copies being written, or written and waiting for the
    /// write that names them, which no write's removal takes (Appendix C.3):
    /// staged app bytes and cache files. Shared with what holds them, so
    /// letting one go never takes the store's own lock. Each change under
    /// its lock is whole before it can panic, so the lock is taken as it is (E15).
    staging: Arc<crate::reserved_names::ReservedNames>,
    /// The process facts, including the copy reservations used by writes
    /// and removals (§17.3).
    facts: crate::memory_facts::ProcessFacts,
    /// Runs where released files' bytes are about to go, after the writer is
    /// free, so a test can stall that disk work.
    #[cfg(test)]
    removal_gate: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Runs where a write's staged files have been flushed, before its
    /// commit, so a test can see the order or fail the flush.
    #[cfg(test)]
    flush_hook: Mutex<Option<coven_foundation::files::FlushHook>>,
    /// Fail this many coming `end_retry_waits` transactions with a disk
    /// error, so a test can show that a failed end leaves every wait.
    #[cfg(any(test, feature = "test-utils"))]
    retry_end_failures: std::sync::atomic::AtomicUsize,
    /// Fail this many coming `applied_slot` reads with a disk error, so a
    /// test can show what a failed read leaves in memory.
    #[cfg(any(test, feature = "test-utils"))]
    applied_slot_failures: std::sync::atomic::AtomicUsize,
}

/// The connections every handle reads through, and the lock on the store's
/// directory, which outlives every connection, including when the last
/// handle is dropped.
pub struct DatabaseAccess<L> {
    /// The store's database: its writer, readers and upkeep threads.
    database: sqlive::Database<CovenPolicy>,
    // Every connection reports its commits and handle failures here (§4).
    observer: StoreHandle,
    write_schema: crate::write_schema::WriteSchema,
    directory: StoreDir,
    device: DeviceId,
    lock: L,
    /// What coven's tests read back of the calls on these connections.
    tests: crate::store_call::TestRecords,
    /// Runs once, where the next read of a file on this device only has
    /// found where its bytes are and is about to open them, so a test can
    /// commit between the two.
    #[cfg(test)]
    local_open_gate: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl<L> DatabaseAccess<L> {
    /// Refuse calls once a connection failed the handle. A call whose own
    /// work failed the handle with a bug returns that bug as `DbError::Bug`:
    /// its own error, such as a statement the broken authorizer refused,
    /// says less, and the app's closure may have dropped it. A bug its error
    /// carries beside that one goes to the sink, so neither is lost. Any
    /// other failing call keeps its own error, and one that succeeds while
    /// the handle fails reports the failure (E15).
    fn call<R, E: From<DbError> + CarriesBug>(
        &self,
        run: impl FnOnce() -> Result<R, E>,
    ) -> Result<R, E> {
        if let Some(error) = self.observer.error() {
            return Err(DbError::from(error).into());
        }
        let (result, own) = self.observer.calling(run);
        if let Some(bug) = own {
            let carried = result.as_ref().err().and_then(CarriesBug::bug);
            if let Some(other) = carried.filter(|other| **other != bug) {
                self.observer.report(other);
            }
            return Err(DbError::Bug(bug).into());
        }
        match (result, self.observer.error()) {
            (Ok(_), Some(error)) => Err(DbError::from(error).into()),
            (result, _) => result,
        }
    }

    /// Remove released files' bytes now that the writer is free, then forget
    /// their records under it (§21.2, Appendix C.3). `chosen` is what the writer
    /// that recorded them chose.
    fn remove_released(&self, chosen: crate::file_removals::Chosen) -> Vec<DbError> {
        let released = match chosen {
            Ok(released) => released,
            Err(error) => return vec![error],
        };
        let (removed, mut failures) = crate::file_removals::remove_bytes(&self.directory, released);
        if !removed.is_empty() {
            let forgotten = self.with_writer(|writer, call| {
                writer.transaction(|db| crate::file_removals::forget(db, call, &removed))?;
                #[cfg(any(test, feature = "test-utils"))]
                call.write_checkpoint(crate::test_utils::WriteCheckpoint::Finished);
                Ok::<_, DbError>(())
            });
            if let Err(error) = forgotten {
                failures.push(error);
            }
        }
        failures
    }

    /// The bytes here of a file its record says is on this device only,
    /// opened, or the record that says otherwise. They are found in one
    /// committed state and opened after it, and a write committing between
    /// the two, such as its upload's completion, can release them, which
    /// coven deletes as that write returns (Appendix C.3). Bytes are released
    /// only once no record names them, so bytes missing or changed are
    /// judged by the record as it stands after the open: one that no longer
    /// names bytes here means the file left this device only meanwhile, and
    /// the read goes on as that record says; one that still names them
    /// leaves the failure standing.
    fn open_local_file(
        &self,
        reference: &crate::FileRef,
        place: coven_foundation::files::LeasePlace,
    ) -> Result<crate::Located<crate::LocalFileStream>, crate::LocalFileError> {
        let file = reference.id();
        let source = || {
            self.with_committed(|db, _| crate::file_ref::device_only_source(db, self.device, file))
        };
        let found = match source()?.here() {
            Ok(found) => found,
            Err(elsewhere) => return Ok(elsewhere),
        };
        #[cfg(test)]
        if let Some(gate) = lock_unpoisoned(&self.local_open_gate).take() {
            gate();
        }
        match crate::file_ref::open_source(
            &self.directory,
            &file.to_string(),
            reference.size(),
            found,
            place,
        ) {
            Ok(stream) => Ok(crate::Located::Here(stream)),
            Err(error) if error.judges_bytes() => match source()?.here() {
                Ok(_) => Err(error),
                Err(elsewhere) => Ok(elsewhere),
            },
            Err(error) => Err(error),
        }
    }

    fn file_ref(&self, file: FileId) -> Result<crate::FileRef, DbError> {
        self.with_committed(|db, _| crate::file_ref::read(db, file))
    }

    fn user_file(&self, file: FileId) -> Result<Option<crate::UserFile>, DbError> {
        self.with_committed(|db, _| crate::user_file::read(db, file))
    }

    /// Run `read` in one snapshot on an idle reader, reading each constant
    /// once.
    fn read<R, E: From<DbError>>(
        &self,
        read: impl FnOnce(SqlReadContext<'_>) -> Result<R, E>,
    ) -> Result<R, E> {
        self.with_reader(|reader, call| {
            let _constants = crate::transaction_constants::keep_constants(reader, call);
            reader
                .read_snapshot(|reader| read(SqlReadContext::new(reader)).map_err(Hosted))
                .map_err(Hosted::into_inner)
        })
    }

    fn schema_version(&self) -> Result<u32, DbError> {
        self.with_reader(crate::transaction_constants::schema_version)
    }

    /// Every connection has the same settings, so one reader reports them.
    fn sqlite_settings(&self) -> Result<crate::SqliteSettingsInEffect, DbError> {
        Ok(self.database.reader().host(StoreSql::sqlite_settings)?)
    }

    fn close(self) -> Result<(), DbError> {
        self.observer.close();
        let failures: Vec<DbError> = self.database.close().into_iter().map(DbError::from).collect();
        drop(self.lock);
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Closing { failures })
        }
    }
}

impl DatabaseInner {
    fn call<R, E: From<DbError> + CarriesBug>(
        &self,
        run: impl FnOnce(&Self) -> Result<R, E>,
    ) -> Result<R, E> {
        self.access.call(|| run(self))
    }

    // Their durable removal records remain for the next write or open.
    fn release_file_reservations(&self, names: &[crate::file_removals::OwnedFile]) {
        self.staging.release(names);
    }

    /// Run `operation`, a write, under the writer, then remove the files it
    /// released once the writer is free (Appendix C.3), and run what its
    /// steps asked to follow its commit. A panic in it fails it with
    /// `DbError::Bug` once its transaction has rolled back, and its released
    /// files go as after any failed write.
    fn with_files<R, E: crate::WriteFailure>(
        &self,
        operation: &'static str,
        run: impl FnOnce(
            &mut StoreSql,
            &StoreCall<'_>,
            &crate::file_write::WriteFiles<'_>,
        ) -> Result<R, E>,
    ) -> Result<R, E> {
        let (result, chosen, committed) = self.access.with_writer(|writer, call| {
            let files = crate::file_write::WriteFiles::new(
                &self.access.write_schema,
                self.access.device,
                self.ids.as_ref(),
                &self.staging,
                &self.facts,
                self.before_commit.as_deref(),
            );
            let result =
                coven_foundation::task::caught_sync(operation, || run(writer, call, &files))
                    .unwrap_or_else(|bug| Err(E::from(DbError::Bug(bug))));
            // After a failed rollback the writer may still hold this write;
            // removal records wait for the next open (Appendix C.3).
            let usable = !matches!(
                self.access.observer.error::<DbError>(),
                Some(sqlive::Error::Host(StoreFailure::Rollback))
            );
            let chosen = usable.then(|| released(writer, call, self));
            Ok::<_, E>((result, chosen, files.take_committed()))
        })?;
        let result = match chosen {
            Some(chosen) => self.finish_removals(result, chosen),
            None => result,
        };
        // A write that returns success has committed.
        if result.is_ok() {
            committed.into_iter().for_each(|effect| effect());
        }
        result
    }

    /// Remove what `chosen` names, after the writer that chose it is
    /// released (§21.2, Appendix C.3).
    fn remove_chosen(&self, chosen: crate::file_removals::Chosen) -> Vec<DbError> {
        #[cfg(test)]
        {
            let gate = lock_unpoisoned(&self.removal_gate).clone();
            if let Some(gate) = gate {
                gate();
            }
        }
        self.access.remove_released(chosen)
    }

    /// `result`, after removing the files it released, as `chosen` names
    /// them; a failed removal keeps its record for the next write or open.
    fn finish_removals<R, E: crate::WriteFailure>(
        &self,
        result: Result<R, E>,
        chosen: crate::file_removals::Chosen,
    ) -> Result<R, E> {
        crate::write_failure::after_cleanup(result, self.remove_chosen(chosen))
    }
}

/// The removals the write that just ran under `writer` released, chosen
/// against the staging names and a snapshot of the copy reservations (Appendix C.3).
fn released(
    writer: &StoreSql,
    call: &StoreCall<'_>,
    inner: &DatabaseInner,
) -> crate::file_removals::Chosen {
    inner.staging.with_names(|active| {
        inner.facts.with_reserved_copies(|copies| {
            crate::file_removals::choose(writer, call, active, &|path| copies.contains_key(path))
        })
    })
}

impl<M: Mode> StoreDatabase<M> {
    /// Where a bug in the store's work that nobody waits for goes (E15).
    pub fn bugs(&self) -> &coven_foundation::task::BugSinkRef {
        &self.bugs
    }

    /// Run `run` on the open store on the blocking pool, refused once a
    /// connection failed the handle.
    async fn call<R, E>(
        &self,
        run: impl FnOnce(&M::Open) -> Result<R, E> + Send + 'static,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        finish_blocking(
            start_call("database call", &self.bugs, self.inner.clone(), move |open| {
                M::access(open).call(|| run(open))
            })
            .await,
        )
    }

    /// Open the local source of a file still on this device only, in `place`
    /// of the app's budget, which the stream holds until its last read ends.
    /// No storage operation occurs. For a file whose record no longer keeps
    /// it on this device only, or that has no record, it returns what the
    /// open found instead, and the place goes back.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
        place: coven_foundation::files::LeasePlace,
    ) -> Result<crate::Located<crate::LocalFileStream>, crate::LocalFileError> {
        let reference = reference.clone();
        self.call(move |open| M::access(open).open_local_file(&reference, place)).await
    }

    /// The file this device has a record of under this id, from one
    /// committed state.
    pub async fn file_ref(&self, file: FileId) -> Result<crate::FileRef, DbError> {
        self.call(move |open| M::access(open).file_ref(file)).await
    }

    /// The recorded original's path, size and modification time; never reread its bytes.
    pub async fn user_file(&self, file: FileId) -> Result<Option<crate::UserFile>, DbError> {
        self.call(move |open| M::access(open).user_file(file)).await
    }

    /// Run ordinary SQL against one consistent snapshot when awaited. The
    /// read fails with the closure's own error type, and with a failure of
    /// the database's own as that type's `From<DbError>`.
    pub fn read<F, R, E>(&self, read: F) -> Read<'_, F, M>
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        Read::new(self, read)
    }

    /// Decode every lost cell and removed row in one snapshot.
    pub async fn lost_values(&self) -> Result<Vec<LostValue>, DbError> {
        self.read_committed(crate::loss_record::lost_values).await
    }

    /// Run `read`, coven's own SQL on its tables, in one committed state on
    /// an idle reader, on the blocking pool.
    pub(crate) async fn read_committed<R, E>(
        &self,
        read: impl FnOnce(&StoreSql) -> Result<R, E> + Send + 'static,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        self.call(move |open| M::access(open).with_committed(|db, _| read(db))).await
    }

    pub(crate) fn start_read<F, R, E>(&self, read: F) -> RunningRead<R, E>
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        RunningRead::new(start_call("read", &self.bugs, self.inner.clone(), move |open| {
            let access = M::access(open);
            access.call(|| access.read(read))
        }))
    }

    /// The committed app schema version, read on a read-only connection.
    pub async fn schema_version(&self) -> Result<u32, DbError> {
        self.call(|open| M::access(open).schema_version()).await
    }

    /// The SQLite settings the store's connections have in effect (§4).
    pub async fn sqlite_settings(&self) -> Result<crate::SqliteSettingsInEffect, DbError> {
        self.call(|open| M::access(open).sqlite_settings()).await
    }
}

impl StoreDatabase {
    /// Mark this instance as closing for a reopen to wait on, though workers
    /// still hold clones of it: the facade's owning scope ended (F.4).
    pub fn begin_close(&self) {
        self.closing.begin_close();
    }
    /// The handle's clock could not wait for an invite's expiry: every
    /// later call and live query fails with `DbError::WallTimer` until the
    /// store is reopened (§13.2).
    pub fn fail_expiry_wait(&self, error: coven_foundation::clock::WallTimerError) {
        self.observer.fail(StoreFailure::WallTimer(error));
    }

    /// The files this process is sending now: the upload worker marks each
    /// attempt, and coven's views read them (§4, §17.9).
    pub fn uploads_in_flight(&self) -> crate::InFlight {
        self.writer.facts.uploads()
    }

    /// The files this process is downloading to keep whole now: each pin's
    /// download marks its attempt, and `coven_downloading` reads them (§4,
    /// §17.8).
    pub fn downloads_in_flight(&self) -> crate::InFlight {
        self.writer.facts.downloads()
    }

    /// The store's watch says the device's network is now `network`:
    /// coven's views show it, and a live query that read them runs again
    /// (§4, §6.7).
    pub fn record_network(&self, network: coven_foundation::network_watch::NetworkState) {
        self.writer.facts.record_network(network);
    }

    /// Run app SQL and coven's steps, and commit its unsigned write record
    /// and merge metadata in one IMMEDIATE transaction (§4). The closure's
    /// result is returned after commit.
    pub async fn write<F, R>(&self, sql: F) -> Result<R, DbError>
    where
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.write_as(sql).await
    }

    /// `write`, for a closure whose failures are `E`: callback errors retain
    /// their type alongside rollback or byte-cleanup failures.
    pub async fn write_as<F, R, E>(&self, sql: F) -> Result<R, E>
    where
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        self.call(move |inner| {
            inner.with_files("write", |writer, call, files| {
                let result = coven_foundation::task::caught_sync("write", || {
                    crate::local_write::local_write(
                        writer,
                        call,
                        &inner.access.write_schema,
                        inner.access.device,
                        inner.clock.now(),
                        files,
                        |files| sql(crate::SqlContext::new(files)),
                    )
                });
                // Staged files the write brought in go free once it has
                // decided: a write that rolled back leaves their bytes to the
                // removals its cleanup chooses.
                inner.release_file_reservations(&files.take_brought());
                inner.facts.release_copies(files.take_held_copies());
                #[cfg(any(test, feature = "test-utils"))]
                call.write_checkpoint(crate::test_utils::WriteCheckpoint::StagingReleased);
                result.unwrap_or_else(|bug| Err(E::from(DbError::Bug(bug))))
            })
        })
        .await
    }

    /// Stage app bytes for a later write (§17.2): `stage` names each file's
    /// namespace and place and returns what that write's SQL needs, such as
    /// the ids staging gave them. Their bytes are streamed to coven's own
    /// copies, each synced, and flushed, before this returns; close waits for
    /// that, and for its cleanup if it fails or is cancelled. The result
    /// holds nothing close waits for. A write takes it with `put_staged`.
    pub async fn stage_files<F, T, E>(&self, stage: F) -> Result<StagedFiles<T>, E>
    where
        F: FnOnce(&mut crate::WriteBatch<'_>) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        let lease = self.writer.file_tasks.clone().read_owned().await;
        let database = self.inner.clone();
        let (staging, value) = finish_blocking(
            blocking_completing(
                "stage files",
                self.bugs.clone(),
                crate::commit_task::bind(move || {
                    file_staging::FileStaging::new(database, lease, stage)
                }),
            )
            .await,
        )?;
        let (staging, flushed) =
            staging.write().await.map_err(|failed| E::from(DbError::from(failed)))?;
        match flushed {
            Ok(_flushed) => Ok(staging.into_staged(value)),
            // A failed copy or flush removes what the staging wrote.
            // The discard's result is the call's failure, so a bug it
            // carries reaches the sink too once no caller waits.
            Err(error) => finish_blocking(
                blocking_completing(
                    "discard staged files",
                    self.bugs.clone(),
                    crate::commit_task::bind(move || {
                        Err::<std::convert::Infallible, E>(staging.discard::<E>(error))
                    }),
                )
                .await,
            )
            .map(|never| match never {}),
        }
    }

    /// Stage app bytes, then run a write that puts them in its rows, as an
    /// app does with `stage_files` and `put_staged`, for tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn staged_write<F, S, T, R, E>(&self, stage: F, write: S) -> Result<R, E>
    where
        F: FnOnce(&mut crate::WriteBatch<'_>) -> Result<T, E> + Send + 'static,
        S: FnOnce(crate::SqlContext<'_, '_>, T) -> Result<R, E> + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        let staged = self.stage_files(stage).await?;
        self.write_as(move |sql| {
            let value = sql.put_staged(staged)?;
            write(sql, value)
        })
        .await
    }

    /// Run app SQL that writes device-local tables alone in one IMMEDIATE
    /// transaction, as plain SQLite: no pre-update hook records its row
    /// changes, so SQLite empties a table in one step and fills one at its
    /// own speed (§4). A statement that would write a synced table, itself or
    /// through a trigger or foreign-key action, fails the write with
    /// `SyncedTableInBulkWrite`, and everything rolls back. The app's
    /// before-commit function runs after the closure, in the same
    /// transaction (§9.4). After commit, every live query that read a table
    /// the write's statements or the function's may change runs again,
    /// whatever rows it read. The closure's result is returned after commit.
    pub async fn write_bulk<F, R, E>(&self, sql: F) -> Result<R, E>
    where
        F: FnOnce(crate::BulkSqlContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer, _| {
                writer
                    .bulk_write(|bulk| {
                        let result = sql(crate::BulkSqlContext::new(bulk)).map_err(Hosted)?;
                        crate::before_commit::run(bulk, inner.before_commit.as_deref())
                            .map_err(|error| Hosted(E::from(error)))?;
                        Ok(result)
                    })
                    .map_err(Hosted::into_inner)
            })
        })
        .await
    }

    /// Checkpoint the store's WAL on the writer, between writes (§4).
    pub async fn checkpoint(
        &self,
        mode: crate::CheckpointMode,
    ) -> Result<crate::WalCheckpoint, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer, _| Ok(writer.wal_checkpoint(mode)?))
        })
        .await
    }

    /// Give back free pages of a store created with incremental auto-vacuum,
    /// on the writer, between writes (§4). On any other store it does nothing.
    pub async fn incremental_vacuum(&self, pages: crate::IncrementalVacuum) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer, _| Ok(writer.incremental_vacuum(pages)?))
        })
        .await
    }

    /// Give back every connection's cached pages that no statement is using
    /// (§4). SQLite as coven builds it (`SQLITE_ENABLE_MEMORY_MANAGEMENT`)
    /// keeps the unused pages of every connection in the process in one
    /// pool, and one connection's release empties the whole pool: so one
    /// release on an idle reader frees the writer's, the input database's
    /// and the checkpointers' too, and never waits for a write.
    pub async fn release_memory(&self) -> Result<(), DbError> {
        self.call(|inner| {
            inner.access.database.reader().host(|reader| Ok(reader.release_memory()?))
        })
        .await
    }

    /// The migrations this open committed, classified individually.
    pub fn applied_migrations(&self) -> Result<Vec<MigrationOutcome>, DbError> {
        let inner = read_unpoisoned(&self.inner);
        let inner = inner.as_ref().ok_or(DbError::StoreClosed)?;
        if let Some(error) = inner.access.observer.error() {
            return Err(error.into());
        }
        Ok(inner.migrations.clone())
    }

    /// Wait for database calls, close all connections, then release the writer lock.
    /// Every subsequent database call, including close, reports `StoreClosed`.
    pub async fn close(&self) -> Result<(), DbError> {
        self.closing.begin_close();
        let database = self.clone();
        // Closing runs as its own task so that a caller dropping this future
        // doesn't stop it partway; the runtime dropping that task can.
        let bugs = self.bugs.clone();
        coven_foundation::task::spawn_completing("close the store", self.bugs.clone(), async move {
            let file_tasks = database.writer.file_tasks.clone().write_owned().await;
            finish_blocking(
                blocking_completing(
                    "close the store",
                    bugs,
                    crate::commit_task::bind(move || {
                        let _file_tasks = file_tasks;
                        #[cfg(test)]
                        database.tell_close_wait();
                        let mut slot = write_unpoisoned(&database.inner);
                        let Some(inner) = slot.take() else {
                            return Err(DbError::StoreClosed);
                        };
                        let mut failures = inner.inputs.close();
                        match inner.access.close() {
                            Err(DbError::Closing { failures: application }) => {
                                failures.extend(application)
                            }
                            Err(error) => failures.push(error),
                            Ok(()) => (),
                        }
                        if failures.is_empty() {
                            Ok(())
                        } else {
                            Err(DbError::Closing { failures })
                        }
                    }),
                )
                .await,
            )
        })
        .await
        .map_err(DbError::from)?
    }
}

impl StoreDatabase<ReadOnly> {
    /// Begin closing every clone of this handle, at once and before its
    /// connections close: every later call fails with `StoreClosed`, a call
    /// already running answers `StoreClosed` too, and a deletion in this
    /// process waits for the readers' lock to be released (E1).
    pub fn begin_close(&self) {
        self.observer.close();
        self.closing.begin_close();
    }

    /// Wait for active calls and close every clone of this handle.
    pub async fn close(&self) -> Result<(), DbError> {
        let handle = self.clone();
        finish_blocking(
            blocking_completing("close the store", self.bugs.clone(), move || {
                let mut slot = write_unpoisoned(&handle.inner);
                slot.take().ok_or(DbError::StoreClosed)?.close()
            })
            .await,
        )
    }
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;
