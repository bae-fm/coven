//! Storage setup as an operation (§11.6, §20.8). Setup journals itself
//! before its first write to storage, keeping the credentials it connects
//! with apart from the committed ones. Until its point of no return a
//! cancel or a failure undoes it, and opening the store undoes one a crash
//! cut. Past that point it finishes: in the call, or after a crash in the
//! first connection attempt, with the credentials it kept. Only finishing
//! commits the connection.

use super::*;
use coven_crypto::custody::StoreKeychain;
use coven_database::{OperationOutcome, OperationRejection, UndoCause};
use coven_sync::SetupJournal;

/// Where this session's storage setup stands, which a cancel reads (E5).
pub(crate) enum SetupRun {
    /// No setup runs.
    Idle,
    /// A setup checks the provider; `cancelled` once the app asked to
    /// cancel it.
    Checking { cancelled: bool },
    /// A setup is past its point of no return: this session's, or one a
    /// crash cut, which the first connection finishes.
    Finishing,
}

/// What cancelling storage setup did (E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetupCancellation {
    /// Setup hadn't reached its point of no return: it undoes what it
    /// wrote and fails with `StorageSetupError::Cancelled`.
    Undoing,
    /// Setup is past its point of no return, and finishes.
    Finishing,
    /// No setup is running.
    NoSetup,
}

/// A place between two of setup's steps, where a test stops it as a crash
/// would, or holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetupStop {
    /// Its row and credentials are journaled.
    Journaled,
    /// S3 detection is done.
    Detected,
    /// The provider check is done.
    Checked,
    /// A creation's store key is kept in custody.
    KeyKept,
    /// Its row is past the point of no return.
    Claiming,
    /// What it claims has landed.
    Created,
    /// The connection is committed.
    Committed,
    /// Its row has ended.
    Finished,
}

/// What a test does to the next setup at one of its stops.
#[cfg(test)]
pub(crate) enum SetupHook {
    /// Stop there as a crash would: the call returns at once, undoing and
    /// finishing nothing.
    Crash(SetupStop),
    /// Say it got there on `entered`, then wait for `resume`.
    Pause {
        at: SetupStop,
        entered: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    },
}

/// The error a crash hook stops setup with.
#[cfg(test)]
#[derive(Debug, thiserror::Error)]
#[error("setup stopped as a crash would")]
struct Crashed;

/// Whether `error` is a crash hook's.
#[cfg(test)]
fn crashed(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if error.is::<Crashed>() {
            return true;
        }
        cause = error.source();
    }
    false
}

/// The connections a setup past its point of no return finishes on: the
/// store's, which it commits, and the one its own requests go through.
struct FinishingConnections {
    storage: Arc<StorageConnection>,
    setup: Arc<StorageConnection>,
}

/// A setup failure, and the undo or start that failed after it.
#[derive(Debug, thiserror::Error)]
#[error("{operation}; cleanup failed: {cleanup}")]
struct SetupCleanup {
    operation: StorageSetupError,
    #[source]
    cleanup: SyncError,
}

pub(super) fn with_cleanup(operation: StorageSetupError, cleanup: SyncError) -> StorageSetupError {
    StorageSetupError::Internal(Box::new(SetupCleanup { operation, cleanup }))
}

/// Settle a setup a crash cut, as the store opens (§11.6). One before its
/// point of no return is undone: a creation's store key and its credentials
/// go, and `coven_operations` lists it undone. One past it stays for the first connection to finish.
/// Credentials a finished setup left behind go too. Answers whether a setup
/// is finishing.
pub(crate) async fn settle_opened(
    database: &coven_database::Database,
    keychain: &Arc<StoreKeychain>,
    keys: &Arc<KeySession<StoreKeyring>>,
) -> Result<bool, SyncError> {
    let running = SetupJournal::running(database).await?;
    if running.as_ref().is_some_and(|journal| journal.location().is_some()) {
        return Ok(true);
    }
    let creating = running.as_ref().is_some_and(SetupJournal::creating);
    let (kept, held) = (keychain.clone(), keys.clone());
    let bugs = database.bugs().clone();
    crate::task::blocking_completing("undo storage setup's local state", bugs, move || {
        // Before the point of no return nothing claimed the store key a
        // creation kept.
        if creating {
            held.forget()?;
        }
        if kept.setup_credentials()?.is_some() {
            kept.delete_setup_credentials()?;
        }
        Ok::<_, KeyError>(())
    })
    .await??;
    if let Some(journal) = running {
        journal.end(database, OperationOutcome::Undone(UndoCause::Interrupted)).await?;
    }
    Ok(false)
}

impl StorageConnections {
    /// Cancel this session's setup (E5): before its point of no return it
    /// undoes what it wrote; past it, it finishes.
    pub(crate) fn cancel_setup(&self) -> SetupCancellation {
        let answer = match &mut *lock_unpoisoned(&self.setup) {
            SetupRun::Idle => SetupCancellation::NoSetup,
            SetupRun::Checking { cancelled } => {
                *cancelled = true;
                SetupCancellation::Undoing
            }
            SetupRun::Finishing => SetupCancellation::Finishing,
        };
        self.setup_cancelled.notify_waiters();
        answer
    }

    /// Whether a setup is past its point of no return and not yet finished.
    pub(super) fn setup_finishing(&self) -> bool {
        matches!(*lock_unpoisoned(&self.setup), SetupRun::Finishing)
    }

    pub(super) fn check_cancel(&self) -> Result<(), StorageSetupError> {
        match *lock_unpoisoned(&self.setup) {
            SetupRun::Checking { cancelled: true } => Err(StorageSetupError::Cancelled),
            _ => Ok(()),
        }
    }

    /// Resolves once the app has cancelled the setup checking now.
    async fn cancelled(&self) {
        loop {
            let notified = self.setup_cancelled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.check_cancel().is_err() {
                return;
            }
            notified.await;
        }
    }

    /// Where a test stops or holds setup.
    pub(super) async fn reached(
        &self,
        #[cfg_attr(not(test), expect(unused_variables, reason = "only tests stop setup"))]
        at: SetupStop,
    ) -> Result<(), StorageError> {
        #[cfg(test)]
        {
            let hook = {
                let mut held = lock_unpoisoned(&self.setup_hook);
                match &*held {
                    Some(SetupHook::Crash(stop) | SetupHook::Pause { at: stop, .. })
                        if *stop == at =>
                    {
                        held.take()
                    }
                    _ => None,
                }
            };
            match hook {
                Some(SetupHook::Crash(_)) => {
                    return Err(StorageFailure::Network.with_source(Crashed));
                }
                Some(SetupHook::Pause { entered, resume, .. }) => {
                    entered.send(()).expect("the test waits for the pause");
                    resume.await.expect("the test resumes the pause");
                }
                None => {}
            }
        }
        Ok(())
    }

    /// Have the next setup stop or wait at one of its stops.
    #[cfg(test)]
    pub(crate) fn test_setup_hook(&self, hook: SetupHook) {
        *lock_unpoisoned(&self.setup_hook) = Some(hook);
    }

    /// Journal a setup before its first write to storage: its row, then
    /// `credentials`, kept apart from the committed ones (§11.6). Refused
    /// while another is past its point of no return.
    pub(super) async fn begin_setup(
        &self,
        device_name: &str,
        credentials: &StorageCredentials,
    ) -> Result<SetupJournal, StorageSetupError> {
        if self.setup_finishing() {
            return Err(StorageSetupError::SetupRunning);
        }
        let creating = self.codes.location().await.map_err(setup_error)?.is_none();
        let journal = SetupJournal::start(&self.database, creating, device_name)
            .await
            .map_err(setup_error)?;
        *lock_unpoisoned(&self.setup) = SetupRun::Checking { cancelled: false };
        let kept = credentials
            .encode()
            .map_err(StorageSetupError::from)
            .and_then(|bytes| Ok(self.keychain.set_setup_credentials(&bytes)?));
        if let Err(error) = kept {
            return Err(self.undo_setup(&journal, error).await);
        }
        self.reached(SetupStop::Journaled).await?;
        Ok(journal)
    }

    /// Run setup's `steps` under `journal`. A failure before the point of no
    /// return undoes setup. One after it leaves setup finishing, unless a
    /// refusal undid it, and starts the loop, whose connection finishes it
    /// (§11.6).
    pub(super) async fn run_setup<T>(
        self: &Arc<Self>,
        mut journal: SetupJournal,
        steps: impl AsyncFnOnce(&mut SetupJournal) -> Result<T, StorageSetupError>,
    ) -> Result<T, StorageSetupError> {
        let error = match steps(&mut journal).await {
            Ok(done) => return Ok(done),
            Err(error) => error,
        };
        #[cfg(test)]
        if crashed(&error) {
            return Err(error);
        }
        if journal.location().is_none() {
            return Err(self.undo_setup(&journal, error).await);
        }
        if self.setup_finishing() {
            if let Err(cleanup) = self.start_locked().await {
                return Err(with_cleanup(error, cleanup));
            }
        }
        Err(error)
    }

    /// Undo a setup before its point of no return: its credentials go, then
    /// its row ends cancelled, or undone with `error` (§11.6). Nothing it
    /// wrote to storage is state: a test object or a probe is space.
    async fn undo_setup(
        &self,
        journal: &SetupJournal,
        error: StorageSetupError,
    ) -> StorageSetupError {
        let outcome = match &error {
            StorageSetupError::Cancelled => OperationOutcome::Cancelled,
            error => OperationOutcome::Undone(UndoCause::Failed { error: error.to_string() }),
        };
        let undone = async {
            // Before the point of no return nothing claimed the store key a
            // creation kept.
            if journal.creating() {
                self.forget_keys()?;
            }
            self.keychain.delete_setup_credentials()?;
            journal.end(&self.database, outcome).await?;
            *lock_unpoisoned(&self.setup) = SetupRun::Idle;
            Ok::<_, SyncError>(())
        }
        .await;
        match undone {
            Ok(()) => error,
            Err(cleanup) => with_cleanup(error, cleanup),
        }
    }

    /// Pass setup's point of no return, claiming at `location`, unless the
    /// app has cancelled it: from here on a cancel is answered `Finishing`.
    async fn pass_point(
        &self,
        journal: &mut SetupJournal,
        location: StorageConfig,
    ) -> Result<(), StorageSetupError> {
        {
            let mut run = lock_unpoisoned(&self.setup);
            if matches!(*run, SetupRun::Checking { cancelled: true }) {
                return Err(StorageSetupError::Cancelled);
            }
            *run = SetupRun::Finishing;
        }
        if let Err(error) = journal.claim(&self.database, location).await {
            *lock_unpoisoned(&self.setup) = SetupRun::Checking { cancelled: false };
            return Err(setup_error(error));
        }
        Ok(())
    }

    /// Check and connect `config` reached through `auth`, under `journal`.
    /// A new OAuth sign-in is `sign_in`, the session custody `auth`
    /// refreshes in until setup commits it to device custody. Reconnecting
    /// then loads what other devices published; a cancel still undoes setup
    /// meanwhile. Then setup passes its point of no return and finishes.
    /// The setup call's own requests, its checks and its claims, go through
    /// a connection made beside the store's that takes the call's `waits`
    /// and sends rate-limited requests again while they allow; the store's
    /// connection, which the sequence, the files and the sync loop keep,
    /// never does (§20.8). Its steps go to `progress`, from `Connecting` on.
    pub(super) async fn setup_connection(
        &self,
        journal: &mut SetupJournal,
        config: StorageConfig,
        auth: ProviderAuth,
        sign_in: Option<Arc<SessionSignIn>>,
        waits: SetupWaits,
        progress: &mut ProgressReport<StorageSetupStep>,
    ) -> Result<StorageSetup, StorageSetupError> {
        progress.begin(StorageSetupStep::Connecting, 1);
        let credentials = auth.credentials();
        let storage = self
            .connector
            .connect(
                config.clone(),
                auth,
                self.device,
                self.state.clone(),
                self.waits.clone(),
                self.transfers.clone(),
            )
            .await?;
        let setup = Arc::new(storage.during_setup(waits));
        if !journal.creating() {
            setup.check_location_claim(self.store()).await?;
        }
        let path = ObjectPath::file(self.store(), self.device, FileId(self.ids.new_id()));
        let plaintext = b"coven storage check";
        let header = coven_format::file::FileHeader::new(plaintext.len() as u64);
        let key = coven_crypto::FileKey::generate()
            .map_err(|error| StorageSetupError::Internal(Box::new(error)))?;
        let mut bytes = header.encode().to_vec();
        bytes.extend(
            header
                .seal_chunk(&key, path.as_str(), 0, plaintext)
                .map_err(|error| StorageSetupError::Internal(Box::new(error)))?,
        );
        {
            let _permit = self.state.permit_provider_check(&path);
            coven_storage::check_provider(setup.as_ref(), &path, &bytes).await?;
        }
        progress.done(1);
        self.reached(SetupStop::Checked).await?;
        self.check_cancel()?;
        if journal.creating() {
            // A custody failure here still undoes setup with nothing claimed.
            self.keep_creation_key().map_err(setup_error)?;
            self.reached(SetupStop::KeyKept).await?;
        }
        let mut live = self.live.lock().await;
        let sync = self.sequence(storage.clone(), &live).await.map_err(setup_error)?;
        // The claims, and the catch-up an existing store needs, are the
        // setup call's own requests, sent on its connection.
        let claiming = Arc::new(sync.reconnect(setup.clone()));
        if !journal.creating() {
            let relayed = std::sync::Mutex::new(&mut *progress);
            let loading = claiming.subscribe_progress().forward(claiming.unlock(), |load| {
                lock_unpoisoned(&relayed).relay(setup_step::loading(load))
            });
            // Loading claims nothing, so a cancel still undoes setup.
            tokio::select! {
                biased;
                () = self.cancelled() => return Err(StorageSetupError::Cancelled),
                loaded = loading => loaded.map_err(setup_error)?,
            }
        }
        self.pass_point(journal, config.clone()).await?;
        self.reached(SetupStop::Claiming).await?;
        let restore_code = self
            .finish_setup(
                &mut live,
                journal,
                sync,
                claiming,
                FinishingConnections { storage, setup },
                credentials,
                sign_in,
                false,
                true,
                progress,
            )
            .await
            .map_err(setup_error)?;
        Ok(StorageSetup {
            connection: ConnectedStorage { storage: config, key_state: StoreKeyState::Available },
            restore_code,
        })
    }

    /// Finish a setup a crash cut past its point of no return, connecting
    /// with the credentials it kept, as the first connection after opening
    /// does (§11.6). An OAuth sign-in refreshes in this session's custody
    /// until finishing commits it.
    pub(super) async fn resume_setup(
        &self,
        live: &mut ConnectionState,
        journal: SetupJournal,
    ) -> Result<(), SyncError> {
        let location = journal
            .location()
            .ok_or(DbError::Invariant { what: "a finishing setup is past its point of no return" })?
            .clone();
        let bytes = self
            .keychain
            .setup_credentials()?
            .ok_or(DbError::Invariant { what: "a finishing setup keeps its credentials" })?;
        let credentials = StorageCredentials::decode(bytes.as_bytes())?;
        let (auth, sign_in) = match &credentials {
            StorageCredentials::S3(keys) => (ProviderAuth::S3(keys.clone()), None),
            StorageCredentials::CloudKit => (ProviderAuth::CloudKit, None),
            StorageCredentials::OAuth(tokens) => {
                let sign_in = SessionSignIn::new(tokens.clone(), self.clients.clone());
                let auth = ProviderAuth::OAuth { tokens: tokens.clone(), custody: sign_in.clone() };
                (auth, Some(sign_in))
            }
        };
        let storage = self
            .connector
            .connect(
                location,
                auth,
                self.device,
                self.state.clone(),
                self.waits.clone(),
                self.transfers.clone(),
            )
            .await?;
        let sync = self.sequence(storage.clone(), live).await?;
        let claiming = Arc::new(sync.reconnect(storage.clone()));
        let mut progress =
            ProgressReport::new(tokio::sync::watch::channel(None).0, self.clock.clone());
        self.finish_setup(
            live,
            &journal,
            sync,
            claiming,
            FinishingConnections { storage: storage.clone(), setup: storage },
            credentials,
            sign_in,
            true,
            false,
            &mut progress,
        )
        .await
        .map(drop)
    }

    /// Past the point of no return: claim what creating the store or a
    /// changed access needs on the candidate's own connection, commit its
    /// store connection, end the
    /// journal row with its outcome and install `sync`, starting its loop
    /// when `start` (§11.6). A reconnect resumed after a crash catches up
    /// first, when `catch_up`. A claim refused before it landed undoes
    /// setup.
    #[expect(
        clippy::too_many_arguments,
        reason = "the call and a crash's resumption differ in each"
    )]
    async fn finish_setup(
        &self,
        live: &mut ConnectionState,
        journal: &SetupJournal,
        sync: Arc<SequenceSync>,
        claiming: Arc<SequenceSync>,
        candidate: FinishingConnections,
        credentials: StorageCredentials,
        sign_in: Option<Arc<SessionSignIn>>,
        catch_up: bool,
        start: bool,
        progress: &mut ProgressReport<StorageSetupStep>,
    ) -> Result<SecretText, SyncError> {
        let location = journal
            .location()
            .ok_or(DbError::Invariant { what: "a finishing setup is past its point of no return" })?
            .clone();
        let provider = location.provider();
        let FinishingConnections { storage, setup } = candidate;
        let access = member_access(&credentials, &setup).await?;
        // The old provider remains installed until every fallible step
        // succeeds. Its stopped or started state survives a refused claim.
        let prepared = Self::with_passes_held(live, async {
            let claimed = if journal.creating() {
                progress.begin(StorageSetupStep::CreatingStore, 1);
                match setup.claim_location(self.store()).await {
                    Ok(()) => {
                        self.claim_creation(&claiming, access, journal.device_name(), progress)
                            .await
                    }
                    Err(error) => Err(error.into()),
                }
            } else {
                self.claim_access(&claiming, access, catch_up, progress).await
            };
            let slot = match claimed {
                Ok(slot) => slot,
                Err(error) if refused(&error) => {
                    return Err(self.refuse_setup(journal, error).await)
                }
                Err(error) => return Err(error),
            };
            self.reached(SetupStop::Created).await?;
            let (commit, restore_code) = self
                .codes
                .prepare_setup(ConnectionCredentials { location, credentials }, sign_in)
                .await?;
            // Claims may use file transfers; only the custody commit runs
            // inside the transfer exclusion needed to replace their provider.
            self.files.set_storage(Some(storage), async { commit().map(drop) }).await?;
            self.reached(SetupStop::Committed).await?;
            Ok::<_, SyncError>((slot, restore_code))
        })
        .await;
        let (slot, restore_code) = prepared?;
        let outcome = match slot {
            Some(slot) => OperationOutcome::Landed(slot),
            None => OperationOutcome::Satisfied,
        };
        journal.end(&self.database, outcome).await?;
        *lock_unpoisoned(&self.setup) = SetupRun::Idle;
        self.reached(SetupStop::Finished).await?;
        // The row has ended, so these are credentials nothing uses; opening
        // deletes them if this fails.
        if let Err(error) = self.keychain.delete_setup_credentials() {
            tracing::warn!("a finished setup's own credentials can't be deleted yet: {error}");
        }
        let released = self.close_live(live).await;
        let started = self.activate(live, sync, start).await;
        // Custody and provider are committed, and setup read the store to its
        // end. Worker failures now belong to the sync state; setup must
        // return the connection it published. A row that can't be written
        // has failed the handle, which every later call reports; what it
        // would have recorded goes to the log.
        let recorded = coven_database::RecordedLocation::Connected(provider);
        if let Err(written) = self.record_location(recorded, Some(self.clock.now())).await {
            tracing::warn!("storage set up after setup can't be recorded: {written}");
        }
        let result = match (released, started) {
            (Ok(()), result) | (result, Ok(())) => result,
            (Err(operation), Err(cleanup)) => Err(SyncError::cleanup(operation, cleanup)),
        };
        if let Err(error) = result {
            let failure = LastPass::failed(error);
            if let Err(written) = self.show(failure.clone()).await {
                tracing::warn!(
                    ?failure,
                    "how sync failed after setup can't be recorded: {written}"
                );
            }
        }
        Ok(restore_code)
    }

    /// Create the store, once its location is claimed where it holds one
    /// store (§20.3): keep its first store key, then claim slot 1 and the
    /// creator's key copy (§10.3). A crash may have left the creation journaled or
    /// applied: it is settled first, and an applied one claims nothing more.
    async fn claim_creation(
        &self,
        sync: &Arc<SequenceSync>,
        access: coven_format::MemberAccess,
        device_name: &str,
        progress: &mut ProgressReport<StorageSetupStep>,
    ) -> Result<Option<coven_merge::Slot>, SyncError> {
        let member = self.identity.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let ring = self.keep_creation_key()?;
        let key = ring.store_key_ids().next().ok_or(StorageFailure::MemberKeysMissing)?;
        let key_hash = ring.store_key(key)?.hash();
        sync.settle_operations().await?;
        if self.database.store_state().await?.store.is_none() {
            sync.administer_at(coven_format::admin::StoreChange::CreateStore {
                store: self.store(),
                name: self.database.store_name().await.map_err(SyncError::from)?,
                admin: member.keys().public_keys(),
                access,
                device_name: device_name.into(),
                key,
                key_hash,
            })
            .await?;
        }
        progress.done(1);
        Ok(Some(coven_merge::Slot(1)))
    }

    /// The store key a creation's slot 1 introduces, kept in custody before
    /// setup's point of no return: the creator's sequence owner seals slot 1
    /// and its key copy with the store key custody holds. One kept by this
    /// setup, or by one a crash cut, is the same key.
    fn keep_creation_key(&self) -> Result<StoreKeyring, SyncError> {
        let ring = match self.with_keys(|keys| keys.read())? {
            Some(ring) => ring,
            None => StoreKeyring::new(coven_crypto::StoreKey::generate(KeyId(self.ids.new_id()))?),
        };
        self.persist_keys(&ring)?;
        Ok(ring)
    }

    /// Record changed access for an existing device (§10.1, §14.2), once a
    /// reconnect resumed after a crash has caught up, when `catch_up`.
    async fn claim_access(
        &self,
        sync: &Arc<SequenceSync>,
        access: coven_format::MemberAccess,
        catch_up: bool,
        progress: &mut ProgressReport<StorageSetupStep>,
    ) -> Result<Option<coven_merge::Slot>, SyncError> {
        if catch_up {
            sync.unlock().await?;
        }
        let member = self.identity.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let state = self.database.store_state().await?;
        match access_change(&state, member.member(), access)? {
            Some(change) => {
                let recording = sync.administer_at(change);
                setup_step::single(progress, StorageSetupStep::RecordingAccess, recording).await
            }
            None => Ok(None),
        }
    }

    /// Undo a setup whose claim was refused before anything it claimed
    /// landed: the store key it kept for a creation no claim can still use
    /// goes, then its credentials, and its row ends with the refusal
    /// (§11.2, §11.6).
    async fn refuse_setup(&self, journal: &SetupJournal, error: SyncError) -> SyncError {
        let outcome = match &error {
            SyncError::Rejected(reason) => OperationOutcome::Rejected(reason.clone()),
            SyncError::StoreOwner => OperationOutcome::Rejected(OperationRejection::StoreOwner),
            error => OperationOutcome::Undone(UndoCause::Failed { error: error.to_string() }),
        };
        let undone = async {
            if journal.creating()
                && self.database.current_claim().await?.is_none()
                && self.database.store_state().await?.store.is_none()
            {
                self.forget_keys()?;
            }
            self.keychain.delete_setup_credentials()?;
            journal.end(&self.database, outcome).await?;
            *lock_unpoisoned(&self.setup) = SetupRun::Idle;
            Ok::<_, SyncError>(())
        }
        .await;
        match undone {
            Ok(()) => error,
            Err(cleanup) => SyncError::cleanup(error, cleanup),
        }
    }
}

/// Whether a claim past the point of no return was refused before it
/// landed: another store holds the location or slot 1, or the caught-up
/// history refuses the access change. Any other failure is retried.
fn refused(error: &SyncError) -> bool {
    match error {
        SyncError::Rejected(_) | SyncError::StoreOwner => true,
        SyncError::Storage(error) => matches!(
            error.failure(),
            StorageFailure::StoreExists | StorageFailure::OwnLocationRequired
        ),
        _ => false,
    }
}

#[cfg(test)]
#[path = "storage_setup_tests.rs"]
mod tests;
