//! A new Dropbox shared folder for the picker (§20.3). Dropbox may run the
//! share as a job that outlasts the call, the connection and the app, so each
//! name an account asks for is journaled before its share request, and a
//! later call or the next open goes on from the journal. None of it holds
//! the store's call lock: a close stops it.

use super::*;
use coven_crypto::custody::StoreKeychain;
use coven_database::StoreDatabase;
use coven_storage::providers::{DropboxName, DropboxShare, DropboxShareJob, StorageFolder};
use coven_sync::{DropboxFolderJournal, DropboxFolderStep};
use std::collections::HashMap;

/// Whether folders may be made now, which running work watches.
#[derive(Clone, Copy, Default)]
struct Stops {
    closed: bool,
    /// How many committed setups have ended the folders being made.
    setups: u64,
}

/// The folders this session is making.
pub(super) struct FolderCreations {
    stops: tokio::sync::watch::Sender<Stops>,
    /// Held shared by each piece of work while it runs, and alone by what
    /// stops them all.
    running: tokio::sync::RwLock<()>,
    /// One piece of work at a time makes the folder of a name, by the name
    /// in lower case, as Dropbox matches names.
    names: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Held while the kept sign-in and the rows that need it change
    /// together.
    sign_in: tokio::sync::Mutex<()>,
}

/// Why folder work stopped before its end.
enum Stopped {
    Closed,
    SetUp,
}

impl FolderCreations {
    pub(super) fn new() -> Self {
        Self {
            stops: tokio::sync::watch::channel(Stops::default()).0,
            running: Default::default(),
            names: Default::default(),
            sign_in: Default::default(),
        }
    }

    /// Run `work` until it ends, or until something stops all folder work.
    async fn run<T>(&self, work: impl Future<Output = T>) -> Result<T, Stopped> {
        let _running = self.running.read().await;
        let mut stops = self.stops.subscribe();
        let before = *stops.borrow_and_update();
        if before.closed {
            return Err(Stopped::Closed);
        }
        tokio::select! {
            biased;
            stop = stops.wait_for(|now| now.closed || now.setups != before.setups) => {
                match stop {
                    Ok(now) if !now.closed => Err(Stopped::SetUp),
                    Ok(_) | Err(_) => Err(Stopped::Closed),
                }
            }
            done = work => Ok(done),
        }
    }

    /// Stop all folder work for good, and wait until it has stopped.
    pub(super) async fn close(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.stops.send_modify(|stops| stops.closed = true);
        self.running.write().await
    }

    fn name(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        lock_unpoisoned(&self.names).entry(name.to_lowercase()).or_default().clone()
    }
}

/// The sharing jobs the journal holds as a store opens, with the sign-in
/// kept to follow them.
pub(crate) struct OpenedFolders {
    jobs: Vec<DropboxFolderJournal>,
    sign_in: Option<coven_crypto::SecretBytes>,
}

/// Read the sharing jobs to follow as the store opens. A kept sign-in no job
/// needs, as a crash can leave, goes.
pub(crate) async fn settle_opened_folders(
    database: &StoreDatabase,
    keychain: &Arc<StoreKeychain>,
) -> Result<OpenedFolders, SyncError> {
    let mut jobs = DropboxFolderJournal::all(database).await?;
    jobs.retain(|row| matches!(row.step(), DropboxFolderStep::Job { .. }));
    let kept = keychain.clone();
    let needed = !jobs.is_empty();
    let bugs = database.bugs().clone();
    let sign_in = crate::task::blocking_completing("read a folder's sign-in", bugs, move || {
        let sign_in = kept.dropbox_folder_credentials()?;
        if needed {
            return Ok::<_, KeyError>(sign_in);
        }
        if sign_in.is_some() {
            kept.delete_dropbox_folder_credentials()?;
        }
        Ok(None)
    })
    .await??;
    Ok(OpenedFolders { jobs, sign_in })
}

impl StorageConnections {
    /// Make a folder named `name` at the top of the signed-in Dropbox and
    /// share it, or go on with the one this account already asked for.
    pub(crate) async fn create_dropbox_shared_folder(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<StorageFolder, StorageSetupError> {
        self.with_keys(|_| Ok(()))?;
        let (owner, name) = (self.clone(), name.to_owned());
        let creating = async move {
            let work = async {
                let auth = {
                    let held = owner.authentication.lock().await;
                    let provider = CloudProvider::Dropbox;
                    let sign_in = held.as_ref().ok_or(OAuthError::Reauthorize(provider))?;
                    crate::authentication::held_auth(sign_in, provider)?
                };
                let one = owner.folder_creations.name(&name);
                let _one = one.lock().await;
                Box::pin(owner.make_folder(&name, auth)).await
            };
            match owner.folder_creations.run(work).await {
                Ok(made) => made,
                Err(Stopped::Closed) => Err(KeyError::StoreClosed.into()),
                Err(Stopped::SetUp) => Err(StorageSetupError::LocationChangeUnsupported),
            }
        };
        // A call the app drops goes on to its end (§11.6).
        crate::task::spawn_completing(
            "create a Dropbox folder",
            self.database.bugs().clone(),
            creating,
        )
        .await?
    }

    /// Make the folder `name` from where the journal says this account's
    /// request for it stands. The caller holds the name.
    async fn make_folder(
        &self,
        name: &str,
        auth: ProviderAuth,
    ) -> Result<StorageFolder, StorageSetupError> {
        // The lookup refuses a name no folder can have before any request.
        let found = self.connector.dropbox_name(name, auth.clone(), self.waits.clone()).await?;
        let account = self.dropbox_account(auth.clone()).await?;
        let row = DropboxFolderJournal::find(&self.database, &account, name)
            .await
            .map_err(setup_error)?;
        let Some(mut row) = row else {
            if found != DropboxName::Free {
                return Err(name_taken());
            }
            let row = DropboxFolderJournal::start(&self.database, &account, name)
                .await
                .map_err(setup_error)?;
            return self.share_folder(row, auth, true).await;
        };
        match row.step().clone() {
            DropboxFolderStep::Asked => self.take_or_share(row, found, auth).await,
            DropboxFolderStep::Job { job } => {
                // Kept again: this call's sign-in may be newer.
                self.keep_job(&mut row, job.clone(), &auth).await?;
                match self.follow_job(&mut row, &job, auth.clone()).await? {
                    Some(folder) => Ok(folder),
                    None => {
                        let found = self
                            .connector
                            .dropbox_name(name, auth.clone(), self.waits.clone())
                            .await?;
                        self.take_or_share(row, found, auth).await
                    }
                }
            }
            DropboxFolderStep::Made { shared_folder_id, folder_name } => match found {
                DropboxName::Taken { shared_folder_id: Some(id) } if id == shared_folder_id => {
                    Ok(StorageFolder::dropbox_shared(&folder_name, &id)?)
                }
                DropboxName::Taken { .. } => Err(name_taken()),
                // The person deleted or moved the folder since.
                DropboxName::Free => {
                    row.advance(&self.database, DropboxFolderStep::Asked)
                        .await
                        .map_err(setup_error)?;
                    self.share_folder(row, auth, false).await
                }
            },
        }
    }

    /// Go on with a request whose reply is unknown, with what now stands at
    /// its name: send it, or take the folder it left.
    async fn take_or_share(
        &self,
        mut row: DropboxFolderJournal,
        found: DropboxName,
        auth: ProviderAuth,
    ) -> Result<StorageFolder, StorageSetupError> {
        match found {
            DropboxName::Free => self.share_folder(row, auth, false).await,
            DropboxName::Taken { shared_folder_id } => {
                self.take_folder(&mut row, shared_folder_id, auth).await
            }
        }
    }

    /// Take the folder at `row`'s name when it is as the row's request
    /// leaves one. The row stays otherwise: Dropbox may not have finished
    /// the request.
    async fn take_folder(
        &self,
        row: &mut DropboxFolderJournal,
        shared_folder_id: Option<String>,
        auth: ProviderAuth,
    ) -> Result<StorageFolder, StorageSetupError> {
        let Some(id) = shared_folder_id else {
            return Err(name_taken());
        };
        let untouched = self
            .connector
            .untouched_dropbox_folder(row.name(), &id, auth, self.waits.clone())
            .await?;
        match untouched {
            Some(folder) => self.folder_made(row, folder).await,
            None => Err(name_taken()),
        }
    }

    /// Send `row`'s share request, journaled before this. `first` is the
    /// row's first request: a refusal of it, or a shared folder already at
    /// the name, means the row's account made nothing, and ends it.
    async fn share_folder(
        &self,
        mut row: DropboxFolderJournal,
        auth: ProviderAuth,
        first: bool,
    ) -> Result<StorageFolder, StorageSetupError> {
        let shared =
            self.connector.share_dropbox_folder(row.name(), auth.clone(), self.waits.clone()).await;
        match shared {
            Ok(DropboxShare::Shared(folder)) => self.folder_made(&mut row, folder).await,
            Ok(DropboxShare::Job(job)) => {
                self.keep_job(&mut row, job.clone(), &auth).await?;
                self.follow_job(&mut row, &job, auth).await?.ok_or_else(|| {
                    StorageFailure::Protocol
                        .with_source("Dropbox forgot a sharing job it had just launched")
                        .into()
                })
            }
            Ok(DropboxShare::AlreadyShared) if first => {
                row.end(&self.database).await.map_err(setup_error)?;
                Err(name_taken())
            }
            // An earlier request of this row made the folder meanwhile.
            Ok(DropboxShare::AlreadyShared) => {
                let found = self
                    .connector
                    .dropbox_name(row.name(), auth.clone(), self.waits.clone())
                    .await?;
                match found {
                    DropboxName::Taken { shared_folder_id } => {
                        self.take_folder(&mut row, shared_folder_id, auth).await
                    }
                    DropboxName::Free => Err(name_taken()),
                }
            }
            Err(error) => {
                if first && error.refused() {
                    row.end(&self.database).await.map_err(setup_error)?;
                }
                Err(error.into())
            }
        }
    }

    /// Keep `auth`'s sign-in, then journal that Dropbox runs `row`'s request
    /// as `job`.
    async fn keep_job(
        &self,
        row: &mut DropboxFolderJournal,
        job: String,
        auth: &ProviderAuth,
    ) -> Result<(), StorageSetupError> {
        let bytes = auth.credentials().encode()?;
        let _kept = self.folder_creations.sign_in.lock().await;
        self.keychain.set_dropbox_folder_credentials(&bytes)?;
        row.advance(&self.database, DropboxFolderStep::Job { job }).await.map_err(setup_error)
    }

    /// Follow `row`'s job to its end: its folder, or `None` when Dropbox no
    /// longer knows the job, which leaves the request's reply unknown. A
    /// failed job made nothing and ends the row; a status check that fails
    /// leaves the job journaled.
    async fn follow_job(
        &self,
        row: &mut DropboxFolderJournal,
        job: &str,
        auth: ProviderAuth,
    ) -> Result<Option<StorageFolder>, StorageSetupError> {
        match self.connector.follow_dropbox_share(job, auth, self.waits.clone()).await? {
            DropboxShareJob::Shared(folder) => self.folder_made(row, folder).await.map(Some),
            DropboxShareJob::Failed(failure) => {
                row.end(&self.database).await.map_err(setup_error)?;
                self.forget_unneeded_sign_in().await.map_err(setup_error)?;
                Err(failure.into())
            }
            DropboxShareJob::Forgotten => {
                row.advance(&self.database, DropboxFolderStep::Asked).await.map_err(setup_error)?;
                self.forget_unneeded_sign_in().await.map_err(setup_error)?;
                Ok(None)
            }
        }
    }

    /// Journal `folder` as what `row`'s request made.
    async fn folder_made(
        &self,
        row: &mut DropboxFolderJournal,
        folder: StorageFolder,
    ) -> Result<StorageFolder, StorageSetupError> {
        let StorageConfig::Dropbox {
            location: DropboxLocation::SharedFolder { namespace_id }, ..
        } = &folder.storage
        else {
            return Err(StorageFailure::Protocol
                .with_source("a new Dropbox folder is a shared folder")
                .into());
        };
        let step = DropboxFolderStep::Made {
            shared_folder_id: namespace_id.clone(),
            folder_name: folder.name.clone(),
        };
        row.advance(&self.database, step).await.map_err(setup_error)?;
        self.forget_unneeded_sign_in().await.map_err(setup_error)?;
        Ok(folder)
    }

    /// Delete the kept sign-in once no row has a job to follow.
    async fn forget_unneeded_sign_in(&self) -> Result<(), SyncError> {
        let _kept = self.folder_creations.sign_in.lock().await;
        let rows = DropboxFolderJournal::all(&self.database).await?;
        if !rows.iter().any(|row| matches!(row.step(), DropboxFolderStep::Job { .. })) {
            self.keychain.delete_dropbox_folder_credentials()?;
        }
        Ok(())
    }

    async fn dropbox_account(&self, auth: ProviderAuth) -> Result<String, StorageError> {
        let account = self.connector.signed_in_account(auth, self.waits.clone()).await?;
        account.id.ok_or_else(|| {
            StorageFailure::Protocol.with_source("Dropbox named no id for the signed-in account")
        })
    }

    /// Follow the jobs the store opened with, each as its own work that
    /// nobody waits for. A job Dropbox or its sign-in fails stays journaled
    /// for the next call that asks for the folder, or the next open; any
    /// other failure is a bug, which goes to the app's sink.
    pub(crate) fn resume_folder_creations(self: &Arc<Self>, opened: OpenedFolders) {
        let OpenedFolders { jobs, sign_in } = opened;
        for row in jobs {
            let bytes = sign_in
                .as_ref()
                .map(|bytes| coven_crypto::SecretBytes::new(bytes.as_bytes().to_vec()));
            let owner = self.clone();
            let resuming = async move {
                let work = Box::pin(owner.resume_folder(row, bytes));
                match owner.folder_creations.run(work).await {
                    Ok(resumed) => resumed.or_else(left_journaled),
                    Err(Stopped::Closed | Stopped::SetUp) => Ok(()),
                }
            };
            let bugs = self.database.bugs().clone();
            // The task runs to its end though nobody waits for it, and the
            // bug its failure carries goes to the sink.
            drop(crate::task::spawn_completing(RESUMING, bugs, resuming));
        }
    }

    async fn resume_folder(
        &self,
        row: DropboxFolderJournal,
        sign_in: Option<coven_crypto::SecretBytes>,
    ) -> Result<(), StorageSetupError> {
        // The sign-in is kept before its job is journaled.
        let sign_in =
            sign_in.ok_or_else(|| resume_bug("a journaled sharing job has no kept sign-in"))?;
        let decoded = StorageCredentials::decode(sign_in.as_bytes())
            .map_err(|error| resume_bug(format!("a kept sign-in can't be read: {error}")))?;
        let StorageCredentials::OAuth(tokens) = decoded else {
            return Err(resume_bug("a folder's kept sign-in is not an OAuth sign-in"));
        };
        let sign_in = SessionSignIn::new(tokens, self.clients.clone());
        let auth = crate::authentication::held_auth(&sign_in, CloudProvider::Dropbox)?;
        // The account says whose job the kept sign-in can follow. Offline as
        // the store opens, it is asked again on the backoff of §6.7.
        let mut attempt = 0u32;
        let account = loop {
            match self.dropbox_account(auth.clone()).await {
                Ok(account) => break account,
                Err(error) if error.retryable() => {
                    self.clock.wait_out(coven_storage::retry_delay(attempt)).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        };
        // One sign-in is kept, the latest a job was followed with, so a job
        // another account asked for waits for a call by that account.
        if row.account() != account {
            tracing::warn!(
                folder = row.name(),
                asked_by = row.account(),
                signed_in = %account,
                "the kept sign-in reaches another account than a Dropbox sharing job's, which waits"
            );
            return Ok(());
        }
        let one = self.folder_creations.name(row.name());
        let _one = one.lock().await;
        // A call may have gone on with the row meanwhile.
        let row = DropboxFolderJournal::find(&self.database, &account, row.name())
            .await
            .map_err(setup_error)?;
        let Some(mut row) = row else {
            return Ok(());
        };
        if let DropboxFolderStep::Job { job } = row.step().clone() {
            self.follow_job(&mut row, &job, auth).await?;
        }
        Ok(())
    }

    /// A storage setup committed: stop making folders, end every folder's
    /// row, and delete the sign-in kept for them.
    pub(super) async fn end_folder_creations(&self) -> Result<(), SyncError> {
        self.folder_creations.stops.send_modify(|stops| stops.setups += 1);
        let _stopped = self.folder_creations.running.write().await;
        for row in DropboxFolderJournal::all(&self.database).await? {
            row.end(&self.database).await?;
        }
        self.keychain.delete_dropbox_folder_credentials()?;
        Ok(())
    }
}

/// What following a journaled job is called in a bug and its task.
const RESUMING: &str = "follow a Dropbox sharing job";

fn resume_bug(message: impl Into<String>) -> StorageSetupError {
    StorageSetupError::Bug(crate::task::Bug::new(RESUMING, message))
}

/// A resumed follow's failure. Dropbox's and the sign-in's are expected, as
/// when the person has signed coven out: the job stays journaled. Any other
/// is returned as a bug.
fn left_journaled(error: StorageSetupError) -> Result<(), StorageSetupError> {
    if crate::task::CarriesBug::bug(&error).is_some() {
        return Err(error);
    }
    match error {
        StorageSetupError::Storage(_) | StorageSetupError::OAuth(_) => {
            tracing::warn!("a Dropbox sharing job can't be followed yet: {error}");
            Ok(())
        }
        error => Err(resume_bug(format!("{error:?}"))),
    }
}

fn name_taken() -> StorageSetupError {
    StorageFailure::AlreadyExists.with_source("the Dropbox folder name is already taken").into()
}
