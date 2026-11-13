//! Replace this device's installation of a store with a new one, loaded from
//! storage beside the old one until it is ready (§19.3).

use super::*;

/// The app's step on a restored installation before it swaps in: one bulk
/// write of device-local tables on the loaded database (E10).
pub(crate) type Prepare = Box<dyn FnOnce(BulkSqlContext<'_>) -> CovenResult<()> + Send>;

/// Which call replaces the installation: a reset, or a restore with the
/// app's step on the new installation, which a resumed restore past its
/// record no longer has to run.
pub(super) enum Replacing {
    Reset,
    Restore(Option<Prepare>),
}

impl Replacing {
    fn by(&self) -> ReplacedBy {
        match self {
            Self::Reset => ReplacedBy::Reset,
            Self::Restore(_) => ReplacedBy::Restore,
        }
    }
}

/// Restore this device's installation of `store` from the person's current
/// restore code, in place of the installation it holds (§13.1). The code must
/// name this store; its member keys are checked against history before the
/// store or keychain changes. A device whose custody holds the code's pair and whose
/// applied history binds it has nothing to restore, which is also what a
/// retry finds after an interruption once the restore was published: the
/// store simply opens. `prepare` runs on the new installation before it
/// swaps in.
pub(crate) async fn restore_device(
    mut builder: CovenBuilder,
    store: StoreId,
    code: &str,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    prepare: Prepare,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let code = coven_sync::read_restore_code(code)?;
    CodeError::require_store(store, code.store)?;
    let storage = RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let (data, sign_in) = restore_connection(&builder, storage, s3_credentials)?;
    let directory = builder.directories.store_dir(&store);
    let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, store));
    // One identity session reads now and keeps the code's pair at publication.
    let identity = CovenBuilder::make_session(
        std::mem::replace(
            &mut builder.identity,
            SessionSource::Configured(CustodyChoice::InMemory),
        ),
        &directory,
        store,
        keychain,
    )?;
    builder.identity = SessionSource::Session(identity.clone());
    let pair = code.member_keys.public_keys();
    let held = identity
        .read()?
        .is_some_and(|held| held.member() == code.member && held.keys().public_keys() == pair);
    let unfinished = directory.unfinished_replacement().map_err(CovenError::from)?;
    if held && unfinished.is_none() {
        // Custody alone also holds a stale pair; the applied history decides.
        let database = builder.database(directory)?.open_read_only().await?;
        let state = database.store_state().await;
        database.close().await.map_err(CovenError::from)?;
        let bound = state?.active_member(&code.member).is_some_and(|member| member.keys == pair);
        if bound {
            return Ok(builder
                .open(store, |progress| status(BootstrapStatus::Opening(progress)))
                .await?);
        }
    }
    replace_installation(
        builder,
        store,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in: sign_in.map_or(SignInCustody::None, SignInCustody::Session),
            name: device_name.into(),
        },
        Replacing::Restore(Some(prepare)),
        status,
        cancel,
    )
    .await
}

/// Finish a replacement past its record from what it kept
/// (`CovenBuilder::resume_replacement`): a reset from the store's own
/// custody and the device name it kept (a connected reset whose record is
/// gone is refused, as a restore's is), a restore from the code and name it
/// kept, its S3 key inside the code, and its sign-in in the store's device
/// credentials or the builder's.
pub(crate) async fn resume_replacement(
    mut builder: CovenBuilder,
    store: StoreId,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let directory = builder.directories.store_dir(&store);
    let record = directory
        .unfinished_replacement()
        .map_err(CovenError::from)?
        .ok_or(BootstrapError::NoUnfinishedReplacement(store))?;
    let kept = StoreKeychain::new(builder.keychain()?, store).replacement_record()?;
    match record.by {
        ReplacedBy::Reset => {
            // An unconnected reset registers nothing, so it keeps no name
            // and ignores the one it is given. A connected reset keeps its
            // record before it registers: a missing one is lost, and the
            // fresh device is not registered under an empty name.
            let connected =
                coven_storage::recorded_config(&directory).map_err(SyncError::from)?.is_some();
            let name = match (connected, kept) {
                (false, _) => String::new(),
                (true, Some((_, name))) => name,
                (true, None) => return Err(BootstrapError::NoUnfinishedReplacement(store)),
            };
            Box::pin(reset::reset_device(builder, store, &name, status)).await
        }
        ReplacedBy::Restore => {
            let (text, name) = kept.ok_or(BootstrapError::NoUnfinishedReplacement(store))?;
            let code = coven_sync::read_restore_code(text.as_str())?;
            CodeError::require_store(store, code.store)?;
            let storage =
                RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
            let (data, sign_in) = match storage.location.provider() {
                provider if provider.signs_in_with_oauth() => {
                    let (custody, tokens) =
                        join_sign_in(&mut builder, store, &storage.location, None).await?;
                    let data = ConnectionCredentials {
                        location: storage.location,
                        credentials: StorageCredentials::OAuth(tokens),
                    };
                    data.validate().map_err(SyncError::from)?;
                    (data, custody)
                }
                _ => (restore_connection(&builder, storage, None)?.0, SignInCustody::None),
            };
            replace_installation(
                builder,
                store,
                BootstrapRequest::Restore { code, data, sign_in, name },
                Replacing::Restore(None),
                status,
                cancel,
            )
            .await
        }
    }
}

/// Load the new installation beside the old one, record the replacement and
/// register its fresh device, swap it into the store's directory, commit its
/// custody, settle the old installation's local state, then publish it. A
/// reset keeps the old local state for inspection; a restore removes it
/// (§13.1), and runs the app's step on the new installation before the
/// record. Until the record, the old installation and the store's custody
/// stay as they were, and the store opens as before. From the record on, the
/// store refuses opens and the replacement only moves forward: a failure,
/// a cancel or an interruption leaves it for the next call of the same kind
/// to finish, or for `resume_replacement`, and the other kind is refused.
/// Only the facts end it before the swap: its registration refused, or its
/// fresh device gone for good, removes the new installation and its record.
pub(super) async fn replace_installation(
    mut builder: CovenBuilder,
    store: StoreId,
    request: BootstrapRequest,
    call: Replacing,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let by = call.by();
    let prepare = match call {
        Replacing::Reset => None,
        Replacing::Restore(prepare) => prepare,
    };
    let keychain = builder.keychain()?;
    let entries = StoreKeychain::new(keychain.clone(), store);
    let replacement =
        builder.directories.begin_replacement(store, by, builder.ids.as_ref()).await?;
    let swapping = !replacement.is_swapped();
    let loaded = prepare_and_load(
        &mut builder,
        Installing::Replacing(&replacement),
        &request,
        prepare,
        &status,
        cancel,
    )
    .await;
    let Loaded { code, location, provider, credentials, sign_in, ring, database, waits, .. } =
        match loaded {
            Ok(Some(loaded)) => loaded,
            Ok(None) => unreachable!("only a join waits for approval"),
            Err(error) => return ended(&builder, replacement, &entries, error).await,
        };
    #[cfg(test)]
    if builder.interrupt_after_registering {
        // As if the process stopped here: nothing is undone.
        return combine(Err(BootstrapError::Cancelled), close(database).await);
    }
    let (database, waits) = if swapping {
        // Listings show this name until the replacement finishes.
        let name = database.store_name().await;
        // The loading database closes before its files move.
        database.close().await.map_err(CovenError::from)?;
        let name = name?;
        // Loaded, the fresh device is registered: past the record, a cancel
        // can't take it back (§11.6).
        if check_cancel(cancel).is_err() {
            return Err(BootstrapError::RestoreRegistered);
        }
        replacement.swap(name)?;
        #[cfg(test)]
        if builder.interrupt_after_swap {
            // As if the process stopped here: nothing is undone.
            return Err(BootstrapError::Cancelled);
        }
        open_swapped(&builder, &replacement, &status).await?
    } else {
        (database, waits)
    };
    status(BootstrapStatus::Committing);
    builder.authentication.take();
    let directory = replacement.directory();
    let owners = match builder.owners(
        directory.clone(),
        Arc::new(StoreKeychain::new(keychain, store)),
        crate::builder::Continuing::Fresh,
    ) {
        Ok(owners) => owners,
        Err(error) => return combine(Err(error.into()), close(database).await),
    };
    let name = match commit::loaded_name(&database).await {
        Ok(name) => name,
        Err(error) => return combine(Err(error), close(database).await),
    };
    // The swapped store's own publication follows, and moves only forward:
    // a commit that fails keeps what it wrote and the swap, for the next
    // call to finish.
    if let Err(error) = commit::commit(
        &directory,
        &code,
        &location,
        &name,
        &credentials,
        &sign_in,
        &ring,
        &owners,
        &commit::PublishedLater,
    ) {
        return combine(Err(error), close(database).await);
    }
    if by == ReplacedBy::Restore {
        if let Err(error) = replacement.remove_replaced().await {
            return combine(Err(error.into()), close(database).await);
        }
    }
    let finished = owners.clock.now();
    let location = coven_database::RecordedLocation::Connected(provider);
    let published = publish(&replacement, owners, database, waits, location, Some(finished)).await;
    // A published replacement no longer needs what it kept to finish.
    forget_kept(published, || entries.delete_replacement_record())
}

/// Run the app's step on the loaded installation, keep the code and the
/// device name, record the replacement, then keep its sign-in: all before
/// the fresh device registers (§19.3). A reader holding the store refuses
/// the record, and what was kept for it goes. A replacement an earlier call
/// recorded has its step's rows already, and keeps this call's code and
/// sign-in again.
pub(super) async fn record_replacement(
    replacement: &ReplacementStore,
    recording: &restore::Recording,
    database: &Database,
    prepare: Option<Prepare>,
    (member, store, data): (&MemberIdentity, StoreId, &ConnectionCredentials),
    sign_in: SignInCustody,
) -> Result<SignInCustody, BootstrapError> {
    if !replacement.is_recorded() {
        if let Some(prepare) = prepare {
            database.write_bulk(prepare).await?;
        }
        let name = database.store_name().await?;
        recording.keep(member, store, data)?;
        if let Err(error) = replacement.record(name) {
            return combine(Err(error.into()), recording.forget());
        }
    } else {
        recording.keep(member, store, data)?;
    }
    recording.keep_sign_in(sign_in)
}

/// End a replacement whose call failed or was cancelled before it swapped.
/// Before the record, a refusal no retry can change, or a cancel, removes the
/// new installation: the store stays exactly as it was; any other failure
/// keeps the loaded work for the next call. From the record on, it stays for
/// the next call unless the facts end it: a cancel, its registration
/// refused, or its member removed or its keys replaced, while no
/// registration of the fresh device is journaled and the history it applied
/// doesn't name the device as active, or names it as gone for good. Then
/// the record, what it kept and the new installation go, and the old
/// installation is the store again. A cancel the facts don't end is refused
/// with `RestoreRegistered` (§19.3, §11.6).
async fn ended(
    builder: &CovenBuilder,
    replacement: ReplacementStore,
    entries: &StoreKeychain,
    error: BootstrapError,
) -> Result<CovenHandle, BootstrapError> {
    if !replacement.is_recorded() {
        if !replacement.is_swapped() && abandons(&error) {
            return combine(
                combine(Err(error), entries.delete_replacement_record().map_err(Into::into)),
                replacement.cancel().map_err(Into::into),
            );
        }
        return Err(error);
    }
    let cancelled = matches!(error, BootstrapError::Cancelled);
    let ends = cancelled
        || matches!(
            error,
            BootstrapError::Sync(
                SyncError::Removed | SyncError::MemberKeysReplaced { .. } | SyncError::Rejected(_)
            )
        );
    if !ends || replacement.is_swapped() {
        return Err(error);
    }
    match restore::registered(builder, replacement.directory()).await {
        Ok(false) => {
            let directory = replacement.directory();
            let abandoned = replacement.abandon().map_err(BootstrapError::from);
            tracing::info!(store = %directory.id(), "the facts ended a recorded replacement");
            combine(
                combine(Err(error), abandoned),
                entries.delete_replacement_record().map_err(Into::into),
            )
        }
        Ok(true) if cancelled => Err(BootstrapError::RestoreRegistered),
        Ok(true) => Err(error),
        Err(read) => combine(Err(error), Err(read)),
    }
}

/// Publish the committed replacement and hand back its open store, with
/// `location` recorded and, for one loaded from storage, when it finished. A
/// failure before publication leaves the swap for the next call to finish.
pub(super) async fn publish(
    replacement: &ReplacementStore,
    owners: OpeningOwners,
    database: Database,
    waits: Arc<coven_sync::RetryWaits>,
    location: coven_database::RecordedLocation<CloudProvider>,
    finished: Option<std::time::SystemTime>,
) -> Result<CovenHandle, BootstrapError> {
    let configured = location.is_connected();
    if let Err(error) =
        crate::storage::record_opened(&database, &owners.clock, location, finished).await
    {
        return combine(Err(error.into()), close(database).await);
    }
    match replacement.publish() {
        Ok(()) => Ok(owners.into_handle(database, waits, configured, false)),
        Err(error) if error.published_unsynced() => Err(BootstrapError::Published {
            handle: owners.into_handle(database, waits, configured, false),
            source: Box::new(error.into()),
        }),
        Err(error) => combine(Err(error.into()), close(database).await),
    }
}

/// A refusal no retry can change, or cancellation, removes a replacement
/// not yet recorded: the store stays exactly as it was.
fn abandons(error: &BootstrapError) -> bool {
    matches!(
        error,
        BootstrapError::Cancelled
            | BootstrapError::Sync(
                SyncError::MemberKeysReplaced { .. }
                    | SyncError::Removed
                    | SyncError::Rejected(coven_sync::OperationRejection::TargetGone(
                        coven_sync::EntryTarget::Member(_)
                    ))
            )
    )
}

/// Open the swapped installation's database in the store's directory.
pub(super) async fn open_swapped(
    builder: &CovenBuilder,
    replacement: &ReplacementStore,
    status: &impl Fn(BootstrapStatus),
) -> Result<(Database, Arc<coven_sync::RetryWaits>), BootstrapError> {
    let database = open_installation(builder.database(replacement.directory())?, status).await?;
    match coven_sync::RetryWaits::open(database.clone(), builder.clock.clone()).await {
        Ok(waits) => Ok((database, Arc::new(waits))),
        Err(error) => combine(Err(CovenError::from(error).into()), close(database).await),
    }
}

async fn close(database: Database) -> Result<(), BootstrapError> {
    close_database(&database).await
}

#[cfg(test)]
#[path = "bootstrap_replace_tests.rs"]
mod tests;
