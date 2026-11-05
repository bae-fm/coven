//! A person's new device restoring from their restore code (§13.1): from a
//! code or a saved code; the record it keeps on the device from just before
//! it registers the device until it ends; the listing of unfinished
//! restores, resuming one from its record, and how a call that ends without
//! the store settles it (§11.6).

use super::*;
use coven_sync::DeviceCredentials;

/// Restore this person's store on a new device, returning its open handle.
/// S3 requires a key: the one the code carries, or one entered separately,
/// never both. Other providers require None and use the builder's own
/// sign-in. Invalid key inputs fail before bootstrap begins. A store whose
/// unfinished installation holds a join's saved code is refused with
/// `StoreLockError::BootstrapPending` before anything changes (§13.4). Until
/// the restore records itself, a cancel or a failure removes the
/// installation it loaded into (§13.1).
pub async fn restore_from_code(
    builder: CovenBuilder,
    code: &str,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let code = coven_sync::read_restore_code(code)?;
    let storage = RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let (data, sign_in) = restore_connection(&builder, storage, s3_credentials)?;
    let sign_in = sign_in.map_or(SignInCustody::None, SignInCustody::Session);
    Box::pin(bootstrap_device(
        builder,
        BootstrapRequest::Restore { code, data, sign_in, name: device_name.into() },
        on_status,
        cancel,
    ))
    .await?
    .ok_or_else(|| SyncError::from(StorageFailure::MemberKeysMissing).into())
}

/// Restore a readable entry selected from `saved_restore_codes`. Reads the current
/// code by store id, including a replacement saved since listing. Returns None
/// if that store's code is no longer saved.
/// S3 uses the explicit key when supplied, otherwise its key saved beside the code.
/// Missing keys, wrong-provider keys and keychain failures precede bootstrap.
pub async fn restore_from_saved_code(
    builder: CovenBuilder,
    saved: &SavedRestoreCode,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    check_cancel(cancel)?;
    let person = PersonEntries::new(builder.keychain()?, saved.store_id);
    let Some((code, location)) = keychain_code(&person, saved.store_id)? else {
        return Ok(None);
    };
    let s3_credentials = if location.provider() == CloudProvider::S3 && s3_credentials.is_none() {
        person
            .s3_credentials()?
            .map(|bytes| S3Credentials::decode(bytes.as_bytes()).map_err(SyncError::from))
            .transpose()?
    } else {
        s3_credentials
    };
    // The saved code carries no key; the person entries hold it beside it.
    let storage = RestoreStorage::location_only(location);
    let (data, sign_in) = restore_connection(&builder, storage, s3_credentials)?;
    let sign_in = sign_in.map_or(SignInCustody::None, SignInCustody::Session);
    Box::pin(bootstrap_device(
        builder,
        BootstrapRequest::Restore { code, data, sign_in, name: device_name.into() },
        on_status,
        cancel,
    ))
    .await
}

#[expect(
    clippy::map_err_ignore,
    reason = "read_saved_code logs why a saved code is unreadable; restoring sees Invalid"
)]
pub(super) fn keychain_code(
    keychain: &PersonEntries,
    store: StoreId,
) -> Result<Option<(RestoreCode, StorageConfig)>, BootstrapError> {
    keychain
        .restore_code()?
        .map(|bytes| {
            coven_sync::read_saved_code(store, bytes.as_bytes())
                .map(|(saved, location)| (saved.code, location))
                .map_err(|_| CodeError::Invalid.into())
        })
        .transpose()
}

/// What a new device's restore keeps on the device before its registration,
/// its first write to storage: from then on the restore only goes forward,
/// and finishes from what it kept (§13.1).
pub(super) struct Recording {
    entries: Arc<StoreKeychain>,
    credentials: Arc<DeviceCredentials>,
    name: String,
}

impl Recording {
    pub(super) fn new(
        builder: &CovenBuilder,
        store: StoreId,
        name: &str,
    ) -> Result<Self, BootstrapError> {
        let keychain = builder.keychain()?;
        Ok(Self {
            entries: Arc::new(StoreKeychain::new(keychain.clone(), store)),
            credentials: crate::builder::device_credentials(
                keychain,
                store,
                builder.directories.store_dir(&store),
                builder.oauth_exchange(),
            ),
            name: name.to_owned(),
        })
    }

    /// Record the restore: the code, holding the S3 key on S3, and the
    /// device's name, then on Dropbox and OneDrive the sign-in, which from
    /// now on refreshes in the store's device credentials. A record without
    /// its sign-in lists a restore that resumes once the person signs in;
    /// a sign-in is never kept without its record. Returns the custody the
    /// sign-in refreshes in.
    pub(super) fn record(
        &self,
        member: &MemberIdentity,
        store: StoreId,
        data: &ConnectionCredentials,
        sign_in: SignInCustody,
    ) -> Result<SignInCustody, BootstrapError> {
        let s3_key = match &data.credentials {
            StorageCredentials::S3(key) => Some(key.clone()),
            _ => None,
        };
        let code = RestoreCode {
            store,
            member: member.member(),
            member_keys: member.keys().clone(),
            storage: RestoreStorage { location: data.location.clone(), s3_key }
                .encode()
                .map_err(SyncError::from)?,
        };
        let text = SecretText::new(code.to_text().map_err(SyncError::from)?.to_string());
        self.entries.set_restore_record(&text, &self.name)?;
        Ok(match sign_in {
            SignInCustody::Session(session) => {
                session.publish(&self.credentials, |keychains, tokens| {
                    coven_sync::commit_credentials(
                        keychains,
                        &StorageCredentials::OAuth(tokens.clone()),
                        None,
                    )
                    .map(drop)
                })?;
                SignInCustody::Device { credentials: self.credentials.clone(), kept: false }
            }
            custody => custody,
        })
    }
}

/// The restores under way on this device (`CovenBuilder::unfinished_restores`).
pub(crate) async fn unfinished_restores(
    builder: &CovenBuilder,
) -> Result<Vec<CodeMetadata>, BootstrapError> {
    let keychain = builder.keychain()?;
    let mut restores = Vec::new();
    for store in builder
        .directories
        .unfinished_first_installations()
        .await
        .map_err(BootstrapDirectoryError::from)?
    {
        // A join keeps no record; an installation that keeps nothing is
        // reclaimed.
        let entries = StoreKeychain::new(keychain.clone(), store);
        let Kept::Restore(code) = kept(builder, &entries, store).await? else {
            continue;
        };
        let metadata = match coven_sync::decode_code_info(code.as_str())? {
            CodeInfo::Restore(metadata) => metadata,
            CodeInfo::Invite(_) => {
                return Err(CodeError::WrongKind {
                    expected: CodeKind::Restore,
                    actual: CodeKind::Invite,
                }
                .into())
            }
        };
        CodeError::require_store(store, metadata.store_id)?;
        restores.push(metadata);
    }
    Ok(restores)
}

/// Continue an unfinished restore from its record (`CovenBuilder::resume_restore`).
pub(crate) async fn resume_restore(
    builder: CovenBuilder,
    store: StoreId,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let (text, name) = StoreKeychain::new(builder.keychain()?, store)
        .restore_record()?
        .ok_or(BootstrapError::NoUnfinishedRestore(store))?;
    let code = coven_sync::read_restore_code(text.as_str())?;
    CodeError::require_store(store, code.store)?;
    let storage = RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let (data, sign_in) = match storage.location.provider() {
        // The sign-in the restore kept, or a new one the builder holds.
        provider if provider.signs_in_with_oauth() => {
            let (custody, tokens) = join_sign_in(&builder, store, &storage.location)?;
            let data = ConnectionCredentials {
                location: storage.location,
                credentials: StorageCredentials::OAuth(tokens),
            };
            data.validate().map_err(SyncError::from)?;
            (data, custody)
        }
        // The code holds the S3 key; CloudKit uses the Apple account.
        _ => {
            let (data, _) = restore_connection(&builder, storage, None)?;
            (data, SignInCustody::None)
        }
    };
    Box::pin(bootstrap_device(
        builder,
        BootstrapRequest::Restore { code, data, sign_in, name },
        on_status,
        cancel,
    ))
    .await?
    .ok_or_else(|| SyncError::from(StorageFailure::MemberKeysMissing).into())
}

/// End a restore whose call failed or was cancelled before it published the
/// store. Before the record the restore has only read storage, and a cancel
/// or any failure removes its installation (§11.6). Once the restore is
/// recorded it only goes forward, and stays for the next call, unless the
/// facts end it: a cancel, a registration refused, a member removed or its
/// keys replaced, while no registration is journaled and the history the
/// installation applied doesn't name the device as active, or names it as
/// gone for good. Then nothing in storage
/// stands for the device, and the restore removes everything it kept. A
/// cancel the facts don't end is refused with `RestoreRegistered` (§13.1,
/// §11.6).
pub(super) async fn ended(
    builder: &CovenBuilder,
    pending: &BootstrapStore,
    entries: &StoreKeychain,
    error: BootstrapError,
) -> Result<Option<CovenHandle>, BootstrapError> {
    let recorded = match entries.restore_record() {
        Ok(record) => record.is_some(),
        Err(read) => return combine(Err(error), Err(read.into())),
    };
    if !recorded {
        return remove_unfinished(entries, pending, Err(error));
    }
    let cancelled = matches!(error, BootstrapError::Cancelled);
    let ends = cancelled
        || matches!(
            error,
            BootstrapError::Sync(
                SyncError::Removed | SyncError::MemberKeysReplaced { .. } | SyncError::Rejected(_)
            )
        );
    if !ends {
        return Err(error);
    }
    match registered(builder, pending).await {
        Ok(false) => remove_unfinished(entries, pending, Err(error)),
        Ok(true) if cancelled => Err(BootstrapError::RestoreRegistered),
        Ok(true) => Err(error),
        Err(read) => combine(Err(error), Err(read)),
    }
}

/// Whether storage may name this installation's device as active. The
/// history the installation applied decides once it names the device: its
/// registration landed, and the device is active, or gone for good since,
/// removed or its member's keys replaced with a pair it doesn't hold (§14.1).
/// A replaced pair leaves the device's entry active, so the entry alone
/// doesn't decide. Until history names the device, a registration still
/// journaled may land. Read from the installation's own database, with
/// nothing of it running.
async fn registered(
    builder: &CovenBuilder,
    pending: &BootstrapStore,
) -> Result<bool, BootstrapError> {
    let directory = pending.directory();
    let device = directory.settings().map_err(CovenError::from)?.device_id;
    let database = open_installation(builder.database(directory)?, &|_| ()).await?;
    let facts = async {
        if database.device_stop().await.map_err(CovenError::from)?.is_some() {
            return Ok(false);
        }
        match database.store_state().await?.devices.get(&device) {
            Some(_) => Ok(true),
            None => Ok::<_, BootstrapError>(coven_sync::registration_journaled(&database).await?),
        }
    }
    .await;
    combine(facts, close_database(&database).await)
}
