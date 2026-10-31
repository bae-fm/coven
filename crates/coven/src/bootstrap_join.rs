//! Joining with an invite: the fixed request, provider access and the wait for
//! approval, before the shared new-device loading (§13.2).

use super::*;
use coven_crypto::ConfirmationNumber;
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{AtomicFile, StoreDir, StoreFile};
use coven_sync::{JoinOutcome, JoiningIdentity, NextCheck};

/// A prepared join request with the provider access its invite grants.
pub(super) struct Joining<'a> {
    identity: JoiningIdentity,
    invitation: coven_storage::StorageInvitation,
    code: &'a InviteCode,
    // The encrypted local copy of the request, outside final custody.
    file: AtomicFile,
    /// Changes each time the app asks for a check for approval now.
    check: tokio::sync::watch::Receiver<u64>,
}

/// Restore or fix the request, then obtain credentials for the invite's
/// location: S3 invites carry them, Dropbox and OneDrive use the sign-in the
/// join keeps on the device beside its code, and CloudKit the Apple account.
pub(super) fn prepare_request<'a>(
    builder: &CovenBuilder,
    directory: &StoreDir,
    code: &'a InviteCode,
    name: &DeviceName,
    check: tokio::sync::watch::Receiver<u64>,
) -> Result<(Joining<'a>, ConnectionCredentials, SignInCustody), BootstrapError> {
    let identity = match name {
        DeviceName::Given(name) => JoiningIdentity::prepare(directory, code, name)?,
        DeviceName::Saved => JoiningIdentity::resume(directory, code)?,
    };
    // Kept from before the request can be sent until the join ends, so a
    // join that may have sent its request is always listed and resumable
    // (§13.2). Its request is saved first: a listed join can be read.
    StoreKeychain::new(builder.keychain()?, code.store)
        .set_invite_code(&SecretText::new(code.to_text().map_err(SyncError::from)?.to_string()))?;
    let (invitation, credentials, custody) = match InviteStorage::decode(code.storage.as_bytes())
        .map_err(SyncError::from)?
    {
        InviteStorage::S3 { invitation, credentials } => {
            (invitation, StorageCredentials::S3(credentials), SignInCustody::None)
        }
        InviteStorage::Account(invitation) => match invitation.location().provider() {
            CloudProvider::CloudKit => {
                (invitation, StorageCredentials::CloudKit, SignInCustody::None)
            }
            // The sign-in is kept only once the code is: a kept sign-in
            // always belongs to a listed join.
            _ => {
                let (custody, tokens) = join_sign_in(builder, code.store, invitation.location())?;
                (invitation, StorageCredentials::OAuth(tokens), custody)
            }
        },
    };
    let data = ConnectionCredentials { location: invitation.location().clone(), credentials };
    // Validation refuses a sign-in without the location's access.
    data.validate().map_err(SyncError::from)?;
    Ok((
        Joining {
            identity,
            invitation,
            code,
            file: directory.owned_file(StoreFile::Bootstrap),
            check,
        },
        data,
        custody,
    ))
}

impl Joining<'_> {
    pub(super) fn identity(&self) -> coven_crypto::MemberIdentity {
        self.identity.identity()
    }

    /// The name the device gave itself in its request.
    pub(super) fn device_name(&self) -> &str {
        self.identity.device_name()
    }

    /// Whether the request may have been sent, here or before a restart.
    pub(super) fn sent(&self) -> bool {
        self.identity.attempted()
    }

    /// Accept provider access, send the request once, and wait for its outcome.
    /// Returns false when the request was declined or expired.
    /// The connection refreshes its sign-in on rejection however long the
    /// wait lasts (§20.2).
    pub(super) async fn admit(
        &mut self,
        sync: &SequenceSync,
        database: &Database,
        status: &impl Fn(BootstrapStatus),
    ) -> Result<bool, BootstrapError> {
        status(BootstrapStatus::Connecting);
        sync.accept_join(&self.invitation).await?;
        if !self.identity.attempted() {
            self.identity.record_attempt(&self.file, self.code)?;
            if let Err(error) = sync.submit_join(&self.identity).await {
                // Only a request that may be stored makes its absence final.
                if matches!(&error, SyncError::Storage(error) if error.refused()) {
                    self.identity.record_refusal(&self.file, self.code)?;
                }
                return Err(error.into());
            }
        }
        let confirmation = self.identity.confirmation();
        // Each status is shown once, when the next check changes.
        let shown = std::sync::Mutex::new(None);
        let show = |next: BootstrapStatus| {
            let mut shown = shown.lock().expect("join status lock poisoned");
            if shown.as_ref() != Some(&next) {
                status(next.clone());
                *shown = Some(next);
            }
        };
        // The conditions the last check found unmet, shown instead of the wait.
        let mut pending = Vec::new();
        // Polls back off 1, 2, 4, … 300 seconds apart; the persisted delay
        // survives reopening and a provider cooldown holds them (§6.7).
        loop {
            sync.join_poll_due(&mut self.check, |next_check| {
                show(waiting(confirmation, &pending, next_check));
            })
            .await?;
            show(waiting(confirmation, &pending, NextCheck::Checking));
            match loading(sync, sync.join_outcome(&self.identity), status).await? {
                JoinOutcome::Admitted => {
                    sync.join_poll_settled().await?;
                    return Ok(true);
                }
                JoinOutcome::Declined => {
                    sync.join_poll_settled().await?;
                    return Ok(false);
                }
                JoinOutcome::Waiting => {
                    pending = database.pending::<coven_sync::PendingRecord>().await?;
                    sync.join_poll_waiting().await?
                }
            }
        }
    }
}

/// What a waiting join shows: the unmet conditions its last check found, or
/// its wait for approval.
fn waiting(
    confirmation: Option<ConfirmationNumber>,
    pending: &[coven_sync::PendingRecord],
    next_check: NextCheck,
) -> BootstrapStatus {
    if pending.is_empty() {
        BootstrapStatus::WaitingForApproval { confirmation, next_check }
    } else {
        BootstrapStatus::Pending(pending.to_vec())
    }
}
