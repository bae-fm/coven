//! Joining with an invite: the fixed request, provider access and the wait for
//! approval, before the shared new-device loading (§12.2).

use super::*;
use coven_foundation::files::{AtomicFile, StoreDir, StoreFile};
use coven_sync::{JoinOutcome, JoiningIdentity};

/// A prepared join request with the provider access its invite grants.
pub(super) struct Joining<'a> {
    identity: JoiningIdentity,
    invitation: coven_storage::StorageInvitation,
    code: &'a InviteCode,
    // The encrypted local copy of the request, outside final custody.
    file: AtomicFile,
}

/// Restore or fix the request, then obtain credentials for the invite's
/// location: S3 invites carry them, account providers use the held sign-in.
pub(super) fn prepare_request<'a>(
    builder: &CovenBuilder,
    directory: &StoreDir,
    code: &'a InviteCode,
    name: &str,
) -> Result<
    (
        Joining<'a>,
        ConnectionCredentials,
        Option<Arc<SessionSignIn>>,
    ),
    BootstrapError,
> {
    let identity = JoiningIdentity::prepare(directory, code, name)?;
    let mut sign_in = None;
    let (invitation, credentials) =
        match InviteStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)? {
            InviteStorage::S3 {
                invitation,
                credentials,
            } => (invitation, StorageCredentials::S3(credentials)),
            InviteStorage::Account(invitation) => match invitation.location().provider() {
                CloudProvider::CloudKit => (invitation, StorageCredentials::CloudKit),
                provider => {
                    let held = account_sign_in(builder, provider)?;
                    let tokens = held.tokens()?;
                    sign_in = Some(held);
                    (invitation, StorageCredentials::OAuth(tokens))
                }
            },
        };
    let data = ConnectionCredentials {
        location: invitation.location().clone(),
        credentials,
    };
    // Validation refuses a sign-in without the location's access.
    data.validate().map_err(SyncError::from)?;
    Ok((
        Joining {
            identity,
            invitation,
            code,
            file: directory.owned_file(StoreFile::Bootstrap),
        },
        data,
        sign_in,
    ))
}

impl Joining<'_> {
    pub(super) fn identity(&self) -> coven_crypto::MemberIdentity {
        self.identity.identity()
    }

    /// Accept provider access, send the request once, and wait for its outcome.
    /// Returns false when the request was declined or expired.
    /// The connection refreshes its sign-in on rejection however long the
    /// wait lasts (§4).
    pub(super) async fn admit(
        &mut self,
        sync: &mut StoreLogSync,
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
        let mut shown = BootstrapStatus::WaitingForApproval;
        status(shown.clone());
        // Polls back off 1, 2, 4, … 300 seconds apart; the persisted delay
        // survives reopening and a provider cooldown holds them (§16.5).
        loop {
            sync.join_poll_due().await?;
            match sync.join_outcome(&self.identity).await? {
                JoinOutcome::Admitted => {
                    sync.join_poll_settled().await?;
                    return Ok(true);
                }
                JoinOutcome::Declined => {
                    sync.join_poll_settled().await?;
                    return Ok(false);
                }
                JoinOutcome::Waiting => {
                    let pending = database.pending::<coven_sync::PendingRecord>().await?;
                    let waiting = if pending.is_empty() {
                        BootstrapStatus::WaitingForApproval
                    } else {
                        BootstrapStatus::Pending(pending)
                    };
                    if waiting != shown {
                        shown = waiting;
                        status(shown.clone());
                    }
                    sync.join_poll_waiting().await?
                }
            }
        }
    }
}
