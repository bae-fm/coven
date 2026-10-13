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
/// location: S3 invites carry them, account providers sign in.
pub(super) async fn prepare_request<'a>(
    builder: &mut CovenBuilder,
    directory: &StoreDir,
    code: &'a InviteCode,
    name: &str,
    cancel: &watch::Receiver<bool>,
) -> Result<(Joining<'a>, ConnectionCredentials), BootstrapError> {
    let identity = JoiningIdentity::prepare(directory, code, name)?;
    let (invitation, credentials) =
        match InviteStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)? {
            InviteStorage::S3 {
                invitation,
                credentials,
            } => (invitation, StorageCredentials::S3(credentials)),
            InviteStorage::Account(invitation) => {
                let provider = invitation.location().provider();
                let credentials = account_credentials(builder, provider, cancel).await?;
                (invitation, credentials)
            }
        };
    let data = ConnectionCredentials {
        location: invitation.location().clone(),
        credentials,
    };
    data.validate().map_err(SyncError::from)?;
    if let StorageCredentials::OAuth(tokens) = &data.credentials {
        let required = if data.location.provider() == CloudProvider::GoogleDrive {
            OAuthAccess::GoogleDrive(GoogleDriveAccess::Full)
        } else {
            data.location.oauth_access().expect("OAuth location")
        };
        if !tokens.access.permits(required) {
            return Err(SyncError::from(StorageFailure::AccessNotGranted { required }).into());
        }
    }
    Ok((
        Joining {
            identity,
            invitation,
            code,
            file: directory.owned_file(StoreFile::Bootstrap),
        },
        data,
    ))
}

impl Joining<'_> {
    pub(super) fn member_keys(&self) -> coven_crypto::MemberKeys {
        self.identity.member_keys()
    }

    /// Accept provider access, send the request once, and wait for its outcome.
    /// Returns false when the request was declined or expired.
    pub(super) async fn admit(
        &mut self,
        builder: &mut CovenBuilder,
        sync: &mut StoreLogSync,
        data: &mut ConnectionCredentials,
        storage: &dyn coven_storage::Storage,
        status: &impl Fn(&str),
        cancel: &watch::Receiver<bool>,
    ) -> Result<bool, BootstrapError> {
        status("Accepting provider access");
        sync.accept_join(&self.invitation).await?;
        if !self.identity.attempted() {
            status("Sending the join request");
            self.identity.record_attempt(&self.file, self.code)?;
            sync.submit_join(&self.identity).await?;
        }
        status("Waiting for approval");
        loop {
            refresh_sign_in(builder, data, storage, cancel).await?;
            match sync.join_outcome(&self.identity).await? {
                JoinOutcome::Admitted => return Ok(true),
                JoinOutcome::Declined => return Ok(false),
                JoinOutcome::Waiting => {}
            }
            builder.clock.sleep(Duration::from_secs(1)).await;
        }
    }
}
