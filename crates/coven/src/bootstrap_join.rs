//! Joining with an invite: the fixed request, provider access and the wait for
//! approval, which ends once the join finds its admission and publishes the
//! store, which loads after (§13.2).

use super::*;
use coven_crypto::ConfirmationNumber;
use coven_foundation::files::StoreDir;
use coven_foundation::locks::lock_unpoisoned;
use coven_sync::{ApprovalWaits, HeldIntervals, JoinOutcome, JoinPoll, JoiningIdentity, Parks};

/// A prepared join request with the provider access its invite grants.
pub(super) struct Joining {
    identity: JoiningIdentity,
    invitation: coven_storage::StorageInvitation,
    /// The intervals the app asks the join to check at.
    held: HeldIntervals,
    parks: Parks,
}

/// Restore or fix the request, then obtain credentials for the invite's
/// location: S3 invites carry them, Dropbox and OneDrive use the sign-in the
/// join keeps on the device beside its code, and CloudKit the Apple account.
pub(super) fn prepare_request(
    builder: &CovenBuilder,
    directory: &StoreDir,
    code: &InviteCode,
    terms: &RequestTerms,
    held: HeldIntervals,
    parks: Parks,
) -> Result<(Joining, ConnectionCredentials, SignInCustody), BootstrapError> {
    let identity = match terms {
        RequestTerms::Given { device_name, name } => {
            let device = directory.settings().map_err(CovenError::from)?.device_id;
            JoiningIdentity::prepare(directory, code, device, device_name, name.clone())?
        }
        RequestTerms::Saved => JoiningIdentity::resume(directory, code)?,
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
    Ok((Joining { identity, invitation, held, parks }, data, custody))
}

impl Joining {
    pub(super) fn identity(&self) -> coven_crypto::MemberIdentity {
        self.identity.identity()
    }

    /// The name the device gave itself in its request.
    pub(super) fn device_name(&self) -> &str {
        self.identity.device_name()
    }

    /// The new person's name the request carries, which the store this join
    /// publishes keeps until the app acknowledges it; none for a recovery.
    pub(super) fn invitee(&self) -> Option<String> {
        match self.identity.joiner() {
            Joiner::NewPerson { name } => Some(name.clone()),
            Joiner::MemberKeys => None,
        }
    }

    /// Whether the request may have been sent, here or before a restart.
    pub(super) fn sent(&self) -> bool {
        self.identity.attempted()
    }

    /// Whether the invite's share may have been accepted for this account
    /// while the request is unsent, here or before a restart: a cancel takes
    /// the acceptance back first (§13.2).
    pub(super) fn accepted(&self) -> bool {
        self.identity.accepted()
    }

    /// Take back the provider acceptance, for a join cancelled before its
    /// request was sent (§13.2).
    pub(super) async fn leave(&self, sync: &SequenceSync) -> Result<(), BootstrapError> {
        Ok(sync.leave_join(&self.invitation).await?)
    }

    /// Accept provider access, recording first that it accepts. Returns
    /// false when the invite has ended before the request was sent: it takes
    /// no first request (§13.2).
    pub(super) async fn accept(
        &mut self,
        sync: &SequenceSync,
        status: &impl Fn(BootstrapStatus),
    ) -> Result<bool, BootstrapError> {
        if !self.identity.attempted() && !sync.join_open(&self.identity) {
            return Ok(false);
        }
        status(BootstrapStatus::Connecting);
        self.identity.record_acceptance()?;
        sync.accept_join(&self.invitation).await?;
        Ok(true)
    }

    /// Send the request once, once the share is accepted, and wait for its
    /// outcome. Returns true once the join finds its admission, its copies'
    /// keys in the sync's custody, and false when the request was declined
    /// or expired. The connection refreshes its sign-in on rejection however
    /// long the wait lasts (§20.2).
    pub(super) async fn admit(
        &mut self,
        sync: &SequenceSync,
        approval: &ApprovalWaits,
        database: &Database,
        status: &impl Fn(BootstrapStatus),
    ) -> Result<bool, BootstrapError> {
        let sent_before = self.identity.attempted() && !self.identity.opened();
        if !self.identity.attempted() {
            self.identity.record_attempt()?;
            match sync.submit_join(&self.identity).await {
                Ok(true) => (),
                // The invite ended first; its marker holds the path.
                Ok(false) => return Ok(false),
                Err(error) => {
                    // Only a request that may be stored makes its absence final.
                    if matches!(&error, SyncError::Storage(error) if error.refused()) {
                        self.identity.record_refusal()?;
                    }
                    return Err(error.into());
                }
            }
        }
        // A send an earlier call recorded may not have reached storage.
        if sent_before {
            sync.resend_join(&self.identity).await?;
        }
        let confirmation = self.identity.confirmation();
        // Each status is shown once, when it changes.
        let shown = std::sync::Mutex::new(None);
        let show = |next: BootstrapStatus| {
            let mut shown = lock_unpoisoned(&shown);
            if shown.as_ref() != Some(&next) {
                status(next.clone());
                *shown = Some(next);
            }
        };
        // The conditions the last check found unmet, shown instead of the wait.
        let mut pending = Vec::new();
        let _running = self.parks.running();
        let mut poll = JoinPoll::new(approval.clone(), self.held.clone(), self.parks.clone());
        // Once its copies open, in its admission, in this call or an earlier
        // one, its status stays at loading, never going back to waiting
        // (E10). Polls back off 1, 2, 4, … 300 seconds apart; the persisted
        // delay survives reopening and a provider cooldown holds them (§6.7).
        loop {
            if !self.identity.opened() {
                show(waiting(confirmation, &pending));
            }
            poll.due().await?;
            match loading(sync, sync.join_outcome(&mut self.identity), status).await? {
                JoinOutcome::Admitted => {
                    approval.settled().await?;
                    return Ok(true);
                }
                JoinOutcome::Declined => {
                    approval.settled().await?;
                    return Ok(false);
                }
                JoinOutcome::Waiting => {
                    pending = database.pending::<coven_sync::PendingRecord>().await?;
                    approval.waiting().await?
                }
            }
        }
    }
}

/// What a waiting join shows: the unmet conditions its last check found, or
/// its wait for approval.
fn waiting(
    confirmation: ConfirmationNumber,
    pending: &[coven_sync::PendingRecord],
) -> BootstrapStatus {
    if pending.is_empty() {
        BootstrapStatus::WaitingForApproval { confirmation }
    } else {
        BootstrapStatus::Pending(pending.to_vec())
    }
}
