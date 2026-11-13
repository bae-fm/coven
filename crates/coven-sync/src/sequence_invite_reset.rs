//! A device reset keeps provider obligations, without resuming its invitations.

use super::*;
use crate::sequence_sync::sequence_pairings::PairingWork;
use coven_database::operation_kind;
use coven_foundation::files::DeviceReset;

impl SequenceSync {
    /// Carry a discarded installation's provider obligations after loading
    /// storage under its replacement id. Invites use their existing expiry
    /// cleanup; retained member access still follows the applied history;
    /// detection runs keep their records for a later detection or
    /// connection to clean up after (§20.7).
    pub async fn record_device_reset(
        &self,
        reset: DeviceReset,
        records: &[OperationRecord],
    ) -> Result<(), SyncError> {
        let session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut operations = crate::DetectionRecord::carried(records);
        let mut accounts = BTreeSet::new();
        for record in records {
            match record.kind.as_str() {
                operation_kind::INVITE => {
                    let mut work = InviteWork::read(record)?;
                    // An invite no pass has granted yet adopted no account.
                    if !matches!(work.state, InviteState::Requested | InviteState::Sharing { .. }) {
                        accounts.extend(work.account().map(str::to_owned));
                    }
                    // The operations it started stay with the discarded
                    // installation: the share it recorded there ends when
                    // this one registers in its place (§19.3).
                    work.state = InviteState::Settled { share: None, end: None, refusal: None };
                    operations.push(NewOperation {
                        kind: operation_kind::INVITE.into(),
                        data: serde_json::to_vec(&work)?,
                        started_by: operation_kind::COVEN.into(),
                        pauses_writes: false,
                    });
                }
                // A pairing's screen is gone with the discarded installation:
                // the new one puts the marker where the request is, under the
                // old device's id, and forgets the pairing.
                operation_kind::PAIRING => {
                    let mut work = PairingWork::read(record)?;
                    work.forget();
                    operations.push(NewOperation {
                        kind: operation_kind::PAIRING.into(),
                        data: serde_json::to_vec(&work)?,
                        started_by: operation_kind::COVEN.into(),
                        pauses_writes: false,
                    });
                }
                // An ended invite may already have deleted its journal while
                // its account's revocation is still owed.
                operation_kind::PROVIDER_ACCESS => {
                    accounts.insert(serde_json::from_slice::<String>(&record.data)?);
                }
                _ => (),
            }
        }
        let mut provider_access = Vec::new();
        // Only the owner's devices record provider work (§14.2).
        if !session.history.owns_storage(member.member()) {
            accounts.clear();
        }
        for account in accounts {
            // An active member or an open invite share, this installation's
            // under its old id included until its replacement registers,
            // still holds the account (§14.2).
            let intended = session.history.account_intended(&account);
            let mut change = crate::sequence_sync::account_change(&account, intended, false)?;
            // The discarded invite may have granted access even if its reply
            // was lost. The replacement must issue the owed revocation.
            change.initially_granted = true;
            provider_access.push(change);
        }
        self.database
            .record_device_reset::<PendingRecord>(reset, operations, provider_access)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_invite_reset_tests.rs"]
mod tests;
