//! A device reset keeps provider obligations, without resuming its invitations.

use super::*;
use coven_database::ProviderChanges;
use coven_foundation::files::DeviceReset;

impl SequenceSync {
    /// Carry a discarded installation's provider obligations after loading
    /// storage under its replacement id. Invites use their existing expiry
    /// cleanup; retained member access still follows the applied history.
    pub async fn record_device_reset(
        &self,
        reset: DeviceReset,
        records: &[OperationRecord],
    ) -> Result<(), SyncError> {
        let session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut operations = Vec::new();
        let mut accounts = BTreeSet::new();
        for record in records {
            match record.kind.as_str() {
                INVITE => {
                    let mut work = InviteWork::read(record)?;
                    // An invite no pass has checked yet adopted no account.
                    if !matches!(work.state, InviteState::Requested) {
                        accounts.extend(work.account().map(str::to_owned));
                    }
                    if !matches!(work.state, InviteState::Settled) {
                        work.state = InviteState::Settled;
                    }
                    operations.push(NewOperation {
                        kind: INVITE.into(),
                        data: serde_json::to_vec(&work)?,
                        started_by: "coven".into(),
                        pauses_writes: false,
                    });
                }
                // An ended invite may already have deleted its journal while
                // its account's revocation is still owed.
                "provider-access" => {
                    accounts.insert(serde_json::from_slice::<String>(&record.data)?);
                }
                _ => (),
            }
        }
        let mut provider = ProviderChanges { accounts: Vec::new() };
        // Only the owner's devices record provider work (§14.2).
        if !session.history.owns_storage(member.member()) {
            accounts.clear();
        }
        for account in accounts {
            // An active member or an open invite share, this installation's
            // under its old id included until its replacement registers,
            // still holds the account (§14.2).
            let intended = session.history.account_intended(&account);
            let mut change = self.account_change(&account, intended, false)?;
            // The discarded invite may have granted access even if its reply
            // was lost. The replacement must issue the owed revocation.
            change.initially_granted = true;
            provider.accounts.push(change);
        }
        self.database.record_device_reset::<PendingRecord>(reset, operations, provider).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_invite_reset_tests.rs"]
mod tests;
