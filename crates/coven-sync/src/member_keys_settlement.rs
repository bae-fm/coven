//! Settling a member-key replacement (§9, §10, §18): a kept own binding
//! installs its candidate and refreshes the synced restore code before the
//! pause lifts, a dropped one is reported and keeps its candidate held, and
//! discarding releases the pause only when nothing remains behind it.

use super::StoreLogSync;
use crate::{
    operation_data::{Data, MemberKeysWork},
    operations::{Begun, Output, Progress},
    *,
};
use coven_database::{EntryOutcome, OperationRecord};

impl StoreLogSync {
    /// Make the kept binding's pair the active one, keeping the previous
    /// pair for copies that name it (§11). Repeating it changes nothing.
    pub(super) fn install_member_keys(&self, work: &MemberKeysWork) -> Result<(), SyncError> {
        let keys = work.keys.ok_or(coven_database::DbError::Invariant {
            what: "a kept self-replacement names its candidate",
        })?;
        let identity = self.operation_member()?;
        if identity.keys().public_keys() == keys && identity.candidate().is_none() {
            return Ok(());
        }
        let installed = identity
            .activate(&keys)
            .ok_or(SyncError::MemberKeysReplaced {
                member: work.member,
            })?;
        self.member_keys.persist(&installed)?;
        Ok(())
    }

    /// A dropped replacement ends with its replay reason; nothing publishes
    /// again over the winner (§9, E9). An own candidate stays held as a
    /// retired credential, because copies to it may exist (§11).
    pub(super) async fn reject_member_keys(
        &self,
        record: &OperationRecord,
        work: &MemberKeysWork,
    ) -> Result<(), SyncError> {
        if work.own {
            let identity = self.operation_member()?;
            if identity.candidate().is_some() {
                self.member_keys.persist(&identity.retire_candidate())?;
            }
        }
        Ok(self.database.finish_operation(record.id).await?)
    }

    /// After the binding is kept: install its pair and refresh the synced
    /// restore code, then finish, which releases the pause (§18).
    pub(super) async fn member_keys_finish(
        &self,
        record: &OperationRecord,
        work: &MemberKeysWork,
    ) -> Result<Progress, SyncError> {
        if work.own {
            self.install_member_keys(work)?;
            self.refresh_restore_code().await?;
        }
        self.database.finish_operation(record.id).await?;
        Ok(Progress::Finished(Output::Unit))
    }

    async fn refresh_restore_code(&self) -> Result<(), SyncError> {
        let Some(codes) = &self.restore_code else {
            return Ok(());
        };
        let local = self.database.local_store_log().await?;
        codes.refresh(&local, &self.operation_member()?)
    }

    /// Discarding a self-replacement releases its pause only when nothing
    /// remains behind it (§18): before reservation while the held pair is
    /// still current, or once replay settles its entry and custody installs
    /// a kept binding. Otherwise the call returns the work's pending record.
    /// An admin's replacement is only its entry, which the generic discard
    /// publishes when reserved.
    pub(super) async fn discard_member_keys(
        &self,
        record: &OperationRecord,
    ) -> Result<Option<Begun>, SyncError> {
        let data = Data::read(record)?;
        let Data::ReplaceMemberKeys(work) = &data else {
            return Ok(None);
        };
        if !work.own {
            return Ok(None);
        }
        let local = self.database.local_store_log().await?;
        let identity = self.operation_member()?;
        let released = match data.entry()? {
            None if super::member_keys_replacement::held_current(&local, &identity).is_ok() => {
                // Nothing was reserved, so no copy or entry names the candidate.
                if identity.candidate().is_some() {
                    self.member_keys.persist(&identity.without_candidate())?;
                }
                true
            }
            None => false,
            Some(entry) => match local.log.replay.entries.get(&entry.position) {
                Some(EntryOutcome::Dropped(_)) => {
                    if identity.candidate().is_some() {
                        self.member_keys.persist(&identity.retire_candidate())?;
                    }
                    true
                }
                Some(EntryOutcome::Kept) => {
                    self.install_member_keys(work)?;
                    self.refresh_restore_code().await?;
                    true
                }
                None => false,
            },
        };
        if !released {
            let pending = self
                .database
                .operation_pending::<PendingRecord>(record.id)
                .await?
                .ok_or(SyncError::NotPending(record.id))?;
            return Err(SyncError::Pending(Box::new(pending)));
        }
        self.database.finish_operation(record.id).await?;
        Ok(Some(Begun::Value(Output::Unit)))
    }
}
