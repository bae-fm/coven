//! Member S3 keys coven makes with admin access (§4, §18). One mint call names
//! its principal afresh, deletes nothing, makes the key and checks it from the
//! member's side; only a checked key joins the journal. A failed call rolls
//! back what its own names made and reports both causes if that fails too, so
//! no copy of the journal holds a key another copy could delete (KeyListing
//! `clone_mint_spares_recorded_key`, `clone_discard_spares_recorded_key`).
//! Admin access stays in memory with its operation.

use super::StoreLogSync;
use crate::*;
use coven_storage::{AccessKeyAdmin, MintScope, S3MemberKey};
use std::sync::Arc;

impl StoreLogSync {
    /// Keep admin access for this operation only, replacing any earlier one.
    pub(crate) fn hold_admin_access(&mut self, id: OperationId, admin: Arc<dyn AccessKeyAdmin>) {
        self.admin_access.insert(id, admin);
    }

    /// Forget this operation's admin access once its provider work is done.
    pub(crate) fn release_admin_access(&mut self, id: OperationId) {
        self.admin_access.remove(&id);
    }

    pub(crate) fn admin_access(&self, id: OperationId) -> Option<Arc<dyn AccessKeyAdmin>> {
        self.admin_access.get(&id).cloned()
    }

    /// Make a key under names fresh for this call and check it from the
    /// member's side, or roll back what the call made.
    pub(crate) async fn mint_checked(
        &self,
        admin: &dyn AccessKeyAdmin,
        template: &MintScope,
    ) -> Result<S3MemberKey, SyncError> {
        let scope = MintScope::new(
            template.store,
            &template.member,
            template.device,
            self.ids.new_id(),
        );
        let error = match admin.mint(&scope).await {
            Ok(key) => match admin.probe(&scope, &key.credentials).await {
                Ok(()) => return Ok(key),
                Err(error) => SyncError::Storage(error),
            },
            Err(error) => SyncError::Storage(error),
        };
        match admin.roll_back(&scope).await {
            Ok(()) => Err(error),
            Err(cleanup) => Err(SyncError::Cleanup {
                operation: Box::new(error),
                cleanup: Box::new(SyncError::Storage(cleanup)),
            }),
        }
    }
}
